use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::{TcpListener, TcpStream};

use crate::channel::{drain_eventfd, notify_eventfd, ShardRequest, ShardResponse};
use crate::protocol::codec::{decode_request, encode_response};
use crate::protocol::types::{Response, Status};
use crate::shard::router::ShardRouter;

/// Per-connection response sender, stored in a shared map.
/// The connection task polls this for responses.
type ResponseSender = Rc<RefCell<Vec<ShardResponse>>>;
type ResponseMap = Rc<RefCell<HashMap<u64, ResponseSender>>>;

/// TCP acceptor running on its own monoio runtime.
pub struct Acceptor {
    listener: TcpListener,
    router: ShardRouter,
    shard_txs: Vec<crossbeam_channel::Sender<ShardRequest>>,
    request_eventfds: Vec<i32>,
    response_rxs: Vec<crossbeam_channel::Receiver<ShardResponse>>,
    response_eventfds: Vec<i32>,
}

impl Acceptor {
    pub fn new(
        listener: TcpListener,
        num_shards: u16,
        shard_txs: Vec<crossbeam_channel::Sender<ShardRequest>>,
        request_eventfds: Vec<i32>,
        response_rxs: Vec<crossbeam_channel::Receiver<ShardResponse>>,
        response_eventfds: Vec<i32>,
    ) -> Self {
        Self {
            listener,
            router: ShardRouter::new(num_shards),
            shard_txs,
            request_eventfds,
            response_rxs,
            response_eventfds,
        }
    }

    pub async fn run(self) {
        let response_map: ResponseMap = Rc::new(RefCell::new(HashMap::new()));
        let next_conn_id = Rc::new(RefCell::new(0u64));

        // Spawn response drainer — collects from crossbeam channels and dispatches
        // to per-connection response queues
        let rmap = response_map.clone();
        let response_rxs = self.response_rxs;
        let response_eventfds = self.response_eventfds;
        monoio::spawn(async move {
            drain_responses_to_map(rmap, response_rxs, response_eventfds).await;
        });

        tracing::info!("acceptor listening");

        loop {
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    let conn_id = {
                        let mut id = next_conn_id.borrow_mut();
                        let cid = *id;
                        *id += 1;
                        cid
                    };

                    tracing::debug!(conn_id, %addr, "new connection");

                    // Create a response queue for this connection
                    let resp_queue: ResponseSender = Rc::new(RefCell::new(Vec::new()));
                    response_map.borrow_mut().insert(conn_id, resp_queue.clone());

                    let shard_txs = self.shard_txs.clone();
                    let request_eventfds = self.request_eventfds.clone();
                    let num_shards = self.router.num_shards();
                    let rmap = response_map.clone();

                    monoio::spawn(async move {
                        handle_connection(
                            conn_id,
                            stream,
                            resp_queue,
                            shard_txs,
                            request_eventfds,
                            num_shards,
                        )
                        .await;
                        rmap.borrow_mut().remove(&conn_id);
                        tracing::debug!(conn_id, "connection closed");
                    });
                }
                Err(e) => {
                    tracing::error!("accept error: {e}");
                }
            }
        }
    }
}

async fn handle_connection(
    conn_id: u64,
    mut stream: TcpStream,
    resp_queue: ResponseSender,
    shard_txs: Vec<crossbeam_channel::Sender<ShardRequest>>,
    request_eventfds: Vec<i32>,
    num_shards: u16,
) {
    let router = ShardRouter::new(num_shards);
    let mut parse_buf = Vec::with_capacity(8192);

    loop {
        // Flush any pending responses first
        {
            let mut queue = resp_queue.borrow_mut();
            for shard_resp in queue.drain(..) {
                let resp_bytes = encode_response(&shard_resp.response);
                let (res, _) = stream.write_all(resp_bytes.to_vec()).await;
                if res.is_err() {
                    return;
                }
            }
        }

        // Read data from socket. monoio takes ownership of the buffer and returns it.
        let read_buf = vec![0u8; 4096];
        let (res, read_buf) = stream.read(read_buf).await;

        match res {
            Ok(0) => return, // EOF
            Ok(n) => {
                parse_buf.extend_from_slice(&read_buf[..n]);
            }
            Err(e) => {
                tracing::debug!(conn_id, "read error: {e}");
                return;
            }
        }

        // Parse complete frames
        loop {
            match decode_request(&parse_buf) {
                Ok(Some((req, consumed))) => {
                    let shard_id = router.route(&req.key);
                    let shard_req = ShardRequest {
                        connection_id: conn_id,
                        request: req,
                    };

                    if shard_txs[shard_id as usize].try_send(shard_req).is_ok() {
                        notify_eventfd(request_eventfds[shard_id as usize]);
                    } else {
                        let resp = encode_response(&Response {
                            status: Status::ErrShardUnavailable,
                            lsn: 0,
                        });
                        let (res, _) = stream.write_all(resp.to_vec()).await;
                        if res.is_err() {
                            return;
                        }
                    }

                    parse_buf.drain(..consumed);
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!(conn_id, "protocol error: {e}");
                    return;
                }
            }
        }
    }
}

/// Background task: drain crossbeam response channels and dispatch
/// to per-connection response queues.
async fn drain_responses_to_map(
    response_map: ResponseMap,
    response_rxs: Vec<crossbeam_channel::Receiver<ShardResponse>>,
    response_eventfds: Vec<i32>,
) {
    loop {
        let mut any_work = false;

        for (i, rx) in response_rxs.iter().enumerate() {
            drain_eventfd(response_eventfds[i]);
            while let Ok(shard_resp) = rx.try_recv() {
                any_work = true;
                let map = response_map.borrow();
                if let Some(queue) = map.get(&shard_resp.connection_id) {
                    queue.borrow_mut().push(shard_resp);
                }
            }
        }

        if !any_work {
            monoio::time::sleep(Duration::from_micros(100)).await;
        }
    }
}
