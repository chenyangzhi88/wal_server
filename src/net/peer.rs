use std::collections::HashMap;
use std::time::Duration;

use crossbeam_channel::Receiver;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::{TcpListener, TcpStream};

use crate::channel::{
    encode_raft_message, notify_eventfd, try_decode_raft_message, RaftInbound, RaftOutbound,
};
use crate::config::PeerConfig;

pub struct PeerTransport {
    listener: TcpListener,
    peers: HashMap<u64, String>,
    raft_tx: crossbeam_channel::Sender<RaftInbound>,
    raft_eventfd: i32,
    outbound_rx: crossbeam_channel::Receiver<RaftOutbound>,
}

impl PeerTransport {
    pub fn new(
        listener: TcpListener,
        peers: Vec<PeerConfig>,
        raft_tx: crossbeam_channel::Sender<RaftInbound>,
        raft_eventfd: i32,
        outbound_rx: crossbeam_channel::Receiver<RaftOutbound>,
    ) -> Self {
        Self {
            listener,
            peers: peers.into_iter().map(|p| (p.id, p.addr)).collect(),
            raft_tx,
            raft_eventfd,
            outbound_rx,
        }
    }

    pub async fn run(self) {
        let PeerTransport {
            listener,
            peers,
            raft_tx,
            raft_eventfd,
            outbound_rx,
        } = self;

        let inbound_tx = raft_tx.clone();
        let inbound_eventfd = raft_eventfd;
        monoio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let tx = inbound_tx.clone();
                        monoio::spawn(async move {
                            handle_inbound(stream, tx, inbound_eventfd).await;
                        });
                    }
                    Err(e) => tracing::error!("peer accept error: {e}"),
                }
            }
        });

        let mut peer_queues = HashMap::new();
        for (peer_id, addr) in peers {
            let (tx, rx) = crossbeam_channel::bounded(4096);
            peer_queues.insert(peer_id, tx);
            monoio::spawn(async move {
                run_peer_sender(addr, rx).await;
            });
        }

        loop {
            match outbound_rx.try_recv() {
                Ok(outbound) => {
                    let target_id = outbound.target_id;
                    if let Some(tx) = peer_queues.get(&target_id) {
                        if tx.send(outbound).is_err() {
                            tracing::warn!(target_id, "peer sender stopped");
                        }
                    }
                }
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    monoio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(crossbeam_channel::TryRecvError::Disconnected) => return,
            }
        }
    }
}

async fn run_peer_sender(addr: String, rx: Receiver<RaftOutbound>) {
    let mut stream = None;
    loop {
        let first = match rx.try_recv() {
            Ok(message) => message,
            Err(crossbeam_channel::TryRecvError::Empty) => {
                monoio::time::sleep(Duration::from_millis(2)).await;
                continue;
            }
            Err(crossbeam_channel::TryRecvError::Disconnected) => return,
        };

        let mut batch = Vec::with_capacity(128);
        batch.push(first);
        while batch.len() < 128 {
            match rx.try_recv() {
                Ok(message) => batch.push(message),
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => break,
            }
        }

        if stream.is_none() {
            match TcpStream::connect(addr.as_str()).await {
                Ok(conn) => stream = Some(conn),
                Err(e) => {
                    tracing::warn!("raft connect failed to {}: {e}", addr);
                    monoio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            }
        }

        let mut frames = Vec::new();
        let mut encode_failed = false;
        for outbound in batch {
            match encode_raft_message(&outbound.message) {
                Ok(frame) => frames.extend_from_slice(&frame),
                Err(e) => {
                    tracing::warn!("raft outbound encode failed: {e}");
                    encode_failed = true;
                    break;
                }
            }
        }
        if encode_failed || frames.is_empty() {
            continue;
        }

        let mut conn = stream.take().expect("stream established");
        let (res, _) = conn.write_all(frames).await;
        match res {
            Ok(_) => stream = Some(conn),
            Err(e) => {
                tracing::warn!("raft outbound send failed to {}: {e}", addr);
                monoio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

async fn handle_inbound(
    mut stream: TcpStream,
    raft_tx: crossbeam_channel::Sender<RaftInbound>,
    raft_eventfd: i32,
) {
    let mut parse_buf = Vec::with_capacity(8192);

    loop {
        let read_buf = vec![0u8; 4096];
        let (res, read_buf) = stream.read(read_buf).await;
        match res {
            Ok(0) => return,
            Ok(n) => parse_buf.extend_from_slice(&read_buf[..n]),
            Err(_) => return,
        }

        loop {
            match try_decode_raft_message(&parse_buf) {
                Ok(Some((message, consumed))) => {
                    if raft_tx.try_send(RaftInbound { message }).is_ok() {
                        notify_eventfd(raft_eventfd);
                    }
                    parse_buf.drain(..consumed);
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!("invalid raft peer frame: {e}");
                    return;
                }
            }
        }
    }
}
