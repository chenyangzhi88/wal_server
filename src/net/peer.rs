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
    raft_txs: HashMap<u16, crossbeam_channel::Sender<RaftInbound>>,
    raft_eventfds: HashMap<u16, i32>,
    outbound_rxs: Vec<crossbeam_channel::Receiver<RaftOutbound>>,
}

impl PeerTransport {
    pub fn new(
        listener: TcpListener,
        peers: Vec<PeerConfig>,
        raft_txs: HashMap<u16, crossbeam_channel::Sender<RaftInbound>>,
        raft_eventfds: HashMap<u16, i32>,
        outbound_rxs: Vec<crossbeam_channel::Receiver<RaftOutbound>>,
    ) -> Self {
        Self {
            listener,
            peers: peers.into_iter().map(|p| (p.id, p.addr)).collect(),
            raft_txs,
            raft_eventfds,
            outbound_rxs,
        }
    }

    pub async fn run(self) {
        let PeerTransport {
            listener,
            peers,
            raft_txs,
            raft_eventfds,
            outbound_rxs,
        } = self;

        let inbound_txs = raft_txs.clone();
        let inbound_eventfds = raft_eventfds.clone();
        monoio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let txs = inbound_txs.clone();
                        let eventfds = inbound_eventfds.clone();
                        monoio::spawn(async move {
                            handle_inbound(stream, txs, eventfds).await;
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
            let mut any_work = false;
            for outbound_rx in &outbound_rxs {
                match outbound_rx.try_recv() {
                    Ok(outbound) => {
                        any_work = true;
                        let target_id = outbound.target_id;
                        if let Some(tx) = peer_queues.get(&target_id) {
                            let mut outbound = outbound;
                            loop {
                                match tx.try_send(outbound) {
                                    Ok(()) => break,
                                    Err(crossbeam_channel::TrySendError::Full(returned)) => {
                                        outbound = returned;
                                        tracing::warn!(
                                            target_id,
                                            "peer sender queue full, retrying"
                                        );
                                        monoio::time::sleep(Duration::from_millis(1)).await;
                                    }
                                    Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                                        tracing::warn!(target_id, "peer sender stopped");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Err(crossbeam_channel::TryRecvError::Empty) => {}
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {}
                }
            }
            if !any_work {
                monoio::time::sleep(Duration::from_millis(10)).await;
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
            match encode_raft_message(outbound.group_id, &outbound.message) {
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
    raft_txs: HashMap<u16, crossbeam_channel::Sender<RaftInbound>>,
    raft_eventfds: HashMap<u16, i32>,
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
                Ok(Some((group_id, message, consumed))) => {
                    let Some(raft_tx) = raft_txs.get(&group_id) else {
                        tracing::warn!(group_id, "dropping raft frame for unknown group");
                        parse_buf.drain(..consumed);
                        continue;
                    };
                    let Some(&raft_eventfd) = raft_eventfds.get(&group_id) else {
                        tracing::warn!(group_id, "missing eventfd for raft group");
                        parse_buf.drain(..consumed);
                        continue;
                    };
                    let mut inbound = RaftInbound { group_id, message };
                    loop {
                        match raft_tx.try_send(inbound) {
                            Ok(()) => {
                                notify_eventfd(raft_eventfd);
                                break;
                            }
                            Err(crossbeam_channel::TrySendError::Full(returned)) => {
                                inbound = returned;
                                tracing::warn!(group_id, "raft inbound queue full, retrying");
                                monoio::time::sleep(Duration::from_millis(1)).await;
                            }
                            Err(crossbeam_channel::TrySendError::Disconnected(_)) => return,
                        }
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
