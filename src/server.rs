use std::thread;

use crate::channel::ShardMailbox;
use crate::config::ServerConfig;
use crate::net::acceptor::Acceptor;
use crate::net::peer::PeerTransport;
use crate::numa::placement::ShardPlacement;
use crate::shard::engine::ShardEngine;

/// Start the WAL server: spawn shard threads + acceptor thread(s).
pub fn start(config: ServerConfig, placement: ShardPlacement) {
    let num_shards = placement.shard_assignments.len();
    tracing::info!(num_shards, "starting WAL server");

    // Create mailboxes for each shard
    let mut mailboxes: Vec<ShardMailbox> = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        let mailbox =
            ShardMailbox::new(config.channel_capacity).expect("failed to create shard mailbox");
        mailboxes.push(mailbox);
    }

    // Collect channel endpoints for the acceptor
    let shard_txs: Vec<_> = mailboxes.iter().map(|m| m.request_tx.clone()).collect();
    let request_eventfds: Vec<_> = mailboxes.iter().map(|m| m.request_eventfd).collect();
    let response_rxs: Vec<_> = mailboxes.iter().map(|m| m.response_rx.clone()).collect();
    let response_eventfds: Vec<_> = mailboxes.iter().map(|m| m.response_eventfd).collect();
    let raft_txs: Vec<_> = mailboxes.iter().map(|m| m.raft_tx.clone()).collect();
    let raft_outbound_rxs: Vec<_> = mailboxes
        .iter()
        .map(|m| m.raft_outbound_rx.clone())
        .collect();
    let raft_eventfds: Vec<_> = mailboxes.iter().map(|m| m.raft_eventfd).collect();

    // Spawn shard threads
    let mut shard_handles = Vec::with_capacity(num_shards);

    for (i, assignment) in placement.shard_assignments.iter().enumerate() {
        let shard_id = assignment.shard_id;
        let cpu_id = assignment.cpu_id;
        let shard_config = config.clone();

        let request_rx = mailboxes[i].request_rx.clone();
        let response_tx = mailboxes[i].response_tx.clone();
        let req_efd = mailboxes[i].request_eventfd;
        let resp_efd = mailboxes[i].response_eventfd;
        let raft_rx = mailboxes[i].raft_rx.clone();
        let raft_outbound_tx = mailboxes[i].raft_outbound_tx.clone();
        let raft_efd = mailboxes[i].raft_eventfd;

        let handle = thread::Builder::new()
            .name(format!("shard-{}", shard_id))
            .spawn(move || {
                // Bind to CPU core
                crate::numa::placement::bind_to_cpu(cpu_id);
                tracing::info!(shard_id, cpu_id, "shard thread started");

                // Create monoio runtime on this thread
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("failed to build monoio runtime");

                rt.block_on(async move {
                    let engine = ShardEngine::open(
                        shard_id,
                        shard_config,
                        request_rx,
                        raft_rx,
                        response_tx,
                        raft_outbound_tx,
                        req_efd,
                        raft_efd,
                        resp_efd,
                    )
                    .await
                    .expect("failed to open shard engine");

                    if let Err(e) = engine.run().await {
                        tracing::error!(shard_id, "shard engine error: {e}");
                    }
                });
            })
            .expect("failed to spawn shard thread");

        shard_handles.push(handle);
    }

    // Spawn acceptor thread(s) — one per NUMA node
    let mut acceptor_handles = Vec::new();
    let mut peer_handles = Vec::new();

    for &acceptor_cpu in &placement.acceptor_cpus {
        let listen_addr = config.listen_addr.clone();
        let num_shards = num_shards as u16;
        let txs = shard_txs.clone();
        let req_efds = request_eventfds.clone();
        let rxs = response_rxs.clone();
        let resp_efds = response_eventfds.clone();

        let handle = thread::Builder::new()
            .name("acceptor".to_string())
            .spawn(move || {
                crate::numa::placement::bind_to_cpu(acceptor_cpu);
                tracing::info!(cpu_id = acceptor_cpu, "acceptor thread started");

                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("failed to build monoio runtime for acceptor");

                rt.block_on(async move {
                    let listener = monoio::net::TcpListener::bind(&listen_addr)
                        .expect("failed to bind TCP listener");
                    tracing::info!(addr = %listen_addr, "listening");

                    let acceptor =
                        Acceptor::new(listener, num_shards, txs, req_efds, rxs, resp_efds);
                    acceptor.run().await;
                });
            })
            .expect("failed to spawn acceptor thread");

        acceptor_handles.push(handle);
    }

    for &acceptor_cpu in &placement.acceptor_cpus {
        let raft_listen_addr = config.raft_listen_addr.clone();
        let peers = config.peers.clone();
        let raft_tx = raft_txs[0].clone();
        let outbound_rx = raft_outbound_rxs[0].clone();
        let raft_eventfd = raft_eventfds[0];

        let handle = thread::Builder::new()
            .name("peer-transport".to_string())
            .spawn(move || {
                crate::numa::placement::bind_to_cpu(acceptor_cpu);
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("failed to build monoio runtime for peer transport");

                rt.block_on(async move {
                    let listener = monoio::net::TcpListener::bind(&raft_listen_addr)
                        .expect("failed to bind raft listener");
                    tracing::info!(addr = %raft_listen_addr, "raft peer listener");
                    let transport =
                        PeerTransport::new(listener, peers, raft_tx, raft_eventfd, outbound_rx);
                    transport.run().await;
                });
            })
            .expect("failed to spawn peer transport thread");

        peer_handles.push(handle);
    }

    // Wait for all threads
    for handle in acceptor_handles {
        handle.join().expect("acceptor thread panicked");
    }
    for handle in peer_handles {
        handle.join().expect("peer transport thread panicked");
    }
    for handle in shard_handles {
        handle.join().expect("shard thread panicked");
    }
}
