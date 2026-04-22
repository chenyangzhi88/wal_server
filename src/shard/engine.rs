use nix::libc;
use raft::eraftpb::EntryType;
use raft::{Config as RaftConfig, RawNode, StateRole};
use slog::o;
use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use crate::channel::{
    drain_eventfd, notify_eventfd, RaftInbound, RaftOutbound, ShardRequest, ShardResponse,
};
use crate::config::ServerConfig;
use crate::protocol::codec::encode_stream_status_payload;
use crate::protocol::types::{OpCode, Response, Status, StreamStatusPayload};
use crate::raft::command::{decode_append_command, encode_append_command, AppendCommand};
use crate::raft::storage::PersistentStorage;
use crate::shard::stream_state::{EpochFenceError, StreamStateTable};
use crate::wal::index::StreamLogIndex;
use crate::wal::reader::WalReader;
use crate::wal::segment::{list_segments, remove_segment_file};
use crate::wal::writer::WalWriter;

struct PendingClientWrite {
    connection_id: u64,
    stream_id: u64,
    epoch: u64,
    stream_lsn: u64,
}

pub struct ShardEngine {
    shard_id: u16,
    node_id: u64,
    wal_writer: WalWriter,
    wal_reader: WalReader,
    index: StreamLogIndex,
    streams: StreamStateTable,
    raft: RawNode<PersistentStorage>,
    request_seq: u64,
    pending_writes: HashMap<u64, PendingClientWrite>,
    bootstrapped_campaign: bool,
    started_at: Instant,
    last_tick_at: Instant,
    last_gc_at: Instant,
    observed_role: StateRole,
    observed_leader_id: u64,
    config: ServerConfig,
    request_rx: crossbeam_channel::Receiver<ShardRequest>,
    raft_rx: crossbeam_channel::Receiver<RaftInbound>,
    response_tx: crossbeam_channel::Sender<ShardResponse>,
    raft_outbound_tx: crossbeam_channel::Sender<RaftOutbound>,
    request_eventfd: i32,
    raft_eventfd: i32,
    response_eventfd: i32,
}

impl ShardEngine {
    pub async fn open(
        shard_id: u16,
        config: ServerConfig,
        request_rx: crossbeam_channel::Receiver<ShardRequest>,
        raft_rx: crossbeam_channel::Receiver<RaftInbound>,
        response_tx: crossbeam_channel::Sender<ShardResponse>,
        raft_outbound_tx: crossbeam_channel::Sender<RaftOutbound>,
        request_eventfd: i32,
        raft_eventfd: i32,
        response_eventfd: i32,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let shard_dir = Path::new(&config.data_dir).join(format!("shard_{:04}", shard_id));
        let mut conf_state = raft::eraftpb::ConfState::default();
        conf_state.voters = config.peers.iter().map(|peer| peer.id).collect();
        let storage = PersistentStorage::open(&shard_dir.join("raft"), conf_state)?;
        let snapshot = storage.snapshot();
        let snapshot_index = snapshot.get_metadata().index;
        let recovered_streams =
            StreamStateTable::decode_snapshot(config.tail_cache_entries, snapshot.get_data())?;
        let (wal_writer, index, streams) = WalWriter::open(
            shard_id,
            &shard_dir,
            config.segment_max_bytes,
            config.tail_cache_entries,
            Some(recovered_streams),
            snapshot_index,
        )
        .await?;
        let wal_reader = WalReader::new(shard_id, &shard_dir);
        let raft_cfg = RaftConfig {
            id: config.node_id,
            election_tick: 100,
            heartbeat_tick: 10,
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: 256,
            check_quorum: false,
            pre_vote: false,
            ..Default::default()
        };
        let logger = slog::Logger::root(slog::Discard, o!());
        let raft = RawNode::new(&raft_cfg, storage, &logger)?;

        Ok(Self {
            shard_id,
            node_id: config.node_id,
            wal_writer,
            wal_reader,
            index,
            streams,
            raft,
            request_seq: 0,
            pending_writes: HashMap::new(),
            bootstrapped_campaign: false,
            started_at: Instant::now(),
            last_tick_at: Instant::now(),
            last_gc_at: Instant::now(),
            observed_role: StateRole::Follower,
            observed_leader_id: 0,
            config,
            request_rx,
            raft_rx,
            response_tx,
            raft_outbound_tx,
            request_eventfd,
            raft_eventfd,
            response_eventfd,
        })
    }

    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!(
            shard_id = self.shard_id,
            node_id = self.node_id,
            "raft shard engine started"
        );

        loop {
            let request_fut = async { wait_eventfd_readable(self.request_eventfd).await };
            let raft_fut = async { wait_eventfd_readable(self.raft_eventfd).await };
            let timer_fut = monoio::time::sleep(Duration::from_millis(10));

            monoio::select! {
                _ = request_fut => {
                    drain_eventfd(self.request_eventfd);
                    self.process_client_requests().await?;
                }
                _ = raft_fut => {
                    drain_eventfd(self.raft_eventfd);
                    self.process_raft_messages()?;
                }
                _ = timer_fut => {}
            }

            if self.last_tick_at.elapsed() >= Duration::from_millis(100) {
                self.raft.tick();
                self.last_tick_at = Instant::now();
            }

            if !self.bootstrapped_campaign
                && self.node_id == 1
                && self.started_at.elapsed() >= Duration::from_millis(1500)
            {
                self.raft.campaign()?;
                self.bootstrapped_campaign = true;
            }

            self.drive_ready().await?;
            self.log_raft_state_if_changed();

            if self.last_gc_at.elapsed() >= Duration::from_micros(self.config.gc_interval_us) {
                self.run_gc()?;
                self.last_gc_at = Instant::now();
            }
        }
    }

    async fn process_client_requests(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        while let Ok(req) = self.request_rx.try_recv() {
            self.handle_client_request(req).await?;
        }
        Ok(())
    }

    fn process_raft_messages(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        while let Ok(inbound) = self.raft_rx.try_recv() {
            self.raft.step(inbound.message)?;
        }
        Ok(())
    }

    async fn handle_client_request(
        &mut self,
        req: ShardRequest,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match req.request.op {
            OpCode::Write => {
                if self.raft.raft.state != StateRole::Leader {
                    return self.send_not_leader(req.connection_id);
                }

                let stream_lsn = match self
                    .streams
                    .allocate_append(req.request.stream_id, req.request.epoch)
                {
                    Ok(stream_lsn) => stream_lsn,
                    Err(EpochFenceError::Stale { current_epoch, .. }) => {
                        return self.send_response(
                            req.connection_id,
                            Status::ErrEpochFenced,
                            current_epoch,
                            0,
                            bytes::Bytes::new(),
                        );
                    }
                };

                self.request_seq += 1;
                let request_id = self.request_seq;
                let cmd = AppendCommand {
                    request_id,
                    stream_id: req.request.stream_id,
                    epoch: req.request.epoch,
                    stream_lsn,
                    payload: req.request.payload,
                };
                self.pending_writes.insert(
                    request_id,
                    PendingClientWrite {
                        connection_id: req.connection_id,
                        stream_id: cmd.stream_id,
                        epoch: cmd.epoch,
                        stream_lsn: cmd.stream_lsn,
                    },
                );
                self.raft.propose(vec![], encode_append_command(&cmd))?;
            }
            OpCode::Read => {
                if self.raft.raft.state != StateRole::Leader {
                    return self.send_not_leader(req.connection_id);
                }
                self.read_stream(req.connection_id, req.request.stream_id, req.request.offset)
                    .await?;
            }
            OpCode::Ack => {
                let consumed = self
                    .streams
                    .advance_consumed(req.request.stream_id, req.request.offset);
                self.send_response(
                    req.connection_id,
                    Status::Ok,
                    self.streams.current_epoch(req.request.stream_id),
                    consumed,
                    bytes::Bytes::new(),
                )?;
            }
            OpCode::GetStatus => {
                let (epoch, offset, payload) =
                    if let Some(status) = self.streams.status(req.request.stream_id) {
                        let meta = StreamStatusPayload {
                            next_stream_lsn: status.next_stream_lsn,
                            commit_stream_lsn: status.commit_stream_lsn,
                            consumed_stream_lsn: status.consumed_stream_lsn,
                            commit_index: self.raft.raft.raft_log.committed,
                            last_applied: self.raft.raft.raft_log.applied,
                        };
                        (
                            status.max_epoch,
                            status.next_stream_lsn,
                            encode_stream_status_payload(&meta),
                        )
                    } else {
                        (0, 0, bytes::Bytes::new())
                    };
                self.send_response(req.connection_id, Status::Ok, epoch, offset, payload)?;
            }
        }
        Ok(())
    }

    async fn read_stream(
        &mut self,
        connection_id: u64,
        stream_id: u64,
        stream_lsn: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(status) = self.streams.status(stream_id) {
            if stream_lsn >= status.commit_stream_lsn {
                return self.send_response(
                    connection_id,
                    Status::ErrNotFound,
                    status.max_epoch,
                    stream_lsn,
                    bytes::Bytes::new(),
                );
            }
        }

        if let Some(tail) = self.streams.read_from_tail(stream_id, stream_lsn) {
            return self.send_response(
                connection_id,
                Status::Ok,
                self.streams.current_epoch(stream_id),
                tail.stream_lsn,
                tail.payload,
            );
        }

        match self
            .wal_reader
            .read_record(stream_id, stream_lsn, &self.index)
            .await
        {
            Ok(record) => self.send_response(
                connection_id,
                Status::Ok,
                record.epoch,
                record.stream_lsn,
                record.payload,
            ),
            Err(_) => self.send_response(
                connection_id,
                Status::ErrNotFound,
                self.streams.current_epoch(stream_id),
                stream_lsn,
                bytes::Bytes::new(),
            ),
        }
    }

    async fn drive_ready(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.raft.has_ready() {
            return Ok(());
        }

        let mut ready = self.raft.ready();
        let committed_entries = ready.take_committed_entries();
        if let Some(hs) = ready.hs() {
            self.raft.mut_store().set_hardstate(hs.clone())?;
        }
        if !ready.snapshot().is_empty() {
            self.raft
                .mut_store()
                .apply_snapshot(ready.snapshot().clone())?;
        }
        if !ready.entries().is_empty() {
            self.raft.mut_store().append(ready.entries())?;
        }

        self.dispatch_raft_messages(ready.take_messages()).await?;
        self.dispatch_raft_messages(ready.take_persisted_messages())
            .await?;

        for entry in committed_entries {
            if entry.get_data().is_empty() {
                continue;
            }
            if entry.get_entry_type() != EntryType::EntryNormal {
                continue;
            }
            let cmd = decode_append_command(entry.get_data())?;
            self.apply_append_entry(entry.get_index(), entry.get_term(), cmd)
                .await?;
        }

        let light_ready = self.raft.advance(ready);
        self.dispatch_raft_messages(light_ready.messages().to_vec())
            .await?;
        Ok(())
    }

    async fn dispatch_raft_messages(
        &mut self,
        messages: Vec<raft::eraftpb::Message>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for msg in messages {
            if msg.to == self.node_id {
                self.raft.step(msg)?;
            } else {
                let mut outbound = RaftOutbound {
                    group_id: self.shard_id,
                    target_id: msg.to,
                    message: msg,
                };
                loop {
                    match self.raft_outbound_tx.try_send(outbound) {
                        Ok(()) => break,
                        Err(crossbeam_channel::TrySendError::Full(returned)) => {
                            outbound = returned;
                            tracing::warn!(
                                node_id = self.node_id,
                                target_id = outbound.target_id,
                                "raft outbound queue full, retrying"
                            );
                            monoio::time::sleep(Duration::from_millis(1)).await;
                        }
                        Err(crossbeam_channel::TrySendError::Disconnected(returned)) => {
                            return Err(format!(
                                "raft outbound queue disconnected for target {}",
                                returned.target_id
                            )
                            .into());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn apply_append_entry(
        &mut self,
        raft_index: u64,
        raft_term: u64,
        cmd: AppendCommand,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_, loc) = self
            .wal_writer
            .append_with_log_index(
                cmd.stream_id,
                cmd.epoch,
                cmd.stream_lsn,
                raft_index,
                raft_term,
                &cmd.payload,
            )
            .await?;
        self.wal_writer.sync().await?;

        self.index.insert(cmd.stream_id, cmd.stream_lsn, loc);
        self.streams.mark_appended(
            cmd.stream_id,
            cmd.epoch,
            raft_index,
            cmd.stream_lsn,
            cmd.payload.clone(),
        );
        self.streams
            .mark_committed(cmd.stream_id, raft_index, cmd.stream_lsn);

        if let Some(pending) = self.pending_writes.remove(&cmd.request_id) {
            self.send_response(
                pending.connection_id,
                Status::Ok,
                pending.epoch,
                pending.stream_lsn,
                bytes::Bytes::new(),
            )?;
        }
        self.maybe_snapshot_raft_state()?;
        notify_eventfd(self.response_eventfd);
        Ok(())
    }

    fn maybe_snapshot_raft_state(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let applied = self.raft.raft.raft_log.applied;
        let snap_index = self.raft.store().snapshot_index();
        const SNAPSHOT_INTERVAL: u64 = 64;
        if applied >= snap_index + SNAPSHOT_INTERVAL {
            self.raft
                .mut_store()
                .create_snapshot(applied, self.streams.encode_snapshot())?;
            self.raft.mut_store().compact(applied + 1)?;
        }
        Ok(())
    }

    fn run_gc(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        for status in self.streams.all_statuses() {
            if status.consumed_stream_lsn > 0 {
                self.index
                    .truncate_stream_before(status.stream_id, status.consumed_stream_lsn);
            }
        }

        let live_segments = self.index.live_segment_ids();
        let current_segment_id = self.wal_writer.current_segment_id();
        for (segment_id, path) in list_segments(self.wal_writer.current_data_dir(), self.shard_id)?
        {
            if segment_id == current_segment_id {
                continue;
            }
            if !live_segments.contains(&segment_id) {
                self.wal_reader.evict_segment(segment_id);
                remove_segment_file(&path)?;
            }
        }
        Ok(())
    }

    fn send_not_leader(&self, connection_id: u64) -> Result<(), Box<dyn std::error::Error>> {
        self.send_response(
            connection_id,
            Status::ErrNotLeader,
            0,
            self.raft.raft.leader_id,
            bytes::Bytes::new(),
        )
    }

    fn log_raft_state_if_changed(&mut self) {
        let role = self.raft.raft.state;
        let leader_id = self.raft.raft.leader_id;
        if role != self.observed_role || leader_id != self.observed_leader_id {
            if self.observed_role == StateRole::Leader && role != StateRole::Leader {
                self.fail_pending_writes_not_leader();
            }
            tracing::info!(
                node_id = self.node_id,
                ?role,
                leader_id,
                term = self.raft.raft.term,
                committed = self.raft.raft.raft_log.committed,
                applied = self.raft.raft.raft_log.applied,
                "raft state changed"
            );
            self.observed_role = role;
            self.observed_leader_id = leader_id;
        }
    }

    fn fail_pending_writes_not_leader(&mut self) {
        if self.pending_writes.is_empty() {
            return;
        }

        let mut pending = self
            .pending_writes
            .drain()
            .map(|(_, pending)| pending)
            .collect::<Vec<_>>();
        pending.sort_by(|a, b| {
            a.stream_id
                .cmp(&b.stream_id)
                .then_with(|| b.stream_lsn.cmp(&a.stream_lsn))
        });

        let leader_id = self.raft.raft.leader_id;
        for pending in pending {
            self.streams
                .rollback_uncommitted_append(pending.stream_id, pending.stream_lsn);
            let _ = self.send_response(
                pending.connection_id,
                Status::ErrNotLeader,
                pending.epoch,
                leader_id,
                bytes::Bytes::new(),
            );
        }
    }

    fn send_response(
        &self,
        connection_id: u64,
        status: Status,
        epoch: u64,
        offset: u64,
        payload: bytes::Bytes,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let _ = self.response_tx.try_send(ShardResponse {
            connection_id,
            response: Response {
                status,
                epoch,
                offset,
                payload,
            },
        });
        notify_eventfd(self.response_eventfd);
        Ok(())
    }
}

async fn wait_eventfd_readable(fd: i32) {
    loop {
        let mut val: u64 = 0;
        let ret = unsafe { libc::read(fd, &mut val as *mut u64 as *mut libc::c_void, 8) };
        if ret > 0 {
            return;
        }
        monoio::time::sleep(Duration::from_micros(50)).await;
    }
}
