use nix::libc;
use std::path::Path;
use std::time::Duration;

use crate::channel::{drain_eventfd, notify_eventfd, ShardRequest, ShardResponse};
use crate::config::ServerConfig;
use crate::protocol::types::{OpCode, Response, Status};
use crate::shard::batch::GroupCommitBatcher;
use crate::shard::stream_state::{derive_stream_id, StreamStateTable};
use crate::wal::index::LsnIndex;
use crate::wal::reader::WalReader;
use crate::wal::writer::{PendingWrite, WalWriter};

/// The core shard engine. Runs on a single thread with its own monoio runtime.
/// Owns the WAL writer/reader, index, and communicates via crossbeam channels.
pub struct ShardEngine {
    shard_id: u16,
    wal_writer: WalWriter,
    wal_reader: WalReader,
    index: LsnIndex,
    streams: StreamStateTable,
    batcher: GroupCommitBatcher,
    config: ServerConfig,
    // Channel endpoints
    request_rx: crossbeam_channel::Receiver<ShardRequest>,
    response_tx: crossbeam_channel::Sender<ShardResponse>,
    request_eventfd: i32,
    response_eventfd: i32,
}

impl ShardEngine {
    pub async fn open(
        shard_id: u16,
        config: ServerConfig,
        request_rx: crossbeam_channel::Receiver<ShardRequest>,
        response_tx: crossbeam_channel::Sender<ShardResponse>,
        request_eventfd: i32,
        response_eventfd: i32,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let shard_dir = Path::new(&config.data_dir).join(format!("shard_{:04}", shard_id));
        let (wal_writer, index, streams) =
            WalWriter::open(shard_id, &shard_dir, config.segment_max_bytes).await?;
        let wal_reader = WalReader::new(shard_id, &shard_dir);
        let batcher = GroupCommitBatcher::new(config.group_commit_max_batch);

        Ok(Self {
            shard_id,
            wal_writer,
            wal_reader,
            index,
            streams,
            batcher,
            config,
            request_rx,
            response_tx,
            request_eventfd,
            response_eventfd,
        })
    }

    /// Main event loop. Uses monoio::time::sleep racing against eventfd reads.
    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error>> {
        let flush_interval = Duration::from_micros(self.config.group_commit_interval_us);

        tracing::info!(shard_id = self.shard_id, "shard engine started");

        loop {
            // Wait for either: eventfd notification (new requests) or flush timer
            let eventfd_fut = async {
                // Use monoio's async read on the raw fd via a helper
                // We wrap the eventfd in an AsyncReadRent-compatible type
                wait_eventfd_readable(self.request_eventfd).await;
            };

            let timer_fut = monoio::time::sleep(flush_interval);

            // Race the two futures
            monoio::select! {
                _ = eventfd_fut => {
                    drain_eventfd(self.request_eventfd);
                    self.process_pending_requests().await?;
                }
                _ = timer_fut => {
                    // Timer expired — flush if anything pending
                }
            }

            if self.batcher.should_flush(flush_interval) {
                self.flush_batch().await?;
            }
        }
    }

    async fn process_pending_requests(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Drain all available requests from the channel
        while let Ok(shard_req) = self.request_rx.try_recv() {
            self.handle_request(shard_req).await?;
            if self.batcher.is_batch_full() {
                self.flush_batch().await?;
            }
        }
        Ok(())
    }

    async fn handle_request(
        &mut self,
        req: ShardRequest,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match req.request.op {
            OpCode::Write => {
                let stream_id = derive_stream_id(&req.request.key);
                let (term, stream_lsn) = self.streams.allocate_append(stream_id);
                let (entry_id, loc) = self
                    .wal_writer
                    .append(stream_id, term, stream_lsn, &req.request.key, &req.request.payload)
                    .await?;
                self.index.insert(entry_id, loc);
                self.streams
                    .mark_appended(stream_id, entry_id, stream_lsn, term);
                self.batcher.push(PendingWrite {
                    connection_id: req.connection_id,
                    stream_id,
                    entry_id,
                    stream_lsn,
                });
            }
            OpCode::Read => {
                // Parse LSN from key (key contains the LSN as big-endian u64)
                let lsn = if req.request.key.len() >= 8 {
                    u64::from_be_bytes(req.request.key[..8].try_into().unwrap())
                } else {
                    return self.send_response(req.connection_id, Status::ErrInvalidRequest, 0);
                };

                match self.wal_reader.read_by_lsn(lsn, &self.index).await {
                    Ok(_record) => {
                        // For now, just ack with the LSN. Full record read would need
                        // a richer response protocol.
                        self.send_response(req.connection_id, Status::Ok, lsn)?;
                    }
                    Err(_) => {
                        self.send_response(req.connection_id, Status::ErrNotFound, lsn)?;
                    }
                }
            }
            OpCode::Sync => {
                self.wal_writer.sync().await?;
                self.send_response(req.connection_id, Status::Ok, self.wal_writer.next_entry_id())?;
            }
            OpCode::GetStatus => {
                self.send_response(req.connection_id, Status::Ok, self.wal_writer.next_entry_id())?;
            }
        }
        Ok(())
    }

    async fn flush_batch(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.wal_writer.sync().await?;
        let pending = self.batcher.drain();
        for pw in pending {
            self.streams
                .mark_committed(pw.stream_id, pw.entry_id, pw.stream_lsn);
            let _ = self.response_tx.try_send(ShardResponse {
                connection_id: pw.connection_id,
                response: Response {
                    status: Status::Ok,
                    lsn: pw.entry_id,
                },
            });
        }
        // Wake the acceptor
        notify_eventfd(self.response_eventfd);
        Ok(())
    }

    fn send_response(
        &self,
        connection_id: u64,
        status: Status,
        lsn: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let _ = self.response_tx.try_send(ShardResponse {
            connection_id,
            response: Response { status, lsn },
        });
        notify_eventfd(self.response_eventfd);
        Ok(())
    }
}

/// Wait until an eventfd is readable using monoio's io_uring polling.
async fn wait_eventfd_readable(fd: i32) {
    // Create a tokio-compatible async fd wrapper
    // monoio supports polling raw fds via its reactor
    // We use a simple sleep-and-poll approach as a fallback
    // In production, you'd use monoio's IoUringDrive poll_ready
    //
    // For now, we use a short sleep + check pattern.
    // This is a simplification; the proper approach uses monoio's
    // async fd registration, which we'll optimize in Phase 5.
    loop {
        let mut val: u64 = 0;
        let ret = unsafe { libc::read(fd, &mut val as *mut u64 as *mut libc::c_void, 8) };
        if ret > 0 {
            return;
        }
        // Not ready yet, yield briefly
        monoio::time::sleep(Duration::from_micros(50)).await;
    }
}
