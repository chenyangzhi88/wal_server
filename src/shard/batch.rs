use std::time::Instant;

use crate::wal::writer::PendingWrite;

/// Group commit batcher: accumulates pending writes and flushes
/// when the batch is full or the timer expires.
pub struct GroupCommitBatcher {
    pending: Vec<PendingWrite>,
    max_batch_size: usize,
    first_pending_at: Option<Instant>,
}

impl GroupCommitBatcher {
    pub fn new(max_batch_size: usize) -> Self {
        Self {
            pending: Vec::with_capacity(max_batch_size),
            max_batch_size,
            first_pending_at: None,
        }
    }

    /// Add a pending write.
    pub fn push(&mut self, pw: PendingWrite) {
        if self.pending.is_empty() {
            self.first_pending_at = Some(Instant::now());
        }
        self.pending.push(pw);
    }

    /// Returns true if the batch should be flushed now (batch full).
    pub fn is_batch_full(&self) -> bool {
        self.pending.len() >= self.max_batch_size
    }

    /// Returns true if there are pending writes.
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Drain all pending writes, resetting the batch.
    pub fn drain(&mut self) -> Vec<PendingWrite> {
        self.first_pending_at = None;
        std::mem::take(&mut self.pending)
    }

    pub fn should_flush(&self, max_delay: std::time::Duration) -> bool {
        self.is_batch_full()
            || self
                .first_pending_at
                .is_some_and(|started| started.elapsed() >= max_delay)
    }
}
