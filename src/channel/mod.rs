use nix::libc;

use crate::protocol::types::{Request, Response};

/// Message from acceptor → shard.
pub struct ShardRequest {
    pub connection_id: u64,
    pub request: Request,
}

/// Message from shard → acceptor.
pub struct ShardResponse {
    pub connection_id: u64,
    pub response: Response,
}

/// A mailbox for one shard, consisting of crossbeam channels + eventfds.
pub struct ShardMailbox {
    pub request_tx: crossbeam_channel::Sender<ShardRequest>,
    pub request_rx: crossbeam_channel::Receiver<ShardRequest>,
    pub response_tx: crossbeam_channel::Sender<ShardResponse>,
    pub response_rx: crossbeam_channel::Receiver<ShardResponse>,
    /// eventfd for waking the shard's io_uring loop when requests arrive
    pub request_eventfd: i32,
    /// eventfd for waking the acceptor's io_uring loop when responses arrive
    pub response_eventfd: i32,
}

impl ShardMailbox {
    pub fn new(capacity: usize) -> std::io::Result<Self> {
        let (request_tx, request_rx) = crossbeam_channel::bounded(capacity);
        let (response_tx, response_rx) = crossbeam_channel::bounded(capacity);

        // Create eventfds for cross-thread wakeup
        let request_eventfd = create_eventfd()?;
        let response_eventfd = create_eventfd()?;

        Ok(Self {
            request_tx,
            request_rx,
            response_tx,
            response_rx,
            request_eventfd,
            response_eventfd,
        })
    }
}

impl Drop for ShardMailbox {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.request_eventfd);
            libc::close(self.response_eventfd);
        }
    }
}

fn create_eventfd() -> std::io::Result<i32> {
    let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(fd)
}

/// Write to an eventfd to signal the other side.
pub fn notify_eventfd(fd: i32) {
    let val: u64 = 1;
    unsafe {
        libc::write(fd, &val as *const u64 as *const libc::c_void, 8);
    }
}

/// Read (consume) from an eventfd. Non-blocking.
pub fn drain_eventfd(fd: i32) {
    let mut val: u64 = 0;
    unsafe {
        libc::read(fd, &mut val as *mut u64 as *mut libc::c_void, 8);
    }
}
