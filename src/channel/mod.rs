use nix::libc;
use protobuf::Message as PbMessage;
use raft::eraftpb::Message;

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

/// Message from peer transport -> shard raft state machine.
pub struct RaftInbound {
    pub group_id: u16,
    pub message: Message,
}

/// Message from shard raft state machine -> peer transport.
pub struct RaftOutbound {
    pub group_id: u16,
    pub target_id: u64,
    pub message: Message,
}

/// A mailbox for one shard, consisting of crossbeam channels + eventfds.
pub struct ShardMailbox {
    pub request_tx: crossbeam_channel::Sender<ShardRequest>,
    pub request_rx: crossbeam_channel::Receiver<ShardRequest>,
    pub response_tx: crossbeam_channel::Sender<ShardResponse>,
    pub response_rx: crossbeam_channel::Receiver<ShardResponse>,
    pub raft_rx: crossbeam_channel::Receiver<RaftInbound>,
    pub raft_tx: crossbeam_channel::Sender<RaftInbound>,
    pub raft_outbound_rx: crossbeam_channel::Receiver<RaftOutbound>,
    pub raft_outbound_tx: crossbeam_channel::Sender<RaftOutbound>,
    /// eventfd for waking the shard's io_uring loop when requests arrive
    pub request_eventfd: i32,
    /// eventfd for waking the acceptor's io_uring loop when responses arrive
    pub response_eventfd: i32,
    /// eventfd for waking raft processing when peer messages arrive
    pub raft_eventfd: i32,
}

impl ShardMailbox {
    pub fn new(capacity: usize) -> std::io::Result<Self> {
        let (request_tx, request_rx) = crossbeam_channel::bounded(capacity);
        let (response_tx, response_rx) = crossbeam_channel::bounded(capacity);
        let (raft_tx, raft_rx) = crossbeam_channel::bounded(capacity * 4);
        let (raft_outbound_tx, raft_outbound_rx) = crossbeam_channel::bounded(capacity * 4);

        // Create eventfds for cross-thread wakeup
        let request_eventfd = create_eventfd()?;
        let response_eventfd = create_eventfd()?;
        let raft_eventfd = create_eventfd()?;

        Ok(Self {
            request_tx,
            request_rx,
            response_tx,
            response_rx,
            raft_rx,
            raft_tx,
            raft_outbound_rx,
            raft_outbound_tx,
            request_eventfd,
            response_eventfd,
            raft_eventfd,
        })
    }
}

impl Drop for ShardMailbox {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.request_eventfd);
            libc::close(self.response_eventfd);
            libc::close(self.raft_eventfd);
        }
    }
}

pub fn encode_raft_message(
    group_id: u16,
    message: &Message,
) -> Result<Vec<u8>, protobuf::ProtobufError> {
    let body = message.write_to_bytes()?;
    let mut frame = Vec::with_capacity(2 + 4 + body.len());
    frame.extend_from_slice(&group_id.to_be_bytes());
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

pub fn try_decode_raft_message(
    buf: &[u8],
) -> Result<Option<(u16, Message, usize)>, protobuf::ProtobufError> {
    if buf.len() < 6 {
        return Ok(None);
    }
    let group_id = u16::from_be_bytes(buf[0..2].try_into().expect("slice length"));
    let len = u32::from_be_bytes(buf[2..6].try_into().expect("slice length")) as usize;
    if buf.len() < 6 + len {
        return Ok(None);
    }
    let msg = Message::parse_from_bytes(&buf[6..6 + len])?;
    Ok(Some((group_id, msg, 6 + len)))
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
