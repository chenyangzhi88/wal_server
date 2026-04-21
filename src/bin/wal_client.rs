use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Instant;

use bytes::Bytes;
use wal_server::protocol::codec::{
    ack_request, append_request, decode_response, decode_stream_status_payload, encode_request,
    get_status_request, read_request,
};
use wal_server::protocol::types::{Response, Status};
use wal_server::protocol::wire::RESPONSE_HEADER_SIZE;

fn main() {
    let args: Vec<String> = env::args().collect();
    let scenario = args.get(1).map(String::as_str).unwrap_or("append");
    let addr = args.get(2).map(String::as_str).unwrap_or("127.0.0.1:9876");

    match scenario {
        "append" => run_append(
            addr,
            parse_u64(args.get(3), 7),
            parse_u64(args.get(4), 1),
            parse_usize(args.get(5), 1024),
            parse_usize(args.get(6), 10_000),
        ),
        "read" => run_read(
            addr,
            parse_u64(args.get(3), 7),
            parse_u64(args.get(4), 1),
            parse_u64(args.get(5), 0),
        ),
        "ack" => run_ack(
            addr,
            parse_u64(args.get(3), 7),
            parse_u64(args.get(4), 1),
            parse_u64(args.get(5), 0),
        ),
        "status" => run_status(addr, parse_u64(args.get(3), 7)),
        other => {
            eprintln!(
                "usage:\n  wal_client append [addr] [stream_id] [epoch] [payload_bytes] [requests]\n  wal_client read [addr] [stream_id] [epoch] [stream_lsn]\n  wal_client ack [addr] [stream_id] [epoch] [consumed_stream_lsn]\n  wal_client status [addr] [stream_id]\nunknown scenario: {other}"
            );
            std::process::exit(2);
        }
    }
}

fn parse_u64(arg: Option<&String>, default: u64) -> u64 {
    arg.and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn parse_usize(arg: Option<&String>, default: usize) -> usize {
    arg.and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn send_request(stream: &mut TcpStream, req: wal_server::protocol::types::Request) -> Response {
    let frame = encode_request(&req);
    stream.write_all(&frame).expect("write request");

    let mut header = [0u8; RESPONSE_HEADER_SIZE];
    stream
        .read_exact(&mut header)
        .expect("read response header");
    let payload_len = u32::from_be_bytes(header[22..26].try_into().expect("payload len")) as usize;
    let mut frame = header.to_vec();
    if payload_len > 0 {
        let mut payload = vec![0u8; payload_len];
        stream
            .read_exact(&mut payload)
            .expect("read response payload");
        frame.extend_from_slice(&payload);
    }
    decode_response(&frame).expect("decode response")
}

fn run_append(addr: &str, stream_id: u64, epoch: u64, payload_bytes: usize, requests: usize) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    let payload = Bytes::from(vec![b'v'; payload_bytes]);
    let start = Instant::now();
    let mut last_offset = 0;

    for _ in 0..requests {
        let resp = send_request(
            &mut stream,
            append_request(stream_id, epoch, payload.clone()),
        );
        assert_eq!(resp.status, Status::Ok, "append failed: {:?}", resp.status);
        last_offset = resp.offset;
    }

    let elapsed = start.elapsed();
    println!(
        "append: addr={} stream_id={} epoch={} requests={} payload={}B last_offset={} elapsed={:.3}s req/s={:.0}",
        addr,
        stream_id,
        epoch,
        requests,
        payload_bytes,
        last_offset,
        elapsed.as_secs_f64(),
        requests as f64 / elapsed.as_secs_f64().max(1e-9)
    );
}

fn run_read(addr: &str, stream_id: u64, epoch: u64, stream_lsn: u64) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    let resp = send_request(&mut stream, read_request(stream_id, epoch, stream_lsn));
    println!(
        "read: addr={} stream_id={} epoch={} offset={} status={:?} payload={}B",
        addr,
        stream_id,
        epoch,
        resp.offset,
        resp.status,
        resp.payload.len()
    );
}

fn run_ack(addr: &str, stream_id: u64, epoch: u64, consumed_stream_lsn: u64) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    let resp = send_request(
        &mut stream,
        ack_request(stream_id, epoch, consumed_stream_lsn),
    );
    println!(
        "ack: addr={} stream_id={} epoch={} consumed={} status={:?}",
        addr, stream_id, epoch, resp.offset, resp.status
    );
}

fn run_status(addr: &str, stream_id: u64) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    let resp = send_request(&mut stream, get_status_request(stream_id));
    println!(
        "status: addr={} stream_id={} status={:?} epoch={} offset={}",
        addr, stream_id, resp.status, resp.epoch, resp.offset
    );
    if !resp.payload.is_empty() {
        let decoded = decode_stream_status_payload(&resp.payload).expect("decode status payload");
        println!(
            "  next_stream_lsn={} commit_stream_lsn={} consumed_stream_lsn={} commit_index={} last_applied={}",
            decoded.next_stream_lsn,
            decoded.commit_stream_lsn,
            decoded.consumed_stream_lsn,
            decoded.commit_index,
            decoded.last_applied
        );
    }
}
