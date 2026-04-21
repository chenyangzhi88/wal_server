use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::Parser;
use wal_server::protocol::codec::{append_request, decode_response, encode_request, read_request};
use wal_server::protocol::types::{Request, Response, Status};
use wal_server::protocol::wire::RESPONSE_HEADER_SIZE;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "cluster_bench",
    about = "Primary writer + reader fanout benchmark with long-lived client connections"
)]
struct Cli {
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "127.0.0.1:7101,127.0.0.1:7102,127.0.0.1:7103"
    )]
    addrs: Vec<String>,

    #[arg(long, default_value_t = 7)]
    stream_id: u64,

    #[arg(long, default_value_t = 1)]
    epoch: u64,

    #[arg(long, default_value_t = 1024)]
    payload_bytes: usize,

    #[arg(long, default_value_t = 10000)]
    writes: u64,

    #[arg(long, default_value_t = 0)]
    duration_secs: u64,

    #[arg(long, default_value_t = 3)]
    readers: usize,

    #[arg(long, default_value_t = 0)]
    think_us: u64,

    #[arg(long, default_value_t = 1000)]
    report_ms: u64,
}

struct BenchConnection {
    addr: String,
    stream: Option<TcpStream>,
}

impl BenchConnection {
    fn new(addr: String) -> Self {
        Self { addr, stream: None }
    }

    fn set_addr(&mut self, addr: String) {
        if self.addr != addr {
            self.addr = addr;
            self.stream = None;
        }
    }

    fn send(&mut self, req: Request) -> std::io::Result<Response> {
        if self.stream.is_none() {
            self.stream = Some(connect(&self.addr)?);
        }

        let result = send_on_stream(self.stream.as_mut().expect("stream present"), req);
        if result.is_err() {
            self.stream = None;
        }
        result
    }
}

fn main() {
    let cli = Cli::parse();
    assert!(!cli.addrs.is_empty(), "at least one address is required");
    assert!(cli.readers > 0, "at least one reader is required");
    assert!(
        cli.duration_secs > 0 || cli.writes > 0,
        "either --duration-secs or --writes must be greater than zero"
    );

    let payload = Bytes::from(vec![b'v'; cli.payload_bytes]);
    let writer_done = Arc::new(AtomicBool::new(false));
    let highest_written = Arc::new(AtomicU64::new(0));
    let highest_read = Arc::new(
        (0..cli.readers)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );
    let write_errors = Arc::new(AtomicU64::new(0));
    let read_errors = Arc::new(
        (0..cli.readers)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );

    let start = Instant::now();
    let writer_deadline = if cli.duration_secs > 0 {
        Some(start + Duration::from_secs(cli.duration_secs))
    } else {
        None
    };

    let writer = {
        let addrs = cli.addrs.clone();
        let done = writer_done.clone();
        let highest_written = highest_written.clone();
        let write_errors = write_errors.clone();
        let payload = payload.clone();
        let stream_id = cli.stream_id;
        let epoch = cli.epoch;
        let writes = cli.writes;
        let think = Duration::from_micros(cli.think_us);
        thread::spawn(move || {
            let mut leader_hint = 0usize;
            let mut conn = BenchConnection::new(addrs[leader_hint].clone());
            let mut issued = 0u64;

            loop {
                if let Some(deadline) = writer_deadline {
                    if Instant::now() >= deadline {
                        break;
                    }
                } else if issued >= writes {
                    break;
                }

                let addr = addrs[leader_hint % addrs.len()].clone();
                conn.set_addr(addr);
                let req = append_request(stream_id, epoch, payload.clone());
                match conn.send(req) {
                    Ok(resp) if resp.status == Status::Ok => {
                        issued += 1;
                        highest_written.store(resp.offset + 1, Ordering::Release);
                        if think > Duration::ZERO {
                            thread::sleep(think);
                        }
                    }
                    Ok(resp) if resp.status == Status::ErrNotLeader => {
                        leader_hint = discover_leader(&addrs, stream_id).unwrap_or(leader_hint + 1);
                        conn.stream = None;
                        thread::sleep(Duration::from_millis(5));
                    }
                    Ok(_) | Err(_) => {
                        write_errors.fetch_add(1, Ordering::Relaxed);
                        leader_hint = discover_leader(&addrs, stream_id).unwrap_or(leader_hint + 1);
                        conn.stream = None;
                        thread::sleep(Duration::from_millis(20));
                    }
                }
            }
            done.store(true, Ordering::Release);
        })
    };

    let mut reader_threads = Vec::with_capacity(cli.readers);
    for reader_id in 0..cli.readers {
        let addrs = cli.addrs.clone();
        let done = writer_done.clone();
        let highest_written = highest_written.clone();
        let highest_read = highest_read.clone();
        let read_errors = read_errors.clone();
        let stream_id = cli.stream_id;
        let epoch = cli.epoch;
        reader_threads.push(thread::spawn(move || {
            let mut next_lsn = 0u64;
            let mut leader_hint = reader_id % addrs.len();
            let mut conn = BenchConnection::new(addrs[leader_hint].clone());

            loop {
                let available = highest_written.load(Ordering::Acquire);
                if done.load(Ordering::Acquire) && next_lsn >= available {
                    break;
                }
                if next_lsn >= available {
                    thread::sleep(Duration::from_millis(2));
                    continue;
                }

                let addr = addrs[leader_hint % addrs.len()].clone();
                conn.set_addr(addr);
                match conn.send(read_request(stream_id, epoch, next_lsn)) {
                    Ok(resp) if resp.status == Status::Ok => {
                        highest_read[reader_id].store(resp.offset + 1, Ordering::Release);
                        next_lsn = resp.offset + 1;
                    }
                    Ok(resp) if resp.status == Status::ErrNotLeader => {
                        leader_hint = discover_leader(&addrs, stream_id).unwrap_or(leader_hint + 1);
                        conn.stream = None;
                        thread::sleep(Duration::from_millis(5));
                    }
                    Ok(resp) if resp.status == Status::ErrNotFound => {
                        let visible = highest_written.load(Ordering::Acquire);
                        if next_lsn >= visible {
                            thread::sleep(Duration::from_millis(2));
                        } else {
                            read_errors[reader_id].fetch_add(1, Ordering::Relaxed);
                            conn.stream = None;
                            thread::sleep(Duration::from_millis(5));
                        }
                    }
                    Ok(_) | Err(_) => {
                        read_errors[reader_id].fetch_add(1, Ordering::Relaxed);
                        leader_hint = discover_leader(&addrs, stream_id).unwrap_or(leader_hint + 1);
                        conn.stream = None;
                        thread::sleep(Duration::from_millis(20));
                    }
                }
            }
        }));
    }

    let reporter = {
        let done = writer_done.clone();
        let highest_written = highest_written.clone();
        let highest_read = highest_read.clone();
        let write_errors = write_errors.clone();
        let read_errors = read_errors.clone();
        let report_interval = Duration::from_millis(cli.report_ms);
        thread::spawn(move || loop {
            thread::sleep(report_interval);
            let writes = highest_written.load(Ordering::Acquire);
            let reads = highest_read
                .iter()
                .map(|v| v.load(Ordering::Acquire))
                .collect::<Vec<_>>();
            let min_read = reads.iter().copied().min().unwrap_or(0);
            let max_lag = writes.saturating_sub(min_read);
            let elapsed = start.elapsed().as_secs_f64().max(1e-9);
            let read_errors_total: u64 =
                read_errors.iter().map(|v| v.load(Ordering::Relaxed)).sum();
            println!(
                    "progress elapsed={:.1}s writes={} write_qps={:.0} min_read={} max_lag={} per_reader={:?} write_errors={} read_errors={}",
                    elapsed,
                    writes,
                    writes as f64 / elapsed,
                    min_read,
                    max_lag,
                    reads,
                    write_errors.load(Ordering::Relaxed),
                    read_errors_total
                );
            if done.load(Ordering::Acquire) && max_lag == 0 {
                break;
            }
        })
    };

    writer.join().expect("writer thread panicked");
    for handle in reader_threads {
        handle.join().expect("reader thread panicked");
    }
    reporter.join().expect("reporter thread panicked");

    let elapsed = start.elapsed();
    let writes = highest_written.load(Ordering::Acquire);
    let reads = highest_read
        .iter()
        .map(|v| v.load(Ordering::Acquire))
        .collect::<Vec<_>>();
    let min_read = reads.iter().copied().min().unwrap_or(0);
    let total_bytes = writes as usize * cli.payload_bytes;
    let mib = total_bytes as f64 / (1024.0 * 1024.0);
    let read_errors_total: u64 = read_errors.iter().map(|v| v.load(Ordering::Relaxed)).sum();
    println!(
        "result elapsed={:.3}s writes={} readers={} payload={}B write_qps={:.0} write_mib_s={:.2} min_read={} max_lag={} per_reader={:?} write_errors={} read_errors={}",
        elapsed.as_secs_f64(),
        writes,
        cli.readers,
        cli.payload_bytes,
        writes as f64 / elapsed.as_secs_f64().max(1e-9),
        mib / elapsed.as_secs_f64().max(1e-9),
        min_read,
        writes.saturating_sub(min_read),
        reads,
        write_errors.load(Ordering::Relaxed),
        read_errors_total
    );
}

fn discover_leader(addrs: &[String], stream_id: u64) -> Option<usize> {
    for (idx, addr) in addrs.iter().enumerate() {
        let mut conn = BenchConnection::new(addr.clone());
        let Ok(resp) = conn.send(read_request(stream_id, 1, 0)) else {
            continue;
        };
        if resp.status != Status::ErrNotLeader {
            return Some(idx);
        }
    }
    None
}

fn connect(addr: &str) -> std::io::Result<TcpStream> {
    let stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

fn send_on_stream(stream: &mut TcpStream, req: Request) -> std::io::Result<Response> {
    let frame = encode_request(&req);
    stream.write_all(&frame)?;

    let mut header = [0u8; RESPONSE_HEADER_SIZE];
    stream.read_exact(&mut header)?;
    let payload_len = u32::from_be_bytes(header[22..26].try_into().expect("slice")) as usize;
    let mut frame = header.to_vec();
    if payload_len > 0 {
        let mut payload = vec![0u8; payload_len];
        stream.read_exact(&mut payload)?;
        frame.extend_from_slice(&payload);
    }

    Ok(decode_response(&frame).expect("decode response"))
}
