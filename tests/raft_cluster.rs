use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use wal_server::protocol::codec::{
    append_request, decode_response, decode_stream_status_payload, encode_request,
    get_status_request, read_request,
};
use wal_server::protocol::types::{Response, Status, StreamStatusPayload};
use wal_server::protocol::wire::RESPONSE_HEADER_SIZE;
use wal_server::shard::router::ShardRouter;

fn unique_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("wal_server_{name}_{nanos}"));
    fs::create_dir_all(&path).unwrap();
    path
}

fn unique_base_port() -> u16 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    20000 + (nanos % 20000) as u16
}

fn write_config(
    path: &Path,
    node_id: u64,
    listen_addr: &str,
    raft_listen_addr: &str,
    data_dir: &Path,
    peers: &[(u64, &str)],
    num_shards: usize,
) {
    let peers_toml = peers
        .iter()
        .map(|(id, addr)| format!("{{ id = {id}, addr = \"{addr}\" }}"))
        .collect::<Vec<_>>()
        .join(", ");
    let toml = format!(
        "node_id = {node_id}\nlisten_addr = \"{listen_addr}\"\nraft_listen_addr = \"{raft_listen_addr}\"\ndata_dir = \"{}\"\nnum_shards = {num_shards}\nnuma_aware = false\npeers = [{}]\n",
        data_dir.display(),
        peers_toml
    );
    fs::write(path, toml).unwrap();
}

fn spawn_node(config: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_wal_server"))
        .arg("--config")
        .arg(config)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn wal_server")
}

fn send(addr: &str, req: wal_server::protocol::types::Request) -> std::io::Result<Response> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
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

fn wait_for_append(addrs: &[String], payload: &'static [u8]) -> (usize, Response) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = Vec::new();
    loop {
        last.clear();
        for (idx, addr) in addrs.iter().enumerate() {
            match send(addr, append_request(7, 1, Bytes::from_static(payload))) {
                Ok(resp) if resp.status == Status::Ok => return (idx, resp),
                Ok(resp) => last.push(format!("{addr}: {:?}/leader={}", resp.status, resp.offset)),
                Err(err) => last.push(format!("{addr}: {err}")),
            }
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for leader append: {}", last.join(", "));
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn wait_for_append_stream(addrs: &[String], stream_id: u64, payload: Bytes) -> (usize, Response) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = Vec::new();
    loop {
        last.clear();
        for (idx, addr) in addrs.iter().enumerate() {
            match send(addr, append_request(stream_id, 1, payload.clone())) {
                Ok(resp) if resp.status == Status::Ok => return (idx, resp),
                Ok(resp) => last.push(format!("{addr}: {:?}/leader={}", resp.status, resp.offset)),
                Err(err) => last.push(format!("{addr}: {err}")),
            }
        }
        if Instant::now() >= deadline {
            panic!(
                "timed out waiting for leader append on stream {stream_id}: {}",
                last.join(", ")
            );
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn wait_for_read(addr: &str, offset: u64) -> Response {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match send(addr, read_request(7, 1, offset)) {
            Ok(resp) if resp.status == Status::Ok => return resp,
            Ok(_) | Err(_) => {}
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for read offset {offset}");
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn wait_for_status(addr: &str, min_commit_stream_lsn: u64) -> StreamStatusPayload {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match send(addr, get_status_request(7)) {
            Ok(resp) if resp.status == Status::Ok && !resp.payload.is_empty() => {
                let status = decode_stream_status_payload(&resp.payload).expect("decode status");
                if status.commit_stream_lsn >= min_commit_stream_lsn {
                    return status;
                }
            }
            Ok(_) | Err(_) => {}
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for status >= {min_commit_stream_lsn} from {addr}");
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn append_to_addr(addr: &str, payload: Bytes) -> Response {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match send(addr, append_request(7, 1, payload.clone())) {
            Ok(resp) if resp.status == Status::Ok => return resp,
            Ok(_) | Err(_) => {}
        }
        if Instant::now() >= deadline {
            panic!("timed out appending via {addr}");
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn append_stream_to_addr(addr: &str, stream_id: u64, payload: Bytes) -> Response {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match send(addr, append_request(stream_id, 1, payload.clone())) {
            Ok(resp) if resp.status == Status::Ok => return resp,
            Ok(_) | Err(_) => {}
        }
        if Instant::now() >= deadline {
            panic!("timed out appending stream {stream_id} via {addr}");
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn wait_for_read_stream(addr: &str, stream_id: u64, offset: u64) -> Response {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match send(addr, read_request(stream_id, 1, offset)) {
            Ok(resp) if resp.status == Status::Ok => return resp,
            Ok(_) | Err(_) => {}
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for stream {stream_id} read offset {offset}");
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn wait_for_status_stream(
    addr: &str,
    stream_id: u64,
    min_commit_stream_lsn: u64,
) -> StreamStatusPayload {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match send(addr, get_status_request(stream_id)) {
            Ok(resp) if resp.status == Status::Ok && !resp.payload.is_empty() => {
                let status = decode_stream_status_payload(&resp.payload).expect("decode status");
                if status.commit_stream_lsn >= min_commit_stream_lsn {
                    return status;
                }
            }
            Ok(_) | Err(_) => {}
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for status >= {min_commit_stream_lsn} for stream {stream_id} from {addr}");
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn streams_for_distinct_groups(num_shards: u16) -> (u64, u64) {
    let router = ShardRouter::new(num_shards);
    let first_stream = 7u64;
    let first_group = router.route_stream(first_stream);
    for candidate in 8u64..10_000 {
        if router.route_stream(candidate) != first_group {
            return (first_stream, candidate);
        }
    }
    panic!("failed to find streams for distinct groups");
}

#[test]
fn three_node_cluster_election_replication_and_failover() {
    let root = unique_dir("cluster");
    let base = unique_base_port();
    let peers = vec![
        (1u64, format!("127.0.0.1:{}", base + 100)),
        (2u64, format!("127.0.0.1:{}", base + 101)),
        (3u64, format!("127.0.0.1:{}", base + 102)),
    ];
    let client_addrs = vec![
        format!("127.0.0.1:{}", base),
        format!("127.0.0.1:{}", base + 1),
        format!("127.0.0.1:{}", base + 2),
    ];

    let mut children = Vec::new();
    for i in 0..3usize {
        let node_dir = root.join(format!("node{}", i + 1));
        fs::create_dir_all(&node_dir).unwrap();
        let config = node_dir.join("config.toml");
        write_config(
            &config,
            (i + 1) as u64,
            &client_addrs[i],
            &peers[i].1,
            &node_dir.join("data"),
            &peers
                .iter()
                .map(|(id, addr)| (*id, addr.as_str()))
                .collect::<Vec<_>>(),
            1,
        );
        children.push(spawn_node(&config));
    }

    thread::sleep(Duration::from_secs(3));

    let (leader_idx, first_resp) = wait_for_append(&client_addrs, b"first");
    assert_eq!(first_resp.offset, 0);
    let read0 = wait_for_read(&client_addrs[leader_idx], 0);
    assert_eq!(read0.payload.as_ref(), b"first");

    children[leader_idx].kill().ok();
    children[leader_idx].wait().ok();

    let survivor_addrs = client_addrs
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != leader_idx)
        .map(|(_, addr)| addr.clone())
        .collect::<Vec<_>>();
    let (new_leader_rel_idx, second_resp) = wait_for_append(&survivor_addrs, b"second");
    assert_eq!(second_resp.offset, 1);
    let new_leader_addr = &survivor_addrs[new_leader_rel_idx];

    let read_old = wait_for_read(new_leader_addr, 0);
    assert_eq!(read_old.payload.as_ref(), b"first");
    let read_new = wait_for_read(new_leader_addr, 1);
    assert_eq!(read_new.payload.as_ref(), b"second");

    for (idx, child) in children.iter_mut().enumerate() {
        if idx != leader_idx {
            child.kill().ok();
            child.wait().ok();
        }
    }
    let _ = fs::remove_dir_all(root);
}

#[test]
fn restarted_follower_recovers_snapshot_state_and_cluster_keeps_serving() {
    let root = unique_dir("snapshot_restart");
    let base = unique_base_port();
    let peers = vec![
        (1u64, format!("127.0.0.1:{}", base + 100)),
        (2u64, format!("127.0.0.1:{}", base + 101)),
        (3u64, format!("127.0.0.1:{}", base + 102)),
    ];
    let client_addrs = vec![
        format!("127.0.0.1:{}", base),
        format!("127.0.0.1:{}", base + 1),
        format!("127.0.0.1:{}", base + 2),
    ];

    let mut config_paths = Vec::new();
    let mut children = Vec::new();
    for i in 0..3usize {
        let node_dir = root.join(format!("node{}", i + 1));
        fs::create_dir_all(&node_dir).unwrap();
        let config = node_dir.join("config.toml");
        write_config(
            &config,
            (i + 1) as u64,
            &client_addrs[i],
            &peers[i].1,
            &node_dir.join("data"),
            &peers
                .iter()
                .map(|(id, addr)| (*id, addr.as_str()))
                .collect::<Vec<_>>(),
            1,
        );
        config_paths.push(config.clone());
        children.push(spawn_node(&config));
    }

    thread::sleep(Duration::from_secs(3));

    let (leader_idx, first_resp) = wait_for_append(&client_addrs, b"seed-0");
    assert_eq!(first_resp.offset, 0);
    let leader_addr = client_addrs[leader_idx].clone();
    for i in 1..80u64 {
        let payload = Bytes::from(format!("seed-{i}"));
        let resp = append_to_addr(&leader_addr, payload);
        assert_eq!(resp.offset, i);
    }

    let restart_idx = (0..3usize).find(|idx| *idx != leader_idx).unwrap();
    let restart_addr = client_addrs[restart_idx].clone();
    let status_before_restart = wait_for_status(&restart_addr, 80);
    assert!(status_before_restart.commit_index >= 80);
    assert_eq!(status_before_restart.next_stream_lsn, 80);

    children[restart_idx].kill().ok();
    children[restart_idx].wait().ok();
    thread::sleep(Duration::from_secs(1));
    children[restart_idx] = spawn_node(&config_paths[restart_idx]);

    let recovered_status = wait_for_status(&restart_addr, 80);
    assert_eq!(recovered_status.next_stream_lsn, 80);
    assert_eq!(recovered_status.commit_stream_lsn, 80);

    let survivor_addrs = client_addrs
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != leader_idx)
        .map(|(_, addr)| addr.clone())
        .collect::<Vec<_>>();
    children[leader_idx].kill().ok();
    children[leader_idx].wait().ok();

    let (new_leader_idx, after_restart) = wait_for_append(&survivor_addrs, b"after-restart");
    assert_eq!(after_restart.offset, 80);
    let new_leader_addr = &survivor_addrs[new_leader_idx];

    let restarted_status = wait_for_status(&restart_addr, 81);
    assert_eq!(restarted_status.next_stream_lsn, 81);
    assert_eq!(restarted_status.commit_stream_lsn, 81);

    let read_old = wait_for_read(new_leader_addr, 0);
    assert_eq!(read_old.payload.as_ref(), b"seed-0");
    let read_new = wait_for_read(new_leader_addr, 80);
    assert_eq!(read_new.payload.as_ref(), b"after-restart");

    for (idx, child) in children.iter_mut().enumerate() {
        if idx != leader_idx {
            child.kill().ok();
            child.wait().ok();
        }
    }
    let _ = fs::remove_dir_all(root);
}

#[test]
fn two_raft_groups_replicate_independently_across_the_same_cluster() {
    let root = unique_dir("multi_group");
    let base = unique_base_port();
    let peers = vec![
        (1u64, format!("127.0.0.1:{}", base + 100)),
        (2u64, format!("127.0.0.1:{}", base + 101)),
        (3u64, format!("127.0.0.1:{}", base + 102)),
    ];
    let client_addrs = vec![
        format!("127.0.0.1:{}", base),
        format!("127.0.0.1:{}", base + 1),
        format!("127.0.0.1:{}", base + 2),
    ];
    let num_shards = 2usize;
    let (stream_a, stream_b) = streams_for_distinct_groups(num_shards as u16);

    let mut children = Vec::new();
    for i in 0..3usize {
        let node_dir = root.join(format!("node{}", i + 1));
        fs::create_dir_all(&node_dir).unwrap();
        let config = node_dir.join("config.toml");
        write_config(
            &config,
            (i + 1) as u64,
            &client_addrs[i],
            &peers[i].1,
            &node_dir.join("data"),
            &peers
                .iter()
                .map(|(id, addr)| (*id, addr.as_str()))
                .collect::<Vec<_>>(),
            num_shards,
        );
        children.push(spawn_node(&config));
    }

    thread::sleep(Duration::from_secs(4));

    let (leader_a_idx, append_a0) =
        wait_for_append_stream(&client_addrs, stream_a, Bytes::from_static(b"group-a-0"));
    assert_eq!(append_a0.offset, 0);
    let leader_a_addr = &client_addrs[leader_a_idx];

    let (leader_b_idx, append_b0) =
        wait_for_append_stream(&client_addrs, stream_b, Bytes::from_static(b"group-b-0"));
    assert_eq!(append_b0.offset, 0);
    let leader_b_addr = &client_addrs[leader_b_idx];

    let append_a1 =
        append_stream_to_addr(leader_a_addr, stream_a, Bytes::from_static(b"group-a-1"));
    assert_eq!(append_a1.offset, 1);
    let append_b1 =
        append_stream_to_addr(leader_b_addr, stream_b, Bytes::from_static(b"group-b-1"));
    assert_eq!(append_b1.offset, 1);

    for addr in &client_addrs {
        let status_a = wait_for_status_stream(addr, stream_a, 2);
        assert_eq!(status_a.next_stream_lsn, 2);
        let status_b = wait_for_status_stream(addr, stream_b, 2);
        assert_eq!(status_b.next_stream_lsn, 2);
    }

    let read_a0 = wait_for_read_stream(leader_a_addr, stream_a, 0);
    assert_eq!(read_a0.payload.as_ref(), b"group-a-0");
    let read_a1 = wait_for_read_stream(leader_a_addr, stream_a, 1);
    assert_eq!(read_a1.payload.as_ref(), b"group-a-1");
    let read_b0 = wait_for_read_stream(leader_b_addr, stream_b, 0);
    assert_eq!(read_b0.payload.as_ref(), b"group-b-0");
    let read_b1 = wait_for_read_stream(leader_b_addr, stream_b, 1);
    assert_eq!(read_b1.payload.as_ref(), b"group-b-1");

    for child in &mut children {
        child.kill().ok();
        child.wait().ok();
    }
    let _ = fs::remove_dir_all(root);
}
