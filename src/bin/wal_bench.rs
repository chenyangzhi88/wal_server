use std::env;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use wal_server::wal::reader::WalReader;
use wal_server::wal::writer::WalWriter;

fn main() {
    let args: Vec<String> = env::args().collect();
    let scenario = args.get(1).map(String::as_str).unwrap_or("append-sync");
    let records = parse_usize(args.get(2), 200_000);
    let payload_size = parse_usize(args.get(3), 1024);
    let segment_bytes = parse_u64(args.get(4), 64 * 1024 * 1024);

    match scenario {
        "append-sync" => run_append_sync(records, payload_size, segment_bytes),
        "recovery" => run_recovery(records, payload_size, segment_bytes),
        "point-read" => run_point_read(records, payload_size, segment_bytes),
        other => {
            eprintln!(
                "unknown scenario: {other}\nusage: cargo run --release --bin wal_bench -- [append-sync|recovery|point-read] [records] [payload_size] [segment_bytes]"
            );
            std::process::exit(2);
        }
    }
}

fn parse_usize(arg: Option<&String>, default: usize) -> usize {
    arg.and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn parse_u64(arg: Option<&String>, default: u64) -> u64 {
    arg.and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("{prefix}_{unique}"));
    std::fs::create_dir_all(&path).expect("temp dir");
    path
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
}

fn format_rate(bytes: usize, elapsed: Duration) -> String {
    let seconds = elapsed.as_secs_f64().max(1e-9);
    let mib = bytes as f64 / (1024.0 * 1024.0);
    format!("{:.2} MiB/s", mib / seconds)
}

fn populate(dir: &Path, records: usize, payload_size: usize, segment_bytes: u64) {
    let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
        .enable_timer()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let (mut writer, _index, _streams) = WalWriter::open(0, dir, segment_bytes, 32, None, 0)
            .await
            .expect("open writer");
        let value = vec![b'v'; payload_size];
        let stream_id = 7;

        for stream_lsn in 0..records as u64 {
            writer
                .append(stream_id, 1, stream_lsn, &value)
                .await
                .expect("append");
        }
        writer.sync().await.expect("sync");
    });
}

fn run_append_sync(records: usize, payload_size: usize, segment_bytes: u64) {
    let root = unique_temp_dir("wal_bench_append");
    let shard_dir = root.join("shard_0000");
    let value = vec![b'v'; payload_size];
    let bytes_written = records * value.len();

    let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
        .enable_timer()
        .build()
        .expect("runtime");
    let start = Instant::now();
    rt.block_on(async {
        let (mut writer, _index, _streams) =
            WalWriter::open(0, &shard_dir, segment_bytes, 32, None, 0)
                .await
                .expect("open writer");
        let stream_id = 7;
        for stream_lsn in 0..records as u64 {
            writer
                .append(stream_id, 1, stream_lsn, &value)
                .await
                .expect("append");
        }
        writer.sync().await.expect("sync");
        println!(
            "append-sync: records={} payload={}B last_entry_id={} segment_id={}",
            records,
            payload_size,
            writer.next_entry_id().saturating_sub(1),
            writer.current_segment_id()
        );
    });
    let elapsed = start.elapsed();
    println!(
        "elapsed={:.3}s throughput={} records/s={:.0}",
        elapsed.as_secs_f64(),
        format_rate(bytes_written, elapsed),
        records as f64 / elapsed.as_secs_f64().max(1e-9)
    );
    cleanup(&root);
}

fn run_recovery(records: usize, payload_size: usize, segment_bytes: u64) {
    let root = unique_temp_dir("wal_bench_recovery");
    let shard_dir = root.join("shard_0000");
    populate(&shard_dir, records, payload_size, segment_bytes);

    let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
        .enable_timer()
        .build()
        .expect("runtime");
    let start = Instant::now();
    let (writer, index, streams) = rt
        .block_on(async { WalWriter::open(0, &shard_dir, segment_bytes, 32, None, 0).await })
        .expect("recover");
    let elapsed = start.elapsed();

    println!(
        "recovery: records={} payload={}B recovered_index={} streams={} next_entry_id={}",
        records,
        payload_size,
        index.len(),
        streams.len(),
        writer.next_entry_id()
    );
    println!(
        "elapsed={:.3}s records/s={:.0}",
        elapsed.as_secs_f64(),
        records as f64 / elapsed.as_secs_f64().max(1e-9)
    );

    cleanup(&root);
}

fn run_point_read(records: usize, payload_size: usize, segment_bytes: u64) {
    let root = unique_temp_dir("wal_bench_read");
    let shard_dir = root.join("shard_0000");
    populate(&shard_dir, records, payload_size, segment_bytes);

    let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
        .enable_timer()
        .build()
        .expect("runtime");
    let (_writer, index, _streams) = rt
        .block_on(async { WalWriter::open(0, &shard_dir, segment_bytes, 32, None, 0).await })
        .expect("recover");
    let mut reader = WalReader::new(0, &shard_dir);
    let stream_id = 7;
    let target = (records / 2) as u64;

    let start = Instant::now();
    let record = rt
        .block_on(async { reader.read_record(stream_id, target, &index).await })
        .expect("point read");
    let elapsed = start.elapsed();

    println!(
        "point-read: target={} payload={}B stream_id={} stream_lsn={} entry_id={}",
        target,
        record.payload.len(),
        record.stream_id,
        record.stream_lsn,
        record.entry_id
    );
    println!("latency_us={:.2}", elapsed.as_secs_f64() * 1_000_000.0);

    cleanup(&root);
}
