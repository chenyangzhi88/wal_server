#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  cat <<EOF
Usage: $(basename "$0") [run_dir]

Environment:
  BASE_PORT       Base client port, default 7101
  RAFT_BASE_PORT  Base raft port, default 7201
  RUST_LOG        Log level for wal_server, default info

Example:
  BASE_PORT=7101 RAFT_BASE_PORT=7201 $(basename "$0") ./.run/cluster
EOF
  exit 0
fi

RUN_DIR="${1:-$ROOT_DIR/.run/cluster}"
BASE_PORT="${BASE_PORT:-7101}"
RAFT_BASE_PORT="${RAFT_BASE_PORT:-7201}"
RUST_LOG_VALUE="${RUST_LOG:-info}"

mkdir -p "$RUN_DIR"
mkdir -p "$RUN_DIR/bin" "$RUN_DIR/configs" "$RUN_DIR/logs" "$RUN_DIR/data"

echo "building wal_server binary"
cargo build --release --bin wal_server --manifest-path "$ROOT_DIR/Cargo.toml"

BIN="$ROOT_DIR/target/release/wal_server"
PID_FILE="$RUN_DIR/pids.txt"
: > "$PID_FILE"

peer_list() {
  cat <<EOF
{ id = 1, addr = "127.0.0.1:${RAFT_BASE_PORT}" },
{ id = 2, addr = "127.0.0.1:$((RAFT_BASE_PORT + 1))" },
{ id = 3, addr = "127.0.0.1:$((RAFT_BASE_PORT + 2))" }
EOF
}

write_config() {
  local node_id="$1"
  local client_port="$2"
  local raft_port="$3"
  local config_path="$4"
  local data_dir="$5"
  cat > "$config_path" <<EOF
node_id = ${node_id}
listen_addr = "127.0.0.1:${client_port}"
raft_listen_addr = "127.0.0.1:${raft_port}"
data_dir = "${data_dir}"
num_shards = 1
numa_aware = false
peers = [
$(peer_list)
]
EOF
}

for i in 0 1 2; do
  node_id=$((i + 1))
  client_port=$((BASE_PORT + i))
  raft_port=$((RAFT_BASE_PORT + i))
  node_dir="$RUN_DIR/data/node${node_id}"
  config_path="$RUN_DIR/configs/node${node_id}.toml"
  log_path="$RUN_DIR/logs/node${node_id}.log"

  mkdir -p "$node_dir"
  write_config "$node_id" "$client_port" "$raft_port" "$config_path" "$node_dir"

  echo "starting node ${node_id}: client=127.0.0.1:${client_port} raft=127.0.0.1:${raft_port}"
  RUST_LOG="$RUST_LOG_VALUE" "$BIN" --config "$config_path" >"$log_path" 2>&1 &
  pid=$!
  echo "${node_id} ${pid} ${config_path} ${log_path}" >> "$PID_FILE"
done

cat <<EOF
cluster started
run_dir: $RUN_DIR
pid_file: $PID_FILE
client_addrs:
  127.0.0.1:${BASE_PORT}
  127.0.0.1:$((BASE_PORT + 1))
  127.0.0.1:$((BASE_PORT + 2))

example write/read checks:
  cargo run --release --bin wal_client -- append 127.0.0.1:${BASE_PORT} 7 1 1024 100
  cargo run --release --bin wal_client -- status 127.0.0.1:${BASE_PORT} 7

example benchmark:
  cargo run --release --bin cluster_bench -- --addrs 127.0.0.1:${BASE_PORT},127.0.0.1:$((BASE_PORT + 1)),127.0.0.1:$((BASE_PORT + 2)) --writes 10000 --payload-bytes 1024 --readers 3

stop:
  awk '{print \$2}' "$PID_FILE" | xargs -r kill
EOF
