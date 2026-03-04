#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TAOSD_BIN="${1:-$REPO_ROOT/debug/build/bin/taosd}"
TSDB_FORCE_SCRIPT="$SCRIPT_DIR/repair_tsdb_force.sh"
META_FORCE_SCRIPT="$SCRIPT_DIR/repair_meta_force.sh"

if [[ ! -x "$TAOSD_BIN" ]]; then
  echo "taosd binary not found or not executable: $TAOSD_BIN"
  echo "usage: $0 [path/to/taosd]"
  exit 1
fi

if [[ ! -x "$TSDB_FORCE_SCRIPT" || ! -x "$META_FORCE_SCRIPT" ]]; then
  echo "required force-mode scripts are missing or not executable"
  exit 1
fi

TMP_ROOT="$(mktemp -d /tmp/td-repair-mode-matrix-XXXXXX)"
cleanup() {
  rm -rf "$TMP_ROOT"
}
trap cleanup EXIT

run_replica_case() {
  local case_root="$TMP_ROOT/replica"
  local data_dir="$case_root/data"
  local backup_dir="$case_root/backup"
  local log_dir="$case_root/taoslog"
  local run_log="$case_root/repair-run.log"
  mkdir -p "$data_dir/vnode/vnode2/wal" "$backup_dir" "$log_dir"
  printf 'replica-evidence\n' > "$data_dir/vnode/vnode2/wal/000001.log"

  local rc=0
  set +e
  if command -v timeout >/dev/null 2>&1; then
    TAOS_DATA_DIR="$data_dir" ASAN_OPTIONS=detect_leaks=0 timeout 30s \
      "$TAOSD_BIN" -o "$log_dir" -r --node-type vnode --file-type wal --vnode-id 2 --mode replica \
      --backup-path "$backup_dir" >"$run_log" 2>&1
    rc=$?
  else
    TAOS_DATA_DIR="$data_dir" ASAN_OPTIONS=detect_leaks=0 \
      "$TAOSD_BIN" -o "$log_dir" -r --node-type vnode --file-type wal --vnode-id 2 --mode replica \
      --backup-path "$backup_dir" >"$run_log" 2>&1
    rc=$?
  fi
  set -e

  if [[ $rc -eq 124 ]]; then
    echo "[replica] repair workflow timed out"
    cat "$run_log"
    return 1
  fi

  if ! grep -Eq "repair progress: .*step=replica .*vnode=1/1 .*progress=100%" "$run_log"; then
    echo "[replica] missing progress output"
    cat "$run_log"
    return 1
  fi

  if ! grep -Eq "repair summary: .*status=success .*successVnodes=1 .*failedVnodes=0" "$run_log"; then
    echo "[replica] missing successful summary output"
    cat "$run_log"
    return 1
  fi

  local session_dir
  session_dir="$(find "$backup_dir" -maxdepth 1 -mindepth 1 -type d -name 'repair-*' | head -n 1)"
  if [[ -z "$session_dir" || ! -f "$session_dir/repair.log" ]]; then
    echo "[replica] repair session log not found"
    find "$backup_dir" -maxdepth 3 -print
    return 1
  fi

  if ! grep -q "replica dispatch detail" "$session_dir/repair.log"; then
    echo "[replica] dispatch detail log missing"
    cat "$session_dir/repair.log"
    return 1
  fi
  if ! grep -q "replica restore detail" "$session_dir/repair.log"; then
    echo "[replica] restore detail log missing"
    cat "$session_dir/repair.log"
    return 1
  fi

  echo "[replica] ok (taosd exit code: $rc)"
}

run_copy_case() {
  local case_root="$TMP_ROOT/copy"
  local local_data_dir="$case_root/local-data"
  local remote_data_dir="$case_root/remote-data"
  local backup_dir="$case_root/backup"
  local log_dir="$case_root/taoslog"
  local bin_dir="$case_root/mock-bin"
  local run_log="$case_root/repair-run.log"
  mkdir -p "$local_data_dir/vnode/vnode2/wal" "$remote_data_dir/vnode/vnode2/wal/meta" \
           "$backup_dir" "$log_dir" "$bin_dir"
  printf 'local-stale\n' > "$local_data_dir/vnode/vnode2/wal/stale.log"
  printf 'remote-wal\n' > "$remote_data_dir/vnode/vnode2/wal/000001.log"
  printf 'remote-meta\n' > "$remote_data_dir/vnode/vnode2/wal/meta/checkpoint"

  local ssh_mock="$bin_dir/ssh-mock"
  local scp_mock="$bin_dir/scp-mock"

  cat > "$ssh_mock" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cmd="${@: -1}"
bash -c "$cmd"
EOF

  cat > "$scp_mock" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
src="${@: -2:1}"
dst="${@: -1}"
remote="${src#*:}"
mkdir -p "$dst"
cp -r "$remote/." "$dst"
chmod -R 755 "$dst"
EOF

  chmod +x "$ssh_mock" "$scp_mock"

  local rc=0
  set +e
  if command -v timeout >/dev/null 2>&1; then
    TAOS_DATA_DIR="$local_data_dir" TAOS_REPAIR_SSH_BIN="$ssh_mock" TAOS_REPAIR_SCP_BIN="$scp_mock" \
      ASAN_OPTIONS=detect_leaks=0 timeout 30s "$TAOSD_BIN" -o "$log_dir" -r --node-type vnode --file-type wal \
      --vnode-id 2 --mode copy --replica-node "tdnode1:$remote_data_dir" --backup-path "$backup_dir" \
      >"$run_log" 2>&1
    rc=$?
  else
    TAOS_DATA_DIR="$local_data_dir" TAOS_REPAIR_SSH_BIN="$ssh_mock" TAOS_REPAIR_SCP_BIN="$scp_mock" \
      ASAN_OPTIONS=detect_leaks=0 "$TAOSD_BIN" -o "$log_dir" -r --node-type vnode --file-type wal \
      --vnode-id 2 --mode copy --replica-node "tdnode1:$remote_data_dir" --backup-path "$backup_dir" \
      >"$run_log" 2>&1
    rc=$?
  fi
  set -e

  if [[ $rc -eq 124 ]]; then
    echo "[copy] repair workflow timed out"
    cat "$run_log"
    return 1
  fi

  if ! grep -Eq "repair progress: .*step=copy .*vnode=1/1 .*progress=100%" "$run_log"; then
    echo "[copy] missing progress output"
    cat "$run_log"
    return 1
  fi

  if ! grep -Eq "repair summary: .*status=success .*successVnodes=1 .*failedVnodes=0" "$run_log"; then
    echo "[copy] missing successful summary output"
    cat "$run_log"
    return 1
  fi

  if [[ -e "$local_data_dir/vnode/vnode2/wal/stale.log" ]]; then
    echo "[copy] stale local file should be removed after successful copy"
    find "$local_data_dir/vnode/vnode2/wal" -maxdepth 3 -type f | sort
    return 1
  fi

  if [[ ! -f "$local_data_dir/vnode/vnode2/wal/000001.log" || \
        ! -f "$local_data_dir/vnode/vnode2/wal/meta/checkpoint" ]]; then
    echo "[copy] copied files are incomplete"
    find "$local_data_dir/vnode/vnode2/wal" -maxdepth 3 -type f | sort
    return 1
  fi

  local session_dir
  session_dir="$(find "$backup_dir" -maxdepth 1 -mindepth 1 -type d -name 'repair-*' | head -n 1)"
  if [[ -z "$session_dir" || ! -f "$session_dir/repair.log" ]]; then
    echo "[copy] repair session log not found"
    find "$backup_dir" -maxdepth 3 -print
    return 1
  fi

  if ! grep -q "copy replica detail" "$session_dir/repair.log"; then
    echo "[copy] copy detail log missing"
    cat "$session_dir/repair.log"
    return 1
  fi

  if ! grep -q "consistency=verified" "$session_dir/repair.log"; then
    echo "[copy] consistency verified marker missing"
    cat "$session_dir/repair.log"
    return 1
  fi

  echo "[copy] ok (taosd exit code: $rc)"
}

echo "[matrix] force(tsdb) begin"
bash "$TSDB_FORCE_SCRIPT" "$TAOSD_BIN"

echo "[matrix] force(meta) begin"
bash "$META_FORCE_SCRIPT" "$TAOSD_BIN"

echo "[matrix] replica begin"
run_replica_case

echo "[matrix] copy begin"
run_copy_case

echo "repair mode matrix script passed"
