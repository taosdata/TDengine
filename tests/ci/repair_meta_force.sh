#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TAOSD_BIN="${1:-$REPO_ROOT/debug/build/bin/taosd}"

if [[ ! -x "$TAOSD_BIN" ]]; then
  echo "taosd binary not found or not executable: $TAOSD_BIN"
  echo "usage: $0 [path/to/taosd]"
  exit 1
fi

TMP_ROOT="$(mktemp -d /tmp/td-repair-meta-force-XXXXXX)"
cleanup() {
  rm -rf "$TMP_ROOT"
}
trap cleanup EXIT

run_meta_case() {
  local case_name="$1"
  local case_mode="$2"

  local case_root="$TMP_ROOT/$case_name"
  local data_dir="$case_root/data"
  local backup_dir="$case_root/backup"
  local log_dir="$case_root/taoslog"
  local run_log="$case_root/repair-run.log"

  mkdir -p "$data_dir/vnode/vnode2/meta" "$data_dir/vnode/vnode2/wal" "$backup_dir" "$log_dir"
  : > "$data_dir/vnode/vnode2/wal/000001.log"

  if [[ "$case_mode" == "partial" ]]; then
    : > "$data_dir/vnode/vnode2/meta/table.db"
    : > "$data_dir/vnode/vnode2/meta/tag.idx"
  fi

  set +e
  if command -v timeout >/dev/null 2>&1; then
    TAOS_DATA_DIR="$data_dir" ASAN_OPTIONS=detect_leaks=0 timeout 30s \
      "$TAOSD_BIN" -o "$log_dir" -r --node-type vnode --file-type meta --vnode-id 2 --mode force \
      --backup-path "$backup_dir" >"$run_log" 2>&1
    rc=$?
  else
    TAOS_DATA_DIR="$data_dir" ASAN_OPTIONS=detect_leaks=0 \
      "$TAOSD_BIN" -o "$log_dir" -r --node-type vnode --file-type meta --vnode-id 2 --mode force \
      --backup-path "$backup_dir" >"$run_log" 2>&1
    rc=$?
  fi
  set -e

  if [[ $rc -eq 124 ]]; then
    echo "[$case_name] repair workflow timed out"
    cat "$run_log"
    return 1
  fi

  if ! grep -Eq "repair progress: .*step=meta .*vnode=1/1 .*progress=100%" "$run_log"; then
    echo "[$case_name] missing meta progress output"
    cat "$run_log"
    return 1
  fi

  if ! grep -Eq "repair summary: .*status=success .*successVnodes=1 .*failedVnodes=0" "$run_log"; then
    echo "[$case_name] missing successful summary output"
    cat "$run_log"
    return 1
  fi

  local session_dir
  session_dir="$(find "$backup_dir" -maxdepth 1 -mindepth 1 -type d -name 'repair-*' | head -n 1)"
  if [[ -z "$session_dir" ]]; then
    echo "[$case_name] repair session directory not found"
    find "$backup_dir" -maxdepth 3 -print
    return 1
  fi

  if [[ ! -f "$session_dir/repair.log" || ! -f "$session_dir/repair.state.json" ]]; then
    echo "[$case_name] repair session artifacts are incomplete"
    find "$session_dir" -maxdepth 3 -print
    return 1
  fi

  if ! grep -q "meta missing marker" "$session_dir/repair.log"; then
    echo "[$case_name] missing marker log not found"
    cat "$session_dir/repair.log"
    return 1
  fi

  if ! grep -q "meta infer detail" "$session_dir/repair.log"; then
    echo "[$case_name] infer detail log not found"
    cat "$session_dir/repair.log"
    return 1
  fi

  if ! grep -q "meta rebuild detail" "$session_dir/repair.log"; then
    echo "[$case_name] rebuild detail log not found"
    cat "$session_dir/repair.log"
    return 1
  fi

  if [[ ! -f "$data_dir/vnode/vnode2/meta/table.db" || ! -f "$data_dir/vnode/vnode2/meta/schema.db" || \
        ! -f "$data_dir/vnode/vnode2/meta/uid.idx" || ! -f "$data_dir/vnode/vnode2/meta/name.idx" ]]; then
    echo "[$case_name] required meta files are not complete after repair"
    find "$data_dir/vnode/vnode2/meta" -maxdepth 2 -type f | sort
    return 1
  fi

  if [[ "$case_mode" == "partial" && ! -f "$data_dir/vnode/vnode2/meta/tag.idx" ]]; then
    echo "[$case_name] optional index file should be preserved"
    find "$data_dir/vnode/vnode2/meta" -maxdepth 2 -type f | sort
    return 1
  fi

  echo "[$case_name] ok (taosd exit code: $rc)"
}

run_meta_case "meta-partial" "partial"
run_meta_case "meta-complete" "complete"

echo "meta force repair script passed"
