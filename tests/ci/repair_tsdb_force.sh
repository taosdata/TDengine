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

TMP_ROOT="$(mktemp -d /tmp/td-repair-tsdb-force-XXXXXX)"
DATA_DIR="$TMP_ROOT/data"
BACKUP_DIR="$TMP_ROOT/backup"
LOG_DIR="$TMP_ROOT/taoslog"
RUN_LOG="$TMP_ROOT/repair-run.log"

cleanup() {
  rm -rf "$TMP_ROOT"
}
trap cleanup EXIT

mkdir -p "$DATA_DIR/vnode/vnode2/tsdb/f100" "$DATA_DIR/vnode/vnode2/tsdb/f200" "$BACKUP_DIR" "$LOG_DIR"
printf 'recoverable-head\n' > "$DATA_DIR/vnode/vnode2/tsdb/f100/block.head"
printf 'recoverable-data\n' > "$DATA_DIR/vnode/vnode2/tsdb/f100/block.data"
printf 'corrupted-head-only\n' > "$DATA_DIR/vnode/vnode2/tsdb/f200/bad.head"

run_repair() {
  if command -v timeout >/dev/null 2>&1; then
    TAOS_DATA_DIR="$DATA_DIR" ASAN_OPTIONS=detect_leaks=0 timeout 30s \
      "$TAOSD_BIN" -o "$LOG_DIR" -r --node-type vnode --file-type tsdb --vnode-id 2 --mode force \
      --backup-path "$BACKUP_DIR" >"$RUN_LOG" 2>&1
  else
    TAOS_DATA_DIR="$DATA_DIR" ASAN_OPTIONS=detect_leaks=0 \
      "$TAOSD_BIN" -o "$LOG_DIR" -r --node-type vnode --file-type tsdb --vnode-id 2 --mode force \
      --backup-path "$BACKUP_DIR" >"$RUN_LOG" 2>&1
  fi
}

set +e
run_repair
RC=$?
set -e

if [[ $RC -eq 124 ]]; then
  echo "repair workflow timed out"
  cat "$RUN_LOG"
  exit 1
fi

if ! grep -Eq "repair progress: .*step=tsdb .*vnode=1/1 .*progress=100%" "$RUN_LOG"; then
  echo "missing tsdb repair progress in output"
  cat "$RUN_LOG"
  exit 1
fi

if ! grep -Eq "repair summary: .*status=success .*successVnodes=1 .*failedVnodes=0" "$RUN_LOG"; then
  echo "missing successful repair summary in output"
  cat "$RUN_LOG"
  exit 1
fi

if [[ ! -f "$DATA_DIR/vnode/vnode2/tsdb/f100/block.head" || ! -f "$DATA_DIR/vnode/vnode2/tsdb/f100/block.data" ]]; then
  echo "recoverable tsdb block was not kept in target directory"
  find "$DATA_DIR/vnode/vnode2/tsdb" -maxdepth 4 -type f | sort
  exit 1
fi

if [[ -e "$DATA_DIR/vnode/vnode2/tsdb/f200/bad.head" ]]; then
  echo "corrupted tsdb block should not exist after rebuild"
  find "$DATA_DIR/vnode/vnode2/tsdb" -maxdepth 4 -type f | sort
  exit 1
fi

SESSION_DIR="$(find "$BACKUP_DIR" -maxdepth 1 -mindepth 1 -type d -name 'repair-*' | head -n 1)"
if [[ -z "$SESSION_DIR" ]]; then
  echo "repair session directory not found"
  find "$BACKUP_DIR" -maxdepth 3 -print
  exit 1
fi

if [[ ! -f "$SESSION_DIR/repair.log" || ! -f "$SESSION_DIR/repair.state.json" ]]; then
  echo "repair session artifacts are incomplete"
  find "$SESSION_DIR" -maxdepth 3 -print
  exit 1
fi

if [[ ! -f "$SESSION_DIR/vnode2/tsdb/f200/bad.head" ]]; then
  echo "backup directory does not contain corrupted source block"
  find "$SESSION_DIR/vnode2/tsdb" -maxdepth 4 -type f | sort
  exit 1
fi

if ! grep -Eq '"step"[[:space:]]*:[[:space:]]*"preflight"' "$SESSION_DIR/repair.state.json"; then
  echo "repair state file does not record preflight completion"
  cat "$SESSION_DIR/repair.state.json"
  exit 1
fi

echo "tsdb force repair script passed (taosd exit code: $RC)"
