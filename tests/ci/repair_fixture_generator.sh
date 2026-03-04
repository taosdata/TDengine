#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  repair_fixture_generator.sh --output-dir <dir> [--type wal|tsdb|meta|all] [--vnode-id <id>] [--clean]

Description:
  Generate reproducible corruption fixtures for TDengine repair tests.
  Fixtures include WAL, TSDB, and META scenarios.

Options:
  --output-dir <dir>  Required. Root directory to place generated fixtures.
  --type <type>       Optional. One of wal|tsdb|meta|all. Default: all.
  --vnode-id <id>     Optional. Target vnode id. Default: 2.
  --clean             Optional. Remove output-dir before generation.
  -h, --help          Show this help.
EOF
}

OUTPUT_DIR=""
FIXTURE_TYPE="all"
VNODE_ID=2
CLEAN_OUTPUT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      [[ $# -ge 2 ]] || {
        echo "'--output-dir' requires a value"
        exit 1
      }
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --type)
      [[ $# -ge 2 ]] || {
        echo "'--type' requires a value"
        exit 1
      }
      FIXTURE_TYPE="$2"
      shift 2
      ;;
    --vnode-id)
      [[ $# -ge 2 ]] || {
        echo "'--vnode-id' requires a value"
        exit 1
      }
      VNODE_ID="$2"
      shift 2
      ;;
    --clean)
      CLEAN_OUTPUT=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$OUTPUT_DIR" ]]; then
  echo "'--output-dir' is required"
  usage
  exit 1
fi

case "$FIXTURE_TYPE" in
  wal|tsdb|meta|all) ;;
  *)
    echo "Invalid '--type': $FIXTURE_TYPE"
    exit 1
    ;;
esac

if [[ ! "$VNODE_ID" =~ ^[0-9]+$ ]]; then
  echo "Invalid '--vnode-id': $VNODE_ID"
  exit 1
fi

if [[ $CLEAN_OUTPUT -eq 1 ]]; then
  rm -rf "$OUTPUT_DIR"
fi

mkdir -p "$OUTPUT_DIR"

MANIFEST="$OUTPUT_DIR/manifest.txt"
: > "$MANIFEST"

manifest_append() {
  local line="$1"
  echo "$line" >> "$MANIFEST"
}

generate_wal_fixture() {
  local case_root="$OUTPUT_DIR/wal-force-corrupted"
  local wal_dir="$case_root/vnode/vnode${VNODE_ID}/wal"
  mkdir -p "$wal_dir"

  printf 'wal-meta-ok\n' > "$wal_dir/000001.meta"
  printf 'wal-idx-ok\n' > "$wal_dir/000001.idx"
  printf 'wal-log-segment-000001-valid\n' > "$wal_dir/000001.log"

  printf 'wal-log-segment-000002-corrupted\n' > "$wal_dir/000002.log"
  truncate -s 9 "$wal_dir/000002.log"
  printf 'corrupted-idx-payload\n' > "$wal_dir/000002.idx"

  manifest_append "wal=$case_root"
}

generate_tsdb_fixture() {
  local case_root="$OUTPUT_DIR/tsdb-force-mixed"
  local tsdb_dir="$case_root/vnode/vnode${VNODE_ID}/tsdb"
  mkdir -p "$tsdb_dir/f100" "$tsdb_dir/f200" "$tsdb_dir/f300"

  printf 'recoverable-head\n' > "$tsdb_dir/f100/block.head"
  printf 'recoverable-data\n' > "$tsdb_dir/f100/block.data"

  printf 'corrupted-head-only\n' > "$tsdb_dir/f200/bad.head"
  printf 'orphan-sma-only\n' > "$tsdb_dir/f300/orphan.sma"

  manifest_append "tsdb=$case_root"
}

generate_meta_fixture() {
  local partial_root="$OUTPUT_DIR/meta-force-partial"
  local complete_root="$OUTPUT_DIR/meta-force-complete"

  local partial_meta_dir="$partial_root/vnode/vnode${VNODE_ID}/meta"
  local partial_wal_dir="$partial_root/vnode/vnode${VNODE_ID}/wal"
  mkdir -p "$partial_meta_dir" "$partial_wal_dir"
  printf 'table-partial\n' > "$partial_meta_dir/table.db"
  printf 'tag-index-preserved\n' > "$partial_meta_dir/tag.idx"
  printf 'wal-evidence\n' > "$partial_wal_dir/000001.log"

  local complete_meta_dir="$complete_root/vnode/vnode${VNODE_ID}/meta"
  local complete_wal_dir="$complete_root/vnode/vnode${VNODE_ID}/wal"
  mkdir -p "$complete_meta_dir" "$complete_wal_dir"
  printf 'wal-evidence\n' > "$complete_wal_dir/000001.log"

  manifest_append "meta_partial=$partial_root"
  manifest_append "meta_complete=$complete_root"
}

if [[ "$FIXTURE_TYPE" == "wal" || "$FIXTURE_TYPE" == "all" ]]; then
  generate_wal_fixture
fi

if [[ "$FIXTURE_TYPE" == "tsdb" || "$FIXTURE_TYPE" == "all" ]]; then
  generate_tsdb_fixture
fi

if [[ "$FIXTURE_TYPE" == "meta" || "$FIXTURE_TYPE" == "all" ]]; then
  generate_meta_fixture
fi

echo "repair fixture generation complete"
echo "output: $OUTPUT_DIR"
echo "manifest: $MANIFEST"
