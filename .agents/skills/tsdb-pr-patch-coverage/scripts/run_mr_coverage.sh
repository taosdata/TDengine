#!/usr/bin/env bash
# run_mr_coverage.sh — orchestration helper for the tsdb-pr-patch-coverage skill.
#
# Wraps the canonical sequence:
#   1. kill stale taosd
#   2. relink taosd (mandatory — prevents stale-binary measurement)
#   3. run gtest binaries, lcov capture
#   4. clear .gcda, run pytest, lcov capture
#   5. merge → lcov2gcovr.py → gcovr HTML
#   6. patch_cov.py against the diff range
#
# All paths are passed by the caller; this script makes no assumption about
# where the TDengine checkout lives.

set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 \
  --repo <repo-root> \
  --build-dir <debug-build-dir> \
  --bin-dir <bin-dir-with-taosd> \
  --test-dir <pytest-cwd-dir> \
  --base <git-base-rev> \
  --head <git-head-rev> \
  --pytest-files "<rel/path/a.py> <rel/path/b.py>" \
  --gtest-bins   "<binary1> <binary2>" \
  --out-dir <output-dir> \
  --skill-scripts <skill-scripts-dir>

Examples:
  $0 --repo /root/code/tsdb \\
     --build-dir /root/code/tsdb/source/taos-community/debug \\
     --bin-dir   /root/code/tsdb/source/taos-community/debug/build/bin \\
     --test-dir  /root/code/tsdb/source/taos-community/test \\
     --base ac53de75aa2 --head feat/6986382331 \\
     --pytest-files "cases/18-StreamProcessing/02-Stream/stream_vtable_chain_ref.py" \\
     --gtest-bins   "vnodeStreamVTableTest" \\
     --out-dir /tmp/mr-cov \\
    --skill-scripts \$HOME/.skills/tsdb-pr-patch-coverage/scripts
EOF
  exit 1
}

REPO=""; BUILD_DIR=""; BIN_DIR=""; TEST_DIR=""; BASE=""; HEAD=""
PYTEST_FILES=""; GTEST_BINS=""; OUT_DIR=""; SKILL_SCRIPTS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) REPO="$2"; shift 2;;
    --build-dir) BUILD_DIR="$2"; shift 2;;
    --bin-dir) BIN_DIR="$2"; shift 2;;
    --test-dir) TEST_DIR="$2"; shift 2;;
    --base) BASE="$2"; shift 2;;
    --head) HEAD="$2"; shift 2;;
    --pytest-files) PYTEST_FILES="$2"; shift 2;;
    --gtest-bins) GTEST_BINS="$2"; shift 2;;
    --out-dir) OUT_DIR="$2"; shift 2;;
    --skill-scripts) SKILL_SCRIPTS="$2"; shift 2;;
    -h|--help) usage;;
    *) echo "Unknown arg: $1" >&2; usage;;
  esac
done

[ -n "$REPO" ] && [ -n "$BUILD_DIR" ] && [ -n "$BIN_DIR" ] && [ -n "$TEST_DIR" ] \
  && [ -n "$BASE" ] && [ -n "$HEAD" ] && [ -n "$OUT_DIR" ] && [ -n "$SKILL_SCRIPTS" ] || usage

mkdir -p "$OUT_DIR"
LOG_DIR="$OUT_DIR/logs"
mkdir -p "$LOG_DIR"

echo "==> [1/6] kill stale taosd"
pids=$(pgrep taosd 2>/dev/null || true)
if [ -n "$pids" ]; then
  for p in $pids; do kill -9 "$p" 2>/dev/null || true; done
  sleep 2
fi

echo "==> [2/6] relink taosd (critical: avoids stale-binary measurement)"
(cd "$BUILD_DIR" && nice -n 5 cmake --build . --target taosd -j 4 \
  > "$LOG_DIR/relink.log" 2>&1) || { tail "$LOG_DIR/relink.log"; exit 1; }
stat -c 'taosd: %y' "$BIN_DIR/taosd"

# Patch pytest framework: replace SIGKILL with SIGTERM so atexit -> __gcov_dump
# can flush .gcda. SIGKILL bypasses atexit and silently loses coverage on every
# case (worst-observed failure: 80% real -> 13% measured). Idempotent; safe to
# re-run. Restore via `git checkout` once coverage run is done.
FW_TAOSD="$REPO/source/taos-community/test/new_test_framework/taostest/components/taosd.py"
FW_DNODES="$REPO/source/taos-community/test/new_test_framework/utils/server/dnodes.py"
echo "==> [2.5/6] patch test framework: SIGKILL -> SIGTERM (coverage flush)"
if [ -f "$FW_TAOSD" ]; then
  sed -i "s/xargs kill -9 /xargs kill -TERM /" "$FW_TAOSD"
fi
if [ -f "$FW_DNODES" ]; then
  sed -i 's/kill -9 %s/kill -TERM %s/g' "$FW_DNODES"
fi

run_lcov_capture() {
  local out="$1"
  lcov --capture --directory "$BUILD_DIR" --output-file "$out" \
    --rc lcov_branch_coverage=0 --quiet 2>/dev/null || true
}

UT_INFO="$OUT_DIR/ut.info"
PY_INFO="$OUT_DIR/pytest.info"
MERGED_INFO="$OUT_DIR/merged.info"

if [ -n "$GTEST_BINS" ]; then
  echo "==> [3/6] run gtest binaries"
  find "$BUILD_DIR" -name '*.gcda' -delete
  for b in $GTEST_BINS; do
    bin="$BIN_DIR/$b"
    if [ ! -x "$bin" ]; then echo "  missing: $bin (skipped)"; continue; fi
    echo "  -> $b"
    "$bin" > "$LOG_DIR/$b.log" 2>&1 || { echo "FAIL: $b"; tail "$LOG_DIR/$b.log"; exit 1; }
  done
  run_lcov_capture "$UT_INFO"
  echo "  captured $(wc -l < "$UT_INFO") lines"
fi

if [ -n "$PYTEST_FILES" ]; then
  echo "==> [4/6] run pytest"
  # Clean sim + gcda
  rm -rf "$REPO/source/taos-community/sim/dnode"* \
         "$REPO/source/taos-community/sim/psim" 2>/dev/null || true
  rm -rf "$REPO/sim/asan/"* "$REPO/sim/tsim/"* 2>/dev/null || true
  find "$BUILD_DIR" -name '*.gcda' -delete
  export TAOS_BIN_PATH="$BIN_DIR"
  (cd "$TEST_DIR" && ./ci/pytest.sh pytest $PYTEST_FILES) \
    > "$LOG_DIR/pytest.log" 2>&1 || true
  if ! grep -q ' passed' "$LOG_DIR/pytest.log"; then
    echo "PYTEST FAILED — see $LOG_DIR/pytest.log"
    tail -30 "$LOG_DIR/pytest.log"
    exit 1
  fi
  # Flush gcda (graceful stop)
  pids=$(pgrep taosd 2>/dev/null || true)
  for p in $pids; do kill -15 "$p" 2>/dev/null || true; done
  sleep 5
  pids=$(pgrep taosd 2>/dev/null || true)
  for p in $pids; do kill -9 "$p" 2>/dev/null || true; done
  sleep 2
  run_lcov_capture "$PY_INFO"
  echo "  captured $(wc -l < "$PY_INFO") lines"
fi

echo "==> [5/6] merge + lcov2gcovr + gcovr HTML"
MERGE_ARGS=""
[ -s "$UT_INFO" ] && MERGE_ARGS="$MERGE_ARGS --add-tracefile $UT_INFO"
[ -s "$PY_INFO" ] && MERGE_ARGS="$MERGE_ARGS --add-tracefile $PY_INFO"
[ -z "$MERGE_ARGS" ] && { echo "no coverage captured"; exit 1; }
lcov $MERGE_ARGS --output-file "$MERGED_INFO" --rc lcov_branch_coverage=0 --quiet 2>/dev/null

# Filter to MR-touched source files
SRC_FILES_LIST="$OUT_DIR/mr_src_files.txt"
(cd "$REPO" && git --no-pager diff --name-only "$BASE..$HEAD" -- \
  '*.c' '*.h' '*.cpp' '*.hpp') > "$SRC_FILES_LIST"
FILTERED_INFO="$OUT_DIR/filtered.info"
python3 - <<PYEOF
abs = set()
with open('$SRC_FILES_LIST') as f:
    for ln in f:
        ln = ln.strip()
        if ln: abs.add('$REPO/' + ln)
out=[]; keep=False; buf=[]
with open('$MERGED_INFO') as f:
    for ln in f:
        if ln.startswith('SF:'):
            buf=[ln]; keep = ln[3:].strip() in abs
        elif ln.strip()=='end_of_record':
            buf.append(ln)
            if keep: out.extend(buf)
            buf=[]; keep=False
        else:
            buf.append(ln)
open('$FILTERED_INFO','w').writelines(out)
print(f"filtered: {len(out)} lines from {len(abs)} MR-touched files")
PYEOF

GCOVR_JSON="$OUT_DIR/coverage.gcovr.json"
python3 "$SKILL_SCRIPTS/lcov2gcovr.py" "$FILTERED_INFO" "$GCOVR_JSON"

HTML_DIR="$OUT_DIR/html"
rm -rf "$HTML_DIR"; mkdir -p "$HTML_DIR"
gcovr --json-add-tracefile "$GCOVR_JSON" \
  --html --html-details --html-theme blue \
  --root "$REPO" -o "$HTML_DIR/index.html" \
  --gcov-ignore-errors all 2>&1 | tail -2

echo "==> [6/6] patch coverage"
python3 "$SKILL_SCRIPTS/patch_cov.py" \
  --repo "$REPO" --base "$BASE" --head "$HEAD" \
  --gcovr-json "$GCOVR_JSON" \
  --out "$OUT_DIR/patch_coverage.txt"

echo ""
echo "Done."
echo "  HTML:   file://$HTML_DIR/index.html"
echo "  Report: $OUT_DIR/patch_coverage.txt"
