#!/bin/bash
# Run VCT depth benchmark for each depth point as a separate pytest process.
# pytest conftest handles taosd lifecycle (fresh deploy per test).
# Usage: bash run_perf_vct_depth.sh

set -e

CASE_DIR="$(cd "$(dirname "$0")" && pwd)"
TEST_FILE="$CASE_DIR/test_perf_vct_depth_one.py"
DEPTHS=(1 2 4 8 16 32)

cd "$(dirname "$CASE_DIR")/.."  # test/ directory

echo "=== VCT Depth Benchmark Runner ==="
echo "Depths: ${DEPTHS[*]}"
echo ""

for D in "${DEPTHS[@]}"; do
    echo ">>> Running DEPTH=$D ..."
    DEPTH=$D pytest -s --timeout=600 "$TEST_FILE" 2>&1 \
        | grep -E "INFO|ERROR|PASS|FAIL|Depth=" || true
    echo ">>> Depth=$D done."
    echo ""
done

echo "=== Merging results ==="

python3 << 'PYEOF'
import json

DEPTHS = [1, 2, 4, 8, 16, 32]
QUERIES = [
    "A1  COUNT", "A2  SUM/AVG/MAX", "A3  LAST", "A4  WHERE >",
    "A5  SELECT *", "A6  BETWEEN", "A7  AVG/STDDEV", "A8  FIRST/LAST",
    "A9  ORDER BY", "A10 LIMIT",
]

all_data = {}
rows = None
repeats = None
for d in DEPTHS:
    with open(f"/tmp/perf_vct_d{d}.json") as f:
        data = json.load(f)
        all_data[d] = data["results"]
        rows = data["rows"]
        repeats = data["repeats"]

REPORT = "/tmp/perf_vtable_ref_vct_depth_report.txt"
lines = []
def emit(s=""):
    lines.append(s)

emit("PERF: VTable-ref-VTable Depth (VCT dimension - isolated runs)")
emit("=" * 100)
emit(f"  rows={rows:,}, repeats={repeats}, depths={DEPTHS}")
emit(f"  Each depth: separate pytest process (fresh taosd)")
emit("")

emit("Absolute Latency (ms)")
header = f"  {'Query':<16}" + "".join(f"{'D'+str(d):>10}" for d in DEPTHS)
emit(header)
emit("  " + "-" * (16 + 10 * len(DEPTHS)))
for q in QUERIES:
    row = f"  {q:<16}"
    for d in DEPTHS:
        row += f"{all_data[d][q]:>10.2f}"
    emit(row)

emit("")
emit("Overhead vs D1 Baseline (%)")
emit(header)
emit("  " + "-" * (16 + 10 * len(DEPTHS)))
for q in QUERIES:
    row = f"  {q:<16}"
    base = all_data[1][q]
    for d in DEPTHS:
        val = all_data[d][q]
        pct = (val - base) / base * 100 if base > 0 else 0
        row += f"{pct:>+9.1f}%"
    emit(row)

emit("")
emit("D32 vs D1 Summary")
emit(f"  {'Query':<16} {'D1(ms)':>10} {'D32(ms)':>10} {'Overhead':>10} {'Delta(ms)':>12}")
emit("  " + "-" * 60)
for q in QUERIES:
    d1 = all_data[1][q]
    d32 = all_data[32][q]
    overhead = f"{(d32 - d1) / d1 * 100:+.1f}%" if d1 > 0 else "N/A"
    delta = f"{d32 - d1:+.2f}"
    emit(f"  {q:<16} {d1:>10.2f} {d32:>10.2f} {overhead:>10} {delta:>12}")

emit("")
emit("  Topology: src_c0 <- vct_d1 <- vct_d2 <- ... <- vct_d32 (single chain)")
emit(f"  Data: 1 child x {rows:,} rows")
emit("  Cache: each depth tested in isolated pytest process")
emit("")
emit("=" * 100)

with open(REPORT, "w") as f:
    f.write("\n".join(lines) + "\n")
print(f"Report: {REPORT}")
PYEOF

echo "=== Done ==="
cat /tmp/perf_vtable_ref_vct_depth_report.txt
