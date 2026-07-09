#!/bin/bash
# Run all performance benchmarks sequentially
# Each test gets a clean sim environment

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
unset LD_PRELOAD

RESULTS_DIR="/tmp/perf_benchmark_results"
mkdir -p "$RESULTS_DIR"

# Function to run a single test with cleanup
run_test() {
    local test_file=$1
    local name=$(basename "$test_file" .py)

    echo "=== Running $name ==="

    # Kill any leftover taosd
    ps aux | grep taosd | grep -v grep | awk '{print $2}' | xargs kill -9 2>/dev/null
    sleep 1
    rm -rf /home/yihao/TDengine/sim

    # Run test with 10min timeout
    timeout 600 python3 -m pytest "$test_file" -v --timeout=300 2>&1 | tee "$RESULTS_DIR/${name}.log" | tail -5

    local exit_code=${PIPESTATUS[0]}
    if [ $exit_code -eq 0 ]; then
        echo "=== PASSED $name ==="
    else
        echo "=== FAILED $name (exit=$exit_code) ==="
    fi
    echo ""
}

# Feature A: Tag-ref (7 files)
echo "#########################"
echo "# Feature A: Tag-ref    #"
echo "#########################"
for f in test_perf_tag_ref_r1_baseline.py \
         test_perf_tag_ref_r2_tag_queries.py \
         test_perf_tag_ref_r3_many_tables.py \
         test_perf_tag_ref_r4_big_data.py \
         test_perf_tag_ref_r5_many_tagrefs.py \
         test_perf_tag_ref_r6_cross_db.py \
         test_perf_tag_ref_r7_mixed_tags.py; do
    run_test "$f"
done

# Feature B: VChild (7 files)
echo "#########################"
echo "# Feature B: VChild     #"
echo "#########################"
for f in test_perf_vchild_r1_baseline.py \
         test_perf_vchild_r2_depth.py \
         test_perf_vchild_r3_shape.py \
         test_perf_vchild_r4_big_data.py \
         test_perf_vchild_r5_many_colrefs.py \
         test_perf_vchild_r6_cross_db.py \
         test_perf_vchild_r7_small_data_deep.py; do
    run_test "$f"
done

# Feature C: VStable (7 files)
echo "#########################"
echo "# Feature C: VStable    #"
echo "#########################"
for f in test_perf_vstable_r1_baseline.py \
         test_perf_vstable_r2_depth.py \
         test_perf_vstable_r3_many_tables.py \
         test_perf_vstable_r4_big_data.py \
         test_perf_vstable_r5_many_colrefs.py \
         test_perf_vstable_r6_cross_db.py \
         test_perf_vstable_r7_with_tag_ref.py; do
    run_test "$f"
done

echo "#########################"
echo "# All benchmarks done   #"
echo "#########################"

# Collect all reports
echo "=== Report Files ==="
ls -la /tmp/perf_*_r*_report.txt 2>/dev/null

# Summary
echo ""
echo "=== Pass/Fail Summary ==="
for f in test_perf_tag_ref_r*.py test_perf_vchild_r*.py test_perf_vstable_r*.py; do
    name=$(basename "$f" .py)
    if grep -q "passed" "$RESULTS_DIR/${name}.log" 2>/dev/null; then
        echo "  PASS  $name"
    else
        echo "  FAIL  $name"
    fi
done
