#!/bin/bash
# ============================================================================
# benchmark.sh -- Benchmark boosttools sync performance
#
# Runs multiple configurations and reports throughput.
# ============================================================================

set -euo pipefail

BOOSTTOOLS="${BOOSTTOOLS:-./boosttools}"
SRC_HOST="${SRC_HOST:-127.0.0.1}"
SRC_PORT="${SRC_PORT:-6030}"
DST_HOST="${DST_HOST:-127.0.0.1}"
DST_PORT="${DST_PORT:-6130}"
DATABASE="${DATABASE:-testdb}"
RESULT_FILE="benchmark_results.txt"

echo "==========================================" | tee "$RESULT_FILE"
echo " boosttools Benchmark" | tee -a "$RESULT_FILE"
echo " $(date)" | tee -a "$RESULT_FILE"
echo "==========================================" | tee -a "$RESULT_FILE"
echo "" | tee -a "$RESULT_FILE"

# Get source data info
echo "Collecting source data info..." | tee -a "$RESULT_FILE"
SRC_SIZE=$(du -sh /var/lib/taos_a/ | awk '{print $1}')
SRC_TABLES=$(taos -h $SRC_HOST -P $SRC_PORT -s "select count(*) from information_schema.ins_tables where db_name='${DATABASE}'" < /dev/null 2>&1 | grep -oE '[0-9]+' | tail -1)
SRC_ROWS=$(taos -h $SRC_HOST -P $SRC_PORT -s "select count(*) from ${DATABASE}.meters" < /dev/null 2>&1 | grep -oE '[0-9]+' | tail -1)

echo "Source: ${SRC_SIZE} data, ${SRC_TABLES} tables, ${SRC_ROWS} rows" | tee -a "$RESULT_FILE"
echo "" | tee -a "$RESULT_FILE"

run_bench() {
    local workers=$1
    local label=$2

    echo "--- Test: $label (workers=$workers) ---" | tee -a "$RESULT_FILE"

    # Clean destination
    taos -h $DST_HOST -P $DST_PORT -s "DROP DATABASE IF EXISTS ${DATABASE}" < /dev/null 2>&1 > /dev/null

    # Run sync
    local start_ts=$(date +%s)

    $BOOSTTOOLS \
        --src-host $SRC_HOST --src-port $SRC_PORT \
        --dst-host $DST_HOST --dst-port $DST_PORT \
        --database $DATABASE \
        --workers $workers 2>&1 | tee /tmp/boost_${workers}.log

    local end_ts=$(date +%s)
    local elapsed=$((end_ts - start_ts))

    # Verify destination
    local dst_tables=$(taos -h $DST_HOST -P $DST_PORT -s "select count(*) from information_schema.ins_tables where db_name='${DATABASE}'" < /dev/null 2>&1 | grep -oE '[0-9]+' | tail -1)
    local dst_rows=$(taos -h $DST_HOST -P $DST_PORT -s "select count(*) from ${DATABASE}.meters" < /dev/null 2>&1 | grep -oE '[0-9]+' | tail -1)
    local dst_size=$(du -sh /var/lib/taos_b/ | awk '{print $1}')

    echo "" | tee -a "$RESULT_FILE"
    echo "Results ($label):" | tee -a "$RESULT_FILE"
    echo "  Workers:     $workers" | tee -a "$RESULT_FILE"
    echo "  Elapsed:     ${elapsed}s" | tee -a "$RESULT_FILE"
    echo "  Src tables:  $SRC_TABLES" | tee -a "$RESULT_FILE"
    echo "  Dst tables:  $dst_tables" | tee -a "$RESULT_FILE"
    echo "  Src rows:    $SRC_ROWS" | tee -a "$RESULT_FILE"
    echo "  Dst rows:    $dst_rows" | tee -a "$RESULT_FILE"
    echo "  Dst size:    $dst_size" | tee -a "$RESULT_FILE"

    if [ "$elapsed" -gt 0 ]; then
        local tps=$((${SRC_ROWS:-0} / elapsed))
        echo "  Throughput:  ${tps} rows/s" | tee -a "$RESULT_FILE"
    fi

    echo "  Match:       tables=$( [ "$SRC_TABLES" = "$dst_tables" ] && echo 'YES' || echo 'NO') rows=$( [ "$SRC_ROWS" = "$dst_rows" ] && echo 'YES' || echo 'NO')" | tee -a "$RESULT_FILE"
    echo "" | tee -a "$RESULT_FILE"
}

# Run benchmarks with different worker counts
run_bench 4   "4-workers"
run_bench 8   "8-workers"
run_bench 16  "16-workers"
run_bench 32  "32-workers"

echo "==========================================" | tee -a "$RESULT_FILE"
echo " Benchmark Complete" | tee -a "$RESULT_FILE"
echo " Results saved to: $RESULT_FILE" | tee -a "$RESULT_FILE"
echo "==========================================" | tee -a "$RESULT_FILE"
