---
name: tsdb-perf-verify
description: "Re-run performance test scenarios to verify optimization improvements. Compare metrics before and after, iterate until performance goals are met. Keywords: performance verify, benchmark, metrics comparison, optimization validation"
metadata:
  author: beryl
  version: 1.0.0
  owner_team: engine
---

# Performance Verification

## Quick Start

This skill verifies that performance optimizations achieved the expected improvements by re-running test scenarios and comparing metrics.

## Prerequisites

- Performance fixes implemented (see tsdb-perf-fix)
- Original baseline metrics recorded
- Test scenarios available (from tsdb-perf-scenario-setup)

## Step 1: Prepare Baseline Comparison

### Load Baseline Metrics

```bash
# Review original performance data
cat /tmp/baseline_metrics.txt 2>/dev/null || echo "No baseline found"

# If baseline doesn't exist, document current metrics as baseline
cat > /tmp/baseline_metrics.txt << EOF
Baseline Performance Metrics
Date: $(date)
Branch: $(git branch --show-current)
Commit: $(git rev-parse --short HEAD)

CPU Hotspots:
- function_name: 35% overhead
- other_function: 15% overhead

Memory:
- Peak usage: 2.5GB
- Allocations: 1M/sec

Throughput:
- Write QPS: 50,000
- Query QPS: 5,000

Latency:
- P50: 10ms
- P95: 50ms
- P99: 200ms
EOF
```

## Step 2: Re-run Test Scenario

### Execute Same Workload

```bash
# Ensure clean state
pkill -9 taosd
rm -rf /var/lib/taos/*

# Start optimized taosd
/root/workspace/TDinternal/debug/build/bin/taosd -c /etc/taos &
sleep 3

# Re-run the exact same test scenario
/tmp/perf_write.sh
/tmp/perf_query.sh
```

### Measure Throughput

```bash
# Write throughput test
echo "Testing write throughput..."
START_TIME=$(date +%s)
/tmp/perf_write.sh
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
echo "Write test completed in ${DURATION} seconds" | tee -a /tmp/optimized_metrics.txt

# Query throughput test
echo "Testing query throughput..."
START_TIME=$(date +%s)
/tmp/perf_query.sh
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
echo "Query test completed in ${DURATION} seconds" | tee -a /tmp/optimized_metrics.txt
```

### Measure Latency

```bash
# Create latency test script
cat > /tmp/measure_latency.sh << 'EOF'
#!/bin/bash
DB_NAME="perf_db"
ITERATIONS=1000

echo "Measuring query latency..."
for i in $(seq 1 $ITERATIONS); do
    START=$(date +%s%N)
    taos -s "SELECT * FROM ${DB_NAME}.meters LIMIT 100;" > /dev/null 2>&1
    END=$(date +%s%N)
    LATENCY=$(( (END - START) / 1000000 ))  # Convert to milliseconds
    echo $LATENCY
done | sort -n > /tmp/latencies.txt

# Calculate percentiles
TOTAL=$(wc -l < /tmp/latencies.txt)
P50_LINE=$(( TOTAL / 2 ))
P95_LINE=$(( TOTAL * 95 / 100 ))
P99_LINE=$(( TOTAL * 99 / 100 ))

echo "Latency P50: $(sed -n "${P50_LINE}p" /tmp/latencies.txt)ms"
echo "Latency P95: $(sed -n "${P95_LINE}p" /tmp/latencies.txt)ms"
echo "Latency P99: $(sed -n "${P99_LINE}p" /tmp/latencies.txt)ms"
EOF

chmod +x /tmp/measure_latency.sh
/tmp/measure_latency.sh | tee -a /tmp/optimized_metrics.txt
```

## Step 3: Re-profile with Same Tools

### CPU Profiling

```bash
# Capture new CPU profile
TAOSD_PID=$(pgrep taosd)
perf record -F 99 -p $TAOSD_PID -g -- sleep 30
perf report -n --stdio > /tmp/perf_report_optimized.txt

# Generate flame graph
perf script | /tmp/FlameGraph/stackcollapse-perf.pl | /tmp/FlameGraph/flamegraph.pl > /tmp/cpu_flamegraph_optimized.svg

# Extract top functions
grep -A 20 "Overhead" /tmp/perf_report_optimized.txt | head -25 > /tmp/top_functions_optimized.txt
```

### Memory Profiling

```bash
# Memory snapshot
pmap -x $(pgrep taosd) > /tmp/memory_map_optimized.txt

# Compare memory usage
echo "Memory comparison:" | tee -a /tmp/optimized_metrics.txt
echo "Before: $(grep -E "total.*K" /tmp/memory_map.txt 2>/dev/null || echo 'N/A')" | tee -a /tmp/optimized_metrics.txt
echo "After: $(grep -E "total.*K" /tmp/memory_map_optimized.txt)" | tee -a /tmp/optimized_metrics.txt
```

## Step 4: Compare Metrics

### Create Comparison Report

```bash
cat > /tmp/performance_comparison.txt << 'EOF'
===========================================
Performance Optimization Results
===========================================

Test Date: $(date)
Optimization: [Brief description]

--- CPU Performance ---
Hotspot Function: function_name
Before: 35% overhead
After: [New percentage]
Improvement: [Calculate %]

--- Throughput ---
Write QPS:
  Before: 50,000
  After: [New value]
  Improvement: [Calculate %]

Query QPS:
  Before: 5,000
  After: [New value]
  Improvement: [Calculate %]

--- Latency ---
P50:
  Before: 10ms
  After: [New value]
  Improvement: [Calculate %]

P95:
  Before: 50ms
  After: [New value]
  Improvement: [Calculate %]

P99:
  Before: 200ms
  After: [New value]
  Improvement: [Calculate %]

--- Memory ---
Peak Usage:
  Before: 2.5GB
  After: [New value]
  Improvement: [Calculate %]

--- Overall Assessment ---
Status: [PASS/FAIL/NEEDS_MORE_WORK]
Goal Achievement: [Percentage of target met]
Next Steps: [If not meeting goals]

===========================================
EOF
```

### AI Analysis of Results

AI should analyze the comparison and provide:

1. **Improvement Summary**
   - Which metrics improved
   - By how much (percentage)
   - Whether goals were met

2. **Regression Check**
   - Any metrics that got worse
   - Unexpected side effects
   - New hotspots introduced

3. **Goal Assessment**
   - Original target: e.g., "Improve QPS by 50%"
   - Actual achievement: e.g., "Improved QPS by 65%"
   - Status: PASS/FAIL

4. **Recommendations**
   - If goals met: Document and close
   - If goals not met: Identify next optimization
   - If regressions: Investigate and fix

## Step 5: Detailed Comparison

### Compare CPU Profiles

```bash
# Side-by-side comparison of top functions
echo "=== CPU Profile Comparison ===" > /tmp/cpu_comparison.txt
echo "" >> /tmp/cpu_comparison.txt
echo "BEFORE:" >> /tmp/cpu_comparison.txt
head -20 /tmp/top_functions.txt >> /tmp/cpu_comparison.txt
echo "" >> /tmp/cpu_comparison.txt
echo "AFTER:" >> /tmp/cpu_comparison.txt
head -20 /tmp/top_functions_optimized.txt >> /tmp/cpu_comparison.txt

cat /tmp/cpu_comparison.txt
```

### Compare Flame Graphs

```bash
# List flame graphs for visual comparison
ls -lh /tmp/cpu_flamegraph.svg /tmp/cpu_flamegraph_optimized.svg

echo "Open these files to visually compare:"
echo "  Before: /tmp/cpu_flamegraph.svg"
echo "  After: /tmp/cpu_flamegraph_optimized.svg"
```

### Statistical Comparison

```bash
# Calculate improvement percentages
cat > /tmp/calculate_improvement.sh << 'EOF'
#!/bin/bash

calculate_improvement() {
    local before=$1
    local after=$2
    local improvement=$(echo "scale=2; (($before - $after) / $before) * 100" | bc)
    echo "${improvement}%"
}

# Example usage:
# BEFORE_QPS=50000
# AFTER_QPS=75000
# echo "QPS Improvement: $(calculate_improvement $BEFORE_QPS $AFTER_QPS)"
EOF

chmod +x /tmp/calculate_improvement.sh
```

## Step 6: Validate Correctness

### Functional Tests

```bash
# Run test suite to ensure no regressions
cd /root/workspace/TDinternal/community/test

# Run relevant test cases
pytest test_insert.py -v
pytest test_query.py -v
pytest test_stream.py -v

# Check results
if [ $? -eq 0 ]; then
    echo "All tests passed" | tee -a /tmp/optimized_metrics.txt
else
    echo "TESTS FAILED - Optimization may have broken functionality" | tee -a /tmp/optimized_metrics.txt
fi
```

### Data Integrity Check

```bash
# Verify data correctness
taos -s "SELECT COUNT(*) FROM perf_db.meters;" > /tmp/count_after.txt
# Compare with expected count
```

## Step 7: Decision Point

### If Goals Met

```bash
# Document success
cat >> /tmp/performance_comparison.txt << EOF

=== OPTIMIZATION SUCCESSFUL ===
Goals achieved. Ready to commit and merge.

Improvements:
- [List key improvements]

No regressions detected.
All tests passing.
EOF

# Update commit message with results
git commit --amend -m "perf: optimize function_name

- Replace O(n) linear search with O(1) hash lookup
- Reduce memory allocations in hot path

Performance improvements:
- QPS: +65% (50K -> 82.5K)
- Latency P99: -60% (200ms -> 80ms)
- CPU overhead: -50% (35% -> 17.5%)

All tests passing. No regressions detected."
```

### If Goals Not Met

```bash
# Analyze why goals weren't met
cat >> /tmp/performance_comparison.txt << EOF

=== NEEDS MORE OPTIMIZATION ===
Goals not fully achieved. Further optimization needed.

Current Status:
- Target: [Original goal]
- Achieved: [Current result]
- Gap: [Remaining improvement needed]

Next Steps:
1. [Identify next bottleneck]
2. [Propose additional optimization]
3. [Iterate]
EOF

# Return to analysis phase
echo "Re-analyzing performance data to find next optimization opportunity..."
```

### If Regressions Detected

```bash
# Document regressions
cat >> /tmp/performance_comparison.txt << EOF

=== REGRESSIONS DETECTED ===
Optimization improved target metric but caused regressions.

Improvements:
- [List improvements]

Regressions:
- [List regressions]

Action Required:
- Investigate regression cause
- Adjust optimization approach
- Re-test
EOF
```

## Step 8: Iteration Loop

### Continue Optimization Cycle

```bash
# If more optimization needed, repeat the cycle:
# 1. Re-run tsdb-perf-profiling to find next bottleneck
# 2. Use tsdb-perf-analysis to identify new hotspots
# 3. Use tsdb-perf-code-locate to find code
# 4. Use tsdb-perf-fix to implement next optimization
# 5. Return here to verify

echo "Starting next optimization iteration..."
```

## Execution Rules

- Use identical test scenarios for fair comparison
- Run tests multiple times to account for variance
- Compare both absolute and relative improvements
- Check for regressions in other metrics
- Validate functional correctness
- Document all results
- Make data-driven decisions
- Iterate until goals are met or diminishing returns

## Success Criteria

Optimization is successful when:
- [ ] Target performance goals achieved
- [ ] No functional regressions
- [ ] No significant performance regressions in other areas
- [ ] All tests passing
- [ ] Improvements are reproducible
- [ ] Code is maintainable

## Output

This skill should produce:

1. **Performance Comparison Report** (`/tmp/performance_comparison.txt`)
2. **Optimized Metrics** (`/tmp/optimized_metrics.txt`)
3. **CPU Profile Comparison** (`/tmp/cpu_comparison.txt`)
4. **Decision**: PASS/FAIL/ITERATE
5. **Updated Git Commit** with benchmark results

## Final Report Template

```
Performance Optimization Summary
================================

Optimization: [Description]
Date: [Date]
Branch: [Branch name]
Commit: [Commit hash]

Baseline Metrics:
- Write QPS: 50,000
- Query QPS: 5,000
- Latency P99: 200ms
- CPU Hotspot: function_name (35%)

Optimized Metrics:
- Write QPS: 82,500 (+65%)
- Query QPS: 8,000 (+60%)
- Latency P99: 80ms (-60%)
- CPU Hotspot: function_name (17.5%, -50%)

Status: ✓ GOALS MET

All functional tests passing.
No regressions detected.
Ready for code review and merge.
```

## Next Steps

After verification:
- If successful: Code review and merge
- If not meeting goals: Return to **tsdb-perf-profiling** and iterate
- If regressions: Return to **tsdb-perf-fix** and adjust approach

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-perf-verify version=1.0.0 author=beryl`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
