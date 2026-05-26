---
name: tsdb-perf-analysis
description: "AI analyzes captured performance data files, identifies hotspot functions, analyzes call chains, and locates performance bottlenecks. Keywords: performance analysis, hotspot, flame graph, profiling data"
metadata:
  author: beryl
  version: 1.0.0
  owner_team: engine
---

# Performance Data Analysis

## Quick Start

This skill uses AI to analyze profiling data and identify performance bottlenecks.

## Prerequisites

- Profiling data captured (see tsdb-perf-profiling)
- Performance reports available in /tmp/

## Step 1: Read Profiling Reports

```bash
# List available profiling reports
ls -lh /tmp/*report.txt /tmp/*.svg /tmp/perf.data 2>/dev/null

# Display perf report summary
head -100 /tmp/perf_report.txt

# Display heap report summary
head -50 /tmp/heap_report.txt

# Display lock report
cat /tmp/lock_report.txt
```

## Step 2: Analyze CPU Hotspots

### Identify Top Functions

AI should analyze the perf report and identify:

1. **Top CPU consumers** (functions with highest overhead %)
2. **Call chains** (how these functions are called)
3. **Self time vs total time** (distinguish between direct cost and called functions)

Example analysis pattern:

```
From perf report, identify:
- Functions with >5% overhead
- Functions called frequently (high sample count)
- Deep call stacks indicating recursion
- Unexpected functions in hot path
```

### Key Metrics to Extract

```bash
# Extract top 20 functions by overhead
grep -A 20 "Overhead" /tmp/perf_report.txt | head -25

# Find specific function details
grep -A 10 "function_name" /tmp/perf_report.txt
```

## Step 3: Analyze Memory Issues

### Heap Allocation Analysis

AI should analyze heap reports for:

1. **Large allocations** (functions allocating most memory)
2. **Allocation frequency** (functions called many times)
3. **Memory growth** (comparing snapshots)
4. **Potential leaks** (allocations without corresponding frees)

Example analysis:

```
From heap report, identify:
- Functions allocating >10MB
- Allocation patterns (many small vs few large)
- Unexpected allocators
- Growth rate between snapshots
```

### Memory Commands

```bash
# Show top memory allocators
grep -E "^\s+[0-9]+\.[0-9]+%" /tmp/heap_report.txt | head -20

# Compare memory growth
if [ -f /tmp/heap_growth.txt ]; then
  cat /tmp/heap_growth.txt
fi
```

## Step 4: Analyze Lock Contention

### Lock Analysis Pattern

AI should identify:

1. **Contended locks** (locks with high wait time)
2. **Lock holders** (functions holding locks longest)
3. **Lock acquisition patterns** (frequency and duration)
4. **Deadlock potential** (circular dependencies)

```bash
# Show lock contention summary
grep -E "(contended|acquired)" /tmp/lock_report.txt | head -20
```

## Step 5: Flame Graph Analysis

### Visual Pattern Recognition

When analyzing flame graphs (/tmp/*.svg), AI should identify:

1. **Wide plateaus** - functions consuming significant CPU
2. **Tall towers** - deep call stacks, potential recursion
3. **Unexpected patterns** - functions that shouldn't be hot
4. **Missing optimizations** - known optimization opportunities

## Analysis Checklist

### CPU Performance

- [ ] Identify top 5 hotspot functions
- [ ] Analyze call chains for each hotspot
- [ ] Check for unexpected functions in hot path
- [ ] Identify optimization opportunities (loops, allocations, etc.)
- [ ] Estimate potential improvement for each issue

### Memory Performance

- [ ] Identify top memory allocators
- [ ] Check for excessive allocations
- [ ] Look for memory growth patterns
- [ ] Identify potential memory leaks
- [ ] Check for unnecessary copies

### Lock Performance

- [ ] Identify contended locks
- [ ] Measure lock hold times
- [ ] Check lock acquisition frequency
- [ ] Look for lock ordering issues
- [ ] Identify lock-free opportunities

### I/O Performance

- [ ] Check I/O wait time
- [ ] Identify synchronous I/O in hot path
- [ ] Look for small I/O operations
- [ ] Check for unnecessary fsync calls

## AI Analysis Template

When analyzing performance data, AI should provide:

### 1. Executive Summary

```
Performance Issue: [Brief description]
Primary Bottleneck: [CPU/Memory/Lock/I/O]
Estimated Impact: [High/Medium/Low]
Recommended Action: [Specific optimization]
```

### 2. Detailed Findings

```
Hotspot #1: function_name
- Overhead: X%
- Samples: N
- Call chain: caller1 -> caller2 -> function_name
- Issue: [Description of problem]
- Recommendation: [Specific fix]

Hotspot #2: ...
```

### 3. Prioritized Action Items

```
Priority 1 (High Impact):
- Fix function_x: Expected improvement 30%
- Optimize allocation in function_y: Expected improvement 20%

Priority 2 (Medium Impact):
- Reduce lock contention in function_z: Expected improvement 10%

Priority 3 (Low Impact):
- Minor optimizations: Expected improvement 5%
```

## Common Performance Patterns

### Pattern 1: Excessive String Operations

```
Symptoms:
- High CPU in strlen, strcmp, strcpy
- Many small allocations
- String manipulation in hot path

Solution:
- Use string views/references
- Pre-allocate buffers
- Cache string lengths
```

### Pattern 2: Lock Contention

```
Symptoms:
- High wait time in pthread_mutex_lock
- Many threads blocked
- Low CPU utilization despite load

Solution:
- Reduce lock scope
- Use finer-grained locks
- Consider lock-free structures
```

### Pattern 3: Memory Allocation Storm

```
Symptoms:
- High CPU in malloc/free
- Many small allocations
- Fragmentation

Solution:
- Object pooling
- Arena allocators
- Batch allocations
```

### Pattern 4: Cache Misses

```
Symptoms:
- High cache-miss events
- Poor data locality
- Random access patterns

Solution:
- Improve data layout
- Use cache-friendly structures
- Prefetching
```

## Execution Rules

- Read all available profiling reports
- Focus on functions with >5% overhead
- Consider both self time and total time
- Look for unexpected functions in hot path
- Provide specific, actionable recommendations
- Estimate potential improvement for each fix
- Prioritize by impact and implementation effort

## Output Format

After analyzing performance data, AI **must** generate a written analysis report file and save it to the issue workspace. This report serves as a permanent record of findings and the basis for optimization decisions.

### Generate Analysis Report File

```bash
# Save analysis report to the issue workspace
ISSUE_DIR="/root/ccdocs/${ISSUE_NAME}"
REPORT_FILE="${ISSUE_DIR}/results/perf_analysis_report.md"

cat > ${REPORT_FILE} << 'EOF'
# Performance Analysis Report

**Date**: YYYY-MM-DD
**Issue**: [Issue name]
**Profiling Duration**: Xs
**Workload**: [Description of workload during profiling]

## 1. Executive Summary

| Item | Value |
|------|-------|
| Primary Bottleneck | [CPU/Memory/Lock/I/O] |
| Estimated Impact | [High/Medium/Low] |
| Key Hotspot Function | [function_name (X%)] |
| Recommended Action | [Brief description] |

## 2. CPU Hotspot Analysis

### Top 5 Functions by Overhead

| Rank | Function | Self% | Total% | Samples | Module |
|------|----------|-------|--------|---------|--------|
| 1 | function_a | X% | Y% | N | taosd |
| 2 | function_b | X% | Y% | N | taosd |
| 3 | function_c | X% | Y% | N | libc |
| 4 | function_d | X% | Y% | N | taosd |
| 5 | function_e | X% | Y% | N | taosd |

### Call Chain Analysis

```
Hotspot #1: function_a
  Call chain: caller1 -> caller2 -> function_a
  Issue: [Description of why this is hot]
  Impact: X% of total CPU

Hotspot #2: function_b
  Call chain: caller3 -> caller4 -> function_b
  Issue: [Description]
  Impact: X% of total CPU
```

## 3. Lock Contention Analysis

| Lock | Contention Level | Holders | Wait Pattern |
|------|-----------------|---------|-------------|
| lock_name | High/Medium/Low | [functions] | [description] |

## 4. Memory Analysis

- Peak usage: X MB
- Top allocators: [list]
- Allocation frequency: [description]

## 5. I/O Analysis

- I/O wait time: X%
- Sync I/O in hot path: [yes/no, details]

## 6. Prioritized Action Items

### Priority 1 (High Impact, estimated improvement X%)
- **Target**: function_name (file:line)
- **Problem**: [specific issue]
- **Proposed Fix**: [specific optimization]
- **Risk**: [Low/Medium/High]

### Priority 2 (Medium Impact, estimated improvement X%)
- **Target**: function_name (file:line)
- **Problem**: [specific issue]
- **Proposed Fix**: [specific optimization]
- **Risk**: [Low/Medium/High]

### Priority 3 (Low Impact, estimated improvement X%)
- **Target**: function_name (file:line)
- **Problem**: [specific issue]
- **Proposed Fix**: [specific optimization]
- **Risk**: [Low/Medium/High]

## 7. Baseline Reference

- Baseline report: results/baseline_metrics.txt
- Perf data: results/perf.data
- Perf report: results/perf_report.txt
- Flame graph: results/flame_graph.svg
EOF

echo "Analysis report saved to: ${REPORT_FILE}"
```

### Report Requirements

The analysis report file **must** include:

1. **Executive Summary**: One-paragraph overview with primary bottleneck type and recommended action
2. **Hotspot List**: Top 5-10 functions with overhead percentages, sample counts, and call chains
3. **Lock/Memory/IO sections**: Filled in based on actual profiling data (mark N/A if not profiled)
4. **Prioritized Action Items**: Each item must have target function, file:line, problem description, proposed fix, and estimated improvement
5. **Baseline Reference**: Links to the raw profiling data files

AI should produce:

1. **Analysis Report File**: Saved to `${ISSUE_DIR}/results/perf_analysis_report.md` — this is the primary deliverable
2. **Summary Report**: Key findings and recommendations (printed to console)
3. **Hotspot List**: Top 10 performance issues with details
4. **Action Plan**: Prioritized list of optimizations
5. **Expected Impact**: Estimated improvement for each fix

## Next Steps

After analyzing performance data, proceed to:
- **tsdb-perf-code-locate**: Examine the source code of identified hotspot functions
## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-perf-analysis version=0.1.0 author=beryl`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->

