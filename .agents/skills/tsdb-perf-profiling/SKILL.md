---
name: tsdb-perf-profiling
description: "Use perf, google-perftools and other profiling tools to capture CPU, memory, and lock performance data. Generate flame graphs and analysis reports. Keywords: perf, profiling, flame graph, CPU, memory, google-perftools"
metadata:
  author: beryl
  version: 1.0.0
  owner_team: engine
---

# Performance Profiling

## Quick Start

This skill captures performance data using various profiling tools to identify bottlenecks.

## Prerequisites

- Performance scenario already set up (see tsdb-perf-scenario-setup)
- taosd running with test workload
- Profiling tools installed

## Install Profiling Tools

```bash
# Install perf
apt-get install -y linux-tools-common linux-tools-generic linux-tools-$(uname -r)

# Install google-perftools
apt-get install -y google-perftools libgoogle-perftools-dev

# Install FlameGraph tools
cd /tmp
git clone https://github.com/brendangregg/FlameGraph.git
export PATH=$PATH:/tmp/FlameGraph
```

## CPU Profiling with perf

### Method 1: Record CPU Profile

```bash
# Get taosd PID
TAOSD_PID=$(pgrep taosd)

# Record CPU profile for 30 seconds
perf record -F 99 -p $TAOSD_PID -g -- sleep 30

# Generate report
perf report -n --stdio > /tmp/perf_report.txt

# Generate flame graph
perf script | /tmp/FlameGraph/stackcollapse-perf.pl | /tmp/FlameGraph/flamegraph.pl > /tmp/cpu_flamegraph.svg
```

### Method 2: Real-time CPU Monitoring

```bash
# Monitor top functions in real-time
perf top -p $(pgrep taosd) -g

# Record with call graph
perf record -F 99 -p $(pgrep taosd) -g --call-graph dwarf -- sleep 60
```

### Method 3: Event-based Profiling

```bash
# Profile cache misses
perf record -e cache-misses -p $(pgrep taosd) -g -- sleep 30

# Profile branch mispredictions
perf record -e branch-misses -p $(pgrep taosd) -g -- sleep 30

# Profile page faults
perf record -e page-faults -p $(pgrep taosd) -g -- sleep 30
```

## Memory Profiling with google-perftools

### Heap Profiling

```bash
# Set environment variables
export HEAPPROFILE=/tmp/taosd_heap
export HEAP_PROFILE_ALLOCATION_INTERVAL=104857600  # 100MB

# Restart taosd with heap profiling
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libtcmalloc.so.4 \
  HEAPPROFILE=/tmp/taosd_heap \
  /root/workspace/TDinternal/debug/build/bin/taosd -c /etc/taos &

# Run workload
/tmp/perf_write.sh
/tmp/perf_query.sh

# Analyze heap profile
pprof --text /root/workspace/TDinternal/debug/build/bin/taosd /tmp/taosd_heap.*.heap > /tmp/heap_report.txt
pprof --pdf /root/workspace/TDinternal/debug/build/bin/taosd /tmp/taosd_heap.*.heap > /tmp/heap_profile.pdf
```

### Memory Growth Analysis

```bash
# Compare heap snapshots
pprof --base=/tmp/taosd_heap.0001.heap \
  --text /root/workspace/TDinternal/debug/build/bin/taosd \
  /tmp/taosd_heap.0010.heap > /tmp/heap_growth.txt
```

## CPU Profiling with google-perftools

```bash
# CPU profiling
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libprofiler.so.0 \
  CPUPROFILE=/tmp/taosd_cpu.prof \
  /root/workspace/TDinternal/debug/build/bin/taosd -c /etc/taos &

# Run workload
/tmp/perf_query.sh

# Stop taosd to flush profile
pkill taosd

# Analyze CPU profile
pprof --text /root/workspace/TDinternal/debug/build/bin/taosd /tmp/taosd_cpu.prof > /tmp/cpu_report.txt
pprof --pdf /root/workspace/TDinternal/debug/build/bin/taosd /tmp/taosd_cpu.prof > /tmp/cpu_profile.pdf
```

## Lock Profiling

### Method 1: perf lock

```bash
# Record lock events
perf lock record -p $(pgrep taosd) -- sleep 30

# Analyze lock contention
perf lock report > /tmp/lock_report.txt
```

### Method 2: SystemTap (if available)

```bash
# Monitor mutex contention
stap -e 'probe process("/root/workspace/TDinternal/debug/build/bin/taosd").function("pthread_mutex_lock") {
  printf("Lock attempt at %s\n", probefunc())
}' > /tmp/lock_trace.txt
```

## I/O Profiling

```bash
# Monitor I/O operations
perf record -e block:block_rq_issue -p $(pgrep taosd) -g -- sleep 30
perf report -n --stdio > /tmp/io_report.txt

# Detailed I/O stats
iostat -x 1 30 > /tmp/iostat.txt

# Per-process I/O
pidstat -d -p $(pgrep taosd) 1 30 > /tmp/pidstat_io.txt
```

## System-wide Profiling

```bash
# Capture system-wide CPU profile
perf record -F 99 -a -g -- sleep 30
perf report -n --stdio > /tmp/system_perf_report.txt

# Monitor all processes
top -b -n 30 -d 1 > /tmp/top_output.txt

# Memory usage over time
vmstat 1 30 > /tmp/vmstat.txt
```

## Profiling During Workload

```bash
# Start profiling and run workload simultaneously
TAOSD_PID=$(pgrep taosd)

# Start perf recording in background
perf record -F 99 -p $TAOSD_PID -g -o /tmp/perf_workload.data -- sleep 60 &
PERF_PID=$!

# Run workload
/tmp/perf_write.sh
/tmp/perf_query.sh

# Wait for perf to finish
wait $PERF_PID

# Generate reports
perf report -i /tmp/perf_workload.data -n --stdio > /tmp/workload_report.txt
perf script -i /tmp/perf_workload.data | /tmp/FlameGraph/stackcollapse-perf.pl | /tmp/FlameGraph/flamegraph.pl > /tmp/workload_flamegraph.svg
```

## Quick Profiling Commands

### 30-second CPU profile

```bash
perf record -F 99 -p $(pgrep taosd) -g -- sleep 30 && perf report -n --stdio > /tmp/quick_perf.txt
```

### Generate flame graph

```bash
perf record -F 99 -p $(pgrep taosd) -g -- sleep 30 && \
perf script | /tmp/FlameGraph/stackcollapse-perf.pl | /tmp/FlameGraph/flamegraph.pl > /tmp/flame.svg
```

### Memory snapshot

```bash
pmap -x $(pgrep taosd) > /tmp/memory_map.txt
```

### Lock contention check

```bash
perf lock record -p $(pgrep taosd) -- sleep 10 && perf lock report > /tmp/locks.txt
```

## Execution Rules

- Always verify taosd is running before profiling
- Use appropriate sampling frequency (99 Hz is recommended for CPU)
- Profile for sufficient duration (30-60 seconds minimum)
- Generate both text reports and flame graphs
- Save all profiling data to /tmp/ directory
- Document the workload being profiled
- Keep profiling overhead in mind (typically 1-5%)

## Output Files

This skill generates:
- `/tmp/perf_report.txt` - CPU profiling text report
- `/tmp/cpu_flamegraph.svg` - CPU flame graph
- `/tmp/heap_report.txt` - Memory allocation report
- `/tmp/lock_report.txt` - Lock contention report
- `/tmp/io_report.txt` - I/O profiling report
- `/tmp/perf.data` - Raw perf data (can be analyzed later)

## Next Steps

After capturing performance data, proceed to:
- **tsdb-perf-analysis**: Analyze the profiling reports to identify bottlenecks

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-perf-profiling version=1.0.0 author=beryl`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
