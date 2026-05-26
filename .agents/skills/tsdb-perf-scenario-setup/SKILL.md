---
name: tsdb-perf-scenario-setup
description: "Setup performance test scenario with taosd, generate test data using taosBenchmark/taosgen, and create write/query scripts to reproduce performance issues. Keywords: performance test, taosBenchmark, taosgen, scenario setup"
metadata:
  author: beryl
  version: 1.0.0
  owner_team: engine
---

# Performance Scenario Setup

## Quick Start

This skill helps you build a reproducible performance test scenario for TDengine.

## Prerequisites

- TDengine compiled and installed
- taosBenchmark or taosgen available
- Test scripts directory prepared

## Step 1: Start taosd

```bash
# Stop existing taosd if running
pkill -9 taosd

# Clean data directory (optional, for fresh start)
rm -rf /var/lib/taos/*

# Start taosd
/root/workspace/TDinternal/debug/build/bin/taosd -c /etc/taos &

# Wait for taosd to be ready
sleep 3

# Verify taosd is running
ps aux | grep taosd
```

## Step 2: Generate Test Data

### Option A: Using taosBenchmark

```bash
# Basic write test - 10 tables, 10000 records each
taosBenchmark -y \
  -d test_db \
  -t 10 \
  -n 10000 \
  -T 4 \
  -b "sensor_id,temperature,humidity,voltage" \
  -w "INT,FLOAT,FLOAT,FLOAT"

# High-volume write test - 1000 tables, 100000 records each
taosBenchmark -y \
  -d perf_db \
  -t 1000 \
  -n 100000 \
  -T 16 \
  -b "device_id,value1,value2,value3,status" \
  -w "INT,DOUBLE,DOUBLE,DOUBLE,INT"
```

### Option B: Using taosgen

```bash
# Generate custom data pattern
taosgen --database perf_db \
  --tables 100 \
  --records 50000 \
  --threads 8 \
  --schema "ts TIMESTAMP,temperature FLOAT,humidity FLOAT,pressure FLOAT"
```

## Step 3: Create Write Script

Create a write script to reproduce the performance issue:

```bash
cat > /tmp/perf_write.sh << 'EOF'
#!/bin/bash

DB_NAME="perf_db"
TABLE_COUNT=1000
BATCH_SIZE=1000
THREADS=16

for i in $(seq 1 $TABLE_COUNT); do
  taos -s "INSERT INTO ${DB_NAME}.t${i} VALUES (now, $RANDOM, $RANDOM, $RANDOM);" &

  if [ $((i % THREADS)) -eq 0 ]; then
    wait
  fi
done

wait
echo "Write completed"
EOF

chmod +x /tmp/perf_write.sh
```

## Step 4: Create Query Script

Create a query script to reproduce the performance issue:

```bash
cat > /tmp/perf_query.sh << 'EOF'
#!/bin/bash

DB_NAME="perf_db"
QUERY_COUNT=1000

# Simple query
for i in $(seq 1 $QUERY_COUNT); do
  taos -s "SELECT * FROM ${DB_NAME}.meters LIMIT 100;" > /dev/null
done

# Aggregation query
for i in $(seq 1 100); do
  taos -s "SELECT AVG(temperature), MAX(humidity) FROM ${DB_NAME}.meters WHERE ts > now - 1h;" > /dev/null
done

# Complex query
for i in $(seq 1 50); do
  taos -s "SELECT tbname, COUNT(*), AVG(temperature) FROM ${DB_NAME}.meters WHERE ts > now - 1d GROUP BY tbname;" > /dev/null
done

echo "Query completed"
EOF

chmod +x /tmp/perf_query.sh
```

## Step 5: Reproduce Performance Issue and Collect Baseline

```bash
# Run write test and record baseline
echo "=== Write Baseline ===" | tee /tmp/baseline_metrics.txt
echo "Date: $(date)" >> /tmp/baseline_metrics.txt
echo "Branch: $(cd /root/workspace/TDinternal && git branch --show-current)" >> /tmp/baseline_metrics.txt
echo "Commit: $(cd /root/workspace/TDinternal && git rev-parse --short HEAD)" >> /tmp/baseline_metrics.txt
echo "" >> /tmp/baseline_metrics.txt

START_TIME=$(date +%s%N)
/tmp/perf_write.sh
END_TIME=$(date +%s%N)
WRITE_MS=$(( (END_TIME - START_TIME) / 1000000 ))
echo "Write Duration: ${WRITE_MS}ms" | tee -a /tmp/baseline_metrics.txt

# Run query test and record baseline
START_TIME=$(date +%s%N)
/tmp/perf_query.sh
END_TIME=$(date +%s%N)
QUERY_MS=$(( (END_TIME - START_TIME) / 1000000 ))
echo "Query Duration: ${QUERY_MS}ms" | tee -a /tmp/baseline_metrics.txt

# Monitor system resources
echo "" >> /tmp/baseline_metrics.txt
echo "=== System Resources ===" >> /tmp/baseline_metrics.txt
top -b -n 1 -p $(pgrep taosd) >> /tmp/baseline_metrics.txt

# Memory usage
echo "" >> /tmp/baseline_metrics.txt
echo "=== Memory Usage ===" >> /tmp/baseline_metrics.txt
pmap -x $(pgrep taosd) 2>/dev/null | tail -1 >> /tmp/baseline_metrics.txt

echo ""
echo "Baseline metrics saved to /tmp/baseline_metrics.txt"
cat /tmp/baseline_metrics.txt
```

**Important**: The baseline report must be saved before any optimization work begins. Copy it into the issue workspace for permanent record:

```bash
cp /tmp/baseline_metrics.txt /root/ccdocs/${ISSUE_NAME}/results/baseline_metrics.txt
```

## Common Scenarios

### Scenario 1: High Write Load

```bash
# Generate continuous write load
taosBenchmark -y -d write_test -t 500 -n 1000000 -T 32 -r 10000
```

### Scenario 2: Complex Query

```bash
# Create database with large dataset
taos -s "CREATE DATABASE IF NOT EXISTS query_test KEEP 365 DAYS 10;"
taosBenchmark -y -d query_test -t 1000 -n 500000 -T 16

# Run complex aggregation
taos -s "SELECT _wstart, AVG(current), MAX(voltage) FROM query_test.meters WHERE ts > now - 7d INTERVAL(1h) FILL(LINEAR);"
```

### Scenario 3: Concurrent Mixed Workload

```bash
# Start write workload in background
taosBenchmark -y -d mixed_test -t 200 -n 1000000 -T 8 &

# Run concurrent queries
for i in {1..10}; do
  /tmp/perf_query.sh &
done

wait
```

## Execution Rules

- Always verify taosd is running before generating data
- Use appropriate data volume based on the performance issue
- Save test scripts for reproducibility
- Document the exact scenario that triggers the performance problem
- Monitor system resources during test execution
- Keep test data consistent across optimization iterations

## Output

This skill should produce:
- Running taosd instance
- Test database with generated data
- Write script at `/tmp/perf_write.sh`
- Query script at `/tmp/perf_query.sh`
- **Baseline performance report at `/tmp/baseline_metrics.txt`** (including write/query duration, system resources, memory usage)
- Clear reproduction steps for the performance issue

## Next Steps

After setting up the scenario, proceed to:
- **tsdb-perf-profiling**: Capture performance data using profiling tools
## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-perf-scenario-setup version=0.1.0 author=beryl`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->

