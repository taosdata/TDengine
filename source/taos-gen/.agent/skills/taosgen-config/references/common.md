# Common Concepts and Best Practices

General concepts, job structure, and best practices for taosgen configurations.

## Job Structure

Jobs define the workflow. Each job contains steps that execute actions.

```yaml
jobs:
  job-name:
    needs: [dependency-job1, dependency-job2]
    steps:
      - uses: action-name
        with:
          parameter: value
      - uses: another-action
        with:
          parameter: value
```

### Job Parameters

- **Job Key**: Unique key name in the jobs list, used for internal reference and dependency management
  - Example: `insert-data:`, `create-tables:`

- **name** (optional): Display name for the job, used in logs and UI
  - Example: `name: "Data Insertion Job"`

- **needs**: List of job identifiers this job depends on
  - Creates DAG (Directed Acyclic Graph) execution flow
  - Empty list or omit if no dependencies
  - Example: `needs: [create-tables]`

- **steps**: List of one or more steps executed in order, defining the job's operation flow

### Step Structure

Each step represents a basic operation unit and includes:

- **name** (optional): Display name for the step, used in logs and UI
  - Example: `name: "Create Super Table"`

- **uses**: Action path or identifier to indicate which operation module to call
  - Examples: `tdengine/insert`, `mqtt/publish`, `kafka/produce`

- **with**: Map (dictionary) of parameters passed to the action. Parameter content varies by action type.
  - Optional - omit if action requires no parameters

## Job Patterns

### Pattern 1: Single Job

Simple workflow with one job.

```yaml
jobs:
  insert-data:
    steps:
      - uses: tdengine/create-super-table
      - uses: tdengine/create-child-table
      - uses: tdengine/insert
        with:
          concurrency: 8
```

### Pattern 2: Multi-Stage with Dependencies

Multiple jobs with execution order.

```yaml
jobs:
  setup:
    steps:
      - uses: tdengine/create-database
      - uses: tdengine/create-super-table

  create-tables:
    needs: [setup]
    steps:
      - uses: tdengine/create-child-table
        with:
          batch:
            size: 1000
            concurrency: 10

  insert-data:
    needs: [create-tables]
    steps:
      - uses: tdengine/insert
        with:
          concurrency: 16
```

### Pattern 3: Parallel Jobs

Multiple independent jobs that can run in parallel.

```yaml
jobs:
  insert-historical:
    steps:
      - uses: tdengine/create-super-table
      - uses: tdengine/insert
        with:
          concurrency: 8

  publish-mqtt:
    steps:
      - uses: mqtt/publish
        with:
          concurrency: 4
```

### Pattern 4: Sequential Phases

Historical data followed by real-time simulation.

```yaml
jobs:
  load-history:
    steps:
      - uses: tdengine/create-super-table
      - uses: tdengine/insert
        with:
          concurrency: 16

  simulate-realtime:
    needs: [load-history]
    steps:
      - uses: tdengine/insert
        with:
          concurrency: 4
          time_interval:
            enabled: true
            interval_strategy: literal
            wait_strategy: sleep
```

## Configuration Best Practices

### Performance Optimization

**For maximum throughput:**
- TDengine: Use `format: stmt`, high concurrency (12-20), large batches (20000-50000)
- MQTT: Increase `records_per_message` (100-500), use QoS 0
- Kafka: Use `acks: 0`, fast compression (lz4/snappy), high batching (500-1000)
- Set `rows_per_batch` to 10000-50000
- Enable connection pooling (TDengine)
- Use `auto_create_table: true` only when convenience is preferred over performance (default is `false` for better performance)

**For low latency:**
- Smaller batches (1000-5000)
- Lower concurrency (4-8)
- MQTT: `records_per_message: 1`, QoS 0
- Kafka: `records_per_message: 1`, `acks: 0`
- Use `time_interval` for realistic timing

**For reliability:**
- Enable checkpoints for long-running tests
- Configure failure handling with retries
- TDengine: Use `format: stmt` with retries
- MQTT: Use QoS 1 or 2
- Kafka: Use `acks: all`

### Data Generation

**Realistic patterns:**
- Use expressions with sine/cosine for periodic patterns
- Add random noise to expressions: `base + pattern + math.random(-noise, noise)`
- Use `_last` for random walk patterns
- Combine multiple patterns: `trend + seasonal + noise`
- Set `num_cached_batches: 0` for lower memory and more realistic real-time generation

**Performance:**
- Use `tables_reuse_data: true` for testing (faster, less memory, default)
- Use positive `num_cached_batches` to pre-generate batches (default, faster, higher memory)
- Use `interlace: 1` for realistic time-series patterns

**Timestamp configuration:**
- Match precision to use case (ms for most, us/ns for high-frequency)
- Use `start: now` for real-time simulation
- Use `start: now - Xh` for historical data
- Set `step` based on data frequency

### Workflow Design

**Separate concerns:**
- Job 1: Database and table creation
- Job 2: Data insertion
- Use dependencies to enforce order

**Use meaningful names:**
- Jobs: `setup`, `load-data`, `simulate-realtime`
- Tables: descriptive prefixes (`sensor`, `meter`, `device`)

**Enable checkpoints for long tests:**
```yaml
checkpoint:
  enabled: true
  interval_sec: 30
```

**Configure failure handling:**
```yaml
failure_handling:
  max_retries: 3
  retry_interval_ms: 1000
  on_failure: skip  # or exit
```

## Common Scenarios

### Scenario 1: Quick Performance Benchmark

Goal: Test maximum write throughput.

**Configuration:**
- High concurrency (16-20)
- Large batches (20000-50000)
- Pre-create tables
- No checkpoints or special timing

**Example:**
```yaml
jobs:
  benchmark:
    steps:
      - uses: tdengine/create-super-table
      - uses: tdengine/create-child-table
      - uses: tdengine/insert
        with:
          concurrency: 20
          format: stmt
```

### Scenario 2: Realistic Load Test

Goal: Simulate realistic production workload.

**Configuration:**
- Moderate concurrency (8-12)
- Realistic data patterns (expressions)
- Time intervals for pacing
- Checkpoints for resumability

**Example:**
```yaml
jobs:
  load-test:
    steps:
      - uses: tdengine/create-super-table
      - uses: tdengine/create-child-table
      - uses: tdengine/insert
        with:
          concurrency: 12
          format: stmt
          time_interval:
            enabled: true
            interval_strategy: literal
            wait_strategy: sleep
          checkpoint:
            enabled: true
            interval_sec: 30
```

### Scenario 3: Data Migration

Goal: Import historical data from CSV.

**Configuration:**
- CSV data source
- Moderate concurrency
- Large batches for efficiency
- Failure handling for robustness

**Example:**
```yaml
jobs:
  migrate:
    steps:
      - uses: tdengine/create-super-table
      - uses: tdengine/create-child-table
        with:
          batch:
            size: 1000
            concurrency: 10
      - uses: tdengine/insert
        with:
          concurrency: 8
          format: stmt
          failure_handling:
            max_retries: 5
            retry_interval_ms: 2000
            on_failure: skip
```

### Scenario 4: Multi-Protocol Testing

Goal: Test multiple systems simultaneously.

**Configuration:**
- Separate jobs for each protocol
- No dependencies (parallel execution)
- Appropriate settings for each system

**Example:**
```yaml
jobs:
  test-tdengine:
    steps:
      - uses: tdengine/create-super-table
      - uses: tdengine/insert
        with:
          concurrency: 12

  test-mqtt:
    steps:
      - uses: mqtt/publish
        with:
          concurrency: 8
          qos: 0

  test-kafka:
    steps:
      - uses: kafka/produce
        with:
          concurrency: 10
          acks: 1
```

## Troubleshooting

### Low Throughput

**Symptoms:** Write rate lower than expected

**Solutions:**
- Increase concurrency
- Increase batch size
- Use faster format (stmt for TDengine)
- Check target system performance (CPU, disk, network)
- Reduce compression or use faster algorithm
- Increase vgroups (TDengine)
- Increase topic partitions (Kafka)

### High Memory Usage

**Symptoms:** taosgen consuming too much memory

**Solutions:**
- Reduce `rows_per_batch`
- Set `num_cached_batches: 0` (lower memory, more realistic real-time generation)
- Use `tables_reuse_data: true`
- Reduce concurrency
- Process fewer tables at once

### Connection Errors

**Symptoms:** Connection timeouts or failures

**Solutions:**
- Enable connection pooling (TDengine)
- Reduce concurrency
- Check network connectivity
- Verify credentials and permissions
- Check target system connection limits

### Data Loss

**Symptoms:** Not all data written successfully

**Solutions:**
- Enable checkpoints
- Configure failure handling with retries
- Use higher acknowledgment levels (MQTT QoS, Kafka acks)
- Check target system logs for errors
- Monitor disk space on target system

## Performance Tuning Guidelines

### Concurrency Settings

| Tables | Recommended Concurrency |
|--------|------------------------|
| < 1,000 | 4-8 |
| 1,000 - 10,000 | 8-12 |
| 10,000 - 100,000 | 12-16 |
| > 100,000 | 16-20 |

Adjust based on:
- CPU cores available
- Target system capacity
- Network bandwidth
- Memory constraints

### Batch Size Settings

| Use Case | Recommended Batch Size |
|----------|----------------------|
| Low latency | 1,000 - 5,000 |
| Balanced | 10,000 - 20,000 |
| High throughput | 20,000 - 50,000 |

Larger batches:
- ✅ Better throughput
- ✅ More efficient
- ❌ Higher memory usage
- ❌ Higher latency

### Interlace Settings

| Pattern | Interlace Value | Use Case |
|---------|----------------|----------|
| Round-robin | 1 | Realistic time-series, even distribution |
| Small chunks | 10-100 | Balance between realism and cache locality |
| Large chunks | 1000+ | Maximum cache locality, less realistic |

## Configuration Checklist

Before running taosgen, verify:

**Connection:**
- [ ] DSN/URI is correct
- [ ] Credentials are valid
- [ ] Target system is accessible
- [ ] Database/topic exists or will be created

**Schema:**
- [ ] Column types match generation methods
- [ ] Timestamp precision is consistent
- [ ] Tag definitions are appropriate
- [ ] Table count and naming are correct

**Performance:**
- [ ] Concurrency is appropriate for system
- [ ] Batch size is optimized
- [ ] Memory usage is acceptable
- [ ] Checkpoints enabled for long tests

**Data:**
- [ ] Generation methods produce expected values
- [ ] Expressions are syntactically correct
- [ ] CSV files exist and are readable (if using CSV)
- [ ] Timestamp ranges are appropriate

**Jobs:**
- [ ] Dependencies are correct
- [ ] Actions are in proper order
- [ ] Parameters are valid
- [ ] Failure handling is configured

## Running taosgen

**Basic usage:**
```bash
taosgen -c config.yaml
```

**With command-line overrides:**
```bash
taosgen -c config.yaml -h localhost -P 6030 -u root -p password
```

**View help:**
```bash
taosgen --help
```

**Check version:**
```bash
taosgen --version
```

## Monitoring Progress

taosgen outputs progress information:
- Current job and step
- Rows written
- Write rate (rows/second)
- Elapsed time

**Example output:**
```
[2024-01-15 10:30:00] Job: insert-data, Step: tdengine/insert
[2024-01-15 10:30:05] Progress: 1000000/10000000 rows (10.0%), Rate: 200000 rows/s
[2024-01-15 10:30:10] Progress: 2000000/10000000 rows (20.0%), Rate: 200000 rows/s
```

Monitor target system as well:
- TDengine: Check taosd logs, monitor CPU/disk/memory
- MQTT: Check broker logs, monitor connections
- Kafka: Check broker metrics, monitor lag
