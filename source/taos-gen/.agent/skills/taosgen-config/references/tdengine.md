# TDengine Configuration Reference

Complete reference for generating TDengine performance test configurations.

## TDengine Connection Configuration

```yaml
tdengine:
  dsn: taos+ws://root:taosdata@127.0.0.1:6041/database_name
  drop_if_exists: true
  props: precision 'us' vgroups 4 replica 1 keep 3650
  pool:
    enabled: true
    max_size: 10
    min_size: 2
    timeout: 3000
```

### Parameters

- **dsn**: Connection string
  - Format: `protocol://user:password@host:port/database`
  - Protocols: `taos+ws` (WebSocket), `taos` (native)
  - Default: `taos+ws://root:taosdata@localhost:6041/tsbench`

- **drop_if_exists**: Boolean - recreate database if exists

- **props**: Database properties (space-separated)
  - `precision`: Timestamp precision ('ms', 'us', 'ns')
  - `vgroups`: Number of vgroups (affects write parallelism, typically 4-32)
  - `replica`: Replication factor (1 or 3)
  - `keep`: Data retention period in days

- **pool**: Connection pooling (optional, improves performance)
  - `enabled`: Enable connection pool
  - `max_size`: Maximum connections (typically 10-20)
  - `min_size`: Minimum connections
  - `timeout`: Connection timeout in milliseconds, default: 1000

## TDengine Actions

### tdengine/create-database

Creates the database specified in the DSN.

```yaml
- uses: tdengine/create-database
  with:
    checkpoint:
      enabled: true
      interval_sec: 10
```

**Parameters:**

- **checkpoint** (optional): Enable write interruption/recovery functionality
  - `enabled`: Boolean - whether to enable checkpoint
  - `interval_sec`: Integer - interval for storing write progress, in seconds

### tdengine/create-super-table

Creates the super table based on schema definition.

```yaml
- uses: tdengine/create-super-table
  with:
    schema:
      # Override global schema if needed
```

**Parameters:**

- **schema** (optional): Override global schema configuration for this action. By default uses the global `schema` section.

### tdengine/create-child-table

Creates child tables in batches.

```yaml
- uses: tdengine/create-child-table
  with:
    schema:
      # Override global schema if needed
    batch:
      size: 1000
      concurrency: 10
```

**Parameters:**

- **schema** (optional): Override global schema configuration for this action
- **batch**: Batch creation control
  - `size`: Number of tables per batch, default: 1000
  - `concurrency`: Parallel batch creation threads, default: 10

### tdengine/insert

Inserts data into tables.

```yaml
- uses: tdengine/insert
  with:
    concurrency: 8
    format: stmt
    auto_create_table: false
    time_interval:
      enabled: true
      interval_strategy: literal
      wait_strategy: sleep
    checkpoint:
      enabled: false
      interval_sec: 5
    failure_handling:
      max_retries: 3
      retry_interval_ms: 1000
      on_failure: exit
```

**Parameters:**

- **concurrency**: Number of parallel write threads (typically 8-20, based on CPU cores)

- **format**: Write format
  - `stmt`: Prepared statement (faster, recommended)
  - `sql`: SQL text

- **auto_create_table**: Boolean - create tables automatically during insert (optional, default: `false`)
  - Set to `true` only when explicitly requested for convenience
  - **Performance note**: Auto-creating tables during insert is SLOWER than pre-creating tables
  - When `false` (default), generate a separate `tdengine/create-child-table` step before insert
  - When `true`, skip `create-child-table` step (convenient but lower performance)
  - Requires tags to be defined in schema

- **time_interval**: Control write timing (optional)
  - `enabled`: Boolean - whether to enable time interval control, default: false
  - `interval_strategy`: String - timing strategy, options:
    - `fixed`: Fixed interval between batches
    - `first_to_first`: First row time of this batch - first row time of last batch
    - `last_to_first`: First row time of this batch - last row time of last batch
    - `literal`: Send based on first row timestamp, simulating real-time data
  - `fixed_interval`: Object (required if interval_strategy = fixed)
    - `base_interval`: Integer - fixed interval value in milliseconds, default: 1000
  - `dynamic_interval`: Object (for first_to_first / last_to_first strategies)
    - `min_interval`: Integer - minimum interval threshold in ms, default: -1 (inactive)
    - `max_interval`: Integer - maximum interval threshold in ms, default: -1 (inactive)
  - `wait_strategy`: String - wait strategy, options: 'sleep' (CPU-friendly) or 'busy_wait' (lower latency)

- **checkpoint**: Enable write interruption/recovery (optional)
  - `enabled`: Boolean - whether to enable checkpoint
  - `interval_sec`: Integer - interval for storing progress, in seconds

- **failure_handling**: Error handling behavior (optional)
  - `max_retries`: Retry attempts per batch (typically 3-5)
  - `retry_interval_ms`: Wait time between retries
  - `on_failure`: 'exit' (stop on error) or 'skip' (continue)

## Complete Examples

### Example 1: Basic Performance Test (Optimal Performance - Default)

High-throughput benchmark using pre-created tables (best performance).

```yaml
tdengine:
  dsn: taos+ws://root:taosdata@127.0.0.1:6041/test
  drop_if_exists: true
  props: precision 'ms' vgroups 8
  pool:
    enabled: true
    max_size: 20
    min_size: 5

schema:
  name: sensors
  tbname:
    prefix: sensor
    count: 50000
    from: 1
  columns:
    - name: ts
      type: timestamp
      start: now + 10s
      precision: ms
      step: 1000
    - name: temperature
      type: float
      min: -20.0
      max: 50.0
    - name: humidity
      type: float
      min: 0.0
      max: 100.0
  tags:
    - name: region
      type: int
      min: 1
      max: 5
  generation:
    interlace: 1
    rows_per_table: 500
    rows_per_batch: 5000

jobs:
  setup-and-insert:
    steps:
      - uses: tdengine/create-super-table
      - uses: tdengine/create-child-table
        with:
          batch:
            size: 1000
            concurrency: 10
      - uses: tdengine/insert
        with:
          concurrency: 16
          format: stmt

```

**Key points:**
- Pre-creates child tables using `create-child-table` for optimal performance (default)
- High concurrency (16 threads) for maximum throughput
- `stmt` format for best performance
- 8 vgroups for good write parallelism

### Example 2: Multi-Stage Performance Test (Pre-create Tables with Convenience Alternative)

Multi-job workflow showing both optimal performance (default) and convenience mode alternatives.

```yaml
tdengine:
  dsn: taos+ws://root:taosdata@127.0.0.1:6041/benchmark
  drop_if_exists: true
  props: precision 'us' vgroups 8

schema:
  name: meters
  tbname:
    prefix: d
    count: 100000
    from: 0
  columns:
    - name: ts
      type: timestamp
      start: now
      precision: us
      step: 1
    - name: current
      type: float
      min: 0
      max: 100
    - name: voltage
      type: int
      min: 200
      max: 240
    - name: phase
      type: float
      expr: _i * math.pi % 180
  tags:
    - name: groupid
      type: int
      min: 1
      max: 10
    - name: location
      type: binary(24)
      values:
        - Beijing
        - Shanghai
        - Shenzhen
        - Guangzhou
  generation:
    interlace: 1
    rows_per_table: 1000
    rows_per_batch: 10000

jobs:
  create-tables:
    needs: [setup]
    steps:
      - uses: tdengine/create-super-table
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
          format: stmt
          # auto_create_table: false  # Default - pre-created tables for better performance

```

**Key points:**
- Separate table creation step for optimal performance (default behavior)
- Connection pooling enabled for efficiency
- Batch table creation with 1000 tables per batch

**Alternative - Convenience Mode (only when explicitly requested):**
```yaml
jobs:
  quick-setup:
    steps:
      - uses: tdengine/create-super-table
      - uses: tdengine/insert
        with:
          concurrency: 8
          format: stmt
          auto_create_table: true  # Only when user explicitly requests convenience over performance
```

### Example 3: Long-Running Test with Checkpoint (Convenience Mode)

Resumable test for large data volumes using auto-create (convenience mode - only use when explicitly requested).

```yaml
tdengine:
  dsn: taos+ws://root:taosdata@127.0.0.1:6041/longtest
  drop_if_exists: false
  props: precision 'us' vgroups 16

schema:
  name: devices
  tbname:
    prefix: dev
    count: 1000000
    from: 0
  columns:
    - name: ts
      type: timestamp
      start: now
      precision: us
      step: 1
    - name: value
      type: double
      min: 0
      max: 1000
  tags:
    - name: type
      type: int
      min: 1
      max: 100
  generation:
    interlace: 1
    rows_per_table: 10000
    rows_per_batch: 50000

jobs:
  insert-data:
    steps:
      - uses: tdengine/create-super-table
      # Note: Using auto_create_table for convenience (slower than pre-creating)
      # Only use this mode when explicitly requested
      - uses: tdengine/insert
        with:
          concurrency: 20
          format: stmt
          auto_create_table: true  # Convenience mode - slower performance
          checkpoint:
            enabled: true
            interval_sec: 30
          failure_handling:
            max_retries: 5
            retry_interval_ms: 2000
            on_failure: skip
```

**Key points:**
- **Convenience Mode**: Uses `auto_create_table: true` (only when explicitly requested)
- For better performance, use separate `create-child-table` step (Example 1 or 2)
- Checkpoint enabled for resumability (supports `enabled` and `interval_sec`)
- Large batch size (50000) for efficiency
- Failure handling with retries
- `drop_if_exists: false` to preserve data on restart

### Example 4: CSV Data Import

Import data from CSV files into TDengine.

```yaml
tdengine:
  dsn: taos+ws://root:taosdata@127.0.0.1:6041/import
  drop_if_exists: true
  props: precision 'ms' vgroups 4

schema:
  name: meters
  from_csv:
    tags:
      file_path: /data/tags.csv
      tbname_index: 0
      exclude_indices: []
    columns:
      file_path: /data/timeseries.csv
      tbname_index: 0
      timestamp_index: 1
      timestamp_precision: ms
      timestamp_offset:
        offset_type: relative
        value: +1h
      repeat_read: false
  columns:
    - name: ts
      type: timestamp
      precision: ms
    - name: current
      type: float
    - name: voltage
      type: int
    - name: phase
      type: float
  tags:
    - name: groupid
      type: int
    - name: location
      type: binary(24)
  generation:
    interlace: 1
    rows_per_table: -1
    rows_per_batch: 10000

jobs:
  import-data:
    steps:
      - uses: tdengine/create-super-table
      - uses: tdengine/create-child-table
        with:
          batch:
            size: 500
            concurrency: 8
      - uses: tdengine/insert
        with:
          concurrency: 8
          format: stmt
```

**Key points:**
- Uses `from_csv` to load data from files
- Timestamp offset adjusts CSV timestamps
- `rows_per_table: -1` means read all rows from CSV
- Pre-creates child tables for optimal performance (default behavior)
- See references/schema.md for detailed CSV configuration

## Performance Tuning

**For maximum throughput:**
- Pre-create child tables using `create-child-table` step (default `auto_create_table: false`)
- Use `format: stmt`
- Set `rows_per_batch: 20000-50000`
- Set `concurrency: 12-20` (based on CPU cores)
- Set `vgroups` to 8-32 (higher for more tables)
- Enable connection pooling
- **Note**: Avoid `auto_create_table: true` for maximum performance (only use for convenience)

**For reliability:**
- Enable checkpoints for long tests
- Configure failure handling with retries
- Use `on_failure: skip` to continue on errors

**For realistic timing:**
- Use `time_interval` with appropriate strategy
- Set `interlace: 1` for round-robin writes
- Adjust `step` in timestamp column for desired frequency

## Common Patterns

**Pattern 1: Optimal Performance (Default - Pre-create tables)**
```yaml
jobs:
  setup-and-test:
    steps:
      - uses: tdengine/create-super-table
      - uses: tdengine/create-child-table
        with:
          batch:
            size: 1000
            concurrency: 10
      - uses: tdengine/insert
        with:
          concurrency: 16
          # auto_create_table: false  # Default - best performance
```

**Pattern 2: Quick Setup (Convenience Mode - only when explicitly requested)**
```yaml
jobs:
  quick-benchmark:
    steps:
      - uses: tdengine/create-super-table
      - uses: tdengine/insert
        with:
          concurrency: 16
          auto_create_table: true  # Only when user explicitly requests convenience over performance
```

**Pattern 3: Multi-stage workflow**
```yaml
jobs:
  setup:
    steps:
      - uses: tdengine/create-database
      - uses: tdengine/create-super-table
      - uses: tdengine/create-child-table
        with:
          batch:
            size: 1000
            concurrency: 10

  load-data:
    needs: [setup]
    steps:
      - uses: tdengine/insert
        with:
          concurrency: 12
```
