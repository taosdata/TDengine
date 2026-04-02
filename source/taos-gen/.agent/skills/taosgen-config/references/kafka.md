# Kafka Configuration Reference

Complete reference for generating Kafka producer configurations.

## Kafka Connection Configuration

```yaml
kafka:
  bootstrap_servers: localhost:9092
  topic: topic-name
  client_id: taosgen
  rdkafka_options:
    security.protocol: SASL_SSL
    sasl.mechanism: PLAIN
    sasl.username: user
    sasl.password: password
```

### Parameters

- **bootstrap_servers**: Comma-separated broker addresses
  - Format: `host1:port1,host2:port2,host3:port3`
  - Example: `broker1:9092,broker2:9092,broker3:9092`
  - Default: `localhost:9092`

- **topic**: Target topic name
  - Must exist or auto-creation must be enabled on broker
  - All messages go to this topic

- **client_id**: Client identifier prefix
  - taosgen appends thread number to this prefix
  - Default: `taosgen`

- **rdkafka_options**: librdkafka configuration options (optional)
  - Any valid librdkafka configuration parameter
  - Common options:
    - `security.protocol`: `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl`
    - `sasl.mechanisms`: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `GSSAPI`
    - `sasl.username`: SASL username
    - `sasl.password`: SASL password
    - `ssl.ca.location`: CA certificate path
    - `ssl.certificate.location`: Client certificate path
    - `ssl.key.location`: Client key path

## Kafka Actions

### kafka/produce

Produces messages to Kafka topic.

```yaml
- uses: kafka/produce
  with:
    schema:  # Optional: override global schema
    concurrency: 8
    failure_handling:
      max_retries: 3
      retry_interval_ms: 1000
      on_failure: exit
    time_interval:
      enabled: true
      interval_strategy: literal
      wait_strategy: sleep
    key_pattern: "{table}"
    key_serializer: "string-utf8"
    value_serializer: "json"
    acks: "0"
    compression: "none"
    tbname_key: "table"
    records_per_message: 1
```

**Parameters:**

- **schema** (optional): Override global schema configuration for this action

- **concurrency**: Number of parallel producer threads
  - Typically 8-20 based on broker capacity
  - Each thread creates a separate Kafka producer
  - Default: `8`

- **failure_handling**: Error handling behavior (optional)
  - `max_retries`: Retry attempts per batch (typically 3-5)
  - `retry_interval_ms`: Wait time between retries
  - `on_failure`: `'exit'` (stop on error) or `'skip'` (continue)
  - See `tdengine/insert` for detailed failure handling documentation

- **time_interval**: Control publish timing (optional)
  - `enabled`: Boolean - whether to enable time interval control
  - `interval_strategy`: String - timing strategy
    - `'fixed'`: Fixed interval between batches
    - `'first_to_first'`: First row time of this batch - first row time of last batch
    - `'last_to_first'`: First row time of this batch - last row time of last batch
    - `'literal'`: Send based on first row timestamp, simulating real-time data
  - `fixed_interval`: Object (required if interval_strategy = fixed)
    - `base_interval`: Integer - fixed interval value in milliseconds
  - `dynamic_interval`: Object (for first_to_first / last_to_first strategies)
    - `min_interval`: Integer - minimum interval threshold in ms, default: -1 (inactive)
    - `max_interval`: Integer - maximum interval threshold in ms, default: -1 (inactive)
  - `wait_strategy`: String - wait strategy, options: `'sleep'` (CPU-friendly) or `'busy_wait'` (lower latency)
  - See `tdengine/insert` for detailed time_interval documentation

- **key_pattern**: Message key pattern (optional)
  - Supports placeholder syntax for dynamic key generation
  - Default: `"{table}"`
  - Placeholders:
    - `{table}`: Table name
    - `{column}`: Column value (replace 'column' with actual column name)

- **key_serializer**: Serialization method for message key (optional)
  - `"string-utf8"`: Treats as string, encodes to UTF-8 bytes (default)
  - `"int8"`, `"uint8"`, `"int16"`, `"uint16"`, `"int32"`, `"uint32"`, `"int64"`, `"uint64"`: Parses as integer, serializes in big-endian
  - For integer types, use single field placeholder only

- **value_serializer**: Serialization method for message value (optional)
  - `"json"`: JSON format (default)
  - `"influx"`: InfluxDB line protocol format

- **acks**: Producer acknowledgment setting
  - `"0"`: No acknowledgment (fire and forget, fastest)
  - `"1"`: Leader acknowledgment only (balanced)
  - `"all"`: All in-sync replicas (slowest, most reliable)
  - Default: `"0"`

- **compression**: Message compression type (optional)
  - `"none"`: No compression (default)
  - `"gzip"`: Good compression ratio, moderate CPU
  - `"snappy"`: Fast compression, lower ratio
  - `"lz4"`: Very fast, good ratio
  - `"zstd"`: Best compression, higher CPU

- **tbname_key**: Field name for table name in JSON output (optional)
  - Set to empty string `""` to exclude table name from output
  - Default: `"table"`

- **records_per_message**: Number of records per message
  - `1`: One record per message (lowest latency)
  - Higher values: Better throughput through batching
  - Typically 1-1000 depending on use case
  - Default: `1`

## Complete Examples

### Example 1: High-Throughput Load Test

Maximum throughput with batching and compression.

```yaml
kafka:
  bootstrap_servers: broker1:9092,broker2:9092,broker3:9092
  topic: benchmark-topic
  client_id: taosgen-benchmark

schema:
  name: meters
  tbname:
    prefix: meter
    count: 100000
    from: 0
  columns:
    - name: ts
      type: timestamp
      start: now
      precision: ms
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
      max: 100
    - name: location
      type: binary(24)
      values:
        - Region-A
        - Region-B
        - Region-C
        - Region-D
  generation:
    interlace: 1
    rows_per_table: 100
    rows_per_batch: 10000

jobs:
  produce-data:
    steps:
      - uses: kafka/produce
        with:
          concurrency: 20
          acks: 0
          records_per_message: 500
          compression: lz4
          queue_capacity: 1000
```

**Key points:**
- `acks: 0` for maximum throughput
- 500 records per message for efficient batching
- lz4 compression for speed
- High concurrency (20 threads)
- Large queue capacity for buffering

### Example 2: Reliable Data Ingestion

Guaranteed delivery with acknowledgments.

```yaml
kafka:
  bootstrap_servers: kafka.example.com:9092
  topic: sensor-data
  client_id: taosgen-reliable

schema:
  name: sensors
  tbname:
    prefix: sensor
    count: 10000
    from: 1
  columns:
    - name: ts
      type: timestamp
      start: now
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
    - name: pressure
      type: float
      min: 980.0
      max: 1050.0
  tags:
    - name: location
      type: binary(32)
      values:
        - Warehouse-1
        - Warehouse-2
        - Warehouse-3
    - name: sensor_type
      type: binary(16)
      values:
        - DHT22
        - BME280
        - SHT31
  generation:
    interlace: 1
    rows_per_table: 1000
    rows_per_batch: 5000

jobs:
  produce-data:
    steps:
      - uses: kafka/produce
        with:
          concurrency: 8
          acks: all
          records_per_message: 10
          compression: gzip
```

**Key points:**
- `acks: all` for guaranteed delivery
- Moderate batching (10 records)
- gzip compression for network efficiency
- Lower concurrency for reliability

### Example 3: Secure Production with SASL/SSL

Production setup with authentication and encryption.

```yaml
kafka:
  bootstrap_servers: secure-broker1:9093,secure-broker2:9093
  topic: production-data
  client_id: taosgen-secure
  rdkafka_options:
    security.protocol: SASL_SSL
    sasl.mechanism: SCRAM-SHA-256
    sasl.username: producer-user
    sasl.password: secure-password
    ssl.ca.location: /path/to/ca-cert.pem

schema:
  name: events
  tbname:
    prefix: event
    count: 50000
    from: 0
  columns:
    - name: ts
      type: timestamp
      start: now
      precision: ms
      step: 100
    - name: event_type
      type: int
      min: 1
      max: 20
    - name: value
      type: double
      min: 0
      max: 10000
    - name: status
      type: int
      values: [0, 1, 2, 3]
  tags:
    - name: source
      type: binary(32)
      values:
        - Application-A
        - Application-B
        - Application-C
  generation:
    interlace: 1
    rows_per_table: 500
    rows_per_batch: 5000

jobs:
  produce-events:
    steps:
      - uses: kafka/produce
        with:
          concurrency: 12
          acks: 1
          records_per_message: 50
          compression: snappy
```

**Key points:**
- SASL_SSL for authentication and encryption
- SCRAM-SHA-256 for secure authentication
- Balanced settings for production use
- snappy compression for good performance

### Example 4: Time-Series Metrics

IoT metrics with realistic patterns.

```yaml
kafka:
  bootstrap_servers: localhost:9092
  topic: iot-metrics
  client_id: taosgen-iot

schema:
  name: devices
  tbname:
    prefix: device
    count: 20000
    from: 1
  columns:
    - name: ts
      type: timestamp
      start: now
      precision: ms
      step: 5000
    - name: cpu_percent
      type: float
      expr: 30 + math.random() * 40 + math.sin(_i * 0.1) * 20
    - name: memory_mb
      type: int
      expr: math.floor(1024 + math.random() * 2048)
    - name: disk_io_read
      type: bigint
      expr: math.floor(1000000 + math.random() * 5000000)
    - name: disk_io_write
      type: bigint
      expr: math.floor(500000 + math.random() * 3000000)
    - name: network_in
      type: bigint
      expr: math.floor(10000000 + math.random() * 50000000)
    - name: network_out
      type: bigint
      expr: math.floor(5000000 + math.random() * 30000000)
  tags:
    - name: datacenter
      type: binary(16)
      values:
        - DC-US-EAST
        - DC-US-WEST
        - DC-EU
        - DC-ASIA
    - name: rack
      type: int
      min: 1
      max: 50
  generation:
    interlace: 1
    rows_per_table: 1000
    rows_per_batch: 10000

jobs:
  produce-metrics:
    steps:
      - uses: kafka/produce
        with:
          concurrency: 16
          acks: 1
          records_per_message: 100
          compression: lz4
```

**Key points:**
- Expression-based data for realistic patterns
- Multiple metrics per record
- Balanced throughput and reliability
- lz4 compression for time-series data

### Example 5: Simple Test

Minimal configuration for quick testing.

```yaml
kafka:
  bootstrap_servers: localhost:9092
  topic: test-topic

schema:
  name: test
  tbname:
    prefix: t
    count: 1000
    from: 0
  columns:
    - name: ts
      type: timestamp
      start: now
      precision: ms
      step: 1
    - name: value
      type: int
      min: 0
      max: 100
  tags:
    - name: group
      type: int
      min: 1
      max: 10
  generation:
    interlace: 1
    rows_per_table: 100
    rows_per_batch: 1000

jobs:
  produce-data:
    steps:
      - uses: kafka/produce
        with:
          concurrency: 4
          acks: 1
          records_per_message: 1
```

**Key points:**
- Minimal configuration
- Default settings for most parameters
- Good for quick testing

## Message Format

taosgen produces messages in JSON format. Each message contains one or more records:

**Single record (records_per_message: 1):**
```json
{
  "table": "meter0001",
  "ts": 1704067200000,
  "current": 45.3,
  "voltage": 220,
  "phase": 1.57,
  "groupid": 5,
  "location": "Region-A"
}
```

**Multiple records (records_per_message: 3):**
```json
[
  {
    "table": "meter0001",
    "ts": 1704067200000,
    "current": 45.3,
    "voltage": 220,
    "phase": 1.57,
    "groupid": 5,
    "location": "Region-A"
  },
  {
    "table": "meter0001",
    "ts": 1704067201000,
    "current": 46.1,
    "voltage": 221,
    "phase": 1.58,
    "groupid": 5,
    "location": "Region-A"
  },
  {
    "table": "meter0001",
    "ts": 1704067202000,
    "current": 44.8,
    "voltage": 219,
    "phase": 1.56,
    "groupid": 5,
    "location": "Region-A"
  }
]
```

## Performance Tuning

**For maximum throughput:**
- Use `acks: 0` (no acknowledgments)
- Increase `records_per_message` (500-1000)
- Increase `concurrency` (16-20)
- Use fast compression: `lz4` or `snappy`
- Increase `queue_capacity` (1000-5000)
- Ensure broker has sufficient partitions

**For reliability:**
- Use `acks: all` (all replicas)
- Moderate batching (10-100 records)
- Use `gzip` or `zstd` compression
- Lower concurrency (4-8)
- Enable idempotence on broker

**For balanced performance:**
- Use `acks: 1` (leader only)
- Moderate batching (50-200 records)
- Use `lz4` or `snappy` compression
- Moderate concurrency (8-12)

**Broker considerations:**
- Ensure topic has enough partitions for parallelism
- Monitor broker CPU, memory, and disk I/O
- Adjust `num.io.threads` and `num.network.threads` on broker
- Consider replication factor vs. performance tradeoff

## Common Patterns

**Pattern 1: Maximum throughput**
```yaml
- uses: kafka/produce
  with:
    concurrency: 20
    acks: 0
    records_per_message: 1000
    compression: lz4
```

**Pattern 2: Reliable ingestion**
```yaml
- uses: kafka/produce
  with:
    concurrency: 8
    acks: all
    records_per_message: 50
    compression: gzip
```

**Pattern 3: Balanced production**
```yaml
- uses: kafka/produce
  with:
    concurrency: 12
    acks: 1
    records_per_message: 100
    compression: snappy
```

## Security Configuration Examples

**SASL/PLAIN with SSL:**
```yaml
rdkafka_options:
  security.protocol: sasl_ssl
  sasl.mechanisms: PLAIN
  sasl.username: user
  sasl.password: password
  ssl.ca.location: /path/to/ca-cert.pem
```

**SASL/SCRAM-SHA-256:**
```yaml
rdkafka_options:
  security.protocol: sasl_ssl
  sasl.mechanisms: SCRAM-SHA-256
  sasl.username: user
  sasl.password: password
```

**SSL only (no SASL):**
```yaml
rdkafka_options:
  security.protocol: ssl
  ssl.ca.location: /path/to/ca-cert.pem
  ssl.certificate.location: /path/to/client-cert.pem
  ssl.key.location: /path/to/client-key.pem
```

**Kerberos (GSSAPI):**
```yaml
rdkafka_options:
  security.protocol: sasl_plaintext
  sasl.mechanisms: GSSAPI
  sasl.kerberos.service.name: kafka
  sasl.kerberos.principal: user@REALM
```

## Compression Comparison

| Algorithm | Speed | Ratio | CPU Usage | Best For |
|-----------|-------|-------|-----------|----------|
| none | Fastest | 1.0x | Lowest | Low latency, fast network |
| lz4 | Very Fast | 2-3x | Low | General purpose, high throughput |
| snappy | Fast | 2-2.5x | Low | Balanced performance |
| gzip | Moderate | 3-4x | Medium | Network-constrained, storage |
| zstd | Moderate | 3-5x | Medium-High | Best compression, modern systems |

## Troubleshooting

**Low throughput:**
- Increase `concurrency`
- Increase `records_per_message`
- Use faster compression (lz4/snappy)
- Check broker partition count
- Monitor broker performance

**Message loss:**
- Use `acks: all`
- Enable idempotence on broker
- Check broker replication settings
- Monitor broker health

**High latency:**
- Reduce `records_per_message`
- Use `acks: 0` or `acks: 1`
- Reduce compression or use faster algorithm
- Check network latency to brokers
