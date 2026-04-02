# MQTT Configuration Reference

Complete reference for generating MQTT publishing configurations.

## MQTT Connection Configuration

```yaml
mqtt:
  uri: tcp://localhost:1883
  user: ""
  password: ""
  client_id: taosgen
  keep_alive: 60
  clean_session: true
  max_buffered_messages: 10000
```

### Parameters

- **uri**: Broker address
  - Format: `protocol://host:port`
  - Protocols: `tcp://` (plain), `ssl://` (TLS)
  - Default: `tcp://localhost:1883`

- **user**: Authentication username (optional)

- **password**: Authentication password (optional)

- **client_id**: Client identifier prefix
  - taosgen appends thread number to this prefix
  - Default: `taosgen`

- **keep_alive**: Heartbeat interval in seconds
  - Default: 60

- **clean_session**: Clear session state on connect
  - `true`: Start fresh (no persistent session)
  - `false`: Resume previous session
  - Default: true

- **max_buffered_messages**: Maximum buffered messages
  - Controls memory usage
  - Default: 10000

## MQTT Actions

### mqtt/publish

Publishes messages to MQTT broker.

```yaml
- uses: mqtt/publish
  with:
    schema:  # Optional: override global schema
    format: json
    concurrency: 8
    failure_handling:
      max_retries: 3
      retry_interval_ms: 1000
      on_failure: exit
    time_interval:
      enabled: true
      interval_strategy: literal
      wait_strategy: sleep
    topic: tsbench/{table}
    qos: 0
    retain: false
    tbname_key: "table"
    records_per_message: 1
```

**Parameters:**

- **schema** (optional): Override global schema configuration for this action

- **format**: Message format (only supports `json`)
  - Default: `json`

- **concurrency**: Number of parallel publish threads
  - Typically 4-20 based on broker capacity
  - Each thread creates a separate MQTT client

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

- **topic**: Topic pattern
  - Use `{table}` placeholder for table name
  - Supports `{column}` placeholders for column values
  - Default: `tsbench/{table}`
  - Examples:
    - `sensors/{table}` → `sensors/d0001`, `sensors/d0002`, ...
    - `data/{table}/metrics` → `data/d0001/metrics`, ...
    - `fixed-topic` → all messages to same topic

- **qos**: Quality of Service level
  - `0`: At most once (fire and forget, fastest)
  - `1`: At least once (acknowledged)
  - `2`: Exactly once (slowest, most reliable)
  - Default: `0`

- **retain**: Retain flag for MQTT messages
  - `true`: Broker retains the last message
  - `false`: No retention
  - Default: `false`

- **tbname_key**: JSON key name for table name in message
  - Default: `"table"`

- **records_per_message**: Number of records to batch per message
  - `1`: One record per message (lowest latency)
  - Higher values: Better throughput, higher latency
  - Typically 1-500 depending on use case
  - Default: `1`

## Complete Examples

### Example 1: Real-Time Sensor Data

Low-latency publishing with individual messages.

```yaml
mqtt:
  uri: tcp://localhost:1883
  user: root
  password: taosdata
  keep_alive: 60
  clean_session: true

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
      min: 15.0
      max: 35.0
    - name: humidity
      type: float
      min: 30.0
      max: 90.0
    - name: pressure
      type: float
      min: 980.0
      max: 1050.0
  tags:
    - name: location
      type: binary(32)
      values:
        - Building-A
        - Building-B
        - Building-C
    - name: floor
      type: int
      min: 1
      max: 10
  generation:
    interlace: 1
    rows_per_table: 1000
    rows_per_batch: 100

jobs:
  publish-data:
    steps:
      - uses: mqtt/publish
        with:
          concurrency: 8
          topic: sensors/{table}/data
          qos: 0
          records_per_message: 1
```

**Key points:**
- QoS 0 for lowest latency
- One record per message for real-time feel
- Dynamic topic per sensor
- Moderate concurrency (8 threads)

### Example 2: High-Throughput Batch Publishing

Maximize throughput with message batching.

```yaml
mqtt:
  uri: tcp://broker.example.com:1883
  user: producer
  password: secret
  client_id: taosgen-batch
  max_buffered_messages: 5000

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
  generation:
    interlace: 1
    rows_per_table: 100
    rows_per_batch: 10000

jobs:
  publish-data:
    steps:
      - uses: mqtt/publish
        with:
          concurrency: 20
          topic: meters/batch
          qos: 0
          records_per_message: 500
```

**Key points:**
- 500 records per message for high throughput
- High concurrency (20 threads)
- Single topic for all data
- Large buffer for message queuing

### Example 3: Reliable Delivery with QoS 1

Guaranteed delivery with acknowledgments.

```yaml
mqtt:
  uri: tcp://localhost:1883
  user: reliable-client
  password: password
  clean_session: false

schema:
  name: events
  tbname:
    prefix: device
    count: 5000
    from: 1
  columns:
    - name: ts
      type: timestamp
      start: now
      precision: ms
      step: 5000
    - name: event_type
      type: int
      min: 1
      max: 10
    - name: value
      type: double
      min: 0
      max: 1000
  tags:
    - name: device_type
      type: binary(16)
      values:
        - sensor
        - actuator
        - controller
  generation:
    interlace: 1
    rows_per_table: 200
    rows_per_batch: 1000

jobs:
  publish-events:
    steps:
      - uses: mqtt/publish
        with:
          concurrency: 4
          topic: events/{table}
          qos: 1
          records_per_message: 10
```

**Key points:**
- QoS 1 for acknowledged delivery
- `clean_session: false` for persistent session
- Lower concurrency to avoid overwhelming broker
- Moderate batching (10 records per message)

### Example 4: IoT Device Simulation

Realistic IoT device behavior with expressions.

```yaml
mqtt:
  uri: tcp://iot-broker.local:1883
  user: iot-user
  password: iot-pass
  keep_alive: 120

schema:
  name: iot_devices
  tbname:
    prefix: iot
    count: 50000
    from: 1
  columns:
    - name: ts
      type: timestamp
      start: now
      precision: ms
      step: 10000
    - name: cpu_usage
      type: float
      expr: 20 + math.random() * 60 + math.sin(_i * 0.1) * 15
    - name: memory_usage
      type: float
      expr: 40 + math.random() * 40 + math.cos(_i * 0.05) * 10
    - name: network_tx
      type: bigint
      expr: math.floor(1000000 + math.random() * 5000000)
    - name: network_rx
      type: bigint
      expr: math.floor(2000000 + math.random() * 8000000)
  tags:
    - name: region
      type: binary(16)
      values:
        - us-east
        - us-west
        - eu-central
        - ap-southeast
    - name: device_model
      type: binary(32)
      values:
        - Model-X100
        - Model-X200
        - Model-X300
  generation:
    interlace: 1
    rows_per_table: 500
    rows_per_batch: 5000

jobs:
  simulate-devices:
    steps:
      - uses: mqtt/publish
        with:
          concurrency: 12
          topic: iot/{table}/telemetry
          qos: 0
          records_per_message: 5
```

**Key points:**
- Expression-based data for realistic patterns
- Sine/cosine waves simulate periodic behavior
- Random variations for natural fluctuations
- Topic includes device identifier

## Message Format

taosgen publishes messages in JSON format. Each message contains one or more records:

**Single record (records_per_message: 1):**
```json
{
  "table": "sensor0001",
  "ts": 1704067200000,
  "temperature": 25.3,
  "humidity": 65.2,
  "location": "Building-A",
  "floor": 3
}
```

**Multiple records (records_per_message: 3):**
```json
[
  {
    "table": "sensor0001",
    "ts": 1704067200000,
    "temperature": 25.3,
    "humidity": 65.2,
    "location": "Building-A",
    "floor": 3
  },
  {
    "table": "sensor0001",
    "ts": 1704067201000,
    "temperature": 25.4,
    "humidity": 65.1,
    "location": "Building-A",
    "floor": 3
  },
  {
    "table": "sensor0001",
    "ts": 1704067202000,
    "temperature": 25.5,
    "humidity": 65.3,
    "location": "Building-A",
    "floor": 3
  }
]
```

## Performance Tuning

**For maximum throughput:**
- Use QoS 0 (no acknowledgments)
- Increase `records_per_message` (100-500)
- Increase `concurrency` (12-20)
- Increase `max_buffered_messages` (5000-10000)
- Use single topic or fewer topics

**For low latency:**
- Use QoS 0
- Set `records_per_message: 1`
- Moderate concurrency (4-8)
- Use dynamic topics per device

**For reliability:**
- Use QoS 1 or 2
- Set `clean_session: false`
- Lower concurrency (4-8)
- Moderate batching (10-50 records)

**Broker considerations:**
- Monitor broker CPU and memory
- Adjust concurrency based on broker capacity
- Consider broker's max connections limit
- Use authentication for production

## Common Patterns

**Pattern 1: Real-time streaming**
```yaml
- uses: mqtt/publish
  with:
    concurrency: 8
    topic: stream/{table}
    qos: 0
    records_per_message: 1
```

**Pattern 2: Batch upload**
```yaml
- uses: mqtt/publish
  with:
    concurrency: 16
    topic: batch/data
    qos: 0
    records_per_message: 500
```

**Pattern 3: Reliable telemetry**
```yaml
- uses: mqtt/publish
  with:
    concurrency: 4
    topic: telemetry/{table}
    qos: 1
    records_per_message: 10
```

**Pattern 4: Retained last-will messages**
```yaml
- uses: mqtt/publish
  with:
    concurrency: 4
    topic: devices/{table}/status
    qos: 1
    retain: true
    records_per_message: 1
```

## Topic Design Tips

- Use `{table}` for per-device topics: `devices/{table}/data`
- Include hierarchy: `region/building/{table}/sensors`
- Keep topics short for efficiency
- Use wildcards in subscribers: `devices/+/data` or `devices/#`
- Avoid deep hierarchies (3-4 levels max)
