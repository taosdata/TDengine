# taosgen 支持发布到 Kafka FS

## 1. 背景

taosgen 支持发布数据到 Kafka
Jira https://jira.taosdata.com:18080/browse/TS-7353

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/11/7 | 0.1 | 裴亚明 | 初稿 |

## 3. 定义

Kafka：一个开源的分布式流处理平台，由Apache开发，主要用于构建实时数据管道和流应用，具备高吞吐、低延迟、可扩展和容错等特性。

## 4. 行为说明

通过以下配置，可支持发布数据到 Kafka。
```yaml
kafka:
  bootstrap_servers: "kafka-broker1:9092,kafka-broker2:9092,kafka-broker3:9092"
  topic: "your_topic_name"
  rdkafka_options:
      security.protocol: "sasl_ssl"   # e.g., "sasl_plaintext", "sasl_ssl"
      sasl.mechanism: "SCRAM-SHA-256" # e.g., "PLAIN", "SCRAM-SHA-256"
      sasl.username: "your_username"
      sasl.password: "your_password"

jobs:
  # Kafka produce job
  kafka-produce-job:
    steps:
      - uses: kafka/produce
        with:
          concurrency: 8
          key_pattern: "{table}"           # 支持 {table}, {field} 等占位符
          key_serializer: "string-utf8"    # 可选值: "string-utf8", "int32"，"uint32", "int64", "uint64"
          value_serializer: "json"         # 可选值: "json"、"influx"
          acks: "all"                      # 可选值: "all", "1", "0"
          compression: "none"              # 可选值: "none", "gzip", "snappy", "lz4", "zstd"
          timestamp_precision: "ms"        # 可选值: "ms", "us", "ns"

```

### 4.1 新增 Kafka 连接相关配置参数

- bootstrap_servers (字符串)： Kafka 集群地址列表，格式为 "host:port"，多个地址用逗号分隔；
- client_id（字符串）：客户端唯一标识符前缀，默认值为 taosgen；
- topic (字符串)： 指定要写入的 Kafka Topic 名称；
- rdkafka_options（映射）：可指定底层 librdkafka 库支持的可选参数，如：security.protocol、sasl.mechanisms、sasl.username、sasl.password。
  - security.protocol (字符串)：指定客户端与 Kafka 集群之间通信的安全协议。可选值：
    - "plaintext"：明文传输，无加密（默认，若未配置）。
    - "ssl"：使用 SSL/TLS 加密通信。
    - "sasl_plaintext"：使用 SASL 进行身份验证，但通信为明文。
    - "sasl_ssl"：使用 SASL 进行身份验证，并使用 SSL/TLS 加密通信。
    - 默认值：未设置（即等效于 "plaintext"）。
  - sasl.mechanism (字符串)：当 security.protocol 设置为 "sasl_plaintext" 或 "sasl_ssl" 时，指定使用的 SASL 身份验证机制。常见可选值：
    - "PLAIN"：简单的用户名/密码验证，常用于外部身份提供商或基本认证。
    - "SCRAM-SHA-256"：基于挑战-响应的更安全机制，比 PLAIN 更安全。
    - "SCRAM-SHA-512"：比 SHA-256 更强的哈希算法。
    - "GSSAPI"：用于 Kerberos 认证。
  注意：此字段必须与 security.protocol 同时配置，且其值取决于 Kafka Broker 端启用的 SASL 机制。
  - sasl.username (字符串)：SASL 身份验证的用户名。当使用 "PLAIN" 或 "SCRAM" 机制时需要提供。
  - sasl.password (字符串)：SASL 身份验证的密码。
  
  更多参数请参考 librdkafka 库的配置说明：https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md

### 4.2 发布到 Kafka 行为

新增 Action：kafka/produce，支持如下参数：
- schema：默认使用全局的 schema 配置信息，当需要差异化时可在此行动下单独定义。
- concurrency（整数）：生成数据并发送到 Kafka 的并发度；
- failure_handling：描述同“写入 TDengine 数据行动的格式”中的同名参数。[taosgen 参考手册 | 写入 TDengine 数据行动的格式](https://docs.taosdata.com/reference/tools/taosgen/#%E5%86%99%E5%85%A5-tdengine-%E6%95%B0%E6%8D%AE%E8%A1%8C%E5%8A%A8%E7%9A%84%E6%A0%BC%E5%BC%8F)
- time_interval：描述同“写入 TDengine 数据行动的格式”中的同名参数。[taosgen 参考手册 | 写入 TDengine 数据行动的格式](https://docs.taosdata.com/reference/tools/taosgen/#%E5%86%99%E5%85%A5-tdengine-%E6%95%B0%E6%8D%AE%E8%A1%8C%E5%8A%A8%E7%9A%84%E6%A0%BC%E5%BC%8F)
- key_pattern (字符串)：消息 Key 的组成模式，支持 schema 的实例名/字段名称的占位符，占位符格式为：{xxx}，默认为 {table}；
- key_serializer (字符串)： 消息 Key 的序列化方式，支持 "string-utf8"、"int8"、"uint8"、"int16"、"uint16"、"int32"，"uint32", "int64", "uint64"，默认为 "string-utf8"；控制如何将 key_pattern 解析后的结果序列化为 Kafka 消息的 key 字节流。
  - `"string-utf8"`:  将模板替换后的结果视为字符串，直接以 UTF-8 编码生成字节流。
  - `整数`: 将字段模板替换后的结果解析为整数。仅支持单个字段占位符（如 {device_id}），不支持多字段组合（如 {table}_{id}）或表达式运算（如 {id+1}）。序列化时使用该整数类型，并以大端序（big-endian） 格式编码为二进制数据发送。
- value_serializer (字符串)： 消息 Value 的序列化方式，支持 "json"、"influx"，默认为 "json"；
- tbname_key (字符串)：用于指定 json 格式输出中代表表名的字段名称。如果此参数被设置为空字符串 ("")，则不输出表名信息。默认值为 "table"；
- acks (字符串)： 生产者确认机制设置，如 "all"、"1"、"0"，默认为 "0"；
  - "all"：生产者必须等待ISR（In-Sync Replicas，同步副本集）中的所有副本都成功接收到消息并将其写入本地日志后，才会认为消息发送成功。
  - "1"：生产者只需要等待分区 Leader 副本成功接收到消息并将其写入本地日志（Log），就会认为消息发送成功，并立即向应用程序返回确认；
  - "0"：生产者完全不等待任何确认。一旦消息被成功发送到网络（甚至只是放入了生产者的发送缓冲区），就立即认为发送成功。
- compression (字符串)： 消息压缩类型，支持 "none"、"gzip"、"snappy"、"lz4"、"zstd"，默认为 "none"；
- records_per_message（整数）：每条消息包含的记录数，默认为1；
- timestamp_precision (字符串)： 表示消息时间戳的精度，可选值为："ms"、"us"、"ns"，默认为 "ms"；

## 5. 性能

taosgen 在向 Kafka 发布数据时的性能表现，主要受以下两个核心参数的影响：每条消息包含的记录数 (records_per_message) 和 生产者确认机制 (acks)。合理配置这两个参数，可以在数据可靠性、系统吞吐量和延迟之间取得最佳平衡。

### 5.1 每条消息包含的记录数 (records_per_message)

此参数控制单个 Kafka 消息（Message）中打包的模拟数据记录（Row）的数量。其取值对性能有显著影响。

| 参数值 | 性能影响分析 |
| --- | --- |
| `1` (默认) | - 优点：语义最清晰，每条记录独立成一条消息，便于下游消费者按记录粒度处理。 - 缺点：性能开销最大。每条消息都需要独立的网络请求头、序列化/反序列化、Kafka Broker 的日志写入和确认流程。这导致： - 高网络开销：大量小消息（Small Messages）会显著增加网络往返次数（RTT），降低带宽利用率。 - 高 CPU 开销：生产者和 Broker 都需要为每条消息执行更多的元数据处理和 I/O 操作。 - 低吞吐量：单位时间内可发送的总记录数受限。 |
| `>1` (批量打包，如 10, 100, 1000) | - 优点：显著提升吞吐量，降低延迟和资源消耗。 - 减少网络开销：多个记录被打包成一条更大的消息，减少了单位记录的协议开销（如 TCP/IP 头、Kafka 消息头），提高了网络带宽利用率。 - 降低 CPU 开销：减少了生产者和 Broker 处理消息头的次数，I/O 操作更高效（大块写入比小块写入快）。 - 提高吞吐量：在相同硬件条件下，单位时间内可发送的记录总数大幅增加。 - 缺点： - 增加单条消息延迟：必须等待 `records_per_message` 条记录生成后才能发送，增加了消息的“等待时间”。 - 增加内存占用：生产者需要在内存中缓存一批记录，直到达到指定数量或超时。 - 增加单点故障影响：如果一条包含 1000 条记录的消息在传输中失败且无法重试，可能导致这 1000 条记录全部丢失（取决于 `acks` 和重试策略）。 |

性能调优建议：
- 追求高吞吐量：将 `records_per_message` 设置为一个较高的值（如 500 或 1000），并配合 `acks=1` 使用。
- 追求低延迟：保持 `records_per_message=1`，以确保数据能尽快发出。
- 平衡选择：通常设置为 `100` 左右是一个不错的起点，可以在吞吐和延迟之间取得良好平衡。

### 5.2 生产者确认机制 (`acks`)

该参数决定了生产者在认为消息“发送成功”前，需要等待的确认级别，是可靠性与性能权衡的关键。

| 参数值 | 性能影响分析 |
| --- | --- |
| `0` (默认) | - 优点：性能最优。生产者无需等待任何网络响应，发送后立即返回，吞吐量最高，延迟最低。 - 缺点：可靠性最差。无法保证消息是否被 Broker 接收。网络问题、Broker 宕机都会导致消息静默丢失。 |
| `1` | - 优点：性能与可靠性的良好折衷。只需等待 Leader 写盘成功，延迟和吞吐量表现良好。 - 缺点：存在数据丢失风险。如果 Leader 在写入后、同步给 Follower 前宕机，且新选举的 Leader 未包含该消息，则消息丢失。 |
| `all` | - 优点：提供最强的持久性保证。确保消息被所有同步副本（ISR）持久化，即使 Leader 宕机，数据也不会丢失。 - 缺点：性能最差。延迟最高，吞吐量最低。因为生产者必须等待最慢的那个 ISR 副本完成写入。如果某个 Follower 副本性能差或网络延迟高，会严重拖慢整个生产者。 |

性能调优建议：
- 压测 Kafka 极限吞吐：使用 `acks=0`，但这会牺牲数据可靠性，仅用于性能基准测试。
- 常规高吞吐场景：使用 `acks=1`，这是大多数生产环境的推荐选择。
- 关键数据、金融场景：必须使用 `acks=all`，并确保 Broker 端配置了合理的 `min.insync.replicas`（如 2），以保证高可用性。

## 6. 兼容性

无。

## 7. 运维

无。

## 8. 使用场景

```yaml
kafka:
  bootstrap_servers: "kafka-broker1:9092,kafka-broker2:9092,kafka-broker3:9092"
  topic: "factory/electric-meter"
  rdkafka_options:
      security.protocol: "sasl_ssl"   # e.g., "sasl_plaintext", "sasl_ssl"
      sasl.mechanism: "SCRAM-SHA-256" # e.g., "PLAIN", "SCRAM-SHA-256"
      sasl.username: "your_username"
      sasl.password: "your_password"

schema:
  name: meters
  tbname:
    prefix: d
    count: 10000
    from: 0
  columns:
    - name: ts
      type: timestamp
      start: 1700000000000
      precision : ms
      step: 300s
    - name: current
      type: float
      min: 0
      max: 100
    - name: voltage
      type: int
      expr: 220 * math.sqrt(2) * math.sin(_i)
    - name: phase
      type: float
      min: 0
      max: 360
    - name: location
      type: varchar(20)
      values:
        - Chicago
        - Houston
        - Phoenix
        - Philadelphia
        - Dallas
        - Austin
  generation:
    interlace: 1
    concurrency: 8
    rows_per_table: 10000
    rows_per_batch: 10000
    num_cached_batches: 0

jobs:
  # Kafka produce job
  kafka-produce-job:
    steps:
      - uses: kafka/produce
        with:
          concurrency: 8
          key_pattern: "{table}"       # 支持 {table}, {field} 等占位符
          key_serializer: "str"        # 可选值: "str", "int"
          value_serializer: "json"     # 可选值: "json"、"influx"
          acks: "all"                  # 可选值: "all", "1", "0"
          compression: "none"          # 可选值: "none", "gzip", "snappy", "lz4", "zstd"
          timestamp_precision: "ms"    # 可选值: "ms", "us", "ns"
```


该示例展示了如何使用 taosgen 工具模拟一万台智能电表，每台智能电表采集电流、电压、相位、位置四个物理量，它们每隔 5 分钟产生一条记录，电流的数据用随机数，电压用正弦波模拟，产生的这些数据发布到 Kafka。
配置详解：
- Kafka 配置参数
  - 连接信息: 使用 bootstrap_servers 描述连接 Kafka Broker 的信息。
  - 主题配置 (topic): 使用主题 factory/electric-meter。
  - rdkafka 可选参数: 配置了认证信息。
- schema 配置参数
  - 名称：指定 schema 的名称。
  - 表名称：定义生成一万张表名称的规则，格式为 d0 到 d9999。虽然不直接创建数据库表，此处表作为逻辑概念用来组织数据。
  - 表字段结构信息: 定义数据表结构，包含4个普通列（电流、电压、相位、设备位置）。
    - 时间戳: 配置了时间戳生成策略，从指定时间戳 1700000000000 (2023-11-14 22:13:20 UTC) 开始，以 5 分钟的步长递增。
    - 时序数据: current、phase 和 location 使用指定范围的随机数，voltage 使用正弦波模拟。
  - 数据生成行为：使用交错模式写入，每张表写入 1 万条记录，每批写入请求最大行数为 1000 行。
- 数据发布：使用 8 线程并发向Kafka Broker 发布数据，提高吞吐量。
场景说明：
此配置专为向 Kafka 消息代理发布模拟设备数据而设计。它适用于以下场景：
- Kafka 生产者性能压测：
模拟大规模设备并发写入场景，测试 Kafka Broker 的吞吐能力、网络带宽占用及生产者端的资源消耗，验证不同压缩算法（如 `gzip`, `zstd`）对性能的影响。
- 流处理系统集成测试：
向 Kafka 发布结构化的设备数据流，用于测试基于 Flink、Spark Streaming、ksqlDB 或 Pulsar Functions 等流处理引擎的数据接入、窗口计算、状态管理与实时告警功能。
- 物联网平台数据接入验证：
快速构建一个高并发的设备数据注入环境，模拟万台智能电表上报数据，验证 IoT 平台后端服务从 Kafka 消费数据、解析、入库（如时序数据库 TDengine、InfluxDB）的完整链路稳定性。
- 规则引擎与消息路由测试：
利用 Kafka 主题的分层命名（如 `factory/electric-meter`）和消息 Key（如 `{table}` 表示设备 ID），测试基于 Kafka Connect 或自定义消费者组的消息过滤、多路复用、按设备标签（location）进行动态路由的能力。
- 实时数据管道压力测试：
模拟持续高频率的时序数据流（每 5 分钟/条 × 10,000 台设备），评估从 Kafka 到下游系统（如数据湖、数仓、监控面板）的端到端延迟、积压情况和消费速率匹配度。
- 安全认证机制验证：
配置 SASL_SSL 和 SCRAM-SHA-256 认证，用于测试启用了身份验证和加密传输的 Kafka 集群在真实生产环境下的客户端连接稳定性与安全性。
- 数据格式兼容性测试：
使用 JSON 或 InfluxDB Line Protocol 格式序列化消息体，验证下游消费者或中间件对不同数据格式的解析能力，确保协议兼容性和字段映射正确性。
- 灾备与高可用演练：
在多 Broker 集群环境下，通过高并发写入测试 Kafka 的副本同步、Leader 选举、Broker 故障转移等高可用机制的表现，确保数据不丢失、服务不间断。

## 9. 约束和限制

本节描述了 taosgen 在将模拟数据发布到 Kafka 时的当前实现所遵循的假设、前提条件以及功能上的限制

### 9.1 实现假设与前提

1. 单 Topic 写入：
  - 当前实现假设所有由 `taosgen` 生成的数据都写入一个预先存在且已配置好分区的单一 Kafka Topic。
  - 不支持根据数据内容（如 schema 名称、表名或字段值）动态路由到不同的 Topic。
1. 消息 Key 的确定性：
  - 消息的 Key 是通过 `key_pattern` 模板对每条记录的元数据（如 `{table}`）进行字符串替换后生成的。
  - 使用 `"int"` 序列化方式时，假设占位符解析后的结果可以被无歧义地转换为整数。
1. Schema 定义完备性：
  - 要求 `schema` 配置必须完整定义所有需要生成的字段结构（名称、类型、生成规则）。
  - `taosgen` 不会从 Kafka Topic 的 Schema Registry 或其他外部源推断数据结构。
1. Kafka Producer 的长连接：
  - 假设 Kafka 集群网络稳定，`taosgen` 启动后会建立一个或多个持久化的生产者连接，并在整个数据生成周期内保持连接。

### 9.2 功能限制

1. 不支持 Avro、Protobuf 等二进制序列化：
  - 目前 `value_serializer` 仅支持 `"json"` 和 `"influx"` 格式。
  - 不支持将数据序列化为 Avro、Protocol Buffers (Protobuf) 或 MessagePack 等二进制格式。
1. 不支持复杂的消息 Header 操作：
  - 当前接口不提供自定义 Kafka 消息 Header 的能力。
  - 所有消息均以默认的、空的 Header 发送。
1. 不支持事务性写入 (Transactional Writes)：
  - `taosgen` 使用的是标准的 Kafka Producer API，不支持开启事务模式（即设置 `enable.idempotence=true` 并配合 `transactional.id`）。
  - 因此，无法保证跨多条消息的原子性提交（All-or-Nothing），也无法完全避免消息的重复。
1. 不支持精确一次 (Exactly-Once) 语义：
  - 由于不支持事务，`taosgen` 本身不能保证端到端的精确一次投递。在极端情况下（如生产者崩溃并重启），可能会产生重复消息。
1. Key 的 "int" 系列序列化方式限制：
  - 当 `key_serializer="int"` 时，`key_pattern` 只能包含一个整数类型的占位符（例如 `{device_id}`）。
  - 不支持任何形式的组合或表达式，包括：
    - 多个占位符：`{table}_{id}` ❌
    - 字符串拼接：`prefix_{id}` ❌
    - 数学运算：`{id + 1}`, `{current * 100}` ❌
  - 如果尝试使用，将导致运行时错误。
1. 不支持动态 schema 变更：
  - 在 `taosgen` 运行期间，`schema` 配置是静态的。程序启动后，无法动态修改正在生成的数据结构。
1. 不支持压缩算法的自动协商：
  - `compression` 参数指定了生产者使用的压缩算法，但不保证Broker 会接受或使用该算法。最终的压缩方式由 Broker 的 `compression.type` 配置和主题级别设置决定。
  - 生产者只是“建议”使用某种压缩，实际生效情况需在 Broker 端确认。
1. 不直接管理 Topic 生命周期：
  - `taosgen` 不会自动创建、删除或修改 Kafka Topic。
  - 用户必须确保目标 `topic` 已经存在，并且具有足够的分区数来满足并发写入的需求。如果 Topic 不存在，生产者将报错。

## 10. 常见错误和排查

错误排查在开发测试中补充。

## 11. 可观测性

`taosgen` 在向 Kafka 发布数据时，提供了全面的运行时监控与事后统计能力，确保用户能够清晰掌握数据生成、传输性能及系统资源消耗情况。所有信息均通过结构化日志实时输出，便于观察、分析与问题排查。

#### 11.0.1 实时运行状态监控

在数据生成和发布过程中，程序会周期性（默认每秒）输出关键指标，包括：
- 运行时间（Runtime）：任务已持续执行的时间（秒）。
- 实时写入速率（Rate）：当前每秒成功发送到 Kafka 的数据行数（rows/s），反映瞬时吞吐能力。
- 累计发送量（Total）：自任务启动以来已发布的总数据行数。
- 内部队列积压（Queue）：等待提交的消息批数量，用于判断生产者是否跟得上生成速度。
- 资源使用情况：
  - CPU 使用率（CPU Usage）：多核并行下的总体占用百分比（如 `462.76%` 表示接近 5 核满载）。
  - 内存使用量（Memory Usage）：当前进程占用的物理内存大小。
  - 线程数量（Thread Count）：活跃线程数，反映并发负载状态。
示例日志：
```plaintext
[2025-10-30 17:00:30.732043] [info] [thread 263347] Runtime:   42s | Rate:   187400 rows/s | Total:  8952500 rows | Queue:  71 items | CPU Usage:  462.76% | Memory Usage:   1.50 GB | Thread Count:  31
```


#### 11.0.2 任务结束后的聚合统计

当所有数据生成完成且消息队列清空后，程序将输出详细的汇总报告，包含：
- 总运行时长（Total Duration）：从开始到完全结束的 wall-clock 时间。
- 平均发布速率（Average Rate）：总行数 ÷ 总时长，衡量整体吞吐性能。
- 纯写入延迟分布（Write Latency Distribution）：每批次消息从提交到收到 Kafka 响应的延迟统计：
  - 最小值（min）、平均值（avg）
  - 分位数：p90、p95、p99
  - 最大值（max）
- 效率分析（Efficiency Metrics）：
  - 纯插入耗时（Pure Insert Latency）：实际用于发送数据的时间。
  - 框架开销占比（Framework Overhead）：非核心写入时间（如初始化、收尾、等待等）占总时间的比例。
  - 结束后空闲时间（Idle Time After Finish）：所有线程完成后到主进程退出的延迟。
示例日志：
```plaintext
=================================================== Insert Summary Statistics ========================================================
Insert Threads: 8
Total Rows: 10000000
Total Duration: 50.86 seconds
Average Rate: 196633.74 rows/second
...
Write Latency Distribution: min: 35.1172ms, avg: 179.8308ms, p90: 324.7350ms, p95: 348.8472ms, p99: 400.0193ms, max: 437.4312ms
======================================================================================================================================
```

#### 11.0.3 消费者与线程生命周期追踪（调试级）

启用调试日志级别时，可查看：
- 各 Kafka 消费者线程（Producer）的终止信号接收情况。
- 每个写入线程（Writer）的精确起止时间戳，用于分析并行效率与负载均衡。

## 12. 安装和卸载

无

## 13. 文档

需要修改 taosgen 官网文档

## 14. 参考文档

## 15. 附录

无。
