# taosgen-config Skill FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-03-06 | - | 0.1.0 | 裴亚明 | 初始版本 |

## 2. 背景

### 2.1 需求背景

taosgen 是时序数据产品性能基准测试工具，支持数据生成、写入性能测试、消息发布等功能。taosgen 使用 YAML 配置文件定义测试任务，配置文件包含：
- 目标系统连接参数（TDengine/MQTT/Kafka）
- 数据模式定义（Schema）
- 数据生成策略（随机/表达式/CSV）
- 工作流编排（Job DAG）
然而，taosgen 的配置语法复杂，涉及多个目标系统的不同参数规范，且官方文档时常更新。用户在实际使用中面临以下痛点：
1. **配置学习成本高**：需要记忆大量参数名、取值范围、默认值
2. **文档查阅繁琐**：需要在官方文档中反复查找参数说明
3. **容易配置错误**：参数名拼写错误、使用已废弃参数、默认值与预期不符
4. **难以优化性能**：不清楚如何根据性能目标（吞吐量/延迟/可靠性）调整参数

### 2.2 设计目标

本 Skill 旨在为 Claude Code 等 AI Agent 提供智能化的 taosgen 配置生成能力，实现：
1. **自然语言生成配置**：用户通过自然语言描述测试需求，Skill 自动生成符合官方文档规范的 YAML 配置
2. **文档双源策略**：优先使用 WebFetch 获取官方最新文档，失败时 Fallback 到本地参考文档，确保配置始终符合最新规范
3. **智能参数推断**：根据用户性能目标（吞吐量优先/延迟优先/可靠性优先）自动推荐最佳参数
4. **配置验证机制**：通过临时运行测试验证配置有效性，提前发现语法错误
5. **多目标系统统一支持**：统一支持 TDengine、MQTT、Kafka 三种目标系统的配置生成

### 2.3 目标用户

- **开发者**：需要快速生成 taosgen 配置进行时序数据库性能测试
- **测试工程师**：进行 IoT 场景消息流测试，需要配置 MQTT/Kafka 发布
- **DevOps 人员**：需要自动化基准测试流程，集成到 CI/CD 管道

## 3. 定义

### 3.1 术语定义

| 术语 | 定义 |
| --- | --- |
| **Skill** | Agentic Skill，面向 AI Agent 的可执行工作说明书，包含 YAML frontmatter 的元数据和 Markdown 格式的指令 |
| **YAML** | YAML Ain't Markup Language，一种人类可读的数据序列化标准，taosgen 使用 YAML 作为配置格式 |
| **DAG** | Directed Acyclic Graph，有向无环图。taosgen 的 Job 依赖关系使用 DAG 建模 |
| **DSN** | Data Source Name，数据源名称。TDengine 使用 DSN 格式定义连接信息 |
| **Schema** | 数据模式定义，包含表名生成规则、列定义、标签定义、数据生成策略 |
| **Job** | taosgen 的基本工作单元，包含一组有序执行的 Step，可指定依赖关系形成 DAG |
| **Step** | Job 的基本操作单元，通过 `uses` 指定 Action，通过 `with` 传递参数 |
| **Action** | taosgen 的预定义操作，如 `tdengine/insert`、`mqtt/publish`、`kafka/produce` |
| **WebFetch** | Claude Code 的网络获取工具，用于获取官方文档内容 |
| **Fallback** | 文档获取失败时的降级策略，切换到本地参考文档 |

### 3.2 缩略语

| 缩略语 | 全称 | 说明 |
| --- | --- | --- |
| **QoS** | Quality of Service | MQTT 服务质量等级（0/1/2） |
| **SASL** | Simple Authentication and Security Layer | 简单认证和安全层，Kafka 认证机制 |
| **SSL/TLS** | Secure Sockets Layer / Transport Layer Security | 安全传输层协议 |
| **JSON** | JavaScript Object Notation | 轻量级数据交换格式 |
| **CSV** | Comma-Separated Values | 逗号分隔值文件格式 |

## 4. 行为说明

### 4.1 Skill 激活与路由

**触发条件**：
用户输入包含以下关键词或意图时，Skill 被激活：
- "创建 taosgen 配置"
- "生成 taosgen YAML"
- "设置 TDengine 测试"
- "MQTT 性能测试配置"
- "Kafka 负载测试"
- "taosgen" + "配置/测试/性能/压测"

**激活行为**：
1. Claude Code 读取 `skills/taosgen-config/SKILL.md` 的 frontmatter 元数据（name, description）
2. 匹配成功则加载完整 SKILL.md 内容（~2000 tokens）
3. Skill 进入激活状态，开始执行 Workflow

### 4.2 需求理解行为

#### 4.2.1 目标系统识别

**识别逻辑**：
Skill 扫描用户输入中的关键词，识别目标系统：

| 关键词类别 | 关键词示例 | 识别结果 | 置信度 |
| --- | --- | --- | --- |
| TDengine | TDengine, 涛思, 时序数据库, super table, 超级表, 子表 | TDengine | 高 (0.9+) |
| MQTT | MQTT, Broker, 消息发布, topic, QoS, 消息保留 | MQTT | 高 (0.9+) |
| Kafka | Kafka, Producer, Partition, Broker, Consumer Group | Kafka | 高 (0.9+) |

**低置信度处理**：
当置信度 < 0.7 时，Skill 主动询问用户：
```plaintext
您想为哪个目标系统生成配置？
1. TDengine（时序数据库）
2. MQTT（消息队列）
3. Kafka（流处理平台）
```

#### 4.2.2 数据特征收集

**收集方式**：
Skill 通过自然语言对话收集以下信息：

| 参数 | 询问示例 | 默认值 | 有效范围 |
| --- | --- | --- | --- |
| 表数量 | "需要测试多少张表/设备？" | 10000 | 1 - 1000000 |
| 表名前缀 | "表名前缀是什么？" | d/sensor/meter | 任意字符串 |
| 起始索引 | "表名从多少开始编号？" | 0 | 0 - N |
| 列定义 | "有哪些数据列？数据类型？" | ts, current, voltage, phase | 官方支持的数据类型 |
| 标签定义 | "有哪些标签列？" | groupid, location | 官方支持的数据类型 |
| 每表行数 | "每张表写入多少行数据？" | 10000 | -1 (无限), 1 - N |
| 数据生成方式 | "数据如何生成？随机/表达式/CSV？" | random | random/order/expression/csv |

**数据生成方式识别**：

| 用户描述 | gen_type | 关键参数 | 示例 |
| --- | --- | --- | --- |
| "随机生成", "范围内随机" | random | min, max | min: -20, max: 50 |
| "顺序递增", "从X到Y", "循环计数" | order | min, max | min: 1, max: 1000 |
| "公式计算", "表达式", "正弦波", "Lua" | expression | expr | expr: math.sin(_i) * 100 |
| "从文件读取", "CSV导入" | csv | from_csv | from_csv: {file_path: ...} |

#### 4.2.3 性能目标识别

**性能模式识别逻辑**：

| 用户描述 | 性能模式 | 推荐参数 |
| --- | --- | --- |
| "最大吞吐量", "越高越好", "性能压测", "极限测试" | 吞吐量优先 | concurrency: 16-20, rows_per_batch: 20000-50000, format: stmt, acks: 0, qos: 0 |
| "低延迟", "实时写入", "快速响应", "毫秒级" | 延迟优先 | concurrency: 4-8, rows_per_batch: 1000-5000, records_per_message: 1 |
| "可靠传输", "不丢数据", "安全传输", "生产环境" | 可靠性优先 | qos: 1/2, acks: all, max_retries: 3, retry_interval_ms: 1000 |
| "正常测试", "平衡", "综合测试", "默认即可" | 平衡模式 | concurrency: 8-12, rows_per_batch: 10000-20000 |

### 4.3 文档获取行为

#### 4.3.1 文档获取策略

**优先级队列**：
1. **第一优先级：WebFetch 官方文档**
  - URL: https://docs.tdengine.com/tdengine-reference/tools/taosgen/
  - 超时: 5 秒
  - 成功条件: HTTP 200 且内容包含关键参数（如 "tdengine", "mqtt", "schema"）
  - 失败处理: 自动降级到第二优先级
    
1. **第二优先级：本地 Reference 文件**
  - 根据目标系统选择对应文件
  - 路径: `skills/taosgen-config/references/{tdengine,mqtt,kafka,schema,common}.md`
  - 加载时机: WebFetch 失败或返回内容不完整时

#### 4.3.2 文档内容解析

**关键章节映射**：

| 目标系统 | WebFetch 章节 | 本地 Reference 文件 | 关键参数 |
| --- | --- | --- | --- |
| TDengine | TDengine Parameters, tdengine/* Actions | references/tdengine.md | dsn, format, auto_create_table, stmt, sml, json, csv |
| MQTT | MQTT Parameters, mqtt/publish Action | references/mqtt.md | uri, user, client_id, keep_alive, topic, qos, records_per_message, retain |
| Kafka | Kafka Parameters, kafka/produce Action | references/kafka.md | bootstrap_servers, client_id, topic, acks, compression, key_pattern, value_serializer |
| Schema | Schema Parameters, Column Config | references/schema.md | tbname, columns, tags, gen_type, min, max, expr, from_csv |
| Job | Job Format, Step Format | references/common.md | needs, steps, uses, with, checkpoint, failure_handling, time_interval |

### 4.4 配置生成行为

#### 4.4.1 配置结构模板

**基础配置结构**（所有目标系统共用）：
```yaml

## 5. 目标系统配置（根据选择填充 tdengine/mqtt/kafka）

{target_system}:
  {system_specific_params}

## 6. 数据模式配置

schema:
  name: {table_name}
  tbname:
    prefix: {prefix}
    count: {table_count}
    from: {start_index}
  columns:
    {column_definitions}
  tags:
    {tag_definitions}
  generation:
    interlace: {interlace}
    rows_per_table: {rows_per_table}
    rows_per_batch: {rows_per_batch}
    tables_reuse_data: {true|false}  # 默认 true
    num_cached_batches: {0|N}

## 7. 任务工作流

jobs:
  {job_name}:
    needs: [{dependencies}]
    steps:
      - name: {step_name}  # 可选
        uses: {action_name}
        with:
          {action_parameters}
```


#### 7.0.1 TDengine 配置生成

**配置层级结构**：
1. **DSN 连接配置**（必需）：
  ```yaml
  tdengine:
    dsn: "{protocol}://{user}:{password}@{host}:{port}/{database}"
  ```

  
  DSN 各组件说明：

  | 组件 | 可选值 | 默认值 | 说明 |
| --- | --- | --- | --- |
| protocol | ws, wss, taos, taosws | ws | 连接协议 |
| user | 任意字符串 | root | 用户名 |
| password | 任意字符串 | taosdata | 密码（建议使用环境变量） |
| host | IP 或主机名 | localhost | 服务器地址 |
| port | 端口号 | 6041 (ws)/6030 (native) | 服务端口 |
| database | 数据库名 | tsbench | 目标数据库 |


1. **超级表创建配置**：
  ```yaml
  - uses: tdengine/create-super-table
    with:
      columns:
        - name: ts
          type: timestamp
          precision: ms
        - name: current
          type: float
          min: 0
          max: 100
        # ... 更多列
      tags:
        - name: groupid
          type: int
          min: 1
          max: 100
        # ... 更多标签
  ```

  
1. **数据写入配置**：
  ```yaml
  - uses: tdengine/insert
    with:
      concurrency: 16  # 并发写入线程数，默认 8，范围 1-100
      format: stmt  # 写入格式，可选 stmt/sql，默认 stmt
      auto_create_table: true  # 自动创建子表，默认 false
      source: generator  # 数据来源，可选 generator/csv，默认 generator
      # checkpoint 仅用户明确要求时添加
      # time_interval 仅用户要求模拟实时时添加
      # failure_handling 仅用户要求错误处理时添加
  ```

#### 7.0.2 MQTT 配置生成

**MQTT 连接配置**：
```yaml
mqtt:
  uri: "tcp://{host}:{port}"  # 格式: protocol://host:port
  user: "{username}"  # 可选，认证用户名
  password: "{password}"  # 可选
  client_id: "taosgen"  # 客户端ID前缀，默认 taosgen
  keep_alive: 5  # 心跳间隔，单位秒，默认 5，范围 1-65535
  clean_session: true  # 是否清除会话，默认 true
  max_buffered_messages: 10000  # 最大缓冲消息数，默认 10000
```

**MQTT 发布配置**：
```yaml
- uses: mqtt/publish
  with:
    schema:  # 可选，覆盖全局 schema
    format: json  # 消息格式，仅支持 json，默认 json
    concurrency: 8  # 并发线程数，默认 8，范围 1-100
    topic: "tsbench/{table}"  # 主题格式，支持 {table} {column} 占位符
    qos: 0  # 服务质量，0/1/2，默认 0
    retain: false  # 消息保留标志，默认 false
    tbname_key: "table"  # JSON 中表名字段名，默认 "table"
    records_per_message: 1  # 每消息记录数，默认 1，范围 1-500
    # time_interval 仅用户要求模拟实时时添加
    # failure_handling 仅用户要求错误处理时添加
```

**Topic 占位符说明**：

| 占位符 | 替换内容 | 示例 |
| --- | --- | --- |
| {table} | 表名 | sensors/{table} → sensors/d0001 |
| {column_name} | 列值 | data/{status} → data/active |

#### 7.0.3 Kafka 配置生成

**Kafka 连接配置**：
```yaml
kafka:
  bootstrap_servers: "{host1:port1},{host2:port2}"  # Broker 地址列表
  topic: "{topic_name}"  # 目标主题名
  client_id: "taosgen"  # 客户端ID前缀，默认 taosgen
  rdkafka_options:  # librdkafka 高级配置
    security.protocol: SASL_SSL  # 安全协议: PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL
    sasl.mechanism: PLAIN  # SASL 机制: PLAIN/SCRAM-SHA-256/SCRAM-SHA-512/GSSAPI
    sasl.username: "{username}"  # SASL 用户名
    sasl.password: "{password}"  # SASL 密码
    ssl.ca.location: "/path/to/ca-cert.pem"  # SSL CA 证书路径
    ssl.certificate.location: "/path/to/client-cert.pem"  # 客户端证书
    ssl.key.location: "/path/to/client-key.pem"  # 客户端密钥
```


**Kafka 生产配置**：
```yaml
- uses: kafka/produce
  with:
    schema:  # 可选，覆盖全局 schema
    concurrency: 8  # 并发线程数，默认 8
    failure_handling:  # 可选，同 MQTT
    time_interval:  # 可选，同 MQTT
    key_pattern: "{table}"  # 消息 Key 格式，默认 "{table}"
    key_serializer: "string-utf8"  # Key 序列化: string-utf8/int8/uint8/.../uint64
    value_serializer: "json"  # Value 序列化: json/influx，默认 json
    acks: "0"  # 确认级别: 0/1/all，默认 "0"
    compression: "none"  # 压缩类型: none/gzip/snappy/lz4/zstd，默认 none
    tbname_key: "table"  # JSON 中表名字段名，默认 "table"
    records_per_message: 1  # 每消息记录数，默认 1，范围 1-1000
```

#### 7.0.4 Schema 数据生成配置

**数据生成方式配置**：
**方式 1：随机生成（random，默认）**：
```yaml
columns:
  - name: temperature
    type: float
    gen_type: random  # 可省略，默认值
    distribution: uniform  # 分布类型，目前仅支持 uniform
    min: -20.0  # 最小值（包含）
    max: 50.0  # 最大值（不包含）
  - name: status
    type: int
    values: [0, 1, 2, 3]  # 从列表中随机选择
```

**方式 2：顺序递增（order，仅整数）**：
```yaml
columns:
  - name: seq_id
    type: int
    gen_type: order  # 必须指定
    min: 1  # 起始值（包含）
    max: 1000  # 最大值（不包含），达到后回绕到 min
```

**方式 3：表达式生成（expression）**：
```yaml
columns:
  - name: phase
    type: float
    gen_type: expression  # 检测到 expr 时自动推断，可省略
    expr: "math.sin(_i * 0.1) * 100 + math.random(-5, 5)"
```

表达式内置变量：

| 变量 | 类型 | 说明 |
| --- | --- | --- |
| _i | int | 调用计数器，从 0 开始递增 |
| _table | int | 当前表索引 |
| _last | float | 上一次生成的值（仅数值类型） |

**方式 4：CSV 导入（csv）**：
```yaml
schema:
  from_csv:
    tags:  # 标签数据源
      file_path: "/path/to/tags.csv"
      has_header: true  # 是否有表头行，默认 true
      tbname_index: 0  # 表名列索引，默认 -1（无）
      exclude_indices: [1, 2]  # 排除的列索引
    columns:  # 时序数据源
      file_path: "/path/to/data.csv"
      has_header: true
      tbname_index: 0  # 表名列索引
      timestamp_index: 1  # 时间戳列索引
      timestamp_precision: ms  # 时间戳精度: s/ms/us/ns
      timestamp_offset:  # 时间戳偏移
        offset_type: relative  # 类型: relative/absolute
        value: "+10s"  # 偏移值（relative 格式: ±[value][unit]）
      repeat_read: false  # 读完是否重复，默认 false
```

#### 7.0.5 Job DAG 配置生成

**Job 依赖关系**：
```yaml
jobs:
  job-name:  # Job Key，唯一标识符
    name: "显示名称"  # 可选，用于日志和 UI
    needs: [dependency-job-1, dependency-job-2]  # 依赖的 Job 列表，可选
    steps:  # Step 列表，按顺序执行
      - name: "步骤显示名"  # 可选
        uses: action-name  # Action 标识符
        with:  # Action 参数，可选
          param: value
```


### 7.1 配置验证行为

由于 taosgen 无 dry-run 模式，Skill 采用**临时运行测试验证**：
**验证流程**：
1. **生成测试配置**：基于用户配置创建临时版本，修改参数：
  - `schema.tbname.count` = 10（最小化表数量）
  - `schema.generation.rows_per_table` = 10（最小化行数量）
1. **执行临时运行**：
  ```bash
  timeout 10s taosgen -h {host} -c /tmp/test_config.yaml 2>&1 | head -50
  ```

1. **结果检查**：
  - 检查输出中是否出现 "Config validation failed"等
  - 检查是否出现 "Error"、"Fatal" 关键字
  - 检查进程退出码
1. **结果返回**：
  - **成功**：向用户展示 "✅ 配置已通过临时测试验证"
  - **失败**：解析错误信息，提供修复建议
1. **清理**：删除临时配置文件 `/tmp/test_config.yaml`
  
**常见错误识别**：

| 错误信息 | 原因 | 修复建议 |
| --- | --- | --- |
| Unknown key: broker | MQTT 使用错误的参数名 | 将 broker 改为 uri |
| Unknown key: username | MQTT 使用错误的参数名 | 将 username 改为 user |
| gen_type not allowed | 无效的 gen_type 值 | 使用 random/order/expression，或省略让系统自动推断 |
| rows_per_table not found | 参数名错误或位置错误 | 确保在 schema.generation 下使用 rows_per_table |
| precision without quotes | props 中 precision 未加引号 | 改为 `precision 'ms'` |
| failed to parse DSN | DSN 格式错误 | 检查协议、用户名、密码、主机格式 |

### 7.2 输出展示行为

Skill 生成配置后，按以下结构输出：
**1. 配置摘要**（1 句话）：
```plaintext
这是一个针对 {target_system} 的性能测试配置，模拟 {table_count} 个设备，每设备 {rows_per_table} 行数据，使用 {concurrency} 并发写入，优化目标为 {performance_mode}。
```

**2. 完整 YAML**（代码块）：
```yaml

## 8. 完整配置内容，带注释说明关键参数

```

**3. 验证状态**：
```plaintext
✅ 配置已通过临时测试验证（使用 10 表 x 10 行测试）
或
⚠️ 配置验证失败：{error_message}
建议修复：{fix_suggestion}
```

**4. 关键参数说明**（2-4 点）：
```markdown
- **{param1}**: {value1} - {选择理由}
- **{param2}**: {value2} - {选择理由}
- **{param3}**: {value3} - {选择理由}
```

**5. 使用命令**：
```bash
taosgen -c {config_file_path}

## 9. 或带参数运行

taosgen -h {host} -c {config_file_path}
```

**6. 输出路径**（必须）：
```plaintext
OutputPath: /absolute/path/to/file
```

**7. 反馈询问**：
```plaintext
这个配置是否符合您的需求？是否需要调整某些参数？例如：
- 修改并发数（当前 {concurrency}）
- 调整批次大小（当前 {rows_per_batch}）
- 添加 checkpoint 配置（当前未启用）
- 修改数据生成方式
```

### 9.1 文件输出行为

输出到用户指定路径或当前工作路径。

## 10. 性能

### 10.1 性能指标要求

| 指标 | 目标值 | 说明 |
| --- | --- | --- |
| 配置生成响应时间（简单） | < 10 秒 | < 100 行 YAML，如基础 TDengine 配置 |
| 配置生成响应时间（复杂） | < 60 秒 | > 500 行 YAML，如多 Job DAG、CSV 导入配置 |
| 文档 WebFetch 超时 | 10 秒 | 超过则自动 Fallback 到本地文档 |
| 本地文档读取时间 | < 1 秒 | 单个 reference 文件读取和解析 |
| 配置验证时间 | < 15 秒 | 包含临时配置创建、运行、检查、清理 |
| 单次 Skill 调用总时间 | < 120 秒 | 包含需求理解、文档获取、生成、验证、输出 |

### 10.2 性能优化策略

1. **文档分层加载**：
  - L1（元数据）：启动时加载，~50 tokens
  - L2（SKILL.md）：激活时加载，~2000 tokens
  - L3（Reference）：需要时加载，~3000-5000 tokens/文件
  - L4（WebFetch）：异步尝试，失败无阻塞
1. **缓存机制**：
  - 同一会话内缓存已读取的 reference 文件
  - 缓存 WebFetch 结果（短期缓存 5 分钟）
1. **验证优化**：
  - 使用最小数据量（10 表 x 10 行 = 100 行数据）
  - 使用 timeout 命令限制验证时间（10 秒）
  - 仅检查前 50 行输出，避免日志过多

### 10.3 规模配置支持

| 配置规模 | 处理策略 |
| --- | --- |
| 小型（< 1000 表） | 完整生成，直接验证 |
| 中型（1k-10k 表） | 完整生成，验证时使用 10 表测试 |
| 大型（10k-100k 表） | 完整生成，建议用户分批验证 |
| 超大型（> 100k 表） | 生成分段配置，建议使用 tables_reuse_data: true |

## 11. 安全

### 11.1 敏感信息处理

**密码处理策略**：
1. **不硬编码**：生成的配置中不应包含真实密码明文
2. **密码输入建议**：
**Skill 行为约束**：
- 不得索要用户的真实密码
- 不得在日志中记录密码
- 不得将包含密码的配置保存到共享位置

### 11.2 配置安全

**风险提示**：
- 当配置包含 `drop_if_exists: true` 时，必须明确提示用户风险：
**安全协议推荐**：
- MQTT：优先推荐使用 ssl:// 或 wss://（TLS 加密）
- Kafka：优先推荐使用 SASL_SSL 或 SSL（加密 + 认证）
- TDengine：推荐使用 wss://（WebSocket TLS）

### 11.3 命令安全

**禁止的操作**：
- 不得在生成的命令中包含破坏性 shell 命令（rm -rf, format, dd 等）
- 不得在配置中引用系统敏感文件（/etc/passwd, ~/.ssh/id_rsa 等）

## 12. 兼容性

### 12.1 taosgen 版本兼容


| taosgen 版本 | 兼容状态 | 说明 |
| --- | --- | --- |
| v0.8.0+ | ✅ 完全兼容 | 推荐版本，支持所有文档中的参数 |
| v0.7.x或以下 | ❌ 不兼容 | 可能缺少部分新参数或格式差异 |

**版本检测建议**：
Skill 可建议用户检查版本：
```bash
taosgen --version
```

### 12.2 目标系统版本兼容

| 目标系统 | 版本要求 | 说明 |
| --- | --- | --- |
| TDengine | 3.x | 生成的配置兼容 TDengine 3.x 版本 |
| MQTT Broker | 3.1.1 / 5.0 | 支持标准 MQTT 协议 |
| Kafka | 2.x / 3.x | 通过 librdkafka 兼容 |

### 12.3 向后兼容性

Skill 生成的配置遵循官方文档最新规范，与旧版本 taosgen 的兼容性由 taosgen 自身保证。
**已废弃参数处理**：
- Skill 不生成已废弃的参数
- 如用户提及旧参数名，Skill 应提示使用新参数名

## 13. 运维

### 13.1 部署影响

**对用户的影响**：
1. **安装要求**：用户需要安装 taosgen 工具以使用生成的配置
2. **环境准备**：用户需要准备目标系统（TDengine/MQTT/Kafka）服务
3. **配置文件管理**：建议用户将生成的配置纳入版本控制（Git）
**对运维的影响**：
- 无额外运维负担，Skill 本身不运行服务
- 生成的配置文件需要用户自行管理

### 13.2 Skill 维护

**文档更新策略**：
1. 定期（每月）对比本地 reference 文档与官方文档的差异
2. 当官方文档更新时，同步更新本地 reference 文件

**版本管理**：
- Skill 版本遵循语义化版本（Semantic Versioning）
- 重大更新（如支持新的目标系统）升级主版本号
- 文档同步更新升级次版本号
- Bug 修复升级修订号

### 13.3 故障排查支持

**用户支持清单**：
当用户报告配置问题时，收集以下信息：
1. taosgen 版本（`taosgen --version`）
2. 生成的配置文件（脱敏后）
3. 错误输出日志（前 50 行）
4. 目标系统类型和版本
5. Skill 调用时的用户输入

## 14. 使用场景

### 14.1 场景 1：TDengine 基础性能测试

**用户**：数据库性能测试工程师
**需求**：快速生成标准性能测试配置，评估 TDengine 写入性能
**输入**：
```plaintext
"创建一个标准的 TDengine 性能测试配置，测试 10000 个智能电表，
每个电表有电流、电压、相位三个指标，使用 stmt 格式写入，
目标是最大吞吐量"
```

**Skill 行为**：
1. 识别目标系统：TDengine
2. 识别性能模式：吞吐量优先
3. 推荐参数：concurrency=20, format=stmt, rows_per_batch=30000
4. 生成配置并验证
**输出配置特点**：
- 高并发（16-20）
- 大批次（20000-50000）
- stmt 参数绑定方式写入

### 14.2 场景 2：MQTT IoT 设备模拟

**用户**：IoT 平台开发者
**需求**：模拟 5000 个传感器设备向 MQTT Broker 发布数据
**输入**：
```plaintext
"生成 MQTT 配置，模拟 5000 个温湿度传感器，
主题格式是 devices/{sensor_id}/telemetry，QoS 1，
每个消息包含 5 条记录"
```

**Skill 行为**：
1. 识别目标系统：MQTT
2. 配置 topic 使用 {table} 占位符
3. 设置 qos: 1 保证消息送达
4. 配置 records_per_message: 5 提高吞吐量
**输出配置特点**：
- 中等并发（8-12）
- QoS 1（可靠性优先）
- 主题层级化（devices/{table}/telemetry）
- 消息批处理（records_per_message > 1）

### 14.3 场景 3：Kafka 高吞吐量测试

**用户**：流处理平台测试工程师
**需求**：测试 Kafka 集群的峰值吞吐量
**输入**：
```plaintext
"创建 Kafka 压测配置，100000 个设备，
使用 lz4 压缩，acks=0，16 并发，
key 使用设备 ID，value 使用 json"
```

**Skill 行为**：
1. 识别目标系统：Kafka
2. 识别性能模式：吞吐量优先
3. 配置 acks: "0"（最高吞吐）
4. 配置 compression: lz4（快速压缩）
5. 配置 key_pattern: "{table}"（按设备分区）
**输出配置特点**：
- 高并发（16-20）
- 零确认（acks: 0）
- 快速压缩（lz4/snappy）
- 高分区利用率（key 包含设备 ID）

### 14.4 场景 4：CSV 历史数据导入

**用户**：数据迁移工程师
**需求**：将历史 CSV 数据导入 TDengine
**输入**：
```plaintext
"从 CSV 文件导入数据到 TDengine，
标签数据在 tags.csv（表名在第 1 列），
时序数据在 data.csv（时间戳在第 2 列，精度毫秒），
时间戳要整体往后偏移 1 小时"
```

**Skill 行为**：
1. 识别数据源：CSV
2. 配置 from_csv.tags：指定 tbname_index 和 exclude_indices
3. 配置 from_csv.columns：指定 timestamp_index 和 offset
4. 设置 rows_per_table: -1（读取所有数据）
**输出配置特点**：
- CSV 数据源配置（from_csv）
- 时间戳偏移（+1h）
- 列映射（tbname_index, timestamp_index）
- 无限行数（rows_per_table: -1）

## 15. 约束和限制

### 15.1 约束条件

**必须满足的条件**：
1. **taosgen 已安装**：用户必须已安装 taosgen 工具才能运行生成的配置
2. **目标系统可访问**：TDengine/MQTT/Kafka 服务必须可网络访问
3. **YAML 语法正确**：生成的配置必须符合 YAML 1.2 规范
4. **参数有效**：所有参数值必须在官方文档规定的有效范围内
**使用限制**：
1. **单次 Skill 调用**：一次调用生成一个配置文件
2. **配置复杂度**：建议单个配置文件不超过 1000 行 YAML（过大文件建议拆分）
3. **验证依赖**：配置验证依赖本地 taosgen 安装，无 taosgen 时只能做 YAML 语法检查

### 15.2 功能限制

**当前版本不支持**：
1. **配置热更新**：生成的配置文件不支持运行时热更新（需重新运行 taosgen）
2. **动态参数调整**：不支持在测试运行中动态调整参数（需停止后修改配置重跑）
3. **结果自动分析**：不自动分析 taosgen 运行结果（仅提供配置生成）
4. **多版本兼容**：不自动检测 taosgen 版本并生成对应版本配置（假设用户使用的版本与文档一致）
  
**应当避免的使用方式**：
1. **超大表数量**：单配置中表数量不建议超过 1000 万（可能导致内存问题）
2. **超长时间戳步长**：timestamp step 不建议超过 1 年（可能导致时间戳溢出）
3. **循环依赖**：Job needs 中不应出现循环依赖（A needs B, B needs A）
4. **敏感信息硬编码**：密码等敏感信息不应明文写在配置中（应使用环境变量）

## 16. 常见错误和排查

### 16.1 配置生成阶段错误

**错误 1：目标系统识别失败**
- **现象**：Skill 反复询问用户目标系统
- **原因**：用户输入中缺少明确的关键词
- **排查**：用户提供明确的目标系统名称（TDengine/MQTT/Kafka）
**错误 2：文档获取失败**
- **现象**：Skill 提示 "无法获取参考文档"
- **原因**：网络不可达且本地 reference 文件缺失
- **排查**：
   - 检查网络连接（WebFetch 需要访问 docs.tdengine.com）
   - 检查本地文件：`ls skills/taosgen-config/references/`
   - 如文件缺失，从仓库重新克隆或更新
**错误 3：参数推断冲突**
- **现象**：Skill 询问用户确认参数选择
- **原因**：用户需求中存在矛盾（如 "最小延迟" + "最大批处理"）
- **排查**：用户明确优先级（延迟优先还是吞吐量优先）

### 16.2 配置验证阶段错误

**错误 1：Unknown key XXX**
- **现象**：验证时报 "Config validation failed: Unknown key 'broker'"
- **原因**：使用了错误的参数名
- **修复对照表**：


| 错误参数 | 正确参数 | 适用场景 |
| --- | --- | --- |
| broker | uri | MQTT 连接地址 |
| username | user | MQTT 认证用户名 |
| password | 同左 | MQTT/Kafka 密码（参数名正确但建议用环境变量） |
| table_count | schema.tbname.count | 表数量 |
| per_table_rows | schema.generation.rows_per_table | 每表行数 |
| batch_size | schema.generation.rows_per_batch | 批次大小 |

**错误 2：gen_type 相关错误**
- **现象**："gen_type not allowed" 或 "gen_type mismatch"
- **原因**：
  - 显式指定了错误的 gen_type 值
  - 参数组合冲突（如 gen_type: random 但提供了 expr）
- **修复**：
  - 省略 gen_type，让系统自动推断
  - 或确保 gen_type 与提供的参数匹配：
    - random → 提供 min/max 或 values
    - order → 提供 min/max（仅整数）
    - expression → 提供 expr
**错误 3：DSN 解析失败**
- **现象**："failed to parse DSN"
- **原因**：DSN 格式错误
- **排查**：
  - 格式：`protocol://user:password@host:port/database`
  - 检查特殊字符是否 URL 编码（如密码中的 @ → %40）
  - 检查协议是否支持（ws/wss/taos/taosws）

### 16.3 运行时错误

**错误 1：连接超时**
- **现象**："connection timeout" 或 "failed to connect"
- **原因**：目标系统不可达或防火墙阻挡
- **排查**：
   - 检查网络连通性：`ping host`
   - 检查端口开放：`telnet host port`
   - 检查服务状态：确认 TDengine/MQTT/Kafka 服务已启动
**错误 2：认证失败**
- **现象**："authentication failed" 或 "invalid credentials"
- **原因**：用户名或密码错误
- **排查**：
   - 检查用户名和密码
   - 确认环境变量已正确设置：`echo $TAOS_PASSWORD`
   - 手动测试连接：`taos -h host -u user -p`
**错误 3：内存不足**
- **现象**：进程被杀或 "out of memory"
- **原因**：配置参数过大（表数量 x 行数 x 批次）
- **排查**：
   - 降低 rows_per_batch
   - 设置 num_cached_batches: 0
   - 使用 tables_reuse_data: true
   - 减少并发数

## 17. 可观测性

**日志输出**：
Skill 在生成配置过程中输出以下信息：
1. 目标系统识别结果
2. 文档获取来源（WebFetch/Local）
3. 关键参数选择理由
4. 验证过程（临时测试执行）
5. 最终输出路径

## 18. 安装和卸载

### 18.1 Skill 安装

**安装方式**：通过 cowork 工具安装到本地 Claude Code 项目

```bash

## 19. 在 agent-skills 仓库目录

cd /opt/source/agent-skills

## 20. 安装到目标项目

cd /opt/source/target-project
cowork install taosdata/agent-skills --local

## 21. 或手动复制

mkdir -p .claude/skills
cp -r /opt/source/agent-skills/skills/taosgen-config .claude/skills/
```


**安装后结构**：
```plaintext
target-project/
└── .claude/skills/taosgen-config/
    ├── SKILL.md
    └── references/
        ├── tdengine.md
        ├── mqtt.md
        ├── kafka.md
        ├── schema.md
        └── common.md
```


**依赖检查**：
安装后检查以下依赖：
1. taosgen 是否安装：`which taosgen`
2. taosgen 版本：`taosgen --version`（建议 v0.8.0+）
3. 网络连接（用于 WebFetch）：`curl -I https://docs.tdengine.com/`

### 21.1 Skill 更新

**更新方式**：
```bash

## 22. 重新安装最新版本

cowork update taosdata/agent-skills

## 23. 或手动更新

cd .claude/skills/taosgen-config
git pull origin main
```

### 23.1 Skill 卸载

**卸载方式**：
```bash

## 24. 直接删除 Skill 目录

rm -rf .claude/skills/taosgen-config

## 25. 或使用 cowork

cowork uninstall taosdata/agent-skills
```

## 26. 文档

**Skill 内部文档维护**：
- `SKILL.md`：主使用说明，随 Skill 版本更新
- `references/*.md`：参数参考文档，跟随官方文档同步
  
### 26.1 文档更新流程

**官方文档同步流程**（月度）：
1. 使用 WebFetch 获取官方文档最新版本
2. 对比本地 reference 文件与官方文档的差异
3. 更新本地文件中的参数描述、默认值、示例
4. 在修订记录中标注更新日期
5. 提交 PR 到 agent-skills 仓库
  
**紧急更新触发条件**：
- taosgen 发布重大版本更新（如 v0.9.0）
- 官方文档修复关键错误（如参数名更正）
- 用户报告文档与实际行为不符

## 27. 参考文档

1. **taosgen 官方文档**: https://docs.tdengine.com/tdengine-reference/tools/taosgen/
2. **taosgen GitHub 仓库**: https://github.com/taosdata/taosgen
3. **Agent Skills 规范**: https://agentskills.io/specification
4. **YAML 1.2 规范**: https://yaml.org/spec/1.2/spec.html
5. **MQTT 3.1.1 协议规范**: http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
6. **Kafka 协议文档**: https://kafka.apache.org/protocol.html
