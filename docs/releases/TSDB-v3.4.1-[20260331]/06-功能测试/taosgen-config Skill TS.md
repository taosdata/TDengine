# taosgen-config Skill TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-03-06 | - | 0.1.0 | 裴亚明 | 初始版本 |

## 2. 测试目标

本次测试的主要目标是验证 taosgen-config Skill 的以下能力：
- **配置生成准确性**：能够根据用户需求生成符合 taosgen 官方文档规范的 YAML 配置文件
- **多目标系统支持**：正确支持 TDengine、MQTT、Kafka 三种目标系统的配置生成
- **文档双源策略**：优先使用 WebFetch 获取官方文档，失败时正确 Fallback 到本地 reference 文档
- **配置验证机制**：通过临时运行测试验证生成的配置有效性
- **智能参数推断**：根据性能目标（吞吐量/延迟/可靠性）自动推荐合适的参数
- **数据生成方式**：正确支持随机、顺序、表达式、CSV 四种数据生成方式
- **Job DAG 编排**：正确生成多阶段 Job 依赖关系配置
- **安全性**：敏感信息（密码）不硬编码，推荐使用环境变量

## 3. 参考文档

1. **taosgen 官方文档**: https://docs.tdengine.com/tdengine-reference/tools/taosgen/
2. **taosgen GitHub 仓库**: https://github.com/taosdata/taosgen
3. **Agent Skills 规范**: https://agentskills.io/specification

## 4. 测试结论

**测试结果：通过**
taosgen-config Skill 在功能测试中表现良好，能够准确理解用户需求并生成符合官方文档规范的 YAML 配置文件。

### 4.1 关键测试数据

### 4.2 主要发现

**优势**：
- 配置生成准确性高，参数默认值与官方文档一致
- 文档 Fallback 机制在无法访问官网文档时能够无缝切换
- 临时运行验证能够有效发现配置语法错误
- 多目标系统（TDengine/MQTT/Kafka）支持完整
**待改进**：
- 由于 claude 的安全策略，WebFetch 无法获取官网文档
```shell
● Fetch(https://docs.tdengine.com/tdengine-reference/tools/taosgen/)
  ⎿  Error: Unable to verify if domain docs.tdengine.com is safe to fetch. This may be due to network restrictions or enterprise security policies blocking claude.ai.
```

## 5. 测试环境

### 5.1 硬件环境

- **CPU**: x86_64, 8 cores, 2.5GHz
- **内存**: 16GB RAM
- **磁盘**: 100GB SSD 可用空间
- **网络**: 100+Mbps，可访问互联网

### 5.2 软件环境

| 组件 | 版本 | 说明 |
| --- | --- | --- |
| 操作系统 | Ubuntu 24.04 LTS | 主要测试平台 |
| Claude Code | 最新版本 | Skill 执行环境 |
| taosgen | 最新版本 | 配置验证工具 |
| TDengine | 3.4.0.2 | TDengine 测试场景 |
| MQTT Broker | mosquitto 2.0.18 | MQTT 测试场景 |
| Kafka | 3.6.0 | Kafka 测试场景 |
| Python | 3.10.12 | YAML 验证工具 |

### 5.3 Skill 环境

```plaintext
/opt/source/agent-skills/
├── skills/taosgen-config/
│   ├── SKILL.md
│   └── references/
│       ├── tdengine.md
│       ├── mqtt.md
│       ├── kafka.md
│       ├── schema.md
│       └── common.md
```

## 6. 功能测试

### 6.1 需求理解功能

#### 6.1.1 测试要点

- 验证目标系统识别准确性（TDengine/MQTT/Kafka）
- 验证数据特征收集完整性（表数量、列定义、标签定义）
- 验证性能模式识别正确性（吞吐量/延迟/可靠性/平衡）

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 001 | TDengine 目标识别 | 输入包含"TDengine"、"时序数据库"、"超级表"等关键词，验证 Skill 正确识别目标为 TDengine | 通过 |
| 002 | MQTT 目标识别 | 输入包含"MQTT"等关键词，验证 Skill 正确识别目标为 MQTT | 通过 |
| 003 | Kafka 目标识别 | 输入包含"Kafka"等关键词，验证 Skill 正确识别目标为 Kafka | 通过 |
| 004 | 模糊输入处理 | 输入目标不明确（如只写"性能测试"），验证 Skill 主动询问用户确认目标系统 | 通过 |
| 005 | 表数量提取 | 输入"测试 10000 个设备"，验证 Skill 正确提取表数量为 10000 | 通过 |
| 006 | 列定义提取 | 输入包含"电流(float)、电压(int)"，验证正确解析列名和类型 | 通过 |
| 007 | 数据生成方式识别 | 输入"随机生成数据"、"表达式计算"，验证正确识别 gen_type | 通过 |
| 008 | 吞吐量优先模式 | 输入"最大吞吐量"、"性能压测"，验证推荐高并发(16-20)、大批次(20000+)参数 | 通过 |
| 009 | 延迟优先模式 | 输入"低延迟"、"实时写入"，验证推荐小批次(1000-5000)、低并发(4-8)参数 | 通过 |
| 010 | 可靠性优先模式 | 输入"可靠传输"、"不丢数据"，验证推荐 QoS 1/2、acks=all、重试配置 | 通过 |

### 6.2 文档获取功能

#### 6.2.1 测试要点

- 验证 WebFetch 能够成功获取官方文档
- 验证 WebFetch 失败时正确 Fallback 到本地文档
- 验证本地文档读取成功

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 001 | WebFetch 成功 | 网络正常时，验证 WebFetch 成功获取 https://docs.tdengine.com/tdengine-reference/tools/taosgen/ 内容 | 失败，claude 安全策略阻止 |
| 002 | WebFetch 失败 Fallback | WebFetch 失败 ，验证 Skill 自动切换到本地 reference 文件 | 通过 |
| 003 | 本地文档读取 | 断开网络，验证 Skill 能够读取 references/tdengine.md 并生成正确配置 | 通过 |
| 004 | 文档内容完整性 | 对比本地文档与官方文档，验证关键参数（client_id、keep_alive、acks）一致性 | 通过 |

### 6.3 配置生成功能

#### 6.3.1 测试要点

- 验证 TDengine 配置生成正确性（DSN、format、auto_create_table）
- 验证 MQTT 配置生成正确性（uri、topic、qos、retain）
- 验证 Kafka 配置生成正确性（bootstrap_servers、acks、compression）
- 验证 Schema 定义正确性（gen_type 推断、列定义、标签定义）
- 验证 Job DAG 配置正确性（依赖关系、steps、uses）

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 001 | TDengine 基础配置 | 生成 TDengine 配置，验证包含 dsn、schema、jobs | 通过 |
| 002 | TDengine DSN 格式 | 验证 DSN 格式为 "taos+ws://user:pass@host:port/db" | 通过 |
| 003 | MQTT 连接配置 | 生成 MQTT 配置，验证包含 uri、user、client_id、keep_alive，keep_alive 默认为 5 | 通过 |
| 004 | MQTT 发布配置 | 验证 topic 支持 {table} 占位符，qos 默认为 0，retain 默认为 false | 通过 |
| 005 | Kafka 连接配置 | 生成 Kafka 配置，验证包含 bootstrap_servers、topic、client_id，client_id 默认为 taosgen | 通过 |
| 006 | Kafka 生产配置 | 验证 acks 默认为 "0"，compression 默认为 "none"，key_pattern 默认为 "{table}" | 通过 |
| 007 | Schema 随机生成 | 配置随机生成列，验证 gen_type 默认为 random，包含 min/max | 通过 |
| 008 | Schema 顺序生成 | 配置顺序生成列，验证 gen_type 为 order，仅支持整数类型 | 通过 |
| 009 | Schema 表达式生成 | 配置包含 expr 属性，验证 gen_type 自动推断为 expression | 通过 |
| 010 | CSV 数据源配置 | 生成 CSV 导入配置，验证包含 from_csv、file_path、timestamp_offset | 通过 |
| 011 | Job 单阶段配置 | 生成单 Job 配置，验证包含 create-super-table 和 insert 两个 steps | 通过 |
| 012 | Job 多阶段依赖 | 生成多 Job 配置，验证 needs 依赖关系正确，DAG 无循环 | 通过 |
| 013 | Checkpoint 按需生成 | 验证默认不生成 checkpoint，明确要求后才生成 | 通过 |
| 014 | Time Interval 配置 | 生成实时模拟配置，验证 time_interval.enabled 和 interval_strategy 设置正确 | 通过 |

### 6.4 配置验证功能

#### 6.4.1 测试要点

- 验证临时测试配置文件生成正确（10 表 x 10 行）
- 验证 taosgen 运行命令执行成功
- 验证常见配置错误被正确识别和提示

#### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 001 | 临时配置生成 | 验证测试配置中表数量=10，行数量=10，其他参数保持原配置 | 通过 |
| 002 | 临时运行成功 | taosgen 临时运行（timeout 10s）无 "Config validation failed" 错误 | 通过 |
| 003 | Unknown key 识别 | 配置中使用 broker 而非 uri，验证 Skill 提示使用正确参数名 | 通过 |
| 004 | gen_type 错误识别 | 配置无效 gen_type 正确识别 | 通过 |
| 005 | DSN 格式错误识别 | 配置无效 DSN 格式，正确识别 | 通过 |

### 6.5 输出展示功能

#### 6.5.1 测试要点

- 验证配置摘要生成正确（1 句话描述）
- 验证关键参数说明（2-4 点）
- 验证输出路径格式正确（OutputPath: /absolute/path/to/file）

#### 6.5.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 001 | 配置摘要生成 | 验证输出包含配置用途、表数量、行数量、并发数等关键信息 | 通过 |
| 002 | 关键参数说明 | 验证输出包含 2-4 个关键参数及其选择理由 | 通过 |
| 003 | 使用命令生成 | 验证输出包含运行命令：taosgen -c config.yaml | 通过 |
| 004 | 输出路径格式 | 验证输出包含 OutputPath: /absolute/path/to/file（绝对路径） | 通过 |
| 005 | 反馈询问 | 验证输出包含反馈询问，提示用户可调整的参数 | 通过 |

## 7. 易用性测试

### 7.1 测试要点

- 验证 Skill 响应语言自然、易懂
- 验证错误提示清晰，包含修复建议
- 验证配置注释完整，便于理解

### 7.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 001 | 自然语言响应 | 验证 Skill 使用中文/自然语言与用户交流，无机械感 | 通过 |
| 002 | 错误提示友好性 | 配置错误时，验证提示信息包含错误原因和修复建议 | 通过 |
| 003 | 配置注释完整 | 生成的 YAML 包含关键参数的注释说明 | 通过 |
| 004 | 示例配置可读性 | 参考文档中的示例配置清晰，关键参数有注释 | 通过 |
| 005 | 参数解释清晰 | Skill 解释参数选择理由时使用通俗语言，避免过多术语 | 通过 |

## 8. 性能测试

### 8.1 测试目标

验证 Skill 的响应时间和资源消耗满足 RS 要求：
- 简单配置（<100 行 YAML）生成 < 10 秒
- 复杂配置（>500 行 YAML）生成 < 60 秒
- 文档 WebFetch < 10 秒
- 配置验证（临时运行）< 15 秒

### 8.2 用例列表

| # | 测试用例 | 测试描述 | 目标值 | 实测值 | 结果 |
| --- | --- | --- | --- | --- | --- |
| 001 | 简单配置生成时间 | 生成基础 TDengine 配置（约 80 行） | < 10s | 2.1s | 通过 |
| 002 | 中等配置生成时间 | 生成 MQTT + 多 Job 配置（约 300 行） | < 10s | 4.5s | 通过 |
| 003 | 复杂配置生成时间 | 生成多阶段 Kafka 配置（约 600 行） | < 10s | 8.3s | 通过 |
| 004 | WebFetch 超时 | 网络正常情况下 WebFetch 官方文档 | < 10s | 失败 | 网络安全策略阻止访问 |
| 005 | 本地文档读取时间 | 读取单个 reference 文件（约 4000 tokens） | < 1s | 0.4s | 通过 |
| 006 | 配置验证时间 | 临时运行测试（10 表 x 10 行） | < 15s | 7.2s | 通过 |
| 007 | 内存使用 | 生成复杂配置时内存峰值 | < 500MB | 180MB | 通过 |
| 008 | Token 消耗 | 单次 Skill 调用总 Token 消耗 | < 8000 | ~6500 | 通过 |

## 9. 安全测试

### 9.1 测试目标

- 验证 Skill 不记录或索要用户真实密码
- 验证风险提示在危险操作时出现

### 9.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| SEC-001 | 密码不记录在日志 | 检查 Skill 日志，验证不包含用户密码明文 | 通过 |
| SEC-002 | 风险提示出现 | 配置包含 drop_if_exists: true 时，验证 Skill 明确提示数据丢失风险 | 通过 |
| SEC-003 | 安全协议推荐 | MQTT/Kafka 配置默认推荐安全协议（SSL/SASL_SSL） | 通过 |

## 10. 兼容性测试

### 10.1 测试目标

- 验证 Skill 兼容 taosgen v0.8.0+
- 验证配置兼容 TDengine 3.x
- 验证生成配置可在不同操作系统使用

### 10.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| C-001 | taosgen v0.8.0 兼容 | 使用 taosgen v0.8.0 验证生成的配置 | 通过 |
| C-002 | TDengine 3.4 兼容 | 配置在 TDengine 3.4.0 环境下可正常运行 | 通过 |
| C-003 | Ubuntu 环境 | Skill 在 Ubuntu 22.04 下正常运行 | 通过 |
| C-004 | 配置可移植性 | 在 Ubuntu 生成的配置文件可在 macOS 的 taosgen 中使用 | 通过 |

## 11. 已知问题和限制

1. **taosgen 版本要求**：生成的配置针对 taosgen v0.8.0+ 优化，旧版本可能不支持部分参数
2. **单次配置规模**：建议单个配置文件不超过 1000 行 YAML，超大配置建议拆分
3. **Checkpoint 默认关闭**：除非用户明确要求，否则不生成 checkpoint 配置
4. **WebFetch 依赖网络**：网络不可用时完全依赖本地文档，可能不是最新版本
5. **验证依赖 taosgen**：配置验证需要本地安装 taosgen，无 taosgen 时仅做 YAML 语法检查
  
### 11.1 不支持场景

1. **taosgen v0.8.x 以下版本**：不保证兼容性
2. **动态配置更新**：不支持在 taosgen 运行时修改配置
3. **测试结果自动分析**：仅提供配置生成，不分析 taosgen 运行结果
4. **非标准 MQTT/Kafka 协议**：仅支持标准协议，不支持自定义扩展
