# taosAdapter 支持 HTTP JSON 写入 - RS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-09-30 | 2025-09-30 | 1.0 | 霍琳贺 | 初稿 |

## 2. 引言

### 2.1 术语与缩写名词

- **POST**：HTTP 协议中的请求方法，用于向服务器提交数据
- **JSON**：轻量级数据交换格式
- **JSONata**：JSON 查询与转换语言，用于从复杂 JSON payload 中提取/重组目标数据
- **Rule**：一条解析规则配置，用于定义某个 `endpoint` 的解析与写入行为
- **dry_run**：调试参数，仅返回转换后的 JSON 与生成的 SQL，不执行写入
- **IANA 时区**：IANA Time Zone Database 中定义的时区标识（如 `Asia/Shanghai`）

### 2.2 相关文档资料

- JSONata 文档：`https://docs.jsonata.org/overview.html`
- JSONata 在线调试：`https://try.jsonata.org/`（需选择 1.5.4）

### 2.3 优先级要求

- **重要程度**：高（面向 IoT/HTTP 直连写入场景）
- **期望交付时间**：2025-12-31

### 2.4 版本要求

- **适用范围**：taosAdapter
- **开源/企业**：社区版与企业版均支持
- **依赖**：TDengine 写入能力（通过 taosAdapter 连接池写入）

## 3. 需求目标

### 3.1 背景

许多 IoT 设备通过 MQTT 上报 JSON payload，但也存在直接通过 HTTP POST 上报 JSON 的客户场景。为了降低接入成本，需要 taosAdapter 提供一个“HTTP + JSON + 可配置映射”的写入入口，将用户上报的 JSON 解析、转换并写入 TDengine。

### 3.2 目标

1. 提供 HTTP POST 接口接收 JSON 数据
2. 支持通过配置文件定义解析与写入规则（路由、库/表映射、字段映射、时间解析等）
3. 支持 JSONata 规则将复杂 JSON 转换为写入所需的一维数组结构
4. 自动拼接 SQL（自动建表写入语句）并写入 TDengine
5. 提供 `dry_run` 调试能力，便于定位 JSON 转换/SQL 生成问题
6. 提供基础可观测性指标，便于监控成功/失败/在途等数据

## 4. 功能需求

| 序号 | 功能类别 | 功能名称 | 功能描述 |
| --- | --- | --- | --- |
| 1 | 接口 | HTTP JSON 写入接口 | 提供 `POST /input_json/v1/{endpoint}` 接收 JSON 数据并写入 TDengine |
| 2 | 配置 | input_json 总开关与规则列表 | 新增 `input_json.enable`、`input_json.rules`，仅支持配置文件配置 |
| 3 | 配置 | endpoint 路由规则 | 每条规则绑定一个 `{endpoint}`，用于匹配请求并应用对应的转换与写入配置 |
| 4 | 转换 | JSONata 转换 | 支持配置 `transformation`（JSONata 表达式），仅支持 JSONata **1.5.4** |
| 5 | 数据模型 | 一维数组写入模型 | JSONata 转换结果必须为**打平的一维数组**，数组每个元素对应一行写入 |
| 6 | 时间 | 时间字段提取与解析 | 支持 `timeKey`/`timeFormat`/`timezone`/`timeFieldName`，未设置 `timeKey` 时使用接收时间 |
| 7 | 字段 | fields 映射与可选字段 | 使用 `fields: []Field` 描述标签/列（不含时间列），支持 `optional=true` 忽略缺失字段 |
| 8 | SQL | SQL 拼接与合并策略 | 对同库同超级表且字段齐全的数据合并写入；SQL 总体大小应拼接至接近 1MB 再提交 |
| 9 | SQL | 字符串转义与注入防护 | 字符串单引号包裹并按规则转义；避免多语句注入与回显风险 |
| 10 | 调试 | dry_run | `dry_run=true` 返回转换后的 JSON 与生成的 SQL，不执行写入 |
| 11 | 连接 | 连接池复用 | 与 RESTful/其他 schemaless 接口共用连接池 |
| 12 | 可观测性 | 指标采集 | 增加 `adapter_input_json` 指标表（total/success/fail/inflight/affected 等） |
| 13 | 错误处理 | 常见错误可定位 | JSON 转换失败/写入失败需在日志中可定位，错误码与典型原因需可追溯 |
| 14 | 未来规划（非本期） | batch/flush | 预留 `batch`、`batchSize`、`batchTimeout` 与 `flush` 接口设计，但本周期不要求交付 |

## 5. 性能需求

1. JSON 转换耗时与 JSONata 复杂度相关，要求整体处理在可接受范围内（以业务场景压测结果为准）
2. 单请求写入 SQL 允许拼接至接近 1MB 后提交
3. 不应破坏现有连接池复用与稳定性

## 6. 安全需求

1. 所有接口必须进行身份验证（与 SQL 执行接口一致）
2. 防 SQL 注入：
  - 关键字符需按规则转义
  - 即便构造多条语句也仅识别第一条语句（以实现策略为准）
1. 接口仅返回影响行数/调试信息（`dry_run`），不返回查询结果，避免敏感数据回显

## 7. 其他需求

### 7.1 兼容性需求

- 无特殊兼容性要求

### 7.2 接口需求

- 与 taosAdapter 现有认证与连接池保持一致

### 7.3 运维需求

1. 上线前必须针对目标 JSON payload 与配置进行写入测试
2. 多节点部署时，各节点 taosAdapter 配置需保持一致

### 7.4 易用性需求

- 提供 `dry_run` 用于快速定位配置/转换问题

### 7.5 测试需求（不含测试例）

1. 单元测试：时间解析（各预设格式）、字符串转义、optional 字段缺失逻辑
2. 集成测试：curl 请求写入、dry_run 返回内容正确性
3. 失败场景：数据库不存在、超级表不存在、字段不存在、数据长度超限、类型不匹配等错误可定位

## 8. 约束与限制

1. 需提前创建 db 与超级表
2. 写入不会自动变更数据类型与长度
3. 大数字可能在转换过程中产生精度丢失；如纳秒时间戳建议使用字符串传递
4. 未来 batch 写入失败时错误信息写入日志；batch 模式重启存在丢数据风险（本期非交付项）
