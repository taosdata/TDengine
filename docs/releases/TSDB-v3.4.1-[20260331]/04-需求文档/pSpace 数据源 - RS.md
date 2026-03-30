# pSpace 数据源 - RS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-05 | 2026-03-05 | 1.0 | @杨志宇 | 初始版本 |

## 2. 引言

### 2.1 术语与缩写名词

- **pSpace**: 力控的 pspace 数据库。
- **DSN**: Data Source Name，数据源名称，用于配置数据源连接

### 2.2 相关文档资料

- [需求报告 TX-822](https://taosdata.feishu.cn/wiki/OaNhwAnv0i15nYkGfUrcB5TgnOf)
- 

### 2.3 优先级要求

- **优先级**: 中
- **期望交付时间**: 20260331 迭代
- **重要程度**: 为用户提供更多数据源选项，增强产品竞争力

### 2.4 版本要求

- 企业版支持
- 计划在 taosX 3.4.0 版本发布

## 3. 需求目标

力控和亚控在行业中并称工控双雄，实时库部署有较深厚的市场基础，此次客户是冀南钢铁集团有限公司升级改造，需要将部署的力控pspace数据迁移到TDengine中。销售已经进行项目承诺。
本需求的目标是：
1. 需要将其迁移到TDengine中，并进行实时同步，类似KingHistorian采集需求
2. 支持java和python迁移，厂家建议走java开发，性能和稳定性更好，附件是java的帮助和SDK，需要注意，客户使用的是7.0版本pspace，且是windows版本数据库。

## 4. 功能需求

支持实时同步和历史迁移，支持断线续传，支持csv和自动枚举变量两种方式，按照当前 KingHistorian 的采集功能需求为例。

## 5. 性能需求

要求单任务支持测点数不小于 5w 点，实时同步要求不小于10w row/sec，历史同步要求不小于30w rows/sec。

## 6. 安全需求

日志中不要出现密码明文。

## 7. 其他需求

### 7.1 兼容性需求

- 与现有的 taosX 任务系统兼容
- 支持标准的 DSN 配置方式

### 7.2 接口需求

DSN 格式
```shell {wrap}
pspace://<username>:<password>@<host>:<port>?<params>
```

#### 7.2.1 **连接参数**

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | 是 | — | pSpace 服务器地址 |
| `port` | 否 | `5678` | pSpace 服务器端口 |
| `username` | 是 | — | 用户名 |
| `password` | 是 | — | 密码 |
| `connect_timeout` | 否 | `30s` | 连接超时，支持 duration 格式（如 `10s`、`1m`） |

#### 7.2.2 **节点与数据点参数**

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `pspace_mode` | 是（查询时） | — | 查询模式：`nodes`（查询节点）或 `points`（查询数据点） |
| `root` | 否 | — | 根节点 ID，指定从哪个节点开始浏览 |
| `point_name_pattern` | 否 | — | 数据点名称过滤表达式，支持通配符（如 `\北京\朝阳\*气温*`） |
| `include_data_type` | 否 | — | 是否在数据点列表中返回数据类型信息 |

#### 7.2.3 **任务参数**

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `pspace_task_mode` | 是（采集时） | — | 采集模式：`query`、`subscribe`、`query_sync` |
| `start_time` | query/query_sync 必填 | — | 起始时间，ISO 8601 格式（如 `2024-01-01T00:00:00Z`） |
| `end_time` | 否 | — | 结束时间，ISO 8601 格式 |
| `time_window` | 否 | — | 时间窗口大小，duration 格式（如 `1h`、`1d`），写入 TOML 时转换为秒数 |
| `time_excursion` | 否 | — | 时间偏移，仅 `query_sync` 模式有效，duration 格式 |
| `query_interval` | 否 | — | 查询轮询间隔，仅 `query_sync` 模式有效，duration 格式 |

#### 7.2.4 **点位配置参数**

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `point_config_mode` | 否 | `select_all_points` | 点位配置模式：`select_all_points` 或 `csv_config_file` |
| `super_table_expression` | 否 | `pspace_{type}` | 超级表命名模式，`{type}` 会替换为数据类型（如 `pspace_float`） |
| `child_table_expression` | 否 | `t_{point_id}` | 子表命名模式，`{point_id}` 替换为数据点 ID |
| `table_primary_key` | 否 | `original_ts` | 主键列，可选 `original_ts`、`request_ts`、`received_ts` |
| `table_primary_key_alias` | 否 | `ts` | 主键列在 TDengine 中的别名 |
| `value_col` | 否 | `val` | 值列在 TDengine 中的别名 |
| `value_transform` | 否 | — | 值转换表达式 |
| `quality_col` | 否 | `quality` | 质量码列在 TDengine 中的别名 |
| `csv_config_file` | csv 模式必填 | — | CSV 配置文件路径，可加 `@` 前缀 |
| `csv_format` | 否 | `full` | CSV 导出格式：`preview`（仅预览数据点）或 `full`（完整配置文件） |

#### 7.2.5 **高级参数**

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `log_level` | 否 | — | Java 插件日志级别：`Error`、`Warn`、`Info`、`Debug`、`Trace` |
| `batch_size` | 否 | — | 批量写入大小 |
| `batch_timeout` | 否 | — | 批量写入超时（毫秒） |
| `read_concurrency` | 否 | — | 读取并发度 |
| `write_concurrency` | 否 | — | 写入并发度 |
| `keep_raw_data` | 否 | — | 是否保留原始数据 |
| `keep_raw_data_days` | 否 | — | 原始数据保留天数 |
| `keep_raw_data_dir` | 否 | — | 原始数据保存目录 |

### 7.3 运维需求

- 提供清晰的日志输出，便于问题排查
- 支持任务状态监控和通知
- 失败时提供详细的错误信息

### 7.4 易用性需求

- 配置简单，与其他数据源保持一致
- 错误信息清晰易懂
- 提供使用示例和文档

### 7.5 测试需求（不含测试例）

- 功能测试：验证基本的读取和写入功能
- 性能测试：验证多测点的数据迁移功能
- 错误处理测试：验证各种错误场景的处理
