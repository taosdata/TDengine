# AVEVA Historian 配置项

AVEVA Historian 数据源通过 DSN 字符串传递所有配置参数，格式为：

```
historian://<username>:<password>@<host>:<port>?param1=value1&param2=value2
```

> 协议前缀也支持 `avevaHistorian://`。

---

## DSN 参数

### 连接配置

| 参数         | DSN 字段             | 类型   | 默认值      | 必填 | 说明                                                 |
| ------------ | -------------------- | ------ | ----------- | ---- | ---------------------------------------------------- |
| Server 地址  | `host`               | string | —           | ✅   | AVEVA Historian SQL Server 的 IP 地址或域名          |
| Server 端口  | `port`               | u16    | `1433`      | ❌   | SQL Server 端口，范围 0–65535                        |
| 连接超时     | `connection_timeout` | u64    | `120`（秒） | ❌   | 连接 Historian 数据库的超时时间，单位秒。最小 1      |
| 重连尝试次数 | `reconnect_times`    | usize  | `10`        | ❌   | 连接断开后的最大重试次数，超过后任务报错退出。最小 1 |
| 重连间隔     | `reconnect_interval` | usize  | `5`（秒）   | ❌   | 连接断开后的重试间隔，单位秒。最小 1                 |

> 连接的目标数据库固定为 `Runtime`。
>
> 所有 Historian 数据库连接均启用 TCP Keep-Alive（60s 探活，10s 间隔）。

### 认证配置

| 参数     | DSN 字段     | 类型   | 默认值 | 必填 | 说明                                                                          |
| -------- | ------------ | ------ | ------ | ---- | ----------------------------------------------------------------------------- |
| 用户名   | `username`   | string | —      | ✅   | SQL Server 登录用户名                                                         |
| 密码     | `password`   | string | —      | ✅   | SQL Server 登录密码                                                           |
| 加密级别 | `encryption` | enum   | `Off`  | ❌   | 连接加密级别，可选值：`Off`、`On`、`NotSupported`、`Required`（不区分大小写） |

### 采集配置

| 参数         | DSN 字段           | 类型     | 默认值                | 必填     | 说明                                                                                                                     |
| ------------ | ------------------ | -------- | --------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------ |
| 采集模式     | `mode`             | enum     | `synchronize`         | ✅       | `synchronize`：实时同步；`migrate`：历史迁移                                                                             |
| 表           | `table`            | enum     | `Runtime.dbo.History` | ✅       | 数据表，可选 `Runtime.dbo.History`（历史数据）或 `Runtime.dbo.Live`（实时数据）。支持自定义表名                          |
| 标签         | `tags`             | string   | `*`                   | ❌       | 需要迁移/同步的 TagName，逗号分隔。`*` 代表除 `Sys` 开头以外的全部 tag                                                   |
| 标签组大小   | `tagListSize`      | usize    | `10`                  | ❌       | 当 `table` 为 History 且 tag 数量超过此值时，按此大小分组查询以提升效率。范围 1–1000                                     |
| 任务开始时间 | `beginDateTime`    | DateTime | —                     | 条件必填 | RFC 3339 格式。当 `table` 为 `Runtime.dbo.History` 时必填                                                                |
| 任务结束时间 | `endDateTime`      | DateTime | —                     | 条件必填 | RFC 3339 格式。当 `mode` 为 `migrate` 且 `table` 为 History 时必填                                                       |
| 查询时间窗口 | `timeWindow`       | Duration | `1d`                  | ❌       | 历史数据迁移时每次查询的时间窗口大小。仅 `table` 为 History 时显示。支持单位：`y`/`mo`/`d`/`w`/`h`/`m`/`s`/`ms`/`u`/`ns` |
| 实时同步间隔 | `retrieveInterval` | Duration | `10s`                 | ❌       | 实时同步时每次查询的时间间隔，最小 1s。仅 `mode` 为 `synchronize` 时显示。支持单位：`d`/`h`/`m`/`s`/`ms`                 |
| 乱序时间上限 | `tolerance`        | Duration | `0ms`                 | ❌       | 容忍乱序数据延迟到达的时间上限。仅 `mode` 为 `synchronize` 且 `table` 为 History 时显示。支持单位：`d`/`h`/`m`/`s`/`ms`  |

**参数联动约束：**

- `mode=migrate` 时 `table` 不可为 `Runtime.dbo.Live`
- `table=Runtime.dbo.History` 时 `beginDateTime` 必填
- `mode=migrate` + `table=Runtime.dbo.History` 时 `endDateTime` 必填

### Payload 转换

Payload 转换允许用户定义 TDengine 中的数据模型（超级表名、子表名、普通列、标签列映射等）。

AVEVA Historian 提供以下源字段供映射：

| 字段名          | 类型      | 说明                                             |
| --------------- | --------- | ------------------------------------------------ |
| `DateTime`      | timestamp | 值对应的时间戳                                   |
| `TagName`       | varchar   | 测点名称                                         |
| `Value`         | double    | 数值类型的值。对于字符串 tag，该值始终为 NULL    |
| `vValue`        | varchar   | 字符串形式的值，允许使用混合数据类型             |
| `Quality`       | int       | 与数据值关联的基本数据质量指标                   |
| `QualityDetail` | int       | 数据质量的内部表示                               |
| `OPCQuality`    | int       | 从数据源接收到的 OPC 质量值                      |
| `wwTagKey`      | int       | 单个 AVEVA Historian 实例中 tag 的唯一数字标识符 |
| `wwResolution`  | int       | 循环模式下检索数据的采样率（毫秒）               |
| `StartDateTime` | timestamp | 返回该行对应检索周期的开始时间                   |
| `SourceTag`     | varchar   | 存储该点时复制标记的源标记名称                   |
| `SourceServer`  | varchar   | 存储该点时复制标记的服务器名称                   |

### 高级选项

| 参数             | DSN 字段                        | 类型     | 默认值                         | 范围     | 说明                                                    |
| ---------------- | ------------------------------- | -------- | ------------------------------ | -------- | ------------------------------------------------------- |
| 最大读取并发数   | `read_concurrency`              | int      | `0`                            | 0–1000   | 数据源连接数/读取线程数限制，0 表示自动                 |
| 批次大小         | `batch_size`                    | int      | `10000`                        | 1–100000 | 单次发送的最大行数                                      |
| 保存原始数据     | `keep_raw_data`                 | bool     | `false`                        | —        | 是否保存原始数据                                        |
| 最大保留天数     | `keep_raw_data_days`            | int      | `1`                            | 1–365    | 原始数据最大保存天数                                    |
| 原始数据存储目录 | `keep_raw_data_dir`             | string   | `$DATA_DIR/tasks/:id/rawdata/` | —        | 自定义原始数据存储目录                                  |
| 健康监测时段     | `health_check_window_in_second` | Duration | `0s`                           | 0–60000s | 对最近多长时间的任务状态进行统计                        |
| Busy 状态阈值    | `busy_threshold`                | percent  | `100%`                         | 0–100%   | 写入队列入队元素数量与队列长度之比                      |
| 写入队列长度     | `max_queue_length`              | int      | `1000`                         | 0–10000  | 单个 IPC 连接对应的写入队列长度最大值                   |
| 写入错误阈值     | `max_errors_in_window`          | int      | `10`                           | 0–10000  | 健康监测时段内允许的写入错误数量，超出则触发 Fatal 告警 |

### 异常处理策略

异常处理策略（`exceptionStrategy`）定义了数据写入过程中各类错误的处理方式。该配置为所有数据源共用，非 AVEVA Historian 独有。

**数据库级异常：**

| 场景           | DSN 字段                    | 默认策略 | 可选策略                            |
| -------------- | --------------------------- | -------- | ----------------------------------- |
| 数据库连接异常 | `database_connection_error` | `cache`  | `cache`、`archive`、`skip`、`break` |
| 数据库不存在   | `database_not_exist`        | `break`  | `archive`、`skip`、`break`          |

**表级异常：**

| 场景           | DSN 字段                           | 默认策略                   | 可选策略                                                       |
| -------------- | ---------------------------------- | -------------------------- | -------------------------------------------------------------- |
| 表不存在       | `table_not_exist`                  | `retry`                    | `archive`、`skip`、`break`、`retry`                            |
| 表名过长       | `table_name_length_overflow`       | `archive`                  | `archive`、`skip`、`break`、`truncate`、`truncate_and_archive` |
| 表名含非法字符 | `table_name_contains_illegal_char` | `replace_to`（替换为 `_`） | `archive`、`skip`、`break`、`replace_to`                       |

**时间戳异常：**

| 场景         | DSN 字段                     | 默认策略  | 可选策略                                       |
| ------------ | ---------------------------- | --------- | ---------------------------------------------- |
| 主时间戳溢出 | `primary_timestamp_overflow` | `archive` | `archive`、`skip`、`break`                     |
| 主时间戳为空 | `primary_timestamp_null`     | `archive` | `archive`、`skip`、`break`、`use_current_time` |

**字段级异常：**

| 场景             | DSN 字段                     | 默认策略    | 可选策略                                                       |
| ---------------- | ---------------------------- | ----------- | -------------------------------------------------------------- |
| 主键为空         | `primary_key_null`           | `archive`   | `archive`、`skip`、`break`                                     |
| 字段名不存在     | `field_name_not_found`       | `add_field` | `archive`、`skip`、`break`、`add_field`                        |
| 字段名过长       | `field_name_length_overflow` | `archive`   | `archive`、`skip`、`break`                                     |
| 字段长度自动扩展 | `field_length_extend`        | `true`      | `true`、`false`                                                |
| 字段长度溢出     | `field_length_overflow`      | `archive`   | `archive`、`skip`、`break`、`truncate`、`truncate_and_archive` |

**其他异常：**

| 场景           | DSN 字段                                    | 默认策略                 | 可选策略                            |
| -------------- | ------------------------------------------- | ------------------------ | ----------------------------------- |
| 写入错误       | `ingesting_error`                           | `archive`                | `archive`、`skip`、`break`          |
| 模板变量不存在 | `variable_not_exist_in_table_name_template` | `replace_to`（`"NULL"`） | `skip`、`leave_blank`、`replace_to` |

**缓存与归档配置：**

| 参数                           | 默认值                         | 范围                      | 说明                 |
| ------------------------------ | ------------------------------ | ------------------------- | -------------------- |
| `connection_timeout_in_second` | `30s`                          | 1–600s                    | 连接超时             |
| `cache.keep_days`              | `30d`                          | 0–65535                   | 缓存保留天数         |
| `cache.max_size`               | `1GB`                          | 0–65535 MB/GB             | 缓存最大体积         |
| `cache.rotate_count`           | `100`                          | 0–65535                   | 缓存轮转次数         |
| `cache.location`               | `$DATA_DIR/tasks/:id/cache`    | —                         | 缓存存储路径         |
| `cache.on_fail`                | `skip`                         | `skip`、`break`           | 缓存失败时的处理策略 |
| `archive.keep_days`            | `30d`                          | 0–65535                   | 归档保留天数         |
| `archive.max_size`             | `1GB`                          | 0–65535 MB/GB             | 归档最大体积         |
| `archive.rotate_count`         | `100`                          | 0–65535                   | 归档轮转次数         |
| `archive.location`             | `$DATA_DIR/tasks/:id/archived` | —                         | 归档存储路径         |
| `archive.on_fail`              | `rotate`                       | `rotate`、`skip`、`break` | 归档失败时的处理策略 |
