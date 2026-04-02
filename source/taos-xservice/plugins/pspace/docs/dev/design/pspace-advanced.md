# pSpace Plugin 高级选项

## 概述

本文档描述 pSpace plugin 的高级选项，适用于所有运行模式（Query、QuerySync、Subscribe）。

高级选项通过 TOML 配置文件的 `[advanced_options]` section 下发，由 taosx（Rust）从 DSN 参数解析后生成。完整的 TOML 配置结构参见 [source-pspace TOML 配置生成规则](../../../../../crates/source-pspace/docs/dev/design/toml-config-generation.md)。

## TOML 配置结构

pSpace 的完整 TOML 配置包含以下 section：

| Section              | 说明                  | 高级选项相关 |
| -------------------- | --------------------- | ------------ |
| `[connection]`       | pSpace 连接配置       | ✗            |
| `[nodes]`            | 节点配置              | ✗            |
| `[points]`           | 数据点配置            | ✗            |
| `[run]`              | 任务运行配置          | ✗            |
| `[report]`           | 上报配置（仅 remote） | ✗            |
| `[advanced_options]` | 高级选项              | ✅           |

## 1. `[advanced_options]` 配置参数

所有高级选项均为可选字段，仅在 DSN 中设置了对应参数时才会出现在 TOML 中。

| 参数             | TOML 字段            | 类型    | DSN 来源             | 默认值 | 说明                                                    |
| ---------------- | -------------------- | ------- | -------------------- | ------ | ------------------------------------------------------- |
| 日志级别         | `log_level`          | String  | `log_level`          | 无     | 日志级别：`Error` / `Warn` / `Info` / `Debug` / `Trace` |
| 读取并发数       | `read_concurrency`   | Integer | `read_concurrency`   | 无     | 读取并发数                                              |
| 写入并发数       | `write_concurrency`  | Integer | `write_concurrency`  | 无     | 写入并发数                                              |
| 批量大小         | `batch_size`         | Integer | `batch_size`         | 无     | 批量写入大小                                            |
| 批量超时         | `batch_timeout`      | Integer | `batch_timeout`      | 无     | 批量写入超时（毫秒）                                    |
| 启用原始数据保留 | `keep_raw_data`      | Boolean | `keep_raw_data`      | 无     | 是否将查询/订阅的数据写入本地文件                       |
| 保留天数         | `keep_raw_data_days` | Integer | `keep_raw_data_days` | 无     | 原始数据文件的保留天数，过期自动清理                    |
| 保存目录         | `keep_raw_data_dir`  | String  | `keep_raw_data_dir`  | 无     | 原始数据文件的存储目录                                  |

### 配置示例

```toml
[advanced_options]
log_level = "Debug"
write_concurrency = 2
batch_size = 1000
batch_timeout = 1
keep_raw_data = true
keep_raw_data_days = 7
keep_raw_data_dir = "./raw_data"
```

## 2. 原始数据保留（Raw Data）

### 功能描述

支持将 pSpace 查询/订阅到的原始数据写入本地文件（rawdata），便于数据审计、问题排查和离线分析。该功能通过 `[advanced_options]` section 中的 `keep_raw_data`、`keep_raw_data_days`、`keep_raw_data_dir` 参数控制。

### 配置示例

```toml
[report]
remote = "127.0.0.1:6055"

[advanced_options]
keep_raw_data = true
keep_raw_data_days = 7
keep_raw_data_dir = "/var/log/taos/pspace/rawdata"
```

### 参考实现

taosx 中已有两种原始数据保留机制可供参考：

1. **`RawDataLogger`**（Historian 插件使用）：基于 `flume` 通道 + `RollingFileAppender`，支持文件滚动、压缩和按天保留。位于 `taosx-core/src/plugins/raw_data.rs`。

2. **`DumpConfig`**（OPC 插件使用）：通过 TOML 配置传递给外部进程，在外部进程内部完成数据写入。位于 `taosx-core/src/plugins/runners/opc/config/collect/dump.rs`。

pSpace 作为 Java 外部插件，模式与 OPC 类似（外部进程），建议参考 `DumpConfig` 的方式，在 Java 插件内部实现原始数据写入。

## 3. 完整配置示例

### 正常运行（上报 taosx + 保留原始数据）

```toml
[connection]
server = "192.168.2.149"
port = 8889
username = "admin"
password = "admin888"
timeout_sec = 30

[nodes]
root = 150016

[points]
include_data_type = true
point_ids = [150019]

[run]
mode = "Query"
start_time = "2026-03-01T00:00:00+08:00"
end_time = "2026-03-01T05:00:00+08:00"
time_window = 3600
time_excursion = 0

[report]
remote = "127.0.0.1:6051"

[advanced_options]
log_level = "Debug"
keep_raw_data = true
keep_raw_data_days = 7
keep_raw_data_dir = "./raw_data"
```

## 相关代码

### taosx（Rust）

- PspaceConfig 定义：`crates/source-pspace/src/config.rs`
- TOML 生成与下发：`crates/source-pspace/src/lib.rs`（`pspace_to_taos`）
- AdvancedOptions 定义：`taosx-core/src/plugins/config/mod.rs`
- TOML 配置生成规则文档：`crates/source-pspace/docs/dev/design/toml-config-generation.md`
- RawDataLogger：`taosx-core/src/plugins/raw_data.rs`
- OPC DumpConfig：`taosx-core/src/plugins/runners/opc/config/collect/dump.rs`
- IPC TCP 监听：`taosx-core/src/plugins/mod.rs`（`build_ipc`）
- IPC 数据接收：`taosx-core/src/plugins/sink/mod.rs`（`ipc_tcp_read`）

### pSpace（Java）

- 当前 ReportConfig：[ReportConfig.java](../../../src/main/java/com/taosdata/taosx/pspace/config/ReportConfig.java)
- ArrowWriter：[PSpaceArrowWriter.java](../../../src/main/java/com/taosdata/taosx/pspace/arrow/PSpaceArrowWriter.java)
- Netty 客户端：[PSpaceNettyClient.java](../../../src/main/java/com/taosdata/taosx/pspace/netty/PSpaceNettyClient.java)
