# 需求规格说明书（Requirement Spec）- taosBackup

# 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-31 | 2026-03-31 | 1.0 | Alex Duan | 初版创建，基于源代码反推 |

# 引言

## 术语与缩写名词

| 术语 | 定义 |
| --- | --- |
| STB | Super Table，超级表，TDengine 中带标签的模板表 |
| CTB | Child Table，子表，隶属于某个超级表的具体表 |
| NTB | Normal Table，普通表，不隶属于任何超级表的独立表 |
| VTAB | Virtual Table，虚拟表（含 VIRTUAL_NORMAL_TABLE 和 VIRTUAL_CHILD_TABLE） |
| VSTB | Virtual Super Table，虚拟超级表 |
| Checkpoint | 断点续传记录，备份使用 `backup_complete.flag` 文件标记完成，恢复使用 `restore_checkpoint.txt` 逐文件记录 |
| STMT / STMT2 | TDengine 参数化写入 API，STMT1 为旧版，STMT2（v3.3+）支持多表绑定，性能更高 |
| DSN | Data Source Name，云连接字符串，格式 `https://host?token=<TOKEN>` |
| Native | 原生连接模式（CONN_MODE_NATIVE=0），通过 libtaos 直连 TDengine |
| WebSocket | WebSocket 连接模式（CONN_MODE_WEBSOCKET=1），通过 taosadapter 连接 |

## 相关文档资料

- TDengine 官方文档：[https://docs.tdengine.com](https://docs.tdengine.com)
- taosdump 工具文档
- TDengine STMT2 API 文档
- Apache Parquet 格式规范

## 优先级要求

P0 级核心功能，作为 TDengine 生态工具链的重要组成部分，需随 TDengine 主版本同步发布。

## 版本要求

- 开源版本：是
- 企业版支持：是
- 目标发布版本：TDengine 3.4.x 及以上

# 需求目标

TDengine 现有的备份工具 taosdump 采用 Avro 行存储格式存储数据，在大数据量场景下备份/恢复速度慢、文件体积大。需要设计并实现一个新的备份恢复工具 taosBackup，采用列存储方式，具备以下核心目标：

1. **高性能**：备份/恢复速度显著优于 taosdump，支持多线程并行处理
2. **紧凑存储**：采用私有二进制列存格式（zstd 逐列压缩）或 Apache Parquet 列存格式存储数据，文件体积显著小于 Avro 行存储
3. **完备性**：支持 TDengine 全部数据类型（含 DECIMAL）、全部精度（ms/us/ns）、全部表类型（STB/CTB/NTB/VTAB/VSTB）
4. **灵活性**：支持全量备份、时间范围过滤备份、Schema-only 备份、指定库/表备份
5. **可靠性**：支持断点续传（Checkpoint），备份/恢复中断后可从上次断点继续
6. **云原生**：支持通过 DSN 或 WebSocket 连接 TDengine Cloud

# 功能需求

| 序号 | 功能类别 | 功能名称 | 功能描述 |
| --- | --- | --- | --- |
| 1 | 备份 | 全量数据库备份 | 不指定 `-D` 时，通过 `SHOW DATABASES` 发现所有非系统数据库（排除 `information_schema`、`performance_schema`）并逐库备份 |
| 2 | 备份 | 指定数据库备份 | 通过 `-D db1,db2,...` 指定一个或多个数据库（上限 64 个） |
| 3 | 备份 | 指定表备份 | 位置参数 `taosBackup dbname tb1 tb2 ...` 指定库内特定子表（上限 1000 个），仅备份指定表的数据 |
| 4 | 备份 | Schema-only 备份 | `-s` / `--schemaonly` 仅备份 DDL（db.sql / stb.sql / ntb.sql / vtb.sql / *.csv），跳过数据文件 |
| 5 | 备份 | 时间范围过滤 | `-S` / `-E` 参数指定起止时间，支持 epoch 毫秒或 ISO8601 格式，生成 `WHERE ts >= ... AND ts <= ...` 过滤条件 |
| 6 | 备份 | 元数据备份 | 按库导出：db.sql（`SHOW CREATE DATABASE`）、stb.sql（`SHOW CREATE TABLE`，每 STB 一行）、{stb}.csv（DESCRIBE 输出的列 Schema）、tags/ 目录存放标签数据、ntb.sql、vtb.sql、vtags/ |
| 7 | 备份 | 二进制格式（默认） | 私有二进制格式 `.dat` 文件，Magic 头 `TAOS`，版本号 2，逐列 zstd 压缩，支持 All-NULL 列优化（COL_LEN_ALL_NULL = -1） |
| 8 | 备份 | Parquet 格式 | `-F parquet` 使用 Apache Parquet `.par` 文件，通过 Arrow C++ 静态库写入，支持 DECIMAL precision/scale 保留（仅 Linux/macOS） |
| 9 | 备份 | 断点续传（备份） | 每库备份成功后写入 `backup_complete.flag`；`-C` 运行时若该 flag 存在则跳过该库 |
| 10 | 备份 | 多线程并行 | 数据线程 `-T`（默认 8）、标签线程 `-m`（默认 2）分别并行处理 CTB 数据和标签导出 |
| 11 | 备份 | 进度显示 | TTY 模式：每秒刷新滚动行（`\r\033[K`）；非 TTY（管道/CI）：每 30 秒输出一行。显示 db/stb 进度、ctb 计数/百分比、行数、速度（行/秒）、ETA |
| 12 | 备份 | 空表跳过 | 查询结果为 0 行的 CTB/NTB 不生成 `.dat`/`.par` 文件，先写 `.tmp` 原子重命名 |
| 13 | 备份 | 目录分片 | 每个目录最多 100,000 文件（FOLDER_MAXFILE），超出后自动创建 `_data1/`、`_data2/` 子目录 |
| 14 | 恢复 | 全量恢复 | 不指定 `-D` 时扫描备份目录，发现所有含 `db.sql` 的子目录并逐库恢复 |
| 15 | 恢复 | 指定数据库恢复 | 通过 `-D` 指定恢复特定数据库 |
| 16 | 恢复 | 数据库重命名 | `-W "db1=newdb1\|db2=newdb2"` 批量重命名（最多 64 对） |
| 17 | 恢复 | STMT2 批量写入（默认） | 默认使用 STMT2 API，支持多表模式（一次 prepare 绑定多个 CTB），batch 默认 10,000 行，上限 16,384 行 |
| 18 | 恢复 | STMT1 兼容写入 | `-v 1` 使用旧版 STMT1 API，batch 默认 60,000 行，上限 100,000 行 |
| 19 | 恢复 | Schema 变更适配 | 恢复前解析 DESCRIBE 对比服务端与备份的列定义，不匹配时生成 ColMapping 进行部分列写入 |
| 20 | 恢复 | 断点续传（恢复） | 逐文件记录到 `restore_checkpoint.txt`（哈希表 256 桶 O(1) 查找）；`-C` 跳过已恢复文件；成功后删除该文件 |
| 21 | 恢复 | 虚拟表恢复 | 从 vtb.sql 执行 CREATE VTABLE / CREATE STABLE ... VIRTUAL DDL，从 vtags/ 恢复虚拟标签数据 |
| 22 | 连接 | Native 连接（默认） | CONN_MODE_NATIVE，通过 libtaos 原生协议 |
| 23 | 连接 | WebSocket 连接 | `-Z WebSocket` 或设定 DSN 时自动切换 |
| 24 | 连接 | DSN/Cloud 连接 | `-X <DSN>` 或环境变量 `TDENGINE_CLOUD_DSN`（命令行覆盖环境变量） |
| 25 | 连接 | 连接池 | 池大小 = `dataThread*2 + tagThread + 4`（默认 22）；空闲/忙碌/连接中三态管理；pthread 条件变量等待 |
| 26 | 连接 | 连接重试 | 指数退避：初始 1s，最大 30s，`-k` 配置重试次数（默认 3），`-z` 配置重试间隔基准（默认 1000ms）；可重试错误码覆盖 RPC_NETWORK_ERROR / RPC_TIMEOUT / SYN_NOT_LEADER 等 |
| 27 | 其他 | 版本信息 | `-V` 显示 `TD_VER_NUMBER`、git commit SHA、构建信息 |
| 28 | 其他 | 调试模式 | `-g` 启用 DEBUG 级日志 |
| 29 | 其他 | 信号处理 | 注册 SIGINT/SIGTERM 处理函数，设置 `g_interrupted` 标志位，各线程检查该标志优雅退出 |

# 性能需求

基准测试环境：12 核 CPU，62 GiB RAM，TDengine 3.4.1.0.alpha，10,000 子表 × 10,000 行 = 1 亿行，vgroups=8。实测数据（见 `docs/benchmark.md`）：

| 场景 | 格式 | 线程 | 耗时 | 吞吐（行/秒） | 文件大小 |
| --- | --- | :---: | :---: | :---: | :---: |
| 备份 | binary | T8 | 28s | 3,623,000 | 509 MB |
| 备份 | binary | T1 | 1m22s | 1,225,000 | 509 MB |
| 备份 | parquet | T8 | 51s | 1,967,000 | 548 MB |
| 恢复 | binary | T8 | 29s | 3,435,000 | — |
| 恢复 | binary | T1 | 1m03s | 1,582,000 | — |

文件体积对比：binary 列存格式 509 MB 约为 taosdump Avro 行存格式 1.2 GB 的 42%。

# 安全需求

1. 用户名和密码通过命令行参数传入，不存储在备份文件中
2. 备份文件为本地文件，访问权限由操作系统文件权限管控，工具本身不添加加密

# 其他需求

## 兼容性需求

1. 支持 TDengine v3.0.0.0 及以上版本（STMT2 需 v3.3+）
2. 平台支持：Linux（x86_64、x86、ARM64、ARMv7l、MIPS64、LoongArch64）和 macOS（Darwin/Apple Silicon）
3. Parquet 格式仅在 Linux 和 macOS 上编译启用，通过 `#ifndef TD_WINDOWS` 守护
4. 支持 ms、us、ns 三种时间精度数据库的备份与恢复

## 接口需求

1. 纯命令行工具，无编程 API
2. 输出格式：启动摘要（`===` 框线显示连接参数、数据库列表、线程数等）、进度行、结尾统计（db/stb/ctb 计数、总行数、总文件大小、耗时、结果码）

## 运维需求

1. 仅依赖 TDengine 客户端库（libtaos），Parquet 相关静态链接 Arrow/Parquet
2. 非 TTY 环境（CI/管道）每 30 秒输出一行进度，适配日志采集
3. 中断后通过 `-C` 参数续传，无需手工清理中间文件

## 易用性需求

1. `--help` 输出完整帮助文本，覆盖所有参数、默认值及示例
2. 启动时打印连接模式（Native/WebSocket）、服务器地址、数据库列表、线程数等摘要
3. 结束时打印 SUCCESS/FAILED 及错误码（十六进制 `0x%08X`），CANCELLED BY USER 有独立提示

## 测试需求

1. 覆盖全部 TDengine 数据类型（BOOL/TINYINT/SMALLINT/INT/BIGINT/FLOAT/DOUBLE/BINARY/NCHAR/TIMESTAMP/各 UNSIGNED/JSON/VARBINARY/GEOMETRY/DECIMAL）
2. 覆盖 Schema-only、时间范围过滤、数据库重命名、断点续传、Parquet 格式等功能路径
3. 覆盖精度（ms/us/ns）、虚拟表、中文/特殊字符标识符、主键表等特殊场景
4. 覆盖错误路径：连接中断退避、文件损坏、Schema 变更部分列写入、SIGINT 优雅退出
5. 性能基准测试：与 taosdump 同条件对比
