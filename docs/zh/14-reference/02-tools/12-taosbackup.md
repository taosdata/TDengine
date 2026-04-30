---
title: taosBackup 参考手册
sidebar_label: taosBackup
toc_max_heading_level: 4
---

taosBackup 是为 TDengine TSDB 提供的高性能数据备份与恢复工具。备份数据采用高效的列式存储格式，支持全量备份、按库备份、按表备份、按时间范围备份及仅备份元数据等多种场景，并提供断点续传能力，适用于各类数据保护和迁移需求。

## 与 taosdump 的关系

taosBackup 是原 taosdump 的全新升级版本，由于存储格式完全不兼容，故进行了更名。taosdump 在完成过渡期后安装包中将不再提供。

taosBackup 在性能及备份数据大小方面较 taosdump 有极大提升。

## 性能比较

| 工具           | 备份  | 恢复  |
| ------------  | ----  | ---- |
| taosdump(基准) | 1x    | 1x  |
| taosBackup    | 5x    | 3x |

## 备份数据大小

| 工具           |  存储格式 | 占比   |
| ------------- | ------ | ---- |
| taosdump(基准) | 行存   | 100% |
| taosBackup    | 列存   | 42% |

## 参数及功能

taosBackup 支持绝大全部 taosdump 的命令行参数（个别参数除外）。功能上新增了断点续传、导出 Parquet 格式、仅恢复指定数据库、优化多表低频场景的备份/恢复性能等。

## 工具获取

taosBackup 在 TDengine TSDB v3.4.1.1 及之后版本的服务器或客户端安装包中均提供，安装请参考 [TDengine TSDB 安装](../../../get-started/)

## 运行

taosBackup 支持 Windows/MacOS/Linux 平台，在命令行终端中运行，运行时必须带参数，指明备份操作（`-o`）或恢复操作（`-i`）。

:::tip
在运行 taosBackup 之前要确保目标 TDengine TSDB 集群已经正确运行。
:::

### 备份示例

```bash
taosBackup -h my-server -D test -o /root/backup/
```

以上命令表示将主机名为 `my-server` 的 TDengine 服务上的 `test` 数据库备份到 `/root/backup/` 目录下。

```bash
taosBackup -h my-server -o /root/backup/
```

不指定 `-D` 参数时，默认备份所有用户数据库（`information_schema` 和 `performance_schema` 系统库除外）。

### 恢复示例

```bash
taosBackup -h my-server -i /root/backup/
```

以上命令表示将 `/root/backup/` 目录下的备份数据恢复到主机名为 `my-server` 的 TDengine 服务中。

```bash
taosBackup -h my-server -D test -i /root/backup/
```

以上命令表示将 `/root/backup/` 目录下的备份数据恢复到主机名为 `my-server` 的 TDengine 服务中，并仅恢复 `test` 数据库的数据。

## 命令行参数

```bash
Usage: taosBackup [OPTION...] dbname [tbname ...] -o outpath
  or:  taosBackup [OPTION...] -o outpath
  or:  taosBackup [OPTION...] -i inpath
  or:  taosBackup [OPTION...] --databases db1,db2,...
```

| 命令行参数 | 功能说明 |
| --------- | ------- |
| `-h, --host=HOST` | 要连接的 TDengine 服务端 FQDN 或 IP，默认值为 localhost |
| `-P, --port=PORT` | 要连接的 TDengine 服务端端口号，默认值为 6030 |
| `-c, --config-dir=CONFIG_DIR` | 指定 taos.cfg 配置文件所在目录，不指定使用默认路径 |
| `-u, --user=USER` | 连接用户名，默认值为 root |
| `-p, --password=PASSWORD` | 连接密码，默认值为 taosdata |
| `-o, --outpath=OUTPATH` | 备份输出目录路径，默认值为 ./output |
| `-i, --inpath=INPATH` | 恢复操作时指定备份文件所在的输入路径 |
| `-D, --databases=DATABASES` | 指定要备份/恢复的数据库，多个库以逗号分隔；不指定则默认操作所有用户数据库 |
| `-F, --format=FORMAT` | 备份文件存储格式，可选值为 `binary`（默认）或 `parquet` |
| `-s, --schemaonly` | 开关参数，仅备份表结构（Schema）和标签（Tag）数据，不备份时序数据 |
| `-S, --start-time=START_TIME` | 备份数据的起始时间，支持毫秒时间戳或 ISO8601 格式，如 `2017-10-01T00:00:00.000+0800`。仅备份操作生效 |
| `-E, --end-time=END_TIME` | 备份数据的结束时间，支持毫秒时间戳或 ISO8601 格式。仅备份操作生效 |
| `-T, --thread-num=THREAD_NUM` | 数据备份/恢复的并行线程数，默认值为 8 |
| `-m, --tag-thread-num=THREAD_NUM` | 标签数据备份的并行线程数，默认值为 2 |
| `-B, --data-batch=DATA_BATCH` | 恢复时每次 STMT 批量写入的行数。STMT2（默认）有效范围 [1, 16384]，默认 10000；STMT1 有效范围 [1, 100000]，默认 60000。仅恢复操作生效 |
| `-v, --stmt-version=VER` | 恢复时使用的 STMT API 版本：`2`（默认，TAOS_STMT2，速度更快，需 TDengine v3.3+）或 `1`（兼容旧版 TAOS_STMT API）。仅恢复操作生效 |
| `-W, --rename=RENAME-LIST` | 恢复时对数据库进行重命名，格式为 `"db1=newdb1\|db2=newdb2"`。仅恢复操作生效 |
| `-C, --checkpoint` | 开关参数，开启断点续传模式。taosBackup 始终会写入检查点文件；开启此参数后，再次运行时会跳过已完成的项目 |
| `-k, --retry-count=VALUE` | 连接或查询失败后的重试次数，默认值为 3 |
| `-z, --retry-sleep-ms=VALUE` | 每次重试之间的等待时间，单位为毫秒，默认值为 1000 |
| `-X, --dsn=DSN` | 连接云服务的 DSN，格式如 `https://host?token=<TOKEN>`。也可通过环境变量 `TDENGINE_CLOUD_DSN` 设置，命令行参数优先级更高 |
| `-Z, --driver=DRIVER` | 指定连接驱动，可选值为 `Native`（原生连接）或 `WebSocket`。默认为 Native；当设置了 DSN 时，默认切换为 WebSocket |
| `-g, --debug` | 开关参数，开启调试信息输出，默认关闭 |
| `-V, --version` | 显示版本信息并退出 |
| `--help` | 显示帮助信息并退出 |

## 常用使用场景

### 备份数据

#### 备份所有数据库

```bash
taosBackup -h my-server -o /root/backup/
```

备份所有用户数据库（`information_schema` 和 `performance_schema` 自动排除）到 `/root/backup/` 目录。

#### 备份指定数据库

```bash
taosBackup -h my-server -D db1,db2 -o /root/backup/
```

仅备份 `db1` 和 `db2` 两个数据库。

#### 备份指定数据库中的指定超级表或普通表

```bash
taosBackup -h my-server -o /root/backup/ mydb meters d1 d2
```

备份 `mydb` 库中的超级表 `meters` 以及普通表 `d1`、`d2`。其中第一个位置参数为数据库名，后续参数为该库中的表名或超级表名，以空格分隔。

#### 按时间范围备份

```bash
taosBackup -h my-server -D test -S "2024-01-01T00:00:00.000+0800" -E "2024-12-31T23:59:59.999+0800" -o /root/backup/
```

仅备份 `test` 数据库中 2024 年全年的数据。

#### 仅备份元数据（Schema）

```bash
taosBackup -h my-server -D test -s -o /root/backup/
```

仅备份 `test` 数据库的表结构和标签信息，不备份时序数据，适用于快速迁移表结构。

#### 备份为 Parquet 格式

```bash
taosBackup -h my-server -D test -F parquet -o /root/backup/
```

将 `test` 数据库以 Parquet 格式导出，便于与大数据生态（如 Spark、Hive、DuckDB）对接。

#### 断点续传备份

断点续传功能默认不开启，需要通过 `-C` 参数显式指定。当备份过程中因故中断时，再次运行相同命令并加上 `-C` 参数，taosBackup 会自动跳过已成功完成的数据库/超级表/子表，继续备份未完成的部分。

```bash
# 第一次备份（因故中断）
taosBackup -h my-server -D test -o /root/backup/

# 再次运行，开启断点续传，跳过已完成的超级表/子表
taosBackup -h my-server -D test -o /root/backup/ -C
```

taosBackup 每次运行都会在输出目录中自动写入检查点文件。使用 `-C` 参数重新运行时，会读取检查点文件并跳过已成功完成的项目，从中断位置继续执行。

:::tip

- `-o` 参数指定的目录下如果已存在备份文件，taosBackup 在未开启断点续传模式时会直接覆盖同名文件，建议使用空目录进行全量备份。
- 如果备份数据量很大，建议配合 `-S`/`-E` 参数分段备份，或使用 `-C` 断点续传。

:::

### 恢复数据

#### 恢复到原库

```bash
taosBackup -h my-server -i /root/backup/
```

将 `/root/backup/` 目录下的备份数据恢复到 `my-server`。恢复时会自动创建对应数据库、超级表及子表（若已存在则跳过建表）。

#### 恢复时重命名数据库

```bash
taosBackup -h my-server -i /root/backup/ -W "db1=db1_restored|db2=db2_restored"
```

将备份中的 `db1` 恢复为 `db1_restored`，`db2` 恢复为 `db2_restored`，适用于测试验证或平行运行场景。

#### 断点续传恢复

```bash
taosBackup -h my-server -i /root/backup/ -C
```

恢复同样支持断点续传模式，再次运行时自动跳过已成功恢复的数据文件。

#### Schema 变更场景下的恢复

taosBackup 在恢复时会自动检测备份时的表结构与目标服务端现有表结构的差异。当目标端超级表的列集合与备份相比有变化（如新增或删除了列）时，taosBackup 会自动计算公共列并执行部分列写入，保证数据安全写入，无需人工干预。

#### 调整写入批量以避免 WAL 溢出

```bash
taosBackup -h my-server -i /root/backup/ -B 2000
```

恢复时如遇到 `WAL size exceeds limit` 错误，可通过 `-B` 参数减小每次批量写入的行数。

#### 连接 TDengine Cloud

```bash
taosBackup -i /root/backup/ -X "https://cloud-host?token=<TOKEN>"
```

通过 DSN 连接 TDengine Cloud 服务进行数据恢复，驱动类型自动切换为 WebSocket。

## 备份文件结构

备份输出目录下，每个数据库对应一个子目录，目录内包含以下内容：

```bash
{outpath}/
└── {dbname}/
    ├── db.sql             # 建库 SQL
    ├── tags/
    │   ├── {stbname}.sql  # 建超级表 SQL
    │   └── {stbname}_data{N}.{ext}  # 标签数据（CSV 格式）
    ├── data/
    │   └── {stbname}/
    │       └── {stbname}_data{N}.{ext}  # 时序数据文件（binary 或 parquet 格式）
    └── _ntb/
        ├── {tbname}.sql   # 建普通表 SQL
        └── {tbname}/
            └── {tbname}_data{N}.{ext}  # 普通表时序数据文件
```

其中 `.ext` 在 binary 格式下为 `.bin`，Parquet 格式下为 `.par`。

## 输出指标

### 启动汇总

备份/恢复开始时，taosBackup 会打印当前运行参数摘要，示例如下：

```bash
===========================================================================
  taosBackup - BACKUP
===========================================================================
  Connect Mode : Native
  Server       : my-server:6030
  User         : root
  Output Path  : /root/backup/
  Databases    : test
  Data Threads : 8
  Tag Threads  : 2
  Format       : binary
  Schema Only  : no
  Time Range   : ALL
  Check Point  : no
===========================================================================
```

### 实时进度

运行过程中，taosBackup 会持续输出进度信息，显示当前处理的数据库、超级表、已完成子表数及预计剩余时间：

```bash
[DB 1/2: test] [STB 3/10: meters] [CTB 1500/5000 (30.0%)] elapsed: 12s, eta: 28s
```

### 结束汇总

备份/恢复完成后，打印最终统计摘要：

```bash
===========================================================================
  Result       : SUCCESS
---------------------------------------------------------------------------
  Databases    : total=1, success=1, failed=0
  Super Tables : 10
  Child Tables : 5000 (data exported)
  Normal Tables: 2
  Total Rows   : 50000000
  Elapsed      : 45.23 s
===========================================================================
```

各字段含义：

- **Databases**：处理的数据库总数及成功/失败数量。
- **Super Tables**：处理的超级表数量。
- **Child Tables**：已导出/恢复数据的子表数量。
- **Normal Tables**：处理的普通表数量。
- **Total Rows**：备份/恢复的数据总行数。
- **Elapsed**：操作总耗时，单位为秒。

:::tip
若发现失败数量不为零，可添加 `-g` 参数开启调试输出，查看详细错误信息，或检查 TDengine 服务端日志进行排查。
:::
