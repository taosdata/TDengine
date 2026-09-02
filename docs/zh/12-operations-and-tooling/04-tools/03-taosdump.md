---
title: taosdump 参考手册
sidebar_label: taosdump
toc_max_heading_level: 4
---

taosdump 是 TDengine 提供的高性能数据备份/恢复工具。备份数据采用高压缩率的列式存储，支持全量备份、按库备份、按表备份、按时间范围备份及仅元数据备份等多种场景，并提供断点续传能力。

## taosdump 全新升级版本

从 `v3.4.2.0` 开始，taosdump 进行了全新升级，升级后的版本提供了更高性能、更小备份数据大小及增加了更多实用功能。
新版本支持老版本生成的 avro 格式数据导入，但不再支持生成 avro 格式备份数据。

## 工具获取

taosdump 在 TDengine 服务器或客户端安装包中均提供，安装请参考 [TDengine 安装](../../04-quick-start/index.md)

## 运行

taosdump 支持 Windows/MacOS/Linux 平台，在命令行终端中运行，运行时必须带参数，指明备份操作（`-o`）或恢复操作（`-i`）。

:::tip
在运行 taosdump 之前要确保目标 TDengine 集群已经正确运行。
:::

### 备份示例

```bash
taosdump -h my-server -D test -o /root/backup/
```

以上命令表示将主机名为 `my-server` 的 TDengine 服务上的 `test` 数据库备份到 `/root/backup/` 目录下。

```bash
taosdump -h my-server -o /root/backup/
```

不指定 `-D` 参数时，默认备份所有用户数据库（`information_schema` 和 `performance_schema` 系统库除外）。

### 恢复示例

```bash
taosdump -h my-server -i /root/backup/
```

以上命令表示将 `/root/backup/` 目录下的备份数据恢复到主机名为 `my-server` 的 TDengine 服务中。

```bash
taosdump -h my-server -D test -i /root/backup/
```

以上命令表示将 `/root/backup/` 目录下的备份数据恢复到主机名为 `my-server` 的 TDengine 服务中，并仅恢复 `test` 数据库的数据。

## 命令行参数

```bash
Usage: taosdump [OPTION...] dbname [tbname ...] -o outpath
  or:  taosdump [OPTION...] -o outpath
  or:  taosdump [OPTION...] -i inpath
  or:  taosdump [OPTION...] --databases db1,db2,...
```

| 命令行参数 | 功能说明 |
| --------- | ------- |
| `-h, --host=HOST` | 要连接的 TDengine 服务端 FQDN 或 IP，默认值为 localhost |
| `-P, --port=PORT` | 要连接的 TDengine 服务端端口号，默认值为 6030 |
| `-c, --config-dir=CONFIG_DIR` | 指定 taos.cfg 配置文件所在目录，不指定使用默认路径 |
| `-u, --user=USER` | 连接用户名，默认值为 root |
| `-p, --password` | 交互式输入连接密码。也可使用 `-pPASSWORD` 或 `--password=PASSWORD` 在命令行中指定密码。默认值为 taosdata |
| `-o, --outpath=OUTPATH` | 备份输出目录路径，默认值为 ./output |
| `-i, --inpath=INPATH` | 恢复操作时指定备份文件所在的输入路径 |
| `-D, --databases=DATABASES` | 指定要备份/恢复的数据库，多个库以逗号分隔；不指定则默认操作所有用户数据库 |
| `-F, --format=FORMAT` | 备份文件存储格式，可选值为 `binary`（默认）或 `parquet` |
| `-M, --content=CONTENT` | 指定备份/恢复内容，备份/恢复操作均生效。可选值：<br />`basic`（默认）——基础数据，包括超级表、子表、普通表及标签和时序数据；<br />`ext-meta`——扩展元数据，包括虚拟表、流和订阅（Topic）；<br />`all`——`basic` + `ext-meta`。详见 [备份/恢复内容选择](#备份恢复内容选择) |
| `-s, --schemaonly` | 开关参数，仅备份表结构（Schema）和标签（Tag）数据，不备份时序数据 |
| `-S, --start-time=START_TIME` | 备份数据的起始时间，支持毫秒时间戳或 ISO8601 格式，如 `2017-10-01T00:00:00.000+0800`。仅备份操作生效 |
| `-E, --end-time=END_TIME` | 备份数据的结束时间，支持毫秒时间戳或 ISO8601 格式。仅备份操作生效 |
| `-T, --thread-num=THREAD_NUM` | 数据备份/恢复的并行线程数，默认值为 8 |
| `-m, --tag-thread-num=THREAD_NUM` | 标签数据备份的并行线程数，默认值为 2 |
| `-B, --data-batch=DATA_BATCH` | 恢复时每次 STMT 批量写入的行数。STMT2（默认）有效范围 [1, 16384]，默认 10000；STMT1 有效范围 [1, 100000]，默认 60000。仅恢复操作生效 |
| `-v, --stmt-version=VER` | 恢复时使用的 STMT API 版本：`2`（默认，TAOS_STMT2，速度更快，需 TDengine v3.3+）或 `1`（兼容旧版 TAOS_STMT API）。仅恢复操作生效 |
| `-W, --rename=RENAME-LIST` | 恢复时对数据库进行重命名，格式为 `"db1->newdb1\|db2->newdb2"`，表示将 `db1` 重命名为 `newdb1`，`db2` 重命名为 `newdb2`，仅恢复操作生效 |
| `-C, --checkpoint` | 断点续传开关参数，默认关闭，加此参数时才开启，跳过已备份的表或已恢复过的文件，备份/恢复操作均生效，适用于大数据量备份/恢复场景|
| `-k, --retry-count=VALUE` | 连接或查询失败后的重试次数，默认值为 3 |
| `-z, --retry-sleep-ms=VALUE` | 每次重试之间的等待时间，单位为毫秒，默认值为 1000 |
| `-X, --dsn=DSN` | 连接云服务的 DSN，格式如 `https://host?token=<TOKEN>`。也可通过环境变量 `TDENGINE_CLOUD_DSN` 设置，命令行参数优先级更高 |
| `-Z, --driver=DRIVER` | 指定连接驱动，可选值为 `Native`（端口 6030，速度更快，也可写 `0`，默认）或 `WebSocket`（端口 6041，无需安装客户端驱动，兼容性更好，也可写 `1`）。当设置了 DSN 时，默认切换为 WebSocket |
| `-g, --debug` | 开关参数，开启调试信息输出，默认关闭 |
| `-V, --version` | 显示版本信息并退出 |
| `--help` | 显示帮助信息并退出 |

## 备份恢复内容选择

从 `v3.4.2.5` 开始，taosdump 通过 `-M, --content` 参数支持指定备份/恢复内容。

| 取值 | 包含内容 | 对应备份文件 |
| --- | --- | --- |
| `basic`（默认） | 超级表、子表、普通表及标签和时序数据 | `db.sql`、`stb.sql`、`{stbname}.csv`、`tags/`、`ntb.sql`、`{stbname}_data{N}/`、`_ntb_data{N}/` |
| `ext-meta` | 虚拟表、流、订阅（Topic） | `vtb.sql`、`vtags/`、`stream.sql`、`topic.sql` |
| `all` | `basic` + `ext-meta` | 以上全部 |

### 为什么要分成两部分

虚拟表的每一列都可以来自任意数据库中的任意表（例如 `db2` 的虚拟表引用 `db1` 的实体表）。
恢复时如果按库逐个完整恢复，当含虚拟表的库先于被引用的库恢复时，`CREATE VTABLE` 会因被引用的库
尚不存在而失败。因此 taosdump 的恢复分为两个阶段执行：

1. **第一阶段**：完成**所有**数据库的实体表创建及时序数据导入（`basic`）
2. **第二阶段**：在所有数据库就绪后，统一执行所有库的虚拟表、流、订阅 DDL（`ext-meta`）

这样无论数据库以何种顺序列出，跨库引用都能正确解析。

### 典型用法

```bash
# 备份全部内容（基础数据 + 扩展元数据）
taosdump --content=all -D db1,db2 -o /root/backup/

# 只备份基础数据（默认行为，等同于不写 --content）
taosdump -D db1,db2 -o /root/backup/

# 恢复：先导入基础数据，确认无误后再导入扩展元数据
taosdump --content=basic   -i /root/backup/
taosdump --content=ext-meta -i /root/backup/
```

分阶段恢复的价值在于：扩展元数据（流/虚拟表/订阅）中个别对象导入失败时，不会影响已经导入完成的
基础数据；修正问题后只需单独重跑 `--content=ext-meta` 即可，无需重新导入全部时序数据。

:::note

- `--content=ext-meta` 恢复时若目标数据库不存在，会先执行 `db.sql` 创建数据库；因此 `ext-meta`
  模式的备份中也会包含 `db.sql`。
- 虚拟超级表的 DDL 保存在 `stb.sql` 中，属于 `basic` 内容。它本身不含跨库引用（列到源表的映射
  发生在虚拟子表/虚拟普通表上），因此 `basic` 备份中会包含一个不带虚拟子表的虚拟超级表结构。
- 老版本 avro 格式的备份目录不含 `vtb.sql`/`stream.sql`/`topic.sql`，其虚拟表在第一阶段内部
  单独处理，`ext-meta` 阶段会跳过并给出提示。
- 使用 `-W/--rename` 重命名数据库时，虚拟表、流、订阅中的库名引用按以下规则处理（均已实测验证）：
  `-W` 支持一次配置多对库名映射，恢复时会将 DDL 中出现的**每一对**映射关系全部应用，而不仅是
  当前正在恢复的这一个库自己的那一对——因此虚拟表跨库引用的源库，只要也在 `-W` 映射表中，同样
  会被改写为新库名。
  - **虚拟表引用本库或其他库的列**：只要引用的库名出现在 `-W` 映射表中（不论是不是当前正在
    恢复的库），都会被替换为对应的新库名。若被引用库不在映射表中（未被重命名），则保持原样，
    继续指向原库。
  - **流**：DDL 中所有库名引用（源库、目标库）及流名的库限定前缀，按同样的规则替换。
  - **订阅（Topic）**：Topic 是集群级对象，**名称本身不随 `-W/--rename` 改变**，但其查询语句
    中的库名引用会被替换为新库名。恢复到同一集群时，若该 Topic 名已存在会命中"已存在"并按
    成功处理。
- 订阅与流的 DDL 取自 `information_schema.ins_topics` / `ins_streams` 的 `sql` 列，该列长度上限
  为 2048 字节，超长 DDL 会被服务端截断。

:::

## 备份文件结构

备份输出目录下，每个数据库对应一个子目录，目录内包含以下内容：

```bash
{outpath}/
└── {dbname}/
    ├── db.sql                        # 建库 SQL
    ├── stb.sql                       # 所有超级表 DDL（每行一个，含虚拟超级表）
    ├── ntb.sql                       # 所有普通表 DDL
    ├── {stbname}.csv                 # 超级表列/标签 Schema（DESCRIBE 输出）
    ├── tags/
    │   └── {stbname}_data{N}.{ext}   # 子表标签数据
    ├── {stbname}_data{dirIndex}/
    │   └── {ctbname}.{ext}           # 子表时序数据文件
    ├── _ntb_data{dirIndex}/
    │   └── {ntbname}.{ext}           # 普通表时序数据文件
    ├── vtb.sql                       # 虚拟表 DDL（ext-meta）
    ├── vtags/
    │   └── {vstbname}_data{N}.{ext}  # 虚拟子表标签数据（ext-meta）
    ├── stream.sql                    # 流 DDL（ext-meta）
    ├── topic.sql                     # 订阅 DDL（ext-meta）
    └── backup_complete.flag          # 备份完成标记
```

其中 `.ext` 在 binary 格式下为 `.dat`，Parquet 格式下为 `.par`。
`vtb.sql`、`vtags/`、`stream.sql`、`topic.sql` 仅在 `--content` 为 `ext-meta` 或 `all` 时生成。

## 输出指标

### 启动汇总

备份/恢复开始时，taosdump 会打印当前运行参数摘要，示例如下：

```bash
===========================================================================
  taosdump - BACKUP
===========================================================================
  Connect Mode : Native
  Server       : my-server:6030
  User         : root
  Output Path  : /root/backup/
  Content      : basic
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

运行过程中，taosdump 会持续输出进度信息，显示当前处理的数据库、超级表、已完成子表数及预计剩余时间：

```bash
[DB 1/2: test] [STB 3/10: meters] [CTB 1500/5000 (30.0%)] elapsed: 12s, eta: 28s
```

### 结束汇总

备份/恢复完成后，打印最终统计摘要：

```bash
===========================================================================
  Result       : SUCCESS (BACKUP)
---------------------------------------------------------------------------
  Databases    : total=1, success=1, failed=0
  Super Tables : 10
  Child Tables : 5000 (data exported)
  Normal Tables: 2
  Total Rows   : 50000000
  Ext Meta     : vtable=8, stream=2, topic=1
  Elapsed      : 45.23 s
===========================================================================
```

各字段含义：

- **Result**：`SUCCESS`/`FAILED`/`CANCELLED BY USER`，括号内标注本次操作是 `BACKUP` 还是 `RESTORE`。
- **Databases**：处理的数据库总数及成功/失败数量。
- **Super Tables**：处理的超级表数量。
- **Child Tables**：已导出/恢复数据的子表数量。
- **Normal Tables**：处理的普通表数量。
- **Total Rows**：备份/恢复的数据总行数。
- **Ext Meta**：处理的虚拟表、流、订阅数量，仅当 `--content` 为 `ext-meta` 或 `all` 时输出。
- **Elapsed**：操作总耗时，单位为秒。

:::tip
若发现失败数量不为零，可添加 `-g` 参数开启调试输出，查看详细错误信息，或检查 TDengine 服务端日志进行排查。
:::

## 日志文件

- 备份：日志文件名为 `backup.log`，保存在备份目录根目录下。
- 恢复：日志文件名为 `restore.log`，保存在当前目录下。

## 常用使用场景

### 备份数据

#### 备份所有数据库

```bash
taosdump -h my-server -o /root/backup/
```

备份所有用户数据库（`information_schema` 和 `performance_schema` 自动排除）到 `/root/backup/` 目录。

#### 备份指定数据库

```bash
taosdump -h my-server -D db1,db2 -o /root/backup/
```

仅备份 `db1` 和 `db2` 两个数据库。

#### 备份指定表

```bash
taosdump -h my-server -o /root/backup/ test meters t1 t2
```

备份 `test` 库中的超级表 `meters` 以及普通表 `t1`、`t2`。其中第一个位置参数为数据库名，后续参数为该库中的一个或多个超级表/子表/普通表名，多个以空格分隔。
注：只能指定一个数据库名，不支持多个库。

#### 按时间范围备份

```bash
taosdump -h my-server -D test -S "2024-01-01T00:00:00.000+0800" -E "2024-12-31T23:59:59.999+0800" -o /root/backup/
```

仅备份 `test` 数据库中 2024 年全年的数据。

#### 仅备份元数据（Schema）

```bash
taosdump -h my-server -D test -s -o /root/backup/
```

仅备份 `test` 数据库的表结构和标签信息，不备份时序数据，适用于快速迁移表结构。

#### 备份虚拟表、流和订阅

```bash
taosdump -h my-server -D test --content=all -o /root/backup/
```

默认的 `basic` 模式只备份基础数据。需要同时备份虚拟表、流和订阅时，指定 `--content=all`；
只备份这三类对象时指定 `--content=ext-meta`。详见 [备份恢复内容选择](#备份恢复内容选择)。

#### 备份为 Parquet 格式

```bash
taosdump -h my-server -D test -F parquet -o /root/backup/
```

将 `test` 数据库以 Parquet 格式导出，便于与大数据生态（如 Spark、Hive、DuckDB）对接。

#### 断点续传备份

断点续传功能默认不开启，需要通过 `-C` 参数显式指定。当备份过程中因故中断时，再次运行相同命令并加上 `-C` 参数，taosdump 会自动跳过已成功完成的数据库/超级表/子表，继续备份未完成的部分。

说明：断点续传只针对数据备份有效，元数据备份因速度快，不提供断点续传功能。

```bash
# 第一次备份（因故中断）
taosdump -h my-server -D test -o /root/backup/

# 再次运行，开启断点续传，跳过已完成的超级表/子表
taosdump -h my-server -D test -o /root/backup/ -C
```

taosdump 每次运行都会在输出目录中自动写入检查点文件。使用 `-C` 参数重新运行时，会读取检查点文件并跳过已成功完成的项目，从中断位置继续执行。

:::tip

- `-o` 参数指定的目录下如果已存在备份文件，taosdump 在未开启断点续传模式时会直接覆盖同名文件，建议使用空目录进行全量备份。
- 如果备份数据量很大，建议配合 `-S`/`-E` 参数分段备份，或使用 `-C` 断点续传。

:::

### 恢复数据

#### 恢复到原库

```bash
taosdump -h my-server -i /root/backup/
```

将 `/root/backup/` 目录下的备份数据恢复到 `my-server`。恢复时会自动创建对应数据库、超级表及子表（若已存在则跳过建表）。

#### 恢复时重命名数据库

```bash
taosdump -h my-server -i /root/backup/ -W "db1->db1_restored|db2->db2_restored"
```

将备份中的 `db1` 恢复为 `db1_restored`，`db2` 恢复为 `db2_restored`，适用于测试验证或平行运行场景。

#### 恢复指定表

```bash
taosdump -h my-server -i /root/backup/ test t1
```

仅恢复 `test` 数据库中的普通表 `t1`。其中第一个位置参数为数据库名，后续参数为该库中的一个或多个超级表/子表/普通表名，多个以空格分隔。

#### 断点续传恢复

```bash
taosdump -h my-server -i /root/backup/ -C
```

恢复同样支持断点续传模式，再次运行时自动跳过已成功恢复的数据文件。

#### 分阶段恢复虚拟表、流和订阅

```bash
# 第一步：恢复实体表及时序数据
taosdump -h my-server --content=basic -i /root/backup/

# 第二步：恢复虚拟表、流和订阅
taosdump -h my-server --content=ext-meta -i /root/backup/
```

等价于一次 `--content=all` 恢复，但把扩展元数据单独拆出来执行。适用于扩展元数据中个别对象
导入失败的场景：基础数据已完整导入不受影响，修正问题后只需重跑第二步。

#### 跨库虚拟表的恢复

虚拟表的列可以来自任意数据库。恢复时 taosdump 会先完成**所有**数据库的实体表和数据，再统一
执行虚拟表 DDL，因此无需关心数据库在 `-D` 中的先后顺序：

```bash
# db2 的虚拟表引用 db1 的实体表；即使 db2 在前也能正确恢复
taosdump -h my-server --content=all -D db2,db1 -i /root/backup/
```

#### Schema 变更场景下的恢复

taosdump 在恢复时会自动检测备份时的表结构与目标服务端现有表结构的差异。当目标端超级表的列集合与备份相比有变化（如新增或删除了列）时，taosdump 会自动计算公共列并执行部分列写入，保证数据安全写入，无需人工干预。

#### 调整写入批量以避免 WAL 溢出

```bash
taosdump -h my-server -i /root/backup/ -B 2000
```

恢复时如遇到 `WAL size exceeds limit` 错误，可通过 `-B` 参数减小每次批量写入的行数。

#### 连接 TDengine Cloud

```bash
taosdump -i /root/backup/ -X "https://cloud-host?token=<TOKEN>"
```

通过 DSN 连接 TDengine Cloud 服务进行数据恢复，驱动类型自动切换为 WebSocket。

### 输入连接密码

如果不指定密码参数，taosdump 使用默认密码 `taosdata`。需要输入密码时，推荐使用交互式输入，避免密码出现在命令历史或进程列表中：

```bash
taosdump -u root -p -D test -o /root/backup/
```

也可以使用长选项交互式输入密码：

```bash
taosdump -u root --password -D test -o /root/backup/
```

如需在命令行中直接指定密码，短选项必须与密码紧贴，中间不能有空格：

```bash
taosdump -u root -ptaosdata -D test -o /root/backup/
```

长选项直接指定密码时，请使用等号形式：

```bash
taosdump -u root --password=taosdata -D test -o /root/backup/
```

以下写法不支持，taosdump 会将其判定为密码参数使用错误：

```bash
taosdump -u root -p taosdata -D test -o /root/backup/
taosdump -u root --password taosdata -D test -o /root/backup/
```

如果要在交互式输入密码的同时通过位置参数指定数据库名，请使用 `--` 明确结束选项解析，或改用 `-D` 指定数据库。推荐使用 `-D`：

```bash
taosdump -u root -p -D test -o /root/backup/
```

## 新版本行为变更

### 性能提升

| 工具            | 备份  | 恢复  |
| -------------  | ----  | ---- |
| 老版本（基准）   | 1x    | 1x  |
| 新版本          | 5x    | 3x |

### 备份数据压缩率提升

| 工具           |  存储格式 | 占比   |
| ------------- | ------ | ---- |
| 老版本（基准）  | 行存   | 100% |
| 新版本        | 列存   | 42% |

### 新增功能

- 断点续传
- 导出 [Parquet](https://parquet.apache.org/) 格式
- STMT2 导入
- 多线程元数据备份
- 仅恢复指定数据库
- 全新展示界面
- 优化多表低频场景的备份/恢复性能
- 备份/恢复内容选择（`-M/--content`），支持仅备份/恢复基础数据或扩展元数据
- 支持订阅（Topic）的备份与恢复
- 恢复分两阶段执行，修复跨库虚拟表因数据库恢复顺序导致的失败

新版本支持绝大部分老版本命令行参数（个别参数除外）。
