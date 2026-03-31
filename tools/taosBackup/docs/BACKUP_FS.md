# 概要设计说明书（Functional Spec）- taosBackup

# 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-31 | 2026-03-31 | 1.0 | Alex Duan | 初版创建，基于源代码反推 |

# 背景

TDengine 现有的 taosdump 工具采用 Avro 行存储格式导出数据，在大数据量场景下有以下问题：

1. **速度慢**：Avro 行存储序列化/反序列化开销大
2. **文件大**：行存储格式压缩效率有限
3. **恢复低效**：STMT2 逐表绑定（bind_one），无多表批量模式

taosBackup 工具旨在提供高性能、紧凑存储的数据库备份恢复方案，作为 taosdump 的替代和升级。

# 定义

| 术语 | 定义 |
| --- | --- |
| STB | Super Table，超级表 |
| CTB | Child Table，子表 |
| NTB | Normal Table，普通表 |
| VTAB | Virtual Table，虚拟表（VIRTUAL_NORMAL_TABLE / VIRTUAL_CHILD_TABLE） |
| VSTB | Virtual Super Table，虚拟超级表（CREATE STABLE ... VIRTUAL 1） |
| .dat | 二进制数据文件，Magic `TAOS`，版本 2，zstd 逐列压缩 |
| .par | Apache Parquet 数据文件 |
| STMT2 | TDengine v3.3+ 参数化写入 API，支持多表绑定 |
| STMT1 | TDengine 旧版参数化写入 API |

# 行为说明

## 命令行接口

### 用法

```
taosBackup [OPTION...] dbname [tbname ...] -o outpath    # 备份指定库/表
taosBackup [OPTION...] -o outpath                        # 备份所有库
taosBackup [OPTION...] -i inpath                         # 恢复
taosBackup [OPTION...] --databases db1,db2,... -o outpath
```

### 参数列表

以下参数均已在 `bckArgs.c` 中实现：

| 短选项 | 长选项 | 类型 | 默认值 | 有效范围 | 说明 |
| :---: | --- | --- | :---: | --- | --- |
| `-h` | `--host` | string | `localhost` | — | TDengine 服务器地址 |
| `-P` | `--port` | int | `6030` | 1-65535 | 服务器端口 |
| `-u` | `--user` | string | `root` | — | 用户名 |
| `-p` | `--password` | string | `taosdata` | — | 密码 |
| `-o` | `--outpath` | string | `./output` | — | 备份输出目录 |
| `-i` | `--inpath` | string | — | — | 恢复输入目录（指定时触发恢复模式） |
| `-D` | `--databases` | string | — | 逗号分隔，上限 64 个 | 指定数据库列表 |
| `-F` | `--format` | string | `binary` | `binary` / `parquet` | 数据文件格式 |
| `-v` | `--stmt-version` | int | `2` | 1 / 2 | 恢复时 STMT API 版本 |
| `-B` | `--data-batch` | int | 见下文 | STMT2: 1-16384; STMT1: 1-100000 | 每次 STMT 绑定的行数 |
| `-s` | `--schemaonly` | flag | off | — | 仅备份 Schema，不备份数据 |
| `-S` | `--start-time` | string | — | epoch ms 或 ISO8601 | 备份起始时间 |
| `-E` | `--end-time` | string | — | epoch ms 或 ISO8601 | 备份截止时间 |
| `-T` | `--thread-num` | int | `8` | ≥ 1 | 数据备份/恢复线程数 |
| `-m` | `--tag-thread-num` | int | `2` | ≥ 1 | 标签备份线程数 |
| `-k` | `--retry-count` | int | `3` | ≥ 0 | 重试次数 |
| `-z` | `--retry-sleep-ms` | int | `1000` | ≥ 0 | 重试间隔（ms） |
| `-W` | `--rename` | string | — | 格式 `db1=new1\|db2=new2`，上限 64 对 | 恢复时重命名数据库 |
| `-X` | `--dsn` | string | — | URL 格式 | 云服务 DSN 连接串 |
| `-Z` | `--driver` | string | auto | `Native` / `WebSocket` | 连接驱动。设定 DSN 时默认 WebSocket |
| `-C` | `--checkpoint` | flag | off | — | 启用断点续传（跳过已完成项） |
| `-g` | `--debug` | flag | off | — | 启用 DEBUG 级日志 |
| `-V` | `--version` | flag | — | — | 打印版本信息 |
| — | `--help` | flag | — | — | 打印帮助信息 |

**data-batch 默认值**（源自 `bckArgs.c`）：
- STMT2（`-v 2`，默认）：默认 10,000 行，上限 16,384 行（`STMT2_BATCH_DEFAULT` / `STMT2_BATCH_MAX`）
- STMT1（`-v 1`）：默认 60,000 行，上限 100,000 行（`STMT1_BATCH_DEFAULT` / `STMT1_BATCH_MAX`）

**位置参数**：
- 第一个位置参数为 database name（存入 `g_specDb`）
- 后续位置参数为 table name（存入 `g_specTables`，上限 1000 个）

### 示例语句

```bash
# 备份所有数据库到 /data/backup/
taosBackup -o /data/backup/

# 备份指定数据库
taosBackup -D db1,db2 -o /data/backup/

# 备份指定子表
taosBackup db1 t1 t2 t3 -o /data/backup/

# 仅备份 Schema
taosBackup -D db1 -s -o /data/backup/

# 备份指定时间范围
taosBackup -D db1 -S 1625068800000 -E 1625155200000 -o /data/backup/
taosBackup -D db1 -S 2021-07-01T00:00:00.000+0800 -o /data/backup/

# Parquet 格式
taosBackup -D db1 -F parquet -o /data/backup/

# 恢复
taosBackup -i /data/backup/

# 恢复并重命名
taosBackup -W "db1=newdb1|db2=newdb2" -i /data/backup/

# 使用 STMT1 恢复
taosBackup -v 1 -i /data/backup/

# WebSocket 连接
taosBackup -Z WebSocket -h 192.168.1.100 -D db1 -o /data/backup/

# DSN/Cloud 连接
taosBackup -X "https://cloud.tdengine.com?token=xxx" -D db1 -o /data/backup/

# 断点续传备份
taosBackup -C -D db1 -o /data/backup/

# 调试模式
taosBackup -g -D db1 -o /data/backup/
```

## 备份行为

### 备份流程

1. **解析参数**：`argsInit()` 解析命令行，确定 `ACTION_BACKUP`
2. **获取数据库列表**：
   - 指定 `-D`：使用给定列表
   - 位置参数 `dbname`：使用该单库
   - 未指定：`SHOW DATABASES` 排除 `information_schema`、`performance_schema`
3. **启动进度线程**：`progressStart()` 创建后台线程
4. **逐库备份** `backDatabase(dbName)`：
   - 创建目录 `{outdir}/{db}/` 和 `{outdir}/{db}/tags/`
   - **元数据阶段**（`PROGRESS_PHASE_META`）：
     - `db.sql`：执行 `SHOW CREATE DATABASE`，写入创建语句
     - `stb.sql`：对每个 STB 执行 `SHOW CREATE TABLE`，每 STB 一行（内嵌换行替换为空格）
     - `{stbName}.csv`：执行 `DESCRIBE`，以 CSV 格式（`field,type,length,note`）写入列/标签 Schema
     - `tags/`：多线程（`-m` 控制）导出 CTB 标签数据到 `.dat` 或 `.par`
     - `ntb.sql`：普通表 DDL
     - `vtb.sql`：虚拟表 DDL
     - `vtags/`：虚拟表标签数据
   - **数据阶段**（`PROGRESS_PHASE_DATA`，`-s` 模式跳过）：
     - 对每个 STB 下的 CTB，构建 `SELECT * FROM db.ctb [WHERE ts >= ... AND ts <= ...]`
     - 多线程（`-T` 控制）通过 `queryWriteBinaryEx()` 或 `resultToFileParquet()` 写入数据文件
     - 数据先写入 `.tmp` 文件，成功后原子 rename
     - 查询结果为 0 行的表不生成文件
     - 每个目录最多 100,000 文件（`FOLDER_MAXFILE`），超出则创建 `_data1/`、`_data2/` ...
     - NTB 数据写入 `_ntb_data{N}/` 目录
5. **完成标记**：成功后写入 `backup_complete.flag`
6. **统计输出**：打印 db/stb/ctb 数量、总行数、文件大小、耗时

### 备份文件目录结构

```
{outdir}/
├── {db}/
│   ├── db.sql                       # CREATE DATABASE 语句
│   ├── stb.sql                      # 所有 STB DDL（每行一个）
│   ├── ntb.sql                      # 所有 NTB DDL
│   ├── vtb.sql                      # 所有虚拟表 DDL
│   ├── {stbName}.csv                # STB 列 Schema（DESCRIBE 输出）
│   ├── tags/
│   │   └── {stbName}_data{N}.dat    # 标签数据（binary 或 .par）
│   ├── {stbName}_data{dirIndex}/    # CTB 数据目录
│   │   ├── {ctbName}.dat            # 子表数据（binary）
│   │   └── {ctbName}.par            # 子表数据（parquet）
│   ├── _ntb_data{dirIndex}/         # NTB 数据目录
│   │   └── {ntbName}.dat
│   ├── vtags/                       # 虚拟表标签目录
│   │   └── {vstbName}_data{N}.dat
│   └── backup_complete.flag         # 备份完成标记
└── {db2}/
    └── ...
```

### 二进制数据文件格式（.dat）

源自 `storageTaos.c`：

- **Magic**：`TAOS`（4 字节）
- **版本**：2（uint16_t）
- **文件头** `TaosFileHeader`：
  - `version`（uint32_t）、`actualLen`（int32_t）、`rows`（int32_t）
  - `numOfCols`（int32_t）、`flagSegment`（int32_t）、`groupId`（uint64_t）
  - `nBlocks`（uint32_t，关闭时更新）、`numRows`（uint32_t，累计）
  - `schemaLen`（uint32_t）+ `schema[]` FieldInfo 数组
  - 32 字节保留
- **数据块**：逐块写入，每块包含 block header + 列长度数组 + 逐列 zstd 压缩数据
- **All-NULL 优化**：整列全 NULL 时以 `COL_LEN_ALL_NULL = -1` 标记，不写数据
- **写缓冲区**：每线程 4MB（`TAOS_FILE_WRITE_BUF_SIZE`）

## 恢复行为

### 恢复流程

1. **解析参数**：检测到 `-i` 触发 `ACTION_RESTORE`
2. **获取数据库列表**：
   - 指定 `-D`：使用给定列表
   - 未指定：扫描备份目录中含 `db.sql` 的子目录
3. **启动进度线程**
4. **逐库恢复** `restoreDatabase(dbName)`：
   - **加载 Checkpoint**：`-C` 模式下读取 `restore_checkpoint.txt`，构建 256 桶哈希表
   - **元数据阶段**：
     - 执行 `db.sql` 创建数据库（应用 `-W` 重命名替换）
     - 执行 `stb.sql` 创建超级表
     - 解析 `{stb}.csv` 获取备份 Schema
     - 对比服务端当前 Schema（`DESCRIBE`），若不一致则构建 `ColMapping[]`
     - 从 `tags/` 恢复标签数据并创建 CTB
     - 执行 `ntb.sql` 创建普通表
     - 执行 `vtb.sql` 创建虚拟表，恢复 `vtags/`
   - **数据阶段**：
     - 多线程从 `.dat`/`.par` 文件恢复数据
     - STMT2 模式（默认）：支持多表批量绑定，64 个 pending slot（`STMT2_MULTI_TABLE_PENDING`）
     - STMT1 模式（`-v 1`）：逐表 prepare + bind + execute
     - Parquet 恢复：`parquetReaderReadAll()` 回调，batch 阈值 50,000 行（`PAR_STMT_BATCH_THRESHOLD`）
     - Checkpoint 记录：每文件恢复完成后调用 `markRestoreDone()` 追加到 `restore_checkpoint.txt`
5. **清理**：成功删除 `restore_checkpoint.txt`；失败保留供 `-C` 续传

### Schema 变更处理

源自 `bckSchemaChange.c`：

1. 解析备份 CSV 的列定义（`parseBackupCsv()`）
2. 查询服务端 `DESCRIBE` 获取当前列定义
3. 逐列比对 name、type、bytes
4. 构建 `ColMapping[]` 和 `bindIdxMap[]`
5. 生成部分列 INSERT 语句：`INSERT INTO ? (col1, col2, col3) VALUES(?,?,?)`
6. 以 `StbChangeMap`（256 桶哈希）缓存，O(1) 位找；只在所有 insert 前填充，数据恢复线程只读

## 连接行为

### 连接池

源自 `bckPool.c`：

- 池大小 = `argDataThread() * 2 + argTagThread() + 4`（默认 22）
- 三态管理：`CONN_EMPTY` / `CONN_IDLE` / `CONN_BUSY` / `CONN_CONNECTING`
- `getConnection()`：优先复用 IDLE 连接；若池满，pthread 条件变量等待释放
- `releaseConnection()`：标记为 IDLE，通知等待线程
- `releaseConnectionBad()`：关闭异常连接并从池中移除

### 重试策略

- 指数退避：初始 1,000ms（`BCK_RECONNECT_INIT_MS`），最大 30,000ms（`BCK_RECONNECT_MAX_MS`），每次翻倍
- 200ms 粒度 sleep 检查 `g_interrupted` 标志
- 可重试错误码：`RPC_NETWORK_ERROR`、`RPC_NETWORK_BUSY`、`RPC_TIMEOUT`、`RPC_BROKEN_LINK`、`SYN_NOT_LEADER`、`SYN_RESTORING`、`SYN_TIMEOUT`、`VND_QUERY_BUSY`

## 进度显示

源自 `bckProgress.c`：

- TTY 模式（`isatty(STDOUT_FILENO)`=true）：每秒以 `\r\033[K` 覆盖当前行
- 非 TTY 模式：每 30 秒输出一行
- 显示内容：`[HH:MM:SS] [db/total] db: name [stb/total] stb: name ctb|file: done/total (%) rows: N speed: N/s ETA: ~Xs`
- 备份阶段显示 `ctb` 标签，恢复阶段显示 `file` 标签（`g_progress.isRestore`）
- 启动：`progressStart()` 创建 pthread；停止：`progressStop()` join 并清理末行

## 信号处理

源自 `main.c`：

- 注册 SIGINT / SIGTERM 处理函数 `signalHandler()`
- 设置 `g_interrupted = 1`，使用 `write()` 输出提示（async-signal-safe）
- 各线程在循环中检查 `g_interrupted`，中止当前操作，不损坏已写文件

## 日志系统

源自 `bckLog.c`：

- 四个级别：`logError()`（stderr）、`logWarn()`（stderr）、`logInfo()`（stdout）、`logDebug()`（stdout，需 `-g`）
- 线程安全：`flockfile/funlockfile` 保护
- 格式：`[HH:MM:SS] [LEVEL:] message`
- 每条消息最大 4KB 缓冲

## 错误码

源自 `bckError.h`：

| 错误码 | 名称 | 说明 |
| :---: | --- | --- |
| 0xA000 | BCK_INVALID_PARAM | 无效参数 |
| 0xA001 | BCK_MALLOC_FAILED | 内存分配失败 |
| 0xA002 | BCK_CREATE_THREAD_FAILED | 线程创建失败 |
| 0xA003 | BCK_CREATE_FILE_FAILED | 文件创建失败 |
| 0xA004 | BCK_NO_FIELDS | 查询结果无字段 |
| 0xA005 | BCK_FETCH_FIELDS_FAILED | 获取字段元数据失败 |
| 0xA006 | BCK_COMPRESS_FAILED | 压缩失败 |
| 0xA007 | BCK_WRITE_FILE_FAILED | 文件写入失败 |
| 0xA008 | BCK_CONN_POOL_EXHAUSTED | 连接池耗尽 |
| 0xA009 | BCK_READ_FILE_FAILED | 文件读取失败 |
| 0xA00A | BCK_INVALID_FILE | 无效备份文件格式 |
| 0xA00B | BCK_STMT_FAILED | STMT API 执行失败 |
| 0xA00C | BCK_DECOMPRESS_FAILED | 解压缩失败 |
| 0xA00D | BCK_OPEN_DIR_FAILED | 目录打开失败 |
| 0xA00E | BCK_EXEC_SQL_FAILED | SQL 执行失败 |
| 0xA00F | BCK_USER_CANCEL | 用户取消（Ctrl+C） |

## 输出摘要格式

**启动摘要**（源自 `printStartSummary()`）：

```
===========================================================================
  taosBackup - BACKUP
===========================================================================
  Connect Mode : Native
  Server       : localhost:6030
  User         : root
  Output Path  : ./output
  Databases    : ALL (system databases excluded)
  Data Threads : 8
  Tag Threads  : 2
  Format       : binary
  Schema Only  : no
  Time Range   : ALL
  Check Point  : no
===========================================================================
```

**结束摘要**（源自 `printEndSummary()`）：

```
===========================================================================
  Result       : SUCCESS
---------------------------------------------------------------------------
  Databases    : total=1, success=1, failed=0
  Super Tables : 1
  Child Tables : 10000 (data exported)
  Normal Tables: 0
  Total Rows   : 100000000
  Total Size   : 509.3 MB
  Elapsed      : 28.5 s
===========================================================================
```

# 性能

基准数据（源自 `docs/benchmark.md`，12 核 CPU，62 GiB RAM，1 亿行）：

| 对比项 | taosBackup binary T8 | taosdump SQL T8 | 倍数 |
| --- | :---: | :---: | :---: |
| 备份耗时 | 28s | 1m28s | 3.1x |
| 恢复耗时 | 29s | — | — |
| 文件大小 | 509 MB | 1.2 GB | 42% |

# 安全

1. 凭证仅通过命令行参数传入，不写入备份文件
2. 备份文件为本地文件，无内置加密，依赖 OS 文件权限

# 兼容性

无破坏性变更。taosBackup 是新增工具，不影响 taosdump 或其他组件。

# 运维

1. 依赖 TDengine 客户端库（libtaos）；Parquet 后端静态链接 Arrow/Parquet 库
2. 通过 CMake 编译（`CMakeLists.txt`），自动检测平台（Linux/macOS），Parquet 在非 Windows 下编译
3. 编译产物位于 `${CMAKE_BINARY_DIR}/build/bin/taosBackup`

# 使用场景

1. **定期全量备份**：`taosBackup -o /backup/$(date +%Y%m%d)` 定时任务
2. **数据迁移**：从环境 A 备份、在环境 B 恢复，支持 `-W` 重命名
3. **Schema-only 迁移**：`-s` 仅迁移表结构
4. **增量时间窗口备份**：`-S`/`-E` 指定时间范围仅备份增量数据
5. **云-本地同步**：通过 `-X` DSN 从/向 TDengine Cloud 备份恢复
6. **大规模断点续传**：数亿行数据批次备份，中断后 `-C` 续传
7. **Parquet 导出**：`-F parquet` 导出后可供 Spark/Presto 等大数据工具消费

# 约束和限制

**约束**：
1. 需要 TDengine 服务端运行中
2. 连接用户需要对目标数据库有读权限（备份）或写权限（恢复）
3. STMT2 多表模式仅在 Native 连接下生效（WebSocket/DSN 模式退化为单表模式或通过环境变量 `TAOSBK_SINGLE_TABLE=1` 强制单表）
4. Parquet 格式仅在 Linux 和 macOS 上可用（`#ifndef TD_WINDOWS` 编译守护）

**限制**：
1. 仅支持 TDengine v3.0.0.0+（STMT2 需 v3.3+）
2. 单次 `-D` 最多 64 个数据库
3. 位置参数指定表名最多 1000 个
4. 单目录最多 100,000 文件（超出自动分片）
5. `-W` 重命名最多 64 对映射

# 常见错误和排查

| 错误信息 | 可能原因 | 排查方法 |
| --- | --- | --- |
| `Result : FAILED (code: 0xA000)` | 参数无效 | 检查命令行参数，参考 `--help` |
| `get connection failed` | 连接 TDengine 失败 | 检查 `-h`/`-P` 地址、taosd 是否运行 |
| `open backup dir failed` | 恢复目录不存在 | 检查 `-i` 路径是否正确 |
| `no database found in backup directory` | 备份目录无有效数据 | 检查目录下是否存在 `{db}/db.sql` |
| `CANCELLED BY USER` | Ctrl+C 中断 | 使用 `-C` 续传 |
| `invalid .dat magic` | 备份文件损坏 | 重新备份源数据 |

# 可观测性

taosBackup 为独立命令行工具，不影响 taos shell、taosExplorer、TDinsight 等组件行为。

# 安装和卸载

- 随 TDengine 一起编译安装，二进制位于 `bin/taosBackup`
- 卸载时随安装目录删除即可

# 文档

- 需更新官网文档（工具类文档增加 taosBackup 章节）

# 参考文档

- BACKUP_RS.md（需求规格说明书）
- BACKUP_DS.md（详细设计说明书）
- BACKUP_TS.md（测试报告）

# 附录

## 支持的数据类型

源自 `bckSchemaChange.c` 中 `typeStrToTdType()` 的完整映射：

| TDengine 类型 | 内部类型码 | 备注 |
| --- | --- | --- |
| BOOL | TSDB_DATA_TYPE_BOOL | |
| TINYINT | TSDB_DATA_TYPE_TINYINT | |
| SMALLINT | TSDB_DATA_TYPE_SMALLINT | |
| INT | TSDB_DATA_TYPE_INT | |
| BIGINT | TSDB_DATA_TYPE_BIGINT | |
| FLOAT | TSDB_DATA_TYPE_FLOAT | |
| DOUBLE | TSDB_DATA_TYPE_DOUBLE | |
| BINARY(n) | TSDB_DATA_TYPE_BINARY | |
| VARCHAR(n) | TSDB_DATA_TYPE_VARCHAR | 与 BINARY 等价 |
| NCHAR(n) | TSDB_DATA_TYPE_NCHAR | |
| TIMESTAMP | TSDB_DATA_TYPE_TIMESTAMP | |
| TINYINT UNSIGNED | TSDB_DATA_TYPE_UTINYINT | |
| SMALLINT UNSIGNED | TSDB_DATA_TYPE_USMALLINT | |
| INT UNSIGNED | TSDB_DATA_TYPE_UINT | |
| BIGINT UNSIGNED | TSDB_DATA_TYPE_UBIGINT | |
| JSON | TSDB_DATA_TYPE_JSON | 仅作为 tag |
| VARBINARY | TSDB_DATA_TYPE_VARBINARY | |
| GEOMETRY | TSDB_DATA_TYPE_GEOMETRY | |
| BLOB | TSDB_DATA_TYPE_BLOB | |
| MEDIUMBLOB | TSDB_DATA_TYPE_MEDIUMBLOB | |
| DECIMAL(p,s) | DECIMAL64（p≤18）/ DECIMAL（p>18） | p/s 编码在 FieldInfo.bytes |

## 支持的平台架构

源自 `src/CMakeLists.txt`：

| 平台 | CPU 架构 |
| --- | --- |
| Linux | x86_64 (amd64)、x86 (i386)、ARM64 (aarch64)、ARMv7l (arm)、MIPS64、LoongArch64 |
| macOS (Darwin) | Apple Silicon (arm64) |
