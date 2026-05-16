# 详细设计说明书（Design Spec）- taosBackup

# 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-31 | 2026-03-31 | 1.0 | Alex Duan | 初版创建，基于源代码反推 |

# 引言

## 目的

本文档描述 taosBackup 工具的详细设计，包括模块划分、核心数据结构、备份/恢复流程、文件格式及线程模型，供开发和维护人员参考。

## 范围

涵盖 taosBackup 工具的所有已实现功能，包括备份、恢复、存储格式、连接池、断点续传、Schema 变更检测等。

## 受众

- TDengine 工具链开发工程师
- QA 测试工程师
- 技术支持工程师

# 术语

| 术语 | 定义 |
| --- | --- |
| FieldInfo | 列元信息结构体，含 name/type/bytes/encode/compress/level |
| ColMapping | Schema 变更时的列映射信息 |
| StbChangeMap | 超级表 Schema 变更哈希表（256 桶） |
| CkptHashTable | 恢复断点续传哈希表（256 桶） |
| TaosFileHeader | .dat 文件头结构体 |

# 概述

## 架构

taosBackup 采用分层模块化设计：

```text
┌─────────────┐
│   main.c    │  入口：信号处理、参数解析、分发 backup/restore
├─────────────┤
│   backup/   │  备份模块：backupMeta + backupData
│   restore/  │  恢复模块：restoreMeta + restoreData + restoreCkpt + restoreStmt/Stmt2
├─────────────┤
│   core/     │  基础设施：bckArgs + bckPool + bckLog + bckFile + bckDb + bckProgress + bckUtil
├─────────────┤
│   storage/  │  存储引擎：storageTaos(.dat) + storageParquet(.par) + blockReader + colCompress + parquetBlock
└─────────────┘
```

## 技术

| 项目 | 技术 |
| --- | --- |
| 语言 | C（主体）+ C++17（Parquet 桥接部分 parquetBlock.cpp） |
| 构建 | CMake 3.0+，仅 Linux/macOS 编译 |
| 压缩 | zstd（逐列压缩） |
| Parquet | Apache Arrow C++ 静态库（ExternalProject_Add 编译） |
| 线程 | pthread（POSIX 线程） |
| 连接 | TDengine C 客户端 libtaos（STMT/STMT2 API） |

## 依赖项

| 依赖 | 用途 | 链接方式 |
| --- | --- | --- |
| libtaos | TDengine 客户端连接和查询 | 动态链接 |
| libparquet.a | Parquet 文件读写 | 静态链接 |
| libarrow.a | Arrow 内存格式 | 静态链接 |
| libarrow_bundled_dependencies.a | Thrift/Snappy/Zstd 等 | 静态链接 |
| pthread | 多线程 | 系统库 |
| CoreFoundation / Security | macOS Arrow 依赖 | 系统框架 |

# 设计考虑

## 假设和限制

1. TDengine 服务端在备份/恢复过程中保持运行
2. 备份目录有足够磁盘空间和写权限
3. 单目录文件数上限由 `FOLDER_MAXFILE = 100,000` 控制
4. STMT2 多表模式仅 Native 连接可用
5. Parquet 后端在 Windows 上通过 `#ifndef TD_WINDOWS` 禁用

## 设计模式和原则

1. **生产-消费模式**：备份/恢复线程池按 STB 分配任务，线程从任务队列取文件处理
2. **连接池模式**：`bckPool.c` 实现固定大小连接池，三态管理，条件变量同步
3. **策略模式**：STMT1/STMT2 通过 `StmtVersion` 枚举切换运行时策略
4. **原子写入**：数据文件先写 `.tmp` 后 rename，避免中间状态
5. **O(1) 查找**：Checkpoint 和 Schema 变更使用哈希表快速查找

## 风险和缓解措施

| 风险 | 缓解措施 |
| --- | --- |
| ZSTD 符号冲突（libtaos 内含 TSZ 的 ZSTD） | `zstd_local.map` 版本脚本标记 ZSTD_* 为 local |
| 大目录文件数过多 | FOLDER_MAXFILE 自动分片到子目录 |
| 网络中断 | 指数退避重试 + 断点续传 |
| SIGINT 中途退出 | 原子写入 + checkpoint 机制 |

# 详细设计

## 组件设计

### 1. 入口模块 — main.c

- 注册信号处理：`signalHandler()` 设置 `g_interrupted`（`volatile sig_atomic_t`）
- 调用 `argsInit()` 解析参数
- 初始化连接池 `initConnectionPool()`
- 根据 `argAction()` 分发到 `backupMain()` 或 `restoreMain()`
- 打印起止摘要 `printStartSummary()` / `printEndSummary()`
- 全局统计 `BckStats g_stats`

### 2. 参数模块 — core/bckArgs.c

- 全局静态变量存储所有参数
- `argsInit()` 解析短选项（`-h/-p/-P/-u/-i/-o/-D/-F/-v/-B/-s/-S/-E/-T/-m/-k/-z/-W/-X/-Z/-C/-g/-V`）和长选项（`--help/--host/--port` 等）
- 长选项支持 `--option=value` 和 `--option value` 两种形式
- 位置参数解析：第一个非选项参数为 `g_specDb`，后续为 `g_specTables[]`
- DSN 模式：`-X` 设定或环境变量 `TDENGINE_CLOUD_DSN`
- Rename 解析：`-W` 按 `|` 分割，每对按 `=` 拆分

### 3. 连接池 — core/bckPool.c

```c
typedef enum {
    CONN_EMPTY = 0,
    CONN_IDLE,
    CONN_BUSY,
    CONN_CONNECTING
} ConnState;

// 池大小 = dataThread * 2 + tagThread + 4
```

**核心逻辑**：

- `getConnection(code)`：遍历池找 IDLE → 返回；无 IDLE 则找 EMPTY → 创建新连接；池满则 `pthread_cond_wait()` 等待
- 创建失败：指数退避（1s → 2s → 4s → ... → 30s 上限），200ms sleep 粒度检查中断
- `releaseConnection(conn)`：标记 IDLE + `pthread_cond_signal()`
- `releaseConnectionBad(conn)`：关闭连接 + 从池中移除（数组平移填充）

### 4. 备份模块

#### 4.1 backupMeta.c — 元数据备份

- `backCreateDbSql()`：`SHOW CREATE DATABASE` → `db.sql`
- `backCreateStbSql()`：`SHOW CREATE TABLE` → 追加写入 `stb.sql`（内嵌 `\n` 替换为空格，确保每 STB 一行）
- `backStbSchema()`：`DESCRIBE` → `{stb}.csv`（CSV 格式 `field,type,length,note`）
- Tag 导出：多线程将 CTB 标签写入 `tags/{stb}_data{N}.dat`
- NTB DDL 写入 `ntb.sql`
- VTAB DDL 写入 `vtb.sql`，虚拟标签写入 `vtags/`

#### 4.2 backupData.c — 数据备份

- 按 STB 创建数据线程（`-T` 个）
- 线程从 CTB 列表取任务，生成查询：`SELECT * FROM db.ctb [WHERE ts >= S AND ts <= E] [AND tbname IN ('t1','t2',...)]`
- 调用 `queryWriteBinaryEx()`（binary）或 `resultToFileParquet()`（parquet）
- 先写 `.tmp`，成功后 `rename()`
- 空结果不生成文件
- 目录分片：`fileCount / FOLDER_MAXFILE` 决定 `dirIndex`

### 5. 恢复模块

#### 5.1 restoreMeta.c — 元数据恢复

- 读取 `db.sql` 并执行（应用 `-W` 重命名）
- 读取 `stb.sql` 逐行执行
- 解析 `{stb}.csv` 构建 FieldInfo[]
- Schema 变更检测：调用 `addStbChanged()` 对比服务端
- 从 `tags/` 读取 .dat/.par 恢复标签 → 创建 CTB
- 执行 `ntb.sql` / `vtb.sql`

#### 5.2 restoreData.c — 数据恢复

- 扫描 `{stb}_data{N}/` 和 `_ntb_data{N}/` 目录获取文件列表
- 多线程分配文件
- 每文件调用 `restoreStmt2` 或 `restoreStmt` 的恢复函数

#### 5.3 restoreStmt2.c — STMT2 恢复

- 多表模式：一次 `taos_stmt2_prepare()` 准备 INSERT，64 个 pending slot（`STMT2_MULTI_TABLE_PENDING`）
- 每读取一个 .dat 文件的 block → 解压 → 绑定到对应 slot
- slot 满或切换 STB 时 `stmt2FlushMultiTableSlots()` 批量执行
- 多表模式仅 Native 且未设置 `TAOSBK_SINGLE_TABLE=1` 时启用
- DECIMAL 通过 `decimalToStr()` 转文本后以 BINARY 类型绑定

#### 5.4 restoreStmt.c — STMT1 恢复

- 逐文件：`taos_stmt_prepare()` → `taos_stmt_set_tbname()` → 循环 `taos_stmt_bind_param_batch()` + `taos_stmt_add_batch()` → `taos_stmt_execute()`
- batch 由 `-B` 控制（默认 60,000）

#### 5.5 restoreCkpt.c — 断点续传

```c
typedef struct CkptHashEntry {
    char *filePath;
    struct CkptHashEntry *next;
} CkptHashEntry;

typedef struct CkptHashTable {
    CkptHashEntry *buckets[CKPT_HASH_BUCKETS];  // 256 桶
} CkptHashTable;
```

- `loadRestoreCheckpoint()`：读取 `restore_checkpoint.txt` 每行 → `insertCkptHash()`
- `isRestoreDone(path)`：O(1) 哈希查找
- `markRestoreDone(path)`：追加写入文件 + 哈希插入
- 线程本地缓冲 `t_ckptBuf`（16KB），`flushCkptBuffer()` 定期刷盘
- 全局文件句柄 `g_ckptFp`：恢复期间保持打开

#### 5.6 bckSchemaChange.c — Schema 变更检测

```c
typedef struct StbChange {
    int      nCols;
    ColMapping *cols;
    char    *partColsStr;          // "(col1,col2,col3)"
    int     *bindIdxMap;           // bindIdxMap[backupIdx] = serverIdx or -1
    bool     changed;
} StbChange;

typedef struct StbChangeMap {
    StbChangeEntry *buckets[256];  // SCHEMA_CHANGE_MAP_BUCKETS
    pthread_mutex_t lock;
} StbChangeMap;
```

- 解析备份 CSV：`parseBackupCsv()` → `CsvColInfo[]`（field/type/length/note/tdType）
- 查询服务端：`DESCRIBE` 获取当前列
- 比对逻辑：按列名匹配、比较 type 和 bytes
- DECIMAL 类型：从 DESCRIBE 输出 `DECIMAL(p, s)` 解析 precision/scale，`p≤18` → DECIMAL64，`p>18` → DECIMAL，bytes 编码 `(actualBytes<<24)|(precision<<8)|scale`
- 结果缓存：`stbChangeMapInsert()` 写入哈希表；数据恢复阶段只读（无需加锁）

### 6. 存储引擎

#### 6.1 storageTaos.c — 二进制格式

**写入**：

```c
// 文件结构
MAGIC("TAOS", 4B) + VERSION(uint16, 2B) + TaosFileHeader + [Block]*N

// TaosFileHeader
typedef struct {
    uint32_t version;
    int32_t  actualLen;
    int32_t  rows;
    int32_t  numOfCols;
    int32_t  flagSegment;
    uint64_t groupId;
    uint32_t nBlocks;      // 关闭时回写
    uint32_t numRows;      // 累计行数
    uint32_t schemaLen;
    FieldInfo schema[];    // 列元信息数组
    char reserved[32];
} TaosFileHeader;
```

- `openTaosFileForWrite()`：写入 magic/version/header
- `writeTaosFileBlock()`：对每列进行 zstd 压缩，写入 blockHeader + 列长度数组 + 列数据
- All-NULL 优化：列长度设为 `COL_LEN_ALL_NULL = -1`，跳过数据写入
- 写缓冲：4MB（`TAOS_FILE_WRITE_BUF_SIZE`）
- 关闭时回写 nBlocks 和 numRows

**读取**：

- `openTaosFileForRead()`：验证 magic/version，读取 header 和 schema
- `readTaosFileBlocks()`：逐块读取 → 解压 → 回调处理
- BINARY 列大值触发 realloc

#### 6.2 storageParquet.c — Parquet 格式

**写入**：

- `resultToFileParquet()`：从 `TAOS_RES` 获取 raw block → `parquetWriterWriteBlock()` → Arrow row-group
- 支持 DECIMAL extended fields（precision/scale）
- `parquetWriterClose()` 完成写入

**读取**（恢复）：

- `parquetReaderReadAll()`：回调 `parStmtCallback()`
- STMT1 恢复：`taos_stmt_bind_param_batch()` + `taos_stmt_add_batch()`，每 50,000 行执行一次
- STMT2 恢复：`taos_stmt2_bind_param()` + `taos_stmt2_exec()`
- DECIMAL 列：从 Parquet 读取原始字节 → `decimalToStr()` 转文本 → 以 BINARY 绑定

#### 6.3 colCompress.c — 列压缩

- 固定长度列：null bitmap（压缩）+ 数据
- 变长列：长度前缀 + 数据
- DECIMAL：`fieldGetRawBytes()`/`fieldGetPrecision()`/`fieldGetScale()` 从 bytes 字段解包

#### 6.4 parquetBlock.cpp — C++ 桥接

- `parquetWriterCreate()` / `parquetWriterWriteBlock()` / `parquetWriterClose()`
- `parquetReaderOpen()` / `parquetReaderReadAll()` + callback
- C++ 17，Arrow C++ API

### 7. 基础设施

#### 7.1 bckFile.c — 文件操作

- `queryWriteTxt(sql, col, path)`：查询结果第 col 列写入文本文件
- `queryWriteCsv(sql, path, selectTags)`：查询结果写 CSV（标签导出）
- `queryWriteBinaryEx()`：查询结果写 .dat（主数据导出路径）
- `obtainFileName(type, db, stb, tb, ...)`：根据 `BackFileType` 枚举生成统一路径

#### 7.2 bckDb.c — 数据库操作

- 数据库查询、表查询的封装
- 虚拟表查询：`getDBVirtualTableNames()` 通过 `information_schema.ins_tables` 查询 `VIRTUAL_NORMAL_TABLE` / `VIRTUAL_CHILD_TABLE`
- 虚拟超级表检测：`isVirtualSuperTable()`

#### 7.3 bckProgress.c — 进度显示

```c
typedef struct {
    volatile int64_t phase;        // IDLE / META / DATA
    volatile int64_t dbIndex;      // 当前 db 序号
    volatile int64_t dbTotal;      // db 总数
    char dbName[64];               // 当前 db 名
    volatile int64_t stbIndex;     // 当前 stb 序号
    volatile int64_t stbTotal;     // stb 总数
    char stbName[128];             // 当前 stb 名
    volatile int64_t ctbTotalAll;  // 全局 ctb 总数
    volatile int64_t ctbDoneAll;   // 全局已完成 ctb
    volatile int64_t ctbDoneCur;   // 当前 stb 已完成 ctb
    volatile int64_t ctbTotalCur;  // 当前 stb 总 ctb
    volatile int64_t startMs;      // 开始时间戳（ms）
    volatile int  isRestore;       // 恢复模式标记
} BckProgress;
```

- 后台 pthread 每秒检查
- TTY 模式：`\r\033[K` 覆盖行
- 非 TTY：每 30 秒输出
- ETA 计算：`(totalAll - doneAll) / speed`
- 行数缩写：K/M/B

#### 7.4 bckLog.c — 日志

- `logError()/logWarn()/logInfo()/logDebug()`
- 线程安全：`flockfile()/funlockfile()`
- 4KB 缓冲区，`[HH:MM:SS]` 时间戳前缀
- DEBUG 级需 `-g` 启用

#### 7.5 bckUtil.c — 工具函数

- 通用辅助函数

## 关键数据结构

### BckStats — 全局统计

```c
typedef struct {
    volatile int64_t dbTotal;
    volatile int64_t dbSuccess;
    volatile int64_t dbFailed;
    volatile int64_t stbTotal;
    volatile int64_t ntbTotal;
    volatile int64_t dataFilesTotal;
    volatile int64_t dataFilesSkipped;
    volatile int64_t dataFilesFailed;
    volatile int64_t childTablesTotal;
    volatile int64_t totalRows;
    volatile int64_t dataFilesSizeBytes;
} BckStats;
```

### BackFileType — 文件类型枚举

```c
typedef enum BackFileType {
    BACK_DIR_DB      = 0,   // {outdir}/{db}/
    BACK_DIR_TAG     = 1,   // {outdir}/{db}/tags/
    BACK_DIR_DATA    = 2,   // {outdir}/{db}/{stb}_data{dirIndex}/
    BACK_FILE_DBSQL  = 3,   // db.sql
    BACK_FILE_STBSQL = 4,   // stb.sql
    BACK_FILE_STBCSV = 5,   // {stb}.csv
    BACK_FILE_TAG    = 6,   // tags/{stb}_data{N}.{ext}
    BACK_FILE_DATA   = 7,   // {stb}_data{dirIndex}/{tb}.{ext}
    BACK_FILE_NTBSQL = 8,   // ntb.sql
    BACK_DIR_NTBDATA = 9,   // _ntb_data{dirIndex}/
    BACK_FILE_VTBSQL = 10,  // vtb.sql
    BACK_DIR_VTAG    = 11,  // vtags/
    BACK_FILE_VTAG   = 12,  // vtags/{vstb}_data{N}.{ext}
} BackFileType;
```

### StorageFormat — 存储格式枚举

```c
typedef enum StorageFormat {
    BINARY_TAOS    = 0,
    BINARY_PARQUET = 1,
} StorageFormat;
```

### FieldInfo — 列元信息

```c
typedef struct {
    char    name[65];
    int8_t  type;
    int32_t bytes;      // 普通类型为原始字节数；DECIMAL 类型编码为 (actualBytes<<24)|(precision<<8)|scale
    int8_t  encode;
    int8_t  compress;
    int8_t  level;
} FieldInfo;
```

## 数据流图

### 备份数据流

```text
TDengine Server
    │
    ▼ SHOW CREATE DATABASE / SHOW CREATE TABLE / DESCRIBE / SELECT *
    │
┌───────────────┐
│  bckFile.c    │   queryWriteTxt() / queryWriteCsv() / queryWriteBinaryEx()
│  storageTaos  │   resultToFile()  →  zstd 逐列压缩  →  .dat
│  storagePar   │   resultToFileParquet()  →  Arrow writer  →  .par
└───────────────┘
    │
    ▼ write
    │
┌───────────────┐
│  本地文件系统   │   {outdir}/{db}/ ...
└───────────────┘
```

### 恢复数据流

```text
┌───────────────┐
│  本地文件系统   │   {outdir}/{db}/ ...
└───────────────┘
    │
    ▼ read
    │
┌───────────────┐
│  storageTaos  │   openTaosFileForRead() → readTaosFileBlocks() → 解压
│  storagePar   │   parquetReaderReadAll() → callback
└───────────────┘
    │
    ▼ STMT bind
    │
┌───────────────┐
│  restoreStmt2 │   taos_stmt2_prepare / bind_param / exec（多表批量）
│  restoreStmt  │   taos_stmt_prepare / bind_param_batch / add_batch / execute
└───────────────┘
    │
    ▼ insert
    │
TDengine Server
```

### 线程模型

```text
main thread
  ├── progressThread (pthread)     ← 后台进度显示
  ├── dataThread[0..T-1] (pthread) ← 数据备份/恢复（-T 控制）
  └── tagThread[0..m-1] (pthread)  ← 标签备份（-m 控制，仅备份用）
```

### 状态转换图 — 连接池

```text
CONN_EMPTY ──┬── taos_connect() ──→ CONN_IDLE
             │                          │
             │                    getConnection()
             │                          │
             │                          ▼
             │                       CONN_BUSY
             │                          │
             │              ┌──── releaseConnection() ──→ CONN_IDLE
             │              │
             │              └──── releaseConnectionBad() ──→ CONN_EMPTY (关闭并移除)
             │
             └── connect failed ──→ CONN_EMPTY (指数退避重试)
```

# 接口规范

## CLI 接口

详见 BACKUP_FS.md 参数列表。

## 内部 API

### bckArgs 模块

| 函数 | 返回类型 | 说明 |
| --- | --- | --- |
| `argsInit(argc, argv)` | int | 解析命令行参数 |
| `argsDestroy()` | void | 释放参数资源 |
| `argAction()` | ActionType | ACTION_BACKUP / ACTION_RESTORE |
| `argHost()` | char* | 服务器地址 |
| `argPort()` | int | 服务器端口 |
| `argUser()` | char* | 用户名 |
| `argPassword()` | char* | 密码 |
| `argOutPath()` | char* | 输出/输入路径 |
| `argBackDB()` | char** | NULL-terminated 数据库数组 |
| `argStorageFormat()` | StorageFormat | BINARY_TAOS / BINARY_PARQUET |
| `argStmtVersion()` | StmtVersion | STMT_VERSION_1 / STMT_VERSION_2 |
| `argDataThread()` | int | 数据线程数 |
| `argTagThread()` | int | 标签线程数 |
| `argSchemaOnly()` | int | 是否 schema-only |
| `argCheckpoint()` | int | 是否启用断点续传 |
| `argDebug()` | int | 是否调试模式 |
| `argTimeFilter()` | char* | 时间过滤 SQL 片段 |
| `argStartTime()` | char* | 起始时间字符串 |
| `argEndTime()` | char* | 截止时间字符串 |
| `argRenameDb(old)` | const char* | 重命名映射 |
| `argRenameList()` | const char* | 重命名原始字符串 |
| `argIsDsn()` | bool | 是否 DSN 模式 |
| `argDsn()` | const char* | DSN 字符串 |
| `argDriver()` | int8_t | CONN_MODE_* |
| `argSpecDb()` | const char* | 位置参数数据库名 |
| `argSpecTables()` | char** | 位置参数表名数组 |
| `argRetryCount()` | int | 重试次数 |
| `argRetrySleepMs()` | int | 重试间隔 |

### bckPool 模块

| 函数 | 说明 |
| --- | --- |
| `initConnectionPool(size)` | 创建连接池 |
| `getConnection(code)` | 获取连接（阻塞） |
| `releaseConnection(conn)` | 归还连接 |
| `releaseConnectionBad(conn)` | 关闭并移除连接 |
| `destroyConnectionPool()` | 销毁连接池 |

### 存储模块

| 函数 | 说明 |
| --- | --- |
| `openTaosFileForWrite(path, fields, nFields)` | 创建 .dat 写入句柄 |
| `writeTaosFileBlock(handle, block, rows)` | 写入一个数据块 |
| `closeTaosFile(handle)` | 关闭并回写 header |
| `openTaosFileForRead(path, header)` | 打开 .dat 读取 |
| `readTaosFileBlocks(handle, callback, ctx)` | 逐块读取回调 |
| `resultToFileParquet(res, path, outRows)` | TAOS_RES → .par |
| `parquetReaderOpen(path)` | 打开 .par 读取 |
| `parquetReaderReadAll(reader, callback, ctx)` | 逐 row-group 回调 |

# 安全考虑

1. 凭证仅通过命令行参数传入，不存储在备份文件中
2. 文件权限由操作系统管控
3. ZSTD 符号冲突通过 linker version script（`zstd_local.map`）隔离，防止与 libtaos 内置 ZSTD 符号碰撞导致 NULL 指针崩溃

# 性能和可扩展性

## 性能设计

1. **列式压缩**：zstd 逐列压缩，利用列内数据相似性
2. **STMT2 多表模式**：64 个 pending slot 批量写入，减少 prepare/execute 开销
3. **连接池复用**：避免反复建连开销
4. **原子写入**：`.tmp` → `rename()`，避免 fsync 开销
5. **写缓冲**：4MB 缓冲区减少系统调用

## 可扩展性

1. 线程数线性扩展：`-T` 可根据 CPU 核数调整
2. 连接池自适应：`dataThread * 2 + tagThread + 4`
3. 目录分片：FOLDER_MAXFILE 防止单目录瓶颈

# 部署和配置

## 部署流程

1. 随 TDengine 整体编译：`cmake .. && make` 在 `build/build/bin/` 生成 `taosBackup`
2. 安装到 TDengine bin 目录

## 配置管理

- 无配置文件，所有参数通过命令行传入
- 环境变量 `TDENGINE_CLOUD_DSN`：DSN 连接串
- 环境变量 `TAOSBK_SINGLE_TABLE`：设为 1 则强制 STMT2 单表模式

## 版本控制

- 版本号随 TDengine 主版本（`TD_VER_NUMBER`）
- .dat 文件格式版本：2（向前兼容需保持读取旧版本能力）
- git commit SHA 嵌入编译产物

# 监控和维护

## 日志记录

- 四级日志：ERROR（stderr）/ WARN（stderr）/ INFO（stdout）/ DEBUG（stdout，`-g`）
- 格式：`[HH:MM:SS] [LEVEL:] message`
- 4KB 缓冲，flockfile 线程安全

## 诊断

- 错误码十六进制输出（`0x%08X`）
- 结束摘要包含 db/stb/ctb 计数、成功/失败数、总行数、耗时
- `-g` 调试模式输出详细流程

## 维护

- 代码模块化，按功能域拆分到 backup/restore/core/storage 目录
- 每个组件头文件独立，最小化跨模块依赖

# 参考资料

- BACKUP_RS.md（需求规格说明书）
- BACKUP_FS.md（概要设计说明书）
- BACKUP_TS.md（测试报告）
