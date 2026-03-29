# taosBackup 覆盖率分析报告

> **Generated:** 2026-03-27  
> **基础数据:** 18/18 PASS，全新编译（ENABLE_COVERAGE=ON），gcda 重置后首次运行

---

## 一、整体覆盖率

| Metric | Hit | Total | Coverage |
|--------|-----|-------|----------|
| Lines | 5,103 | 6,504 | **78.5%** |
| Branches | 3,592 | 6,354 | **56.5%** |
| Functions | 213 | 225 | **94.7%** |

分支覆盖率（56.5%）明显低于行覆盖率（78.5%），说明大量分支是**防御性错误处理**——代码能执行到判断语句本身，但"失败侧"从未被触发。

---

## 二、文件覆盖率排名

| 排名 | 文件 | 行覆盖 | 分支覆盖 | 未覆盖行数 | 所属模块 |
|------|------|--------|----------|-----------|---------|
| 1 🔴 | `storage/storageTaos.c` | 55.7% | 33.7% | 125 | 存储层·Binary格式 |
| 2 🔴 | `core/bckPool.c` | 54.1% | 36.8% | 56 | 核心·连接池 |
| 3 🔴 | `core/bckDb.c` | 59.3% | 42.7% | 61 | 核心·DB查询 |
| 4 🔴 | `storage/storageParquet.c` | 74.3% | 48.3% | 53 | 存储层·Parquet格式 |
| 5 🔴 | `restore/bckSchemaChange.c` | 82.5% | 50.9% | 67 | 恢复·Schema变更 |
| 6 🔴 | `core/bckProgress.c` | 85.1% | 53.4% | 14 | 核心·进度显示 |
| 7 🔴 | `core/bckFile.c` | 62.3% | 54.3% | 58 | 核心·文件操作 |
| 8 🔴 | `restore/restoreMeta.c` | 78.5% | 54.4% | 252 | 恢复·元数据 |
| 9 🔴 | `restore/restoreData.c` | 74.9% | 55.1% | 107 | 恢复·数据 |
| 10 🟡 | `storage/parquetBlock.cpp` | 84.1% | 57.2% | 69 | 存储层·Parquet块 |

---

## 三、未覆盖代码分类分析

通过逐一检查未覆盖行的源码，总结出 **6 类未覆盖代码模式**，按占比排序。

---

### 类型 A：资源分配失败路径（malloc/realloc 失败）

**占比：约 30%**，遍布几乎所有文件。

**典型代码（`core/bckDb.c`）：**
```c
char **tmp = taosMemoryRealloc(names, (capacity + 1) * sizeof(char *));
if (!tmp) {                          // ← 未覆盖
    freeArrayPtr(names);             // ← 未覆盖
    *code = TSDB_CODE_BCK_MALLOC_FAILED;
    taos_free_result(res);
    releaseConnection(conn);
    return NULL;
}
names = tmp;
```

**典型代码（`storage/storageParquet.c`）：**
```c
if (!binds2 || !decStrBufs || !decLens) {   // ← 未覆盖
    taosMemoryFree(binds2);
    taosMemoryFree(decStrBufs);
    taosMemoryFree(decLens);
    return TSDB_CODE_BCK_MALLOC_FAILED;
}
```

**原因：** 现有测试环境内存充足，`malloc`/`realloc` 从不返回 NULL。  
**覆盖方式：** 需要 fault injection（如 LD_PRELOAD 劫持内存分配函数 或 libfiu 注入）。

---

### 类型 B：连接失败 / DB 操作失败路径

**占比：约 25%**，集中在 `bckDb.c`、`bckFile.c`、`bckPool.c`、各 restore 文件。

**典型代码（`core/bckDb.c`）：**
```c
if (!conn) {
    logError("get connection failed");   // ← 未覆盖
    return NULL;
}

if (!res || *code) {
    logError("query stables failed");    // ← 未覆盖
    if (res) taos_free_result(res);
    releaseConnection(conn);
    return NULL;
}
```

**典型代码（`core/bckFile.c`）：**
```c
if (!fp) {
    logError("open file failed: %s", pathFile);   // ← 未覆盖
    taos_free_result(res);
    releaseConnection(conn);
    return TSDB_CODE_BCK_CREATE_FILE_FAILED;
}
if (written != length[col]) {
    logError("write file failed");                // ← 未覆盖
    code = TSDB_CODE_BCK_WRITE_FILE_FAILED;
    break;
}
```

**原因：** 测试在本地正常运行，连接始终成功，文件系统未满，查询未超时。  
**覆盖方式：** 需要 mock taos_connect / taosOpenFile，或模拟磁盘满、权限拒绝等异常。

---

### 类型 C：连接池容错机制（服务端重启 / 用户中断 / 坏连接回收）

**占比：约 15%**，集中在 `core/bckPool.c`。

**典型代码：**
```c
// 连接失败后指数退避重试
rollbackSlot(idx);               // ← 未覆盖：需要连接真的失败
logWarn("connect failed, retry in %d ms", reconnectWaitMs);
pthread_mutex_unlock(&g_pool.mutex);
// 小切片睡眠以响应 g_interrupted
while (slept < reconnectWaitMs) {
    if (g_interrupted) break;   // ← 未覆盖：需要在睡眠中触发中断
    taosMsleep(thisSlice);
}
reconnectWaitMs *= 2;           // ← 未覆盖：指数退避加倍

// 坏连接清除
void releaseConnectionBad(TAOS* conn) {  // ← 整函数未覆盖
    // 从连接池中移除失效连接
}

// 用户中断判断
if (g_interrupted) {
    *code = TSDB_CODE_BCK_USER_CANCEL;   // ← 未覆盖
    return NULL;
}
```

**原因：** `test_taosbackup_except.py` 测试了服务端重启场景，但重启后 taosBackup 重连成功，未触发"重连失败后退避"这一分支。`releaseConnectionBad` 只在连接被服务端主动断开时调用。  
**覆盖方式：** 需要在备份/恢复执行**中途**停止 taosd 并延迟重启，或发送 SIGINT 给 taosBackup 进程。

---

### 类型 D：Binary 格式（TaosFile）写入路径

**占比：约 12%**，集中在 `storage/storageTaos.c`。

**未覆盖的主体：** `createTaosFile()` 整函数（写 binary 格式文件头、Schema、分配写缓冲区）以及 `writeTaosFile()` 中的大块写入路径（`len >= writeBufCap` 时绕过缓冲区直接写）。

```c
TaosFile* createTaosFile(...) {     // ← 整函数未覆盖
    taosFile->fp = taosOpenFile(fileName, TD_FILE_WRITE | TD_FILE_CREATE);
    ...
    *code = writeTaosFile(taosFile, &taosFile->header, sizeof(TaosFileHeader));
    *code = writeTaosFile(taosFile, taosFile->header.schema, schemaLen);
    return taosFile;
}

// 大块直接写（不走缓冲区）
if (len >= taosFile->writeBufCap) {    // ← 未覆盖
    code = flushWriteBuffer(taosFile);
    int64_t writeLen = taosWriteFile(taosFile->fp, data, len);
}
```

**原因：** 使用 `-F binary` 格式的测试（`test_taosbackup_commandline.py` 的 basicCommandLine 用例）虽调用了 binary 格式，但对应的写缓冲区大块写路径（单次写入 ≥ 8MB 时才触发）在小数据量测试中从未触发。另外 `storageTaos.c` 主要是 binary 格式写入层，Parquet 才是主流测试路径。  
**覆盖方式：** 增加专门的 `-F binary` 大数据量测试；或在 `test_taosbackup_format.py` 中补充 binary 格式大批量写入场景。

---

### 类型 E：Schema 变更，容错边缘情况

**占比：约 10%**，集中在 `restore/bckSchemaChange.c`。

**未覆盖场景：**
```c
// CSV 文件读取失败
if (fp == NULL) {
    logError("open csv file failed: %s", csvPath);  // ← 未覆盖
    return NULL;
}
if (taosFStatFile(fp, &fileSize, NULL) != 0 || fileSize <= 0) {
    fileSize = 1024 * 1024;    // ← 未覆盖：stat 失败时的默认大小

// 服务端 schema 查询失败
if (serverCols == NULL || serverColCount == 0) {
    logWarn("ntb %s: cannot query server schema, skip"); // ← 未覆盖

// 源表与目标列完全不匹配
if (matchCount == 0) {
    logError("no matching columns between backup and server!"); // ← 未覆盖

// HashMap 哈希冲突（链表遍历 next 节点）
entry = entry->next;    // ← 未覆盖：需要 stbName 恰好哈希冲突
```

**原因：** `test_taosbackup_schema_change.py` 覆盖了正常 schema 变更（新增列）场景，但未测试：schema 完全不兼容、CSV 文件损坏、大量 STB 导致 HashMap 碰撞等边缘情况。  
**覆盖方式：** 增加测试用例：列名完全变更（无交集）、使用损坏的 CSV 文件还原、插入足够多 STB 使哈希桶碰撞。

---

### 类型 F：进度显示边缘分支

**占比：约 5%**，集中在 `core/bckProgress.c`。

**未覆盖场景：**
```c
// 极短时间（< 60s）以外的 ETA 格式
else if (s < 3600)
    snprintf(buf, sz, "~%2dm%02ds", s/60, s%60);  // ← 分钟级 ETA 未触发
else
    snprintf(buf, sz, "~%dh%02dm", ...);            // ← 小时级 ETA 未触发

// ETA 已完成状态
else if (totAll > 0 && doneAll >= totAll)
    snprintf(etaBuf, sizeof(etaBuf), "done");       // ← 未触发

// TTY 进度刷新（\r 清行）
if (g_tty_progress) {
    printf("\r%s\033[K", line);   // ← 未触发（测试跑在 pipe 模式）
```

**原因：** `test_taosbackup_progress.py` 测试了 TTY 和 pipe 模式，但数据量太小，备份在 30 秒内完成，ETA 始终显示秒级，未触发分钟/小时格式分支。TTY 模式测试中进度刷新路径实际也未被调用。  
**覆盖方式：** 设计一个运行时间超过 60 秒的数据量测试（或 mock 时间），强制触发 ETA 分钟/小时格式；在真实 TTY（pseudoterminal）中运行备份触发 `\r` 刷新路径。

---

## 四、汇总分类

| 类型 | 描述 | 涉及文件 | 覆盖难度 | 建议优先级 |
|------|------|---------|---------|-----------|
| **A** | malloc/realloc 失败路径 | 全部文件 | 高（需 fault injection） | P3 |
| **B** | 连接/IO 失败路径 | bckDb、bckFile、restore/* | 中（需 mock 或环境异常） | P2 |
| **C** | 连接池容错（重连/中断/坏连接） | bckPool | 高（需中途停服务端） | P2 |
| **D** | Binary 格式大批量写入路径 | storageTaos | 低（加大数据量即可） | **P1** |
| **E** | Schema 变更边缘情况 | bckSchemaChange | 低（补充测试用例） | **P1** |
| **F** | 进度显示边缘分支 | bckProgress | 低（增加数据量/PTY） | P2 |

---

## 五、可快速提升覆盖率的建议

### 优先级 P1（改动小、收益大）

1. **`storage/storageTaos.c` +8%~10% line**  
   在 `test_taosbackup_format.py` 或新建用例中，使用 `-F binary` + 大数据量（≥ 10万行），触发 `createTaosFile`、缓冲区满后直接写路径。

2. **`restore/bckSchemaChange.c` +5%~8% branch**  
   补充测试用例：
   - 备份后手动修改目标表使所有列名不同（`matchCount == 0` 路径）
   - 插入 100+ STB 触发 HashMap 链表遍历（`entry->next` 路径）

3. **`core/bckDb.c` + `core/bckFile.c` +5% branch（B类中较易部分）**  
   在测试用例中使用无访问权限的目录作为输出路径，触发文件打开失败路径（`errno: EACCES`）。

### 优先级 P2（需要一定工程投入）

4. **`core/bckPool.c` +8% branch**  
   在 `test_taosbackup_except.py` 中增加场景：备份启动后，在连接池**扩容期间**停止 taosd，等待 taosBackup 触发指数退避重连，然后再重启成功。

5. **`core/bckProgress.c` +6% branch**  
   使用 `pty` 模块在 pseudoterminal 中运行 taosBackup，并使用足够大的数据量让备份超过 60 秒。

### 预期可达覆盖率

| Metric | 当前 | P1完成后预期 | P2完成后预期 |
|--------|------|------------|------------|
| Lines | 78.5% | ~83% | ~87% |
| Branches | 56.5% | ~62% | ~68% |
| Functions | 94.7% | ~96% | ~97% |

---

## 六、不建议覆盖的代码

**类型 A（malloc 失败）**：这类代码在生产中极难复现，用 fault injection 覆盖的测试维护成本高，ROI 低。许多工业级项目（如 SQLite、Redis）也不覆盖 malloc 失败路径。建议维持现状，不做强制覆盖要求。
