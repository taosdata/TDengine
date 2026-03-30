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

## 四、汇总分类

| 类型 | 描述 | 涉及文件 | 覆盖难度 | 建议优先级 |
|------|------|---------|---------|-----------|
| **A** | malloc/realloc 失败路径 | 全部文件 | 高（需 fault injection） | P3 |
| **B** | 连接/IO 失败路径 | bckDb、bckFile、restore/* | 中（需 mock 或环境异常） | P2 |
| **C** | 连接池容错（重连/中断/坏连接） | bckPool | 高（需中途停服务端） | P2 |
