# TDengine 安全审查报告（完整版）

**审查范围**：`source/dnode/vnode/src/tsdb/`（TSDB 全部）+ `source/` 全库关键路径
**文件规模**：474 个 C 文件，约 48 万行代码
**审查日期**：2026-04-21

---

# 一、TSDB 目录（source/dnode/vnode/src/tsdb/）

## 漏洞 T1：堆缓冲区溢出 — RocksDB 反序列化 numOfPKs

- **文件**：`tsdbCache.c:360-370`
- **严重级别**：High | **置信度**：9/10
- **类别**：`heap_buffer_overflow`

`tsdbCacheDeserialize()` 将 `numOfPKs`（uint8_t，0–255）从 RocksDB 缓存字节流中直接读取，无边界检查地驱动写入 `pLastCol->rowKey.pks[i]`：

```c
pLastCol->rowKey.numOfPKs = *(uint8_t *)(value + offset);
for (int32_t i = 0; i < pLastCol->rowKey.numOfPKs; i++) {
    pLastCol->rowKey.pks[i] = *(SValue *)(value + offset);  // ← OOB if i >= 2
}
if (offset > size) { ... }  // 已溢出后才检查
```

`SRowKey.pks` 固定大小 `TD_MAX_PK_COLS = 2`（`tdataformat.h:258`）。攻击者篡改 `cache.rdb`，构造 `numOfPKs = 200`，重启时触发堆溢出，可覆写相邻堆对象。

**修复**：循环前加 `if (numOfPKs > TD_MAX_PK_COLS) return TSDB_CODE_INVALID_DATA_FMT;`

---

## 漏洞 T2：栈缓冲区溢出 — brin block 文件解析

- **文件**：`tsdbDataFileRW.c:259-283`
- **严重级别**：High | **置信度**：9/10
- **类别**：`stack_buffer_overflow`

`brinBlk->numOfPKs` 直接来自 `.head` 磁盘文件，无界驱动写入两个固定大小为 2 的栈数组：

```c
SValueColumnCompressInfo firstInfos[TD_MAX_PK_COLS];  // 大小 2
SValueColumnCompressInfo lastInfos[TD_MAX_PK_COLS];   // 大小 2
for (int32_t i = 0; i < brinBlk->numOfPKs; i++) {    // numOfPKs 无上界
    tValueColumnCompressInfoDecode(&br, firstInfos + i); // ← 栈溢出
}
```

**修复**：`if (brinBlk->numOfPKs > TD_MAX_PK_COLS) return TSDB_CODE_FILE_CORRUPTED;`

---

## 漏洞 T3：栈缓冲区溢出 — statis block 文件解析（同类）

- **文件**：`tsdbSttFileRW.c:434-454`
- **严重级别**：High | **置信度**：9/10
- **类别**：`stack_buffer_overflow`

`statisBlk->numOfPKs` 来自 `.stt` 文件，同样无界写入大小为 2 的栈数组，模式与 T2 完全相同。

**修复**：`if (statisBlk->numOfPKs > TD_MAX_PK_COLS) return TSDB_CODE_FILE_CORRUPTED;`

---

## 漏洞 T4：堆缓冲区溢出 — statis block 转 record 填充

- **文件**：`tsdbUtil2.c:217-224`
- **严重级别**：High | **置信度**：9/10
- **类别**：`heap_buffer_overflow`

`tStatisBlockGetRecord()` 以 `statisBlock->numOfPKs`（来自漏洞 T3 的数据流）驱动写入 `pks[]`（大小 2）：

```c
for (record->firstKey.numOfPKs = 0;
     record->firstKey.numOfPKs < statisBlock->numOfPKs; // 无上界
     record->firstKey.numOfPKs++) {
    tValueColumnGet(..., &record->firstKey.pks[record->firstKey.numOfPKs]); // ← OOB
}
```

**修复**：`if (statisBlock->numOfPKs > TD_MAX_PK_COLS) return TSDB_CODE_FILE_CORRUPTED;`

---

## 漏洞 T5：堆越界读取 — V1+ 反序列化缺逐步边界检查

- **文件**：`tsdbCache.c:356-388`
- **严重级别**：High | **置信度**：8/10
- **类别**：`out_of_bounds_read`

`tsdbCacheDeserialize()` V1+ 路径的所有裸指针读操作都先于最终 `offset > size` 检查，当 V0 解析后剩余字节不足 `sizeof(SValue)` 时读取越界。

**修复**：每次读取前加 `if (offset + sizeof(T) > size) return error`，或改用 `tDecoder` 框架。

---

## 漏洞 T6：NULL 指针解引用 — 复制粘贴错误

- **文件**：`tsdbRead2.c:684-688`
- **严重级别**：High | **置信度**：9/10
- **类别**：`null_pointer_dereference`

分配 `pks[1].pData` 后，NULL 检查错误地验证 `pks[0].pData`：

```c
p->info.pks[1].pData = taosMemoryCalloc(1, pSup->pk.bytes);
TSDB_CHECK_NULL(p->info.pks[0].pData, ...);  // ← 应为 pks[1]
```

`pks[1].pData` 为 NULL 时检查通过，后续 `tsdbRead2.c:3765` 的 `memcpy(pInfo->pks[1].pData, ...)` 崩溃。

**修复**：`TSDB_CHECK_NULL(p->info.pks[1].pData, code, lino, _end, terrno);`

---

## 漏洞 T7：int64_t 截断为 int32_t — 数组边界错误

- **文件**：`tsdbDataFileRW.c:196,569`；`tsdbSttFileRW.c:123,157,191`（5处）
- **严重级别**：Medium | **置信度**：8/10
- **类别**：`integer_truncation`

```c
int32_t size = reader->headFooter->brinBlkPtr->size / sizeof(SBrinBlk); // int64→int32 截断
TARRAY2_INIT_EX(reader->brinBlkArray, size, size, data);
```

构造 `size` 截断为负数时，TARRAY2 size/capacity 与实际内存不符，后续遍历越界读。

**修复**：截断前加上界检查 `if (val / sizeof(T) > INT32_MAX) return TSDB_CODE_FILE_CORRUPTED;`

---

## 漏洞 T8：内存泄漏 — 错误路径 key_list 未释放

- **文件**：`tsdbCache.c:1931-1936`
- **严重级别**：Medium | **置信度**：9/10
- **类别**：`memory_leak`

三块内存分配后的 NULL 检查只释放了两块，`key_list` 泄漏。

**修复**：添加 `taosMemoryFree(key_list);`

---

# 二、全库其他目录

## 漏洞 G1：strcpy 无边界检查 — JSON 配置解析

- **文件**：`source/util/src/tjson.c:206`
- **严重级别**：High | **置信度**：9/10
- **类别**：`heap/stack_buffer_overflow`

`tjsonGetStringValue()` 将 JSON 字符串值 `strcpy` 到调用方缓冲区，无任何长度校验：

```c
int32_t tjsonGetStringValue(const SJson* pJson, const char* pName, char* pVal) {
    char* p = cJSON_GetStringValue(...);
    strcpy(pVal, p);   // ← 无长度检查
    return TSDB_CODE_SUCCESS;
}
```

此函数有 **112 处调用**，典型调用如：

```c
tjsonGetStringValue(dnode, "fqdn", dnodeEp.ep.fqdn);  // fqdn[128]
tjsonGetStringValue(pJson, "dbname", pCfg->dbname);   // 有固定大小
```

若本地配置文件中对应字段超出接收缓冲区（如 `fqdn` 字段超过 128 字节），触发缓冲区溢出。攻击面：任何能写入 TDengine 配置文件或 JSON 元数据文件的本地用户/进程。

**修复**：统一改为 `tjsonGetStringValue2()`（已有 `maxLen` 参数版本），或内部改用 `strncpy`：

```c
size_t pLen = strlen(p);
if (pLen >= (size_t)maxLen) return TSDB_CODE_OUT_OF_MEMORY;
memcpy(pVal, p, pLen + 1);
```

---

## 漏洞 G2：sprintf 写入固定 256 字节栈缓冲区 — HTTP 报告模块

- **文件**：`source/libs/transport/src/thttp.c:572,590`
- **严重级别**：Medium | **置信度**：8/10
- **类别**：`stack_buffer_overflow`

两处 `sprintf` 将 `server`（来自配置）与端口号格式化到 256 字节栈缓冲区：

```c
char buf[256] = {0};
sprintf(buf, "%s:%d", server, port);  // server 可达 253 字节（FQDN 上限）
```

合法 FQDN 最长 253 字符，加端口 `":65535"` = 259 字节，超出 `buf[256]` → 栈溢出。攻击面：能控制 TDengine 监控上报目标地址（`monitorFqdn` 配置项）的攻击者。

**修复**：改为 `snprintf(buf, sizeof(buf), "%s:%d", server, port);`

---

## 漏洞 G3：负数 len 传入解码器 — 客户端消息处理

- **文件**：`source/client/src/clientRawBlockWrite.c:391`
- **严重级别**：Medium | **置信度**：8/10
- **类别**：`integer_underflow`

解析服务端响应时，未校验 `metaRspLen` 的合法性就直接做减法：

```c
void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
int32_t len  = metaRsp->metaRspLen - sizeof(SMsgHead);  // 若 metaRspLen < sizeof(SMsgHead) → 负数
tDecoderInit(&coder, data, len);   // 负数 len 传入解码器
```

若 `metaRspLen < sizeof(SMsgHead)`，`len` 为负数。`tDecoderInit` 将 `int32_t len` 直接存入 `decoder->totalSize`，后续 `tDecodeXxx` 的 `decoder->pos < decoder->totalSize` 比较在无符号语义下变为极大正数，可读取任意堆内存（信息泄露）或触发崩溃。攻击面：恶意服务端或中间人。

**修复**：

```c
if (metaRsp->metaRspLen <= (int32_t)sizeof(SMsgHead)) return;
int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
```

---

# 三、汇总

## TSDB 目录

| # | 文件 | 严重级别 | 类别 | 置信度 |
|---|------|---------|------|--------|
| T1 | `tsdbCache.c:360` | **High** | Heap Buffer Overflow（numOfPKs→堆） | 9/10 |
| T2 | `tsdbDataFileRW.c:259` | **High** | Stack Buffer Overflow（numOfPKs→栈，brin block） | 9/10 |
| T3 | `tsdbSttFileRW.c:434` | **High** | Stack Buffer Overflow（numOfPKs→栈，statis block） | 9/10 |
| T4 | `tsdbUtil2.c:217` | **High** | Heap Buffer Overflow（numOfPKs→堆，record 填充） | 9/10 |
| T5 | `tsdbCache.c:356` | **High** | Out-of-Bounds Read（V1+ 反序列化） | 8/10 |
| T6 | `tsdbRead2.c:684` | **High** | NULL Pointer Dereference（错误 NULL 检查目标） | 9/10 |
| T7 | 5处 DataFileRW/SttFileRW | **Medium** | Integer Truncation（int64→int32） | 8/10 |
| T8 | `tsdbCache.c:1931` | **Medium** | Memory Leak（key_list 未释放） | 9/10 |

## 全库其他目录

| # | 文件 | 严重级别 | 类别 | 置信度 |
|---|------|---------|------|--------|
| G1 | `util/src/tjson.c:206`（112处调用） | **High** | Heap/Stack Buffer Overflow（strcpy 无边界） | 9/10 |
| G2 | `transport/src/thttp.c:572,590` | **Medium** | Stack Buffer Overflow（sprintf 256 字节缓冲） | 8/10 |
| G3 | `client/src/clientRawBlockWrite.c:391` | **Medium** | Integer Underflow（负数 len 传解码器） | 8/10 |

---

## 根因归纳

| 根因 | 涉及漏洞 | 修复方向 |
|------|---------|---------|
| 磁盘/缓存中 `numOfPKs` 字段无上界验证 | T1、T2、T3、T4 | 统一在读取后校验 `<= TD_MAX_PK_COLS` |
| 反序列化时裸指针读先于边界检查 | T1、T5 | 改用 `tDecoder` 框架逐步校验 |
| 不安全字符串函数（strcpy/sprintf） | G1、G2 | 全库 grep 替换为带长度版本 |
| 来自网络/文件的整数字段未做范围校验 | T7、G3 | 边界系统接入点统一做 sanity check |
