# vtable 链式引用流计算适配 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让流计算（new-stream）支持 vtable 多层 ref 链式追踪：把 vtable 上每个 col/tag 的链式引用一路解析到物理表 column / 实际 tag 值，并在变化时通过既有 patch / 失败终态机制通知 trigger。

**Architecture:** 在最初 vnode（reader 所在）实现批量编排函数 `streamResolveVTableRefChain`（函数 A），按 vgId 聚合后向中间 vnode 发送新增的 fetch 消息 `TDMT_VND_VTABLE_REF_RESOLVE`（vnode 通用、不依赖 stream），由处理函数 `vnodeProcessVTableRefResolveReq`（函数 B）单跳解析本地 meta 后返回。reader 维护 per-stream 的 `SStreamVTableInfoCache` 缓存解析结果，并在 WAL meta 路径上 ≥10s 节流地做 diff 检测（tag 变化→失败终态；col 终点变化→既有 IS_PATCHING_VITRUAL_TABLE patch 路径）。

**Tech Stack:** C / TDengine 内部 RPC（tmsg + tmsgdef）/ TSDB meta API / new-stream reader 框架 / pytest（new_test_framework）端到端测试。

**Spec:** `docs/superpowers/specs/2026-05-11-vtable-chain-ref-stream-design.md` v0.3

**项目约定**：
1. **不主动 commit**：每个 "Commit" step 必须等用户明确许可后再执行；
2. **不主动编译/测试**：每个 "Build" / pytest 步骤必须等用户明确许可后再执行；
3. 注释一律英文；fail-fast，不做防御式 fallback。

---

## 文件结构（一览）

| 文件 | 操作 | 责任 |
|---|---|---|
| `include/util/taoserror.h` | modify | 新增 3 个错误码 |
| `include/libs/new-stream/streamReader.h` | modify | `SStreamTriggerReaderInfo` 上挂 `SStreamVTableInfoCache vtbCache` |
| `source/libs/new-stream/inc/streamInt.h` | modify | `SColResolveItem` / `STagValue` / `SVTableResolveResult` / `SStreamVTableInfoCache` 结构定义 + cache lifecycle 函数声明 |
| `source/libs/new-stream/src/streamReader.c` | modify | cache init / destroy 实现 |
| `include/common/tmsgdef.h` | modify | `TDMT_VND_VTABLE_REF_RESOLVE` 类型注册 |
| `include/common/streamMsg.h` | modify | `EStreamVRefKind` / `SVTableRefResolveItem` / `SVTableRefResolveReq` / `SVTableRefResolveRspItem` / `SVTableRefResolveRsp` 结构 + `tSerialize/tDeserialize*` 函数声明 |
| `source/common/src/msg/streamMsg.c` | modify | 编解码实现 |
| `source/dnode/vnode/src/vnd/vnodeSvr.c` | modify | `vnodeProcessFetchMsg` 派发新消息到函数 B |
| `source/dnode/mgmt/mgmt_vnode/src/vmHandle.c` | modify | `dmSetMgmtHandle` 把 `TDMT_VND_VTABLE_REF_RESOLVE` 注册到 fetch queue（v0.4 补） |
| `source/dnode/vnode/src/vnd/vnodeStream.c` | modify | 函数 B、函数 A、`vnodeProcessStreamVTableInfoReq` 改造、PSEUDO_COL 改造、WAL meta hook + `streamRecheckVTableCache`、SMsgHead 前置、`streamFetchDbVgInfo` 走 `asyncSendMsgToServer` 拉 `TDMT_MND_GET_DB_INFO`（v0.4 补） |
| `test/cases/18-StreamProcessing/02-Stream/stream_vtable_chain_ref.py` | create | TC01–TC13 端到端 |

---

### Task 1: 新错误码定义 (T1)

**Files:**
- Modify: `include/util/taoserror.h`（在 stream 段尾追加）

- [x] **Step 1: 找到 stream 错误码段**

Run: `grep -n "TSDB_CODE_STREAM_VTB" include/util/taoserror.h | head -5`
Expected: 列出 `TSDB_CODE_STREAM_VTB_REF_TOO_DEEP` 等已有定义的行号，作为插入锚点。

- [x] **Step 2: 追加 3 个错误码定义**

在已有 `TSDB_CODE_STREAM_VTB_REF_TOO_DEEP` 行的下面追加：

```c
TAOS_DEFINE_ERROR(TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST, "Stream vtable ref table not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST,   "Stream vtable ref column/tag not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_STREAM_VTB_TAG_CHANGED,         "Stream vtable partition tag changed")
```

注意：本仓库 `taoserror.h` 错误码是 `#define + TAOS_DEFINE_ERROR` 配对，参考已有 stream 段的写法（先 `#define TSDB_CODE_STREAM_VTB_XXX TAOS_DEF_ERROR_CODE(0, 0xXXXX)` 后 `TAOS_DEFINE_ERROR(...)`），按段内最大编号 +1 顺延。

- [x] **Step 3: 待用户许可后编译验证**

Run（待用户许可后）: `cd debug && cmake --build . --target common -- -j4`
Expected: 无重复错误码冲突、编译通过。

- [x] **Step 4: 待用户许可后 commit**

```bash
git add include/util/taoserror.h
git commit -m "feat(stream): add vtable chain ref error codes"
```

---

### Task 2: 数据结构与 cache lifecycle (T2)

**Files:**
- Modify: `source/libs/new-stream/inc/streamInt.h`（新增结构 + lifecycle 函数声明）
- Modify: `include/libs/new-stream/streamReader.h`（在 `SStreamTriggerReaderInfo` 末尾添加 `SStreamVTableInfoCache vtbCache`）
- Modify: `source/libs/new-stream/src/streamReader.c`（init / destroy 实现 + 在 reader info 创建/销毁路径调用）

- [x] **Step 1: 在 streamInt.h 添加结构定义**

在 streamInt.h 末尾（保持 include 顺序合理，必要时在前部加 `#include "tcommon.h"` / `tarray.h` / `thash.h`）追加：

```c
// Resolved column reference terminal item.
// kind=COL: chain ends at a physical table column.
// kind=TAG: chain ends at a child table tag value (carried by STagValue elsewhere).
typedef struct SColResolveItem {
  bool    hasRef;
  char    refDbName   [TSDB_DB_NAME_LEN];
  char    refTableName[TSDB_TABLE_NAME_LEN];
  char    refColName  [TSDB_COL_NAME_LEN];
} SColResolveItem;

typedef struct STagValue {
  int8_t   type;
  int32_t  nLen;
  char    *pData;       // owned, freed by destroy helper
} STagValue;

typedef struct SVTableResolveResult {
  SSHashObj *colMap;    // key: virtual col cid (col_id_t), value: SColResolveItem*
  SSHashObj *tagMap;    // key: virtual tag cid (col_id_t), value: STagValue*
} SVTableResolveResult;

typedef struct SStreamVTableInfoCache {
  SRWLatch    lock;
  SArray     *reqColCids;     // SArray<col_id_t>
  SArray     *reqTagCids;     // SArray<col_id_t>
  SSHashObj  *uid2Result;     // key: int64_t uid, value: SVTableResolveResult*
  SHashObj   *dbVgInfo;       // key: dbFName, value: SUseDbRsp
  int64_t     lastCheckMs;     // ms timestamp; throttle the timed re-check (>= 10s)
  bool        valid;
} SStreamVTableInfoCache;

int32_t streamVTableInfoCacheInit   (SStreamVTableInfoCache *pCache);
void    streamVTableInfoCacheDestroy(SStreamVTableInfoCache *pCache);
void    streamVTableResolveResultDestroy(SVTableResolveResult *pRes);
```

- [x] **Step 2: 在 streamReader.h 挂载到 SStreamTriggerReaderInfo**

定位 `SStreamTriggerReaderInfo` 结构体（约 line 80）末尾、闭合大括号前，添加：

```c
  SStreamVTableInfoCache vtbCache;   // chain-resolve cache
```

streamReader.h 顶部如缺 `streamInt.h` 依赖，前向声明 `struct SStreamVTableInfoCache;` 然后这里改为指针 `SStreamVTableInfoCache *vtbCache`，并在 init/destroy 路径里 alloc/free。结构选择"嵌入还是指针"取决于现有头文件依赖（streamInt.h 是私有头，不应 include 到对外 streamReader.h）—— **采用前向声明 + 指针** 方式：

```c
// in streamReader.h, near top:
struct SStreamVTableInfoCache;
typedef struct SStreamVTableInfoCache SStreamVTableInfoCache;

// in struct SStreamTriggerReaderInfo:
  SStreamVTableInfoCache *vtbCache;
```

- [x] **Step 3: 在 streamReader.c 实现 init / destroy**

```c
int32_t streamVTableInfoCacheInit(SStreamVTableInfoCache *pCache) {
  if (pCache == NULL) return TSDB_CODE_INVALID_PARA;
  taosInitRWLatch(&pCache->lock);
  pCache->reqColCids  = taosArrayInit(0, sizeof(col_id_t));
  pCache->reqTagCids  = taosArrayInit(0, sizeof(col_id_t));
  pCache->uid2Result  = tSimpleHashInit(64, taosGetI64HashFunc());
  pCache->dbVgInfo    = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  pCache->lastCheckMs = 0;
  pCache->valid       = false;
  if (!pCache->reqColCids || !pCache->reqTagCids || !pCache->uid2Result || !pCache->dbVgInfo) {
    streamVTableInfoCacheDestroy(pCache);
    return terrno;
  }
  return 0;
}

void streamVTableResolveResultDestroy(SVTableResolveResult *pRes) {
  if (!pRes) return;
  if (pRes->colMap) {
    void *iter = NULL;  size_t kLen = 0;
    while ((iter = tSimpleHashIterate(pRes->colMap, iter, &kLen))) {
      SColResolveItem **pp = (SColResolveItem **)iter;
      taosMemoryFreeClear(*pp);
    }
    tSimpleHashCleanup(pRes->colMap);
  }
  if (pRes->tagMap) {
    void *iter = NULL;  size_t kLen = 0;
    while ((iter = tSimpleHashIterate(pRes->tagMap, iter, &kLen))) {
      STagValue **pp = (STagValue **)iter;
      if (*pp) taosMemoryFreeClear((*pp)->pData);
      taosMemoryFreeClear(*pp);
    }
    tSimpleHashCleanup(pRes->tagMap);
  }
  taosMemoryFree(pRes);
}

void streamVTableInfoCacheDestroy(SStreamVTableInfoCache *pCache) {
  if (!pCache) return;
  if (pCache->uid2Result) {
    void *iter = NULL;  size_t kLen = 0;
    while ((iter = tSimpleHashIterate(pCache->uid2Result, iter, &kLen))) {
      SVTableResolveResult **pp = (SVTableResolveResult **)iter;
      streamVTableResolveResultDestroy(*pp);
    }
    tSimpleHashCleanup(pCache->uid2Result);
  }
  taosArrayDestroy(pCache->reqColCids);
  taosArrayDestroy(pCache->reqTagCids);
  taosHashCleanup(pCache->dbVgInfo);
  pCache->uid2Result  = NULL;
  pCache->reqColCids  = NULL;
  pCache->reqTagCids  = NULL;
  pCache->dbVgInfo    = NULL;
  pCache->valid       = false;
}
```

- [x] **Step 4: 在 reader info 创建/销毁路径调用 lifecycle**

定位 `SStreamTriggerReaderInfo` 的创建函数（grep `SStreamTriggerReaderInfo *` 找 alloc / 工厂），在分配后调用：

```c
pInfo->vtbCache = taosMemoryCalloc(1, sizeof(SStreamVTableInfoCache));
if (pInfo->vtbCache == NULL) { /* fail-fast */ return terrno; }
int32_t code = streamVTableInfoCacheInit(pInfo->vtbCache);
if (code) { taosMemoryFreeClear(pInfo->vtbCache); return code; }
```

在销毁函数对应位置：

```c
if (pInfo->vtbCache) {
  streamVTableInfoCacheDestroy(pInfo->vtbCache);
  taosMemoryFreeClear(pInfo->vtbCache);
}
```

- [x] **Step 5: 待用户许可后编译验证**

Run: `cd debug && cmake --build . --target stream -- -j4`
Expected: 编译通过；如果 `tSimpleHashIterate` / `taosInitRWLatch` 等签名与本地实际签名不一致，根据编译报错调整。

- [x] **Step 6: 待用户许可后 commit**

```bash
git add source/libs/new-stream/inc/streamInt.h \
        include/libs/new-stream/streamReader.h \
        source/libs/new-stream/src/streamReader.c
git commit -m "feat(stream): add vtable chain resolve cache structures"
```

---

### Task 3: TDMT_VND_VTABLE_REF_RESOLVE 消息类型与请求/响应结构 (T3a)

**Files:**
- Modify: `include/common/tmsgdef.h`（在 `TD_NEW_MSG_SEG(TDMT_VND_STREAM_MSG)` 之外、TDMT_VND 段内追加）
- Modify: `include/common/streamMsg.h`（追加结构体 + 编解码函数声明）

- [x] **Step 1: 注册新消息类型**

在 `include/common/tmsgdef.h` 中找到合适的 VND fetch 段（参考 `TDMT_VND_VSUBTABLES_META`），追加：

```c
TD_DEF_MSG_TYPE(TDMT_VND_VTABLE_REF_RESOLVE, "vnode-vtable-ref-resolve", NULL, NULL)
```

注意：要严格保持现有消息编号顺序，新增放在该 SEG 末尾、`TDMT_VND_MAX_MSG` 之前。

- [x] **Step 2: 在 streamMsg.h 追加请求/响应结构**

在 `SSTriggerVirTableInfoRequest` 附近（同段聚合）追加：

```c
typedef enum {
  STREAM_VREF_KIND_COL = 1,
  STREAM_VREF_KIND_TAG = 2,
} EStreamVRefKind;

typedef struct SVTableRefResolveItem {
  int8_t  kind;                                  // EStreamVRefKind
  char    refDbName   [TSDB_DB_NAME_LEN];
  char    refTableName[TSDB_TABLE_NAME_LEN];
  char    refColName  [TSDB_COL_NAME_LEN];
} SVTableRefResolveItem;

typedef struct SVTableRefResolveReq {
  int64_t  ver;
  SArray  *items;                                // SArray<SVTableRefResolveItem>
} SVTableRefResolveReq;

typedef struct SVTableRefResolveRspItem {
  int32_t  code;
  bool     terminated;
  SVTableRefResolveItem nextRef;                 // doubly-purpose: next-hop ref OR terminal physical (kind=COL)
  // tag value carried separately to keep encoding straightforward:
  int8_t   tagType;
  int32_t  tagLen;
  char    *tagData;                              // owned by recv side
} SVTableRefResolveRspItem;

typedef struct SVTableRefResolveRsp {
  SArray *items;                                 // SArray<SVTableRefResolveRspItem>, same order as req
} SVTableRefResolveRsp;

int32_t tSerializeSVTableRefResolveReq  (void *buf, int32_t bufLen, const SVTableRefResolveReq *pReq);
int32_t tDeserializeSVTableRefResolveReq(void *buf, int32_t bufLen,       SVTableRefResolveReq *pReq);
void    tFreeSVTableRefResolveReq       (SVTableRefResolveReq *pReq);

int32_t tSerializeSVTableRefResolveRsp  (void *buf, int32_t bufLen, const SVTableRefResolveRsp *pRsp);
int32_t tDeserializeSVTableRefResolveRsp(void *buf, int32_t bufLen,       SVTableRefResolveRsp *pRsp);
void    tFreeSVTableRefResolveRsp       (SVTableRefResolveRsp *pRsp);
```

> 设计说明：响应里把 tag 值 `(tagType, tagLen, tagData)` 平铺，避免单独引用 `STagValue`（streamMsg.h 不应依赖 streamInt.h 的私有结构）。reader 在函数 A 收到响应后再装箱成 `STagValue` 写入 cache。

- [x] **Step 3: 待用户许可后 commit**

```bash
git add include/common/tmsgdef.h include/common/streamMsg.h
git commit -m "feat(stream): declare TDMT_VND_VTABLE_REF_RESOLVE and structures"
```

---

### Task 4: SVTableRefResolveReq/Rsp 编解码实现 (T3b)

**Files:**
- Modify: `source/common/src/msg/streamMsg.c`

- [x] **Step 1: 实现 tSerializeSVTableRefResolveReq**

```c
int32_t tSerializeSVTableRefResolveReq(void *buf, int32_t bufLen, const SVTableRefResolveReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->ver) < 0) return -1;
  int32_t n = (pReq->items != NULL) ? taosArrayGetSize(pReq->items) : 0;
  if (tEncodeI32(&encoder, n) < 0) return -1;
  for (int32_t i = 0; i < n; ++i) {
    SVTableRefResolveItem *p = taosArrayGet(pReq->items, i);
    if (tEncodeI8     (&encoder, p->kind        ) < 0) return -1;
    if (tEncodeCStr   (&encoder, p->refDbName   ) < 0) return -1;
    if (tEncodeCStr   (&encoder, p->refTableName) < 0) return -1;
    if (tEncodeCStr   (&encoder, p->refColName  ) < 0) return -1;
  }
  tEndEncode(&encoder);
  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}
```

- [x] **Step 2: 实现 tDeserializeSVTableRefResolveReq**

对称地用 `SDecoder`，items 用 `taosArrayInit(n, sizeof(SVTableRefResolveItem))` + `taosArrayPush`，每条用 `tDecodeI8/tDecodeCStrTo` 填回。任何中途失败要 `taosArrayDestroy(pReq->items)` 后返回 `-1`。

- [x] **Step 3: 实现 tFreeSVTableRefResolveReq**

```c
void tFreeSVTableRefResolveReq(SVTableRefResolveReq *pReq) {
  if (!pReq) return;
  taosArrayDestroy(pReq->items);
  pReq->items = NULL;
}
```

- [x] **Step 4: 实现 tSerializeSVTableRefResolveRsp**

每条 rsp item 序列化：`code (i32) | terminated (i8) | nextRef.kind (i8) | nextRef.refDbName/Tbl/Col (cstr) | tagType (i8) | tagLen (i32) | tagData (binary, lenBytes=tagLen, 仅当 terminated && kind=TAG)`。约束：当 `terminated=false || kind=COL` 时 `tagLen=0` 且不写 tagData。

- [x] **Step 5: 实现 tDeserializeSVTableRefResolveRsp + tFreeSVTableRefResolveRsp**

对称解码；free 时遍历 items 释放 `tagData`。

- [x] **Step 6: 待用户许可后编译验证**

Run: `cd debug && cmake --build . --target common -- -j4`
Expected: 编译通过。

- [x] **Step 7: 待用户许可后 commit**

```bash
git add source/common/src/msg/streamMsg.c
git commit -m "feat(stream): serde for SVTableRefResolveReq/Rsp"
```

---

### Task 5: 函数 B 实现 + svr 派发 (T4)

**Files:**
- Modify: `source/dnode/vnode/src/vnd/vnodeStream.c`（新增 `vnodeProcessVTableRefResolveReq`）
- Modify: `source/dnode/vnode/src/vnd/vnodeSvr.c:1099`（在 switch 加 case）
- Modify: `source/dnode/vnode/src/inc/vnodeInt.h`（如有 vnode 内部函数声明集中处，追加 `vnodeProcessVTableRefResolveReq` 声明）

- [x] **Step 1: 在 vnodeStream.c 实现函数 B 骨架**

```c
// Process TDMT_VND_VTABLE_REF_RESOLVE: single-hop chain resolver. Reads local meta only,
// does NOT depend on stream task existence.
int32_t vnodeProcessVTableRefResolveReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  SVTableRefResolveReq req = {0};
  SVTableRefResolveRsp rsp = {0};

  if (tDeserializeSVTableRefResolveReq(pMsg->pCont, pMsg->contLen, &req) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _end;
  }
  int32_t n = (req.items != NULL) ? taosArrayGetSize(req.items) : 0;
  rsp.items = taosArrayInit(n, sizeof(SVTableRefResolveRspItem));
  if (rsp.items == NULL) { code = terrno; goto _end; }

  for (int32_t i = 0; i < n; ++i) {
    SVTableRefResolveItem    *q = taosArrayGet(req.items, i);
    SVTableRefResolveRspItem  r = {0};
    code = vnodeResolveOneHop(pVnode, q, &r);    // helper described in Step 2
    if (code != 0) { r.code = code; code = 0; }   // per-item error, never abort batch
    if (taosArrayPush(rsp.items, &r) == NULL) { code = terrno; goto _end; }
  }

  // encode rsp into pMsg->info.rsp
  int32_t rspLen = tSerializeSVTableRefResolveRsp(NULL, 0, &rsp);
  if (rspLen < 0) { code = TSDB_CODE_OUT_OF_MEMORY; goto _end; }
  void *pBuf = rpcMallocCont(rspLen);
  if (pBuf == NULL) { code = terrno; goto _end; }
  if (tSerializeSVTableRefResolveRsp(pBuf, rspLen, &rsp) < 0) {
    rpcFreeCont(pBuf); code = TSDB_CODE_OUT_OF_MEMORY; goto _end;
  }
  pMsg->info.rsp     = pBuf;
  pMsg->info.rspLen  = rspLen;

_end:
  tFreeSVTableRefResolveReq(&req);
  tFreeSVTableRefResolveRsp(&rsp);
  return code;
}
```

- [x] **Step 2: 实现单跳解析 helper `vnodeResolveOneHop`**

```c
// Look up (refDbName, refTableName, refColName) on this vnode's meta.
// - table not on this vnode → r->code = TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST
// - col not found           → r->code = TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST
// - table is vtable         → r->terminated=false, r->nextRef = SColRef of that col/tag
// - table is normal/child   → r->terminated=true:
//     kind=COL → r->nextRef = (db, table, col) terminal triple
//     kind=TAG → fetch tag value and fill (r->tagType,tagLen,tagData)
static int32_t vnodeResolveOneHop(SVnode *pVnode, const SVTableRefResolveItem *q,
                                  SVTableRefResolveRspItem *r) {
  SMetaReader mr = {0};
  metaReaderDoInit(&mr, pVnode->pMeta, META_READER_LOCK);
  int32_t code = metaGetTableEntryByName(&mr, q->refTableName);
  if (code != 0) {
    metaReaderClear(&mr);
    return TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST;
  }
  // distinguish vtable vs normal/child by mr.me.type:
  bool isVtable = (mr.me.type == TSDB_VIRTUAL_NORMAL_TABLE || mr.me.type == TSDB_VIRTUAL_CHILD_TABLE);
  if (q->kind == STREAM_VREF_KIND_COL) {
    if (isVtable) {
      // find SColRef for refColName, fill r->nextRef from it; terminated=false
      // ... walk mr.me.colRef[].name == q->refColName
      // if hasRef==false (NULL ref) treat as terminated=true with same triple? per spec NULL ref is legal
    } else {
      // physical: terminated=true, r->nextRef = (refDbName, refTableName, refColName)
      r->terminated = true;
      r->nextRef.kind = STREAM_VREF_KIND_COL;
      tstrncpy(r->nextRef.refDbName,   q->refDbName,   TSDB_DB_NAME_LEN);
      tstrncpy(r->nextRef.refTableName,q->refTableName,TSDB_TABLE_NAME_LEN);
      tstrncpy(r->nextRef.refColName,  q->refColName,  TSDB_COL_NAME_LEN);
    }
  } else {  // STREAM_VREF_KIND_TAG
    if (isVtable) {
      // similar walk on tag schema's ColRef
    } else {
      // child table: read STag for refColName from mr.me.ctbEntry.pTags, fill (tagType,tagLen,tagData)
      r->terminated = true;
    }
  }
  metaReaderClear(&mr);
  return 0;
}
```

> 实现细节（vtable 的 colRef 数组结构、`tagSchemaGet*` API）参考现有 `vnodeGetVSubtablesMeta` / `vTableInfo` 编码路径找对应 helper；不要为 vtable 写新的 meta 解析代码，复用既有函数。

- [x] **Step 3: 在 vnodeSvr.c 派发新消息**

定位 `vnodeProcessFetchMsg` 内 switch（第 1112 行起），在 `TDMT_VND_VSUBTABLES_META` 之后追加：

```c
    case TDMT_VND_VTABLE_REF_RESOLVE:
      return vnodeProcessVTableRefResolveReq(pVnode, pMsg);
```

并把 `TDMT_VND_VTABLE_REF_RESOLVE` 也加入第 1104 行的 `syncIsReadyForRead` 检查列表（纯 meta 读，要求 sync ready）：

```c
       pMsg->msgType == TDMT_VND_VSTB_REF_DBS ||
       pMsg->msgType == TDMT_VND_VTABLE_REF_RESOLVE) &&
```

- [x] **Step 4: 待用户许可后编译验证**

Run: `cd debug && cmake --build . --target vnode -- -j4`
Expected: 编译通过。

- [x] **Step 5: 待用户许可后 commit**

```bash
git add source/dnode/vnode/src/vnd/vnodeStream.c \
        source/dnode/vnode/src/vnd/vnodeSvr.c \
        source/dnode/vnode/src/inc/vnodeInt.h
git commit -m "feat(stream): vnode handler for TDMT_VND_VTABLE_REF_RESOLVE"
```

---

### Task 6: 函数 A — 单 vgId RPC 主循环 (T5a)

**Files:**
- Modify: `source/dnode/vnode/src/vnd/vnodeStream.c`（新增 `streamResolveVTableRefChain` + 内部 helper）

- [x] **Step 1: 定义内部 work-item 结构与函数签名**

```c
typedef struct {
  int64_t  originVtbUid;
  int32_t  originCid;
  int8_t   kind;                   // EStreamVRefKind
  char     refDbName   [TSDB_DB_NAME_LEN];
  char     refTableName[TSDB_TABLE_NAME_LEN];
  char     refColName  [TSDB_COL_NAME_LEN];
} SResolveWorkItem;

#define STREAM_VTB_MAX_HOPS  32

// v0.3 实现：合并 col/tag 为单 SVTableResolveResult map；pCache 上提为独立可空参数
//   pCache=NULL → 不读不写 dbVgInfo cache（PSEUDO_COL 路径用）
int32_t streamResolveVTableRefChain(SVnode                   *pVnode,
                                    SStreamVTableInfoCache   *pCache,
                                    SStreamTriggerReaderInfo *pReaderInfo,
                                    int64_t                    ver,
                                    SArray                    *vtbUids,
                                    SArray                    *virtColCids,
                                    SArray                    *virtTagCids,
                                    SSHashObj                **ppUid2Result);
```

- [x] **Step 2: 实现 work-list 初始化（单 vgId 简版，先不聚合）**

主体骨架：
```c
int32_t streamResolveVTableRefChain(...) {
  int32_t code = 0;
  SArray *workList = taosArrayInit(0, sizeof(SResolveWorkItem));
  *uid2ColMap = tSimpleHashInit(64, taosGetI64HashFunc());
  *uid2TagMap = tSimpleHashInit(64, taosGetI64HashFunc());
  if (!workList || !*uid2ColMap || !*uid2TagMap) { code = terrno; goto _end; }

  // 1. 用 vtbUids 初始化 work-list（暂不处理 NULL=全量分支，留到 Task 8）
  if (vtbUids == NULL) { code = TSDB_CODE_INVALID_PARA; goto _end; }
  int32_t nUid = taosArrayGetSize(vtbUids);
  for (int32_t i = 0; i < nUid; ++i) {
    int64_t uid = *(int64_t *)taosArrayGet(vtbUids, i);
    code = pushInitialWorkItemsForUid(pVnode, pInfo, uid, virtColCids, virtTagCids, workList);
    if (code != 0) {
      ST_TASK_WLOG("vtb uid:%" PRId64 " init failed, skip per H2", uid);
      code = 0;  // H2: per-uid skip
    }
  }
  // 2. 主循环：留到 Task 7（按 vgId 聚合 + RPC + 回填）
  ...
_end:
  taosArrayDestroy(workList);
  if (code != 0) {
    streamFreeUid2Map(*uid2ColMap, *uid2TagMap);
    *uid2ColMap = *uid2TagMap = NULL;
  }
  return code;
}
```

实现 `pushInitialWorkItemsForUid`：加锁 metaReader 拿 vtable entry，迭代 `cid in virtColCids`/`virtTagCids`：

- **column 分支**：找其 `SColRef`，`hasRef=true` → push work item；`hasRef=false`（链式中显式 NULL 占位）→ 直接写 `uid2Result->colMap[cid] = {hasRef=false}`。
- **tag 分支**：找其 `SColRef`，`hasRef=true` → push work item；`hasRef=false` 且当前 entry 是 `TSDB_VIRTUAL_CHILD_TABLE` → 调 `streamReadChildTagConstValue` 读 vchild 自身 STag 的常量 tag 值，直接写 `uid2Result->tagMap[cid] = {type, nLen, pData}`；`hasRef=false` 但 entry 不是 CHILD vtable → 报 `TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST`（NORMAL vtable 没有 tag）。

任何 ref 三元组不合法（hasRef=true 但字段空）→ return non-zero（外层 catch 后 H2 整 uid 跳过）。

- [x] **Step 3: 实现单 vgId 同步 RPC helper（暂时所有 work-item 视为同一 vgId，留到 Task 7 拆分）**

```c
// Send TDMT_VND_VTABLE_REF_RESOLVE to a single vgroup. v0.3 实现：单次同步 RPC，
// 失败即返回非零让 caller 把整组 uid 标 skipped；不做本地重试（依赖 ≥10s 节流定时检测）。
static int32_t streamSendOneVgResolveRpc(SVnode *pVnode, const SEpSet *pEpSet, int32_t vgId,
                                         int64_t ver, SArray *batch, SArray *indexList,
                                         SArray *outRspItems);
```

实现要点：构建 `SVTableRefResolveReq.items` 与 batch 严格同序；调用 `rpcSendRecv` 同步语义；单次失败直接 return（caller `streamCallResolveBatched` 收到非零后把该 vg 的 uid 全部加入 skipped 集合）。

- [x] **Step 4: 实现回填与下一轮组装**

```c
// for each rsp item r at index i:
//   work = batch[i]
//   if r.code != 0:                       // 业务错误 H2
//     mark work->originVtbUid as skipped (set flag in skipped-set)
//     continue
//   if r.terminated:
//     if work->kind == COL:
//       SColResolveItem *item = ...; copy from r.nextRef; insert to (*uid2ColMap)[uid][cid]
//     else:
//       STagValue *tv = malloc; copy (r.tagType, tagLen, dup tagData); insert to (*uid2TagMap)
//   else:
//     // next hop: construct new SResolveWorkItem from r.nextRef (originVtbUid/Cid/kind 不变)
//     push to nextWorkList
// 主循环: workList = nextWorkList，hop++ ；hop > STREAM_VTB_MAX_HOPS 则余下 originVtbUid 全部按 H2 跳过
```

- [x] **Step 5: 已被 H2 跳过的 uid 必须从 `uid2ColMap`/`uid2TagMap` 中移除**

跳过逻辑：维护 `SHashObj *skippedUids`，每轮回填后遍历 work-item，对 originVtbUid 在 skipped 集中的：把已写入的 partial col/tag entry 回滚（避免半成品 cache）。

- [x] **Step 6: 待用户许可后编译验证**

Run: `cd debug && cmake --build . --target vnode -- -j4`
Expected: 编译通过。

- [x] **Step 7: 待用户许可后 commit**

```bash
git add source/dnode/vnode/src/vnd/vnodeStream.c
git commit -m "feat(stream): chain resolver A (single-vg loop + retry)"
```

---

### Task 7: 函数 A — 跨 vgId 聚合 + dbVgInfo 路由 (T5b)

**Files:**
- Modify: `source/dnode/vnode/src/vnd/vnodeStream.c`

- [x] **Step 1: 实现 dbVgInfo 同步获取 helper**

```c
// Look up (or fetch from mnode) SUseDbRsp for dbFName, cached in pInfo->vtbCache.dbVgInfo.
static int32_t getOrFetchDbVgInfo(SVnode *pVnode, SStreamTriggerReaderInfo *pInfo,
                                  const char *dbFName, SUseDbRsp **ppRsp);
```

实现要点：先 `taosHashGet(pInfo->vtbCache.dbVgInfo, dbFName, ...)`；miss 则发 `TDMT_MND_GET_DB_INFO` 同步 RPC（参考 catalog 已有调用代码），写回 cache；任何错误返回非零让上层走技术性错误路径。

- [x] **Step 2: 实现 vgId 路由**

```c
// For (dbFName, refTableName), compute target vgId by hashing tableName per the db's vgInfo.
static int32_t routeToVgId(const SUseDbRsp *pDbRsp, const char *refTableName,
                           int32_t *pVgId, SEpSet *pEpSet);
```

复用 catalog/parser 里已有 `tableNameToVgId` 之类 helper（`grep -rn "vgHashFunc\|getTableHashVgId"` 找）。

- [x] **Step 3: 在主循环中按 vgId 聚合**

```c
// For each iteration:
// 1. Build SHashObj<vgId, SArray<int>> = work-item indices grouped by target vg.
// 2. For each (vgId, indexList):
//      build batch SArray<SResolveWorkItem*> in indexList order;
//      sendOneRefResolveRpc(...);
//      backfill rsp items per index → originVtbUid/Cid;
// 3. nextWorkList = unterminated items; loop or exit.
```

- [x] **Step 4: 待用户许可后编译验证**

Run: `cd debug && cmake --build . --target vnode -- -j4`
Expected: 编译通过。

- [x] **Step 5: 待用户许可后 commit**

```bash
git add source/dnode/vnode/src/vnd/vnodeStream.c
git commit -m "feat(stream): chain resolver A (multi-vg aggregation + dbVgInfo)"
```

---

### Task 8: 函数 A — 全量 uid 分支 (T5c)

**Files:**
- Modify: `source/dnode/vnode/src/vnd/vnodeStream.c`

- [x] **Step 1: 全量 uid 分支接入**

在 Task 6 Step 2 留下的 `vtbUids == NULL` 分支替换：

```c
SArray *fullUids = NULL;
if (vtbUids == NULL || taosArrayGetSize(vtbUids) == 0) {
  fullUids = taosArrayInit(64, sizeof(int64_t));
  if (fullUids == NULL) { code = terrno; goto _end; }
  // mirror getAllVinfo @ vnodeStream.c:3689
  StreamTableListInfo *pTbList = qStreamGetTableArrayList(pInfo);
  int32_t nAll = (pTbList && pTbList->pTableList) ? taosArrayGetSize(pTbList->pTableList) : 0;
  for (int32_t i = 0; i < nAll; ++i) {
    SStreamTableKeyInfo *k = taosArrayGet(pTbList->pTableList, i);
    if (k->markedDeleted) continue;
    taosArrayPush(fullUids, &k->uid);
  }
  vtbUids = fullUids;
}
// ... 走通用初始化 work-list 逻辑
_end:
  taosArrayDestroy(fullUids);
```

- [x] **Step 2: 待用户许可后编译验证**

Run: `cd debug && cmake --build . --target vnode -- -j4`
Expected: 编译通过。

- [x] **Step 3: 待用户许可后 commit**

```bash
git add source/dnode/vnode/src/vnd/vnodeStream.c
git commit -m "feat(stream): chain resolver A (full-uid branch via qStreamGetTableArrayList)"
```

---

### Task 9: VTABLE_INFO reader 改造 (T8)

**Files:**
- Modify: `source/dnode/vnode/src/vnd/vnodeStream.c`（`vnodeProcessStreamVTableInfoReq` @ 3751）

- [x] **Step 1: 在入口新增"从 partitionCols 解析 tagCids"**

```c
SArray *tagCids = taosArrayInit(0, sizeof(col_id_t));
SNodeList *partCols = pInfo->partitionCols;
SNode     *pNode    = NULL;
FOREACH(pNode, partCols) {
  if (nodeType(pNode) == QUERY_NODE_COLUMN) {
    SColumnNode *c = (SColumnNode *)pNode;
    if (c->colType == COLUMN_TYPE_TAG) {
      taosArrayPush(tagCids, &c->colId);
    }
  }
}
```

- [x] **Step 2: 替换原有 `getAllVinfo` / `getSpicificVinfo` 调用**

现有分支 :3770 改为：
```c
SArray *uidList = NULL;
bool    fullScan = req.fetchAllTable || req.uids == NULL || taosArrayGetSize(req.uids) == 0;
if (!fullScan) uidList = req.uids;
SArray *cids = req.cids;            // 已有

SSHashObj *uid2ColMap = NULL, *uid2TagMap = NULL;
code = streamResolveVTableRefChain(pVnode, pInfo, req.ver, uidList,
                                   cids, tagCids, &uid2ColMap, &uid2TagMap);
if (code != 0) {
  taosArrayDestroy(tagCids);
  goto _send_err;
}
```

- [x] **Step 3: 编码响应（仅 colMap，不含 tagMap）**

按现有 `vTableInfo->infos` 结构编码。fullScan：遍历 uid2ColMap 全部 uid；局部：遍历 reqUids ∩ uid2ColMap。每个 uid 输出每个 cids 对应 col 终点（hasRef + 三元组）。**tagMap 完全不进响应**。

- [x] **Step 4: 写 cache**

```c
taosWLockLatch(&pInfo->vtbCache->lock);
if (fullScan) {
  // C2a 原子替换
  SSHashObj *newMap = tSimpleHashInit(...);
  void *iter = NULL;  size_t kLen = 0;
  while ((iter = tSimpleHashIterate(uid2ColMap, iter, &kLen))) {
    int64_t uid = *(int64_t *)tSimpleHashGetKey(iter, &kLen);
    SVTableResolveResult *r = taosMemoryCalloc(1, sizeof(*r));
    r->colMap = *(SSHashObj **)iter;                        // move ownership
    r->tagMap = takeTagMap(uid2TagMap, uid);                // helper that detaches
    tSimpleHashPut(newMap, &uid, sizeof(uid), &r, POINTER_BYTES);
  }
  // free old
  destroyUid2Result(pInfo->vtbCache->uid2Result);
  pInfo->vtbCache->uid2Result = newMap;
} else {
  // M1 局部覆盖
  void *iter = NULL;  size_t kLen = 0;
  while ((iter = tSimpleHashIterate(uid2ColMap, iter, &kLen))) {
    int64_t uid = *(int64_t *)tSimpleHashGetKey(iter, &kLen);
    SVTableResolveResult **pp = tSimpleHashGet(pInfo->vtbCache->uid2Result, &uid, sizeof(uid));
    if (pp && *pp) streamVTableResolveResultDestroy(*pp);
    SVTableResolveResult *r = taosMemoryCalloc(1, sizeof(*r));
    r->colMap = *(SSHashObj **)iter;
    r->tagMap = takeTagMap(uid2TagMap, uid);
    tSimpleHashPut(pInfo->vtbCache->uid2Result, &uid, sizeof(uid), &r, POINTER_BYTES);
  }
}
  pInfo->vtbCache->lastCheckMs = taosGetTimestampMs();
pInfo->vtbCache->valid       = true;
// 首次或集合变化时同步 reqColCids/reqTagCids
syncReqCidArrays(pInfo->vtbCache, cids, tagCids);
taosWUnLockLatch(&pInfo->vtbCache->lock);
```

- [x] **Step 5: 删除旧的 getAllVinfo / getSpicificVinfo 调用（如已无其它引用）**

`grep -n "getAllVinfo\|getSpicificVinfo" source/dnode/vnode/src/vnd/vnodeStream.c`，确认仅本入口在用，删除函数体或保留为 deprecated 注释。

- [x] **Step 6: 待用户许可后编译验证**

Run: `cd debug && cmake --build . --target vnode -- -j4`
Expected: 编译通过。

- [x] **Step 7: 待用户许可后 commit**

```bash
git add source/dnode/vnode/src/vnd/vnodeStream.c
git commit -m "feat(stream): rework VTABLE_INFO reader to use chain resolver"
```

---

### Task 10: PSEUDO_COL reader 改造 (T9)

**Files:**
- Modify: `source/dnode/vnode/src/vnd/vnodeStream.c`（`vnodeProcessStreamVTableTagInfoReq` @ 3867）

- [x] **Step 1: 调用函数 A 单 uid 解析**

```c
SArray *singleUid = taosArrayInit(1, sizeof(int64_t));
taosArrayPush(singleUid, &req.virTablePseudoColReq.uid);
SArray *emptyCols = taosArrayInit(0, sizeof(col_id_t));
SArray *tagCids   = req.cids;        // PSEUDO_COL 走 SQL 投影列

SSHashObj *uid2ColMap = NULL, *uid2TagMap = NULL;
code = streamResolveVTableRefChain(pVnode, pInfo, req.ver, singleUid,
                                   emptyCols, tagCids, &uid2ColMap, &uid2TagMap);
taosArrayDestroy(singleUid);
taosArrayDestroy(emptyCols);
if (code != 0) goto _send_err;

SSHashObj **pp = tSimpleHashGet(uid2TagMap, &req.virTablePseudoColReq.uid, sizeof(int64_t));
if (pp == NULL || *pp == NULL) {
  // H2 跳过 → PSEUDO_COL 单 uid 必须成功
  code = TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST;
  goto _send_err;
}
SSHashObj *tagMap = *pp;
// encode tagMap into PSEUDO_COL response（按现有响应结构遍历 tagCids）
```

- [x] **Step 2: 不读不写 cache**

PSEUDO_COL 调用完 `streamResolveVTableRefChain` 后：
- 不查 `pInfo->vtbCache`；
- 编码完响应直接 `streamFreeUid2Map(uid2ColMap, uid2TagMap)`。

- [x] **Step 3: 待用户许可后编译验证 + commit**

Run: `cd debug && cmake --build . --target vnode -- -j4`

```bash
git add source/dnode/vnode/src/vnd/vnodeStream.c
git commit -m "feat(stream): rework PSEUDO_COL reader to use chain resolver"
```

---

### Task 11: 定时检测 hook + diff 算法 (T10)

**Files:**
- Modify: `source/dnode/vnode/src/vnd/vnodeStream.c`（`vnodeProcessStreamWalMetaNewReq` / `vnodeProcessStreamWalMetaDataNewReq` 入口 + 新增 `streamRecheckVTableCache`）
- Modify: `include/libs/new-stream/streamReader.h`（`SStreamVTableInfoCache` 增 `uidSlice + sliceCursor` 字段，v0.6）
- Modify: `source/libs/new-stream/src/streamReader.c`（init/destroy 同步处理新字段，v0.6）

**v0.6 关键参数**（顶部宏）：

```c
#define STREAM_VTB_RECHECK_INTERVAL_MS  1000   // 节流：每秒最多一次 tick
#define STREAM_VTB_RECHECK_SLICE_SIZE   1000   // 单 tick 上限 uid 数
```

- [x] **Step 1: 新增 streamRecheckVTableCache（v0.6 分片化）**

```c
// Returns:
//   0                                — 无 col/tag 变化（changedUids 也可能为空）
//   TSDB_CODE_STREAM_VTB_TAG_CHANGED — tag 变化（caller 要把 rsp.code 设此值）
//   其它非零                          — 技术性错误
//
// 出参 changedUids: SArray<int64_t>，col 链式终点变化的 uid 列表
static int32_t streamRecheckVTableCache(SVnode *pVnode, SStreamTriggerReaderInfo *pInfo,
                                        int64_t walVer, SArray *changedUids);
```

实现按 spec §5.6.3（v0.6 分片轮转）：
1. `sliceCursor == 0` → 重建 `uidSlice = qStreamGetTableArrayList(pInfo)` 中所有非 `markedDeleted` 的 uid 快照（**不是 `uid2Result.keys()`**：让 cache-miss 未写入的新 vtable 也进入检测，drop 的随 markedDeleted 过滤掉）；
2. 取 `[cursor, min(cursor + SLICE_SIZE, total))` 切片传给 `streamResolveVTableRefChain`（第 5 参 vtbUids = sliceUids）；
3. for each uid in slice（per-uid diff，M1 局部更新）：
   - `newRes == NULL`（H2 跳过的 drop 顶层）→ 从 `uid2Result` 删除该 uid；
   - tag diff（type/nLen/memcmp）→ 任一不同立刻 return TAG_CHANGED；
   - col diff（hasRef + 三元组 strcmp）→ uid 入 changedUids；
   - per-uid 替换：`tSimpleHashPut(uid2Result, uid, newRes)` 成功后 `*ppNew = NULL` 转移所有权，旧 entry destroy（oldRes == NULL 即首次写入，等同 cache-miss 路径）；
4. `sliceCursor = (end >= total) ? 0 : end`；
5. return 0（changedUids 已填）。

**语义对比 v0.5**：原 C2a 原子全量替换 → v0.6 M1 per-uid 替换。被 H2 跳过的 drop uid 在本片同步从 cache 删除，与全量替换语义等价但按 uid 粒度推进。

- [x] **Step 2: 在两个 WAL meta 入口加节流 hook**

```c
{
  int64_t now = taosGetTimestampMs();
  if (pInfo->vtbCache && pInfo->vtbCache->valid &&
      now - pInfo->vtbCache->lastCheckMs >= STREAM_VTB_RECHECK_INTERVAL_MS) {
    taosWLockLatch(&pInfo->vtbCache->lock);
    if (now - pInfo->vtbCache->lastCheckMs >= STREAM_VTB_RECHECK_INTERVAL_MS) {
      SArray *changedUids = taosArrayInit(0, sizeof(int64_t));
      int32_t rc = streamRecheckVTableCache(pVnode, pInfo, walVer, changedUids);
      pInfo->vtbCache->lastCheckMs = taosGetTimestampMs();
      taosWUnLockLatch(&pInfo->vtbCache->lock);
      if (rc == TSDB_CODE_STREAM_VTB_TAG_CHANGED) {
        // 直接在响应里把 code 设为该值并提前返回
        ...
        taosArrayDestroy(changedUids);
        return rc;
      } else if (rc != 0) {
        taosArrayDestroy(changedUids);
        return rc;  // 技术性错误透传
      }
      if (taosArrayGetSize(changedUids) > 0) {
        // 通过既有 IS_PATCHING_VITRUAL_TABLE 路径，把 changedUids 装进 TABLE_BLOCK_ADD
        appendChangedUidsToWalMetaRsp(pRsp, changedUids);
      }
      taosArrayDestroy(changedUids);
    } else {
      taosWUnLockLatch(&pInfo->vtbCache->lock);
    }
  }
}
// continue with normal WAL meta processing
```

`appendChangedUidsToWalMetaRsp` 复用现有 `IS_PATCHING_VITRUAL_TABLE` 编码 helper（grep 找）。

- [x] **Step 3: 待用户许可后编译验证 + commit**

Run: `cd debug && cmake --build . --target vnode -- -j4`

```bash
git add source/dnode/vnode/src/vnd/vnodeStream.c
git commit -m "feat(stream): timed re-check hook + diff for vtable chain cache"
```

---

### Task 12: 端到端测试 TC01–TC13 (T11a)

**Files:**
- Create: `test/cases/18-StreamProcessing/02-Stream/stream_vtable_chain_ref.py`

- [x] **Step 1: 编写测试类骨架**

```python
import time
from new_test_framework.utils import (tdLog, tdSql, tdStream, StreamCheckItem, waitForRows)


class TestStreamVtableChainRef:
    """End-to-end tests for vtable chain-ref resolution in stream processing.

    Covers TC01–TC13 from the design spec.
    """
    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_chain_ref(self):
        """TC01–TC13 — vtable chain-ref end-to-end.

        Since: v3.4.1.0
        Labels: common, ci
        """
        try:
            tdStream.createSnode()
            self._tc01_one_hop()
            self._tc02_three_hop_same_vg()
            self._tc03_three_hop_cross_vg()
            self._tc04_partition_by_tag()
            self._tc05_tag_changed_fatal()
            self._tc06_col_terminal_changed_patch()
            self._tc07_ref_table_not_exist()
            self._tc08_ref_col_not_exist()
            self._tc09_chain_too_deep()
        finally:
            tdStream.dropAllStreamsAndDbs()
```

- [x] **Step 2: 实现 TC01 — 单跳 ref**

每个 `_tcXX_*` 方法独立创建 db / 物理 stable / vtable 引用 / stream，断言：
```python
def _tc01_one_hop(self):
    tdSql.execute("create database tc01")
    tdSql.execute("use tc01")
    tdSql.execute("create stable st (ts timestamp, v int) tags (g int)")
    tdSql.execute("create table ct1 using st tags (1)")
    tdSql.execute("create vtable vt (ts timestamp, v int from ct1.v)")
    tdSql.execute("create stream s1 trigger sliding(1s) into res as select count(*) from vt")
    tdSql.execute("insert into ct1 values (now, 1)")
    waitForRows("select * from res", 1, timeout=20)
    tdSql.checkData(0, 0, 1)
```

- [x] **Step 3: 实现 TC02 — 3 跳同 vg**

链式：vt3 → vt2 → vt1 → ct1.v；同库，期望 1 次 db 路由 + 多次 RPC（同 vg）。断言数据回灌正确。

- [x] **Step 4: 实现 TC03 — 跨 vnode 3 跳**

每跳目标在不同 vgroup（建库时 `vgroups 3` + tag-based child table 分布到不同 vg）。

- [x] **Step 5: 实现 TC04 — partition by tag**

```python
tdSql.execute("create stream s4 trigger sliding(1s) ... from vt partition by g_tag ...")
# 断言 PSEUDO_COL 查询拿到的 g_tag 值与底层 child table tag 一致
```

- [x] **Step 6: 实现 TC05 — tag 变化触发失败终态**

修改链路终点 child table 的 tag → 等待 ≥10s → 检测 stream status = 'failed'，错误码包含 `TSDB_CODE_STREAM_VTB_TAG_CHANGED`。

- [x] **Step 7: 实现 TC06 — col 链式终点变化触发 patch**

drop+recreate vtable 中间节点，把 ref 列从 `ct1.v` 改到 `ct2.v` → 等待 ≥10s → stream 仍 running，新数据按新 ref 回灌。

- [x] **Step 8: 实现 TC07 / TC08 / TC09**

```python
def _tc07_ref_table_not_exist(self):
    # 创建一个 ref 不存在表的 vtable（或先建 vtable 再 drop ref 表）
    # 断言 stream 失败错误码包含 STREAM_VTB_REF_TABLE_NOT_EXIST 字样
def _tc08_ref_col_not_exist(self):
    ...
def _tc09_chain_too_deep(self):
    # 构造 33 跳链，断言失败错误 STREAM_VTB_REF_TOO_DEEP
```

- [x] **Step 9: 待用户许可后跑测试**

Run: `cd test && python ./new_test_framework/utils/test_runner.py -p 02-Stream/stream_vtable_chain_ref.py`
Expected: 全部 PASS。

- [x] **Step 10: 待用户许可后 commit**

```bash
git add test/cases/18-StreamProcessing/02-Stream/stream_vtable_chain_ref.py
git commit -m "test(stream): vtable chain-ref end-to-end TC01-TC13"
```

---

## 自审清单（写完 plan 后，开干前的最后一道）

| 检查项 | 结果 |
|---|---|
| spec § 5.1 数据结构 | ✅ Task 2 |
| spec § 5.2 函数 A 批量编排器 | ✅ Task 6 / 7 / 8 |
| spec § 5.3 新消息 + 函数 B | ✅ Task 3 / 4 / 5 |
| spec § 5.4.1 trigger 端无改动 | ✅ 无任务（spec 已显式约束） |
| spec § 5.4.2 reader 入口 + 响应只编 col | ✅ Task 9 |
| spec § 5.5 PSEUDO_COL（不读不写 cache） | ✅ Task 10 |
| spec § 5.6 定时检测 + diff | ✅ Task 11 |
| spec § 5.7 错误码 | ✅ Task 1 |
| spec § 5.8 错误处理表 | ✅ 已分散到 Task 6 (H2/I3/J1) + Task 9 (响应) + Task 11 (TAG_CHANGED) |
| spec § 9 测试 TC01-TC13 | ✅ Task 12 (TC01-TC13)，原 TC10-TC18 永久移除后改按调用顺序连续重编 |
| 函数签名一致性（A、B、辅助） | ✅ 全文用同一 typedef |
| 是否有 TBD / 未定义类型 | ✅ 已自查（前向声明的 `streamFreeUid2Map`/`destroyUid2Result`/`takeTagMap`/`appendChangedUidsToWalMetaRsp`/`syncReqCidArrays` 在 Task 6/9/11 内文有上下文，实施时一并加） |

> 备注：Task 6/9/11 内文出现的内部 helper（`streamFreeUid2Map`、`destroyUid2Result`、`takeTagMap`、`appendChangedUidsToWalMetaRsp`、`syncReqCidArrays`）为同文件内 `static` helper，实施时随主函数一并写出，不另列任务避免破碎化；其语义在调用处的注释里已明确。

---

## 风险与注意事项（开发期）

1. **同步阻塞**：A 函数最坏 32 跳 × N 个 vg 同步 RPC，定时检测 ≥10s 节流是关键护栏。如压测出现尾延迟，考虑将 RPC 改成并发投递（保留同步语义但批量 epoll 等待）—— 不在本期范围。
2. **fail-fast 原则**：所有 helper 失败一律 `return non-zero`，禁止 fallback / 默认值 / silent skip（H2 是显式合规的"业务跳过"，由调用方记录 warning，不是隐式 fallback）。
3. **cache 锁粒度**：`SRWLatch` 选用读写锁；Task 11 的定时检测必须用 `taosWLockLatch`（替换整 cache）。Task 9 写 cache 也是写锁，避免 reader 端其它入口并发读到半成品。

---

## v0.4 联调补丁（Task 14：传输层修正）

完成 Task 1-12 端到端联调后发现 4 处必须补的工程细节，统一收敛在 Task 14：

- **14.1 vmHandle 注册**：`source/dnode/mgmt/mgmt_vnode/src/vmHandle.c::dmSetMgmtHandle` 增 `dmSetMgmtHandle(pArray, TDMT_VND_VTABLE_REF_RESOLVE, vmPutMsgToFetchQueue, 0)`。缺此注册 → dnode 派发层默认丢弃，与 vnode 端注册无关。
- **14.2 SMsgHead 前置**：sender (`streamSendOneVgResolveRpc`) 必须在 body 前 `prepend SMsgHead{ htonl(vgId), htonl(contLen) }`；receiver (`vnodeProcessVTableRefResolveReq`) 反序列化前必须 `POINTER_SHIFT(pCont, sizeof(SMsgHead))`。否则 `vmPutMsgToQueue` 用脏 vgId acquire vnode → "Vnode is closed or removed"。
- **14.3 GET_DB_INFO**：`streamFetchDbVgInfo` 用 `TDMT_MND_GET_DB_INFO` 而非 `TDMT_MND_USE_DB`（mnode 端 handler 共用 `mndProcessUseDbReq`、req/rsp 等价，但语义层面 GET_DB_INFO 给 catalog/内部刷新路由，与 catalog `CTG_TASK_GET_DB_INFO` 一致）。
- **14.4 async 化**：`streamFetchDbVgInfo` 替换 `rpcSendRecv` 为 `asyncSendMsgToServer + tsem_wait`，参考 `source/libs/executor/src/dataInserter.c::buildDbVgInfoMap` (561-690)。callback 内 deserialize `SUseDbRsp`、free `pMsg->pData`/`pEpSet`、`tsem_post`；payload 用 `taosMemoryCalloc`（async 路径走 `taosMemoryFree`，不能 `rpcMallocCont`）。

实施验收：
- `./build.sh bld` 通过；
- 端到端 `stream_vtable_chain_ref.py` TC01 跨 vnode 链路解析能拉到响应（不再卡 "Vnode is closed or removed"）；
- 调试日志 `streamRouteTableToVg` 应能打印出 `fullName / vgId / epSet.inUse / numOfEps + 每个 ep fqdn:port`。

> 注：Task 14 不新建 plan，所有改动落在已存在的 `vnodeStream.c` + `vmHandle.c` 两文件，commit `15126426056` 已落盘。
4. **测试稳定性**：CI 上 vtable chain-ref 用例依赖 vnode debugFlag 输出，必要时新增 stream-vtable-cache 专用 debug 子项。
