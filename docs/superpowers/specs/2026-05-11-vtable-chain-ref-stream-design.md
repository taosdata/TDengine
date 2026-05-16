# 虚拟表链式引用 - 流计算适配设计文档

| 项 | 内容 |
|---|---|
| 主题 | 虚拟表 (vtable) 链式引用在新流 (new-stream) 的支持 |
| 飞书需求 | https://taosdata.feishu.cn/wiki/F889wfBIXiYfbSk8LJ5cP0CZneh |
| 创建日期 | 2026-05-11 |
| 文档类型 | 详细设计 |
| 关联模块 | `source/dnode/vnode/src/vnd/vnodeStream.c`、`source/libs/new-stream/`、`source/dnode/mnode/impl/src/mndDb.c` |

## 1. 修订记录

| 版本 | 日期 | 修订人 | 修订内容 |
|---|---|---|---|
| v0.1 | 2026-05-11 | — | 初稿，覆盖 brainstorming 阶段确认的 Q1–Q8 |
| v0.2 | 2026-05-11 | — | 按"批量 uid + cache 全量/局部"决策（Q1~Q6/H/I/J/K/L/M）改造 A 函数为批量、重写 §5.4.2/§5.5/§5.6.3/§5.8，新增附录 A 决策表 |
| v0.3 | 2026-05-11 | — | 按代码实现回灌：cache 字段 `lastCheckNs`→`lastCheckMs`（10s 节流用 ms 即可）；函数 A 签名合并 col/tag 为单 `SVTableResolveResult` map 并把 `SStreamVTableInfoCache` 上提为独立可空参数（PSEUDO_COL 传 NULL → cache-bypass）；RPC 重试简化为单次失败即整组 uid 标 skipped（依赖 ≥10s 节流自带重试）；响应字段把 `STagValue` 平铺成 `tagType/tagLen/tagData` 解耦；删除函数 B "可选 N 跳本地展开"段落（实际严格单跳）；§5.4.2 增 `isVtableOnlyTs` 快速路径与 `streamFillVTableInfoFromResolved` 重建步骤；§5.5 PSEUDO_COL 增 NORMAL vtable 分支说明；§10 任务表对齐 plan 13 task |
| v0.4 | 2026-05-13 | — | 端到端联调回灌：1) `TDMT_VND_VTABLE_REF_RESOLVE` 必须在 `vmHandle.c::dmSetMgmtHandle` 注册到 fetch queue（与 `tmsgdef.h` + `vnodeSvr.c` switch 三处必须同步）；2) sender 构造请求时必须前置 `SMsgHead` 头（含 `htonl(vgId)` 与 `htonl(contLen)`），dispatcher `vmPutMsgToQueue` 强制按头部 8 字节 ntohl 出 vgId 路由 vnode，缺头或字节序错会被误判为 "Vnode is closed or removed"；receiver `vnodeProcessVTableRefResolveReq` 反序列化前必须 `POINTER_SHIFT(pCont, sizeof(SMsgHead))`；3) mnode 路由刷新必须用 `TDMT_MND_GET_DB_INFO` 而不是 `TDMT_MND_USE_DB`（mnode 端二者复用同一 handler `mndProcessUseDbReq`，req/rsp 等价，但语义上 USE_DB 给客户端 USE 命令、GET_DB_INFO 给 catalog/vnode 内部刷新路由，与 catalog `CTG_TASK_GET_DB_INFO` 一致）；4) `streamFetchDbVgInfo` 改为 `asyncSendMsgToServer` + `tsem_wait` 同步等待，不用 `rpcSendRecv`，与 `dataInserter.c::buildDbVgInfoMap` 模式对齐 |
| v0.5 | 2026-05-15 | — | **H2 收紧 + I3' 收紧**：原 H2 "单 uid 业务错误 warning + continue" 改为"严格上报"——仅"顶层 uid 在本地 meta 中不存在（vtable 被并发 drop，metaGetTableEntryByUid NOT_EXIST）"做 warning + skip 容错；其余任何错误（中间链路业务错误码 REF_TABLE_NOT_EXIST/REF_COL_NOT_EXIST/TAG_CHANGED、超 32 跳、单次 RPC 失败、技术错误）一律 A 函数立即返回 errCode，由调用方 reader 透传到 VTABLE_INFO/WAL_META_NEW 响应 rsp.code，trigger 进入失败终态触发 mnode redeploy 自然完成"重试"。原 I3' "RPC 失败按 H2 跳过 + 下一次定时检测重试" 同步收紧为"RPC 失败直接上报、不重试"。同步更新 §5.2/§5.4.2/§5.5/§5.6.3/§5.8、TC12/TC15、附录决策表 H/I/J |
| v0.6 | 2026-05-16 | — | **定时检测分片化**：节流间隔 10s → 1s，单次扫描上限 = `STREAM_VTB_RECHECK_SLICE_SIZE = 1000` 个 uid。`SStreamVTableInfoCache` 新增 `uidSlice + sliceCursor`：每轮起点重建 uid 快照，按游标顺序切片，扫完回绕。diff 从"全量遍历 + C2a 原子全量替换" 改为"切片遍历 + per-uid 替换 (M1)"，被 H2 跳过的顶层 uid 在本片同步从 cache 删除。收益：N ≤ 1000 时 1s 内全量检测；N=10000 时 10s 一轮但单 tick 不再阻塞读路径，最长 tail 延迟与 uid 总数解耦 |

## 2. 背景与目标

### 2.1 背景

虚拟表 (vtable) 当前已支持 `CREATE VTABLE ... USING ... TAGS(...)` 语法，并允许列与 tag 通过 `SColRef`（hasRef + refDbName + refTableName + refColName）链式引用其他表的列或 tag。

但**新流 (new-stream)** 在 vnode 端处理 vtable 时，仅支持**单跳**展开（即 vtable 的列直接引用一个物理表的列）。当链式深度 > 1（vtable A 引用 vtable B 的列，B 再引用 vtable C 或物理表 X 的列）时存在以下缺口：

1. **链式追踪缺失**：`vnodeProcessStreamVTableInfoReq` 只能解析直接 ref，不能跨 vnode 多跳追踪到链式终点（最终的物理表 stable+uid+cid 或 tag 值）；
2. **跨 vnode 编排缺失**：链式中间节点位于不同 vnode 时，没有现成机制让最初 vnode 与中间 vnode 协作把 ref 链解到底；
3. **链式终值变化感知缺失**：链式中任意一环的 child table 的实际 tag 取值变了、或 vtable 的 ref 关系变了，trigger 现有的 patch 机制无法感知；
4. **PSEUDO_COL 链式缺失**：`STRIGGER_PULL_VTABLE_PSEUDO_COL` 当前也只取直接 ref 的 tag 值，未做链式追踪。

### 2.2 目标

| 目标 | 度量 |
|---|---|
| G1 | `STRIGGER_PULL_VTABLE_INFO` 支持任意深度（≤ 32 跳）跨 vnode 链式追踪，返回最终物理表的 (suid, uid, cid) |
| G2 | `STRIGGER_PULL_VTABLE_PSEUDO_COL` 支持任意深度链式追踪，返回最终 tag 值 |
| G3 | reader vnode 在每次新数据到达时，按 ≥ 10s 节流周期校验缓存的链式终点；变化时通过既有 patch / RPC 错误码通知 trigger 处理 |
| G4 | 新增的链式追踪逻辑封装为公共函数，VTABLE_INFO / PSEUDO_COL / 定时校验 三处复用同一段代码 |

### 2.3 非目标

- 不改变 vtable 的 SQL 语法和元数据结构（`SColRef` 不动）；
- 不优化跨 vnode RPC（同步阻塞实现，最坏 32 跳 × N 个 db 的 RPC 串行延迟，依赖定时器节流避免热路径过多触发）；
- 不为 mnode 引入 vtable 链式 cache（mnode 端无任何改动，除复用既有 `TDMT_MND_GET_DB_INFO`）。

## 3. 术语

| 术语 | 含义 |
|---|---|
| 最初 vnode | trigger 直连的、持有原始 vtable 元数据的 reader vnode；链式追踪的**编排者** |
| 中间 vnode | 链式追踪过程中被最初 vnode 临时调用的远端 vnode；**无状态单跳解析器** |
| 链式终点 | 链式追踪展开到的最终目标——要么是物理表 (normal/child) 的 (suid, uid, cid)（列场景），要么是物理表 child table 上的 tag 实际取值（tag 场景） |
| ref 三元组 | `(refDbName, refTableName, refColName)`，参考 `SColRef` 字段 |
| VTABLE_INFO | `STRIGGER_PULL_VTABLE_INFO`，trigger 向 reader 拉取 vtable 列的展开结果 |
| PSEUDO_COL | `STRIGGER_PULL_VTABLE_PSEUDO_COL`，trigger 向 reader 拉取 vtable 上的伪列 / tag 取值 |
| OTABLE_INFO | `STRIGGER_PULL_OTABLE_INFO`，trigger 向 reader 拉取 normal/child table 信息（**本设计后已不再支持 vtable**） |

## 4. 总体方案

### 4.1 核心思想

```
                  +----------+
                  | trigger  |
                  +----+-----+
                       |  VTABLE_INFO / PSEUDO_COL
                       v
            +----------+-----------+
            |    最初 vnode (V0)   |  <- 持有原始 vtable + 编排器
            +----------+-----------+
                       | 循环：根据 hasRef 找下一跳
                       |
            +----------+----------+----------+
            |                     |          |
   TDMT_MND_GET_DB_INFO   TDMT_VND_VTABLE_REF_RESOLVE (sync)
            |                     |          |
            v                     v          v
        +-------+         +--------+    +--------+
        | mnode |         | 中间 V1|    | 中间 V2|
        +-------+         +--------+    +--------+
                          单跳解析     单跳解析
```

**两个角色**：

- **最初 vnode 是编排者**：持有完整状态（"还有哪些 col/tag 没解到底"），按目标 db/vnode 分组打包，循环调用直至所有项 terminated 或超过 32 层。
- **中间 vnode 是无状态单跳解析器**：本地查 meta，能本地解到底就到底（优化），不能就把当前 hasRef 状态原样回给最初 vnode。**中间 vnode 不需要 stream task 存在**——这是新消息走 `TDMT_VND_*` 通用命名空间而非 `STRIGGER_PULL_*` 的根本原因。

### 4.2 关键设计决策

| # | 决策 | 选择 | 理由 |
|---|---|---|---|
| D1 | 跨 vnode 递归编排者 | 最初 vnode 集中编排 | 状态集中，易于实现 32 层深度限制和错误回滚 |
| D2 | 远端 vnode 通信消息 | 新增 `TDMT_VND_VTABLE_REF_RESOLVE`（vnode 通用消息，**不在 stream 命名空间**） | 中间 vnode 上不一定有 stream task，不能依赖 stream 模块 |
| D3 | reader 缓存挂载点 | `SStreamTriggerReaderInfo`（per-stream） | 每个 stream 的 partitionCols / 监听 vtable 集合不同，必须隔离 |
| D4 | tag 链式终值表示 | 直接存 `STagValue {type, nLen, pData}` | tag 链式终点是 child table 实际 tag 值，无 cid 概念；CHILD vtable 上 tag 也可以是常量（!hasRef），常量值由首跳 vnode 直接读 vchild 自身 STag 写入 |
| D5 | column 链式终值表示 | 存 `SColResolveItem{hasRef, refDbName, refTableName, refColName}` 风格的三元组 | 与 `SColRef` 风格一致；**虚拟子表的 column 必须是引用**，hasRef=false 仅表示链式中显式声明的 NULL 占位（合法但罕见） |
| D6 | tag 变化通知方式 | RPC `rsp.code = TSDB_CODE_STREAM_VTB_TAG_CHANGED` | partition 划分依赖 tag，tag 变即流终态，不可恢复 |
| D7 | column 变化通知方式 | reader 把 changedUids 装进 `TABLE_BLOCK_ADD` 塞进 `WAL_META_NEW` 响应 | 复用既有 `IS_PATCHING_VITRUAL_TABLE` patch 流程 |
| D8 | PSEUDO_COL 是否读 cache | **不读 cache** | PSEUDO_COL 的 cid 集合可能与 cache 不一致；每次走链式拿最新值更安全 |
| D9 | 定时检测节流 | `≥ 10s`，挂 `vnodeProcessStreamWalMetaNewReq` / `vnodeProcessStreamWalMetaDataNewReq` 入口 | 避免每条 WAL meta 都触发链式 RPC |
| D10 | 下一跳 epset 获取 | 复用 `TDMT_MND_GET_DB_INFO` + 本地 db→vgInfo cache | 已有同构先例（sysscanoperator.c VTABLE VALIDATE） |
| D11 | 链式深度上限 | 32 跳 | 与 `TSDB_CODE_STREAM_VTB_REF_TOO_DEEP` 既有约定一致 |

## 5. 详细设计

### 5.1 数据结构

#### 5.1.1 链式终点表示

```c
// Resolved column reference item.
// kind=COL: the chain ends at a physical table column.
// kind=TAG: the chain ends at a child table tag value.
typedef struct SColResolveItem {
  bool    hasRef;                              // false means NULL (no reference)
  char    refDbName   [TSDB_DB_NAME_LEN];      // reuse SColRef field name style
  char    refTableName[TSDB_TABLE_NAME_LEN];
  char    refColName  [TSDB_COL_NAME_LEN];     // physical column name on the terminal table
} SColResolveItem;

typedef struct STagValue {
  int8_t   type;                               // TSDB_DATA_TYPE_*
  int32_t  nLen;
  char    *pData;                              // owned, freed when STagValue is freed
} STagValue;
```

**字段说明**：

- `SColResolveItem` 用于 column 链式终点。**虚拟子表（VTABLE）的 column 必须是引用**——`hasRef = false` 仅出现在链式中间某一环显式声明无引用（NULL 占位列）的合法情况。
- `STagValue` 用于 tag 链式终点。**虚拟子表（CHILD vtable）的 tag 既可以是引用，也可以是常量**：
  - tag 是引用（`SColRef.hasRef = true`）：链式追踪到底层 child table 的 tag 实际值，写入 `STagValue`。
  - tag 是常量（`SColRef.hasRef = false`）：由首跳 vnode 在准备阶段直接读取 vchild 自身 `STag` 中的常量 tag 值（`streamReadChildTagConstValue`），不发起链式 RPC，直接写 terminal `STagValue`；NORMAL vtable 没有 tag 概念，遇到该路径直接报 `STREAM_VTB_REF_COL_NOT_EXIST`。
- `pData` 由 cache 持有，diff 比较用 `(type, nLen, memcmp(pData))` 三元组。

#### 5.1.2 vtable 链式解析结果（per virtual uid）

```c
typedef struct SVTableResolveResult {
  SSHashObj *colMap;   // key: virtual col cid (col_id_t),     value: SColResolveItem
  SSHashObj *tagMap;   // key: virtual tag cid (col_id_t),     value: STagValue
} SVTableResolveResult;
```

**说明**：

- `colMap` 存一个 vtable 上每个被 stream 关心的 column cid 的链式终点（最终物理表 + 列名）；
- `tagMap` 存一个 vtable 上每个被 stream 关心的 tag cid 的最终 tag **取值**（不是 ref，是实际 child table tag 数据）。

#### 5.1.3 reader 端 cache（per-stream）

```c
typedef struct SStreamVTableInfoCache {
  SRWLatch    lock;
  SArray     *reqColCids;     // SArray<col_id_t>: column cids the trigger asked for
  SArray     *reqTagCids;     // SArray<col_id_t>: tag    cids in partitionCols (NEW)
  SSHashObj  *uid2Result;     // key: virtual table uid, value: SVTableResolveResult
  SHashObj   *dbVgInfo;       // key: dbFName,           value: SUseDbRsp (vgInfo cache)
  int64_t     lastCheckMs;    // ms timestamp; throttle the timed re-check (>= STREAM_VTB_RECHECK_INTERVAL_MS)
  // Sliced re-check cursor: every tick scans uidSlice[sliceCursor : sliceCursor+SLICE_SIZE].
  // When cursor wraps to 0, uidSlice is rebuilt from current uid2Result keys to pick up
  // newly-added uids from the cache-miss path.
  SArray     *uidSlice;       // SArray<int64_t>: snapshot of uids to scan in this sweep
  int32_t     sliceCursor;    // next index into uidSlice; 0 triggers rebuild
  bool        valid;          // false until the first VTABLE_INFO populates it
} SStreamVTableInfoCache;
```

> **节流 + 分片参数（v0.6）**：`STREAM_VTB_RECHECK_INTERVAL_MS = 1000`、`STREAM_VTB_RECHECK_SLICE_SIZE = 1000`。一次 tick 最多检测 1000 个 uid，全量扫描周期 ≈ `ceil(N / 1000) × 1s`。N ≤ 1000 时 1s 内全量；N = 10000 时 10s 完成一轮。对比 v0.5 的"10s 全量扫描"，单次 RPC 数量上限固定、读路径不再被长尾阻塞，且变化感知延迟从最坏 10s 降到 1s（每个 uid 最快 1s 内被检测一次的下界）。

挂载点 — 在 `include/libs/new-stream/streamReader.h` 的 `SStreamTriggerReaderInfo` 结构体（per-stream）中新增字段：

```c
typedef struct SStreamTriggerReaderInfo {
  // ... existing fields ...
  SStreamVTableInfoCache vtbCache;   // NEW: per-stream chain-resolve cache
} SStreamTriggerReaderInfo;
```

### 5.2 公共函数 A：链式追踪编排器（最初 vnode 侧，批量）

```c
// Resolve chain terminals for a batch of virtual tables on the local vnode.
//
// Caller owns *ppUid2Result on success and must free it via the per-result
// destroy helper. On any failure the function cleans up its own intermediate
// allocations and returns a non-zero code.
//
// Parameters:
//   pVnode          - current (originating) vnode
//   pCache          - per-stream chain-resolve cache used as dbVgInfo cache;
//                     pass NULL to bypass cache (e.g. PSEUDO_COL path)
//   pReaderInfo     - per-stream reader info (storage API + ST_TASK_DLOG)
//   ver             - WAL version at which the resolution is anchored (advisory)
//   vtbUids         - SArray<int64_t>: virtual table uids on the local vnode to resolve
//                     (NULL or empty => all uids from qStreamGetTableArrayList)
//   virtColCids     - SArray<col_id_t>: column cids shared by every uid (Q1=A1)
//   virtTagCids     - SArray<col_id_t>: tag    cids shared by every uid (Q1=A1)
//   ppUid2Result [out] - SSHashObj< uid -> SVTableResolveResult* >
//                        SVTableResolveResult bundles {colMap, tagMap} for that uid
//                        (single map keeps cache writes atomic per uid).
//
// Per-uid failure semantics (H2, v0.5 strict):
//   - Top-level uid missing in local meta (vtable was concurrently dropped,
//     i.e. metaGetTableEntryByUid returns NOT_EXIST while building the
//     work-list): log a warning and skip that uid; *ppUid2Result will not
//     contain an entry for it (J1). Function still returns success.
//   - ANY OTHER error along the chain (invalid ref triplet on a vchild,
//     business error from RPC response such as REF_TABLE_NOT_EXIST /
//     REF_COL_NOT_EXIST / TAG_CHANGED, chain deeper than 32, RPC transport
//     failure, OOM, encoding error, mnode unreachable, ...) is propagated
//     up as the function return code; the partially filled *ppUid2Result
//     is released by the caller.
int32_t streamResolveVTableRefChain(SVnode                   *pVnode,
                                    SStreamVTableInfoCache   *pCache,
                                    SStreamTriggerReaderInfo *pReaderInfo,
                                    int64_t                    ver,
                                    SArray                    *vtbUids,
                                    SArray                    *virtColCids,
                                    SArray                    *virtTagCids,
                                    SSHashObj                **ppUid2Result);
```

**实现要点**：

1. **uid 来源**（参考 `getAllVinfo` @ vnodeStream.c:3689）：
   - `vtbUids == NULL || size == 0` → 调 `qStreamGetTableArrayList(pInfo)` 取全集，遍历跳过 `markedDeleted` 的项，组成内部 uid 集；
   - 否则使用调用方传入的 `vtbUids`。
2. **work-list 初始化**：对每个 uid，加锁 metaReader 加载 vtable meta，按 `virtColCids` / `virtTagCids` 展开，每个未终结项产生一个 `SResolveWorkItem`：
   ```c
   typedef struct {
     int64_t  originVtbUid;     // which vtable this item belongs to (for归位)
     int32_t  originCid;        // which cid on origin vtable
     bool     isTag;            // false=col, true=tag
     char     refDbName   [TSDB_DB_NAME_LEN];
     char     refTableName[TSDB_TABLE_NAME_LEN];
     char     refColName  [TSDB_COL_NAME_LEN];
   } SResolveWorkItem;
   ```
   单 uid 在准备 work-list 阶段：
   - 若该 uid 在本地 meta 中不存在（`metaGetTableEntryByUid` 返回 NOT_EXIST，通常是并发 drop）→ 按 H2 仅 warning + skip 该 uid，继续处理其他 uid，函数仍返回 0；
   - 若 ref 三元组不合法（例如 hasRef=true 但 refTableName 空，或 vchild 元数据自身损坏）→ A 函数立即返回 `TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST` / `TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST`，整轮失败。
3. **主循环（最多 32 轮，跨 uid 合并 RPC，Q4=G1）**：
   - 把当轮全部 work-item 按 `refDbName` 查 `pCache->dbVgInfo`（pCache=NULL 时每次 miss；非 NULL 且 miss → 同步 `TDMT_MND_GET_DB_INFO` 拉一次 db→vgInfo 写 cache）；
   - 再按 `vgHashFunc(refTableName) → vgId` 聚合，每个 vgId 一次 `TDMT_VND_VTABLE_REF_RESOLVE` 同步 RPC（**即使目标 vgId 是本 vnode 也走 RPC**，统一路径）；
   - 单次 RPC 失败（含 OOM / 序列化错误 / 通信超时 / 目标 vnode 不可达）→ A 函数立即返回该 RPC 的 errCode，整轮失败；不再做内部重试，也不再做"按 uid 跳过"。重试责任完全下放到上层 ≥10s 节流定时检测——下一次窗口到达自然重试。
   - 收回响应：每个响应项按 `originVtbUid + originCid` 归位 → terminated 项写入 `result.colMap[originCid]` / `result.tagMap[originCid]`（其中 result 就是 `(*ppUid2Result)[originVtbUid]`），未 terminated 项更新 ref 三元组进入下一轮。
4. **业务错误一律上报（H2 收紧版）**：响应里任一 item 带业务错误码（REF_TABLE_NOT_EXIST / REF_COL_NOT_EXIST / TAG_CHANGED）或任一 uid 跨度超过 32 跳 → A 函数立即返回该错误码，整轮失败；已部分填充的 `*ppUid2Result` 由调用方释放。
5. 循环退出：work-list 为空（结束）→ 返回 0；任何错误（业务 / RPC / 技术）→ 立即返回非零；整轮无进展（视为 32 跳上限触发）→ 返回 `TSDB_CODE_STREAM_VTB_REF_TOO_DEEP`。
6. 函数返回 0 仅当 work-list 全部跑完且无任何错误（顶层 uid 不存在按 H2 跳过不计入错误）。

### 5.3 新消息 `TDMT_VND_VTABLE_REF_RESOLVE`（vnode 通用，不在 stream 命名空间）

#### 5.3.1 消息定义

`include/common/tmsgdef.h`：

```c
// Add under VND group, NOT under MND or stream group.
TD_DEF_MSG_TYPE(TDMT_VND_VTABLE_REF_RESOLVE, "vtable-ref-resolve", NULL, NULL)
```

派发：`source/dnode/vnode/src/vnd/vnodeSvr.c` 的 **fetch 消息表**（`vnodeProcessFetchMsg`）。该消息纯只读、仅访问本地 meta，不走 sync raft 路径。

#### 5.3.2 请求 / 响应结构

```c
typedef enum {
  STREAM_VREF_KIND_COL = 1,
  STREAM_VREF_KIND_TAG = 2,
} EStreamVRefKind;

typedef struct SVTableRefResolveItem {
  int8_t          kind;            // EStreamVRefKind
  bool            hasRef;          // true: triple below is valid; false: terminal-empty marker
  char            refDbName   [TSDB_DB_NAME_LEN];
  char            refTableName[TSDB_TABLE_NAME_LEN];
  char            refColName  [TSDB_COL_NAME_LEN];
} SVTableRefResolveItem;

typedef struct SVTableRefResolveReq {
  int64_t  ver;                   // anchor version (caller's, advisory)
  SArray  *items;                 // SArray<SVTableRefResolveItem>
} SVTableRefResolveReq;

typedef struct SVTableRefResolveRspItem {
  int32_t                code;          // 0 if resolved (terminated or next-hop returned)
  bool                   terminated;    // true: chain ended here
  SVTableRefResolveItem  nextRef;       // dual purpose:
                                        //   terminated=false → next hop's ref triple (hasRef=true)
                                        //   terminated=true  && kind=COL → terminal physical (db,tbl,col)
                                        //   terminated=true  && kind=TAG → triple unused (hasRef=false)
  // tag value carried separately to keep wire format independent of streamInt.h:
  int8_t   tagType;               // valid when terminated=true && kind=TAG
  int32_t  tagLen;                // 0 when not applicable
  char    *tagData;               // owned by recv side (free in tFreeSVTableRefResolveRsp)
} SVTableRefResolveRspItem;

typedef struct SVTableRefResolveRsp {
  SArray *items;                  // SArray<SVTableRefResolveRspItem>, same order as req
} SVTableRefResolveRsp;
```

#### 5.3.3 处理函数 B（中间 vnode 侧）

```c
// Single-hop resolver. Looks up each requested (refDbName, refTableName, refColName)
// triple in this vnode's local meta and returns either:
//   * terminated=true with the terminal value (physical column for COL, tag value for TAG), or
//   * terminated=false with the next hop's SColRef triple (so the caller can continue).
//
// Strict single-hop: even when the next hop also lives on this vnode, the
// triple is returned to the originating vnode for the next round of RPC.
// Rationale: keep this handler trivial and easy to test; multi-hop locality
// is rare enough that the extra round-trip cost is acceptable.
//
// Does NOT depend on stream task existence.
int32_t vnodeProcessVTableRefResolveReq(SVnode *pVnode, SRpcMsg *pReq);
```

**关键属性**：

- 仅依赖 `pVnode->pMeta`，不访问任何 stream 状态；
- **严格单跳**：每个请求 item 只查一次本地 meta，不在中间 vnode 内部继续展开（即使下一跳 ref 又落在本 vnode 上，也直接把 ref 三元组返回给最初 vnode，由其再次发 RPC）。这样函数 B 实现极简，易测试；同 vnode 链路重复 RPC 的开销可接受（vtable 链式跨多跳本身罕见）；
- 任何引用表 / 列不存在直接在响应 item 上 `code = TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST` / `_COL_NOT_EXIST`。

**判断"本地"**：调用 `metaGetTableEntryByName(pMeta, refTableName)` 试探，hit 则解析当前跳，miss 则在响应 item 设 `code = TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST`（最初 vnode 通过 `dbVgInfo` 路由能区分"真不存在"与"路由错了"——前者透传错误，后者重发到正确 vnode）。

#### 5.3.4 传输层细节（v0.4 联调回灌）

新消息端到端链路涉及三处**必须**同步的注册点：

| 注册点 | 文件 | 作用 |
|---|---|---|
| 消息号定义 | `include/common/tmsgdef.h` | `TD_DEF_MSG_TYPE(TDMT_VND_VTABLE_REF_RESOLVE, ...)` |
| dnode 派发 | `source/dnode/mgmt/mgmt_vnode/src/vmHandle.c::dmSetMgmtHandle` | `dmSetMgmtHandle(pArray, TDMT_VND_VTABLE_REF_RESOLVE, vmPutMsgToFetchQueue, 0)` |
| vnode 处理 | `source/dnode/vnode/src/vnd/vnodeSvr.c::vnodeProcessFetchMsg` | `case TDMT_VND_VTABLE_REF_RESOLVE: return vnodeProcessVTableRefResolveReq(...)` |

任一遗漏都会导致 RPC 在 dnode 派发层被丢弃（默认走 unknown handler）。

**SMsgHead 前置（强制要求）**：

`vmPutMsgToQueue` 实现里强制对 `pMsg->contLen >= sizeof(SMsgHead)`、并按 `pCont` 头 8 字节 `ntohl` 解出 `(vgId, contLen)` 然后 `vmAcquireVnodeWrapper(vgId)`。因此 sender 必须按下面 pattern 构造请求（参考 `source/dnode/vnode/src/sma/smaRollup.c:625-651`）：

```c
int32_t   bodyLen = tSerializeSVTableRefResolveReq(NULL, 0, &req);
int32_t   totalLen = bodyLen + sizeof(SMsgHead);
void     *pBuf = rpcMallocCont(totalLen);
SMsgHead *pHead = (SMsgHead *)pBuf;
pHead->vgId    = htonl(vgId);
pHead->contLen = htonl(totalLen);
tSerializeSVTableRefResolveReq(POINTER_SHIFT(pBuf, sizeof(SMsgHead)), bodyLen, &req);
rpcMsg.pCont   = pBuf;
rpcMsg.contLen = totalLen;
```

receiver `vnodeProcessVTableRefResolveReq` 必须对应跳过 head：

```c
void   *pBody = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
int32_t bodyLen = pMsg->contLen - sizeof(SMsgHead);
if (tDeserializeSVTableRefResolveReq(pBody, bodyLen, &req) < 0) { ... }
```

**典型坑**：缺 SMsgHead 或 vgId/contLen 未做 `htonl` → `vmPutMsgToQueue` 用脏 vgId 取 vnode → 失败日志显示 "Vnode is closed or removed"，与 vnode 真实状态无关。

**mnode 路由刷新协议（GET_DB_INFO vs USE_DB）**：

`streamFetchDbVgInfo` 向 mnode 拉 db→vgInfo 路由表时**统一用 `TDMT_MND_GET_DB_INFO`**：

- mnode 端 `mndDb.c:99` 把 `TDMT_MND_USE_DB` 与 `TDMT_MND_GET_DB_INFO` 都注册到同一 handler `mndProcessUseDbReq`，请求体 `SUseDbReq` / 响应体 `SUseDbRsp` 完全一致，**功能 100% 等价**；
- 但语义层面：`USE_DB` 给客户端 `USE database` 命令，`GET_DB_INFO` 给 catalog / vnode 内部刷新路由（catalog 自身就用 `CTG_TASK_GET_DB_INFO`）。
- 选 `GET_DB_INFO` 与 catalog 内部统一，便于审计与日志区分用户行为 vs 系统行为。

**异步同步等待 pattern（参考 `dataInserter.c::buildDbVgInfoMap`）**：

`streamFetchDbVgInfo` 不用 `rpcSendRecv`（同步阻塞 RPC API），改用 `asyncSendMsgToServer` + 本地 `tsem_t ready` 信号量：

```c
typedef struct SStreamFetchDbVgCtx {
  tsem_t     ready;
  SUseDbRsp *pRsp;
  int32_t    code;
} SStreamFetchDbVgCtx;

// callback drains pMsg, deserializes into ctx.pRsp, posts ready.
static int32_t streamProcessFetchDbVgRsp(void *param, SDataBuf *pMsg, int32_t code);

// caller path:
SMsgSendInfo *pSI = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
pSI->param = &ctx; pSI->fp = streamProcessFetchDbVgRsp;
pSI->msgType = TDMT_MND_GET_DB_INFO;
pSI->msgInfo.pData = pReqBuf; pSI->msgInfo.len = reqLen;
asyncSendMsgToServer(clientRpc, &mnodeEpset, NULL, pSI);
tsem_wait(&ctx.ready);
```

理由：与 inserter / executor 模块统一调度模型，异步框架可以让 RPC 复用 client transport 的工作线程池，避免在调用上下文阻塞 vnode 自身的 RPC 线程；同时与未来批量并发投递路径自然兼容（`asyncSendMsgToServer` 已支持多请求并发等待）。

注意点：
- `pSI->msgInfo.pData` 用 `taosMemoryCalloc` 而非 `rpcMallocCont`（async 路径所有权交由 transport，最终走 `taosMemoryFree`）；
- `tsem_init` / `tsem_destroy` 配对；
- callback 内 `taosMemoryFreeClear(pMsg->pData)` + `taosMemoryFreeClear(pMsg->pEpSet)`，否则 transport buffer 泄漏。

### 5.4 VTABLE_INFO 处理改造

#### 5.4.1 trigger 端

**无改动**。`STRIGGER_PULL_VTABLE_INFO` 请求结构和构建逻辑均保持不变（trigger 仍只发 column cids）。tag cids 由 reader 端从本地 `SStreamTriggerReaderInfo->partitionCols` 自行解析得出，避免 trigger / reader 重复维护同一信息。

#### 5.4.2 reader 端（`vnodeProcessStreamVTableInfoReq` @ vnodeStream.c:3751）

入口判断完全沿用现有分支（参考 vnodeStream.c:3770）：

```
1. 解析请求 → 得到 cids（virtColCids）和 reqUids、fetchAllTable;
2. 从 pInfo->partitionCols 解析出 tagCids（一次性）:
     tagCids = [];
     for node in pInfo->partitionCols:
       if isTagColumn(node):  // SColumnNode.colType == COLUMN_TYPE_TAG
         tagCids.push(node->colId);
3. 选 uidList & cids:
     if fetchAllTable || reqUids == NULL || size(reqUids) == 0:
       uidList = NULL;                       // A 函数内部走全量分支 (Q2 全量)
       fullScan  = true;
     else:
       uidList = reqUids;                    // 局部分支 (Q2 指定)
       fullScan  = false;
     // isVtableOnlyTs 快速路径：当 trigger 只查 PRIMARYKEY_TIMESTAMP_COL_ID 时
     // （atomic_val_compare_exchange_8 标记），把 cids 置 NULL 触发 work-list
     // 初始化时遍历 vtable 全部 colRef（streamPushInitialWorkItemsForUid 的
     // colCids==NULL 分支）。
4. 调用:
     streamResolveVTableRefChain(pVnode, &pInfo->vtbCache, pInfo, ver, uidList,
                                 cids, tagCids, &uid2Result);
   返回非零 → 透传 tmsgSendRsp 失败码（OOM / 结构性技术错误）；
   返回零 → 进入步骤 5。
5. 编码响应（仍照现有协议）:
   - 调 `streamFillVTableInfoFromResolved`：从 `uid2Result[uid].colMap` 重建
     `vTableInfo->infos[uid].cols`（每条 col 终点 = SColResolveItem 的 hasRef +
     refDbName/refTableName/refColName）。**响应只编 col；tagMap 仅写 cache，不进响应**；
   - fullScan 模式：遍历 uid2Result 全部 uid；
   - 局部模式：仅遍历 reqUids 中且存在于 uid2Result 的 uid；
   - 缺失 uid（顶层 uid 不存在被 H2 跳过）按现有 getAllVinfo 行为：直接不出现在响应里。任何业务/RPC 错误此时已让 A 函数整体返回非零，调用方直接把 errCode 透传到 VTABLE_INFO 响应 rsp.code，不会走到这里。
6. 写 cache（taosWLockLatch）:
   - fullScan 模式 (Q3=C2a 原子替换)：
       newCache = uid2Result（move ownership）；
       swap(pInfo->vtbCache.uid2Result, newCache);
       streamCacheCommitResolved 释放旧 cache；
       // 旧 uid 不在新 list → 自然从 cache 删除
   - 局部模式 (M1 局部覆盖)：
       for each (uid, result) in uid2Result:
         oldEntry = pInfo->vtbCache.uid2Result[uid];
         pInfo->vtbCache.uid2Result[uid] = result;
         streamVTableResolveResultDestroy(oldEntry);
       // 不在 reqUids 中的 uid 不动
   - pInfo->vtbCache.lastCheckMs = taosGetTimestampMs();
   - pInfo->vtbCache.valid = true;
   - 首次写入或 cids/tagCids 集合变化时，同步 reqColCids / reqTagCids（用于定时检测）。
7. tmsgSendRsp 成功响应。
```

**响应字段**：保持现有 `STRIGGER_PULL_VTABLE_INFO` 响应结构不变（trigger 仍按列消费）。tag 信息**仅 reader 端缓存**，trigger 端不直接消费 tag 值（trigger 关心的是"tag 是否变化"，由 RPC error code 通知）。

### 5.5 VTABLE_PSEUDO_COL 改造

#### 5.5.1 reader 端

将原本"直接读 ref 一跳拿 tag 值"的逻辑改为：

```c
// old: read SColRef and fetch tag value from direct ref
// new (CHILD vtable):  call the chain resolver with a single-uid list, pCache=NULL
// new (NORMAL vtable): keep the legacy metaReader path (取表名),
//                      普通 vtable 没有 tag 概念，无需链式追踪
SMetaReader mr = {0};
metaReaderDoInit(&mr, pVnode->pMeta, META_READER_LOCK);
metaGetTableEntryByUid(&mr, req->virTablePseudoColReq.uid);
if (mr.me.type == TSDB_VIRTUAL_NORMAL_TABLE) {
  // NORMAL vtable: 直接用 metaReader 取表名 / db 名编码即可
  ...
  metaReaderClear(&mr);
  return tmsgSendRsp(...);
}
// CHILD vtable: 走链式追踪
metaReaderClear(&mr);
SSHashObj *uid2Result = NULL;
SArray    *singleUid  = taosArrayInit(1, sizeof(int64_t));
taosArrayPush(singleUid, &req->virTablePseudoColReq.uid);
SArray    *emptyCols  = taosArrayInit(0, sizeof(col_id_t));
streamResolveVTableRefChain(pVnode,
                            /*pCache=*/NULL,         // bypass cache
                            pInfo, ver, singleUid, emptyCols,
                            requestedTagCids, &uid2Result);
SVTableResolveResult **pp = tSimpleHashGet(uid2Result, &uid, sizeof(uid));
// encode (*pp)->tagMap into PSEUDO_COL response
```

**关键**：
- 仅 CHILD vtable 走链式追踪；NORMAL vtable 沿用既有 metaReader 表名分支（普通 vtable 没有 tag 语义，无需追踪）；
- PSEUDO_COL 仅需一个 uid，但仍走批量 A 函数（接口统一）；
- PSEUDO_COL 的 tag cid 集合可能与 cache 中 `reqTagCids` 不同（PSEUDO_COL 走 SQL 投影列，cache 走 partitionCols），故 **PSEUDO_COL 调函数 A 时传 `pCache=NULL` 实现"读不写不"** —— 每次重新解析返回最新值，且不污染 partitionCols cache；
- 任何错误（顶层 uid 不存在 / 业务错误码 / RPC 失败）均由 A 函数返回值反映：顶层 uid 不存在 → 函数返回 0 但 `uid2Result` 中无 entry，PSEUDO_COL 透传 `TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST`；其它任何错误 → A 函数已返回该 errCode，PSEUDO_COL 直接透传给 trigger（PSEUDO_COL 单 uid 必须成功，无"部分成功"语义）。

### 5.6 定时检测 hook

#### 5.6.1 触发点

`source/dnode/vnode/src/vnd/vnodeStream.c`：

- `vnodeProcessStreamWalMetaNewReq` 入口
- `vnodeProcessStreamWalMetaDataNewReq` 入口

#### 5.6.2 节流逻辑

```c
int64_t now = taosGetTimestampMs();
if (pInfo->vtbCache.valid &&
    now - pInfo->vtbCache.lastCheckMs >= STREAM_VTB_RECHECK_INTERVAL_MS /* 1s */) {
  // serialize via vtbCache.lock to avoid concurrent re-check
  taosWLockLatch(&pInfo->vtbCache.lock);
  if (now - pInfo->vtbCache.lastCheckMs >= STREAM_VTB_RECHECK_INTERVAL_MS) {
    int32_t rc = streamRecheckVTableCache(pVnode, pInfo, /*walVer=*/...);
    if (rc != 0) {
      // tag changed: set rsp.code = TSDB_CODE_STREAM_VTB_TAG_CHANGED, return early
      // col changed: append TABLE_BLOCK_ADD entries with changedUids
    }
    pInfo->vtbCache.lastCheckMs = taosGetTimestampMs();
  }
  taosWUnLockLatch(&pInfo->vtbCache.lock);
}
// continue with normal WAL meta processing
```

#### 5.6.3 `streamRecheckVTableCache` 内部 diff 算法（K + L1，v0.6 分片化）

```
1. 若 sliceCursor == 0：
     重建 uidSlice = qStreamGetTableArrayList(pInfo) 中所有非 markedDeleted 的 uid
     // 取自 reader 维护的全量表列表（非 uid2Result.keys()），这样
     //   ① cache-miss 路径还没写入 cache 的新 vtable 也能纳入检测；
     //   ② 已 drop 的 vtable 自动随 markedDeleted 过滤掉。
   total = uidSlice.size()
   begin = sliceCursor
   end   = min(begin + STREAM_VTB_RECHECK_SLICE_SIZE, total)
   sliceUids = uidSlice[begin : end]

2. 调 streamResolveVTableRefChain(pVnode, NULL, pInfo, walVer, sliceUids,
                                  pInfo->vtbCache.reqColCids,
                                  pInfo->vtbCache.reqTagCids,
                                  &newUid2Result)
   若 A 函数返回非零（业务错误 / RPC 失败 / 技术错误）→ 把该 errCode 塞进
   WAL_META_NEW 响应 rsp.code，trigger 收到后按既有失败路径走（INTERNAL_ERROR
   → mnode → undeploy/redeploy）。仅当 A 返回 0 时才继续步骤 3。

3. for each uid in sliceUids:                            // L1: 只 diff 本片
     oldResult = pInfo->vtbCache.uid2Result[uid]
     newResult = lookup(newUid2Result, uid)
     if newResult == NULL:                               // 顶层 uid 被 H2 跳过（已 drop）
       if oldResult: destroy(oldResult); uid2Result.remove(uid)  // 同步从 cache 删
       continue
     // tag diff (任一变化 → fatal)
     for each (cid, oldTag) in oldResult.tagMap:
       newTag = lookup(newResult.tagMap, cid)
       if newTag == NULL: continue
       if oldTag.type != newTag.type
          || oldTag.nLen != newTag.nLen
          || memcmp(oldTag.pData, newTag.pData, nLen) != 0:
         tagChanged = true; break
     if tagChanged: return TSDB_CODE_STREAM_VTB_TAG_CHANGED
     // col diff
     for each (cid, oldItem) in oldResult.colMap:
       newItem = lookup(newResult.colMap, cid)
       if newItem == NULL: continue
       if oldItem.hasRef != newItem.hasRef
          || (oldItem.hasRef && (strcmp refDbName/refTableName/refColName) != 0):
         changedUids.add(uid); break
     // per-uid 替换 cache（M1，局部覆盖）
     pInfo->vtbCache.uid2Result[uid] = newResult       // 转移所有权
     destroy(oldResult)

4. sliceCursor = (end >= total) ? 0 : end              // 游标推进 / 回绕

5. 若 changedUids 非空：
     在 WAL_META_NEW 响应里把 changedUids 装进 TABLE_BLOCK_ADD（既有 IS_PATCHING_VITRUAL_TABLE 路径）
```

**diff 复杂度**：每次 tick 最多 `SLICE_SIZE` 个 uid，hash 直接 lookup。不再做全量替换，避免 N 个 uid 一次性 RPC 阻塞读路径。新增 uid 由 reader cache-miss 路径写入；H2 跳过的 uid 在 diff 阶段同步从 cache 删除（与原 C2a 全量替换语义等价，但按 uid 粒度推进）。

### 5.7 错误码

新增到 `include/util/taoserror.h` 的 stream 段：

```c
TAOS_DEFINE_ERROR(TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST, "Stream vtable ref table not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST,   "Stream vtable ref column/tag not exist")
TAOS_DEFINE_ERROR(TSDB_CODE_STREAM_VTB_TAG_CHANGED,         "Stream vtable partition tag changed")
// TSDB_CODE_STREAM_VTB_REF_TOO_DEEP: reuse existing
```

### 5.8 错误处理与回灌路径

| 场景 | 路径 |
|---|---|
| 顶层 uid 不存在（vtable 被并发 drop 等，metaGetTableEntryByUid NOT_EXIST） | A 函数内部按 H2 仅 warning + skip 该 uid，函数仍返回 0；调用方 reader 在 VTABLE_INFO 响应里直接不出现该 uid（与现有 `getAllVinfo` 行为一致） |
| 业务错误（REF_TABLE_NOT_EXIST / REF_COL_NOT_EXIST / REF_TOO_DEEP / TAG_CHANGED） | A 函数立即返回业务错误码 → reader 透传到 VTABLE_INFO / WAL_META_NEW 响应 rsp.code → trigger 进入失败终态 |
| 单次 RPC 通信失败 | A 函数立即返回 RPC 错误码 → reader 透传 → trigger 进入失败终态；不再做任何内部重试或单 uid 跳过（v0.4 收紧），重试由 mnode redeploy 自然完成 |
| 技术性错误（OOM / 编码错误 / mnode 不可达） | A 函数返回非零，VTABLE_INFO 响应透传错误 code → trigger 失败终态 |
| tag 变化（定时检测路径） | reader 在 `WAL_META_NEW` 响应里设 `rsp.code = TSDB_CODE_STREAM_VTB_TAG_CHANGED` → trigger 进入失败终态 |
| col 变化（定时检测路径） | reader 在 `WAL_META_NEW` 响应里把 changedUids 装进 `TABLE_BLOCK_ADD` → trigger 走既有 `IS_PATCHING_VITRUAL_TABLE` patch 流程 → trigger 主动发 `STRIGGER_PULL_VTABLE_INFO`（fetchAllTable=false, uids=changedUids）→ reader 收到后局部更新 cache（M1）|

## 6. 接口规范

### 6.1 公共函数

见 §5.2 `streamResolveVTableRefChain`。

### 6.2 RPC 消息

| 消息 | 方向 | 用途 |
|---|---|---|
| `TDMT_VND_VTABLE_REF_RESOLVE` (新增) | 最初 vnode → 中间 vnode | 单跳 ref 解析 |
| `TDMT_MND_GET_DB_INFO` (复用) | vnode → mnode | 获取 db 的 vgInfo（含 epset 列表） |
| `STRIGGER_PULL_VTABLE_INFO` (语义改) | trigger → reader | 协议结构不变；reader 内部从 `partitionCols` 解析 tag cids 并走链式追踪 |
| `STRIGGER_PULL_VTABLE_PSEUDO_COL` (语义改) | trigger → reader | 内部走链式追踪 |

## 7. 兼容性

- `STRIGGER_PULL_OTABLE_INFO` 现已只处理 normal/child 物理表（vtable 部分代码已删除），无兼容性问题。
- VTABLE_INFO 请求/响应**协议结构不变**，纯 reader 端内部改造；旧 trigger 与新 reader、新 trigger 与旧 reader 均无 wire format 不兼容问题。
- 新错误码对 client 透明，client 看到错误时按既有错误流程处理。

## 8. 风险与缓解

| 风险 | 等级 | 缓解 |
|---|---|---|
| 同步阻塞：A 函数最坏 32 跳 × N 个 db RPC ≈ 上百 ms | 中 | 定时检测 ≥ 10s 节流；db→vgInfo cache 复用 |
| 中间 vnode 上无 stream task 但要响应消息 | 高（设计前期） | 已通过 D2 解决：用 `TDMT_VND_*` 通用消息派发，不依赖 stream |
| 链式中循环引用（A→B→A） | 低 | 32 层深度限制兜底 |
| cache 与 schema 变化的一致性 | 中 | 每次 cache 写入带 `lastCheckNs`，定时检测覆盖；schema 变化由 trigger 主动 patch 兜底 |

## 9. 测试方案

新增测试用例目录：`community/test/cases/18-StreamProcessing/vtable-chain-ref/`

| 用例 | 场景 |
|---|---|
| TC01 | 1 跳 ref：vtable A → 物理表 X，验证 VTABLE_INFO 链式终点正确 |
| TC02 | 单 vgroup 3 跳 col chain：vt3 → vt2 → vt1 → ct0 |
| TC03 | 单 vgroup 3 跳 tag chain：vct3.region → vct2 → vct1 → ct0，stream partition by region |
| TC04 | 跨 vgroup 3 跳 col chain：每跳目标分布在不同 vgroup（动态选址重试直至跨 vg） |
| TC05 | 跨 vgroup 3 跳 tag chain：跨 vgroup 验证 tag 链式 PSEUDO_COL 正确 |
| TC06 | 单 vgroup event window + tag 过滤：select start/end with tag 条件 |
| TC07 | 跨 vgroup event window + tag 过滤：TC06 的跨 vg 版本 |
| TC08 | partition by tag 链式追踪：PSEUDO_COL 拿到正确 tag 值 |
| TC09 | tag 变化触发 `TSDB_CODE_STREAM_VTB_TAG_CHANGED`，stream 进入失败终态 |
| TC10 | column 链式终点变化触发 patch，stream 继续运行且数据正确 |
| TC11 | 引用表不存在 → `TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST` |
| TC12 | 引用列不存在 → `TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST` |
| TC13 | 链式深度 > 32 → `TSDB_CODE_STREAM_VTB_REF_TOO_DEEP` |

## 10. 工作量拆解（不含时间估计，与 plan 13 task 对齐）

| Task | 任务 | 模块 |
|---|---|---|
| Task 1 | 新错误码定义（REF_TABLE_NOT_EXIST / REF_COL_NOT_EXIST / TAG_CHANGED） | `include/util/taoserror.h`、`source/util/src/terror.c` |
| Task 2 | `SColResolveItem` / `STagValue` / `SVTableResolveResult` / `SStreamVTableInfoCache` 结构 + cache lifecycle + 挂载到 `SStreamTriggerReaderInfo` | `include/libs/new-stream/streamReader.h`、`source/libs/new-stream/src/streamReader.c` |
| Task 3 | `TDMT_VND_VTABLE_REF_RESOLVE` 消息类型 + 请求/响应结构声明（`SVTableRefResolveItem/Req/RspItem/Rsp`） | `include/common/tmsgdef.h`、`include/common/streamMsg.h` |
| Task 4 | `SVTableRefResolveReq/Rsp` 编解码 + free 实现 | `source/common/src/msg/streamMsg.c` |
| Task 5 | 处理函数 B：`vnodeProcessVTableRefResolveReq` + `vnodeResolveOneHop` + svr fetch 派发 | `source/dnode/vnode/src/vnd/vnodeStream.c`、`source/dnode/vnode/src/vnd/vnodeSvr.c`、`source/dnode/vnode/src/inc/vnd.h` |
| Task 6 | 函数 A 主循环（单 vgId 简版，work-list + 单次 RPC） | `source/dnode/vnode/src/vnd/vnodeStream.c` |
| Task 7 | 函数 A 跨 vgId 聚合 + dbVgInfo cache（`streamGetOrFetchDbVgInfo` / `streamRouteTableToVg` / `streamSendOneVgResolveRpc` / `streamCallResolveBatched`） | `source/dnode/vnode/src/vnd/vnodeStream.c` |
| Task 8 | 函数 A 全量 uid 分支（`vtbUids==NULL` → `qStreamGetTableArrayList`） | `source/dnode/vnode/src/vnd/vnodeStream.c` |
| Task 9 | reader VTABLE_INFO 改造（`vnodeProcessStreamVTableInfoReq` + `streamCollectTagCidsFromPartitionCols` + `streamFillVTableInfoFromResolved` + `streamCacheCommitResolved` + `isVtableOnlyTs` 路径） | `source/dnode/vnode/src/vnd/vnodeStream.c` |
| Task 10 | reader PSEUDO_COL 改造（`vnodeProcessStreamVTableTagInfoReq`：CHILD vtable 走链式追踪 + cache-bypass；NORMAL vtable 沿用 metaReader 表名分支） | `source/dnode/vnode/src/vnd/vnodeStream.c` |
| Task 11 | 定时检测 hook + diff 算法（`streamMaybeRecheckVTableCache` + `streamRecheckVTableCache` + `colResolveItemEqual` / `tagValueEqual`，挂在 `vnodeProcessStreamWalMetaNewReq` / `vnodeProcessStreamWalMetaDataNewReq`） | `source/dnode/vnode/src/vnd/vnodeStream.c` |
| Task 12 | 端到端测试 TC01–TC13 | `test/cases/18-StreamProcessing/02-Stream/stream_vtable_chain_ref.py` |

## 11. 待确认事项

（无）

## 附录 A：本轮关键决策（批量 + cache 策略）

| 编号 | 决议 | 适用范围 |
|---|---|---|
| Q1 | A1 — `virtColCids` / `virtTagCids` 所有 uid 共享一份 | A 函数签名 |
| Q2 | 沿用 `vnodeProcessStreamVTableInfoReq` 现有分支：`fetchAllTable` 或 `uids` 空 → 全量；否则按指定 uids | reader 入口 |
| Q3 | C2a — 全量请求"原子替换" cache，旧 uid 不在新 list → 删除 | cache 写入 |
| Q4 | G1 — 跨 uid 合并 RPC，按 vgId 聚合 | A 函数主循环 |
| Q5 / H | H2（v0.5 收紧）— 仅"顶层 uid 在本地 meta 中不存在（vtable 被并发 drop）"做 warning + skip 容错；其余任何错误（中间链路业务错误 / RPC 失败 / 技术错误）一律向上报错，由调用方决定（reader 透传到响应 rsp.code，trigger 进入失败终态触发 mnode redeploy） | A 函数错误处理 |
| Q6 | uid 来源用 `qStreamGetTableArrayList`，跳 `markedDeleted`（参考 getAllVinfo） | A 函数全量分支 |
| I | I3'（v0.5 收紧）— RPC 通信失败直接向上报错，不再做内部重试也不再做单 uid 跳过；重试由上层 mnode redeploy 自然完成 | A 函数 RPC 层 |
| J | J1 — H2 跳过的 uid（顶层不存在）不写 cache，下次请求重新解析；任何上报错误的整轮也不写 cache | A 函数返回值 |
| K | 定时检测路径也走"全量 uid + 原子替换"，与 reader 全量请求同 cache 入口 | §5.6 |
| L | L1 — diff 通知仅限 cache 中已存在的 uid；新增 uid 走 trigger 自有 PULL 路径 | §5.6.3 |
| M | M1 — 局部请求只解析 + 只覆盖请求子集；不强制升级为全量 | §5.4.2 局部分支 |
