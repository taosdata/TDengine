/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _TD_VND_STREAM_VTABLE_H_
#define _TD_VND_STREAM_VTABLE_H_

#include "osSemaphore.h"
#include "nodes.h"
#include "streamMsg.h"
#include "streamReader.h"
#include "tarray.h"
#include "tcommon.h"
#include "thash.h"
#include "vnd.h"

#ifdef __cplusplus
extern "C" {
#endif

// ---------------------------------------------------------------------------
// Shared declarations between vnodeStream.c and vnodeStreamVTable.c
// ---------------------------------------------------------------------------

// vnodeStream.c
int32_t addUidListToBlock(SArray *uidListAdd, void **block, int64_t ver, int32_t *totalRows, ETableBlockType type);
int32_t vnodeCollectNodeGroupReadRange(const SStreamRuntimeFuncInfo *pRuntime, STimeWindow *pRange);
int32_t vnodeCalculateNodeGroupReadRange(STimeRangeNode *node, SStreamRuntimeFuncInfo *pRuntime,
                                         SStreamRuntimeFuncInfo *pTmpRuntime, int32_t actualNodeId, STimeWindow *pRange,
                                         bool *pRangeValid);

// On success the caller owns *readRanges; errors leave it NULL.
int32_t vnodeCalculateNodeGroupReadRanges(STimeRangeNode *node, SStreamRuntimeFuncInfo *runtime,
                                          SStreamRuntimeFuncInfo *tmpRuntime, int32_t actualNodeId,
                                          STimeWindow *combined, bool *combinedValid, SArray **readRanges);

// vnodeStreamVTable.c
int32_t streamMaybeRecheckVTableCache(SVnode *pVnode, SStreamTriggerReaderInfo *pInfo,
                                      int64_t walVer, SSTriggerWalNewRsp *pRsp);
int32_t streamCollectTagCidsFromPartitionCols(SNodeList *partitionCols, SArray **ppTagCids);
int32_t streamValidateVTableColRef(const SColRef *pRef);
bool streamVTableInfoHasColumnRef(const SStreamMsgVTableInfo *pVTableInfo);
int32_t streamFillVTableInfoFromResolved(SVnode *pVnode, SStreamTriggerReaderInfo *sStreamReaderInfo,
                                         int64_t uid, uint64_t gid, int64_t ver, SArray *cids,
                                         SVTableResolveResult *pRes, SMetaReader *metaReader,
                                         SArray *infos);
int32_t streamCacheCommitResolved(SStreamVTableInfoCache *pCache, bool fullScan,
                                  SArray *cids, SArray *tagCids, SSHashObj **ppUid2Result);

// Drive multi-hop resolution of vtable column/tag refs on the triggering vnode.
// Each batch is grouped by target vgId and dispatched via one RPC per vg group.
// Resolution is seeded in bounded batches: a batch closes at
// STREAM_VTB_RESOLVE_UID_CHUNK uids or STREAM_VTB_RESOLVE_MAX_ITEMS work items,
// whichever comes first, and the per-batch results are merged, so the peak
// memory is bounded by the batch size instead of the whole table list.
// pCache (optional): caches db routing info (SUseDbRsp) across calls.
// Its tblRefCache and uid2Result are untouched; ref caching is call-local, and
// db -> routing lookups are memoized for the whole call (across batches).
// The call-private ref cache retains multi-ref batches and nonempty batches
// with unseeded uids, including sparse batches with only one remote ref.
// A final single-ref batch does not allocate a new ref cache.
// pReaderInfo (optional): when vtbUids is NULL/empty, all live uids are pulled
//                          from qStreamGetTableArrayList(pReaderInfo).
// virtColCids: NULL resolves EVERY column ref of each vtable ("full resolve");
//              an empty array resolves no columns at all. The two sentinels are
//              NOT symmetric with virtTagCids, so callers that only need tags
//              must pass an empty array here, never NULL.
// virtTagCids: NULL resolves no tags; only the listed cids are resolved.
// Output ownership is transferred to caller (free via streamVTableResolveResultDestroy
// + tSimpleHashCleanup in stream lib).
int32_t streamResolveVTableRefChain(SVnode *pVnode, SStreamVTableInfoCache *pCache,
                                    SStreamTriggerReaderInfo *pReaderInfo, int64_t ver,
                                    SArray *vtbUids, SArray *virtColCids, SArray *virtTagCids,
                                    SSHashObj **ppUid2Result, int64_t txnId);

// Batch budget used by streamResolveVTableRefChain: true once the current batch
// has consumed either budget (seeded uids or accumulated work items). The uid
// budget alone is not enough because one virtual child table can carry an
// arbitrary number of ref columns/tags.
//
// The budgets live here (not in the .c file) so the unit test pins exactly the
// boundaries the resolver uses. A 12k-child table list used to materialize
// ~600 MB of SResolveWorkItem entries (plus the matching response array) in a
// single pass and produce equally huge fan-out RPC payloads; slicing bounds the
// peak to SLICE * items-per-uid while keeping the merged result identical. 20k
// work items is ~10 MB regardless of the table shape.
#define STREAM_VTB_RESOLVE_UID_CHUNK   1000
#define STREAM_VTB_RESOLVE_MAX_ITEMS   20000
bool streamResolveBatchFull(int32_t seededUids, int32_t workItemCnt);

// Build / release the call-private cache view used by
// streamResolveVTableRefChain. dbVgInfo is borrowed from pShared (which may be
// NULL when there is no reader cache) and is never freed here; tblRefCache is
// created only when createRefCache is true. On success every field of pOut is
// initialized, so callers can always pair it with streamResolveCacheDestroy().
int32_t streamResolveCacheInit(SStreamVTableInfoCache *pOut, const SStreamVTableInfoCache *pShared,
                               bool createRefCache);
void    streamResolveCacheDestroy(SStreamVTableInfoCache *pCache);

// Allocate the ref cache of a call-private view on demand (idempotent, NULL-safe).
// The resolver uses it after empty batches when references first appear and
// can be reused in this batch or by unseeded uids.
// MUST only be passed a view created by streamResolveCacheInit(): enabling it on
// a shared reader cache would make ref results call-mutable again.
int32_t streamResolveCacheEnableRefCache(SStreamVTableInfoCache *pCache);

// Whether a batch's reference results must be cached for this resolve call:
// several refs inside the batch (or across hops), or a nonempty batch that
// still has unseeded uids which may resolve the same ref later.
bool streamResolveNeedsRefCache(int32_t workItemCnt, bool hasMoreUids);

// Whether a resolve call needs the call-private cache *view* (not necessarily
// the ref cache inside it: a view can exist without one). True when either a
// shared reader cache exists (to borrow routing from) or this batch's refs must
// be cached.
bool streamResolveNeedsPrivateCacheView(const SStreamVTableInfoCache *pShared, bool needsRefCache);

// Bound on the number of distinct source dbs the linear fast path of the route
// memo remembers. One batch normally touches one or two dbs (all items share the
// account, and a ref chain usually stays inside one db), so a small linear array
// beats a hash allocation on this hot path. Beyond the bound a lazily created
// hash takes over: without it, every work item of the 9th+ db would go back to
// mnode for the same routing info, i.e. exactly the storm this memo removes.
#define STREAM_VTB_LOCAL_DB_ROUTE_MAX 8

typedef struct SStreamLocalDbRoute {
  char       dbFName[TSDB_DB_FNAME_LEN];
  SUseDbRsp *pRsp;
} SStreamLocalDbRoute;

// Per-resolve-call db -> routing memo, owned by streamResolveVTableRefChain and
// passed to streamBatchRouteToVgs through streamCallResolveBatched. Owns the
// SUseDbRsp entries it holds (both the inline slots and the overflow hash);
// streamDbRouteMemoDestroy() releases them.
typedef struct SStreamDbRouteMemo {
  SStreamLocalDbRoute slots[STREAM_VTB_LOCAL_DB_ROUTE_MAX];
  int32_t             num;
  SHashObj           *pOverflow;  // dbFName -> SUseDbRsp*, created on the first overflow
} SStreamDbRouteMemo;

void       streamDbRouteMemoInit(SStreamDbRouteMemo *pMemo);
SUseDbRsp *streamDbRouteMemoGet(SStreamDbRouteMemo *pMemo, const char *dbFName);
// Takes ownership of pRsp on success (TSDB_CODE_SUCCESS only); on any failure,
// including TSDB_CODE_DUP_KEY, the caller keeps ownership and must release pRsp
// after use.
int32_t    streamDbRouteMemoPut(SStreamDbRouteMemo *pMemo, const char *dbFName, SUseDbRsp *pRsp);
void       streamDbRouteMemoDestroy(SStreamDbRouteMemo *pMemo);

// Resolve virtual child table tag chains for a list of STUidTagInfo. If suid is not
// a virtual super table this is a no-op. For each vchild uid that resolves, the
// existing pTagVal is freed and replaced by a freshly built STag carrying the
// chain-resolved literal tag values (in stable schemaTag order).
// pTagCids (SArray of col_id_t) limits the resolve scope to the given TAG columns;
// NULL resolves every TAG of the stable, an empty array resolves nothing.
// Resolution and rebuild failures are returned; filtering callers must not evaluate
// partially resolved tags after an error. txnId is the caller's batch metadata
// transaction; it is applied to every local meta read so children created in the same
// transaction are resolved instead of being treated as missing.
int32_t vnodeResolveVTableTagChain(void *pVnode, int64_t suid, SArray *pUidTagList, const SArray *pTagCids,
                                   int64_t txnId);

// true when suid is a virtual super table (whose children may carry TAG refs).
bool vnodeIsVirtualSuperTable(void *pVnode, int64_t suid, int64_t txnId);

// ---------------------------------------------------------------------------
// Test-only declarations: small leaf helpers exposed for gtest unit testing.
// These are pure-logic functions with no SVnode/RPC dependencies so they can
// be exercised directly. Not intended for use outside vnodeStreamVTable.c and
// its test binary. Keep this block in sync with the implementations in
// vnodeStreamVTable.c (search for the same names there).
// ---------------------------------------------------------------------------

// Work item used by the Phase-1 batch resolve path.
typedef struct SResolveWorkItem {
  int64_t  originVtbUid;
  col_id_t originCid;
  int8_t   kind;
  int8_t   refType;
  char     refSourceName[TSDB_EXT_SOURCE_NAME_LEN];
  char     refSchemaName[TSDB_EXT_SOURCE_SCHEMA_LEN];
  char     refDbName   [TSDB_DB_NAME_LEN];
  char     refTableName[TSDB_TABLE_NAME_LEN];
  char     refColName  [TSDB_COL_NAME_LEN];
  int32_t  tagCondLen;
  char    *tagCondJson;   // Borrowed from the source ref or owned by this work item when ownsTagCond is true.
  bool     ownsTagCond;
} SResolveWorkItem;

// Shared cross-vg fanout sync (shared sem + ref/pending counters).
// Struct is defined here so tests can drive refs directly via atomic_store_32.
// Production code in vnodeStreamVTable.c is the only non-test consumer.
typedef struct SStreamFanoutSync {
  tsem2_t sem;
  int32_t pending;
  int32_t refs;
} SStreamFanoutSync;

SStreamFanoutSync *streamFanoutSyncCreate(void);
void               streamFanoutSyncDestroy(SStreamFanoutSync *p);
bool               streamFanoutSyncRelease(SStreamFanoutSync *p);

bool tagValueEqual(const STagValue *a, const STagValue *b);
bool streamColResolveItemEqual(const SColResolveItem *pLeft, const SColResolveItem *pRight);

void streamBuildTblColKey(const char *db, const char *tb, const char *col,
                          char *out, int32_t *outLen);

void streamBuildRefKey(const SResolveWorkItem *item, char *out, int32_t *outLen);

int32_t streamWriteRspItemDeepCopy(const SVTableRefResolveRspItem *src,
                                   SVTableRefResolveRspItem *dst);

SVTableRefResolveRspItem *streamTblRefCacheLookup(SStreamVTableInfoCache *pCache,
                                                  const char *dbName, const char *tableName,
                                                  const char *colName, int8_t kind);
SVTableRefResolveRspItem *streamTblRefCacheLookupEx(SStreamVTableInfoCache *pCache,
                                                    const char *dbName, const char *tableName,
                                                    const char *colName, int8_t kind,
                                                    const char *tagCondJson, int32_t tagCondLen);

void streamTblRefCacheInsert(SStreamVTableInfoCache *pCache,
                             const char *dbName, const char *tableName,
                             const char *colName, int8_t kind,
                             const SVTableRefResolveRspItem *pItem);
void streamTblRefCacheInsertEx(SStreamVTableInfoCache *pCache,
                               const char *dbName, const char *tableName,
                               const char *colName, int8_t kind,
                               const char *tagCondJson, int32_t tagCondLen,
                               const SVTableRefResolveRspItem *pItem);

int32_t streamBatchTryCacheAndDedup(SStreamVTableInfoCache *pCache, SArray *batch,
                                    SArray *outRspItems, SHashObj *dedupMap,
                                    SArray *dedupItems, int32_t *origToDedupIdx,
                                    int32_t *pCacheHits);

#ifdef __cplusplus
}
#endif

#endif /* _TD_VND_STREAM_VTABLE_H_ */
