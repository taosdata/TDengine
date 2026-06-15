/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

// vnodeStreamVTable.c
int32_t streamMaybeRecheckVTableCache(SVnode *pVnode, SStreamTriggerReaderInfo *pInfo,
                                      int64_t walVer, SSTriggerWalNewRsp *pRsp);
int32_t streamCollectTagCidsFromPartitionCols(SNodeList *partitionCols, SArray **ppTagCids);
int32_t streamFillVTableInfoFromResolved(SVnode *pVnode, SStreamTriggerReaderInfo *sStreamReaderInfo,
                                         int64_t uid, uint64_t gid, int64_t ver, SArray *cids,
                                         SVTableResolveResult *pRes, SMetaReader *metaReader,
                                         SArray *infos);
int32_t streamCacheCommitResolved(SStreamVTableInfoCache *pCache, bool fullScan,
                                  SArray *cids, SArray *tagCids, SSHashObj **ppUid2Result);

// Drive multi-hop resolution of vtable column/tag refs on the triggering vnode.
// Each batch is grouped by target vgId and dispatched via one RPC per vg group.
// pCache (optional): caches db routing info (SUseDbRsp) across calls.
// pReaderInfo (optional): when vtbUids is NULL/empty, all live uids are pulled
//                          from qStreamGetTableArrayList(pReaderInfo).
// Output ownership is transferred to caller (free via streamVTableResolveResultDestroy
// + tSimpleHashCleanup in stream lib).
int32_t streamResolveVTableRefChain(SVnode *pVnode, SStreamVTableInfoCache *pCache,
                                    SStreamTriggerReaderInfo *pReaderInfo, int64_t ver,
                                    SArray *vtbUids, SArray *virtColCids, SArray *virtTagCids,
                                    SSHashObj **ppUid2Result);

// Resolve virtual child table tag chains for a list of STUidTagInfo. If suid is not
// a virtual super table this is a no-op. For each vchild uid that resolves, the
// existing pTagVal is freed and replaced by a freshly built STag carrying the
// chain-resolved literal tag values (in stable schemaTag order).
int32_t vnodeResolveVTableTagChain(void *pVnode, int64_t suid, SArray *pUidTagList);

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
  char     refDbName   [TSDB_DB_NAME_LEN];
  char     refTableName[TSDB_TABLE_NAME_LEN];
  char     refColName  [TSDB_COL_NAME_LEN];
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

void streamBuildTblColKey(const char *db, const char *tb, const char *col,
                          char *out, int32_t *outLen);

int32_t streamWriteRspItemDeepCopy(const SVTableRefResolveRspItem *src,
                                   SVTableRefResolveRspItem *dst);

SVTableRefResolveRspItem *streamTblRefCacheLookup(SStreamVTableInfoCache *pCache,
                                                  const char *dbName, const char *tableName,
                                                  const char *colName, int8_t kind);

void streamTblRefCacheInsert(SStreamVTableInfoCache *pCache,
                             const char *dbName, const char *tableName,
                             const char *colName, int8_t kind,
                             const SVTableRefResolveRspItem *pItem);

int32_t streamBatchTryCacheAndDedup(SStreamVTableInfoCache *pCache, SArray *batch,
                                    SArray *outRspItems, SHashObj *dedupMap,
                                    SArray *dedupItems, int32_t *origToDedupIdx,
                                    int32_t *pCacheHits);

#ifdef __cplusplus
}
#endif

#endif /* _TD_VND_STREAM_VTABLE_H_ */
