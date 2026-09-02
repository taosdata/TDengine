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

// vnodeStreamVTable.c
//
// Virtual-table (vtable) reference-chain resolution helpers for the stream
// trigger reader path. Extracted from vnodeStream.c to keep that file at a
// manageable size. Functions here cover:
//   - per-uid VTableInfo packing from resolved chain results
//   - throttled vtable-cache recheck hook (WAL meta entry points)
//   - single-hop TDMT_VND_VTABLE_REF_RESOLVE server handler
//   - multi-hop driver that fans out RPCs and walks the ref chain
//   - vchild tag-chain resolver used by executor tag-ref scans

#include <stdbool.h>
#include <stdint.h>
#include <taos.h>
#include <tdef.h>
#include "executor.h"
#include "nodes.h"
#include "osMemPool.h"
#include "osMemory.h"
#include "osSemaphore.h"
#include "query.h"
#include "scalar.h"
#include "stream.h"
#include "streamReader.h"
#include "taosdef.h"
#include "taoserror.h"
#include "tarray.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "tdb.h"
#include "tencode.h"
#include "tglobal.h"
#include "thash.h"
#include "tlist.h"
#include "tlockfree.h"
#include "tmsg.h"
#include "tsimplehash.h"
#include "ttypes.h"
#include "tutil.h"
#include "vnd.h"
#include "vnode.h"
#include "vnodeInt.h"
#include "vnodeStreamVTable.h"


// ---------------------------------------------------------------------------
// Block A: per-uid VTableInfo helpers + cache commit
// ---------------------------------------------------------------------------

// Extract tag-typed column ids from the reader's partition-cols node list.
// On success returns a fresh SArray<col_id_t> (may be empty); caller frees it.
int32_t streamCollectTagCidsFromPartitionCols(SNodeList *partitionCols, SArray **ppTagCids) {
  *ppTagCids = NULL;
  SArray *tagCids = taosArrayInit(0, sizeof(col_id_t));
  if (tagCids == NULL) return terrno;
  SNode *pNode = NULL;
  FOREACH(pNode, partitionCols) {
    if (pNode == NULL || nodeType(pNode) != QUERY_NODE_COLUMN) continue;
    SColumnNode *c = (SColumnNode *)pNode;
    if (c->colType != COLUMN_TYPE_TAG) continue;
    col_id_t cid = c->colId;
    if (taosArrayPush(tagCids, &cid) == NULL) { taosArrayDestroy(tagCids); return terrno; }
  }
  *ppTagCids = tagCids;
  return 0;
}

// For a single resolved uid, fill one VTableInfo entry: copy each requested cid's
// resolved terminal SColResolveItem into pColRef[i] (hasRef + ref{Db,Table,Col}Name);
// id is the virtual cid itself. version is taken from metaReader if available.
int32_t streamFillVTableInfoFromResolved(SVnode *pVnode, SStreamTriggerReaderInfo *sStreamReaderInfo,
                                                int64_t uid, uint64_t gid, int64_t ver, SArray *cids,
                                                SVTableResolveResult *pRes, SMetaReader *metaReader,
                                                SArray *infos) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *pTask = sStreamReaderInfo->pTask;

  VTableInfo *vTable = taosArrayReserve(infos, 1);
  STREAM_CHECK_NULL_GOTO(vTable, terrno);
  vTable->uid = uid;
  vTable->gId = gid;

  // Pull schema version + colRef from meta. cids==NULL means "all columns of
  // this vtable", in which case we also need me.colRef.pColRef as the iteration
  // source. Soft-fail (leave version=0 / nCols=0) if the entry is gone.
  int32_t version  = 0;
  bool    haveMeta = false;
  code = sStreamReaderInfo->storageApi.metaReaderFn.getTableEntryByVersionUid(metaReader, ver, uid);
  if (code == 0) {
    version  = metaReader->me.colRef.version;
    haveMeta = true;
  } else {
    code = 0;
  }

  if (cids == NULL) {
    // "All columns" mode: enumerate the vtable's own pColRef.
    int32_t nAll = haveMeta ? metaReader->me.colRef.nCols : 0;
    vTable->cols.nCols   = nAll;
    vTable->cols.version = version;
    if (nAll > 0) {
      vTable->cols.pColRef = taosMemoryCalloc(nAll, sizeof(SColRef));
      STREAM_CHECK_NULL_GOTO(vTable->cols.pColRef, terrno);
      for (int32_t j = 0; j < nAll; ++j) {
        col_id_t cid = metaReader->me.colRef.pColRef[j].id;
        vTable->cols.pColRef[j].id = cid;
        if (pRes == NULL || pRes->colMap == NULL) continue;
        SColResolveItem **pp = (SColResolveItem **)tSimpleHashGet(pRes->colMap, &cid, sizeof(cid));
        if (pp == NULL || *pp == NULL) continue;
        SColResolveItem *item = *pp;
        vTable->cols.pColRef[j].hasRef = item->hasRef;
        if (item->hasRef) {
          tstrncpy(vTable->cols.pColRef[j].refDbName,    item->refDbName,    TSDB_DB_NAME_LEN);
          tstrncpy(vTable->cols.pColRef[j].refTableName, item->refTableName, TSDB_TABLE_NAME_LEN);
          tstrncpy(vTable->cols.pColRef[j].refColName,   item->refColName,   TSDB_COL_NAME_LEN);
        }
      }
    }
  } else {
    int32_t nCids = (int32_t)taosArrayGetSize(cids);
    vTable->cols.nCols   = nCids;
    vTable->cols.version = version;
    vTable->cols.pColRef = taosMemoryCalloc(nCids, sizeof(SColRef));
    STREAM_CHECK_NULL_GOTO(vTable->cols.pColRef, terrno);

    for (int32_t i = 0; i < nCids; ++i) {
      col_id_t cid = *(col_id_t *)taosArrayGet(cids, i);
      vTable->cols.pColRef[i].id = cid;
      if (pRes == NULL || pRes->colMap == NULL) continue;
      SColResolveItem **pp = (SColResolveItem **)tSimpleHashGet(pRes->colMap, &cid, sizeof(cid));
      if (pp == NULL || *pp == NULL) continue;
      SColResolveItem *item = *pp;
      vTable->cols.pColRef[i].hasRef = item->hasRef;
      if (item->hasRef) {
        tstrncpy(vTable->cols.pColRef[i].refDbName,    item->refDbName,    TSDB_DB_NAME_LEN);
        tstrncpy(vTable->cols.pColRef[i].refTableName, item->refTableName, TSDB_TABLE_NAME_LEN);
        tstrncpy(vTable->cols.pColRef[i].refColName,   item->refColName,   TSDB_COL_NAME_LEN);
      }
    }
  }

  if (haveMeta) {
    tDecoderClear(&metaReader->coder);
  }

end:
  return code;
}

int32_t streamCacheCommitResolved(SStreamVTableInfoCache *pCache, bool fullScan,
                                         SArray *cids, SArray *tagCids, SSHashObj **ppUid2Result) {
  int32_t code = 0;
  if (pCache == NULL || ppUid2Result == NULL || *ppUid2Result == NULL) return TSDB_CODE_INVALID_PARA;

  taosWLockLatch(&pCache->lock);
  if (fullScan) {
    TSWAP(pCache->uid2Result, *ppUid2Result);
  } else {
    void *iter = NULL; int32_t it = 0;
    while ((iter = tSimpleHashIterate(*ppUid2Result, iter, &it)) != NULL) {
      int64_t                uid = *(int64_t *)tSimpleHashGetKey(iter, NULL);
      SVTableResolveResult **pSlot = (SVTableResolveResult **)iter;
      SVTableResolveResult  *r     = *pSlot;
      if (r == NULL) continue;
      code = tSimpleHashRemove(pCache->uid2Result, &uid, sizeof(uid));
      if (code == 0) {
        code = tSimpleHashPut(pCache->uid2Result, &uid, sizeof(uid), &r, POINTER_BYTES);
        if (code != 0) {
          goto _exit;
        }
      }
    }
  }

  taosArrayDestroy(pCache->reqColCids);
  pCache->reqColCids = NULL;
  if (cids != NULL) {
    pCache->reqColCids = taosArrayDup(cids, NULL);
    if (pCache->reqColCids == NULL) { code = terrno; goto _exit; }
  }
  taosArrayDestroy(pCache->reqTagCids);
  pCache->reqTagCids = NULL;
  if (tagCids != NULL) {
    pCache->reqTagCids = taosArrayDup(tagCids, NULL);
    if (pCache->reqTagCids == NULL) { code = terrno; goto _exit; }
  }
  pCache->lastCheckMs = taosGetTimestampMs();
  pCache->valid       = true;

_exit:
  taosWUnLockLatch(&pCache->lock);
  return code;
}

// ---------------------------------------------------------------------------
// Block B: throttled vtable cache recheck hook (and its small equality helpers)
// ---------------------------------------------------------------------------

// Compare two SColResolveItem; returns true if they refer to the same terminal column.
static bool colResolveItemEqual(const SColResolveItem *a, const SColResolveItem *b) {
  if (a == NULL && b == NULL) return true;
  if (a == NULL || b == NULL) return false;
  if (a->hasRef != b->hasRef) return false;
  if (!a->hasRef) return true;
  return strcmp(a->refDbName, b->refDbName) == 0 &&
         strcmp(a->refTableName, b->refTableName) == 0 &&
         strcmp(a->refColName, b->refColName) == 0;
}

bool tagValueEqual(const STagValue *a, const STagValue *b) {
  if (a == NULL && b == NULL) return true;
  if (a == NULL || b == NULL) return false;
  if (a->type != b->type) return false;
  if (a->nLen != b->nLen) return false;
  if (a->nLen == 0) return true;
  if (a->pData == NULL || b->pData == NULL) return a->pData == b->pData;
  return memcmp(a->pData, b->pData, a->nLen) == 0;
}

// Sliced re-check tuning: every STREAM_VTB_RECHECK_INTERVAL_MS scans at most
// STREAM_VTB_RECHECK_SLICE_SIZE uids. A full sweep of N uids therefore takes
// roughly ceil(N / SLICE_SIZE) * INTERVAL_MS. With INTERVAL=1000 ms and
// SLICE=1000, up to 1000 uids/sec are verified per vnode.
#define STREAM_VTB_RECHECK_INTERVAL_MS 1000
#define STREAM_VTB_RECHECK_SLICE_SIZE  1000
#define STREAM_VTB_RPC_TIMEOUT_MS      30000

// Throttled hook called at the entry of every WAL meta request.
// On tag change: returns TSDB_CODE_STREAM_VTB_TAG_CHANGED so caller bails out fast.
// On col-only change: appends affected uids into rsp->tableBlock as TABLE_BLOCK_ADD.
// All other cases: returns 0 and lets caller continue normal processing.
//
// Locking discipline: the resolver round-trip (RPC + tsem2_timewait) is expensive
// and MUST run outside the cache W-latch, otherwise WAL meta processing and
// the foreground vtable-info request path stall on every recheck tick. The
// hook therefore splits work into three phases:
//   1) under lock: throttle check + snapshot of reqColCids/reqTagCids and the
//      slice uid list, advance the slice cursor, and claim lastCheckMs = now
//      so concurrent callers see the throttle and skip;
//   2) without lock: streamResolveVTableRefChain over the snapshot;
//   3) under lock: diff the resolved result against the live cache and apply
//      M1-style per-uid updates / fail-fast on tag changes.
// Phase 1 of streamMaybeRecheckVTableCache: take the W-lock, do the double-
// checked throttle, refill uidSlice if the cursor wrapped, snapshot the
// current slice + reqCol/Tag cids, then advance the cursor and claim the
// throttle slot. Drops the lock before returning.
//
// On success returns 0 and:
//   *ppSliceUids != NULL when work should proceed (caller owns and frees).
//   *ppSliceUids == NULL when the recheck was throttled or the cache was
//                  empty -- caller should treat this as a no-op success.
// On failure returns the error code; all out-params are left NULL / 0.
static int32_t streamRecheckTakeSlice(SVnode *pVnode, SStreamVTableInfoCache *pCache,
                                      SStreamTriggerReaderInfo *pInfo,
                                      SArray **ppSliceUids, SArray **ppReqColCids,
                                      SArray **ppReqTagCids, int32_t *pBegin,
                                      int32_t *pEnd, int32_t *pTotal) {
  int32_t code       = 0;
  SArray *sliceUids  = NULL;
  SArray *reqColCids = NULL;
  SArray *reqTagCids = NULL;
  int32_t begin = 0, end = 0, total = 0;

  *ppSliceUids  = NULL;
  *ppReqColCids = NULL;
  *ppReqTagCids = NULL;
  *pBegin = *pEnd = *pTotal = 0;

  taosWLockLatch(&pCache->lock);

  // Double-checked throttle: concurrent caller may have just done a sweep.
  int64_t now = taosGetTimestampMs();
  if (now - pCache->lastCheckMs < STREAM_VTB_RECHECK_INTERVAL_MS) {
    taosWUnLockLatch(&pCache->lock);
    return 0;
  }

  // Refill uidSlice whenever the cursor wraps to 0 so newly registered vtables
  // (not yet in uid2Result) are picked up by the next sweep.
  if (pCache->sliceCursor == 0) {
    taosArrayClear(pCache->uidSlice);
    SArray *pTableListArray = qStreamGetTableArrayList(pInfo);
    if (pTableListArray == NULL) {
      taosWUnLockLatch(&pCache->lock);
      return terrno;
    }
    int32_t nAll = (int32_t)taosArrayGetSize(pTableListArray);
    for (int32_t i = 0; i < nAll; ++i) {
      SStreamTableKeyInfo *pKey = taosArrayGetP(pTableListArray, i);
      if (pKey == NULL || pKey->markedDeleted) continue;
      if (taosArrayPush(pCache->uidSlice, &pKey->uid) == NULL) {
        code = terrno;
        taosArrayDestroyP(pTableListArray, taosMemFree);
        goto _unlock;
      }
    }
    taosArrayDestroyP(pTableListArray, taosMemFree);
  }

  total = (int32_t)taosArrayGetSize(pCache->uidSlice);
  if (total == 0) {
    pCache->lastCheckMs = taosGetTimestampMs();
    taosWUnLockLatch(&pCache->lock);
    stDebug("vgId:%d %s skip: cache empty", TD_VID(pVnode), __func__);
    return 0;
  }

  begin = pCache->sliceCursor;
  end   = TMIN(begin + STREAM_VTB_RECHECK_SLICE_SIZE, total);
  sliceUids = taosArrayInit(end - begin, sizeof(int64_t));
  if (sliceUids == NULL) { code = terrno; goto _unlock; }
  for (int32_t i = begin; i < end; ++i) {
    if (taosArrayPush(sliceUids, taosArrayGet(pCache->uidSlice, i)) == NULL) {
      code = terrno;
      goto _unlock;
    }
  }

  if (pCache->reqColCids != NULL) {
    reqColCids = taosArrayDup(pCache->reqColCids, NULL);
    if (reqColCids == NULL) { code = terrno; goto _unlock; }
  }
  if (pCache->reqTagCids != NULL) {
    reqTagCids = taosArrayDup(pCache->reqTagCids, NULL);
    if (reqTagCids == NULL) { code = terrno; goto _unlock; }
  }

  // Advance cursor and claim the throttle slot so concurrent callers skip.
  pCache->sliceCursor = (end >= total) ? 0 : end;
  pCache->lastCheckMs = taosGetTimestampMs();

_unlock:
  taosWUnLockLatch(&pCache->lock);
  if (code != 0) {
    taosArrayDestroy(sliceUids);
    taosArrayDestroy(reqColCids);
    taosArrayDestroy(reqTagCids);
    return code;
  }
  *ppSliceUids  = sliceUids;
  *ppReqColCids = reqColCids;
  *ppReqTagCids = reqTagCids;
  *pBegin = begin; *pEnd = end; *pTotal = total;
  return 0;
}

// Phase 3 per-uid body of streamMaybeRecheckVTableCache: diff the freshly
// resolved newRes against the cached oldRes for one uid; on tag change return
// TSDB_CODE_STREAM_VTB_TAG_CHANGED so the caller bails out of the loop; on
// col change append uid to changedUids; otherwise replace the cache entry
// with newRes (ownership transferred from uid2Result).
//
// Must be called with pCache->lock held in write mode (caller's responsibility).
static int32_t streamRecheckDiffAndApplyOneUid(SVnode *pVnode, SStreamVTableInfoCache *pCache,
                                               int64_t uid, SSHashObj *uid2Result,
                                               SArray *changedUids) {
  SVTableResolveResult **ppNew = (SVTableResolveResult **)tSimpleHashGet(uid2Result, &uid, sizeof(uid));
  SVTableResolveResult **ppOld = (SVTableResolveResult **)tSimpleHashGet(pCache->uid2Result, &uid, sizeof(uid));
  SVTableResolveResult  *newRes = (ppNew == NULL) ? NULL : *ppNew;
  SVTableResolveResult  *oldRes = (ppOld == NULL) ? NULL : *ppOld;

  // uid skipped by resolver (top-level vtable dropped, H2 fallback) -> drop from cache.
  if (newRes == NULL) {
    if (oldRes != NULL) {
      stDebug("vgId:%d %s uid dropped: uid=%" PRId64, TD_VID(pVnode), __func__, uid);
      int32_t rc = tSimpleHashRemove(pCache->uid2Result, &uid, sizeof(uid));
      if (rc != 0) {
        stWarn("vgId:%d %s remove uid=%" PRId64 " from cache failed: 0x%x",
               TD_VID(pVnode), __func__, uid, rc);
      }
    }
    return 0;
  }

  // Tag diff -- any tag change is fatal.
  bool tagChanged = false;
  if (oldRes != NULL && oldRes->tagMap != NULL) {
    void *it2 = NULL; int32_t i2 = 0;
    while ((it2 = tSimpleHashIterate(oldRes->tagMap, it2, &i2)) != NULL) {
      col_id_t   cid  = *(col_id_t *)tSimpleHashGetKey(it2, NULL);
      STagValue *oldV = *(STagValue **)it2;
      STagValue **ppNewV = (newRes->tagMap == NULL) ? NULL :
                           (STagValue **)tSimpleHashGet(newRes->tagMap, &cid, sizeof(cid));
      STagValue  *newV   = (ppNewV == NULL) ? NULL : *ppNewV;
      if (!tagValueEqual(oldV, newV)) {
        stDebug("vgId:%d %s tag changed: uid=%" PRId64 " cid=%d", TD_VID(pVnode), __func__,
                uid, (int32_t)cid);
        tagChanged = true;
        break;
      }
    }
  }
  if (tagChanged) return TSDB_CODE_STREAM_VTB_TAG_CHANGED;

  // Col diff -- collect uids that need re-publication.
  bool colChanged = false;
  if (oldRes != NULL && oldRes->colMap != NULL) {
    void *it2 = NULL; int32_t i2 = 0;
    while ((it2 = tSimpleHashIterate(oldRes->colMap, it2, &i2)) != NULL) {
      col_id_t          cid  = *(col_id_t *)tSimpleHashGetKey(it2, NULL);
      SColResolveItem  *oldI = *(SColResolveItem **)it2;
      SColResolveItem **ppNewI = (newRes->colMap == NULL) ? NULL :
                                 (SColResolveItem **)tSimpleHashGet(newRes->colMap, &cid, sizeof(cid));
      SColResolveItem  *newI   = (ppNewI == NULL) ? NULL : *ppNewI;
      if (!colResolveItemEqual(oldI, newI)) { colChanged = true; break; }
    }
  }
  if (colChanged) {
    if (taosArrayPush(changedUids, &uid) == NULL) return terrno;
  }

  // Replace cache entry with the freshly resolved result; transfer ownership.
  if (tSimpleHashPut(pCache->uid2Result, &uid, sizeof(uid), &newRes, POINTER_BYTES) != 0) {
    return terrno;
  }

  streamVTableResolveResultDestroy(&oldRes);
  *ppNew = NULL;
  return 0;
}

int32_t streamMaybeRecheckVTableCache(SVnode *pVnode, SStreamTriggerReaderInfo *pInfo,
                                             int64_t walVer, SSTriggerWalNewRsp *pRsp) {
  if (pInfo == NULL || pInfo->vtbCache == NULL || !pInfo->vtbCache->valid) {
    return 0;
  }
  SStreamVTableInfoCache *pCache = pInfo->vtbCache;
  // Throttle check is done under lock in streamRecheckTakeSlice (Phase 1).

  int32_t    code         = 0;
  SArray    *sliceUids    = NULL;
  SArray    *reqColCids   = NULL;
  SArray    *reqTagCids   = NULL;
  SArray    *changedUids  = NULL;
  SSHashObj *uid2Result   = NULL;
  int32_t    begin = 0, end = 0, total = 0;

  // ---- Phase 1: snapshot under lock ----
  code = streamRecheckTakeSlice(pVnode, pCache, pInfo, &sliceUids, &reqColCids, &reqTagCids,
                                &begin, &end, &total);
  if (code != 0) goto _cleanup;
  if (sliceUids == NULL) return 0;  // throttled or empty cache

  stDebug("vgId:%d %s walVer=%" PRId64 " total=%d slice=[%d,%d)",
          TD_VID(pVnode), __func__, walVer, total, begin, end);

  // ---- Phase 2: resolver round-trip, no lock held ----
  code = streamResolveVTableRefChain(pVnode, pCache, pInfo, walVer, sliceUids,
                                     reqColCids, reqTagCids, &uid2Result);
  if (code != 0) goto _cleanup;

  changedUids = taosArrayInit(0, sizeof(int64_t));
  if (changedUids == NULL) { code = terrno; goto _cleanup; }

  // ---- Phase 3: diff + apply under lock ----
  taosWLockLatch(&pCache->lock);
  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(sliceUids); ++i) {
    int64_t uid = *(int64_t *)taosArrayGet(sliceUids, i);
    code = streamRecheckDiffAndApplyOneUid(pVnode, pCache, uid, uid2Result, changedUids);
    if (code != 0) break;
  }
  pCache->lastCheckMs = taosGetTimestampMs();
  taosWUnLockLatch(&pCache->lock);

_cleanup:
  tSimpleHashCleanup(uid2Result);
  taosArrayDestroy(sliceUids);
  taosArrayDestroy(reqColCids);
  taosArrayDestroy(reqTagCids);

  if (code == TSDB_CODE_STREAM_VTB_TAG_CHANGED) {
    stWarn("vgId:%d %s tag changed, abort fast walVer=%" PRId64, TD_VID(pVnode), __func__, walVer);
    taosArrayDestroy(changedUids);
    return code;
  }
  if (code != 0) {
    stError("vgId:%d %s recheck failed since %s", TD_VID(pVnode), __func__, tstrerror(code));
    taosArrayDestroy(changedUids);
    return code;
  }
  if (pRsp != NULL && changedUids != NULL && taosArrayGetSize(changedUids) > 0) {
    int32_t rc = addUidListToBlock(changedUids, &pRsp->tableBlock, walVer, &pRsp->totalRows, TABLE_BLOCK_ADD);
    stDebug("vgId:%d %s appended %d changed uids walVer=%" PRId64, TD_VID(pVnode), __func__,
            (int32_t)taosArrayGetSize(changedUids), walVer);
    if (rc != 0) { taosArrayDestroy(changedUids); return rc; }
  }
  taosArrayDestroy(changedUids);
  return 0;
}

// ---------------------------------------------------------------------------
// Block C: vtable chain resolution
//
// This block has THREE sub-sections that should be kept distinct. Do NOT call
// internal helpers across sub-section boundaries; the only legal entry points
// across sub-sections are the public top-level functions listed below.
//
//   C1 (server)   - TDMT_VND_VTABLE_REF_RESOLVE single-hop server handler.
//                   Public entry: vnodeProcessVTableRefResolveReq.
//                   Statics:      vnodeFindVTableColRef, vnodeResolveTableGroup,
//                                 vnodeResolveOneHop (also reused by C2 local fast-path),
//                                 vnodeFillTagValueFromChild.
//
//   C2 (driver)   - multi-hop client driver: fans out per-vgId RPCs, walks the
//                   chain, integrates with reader tblRefCache / dbVgInfo cache.
//                   Public entry: streamResolveVTableRefChain.
//
//   C3 (executor) - vchild-tag chain helper used by executor tag-ref scans.
//                   Public entry: vnodeResolveVTableTagChain.
//
// If a new RPC type is added in the future, prefer splitting C1 / C2 into
// dedicated files (vnodeStreamVTableRpc.c / vnodeStreamVTableDriver.c) over
// growing this file further.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// C1: single-hop RPC server (TDMT_VND_VTABLE_REF_RESOLVE)
// ---------------------------------------------------------------------------

// ============================================================================
// TDMT_VND_VTABLE_REF_RESOLVE — single-hop chain resolver for vtable references.
//
// Caller (driver A in stream-trigger reader info path) groups per-batch refs by
// vgId and sends one request per vgId. Each item carries (kind, refDb, refTbl,
// refCol). For every item we do exactly ONE hop:
//   - table not on this vnode               -> r.code = STREAM_VTB_REF_TABLE_NOT_EXIST
//   - column/tag name not found             -> r.code = STREAM_VTB_REF_COL_NOT_EXIST
//   - vtable + COL + hasRef                 -> r.terminated = false, r.nextRef = stored ref
//   - vtable + COL + !hasRef                -> r.terminated = true,  r.nextRef.hasRef = false
//                                              (terminal triple is meaningless; signals NULL value)
//   - vtable + TAG + hasRef                 -> r.terminated = false, r.nextRef = stored ref
//   - vchild + TAG + !hasRef                -> r.terminated = true,  r.nextRef.hasRef = false,
//                                              r.tagType/tagLen/tagData filled from local STag
//   - vnormal+ TAG + !hasRef                -> r.code = STREAM_VTB_REF_COL_NOT_EXIST
//                                              (normal vtable has no tag concept)
//   - physical table  + COL kind            -> r.terminated = true,  r.nextRef = current triple
//   - child table     + TAG kind            -> r.terminated = true,  r.tagType/tagLen/tagData filled
//   - normal table    + TAG kind            -> r.code = STREAM_VTB_REF_COL_NOT_EXIST
// Per-item errors never abort the batch — they are reported in r.code.
// ============================================================================

// Reads a tag's constant value from a (virtual or physical) child table entry.
// The stable schema (for type/colId lookup) is fetched here under META_READER_LOCK;
// the child entry is provided by the caller.
//   pVnode      : owning vnode
//   pChildEntry : decoded entry whose type is *_CHILD_TABLE (carries ctbEntry.suid/pTags)
//   tagColName  : tag name on the stable (vchild's SColRef.colName matches stable tag name
//                 by build-time convention)
// Outputs:
//   *outType    : tag SDataType
//   *outLen     : payload length; 0 when tag absent on this child
//   *outData    : newly allocated buffer (caller frees); NULL when *outLen==0
// Returns:
//   0                                          success (incl. "tag absent")
//   TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST   suid not present on this vnode
//   TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST     tag name not in stable schema
//   terrno                                     OOM
// Internal helper: read a constant tag value from a virtual child table.
// Tag is located in the parent stable's schemaTag by either colId (preferred when > 0)
// or by colName (fallback). vtable on-disk SColRef does not persist colName, so callers
// holding only a SColRef entry must pass the cid.
static int32_t streamReadChildTagConstValueImpl(SVnode *pVnode, const SMetaEntry *pChildEntry,
                                                col_id_t tagColId, const char *tagColName,
                                                int8_t *outType, int32_t *outLen, char **outData) {
  SMetaReader stb  = {0};
  int32_t     code = 0;
  *outType = 0;
  *outLen  = 0;
  *outData = NULL;

  metaReaderDoInit(&stb, pVnode->pMeta, META_READER_LOCK, 0);
  if (metaReaderGetTableEntryByUid(&stb, pChildEntry->ctbEntry.suid) != 0) {
    code = TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST;
    goto _end;
  }

  SSchemaWrapper *pSW = &stb.me.stbEntry.schemaTag;
  SSchema        *pTagSchema = NULL;
  for (int32_t i = 0; i < pSW->nCols; ++i) {
    if (tagColId > 0) {
      if (pSW->pSchema[i].colId == tagColId) {
        pTagSchema = &pSW->pSchema[i];
        break;
      }
    } else if (tagColName != NULL &&
               strncmp(pSW->pSchema[i].name, tagColName, TSDB_COL_NAME_LEN) == 0) {
      pTagSchema = &pSW->pSchema[i];
      break;
    }
  }
  if (pTagSchema == NULL) {
    code = TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST;
    goto _end;
  }

  *outType = pTagSchema->type;

  STag   *pTag  = (STag *)pChildEntry->ctbEntry.pTags;
  STagVal tv    = {.cid = pTagSchema->colId, .type = pTagSchema->type};
  bool    found = (pTag != NULL) && tTagGet(pTag, &tv);
  if (!found) {
    // tag has no value on this child: outLen=0 / outData=NULL
    goto _end;
  }

  if (IS_VAR_DATA_TYPE(pTagSchema->type)) {
    *outLen = (int32_t)tv.nData;
    if (*outLen > 0) {
      *outData = taosMemoryMalloc(*outLen);
      if (*outData == NULL) { code = terrno; goto _end; }
      memcpy(*outData, tv.pData, *outLen);
    }
  } else {
    *outLen  = (int32_t)tDataTypes[pTagSchema->type].bytes;
    *outData = taosMemoryMalloc(*outLen);
    if (*outData == NULL) { code = terrno; goto _end; }
    memcpy(*outData, &tv.i64, *outLen);
  }

_end:
  metaReaderClear(&stb);
  return code;
}

// Look up by name (used by request-driven path where wire holds refColName).
static int32_t streamReadChildTagConstValue(SVnode *pVnode, const SMetaEntry *pChildEntry,
                                            const char *tagColName, int8_t *outType,
                                            int32_t *outLen, char **outData) {
  return streamReadChildTagConstValueImpl(pVnode, pChildEntry, 0, tagColName,
                                          outType, outLen, outData);
}

// Look up by cid (used by local seed where SColRef.colName is not persisted).
static int32_t streamReadChildTagConstValueByCid(SVnode *pVnode, const SMetaEntry *pChildEntry,
                                                 col_id_t tagColId, int8_t *outType,
                                                 int32_t *outLen, char **outData) {
  return streamReadChildTagConstValueImpl(pVnode, pChildEntry, tagColId, NULL,
                                          outType, outLen, outData);
}

static int32_t vnodeFillTagValueFromChild(SVnode *pVnode, const SMetaEntry *pChildEntry,
                                          const char *tagColName, SVTableRefResolveRspItem *r) {
  r->terminated = true;
  int32_t code = streamReadChildTagConstValue(pVnode, pChildEntry, tagColName,
                                              &r->tagType, &r->tagLen, &r->tagData);
  vDebug("vgId:%d %s tag=%s code=0x%x type=%d len=%d", TD_VID(pVnode), __func__, tagColName, code,
         r->tagType, r->tagLen);
  if (code == TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST ||
      code == TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST) {
    // per-item soft error: surface via r->code, do not abort the batch.
    r->code = code;
    return 0;
  }
  return code;
}

// Lookup the SColRef on a vtable for (kind, colName), returning a pointer into
// the vtable's pColRef/pTagRef array (or NULL when the column name is not in
// the schema — this is a per-item soft miss, not a function error).
//
// For VIRTUAL_CHILD_TABLE the parent stable's schema is needed to translate
// colName -> cid. If the caller has already opened the parent stable entry
// (e.g. to share it across multiple columns of the same vchild), it can pass
// pStbEntry to avoid the extra meta read; otherwise pass NULL and the helper
// will open & close a temporary reader internally.
//
// Returns: 0 on success (*ppFoundRef set, may be NULL).
//          TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST when pStbEntry==NULL and
//          the parent stable cannot be opened — the caller should surface this
//          via the per-item rsp.code.
static int32_t vnodeFindVTableColRef(SVnode *pVnode, const SMetaEntry *pVtbEntry,
                                     const SMetaEntry *pStbEntry, EStreamVRefKind kind,
                                     const char *colName, SColRef **ppFoundRef) {
  *ppFoundRef = NULL;
  if (pVtbEntry->type != TSDB_VIRTUAL_NORMAL_TABLE &&
      pVtbEntry->type != TSDB_VIRTUAL_CHILD_TABLE) {
    return 0;
  }

  const SColRefWrapper *pWrap = &pVtbEntry->colRef;
  SColRef              *pArr  = (kind == STREAM_VREF_KIND_TAG) ? pWrap->pTagRef : pWrap->pColRef;
  int32_t               nArr  = (kind == STREAM_VREF_KIND_TAG) ? pWrap->nTagRefs : pWrap->nCols;

  SMetaReader           tmpStb    = {0};
  bool                  tmpInited = false;
  const SSchemaWrapper *pSW       = NULL;

  if (pVtbEntry->type == TSDB_VIRTUAL_NORMAL_TABLE) {
    // Normal vtable: schema lives on the vtable entry itself.
    pSW = &pVtbEntry->ntbEntry.schemaRow;
  } else if (pStbEntry != NULL) {
    // Caller pre-loaded parent stable — reuse it.
    pSW = (kind == STREAM_VREF_KIND_TAG) ? &pStbEntry->stbEntry.schemaTag
                                         : &pStbEntry->stbEntry.schemaRow;
  } else {
    // Open parent stable on the fly.
    metaReaderDoInit(&tmpStb, pVnode->pMeta, META_READER_LOCK, 0);
    if (metaReaderGetTableEntryByUid(&tmpStb, pVtbEntry->ctbEntry.suid) != 0) {
      vDebug("vgId:%d %s parent stable not found: suid=%" PRId64, TD_VID(pVnode), __func__,
             pVtbEntry->ctbEntry.suid);
      metaReaderClear(&tmpStb);
      return TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST;
    }
    tmpInited = true;
    pSW = (kind == STREAM_VREF_KIND_TAG) ? &tmpStb.me.stbEntry.schemaTag
                                         : &tmpStb.me.stbEntry.schemaRow;
  }

  // Step 1: resolve colName -> cid using the chosen schema wrapper.
  col_id_t targetCid = 0;
  bool     cidFound  = false;
  for (int32_t k = 0; pSW != NULL && k < pSW->nCols; ++k) {
    if (strncmp(pSW->pSchema[k].name, colName, TSDB_COL_NAME_LEN) == 0) {
      targetCid = pSW->pSchema[k].colId;
      cidFound  = true;
      break;
    }
  }

  // Step 2: scan the vtable's ref array for that cid.
  if (cidFound) {
    for (int32_t j = 0; j < nArr && pArr != NULL; ++j) {
      if (pArr[j].id == targetCid) {
        *ppFoundRef = &pArr[j];
        break;
      }
    }
  }

  if (tmpInited) metaReaderClear(&tmpStb);
  return 0;
}

// Fill a SVTableRefResolveRspItem based on the resolved SColRef (or lack thereof).
// Handles all vtable/physical-table × COL/TAG × hasRef/!hasRef combinations in
// one place so that both vnodeResolveTableGroup and vnodeResolveOneHop share the
// same branching logic without duplication.
//
// Parameters:
//   pVnode    - vnode handle (for vnodeFillTagValueFromChild)
//   pEntry   - the meta entry of the table being resolved
//   pFound   - resolved SColRef, may be NULL (col not found)
//   kind     - STREAM_VREF_KIND_COL or STREAM_VREF_KIND_TAG
//   dbName   - database name for the physical-table terminal triple
//   tableName- table name for the physical-table terminal triple
//   colName  - column name (for tag value lookup)
//   isVtable - whether the table is a virtual table
//   r        - output response item (caller zeroes it before call)
//
// Returns 0 on success, or an error code for fatal failures (e.g. OOM in tag
// fill). Logical resolution errors (col-not-exist etc.) are written into r->code
// and the function still returns 0.
static int32_t vnodeFillResolveRspFromColRef(SVnode *pVnode, const SMetaEntry *pEntry,
                                             SColRef *pFound, int8_t kind,
                                             const char *dbName, const char *tableName,
                                             const char *colName, bool isVtable,
                                             SVTableRefResolveRspItem *r) {
  if (isVtable) {
    if (pFound == NULL) {
      r->code = TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST;
      return 0;
    }
    if (pFound->hasRef) {
      r->terminated     = false;
      r->nextRef.kind   = kind;
      r->nextRef.hasRef = true;
      tstrncpy(r->nextRef.refDbName,    pFound->refDbName,    TSDB_DB_NAME_LEN);
      tstrncpy(r->nextRef.refTableName, pFound->refTableName, TSDB_TABLE_NAME_LEN);
      tstrncpy(r->nextRef.refColName,   pFound->refColName,   TSDB_COL_NAME_LEN);
      return 0;
    }
    // !hasRef
    if (kind == STREAM_VREF_KIND_TAG) {
      if (pEntry->type != TSDB_VIRTUAL_CHILD_TABLE) {
        r->code = TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST;
        return 0;
      }
      r->nextRef.kind            = STREAM_VREF_KIND_TAG;
      r->nextRef.hasRef          = false;
      r->nextRef.refDbName[0]    = '\0';
      r->nextRef.refTableName[0] = '\0';
      r->nextRef.refColName[0]   = '\0';
      int32_t rc = vnodeFillTagValueFromChild(pVnode, pEntry, colName, r);
      if (rc != 0) r->code = rc;
      return 0;
    }
    // STREAM_VREF_KIND_COL on vtable with NULL ref: terminal empty
    r->terminated              = true;
    r->nextRef.kind            = STREAM_VREF_KIND_COL;
    r->nextRef.hasRef          = false;
    r->nextRef.refDbName[0]    = '\0';
    r->nextRef.refTableName[0] = '\0';
    r->nextRef.refColName[0]   = '\0';
    return 0;
  }

  // Physical table
  if (kind == STREAM_VREF_KIND_COL) {
    r->terminated     = true;
    r->nextRef.kind   = STREAM_VREF_KIND_COL;
    r->nextRef.hasRef = true;
    tstrncpy(r->nextRef.refDbName,    dbName,    TSDB_DB_NAME_LEN);
    tstrncpy(r->nextRef.refTableName, tableName, TSDB_TABLE_NAME_LEN);
    tstrncpy(r->nextRef.refColName,   colName,   TSDB_COL_NAME_LEN);
    return 0;
  }

  // TAG on physical table: only child table carries tag values
  if (pEntry->type != TSDB_CHILD_TABLE) {
    r->code = TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST;
    return 0;
  }
  r->nextRef.kind   = STREAM_VREF_KIND_TAG;
  r->nextRef.hasRef = false;
  int32_t rc = vnodeFillTagValueFromChild(pVnode, pEntry, colName, r);
  if (rc != 0) r->code = rc;
  return 0;
}

// Batch-resolve multiple columns within the same table. Opens meta once for the
// table, then resolves each (colName, kind) pair against the same metadata.
// Results are appended to pRspItems in the same order as pCols.
static int32_t vnodeResolveTableGroup(SVnode *pVnode, const char *dbName, const char *tableName,
                                      SArray *pCols, SArray *pRspItems) {
  SMetaReader mr   = {0};
  int32_t     code = 0;
  int32_t     nCols = (pCols != NULL) ? (int32_t)taosArrayGetSize(pCols) : 0;

  vDebug("vgId:%d %s enter: db=%s table=%s nCols=%d", TD_VID(pVnode), __func__, dbName, tableName, nCols);

  metaReaderDoInit(&mr, pVnode->pMeta, META_READER_LOCK, 0);
  if (metaGetTableEntryByName(&mr, tableName) != 0) {
    vDebug("vgId:%d %s ref table not exist: %s", TD_VID(pVnode), __func__, tableName);
    // Fill all columns with table-not-exist error
    for (int32_t i = 0; i < nCols; ++i) {
      SVTableRefResolveRspItem r = {0};
      r.code = TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST;
      if (taosArrayPush(pRspItems, &r) == NULL) { code = terrno; break; }
    }
    metaReaderClear(&mr);
    return code;
  }

  bool isVtable = (mr.me.type == TSDB_VIRTUAL_NORMAL_TABLE || mr.me.type == TSDB_VIRTUAL_CHILD_TABLE);

  // Release mr's meta read lock before opening any further LOCK readers below
  // (stbReader, and the per-column tag-value readers inside vnodeFillResolveRspFromColRef):
  // a nested META_READER_LOCK rdlock deadlocks once a writer is queued on the
  // meta rwlock (glibc blocks new readers behind a pending writer).
  // mr.me stays valid until metaReaderClear.
  metaReaderReleaseLock(&mr);

  // Pre-read parent stable info for virtual child table (shared across all columns)
  SMetaReader stbReader      = {0};
  bool        stbReaderInited = false;
  if (isVtable && mr.me.type == TSDB_VIRTUAL_CHILD_TABLE) {
    metaReaderDoInit(&stbReader, pVnode->pMeta, META_READER_LOCK, 0);
    if (metaReaderGetTableEntryByUid(&stbReader, mr.me.ctbEntry.suid) != 0) {
      // Parent stable not found: all columns fail
      for (int32_t i = 0; i < nCols; ++i) {
        SVTableRefResolveRspItem r = {0};
        r.code = TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST;
        if (taosArrayPush(pRspItems, &r) == NULL) { code = terrno; break; }
      }
      metaReaderClear(&stbReader);
      metaReaderClear(&mr);
      return code;
    }
    stbReaderInited = true;
    // Same as above: drop the lock before the per-column loop, which opens
    // further LOCK readers via vnodeFillResolveRspFromColRef. stbReader.me
    // stays valid until metaReaderClear.
    metaReaderReleaseLock(&stbReader);
  }

  for (int32_t ci = 0; ci < nCols; ++ci) {
    SVTableRefResolveColSpec *c = taosArrayGet(pCols, ci);
    SVTableRefResolveRspItem  r = {0};

    SColRef *pFound = NULL;
    if (isVtable) {
      (void)vnodeFindVTableColRef(pVnode, &mr.me, stbReaderInited ? &stbReader.me : NULL,
                                  c->kind, c->colName, &pFound);
    }
    (void)vnodeFillResolveRspFromColRef(pVnode, &mr.me, pFound, c->kind,
                                        dbName, tableName, c->colName, isVtable, &r);

    if (taosArrayPush(pRspItems, &r) == NULL) {
      taosMemoryFreeClear(r.tagData);
      code = terrno;
      break;
    }
  }

  if (stbReaderInited) metaReaderClear(&stbReader);
  metaReaderClear(&mr);
  return code;
}

static int32_t vnodeResolveOneHop(SVnode *pVnode, const SVTableRefResolveItem *q,
                                  SVTableRefResolveRspItem *r) {
  SMetaReader mr   = {0};
  int32_t     code = 0;

  vDebug("vgId:%d %s enter: kind=%d ref=%s.%s.%s", TD_VID(pVnode), __func__, q->kind, q->refDbName,
         q->refTableName, q->refColName);

  metaReaderDoInit(&mr, pVnode->pMeta, META_READER_LOCK, 0);
  if (metaGetTableEntryByName(&mr, q->refTableName) != 0) {
    vDebug("vgId:%d %s ref table not exist: %s", TD_VID(pVnode), __func__, q->refTableName);
    r->code = TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST;
    metaReaderClear(&mr);
    return 0;
  }

  bool isVtable = (mr.me.type == TSDB_VIRTUAL_NORMAL_TABLE || mr.me.type == TSDB_VIRTUAL_CHILD_TABLE);
  vDebug("vgId:%d %s table found: name=%s type=%d isVtable=%d", TD_VID(pVnode), __func__,
         q->refTableName, mr.me.type, isVtable);

  // Release mr's meta read lock before the nested LOCK readers opened by
  // vnodeFindVTableColRef (tmpStb) and vnodeFillResolveRspFromColRef
  // (streamReadChildTagConstValueImpl): a nested rdlock deadlocks once a writer
  // is queued on the meta rwlock. mr.me stays valid until metaReaderClear.
  metaReaderReleaseLock(&mr);

  if (isVtable) {
    // Lookup the SColRef via shared helper. Pass NULL for pStbEntry — for
    // single-hop the per-call parent-stable open cost is acceptable.
    SColRef *pFound = NULL;
    int32_t  rc     = vnodeFindVTableColRef(pVnode, &mr.me, NULL, q->kind, q->refColName, &pFound);
    if (rc != 0) {
      r->code = rc;
      metaReaderClear(&mr);
      return 0;
    }

    (void)vnodeFillResolveRspFromColRef(pVnode, &mr.me, pFound, q->kind,
                                        q->refDbName, q->refTableName, q->refColName,
                                        true, r);
    metaReaderClear(&mr);
    return 0;
  }

  // Physical table
  (void)vnodeFillResolveRspFromColRef(pVnode, &mr.me, NULL, q->kind,
                                      q->refDbName, q->refTableName, q->refColName,
                                      false, r);
  metaReaderClear(&mr);
  return 0;
}

int32_t vnodeProcessVTableRefResolveReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t              code   = 0;
  int32_t              rspLen = 0;
  void                *pBuf   = NULL;
  SVTableRefResolveReq req    = {0};
  SVTableRefResolveRsp rsp    = {0};
  SRpcMsg              rspMsg = {0};

  vTrace("vgId:%d %s enter: contLen=%d msgType=%d", TD_VID(pVnode), __func__, pMsg->contLen,
         pMsg->msgType);

  if (tDeserializeSVTableRefResolveReq((char *)pMsg->pCont + sizeof(SMsgHead),
                                       pMsg->contLen - (int32_t)sizeof(SMsgHead), &req) < 0) {
    vError("vgId:%d %s deserialize failed", TD_VID(pVnode), __func__);
    code = TSDB_CODE_INVALID_MSG;
    goto _end;
  }

  {
    // Table-grouped format: resolve per-table batch (meta opened once per table)
    int32_t nGroups = (req.groups != NULL) ? (int32_t)taosArrayGetSize(req.groups) : 0;
    // Count total columns across all groups for pre-allocation
    int32_t totalCols = 0;
    for (int32_t i = 0; i < nGroups; ++i) {
      SVTableRefResolveGroupItem *g = taosArrayGet(req.groups, i);
      totalCols += (g->cols != NULL) ? (int32_t)taosArrayGetSize(g->cols) : 0;
    }
    vTrace("vgId:%d %s req: ver=%" PRId64 " groups=%d totalCols=%d",
           TD_VID(pVnode), __func__, req.ver, nGroups, totalCols);

    rsp.items = taosArrayInit(totalCols, sizeof(SVTableRefResolveRspItem));
    if (rsp.items == NULL) { code = terrno; goto _end; }

    for (int32_t i = 0; i < nGroups; ++i) {
      SVTableRefResolveGroupItem *g = taosArrayGet(req.groups, i);
      int32_t rc = vnodeResolveTableGroup(pVnode, g->dbName, g->tableName, g->cols, rsp.items);
      if (rc != 0) {
        code = rc;
        goto _end;
      }
    }
  }

  rspLen = tSerializeSVTableRefResolveRsp(NULL, 0, &rsp);
  if (rspLen < 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }
  pBuf = rpcMallocCont(rspLen);
  if (pBuf == NULL) {
    code = terrno;
    goto _end;
  }
  if (tSerializeSVTableRefResolveRsp(pBuf, rspLen, &rsp) < 0) {
    rpcFreeCont(pBuf);
    pBuf = NULL;
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }

_end:
  tFreeSVTableRefResolveReq(&req);
  tFreeSVTableRefResolveRsp(&rsp);

  rspMsg.info    = pMsg->info;
  rspMsg.pCont   = (code == 0) ? pBuf : NULL;
  rspMsg.contLen = (code == 0) ? rspLen : 0;
  rspMsg.code    = code;
  rspMsg.msgType = pMsg->msgType;

  if (code != 0) {
    vError("vgId:%d, vtable ref resolve failed since %s", TD_VID(pVnode), tstrerror(code));
    if (pBuf != NULL) rpcFreeCont(pBuf);
  }

  vDebug("vgId:%d %s send rsp: code=0x%x rspLen=%d", TD_VID(pVnode), __func__, code, rspMsg.contLen);
  tmsgSendRsp(&rspMsg);
  return 0;
}

// ============================================================================
// C2: multi-hop chain driver (client side)
// Task 6: chain resolution loop (single-vgId / local-vnode simplified version)
// ============================================================================

#define STREAM_VTB_MAX_HOPS 32

static void freeColMap(void* ptr) {
  taosMemoryFree(*(void**)ptr); 
}

static void freeTagMap(void* ptr) {
  STagValue **pp = (STagValue **)ptr;
  if (*pp) taosMemoryFreeClear((*pp)->pData);
  taosMemoryFreeClear(*pp);
}

// (SResolveWorkItem moved to vnodeStreamVTable.h for testability.)
static SVTableResolveResult *streamGetOrCreateUidResult(SSHashObj *uid2Result, int64_t uid) {
  SVTableResolveResult **ppRes = (SVTableResolveResult **)tSimpleHashGet(uid2Result, &uid, sizeof(uid));
  if (ppRes != NULL && *ppRes != NULL) {
    return *ppRes;
  }

  SVTableResolveResult *pRes = taosMemoryCalloc(1, sizeof(*pRes));
  if (pRes == NULL) return NULL;
  pRes->colMap = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));
  pRes->tagMap = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));

  if (pRes->colMap == NULL || pRes->tagMap == NULL) {
    streamVTableResolveResultDestroy(&pRes);
    return NULL;
  }
  tSimpleHashSetFreeFp(pRes->colMap, freeColMap);
  tSimpleHashSetFreeFp(pRes->tagMap, freeTagMap);

  if (tSimpleHashPut(uid2Result, &uid, sizeof(uid), &pRes, sizeof(pRes)) != 0) {
    streamVTableResolveResultDestroy(&pRes);
    return NULL;
  }
  return pRes;
}

// Push initial work-items for a single vtable uid. Each requested cid (col or tag)
// is resolved against the local vtable entry's pColRef / pTagRef:
//   - COL hasRef=true   -> push next-hop work-item
//   - COL hasRef=false  -> directly write terminal SColResolveItem{hasRef=false} into colMap
//   - TAG hasRef=true   -> push next-hop work-item
//   - TAG hasRef=false  -> on a virtual child table the tag may be stored as a
//                          constant value on the vchild's own STag; read it locally
//                          and write a terminal STagValue into tagMap (no work-item).
//                          Virtual normal tables have no tag concept and fail.
//
// colCids == NULL means "all columns of this vtable" (used by the only-ts trigger
// path where the request carries just the primary-key TS but the response must
// describe every column ref). tagCids == NULL is treated as "no tag".
// Returns 0 on success; non-zero means whole-uid skip (table missing / cid missing / OOM).
static int32_t streamPushInitialWorkItemsForUid(SVnode *pVnode, int64_t uid, SArray *colCids, SArray *tagCids,
                                                SArray *workList, SSHashObj *uid2Result) {
  int32_t     code = 0;
  SMetaReader mr   = {0};
  metaReaderDoInit(&mr, pVnode->pMeta, META_READER_LOCK, 0);

  // H2 v0.5: top-level vtable uid not present in local meta (concurrently
  // dropped) or entry type is not a vtable. Treat as a soft skip: log a
  // warning and return 0 without producing any uid2Result entry. The caller
  // (streamResolveVTableRefChain seed loop) sees rc==0 and simply continues;
  // downstream consumers that strictly require this uid (e.g. PSEUDO_COL
  // single-uid path) detect the missing entry and raise the error.
  if (metaReaderGetTableEntryByUid(&mr, uid) != 0) {
    stWarn("vgId:%d %s uid=%" PRId64 " META_NOT_FOUND -> H2 skip", TD_VID(pVnode), __func__, uid);
    goto _end;
  }
  if (mr.me.type != TSDB_VIRTUAL_NORMAL_TABLE && mr.me.type != TSDB_VIRTUAL_CHILD_TABLE) {
    stWarn("vgId:%d %s uid=%" PRId64 " type=%d not vtable -> H2 skip",
           TD_VID(pVnode), __func__, uid, mr.me.type);
    goto _end;
  }

  // Release mr's meta read lock: the tag loop below opens a nested LOCK reader
  // via streamReadChildTagConstValueByCid, and a nested rdlock deadlocks once a
  // writer is queued on the meta rwlock. mr.me stays valid until metaReaderClear.
  metaReaderReleaseLock(&mr);

  SVTableResolveResult *pRes = streamGetOrCreateUidResult(uid2Result, uid);
  if (pRes == NULL) {
    code = terrno;
    goto _end;
  }

  // Resolve column cids against pColRef. When colCids==NULL, iterate every
  // entry of this vtable's pColRef directly (no per-cid lookup needed).
  if (colCids == NULL) {
    for (int32_t j = 0; j < mr.me.colRef.nCols; ++j) {
      SColRef *pRef = &mr.me.colRef.pColRef[j];
      col_id_t cid  = pRef->id;
      if (!pRef->hasRef) {
        SColResolveItem *item = taosMemoryCalloc(1, sizeof(*item));
        if (item == NULL) { code = terrno; goto _end; }
        item->hasRef = false;
        // Snapshot old pointer before put; free it only after successful put so
        // the hash never holds a dangling pointer (avoids double-free on cleanup).
        SColResolveItem **ppOld = (SColResolveItem **)tSimpleHashGet(pRes->colMap, &cid, sizeof(cid));
        SColResolveItem  *oldItem = (ppOld && *ppOld) ? *ppOld : NULL;
        if (tSimpleHashPut(pRes->colMap, &cid, sizeof(cid), &item, sizeof(item)) != 0) {
          taosMemoryFree(item);
          code = terrno;
          goto _end;
        }
        if (oldItem) { taosMemoryFree(oldItem); }
        continue;
      }
      SResolveWorkItem w = {0};
      w.originVtbUid = uid;
      w.originCid    = cid;
      w.kind         = STREAM_VREF_KIND_COL;
      tstrncpy(w.refDbName,    pRef->refDbName,    TSDB_DB_NAME_LEN);
      tstrncpy(w.refTableName, pRef->refTableName, TSDB_TABLE_NAME_LEN);
      tstrncpy(w.refColName,   pRef->refColName,   TSDB_COL_NAME_LEN);
      if (taosArrayPush(workList, &w) == NULL) { code = terrno; goto _end; }
    }
  } else {
    int32_t nCol = (int32_t)taosArrayGetSize(colCids);
    for (int32_t i = 0; i < nCol; ++i) {
      col_id_t cid    = *(col_id_t *)taosArrayGet(colCids, i);
      SColRef *pFound = NULL;
      for (int32_t j = 0; j < mr.me.colRef.nCols; ++j) {
        if (mr.me.colRef.pColRef[j].id == cid) {
          pFound = &mr.me.colRef.pColRef[j];
          break;
        }
      }
      if (pFound == NULL) {
        stWarn("vgId:%d %s uid=%" PRId64 " COL cid=%d NOT_IN_COLREF -> uid skip",
               TD_VID(pVnode), __func__, uid, cid);
        code = TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST;
        goto _end;
      }
      if (!pFound->hasRef) {
        SColResolveItem *item = taosMemoryCalloc(1, sizeof(*item));
        if (item == NULL) { code = terrno; goto _end; }
        item->hasRef = false;
        if (tSimpleHashPut(pRes->colMap, &cid, sizeof(cid), &item, sizeof(item)) != 0) {
          taosMemoryFree(item);
          code = terrno;
          goto _end;
        }
        continue;
      }
      SResolveWorkItem w = {0};
      w.originVtbUid = uid;
      w.originCid    = cid;
      w.kind         = STREAM_VREF_KIND_COL;
      tstrncpy(w.refDbName,    pFound->refDbName,    TSDB_DB_NAME_LEN);
      tstrncpy(w.refTableName, pFound->refTableName, TSDB_TABLE_NAME_LEN);
      tstrncpy(w.refColName,   pFound->refColName,   TSDB_COL_NAME_LEN);
      if (taosArrayPush(workList, &w) == NULL) { code = terrno; goto _end; }
    }
  }

  // resolve tag cids against pTagRef
  int32_t nTag = (tagCids != NULL) ? (int32_t)taosArrayGetSize(tagCids) : 0;
  for (int32_t i = 0; i < nTag; ++i) {
    col_id_t cid    = *(col_id_t *)taosArrayGet(tagCids, i);
    SColRef *pFound = NULL;
    for (int32_t j = 0; j < mr.me.colRef.nTagRefs; ++j) {
      if (mr.me.colRef.pTagRef[j].id == cid) {
        pFound = &mr.me.colRef.pTagRef[j];
        break;
      }
    }
    // For a VCT, a tag cid that is absent from pTagRef[] (or present with
    // hasRef=0) means the value is stored locally on the child entry as a
    // constant inherited from the parent vstable schemaTag. Both cases must
    // go through the local constant-read path. Only non-VCT (i.e. VNT) tags
    // truly do not exist and should skip the uid.
    if (pFound == NULL && mr.me.type != TSDB_VIRTUAL_CHILD_TABLE) {
      stWarn("vgId:%d %s uid=%" PRId64 " TAG cid=%d NOT_IN_TAGREF type=%d -> uid skip",
             TD_VID(pVnode), __func__, uid, cid, mr.me.type);
      code = TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST;
      goto _end;
    }

    if (pFound == NULL || !pFound->hasRef) {
      // Constant tag on a virtual child table: read locally, write terminal STagValue.
      STagValue *tv = taosMemoryCalloc(1, sizeof(*tv));
      if (tv == NULL) { code = terrno; goto _end; }
      // Use cid: SColRef.colName is not persisted for vtable on disk
      // (the field is "for tmq get json" only). Resolve tag by colId in stable schemaTag.
      int32_t rc = streamReadChildTagConstValueByCid(pVnode, &mr.me, cid,
                                                    &tv->type, &tv->nLen, &tv->pData);
      if (rc != 0) {
        stWarn("vgId:%d %s uid=%" PRId64 " TAG cid=%d const-read err=0x%x -> uid skip",
               TD_VID(pVnode), __func__, uid, cid, rc);
        taosMemoryFreeClear(tv->pData);
        taosMemoryFree(tv);
        code = rc;
        goto _end;
      }
      if (tSimpleHashPut(pRes->tagMap, &cid, sizeof(cid), &tv, sizeof(tv)) != 0) {
        taosMemoryFreeClear(tv->pData);
        taosMemoryFree(tv);
        code = terrno;
        goto _end;
      }
      continue;
    }

    SResolveWorkItem w = {0};
    w.originVtbUid = uid;
    w.originCid    = cid;
    w.kind         = STREAM_VREF_KIND_TAG;
    tstrncpy(w.refDbName,    pFound->refDbName,    TSDB_DB_NAME_LEN);
    tstrncpy(w.refTableName, pFound->refTableName, TSDB_TABLE_NAME_LEN);
    tstrncpy(w.refColName,   pFound->refColName,   TSDB_COL_NAME_LEN);
    if (taosArrayPush(workList, &w) == NULL) { code = terrno; goto _end; }
  }

_end:
  metaReaderClear(&mr);
  return code;
}

// Local hash comparator: search a hash value in a sorted SArray<SVgroupInfo>.
// Mirrors catalog/ctgUtil.c:ctgHashValueComp; keep them in sync.
static int32_t streamVgHashValueComp(void const *lp, void const *rp) {
  uint32_t    *key = (uint32_t *)lp;
  SVgroupInfo *pVg = (SVgroupInfo *)rp;
  if (*key < pVg->hashBegin) return -1;
  if (*key > pVg->hashEnd)   return 1;
  return 0;
}

static int32_t streamVgInfoBeginComp(void const *lp, void const *rp) {
  SVgroupInfo *pLeft  = (SVgroupInfo *)lp;
  SVgroupInfo *pRight = (SVgroupInfo *)rp;
  if (pLeft->hashBegin < pRight->hashBegin) return -1;
  if (pLeft->hashBegin > pRight->hashBegin) return 1;
  return 0;
}

// Async-callback context used by streamFetchDbVgInfo to receive SUseDbRsp.
// Heap-allocated so the callback can safely access it even after caller timeout.
//
// Ownership is transferred via the `state` CAS:
//   FETCH_DBVG_INFLIGHT(0) -> FETCH_DBVG_CB_DONE(1)    : callback finished, driver owns ctx
//   FETCH_DBVG_INFLIGHT(0) -> FETCH_DBVG_DRIVER_GONE(2): driver timed out, callback owns ctx
// Whichever side wins the CAS is responsible for releasing ctx (and destroying the sem).
#define FETCH_DBVG_INFLIGHT    0
#define FETCH_DBVG_CB_DONE     1
#define FETCH_DBVG_DRIVER_GONE 2

typedef struct SStreamFetchDbVgCtx {
  tsem2_t    ready;
  SUseDbRsp *pRsp;
  int32_t    code;
  int8_t     state;
} SStreamFetchDbVgCtx;

static void streamDestroyFetchDbVgCtx(SStreamFetchDbVgCtx *pCtx) {
  if (pCtx == NULL) return;
  if (pCtx->pRsp != NULL) {
    tFreeSUsedbRsp(pCtx->pRsp);
    taosMemoryFree(pCtx->pRsp);
  }
  TAOS_UNUSED(tsem2_destroy(&pCtx->ready));
  taosMemoryFree(pCtx);
}

static int32_t streamProcessFetchDbVgRsp(void *param, SDataBuf *pMsg, int32_t code) {
  SStreamFetchDbVgCtx *pCtx = (SStreamFetchDbVgCtx *)param;
  if (code == TSDB_CODE_SUCCESS && pMsg != NULL && pMsg->pData != NULL && pMsg->len > 0) {
    pCtx->pRsp = taosMemoryCalloc(1, sizeof(SUseDbRsp));
    if (pCtx->pRsp == NULL) {
      code = terrno;
    } else if (tDeserializeSUseDbRsp(pMsg->pData, (int32_t)pMsg->len, pCtx->pRsp) != 0) {
      code = TSDB_CODE_INVALID_MSG;
    }
  } else if (code == TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_INVALID_MSG;
  }
  pCtx->code = code;

  if (pMsg != NULL) {
    taosMemoryFreeClear(pMsg->pData);
    taosMemoryFreeClear(pMsg->pEpSet);
  }

  // Atomically claim ownership: CAS INFLIGHT -> CB_DONE.
  // If CAS fails, the driver already abandoned ctx; callback must clean up.
  int8_t prev = atomic_val_compare_exchange_8(&pCtx->state, FETCH_DBVG_INFLIGHT, FETCH_DBVG_CB_DONE);
  if (prev == FETCH_DBVG_DRIVER_GONE) {
    streamDestroyFetchDbVgCtx(pCtx);
    return code;
  }

  // Driver still owns ctx and is (or will be) waiting on the sem.
  TAOS_UNUSED(tsem2_post(&pCtx->ready));
  return code;
}

// Fetch SUseDbRsp asynchronously from mnode for dbFName ("acctId.dbName").
// Caller owns *ppOut on success and must call tFreeSUsedbRsp + free the pointer.
static int32_t streamFetchDbVgInfo(SVnode *pVnode, const char *dbFName, SUseDbRsp **ppOut) {
  int32_t              code      = 0;
  SUseDbReq            req       = {0};
  void                *pReqBuf   = NULL;
  SMsgSendInfo        *pSendInfo = NULL;
  SStreamFetchDbVgCtx *pCtx      = NULL;
  SEpSet               epSet     = {0};

  *ppOut = NULL;
  tstrncpy(req.db, dbFName, sizeof(req.db));
  req.vgVersion  = -1;
  req.dbId       = 0;
  req.numOfTable = 0;
  req.stateTs    = 0;

  void *clientRpc = pVnode->msgCb.clientRpc;
  if (clientRpc == NULL) { code = TSDB_CODE_INVALID_PARA; goto _end; }

  // Heap-allocate ctx so callback can safely access it after caller timeout.
  pCtx = taosMemoryCalloc(1, sizeof(SStreamFetchDbVgCtx));
  if (pCtx == NULL) { code = terrno; goto _end; }
  if (tsem2_init(&pCtx->ready, 0, 0) != 0) {
    // sem not initialized yet; free directly to avoid destroying an uninitialized sem.
    code = terrno;
    taosMemoryFree(pCtx);
    pCtx = NULL;
    goto _end;
  }

  int32_t reqLen = tSerializeSUseDbReq(NULL, 0, &req);
  if (reqLen < 0) { code = terrno; goto _end; }
  pReqBuf = taosMemoryCalloc(1, reqLen);
  if (pReqBuf == NULL) { code = terrno; goto _end; }
  if (tSerializeSUseDbReq(pReqBuf, reqLen, &req) < 0) { code = terrno; goto _end; }

  pSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (pSendInfo == NULL) { code = terrno; goto _end; }

  pSendInfo->param          = pCtx;
  pSendInfo->msgInfo.pData  = pReqBuf;
  pSendInfo->msgInfo.len    = reqLen;
  pSendInfo->msgType        = TDMT_MND_GET_DB_INFO;
  pSendInfo->fp             = streamProcessFetchDbVgRsp;
  pReqBuf = NULL;  // ownership transferred to pSendInfo

  streamGetMnodeEpset(&epSet);

  code = asyncSendMsgToServer(clientRpc, &epSet, NULL, pSendInfo);
  pSendInfo = NULL;  // ownership transferred (freed by asyncSendMsgToServer on any path)
  if (code != 0) goto _end;

  if (tsem2_timewait(&pCtx->ready, STREAM_VTB_RPC_TIMEOUT_MS) != 0) {
    // Timewait reported timeout. Try to atomically claim "driver gives up".
    // If CAS succeeds, callback (whenever it fires) will free ctx.
    // If CAS fails, callback finished concurrently with the timeout; treat as success
    // and fall through to the normal data-handling path. The matching post may or may
    // not have already been observed -- drain it via tsem2_timewait so we can safely destroy.
    int8_t prev = atomic_val_compare_exchange_8(&pCtx->state, FETCH_DBVG_INFLIGHT, FETCH_DBVG_DRIVER_GONE);
    if (prev == FETCH_DBVG_INFLIGHT) {
      stWarn("vgId:%d %s timeout waiting for mnode db-vg-info rsp for %s",
             TD_VID(pVnode), __func__, dbFName);
      pCtx = NULL;  // ownership transferred to callback
      code = TSDB_CODE_TIMEOUT_ERROR;
      goto _end;
    }
    // Late win by callback: consume the pending post (non-blocking by design).
    TAOS_UNUSED(tsem2_wait(&pCtx->ready));
    stDebug("vgId:%d %s timewait raced with callback; proceeding with received rsp for %s",
            TD_VID(pVnode), __func__, dbFName);
  }

  if (pCtx->code != 0) { code = pCtx->code; goto _end; }
  if (pCtx->pRsp == NULL) { code = TSDB_CODE_INVALID_MSG; goto _end; }

  // Sort vgroup array by hashBegin so we can binary-search for routing.
  if (pCtx->pRsp->pVgroupInfos != NULL) {
    taosArraySort(pCtx->pRsp->pVgroupInfos, streamVgInfoBeginComp);
  }

  *ppOut  = pCtx->pRsp;
  pCtx->pRsp = NULL;

_end:
  if (pReqBuf != NULL) taosMemoryFree(pReqBuf);
  if (pSendInfo != NULL) taosMemoryFree(pSendInfo);
  if (pCtx != NULL) {
    // Either the RPC never went out (state still INFLIGHT, no callback will fire),
    // or the callback already completed (state == CB_DONE). In both cases the driver
    // owns pCtx and is responsible for releasing it. The handoff CAS in the timeout
    // branch above sets pCtx=NULL on the abandoned path, so we never reach here with
    // ownership transferred away.
    streamDestroyFetchDbVgCtx(pCtx);
  }
  return code;
}

// Get SUseDbRsp for dbFName, using cache if available; otherwise fetch and insert.
// Returned *ppOut is owned by the cache (when pCache != NULL) or by the caller
// (when pCache == NULL); caller never frees the cached entry.
static int32_t streamGetOrFetchDbVgInfo(SVnode *pVnode, SStreamVTableInfoCache *pCache,
                                        const char *dbFName, SUseDbRsp **ppOut, bool *pCached) {
  *ppOut = NULL;
  if (pCached) *pCached = false;

  if (pCache != NULL && pCache->dbVgInfo != NULL) {
    SUseDbRsp *pHit = (SUseDbRsp *)taosHashGet(pCache->dbVgInfo, dbFName, strlen(dbFName));
    if (pHit != NULL) {
      *ppOut = pHit;
      if (pCached) *pCached = true;
      return 0;
    }
  }

  SUseDbRsp *pNew = NULL;
  int32_t    code = streamFetchDbVgInfo(pVnode, dbFName, &pNew);
  if (code != 0) return code;

  if (pCache != NULL && pCache->dbVgInfo != NULL) {
    // taosHashPut copies the value bytes; we must still keep the inner array
    // alive (pVgroupInfos is a heap pointer the cached entry now owns).
    if (taosHashPut(pCache->dbVgInfo, dbFName, strlen(dbFName), pNew, sizeof(*pNew)) != 0) {
      tFreeSUsedbRsp(pNew);
      taosMemoryFree(pNew);
      return terrno;
    }
    // Hash now owns pVgroupInfos via the copied struct; drop our outer wrapper
    // without freeing the array (cleanup uses tFreeSUsedbRsp on hash entries).
    taosMemoryFree(pNew);
    *ppOut = (SUseDbRsp *)taosHashGet(pCache->dbVgInfo, dbFName, strlen(dbFName));
    return 0;
  }

  *ppOut = pNew;
  return 0;
}

// Resolve target vgId/epSet for a (db, table) using cached SUseDbRsp routing info.
// dbFName is "acctId.dbName" (matches SUseDbRsp->db); tableName is the child name.
static int32_t streamRouteTableToVg(SUseDbRsp *pRsp, const char *dbFName, const char *tableName,
                                    int32_t *pVgId, SEpSet *pEpSet) {
  if (pRsp == NULL || pRsp->pVgroupInfos == NULL) return TSDB_CODE_INVALID_PARA;
  int32_t vgNum = (int32_t)taosArrayGetSize(pRsp->pVgroupInfos);
  if (vgNum <= 0) return TSDB_CODE_MND_DB_NOT_EXIST;

  char fullName[TSDB_TABLE_FNAME_LEN] = {0};
  int32_t n = tsnprintf(fullName, sizeof(fullName), "%s.%s", dbFName, tableName);
  if (n <= 0) return TSDB_CODE_INVALID_PARA;

  uint32_t hashValue = (uint32_t)taosGetTbHashVal(fullName, n, pRsp->hashMethod,
                                                  pRsp->hashPrefix, pRsp->hashSuffix);
  SVgroupInfo *pVg = (SVgroupInfo *)taosArraySearch(pRsp->pVgroupInfos, &hashValue,
                                                    streamVgHashValueComp, TD_EQ);
  if (pVg == NULL) return TSDB_CODE_MND_DB_NOT_EXIST;
  *pVgId  = pVg->vgId;
  *pEpSet = pVg->epSet;
  vDebug("stream route table:%s to vgId:%d, epSet inUse:%d numOfEps:%d",
        fullName, pVg->vgId, pVg->epSet.inUse, pVg->epSet.numOfEps);
  for (int32_t i = 0; i < pVg->epSet.numOfEps; ++i) {
    vDebug("stream route table:%s vgId:%d ep[%d]: %s:%u",
          fullName, pVg->vgId, i, pVg->epSet.eps[i].fqdn, pVg->epSet.eps[i].port);
  }
  return 0;
}

// Shared fan-out completion sync owned by streamBatchFanoutDrain. All fired
// handles point at the same instance via SStreamVgResolveCtx::pSync. The sync
// is heap-allocated so it can outlive the drain frame on the timeout path:
// any late callback can still safely deref pSync and (if it is the last
// reference) free it.
//
// Two atomic counters:
//   - pending: number of in-flight callbacks PLUS a "fan-out in progress"
//     reservation held by the driver. Gates the binary sem. The cb that
//     drives pending -> 0 posts the sem exactly once.
//   - refs:    lifetime refcount. Driver holds 1, each successful fire holds 1.
//     Whoever drives refs -> 0 (driver after wait, or the last cb on the
//     timeout-abandoned path) calls streamFanoutSyncDestroy.
//
// Why two counters? The driver releases its `pending` reservation BEFORE
// tsem2_timewait, but must keep `refs` so the sync stays alive while it's
// blocked. On a real timeout the driver decrements `refs` and walks away;
// the last late cb to dec refs frees the sync. This decouples "wakeup
// signalling" from "object lifetime" and avoids a destroy-vs-late-post race.
// SStreamFanoutSync moved to vnodeStreamVTable.h for testability.

SStreamFanoutSync *streamFanoutSyncCreate(void) {
  SStreamFanoutSync *p = taosMemoryCalloc(1, sizeof(*p));
  if (p == NULL) return NULL;
  if (tsem2_init(&p->sem, 0, 0) != 0) {
    taosMemoryFree(p);
    return NULL;
  }
  atomic_store_32(&p->pending, 0);
  atomic_store_32(&p->refs, 0);
  return p;
}

void streamFanoutSyncDestroy(SStreamFanoutSync *p) {
  if (p == NULL) return;
  TAOS_UNUSED(tsem2_destroy(&p->sem));
  taosMemoryFree(p);
}

// Release one ref to the shared sync. Caller MUST stop touching sync after
// this returns. Returns true if this call freed the sync (last reference).
bool streamFanoutSyncRelease(SStreamFanoutSync *p) {
  if (p == NULL) return false;
  int32_t r = atomic_sub_fetch_32(&p->refs, 1);
  if (r == 0) {
    streamFanoutSyncDestroy(p);
    return true;
  }
  return false;
}

// Async-callback context used by streamPrepareAndFireOneVgResolve /
// streamScatterOneVgResolve to receive SVTableRefResolveRsp from a
// remote vnode. One ctx is owned by each SStreamVgRpcHandle so callbacks can
// fire any time during the fan-out window without racing handle teardown.
typedef struct SStreamVgResolveCtx {
  SStreamFanoutSync   *pSync;    // borrowed: shared completion sync owned by drain
  SVTableRefResolveRsp rsp;
  int32_t              code;
} SStreamVgResolveCtx;

// Handle for an in-flight per-vg resolve RPC. Driver allocates one per remote
// vg in fan-out phase A; phase B then waits + scatters + destroys them all,
// possibly in parallel (each ctx is heap-resident so callbacks can fire any
// time during phase A without racing with stack teardown).
//
// `state` is an atomic CAS-arbitrated ownership word used to hand off the
// handle between callback and driver on the timeout path:
//   VG_HANDLE_INFLIGHT(0)   : cb hasn't completed yet, driver owns
//   VG_HANDLE_CB_DONE(1)    : cb completed normally, driver owns h
//   VG_HANDLE_DRIVER_GONE(2): driver gave up on this handle, cb owns h
// Whichever side wins the CAS becomes responsible for streamDestroyVgRpcHandle.
#define VG_HANDLE_INFLIGHT    0
#define VG_HANDLE_CB_DONE     1
#define VG_HANDLE_DRIVER_GONE 2

typedef struct SStreamVgRpcHandle {
  int32_t              vgId;
  int8_t               state;           // atomic, see header comment
  SArray              *indexList;       // borrowed: position list inside dedup batch
  int32_t              totalCols;       // expected rsp.items count = sum of group cols
  // scatterOrder[i] = dedupItems position of the i-th flattened column in req
  // (groups[0].cols[0], groups[0].cols[1], ..., groups[1].cols[0], ...).
  // This matches the server response order exactly and is the correct mapping
  // to use during scatter, replacing the incorrect indexList-order assumption.
  SArray              *scatterOrder;    // owned: SArray<int32_t>, freed on destroy
  SVTableRefResolveReq req;             // owned: tFreeSVTableRefResolveReq on destroy
  SStreamVgResolveCtx  ctx;            // owned: decoded rsp + shared-sem pointer
} SStreamVgRpcHandle;

static void streamDestroyVgRpcHandle(SStreamVgRpcHandle **ppHandle) {
  if (ppHandle == NULL || *ppHandle == NULL) return;
  SStreamVgRpcHandle *h = *ppHandle;
  tFreeSVTableRefResolveReq(&h->req);
  tFreeSVTableRefResolveRsp(&h->ctx.rsp);
  taosArrayDestroy(h->scatterOrder);
  h->scatterOrder = NULL;
  // Note: ctx.pSync is a refcounted shared sync; its lifetime is governed by
  // streamFanoutSyncRelease (called by both driver and each cb) and is NOT
  // released here.
  taosMemoryFree(h);
  *ppHandle = NULL;
}

static int32_t streamProcessVgResolveRsp(void *param, SDataBuf *pMsg, int32_t code) {
  SStreamVgResolveCtx *pCtx  = (SStreamVgResolveCtx *)param;
  SStreamFanoutSync   *pSync = pCtx->pSync;
  // Recover the enclosing handle from pCtx via offsetof: pCtx is the `ctx`
  // member of the SStreamVgRpcHandle that owns it.
  SStreamVgRpcHandle *h =
      (SStreamVgRpcHandle *)((char *)pCtx - offsetof(SStreamVgRpcHandle, ctx));

  stTrace("stream vtable resolve rsp arrived: code=0x%x len=%d pData=%p", code,
          pMsg ? (int32_t)pMsg->len : -1, pMsg ? pMsg->pData : NULL);
  if (code == TSDB_CODE_SUCCESS) {
    if (pMsg != NULL && pMsg->pData != NULL && pMsg->len > 0) {
      if (tDeserializeSVTableRefResolveRsp(pMsg->pData, (int32_t)pMsg->len, &pCtx->rsp) < 0) {
        code = TSDB_CODE_OUT_OF_MEMORY;
      }
    } else {
      code = TSDB_CODE_INVALID_MSG;
    }
  }
  pCtx->code = code;
  stTrace("stream vtable resolve rsp processed: code=0x%x rspItems=%d", code,
          pCtx->rsp.items ? (int32_t)taosArrayGetSize(pCtx->rsp.items) : 0);

  if (pMsg != NULL) {
    taosMemoryFreeClear(pMsg->pData);
    taosMemoryFreeClear(pMsg->pEpSet);
  }

  // Claim ownership of the handle: CAS INFLIGHT -> CB_DONE. If CAS fails
  // (state is VG_HANDLE_DRIVER_GONE), the driver abandoned this handle on
  // the timeout path and the cb is responsible for releasing it.
  int8_t prev = atomic_val_compare_exchange_8(&h->state,
                                              VG_HANDLE_INFLIGHT,
                                              VG_HANDLE_CB_DONE);
  if (prev == VG_HANDLE_DRIVER_GONE) {
    stTrace("stream vtable resolve cb: driver abandoned handle, cb destroys h=%p", h);
    streamDestroyVgRpcHandle(&h);
  }

  // Decrement shared pending; only the last completer posts the sem. The
  // atomic dec is a release barrier -- the drain's matching tsem2_timewait
  // acquire pairs with it so rsp/code stores above are visible after the wait.
  int32_t remaining = atomic_sub_fetch_32(&pSync->pending, 1);
  stTrace("stream vtable resolve cb done: code=0x%x remaining=%d", code, remaining);
  if (remaining == 0) {
    TAOS_UNUSED(tsem2_post(&pSync->sem));
  }

  // Release this cb's lifetime ref on the sync. If this was the last ref
  // (driver already walked away after timeout AND we are the last in-flight
  // cb), this frees the sync. After this call pSync must not be touched.
  TAOS_UNUSED(streamFanoutSyncRelease(pSync));
  return code;
}

// Phase A of fan-out: build a table-grouped request from the (batch, indexList)
// slice, serialize it with SMsgHead, and asyncSendMsgToServer it.
//
// The caller owns `h` (already pre-allocated and pushed into the drain's
// rpcHandles array) with h->vgId / h->indexList / h->ctx.pSync pre-filled.
// This function only fills in h->req / h->totalCols and fires the RPC.
//
// On success returns 0; the callback is guaranteed to fire and decrement the
// shared sync counter exactly once.
// On ANY failure (serialize / send) returns the error code; h stays owned by
// the caller and will be freed by the drain cleanup loop. The shared sync
// counter is left untouched in the failure case (no callback will fire).
static int32_t streamPrepareAndFireOneVgResolve(SVnode *pVnode, const SEpSet *pEpSet,
                                                int64_t ver, SArray *batch, SArray *indexList,
                                                SStreamVgRpcHandle *h) {
  int32_t       code         = 0;
  SHashObj     *tblGroupMap  = NULL;
  void         *pReqBuf      = NULL;
  SMsgSendInfo *pSendInfo    = NULL;
  SArray       *groupPosList = NULL;  // SArray<SArray<int32_t>*>, temp per-group pos lists

  int32_t cnt = (int32_t)taosArrayGetSize(indexList);
  stTrace("vgId:%d %s enter: targetVgId=%d ver=%" PRId64 " items=%d", TD_VID(pVnode), __func__,
          h->vgId, ver, cnt);

  h->req.ver    = ver;
  h->req.groups = taosArrayInit(4, sizeof(SVTableRefResolveGroupItem));
  if (h->req.groups == NULL) { code = terrno; goto _capture; }

  // scatterOrder[i] records the dedupItems position of the i-th column in the
  // flattened request (groups[0].cols[0], groups[0].cols[1], ...,
  // groups[1].cols[0], ...).  The server returns responses in this same
  // flattened order, so scatter must use scatterOrder — not indexList — to map
  // rsp.items[i] back to its original slot in dedupRspItems.
  h->scatterOrder = taosArrayInit(cnt, sizeof(int32_t));
  if (h->scatterOrder == NULL) { code = terrno; goto _capture; }

  // Use a temp hash to map "dbName\0tableName" -> index in req.groups
  tblGroupMap = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (tblGroupMap == NULL) { code = terrno; goto _capture; }

  // Two-pass grouping: iterate indexList to build per-table groups (re-grouping
  // interleaved columns like [t1.c1, t2.c1, t1.c2] into contiguous groups).
  // Track per-group column positions in groupPosList so we can flatten them in
  // group order into scatterOrder, which mirrors the server's response order.
  groupPosList = taosArrayInit(4, sizeof(SArray *));
  if (groupPosList == NULL) { code = terrno; goto _capture; }

  int32_t totalCols = 0;
  for (int32_t i = 0; i < cnt; ++i) {
    int32_t           pos = *(int32_t *)taosArrayGet(indexList, i);
    SResolveWorkItem *w   = taosArrayGet(batch, pos);

    char    tblKey[TSDB_DB_NAME_LEN + 1 + TSDB_TABLE_NAME_LEN];
    int32_t dLen = (int32_t)strlen(w->refDbName);
    int32_t tLen = (int32_t)strlen(w->refTableName);
    memcpy(tblKey, w->refDbName, dLen);
    tblKey[dLen] = '\0';
    memcpy(tblKey + dLen + 1, w->refTableName, tLen);
    int32_t keyLen = dLen + 1 + tLen;

    int32_t *pGroupIdx = taosHashGet(tblGroupMap, tblKey, keyLen);
    int32_t  groupIdx;
    if (pGroupIdx == NULL) {
      SVTableRefResolveGroupItem g = {0};
      tstrncpy(g.dbName, w->refDbName, TSDB_DB_NAME_LEN);
      tstrncpy(g.tableName, w->refTableName, TSDB_TABLE_NAME_LEN);
      g.cols = taosArrayInit(4, sizeof(SVTableRefResolveColSpec));
      if (g.cols == NULL) { code = terrno; goto _capture; }
      if (taosArrayPush(h->req.groups, &g) == NULL) {
        taosArrayDestroy(g.cols);
        code = terrno;
        goto _capture;
      }
      groupIdx = (int32_t)taosArrayGetSize(h->req.groups) - 1;
      // Abort on put failure: a duplicate group would break the rsp scatter
      // ordering assumption used by streamScatterOneVgResolve below.
      if (taosHashPut(tblGroupMap, tblKey, keyLen, &groupIdx, sizeof(groupIdx)) != 0) {
        code = terrno;
        goto _capture;
      }
      // Create a parallel position list for this new group.
      SArray *posList = taosArrayInit(4, sizeof(int32_t));
      if (posList == NULL) { code = terrno; goto _capture; }
      if (taosArrayPush(groupPosList, &posList) == NULL) {
        taosArrayDestroy(posList);
        code = terrno;
        goto _capture;
      }
    } else {
      groupIdx = *pGroupIdx;
    }

    SVTableRefResolveGroupItem *gp = taosArrayGet(h->req.groups, groupIdx);
    SVTableRefResolveColSpec    colSpec = {0};
    tstrncpy(colSpec.colName, w->refColName, TSDB_COL_NAME_LEN);
    colSpec.kind = w->kind;
    if (taosArrayPush(gp->cols, &colSpec) == NULL) {
      code = terrno;
      goto _capture;
    }
    // Record the dedupItems position in the per-group list; this mirrors the
    // column append order exactly, matching the server's response order.
    SArray *curPosList = *(SArray **)taosArrayGet(groupPosList, groupIdx);
    if (taosArrayPush(curPosList, &pos) == NULL) {
      code = terrno;
      goto _capture;
    }
    totalCols++;
  }

  // Flatten per-group position lists into scatterOrder in group order.
  // The server iterates groups[0], groups[1], ... and within each group
  // appends cols[0], cols[1], ..., so this produces the exact same order.
  int32_t nGroups = (int32_t)taosArrayGetSize(groupPosList);
  for (int32_t g = 0; g < nGroups; ++g) {
    SArray *posList = *(SArray **)taosArrayGet(groupPosList, g);
    int32_t nPos    = (int32_t)taosArrayGetSize(posList);
    for (int32_t j = 0; j < nPos; ++j) {
      int32_t p = *(int32_t *)taosArrayGet(posList, j);
      if (taosArrayPush(h->scatterOrder, &p) == NULL) {
        code = terrno;
        goto _capture;
      }
    }
  }
  // Free the temporary per-group position lists.
  for (int32_t g = 0; g < nGroups; ++g) {
    SArray *posList = *(SArray **)taosArrayGet(groupPosList, g);
    taosArrayDestroy(posList);
  }
  taosArrayDestroy(groupPosList);
  groupPosList = NULL;

  h->totalCols = totalCols;

  void *clientRpc = pVnode->msgCb.clientRpc;
  if (clientRpc == NULL) { code = TSDB_CODE_INVALID_PARA; goto _capture; }

  int32_t reqLen = tSerializeSVTableRefResolveReq(NULL, 0, &h->req);
  if (reqLen < 0) { code = terrno; goto _capture; }
  // Prepend SMsgHead so dnode dispatcher (vmPutMsgToQueue) can route by vgId.
  int32_t totalLen = reqLen + (int32_t)sizeof(SMsgHead);
  pReqBuf = taosMemoryCalloc(1, totalLen);
  if (pReqBuf == NULL) { code = terrno; goto _capture; }
  if (tSerializeSVTableRefResolveReq((char *)pReqBuf + sizeof(SMsgHead), reqLen, &h->req) < 0) {
    code = terrno;
    goto _capture;
  }
  ((SMsgHead *)pReqBuf)->vgId    = htonl(h->vgId);
  ((SMsgHead *)pReqBuf)->contLen = htonl(totalLen);

  pSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (pSendInfo == NULL) { code = terrno; goto _capture; }

  pSendInfo->param         = &h->ctx;
  pSendInfo->msgInfo.pData = pReqBuf;
  pSendInfo->msgInfo.len   = totalLen;
  pSendInfo->msgType       = TDMT_VND_VTABLE_REF_RESOLVE;
  pSendInfo->fp            = streamProcessVgResolveRsp;
  pReqBuf = NULL;  // ownership transferred to pSendInfo

  // Reserve BOTH counters BEFORE asyncSend so a fast/synchronous callback
  // cannot race past us. `pending` gates the sem post; `refs` gates sync
  // lifetime. Both are rolled back if send is rejected.
  TAOS_UNUSED(atomic_add_fetch_32(&h->ctx.pSync->pending, 1));
  TAOS_UNUSED(atomic_add_fetch_32(&h->ctx.pSync->refs, 1));

  code = asyncSendMsgToServer(clientRpc, (SEpSet *)pEpSet, NULL, pSendInfo);
  pSendInfo = NULL;  // ownership transferred (or freed by asyncSendMsgToServer on error)
  stTrace("vgId:%d %s asyncSend done: targetVgId=%d code=0x%x reqLen=%d", TD_VID(pVnode), __func__,
          h->vgId, code, totalLen);
  if (code != 0) {
    // Send rejected -- no callback will fire, release both reservations.
    // The drain holds its own reservation on both counters, so neither can
    // cross 0 here.
    TAOS_UNUSED(atomic_sub_fetch_32(&h->ctx.pSync->pending, 1));
    TAOS_UNUSED(atomic_sub_fetch_32(&h->ctx.pSync->refs, 1));
    goto _capture;
  }

  // Successfully queued: callback will fire and decrement the shared counter.
  if (tblGroupMap != NULL) taosHashCleanup(tblGroupMap);
  return 0;

_capture:
  // Any failure before asyncSend accepted the request: no callback will fire.
  // h is owned by the caller (already in rpcHandles); free only local buffers.
  if (tblGroupMap != NULL) taosHashCleanup(tblGroupMap);
  if (pReqBuf != NULL) taosMemoryFree(pReqBuf);
  if (pSendInfo != NULL) taosMemoryFree(pSendInfo);
  if (groupPosList != NULL) {
    int32_t ngl = (int32_t)taosArrayGetSize(groupPosList);
    for (int32_t g = 0; g < ngl; ++g) {
      SArray *pl = *(SArray **)taosArrayGet(groupPosList, g);
      taosArrayDestroy(pl);
    }
    taosArrayDestroy(groupPosList);
  }
  return code;
}

// Phase B of fan-out: scatter a single completed RPC's rsp items into
// outRspItems using the scatterOrder mapping built during phase A. Items'
// tagData ownership is transferred from the rsp into outRspItems (rsp slots
// NULLed) to match the existing scatter semantics used downstream by the
// deep-copy step.
//
// Waiting for callback completion is NOT done here; it is centralized in
// streamBatchFanoutDrain via the shared SStreamFanoutSync. By the time the
// drain returns, every fired callback has completed (pending == 0) and the
// rsp/code fields are safely published.
//
// Returns 0 on success; non-zero on rsp decode error or size mismatch. Caller
// must still call streamDestroyVgRpcHandle to release ctx/req memory.
static int32_t streamScatterOneVgResolve(SVnode *pVnode, SStreamVgRpcHandle *h,
                                         SArray *outRspItems) {
  if (h == NULL) return TSDB_CODE_INVALID_PARA;

  stTrace("vgId:%d %s scatter: targetVgId=%d ctxCode=0x%x rspItems=%d", TD_VID(pVnode), __func__,
          h->vgId, h->ctx.code, h->ctx.rsp.items ? (int32_t)taosArrayGetSize(h->ctx.rsp.items) : 0);

  if (h->ctx.code != 0) return h->ctx.code;

  int32_t cnt = (int32_t)taosArrayGetSize(h->scatterOrder);
  int32_t m   = (h->ctx.rsp.items != NULL) ? (int32_t)taosArrayGetSize(h->ctx.rsp.items) : 0;
  // Both scatterOrder and rsp.items must match totalCols; cnt mismatch would
  // cause an out-of-bounds read in the loop below.
  if (cnt != m || m != h->totalCols) return TSDB_CODE_INVALID_MSG;

  // Move each rsp item back to its original dedupItems slot.
  // scatterOrder[i] is the dedupItems position of the i-th column in the
  // flattened request, which exactly matches the server response order
  // (groups[0].cols[0], groups[0].cols[1], ..., groups[1].cols[0], ...).
  // Using scatterOrder instead of indexList is correct even when columns of
  // the same table are interleaved across multiple tables in the original
  // vtable column list (e.g. [t1.c1, t2.c1, t1.c2]), where the grouping
  // re-orders them but scatterOrder tracks the mapping precisely.
  for (int32_t i = 0; i < cnt; ++i) {
    int32_t                   pos = *(int32_t *)taosArrayGet(h->scatterOrder, i);
    SVTableRefResolveRspItem *src = taosArrayGet(h->ctx.rsp.items, i);
    SVTableRefResolveRspItem *dst = taosArrayGet(outRspItems, pos);
    *dst = *src;
    src->tagData = NULL;
    src->tagLen  = 0;
  }
  return 0;
}


// Drive one resolution round for a heterogeneous batch: group work-items by the
// target vgId of (refDbName, refTableName), issue one RPC per vg, and write
// responses back to outRspItems in batch order.
//
// pCache (optional): caches db routing info across hops/uids to avoid hammering
// mnode. NULL means no cache (every miss goes to mnode).
//
// outRspItems must be pre-sized with batch.size() default-zero entries; this
// function fills them in place.
//
// Build the flat composite key "dbName\0tableName\0colName" used by BOTH
// the Phase-1 dedup map and the tblRefCache. Tags and columns cannot share
// a name within the same physical table, so kind does not enter the key.
// Caller-provided buffer must hold at least
// TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN + 4 bytes.
// Example: db="mydb", tb="t1", col="voltage" => key="mydb\0t1\0voltage" (len=16)
void streamBuildTblColKey(const char *db, const char *tb, const char *col,
                                 char *out, int32_t *outLen) {
  int32_t n = 0;
  int32_t dbLen = (int32_t)strlen(db);
  memcpy(out + n, db, dbLen); n += dbLen;
  out[n++] = '\0';
  int32_t tbLen = (int32_t)strlen(tb);
  memcpy(out + n, tb, tbLen); n += tbLen;
  out[n++] = '\0';
  int32_t clLen = (int32_t)strlen(col);
  memcpy(out + n, col, clLen); n += clLen;
  *outLen = n;
}

// Helper: look up tblRefCache for a resolved column. Returns pointer to cached
// SVTableRefResolveRspItem or NULL if not cached.
SVTableRefResolveRspItem *streamTblRefCacheLookup(SStreamVTableInfoCache *pCache,
                                                          const char *dbName, const char *tableName,
                                                          const char *colName, int8_t kind) {
  (void)kind;  // tag and col cannot share a name within a table
  if (pCache == NULL || pCache->tblRefCache == NULL) return NULL;
  char    key[TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN + 4];
  int32_t keyLen = 0;
  streamBuildTblColKey(dbName, tableName, colName, key, &keyLen);
  return (SVTableRefResolveRspItem *)taosHashGet(pCache->tblRefCache, key, keyLen);
}

// Helper: insert a resolved column result into tblRefCache.
void streamTblRefCacheInsert(SStreamVTableInfoCache *pCache,
                                     const char *dbName, const char *tableName,
                                     const char *colName, int8_t kind,
                                     const SVTableRefResolveRspItem *pItem) {
  (void)kind;  // tag and col cannot share a name within a table
  if (pCache == NULL || pCache->tblRefCache == NULL) return;
  char    key[TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN + 4];
  int32_t keyLen = 0;
  streamBuildTblColKey(dbName, tableName, colName, key, &keyLen);
  // Store a deep copy including tagData so cache outlives the original rsp.
  SVTableRefResolveRspItem copy = *pItem;
  if (pItem->tagData != NULL && pItem->tagLen > 0) {
    copy.tagData = taosMemoryMalloc(pItem->tagLen);
    if (copy.tagData != NULL) {
      memcpy(copy.tagData, pItem->tagData, pItem->tagLen);
      copy.tagLen = pItem->tagLen;
    } else {
      copy.tagData = NULL;
      copy.tagLen  = 0;
    }
  } else {
    copy.tagData = NULL;
    copy.tagLen  = 0;
  }
  if (taosHashPut(pCache->tblRefCache, key, keyLen, &copy, sizeof(copy)) != 0) {
    // Put failed (likely OOM/rehash); release the freshly deep-copied tagData
    // to avoid leaking it. Cache miss on next lookup will simply re-resolve.
    stWarn("%s taosHashPut failed for col=%s, code=0x%x", __func__, colName, terrno);
    taosMemoryFreeClear(copy.tagData);
    copy.tagLen = 0;
  }
}

// (dedup map and tblRefCache share streamBuildTblColKey above; no extra alias.)


// Phase 1 helper: deep-copy one rsp item from cache (or remote) into outRspItems[i].
int32_t streamWriteRspItemDeepCopy(const SVTableRefResolveRspItem *src,
                                          SVTableRefResolveRspItem *dst) {
  *dst = *src;
  if (src->tagData != NULL && src->tagLen > 0) {
    dst->tagData = taosMemoryMalloc(src->tagLen);
    if (dst->tagData == NULL) {
      // Mirror the existing graceful-degrade behavior: keep dst->code,
      // surface the missing payload by zeroing tagLen instead of failing
      // the entire batch.
      dst->tagLen = 0;
      return terrno;
    }
    memcpy(dst->tagData, src->tagData, src->tagLen);
  } else {
    dst->tagData = NULL;
    dst->tagLen  = 0;
  }
  return 0;
}

// Phase 1 of streamCallResolveBatched: walk batch[]; for each item, either
// resolve from tblRefCache (writing directly into outRspItems[i] and marking
// origToDedupIdx[i]=-1) or deduplicate by (db,table,col) into dedupItems and
// record origToDedupIdx[i] = dedup slot.
int32_t streamBatchTryCacheAndDedup(SStreamVTableInfoCache *pCache, SArray *batch,
                                           SArray *outRspItems, SHashObj *dedupMap,
                                           SArray *dedupItems, int32_t *origToDedupIdx,
                                           int32_t *pCacheHits) {
  int32_t n = (int32_t)taosArrayGetSize(batch);
  int32_t cacheHits = 0;
  for (int32_t i = 0; i < n; ++i) {
    SResolveWorkItem *w = taosArrayGet(batch, i);

    SVTableRefResolveRspItem *cached = streamTblRefCacheLookup(pCache, w->refDbName, w->refTableName,
                                                               w->refColName, w->kind);
    if (cached != NULL) {
      // Cache hit: deep copy into the caller-owned outRspItems slot directly.
      SVTableRefResolveRspItem *dst = taosArrayGet(outRspItems, i);
      int32_t rc = streamWriteRspItemDeepCopy(cached, dst);
      if (rc != 0) {
        // OOM: treat as cache miss so this item falls through to RPC path.
        stWarn("streamBatchTryCacheAndDedup deep copy failed (OOM), falling back to RPC: i=%d rc=0x%x", i, rc);
        dst->tagLen = 0;
        dst->tagData = NULL;
        // Fall through to dedup path below.
      } else {
        origToDedupIdx[i] = -1;
        cacheHits++;
        continue;
      }
    }

    // Dedup key: tag and col share namespace within a table so kind is omitted.
    char    dedupKey[TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN + 4];
    int32_t dkLen = 0;
    streamBuildTblColKey(w->refDbName, w->refTableName, w->refColName, dedupKey, &dkLen);

    int32_t *pExistIdx = (int32_t *)taosHashGet(dedupMap, dedupKey, dkLen);
    if (pExistIdx != NULL) {
      origToDedupIdx[i] = *pExistIdx;
    } else {
      int32_t newIdx = (int32_t)taosArrayGetSize(dedupItems);
      if (taosArrayPush(dedupItems, w) == NULL) return terrno;
      if (taosHashPut(dedupMap, dedupKey, dkLen, &newIdx, sizeof(newIdx)) != 0) return terrno;
      origToDedupIdx[i] = newIdx;
    }
  }
  *pCacheHits = cacheHits;
  return 0;
}

// Phase 2 of streamCallResolveBatched: route each dedup item to its target vg
// (via the mnode cache or live lookup) and bucket the dedup-index into
// vg2Idx[vgId]. Also records the SEpSet per vg in vg2Ep for the fan-out phase.
static int32_t streamBatchRouteToVgs(SVnode *pVnode, SStreamVTableInfoCache *pCache,
                                     SArray *dedupItems, SHashObj *vg2Idx, SHashObj *vg2Ep) {
  int32_t acctId = 0;
  if (sscanf(pVnode->config.dbname, "%d.", &acctId) != 1) return TSDB_CODE_INVALID_PARA;

  int32_t dedupN = (int32_t)taosArrayGetSize(dedupItems);
  for (int32_t i = 0; i < dedupN; ++i) {
    SResolveWorkItem *w = taosArrayGet(dedupItems, i);

    char dbFName[TSDB_DB_FNAME_LEN] = {0};
    (void)tsnprintf(dbFName, sizeof(dbFName), "%d.%s", acctId, w->refDbName);

    SUseDbRsp *pRsp      = NULL;
    bool       fromCache = false;
    int32_t    rc = streamGetOrFetchDbVgInfo(pVnode, pCache, dbFName, &pRsp, &fromCache);
    if (rc != 0) {
      stError("vgId:%d %s uid=%" PRId64 " getDbVgInfo db=%s rc=0x%x -> propagate",
              TD_VID(pVnode), __func__, w->originVtbUid, dbFName, rc);
      return rc;
    }

    int32_t vgId  = 0;
    SEpSet  epSet = {0};
    rc = streamRouteTableToVg(pRsp, dbFName, w->refTableName, &vgId, &epSet);
    if (!fromCache && pCache == NULL) {
      tFreeSUsedbRsp(pRsp);
      taosMemoryFree(pRsp);
    }
    if (rc != 0) {
      stError("vgId:%d %s uid=%" PRId64 " routeTableToVg db=%s tb=%s rc=0x%x -> propagate",
              TD_VID(pVnode), __func__, w->originVtbUid, dbFName, w->refTableName, rc);
      return rc;
    }

    SArray **ppList = (SArray **)taosHashGet(vg2Idx, &vgId, sizeof(vgId));
    SArray  *pList  = NULL;
    if (ppList == NULL) {
      pList = taosArrayInit(4, sizeof(int32_t));
      if (pList == NULL) return terrno;
      if (taosHashPut(vg2Idx, &vgId, sizeof(vgId), &pList, sizeof(pList)) != 0) {
        taosArrayDestroy(pList);
        return terrno;
      }
      if (taosHashPut(vg2Ep, &vgId, sizeof(vgId), &epSet, sizeof(epSet)) != 0) return terrno;
    } else {
      pList = *ppList;
    }
    if (taosArrayPush(pList, &i) == NULL) return terrno;
  }
  return 0;
}

// Phase 3a sub-step: run the local vg's items synchronously via vnodeResolveOneHop.
// Returns the first OOM (if any); other per-item errors are stashed in dst->code
// without aborting the loop, matching the original behavior.
static int32_t streamBatchExecuteLocalVg(SVnode *pVnode, SArray *dedupItems, SArray *pList,
                                         SArray *dedupRspItems) {
  int32_t cnt = (int32_t)taosArrayGetSize(pList);
  for (int32_t j = 0; j < cnt; ++j) {
    int32_t           pos = *(int32_t *)taosArrayGet(pList, j);
    SResolveWorkItem *w   = taosArrayGet(dedupItems, pos);
    SVTableRefResolveItem q = {0};
    q.kind   = w->kind;
    q.hasRef = true;
    tstrncpy(q.refDbName,    w->refDbName,    TSDB_DB_NAME_LEN);
    tstrncpy(q.refTableName, w->refTableName, TSDB_TABLE_NAME_LEN);
    tstrncpy(q.refColName,   w->refColName,   TSDB_COL_NAME_LEN);
    SVTableRefResolveRspItem *dst = taosArrayGet(dedupRspItems, pos);
    int32_t one = vnodeResolveOneHop(pVnode, &q, dst);
    if (one != 0) {
      dst->code = one;
      if (one == TSDB_CODE_OUT_OF_MEMORY) return one;
    }
  }
  return 0;
}

// Phase 3 of streamCallResolveBatched: fan-out one RPC per remote vg + drain.
// See the header comment of streamCallResolveBatched for the concurrency model.
//
// Signalling: a single heap-allocated SStreamFanoutSync (binary sem + atomic
// pending + atomic refs) is shared across every fired RPC handle.
//   - pending gates the sem post: the cb that drives pending->0 posts the sem.
//   - refs gates sync lifetime: driver holds 1, each successful fire holds 1,
//     whichever side drives refs->0 destroys the sync.
//
// Wait uses tsem2_timewait. On timeout the driver atomically hands off each
// still-INFLIGHT handle to its (future) cb via a per-handle state CAS, so
// late cbs free both their handle and (when last) the shared sync without
// touching driver-frame storage. The driver then releases its sync ref and
// returns TSDB_CODE_TIMEOUT_ERROR.
static int32_t streamBatchFanoutDrain(SVnode *pVnode, int64_t ver, SArray *dedupItems,
                                      SHashObj *vg2Idx, SHashObj *vg2Ep, SArray *dedupRspItems) {
  int32_t            code        = 0;
  void              *pIter       = NULL;
  SStreamFanoutSync *pSync       = NULL;
  SArray            *rpcHandles  = NULL;

  // Pre-reserve capacity equal to the number of remote vgs so taosArrayPush
  // does not need to grow / allocate during fan-out.
  int32_t nRemoteVgs = taosHashGetSize(vg2Idx);
  if (nRemoteVgs < 1) nRemoteVgs = 1;
  rpcHandles = taosArrayInit(nRemoteVgs, sizeof(SStreamVgRpcHandle *));
  if (rpcHandles == NULL) {
    code = terrno;
    stError("vgId:%d %s init rpcHandles failed: code=0x%x", TD_VID(pVnode), __func__, code);
    goto _exit;
  }

  pSync = streamFanoutSyncCreate();
  if (pSync == NULL) {
    code = terrno;
    stError("vgId:%d %s create sync failed: code=0x%x", TD_VID(pVnode), __func__, code);
    goto _exit;
  }
  // Reserve a "fan-out in progress" slot so `pending` cannot transiently
  // hit 0 between firing handles; released right before tsem2_timewait.
  atomic_store_32(&pSync->pending, 1);
  // Driver holds one lifetime ref; each successful fire adds one. Whichever
  // party drives refs->0 calls streamFanoutSyncDestroy.
  atomic_store_32(&pSync->refs, 1);

  // Phase 3a: process local-vg in-process; fire async RPC for each remote vg.
  // On any error, stop iterating immediately and jump to cleanup; already
  // fired RPCs are still drained in _exit to preserve callback ownership.
  pIter = taosHashIterate(vg2Idx, NULL);
  while (pIter != NULL) {
    SArray  *pList  = *(SArray **)pIter;
    size_t   keyLen = 0;
    int32_t *pVgKey = (int32_t *)taosHashGetKey(pIter, &keyLen);
    int32_t  vgId   = *pVgKey;
    SEpSet  *pEpSet = (SEpSet *)taosHashGet(vg2Ep, &vgId, sizeof(vgId));

    if (vgId == TD_VID(pVnode)) {
      int32_t rc = streamBatchExecuteLocalVg(pVnode, dedupItems, pList, dedupRspItems);
      stTrace("vgId:%d %s local-vg done: targetVgId=%d items=%d rc=0x%x", TD_VID(pVnode),
              __func__, vgId, (int32_t)taosArrayGetSize(pList), rc);
      if (rc != 0) {
        code = rc;
        stError("vgId:%d %s local-vg failed: targetVgId=%d code=0x%x",
                TD_VID(pVnode), __func__, vgId, code);
        goto _exit;
      }
    } else {
      // Pre-allocate and push the handle BEFORE firing. This way the only
      // race-free failure modes are alloc/push (no RPC in flight) and
      // prepare-fire (handle owned by array; cleanup loop frees it).
      SStreamVgRpcHandle *h = taosMemoryCalloc(1, sizeof(SStreamVgRpcHandle));
      if (h == NULL) {
        code = terrno;
        stError("vgId:%d %s alloc handle failed: targetVgId=%d code=0x%x",
                TD_VID(pVnode), __func__, vgId, code);
        goto _exit;
      }
      h->vgId      = vgId;
      h->state     = VG_HANDLE_INFLIGHT;  // explicit; calloc already zeroed it
      h->indexList = pList;
      h->ctx.pSync = pSync;

      if (taosArrayPush(rpcHandles, &h) == NULL) {
        // Capacity was pre-reserved above so this should not happen.
        code = terrno;
        stError("vgId:%d %s push handle failed: targetVgId=%d code=0x%x",
                TD_VID(pVnode), __func__, vgId, code);
        streamDestroyVgRpcHandle(&h);
        goto _exit;
      }

      int32_t rc = streamPrepareAndFireOneVgResolve(pVnode, pEpSet, ver,
                                                    dedupItems, pList, h);
      if (rc != 0) {
        // h is already in rpcHandles; the cleanup loop in _exit frees it.
        // No callback fired, so sync counters are untouched.
        code = rc;
        stError("vgId:%d %s prepare/fire failed: targetVgId=%d code=0x%x",
                TD_VID(pVnode), __func__, vgId, code);
        goto _exit;
      }
    }
    pIter = taosHashIterate(vg2Idx, pIter);
  }

_exit:
  if (pIter != NULL) {
    taosHashCancelIterate(vg2Idx, pIter);
    pIter = NULL;
  }

  // Phase 3b: release the driver's pending reservation, then either skip
  // the wait (nothing in flight), wait normally, or time out and hand off.
  if (pSync != NULL) {
    int32_t remaining = atomic_sub_fetch_32(&pSync->pending, 1);
    stTrace("vgId:%d %s drain release: remaining=%d", TD_VID(pVnode), __func__, remaining);
    if (remaining != 0) {
      int32_t waitRc = tsem2_timewait(&pSync->sem, STREAM_VTB_RPC_TIMEOUT_MS);
      if (waitRc != 0) {
        // Real timeout (a post racing exactly with the deadline would have
        // been consumed by timewait itself). Atomically abandon every still
        // in-flight handle. Each successful CAS transfers ownership of that
        // handle to its (future) cb.
        stWarn("vgId:%d %s fan-out timed out after %dms; abandoning in-flight handles",
               TD_VID(pVnode), __func__, STREAM_VTB_RPC_TIMEOUT_MS);
        if (rpcHandles != NULL) {
          int32_t nHandles = (int32_t)taosArrayGetSize(rpcHandles);
          for (int32_t i = 0; i < nHandles; ++i) {
            SStreamVgRpcHandle **pSlot = (SStreamVgRpcHandle **)taosArrayGet(rpcHandles, i);
            SStreamVgRpcHandle  *h     = *pSlot;
            if (h == NULL) continue;
            int8_t prev = atomic_val_compare_exchange_8(&h->state,
                                                        VG_HANDLE_INFLIGHT,
                                                        VG_HANDLE_DRIVER_GONE);
            if (prev == VG_HANDLE_INFLIGHT) {
              // Handoff succeeded: cb will free this handle when it eventually
              // fires. Null the slot so the cleanup loop below skips it.
              *pSlot = NULL;
            }
            // else: cb already completed (state == VG_HANDLE_CB_DONE); the
            // driver still owns h and the cleanup loop will destroy it.
          }
        }
        code = TSDB_CODE_TIMEOUT_ERROR;
      }
    }
  }

  // Phase 3c: on success scatter rsp from each handle; on error skip the
  // scatter (results are not consumable) and just destroy handles owned
  // by the driver. NULL slots (abandoned on timeout) are owned by cbs.
  if (rpcHandles != NULL) {
    int32_t nHandles = (int32_t)taosArrayGetSize(rpcHandles);
    for (int32_t i = 0; i < nHandles; ++i) {
      SStreamVgRpcHandle **pSlot = (SStreamVgRpcHandle **)taosArrayGet(rpcHandles, i);
      SStreamVgRpcHandle  *h     = *pSlot;
      if (h == NULL) continue;
      if (code == 0) {
        int32_t rc = streamScatterOneVgResolve(pVnode, h, dedupRspItems);
        stTrace("vgId:%d %s remote-vg done: targetVgId=%d items=%d rc=0x%x", TD_VID(pVnode),
                __func__, h->vgId, (int32_t)taosArrayGetSize(h->indexList), rc);
        if (rc != 0) {
          code = rc;
          stError("vgId:%d %s scatter failed: targetVgId=%d code=0x%x",
                  TD_VID(pVnode), __func__, h->vgId, code);
        }
      }
      streamDestroyVgRpcHandle(&h);
      *pSlot = NULL;
    }
    taosArrayDestroy(rpcHandles);
  }

  // Release the driver's lifetime ref on the sync. If any cbs are still
  // pending (timeout path), the last one frees it; otherwise this call
  // does. After this point pSync must not be touched.
  TAOS_UNUSED(streamFanoutSyncRelease(pSync));
  return code;
}

// Phase 4 of streamCallResolveBatched: publish per-(table,col) results into
// the tblRefCache (so future hops in this resolve cycle see them) and scatter
// dedup results back to the caller's outRspItems[] positions, deep-copying
// tagData since one dedup slot may fan out to multiple original positions.
static int32_t streamBatchScatterAndPublish(SStreamVTableInfoCache *pCache, SArray *dedupItems,
                                            SArray *dedupRspItems, int32_t *origToDedupIdx,
                                            int32_t n, SArray *outRspItems) {
  int32_t dedupN = (int32_t)taosArrayGetSize(dedupItems);
  for (int32_t i = 0; i < dedupN; ++i) {
    SResolveWorkItem         *w   = taosArrayGet(dedupItems, i);
    SVTableRefResolveRspItem *rsp = taosArrayGet(dedupRspItems, i);
    streamTblRefCacheInsert(pCache, w->refDbName, w->refTableName, w->refColName, w->kind, rsp);
  }

  for (int32_t i = 0; i < n; ++i) {
    int32_t dedupIdx = origToDedupIdx[i];
    if (dedupIdx < 0) continue;  // already filled from cache in Phase 1
    SVTableRefResolveRspItem *src = taosArrayGet(dedupRspItems, dedupIdx);
    SVTableRefResolveRspItem *dst = taosArrayGet(outRspItems, i);
    int32_t rc = streamWriteRspItemDeepCopy(src, dst);
    if (rc != 0) {
      // OOM during scatter: dst has incomplete tagData; propagate error so
      // the caller can fail the batch rather than return corrupted results.
      return rc;
    }
  }
  return 0;
}

//
// streamCallResolveBatched: drive one hop of resolution with table-level dedup.
//
// Optimization (Issue 4): instead of sending per-(table,column) items blindly,
// we (a) check the local tblRefCache first, (b) deduplicate by (db,table,col)
// so the same physical column is only resolved once per RPC round, and (c) cache
// the results for use in subsequent hops.
//
// Concurrency (review finding #4): remote per-vg RPCs are FAN-OUT — phase 3a
// fires all asyncSends in a tight loop, phase 3b drains every fired handle.
// Total wall time is bounded by max(per-vg RTT) instead of sum(per-vg RTT).
// The local vg (if present) is still processed synchronously in phase 3a since
// it bypasses RPC entirely.
//
// H2 v0.5 strict: any per-vg routing/RPC failure is propagated upward as the
// return value. Per-item business errors are still reported through
// outRspItems[i].code so the caller can include the originating uid/cid in
// its log; the caller (streamResolveVTableRefChain) decides how to react.
//
// Returns 0 on success; non-zero on OOM, routing, or RPC failure.
//
// This driver is intentionally thin — each phase lives in its own helper
// (streamBatchTryCacheAndDedup / streamBatchRouteToVgs / streamBatchFanoutDrain
// / streamBatchScatterAndPublish) so the per-phase invariants are easier to
// read and modify in isolation.
static int32_t streamCallResolveBatched(SVnode *pVnode, SStreamVTableInfoCache *pCache,
                                        int64_t ver, SArray *batch, SArray *outRspItems) {
  int32_t   code     = 0;
  SHashObj *vg2Idx   = NULL;  // key: int32_t vgId, value: SArray<int32_t>* (positions in dedupItems)
  SHashObj *vg2Ep    = NULL;  // key: int32_t vgId, value: SEpSet
  SHashObj *dedupMap = NULL;  // key: "db\0table\0col", value: int32_t (position in dedupItems)
  SArray   *dedupItems    = NULL;  // SArray<SResolveWorkItem> unique items to send
  SArray   *dedupRspItems = NULL;  // SArray<SVTableRefResolveRspItem> responses for dedup items
  int32_t  *origToDedupIdx = NULL;  // batch index -> dedupItems index, or -1 if served from cache

  int32_t n = (int32_t)taosArrayGetSize(batch);
  stDebug("vgId:%d %s enter: ver=%" PRId64 " batch=%d", TD_VID(pVnode), __func__, ver, n);
  // Pre-size outRspItems with n zero entries so positional writes are safe.
  for (int32_t i = (int32_t)taosArrayGetSize(outRspItems); i < n; ++i) {
    SVTableRefResolveRspItem zero = {0};
    if (taosArrayPush(outRspItems, &zero) == NULL) { code = terrno; goto _end; }
  }

  // ---- Phase 1: cache lookup + dedup ----
  dedupMap   = taosHashInit(n, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  dedupItems = taosArrayInit(n, sizeof(SResolveWorkItem));
  if (dedupMap == NULL || dedupItems == NULL) { code = terrno; goto _end; }
  origToDedupIdx = taosMemoryCalloc(n, sizeof(int32_t));
  if (origToDedupIdx == NULL) { code = terrno; goto _end; }

  int32_t cacheHits = 0;
  code = streamBatchTryCacheAndDedup(pCache, batch, outRspItems, dedupMap, dedupItems,
                                     origToDedupIdx, &cacheHits);
  if (code != 0) goto _end;

  int32_t dedupN = (int32_t)taosArrayGetSize(dedupItems);
  stDebug("vgId:%d %s dedup: batch=%d cacheHits=%d dedupItems=%d",
          TD_VID(pVnode), __func__, n, cacheHits, dedupN);
  if (dedupN == 0) goto _end;  // all items served from cache

  // ---- Phase 2: route dedup items to vg groups ----
  vg2Idx = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  vg2Ep  = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (vg2Idx == NULL || vg2Ep == NULL) { code = terrno; goto _end; }
  code = streamBatchRouteToVgs(pVnode, pCache, dedupItems, vg2Idx, vg2Ep);
  if (code != 0) goto _end;

  // ---- Phase 3: fan-out + drain ----
  dedupRspItems = taosArrayInit(dedupN, sizeof(SVTableRefResolveRspItem));
  if (dedupRspItems == NULL) { code = terrno; goto _end; }
  for (int32_t i = 0; i < dedupN; ++i) {
    SVTableRefResolveRspItem zero = {0};
    if (taosArrayPush(dedupRspItems, &zero) == NULL) { code = terrno; goto _end; }
  }
  code = streamBatchFanoutDrain(pVnode, ver, dedupItems, vg2Idx, vg2Ep, dedupRspItems);
  if (code != 0) {
    stError("vgId:%d %s fan-out failed: rc=0x%x", TD_VID(pVnode), __func__, code);
    goto _end;
  }

  // ---- Phase 4: publish to cache + scatter to outRspItems ----
  code = streamBatchScatterAndPublish(pCache, dedupItems, dedupRspItems, origToDedupIdx, n, outRspItems);
  if (code != 0) {
    stError("vgId:%d %s scatter failed (OOM): rc=0x%x", TD_VID(pVnode), __func__, code);
  }

_end:
  stDebug("vgId:%d %s exit: code=0x%x", TD_VID(pVnode), __func__, code);
  taosMemoryFreeClear(origToDedupIdx);
  if (vg2Idx != NULL) {
    void *p = taosHashIterate(vg2Idx, NULL);
    while (p != NULL) {
      taosArrayDestroy(*(SArray **)p);
      p = taosHashIterate(vg2Idx, p);
    }
    taosHashCleanup(vg2Idx);
  }
  if (vg2Ep != NULL) taosHashCleanup(vg2Ep);
  if (dedupMap != NULL) taosHashCleanup(dedupMap);
  taosArrayDestroy(dedupItems);
  if (dedupRspItems != NULL) {
    // Free any remaining tagData in dedup responses that were not transferred
    int32_t sz = (int32_t)taosArrayGetSize(dedupRspItems);
    for (int32_t i = 0; i < sz; ++i) {
      SVTableRefResolveRspItem *r = taosArrayGet(dedupRspItems, i);
      taosMemoryFreeClear(r->tagData);
    }
    taosArrayDestroy(dedupRspItems);
  }
  return code;
}

// Function A: drive multi-hop chain resolution for a batch of vtable uids on the
// triggering vnode. Cross-vgId version: groups each batch by target vgId, then
// dispatches one TDMT_VND_VTABLE_REF_RESOLVE RPC per group via streamCallResolveBatched.
//
// H2 v0.5 strict error policy:
//   - top-level uid not in local meta (or not a vtable type) -> warn + skip
//     that uid; function returns 0 and uid simply has no entry in *ppUid2Result.
//   - any other error (mid-chain table/col/tag missing, RPC failure, OOM,
//     hop > MAX_HOPS, ref-triple inconsistency) -> A returns the underlying
//     errCode; caller (reader -> trigger -> mnode) propagates and fail-fasts.
// pCache (optional): caches db routing info (SUseDbRsp) across calls.
// pReaderInfo (optional): when vtbUids is NULL/empty, all live uids are pulled from
//                          qStreamGetTableArrayList(pReaderInfo). If both are NULL/empty
//                          this function returns INVALID_PARA.
// Output: *ppUid2Result is a fresh SSHashObj<uid -> SVTableResolveResult*>;
// caller owns it and must use streamVTableResolveResultDestroy + tSimpleHashCleanup.
// Full-uid branch helper: pull live (non-deleted) uids from the reader's table
// list into a newly-allocated SArray<int64_t>. The caller owns both *ppFullUids
// and *ppTableListArray and must free them. On failure both outputs are NULL.
static int32_t streamCollectActiveVtableUids(SStreamTriggerReaderInfo *pReaderInfo,
                                             SArray **ppTableListArray, SArray **ppFullUids) {
  *ppTableListArray = NULL;
  *ppFullUids       = NULL;
  if (pReaderInfo == NULL) return TSDB_CODE_INVALID_PARA;

  SArray *pTableListArray = qStreamGetTableArrayList(pReaderInfo);
  if (pTableListArray == NULL) return terrno;

  int32_t nAll     = (int32_t)taosArrayGetSize(pTableListArray);
  SArray *fullUids = taosArrayInit(nAll, sizeof(int64_t));
  if (fullUids == NULL) {
    taosArrayDestroyP(pTableListArray, taosMemFree);
    return terrno;
  }
  for (int32_t i = 0; i < nAll; ++i) {
    SStreamTableKeyInfo *pKey = taosArrayGetP(pTableListArray, i);
    if (pKey == NULL || pKey->markedDeleted) continue;
    if (taosArrayPush(fullUids, &pKey->uid) == NULL) {
      taosArrayDestroy(fullUids);
      taosArrayDestroyP(pTableListArray, taosMemFree);
      return terrno;
    }
  }
  taosArrayRemoveDuplicate(fullUids, compareInt64Val, NULL);
  *ppTableListArray = pTableListArray;
  *ppFullUids       = fullUids;
  return 0;
}

// Consume one hop's rspItems[]: for each (workItem, rspItem) pair, either
// propagate the per-item error, materialize the terminated result into
// uid2Result (colMap or tagMap), or push the follow-up work item into
// nextWorkList. This helper takes ownership of every rspItem.tagData (frees
// it on every path) so the caller only has to manage the array itself.
static int32_t streamConsumeOneHopResults(SVnode *pVnode, int32_t hop, SArray *workList,
                                          SArray *rspItems, SSHashObj *uid2Result,
                                          SArray *nextWorkList) {
  int32_t bn = (int32_t)taosArrayGetSize(workList);
  for (int32_t i = 0; i < bn; ++i) {
    SResolveWorkItem         *w = taosArrayGet(workList, i);
    SVTableRefResolveRspItem *r = taosArrayGet(rspItems, i);

    if (r->code != 0) {
      // H2 v0.5: any per-item business error (mid-chain ref-table missing,
      // ref-col missing, tag changed, etc.) is propagated upward.
      stError("vgId:%d %s hop=%d uid=%" PRId64 " kind=%d cid=%d rspCode=0x%x -> propagate",
              TD_VID(pVnode), __func__, hop, w->originVtbUid, w->kind, w->originCid, r->code);
      int32_t rspCode = r->code;
      taosMemoryFreeClear(r->tagData);
      return rspCode;
    }

    if (r->terminated) {
      SVTableResolveResult *pRes = streamGetOrCreateUidResult(uid2Result, w->originVtbUid);
      if (pRes == NULL) { taosMemoryFreeClear(r->tagData); return terrno; }

      if (w->kind == STREAM_VREF_KIND_COL) {
        SColResolveItem *item = taosMemoryCalloc(1, sizeof(*item));
        if (item == NULL) { taosMemoryFreeClear(r->tagData); return terrno; }
        item->hasRef = r->nextRef.hasRef;
        if (item->hasRef) {
          tstrncpy(item->refDbName,    r->nextRef.refDbName,    TSDB_DB_NAME_LEN);
          tstrncpy(item->refTableName, r->nextRef.refTableName, TSDB_TABLE_NAME_LEN);
          tstrncpy(item->refColName,   r->nextRef.refColName,   TSDB_COL_NAME_LEN);
        }
        // Snapshot old pointer before put; free it only after successful put so
        // the hash never holds a dangling pointer (avoids double-free on cleanup).
        SColResolveItem **ppOld = (SColResolveItem **)tSimpleHashGet(pRes->colMap, &w->originCid, sizeof(w->originCid));
        SColResolveItem  *oldItem = (ppOld && *ppOld) ? *ppOld : NULL;
        if (tSimpleHashPut(pRes->colMap, &w->originCid, sizeof(w->originCid), &item, sizeof(item)) != 0) {
          taosMemoryFree(item);
          taosMemoryFreeClear(r->tagData);
          return terrno;
        }
        if (oldItem) { taosMemoryFree(oldItem); }
        stDebug("vgId:%d %s hop=%d uid=%" PRId64 " COL cid=%d TERMINATED hasRef=%d ref=%s.%s.%s -> colMap",
                TD_VID(pVnode), __func__, hop, w->originVtbUid, w->originCid,
                item->hasRef, item->refDbName, item->refTableName, item->refColName);
        taosMemoryFreeClear(r->tagData);
      } else {
        STagValue *tv = taosMemoryCalloc(1, sizeof(*tv));
        if (tv == NULL) { taosMemoryFreeClear(r->tagData); return terrno; }
        tv->type  = r->tagType;
        tv->nLen  = r->tagLen;
        tv->pData = r->tagData;
        r->tagData = NULL;  // ownership transferred to STagValue
        // Snapshot old pointer before put; free it only after successful put so
        // the hash never holds a dangling pointer (avoids double-free on cleanup).
        STagValue **ppOldTag = (STagValue **)tSimpleHashGet(pRes->tagMap, &w->originCid, sizeof(w->originCid));
        STagValue  *oldTag   = (ppOldTag && *ppOldTag) ? *ppOldTag : NULL;
        if (tSimpleHashPut(pRes->tagMap, &w->originCid, sizeof(w->originCid), &tv, sizeof(tv)) != 0) {
          taosMemoryFreeClear(tv->pData);
          taosMemoryFree(tv);
          return terrno;
        }
        if (oldTag) { taosMemoryFreeClear(oldTag->pData); taosMemoryFree(oldTag); }
      }
    } else {
      SResolveWorkItem next = {0};
      next.originVtbUid = w->originVtbUid;
      next.originCid    = w->originCid;
      next.kind         = r->nextRef.kind;
      tstrncpy(next.refDbName,    r->nextRef.refDbName,    TSDB_DB_NAME_LEN);
      tstrncpy(next.refTableName, r->nextRef.refTableName, TSDB_TABLE_NAME_LEN);
      tstrncpy(next.refColName,   r->nextRef.refColName,   TSDB_COL_NAME_LEN);
      if (taosArrayPush(nextWorkList, &next) == NULL) {
        taosMemoryFreeClear(r->tagData);
        return terrno;
      }
      stDebug("vgId:%d %s hop=%d uid=%" PRId64 " kind=%d cid=%d NEXT-HOP -> %s.%s.%s",
              TD_VID(pVnode), __func__, hop, w->originVtbUid, next.kind, w->originCid,
              next.refDbName, next.refTableName, next.refColName);
      taosMemoryFreeClear(r->tagData);
    }
  }
  return 0;
}

// Debug-only dump of the final per-uid resolve result. Pure stDebug; called
// once at the end of streamResolveVTableRefChain to ease post-mortem.
static void streamLogFinalResolveResult(SVnode *pVnode, SSHashObj *uid2Result) {
  void   *p  = NULL;
  int32_t it = 0;
  while ((p = tSimpleHashIterate(uid2Result, p, &it)) != NULL) {
    int64_t               uid = *(int64_t *)tSimpleHashGetKey(p, NULL);
    SVTableResolveResult *res = *(SVTableResolveResult **)p;
    stDebug("vgId:%d %s FINAL uid=%" PRId64 " colMapSz=%d tagMapSz=%d",
            TD_VID(pVnode), __func__, uid,
            tSimpleHashGetSize(res->colMap),
            tSimpleHashGetSize(res->tagMap));
    void *cp = NULL; int32_t ci = 0;
    while ((cp = tSimpleHashIterate(res->colMap, cp, &ci)) != NULL) {
      col_id_t         cid  = *(col_id_t *)tSimpleHashGetKey(cp, NULL);
      SColResolveItem *item = *(SColResolveItem **)cp;
      stDebug("vgId:%d %s   FINAL uid=%" PRId64 " COL cid=%d hasRef=%d ref=%s.%s.%s",
              TD_VID(pVnode), __func__, uid, cid, item ? item->hasRef : -1,
              item ? item->refDbName : "", item ? item->refTableName : "",
              item ? item->refColName : "");
    }
    void *tp = NULL; int32_t ti = 0;
    while ((tp = tSimpleHashIterate(res->tagMap, tp, &ti)) != NULL) {
      col_id_t   cid = *(col_id_t *)tSimpleHashGetKey(tp, NULL);
      STagValue *tv  = *(STagValue **)tp;
      stDebug("vgId:%d %s   FINAL uid=%" PRId64 " TAG cid=%d type=%d nLen=%d",
              TD_VID(pVnode), __func__, uid, cid, tv ? tv->type : -1, tv ? tv->nLen : -1);
    }
  }
}

int32_t streamResolveVTableRefChain(SVnode *pVnode, SStreamVTableInfoCache *pCache,
                                    SStreamTriggerReaderInfo *pReaderInfo, int64_t ver,
                                    SArray *vtbUids, SArray *virtColCids, SArray *virtTagCids,
                                    SSHashObj **ppUid2Result) {
  int32_t    code         = 0;
  SArray    *workList     = NULL;
  SArray    *nextWorkList = NULL;
  SArray    *rspItems     = NULL;
  SSHashObj *uid2Result   = NULL;

  SArray    *fullUids     = NULL;
  SArray    *pTableListArray = NULL;

  if (pVnode == NULL || ppUid2Result == NULL) return TSDB_CODE_INVALID_PARA;
  *ppUid2Result = NULL;

  // Invalidate per-table ref cache at the start of each full resolve cycle.
  // The cache is only useful within a single multi-hop resolve call to avoid
  // redundant RPC for the same (db,table,col) across hops; stale results from
  // a previous cycle could mask schema changes.
  if (pCache) {
    taosHashClear(pCache->tblRefCache);
  }

  stDebug("vgId:%d %s enter: ver=%" PRId64 " vtbUids=%d virtCols=%d virtTags=%d", TD_VID(pVnode),
          __func__, ver, (int32_t)taosArrayGetSize(vtbUids),
          (int32_t)taosArrayGetSize(virtColCids), (int32_t)taosArrayGetSize(virtTagCids));

  // Full-uid branch: pull live uids from the reader's table list.
  if (vtbUids == NULL || taosArrayGetSize(vtbUids) == 0) {
    code = streamCollectActiveVtableUids(pReaderInfo, &pTableListArray, &fullUids);
    if (code != 0) goto _end;
    vtbUids = fullUids;
    stDebug("vgId:%d %s full-uid branch: tableList=%d activeUids=%d", TD_VID(pVnode), __func__,
            (int32_t)taosArrayGetSize(pTableListArray), (int32_t)taosArrayGetSize(fullUids));
  }

  uid2Result = tSimpleHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (uid2Result == NULL) { code = terrno; goto _end; }
  tSimpleHashSetFreeFp(uid2Result, streamVTableResolveResultDestroy);

  workList = taosArrayInit(64, sizeof(SResolveWorkItem));
  if (workList == NULL) { code = terrno; goto _end; }

  // 1. seed work-list. H2 v0.5: streamPushInitialWorkItemsForUid swallows
  //    top-level uid-not-exist (warn + return 0 without entry); any other
  //    error (col/tag not in ref triple, OOM) is propagated upward so the
  //    caller (reader -> trigger -> mnode) can fail-fast and trigger a
  //    redeploy.
  int32_t nUid = (int32_t)taosArrayGetSize(vtbUids);
  for (int32_t i = 0; i < nUid; ++i) {
    int64_t uid = *(int64_t *)taosArrayGet(vtbUids, i);
    int32_t rc  = streamPushInitialWorkItemsForUid(pVnode, uid, virtColCids, virtTagCids, workList, uid2Result);
    if (rc == 0) continue;
    stError("vgId:%d %s seed uid=%" PRId64 " push rc=0x%x -> propagate (strict)",
            TD_VID(pVnode), __func__, uid, rc);
    code = rc;
    goto _end;
  }
  stDebug("vgId:%d %s after seed: workListSz=%d uid2ResultSz=%d",
          TD_VID(pVnode), __func__,
          (int32_t)taosArrayGetSize(workList),
          tSimpleHashGetSize(uid2Result));

  // 2. main hop loop
  for (int32_t hop = 0; hop < STREAM_VTB_MAX_HOPS; ++hop) {
    int32_t cur = (int32_t)taosArrayGetSize(workList);
    stDebug("vgId:%d %s hop=%d workListSz=%d", TD_VID(pVnode), __func__, hop, cur);
    if (cur == 0) break;

    rspItems = taosArrayInit(cur, sizeof(SVTableRefResolveRspItem));
    if (rspItems == NULL) { code = terrno; goto _end; }

    int32_t rc = streamCallResolveBatched(pVnode, pCache, ver, workList, rspItems);
    if (rc != 0) {
      // H2 v0.5: any error (OOM, routing, RPC) propagates immediately.
      code = rc;
      goto _end;
    }

    nextWorkList = taosArrayInit(cur, sizeof(SResolveWorkItem));
    if (nextWorkList == NULL) { code = terrno; goto _end; }

    code = streamConsumeOneHopResults(pVnode, hop, workList, rspItems, uid2Result, nextWorkList);
    if (code != 0) goto _end;

    taosArrayDestroy(rspItems); rspItems = NULL;
    taosArrayDestroy(workList);
    workList     = nextWorkList;
    nextWorkList = NULL;
  }

  // 3. hop overflow: any leftover work-items mean the chain exceeded MAX_HOPS.
  //    H2 v0.5: report TSDB_CODE_STREAM_VTB_REF_TOO_DEEP rather than silently
  //    skipping the offending uids.
  if (workList != NULL) {
    int32_t leftover = (int32_t)taosArrayGetSize(workList);
    if (leftover > 0) {
      for (int32_t i = 0; i < leftover; ++i) {
        SResolveWorkItem *w = taosArrayGet(workList, i);
        stError("vgId:%d %s OVERFLOW uid=%" PRId64 " kind=%d cid=%d ref=%s.%s.%s",
                TD_VID(pVnode), __func__, w->originVtbUid, w->kind, w->originCid,
                w->refDbName, w->refTableName, w->refColName);
      }
      stError("vgId:%d %s HOP_OVERFLOW leftover=%d -> propagate TOO_DEEP",
              TD_VID(pVnode), __func__, leftover);
      code = TSDB_CODE_STREAM_VTB_REF_TOO_DEEP;
      goto _end;
    }
  }

  // Final dump: per-uid colMap/tagMap contents.
  streamLogFinalResolveResult(pVnode, uid2Result);

  *ppUid2Result = uid2Result;
  uid2Result    = NULL;

_end:
  stDebug("vgId:%d %s exit: code=0x%x outUidCnt=%d", TD_VID(pVnode), __func__, code,
          uid2Result ? tSimpleHashGetSize(uid2Result) :
          (*ppUid2Result ? tSimpleHashGetSize(*ppUid2Result) : 0));
  if (fullUids        != NULL) taosArrayDestroy(fullUids);
  if (pTableListArray != NULL) taosArrayDestroyP(pTableListArray, taosMemFree);
  if (workList     != NULL) taosArrayDestroy(workList);
  if (nextWorkList != NULL) taosArrayDestroy(nextWorkList);
  if (rspItems     != NULL) {
    int32_t m = (int32_t)taosArrayGetSize(rspItems);
    for (int32_t i = 0; i < m; ++i) {
      SVTableRefResolveRspItem *r = taosArrayGet(rspItems, i);
      taosMemoryFreeClear(r->tagData);
    }
    taosArrayDestroy(rspItems);
  }
  tSimpleHashCleanup(uid2Result);

  return code;
}

// ============================================================================
// C3: vchild-tag chain helper (executor side)
// vnodeResolveVTableTagChain
//
// For trigger streams whose source is a virtual super table, executor's
// `getColInfoResultForGroupbyForStream` needs literal tag values per vchild to
// compute the partition groupId. The default `metaGetTableTagsByUidsVersion`
// only reads ctbEntry.pTags directly, so col-ref tags resolve to NULL and all
// vchildren collapse into the same group.
//
// This helper post-processes the STUidTagInfo list: when suid is a virtual
// stable, each vchild uid is fed into `streamResolveVTableRefChain` (which
// already handles multi-hop and cross-vnode resolution), and the returned tag
// values are repacked into a fresh STag in stable-schemaTag order. Failures
// per uid are best-effort and leave the original pTagVal untouched.
// ============================================================================
int32_t vnodeResolveVTableTagChain(void *pVnode, int64_t suid, SArray *pUidTagList) {
  if (pVnode == NULL || pUidTagList == NULL) return 0;

  int32_t      code         = 0;
  SVnode      *pVn          = (SVnode *)pVnode;
  stTrace("vgId:%d %s ENTER suid=%" PRId64 " nUids=%d", TD_VID(pVn), __func__, suid,
          (int32_t)taosArrayGetSize(pUidTagList));
  SMetaReader  mr           = {0};
  bool         readerInited = false;
  SArray      *uids         = NULL;
  SArray      *tagCids      = NULL;
  SArray      *tagVals      = NULL;
  SSHashObj   *uid2Result = NULL;
  int32_t      nTagCols     = 0;

  int32_t nUids = (int32_t)taosArrayGetSize(pUidTagList);
  if (nUids == 0) return 0;

  // 1) confirm suid refers to a virtual super table; otherwise no-op.
  metaReaderDoInit(&mr, pVn->pMeta, META_READER_LOCK, 0);
  readerInited = true;
  if (metaReaderGetTableEntryByUid(&mr, suid) != 0) {
    stDebug("vgId:%d %s metaReader miss suid=%" PRId64, TD_VID(pVn), __func__, suid);
    goto _end;
  }
  if (mr.me.type != TSDB_SUPER_TABLE || !TABLE_IS_VIRTUAL(mr.me.flags)) {
    stDebug("vgId:%d %s skip suid=%" PRId64 " type=%d flags=0x%x", TD_VID(pVn), __func__,
            suid, (int32_t)mr.me.type, (uint32_t)mr.me.flags);
    goto _end;
  }
  SSchema *pTagSchema = mr.me.stbEntry.schemaTag.pSchema;
  nTagCols = mr.me.stbEntry.schemaTag.nCols;
  if (pTagSchema == NULL || nTagCols <= 0) {
    goto _end;
  }

  stTrace("vgId:%d %s suid=%" PRId64 " nUids=%d nTagCols=%d", TD_VID(pVn), __func__,
          suid, nUids, nTagCols);

  tagCids = taosArrayInit(nTagCols, sizeof(col_id_t));
  if (tagCids == NULL) {
    code = terrno;
    goto _end;
  }
  for (int32_t i = 0; i < nTagCols; ++i) {
    col_id_t cid = pTagSchema[i].colId;
    if (taosArrayPush(tagCids, &cid) == NULL) {
      code = terrno;
      goto _end;
    }
  }

  metaReaderClear(&mr);
  readerInited = false;

  // 2) build uid array for the chain resolver.
  uids = taosArrayInit(nUids, sizeof(int64_t));
  if (uids == NULL) { code = terrno; goto _end; }
  for (int32_t i = 0; i < nUids; ++i) {
    STUidTagInfo *p = taosArrayGet(pUidTagList, i);
    if (p == NULL) continue;
    int64_t uid = (int64_t)p->uid;
    if (taosArrayPush(uids, &uid) == NULL) { code = terrno; goto _end; }
  }

  // 3) chain-resolve.
  code = streamResolveVTableRefChain(pVn, NULL, NULL, -1, uids, NULL, tagCids, &uid2Result);
  if (code != 0 || uid2Result == NULL) {
    stTrace("vgId:%d %s chain resolve rc=0x%x uid2Result=%p", TD_VID(pVn), __func__,
            code, (void *)uid2Result);
    code = 0;  // best-effort; do not propagate to caller
    goto _end;
  }

  // 4) rebuild STag per uid by merging:
  //      - literal STagVals already present in p->pTagVal (vchild may declare
  //        some tags as plain literals and only some as colRefs)
  //      - chain-resolved STagValues from streamResolveVTableRefChain
  //    The chain resolver only fills cids that appeared as colRefs, so without
  //    this merge the literal tags would be lost when we tTagNew a fresh STag.
  for (int32_t i = 0; i < nUids; ++i) {
    STUidTagInfo *p = taosArrayGet(pUidTagList, i);
    if (p == NULL) continue;
    int64_t uid = (int64_t)p->uid;

    SVTableResolveResult **ppRes =
        (SVTableResolveResult **)tSimpleHashGet(uid2Result, &uid, sizeof(uid));
    SVTableResolveResult  *pRes  = (ppRes != NULL) ? *ppRes : NULL;
    bool hasResolvedTags = (pRes != NULL && pRes->tagMap != NULL && tSimpleHashGetSize(pRes->tagMap) > 0);
    stTrace("vgId:%d %s merge uid=%" PRId64 " ppRes=%p pRes=%p tagMapSz=%d",
            TD_VID(pVn), __func__, uid, (void *)ppRes, (void *)pRes,
            (pRes && pRes->tagMap) ? tSimpleHashGetSize(pRes->tagMap) : -1);

    if (tagVals != NULL) {
      taosArrayClear(tagVals);
    } else {
      tagVals = taosArrayInit(nTagCols, sizeof(STagVal));
      if (tagVals == NULL) { code = terrno; goto _end; }
    }

    bool anyChange = false;
    for (int32_t j = 0; j < nTagCols; ++j) {
      col_id_t cid = *(col_id_t *)taosArrayGet(tagCids, j);

      // Prefer chain-resolved value when present (overrides any stale literal).
      if (hasResolvedTags) {
        STagValue **ppTV = (STagValue **)tSimpleHashGet(pRes->tagMap, &cid, sizeof(cid));
        if (ppTV != NULL && *ppTV != NULL) {
          STagValue *tv = *ppTV;
          if (tv->pData != NULL && tv->nLen > 0) {
            STagVal v = {0};
            v.cid  = cid;
            v.type = tv->type;
            if (IS_VAR_DATA_TYPE(tv->type)) {
              v.nData = (uint32_t)tv->nLen;
              v.pData = (uint8_t *)tv->pData;
            } else {
              int32_t copyLen = tv->nLen < (int32_t)sizeof(int64_t) ? tv->nLen : (int32_t)sizeof(int64_t);
              memcpy(&v.i64, tv->pData, copyLen);
            }
            if (taosArrayPush(tagVals, &v) == NULL) { code = terrno; goto _end; }
            anyChange = true;
            continue;
          }
        }
      }

      // Fall back to the literal tag in the original STag, if any.
      if (p->pTagVal != NULL) {
        STagVal probe = {.cid = cid};
        if (tTagGet((const STag *)p->pTagVal, &probe)) {
          if (taosArrayPush(tagVals, &probe) == NULL) { code = terrno; goto _end; }
        }
      }
    }

    if (!anyChange) continue;  // nothing was resolved -> keep the original STag

    STag   *pNewTag = NULL;
    int32_t rc      = tTagNew(tagVals, 1, false, &pNewTag);
    if (rc != 0 || pNewTag == NULL) {
      stDebug("vgId:%d %s uid=%" PRId64 " tTagNew rc=0x%x -> keep original", TD_VID(pVn),
              __func__, uid, rc);
      continue;
    }
    if (p->pTagVal != NULL) taosMemoryFree(p->pTagVal);
    p->pTagVal = pNewTag;
    stDebug("vgId:%d %s uid=%" PRId64 " rebuilt STag with %d tag(s) (literals merged)",
            TD_VID(pVn), __func__, uid, (int32_t)taosArrayGetSize(tagVals));
  }

_end:
  if (readerInited) metaReaderClear(&mr);
  if (uids    != NULL) taosArrayDestroy(uids);
  if (tagCids != NULL) taosArrayDestroy(tagCids);
  if (tagVals != NULL) taosArrayDestroy(tagVals);
  tSimpleHashCleanup(uid2Result);
  return code;
}
