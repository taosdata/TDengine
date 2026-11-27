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

#include "catalogInt.h"
#include "query.h"
#include "systable.h"
#include "tname.h"
#include "trpc.h"

int32_t ctgMetaRentInit(SCtgRentMgmt *mgmt, uint32_t rentSec, int8_t type, int32_t size) {
  mgmt->slotRIdx = 0;
  mgmt->slotNum = rentSec / CTG_RENT_SLOT_SECOND;
  mgmt->type = type;
  mgmt->metaSize = size;

  size_t msgSize = sizeof(SCtgRentSlot) * mgmt->slotNum;

  mgmt->slots = taosMemoryCalloc(1, msgSize);
  if (NULL == mgmt->slots) {
    qError("calloc %d failed", (int32_t)msgSize);
    CTG_ERR_RET(terrno);
  }

  mgmt->rentCacheSize = msgSize;

  qDebug("meta rent initialized, type:%d, slotNum:%d", type, mgmt->slotNum);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgMetaRentAdd(SCtgRentMgmt *mgmt, void *meta, int64_t id, int32_t size) {
  int16_t widx = abs((int)(id % mgmt->slotNum));

  SCtgRentSlot *slot = &mgmt->slots[widx];
  int32_t       code = 0;

  CTG_LOCK(CTG_WRITE, &slot->lock);
  if (NULL == slot->meta) {
    slot->meta = taosArrayInit(CTG_DEFAULT_RENT_SLOT_SIZE, size);
    if (NULL == slot->meta) {
      qError("id:0x%" PRIx64 ", taosArrayInit %d failed, slot idx:%d, type:%d", id, CTG_DEFAULT_RENT_SLOT_SIZE, widx,
             mgmt->type);
      CTG_ERR_JRET(terrno);
    }
  }

  if (NULL == taosArrayPush(slot->meta, meta)) {
    qError("id:0x%" PRIx64 ", taosArrayPush meta to rent failed, slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(terrno);
  }

  mgmt->rentCacheSize += size;
  slot->needSort = true;

  qDebug("id:0x%" PRIx64 ", add meta to rent, slot idx:%d, type:%d", id, widx, mgmt->type);

_return:

  CTG_UNLOCK(CTG_WRITE, &slot->lock);
  CTG_RET(code);
}

int32_t ctgMetaRentUpdate(SCtgRentMgmt *mgmt, void *meta, int64_t id, int32_t size, __compar_fn_t sortCompare,
                          __compar_fn_t searchCompare) {
  int16_t widx = abs((int)(id % mgmt->slotNum));

  SCtgRentSlot *slot = &mgmt->slots[widx];
  int32_t       code = 0;

  CTG_LOCK(CTG_WRITE, &slot->lock);
  if (NULL == slot->meta) {
    qDebug("id:0x%" PRIx64 ", empty meta slot, slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (slot->needSort) {
    qDebug("id:0x%" PRIx64 ", meta slot before sort, slot idx:%d, type:%d, size:%d", id, widx, mgmt->type,
           (int32_t)taosArrayGetSize(slot->meta));
    taosArraySort(slot->meta, sortCompare);
    slot->needSort = false;
    qDebug("id:0x%" PRIx64 ", meta slot sorted, slot idx:%d, type:%d, size:%d", id, widx, mgmt->type,
           (int32_t)taosArrayGetSize(slot->meta));
  }

  void *orig = taosArraySearch(slot->meta, &id, searchCompare, TD_EQ);
  if (NULL == orig) {
    qDebug("id:0x%" PRIx64 ", meta not found in slot, slot idx:%d, type:%d, size:%d", id, widx, mgmt->type,
           (int32_t)taosArrayGetSize(slot->meta));
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  TAOS_MEMCPY(orig, meta, size);

  qDebug("id:0x%" PRIx64 ", meta in rent updated, slot idx:%d, type:%d", id, widx, mgmt->type);

_return:

  CTG_UNLOCK(CTG_WRITE, &slot->lock);

  if (code) {
    qDebug("id:0x%" PRIx64 ", meta in rent update failed, will try to add it, code:0x%x, slot idx:%d, type:%d", id,
           code, widx, mgmt->type);
    CTG_RET(ctgMetaRentAdd(mgmt, meta, id, size));
  }

  CTG_RET(code);
}

int32_t ctgMetaRentRemove(SCtgRentMgmt *mgmt, int64_t id, __compar_fn_t sortCompare, __compar_fn_t searchCompare) {
  int16_t widx = abs((int)(id % mgmt->slotNum));

  SCtgRentSlot *slot = &mgmt->slots[widx];
  int32_t       code = 0;

  CTG_LOCK(CTG_WRITE, &slot->lock);
  if (NULL == slot->meta) {
    qError("id:0x%" PRIx64 ", empty meta slot, slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (slot->needSort) {
    taosArraySort(slot->meta, sortCompare);
    slot->needSort = false;
    qDebug("id:0x%" PRIx64 ", meta slot sorted, slot idx:%d, type:%d", id, widx, mgmt->type);
  }

  int32_t idx = taosArraySearchIdx(slot->meta, &id, searchCompare, TD_EQ);
  if (idx < 0) {
    qError("id:0x%" PRIx64 ", meta not found in slot, slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  taosArrayRemove(slot->meta, idx);
  mgmt->rentCacheSize -= mgmt->metaSize;

  qDebug("id:0x%" PRIx64 ", meta in rent removed, slot idx:%d, type:%d", id, widx, mgmt->type);

_return:

  CTG_UNLOCK(CTG_WRITE, &slot->lock);

  CTG_RET(code);
}

int32_t ctgMetaRentGetImpl(SCtgRentMgmt *mgmt, void **res, uint32_t *num, int32_t size) {
  int16_t ridx = atomic_add_fetch_16(&mgmt->slotRIdx, 1);
  if (ridx >= mgmt->slotNum) {
    ridx %= mgmt->slotNum;
    atomic_store_16(&mgmt->slotRIdx, ridx);
  }

  SCtgRentSlot *slot = &mgmt->slots[ridx];
  int32_t       code = 0;

  CTG_LOCK(CTG_READ, &slot->lock);
  if (NULL == slot->meta) {
    qDebug("empty meta in slot:%d, type:%d", ridx, mgmt->type);
    *num = 0;
    goto _return;
  }

  size_t metaNum = taosArrayGetSize(slot->meta);
  if (metaNum <= 0) {
    qDebug("no meta in slot:%d, type:%d", ridx, mgmt->type);
    *num = 0;
    goto _return;
  }

  size_t msize = metaNum * size;
  *res = taosMemoryMalloc(msize);
  if (NULL == *res) {
    qError("malloc %d failed", (int32_t)msize);
    CTG_ERR_JRET(terrno);
  }

  void *meta = taosArrayGet(slot->meta, 0);
  if (NULL == meta) {
    qError("get the 0th meta in slot failed, total:%d", (int32_t)metaNum);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  TAOS_MEMCPY(*res, meta, msize);

  *num = (uint32_t)metaNum;

  qDebug("get %d meta from rent, type:%d", (int32_t)metaNum, mgmt->type);

_return:

  CTG_UNLOCK(CTG_READ, &slot->lock);

  CTG_RET(code);
}

int32_t ctgMetaRentGet(SCtgRentMgmt *mgmt, void **res, uint32_t *num, int32_t size) {
  while (true) {
    int64_t msec = taosGetMonoTimestampMs();
    int64_t lsec = atomic_load_64(&mgmt->lastReadMsec);
    if ((msec - lsec) < CTG_RENT_SLOT_SECOND * 1000) {
      *res = NULL;
      *num = 0;
      qDebug("too short time period to get expired meta, type:%d", mgmt->type);
      return TSDB_CODE_SUCCESS;
    }

    if (lsec != atomic_val_compare_exchange_64(&mgmt->lastReadMsec, lsec, msec)) {
      continue;
    }

    break;
  }

  CTG_ERR_RET(ctgMetaRentGetImpl(mgmt, res, num, size));

  return TSDB_CODE_SUCCESS;
}

/*
 * Copy all entries of one specific rent slot. Used by the view heartbeat sweep, which
 * collects several slots per round and therefore cannot use the round-robin getter.
 * The caller owns *res.
 */
int32_t ctgMetaRentGetSlotByIdx(SCtgRentMgmt *mgmt, int32_t slotIdx, void **res, uint32_t *num, int32_t size) {
  int32_t       code = 0;
  void         *buf = NULL;
  SCtgRentSlot *slot = NULL;

  if (NULL == mgmt || NULL == res || NULL == num || size <= 0) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  *res = NULL;
  *num = 0;

  if (slotIdx < 0 || slotIdx >= mgmt->slotNum) {
    qError("invalid slot idx:%d, slotNum:%d, type:%d", slotIdx, mgmt->slotNum, mgmt->type);
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  slot = &mgmt->slots[slotIdx];

  CTG_LOCK(CTG_READ, &slot->lock);
  if (NULL == slot->meta) {
    goto _return;
  }

  size_t metaNum = taosArrayGetSize(slot->meta);
  if (metaNum <= 0) {
    goto _return;
  }

  int64_t msize = (int64_t)metaNum * size;
  if (msize <= 0 || msize > INT32_MAX) {
    qError("invalid slot meta size:%" PRId64 ", metaNum:%d, type:%d", msize, (int32_t)metaNum, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  buf = taosMemoryMalloc(msize);
  if (NULL == buf) {
    qError("malloc %" PRId64 " failed, type:%d", msize, mgmt->type);
    CTG_ERR_JRET(terrno);
  }

  void *meta = taosArrayGet(slot->meta, 0);
  if (NULL == meta) {
    qError("get the 0th meta in slot failed, slot idx:%d, total:%d, type:%d", slotIdx, (int32_t)metaNum, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  TAOS_MEMCPY(buf, meta, msize);
  *res = buf;
  *num = (uint32_t)metaNum;
  buf = NULL;

  qDebug("get %d meta from rent slot:%d, type:%d", (int32_t)metaNum, slotIdx, mgmt->type);

_return:

  CTG_UNLOCK(CTG_READ, &slot->lock);
  taosMemoryFree(buf);

  CTG_RET(code);
}

void ctgRemoveStbRent(SCatalog *pCtg, SCtgDBCache *dbCache) {
  if (NULL == dbCache->stbCache) {
    return;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  void   *pIter = taosHashIterate(dbCache->stbCache, NULL);
  while (pIter) {
    uint64_t *pSuid = taosHashGetKey(pIter, NULL);
    uint64_t  suid = taosGetUInt64Aligned(pSuid);

    code = ctgMetaRentRemove(&pCtg->stbRent, suid, ctgStbVersionSortCompare, ctgStbVersionSearchCompare);
    if (TSDB_CODE_SUCCESS == code) {
      ctgDebug("suid:0x%" PRIx64 ", stb removed from rent", suid);
    }

    pIter = taosHashIterate(dbCache->stbCache, pIter);
  }
}

void ctgRemoveViewRent(SCatalog *pCtg, SCtgDBCache *dbCache) {
  if (NULL == dbCache->viewCache) {
    return;
  }

  void *pIter = taosHashIterate(dbCache->viewCache, NULL);
  while (pIter) {
    SCtgViewCache *pCache = (SCtgViewCache *)pIter;
    if (NULL != pCache->pMeta) {
      uint64_t viewId = pCache->pMeta->viewId;

      if (TSDB_CODE_SUCCESS ==
          ctgMetaRentRemove(&pCtg->viewRent, viewId, ctgViewVersionSortCompare, ctgViewVersionSearchCompare)) {
        ctgDebug("viewId:0x%" PRIx64 ", view removed from rent", viewId);
      }
    } else {
      ctgWarn("view cache entry has no meta, skip rent removal, dbId:0x%" PRIx64, dbCache->dbId);
    }

    pIter = taosHashIterate(dbCache->viewCache, pIter);
  }
}

void ctgRemoveTSMARent(SCatalog *pCtg, SCtgDBCache *dbCache) {
  if (!dbCache->tsmaCache) return;

  void* pIter = taosHashIterate(dbCache->tsmaCache, NULL);
  while (pIter) {
    SCtgTSMACache* pCtgCache = pIter;
    
    CTG_LOCK(CTG_READ, &pCtgCache->tsmaLock);
    int32_t size = (pCtgCache && pCtgCache->pTsmas) ? pCtgCache->pTsmas->size : 0;
    for (int32_t i = 0; i < size; ++i) {
      STSMACache* pCache = taosArrayGetP(pCtgCache->pTsmas, i);
      if (TSDB_CODE_SUCCESS == ctgMetaRentRemove(&pCtg->tsmaRent, pCache->tsmaId, ctgTSMAVersionSortCompare, ctgTSMAVersionSearchCompare)) {
        ctgDebug("tsma:0x%" PRIx64 ", tsma removed from rent, name:%s.%s.%s", pCache->tsmaId, pCache->dbFName, pCache->tb, pCache->name);
      }
    }
    CTG_UNLOCK(CTG_READ, &pCtgCache->tsmaLock);
    
    pIter = taosHashIterate(dbCache->tsmaCache, pIter);
  }
}

int32_t ctgUpdateRentStbVersion(SCatalog *pCtg, char *dbFName, char *tbName, uint64_t dbId, uint64_t suid,
                                SCtgTbCache *pCache) {
  SSTableVersion metaRent = {.dbId = dbId, .suid = suid};
  if (pCache->pMeta) {
    metaRent.sversion = pCache->pMeta->sversion;
    metaRent.tversion = pCache->pMeta->tversion;
  }

  if (pCache->pIndex) {
    metaRent.smaVer = pCache->pIndex->version;
  }

  tstrncpy(metaRent.dbFName, dbFName, sizeof(metaRent.dbFName));
  tstrncpy(metaRent.stbName, tbName, sizeof(metaRent.stbName));

  CTG_ERR_RET(ctgMetaRentUpdate(&pCtg->stbRent, &metaRent, metaRent.suid, sizeof(SSTableVersion),
                                ctgStbVersionSortCompare, ctgStbVersionSearchCompare));

  ctgDebug("suid:0x%" PRIx64 ", db %s, dbId:0x%" PRIx64 ", stb:%s, sver:%d tver:%d smaVer:%d updated to stbRent", suid,
           dbFName, dbId, tbName, metaRent.sversion, metaRent.tversion, metaRent.smaVer);

  return TSDB_CODE_SUCCESS;
}

                                
int32_t ctgUpdateRentViewVersion(SCatalog *pCtg, char *dbFName, char *viewName, uint64_t dbId, uint64_t viewId,
                                SCtgViewCache *pCache) {
  SViewVersion metaRent = {.dbId = dbId, .viewId = viewId};
  metaRent.version = pCache->pMeta->version;

  tstrncpy(metaRent.dbFName, dbFName, sizeof(metaRent.dbFName));
  tstrncpy(metaRent.viewName, viewName, sizeof(metaRent.viewName));

  CTG_ERR_RET(ctgMetaRentUpdate(&pCtg->viewRent, &metaRent, metaRent.viewId, sizeof(SViewVersion),
                                ctgViewVersionSortCompare, ctgViewVersionSearchCompare));

  ctgDebug("viewId:0x%" PRIx64 ", db %s, dbId:0x%" PRIx64 ", view %s, version:%d updated to viewRent", viewId, dbFName, dbId, viewName, metaRent.version);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgUpdateRentTSMAVersion(SCatalog *pCtg, char *dbFName, const STSMACache *pCache) {
  const STableTSMAInfo *pTsmaInfo = pCache;
  STSMAVersion    tsmaRent = {.dbId = pTsmaInfo->dbId, .tsmaId = pTsmaInfo->tsmaId, .version = pTsmaInfo->version};
  tstrncpy(tsmaRent.name, pTsmaInfo->name, TSDB_TABLE_NAME_LEN);
  tstrncpy(tsmaRent.dbFName, dbFName, TSDB_DB_FNAME_LEN);
  tstrncpy(tsmaRent.tbName, pTsmaInfo->tb, TSDB_TABLE_NAME_LEN);

  CTG_ERR_RET(ctgMetaRentUpdate(&pCtg->tsmaRent, &tsmaRent, tsmaRent.tsmaId, sizeof(STSMAVersion),
                                ctgTSMAVersionSortCompare, ctgTSMAVersionSearchCompare));

  ctgDebug("tsma:0x%" PRIx64 ", db:%s, dbId:0x%" PRIx64 ", view:%s, version:%d updated to tsmaRent", pTsmaInfo->tsmaId,
           dbFName, tsmaRent.dbId, pTsmaInfo->name, pTsmaInfo->version);

  return TSDB_CODE_SUCCESS;
}

/* -------------------------------------------------------------------------
 * View metadata heartbeat sweep
 *
 * The client reports at most one batch of cached view versions per heartbeat.
 * To keep the global view version meaningful, the committed dynViewVer is only
 * advanced after every rent slot has been validated against the same pending
 * version. Slots are collected round-robin; a round carries at least
 * CTG_VIEW_HB_MIN_NUM views (or all remaining slots when fewer views exist).
 * ------------------------------------------------------------------------- */

static bool ctgViewVerEqual(const SDynViewVersion *pLeft, const SDynViewVersion *pRight) {
  return pLeft->svrBootTs == pRight->svrBootTs && pLeft->dynViewVer == pRight->dynViewVer;
}

static bool ctgViewSlotIsVerified(const SCtgViewSweep *pSweep, uint32_t slot) {
  return (pSweep->verifiedSlots[slot >> 3] & (uint8_t)(1u << (slot & 7))) != 0;
}

static void ctgViewSlotSetVerified(SCtgViewSweep *pSweep, uint32_t slot) {
  pSweep->verifiedSlots[slot >> 3] |= (uint8_t)(1u << (slot & 7));
}

static int32_t ctgViewSweepEnsureBuffers(SCtgViewSweep *pSweep, uint32_t slotNum) {
  uint32_t bytes = (slotNum + 7) / 8;

  if (NULL == pSweep->verifiedSlots || pSweep->verifiedCapacity < bytes) {
    uint8_t *pBitmap = taosMemoryCalloc(1, bytes);
    if (NULL == pBitmap) {
      qError("failed to alloc %u bytes for view sweep bitmap, code:0x%x", bytes, terrno);
      CTG_ERR_RET(terrno);
    }

    taosMemoryFree(pSweep->verifiedSlots);
    pSweep->verifiedSlots = pBitmap;
    pSweep->verifiedCapacity = bytes;
    // A new bitmap means the old verified set is gone; restart the verification.
    pSweep->verifiedNum = 0;
  }

  if (NULL == pSweep->sentSlots || pSweep->sentCapacity < slotNum) {
    uint32_t *pSlots = taosMemoryMalloc((int64_t)slotNum * sizeof(uint32_t));
    if (NULL == pSlots) {
      qError("failed to alloc %u sent slots for view sweep, code:0x%x", slotNum, terrno);
      CTG_ERR_RET(terrno);
    }

    taosMemoryFree(pSweep->sentSlots);
    pSweep->sentSlots = pSlots;
    pSweep->sentCapacity = slotNum;
  }

  return TSDB_CODE_SUCCESS;
}

// Commit the pending version once every rent slot has been verified.
// The caller must hold the sweep write lock.
static void ctgViewSweepCommit(SCatalog *pCtg, SCtgViewSweep *pSweep) {
  atomic_store_64(&pCtg->dynViewVer.svrBootTs, pSweep->pendingVer.svrBootTs);
  atomic_store_64(&pCtg->dynViewVer.dynViewVer, (int64_t)pSweep->pendingVer.dynViewVer);

  ctgDebug("view hb sweep: committed dyn view ver:%" PRIu64 ", bootTs:%" PRId64 ", verified slots:%u",
           pSweep->pendingVer.dynViewVer, pSweep->pendingVer.svrBootTs, pSweep->verifiedNum);

  pSweep->hasPending = false;
  pSweep->verifiedNum = 0;
  pSweep->sentNum = 0;
  if (NULL != pSweep->verifiedSlots && pSweep->verifiedCapacity > 0) {
    TAOS_MEMSET(pSweep->verifiedSlots, 0, pSweep->verifiedCapacity);
  }
}

void ctgViewSweepFree(SCtgViewSweep *pSweep) {
  if (NULL == pSweep) {
    return;
  }

  taosMemoryFreeClear(pSweep->verifiedSlots);
  taosMemoryFreeClear(pSweep->sentSlots);
  TAOS_MEMSET(pSweep, 0, sizeof(*pSweep));
}

/*
 * Collect the view versions to report in the next heartbeat.
 *
 * The returned list is a flat array owned by the caller. `dynViewVersion` receives the
 * committed global version (also caller-owned) even when no views are returned; the
 * caller frees it on the early-return path.
 *
 * While a previous round is still waiting for its response, no new batch is produced
 * (unless the response timeout fires), so a response can always be mapped to the slots
 * that were sent.
 */
int32_t ctgGetViewsForHbCheck(SCatalog *pCtg, int64_t reqId, SViewVersion **views, uint32_t *num,
                              SDynViewVersion **dynViewVersion) {
  int32_t          code = TSDB_CODE_SUCCESS;
  SArray          *pViews = NULL;
  SViewVersion    *pFlat = NULL;
  SDynViewVersion *pVer = NULL;
  SCtgViewSweep   *pSweep = NULL;
  uint32_t         slotNum = 0;
  uint32_t         total = 0;
  uint32_t         scanned = 0;
  uint32_t         slot = 0;
  int64_t          now = 0;

  if (NULL == pCtg || NULL == views || NULL == num || NULL == dynViewVersion) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  *views = NULL;
  *num = 0;
  *dynViewVersion = NULL;

  pVer = taosMemoryCalloc(1, sizeof(SDynViewVersion));
  if (NULL == pVer) {
    CTG_ERR_RET(terrno);
  }
  pVer->svrBootTs = atomic_load_64(&pCtg->dynViewVer.svrBootTs);
  pVer->dynViewVer = (uint64_t)atomic_load_64(&pCtg->dynViewVer.dynViewVer);
  *dynViewVersion = pVer;

  pViews = taosArrayInit(CTG_VIEW_HB_MIN_NUM, sizeof(SViewVersion));
  if (NULL == pViews) {
    code = terrno;
    goto _return;
  }

  pSweep = &pCtg->viewSweep;
  CTG_LOCK(CTG_WRITE, &pSweep->lock);

  now = taosGetTimestampMs();
  if (pSweep->awaitingRsp) {
    if (now - pSweep->sentMsec < CTG_VIEW_HB_RSP_TIMEOUT_MS) {
      ctgDebug("view hb sweep: reqId:%" PRId64 " still awaiting rsp, skip this round", pSweep->sentReqId);
      goto _unlock;
    }

    ctgWarn("view hb sweep: no rsp for reqId:%" PRId64 " after %" PRId64 "ms, resend", pSweep->sentReqId,
            now - pSweep->sentMsec);
    pSweep->awaitingRsp = false;
    pSweep->sentNum = 0;
  }

  slotNum = pCtg->viewRent.slotNum;
  if (0 == slotNum) {
    ctgWarn("view rent slot number is 0, skip view hb check, type:%d, metaSize:%d", pCtg->viewRent.type,
            pCtg->viewRent.metaSize);
    goto _unlock;
  }

  code = ctgViewSweepEnsureBuffers(pSweep, slotNum);
  if (TSDB_CODE_SUCCESS != code) {
    goto _unlock;
  }

  pSweep->sentNum = 0;
  pSweep->nextSlot %= slotNum;
  slot = pSweep->nextSlot;

  while (scanned < slotNum && total < CTG_VIEW_HB_MIN_NUM) {
    void    *slotViews = NULL;
    uint32_t slotViewNum = 0;

    if (pSweep->hasPending && ctgViewSlotIsVerified(pSweep, slot)) {
      slot = (slot + 1) % slotNum;
      scanned++;
      continue;
    }

    code = ctgMetaRentGetSlotByIdx(&pCtg->viewRent, (int32_t)slot, &slotViews, &slotViewNum, sizeof(SViewVersion));
    if (TSDB_CODE_SUCCESS != code) {
      ctgError("view hb sweep: failed to read rent slot:%u since %s", slot, tstrerror(code));
      goto _unlock;
    }

    if (0 == slotViewNum) {
      // An empty slot needs no server-side validation; mark it verified immediately.
      if (pSweep->hasPending && !ctgViewSlotIsVerified(pSweep, slot)) {
        ctgViewSlotSetVerified(pSweep, slot);
        pSweep->verifiedNum++;
        ctgDebug("view hb sweep: empty slot:%u marked verified, verified:%u/%u", slot, pSweep->verifiedNum, slotNum);
      }
    } else {
      if (NULL == taosArrayAddBatch(pViews, slotViews, slotViewNum)) {
        code = terrno;
        ctgError("view hb sweep: failed to append %u views from slot:%u", slotViewNum, slot);
        taosMemoryFree(slotViews);
        goto _unlock;
      }

      pSweep->sentSlots[pSweep->sentNum++] = slot;
      total += slotViewNum;
      ctgDebug("view hb sweep: slot:%u carries %u views, total:%u", slot, slotViewNum, total);
    }

    taosMemoryFree(slotViews);

    pSweep->nextSlot = (slot + 1) % slotNum;
    slot = pSweep->nextSlot;
    scanned++;
  }

  // All slots verified without needing this round's response (e.g. only empty slots
  // remained): commit the pending version right away.
  if (pSweep->hasPending && pSweep->verifiedNum >= slotNum && 0 == total) {
    ctgViewSweepCommit(pCtg, pSweep);
  }

  if (total > 0) {
    pFlat = taosMemoryMalloc((int64_t)total * sizeof(SViewVersion));
    if (NULL == pFlat) {
      code = terrno;
      ctgError("view hb sweep: failed to alloc %u view versions", total);
      goto _unlock;
    }

    TAOS_MEMCPY(pFlat, taosArrayGet(pViews, 0), (size_t)total * sizeof(SViewVersion));
    *views = pFlat;
    *num = total;
    pFlat = NULL;

    pSweep->awaitingRsp = true;
    pSweep->sentMsec = now;
    pSweep->sentReqId = reqId;

    ctgDebug("view hb sweep: collected %u views from %u slots, reqId:%" PRId64 ", pending:%d", total,
             pSweep->sentNum, reqId, (int32_t)pSweep->hasPending);
  }

_unlock:
  CTG_UNLOCK(CTG_WRITE, &pSweep->lock);

_return:
  taosArrayDestroy(pViews);
  taosMemoryFree(pFlat);
  if (TSDB_CODE_SUCCESS != code) {
    taosMemoryFreeClear(*views);
    *num = 0;
  }

  CTG_RET(code);
}

/*
 * Apply the dyn view version carried by a heartbeat response.
 *
 * Only a response that matches the outstanding request may advance the sweep. When the
 * server version differs from the pending one the sweep restarts: the batch in this very
 * response was validated against the server's current state and its slots are marked
 * verified under the new version.
 */
int32_t ctgApplyViewHbVersion(SCatalog *pCtg, const SDynViewVersion *pVer, int64_t rspReqId) {
  int32_t          code = TSDB_CODE_SUCCESS;
  SCtgViewSweep   *pSweep = NULL;
  uint32_t         slotNum = 0;
  bool             restart = false;
  bool             wasPending = false;
  uint32_t         i = 0;

  if (NULL == pCtg || NULL == pVer) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  pSweep = &pCtg->viewSweep;
  CTG_LOCK(CTG_WRITE, &pSweep->lock);

  // rspReqId == 0 means the server did not echo the batch id (older mnode); only one
  // batch is outstanding at a time, so it can still be attributed safely.
  if (!pSweep->awaitingRsp || (0 != rspReqId && pSweep->sentReqId != rspReqId)) {
    ctgDebug("view hb sweep: ignore dyn-view rsp for stale request, rspReqId:%" PRId64 ", sentReqId:%" PRId64
             ", awaiting:%d",
             rspReqId, pSweep->sentReqId, (int32_t)pSweep->awaitingRsp);
    goto _return;
  }

  wasPending = pSweep->hasPending;
  restart = (!pSweep->hasPending || !ctgViewVerEqual(&pSweep->pendingVer, pVer));
  if (restart) {
    if (NULL != pSweep->verifiedSlots && pSweep->verifiedCapacity > 0) {
      TAOS_MEMSET(pSweep->verifiedSlots, 0, pSweep->verifiedCapacity);
    }
    pSweep->verifiedNum = 0;
    pSweep->hasPending = true;
    pSweep->pendingVer = *pVer;
    ctgDebug("view hb sweep: %s sweep for dyn ver:%" PRIu64 ", bootTs:%" PRId64, wasPending ? "restart" : "start",
             pVer->dynViewVer, pVer->svrBootTs);
  }

  slotNum = pCtg->viewRent.slotNum;
  for (i = 0; i < pSweep->sentNum; ++i) {
    uint32_t slot = pSweep->sentSlots[i];
    if (slot >= slotNum || slot >= pSweep->verifiedCapacity * 8) {
      continue;
    }

    if (!ctgViewSlotIsVerified(pSweep, slot)) {
      ctgViewSlotSetVerified(pSweep, slot);
      pSweep->verifiedNum++;
    }
  }

  ctgDebug("view hb sweep: rsp applied, reqId:%" PRId64 ", verified:%u/%u slots for dyn ver:%" PRIu64, rspReqId,
           pSweep->verifiedNum, slotNum, pSweep->pendingVer.dynViewVer);

  pSweep->awaitingRsp = false;
  pSweep->sentNum = 0;

_return:
  CTG_UNLOCK(CTG_WRITE, &pSweep->lock);

  CTG_RET(code);
}

/*
 * Commit the pending version when every slot has been verified. Called after a
 * heartbeat response has been fully processed, so the view cache updates from the same
 * response are already applied.
 */
int32_t ctgViewSweepTryCommit(SCatalog *pCtg) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SCtgViewSweep *pSweep = NULL;
  uint32_t       slotNum = 0;

  if (NULL == pCtg) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  pSweep = &pCtg->viewSweep;
  CTG_LOCK(CTG_WRITE, &pSweep->lock);

  slotNum = pCtg->viewRent.slotNum;
  if (pSweep->hasPending && !pSweep->awaitingRsp && slotNum > 0 && pSweep->verifiedNum >= slotNum) {
    ctgViewSweepCommit(pCtg, pSweep);
  }

  CTG_UNLOCK(CTG_WRITE, &pSweep->lock);

  CTG_RET(code);
}

/*
 * A heartbeat send/receive failure invalidates the outstanding round: its slots must be
 * re-sent. Verified slots from previous rounds are kept.
 */
int32_t ctgViewSweepOnHbError(SCatalog *pCtg) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SCtgViewSweep *pSweep = NULL;

  if (NULL == pCtg) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  pSweep = &pCtg->viewSweep;
  CTG_LOCK(CTG_WRITE, &pSweep->lock);

  if (pSweep->awaitingRsp) {
    ctgWarn("view hb sweep: heartbeat failed, drop outstanding reqId:%" PRId64, pSweep->sentReqId);
    pSweep->awaitingRsp = false;
    pSweep->sentNum = 0;
  }

  CTG_UNLOCK(CTG_WRITE, &pSweep->lock);

  CTG_RET(code);
}

/*
 * Finish the sweep bookkeeping for one heartbeat batch response.
 *
 * When the response did not carry HEARTBEAT_KEY_DYN_VIEW the server reported that the
 * client's committed version is current; the outstanding round (and any pending sweep,
 * which can only be obsolete in that case) is cleared without advancing anything.
 * `sawDynVer` is true when ctgApplyViewHbVersion already handled the round.
 */
int32_t ctgViewSweepOnHbRspDone(SCatalog *pCtg, int64_t rspReqId, bool sawDynVer) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SCtgViewSweep *pSweep = NULL;

  if (NULL == pCtg) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  pSweep = &pCtg->viewSweep;
  CTG_LOCK(CTG_WRITE, &pSweep->lock);

  if (sawDynVer) {
    // Already applied (awaitingRsp cleared) by ctgApplyViewHbVersion.
    goto _return;
  }

  if (!pSweep->awaitingRsp || (0 != rspReqId && pSweep->sentReqId != rspReqId)) {
    ctgDebug("view hb sweep: rsp done for non-outstanding request, rspReqId:%" PRId64 ", sentReqId:%" PRId64
             ", awaiting:%d",
             rspReqId, pSweep->sentReqId, (int32_t)pSweep->awaitingRsp);
    goto _return;
  }

  ctgDebug("view hb sweep: rsp reqId:%" PRId64 " has no dyn view ver, committed version is current", rspReqId);
  pSweep->awaitingRsp = false;
  pSweep->sentNum = 0;

  if (pSweep->hasPending) {
    // The server reports the committed version as current, so a pending sweep is stale.
    pSweep->hasPending = false;
    pSweep->verifiedNum = 0;
    if (NULL != pSweep->verifiedSlots && pSweep->verifiedCapacity > 0) {
      TAOS_MEMSET(pSweep->verifiedSlots, 0, pSweep->verifiedCapacity);
    }
  }

_return:
  CTG_UNLOCK(CTG_WRITE, &pSweep->lock);

  CTG_RET(code);
}
