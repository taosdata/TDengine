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
    int64_t msec = taosGetTimestampMs();
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
  if (NULL == dbCache->stbCache) {
    return;
  }

  void *pIter = taosHashIterate(dbCache->viewCache, NULL);
  while (pIter) {
    uint64_t viewId = ((SCtgViewCache*)pIter)->pMeta->viewId;

    if (TSDB_CODE_SUCCESS ==
        ctgMetaRentRemove(&pCtg->viewRent, viewId, ctgViewVersionSortCompare, ctgViewVersionSearchCompare)) {
      ctgDebug("viewId:0x%" PRIx64 ", view removed from rent", viewId);
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
