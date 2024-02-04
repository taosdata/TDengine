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
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
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
      qError("taosArrayInit %d failed, id:0x%" PRIx64 ", slot idx:%d, type:%d", CTG_DEFAULT_RENT_SLOT_SIZE, id, widx,
             mgmt->type);
      CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  if (NULL == taosArrayPush(slot->meta, meta)) {
    qError("taosArrayPush meta to rent failed, id:0x%" PRIx64 ", slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  mgmt->rentCacheSize += size;
  slot->needSort = true;

  qDebug("add meta to rent, id:0x%" PRIx64 ", slot idx:%d, type:%d", id, widx, mgmt->type);

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
    qDebug("empty meta slot, id:0x%" PRIx64 ", slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (slot->needSort) {
    qDebug("meta slot before sorte, slot idx:%d, type:%d, size:%d", widx, mgmt->type,
           (int32_t)taosArrayGetSize(slot->meta));
    taosArraySort(slot->meta, sortCompare);
    slot->needSort = false;
    qDebug("meta slot sorted, slot idx:%d, type:%d, size:%d", widx, mgmt->type, (int32_t)taosArrayGetSize(slot->meta));
  }

  void *orig = taosArraySearch(slot->meta, &id, searchCompare, TD_EQ);
  if (NULL == orig) {
    qDebug("meta not found in slot, id:0x%" PRIx64 ", slot idx:%d, type:%d, size:%d", id, widx, mgmt->type,
           (int32_t)taosArrayGetSize(slot->meta));
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  memcpy(orig, meta, size);

  qDebug("meta in rent updated, id:0x%" PRIx64 ", slot idx:%d, type:%d", id, widx, mgmt->type);

_return:

  CTG_UNLOCK(CTG_WRITE, &slot->lock);

  if (code) {
    qDebug("meta in rent update failed, will try to add it, code:%x, id:0x%" PRIx64 ", slot idx:%d, type:%d", code, id,
           widx, mgmt->type);
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
    qError("empty meta slot, id:0x%" PRIx64 ", slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (slot->needSort) {
    taosArraySort(slot->meta, sortCompare);
    slot->needSort = false;
    qDebug("meta slot sorted, slot idx:%d, type:%d", widx, mgmt->type);
  }

  int32_t idx = taosArraySearchIdx(slot->meta, &id, searchCompare, TD_EQ);
  if (idx < 0) {
    qError("meta not found in slot, id:0x%" PRIx64 ", slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  taosArrayRemove(slot->meta, idx);
  mgmt->rentCacheSize -= mgmt->metaSize;

  qDebug("meta in rent removed, id:0x%" PRIx64 ", slot idx:%d, type:%d", id, widx, mgmt->type);

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
    CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  void *meta = taosArrayGet(slot->meta, 0);

  memcpy(*res, meta, msize);

  *num = (uint32_t)metaNum;

  qDebug("Got %d meta from rent, type:%d", (int32_t)metaNum, mgmt->type);

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

  void *pIter = taosHashIterate(dbCache->stbCache, NULL);
  while (pIter) {
    uint64_t *suid = NULL;
    suid = taosHashGetKey(pIter, NULL);

    if (TSDB_CODE_SUCCESS ==
        ctgMetaRentRemove(&pCtg->stbRent, *suid, ctgStbVersionSortCompare, ctgStbVersionSearchCompare)) {
      ctgDebug("stb removed from rent, suid:0x%" PRIx64, *suid);
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
      ctgDebug("view removed from rent, viewId:0x%" PRIx64, viewId);
    }

    pIter = taosHashIterate(dbCache->viewCache, pIter);
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

  ctgDebug("db %s,0x%" PRIx64 " stb %s,0x%" PRIx64 " sver %d tver %d smaVer %d updated to stbRent", dbFName, dbId,
           tbName, suid, metaRent.sversion, metaRent.tversion, metaRent.smaVer);

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

  ctgDebug("db %s,0x%" PRIx64 " view %s,0x%" PRIx64 " version %d updated to viewRent", dbFName, dbId, viewName, viewId, metaRent.version);

  return TSDB_CODE_SUCCESS;
}
