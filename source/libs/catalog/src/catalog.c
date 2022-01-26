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

#include "trpc.h"
#include "query.h"
#include "tname.h"
#include "catalogInt.h"

SCatalogMgmt ctgMgmt = {0};

SCTGDebug gCTGDebug = {0};


int32_t ctgGetDBVgroupFromCache(struct SCatalog* pCatalog, const char *dbName, SDBVgroupInfo **dbInfo, bool *inCache) {
  if (NULL == pCatalog->dbCache.cache) {
    *inCache = false;
    ctgWarn("empty db cache, dbName:%s", dbName);
    return TSDB_CODE_SUCCESS;
  }

  SDBVgroupInfo *info = NULL;

  while (true) {
    info = taosHashAcquire(pCatalog->dbCache.cache, dbName, strlen(dbName));

    if (NULL == info) {
      *inCache = false;
      ctgWarn("not in db vgroup cache, dbName:%s", dbName);
      return TSDB_CODE_SUCCESS;
    }

    CTG_LOCK(CTG_READ, &info->lock);
    if (NULL == info->vgInfo) {
      CTG_UNLOCK(CTG_READ, &info->lock);
      taosHashRelease(pCatalog->dbCache.cache, info);
      ctgWarn("db cache vgInfo is NULL, dbName:%s", dbName);
      
      continue;
    }

    break;
  }

  *dbInfo = info;
  *inCache = true;

  ctgDebug("Got db vgroup from cache, dbName:%s", dbName);
  
  return TSDB_CODE_SUCCESS;
}



int32_t ctgGetDBVgroupFromMnode(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, SBuildUseDBInput *input, SUseDbOutput *out) {
  char *msg = NULL;
  int32_t msgLen = 0;

  ctgDebug("try to get db vgroup from mnode, db:%s", input->db);

  int32_t code = queryBuildMsg[TMSG_INDEX(TDMT_MND_USE_DB)](input, &msg, 0, &msgLen);
  if (code) {
    ctgError("Build use db msg failed, code:%x, db:%s", code, input->db);
    CTG_ERR_RET(code);
  }
  
  SRpcMsg rpcMsg = {
      .msgType = TDMT_MND_USE_DB,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};

  rpcSendRecv(pRpc, (SEpSet*)pMgmtEps, &rpcMsg, &rpcRsp);
  if (TSDB_CODE_SUCCESS != rpcRsp.code) {
    ctgError("error rsp for use db, code:%s, db:%s", tstrerror(rpcRsp.code), input->db);
    CTG_ERR_RET(rpcRsp.code);
  }

  code = queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_USE_DB)](out, rpcRsp.pCont, rpcRsp.contLen);
  if (code) {
    ctgError("Process use db rsp failed, code:%x, db:%s", code, input->db);
    CTG_ERR_RET(code);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgIsTableMetaExistInCache(struct SCatalog* pCatalog, const char* tbFullName, int32_t *exist) {
  if (NULL == pCatalog->tableCache.cache) {
    *exist = 0;
    ctgWarn("empty tablemeta cache, tbName:%s", tbFullName);
    return TSDB_CODE_SUCCESS;
  }

  size_t sz = 0;
  STableMeta *tbMeta = taosHashGet(pCatalog->tableCache.cache, tbFullName, strlen(tbFullName));

  if (NULL == tbMeta) {
    *exist = 0;
    ctgDebug("tablemeta not in cache, tbName:%s", tbFullName);
    return TSDB_CODE_SUCCESS;
  }

  *exist = 1;
  
  ctgDebug("tablemeta is in cache, tbName:%s", tbFullName);
  
  return TSDB_CODE_SUCCESS;
}


int32_t ctgGetTableMetaFromCache(struct SCatalog* pCatalog, const SName* pTableName, STableMeta** pTableMeta, int32_t *exist) {
  if (NULL == pCatalog->tableCache.cache) {
    *exist = 0;
    ctgWarn("empty tablemeta cache, tbName:%s", pTableName->tname);
    return TSDB_CODE_SUCCESS;
  }

  char tbFullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pTableName, tbFullName);

  *pTableMeta = NULL;

  size_t sz = 0;
  STableMeta *tbMeta = taosHashGetCloneExt(pCatalog->tableCache.cache, tbFullName, strlen(tbFullName), NULL, (void **)pTableMeta, &sz);

  if (NULL == *pTableMeta) {
    *exist = 0;
    ctgDebug("tablemeta not in cache, tbName:%s", tbFullName);
    return TSDB_CODE_SUCCESS;
  }

  *exist = 1;
  
  tbMeta = *pTableMeta;

  if (tbMeta->tableType != TSDB_CHILD_TABLE) {
    ctgDebug("Got tablemeta from cache, tbName:%s", tbFullName);

    return TSDB_CODE_SUCCESS;
  }
  
  CTG_LOCK(CTG_READ, &pCatalog->tableCache.stableLock);
  
  STableMeta **stbMeta = taosHashGet(pCatalog->tableCache.stableCache, &tbMeta->suid, sizeof(tbMeta->suid));
  if (NULL == stbMeta || NULL == *stbMeta) {
    CTG_UNLOCK(CTG_READ, &pCatalog->tableCache.stableLock);
    ctgError("stable not in stableCache, suid:%"PRIx64, tbMeta->suid);
    tfree(*pTableMeta);
    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }

  if ((*stbMeta)->suid != tbMeta->suid) {    
    CTG_UNLOCK(CTG_READ, &pCatalog->tableCache.stableLock);
    tfree(*pTableMeta);
    ctgError("stable suid in stableCache mis-match, expected suid:%"PRIx64 ",actual suid:%"PRIx64, tbMeta->suid, (*stbMeta)->suid);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  int32_t metaSize = sizeof(STableMeta) + ((*stbMeta)->tableInfo.numOfTags + (*stbMeta)->tableInfo.numOfColumns) * sizeof(SSchema);
  *pTableMeta = realloc(*pTableMeta, metaSize);
  if (NULL == *pTableMeta) {    
    CTG_UNLOCK(CTG_READ, &pCatalog->tableCache.stableLock);
    ctgError("realloc size[%d] failed", metaSize);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  memcpy(&(*pTableMeta)->sversion, &(*stbMeta)->sversion, metaSize - sizeof(SCTableMeta));

  CTG_UNLOCK(CTG_READ, &pCatalog->tableCache.stableLock);

  ctgDebug("Got tablemeta from cache, tbName:%s", tbFullName);
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTableTypeFromCache(struct SCatalog* pCatalog, const SName* pTableName, int32_t *tbType) {
  if (NULL == pCatalog->tableCache.cache) {
    ctgWarn("empty tablemeta cache, tbName:%s", pTableName->tname);  
    return TSDB_CODE_SUCCESS;
  }

  char tbFullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pTableName, tbFullName);

  size_t sz = 0;
  STableMeta *pTableMeta = NULL;
  
  taosHashGetCloneExt(pCatalog->tableCache.cache, tbFullName, strlen(tbFullName), NULL, (void **)&pTableMeta, &sz);

  if (NULL == pTableMeta) {
    ctgWarn("tablemeta not in cache, tbName:%s", tbFullName);  
  
    return TSDB_CODE_SUCCESS;
  }

  *tbType = pTableMeta->tableType;

  ctgDebug("Got tabletype from cache, tbName:%s, type:%d", tbFullName, *tbType);  
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTableMetaFromMnodeImpl(struct SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, char* tbFullName, STableMetaOutput* output) {
  SBuildTableMetaInput bInput = {.vgId = 0, .dbName = NULL, .tableFullName = tbFullName};
  char *msg = NULL;
  SEpSet *pVnodeEpSet = NULL;
  int32_t msgLen = 0;

  ctgDebug("try to get table meta from mnode, tbName:%s", tbFullName);

  int32_t code = queryBuildMsg[TMSG_INDEX(TDMT_MND_STB_META)](&bInput, &msg, 0, &msgLen);
  if (code) {
    ctgError("Build mnode stablemeta msg failed, code:%x", code);
    CTG_ERR_RET(code);
  }

  SRpcMsg rpcMsg = {
      .msgType = TDMT_MND_STB_META,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};

  rpcSendRecv(pTransporter, (SEpSet*)pMgmtEps, &rpcMsg, &rpcRsp);
  
  if (TSDB_CODE_SUCCESS != rpcRsp.code) {
    if (CTG_TABLE_NOT_EXIST(rpcRsp.code)) {
      SET_META_TYPE_NONE(output->metaType);
      ctgDebug("stablemeta not exist in mnode, tbName:%s", tbFullName);
      return TSDB_CODE_SUCCESS;
    }
    
    ctgError("error rsp for stablemeta from mnode, code:%s, tbName:%s", tstrerror(rpcRsp.code), tbFullName);
    CTG_ERR_RET(rpcRsp.code);
  }

  code = queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_STB_META)](output, rpcRsp.pCont, rpcRsp.contLen);
  if (code) {
    ctgError("Process mnode stablemeta rsp failed, code:%x, tbName:%s", code, tbFullName);
    CTG_ERR_RET(code);
  }

  ctgDebug("Got table meta from mnode, tbName:%s", tbFullName);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTableMetaFromMnode(struct SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, STableMetaOutput* output) {
  char tbFullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pTableName, tbFullName);

  return ctgGetTableMetaFromMnodeImpl(pCatalog, pTransporter, pMgmtEps, tbFullName, output);
}

int32_t ctgGetTableMetaFromVnode(struct SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, SVgroupInfo *vgroupInfo, STableMetaOutput* output) {
  if (NULL == pCatalog || NULL == pTransporter || NULL == pMgmtEps || NULL == pTableName || NULL == vgroupInfo || NULL == output) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  char dbFullName[TSDB_DB_FNAME_LEN];
  tNameGetFullDbName(pTableName, dbFullName);

  ctgDebug("try to get table meta from vnode, db:%s, tbName:%s", dbFullName, tNameGetTableName(pTableName));

  SBuildTableMetaInput bInput = {.vgId = vgroupInfo->vgId, .dbName = dbFullName, .tableFullName = (char *)tNameGetTableName(pTableName)};
  char *msg = NULL;
  int32_t msgLen = 0;

  int32_t code = queryBuildMsg[TMSG_INDEX(TDMT_VND_TABLE_META)](&bInput, &msg, 0, &msgLen);
  if (code) {
    ctgError("Build vnode tablemeta msg failed, code:%x, tbName:%s", code, tNameGetTableName(pTableName));
    CTG_ERR_RET(code);
  }

  SRpcMsg rpcMsg = {
      .msgType = TDMT_VND_TABLE_META,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pTransporter, &vgroupInfo->epset, &rpcMsg, &rpcRsp);
  
  if (TSDB_CODE_SUCCESS != rpcRsp.code) {
    if (CTG_TABLE_NOT_EXIST(rpcRsp.code)) {
      SET_META_TYPE_NONE(output->metaType);
      ctgDebug("tablemeta not exist in vnode, tbName:%s", tNameGetTableName(pTableName));
      return TSDB_CODE_SUCCESS;
    }
  
    ctgError("error rsp for table meta from vnode, code:%s, tbName:%s", tstrerror(rpcRsp.code), tNameGetTableName(pTableName));
    CTG_ERR_RET(rpcRsp.code);
  }

  code = queryProcessMsgRsp[TMSG_INDEX(TDMT_VND_TABLE_META)](output, rpcRsp.pCont, rpcRsp.contLen);
  if (code) {
    ctgError("Process vnode tablemeta rsp failed, code:%s, tbName:%s", tstrerror(code), tNameGetTableName(pTableName));
    CTG_ERR_RET(code);
  }

  ctgDebug("Got table meta from vnode, db:%s, tbName:%s", dbFullName, tNameGetTableName(pTableName));
  return TSDB_CODE_SUCCESS;
}


int32_t ctgGetHashFunction(int8_t hashMethod, tableNameHashFp *fp) {
  switch (hashMethod) {
    default:
      *fp = MurmurHash3_32;
      break;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetVgInfoFromDB(struct SCatalog *pCatalog, void *pRpc, const SEpSet *pMgmtEps, SDBVgroupInfo *dbInfo, SArray** vgroupList) {
  SHashObj *vgroupHash = NULL;
  SVgroupInfo *vgInfo = NULL;
  SArray *vgList = NULL;
  int32_t code = 0;
  int32_t vgNum = taosHashGetSize(dbInfo->vgInfo);

  vgList = taosArrayInit(vgNum, sizeof(SVgroupInfo));
  if (NULL == vgList) {
    ctgError("taosArrayInit failed, num:%d", vgNum);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);    
  }

  void *pIter = taosHashIterate(dbInfo->vgInfo, NULL);
  while (pIter) {
    vgInfo = pIter;

    if (NULL == taosArrayPush(vgList, vgInfo)) {
      ctgError("taosArrayPush failed, vgId:%d", vgInfo->vgId);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }
    
    pIter = taosHashIterate(dbInfo->vgInfo, pIter);
    vgInfo = NULL;
  }

  *vgroupList = vgList;
  vgList = NULL;

  ctgDebug("Got vg list from DB, vgNum:%d", vgNum);

  return TSDB_CODE_SUCCESS;

_return:

  if (vgList) {
    taosArrayDestroy(vgList);
  }

  CTG_RET(code);
}

int32_t ctgGetVgInfoFromHashValue(struct SCatalog *pCatalog, SDBVgroupInfo *dbInfo, const SName *pTableName, SVgroupInfo *pVgroup) {
  int32_t code = 0;
  
  int32_t vgNum = taosHashGetSize(dbInfo->vgInfo);
  char db[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, db);

  if (vgNum <= 0) {
    ctgError("db vgroup cache invalid, db:%s, vgroup number:%d", db, vgNum);
    CTG_ERR_RET(TSDB_CODE_TSC_DB_NOT_SELECTED);
  }

  tableNameHashFp fp = NULL;
  SVgroupInfo *vgInfo = NULL;

  CTG_ERR_JRET(ctgGetHashFunction(dbInfo->hashMethod, &fp));

  char tbFullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pTableName, tbFullName);

  uint32_t hashValue = (*fp)(tbFullName, (uint32_t)strlen(tbFullName));

  void *pIter = taosHashIterate(dbInfo->vgInfo, NULL);
  while (pIter) {
    vgInfo = pIter;
    if (hashValue >= vgInfo->hashBegin && hashValue <= vgInfo->hashEnd) {
      taosHashCancelIterate(dbInfo->vgInfo, pIter);
      break;
    }
    
    pIter = taosHashIterate(dbInfo->vgInfo, pIter);
    vgInfo = NULL;
  }

  if (NULL == vgInfo) {
    ctgError("no hash range found for hash value [%u], db:%s, numOfVgId:%d", hashValue, db, taosHashGetSize(dbInfo->vgInfo));
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  *pVgroup = *vgInfo;

_return:
  CTG_RET(code);
}

int32_t ctgSTableVersionCompare(const void* key1, const void* key2) {
  if (((SSTableMetaVersion*)key1)->suid < ((SSTableMetaVersion*)key2)->suid) {
    return -1;
  } else if (((SSTableMetaVersion*)key1)->suid > ((SSTableMetaVersion*)key2)->suid) {
    return 1;
  } else {
    return 0;
  }
}

int32_t ctgDbVgVersionCompare(const void* key1, const void* key2) {
  if (((SDbVgVersion*)key1)->dbId < ((SDbVgVersion*)key2)->dbId) {
    return -1;
  } else if (((SDbVgVersion*)key1)->dbId > ((SDbVgVersion*)key2)->dbId) {
    return 1;
  } else {
    return 0;
  }
}


int32_t ctgMetaRentInit(SMetaRentMgmt *mgmt, uint32_t rentSec, int8_t type) {
  mgmt->slotRIdx = 0;
  mgmt->slotNum = rentSec / CTG_RENT_SLOT_SECOND;
  mgmt->type = type;

  size_t msgSize = sizeof(SRentSlotInfo) * mgmt->slotNum;
  
  mgmt->slots = calloc(1, msgSize);
  if (NULL == mgmt->slots) {
    qError("calloc %d failed", (int32_t)msgSize);
    return TSDB_CODE_CTG_MEM_ERROR;
  }

  qDebug("meta rent initialized, type:%d, slotNum:%d", type, mgmt->slotNum);
  
  return TSDB_CODE_SUCCESS;
}


int32_t ctgMetaRentAdd(SMetaRentMgmt *mgmt, void *meta, int64_t id, int32_t size) {
  int16_t widx = abs(id % mgmt->slotNum);

  SRentSlotInfo *slot = &mgmt->slots[widx];
  int32_t code = 0;
  
  CTG_LOCK(CTG_WRITE, &slot->lock);
  if (NULL == slot->meta) {
    slot->meta = taosArrayInit(CTG_DEFAULT_RENT_SLOT_SIZE, size);
    if (NULL == slot->meta) {
      qError("taosArrayInit %d failed, id:%"PRIx64", slot idx:%d, type:%d", CTG_DEFAULT_RENT_SLOT_SIZE, id, widx, mgmt->type);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }
  }

  if (NULL == taosArrayPush(slot->meta, meta)) {
    qError("taosArrayPush meta to rent failed, id:%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
  }

  slot->needSort = true;

  qDebug("add meta to rent, id:%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);

_return:

  CTG_UNLOCK(CTG_WRITE, &slot->lock);
  CTG_RET(code);
}

int32_t ctgMetaRentUpdate(SMetaRentMgmt *mgmt, void *meta, int64_t id, int32_t size, __compar_fn_t compare) {
  int16_t widx = abs(id % mgmt->slotNum);

  SRentSlotInfo *slot = &mgmt->slots[widx];
  int32_t code = 0;
  
  CTG_LOCK(CTG_WRITE, &slot->lock);
  if (NULL == slot->meta) {
    qError("meta in slot is empty, id:%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
  }

  if (slot->needSort) {
    taosArraySort(slot->meta, compare);
    slot->needSort = false;
    qDebug("slot meta sorted, slot idx:%d, type:%d", widx, mgmt->type);
  }

  void *orig = taosArraySearch(slot->meta, &id, compare, TD_EQ);
  if (NULL == orig) {
    qError("meta not found in slot, id:%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  memcpy(orig, meta, size);

  qDebug("meta in rent updated, id:%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);

_return:

  CTG_UNLOCK(CTG_WRITE, &slot->lock);

  if (code) {
    qWarn("meta in rent update failed, will try to add it, code:%x, id:%"PRIx64", slot idx:%d, type:%d", code, id, widx, mgmt->type);
    CTG_RET(ctgMetaRentAdd(mgmt, meta, id, size));
  }

  CTG_RET(code);
}

int32_t ctgMetaRentGetImpl(SMetaRentMgmt *mgmt, void **res, uint32_t *num, int32_t size) {
  int16_t ridx = atomic_add_fetch_16(&mgmt->slotRIdx, 1);
  if (ridx >= mgmt->slotNum) {
    ridx %= mgmt->slotNum;
    atomic_store_16(&mgmt->slotRIdx, ridx);
  }

  SRentSlotInfo *slot = &mgmt->slots[ridx];
  int32_t code = 0;
  
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
  *res = malloc(msize);
  if (NULL == *res) {
    qError("malloc %d failed", (int32_t)msize);
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
  }

  void *meta = taosArrayGet(slot->meta, 0);

  memcpy(*res, meta, msize);

  *num = (uint32_t)metaNum;

  qDebug("Got %d meta from rent, type:%d", (int32_t)metaNum, mgmt->type);

_return:

  CTG_UNLOCK(CTG_READ, &slot->lock);

  CTG_RET(code);
}

int32_t ctgMetaRentGet(SMetaRentMgmt *mgmt, void **res, uint32_t *num, int32_t size) {
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



int32_t ctgUpdateTableMetaCache(struct SCatalog *pCatalog, STableMetaOutput *output) {
  int32_t code = 0;

  if (NULL == output->tbMeta) {
    ctgError("no valid table meta got from meta rsp, tbName:%s", output->tbFname);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == pCatalog->tableCache.cache) {
    SHashObj *cache = taosHashInit(ctgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (NULL == cache) {
      ctgError("taosHashInit failed, num:%d", ctgMgmt.cfg.maxTblCacheNum);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }

    if (NULL != atomic_val_compare_exchange_ptr(&pCatalog->tableCache.cache, NULL, cache)) {
      taosHashCleanup(cache);
    }
  }

  if (NULL == pCatalog->tableCache.stableCache) {
    SHashObj *cache = taosHashInit(ctgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), true, HASH_ENTRY_LOCK);
    if (NULL == cache) {
      ctgError("taosHashInit failed, num:%d", ctgMgmt.cfg.maxTblCacheNum);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }

    if (NULL != atomic_val_compare_exchange_ptr(&pCatalog->tableCache.stableCache, NULL, cache)) {
      taosHashCleanup(cache);
    }
  }

  if (CTG_IS_META_CTABLE(output->metaType) || CTG_IS_META_BOTH(output->metaType)) {
    if (taosHashPut(pCatalog->tableCache.cache, output->ctbFname, strlen(output->ctbFname), &output->ctbMeta, sizeof(output->ctbMeta)) != 0) {
      ctgError("taosHashPut ctablemeta to cache failed, ctbName:%s", output->ctbFname);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }

    ctgDebug("update child tablemeta to cache, tbName:%s", output->ctbFname);
  }

  if (CTG_IS_META_CTABLE(output->metaType)) {
    return TSDB_CODE_SUCCESS;
  }
  
  if (CTG_IS_META_BOTH(output->metaType) && TSDB_SUPER_TABLE != output->tbMeta->tableType) {
    ctgError("table type error, expected:%d, actual:%d", TSDB_SUPER_TABLE, output->tbMeta->tableType);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }    

  int32_t tbSize = sizeof(*output->tbMeta) + sizeof(SSchema) * (output->tbMeta->tableInfo.numOfColumns + output->tbMeta->tableInfo.numOfTags);

  if (TSDB_SUPER_TABLE == output->tbMeta->tableType) {
    bool newAdded = false;
    SSTableMetaVersion metaRent = {.suid = output->tbMeta->suid, .sversion = output->tbMeta->sversion, .tversion = output->tbMeta->tversion};
    
    CTG_LOCK(CTG_WRITE, &pCatalog->tableCache.stableLock);
    if (taosHashPut(pCatalog->tableCache.cache, output->tbFname, strlen(output->tbFname), output->tbMeta, tbSize) != 0) {
      CTG_UNLOCK(CTG_WRITE, &pCatalog->tableCache.stableLock);
      ctgError("taosHashPut tablemeta to cache failed, tbName:%s", output->tbFname);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }

    STableMeta *tbMeta = taosHashGet(pCatalog->tableCache.cache, output->tbFname, strlen(output->tbFname));
    if (taosHashPutExt(pCatalog->tableCache.stableCache, &output->tbMeta->suid, sizeof(output->tbMeta->suid), &tbMeta, POINTER_BYTES, &newAdded) != 0) {
      CTG_UNLOCK(CTG_WRITE, &pCatalog->tableCache.stableLock);
      ctgError("taosHashPutExt stable to stable cache failed, suid:%"PRIx64, output->tbMeta->suid);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }
    CTG_UNLOCK(CTG_WRITE, &pCatalog->tableCache.stableLock);

    ctgDebug("update stable to cache, suid:%"PRIx64, output->tbMeta->suid);

    if (newAdded) {
      CTG_ERR_RET(ctgMetaRentAdd(&pCatalog->stableRent, &metaRent, metaRent.suid, sizeof(SSTableMetaVersion)));
    } else {
      CTG_ERR_RET(ctgMetaRentUpdate(&pCatalog->stableRent, &metaRent, metaRent.suid, sizeof(SSTableMetaVersion), ctgSTableVersionCompare));
    }
  } else {
    if (taosHashPut(pCatalog->tableCache.cache, output->tbFname, strlen(output->tbFname), output->tbMeta, tbSize) != 0) {
      ctgError("taosHashPut tablemeta to cache failed, tbName:%s", output->tbFname);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }
  }

  ctgDebug("update tablemeta to cache, tbName:%s", output->tbFname);

  CTG_RET(code);
}

int32_t ctgGetDBVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* dbName, bool forceUpdate, SDBVgroupInfo** dbInfo) {
  bool inCache = false;
  if (!forceUpdate) {
    CTG_ERR_RET(ctgGetDBVgroupFromCache(pCatalog, dbName, dbInfo, &inCache));
    if (inCache) {
      return TSDB_CODE_SUCCESS;
    }

    ctgDebug("failed to get DB vgroupInfo from cache, dbName:%s, load it from mnode, update:%d", dbName, forceUpdate);
  }

  SUseDbOutput DbOut = {0};
  SBuildUseDBInput input = {0};

  tstrncpy(input.db, dbName, tListLen(input.db));
  input.vgVersion = CTG_DEFAULT_INVALID_VERSION;

  while (true) {
    CTG_ERR_RET(ctgGetDBVgroupFromMnode(pCatalog, pRpc, pMgmtEps, &input, &DbOut));
    CTG_ERR_RET(catalogUpdateDBVgroup(pCatalog, dbName, &DbOut.dbVgroup));
    CTG_ERR_RET(ctgGetDBVgroupFromCache(pCatalog, dbName, dbInfo, &inCache));

    if (!inCache) {
      ctgWarn("can't get db vgroup from cache, will retry, db:%s", dbName);
      continue;
    }

    break;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t ctgValidateAndRemoveDb(struct SCatalog* pCatalog, const char* dbName, SDBVgroupInfo* dbInfo) {
  SDBVgroupInfo *oldInfo = (SDBVgroupInfo *)taosHashAcquire(pCatalog->dbCache.cache, dbName, strlen(dbName));
  if (oldInfo) {
    CTG_LOCK(CTG_WRITE, &oldInfo->lock);
    if (dbInfo->vgVersion <= oldInfo->vgVersion) {
      ctgInfo("db vgVersion is not new, db:%s, vgVersion:%d, current:%d", dbName, dbInfo->vgVersion, oldInfo->vgVersion);
      CTG_UNLOCK(CTG_WRITE, &oldInfo->lock);
      taosHashRelease(pCatalog->dbCache.cache, oldInfo);
      
      return TSDB_CODE_SUCCESS;
    }
    
    if (oldInfo->vgInfo) {
      ctgInfo("cleanup db vgInfo, db:%s", dbName);
      taosHashCleanup(oldInfo->vgInfo);
      oldInfo->vgInfo = NULL;
    }
    
    CTG_UNLOCK(CTG_WRITE, &oldInfo->lock);
  
    taosHashRelease(pCatalog->dbCache.cache, oldInfo);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgRenewTableMetaImpl(struct SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, int32_t isSTable) {
  if (NULL == pCatalog || NULL == pTransporter || NULL == pMgmtEps || NULL == pTableName) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SVgroupInfo vgroupInfo = {0};
  int32_t code = 0;

  CTG_ERR_RET(catalogGetTableHashVgroup(pCatalog, pTransporter, pMgmtEps, pTableName, &vgroupInfo));

  STableMetaOutput voutput = {0};
  STableMetaOutput moutput = {0};
  STableMetaOutput *output = &voutput;

  if (CTG_IS_STABLE(isSTable)) {
    ctgDebug("will renew table meta, supposed to be stable, tbName:%s", tNameGetTableName(pTableName));

    // if get from mnode failed, will not try vnode
    CTG_ERR_JRET(ctgGetTableMetaFromMnode(pCatalog, pTransporter, pMgmtEps, pTableName, &moutput));

    if (CTG_IS_META_NONE(moutput.metaType)) {
      CTG_ERR_JRET(ctgGetTableMetaFromVnode(pCatalog, pTransporter, pMgmtEps, pTableName, &vgroupInfo, &voutput));
    } else {
      output = &moutput;
    }
  } else {
    ctgDebug("will renew table meta, not supposed to be stable, tbName:%s, isStable:%d", tNameGetTableName(pTableName), isSTable);

    // if get from vnode failed or no table meta, will not try mnode
    CTG_ERR_JRET(ctgGetTableMetaFromVnode(pCatalog, pTransporter, pMgmtEps, pTableName, &vgroupInfo, &voutput));

    if (CTG_IS_META_TABLE(voutput.metaType) && TSDB_SUPER_TABLE == voutput.tbMeta->tableType) {
      ctgDebug("will continue to renew table meta since got stable, tbName:%s, metaType:%d", tNameGetTableName(pTableName), voutput.metaType);
      
      CTG_ERR_JRET(ctgGetTableMetaFromMnodeImpl(pCatalog, pTransporter, pMgmtEps, voutput.tbFname, &moutput));

      tfree(voutput.tbMeta);
      voutput.tbMeta = moutput.tbMeta;
      moutput.tbMeta = NULL;
    } else if (CTG_IS_META_BOTH(voutput.metaType)) {
      int32_t exist = 0;
      CTG_ERR_JRET(ctgIsTableMetaExistInCache(pCatalog, voutput.tbFname, &exist));
      if (0 == exist) {
        CTG_ERR_JRET(ctgGetTableMetaFromMnodeImpl(pCatalog, pTransporter, pMgmtEps, voutput.tbFname, &moutput));

        if (CTG_IS_META_NONE(moutput.metaType)) {
          SET_META_TYPE_NONE(voutput.metaType);
        }
        
        tfree(voutput.tbMeta);
        voutput.tbMeta = moutput.tbMeta;
        moutput.tbMeta = NULL;
      } else {
        SET_META_TYPE_CTABLE(voutput.metaType); 
      }
    }
  }

  if (CTG_IS_META_NONE(output->metaType)) {
    ctgError("no tablemeta got, tbNmae:%s", tNameGetTableName(pTableName));
    CTG_ERR_JRET(CTG_ERR_CODE_TABLE_NOT_EXIST);
  }

  CTG_ERR_JRET(ctgUpdateTableMetaCache(pCatalog, output));

_return:

  tfree(voutput.tbMeta);
  tfree(moutput.tbMeta);
  
  CTG_RET(code);
}

int32_t ctgGetTableMeta(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const SName* pTableName, bool forceUpdate, STableMeta** pTableMeta, int32_t isSTable) {
  if (NULL == pCatalog || NULL == pRpc || NULL == pMgmtEps || NULL == pTableName || NULL == pTableMeta) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }
  
  int32_t exist = 0;

  if (!forceUpdate) {
    CTG_ERR_RET(ctgGetTableMetaFromCache(pCatalog, pTableName, pTableMeta, &exist));

    if (exist && CTG_TBTYPE_MATCH(isSTable, (*pTableMeta)->tableType)) {
      return TSDB_CODE_SUCCESS;
    }
  } else if (CTG_IS_UNKNOWN_STABLE(isSTable)) {
    int32_t tbType = 0;
    
    CTG_ERR_RET(ctgGetTableTypeFromCache(pCatalog, pTableName, &tbType));

    CTG_SET_STABLE(isSTable, tbType);
  }

  CTG_ERR_RET(ctgRenewTableMetaImpl(pCatalog, pRpc, pMgmtEps, pTableName, isSTable));

  CTG_ERR_RET(ctgGetTableMetaFromCache(pCatalog, pTableName, pTableMeta, &exist));

  if (0 == exist) {
    ctgError("renew tablemeta succeed but get from cache failed, may be deleted, tbName:%s", tNameGetTableName(pTableName));
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }
  
  return TSDB_CODE_SUCCESS;
}

void ctgFreeMetaRent(SMetaRentMgmt *mgmt) {
  if (NULL == mgmt->slots) {
    return;
  }

  for (int32_t i = 0; i < mgmt->slotNum; ++i) {
    SRentSlotInfo *slot = &mgmt->slots[i];
    if (slot->meta) {
      taosArrayDestroy(slot->meta);
      slot->meta = NULL;
    }
  }

  tfree(mgmt->slots);
}

void ctgFreeDbCache(SDBVgroupCache *db) {
  if (NULL == db->cache) {
    return;
  }

  SDBVgroupInfo *dbInfo = NULL;
  void *pIter = taosHashIterate(db->cache, NULL);
  while (pIter) {
    dbInfo = pIter;

    if (dbInfo->vgInfo) {
      taosHashCleanup(dbInfo->vgInfo);
      dbInfo->vgInfo = NULL;
    }
    
    pIter = taosHashIterate(db->cache, pIter);
  }

  taosHashCleanup(db->cache);
  db->cache = NULL;
}

void ctgFreeTableMetaCache(STableMetaCache *table) {
  if (table->stableCache) {
    taosHashCleanup(table->stableCache);
    table->stableCache = NULL;
  }

  if (table->cache) {
    taosHashCleanup(table->cache);
    table->cache = NULL;
  }
}

void ctgFreeHandle(struct SCatalog* pCatalog) {
  ctgFreeMetaRent(&pCatalog->dbRent);
  ctgFreeMetaRent(&pCatalog->stableRent);
  ctgFreeDbCache(&pCatalog->dbCache);
  ctgFreeTableMetaCache(&pCatalog->tableCache);
  
  free(pCatalog);
}

int32_t catalogInit(SCatalogCfg *cfg) {
  if (ctgMgmt.pCluster) {
    qError("catalog already init");
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (cfg) {
    memcpy(&ctgMgmt.cfg, cfg, sizeof(*cfg));

    if (ctgMgmt.cfg.maxDBCacheNum == 0) {
      ctgMgmt.cfg.maxDBCacheNum = CTG_DEFAULT_CACHE_DB_NUMBER;
    }

    if (ctgMgmt.cfg.maxTblCacheNum == 0) {
      ctgMgmt.cfg.maxTblCacheNum = CTG_DEFAULT_CACHE_TABLEMETA_NUMBER;
    }

    if (ctgMgmt.cfg.dbRentSec == 0) {
      ctgMgmt.cfg.dbRentSec = CTG_DEFAULT_RENT_SECOND;
    }

    if (ctgMgmt.cfg.stableRentSec == 0) {
      ctgMgmt.cfg.stableRentSec = CTG_DEFAULT_RENT_SECOND;
    }
  } else {
    ctgMgmt.cfg.maxDBCacheNum = CTG_DEFAULT_CACHE_DB_NUMBER;
    ctgMgmt.cfg.maxTblCacheNum = CTG_DEFAULT_CACHE_TABLEMETA_NUMBER;
    ctgMgmt.cfg.dbRentSec = CTG_DEFAULT_RENT_SECOND;
    ctgMgmt.cfg.stableRentSec = CTG_DEFAULT_RENT_SECOND;
  }

  ctgMgmt.pCluster = taosHashInit(CTG_DEFAULT_CACHE_CLUSTER_NUMBER, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == ctgMgmt.pCluster) {
    qError("taosHashInit %d cluster cache failed", CTG_DEFAULT_CACHE_CLUSTER_NUMBER);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  qDebug("catalog initialized, maxDb:%u, maxTbl:%u, dbRentSec:%u, stableRentSec:%u", ctgMgmt.cfg.maxDBCacheNum, ctgMgmt.cfg.maxTblCacheNum, ctgMgmt.cfg.dbRentSec, ctgMgmt.cfg.stableRentSec);

  return TSDB_CODE_SUCCESS;
}

int32_t catalogGetHandle(uint64_t clusterId, struct SCatalog** catalogHandle) {
  if (NULL == catalogHandle) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (NULL == ctgMgmt.pCluster) {
    qError("cluster cache are not ready, clusterId:%"PRIx64, clusterId);
    CTG_ERR_RET(TSDB_CODE_CTG_NOT_READY);
  }

  int32_t code = 0;
  SCatalog *clusterCtg = NULL;

  while (true) {
    SCatalog **ctg = (SCatalog **)taosHashGet(ctgMgmt.pCluster, (char*)&clusterId, sizeof(clusterId));

    if (ctg && (*ctg)) {
      *catalogHandle = *ctg;
      qDebug("got catalog handle from cache, clusterId:%"PRIx64", CTG:%p", clusterId, *ctg);
      return TSDB_CODE_SUCCESS;
    }

    clusterCtg = calloc(1, sizeof(SCatalog));
    if (NULL == clusterCtg) {
      qError("calloc %d failed", (int32_t)sizeof(SCatalog));
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }

    clusterCtg->clusterId = clusterId;

    CTG_ERR_JRET(ctgMetaRentInit(&clusterCtg->dbRent, ctgMgmt.cfg.dbRentSec, CTG_RENT_DB));
    CTG_ERR_JRET(ctgMetaRentInit(&clusterCtg->stableRent, ctgMgmt.cfg.stableRentSec, CTG_RENT_STABLE));

    code = taosHashPut(ctgMgmt.pCluster, &clusterId, sizeof(clusterId), &clusterCtg, POINTER_BYTES);
    if (code) {
      if (HASH_NODE_EXIST(code)) {
        ctgFreeHandle(clusterCtg);
        continue;
      }
      
      qError("taosHashPut CTG to cache failed, clusterId:%"PRIx64, clusterId);
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    qDebug("add CTG to cache, clusterId:%"PRIx64", CTG:%p", clusterId, clusterCtg);

    break;
  }

  *catalogHandle = clusterCtg;
  
  return TSDB_CODE_SUCCESS;

_return:

  ctgFreeHandle(clusterCtg);
  
  CTG_RET(code);
}

void catalogFreeHandle(struct SCatalog* pCatalog) {
  if (NULL == pCatalog) {
    return;
  }

  if (taosHashRemove(ctgMgmt.pCluster, &pCatalog->clusterId, sizeof(pCatalog->clusterId))) {
    ctgWarn("taosHashRemove from cluster failed, may already be freed, clusterId:%"PRIx64, pCatalog->clusterId);
    return;
  }

  uint64_t clusterId = pCatalog->clusterId;
  
  ctgFreeHandle(pCatalog);

  ctgInfo("handle freed, culsterId:%"PRIx64, clusterId);
}

int32_t catalogGetDBVgroupVersion(struct SCatalog* pCatalog, const char* dbName, int32_t* version) {
  if (NULL == pCatalog || NULL == dbName || NULL == version) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (NULL == pCatalog->dbCache.cache) {
    *version = CTG_DEFAULT_INVALID_VERSION;
    ctgInfo("empty db cache, dbName:%s", dbName);
    return TSDB_CODE_SUCCESS;
  }

  SDBVgroupInfo * dbInfo = taosHashAcquire(pCatalog->dbCache.cache, dbName, strlen(dbName));
  if (NULL == dbInfo) {
    *version = CTG_DEFAULT_INVALID_VERSION;
    ctgInfo("db not in cache, dbName:%s", dbName);
    return TSDB_CODE_SUCCESS;
  }

  *version = dbInfo->vgVersion;
  taosHashRelease(pCatalog->dbCache.cache, dbInfo);

  ctgDebug("Got db vgVersion from cache, dbName:%s, vgVersion:%d", dbName, *version);

  return TSDB_CODE_SUCCESS;
}

int32_t catalogGetDBVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* dbName, bool forceUpdate, SArray** vgroupList) {
  if (NULL == pCatalog || NULL == dbName || NULL == pRpc || NULL == pMgmtEps || NULL == vgroupList) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SDBVgroupInfo* db   = NULL;
  SVgroupInfo *vgInfo = NULL;

  int32_t code = 0;
  SArray *vgList = NULL;
  CTG_ERR_JRET(ctgGetDBVgroup(pCatalog, pRpc, pMgmtEps, dbName, forceUpdate, &db));

  vgList = taosArrayInit(taosHashGetSize(db->vgInfo), sizeof(SVgroupInfo));
  if (NULL == vgList) {
    ctgError("taosArrayInit %d failed", taosHashGetSize(db->vgInfo));
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);    
  }

  void *pIter = taosHashIterate(db->vgInfo, NULL);
  while (pIter) {
    vgInfo = pIter;

    if (NULL == taosArrayPush(vgList, vgInfo)) {
      ctgError("taosArrayPush failed, vgId:%d", vgInfo->vgId);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }
    
    pIter = taosHashIterate(db->vgInfo, pIter);
    vgInfo = NULL;
  }

  *vgroupList = vgList;
  vgList = NULL;

_return:
  if (db) {
    CTG_UNLOCK(CTG_READ, &db->lock);
    taosHashRelease(pCatalog->dbCache.cache, db);
  }

  if (vgList) {
    taosArrayDestroy(vgList);
    vgList = NULL;
  }

  CTG_RET(code);  
}


int32_t catalogUpdateDBVgroup(struct SCatalog* pCatalog, const char* dbName, SDBVgroupInfo* dbInfo) {
  int32_t code = 0;
  
  if (NULL == pCatalog || NULL == dbName || NULL == dbInfo) {
    CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (NULL == dbInfo->vgInfo || dbInfo->vgVersion < 0 || taosHashGetSize(dbInfo->vgInfo) <= 0) {
    ctgError("invalid db vgInfo, dbName:%s, vgInfo:%p, vgVersion:%d", dbName, dbInfo->vgInfo, dbInfo->vgVersion);
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
  }

  if (dbInfo->vgVersion < 0) {
    ctgWarn("db vgVersion less than 0, dbName:%s, vgVersion:%d", dbName, dbInfo->vgVersion);

    if (pCatalog->dbCache.cache) {
      CTG_ERR_JRET(ctgValidateAndRemoveDb(pCatalog, dbName, dbInfo));
      
      CTG_ERR_JRET(taosHashRemove(pCatalog->dbCache.cache, dbName, strlen(dbName)));
    }
    
    ctgWarn("db removed from cache, db:%s", dbName);
    goto _return;
  }

  if (NULL == pCatalog->dbCache.cache) {
    SHashObj *cache = taosHashInit(ctgMgmt.cfg.maxDBCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (NULL == cache) {
      ctgError("taosHashInit %d failed", CTG_DEFAULT_CACHE_DB_NUMBER);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }

    if (NULL != atomic_val_compare_exchange_ptr(&pCatalog->dbCache.cache, NULL, cache)) {
      taosHashCleanup(cache);
    }
  } else {
    CTG_ERR_JRET(ctgValidateAndRemoveDb(pCatalog, dbName, dbInfo));
  }

  bool newAdded = false;
  if (taosHashPutExt(pCatalog->dbCache.cache, dbName, strlen(dbName), dbInfo, sizeof(*dbInfo), &newAdded) != 0) {
    ctgError("taosHashPutExt db vgroup to cache failed, db:%s", dbName);
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
  }

  dbInfo->vgInfo = NULL;

  SDbVgVersion vgVersion = {.dbId = dbInfo->dbId, .vgVersion = dbInfo->vgVersion};
  if (newAdded) {
    CTG_ERR_JRET(ctgMetaRentAdd(&pCatalog->dbRent, &vgVersion, dbInfo->dbId, sizeof(SDbVgVersion)));
  } else {
    CTG_ERR_JRET(ctgMetaRentUpdate(&pCatalog->dbRent, &vgVersion, dbInfo->dbId, sizeof(SDbVgVersion), ctgDbVgVersionCompare));
  }
  
  ctgDebug("dbName:%s vgroup updated, vgVersion:%d", dbName, dbInfo->vgVersion);


_return:

  if (dbInfo && dbInfo->vgInfo) {
    taosHashCleanup(dbInfo->vgInfo);
    dbInfo->vgInfo = NULL;
  }
  
  CTG_RET(code);
}

int32_t catalogGetTableMeta(struct SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, STableMeta** pTableMeta) {
  return ctgGetTableMeta(pCatalog, pTransporter, pMgmtEps, pTableName, false, pTableMeta, -1);
}

int32_t catalogGetSTableMeta(struct SCatalog* pCatalog, void * pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, STableMeta** pTableMeta) {
  return ctgGetTableMeta(pCatalog, pTransporter, pMgmtEps, pTableName, false, pTableMeta, 1);
}

int32_t catalogRenewTableMeta(struct SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, int32_t isSTable) {
  if (NULL == pCatalog || NULL == pTransporter || NULL == pMgmtEps || NULL == pTableName) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  return ctgRenewTableMetaImpl(pCatalog, pTransporter, pMgmtEps, pTableName, isSTable);
}

int32_t catalogRenewAndGetTableMeta(struct SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, STableMeta** pTableMeta, int32_t isSTable) {
  return ctgGetTableMeta(pCatalog, pTransporter, pMgmtEps, pTableName, true, pTableMeta, isSTable);
}

int32_t catalogGetTableDistVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const SName* pTableName, SArray** pVgroupList) {
  if (NULL == pCatalog || NULL == pRpc || NULL == pMgmtEps || NULL == pTableName || NULL == pVgroupList) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }
  
  STableMeta *tbMeta = NULL;
  int32_t code = 0;
  SVgroupInfo vgroupInfo = {0};
  SDBVgroupInfo* dbVgroup = NULL;
  SArray *vgList = NULL;

  *pVgroupList = NULL;
  
  CTG_ERR_JRET(ctgGetTableMeta(pCatalog, pRpc, pMgmtEps, pTableName, false, &tbMeta, -1));

  char db[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, db);
  CTG_ERR_JRET(ctgGetDBVgroup(pCatalog, pRpc, pMgmtEps, db, false, &dbVgroup));

  // REMOEV THIS ....
  if (0 == tbMeta->vgId) {
    SVgroupInfo vgroup = {0};
    
    catalogGetTableHashVgroup(pCatalog, pRpc, pMgmtEps, pTableName, &vgroup);

    tbMeta->vgId = vgroup.vgId;
  }
  // REMOVE THIS ....

  if (tbMeta->tableType == TSDB_SUPER_TABLE) {
    CTG_ERR_JRET(ctgGetVgInfoFromDB(pCatalog, pRpc, pMgmtEps, dbVgroup, pVgroupList));
  } else {
    int32_t vgId = tbMeta->vgId;
    if (NULL == taosHashGetClone(dbVgroup->vgInfo, &vgId, sizeof(vgId), &vgroupInfo)) {
      ctgError("table's vgId not found in vgroup list, vgId:%d, tbName:%s", vgId, tNameGetTableName(pTableName));
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);    
    }

    vgList = taosArrayInit(1, sizeof(SVgroupInfo));
    if (NULL == vgList) {
      ctgError("taosArrayInit %d failed", (int32_t)sizeof(SVgroupInfo));
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);    
    }

    if (NULL == taosArrayPush(vgList, &vgroupInfo)) {
      ctgError("taosArrayPush vgroupInfo to array failed, vgId:%d, tbName:%s", vgId, tNameGetTableName(pTableName));
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    *pVgroupList = vgList;
    vgList = NULL;
  }

_return:
  tfree(tbMeta);

  if (dbVgroup) {
    CTG_UNLOCK(CTG_READ, &dbVgroup->lock);
    taosHashRelease(pCatalog->dbCache.cache, dbVgroup);
  }

  if (vgList) {
    taosArrayDestroy(vgList);
    vgList = NULL;
  }
  
  CTG_RET(code);
}


int32_t catalogGetTableHashVgroup(struct SCatalog *pCatalog, void *pTransporter, const SEpSet *pMgmtEps, const SName *pTableName, SVgroupInfo *pVgroup) {
  SDBVgroupInfo* dbInfo = NULL;
  int32_t code = 0;

  char db[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, db);

  CTG_ERR_RET(ctgGetDBVgroup(pCatalog, pTransporter, pMgmtEps, db, false, &dbInfo));

  CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCatalog, dbInfo, pTableName, pVgroup));

_return:
  if (dbInfo) {
    CTG_UNLOCK(CTG_READ, &dbInfo->lock);  
    taosHashRelease(pCatalog->dbCache.cache, dbInfo);
  }

  CTG_RET(code);
}


int32_t catalogGetAllMeta(struct SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SCatalogReq* pReq, SMetaData* pRsp) {
  if (NULL == pCatalog || NULL == pTransporter || NULL == pMgmtEps || NULL == pReq || NULL == pRsp) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;

  if (pReq->pTableName) {
    int32_t tbNum = (int32_t)taosArrayGetSize(pReq->pTableName);
    if (tbNum <= 0) {
      ctgError("empty table name list, tbNum:%d", tbNum);
      CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
    }

    pRsp->pTableMeta = taosArrayInit(tbNum, POINTER_BYTES);
    if (NULL == pRsp->pTableMeta) {
      ctgError("taosArrayInit %d failed", tbNum);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }
    
    for (int32_t i = 0; i < tbNum; ++i) {
      SName *name = taosArrayGet(pReq->pTableName, i);
      STableMeta *pTableMeta = NULL;
      
      CTG_ERR_JRET(ctgGetTableMeta(pCatalog, pTransporter, pMgmtEps, name, false, &pTableMeta, -1));

      if (NULL == taosArrayPush(pRsp->pTableMeta, &pTableMeta)) {
        ctgError("taosArrayPush failed, idx:%d", i);
        tfree(pTableMeta);
        CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
      }
    }
  }

  return TSDB_CODE_SUCCESS;

_return:  
  if (pRsp->pTableMeta) {
    int32_t aSize = taosArrayGetSize(pRsp->pTableMeta);
    for (int32_t i = 0; i < aSize; ++i) {
      STableMeta *pMeta = taosArrayGetP(pRsp->pTableMeta, i);
      tfree(pMeta);
    }
    
    taosArrayDestroy(pRsp->pTableMeta);
    pRsp->pTableMeta = NULL;
  }
  
  CTG_RET(code);
}

int32_t catalogGetQnodeList(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, SArray* pQnodeList) {
  if (NULL == pCatalog || NULL == pRpc  || NULL == pMgmtEps || NULL == pQnodeList) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  //TODO

  return TSDB_CODE_SUCCESS;
}

int32_t catalogGetExpiredSTables(struct SCatalog* pCatalog, SSTableMetaVersion **stables, uint32_t *num) {
  if (NULL == pCatalog || NULL == stables || NULL == num) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_RET(ctgMetaRentGet(&pCatalog->stableRent, (void **)stables, num, sizeof(SSTableMetaVersion)));
}

int32_t catalogGetExpiredDBs(struct SCatalog* pCatalog, SDbVgVersion **dbs, uint32_t *num) {
  if (NULL == pCatalog || NULL == dbs || NULL == num) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_RET(ctgMetaRentGet(&pCatalog->dbRent, (void **)dbs, num, sizeof(SDbVgVersion)));
}


void catalogDestroy(void) {
  if (NULL == ctgMgmt.pCluster) {
    return;
  }

  SCatalog *pCatalog = NULL;
  void *pIter = taosHashIterate(ctgMgmt.pCluster, NULL);
  while (pIter) {
    pCatalog = *(SCatalog **)pIter;

    if (pCatalog) {
      catalogFreeHandle(pCatalog);
    }
    
    pIter = taosHashIterate(ctgMgmt.pCluster, pIter);
  }
  
  taosHashCleanup(ctgMgmt.pCluster);
  ctgMgmt.pCluster = NULL;

  qInfo("catalog destroyed");
}



