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

SCtgDebug gCTGDebug = {0};


void ctgFreeMetaRent(SCtgRentMgmt *mgmt) {
  if (NULL == mgmt->slots) {
    return;
  }

  for (int32_t i = 0; i < mgmt->slotNum; ++i) {
    SCtgRentSlot *slot = &mgmt->slots[i];
    if (slot->meta) {
      taosArrayDestroy(slot->meta);
      slot->meta = NULL;
    }
  }

  tfree(mgmt->slots);
}


void ctgFreeTableMetaCache(SCtgTbMetaCache *table) {
  CTG_LOCK(CTG_WRITE, &table->stbLock);
  if (table->stbCache) {
    taosHashCleanup(table->stbCache);
    table->stbCache = NULL;
  }
  CTG_UNLOCK(CTG_WRITE, &table->stbLock);

  if (table->cache) {
    taosHashCleanup(table->cache);
    table->cache = NULL;
  }
}

void ctgFreeDbCache(SCtgDBCache *dbCache) {
  if (NULL == dbCache) {
    return;
  }

  atomic_store_8(&dbCache->deleted, 1);

  SDBVgroupInfo *dbInfo = NULL;
  if (dbCache->vgInfo) {
    CTG_LOCK(CTG_WRITE, &dbCache->vgLock);

    if (dbCache->vgInfo->vgHash) {
      taosHashCleanup(dbCache->vgInfo->vgHash);
      dbCache->vgInfo->vgHash = NULL;
    }

    tfree(dbCache->vgInfo);
    CTG_UNLOCK(CTG_WRITE, &dbCache->vgLock);
  }

  ctgFreeTableMetaCache(&dbCache->tbCache);
}


void ctgFreeHandle(struct SCatalog* pCatalog) {
  ctgFreeMetaRent(&pCatalog->dbRent);
  ctgFreeMetaRent(&pCatalog->stbRent);
  if (pCatalog->dbCache) {
    void *pIter = taosHashIterate(pCatalog->dbCache, NULL);
    while (pIter) {
      SCtgDBCache *dbCache = pIter;

      ctgFreeDbCache(dbCache);
      
      pIter = taosHashIterate(pCatalog->dbCache, pIter);
    }  

    taosHashCleanup(pCatalog->dbCache);
  }
  
  free(pCatalog);
}


int32_t ctgGetDBVgroupFromCache(struct SCatalog* pCatalog, const char *dbName, SCtgDBCache **dbCache, bool *inCache) {
  if (NULL == pCatalog->dbCache) {
    *inCache = false;
    ctgWarn("empty db cache, dbName:%s", dbName);
    return TSDB_CODE_SUCCESS;
  }

  SCtgDBCache *cache = NULL;

  while (true) {
    cache = taosHashAcquire(pCatalog->dbCache, dbName, strlen(dbName));

    if (NULL == cache) {
      *inCache = false;
      ctgWarn("not in db vgroup cache, dbName:%s", dbName);
      return TSDB_CODE_SUCCESS;
    }

    CTG_LOCK(CTG_READ, &cache->vgLock);
    if (NULL == cache->vgInfo) {
      CTG_UNLOCK(CTG_READ, &cache->vgLock);
      taosHashRelease(pCatalog->dbCache, cache);
      ctgWarn("db cache vgInfo is NULL, dbName:%s", dbName);
      
      continue;
    }

    break;
  }

  *dbCache = cache;
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

int32_t ctgIsTableMetaExistInCache(struct SCatalog* pCatalog, char *dbFName, char* tbName, int32_t *exist) {
  if (NULL == pCatalog->dbCache) {
    *exist = 0;
    ctgWarn("empty db cache, dbFName:%s, tbName:%s", dbFName, tbName);
    return TSDB_CODE_SUCCESS;
  }

  SCtgDBCache *dbCache = taosHashAcquire(pCatalog->dbCache, dbFName, strlen(dbFName));
  if (NULL == dbCache) {
    *exist = 0;
    ctgWarn("db not exist in cache, dbFName:%s", dbFName);
    return TSDB_CODE_SUCCESS;
  }


  size_t sz = 0;
  STableMeta *tbMeta = taosHashGet(dbCache->tbCache.cache, tbName, strlen(tbName));
  if (NULL == tbMeta) {
    taosHashRelease(pCatalog->dbCache, dbCache);
    
    *exist = 0;
    ctgDebug("tbmeta not in cache, dbFName:%s, tbName:%s", dbFName, tbName);
    return TSDB_CODE_SUCCESS;
  }

  *exist = 1;

  taosHashRelease(pCatalog->dbCache, dbCache);
  
  ctgDebug("tbmeta is in cache, dbFName:%s, tbName:%s", dbFName, tbName);
  
  return TSDB_CODE_SUCCESS;
}


int32_t ctgGetTableMetaFromCache(struct SCatalog* pCatalog, const SName* pTableName, STableMeta** pTableMeta, int32_t *exist) {
  if (NULL == pCatalog->dbCache) {
    *exist = 0;
    ctgWarn("empty tbmeta cache, tbName:%s", pTableName->tname);
    return TSDB_CODE_SUCCESS;
  }

  char db[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, db);

  *pTableMeta = NULL;

  SCtgDBCache *dbCache = taosHashAcquire(pCatalog->dbCache, db, strlen(db));
  if (NULL == dbCache) {
    *exist = 0;
    ctgWarn("no db cache, dbFName:%s, tbName:%s", db, pTableName->tname);
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == dbCache->tbCache.cache) {
    *exist = 0;
    taosHashRelease(pCatalog->dbCache, dbCache);
    ctgWarn("empty tbmeta cache, dbFName:%s, tbName:%s", db, pTableName->tname);
    return TSDB_CODE_SUCCESS;
  }
  
  size_t sz = 0;
  STableMeta *tbMeta = taosHashGetCloneExt(dbCache->tbCache.cache, pTableName->tname, strlen(pTableName->tname), NULL, (void **)pTableMeta, &sz);
  if (NULL == *pTableMeta) {
    *exist = 0;
    taosHashRelease(pCatalog->dbCache, dbCache);
    ctgDebug("tbmeta not in cache, dbFName:%s, tbName:%s", db, pTableName->tname);
    return TSDB_CODE_SUCCESS;
  }

  *exist = 1;
  
  tbMeta = *pTableMeta;

  if (tbMeta->tableType != TSDB_CHILD_TABLE) {
    ctgDebug("Got tbmeta from cache, type:%d, dbFName:%s, tbName:%s", tbMeta->tableType, db, pTableName->tname);
    return TSDB_CODE_SUCCESS;
  }
  
  CTG_LOCK(CTG_READ, &dbCache->tbCache.stbLock);
  
  STableMeta **stbMeta = taosHashGet(dbCache->tbCache.stbCache, &tbMeta->suid, sizeof(tbMeta->suid));
  if (NULL == stbMeta || NULL == *stbMeta) {
    CTG_UNLOCK(CTG_READ, &dbCache->tbCache.stbLock);
    ctgError("stable not in stbCache, suid:%"PRIx64, tbMeta->suid);
    tfree(*pTableMeta);
    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }

  if ((*stbMeta)->suid != tbMeta->suid) {    
    CTG_UNLOCK(CTG_READ, &dbCache->tbCache.stbLock);
    tfree(*pTableMeta);
    ctgError("stable suid in stbCache mis-match, expected suid:%"PRIx64 ",actual suid:%"PRIx64, tbMeta->suid, (*stbMeta)->suid);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  int32_t metaSize = sizeof(STableMeta) + ((*stbMeta)->tableInfo.numOfTags + (*stbMeta)->tableInfo.numOfColumns) * sizeof(SSchema);
  *pTableMeta = realloc(*pTableMeta, metaSize);
  if (NULL == *pTableMeta) {    
    CTG_UNLOCK(CTG_READ, &dbCache->tbCache.stbLock);
    ctgError("realloc size[%d] failed", metaSize);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  memcpy(&(*pTableMeta)->sversion, &(*stbMeta)->sversion, metaSize - sizeof(SCTableMeta));

  CTG_UNLOCK(CTG_READ, &dbCache->tbCache.stbLock);

  ctgDebug("Got tbmeta from cache, dbFName:%s, tbName:%s", db, pTableName->tname);
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTableTypeFromCache(struct SCatalog* pCatalog, const SName* pTableName, int32_t *tbType) {
  if (NULL == pCatalog->dbCache) {
    ctgWarn("empty db cache, tbName:%s", pTableName->tname);  
    return TSDB_CODE_SUCCESS;
  }

  char dbName[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, dbName);

  SCtgDBCache *dbCache = taosHashAcquire(pCatalog->dbCache, dbName, strlen(dbName));
  if (NULL == dbCache) {
    ctgInfo("db not in cache, dbFName:%s", dbName);
    return TSDB_CODE_SUCCESS;
  }

  STableMeta *pTableMeta = (STableMeta *)taosHashAcquire(dbCache->tbCache.cache, pTableName->tname, strlen(pTableName->tname));
  if (NULL == pTableMeta) {
    ctgWarn("tbmeta not in cache, dbFName:%s, tbName:%s", dbName, pTableName->tname);  
    taosHashRelease(pCatalog->dbCache, dbCache);
    
    return TSDB_CODE_SUCCESS;
  }

  *tbType = atomic_load_8(&pTableMeta->tableType);

  taosHashRelease(dbCache->tbCache.cache, dbCache);
  taosHashRelease(pCatalog->dbCache, dbCache);

  ctgDebug("Got tbtype from cache, dbFName:%s, tbName:%s, type:%d", dbName, pTableName->tname, *tbType);  
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTableMetaFromMnodeImpl(struct SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, char *dbFName, char* tbName, STableMetaOutput* output) {
  SBuildTableMetaInput bInput = {.vgId = 0, .dbFName = dbFName, .tbName = tbName};
  char *msg = NULL;
  SEpSet *pVnodeEpSet = NULL;
  int32_t msgLen = 0;

  ctgDebug("try to get table meta from mnode, dbFName:%s, tbName:%s", dbFName, tbName);

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
      SET_META_TYPE_NULL(output->metaType);
      ctgDebug("stablemeta not exist in mnode, dbFName:%s, tbName:%s", dbFName, tbName);
      return TSDB_CODE_SUCCESS;
    }
    
    ctgError("error rsp for stablemeta from mnode, code:%s, dbFName:%s, tbName:%s", tstrerror(rpcRsp.code), dbFName, tbName);
    CTG_ERR_RET(rpcRsp.code);
  }

  code = queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_STB_META)](output, rpcRsp.pCont, rpcRsp.contLen);
  if (code) {
    ctgError("Process mnode stablemeta rsp failed, code:%x, dbFName:%s, tbName:%s", code, dbFName, tbName);
    CTG_ERR_RET(code);
  }

  ctgDebug("Got table meta from mnode, dbFName:%s, tbName:%s", dbFName, tbName);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTableMetaFromMnode(struct SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, STableMetaOutput* output) {
  char dbFName[TSDB_DB_FNAME_LEN];
  tNameGetFullDbName(pTableName, dbFName);

  return ctgGetTableMetaFromMnodeImpl(pCatalog, pTransporter, pMgmtEps, dbFName, (char *)pTableName->tname, output);
}

int32_t ctgGetTableMetaFromVnode(struct SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, SVgroupInfo *vgroupInfo, STableMetaOutput* output) {
  if (NULL == pCatalog || NULL == pTransporter || NULL == pMgmtEps || NULL == pTableName || NULL == vgroupInfo || NULL == output) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  char dbFName[TSDB_DB_FNAME_LEN];
  tNameGetFullDbName(pTableName, dbFName);

  ctgDebug("try to get table meta from vnode, dbFName:%s, tbName:%s", dbFName, tNameGetTableName(pTableName));

  SBuildTableMetaInput bInput = {.vgId = vgroupInfo->vgId, .dbFName = dbFName, .tbName = (char *)tNameGetTableName(pTableName)};
  char *msg = NULL;
  int32_t msgLen = 0;

  int32_t code = queryBuildMsg[TMSG_INDEX(TDMT_VND_TABLE_META)](&bInput, &msg, 0, &msgLen);
  if (code) {
    ctgError("Build vnode tablemeta msg failed, code:%x, dbFName:%s, tbName:%s", code, dbFName, tNameGetTableName(pTableName));
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
      SET_META_TYPE_NULL(output->metaType);
      ctgDebug("tablemeta not exist in vnode, dbFName:%s, tbName:%s", dbFName, tNameGetTableName(pTableName));
      return TSDB_CODE_SUCCESS;
    }
  
    ctgError("error rsp for table meta from vnode, code:%s, dbFName:%s, tbName:%s", tstrerror(rpcRsp.code), dbFName, tNameGetTableName(pTableName));
    CTG_ERR_RET(rpcRsp.code);
  }

  code = queryProcessMsgRsp[TMSG_INDEX(TDMT_VND_TABLE_META)](output, rpcRsp.pCont, rpcRsp.contLen);
  if (code) {
    ctgError("Process vnode tablemeta rsp failed, code:%s, dbFName:%s, tbName:%s", tstrerror(code), dbFName, tNameGetTableName(pTableName));
    CTG_ERR_RET(code);
  }

  ctgDebug("Got table meta from vnode, dbFName:%s, tbName:%s", dbFName, tNameGetTableName(pTableName));
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
  int32_t vgNum = taosHashGetSize(dbInfo->vgHash);

  vgList = taosArrayInit(vgNum, sizeof(SVgroupInfo));
  if (NULL == vgList) {
    ctgError("taosArrayInit failed, num:%d", vgNum);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);    
  }

  void *pIter = taosHashIterate(dbInfo->vgHash, NULL);
  while (pIter) {
    vgInfo = pIter;

    if (NULL == taosArrayPush(vgList, vgInfo)) {
      ctgError("taosArrayPush failed, vgId:%d", vgInfo->vgId);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }
    
    pIter = taosHashIterate(dbInfo->vgHash, pIter);
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
  
  int32_t vgNum = taosHashGetSize(dbInfo->vgHash);
  char db[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, db);

  if (vgNum <= 0) {
    ctgError("db vgroup cache invalid, db:%s, vgroup number:%d", db, vgNum);
    CTG_ERR_RET(TSDB_CODE_TSC_DB_NOT_SELECTED);
  }

  tableNameHashFp fp = NULL;
  SVgroupInfo *vgInfo = NULL;

  CTG_ERR_RET(ctgGetHashFunction(dbInfo->hashMethod, &fp));

  char tbFullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pTableName, tbFullName);

  uint32_t hashValue = (*fp)(tbFullName, (uint32_t)strlen(tbFullName));

  void *pIter = taosHashIterate(dbInfo->vgHash, NULL);
  while (pIter) {
    vgInfo = pIter;
    if (hashValue >= vgInfo->hashBegin && hashValue <= vgInfo->hashEnd) {
      taosHashCancelIterate(dbInfo->vgHash, pIter);
      break;
    }
    
    pIter = taosHashIterate(dbInfo->vgHash, pIter);
    vgInfo = NULL;
  }

  if (NULL == vgInfo) {
    ctgError("no hash range found for hash value [%u], db:%s, numOfVgId:%d", hashValue, db, taosHashGetSize(dbInfo->vgHash));
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  *pVgroup = *vgInfo;

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
  if (*(int64_t *)key1 < ((SDbVgVersion*)key2)->dbId) {
    return -1;
  } else if (*(int64_t *)key1 > ((SDbVgVersion*)key2)->dbId) {
    return 1;
  } else {
    return 0;
  }
}


int32_t ctgMetaRentInit(SCtgRentMgmt *mgmt, uint32_t rentSec, int8_t type) {
  mgmt->slotRIdx = 0;
  mgmt->slotNum = rentSec / CTG_RENT_SLOT_SECOND;
  mgmt->type = type;

  size_t msgSize = sizeof(SCtgRentSlot) * mgmt->slotNum;
  
  mgmt->slots = calloc(1, msgSize);
  if (NULL == mgmt->slots) {
    qError("calloc %d failed", (int32_t)msgSize);
    return TSDB_CODE_CTG_MEM_ERROR;
  }

  qDebug("meta rent initialized, type:%d, slotNum:%d", type, mgmt->slotNum);
  
  return TSDB_CODE_SUCCESS;
}


int32_t ctgMetaRentAdd(SCtgRentMgmt *mgmt, void *meta, int64_t id, int32_t size) {
  int16_t widx = abs(id % mgmt->slotNum);

  SCtgRentSlot *slot = &mgmt->slots[widx];
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

int32_t ctgMetaRentUpdate(SCtgRentMgmt *mgmt, void *meta, int64_t id, int32_t size, __compar_fn_t compare) {
  int16_t widx = abs(id % mgmt->slotNum);

  SCtgRentSlot *slot = &mgmt->slots[widx];
  int32_t code = 0;
  
  CTG_LOCK(CTG_WRITE, &slot->lock);
  if (NULL == slot->meta) {
    qError("empty meta slot, id:%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (slot->needSort) {
    taosArraySort(slot->meta, compare);
    slot->needSort = false;
    qDebug("meta slot sorted, slot idx:%d, type:%d", widx, mgmt->type);
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

int32_t ctgMetaRentRemove(SCtgRentMgmt *mgmt, int64_t id, __compar_fn_t compare) {
  int16_t widx = abs(id % mgmt->slotNum);

  SCtgRentSlot *slot = &mgmt->slots[widx];
  int32_t code = 0;
  
  CTG_LOCK(CTG_WRITE, &slot->lock);
  if (NULL == slot->meta) {
    qError("empty meta slot, id:%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (slot->needSort) {
    taosArraySort(slot->meta, compare);
    slot->needSort = false;
    qDebug("meta slot sorted, slot idx:%d, type:%d", widx, mgmt->type);
  }

  int32_t idx = taosArraySearchIdx(slot->meta, &id, compare, TD_EQ);
  if (idx < 0) {
    qError("meta not found in slot, id:%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  taosArrayRemove(slot->meta, idx);

  qDebug("meta in rent removed, id:%"PRIx64", slot idx:%d, type:%d", id, widx, mgmt->type);

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



int32_t ctgUpdateTableMetaCache(struct SCatalog *pCatalog, STableMetaOutput *output) {
  int32_t code = 0;
  SCtgDBCache *dbCache = NULL;

  if ((!CTG_IS_META_CTABLE(output->metaType)) && NULL == output->tbMeta) {
    ctgError("no valid tbmeta got from meta rsp, dbFName:%s, tbName:%s", output->dbFName, output->tbName);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == pCatalog->dbCache) {
    SHashObj *cache = taosHashInit(ctgMgmt.cfg.maxDBCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (NULL == cache) {
      ctgError("taosHashInit %d failed", CTG_DEFAULT_CACHE_DB_NUMBER);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }

    if (NULL != atomic_val_compare_exchange_ptr(&pCatalog->dbCache, NULL, cache)) {
      taosHashCleanup(cache);
    }
  }

  while (true) {
    dbCache = (SCtgDBCache *)taosHashAcquire(pCatalog->dbCache, output->dbFName, strlen(output->dbFName));
    if (dbCache) {
      break;
    }
    
    SCtgDBCache newDbCache = {0};

    if (taosHashPut(pCatalog->dbCache, output->dbFName, strlen(output->dbFName), &newDbCache, sizeof(newDbCache))) {
      ctgError("taosHashPut db to cache failed, db:%s", output->dbFName);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }
  }

  if (NULL == dbCache->tbCache.cache) {
    SHashObj *cache = taosHashInit(ctgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (NULL == cache) {
      ctgError("taosHashInit failed, num:%d", ctgMgmt.cfg.maxTblCacheNum);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }

    if (NULL != atomic_val_compare_exchange_ptr(&dbCache->tbCache.cache, NULL, cache)) {
      taosHashCleanup(cache);
    }
  }

  if (NULL == dbCache->tbCache.stbCache) {
    SHashObj *cache = taosHashInit(ctgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), true, HASH_ENTRY_LOCK);
    if (NULL == cache) {
      ctgError("taosHashInit failed, num:%d", ctgMgmt.cfg.maxTblCacheNum);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }

    if (NULL != atomic_val_compare_exchange_ptr(&dbCache->tbCache.stbCache, NULL, cache)) {
      taosHashCleanup(cache);
    }
  }

  if (CTG_IS_META_CTABLE(output->metaType) || CTG_IS_META_BOTH(output->metaType)) {
    if (taosHashPut(dbCache->tbCache.cache, output->ctbName, strlen(output->ctbName), &output->ctbMeta, sizeof(output->ctbMeta)) != 0) {
      ctgError("taosHashPut ctbmeta to cache failed, ctbName:%s", output->ctbName);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }

    ctgDebug("ctbmeta updated to cache, ctbName:%s", output->ctbName);
  }

  if (CTG_IS_META_CTABLE(output->metaType)) {
    goto _return;
  }
  
  if (CTG_IS_META_BOTH(output->metaType) && TSDB_SUPER_TABLE != output->tbMeta->tableType) {
    ctgError("table type error, expected:%d, actual:%d", TSDB_SUPER_TABLE, output->tbMeta->tableType);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }    

  int32_t tbSize = sizeof(*output->tbMeta) + sizeof(SSchema) * (output->tbMeta->tableInfo.numOfColumns + output->tbMeta->tableInfo.numOfTags);

  if (TSDB_SUPER_TABLE == output->tbMeta->tableType) {
    bool newAdded = false;
    SSTableMetaVersion metaRent = {.suid = output->tbMeta->suid, .sversion = output->tbMeta->sversion, .tversion = output->tbMeta->tversion};
    
    CTG_LOCK(CTG_WRITE, &dbCache->tbCache.stbLock);
    if (taosHashPut(dbCache->tbCache.cache, output->tbName, strlen(output->tbName), output->tbMeta, tbSize) != 0) {
      CTG_UNLOCK(CTG_WRITE, &dbCache->tbCache.stbLock);
      ctgError("taosHashPut tablemeta to cache failed, dbFName:%s, tbName:%s", output->dbFName, output->tbName);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }

    STableMeta *tbMeta = taosHashGet(dbCache->tbCache.cache, output->tbName, strlen(output->tbName));
    if (taosHashPutExt(dbCache->tbCache.stbCache, &output->tbMeta->suid, sizeof(output->tbMeta->suid), &tbMeta, POINTER_BYTES, &newAdded) != 0) {
      CTG_UNLOCK(CTG_WRITE, &dbCache->tbCache.stbLock);
      ctgError("taosHashPutExt stable to stable cache failed, suid:%"PRIx64, output->tbMeta->suid);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }
    CTG_UNLOCK(CTG_WRITE, &dbCache->tbCache.stbLock);

    ctgDebug("update stable to cache, suid:%"PRIx64, output->tbMeta->suid);

    if (newAdded) {
      CTG_ERR_JRET(ctgMetaRentAdd(&pCatalog->stbRent, &metaRent, metaRent.suid, sizeof(SSTableMetaVersion)));
    } else {
      CTG_ERR_JRET(ctgMetaRentUpdate(&pCatalog->stbRent, &metaRent, metaRent.suid, sizeof(SSTableMetaVersion), ctgSTableVersionCompare));
    }
  } else {
    if (taosHashPut(dbCache->tbCache.cache, output->tbName, strlen(output->tbName), output->tbMeta, tbSize) != 0) {
      ctgError("taosHashPut tablemeta to cache failed, dbFName:%s, tbName:%s", output->dbFName, output->tbName);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }
  }

  ctgDebug("update tablemeta to cache, dbFName:%s, tbName:%s", output->dbFName, output->tbName);

_return:

  if (dbCache) {
    taosHashRelease(pCatalog->dbCache, dbCache);
  }

  CTG_RET(code);
}

int32_t ctgGetDBVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* dbName, bool forceUpdate, SCtgDBCache** dbCache) {
  bool inCache = false;
  if (!forceUpdate) {
    CTG_ERR_RET(ctgGetDBVgroupFromCache(pCatalog, dbName, dbCache, &inCache));
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
    CTG_ERR_RET(catalogUpdateDBVgroup(pCatalog, dbName, DbOut.dbVgroup));
    CTG_ERR_RET(ctgGetDBVgroupFromCache(pCatalog, dbName, dbCache, &inCache));

    if (!inCache) {
      ctgWarn("can't get db vgroup from cache, will retry, db:%s", dbName);
      continue;
    }

    break;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t ctgValidateAndRemoveDb(struct SCatalog* pCatalog, const char* dbName, uint64_t dbId, bool *removed) {
  *removed = false;

  SCtgDBCache *dbCache = (SCtgDBCache *)taosHashAcquire(pCatalog->dbCache, dbName, strlen(dbName));
  if (NULL == dbCache) {
    ctgInfo("db not exist in dbCache, may be removed, db:%s", dbName);
    return TSDB_CODE_SUCCESS;
  }
  
  CTG_LOCK(CTG_WRITE, &dbCache->vgLock);
  
  if (NULL == dbCache->vgInfo) {
    ctgInfo("db vgInfo not in dbCache, may be removed, db:%s, dbId:%"PRIx64, dbName, dbId);
    CTG_UNLOCK(CTG_WRITE, &dbCache->vgLock);
    taosHashRelease(pCatalog->dbCache, dbCache);
    return TSDB_CODE_SUCCESS;
  }
  
  if (dbCache->vgInfo->dbId != dbId) {
    ctgInfo("db id already updated, db:%s, dbId:%"PRIx64 ", targetId:%"PRIx64, dbName, dbCache->vgInfo->dbId, dbId);
    CTG_UNLOCK(CTG_WRITE, &dbCache->vgLock);
    taosHashRelease(pCatalog->dbCache, dbCache);
    return TSDB_CODE_SUCCESS;
  }
    
  if (dbCache->vgInfo->vgHash) {
    ctgInfo("cleanup db vgInfo, db:%s, dbId:%"PRIx64, dbName, dbId);
    taosHashCleanup(dbCache->vgInfo->vgHash);
    tfree(dbCache->vgInfo);
  }

  if (taosHashRemove(pCatalog->dbCache, dbName, strlen(dbName))) {
    ctgError("taosHashRemove from dbCache failed, db:%s", dbName);
    CTG_UNLOCK(CTG_WRITE, &dbCache->vgLock);      
    taosHashRelease(pCatalog->dbCache, dbCache);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  dbCache->deleted = true;
  
  CTG_UNLOCK(CTG_WRITE, &dbCache->vgLock);

  ctgFreeTableMetaCache(&dbCache->tbCache);

  taosHashRelease(pCatalog->dbCache, dbCache);

  *removed = true;
  
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
    ctgDebug("will renew tbmeta, supposed to be stb, tbName:%s", tNameGetTableName(pTableName));

    // if get from mnode failed, will not try vnode
    CTG_ERR_JRET(ctgGetTableMetaFromMnode(pCatalog, pTransporter, pMgmtEps, pTableName, &moutput));

    if (CTG_IS_META_NULL(moutput.metaType)) {
      CTG_ERR_JRET(ctgGetTableMetaFromVnode(pCatalog, pTransporter, pMgmtEps, pTableName, &vgroupInfo, &voutput));
    } else {
      output = &moutput;
    }
  } else {
    ctgDebug("will renew tbmeta, not supposed to be stb, tbName:%s, isStable:%d", tNameGetTableName(pTableName), isSTable);

    // if get from vnode failed or no table meta, will not try mnode
    CTG_ERR_JRET(ctgGetTableMetaFromVnode(pCatalog, pTransporter, pMgmtEps, pTableName, &vgroupInfo, &voutput));

    if (CTG_IS_META_TABLE(voutput.metaType) && TSDB_SUPER_TABLE == voutput.tbMeta->tableType) {
      ctgDebug("will continue to renew tbmeta since got stb, tbName:%s, metaType:%d", tNameGetTableName(pTableName), voutput.metaType);
      
      CTG_ERR_JRET(ctgGetTableMetaFromMnodeImpl(pCatalog, pTransporter, pMgmtEps, voutput.dbFName, voutput.tbName, &moutput));

      voutput.metaType = moutput.metaType;
      
      tfree(voutput.tbMeta);
      voutput.tbMeta = moutput.tbMeta;
      moutput.tbMeta = NULL;
    } else if (CTG_IS_META_BOTH(voutput.metaType)) {
      int32_t exist = 0;
      CTG_ERR_JRET(ctgIsTableMetaExistInCache(pCatalog, voutput.dbFName, voutput.tbName, &exist));
      if (0 == exist) {
        CTG_ERR_JRET(ctgGetTableMetaFromMnodeImpl(pCatalog, pTransporter, pMgmtEps, voutput.dbFName, voutput.tbName, &moutput));

        if (CTG_IS_META_NULL(moutput.metaType)) {
          SET_META_TYPE_NULL(voutput.metaType);
        }
        
        tfree(voutput.tbMeta);
        voutput.tbMeta = moutput.tbMeta;
        moutput.tbMeta = NULL;
      } else {
        tfree(voutput.tbMeta);
        
        SET_META_TYPE_CTABLE(voutput.metaType); 
      }
    }
  }

  if (CTG_IS_META_NULL(output->metaType)) {
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

    if (ctgMgmt.cfg.stbRentSec == 0) {
      ctgMgmt.cfg.stbRentSec = CTG_DEFAULT_RENT_SECOND;
    }
  } else {
    ctgMgmt.cfg.maxDBCacheNum = CTG_DEFAULT_CACHE_DB_NUMBER;
    ctgMgmt.cfg.maxTblCacheNum = CTG_DEFAULT_CACHE_TABLEMETA_NUMBER;
    ctgMgmt.cfg.dbRentSec = CTG_DEFAULT_RENT_SECOND;
    ctgMgmt.cfg.stbRentSec = CTG_DEFAULT_RENT_SECOND;
  }

  ctgMgmt.pCluster = taosHashInit(CTG_DEFAULT_CACHE_CLUSTER_NUMBER, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == ctgMgmt.pCluster) {
    qError("taosHashInit %d cluster cache failed", CTG_DEFAULT_CACHE_CLUSTER_NUMBER);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  qDebug("catalog initialized, maxDb:%u, maxTbl:%u, dbRentSec:%u, stbRentSec:%u", ctgMgmt.cfg.maxDBCacheNum, ctgMgmt.cfg.maxTblCacheNum, ctgMgmt.cfg.dbRentSec, ctgMgmt.cfg.stbRentSec);

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
    CTG_ERR_JRET(ctgMetaRentInit(&clusterCtg->stbRent, ctgMgmt.cfg.stbRentSec, CTG_RENT_STABLE));

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

  if (NULL == pCatalog->dbCache) {
    *version = CTG_DEFAULT_INVALID_VERSION;
    ctgInfo("empty db cache, dbName:%s", dbName);
    return TSDB_CODE_SUCCESS;
  }

  SCtgDBCache *db = taosHashAcquire(pCatalog->dbCache, dbName, strlen(dbName));
  if (NULL == db) {
    *version = CTG_DEFAULT_INVALID_VERSION;
    ctgInfo("db not in cache, dbName:%s", dbName);
    return TSDB_CODE_SUCCESS;
  }

  CTG_LOCK(CTG_READ, &db->vgLock);
  
  if (NULL == db->vgInfo) {
    CTG_UNLOCK(CTG_READ, &db->vgLock);

    *version = CTG_DEFAULT_INVALID_VERSION;
    ctgInfo("db not in cache, dbName:%s", dbName);
    return TSDB_CODE_SUCCESS;
  }

  *version = db->vgInfo->vgVersion;
  CTG_UNLOCK(CTG_READ, &db->vgLock);
  
  taosHashRelease(pCatalog->dbCache, db);

  ctgDebug("Got db vgVersion from cache, dbName:%s, vgVersion:%d", dbName, *version);

  return TSDB_CODE_SUCCESS;
}

int32_t catalogGetDBVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* dbName, bool forceUpdate, SArray** vgroupList) {
  if (NULL == pCatalog || NULL == dbName || NULL == pRpc || NULL == pMgmtEps || NULL == vgroupList) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SCtgDBCache* dbCache   = NULL;
  SVgroupInfo *vgInfo = NULL;

  int32_t code = 0;
  SArray *vgList = NULL;
  CTG_ERR_JRET(ctgGetDBVgroup(pCatalog, pRpc, pMgmtEps, dbName, forceUpdate, &dbCache));

  int32_t vgNum = (int32_t)taosHashGetSize(dbCache->vgInfo->vgHash);
  vgList = taosArrayInit(vgNum, sizeof(SVgroupInfo));
  if (NULL == vgList) {
    ctgError("taosArrayInit %d failed", vgNum);
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);    
  }

  void *pIter = taosHashIterate(dbCache->vgInfo->vgHash, NULL);
  while (pIter) {
    vgInfo = pIter;

    if (NULL == taosArrayPush(vgList, vgInfo)) {
      ctgError("taosArrayPush failed, vgId:%d", vgInfo->vgId);
      taosHashCancelIterate(dbCache->vgInfo->vgHash, pIter);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }
    
    pIter = taosHashIterate(dbCache->vgInfo->vgHash, pIter);
    vgInfo = NULL;
  }

  *vgroupList = vgList;
  vgList = NULL;

_return:

  if (dbCache) {
    CTG_UNLOCK(CTG_READ, &dbCache->vgLock);
    taosHashRelease(pCatalog->dbCache, dbCache);
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

  if (NULL == dbInfo->vgHash || dbInfo->vgVersion < 0 || taosHashGetSize(dbInfo->vgHash) <= 0) {
    ctgError("invalid db vgInfo, dbName:%s, vgHash:%p, vgVersion:%d", dbName, dbInfo->vgHash, dbInfo->vgVersion);
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
  }

  if (NULL == pCatalog->dbCache) {
    SHashObj *cache = taosHashInit(ctgMgmt.cfg.maxDBCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (NULL == cache) {
      ctgError("taosHashInit %d failed", CTG_DEFAULT_CACHE_DB_NUMBER);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }

    if (NULL != atomic_val_compare_exchange_ptr(&pCatalog->dbCache, NULL, cache)) {
      taosHashCleanup(cache);
    }
  }

  bool newAdded = false;
  SDbVgVersion vgVersion = {.dbId = dbInfo->dbId, .vgVersion = dbInfo->vgVersion};

  SCtgDBCache *dbCache = (SCtgDBCache *)taosHashAcquire(pCatalog->dbCache, dbName, strlen(dbName));
  if (dbCache) {
    CTG_LOCK(CTG_WRITE, &dbCache->vgLock);
    
    if (NULL == dbCache->vgInfo) {
      newAdded = true;

      dbCache->vgInfo = dbInfo;
    } else {
      if (dbCache->vgInfo->dbId != dbInfo->dbId) {
        ctgMetaRentRemove(&pCatalog->dbRent, dbCache->vgInfo->dbId, ctgDbVgVersionCompare);
        newAdded = true;
      } else if (dbInfo->vgVersion <= dbCache->vgInfo->vgVersion) {
        ctgInfo("db vgVersion is not new, db:%s, vgVersion:%d, current:%d", dbName, dbInfo->vgVersion, dbCache->vgInfo->vgVersion);
        CTG_UNLOCK(CTG_WRITE, &dbCache->vgLock);
        taosHashRelease(pCatalog->dbCache, dbCache);
        
        goto _return;
      }
      
      if (dbCache->vgInfo->vgHash) {
        ctgInfo("cleanup db vgHash, db:%s", dbName);
        taosHashCleanup(dbCache->vgInfo->vgHash);
        dbCache->vgInfo->vgHash = NULL;
      }
    }

    CTG_UNLOCK(CTG_WRITE, &dbCache->vgLock);
    taosHashRelease(pCatalog->dbCache, dbCache);
  } else {
    SCtgDBCache newDBCache = {0};
    newDBCache.vgInfo = dbInfo;
    
    if (taosHashPut(pCatalog->dbCache, dbName, strlen(dbName), &newDBCache, sizeof(newDBCache)) != 0) {
      ctgError("taosHashPut db & db vgroup to cache failed, db:%s", dbName);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }

    newAdded = true;
  }

  dbInfo = NULL;

  strncpy(vgVersion.dbName, dbName, sizeof(vgVersion.dbName));
  
  if (newAdded) {
    CTG_ERR_JRET(ctgMetaRentAdd(&pCatalog->dbRent, &vgVersion, vgVersion.dbId, sizeof(SDbVgVersion)));
  } else {
    CTG_ERR_JRET(ctgMetaRentUpdate(&pCatalog->dbRent, &vgVersion, vgVersion.dbId, sizeof(SDbVgVersion), ctgDbVgVersionCompare));
  }
  
  ctgDebug("dbName:%s vgroup updated, vgVersion:%d", dbName, vgVersion.vgVersion);


_return:

  if (dbInfo) {
    taosHashCleanup(dbInfo->vgHash);
    dbInfo->vgHash = NULL;
    tfree(dbInfo);
  }
  
  CTG_RET(code);
}


int32_t catalogRemoveDB(struct SCatalog* pCatalog, const char* dbName, uint64_t dbId) {
  int32_t code = 0;
  bool removed = false;
  
  if (NULL == pCatalog || NULL == dbName) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (NULL == pCatalog->dbCache) {
    return TSDB_CODE_SUCCESS;
  }
  
  CTG_ERR_RET(ctgValidateAndRemoveDb(pCatalog, dbName, dbId, &removed));
  if (!removed) {
    return TSDB_CODE_SUCCESS;
  }
  
  ctgInfo("db removed from cache, db:%s, uid:%"PRIx64, dbName, dbId);

  CTG_ERR_RET(ctgMetaRentRemove(&pCatalog->dbRent, dbId, ctgDbVgVersionCompare));
  
  ctgDebug("db removed from rent, db:%s, uid:%"PRIx64, dbName, dbId);
  
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
  SCtgDBCache* dbCache = NULL;
  SArray *vgList = NULL;

  *pVgroupList = NULL;
  
  CTG_ERR_JRET(ctgGetTableMeta(pCatalog, pRpc, pMgmtEps, pTableName, false, &tbMeta, -1));

  char db[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, db);
  CTG_ERR_JRET(ctgGetDBVgroup(pCatalog, pRpc, pMgmtEps, db, false, &dbCache));

  // REMOEV THIS ....
  if (0 == tbMeta->vgId) {
    SVgroupInfo vgroup = {0};
    
    catalogGetTableHashVgroup(pCatalog, pRpc, pMgmtEps, pTableName, &vgroup);

    tbMeta->vgId = vgroup.vgId;
  }
  // REMOVE THIS ....

  if (tbMeta->tableType == TSDB_SUPER_TABLE) {
    CTG_ERR_JRET(ctgGetVgInfoFromDB(pCatalog, pRpc, pMgmtEps, dbCache->vgInfo, pVgroupList));
  } else {
    int32_t vgId = tbMeta->vgId;
    if (NULL == taosHashGetClone(dbCache->vgInfo->vgHash, &vgId, sizeof(vgId), &vgroupInfo)) {
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

  if (dbCache) {
    CTG_UNLOCK(CTG_READ, &dbCache->vgLock);
    taosHashRelease(pCatalog->dbCache, dbCache);
  }

  if (vgList) {
    taosArrayDestroy(vgList);
    vgList = NULL;
  }
  
  CTG_RET(code);
}


int32_t catalogGetTableHashVgroup(struct SCatalog *pCatalog, void *pTransporter, const SEpSet *pMgmtEps, const SName *pTableName, SVgroupInfo *pVgroup) {
  SCtgDBCache* dbCache = NULL;
  int32_t code = 0;

  char db[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, db);

  CTG_ERR_RET(ctgGetDBVgroup(pCatalog, pTransporter, pMgmtEps, db, false, &dbCache));

  CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCatalog, dbCache->vgInfo, pTableName, pVgroup));

_return:

  if (dbCache) {
    CTG_UNLOCK(CTG_READ, &dbCache->vgLock);  
    taosHashRelease(pCatalog->dbCache, dbCache);
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

  CTG_RET(ctgMetaRentGet(&pCatalog->stbRent, (void **)stables, num, sizeof(SSTableMetaVersion)));
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



