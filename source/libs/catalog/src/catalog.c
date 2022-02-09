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

int32_t ctgDbgGetTbMetaNum(SCtgDBCache *dbCache) {
  return dbCache->tbCache.metaCache ? (int32_t)taosHashGetSize(dbCache->tbCache.metaCache) : 0;
}

int32_t ctgDbgGetStbNum(SCtgDBCache *dbCache) {
  return dbCache->tbCache.stbCache ? (int32_t)taosHashGetSize(dbCache->tbCache.stbCache) : 0;
}

int32_t ctgDbgGetRentNum(SCtgRentMgmt *rent) {
  int32_t num = 0;
  for (uint16_t i = 0; i < rent->slotNum; ++i) {
    SCtgRentSlot *slot = &rent->slots[i];
    if (NULL == slot->meta) {
      continue;
    }

    num += taosArrayGetSize(slot->meta);
  }

  return num;
}

int32_t ctgDbgGetClusterCacheNum(struct SCatalog* pCatalog, int32_t type) {
  if (NULL == pCatalog || NULL == pCatalog->dbCache) {
    return 0;
  }

  switch (type) {
    case CTG_DBG_DB_NUM:
      return (int32_t)taosHashGetSize(pCatalog->dbCache);
    case CTG_DBG_DB_RENT_NUM:
      return ctgDbgGetRentNum(&pCatalog->dbRent);
    case CTG_DBG_STB_RENT_NUM:
      return ctgDbgGetRentNum(&pCatalog->stbRent);
    default:
      break;
  }

  SCtgDBCache *dbCache = NULL;
  int32_t num = 0;
  void *pIter = taosHashIterate(pCatalog->dbCache, NULL);
  while (pIter) {
    dbCache = (SCtgDBCache *)pIter;
    switch (type) {
      case CTG_DBG_META_NUM:
        num += ctgDbgGetTbMetaNum(dbCache);
        break;
      case CTG_DBG_STB_NUM:
        num += ctgDbgGetStbNum(dbCache);
        break;
      default:
        ctgError("invalid type:%d", type);
        break;
    }
    pIter = taosHashIterate(pCatalog->dbCache, pIter);
  }

  return num;
}


void ctgDbgShowDBCache(SHashObj *dbHash) {
  if (NULL == dbHash) {
    return;
  }

  int32_t i = 0;
  SCtgDBCache *dbCache = NULL;
  void *pIter = taosHashIterate(dbHash, NULL);
  while (pIter) {
    char *dbFName = NULL;
    size_t len = 0;
    
    dbCache = (SCtgDBCache *)pIter;

    taosHashGetKey(dbCache, (void **)&dbFName, &len);
    
    CTG_CACHE_DEBUG("** %dth db [%.*s][%"PRIx64"] **", i, (int32_t)len, dbFName, dbCache->dbId);
    
    pIter = taosHashIterate(dbHash, pIter);
  }
}




void ctgDbgShowClusterCache(struct SCatalog* pCatalog) {
  if (NULL == pCatalog) {
    return;
  }

  CTG_CACHE_DEBUG("## cluster %"PRIx64" %p cache Info ##", pCatalog->clusterId, pCatalog);
  CTG_CACHE_DEBUG("db:%d meta:%d stb:%d dbRent:%d stbRent:%d", ctgDbgGetClusterCacheNum(pCatalog, CTG_DBG_DB_NUM), ctgDbgGetClusterCacheNum(pCatalog, CTG_DBG_META_NUM), 
    ctgDbgGetClusterCacheNum(pCatalog, CTG_DBG_STB_NUM), ctgDbgGetClusterCacheNum(pCatalog, CTG_DBG_DB_RENT_NUM), ctgDbgGetClusterCacheNum(pCatalog, CTG_DBG_STB_RENT_NUM));    
  
  ctgDbgShowDBCache(pCatalog->dbCache);
}

int32_t ctgInitDBCache(struct SCatalog* pCatalog) {
  if (NULL == pCatalog->dbCache) {
    SHashObj *cache = taosHashInit(ctgMgmt.cfg.maxDBCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    if (NULL == cache) {
      ctgError("taosHashInit %d failed", CTG_DEFAULT_CACHE_DB_NUMBER);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }

    if (NULL != atomic_val_compare_exchange_ptr(&pCatalog->dbCache, NULL, cache)) {
      taosHashCleanup(cache);
    }
  }

  return TSDB_CODE_SUCCESS;
}


int32_t ctgInitTbMetaCache(struct SCatalog* pCatalog, SCtgDBCache *dbCache) {
  if (NULL == dbCache->tbCache.metaCache) {
    if (dbCache->deleted) {
      ctgInfo("db is dropping, dbId:%"PRIx64, dbCache->dbId);
      CTG_ERR_RET(TSDB_CODE_CTG_DB_DROPPED);
    }

    SHashObj *metaCache = taosHashInit(ctgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (NULL == metaCache) {
      ctgError("taosHashInit failed, num:%d", ctgMgmt.cfg.maxTblCacheNum);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }
    
    if (NULL != atomic_val_compare_exchange_ptr(&dbCache->tbCache.metaCache, NULL, metaCache)) {
      taosHashCleanup(metaCache);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitStbCache(struct SCatalog* pCatalog, SCtgDBCache *dbCache) {
  if (NULL == dbCache->tbCache.stbCache) {
    if (dbCache->deleted) {
      ctgInfo("db is dropping, dbId:%"PRIx64, dbCache->dbId);
      CTG_ERR_RET(TSDB_CODE_CTG_DB_DROPPED);
    }
  
    SHashObj *cache = taosHashInit(ctgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), true, HASH_ENTRY_LOCK);
    if (NULL == cache) {
      ctgError("taosHashInit failed, num:%d", ctgMgmt.cfg.maxTblCacheNum);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }

    if (NULL != atomic_val_compare_exchange_ptr(&dbCache->tbCache.stbCache, NULL, cache)) {
      taosHashCleanup(cache);
    }
  }

  return TSDB_CODE_SUCCESS;
}



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


void ctgFreeTableMetaCache(SCtgTbMetaCache *cache) {
  CTG_LOCK(CTG_WRITE, &cache->stbLock);
  if (cache->stbCache) {
    taosHashCleanup(cache->stbCache);
    cache->stbCache = NULL;
  }
  CTG_UNLOCK(CTG_WRITE, &cache->stbLock);

  CTG_LOCK(CTG_WRITE, &cache->metaLock);
  if (cache->metaCache) {
    taosHashCleanup(cache->metaCache);
    cache->metaCache = NULL;
  }
  CTG_UNLOCK(CTG_WRITE, &cache->metaLock);
}

void ctgFreeDbCache(SCtgDBCache *dbCache) {
  if (NULL == dbCache) {
    return;
  }

  atomic_store_8(&dbCache->deleted, 1);

  CTG_LOCK(CTG_WRITE, &dbCache->vgLock);
  if (dbCache->vgInfo) {

    if (dbCache->vgInfo->vgHash) {
      taosHashCleanup(dbCache->vgInfo->vgHash);
      dbCache->vgInfo->vgHash = NULL;
    }

    tfree(dbCache->vgInfo);
  }
  CTG_UNLOCK(CTG_WRITE, &dbCache->vgLock);

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

int32_t ctgGetDBVgroupFromCache(struct SCatalog* pCatalog, const char *dbFName, SCtgDBCache **dbCache, bool *inCache) {
  if (NULL == pCatalog->dbCache) {
    *inCache = false;
    ctgWarn("empty db cache, dbFName:%s", dbFName);
    return TSDB_CODE_SUCCESS;
  }

  SCtgDBCache *cache = NULL;

  while (true) {
    cache = taosHashAcquire(pCatalog->dbCache, dbFName, strlen(dbFName));

    if (NULL == cache) {
      *inCache = false;
      ctgWarn("not in db vgroup cache, dbFName:%s", dbFName);
      return TSDB_CODE_SUCCESS;
    }

    CTG_LOCK(CTG_READ, &cache->vgLock);
    if (NULL == cache->vgInfo) {
      CTG_UNLOCK(CTG_READ, &cache->vgLock);
      taosHashRelease(pCatalog->dbCache, cache);
      ctgWarn("db cache vgInfo is NULL, dbFName:%s", dbFName);
      
      continue;
    }

    break;
  }

  *dbCache = cache;
  *inCache = true;

  ctgDebug("Got db vgroup from cache, dbFName:%s", dbFName);
  
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
  CTG_LOCK(CTG_READ, &dbCache->tbCache.metaLock);  
  STableMeta *tbMeta = taosHashGet(dbCache->tbCache.metaCache, tbName, strlen(tbName));
  CTG_UNLOCK(CTG_READ, &dbCache->tbCache.metaLock);
  
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

  if (NULL == dbCache->tbCache.metaCache) {
    *exist = 0;
    taosHashRelease(pCatalog->dbCache, dbCache);
    ctgWarn("empty tbmeta cache, dbFName:%s, tbName:%s", db, pTableName->tname);
    return TSDB_CODE_SUCCESS;
  }
  
  size_t sz = 0;  
  CTG_LOCK(CTG_READ, &dbCache->tbCache.metaLock);
  STableMeta *tbMeta = taosHashGetCloneExt(dbCache->tbCache.metaCache, pTableName->tname, strlen(pTableName->tname), NULL, (void **)pTableMeta, &sz);
  CTG_UNLOCK(CTG_READ, &dbCache->tbCache.metaLock);

  if (NULL == *pTableMeta) {
    *exist = 0;
    taosHashRelease(pCatalog->dbCache, dbCache);
    ctgDebug("tbmeta not in cache, dbFName:%s, tbName:%s", db, pTableName->tname);
    return TSDB_CODE_SUCCESS;
  }

  *exist = 1;
  
  tbMeta = *pTableMeta;

  if (tbMeta->tableType != TSDB_CHILD_TABLE) {
    taosHashRelease(pCatalog->dbCache, dbCache);
    ctgDebug("Got tbmeta from cache, type:%d, dbFName:%s, tbName:%s", tbMeta->tableType, db, pTableName->tname);
    return TSDB_CODE_SUCCESS;
  }
  
  CTG_LOCK(CTG_READ, &dbCache->tbCache.stbLock);
  
  STableMeta **stbMeta = taosHashGet(dbCache->tbCache.stbCache, &tbMeta->suid, sizeof(tbMeta->suid));
  if (NULL == stbMeta || NULL == *stbMeta) {
    CTG_UNLOCK(CTG_READ, &dbCache->tbCache.stbLock);
    taosHashRelease(pCatalog->dbCache, dbCache);
    ctgError("stable not in stbCache, suid:%"PRIx64, tbMeta->suid);
    tfree(*pTableMeta);
    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }

  if ((*stbMeta)->suid != tbMeta->suid) {    
    CTG_UNLOCK(CTG_READ, &dbCache->tbCache.stbLock);
    taosHashRelease(pCatalog->dbCache, dbCache);
    tfree(*pTableMeta);
    ctgError("stable suid in stbCache mis-match, expected suid:%"PRIx64 ",actual suid:%"PRIx64, tbMeta->suid, (*stbMeta)->suid);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  int32_t metaSize = sizeof(STableMeta) + ((*stbMeta)->tableInfo.numOfTags + (*stbMeta)->tableInfo.numOfColumns) * sizeof(SSchema);
  *pTableMeta = realloc(*pTableMeta, metaSize);
  if (NULL == *pTableMeta) {    
    CTG_UNLOCK(CTG_READ, &dbCache->tbCache.stbLock);
    taosHashRelease(pCatalog->dbCache, dbCache);
    ctgError("realloc size[%d] failed", metaSize);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  memcpy(&(*pTableMeta)->sversion, &(*stbMeta)->sversion, metaSize - sizeof(SCTableMeta));

  CTG_UNLOCK(CTG_READ, &dbCache->tbCache.stbLock);

  taosHashRelease(pCatalog->dbCache, dbCache);

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

  CTG_LOCK(CTG_READ, &dbCache->tbCache.metaLock);
  STableMeta *pTableMeta = (STableMeta *)taosHashAcquire(dbCache->tbCache.metaCache, pTableName->tname, strlen(pTableName->tname));
  CTG_UNLOCK(CTG_READ, &dbCache->tbCache.metaLock);

  if (NULL == pTableMeta) {
    ctgWarn("tbmeta not in cache, dbFName:%s, tbName:%s", dbName, pTableName->tname);  
    taosHashRelease(pCatalog->dbCache, dbCache);
    
    return TSDB_CODE_SUCCESS;
  }

  *tbType = atomic_load_8(&pTableMeta->tableType);

  taosHashRelease(dbCache->tbCache.metaCache, dbCache);
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

#if 1
int32_t ctgSTableVersionCompare(const void* key1, const void* key2) {
  if (*(uint64_t *)key1 < ((SSTableMetaVersion*)key2)->suid) {
    return -1;
  } else if (*(uint64_t *)key1 > ((SSTableMetaVersion*)key2)->suid) {
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
#else

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

#endif

int32_t ctgMetaRentInit(SCtgRentMgmt *mgmt, uint32_t rentSec, int8_t type) {
  mgmt->slotRIdx = 0;
  mgmt->slotNum = rentSec / CTG_RENT_SLOT_SECOND;
  mgmt->type = type;

  size_t msgSize = sizeof(SCtgRentSlot) * mgmt->slotNum;
  
  mgmt->slots = calloc(1, msgSize);
  if (NULL == mgmt->slots) {
    qError("calloc %d failed", (int32_t)msgSize);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
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
    qDebug("meta slot before sorte, slot idx:%d, type:%d, size:%d", widx, mgmt->type, (int32_t)taosArrayGetSize(slot->meta));
    taosArraySort(slot->meta, compare);
    slot->needSort = false;
    qDebug("meta slot sorted, slot idx:%d, type:%d, size:%d", widx, mgmt->type, (int32_t)taosArrayGetSize(slot->meta));
  }

  void *orig = taosArraySearch(slot->meta, &id, compare, TD_EQ);
  if (NULL == orig) {
    qError("meta not found in slot, id:%"PRIx64", slot idx:%d, type:%d, size:%d", id, widx, mgmt->type, (int32_t)taosArrayGetSize(slot->meta));
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

int32_t ctgAddDBCache(struct SCatalog *pCatalog, const char *dbFName, SCtgDBCache *dbCache) {
  int32_t code = 0;

  code = taosHashPut(pCatalog->dbCache, dbFName, strlen(dbFName), dbCache, sizeof(SCtgDBCache));
  if (code) {
    if (HASH_NODE_EXIST(code)) {
      ctgDebug("db already in cache, dbFName:%s", dbFName);
      return TSDB_CODE_SUCCESS;
    }
    
    ctgError("taosHashPut db to cache failed, dbFName:%s", dbFName);
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
  }
  
  SDbVgVersion vgVersion = {.dbId = dbCache->dbId, .vgVersion = dbCache->vgInfo ? dbCache->vgInfo->vgVersion : -1};
  strncpy(vgVersion.dbFName, dbFName, sizeof(vgVersion.dbFName));

  ctgDebug("dbCache added, dbFName:%s, vgVersion:%d, dbId:%"PRIx64, dbFName, vgVersion.vgVersion, dbCache->dbId);

  CTG_ERR_JRET(ctgMetaRentAdd(&pCatalog->dbRent, &vgVersion, vgVersion.dbId, sizeof(SDbVgVersion)));

  return TSDB_CODE_SUCCESS;

_return:

  ctgFreeDbCache(dbCache);

  CTG_RET(code);
}


void ctgRemoveAndFreeTableMeta(struct SCatalog* pCatalog, SCtgTbMetaCache *cache) {
  CTG_LOCK(CTG_WRITE, &cache->stbLock);
  if (cache->stbCache) {
    void *pIter = taosHashIterate(cache->stbCache, NULL);
    while (pIter) {
      uint64_t *suid = NULL;
      taosHashGetKey(pIter, (void **)&suid, NULL);

      if (TSDB_CODE_SUCCESS == ctgMetaRentRemove(&pCatalog->stbRent, *suid, ctgSTableVersionCompare)) {
        ctgDebug("stb removed from rent, suid:%"PRIx64, *suid);
      }
          
      pIter = taosHashIterate(cache->stbCache, pIter);
    }
  }
  CTG_UNLOCK(CTG_WRITE, &cache->stbLock);

  ctgFreeTableMetaCache(cache);
}


int32_t ctgValidateAndRemoveDb(struct SCatalog* pCatalog, SCtgDBCache *dbCache, const char* dbFName) {
  if (taosHashRemove(pCatalog->dbCache, dbFName, strlen(dbFName))) {
    ctgInfo("taosHashRemove from dbCache failed, may be removed, dbFName:%s", dbFName);
    CTG_ERR_RET(TSDB_CODE_CTG_DB_DROPPED);
  }

  atomic_store_8(&dbCache->deleted, 1);
  
  CTG_LOCK(CTG_WRITE, &dbCache->vgLock);
  if (dbCache->vgInfo) {
    ctgInfo("cleanup db vgInfo, dbFName:%s, dbId:%"PRIx64, dbFName, dbCache->dbId);
    
    if (dbCache->vgInfo->vgHash) { 
      taosHashCleanup(dbCache->vgInfo->vgHash);
    }
    
    tfree(dbCache->vgInfo);
  }
  CTG_UNLOCK(CTG_WRITE, &dbCache->vgLock);

  ctgRemoveAndFreeTableMeta(pCatalog, &dbCache->tbCache);

  ctgInfo("db removed from cache, dbFName:%s, uid:%"PRIx64, dbFName, dbCache->dbId);

  CTG_ERR_RET(ctgMetaRentRemove(&pCatalog->dbRent, dbCache->dbId, ctgDbVgVersionCompare));
  
  ctgDebug("db removed from rent, dbFName:%s, uid:%"PRIx64, dbFName, dbCache->dbId);
  
  return TSDB_CODE_SUCCESS;
}


int32_t ctgAcquireDBCache(struct SCatalog* pCatalog, const char *dbFName, uint64_t dbId, SCtgDBCache **pCache) {
  int32_t code = 0;
  SCtgDBCache *dbCache = NULL;

  CTG_LOCK(CTG_WRITE, &pCatalog->dbLock);
  
  while (true) {
    dbCache = (SCtgDBCache *)taosHashAcquire(pCatalog->dbCache, dbFName, strlen(dbFName));
    if (dbCache) {
    // TODO OPEN IT
#if 0    
      if (dbCache->dbId == dbId) {
        *pCache = dbCache;
        return TSDB_CODE_SUCCESS;
      }
#else
      if (0 == dbId) {
        *pCache = dbCache;
        return TSDB_CODE_SUCCESS;
      }

      if (dbId && (dbCache->dbId == 0)) {
        dbCache->dbId = dbId;
        *pCache = dbCache;
        return TSDB_CODE_SUCCESS;
      }
      
      if (dbCache->dbId == dbId) {
        *pCache = dbCache;
        return TSDB_CODE_SUCCESS;
      }
#endif
      code = ctgValidateAndRemoveDb(pCatalog, dbCache, dbFName);
      taosHashRelease(pCatalog->dbCache, dbCache);
      dbCache = NULL;
      if (code) {
        if (TSDB_CODE_CTG_DB_DROPPED == code) {
          continue;
        }
        
        CTG_ERR_JRET(code);
      }
    }

    SCtgDBCache newDBCache = {0};
    newDBCache.dbId = dbId;
    
    CTG_ERR_JRET(ctgAddDBCache(pCatalog, dbFName, &newDBCache));
  }

_return:

  if (dbCache) {
    taosHashRelease(pCatalog->dbCache, dbCache);
  }

  CTG_UNLOCK(CTG_WRITE, &pCatalog->dbLock);
  
  CTG_RET(code);
}



int32_t ctgUpdateTbMetaImpl(struct SCatalog *pCatalog, SCtgTbMetaCache *tbCache, char *dbFName, char *tbName, STableMeta *meta, int32_t metaSize) {
  CTG_LOCK(CTG_READ, &tbCache->metaLock);  
  if (taosHashPut(tbCache->metaCache, tbName, strlen(tbName), meta, metaSize) != 0) {
    CTG_UNLOCK(CTG_READ, &tbCache->metaLock);  
    ctgError("taosHashPut tbmeta to cache failed, dbFName:%s, tbName:%s, tbType:%d", dbFName, tbName, meta->tableType);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }
  CTG_UNLOCK(CTG_READ, &tbCache->metaLock);
  
  ctgDebug("tbmeta updated to cache, dbFName:%s, tbName:%s, tbType:%d", dbFName, tbName, meta->tableType);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgUpdateStbMetaImpl(struct SCatalog *pCatalog, SCtgTbMetaCache *tbCache, char *dbFName, char *tbName, STableMeta *meta, int32_t metaSize) {
  bool newAdded = false;
  int32_t code = 0;
  SSTableMetaVersion metaRent = {.suid = meta->suid, .sversion = meta->sversion, .tversion = meta->tversion};
  strcpy(metaRent.dbFName, dbFName);
  strcpy(metaRent.stbName, tbName);
  
  CTG_LOCK(CTG_WRITE, &tbCache->stbLock);
  
  CTG_LOCK(CTG_READ, &tbCache->metaLock);
  STableMeta *orig = taosHashAcquire(tbCache->metaCache,  tbName, strlen(tbName));
  if (orig) {
    if (orig->suid != meta->suid) {
      if (taosHashRemove(tbCache->stbCache, &orig->suid, sizeof(orig->suid))) {
        ctgError("stb not exist in stbCache, db:%s, stb:%s, suid:%"PRIx64, dbFName, tbName, orig->suid);
      }
      
      ctgMetaRentRemove(&pCatalog->stbRent, orig->suid, ctgSTableVersionCompare);
    }
  
    taosHashRelease(tbCache->metaCache, orig);
  }
  CTG_UNLOCK(CTG_READ, &tbCache->metaLock);    

  CTG_ERR_JRET(ctgUpdateTbMetaImpl(pCatalog, tbCache, dbFName, tbName, meta, metaSize));

  CTG_LOCK(CTG_READ, &tbCache->metaLock);  
  STableMeta *tbMeta = taosHashGet(tbCache->metaCache, tbName, strlen(tbName));
  if (taosHashPutExt(tbCache->stbCache, &meta->suid, sizeof(meta->suid), &tbMeta, POINTER_BYTES, &newAdded) != 0) {
    CTG_UNLOCK(CTG_READ, &tbCache->metaLock);    
    CTG_UNLOCK(CTG_WRITE, &tbCache->stbLock);
    ctgError("taosHashPutExt stable to stable cache failed, suid:%"PRIx64, meta->suid);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }
  CTG_UNLOCK(CTG_READ, &tbCache->metaLock);    
  
  CTG_UNLOCK(CTG_WRITE, &tbCache->stbLock);
  
  ctgDebug("update stable to cache, suid:%"PRIx64, meta->suid);
  
  if (newAdded) {
    CTG_ERR_RET(ctgMetaRentAdd(&pCatalog->stbRent, &metaRent, metaRent.suid, sizeof(SSTableMetaVersion)));
  } else {
    CTG_ERR_RET(ctgMetaRentUpdate(&pCatalog->stbRent, &metaRent, metaRent.suid, sizeof(SSTableMetaVersion), ctgSTableVersionCompare));
  }

  return TSDB_CODE_SUCCESS;

_return:

  CTG_UNLOCK(CTG_WRITE, &tbCache->stbLock);

  CTG_RET(code);
}


int32_t ctgUpdateTableMetaCache(struct SCatalog *pCatalog, STableMetaOutput *output) {
  int32_t code = 0;
  SCtgDBCache *dbCache = NULL;

  if ((!CTG_IS_META_CTABLE(output->metaType)) && NULL == output->tbMeta) {
    ctgError("no valid tbmeta got from meta rsp, dbFName:%s, tbName:%s", output->dbFName, output->tbName);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  CTG_ERR_RET(ctgInitDBCache(pCatalog));

  CTG_ERR_JRET(ctgAcquireDBCache(pCatalog, output->dbFName, output->dbId, &dbCache));

  CTG_ERR_JRET(ctgInitTbMetaCache(pCatalog, dbCache));
  CTG_ERR_JRET(ctgInitStbCache(pCatalog, dbCache));

  if (CTG_IS_META_CTABLE(output->metaType) || CTG_IS_META_BOTH(output->metaType)) {
    CTG_ERR_JRET(ctgUpdateTbMetaImpl(pCatalog, &dbCache->tbCache, output->dbFName, output->ctbName, (STableMeta *)&output->ctbMeta, sizeof(output->ctbMeta)));
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
    CTG_ERR_JRET(ctgUpdateStbMetaImpl(pCatalog, &dbCache->tbCache, output->dbFName, output->tbName, output->tbMeta, tbSize));
  } else {
    CTG_ERR_JRET(ctgUpdateTbMetaImpl(pCatalog, &dbCache->tbCache, output->dbFName, output->tbName, output->tbMeta, tbSize));
  }

_return:

  if (dbCache) {
    taosHashRelease(pCatalog->dbCache, dbCache);    
    CTG_UNLOCK(CTG_WRITE, &pCatalog->dbLock);
  }

  CTG_RET(code);
}

int32_t ctgGetDBVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* dbFName, bool forceUpdate, SCtgDBCache** dbCache) {
  bool inCache = false;
  if (!forceUpdate) {
    CTG_ERR_RET(ctgGetDBVgroupFromCache(pCatalog, dbFName, dbCache, &inCache));
    if (inCache) {
      return TSDB_CODE_SUCCESS;
    }

    ctgDebug("failed to get DB vgroupInfo from cache, dbName:%s, load it from mnode, update:%d", dbFName, forceUpdate);
  }

  SUseDbOutput DbOut = {0};
  SBuildUseDBInput input = {0};

  tstrncpy(input.db, dbFName, tListLen(input.db));
  input.vgVersion = CTG_DEFAULT_INVALID_VERSION;

  while (true) {
    CTG_ERR_RET(ctgGetDBVgroupFromMnode(pCatalog, pRpc, pMgmtEps, &input, &DbOut));
    CTG_ERR_RET(catalogUpdateDBVgroup(pCatalog, dbFName, DbOut.dbId, DbOut.dbVgroup));
    CTG_ERR_RET(ctgGetDBVgroupFromCache(pCatalog, dbFName, dbCache, &inCache));

    if (!inCache) {
      ctgWarn("can't get db vgroup from cache, will retry, db:%s", dbFName);
      continue;
    }

    break;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgValidateAndRemoveStbMeta(struct SCatalog* pCatalog, const char* dbName, const char* stbName, uint64_t suid, bool *removed) {
  *removed = false;

  SCtgDBCache *dbCache = (SCtgDBCache *)taosHashAcquire(pCatalog->dbCache, dbName, strlen(dbName));
  if (NULL == dbCache) {
    ctgInfo("db not exist in dbCache, may be removed, db:%s", dbName);
    return TSDB_CODE_SUCCESS;
  }

  CTG_LOCK(CTG_WRITE, &dbCache->tbCache.stbLock);
  if (taosHashRemove(dbCache->tbCache.stbCache, &suid, sizeof(suid))) {
    CTG_UNLOCK(CTG_WRITE, &dbCache->tbCache.stbLock);
    taosHashRelease(pCatalog->dbCache, dbCache);
    ctgInfo("stb not exist in stbCache, may be removed, db:%s, stb:%s, suid:%"PRIx64, dbName, stbName, suid);
    return TSDB_CODE_SUCCESS;
  }

  CTG_LOCK(CTG_READ, &dbCache->tbCache.metaLock);
  if (taosHashRemove(dbCache->tbCache.metaCache, stbName, strlen(stbName))) {  
    CTG_UNLOCK(CTG_READ, &dbCache->tbCache.metaLock);
    CTG_UNLOCK(CTG_WRITE, &dbCache->tbCache.stbLock);
    taosHashRelease(pCatalog->dbCache, dbCache);
    ctgError("stb not exist in cache, db:%s, stb:%s, suid:%"PRIx64, dbName, stbName, suid);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }  
  CTG_UNLOCK(CTG_READ, &dbCache->tbCache.metaLock);
  
  CTG_UNLOCK(CTG_WRITE, &dbCache->tbCache.stbLock);
  
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
    qError("catalog already initialized");
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
    qError("catalog cluster cache are not ready, clusterId:%"PRIx64, clusterId);
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

  CTG_API_ENTER();

  if (NULL == pCatalog->dbCache) {
    *version = CTG_DEFAULT_INVALID_VERSION;
    ctgInfo("empty db cache, dbName:%s", dbName);
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }

  SCtgDBCache *db = taosHashAcquire(pCatalog->dbCache, dbName, strlen(dbName));
  if (NULL == db) {
    *version = CTG_DEFAULT_INVALID_VERSION;
    ctgInfo("db not in cache, dbName:%s", dbName);
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }

  CTG_LOCK(CTG_READ, &db->vgLock);
  
  if (NULL == db->vgInfo) {
    CTG_UNLOCK(CTG_READ, &db->vgLock);

    *version = CTG_DEFAULT_INVALID_VERSION;
    ctgInfo("db not in cache, dbName:%s", dbName);
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }

  *version = db->vgInfo->vgVersion;
  CTG_UNLOCK(CTG_READ, &db->vgLock);
  
  taosHashRelease(pCatalog->dbCache, db);

  ctgDebug("Got db vgVersion from cache, dbName:%s, vgVersion:%d", dbName, *version);

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);
}

int32_t catalogGetDBVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* dbFName, bool forceUpdate, SArray** vgroupList) {
  if (NULL == pCatalog || NULL == dbFName || NULL == pRpc || NULL == pMgmtEps || NULL == vgroupList) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_ENTER();

  SCtgDBCache* dbCache   = NULL;
  SVgroupInfo *vgInfo = NULL;

  int32_t code = 0;
  SArray *vgList = NULL;
  CTG_ERR_JRET(ctgGetDBVgroup(pCatalog, pRpc, pMgmtEps, dbFName, forceUpdate, &dbCache));

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

  CTG_API_LEAVE(code);  
}


int32_t catalogUpdateDBVgroup(struct SCatalog* pCatalog, const char* dbFName, uint64_t dbId, SDBVgroupInfo* dbInfo) {
  int32_t code = 0;

  CTG_API_ENTER();
  
  if (NULL == pCatalog || NULL == dbFName || NULL == dbInfo) {
    CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (NULL == dbInfo->vgHash || dbInfo->vgVersion < 0 || taosHashGetSize(dbInfo->vgHash) <= 0) {
    ctgError("invalid db vgInfo, dbFName:%s, vgHash:%p, vgVersion:%d", dbFName, dbInfo->vgHash, dbInfo->vgVersion);
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
  }

  CTG_ERR_JRET(ctgInitDBCache(pCatalog));

  bool newAdded = false;
  SDbVgVersion vgVersion = {.dbId = dbId, .vgVersion = dbInfo->vgVersion};

  SCtgDBCache *dbCache = NULL;
  CTG_ERR_JRET(ctgAcquireDBCache(pCatalog, dbFName, dbId, &dbCache));
  
  CTG_LOCK(CTG_WRITE, &dbCache->vgLock);
  if (dbCache->deleted) {
    ctgInfo("db is dropping, dbFName:%s, dbId:%"PRIx64, dbFName, dbId);
    CTG_UNLOCK(CTG_WRITE, &dbCache->vgLock);
    taosHashRelease(pCatalog->dbCache, dbCache);
    CTG_ERR_JRET(TSDB_CODE_CTG_DB_DROPPED);
  }
  
  if (NULL == dbCache->vgInfo) {
    dbCache->vgInfo = dbInfo;
  } else {
    if (dbInfo->vgVersion <= dbCache->vgInfo->vgVersion) {
      ctgInfo("db vgVersion is old, dbFName:%s, vgVersion:%d, current:%d", dbFName, dbInfo->vgVersion, dbCache->vgInfo->vgVersion);
      CTG_UNLOCK(CTG_WRITE, &dbCache->vgLock);
      taosHashRelease(pCatalog->dbCache, dbCache);
      
      goto _return;
    }
    
    if (dbCache->vgInfo->vgHash) {
      ctgDebug("cleanup db vgHash, dbFName:%s", dbFName);
      taosHashCleanup(dbCache->vgInfo->vgHash);
      dbCache->vgInfo->vgHash = NULL;
    }

    tfree(dbCache->vgInfo);
    dbCache->vgInfo = dbInfo;
  }

  dbInfo = NULL;

  CTG_UNLOCK(CTG_WRITE, &dbCache->vgLock);
  taosHashRelease(pCatalog->dbCache, dbCache);

  strncpy(vgVersion.dbFName, dbFName, sizeof(vgVersion.dbFName));
  CTG_ERR_JRET(ctgMetaRentUpdate(&pCatalog->dbRent, &vgVersion, vgVersion.dbId, sizeof(SDbVgVersion), ctgDbVgVersionCompare));

  ctgDebug("dbCache updated, dbFName:%s, vgVersion:%d, dbId:%"PRIx64, dbFName, vgVersion.vgVersion, vgVersion.dbId);

_return:

  if (dbCache) {
    CTG_UNLOCK(CTG_WRITE, &pCatalog->dbLock);
  }

  if (dbInfo) {
    taosHashCleanup(dbInfo->vgHash);
    dbInfo->vgHash = NULL;
    tfree(dbInfo);
  }
  
  CTG_API_LEAVE(code);
}


int32_t catalogRemoveDB(struct SCatalog* pCatalog, const char* dbFName, uint64_t dbId) {
  int32_t code = 0;
  
  if (NULL == pCatalog || NULL == dbFName) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_ENTER();

  if (NULL == pCatalog->dbCache) {
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }

  SCtgDBCache *dbCache = (SCtgDBCache *)taosHashAcquire(pCatalog->dbCache, dbFName, strlen(dbFName));
  if (NULL == dbCache) {
    ctgInfo("db not exist in dbCache, may be removed, dbFName:%s", dbFName);
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }

  if (dbCache->dbId != dbId) {
    ctgInfo("db id already updated, dbFName:%s, dbId:%"PRIx64 ", targetId:%"PRIx64, dbFName, dbCache->dbId, dbId);
    taosHashRelease(pCatalog->dbCache, dbCache);
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }
  
  CTG_ERR_JRET(ctgValidateAndRemoveDb(pCatalog, dbCache, dbFName));

_return:
  
  taosHashRelease(pCatalog->dbCache, dbCache);

  CTG_API_LEAVE(code);
}

int32_t catalogRemoveSTableMeta(struct SCatalog* pCatalog, const char* dbName, const char* stbName, uint64_t suid) {
  int32_t code = 0;
  bool removed = false;
  
  if (NULL == pCatalog || NULL == dbName || NULL == stbName) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_ENTER();

  if (NULL == pCatalog->dbCache) {
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }
  
  CTG_ERR_RET(ctgValidateAndRemoveStbMeta(pCatalog, dbName, stbName, suid, &removed));
  if (!removed) {
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }
  
  ctgInfo("stb removed from cache, db:%s, stbName:%s, suid:%"PRIx64, dbName, stbName, suid);

  CTG_ERR_JRET(ctgMetaRentRemove(&pCatalog->stbRent, suid, ctgSTableVersionCompare));
  
  ctgDebug("stb removed from rent, db:%s, stbName:%s, suid:%"PRIx64, dbName, stbName, suid);

_return:
  
  CTG_API_LEAVE(code);
}


int32_t catalogGetTableMeta(struct SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, STableMeta** pTableMeta) {
  CTG_API_ENTER();

  CTG_API_LEAVE(ctgGetTableMeta(pCatalog, pTransporter, pMgmtEps, pTableName, false, pTableMeta, -1));
}

int32_t catalogGetSTableMeta(struct SCatalog* pCatalog, void * pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, STableMeta** pTableMeta) {
  CTG_API_ENTER();

  CTG_API_LEAVE(ctgGetTableMeta(pCatalog, pTransporter, pMgmtEps, pTableName, false, pTableMeta, 1));
}

int32_t catalogUpdateSTableMeta(struct SCatalog* pCatalog, STableMetaRsp *rspMsg) {
  STableMetaOutput output = {0};
  int32_t code = 0;

  CTG_API_ENTER();

  strcpy(output.dbFName, rspMsg->dbFName);
  strcpy(output.tbName, rspMsg->tbName);
  
  SET_META_TYPE_TABLE(output.metaType);
  
  CTG_ERR_JRET(queryCreateTableMetaFromMsg(rspMsg, true, &output.tbMeta));

  CTG_ERR_JRET(ctgUpdateTableMetaCache(pCatalog, &output));

_return:

  tfree(output.tbMeta);
  
  CTG_API_LEAVE(code);
}


int32_t catalogRenewTableMeta(struct SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, int32_t isSTable) {
  if (NULL == pCatalog || NULL == pTransporter || NULL == pMgmtEps || NULL == pTableName) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_ENTER();

  CTG_API_LEAVE(ctgRenewTableMetaImpl(pCatalog, pTransporter, pMgmtEps, pTableName, isSTable));
}

int32_t catalogRenewAndGetTableMeta(struct SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, STableMeta** pTableMeta, int32_t isSTable) {
  CTG_API_ENTER();

  CTG_API_LEAVE(ctgGetTableMeta(pCatalog, pTransporter, pMgmtEps, pTableName, true, pTableMeta, isSTable));
}

int32_t catalogGetTableDistVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const SName* pTableName, SArray** pVgroupList) {
  if (NULL == pCatalog || NULL == pRpc || NULL == pMgmtEps || NULL == pTableName || NULL == pVgroupList) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_ENTER();
  
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

  // TODO REMOEV THIS ....
  if (0 == tbMeta->vgId) {
    SVgroupInfo vgroup = {0};
    
    catalogGetTableHashVgroup(pCatalog, pRpc, pMgmtEps, pTableName, &vgroup);

    tbMeta->vgId = vgroup.vgId;
  }
  // TODO REMOVE THIS ....

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
  
  CTG_API_LEAVE(code);
}


int32_t catalogGetTableHashVgroup(struct SCatalog *pCatalog, void *pTransporter, const SEpSet *pMgmtEps, const SName *pTableName, SVgroupInfo *pVgroup) {
  SCtgDBCache* dbCache = NULL;
  int32_t code = 0;

  CTG_API_ENTER();

  char db[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, db);

  CTG_ERR_JRET(ctgGetDBVgroup(pCatalog, pTransporter, pMgmtEps, db, false, &dbCache));

  CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCatalog, dbCache->vgInfo, pTableName, pVgroup));

_return:

  if (dbCache) {
    CTG_UNLOCK(CTG_READ, &dbCache->vgLock);  
    taosHashRelease(pCatalog->dbCache, dbCache);
  }

  CTG_API_LEAVE(code);
}


int32_t catalogGetAllMeta(struct SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SCatalogReq* pReq, SMetaData* pRsp) {
  if (NULL == pCatalog || NULL == pTransporter || NULL == pMgmtEps || NULL == pReq || NULL == pRsp) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_ENTER();

  int32_t code = 0;
  pRsp->pTableMeta = NULL;

  if (pReq->pTableName) {
    int32_t tbNum = (int32_t)taosArrayGetSize(pReq->pTableName);
    if (tbNum <= 0) {
      ctgError("empty table name list, tbNum:%d", tbNum);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }

    pRsp->pTableMeta = taosArrayInit(tbNum, POINTER_BYTES);
    if (NULL == pRsp->pTableMeta) {
      ctgError("taosArrayInit %d failed", tbNum);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
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

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);

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
  
  CTG_API_LEAVE(code);
}

int32_t catalogGetQnodeList(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, SArray* pQnodeList) {
  if (NULL == pCatalog || NULL == pRpc  || NULL == pMgmtEps || NULL == pQnodeList) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_ENTER();

  //TODO

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);
}

int32_t catalogGetExpiredSTables(struct SCatalog* pCatalog, SSTableMetaVersion **stables, uint32_t *num) {
  if (NULL == pCatalog || NULL == stables || NULL == num) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_ENTER();

  CTG_API_LEAVE(ctgMetaRentGet(&pCatalog->stbRent, (void **)stables, num, sizeof(SSTableMetaVersion)));
}

int32_t catalogGetExpiredDBs(struct SCatalog* pCatalog, SDbVgVersion **dbs, uint32_t *num) {
  if (NULL == pCatalog || NULL == dbs || NULL == num) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_ENTER();

  CTG_API_LEAVE(ctgMetaRentGet(&pCatalog->dbRent, (void **)dbs, num, sizeof(SDbVgVersion)));
}


void catalogDestroy(void) {
  if (NULL == ctgMgmt.pCluster || atomic_load_8(&ctgMgmt.exit)) {
    return;
  }

  atomic_store_8(&ctgMgmt.exit, true);

  CTG_LOCK(CTG_WRITE, &ctgMgmt.lock);

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

  CTG_UNLOCK(CTG_WRITE, &ctgMgmt.lock);

  qInfo("catalog destroyed");
}



