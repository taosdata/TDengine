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

int32_t ctgGetDBVgroupFromCache(struct SCatalog* pCatalog, const char *dbName, SDBVgroupInfo **dbInfo, bool *inCache) {
  if (NULL == pCatalog->dbCache.cache) {
    *inCache = false;
    ctgWarn("no db cache");
    return TSDB_CODE_SUCCESS;
  }

  SDBVgroupInfo *info = NULL;

  while (true) {
    info = taosHashAcquire(pCatalog->dbCache.cache, dbName, strlen(dbName));

    if (NULL == info) {
      *inCache = false;
      ctgWarn("no db cache, dbName:%s", dbName);
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
  
  return TSDB_CODE_SUCCESS;
}



int32_t ctgGetDBVgroupFromMnode(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, SBuildUseDBInput *input, SUseDbOutput *out) {
  char *msg = NULL;
  SEpSet *pVnodeEpSet = NULL;
  int32_t msgLen = 0;

  CTG_ERR_RET(queryBuildMsg[TMSG_INDEX(TDMT_MND_USE_DB)](input, &msg, 0, &msgLen));
  
  SRpcMsg rpcMsg = {
      .msgType = TDMT_MND_USE_DB,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};

  rpcSendRecv(pRpc, (SEpSet*)pMgmtEps, &rpcMsg, &rpcRsp);
  if (TSDB_CODE_SUCCESS != rpcRsp.code) {
    ctgError("error rsp for use db, code:%x", rpcRsp.code);
    CTG_ERR_RET(rpcRsp.code);
  }

  CTG_ERR_RET(queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_USE_DB)](out, rpcRsp.pCont, rpcRsp.contLen));

  return TSDB_CODE_SUCCESS;
}


int32_t ctgGetTableMetaFromCache(struct SCatalog* pCatalog, const SName* pTableName, STableMeta** pTableMeta, int32_t *exist) {
  if (NULL == pCatalog->tableCache.cache) {
    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }

  char tbFullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pTableName, tbFullName);

  *pTableMeta = NULL;

  size_t sz = 0;
  STableMeta *tbMeta = taosHashGetCloneExt(pCatalog->tableCache.cache, tbFullName, strlen(tbFullName), NULL, (void **)pTableMeta, &sz);

  if (NULL == *pTableMeta) {
    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }

  *exist = 1;
  
  tbMeta = *pTableMeta;

  if (tbMeta->tableType != TSDB_CHILD_TABLE) {
    return TSDB_CODE_SUCCESS;
  }
  
  CTG_LOCK(CTG_READ, &pCatalog->tableCache.stableLock);
  
  STableMeta **stbMeta = taosHashGet(pCatalog->tableCache.stableCache, &tbMeta->suid, sizeof(tbMeta->suid));
  if (NULL == stbMeta || NULL == *stbMeta) {
    CTG_UNLOCK(CTG_READ, &pCatalog->tableCache.stableLock);
    qError("no stable:%"PRIx64 " meta in cache", tbMeta->suid);
    tfree(*pTableMeta);
    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }

  if ((*stbMeta)->suid != tbMeta->suid) {    
    CTG_UNLOCK(CTG_READ, &pCatalog->tableCache.stableLock);
    tfree(*pTableMeta);
    ctgError("stable cache error, expected suid:%"PRId64 ",actual suid:%"PRId64, tbMeta->suid, (*stbMeta)->suid);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  int32_t metaSize = sizeof(STableMeta) + ((*stbMeta)->tableInfo.numOfTags + (*stbMeta)->tableInfo.numOfColumns) * sizeof(SSchema);
  *pTableMeta = realloc(*pTableMeta, metaSize);
  if (NULL == *pTableMeta) {    
    CTG_UNLOCK(CTG_READ, &pCatalog->tableCache.stableLock);
    ctgError("calloc size[%d] failed", metaSize);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  memcpy(&(*pTableMeta)->sversion, &(*stbMeta)->sversion, metaSize - sizeof(SCTableMeta));

  CTG_UNLOCK(CTG_READ, &pCatalog->tableCache.stableLock);
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTableTypeFromCache(struct SCatalog* pCatalog, const SName* pTableName, int32_t *tbType) {
  if (NULL == pCatalog->tableCache.cache) {
    return TSDB_CODE_SUCCESS;
  }

  char tbFullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pTableName, tbFullName);

  size_t sz = 0;
  STableMeta *pTableMeta = NULL;
  
  taosHashGetCloneExt(pCatalog->tableCache.cache, tbFullName, strlen(tbFullName), NULL, (void **)&pTableMeta, &sz);

  if (NULL == pTableMeta) {
    return TSDB_CODE_SUCCESS;
  }

  *tbType = pTableMeta->tableType;
  
  return TSDB_CODE_SUCCESS;
}


void ctgGenEpSet(SEpSet *epSet, SVgroupInfo *vgroupInfo) {
  epSet->inUse = 0;
  epSet->numOfEps = vgroupInfo->numOfEps;

  for (int32_t i = 0; i < vgroupInfo->numOfEps; ++i) {
    memcpy(&epSet->port[i], &vgroupInfo->epAddr[i].port, sizeof(epSet->port[i]));
    memcpy(&epSet->fqdn[i], &vgroupInfo->epAddr[i].fqdn, sizeof(epSet->fqdn[i]));
  }
}

int32_t ctgGetTableMetaFromMnodeImpl(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, char* tbFullName, STableMetaOutput* output) {
  SBuildTableMetaInput bInput = {.vgId = 0, .dbName = NULL, .tableFullName = tbFullName};
  char *msg = NULL;
  SEpSet *pVnodeEpSet = NULL;
  int32_t msgLen = 0;

  CTG_ERR_RET(queryBuildMsg[TMSG_INDEX(TDMT_MND_STB_META)](&bInput, &msg, 0, &msgLen));

  SRpcMsg rpcMsg = {
      .msgType = TDMT_MND_STB_META,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};

  rpcSendRecv(pRpc, (SEpSet*)pMgmtEps, &rpcMsg, &rpcRsp);
  
  if (TSDB_CODE_SUCCESS != rpcRsp.code) {
    if (CTG_TABLE_NOT_EXIST(rpcRsp.code)) {
      output->metaNum = 0;
      ctgDebug("tbmeta:%s not exist in mnode", tbFullName);
      return TSDB_CODE_SUCCESS;
    }
    
    ctgError("error rsp for table meta, code:%x", rpcRsp.code);
    CTG_ERR_RET(rpcRsp.code);
  }

  CTG_ERR_RET(queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_STB_META)](output, rpcRsp.pCont, rpcRsp.contLen));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTableMetaFromMnode(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const SName* pTableName, STableMetaOutput* output) {
  char tbFullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pTableName, tbFullName);

  return ctgGetTableMetaFromMnodeImpl(pCatalog, pRpc, pMgmtEps, tbFullName, output);
}


int32_t ctgGetTableMetaFromVnode(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const SName* pTableName, SVgroupInfo *vgroupInfo, STableMetaOutput* output) {
  if (NULL == pCatalog || NULL == pRpc || NULL == pMgmtEps || NULL == pTableName || NULL == vgroupInfo || NULL == output) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  char dbFullName[TSDB_DB_FNAME_LEN];
  tNameGetFullDbName(pTableName, dbFullName);

  SBuildTableMetaInput bInput = {.vgId = vgroupInfo->vgId, .dbName = dbFullName, .tableFullName = (char *)pTableName->tname};
  char *msg = NULL;
  SEpSet *pVnodeEpSet = NULL;
  int32_t msgLen = 0;

  CTG_ERR_RET(queryBuildMsg[TMSG_INDEX(TDMT_VND_TABLE_META)](&bInput, &msg, 0, &msgLen));

  SRpcMsg rpcMsg = {
      .msgType = TDMT_VND_TABLE_META,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  SEpSet  epSet;
  
  ctgGenEpSet(&epSet, vgroupInfo);
  rpcSendRecv(pRpc, &epSet, &rpcMsg, &rpcRsp);
  
  if (TSDB_CODE_SUCCESS != rpcRsp.code) {
    if (CTG_TABLE_NOT_EXIST(rpcRsp.code)) {
      output->metaNum = 0;
      ctgDebug("tbmeta:%s not exist in vnode", pTableName->tname);
      return TSDB_CODE_SUCCESS;
    }
  
    ctgError("error rsp for table meta, code:%x", rpcRsp.code);
    CTG_ERR_RET(rpcRsp.code);
  }

  CTG_ERR_RET(queryProcessMsgRsp[TMSG_INDEX(TDMT_VND_TABLE_META)](output, rpcRsp.pCont, rpcRsp.contLen));

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

  vgList = taosArrayInit(taosHashGetSize(dbInfo->vgInfo), sizeof(SVgroupInfo));
  if (NULL == vgList) {
    ctgError("taosArrayInit failed");
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);    
  }

  void *pIter = taosHashIterate(dbInfo->vgInfo, NULL);
  while (pIter) {
    vgInfo = pIter;

    if (NULL == taosArrayPush(vgList, vgInfo)) {
      ctgError("taosArrayPush failed");
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }
    
    pIter = taosHashIterate(dbInfo->vgInfo, pIter);
    vgInfo = NULL;
  }

  *vgroupList = vgList;
  vgList = NULL;

  return TSDB_CODE_SUCCESS;

_return:

  if (vgList) {
    taosArrayDestroy(vgList);
  }

  CTG_RET(code);
}

int32_t ctgGetVgInfoFromHashValue(SDBVgroupInfo *dbInfo, const SName *pTableName, SVgroupInfo *pVgroup) {
  int32_t code = 0;
  
  int32_t vgNum = taosHashGetSize(dbInfo->vgInfo);
  char db[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, db);

  if (vgNum <= 0) {
    ctgError("db[%s] vgroup cache invalid, vgroup number:%d", db, vgNum);
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
      break;
    }
    
    pIter = taosHashIterate(dbInfo->vgInfo, pIter);
    vgInfo = NULL;
  }

  if (NULL == vgInfo) {
    ctgError("no hash range found for hashvalue[%u]", hashValue);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  *pVgroup = *vgInfo;

_return:
  
  CTG_RET(TSDB_CODE_SUCCESS);
}

int32_t ctgGetTableMetaImpl(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const SName* pTableName, bool forceUpdate, STableMeta** pTableMeta, int32_t isSTable) {
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
    ctgError("get table meta from cache failed, but fetch succeed");
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }
  
  return TSDB_CODE_SUCCESS;
}


int32_t ctgUpdateTableMetaCache(struct SCatalog *pCatalog, STableMetaOutput *output) {
  int32_t code = 0;
  
  if (output->metaNum != 1 && output->metaNum != 2) {
    ctgError("invalid table meta number[%d] got from meta rsp", output->metaNum);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == output->tbMeta) {
    ctgError("no valid table meta got from meta rsp");
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == pCatalog->tableCache.cache) {
    SHashObj *cache = taosHashInit(ctgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (NULL == cache) {
      ctgError("init hash[%d] for tablemeta cache failed", ctgMgmt.cfg.maxTblCacheNum);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }

    if (NULL != atomic_val_compare_exchange_ptr(&pCatalog->tableCache.cache, NULL, cache)) {
      taosHashCleanup(cache);
    }
  }

  if (NULL == pCatalog->tableCache.stableCache) {
    SHashObj *cache = taosHashInit(ctgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), true, HASH_ENTRY_LOCK);
    if (NULL == cache) {
      ctgError("init hash[%d] for stablemeta cache failed", ctgMgmt.cfg.maxTblCacheNum);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }

    if (NULL != atomic_val_compare_exchange_ptr(&pCatalog->tableCache.stableCache, NULL, cache)) {
      taosHashCleanup(cache);
    }
  }

  if (output->metaNum == 2) {
    if (taosHashPut(pCatalog->tableCache.cache, output->ctbFname, strlen(output->ctbFname), &output->ctbMeta, sizeof(output->ctbMeta)) != 0) {
      ctgError("push ctable[%s] to table cache failed", output->ctbFname);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }

    if (TSDB_SUPER_TABLE != output->tbMeta->tableType) {
      ctgError("table type[%d] error, expected:%d", output->tbMeta->tableType, TSDB_SUPER_TABLE);
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }    
  }

  int32_t tbSize = sizeof(*output->tbMeta) + sizeof(SSchema) * (output->tbMeta->tableInfo.numOfColumns + output->tbMeta->tableInfo.numOfTags);

  if (TSDB_SUPER_TABLE == output->tbMeta->tableType) {
    CTG_LOCK(CTG_WRITE, &pCatalog->tableCache.stableLock);
    if (taosHashPut(pCatalog->tableCache.cache, output->tbFname, strlen(output->tbFname), output->tbMeta, tbSize) != 0) {
      CTG_UNLOCK(CTG_WRITE, &pCatalog->tableCache.stableLock);
      ctgError("push table[%s] to table cache failed", output->tbFname);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }

    STableMeta *tbMeta = taosHashGet(pCatalog->tableCache.cache, output->tbFname, strlen(output->tbFname));
    if (taosHashPut(pCatalog->tableCache.stableCache, &output->tbMeta->suid, sizeof(output->tbMeta->suid), &tbMeta, POINTER_BYTES) != 0) {
      CTG_UNLOCK(CTG_WRITE, &pCatalog->tableCache.stableLock);
      ctgError("push suid[%"PRIu64"] to stable cache failed", output->tbMeta->suid);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }
    CTG_UNLOCK(CTG_WRITE, &pCatalog->tableCache.stableLock);
  } else {
    if (taosHashPut(pCatalog->tableCache.cache, output->tbFname, strlen(output->tbFname), output->tbMeta, tbSize) != 0) {
      ctgError("push table[%s] to table cache failed", output->tbFname);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }
  }

  CTG_RET(code);
}


int32_t ctgGetDBVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* dbName, int32_t forceUpdate, SDBVgroupInfo** dbInfo) {
  bool inCache = false;
  if (0 == forceUpdate) {
    CTG_ERR_RET(ctgGetDBVgroupFromCache(pCatalog, dbName, dbInfo, &inCache));

    if (inCache) {
      return TSDB_CODE_SUCCESS;
    }
  }

  SUseDbOutput DbOut = {0};
  SBuildUseDBInput input = {0};

  strncpy(input.db, dbName, sizeof(input.db));
  input.db[sizeof(input.db) - 1] = 0;
  input.vgVersion = CTG_DEFAULT_INVALID_VERSION;

  while (true) {
    CTG_ERR_RET(ctgGetDBVgroupFromMnode(pCatalog, pRpc, pMgmtEps, &input, &DbOut));

    CTG_ERR_RET(catalogUpdateDBVgroup(pCatalog, dbName, &DbOut.dbVgroup));

    CTG_ERR_RET(ctgGetDBVgroupFromCache(pCatalog, dbName, dbInfo, &inCache));

    if (!inCache) {
      ctgWarn("get db vgroup from cache failed, db:%s", dbName);
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
      ctgInfo("dbName:%s vg will not update, vgVersion:%d , current:%d", dbName, dbInfo->vgVersion, oldInfo->vgVersion);
      CTG_UNLOCK(CTG_WRITE, &oldInfo->lock);
      taosHashRelease(pCatalog->dbCache.cache, oldInfo);
      
      return TSDB_CODE_SUCCESS;
    }
    
    if (oldInfo->vgInfo) {
      ctgInfo("dbName:%s vg will be cleanup", dbName);
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
    CTG_ERR_JRET(ctgGetTableMetaFromMnode(pCatalog, pTransporter, pMgmtEps, pTableName, &moutput));

    if (0 == moutput.metaNum) {
      CTG_ERR_JRET(ctgGetTableMetaFromVnode(pCatalog, pTransporter, pMgmtEps, pTableName, &vgroupInfo, &voutput));
    } else {
      output = &moutput;
    }
  } else {
    CTG_ERR_JRET(ctgGetTableMetaFromVnode(pCatalog, pTransporter, pMgmtEps, pTableName, &vgroupInfo, &voutput));

    if (voutput.metaNum > 0 && TSDB_SUPER_TABLE == voutput.tbMeta->tableType) {
      CTG_ERR_JRET(ctgGetTableMetaFromMnodeImpl(pCatalog, pTransporter, pMgmtEps, voutput.tbFname, &moutput));

      tfree(voutput.tbMeta);
      voutput.tbMeta = moutput.tbMeta;
      moutput.tbMeta = NULL;
    }
  }

  CTG_ERR_JRET(ctgUpdateTableMetaCache(pCatalog, output));

_return:

  tfree(voutput.tbMeta);
  tfree(moutput.tbMeta);
  
  CTG_RET(code);
}


int32_t catalogInit(SCatalogCfg *cfg) {
  if (ctgMgmt.pCluster) {
    ctgError("catalog already init");
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
  } else {
    ctgMgmt.cfg.maxDBCacheNum = CTG_DEFAULT_CACHE_DB_NUMBER;
    ctgMgmt.cfg.maxTblCacheNum = CTG_DEFAULT_CACHE_TABLEMETA_NUMBER;
  }

  ctgMgmt.pCluster = taosHashInit(CTG_DEFAULT_CACHE_CLUSTER_NUMBER, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  if (NULL == ctgMgmt.pCluster) {
    CTG_ERR_LRET(TSDB_CODE_CTG_INTERNAL_ERROR, "init %d cluster cache failed", CTG_DEFAULT_CACHE_CLUSTER_NUMBER);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t catalogGetHandle(uint64_t clusterId, struct SCatalog** catalogHandle) {
  if (NULL == catalogHandle) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (NULL == ctgMgmt.pCluster) {
    ctgError("cluster cache are not ready");
    CTG_ERR_RET(TSDB_CODE_CTG_NOT_READY);
  }

  SCatalog **ctg = (SCatalog **)taosHashGet(ctgMgmt.pCluster, (char*)&clusterId, sizeof(clusterId));

  if (ctg && (*ctg)) {
    *catalogHandle = *ctg;
    return TSDB_CODE_SUCCESS;
  }

  SCatalog *clusterCtg = calloc(1, sizeof(SCatalog));
  if (NULL == clusterCtg) {
    ctgError("calloc %d failed", (int32_t)sizeof(SCatalog));
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  if (taosHashPut(ctgMgmt.pCluster, &clusterId, sizeof(clusterId), &clusterCtg, POINTER_BYTES)) {
    ctgError("put cluster %"PRIx64" cache to hash failed", clusterId);
    tfree(clusterCtg);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  *catalogHandle = clusterCtg;
  
  return TSDB_CODE_SUCCESS;
}

int32_t catalogGetDBVgroupVersion(struct SCatalog* pCatalog, const char* dbName, int32_t* version) {
  if (NULL == pCatalog || NULL == dbName || NULL == version) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (NULL == pCatalog->dbCache.cache) {
    *version = CTG_DEFAULT_INVALID_VERSION;
    return TSDB_CODE_SUCCESS;
  }

  SDBVgroupInfo * dbInfo = taosHashAcquire(pCatalog->dbCache.cache, dbName, strlen(dbName));
  if (NULL == dbInfo) {
    *version = CTG_DEFAULT_INVALID_VERSION;
    return TSDB_CODE_SUCCESS;
  }

  *version = dbInfo->vgVersion;
  taosHashRelease(pCatalog->dbCache.cache, dbInfo);

  return TSDB_CODE_SUCCESS;
}

int32_t catalogGetDBVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* dbName, int32_t forceUpdate, SArray** vgroupList) {
  if (NULL == pCatalog || NULL == dbName || NULL == pRpc || NULL == pMgmtEps || NULL == vgroupList) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SDBVgroupInfo* db = NULL;
  int32_t code = 0;
  SVgroupInfo *vgInfo = NULL;
  SArray *vgList = NULL;
  
  CTG_ERR_JRET(ctgGetDBVgroup(pCatalog, pRpc, pMgmtEps, dbName, forceUpdate, &db));

  vgList = taosArrayInit(taosHashGetSize(db->vgInfo), sizeof(SVgroupInfo));
  if (NULL == vgList) {
    ctgError("taosArrayInit failed");
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);    
  }

  void *pIter = taosHashIterate(db->vgInfo, NULL);
  while (pIter) {
    vgInfo = pIter;

    if (NULL == taosArrayPush(vgList, vgInfo)) {
      ctgError("taosArrayPush failed");
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
    ctgError("invalid db vg, dbName:%s", dbName);
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
  }

  if (dbInfo->vgVersion < 0) {
    ctgWarn("invalid db vgVersion:%d, dbName:%s", dbInfo->vgVersion, dbName);

    if (pCatalog->dbCache.cache) {
      CTG_ERR_JRET(ctgValidateAndRemoveDb(pCatalog, dbName, dbInfo));
      
      CTG_ERR_JRET(taosHashRemove(pCatalog->dbCache.cache, dbName, strlen(dbName)));
    }
    
    ctgWarn("remove db [%s] from cache", dbName);
    goto _return;
  }

  if (NULL == pCatalog->dbCache.cache) {
    SHashObj *cache = taosHashInit(ctgMgmt.cfg.maxDBCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (NULL == cache) {
      ctgError("init hash[%d] for db cache failed", CTG_DEFAULT_CACHE_DB_NUMBER);
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
    }

    if (NULL != atomic_val_compare_exchange_ptr(&pCatalog->dbCache.cache, NULL, cache)) {
      taosHashCleanup(cache);
    }
  } else {
    CTG_ERR_JRET(ctgValidateAndRemoveDb(pCatalog, dbName, dbInfo));
  }

  if (taosHashPut(pCatalog->dbCache.cache, dbName, strlen(dbName), dbInfo, sizeof(*dbInfo)) != 0) {
    ctgError("push to vgroup hash cache failed");
    CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);
  }

  ctgDebug("dbName:%s vgroup updated, vgVersion:%d", dbName, dbInfo->vgVersion);

  dbInfo->vgInfo = NULL;

_return:

  if (dbInfo && dbInfo->vgInfo) {
    taosHashCleanup(dbInfo->vgInfo);
    dbInfo->vgInfo = NULL;
  }
  
  CTG_RET(code);
}

int32_t catalogGetTableMeta(struct SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, STableMeta** pTableMeta) {
  return ctgGetTableMetaImpl(pCatalog, pTransporter, pMgmtEps, pTableName, false, pTableMeta, -1);
}

int32_t catalogGetSTableMeta(struct SCatalog* pCatalog, void * pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, STableMeta** pTableMeta) {
  return ctgGetTableMetaImpl(pCatalog, pTransporter, pMgmtEps, pTableName, false, pTableMeta, 1);
}

int32_t catalogRenewTableMeta(struct SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, int32_t isSTable) {
  if (NULL == pCatalog || NULL == pTransporter || NULL == pMgmtEps || NULL == pTableName) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  return ctgRenewTableMetaImpl(pCatalog, pTransporter, pMgmtEps, pTableName, isSTable);
}

int32_t catalogRenewAndGetTableMeta(struct SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const SName* pTableName, STableMeta** pTableMeta, int32_t isSTable) {
  return ctgGetTableMetaImpl(pCatalog, pTransporter, pMgmtEps, pTableName, true, pTableMeta, isSTable);
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
  
  CTG_ERR_JRET(catalogGetTableMeta(pCatalog, pRpc, pMgmtEps, pTableName, &tbMeta));

  char db[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, db);
  CTG_ERR_JRET(ctgGetDBVgroup(pCatalog, pRpc, pMgmtEps, db, false, &dbVgroup));

  if (tbMeta->tableType == TSDB_SUPER_TABLE) {
    CTG_ERR_JRET(ctgGetVgInfoFromDB(pCatalog, pRpc, pMgmtEps, dbVgroup, pVgroupList));
  } else {
    int32_t vgId = tbMeta->vgId;
    if (NULL == taosHashGetClone(dbVgroup->vgInfo, &vgId, sizeof(vgId), &vgroupInfo)) {
      ctgError("vgId[%d] not found in vgroup list", vgId);
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);    
    }

    vgList = taosArrayInit(1, sizeof(SVgroupInfo));
    if (NULL == vgList) {
      ctgError("taosArrayInit failed");
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);    
    }

    if (NULL == taosArrayPush(vgList, &vgroupInfo)) {
      ctgError("push vgroupInfo to array failed");
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

  CTG_ERR_JRET(ctgGetVgInfoFromHashValue(dbInfo, pTableName, pVgroup));

_return:

  if (dbInfo) {
    CTG_UNLOCK(CTG_READ, &dbInfo->lock);  
    taosHashRelease(pCatalog->dbCache.cache, dbInfo);
  }

  CTG_RET(code);
}


int32_t catalogGetAllMeta(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const SCatalogReq* pReq, SMetaData* pRsp) {
  if (NULL == pCatalog || NULL == pRpc  || NULL == pMgmtEps || NULL == pReq || NULL == pRsp) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;

  if (pReq->pTableName) {
    int32_t tbNum = (int32_t)taosArrayGetSize(pReq->pTableName);
    if (tbNum <= 0) {
      ctgError("empty table name list");
      CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
    }

    pRsp->pTableMeta = taosArrayInit(tbNum, POINTER_BYTES);
    if (NULL == pRsp->pTableMeta) {
      ctgError("taosArrayInit num[%d] failed", tbNum);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }
    
    for (int32_t i = 0; i < tbNum; ++i) {
      SName *name = taosArrayGet(pReq->pTableName, i);
      STableMeta *pTableMeta = NULL;
      
      CTG_ERR_JRET(catalogGetTableMeta(pCatalog, pRpc, pMgmtEps, name, &pTableMeta));

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


  return TSDB_CODE_SUCCESS;
}


void catalogDestroy(void) {
  if (ctgMgmt.pCluster) {
    taosHashCleanup(ctgMgmt.pCluster); //TBD
    ctgMgmt.pCluster = NULL;
  }
}



