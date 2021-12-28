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
#include "trpc.h"
#include "query.h"
#include "tname.h"

SCatalogMgmt ctgMgmt = {0};

int32_t ctgGetDBVgroupFromCache(struct SCatalog* pCatalog, const char *dbName, SDBVgroupInfo *dbInfo, int32_t *exist) {
  if (NULL == pCatalog->dbCache.cache) {
    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }

  SDBVgroupInfo *info = taosHashGet(pCatalog->dbCache.cache, dbName, strlen(dbName));

  if (NULL == info) {
    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }

  if (dbInfo) {
    *dbInfo = *info;
  }

  *exist = 1;
  
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


int32_t ctgGetTableMetaFromCache(struct SCatalog* pCatalog, const char *dbName, const char* pTableName, STableMeta** pTableMeta, int32_t *exist) {
  if (NULL == pCatalog->tableCache.cache) {
    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }

  char tbFullName[TSDB_TABLE_FNAME_LEN];

  snprintf(tbFullName, sizeof(tbFullName), "%s.%s", dbName, pTableName);

  STableMeta *tbMeta = taosHashGet(pCatalog->tableCache.cache, tbFullName, strlen(tbFullName));

  if (NULL == tbMeta) {
    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }

  if (tbMeta->tableType == TSDB_CHILD_TABLE) {
    STableMeta **stbMeta = taosHashGet(pCatalog->tableCache.stableCache, &tbMeta->suid, sizeof(tbMeta->suid));
    if (NULL == stbMeta || NULL == *stbMeta) {
      *exist = 0;
      return TSDB_CODE_SUCCESS;
    }

    if ((*stbMeta)->suid != tbMeta->suid) {
      ctgError("stable cache error, expected suid:%"PRId64 ",actual suid:%"PRId64, tbMeta->suid, (*stbMeta)->suid);
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    int32_t metaSize = sizeof(STableMeta) + ((*stbMeta)->tableInfo.numOfTags + (*stbMeta)->tableInfo.numOfColumns) * sizeof(SSchema);
    *pTableMeta = calloc(1, metaSize);
    if (NULL == *pTableMeta) {
      ctgError("calloc size[%d] failed", metaSize);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }

    memcpy(*pTableMeta, tbMeta, sizeof(SCTableMeta));
    memcpy(&(*pTableMeta)->sversion, &(*stbMeta)->sversion, metaSize - sizeof(SCTableMeta));
  } else {
    int32_t metaSize = sizeof(STableMeta) + (tbMeta->tableInfo.numOfTags + tbMeta->tableInfo.numOfColumns) * sizeof(SSchema);
    *pTableMeta = calloc(1, metaSize);
    if (NULL == *pTableMeta) {
      ctgError("calloc size[%d] failed", metaSize);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }

    memcpy(*pTableMeta, tbMeta, metaSize);
  }

  *exist = 1;
  
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

int32_t ctgGetTableMetaFromMnode(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char *pDBName, const char* pTableName, STableMetaOutput* output) {
  if (NULL == pCatalog || NULL == pRpc || NULL == pMgmtEps || NULL == pDBName || NULL == pTableName || NULL == output) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  char tbFullName[TSDB_TABLE_FNAME_LEN];

  snprintf(tbFullName, sizeof(tbFullName), "%s.%s", pDBName, pTableName);

  SBuildTableMetaInput bInput = {.vgId = 0, .tableFullName = tbFullName};
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
    ctgError("error rsp for table meta, code:%x", rpcRsp.code);
    CTG_ERR_RET(rpcRsp.code);
  }

  CTG_ERR_RET(queryProcessMsgRsp[TMSG_INDEX(TDMT_MND_STB_META)](output, rpcRsp.pCont, rpcRsp.contLen));

  return TSDB_CODE_SUCCESS;
}


int32_t ctgGetTableMetaFromVnode(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char *pDBName, const char* pTableName, SVgroupInfo *vgroupInfo, STableMetaOutput* output) {
  if (NULL == pCatalog || NULL == pRpc || NULL == pMgmtEps || NULL == pDBName || NULL == pTableName || NULL == vgroupInfo || NULL == output) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  char tbFullName[TSDB_TABLE_FNAME_LEN];

  snprintf(tbFullName, sizeof(tbFullName), "%s.%s", pDBName, pTableName);

  SBuildTableMetaInput bInput = {.vgId = vgroupInfo->vgId, .tableFullName = tbFullName};
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

  *vgroupList = taosArrayInit(taosHashGetSize(dbInfo->vgInfo), sizeof(SVgroupInfo));
  if (NULL == *vgroupList) {
    ctgError("taosArrayInit failed");
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);    
  }

  void *pIter = taosHashIterate(dbInfo->vgInfo, NULL);
  while (pIter) {
    vgInfo = pIter;

    if (NULL == taosArrayPush(*vgroupList, vgInfo)) {
      ctgError("taosArrayPush failed");
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }
    
    pIter = taosHashIterate(dbInfo->vgInfo, pIter);
    vgInfo = NULL;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetVgInfoFromHashValue(SDBVgroupInfo *dbInfo, const char *pDBName, const char *pTableName, SVgroupInfo *pVgroup) {
  int32_t vgNum = taosHashGetSize(dbInfo->vgInfo);
  if (vgNum <= 0) {
    ctgError("db[%s] vgroup cache invalid, vgroup number:%d", pDBName, vgNum);
    CTG_ERR_RET(TSDB_CODE_TSC_DB_NOT_SELECTED);
  }

  tableNameHashFp fp = NULL;
  SVgroupInfo *vgInfo = NULL;

  CTG_ERR_RET(ctgGetHashFunction(dbInfo->hashMethod, &fp));

  char tbFullName[TSDB_TABLE_FNAME_LEN];

  snprintf(tbFullName, sizeof(tbFullName), "%s.%s", pDBName, pTableName);

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
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  *pVgroup = *vgInfo;

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTableMetaImpl(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pDBName, const char* pTableName, bool forceUpdate, STableMeta** pTableMeta) {
  if (NULL == pCatalog || NULL == pDBName || NULL == pRpc || NULL == pMgmtEps || NULL == pTableName || NULL == pTableMeta) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t exist = 0;

  if (!forceUpdate) {  
    CTG_ERR_RET(ctgGetTableMetaFromCache(pCatalog, pDBName, pTableName, pTableMeta, &exist));

    if (exist) {
      return TSDB_CODE_SUCCESS;
    }
  }

  CTG_ERR_RET(catalogRenewTableMeta(pCatalog, pRpc, pMgmtEps, pDBName, pTableName));

  CTG_ERR_RET(ctgGetTableMetaFromCache(pCatalog, pDBName, pTableName, pTableMeta, &exist));

  if (0 == exist) {
    ctgError("get table meta from cache failed, but fetch succeed");
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }
  
  return TSDB_CODE_SUCCESS;
}


int32_t ctgUpdateTableMetaCache(struct SCatalog *pCatalog, STableMetaOutput *output) {
  if (output->metaNum != 1 && output->metaNum != 2) {
    ctgError("invalid table meta number[%d] got from meta rsp", output->metaNum);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == output->tbMeta) {
    ctgError("no valid table meta got from meta rsp");
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == pCatalog->tableCache.cache) {
    pCatalog->tableCache.cache = taosHashInit(ctgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (NULL == pCatalog->tableCache.cache) {
      ctgError("init hash[%d] for tablemeta cache failed", ctgMgmt.cfg.maxTblCacheNum);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }
  }

  if (NULL == pCatalog->tableCache.stableCache) {
    pCatalog->tableCache.stableCache = taosHashInit(ctgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), true, HASH_ENTRY_LOCK);
    if (NULL == pCatalog->tableCache.stableCache) {
      ctgError("init hash[%d] for stablemeta cache failed", ctgMgmt.cfg.maxTblCacheNum);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
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
  if (taosHashPut(pCatalog->tableCache.cache, output->tbFname, strlen(output->tbFname), output->tbMeta, tbSize) != 0) {
    ctgError("push table[%s] to table cache failed", output->tbFname);
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  if (TSDB_SUPER_TABLE == output->tbMeta->tableType) {
    if (taosHashPut(pCatalog->tableCache.stableCache, &output->tbMeta->suid, sizeof(output->tbMeta->suid), &output->tbMeta, POINTER_BYTES) != 0) {
      ctgError("push suid[%"PRIu64"] to stable cache failed", output->tbMeta->suid);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }
  }
  
  return TSDB_CODE_SUCCESS;
}

int32_t catalogInit(SCatalogCfg *cfg) {
  if (ctgMgmt.pCluster) {
    ctgError("catalog already init");
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (cfg) {
    memcpy(&ctgMgmt.cfg, cfg, sizeof(*cfg));
  } else {
    ctgMgmt.cfg.maxDBCacheNum = CTG_DEFAULT_CACHE_DB_NUMBER;
    ctgMgmt.cfg.maxTblCacheNum = CTG_DEFAULT_CACHE_TABLEMETA_NUMBER;
  }

  if (CTG_CACHE_ENABLED()) {
    ctgMgmt.pCluster = taosHashInit(CTG_DEFAULT_CACHE_CLUSTER_NUMBER, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (NULL == ctgMgmt.pCluster) {
      CTG_ERR_LRET(TSDB_CODE_CTG_INTERNAL_ERROR, "init %d cluster cache failed", CTG_DEFAULT_CACHE_CLUSTER_NUMBER);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t catalogGetHandle(const char* clusterId , struct SCatalog** catalogHandle) {
  if (NULL == clusterId || NULL == catalogHandle) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (NULL == ctgMgmt.pCluster) {
    ctgError("cluster cache are not ready");
    CTG_ERR_RET(TSDB_CODE_CTG_NOT_READY);
  }

  size_t clen = strlen(clusterId);
  SCatalog **ctg = (SCatalog **)taosHashGet(ctgMgmt.pCluster, clusterId, clen);

  if (ctg && (*ctg)) {
    *catalogHandle = *ctg;
    return TSDB_CODE_SUCCESS;
  }

  SCatalog *clusterCtg = calloc(1, sizeof(SCatalog));
  if (NULL == clusterCtg) {
    ctgError("calloc %d failed", (int32_t)sizeof(SCatalog));
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  if (taosHashPut(ctgMgmt.pCluster, clusterId, clen, &clusterCtg, POINTER_BYTES)) {
    ctgError("put cluster %s cache to hash failed", clusterId);
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

  SDBVgroupInfo * dbInfo = taosHashGet(pCatalog->dbCache.cache, dbName, strlen(dbName));
  if (NULL == dbInfo) {
    *version = CTG_DEFAULT_INVALID_VERSION;
    return TSDB_CODE_SUCCESS;
  }

  *version = dbInfo->vgVersion;

  return TSDB_CODE_SUCCESS;
}

int32_t catalogUpdateDBVgroupCache(struct SCatalog* pCatalog, const char* dbName, SDBVgroupInfo* dbInfo) {
  if (NULL == pCatalog || NULL == dbName || NULL == dbInfo) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (dbInfo->vgVersion < 0) {
    if (pCatalog->dbCache.cache) {
      taosHashRemove(pCatalog->dbCache.cache, dbName, strlen(dbName));
    }
    
    ctgWarn("remove db [%s] from cache", dbName);
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == pCatalog->dbCache.cache) {
    pCatalog->dbCache.cache = taosHashInit(ctgMgmt.cfg.maxDBCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (NULL == pCatalog->dbCache.cache) {
      ctgError("init hash[%d] for db cache failed", CTG_DEFAULT_CACHE_DB_NUMBER);
      CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
    }
  } else {
    SDBVgroupInfo *oldInfo = taosHashGet(pCatalog->dbCache.cache, dbName, strlen(dbName));
    if (oldInfo && oldInfo->vgInfo) {
      taosHashCleanup(oldInfo->vgInfo);
      oldInfo->vgInfo = NULL;
    }
  }

  if (taosHashPut(pCatalog->dbCache.cache, dbName, strlen(dbName), dbInfo, sizeof(*dbInfo)) != 0) {
    ctgError("push to vgroup hash cache failed");
    CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
  }

  return TSDB_CODE_SUCCESS;
}




int32_t catalogGetDBVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* dbName, int32_t forceUpdate, SDBVgroupInfo* dbInfo) {
  if (NULL == pCatalog || NULL == dbName || NULL == pRpc || NULL == pMgmtEps) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t exist = 0;

  if (0 == forceUpdate) {
    CTG_ERR_RET(ctgGetDBVgroupFromCache(pCatalog, dbName, dbInfo, &exist));

    if (exist) {
      return TSDB_CODE_SUCCESS;
    }
  }

  SUseDbOutput DbOut = {0};
  SBuildUseDBInput input = {0};

  strncpy(input.db, dbName, sizeof(input.db));
  input.db[sizeof(input.db) - 1] = 0;
  input.vgVersion = CTG_DEFAULT_INVALID_VERSION;
  
  CTG_ERR_RET(ctgGetDBVgroupFromMnode(pCatalog, pRpc, pMgmtEps, &input, &DbOut));

  CTG_ERR_RET(catalogUpdateDBVgroupCache(pCatalog, dbName, &DbOut.dbVgroup));

  if (dbInfo) {
    *dbInfo = DbOut.dbVgroup;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t catalogGetTableMeta(struct SCatalog* pCatalog, void *pTransporter, const SEpSet* pMgmtEps, const char* pDBName, const char* pTableName, STableMeta** pTableMeta) {
  return ctgGetTableMetaImpl(pCatalog, pTransporter, pMgmtEps, pDBName, pTableName, false, pTableMeta);
}

int32_t catalogRenewTableMeta(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pDBName, const char* pTableName) {
  if (NULL == pCatalog || NULL == pDBName || NULL == pRpc || NULL == pMgmtEps || NULL == pTableName) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SVgroupInfo vgroupInfo = {0};
  
  CTG_ERR_RET(catalogGetTableHashVgroup(pCatalog, pRpc, pMgmtEps, pDBName, pTableName, &vgroupInfo));

  STableMetaOutput output = {0};
  
  //CTG_ERR_RET(ctgGetTableMetaFromVnode(pCatalog, pRpc, pMgmtEps, pDBName, pTableName, &vgroupInfo, &output));

  CTG_ERR_RET(ctgGetTableMetaFromMnode(pCatalog, pRpc, pMgmtEps, pDBName, pTableName, &output));

  CTG_ERR_RET(ctgUpdateTableMetaCache(pCatalog, &output));

  tfree(output.tbMeta);
  
  return TSDB_CODE_SUCCESS;
}

int32_t catalogRenewAndGetTableMeta(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pDBName, const char* pTableName, STableMeta** pTableMeta) {
  return ctgGetTableMetaImpl(pCatalog, pRpc, pMgmtEps, pDBName, pTableName, true, pTableMeta);
}

int32_t catalogGetTableDistVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pDBName, const char* pTableName, SArray** pVgroupList) {
  if (NULL == pCatalog || NULL == pRpc || NULL == pMgmtEps || NULL == pDBName || NULL == pTableName || NULL == pVgroupList) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }
  
  STableMeta *tbMeta = NULL;
  int32_t code = 0;
  SVgroupInfo vgroupInfo = {0};
  SDBVgroupInfo dbVgroup = {0};
  
  CTG_ERR_JRET(catalogGetTableMeta(pCatalog, pRpc, pMgmtEps, pDBName, pTableName, &tbMeta));

  CTG_ERR_JRET(catalogGetDBVgroup(pCatalog, pRpc, pMgmtEps, pDBName, false, &dbVgroup));

  if (tbMeta->tableType == TSDB_SUPER_TABLE) {
    CTG_ERR_JRET(ctgGetVgInfoFromDB(pCatalog, pRpc, pMgmtEps, &dbVgroup, pVgroupList));
  } else {
    int32_t vgId = tbMeta->vgId;
    if (NULL == taosHashGetClone(dbVgroup.vgInfo, &vgId, sizeof(vgId), &vgroupInfo)) {
      ctgError("vgId[%d] not found in vgroup list", vgId);
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);    
    }

    *pVgroupList = taosArrayInit(1, sizeof(SVgroupInfo));
    if (NULL == *pVgroupList) {
      ctgError("taosArrayInit failed");
      CTG_ERR_JRET(TSDB_CODE_CTG_MEM_ERROR);    
    }

    if (NULL == taosArrayPush(*pVgroupList, &vgroupInfo)) {
      ctgError("push vgroupInfo to array failed");
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }
  }

  tfree(tbMeta);

  return TSDB_CODE_SUCCESS;

_return:
  tfree(tbMeta);

  taosArrayDestroy(*pVgroupList);
  
  CTG_RET(code);
}


int32_t catalogGetTableHashVgroup(struct SCatalog *pCatalog, void *pTransporter, const SEpSet *pMgmtEps, const char *pDBName, const char *pTableName, SVgroupInfo *pVgroup) {
  SDBVgroupInfo dbInfo = {0};
  int32_t code = 0;
  int32_t vgId = 0;

  CTG_ERR_RET(catalogGetDBVgroup(pCatalog, pTransporter, pMgmtEps, pDBName, false, &dbInfo));

  if (dbInfo.vgVersion < 0 || NULL == dbInfo.vgInfo) {
    ctgError("db[%s] vgroup cache invalid, vgroup version:%d, vgInfo:%p", pDBName, dbInfo.vgVersion, dbInfo.vgInfo);
    CTG_ERR_RET(TSDB_CODE_TSC_DB_NOT_SELECTED);
  }

  CTG_ERR_RET(ctgGetVgInfoFromHashValue(&dbInfo, pDBName, pTableName, pVgroup));

  CTG_RET(code);
}


int32_t catalogGetAllMeta(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const SCatalogReq* pReq, SMetaData* pRsp) {
  if (NULL == pCatalog || NULL == pRpc  || NULL == pMgmtEps || NULL == pReq || NULL == pRsp) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;

  if (pReq->pTableName) {
    char dbName[TSDB_DB_FNAME_LEN];
    int32_t tbNum = (int32_t)taosArrayGetSize(pReq->pTableName);
    if (tbNum > 0) {
      pRsp->pTableMeta = taosArrayInit(tbNum, POINTER_BYTES);
      if (NULL == pRsp->pTableMeta) {
        ctgError("taosArrayInit num[%d] failed", tbNum);
        CTG_ERR_RET(TSDB_CODE_CTG_MEM_ERROR);
      }
    }
    
    for (int32_t i = 0; i < tbNum; ++i) {
      SName *name = taosArrayGet(pReq->pTableName, i);
      STableMeta *pTableMeta = NULL;
      
      snprintf(dbName, sizeof(dbName), "%d.%s", name->acctId, name->dbname);

      CTG_ERR_JRET(catalogGetTableMeta(pCatalog, pRpc, pMgmtEps, dbName, name->tname, &pTableMeta));

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



