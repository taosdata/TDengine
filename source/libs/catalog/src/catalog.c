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

SCatalogMgmt ctgMgmt = {0};

int32_t ctgGetVgroupFromMnode(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, SVgroupListInfo** pVgroup) {
  char *msg = NULL;
  SEpSet *pVnodeEpSet = NULL;
  int32_t msgLen = 0;

  int32_t code = queryBuildMsg[TSDB_MSG_TYPE_VGROUP_LIST](NULL, &msg, 0, &msgLen);
  if (code) {
    return code;
  }

  SRpcMsg rpcMsg = {
      .msgType = TSDB_MSG_TYPE_VGROUP_LIST,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};

  rpcSendRecv(pRpc, (SEpSet*)pMgmtEps, &rpcMsg, &rpcRsp);

  code = queryProcessMsgRsp[TSDB_MSG_TYPE_VGROUP_LIST](pVgroup, rpcRsp.pCont, rpcRsp.contLen);
  if (code) {
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetVgroupFromCache(SCatalog* pCatalog, SHashObj** pVgroupList, int32_t* exist) {
  if (NULL == pCatalog->vgroupCache.cache || pCatalog->vgroupCache.vgroupVersion < 0) {
    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }

  if (pVgroupList) {
    *pVgroupList = pCatalog->vgroupCache.cache;
  }

  *exist = 1;
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetDBVgroupFromCache(SCatalog* pCatalog, const char *dbName, SDBVgroupInfo **dbInfo, int32_t *exist) {
  if (NULL == pCatalog->dbCache.cache) {
    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }

  SDBVgroupInfo *info = taosHashGet(pCatalog->dbCache.cache, dbName, strlen(dbName));

  if (NULL == info || info->vgroupVersion < pCatalog->vgroupCache.vgroupVersion) {
    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }

  if (dbInfo) {
    *dbInfo = calloc(1, sizeof(**dbInfo));
    if (NULL == *dbInfo) {
      ctgError("calloc size[%d] failed", (int32_t)sizeof(**dbInfo));
      return TSDB_CODE_CTG_MEM_ERROR;
    }
    
    (*dbInfo)->vgId = taosArrayDup(info->vgId);
    if (NULL == (*dbInfo)->vgId) {
      ctgError("taos array duplicate failed");
      tfree(*dbInfo);
      return TSDB_CODE_CTG_MEM_ERROR;
    }

    (*dbInfo)->vgroupVersion = info->vgroupVersion;
    (*dbInfo)->hashRange = info->hashRange;
    (*dbInfo)->hashType = info->hashType;
  }

  *exist = 1;
  
  return TSDB_CODE_SUCCESS;
}



int32_t ctgGetDBVgroupFromMnode(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, SBuildUseDBInput *input, SUseDbOutput *out) {
  char *msg = NULL;
  SEpSet *pVnodeEpSet = NULL;
  int32_t msgLen = 0;

  int32_t code = queryBuildMsg[TSDB_MSG_TYPE_USE_DB](input, &msg, 0, &msgLen);
  if (code) {
    return code;
  }

  SRpcMsg rpcMsg = {
      .msgType = TSDB_MSG_TYPE_USE_DB,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};

  rpcSendRecv(pRpc, (SEpSet*)pMgmtEps, &rpcMsg, &rpcRsp);

  code = queryProcessMsgRsp[TSDB_MSG_TYPE_USE_DB](out, rpcRsp.pCont, rpcRsp.contLen);
  if (code) {
    return code;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t ctgGetTableMetaFromCache(SCatalog* pCatalog, const char *dbName, const char* pTableName, STableMeta** pTableMeta, int32_t *exist) {
  if (NULL == pCatalog->tableCache.cache) {
    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }

  char tbFullName[TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + 1];

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
      return TSDB_CODE_CTG_INTERNAL_ERROR;
    }

    int32_t metaSize = sizeof(STableMeta) + ((*stbMeta)->tableInfo.numOfTags + (*stbMeta)->tableInfo.numOfColumns) * sizeof(SSchema);
    *pTableMeta = calloc(1, metaSize);
    if (NULL == *pTableMeta) {
      ctgError("calloc size[%d] failed", metaSize);
      return TSDB_CODE_CTG_MEM_ERROR;
    }

    memcpy(*pTableMeta, tbMeta, sizeof(SCTableMeta));
    memcpy(&(*pTableMeta)->sversion, &(*stbMeta)->sversion, metaSize - sizeof(SCTableMeta));
  } else {
    int32_t metaSize = sizeof(STableMeta) + (tbMeta->tableInfo.numOfTags + tbMeta->tableInfo.numOfColumns) * sizeof(SSchema);
    *pTableMeta = calloc(1, metaSize);
    if (NULL == *pTableMeta) {
      ctgError("calloc size[%d] failed", metaSize);
      return TSDB_CODE_CTG_MEM_ERROR;
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


int32_t ctgGetTableMetaFromMnode(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char *pDBName, const char* pTableName, SVgroupInfo *vgroupInfo, STableMetaOutput* output) {
  if (NULL == pCatalog || NULL == pRpc || NULL == pMgmtEps || NULL == pDBName || NULL == pTableName || NULL == vgroupInfo || NULL == output) {
    return TSDB_CODE_CTG_INVALID_INPUT;
  }

  char tbFullName[TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + 1];

  snprintf(tbFullName, sizeof(tbFullName), "%s.%s", pDBName, pTableName);

  SBuildTableMetaInput bInput = {.vgId = vgroupInfo->vgId, .tableFullName = tbFullName};
  char *msg = NULL;
  SEpSet *pVnodeEpSet = NULL;
  int32_t msgLen = 0;

  int32_t code = queryBuildMsg[TSDB_MSG_TYPE_TABLE_META](&bInput, &msg, 0, &msgLen);
  if (code) {
    return code;
  }

  SRpcMsg rpcMsg = {
      .msgType = TSDB_MSG_TYPE_TABLE_META,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  SEpSet  epSet;
  
  ctgGenEpSet(&epSet, vgroupInfo);

  rpcSendRecv(pRpc, &epSet, &rpcMsg, &rpcRsp);

  if (TSDB_CODE_SUCCESS != rpcRsp.code) {
    ctgError("get table meta from mnode failed, error code:%d", rpcRsp.code);
    return rpcRsp.code;
  }

  code = queryProcessMsgRsp[TSDB_MSG_TYPE_TABLE_META](output, rpcRsp.pCont, rpcRsp.contLen);
  if (code) {
    return code;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t ctgGetHashFunction(int32_t hashType, tableNameHashFp *fp) {
  switch (hashType) {
    default:
      *fp = MurmurHash3_32;
      break;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTableHashVgroup(SCatalog *pCatalog, void *pRpc, const SEpSet *pMgmtEps, const char *pDBName, const char *pTableName, SVgroupInfo *pVgroup) {
  SDBVgroupInfo *dbInfo = NULL;
  int32_t code = 0;

  CTG_ERR_RET(catalogGetDBVgroup(pCatalog, pRpc, pMgmtEps, pDBName, false, &dbInfo));

  if (NULL == dbInfo) {
    ctgWarn("db[%s] vgroup info not found", pDBName);
    return TSDB_CODE_TSC_DB_NOT_SELECTED;
  }

  if (dbInfo->vgroupVersion < 0 || NULL == dbInfo->vgId) {
    ctgError("db[%s] vgroup cache invalid, vgroup version:%d, vgId:%p", pDBName, dbInfo->vgroupVersion, dbInfo->vgId);
    CTG_ERR_JRET(TSDB_CODE_TSC_DB_NOT_SELECTED);
  }

  int32_t vgNum = taosArrayGetSize(dbInfo->vgId);
  if (vgNum <= 0) {
    ctgError("db[%s] vgroup cache invalid, vgroup number:%p", vgNum);
    CTG_ERR_JRET(TSDB_CODE_TSC_DB_NOT_SELECTED);
  }

  tableNameHashFp fp = NULL;

  CTG_ERR_JRET(ctgGetHashFunction(dbInfo->hashType, &fp));

  char tbFullName[TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + 1];

  snprintf(tbFullName, sizeof(tbFullName), "%s.%s", pDBName, pTableName);

  uint32_t hashValue = (*fp)(tbFullName, (uint32_t)strlen(tbFullName));
  uint32_t hashUnit = dbInfo->hashRange / vgNum;
  uint32_t vgId = hashValue / hashUnit;

  SHashObj *vgroupHash = NULL;
  
  CTG_ERR_JRET(catalogGetVgroup(pCatalog, pRpc, pMgmtEps, &vgroupHash));
  if (NULL == vgroupHash) {
    ctgError("get empty vgroup cache");
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);    
  }

  if (NULL == taosHashGetClone(vgroupHash, &vgId, sizeof(vgId), pVgroup)) {
    ctgError("vgId[%d] not found in vgroup list", vgId);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);    
  }

_return:
  if (dbInfo && dbInfo->vgId) {
    taosArrayDestroy(dbInfo->vgId);
    dbInfo->vgId = NULL;
  }
  
  tfree(dbInfo);

  return code;
}



STableMeta* ctgCreateSTableMeta(STableMetaMsg* pChild) {
  assert(pChild != NULL);
  int32_t total = pChild->numOfColumns + pChild->numOfTags;

  STableMeta* pTableMeta = calloc(1, sizeof(STableMeta) + sizeof(SSchema) * total);
  pTableMeta->tableType = TSDB_SUPER_TABLE;
  pTableMeta->tableInfo.numOfTags = pChild->numOfTags;
  pTableMeta->tableInfo.numOfColumns = pChild->numOfColumns;
  pTableMeta->tableInfo.precision = pChild->precision;

  pTableMeta->uid = pChild->suid;
  pTableMeta->tversion = pChild->tversion;
  pTableMeta->sversion = pChild->sversion;

  memcpy(pTableMeta->schema, pChild->pSchema, sizeof(SSchema) * total);

  int32_t num = pTableMeta->tableInfo.numOfColumns;
  for(int32_t i = 0; i < num; ++i) {
    pTableMeta->tableInfo.rowSize += pTableMeta->schema[i].bytes;
  }

  return pTableMeta;
}

int32_t ctgGetTableMetaImpl(SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pDBName, const char* pTableName, bool forceUpdate, STableMeta** pTableMeta) {
  if (NULL == pCatalog || NULL == pDBName || NULL == pRpc || NULL == pMgmtEps || NULL == pTableName || NULL == pTableMeta) {
    return TSDB_CODE_CTG_INVALID_INPUT;
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
    return TSDB_CODE_CTG_INTERNAL_ERROR;
  }
  
  return TSDB_CODE_SUCCESS;
}


int32_t ctgUpdateTableMetaCache(SCatalog *pCatalog, STableMetaOutput *output) {
  if (output->metaNum != 1 && output->metaNum != 2) {
    ctgError("invalid table meta number[%d] got from meta rsp", output->metaNum);
    return TSDB_CODE_CTG_INTERNAL_ERROR;
  }

  if (NULL == output->tbMeta) {
    ctgError("no valid table meta got from meta rsp");
    return TSDB_CODE_CTG_INTERNAL_ERROR;
  }

  if (NULL == pCatalog->tableCache.cache) {
    pCatalog->tableCache.cache = taosHashInit(ctgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (NULL == pCatalog->tableCache.cache) {
      ctgError("init hash[%d] for tablemeta cache failed", ctgMgmt.cfg.maxTblCacheNum);
      return TSDB_CODE_CTG_MEM_ERROR;
    }
  }

  if (NULL == pCatalog->tableCache.cache) {
    pCatalog->tableCache.cache = taosHashInit(ctgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (NULL == pCatalog->tableCache.cache) {
      ctgError("init hash[%d] for tablemeta cache failed", ctgMgmt.cfg.maxTblCacheNum);
      return TSDB_CODE_CTG_MEM_ERROR;
    }

    pCatalog->tableCache.stableCache = taosHashInit(ctgMgmt.cfg.maxTblCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), true, HASH_ENTRY_LOCK);
    if (NULL == pCatalog->tableCache.stableCache) {
      ctgError("init hash[%d] for stablemeta cache failed", ctgMgmt.cfg.maxTblCacheNum);
      return TSDB_CODE_CTG_MEM_ERROR;
    }
  }

  if (output->metaNum == 2) {
    if (taosHashPut(pCatalog->tableCache.cache, output->ctbFname, strlen(output->ctbFname), &output->ctbMeta, sizeof(output->ctbMeta)) != 0) {
      ctgError("push ctable[%s] to table cache failed", output->ctbFname);
      goto error_exit;
    }

    if (TSDB_SUPER_TABLE != output->tbMeta->tableType) {
      ctgError("table type[%d] error, expected:%d", output->tbMeta->tableType, TSDB_SUPER_TABLE);
      goto error_exit;
    }    
  }

  if (taosHashPut(pCatalog->tableCache.cache, output->tbFname, strlen(output->tbFname), output->tbMeta, sizeof(*output->tbMeta)) != 0) {
    ctgError("push table[%s] to table cache failed", output->tbFname);
    goto error_exit;
  }

  if (TSDB_SUPER_TABLE == output->tbMeta->tableType) {
    if (taosHashPut(pCatalog->tableCache.stableCache, &output->tbMeta->suid, sizeof(output->tbMeta->suid), &output->tbMeta, POINTER_BYTES) != 0) {
      ctgError("push suid[%"PRIu64"] to stable cache failed", output->tbMeta->suid);
      goto error_exit;
    }
  }
  
  return TSDB_CODE_SUCCESS;

error_exit:
  if (pCatalog->vgroupCache.cache) {
    taosHashCleanup(pCatalog->vgroupCache.cache);
    pCatalog->vgroupCache.cache = NULL;
  }

  pCatalog->vgroupCache.vgroupVersion = CTG_DEFAULT_INVALID_VERSION;

  return TSDB_CODE_CTG_INTERNAL_ERROR;
}


int32_t catalogInit(SCatalogCfg *cfg) {
  ctgMgmt.pCluster = taosHashInit(CTG_DEFAULT_CACHE_CLUSTER_NUMBER, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (NULL == ctgMgmt.pCluster) {
    CTG_ERR_LRET(TSDB_CODE_CTG_INTERNAL_ERROR, "init %d cluster cache failed", CTG_DEFAULT_CACHE_CLUSTER_NUMBER);
  }

  if (cfg) {
    memcpy(&ctgMgmt.cfg, cfg, sizeof(*cfg));
  } else {
    ctgMgmt.cfg.enableVgroupCache = true;
    ctgMgmt.cfg.maxDBCacheNum = CTG_DEFAULT_CACHE_DB_NUMBER;
    ctgMgmt.cfg.maxTblCacheNum = CTG_DEFAULT_CACHE_TABLEMETA_NUMBER;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t catalogGetHandle(const char *clusterId, SCatalog** catalogHandle) {
  if (NULL == clusterId || NULL == catalogHandle) {
    return TSDB_CODE_CTG_INVALID_INPUT;
  }

  if (NULL == ctgMgmt.pCluster) {
    ctgError("cluster cache are not ready");
    return TSDB_CODE_CTG_NOT_READY;
  }

  size_t clen = strlen(clusterId);
  SCatalog *clusterCtg = (SCatalog *)taosHashGet(ctgMgmt.pCluster, clusterId, clen);

  if (clusterCtg) {
    *catalogHandle = clusterCtg;
    return TSDB_CODE_SUCCESS;
  }

  clusterCtg = calloc(1, sizeof(*clusterCtg));
  if (NULL == clusterCtg) {
    ctgError("calloc %d failed", (int32_t)sizeof(*clusterCtg));
    return TSDB_CODE_CTG_MEM_ERROR;
  }

  clusterCtg->vgroupCache.vgroupVersion = CTG_DEFAULT_INVALID_VERSION;

  if (taosHashPut(ctgMgmt.pCluster, clusterId, clen, &clusterCtg, POINTER_BYTES)) {
    ctgError("put cluster %s cache to hash failed", clusterId);
    tfree(clusterCtg);
    return TSDB_CODE_CTG_INTERNAL_ERROR;
  }

  *catalogHandle = clusterCtg;
  
  return TSDB_CODE_SUCCESS;
}


int32_t catalogGetVgroupVersion(struct SCatalog* pCatalog, int32_t* version) {
  if (NULL == pCatalog || NULL == version) {
    return TSDB_CODE_CTG_INVALID_INPUT;
  }
  
  *version = pCatalog->vgroupCache.vgroupVersion;

  return TSDB_CODE_SUCCESS;
}



int32_t catalogUpdateVgroupCache(struct SCatalog* pCatalog, SVgroupListInfo* pVgroup) {
  if (NULL == pVgroup) {
    ctgError("no valid vgroup list info to update");
    return TSDB_CODE_CTG_INTERNAL_ERROR;
  }

  if (pVgroup->vgroupVersion < 0) {
    ctgError("vgroup version[%d] is invalid", pVgroup->vgroupVersion);
    return TSDB_CODE_CTG_INVALID_INPUT;
  }

  if (NULL == pCatalog->vgroupCache.cache) {
    pCatalog->vgroupCache.cache = taosHashInit(CTG_DEFAULT_CACHE_VGROUP_NUMBER, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
    if (NULL == pCatalog->vgroupCache.cache) {
      ctgError("init hash[%d] for cluster cache failed", CTG_DEFAULT_CACHE_VGROUP_NUMBER);
      return TSDB_CODE_CTG_MEM_ERROR;
    }
  } else {
    taosHashClear(pCatalog->vgroupCache.cache);
  }

  SVgroupInfo *vInfo = NULL;
  for (int32_t i = 0; i < pVgroup->vgroupNum; ++i) {
    if (taosHashPut(pCatalog->vgroupCache.cache, &pVgroup->vgroupInfo[i].vgId, sizeof(pVgroup->vgroupInfo[i].vgId), &pVgroup->vgroupInfo[i], sizeof(pVgroup->vgroupInfo[i])) != 0) {
      ctgError("push to vgroup hash cache failed");
      goto error_exit;
    }
  }

  pCatalog->vgroupCache.vgroupVersion = pVgroup->vgroupVersion;

  return TSDB_CODE_SUCCESS;

error_exit:
  if (pCatalog->vgroupCache.cache) {
    taosHashCleanup(pCatalog->vgroupCache.cache);
    pCatalog->vgroupCache.cache = NULL;
  }

  pCatalog->vgroupCache.vgroupVersion = CTG_DEFAULT_INVALID_VERSION;

  return TSDB_CODE_CTG_INTERNAL_ERROR;
}

int32_t catalogGetVgroup(SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, SHashObj** pVgroupHash) {
  if (NULL == pCatalog || NULL == pMgmtEps || NULL == pRpc) {
    return TSDB_CODE_CTG_INVALID_INPUT;
  }

  int32_t exist = 0;
  
  CTG_ERR_RET(ctgGetVgroupFromCache(pCatalog, pVgroupHash, &exist));

  if (exist) {
    return TSDB_CODE_SUCCESS;
  }

  SVgroupListInfo *pVgroup = NULL;
  
  CTG_ERR_RET(ctgGetVgroupFromMnode(pCatalog, pRpc, pMgmtEps, &pVgroup));

  CTG_ERR_RET(catalogUpdateVgroupCache(pCatalog, pVgroup));

  if (pVgroupHash) {
    CTG_ERR_RET(ctgGetVgroupFromCache(pCatalog, pVgroupHash, &exist));
  }

  if (0 == exist) {
    ctgError("catalog fetched but get from cache failed");
    return TSDB_CODE_CTG_INTERNAL_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t catalogGetDBVgroupVersion(struct SCatalog* pCatalog, const char* dbName, int32_t* version) {
  if (NULL == pCatalog || NULL == dbName || NULL == version) {
    return TSDB_CODE_CTG_INVALID_INPUT;
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

  *version = dbInfo->vgroupVersion;

  return TSDB_CODE_SUCCESS;
}

int32_t catalogUpdateDBVgroupCache(struct SCatalog* pCatalog, const char* dbName, SDBVgroupInfo* dbInfo) {
  if (NULL == pCatalog || NULL == dbName || NULL == dbInfo) {
    return TSDB_CODE_CTG_INVALID_INPUT;
  }

  if (dbInfo->vgroupVersion < 0) {
    if (pCatalog->dbCache.cache) {
      taosHashRemove(pCatalog->dbCache.cache, dbName, strlen(dbName));
    }
    
    ctgWarn("remove db [%s] from cache", dbName);
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == pCatalog->dbCache.cache) {
    pCatalog->dbCache.cache = taosHashInit(CTG_DEFAULT_CACHE_DB_NUMBER, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (NULL == pCatalog->dbCache.cache) {
      ctgError("init hash[%d] for db cache failed", CTG_DEFAULT_CACHE_DB_NUMBER);
      return TSDB_CODE_CTG_MEM_ERROR;
    }
  }

  if (taosHashPut(pCatalog->dbCache.cache, dbName, strlen(dbName), dbInfo, sizeof(*dbInfo)) != 0) {
    ctgError("push to vgroup hash cache failed");
    return TSDB_CODE_CTG_MEM_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}




int32_t catalogGetDBVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* dbName, int32_t forceUpdate, SDBVgroupInfo** dbInfo) {
  if (NULL == pCatalog || NULL == dbName || NULL == pRpc || NULL == pMgmtEps) {
    return TSDB_CODE_CTG_INVALID_INPUT;
  }

  int32_t exist = 0;
  int32_t code = 0;

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
  input.vgroupVersion = pCatalog->vgroupCache.vgroupVersion;
  input.dbGroupVersion = CTG_DEFAULT_INVALID_VERSION;
  
  CTG_ERR_RET(ctgGetDBVgroupFromMnode(pCatalog, pRpc, pMgmtEps, &input, &DbOut));

  if (DbOut.vgroupList) {
    CTG_ERR_JRET(catalogUpdateVgroupCache(pCatalog, DbOut.vgroupList));
  }

  if (DbOut.dbVgroup) {
    CTG_ERR_JRET(catalogUpdateDBVgroupCache(pCatalog, dbName, DbOut.dbVgroup));
  }

  if (dbInfo) {
    *dbInfo = DbOut.dbVgroup;
    DbOut.dbVgroup = NULL;
  }

_return:
  tfree(DbOut.dbVgroup);
  tfree(DbOut.vgroupList);

  return code;
}

int32_t catalogGetTableMeta(SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pDBName, const char* pTableName, STableMeta** pTableMeta) {
  return ctgGetTableMetaImpl(pCatalog, pRpc, pMgmtEps, pDBName, pTableName, false, pTableMeta);
}

int32_t catalogRenewTableMeta(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pDBName, const char* pTableName) {
  if (NULL == pCatalog || NULL == pDBName || NULL == pRpc || NULL == pMgmtEps || NULL == pTableName) {
    return TSDB_CODE_CTG_INVALID_INPUT;
  }

  SVgroupInfo vgroupInfo = {0};
  
  CTG_ERR_RET(ctgGetTableHashVgroup(pCatalog, pRpc, pMgmtEps, pDBName, pTableName, &vgroupInfo));

  STableMetaOutput output = {0};
  
  CTG_ERR_RET(ctgGetTableMetaFromMnode(pCatalog, pRpc, pMgmtEps, pDBName, pTableName, &vgroupInfo, &output));

  CTG_ERR_RET(ctgUpdateTableMetaCache(pCatalog, &output));

  tfree(output.tbMeta);
  
  return TSDB_CODE_SUCCESS;
}

int32_t catalogRenewAndGetTableMeta(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pDBName, const char* pTableName, STableMeta* pTableMeta) {
  return ctgGetTableMetaImpl(pCatalog, pRpc, pMgmtEps, pDBName, pTableName, true, pTableMeta);
}

int32_t catalogGetTableVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pTableName, SArray* pVgroupList) {

}


int32_t catalogGetAllMeta(struct SCatalog* pCatalog, const SEpSet* pMgmtEps, const SCatalogReq* pReq, SMetaData* pRsp) {
  if (NULL == pCatalog || NULL == pMgmtEps || NULL == pReq || NULL == pRsp) {
    return TSDB_CODE_CTG_INVALID_INPUT;
  }
  
  return 0;
}

void catalogDestroy(void) {
  if (ctgMgmt.pCluster) {
    taosHashCleanup(ctgMgmt.pCluster); //TBD
    ctgMgmt.pCluster = NULL;
  }
}



