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
#include "tmessage.h"

SCatalogMgmt ctgMgmt = {0};

int32_t ctgGetVgroupFromMnode(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, SVgroupListInfo** pVgroup) {
  char *msg = NULL;
  SEpSet *pVnodeEpSet = NULL;
  int32_t msgLen = 0;

  int32_t code = tscBuildMsg[TSDB_MSG_TYPE_VGROUP_LIST](NULL, &msg, 0, &msgLen);
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

  code = tscProcessMsgRsp[TSDB_MSG_TYPE_VGROUP_LIST](pVgroup, rpcRsp.pCont, rpcRsp.contLen);
  if (code) {
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetVgroupFromCache(SCatalog* pCatalog, SArray** pVgroupList, int32_t* exist) {
  if (NULL == pCatalog->vgroupCache.arrayCache || pCatalog->vgroupCache.vgroupVersion < 0) {
    *exist = 0;
    return TSDB_CODE_SUCCESS;
  }

  if (pVgroupList) {
    *pVgroupList = taosArrayDup(pCatalog->vgroupCache.arrayCache);
  }

  *exist = 1;
  
  return TSDB_CODE_SUCCESS;
}





int32_t catalogInit(SCatalogCfg *cfg) {
  ctgMgmt.pCluster = taosHashInit(CTG_DEFAULT_CLUSTER_NUMBER, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (NULL == ctgMgmt.pCluster) {
    CTG_ERR_LRET(TSDB_CODE_CTG_INTERNAL_ERROR, "init %d cluster cache failed", CTG_DEFAULT_CLUSTER_NUMBER);
  }

  return TSDB_CODE_SUCCESS;
}


int32_t catalogGetHandle(const char *clusterId, struct SCatalog** catalogHandle) {
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



int32_t catalogUpdateVgroup(struct SCatalog* pCatalog, SVgroupListInfo* pVgroup) {
  if (NULL == pVgroup) {
    ctgError("vgroup get from mnode succeed, but no output");
    return TSDB_CODE_CTG_INTERNAL_ERROR;
  }

  if (pVgroup->vgroupVersion < 0) {
    ctgError("vgroup version[%d] is invalid", pVgroup->vgroupVersion);
    return TSDB_CODE_CTG_INVALID_INPUT;
  }
  

  if (NULL == pCatalog->vgroupCache.arrayCache) {
    pCatalog->vgroupCache.arrayCache = taosArrayInit(pVgroup->vgroupNum, sizeof(pVgroup->vgroupInfo[0]));
    if (NULL == pCatalog->vgroupCache.arrayCache) {
      ctgError("init array[%d] for cluster cache failed", pVgroup->vgroupNum);
      return TSDB_CODE_CTG_MEM_ERROR;
    }
  } else {
    taosArrayClear(pCatalog->vgroupCache.arrayCache);
  }

  if (NULL == pCatalog->vgroupCache.cache) {
    pCatalog->vgroupCache.cache = taosHashInit(CTG_DEFAULT_VGROUP_NUMBER, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
    if (NULL == pCatalog->vgroupCache.cache) {
      ctgError("init hash[%d] for cluster cache failed", CTG_DEFAULT_VGROUP_NUMBER);
      return TSDB_CODE_CTG_MEM_ERROR;
    }
  } else {
    taosHashClear(pCatalog->vgroupCache.cache);
  }

  SVgroupInfo *vInfo = NULL;
  for (int32_t i = 0; i < pVgroup->vgroupNum; ++i) {
    vInfo = taosArrayPush(pCatalog->vgroupCache.arrayCache, &pVgroup->vgroupInfo[i]);
    if (NULL == vInfo) {
      ctgError("push to vgroup array cache failed");
      goto error_exit;
    }
    
    if (taosHashPut(pCatalog->vgroupCache.cache, &pVgroup->vgroupInfo[i].vgId, sizeof(pVgroup->vgroupInfo[i].vgId), &vInfo, POINTER_BYTES) != 0) {
      ctgError("push to vgroup hash cache failed");
      goto error_exit;
    }
  }

  pCatalog->vgroupCache.vgroupVersion = pVgroup->vgroupVersion;

  return TSDB_CODE_SUCCESS;

error_exit:
  if (pCatalog->vgroupCache.arrayCache) {
    taosArrayDestroy(pCatalog->vgroupCache.arrayCache);
    pCatalog->vgroupCache.arrayCache = NULL;
  }

  if (pCatalog->vgroupCache.cache) {
    taosHashCleanup(pCatalog->vgroupCache.cache);
    pCatalog->vgroupCache.cache = NULL;
  }

  pCatalog->vgroupCache.vgroupVersion = CTG_DEFAULT_INVALID_VERSION;

  return TSDB_CODE_CTG_INTERNAL_ERROR;
}


int32_t catalogGetVgroup(SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, SArray** pVgroupList) {
  if (NULL == pCatalog || NULL == pMgmtEps || NULL == pRpc) {
    return TSDB_CODE_CTG_INVALID_INPUT;
  }

  int32_t exist = 0;
  
  CTG_ERR_RET(ctgGetVgroupFromCache(pCatalog, pVgroupList, &exist));

  if (exist) {
    return TSDB_CODE_SUCCESS;
  }

  SVgroupListInfo *pVgroup = NULL;
  
  CTG_ERR_RET(ctgGetVgroupFromMnode(pCatalog, pRpc, pMgmtEps, &pVgroup));

  CTG_ERR_RET(catalogUpdateVgroup(pCatalog, pVgroup));

  if (pVgroupList) {
    CTG_ERR_RET(ctgGetVgroupFromCache(pCatalog, pVgroupList, &exist));
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

int32_t catalogGetDBVgroup(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* dbName, int32_t forceUpdate, SDBVgroupInfo* dbInfo) {

}

int32_t catalogUpdateDBVgroup(struct SCatalog* pCatalog, const char* dbName, SDBVgroupInfo* dbInfo) {

}



int32_t catalogGetTableMetaFromMnode(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pTableName, const STagData* tagData, STableMeta* pTableMeta) {
  if (NULL == pCatalog || NULL == pMgmtEps || NULL == pTableName || NULL == pTableMeta) {
    return TSDB_CODE_CTG_INVALID_INPUT;
  }

  SBuildTableMetaInput bInput = {0};
  char *msg = NULL;
  SEpSet *pVnodeEpSet = NULL;
  int32_t msgLen = 0;

  int32_t code = tscBuildMsg[TSDB_MSG_TYPE_TABLE_META](&bInput, &msg, 0, &msgLen);
  if (code) {
    return code;
  }

  SRpcMsg rpcMsg = {
      .msgType = TSDB_MSG_TYPE_TABLE_META,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};

  rpcSendRecv(pRpc, (SEpSet*)pMgmtEps, &rpcMsg, &rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t catalogGetTableMeta(struct SCatalog* pCatalog, void *pRpc, const SEpSet* pMgmtEps, const char* pTableName, STableMeta* pTableMeta) {

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



