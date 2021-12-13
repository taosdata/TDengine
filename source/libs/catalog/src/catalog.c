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

SCatalogMgmt ctgMgmt = {0};


int32_t catalogInit(SCatalog *cfg) {
  ctgMgmt.pCluster = taosHashInit(CTG_DEFAULT_CLUSTER_NUMBER, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (NULL == ctgMgmt.pCluster) {
    CTG_ERR_LRET(TSDB_CODE_CTG_INTERNAL_EROR, "init %d cluster cache failed", CTG_DEFAULT_CLUSTER_NUMBER);
  }

  return TSDB_CODE_SUCCESS;
}


struct SCatalog* catalogGetHandle(const char *clusterId) {
  if (NULL == clusterId) {
    return NULL;
  }

  if (NULL == ctgMgmt.pCluster) {
    ctgError("cluster cache are not ready");
    return NULL;
  }

  size_t clen = strlen(clusterId);
  SCatalog *clusterCtg = (SCatalog *)taosHashGet(ctgMgmt.pCluster, clusterId, clen);

  if (clusterCtg) {
    return clusterCtg;
  }

  clusterCtg = calloc(1, sizeof(*clusterCtg));
  if (NULL == clusterCtg) {
    ctgError("calloc %d failed", sizeof(*clusterCtg));
    return NULL;
  }

  if (taosHashPut(ctgMgmt.pCluster, clusterId, clen, &clusterCtg, POINTER_BYTES)) {
    ctgError("put cluster %s cache to hash failed", clusterId);
    tfree(clusterCtg);
    return NULL;
  }
  
  return clusterCtg;
}

int32_t catalogGetTableMeta(struct SCatalog* pCatalog, SRpcObj *pRpcObj, const SEpSet* pMgmtEps, const char* pTableName, const STagData* tagData, STableMeta* pTableMeta) {
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
      .ahandle = (void*)pSql->self,
      .handle  = NULL,
      .code    = 0
  };

  rpcSendRequest(pRpcObj->pDnodeConn, pVnodeEpSet, &rpcMsg, &pSql->rpcRid);
}


int32_t catalogGetAllMeta(struct SCatalog* pCatalog, const SEpSet* pMgmtEps, const SCatalogReq* pReq, SCatalogRsp* pRsp) {
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



