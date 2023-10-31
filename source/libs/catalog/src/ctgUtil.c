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

void ctgFreeSViewMeta(SViewMeta* pMeta) {
  if (NULL == pMeta) {
    return;
  }

  taosMemoryFree(pMeta->user);
  taosMemoryFree(pMeta->querySql);
  taosMemoryFree(pMeta->pSchema);
}

void ctgFreeMsgSendParam(void* param) {
  if (NULL == param) {
    return;
  }

  SCtgTaskCallbackParam* pParam = (SCtgTaskCallbackParam*)param;
  taosArrayDestroy(pParam->taskId);
  taosArrayDestroy(pParam->msgIdx);

  taosMemoryFree(param);
}

void ctgFreeBatchMsg(void* msg) {
  if (NULL == msg) {
    return;
  }
  SBatchMsg* pMsg = (SBatchMsg*)msg;
  taosMemoryFree(pMsg->msg);
}

void ctgFreeBatch(SCtgBatch* pBatch) {
  if (NULL == pBatch) {
    return;
  }

  taosArrayDestroyEx(pBatch->pMsgs, ctgFreeBatchMsg);
  taosArrayDestroy(pBatch->pTaskIds);
}

void ctgFreeBatchs(SHashObj* pBatchs) {
  void* p = taosHashIterate(pBatchs, NULL);
  while (NULL != p) {
    SCtgBatch* pBatch = (SCtgBatch*)p;

    ctgFreeBatch(pBatch);

    p = taosHashIterate(pBatchs, p);
  }

  taosHashCleanup(pBatchs);
}

char* ctgTaskTypeStr(CTG_TASK_TYPE type) {
  switch (type) {
    case CTG_TASK_GET_QNODE:
      return "[get qnode list]";
    case CTG_TASK_GET_DNODE:
      return "[get dnode list]";
    case CTG_TASK_GET_DB_VGROUP:
      return "[get db vgroup]";
    case CTG_TASK_GET_DB_CFG:
      return "[get db cfg]";
    case CTG_TASK_GET_DB_INFO:
      return "[get db info]";
    case CTG_TASK_GET_TB_META:
      return "[get table meta]";
    case CTG_TASK_GET_TB_HASH:
      return "[get table hash]";
    case CTG_TASK_GET_TB_SMA_INDEX:
      return "[get table sma]";
    case CTG_TASK_GET_TB_CFG:
      return "[get table cfg]";
    case CTG_TASK_GET_INDEX_INFO:
      return "[get index info]";
    case CTG_TASK_GET_UDF:
      return "[get udf]";
    case CTG_TASK_GET_USER:
      return "[get user]";
    case CTG_TASK_GET_SVR_VER:
      return "[get svr ver]";
    case CTG_TASK_GET_TB_META_BATCH:
      return "[bget table meta]";
    case CTG_TASK_GET_TB_HASH_BATCH:
      return "[bget table hash]";
    case CTG_TASK_GET_TB_TAG:
      return "[get table tag]";
    case CTG_TASK_GET_VIEW:
      return "[get view]";
    default:
      return "unknown";
  }
}

void ctgFreeQNode(SCtgQNode* node) {
  if (NULL == node) {
    return;
  }

  if (node->op) {
    taosMemoryFree(node->op->data);
    taosMemoryFree(node->op);
  }

  taosMemoryFree(node);
}

void ctgFreeSTableIndex(void* info) {
  if (NULL == info) {
    return;
  }

  STableIndex* pInfo = (STableIndex*)info;

  taosArrayDestroyEx(pInfo->pIndex, tFreeSTableIndexInfo);
}

void ctgFreeSMetaData(SMetaData* pData) {
  taosArrayDestroy(pData->pTableMeta);
  pData->pTableMeta = NULL;

  /*
    for (int32_t i = 0; i < taosArrayGetSize(pData->pDbVgroup); ++i) {
      SArray** pArray = taosArrayGet(pData->pDbVgroup, i);
      taosArrayDestroy(*pArray);
    }
  */
  taosArrayDestroy(pData->pDbVgroup);
  pData->pDbVgroup = NULL;

  taosArrayDestroy(pData->pTableHash);
  pData->pTableHash = NULL;

  taosArrayDestroy(pData->pTableIndex);
  pData->pTableIndex = NULL;

  taosArrayDestroy(pData->pUdfList);
  pData->pUdfList = NULL;

  /*
    for (int32_t i = 0; i < taosArrayGetSize(pData->pDbCfg); ++i) {
      SDbCfgInfo* pInfo = taosArrayGet(pData->pDbCfg, i);
      taosArrayDestroy(pInfo->pRetensions);
    }
  */
  taosArrayDestroy(pData->pDbCfg);
  pData->pDbCfg = NULL;

  taosArrayDestroy(pData->pDbInfo);
  pData->pDbInfo = NULL;

  taosArrayDestroy(pData->pIndex);
  pData->pIndex = NULL;

  taosArrayDestroy(pData->pUser);
  pData->pUser = NULL;

  taosArrayDestroy(pData->pQnodeList);
  pData->pQnodeList = NULL;

  taosArrayDestroy(pData->pDnodeList);
  pData->pDnodeList = NULL;

  taosArrayDestroy(pData->pTableCfg);
  pData->pTableCfg = NULL;

  taosArrayDestroy(pData->pTableTag);
  pData->pTableTag = NULL;

  taosArrayDestroy(pData->pView);
  pData->pView = NULL;

  taosMemoryFreeClear(pData->pSvrVer);
}

void ctgFreeSCtgUserAuth(SCtgUserAuth* userCache) {
  taosHashCleanup(userCache->userAuth.createdDbs);
  taosHashCleanup(userCache->userAuth.readDbs);
  taosHashCleanup(userCache->userAuth.writeDbs);
  taosHashCleanup(userCache->userAuth.readTbs);
  taosHashCleanup(userCache->userAuth.writeTbs);
  taosHashCleanup(userCache->userAuth.alterTbs);
  taosHashCleanup(userCache->userAuth.readViews);
  taosHashCleanup(userCache->userAuth.writeViews);
  taosHashCleanup(userCache->userAuth.alterViews);
  taosHashCleanup(userCache->userAuth.useDbs);
}

void ctgFreeMetaRent(SCtgRentMgmt* mgmt) {
  if (NULL == mgmt->slots) {
    return;
  }

  for (int32_t i = 0; i < mgmt->slotNum; ++i) {
    SCtgRentSlot* slot = &mgmt->slots[i];
    if (slot->meta) {
      taosArrayDestroy(slot->meta);
      slot->meta = NULL;
    }
  }

  taosMemoryFreeClear(mgmt->slots);
  mgmt->rentCacheSize = 0;
}

void ctgFreeStbMetaCache(SCtgDBCache* dbCache) {
  if (NULL == dbCache->stbCache) {
    return;
  }

  int32_t stbNum = taosHashGetSize(dbCache->stbCache);
  taosHashCleanup(dbCache->stbCache);
  dbCache->stbCache = NULL;
}

void ctgFreeTbCacheImpl(SCtgTbCache* pCache, bool lock) {
  if (pCache->pMeta) {
    if (lock) { 
      CTG_LOCK(CTG_WRITE, &pCache->metaLock);
    }
    taosMemoryFreeClear(pCache->pMeta);
    if (lock) { 
      CTG_UNLOCK(CTG_WRITE, &pCache->metaLock);
    }
  }

  if (pCache->pIndex) {
    if (lock) { 
      CTG_LOCK(CTG_WRITE, &pCache->indexLock);
    }
    taosArrayDestroyEx(pCache->pIndex->pIndex, tFreeSTableIndexInfo);
    taosMemoryFreeClear(pCache->pIndex);
    if (lock) { 
      CTG_UNLOCK(CTG_WRITE, &pCache->indexLock);
    }
  }
}

void ctgFreeViewCacheImpl(SCtgViewCache* pCache, bool lock) {
  if (lock) { 
    CTG_LOCK(CTG_WRITE, &pCache->viewLock);
  }
  if (pCache->pMeta) {
    ctgFreeSViewMeta(pCache->pMeta);
    taosMemoryFreeClear(pCache->pMeta);
  }
  if (lock) { 
    CTG_UNLOCK(CTG_WRITE, &pCache->viewLock);
  }
}

void ctgFreeViewCache(SCtgDBCache* dbCache) {
  if (NULL == dbCache->viewCache) {
    return;
  }

  SCtgViewCache* pCache = taosHashIterate(dbCache->viewCache, NULL);
  while (NULL != pCache) {
    ctgFreeViewCacheImpl(pCache, false);
    pCache = taosHashIterate(dbCache->viewCache, pCache);
  }
  taosHashCleanup(dbCache->viewCache);
  dbCache->viewCache = NULL;
}

void ctgFreeTbCache(SCtgDBCache* dbCache) {
  if (NULL == dbCache->tbCache) {
    return;
  }

  SCtgTbCache* pCache = taosHashIterate(dbCache->tbCache, NULL);
  while (NULL != pCache) {
    ctgFreeTbCacheImpl(pCache, false);
    pCache = taosHashIterate(dbCache->tbCache, pCache);
  }
  taosHashCleanup(dbCache->tbCache);
  dbCache->tbCache = NULL;
}

void ctgFreeVgInfoCache(SCtgDBCache* dbCache) { freeVgInfo(dbCache->vgCache.vgInfo); }
void ctgFreeCfgInfoCache(SCtgDBCache* dbCache) { freeDbCfgInfo(dbCache->cfgCache.cfgInfo); }

void ctgFreeDbCache(SCtgDBCache* dbCache) {
  if (NULL == dbCache) {
    return;
  }

  ctgFreeVgInfoCache(dbCache);
  ctgFreeCfgInfoCache(dbCache);
  ctgFreeStbMetaCache(dbCache);
  ctgFreeTbCache(dbCache);
  ctgFreeViewCache(dbCache);
}

void ctgFreeInstDbCache(SHashObj* pDbCache) {
  if (NULL == pDbCache) {
    return;
  }

  int32_t dbNum = taosHashGetSize(pDbCache);

  void* pIter = taosHashIterate(pDbCache, NULL);
  while (pIter) {
    SCtgDBCache* dbCache = pIter;
    atomic_store_8(&dbCache->deleted, 1);
    ctgFreeDbCache(dbCache);

    pIter = taosHashIterate(pDbCache, pIter);
  }

  taosHashCleanup(pDbCache);
}

void ctgFreeInstUserCache(SHashObj* pUserCache) {
  if (NULL == pUserCache) {
    return;
  }

  int32_t userNum = taosHashGetSize(pUserCache);

  void* pIter = taosHashIterate(pUserCache, NULL);
  while (pIter) {
    SCtgUserAuth* userCache = pIter;
    ctgFreeSCtgUserAuth(userCache);

    pIter = taosHashIterate(pUserCache, pIter);
  }

  taosHashCleanup(pUserCache);
}

void ctgFreeHandleImpl(SCatalog* pCtg) {
  ctgFreeMetaRent(&pCtg->dbRent);
  ctgFreeMetaRent(&pCtg->stbRent);
  ctgFreeMetaRent(&pCtg->viewRent);

  ctgFreeInstDbCache(pCtg->dbCache);
  ctgFreeInstUserCache(pCtg->userCache);

  taosMemoryFree(pCtg);
}

int32_t ctgRemoveCacheUser(SCatalog* pCtg, SCtgUserAuth* pUser, const char* user) {
  CTG_LOCK(CTG_WRITE, &pUser->lock);  
  ctgFreeSCtgUserAuth(pUser);
  CTG_UNLOCK(CTG_WRITE, &pUser->lock);  
  
  if (taosHashRemove(pCtg->userCache, user, strlen(user)) == 0) {
    return 0;  // user found and removed
  }

  return -1;
}

void ctgFreeHandle(SCatalog* pCtg) {
  if (NULL == pCtg) {
    return;
  }

  uint64_t clusterId = pCtg->clusterId;

  ctgFreeMetaRent(&pCtg->dbRent);
  ctgFreeMetaRent(&pCtg->stbRent);
  ctgFreeMetaRent(&pCtg->viewRent);

  ctgFreeInstDbCache(pCtg->dbCache);
  ctgFreeInstUserCache(pCtg->userCache);

  CTG_STAT_NUM_DEC(CTG_CI_CLUSTER, 1);

  taosMemoryFree(pCtg);

  ctgInfo("handle freed, clusterId:0x%" PRIx64, clusterId);
}

void ctgClearHandleMeta(SCatalog* pCtg, int64_t *pClearedSize, int64_t *pCleardNum, bool *roundDone) {
  int64_t cacheSize = 0;
  void* pIter = taosHashIterate(pCtg->dbCache, NULL);
  while (pIter) {
    SCtgDBCache* dbCache = pIter;
    
    SCtgTbCache* pCache = taosHashIterate(dbCache->tbCache, NULL);
    while (NULL != pCache) {
      size_t len = 0;
      void*  key = taosHashGetKey(pCache, &len);

      if (pCache->pMeta && TSDB_SUPER_TABLE == pCache->pMeta->tableType) {
        pCache = taosHashIterate(dbCache->tbCache, pCache);
        continue;
      }
      
      taosHashRemove(dbCache->tbCache, key, len);
      cacheSize = len + sizeof(SCtgTbCache) + ctgGetTbMetaCacheSize(pCache->pMeta) + ctgGetTbIndexCacheSize(pCache->pIndex);
      atomic_sub_fetch_64(&dbCache->dbCacheSize, cacheSize);
      *pClearedSize += cacheSize;
      (*pCleardNum)++;

      if (pCache->pMeta) {
        CTG_META_NUM_DEC(pCache->pMeta->tableType);
      }
      
      ctgFreeTbCacheImpl(pCache, true);
      
      if (*pCleardNum >= CTG_CLEAR_CACHE_ROUND_TB_NUM) {
        taosHashCancelIterate(dbCache->tbCache, pCache);
        goto _return;
      }
            
      pCache = taosHashIterate(dbCache->tbCache, pCache);
    }

    pIter = taosHashIterate(pCtg->dbCache, pIter);
  }

_return:

  if (*pCleardNum >= CTG_CLEAR_CACHE_ROUND_TB_NUM) {
    *roundDone = true;
  }
}

void ctgClearAllHandleMeta(int64_t *clearedSize, int64_t *clearedNum, bool *roundDone) {
  SCatalog *pCtg = NULL;

  void *pIter = taosHashIterate(gCtgMgmt.pCluster, NULL);
  while (pIter) {
    pCtg = *(SCatalog **)pIter;

    if (pCtg) {
      ctgClearHandleMeta(pCtg, clearedSize, clearedNum, roundDone);
      if (*roundDone) {
        taosHashCancelIterate(gCtgMgmt.pCluster, pIter);
        break;
      }
    }

    pIter = taosHashIterate(gCtgMgmt.pCluster, pIter);
  }
}

void ctgClearHandle(SCatalog* pCtg) {
  if (NULL == pCtg) {
    return;
  }

  uint64_t clusterId = pCtg->clusterId;
  
  ctgFreeMetaRent(&pCtg->dbRent);
  ctgFreeMetaRent(&pCtg->stbRent);
  ctgFreeMetaRent(&pCtg->viewRent);

  ctgFreeInstDbCache(pCtg->dbCache);
  ctgFreeInstUserCache(pCtg->userCache);

  ctgMetaRentInit(&pCtg->dbRent, gCtgMgmt.cfg.dbRentSec, CTG_RENT_DB, sizeof(SDbCacheInfo));
  ctgMetaRentInit(&pCtg->stbRent, gCtgMgmt.cfg.stbRentSec, CTG_RENT_STABLE, sizeof(SSTableVersion));
  ctgMetaRentInit(&pCtg->viewRent, gCtgMgmt.cfg.viewRentSec, CTG_RENT_VIEW, sizeof(SViewVersion));

  pCtg->dbCache = taosHashInit(gCtgMgmt.cfg.maxDBCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false,
                               HASH_ENTRY_LOCK);
  if (NULL == pCtg->dbCache) {
    qError("taosHashInit %d dbCache failed", CTG_DEFAULT_CACHE_DB_NUMBER);
  }

  pCtg->userCache = taosHashInit(gCtgMgmt.cfg.maxUserCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false,
                                 HASH_ENTRY_LOCK);
  if (NULL == pCtg->userCache) {
    ctgError("taosHashInit %d user cache failed", gCtgMgmt.cfg.maxUserCacheNum);
  }

  memset(pCtg->cacheStat.cacheNum, 0, sizeof(pCtg->cacheStat.cacheNum));

  CTG_STAT_RT_INC(numOfOpClearCache, 1);

  ctgInfo("handle cleared, clusterId:0x%" PRIx64, clusterId);
}

void ctgFreeSUseDbOutput(SUseDbOutput* pOutput) {
  if (NULL == pOutput) {
    return;
  }

  if (pOutput->dbVgroup) {
    freeVgInfo(pOutput->dbVgroup);
  }

  taosMemoryFree(pOutput);
}

void ctgFreeMsgCtx(SCtgMsgCtx* pCtx) {
  taosMemoryFreeClear(pCtx->target);
  if (NULL == pCtx->out) {
    return;
  }

  switch (pCtx->reqType) {
    case TDMT_MND_GET_DB_CFG: {
      SDbCfgInfo* pOut = (SDbCfgInfo*)pCtx->out;
      taosArrayDestroy(pOut->pRetensions);
      taosMemoryFreeClear(pCtx->out);
      break;
    }
    case TDMT_MND_USE_DB: {
      SUseDbOutput* pOut = (SUseDbOutput*)pCtx->out;
      ctgFreeSUseDbOutput(pOut);
      pCtx->out = NULL;
      break;
    }
    case TDMT_MND_GET_INDEX: {
      SIndexInfo* pOut = (SIndexInfo*)pCtx->out;
      taosMemoryFreeClear(pCtx->out);
      break;
    }
    case TDMT_MND_QNODE_LIST: {
      SArray* pOut = (SArray*)pCtx->out;
      taosArrayDestroy(pOut);
      pCtx->out = NULL;
      break;
    }
    case TDMT_VND_TABLE_META:
    case TDMT_MND_TABLE_META: {
      STableMetaOutput* pOut = (STableMetaOutput*)pCtx->out;
      taosMemoryFree(pOut->tbMeta);
      taosMemoryFreeClear(pCtx->out);
      break;
    }
    case TDMT_MND_GET_TABLE_INDEX: {
      STableIndex* pOut = (STableIndex*)pCtx->out;
      if (pOut) {
        taosArrayDestroyEx(pOut->pIndex, tFreeSTableIndexInfo);
        taosMemoryFreeClear(pCtx->out);
      }
      break;
    }
    case TDMT_VND_TABLE_CFG:
    case TDMT_MND_TABLE_CFG: {
      STableCfgRsp* pOut = (STableCfgRsp*)pCtx->out;
      tFreeSTableCfgRsp(pOut);
      taosMemoryFreeClear(pCtx->out);
      break;
    }
    case TDMT_MND_RETRIEVE_FUNC: {
      SFuncInfo* pOut = (SFuncInfo*)pCtx->out;
      taosMemoryFree(pOut->pCode);
      taosMemoryFree(pOut->pComment);
      taosMemoryFreeClear(pCtx->out);
      break;
    }
    case TDMT_MND_GET_USER_AUTH: {
      SGetUserAuthRsp* pOut = (SGetUserAuthRsp*)pCtx->out;
      taosHashCleanup(pOut->createdDbs);
      taosHashCleanup(pOut->readDbs);
      taosHashCleanup(pOut->writeDbs);
      taosHashCleanup(pOut->readTbs);
      taosHashCleanup(pOut->writeTbs);
      taosHashCleanup(pOut->alterTbs);
      taosHashCleanup(pOut->readViews);
      taosHashCleanup(pOut->writeViews);
      taosHashCleanup(pOut->alterViews);
      taosHashCleanup(pOut->useDbs);
      taosMemoryFreeClear(pCtx->out);
      break;
    }
    case TDMT_MND_VIEW_META: {
      if (NULL != pCtx->out) {
        SViewMetaRsp* pOut = *(SViewMetaRsp**)pCtx->out;
        if (NULL != pOut) {
          tFreeSViewMetaRsp(pOut);
          taosMemoryFree(pOut);
        }
        taosMemoryFreeClear(pCtx->out);
      }
      break;
    }
    default:
      qError("invalid reqType %d", pCtx->reqType);
      break;
  }
}

void ctgFreeTbMetasMsgCtx(SCtgMsgCtx* pCtx) {
  ctgFreeMsgCtx(pCtx);
  if (pCtx->lastOut) {
    ctgFreeSTableMetaOutput((STableMetaOutput*)pCtx->lastOut);
    pCtx->lastOut = NULL;
  }
}

void ctgFreeSTableMetaOutput(STableMetaOutput* pOutput) {
  if (NULL == pOutput) {
    return;
  }

  taosMemoryFree(pOutput->tbMeta);
  taosMemoryFree(pOutput);
}

void ctgResetTbMetaTask(SCtgTask* pTask) {
  SCtgTbMetaCtx* taskCtx = (SCtgTbMetaCtx*)pTask->taskCtx;
  memset(&taskCtx->tbInfo, 0, sizeof(taskCtx->tbInfo));
  taskCtx->flag = CTG_FLAG_UNKNOWN_STB;

  if (pTask->msgCtx.lastOut) {
    ctgFreeSTableMetaOutput((STableMetaOutput*)pTask->msgCtx.lastOut);
    pTask->msgCtx.lastOut = NULL;
  }
  if (pTask->msgCtx.out) {
    ctgFreeSTableMetaOutput((STableMetaOutput*)pTask->msgCtx.out);
    pTask->msgCtx.out = NULL;
  }
  taosMemoryFreeClear(pTask->msgCtx.target);
  taosMemoryFreeClear(pTask->res);
}

void ctgFreeBatchMeta(void* meta) {
  if (NULL == meta) {
    return;
  }

  SMetaRes* pRes = (SMetaRes*)meta;
  taosMemoryFreeClear(pRes->pRes);
}

void ctgFreeBatchHash(void* hash) {
  if (NULL == hash) {
    return;
  }

  SMetaRes* pRes = (SMetaRes*)hash;
  taosMemoryFreeClear(pRes->pRes);
}

void ctgFreeViewMetaRes(void* res) {
  if (NULL == res) {
    return;
  }

  SMetaRes* pRes = (SMetaRes*)res;
  if (NULL != pRes->pRes) {
    SViewMeta* pMeta = (SViewMeta*)pRes->pRes;
    ctgFreeSViewMeta(pMeta);
    taosMemoryFreeClear(pRes->pRes);
  }
}

void ctgFreeJsonTagVal(void* val) {
  if (NULL == val) {
    return;
  }

  STagVal* pVal = (STagVal*)val;

  if (TSDB_DATA_TYPE_JSON == pVal->type) {
    taosMemoryFree(pVal->pData);
  }
}

void ctgFreeTaskRes(CTG_TASK_TYPE type, void** pRes) {
  switch (type) {
    case CTG_TASK_GET_QNODE:
    case CTG_TASK_GET_DNODE:
    case CTG_TASK_GET_DB_VGROUP: {
      taosArrayDestroy((SArray*)*pRes);
      *pRes = NULL;
      break;
    }
    case CTG_TASK_GET_DB_CFG: {
      if (*pRes) {
        SDbCfgInfo* pInfo = (SDbCfgInfo*)*pRes;
        taosArrayDestroy(pInfo->pRetensions);
        taosMemoryFreeClear(*pRes);
      }
      break;
    }
    case CTG_TASK_GET_TB_SMA_INDEX: {
      taosArrayDestroyEx(*pRes, tFreeSTableIndexInfo);
      *pRes = NULL;
      break;
    }
    case CTG_TASK_GET_TB_CFG: {
      if (*pRes) {
        STableCfg* pInfo = (STableCfg*)*pRes;
        tFreeSTableCfgRsp(pInfo);
        taosMemoryFreeClear(*pRes);
      }
      break;
    }
    case CTG_TASK_GET_USER: {
      if (*pRes) {
        SUserAuthRes* pAuth = (SUserAuthRes*)*pRes;
        for (int32_t i = 0; i < AUTH_RES_MAX_VALUE; ++i) {
          nodesDestroyNode(pAuth->pCond[i]);
        }
        taosMemoryFreeClear(*pRes);
      }
      break;
    }
    case CTG_TASK_GET_TB_HASH:
    case CTG_TASK_GET_DB_INFO:
    case CTG_TASK_GET_INDEX_INFO:
    case CTG_TASK_GET_UDF:
    case CTG_TASK_GET_SVR_VER:
    case CTG_TASK_GET_TB_META: {
      taosMemoryFreeClear(*pRes);
      break;
    }
    case CTG_TASK_GET_TB_TAG: {
      if (1 == taosArrayGetSize(*pRes)) {
        taosArrayDestroyEx(*pRes, ctgFreeJsonTagVal);
      } else {
        taosArrayDestroy(*pRes);
      }
      *pRes = NULL;
      break;
    }
    case CTG_TASK_GET_TB_META_BATCH: {
      SArray* pArray = (SArray*)*pRes;
      int32_t num = taosArrayGetSize(pArray);
      for (int32_t i = 0; i < num; ++i) {
        ctgFreeBatchMeta(taosArrayGet(pArray, i));
      }
      *pRes = NULL;  // no need to free it
      break;
    }
    case CTG_TASK_GET_TB_HASH_BATCH: {
      SArray* pArray = (SArray*)*pRes;
      int32_t num = taosArrayGetSize(pArray);
      for (int32_t i = 0; i < num; ++i) {
        ctgFreeBatchHash(taosArrayGet(pArray, i));
      }
      *pRes = NULL;  // no need to free it
      break;
    }
    case CTG_TASK_GET_VIEW: {
      SArray* pArray = (SArray*)*pRes;
      int32_t num = taosArrayGetSize(pArray);
      for (int32_t i = 0; i < num; ++i) {
        ctgFreeViewMetaRes(taosArrayGet(pArray, i));
      }
      *pRes = NULL;  // no need to free it
      break;
    }
    default:
      qError("invalid task type %d", type);
      break;
  }
}

void ctgFreeSubTaskRes(CTG_TASK_TYPE type, void** pRes) {
  switch (type) {
    case CTG_TASK_GET_QNODE:
    case CTG_TASK_GET_DNODE: {
      taosArrayDestroy((SArray*)*pRes);
      *pRes = NULL;
      break;
    }
    case CTG_TASK_GET_DB_VGROUP: {
      if (*pRes) {
        SDBVgInfo* pInfo = (SDBVgInfo*)*pRes;
        freeVgInfo(pInfo);
      }
      break;
    }
    case CTG_TASK_GET_DB_CFG: {
      if (*pRes) {
        SDbCfgInfo* pInfo = (SDbCfgInfo*)*pRes;
        taosArrayDestroy(pInfo->pRetensions);
        taosMemoryFreeClear(*pRes);
      }
      break;
    }
    case CTG_TASK_GET_TB_SMA_INDEX: {
      taosArrayDestroyEx(*pRes, tFreeSTableIndexInfo);
      *pRes = NULL;
      break;
    }
    case CTG_TASK_GET_TB_CFG: {
      if (*pRes) {
        STableCfg* pInfo = (STableCfg*)*pRes;
        tFreeSTableCfgRsp(pInfo);
        taosMemoryFreeClear(*pRes);
      }
      break;
    }
    case CTG_TASK_GET_TB_META:
    case CTG_TASK_GET_DB_INFO:
    case CTG_TASK_GET_TB_HASH:
    case CTG_TASK_GET_INDEX_INFO:
    case CTG_TASK_GET_UDF:
    case CTG_TASK_GET_SVR_VER:
    case CTG_TASK_GET_USER: {
      taosMemoryFreeClear(*pRes);
      break;
    }
    case CTG_TASK_GET_TB_META_BATCH: {
      taosArrayDestroyEx(*pRes, ctgFreeBatchMeta);
      *pRes = NULL;
      break;
    }
    case CTG_TASK_GET_TB_HASH_BATCH: {
      taosArrayDestroyEx(*pRes, ctgFreeBatchHash);
      *pRes = NULL;
      break;
    }
    default:
      qError("invalid task type %d", type);
      break;
  }
}

void ctgClearSubTaskRes(SCtgSubRes* pRes) {
  pRes->code = 0;

  if (NULL == pRes->res) {
    return;
  }

  ctgFreeSubTaskRes(pRes->type, &pRes->res);
}

void ctgFreeTaskCtx(SCtgTask* pTask) {
  switch (pTask->type) {
    case CTG_TASK_GET_TB_META: {
      SCtgTbMetaCtx* taskCtx = (SCtgTbMetaCtx*)pTask->taskCtx;
      taosMemoryFreeClear(taskCtx->pName);
      if (pTask->msgCtx.lastOut) {
        ctgFreeSTableMetaOutput((STableMetaOutput*)pTask->msgCtx.lastOut);
        pTask->msgCtx.lastOut = NULL;
      }
      taosMemoryFreeClear(pTask->taskCtx);
      break;
    }
    case CTG_TASK_GET_TB_META_BATCH: {
      SCtgTbMetasCtx* taskCtx = (SCtgTbMetasCtx*)pTask->taskCtx;
      taosArrayDestroyEx(taskCtx->pResList, ctgFreeBatchMeta);
      taosArrayDestroy(taskCtx->pFetchs);
      // NO NEED TO FREE pNames

      taosArrayDestroyEx(pTask->msgCtxs, (FDelete)ctgFreeTbMetasMsgCtx);

      if (pTask->msgCtx.lastOut) {
        ctgFreeSTableMetaOutput((STableMetaOutput*)pTask->msgCtx.lastOut);
        pTask->msgCtx.lastOut = NULL;
      }
      taosMemoryFreeClear(pTask->taskCtx);
      break;
    }
    case CTG_TASK_GET_TB_HASH: {
      SCtgTbHashCtx* taskCtx = (SCtgTbHashCtx*)pTask->taskCtx;
      taosMemoryFreeClear(taskCtx->pName);
      taosMemoryFreeClear(pTask->taskCtx);
      break;
    }
    case CTG_TASK_GET_TB_HASH_BATCH: {
      SCtgTbHashsCtx* taskCtx = (SCtgTbHashsCtx*)pTask->taskCtx;
      taosArrayDestroyEx(taskCtx->pResList, ctgFreeBatchHash);
      taosArrayDestroy(taskCtx->pFetchs);
      // NO NEED TO FREE pNames

      taosArrayDestroyEx(pTask->msgCtxs, (FDelete)ctgFreeMsgCtx);

      taosMemoryFreeClear(pTask->taskCtx);
      break;
    }
    case CTG_TASK_GET_TB_SMA_INDEX: {
      SCtgTbIndexCtx* taskCtx = (SCtgTbIndexCtx*)pTask->taskCtx;
      taosMemoryFreeClear(taskCtx->pName);
      taosMemoryFreeClear(pTask->taskCtx);
      break;
    }
    case CTG_TASK_GET_TB_CFG: {
      SCtgTbCfgCtx* taskCtx = (SCtgTbCfgCtx*)pTask->taskCtx;
      taosMemoryFreeClear(taskCtx->pName);
      taosMemoryFreeClear(taskCtx->pVgInfo);
      taosMemoryFreeClear(pTask->taskCtx);
      break;
    }
    case CTG_TASK_GET_TB_TAG: {
      SCtgTbTagCtx* taskCtx = (SCtgTbTagCtx*)pTask->taskCtx;
      taosMemoryFreeClear(taskCtx->pName);
      taosMemoryFreeClear(taskCtx->pVgInfo);
      taosMemoryFreeClear(taskCtx);
      break;
    }
    case CTG_TASK_GET_DB_VGROUP:
    case CTG_TASK_GET_DB_CFG:
    case CTG_TASK_GET_DB_INFO:
    case CTG_TASK_GET_INDEX_INFO:
    case CTG_TASK_GET_UDF:
    case CTG_TASK_GET_QNODE:
    case CTG_TASK_GET_USER: {
      taosMemoryFreeClear(pTask->taskCtx);
      break;
    }
    case CTG_TASK_GET_VIEW: {
      SCtgViewsCtx* taskCtx = (SCtgViewsCtx*)pTask->taskCtx;
      taosArrayDestroyEx(taskCtx->pResList, ctgFreeViewMetaRes);
      taosArrayDestroy(taskCtx->pFetchs);
      // NO NEED TO FREE pNames

      taosArrayDestroyEx(pTask->msgCtxs, (FDelete)ctgFreeMsgCtx);

      taosMemoryFreeClear(pTask->taskCtx);
      break;
    }    
    default:
      qError("invalid task type %d", pTask->type);
      break;
  }
}

void ctgFreeTask(SCtgTask* pTask, bool freeRes) {
  ctgFreeMsgCtx(&pTask->msgCtx);
  if (freeRes || pTask->subTask) {
    ctgFreeTaskRes(pTask->type, &pTask->res);
  }
  ctgFreeTaskCtx(pTask);

  taosArrayDestroy(pTask->pParents);
  ctgClearSubTaskRes(&pTask->subRes);
}

void ctgFreeTasks(SArray* pArray, bool freeRes) {
  if (NULL == pArray) {
    return;
  }

  int32_t num = taosArrayGetSize(pArray);
  for (int32_t i = 0; i < num; ++i) {
    SCtgTask* pTask = taosArrayGet(pArray, i);
    ctgFreeTask(pTask, freeRes);
  }

  taosArrayDestroy(pArray);
}

void ctgFreeJob(void* job) {
  if (NULL == job) {
    return;
  }

  SCtgJob* pJob = (SCtgJob*)job;

  int64_t  rid = pJob->refId;
  uint64_t qid = pJob->queryId;

  ctgFreeTasks(pJob->pTasks, pJob->jobRes.ctgFree);
  ctgFreeBatchs(pJob->pBatchs);

  ctgFreeSMetaData(&pJob->jobRes);

  taosMemoryFree(job);

  qDebug("QID:0x%" PRIx64 ", ctg job 0x%" PRIx64 " freed", qid, rid);
}

int32_t ctgUpdateMsgCtx(SCtgMsgCtx* pCtx, int32_t reqType, void* out, char* target) {
  ctgFreeMsgCtx(pCtx);

  pCtx->reqType = reqType;
  pCtx->out = out;
  if (target) {
    pCtx->target = taosStrdup(target);
    if (NULL == pCtx->target) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  } else {
    pCtx->target = NULL;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgAddMsgCtx(SArray* pCtxs, int32_t reqType, void* out, char* target) {
  SCtgMsgCtx ctx = {0};

  ctx.reqType = reqType;
  ctx.out = out;
  if (target) {
    ctx.target = taosStrdup(target);
    if (NULL == ctx.target) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  taosArrayPush(pCtxs, &ctx);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetHashFunction(int8_t hashMethod, tableNameHashFp* fp) {
  switch (hashMethod) {
    default:
      *fp = MurmurHash3_32;
      break;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGenerateVgList(SCatalog* pCtg, SHashObj* vgHash, SArray** pList) {
  SHashObj*    vgroupHash = NULL;
  SVgroupInfo* vgInfo = NULL;
  SArray*      vgList = NULL;
  int32_t      code = 0;
  int32_t      vgNum = taosHashGetSize(vgHash);

  vgList = taosArrayInit(vgNum, sizeof(SVgroupInfo));
  if (NULL == vgList) {
    ctgError("taosArrayInit failed, num:%d", vgNum);
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  void* pIter = taosHashIterate(vgHash, NULL);
  while (pIter) {
    vgInfo = pIter;

    if (NULL == taosArrayPush(vgList, vgInfo)) {
      ctgError("taosArrayPush failed, vgId:%d", vgInfo->vgId);
      taosHashCancelIterate(vgHash, pIter);
      CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
    }

    pIter = taosHashIterate(vgHash, pIter);
  }

  *pList = vgList;

  ctgDebug("Got vgList from cache, vgNum:%d", vgNum);

  return TSDB_CODE_SUCCESS;

_return:

  if (vgList) {
    taosArrayDestroy(vgList);
  }

  CTG_RET(code);
}

int ctgVgInfoComp(const void* lp, const void* rp) {
  SVgroupInfo* pLeft = (SVgroupInfo*)lp;
  SVgroupInfo* pRight = (SVgroupInfo*)rp;
  if (pLeft->hashBegin < pRight->hashBegin) {
    return -1;
  } else if (pLeft->hashBegin > pRight->hashBegin) {
    return 1;
  }

  return 0;
}

int32_t ctgHashValueComp(void const* lp, void const* rp) {
  uint32_t*    key = (uint32_t*)lp;
  SVgroupInfo* pVg = (SVgroupInfo*)rp;

  if (*key < pVg->hashBegin) {
    return -1;
  } else if (*key > pVg->hashEnd) {
    return 1;
  }

  return 0;
}

int32_t ctgGetVgInfoFromHashValue(SCatalog* pCtg, SEpSet* pMgmtEps, SDBVgInfo* dbInfo, const SName* pTableName, SVgroupInfo* pVgroup) {
  int32_t code = 0;
  CTG_ERR_RET(ctgMakeVgArray(dbInfo));

  int32_t vgNum = taosArrayGetSize(dbInfo->vgArray);
  char    db[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pTableName, db);

  if (IS_SYS_DBNAME(pTableName->dbname)) {
    pVgroup->vgId = MNODE_HANDLE;
    if (pMgmtEps) {
      memcpy(&pVgroup->epSet, pMgmtEps, sizeof(pVgroup->epSet));
    }
    return TSDB_CODE_SUCCESS;
  }

  if (vgNum <= 0) {
    ctgError("db vgroup cache invalid, db:%s, vgroup number:%d", db, vgNum);
    CTG_ERR_RET(TSDB_CODE_TSC_DB_NOT_SELECTED);
  }

  SVgroupInfo* vgInfo = NULL;
  char         tbFullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pTableName, tbFullName);

  uint32_t hashValue = taosGetTbHashVal(tbFullName, (uint32_t)strlen(tbFullName), dbInfo->hashMethod,
                                        dbInfo->hashPrefix, dbInfo->hashSuffix);

  vgInfo = taosArraySearch(dbInfo->vgArray, &hashValue, ctgHashValueComp, TD_EQ);

  /*
    void* pIter = taosHashIterate(dbInfo->vgHash, NULL);
    while (pIter) {
      vgInfo = pIter;
      if (hashValue >= vgInfo->hashBegin && hashValue <= vgInfo->hashEnd) {
        taosHashCancelIterate(dbInfo->vgHash, pIter);
        break;
      }

      pIter = taosHashIterate(dbInfo->vgHash, pIter);
      vgInfo = NULL;
    }
  */

  if (NULL == vgInfo) {
    ctgError("no hash range found for hash value [%u], db:%s, numOfVgId:%d", hashValue, db,
             (int32_t)taosArrayGetSize(dbInfo->vgArray));
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  *pVgroup = *vgInfo;

  ctgDebug("Got tb %s hash vgroup, vgId:%d, epNum %d, current %s port %d", tbFullName, vgInfo->vgId,
           vgInfo->epSet.numOfEps, vgInfo->epSet.eps[vgInfo->epSet.inUse].fqdn,
           vgInfo->epSet.eps[vgInfo->epSet.inUse].port);

  CTG_RET(code);
}

int32_t ctgGetVgInfosFromHashValue(SCatalog* pCtg, SEpSet* pMgmgEpSet, SCtgTaskReq* tReq, SDBVgInfo* dbInfo, SCtgTbHashsCtx* pCtx,
                                   char* dbFName, SArray* pNames, bool update) {
  int32_t   code = 0;
  SCtgTask* pTask = tReq->pTask;
  SMetaRes  res = {0};
  SVgroupInfo* vgInfo = NULL;

  CTG_ERR_RET(ctgMakeVgArray(dbInfo));

  int32_t      tbNum = taosArrayGetSize(pNames);

  char* pSep = strchr(dbFName, '.');
  if (pSep && IS_SYS_DBNAME(pSep + 1)) {
    SVgroupInfo mgmtInfo = {0};
    mgmtInfo.vgId = MNODE_HANDLE;
    if (pMgmgEpSet) {
      memcpy(&mgmtInfo.epSet, pMgmgEpSet, sizeof(mgmtInfo.epSet));
    }
    for (int32_t i = 0; i < tbNum; ++i) {
      vgInfo = taosMemoryMalloc(sizeof(SVgroupInfo));
      if (NULL == vgInfo) {
        CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
      }

      memcpy(vgInfo, &mgmtInfo, sizeof(mgmtInfo));

      ctgDebug("Got tb hash vgroup, vgId:%d, epNum %d, current %s port %d", vgInfo->vgId, vgInfo->epSet.numOfEps,
               vgInfo->epSet.eps[vgInfo->epSet.inUse].fqdn, vgInfo->epSet.eps[vgInfo->epSet.inUse].port);

      if (update) {
        SCtgFetch* pFetch = taosArrayGet(pCtx->pFetchs, tReq->msgIdx);
        SMetaRes*  pRes = taosArrayGet(pCtx->pResList, pFetch->resIdx + i);
        pRes->pRes = vgInfo;
      } else {
        res.pRes = vgInfo;
        taosArrayPush(pCtx->pResList, &res);
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t vgNum = taosArrayGetSize(dbInfo->vgArray);
  if (vgNum <= 0) {
    ctgError("db vgroup cache invalid, db:%s, vgroup number:%d", dbFName, vgNum);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (1 == vgNum) {
    for (int32_t i = 0; i < tbNum; ++i) {
      vgInfo = taosMemoryMalloc(sizeof(SVgroupInfo));
      if (NULL == vgInfo) {
        CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
      }

      *vgInfo = *(SVgroupInfo*)taosArrayGet(dbInfo->vgArray, 0);

      ctgDebug("Got tb hash vgroup, vgId:%d, epNum %d, current %s port %d", vgInfo->vgId, vgInfo->epSet.numOfEps,
               vgInfo->epSet.eps[vgInfo->epSet.inUse].fqdn, vgInfo->epSet.eps[vgInfo->epSet.inUse].port);

      if (update) {
        SCtgFetch* pFetch = taosArrayGet(pCtx->pFetchs, tReq->msgIdx);
        SMetaRes*  pRes = taosArrayGet(pCtx->pResList, pFetch->resIdx + i);
        pRes->pRes = vgInfo;
      } else {
        res.pRes = vgInfo;
        taosArrayPush(pCtx->pResList, &res);
      }
    }

    return TSDB_CODE_SUCCESS;
  }

  char tbFullName[TSDB_TABLE_FNAME_LEN];
  sprintf(tbFullName, "%s.", dbFName);
  int32_t offset = strlen(tbFullName);
  SName*  pName = NULL;
  int32_t tbNameLen = 0;

  for (int32_t i = 0; i < tbNum; ++i) {
    pName = taosArrayGet(pNames, i);

    tbNameLen = offset + strlen(pName->tname);
    strcpy(tbFullName + offset, pName->tname);

    uint32_t hashValue = taosGetTbHashVal(tbFullName, (uint32_t)strlen(tbFullName), dbInfo->hashMethod,
                                          dbInfo->hashPrefix, dbInfo->hashSuffix);

    vgInfo = taosArraySearch(dbInfo->vgArray, &hashValue, ctgHashValueComp, TD_EQ);
    if (NULL == vgInfo) {
      ctgError("no hash range found for hash value [%u], db:%s, numOfVgId:%d", hashValue, dbFName,
               (int32_t)taosArrayGetSize(dbInfo->vgArray));
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    SVgroupInfo* pNewVg = taosMemoryMalloc(sizeof(SVgroupInfo));
    if (NULL == pNewVg) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    *pNewVg = *vgInfo;

    ctgDebug("Got tb %s hash vgroup, vgId:%d, epNum %d, current %s port %d", tbFullName, vgInfo->vgId,
             vgInfo->epSet.numOfEps, vgInfo->epSet.eps[vgInfo->epSet.inUse].fqdn,
             vgInfo->epSet.eps[vgInfo->epSet.inUse].port);

    if (update) {
      SCtgFetch* pFetch = taosArrayGet(pCtx->pFetchs, tReq->msgIdx);
      SMetaRes*  pRes = taosArrayGet(pCtx->pResList, pFetch->resIdx + i);
      pRes->pRes = pNewVg;
    } else {
      res.pRes = pNewVg;
      taosArrayPush(pCtx->pResList, &res);
    }
  }

  CTG_RET(code);
}

int32_t ctgGetVgIdsFromHashValue(SCatalog* pCtg, SDBVgInfo* dbInfo, char* dbFName, const char* pTbs[], int32_t tbNum,
                                 int32_t* vgId) {
  int32_t code = 0;
  CTG_ERR_RET(ctgMakeVgArray(dbInfo));

  int32_t vgNum = taosArrayGetSize(dbInfo->vgArray);

  if (vgNum <= 0) {
    ctgError("db vgroup cache invalid, db:%s, vgroup number:%d", dbFName, vgNum);
    CTG_ERR_RET(TSDB_CODE_TSC_DB_NOT_SELECTED);
  }

  SVgroupInfo* vgInfo = NULL;
  char         tbFullName[TSDB_TABLE_FNAME_LEN];
  snprintf(tbFullName, sizeof(tbFullName), "%s.", dbFName);
  int32_t offset = strlen(tbFullName);

  for (int32_t i = 0; i < tbNum; ++i) {
    snprintf(tbFullName + offset, sizeof(tbFullName) - offset, "%s", pTbs[i]);
    uint32_t hashValue = taosGetTbHashVal(tbFullName, (uint32_t)strlen(tbFullName), dbInfo->hashMethod,
                                          dbInfo->hashPrefix, dbInfo->hashSuffix);

    vgInfo = taosArraySearch(dbInfo->vgArray, &hashValue, ctgHashValueComp, TD_EQ);
    if (NULL == vgInfo) {
      ctgError("no hash range found for hash value [%u], db:%s, numOfVgId:%d", hashValue, dbFName,
               (int32_t)taosArrayGetSize(dbInfo->vgArray));
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    vgId[i] = vgInfo->vgId;

    ctgDebug("Got tb %s vgId:%d", tbFullName, vgInfo->vgId);
  }

  CTG_RET(code);
}

int32_t ctgStbVersionSearchCompare(const void* key1, const void* key2) {
  if (*(uint64_t*)key1 < ((SSTableVersion*)key2)->suid) {
    return -1;
  } else if (*(uint64_t*)key1 > ((SSTableVersion*)key2)->suid) {
    return 1;
  } else {
    return 0;
  }
}

int32_t ctgDbCacheInfoSearchCompare(const void* key1, const void* key2) {
  if (*(int64_t*)key1 < ((SDbCacheInfo*)key2)->dbId) {
    return -1;
  } else if (*(int64_t*)key1 > ((SDbCacheInfo*)key2)->dbId) {
    return 1;
  } else {
    return 0;
  }
}

int32_t ctgViewVersionSearchCompare(const void* key1, const void* key2) {
  if (*(uint64_t*)key1 < ((SViewVersion*)key2)->viewId) {
    return -1;
  } else if (*(uint64_t*)key1 > ((SViewVersion*)key2)->viewId) {
    return 1;
  } else {
    return 0;
  }
}


int32_t ctgStbVersionSortCompare(const void* key1, const void* key2) {
  if (((SSTableVersion*)key1)->suid < ((SSTableVersion*)key2)->suid) {
    return -1;
  } else if (((SSTableVersion*)key1)->suid > ((SSTableVersion*)key2)->suid) {
    return 1;
  } else {
    return 0;
  }
}

int32_t ctgDbCacheInfoSortCompare(const void* key1, const void* key2) {
  if (((SDbCacheInfo*)key1)->dbId < ((SDbCacheInfo*)key2)->dbId) {
    return -1;
  } else if (((SDbCacheInfo*)key1)->dbId > ((SDbCacheInfo*)key2)->dbId) {
    return 1;
  } else {
    return 0;
  }
}

int32_t ctgViewVersionSortCompare(const void* key1, const void* key2) {
  if (((SViewVersion*)key1)->viewId < ((SViewVersion*)key2)->viewId) {
    return -1;
  } else if (((SViewVersion*)key1)->viewId > ((SViewVersion*)key2)->viewId) {
    return 1;
  } else {
    return 0;
  }
}


int32_t ctgMakeVgArray(SDBVgInfo* dbInfo) {
  if (NULL == dbInfo) {
    return TSDB_CODE_SUCCESS;
  }

  if (dbInfo->vgHash && NULL == dbInfo->vgArray) {
    dbInfo->vgArray = taosArrayInit(100, sizeof(SVgroupInfo));
    if (NULL == dbInfo->vgArray) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    void* pIter = taosHashIterate(dbInfo->vgHash, NULL);
    while (pIter) {
      taosArrayPush(dbInfo->vgArray, pIter);
      pIter = taosHashIterate(dbInfo->vgHash, pIter);
    }

    taosArraySort(dbInfo->vgArray, ctgVgInfoComp);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgCloneVgInfo(SDBVgInfo* src, SDBVgInfo** dst) {
  CTG_ERR_RET(ctgMakeVgArray(src));

  *dst = taosMemoryMalloc(sizeof(SDBVgInfo));
  if (NULL == *dst) {
    qError("malloc %d failed", (int32_t)sizeof(SDBVgInfo));
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  memcpy(*dst, src, sizeof(SDBVgInfo));

  size_t hashSize = taosHashGetSize(src->vgHash);
  (*dst)->vgHash = taosHashInit(hashSize, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (NULL == (*dst)->vgHash) {
    qError("taosHashInit %d failed", (int32_t)hashSize);
    taosMemoryFreeClear(*dst);
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  int32_t* vgId = NULL;
  void*    pIter = taosHashIterate(src->vgHash, NULL);
  while (pIter) {
    vgId = taosHashGetKey(pIter, NULL);

    if (taosHashPut((*dst)->vgHash, (void*)vgId, sizeof(int32_t), pIter, sizeof(SVgroupInfo))) {
      qError("taosHashPut failed, hashSize:%d", (int32_t)hashSize);
      taosHashCancelIterate(src->vgHash, pIter);
      taosHashCleanup((*dst)->vgHash);
      taosMemoryFreeClear(*dst);
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    pIter = taosHashIterate(src->vgHash, pIter);
  }

  if (src->vgArray) {
    (*dst)->vgArray = taosArrayDup(src->vgArray, NULL);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgCloneMetaOutput(STableMetaOutput* output, STableMetaOutput** pOutput) {
  *pOutput = taosMemoryMalloc(sizeof(STableMetaOutput));
  if (NULL == *pOutput) {
    qError("malloc %d failed", (int32_t)sizeof(STableMetaOutput));
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  memcpy(*pOutput, output, sizeof(STableMetaOutput));

  if (output->tbMeta) {
    int32_t metaSize = CTG_META_SIZE(output->tbMeta);
    (*pOutput)->tbMeta = taosMemoryMalloc(metaSize);
    qDebug("tbMeta cloned, size:%d, p:%p", metaSize, (*pOutput)->tbMeta);
    if (NULL == (*pOutput)->tbMeta) {
      qError("malloc %d failed", (int32_t)sizeof(STableMetaOutput));
      taosMemoryFreeClear(*pOutput);
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    memcpy((*pOutput)->tbMeta, output->tbMeta, metaSize);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgCloneTableIndex(SArray* pIndex, SArray** pRes) {
  if (NULL == pIndex) {
    *pRes = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int32_t num = taosArrayGetSize(pIndex);
  *pRes = taosArrayInit(num, sizeof(STableIndexInfo));
  if (NULL == *pRes) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  for (int32_t i = 0; i < num; ++i) {
    STableIndexInfo* pInfo = taosArrayGet(pIndex, i);
    pInfo = taosArrayPush(*pRes, pInfo);
    pInfo->expr = taosStrdup(pInfo->expr);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgUpdateSendTargetInfo(SMsgSendInfo* pMsgSendInfo, int32_t msgType, char* dbFName, int32_t vgId) {
  if (msgType == TDMT_VND_TABLE_META || msgType == TDMT_VND_TABLE_CFG || msgType == TDMT_VND_BATCH_META) {
    pMsgSendInfo->target.type = TARGET_TYPE_VNODE;
    pMsgSendInfo->target.vgId = vgId;
    pMsgSendInfo->target.dbFName = taosStrdup(dbFName);
  } else {
    pMsgSendInfo->target.type = TARGET_TYPE_MNODE;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTablesReqNum(SArray* pList) {
  if (NULL == pList) {
    return 0;
  }

  int32_t total = 0;
  int32_t n = taosArrayGetSize(pList);
  for (int32_t i = 0; i < n; ++i) {
    STablesReq* pReq = taosArrayGet(pList, i);
    total += taosArrayGetSize(pReq->pTables);
  }

  return total;
}

int32_t ctgAddFetch(SArray** pFetchs, int32_t dbIdx, int32_t tbIdx, int32_t* fetchIdx, int32_t resIdx, int32_t flag) {
  if (NULL == (*pFetchs)) {
    *pFetchs = taosArrayInit(CTG_DEFAULT_FETCH_NUM, sizeof(SCtgFetch));
  }

  SCtgFetch fetch = {0};
  fetch.dbIdx = dbIdx;
  fetch.tbIdx = tbIdx;
  fetch.fetchIdx = (*fetchIdx)++;
  fetch.resIdx = resIdx;
  fetch.flag = flag;

  taosArrayPush(*pFetchs, &fetch);

  return TSDB_CODE_SUCCESS;
}

SName* ctgGetFetchName(SArray* pNames, SCtgFetch* pFetch) {
  STablesReq* pReq = (STablesReq*)taosArrayGet(pNames, pFetch->dbIdx);
  return (SName*)taosArrayGet(pReq->pTables, pFetch->tbIdx);
}

static void* ctgCloneDbVgroup(void* pSrc) { return taosArrayDup((const SArray*)pSrc, NULL); }

static void ctgFreeDbVgroup(void* p) { taosArrayDestroy((SArray*)((SMetaRes*)p)->pRes); }

void* ctgCloneDbCfgInfo(void* pSrc) {
  SDbCfgInfo* pDst = taosMemoryMalloc(sizeof(SDbCfgInfo));
  if (NULL == pDst) {
    return NULL;
  }
  memcpy(pDst, pSrc, sizeof(SDbCfgInfo));
  pDst->pRetensions = taosArrayDup(((SDbCfgInfo *)pSrc)->pRetensions, NULL);
  return pDst;
}

static void ctgFreeDbCfgInfo(void* p) { 
  SDbCfgInfo* pDst = (SDbCfgInfo *)((SMetaRes*)p)->pRes;
  freeDbCfgInfo(pDst);
}

static void* ctgCloneDbInfo(void* pSrc) {
  SDbInfo* pDst = taosMemoryMalloc(sizeof(SDbInfo));
  if (NULL == pDst) {
    return NULL;
  }
  memcpy(pDst, pSrc, sizeof(SDbInfo));
  return pDst;
}

static void ctgFreeDbInfo(void* p) { taosMemoryFree(((SMetaRes*)p)->pRes); }

static void* ctgCloneTableMeta(void* pSrc) {
  STableMeta* pMeta = pSrc;
  int32_t size = sizeof(STableMeta) + (pMeta->tableInfo.numOfColumns + pMeta->tableInfo.numOfTags) * sizeof(SSchema);
  STableMeta* pDst = taosMemoryMalloc(size);
  if (NULL == pDst) {
    return NULL;
  }
  memcpy(pDst, pSrc, size);
  return pDst;
}

static void ctgFreeTableMeta(void* p) { taosMemoryFree(((SMetaRes*)p)->pRes); }

static void* ctgCloneVgroupInfo(void* pSrc) {
  SVgroupInfo* pDst = taosMemoryMalloc(sizeof(SVgroupInfo));
  if (NULL == pDst) {
    return NULL;
  }
  memcpy(pDst, pSrc, sizeof(SVgroupInfo));
  return pDst;
}

static void ctgFreeVgroupInfo(void* p) { taosMemoryFree(((SMetaRes*)p)->pRes); }

static void* ctgCloneTableIndexs(void* pSrc) { return taosArrayDup((const SArray*)pSrc, NULL); }

static void ctgFreeTableIndexs(void* p) { taosArrayDestroy((SArray*)((SMetaRes*)p)->pRes); }

static void* ctgCloneFuncInfo(void* pSrc) {
  SFuncInfo* pDst = taosMemoryMalloc(sizeof(SFuncInfo));
  if (NULL == pDst) {
    return NULL;
  }
  memcpy(pDst, pSrc, sizeof(SFuncInfo));
  return pDst;
}

static void ctgFreeFuncInfo(void* p) { taosMemoryFree(((SMetaRes*)p)->pRes); }

static void* ctgCloneIndexInfo(void* pSrc) {
  SIndexInfo* pDst = taosMemoryMalloc(sizeof(SIndexInfo));
  if (NULL == pDst) {
    return NULL;
  }
  memcpy(pDst, pSrc, sizeof(SIndexInfo));
  return pDst;
}

static void ctgFreeIndexInfo(void* p) { taosMemoryFree(((SMetaRes*)p)->pRes); }

static void* ctgCloneUserAuth(void* pSrc) {
  bool* pDst = taosMemoryMalloc(sizeof(bool));
  if (NULL == pDst) {
    return NULL;
  }
  *pDst = *(bool*)pSrc;
  return pDst;
}

static void ctgFreeUserAuth(void* p) { taosMemoryFree(((SMetaRes*)p)->pRes); }

static void* ctgCloneQnodeList(void* pSrc) { return taosArrayDup((const SArray*)pSrc, NULL); }

static void ctgFreeQnodeList(void* p) { taosArrayDestroy((SArray*)((SMetaRes*)p)->pRes); }

static void* ctgCloneTableCfg(void* pSrc) {
  STableCfg* pDst = taosMemoryMalloc(sizeof(STableCfg));
  if (NULL == pDst) {
    return NULL;
  }
  memcpy(pDst, pSrc, sizeof(STableCfg));
  return pDst;
}

static void ctgFreeTableCfg(void* p) { taosMemoryFree(((SMetaRes*)p)->pRes); }

static void* ctgCloneDnodeList(void* pSrc) { return taosArrayDup((const SArray*)pSrc, NULL); }

static void ctgFreeDnodeList(void* p) { taosArrayDestroy((SArray*)((SMetaRes*)p)->pRes); }

static void* ctgCloneViewMeta(void* pSrc) {
  SViewMeta* pSrcMeta = pSrc;
  SViewMeta* pDst = taosMemoryMalloc(sizeof(SViewMeta));
  if (NULL == pDst) {
    return NULL;
  }
  pDst->user = tstrdup(pSrcMeta->user);
  pDst->querySql = tstrdup(pSrcMeta->querySql);
  pDst->pSchema = taosMemoryMalloc(pSrcMeta->numOfCols * sizeof(*pSrcMeta->pSchema));
  if (NULL == pDst->pSchema) {
    return pDst;
  }
  memcpy(pDst->pSchema, pSrcMeta->pSchema, pSrcMeta->numOfCols * sizeof(*pSrcMeta->pSchema));
  return pDst;
}

static void ctgFreeViewMeta(void* p) { 
  SViewMeta* pMeta = ((SMetaRes*)p)->pRes; 
  if (NULL == pMeta) {
    return;
  }
  taosMemoryFree(pMeta->user);
  taosMemoryFree(pMeta->querySql);
  taosMemoryFree(pMeta->pSchema);  
  taosMemoryFree(pMeta);
}


int32_t ctgChkSetTbAuthRes(SCatalog* pCtg, SCtgAuthReq* req, SCtgAuthRsp* res) {
  int32_t          code = 0;
  STableMeta*      pMeta = NULL;
  SGetUserAuthRsp* pInfo = &req->authInfo;
  SHashObj*        pTbs = (AUTH_TYPE_READ == req->singleType) ? pInfo->readTbs : pInfo->writeTbs;
  char*            stbName = NULL;

  char tbFName[TSDB_TABLE_FNAME_LEN];
  char dbFName[TSDB_DB_FNAME_LEN];
  tNameExtractFullName(&req->pRawReq->tbName, tbFName);
  tNameGetFullDbName(&req->pRawReq->tbName, dbFName);

  while (true) {
    taosMemoryFreeClear(pMeta);

    char* pCond = taosHashGet(pTbs, tbFName, strlen(tbFName) + 1);
    if (pCond) {
      if (strlen(pCond) > 1) {
        CTG_ERR_JRET(nodesStringToNode(pCond, &res->pRawRes->pCond[AUTH_RES_BASIC]));
      }

      res->pRawRes->pass[AUTH_RES_BASIC] = true;
      goto _return;
    }

    if (stbName) {
      res->pRawRes->pass[AUTH_RES_BASIC] = false;
      goto _return;
    }

    CTG_ERR_JRET(catalogGetCachedTableMeta(pCtg, &req->pRawReq->tbName, &pMeta));
    if (NULL == pMeta) {
      if (req->onlyCache) {
        res->metaNotExists = true;
        ctgDebug("db %s tb %s meta not in cache for auth", req->pRawReq->tbName.dbname, req->pRawReq->tbName.tname);
        goto _return;
      }

      SCtgTbMetaCtx ctx = {0};
      ctx.pName = (SName*)&req->pRawReq->tbName;
      ctx.flag = CTG_FLAG_UNKNOWN_STB | CTG_FLAG_SYNC_OP;

      CTG_ERR_JRET(ctgGetTbMeta(pCtg, req->pConn, &ctx, &pMeta));
    }

    if (TSDB_SUPER_TABLE == pMeta->tableType || TSDB_NORMAL_TABLE == pMeta->tableType) {
      res->pRawRes->pass[AUTH_RES_BASIC] = false;
      goto _return;
    }

    if (TSDB_CHILD_TABLE == pMeta->tableType) {
      CTG_ERR_JRET(ctgGetCachedStbNameFromSuid(pCtg, dbFName, pMeta->suid, &stbName));
      if (NULL == stbName) {
        if (req->onlyCache) {
          res->metaNotExists = true;
          ctgDebug("suid %" PRIu64 " name not in cache for auth", pMeta->suid);
          goto _return;
        }

        continue;
      }

      sprintf(tbFName, "%s.%s", dbFName, stbName);
      continue;
    }

    ctgError("Invalid table type %d for %s", pMeta->tableType, tbFName);
    CTG_ERR_JRET(TSDB_CODE_INVALID_PARA);
  }

_return:

  taosMemoryFree(pMeta);
  taosMemoryFree(stbName);

  CTG_RET(code);
}

int32_t ctgChkSetBasicAuthRes(SCatalog* pCtg, SCtgAuthReq* req, SCtgAuthRsp* res) {
  int32_t          code = 0;
  SUserAuthInfo*   pReq = req->pRawReq;
  SUserAuthRes*    pRes = res->pRawRes;
  SGetUserAuthRsp* pInfo = &req->authInfo;

  pRes->pass[AUTH_RES_BASIC] = false;
  pRes->pCond[AUTH_RES_BASIC] = NULL;

  if (!pInfo->enable) {
    return TSDB_CODE_SUCCESS;
  }

  if (pInfo->superAuth) {
    pRes->pass[AUTH_RES_BASIC] = true;
    return TSDB_CODE_SUCCESS;
  }

  if (IS_SYS_DBNAME(pReq->tbName.dbname)) {
    pRes->pass[AUTH_RES_BASIC] = true;
    ctgDebug("sysdb %s, pass", pReq->tbName.dbname);
    return TSDB_CODE_SUCCESS;
  }

  if (req->tbNotExists) {
    //pRes->pass[AUTH_RES_BASIC] = true;
    //return TSDB_CODE_SUCCESS;
    pReq->tbName.type = TSDB_DB_NAME_T;
  }

  char dbFName[TSDB_DB_FNAME_LEN];
  tNameGetFullDbName(&pReq->tbName, dbFName);

  // since that we add read/write previliges when create db, there is no need to check createdDbs
#if 0
  if (pInfo->createdDbs && taosHashGet(pInfo->createdDbs, dbFName, strlen(dbFName))) {
    pRes->pass = true;
    return TSDB_CODE_SUCCESS;
  }
#endif

  switch (pReq->type) {
    case AUTH_TYPE_READ: {
      if (pReq->tbName.type == TSDB_TABLE_NAME_T && pInfo->readTbs && taosHashGetSize(pInfo->readTbs) > 0) {
        req->singleType = AUTH_TYPE_READ;
        CTG_ERR_RET(ctgChkSetTbAuthRes(pCtg, req, res));
        if (pRes->pass[AUTH_RES_BASIC] || res->metaNotExists) {
          return TSDB_CODE_SUCCESS;
        }
      }

      if (pInfo->readDbs && taosHashGet(pInfo->readDbs, dbFName, strlen(dbFName) + 1)) {
        pRes->pass[AUTH_RES_BASIC] = true;
        return TSDB_CODE_SUCCESS;
      }

      break;
    }
    case AUTH_TYPE_WRITE: {
      if (pReq->tbName.type == TSDB_TABLE_NAME_T && pInfo->writeTbs && taosHashGetSize(pInfo->writeTbs) > 0) {
        req->singleType = AUTH_TYPE_WRITE;
        CTG_ERR_RET(ctgChkSetTbAuthRes(pCtg, req, res));
        if (pRes->pass[AUTH_RES_BASIC] || res->metaNotExists) {
          return TSDB_CODE_SUCCESS;
        }
      }

      if (pInfo->writeDbs && taosHashGet(pInfo->writeDbs, dbFName, strlen(dbFName) + 1)) {
        pRes->pass[AUTH_RES_BASIC] = true;
        return TSDB_CODE_SUCCESS;
      }

      break;
    }
    case AUTH_TYPE_READ_OR_WRITE: {
      if ((pInfo->readDbs && taosHashGet(pInfo->readDbs, dbFName, strlen(dbFName) + 1)) ||
          (pInfo->writeDbs && taosHashGet(pInfo->writeDbs, dbFName, strlen(dbFName) + 1)) ||
          (pInfo->useDbs && taosHashGet(pInfo->useDbs, dbFName, strlen(dbFName) + 1))) {
        pRes->pass[AUTH_RES_BASIC] = true;
        return TSDB_CODE_SUCCESS;
      }

      break;
    }
    default:
      break;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgChkSetViewAuthRes(SCatalog* pCtg, SCtgAuthReq* req, SCtgAuthRsp* res) {
  int32_t          code = 0;
  SUserAuthInfo*   pReq = req->pRawReq;
  SUserAuthRes*    pRes = res->pRawRes;
  SGetUserAuthRsp* pInfo = &req->authInfo;

  pRes->pass[AUTH_RES_VIEW] = false;
  pRes->pCond[AUTH_RES_VIEW] = NULL;

  if (!pInfo->enable) {
    return TSDB_CODE_SUCCESS;
  }

  if (pInfo->superAuth) {
    pRes->pass[AUTH_RES_VIEW] = true;
    return TSDB_CODE_SUCCESS;
  }

  if (pReq->tbName.type != TSDB_TABLE_NAME_T) {
    return TSDB_CODE_SUCCESS;
  }

  char viewFName[TSDB_VIEW_FNAME_LEN];
  if (IS_SYS_DBNAME(req->pRawReq->tbName.dbname)) {
    snprintf(viewFName, sizeof(viewFName), "%s.%s", req->pRawReq->tbName.dbname, req->pRawReq->tbName.tname);
  } else {
    tNameExtractFullName(&req->pRawReq->tbName, viewFName);
  }
  int32_t len = strlen(viewFName) + 1;

  switch (pReq->type) {
    case AUTH_TYPE_READ: {
      char *value = taosHashGet(pInfo->readViews, viewFName, len);
      if (NULL != value) {
        pRes->pass[AUTH_RES_VIEW] = true;
        return TSDB_CODE_SUCCESS;
      }
      break;
    }
    case AUTH_TYPE_WRITE: {
      char *value = taosHashGet(pInfo->writeViews, viewFName, len);
      if (NULL != value) {
        pRes->pass[AUTH_RES_VIEW] = true;
        return TSDB_CODE_SUCCESS;
      }
      break;
    }
    case AUTH_TYPE_ALTER: {
      char *value = taosHashGet(pInfo->alterViews, viewFName, len);
      if (NULL != value) {
        pRes->pass[AUTH_RES_VIEW] = true;
        return TSDB_CODE_SUCCESS;
      }
      break;
    }
    default:
      break;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgChkSetAuthRes(SCatalog* pCtg, SCtgAuthReq* req, SCtgAuthRsp* res) {
#ifdef TD_ENTERPRISE
  CTG_ERR_RET(ctgChkSetViewAuthRes(pCtg, req, res));
  if (req->pRawReq->isView) {
    return TSDB_CODE_SUCCESS;
  }
#endif
  CTG_RET(ctgChkSetBasicAuthRes(pCtg, req, res));
}

#if 0
static int32_t ctgCloneMetaDataArray(SArray* pSrc, __array_item_dup_fn_t copyFunc, SArray** pDst) {
  if (NULL == pSrc) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t size = taosArrayGetSize(pSrc);
  *pDst = taosArrayInit(size, sizeof(SMetaRes));
  if (NULL == *pDst) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  for (int32_t i = 0; i < size; ++i) {
    SMetaRes* pRes = taosArrayGet(pSrc, i);
    SMetaRes  res = {.code = pRes->code, .pRes = copyFunc(pRes->pRes)};
    if (NULL == res.pRes) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    taosArrayPush(*pDst, &res);
  }

  return TSDB_CODE_SUCCESS;
}

SMetaData* catalogCloneMetaData(SMetaData* pData) {
  SMetaData* pRes = taosMemoryCalloc(1, sizeof(SMetaData));
  if (NULL == pRes) {
    return NULL;
  }

  int32_t code = ctgCloneMetaDataArray(pData->pDbVgroup, ctgCloneDbVgroup, &pRes->pDbVgroup);
  if (TSDB_CODE_SUCCESS == code) {
    code = ctgCloneMetaDataArray(pData->pDbCfg, ctgCloneDbCfgInfo, &pRes->pDbCfg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = ctgCloneMetaDataArray(pData->pDbInfo, ctgCloneDbInfo, &pRes->pDbInfo);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = ctgCloneMetaDataArray(pData->pTableMeta, ctgCloneTableMeta, &pRes->pTableMeta);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = ctgCloneMetaDataArray(pData->pTableHash, ctgCloneVgroupInfo, &pRes->pTableHash);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = ctgCloneMetaDataArray(pData->pTableIndex, ctgCloneTableIndices, &pRes->pTableIndex);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = ctgCloneMetaDataArray(pData->pUdfList, ctgCloneFuncInfo, &pRes->pUdfList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = ctgCloneMetaDataArray(pData->pIndex, ctgCloneIndexInfo, &pRes->pIndex);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = ctgCloneMetaDataArray(pData->pUser, ctgCloneUserAuth, &pRes->pUser);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = ctgCloneMetaDataArray(pData->pQnodeList, ctgCloneQnodeList, &pRes->pQnodeList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = ctgCloneMetaDataArray(pData->pTableCfg, ctgCloneTableCfg, &pRes->pTableCfg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = ctgCloneMetaDataArray(pData->pDnodeList, ctgCloneDnodeList, &pRes->pDnodeList);
  }

  if (TSDB_CODE_SUCCESS != code) {
    catalogFreeMetaData(pRes);
    return NULL;
  }

  return pRes;
}
#endif

void ctgDestroySMetaData(SMetaData* pData) {
  if (NULL == pData) {
    return;
  }

  taosArrayDestroyEx(pData->pDbVgroup, ctgFreeDbVgroup);
  taosArrayDestroyEx(pData->pDbCfg, ctgFreeDbCfgInfo);
  taosArrayDestroyEx(pData->pDbInfo, ctgFreeDbInfo);
  taosArrayDestroyEx(pData->pTableMeta, ctgFreeTableMeta);
  taosArrayDestroyEx(pData->pTableHash, ctgFreeVgroupInfo);
  taosArrayDestroyEx(pData->pTableIndex, ctgFreeTableIndexs);
  taosArrayDestroyEx(pData->pUdfList, ctgFreeFuncInfo);
  taosArrayDestroyEx(pData->pIndex, ctgFreeIndexInfo);
  taosArrayDestroyEx(pData->pUser, ctgFreeUserAuth);
  taosArrayDestroyEx(pData->pQnodeList, ctgFreeQnodeList);
  taosArrayDestroyEx(pData->pTableCfg, ctgFreeTableCfg);
  taosArrayDestroyEx(pData->pDnodeList, ctgFreeDnodeList);
  taosArrayDestroyEx(pData->pView, ctgFreeViewMeta);
  taosMemoryFreeClear(pData->pSvrVer);
}

uint64_t ctgGetTbIndexCacheSize(STableIndex *pIndex) {
  if (NULL == pIndex) {
    return 0;
  }

  return sizeof(*pIndex) + pIndex->indexSize;
}

uint64_t ctgGetViewMetaCacheSize(SViewMeta *pMeta) {
  if (NULL == pMeta) {
    return 0;
  }

  return sizeof(*pMeta) + strlen(pMeta->querySql) + 1 + strlen(pMeta->user) + 1 + pMeta->numOfCols * sizeof(SSchema);
}


FORCE_INLINE uint64_t ctgGetTbMetaCacheSize(STableMeta *pMeta) {
  if (NULL == pMeta) {
    return 0;
  }

  switch (pMeta->tableType) {
    case TSDB_SUPER_TABLE:
      return sizeof(*pMeta) + (pMeta->tableInfo.numOfColumns + pMeta->tableInfo.numOfTags) * sizeof(SSchema);
    case TSDB_CHILD_TABLE:
      return sizeof(SCTableMeta);
    default:
      return sizeof(*pMeta) + pMeta->tableInfo.numOfColumns * sizeof(SSchema);
  }

  return 0;
}

uint64_t ctgGetDbVgroupCacheSize(SDBVgInfo *pVg) {
  if (NULL == pVg) {
    return 0;
  }

  return sizeof(*pVg) + taosHashGetSize(pVg->vgHash) * (sizeof(SVgroupInfo) + sizeof(int32_t)) 
    + taosArrayGetSize(pVg->vgArray) * sizeof(SVgroupInfo);
}

uint64_t ctgGetUserCacheSize(SGetUserAuthRsp *pAuth) {
  if (NULL == pAuth) {
    return 0;
  }

  uint64_t cacheSize = 0;
  char* p = taosHashIterate(pAuth->createdDbs, NULL);
  while (p != NULL) {
    size_t len = 0;
    void*  key = taosHashGetKey(p, &len);
    cacheSize += len + strlen(p) + 1;

    p = taosHashIterate(pAuth->createdDbs, p);
  }

  p = taosHashIterate(pAuth->readDbs, NULL);
  while (p != NULL) {
    size_t len = 0;
    void*  key = taosHashGetKey(p, &len);
    cacheSize += len + strlen(p) + 1;

    p = taosHashIterate(pAuth->readDbs, p);
  }  

  p = taosHashIterate(pAuth->writeDbs, NULL);
  while (p != NULL) {
    size_t len = 0;
    void*  key = taosHashGetKey(p, &len);
    cacheSize += len + strlen(p) + 1;

    p = taosHashIterate(pAuth->writeDbs, p);
  } 

  p = taosHashIterate(pAuth->readTbs, NULL);
  while (p != NULL) {
    size_t len = 0;
    void*  key = taosHashGetKey(p, &len);
    cacheSize += len + strlen(p) + 1;

    p = taosHashIterate(pAuth->readTbs, p);
  } 

  p = taosHashIterate(pAuth->writeTbs, NULL);
  while (p != NULL) {
    size_t len = 0;
    void*  key = taosHashGetKey(p, &len);
    cacheSize += len + strlen(p) + 1;

    p = taosHashIterate(pAuth->writeTbs, p);
  } 

  p = taosHashIterate(pAuth->alterTbs, NULL);
  while (p != NULL) {
    size_t len = 0;
    void*  key = taosHashGetKey(p, &len);
    cacheSize += len + strlen(p) + 1;

    p = taosHashIterate(pAuth->alterTbs, p);
  } 

  p = taosHashIterate(pAuth->readViews, NULL);
  while (p != NULL) {
    size_t len = 0;
    void*  key = taosHashGetKey(p, &len);
    cacheSize += len + strlen(p) + 1;

    p = taosHashIterate(pAuth->readViews, p);
  } 

  p = taosHashIterate(pAuth->writeViews, NULL);
  while (p != NULL) {
    size_t len = 0;
    void*  key = taosHashGetKey(p, &len);
    cacheSize += len + strlen(p) + 1;

    p = taosHashIterate(pAuth->writeViews, p);
  } 

  p = taosHashIterate(pAuth->alterViews, NULL);
  while (p != NULL) {
    size_t len = 0;
    void*  key = taosHashGetKey(p, &len);
    cacheSize += len + strlen(p) + 1;

    p = taosHashIterate(pAuth->alterViews, p);
  } 

  int32_t *ref = taosHashIterate(pAuth->useDbs, NULL);
  while (ref != NULL) {
    size_t len = 0;
    void*  key = taosHashGetKey(ref, &len);
    cacheSize += len + sizeof(*ref);

    ref = taosHashIterate(pAuth->useDbs, ref);
  } 

  return cacheSize;
}

uint64_t ctgGetClusterCacheSize(SCatalog *pCtg) {
  uint64_t cacheSize = sizeof(SCatalog);
  
  SCtgUserAuth* pAuth = taosHashIterate(pCtg->userCache, NULL);
  while (pAuth != NULL) {
    size_t len = 0;
    void*  key = taosHashGetKey(pAuth, &len);
    cacheSize += len + sizeof(SCtgUserAuth) + atomic_load_64(&pAuth->userCacheSize);

    pAuth = taosHashIterate(pCtg->userCache, pAuth);
  }

  SCtgDBCache* pDb = taosHashIterate(pCtg->dbCache, NULL);
  while (pDb != NULL) {
    size_t len = 0;
    void*  key = taosHashGetKey(pDb, &len);
    cacheSize += len + sizeof(SCtgDBCache) + atomic_load_64(&pDb->dbCacheSize);

    pDb = taosHashIterate(pCtg->dbCache, pDb);
  }

  cacheSize += pCtg->dbRent.rentCacheSize;
  cacheSize += pCtg->stbRent.rentCacheSize;
  cacheSize += pCtg->viewRent.rentCacheSize;

  return cacheSize;
}

void ctgGetClusterCacheStat(SCatalog* pCtg) {
  for (int32_t i = 0; i < CTG_CI_MAX_VALUE; ++i) {
    if (0 == (gCtgStatItem[i].flag & CTG_CI_FLAG_LEVEL_DB)) {
      continue;
    }

    pCtg->cacheStat.cacheNum[i] = 0;
  }

  SCtgDBCache* dbCache = NULL;
  void*        pIter = taosHashIterate(pCtg->dbCache, NULL);
  while (pIter) {
    dbCache = (SCtgDBCache*)pIter;

    for (int32_t i = 0; i < CTG_CI_MAX_VALUE; ++i) {
      if (0 == (gCtgStatItem[i].flag & CTG_CI_FLAG_LEVEL_DB)) {
        continue;
      }

      pCtg->cacheStat.cacheNum[i] += dbCache->dbCacheNum[i];
    }

    pIter = taosHashIterate(pCtg->dbCache, pIter);
  }
}

void ctgSummaryClusterCacheStat(SCatalog* pCtg) {
  for (int32_t i = 0; i < CTG_CI_MAX_VALUE; ++i) {
    if (gCtgStatItem[i].flag & CTG_CI_FLAG_LEVEL_GLOBAL) {
      continue;
    }

    gCtgMgmt.statInfo.cache.cacheNum[i] += pCtg->cacheStat.cacheNum[i];
    gCtgMgmt.statInfo.cache.cacheHit[i] += pCtg->cacheStat.cacheHit[i];
    gCtgMgmt.statInfo.cache.cacheNHit[i] += pCtg->cacheStat.cacheNHit[i];
  }
}

void ctgGetGlobalCacheStat(SCtgCacheStat* pStat) {
  for (int32_t i = 0; i < CTG_CI_MAX_VALUE; ++i) {
    if (gCtgStatItem[i].flag & CTG_CI_FLAG_LEVEL_GLOBAL) {
      continue;
    }

    gCtgMgmt.statInfo.cache.cacheNum[i] = 0;
    gCtgMgmt.statInfo.cache.cacheHit[i] = 0;
    gCtgMgmt.statInfo.cache.cacheNHit[i] = 0;
  }

  SCatalog* pCtg = NULL;
  void*     pIter = taosHashIterate(gCtgMgmt.pCluster, NULL);
  while (pIter) {
    pCtg = *(SCatalog**)pIter;

    if (pCtg) {
      ctgGetClusterCacheStat(pCtg);
      ctgSummaryClusterCacheStat(pCtg);
    }

    pIter = taosHashIterate(gCtgMgmt.pCluster, pIter);
  }

  memcpy(pStat, &gCtgMgmt.statInfo.cache, sizeof(gCtgMgmt.statInfo.cache));
}

void ctgGetGlobalCacheSize(uint64_t *pSize) {
  *pSize = 0;

  SCatalog* pCtg = NULL;
  void*     pIter = taosHashIterate(gCtgMgmt.pCluster, NULL);
  while (pIter) {
    size_t len = 0;
    void*  key = taosHashGetKey(pIter, &len);
    *pSize += len + POINTER_BYTES; 
    
    pCtg = *(SCatalog**)pIter;
    if (pCtg) {
      *pSize += ctgGetClusterCacheSize(pCtg);
    }

    pIter = taosHashIterate(gCtgMgmt.pCluster, pIter);
  }
}

int32_t ctgBuildViewNullRes(SCtgTask* pTask, SCtgViewsCtx* pCtx) {
  SCatalog* pCtg = pTask->pJob->pCtg;
  int32_t dbNum = taosArrayGetSize(pCtx->pNames);
  for (int32_t i = 0; i < dbNum; ++i) {
    STablesReq* pReq = taosArrayGet(pCtx->pNames, i);
    int32_t viewNum = taosArrayGetSize(pReq->pTables);
    
    ctgDebug("start to check views in db %s, viewNum %d", pReq->dbFName, viewNum);
    
    for (int32_t m = 0; m < viewNum; ++m) {
      taosArrayPush(pCtx->pResList, &(SMetaData){0});
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t dupViewMetaFromRsp(SViewMetaRsp* pRsp, SViewMeta* pViewMeta) {
  pViewMeta->querySql = tstrdup(pRsp->querySql);
  if (NULL == pViewMeta->querySql) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }
  pViewMeta->user = tstrdup(pRsp->user);
  if (NULL == pViewMeta->user) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }
  pViewMeta->version = pRsp->version;
  pViewMeta->viewId = pRsp->viewId;
  pViewMeta->precision = pRsp->precision;
  pViewMeta->type = pRsp->type;
  pViewMeta->numOfCols = pRsp->numOfCols;
  pViewMeta->pSchema = taosMemoryMalloc(pViewMeta->numOfCols * sizeof(SSchema));
  if (pViewMeta->pSchema == NULL) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }
  memcpy(pViewMeta->pSchema, pRsp->pSchema, pViewMeta->numOfCols * sizeof(SSchema));

  return TSDB_CODE_SUCCESS;
}



