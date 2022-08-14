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
#include "systable.h"

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

void ctgFreeBatch(SCtgBatch *pBatch) {
  if (NULL == pBatch) {
    return;
  }
  
  taosArrayDestroyEx(pBatch->pMsgs, ctgFreeBatchMsg);
  taosArrayDestroy(pBatch->pTaskIds);
}

void ctgFreeBatchs(SHashObj *pBatchs) {
  void* p = taosHashIterate(pBatchs, NULL);
  while (NULL != p) {
    SCtgBatch* pBatch = (SCtgBatch*)p;

    ctgFreeBatch(pBatch);

    p = taosHashIterate(pBatchs, p);
  }

  taosHashCleanup(pBatchs);
}

char *ctgTaskTypeStr(CTG_TASK_TYPE type) {
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
    case CTG_TASK_GET_TB_INDEX:
      return "[get table index]";
    case CTG_TASK_GET_TB_CFG:
      return "[get table cfg]";
    case CTG_TASK_GET_INDEX:
      return "[get index]";
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
    default:
      return "unknown";
  }
}

void ctgFreeQNode(SCtgQNode *node) {
  //TODO
}

void ctgFreeSTableIndex(void *info) {
  if (NULL == info) {
    return;
  }

  STableIndex *pInfo = (STableIndex *)info;

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

  taosMemoryFreeClear(pData->pSvrVer);
}

void ctgFreeSCtgUserAuth(SCtgUserAuth *userCache) {
  taosHashCleanup(userCache->createdDbs);
  taosHashCleanup(userCache->readDbs);
  taosHashCleanup(userCache->writeDbs);
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

  taosMemoryFreeClear(mgmt->slots);
}

void ctgFreeStbMetaCache(SCtgDBCache *dbCache) {
  if (NULL == dbCache->stbCache) {
    return;
  }

  int32_t stbNum = taosHashGetSize(dbCache->stbCache);  
  taosHashCleanup(dbCache->stbCache);
  dbCache->stbCache = NULL;
  CTG_CACHE_STAT_DEC(numOfStb, stbNum);
}

void ctgFreeTbCacheImpl(SCtgTbCache *pCache) {
  qDebug("tbMeta freed, p:%p", pCache->pMeta);
  taosMemoryFreeClear(pCache->pMeta);
  if (pCache->pIndex) {
    taosArrayDestroyEx(pCache->pIndex->pIndex, tFreeSTableIndexInfo);
    taosMemoryFreeClear(pCache->pIndex);
  }
}

void ctgFreeTbCache(SCtgDBCache *dbCache) {
  if (NULL == dbCache->tbCache) {
    return;
  }

  int32_t tblNum = taosHashGetSize(dbCache->tbCache);
  SCtgTbCache *pCache = taosHashIterate(dbCache->tbCache, NULL);
  while (NULL != pCache) {
    ctgFreeTbCacheImpl(pCache);
    pCache = taosHashIterate(dbCache->tbCache, pCache);
  }
  taosHashCleanup(dbCache->tbCache);
  dbCache->tbCache = NULL;
  CTG_CACHE_STAT_DEC(numOfTbl, tblNum);
}

void ctgFreeVgInfo(SDBVgInfo *vgInfo) {
  if (NULL == vgInfo) {
    return;
  }

  if (vgInfo->vgHash) {
    taosHashCleanup(vgInfo->vgHash);
    vgInfo->vgHash = NULL;
  }
  
  taosMemoryFreeClear(vgInfo);
}

void ctgFreeVgInfoCache(SCtgDBCache *dbCache) {
  ctgFreeVgInfo(dbCache->vgCache.vgInfo);
}

void ctgFreeDbCache(SCtgDBCache *dbCache) {
  if (NULL == dbCache) {
    return;
  }

  ctgFreeVgInfoCache(dbCache);
  ctgFreeStbMetaCache(dbCache);
  ctgFreeTbCache(dbCache);
}

void ctgFreeInstDbCache(SHashObj* pDbCache) {
  if (NULL == pDbCache) {
    return;
  }
  
  int32_t dbNum = taosHashGetSize(pDbCache);
  
  void *pIter = taosHashIterate(pDbCache, NULL);
  while (pIter) {
    SCtgDBCache *dbCache = pIter;
    atomic_store_8(&dbCache->deleted, 1);
    ctgFreeDbCache(dbCache);
          
    pIter = taosHashIterate(pDbCache, pIter);
  }  

  taosHashCleanup(pDbCache);
  
  CTG_CACHE_STAT_DEC(numOfDb, dbNum);
}

void ctgFreeInstUserCache(SHashObj* pUserCache) {
  if (NULL == pUserCache) {
    return;
  }
  
  int32_t userNum = taosHashGetSize(pUserCache);
  
  void *pIter = taosHashIterate(pUserCache, NULL);
  while (pIter) {
    SCtgUserAuth *userCache = pIter;
    ctgFreeSCtgUserAuth(userCache);
  
    pIter = taosHashIterate(pUserCache, pIter);
  }  
  
  taosHashCleanup(pUserCache);
  
  CTG_CACHE_STAT_DEC(numOfUser, userNum);
}

void ctgFreeHandleImpl(SCatalog* pCtg) {
  ctgFreeMetaRent(&pCtg->dbRent);
  ctgFreeMetaRent(&pCtg->stbRent);

  ctgFreeInstDbCache(pCtg->dbCache);
  ctgFreeInstUserCache(pCtg->userCache);

  taosMemoryFree(pCtg);
}


void ctgFreeHandle(SCatalog* pCtg) {
  if (NULL == pCtg) {
    return;
  }

  uint64_t clusterId = pCtg->clusterId;

  ctgFreeMetaRent(&pCtg->dbRent);
  ctgFreeMetaRent(&pCtg->stbRent);

  ctgFreeInstDbCache(pCtg->dbCache);
  ctgFreeInstUserCache(pCtg->userCache);

  CTG_CACHE_STAT_DEC(numOfCluster, 1);

  taosMemoryFree(pCtg);

  ctgInfo("handle freed, clusterId:0x%" PRIx64, clusterId);
}

void ctgClearHandle(SCatalog* pCtg) {
  if (NULL == pCtg) {
    return;
  }

  uint64_t clusterId = pCtg->clusterId;

  ctgFreeMetaRent(&pCtg->dbRent);
  ctgFreeMetaRent(&pCtg->stbRent);

  ctgFreeInstDbCache(pCtg->dbCache);
  ctgFreeInstUserCache(pCtg->userCache);

  ctgMetaRentInit(&pCtg->dbRent, gCtgMgmt.cfg.dbRentSec, CTG_RENT_DB);
  ctgMetaRentInit(&pCtg->stbRent, gCtgMgmt.cfg.stbRentSec, CTG_RENT_STABLE);
  
  pCtg->dbCache = taosHashInit(gCtgMgmt.cfg.maxDBCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == pCtg->dbCache) {
    qError("taosHashInit %d dbCache failed", CTG_DEFAULT_CACHE_DB_NUMBER);
  }
  
  pCtg->userCache = taosHashInit(gCtgMgmt.cfg.maxUserCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == pCtg->userCache) {
    ctgError("taosHashInit %d user cache failed", gCtgMgmt.cfg.maxUserCacheNum);
  }

  CTG_CACHE_STAT_INC(numOfClear, 1);

  ctgInfo("handle cleared, clusterId:0x%" PRIx64, clusterId);
}

void ctgFreeSUseDbOutput(SUseDbOutput* pOutput) {
  if (NULL == pOutput) {
    return;
  }

  if (pOutput->dbVgroup) {
    taosHashCleanup(pOutput->dbVgroup->vgHash);
    taosMemoryFreeClear(pOutput->dbVgroup);
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
    case TDMT_MND_USE_DB:{
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
      taosMemoryFreeClear(pCtx->out);
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


void ctgFreeTaskRes(CTG_TASK_TYPE type, void **pRes) {
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
    case CTG_TASK_GET_TB_INDEX: {
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
    case CTG_TASK_GET_TB_HASH:
    case CTG_TASK_GET_DB_INFO:
    case CTG_TASK_GET_INDEX:
    case CTG_TASK_GET_UDF: 
    case CTG_TASK_GET_USER: 
    case CTG_TASK_GET_SVR_VER:
    case CTG_TASK_GET_TB_META: {
      taosMemoryFreeClear(*pRes);
      break;
    }
    case CTG_TASK_GET_TB_META_BATCH: {
      SArray* pArray = (SArray*)*pRes;
      int32_t num = taosArrayGetSize(pArray);
      for (int32_t i = 0; i < num; ++i) {
        ctgFreeBatchMeta(taosArrayGet(pArray, i));
      }
      *pRes = NULL; // no need to free it
      break;
    }
    case CTG_TASK_GET_TB_HASH_BATCH: {
      SArray* pArray = (SArray*)*pRes;
      int32_t num = taosArrayGetSize(pArray);
      for (int32_t i = 0; i < num; ++i) {
        ctgFreeBatchHash(taosArrayGet(pArray, i));
      }
      *pRes = NULL; // no need to free it
      break;
    }    
    default:
      qError("invalid task type %d", type);
      break;
  }
}


void ctgFreeSubTaskRes(CTG_TASK_TYPE type, void **pRes) {
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
        taosHashCleanup(pInfo->vgHash);
        taosMemoryFreeClear(*pRes);
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
    case CTG_TASK_GET_TB_INDEX: {
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
    case CTG_TASK_GET_INDEX: 
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


void ctgClearSubTaskRes(SCtgSubRes *pRes) {
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
    case CTG_TASK_GET_TB_INDEX: {
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
    case CTG_TASK_GET_DB_VGROUP:
    case CTG_TASK_GET_DB_CFG:
    case CTG_TASK_GET_DB_INFO:    
    case CTG_TASK_GET_INDEX:
    case CTG_TASK_GET_UDF:
    case CTG_TASK_GET_QNODE:    
    case CTG_TASK_GET_USER: {
      taosMemoryFreeClear(pTask->taskCtx);
      break;
    }
    default:
      qError("invalid task type %d", pTask->type);
      break;
  }
}


void ctgFreeTask(SCtgTask* pTask) {
  ctgFreeMsgCtx(&pTask->msgCtx);
  ctgFreeTaskRes(pTask->type, &pTask->res);
  ctgFreeTaskCtx(pTask);

  taosArrayDestroy(pTask->pParents);
  ctgClearSubTaskRes(&pTask->subRes);
}

void ctgFreeTasks(SArray* pArray) {
  if (NULL == pArray) {
    return;
  }

  int32_t num = taosArrayGetSize(pArray);
  for (int32_t i = 0; i < num; ++i) {
    SCtgTask* pTask = taosArrayGet(pArray, i);
    ctgFreeTask(pTask);
  }

  taosArrayDestroy(pArray);
}

void ctgFreeJob(void* job) {
  if (NULL == job) {
    return;
  }
  
  SCtgJob* pJob = (SCtgJob*)job;

  int64_t rid = pJob->refId;
  uint64_t qid = pJob->queryId;

  ctgFreeTasks(pJob->pTasks);
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
    pCtx->target = strdup(target);
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
    ctx.target = strdup(target);
    if (NULL == ctx.target) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  taosArrayPush(pCtxs, &ctx);

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

int32_t ctgGenerateVgList(SCatalog *pCtg, SHashObj *vgHash, SArray** pList) {
  SHashObj *vgroupHash = NULL;
  SVgroupInfo *vgInfo = NULL;
  SArray *vgList = NULL;
  int32_t code = 0;
  int32_t vgNum = taosHashGetSize(vgHash);

  vgList = taosArrayInit(vgNum, sizeof(SVgroupInfo));
  if (NULL == vgList) {
    ctgError("taosArrayInit failed, num:%d", vgNum);
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);    
  }

  void *pIter = taosHashIterate(vgHash, NULL);
  while (pIter) {
    vgInfo = pIter;

    if (NULL == taosArrayPush(vgList, vgInfo)) {
      ctgError("taosArrayPush failed, vgId:%d", vgInfo->vgId);
      taosHashCancelIterate(vgHash, pIter);      
      CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
    }
    
    pIter = taosHashIterate(vgHash, pIter);
    vgInfo = NULL;
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


int32_t ctgGetVgInfoFromHashValue(SCatalog *pCtg, SDBVgInfo *dbInfo, const SName *pTableName, SVgroupInfo *pVgroup) {
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

  ctgDebug("Got tb %s hash vgroup, vgId:%d, epNum %d, current %s port %d", tbFullName, vgInfo->vgId, vgInfo->epSet.numOfEps,
    vgInfo->epSet.eps[vgInfo->epSet.inUse].fqdn, vgInfo->epSet.eps[vgInfo->epSet.inUse].port);

  CTG_RET(code);
}

int32_t ctgHashValueComp(void const *lp, void const *rp) {
  uint32_t *key = (uint32_t *)lp;
  SVgroupInfo *pVg = *(SVgroupInfo **)rp;

  if (*key < pVg->hashBegin) {
    return -1;
  } else if (*key > pVg->hashEnd) {
    return 1;
  }

  return 0;
}

int ctgVgInfoComp(const void* lp, const void* rp) {
  SVgroupInfo *pLeft = *(SVgroupInfo **)lp;
  SVgroupInfo *pRight = *(SVgroupInfo **)rp;
  if (pLeft->hashBegin < pRight->hashBegin) {
    return -1;
  } else if (pLeft->hashBegin > pRight->hashBegin) {
    return 1;
  }

  return 0;
}


int32_t ctgGetVgInfosFromHashValue(SCatalog *pCtg, SCtgTaskReq* tReq, SDBVgInfo *dbInfo, SCtgTbHashsCtx *pCtx, char* dbFName, SArray* pNames, bool update) {
  int32_t code = 0;
  SCtgTask* pTask = tReq->pTask;
  SMetaRes res = {0};
  int32_t vgNum = taosHashGetSize(dbInfo->vgHash);
  if (vgNum <= 0) {
    ctgError("db vgroup cache invalid, db:%s, vgroup number:%d", dbFName, vgNum);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  tableNameHashFp fp = NULL;
  SVgroupInfo *vgInfo = NULL;

  CTG_ERR_RET(ctgGetHashFunction(dbInfo->hashMethod, &fp));

  int32_t tbNum = taosArrayGetSize(pNames);

  if (1 == vgNum) {
    void *pIter = taosHashIterate(dbInfo->vgHash, NULL);
    for (int32_t i = 0; i < tbNum; ++i) {
      vgInfo = taosMemoryMalloc(sizeof(SVgroupInfo));
      if (NULL == vgInfo) {
        taosHashCancelIterate(dbInfo->vgHash, pIter);
        CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
      }

      *vgInfo = *(SVgroupInfo*)pIter;

      ctgDebug("Got tb hash vgroup, vgId:%d, epNum %d, current %s port %d", vgInfo->vgId, vgInfo->epSet.numOfEps,
        vgInfo->epSet.eps[vgInfo->epSet.inUse].fqdn, vgInfo->epSet.eps[vgInfo->epSet.inUse].port);

      if (update) {
        SCtgFetch* pFetch = taosArrayGet(pCtx->pFetchs, tReq->msgIdx);
        SMetaRes *pRes = taosArrayGet(pCtx->pResList, pFetch->resIdx + i);
        pRes->pRes = vgInfo;
      } else {
        res.pRes = vgInfo;
        taosArrayPush(pCtx->pResList, &res);
      }
    }

    taosHashCancelIterate(dbInfo->vgHash, pIter);
    return TSDB_CODE_SUCCESS;
  }

  SArray* pVgList = taosArrayInit(vgNum, POINTER_BYTES);
  void *pIter = taosHashIterate(dbInfo->vgHash, NULL);
  while (pIter) {
    taosArrayPush(pVgList, &pIter);
    pIter = taosHashIterate(dbInfo->vgHash, pIter);
  }

  taosArraySort(pVgList, ctgVgInfoComp);

  char tbFullName[TSDB_TABLE_FNAME_LEN];
  sprintf(tbFullName, "%s.", dbFName);
  int32_t offset = strlen(tbFullName);
  SName* pName = NULL;
  int32_t tbNameLen = 0;
  
  for (int32_t i = 0; i < tbNum; ++i) {
    pName = taosArrayGet(pNames, i);

    tbNameLen = offset + strlen(pName->tname);
    strcpy(tbFullName + offset, pName->tname);

    uint32_t hashValue = (*fp)(tbFullName, (uint32_t)tbNameLen);

    SVgroupInfo **p = taosArraySearch(pVgList, &hashValue, ctgHashValueComp, TD_EQ);

    if (NULL == p) {
      ctgError("no hash range found for hash value [%u], db:%s, numOfVgId:%d", hashValue, dbFName, taosHashGetSize(dbInfo->vgHash));
      taosArrayDestroy(pVgList);
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    vgInfo = *p;

    SVgroupInfo* pNewVg = taosMemoryMalloc(sizeof(SVgroupInfo));
    if (NULL == pNewVg) {
      taosArrayDestroy(pVgList);
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    *pNewVg = *vgInfo;

    ctgDebug("Got tb %s hash vgroup, vgId:%d, epNum %d, current %s port %d", tbFullName, vgInfo->vgId, vgInfo->epSet.numOfEps,
             vgInfo->epSet.eps[vgInfo->epSet.inUse].fqdn, vgInfo->epSet.eps[vgInfo->epSet.inUse].port);

    if (update) {
      SCtgFetch* pFetch = taosArrayGet(pCtx->pFetchs, tReq->msgIdx);
      SMetaRes *pRes = taosArrayGet(pCtx->pResList, pFetch->resIdx + i);
      pRes->pRes = pNewVg;
    } else {
      res.pRes = pNewVg;
      taosArrayPush(pCtx->pResList, &res);
    }    
  }

  taosArrayDestroy(pVgList);

  CTG_RET(code);
}


int32_t ctgStbVersionSearchCompare(const void* key1, const void* key2) {
  if (*(uint64_t *)key1 < ((SSTableVersion*)key2)->suid) {
    return -1;
  } else if (*(uint64_t *)key1 > ((SSTableVersion*)key2)->suid) {
    return 1;
  } else {
    return 0;
  }
}

int32_t ctgDbVgVersionSearchCompare(const void* key1, const void* key2) {
  if (*(int64_t *)key1 < ((SDbVgVersion*)key2)->dbId) {
    return -1;
  } else if (*(int64_t *)key1 > ((SDbVgVersion*)key2)->dbId) {
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

int32_t ctgDbVgVersionSortCompare(const void* key1, const void* key2) {
  if (((SDbVgVersion*)key1)->dbId < ((SDbVgVersion*)key2)->dbId) {
    return -1;
  } else if (((SDbVgVersion*)key1)->dbId > ((SDbVgVersion*)key2)->dbId) {
    return 1;
  } else {
    return 0;
  }
}




int32_t ctgCloneVgInfo(SDBVgInfo *src, SDBVgInfo **dst) {
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

  int32_t *vgId = NULL;
  void *pIter = taosHashIterate(src->vgHash, NULL);
  while (pIter) {
    vgId = taosHashGetKey(pIter, NULL);

    if (taosHashPut((*dst)->vgHash, (void *)vgId, sizeof(int32_t), pIter, sizeof(SVgroupInfo))) {
      qError("taosHashPut failed, hashSize:%d", (int32_t)hashSize);
      taosHashCancelIterate(src->vgHash, pIter);
      taosHashCleanup((*dst)->vgHash);
      taosMemoryFreeClear(*dst);
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    
    pIter = taosHashIterate(src->vgHash, pIter);
  }


  return TSDB_CODE_SUCCESS;
}



int32_t ctgCloneMetaOutput(STableMetaOutput *output, STableMetaOutput **pOutput) {
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
    STableIndexInfo *pInfo = taosArrayGet(pIndex, i);
    pInfo = taosArrayPush(*pRes, pInfo);
    pInfo->expr = strdup(pInfo->expr);
  }

  return TSDB_CODE_SUCCESS;
}


int32_t ctgUpdateSendTargetInfo(SMsgSendInfo *pMsgSendInfo, int32_t msgType, char* dbFName, int32_t vgId) {
  if (msgType == TDMT_VND_TABLE_META || msgType == TDMT_VND_TABLE_CFG || msgType == TDMT_VND_BATCH_META) {
    pMsgSendInfo->target.type = TARGET_TYPE_VNODE;
    pMsgSendInfo->target.vgId = vgId;
    pMsgSendInfo->target.dbFName = strdup(dbFName);
  } else {
    pMsgSendInfo->target.type = TARGET_TYPE_MNODE;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTablesReqNum(SArray *pList) {
  if (NULL == pList) {
    return 0;
  }

  int32_t total = 0;
  int32_t n = taosArrayGetSize(pList);
  for (int32_t i = 0; i < n; ++i) {
    STablesReq *pReq = taosArrayGet(pList, i);
    total += taosArrayGetSize(pReq->pTables);
  }

  return total;
}

int32_t ctgAddFetch(SArray** pFetchs, int32_t dbIdx, int32_t tbIdx, int32_t *fetchIdx, int32_t resIdx, int32_t flag) {
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


