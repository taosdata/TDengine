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
#include "tref.h"

int32_t ctgInitGetTbMetaTask(SCtgJob *pJob, int32_t taskIdx, SName *name) {
  SCtgTask *pTask = taosArrayGet(pJob->pTasks, taskIdx);

  pTask->type = CTG_TASK_GET_TB_META;
  pTask->taskId = taskIdx;
  pTask->pJob = pJob;

  pTask->taskCtx = taosMemoryCalloc(1, sizeof(SCtgTbMetaCtx));
  if (NULL == pTask->taskCtx) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCtgTbMetaCtx* ctx = pTask->taskCtx;
  ctx->pName = taosMemoryMalloc(sizeof(*name));
  if (NULL == ctx->pName) {
    taosMemoryFree(pTask->taskCtx);
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }
  
  memcpy(ctx->pName, name, sizeof(*name));
  ctx->flag = CTG_FLAG_UNKNOWN_STB;

  qDebug("QID:%" PRIx64 " task %d type %d initialized, tableName:%s", pJob->queryId, taskIdx, pTask->type, name->tname);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetDbVgTask(SCtgJob *pJob, int32_t taskIdx, char *dbFName) {
  SCtgTask *pTask = taosArrayGet(pJob->pTasks, taskIdx);

  pTask->type = CTG_TASK_GET_DB_VGROUP;
  pTask->taskId = taskIdx;
  pTask->pJob = pJob;

  pTask->taskCtx = taosMemoryCalloc(1, sizeof(SCtgDbVgCtx));
  if (NULL == pTask->taskCtx) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCtgDbVgCtx* ctx = pTask->taskCtx;
  
  memcpy(ctx->dbFName, dbFName, sizeof(ctx->dbFName));

  qDebug("QID:%" PRIx64 " task %d type %d initialized, dbFName:%s", pJob->queryId, taskIdx, pTask->type, dbFName);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetDbCfgTask(SCtgJob *pJob, int32_t taskIdx, char *dbFName) {
  SCtgTask *pTask = taosArrayGet(pJob->pTasks, taskIdx);

  pTask->type = CTG_TASK_GET_DB_CFG;
  pTask->taskId = taskIdx;
  pTask->pJob = pJob;

  pTask->taskCtx = taosMemoryCalloc(1, sizeof(SCtgDbCfgCtx));
  if (NULL == pTask->taskCtx) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCtgDbCfgCtx* ctx = pTask->taskCtx;
  
  memcpy(ctx->dbFName, dbFName, sizeof(ctx->dbFName));

  qDebug("QID:%" PRIx64 " task %d type %d initialized, dbFName:%s", pJob->queryId, taskIdx, pTask->type, dbFName);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetTbHashTask(SCtgJob *pJob, int32_t taskIdx, SName *name) {
  SCtgTask *pTask = taosArrayGet(pJob->pTasks, taskIdx);

  pTask->type = CTG_TASK_GET_TB_HASH;
  pTask->taskId = taskIdx;
  pTask->pJob = pJob;

  pTask->taskCtx = taosMemoryCalloc(1, sizeof(SCtgTbHashCtx));
  if (NULL == pTask->taskCtx) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCtgTbHashCtx* ctx = pTask->taskCtx;
  ctx->pName = taosMemoryMalloc(sizeof(*name));
  if (NULL == ctx->pName) {
    taosMemoryFree(pTask->taskCtx);
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }
  
  memcpy(ctx->pName, name, sizeof(*name));
  tNameGetFullDbName(ctx->pName, ctx->dbFName);

  qDebug("QID:%" PRIx64 " task %d type %d initialized, tableName:%s", pJob->queryId, taskIdx, pTask->type, name->tname);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetQnodeTask(SCtgJob *pJob, int32_t taskIdx) {
  SCtgTask *pTask = taosArrayGet(pJob->pTasks, taskIdx);

  pTask->type = CTG_TASK_GET_QNODE;
  pTask->taskId = taskIdx;
  pTask->pJob = pJob;
  pTask->taskCtx = NULL;

  qDebug("QID:%" PRIx64 " task %d type %d initialized", pJob->queryId, taskIdx, pTask->type);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetIndexTask(SCtgJob *pJob, int32_t taskIdx, char *name) {
  SCtgTask *pTask = taosArrayGet(pJob->pTasks, taskIdx);

  pTask->type = CTG_TASK_GET_INDEX;
  pTask->taskId = taskIdx;
  pTask->pJob = pJob;

  pTask->taskCtx = taosMemoryCalloc(1, sizeof(SCtgIndexCtx));
  if (NULL == pTask->taskCtx) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCtgIndexCtx* ctx = pTask->taskCtx;
  
  strcpy(ctx->indexFName, name);

  qDebug("QID:%" PRIx64 " task %d type %d initialized, indexFName:%s", pJob->queryId, taskIdx, pTask->type, name);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetUdfTask(SCtgJob *pJob, int32_t taskIdx, char *name) {
  SCtgTask *pTask = taosArrayGet(pJob->pTasks, taskIdx);

  pTask->type = CTG_TASK_GET_UDF;
  pTask->taskId = taskIdx;
  pTask->pJob = pJob;

  pTask->taskCtx = taosMemoryCalloc(1, sizeof(SCtgUdfCtx));
  if (NULL == pTask->taskCtx) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCtgUdfCtx* ctx = pTask->taskCtx;
  
  strcpy(ctx->udfName, name);

  qDebug("QID:%" PRIx64 " task %d type %d initialized, udfName:%s", pJob->queryId, taskIdx, pTask->type, name);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetUserTask(SCtgJob *pJob, int32_t taskIdx, SUserAuthInfo *user) {
  SCtgTask *pTask = taosArrayGet(pJob->pTasks, taskIdx);

  pTask->type = CTG_TASK_GET_USER;
  pTask->taskId = taskIdx;
  pTask->pJob = pJob;

  pTask->taskCtx = taosMemoryCalloc(1, sizeof(SCtgUserCtx));
  if (NULL == pTask->taskCtx) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCtgUserCtx* ctx = pTask->taskCtx;
  
  memcpy(&ctx->user, user, sizeof(*user));

  qDebug("QID:%" PRIx64 " task %d type %d initialized, user:%s", pJob->queryId, taskIdx, pTask->type, user->user);

  return TSDB_CODE_SUCCESS;
}


int32_t ctgInitJob(CTG_PARAMS, SCtgJob** job, uint64_t reqId, const SCatalogReq* pReq, catalogCallback fp, void* param) {
  int32_t code = 0;
  int32_t tbMetaNum = (int32_t)taosArrayGetSize(pReq->pTableMeta);
  int32_t dbVgNum = (int32_t)taosArrayGetSize(pReq->pDbVgroup);
  int32_t tbHashNum = (int32_t)taosArrayGetSize(pReq->pTableHash);
  int32_t udfNum = (int32_t)taosArrayGetSize(pReq->pUdf);
  int32_t qnodeNum = pReq->qNodeRequired ? 1 : 0;
  int32_t dbCfgNum = (int32_t)taosArrayGetSize(pReq->pDbCfg);
  int32_t indexNum = (int32_t)taosArrayGetSize(pReq->pIndex);
  int32_t userNum = (int32_t)taosArrayGetSize(pReq->pUser);

  int32_t taskNum = tbMetaNum + dbVgNum + udfNum + tbHashNum + qnodeNum + dbCfgNum + indexNum + userNum;
  if (taskNum <= 0) {
    ctgError("empty input for job, taskNum:%d", taskNum);
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }
  
  *job = taosMemoryCalloc(1, sizeof(SCtgJob));
  if (NULL == *job) {
    ctgError("calloc %d failed", (int32_t)sizeof(SCtgJob));
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCtgJob *pJob = *job;
  
  pJob->queryId = reqId;
  pJob->userFp = fp;
  pJob->pCtg    = pCtg;
  pJob->pTrans    = pTrans;
  pJob->pMgmtEps    = pMgmtEps;
  pJob->userParam = param;
  
  pJob->tbMetaNum = tbMetaNum;
  pJob->tbHashNum = tbHashNum;
  pJob->qnodeNum = qnodeNum;
  pJob->dbVgNum = dbVgNum;
  pJob->udfNum = udfNum;
  pJob->dbCfgNum = dbCfgNum;
  pJob->indexNum = indexNum;
  pJob->userNum = userNum;
  
  pJob->pTasks = taosArrayInit(taskNum, sizeof(SCtgTask));
  
  if (NULL == pJob->pTasks) {
    ctgError("taosArrayInit %d tasks failed", taskNum);
    CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  int32_t taskIdx = 0;
  for (int32_t i = 0; i < dbVgNum; ++i) {
    char *dbFName = taosArrayGet(pReq->pDbVgroup, i);
    CTG_ERR_JRET(ctgInitGetDbVgTask(pJob, taskIdx++, dbFName));
  }

  for (int32_t i = 0; i < dbCfgNum; ++i) {
    char *dbFName = taosArrayGet(pReq->pDbCfg, i);
    CTG_ERR_JRET(ctgInitGetDbCfgTask(pJob, taskIdx++, dbFName));
  }

  for (int32_t i = 0; i < tbMetaNum; ++i) {
    SName *name = taosArrayGet(pReq->pTableMeta, i);
    CTG_ERR_JRET(ctgInitGetTbMetaTask(pJob, taskIdx++, name));
  }

  for (int32_t i = 0; i < tbHashNum; ++i) {
    SName *name = taosArrayGet(pReq->pTableHash, i);
    CTG_ERR_JRET(ctgInitGetTbHashTask(pJob, taskIdx++, name));
  }

  for (int32_t i = 0; i < indexNum; ++i) {
    char *indexName = taosArrayGet(pReq->pIndex, i);
    CTG_ERR_JRET(ctgInitGetIndexTask(pJob, taskIdx++, indexName));
  }

  for (int32_t i = 0; i < udfNum; ++i) {
    char *udfName = taosArrayGet(pReq->pUdf, i);
    CTG_ERR_JRET(ctgInitGetUdfTask(pJob, taskIdx++, udfName));
  }

  for (int32_t i = 0; i < userNum; ++i) {
    SUserAuthInfo *user = taosArrayGet(pReq->pUser, i);
    CTG_ERR_JRET(ctgInitGetUserTask(pJob, taskIdx++, user));
  }

  if (qnodeNum) {
    CTG_ERR_JRET(ctgInitGetQnodeTask(pJob, taskIdx++));
  }

  pJob->refId = taosAddRef(gCtgMgmt.jobPool, pJob);
  if (pJob->refId < 0) {
    ctgError("add job to ref failed, error: %s", tstrerror(terrno));
    CTG_ERR_JRET(terrno);
  }

  taosAcquireRef(gCtgMgmt.jobPool, pJob->refId);

  qDebug("QID:%" PRIx64 ", job %" PRIx64 " initialized, task num %d", pJob->queryId, pJob->refId, taskNum);

  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFreeClear(*job);

  CTG_RET(code);
}

int32_t ctgDumpTbMetaRes(SCtgTask* pTask) {
  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pTableMeta) {
    pJob->jobRes.pTableMeta = taosArrayInit(pJob->tbMetaNum, sizeof(STableMeta));
    if (NULL == pJob->jobRes.pTableMeta) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  taosArrayPush(pJob->jobRes.pTableMeta, pTask->res);

  taosMemoryFreeClear(pTask->res);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpDbVgRes(SCtgTask* pTask) {
  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pDbVgroup) {
    pJob->jobRes.pDbVgroup = taosArrayInit(pJob->dbVgNum, POINTER_BYTES);
    if (NULL == pJob->jobRes.pDbVgroup) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  taosArrayPush(pJob->jobRes.pDbVgroup, &pTask->res);
  pTask->res = NULL;

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpTbHashRes(SCtgTask* pTask) {
  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pTableHash) {
    pJob->jobRes.pTableHash = taosArrayInit(pJob->tbHashNum, sizeof(SVgroupInfo));
    if (NULL == pJob->jobRes.pTableHash) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  taosArrayPush(pJob->jobRes.pTableHash, &pTask->res);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpIndexRes(SCtgTask* pTask) {
  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pIndex) {
    pJob->jobRes.pIndex = taosArrayInit(pJob->indexNum, sizeof(SIndexInfo));
    if (NULL == pJob->jobRes.pIndex) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  taosArrayPush(pJob->jobRes.pIndex, pTask->res);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpQnodeRes(SCtgTask* pTask) {
  SCtgJob* pJob = pTask->pJob;

  TSWAP(pJob->jobRes.pQnodeList, pTask->res); 

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpDbCfgRes(SCtgTask* pTask) {
  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pDbCfg) {
    pJob->jobRes.pDbCfg = taosArrayInit(pJob->dbCfgNum, sizeof(SDbCfgInfo));
    if (NULL == pJob->jobRes.pDbCfg) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  taosArrayPush(pJob->jobRes.pDbCfg, &pTask->res);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpUdfRes(SCtgTask* pTask) {
  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pUdfList) {
    pJob->jobRes.pUdfList = taosArrayInit(pJob->udfNum, sizeof(SFuncInfo));
    if (NULL == pJob->jobRes.pUdfList) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  taosArrayPush(pJob->jobRes.pUdfList, pTask->res);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpUserRes(SCtgTask* pTask) {
  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pUser) {
    pJob->jobRes.pUser = taosArrayInit(pJob->userNum, sizeof(bool));
    if (NULL == pJob->jobRes.pUser) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  taosArrayPush(pJob->jobRes.pUser, pTask->res);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgHandleTaskEnd(SCtgTask* pTask, int32_t rspCode) {
  SCtgJob* pJob = pTask->pJob;
  int32_t code = 0;

  qDebug("QID:%" PRIx64 " task %d end with rsp %s", pJob->queryId, pTask->taskId, tstrerror(rspCode));

  if (rspCode) {
    int32_t lastCode = atomic_val_compare_exchange_32(&pJob->rspCode, 0, rspCode);
    if (0 == lastCode) {
      CTG_ERR_JRET(rspCode);
    }

    return TSDB_CODE_SUCCESS;  
  }

  int32_t taskDone = atomic_add_fetch_32(&pJob->taskDone, 1);
  if (taskDone < taosArrayGetSize(pJob->pTasks)) {
    qDebug("task done: %d, total: %d", taskDone, (int32_t)taosArrayGetSize(pJob->pTasks));
    return TSDB_CODE_SUCCESS;
  }

  CTG_ERR_JRET(ctgMakeAsyncRes(pJob));

_return:

  qDebug("QID:%" PRIx64 " user callback with rsp %s", pJob->queryId, tstrerror(code));

  (*pJob->userFp)(&pJob->jobRes, pJob->userParam, code);

  taosRemoveRef(gCtgMgmt.jobPool, pJob->refId);
  
  CTG_RET(code);
}

int32_t ctgHandleGetTbMetaRsp(SCtgTask* pTask, int32_t reqType, const SDataBuf *pMsg, int32_t rspCode) {
  int32_t code = 0;
  SCtgDBCache *dbCache = NULL;
  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  SCtgTbMetaCtx* ctx = (SCtgTbMetaCtx*)pTask->taskCtx;
  SCatalog* pCtg = pTask->pJob->pCtg; 
  void *pTrans = pTask->pJob->pTrans; 
  const SEpSet* pMgmtEps = pTask->pJob->pMgmtEps;

  switch (reqType) {
    case TDMT_MND_USE_DB: {
      SUseDbOutput* pOut = (SUseDbOutput*)pTask->msgCtx.out;

      SVgroupInfo vgInfo = {0};
      CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, pOut->dbVgroup, ctx->pName, &vgInfo));

      ctgDebug("will refresh tbmeta, not supposed to be stb, tbName:%s, flag:%d", tNameGetTableName(ctx->pName), ctx->flag);

      CTG_ERR_JRET(ctgGetTbMetaFromVnode(CTG_PARAMS_LIST(), ctx->pName, &vgInfo, NULL, pTask));

      return TSDB_CODE_SUCCESS;
    }
    case TDMT_MND_TABLE_META: {
      STableMetaOutput* pOut = (STableMetaOutput*)pTask->msgCtx.out;
      
      if (CTG_IS_META_NULL(pOut->metaType)) {
        if (CTG_FLAG_IS_STB(ctx->flag)) {
          char dbFName[TSDB_DB_FNAME_LEN] = {0};
          tNameGetFullDbName(ctx->pName, dbFName);
          
          CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, dbFName, &dbCache));
          if (NULL != dbCache) {
            SVgroupInfo vgInfo = {0};
            CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, dbCache->vgInfo, ctx->pName, &vgInfo));
          
            ctgDebug("will refresh tbmeta, not supposed to be stb, tbName:%s, flag:%d", tNameGetTableName(ctx->pName), ctx->flag);
          
            CTG_ERR_JRET(ctgGetTbMetaFromVnode(CTG_PARAMS_LIST(), ctx->pName, &vgInfo, NULL, pTask));

            ctgReleaseVgInfo(dbCache);
            ctgReleaseDBCache(pCtg, dbCache);
          } else {
            SBuildUseDBInput input = {0};
          
            tstrncpy(input.db, dbFName, tListLen(input.db));
            input.vgVersion = CTG_DEFAULT_INVALID_VERSION;
          
            CTG_ERR_JRET(ctgGetDBVgInfoFromMnode(pCtg, pTrans, pMgmtEps, &input, NULL, pTask));
          }

          return TSDB_CODE_SUCCESS;
        }
        
        ctgError("no tbmeta got, tbNmae:%s", tNameGetTableName(ctx->pName));
        ctgRemoveTbMetaFromCache(pCtg, ctx->pName, false);
        
        CTG_ERR_JRET(CTG_ERR_CODE_TABLE_NOT_EXIST);
      }

      if (pTask->msgCtx.lastOut) {
        TSWAP(pTask->msgCtx.out, pTask->msgCtx.lastOut);
        STableMetaOutput* pLastOut = (STableMetaOutput*)pTask->msgCtx.out;
        TSWAP(pLastOut->tbMeta, pOut->tbMeta);
      }
      
      break;
    }
    case TDMT_VND_TABLE_META: {
      STableMetaOutput* pOut = (STableMetaOutput*)pTask->msgCtx.out;
      
      if (CTG_IS_META_NULL(pOut->metaType)) {
        ctgError("no tbmeta got, tbNmae:%s", tNameGetTableName(ctx->pName));
        ctgRemoveTbMetaFromCache(pCtg, ctx->pName, false);
        CTG_ERR_JRET(CTG_ERR_CODE_TABLE_NOT_EXIST);
      }

      if (CTG_FLAG_IS_STB(ctx->flag)) {
        break;
      }
      
      if (CTG_IS_META_TABLE(pOut->metaType) && TSDB_SUPER_TABLE == pOut->tbMeta->tableType) {
        ctgDebug("will continue to refresh tbmeta since got stb, tbName:%s", tNameGetTableName(ctx->pName));
      
        taosMemoryFreeClear(pOut->tbMeta);
        
        CTG_ERR_JRET(ctgGetTbMetaFromMnode(CTG_PARAMS_LIST(), ctx->pName, NULL, pTask));
      } else if (CTG_IS_META_BOTH(pOut->metaType)) {
        int32_t exist = 0;
        if (!CTG_FLAG_IS_FORCE_UPDATE(ctx->flag)) {
          CTG_ERR_JRET(ctgTbMetaExistInCache(pCtg, pOut->dbFName, pOut->tbName, &exist));
        }
      
        if (0 == exist) {
          TSWAP(pTask->msgCtx.lastOut, pTask->msgCtx.out);
          CTG_ERR_JRET(ctgGetTbMetaFromMnodeImpl(CTG_PARAMS_LIST(), pOut->dbFName, pOut->tbName, NULL, pTask));
        } else {
          taosMemoryFreeClear(pOut->tbMeta);
          
          SET_META_TYPE_CTABLE(pOut->metaType); 
        }
      }
      break;
    }
    default:
      ctgError("invalid reqType %d", reqType);
      CTG_ERR_JRET(TSDB_CODE_INVALID_MSG);
      break;
  }

  STableMetaOutput* pOut = (STableMetaOutput*)pTask->msgCtx.out;

  ctgUpdateTbMetaToCache(pCtg, pOut, false);

  if (CTG_IS_META_BOTH(pOut->metaType)) {
    memcpy(pOut->tbMeta, &pOut->ctbMeta, sizeof(pOut->ctbMeta));
  } else if (CTG_IS_META_CTABLE(pOut->metaType)) {
    SName stbName = *ctx->pName;
    strcpy(stbName.tname, pOut->tbName);
    SCtgTbMetaCtx stbCtx = {0};
    stbCtx.flag = ctx->flag;
    stbCtx.pName = &stbName;
    
    CTG_ERR_JRET(ctgReadTbMetaFromCache(pCtg, &stbCtx, &pOut->tbMeta));
    if (NULL == pOut->tbMeta) {
      ctgDebug("stb no longer exist, stbName:%s", stbName.tname);
      CTG_ERR_JRET(ctgRelaunchGetTbMetaTask(pTask));

      return TSDB_CODE_SUCCESS;
    }

    memcpy(pOut->tbMeta, &pOut->ctbMeta, sizeof(pOut->ctbMeta));
  }

  TSWAP(pTask->res, pOut->tbMeta);

_return:

  if (dbCache) {
    ctgReleaseVgInfo(dbCache);
    ctgReleaseDBCache(pCtg, dbCache);
  }

  ctgHandleTaskEnd(pTask, code);

  CTG_RET(code);
}

int32_t ctgHandleGetDbVgRsp(SCtgTask* pTask, int32_t reqType, const SDataBuf *pMsg, int32_t rspCode) {
  int32_t code = 0;
  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  SCtgDbVgCtx* ctx = (SCtgDbVgCtx*)pTask->taskCtx;
  SCatalog* pCtg = pTask->pJob->pCtg; 
  void *pTrans = pTask->pJob->pTrans; 
  const SEpSet* pMgmtEps = pTask->pJob->pMgmtEps;

  switch (reqType) {
    case TDMT_MND_USE_DB: {
      SUseDbOutput* pOut = (SUseDbOutput*)pTask->msgCtx.out;

      CTG_ERR_JRET(ctgGenerateVgList(pCtg, pOut->dbVgroup->vgHash, (SArray**)&pTask->res));
      
      CTG_ERR_JRET(ctgPutUpdateVgToQueue(pCtg, ctx->dbFName, pOut->dbId, pOut->dbVgroup, false));
      pOut->dbVgroup = NULL;

      break;
    }
    default:
      ctgError("invalid reqType %d", reqType);
      CTG_ERR_JRET(TSDB_CODE_INVALID_MSG);
      break;
  }


_return:

  ctgHandleTaskEnd(pTask, code);

  CTG_RET(code);
}

int32_t ctgHandleGetTbHashRsp(SCtgTask* pTask, int32_t reqType, const SDataBuf *pMsg, int32_t rspCode) {
  int32_t code = 0;
  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  SCtgTbHashCtx* ctx = (SCtgTbHashCtx*)pTask->taskCtx;
  SCatalog* pCtg = pTask->pJob->pCtg; 
  void *pTrans = pTask->pJob->pTrans; 
  const SEpSet* pMgmtEps = pTask->pJob->pMgmtEps;

  switch (reqType) {
    case TDMT_MND_USE_DB: {
      SUseDbOutput* pOut = (SUseDbOutput*)pTask->msgCtx.out;

      pTask->res = taosMemoryMalloc(sizeof(SVgroupInfo));
      if (NULL == pTask->res) {
        CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
      }

      CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, pOut->dbVgroup, ctx->pName, (SVgroupInfo*)pTask->res));
      
      CTG_ERR_JRET(ctgPutUpdateVgToQueue(pCtg, ctx->dbFName, pOut->dbId, pOut->dbVgroup, false));
      pOut->dbVgroup = NULL;

      break;
    }
    default:
      ctgError("invalid reqType %d", reqType);
      CTG_ERR_JRET(TSDB_CODE_INVALID_MSG);
      break;
  }


_return:

  ctgHandleTaskEnd(pTask, code);

  CTG_RET(code);
}

int32_t ctgHandleGetDbCfgRsp(SCtgTask* pTask, int32_t reqType, const SDataBuf *pMsg, int32_t rspCode) {
  int32_t code = 0;
  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  TSWAP(pTask->res, pTask->msgCtx.out);
  
_return:

  ctgHandleTaskEnd(pTask, code);

  CTG_RET(code);
}

int32_t ctgHandleGetQnodeRsp(SCtgTask* pTask, int32_t reqType, const SDataBuf *pMsg, int32_t rspCode) {
  int32_t code = 0;
  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  TSWAP(pTask->res, pTask->msgCtx.out);
  
_return:

  ctgHandleTaskEnd(pTask, code);

  CTG_RET(code);
}

int32_t ctgHandleGetIndexRsp(SCtgTask* pTask, int32_t reqType, const SDataBuf *pMsg, int32_t rspCode) {
  int32_t code = 0;
  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  TSWAP(pTask->res, pTask->msgCtx.out);
  
_return:

  ctgHandleTaskEnd(pTask, code);

  CTG_RET(code);
}

int32_t ctgHandleGetUdfRsp(SCtgTask* pTask, int32_t reqType, const SDataBuf *pMsg, int32_t rspCode) {
  int32_t code = 0;
  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  TSWAP(pTask->res, pTask->msgCtx.out);
  
_return:

  ctgHandleTaskEnd(pTask, code);

  CTG_RET(code);
}

int32_t ctgHandleGetUserRsp(SCtgTask* pTask, int32_t reqType, const SDataBuf *pMsg, int32_t rspCode) {
  int32_t code = 0;
  SCtgDBCache *dbCache = NULL;
  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  SCtgUserCtx* ctx = (SCtgUserCtx*)pTask->taskCtx;
  SCatalog* pCtg = pTask->pJob->pCtg; 
  void *pTrans = pTask->pJob->pTrans; 
  const SEpSet* pMgmtEps = pTask->pJob->pMgmtEps;
  bool pass = false;
  SGetUserAuthRsp* pOut = (SGetUserAuthRsp*)pTask->msgCtx.out;
  
  if (pOut->superAuth) {
    pass = true;
    goto _return;
  }

  if (pOut->createdDbs && taosHashGet(pOut->createdDbs, ctx->user.dbFName, strlen(ctx->user.dbFName))) {
    pass = true;
    goto _return;
  }

  if (ctx->user.type == AUTH_TYPE_READ && pOut->readDbs && taosHashGet(pOut->readDbs, ctx->user.dbFName, strlen(ctx->user.dbFName))) {
    pass = true;
  } else if (ctx->user.type == AUTH_TYPE_WRITE && pOut->writeDbs && taosHashGet(pOut->writeDbs, ctx->user.dbFName, strlen(ctx->user.dbFName))) {
    pass = true;
  }

_return:

  if (TSDB_CODE_SUCCESS == code) {
    pTask->res = taosMemoryCalloc(1, sizeof(bool));
    if (NULL == pTask->res) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      *(bool*)pTask->res = pass;
    }
  }

  ctgPutUpdateUserToQueue(pCtg, pOut, false);
  pTask->msgCtx.out = NULL;

  ctgHandleTaskEnd(pTask, code);

  CTG_RET(code);
}

int32_t ctgAsyncRefreshTbMeta(SCtgTask *pTask) {
  SCatalog* pCtg = pTask->pJob->pCtg; 
  void *pTrans = pTask->pJob->pTrans; 
  const SEpSet* pMgmtEps = pTask->pJob->pMgmtEps;
  int32_t code = 0;
  SCtgTbMetaCtx* ctx = (SCtgTbMetaCtx*)pTask->taskCtx;

  if (CTG_FLAG_IS_SYS_DB(ctx->flag)) {
    ctgDebug("will refresh sys db tbmeta, tbName:%s", tNameGetTableName(ctx->pName));

    CTG_RET(ctgGetTbMetaFromMnodeImpl(CTG_PARAMS_LIST(), (char *)ctx->pName->dbname, (char *)ctx->pName->tname, NULL, pTask));
  }

  if (CTG_FLAG_IS_STB(ctx->flag)) {
    ctgDebug("will refresh tbmeta, supposed to be stb, tbName:%s", tNameGetTableName(ctx->pName));

    // if get from mnode failed, will not try vnode
    CTG_RET(ctgGetTbMetaFromMnode(CTG_PARAMS_LIST(), ctx->pName, NULL, pTask));
  }

  SCtgDBCache *dbCache = NULL;
  char dbFName[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(ctx->pName, dbFName);
  
  CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, dbFName, &dbCache));
  if (NULL == dbCache) {
    SVgroupInfo vgInfo = {0};
    CTG_ERR_RET(ctgGetVgInfoFromHashValue(pCtg, dbCache->vgInfo, ctx->pName, &vgInfo));

    ctgDebug("will refresh tbmeta, not supposed to be stb, tbName:%s, flag:%d", tNameGetTableName(ctx->pName), ctx->flag);

    CTG_ERR_JRET(ctgGetTbMetaFromVnode(CTG_PARAMS_LIST(), ctx->pName, &vgInfo, NULL, pTask));
  } else {
    SBuildUseDBInput input = {0};

    tstrncpy(input.db, dbFName, tListLen(input.db));
    input.vgVersion = CTG_DEFAULT_INVALID_VERSION;

    CTG_ERR_JRET(ctgGetDBVgInfoFromMnode(pCtg, pTrans, pMgmtEps, &input, NULL, pTask));
  }

_return:

  if (dbCache) {
    ctgReleaseVgInfo(dbCache);
    ctgReleaseDBCache(pCtg, dbCache);
  }

  CTG_RET(code);
}

int32_t ctgLaunchGetTbMetaTask(SCtgTask *pTask) {
  SCatalog* pCtg = pTask->pJob->pCtg; 
  void *pTrans = pTask->pJob->pTrans; 
  const SEpSet* pMgmtEps = pTask->pJob->pMgmtEps;

  CTG_ERR_RET(ctgGetTbMetaFromCache(CTG_PARAMS_LIST(), (SCtgTbMetaCtx*)pTask->taskCtx, (STableMeta**)&pTask->res));
  if (pTask->res) {
    CTG_ERR_RET(ctgHandleTaskEnd(pTask, 0));
    return TSDB_CODE_SUCCESS;
  }

  CTG_ERR_RET(ctgAsyncRefreshTbMeta(pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetDbVgTask(SCtgTask *pTask) {
  int32_t code = 0;
  SCatalog* pCtg = pTask->pJob->pCtg; 
  void *pTrans = pTask->pJob->pTrans; 
  const SEpSet* pMgmtEps = pTask->pJob->pMgmtEps;
  SCtgDBCache *dbCache = NULL;
  SCtgDbVgCtx* pCtx = (SCtgDbVgCtx*)pTask->taskCtx;
  
  CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, pCtx->dbFName, &dbCache));
  if (NULL != dbCache) {
    CTG_ERR_JRET(ctgGenerateVgList(pCtg, dbCache->vgInfo->vgHash, (SArray**)&pTask->res));
    
    CTG_ERR_JRET(ctgHandleTaskEnd(pTask, 0));
  } else {
    SBuildUseDBInput input = {0};
    
    tstrncpy(input.db, pCtx->dbFName, tListLen(input.db));
    input.vgVersion = CTG_DEFAULT_INVALID_VERSION;
    
    CTG_ERR_RET(ctgGetDBVgInfoFromMnode(pCtg, pTrans, pMgmtEps, &input, NULL, pTask));
  }

_return:

  if (dbCache) {
    ctgReleaseVgInfo(dbCache);
    ctgReleaseDBCache(pCtg, dbCache);
  }

  CTG_RET(code);
}

int32_t ctgLaunchGetTbHashTask(SCtgTask *pTask) {
  int32_t code = 0;
  SCatalog* pCtg = pTask->pJob->pCtg; 
  void *pTrans = pTask->pJob->pTrans; 
  const SEpSet* pMgmtEps = pTask->pJob->pMgmtEps;
  SCtgDBCache *dbCache = NULL;
  SCtgTbHashCtx* pCtx = (SCtgTbHashCtx*)pTask->taskCtx;
  
  CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, pCtx->dbFName, &dbCache));
  if (NULL != dbCache) {
    pTask->res = taosMemoryMalloc(sizeof(SVgroupInfo));
    if (NULL == pTask->res) {
      CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
    }
    CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, dbCache->vgInfo, pCtx->pName, (SVgroupInfo*)pTask->res));
    
    CTG_ERR_JRET(ctgHandleTaskEnd(pTask, 0));
  } else {
    SBuildUseDBInput input = {0};
    
    tstrncpy(input.db, pCtx->dbFName, tListLen(input.db));
    input.vgVersion = CTG_DEFAULT_INVALID_VERSION;
    
    CTG_ERR_RET(ctgGetDBVgInfoFromMnode(pCtg, pTrans, pMgmtEps, &input, NULL, pTask));
  }

_return:

  if (dbCache) {
    ctgReleaseVgInfo(dbCache);
    ctgReleaseDBCache(pCtg, dbCache);
  }

  CTG_RET(code);
}

int32_t ctgLaunchGetQnodeTask(SCtgTask *pTask) {
  SCatalog* pCtg = pTask->pJob->pCtg; 
  void *pTrans = pTask->pJob->pTrans; 
  const SEpSet* pMgmtEps = pTask->pJob->pMgmtEps;

  CTG_ERR_RET(ctgGetQnodeListFromMnode(CTG_PARAMS_LIST(), NULL, pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetDbCfgTask(SCtgTask *pTask) {
  SCatalog* pCtg = pTask->pJob->pCtg; 
  void *pTrans = pTask->pJob->pTrans; 
  const SEpSet* pMgmtEps = pTask->pJob->pMgmtEps;
  SCtgDbCfgCtx* pCtx = (SCtgDbCfgCtx*)pTask->taskCtx;

  CTG_ERR_RET(ctgGetDBCfgFromMnode(CTG_PARAMS_LIST(), pCtx->dbFName, NULL, pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetIndexTask(SCtgTask *pTask) {
  SCatalog* pCtg = pTask->pJob->pCtg; 
  void *pTrans = pTask->pJob->pTrans; 
  const SEpSet* pMgmtEps = pTask->pJob->pMgmtEps;
  SCtgIndexCtx* pCtx = (SCtgIndexCtx*)pTask->taskCtx;

  CTG_ERR_RET(ctgGetIndexInfoFromMnode(CTG_PARAMS_LIST(), pCtx->indexFName, NULL, pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetUdfTask(SCtgTask *pTask) {
  SCatalog* pCtg = pTask->pJob->pCtg; 
  void *pTrans = pTask->pJob->pTrans; 
  const SEpSet* pMgmtEps = pTask->pJob->pMgmtEps;
  SCtgUdfCtx* pCtx = (SCtgUdfCtx*)pTask->taskCtx;

  CTG_ERR_RET(ctgGetUdfInfoFromMnode(CTG_PARAMS_LIST(), pCtx->udfName, NULL, pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetUserTask(SCtgTask *pTask) {
  SCatalog* pCtg = pTask->pJob->pCtg; 
  void *pTrans = pTask->pJob->pTrans; 
  const SEpSet* pMgmtEps = pTask->pJob->pMgmtEps;
  SCtgUserCtx* pCtx = (SCtgUserCtx*)pTask->taskCtx;
  bool inCache = false;
  bool pass = false;
  
  CTG_ERR_RET(ctgChkAuthFromCache(pCtg, pCtx->user.user, pCtx->user.dbFName, pCtx->user.type, &inCache, &pass));
  if (inCache) {
    pTask->res = taosMemoryCalloc(1, sizeof(bool));
    if (NULL == pTask->res) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    *(bool*)pTask->res = pass;
    
    CTG_ERR_RET(ctgHandleTaskEnd(pTask, 0));
    return TSDB_CODE_SUCCESS;
  }

  CTG_ERR_RET(ctgGetUserDbAuthFromMnode(CTG_PARAMS_LIST(), pCtx->user.user, NULL, pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgRelaunchGetTbMetaTask(SCtgTask *pTask) {
  ctgResetTbMetaTask(pTask);

  CTG_ERR_RET(ctgLaunchGetTbMetaTask(pTask));

  return TSDB_CODE_SUCCESS;
}

SCtgAsyncFps gCtgAsyncFps[] = {
  {ctgLaunchGetQnodeTask,  ctgHandleGetQnodeRsp,  ctgDumpQnodeRes},
  {ctgLaunchGetDbVgTask,   ctgHandleGetDbVgRsp,   ctgDumpDbVgRes},
  {ctgLaunchGetDbCfgTask,  ctgHandleGetDbCfgRsp,  ctgDumpDbCfgRes},
  {ctgLaunchGetTbMetaTask, ctgHandleGetTbMetaRsp, ctgDumpTbMetaRes},
  {ctgLaunchGetTbHashTask, ctgHandleGetTbHashRsp, ctgDumpTbHashRes},
  {ctgLaunchGetIndexTask,  ctgHandleGetIndexRsp,  ctgDumpIndexRes},
  {ctgLaunchGetUdfTask,    ctgHandleGetUdfRsp,    ctgDumpUdfRes},
  {ctgLaunchGetUserTask,   ctgHandleGetUserRsp,   ctgDumpUserRes},
};

int32_t ctgMakeAsyncRes(SCtgJob *pJob) {
  int32_t code = 0;
  int32_t taskNum = taosArrayGetSize(pJob->pTasks);
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SCtgTask *pTask = taosArrayGet(pJob->pTasks, i);
    CTG_ERR_RET((*gCtgAsyncFps[pTask->type].dumpResFp)(pTask));
  }

  return TSDB_CODE_SUCCESS;
}


int32_t ctgLaunchJob(SCtgJob *pJob) {
  int32_t taskNum = taosArrayGetSize(pJob->pTasks);
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SCtgTask *pTask = taosArrayGet(pJob->pTasks, i);

    qDebug("QID:%" PRIx64 " start to launch task %d", pJob->queryId, pTask->taskId); 
    CTG_ERR_RET((*gCtgAsyncFps[pTask->type].launchFp)(pTask));
  }

  return TSDB_CODE_SUCCESS;
}



