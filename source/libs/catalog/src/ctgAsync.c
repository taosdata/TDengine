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
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_TB_META;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgTbMetaCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCtgTbMetaCtx* ctx = task.taskCtx;
  ctx->pName = taosMemoryMalloc(sizeof(*name));
  if (NULL == ctx->pName) {
    taosMemoryFree(task.taskCtx);
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }
  
  memcpy(ctx->pName, name, sizeof(*name));
  ctx->flag = CTG_FLAG_UNKNOWN_STB;

  taosArrayPush(pJob->pTasks, &task);

  qDebug("QID:0x%" PRIx64 " the %d task type %s initialized, tbName:%s", pJob->queryId, taskIdx, ctgTaskTypeStr(task.type), name->tname);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetDbVgTask(SCtgJob *pJob, int32_t taskIdx, char *dbFName) {
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_DB_VGROUP;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgDbVgCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCtgDbVgCtx* ctx = task.taskCtx;
  
  memcpy(ctx->dbFName, dbFName, sizeof(ctx->dbFName));

  taosArrayPush(pJob->pTasks, &task);

  qDebug("QID:0x%" PRIx64 " the %d task type %s initialized, dbFName:%s", pJob->queryId, taskIdx, ctgTaskTypeStr(task.type), dbFName);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetDbCfgTask(SCtgJob *pJob, int32_t taskIdx, char *dbFName) {
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_DB_CFG;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgDbCfgCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCtgDbCfgCtx* ctx = task.taskCtx;
  
  memcpy(ctx->dbFName, dbFName, sizeof(ctx->dbFName));

  taosArrayPush(pJob->pTasks, &task);

  qDebug("QID:0x%" PRIx64 " the %d task type %s initialized, dbFName:%s", pJob->queryId, taskIdx, ctgTaskTypeStr(task.type), dbFName);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetDbInfoTask(SCtgJob *pJob, int32_t taskIdx, char *dbFName) {
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_DB_INFO;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgDbInfoCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCtgDbInfoCtx* ctx = task.taskCtx;
  
  memcpy(ctx->dbFName, dbFName, sizeof(ctx->dbFName));

  taosArrayPush(pJob->pTasks, &task);

  qDebug("QID:0x%" PRIx64 " the %d task type %s initialized, dbFName:%s", pJob->queryId, taskIdx, ctgTaskTypeStr(task.type), dbFName);

  return TSDB_CODE_SUCCESS;
}


int32_t ctgInitGetTbHashTask(SCtgJob *pJob, int32_t taskIdx, SName *name) {
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_TB_HASH;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgTbHashCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCtgTbHashCtx* ctx = task.taskCtx;
  ctx->pName = taosMemoryMalloc(sizeof(*name));
  if (NULL == ctx->pName) {
    taosMemoryFree(task.taskCtx);
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }
  
  memcpy(ctx->pName, name, sizeof(*name));
  tNameGetFullDbName(ctx->pName, ctx->dbFName);

  taosArrayPush(pJob->pTasks, &task);

  qDebug("QID:0x%" PRIx64 " the %d task type %s initialized, tableName:%s", pJob->queryId, taskIdx, ctgTaskTypeStr(task.type), name->tname);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetQnodeTask(SCtgJob *pJob, int32_t taskIdx) {
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_QNODE;
  task.taskId = taskIdx;
  task.pJob = pJob;
  task.taskCtx = NULL;

  taosArrayPush(pJob->pTasks, &task);

  qDebug("QID:0x%" PRIx64 " the %d task type %s initialized", pJob->queryId, taskIdx, ctgTaskTypeStr(task.type));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetIndexTask(SCtgJob *pJob, int32_t taskIdx, char *name) {
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_INDEX;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgIndexCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCtgIndexCtx* ctx = task.taskCtx;
  
  strcpy(ctx->indexFName, name);

  taosArrayPush(pJob->pTasks, &task);

  qDebug("QID:0x%" PRIx64 " the %d task type %s initialized, indexFName:%s", pJob->queryId, taskIdx, ctgTaskTypeStr(task.type), name);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetUdfTask(SCtgJob *pJob, int32_t taskIdx, char *name) {
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_UDF;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgUdfCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCtgUdfCtx* ctx = task.taskCtx;
  
  strcpy(ctx->udfName, name);

  taosArrayPush(pJob->pTasks, &task);

  qDebug("QID:0x%" PRIx64 " the %d task type %s initialized, udfName:%s", pJob->queryId, taskIdx, ctgTaskTypeStr(task.type), name);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetUserTask(SCtgJob *pJob, int32_t taskIdx, SUserAuthInfo *user) {
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_USER;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgUserCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCtgUserCtx* ctx = task.taskCtx;
  
  memcpy(&ctx->user, user, sizeof(*user));

  taosArrayPush(pJob->pTasks, &task);

  qDebug("QID:0x%" PRIx64 " the %d task type %s initialized, user:%s", pJob->queryId, taskIdx, ctgTaskTypeStr(task.type), user->user);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetTbIndexTask(SCtgJob *pJob, int32_t taskIdx, SName *name) {
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_TB_INDEX;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgTbIndexCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCtgTbIndexCtx* ctx = task.taskCtx;
  ctx->pName = taosMemoryMalloc(sizeof(*name));
  if (NULL == ctx->pName) {
    taosMemoryFree(task.taskCtx);
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }
  
  memcpy(ctx->pName, name, sizeof(*name));

  taosArrayPush(pJob->pTasks, &task);

  qDebug("QID:0x%" PRIx64 " the %d task type %s initialized, tbName:%s", pJob->queryId, taskIdx, ctgTaskTypeStr(task.type), name->tname);

  return TSDB_CODE_SUCCESS;
}


int32_t ctgHandleForceUpdate(SCatalog* pCtg, int32_t taskNum, SCtgJob *pJob, const SCatalogReq* pReq) {
  SHashObj* pDb = taosHashInit(taskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (NULL == pDb) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }
  
  for (int32_t i = 0; i < pJob->dbVgNum; ++i) {
    char* dbFName = taosArrayGet(pReq->pDbVgroup, i);
    taosHashPut(pDb, dbFName, strlen(dbFName), dbFName, TSDB_DB_FNAME_LEN);
  }

  for (int32_t i = 0; i < pJob->dbCfgNum; ++i) {
    char* dbFName = taosArrayGet(pReq->pDbCfg, i);
    taosHashPut(pDb, dbFName, strlen(dbFName), dbFName, TSDB_DB_FNAME_LEN);
  }

  for (int32_t i = 0; i < pJob->dbInfoNum; ++i) {
    char* dbFName = taosArrayGet(pReq->pDbInfo, i);
    taosHashPut(pDb, dbFName, strlen(dbFName), dbFName, TSDB_DB_FNAME_LEN);
  }

  for (int32_t i = 0; i < pJob->tbMetaNum; ++i) {
    SName* name = taosArrayGet(pReq->pTableMeta, i);
    char dbFName[TSDB_DB_FNAME_LEN];
    tNameGetFullDbName(name, dbFName);
    taosHashPut(pDb, dbFName, strlen(dbFName), dbFName, TSDB_DB_FNAME_LEN);
  }
  
  for (int32_t i = 0; i < pJob->tbHashNum; ++i) {
    SName* name = taosArrayGet(pReq->pTableHash, i);
    char dbFName[TSDB_DB_FNAME_LEN];
    tNameGetFullDbName(name, dbFName);
    taosHashPut(pDb, dbFName, strlen(dbFName), dbFName, TSDB_DB_FNAME_LEN);
  }

  char* dbFName = taosHashIterate(pDb, NULL);
  while (dbFName) {
    ctgDropDbVgroupEnqueue(pCtg, dbFName, true);
    dbFName = taosHashIterate(pDb, dbFName);
  }

  taosHashCleanup(pDb);

  int32_t tbNum = pJob->tbMetaNum + pJob->tbHashNum;
  if (tbNum > 0) {
    if (tbNum > pJob->tbMetaNum && tbNum > pJob->tbHashNum) {
      SHashObj* pTb = taosHashInit(tbNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
      for (int32_t i = 0; i < pJob->tbMetaNum; ++i) {
        SName* name = taosArrayGet(pReq->pTableMeta, i);
        taosHashPut(pTb, name, sizeof(SName), name, sizeof(SName));
      }
      
      for (int32_t i = 0; i < pJob->tbHashNum; ++i) {
        SName* name = taosArrayGet(pReq->pTableHash, i);
        taosHashPut(pTb, name, sizeof(SName), name, sizeof(SName));
      }

      SName* name = taosHashIterate(pTb, NULL);
      while (name) {
        catalogRemoveTableMeta(pCtg, name);
        name = taosHashIterate(pTb, name);
      }

      taosHashCleanup(pTb);
    } else {
      for (int32_t i = 0; i < pJob->tbMetaNum; ++i) {
        SName* name = taosArrayGet(pReq->pTableMeta, i);
        catalogRemoveTableMeta(pCtg, name);
      }
      
      for (int32_t i = 0; i < pJob->tbHashNum; ++i) {
        SName* name = taosArrayGet(pReq->pTableHash, i);
        catalogRemoveTableMeta(pCtg, name);
      }
    }
  }

  for (int32_t i = 0; i < pJob->tbIndexNum; ++i) {
    SName* name = taosArrayGet(pReq->pTableIndex, i);
    ctgDropTbIndexEnqueue(pCtg, name, true);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitJob(SCatalog* pCtg, SRequestConnInfo *pConn, SCtgJob** job, uint64_t reqId, const SCatalogReq* pReq, catalogCallback fp, void* param, int32_t* taskNum) {
  int32_t code = 0;
  int32_t tbMetaNum = (int32_t)taosArrayGetSize(pReq->pTableMeta);
  int32_t dbVgNum = (int32_t)taosArrayGetSize(pReq->pDbVgroup);
  int32_t tbHashNum = (int32_t)taosArrayGetSize(pReq->pTableHash);
  int32_t udfNum = (int32_t)taosArrayGetSize(pReq->pUdf);
  int32_t qnodeNum = pReq->qNodeRequired ? 1 : 0;
  int32_t dbCfgNum = (int32_t)taosArrayGetSize(pReq->pDbCfg);
  int32_t indexNum = (int32_t)taosArrayGetSize(pReq->pIndex);
  int32_t userNum = (int32_t)taosArrayGetSize(pReq->pUser);
  int32_t dbInfoNum = (int32_t)taosArrayGetSize(pReq->pDbInfo);
  int32_t tbIndexNum = (int32_t)taosArrayGetSize(pReq->pTableIndex);

  *taskNum = tbMetaNum + dbVgNum + udfNum + tbHashNum + qnodeNum + dbCfgNum + indexNum + userNum + dbInfoNum + tbIndexNum;
  if (*taskNum <= 0) {
    ctgDebug("Empty input for job, no need to retrieve meta, reqId:0x%" PRIx64, reqId);
    return TSDB_CODE_SUCCESS;
  }

  *job = taosMemoryCalloc(1, sizeof(SCtgJob));
  if (NULL == *job) {
    ctgError("failed to calloc, size:%d, reqId:0x%" PRIx64, (int32_t)sizeof(SCtgJob), reqId);
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCtgJob *pJob = *job;
  
  pJob->queryId = reqId;
  pJob->userFp = fp;
  pJob->pCtg    = pCtg;
  pJob->conn    = *pConn;
  pJob->userParam = param;
  
  pJob->tbMetaNum = tbMetaNum;
  pJob->tbHashNum = tbHashNum;
  pJob->qnodeNum = qnodeNum;
  pJob->dbVgNum = dbVgNum;
  pJob->udfNum = udfNum;
  pJob->dbCfgNum = dbCfgNum;
  pJob->indexNum = indexNum;
  pJob->userNum = userNum;
  pJob->dbInfoNum = dbInfoNum;
  pJob->tbIndexNum = tbIndexNum;

  pJob->pTasks = taosArrayInit(*taskNum, sizeof(SCtgTask));

  if (NULL == pJob->pTasks) {
    ctgError("taosArrayInit %d tasks failed", *taskNum);
    CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  if (pReq->forceUpdate) {
    CTG_ERR_JRET(ctgHandleForceUpdate(pCtg, *taskNum, pJob, pReq));
  }

  int32_t taskIdx = 0;
  for (int32_t i = 0; i < dbVgNum; ++i) {
    char* dbFName = taosArrayGet(pReq->pDbVgroup, i);
    CTG_ERR_JRET(ctgInitGetDbVgTask(pJob, taskIdx++, dbFName));
  }

  for (int32_t i = 0; i < dbCfgNum; ++i) {
    char* dbFName = taosArrayGet(pReq->pDbCfg, i);
    CTG_ERR_JRET(ctgInitGetDbCfgTask(pJob, taskIdx++, dbFName));
  }

  for (int32_t i = 0; i < dbInfoNum; ++i) {
    char* dbFName = taosArrayGet(pReq->pDbInfo, i);
    CTG_ERR_JRET(ctgInitGetDbInfoTask(pJob, taskIdx++, dbFName));
  }

  for (int32_t i = 0; i < tbMetaNum; ++i) {
    SName* name = taosArrayGet(pReq->pTableMeta, i);
    CTG_ERR_JRET(ctgInitGetTbMetaTask(pJob, taskIdx++, name));
  }

  for (int32_t i = 0; i < tbHashNum; ++i) {
    SName* name = taosArrayGet(pReq->pTableHash, i);
    CTG_ERR_JRET(ctgInitGetTbHashTask(pJob, taskIdx++, name));
  }

  for (int32_t i = 0; i < tbIndexNum; ++i) {
    SName* name = taosArrayGet(pReq->pTableIndex, i);
    CTG_ERR_JRET(ctgInitGetTbIndexTask(pJob, taskIdx++, name));
  }

  for (int32_t i = 0; i < indexNum; ++i) {
    char* indexName = taosArrayGet(pReq->pIndex, i);
    CTG_ERR_JRET(ctgInitGetIndexTask(pJob, taskIdx++, indexName));
  }

  for (int32_t i = 0; i < udfNum; ++i) {
    char* udfName = taosArrayGet(pReq->pUdf, i);
    CTG_ERR_JRET(ctgInitGetUdfTask(pJob, taskIdx++, udfName));
  }

  for (int32_t i = 0; i < userNum; ++i) {
    SUserAuthInfo* user = taosArrayGet(pReq->pUser, i);
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

  qDebug("QID:0x%" PRIx64 ", jobId: 0x%" PRIx64 " initialized, task num %d, forceUpdate %d", pJob->queryId, pJob->refId, *taskNum, pReq->forceUpdate);
  return TSDB_CODE_SUCCESS;


_return:
  taosMemoryFreeClear(*job);
  CTG_RET(code);
}

int32_t ctgDumpTbMetaRes(SCtgTask* pTask) {
  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pTableMeta) {
    pJob->jobRes.pTableMeta = taosArrayInit(pJob->tbMetaNum, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pTableMeta) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  taosArrayPush(pJob->jobRes.pTableMeta, &res);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpDbVgRes(SCtgTask* pTask) {
  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pDbVgroup) {
    pJob->jobRes.pDbVgroup = taosArrayInit(pJob->dbVgNum, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pDbVgroup) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  taosArrayPush(pJob->jobRes.pDbVgroup, &res);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpTbHashRes(SCtgTask* pTask) {
  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pTableHash) {
    pJob->jobRes.pTableHash = taosArrayInit(pJob->tbHashNum, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pTableHash) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  taosArrayPush(pJob->jobRes.pTableHash, &res);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpTbIndexRes(SCtgTask* pTask) {
  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pTableIndex) {
    pJob->jobRes.pTableIndex = taosArrayInit(pJob->tbIndexNum, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pTableIndex) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  taosArrayPush(pJob->jobRes.pTableIndex, &res);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpIndexRes(SCtgTask* pTask) {
  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pIndex) {
    pJob->jobRes.pIndex = taosArrayInit(pJob->indexNum, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pIndex) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  taosArrayPush(pJob->jobRes.pIndex, &res);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpQnodeRes(SCtgTask* pTask) {
  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pQnodeList) {
    pJob->jobRes.pQnodeList = taosArrayInit(1, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pQnodeList) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  taosArrayPush(pJob->jobRes.pQnodeList, &res);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpDbCfgRes(SCtgTask* pTask) {
  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pDbCfg) {
    pJob->jobRes.pDbCfg = taosArrayInit(pJob->dbCfgNum, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pDbCfg) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  taosArrayPush(pJob->jobRes.pDbCfg, &res);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpDbInfoRes(SCtgTask* pTask) {
  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pDbInfo) {
    pJob->jobRes.pDbInfo = taosArrayInit(pJob->dbInfoNum, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pDbInfo) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  taosArrayPush(pJob->jobRes.pDbInfo, &res);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpUdfRes(SCtgTask* pTask) {
  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pUdfList) {
    pJob->jobRes.pUdfList = taosArrayInit(pJob->udfNum, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pUdfList) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  taosArrayPush(pJob->jobRes.pUdfList, &res);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpUserRes(SCtgTask* pTask) {
  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pUser) {
    pJob->jobRes.pUser = taosArrayInit(pJob->userNum, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pUser) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  taosArrayPush(pJob->jobRes.pUser, &res);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgHandleTaskEnd(SCtgTask* pTask, int32_t rspCode) {
  SCtgJob* pJob = pTask->pJob;
  int32_t code = 0;

  qDebug("QID:0x%" PRIx64 " task %d end with res %s", pJob->queryId, pTask->taskId, tstrerror(rspCode));

  pTask->code = rspCode;

  int32_t taskDone = atomic_add_fetch_32(&pJob->taskDone, 1);
  if (taskDone < taosArrayGetSize(pJob->pTasks)) {
    qDebug("QID:0x%" PRIx64 " task done: %d, total: %d", pJob->queryId, taskDone, (int32_t)taosArrayGetSize(pJob->pTasks));
    return TSDB_CODE_SUCCESS;
  }

  CTG_ERR_JRET(ctgMakeAsyncRes(pJob));

_return:

  qDebug("QID:0x%" PRIx64 " user callback with rsp %s", pJob->queryId, tstrerror(code));

  (*pJob->userFp)(&pJob->jobRes, pJob->userParam, code);

  taosRemoveRef(gCtgMgmt.jobPool, pJob->refId);
  
  CTG_RET(code);
}

int32_t ctgHandleGetTbMetaRsp(SCtgTask* pTask, int32_t reqType, const SDataBuf *pMsg, int32_t rspCode) {
  int32_t code = 0;
  SCtgDBCache *dbCache = NULL;
  SCtgTbMetaCtx* ctx = (SCtgTbMetaCtx*)pTask->taskCtx;
  SCatalog* pCtg = pTask->pJob->pCtg; 
  SRequestConnInfo* pConn = &pTask->pJob->conn;

  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  switch (reqType) {
    case TDMT_MND_USE_DB: {
      SUseDbOutput* pOut = (SUseDbOutput*)pTask->msgCtx.out;

      SVgroupInfo vgInfo = {0};
      CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, pOut->dbVgroup, ctx->pName, &vgInfo));

      ctgDebug("will refresh tbmeta, not supposed to be stb, tbName:%s, flag:%d", tNameGetTableName(ctx->pName), ctx->flag);

      ctx->vgId = vgInfo.vgId;
      CTG_ERR_JRET(ctgGetTbMetaFromVnode(pCtg, pConn, ctx->pName, &vgInfo, NULL, pTask));

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
            CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, dbCache->vgCache.vgInfo, ctx->pName, &vgInfo));
          
            ctgDebug("will refresh tbmeta, not supposed to be stb, tbName:%s, flag:%d", tNameGetTableName(ctx->pName), ctx->flag);

            ctx->vgId = vgInfo.vgId;          
            CTG_ERR_JRET(ctgGetTbMetaFromVnode(pCtg, pConn, ctx->pName, &vgInfo, NULL, pTask));

            ctgReleaseVgInfoToCache(pCtg, dbCache);
          } else {
            SBuildUseDBInput input = {0};
          
            tstrncpy(input.db, dbFName, tListLen(input.db));
            input.vgVersion = CTG_DEFAULT_INVALID_VERSION;
          
            CTG_ERR_JRET(ctgGetDBVgInfoFromMnode(pCtg, pConn, &input, NULL, pTask));
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
        
        CTG_RET(ctgGetTbMetaFromMnode(pCtg, pConn, ctx->pName, NULL, pTask));
      } else if (CTG_IS_META_BOTH(pOut->metaType)) {
        int32_t exist = 0;
        if (!CTG_FLAG_IS_FORCE_UPDATE(ctx->flag)) {
          CTG_ERR_JRET(ctgTbMetaExistInCache(pCtg, pOut->dbFName, pOut->tbName, &exist));
        }
      
        if (0 == exist) {
          TSWAP(pTask->msgCtx.lastOut, pTask->msgCtx.out);
          CTG_RET(ctgGetTbMetaFromMnodeImpl(pCtg, pConn, pOut->dbFName, pOut->tbName, NULL, pTask));
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
    ctgReleaseVgInfoToCache(pCtg, dbCache);
  }

  ctgHandleTaskEnd(pTask, code);

  CTG_RET(code);
}

int32_t ctgHandleGetDbVgRsp(SCtgTask* pTask, int32_t reqType, const SDataBuf *pMsg, int32_t rspCode) {
  int32_t code = 0;
  SCtgDbVgCtx* ctx = (SCtgDbVgCtx*)pTask->taskCtx;
  SCatalog* pCtg = pTask->pJob->pCtg; 

  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  switch (reqType) {
    case TDMT_MND_USE_DB: {
      SUseDbOutput* pOut = (SUseDbOutput*)pTask->msgCtx.out;

      CTG_ERR_JRET(ctgGenerateVgList(pCtg, pOut->dbVgroup->vgHash, (SArray**)&pTask->res));
      
      CTG_ERR_JRET(ctgUpdateVgroupEnqueue(pCtg, ctx->dbFName, pOut->dbId, pOut->dbVgroup, false));
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
  SCtgTbHashCtx* ctx = (SCtgTbHashCtx*)pTask->taskCtx;
  SCatalog* pCtg = pTask->pJob->pCtg; 

  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  switch (reqType) {
    case TDMT_MND_USE_DB: {
      SUseDbOutput* pOut = (SUseDbOutput*)pTask->msgCtx.out;

      pTask->res = taosMemoryMalloc(sizeof(SVgroupInfo));
      if (NULL == pTask->res) {
        CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
      }

      CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, pOut->dbVgroup, ctx->pName, (SVgroupInfo*)pTask->res));
      
      CTG_ERR_JRET(ctgUpdateVgroupEnqueue(pCtg, ctx->dbFName, pOut->dbId, pOut->dbVgroup, false));
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

int32_t ctgHandleGetTbIndexRsp(SCtgTask* pTask, int32_t reqType, const SDataBuf *pMsg, int32_t rspCode) {
  int32_t code = 0;
  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  STableIndex* pOut = (STableIndex*)pTask->msgCtx.out;
  SArray* pInfo = NULL;
  CTG_ERR_JRET(ctgCloneTableIndex(pOut->pIndex, &pInfo));
  pTask->res = pInfo;

  SCtgTbIndexCtx* ctx = pTask->taskCtx;  
  CTG_ERR_JRET(ctgUpdateTbIndexEnqueue(pTask->pJob->pCtg, (STableIndex**)&pTask->msgCtx.out, false));
  
_return:
  if (TSDB_CODE_MND_DB_INDEX_NOT_EXIST == code) {
    code = TSDB_CODE_SUCCESS;
  }
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

int32_t ctgHandleGetDbInfoRsp(SCtgTask* pTask, int32_t reqType, const SDataBuf *pMsg, int32_t rspCode) {
  CTG_RET(TSDB_CODE_APP_ERROR);
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
  SCtgUserCtx* ctx = (SCtgUserCtx*)pTask->taskCtx;
  SCatalog* pCtg = pTask->pJob->pCtg; 
  bool pass = false;
  SGetUserAuthRsp* pOut = (SGetUserAuthRsp*)pTask->msgCtx.out;

  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

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

  ctgUpdateUserEnqueue(pCtg, pOut, false);
  taosMemoryFreeClear(pTask->msgCtx.out);

  ctgHandleTaskEnd(pTask, code);

  CTG_RET(code);
}

int32_t ctgAsyncRefreshTbMeta(SCtgTask *pTask) {
  SCatalog* pCtg = pTask->pJob->pCtg; 
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  int32_t code = 0;
  SCtgTbMetaCtx* ctx = (SCtgTbMetaCtx*)pTask->taskCtx;

  if (CTG_FLAG_IS_SYS_DB(ctx->flag)) {
    ctgDebug("will refresh sys db tbmeta, tbName:%s", tNameGetTableName(ctx->pName));

    CTG_RET(ctgGetTbMetaFromMnodeImpl(pCtg, pConn, (char *)ctx->pName->dbname, (char *)ctx->pName->tname, NULL, pTask));
  }

  if (CTG_FLAG_IS_STB(ctx->flag)) {
    ctgDebug("will refresh tbmeta, supposed to be stb, tbName:%s", tNameGetTableName(ctx->pName));

    // if get from mnode failed, will not try vnode
    CTG_RET(ctgGetTbMetaFromMnode(pCtg, pConn, ctx->pName, NULL, pTask));
  }

  SCtgDBCache *dbCache = NULL;
  char dbFName[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(ctx->pName, dbFName);
  
  CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, dbFName, &dbCache));
  if (dbCache) {
    SVgroupInfo vgInfo = {0};
    CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, dbCache->vgCache.vgInfo, ctx->pName, &vgInfo));

    ctgDebug("will refresh tbmeta, not supposed to be stb, tbName:%s, flag:%d", tNameGetTableName(ctx->pName), ctx->flag);

    ctx->vgId = vgInfo.vgId;
    CTG_ERR_JRET(ctgGetTbMetaFromVnode(pCtg, pConn, ctx->pName, &vgInfo, NULL, pTask));
  } else {
    SBuildUseDBInput input = {0};

    tstrncpy(input.db, dbFName, tListLen(input.db));
    input.vgVersion = CTG_DEFAULT_INVALID_VERSION;

    CTG_ERR_JRET(ctgGetDBVgInfoFromMnode(pCtg, pConn, &input, NULL, pTask));
  }

_return:

  if (dbCache) {
    ctgReleaseVgInfoToCache(pCtg, dbCache);
  }

  CTG_RET(code);
}

int32_t ctgLaunchGetTbMetaTask(SCtgTask *pTask) {
  SCatalog* pCtg = pTask->pJob->pCtg; 
  SRequestConnInfo* pConn = &pTask->pJob->conn;

  CTG_ERR_RET(ctgGetTbMetaFromCache(pCtg, pConn, (SCtgTbMetaCtx*)pTask->taskCtx, (STableMeta**)&pTask->res));
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
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgDBCache *dbCache = NULL;
  SCtgDbVgCtx* pCtx = (SCtgDbVgCtx*)pTask->taskCtx;
  
  CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, pCtx->dbFName, &dbCache));
  if (NULL != dbCache) {
    CTG_ERR_JRET(ctgGenerateVgList(pCtg, dbCache->vgCache.vgInfo->vgHash, (SArray**)&pTask->res));

    ctgReleaseVgInfoToCache(pCtg, dbCache);
    dbCache = NULL;
    
    CTG_ERR_JRET(ctgHandleTaskEnd(pTask, 0));
  } else {
    SBuildUseDBInput input = {0};
    
    tstrncpy(input.db, pCtx->dbFName, tListLen(input.db));
    input.vgVersion = CTG_DEFAULT_INVALID_VERSION;
    
    CTG_ERR_RET(ctgGetDBVgInfoFromMnode(pCtg, pConn, &input, NULL, pTask));
  }

_return:

  if (dbCache) {
    ctgReleaseVgInfoToCache(pCtg, dbCache);
  }

  CTG_RET(code);
}

int32_t ctgLaunchGetTbHashTask(SCtgTask *pTask) {
  int32_t code = 0;
  SCatalog* pCtg = pTask->pJob->pCtg; 
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgDBCache *dbCache = NULL;
  SCtgTbHashCtx* pCtx = (SCtgTbHashCtx*)pTask->taskCtx;
  
  CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, pCtx->dbFName, &dbCache));
  if (NULL != dbCache) {
    pTask->res = taosMemoryMalloc(sizeof(SVgroupInfo));
    if (NULL == pTask->res) {
      CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
    }
    CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, dbCache->vgCache.vgInfo, pCtx->pName, (SVgroupInfo*)pTask->res));

    ctgReleaseVgInfoToCache(pCtg, dbCache);
    dbCache = NULL;
    
    CTG_ERR_JRET(ctgHandleTaskEnd(pTask, 0));
  } else {
    SBuildUseDBInput input = {0};
    
    tstrncpy(input.db, pCtx->dbFName, tListLen(input.db));
    input.vgVersion = CTG_DEFAULT_INVALID_VERSION;
    
    CTG_ERR_RET(ctgGetDBVgInfoFromMnode(pCtg, pConn, &input, NULL, pTask));
  }

_return:

  if (dbCache) {
    ctgReleaseVgInfoToCache(pCtg, dbCache);
  }

  CTG_RET(code);
}

int32_t ctgLaunchGetTbIndexTask(SCtgTask *pTask) {
  int32_t code = 0;
  SCatalog* pCtg = pTask->pJob->pCtg; 
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgTbIndexCtx* pCtx = (SCtgTbIndexCtx*)pTask->taskCtx;
  SArray* pRes = NULL;

  CTG_ERR_RET(ctgReadTbIndexFromCache(pCtg, pCtx->pName, &pRes));
  if (pRes) {
    pTask->res = pRes;
    
    CTG_ERR_RET(ctgHandleTaskEnd(pTask, 0));
    return TSDB_CODE_SUCCESS;
  }
  
  CTG_ERR_RET(ctgGetTbIndexFromMnode(pCtg, pConn, pCtx->pName, NULL, pTask));
  return TSDB_CODE_SUCCESS;
}


int32_t ctgLaunchGetQnodeTask(SCtgTask *pTask) {
  SCatalog* pCtg = pTask->pJob->pCtg; 
  SRequestConnInfo* pConn = &pTask->pJob->conn;

  CTG_ERR_RET(ctgGetQnodeListFromMnode(pCtg, pConn, NULL, pTask));
  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetDbCfgTask(SCtgTask *pTask) {
  SCatalog* pCtg = pTask->pJob->pCtg; 
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgDbCfgCtx* pCtx = (SCtgDbCfgCtx*)pTask->taskCtx;

  CTG_ERR_RET(ctgGetDBCfgFromMnode(pCtg, pConn, pCtx->dbFName, NULL, pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetDbInfoTask(SCtgTask *pTask) {
  int32_t code = 0;
  SCatalog* pCtg = pTask->pJob->pCtg; 
  SCtgDBCache *dbCache = NULL;
  SCtgDbInfoCtx* pCtx = (SCtgDbInfoCtx*)pTask->taskCtx;

  pTask->res = taosMemoryCalloc(1, sizeof(SDbInfo));
  if (NULL == pTask->res) {
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SDbInfo* pInfo = (SDbInfo*)pTask->res;
  CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, pCtx->dbFName, &dbCache));
  if (NULL != dbCache) {
    pInfo->vgVer = dbCache->vgCache.vgInfo->vgVersion;
    pInfo->dbId = dbCache->dbId;
    pInfo->tbNum = dbCache->vgCache.vgInfo->numOfTable;

    ctgReleaseVgInfoToCache(pCtg, dbCache);
    dbCache = NULL;
  } else {
    pInfo->vgVer = CTG_DEFAULT_INVALID_VERSION;
  }

  CTG_ERR_JRET(ctgHandleTaskEnd(pTask, 0));

_return:

  if (dbCache) {
    ctgReleaseVgInfoToCache(pCtg, dbCache);
  }

  CTG_RET(code);
}

int32_t ctgLaunchGetIndexTask(SCtgTask *pTask) {
  SCatalog* pCtg = pTask->pJob->pCtg; 
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgIndexCtx* pCtx = (SCtgIndexCtx*)pTask->taskCtx;

  CTG_ERR_RET(ctgGetIndexInfoFromMnode(pCtg, pConn, pCtx->indexFName, NULL, pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetUdfTask(SCtgTask *pTask) {
  SCatalog* pCtg = pTask->pJob->pCtg; 
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgUdfCtx* pCtx = (SCtgUdfCtx*)pTask->taskCtx;

  CTG_ERR_RET(ctgGetUdfInfoFromMnode(pCtg, pConn, pCtx->udfName, NULL, pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetUserTask(SCtgTask *pTask) {
  SCatalog* pCtg = pTask->pJob->pCtg; 
  SRequestConnInfo* pConn = &pTask->pJob->conn;
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

  CTG_ERR_RET(ctgGetUserDbAuthFromMnode(pCtg, pConn, pCtx->user.user, NULL, pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgRelaunchGetTbMetaTask(SCtgTask *pTask) {
  ctgResetTbMetaTask(pTask);

  CTG_ERR_RET(ctgLaunchGetTbMetaTask(pTask));

  return TSDB_CODE_SUCCESS;
}

SCtgAsyncFps gCtgAsyncFps[] = {
  {ctgLaunchGetQnodeTask,   ctgHandleGetQnodeRsp,   ctgDumpQnodeRes},
  {ctgLaunchGetDbVgTask,    ctgHandleGetDbVgRsp,    ctgDumpDbVgRes},
  {ctgLaunchGetDbCfgTask,   ctgHandleGetDbCfgRsp,   ctgDumpDbCfgRes},
  {ctgLaunchGetDbInfoTask,  ctgHandleGetDbInfoRsp,  ctgDumpDbInfoRes},
  {ctgLaunchGetTbMetaTask,  ctgHandleGetTbMetaRsp,  ctgDumpTbMetaRes},
  {ctgLaunchGetTbHashTask,  ctgHandleGetTbHashRsp,  ctgDumpTbHashRes},
  {ctgLaunchGetTbIndexTask, ctgHandleGetTbIndexRsp, ctgDumpTbIndexRes},
  {ctgLaunchGetIndexTask,   ctgHandleGetIndexRsp,   ctgDumpIndexRes},
  {ctgLaunchGetUdfTask,     ctgHandleGetUdfRsp,     ctgDumpUdfRes},
  {ctgLaunchGetUserTask,    ctgHandleGetUserRsp,    ctgDumpUserRes},
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

    qDebug("QID:0x%" PRIx64 " ctg start to launch task %d", pJob->queryId, pTask->taskId);
    CTG_ERR_RET((*gCtgAsyncFps[pTask->type].launchFp)(pTask));
  }

  return TSDB_CODE_SUCCESS;
}



