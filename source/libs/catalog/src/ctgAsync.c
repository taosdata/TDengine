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
#include "tref.h"
#include "trpc.h"

typedef struct SCtgViewTaskParam {
  bool    forceFetch;
  SArray* pTableReqs;
} SCtgViewTaskParam;

void ctgIsTaskDone(SCtgJob* pJob, CTG_TASK_TYPE type, bool* done) {
  SCtgTask* pTask = NULL;

  *done = true;

  CTG_LOCK(CTG_READ, &pJob->taskLock);

  int32_t taskNum = taosArrayGetSize(pJob->pTasks);
  for (int32_t i = 0; i < taskNum; ++i) {
    pTask = taosArrayGet(pJob->pTasks, i);
    if (type != pTask->type) {
      continue;
    }

    if (pTask->status != CTG_TASK_DONE) {
      *done = false;
      break;
    }
  }

  CTG_UNLOCK(CTG_READ, &pJob->taskLock);
}

int32_t ctgInitGetTbMetaTask(SCtgJob* pJob, int32_t taskIdx, void* param) {
  SCtgTbMetaParam* pParam = (SCtgTbMetaParam*)param;
  SName*           name = pParam->pName;
  SCtgTask         task = {0};

  task.type = CTG_TASK_GET_TB_META;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgTbMetaCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(terrno);
  }

  SCtgTbMetaCtx* ctx = task.taskCtx;
  ctx->pName = taosMemoryMalloc(sizeof(*name));
  if (NULL == ctx->pName) {
    taosMemoryFree(task.taskCtx);
    CTG_ERR_RET(terrno);
  }

  TAOS_MEMCPY(ctx->pName, name, sizeof(*name));
  ctx->flag = pParam->flag | CTG_FLAG_UNKNOWN_STB;

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  qDebug("QID:0x%" PRIx64 " the %dth task type %s initialized, tbName:%s", pJob->queryId, taskIdx,
         ctgTaskTypeStr(task.type), name->tname);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetTbMetasTask(SCtgJob* pJob, int32_t taskIdx, void* param) {
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_TB_META_BATCH;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgTbMetasCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(terrno);
  }

  SCtgTbMetasCtx* ctx = task.taskCtx;
  ctx->pNames = param;
  ctx->pResList = taosArrayInit(pJob->tbMetaNum, sizeof(SMetaRes));
  if (NULL == ctx->pResList) {
    qError("QID:0x%" PRIx64 " taosArrayInit %d SMetaRes %d failed", pJob->queryId, pJob->tbMetaNum,
           (int32_t)sizeof(SMetaRes));
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  qDebug("QID:0x%" PRIx64 " the %dth task type %s initialized, dbNum:%lu, tbNum:%d", pJob->queryId, taskIdx,
         ctgTaskTypeStr(task.type), taosArrayGetSize(ctx->pNames), pJob->tbMetaNum);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetDbVgTask(SCtgJob* pJob, int32_t taskIdx, void* param) {
  char*    dbFName = (char*)param;
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_DB_VGROUP;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgDbVgCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(terrno);
  }

  SCtgDbVgCtx* ctx = task.taskCtx;

  TAOS_MEMCPY(ctx->dbFName, dbFName, sizeof(ctx->dbFName));

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  qDebug("QID:0x%" PRIx64 " the %dth task type %s initialized, dbFName:%s", pJob->queryId, taskIdx,
         ctgTaskTypeStr(task.type), dbFName);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetDbCfgTask(SCtgJob* pJob, int32_t taskIdx, void* param) {
  char*    dbFName = (char*)param;
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_DB_CFG;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgDbCfgCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(terrno);
  }

  SCtgDbCfgCtx* ctx = task.taskCtx;

  TAOS_MEMCPY(ctx->dbFName, dbFName, sizeof(ctx->dbFName));

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  qDebug("QID:0x%" PRIx64 " the %dth task type %s initialized, dbFName:%s", pJob->queryId, taskIdx,
         ctgTaskTypeStr(task.type), dbFName);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetDbInfoTask(SCtgJob* pJob, int32_t taskIdx, void* param) {
  char*    dbFName = (char*)param;
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_DB_INFO;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgDbInfoCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(terrno);
  }

  SCtgDbInfoCtx* ctx = task.taskCtx;

  TAOS_MEMCPY(ctx->dbFName, dbFName, sizeof(ctx->dbFName));

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  qDebug("QID:0x%" PRIx64 " the %dth task type %s initialized, dbFName:%s", pJob->queryId, taskIdx,
         ctgTaskTypeStr(task.type), dbFName);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetTbHashTask(SCtgJob* pJob, int32_t taskIdx, void* param) {
  SName*   name = (SName*)param;
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_TB_HASH;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgTbHashCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(terrno);
  }

  SCtgTbHashCtx* ctx = task.taskCtx;
  ctx->pName = taosMemoryMalloc(sizeof(*name));
  if (NULL == ctx->pName) {
    taosMemoryFree(task.taskCtx);
    CTG_ERR_RET(terrno);
  }

  TAOS_MEMCPY(ctx->pName, name, sizeof(*name));
  (void)tNameGetFullDbName(ctx->pName, ctx->dbFName);

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  qDebug("QID:0x%" PRIx64 " the %dth task type %s initialized, tableName:%s", pJob->queryId, taskIdx,
         ctgTaskTypeStr(task.type), name->tname);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetTbHashsTask(SCtgJob* pJob, int32_t taskIdx, void* param) {
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_TB_HASH_BATCH;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgTbHashsCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(terrno);
  }

  SCtgTbHashsCtx* ctx = task.taskCtx;
  ctx->pNames = param;
  ctx->pResList = taosArrayInit(pJob->tbHashNum, sizeof(SMetaRes));
  if (NULL == ctx->pResList) {
    qError("QID:0x%" PRIx64 " taosArrayInit %d SMetaRes %d failed", pJob->queryId, pJob->tbHashNum,
           (int32_t)sizeof(SMetaRes));
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  qDebug("QID:0x%" PRIx64 " the %dth task type %s initialized, dbNum:%lu, tbNum:%d", pJob->queryId, taskIdx,
         ctgTaskTypeStr(task.type), taosArrayGetSize(ctx->pNames), pJob->tbHashNum);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetQnodeTask(SCtgJob* pJob, int32_t taskIdx, void* param) {
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_QNODE;
  task.taskId = taskIdx;
  task.pJob = pJob;
  task.taskCtx = NULL;

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  qDebug("QID:0x%" PRIx64 " the %dth task type %s initialized", pJob->queryId, taskIdx, ctgTaskTypeStr(task.type));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetDnodeTask(SCtgJob* pJob, int32_t taskIdx, void* param) {
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_DNODE;
  task.taskId = taskIdx;
  task.pJob = pJob;
  task.taskCtx = NULL;

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  qDebug("QID:0x%" PRIx64 " the %dth task type %s initialized", pJob->queryId, taskIdx, ctgTaskTypeStr(task.type));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetIndexTask(SCtgJob* pJob, int32_t taskIdx, void* param) {
  char*    name = (char*)param;
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_INDEX_INFO;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgIndexCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(terrno);
  }

  SCtgIndexCtx* ctx = task.taskCtx;

  tstrncpy(ctx->indexFName, name, sizeof(ctx->indexFName));

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  qDebug("QID:0x%" PRIx64 " the %dth task type %s initialized, indexFName:%s", pJob->queryId, taskIdx,
         ctgTaskTypeStr(task.type), name);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetUdfTask(SCtgJob* pJob, int32_t taskIdx, void* param) {
  char*    name = (char*)param;
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_UDF;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgUdfCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(terrno);
  }

  SCtgUdfCtx* ctx = task.taskCtx;

  tstrncpy(ctx->udfName, name, sizeof(ctx->udfName));

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  qDebug("QID:0x%" PRIx64 " the %dth task type %s initialized, udfName:%s", pJob->queryId, taskIdx,
         ctgTaskTypeStr(task.type), name);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetUserTask(SCtgJob* pJob, int32_t taskIdx, void* param) {
  SUserAuthInfo* user = (SUserAuthInfo*)param;
  SCtgTask       task = {0};

  task.type = CTG_TASK_GET_USER;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgUserCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(terrno);
  }

  SCtgUserCtx* ctx = task.taskCtx;

  TAOS_MEMCPY(&ctx->user, user, sizeof(*user));

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  qDebug("QID:0x%" PRIx64 " the %dth task type %s initialized, user:%s", pJob->queryId, taskIdx,
         ctgTaskTypeStr(task.type), user->user);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetSvrVerTask(SCtgJob* pJob, int32_t taskIdx, void* param) {
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_SVR_VER;
  task.taskId = taskIdx;
  task.pJob = pJob;

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  qDebug("QID:0x%" PRIx64 " the %dth task type %s initialized", pJob->queryId, taskIdx, ctgTaskTypeStr(task.type));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetTbIndexTask(SCtgJob* pJob, int32_t taskIdx, void* param) {
  SName*   name = (SName*)param;
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_TB_SMA_INDEX;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgTbIndexCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(terrno);
  }

  SCtgTbIndexCtx* ctx = task.taskCtx;
  ctx->pName = taosMemoryMalloc(sizeof(*name));
  if (NULL == ctx->pName) {
    taosMemoryFree(task.taskCtx);
    CTG_ERR_RET(terrno);
  }

  TAOS_MEMCPY(ctx->pName, name, sizeof(*name));

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  qDebug("QID:0x%" PRIx64 " the %dth task type %s initialized, tbName:%s", pJob->queryId, taskIdx,
         ctgTaskTypeStr(task.type), name->tname);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetTbCfgTask(SCtgJob* pJob, int32_t taskIdx, void* param) {
  SName*   name = (SName*)param;
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_TB_CFG;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgTbCfgCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(terrno);
  }

  SCtgTbCfgCtx* ctx = task.taskCtx;
  ctx->pName = taosMemoryMalloc(sizeof(*name));
  if (NULL == ctx->pName) {
    taosMemoryFree(task.taskCtx);
    CTG_ERR_RET(terrno);
  }

  TAOS_MEMCPY(ctx->pName, name, sizeof(*name));

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  qDebug("QID:0x%" PRIx64 " the %dth task type %s initialized, tbName:%s", pJob->queryId, taskIdx,
         ctgTaskTypeStr(task.type), name->tname);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetTbTagTask(SCtgJob* pJob, int32_t taskIdx, void* param) {
  SName*   name = (SName*)param;
  SCtgTask task = {0};

  task.type = CTG_TASK_GET_TB_TAG;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgTbTagCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(terrno);
  }

  SCtgTbTagCtx* ctx = task.taskCtx;
  ctx->pName = taosMemoryMalloc(sizeof(*name));
  if (NULL == ctx->pName) {
    taosMemoryFree(task.taskCtx);
    CTG_ERR_RET(terrno);
  }

  TAOS_MEMCPY(ctx->pName, name, sizeof(*name));

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  qDebug("QID:0x%" PRIx64 " the %dth task type %s initialized, tbName:%s", pJob->queryId, taskIdx,
         ctgTaskTypeStr(task.type), name->tname);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetViewsTask(SCtgJob* pJob, int32_t taskIdx, void* param) {
  SCtgTask task = {0};
  SCtgViewTaskParam* p = param;
  task.type = CTG_TASK_GET_VIEW;
  task.taskId = taskIdx;
  task.pJob = pJob;

  task.taskCtx = taosMemoryCalloc(1, sizeof(SCtgViewsCtx));
  if (NULL == task.taskCtx) {
    CTG_ERR_RET(terrno);
  }

  SCtgViewsCtx* ctx = task.taskCtx;
  ctx->pNames = p->pTableReqs;
  ctx->forceFetch = p->forceFetch;
  ctx->pResList = taosArrayInit(pJob->viewNum, sizeof(SMetaRes));
  if (NULL == ctx->pResList) {
    qError("QID:0x%" PRIx64 " taosArrayInit %d SMetaRes %d failed", pJob->queryId, pJob->viewNum,
           (int32_t)sizeof(SMetaRes));
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  qDebug("QID:0x%" PRIx64 " the %dth task type %s initialized, dbNum:%lu, viewNum:%d", pJob->queryId, taskIdx,
         ctgTaskTypeStr(task.type), taosArrayGetSize(ctx->pNames), pJob->viewNum);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetTbTSMATask(SCtgJob* pJob, int32_t taskId, void* param) {
  SCtgTask task = {0};
  task.type = CTG_TASK_GET_TB_TSMA;
  task.taskId = taskId;
  task.pJob = pJob;

  SCtgTbTSMACtx* pTaskCtx = taosMemoryCalloc(1, sizeof(SCtgTbTSMACtx));
  if (NULL == pTaskCtx) {
    CTG_ERR_RET(terrno);
  }

  task.taskCtx = pTaskCtx;
  pTaskCtx->pNames = param;
  pTaskCtx->pResList = taosArrayInit(pJob->tbTsmaNum, sizeof(SMetaRes));
  if (NULL == pTaskCtx->pResList) {
    qError("QID:0x%" PRIx64 " taosArrayInit %d SMetaRes %d failed", pJob->queryId, pJob->tbTsmaNum,
           (int32_t)sizeof(SMetaRes));
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgInitGetTSMATask(SCtgJob* pJob, int32_t taskId, void* param) {
  SCtgTask task = {0};
  task.type = CTG_TASK_GET_TSMA;
  task.taskId = taskId;
  task.pJob = pJob;

  SCtgTbTSMACtx* pTaskCtx = taosMemoryCalloc(1, sizeof(SCtgTbTSMACtx));
  if (NULL == pTaskCtx) {
    CTG_ERR_RET(terrno);
  }
  task.taskCtx = pTaskCtx;
  pTaskCtx->pNames = param;
  pTaskCtx->pResList = taosArrayInit(pJob->tsmaNum, sizeof(SMetaRes));
  if (NULL == pTaskCtx->pResList) {
    qError("QID:0x%" PRIx64 " taosArrayInit %d SMetaRes %d failed", pJob->queryId, pJob->tsmaNum,
           (int32_t)sizeof(SMetaRes));
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t ctgInitGetTbNamesTask(SCtgJob* pJob, int32_t taskId, void* param) {
  SCtgTask task = {0};
  task.type = CTG_TASK_GET_TB_NAME;
  task.taskId = taskId;
  task.pJob = pJob;

  SCtgTbNamesCtx* pTaskCtx = taosMemoryCalloc(1, sizeof(SCtgTbNamesCtx));
  if (NULL == pTaskCtx) {
    CTG_ERR_RET(terrno);
  }
  task.taskCtx = pTaskCtx;
  taosInitRWLatch(&pTaskCtx->lock);
  pTaskCtx->pNames = param;
  pTaskCtx->pResList = taosArrayInit(pJob->tbNameNum, sizeof(SMetaRes));
  if (NULL == pTaskCtx->pResList) {
    qError("QID:0x%" PRIx64 " taosArrayInit %d SMetaRes %d failed", pJob->queryId, pJob->tbNameNum,
           (int32_t)sizeof(SMetaRes));
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  if (NULL == taosArrayPush(pJob->pTasks, &task)) {
    ctgFreeTask(&task, true);
    CTG_ERR_RET(terrno);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgHandleForceUpdateView(SCatalog* pCtg, const SCatalogReq* pReq) {
  int32_t viewNum = taosArrayGetSize(pReq->pView);
  for (int32_t i = 0; i < viewNum; ++i) {
    STablesReq* p = taosArrayGet(pReq->pView, i);
    if (NULL == p) {
      qError("taosArrayGet the %dth view in req failed", i);
      CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
    }

    int32_t viewNum = taosArrayGetSize(p->pTables);
    for (int32_t m = 0; m < viewNum; ++m) {
      SName* name = taosArrayGet(p->pTables, m);
      CTG_ERR_RET(ctgDropViewMetaEnqueue(pCtg, p->dbFName, 0, name->tname, 0, true));
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgHandleForceUpdate(SCatalog* pCtg, int32_t taskNum, SCtgJob* pJob, const SCatalogReq* pReq) {
  int32_t   code = TSDB_CODE_SUCCESS;
  SHashObj* pDb = taosHashInit(taskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  SHashObj* pTb = taosHashInit(taskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (NULL == pDb || NULL == pTb) {
    taosHashCleanup(pDb);
    taosHashCleanup(pTb);
    CTG_ERR_RET(terrno);
  }

  for (int32_t i = 0; i < pJob->dbVgNum; ++i) {
    char* dbFName = taosArrayGet(pReq->pDbVgroup, i);
    if (NULL == dbFName) {
      qError("taosArrayGet the %dth db in req failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }
    CTG_ERR_JRET(taosHashPut(pDb, dbFName, strlen(dbFName), dbFName, TSDB_DB_FNAME_LEN));
  }

  for (int32_t i = 0; i < pJob->dbCfgNum; ++i) {
    char* dbFName = taosArrayGet(pReq->pDbCfg, i);
    if (NULL == dbFName) {
      qError("taosArrayGet the %dth db in req failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }
    CTG_ERR_JRET(taosHashPut(pDb, dbFName, strlen(dbFName), dbFName, TSDB_DB_FNAME_LEN));
  }

  for (int32_t i = 0; i < pJob->dbInfoNum; ++i) {
    char* dbFName = taosArrayGet(pReq->pDbInfo, i);
    if (NULL == dbFName) {
      qError("taosArrayGet the %dth db in req failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }
    CTG_ERR_JRET(taosHashPut(pDb, dbFName, strlen(dbFName), dbFName, TSDB_DB_FNAME_LEN));
  }

  int32_t dbNum = taosArrayGetSize(pReq->pTableMeta);
  for (int32_t i = 0; i < dbNum; ++i) {
    STablesReq* p = taosArrayGet(pReq->pTableMeta, i);
    if (NULL == p) {
      qError("taosArrayGet the %dth tb in req failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }
    CTG_ERR_JRET(taosHashPut(pDb, p->dbFName, strlen(p->dbFName), p->dbFName, TSDB_DB_FNAME_LEN));

    int32_t tbNum = taosArrayGetSize(p->pTables);
    for (int32_t m = 0; m < tbNum; ++m) {
      SName* name = taosArrayGet(p->pTables, m);
      if (NULL == name) {
        qError("taosArrayGet the %dth tb in req failed", m);
        CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
      }
      CTG_ERR_JRET(taosHashPut(pTb, name, sizeof(SName), name, sizeof(SName)));
    }
  }

  dbNum = taosArrayGetSize(pReq->pTableHash);
  for (int32_t i = 0; i < dbNum; ++i) {
    STablesReq* p = taosArrayGet(pReq->pTableHash, i);
    if (NULL == p) {
      qError("taosArrayGet the %dth tb in req failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }

    CTG_ERR_JRET(taosHashPut(pDb, p->dbFName, strlen(p->dbFName), p->dbFName, TSDB_DB_FNAME_LEN));

    int32_t tbNum = taosArrayGetSize(p->pTables);
    for (int32_t m = 0; m < tbNum; ++m) {
      SName* name = taosArrayGet(p->pTables, m);
      if (NULL == name) {
        qError("taosArrayGet the %dth tb in req failed", m);
        CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
      }
      CTG_ERR_JRET(taosHashPut(pTb, name, sizeof(SName), name, sizeof(SName)));
    }
  }

  dbNum = taosArrayGetSize(pReq->pView);
  for (int32_t i = 0; i < dbNum; ++i) {
    STablesReq* p = taosArrayGet(pReq->pView, i);
    if (NULL == p) {
      qError("taosArrayGet the %dth db in req failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }
    CTG_ERR_JRET(taosHashPut(pDb, p->dbFName, strlen(p->dbFName), p->dbFName, TSDB_DB_FNAME_LEN));
  }

  for (int32_t i = 0; i < pJob->tbCfgNum; ++i) {
    SName* name = taosArrayGet(pReq->pTableCfg, i);
    if (NULL == name) {
      qError("taosArrayGet the %dth tb in req failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }

    char dbFName[TSDB_DB_FNAME_LEN];
    (void)tNameGetFullDbName(name, dbFName);
    CTG_ERR_JRET(taosHashPut(pDb, dbFName, strlen(dbFName), dbFName, TSDB_DB_FNAME_LEN));
    CTG_ERR_JRET(taosHashPut(pTb, name, sizeof(SName), name, sizeof(SName)));
  }

  for (int32_t i = 0; i < pJob->tbTagNum; ++i) {
    SName* name = taosArrayGet(pReq->pTableTag, i);
    if (NULL == name) {
      qError("taosArrayGet the %dth tb in req failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }

    char dbFName[TSDB_DB_FNAME_LEN];
    (void)tNameGetFullDbName(name, dbFName);
    CTG_ERR_JRET(taosHashPut(pDb, dbFName, strlen(dbFName), dbFName, TSDB_DB_FNAME_LEN));
    CTG_ERR_JRET(taosHashPut(pTb, name, sizeof(SName), name, sizeof(SName)));
  }

  char* dbFName = taosHashIterate(pDb, NULL);
  while (dbFName) {
    CTG_ERR_JRET(ctgDropDbVgroupEnqueue(pCtg, dbFName, true));
    dbFName = taosHashIterate(pDb, dbFName);
  }

  taosHashCleanup(pDb);
  pDb = NULL;

  // REFRESH TABLE META

  for (int32_t i = 0; i < pJob->tbCfgNum; ++i) {
    SName* name = taosArrayGet(pReq->pTableCfg, i);
    if (NULL == name) {
      qError("taosArrayGet the %dth tb in req failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }
    CTG_ERR_JRET(taosHashPut(pTb, name, sizeof(SName), name, sizeof(SName)));
  }

  SName* name = taosHashIterate(pTb, NULL);
  while (name) {
    CTG_ERR_JRET(ctgRemoveTbMeta(pCtg, name));
    name = taosHashIterate(pTb, name);
  }

  taosHashCleanup(pTb);
  pTb = NULL;

  for (int32_t i = 0; i < pJob->tbIndexNum; ++i) {
    SName* name = taosArrayGet(pReq->pTableIndex, i);
    if (NULL == name) {
      qError("taosArrayGet the %dth tb in req failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }
    CTG_ERR_JRET(ctgDropTbIndexEnqueue(pCtg, name, true));
  }

  for (int32_t i = 0; i < pJob->tbTsmaNum; ++i) {
    STablesReq* pTbReq = taosArrayGet(pReq->pTableTSMAs, i);
    if (NULL == pTbReq) {
      qError("taosArrayGet the %dth tb in req failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }
    for (int32_t j = 0; j < pTbReq->pTables->size; ++j) {
      SName* name = taosArrayGet(pTbReq->pTables, j);
      CTG_ERR_JRET(ctgDropTSMAForTbEnqueue(pCtg, name, true));
    }
  }

  // REFRESH VIEW META
  CTG_ERR_JRET(ctgHandleForceUpdateView(pCtg, pReq));

_return:

  taosHashCleanup(pDb);
  taosHashCleanup(pTb);

  return code;
}

int32_t ctgInitTask(SCtgJob* pJob, CTG_TASK_TYPE type, void* param, int32_t* taskId) {
  int32_t code = 0;
  int32_t tid = atomic_fetch_add_32(&pJob->taskIdx, 1);

  CTG_LOCK(CTG_WRITE, &pJob->taskLock);
  CTG_ERR_JRET((*gCtgAsyncFps[type].initFp)(pJob, tid, param));

  if (taskId) {
    *taskId = tid;
  }

_return:

  CTG_UNLOCK(CTG_WRITE, &pJob->taskLock);

  return code;
}

int32_t ctgInitJob(SCatalog* pCtg, SRequestConnInfo* pConn, SCtgJob** job, const SCatalogReq* pReq, catalogCallback fp,
                   void* param) {
  int32_t code = 0;
  int64_t st = taosGetTimestampUs();

  int32_t tbMetaNum = (int32_t)ctgGetTablesReqNum(pReq->pTableMeta);
  int32_t dbVgNum = (int32_t)taosArrayGetSize(pReq->pDbVgroup);
  int32_t tbHashNum = (int32_t)ctgGetTablesReqNum(pReq->pTableHash);
  int32_t udfNum = (int32_t)taosArrayGetSize(pReq->pUdf);
  int32_t qnodeNum = pReq->qNodeRequired ? 1 : 0;
  int32_t dnodeNum = pReq->dNodeRequired ? 1 : 0;
  int32_t svrVerNum = pReq->svrVerRequired ? 1 : 0;
  int32_t dbCfgNum = (int32_t)taosArrayGetSize(pReq->pDbCfg);
  int32_t indexNum = (int32_t)taosArrayGetSize(pReq->pIndex);
  int32_t userNum = (int32_t)taosArrayGetSize(pReq->pUser);
  int32_t dbInfoNum = (int32_t)taosArrayGetSize(pReq->pDbInfo);
  int32_t tbIndexNum = (int32_t)taosArrayGetSize(pReq->pTableIndex);
  int32_t tbCfgNum = (int32_t)taosArrayGetSize(pReq->pTableCfg);
  int32_t tbTagNum = (int32_t)taosArrayGetSize(pReq->pTableTag);
  int32_t viewNum = (int32_t)ctgGetTablesReqNum(pReq->pView);
  int32_t tbTsmaNum = tsQuerySmaOptimize ? (int32_t)taosArrayGetSize(pReq->pTableTSMAs) : 0;
  int32_t tsmaNum = (int32_t)taosArrayGetSize(pReq->pTSMAs);
  int32_t tbNameNum = (int32_t)ctgGetTablesReqNum(pReq->pTableName);

  int32_t taskNum = tbMetaNum + dbVgNum + udfNum + tbHashNum + qnodeNum + dnodeNum + svrVerNum + dbCfgNum + indexNum +
                    userNum + dbInfoNum + tbIndexNum + tbCfgNum + tbTagNum + viewNum + tbTsmaNum + tbNameNum;
  *job = taosMemoryCalloc(1, sizeof(SCtgJob));
  if (NULL == *job) {
    ctgError("failed to calloc, size:%d,QID:0x%" PRIx64, (int32_t)sizeof(SCtgJob), pConn->requestId);
    CTG_ERR_RET(terrno);
  }

  SCtgJob* pJob = *job;

  pJob->jobRes.ctgFree = true;
  pJob->subTaskNum = taskNum;
  pJob->queryId = pConn->requestId;
  pJob->userFp = fp;
  pJob->pCtg = pCtg;
  pJob->conn = *pConn;
  pJob->userParam = param;

  pJob->tbMetaNum = tbMetaNum;
  pJob->tbHashNum = tbHashNum;
  pJob->qnodeNum = qnodeNum;
  pJob->dnodeNum = dnodeNum;
  pJob->dbVgNum = dbVgNum;
  pJob->udfNum = udfNum;
  pJob->dbCfgNum = dbCfgNum;
  pJob->indexNum = indexNum;
  pJob->userNum = userNum;
  pJob->dbInfoNum = dbInfoNum;
  pJob->tbIndexNum = tbIndexNum;
  pJob->tbCfgNum = tbCfgNum;
  pJob->svrVerNum = svrVerNum;
  pJob->tbTagNum = tbTagNum;
  pJob->viewNum = viewNum;
  pJob->tbTsmaNum = tbTsmaNum;
  pJob->tsmaNum = tsmaNum;
  pJob->tbNameNum = tbNameNum;

#if CTG_BATCH_FETCH
  pJob->pBatchs =
      taosHashInit(CTG_DEFAULT_BATCH_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (NULL == pJob->pBatchs) {
    ctgError("taosHashInit %d batch failed", CTG_DEFAULT_BATCH_NUM);
    CTG_ERR_JRET(terrno);
  }
#endif

  pJob->pTasks = taosArrayInit(taskNum, sizeof(SCtgTask));
  if (NULL == pJob->pTasks) {
    ctgError("taosArrayInit %d tasks failed", taskNum);
    CTG_ERR_JRET(terrno);
  }

  if (pReq->forceUpdate && taskNum) {
    CTG_ERR_JRET(ctgHandleForceUpdate(pCtg, taskNum, pJob, pReq));
  }

  for (int32_t i = 0; i < dbVgNum; ++i) {
    char* dbFName = taosArrayGet(pReq->pDbVgroup, i);
    if (NULL == dbFName) {
      qError("taosArrayGet the %dth db in pDbVgroup failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_DB_VGROUP, dbFName, NULL));
  }

  for (int32_t i = 0; i < dbCfgNum; ++i) {
    char* dbFName = taosArrayGet(pReq->pDbCfg, i);
    if (NULL == dbFName) {
      qError("taosArrayGet the %dth db in pDbCfg failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_DB_CFG, dbFName, NULL));
  }

  for (int32_t i = 0; i < dbInfoNum; ++i) {
    char* dbFName = taosArrayGet(pReq->pDbInfo, i);
    if (NULL == dbFName) {
      qError("taosArrayGet the %dth db in pDbInfo failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_DB_INFO, dbFName, NULL));
  }

#if 0
  for (int32_t i = 0; i < tbMetaNum; ++i) {
    SName* name = taosArrayGet(pReq->pTableMeta, i);
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_TB_META, name, NULL));
  }
#else
  if (tbMetaNum > 0) {
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_TB_META_BATCH, pReq->pTableMeta, NULL));
  }
#endif

#if 0
  for (int32_t i = 0; i < tbHashNum; ++i) {
    SName* name = taosArrayGet(pReq->pTableHash, i);
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_TB_HASH, name, NULL));
  }
#else
  if (tbHashNum > 0) {
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_TB_HASH_BATCH, pReq->pTableHash, NULL));
  }
#endif

  for (int32_t i = 0; i < tbIndexNum; ++i) {
    SName* name = taosArrayGet(pReq->pTableIndex, i);
    if (NULL == name) {
      qError("taosArrayGet the %dth tb in pTableIndex failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_TB_SMA_INDEX, name, NULL));
  }

  for (int32_t i = 0; i < tbCfgNum; ++i) {
    SName* name = taosArrayGet(pReq->pTableCfg, i);
    if (NULL == name) {
      qError("taosArrayGet the %dth tb in pTableCfg failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_TB_CFG, name, NULL));
  }

  for (int32_t i = 0; i < tbTagNum; ++i) {
    SName* name = taosArrayGet(pReq->pTableTag, i);
    if (NULL == name) {
      qError("taosArrayGet the %dth tb in pTableTag failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_TB_TAG, name, NULL));
  }

  for (int32_t i = 0; i < indexNum; ++i) {
    char* indexName = taosArrayGet(pReq->pIndex, i);
    if (NULL == indexName) {
      qError("taosArrayGet the %dth index in pIndex failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_INDEX_INFO, indexName, NULL));
  }

  for (int32_t i = 0; i < udfNum; ++i) {
    char* udfName = taosArrayGet(pReq->pUdf, i);
    if (NULL == udfName) {
      qError("taosArrayGet the %dth udf in pUdf failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_UDF, udfName, NULL));
  }

  for (int32_t i = 0; i < userNum; ++i) {
    SUserAuthInfo* user = taosArrayGet(pReq->pUser, i);
    if (NULL == user) {
      qError("taosArrayGet the %dth user in pUser failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_USER, user, NULL));
  }

  if (viewNum > 0) {
    SCtgViewTaskParam param = {.forceFetch = pReq->forceFetchViewMeta, .pTableReqs = pReq->pView};
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_VIEW, &param, NULL));
  }
  if (tbTsmaNum > 0) {
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_TB_TSMA, pReq->pTableTSMAs, NULL));
  }
  if (tsmaNum > 0) {
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_TSMA, pReq->pTSMAs, NULL));
  }
  if (tbNameNum > 0) {
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_TB_NAME, pReq->pTableName, NULL));
  }
  if (qnodeNum) {
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_QNODE, NULL, NULL));
  }
  if (dnodeNum) {
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_DNODE, NULL, NULL));
  }
  if (svrVerNum) {
    CTG_ERR_JRET(ctgInitTask(pJob, CTG_TASK_GET_SVR_VER, NULL, NULL));
  }

  pJob->refId = taosAddRef(gCtgMgmt.jobPool, pJob);
  if (pJob->refId < 0) {
    ctgError("add job to ref failed, error: %s", tstrerror(terrno));
    CTG_ERR_JRET(terrno);
  }

  void* p = taosAcquireRef(gCtgMgmt.jobPool, pJob->refId);
  if (NULL == p) {
    ctgError("acquire job from ref failed, refId:%" PRId64 ", error: %s", pJob->refId, tstrerror(terrno));
    CTG_ERR_JRET(terrno);
  }

  double el = (taosGetTimestampUs() - st) / 1000.0;
  qDebug("QID:0x%" PRIx64 ", jobId: 0x%" PRIx64 " initialized, task num %d, forceUpdate %d, elapsed time:%.2f ms",
         pJob->queryId, pJob->refId, taskNum, pReq->forceUpdate, el);
  return TSDB_CODE_SUCCESS;

_return:

  ctgFreeJob(*job);
  *job = NULL;

  CTG_RET(code);
}

int32_t ctgDumpTbMetaRes(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pTableMeta) {
    pJob->jobRes.pTableMeta = taosArrayInit(pJob->tbMetaNum, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pTableMeta) {
      CTG_ERR_RET(terrno);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  if (NULL == taosArrayPush(pJob->jobRes.pTableMeta, &res)) {
    CTG_ERR_RET(terrno);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpTbMetasRes(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;

  pJob->jobRes.pTableMeta = pTask->res;

  return TSDB_CODE_SUCCESS;
}

static int32_t ctgDumpTbNamesRes(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;

  pJob->jobRes.pTableMeta = pTask->res;

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpDbVgRes(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pDbVgroup) {
    pJob->jobRes.pDbVgroup = taosArrayInit(pJob->dbVgNum, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pDbVgroup) {
      CTG_ERR_RET(terrno);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  if (NULL == taosArrayPush(pJob->jobRes.pDbVgroup, &res)) {
    CTG_ERR_RET(terrno);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpTbHashRes(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pTableHash) {
    pJob->jobRes.pTableHash = taosArrayInit(pJob->tbHashNum, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pTableHash) {
      CTG_ERR_RET(terrno);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  if (NULL == taosArrayPush(pJob->jobRes.pTableHash, &res)) {
    CTG_ERR_RET(terrno);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpTbHashsRes(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;

  pJob->jobRes.pTableHash = pTask->res;

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpTbIndexRes(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pTableIndex) {
    SArray* pRes = taosArrayInit(pJob->tbIndexNum, sizeof(SMetaRes));
    if (atomic_val_compare_exchange_ptr(&pJob->jobRes.pTableIndex, NULL, pRes)) {
      taosArrayDestroy(pRes);
    }

    if (NULL == pJob->jobRes.pTableIndex) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  if (NULL == taosArrayPush(pJob->jobRes.pTableIndex, &res)) {
    CTG_ERR_RET(terrno);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpTbCfgRes(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pTableCfg) {
    SArray* pRes = taosArrayInit(pJob->tbCfgNum, sizeof(SMetaRes));
    if (atomic_val_compare_exchange_ptr(&pJob->jobRes.pTableCfg, NULL, pRes)) {
      taosArrayDestroy(pRes);
    }

    if (NULL == pJob->jobRes.pTableCfg) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  if (NULL == taosArrayPush(pJob->jobRes.pTableCfg, &res)) {
    CTG_ERR_RET(terrno);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpTbTagRes(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pTableTag) {
    SArray* pRes = taosArrayInit(pJob->tbTagNum, sizeof(SMetaRes));
    if (atomic_val_compare_exchange_ptr(&pJob->jobRes.pTableTag, NULL, pRes)) {
      taosArrayDestroy(pRes);
    }

    if (NULL == pJob->jobRes.pTableTag) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  if (NULL == taosArrayPush(pJob->jobRes.pTableTag, &res)) {
    CTG_ERR_RET(terrno);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpIndexRes(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pIndex) {
    pJob->jobRes.pIndex = taosArrayInit(pJob->indexNum, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pIndex) {
      CTG_ERR_RET(terrno);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  if (NULL == taosArrayPush(pJob->jobRes.pIndex, &res)) {
    CTG_ERR_RET(terrno);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpQnodeRes(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pQnodeList) {
    pJob->jobRes.pQnodeList = taosArrayInit(1, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pQnodeList) {
      CTG_ERR_RET(terrno);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  if (NULL == taosArrayPush(pJob->jobRes.pQnodeList, &res)) {
    CTG_ERR_RET(terrno);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpDnodeRes(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pDnodeList) {
    pJob->jobRes.pDnodeList = taosArrayInit(1, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pDnodeList) {
      CTG_ERR_RET(terrno);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  if (NULL == taosArrayPush(pJob->jobRes.pDnodeList, &res)) {
    CTG_ERR_RET(terrno);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpDbCfgRes(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pDbCfg) {
    pJob->jobRes.pDbCfg = taosArrayInit(pJob->dbCfgNum, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pDbCfg) {
      CTG_ERR_RET(terrno);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  if (NULL == taosArrayPush(pJob->jobRes.pDbCfg, &res)) {
    CTG_ERR_RET(terrno);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpDbInfoRes(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pDbInfo) {
    pJob->jobRes.pDbInfo = taosArrayInit(pJob->dbInfoNum, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pDbInfo) {
      CTG_ERR_RET(terrno);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  if (NULL == taosArrayPush(pJob->jobRes.pDbInfo, &res)) {
    CTG_ERR_RET(terrno);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpUdfRes(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pUdfList) {
    pJob->jobRes.pUdfList = taosArrayInit(pJob->udfNum, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pUdfList) {
      CTG_ERR_RET(terrno);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  if (NULL == taosArrayPush(pJob->jobRes.pUdfList, &res)) {
    CTG_ERR_RET(terrno);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpUserRes(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pUser) {
    pJob->jobRes.pUser = taosArrayInit(pJob->userNum, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pUser) {
      CTG_ERR_RET(terrno);
    }
  }

  SMetaRes res = {.code = pTask->code, .pRes = pTask->res};
  if (NULL == taosArrayPush(pJob->jobRes.pUser, &res)) {
    CTG_ERR_RET(terrno);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpSvrVer(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;
  if (NULL == pJob->jobRes.pSvrVer) {
    pJob->jobRes.pSvrVer = taosMemoryCalloc(1, sizeof(SMetaRes));
    if (NULL == pJob->jobRes.pSvrVer) {
      CTG_ERR_RET(terrno);
    }
  }

  pJob->jobRes.pSvrVer->code = pTask->code;
  pJob->jobRes.pSvrVer->pRes = pTask->res;

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpViewsRes(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;

  pJob->jobRes.pView = pTask->res;

  return TSDB_CODE_SUCCESS;
}

int32_t ctgCallSubCb(SCtgTask* pTask) {
  int32_t code = 0;

  CTG_LOCK(CTG_WRITE, &pTask->lock);

  int32_t parentNum = taosArrayGetSize(pTask->pParents);
  for (int32_t i = 0; i < parentNum; ++i) {
    SCtgMsgCtx* pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, -1);
    SCtgTask*   pParent = taosArrayGetP(pTask->pParents, i);
    if (NULL == pParent) {
      qError("taosArrayGetP the %dth parent failed", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    pParent->subRes.code = pTask->code;
    if (TSDB_CODE_SUCCESS == pTask->code) {
      code = (*gCtgAsyncFps[pTask->type].cloneFp)(pTask, &pParent->subRes.res);
      if (code) {
        pParent->subRes.code = code;
      }
    }

    SCtgMsgCtx* pParMsgCtx = CTG_GET_TASK_MSGCTX(pParent, -1);

    pParMsgCtx->pBatchs = pMsgCtx->pBatchs;
    CTG_ERR_JRET(pParent->subRes.fp(pParent));
  }

_return:

  CTG_UNLOCK(CTG_WRITE, &pTask->lock);

  CTG_RET(code);
}

int32_t ctgCallUserCb(void* param) {
  SCtgJob* pJob = (SCtgJob*)param;

  qDebug("QID:0x%" PRIx64 " ctg start to call user cb with rsp %s", pJob->queryId, tstrerror(pJob->jobResCode));

  (*pJob->userFp)(&pJob->jobRes, pJob->userParam, pJob->jobResCode);

  qDebug("QID:0x%" PRIx64 " ctg end to call user cb", pJob->queryId);

  int64_t refId = pJob->refId;
  int32_t code = taosRemoveRef(gCtgMgmt.jobPool, refId);
  if (code) {
    qError("QID:0x%" PRIx64 " remove ctg job %" PRId64 " from jobPool failed, error:%s", pJob->queryId, refId,
           tstrerror(code));
  }

  return TSDB_CODE_SUCCESS;
}

void ctgUpdateJobErrCode(SCtgJob* pJob, int32_t errCode) {
  if (!NEED_CLIENT_REFRESH_VG_ERROR(errCode) || errCode == TSDB_CODE_SUCCESS) return;

  atomic_store_32(&pJob->jobResCode, errCode);
  qDebug("QID:0x%" PRIx64 " ctg job errCode updated to %s", pJob->queryId, tstrerror(errCode));
  return;
}

int32_t ctgHandleTaskEnd(SCtgTask* pTask, int32_t rspCode) {
  SCtgJob* pJob = pTask->pJob;
  int32_t  code = 0;

  if (CTG_TASK_DONE == pTask->status) {
    return TSDB_CODE_SUCCESS;
  }

  qDebug("QID:0x%" PRIx64 " task %d end with res %s", pJob->queryId, pTask->taskId, tstrerror(rspCode));

  pTask->code = rspCode;
  pTask->status = CTG_TASK_DONE;

  CTG_ERR_JRET(ctgCallSubCb(pTask));

  int32_t taskDone = atomic_add_fetch_32(&pJob->taskDone, 1);
  if (taskDone < taosArrayGetSize(pJob->pTasks)) {
    qDebug("QID:0x%" PRIx64 " task done: %d, total: %d", pJob->queryId, taskDone,
           (int32_t)taosArrayGetSize(pJob->pTasks));

    ctgUpdateJobErrCode(pJob, rspCode);
    return TSDB_CODE_SUCCESS;
  }

  CTG_ERR_JRET(ctgMakeAsyncRes(pJob));

_return:

  ctgUpdateJobErrCode(pJob, rspCode);

  int32_t newCode = taosAsyncExec(ctgCallUserCb, pJob, NULL);
  if (TSDB_CODE_SUCCESS == code && TSDB_CODE_SUCCESS != newCode) {
    code = newCode;
  }

  CTG_RET(code);
}

int32_t ctgHandleGetTbMetaRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  int32_t           code = 0;
  SCtgDBCache*      dbCache = NULL;
  SCtgTask*         pTask = tReq->pTask;
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgMsgCtx*       pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, tReq->msgIdx);
  SCtgTbMetaCtx*    ctx = (SCtgTbMetaCtx*)pTask->taskCtx;
  SName*            pName = ctx->pName;
  int32_t           flag = ctx->flag;
  int32_t*          vgId = &ctx->vgId;

  if (NULL == pMsgCtx) {
    ctgError("fail to get task msgCtx, taskType:%d", pTask->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  CTG_ERR_JRET(ctgProcessRspMsg(pMsgCtx->out, reqType, pMsg->pData, pMsg->len, rspCode, pMsgCtx->target));

  switch (reqType) {
    case TDMT_MND_USE_DB: {
      SUseDbOutput* pOut = (SUseDbOutput*)pMsgCtx->out;

      SVgroupInfo vgInfo = {0};
      CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, &pConn->mgmtEps, pOut->dbVgroup, pName, &vgInfo));

      ctgDebug("will refresh tbmeta, not supposed to be stb, tbName:%s, flag:%d", tNameGetTableName(pName), flag);

      *vgId = vgInfo.vgId;
      CTG_ERR_JRET(ctgGetTbMetaFromVnode(pCtg, pConn, pName, &vgInfo, NULL, tReq));

      return TSDB_CODE_SUCCESS;
    }
    case TDMT_MND_TABLE_META: {
      STableMetaOutput* pOut = (STableMetaOutput*)pMsgCtx->out;

      if (CTG_IS_META_NULL(pOut->metaType)) {
        if (CTG_FLAG_IS_STB(flag)) {
          char dbFName[TSDB_DB_FNAME_LEN] = {0};
          (void)tNameGetFullDbName(pName, dbFName);

          CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, dbFName, &dbCache));
          if (NULL != dbCache) {
            SVgroupInfo vgInfo = {0};
            CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, &pConn->mgmtEps, dbCache->vgCache.vgInfo, pName, &vgInfo));

            ctgDebug("will refresh tbmeta, supposed to be stb, tbName:%s, flag:%d", tNameGetTableName(pName), flag);

            *vgId = vgInfo.vgId;
            CTG_ERR_JRET(ctgGetTbMetaFromVnode(pCtg, pConn, pName, &vgInfo, NULL, tReq));

            ctgReleaseVgInfoToCache(pCtg, dbCache);
            dbCache = NULL;
          } else {
            SBuildUseDBInput input = {0};

            tstrncpy(input.db, dbFName, tListLen(input.db));
            input.vgVersion = CTG_DEFAULT_INVALID_VERSION;

            CTG_ERR_JRET(ctgGetDBVgInfoFromMnode(pCtg, pConn, &input, NULL, tReq));
          }

          return TSDB_CODE_SUCCESS;
        }

        ctgError("no tbmeta got, tbName:%s", tNameGetTableName(pName));
        (void)ctgRemoveTbMetaFromCache(pCtg, pName, false);  // update cache not fatal error

        CTG_ERR_JRET(CTG_ERR_CODE_TABLE_NOT_EXIST);
      }

      if (pMsgCtx->lastOut) {
        TSWAP(pMsgCtx->out, pMsgCtx->lastOut);
        STableMetaOutput* pLastOut = (STableMetaOutput*)pMsgCtx->out;
        TSWAP(pLastOut->tbMeta, pOut->tbMeta);
      }

      break;
    }
    case TDMT_VND_TABLE_META: {
      STableMetaOutput* pOut = (STableMetaOutput*)pMsgCtx->out;

      if (CTG_IS_META_NULL(pOut->metaType)) {
        ctgError("no tbmeta got, tbName:%s", tNameGetTableName(pName));
        (void)ctgRemoveTbMetaFromCache(pCtg, pName, false);  // update cache not fatal error
        CTG_ERR_JRET(CTG_ERR_CODE_TABLE_NOT_EXIST);
      }

      if (CTG_FLAG_IS_STB(flag)) {
        break;
      }

      if (CTG_IS_META_TABLE(pOut->metaType) && TSDB_SUPER_TABLE == pOut->tbMeta->tableType) {
        ctgDebug("will continue to refresh tbmeta since got stb, tbName:%s", tNameGetTableName(pName));

        taosMemoryFreeClear(pOut->tbMeta);

        CTG_RET(ctgGetTbMetaFromMnode(pCtg, pConn, pName, NULL, tReq));
      } else if (CTG_IS_META_BOTH(pOut->metaType)) {
        int32_t exist = 0;
        if (!CTG_FLAG_IS_FORCE_UPDATE(flag)) {
          SName stbName = *pName;
          TAOS_STRCPY(stbName.tname, pOut->tbName);
          SCtgTbMetaCtx stbCtx = {0};
          stbCtx.flag = flag;
          stbCtx.pName = &stbName;

          taosMemoryFreeClear(pOut->tbMeta);
          CTG_ERR_JRET(ctgReadTbMetaFromCache(pCtg, &stbCtx, &pOut->tbMeta));
          if (pOut->tbMeta) {
            exist = 1;
          }
        }

        if (0 == exist) {
          TSWAP(pMsgCtx->lastOut, pMsgCtx->out);
          CTG_RET(ctgGetTbMetaFromMnodeImpl(pCtg, pConn, pOut->dbFName, pOut->tbName, NULL, tReq));
        }
      }
      break;
    }
    default:
      ctgError("invalid reqType %d", reqType);
      CTG_ERR_JRET(TSDB_CODE_INVALID_MSG);
  }

  STableMetaOutput* pOut = (STableMetaOutput*)pMsgCtx->out;

  CTG_ERR_JRET(ctgUpdateTbMetaToCache(pCtg, pOut, flag & CTG_FLAG_SYNC_OP));

  if (CTG_IS_META_BOTH(pOut->metaType)) {
    TAOS_MEMCPY(pOut->tbMeta, &pOut->ctbMeta, sizeof(pOut->ctbMeta));
  }

  /*
    else if (CTG_IS_META_CTABLE(pOut->metaType)) {
      SName stbName = *pName;
      TAOS_STRCPY(stbName.tname, pOut->tbName);
      SCtgTbMetaCtx stbCtx = {0};
      stbCtx.flag = flag;
      stbCtx.pName = &stbName;

      CTG_ERR_JRET(ctgReadTbMetaFromCache(pCtg, &stbCtx, &pOut->tbMeta));
      if (NULL == pOut->tbMeta) {
        ctgDebug("stb no longer exist, stbName:%s", stbName.tname);
        CTG_ERR_JRET(ctgRelaunchGetTbMetaTask(pTask));

        return TSDB_CODE_SUCCESS;
      }

      TAOS_MEMCPY(pOut->tbMeta, &pOut->ctbMeta, sizeof(pOut->ctbMeta));
    }
  */

  TSWAP(pTask->res, pOut->tbMeta);

_return:

  if (dbCache) {
    ctgReleaseVgInfoToCache(pCtg, dbCache);
  }

  if (code) {
    ctgTaskError("Get table %d.%s.%s meta failed with error %s", pName->acctId, pName->dbname, pName->tname,
                 tstrerror(code));
  }
  if (pTask->res || code) {
    int32_t newCode = ctgHandleTaskEnd(pTask, code);
    if (TSDB_CODE_SUCCESS == code && TSDB_CODE_SUCCESS != newCode) {
      code = newCode;
    }
  }

  CTG_RET(code);
}

int32_t ctgHandleGetTbMetasRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  int32_t           code = 0;
  SCtgDBCache*      dbCache = NULL;
  SCtgTask*         pTask = tReq->pTask;
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgMsgCtx*       pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, tReq->msgIdx);
  SCtgTbMetasCtx*   ctx = (SCtgTbMetasCtx*)pTask->taskCtx;
  bool              taskDone = false;

  if (NULL == pMsgCtx) {
    ctgError("fail to get task msgCtx, taskType:%d", pTask->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  SCtgFetch* pFetch = taosArrayGet(ctx->pFetchs, tReq->msgIdx);
  if (NULL == pFetch) {
    ctgError("fail to get the %dth fetch, fetchNum:%d", tReq->msgIdx, (int32_t)taosArrayGetSize(ctx->pFetchs));
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  int32_t  flag = pFetch->flag;
  int32_t* vgId = &pFetch->vgId;
  SName*   pName = NULL;
  CTG_ERR_JRET(ctgGetFetchName(ctx->pNames, pFetch, &pName));

  CTG_ERR_JRET(ctgProcessRspMsg(pMsgCtx->out, reqType, pMsg->pData, pMsg->len, rspCode, pMsgCtx->target));

  switch (reqType) {
    case TDMT_MND_USE_DB: {
      SUseDbOutput* pOut = (SUseDbOutput*)pMsgCtx->out;

      SVgroupInfo vgInfo = {0};
      CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, &pConn->mgmtEps, pOut->dbVgroup, pName, &vgInfo));

      ctgTaskDebug("will refresh tbmeta, not supposed to be stb, tbName:%s, flag:%d", tNameGetTableName(pName), flag);

      *vgId = vgInfo.vgId;
      CTG_ERR_JRET(ctgGetTbMetaFromVnode(pCtg, pConn, pName, &vgInfo, NULL, tReq));

      return TSDB_CODE_SUCCESS;
    }
    case TDMT_MND_TABLE_META: {
      STableMetaOutput* pOut = (STableMetaOutput*)pMsgCtx->out;

      if (CTG_IS_META_NULL(pOut->metaType)) {
        if (CTG_FLAG_IS_STB(flag)) {
          char dbFName[TSDB_DB_FNAME_LEN] = {0};
          (void)tNameGetFullDbName(pName, dbFName);

          CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, dbFName, &dbCache));
          if (NULL != dbCache) {
            SVgroupInfo vgInfo = {0};
            CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, &pConn->mgmtEps, dbCache->vgCache.vgInfo, pName, &vgInfo));

            ctgTaskDebug("will refresh tbmeta, supposed to be stb, tbName:%s, flag:%d", tNameGetTableName(pName), flag);

            *vgId = vgInfo.vgId;
            CTG_ERR_JRET(ctgGetTbMetaFromVnode(pCtg, pConn, pName, &vgInfo, NULL, tReq));

            ctgReleaseVgInfoToCache(pCtg, dbCache);
            dbCache = NULL;
          } else {
            SBuildUseDBInput input = {0};

            tstrncpy(input.db, dbFName, tListLen(input.db));
            input.vgVersion = CTG_DEFAULT_INVALID_VERSION;

            CTG_ERR_JRET(ctgGetDBVgInfoFromMnode(pCtg, pConn, &input, NULL, tReq));
          }

          return TSDB_CODE_SUCCESS;
        }

        ctgTaskError("no tbmeta got, tbName:%s", tNameGetTableName(pName));
        (void)ctgRemoveTbMetaFromCache(pCtg, pName, false);  // cache update not fatal error

        CTG_ERR_JRET(CTG_ERR_CODE_TABLE_NOT_EXIST);
      }

      if (pMsgCtx->lastOut) {
        TSWAP(pMsgCtx->out, pMsgCtx->lastOut);
        STableMetaOutput* pLastOut = (STableMetaOutput*)pMsgCtx->out;
        TSWAP(pLastOut->tbMeta, pOut->tbMeta);
      }

      break;
    }
    case TDMT_VND_TABLE_META: {
      STableMetaOutput* pOut = (STableMetaOutput*)pMsgCtx->out;

      if (CTG_IS_META_NULL(pOut->metaType)) {
        ctgTaskError("no tbmeta got, tbName:%s", tNameGetTableName(pName));
        (void)ctgRemoveTbMetaFromCache(pCtg, pName, false);  // cache update not fatal error
        CTG_ERR_JRET(CTG_ERR_CODE_TABLE_NOT_EXIST);
      }

      if (CTG_FLAG_IS_STB(flag)) {
        break;
      }

      if (CTG_IS_META_TABLE(pOut->metaType) && TSDB_SUPER_TABLE == pOut->tbMeta->tableType) {
        ctgTaskDebug("will continue to refresh tbmeta since got stb, tbName:%s", tNameGetTableName(pName));

        taosMemoryFreeClear(pOut->tbMeta);

        CTG_RET(ctgGetTbMetaFromMnode(pCtg, pConn, pName, NULL, tReq));
      } else if (CTG_IS_META_BOTH(pOut->metaType)) {
        int32_t exist = 0;
        if (!CTG_FLAG_IS_FORCE_UPDATE(flag)) {
          SName stbName = *pName;
          TAOS_STRCPY(stbName.tname, pOut->tbName);
          SCtgTbMetaCtx stbCtx = {0};
          stbCtx.flag = flag;
          stbCtx.pName = &stbName;

          STableMeta* stbMeta = NULL;
          CTG_ERR_JRET(ctgReadTbMetaFromCache(pCtg, &stbCtx, &stbMeta));
          if (stbMeta && stbMeta->sversion >= pOut->tbMeta->sversion) {
            ctgTaskDebug("use cached stb meta, tbName:%s", tNameGetTableName(pName));
            exist = 1;
            taosMemoryFreeClear(stbMeta);
          } else {
            ctgTaskDebug("need to get/update stb meta, tbName:%s", tNameGetTableName(pName));
            taosMemoryFreeClear(pOut->tbMeta);
            taosMemoryFreeClear(stbMeta);
          }
        }

        if (0 == exist) {
          TSWAP(pMsgCtx->lastOut, pMsgCtx->out);
          CTG_RET(ctgGetTbMetaFromMnodeImpl(pCtg, pConn, pOut->dbFName, pOut->tbName, NULL, tReq));
        }
      }
      break;
    }
    default:
      ctgTaskError("invalid reqType %d", reqType);
      CTG_ERR_JRET(TSDB_CODE_INVALID_MSG);
  }

  STableMetaOutput* pOut = (STableMetaOutput*)pMsgCtx->out;

  (void)ctgUpdateTbMetaToCache(pCtg, pOut, false);  // cache update not fatal error

  if (CTG_IS_META_BOTH(pOut->metaType)) {
    TAOS_MEMCPY(pOut->tbMeta, &pOut->ctbMeta, sizeof(pOut->ctbMeta));
  }

  /*
    else if (CTG_IS_META_CTABLE(pOut->metaType)) {
      SName stbName = *pName;
      TAOS_STRCPY(stbName.tname, pOut->tbName);
      SCtgTbMetaCtx stbCtx = {0};
      stbCtx.flag = flag;
      stbCtx.pName = &stbName;

      CTG_ERR_JRET(ctgReadTbMetaFromCache(pCtg, &stbCtx, &pOut->tbMeta));
      if (NULL == pOut->tbMeta) {
        ctgDebug("stb no longer exist, stbName:%s", stbName.tname);
        CTG_ERR_JRET(ctgRelaunchGetTbMetaTask(pTask));

        return TSDB_CODE_SUCCESS;
      }

      TAOS_MEMCPY(pOut->tbMeta, &pOut->ctbMeta, sizeof(pOut->ctbMeta));
    }
  */

  SMetaRes* pRes = taosArrayGet(ctx->pResList, pFetch->resIdx);
  if (NULL == pRes) {
    ctgTaskError("fail to get the %dth res in pResList, resNum:%d", pFetch->resIdx,
                 (int32_t)taosArrayGetSize(ctx->pResList));
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  pRes->code = 0;
  pRes->pRes = pOut->tbMeta;
  pOut->tbMeta = NULL;
  if (0 == atomic_sub_fetch_32(&ctx->fetchNum, 1)) {
    TSWAP(pTask->res, ctx->pResList);
    taskDone = true;
  }

_return:

  if (dbCache) {
    ctgReleaseVgInfoToCache(pCtg, dbCache);
  }

  if (code) {
    SMetaRes* pRes = taosArrayGet(ctx->pResList, pFetch->resIdx);
    if (NULL == pRes) {
      ctgTaskError("fail to get the %dth res in pResList, resNum:%d", pFetch->resIdx,
                   (int32_t)taosArrayGetSize(ctx->pResList));
    } else {
      pRes->code = code;
      pRes->pRes = NULL;
      ctgTaskError("Get table %d.%s.%s meta failed with error %s", pName->acctId, pName->dbname, pName->tname,
                   tstrerror(code));
      if (0 == atomic_sub_fetch_32(&ctx->fetchNum, 1)) {
        TSWAP(pTask->res, ctx->pResList);
        taskDone = true;
      }
    }
  }

  if (pTask->res && taskDone) {
    int32_t newCode = ctgHandleTaskEnd(pTask, code);
    if (newCode && TSDB_CODE_SUCCESS == code) {
      code = newCode;
    }
  }

  CTG_RET(code);
}

static int32_t ctgHandleGetTbNamesRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  int32_t           code = 0;
  SCtgDBCache*      dbCache = NULL;
  SCtgTask*         pTask = tReq->pTask;
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgMsgCtx*       pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, tReq->msgIdx);
  SCtgTbNamesCtx*   ctx = (SCtgTbNamesCtx*)pTask->taskCtx;
  bool              taskDone = false;
  bool              lock = false;

  if (NULL == pMsgCtx) {
    ctgError("fail to get task msgCtx, taskType:%d", pTask->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  SCtgFetch* pFetch = taosArrayGet(ctx->pFetchs, tReq->msgIdx);
  if (NULL == pFetch) {
    ctgError("fail to get the %dth fetch, fetchNum:%d", tReq->msgIdx, (int32_t)taosArrayGetSize(ctx->pFetchs));
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  int32_t  flag = pFetch->flag;
  int32_t* vgId = &pFetch->vgId;
  SName*   pName = NULL;
  CTG_ERR_JRET(ctgGetFetchName(ctx->pNames, pFetch, &pName));

  if (reqType == TDMT_VND_TABLE_NAME) {
    taosWLockLatch(&ctx->lock);
    lock = true;
  }

  CTG_ERR_JRET(ctgProcessRspMsg(pMsgCtx->out, reqType, pMsg->pData, pMsg->len, rspCode, pMsgCtx->target));

  switch (reqType) {
    case TDMT_MND_USE_DB: {
      SUseDbOutput* pOut = (SUseDbOutput*)pMsgCtx->out;
      CTG_ERR_RET(ctgMakeVgArray(pOut->dbVgroup));
      SArray* pVgArray = NULL;
      TSWAP(pVgArray, pOut->dbVgroup->vgArray);
      int32_t vgSize = taosArrayGetSize(pVgArray);
      if (0 == vgSize) {
        taosArrayDestroy(pVgArray);
        ctgTaskError("no vgroup got, dbName:%s", pName->dbname);
        CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
      }
      for (int32_t i = 0; i < vgSize; ++i) {
        SVgroupInfo* vgInfo = TARRAY_GET_ELEM(pVgArray, i);
        if (NULL == vgInfo) {
          taosArrayDestroy(pVgArray);
          ctgTaskError("fail to get the %dth vgInfo, vgSize:%d", i, vgSize);
          CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
        }
        ctgTaskDebug("will refresh tbmeta, not supposed to be stb, tbName:%s, flag:%d, vgId:%d",
                     tNameGetTableName(pName), flag, vgInfo->vgId);
        // *vgId = vgInfo->vgId;
        if (i > 0) atomic_add_fetch_32(&ctx->fetchNum, 1);
        code = ctgGetTbMetaFromVnode(pCtg, pConn, pName, vgInfo, NULL, tReq);
        if (code) {
          taosArrayDestroy(pVgArray);
          CTG_ERR_JRET(code);
        }
      }
      taosArrayDestroy(pVgArray);

      return TSDB_CODE_SUCCESS;
    }
    case TDMT_MND_TABLE_META: {
      STableMetaOutput* pOut = (STableMetaOutput*)pMsgCtx->out;

      if (CTG_IS_META_NULL(pOut->metaType)) {
        if (CTG_FLAG_IS_STB(flag)) {
          char dbFName[TSDB_DB_FNAME_LEN] = {0};
          (void)tNameGetFullDbName(pName, dbFName);

          CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, dbFName, &dbCache));
          if (NULL != dbCache) {
            SVgroupInfo vgInfo = {0};
            CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, &pConn->mgmtEps, dbCache->vgCache.vgInfo, pName, &vgInfo));

            ctgTaskDebug("will refresh tbmeta, supposed to be stb, tbName:%s, flag:%d", tNameGetTableName(pName), flag);

            *vgId = vgInfo.vgId;
            CTG_ERR_JRET(ctgGetTbMetaFromVnode(pCtg, pConn, pName, &vgInfo, NULL, tReq));

            ctgReleaseVgInfoToCache(pCtg, dbCache);
            dbCache = NULL;
          } else {
            SBuildUseDBInput input = {0};

            tstrncpy(input.db, dbFName, tListLen(input.db));
            input.vgVersion = CTG_DEFAULT_INVALID_VERSION;

            CTG_ERR_JRET(ctgGetDBVgInfoFromMnode(pCtg, pConn, &input, NULL, tReq));
          }

          return TSDB_CODE_SUCCESS;
        }

        ctgTaskError("no tbmeta got, tbName:%s", tNameGetTableName(pName));
        (void)ctgRemoveTbMetaFromCache(pCtg, pName, false);  // cache update not fatal error

        CTG_ERR_JRET(CTG_ERR_CODE_TABLE_NOT_EXIST);
      }

      if (pMsgCtx->lastOut) {
        TSWAP(pMsgCtx->out, pMsgCtx->lastOut);
        STableMetaOutput* pLastOut = (STableMetaOutput*)pMsgCtx->out;
        TSWAP(pLastOut->tbMeta, pOut->tbMeta);
      }

      break;
    }
    case TDMT_VND_TABLE_NAME: {
      STableMetaOutput* pOut = (STableMetaOutput*)pMsgCtx->out;

      if (CTG_IS_META_NULL(pOut->metaType)) {
        ctgTaskError("no tbmeta got, tbName:%s", tNameGetTableName(pName));
        CTG_ERR_JRET(CTG_ERR_CODE_TABLE_NOT_EXIST);
      }

      break;
    }
    default:
      ctgTaskError("invalid reqType %d", reqType);
      CTG_ERR_JRET(TSDB_CODE_INVALID_MSG);
  }

  STableMetaOutput* pOut = (STableMetaOutput*)pMsgCtx->out;
  if (CTG_IS_META_BOTH(pOut->metaType)) {
    TAOS_MEMCPY(pOut->tbMeta, &pOut->ctbMeta, sizeof(pOut->ctbMeta));
  }

  SMetaRes* pRes = taosArrayGet(ctx->pResList, pFetch->resIdx);
  if (NULL == pRes) {
    ctgTaskError("fail to get the %dth res in pResList, resNum:%d", pFetch->resIdx,
                 (int32_t)taosArrayGetSize(ctx->pResList));
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (!pRes->pRes) {
    pRes->code = 0;
    pRes->pRes = pOut->tbMeta;
    pOut->tbMeta = NULL;
  } else {
    taosMemoryFreeClear(pOut->tbMeta);
  }

  if (0 == atomic_sub_fetch_32(&ctx->fetchNum, 1)) {
    TSWAP(pTask->res, ctx->pResList);
    taskDone = true;
  }

_return:
  if (dbCache) {
    ctgReleaseVgInfoToCache(pCtg, dbCache);
  }

  if (code) {
    SMetaRes* pRes = taosArrayGet(ctx->pResList, pFetch->resIdx);
    if (NULL == pRes) {
      ctgTaskError("fail to get the %dth res in pResList, resNum:%d", pFetch->resIdx,
                   (int32_t)taosArrayGetSize(ctx->pResList));
    } else {
      if (0 == atomic_sub_fetch_32(&ctx->fetchNum, 1)) {
        TSWAP(pTask->res, ctx->pResList);
        taskDone = true;
      }
      if (TDMT_VND_TABLE_NAME == reqType) {
        if (!pRes->pRes && (0 == pRes->code)) pRes->code = code;
      } else {
        pRes->pRes = NULL;
        pRes->code = code;
      }
      if (taskDone == true) {
        ctgTaskError("Get table %d.%s.%s meta failed with error %s", pName->acctId, pName->dbname, pName->tname,
                     tstrerror(code));
      }
    }
  }

  if (pTask->res && taskDone) {
    int32_t newCode = ctgHandleTaskEnd(pTask, code);
    if (newCode && TSDB_CODE_SUCCESS == code) {
      code = newCode;
    }
  }

  if (lock) {
    taosWUnLockLatch(&ctx->lock);
  }

  CTG_RET(code);
}

int32_t ctgHandleGetDbVgRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  int32_t      code = 0;
  SCtgTask*    pTask = tReq->pTask;
  SCtgDbVgCtx* ctx = (SCtgDbVgCtx*)pTask->taskCtx;
  SCatalog*    pCtg = pTask->pJob->pCtg;
  int32_t      newCode = TSDB_CODE_SUCCESS;

  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  switch (reqType) {
    case TDMT_MND_USE_DB: {
      SUseDbOutput* pOut = (SUseDbOutput*)pTask->msgCtx.out;
      SDBVgInfo*    pDb = NULL;

      CTG_ERR_JRET(ctgGenerateVgList(pCtg, pOut->dbVgroup->vgHash, (SArray**)&pTask->res));

      CTG_ERR_JRET(cloneDbVgInfo(pOut->dbVgroup, &pDb));
      CTG_ERR_JRET(ctgUpdateVgroupEnqueue(pCtg, ctx->dbFName, pOut->dbId, pDb, false));

      break;
    }
    default:
      ctgError("invalid reqType %d", reqType);
      CTG_ERR_JRET(TSDB_CODE_INVALID_MSG);
  }

_return:

  newCode = ctgHandleTaskEnd(pTask, code);
  if (newCode && TSDB_CODE_SUCCESS == code) {
    code = newCode;
  }

  CTG_RET(code);
}

int32_t ctgHandleGetTbHashRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  int32_t        code = 0;
  SCtgTask*      pTask = tReq->pTask;
  SCtgTbHashCtx* ctx = (SCtgTbHashCtx*)pTask->taskCtx;
  SCatalog*      pCtg = pTask->pJob->pCtg;
  int32_t        newCode = TSDB_CODE_SUCCESS;

  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  switch (reqType) {
    case TDMT_MND_USE_DB: {
      SUseDbOutput* pOut = (SUseDbOutput*)pTask->msgCtx.out;

      pTask->res = taosMemoryMalloc(sizeof(SVgroupInfo));
      if (NULL == pTask->res) {
        CTG_ERR_JRET(terrno);
      }

      CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, &pTask->pJob->conn.mgmtEps, pOut->dbVgroup, ctx->pName,
                                             (SVgroupInfo*)pTask->res));

      CTG_ERR_JRET(ctgUpdateVgroupEnqueue(pCtg, ctx->dbFName, pOut->dbId, pOut->dbVgroup, false));
      pOut->dbVgroup = NULL;

      break;
    }
    default:
      ctgError("invalid reqType %d", reqType);
      CTG_ERR_JRET(TSDB_CODE_INVALID_MSG);
  }

_return:

  newCode = ctgHandleTaskEnd(pTask, code);
  if (newCode && TSDB_CODE_SUCCESS == code) {
    code = newCode;
  }

  CTG_RET(code);
}

int32_t ctgHandleGetTbHashsRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  int32_t         code = 0;
  SCtgTask*       pTask = tReq->pTask;
  SCtgTbHashsCtx* ctx = (SCtgTbHashsCtx*)pTask->taskCtx;
  SCatalog*       pCtg = pTask->pJob->pCtg;
  bool            taskDone = false;
  SCtgMsgCtx*     pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, tReq->msgIdx);
  if (NULL == pMsgCtx) {
    ctgError("fail to get task msgCtx, taskType:%d", pTask->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  SCtgFetch* pFetch = taosArrayGet(ctx->pFetchs, tReq->msgIdx);
  if (NULL == pFetch) {
    ctgError("fail to get the %dth fetch, fetchNum:%d", tReq->msgIdx, (int32_t)taosArrayGetSize(ctx->pFetchs));
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  CTG_ERR_JRET(ctgProcessRspMsg(pMsgCtx->out, reqType, pMsg->pData, pMsg->len, rspCode, pMsgCtx->target));

  switch (reqType) {
    case TDMT_MND_USE_DB: {
      SUseDbOutput* pOut = (SUseDbOutput*)pMsgCtx->out;

      STablesReq* pReq = taosArrayGet(ctx->pNames, pFetch->dbIdx);
      if (NULL == pReq) {
        ctgError("fail to get the %dth tb in ctx->pNames, reqNum:%d", pFetch->dbIdx,
                 (int32_t)taosArrayGetSize(ctx->pNames));
        CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
      }

      CTG_ERR_JRET(ctgGetVgInfosFromHashValue(pCtg, &pTask->pJob->conn.mgmtEps, tReq, pOut->dbVgroup, ctx,
                                              pMsgCtx->target, pReq->pTables, true));

      CTG_ERR_JRET(ctgUpdateVgroupEnqueue(pCtg, pMsgCtx->target, pOut->dbId, pOut->dbVgroup, false));
      pOut->dbVgroup = NULL;

      break;
    }
    default:
      ctgError("invalid reqType %d", reqType);
      CTG_ERR_JRET(TSDB_CODE_INVALID_MSG);
  }

  if (0 == atomic_sub_fetch_32(&ctx->fetchNum, 1)) {
    TSWAP(pTask->res, ctx->pResList);
    taskDone = true;
  }

_return:

  if (code) {
    STablesReq* pReq = taosArrayGet(ctx->pNames, pFetch->dbIdx);
    if (NULL == pReq) {
      ctgError("fail to get the %dth tb in ctx->pNames, reqNum:%d", pFetch->dbIdx,
               (int32_t)taosArrayGetSize(ctx->pNames));
    } else {
      int32_t num = taosArrayGetSize(pReq->pTables);
      for (int32_t i = 0; i < num; ++i) {
        SMetaRes* pRes = taosArrayGet(ctx->pResList, pFetch->resIdx + i);
        if (NULL == pRes) {
          ctgError("fail to get the %dth res in ctx->pResList, resNum:%d", pFetch->resIdx + i,
                   (int32_t)taosArrayGetSize(ctx->pResList));
        } else {
          pRes->code = code;
          pRes->pRes = NULL;
        }
      }

      if (0 == atomic_sub_fetch_32(&ctx->fetchNum, 1)) {
        TSWAP(pTask->res, ctx->pResList);
        taskDone = true;
      }
    }
  }

  if (pTask->res && taskDone) {
    int32_t newCode = ctgHandleTaskEnd(pTask, code);
    if (newCode && TSDB_CODE_SUCCESS == code) {
      code = newCode;
    }
  }

  CTG_RET(code);
}

int32_t ctgHandleGetTbIndexRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  int32_t   code = 0;
  SCtgTask* pTask = tReq->pTask;
  int32_t   newCode = TSDB_CODE_SUCCESS;

  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  STableIndex* pOut = (STableIndex*)pTask->msgCtx.out;
  SArray*      pInfo = NULL;
  CTG_ERR_JRET(ctgCloneTableIndex(pOut->pIndex, &pInfo));
  pTask->res = pInfo;

  CTG_ERR_JRET(ctgUpdateTbIndexEnqueue(pTask->pJob->pCtg, (STableIndex**)&pTask->msgCtx.out, false));

_return:

  if (TSDB_CODE_MND_DB_INDEX_NOT_EXIST == code) {
    code = TSDB_CODE_SUCCESS;
  }

  newCode = ctgHandleTaskEnd(pTask, code);
  if (newCode && TSDB_CODE_SUCCESS == code) {
    code = newCode;
  }

  CTG_RET(code);
}

int32_t ctgHandleGetTbCfgRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  int32_t   code = 0;
  SCtgTask* pTask = tReq->pTask;
  int32_t   newCode = TSDB_CODE_SUCCESS;

  CTG_ERR_JRET(ctgProcessRspMsg(&pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  TSWAP(pTask->res, pTask->msgCtx.out);

_return:

  newCode = ctgHandleTaskEnd(pTask, code);
  if (newCode && TSDB_CODE_SUCCESS == code) {
    code = newCode;
  }

  CTG_RET(code);
}

int32_t ctgHandleGetTbTagRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  int32_t   code = 0;
  SCtgTask* pTask = tReq->pTask;
  SCatalog* pCtg = pTask->pJob->pCtg;
  int32_t   newCode = TSDB_CODE_SUCCESS;

  CTG_ERR_JRET(ctgProcessRspMsg(&pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  STableCfgRsp* pRsp = (STableCfgRsp*)pTask->msgCtx.out;
  if (NULL == pRsp->pTags || pRsp->tagsLen <= 0) {
    ctgError("invalid tag in tbCfg rsp, pTags:%p, len:%d", pRsp->pTags, pRsp->tagsLen);
    CTG_ERR_JRET(TSDB_CODE_INVALID_MSG);
  }

  SArray* pTagVals = NULL;
  STag*   pTag = (STag*)pRsp->pTags;

  if (tTagIsJson(pTag)) {
    pTagVals = taosArrayInit(1, sizeof(STagVal));
    if (NULL == pTagVals) {
      CTG_ERR_JRET(terrno);
    }

    char* pJson = NULL;
    parseTagDatatoJson(pTag, &pJson);
    if (NULL == pJson) {
      taosArrayDestroy(pTagVals);
      CTG_ERR_JRET(terrno);
    }
    STagVal tagVal;
    tagVal.cid = 0;
    tagVal.type = TSDB_DATA_TYPE_JSON;
    tagVal.pData = pJson;
    tagVal.nData = strlen(pJson);
    if (NULL == taosArrayPush(pTagVals, &tagVal)) {
      taosMemoryFree(pJson);
      taosArrayDestroy(pTagVals);
      CTG_ERR_JRET(terrno);
    }
  } else {
    CTG_ERR_JRET(tTagToValArray((const STag*)pRsp->pTags, &pTagVals));
  }

  pTask->res = pTagVals;

_return:

  newCode = ctgHandleTaskEnd(pTask, code);
  if (newCode && TSDB_CODE_SUCCESS == code) {
    code = newCode;
  }

  CTG_RET(code);
}

int32_t ctgHandleGetDbCfgRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  int32_t       code = 0;
  SCtgTask*     pTask = tReq->pTask;
  SCtgDbCfgCtx* ctx = pTask->taskCtx;
  int32_t       newCode = TSDB_CODE_SUCCESS;

  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  SDbCfgInfo* pCfg = NULL;
  CTG_ERR_JRET(ctgCloneDbCfgInfo(pTask->msgCtx.out, &pCfg));

  CTG_ERR_RET(ctgUpdateDbCfgEnqueue(pTask->pJob->pCtg, ctx->dbFName, pCfg->dbId, pCfg, false));

  TSWAP(pTask->res, pTask->msgCtx.out);

_return:

  newCode = ctgHandleTaskEnd(pTask, code);
  if (newCode && TSDB_CODE_SUCCESS == code) {
    code = newCode;
  }

  CTG_RET(code);
}

int32_t ctgHandleGetDbInfoRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  CTG_RET(TSDB_CODE_APP_ERROR);
}

int32_t ctgHandleGetQnodeRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  int32_t   code = 0;
  SCtgTask* pTask = tReq->pTask;
  int32_t   newCode = TSDB_CODE_SUCCESS;
  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  TSWAP(pTask->res, pTask->msgCtx.out);

_return:

  newCode = ctgHandleTaskEnd(pTask, code);
  if (newCode && TSDB_CODE_SUCCESS == code) {
    code = newCode;
  }

  CTG_RET(code);
}

int32_t ctgHandleGetDnodeRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  int32_t   code = 0;
  SCtgTask* pTask = tReq->pTask;
  int32_t   newCode = TSDB_CODE_SUCCESS;

  CTG_ERR_JRET(ctgProcessRspMsg(&pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  TSWAP(pTask->res, pTask->msgCtx.out);

_return:

  newCode = ctgHandleTaskEnd(pTask, code);
  if (newCode && TSDB_CODE_SUCCESS == code) {
    code = newCode;
  }

  CTG_RET(code);
}

int32_t ctgHandleGetIndexRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  int32_t   code = 0;
  SCtgTask* pTask = tReq->pTask;
  int32_t   newCode = TSDB_CODE_SUCCESS;
  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  TSWAP(pTask->res, pTask->msgCtx.out);

_return:

  newCode = ctgHandleTaskEnd(pTask, code);
  if (newCode && TSDB_CODE_SUCCESS == code) {
    code = newCode;
  }

  CTG_RET(code);
}

int32_t ctgHandleGetUdfRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  int32_t   code = 0;
  SCtgTask* pTask = tReq->pTask;
  int32_t   newCode = TSDB_CODE_SUCCESS;
  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  TSWAP(pTask->res, pTask->msgCtx.out);

_return:

  newCode = ctgHandleTaskEnd(pTask, code);
  if (newCode && TSDB_CODE_SUCCESS == code) {
    code = newCode;
  }

  CTG_RET(code);
}

int32_t ctgHandleGetUserRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  int32_t          code = 0;
  SCtgTask*        pTask = tReq->pTask;
  SCatalog*        pCtg = pTask->pJob->pCtg;
  SGetUserAuthRsp* pOut = (SGetUserAuthRsp*)pTask->msgCtx.out;
  int32_t          newCode = TSDB_CODE_SUCCESS;

  CTG_ERR_JRET(ctgProcessRspMsg(pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  (void)ctgUpdateUserEnqueue(pCtg, pOut, true);  // cache update not fatal error
  taosMemoryFreeClear(pTask->msgCtx.out);

  CTG_ERR_JRET((*gCtgAsyncFps[pTask->type].launchFp)(pTask));

  return TSDB_CODE_SUCCESS;

_return:

  newCode = ctgHandleTaskEnd(pTask, code);
  if (newCode && TSDB_CODE_SUCCESS == code) {
    code = newCode;
  }

  CTG_RET(code);
}

int32_t ctgHandleGetSvrVerRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  int32_t   code = 0;
  SCtgTask* pTask = tReq->pTask;
  int32_t   newCode = TSDB_CODE_SUCCESS;

  CTG_ERR_JRET(ctgProcessRspMsg(&pTask->msgCtx.out, reqType, pMsg->pData, pMsg->len, rspCode, pTask->msgCtx.target));

  TSWAP(pTask->res, pTask->msgCtx.out);

_return:

  newCode = ctgHandleTaskEnd(pTask, code);
  if (newCode && TSDB_CODE_SUCCESS == code) {
    code = newCode;
  }

  CTG_RET(code);
}

int32_t ctgHandleGetViewsRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  int32_t           code = 0;
  SCtgTask*         pTask = tReq->pTask;
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgMsgCtx*       pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, tReq->msgIdx);
  SCtgViewsCtx*     ctx = (SCtgViewsCtx*)pTask->taskCtx;
  SCtgFetch*        pFetch = taosArrayGet(ctx->pFetchs, tReq->msgIdx);
  bool              taskDone = false;

  if (NULL == pMsgCtx) {
    ctgError("fail to get task msgCtx, taskType:%d", pTask->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == pFetch) {
    ctgError("fail to get the %dth fetch, fetchNum:%d", tReq->msgIdx, (int32_t)taosArrayGetSize(ctx->pFetchs));
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  int32_t  flag = pFetch->flag;
  int32_t* vgId = &pFetch->vgId;
  SName*   pName = NULL;

  CTG_ERR_JRET(ctgGetFetchName(ctx->pNames, pFetch, &pName));

  CTG_ERR_JRET(ctgProcessRspMsg(pMsgCtx->out, reqType, pMsg->pData, pMsg->len, rspCode, pMsgCtx->target));

  SViewMetaRsp* pRsp = *(SViewMetaRsp**)pMsgCtx->out;
  SViewMeta*    pViewMeta = taosMemoryCalloc(1, sizeof(SViewMeta));
  if (NULL == pViewMeta) {
    CTG_ERR_JRET(terrno);
  }

  code = dupViewMetaFromRsp(pRsp, pViewMeta);
  if (TSDB_CODE_SUCCESS != code) {
    ctgFreeSViewMeta(pViewMeta);
    taosMemoryFree(pViewMeta);
    CTG_ERR_JRET(code);
  }

  ctgDebug("start to update view meta to cache, view:%s, querySQL:%s", pRsp->name, pRsp->querySql);
  (void)ctgUpdateViewMetaToCache(pCtg, pRsp, false);  // cache update not fatal error
  taosMemoryFreeClear(pMsgCtx->out);
  pRsp = NULL;

  SMetaRes* pRes = taosArrayGet(ctx->pResList, pFetch->resIdx);
  if (NULL == pRes) {
    ctgTaskError("fail to get the %dth res in ctx->pResList, totalResNum:%d", pFetch->resIdx,
                 (int32_t)taosArrayGetSize(ctx->pResList));
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  pRes->code = 0;
  pRes->pRes = pViewMeta;
  if (0 == atomic_sub_fetch_32(&ctx->fetchNum, 1)) {
    TSWAP(pTask->res, ctx->pResList);
    taskDone = true;
  }

_return:

  if (code) {
    SMetaRes* pRes = taosArrayGet(ctx->pResList, pFetch->resIdx);
    if (NULL == pRes) {
      ctgTaskError("fail to get the %dth res in ctx->pResList, totalResNum:%d", pFetch->resIdx,
                   (int32_t)taosArrayGetSize(ctx->pResList));
    } else {
      pRes->code = code;
      pRes->pRes = NULL;
      if (TSDB_CODE_MND_VIEW_NOT_EXIST == code) {
        ctgTaskDebug("Get view %d.%s.%s meta failed with %s", pName->acctId, pName->dbname, pName->tname,
                     tstrerror(code));
      } else {
        ctgTaskError("Get view %d.%s.%s meta failed with error %s", pName->acctId, pName->dbname, pName->tname,
                     tstrerror(code));
      }
      if (0 == atomic_sub_fetch_32(&ctx->fetchNum, 1)) {
        TSWAP(pTask->res, ctx->pResList);
        taskDone = true;
      }
    }
  }

  if (pTask->res && taskDone) {
    int32_t newCode = ctgHandleTaskEnd(pTask, code);
    if (newCode && TSDB_CODE_SUCCESS == code) {
      code = newCode;
    }
  }

  CTG_RET(code);
}

static int32_t ctgTsmaFetchStreamProgress(SCtgTaskReq* tReq, SHashObj* pVgHash, const STableTSMAInfoRsp* pTsmas) {
  int32_t           code = 0;
  SCtgTask*         pTask = tReq->pTask;
  SCatalog*         pCtg = pTask->pJob->pCtg;
  int32_t           subFetchIdx = 0;
  SCtgTbTSMACtx*    pCtx = pTask->taskCtx;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SVgroupInfo*      pVgInfo = NULL;
  SCtgTSMAFetch*    pFetch = taosArrayGet(pCtx->pFetches, tReq->msgIdx);
  if (NULL == pFetch) {
    ctgError("fail to get the %dth SCtgTSMAFetch, totalNum:%d", tReq->msgIdx,
             (int32_t)taosArrayGetSize(pCtx->pFetches));
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  STablesReq* pTbReq = taosArrayGet(pCtx->pNames, pFetch->dbIdx);
  if (NULL == pTbReq) {
    ctgError("fail to get the %dth STablesReq, totalNum:%d", pFetch->dbIdx, (int32_t)taosArrayGetSize(pCtx->pNames));
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  const SName* pTbName = taosArrayGet(pTbReq->pTables, pFetch->tbIdx);
  if (NULL == pTbName) {
    ctgError("fail to get the %dth SName, totalNum:%d", pFetch->tbIdx, (int32_t)taosArrayGetSize(pTbReq->pTables));
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  pFetch->vgNum = taosHashGetSize(pVgHash);
  for (int32_t i = 0; i < taosArrayGetSize(pTsmas->pTsmas); ++i) {
    STableTSMAInfo* pTsmaInfo = taosArrayGetP(pTsmas->pTsmas, i);
    if (NULL == pTsmaInfo) {
      ctgError("fail to get the %dth STableTSMAInfo, totalNum:%d", i, (int32_t)taosArrayGetSize(pTsmas->pTsmas));
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    pVgInfo = taosHashIterate(pVgHash, NULL);
    pTsmaInfo->reqTs = taosGetTimestampMs();
    while (pVgInfo) {
      // make StreamProgressReq, send it
      SStreamProgressReq req = {.fetchIdx = pFetch->fetchIdx,
                                .streamId = pTsmaInfo->streamUid,
                                .subFetchIdx = subFetchIdx++,
                                .vgId = pVgInfo->vgId};
      CTG_ERR_JRET(ctgGetStreamProgressFromVnode(pCtg, pConn, pTbName, pVgInfo, NULL, tReq, &req));
      pFetch->subFetchNum++;

      pVgInfo = taosHashIterate(pVgHash, pVgInfo);
    }
  }

_return:

  CTG_RET(code);
}

int32_t ctgHandleGetTSMARsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  int32_t        code = 0;
  SCtgTask*      pTask = tReq->pTask;
  SCatalog*      pCtg = pTask->pJob->pCtg;
  SCtgTbTSMACtx* pCtx = pTask->taskCtx;
  int32_t        newCode = TSDB_CODE_SUCCESS;
  SCtgMsgCtx*    pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, tReq->msgIdx);
  if (NULL == pMsgCtx) {
    ctgError("fail to get the %dth SCtgMsgCtx", tReq->msgIdx);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  STablesReq* pTbReq = taosArrayGet(pCtx->pNames, 0);
  if (NULL == pTbReq) {
    ctgError("fail to get the 0th STablesReq, totalNum:%d", (int32_t)taosArrayGetSize(pCtx->pNames));
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  SName* pName = taosArrayGet(pTbReq->pTables, 0);
  if (NULL == pName) {
    ctgError("fail to get the 0th SName, totalNum:%d", (int32_t)taosArrayGetSize(pTbReq->pTables));
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  SRequestConnInfo* pConn = &pTask->pJob->conn;
  CTG_ERR_JRET(ctgProcessRspMsg(pMsgCtx->out, reqType, pMsg->pData, pMsg->len, rspCode, pMsgCtx->target));

  switch (reqType) {
    case TDMT_MND_TABLE_META: {
      STableMetaOutput* pOut = (STableMetaOutput*)pMsgCtx->out;
      if (!CTG_IS_META_NULL(pOut->metaType)) {
        CTG_ERR_JRET(ctgUpdateTbMetaToCache(pCtg, pOut, CTG_FLAG_SYNC_OP));
      }

      break;
    }
    case TDMT_MND_GET_TSMA: {
      SMetaRes* pRes = taosArrayGet(pCtx->pResList, 0);
      if (NULL == pRes) {
        ctgError("fail to get the 0th SMetaRes, totalNum:%d", (int32_t)taosArrayGetSize(pCtx->pResList));
        CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
      }

      STableTSMAInfoRsp* pOut = pMsgCtx->out;
      pRes->code = 0;
      if (1 != pOut->pTsmas->size) {
        ctgError("invalid tsma num:%d", (int32_t)pOut->pTsmas->size);
        CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
      }

      pRes->pRes = pOut;
      pMsgCtx->out = NULL;
      TSWAP(pTask->res, pCtx->pResList);

      STableTSMAInfo* pTsma = taosArrayGetP(pOut->pTsmas, 0);
      if (NULL == pTsma) {
        ctgError("fail to get the 0th STableTSMAInfo, totalNum:%d", (int32_t)taosArrayGetSize(pOut->pTsmas));
        CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
      }

      int32_t exists = false;
      CTG_ERR_JRET(ctgTbMetaExistInCache(pCtg, pTsma->targetDbFName, pTsma->targetTb, &exists));
      if (!exists) {
        TSWAP(pMsgCtx->lastOut, pMsgCtx->out);
        CTG_RET(ctgGetTbMetaFromMnodeImpl(pCtg, pConn, pTsma->targetDbFName, pTsma->targetTb, NULL, tReq));
      }

      break;
    }
    default:
      ctgError("invalid reqType:%d while getting tsma rsp", reqType);
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

_return:

  if (code) {
    if (TSDB_CODE_MND_SMA_NOT_EXIST == code) {
      code = TSDB_CODE_SUCCESS;
    } else {
      ctgTaskError("Get tsma for %d.%s.%s failed with err: %s", pName->acctId, pName->dbname, pName->tname,
                   tstrerror(code));
    }
  }

  newCode = ctgHandleTaskEnd(pTask, code);
  if (newCode && TSDB_CODE_SUCCESS == code) {
    code = newCode;
  }

  CTG_RET(code);
}

int32_t ctgHandleGetTbTSMARsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode) {
  bool              taskDone = false;
  int32_t           code = 0;
  SCtgTask*         pTask = tReq->pTask;
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SCtgTbTSMACtx*    pCtx = pTask->taskCtx;
  SArray*           pTsmas = NULL;
  SHashObj*         pVgHash = NULL;
  SCtgDBCache*      pDbCache = NULL;
  STableTSMAInfo*   pTsma = NULL;
  SRequestConnInfo* pConn = &pTask->pJob->conn;

  SCtgMsgCtx* pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, tReq->msgIdx);
  if (NULL == pMsgCtx) {
    ctgError("fail to get the %dth SCtgMsgCtx", tReq->msgIdx);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  SCtgTSMAFetch* pFetch = taosArrayGet(pCtx->pFetches, tReq->msgIdx);
  if (NULL == pFetch) {
    ctgError("fail to get the %dth SCtgTSMAFetch, totalNum:%d", tReq->msgIdx,
             (int32_t)taosArrayGetSize(pCtx->pFetches));
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  SMetaRes* pRes = taosArrayGet(pCtx->pResList, pFetch->resIdx);
  if (NULL == pRes) {
    ctgError("fail to get the %dth SMetaRes, totalNum:%d", pFetch->resIdx, (int32_t)taosArrayGetSize(pCtx->pResList));
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  STablesReq* pTbReq = taosArrayGet(pCtx->pNames, pFetch->dbIdx);
  if (NULL == pTbReq) {
    ctgError("fail to get the %dth STablesReq, totalNum:%d", pFetch->dbIdx, (int32_t)taosArrayGetSize(pCtx->pNames));
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  SName* pTbName = taosArrayGet(pTbReq->pTables, pFetch->tbIdx);
  if (NULL == pTbName) {
    ctgError("fail to get the %dth SName, totalNum:%d", pFetch->tbIdx, (int32_t)taosArrayGetSize(pTbReq->pTables));
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (reqType != TDMT_VND_GET_STREAM_PROGRESS)
    CTG_ERR_JRET(ctgProcessRspMsg(pMsgCtx->out, reqType, pMsg->pData, pMsg->len, rspCode, pMsgCtx->target));

  switch (reqType) {
    case TDMT_MND_GET_TABLE_TSMA: {
      STableTSMAInfoRsp* pOut = pMsgCtx->out;
      pFetch->fetchType = FETCH_TSMA_STREAM_PROGRESS;
      pRes->pRes = pOut;
      pMsgCtx->out = NULL;

      if (pOut->pTsmas && taosArrayGetSize(pOut->pTsmas) > 0) {
        // fetch progress
        (void)ctgAcquireVgInfoFromCache(pCtg, pTbReq->dbFName, &pDbCache);  // ignore cache error

        if (!pDbCache) {
          // do not know which vnodes to fetch, fetch vnode list first
          SBuildUseDBInput input = {0};
          tstrncpy(input.db, pTbReq->dbFName, tListLen(input.db));
          input.vgVersion = CTG_DEFAULT_INVALID_VERSION;
          CTG_ERR_JRET(ctgGetDBVgInfoFromMnode(pCtg, pConn, &input, NULL, tReq));
        } else {
          // fetch progress from every vnode
          CTG_ERR_JRET(ctgTsmaFetchStreamProgress(tReq, pDbCache->vgCache.vgInfo->vgHash, pOut));
          ctgReleaseVgInfoToCache(pCtg, pDbCache);
          pDbCache = NULL;
        }
      } else {
        // no tsmas
        if (atomic_sub_fetch_32(&pCtx->fetchNum, 1) == 0) {
          TSWAP(pTask->res, pCtx->pResList);
          taskDone = true;
        }
      }

      break;
    }
    case TDMT_VND_GET_STREAM_PROGRESS: {
      SStreamProgressRsp rsp = {0};
      CTG_ERR_JRET(ctgProcessRspMsg(&rsp, reqType, pMsg->pData, pMsg->len, rspCode, pMsgCtx->target));

      // update progress into res
      STableTSMAInfoRsp*  pTsmasRsp = pRes->pRes;
      SArray*             pTsmas = pTsmasRsp->pTsmas;
      SStreamProgressRsp* pRsp = &rsp;
      int32_t             tsmaIdx = pRsp->subFetchIdx / pFetch->vgNum;
      STableTSMAInfo*     pTsmaInfo = taosArrayGetP(pTsmas, tsmaIdx);
      if (NULL == pTsmaInfo) {
        ctgError("fail to get the %dth STableTSMAInfo, totalNum:%d", tsmaIdx, (int32_t)taosArrayGetSize(pTsmas));
        CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
      }

      if (pTsmaInfo->rspTs == 0) {
        pTsmaInfo->fillHistoryFinished = true;
      }

      pTsmaInfo->rspTs = taosGetTimestampMs();
      pTsmaInfo->delayDuration = TMAX(pRsp->progressDelay, pTsmaInfo->delayDuration);
      pTsmaInfo->fillHistoryFinished = pTsmaInfo->fillHistoryFinished && pRsp->fillHisFinished;

      qDebug("received stream progress for tsma %s rsp history: %d vnode: %d, delay: %" PRId64, pTsmaInfo->name,
             pRsp->fillHisFinished, pRsp->subFetchIdx, pRsp->progressDelay);

      if (atomic_add_fetch_32(&pFetch->finishedSubFetchNum, 1) == pFetch->subFetchNum) {
        // subfetch all finished
        for (int32_t i = 0; i < taosArrayGetSize(pTsmas); ++i) {
          STableTSMAInfo* pInfo = taosArrayGetP(pTsmas, i);
          CTG_ERR_JRET(tCloneTbTSMAInfo(pInfo, &pTsma));
          CTG_ERR_JRET(ctgUpdateTbTSMAEnqueue(pCtg, &pTsma, 0, false));
        }

        if (atomic_sub_fetch_32(&pCtx->fetchNum, 1) == 0) {
          TSWAP(pTask->res, pCtx->pResList);
          taskDone = true;
        }
      }

      break;
    }
    case TDMT_MND_USE_DB: {
      SUseDbOutput* pOut = (SUseDbOutput*)pMsgCtx->out;

      switch (pFetch->fetchType) {
        case FETCH_TSMA_SOURCE_TB_META: {
          SVgroupInfo vgInfo = {0};
          CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, &pConn->mgmtEps, pOut->dbVgroup, pTbName, &vgInfo));

          pFetch->vgId = vgInfo.vgId;
          CTG_ERR_JRET(ctgGetTbMetaFromVnode(pCtg, pConn, pTbName, &vgInfo, NULL, tReq));

          break;
        }
        case FETCH_TSMA_STREAM_PROGRESS: {
          STableTSMAInfoRsp* pTsmas = pRes->pRes;
          TSWAP(pOut->dbVgroup->vgHash, pVgHash);
          CTG_ERR_JRET(ctgTsmaFetchStreamProgress(tReq, pVgHash, pTsmas));

          break;
        }
        default:
          ctgError("invalid fetchType:%d while getting tb tsma rsp", pFetch->fetchType);
          CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
      }

      break;
    }
    case TDMT_VND_TABLE_META: {
      // handle source tb meta
      STableMetaOutput* pOut = (STableMetaOutput*)pMsgCtx->out;
      pFetch->fetchType = FETCH_TB_TSMA;
      pFetch->tsmaSourceTbName = *pTbName;

      if (CTG_IS_META_NULL(pOut->metaType)) {
        ctgTaskError("no tbmeta found when fetching tsma source tb meta: %s.%s", pTbName->dbname, pTbName->tname);
        (void)ctgRemoveTbMetaFromCache(pCtg, pTbName, false);  // ignore cache error
        CTG_ERR_JRET(CTG_ERR_CODE_TABLE_NOT_EXIST);
      }

      if (META_TYPE_BOTH_TABLE == pOut->metaType) {
        // rewrite tsma fetch table with it's super table name
        (void)snprintf(pFetch->tsmaSourceTbName.tname, sizeof(pFetch->tsmaSourceTbName.tname), "%s", pOut->tbName);
      }

      CTG_ERR_JRET(ctgGetTbTSMAFromMnode(pCtg, pConn, &pFetch->tsmaSourceTbName, NULL, tReq, TDMT_MND_GET_TABLE_TSMA));

      break;
    }
    default:
      ctgError("invalid reqType:%d while getting tsma rsp", reqType);
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

_return:

  if (pDbCache) {
    ctgReleaseVgInfoToCache(pCtg, pDbCache);
  }
  if (pTsma) {
    tFreeAndClearTableTSMAInfo(pTsma);
    pTsma = NULL;
  }
  if (pVgHash) {
    taosHashCleanup(pVgHash);
  }
  if (code) {
    int32_t   newCode = TSDB_CODE_SUCCESS;
    SMetaRes* pRes = taosArrayGet(pCtx->pResList, pFetch->resIdx);
    if (NULL == pRes) {
      ctgError("fail to get the %dth SMetaRes, totalNum:%d", pFetch->resIdx, (int32_t)taosArrayGetSize(pCtx->pResList));
      newCode = TSDB_CODE_CTG_INTERNAL_ERROR;
    } else {
      pRes->code = code;
      if (TSDB_CODE_MND_SMA_NOT_EXIST == code) {
        code = TSDB_CODE_SUCCESS;
      } else {
        ctgTaskError("Get tsma for %d.%s.%s faield with err: %s", pTbName->acctId, pTbName->dbname, pTbName->tname,
                     tstrerror(code));
      }
    }

    if (newCode && TSDB_CODE_SUCCESS == code) {
      code = newCode;
    }

    bool allSubFetchFinished = false;
    if (pMsgCtx->reqType == TDMT_VND_GET_STREAM_PROGRESS) {
      allSubFetchFinished = atomic_add_fetch_32(&pFetch->finishedSubFetchNum, 1) >= pFetch->subFetchNum;
    }
    if ((allSubFetchFinished || pFetch->subFetchNum == 0) && 0 == atomic_sub_fetch_32(&pCtx->fetchNum, 1)) {
      TSWAP(pTask->res, pCtx->pResList);
      taskDone = true;
    }
  }

  if (pTask->res && taskDone) {
    int32_t newCode = ctgHandleTaskEnd(pTask, code);
    if (newCode && TSDB_CODE_SUCCESS == code) {
      code = newCode;
    }
  }

  CTG_RET(code);
}

int32_t ctgAsyncRefreshTbMeta(SCtgTaskReq* tReq, int32_t flag, SName* pName, int32_t* vgId) {
  SCtgTask*         pTask = tReq->pTask;
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  int32_t           code = 0;

  if (CTG_FLAG_IS_SYS_DB(flag)) {
    ctgDebug("will refresh sys db tbmeta, tbName:%s", tNameGetTableName(pName));

    CTG_RET(ctgGetTbMetaFromMnodeImpl(pCtg, pConn, (char*)pName->dbname, (char*)pName->tname, NULL, tReq));
  }

  if (CTG_FLAG_IS_STB(flag)) {
    ctgDebug("will refresh tbmeta, supposed to be stb, tbName:%s", tNameGetTableName(pName));

    // if get from mnode failed, will not try vnode
    CTG_RET(ctgGetTbMetaFromMnode(pCtg, pConn, pName, NULL, tReq));
  }

  SCtgDBCache* dbCache = NULL;
  char         dbFName[TSDB_DB_FNAME_LEN] = {0};
  (void)tNameGetFullDbName(pName, dbFName);

  CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, dbFName, &dbCache));
  if (dbCache) {
    SVgroupInfo vgInfo = {0};
    CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, &pConn->mgmtEps, dbCache->vgCache.vgInfo, pName, &vgInfo));

    ctgDebug("will refresh tbmeta, not supposed to be stb, tbName:%s, flag:%d", tNameGetTableName(pName), flag);

    *vgId = vgInfo.vgId;
    CTG_ERR_JRET(ctgGetTbMetaFromVnode(pCtg, pConn, pName, &vgInfo, NULL, tReq));
  } else {
    SBuildUseDBInput input = {0};

    tstrncpy(input.db, dbFName, tListLen(input.db));
    input.vgVersion = CTG_DEFAULT_INVALID_VERSION;

    CTG_ERR_JRET(ctgGetDBVgInfoFromMnode(pCtg, pConn, &input, NULL, tReq));
  }

_return:

  if (dbCache) {
    ctgReleaseVgInfoToCache(pCtg, dbCache);
  }

  CTG_RET(code);
}

int32_t ctgLaunchGetTbMetaTask(SCtgTask* pTask) {
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgJob*          pJob = pTask->pJob;
  SCtgMsgCtx*       pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, -1);
  if (NULL == pMsgCtx) {
    ctgError("fail to get task msgCtx, taskType:%d", pTask->type);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == pMsgCtx->pBatchs) {
    pMsgCtx->pBatchs = pJob->pBatchs;
  }

  CTG_ERR_RET(ctgGetTbMetaFromCache(pCtg, (SCtgTbMetaCtx*)pTask->taskCtx, (STableMeta**)&pTask->res));
  if (pTask->res) {
    CTG_ERR_RET(ctgHandleTaskEnd(pTask, 0));
    return TSDB_CODE_SUCCESS;
  }

  SCtgTbMetaCtx* pCtx = (SCtgTbMetaCtx*)pTask->taskCtx;
  SCtgTaskReq    tReq;
  tReq.pTask = pTask;
  tReq.msgIdx = -1;
  CTG_ERR_RET(ctgAsyncRefreshTbMeta(&tReq, pCtx->flag, pCtx->pName, &pCtx->vgId));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetTbMetasTask(SCtgTask* pTask) {
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgTbMetasCtx*   pCtx = (SCtgTbMetasCtx*)pTask->taskCtx;
  SCtgJob*          pJob = pTask->pJob;
  SName*            pName = NULL;

  int32_t dbNum = taosArrayGetSize(pCtx->pNames);
  int32_t fetchIdx = 0;
  int32_t baseResIdx = 0;
  for (int32_t i = 0; i < dbNum; ++i) {
    STablesReq* pReq = taosArrayGet(pCtx->pNames, i);
    if (NULL == pReq) {
      ctgError("fail to get the %dth STablesReq, num:%d", i, dbNum);
      CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
    }

    ctgDebug("start to check tb metas in db %s, tbNum %ld", pReq->dbFName, taosArrayGetSize(pReq->pTables));
    CTG_ERR_RET(ctgGetTbMetasFromCache(pCtg, pConn, pCtx, i, &fetchIdx, baseResIdx, pReq->pTables));
    baseResIdx += taosArrayGetSize(pReq->pTables);
  }

  pCtx->fetchNum = taosArrayGetSize(pCtx->pFetchs);
  if (pCtx->fetchNum <= 0) {
    TSWAP(pTask->res, pCtx->pResList);

    CTG_ERR_RET(ctgHandleTaskEnd(pTask, 0));
    return TSDB_CODE_SUCCESS;
  }

  pTask->msgCtxs = taosArrayInit_s(sizeof(SCtgMsgCtx), pCtx->fetchNum);
  if (NULL == pTask->msgCtxs) {
    ctgError("taosArrayInit_s %d SCtgMsgCtx %d failed", pCtx->fetchNum, (int32_t)sizeof(SCtgMsgCtx));
    CTG_ERR_RET(terrno);
  }

  for (int32_t i = 0; i < pCtx->fetchNum; ++i) {
    SCtgFetch* pFetch = taosArrayGet(pCtx->pFetchs, i);
    if (NULL == pFetch) {
      ctgError("fail to get the %dth fetch in pCtx->pFetchs, fetchNum:%d", i, pCtx->fetchNum);
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    CTG_ERR_RET(ctgGetFetchName(pCtx->pNames, pFetch, &pName));

    SCtgMsgCtx* pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, i);
    if (NULL == pMsgCtx) {
      ctgError("fail to get the %dth pMsgCtx", i);
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    if (NULL == pMsgCtx->pBatchs) {
      pMsgCtx->pBatchs = pJob->pBatchs;
    }

    SCtgTaskReq tReq;
    tReq.pTask = pTask;
    tReq.msgIdx = pFetch->fetchIdx;
    CTG_ERR_RET(ctgAsyncRefreshTbMeta(&tReq, pFetch->flag, pName, &pFetch->vgId));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetDbVgTask(SCtgTask* pTask) {
  int32_t           code = 0;
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgDBCache*      dbCache = NULL;
  SCtgDbVgCtx*      pCtx = (SCtgDbVgCtx*)pTask->taskCtx;
  SCtgJob*          pJob = pTask->pJob;
  SCtgMsgCtx*       pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, -1);
  if (NULL == pMsgCtx) {
    ctgError("fail to get the %dth pMsgCtx", -1);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == pMsgCtx->pBatchs) {
    pMsgCtx->pBatchs = pJob->pBatchs;
  }

  CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, pCtx->dbFName, &dbCache));
  if (NULL != dbCache) {
    if (pTask->subTask) {
      pMsgCtx->reqType = TDMT_MND_USE_DB;
      CTG_ERR_JRET(ctgBuildUseDbOutput((SUseDbOutput**)&pMsgCtx->out, dbCache->vgCache.vgInfo));
    }

    CTG_ERR_JRET(ctgGenerateVgList(pCtg, dbCache->vgCache.vgInfo->vgHash, (SArray**)&pTask->res));

    ctgReleaseVgInfoToCache(pCtg, dbCache);
    dbCache = NULL;

    CTG_ERR_JRET(ctgHandleTaskEnd(pTask, 0));
  } else {
    SBuildUseDBInput input = {0};

    tstrncpy(input.db, pCtx->dbFName, tListLen(input.db));
    input.vgVersion = CTG_DEFAULT_INVALID_VERSION;

    SCtgTaskReq tReq;
    tReq.pTask = pTask;
    tReq.msgIdx = -1;
    CTG_ERR_RET(ctgGetDBVgInfoFromMnode(pCtg, pConn, &input, NULL, &tReq));
  }

_return:

  if (dbCache) {
    ctgReleaseVgInfoToCache(pCtg, dbCache);
  }

  CTG_RET(code);
}

int32_t ctgLaunchGetTbHashTask(SCtgTask* pTask) {
  int32_t           code = 0;
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgDBCache*      dbCache = NULL;
  SCtgTbHashCtx*    pCtx = (SCtgTbHashCtx*)pTask->taskCtx;
  SCtgJob*          pJob = pTask->pJob;
  SCtgMsgCtx*       pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, -1);
  if (NULL == pMsgCtx) {
    ctgError("fail to get the %dth pMsgCtx", -1);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == pMsgCtx->pBatchs) {
    pMsgCtx->pBatchs = pJob->pBatchs;
  }

  CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, pCtx->dbFName, &dbCache));
  if (NULL != dbCache) {
    pTask->res = taosMemoryMalloc(sizeof(SVgroupInfo));
    if (NULL == pTask->res) {
      CTG_ERR_JRET(terrno);
    }
    CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, &pConn->mgmtEps, dbCache->vgCache.vgInfo, pCtx->pName,
                                           (SVgroupInfo*)pTask->res));

    ctgReleaseVgInfoToCache(pCtg, dbCache);
    dbCache = NULL;

    CTG_ERR_JRET(ctgHandleTaskEnd(pTask, 0));
  } else {
    SBuildUseDBInput input = {0};

    tstrncpy(input.db, pCtx->dbFName, tListLen(input.db));
    input.vgVersion = CTG_DEFAULT_INVALID_VERSION;

    SCtgTaskReq tReq;
    tReq.pTask = pTask;
    tReq.msgIdx = -1;
    CTG_ERR_RET(ctgGetDBVgInfoFromMnode(pCtg, pConn, &input, NULL, &tReq));
  }

_return:

  if (dbCache) {
    ctgReleaseVgInfoToCache(pCtg, dbCache);
  }

  CTG_RET(code);
}

int32_t ctgLaunchGetTbHashsTask(SCtgTask* pTask) {
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgTbHashsCtx*   pCtx = (SCtgTbHashsCtx*)pTask->taskCtx;
  SCtgDBCache*      dbCache = NULL;
  SCtgJob*          pJob = pTask->pJob;
  int32_t           dbNum = taosArrayGetSize(pCtx->pNames);
  int32_t           fetchIdx = 0;
  int32_t           baseResIdx = 0;
  int32_t           code = 0;

  for (int32_t i = 0; i < dbNum; ++i) {
    STablesReq* pReq = taosArrayGet(pCtx->pNames, i);
    if (NULL == pReq) {
      ctgError("fail to get the %dth STablesReq, dbNum:%d", i, dbNum);
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, pReq->dbFName, &dbCache));

    if (NULL != dbCache) {
      SCtgTaskReq tReq;
      tReq.pTask = pTask;
      tReq.msgIdx = -1;
      CTG_ERR_JRET(ctgGetVgInfosFromHashValue(pCtg, &pConn->mgmtEps, &tReq, dbCache->vgCache.vgInfo, pCtx,
                                              pReq->dbFName, pReq->pTables, false));

      ctgReleaseVgInfoToCache(pCtg, dbCache);
      dbCache = NULL;

      baseResIdx += taosArrayGetSize(pReq->pTables);
    } else {
      CTG_ERR_JRET(ctgAddFetch(&pCtx->pFetchs, i, -1, &fetchIdx, baseResIdx, 0));

      baseResIdx += taosArrayGetSize(pReq->pTables);
      int32_t inc = baseResIdx - taosArrayGetSize(pCtx->pResList);
      for (int32_t j = 0; j < inc; ++j) {
        if (NULL == taosArrayPush(pCtx->pResList, &(SMetaRes){0})) {
          CTG_ERR_JRET(terrno);
        }
      }
    }
  }

  pCtx->fetchNum = taosArrayGetSize(pCtx->pFetchs);
  if (pCtx->fetchNum <= 0) {
    TSWAP(pTask->res, pCtx->pResList);

    CTG_ERR_JRET(ctgHandleTaskEnd(pTask, 0));
    return TSDB_CODE_SUCCESS;
  }

  pTask->msgCtxs = taosArrayInit_s(sizeof(SCtgMsgCtx), pCtx->fetchNum);
  if (NULL == pTask->msgCtxs) {
    ctgError("taosArrayInit_s %d SCtgMsgCtx %d failed", pCtx->fetchNum, (int32_t)sizeof(SCtgMsgCtx));
    CTG_ERR_RET(terrno);
  }

  for (int32_t i = 0; i < pCtx->fetchNum; ++i) {
    SCtgFetch* pFetch = taosArrayGet(pCtx->pFetchs, i);
    if (NULL == pFetch) {
      ctgError("fail to get the %dth SCtgFetch, fetchNum:%d", i, pCtx->fetchNum);
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    STablesReq* pReq = taosArrayGet(pCtx->pNames, pFetch->dbIdx);
    if (NULL == pFetch) {
      ctgError("fail to get the %dth SCtgFetch, fetchNum:%d", i, pCtx->fetchNum);
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    SCtgMsgCtx* pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, i);
    if (NULL == pFetch) {
      ctgError("fail to get the %dth SCtgMsgCtx", i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    if (NULL == pMsgCtx->pBatchs) {
      pMsgCtx->pBatchs = pJob->pBatchs;
    }

    SBuildUseDBInput input = {0};
    TAOS_STRCPY(input.db, pReq->dbFName);

    input.vgVersion = CTG_DEFAULT_INVALID_VERSION;

    SCtgTaskReq tReq;
    tReq.pTask = pTask;
    tReq.msgIdx = pFetch->fetchIdx;
    CTG_ERR_RET(ctgGetDBVgInfoFromMnode(pCtg, pConn, &input, NULL, &tReq));
  }

_return:

  if (dbCache) {
    ctgReleaseVgInfoToCache(pCtg, dbCache);
  }

  return code;
}

int32_t ctgLaunchGetTbIndexTask(SCtgTask* pTask) {
  int32_t           code = 0;
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgTbIndexCtx*   pCtx = (SCtgTbIndexCtx*)pTask->taskCtx;
  SArray*           pRes = NULL;
  SCtgJob*          pJob = pTask->pJob;
  SCtgMsgCtx*       pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, -1);
  if (NULL == pMsgCtx) {
    ctgError("fail to get the %dth pMsgCtx", -1);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == pMsgCtx->pBatchs) {
    pMsgCtx->pBatchs = pJob->pBatchs;
  }

  CTG_ERR_RET(ctgReadTbIndexFromCache(pCtg, pCtx->pName, &pRes));
  if (pRes) {
    pTask->res = pRes;

    CTG_ERR_RET(ctgHandleTaskEnd(pTask, 0));
    return TSDB_CODE_SUCCESS;
  }

  CTG_ERR_RET(ctgGetTbIndexFromMnode(pCtg, pConn, pCtx->pName, NULL, pTask));
  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetTbCfgTask(SCtgTask* pTask) {
  int32_t           code = 0;
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgTbCfgCtx*     pCtx = (SCtgTbCfgCtx*)pTask->taskCtx;
  SArray*           pRes = NULL;
  SCtgJob*          pJob = pTask->pJob;
  char              dbFName[TSDB_DB_FNAME_LEN];
  (void)tNameGetFullDbName(pCtx->pName, dbFName);

  SCtgMsgCtx* pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, -1);
  if (NULL == pMsgCtx) {
    ctgError("fail to get the %dth pMsgCtx", -1);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == pMsgCtx->pBatchs) {
    pMsgCtx->pBatchs = pJob->pBatchs;
  }

  CTG_CACHE_NHIT_INC(CTG_CI_TBL_CFG, 1);

  if (pCtx->tbType <= 0) {
    CTG_ERR_JRET(ctgReadTbTypeFromCache(pCtg, dbFName, pCtx->pName->tname, &pCtx->tbType));
    if (pCtx->tbType <= 0) {
      SCtgTbMetaParam param;
      param.pName = pCtx->pName;
      param.flag = 0;
      CTG_ERR_JRET(ctgLaunchSubTask(&pTask, CTG_TASK_GET_TB_META, ctgGetTbCfgCb, &param));
      return TSDB_CODE_SUCCESS;
    }
  }

  if (TSDB_SUPER_TABLE == pCtx->tbType || TSDB_SYSTEM_TABLE == pCtx->tbType) {
    CTG_ERR_JRET(ctgGetTableCfgFromMnode(pCtg, pConn, pCtx->pName, NULL, pTask));
  } else {
    if (NULL == pCtx->pVgInfo) {
      CTG_ERR_JRET(ctgGetTbHashVgroupFromCache(pCtg, pCtx->pName, &pCtx->pVgInfo));
      if (NULL == pCtx->pVgInfo) {
        CTG_ERR_JRET(ctgLaunchSubTask(&pTask, CTG_TASK_GET_DB_VGROUP, ctgGetTbCfgCb, dbFName));
        return TSDB_CODE_SUCCESS;
      }
    }

    CTG_ERR_JRET(ctgGetTableCfgFromVnode(pCtg, pConn, pCtx->pName, pCtx->pVgInfo, NULL, pTask));
  }

  return TSDB_CODE_SUCCESS;

_return:

  if (CTG_TASK_LAUNCHED == pTask->status) {
    int32_t newCode = ctgHandleTaskEnd(pTask, code);
    if (newCode && TSDB_CODE_SUCCESS == code) {
      code = newCode;
    }
  }

  CTG_RET(code);
}

int32_t ctgLaunchGetTbTagTask(SCtgTask* pTask) {
  int32_t           code = 0;
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgTbTagCtx*     pCtx = (SCtgTbTagCtx*)pTask->taskCtx;
  SArray*           pRes = NULL;
  SCtgJob*          pJob = pTask->pJob;
  char              dbFName[TSDB_DB_FNAME_LEN];
  (void)tNameGetFullDbName(pCtx->pName, dbFName);

  SCtgMsgCtx* pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, -1);
  if (NULL == pMsgCtx) {
    ctgError("fail to get the %dth pMsgCtx", -1);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == pMsgCtx->pBatchs) {
    pMsgCtx->pBatchs = pJob->pBatchs;
  }

  if (NULL == pCtx->pVgInfo) {
    CTG_ERR_JRET(ctgGetTbHashVgroupFromCache(pCtg, pCtx->pName, &pCtx->pVgInfo));
    if (NULL == pCtx->pVgInfo) {
      CTG_ERR_JRET(ctgLaunchSubTask(&pTask, CTG_TASK_GET_DB_VGROUP, ctgGetTbTagCb, dbFName));
      return TSDB_CODE_SUCCESS;
    }
  }

  CTG_CACHE_NHIT_INC(CTG_CI_TBL_TAG, 1);

  CTG_ERR_JRET(ctgGetTableCfgFromVnode(pCtg, pConn, pCtx->pName, pCtx->pVgInfo, NULL, pTask));

  return TSDB_CODE_SUCCESS;

_return:

  if (CTG_TASK_LAUNCHED == pTask->status) {
    int32_t newCode = ctgHandleTaskEnd(pTask, code);
    if (newCode && TSDB_CODE_SUCCESS == code) {
      code = newCode;
    }
  }

  CTG_RET(code);
}

int32_t ctgLaunchGetQnodeTask(SCtgTask* pTask) {
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgJob*          pJob = pTask->pJob;
  SCtgMsgCtx*       pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, -1);
  if (NULL == pMsgCtx) {
    ctgError("fail to get the %dth pMsgCtx", -1);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == pMsgCtx->pBatchs) {
    pMsgCtx->pBatchs = pJob->pBatchs;
  }

  CTG_CACHE_NHIT_INC(CTG_CI_QNODE, 1);
  CTG_ERR_RET(ctgGetQnodeListFromMnode(pCtg, pConn, NULL, pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetDnodeTask(SCtgTask* pTask) {
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgJob*          pJob = pTask->pJob;
  SCtgMsgCtx*       pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, -1);
  if (NULL == pMsgCtx) {
    ctgError("fail to get the %dth pMsgCtx", -1);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == pMsgCtx->pBatchs) {
    pMsgCtx->pBatchs = pJob->pBatchs;
  }

  CTG_CACHE_NHIT_INC(CTG_CI_DNODE, 1);
  CTG_ERR_RET(ctgGetDnodeListFromMnode(pCtg, pConn, NULL, pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetDbCfgTask(SCtgTask* pTask) {
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgDbCfgCtx*     pCtx = (SCtgDbCfgCtx*)pTask->taskCtx;
  SCtgJob*          pJob = pTask->pJob;
  SCtgMsgCtx*       pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, -1);
  if (NULL == pMsgCtx) {
    ctgError("fail to get the %dth pMsgCtx", -1);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == pMsgCtx->pBatchs) {
    pMsgCtx->pBatchs = pJob->pBatchs;
  }

  SDbCfgInfo cfgInfo;
  CTG_ERR_RET(ctgReadDBCfgFromCache(pCtg, pCtx->dbFName, &cfgInfo));

  if (cfgInfo.cfgVersion < 0) {
    CTG_ERR_RET(ctgGetDBCfgFromMnode(pCtg, pConn, pCtx->dbFName, NULL, pTask));
  } else {
    pTask->res = taosMemoryCalloc(1, sizeof(SDbCfgInfo));
    if (NULL == pTask->res) {
      CTG_ERR_RET(terrno);
    }

    TAOS_MEMCPY(pTask->res, &cfgInfo, sizeof(cfgInfo));
    CTG_ERR_RET(ctgHandleTaskEnd(pTask, 0));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetDbInfoTask(SCtgTask* pTask) {
  int32_t        code = 0;
  SCatalog*      pCtg = pTask->pJob->pCtg;
  SCtgDBCache*   dbCache = NULL;
  SCtgDbInfoCtx* pCtx = (SCtgDbInfoCtx*)pTask->taskCtx;
  SCtgJob*       pJob = pTask->pJob;
  SCtgMsgCtx*    pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, -1);
  if (NULL == pMsgCtx) {
    ctgError("fail to get the %dth pMsgCtx", -1);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == pMsgCtx->pBatchs) {
    pMsgCtx->pBatchs = pJob->pBatchs;
  }

  pTask->res = taosMemoryCalloc(1, sizeof(SDbInfo));
  if (NULL == pTask->res) {
    CTG_ERR_RET(terrno);
  }

  SDbInfo* pInfo = (SDbInfo*)pTask->res;
  CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, pCtx->dbFName, &dbCache));
  if (NULL != dbCache) {
    pInfo->vgVer = dbCache->vgCache.vgInfo->vgVersion;
    pInfo->dbId = dbCache->dbId;
    pInfo->tbNum = dbCache->vgCache.vgInfo->numOfTable;
    pInfo->stateTs = dbCache->vgCache.vgInfo->stateTs;

    CTG_CACHE_HIT_INC(CTG_CI_DB_INFO, 1);

    ctgReleaseVgInfoToCache(pCtg, dbCache);
    dbCache = NULL;
  } else {
    pInfo->vgVer = CTG_DEFAULT_INVALID_VERSION;

    CTG_CACHE_NHIT_INC(CTG_CI_DB_INFO, 1);
  }

  CTG_ERR_JRET(ctgHandleTaskEnd(pTask, 0));

_return:

  CTG_RET(code);
}

int32_t ctgLaunchGetIndexTask(SCtgTask* pTask) {
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgIndexCtx*     pCtx = (SCtgIndexCtx*)pTask->taskCtx;
  SCtgJob*          pJob = pTask->pJob;
  SCtgMsgCtx*       pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, -1);
  if (NULL == pMsgCtx) {
    ctgError("fail to get the %dth pMsgCtx", -1);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == pMsgCtx->pBatchs) {
    pMsgCtx->pBatchs = pJob->pBatchs;
  }

  CTG_CACHE_NHIT_INC(CTG_CI_INDEX_INFO, 1);

  CTG_ERR_RET(ctgGetIndexInfoFromMnode(pCtg, pConn, pCtx->indexFName, NULL, pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetUdfTask(SCtgTask* pTask) {
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgUdfCtx*       pCtx = (SCtgUdfCtx*)pTask->taskCtx;
  SCtgJob*          pJob = pTask->pJob;
  SCtgMsgCtx*       pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, -1);
  if (NULL == pMsgCtx) {
    ctgError("fail to get the %dth pMsgCtx", -1);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == pMsgCtx->pBatchs) {
    pMsgCtx->pBatchs = pJob->pBatchs;
  }

  CTG_CACHE_NHIT_INC(CTG_CI_UDF, 1);

  CTG_ERR_RET(ctgGetUdfInfoFromMnode(pCtg, pConn, pCtx->udfName, NULL, pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetUserTask(SCtgTask* pTask) {
  int32_t           code = 0;
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgUserCtx*      pCtx = (SCtgUserCtx*)pTask->taskCtx;
  bool              inCache = false;
  SCtgAuthRsp       rsp = {0};
  SCtgJob*          pJob = pTask->pJob;
  bool              tbNotExists = false;
  SCtgMsgCtx*       pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, -1);
  if (NULL == pMsgCtx) {
    ctgError("fail to get the %dth pMsgCtx", -1);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == pMsgCtx->pBatchs) {
    pMsgCtx->pBatchs = pJob->pBatchs;
  }

  rsp.pRawRes = taosMemoryCalloc(1, sizeof(SUserAuthRes));
  if (NULL == rsp.pRawRes) {
    CTG_ERR_RET(terrno);
  }

  if (TSDB_CODE_SUCCESS != pCtx->subTaskCode) {
    if (CTG_TABLE_NOT_EXIST(pCtx->subTaskCode)) {
      tbNotExists = true;
      pCtx->subTaskCode = 0;
    } else {
      CTG_ERR_RET(pCtx->subTaskCode);
    }
  }

  CTG_ERR_RET(ctgChkAuthFromCache(pCtg, &pCtx->user, tbNotExists, &inCache, &rsp));
  if (inCache) {
    pTask->res = rsp.pRawRes;

    ctgTaskDebug("Final res got, pass:[%d,%d], pCond:[%p,%p]", rsp.pRawRes->pass[0], rsp.pRawRes->pass[1],
                 rsp.pRawRes->pCond[0], rsp.pRawRes->pCond[1]);

    CTG_ERR_RET(ctgHandleTaskEnd(pTask, 0));
    return TSDB_CODE_SUCCESS;
  }

  taosMemoryFreeClear(rsp.pRawRes);

  if (rsp.metaNotExists) {
    SCtgTbMetaParam param;
    param.pName = &pCtx->user.tbName;
    param.flag = CTG_FLAG_SYNC_OP;
    CTG_ERR_RET(ctgLaunchSubTask(&pTask, CTG_TASK_GET_TB_META, ctgGetUserCb, &param));
  } else {
    CTG_ERR_RET(ctgGetUserDbAuthFromMnode(pCtg, pConn, pCtx->user.user, NULL, pTask));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetSvrVerTask(SCtgTask* pTask) {
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgJob*          pJob = pTask->pJob;
  SCtgMsgCtx*       pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, -1);
  if (NULL == pMsgCtx) {
    ctgError("fail to get the %dth pMsgCtx", -1);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (NULL == pMsgCtx->pBatchs) {
    pMsgCtx->pBatchs = pJob->pBatchs;
  }

  CTG_CACHE_NHIT_INC(CTG_CI_SVR_VER, 1);

  CTG_ERR_RET(ctgGetSvrVerFromMnode(pCtg, pConn, NULL, pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetViewsTask(SCtgTask* pTask) {
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgViewsCtx*     pCtx = (SCtgViewsCtx*)pTask->taskCtx;
  SCtgJob*          pJob = pTask->pJob;
  bool              tbMetaDone = false;
  SName*            pName = NULL;

  ctgIsTaskDone(pJob, CTG_TASK_GET_TB_META_BATCH, &tbMetaDone);
  if (tbMetaDone && !pCtx->forceFetch) {
    CTG_ERR_RET(ctgBuildViewNullRes(pTask, pCtx));
    TSWAP(pTask->res, pCtx->pResList);

    CTG_ERR_RET(ctgHandleTaskEnd(pTask, 0));
    return TSDB_CODE_SUCCESS;
  }

  int32_t dbNum = taosArrayGetSize(pCtx->pNames);
  int32_t fetchIdx = 0;
  int32_t baseResIdx = 0;
  for (int32_t i = 0; i < dbNum; ++i) {
    STablesReq* pReq = taosArrayGet(pCtx->pNames, i);
    if (NULL == pReq) {
      ctgError("fail to get the %dth STablesReq, dbNum:%d", i, dbNum);
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    ctgDebug("start to check views in db %s, viewNum %ld", pReq->dbFName, taosArrayGetSize(pReq->pTables));
    CTG_ERR_RET(ctgGetViewsFromCache(pCtg, pConn, pCtx, i, &fetchIdx, baseResIdx, pReq->pTables));
    baseResIdx += taosArrayGetSize(pReq->pTables);
  }

  pCtx->fetchNum = taosArrayGetSize(pCtx->pFetchs);
  if (pCtx->fetchNum <= 0) {
    TSWAP(pTask->res, pCtx->pResList);

    CTG_ERR_RET(ctgHandleTaskEnd(pTask, 0));
    return TSDB_CODE_SUCCESS;
  }

  pTask->msgCtxs = taosArrayInit_s(sizeof(SCtgMsgCtx), pCtx->fetchNum);
  if (NULL == pTask->msgCtxs) {
    ctgError("taosArrayInit_s %d SCtgMsgCtx %d failed", pCtx->fetchNum, (int32_t)sizeof(SCtgMsgCtx));
    CTG_ERR_RET(terrno);
  }

  for (int32_t i = 0; i < pCtx->fetchNum; ++i) {
    SCtgFetch* pFetch = taosArrayGet(pCtx->pFetchs, i);
    if (NULL == pFetch) {
      ctgError("fail to get the %dth SCtgFetch, fetchNum:%d", i, pCtx->fetchNum);
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    CTG_ERR_RET(ctgGetFetchName(pCtx->pNames, pFetch, &pName));

    SCtgMsgCtx* pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, i);
    if (NULL == pMsgCtx) {
      ctgError("fail to get the %dth SCtgMsgCtx", i);
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    if (NULL == pMsgCtx->pBatchs) {
      pMsgCtx->pBatchs = pJob->pBatchs;
    }

    SCtgTaskReq tReq;
    tReq.pTask = pTask;
    tReq.msgIdx = pFetch->fetchIdx;
    CTG_ERR_RET(ctgGetViewInfoFromMnode(pCtg, pConn, pName, NULL, &tReq));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgAsyncRefreshTbTsma(SCtgTaskReq* pReq, const SCtgTSMAFetch* pFetch) {
  int32_t           code = 0;
  SCtgTask*         pTask = pReq->pTask;
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgTbTSMACtx*    pTaskCtx = pTask->taskCtx;

  SCtgDBCache* pDbCache = NULL;
  STablesReq*  pTbReq = taosArrayGet(pTaskCtx->pNames, pFetch->dbIdx);
  if (NULL == pTbReq) {
    ctgError("fail to get the %dth STablesReq, totalNum:%d", pFetch->dbIdx,
             (int32_t)taosArrayGetSize(pTaskCtx->pNames));
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  (void)ctgAcquireVgInfoFromCache(pCtg, pTbReq->dbFName, &pDbCache);  // ignore error
  if (pDbCache) {
    ctgReleaseVgInfoToCache(pCtg, pDbCache);
  } else {
    SBuildUseDBInput input = {0};
    tstrncpy(input.db, pTbReq->dbFName, tListLen(input.db));
    input.vgVersion = CTG_DEFAULT_INVALID_VERSION;

    CTG_ERR_JRET(ctgGetDBVgInfoFromMnode(pCtg, pConn, &input, NULL, pReq));
  }

_return:

  return code;
}

int32_t ctgLaunchGetTbTSMATask(SCtgTask* pTask) {
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SCtgTbTSMACtx*    pCtx = (SCtgTbTSMACtx*)pTask->taskCtx;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SArray*           pRes = NULL;
  SCtgJob*          pJob = pTask->pJob;

  int32_t dbNum = taosArrayGetSize(pCtx->pNames);
  int32_t fetchIdx = 0, baseResIdx = 0;

  for (int32_t idx = 0; idx < dbNum; ++idx) {
    STablesReq* pReq = taosArrayGet(pCtx->pNames, idx);
    if (NULL == pReq) {
      ctgError("fail to get the %dth STablesReq, dbNum:%d", idx, dbNum);
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    CTG_ERR_RET(ctgGetTbTSMAFromCache(pCtg, pCtx, idx, &fetchIdx, baseResIdx, pReq->pTables));
    baseResIdx += taosArrayGetSize(pReq->pTables);
  }

  pCtx->fetchNum = taosArrayGetSize(pCtx->pFetches);
  if (pCtx->fetchNum <= 0) {
    TSWAP(pTask->res, pCtx->pResList);
    CTG_ERR_RET(ctgHandleTaskEnd(pTask, 0));
    return TSDB_CODE_SUCCESS;
  }

  pTask->msgCtxs = taosArrayInit_s(sizeof(SCtgMsgCtx), pCtx->fetchNum);
  if (NULL == pTask->msgCtxs) {
    ctgError("taosArrayInit_s %d SCtgMsgCtx %d failed", pCtx->fetchNum, (int32_t)sizeof(SCtgMsgCtx));
    CTG_ERR_RET(terrno);
  }

  for (int32_t i = 0; i < pCtx->fetchNum; ++i) {
    SCtgTSMAFetch* pFetch = taosArrayGet(pCtx->pFetches, i);
    if (NULL == pFetch) {
      ctgError("fail to get the %dth SCtgTSMAFetch, fetchNum:%d", i, pCtx->fetchNum);
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    STablesReq* pReq = taosArrayGet(pCtx->pNames, pFetch->dbIdx);
    if (NULL == pReq) {
      ctgError("fail to get the %dth STablesReq, totalNum:%d", pFetch->dbIdx, (int32_t)taosArrayGetSize(pCtx->pNames));
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    SName* pName = taosArrayGet(pReq->pTables, pFetch->tbIdx);
    if (NULL == pName) {
      ctgError("fail to get the %dth SName, totalNum:%d", pFetch->tbIdx, (int32_t)taosArrayGetSize(pReq->pTables));
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    SCtgMsgCtx* pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, i);
    if (NULL == pMsgCtx) {
      ctgError("fail to get the %dth pMsgCtx", i);
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    if (!pMsgCtx->pBatchs) {
      pMsgCtx->pBatchs = pJob->pBatchs;
    }

    SCtgTaskReq tReq;
    tReq.pTask = pTask;
    tReq.msgIdx = pFetch->fetchIdx;

    switch (pFetch->fetchType) {
      case FETCH_TSMA_SOURCE_TB_META: {
        CTG_ERR_RET(ctgAsyncRefreshTbMeta(&tReq, pFetch->flag, pName, &pFetch->vgId));
        break;
      }
      case FETCH_TB_TSMA: {
        CTG_ERR_RET(
            ctgGetTbTSMAFromMnode(pCtg, pConn, &pFetch->tsmaSourceTbName, NULL, &tReq, TDMT_MND_GET_TABLE_TSMA));
        break;
      }
      default:
        ctgError("invalid fetchType:%d in getting tb tsma task", pFetch->fetchType);
        CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
        break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchGetTSMATask(SCtgTask* pTask) {
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SCtgTbTSMACtx*    pCtx = (SCtgTbTSMACtx*)pTask->taskCtx;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SArray*           pRes = NULL;
  SCtgJob*          pJob = pTask->pJob;

  // currently, only support fetching one tsma
  STablesReq* pReq = taosArrayGet(pCtx->pNames, 0);
  if (NULL == pReq) {
    ctgError("fail to get the 0th STablesReq, totalNum:%d", (int32_t)taosArrayGetSize(pCtx->pNames));
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  SName* pTsmaName = taosArrayGet(pReq->pTables, 0);
  if (NULL == pReq) {
    ctgError("fail to get the 0th SName, totalNum:%d", (int32_t)taosArrayGetSize(pReq->pTables));
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  CTG_ERR_RET(ctgGetTSMAFromCache(pCtg, pCtx, pTsmaName));

  if (pCtx->pResList->size == 0) {
    SCtgMsgCtx* pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, 0);
    if (NULL == pMsgCtx) {
      ctgError("fail to get the 0th SCtgMsgCtx, taskType:%d", pTask->type);
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    if (!pMsgCtx->pBatchs) {
      pMsgCtx->pBatchs = pJob->pBatchs;
    }

    SCtgTaskReq tReq = {.pTask = pTask, .msgIdx = 0};
    if (NULL == taosArrayPush(pCtx->pResList, &(SMetaRes){0})) {
      ctgError("taosArrayPush SMetaRes failed, code:%x", terrno);
      CTG_ERR_RET(terrno);
    }

    CTG_ERR_RET(ctgGetTbTSMAFromMnode(pCtg, pConn, pTsmaName, NULL, &tReq, TDMT_MND_GET_TSMA));
  } else {
    SMetaRes* pRes = taosArrayGet(pCtx->pResList, 0);
    if (NULL == pRes) {
      ctgError("fail to get the 0th SMetaRes, totalNum:%d", (int32_t)taosArrayGetSize(pCtx->pResList));
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    STableTSMAInfoRsp* pRsp = (STableTSMAInfoRsp*)pRes->pRes;

    const STSMACache* pTsma = taosArrayGetP(pRsp->pTsmas, 0);
    if (NULL == pTsma) {
      ctgError("fail to get the 0th STSMACache, totalNum:%d", (int32_t)taosArrayGetSize(pRsp->pTsmas));
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    TSWAP(pTask->res, pCtx->pResList);
    // get tsma target stable meta if not existed in cache
    int32_t exists = false;
    CTG_ERR_RET(ctgTbMetaExistInCache(pCtg, pTsma->targetDbFName, pTsma->targetTb, &exists));
    if (!exists) {
      SCtgTaskReq tReq = {.pTask = pTask, .msgIdx = 0};
      SCtgMsgCtx* pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, 0);
      if (!pMsgCtx->pBatchs) pMsgCtx->pBatchs = pJob->pBatchs;
      CTG_RET(ctgGetTbMetaFromMnodeImpl(pCtg, pConn, pTsma->targetDbFName, pTsma->targetTb, NULL, &tReq));
    } else {
      CTG_ERR_RET(ctgHandleTaskEnd(pTask, 0));
    }

    return TSDB_CODE_SUCCESS;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t ctgLaunchGetTbNamesTask(SCtgTask* pTask) {
  SCatalog*         pCtg = pTask->pJob->pCtg;
  SRequestConnInfo* pConn = &pTask->pJob->conn;
  SCtgTbNamesCtx*   pCtx = (SCtgTbNamesCtx*)pTask->taskCtx;
  SCtgJob*          pJob = pTask->pJob;
  SName*            pName = NULL;

  int32_t dbNum = taosArrayGetSize(pCtx->pNames);
  int32_t fetchIdx = 0;
  int32_t baseResIdx = 0;
  for (int32_t i = 0; i < dbNum; ++i) {
    STablesReq* pReq = TARRAY_GET_ELEM(pCtx->pNames, i);
    if (NULL == pReq) {
      ctgError("fail to get the %dth STablesReq, num:%d", i, dbNum);
      CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
    }

    ctgDebug("start to check tbname metas in db %s, tbNum %ld", pReq->dbFName, taosArrayGetSize(pReq->pTables));
    CTG_ERR_RET(ctgGetTbNamesFromCache(pCtg, pConn, pCtx, i, &fetchIdx, baseResIdx, pReq->pTables));
    baseResIdx += taosArrayGetSize(pReq->pTables);
  }

  pCtx->fetchNum = taosArrayGetSize(pCtx->pFetchs);
  if (pCtx->fetchNum <= 0) {
    TSWAP(pTask->res, pCtx->pResList);

    CTG_ERR_RET(ctgHandleTaskEnd(pTask, 0));
    return TSDB_CODE_SUCCESS;
  }

  pTask->msgCtxs = taosArrayInit_s(sizeof(SCtgMsgCtx), pCtx->fetchNum);
  if (NULL == pTask->msgCtxs) {
    ctgError("taosArrayInit_s %d SCtgMsgCtx %d failed", pCtx->fetchNum, (int32_t)sizeof(SCtgMsgCtx));
    CTG_ERR_RET(terrno);
  }

  for (int32_t i = 0; i < pCtx->fetchNum; ++i) {
    SCtgFetch* pFetch = taosArrayGet(pCtx->pFetchs, i);
    if (NULL == pFetch) {
      ctgError("fail to get the %dth fetch in pCtx->pFetchs, fetchNum:%d", i, pCtx->fetchNum);
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    CTG_ERR_RET(ctgGetFetchName(pCtx->pNames, pFetch, &pName));

    SCtgMsgCtx* pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, i);
    if (NULL == pMsgCtx) {
      ctgError("fail to get the %dth pMsgCtx", i);
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    if (NULL == pMsgCtx->pBatchs) {
      pMsgCtx->pBatchs = pJob->pBatchs;
    }

    SCtgTaskReq tReq;
    tReq.pTask = pTask;
    tReq.msgIdx = pFetch->fetchIdx;
    CTG_ERR_RET(ctgAsyncRefreshTbMeta(&tReq, pFetch->flag, pName, &pFetch->vgId));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpTbTSMARes(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;
  pJob->jobRes.pTableTsmas = pTask->res;
  return TSDB_CODE_SUCCESS;
}

int32_t ctgDumpTSMARes(SCtgTask* pTask) {
  if (pTask->subTask) {
    return TSDB_CODE_SUCCESS;
  }

  SCtgJob* pJob = pTask->pJob;
  pJob->jobRes.pTsmas = pTask->res;
  return TSDB_CODE_SUCCESS;
}

int32_t ctgRelaunchGetTbMetaTask(SCtgTask* pTask) {
  ctgResetTbMetaTask(pTask);

  CTG_ERR_RET(ctgLaunchGetTbMetaTask(pTask));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTbCfgCb(SCtgTask* pTask) {
  int32_t code = 0;

  CTG_ERR_JRET(pTask->subRes.code);

  SCtgTbCfgCtx* pCtx = (SCtgTbCfgCtx*)pTask->taskCtx;
  if (CTG_TASK_GET_TB_META == pTask->subRes.type) {
    pCtx->tbType = ((STableMeta*)pTask->subRes.res)->tableType;
  } else if (CTG_TASK_GET_DB_VGROUP == pTask->subRes.type) {
    SDBVgInfo* pDb = (SDBVgInfo*)pTask->subRes.res;

    pCtx->pVgInfo = taosMemoryCalloc(1, sizeof(SVgroupInfo));
    if (NULL == pCtx->pVgInfo) {
      CTG_ERR_JRET(terrno);
    }

    CTG_ERR_JRET(
        ctgGetVgInfoFromHashValue(pTask->pJob->pCtg, &pTask->pJob->conn.mgmtEps, pDb, pCtx->pName, pCtx->pVgInfo));
  }

  CTG_RET(ctgLaunchGetTbCfgTask(pTask));

_return:

  CTG_RET(ctgHandleTaskEnd(pTask, pTask->subRes.code));
}

int32_t ctgGetTbTagCb(SCtgTask* pTask) {
  int32_t code = 0;

  CTG_ERR_JRET(pTask->subRes.code);

  SCtgTbTagCtx* pCtx = (SCtgTbTagCtx*)pTask->taskCtx;
  SDBVgInfo*    pDb = (SDBVgInfo*)pTask->subRes.res;

  if (NULL == pCtx->pVgInfo) {
    pCtx->pVgInfo = taosMemoryCalloc(1, sizeof(SVgroupInfo));
    if (NULL == pCtx->pVgInfo) {
      CTG_ERR_JRET(terrno);
    }

    CTG_ERR_JRET(
        ctgGetVgInfoFromHashValue(pTask->pJob->pCtg, &pTask->pJob->conn.mgmtEps, pDb, pCtx->pName, pCtx->pVgInfo));
  }

  CTG_RET(ctgLaunchGetTbTagTask(pTask));

_return:

  CTG_RET(ctgHandleTaskEnd(pTask, pTask->subRes.code));
}

int32_t ctgGetUserCb(SCtgTask* pTask) {
  int32_t code = 0;

  SCtgUserCtx* pCtx = (SCtgUserCtx*)pTask->taskCtx;
  pCtx->subTaskCode = pTask->subRes.code;

  CTG_RET(ctgLaunchGetUserTask(pTask));

_return:

  CTG_RET(ctgHandleTaskEnd(pTask, pTask->subRes.code));
}

int32_t ctgCompDbVgTasks(SCtgTask* pTask, void* param, bool* equal) {
  SCtgDbVgCtx* ctx = pTask->taskCtx;

  *equal = (0 == strcmp(ctx->dbFName, param));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgCompTbMetaTasks(SCtgTask* pTask, void* param, bool* equal) {
  SCtgTbMetaCtx*   ctx = pTask->taskCtx;
  SCtgTbMetaParam* pParam = (SCtgTbMetaParam*)param;

  *equal = tNameTbNameEqual(ctx->pName, (SName*)pParam->pName);
  if (*equal) {
    ctx->flag |= pParam->flag;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgCloneTbMeta(SCtgTask* pTask, void** pRes) {
  STableMeta* pMeta = (STableMeta*)pTask->res;

  CTG_RET(cloneTableMeta(pMeta, (STableMeta**)pRes));
}

int32_t ctgCloneDbVg(SCtgTask* pTask, void** pRes) {
  SUseDbOutput* pOut = (SUseDbOutput*)pTask->msgCtx.out;

  CTG_RET(cloneDbVgInfo(pOut->dbVgroup, (SDBVgInfo**)pRes));
}

SCtgAsyncFps gCtgAsyncFps[] = {
    {ctgInitGetQnodeTask, ctgLaunchGetQnodeTask, ctgHandleGetQnodeRsp, ctgDumpQnodeRes, NULL, NULL},
    {ctgInitGetDnodeTask, ctgLaunchGetDnodeTask, ctgHandleGetDnodeRsp, ctgDumpDnodeRes, NULL, NULL},
    {ctgInitGetDbVgTask, ctgLaunchGetDbVgTask, ctgHandleGetDbVgRsp, ctgDumpDbVgRes, ctgCompDbVgTasks, ctgCloneDbVg},
    {ctgInitGetDbCfgTask, ctgLaunchGetDbCfgTask, ctgHandleGetDbCfgRsp, ctgDumpDbCfgRes, NULL, NULL},
    {ctgInitGetDbInfoTask, ctgLaunchGetDbInfoTask, ctgHandleGetDbInfoRsp, ctgDumpDbInfoRes, NULL, NULL},
    {ctgInitGetTbMetaTask, ctgLaunchGetTbMetaTask, ctgHandleGetTbMetaRsp, ctgDumpTbMetaRes, ctgCompTbMetaTasks,
     ctgCloneTbMeta},
    {ctgInitGetTbHashTask, ctgLaunchGetTbHashTask, ctgHandleGetTbHashRsp, ctgDumpTbHashRes, NULL, NULL},
    {ctgInitGetTbIndexTask, ctgLaunchGetTbIndexTask, ctgHandleGetTbIndexRsp, ctgDumpTbIndexRes, NULL, NULL},
    {ctgInitGetTbCfgTask, ctgLaunchGetTbCfgTask, ctgHandleGetTbCfgRsp, ctgDumpTbCfgRes, NULL, NULL},
    {ctgInitGetIndexTask, ctgLaunchGetIndexTask, ctgHandleGetIndexRsp, ctgDumpIndexRes, NULL, NULL},
    {ctgInitGetUdfTask, ctgLaunchGetUdfTask, ctgHandleGetUdfRsp, ctgDumpUdfRes, NULL, NULL},
    {ctgInitGetUserTask, ctgLaunchGetUserTask, ctgHandleGetUserRsp, ctgDumpUserRes, NULL, NULL},
    {ctgInitGetSvrVerTask, ctgLaunchGetSvrVerTask, ctgHandleGetSvrVerRsp, ctgDumpSvrVer, NULL, NULL},
    {ctgInitGetTbMetasTask, ctgLaunchGetTbMetasTask, ctgHandleGetTbMetasRsp, ctgDumpTbMetasRes, NULL, NULL},
    {ctgInitGetTbHashsTask, ctgLaunchGetTbHashsTask, ctgHandleGetTbHashsRsp, ctgDumpTbHashsRes, NULL, NULL},
    {ctgInitGetTbTagTask, ctgLaunchGetTbTagTask, ctgHandleGetTbTagRsp, ctgDumpTbTagRes, NULL, NULL},
    {ctgInitGetViewsTask, ctgLaunchGetViewsTask, ctgHandleGetViewsRsp, ctgDumpViewsRes, NULL, NULL},
    {ctgInitGetTbTSMATask, ctgLaunchGetTbTSMATask, ctgHandleGetTbTSMARsp, ctgDumpTbTSMARes, NULL, NULL},
    {ctgInitGetTSMATask, ctgLaunchGetTSMATask, ctgHandleGetTSMARsp, ctgDumpTSMARes, NULL, NULL},
    {ctgInitGetTbNamesTask, ctgLaunchGetTbNamesTask, ctgHandleGetTbNamesRsp, ctgDumpTbNamesRes, NULL, NULL},
};

int32_t ctgMakeAsyncRes(SCtgJob* pJob) {
  int32_t code = 0;
  int32_t taskNum = taosArrayGetSize(pJob->pTasks);

  for (int32_t i = 0; i < taskNum; ++i) {
    SCtgTask* pTask = taosArrayGet(pJob->pTasks, i);
    CTG_ERR_RET((*gCtgAsyncFps[pTask->type].dumpResFp)(pTask));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgSearchExistingTask(SCtgJob* pJob, CTG_TASK_TYPE type, void* param, int32_t* taskId) {
  bool      equal = false;
  SCtgTask* pTask = NULL;
  int32_t   code = 0;

  CTG_LOCK(CTG_READ, &pJob->taskLock);

  int32_t taskNum = taosArrayGetSize(pJob->pTasks);
  for (int32_t i = 0; i < taskNum; ++i) {
    pTask = taosArrayGet(pJob->pTasks, i);
    if (NULL == pTask) {
      qError("fail to get the %dth task, taskNum:%d", i, taskNum);
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    if (type != pTask->type) {
      continue;
    }

    CTG_ERR_JRET((*gCtgAsyncFps[type].compFp)(pTask, param, &equal));
    if (equal) {
      break;
    }
  }

_return:

  CTG_UNLOCK(CTG_READ, &pJob->taskLock);
  if (equal) {
    *taskId = pTask->taskId;
  }

  CTG_RET(code);
}

int32_t ctgSetSubTaskCb(SCtgTask* pSub, SCtgTask* pTask) {
  int32_t code = 0;

  CTG_LOCK(CTG_WRITE, &pSub->lock);
  if (CTG_TASK_DONE == pSub->status) {
    pTask->subRes.code = pSub->code;
    CTG_ERR_JRET((*gCtgAsyncFps[pTask->type].cloneFp)(pSub, &pTask->subRes.res));
    SCtgMsgCtx* pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, -1);
    if (NULL == pMsgCtx) {
      qError("fail to get the -1th SCtgMsgCtx");
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    SCtgMsgCtx* pSubMsgCtx = CTG_GET_TASK_MSGCTX(pSub, -1);
    if (NULL == pSubMsgCtx) {
      qError("fail to get the -1th sub SCtgMsgCtx");
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    pMsgCtx->pBatchs = pSubMsgCtx->pBatchs;

    CTG_ERR_JRET(pTask->subRes.fp(pTask));
  } else {
    if (NULL == pSub->pParents) {
      pSub->pParents = taosArrayInit(4, POINTER_BYTES);
      if (NULL == pSub->pParents) {
        CTG_ERR_JRET(terrno);
      }
    }

    if (NULL == taosArrayPush(pSub->pParents, &pTask)) {
      CTG_ERR_JRET(terrno);
    }
  }

_return:

  CTG_UNLOCK(CTG_WRITE, &pSub->lock);

  CTG_RET(code);
}

SCtgTask* ctgGetTask(SCtgJob* pJob, int32_t taskId) {
  int32_t taskNum = taosArrayGetSize(pJob->pTasks);

  for (int32_t i = 0; i < taskNum; ++i) {
    SCtgTask* pTask = taosArrayGet(pJob->pTasks, i);
    if (pTask->taskId == taskId) {
      return pTask;
    }
  }

  return NULL;
}

int32_t ctgLaunchSubTask(SCtgTask** ppTask, CTG_TASK_TYPE type, ctgSubTaskCbFp fp, void* param) {
  SCtgJob* pJob = (*ppTask)->pJob;
  int32_t  subTaskId = -1;
  bool     newTask = false;
  int32_t  taskId = (*ppTask)->taskId;

  ctgClearSubTaskRes(&(*ppTask)->subRes);
  (*ppTask)->subRes.type = type;
  (*ppTask)->subRes.fp = fp;

  CTG_ERR_RET(ctgSearchExistingTask(pJob, type, param, &subTaskId));
  if (subTaskId < 0) {
    CTG_ERR_RET(ctgInitTask(pJob, type, param, &subTaskId));
    newTask = true;
    *ppTask = ctgGetTask(pJob, taskId);
  }

  SCtgTask* pSub = taosArrayGet(pJob->pTasks, subTaskId);
  if (NULL == pSub) {
    qError("fail to get the %dth sub SCtgTask, taskNum:%d", subTaskId, (int32_t)taosArrayGetSize(pJob->pTasks));
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (newTask) {
    pSub->subTask = true;
  }

  CTG_ERR_RET(ctgSetSubTaskCb(pSub, *ppTask));

  if (newTask) {
    SCtgMsgCtx* pMsgCtx = CTG_GET_TASK_MSGCTX(*ppTask, -1);
    if (NULL == pMsgCtx) {
      qError("fail to get the -1th SCtgMsgCtx");
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    SCtgMsgCtx* pSubMsgCtx = CTG_GET_TASK_MSGCTX(pSub, -1);
    if (NULL == pSubMsgCtx) {
      qError("fail to get the -1th sub SCtgMsgCtx");
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    pSubMsgCtx->pBatchs = pMsgCtx->pBatchs;

    CTG_ERR_RET((*gCtgAsyncFps[pSub->type].launchFp)(pSub));
    pSub->status = CTG_TASK_LAUNCHED;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchJob(SCtgJob* pJob) {
  int32_t taskNum = taosArrayGetSize(pJob->pTasks);

  for (int32_t i = 0; i < taskNum; ++i) {
    SCtgTask* pTask = taosArrayGet(pJob->pTasks, i);
    if (NULL == pTask) {
      qError("fail to get the %dth task, taskNum:%d", i, taskNum);
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    qDebug("QID:0x%" PRIx64 " ctg launch [%dth] task", pJob->queryId, pTask->taskId);
    CTG_ERR_RET((*gCtgAsyncFps[pTask->type].launchFp)(pTask));

    pTask = taosArrayGet(pJob->pTasks, i);
    if (NULL == pTask) {
      qError("fail to get the %dth task, taskNum:%d", i, taskNum);
      CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    (void)atomic_val_compare_exchange_32((int32_t*)&pTask->status, 0, CTG_TASK_LAUNCHED);
  }

  if (taskNum <= 0) {
    qDebug("QID:0x%" PRIx64 " ctg call user callback with rsp %s", pJob->queryId, tstrerror(pJob->jobResCode));

    CTG_ERR_RET(taosAsyncExec(ctgCallUserCb, pJob, NULL));
#if CTG_BATCH_FETCH
  } else {
    CTG_ERR_RET(ctgLaunchBatchs(pJob->pCtg, pJob, pJob->pBatchs));
#endif
  }

  return TSDB_CODE_SUCCESS;
}
