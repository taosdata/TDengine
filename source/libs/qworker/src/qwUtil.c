#include "dataSinkMgt.h"
#include "executor.h"
#include "planner.h"
#include "query.h"
#include "qwInt.h"
#include "qwMsg.h"
#include "qworker.h"
#include "tcommon.h"
#include "tmsg.h"
#include "tname.h"

char *qwPhaseStr(int32_t phase) {
  switch (phase) {
    case QW_PHASE_PRE_QUERY:
      return "PRE_QUERY";
    case QW_PHASE_POST_QUERY:
      return "POST_QUERY";
    case QW_PHASE_PRE_FETCH:
      return "PRE_FETCH";
    case QW_PHASE_POST_FETCH:
      return "POST_FETCH";
    case QW_PHASE_PRE_CQUERY:
      return "PRE_CQUERY";
    case QW_PHASE_POST_CQUERY:
      return "POST_CQUERY";
    default:
      break;
  }

  return "UNKNOWN";
}

char *qwBufStatusStr(int32_t bufStatus) {
  switch (bufStatus) {
    case DS_BUF_LOW:
      return "LOW";
    case DS_BUF_FULL:
      return "FULL";
    case DS_BUF_EMPTY:
      return "EMPTY";
    default:
      break;
  }

  return "UNKNOWN";
}

int32_t qwSetTaskStatus(QW_FPARAMS_DEF, SQWTaskStatus *task, int8_t status, bool dynamicTask) {
  int32_t code = 0;
  int8_t  origStatus = 0;
  bool    ignore = false;

  while (true) {
    origStatus = atomic_load_8(&task->status);

    QW_ERR_RET(qwDbgValidateStatus(QW_FPARAMS(), origStatus, status, &ignore, dynamicTask));
    if (ignore) {
      break;
    }

    if (origStatus != atomic_val_compare_exchange_8(&task->status, origStatus, status)) {
      continue;
    }

    QW_TASK_DLOG("task status updated from %s to %s", jobTaskStatusStr(origStatus), jobTaskStatusStr(status));

    break;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwAddSchedulerImpl(SQWorker *mgmt, uint64_t clientId, int32_t rwType) {
  SQWSchStatus newSch = {0};
  newSch.tasksHash =
      taosHashInit(mgmt->cfg.maxSchTaskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  newSch.hbBrokenTs = taosGetTimestampMs();

  if (NULL == newSch.tasksHash) {
    QW_SCH_ELOG("taosHashInit %d failed", mgmt->cfg.maxSchTaskNum);
    QW_ERR_RET(terrno);
  }

  QW_LOCK(QW_WRITE, &mgmt->schLock);
  int32_t code = taosHashPut(mgmt->schHash, &clientId, sizeof(clientId), &newSch, sizeof(newSch));
  if (0 != code) {
    if (!HASH_NODE_EXIST(code)) {
      QW_UNLOCK(QW_WRITE, &mgmt->schLock);

      QW_SCH_ELOG("taosHashPut new sch to scheduleHash failed, errno:%d", ERRNO);
      taosHashCleanup(newSch.tasksHash);
      QW_ERR_RET(code);
    }

    taosHashCleanup(newSch.tasksHash);
  }
  QW_UNLOCK(QW_WRITE, &mgmt->schLock);

  return TSDB_CODE_SUCCESS;
}

int32_t qwAcquireSchedulerImpl(SQWorker *mgmt, uint64_t clientId, int32_t rwType, SQWSchStatus **sch, int32_t nOpt) {
  while (true) {
    QW_LOCK(rwType, &mgmt->schLock);
    *sch = taosHashGet(mgmt->schHash, &clientId, sizeof(clientId));
    if (NULL == (*sch)) {
      QW_UNLOCK(rwType, &mgmt->schLock);

      if (QW_NOT_EXIST_ADD == nOpt) {
        QW_ERR_RET(qwAddSchedulerImpl(mgmt, clientId, rwType));

        nOpt = QW_NOT_EXIST_RET_ERR;

        continue;
      } else if (QW_NOT_EXIST_RET_ERR == nOpt) {
        QW_RET(TSDB_CODE_QRY_SCH_NOT_EXIST);
      } else {
        QW_SCH_ELOG("unknown notExistOpt:%d", nOpt);
        QW_ERR_RET(TSDB_CODE_APP_ERROR);
      }
    }

    break;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwAcquireAddScheduler(SQWorker *mgmt, uint64_t clientId, int32_t rwType, SQWSchStatus **sch) {
  return qwAcquireSchedulerImpl(mgmt, clientId, rwType, sch, QW_NOT_EXIST_ADD);
}

int32_t qwAcquireScheduler(SQWorker *mgmt, uint64_t clientId, int32_t rwType, SQWSchStatus **sch) {
  return qwAcquireSchedulerImpl(mgmt, clientId, rwType, sch, QW_NOT_EXIST_RET_ERR);
}

void qwReleaseScheduler(int32_t rwType, SQWorker *mgmt) { QW_UNLOCK(rwType, &mgmt->schLock); }

int32_t qwAcquireTaskStatus(QW_FPARAMS_DEF, int32_t rwType, SQWSchStatus *sch, SQWTaskStatus **task) {
  char id[sizeof(qId) + sizeof(cId) + sizeof(tId) + sizeof(eId)] = {0};
  QW_SET_QTID(id, qId, cId, tId, eId);

  QW_LOCK(rwType, &sch->tasksLock);
  *task = taosHashGet(sch->tasksHash, id, sizeof(id));
  if (NULL == (*task)) {
    QW_UNLOCK(rwType, &sch->tasksLock);
    QW_TASK_ELOG_E("task status not exists");
    QW_ERR_RET(TSDB_CODE_QRY_TASK_NOT_EXIST);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwAddTaskStatusImpl(QW_FPARAMS_DEF, SQWSchStatus *sch, int32_t rwType, int32_t status, SQWTaskStatus **task) {
  int32_t code = 0;

  char id[sizeof(qId) + sizeof(cId) + sizeof(tId) + sizeof(eId)] = {0};
  QW_SET_QTID(id, qId, cId, tId, eId);

  SQWTaskStatus ntask = {0};
  ntask.status = status;
  ntask.refId = rId;

  QW_LOCK(QW_WRITE, &sch->tasksLock);
  code = taosHashPut(sch->tasksHash, id, sizeof(id), &ntask, sizeof(ntask));
  if (0 != code) {
    QW_UNLOCK(QW_WRITE, &sch->tasksLock);
    if (HASH_NODE_EXIST(code)) {
      if (rwType && task) {
        QW_RET(qwAcquireTaskStatus(QW_FPARAMS(), rwType, sch, task));
      } else {
        QW_TASK_ELOG("task status already exist, newStatus:%s", jobTaskStatusStr(status));
        QW_ERR_RET(TSDB_CODE_QRY_TASK_ALREADY_EXIST);
      }
    } else {
      QW_TASK_ELOG("taosHashPut to tasksHash failed, error:%x - %s", code, tstrerror(code));
      QW_ERR_RET(code);
    }
  }
  QW_UNLOCK(QW_WRITE, &sch->tasksLock);

  QW_TASK_DLOG("task status added, newStatus:%s", jobTaskStatusStr(status));

  if (rwType && task) {
    QW_ERR_RET(qwAcquireTaskStatus(QW_FPARAMS(), rwType, sch, task));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwAddTaskStatus(QW_FPARAMS_DEF, int32_t status) {
  SQWSchStatus *tsch = NULL;
  int32_t       code = 0;
  QW_ERR_RET(qwAcquireAddScheduler(mgmt, cId, QW_READ, &tsch));

  QW_ERR_JRET(qwAddTaskStatusImpl(QW_FPARAMS(), tsch, 0, status, NULL));

_return:

  qwReleaseScheduler(QW_READ, mgmt);

  QW_RET(code);
}

int32_t qwAddAcquireTaskStatus(QW_FPARAMS_DEF, int32_t rwType, SQWSchStatus *sch, int32_t status,
                               SQWTaskStatus **task) {
  return qwAddTaskStatusImpl(QW_FPARAMS(), sch, rwType, status, task);
}

void qwReleaseTaskStatus(int32_t rwType, SQWSchStatus *sch) { QW_UNLOCK(rwType, &sch->tasksLock); }

int32_t qwAcquireTaskCtx(QW_FPARAMS_DEF, SQWTaskCtx **ctx) {
  char id[sizeof(qId) + sizeof(cId) + sizeof(tId) + sizeof(eId)] = {0};
  QW_SET_QTID(id, qId, cId, tId, eId);

  *ctx = taosHashAcquire(mgmt->ctxHash, id, sizeof(id));
  if (NULL == (*ctx)) {
    QW_TASK_DLOG_E("acquired task ctx not exist, may be dropped");
    QW_ERR_RET(QW_CTX_NOT_EXISTS_ERR_CODE(mgmt));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwGetTaskCtx(QW_FPARAMS_DEF, SQWTaskCtx **ctx) {
  char id[sizeof(qId) + sizeof(cId) + sizeof(tId) + sizeof(eId)] = {0};
  QW_SET_QTID(id, qId, cId, tId, eId);

  *ctx = taosHashGet(mgmt->ctxHash, id, sizeof(id));
  if (NULL == (*ctx)) {
    QW_TASK_DLOG_E("get task ctx not exist, may be dropped");
    QW_ERR_RET(QW_CTX_NOT_EXISTS_ERR_CODE(mgmt));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwAddTaskCtxImpl(QW_FPARAMS_DEF, bool acquire, SQWTaskCtx **ctx) {
  char id[sizeof(qId) + sizeof(cId) + sizeof(tId) + sizeof(eId)] = {0};
  QW_SET_QTID(id, qId, cId, tId, eId);

  SQWTaskCtx nctx = {0};

  int32_t code = taosHashPut(mgmt->ctxHash, id, sizeof(id), &nctx, sizeof(SQWTaskCtx));
  if (0 != code) {
    if (HASH_NODE_EXIST(code)) {
      if (acquire && ctx) {
        QW_RET(qwAcquireTaskCtx(QW_FPARAMS(), ctx));
      } else if (ctx) {
        QW_RET(qwGetTaskCtx(QW_FPARAMS(), ctx));
      } else {
        QW_TASK_ELOG_E("task ctx already exist");
        QW_ERR_RET(TSDB_CODE_QRY_TASK_ALREADY_EXIST);
      }
    } else {
      QW_TASK_ELOG("taosHashPut to ctxHash failed, error:%x", code);
      QW_ERR_RET(code);
    }
  }

  (void)atomic_add_fetch_64(&gQueryMgmt.stat.taskInitNum, 1);

  if (acquire && ctx) {
    QW_RET(qwAcquireTaskCtx(QW_FPARAMS(), ctx));
  } else if (ctx) {
    QW_RET(qwGetTaskCtx(QW_FPARAMS(), ctx));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwAddTaskCtx(QW_FPARAMS_DEF) { QW_RET(qwAddTaskCtxImpl(QW_FPARAMS(), false, NULL)); }

int32_t qwAddAcquireTaskCtx(QW_FPARAMS_DEF, SQWTaskCtx **ctx) { return qwAddTaskCtxImpl(QW_FPARAMS(), true, ctx); }

void qwReleaseTaskCtx(SQWorker *mgmt, void *ctx) { taosHashRelease(mgmt->ctxHash, ctx); }

void qwFreeTaskHandle(SQWTaskCtx *ctx) {
  // Note: free/kill may in RC
  qTaskInfo_t otaskHandle = atomic_load_ptr(&ctx->taskHandle);
  if (otaskHandle && otaskHandle == atomic_val_compare_exchange_ptr(&ctx->taskHandle, otaskHandle, NULL)) {
    taosEnableMemPoolUsage(ctx->memPoolSession);
    qDestroyTask(otaskHandle);
    taosDisableMemPoolUsage();

    (void)atomic_add_fetch_64(&gQueryMgmt.stat.taskExecDestroyNum, 1);

    qDebug("task handle destroyed");
  }
}

void qwFreeSinkHandle(SQWTaskCtx *ctx) {
  // Note: free/kill may in RC
  void *osinkHandle = atomic_load_ptr(&ctx->sinkHandle);
  if (osinkHandle && osinkHandle == atomic_val_compare_exchange_ptr(&ctx->sinkHandle, osinkHandle, NULL)) {
    QW_SINK_ENABLE_MEMPOOL(ctx);
    dsDestroyDataSinker(osinkHandle);
    QW_SINK_DISABLE_MEMPOOL();

    (void)atomic_add_fetch_64(&gQueryMgmt.stat.taskSinkDestroyNum, 1);
    
    qDebug("sink handle destroyed");
  }
}

int32_t qwKillTaskHandle(SQWTaskCtx *ctx, int32_t rspCode) {
  int32_t code = 0;

  // Note: free/kill may in RC
  qTaskInfo_t taskHandle = atomic_load_ptr(&ctx->taskHandle);
  if (taskHandle && atomic_val_compare_exchange_ptr(&ctx->taskHandle, taskHandle, NULL)) {
    qDebug("start to kill task");
    code = qAsyncKillTask(taskHandle, rspCode);
    atomic_store_ptr(&ctx->taskHandle, taskHandle);
  }

  QW_RET(code);
}

void qwFreeTaskCtx(QW_FPARAMS_DEF, SQWTaskCtx *ctx) {
  if (ctx->ctrlConnInfo.handle) {
    tmsgReleaseHandle(&ctx->ctrlConnInfo, TAOS_CONN_SERVER, ctx->rspCode);
  }

  ctx->ctrlConnInfo.handle = NULL;
  ctx->ctrlConnInfo.refId = -1;

  // NO need to release dataConnInfo

  qwFreeTaskHandle(ctx);

  qwFreeSinkHandle(ctx);

  taosArrayDestroy(ctx->tbInfo);

  if (gMemPoolHandle && ctx->memPoolSession) {
    qwDestroySession(QW_FPARAMS(), ctx->pJobInfo, ctx->memPoolSession, true);
    ctx->memPoolSession = NULL;
  }
}

static void freeExplainExecItem(void *param) {
  SExplainExecInfo *pInfo = param;
  taosMemoryFree(pInfo->verboseInfo);
}

int32_t qwSendExplainResponse(QW_FPARAMS_DEF, SQWTaskCtx *ctx) {
  int32_t     code = TSDB_CODE_SUCCESS;
  qTaskInfo_t taskHandle = ctx->taskHandle;

  ctx->explainRsped = true;

  SArray *execInfoList = taosArrayInit(4, sizeof(SExplainExecInfo));
  if (NULL == execInfoList) {
    QW_ERR_JRET(terrno);
  }

  QW_ERR_JRET(qGetExplainExecInfo(taskHandle, execInfoList));

  if (ctx->localExec) {
    SExplainLocalRsp localRsp = {0};
    localRsp.rsp.numOfPlans = taosArrayGetSize(execInfoList);
    SExplainExecInfo *pExec = taosMemoryCalloc(localRsp.rsp.numOfPlans, sizeof(SExplainExecInfo));
    if (NULL == pExec) {
      QW_ERR_JRET(terrno);
    }
    (void)memcpy(pExec, taosArrayGet(execInfoList, 0), localRsp.rsp.numOfPlans * sizeof(SExplainExecInfo));
    localRsp.rsp.subplanInfo = pExec;
    localRsp.qId = qId;
    localRsp.cId = cId;
    localRsp.tId = tId;
    localRsp.rId = rId;
    localRsp.eId = eId;
    if (NULL == taosArrayPush(ctx->explainRes, &localRsp)) {
      QW_ERR_JRET(terrno);
    }

    taosArrayDestroy(execInfoList);
    execInfoList = NULL;
  } else {
    SRpcHandleInfo connInfo = ctx->ctrlConnInfo;
    connInfo.ahandle = NULL;
    int32_t code = qwBuildAndSendExplainRsp(&connInfo, execInfoList);
    taosArrayDestroyEx(execInfoList, freeExplainExecItem);
    execInfoList = NULL;

    QW_ERR_RET(code);
  }

_return:

  taosArrayDestroyEx(execInfoList, freeExplainExecItem);

  return code;
}

int32_t qwDropTaskCtx(QW_FPARAMS_DEF) {
  char id[sizeof(qId) + sizeof(cId) + sizeof(tId) + sizeof(eId)] = {0};
  QW_SET_QTID(id, qId, cId, tId, eId);
  SQWTaskCtx octx;
  int32_t code = TSDB_CODE_SUCCESS;

  SQWTaskCtx *ctx = taosHashGet(mgmt->ctxHash, id, sizeof(id));
  if (NULL == ctx) {
    QW_TASK_DLOG_E("drop task ctx not exist, may be dropped");
    QW_ERR_RET(QW_CTX_NOT_EXISTS_ERR_CODE(mgmt));
  }

  octx = *ctx;

  if (ctx->pJobInfo && TSDB_CODE_SUCCESS != ctx->pJobInfo->errCode) {
    QW_UPDATE_RSP_CODE(ctx, ctx->pJobInfo->errCode);
  } else {
    QW_UPDATE_RSP_CODE(ctx, TSDB_CODE_TSC_QUERY_CANCELLED);
  }

  atomic_store_ptr(&ctx->taskHandle, NULL);
  atomic_store_ptr(&ctx->sinkHandle, NULL);
  atomic_store_ptr(&ctx->pJobInfo, NULL);
  atomic_store_ptr(&ctx->memPoolSession, NULL);

  QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_DROP);

  if (taosHashRemove(mgmt->ctxHash, id, sizeof(id))) {
    QW_TASK_ELOG_E("taosHashRemove from ctx hash failed");
    code = QW_CTX_NOT_EXISTS_ERR_CODE(mgmt);
  }

  qwFreeTaskCtx(QW_FPARAMS(), &octx);
  ctx->tbInfo = NULL;

  QW_TASK_DLOG_E("task ctx dropped");
  
  (void)atomic_add_fetch_64(&gQueryMgmt.stat.taskDestroyNum, 1);

  return code;
}

int32_t qwDropTaskStatus(QW_FPARAMS_DEF) {
  SQWSchStatus  *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t        code = 0;

  char id[sizeof(qId) + sizeof(cId) + sizeof(tId) + sizeof(eId)] = {0};
  QW_SET_QTID(id, qId, cId, tId, eId);

  if (qwAcquireScheduler(mgmt, cId, QW_WRITE, &sch)) {
    QW_TASK_WLOG_E("scheduler does not exist");
    return TSDB_CODE_SUCCESS;
  }

  if (qwAcquireTaskStatus(QW_FPARAMS(), QW_WRITE, sch, &task)) {
    qwReleaseScheduler(QW_WRITE, mgmt);

    QW_TASK_WLOG_E("task does not exist");
    return TSDB_CODE_SUCCESS;
  }

  if (taosHashRemove(sch->tasksHash, id, sizeof(id))) {
    QW_TASK_ELOG_E("taosHashRemove task from hash failed");
    QW_ERR_JRET(TSDB_CODE_APP_ERROR);
  }

  QW_TASK_DLOG_E("task status dropped");

_return:

  if (task) {
    qwReleaseTaskStatus(QW_WRITE, sch);
  }
  qwReleaseScheduler(QW_WRITE, mgmt);

  QW_RET(code);
}

int32_t qwUpdateTaskStatus(QW_FPARAMS_DEF, int8_t status, bool dynamicTask) {
  SQWSchStatus  *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t        code = 0;

  QW_ERR_RET(qwAcquireScheduler(mgmt, cId, QW_READ, &sch));
  QW_ERR_JRET(qwAcquireTaskStatus(QW_FPARAMS(), QW_READ, sch, &task));

  QW_ERR_JRET(qwSetTaskStatus(QW_FPARAMS(), task, status, dynamicTask));

_return:

  if (task) {
    qwReleaseTaskStatus(QW_READ, sch);
  }
  qwReleaseScheduler(QW_READ, mgmt);

  QW_RET(code);
}

int32_t qwHandleDynamicTaskEnd(QW_FPARAMS_DEF) {
  char id[sizeof(qId) + sizeof(cId) + sizeof(tId) + sizeof(eId)] = {0};
  QW_SET_QTID(id, qId, cId, tId, eId);
  SQWTaskCtx octx;

  SQWTaskCtx *ctx = taosHashGet(mgmt->ctxHash, id, sizeof(id));
  if (NULL == ctx) {
    QW_TASK_DLOG_E("task ctx not exist, may be dropped");
    QW_ERR_RET(QW_CTX_NOT_EXISTS_ERR_CODE(mgmt));
  }

  if (!ctx->dynamicTask) {
    return TSDB_CODE_SUCCESS;
  }

  QW_ERR_RET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_SUCC, ctx->dynamicTask));

  QW_ERR_RET(qwHandleTaskComplete(QW_FPARAMS(), ctx));

  return TSDB_CODE_SUCCESS;
}

int32_t qwDropTask(QW_FPARAMS_DEF) {
  QW_ERR_RET(qwHandleDynamicTaskEnd(QW_FPARAMS()));
  QW_ERR_RET(qwDropTaskStatus(QW_FPARAMS()));
  QW_ERR_RET(qwDropTaskCtx(QW_FPARAMS()));

  QW_TASK_DLOG_E("task is dropped");

  return TSDB_CODE_SUCCESS;
}

void qwSetHbParam(int64_t refId, SQWHbParam **pParam) {
  int32_t paramIdx = 0;
  int32_t newParamIdx = 0;

  while (true) {
    paramIdx = atomic_load_32(&gQwMgmt.paramIdx);
    if (paramIdx == tListLen(gQwMgmt.param)) {
      newParamIdx = 1;
    } else {
      newParamIdx = paramIdx + 1;
    }

    if (paramIdx == atomic_val_compare_exchange_32(&gQwMgmt.paramIdx, paramIdx, newParamIdx)) {
      break;
    }
  }

  if (paramIdx == tListLen(gQwMgmt.param)) {
    paramIdx = 0;
  }

  gQwMgmt.param[paramIdx].qwrId = gQwMgmt.qwRef;
  gQwMgmt.param[paramIdx].refId = refId;

  *pParam = &gQwMgmt.param[paramIdx];
}

int32_t qwSaveTbVersionInfo(qTaskInfo_t pTaskInfo, SQWTaskCtx *ctx) {
  char       dbFName[TSDB_DB_FNAME_LEN];
  char       tbName[TSDB_TABLE_NAME_LEN];
  STbVerInfo tbInfo;
  int32_t    i = 0;
  int32_t    code = TSDB_CODE_SUCCESS;
  bool       tbGet = false;

  while (true) {
    tbGet = false;
    code = qGetQueryTableSchemaVersion(pTaskInfo, dbFName, TSDB_DB_FNAME_LEN, tbName, TSDB_TABLE_NAME_LEN,
                                       &tbInfo.sversion, &tbInfo.tversion, &tbInfo.rversion, i, &tbGet);
    if (TSDB_CODE_SUCCESS != code || !tbGet) {
      break;
    }

    if (dbFName[0] && tbName[0]) {
      (void)snprintf(tbInfo.tbFName, sizeof(tbInfo.tbFName), "%s.%s", dbFName, tbName);
    } else {
      tbInfo.tbFName[0] = 0;
    }

    if (NULL == ctx->tbInfo) {
      ctx->tbInfo = taosArrayInit(1, sizeof(tbInfo));
      if (NULL == ctx->tbInfo) {
        QW_ERR_RET(terrno);
      }
    }

    if (NULL == taosArrayPush(ctx->tbInfo, &tbInfo)) {
      QW_ERR_RET(terrno);
    }

    i++;
  }

  QW_RET(code);
}

void qwCloseRef(void) {
  taosWLockLatch(&gQwMgmt.lock);
  if (atomic_load_32(&gQwMgmt.qwNum) <= 0 && gQwMgmt.qwRef >= 0) {
    taosCloseRef(gQwMgmt.qwRef);  // ignore error
    gQwMgmt.qwRef = -1;

    taosHashCleanup(gQueryMgmt.pJobInfo);
    gQueryMgmt.pJobInfo = NULL;
  }
  taosWUnLockLatch(&gQwMgmt.lock);
}

void qwDestroySchStatus(SQWSchStatus *pStatus) { taosHashCleanup(pStatus->tasksHash); }

void qwDestroyImpl(void *pMgmt) {
  SQWorker *mgmt = (SQWorker *)pMgmt;
  int8_t    nodeType = mgmt->nodeType;
  int32_t   nodeId = mgmt->nodeId;

  int32_t taskCount = 0;
  int32_t schStatusCount = 0;
  qDebug("start to destroy qworker, type:%d, id:%d, handle:%p", nodeType, nodeId, mgmt);

  if (taosTmrStop(mgmt->hbTimer)) {
    qTrace("stop qworker hb timer may failed");
  }

  mgmt->hbTimer = NULL;
  taosTmrCleanUp(mgmt->timer);

  uint64_t qId, cId, tId, sId;
  int32_t  eId;
  int64_t  rId = 0;
  void    *pIter = taosHashIterate(mgmt->ctxHash, NULL);

  while (pIter) {
    SQWTaskCtx *ctx = (SQWTaskCtx *)pIter;
    void       *key = taosHashGetKey(pIter, NULL);
    QW_GET_QTID(key, qId, cId, tId, eId);
    sId = ctx->sId;

    qwFreeTaskCtx(QW_FPARAMS(), ctx);
    QW_TASK_DLOG_E("task ctx freed");
    pIter = taosHashIterate(mgmt->ctxHash, pIter);
    taskCount++;
  }
  taosHashCleanup(mgmt->ctxHash);

  pIter = taosHashIterate(mgmt->schHash, NULL);
  while (pIter) {
    SQWSchStatus *sch = (SQWSchStatus *)pIter;
    qwDestroySchStatus(sch);

    pIter = taosHashIterate(mgmt->schHash, pIter);
    schStatusCount++;
  }
  taosHashCleanup(mgmt->schHash);

  *mgmt->destroyed = 1;

  taosMemoryFree(mgmt);

  (void)atomic_sub_fetch_32(&gQwMgmt.qwNum, 1);

  qwCloseRef();

  qDebug("qworker destroyed, type:%d, id:%d, handle:%p, taskCount:%d, schStatusCount: %d", nodeType, nodeId, mgmt,
         taskCount, schStatusCount);
}

int32_t qwOpenRef(void) {
  taosWLockLatch(&gQwMgmt.lock);
  if (gQwMgmt.qwRef < 0) {
    gQwMgmt.qwRef = taosOpenRef(100, qwDestroyImpl);
    if (gQwMgmt.qwRef < 0) {
      taosWUnLockLatch(&gQwMgmt.lock);
      qError("init qworker ref failed");
      QW_RET(gQwMgmt.qwRef);
    }
  }
  taosWUnLockLatch(&gQwMgmt.lock);

  return TSDB_CODE_SUCCESS;
}

int32_t qwUpdateTimeInQueue(SQWorker *mgmt, int64_t ts, EQueueType type) {
  if (ts <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t duration = taosGetTimestampUs() - ts;
  switch (type) {
    case QUERY_QUEUE:
      ++mgmt->stat.msgStat.waitTime[0].num;
      mgmt->stat.msgStat.waitTime[0].total += duration;
      break;
    case FETCH_QUEUE:
      ++mgmt->stat.msgStat.waitTime[1].num;
      mgmt->stat.msgStat.waitTime[1].total += duration;
      break;
    default:
      qError("unsupported queue type %d", type);
      return TSDB_CODE_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}

int64_t qwGetTimeInQueue(SQWorker *mgmt, EQueueType type) {
  SQWTimeInQ *pStat = NULL;
  switch (type) {
    case QUERY_QUEUE:
      pStat = &mgmt->stat.msgStat.waitTime[0];
      return pStat->num ? (pStat->total / pStat->num) : 0;
    case FETCH_QUEUE:
      pStat = &mgmt->stat.msgStat.waitTime[1];
      return pStat->num ? (pStat->total / pStat->num) : 0;
    default:
      qError("unsupported queue type %d", type);
      break;
  }

  return -1;
}

void qwClearExpiredSch(SQWorker *mgmt, SArray *pExpiredSch) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t num = taosArrayGetSize(pExpiredSch);
  for (int32_t i = 0; i < num; ++i) {
    uint64_t     *clientId = taosArrayGet(pExpiredSch, i);
    SQWSchStatus *pSch = NULL;
    if (NULL == clientId) {
      qError("get the %dth client failed, code:%x", i, terrno);
      break;
    }

    code = qwAcquireScheduler(mgmt, *clientId, QW_WRITE, &pSch);
    if (TSDB_CODE_SUCCESS != code) {
      qError("acquire client %" PRIx64 " failed, code:%x", *clientId, code);
      continue;
    }

    if (taosHashGetSize(pSch->tasksHash) <= 0) {
      qwDestroySchStatus(pSch);
      code = taosHashRemove(mgmt->schHash, clientId, sizeof(*clientId));
      qDebug("client %" PRIx64 " destroy result code:%x", *clientId, code);
    }

    qwReleaseScheduler(QW_WRITE, mgmt);
  }
}

void qwDestroyJobInfo(void *job) {
  if (NULL == job) {
    return;
  }

  SQWJobInfo *pJob = (SQWJobInfo *)job;

  taosMemoryFreeClear(pJob->memInfo);
  taosHashCleanup(pJob->pSessions);
  pJob->pSessions = NULL;
}

bool qwStopTask(QW_FPARAMS_DEF, SQWTaskCtx    *ctx, bool forceStop, int32_t errCode) {
  int32_t code = TSDB_CODE_SUCCESS;
  bool resFreed = false;
  
  QW_LOCK(QW_WRITE, &ctx->lock);
  
  QW_TASK_DLOG("start to stop task, forceStop:%d, error:%s", forceStop, tstrerror(errCode));
  
  if ((!forceStop) && (QW_EVENT_RECEIVED(ctx, QW_EVENT_DROP) || QW_EVENT_PROCESSED(ctx, QW_EVENT_DROP))) {
    QW_TASK_WLOG_E("task already dropping");
    QW_UNLOCK(QW_WRITE, &ctx->lock);
  
    return resFreed;
  }

  if (QW_QUERY_RUNNING(ctx)) {
    code = qwKillTaskHandle(ctx, errCode);
    if (TSDB_CODE_SUCCESS != code) {
      QW_TASK_ELOG("task running, async kill failed, error: %x", code);
    } else {
      QW_TASK_DLOG_E("task running, async killed");
    }
  } else if (QW_FETCH_RUNNING(ctx)) {
    QW_TASK_DLOG_E("task fetching");
    QW_UPDATE_RSP_CODE(ctx, errCode);
    if (forceStop) {
      QW_SET_EVENT_RECEIVED(ctx, QW_EVENT_DROP);
      QW_TASK_DLOG_E("update drop received");
    }
  } else if (forceStop) {
    QW_UPDATE_RSP_CODE(ctx, errCode);
    code = qwDropTask(QW_FPARAMS());
    if (TSDB_CODE_SUCCESS != code) {
      QW_TASK_ELOG("task drop failed, error: %x", code);
    } else {
      QW_TASK_DLOG_E("task dropped");
      resFreed = true;
    }
  } else {
    QW_UPDATE_RSP_CODE(ctx, errCode);
    
    qwFreeTaskHandle(ctx);
    qwFreeSinkHandle(ctx);

    resFreed = true;
    
    QW_TASK_DLOG_E("task resources freed");
  }

  QW_UNLOCK(QW_WRITE, &ctx->lock);

  return resFreed;
}

bool qwRetireTask(QW_FPARAMS_DEF, int32_t errCode) {
  SQWTaskCtx    *ctx = NULL;

  int32_t code = qwAcquireTaskCtx(QW_FPARAMS(), &ctx);
  if (TSDB_CODE_SUCCESS != code) {
    return false;
  }

  bool retired = qwStopTask(QW_FPARAMS(), ctx, false, errCode);

  qwReleaseTaskCtx(mgmt, ctx);

  return retired;
}

bool qwRetireJob(SQWJobInfo *pJob) {
  if (NULL == pJob) {
    return false;
  }

  bool  retired = true;
  void *pIter = taosHashIterate(pJob->pSessions, NULL);
  while (pIter) {
    SQWSessionInfo *pSession = (SQWSessionInfo *)pIter;

    if (!qwRetireTask((SQWorker *)pSession->mgmt, pSession->sId, pSession->qId, pSession->cId, pSession->tId, pSession->rId, pSession->eId, pJob->errCode)) {
      retired = false;
    }

    pIter = taosHashIterate(pJob->pSessions, pIter);
  }

  return retired;
}


void qwStopAllTasks(SQWorker *mgmt) {
  uint64_t qId, cId, tId, sId;
  int32_t  eId;
  int64_t  rId = 0;

  void *pIter = taosHashIterate(mgmt->ctxHash, NULL);
  while (pIter) {
    SQWTaskCtx *ctx = (SQWTaskCtx *)pIter;
    void       *key = taosHashGetKey(pIter, NULL);
    QW_GET_QTID(key, qId, cId, tId, eId);

    sId = ctx->sId;

    (void)qwStopTask(QW_FPARAMS(), ctx, true, TSDB_CODE_VND_STOPPED);

    pIter = taosHashIterate(mgmt->ctxHash, pIter);
  }
}


void qwChkDropTimeoutQuery(SQWorker *mgmt, int32_t currTs) {
  uint64_t qId, cId, tId, sId;
  int32_t  eId;
  int64_t  rId = 0;

  void *pIter = taosHashIterate(mgmt->ctxHash, NULL);
  while (pIter) {
    SQWTaskCtx *ctx = (SQWTaskCtx *)pIter;
    if (((ctx->lastAckTs <= 0) || (currTs - ctx->lastAckTs) < tsQueryNoFetchTimeoutSec) && (!QW_EVENT_RECEIVED(ctx, QW_EVENT_DROP))) {
      pIter = taosHashIterate(mgmt->ctxHash, pIter);
      continue;
    }
    
    void       *key = taosHashGetKey(pIter, NULL);
    QW_GET_QTID(key, qId, cId, tId, eId);

    sId = ctx->sId;

    (void)qwStopTask(QW_FPARAMS(), ctx, true, (QW_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) ? ctx->rspCode : TSDB_CODE_QRY_NO_FETCH_TIMEOUT);

    pIter = taosHashIterate(mgmt->ctxHash, pIter);
  }
}

