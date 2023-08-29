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

int32_t qwAddSchedulerImpl(SQWorker *mgmt, uint64_t sId, int32_t rwType) {
  SQWSchStatus newSch = {0};
  newSch.tasksHash =
      taosHashInit(mgmt->cfg.maxSchTaskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  newSch.hbBrokenTs = taosGetTimestampMs();

  if (NULL == newSch.tasksHash) {
    QW_SCH_ELOG("taosHashInit %d failed", mgmt->cfg.maxSchTaskNum);
    QW_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  QW_LOCK(QW_WRITE, &mgmt->schLock);
  int32_t code = taosHashPut(mgmt->schHash, &sId, sizeof(sId), &newSch, sizeof(newSch));
  if (0 != code) {
    if (!HASH_NODE_EXIST(code)) {
      QW_UNLOCK(QW_WRITE, &mgmt->schLock);

      QW_SCH_ELOG("taosHashPut new sch to scheduleHash failed, errno:%d", errno);
      taosHashCleanup(newSch.tasksHash);
      QW_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    taosHashCleanup(newSch.tasksHash);
  }
  QW_UNLOCK(QW_WRITE, &mgmt->schLock);

  return TSDB_CODE_SUCCESS;
}

int32_t qwAcquireSchedulerImpl(SQWorker *mgmt, uint64_t sId, int32_t rwType, SQWSchStatus **sch, int32_t nOpt) {
  while (true) {
    QW_LOCK(rwType, &mgmt->schLock);
    *sch = taosHashGet(mgmt->schHash, &sId, sizeof(sId));
    if (NULL == (*sch)) {
      QW_UNLOCK(rwType, &mgmt->schLock);

      if (QW_NOT_EXIST_ADD == nOpt) {
        QW_ERR_RET(qwAddSchedulerImpl(mgmt, sId, rwType));

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

int32_t qwAcquireAddScheduler(SQWorker *mgmt, uint64_t sId, int32_t rwType, SQWSchStatus **sch) {
  return qwAcquireSchedulerImpl(mgmt, sId, rwType, sch, QW_NOT_EXIST_ADD);
}

int32_t qwAcquireScheduler(SQWorker *mgmt, uint64_t sId, int32_t rwType, SQWSchStatus **sch) {
  return qwAcquireSchedulerImpl(mgmt, sId, rwType, sch, QW_NOT_EXIST_RET_ERR);
}

void qwReleaseScheduler(int32_t rwType, SQWorker *mgmt) { QW_UNLOCK(rwType, &mgmt->schLock); }

int32_t qwAcquireTaskStatus(QW_FPARAMS_DEF, int32_t rwType, SQWSchStatus *sch, SQWTaskStatus **task) {
  char id[sizeof(qId) + sizeof(tId) + sizeof(eId)] = {0};
  QW_SET_QTID(id, qId, tId, eId);

  QW_LOCK(rwType, &sch->tasksLock);
  *task = taosHashGet(sch->tasksHash, id, sizeof(id));
  if (NULL == (*task)) {
    QW_UNLOCK(rwType, &sch->tasksLock);
    QW_ERR_RET(TSDB_CODE_QRY_TASK_NOT_EXIST);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwAddTaskStatusImpl(QW_FPARAMS_DEF, SQWSchStatus *sch, int32_t rwType, int32_t status, SQWTaskStatus **task) {
  int32_t code = 0;

  char id[sizeof(qId) + sizeof(tId) + sizeof(eId)] = {0};
  QW_SET_QTID(id, qId, tId, eId);

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
      QW_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
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
  QW_ERR_RET(qwAcquireAddScheduler(mgmt, sId, QW_READ, &tsch));

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
  char id[sizeof(qId) + sizeof(tId) + sizeof(eId)] = {0};
  QW_SET_QTID(id, qId, tId, eId);

  *ctx = taosHashAcquire(mgmt->ctxHash, id, sizeof(id));
  if (NULL == (*ctx)) {
    QW_TASK_DLOG_E("acquired task ctx not exist, may be dropped");
    QW_ERR_RET(QW_CTX_NOT_EXISTS_ERR_CODE(mgmt));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwGetTaskCtx(QW_FPARAMS_DEF, SQWTaskCtx **ctx) {
  char id[sizeof(qId) + sizeof(tId) + sizeof(eId)] = {0};
  QW_SET_QTID(id, qId, tId, eId);

  *ctx = taosHashGet(mgmt->ctxHash, id, sizeof(id));
  if (NULL == (*ctx)) {
    QW_TASK_DLOG_E("get task ctx not exist, may be dropped");
    QW_ERR_RET(QW_CTX_NOT_EXISTS_ERR_CODE(mgmt));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwAddTaskCtxImpl(QW_FPARAMS_DEF, bool acquire, SQWTaskCtx **ctx) {
  char id[sizeof(qId) + sizeof(tId) + sizeof(eId)] = {0};
  QW_SET_QTID(id, qId, tId, eId);

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
      QW_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

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

void qwFreeTaskHandle(qTaskInfo_t *taskHandle) {
  // Note: free/kill may in RC
  qTaskInfo_t otaskHandle = atomic_load_ptr(taskHandle);
  if (otaskHandle && atomic_val_compare_exchange_ptr(taskHandle, otaskHandle, NULL)) {
    qDestroyTask(otaskHandle);
    qDebug("task handle destroyed");
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

void qwFreeTaskCtx(SQWTaskCtx *ctx) {
  if (ctx->ctrlConnInfo.handle) {
    tmsgReleaseHandle(&ctx->ctrlConnInfo, TAOS_CONN_SERVER);
  }

  ctx->ctrlConnInfo.handle = NULL;
  ctx->ctrlConnInfo.refId = -1;

  // NO need to release dataConnInfo

  qwFreeTaskHandle(&ctx->taskHandle);

  if (ctx->sinkHandle) {
    dsDestroyDataSinker(ctx->sinkHandle);
    ctx->sinkHandle = NULL;
    qDebug("sink handle destroyed");
  }

  taosArrayDestroy(ctx->tbInfo);
}

static void freeExplainExecItem(void *param) {
  SExplainExecInfo *pInfo = param;
  taosMemoryFree(pInfo->verboseInfo);
}


int32_t qwSendExplainResponse(QW_FPARAMS_DEF, SQWTaskCtx *ctx) {
  qTaskInfo_t taskHandle = ctx->taskHandle;

  ctx->explainRsped = true;
  
  SArray *execInfoList = taosArrayInit(4, sizeof(SExplainExecInfo));
  QW_ERR_RET(qGetExplainExecInfo(taskHandle, execInfoList));
  
  if (ctx->localExec) {
    SExplainLocalRsp localRsp = {0};
    localRsp.rsp.numOfPlans = taosArrayGetSize(execInfoList);
    SExplainExecInfo *pExec = taosMemoryCalloc(localRsp.rsp.numOfPlans, sizeof(SExplainExecInfo));
    memcpy(pExec, taosArrayGet(execInfoList, 0), localRsp.rsp.numOfPlans * sizeof(SExplainExecInfo));
    localRsp.rsp.subplanInfo = pExec;
    localRsp.qId = qId;
    localRsp.tId = tId;
    localRsp.rId = rId;
    localRsp.eId = eId;
    taosArrayPush(ctx->explainRes, &localRsp);
    taosArrayDestroy(execInfoList);
  } else {
    SRpcHandleInfo connInfo = ctx->ctrlConnInfo;
    connInfo.ahandle = NULL;
    int32_t code = qwBuildAndSendExplainRsp(&connInfo, execInfoList);
    taosArrayDestroyEx(execInfoList, freeExplainExecItem);
    QW_ERR_RET(code);
  }

  return TSDB_CODE_SUCCESS;
}



int32_t qwDropTaskCtx(QW_FPARAMS_DEF) {
  char id[sizeof(qId) + sizeof(tId) + sizeof(eId)] = {0};
  QW_SET_QTID(id, qId, tId, eId);
  SQWTaskCtx octx;

  SQWTaskCtx *ctx = taosHashGet(mgmt->ctxHash, id, sizeof(id));
  if (NULL == ctx) {
    QW_TASK_DLOG_E("drop task ctx not exist, may be dropped");
    QW_ERR_RET(QW_CTX_NOT_EXISTS_ERR_CODE(mgmt));
  }

  octx = *ctx;

  atomic_store_ptr(&ctx->taskHandle, NULL);
  atomic_store_ptr(&ctx->sinkHandle, NULL);

  QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_DROP);

  if (taosHashRemove(mgmt->ctxHash, id, sizeof(id))) {
    QW_TASK_ELOG_E("taosHashRemove from ctx hash failed");
    QW_ERR_RET(QW_CTX_NOT_EXISTS_ERR_CODE(mgmt));
  }

  qwFreeTaskCtx(&octx);
  ctx->tbInfo = NULL;

  QW_TASK_DLOG_E("task ctx dropped");

  return TSDB_CODE_SUCCESS;
}

int32_t qwDropTaskStatus(QW_FPARAMS_DEF) {
  SQWSchStatus  *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t        code = 0;

  char id[sizeof(qId) + sizeof(tId) + sizeof(eId)] = {0};
  QW_SET_QTID(id, qId, tId, eId);

  if (qwAcquireScheduler(mgmt, sId, QW_WRITE, &sch)) {
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

  QW_ERR_RET(qwAcquireScheduler(mgmt, sId, QW_READ, &sch));
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
  char id[sizeof(qId) + sizeof(tId) + sizeof(eId)] = {0};
  QW_SET_QTID(id, qId, tId, eId);
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

void qwSaveTbVersionInfo(qTaskInfo_t pTaskInfo, SQWTaskCtx *ctx) {
  char dbFName[TSDB_DB_FNAME_LEN];
  char tbName[TSDB_TABLE_NAME_LEN];
  STbVerInfo tbInfo;
  int32_t i = 0;

  while (true) {
    if (qGetQueryTableSchemaVersion(pTaskInfo, dbFName, tbName, &tbInfo.sversion, &tbInfo.tversion, i) < 0) {
      break;
    }

    if (dbFName[0] && tbName[0]) {
      sprintf(tbInfo.tbFName, "%s.%s", dbFName, tbName);
    } else {
      tbInfo.tbFName[0] = 0;
    }

    if (NULL == ctx->tbInfo) {
      ctx->tbInfo = taosArrayInit(1, sizeof(tbInfo));
    }
    
    taosArrayPush(ctx->tbInfo, &tbInfo);
    
    i++;
  }
}

void qwCloseRef(void) {
  taosWLockLatch(&gQwMgmt.lock);
  if (atomic_load_32(&gQwMgmt.qwNum) <= 0 && gQwMgmt.qwRef >= 0) {
    taosCloseRef(gQwMgmt.qwRef);
    gQwMgmt.qwRef = -1;
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

  taosTmrStop(mgmt->hbTimer);
  mgmt->hbTimer = NULL;
  taosTmrCleanUp(mgmt->timer);

  uint64_t qId, tId;
  int32_t  eId;
  void    *pIter = taosHashIterate(mgmt->ctxHash, NULL);

  while (pIter) {
    SQWTaskCtx *ctx = (SQWTaskCtx *)pIter;
    void       *key = taosHashGetKey(pIter, NULL);
    QW_GET_QTID(key, qId, tId, eId);

    qwFreeTaskCtx(ctx);
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

  atomic_sub_fetch_32(&gQwMgmt.qwNum, 1);

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
      QW_RET(TSDB_CODE_OUT_OF_MEMORY);
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
  }

  return -1;
}

void qwClearExpiredSch(SQWorker *mgmt, SArray *pExpiredSch) {
  int32_t num = taosArrayGetSize(pExpiredSch);
  for (int32_t i = 0; i < num; ++i) {
    uint64_t     *sId = taosArrayGet(pExpiredSch, i);
    SQWSchStatus *pSch = NULL;
    if (qwAcquireScheduler(mgmt, *sId, QW_WRITE, &pSch)) {
      continue;
    }

    if (taosHashGetSize(pSch->tasksHash) <= 0) {
      qwDestroySchStatus(pSch);
      taosHashRemove(mgmt->schHash, sId, sizeof(*sId));
      qDebug("sch %" PRIx64 " destroyed", *sId);
    }

    qwReleaseScheduler(QW_WRITE, mgmt);
  }
}
