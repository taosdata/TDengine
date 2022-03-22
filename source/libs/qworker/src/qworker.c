#include "qworker.h"
#include "tcommon.h"
#include "executor.h"
#include "planner.h"
#include "query.h"
#include "qworkerInt.h"
#include "qworkerMsg.h"
#include "tmsg.h"
#include "tname.h"
#include "dataSinkMgt.h"

SQWDebug gQWDebug = {.statusEnable = true};

int32_t qwDbgValidateStatus(QW_FPARAMS_DEF, int8_t oriStatus, int8_t newStatus, bool *ignore) {
  if (!gQWDebug.statusEnable) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = 0;

  if (oriStatus == newStatus) {
    if (newStatus == JOB_TASK_STATUS_EXECUTING || newStatus == JOB_TASK_STATUS_FAILED) {
      *ignore = true;
      return TSDB_CODE_SUCCESS;
    }

    QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
  }
  
  switch (oriStatus) {
    case JOB_TASK_STATUS_NULL:
      if (newStatus != JOB_TASK_STATUS_EXECUTING 
       && newStatus != JOB_TASK_STATUS_FAILED 
       && newStatus != JOB_TASK_STATUS_NOT_START) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }
      
      break;
    case JOB_TASK_STATUS_NOT_START:
      if (newStatus != JOB_TASK_STATUS_CANCELLED) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }
      
      break;
    case JOB_TASK_STATUS_EXECUTING:
      if (newStatus != JOB_TASK_STATUS_PARTIAL_SUCCEED 
       && newStatus != JOB_TASK_STATUS_SUCCEED 
       && newStatus != JOB_TASK_STATUS_FAILED 
       && newStatus != JOB_TASK_STATUS_CANCELLING 
       && newStatus != JOB_TASK_STATUS_CANCELLED 
       && newStatus != JOB_TASK_STATUS_DROPPING) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }
      
      break;
    case JOB_TASK_STATUS_PARTIAL_SUCCEED:
      if (newStatus != JOB_TASK_STATUS_EXECUTING 
       && newStatus != JOB_TASK_STATUS_SUCCEED
       && newStatus != JOB_TASK_STATUS_CANCELLED
       && newStatus != JOB_TASK_STATUS_FAILED
       && newStatus != JOB_TASK_STATUS_DROPPING) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }
      
      break;
    case JOB_TASK_STATUS_SUCCEED:
      if (newStatus != JOB_TASK_STATUS_CANCELLED
       && newStatus != JOB_TASK_STATUS_DROPPING
       && newStatus != JOB_TASK_STATUS_FAILED) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }

      break;
    case JOB_TASK_STATUS_FAILED:
      if (newStatus != JOB_TASK_STATUS_CANCELLED && newStatus != JOB_TASK_STATUS_DROPPING) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }
      break;

    case JOB_TASK_STATUS_CANCELLING:
      if (newStatus != JOB_TASK_STATUS_CANCELLED) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }
      
      break;
    case JOB_TASK_STATUS_CANCELLED:
    case JOB_TASK_STATUS_DROPPING:
      if (newStatus != JOB_TASK_STATUS_FAILED && newStatus != JOB_TASK_STATUS_PARTIAL_SUCCEED) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }
      break;
      
    default:
      QW_TASK_ELOG("invalid task origStatus:%s", jobTaskStatusStr(oriStatus));
      return TSDB_CODE_QRY_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;

_return:

  QW_TASK_ELOG("invalid task status update from %s to %s", jobTaskStatusStr(oriStatus), jobTaskStatusStr(newStatus));
  QW_RET(code);
}


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

int32_t qwSetTaskStatus(QW_FPARAMS_DEF, SQWTaskStatus *task, int8_t status) {
  int32_t code = 0;
  int8_t origStatus = 0;
  bool ignore = false;

  while (true) {
    origStatus = atomic_load_8(&task->status);
    
    QW_ERR_RET(qwDbgValidateStatus(QW_FPARAMS(), origStatus, status, &ignore));
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


int32_t qwAddSchedulerImpl(SQWorkerMgmt *mgmt, uint64_t sId, int32_t rwType, SQWSchStatus **sch) {
  SQWSchStatus newSch = {0};
  newSch.tasksHash = taosHashInit(mgmt->cfg.maxSchTaskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (NULL == newSch.tasksHash) {
    QW_SCH_ELOG("taosHashInit %d failed", mgmt->cfg.maxSchTaskNum);
    QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  QW_LOCK(QW_WRITE, &mgmt->schLock);
  int32_t code = taosHashPut(mgmt->schHash, &sId, sizeof(sId), &newSch, sizeof(newSch));
  if (0 != code) {
    if (!HASH_NODE_EXIST(code)) {
      QW_UNLOCK(QW_WRITE, &mgmt->schLock);
      
      QW_SCH_ELOG("taosHashPut new sch to scheduleHash failed, errno:%d", errno);
      taosHashCleanup(newSch.tasksHash);
      QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    taosHashCleanup(newSch.tasksHash);
  }
  QW_UNLOCK(QW_WRITE, &mgmt->schLock);

  return TSDB_CODE_SUCCESS;  
}

int32_t qwAcquireSchedulerImpl(SQWorkerMgmt *mgmt, uint64_t sId, int32_t rwType, SQWSchStatus **sch, int32_t nOpt) {
  while (true) {
    QW_LOCK(rwType, &mgmt->schLock);
    *sch = taosHashGet(mgmt->schHash, &sId, sizeof(sId));
    if (NULL == (*sch)) {
      QW_UNLOCK(rwType, &mgmt->schLock);
      
      if (QW_NOT_EXIST_ADD == nOpt) {
        QW_ERR_RET(qwAddSchedulerImpl(mgmt, sId, rwType, sch));

        nOpt = QW_NOT_EXIST_RET_ERR;
        
        continue;
      } else if (QW_NOT_EXIST_RET_ERR == nOpt) {
        QW_RET(TSDB_CODE_QRY_SCH_NOT_EXIST);
      } else {
        QW_SCH_ELOG("unknown notExistOpt:%d", nOpt);
        QW_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
      }
    }

    break;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwAcquireAddScheduler(SQWorkerMgmt *mgmt, uint64_t sId, int32_t rwType, SQWSchStatus **sch) {
  return qwAcquireSchedulerImpl(mgmt, sId, rwType, sch, QW_NOT_EXIST_ADD);
}

int32_t qwAcquireScheduler(SQWorkerMgmt *mgmt, uint64_t sId, int32_t rwType, SQWSchStatus **sch) {
  return qwAcquireSchedulerImpl(mgmt, sId, rwType, sch, QW_NOT_EXIST_RET_ERR);
}

void qwReleaseScheduler(int32_t rwType, SQWorkerMgmt *mgmt) {
  QW_UNLOCK(rwType, &mgmt->schLock);
}


int32_t qwAcquireTaskStatus(QW_FPARAMS_DEF, int32_t rwType, SQWSchStatus *sch, SQWTaskStatus **task) {
  char id[sizeof(qId) + sizeof(tId)] = {0};
  QW_SET_QTID(id, qId, tId);

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

  char id[sizeof(qId) + sizeof(tId)] = {0};
  QW_SET_QTID(id, qId, tId);

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
      QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
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
  int32_t code = 0;
  QW_ERR_RET(qwAcquireAddScheduler(mgmt, sId, QW_READ, &tsch));

  QW_ERR_JRET(qwAddTaskStatusImpl(QW_FPARAMS(), tsch, 0, status, NULL));

_return:

  qwReleaseScheduler(QW_READ, mgmt);
  
  QW_RET(code);
}


int32_t qwAddAcquireTaskStatus(QW_FPARAMS_DEF, int32_t rwType, SQWSchStatus *sch, int32_t status, SQWTaskStatus **task) {
  return qwAddTaskStatusImpl(QW_FPARAMS(), sch, rwType, status, task);
}


void qwReleaseTaskStatus(int32_t rwType, SQWSchStatus *sch) {
  QW_UNLOCK(rwType, &sch->tasksLock);
}


int32_t qwAcquireTaskCtx(QW_FPARAMS_DEF, SQWTaskCtx **ctx) {
  char id[sizeof(qId) + sizeof(tId)] = {0};
  QW_SET_QTID(id, qId, tId);

  *ctx = taosHashAcquire(mgmt->ctxHash, id, sizeof(id));
  if (NULL == (*ctx)) {
    QW_TASK_DLOG_E("task ctx not exist, may be dropped");
    QW_ERR_RET(TSDB_CODE_QRY_TASK_CTX_NOT_EXIST);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwGetTaskCtx(QW_FPARAMS_DEF, SQWTaskCtx **ctx) {
  char id[sizeof(qId) + sizeof(tId)] = {0};
  QW_SET_QTID(id, qId, tId);
  
  *ctx = taosHashGet(mgmt->ctxHash, id, sizeof(id));
  if (NULL == (*ctx)) {
    QW_TASK_DLOG_E("task ctx not exist, may be dropped");
    QW_ERR_RET(TSDB_CODE_QRY_TASK_CTX_NOT_EXIST);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwAddTaskCtxImpl(QW_FPARAMS_DEF, bool acquire, SQWTaskCtx **ctx) {
  char id[sizeof(qId) + sizeof(tId)] = {0};
  QW_SET_QTID(id, qId, tId);

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
      QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
  }

  if (acquire && ctx) {
    QW_RET(qwAcquireTaskCtx(QW_FPARAMS(), ctx));
  } else if (ctx) {
    QW_RET(qwGetTaskCtx(QW_FPARAMS(), ctx));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwAddTaskCtx(QW_FPARAMS_DEF) {
  QW_RET(qwAddTaskCtxImpl(QW_FPARAMS(), false, NULL));
}

int32_t qwAddAcquireTaskCtx(QW_FPARAMS_DEF, SQWTaskCtx **ctx) {
  return qwAddTaskCtxImpl(QW_FPARAMS(), true, ctx);
}

void qwReleaseTaskCtx(SQWorkerMgmt *mgmt, void *ctx) {
  taosHashRelease(mgmt->ctxHash, ctx);
}

void qwFreeTaskHandle(QW_FPARAMS_DEF, qTaskInfo_t *taskHandle) {  
  // Note: free/kill may in RC
  qTaskInfo_t otaskHandle = atomic_load_ptr(taskHandle);
  if (otaskHandle && atomic_val_compare_exchange_ptr(taskHandle, otaskHandle, NULL)) {
    qDestroyTask(otaskHandle);
  }
}

int32_t qwKillTaskHandle(QW_FPARAMS_DEF, SQWTaskCtx *ctx) {
  int32_t code = 0;
  // Note: free/kill may in RC
  qTaskInfo_t taskHandle = atomic_load_ptr(&ctx->taskHandle);
  if (taskHandle && atomic_val_compare_exchange_ptr(&ctx->taskHandle, taskHandle, NULL)) {
    code = qAsyncKillTask(taskHandle);
    atomic_store_ptr(&ctx->taskHandle, taskHandle);
  }

  QW_RET(code);
}


void qwFreeTask(QW_FPARAMS_DEF, SQWTaskCtx *ctx) {
  qwFreeTaskHandle(QW_FPARAMS(), &ctx->taskHandle);
  
  if (ctx->sinkHandle) {
    dsDestroyDataSinker(ctx->sinkHandle);
    ctx->sinkHandle = NULL;
  }
}


int32_t qwDropTaskCtx(QW_FPARAMS_DEF) {
  char id[sizeof(qId) + sizeof(tId)] = {0};
  QW_SET_QTID(id, qId, tId);
  SQWTaskCtx octx;

  SQWTaskCtx *ctx = taosHashGet(mgmt->ctxHash, id, sizeof(id));
  if (NULL == ctx) {
    QW_ERR_RET(TSDB_CODE_QRY_TASK_CTX_NOT_EXIST);
  }

  octx = *ctx;

  atomic_store_ptr(&ctx->taskHandle, NULL);
  atomic_store_ptr(&ctx->sinkHandle, NULL);

  QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_DROP);

  if (taosHashRemove(mgmt->ctxHash, id, sizeof(id))) {
    QW_TASK_ELOG_E("taosHashRemove from ctx hash failed");    
    QW_ERR_RET(TSDB_CODE_QRY_TASK_CTX_NOT_EXIST);
  }

  qwFreeTask(QW_FPARAMS(), &octx);

  QW_TASK_DLOG_E("task ctx dropped");
  
  return TSDB_CODE_SUCCESS;
}

int32_t qwDropTaskStatus(QW_FPARAMS_DEF) {
  SQWSchStatus *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t code = 0;
  
  char id[sizeof(qId) + sizeof(tId)] = {0};
  QW_SET_QTID(id, qId, tId);

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
    QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
  }

  QW_TASK_DLOG_E("task status dropped");

_return:

  if (task) {
    qwReleaseTaskStatus(QW_WRITE, sch);
  }
  qwReleaseScheduler(QW_WRITE, mgmt);
  
  QW_RET(code);
}

int32_t qwUpdateTaskStatus(QW_FPARAMS_DEF, int8_t status) {
  SQWSchStatus *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t code = 0;

  QW_ERR_RET(qwAcquireScheduler(mgmt, sId, QW_READ, &sch));
  QW_ERR_JRET(qwAcquireTaskStatus(QW_FPARAMS(), QW_READ, sch, &task));

  QW_ERR_JRET(qwSetTaskStatus(QW_FPARAMS(), task, status));
  
_return:

  if (task) {
    qwReleaseTaskStatus(QW_READ, sch);
  }
  qwReleaseScheduler(QW_READ, mgmt);

  QW_RET(code);
}

int32_t qwDropTask(QW_FPARAMS_DEF) {
  QW_ERR_RET(qwDropTaskStatus(QW_FPARAMS()));
  QW_ERR_RET(qwDropTaskCtx(QW_FPARAMS()));

  return TSDB_CODE_SUCCESS;
}

int32_t qwExecTask(QW_FPARAMS_DEF, SQWTaskCtx *ctx, bool *queryEnd) {
  int32_t code = 0;
  bool  qcontinue = true;
  SSDataBlock* pRes = NULL;
  uint64_t useconds = 0;
  int32_t i = 0;
  int32_t execNum = 0;
  qTaskInfo_t *taskHandle = &ctx->taskHandle; 
  DataSinkHandle sinkHandle = ctx->sinkHandle;
 
  while (true) {
    QW_TASK_DLOG("start to execTask, loopIdx:%d", i++);
    
    code = qExecTask(*taskHandle, &pRes, &useconds);
    if (code) {
      QW_TASK_ELOG("qExecTask failed, code:%x - %s", code, tstrerror(code));
      QW_ERR_RET(code);
    }

    ++execNum;

    if (NULL == pRes) {
      QW_TASK_DLOG("qExecTask end with empty res, useconds:%"PRIu64, useconds);

      dsEndPut(sinkHandle, useconds);
      
      if (TASK_TYPE_TEMP == ctx->taskType) {
        qwFreeTaskHandle(QW_FPARAMS(), taskHandle);
      }

      if (queryEnd) {
        *queryEnd = true;
      }
      
      break;
    }

    int32_t rows = pRes->info.rows;

    ASSERT(pRes->info.rows > 0);

    SInputData inputData = {.pData = pRes};
    code = dsPutDataBlock(sinkHandle, &inputData, &qcontinue);
    if (code) {
      QW_TASK_ELOG("dsPutDataBlock failed, code:%x - %s", code, tstrerror(code));
      QW_ERR_RET(code);
    }

    QW_TASK_DLOG("data put into sink, rows:%d, continueExecTask:%d", rows, qcontinue);
    
    if (!qcontinue) {
      break;
    }

    if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_READY) && execNum >= QW_DEFAULT_SHORT_RUN_TIMES) {
      break;
    }

    if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
      break;
    }

    if (atomic_load_32(&ctx->rspCode)) {
      break;
    }
  }

  QW_RET(code);
}

int32_t qwGenerateSchHbRsp(SQWorkerMgmt *mgmt, SQWSchStatus *sch, SQWHbInfo *hbInfo) {
  int32_t taskNum = 0;

  QW_LOCK(QW_READ, &sch->tasksLock);
  
  taskNum = taosHashGetSize(sch->tasksHash);

  hbInfo->rsp.taskStatus = taosArrayInit(taskNum, sizeof(STaskStatus));
  if (NULL == hbInfo->rsp.taskStatus) {
    QW_UNLOCK(QW_READ, &sch->tasksLock);
    QW_ELOG("taosArrayInit taskStatus failed, num:%d", taskNum);
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  hbInfo->connection = sch->hbConnection;
  hbInfo->rsp.seqId = -1;

  void *key = NULL;
  size_t keyLen = 0;
  int32_t i = 0;
  STaskStatus status = {0};

  void *pIter = taosHashIterate(sch->tasksHash, NULL);
  while (pIter) {
    SQWTaskStatus *taskStatus = (SQWTaskStatus *)pIter;
    key = taosHashGetKey(pIter, &keyLen);

    //TODO GET EXECUTOR API TO GET MORE INFO

    QW_GET_QTID(key, status.queryId, status.taskId);
    status.status = taskStatus->status;
    status.refId = taskStatus->refId;
    
    taosArrayPush(hbInfo->rsp.taskStatus, &status);
    
    ++i;
    pIter = taosHashIterate(sch->tasksHash, pIter);
  }  

  QW_UNLOCK(QW_READ, &sch->tasksLock);

  return TSDB_CODE_SUCCESS;
}


int32_t qwGetResFromSink(QW_FPARAMS_DEF, SQWTaskCtx *ctx, int32_t *dataLen, void **rspMsg, SOutputData *pOutput) {
  int32_t len = 0;
  SRetrieveTableRsp *rsp = NULL;
  bool queryEnd = false;
  int32_t code = 0;

  if (ctx->emptyRes) {
    QW_TASK_DLOG_E("query end with empty result");
    
    qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_SUCCEED);
    QW_ERR_RET(qwMallocFetchRsp(len, &rsp));      
    
    *rspMsg = rsp;
    *dataLen = 0;
    pOutput->queryEnd = true;
    
    return TSDB_CODE_SUCCESS;
  }

  dsGetDataLength(ctx->sinkHandle, &len, &queryEnd);

  if (len < 0) {
    QW_TASK_ELOG("invalid length from dsGetDataLength, length:%d", len);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  if (len == 0) {
    if (queryEnd) {
      code = dsGetDataBlock(ctx->sinkHandle, pOutput);
      if (code) {
        QW_TASK_ELOG("dsGetDataBlock failed, code:%x - %s", code, tstrerror(code));
        QW_ERR_RET(code);
      }
    
      QW_TASK_DLOG_E("no data in sink and query end");

      qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_SUCCEED);
      QW_ERR_RET(qwMallocFetchRsp(len, &rsp));

      *rspMsg = rsp;
      *dataLen = 0;
      return TSDB_CODE_SUCCESS;
    }

    pOutput->bufStatus = DS_BUF_EMPTY;

    return TSDB_CODE_SUCCESS;
  }

  // Got data from sink
  QW_TASK_DLOG("there are data in sink, dataLength:%d", len);

  *dataLen = len;
  
  QW_ERR_RET(qwMallocFetchRsp(len, &rsp));
  *rspMsg = rsp;
  
  pOutput->pData = rsp->data;
  code = dsGetDataBlock(ctx->sinkHandle, pOutput);
  if (code) {
    QW_TASK_ELOG("dsGetDataBlock failed, code:%x - %s", code, tstrerror(code));
    QW_ERR_RET(code);
  }

  if (DS_BUF_EMPTY == pOutput->bufStatus && pOutput->queryEnd) {
    QW_TASK_DLOG_E("task all data fetched, done");
    qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_SUCCEED);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwHandlePrePhaseEvents(QW_FPARAMS_DEF, int8_t phase, SQWPhaseInput *input, SQWPhaseOutput *output) {
  int32_t code = 0;
  SQWTaskCtx *ctx = NULL;
  void *dropConnection = NULL;
  void *cancelConnection = NULL;

  QW_TASK_DLOG("start to handle event at phase %s", qwPhaseStr(phase));

  if (QW_PHASE_PRE_QUERY == phase) {
    QW_ERR_JRET(qwAddAcquireTaskCtx(QW_FPARAMS(), &ctx));
  } else {
    QW_ERR_JRET(qwAcquireTaskCtx(QW_FPARAMS(), &ctx));
  }
  
  QW_LOCK(QW_WRITE, &ctx->lock);

  if (QW_PHASE_PRE_FETCH == phase) {
    atomic_store_8((int8_t*)&ctx->queryFetched, true);
  } else {
    atomic_store_8(&ctx->phase, phase);
  }

  if (atomic_load_8((int8_t*)&ctx->queryEnd)) {
    QW_TASK_ELOG_E("query already end");
    QW_ERR_JRET(TSDB_CODE_QW_MSG_ERROR);
  }

  switch (phase) {
    case QW_PHASE_PRE_QUERY: {
      if (QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_DROP)) {
        QW_TASK_ELOG("task already dropped at wrong phase %s", qwPhaseStr(phase));
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_STATUS_ERROR);
        break;
      }

      if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
        QW_ERR_JRET(qwDropTask(QW_FPARAMS()));

        dropConnection = ctx->dropConnection;
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_DROPPED);
        break;
      }

      QW_ERR_JRET(qwAddTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_EXECUTING));
      break;
    }
    case QW_PHASE_PRE_FETCH: {
      if (QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_DROP) || QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
        QW_TASK_WLOG("task dropping or already dropped, phase:%s", qwPhaseStr(phase));
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_DROPPED);
      }

      if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
        QW_TASK_WLOG("last fetch still not processed, phase:%s", qwPhaseStr(phase));
        QW_ERR_JRET(TSDB_CODE_QRY_DUPLICATTED_OPERATION);
      }

      if (!QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_READY)) {
        QW_TASK_ELOG("ready msg has not been processed, phase:%s", qwPhaseStr(phase));
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_MSG_ERROR);
      }
      break;
    }    
    case QW_PHASE_PRE_CQUERY: {
      if (QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_DROP)) {
        QW_TASK_WLOG("task already dropped, phase:%s", qwPhaseStr(phase));
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_DROPPED);
      }

      if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
        QW_ERR_JRET(qwDropTask(QW_FPARAMS()));

        dropConnection = ctx->dropConnection;
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_DROPPED);
      }

      break;
    }
    default:
      QW_TASK_ELOG("invalid phase %s", qwPhaseStr(phase));
      QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
  }

  if (ctx->rspCode) {
    QW_TASK_ELOG("task already failed at phase %s, error:%x - %s", qwPhaseStr(phase), ctx->rspCode, tstrerror(ctx->rspCode));
    QW_ERR_JRET(ctx->rspCode);
  }

_return:

  if (ctx) {
    QW_UPDATE_RSP_CODE(ctx, code);
    
    QW_UNLOCK(QW_WRITE, &ctx->lock);
    qwReleaseTaskCtx(mgmt, ctx);
  }

  if (dropConnection) {
    qwBuildAndSendDropRsp(dropConnection, code);
    QW_TASK_DLOG("drop msg rsped, code:%x - %s", code, tstrerror(code));
  }

  if (cancelConnection) {
    qwBuildAndSendCancelRsp(cancelConnection, code);
    QW_TASK_DLOG("cancel msg rsped, code:%x - %s", code, tstrerror(code));
  }

  QW_TASK_DLOG("end to handle event at phase %s, code:%x - %s", qwPhaseStr(phase), code, tstrerror(code));

  QW_RET(code);
}


int32_t qwHandlePostPhaseEvents(QW_FPARAMS_DEF, int8_t phase, SQWPhaseInput *input, SQWPhaseOutput *output) {
  int32_t code = 0;
  SQWTaskCtx *ctx = NULL;
  void *readyConnection = NULL;
  void *dropConnection = NULL;
  void *cancelConnection = NULL;

  QW_TASK_DLOG("start to handle event at phase %s", qwPhaseStr(phase));
  
  QW_ERR_JRET(qwAcquireTaskCtx(QW_FPARAMS(), &ctx));
  
  QW_LOCK(QW_WRITE, &ctx->lock);

  if (QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_DROP)) {
    QW_TASK_WLOG("task already dropped, phase:%s", qwPhaseStr(phase));
    QW_ERR_JRET(TSDB_CODE_QRY_TASK_DROPPED);
  }

  if (QW_PHASE_POST_QUERY == phase) {
    if (NULL == ctx->taskHandle && NULL == ctx->sinkHandle) {
      ctx->emptyRes = true;
    }
    
    if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_READY)) {
      readyConnection = ctx->readyConnection;
      QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_READY);
    }
  }

  if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
    if (QW_PHASE_POST_FETCH == phase) {
      QW_TASK_WLOG("drop received at wrong phase %s", qwPhaseStr(phase));
      QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
    }
    
    QW_ERR_JRET(qwDropTask(QW_FPARAMS()));

    dropConnection = ctx->dropConnection;
    QW_ERR_JRET(TSDB_CODE_QRY_TASK_DROPPED);
  }

  if (ctx->rspCode) {
    QW_TASK_ELOG("task already failed, phase %s, error:%x - %s", qwPhaseStr(phase), ctx->rspCode, tstrerror(ctx->rspCode));
    QW_ERR_JRET(ctx->rspCode);
  }      

  QW_ERR_JRET(input->code);

_return:

  if (TSDB_CODE_SUCCESS == code && QW_PHASE_POST_QUERY == phase) {
    qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_PARTIAL_SUCCEED);
  }

  if (ctx) {
    QW_UPDATE_RSP_CODE(ctx, code);

    if (QW_PHASE_POST_FETCH != phase) {
      atomic_store_8(&ctx->phase, phase);
    }
    
    QW_UNLOCK(QW_WRITE, &ctx->lock);
    qwReleaseTaskCtx(mgmt, ctx);
  }

  if (readyConnection) {
    qwBuildAndSendReadyRsp(readyConnection, code);
    QW_TASK_DLOG("ready msg rsped, code:%x - %s", code, tstrerror(code));
  }

  if (dropConnection) {
    qwBuildAndSendDropRsp(dropConnection, code);
    QW_TASK_DLOG("drop msg rsped, code:%x - %s", code, tstrerror(code));
  }

  if (cancelConnection) {
    qwBuildAndSendCancelRsp(cancelConnection, code);
    QW_TASK_DLOG("cancel msg rsped, code:%x - %s", code, tstrerror(code));
  }

  if (code) {
    qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_FAILED);
  }

  QW_TASK_DLOG("end to handle event at phase %s, code:%x - %s", qwPhaseStr(phase), code, tstrerror(code));

  QW_RET(code);
}


int32_t qwProcessQuery(QW_FPARAMS_DEF, SQWMsg *qwMsg, int8_t taskType) {
  int32_t code = 0;
  bool queryRsped = false;
  bool needStop = false;
  struct SSubplan *plan = NULL;
  SQWPhaseInput input = {0};
  qTaskInfo_t pTaskInfo = NULL;
  DataSinkHandle sinkHandle = NULL;
  SQWTaskCtx *ctx = NULL;

  QW_ERR_JRET(qwHandlePrePhaseEvents(QW_FPARAMS(), QW_PHASE_PRE_QUERY, &input, NULL));

  QW_ERR_JRET(qwGetTaskCtx(QW_FPARAMS(), &ctx));
  
  atomic_store_8(&ctx->taskType, taskType);
  
  code = qStringToSubplan(qwMsg->msg, &plan);
  if (TSDB_CODE_SUCCESS != code) {
    QW_TASK_ELOG("task string to subplan failed, code:%x - %s", code, tstrerror(code));
    QW_ERR_JRET(code);
  }
  
  code = qCreateExecTask(qwMsg->node, mgmt->nodeId, tId, plan, &pTaskInfo, &sinkHandle);
  if (code) {
    QW_TASK_ELOG("qCreateExecTask failed, code:%x - %s", code, tstrerror(code));
    QW_ERR_JRET(code);
  }

  if (NULL == sinkHandle || NULL == pTaskInfo) {
    QW_TASK_ELOG("create task result error, taskHandle:%p, sinkHandle:%p", pTaskInfo, sinkHandle);
    QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
  }

  QW_ERR_JRET(qwBuildAndSendQueryRsp(qwMsg->connection, code));
  QW_TASK_DLOG("query msg rsped, code:%x - %s", code, tstrerror(code));

  queryRsped = true;

  atomic_store_ptr(&ctx->taskHandle, pTaskInfo);
  atomic_store_ptr(&ctx->sinkHandle, sinkHandle);

  if (pTaskInfo && sinkHandle) {
    QW_ERR_JRET(qwExecTask(QW_FPARAMS(), ctx, NULL));
  }
  
_return:

  input.code = code;
  code = qwHandlePostPhaseEvents(QW_FPARAMS(), QW_PHASE_POST_QUERY, &input, NULL);
  
  if (!queryRsped) {
    qwBuildAndSendQueryRsp(qwMsg->connection, code);
    QW_TASK_DLOG("query msg rsped, code:%x - %s", code, tstrerror(code));
  }

  QW_RET(code);
}

int32_t qwProcessReady(QW_FPARAMS_DEF, SQWMsg *qwMsg) {
  int32_t code = 0;
  SQWTaskCtx *ctx = NULL;
  int8_t phase = 0;
  bool needRsp = true;

  QW_ERR_JRET(qwAcquireTaskCtx(QW_FPARAMS(), &ctx));

  QW_LOCK(QW_WRITE, &ctx->lock);

  if (QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_DROP) || QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
    QW_TASK_WLOG_E("task is dropping or already dropped");
    QW_ERR_JRET(TSDB_CODE_QRY_TASK_DROPPED);
  }
  
  if (ctx->phase == QW_PHASE_PRE_QUERY) {
    QW_SET_EVENT_RECEIVED(ctx, QW_EVENT_READY);
    ctx->readyConnection = qwMsg->connection;
    needRsp = false;
    QW_TASK_DLOG_E("ready msg will not rsp now");
    goto _return;
  }

  QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_READY);

  if (atomic_load_8((int8_t*)&ctx->queryEnd) || atomic_load_8((int8_t*)&ctx->queryFetched)) {
    QW_TASK_ELOG("got ready msg at wrong status, queryEnd:%d, queryFetched:%d", atomic_load_8((int8_t*)&ctx->queryEnd), atomic_load_8((int8_t*)&ctx->queryFetched));
    QW_ERR_JRET(TSDB_CODE_QW_MSG_ERROR);
  }

  if (ctx->phase == QW_PHASE_POST_QUERY) {
    code = ctx->rspCode;
    goto _return;
  }

  QW_TASK_ELOG("invalid phase when got ready msg, phase:%s", qwPhaseStr(ctx->phase));

  QW_ERR_JRET(TSDB_CODE_QRY_TASK_STATUS_ERROR);

_return:

  if (code && ctx) {
    QW_UPDATE_RSP_CODE(ctx, code);
  }

  if (code) {
    qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_FAILED);
  }

  if (ctx) {
    QW_UNLOCK(QW_WRITE, &ctx->lock);
    qwReleaseTaskCtx(mgmt, ctx);
  }

  if (needRsp) {
    qwBuildAndSendReadyRsp(qwMsg->connection, code);
    QW_TASK_DLOG("ready msg rsped, code:%x - %s", code, tstrerror(code));
  }

  QW_RET(code);
}


int32_t qwProcessCQuery(QW_FPARAMS_DEF, SQWMsg *qwMsg) {
  SQWTaskCtx *ctx = NULL;
  int32_t code = 0;
  SQWPhaseInput input = {0};
  void *rsp = NULL;
  int32_t dataLen = 0;
  bool queryEnd = false;
  
  do {
    QW_ERR_JRET(qwHandlePrePhaseEvents(QW_FPARAMS(), QW_PHASE_PRE_CQUERY, &input, NULL));

    QW_ERR_JRET(qwGetTaskCtx(QW_FPARAMS(), &ctx));

    atomic_store_8((int8_t*)&ctx->queryInQueue, 0);
    atomic_store_8((int8_t*)&ctx->queryContinue, 0);

    QW_ERR_JRET(qwExecTask(QW_FPARAMS(), ctx, &queryEnd));

    if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
      SOutputData sOutput = {0};
      QW_ERR_JRET(qwGetResFromSink(QW_FPARAMS(), ctx, &dataLen, &rsp, &sOutput));
      
      if ((!sOutput.queryEnd) && (DS_BUF_LOW == sOutput.bufStatus || DS_BUF_EMPTY == sOutput.bufStatus)) {    
        QW_TASK_DLOG("task not end and buf is %s, need to continue query", qwBufStatusStr(sOutput.bufStatus));
        
        atomic_store_8((int8_t*)&ctx->queryContinue, 1);
      }
      
      if (rsp) {
        bool qComplete = (DS_BUF_EMPTY == sOutput.bufStatus && sOutput.queryEnd);
        qwBuildFetchRsp(rsp, &sOutput, dataLen, qComplete);
        atomic_store_8((int8_t*)&ctx->queryEnd, qComplete);

        QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_FETCH);            
        
        qwBuildAndSendFetchRsp(qwMsg->connection, rsp, dataLen, code);                
        QW_TASK_DLOG("fetch msg rsped, code:%x, dataLen:%d", code, dataLen);
      } else {
        atomic_store_8((int8_t*)&ctx->queryContinue, 1);
      }
    }

_return:

    if (NULL == ctx) {
      break;
    }

    if (code && QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
      QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_FETCH);    
      qwFreeFetchRsp(rsp);
      rsp = NULL;
      qwBuildAndSendFetchRsp(qwMsg->connection, rsp, 0, code);
      QW_TASK_DLOG("fetch msg rsped, code:%x - %s", code, tstrerror(code));
    }

    QW_LOCK(QW_WRITE, &ctx->lock);
    if (queryEnd || code || 0 == atomic_load_8((int8_t*)&ctx->queryContinue)) {
      // Note: if necessary, fetch need to put cquery to queue again
      atomic_store_8(&ctx->phase, 0);
      QW_UNLOCK(QW_WRITE,&ctx->lock);
      break;
    }
    QW_UNLOCK(QW_WRITE,&ctx->lock);
  } while (true);

  input.code = code;
  QW_RET(qwHandlePostPhaseEvents(QW_FPARAMS(), QW_PHASE_POST_CQUERY, &input, NULL));
}


int32_t qwProcessFetch(QW_FPARAMS_DEF, SQWMsg *qwMsg) {
  int32_t code = 0;
  int32_t dataLen = 0;
  bool locked = false;
  SQWTaskCtx *ctx = NULL;
  void *rsp = NULL;
  SQWPhaseInput input = {0};

  QW_ERR_JRET(qwHandlePrePhaseEvents(QW_FPARAMS(), QW_PHASE_PRE_FETCH, &input, NULL));

  QW_ERR_JRET(qwGetTaskCtx(QW_FPARAMS(), &ctx));
 
  SOutputData sOutput = {0};
  QW_ERR_JRET(qwGetResFromSink(QW_FPARAMS(), ctx, &dataLen, &rsp, &sOutput));

  if (NULL == rsp) {
    QW_SET_EVENT_RECEIVED(ctx, QW_EVENT_FETCH);
  } else {
    bool qComplete = (DS_BUF_EMPTY == sOutput.bufStatus && sOutput.queryEnd);
    qwBuildFetchRsp(rsp, &sOutput, dataLen, qComplete);
    atomic_store_8((int8_t*)&ctx->queryEnd, qComplete);
  }

  if ((!sOutput.queryEnd) && (DS_BUF_LOW == sOutput.bufStatus || DS_BUF_EMPTY == sOutput.bufStatus)) {    
    QW_TASK_DLOG("task not end and buf is %s, need to continue query", qwBufStatusStr(sOutput.bufStatus));

    QW_LOCK(QW_WRITE, &ctx->lock);
    locked = true;

    // RC WARNING
    if (QW_IS_QUERY_RUNNING(ctx)) {
      atomic_store_8((int8_t*)&ctx->queryContinue, 1);
    } else if (0 == atomic_load_8((int8_t*)&ctx->queryInQueue)) {
      qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_EXECUTING);

      atomic_store_8((int8_t*)&ctx->queryInQueue, 1);
      
      QW_ERR_JRET(qwBuildAndSendCQueryMsg(QW_FPARAMS(), qwMsg->connection));
    }
  }
  
_return:

  if (locked) {
    QW_UNLOCK(QW_WRITE, &ctx->lock);
  }

  input.code = code;
  code = qwHandlePostPhaseEvents(QW_FPARAMS(), QW_PHASE_POST_FETCH, &input, NULL);

  if (code) {
    qwFreeFetchRsp(rsp);
    rsp = NULL;
    dataLen = 0;
  }

  if (code || rsp) {
    qwBuildAndSendFetchRsp(qwMsg->connection, rsp, dataLen, code);
    QW_TASK_DLOG("fetch msg rsped, code:%x, dataLen:%d", code, dataLen);
  }

  QW_RET(code);
}


int32_t qwProcessDrop(QW_FPARAMS_DEF, SQWMsg *qwMsg) {
  int32_t code = 0;
  bool needRsp = false;
  SQWTaskCtx *ctx = NULL;
  bool locked = false;

  // TODO : TASK ALREADY REMOVED AND A NEW DROP MSG RECEIVED

  QW_ERR_JRET(qwAddAcquireTaskCtx(QW_FPARAMS(), &ctx));
  
  QW_LOCK(QW_WRITE, &ctx->lock);

  locked = true;

  if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
    QW_TASK_WLOG_E("task already dropping");
    QW_ERR_JRET(TSDB_CODE_QRY_DUPLICATTED_OPERATION);
  }

  if (QW_IS_QUERY_RUNNING(ctx)) {
    QW_ERR_JRET(qwKillTaskHandle(QW_FPARAMS(), ctx));
    qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_DROPPING);
  } else if (ctx->phase > 0) {
    QW_ERR_JRET(qwDropTask(QW_FPARAMS()));
    needRsp = true;
  } else {
    // task not started
  }

  if (!needRsp) {    
    ctx->dropConnection = qwMsg->connection;
    
    QW_SET_EVENT_RECEIVED(ctx, QW_EVENT_DROP);
  }
  
_return:

  if (code) {
    if (ctx) {
      QW_UPDATE_RSP_CODE(ctx, code);
    }

    qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_FAILED);
  }

  if (locked) {
    QW_UNLOCK(QW_WRITE, &ctx->lock);
  }

  if (ctx) {
    qwReleaseTaskCtx(mgmt, ctx);
  }

  if (TSDB_CODE_SUCCESS != code || needRsp) {
    QW_ERR_RET(qwBuildAndSendDropRsp(qwMsg->connection, code));

    QW_TASK_DLOG("drop msg rsped, code:%x", code);
  }

  QW_RET(code);
}

int32_t qwProcessHb(SQWorkerMgmt *mgmt, SQWMsg *qwMsg, SSchedulerHbReq *req) {
  int32_t code = 0;
  SSchedulerHbRsp rsp = {0};
  SQWSchStatus *sch = NULL;
  uint64_t seqId = 0;

  memcpy(&rsp.epId, &req->epId, sizeof(req->epId));
  
  QW_ERR_JRET(qwAcquireAddScheduler(mgmt, req->sId, QW_READ, &sch));

  atomic_store_ptr(&sch->hbConnection, qwMsg->connection);
  ++sch->hbSeqId;

  rsp.seqId = sch->hbSeqId;

  QW_DLOG("hb connection updated, seqId:%" PRIx64 ", sId:%" PRIx64 ", nodeId:%d, fqdn:%s, port:%d, connection:%p",
    sch->hbSeqId, req->sId, req->epId.nodeId, req->epId.ep.fqdn, req->epId.ep.port, qwMsg->connection);

  qwReleaseScheduler(QW_READ, mgmt);

_return:

  qwBuildAndSendHbRsp(qwMsg->connection, &rsp, code);
  
  QW_RET(code);
}


void qwProcessHbTimerEvent(void *param, void *tmrId) {
  return;
  
  SQWorkerMgmt *mgmt = (SQWorkerMgmt *)param;
  SQWSchStatus *sch = NULL;
  int32_t taskNum = 0;
  SQWHbInfo *rspList = NULL;
  int32_t code = 0;

  QW_LOCK(QW_READ, &mgmt->schLock);

  int32_t schNum = taosHashGetSize(mgmt->schHash);
  if (schNum <= 0) {
    QW_UNLOCK(QW_READ, &mgmt->schLock);
    taosTmrReset(qwProcessHbTimerEvent, QW_DEFAULT_HEARTBEAT_MSEC, param, mgmt->timer, &mgmt->hbTimer);
    return;
  }

  rspList = calloc(schNum, sizeof(SQWHbInfo));
  if (NULL == rspList) {
    QW_UNLOCK(QW_READ, &mgmt->schLock);
    QW_ELOG("calloc %d SQWHbInfo failed", schNum);
    taosTmrReset(qwProcessHbTimerEvent, QW_DEFAULT_HEARTBEAT_MSEC, param, mgmt->timer, &mgmt->hbTimer);
    return;
  }

  void *key = NULL;
  size_t keyLen = 0;
  int32_t i = 0;

  void *pIter = taosHashIterate(mgmt->schHash, NULL);
  while (pIter) {
    code = qwGenerateSchHbRsp(mgmt, (SQWSchStatus *)pIter, &rspList[i]);
    if (code) {
      taosHashCancelIterate(mgmt->schHash, pIter);
      QW_ERR_JRET(code);
    }

    ++i;
    pIter = taosHashIterate(mgmt->schHash, pIter);
  }

_return:

  QW_UNLOCK(QW_READ, &mgmt->schLock);

  for (int32_t j = 0; j < i; ++j) {
    QW_DLOG("hb on connection %p, taskNum:%d", rspList[j].connection, (rspList[j].rsp.taskStatus ? (int32_t)taosArrayGetSize(rspList[j].rsp.taskStatus) : 0));
    qwBuildAndSendHbRsp(rspList[j].connection, &rspList[j].rsp, code);
    tFreeSSchedulerHbRsp(&rspList[j].rsp);
  }

  tfree(rspList);

  taosTmrReset(qwProcessHbTimerEvent, QW_DEFAULT_HEARTBEAT_MSEC, param, mgmt->timer, &mgmt->hbTimer);  
}

int32_t qWorkerInit(int8_t nodeType, int32_t nodeId, SQWorkerCfg *cfg, void **qWorkerMgmt, const SMsgCb *pMsgCb) {
  if (NULL == qWorkerMgmt || pMsgCb->pWrapper == NULL) {
    qError("invalid param to init qworker");
    QW_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t code = 0;
  SQWorkerMgmt *mgmt = calloc(1, sizeof(SQWorkerMgmt));
  if (NULL == mgmt) {
    qError("calloc %d failed", (int32_t)sizeof(SQWorkerMgmt));
    QW_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  if (cfg) {
    mgmt->cfg = *cfg;
    if (0 == mgmt->cfg.maxSchedulerNum) {
      mgmt->cfg.maxSchedulerNum = QW_DEFAULT_SCHEDULER_NUMBER;
    }
    if (0 == mgmt->cfg.maxTaskNum) {
      mgmt->cfg.maxTaskNum = QW_DEFAULT_TASK_NUMBER;
    }
    if (0 == mgmt->cfg.maxSchTaskNum) {
      mgmt->cfg.maxSchTaskNum = QW_DEFAULT_SCH_TASK_NUMBER;
    }
  } else {
    mgmt->cfg.maxSchedulerNum = QW_DEFAULT_SCHEDULER_NUMBER;
    mgmt->cfg.maxTaskNum = QW_DEFAULT_TASK_NUMBER;
    mgmt->cfg.maxSchTaskNum = QW_DEFAULT_SCH_TASK_NUMBER;
  }

  mgmt->schHash = taosHashInit(mgmt->cfg.maxSchedulerNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == mgmt->schHash) {
    tfree(mgmt);
    qError("init %d scheduler hash failed", mgmt->cfg.maxSchedulerNum);
    QW_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  mgmt->ctxHash = taosHashInit(mgmt->cfg.maxTaskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == mgmt->ctxHash) {
    qError("init %d task ctx hash failed", mgmt->cfg.maxTaskNum);
    QW_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  mgmt->timer = taosTmrInit(0, 0, 0, "qworker");
  if (NULL == mgmt->timer) {
    qError("init timer failed, error:%s", tstrerror(terrno));
    QW_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  mgmt->hbTimer = taosTmrStart(qwProcessHbTimerEvent, QW_DEFAULT_HEARTBEAT_MSEC, mgmt, mgmt->timer);
  if (NULL == mgmt->hbTimer) {
    qError("start hb timer failed");
    QW_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  mgmt->nodeType = nodeType;
  mgmt->nodeId = nodeId;
  mgmt->msgCb = *pMsgCb;

  *qWorkerMgmt = mgmt;

  qDebug("qworker initialized for node, type:%d, id:%d, handle:%p", mgmt->nodeType, mgmt->nodeId, mgmt);

  return TSDB_CODE_SUCCESS;

_return:

  taosHashCleanup(mgmt->schHash);
  taosHashCleanup(mgmt->ctxHash);

  taosTmrCleanUp(mgmt->timer);
  
  tfree(mgmt);

  QW_RET(code);
}

void qWorkerDestroy(void **qWorkerMgmt) {
  if (NULL == qWorkerMgmt || NULL == *qWorkerMgmt) {
    return;
  }

  SQWorkerMgmt *mgmt = *qWorkerMgmt;

  taosTmrStopA(&mgmt->hbTimer);
  taosTmrCleanUp(mgmt->timer);
  
  //TODO STOP ALL QUERY

  //TODO FREE ALL

  tfree(*qWorkerMgmt);
}

int32_t qwGetSchTasksStatus(SQWorkerMgmt *mgmt, uint64_t sId, SSchedulerStatusRsp **rsp) {
/*
  SQWSchStatus *sch = NULL;
  int32_t taskNum = 0;

  QW_ERR_RET(qwAcquireScheduler(mgmt, sId, QW_READ, &sch));
  
  sch->lastAccessTs = taosGetTimestampSec();

  QW_LOCK(QW_READ, &sch->tasksLock);
  
  taskNum = taosHashGetSize(sch->tasksHash);
  
  int32_t size = sizeof(SSchedulerStatusRsp) + sizeof((*rsp)->status[0]) * taskNum;
  *rsp = calloc(1, size);
  if (NULL == *rsp) {
    QW_SCH_ELOG("calloc %d failed", size);
    QW_UNLOCK(QW_READ, &sch->tasksLock);
    qwReleaseScheduler(QW_READ, mgmt);
    
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  void *key = NULL;
  size_t keyLen = 0;
  int32_t i = 0;

  void *pIter = taosHashIterate(sch->tasksHash, NULL);
  while (pIter) {
    SQWTaskStatus *taskStatus = (SQWTaskStatus *)pIter;
    taosHashGetKey(pIter, &key, &keyLen);

    QW_GET_QTID(key, (*rsp)->status[i].queryId, (*rsp)->status[i].taskId);
    (*rsp)->status[i].status = taskStatus->status;
    
    ++i;
    pIter = taosHashIterate(sch->tasksHash, pIter);
  }  

  QW_UNLOCK(QW_READ, &sch->tasksLock);
  qwReleaseScheduler(QW_READ, mgmt);

  (*rsp)->num = taskNum;
*/
  return TSDB_CODE_SUCCESS;
}



int32_t qwUpdateSchLastAccess(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId) {
  SQWSchStatus *sch = NULL;

/*
  QW_ERR_RET(qwAcquireScheduler(QW_READ, mgmt, sId, &sch));

  sch->lastAccessTs = taosGetTimestampSec();

  qwReleaseScheduler(QW_READ, mgmt);
*/
  return TSDB_CODE_SUCCESS;
}


int32_t qwGetTaskStatus(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, int8_t *taskStatus) {
  SQWSchStatus *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t code = 0;

/*  
  if (qwAcquireScheduler(QW_READ, mgmt, sId, &sch)) {
    *taskStatus = JOB_TASK_STATUS_NULL;
    return TSDB_CODE_SUCCESS;
  }

  if (qwAcquireTask(mgmt, QW_READ, sch, queryId, taskId, &task)) {
    qwReleaseScheduler(QW_READ, mgmt);
    
    *taskStatus = JOB_TASK_STATUS_NULL;
    return TSDB_CODE_SUCCESS;
  }

  *taskStatus = task->status;

  qwReleaseTask(QW_READ, sch);
  qwReleaseScheduler(QW_READ, mgmt);
*/

  QW_RET(code);
}


int32_t qwCancelTask(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId) {
  SQWSchStatus *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t code = 0;

/*
  QW_ERR_RET(qwAcquireAddScheduler(QW_READ, mgmt, sId, &sch));

  QW_ERR_JRET(qwAcquireAddTask(mgmt, QW_READ, sch, qId, tId, JOB_TASK_STATUS_NOT_START, &task));


  QW_LOCK(QW_WRITE, &task->lock);

  task->cancel = true;
  
  int8_t oriStatus = task->status;
  int8_t newStatus = 0;
  
  if (task->status == JOB_TASK_STATUS_CANCELLED || task->status == JOB_TASK_STATUS_NOT_START || task->status == JOB_TASK_STATUS_CANCELLING || task->status == JOB_TASK_STATUS_DROPPING) {
    QW_UNLOCK(QW_WRITE, &task->lock);

    qwReleaseTask(QW_READ, sch);
    qwReleaseScheduler(QW_READ, mgmt);
    
    return TSDB_CODE_SUCCESS;
  } else if (task->status == JOB_TASK_STATUS_FAILED || task->status == JOB_TASK_STATUS_SUCCEED || task->status == JOB_TASK_STATUS_PARTIAL_SUCCEED) {
    QW_ERR_JRET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_CANCELLED));
  } else {
    QW_ERR_JRET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_CANCELLING));
  }

  QW_UNLOCK(QW_WRITE, &task->lock);
  
  qwReleaseTask(QW_READ, sch);
  qwReleaseScheduler(QW_READ, mgmt);

  if (oriStatus == JOB_TASK_STATUS_EXECUTING) {
    //TODO call executer to cancel subquery async
  }
  
  return TSDB_CODE_SUCCESS;

_return:

  if (task) {
    QW_UNLOCK(QW_WRITE, &task->lock);
    
    qwReleaseTask(QW_READ, sch);
  }

  if (sch) {
    qwReleaseScheduler(QW_READ, mgmt);
  }
*/

  QW_RET(code);
}


