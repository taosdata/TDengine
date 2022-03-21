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

SQWDebug gQWDebug = {0};

int32_t qwValidateStatus(QW_FPARAMS_DEF, int8_t oriStatus, int8_t newStatus) {
  int32_t code = 0;

  if (oriStatus == newStatus) {
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
       && newStatus != JOB_TASK_STATUS_CANCELLED) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }
      
      break;
    case JOB_TASK_STATUS_SUCCEED:
      if (newStatus != JOB_TASK_STATUS_CANCELLED
       && newStatus != JOB_TASK_STATUS_DROPPING) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }

      break;
    case JOB_TASK_STATUS_FAILED:
    case JOB_TASK_STATUS_CANCELLING:
      if (newStatus != JOB_TASK_STATUS_CANCELLED) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }
      
      break;
    case JOB_TASK_STATUS_CANCELLED:
    case JOB_TASK_STATUS_DROPPING:
      QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      break;
      
    default:
      QW_TASK_ELOG("invalid task status:%d", oriStatus);
      return TSDB_CODE_QRY_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;

_return:

  QW_TASK_ELOG("invalid task status update from %d to %d", oriStatus, newStatus);
  QW_RET(code);
}

int32_t qwSetTaskStatus(QW_FPARAMS_DEF, SQWTaskStatus *task, int8_t status) {
  int32_t code = 0;
  int8_t origStatus = 0;

  while (true) {
    origStatus = atomic_load_8(&task->status);
    
    QW_ERR_RET(qwValidateStatus(QW_FPARAMS(), origStatus, status));
    
    if (origStatus != atomic_val_compare_exchange_8(&task->status, origStatus, status)) {
      continue;
    }
    
    QW_TASK_DLOG("task status updated from %d to %d", origStatus, status);

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
        QW_TASK_ELOG("task status already exist, id:%s", id);
        QW_ERR_RET(TSDB_CODE_QRY_TASK_ALREADY_EXIST);
      }
    } else {
      QW_TASK_ELOG("taosHashPut to tasksHash failed, code:%x", code);
      QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
  }
  QW_UNLOCK(QW_WRITE, &sch->tasksLock);

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
  
  //QW_LOCK(rwType, &mgmt->ctxLock);
  *ctx = taosHashAcquire(mgmt->ctxHash, id, sizeof(id));
  if (NULL == (*ctx)) {
    //QW_UNLOCK(rwType, &mgmt->ctxLock);
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

int32_t qwAddTaskCtxImpl(QW_FPARAMS_DEF, bool acquire, int32_t status, SQWTaskCtx **ctx) {
  char id[sizeof(qId) + sizeof(tId)] = {0};
  QW_SET_QTID(id, qId, tId);

  SQWTaskCtx nctx = {0};

  //QW_LOCK(QW_WRITE, &mgmt->ctxLock);
  int32_t code = taosHashPut(mgmt->ctxHash, id, sizeof(id), &nctx, sizeof(SQWTaskCtx));
  if (0 != code) {
    //QW_UNLOCK(QW_WRITE, &mgmt->ctxLock);
    
    if (HASH_NODE_EXIST(code)) {
      if (acquire && ctx) {
        QW_RET(qwAcquireTaskCtx(QW_FPARAMS(), ctx));
      } else if (ctx) {
        QW_RET(qwGetTaskCtx(QW_FPARAMS(), ctx));
      } else {
        QW_TASK_ELOG("task ctx already exist, id:%s", id);
        QW_ERR_RET(TSDB_CODE_QRY_TASK_ALREADY_EXIST);
      }
    } else {
      QW_TASK_ELOG("taosHashPut to ctxHash failed, code:%x", code);
      QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
  }
  //QW_UNLOCK(QW_WRITE, &mgmt->ctxLock);

  if (acquire && ctx) {
    QW_RET(qwAcquireTaskCtx(QW_FPARAMS(), ctx));
  } else if (ctx) {
    QW_RET(qwGetTaskCtx(QW_FPARAMS(), ctx));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwAddTaskCtx(QW_FPARAMS_DEF) {
  QW_RET(qwAddTaskCtxImpl(QW_FPARAMS(), false, 0, NULL));
}



int32_t qwAddAcquireTaskCtx(QW_FPARAMS_DEF, SQWTaskCtx **ctx) {
  return qwAddTaskCtxImpl(QW_FPARAMS(), true, 0, ctx);
}

int32_t qwAddGetTaskCtx(QW_FPARAMS_DEF, SQWTaskCtx **ctx) {
  return qwAddTaskCtxImpl(QW_FPARAMS(), false, 0, ctx);
}


void qwReleaseTaskCtx(SQWorkerMgmt *mgmt, void *ctx) {
  //QW_UNLOCK(rwType, &mgmt->ctxLock);
  taosHashRelease(mgmt->ctxHash, ctx);
}

void qwFreeTaskHandle(QW_FPARAMS_DEF, qTaskInfo_t *taskHandle) {  
  // RC WARNING
  qTaskInfo_t otaskHandle = atomic_load_ptr(taskHandle);
  if (otaskHandle && atomic_val_compare_exchange_ptr(taskHandle, otaskHandle, NULL)) {
    qDestroyTask(otaskHandle);
  }
}

int32_t qwKillTaskHandle(QW_FPARAMS_DEF, SQWTaskCtx *ctx) {
  int32_t code = 0;
  // RC WARNING
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


// Note: NEED CTX HASH LOCKED BEFORE ENTRANCE
int32_t qwDropTaskCtx(QW_FPARAMS_DEF, int32_t rwType) {
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

  if (rwType) {
    QW_UNLOCK(rwType, &ctx->lock);
  }

  if (taosHashRemove(mgmt->ctxHash, id, sizeof(id))) {
    QW_TASK_ELOG_E("taosHashRemove from ctx hash failed");    
    QW_ERR_RET(TSDB_CODE_QRY_TASK_CTX_NOT_EXIST);
  }

  if (octx.taskHandle) {
    qDestroyTask(octx.taskHandle);
  }

  if (octx.sinkHandle) {
    dsDestroyDataSinker(octx.sinkHandle);
  }

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

  qwReleaseTaskStatus(QW_WRITE, sch);
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

  qwReleaseTaskStatus(QW_READ, sch);
  qwReleaseScheduler(QW_READ, mgmt);

  QW_RET(code);
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
      QW_TASK_ELOG("qExecTask failed, code:%s", tstrerror(code));
      QW_ERR_JRET(code);
    }

    ++execNum;

    if (NULL == pRes) {
      QW_TASK_DLOG("task query done, useconds:%"PRIu64, useconds);
      dsEndPut(sinkHandle, useconds);
      
      if (TASK_TYPE_TEMP == ctx->taskType) {
        qwFreeTaskHandle(QW_FPARAMS(), taskHandle);
      }

      if (queryEnd) {
        *queryEnd = true;
      }
      
      break;
    }

    ASSERT(pRes->info.rows > 0);

    SInputData inputData = {.pData = pRes};
    code = dsPutDataBlock(sinkHandle, &inputData, &qcontinue);
    if (code) {
      QW_TASK_ELOG("dsPutDataBlock failed, code:%s", tstrerror(code));
      QW_ERR_JRET(code);
    }

    QW_TASK_DLOG("data put into sink, rows:%d, continueExecTask:%d", pRes->info.rows, qcontinue);
    
    if (!qcontinue) {
      break;
    }

    if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_READY) && execNum >= QW_DEFAULT_SHORT_RUN_TIMES) {
      break;
    }

    if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
      break;
    }
  }

_return:

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
    QW_TASK_DLOG("query empty result, query end, phase:%d", ctx->phase);
    
    QW_ERR_RET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_SUCCEED));
    
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
        QW_TASK_ELOG("dsGetDataBlock failed, code:%x", code);
        QW_ERR_RET(code);
      }
    
      QW_TASK_DLOG("no data in sink and query end, phase:%d", ctx->phase);
      
      QW_ERR_RET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_SUCCEED));

      QW_ERR_RET(qwMallocFetchRsp(len, &rsp));
      *rspMsg = rsp;
      *dataLen = 0;
      
      return TSDB_CODE_SUCCESS;
    }

    pOutput->bufStatus = DS_BUF_EMPTY;
    
    QW_TASK_DLOG("no res data in sink, need response later, queryEnd:%d", queryEnd);

    return TSDB_CODE_SUCCESS;
  }


  // Got data from sink

  *dataLen = len;

  QW_TASK_DLOG("task got data in sink, dataLength:%d", len);
  
  QW_ERR_RET(qwMallocFetchRsp(len, &rsp));
  *rspMsg = rsp;
  
  pOutput->pData = rsp->data;
  code = dsGetDataBlock(ctx->sinkHandle, pOutput);
  if (code) {
    QW_TASK_ELOG("dsGetDataBlock failed, code:%x", code);
    QW_ERR_RET(code);
  }

  if (DS_BUF_EMPTY == pOutput->bufStatus && pOutput->queryEnd) {
    QW_SCH_TASK_DLOG("task all fetched, status:%d", JOB_TASK_STATUS_SUCCEED);
    QW_ERR_RET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_SUCCEED));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwHandlePrePhaseEvents(QW_FPARAMS_DEF, int8_t phase, SQWPhaseInput *input, SQWPhaseOutput *output) {
  int32_t code = 0;
  int8_t status = 0;
  SQWTaskCtx *ctx = NULL;
  bool locked = false;
  void *dropConnection = NULL;
  void *cancelConnection = NULL;

  QW_SCH_TASK_DLOG("start to handle event at phase %d", phase);

  output->needStop = false;

  if (QW_PHASE_PRE_QUERY == phase) {
    QW_ERR_JRET(qwAddAcquireTaskCtx(QW_FPARAMS(), &ctx));
  } else {
    QW_ERR_JRET(qwAcquireTaskCtx(QW_FPARAMS(), &ctx));
  }
  
  QW_LOCK(QW_WRITE, &ctx->lock);
  locked = true;

  switch (phase) {
    case QW_PHASE_PRE_QUERY: {
      atomic_store_8(&ctx->phase, phase);

      if (QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_CANCEL) || QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_DROP)) {
        QW_TASK_ELOG("task already cancelled/dropped at wrong phase, phase:%d", phase);
        
        output->needStop = true;
        output->rspCode = TSDB_CODE_QRY_TASK_STATUS_ERROR;
        break;
      }

      if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
        QW_ERR_JRET(qwDropTaskStatus(QW_FPARAMS()));              
        QW_ERR_JRET(qwDropTaskCtx(QW_FPARAMS(), QW_WRITE));

        output->needStop = true;
        output->rspCode = TSDB_CODE_QRY_TASK_DROPPED;
        QW_SET_RSP_CODE(ctx, output->rspCode);
        dropConnection = ctx->dropConnection;
        
        // Note: ctx freed, no need to unlock it
        locked = false;      

        break;
      } else if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_CANCEL)) {
        QW_ERR_JRET(qwAddTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_CANCELLED));
        
        QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_CANCEL);
        output->needStop = true;                
        output->rspCode = TSDB_CODE_QRY_TASK_CANCELLED;
        QW_SET_RSP_CODE(ctx, output->rspCode);
        
        cancelConnection = ctx->cancelConnection;

        break;
      }

      if (ctx->rspCode) {
        QW_TASK_ELOG("task already failed at wrong phase, code:%x, phase:%d", ctx->rspCode, phase);
        output->needStop = true;
        output->rspCode = ctx->rspCode;        
        QW_ERR_JRET(output->rspCode);
      }

      if (!output->needStop) {
        QW_ERR_JRET(qwAddTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_EXECUTING));
      }
      break;
    }
    case QW_PHASE_PRE_FETCH: {
      if (QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_DROP)) {
        QW_TASK_WLOG("task already dropped, phase:%d", phase);
        output->needStop = true;
        output->rspCode = TSDB_CODE_QRY_TASK_DROPPED;        
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_DROPPED);
      }
      if (QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_CANCEL)) {
        QW_TASK_WLOG("task already cancelled, phase:%d", phase);
        output->needStop = true;
        output->rspCode = TSDB_CODE_QRY_TASK_CANCELLED;        
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_CANCELLED);
      }

      if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
        QW_TASK_ELOG("drop event at wrong phase, phase:%d", phase);
        output->needStop = true;
        output->rspCode = TSDB_CODE_QRY_TASK_STATUS_ERROR;        
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_CANCELLED);
      } else if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_CANCEL)) {
        QW_TASK_ELOG("cancel event at wrong phase, phase:%d", phase);
        output->needStop = true;
        output->rspCode = TSDB_CODE_QRY_TASK_STATUS_ERROR;        
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_CANCELLED);
      }

      if (ctx->rspCode) {
        QW_TASK_ELOG("task already failed, code:%x, phase:%d", ctx->rspCode, phase);
        output->needStop = true;
        output->rspCode = ctx->rspCode;        
        QW_ERR_JRET(output->rspCode);
      }

      if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
        QW_TASK_WLOG("last fetch not finished, phase:%d", phase);
        output->needStop = true;
        output->rspCode = TSDB_CODE_QRY_DUPLICATTED_OPERATION;        
        QW_ERR_JRET(TSDB_CODE_QRY_DUPLICATTED_OPERATION);
      }

      if (!QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_READY)) {
        QW_TASK_ELOG("query rsp are not ready, phase:%d", phase);
        output->needStop = true;
        output->rspCode = TSDB_CODE_QRY_TASK_MSG_ERROR;        
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_MSG_ERROR);
      }
      break;
    }    
    case QW_PHASE_PRE_CQUERY: {
      atomic_store_8(&ctx->phase, phase);

      if (QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_CANCEL)) {
        QW_TASK_WLOG("task already cancelled, phase:%d", phase);
        output->needStop = true;
        output->rspCode = TSDB_CODE_QRY_TASK_CANCELLED;        
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_CANCELLED);
      }

      if (QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_DROP)) {
        QW_TASK_WLOG("task already dropped, phase:%d", phase);
        output->needStop = true;
        output->rspCode = TSDB_CODE_QRY_TASK_DROPPED;        
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_DROPPED);
      }

      if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
        QW_ERR_JRET(qwDropTaskStatus(QW_FPARAMS()));  
        QW_ERR_JRET(qwDropTaskCtx(QW_FPARAMS(), QW_WRITE));
        
        output->rspCode = TSDB_CODE_QRY_TASK_DROPPED;
        output->needStop = true;
        QW_SET_RSP_CODE(ctx, output->rspCode);
        dropConnection = ctx->dropConnection;
        
        // Note: ctx freed, no need to unlock it
        locked = false;            
      
        QW_ERR_JRET(output->rspCode);
      } else if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_CANCEL)) {
        QW_ERR_JRET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_CANCELLED));
        qwFreeTask(QW_FPARAMS(), ctx);
        
        QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_CANCEL);
      
        output->needStop = true;        
        output->rspCode = TSDB_CODE_QRY_TASK_CANCELLED;
        QW_SET_RSP_CODE(ctx, output->rspCode);
        cancelConnection = ctx->cancelConnection;
      
        QW_ERR_JRET(output->rspCode);
      }


      if (ctx->rspCode) {
        QW_TASK_ELOG("task already failed, code:%x, phase:%d", ctx->rspCode, phase);
        output->needStop = true;
        output->rspCode = ctx->rspCode;        
        QW_ERR_JRET(output->rspCode);
      }
      break;
    }    
  }

_return:

  if (ctx) {
    if (output->rspCode) {
      QW_UPDATE_RSP_CODE(ctx, output->rspCode);
    }
    
    if (locked) {
      QW_UNLOCK(QW_WRITE, &ctx->lock);
    }

    qwReleaseTaskCtx(mgmt, ctx);
  }

  if (code) {
    output->needStop = true;
    if (TSDB_CODE_SUCCESS == output->rspCode) {
      output->rspCode = code;
    }
  }

  if (dropConnection) {
    qwBuildAndSendDropRsp(dropConnection, output->rspCode);    
    QW_TASK_DLOG("drop msg rsped, code:%x", output->rspCode);
  }

  if (cancelConnection) {
    qwBuildAndSendCancelRsp(cancelConnection, output->rspCode);    
    QW_TASK_DLOG("cancel msg rsped, code:%x", output->rspCode);
  }

  QW_SCH_TASK_DLOG("end to handle event at phase %d", phase);

  QW_RET(code);
}


int32_t qwHandlePostPhaseEvents(QW_FPARAMS_DEF, int8_t phase, SQWPhaseInput *input, SQWPhaseOutput *output) {
  int32_t code = 0;
  int8_t status = 0;
  SQWTaskCtx *ctx = NULL;
  bool locked = false;
  void *readyConnection = NULL;
  void *dropConnection = NULL;
  void *cancelConnection = NULL;

  QW_SCH_TASK_DLOG("start to handle event at phase %d", phase);

  output->needStop = false;
  
  QW_ERR_JRET(qwAcquireTaskCtx(QW_FPARAMS(), &ctx));
  
  QW_LOCK(QW_WRITE, &ctx->lock);
  locked = true;     

  if (QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_DROP)) {
    QW_TASK_WLOG("task already dropped, phase:%d", phase);
    output->needStop = true;
    output->rspCode = TSDB_CODE_QRY_TASK_DROPPED;        
    QW_ERR_JRET(TSDB_CODE_QRY_TASK_DROPPED);
  }
  
  if (QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_CANCEL)) {
    QW_TASK_WLOG("task already cancelled, phase:%d", phase);
    output->needStop = true;
    output->rspCode = TSDB_CODE_QRY_TASK_CANCELLED;        
    QW_ERR_JRET(TSDB_CODE_QRY_TASK_CANCELLED);
  }

  if (input->code) {
    output->rspCode = input->code;
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
    QW_ERR_JRET(qwDropTaskStatus(QW_FPARAMS()));  
    QW_ERR_JRET(qwDropTaskCtx(QW_FPARAMS(), QW_WRITE));
    
    output->rspCode = TSDB_CODE_QRY_TASK_DROPPED;
    output->needStop = true;
    QW_SET_RSP_CODE(ctx, output->rspCode);
    dropConnection = ctx->dropConnection;
    
    // Note: ctx freed, no need to unlock it
    locked = false;            

    QW_ERR_JRET(output->rspCode);
  } else if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_CANCEL)) {
    QW_ERR_JRET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_CANCELLED));
    qwFreeTask(QW_FPARAMS(), ctx);
    
    QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_CANCEL);

    output->needStop = true;        
    output->rspCode = TSDB_CODE_QRY_TASK_CANCELLED;
    QW_SET_RSP_CODE(ctx, output->rspCode);
    cancelConnection = ctx->cancelConnection;

    QW_ERR_JRET(output->rspCode);
  }

  if (ctx->rspCode) {
    QW_TASK_ELOG("task failed, code:%x, phase:%d", ctx->rspCode, phase);
    output->needStop = true;
    output->rspCode = ctx->rspCode;        
    QW_ERR_JRET(output->rspCode);
  }      

  if (QW_PHASE_POST_QUERY == phase && (!output->needStop)) {      
    QW_ERR_JRET(qwUpdateTaskStatus(QW_FPARAMS(), input->taskStatus));
  }

_return:

  if (ctx) {
    if (output->rspCode) {
      QW_UPDATE_RSP_CODE(ctx, output->rspCode);
    }

    if (QW_PHASE_POST_FETCH != phase) {
      atomic_store_8(&ctx->phase, phase);
    }
    
    if (locked) {
      QW_UNLOCK(QW_WRITE, &ctx->lock);
    }
    
    qwReleaseTaskCtx(mgmt, ctx);
  }

  if (code) {
    output->needStop = true;
    if (TSDB_CODE_SUCCESS == output->rspCode) {
      output->rspCode = code;
    }
  }

  if (readyConnection) {
    qwBuildAndSendReadyRsp(readyConnection, output->rspCode);    
    QW_TASK_DLOG("ready msg rsped, code:%x", output->rspCode);
  }

  if (dropConnection) {
    qwBuildAndSendDropRsp(dropConnection, output->rspCode);    
    QW_TASK_DLOG("drop msg rsped, code:%x", output->rspCode);
  }

  if (cancelConnection) {
    qwBuildAndSendCancelRsp(cancelConnection, output->rspCode);    
    QW_TASK_DLOG("cancel msg rsped, code:%x", output->rspCode);
  }

  QW_SCH_TASK_DLOG("end to handle event at phase %d", phase);

  QW_RET(code);
}


int32_t qwProcessQuery(QW_FPARAMS_DEF, SQWMsg *qwMsg, int8_t taskType) {
  int32_t code = 0;
  bool queryRsped = false;
  bool needStop = false;
  struct SSubplan *plan = NULL;
  int32_t rspCode = 0;
  SQWPhaseInput input = {0};
  SQWPhaseOutput output = {0};
  qTaskInfo_t pTaskInfo = NULL;
  DataSinkHandle sinkHandle = NULL;
  SQWTaskCtx *ctx = NULL;

  QW_ERR_JRET(qwHandlePrePhaseEvents(QW_FPARAMS(), QW_PHASE_PRE_QUERY, &input, &output));

  needStop = output.needStop;
  code = output.rspCode;
  
  if (needStop) {
    QW_TASK_DLOG("task need stop, phase:%d", QW_PHASE_PRE_QUERY);
    QW_ERR_JRET(code);
  }

  QW_ERR_JRET(qwGetTaskCtx(QW_FPARAMS(), &ctx));
  
  atomic_store_8(&ctx->taskType, taskType);
  
  code = qStringToSubplan(qwMsg->msg, &plan);
  if (TSDB_CODE_SUCCESS != code) {
    QW_TASK_ELOG("task string to subplan failed, code:%s", tstrerror(code));
    QW_ERR_JRET(code);
  }
  
  code = qCreateExecTask(qwMsg->node, 0, tId, (struct SSubplan *)plan, &pTaskInfo, &sinkHandle);
  if (code) {
    QW_TASK_ELOG("qCreateExecTask failed, code:%s", tstrerror(code));
    QW_ERR_JRET(code);
  }

  if (NULL == sinkHandle || NULL == pTaskInfo) {
    QW_TASK_ELOG("create task result error, taskHandle:%p, sinkHandle:%p", pTaskInfo, sinkHandle);
    QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
  }

  //TODO OPTIMIZE EMTYP RESULT QUERY RSP TO AVOID FURTHER FETCH
  
  QW_ERR_JRET(qwBuildAndSendQueryRsp(qwMsg->connection, code));
  QW_TASK_DLOG("query msg rsped, code:%d", code);

  queryRsped = true;

  atomic_store_ptr(&ctx->taskHandle, pTaskInfo);
  atomic_store_ptr(&ctx->sinkHandle, sinkHandle);

  if (pTaskInfo && sinkHandle) {
    QW_ERR_JRET(qwExecTask(QW_FPARAMS(), ctx, NULL));
  }
  
_return:

  if (code) {
    rspCode = code;
  }
  
  if (!queryRsped) {
    qwBuildAndSendQueryRsp(qwMsg->connection, rspCode);
    QW_TASK_DLOG("query msg rsped, code:%x", rspCode);
  }

  input.code = rspCode;
  input.taskStatus = rspCode ? JOB_TASK_STATUS_FAILED : JOB_TASK_STATUS_PARTIAL_SUCCEED;
  
  QW_ERR_RET(qwHandlePostPhaseEvents(QW_FPARAMS(), QW_PHASE_POST_QUERY, &input, &output));
  
  QW_RET(rspCode);
}

int32_t qwProcessReady(QW_FPARAMS_DEF, SQWMsg *qwMsg) {
  int32_t code = 0;
  SQWTaskCtx *ctx = NULL;
  int8_t phase = 0;
  bool needRsp = false;
  int32_t rspCode = 0;

  QW_ERR_JRET(qwAcquireTaskCtx(QW_FPARAMS(), &ctx));

  QW_LOCK(QW_WRITE, &ctx->lock);

  if (QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_CANCEL) || QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_DROP) ||
      QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_CANCEL) || QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
    QW_TASK_WLOG("task already cancelled/dropped, phase:%d", phase);
    QW_ERR_JRET(TSDB_CODE_QRY_TASK_CANCELLED);
  }

  phase = QW_GET_PHASE(ctx);
  
  if (phase == QW_PHASE_PRE_QUERY) {
    QW_SET_EVENT_RECEIVED(ctx, QW_EVENT_READY);
    ctx->readyConnection = qwMsg->connection;
    QW_TASK_DLOG("ready msg not rsped, phase:%d", phase);
  } else if (phase == QW_PHASE_POST_QUERY) {
    QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_READY);
    needRsp = true;
    rspCode = ctx->rspCode;
  } else {
    QW_TASK_ELOG("invalid phase when got ready msg, phase:%d", phase);
    QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_READY);
    needRsp = true;    
    rspCode = TSDB_CODE_QRY_TASK_STATUS_ERROR;
    QW_ERR_JRET(TSDB_CODE_QRY_TASK_STATUS_ERROR);
  }

_return:

  if (code && ctx) {
    QW_UPDATE_RSP_CODE(ctx, code);
  }

  if (ctx) {
    QW_UNLOCK(QW_WRITE, &ctx->lock);
    qwReleaseTaskCtx(mgmt, ctx);
  }

  if (needRsp) {
    qwBuildAndSendReadyRsp(qwMsg->connection, rspCode);
    QW_TASK_DLOG("ready msg rsped, code:%x", rspCode);
  }

  QW_RET(code);
}


int32_t qwProcessCQuery(QW_FPARAMS_DEF, SQWMsg *qwMsg) {
  SQWTaskCtx *ctx = NULL;
  int32_t code = 0;
  bool queryRsped = false;
  bool needStop = false;
  struct SSubplan *plan = NULL;
  SQWPhaseInput input = {0};
  SQWPhaseOutput output = {0};
  void *rsp = NULL;
  int32_t dataLen = 0;
  bool queryEnd = false;
  
  do {
    QW_ERR_JRET(qwHandlePrePhaseEvents(QW_FPARAMS(), QW_PHASE_PRE_CQUERY, &input, &output));

    needStop = output.needStop;
    code = output.rspCode;
    
    if (needStop) {
      QW_TASK_DLOG("task need stop, phase:%d", QW_PHASE_PRE_CQUERY);
      QW_ERR_JRET(code);
    }

    QW_ERR_JRET(qwGetTaskCtx(QW_FPARAMS(), &ctx));

    atomic_store_8(&ctx->queryInQueue, 0);
    atomic_store_8(&ctx->queryContinue, 0);

    QW_ERR_JRET(qwExecTask(QW_FPARAMS(), ctx, &queryEnd));

    if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
      SOutputData sOutput = {0};
      QW_ERR_JRET(qwGetResFromSink(QW_FPARAMS(), ctx, &dataLen, &rsp, &sOutput));
      
      if ((!sOutput.queryEnd) && (DS_BUF_LOW == sOutput.bufStatus || DS_BUF_EMPTY == sOutput.bufStatus)) {    
        QW_TASK_DLOG("task not end, need to continue, bufStatus:%d", sOutput.bufStatus);
        
        // RC WARNING
        atomic_store_8(&ctx->queryContinue, 1);
      }
      
      if (rsp) {
        bool qComplete = (DS_BUF_EMPTY == sOutput.bufStatus && sOutput.queryEnd);
        qwBuildFetchRsp(rsp, &sOutput, dataLen, qComplete);
        
        QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_FETCH);            
        
        qwBuildAndSendFetchRsp(qwMsg->connection, rsp, dataLen, code);                
        QW_TASK_DLOG("fetch msg rsped, code:%x, dataLen:%d", code, dataLen);
      } else {
        atomic_store_8(&ctx->queryContinue, 1);
      }
    }

    if (queryEnd) {
      needStop = true;
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
      QW_TASK_DLOG("fetch msg rsped, code:%x, dataLen:%d", code, 0);      
    }

    QW_LOCK(QW_WRITE, &ctx->lock);
    if (needStop || code || 0 == atomic_load_8(&ctx->queryContinue)) {
      atomic_store_8(&ctx->phase, 0);
      QW_UNLOCK(QW_WRITE,&ctx->lock);
      break;
    }
    
    QW_UNLOCK(QW_WRITE,&ctx->lock);
  } while (true);

  input.code = code;
  qwHandlePostPhaseEvents(QW_FPARAMS(), QW_PHASE_POST_CQUERY, &input, &output);    

  QW_RET(code);
}


int32_t qwProcessFetch(QW_FPARAMS_DEF, SQWMsg *qwMsg) {
  int32_t code = 0;
  int32_t needRsp = true;
  void *data = NULL;
  int32_t sinkStatus = 0;
  int32_t dataLen = 0;
  bool queryEnd = false;
  bool needStop = false;
  bool locked = false;
  SQWTaskCtx *ctx = NULL;
  int8_t status = 0;
  void *rsp = NULL;

  SQWPhaseInput input = {0};
  SQWPhaseOutput output = {0};

  QW_ERR_JRET(qwHandlePrePhaseEvents(QW_FPARAMS(), QW_PHASE_PRE_FETCH, &input, &output));
  
  needStop = output.needStop;
  code = output.rspCode;
  
  if (needStop) {
    QW_TASK_DLOG("task need stop, phase:%d", QW_PHASE_PRE_FETCH);
    QW_ERR_JRET(code);
  }

  QW_ERR_JRET(qwGetTaskCtx(QW_FPARAMS(), &ctx));
 
  SOutputData sOutput = {0};
  QW_ERR_JRET(qwGetResFromSink(QW_FPARAMS(), ctx, &dataLen, &rsp, &sOutput));

  if (NULL == rsp) {
    QW_SET_EVENT_RECEIVED(ctx, QW_EVENT_FETCH);
  } else {
    bool qComplete = (DS_BUF_EMPTY == sOutput.bufStatus && sOutput.queryEnd);
    qwBuildFetchRsp(rsp, &sOutput, dataLen, qComplete);
  }

  if ((!sOutput.queryEnd) && (DS_BUF_LOW == sOutput.bufStatus || DS_BUF_EMPTY == sOutput.bufStatus)) {    
    QW_TASK_DLOG("task not end, need to continue, bufStatus:%d", sOutput.bufStatus);

    QW_LOCK(QW_WRITE, &ctx->lock);
    locked = true;

    // RC WARNING
    if (QW_IS_QUERY_RUNNING(ctx)) {
      atomic_store_8(&ctx->queryContinue, 1);
    } else if (0 == atomic_load_8(&ctx->queryInQueue)) {
      if (!ctx->multiExec) {
        QW_ERR_JRET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_EXECUTING));      
        ctx->multiExec = true;
      }

      atomic_store_8(&ctx->queryInQueue, 1);
      
      QW_ERR_JRET(qwBuildAndSendCQueryMsg(QW_FPARAMS(), qwMsg->connection));

      QW_TASK_DLOG("schedule query in queue, phase:%d", ctx->phase);
    }
  }
  
_return:

  if (locked) {
    QW_UNLOCK(QW_WRITE, &ctx->lock);
  }

  input.code = code;

  qwHandlePostPhaseEvents(QW_FPARAMS(), QW_PHASE_POST_FETCH, &input, &output);

  if (output.rspCode) {
    code = output.rspCode;
  }

  if (code) {
    qwFreeFetchRsp(rsp);
    rsp = NULL;
    dataLen = 0;
    qwBuildAndSendFetchRsp(qwMsg->connection, rsp, dataLen, code);
    QW_TASK_DLOG("fetch msg rsped, code:%x, dataLen:%d", code, dataLen);
  } else if (rsp) {
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

  QW_ERR_JRET(qwAddAcquireTaskCtx(QW_FPARAMS(), &ctx));
  
  QW_LOCK(QW_WRITE, &ctx->lock);

  locked = true;

  if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
    QW_TASK_WLOG("task already dropping, phase:%d", ctx->phase);
    QW_ERR_JRET(TSDB_CODE_QRY_DUPLICATTED_OPERATION);
  }

  if (QW_IS_QUERY_RUNNING(ctx)) {
    QW_ERR_JRET(qwKillTaskHandle(QW_FPARAMS(), ctx));
    
    QW_ERR_JRET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_DROPPING));
  } else if (ctx->phase > 0) {
    QW_ERR_JRET(qwDropTaskStatus(QW_FPARAMS()));
    QW_ERR_JRET(qwDropTaskCtx(QW_FPARAMS(), QW_WRITE));

    QW_SET_RSP_CODE(ctx, TSDB_CODE_QRY_TASK_DROPPED);

    locked = false;
    needRsp = true;
  }

  if (!needRsp) {    
    ctx->dropConnection = qwMsg->connection;
    
    QW_SET_EVENT_RECEIVED(ctx, QW_EVENT_DROP);
  }
  
_return:

  if (code) {
    QW_UPDATE_RSP_CODE(ctx, code);
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


