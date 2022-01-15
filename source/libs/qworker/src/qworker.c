#include "qworker.h"
#include <common.h>
#include "executor.h"
#include "planner.h"
#include "query.h"
#include "qworkerInt.h"
#include "tmsg.h"
#include "tname.h"
#include "dataSinkMgt.h"

int32_t qwValidateStatus(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, int8_t oriStatus, int8_t newStatus) {
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

int32_t qwSetTaskStatus(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, SQWTaskStatus *task, int8_t status) {
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


int32_t qwAddSchedulerImpl(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, int32_t rwType, SQWSchStatus **sch) {
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

int32_t qwAcquireSchedulerImpl(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, int32_t rwType, SQWSchStatus **sch, int32_t nOpt) {
  while (true) {
    QW_LOCK(rwType, &mgmt->schLock);
    *sch = taosHashGet(mgmt->schHash, &sId, sizeof(sId));
    if (NULL == (*sch)) {
      QW_UNLOCK(rwType, &mgmt->schLock);
      
      if (QW_NOT_EXIST_ADD == nOpt) {
        QW_ERR_RET(qwAddSchedulerImpl(rwType, mgmt, sId, sch));

        nOpt = QW_NOT_EXIST_RET_ERR;
        
        continue;
      } else if (QW_NOT_EXIST_RET_ERR == nOpt) {
        QW_RET(TSDB_CODE_QRY_SCH_NOT_EXIST);
      } else {
        assert(0);
      }
    }

    break;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwAcquireAddScheduler(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, int32_t rwType, SQWSchStatus **sch) {
  return qwAcquireSchedulerImpl(rwType, mgmt, sId, sch, QW_NOT_EXIST_ADD);
}

int32_t qwAcquireScheduler(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, int32_t rwType, SQWSchStatus **sch) {
  return qwAcquireSchedulerImpl(rwType, mgmt, sId, sch, QW_NOT_EXIST_RET_ERR);
}

void qwReleaseScheduler(int32_t rwType, SQWorkerMgmt *mgmt) {
  QW_UNLOCK(rwType, &mgmt->schLock);
}

int32_t qwAddTaskStatusImpl(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, SQWSchStatus *sch, int32_t rwType, int32_t status, SQWTaskStatus **task) {
  int32_t code = 0;

  char id[sizeof(qId) + sizeof(tId)] = {0};
  QW_SET_QTID(id, qId, tId);

  SQWTaskStatus ntask = {0};
  ntask.status = status;

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

int32_t qwAddTaskStatus(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, int32_t status) {
  SQWSchStatus *tsch = NULL;
  int32_t code = 0;
  QW_ERR_RET(qwAcquireAddScheduler(QW_FPARAMS(), QW_READ, &tsch));

  QW_ERR_JRET(qwAddTaskStatusImpl(QW_FPARAMS(), tsch, 0, status, NULL));

_return:

  qwReleaseScheduler(QW_READ, mgmt);
  
  QW_RET(code);
}


int32_t qwAcquireTaskStatus(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, int32_t rwType, SQWSchStatus *sch, SQWTaskStatus **task) {
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


int32_t qwAddAcquireTaskStatus(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, int32_t rwType, SQWSchStatus *sch, int32_t status, SQWTaskStatus **task) {
  return qwAddTaskStatusImpl(QW_FPARAMS(), sch, rwType, status, task);
}


void qwReleaseTaskStatus(int32_t rwType, SQWSchStatus *sch) {
  QW_UNLOCK(rwType, &sch->tasksLock);
}

int32_t qwAddTaskCtxImpl(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, int32_t rwType, int32_t status, SQWTaskCtx **ctx) {
  char id[sizeof(qId) + sizeof(tId)] = {0};
  QW_SET_QTID(id, qId, tId);

  SQWTaskCtx ctx = {0};

  QW_LOCK(QW_WRITE, &mgmt->ctxLock);
  int32_t code = taosHashPut(mgmt->ctxHash, id, sizeof(id), &ctx, sizeof(SQWTaskCtx));
  if (0 != code) {
    QW_UNLOCK(QW_WRITE, &mgmt->ctxLock);
    
    if (HASH_NODE_EXIST(code)) {
      if (rwType && ctx) {
        QW_RET(qwAcquireTaskCtx(QW_FPARAMS(), rwType, ctx));
      } else {
        QW_TASK_ELOG("task ctx already exist, id:%s", id);
        QW_ERR_RET(TSDB_CODE_QRY_TASK_ALREADY_EXIST);
      }
    } else {
      QW_TASK_ELOG("taosHashPut to ctxHash failed, code:%x", code);
      QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
  }
  QW_UNLOCK(QW_WRITE, &mgmt->ctxLock);

  if (rwType && ctx) {
    QW_RET(qwAcquireTaskCtx(QW_FPARAMS(), rwType, ctx));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwAddTaskCtx(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId) {
  QW_RET(qwAddTaskCtxImpl(QW_FPARAMS(), 0, 0, NULL));
}

int32_t qwGetTaskCtx(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, SQWTaskCtx **ctx) {
  char id[sizeof(qId) + sizeof(tId)] = {0};
  QW_SET_QTID(id, qId, tId);
  
  *ctx = taosHashGet(mgmt->ctxHash, id, sizeof(id));
  if (NULL == (*ctx)) {
    QW_TASK_ELOG("ctx not in ctxHash, id:%s", id);
    QW_ERR_RET(TSDB_CODE_QRY_RES_CACHE_NOT_EXIST);
  }

  return TSDB_CODE_SUCCESS;

}

int32_t qwAcquireTaskCtx(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, int32_t rwType, SQWTaskCtx **ctx) {
  char id[sizeof(qId) + sizeof(tId)] = {0};
  QW_SET_QTID(id, qId, tId);
  
  QW_LOCK(rwType, &mgmt->ctxLock);
  *ctx = taosHashGet(mgmt->ctxHash, id, sizeof(id));
  if (NULL == (*ctx)) {
    QW_UNLOCK(rwType, &mgmt->ctxLock);
    QW_TASK_ELOG("ctx not in ctxHash, id:%s", id);
    QW_ERR_RET(TSDB_CODE_QRY_RES_CACHE_NOT_EXIST);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwAddAcquireTaskCtx(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, int32_t rwType, SQWTaskCtx **ctx) {
  return qwAddTaskCtxImpl(QW_FPARAMS(), rwType, 0, ctx);
}

void qwReleaseTaskCtx(int32_t rwType, SQWorkerMgmt *mgmt) {
  QW_UNLOCK(rwType, &mgmt->ctxLock);
}


void qwFreeTask(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, SQWTaskCtx *ctx) {
  if (ctx->taskHandle) {
    qDestroyTask(ctx->taskHandle);
    ctx->taskHandle = NULL;
  }

  // TODO
  if (ctx->sinkHandle) {

  }
}


// Note: NEED CTX HASH LOCKED BEFORE ENTRANCE
int32_t qwDropTaskCtx(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId) {
  char id[sizeof(qId) + sizeof(tId)] = {0};
  QW_SET_QTID(id, qId, tId);
  SQWTaskCtx octx;

  SQWTaskCtx *ctx = taosHashGet(mgmt->ctxHash, id, sizeof(id));
  if (NULL == ctx) {
    QW_ERR_RET(TSDB_CODE_QRY_RES_CACHE_NOT_EXIST);
  }

  octx = *ctx;

  if (taosHashRemove(mgmt->ctxHash, id, sizeof(id))) {
    QW_TASK_ELOG("taosHashRemove from ctx hash failed, id:%s", id);    
    QW_ERR_RET(TSDB_CODE_QRY_RES_CACHE_NOT_EXIST);
  }

  if (octx.taskHandle) {
    qDestroyTask(octx.taskHandle);
  }

  if (octx.sinkHandle) {
    dsDestroyDataSinker(octx.sinkHandle);
  }
  
  return TSDB_CODE_SUCCESS;
}


int32_t qwDropTaskStatus(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId) {
  SQWSchStatus *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t code = 0;
  
  char id[sizeof(qId) + sizeof(tId)] = {0};
  QW_SET_QTID(id, qId, tId);

  if (qwAcquireScheduler(QW_FPARAMS(), QW_WRITE, &sch)) {
    QW_TASK_WLOG("scheduler does not exist, id:%s", id);
    return TSDB_CODE_SUCCESS;
  }

  if (qwAcquireTaskStatus(QW_FPARAMS(), QW_WRITE, sch, &task)) {
    qwReleaseScheduler(QW_WRITE, mgmt);
    
    QW_TASK_WLOG("task does not exist, id:%s", id);
    return TSDB_CODE_SUCCESS;
  }

  if (taosHashRemove(sch->tasksHash, id, sizeof(id))) {
    QW_TASK_ELOG("taosHashRemove task from hash failed, task:%p", task);
    QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
  }

  QW_TASK_DLOG("task dropped, id:%d", id);

_return:

  qwReleaseTaskStatus(QW_WRITE, sch);
  qwReleaseScheduler(QW_WRITE, mgmt);
  
  QW_RET(code);
}

int32_t qwUpdateTaskCtxHandles(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, qTaskInfo_t taskHandle, DataSinkHandle sinkHandle) {
  SQWTaskCtx *ctx = NULL;
  
  QW_ERR_RET(qwAcquireTaskCtx(QW_FPARAMS(), QW_READ, &ctx));

  ctx->taskHandle = taskHandle;
  ctx->sinkHandle = sinkHandle;

  qwReleaseTaskCtx(QW_READ, mgmt);

  return TSDB_CODE_SUCCESS;
}


int32_t qwUpdateTaskStatus(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, int8_t status) {
  SQWSchStatus *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t code = 0;

  QW_ERR_RET(qwAcquireScheduler(QW_READ, mgmt, sId, &sch));
  QW_ERR_JRET(qwAcquireTaskStatus(mgmt, QW_READ, sch, qId, tId, &task));

  QW_ERR_JRET(qwSetTaskStatus(QW_FPARAMS(), task, status));
  
_return:

  qwReleaseTaskStatus(QW_READ, sch);
  qwReleaseScheduler(QW_READ, mgmt);

  QW_RET(code);
}


int32_t qwDropTask(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, bool *needRsp) {
  int32_t code = 0;
  SQWTaskCtx *ctx = NULL;
  bool locked = false;

  QW_ERR_JRET(qwAddAcquireTaskCtx(QW_FPARAMS(), QW_READ, &ctx));
  
  QW_LOCK(QW_WRITE, &ctx->lock);

  locked = true;

  if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
    QW_TASK_WLOG("task already dropping", NULL);
    QW_ERR_JRET(TSDB_CODE_QRY_DUPLICATTED_OPERATION);
  }

  if (QW_IN_EXECUTOR(ctx)) {
    if (ctx->taskHandle) {
      QW_ERR_JRET(qKillTask(ctx->taskHandle));
    }
    
    QW_ERR_JRET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_DROPPING));
  } else if (ctx->phase > 0) {
    QW_ERR_JRET(qwDropTaskStatus(QW_FPARAMS()));              
    QW_ERR_JRET(qwDropTaskCtx(QW_FPARAMS()));

    locked = false;
    *needRsp = true;
  }

  if (locked) {
    QW_SET_EVENT_RECEIVED(ctx, QW_EVENT_DROP);
  }

_return:

  if (locked) {
    QW_UNLOCK(QW_WRITE, &ctx->lock);
  }

  if (ctx) {
    qwReleaseTaskCtx(QW_READ, mgmt);
  }

  QW_RET(code);
}



int32_t qwGetResFromSink(QW_FPARAMS_DEF, SQWTaskCtx *ctx, int32_t *dataLen, void **rspMsg, SOutputData *pOutput) {
  int32_t len = 0;
  SRetrieveTableRsp *rsp = NULL;
  bool queryEnd = false;
  int32_t code = 0;

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
    
      QW_TASK_DLOG("no data in sink and query end", NULL);
      
      QW_ERR_RET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_SUCCEED));

      QW_ERR_RET(qwMallocFetchRsp(len, &rsp));      

      *rspMsg = rsp;

      *dataLen = 0;
      
      return TSDB_CODE_SUCCESS;
    }
    
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
    qwFreeFetchRsp(rsp);
    QW_ERR_RET(code);
  }

  queryEnd = pOutput->queryEnd;
  pOutput->queryEnd = false;

  if (DS_BUF_EMPTY == pOutput->bufStatus && queryEnd) {
    pOutput->queryEnd = true;
    
    QW_SCH_TASK_DLOG("task all fetched, status:%d", JOB_TASK_STATUS_SUCCEED);
    QW_ERR_RET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_SUCCEED));
  }

  return TSDB_CODE_SUCCESS;
}


int32_t qwHandleTaskEvent(QW_FPARAMS_DEF, int32_t phase, SQWPhaseInput *input, SQWPhaseOutput *output) {
  int32_t code = 0;
  int8_t status = 0;
  SQWTaskCtx *ctx = NULL;
  bool locked = false;

  switch (phase) {
    case QW_PHASE_PRE_QUERY: {
      QW_ERR_JRET(qwAddAcquireTaskCtx(QW_FPARAMS(), QW_READ, &ctx));
            
      ctx->phase = phase;

      assert(!QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_CANCEL));

      if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
        output->needStop = true;

        QW_ERR_JRET(qwDropTaskStatus(QW_FPARAMS()));              
        QW_ERR_JRET(qwDropTaskCtx(QW_FPARAMS()));

        output->rspCode = TSDB_CODE_QRY_TASK_DROPPED;
        
        // Note: ctx freed, no need to unlock it
        locked = false;        
      } else if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_CANCEL)) {
        output->needStop = true;
        
        QW_ERR_JRET(qwAddTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_CANCELLED));
        qwFreeTask(QW_FPARAMS(), ctx);
        
        QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_CANCEL);
        
        output->rspCode = TSDB_CODE_QRY_TASK_CANCELLED;
      }

      if (!output->needStop) {
        QW_ERR_JRET(qwAddTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_EXECUTING));
      }
      break;
    }
    case QW_PHASE_POST_QUERY: {
      QW_ERR_JRET(qwAddAcquireTaskCtx(QW_FPARAMS(), QW_READ, &ctx));
      
      QW_LOCK(QW_WRITE, &ctx->lock);
      
      locked = true;        

      assert(!QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_CANCEL));

      if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
        output->needStop = true;

        QW_ERR_JRET(qwDropTaskStatus(QW_FPARAMS()));              
        QW_ERR_JRET(qwDropTaskCtx(QW_FPARAMS()));

        output->rspCode = TSDB_CODE_QRY_TASK_DROPPED;
        
        // Note: ctx freed, no need to unlock it
        locked = false;                
      } else if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_CANCEL)) {
        output->needStop = true;
        
        QW_ERR_JRET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_CANCELLED));
        qwFreeTask(QW_FPARAMS(), ctx);
        
        QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_CANCEL);
        
        output->rspCode = TSDB_CODE_QRY_TASK_CANCELLED;
      } else if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_READY)) {
        output->needRsp = true;

        QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_READY);

        output->rspCode = input.code;
      }

      if (!output->needStop) {      
        QW_ERR_JRET(qwUpdateTaskStatus(QW_FPARAMS(), input.status));
      }
      break;
    }
    case QW_PHASE_PRE_FETCH: {
      QW_ERR_JRET(qwAddAcquireTaskCtx(QW_FPARAMS(), QW_READ, &ctx));
      
      QW_LOCK(QW_WRITE, &ctx->lock);
      
      locked = true;        

      ctx->phase = phase;

      if (QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_CANCEL)) {
        QW_TASK_WLOG("task already cancelled", NULL);
        output->needStop = true;
        output->rspCode = TSDB_CODE_QRY_TASK_CANCELLED;        
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_CANCELLED);
      }

      if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
        QW_TASK_WLOG("task is dropping", NULL);
        output->needStop = true;
        output->rspCode = TSDB_CODE_QRY_TASK_DROPPING;
      } else if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_CANCEL)) {
        QW_TASK_WLOG("task is cancelling", NULL);
        output->needStop = true;
        output->rspCode = TSDB_CODE_QRY_TASK_CANCELLING;
      }

      if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
        QW_TASK_WLOG("last fetch not finished", NULL);
        output->needStop = true;
        output->rspCode = TSDB_CODE_QRY_DUPLICATTED_OPERATION;        
        QW_ERR_JRET(TSDB_CODE_QRY_DUPLICATTED_OPERATION);
      }

      if (!QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_READY)) {
        QW_TASK_ELOG("query rsp are not ready", NULL);
        output->needStop = true;
        output->rspCode = TSDB_CODE_QRY_TASK_MSG_ERROR;        
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_MSG_ERROR);
      }
      break;
    }    
    case QW_PHASE_POST_FETCH: {
      QW_ERR_JRET(qwAddAcquireTaskCtx(QW_FPARAMS(), QW_READ, &ctx));
      
      QW_LOCK(QW_WRITE, &ctx->lock);
      
      locked = true;        

      if (QW_IS_EVENT_PROCESSED(ctx, QW_EVENT_CANCEL)) {
        QW_TASK_WLOG("task already cancelled", NULL);
        output->needStop = true;
        output->rspCode = TSDB_CODE_QRY_TASK_CANCELLED;        
        QW_ERR_JRET(TSDB_CODE_QRY_TASK_CANCELLED);
      }

      if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_DROP)) {
        QW_TASK_WLOG("task is dropping", NULL);
        output->needStop = true;
        output->rspCode = TSDB_CODE_QRY_TASK_DROPPING;
      } else if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_CANCEL)) {
        QW_TASK_WLOG("task is cancelling", NULL);
        output->needStop = true;
        output->rspCode = TSDB_CODE_QRY_TASK_CANCELLING;
      }
      break;
    }    

  }


_return:

  if (locked) {
    ctx->phase = phase;
    
    QW_UNLOCK(QW_WRITE, &ctx->lock);
  }

  if (ctx) {
    qwReleaseTaskCtx(QW_READ, mgmt);
  }

  QW_RET(code);
}


int32_t qwProcessQuery(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, SQWMsg *qwMsg) {
  int32_t code = 0;
  bool queryRsped = false;
  bool needStop = false;
  struct SSubplan *plan = NULL;
  int32_t rspCode = 0;
  SQWPhaseInput input = {0};
  SQWPhaseOutput output = {0};

  QW_ERR_JRET(qwHandleTaskEvent(QW_FPARAMS(), QW_PHASE_PRE_QUERY, &input, &output));

  needStop = output.needStop;
  code = output.rspCode;
  
  if (needStop) {
    QW_TASK_DLOG("task need stop", NULL);
    QW_ERR_JRET(code);
  }
  
  code = qStringToSubplan(qwMsg->msg, &plan);
  if (TSDB_CODE_SUCCESS != code) {
    QW_TASK_ELOG("task string to subplan failed, code:%x", code);
    QW_ERR_JRET(code);
  }

  qTaskInfo_t pTaskInfo = NULL;
  code = qCreateExecTask(node, 0, (struct SSubplan *)plan, &pTaskInfo);
  if (code) {
    QW_TASK_ELOG("qCreateExecTask failed, code:%x", code);
    QW_ERR_JRET(code);
  }
  
  QW_ERR_JRET(qwBuildAndSendQueryRsp(pMsg, TSDB_CODE_SUCCESS));

  queryRsped = true;

  DataSinkHandle sinkHandle = NULL;
  code = qExecTask(pTaskInfo, &sinkHandle);
  if (code) {
    QW_TASK_ELOG("qExecTask failed, code:%x", code);
    QW_ERR_JRET(code);
  }

  QW_ERR_JRET(qwUpdateTaskCtxHandles(QW_FPARAMS(), pTaskInfo, sinkHandle));

_return:

  if (code) {
    rspCode = code;
  }
  
  if (!queryRsped) {
    code = qwBuildAndSendQueryRsp(qwMsg->connection, rspCode);
    if (TSDB_CODE_SUCCESS == rspCode && code) {
      rspCode = code;
    }
  }

  if (needStop) {
    QW_RET(rspCode);
  }

  input.code = rspCode;
  
  if (TSDB_CODE_SUCCESS != rspCode) {
    input.status = JOB_TASK_STATUS_FAILED;
  } else {
    input.status = JOB_TASK_STATUS_PARTIAL_SUCCEED;
  }
  
  QW_ERR_RET(qwHandleTaskEvent(QW_FPARAMS(), QW_PHASE_POST_QUERY, &input, &output));

  if (queryRsped && output.needRsp) {
    qwBuildAndSendReadyRsp(qwMsg->connection, output.rspCode);
  }
  
  QW_RET(rspCode);
}

int32_t qwProcessCQuery(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, SQWMsg *qwMsg) {
  int32_t code = 0;
  bool queryRsped = false;
  bool needStop = false;
  struct SSubplan *plan = NULL;
  int32_t rspCode = 0;
  SQWPhaseInput input = {0};
  SQWPhaseOutput output = {0};
  SQWTaskCtx *ctx = NULL;
  void *rsp = NULL;
  int32_t dataLen = 0;

  QW_ERR_JRET(qwHandleTaskEvent(QW_FPARAMS(), QW_PHASE_PRE_CQUERY, &input, &output));

  needStop = output.needStop;
  code = output.rspCode;
  
  if (needStop) {
    QW_TASK_DLOG("task need stop", NULL);
    QW_ERR_JRET(code);
  }

  QW_ERR_JRET(qwGetTaskCtx(QW_FPARAMS(), &ctx));
  
  qTaskInfo_t     taskHandle = ctx->taskHandle;
  DataSinkHandle  sinkHandle = ctx->sinkHandle;

  code = qExecTask(taskHandle, &sinkHandle);
  if (code) {
    QW_TASK_ELOG("qExecTask failed, code:%x", code);
    QW_ERR_JRET(code);
  }

  if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
    QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_CQUERY);
    
    SOutputData sOutput = {0};
    QW_ERR_JRET(qwGetResFromSink(QW_FPARAMS(), ctx, &dataLen, &rsp, &sOutput));
    
    if (NULL == rsp) {
      QW_SET_EVENT_RECEIVED(ctx, QW_EVENT_FETCH);
    }
    
    // Note: schedule data sink firstly and will schedule query after it's done
    if (sOutput.scheduleJobNo) {
      if (sOutput.scheduleJobNo > ctx.sinkId) {
        QW_TASK_DLOG("sink need schedule, scheduleJobNo:%d", sOutput.scheduleJobNo);    
    
        ctx.sinkId = sOutput.scheduleJobNo;
        QW_ERR_JRET(qwBuildAndSendSchSinkMsg(QW_FPARAMS(), qwMsg->connection));
      }
    } else if ((!sOutput.queryEnd) && (DS_BUF_LOW == sOutput.bufStatus || DS_BUF_EMPTY == sOutput.bufStatus)) {    
      QW_TASK_DLOG("task not end, need to continue, bufStatus:%d", sOutput.bufStatus);
    
      if (!QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_CQUERY)) {
        QW_SET_EVENT_RECEIVED(ctx, QW_EVENT_CQUERY);
    
        QW_ERR_JRET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_EXECUTING));      
        
        QW_ERR_RET(qwBuildAndSendCQueryMsg(QW_FPARAMS(), qwMsg->connection));
      }
    }
    
    if (rsp) {
      qwBuildFetchRsp(rsp, &sOutput, dataLen);
    }

  }

_return:

  qwHandleTaskEvent(QW_FPARAMS(), QW_PHASE_POST_CQUERY, &input, &output);

  if (QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_FETCH)) {
    QW_SET_EVENT_PROCESSED(ctx, QW_EVENT_FETCH);
    
    if (code) {
      qwFreeFetchRsp(rsp);
      rsp = NULL;
      qwBuildAndSendFetchRsp(qwMsg->connection, rsp, 0, code);
    } else if (rsp) {
      qwBuildAndSendFetchRsp(qwMsg->connection, rsp, dataLen, code);
    }
  }
  
  QW_RET(rspCode);
}


int32_t qwProcessFetch(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, SQWMsg *qwMsg) {
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

  QW_ERR_JRET(qwHandleTaskEvent(QW_FPARAMS(), QW_PHASE_PRE_FETCH, &input, &output));
  
  needStop = output.needStop;
  code = output.rspCode;
  
  if (needStop) {
    QW_TASK_DLOG("task need stop", NULL);
    QW_ERR_JRET(code);
  }

  QW_ERR_JRET(qwGetTaskCtx(QW_FPARAMS(), &ctx));
 
  SOutputData sOutput = {0};
  QW_ERR_JRET(qwGetResFromSink(QW_FPARAMS(), ctx, &dataLen, &rsp, &sOutput));

  if (NULL == rsp) {
    QW_SET_EVENT_RECEIVED(ctx, QW_EVENT_FETCH);
  }

  // Note: schedule data sink firstly and will schedule query after it's done
  if (sOutput.scheduleJobNo) {
    if (sOutput.scheduleJobNo > ctx.sinkId) {
      QW_TASK_DLOG("sink need schedule, scheduleJobNo:%d", sOutput.scheduleJobNo);    

      ctx.sinkId = sOutput.scheduleJobNo;
      QW_ERR_JRET(qwBuildAndSendSchSinkMsg(QW_FPARAMS(), qwMsg->connection));
    }
  } else if ((!sOutput.queryEnd) && (DS_BUF_LOW == sOutput.bufStatus || DS_BUF_EMPTY == sOutput.bufStatus)) {    
    QW_TASK_DLOG("task not end, need to continue, bufStatus:%d", sOutput.bufStatus);

    if (!QW_IS_EVENT_RECEIVED(ctx, QW_EVENT_CQUERY)) {
      QW_SET_EVENT_RECEIVED(ctx, QW_EVENT_CQUERY);

      QW_ERR_JRET(qwUpdateTaskStatus(QW_FPARAMS(), JOB_TASK_STATUS_EXECUTING));      
      
      QW_ERR_RET(qwBuildAndSendCQueryMsg(QW_FPARAMS(), qwMsg->connection));
    }
  }

  if (rsp) {
    qwBuildFetchRsp(rsp, &sOutput, dataLen);
  }
  
_return:

  qwHandleTaskEvent(QW_FPARAMS(), QW_PHASE_POST_FETCH, &input, &output);

  if (code) {
    qwFreeFetchRsp(rsp);
    rsp = NULL;
    qwBuildAndSendFetchRsp(qwMsg->connection, rsp, 0, code);
  } else if (rsp) {
    qwBuildAndSendFetchRsp(qwMsg->connection, rsp, dataLen, code);
  }

  
  QW_RET(code);
}


int32_t qwProcessDrop(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, SQWMsg *qwMsg) {
  int32_t code = 0;
  bool needRsp = false;
  
  QW_ERR_JRET(qwDropTask(QW_FPARAMS(), &needRsp));

_return:

  if (TSDB_CODE_SUCCESS != code || needRsp) {
    QW_ERR_RET(qwBuildAndSendDropRsp(qwMsg->connection, code));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerInit(int8_t nodeType, int32_t nodeId, SQWorkerCfg *cfg, void **qWorkerMgmt, void *nodeObj, putReqToQueryQFp fp) {
  if (NULL == qWorkerMgmt || NULL == nodeObj || NULL == fp) {
    qError("invalid param to init qworker");
    QW_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }
  
  SQWorkerMgmt *mgmt = calloc(1, sizeof(SQWorkerMgmt));
  if (NULL == mgmt) {
    qError("calloc %d failed", (int32_t)sizeof(SQWorkerMgmt));
    QW_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  if (cfg) {
    mgmt->cfg = *cfg;
    if (0 == mgmt->cfg.maxSchedulerNum) {
      mgmt->cfg.maxSchedulerNum = QWORKER_DEFAULT_SCHEDULER_NUMBER;
    }
    if (0 == mgmt->cfg.maxTaskNum) {
      mgmt->cfg.maxTaskNum = QWORKER_DEFAULT_TASK_NUMBER;
    }
    if (0 == mgmt->cfg.maxSchTaskNum) {
      mgmt->cfg.maxSchTaskNum = QWORKER_DEFAULT_SCH_TASK_NUMBER;
    }
  } else {
    mgmt->cfg.maxSchedulerNum = QWORKER_DEFAULT_SCHEDULER_NUMBER;
    mgmt->cfg.maxTaskNum = QWORKER_DEFAULT_TASK_NUMBER;
    mgmt->cfg.maxSchTaskNum = QWORKER_DEFAULT_SCH_TASK_NUMBER;
  }

  mgmt->schHash = taosHashInit(mgmt->cfg.maxSchedulerNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == mgmt->schHash) {
    tfree(mgmt);
    qError("init %d scheduler hash failed", mgmt->cfg.maxSchedulerNum);
    QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  mgmt->ctxHash = taosHashInit(mgmt->cfg.maxTaskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == mgmt->ctxHash) {
    taosHashCleanup(mgmt->schHash);
    mgmt->schHash = NULL;
    tfree(mgmt);
    qError("init %d task ctx hash failed", mgmt->cfg.maxTaskNum);
    QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  mgmt->nodeType = nodeType;
  mgmt->nodeId = nodeId;
  mgmt->nodeObj = nodeObj;
  mgmt->putToQueueFp = fp;

  *qWorkerMgmt = mgmt;

  qDebug("qworker initialized for node, type:%d, id:%d, handle:%p", mgmt->nodeType, mgmt->nodeId, mgmt);

  return TSDB_CODE_SUCCESS;
}

void qWorkerDestroy(void **qWorkerMgmt) {
  if (NULL == qWorkerMgmt || NULL == *qWorkerMgmt) {
    return;
  }

  SQWorkerMgmt *mgmt = *qWorkerMgmt;
  
  //TODO STOP ALL QUERY

  //TODO FREE ALL

  tfree(*qWorkerMgmt);
}


#if 0
#endif















int32_t qwGetSchTasksStatus(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, SSchedulerStatusRsp **rsp) {
  SQWSchStatus *sch = NULL;
  int32_t taskNum = 0;

  QW_ERR_RET(qwAcquireScheduler(QW_READ, mgmt, sId, &sch));
  
  sch->lastAccessTs = taosGetTimestampSec();

  QW_LOCK(QW_READ, &sch->tasksLock);
  
  taskNum = taosHashGetSize(sch->tasksHash);
  
  int32_t size = sizeof(SSchedulerStatusRsp) + sizeof((*rsp)->status[0]) * taskNum;
  *rsp = calloc(1, size);
  if (NULL == *rsp) {
    qError("calloc %d failed", size);
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
    
    pIter = taosHashIterate(sch->tasksHash, pIter);
  }  

  QW_UNLOCK(QW_READ, &sch->tasksLock);
  qwReleaseScheduler(QW_READ, mgmt);

  (*rsp)->num = taskNum;

  return TSDB_CODE_SUCCESS;
}



int32_t qwUpdateSchLastAccess(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId) {
  SQWSchStatus *sch = NULL;

  QW_ERR_RET(qwAcquireScheduler(QW_READ, mgmt, sId, &sch));

  sch->lastAccessTs = taosGetTimestampSec();

  qwReleaseScheduler(QW_READ, mgmt);

  return TSDB_CODE_SUCCESS;
}


int32_t qwGetTaskStatus(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, int8_t *taskStatus) {
  SQWSchStatus *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t code = 0;
  
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

  QW_RET(code);
}


int32_t qwCancelTask(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId) {
  SQWSchStatus *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t code = 0;

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

  QW_RET(code);
}


