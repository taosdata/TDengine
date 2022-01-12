#include "qworker.h"
#include <common.h>
#include "executor.h"
#include "planner.h"
#include "query.h"
#include "qworkerInt.h"
#include "tmsg.h"
#include "tname.h"
#include "dataSinkMgt.h"

int32_t qwValidateStatus(SQWorkerMgmt *mgmt, int8_t oriStatus, int8_t newStatus, uint64_t sId, uint64_t qId, uint64_t tId) {
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

int32_t qwUpdateTaskInfo(SQWorkerMgmt *mgmt, SQWTaskStatus *task, int8_t type, void *data, uint64_t sId, uint64_t qId, uint64_t tId) {
  int32_t code = 0;
  int8_t origStatus = 0;
  
  switch (type) {
    case QW_TASK_INFO_STATUS: {
      int8_t newStatus = *(int8_t *)data;
      QW_ERR_RET(qwValidateStatus(mgmt, task->status, newStatus, QW_IDS()));
      
      origStatus = task->status;
      task->status = newStatus;
      
      QW_TASK_DLOG("task status updated from %d to %d", origStatus, newStatus);
      break;
    }
    default:
      QW_TASK_ELOG("unknown task info, type:%d", type);
      return TSDB_CODE_QRY_APP_ERROR;
  }
  
  return TSDB_CODE_SUCCESS;
}

int32_t qwAddTaskHandlesToCache(SQWorkerMgmt *mgmt, uint64_t qId, uint64_t tId, qTaskInfo_t taskHandle, DataSinkHandle sinkHandle) {
  char id[sizeof(qId) + sizeof(tId)] = {0};
  QW_SET_QTID(id, qId, tId);

  SQWTaskCtx resCache = {0};
  resCache.taskHandle = taskHandle;
  resCache.sinkHandle = sinkHandle;

  QW_LOCK(QW_WRITE, &mgmt->ctxLock);
  if (0 != taosHashPut(mgmt->ctxHash, id, sizeof(id), &resCache, sizeof(SQWTaskCtx))) {
    QW_UNLOCK(QW_WRITE, &mgmt->ctxLock);
    qError("taosHashPut queryId[%"PRIx64"] taskId[%"PRIx64"] to resHash failed", qId, tId);
    return TSDB_CODE_QRY_APP_ERROR;
  }

  QW_UNLOCK(QW_WRITE, &mgmt->ctxLock);

  return TSDB_CODE_SUCCESS;
}

static int32_t qwAddScheduler(int32_t rwType, SQWorkerMgmt *mgmt, uint64_t sId, SQWSchStatus **sch) {
  SQWSchStatus newSch = {0};
  newSch.tasksHash = taosHashInit(mgmt->cfg.maxSchTaskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (NULL == newSch.tasksHash) {
    QW_SCH_DLOG("taosHashInit %d failed", mgmt->cfg.maxSchTaskNum);
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  while (true) {
    QW_LOCK(QW_WRITE, &mgmt->schLock);
    int32_t code = taosHashPut(mgmt->schHash, &sId, sizeof(sId), &newSch, sizeof(newSch));
    if (0 != code) {
      if (!HASH_NODE_EXIST(code)) {
        QW_UNLOCK(QW_WRITE, &mgmt->schLock);
        QW_SCH_ELOG("taosHashPut new sch to scheduleHash failed, errno:%d", errno);
        taosHashCleanup(newSch.tasksHash);
        return TSDB_CODE_QRY_APP_ERROR;
      }
    }
    
    QW_UNLOCK(QW_WRITE, &mgmt->schLock);
    if (TSDB_CODE_SUCCESS == qwAcquireScheduler(rwType, mgmt, sId, sch)) {
      if (code) {
        taosHashCleanup(newSch.tasksHash);
      }
      
      return TSDB_CODE_SUCCESS;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t qwAcquireSchedulerImpl(int32_t rwType, SQWorkerMgmt *mgmt, uint64_t sId, SQWSchStatus **sch, int32_t nOpt) {
  QW_LOCK(rwType, &mgmt->schLock);
  *sch = taosHashGet(mgmt->schHash, &sId, sizeof(sId));
  if (NULL == (*sch)) {
    QW_UNLOCK(rwType, &mgmt->schLock);
    
    if (QW_NOT_EXIST_ADD == nOpt) {
      return qwAddScheduler(rwType, mgmt, sId, sch);
    } else if (QW_NOT_EXIST_RET_ERR == nOpt) {
      return TSDB_CODE_QRY_SCH_NOT_EXIST;
    } else {
      assert(0);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t qwAddAcquireScheduler(int32_t rwType, SQWorkerMgmt *mgmt, uint64_t sId, SQWSchStatus **sch) {
  return qwAcquireSchedulerImpl(rwType, mgmt, sId, sch, QW_NOT_EXIST_ADD);
}

static int32_t qwAcquireScheduler(int32_t rwType, SQWorkerMgmt *mgmt, uint64_t sId, SQWSchStatus **sch) {
  return qwAcquireSchedulerImpl(rwType, mgmt, sId, sch, QW_NOT_EXIST_RET_ERR);
}

static FORCE_INLINE void qwReleaseScheduler(int32_t rwType, SQWorkerMgmt *mgmt) {
  QW_UNLOCK(rwType, &mgmt->schLock);
}

static int32_t qwAcquireTaskImpl(int32_t rwType, SQWSchStatus *sch, uint64_t qId, uint64_t tId, SQWTaskStatus **task) {
  char id[sizeof(qId) + sizeof(tId)] = {0};
  QW_SET_QTID(id, qId, tId);

  QW_LOCK(rwType, &sch->tasksLock);
  *task = taosHashGet(sch->tasksHash, id, sizeof(id));
  if (NULL == (*task)) {
    QW_UNLOCK(rwType, &sch->tasksLock);

    return TSDB_CODE_QRY_TASK_NOT_EXIST;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t qwAcquireTask(int32_t rwType, SQWSchStatus *sch, uint64_t qId, uint64_t tId, SQWTaskStatus **task) {
  return qwAcquireTaskImpl(rwType, sch, qId, tId, task);
}

static FORCE_INLINE void qwReleaseTask(int32_t rwType, SQWSchStatus *sch) {
  QW_UNLOCK(rwType, &sch->tasksLock);
}

int32_t qwAddTaskToSch(int32_t rwType, SQWSchStatus *sch, uint64_t qId, uint64_t tId, int8_t status, int32_t eOpt, SQWTaskStatus **task) {
  int32_t code = 0;

  char id[sizeof(qId) + sizeof(tId)] = {0};
  QW_SET_QTID(id, qId, tId);

  SQWTaskStatus ntask = {0};
  ntask.status = status;

  while (true) {
    QW_LOCK(QW_WRITE, &sch->tasksLock);
    int32_t code = taosHashPut(sch->tasksHash, id, sizeof(id), &ntask, sizeof(ntask));
    if (0 != code) {
      QW_UNLOCK(QW_WRITE, &sch->tasksLock);
      if (HASH_NODE_EXIST(code)) {
        if (QW_EXIST_ACQUIRE == eOpt && rwType && task) {
          if (qwAcquireTask(rwType, sch, qId, tId, task)) {
            continue;
          }
        } else if (QW_EXIST_RET_ERR == eOpt) {
          return TSDB_CODE_QRY_TASK_ALREADY_EXIST;
        } else {
          assert(0);
        }

        break;
      } else {
        qError("taosHashPut queryId[%"PRIx64"] taskId[%"PRIx64"] to scheduleHash failed", qId, tId);
        return TSDB_CODE_QRY_APP_ERROR;
      }
    }
    
    QW_UNLOCK(QW_WRITE, &sch->tasksLock);

    if (rwType && task) {
      if (TSDB_CODE_SUCCESS == qwAcquireTask(rwType, sch, qId, tId, task)) {
        return TSDB_CODE_SUCCESS;
      }
    } else {
      break;
    }
  }  

  return TSDB_CODE_SUCCESS;
}

static int32_t qwAddTask(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, int32_t status, int32_t eOpt, SQWSchStatus **sch, SQWTaskStatus **task) {
  SQWSchStatus *tsch = NULL;
  QW_ERR_RET(qwAddAcquireScheduler(QW_READ, mgmt, sId, &tsch));

  int32_t code = qwAddTaskToSch(QW_READ, tsch, qId, tId, status, eOpt, task);
  if (code) {
    qwReleaseScheduler(QW_WRITE, mgmt);
  }

  if (NULL == task) {
    qwReleaseScheduler(QW_READ, mgmt);
  } else if (sch) {
    *sch = tsch;
  }

  QW_RET(code);
}

static FORCE_INLINE int32_t qwAcquireTaskCtx(int32_t rwType, SQWorkerMgmt *mgmt, uint64_t queryId, uint64_t taskId, SQWTaskCtx **handles) {
  char id[sizeof(queryId) + sizeof(taskId)] = {0};
  QW_SET_QTID(id, queryId, taskId);
  
  QW_LOCK(rwType, &mgmt->ctxLock);
  *handles = taosHashGet(mgmt->ctxHash, id, sizeof(id));
  if (NULL == (*handles)) {
    QW_UNLOCK(rwType, &mgmt->ctxLock);
    return TSDB_CODE_QRY_RES_CACHE_NOT_EXIST;
  }

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE void qwReleaseTaskResCache(int32_t rwType, SQWorkerMgmt *mgmt) {
  QW_UNLOCK(rwType, &mgmt->ctxLock);
}


int32_t qwGetSchTasksStatus(SQWorkerMgmt *mgmt, uint64_t sId, SSchedulerStatusRsp **rsp) {
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



int32_t qwUpdateSchLastAccess(SQWorkerMgmt *mgmt, uint64_t sId) {
  SQWSchStatus *sch = NULL;

  QW_ERR_RET(qwAcquireScheduler(QW_READ, mgmt, sId, &sch));

  sch->lastAccessTs = taosGetTimestampSec();

  qwReleaseScheduler(QW_READ, mgmt);

  return TSDB_CODE_SUCCESS;
}

int32_t qwUpdateTaskStatus(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, int8_t status) {
  SQWSchStatus *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t code = 0;

  QW_ERR_RET(qwAcquireScheduler(QW_READ, mgmt, sId, &sch));

  QW_ERR_JRET(qwAcquireTask(QW_READ, sch, qId, tId, &task));

  QW_LOCK(QW_WRITE, &task->lock);
  qwUpdateTaskInfo(mgmt, task, QW_TASK_INFO_STATUS, &status, QW_IDS());
  QW_UNLOCK(QW_WRITE, &task->lock);
  
_return:

  qwReleaseTask(QW_READ, sch);
  qwReleaseScheduler(QW_READ, mgmt);

  QW_RET(code);
}


int32_t qwGetTaskStatus(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t queryId, uint64_t taskId, int8_t *taskStatus) {
  SQWSchStatus *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t code = 0;
  
  if (qwAcquireScheduler(QW_READ, mgmt, sId, &sch)) {
    *taskStatus = JOB_TASK_STATUS_NULL;
    return TSDB_CODE_SUCCESS;
  }

  if (qwAcquireTask(QW_READ, sch, queryId, taskId, &task)) {
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

  QW_ERR_RET(qwAddAcquireScheduler(QW_READ, mgmt, sId, &sch));

  if (qwAcquireTask(QW_READ, sch, qId, tId, &task)) {
    qwReleaseScheduler(QW_READ, mgmt);
    
    code = qwAddTask(mgmt, sId, qId, tId, JOB_TASK_STATUS_NOT_START, QW_EXIST_ACQUIRE, &sch, &task);
    if (code) {
      qwReleaseScheduler(QW_READ, mgmt);
      QW_ERR_RET(code);
    }
  }

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
    newStatus = JOB_TASK_STATUS_CANCELLED;
    QW_ERR_JRET(qwUpdateTaskInfo(mgmt, task, QW_TASK_INFO_STATUS, &newStatus, QW_IDS()));
  } else {
    newStatus = JOB_TASK_STATUS_CANCELLING;
    QW_ERR_JRET(qwUpdateTaskInfo(mgmt, task, QW_TASK_INFO_STATUS, &newStatus, QW_IDS()));
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

int32_t qwDropTask(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId) {
  SQWSchStatus *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t code = 0;
  
  char id[sizeof(qId) + sizeof(tId)] = {0};
  QW_SET_QTID(id, qId, tId);

  QW_LOCK(QW_WRITE, &mgmt->ctxLock);
  if (mgmt->ctxHash) {
    if (taosHashRemove(mgmt->ctxHash, id, sizeof(id))) {
      QW_TASK_WLOG("taosHashRemove from ctx hash failed, id:%s", id);
    }
  }
  QW_UNLOCK(QW_WRITE, &mgmt->ctxLock);
  
  if (qwAcquireScheduler(QW_WRITE, mgmt, sId, &sch)) {
    QW_TASK_WLOG("scheduler does not exist, sch:%p", sch);
    return TSDB_CODE_SUCCESS;
  }

  if (qwAcquireTask(QW_WRITE, sch, qId, tId, &task)) {
    qwReleaseScheduler(QW_WRITE, mgmt);
    
    QW_TASK_WLOG("task does not exist, task:%p", task);
    return TSDB_CODE_SUCCESS;
  }

  QW_TASK_DLOG("drop task, status:%d, code:%x, ready:%d, cancel:%d, drop:%d", task->status, task->code, task->ready, task->cancel, task->drop);

  if (taosHashRemove(sch->tasksHash, id, sizeof(id))) {
    QW_TASK_ELOG("taosHashRemove task from hash failed, task:%p", task);
    QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
  }

_return:

  qwReleaseTask(QW_WRITE, sch);
  qwReleaseScheduler(QW_WRITE, mgmt);
  
  QW_RET(code);
}

int32_t qwCancelDropTask(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId) {
  SQWSchStatus *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t code = 0;

  QW_ERR_RET(qwAddAcquireScheduler(QW_READ, mgmt, sId, &sch));

  if (qwAcquireTask(QW_READ, sch, qId, tId, &task)) {
    qwReleaseScheduler(QW_READ, mgmt);
    
    code = qwAddTask(mgmt, sId, qId, tId, JOB_TASK_STATUS_NOT_START, QW_EXIST_ACQUIRE, &sch, &task);
    if (code) {
      qwReleaseScheduler(QW_READ, mgmt);
      QW_ERR_RET(code);
    }
  }

  QW_LOCK(QW_WRITE, &task->lock);

  task->drop = true;

  int8_t oriStatus = task->status;
  int8_t newStatus = 0;
  
  if (task->status == JOB_TASK_STATUS_EXECUTING) {
    newStatus = JOB_TASK_STATUS_DROPPING;
    QW_ERR_JRET(qwUpdateTaskInfo(mgmt, task, QW_TASK_INFO_STATUS, &newStatus, QW_IDS()));
  } else if (task->status == JOB_TASK_STATUS_CANCELLING || task->status == JOB_TASK_STATUS_DROPPING || task->status == JOB_TASK_STATUS_NOT_START) {    
    QW_UNLOCK(QW_WRITE, &task->lock);
    qwReleaseTask(QW_READ, sch);
    qwReleaseScheduler(QW_READ, mgmt);
    
    return TSDB_CODE_SUCCESS;
  } else {
    QW_UNLOCK(QW_WRITE, &task->lock);
    qwReleaseTask(QW_READ, sch);
    qwReleaseScheduler(QW_READ, mgmt);
  
    QW_ERR_RET(qwDropTask(mgmt, sId, qId, tId));
    return TSDB_CODE_SUCCESS;
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

int32_t qwBuildAndSendQueryRsp(SRpcMsg *pMsg, int32_t code) {
  SQueryTableRsp *pRsp = (SQueryTableRsp *)rpcMallocCont(sizeof(SQueryTableRsp));
  pRsp->code = code;

  SRpcMsg rpcRsp = {
    .handle  = pMsg->handle,
    .ahandle = pMsg->ahandle,
    .pCont   = pRsp,
    .contLen = sizeof(*pRsp),
    .code    = code,
  };

  rpcSendResponse(&rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendReadyRsp(SRpcMsg *pMsg, int32_t code) {
  SResReadyRsp *pRsp = (SResReadyRsp *)rpcMallocCont(sizeof(SResReadyRsp));
  pRsp->code = code;

  SRpcMsg rpcRsp = {
    .handle  = pMsg->handle,
    .ahandle = pMsg->ahandle,
    .pCont   = pRsp,
    .contLen = sizeof(*pRsp),
    .code    = code,
  };

  rpcSendResponse(&rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendStatusRsp(SRpcMsg *pMsg, SSchedulerStatusRsp *sStatus) {
  int32_t size = 0;
  
  if (sStatus) {
    size = sizeof(SSchedulerStatusRsp) + sizeof(sStatus->status[0]) * sStatus->num;
  } else {
    size = sizeof(SSchedulerStatusRsp);
  }
  
  SSchedulerStatusRsp *pRsp = (SSchedulerStatusRsp *)rpcMallocCont(size);

  if (sStatus) {
    memcpy(pRsp, sStatus, size);
  } else {
    pRsp->num = 0;
  }

  SRpcMsg rpcRsp = {
    .msgType = pMsg->msgType + 1,
    .handle  = pMsg->handle,
    .ahandle = pMsg->ahandle,
    .pCont   = pRsp,
    .contLen = size,
    .code    = 0,
  };

  rpcSendResponse(&rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t qwInitFetchRsp(int32_t length, SRetrieveTableRsp **rsp) {
  int32_t msgSize = sizeof(SRetrieveTableRsp) + length;
  
  SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)rpcMallocCont(msgSize);
  if (NULL == pRsp) {
    qError("rpcMallocCont %d failed", msgSize);
    QW_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  memset(pRsp, 0, sizeof(SRetrieveTableRsp));

  *rsp = pRsp;

  return TSDB_CODE_SUCCESS;
}


int32_t qwBuildAndSendFetchRsp(SRpcMsg *pMsg, SRetrieveTableRsp *pRsp, int32_t dataLength, int32_t code) {
  if (NULL == pRsp) {
    pRsp = (SRetrieveTableRsp *)rpcMallocCont(sizeof(SRetrieveTableRsp));
    memset(pRsp, 0, sizeof(SRetrieveTableRsp));
    dataLength = 0;
  }

  SRpcMsg rpcRsp = {
    .handle  = pMsg->handle,
    .ahandle = pMsg->ahandle,
    .pCont   = pRsp,
    .contLen = sizeof(*pRsp) + dataLength,
    .code    = code,
  };

  rpcSendResponse(&rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendCancelRsp(SRpcMsg *pMsg, int32_t code) {
  STaskCancelRsp *pRsp = (STaskCancelRsp *)rpcMallocCont(sizeof(STaskCancelRsp));
  pRsp->code = code;

  SRpcMsg rpcRsp = {
    .handle  = pMsg->handle,
    .ahandle = pMsg->ahandle,
    .pCont   = pRsp,
    .contLen = sizeof(*pRsp),
    .code    = code,
  };

  rpcSendResponse(&rpcRsp);
  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendDropRsp(SRpcMsg *pMsg, int32_t code) {
  STaskDropRsp *pRsp = (STaskDropRsp *)rpcMallocCont(sizeof(STaskDropRsp));
  pRsp->code = code;

  SRpcMsg rpcRsp = {
    .handle  = pMsg->handle,
    .ahandle = pMsg->ahandle,
    .pCont   = pRsp,
    .contLen = sizeof(*pRsp),
    .code    = code,
  };

  rpcSendResponse(&rpcRsp);
  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendShowRsp(SRpcMsg *pMsg, int32_t code) {
  int32_t numOfCols = 6;
  int32_t msgSize = sizeof(SVShowTablesRsp) + sizeof(SSchema) * numOfCols;

  SVShowTablesRsp *pRsp = (SVShowTablesRsp *)rpcMallocCont(msgSize);

  int32_t  cols = 0;
  SSchema *pSchema = pRsp->metaInfo.pSchema;

  const SSchema *s = tGetTbnameColumnSchema();
  *pSchema = createSchema(s->type, htonl(s->bytes), htonl(++cols), "name");
  pSchema++;

  int32_t type = TSDB_DATA_TYPE_TIMESTAMP;
  *pSchema = createSchema(type, htonl(tDataTypes[type].bytes), htonl(++cols), "created");
  pSchema++;

  type = TSDB_DATA_TYPE_SMALLINT;
  *pSchema = createSchema(type, htonl(tDataTypes[type].bytes), htonl(++cols), "columns");
  pSchema++;

  *pSchema = createSchema(s->type, htonl(s->bytes), htonl(++cols), "stable");
  pSchema++;

  type = TSDB_DATA_TYPE_BIGINT;
  *pSchema = createSchema(type, htonl(tDataTypes[type].bytes), htonl(++cols), "uid");
  pSchema++;

  type = TSDB_DATA_TYPE_INT;
  *pSchema = createSchema(type, htonl(tDataTypes[type].bytes), htonl(++cols), "vgId");

  assert(cols == numOfCols);
  pRsp->metaInfo.numOfColumns = htonl(cols);

  SRpcMsg rpcMsg = {
      .handle  = pMsg->handle,
      .ahandle = pMsg->ahandle,
      .pCont   = pRsp,
      .contLen = msgSize,
      .code    = code,
  };

  rpcSendResponse(&rpcMsg);
  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendShowFetchRsp(SRpcMsg *pMsg, SVShowTablesFetchReq* pFetchReq) {
  SVShowTablesFetchRsp *pRsp = (SVShowTablesFetchRsp *)rpcMallocCont(sizeof(SVShowTablesFetchRsp));
  int32_t handle = htonl(pFetchReq->id);

  pRsp->numOfRows = 0;
  SRpcMsg rpcMsg = {
      .handle  = pMsg->handle,
      .ahandle = pMsg->ahandle,
      .pCont   = pRsp,
      .contLen = sizeof(*pRsp),
      .code    = 0,
  };

  rpcSendResponse(&rpcMsg);
  return TSDB_CODE_SUCCESS;
}

int32_t qwCheckAndSendReadyRsp(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t queryId, uint64_t taskId, SRpcMsg *pMsg, int32_t rspCode) {
  SQWSchStatus *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t code = 0;

  QW_ERR_RET(qwAcquireScheduler(QW_READ, mgmt, sId, &sch));

  QW_ERR_JRET(qwAcquireTask(QW_READ, sch, queryId, taskId, &task));

  QW_LOCK(QW_WRITE, &task->lock);

  if (QW_READY_NOT_RECEIVED == task->ready) {
    QW_UNLOCK(QW_WRITE, &task->lock);

    qwReleaseTask(QW_READ, sch);
    qwReleaseScheduler(QW_READ, mgmt);
    
    return TSDB_CODE_SUCCESS;
  } else if (QW_READY_RECEIVED == task->ready) {
    QW_ERR_JRET(qwBuildAndSendReadyRsp(pMsg, rspCode));

    task->ready = QW_READY_RESPONSED;
  } else if (QW_READY_RESPONSED == task->ready) {
    qError("query response already send");
    QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
  } else {
    assert(0);
  }

_return:

  if (task) {
    QW_UNLOCK(QW_WRITE, &task->lock);
    qwReleaseTask(QW_READ, sch);

  }

  qwReleaseScheduler(QW_READ, mgmt);

  QW_RET(code);
}

int32_t qwSetAndSendReadyRsp(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t queryId, uint64_t taskId, SRpcMsg *pMsg) {
  SQWSchStatus *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t code = 0;

  QW_ERR_RET(qwAcquireScheduler(QW_READ, mgmt, sId, &sch));

  QW_ERR_JRET(qwAcquireTask(QW_READ, sch, queryId, taskId, &task));

  QW_LOCK(QW_WRITE, &task->lock);
  if (QW_TASK_READY_RESP(task->status)) {
    QW_ERR_JRET(qwBuildAndSendReadyRsp(pMsg, task->code));

    task->ready = QW_READY_RESPONSED;
  } else {
    task->ready = QW_READY_RECEIVED;
    QW_UNLOCK(QW_WRITE, &task->lock);

    qwReleaseTask(QW_READ, sch);
    qwReleaseScheduler(QW_READ, mgmt);
    
    return TSDB_CODE_SUCCESS;
  }

_return:

  if (task) {
    QW_UNLOCK(QW_WRITE, &task->lock);
    qwReleaseTask(QW_READ, sch);
  }

  qwReleaseScheduler(QW_READ, mgmt);

  QW_RET(code);
}

int32_t qwCheckTaskCancelDrop(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, bool *needStop) {
  SQWSchStatus *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t code = 0;
  int8_t status = JOB_TASK_STATUS_CANCELLED;

  *needStop = false;

  if (qwAcquireScheduler(QW_READ, mgmt, sId, &sch)) {
    return TSDB_CODE_SUCCESS;
  }

  if (qwAcquireTask(QW_READ, sch, qId, tId, &task)) {
    qwReleaseScheduler(QW_READ, mgmt);
    return TSDB_CODE_SUCCESS;
  }

  QW_LOCK(QW_READ, &task->lock);
  
  if ((!task->cancel) && (!task->drop)) {
    QW_TASK_ELOG("no cancel or drop but task exists, status:%d", task->status);
    
    QW_UNLOCK(QW_READ, &task->lock);
    
    qwReleaseTask(QW_READ, sch);
    qwReleaseScheduler(QW_READ, mgmt);

    QW_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  QW_UNLOCK(QW_READ, &task->lock);

  *needStop = true;
  
  if (task->cancel) {
    QW_LOCK(QW_WRITE, &task->lock);
    code = qwUpdateTaskInfo(mgmt, task, QW_TASK_INFO_STATUS, &status, QW_IDS());
    QW_UNLOCK(QW_WRITE, &task->lock);
    
    QW_ERR_JRET(code);
  }

  if (task->drop) {
    qwReleaseTask(QW_READ, sch);
    qwReleaseScheduler(QW_READ, mgmt);
    
    QW_RET(qwDropTask(mgmt, sId, qId, tId));
  }

_return:

  qwReleaseTask(QW_READ, sch);
  qwReleaseScheduler(QW_READ, mgmt);

  return TSDB_CODE_SUCCESS;
}


int32_t qwQueryPostProcess(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, int8_t status, int32_t errCode) {
  SQWSchStatus *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t code = 0;
  int8_t newStatus = JOB_TASK_STATUS_CANCELLED;

  code = qwAddAcquireScheduler(QW_READ, mgmt, sId, &sch);
  if (code) {
    qError("sId:%"PRIx64" not in cache", sId);
    QW_ERR_RET(code);
  }

  code = qwAcquireTask(QW_READ, sch, qId, tId, &task);
  if (code) {
    qwReleaseScheduler(QW_READ, mgmt);
    
    if (JOB_TASK_STATUS_PARTIAL_SUCCEED == status || JOB_TASK_STATUS_SUCCEED == status) {
      qError("sId:%"PRIx64" queryId:%"PRIx64" taskId:%"PRIx64" not in cache", sId, qId, tId);
      QW_ERR_RET(code);
    }

    QW_ERR_RET(qwAddTask(mgmt, sId, qId, tId, status, QW_EXIST_ACQUIRE, &sch, &task));
  }

  if (task->cancel) {
    QW_LOCK(QW_WRITE, &task->lock);
    qwUpdateTaskInfo(mgmt, task, QW_TASK_INFO_STATUS, &newStatus, QW_IDS());
    QW_UNLOCK(QW_WRITE, &task->lock);
  }

  if (task->drop) {
    qwReleaseTask(QW_READ, sch);
    qwReleaseScheduler(QW_READ, mgmt);
    
    qwDropTask(mgmt, sId, qId, tId);

    return TSDB_CODE_SUCCESS;
  }

  if (!(task->cancel || task->drop)) {
    QW_LOCK(QW_WRITE, &task->lock);
    qwUpdateTaskInfo(mgmt, task, QW_TASK_INFO_STATUS, &status, QW_IDS());
    task->code = errCode;
    QW_UNLOCK(QW_WRITE, &task->lock);
  }
  
  qwReleaseTask(QW_READ, sch);
  qwReleaseScheduler(QW_READ, mgmt);

  return TSDB_CODE_SUCCESS;
}

int32_t qwScheduleDataSink(SQWTaskCtx *handles, SQWorkerMgmt *mgmt, uint64_t sId, uint64_t queryId, uint64_t taskId, SRpcMsg *pMsg) {
  if (atomic_load_8(&handles->sinkScheduled)) {
    qDebug("data sink already scheduled");
    return TSDB_CODE_SUCCESS;
  }
  
  SSinkDataReq * req = (SSinkDataReq *)rpcMallocCont(sizeof(SSinkDataReq));
  if (NULL == req) {
    qError("rpcMallocCont %d failed", (int32_t)sizeof(SSinkDataReq));
    QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  req->header.vgId = mgmt->nodeId;
  req->sId = sId;
  req->queryId = queryId;
  req->taskId = taskId;

  SRpcMsg pNewMsg = {
    .handle = pMsg->handle,
    .ahandle = pMsg->ahandle, 
    .msgType = TDMT_VND_SCHEDULE_DATA_SINK,
    .pCont   = req,
    .contLen = sizeof(SSinkDataReq),
    .code    = 0,
  };

  int32_t code = (*mgmt->putToQueueFp)(mgmt->nodeObj, &pNewMsg);
  if (TSDB_CODE_SUCCESS != code) {
    qError("put data sink schedule msg to queue failed, code:%x", code);
    rpcFreeCont(req);
    QW_ERR_RET(code);
  }

  qDebug("put data sink schedule msg to query queue");

  return TSDB_CODE_SUCCESS;
}

int32_t qwScheduleQuery(SQWTaskCtx *handles, SQWorkerMgmt *mgmt, uint64_t sId, uint64_t queryId, uint64_t taskId, SRpcMsg *pMsg) {
  if (atomic_load_8(&handles->queryScheduled)) {
    qDebug("query already scheduled");
    return TSDB_CODE_SUCCESS;
  }

  QW_ERR_RET(qwUpdateTaskStatus(mgmt, sId, queryId, taskId, JOB_TASK_STATUS_EXECUTING));      

  SQueryContinueReq * req = (SQueryContinueReq *)rpcMallocCont(sizeof(SQueryContinueReq));
  if (NULL == req) {
    qError("rpcMallocCont %d failed", (int32_t)sizeof(SQueryContinueReq));
    QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  req->header.vgId = mgmt->nodeId;
  req->sId = sId;
  req->queryId = queryId;
  req->taskId = taskId;

  SRpcMsg pNewMsg = {
    .handle = pMsg->handle,
    .ahandle = pMsg->ahandle,
    .msgType = TDMT_VND_QUERY_CONTINUE,
    .pCont   = req,
    .contLen = sizeof(SQueryContinueReq),
    .code    = 0,
  };

  int32_t code = (*mgmt->putToQueueFp)(mgmt->nodeObj, &pNewMsg);
  if (TSDB_CODE_SUCCESS != code) {
    qError("put query continue msg to queue failed, code:%x", code);
    rpcFreeCont(req);
    QW_ERR_RET(code);
  }


  qDebug("put query continue msg to query queue");

  return TSDB_CODE_SUCCESS;
}



int32_t qwHandleFetch(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t queryId, uint64_t taskId, SRpcMsg *pMsg) {
  SQWSchStatus *sch = NULL;
  SQWTaskStatus *task = NULL;
  int32_t code = 0;
  int32_t needRsp = true;
  void *data = NULL;
  int32_t sinkStatus = 0;
  int32_t dataLength = 0;
  SRetrieveTableRsp *rsp = NULL;
  bool queryEnd = false;
  SQWTaskCtx *handles = NULL;

  QW_ERR_JRET(qwAcquireTaskCtx(QW_READ, mgmt, queryId, taskId, &handles));
  if (atomic_load_8(&handles->needRsp)) {
    qError("last fetch not responsed");
    QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
  }

  QW_ERR_JRET(qwAcquireScheduler(QW_READ, mgmt, sId, &sch));
  QW_ERR_JRET(qwAcquireTask(QW_READ, sch, queryId, taskId, &task));

  QW_LOCK(QW_READ, &task->lock);

  if (task->cancel || task->drop) {
    qError("task is already cancelled or dropped");
    QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
  }

  if (task->status != JOB_TASK_STATUS_EXECUTING && task->status != JOB_TASK_STATUS_PARTIAL_SUCCEED) {
    qError("invalid status %d for fetch", task->status);
    QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
  }

  dsGetDataLength(handles->sinkHandle, &dataLength, &queryEnd);
  
  if (dataLength > 0) {
    SOutputData output = {0};
    QW_ERR_JRET(qwInitFetchRsp(dataLength, &rsp));
    
    output.pData = rsp->data;
    
    code = dsGetDataBlock(handles->sinkHandle, &output);
    if (code) {
      qError("dsGetDataBlock failed, code:%x", code);
      QW_ERR_JRET(code);
    }

    rsp->useconds = htobe64(output.useconds);
    rsp->completed = 0;
    rsp->precision = output.precision;
    rsp->compressed = output.compressed;
    rsp->compLen = htonl(dataLength);
    rsp->numOfRows = htonl(output.numOfRows);
    
    if (DS_BUF_EMPTY == output.bufStatus && output.queryEnd) {
      rsp->completed = 1;
      
      QW_ERR_JRET(qwUpdateTaskStatus(mgmt, sId, queryId, taskId, JOB_TASK_STATUS_SUCCEED));      
    }

    // Note: schedule data sink firstly and will schedule query after it's done
    if (output.needSchedule) {
      QW_ERR_JRET(qwScheduleDataSink(handles, mgmt, sId, queryId, taskId, pMsg));
    } else if ((!output.queryEnd) && (DS_BUF_LOW == output.bufStatus || DS_BUF_EMPTY == output.bufStatus)) {
      QW_ERR_JRET(qwScheduleQuery(handles, mgmt, sId, queryId, taskId, pMsg));
    }
  } else {
    if (dataLength < 0) {
      qError("invalid length from dsGetDataLength, length:%d", dataLength);
      QW_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
    }
    
    if (queryEnd) {
      QW_ERR_JRET(qwUpdateTaskStatus(mgmt, sId, queryId, taskId, JOB_TASK_STATUS_SUCCEED));
    } else {
      assert(0 == handles->needRsp);
      
      qDebug("no res data in sink, need response later");

      QW_LOCK(QW_WRITE, &handles->lock);
      handles->needRsp = true;
      QW_UNLOCK(QW_WRITE, &handles->lock);

      needRsp = false;
    }
  }

_return:

  if (task) {
    QW_UNLOCK(QW_READ, &task->lock);
    qwReleaseTask(QW_READ, sch);    
  }
  
  if (sch) {
    qwReleaseScheduler(QW_READ, mgmt);
  }

  if (needRsp) {
    qwBuildAndSendFetchRsp(pMsg, rsp, dataLength, code);
  }

  if (handles) {
    qwReleaseTaskResCache(QW_READ, mgmt);
  }
  
  QW_RET(code);
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

  mgmt->schHash = taosHashInit(mgmt->cfg.maxSchedulerNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_NO_LOCK);
  if (NULL == mgmt->schHash) {
    tfree(mgmt);
    qError("init %d scheduler hash failed", mgmt->cfg.maxSchedulerNum);
    QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  mgmt->ctxHash = taosHashInit(mgmt->cfg.maxTaskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
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

int32_t qWorkerProcessQueryMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t code = 0;
  bool queryRsped = false;
  bool needStop = false;
  struct SSubplan *plan = NULL;
  SSubQueryMsg *msg = pMsg->pCont;
  SQWorkerMgmt *mgmt = (SQWorkerMgmt *)qWorkerMgmt;
  
  if (NULL == msg || pMsg->contLen <= sizeof(*msg)) {
    QW_ELOG("invalid query msg, contLen:%d", pMsg->contLen);
    QW_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  msg->sId = be64toh(msg->sId);
  msg->queryId = be64toh(msg->queryId);
  msg->taskId = be64toh(msg->taskId);
  msg->contentLen = ntohl(msg->contentLen);
  
  uint64_t sId = msg->sId;
  uint64_t qId = msg->queryId;
  uint64_t tId = msg->taskId;

  QW_ERR_JRET(qwCheckTaskCancelDrop(qWorkerMgmt, msg->sId, msg->queryId, msg->taskId, &needStop));
  if (needStop) {
    qWarn("task need stop");
    qwBuildAndSendQueryRsp(pMsg, TSDB_CODE_QRY_TASK_CANCELLED);
    QW_ERR_RET(TSDB_CODE_QRY_TASK_CANCELLED);
  }
  
  code = qStringToSubplan(msg->msg, &plan);
  if (TSDB_CODE_SUCCESS != code) {
    qError("schId:%"PRIx64",qId:%"PRIx64",taskId:%"PRIx64" string to subplan failed, code:%d", msg->sId, msg->queryId, msg->taskId, code);
    QW_ERR_JRET(code);
  }

  qTaskInfo_t pTaskInfo = NULL;
  code = qCreateExecTask(node, 0, (struct SSubplan *)plan, &pTaskInfo);
  if (code) {
    qError("qCreateExecTask failed, code:%x", code);
    QW_ERR_JRET(code);
  } else {
    QW_ERR_JRET(qwAddTask(qWorkerMgmt, msg->sId, msg->queryId, msg->taskId, JOB_TASK_STATUS_EXECUTING, QW_EXIST_RET_ERR, NULL, NULL));
  }

  QW_ERR_JRET(qwBuildAndSendQueryRsp(pMsg, TSDB_CODE_SUCCESS));

  queryRsped = true;

  DataSinkHandle sinkHandle = NULL;
  code = qExecTask(pTaskInfo, &sinkHandle);

  if (code) {
    qError("qExecTask failed, code:%x", code);  
    QW_ERR_JRET(code);
  } else {
    QW_ERR_JRET(qwAddTaskHandlesToCache(qWorkerMgmt, msg->queryId, msg->taskId, pTaskInfo, sinkHandle));
    QW_ERR_JRET(qwUpdateTaskStatus(qWorkerMgmt, msg->sId, msg->queryId, msg->taskId, JOB_TASK_STATUS_PARTIAL_SUCCEED));
  } 

_return:

  if (queryRsped) {
    code = qwCheckAndSendReadyRsp(qWorkerMgmt, msg->sId, msg->queryId, msg->taskId, pMsg, code);
  } else {
    code = qwBuildAndSendQueryRsp(pMsg, code);
  }
  
  int8_t status = 0;
  if (TSDB_CODE_SUCCESS != code) {
    status = JOB_TASK_STATUS_FAILED;
  } else {
    status = JOB_TASK_STATUS_PARTIAL_SUCCEED;
  }

  qwQueryPostProcess(qWorkerMgmt, msg->sId, msg->queryId, msg->taskId, status, code);
  
  QW_RET(code);
}

int32_t qWorkerProcessQueryContinueMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  int32_t code = 0;
  int8_t status = 0;
  bool queryDone = false;
  SQueryContinueReq *req = (SQueryContinueReq *)pMsg->pCont;
  bool needStop = false;
  SQWTaskCtx *handles = NULL;

  QW_ERR_JRET(qwAcquireTaskCtx(QW_READ, qWorkerMgmt, req->queryId, req->taskId, &handles));

  qTaskInfo_t     taskHandle = handles->taskHandle;
  DataSinkHandle  sinkHandle = handles->sinkHandle;
  bool needRsp = handles->needRsp;

  qwReleaseTaskResCache(QW_READ, qWorkerMgmt);
  
  QW_ERR_JRET(qwCheckTaskCancelDrop(qWorkerMgmt, req->sId, req->queryId, req->taskId, &needStop));
  if (needStop) {
    qWarn("task need stop");
    if (needRsp) {
      qwBuildAndSendQueryRsp(pMsg, TSDB_CODE_QRY_TASK_CANCELLED);
    }
    QW_ERR_RET(TSDB_CODE_QRY_TASK_CANCELLED);
  }

  DataSinkHandle newHandle = NULL;
  code = qExecTask(taskHandle, &newHandle);
  if (code) {
    qError("qExecTask failed, code:%x", code);  
    QW_ERR_JRET(code);
  }
  
  if (sinkHandle != newHandle) {
    qError("data sink mis-match");
    QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
  }
  
_return:

  if (needRsp) {
    code = qwBuildAndSendQueryRsp(pMsg, code);
  }
  
  if (TSDB_CODE_SUCCESS != code) {
    status = JOB_TASK_STATUS_FAILED;
  } else {
    status = JOB_TASK_STATUS_PARTIAL_SUCCEED;
  }

  code = qwQueryPostProcess(qWorkerMgmt, req->sId, req->queryId, req->taskId, status, code);
  
  QW_RET(code);
}



int32_t qWorkerProcessDataSinkMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg){
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SSinkDataReq *msg = pMsg->pCont;
  if (NULL == msg || pMsg->contLen < sizeof(*msg)) {
    qError("invalid sink data msg");
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  //dsScheduleProcess();
  //TODO

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessReadyMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg){
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SResReadyReq *msg = pMsg->pCont;
  if (NULL == msg || pMsg->contLen < sizeof(*msg)) {
    qError("invalid task status msg");  
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }  

  msg->sId = htobe64(msg->sId);
  msg->queryId = htobe64(msg->queryId);
  msg->taskId = htobe64(msg->taskId);

  QW_ERR_RET(qwSetAndSendReadyRsp(qWorkerMgmt, msg->sId, msg->queryId, msg->taskId, pMsg));
  
  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessStatusMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  int32_t code = 0;
  SSchTasksStatusReq *msg = pMsg->pCont;
  if (NULL == msg || pMsg->contLen < sizeof(*msg)) {
    qError("invalid task status msg");
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }  

  msg->sId = htobe64(msg->sId);

  SSchedulerStatusRsp *sStatus = NULL;
  
  QW_ERR_JRET(qwGetSchTasksStatus(qWorkerMgmt, msg->sId, &sStatus));

_return:

  QW_ERR_RET(qwBuildAndSendStatusRsp(pMsg, sStatus));

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessFetchMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SResFetchReq *msg = pMsg->pCont;
  if (NULL == msg || pMsg->contLen < sizeof(*msg)) {
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }  

  msg->sId = htobe64(msg->sId);
  msg->queryId = htobe64(msg->queryId);
  msg->taskId = htobe64(msg->taskId);

  QW_ERR_RET(qwUpdateSchLastAccess(qWorkerMgmt, msg->sId));

  void *data = NULL;
  int32_t code = 0;
  
  QW_ERR_RET(qwHandleFetch(qWorkerMgmt, msg->sId, msg->queryId, msg->taskId, pMsg));

  QW_RET(code);
}

int32_t qWorkerProcessCancelMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  int32_t code = 0;
  STaskCancelReq *msg = pMsg->pCont;
  if (NULL == msg || pMsg->contLen < sizeof(*msg)) {
    qError("invalid task cancel msg");  
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }  

  msg->sId = htobe64(msg->sId);
  msg->queryId = htobe64(msg->queryId);
  msg->taskId = htobe64(msg->taskId);

  QW_ERR_JRET(qwCancelTask(qWorkerMgmt, msg->sId, msg->queryId, msg->taskId));

_return:

  QW_ERR_RET(qwBuildAndSendCancelRsp(pMsg, code));

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessDropMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  int32_t code = 0;
  STaskDropReq *msg = pMsg->pCont;
  if (NULL == msg || pMsg->contLen < sizeof(*msg)) {
    qError("invalid task drop msg");
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }  

  msg->sId = htobe64(msg->sId);
  msg->queryId = htobe64(msg->queryId);
  msg->taskId = htobe64(msg->taskId);

  QW_ERR_JRET(qwCancelDropTask(qWorkerMgmt, msg->sId, msg->queryId, msg->taskId));

_return:

  QW_ERR_RET(qwBuildAndSendDropRsp(pMsg, code));

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessShowMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  int32_t code = 0;
  SVShowTablesReq *pReq = pMsg->pCont;
  QW_ERR_RET(qwBuildAndSendShowRsp(pMsg, code));
}

int32_t qWorkerProcessShowFetchMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SVShowTablesFetchReq *pFetchReq = pMsg->pCont;
  QW_ERR_RET(qwBuildAndSendShowFetchRsp(pMsg, pFetchReq));
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


