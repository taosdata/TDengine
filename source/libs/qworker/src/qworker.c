#include "tmsg.h"
#include "query.h"
#include "qworker.h"
#include "qworkerInt.h"
#include "planner.h"

int32_t qwCheckStatusSwitch(int8_t oriStatus, int8_t newStatus) {
  int32_t code = 0;

  if (oriStatus == newStatus) {
    if (newStatus == JOB_TASK_STATUS_CANCELLING) {
      return TSDB_CODE_SUCCESS;
    }
    
    QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
  }
  
  switch (oriStatus) {
    case JOB_TASK_STATUS_NULL:
      if (newStatus != JOB_TASK_STATUS_EXECUTING && newStatus != JOB_TASK_STATUS_FAILED ) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }
      
      break;
    case JOB_TASK_STATUS_NOT_START:
      if (newStatus != JOB_TASK_STATUS_EXECUTING && newStatus != JOB_TASK_STATUS_FAILED) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }
      
      break;
    case JOB_TASK_STATUS_EXECUTING:
      if (newStatus != JOB_TASK_STATUS_SUCCEED && newStatus != JOB_TASK_STATUS_FAILED && newStatus != JOB_TASK_STATUS_CANCELLING) {
        QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
      }
      
      break;
    case JOB_TASK_STATUS_PARTIAL_SUCCEED:
      if (newStatus != JOB_TASK_STATUS_EXECUTING && newStatus != JOB_TASK_STATUS_CANCELLING) {
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
    default:
      qError("invalid task status:%d", oriStatus);
      return TSDB_CODE_QRY_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;

_return:

  qError("invalid task status:%d", oriStatus);
  QW_ERR_RET(code);
}

int32_t qwUpdateTaskInfo(SQWorkerTaskStatus *task, int8_t type, void *data) {
  int32_t code = 0;
  
  switch (type) {
    case QW_TASK_INFO_STATUS: {
      int8_t newStatus = *(int8_t *)data;
      QW_ERR_RET(qwCheckStatusSwitch(task->status, newStatus));
      task->status = newStatus;
      break;
    }
    default:
      qError("uknown task info type:%d", type);
      return TSDB_CODE_QRY_APP_ERROR;
  }
  
  return TSDB_CODE_SUCCESS;
}

int32_t qwAddTaskResult(SQWorkerMgmt *mgmt, uint64_t queryId, uint64_t taskId, void *data) {
  char id[sizeof(queryId) + sizeof(taskId)] = {0};
  QW_SET_QTID(id, queryId, taskId);

  SQWorkerResCache resCache = {0};
  resCache.data = data;

  QW_LOCK(QW_WRITE, &mgmt->resLock);
  if (0 != taosHashPut(mgmt->resHash, id, sizeof(id), &resCache, sizeof(SQWorkerResCache))) {
    QW_UNLOCK(QW_WRITE, &mgmt->resLock);
    qError("taosHashPut queryId[%"PRIx64"] taskId[%"PRIx64"] to resHash failed", queryId, taskId);
    return TSDB_CODE_QRY_APP_ERROR;
  }

  QW_UNLOCK(QW_WRITE, &mgmt->resLock);

  return TSDB_CODE_SUCCESS;
}


int32_t qwGetTaskResult(SQWorkerMgmt *mgmt, uint64_t queryId, uint64_t taskId, void **data) {
  char id[sizeof(queryId) + sizeof(taskId)] = {0};
  QW_SET_QTID(id, queryId, taskId);

  SQWorkerResCache *resCache = taosHashGet(mgmt->resHash, id, sizeof(id));
  if (NULL == resCache) {
    qError("no task res for queryId[%"PRIx64"] taskId[%"PRIx64"]", queryId, taskId);
    return TSDB_CODE_QRY_APP_ERROR;
  }

  *data = resCache->data;

  return TSDB_CODE_SUCCESS;
}


static FORCE_INLINE int32_t qwAcquireScheduler(int32_t rwType, SQWorkerMgmt *mgmt, uint64_t schedulerId, SQWorkerSchStatus **sch) {
  QW_LOCK(rwType, &mgmt->schLock);
  *sch = taosHashGet(mgmt->schHash, &schedulerId, sizeof(schedulerId));
  if (NULL == (*sch)) {
    QW_LOCK(rwType, &mgmt->schLock);
    return TSDB_CODE_QRY_SCH_NOT_EXIST;
  }

  return TSDB_CODE_SUCCESS;
}


static FORCE_INLINE int32_t qwInsertAndAcquireScheduler(int32_t rwType, SQWorkerMgmt *mgmt, uint64_t schedulerId, SQWorkerSchStatus **sch) {
  SQWorkerSchStatus newSch = {0};
  newSch.tasksHash = taosHashInit(mgmt->cfg.maxSchTaskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (NULL == newSch.tasksHash) {
    qError("taosHashInit %d failed", mgmt->cfg.maxSchTaskNum);
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  while (true) {
    QW_LOCK(QW_WRITE, &mgmt->schLock);
    int32_t code = taosHashPut(mgmt->schHash, &schedulerId, sizeof(schedulerId), &newSch, sizeof(newSch));
    if (0 != code) {
      if (!HASH_NODE_EXIST(code)) {
        QW_UNLOCK(QW_WRITE, &mgmt->schLock);
        qError("taosHashPut schedulerId[%"PRIx64"] to scheduleHash failed", schedulerId);
        taosHashCleanup(newSch.tasksHash);
        return TSDB_CODE_QRY_APP_ERROR;
      }
    }
    
    QW_UNLOCK(QW_WRITE, &mgmt->schLock);
    if (TSDB_CODE_SUCCESS == qwAcquireScheduler(rwType, mgmt, schedulerId, sch)) {
      taosHashCleanup(newSch.tasksHash);
      return TSDB_CODE_SUCCESS;
    }
  }

  return TSDB_CODE_SUCCESS;
}


static FORCE_INLINE void qwReleaseScheduler(int32_t rwType, SQWorkerMgmt *mgmt) {
  QW_UNLOCK(rwType, &mgmt->schLock);
}

static FORCE_INLINE int32_t qwAcquireTask(int32_t rwType, SQWorkerSchStatus *sch, uint64_t queryId, uint64_t taskId, SQWorkerTaskStatus **task) {
  char id[sizeof(queryId) + sizeof(taskId)] = {0};
  QW_SET_QTID(id, queryId, taskId);

  QW_LOCK(rwType, &sch->tasksLock);
  *task = taosHashGet(sch->tasksHash, id, sizeof(id));
  if (NULL == (*task)) {
    QW_UNLOCK(rwType, &sch->tasksLock);
    return TSDB_CODE_QRY_TASK_NOT_EXIST;
  }

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t qwInsertAndAcquireTask(int32_t rwType, SQWorkerSchStatus *sch, uint64_t queryId, uint64_t taskId, int8_t status, bool *inserted, SQWorkerTaskStatus **task) {
  char id[sizeof(queryId) + sizeof(taskId)] = {0};
  QW_SET_QTID(id, queryId, taskId);

  while (true) {
    *inserted = false;
    
    QW_LOCK(QW_WRITE, &sch->tasksLock);
    int32_t code = taosHashPut(sch->tasksHash, id, sizeof(id), &status, sizeof(status));
    if (0 != code) {
      QW_UNLOCK(QW_WRITE, &sch->tasksLock);
      if (HASH_NODE_EXIST(code)) {
        if (qwAcquireTask(rwType, sch, queryId, taskId, task)) {
          continue;
        }

        break;
      } else {
        qError("taosHashPut queryId[%"PRIx64"] taskId[%"PRIx64"] to scheduleHash failed", queryId, taskId);
        return TSDB_CODE_QRY_APP_ERROR;
      }
    }
    QW_UNLOCK(QW_WRITE, &sch->tasksLock);

    *inserted = true;
    
    if (TSDB_CODE_SUCCESS == qwAcquireTask(rwType, sch, queryId, taskId, task)) {
      return TSDB_CODE_SUCCESS;
    }
  }

  return TSDB_CODE_SUCCESS;
}


static FORCE_INLINE void qwReleaseTask(int32_t rwType, SQWorkerSchStatus *sch) {
  QW_UNLOCK(rwType, &sch->tasksLock);
}

static FORCE_INLINE int32_t qwAcquireTaskResCache(int32_t rwType, SQWorkerMgmt *mgmt, uint64_t queryId, uint64_t taskId, SQWorkerResCache **res) {
  char id[sizeof(queryId) + sizeof(taskId)] = {0};
  QW_SET_QTID(id, queryId, taskId);
  
  QW_LOCK(rwType, &mgmt->resLock);
  *res = taosHashGet(mgmt->resHash, id, sizeof(id));
  if (NULL == (*res)) {
    QW_UNLOCK(rwType, &mgmt->resLock);
    return TSDB_CODE_QRY_RES_CACHE_NOT_EXIST;
  }

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE void qwReleaseTaskResCache(int32_t rwType, SQWorkerMgmt *mgmt) {
  QW_UNLOCK(rwType, &mgmt->resLock);
}


int32_t qwGetSchTasksStatus(SQWorkerMgmt *mgmt, uint64_t schedulerId, SSchedulerStatusRsp **rsp) {
  SQWorkerSchStatus *schStatus = NULL;
  int32_t taskNum = 0;

  if (qwAcquireScheduler(QW_READ, mgmt, schedulerId, &schStatus)) {
    qWarn("no scheduler for schedulerId[%"PRIx64"]", schedulerId);
  } else {
    schStatus->lastAccessTs = taosGetTimestampSec();

    QW_LOCK(QW_READ, &schStatus->tasksLock);
    taskNum = taosHashGetSize(schStatus->tasksHash);
  }
  
  int32_t size = sizeof(SSchedulerStatusRsp) + sizeof((*rsp)->status[0]) * taskNum;
  *rsp = calloc(1, size);
  if (NULL == *rsp) {
    qError("calloc %d failed", size);
    if (schStatus) {
      QW_UNLOCK(QW_READ, &schStatus->tasksLock);
      qwReleaseScheduler(QW_READ, mgmt);
    }
    
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  void *key = NULL;
  size_t keyLen = 0;
  int32_t i = 0;

  if (schStatus) {
    void *pIter = taosHashIterate(schStatus->tasksHash, NULL);
    while (pIter) {
      SQWorkerTaskStatus *taskStatus = (SQWorkerTaskStatus *)pIter;
      taosHashGetKey(pIter, &key, &keyLen);

      QW_GET_QTID(key, (*rsp)->status[i].queryId, (*rsp)->status[i].taskId);
      (*rsp)->status[i].status = taskStatus->status;
      
      pIter = taosHashIterate(schStatus->tasksHash, pIter);
    }  
  }

  if (schStatus) {
    QW_UNLOCK(QW_READ, &schStatus->tasksLock);
    qwReleaseScheduler(QW_READ, mgmt);
  }

  (*rsp)->num = taskNum;

  return TSDB_CODE_SUCCESS;
}



int32_t qwUpdateSchLastAccess(SQWorkerMgmt *mgmt, uint64_t schedulerId) {
  SQWorkerSchStatus *schStatus = NULL;

  QW_ERR_RET(qwAcquireScheduler(QW_READ, mgmt, schedulerId, &schStatus));

  schStatus->lastAccessTs = taosGetTimestampSec();

  qwReleaseScheduler(QW_READ, mgmt);

  return TSDB_CODE_SUCCESS;
}


int32_t qwGetTaskStatus(SQWorkerMgmt *mgmt, uint64_t schedulerId, uint64_t queryId, uint64_t taskId, int8_t *taskStatus) {
  SQWorkerSchStatus *sch = NULL;
  SQWorkerTaskStatus *task = NULL;
  int32_t code = 0;
  
  QW_ERR_RET(qwAcquireScheduler(QW_READ, mgmt, schedulerId, &sch));

  QW_ERR_JRET(qwAcquireTask(QW_READ, sch, queryId, taskId, &task));

  *taskStatus = task->status;

_return:
  if (task) {
    qwReleaseTask(QW_READ, sch);
  }

  if (sch) {
    qwReleaseScheduler(QW_READ, mgmt);
  }

  QW_RET(code);
}


int32_t qwSwitchTaskStatus(SQWorkerMgmt *mgmt, uint64_t schedulerId, uint64_t queryId, uint64_t taskId, int8_t taskStatus) {
  SQWorkerSchStatus *sch = NULL;
  SQWorkerTaskStatus *task = NULL;
  int32_t code = 0;
  bool inserted = false;
  
  if (qwAcquireScheduler(QW_READ, mgmt, schedulerId, &sch)) {
    if (qwCheckStatusSwitch(JOB_TASK_STATUS_NULL, taskStatus)) {
      qError("switch status error, not start to %d", taskStatus);
      QW_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
    }
    
    QW_ERR_RET(qwInsertAndAcquireScheduler(QW_READ, mgmt, schedulerId, &sch));
  }

  if (qwAcquireTask(QW_READ, sch, queryId, taskId, &task)) {
    if (qwCheckStatusSwitch(JOB_TASK_STATUS_NOT_START, taskStatus)) {
      qwReleaseScheduler(QW_READ, mgmt);        
      qError("switch status error, not start to %d", taskStatus);
      QW_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
    }
  
    QW_ERR_JRET(qwInsertAndAcquireTask(QW_READ, sch, queryId, taskId, taskStatus, &inserted, &task));
    
    if (inserted) {
      qwReleaseTask(QW_READ, sch);
      qwReleaseScheduler(QW_READ, mgmt);
      return TSDB_CODE_SUCCESS;
    }

    QW_LOCK(QW_WRITE, &task->lock);
    code = qwUpdateTaskInfo(task, QW_TASK_INFO_STATUS, &taskStatus);
    QW_UNLOCK(QW_WRITE, &task->lock);

    qwReleaseTask(QW_READ, sch);
    qwReleaseScheduler(QW_READ, mgmt);    
    
    QW_RET(code);
  }

  QW_LOCK(QW_WRITE, &task->lock);
  code = qwUpdateTaskInfo(task, QW_TASK_INFO_STATUS, &taskStatus);
  QW_UNLOCK(QW_WRITE, &task->lock);

_return:

  qwReleaseTask(QW_READ, sch);
  qwReleaseScheduler(QW_READ, mgmt);    
  
  QW_RET(code);
}


int32_t qwCancelTask(SQWorkerMgmt *mgmt, uint64_t schedulerId, uint64_t queryId, uint64_t taskId) {
  SQWorkerSchStatus *sch = NULL;
  SQWorkerTaskStatus *task = NULL;
  int32_t code = 0;

  if (TSDB_CODE_SUCCESS != qwAcquireScheduler(QW_READ, mgmt, schedulerId, &sch)) {
    QW_ERR_RET(qwSwitchTaskStatus(mgmt, schedulerId, queryId, taskId, JOB_TASK_STATUS_NOT_START));
    
    QW_ERR_RET(qwAcquireScheduler(QW_READ, mgmt, schedulerId, &sch));
  }

  if (qwAcquireTask(QW_READ, sch, queryId, taskId, &task)) {
    code = qwSwitchTaskStatus(mgmt, schedulerId, queryId, taskId, JOB_TASK_STATUS_NOT_START);
    if (code) {
      qwReleaseScheduler(QW_READ, mgmt);
      QW_ERR_RET(code);
    }

    QW_ERR_JRET(qwAcquireTask(QW_READ, sch, queryId, taskId, &task));
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
    QW_ERR_JRET(qwUpdateTaskInfo(task, QW_TASK_INFO_STATUS, &newStatus));
  } else {
    newStatus = JOB_TASK_STATUS_CANCELLING;
    QW_ERR_JRET(qwUpdateTaskInfo(task, QW_TASK_INFO_STATUS, &newStatus));
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



int32_t qwDropTask(SQWorkerMgmt *mgmt, uint64_t schedulerId, uint64_t queryId, uint64_t taskId) {
  SQWorkerSchStatus *sch = NULL;
  SQWorkerTaskStatus *task = NULL;
  int32_t code = 0;
  char id[sizeof(queryId) + sizeof(taskId)] = {0};
  QW_SET_QTID(id, queryId, taskId);

  QW_LOCK(QW_WRITE, &mgmt->resLock);
  if (mgmt->resHash) {
    taosHashRemove(mgmt->resHash, id, sizeof(id));
  }
  QW_UNLOCK(QW_WRITE, &mgmt->resLock);
  
  if (TSDB_CODE_SUCCESS != qwAcquireScheduler(QW_WRITE, mgmt, schedulerId, &sch)) {
    qWarn("scheduler %"PRIx64" doesn't exist", schedulerId);
    return TSDB_CODE_SUCCESS;
  }

  if (qwAcquireTask(QW_WRITE, sch, queryId, taskId, &task)) {
    qwReleaseScheduler(QW_WRITE, mgmt);
    
    qWarn("scheduler %"PRIx64" queryId %"PRIx64" taskId:%"PRIx64" doesn't exist", schedulerId, queryId, taskId);
    return TSDB_CODE_SUCCESS;
  }

  taosHashRemove(sch->tasksHash, id, sizeof(id));

  qwReleaseTask(QW_WRITE, sch);
  qwReleaseScheduler(QW_WRITE, mgmt);
  
  return TSDB_CODE_SUCCESS;
}


int32_t qwCancelDropTask(SQWorkerMgmt *mgmt, uint64_t schedulerId, uint64_t queryId, uint64_t taskId) {
  SQWorkerSchStatus *sch = NULL;
  SQWorkerTaskStatus *task = NULL;
  int32_t code = 0;

  if (TSDB_CODE_SUCCESS != qwAcquireScheduler(QW_READ, mgmt, schedulerId, &sch)) {
    qWarn("scheduler %"PRIx64" doesn't exist", schedulerId);
    return TSDB_CODE_SUCCESS;
  }

  if (qwAcquireTask(QW_READ, sch, queryId, taskId, &task)) {
    qwReleaseScheduler(QW_READ, mgmt);
    
    qWarn("scheduler %"PRIx64" queryId %"PRIx64" taskId:%"PRIx64" doesn't exist", schedulerId, queryId, taskId);
    return TSDB_CODE_SUCCESS;
  }

  QW_LOCK(QW_WRITE, &task->lock);

  task->drop = true;

  int8_t oriStatus = task->status;
  int8_t newStatus = 0;
  
  if (task->status == JOB_TASK_STATUS_EXECUTING) {
    newStatus = JOB_TASK_STATUS_CANCELLING;
    QW_ERR_JRET(qwUpdateTaskInfo(task, QW_TASK_INFO_STATUS, &newStatus));
  } else if (task->status == JOB_TASK_STATUS_CANCELLING || task->status == JOB_TASK_STATUS_DROPPING || task->status == JOB_TASK_STATUS_NOT_START) {    
    QW_UNLOCK(QW_WRITE, &task->lock);
    qwReleaseTask(QW_READ, sch);
    qwReleaseScheduler(QW_READ, mgmt);
    
    return TSDB_CODE_SUCCESS;
  } else {
    QW_UNLOCK(QW_WRITE, &task->lock);
    qwReleaseTask(QW_READ, sch);
    qwReleaseScheduler(QW_READ, mgmt);
  
    QW_ERR_RET(qwDropTask(mgmt, schedulerId, queryId, taskId));
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
    .handle  = pMsg->handle,
    .ahandle = pMsg->ahandle,
    .pCont   = pRsp,
    .contLen = size,
    .code    = 0,
  };

  rpcSendResponse(&rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendFetchRsp(SRpcMsg *pMsg, void *data) {
  SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)rpcMallocCont(sizeof(SRetrieveTableRsp));
  memset(pRsp, 0, sizeof(SRetrieveTableRsp));

  //TODO fill msg
  pRsp->completed = true;

  SRpcMsg rpcRsp = {
    .handle  = pMsg->handle,
    .ahandle = pMsg->ahandle,
    .pCont   = pRsp,
    .contLen = sizeof(*pRsp),
    .code    = 0,
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



int32_t qwCheckAndSendReadyRsp(SQWorkerMgmt *mgmt, uint64_t schedulerId, uint64_t queryId, uint64_t taskId, SRpcMsg *pMsg, int32_t rspCode) {
  SQWorkerSchStatus *sch = NULL;
  SQWorkerTaskStatus *task = NULL;
  int32_t code = 0;

  QW_ERR_RET(qwAcquireScheduler(QW_READ, mgmt, schedulerId, &sch));

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
  }

  if (sch) {
    qwReleaseTask(QW_READ, sch);
  }

  qwReleaseScheduler(QW_READ, mgmt);

  QW_RET(code);
}

int32_t qwSetAndSendReadyRsp(SQWorkerMgmt *mgmt, uint64_t schedulerId, uint64_t queryId, uint64_t taskId, SRpcMsg *pMsg) {
  SQWorkerSchStatus *sch = NULL;
  SQWorkerTaskStatus *task = NULL;
  int32_t code = 0;

  QW_ERR_RET(qwAcquireScheduler(QW_READ, mgmt, schedulerId, &sch));

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
  }

  if (sch) {
    qwReleaseTask(QW_READ, sch);
  }

  qwReleaseScheduler(QW_READ, mgmt);

  QW_RET(code);
}

int32_t qwCheckTaskCancelDrop( SQWorkerMgmt *mgmt, uint64_t schedulerId, uint64_t queryId, uint64_t taskId, bool *needStop) {
  SQWorkerSchStatus *sch = NULL;
  SQWorkerTaskStatus *task = NULL;
  int32_t code = 0;
  int8_t status = JOB_TASK_STATUS_CANCELLED;

  *needStop = false;

  if (qwAcquireScheduler(QW_READ, mgmt, schedulerId, &sch)) {
    return TSDB_CODE_SUCCESS;
  }

  if (qwAcquireTask(QW_READ, sch, queryId, taskId, &task)) {
    qwReleaseScheduler(QW_READ, mgmt);
    return TSDB_CODE_SUCCESS;
  }

  QW_LOCK(QW_READ, &task->lock);
  
  if ((!task->cancel) && (!task->drop)) {
    QW_UNLOCK(QW_READ, &task->lock);
    qwReleaseTask(QW_READ, sch);
    qwReleaseScheduler(QW_READ, mgmt);

    return TSDB_CODE_SUCCESS;
  }

  QW_UNLOCK(QW_READ, &task->lock);

  *needStop = true;
  
  if (task->cancel) {
    QW_LOCK(QW_WRITE, &task->lock);
    qwUpdateTaskInfo(task, QW_TASK_INFO_STATUS, &status);
    QW_UNLOCK(QW_WRITE, &task->lock);
  } else if (task->drop) {
    qwReleaseTask(QW_READ, sch);
    qwReleaseScheduler(QW_READ, mgmt);
    
    qwDropTask(mgmt, schedulerId, queryId, taskId);
  }

  return TSDB_CODE_SUCCESS;
}


int32_t qwHandleFetch(SQWorkerResCache *res, SQWorkerMgmt *mgmt, uint64_t schedulerId, uint64_t queryId, uint64_t taskId, SRpcMsg *pMsg) {
  SQWorkerSchStatus *sch = NULL;
  SQWorkerTaskStatus *task = NULL;
  int32_t code = 0;
  int32_t needRsp = true;
  void *data = NULL;

  QW_ERR_JRET(qwAcquireScheduler(QW_READ, mgmt, schedulerId, &sch));
  QW_ERR_JRET(qwAcquireTask(QW_READ, sch, queryId, taskId, &task));

  QW_LOCK(QW_READ, &task->lock);

  if (task->status != JOB_TASK_STATUS_EXECUTING && task->status != JOB_TASK_STATUS_PARTIAL_SUCCEED && task->status != JOB_TASK_STATUS_SUCCEED) {
    qError("invalid status %d for fetch", task->status);
    QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
  }
  
  if (QW_GOT_RES_DATA(res->data)) {
    data = res->data;
    if (QW_LOW_RES_DATA(res->data)) {
      if (task->status == JOB_TASK_STATUS_PARTIAL_SUCCEED) {
        //TODO add query back to queue
      }
    }
  } else {
    if (task->status != JOB_TASK_STATUS_EXECUTING) {
      qError("invalid status %d for fetch without res", task->status);
      QW_ERR_JRET(TSDB_CODE_QRY_APP_ERROR);
    }
    
    //TODO SET FLAG FOR QUERY TO SEND RSP WHEN RES READY

    needRsp = false;
  }

_return:
  if (task) {
    QW_UNLOCK(QW_READ, &task->lock);
  }
  
  if (sch) {
    qwReleaseTask(QW_READ, sch);
    qwReleaseScheduler(QW_READ, mgmt);
  }

  if (needRsp) {
    qwBuildAndSendFetchRsp(pMsg, res->data);
  }

  QW_RET(code);
}

int32_t qwQueryPostProcess(SQWorkerMgmt *mgmt, uint64_t schedulerId, uint64_t queryId, uint64_t taskId, int8_t status, int32_t errCode) {
  SQWorkerSchStatus *sch = NULL;
  SQWorkerTaskStatus *task = NULL;
  int32_t code = 0;
  int8_t newStatus = JOB_TASK_STATUS_CANCELLED;

  code = qwAcquireScheduler(QW_READ, mgmt, schedulerId, &sch);
  if (code) {
    qError("schedulerId:%"PRIx64" not in cache", schedulerId);
    QW_ERR_RET(code);
  }

  code = qwAcquireTask(QW_READ, sch, queryId, taskId, &task);
  if (code) {
    qwReleaseScheduler(QW_READ, mgmt);
    qError("schedulerId:%"PRIx64" queryId:%"PRIx64" taskId:%"PRIx64" not in cache", schedulerId, queryId, taskId);
    QW_ERR_RET(code);
  }

  if (task->cancel) {
    QW_LOCK(QW_WRITE, &task->lock);
    qwUpdateTaskInfo(task, QW_TASK_INFO_STATUS, &newStatus);
    QW_UNLOCK(QW_WRITE, &task->lock);
  } else if (task->drop) {
    qwReleaseTask(QW_READ, sch);
    qwReleaseScheduler(QW_READ, mgmt);
    
    qwDropTask(mgmt, schedulerId, queryId, taskId);

    return TSDB_CODE_SUCCESS;
  } else {
    QW_LOCK(QW_WRITE, &task->lock);
    qwUpdateTaskInfo(task, QW_TASK_INFO_STATUS, &status);
    task->code = errCode;
    QW_UNLOCK(QW_WRITE, &task->lock);
  }
  
  qwReleaseTask(QW_READ, sch);
  qwReleaseScheduler(QW_READ, mgmt);

  return TSDB_CODE_SUCCESS;
}


int32_t qWorkerInit(SQWorkerCfg *cfg, void **qWorkerMgmt) {
  SQWorkerMgmt *mgmt = calloc(1, sizeof(SQWorkerMgmt));
  if (NULL == mgmt) {
    qError("calloc %d failed", (int32_t)sizeof(SQWorkerMgmt));
    QW_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  if (cfg) {
    mgmt->cfg = *cfg;
  } else {
    mgmt->cfg.maxSchedulerNum = QWORKER_DEFAULT_SCHEDULER_NUMBER;
    mgmt->cfg.maxResCacheNum = QWORKER_DEFAULT_RES_CACHE_NUMBER;
    mgmt->cfg.maxSchTaskNum = QWORKER_DEFAULT_SCH_TASK_NUMBER;
  }

  mgmt->schHash = taosHashInit(mgmt->cfg.maxSchedulerNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_NO_LOCK);
  if (NULL == mgmt->schHash) {
    tfree(mgmt);
    QW_ERR_LRET(TSDB_CODE_QRY_OUT_OF_MEMORY, "init %d schduler hash failed", mgmt->cfg.maxSchedulerNum);
  }

  mgmt->resHash = taosHashInit(mgmt->cfg.maxResCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (NULL == mgmt->resHash) {
    taosHashCleanup(mgmt->schHash);
    mgmt->schHash = NULL;
    tfree(mgmt);
    
    QW_ERR_LRET(TSDB_CODE_QRY_OUT_OF_MEMORY, "init %d res cache hash failed", mgmt->cfg.maxResCacheNum);
  }

  *qWorkerMgmt = mgmt;

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessQueryMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SSubQueryMsg *msg = pMsg->pCont;
  if (NULL == msg || pMsg->contLen <= sizeof(*msg)) {
    qError("invalid query msg");
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  msg->schedulerId = htobe64(msg->schedulerId);
  msg->queryId = htobe64(msg->queryId);
  msg->taskId = htobe64(msg->taskId);
  msg->contentLen = ntohl(msg->contentLen);
  
  bool queryDone = false;
  bool queryRsp = false;
  bool needStop = false;
  SSubplan *plan = NULL;
  int32_t code = 0;

  QW_ERR_JRET(qwCheckTaskCancelDrop(qWorkerMgmt, msg->schedulerId, msg->queryId, msg->taskId, &needStop));
  if (needStop) {
    qWarn("task need stop");
    QW_ERR_JRET(TSDB_CODE_QRY_TASK_CANCELLED);
  }
  
  code = qStringToSubplan(msg->msg, &plan);
  if (TSDB_CODE_SUCCESS != code) {
    qError("schId:%"PRIx64",qId:%"PRIx64",taskId:%"PRIx64" string to subplan failed, code:%d", msg->schedulerId, msg->queryId, msg->taskId, code);
    QW_ERR_JRET(code);
  }

  //TODO call executer to init subquery
  code = 0; // return error directly
  //TODO call executer to init subquery
  
  if (code) {
    QW_ERR_JRET(code);
  } else {
    QW_ERR_JRET(qwSwitchTaskStatus(qWorkerMgmt, msg->schedulerId, msg->queryId, msg->taskId, JOB_TASK_STATUS_EXECUTING));
  }

  QW_ERR_JRET(qwBuildAndSendQueryRsp(pMsg, TSDB_CODE_SUCCESS));

  queryRsp = true;
 
  //TODO call executer to execute subquery
  code = 0; 
  void *data = NULL;
  queryDone = false;
  //TODO call executer to execute subquery

  if (code) {
    QW_ERR_JRET(code);
  } else {
    QW_ERR_JRET(qwAddTaskResult(qWorkerMgmt, msg->queryId, msg->taskId, data));

    QW_ERR_JRET(qwSwitchTaskStatus(qWorkerMgmt, msg->schedulerId, msg->queryId, msg->taskId, JOB_TASK_STATUS_PARTIAL_SUCCEED));
  }

_return:

  if (queryRsp) {
    code = qwCheckAndSendReadyRsp(qWorkerMgmt, msg->schedulerId, msg->queryId, msg->taskId, pMsg, code);
  } else {
    code = qwBuildAndSendQueryRsp(pMsg, code);
  }
  
  int8_t status = 0;
  if (TSDB_CODE_SUCCESS != code || queryDone) {
    if (code) {
      status = JOB_TASK_STATUS_FAILED; //TODO set CANCELLED from code
    } else {
      status = JOB_TASK_STATUS_SUCCEED;
    }

    qwQueryPostProcess(qWorkerMgmt, msg->schedulerId, msg->queryId, msg->taskId, status, code);
  }
  
  QW_RET(code);
}

int32_t qWorkerProcessReadyMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg){
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SResReadyMsg *msg = pMsg->pCont;
  if (NULL == msg || pMsg->contLen <= sizeof(*msg)) {
    qError("invalid task status msg");  
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }  

  QW_ERR_RET(qwSetAndSendReadyRsp(qWorkerMgmt, msg->schedulerId, msg->queryId, msg->taskId, pMsg));
  
  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessStatusMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  int32_t code = 0;
  SSchTasksStatusMsg *msg = pMsg->pCont;
  if (NULL == msg || pMsg->contLen <= sizeof(*msg)) {
    qError("invalid task status msg");
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }  

  SSchedulerStatusRsp *sStatus = NULL;
  
  QW_ERR_JRET(qwGetSchTasksStatus(qWorkerMgmt, msg->schedulerId, &sStatus));

_return:

  QW_ERR_RET(qwBuildAndSendStatusRsp(pMsg, sStatus));

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessFetchMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SResFetchMsg *msg = pMsg->pCont;
  if (NULL == msg || pMsg->contLen <= sizeof(*msg)) {
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }  

  QW_ERR_RET(qwUpdateSchLastAccess(qWorkerMgmt, msg->schedulerId));

  void *data = NULL;
  SQWorkerResCache *res = NULL;
  int32_t code = 0;
  
  QW_ERR_RET(qwAcquireTaskResCache(QW_READ, qWorkerMgmt, msg->queryId, msg->taskId, &res));

  QW_ERR_JRET(qwHandleFetch(res, qWorkerMgmt, msg->schedulerId, msg->queryId, msg->taskId, pMsg));

_return:

  qwReleaseTaskResCache(QW_READ, qWorkerMgmt);
  
  QW_RET(code);
}

int32_t qWorkerProcessCancelMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  int32_t code = 0;
  STaskCancelMsg *msg = pMsg->pCont;
  if (NULL == msg || pMsg->contLen <= sizeof(*msg)) {
    qError("invalid task cancel msg");  
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }  

  QW_ERR_JRET(qwCancelTask(qWorkerMgmt, msg->schedulerId, msg->queryId, msg->taskId));

_return:

  QW_ERR_RET(qwBuildAndSendCancelRsp(pMsg, code));

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessDropMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  int32_t code = 0;
  STaskDropMsg *msg = pMsg->pCont;
  if (NULL == msg || pMsg->contLen <= sizeof(*msg)) {
    qError("invalid task drop msg");
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }  

  QW_ERR_JRET(qwCancelDropTask(qWorkerMgmt, msg->schedulerId, msg->queryId, msg->taskId));

_return:

  QW_ERR_RET(qwBuildAndSendDropRsp(pMsg, code));

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


