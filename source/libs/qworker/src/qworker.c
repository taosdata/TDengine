#include "taosmsg.h"
#include "query.h"
#include "qworker.h"
#include "qworkerInt.h"
#include "planner.h"

int32_t qwAddTaskStatus(SQWorkerMgmt *mgmt, uint64_t schedulerId, uint64_t queryId, uint64_t taskId, int8_t taskStatus) {
  SQWorkerTaskStatus tStatus = {0};
  tStatus.status = taskStatus;
  
  char id[sizeof(queryId) + sizeof(taskId)] = {0};
  QW_SET_QTID(id, queryId, taskId);

  SQWorkerSchTaskStatus *schStatus = taosHashGet(mgmt->scheduleHash, &schedulerId, sizeof(schedulerId));
  if (NULL == schStatus) {
    SQWorkerSchTaskStatus newSchStatus = {0};
    newSchStatus.taskStatus = taosHashInit(mgmt->cfg.maxSchTaskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    if (NULL == newSchStatus.taskStatus) {
      qError("taosHashInit %d failed", mgmt->cfg.maxSchTaskNum);
      return TSDB_CODE_QRY_OUT_OF_MEMORY;
    }

    if (0 != taosHashPut(newSchStatus.taskStatus, id, sizeof(id), &tStatus, sizeof(tStatus))) {
      qError("taosHashPut schedulerId[%"PRIx64"]queryId[%"PRIx64"] taskId[%"PRIx64"] to scheduleHash failed", schedulerId, queryId, taskId);
      taosHashCleanup(newSchStatus.taskStatus);
      return TSDB_CODE_QRY_APP_ERROR;
    }

    newSchStatus.lastAccessTs = taosGetTimestampSec();

    if (0 != taosHashPut(mgmt->scheduleHash, &schedulerId, sizeof(schedulerId), &newSchStatus, sizeof(newSchStatus))) {
      qError("taosHashPut schedulerId[%"PRIx64"] to scheduleHash failed", schedulerId);
      taosHashCleanup(newSchStatus.taskStatus);
      return TSDB_CODE_QRY_APP_ERROR;
    }

    return TSDB_CODE_SUCCESS;
  }

  schStatus->lastAccessTs = taosGetTimestampSec();

  if (0 != taosHashPut(schStatus->taskStatus, id, sizeof(id), &tStatus, sizeof(tStatus))) {
    qError("taosHashPut schedulerId[%"PRIx64"]queryId[%"PRIx64"] taskId[%"PRIx64"] to scheduleHash failed", schedulerId, queryId, taskId);
    return TSDB_CODE_QRY_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qwGetTaskStatus(SQWorkerMgmt *mgmt, uint64_t schedulerId, uint64_t queryId, uint64_t taskId, SQWorkerTaskStatus **taskStatus) {
  SQWorkerSchTaskStatus *schStatus = taosHashGet(mgmt->scheduleHash, &schedulerId, sizeof(schedulerId));
  if (NULL == schStatus) {
    qError("no scheduler for schedulerId[%"PRIx64"]", schedulerId);
    return TSDB_CODE_QRY_APP_ERROR;
  }

  schStatus->lastAccessTs = taosGetTimestampSec();
  
  char id[sizeof(queryId) + sizeof(taskId)] = {0};
  QW_SET_QTID(id, queryId, taskId);

  SQWorkerTaskStatus *tStatus = taosHashGet(schStatus->taskStatus, id, sizeof(id));
  if (NULL == tStatus) {
    qError("no task status for schedulerId[%"PRIx64"] queryId[%"PRIx64"] taskId[%"PRIx64"]", schedulerId, queryId, taskId);
    return TSDB_CODE_QRY_APP_ERROR;
  }

  *taskStatus = tStatus;

  return TSDB_CODE_SUCCESS;
}

int32_t qwAddTaskResult(SQWorkerMgmt *mgmt, uint64_t queryId, uint64_t taskId, void *data) {
  char id[sizeof(queryId) + sizeof(taskId)] = {0};
  QW_SET_QTID(id, queryId, taskId);

  SQWorkerResCache resCache = {0};
  resCache.data = data;
  
  if (0 != taosHashPut(mgmt->resHash, id, sizeof(id), &resCache, sizeof(SQWorkerResCache))) {
    qError("taosHashPut queryId[%"PRIx64"] taskId[%"PRIx64"] to resHash failed", queryId, taskId);
    return TSDB_CODE_QRY_APP_ERROR;
  }

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

int32_t qwUpdateSchLastAccess(SQWorkerMgmt *mgmt, uint64_t schedulerId) {
  SQWorkerSchTaskStatus *schStatus = taosHashGet(mgmt->scheduleHash, &schedulerId, sizeof(schedulerId));
  if (NULL == schStatus) {
    qError("no scheduler for schedulerId[%"PRIx64"]", schedulerId);
    return TSDB_CODE_QRY_APP_ERROR;
  }

  schStatus->lastAccessTs = taosGetTimestampSec();

  return TSDB_CODE_SUCCESS;
}

int32_t qwGetSchTasksStatus(SQWorkerMgmt *mgmt, uint64_t schedulerId, SSchedulerStatusRsp **rsp) {
  SQWorkerSchTaskStatus *schStatus = taosHashGet(mgmt->scheduleHash, &schedulerId, sizeof(schedulerId));
  if (NULL == schStatus) {
    qError("no scheduler for schedulerId[%"PRIx64"]", schedulerId);
    return TSDB_CODE_QRY_APP_ERROR;
  }

  schStatus->lastAccessTs = taosGetTimestampSec();

  int32_t i = 0;
  int32_t taskNum = taosHashGetSize(schStatus->taskStatus);
  int32_t size = sizeof(SSchedulerStatusRsp) + sizeof((*rsp)->status[0]) * taskNum;
  *rsp = calloc(1, size);
  if (NULL == *rsp) {
    qError("calloc %d failed", size);
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  void *key = NULL;
  size_t keyLen = 0;
  void *pIter = taosHashIterate(schStatus->taskStatus, NULL);
  while (pIter) {
    SQWorkerTaskStatus *taskStatus = (SQWorkerTaskStatus *)pIter;
    taosHashGetKey(pIter, &key, &keyLen);

    QW_GET_QTID(key, (*rsp)->status[i].queryId, (*rsp)->status[i].taskId);
    (*rsp)->status[i].status = taskStatus->status;
    
    pIter = taosHashIterate(schStatus->taskStatus, pIter);
  }  

  (*rsp)->num = taskNum;

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildRspMsg(void *data, int32_t msgType);


int32_t qWorkerInit(SQWorkerCfg *cfg, void **qWorkerMgmt) {
  SQWorkerMgmt *mgmt = calloc(1, sizeof(SQWorkerMgmt));
  if (NULL == mgmt) {
    qError("calloc %d failed", (int32_t)sizeof(SQWorkerMgmt));
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  if (cfg) {
    mgmt->cfg = *cfg;
  } else {
    mgmt->cfg.maxSchedulerNum = QWORKER_DEFAULT_SCHEDULER_NUMBER;
    mgmt->cfg.maxResCacheNum = QWORKER_DEFAULT_RES_CACHE_NUMBER;
    mgmt->cfg.maxSchTaskNum = QWORKER_DEFAULT_SCH_TASK_NUMBER;
  }

  mgmt->scheduleHash = taosHashInit(mgmt->cfg.maxSchedulerNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == mgmt->scheduleHash) {
    tfree(mgmt);
    QW_ERR_LRET(TSDB_CODE_QRY_OUT_OF_MEMORY, "init %d schduler hash failed", mgmt->cfg.maxSchedulerNum);
  }

  mgmt->resHash = taosHashInit(mgmt->cfg.maxResCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == mgmt->resHash) {
    taosHashCleanup(mgmt->scheduleHash);
    mgmt->scheduleHash = NULL;
    tfree(mgmt);
    
    QW_ERR_LRET(TSDB_CODE_QRY_OUT_OF_MEMORY, "init %d res cache hash failed", mgmt->cfg.maxResCacheNum);
  }

  *qWorkerMgmt = mgmt;

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessQueryMsg(void *qWorkerMgmt, SSchedulerQueryMsg *msg, SRpcMsg *rsp) {
  if (NULL == qWorkerMgmt || NULL == msg || NULL == rsp) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SSubplan *plan = NULL;
  SQWorkerTaskStatus *tStatus = NULL;

  int32_t code = qStringToSubplan(msg->msg, &plan);
  if (TSDB_CODE_SUCCESS != code) {
    qError("schId:%"PRIx64",qId:%"PRIx64",taskId:%"PRIx64" string to subplan failed, code:%d", msg->schedulerId, msg->queryId, msg->taskId, code);
    return code;
  }
  
  //TODO call executer to init subquery

  QW_ERR_JRET(qwAddTaskStatus(qWorkerMgmt, msg->schedulerId, msg->queryId, msg->taskId, JOB_TASK_STATUS_EXECUTING));

  QW_ERR_JRET(qwBuildRspMsg(NULL, TSDB_MSG_TYPE_QUERY_RSP));

  //TODO call executer to execute subquery
  code = 0;
  void *data = NULL;
  //TODO call executer to execute subquery

  QW_ERR_JRET(qwGetTaskStatus(qWorkerMgmt, msg->schedulerId, msg->queryId, msg->taskId, &tStatus));

  tStatus->status = (code) ? JOB_TASK_STATUS_FAILED : JOB_TASK_STATUS_SUCCEED;

  QW_ERR_JRET(qwAddTaskResult(qWorkerMgmt, msg->queryId, msg->taskId, data));

_return:

  if (tStatus && QW_TASK_DONE(tStatus->status) && QW_READY_RECEIVED == tStatus->ready) {
    QW_ERR_RET(qwBuildRspMsg(NULL, TSDB_MSG_TYPE_RES_READY_RSP));
  }

  qDestroySubplan(plan);
  
  return code;
}

int32_t qWorkerProcessReadyMsg(void *qWorkerMgmt, SSchedulerReadyMsg *msg, SRpcMsg *rsp){
  if (NULL == qWorkerMgmt || NULL == msg || NULL == rsp) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SQWorkerTaskStatus *tStatus = NULL;
  
  QW_ERR_RET(qwGetTaskStatus(qWorkerMgmt, msg->schedulerId, msg->queryId, msg->taskId, &tStatus));
  
  if (QW_TASK_DONE(tStatus->status)) {
    QW_ERR_RET(qwBuildRspMsg(tStatus, TSDB_MSG_TYPE_RES_READY_RSP));
  } else {
    tStatus->ready = QW_READY_RECEIVED;
    
    return TSDB_CODE_SUCCESS;
  }

  tStatus->ready = QW_READY_RESPONSED;
  
  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessStatusMsg(void *qWorkerMgmt, SSchedulerStatusMsg *msg, SRpcMsg *rsp) {
  if (NULL == qWorkerMgmt || NULL == msg || NULL == rsp) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SSchedulerStatusRsp *sStatus = NULL;
  
  QW_ERR_RET(qwGetSchTasksStatus(qWorkerMgmt, msg->schedulerId, &sStatus));

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessFetchMsg(void *qWorkerMgmt, SSchedulerFetchMsg *msg, SRpcMsg *rsp) {
  if (NULL == qWorkerMgmt || NULL == msg || NULL == rsp) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  QW_ERR_RET(qwUpdateSchLastAccess(qWorkerMgmt, msg->schedulerId));

  void *data = NULL;
  
  QW_ERR_RET(qwGetTaskResult(qWorkerMgmt, msg->queryId, msg->taskId, &data));

  QW_ERR_RET(qwBuildRspMsg(data, TSDB_MSG_TYPE_FETCH_RSP));

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessCancelMsg(void *qWorkerMgmt, SSchedulerCancelMsg *msg, SRpcMsg *rsp);

void qWorkerDestroy(void **qWorkerMgmt) {
  if (NULL == qWorkerMgmt || NULL == *qWorkerMgmt) {
    return;
  }

  SQWorkerMgmt *mgmt = *qWorkerMgmt;
  
  //TODO STOP ALL QUERY

  //TODO FREE ALL

  tfree(*qWorkerMgmt);
}


