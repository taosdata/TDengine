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

#include "schedulerInt.h"
#include "taosmsg.h"
#include "query.h"
#include "catalog.h"

SSchedulerMgmt schMgmt = {0};


int32_t schBuildAndSendRequest(void *pRpc, const SEpSet* pMgmtEps, __taos_async_fn_t fp) {
/*
  SRequestObj *pRequest = createRequest(pTscObj, fp, param, TSDB_SQL_CONNECT);
  if (pRequest == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SRequestMsgBody body = {0};
  buildConnectMsg(pRequest, &body);

  int64_t transporterId = 0;
  sendMsgToServer(pTscObj->pTransporter, &pTscObj->pAppInfo->mgmtEp.epSet, &body, &transporterId);

  tsem_wait(&pRequest->body.rspSem);
  destroyConnectMsg(&body);

  if (pRequest->code != TSDB_CODE_SUCCESS) {
    const char *errorMsg = (pRequest->code == TSDB_CODE_RPC_FQDN_ERROR) ? taos_errstr(pRequest) : tstrerror(terrno);
    printf("failed to connect to server, reason: %s\n\n", errorMsg);

    destroyRequest(pRequest);
    taos_close(pTscObj);
    pTscObj = NULL;
  } else {
    tscDebug("0x%"PRIx64" connection is opening, connId:%d, dnodeConn:%p", pTscObj->id, pTscObj->connId, pTscObj->pTransporter);
    destroyRequest(pRequest);
  }
*/  
}

int32_t schBuildTaskRalation(SQueryJob *job, SHashObj *planToTask) {
  for (int32_t i = 0; i < job->levelNum; ++i) {
    SQueryLevel *level = taosArrayGet(job->levels, i);
    
    for (int32_t m = 0; m < level->taskNum; ++m) {
      SQueryTask *task = taosArrayGet(level->subTasks, m);
      SSubplan *plan = task->plan;
      int32_t childNum = (int32_t)taosArrayGetSize(plan->pChildern);
      int32_t parentNum = (int32_t)taosArrayGetSize(plan->pParents);

      if (childNum > 0) {
        task->children = taosArrayInit(childNum, POINTER_BYTES);
        if (NULL == task->children) {
          qError("taosArrayInit %d failed", childNum);
          SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
        }
      }

      for (int32_t n = 0; n < childNum; ++n) {
        SSubplan *child = taosArrayGet(plan->pChildern, n);
        SQueryTask *childTask = taosHashGet(planToTask, &child, POINTER_BYTES);
        if (childTask) {
          qError("subplan relationship error, level:%d, taskIdx:%d, childIdx:%d", i, m, n);
          SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
        }

        if (NULL == taosArrayPush(task->children, &childTask)) {
          qError("taosArrayPush failed");
          SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
        }
      }

      if (parentNum > 0) {
        task->parents = taosArrayInit(parentNum, POINTER_BYTES);
        if (NULL == task->parents) {
          qError("taosArrayInit %d failed", parentNum);
          SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
        }
      }

      for (int32_t n = 0; n < parentNum; ++n) {
        SSubplan *parent = taosArrayGet(plan->pParents, n);
        SQueryTask *parentTask = taosHashGet(planToTask, &parent, POINTER_BYTES);
        if (parentTask) {
          qError("subplan relationship error, level:%d, taskIdx:%d, childIdx:%d", i, m, n);
          SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
        }

        if (NULL == taosArrayPush(task->parents, &parentTask)) {
          qError("taosArrayPush failed");
          SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
        }
      }      
    }
  }

  SQueryLevel *level = taosArrayGet(job->levels, 0);
  if (level->taskNum > 1) {
    qError("invalid plan info, level 0, taskNum:%d", level->taskNum);
    SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
  }

  SQueryTask *task = taosArrayGet(level->subTasks, 0);
  if (task->parents && taosArrayGetSize(task->parents) > 0) {
    qError("invalid plan info, level 0, parentNum:%d", (int32_t)taosArrayGetSize(task->parents));
    SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
  }


  return TSDB_CODE_SUCCESS;
}


int32_t schValidateAndBuildJob(SQueryDag *dag, SQueryJob *job) {
  int32_t code = 0;

  if (dag->numOfSubplans <= 0) {
    qError("invalid subplan num:%d", dag->numOfSubplans);
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }
  
  int32_t levelNum = (int32_t)taosArrayGetSize(dag->pSubplans);
  if (levelNum <= 0) {
    qError("invalid level num:%d", levelNum);
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SHashObj *planToTask = taosHashInit(SCHEDULE_DEFAULT_TASK_NUMBER, taosGetDefaultHashFunction(POINTER_BYTES == sizeof(int64_t) ? TSDB_DATA_TYPE_BIGINT : TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (NULL == planToTask) {
    qError("taosHashInit %d failed", SCHEDULE_DEFAULT_TASK_NUMBER);
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  
  job->levels = taosArrayInit(levelNum, sizeof(SQueryLevel));
  if (NULL == job->levels) {
    qError("taosArrayInit %d failed", levelNum);
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  job->levelNum = levelNum;
  job->levelIdx = levelNum - 1;
  job->status = SCH_STATUS_NOT_START;

  job->subPlans = dag->pSubplans;

  SQueryLevel level = {0};
  SArray *levelPlans = NULL;
  int32_t levelPlanNum = 0;

  level.status = SCH_STATUS_NOT_START;

  for (int32_t i = 0; i < levelNum; ++i) {
    level.level = i;
    levelPlans = taosArrayGetP(dag->pSubplans, i);
    if (NULL == levelPlans) {
      qError("no level plans for level %d", i);
      SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
    }

    levelPlanNum = (int32_t)taosArrayGetSize(levelPlans);
    if (levelPlanNum <= 0) {
      qError("invalid level plans number:%d, level:%d", levelPlanNum, i);
      SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
    }

    level.taskNum = levelPlanNum;
    
    level.subTasks = taosArrayInit(levelPlanNum, sizeof(SQueryTask));
    if (NULL == level.subTasks) {
      qError("taosArrayInit %d failed", levelPlanNum);
      SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
    
    for (int32_t n = 0; n < levelPlanNum; ++n) {
      SSubplan *plan = taosArrayGet(levelPlans, n);
      SQueryTask *task = taosArrayGet(level.subTasks, n);
      
      task->taskId = atomic_add_fetch_64(&schMgmt.taskId, 1);
      task->plan = plan;
      task->status = SCH_STATUS_NOT_START;

      if (0 != taosHashPut(planToTask, &plan, POINTER_BYTES, &task, POINTER_BYTES)) {
        qError("taosHashPut failed");
        SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }
    }

    if (NULL == taosArrayPush(job->levels, &level)) {
      qError("taosArrayPush failed");
      SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
  }

  SCH_ERR_JRET(schBuildTaskRalation(job, planToTask));

  if (planToTask) {
    taosHashCleanup(planToTask);
  }
  
  return TSDB_CODE_SUCCESS;

_return:
  if (level.subTasks) {
    taosArrayDestroy(level.subTasks);
  }

  if (planToTask) {
    taosHashCleanup(planToTask);
  }

  SCH_RET(code);
}

int32_t schAvailableEpSet(SQueryJob *job, SEpSet *epSet) {  
  if (epSet->numOfEps >= SCH_MAX_CONDIDATE_EP_NUM) {
    return TSDB_CODE_SUCCESS;
  }

  if (SCH_HAS_QNODE_IN_CLUSTER(schMgmt.cfg.clusterType)) {
    SCH_ERR_RET(catalogGetQnodeList(job->catalog, job->rpc, job->mgmtEpSet, epSet));
  } else {
    for (int32_t i = 0; i < job->dataSrcEps.numOfEps; ++i) {
      strncpy(epSet->fqdn[epSet->numOfEps], job->dataSrcEps.fqdn[i], sizeof(job->dataSrcEps.fqdn[i]));
      epSet->port[epSet->numOfEps] = job->dataSrcEps.port[i];
      
      ++epSet->numOfEps;
    }
  }

  return TSDB_CODE_SUCCESS;
}


int32_t schPushTaskToExecList(SQueryJob *job, SQueryTask *task) {
  if (0 != taosHashPut(job->execTasks, &task->taskId, sizeof(task->taskId), &task, POINTER_BYTES)) {
    qError("taosHashPut failed");
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schMoveTaskToSuccList(SQueryJob *job, SQueryTask *task, bool *moved) {
  if (0 != taosHashRemove(job->execTasks, &task->taskId, sizeof(task->taskId))) {
    qWarn("remove task[%"PRIx64"] from execTasks failed", task->taskId);
    return TSDB_CODE_SUCCESS;
  }

  if (0 != taosHashPut(job->execTasks, &task->taskId, sizeof(task->taskId), &task, POINTER_BYTES)) {
    qError("taosHashPut failed");
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  *moved = true;
  
  return TSDB_CODE_SUCCESS;
}


int32_t schAsyncSendMsg(SQueryJob *job, SQueryTask *task, int32_t msgType) {
  int32_t msgSize = 0;
  void *msg = NULL;
  
  switch (msgType) {
    case TSDB_MSG_TYPE_QUERY: {
      if (NULL == task->msg) {
        qError("query msg is NULL");
        SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
      }

      int32_t len = strlen(task->msg);
      msgSize = sizeof(SSchedulerQueryMsg) + len;
      msg = calloc(1, msgSize);
      if (NULL == msg) {
        qError("calloc %d failed", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      SSchedulerQueryMsg *pMsg = msg;
      pMsg->queryId = job->queryId;
      pMsg->taskId = task->taskId;
      pMsg->contentLen = len;
      memcpy(pMsg->msg, task->msg, len);
      break;
    }
    case TSDB_MSG_TYPE_RSP_READY: {
      msgSize = sizeof(SSchedulerReadyMsg);
      msg = calloc(1, msgSize);
      if (NULL == msg) {
        qError("calloc %d failed", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      SSchedulerReadyMsg *pMsg = msg;
      pMsg->queryId = job->queryId;
      pMsg->taskId = task->taskId;      
      break;
    }
    case TSDB_MSG_TYPE_FETCH: {
      msgSize = sizeof(SSchedulerFetchMsg);
      msg = calloc(1, msgSize);
      if (NULL == msg) {
        qError("calloc %d failed", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }
    
      SSchedulerFetchMsg *pMsg = msg;
      pMsg->queryId = job->queryId;
      pMsg->taskId = task->taskId;      
      break;
    }
    default:
      qError("unknown msg type:%d", msgType);
      break;
  }

  //TODO SEND MSG

  return TSDB_CODE_SUCCESS;
}

int32_t schTaskCheckAndSetRetry(SQueryJob *job, SQueryTask *task, int32_t errCode, bool *needRetry) {

}

int32_t schHandleRspMsg(SQueryJob *job, SQueryTask *task, int32_t msgType, int32_t rspCode) {
  int32_t code = 0;
  
  switch (msgType) {
    case TSDB_MSG_TYPE_QUERY:
      if (rspCode != TSDB_CODE_SUCCESS) {
        SCH_ERR_JRET(schProcessOnTaskFailure(job, task, rspCode));
      } else {
        code = schAsyncSendMsg(job, task, TSDB_MSG_TYPE_RSP_READY);
        if (code) {
          goto _task_error;
        }
      }
      break;
    case TSDB_MSG_TYPE_RSP_READY:
      if (rspCode != TSDB_CODE_SUCCESS) {
        SCH_ERR_JRET(schProcessOnTaskFailure(job, task, rspCode));
      } else {
        code = schProcessOnTaskSuccess(job, task);
        if (code) {
          goto _task_error;
        }        
      }
      break;
    case TSDB_MSG_TYPE_FETCH:
      SCH_ERR_JRET(rspCode);
      SCH_ERR_JRET(schProcessOnDataFetched(job));
      break;
    default:
      qError("unknown msg type:%d received", msgType);
      return TSDB_CODE_QRY_INVALID_INPUT;
  }

  return TSDB_CODE_SUCCESS;

_task_error:
  SCH_ERR_JRET(schProcessOnTaskFailure(job, task, code));
  return TSDB_CODE_SUCCESS;

_return:
  code = schProcessOnJobFailure(job);
  return code;
}


int32_t schFetchFromRemote(SQueryJob *job) {
  int32_t code = 0;
  
  if (atomic_val_compare_exchange_32(&job->remoteFetch, 0, 1) != 0) {
    qInfo("prior fetching not finished");
    return TSDB_CODE_SUCCESS;
  }

  SCH_ERR_JRET(schAsyncSendMsg(job, NULL, TSDB_MSG_TYPE_FETCH));

  return TSDB_CODE_SUCCESS;
  
_return:
  atomic_val_compare_exchange_32(&job->remoteFetch, 1, 0);

  return code;
}


int32_t schProcessOnJobSuccess(SQueryJob *job) {
  if (job->userFetch) {
    SCH_ERR_RET(schFetchFromRemote(job));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schProcessOnJobFailure(SQueryJob *job) {
  atomic_val_compare_exchange_32(&job->remoteFetch, 1, 0);

  if (job->userFetch) {
    tsem_post(&job->rspSem);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schProcessOnDataFetched(SQueryJob *job) {
  atomic_val_compare_exchange_32(&job->remoteFetch, 1, 0);

  tsem_post(&job->rspSem);
}


int32_t schProcessOnTaskSuccess(SQueryJob *job, SQueryTask *task) {
  bool moved = false;
  
  SCH_ERR_RET(schMoveTaskToSuccList(job, task, &moved));
  if (!moved) {
    SCH_TASK_ERR_LOG("task may already moved, status:%d", task->status);
    return TSDB_CODE_SUCCESS;
  }
  
  int32_t parentNum = (int32_t)taosArrayGetSize(task->parents);
  if (parentNum == 0) {
    if (task->plan->level != 0) {
      qError("level error");
      SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
    }

    strncpy(job->resEp.fqdn, task->execAddr.fqdn, sizeof(job->resEp.fqdn));
    job->resEp.port = task->execAddr.port;

    SCH_ERR_RET(schProcessOnJobSuccess(job));

    return TSDB_CODE_SUCCESS;
  }

  if (SCH_IS_DATA_SRC_TASK(task) && job->dataSrcEps.numOfEps < SCH_MAX_CONDIDATE_EP_NUM) {
    strncpy(job->dataSrcEps.fqdn[job->dataSrcEps.numOfEps], task->execAddr.fqdn, sizeof(task->execAddr.fqdn));
    job->dataSrcEps.port[job->dataSrcEps.numOfEps] = task->execAddr.port;

    ++job->dataSrcEps.numOfEps;
  }

  for (int32_t i = 0; i < parentNum; ++i) {
    SQueryTask *par = taosArrayGet(task->parents, i);

    ++par->childReady;

    SCH_ERR_RET(qSetSubplanExecutionNode(par->plan, task->plan->id.templateId, &task->execAddr));
    
    if (SCH_TASK_READY_TO_LUNCH(par)) {
      SCH_ERR_RET(schTaskRun(job, task));
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schProcessOnTaskFailure(SQueryJob *job, SQueryTask *task, int32_t errCode) {
  bool needRetry = false;
  SCH_ERR_RET(schTaskCheckAndSetRetry(job, task, errCode, &needRetry));
  
  if (!needRetry) {
    SCH_TASK_ERR_LOG("task failed[%x], no more retry", errCode);
    
    job->status = SCH_STATUS_FAILED;
    SCH_ERR_RET(schProcessOnJobFailure(job));

    return TSDB_CODE_SUCCESS;
  }

  SCH_ERR_RET(schTaskRun(job, task));

  return TSDB_CODE_SUCCESS;
}




int32_t schTaskRun(SQueryJob *job, SQueryTask *task) {
  SSubplan *plan = task->plan;
  
  SCH_ERR_RET(qSubPlanToString(plan, &task->msg));
  if (plan->execEpSet.numOfEps <= 0) {
    SCH_ERR_RET(schAvailableEpSet(job, &plan->execEpSet));
  }
  
  SCH_ERR_RET(schAsyncSendMsg(job, task, TSDB_MSG_TYPE_QUERY));

  SCH_ERR_RET(schPushTaskToExecList(job, task));

  return TSDB_CODE_SUCCESS;
}

int32_t schJobRun(SQueryJob *job) {
  SQueryLevel *level = taosArrayGet(job->levels, job->levelIdx);
  for (int32_t i = 0; i < level->taskNum; ++i) {
    SQueryTask *task = taosArrayGet(level->subTasks, i);
    SCH_ERR_RET(schTaskRun(job, task));
  }
  
  return TSDB_CODE_SUCCESS;
}


int32_t schedulerInit(SSchedulerCfg *cfg) {
  schMgmt.Jobs = taosHashInit(SCHEDULE_DEFAULT_JOB_NUMBER, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == schMgmt.Jobs) {
    SCH_ERR_LRET(TSDB_CODE_QRY_OUT_OF_MEMORY, "init %d schduler jobs failed", SCHEDULE_DEFAULT_JOB_NUMBER);
  }

  if (cfg) {
    schMgmt.cfg = *cfg;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t scheduleQueryJob(struct SCatalog *pCatalog, void *pRpc, const SEpSet* pMgmtEps, SQueryDag* pDag, void** pJob) {
  if (NULL == pCatalog || NULL == pRpc || NULL == pMgmtEps || NULL == pDag || NULL == pDag->pSubplans || NULL == pJob) {
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t code = 0;
  SQueryJob *job = calloc(1, sizeof(SQueryJob));
  if (NULL == job) {
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  job->catalog = pCatalog;
  job->rpc = pRpc;
  job->mgmtEpSet = (SEpSet *)pMgmtEps;

  SCH_ERR_JRET(schValidateAndBuildJob(pDag, job));

  job->execTasks = taosHashInit(pDag->numOfSubplans, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == job->execTasks) {
    qError("taosHashInit %d failed", pDag->numOfSubplans);
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  job->succTasks = taosHashInit(pDag->numOfSubplans, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == job->succTasks) {
    qError("taosHashInit %d failed", pDag->numOfSubplans);
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  tsem_init(&job->rspSem, 0, 0);
  
  SCH_ERR_JRET(schJobRun(job));

  *(SQueryJob **)pJob = job;
  
  return TSDB_CODE_SUCCESS;

_return:

  *(SQueryJob **)pJob = NULL;
  scheduleFreeJob(job);
  
  SCH_RET(code);
}

int32_t scheduleFetchRows(void *pRpc, void *pJob, void **data) {
  if (NULL == pRpc || NULL == pJob || NULL == data) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SQueryJob *job = pJob;
  int32_t code = 0;

  if (atomic_val_compare_exchange_32(&job->userFetch, 0, 1) != 0) {
    qError("prior fetching not finished");
    return TSDB_CODE_QRY_APP_ERROR;
  }

  if (job->status == SCH_STATUS_SUCCEED) {
    SCH_ERR_JRET(schFetchFromRemote(job));
  }

  tsem_wait(&job->rspSem);

  *data = job->res;
  job->res = NULL;

_return:
  atomic_val_compare_exchange_32(&job->userFetch, 1, 0);

  return code;
}

int32_t scheduleCancelJob(void *pRpc, void *pJob);

void scheduleFreeJob(void *job) {
  if (NULL == job) {
    return;
  }
}

void schedulerDestroy(void) {
  if (schMgmt.Jobs) {
    taosHashCleanup(schMgmt.Jobs); //TBD
    schMgmt.Jobs = NULL;
  }
}


