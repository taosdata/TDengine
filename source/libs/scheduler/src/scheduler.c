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
#include "tmsg.h"
#include "query.h"
#include "catalog.h"

static SSchedulerMgmt schMgmt = {0};

int32_t schBuildTaskRalation(SSchJob *job, SHashObj *planToTask) {
  for (int32_t i = 0; i < job->levelNum; ++i) {
    SSchLevel *level = taosArrayGet(job->levels, i);
    
    for (int32_t m = 0; m < level->taskNum; ++m) {
      SSchTask *task = taosArrayGet(level->subTasks, m);
      SSubplan *plan = task->plan;
      int32_t childNum = plan->pChildren ? (int32_t)taosArrayGetSize(plan->pChildren) : 0;
      int32_t parentNum = plan->pParents ? (int32_t)taosArrayGetSize(plan->pParents) : 0;

      if (childNum > 0) {
        task->children = taosArrayInit(childNum, POINTER_BYTES);
        if (NULL == task->children) {
          qError("taosArrayInit %d failed", childNum);
          SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
        }
      }

      for (int32_t n = 0; n < childNum; ++n) {
        SSubplan **child = taosArrayGet(plan->pChildren, n);
        SSchTask **childTask = taosHashGet(planToTask, child, POINTER_BYTES);
        if (NULL == childTask || NULL == *childTask) {
          qError("subplan relationship error, level:%d, taskIdx:%d, childIdx:%d", i, m, n);
          SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
        }

        if (NULL == taosArrayPush(task->children, childTask)) {
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
        SSubplan **parent = taosArrayGet(plan->pParents, n);
        SSchTask **parentTask = taosHashGet(planToTask, parent, POINTER_BYTES);
        if (NULL == parentTask || NULL == *parentTask) {
          qError("subplan relationship error, level:%d, taskIdx:%d, childIdx:%d", i, m, n);
          SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
        }

        if (NULL == taosArrayPush(task->parents, parentTask)) {
          qError("taosArrayPush failed");
          SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
        }
      }      
    }
  }

  SSchLevel *level = taosArrayGet(job->levels, 0);
  if (job->attr.queryJob && level->taskNum > 1) {
    qError("invalid plan info, level 0, taskNum:%d", level->taskNum);
    SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
  }

  SSchTask *task = taosArrayGet(level->subTasks, 0);
  if (task->parents && taosArrayGetSize(task->parents) > 0) {
    qError("invalid plan info, level 0, parentNum:%d", (int32_t)taosArrayGetSize(task->parents));
    SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
  }


  return TSDB_CODE_SUCCESS;
}

static SSchTask initTask(SSchJob* pJob, SSubplan* plan, SSchLevel *pLevel) {
  SSchTask task = {0};
  if (plan->type == QUERY_TYPE_MODIFY) {
    pJob->attr.needFetch = false;
  } else {
    pJob->attr.queryJob = true;
  }

  task.plan   = plan;
  task.level  = pLevel;
  task.status = JOB_TASK_STATUS_NOT_START;
  task.taskId = atomic_add_fetch_64(&schMgmt.taskId, 1);

  return task;
}

static void cleanupTask(SSchTask* pTask) {
  taosArrayDestroy(pTask->condidateAddrs);
}

int32_t schValidateAndBuildJob(SQueryDag *dag, SSchJob *pJob) {
  int32_t code = 0;

  pJob->queryId = dag->queryId;
  
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

  pJob->levels = taosArrayInit(levelNum, sizeof(SSchLevel));
  if (NULL == pJob->levels) {
    qError("taosArrayInit %d failed", levelNum);
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  //??
  pJob->attr.needFetch = true;

  pJob->levelNum = levelNum;
  pJob->levelIdx = levelNum - 1;

  pJob->subPlans = dag->pSubplans;

  SSchLevel level = {0};
  SArray *levelPlans = NULL;
  int32_t levelPlanNum = 0;
  SSchLevel *pLevel = NULL;

  level.status = JOB_TASK_STATUS_NOT_START;

  for (int32_t i = 0; i < levelNum; ++i) {
    if (NULL == taosArrayPush(pJob->levels, &level)) {
      qError("taosArrayPush failed");
      SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    pLevel = taosArrayGet(pJob->levels, i);
  
    pLevel->level = i;
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

    pLevel->taskNum = levelPlanNum;
    
    pLevel->subTasks = taosArrayInit(levelPlanNum, sizeof(SSchTask));
    if (NULL == pLevel->subTasks) {
      qError("taosArrayInit %d failed", levelPlanNum);
      SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
    
    for (int32_t n = 0; n < levelPlanNum; ++n) {
      SSubplan *plan = taosArrayGetP(levelPlans, n);
      if (plan->type == QUERY_TYPE_MODIFY) {
        pJob->attr.needFetch = false;
      } else {
        pJob->attr.queryJob = true;
      }

      SSchTask task = initTask(pJob, plan, pLevel);
      void *p = taosArrayPush(pLevel->subTasks, &task);
      if (NULL == p) {
        qError("taosArrayPush failed");
        SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }
      
      if (0 != taosHashPut(planToTask, &plan, POINTER_BYTES, &p, POINTER_BYTES)) {
        qError("taosHashPut failed");
        SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }
    }
  }

  SCH_ERR_JRET(schBuildTaskRalation(pJob, planToTask));

  if (planToTask) {
    taosHashCleanup(planToTask);
  }
  
  return TSDB_CODE_SUCCESS;

_return:
  if (pLevel->subTasks) {
    taosArrayDestroy(pLevel->subTasks);
  }

  if (planToTask) {
    taosHashCleanup(planToTask);
  }

  SCH_RET(code);
}

int32_t schSetTaskCondidateAddrs(SSchJob *job, SSchTask *task) {
  if (task->condidateAddrs) {
    return TSDB_CODE_SUCCESS;
  }

  task->condidateIdx = 0;
  task->condidateAddrs = taosArrayInit(SCH_MAX_CONDIDATE_EP_NUM, sizeof(SQueryNodeAddr));
  if (NULL == task->condidateAddrs) {
    qError("taosArrayInit failed");
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  if (task->plan->execNode.numOfEps > 0) {
    if (NULL == taosArrayPush(task->condidateAddrs, &task->plan->execNode)) {
      qError("taosArrayPush failed");
      SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    return TSDB_CODE_SUCCESS;
  }

  int32_t addNum = 0;
  int32_t nodeNum = taosArrayGetSize(job->nodeList);
  
  for (int32_t i = 0; i < nodeNum && addNum < SCH_MAX_CONDIDATE_EP_NUM; ++i) {
    SQueryNodeAddr *naddr = taosArrayGet(job->nodeList, i);
    
    if (NULL == taosArrayPush(task->condidateAddrs, &task->plan->execNode)) {
      qError("taosArrayPush failed");
      SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
    
    ++addNum;
  }

/*
  for (int32_t i = 0; i < job->dataSrcEps.numOfEps && addNum < SCH_MAX_CONDIDATE_EP_NUM; ++i) {
    strncpy(epSet->fqdn[epSet->numOfEps], job->dataSrcEps.fqdn[i], sizeof(job->dataSrcEps.fqdn[i]));
    epSet->port[epSet->numOfEps] = job->dataSrcEps.port[i];
    
    ++epSet->numOfEps;
  }
*/

  return TSDB_CODE_SUCCESS;
}


int32_t schPushTaskToExecList(SSchJob *job, SSchTask *task) {
  if (0 != taosHashPut(job->execTasks, &task->taskId, sizeof(task->taskId), &task, POINTER_BYTES)) {
    qError("taosHashPut failed");
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SCH_TASK_DLOG("push to %s list", "execTasks");

  return TSDB_CODE_SUCCESS;
}

int32_t schMoveTaskToSuccList(SSchJob *job, SSchTask *task, bool *moved) {
  if (0 != taosHashRemove(job->execTasks, &task->taskId, sizeof(task->taskId))) {
    qWarn("remove task[%"PRIx64"] from execTasks failed", task->taskId);
    return TSDB_CODE_SUCCESS;
  }

  if (0 != taosHashPut(job->succTasks, &task->taskId, sizeof(task->taskId), &task, POINTER_BYTES)) {
    qError("taosHashPut failed");
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SCH_TASK_DLOG("push to %s list", "succTasks");

  *moved = true;
  
  return TSDB_CODE_SUCCESS;
}

int32_t schMoveTaskToFailList(SSchJob *job, SSchTask *task, bool *moved) {
  if (0 != taosHashRemove(job->execTasks, &task->taskId, sizeof(task->taskId))) {
    qWarn("remove task[%"PRIx64"] from execTasks failed, it may not exist", task->taskId);
  }

  if (0 != taosHashPut(job->failTasks, &task->taskId, sizeof(task->taskId), &task, POINTER_BYTES)) {
    qError("taosHashPut failed");
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SCH_TASK_DLOG("push to %s list", "failTasks");

  *moved = true;
  
  return TSDB_CODE_SUCCESS;
}

int32_t schTaskCheckAndSetRetry(SSchJob *job, SSchTask *task, int32_t errCode, bool *needRetry) {
  // TODO set retry or not based on task type/errCode/retry times/job status/available eps...
  // TODO if needRetry, set task retry info

  *needRetry = false;

  return TSDB_CODE_SUCCESS;
}


int32_t schFetchFromRemote(SSchJob *job) {
  int32_t code = 0;
  
  if (atomic_val_compare_exchange_32(&job->remoteFetch, 0, 1) != 0) {
    qInfo("prior fetching not finished");
    return TSDB_CODE_SUCCESS;
  }

  SCH_ERR_JRET(schBuildAndSendMsg(job, job->fetchTask, TDMT_VND_FETCH));

  return TSDB_CODE_SUCCESS;
  
_return:
  atomic_val_compare_exchange_32(&job->remoteFetch, 1, 0);

  return code;
}


int32_t schProcessOnJobPartialSuccess(SSchJob *job) {
  job->status = JOB_TASK_STATUS_PARTIAL_SUCCEED;

  bool needFetch = job->userFetch;
  if ((!job->attr.needFetch) && job->attr.syncSchedule) {
    tsem_post(&job->rspSem);
  }
  
  if (needFetch) {
    SCH_ERR_RET(schFetchFromRemote(job));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schProcessOnJobFailure(SSchJob *job, int32_t errCode) {
  job->status = JOB_TASK_STATUS_FAILED;
  job->errCode = errCode;

  atomic_val_compare_exchange_32(&job->remoteFetch, 1, 0);

  if (job->userFetch || ((!job->attr.needFetch) && job->attr.syncSchedule)) {
    tsem_post(&job->rspSem);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schProcessOnDataFetched(SSchJob *job) {
  atomic_val_compare_exchange_32(&job->remoteFetch, 1, 0);

  tsem_post(&job->rspSem);
}


int32_t schProcessOnTaskSuccess(SSchJob *job, SSchTask *task) {
  bool moved = false;
  
  SCH_ERR_RET(schMoveTaskToSuccList(job, task, &moved));
  if (!moved) {
    SCH_TASK_ELOG("task may already moved, status:%d", task->status);
    return TSDB_CODE_SUCCESS;
  }

  task->status = JOB_TASK_STATUS_SUCCEED;
  
  int32_t parentNum = task->parents ? (int32_t)taosArrayGetSize(task->parents) : 0;
  if (parentNum == 0) {
    if (task->plan->level != 0) {
      qError("level error");
      SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
    }

    int32_t taskDone = 0;
    
    if (SCH_TASK_NEED_WAIT_ALL(task)) {
      SCH_LOCK(SCH_WRITE, &task->level->lock);
      task->level->taskSucceed++;
      taskDone = task->level->taskSucceed + task->level->taskFailed;
      SCH_UNLOCK(SCH_WRITE, &task->level->lock);
      
      if (taskDone < task->level->taskNum) {
        qDebug("wait all tasks, done:%d, all:%d", taskDone, task->level->taskNum);
        return TSDB_CODE_SUCCESS;
      }

      if (task->level->taskFailed > 0) {
        job->status = JOB_TASK_STATUS_FAILED;
        SCH_ERR_RET(schProcessOnJobFailure(job, TSDB_CODE_QRY_APP_ERROR));

        return TSDB_CODE_SUCCESS;
      }
    } else {
      strncpy(job->resEp.fqdn, task->execAddr.epAddr[task->execAddr.inUse].fqdn, sizeof(job->resEp.fqdn));
      job->resEp.port = task->execAddr.epAddr[task->execAddr.inUse].port;
    }

    job->fetchTask = task;
    SCH_ERR_RET(schProcessOnJobPartialSuccess(job));

    return TSDB_CODE_SUCCESS;
  }

/*
  if (SCH_IS_DATA_SRC_TASK(task) && job->dataSrcEps.numOfEps < SCH_MAX_CONDIDATE_EP_NUM) {
    strncpy(job->dataSrcEps.fqdn[job->dataSrcEps.numOfEps], task->execAddr.fqdn, sizeof(task->execAddr.fqdn));
    job->dataSrcEps.port[job->dataSrcEps.numOfEps] = task->execAddr.port;

    ++job->dataSrcEps.numOfEps;
  }
*/

  for (int32_t i = 0; i < parentNum; ++i) {
    SSchTask *par = *(SSchTask **)taosArrayGet(task->parents, i);

    ++par->childReady;

    SCH_ERR_RET(qSetSubplanExecutionNode(par->plan, task->plan->id.templateId, &task->execAddr));
    
    if (SCH_TASK_READY_TO_LUNCH(par)) {
      SCH_ERR_RET(schLaunchTask(job, par));
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schProcessOnTaskFailure(SSchJob *job, SSchTask *task, int32_t errCode) {
  bool needRetry = false;
  bool moved = false;
  int32_t taskDone = 0;
  SCH_ERR_RET(schTaskCheckAndSetRetry(job, task, errCode, &needRetry));
  
  if (!needRetry) {
    SCH_TASK_ELOG("task failed[%x], no more retry", errCode);

    SCH_ERR_RET(schMoveTaskToFailList(job, task, &moved));
    if (!moved) {
      SCH_TASK_ELOG("task may already moved, status:%d", task->status);
    }    
    
    if (SCH_TASK_NEED_WAIT_ALL(task)) {
      SCH_LOCK(SCH_WRITE, &task->level->lock);
      task->level->taskFailed++;
      taskDone = task->level->taskSucceed + task->level->taskFailed;
      SCH_UNLOCK(SCH_WRITE, &task->level->lock);
      
      if (taskDone < task->level->taskNum) {
        qDebug("wait all tasks, done:%d, all:%d", taskDone, task->level->taskNum);
        return TSDB_CODE_SUCCESS;
      }
    }
    
    job->status = JOB_TASK_STATUS_FAILED;
    SCH_ERR_RET(schProcessOnJobFailure(job, errCode));

    return TSDB_CODE_SUCCESS;
  }

  SCH_ERR_RET(schLaunchTask(job, task));

  return TSDB_CODE_SUCCESS;
}

int32_t schProcessRspMsg(SSchJob *job, SSchTask *task, int32_t msgType, char *msg, int32_t msgSize, int32_t rspCode) {
  int32_t code = 0;

  switch (msgType) {
    case TDMT_VND_CREATE_TABLE_RSP: {
      if (rspCode != TSDB_CODE_SUCCESS) {
        SCH_ERR_JRET(schProcessOnTaskFailure(job, task, rspCode));
      } else {
        code = schProcessOnTaskSuccess(job, task);
        if (code) {
          goto _task_error;
        }
      }
      break;
    }
    case TDMT_VND_SUBMIT_RSP: {
        if (rspCode != TSDB_CODE_SUCCESS) {
          SCH_ERR_JRET(schProcessOnTaskFailure(job, task, rspCode));
        } else {
          SShellSubmitRspMsg *rsp = (SShellSubmitRspMsg *)msg;
          job->resNumOfRows += rsp->affectedRows;

          code = schProcessOnTaskSuccess(job, task);
          if (code) {
            goto _task_error;
          }               
        }
        break;
      }
    case TDMT_VND_QUERY_RSP: {
        SQueryTableRsp *rsp = (SQueryTableRsp *)msg;
        
        if (rsp->code != TSDB_CODE_SUCCESS) {
          SCH_ERR_JRET(schProcessOnTaskFailure(job, task, rsp->code));
        } else {
          code = schBuildAndSendMsg(job, task, TDMT_VND_RES_READY);
          if (code) {
            goto _task_error;
          }
        }
        break;
      }
    case TDMT_VND_RES_READY_RSP: {
        SResReadyRsp *rsp = (SResReadyRsp *)msg;
        
        if (rsp->code != TSDB_CODE_SUCCESS) {
          SCH_ERR_JRET(schProcessOnTaskFailure(job, task, rsp->code));
        } else {
          code = schProcessOnTaskSuccess(job, task);
          if (code) {
            goto _task_error;
          }        
        }
        break;
      }
    case TDMT_VND_FETCH_RSP: {
        SCH_ERR_JRET(rspCode);
        SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)msg;

        job->res = rsp;
        job->resNumOfRows = rsp->numOfRows;
        
        SCH_ERR_JRET(schProcessOnDataFetched(job));
        break;
      }
    case TDMT_VND_DROP_TASK: {

      }
    default:
      qError("unknown msg type:%d received", msgType);
      return TSDB_CODE_QRY_INVALID_INPUT;
  }

  return TSDB_CODE_SUCCESS;

_task_error:
  SCH_ERR_JRET(schProcessOnTaskFailure(job, task, code));
  return TSDB_CODE_SUCCESS;

_return:
  code = schProcessOnJobFailure(job, code);
  return code;
}


int32_t schHandleCallback(void* param, const SDataBuf* pMsg, int32_t msgType, int32_t rspCode) {
  int32_t code = 0;
  SSchCallbackParam *pParam = (SSchCallbackParam *)param;
  
  SSchJob **pjob = taosHashGet(schMgmt.jobs, &pParam->queryId, sizeof(pParam->queryId));
  if (NULL == pjob || NULL == (*pjob)) {
    qError("taosHashGet queryId:%"PRIx64" not exist", pParam->queryId);
    SCH_ERR_JRET(TSDB_CODE_SCH_INTERNAL_ERROR);
  }

  SSchJob *job = *pjob;

  SSchTask **ptask = taosHashGet(job->execTasks, &pParam->taskId, sizeof(pParam->taskId));
  if (NULL == ptask || NULL == (*ptask)) {
    qError("taosHashGet taskId:%"PRIx64" not exist", pParam->taskId);
    SCH_ERR_JRET(TSDB_CODE_SCH_INTERNAL_ERROR);
  }

  SSchTask *task = *ptask;

  SCH_TASK_DLOG("Got msg:%d, rspCode:%d", msgType, rspCode);
  
  schProcessRspMsg(job, task, msgType, pMsg->pData, pMsg->len, rspCode);

_return:
  tfree(param);
  SCH_RET(code);
}

int32_t schHandleSubmitCallback(void* param, const SDataBuf* pMsg, int32_t code) {
  return schHandleCallback(param, pMsg, TDMT_VND_SUBMIT_RSP, code);
}

int32_t schHandleCreateTableCallback(void* param, const SDataBuf* pMsg, int32_t code) {
  return schHandleCallback(param, pMsg, TDMT_VND_CREATE_TABLE_RSP, code);
}

int32_t schHandleQueryCallback(void* param, const SDataBuf* pMsg, int32_t code) {
  return schHandleCallback(param, pMsg, TDMT_VND_QUERY_RSP, code);
}

int32_t schHandleFetchCallback(void* param, const SDataBuf* pMsg, int32_t code) {
  return schHandleCallback(param, pMsg, TDMT_VND_FETCH_RSP, code);
}

int32_t schHandleReadyCallback(void* param, const SDataBuf* pMsg, int32_t code) {
  return schHandleCallback(param, pMsg, TDMT_VND_RES_READY_RSP, code);
}

int32_t schHandleDropCallback(void* param, const SDataBuf* pMsg, int32_t code) {  
  SSchCallbackParam *pParam = (SSchCallbackParam *)param;
  qDebug("drop task rsp received, queryId:%"PRIx64 ",taksId:%"PRIx64 ",code:%d", pParam->queryId, pParam->taskId, code);
}

int32_t schGetCallbackFp(int32_t msgType, __async_send_cb_fn_t *fp) {
  switch (msgType) {
    case TDMT_VND_CREATE_TABLE:
      *fp = schHandleCreateTableCallback;
      break;
    case TDMT_VND_SUBMIT: 
      *fp = schHandleSubmitCallback;
      break;
    case TDMT_VND_QUERY: 
      *fp = schHandleQueryCallback;
      break;
    case TDMT_VND_RES_READY: 
      *fp = schHandleReadyCallback;
      break;
    case TDMT_VND_FETCH: 
      *fp = schHandleFetchCallback;
      break;
    case TDMT_VND_DROP_TASK:
      *fp = schHandleDropCallback;
      break;
    default:
      qError("unknown msg type:%d", msgType);
      SCH_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  return TSDB_CODE_SUCCESS;
}


int32_t schAsyncSendMsg(void *transport, SEpSet* epSet, uint64_t qId, uint64_t tId, int32_t msgType, void *msg, uint32_t msgSize) {
  int32_t code = 0;
  SMsgSendInfo* pMsgSendInfo = calloc(1, sizeof(SMsgSendInfo));
  if (NULL == pMsgSendInfo) {
    qError("calloc %d failed", (int32_t)sizeof(SMsgSendInfo));
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SSchCallbackParam *param = calloc(1, sizeof(SSchCallbackParam));
  if (NULL == param) {
    qError("calloc %d failed", (int32_t)sizeof(SSchCallbackParam));
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  __async_send_cb_fn_t fp = NULL;
  SCH_ERR_JRET(schGetCallbackFp(msgType, &fp));

  param->queryId = qId;
  param->taskId = tId;

  pMsgSendInfo->param = param;
  pMsgSendInfo->msgInfo.pData = msg;
  pMsgSendInfo->msgInfo.len = msgSize;
  pMsgSendInfo->msgType = msgType;

  pMsgSendInfo->fp = fp;
  
  int64_t  transporterId = 0;
  SCH_ERR_JRET(asyncSendMsgToServer(transport, epSet, &transporterId, pMsgSendInfo));
  
  return TSDB_CODE_SUCCESS;

_return:
  tfree(param);
  tfree(pMsgSendInfo);

  SCH_RET(code);
}

void schConvertAddrToEpSet(SQueryNodeAddr *addr, SEpSet *epSet) {
  epSet->inUse = addr->inUse;
  epSet->numOfEps = addr->numOfEps;
  
  for (int8_t i = 0; i < epSet->numOfEps; ++i) {
    strncpy(epSet->fqdn[i], addr->epAddr[i].fqdn, sizeof(addr->epAddr[i].fqdn));
    epSet->port[i] = addr->epAddr[i].port;
  }
}


int32_t schBuildAndSendMsg(SSchJob *job, SSchTask *task, int32_t msgType) {
  uint32_t msgSize = 0;
  void *msg = NULL;
  int32_t code = 0;
  
  switch (msgType) {
    case TDMT_VND_CREATE_TABLE:
    case TDMT_VND_SUBMIT: {
      if (NULL == task->msg || task->msgLen <= 0) {
        qError("submit msg is NULL");
        SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
      }

      msgSize = task->msgLen;
      msg = task->msg;
      break;
    }
    case TDMT_VND_QUERY: {
      if (NULL == task->msg) {
        qError("query msg is NULL");
        SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
      }

      msgSize = sizeof(SSubQueryMsg) + task->msgLen;
      msg = calloc(1, msgSize);
      if (NULL == msg) {
        qError("calloc %d failed", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      SSubQueryMsg *pMsg = msg;

      pMsg->header.vgId = htonl(task->plan->execNode.nodeId);
      pMsg->sId = htobe64(schMgmt.sId);
      pMsg->queryId = htobe64(job->queryId);
      pMsg->taskId = htobe64(task->taskId);
      pMsg->contentLen = htonl(task->msgLen);
      memcpy(pMsg->msg, task->msg, task->msgLen);
      break;
    }    
    case TDMT_VND_RES_READY: {
      msgSize = sizeof(SResReadyMsg);
      msg = calloc(1, msgSize);
      if (NULL == msg) {
        qError("calloc %d failed", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      SResReadyMsg *pMsg = msg;
      
      pMsg->header.vgId = htonl(task->plan->execNode.nodeId);      
      pMsg->sId = htobe64(schMgmt.sId);      
      pMsg->queryId = htobe64(job->queryId);
      pMsg->taskId = htobe64(task->taskId);      
      break;
    }
    case TDMT_VND_FETCH: {
      if (NULL == task) {
        SCH_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
      }
      msgSize = sizeof(SResFetchMsg);
      msg = calloc(1, msgSize);
      if (NULL == msg) {
        qError("calloc %d failed", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }
    
      SResFetchMsg *pMsg = msg;
      
      pMsg->header.vgId = htonl(task->plan->execNode.nodeId);            
      pMsg->sId = htobe64(schMgmt.sId);      
      pMsg->queryId = htobe64(job->queryId);
      pMsg->taskId = htobe64(task->taskId);      
      break;
    }
    case TDMT_VND_DROP_TASK:{
      msgSize = sizeof(STaskDropMsg);
      msg = calloc(1, msgSize);
      if (NULL == msg) {
        qError("calloc %d failed", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }
    
      STaskDropMsg *pMsg = msg;
      
      pMsg->header.vgId = htonl(task->plan->execNode.nodeId);            
      pMsg->sId = htobe64(schMgmt.sId);      
      pMsg->queryId = htobe64(job->queryId);
      pMsg->taskId = htobe64(task->taskId);      
      break;
    }
    default:
      qError("unknown msg type:%d", msgType);
      SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
      break;
  }

  SEpSet epSet;
  SQueryNodeAddr *addr = taosArrayGet(task->condidateAddrs, task->condidateIdx);
  
  schConvertAddrToEpSet(addr, &epSet);

  SCH_ERR_JRET(schAsyncSendMsg(job->transport, &epSet, job->queryId, task->taskId, msgType, msg, msgSize));

  return TSDB_CODE_SUCCESS;

_return:

  tfree(msg);
  SCH_RET(code);
}


int32_t schLaunchTask(SSchJob *job, SSchTask *task) {
  SSubplan *plan = task->plan;
  SCH_ERR_RET(qSubPlanToString(plan, &task->msg, &task->msgLen));
  SCH_ERR_RET(schSetTaskCondidateAddrs(job, task));

  if (NULL == task->condidateAddrs || taosArrayGetSize(task->condidateAddrs) <= 0) {
    SCH_TASK_ELOG("no valid condidate node for task:%"PRIx64, task->taskId);
    SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
  }

  SCH_ERR_RET(schBuildAndSendMsg(job, task, plan->msgType));
  SCH_ERR_RET(schPushTaskToExecList(job, task));

  task->status = JOB_TASK_STATUS_EXECUTING;
  return TSDB_CODE_SUCCESS;
}

int32_t schLaunchJob(SSchJob *job) {
  SSchLevel *level = taosArrayGet(job->levels, job->levelIdx);
  for (int32_t i = 0; i < level->taskNum; ++i) {
    SSchTask *task = taosArrayGet(level->subTasks, i);
    SCH_ERR_RET(schLaunchTask(job, task));
  }

  job->status = JOB_TASK_STATUS_EXECUTING;
  
  return TSDB_CODE_SUCCESS;
}

void schDropJobAllTasks(SSchJob *job) {
  void *pIter = taosHashIterate(job->succTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    int32_t msgType = task->plan->msgType;
    if (msgType == TDMT_VND_CREATE_TABLE || msgType == TDMT_VND_SUBMIT) {
      break;
    }

    schBuildAndSendMsg(job, task, TDMT_VND_DROP_TASK);
    pIter = taosHashIterate(job->succTasks, pIter);
  }  

  pIter = taosHashIterate(job->failTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    int32_t msgType = task->plan->msgType;
    if (msgType == TDMT_VND_CREATE_TABLE || msgType == TDMT_VND_SUBMIT) {
      break;
    }

    schBuildAndSendMsg(job, task, TDMT_VND_DROP_TASK);
    pIter = taosHashIterate(job->succTasks, pIter);
  }  
}

uint64_t schGenSchId(void) {
  uint64_t sId = 0;

  // TODO

  qDebug("Gen sId:0x%"PRIx64, sId);

  return sId;
}


int32_t schedulerInit(SSchedulerCfg *cfg) {
  if (schMgmt.jobs) {
    qError("scheduler already init");
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  if (cfg) {
    schMgmt.cfg = *cfg;
    
    if (schMgmt.cfg.maxJobNum <= 0) {
      schMgmt.cfg.maxJobNum = SCHEDULE_DEFAULT_JOB_NUMBER;
    }
  } else {
    schMgmt.cfg.maxJobNum = SCHEDULE_DEFAULT_JOB_NUMBER;
  }

  schMgmt.jobs = taosHashInit(schMgmt.cfg.maxJobNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == schMgmt.jobs) {
    SCH_ERR_LRET(TSDB_CODE_QRY_OUT_OF_MEMORY, "init %d schduler jobs failed", schMgmt.cfg.maxJobNum);
  }

  schMgmt.sId = schGenSchId();
  
  return TSDB_CODE_SUCCESS;
}


int32_t scheduleExecJobImpl(void *transport, SArray *nodeList, SQueryDag* pDag, void** pJob, bool syncSchedule) {
  if (nodeList && taosArrayGetSize(nodeList) <= 0) {
    qInfo("qnodeList is empty");
  }

  int32_t code = 0;
  SSchJob *job = calloc(1, sizeof(SSchJob));
  if (NULL == job) {
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  job->attr.syncSchedule = syncSchedule;
  job->transport = transport;
  job->nodeList = nodeList;

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

  job->failTasks = taosHashInit(pDag->numOfSubplans, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == job->failTasks) {
    qError("taosHashInit %d failed", pDag->numOfSubplans);
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  tsem_init(&job->rspSem, 0, 0);

  code = taosHashPut(schMgmt.jobs, &job->queryId, sizeof(job->queryId), &job, POINTER_BYTES);
  if (0 != code) {
    if (HASH_NODE_EXIST(code)) {
      qError("taosHashPut queryId:0x%"PRIx64" already exist", job->queryId);
      SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
    } else {
      qError("taosHashPut queryId:0x%"PRIx64" failed", job->queryId);
      SCH_ERR_JRET(TSDB_CODE_SCH_INTERNAL_ERROR);
    }
  }

  job->status = JOB_TASK_STATUS_NOT_START;
  SCH_ERR_JRET(schLaunchJob(job));

  *(SSchJob **)pJob = job;

  if (syncSchedule) {
    tsem_wait(&job->rspSem);
  }

  return TSDB_CODE_SUCCESS;

_return:
  *(SSchJob **)pJob = NULL;
  scheduleFreeJob(job);
  
  SCH_RET(code);
}

int32_t scheduleExecJob(void *transport, SArray *nodeList, SQueryDag* pDag, void** pJob, SQueryResult *pRes) {
  if (NULL == transport || /* NULL == nodeList || */ NULL == pDag || NULL == pDag->pSubplans || NULL == pJob || NULL == pRes) {
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SCH_ERR_RET(scheduleExecJobImpl(transport, nodeList, pDag, pJob, true));

  SSchJob *job = *(SSchJob **)pJob;
  
  pRes->code = job->errCode;
  pRes->numOfRows = job->resNumOfRows;
  
  return TSDB_CODE_SUCCESS;
}

int32_t scheduleAsyncExecJob(void *transport, SArray *nodeList, SQueryDag* pDag, void** pJob) {
  if (NULL == transport || NULL == nodeList ||NULL == pDag || NULL == pDag->pSubplans || NULL == pJob) {
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  return scheduleExecJobImpl(transport, nodeList, pDag, pJob, false);
}


int32_t scheduleFetchRows(void *pJob, void **data) {
  if (NULL == pJob || NULL == data) {
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SSchJob *job = pJob;
  int32_t code = 0;

  if (!job->attr.needFetch) {
    qError("no need to fetch data");
    SCH_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  if (job->status == JOB_TASK_STATUS_FAILED) {
    job->res = NULL;
    SCH_RET(job->errCode);
  }

  if (job->status == JOB_TASK_STATUS_SUCCEED) {
    job->res = NULL;
    return TSDB_CODE_SUCCESS;
  }

  if (atomic_val_compare_exchange_32(&job->userFetch, 0, 1) != 0) {
    qError("prior fetching not finished");
    SCH_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  if (job->status == JOB_TASK_STATUS_PARTIAL_SUCCEED) {
    SCH_ERR_JRET(schFetchFromRemote(job));
  }

  tsem_wait(&job->rspSem);

  if (job->status == JOB_TASK_STATUS_FAILED) {
    code = job->errCode;
  }
  
  if (job->res && ((SRetrieveTableRsp *)job->res)->completed) {
    job->status = JOB_TASK_STATUS_SUCCEED;
  }

  *data = job->res;
  job->res = NULL;

_return:
  atomic_val_compare_exchange_32(&job->userFetch, 1, 0);

  SCH_RET(code);
}

int32_t scheduleCancelJob(void *pJob) {
  //TODO

  //TODO MOVE ALL TASKS FROM EXEC LIST TO FAIL LIST

  return TSDB_CODE_SUCCESS;
}

void scheduleFreeJob(void *pJob) {
  if (NULL == pJob) {
    return;
  }

  SSchJob *job = pJob;

  if (job->status > 0) {
    if (0 != taosHashRemove(schMgmt.jobs, &job->queryId, sizeof(job->queryId))) {
      qError("remove job:%"PRIx64"from mgmt failed", job->queryId); // maybe already freed
      return;
    }

    if (job->status == JOB_TASK_STATUS_EXECUTING) {
      scheduleCancelJob(pJob);
    }

    schDropJobAllTasks(job);
  }

  job->subPlans = NULL; // it is a reference to pDag->pSubplans
  int32_t numOfLevels = taosArrayGetSize(job->levels);
  for(int32_t i = 0; i < numOfLevels; ++i) {
    SSchLevel *pLevel = taosArrayGet(job->levels, i);

    int32_t numOfTasks = taosArrayGetSize(pLevel->subTasks);
    for(int32_t j = 0; j < numOfTasks; ++j) {
      SSchTask* pTask = taosArrayGet(pLevel->subTasks, j);
      cleanupTask(pTask);
    }

    taosArrayDestroy(pLevel->subTasks);
  }

  taosHashCleanup(job->execTasks);
  taosHashCleanup(job->failTasks);
  taosHashCleanup(job->succTasks);
  taosArrayDestroy(job->levels);
  tfree(job);
}

void schedulerDestroy(void) {
  if (schMgmt.jobs) {
    taosHashCleanup(schMgmt.jobs); //TODO
    schMgmt.jobs = NULL;
  }
}


