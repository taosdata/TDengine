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
        task->childern = taosArrayInit(childNum, POINTER_BYTES);
        if (NULL == task->childern) {
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

        if (NULL == taosArrayPush(task->childern, &childTask)) {
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

  return TSDB_CODE_SUCCESS;
}


int32_t schValidateAndBuildJob(SQueryDag *dag, SQueryJob *job) {
  int32_t code = 0;
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

int32_t schAvailableEpSet(SEpSet *epSet) {

}


int32_t schAsyncLaunchTask(SQueryJob *job, SQueryTask *task) {

}



int32_t schTaskRun(SQueryJob *job, SQueryTask *task) {
  SSubplan *plan = task->plan;
  
  switch (task->status) {
    case SCH_STATUS_NOT_START:
      SCH_ERR_RET(qSubPlanToString(plan, &task->msg));
      if (plan->execEpSet.numOfEps <= 0) {
        SCH_ERR_RET(schAvailableEpSet(&plan->execEpSet));
      }
      SCH_ERR_RET(schAsyncLaunchTask(job, task));
      break;
    case SCH_STATUS_EXECUTING:
      break;
    case SCH_STATUS_SUCCEED:
      break;
    default:
      SCH_JOB_ERR_LOG("invalid level status:%d, levelIdx:%d", job->status, job->levelIdx);
      SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schJobRun(SQueryJob *job) {
  bool cont = true;
  
  while (cont) {
    switch (job->status) {
      case SCH_STATUS_NOT_START:
      case SCH_STATUS_EXECUTING:

        break;
        
      default:
        SCH_JOB_ERR_LOG("invalid job status:%d", job->status);
        SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
    }
  }

  return TSDB_CODE_SUCCESS;
}


int32_t schedulerInit(SSchedulerCfg *cfg) {
  schMgmt.Jobs = taosHashInit(SCHEDULE_DEFAULT_JOB_NUMBER, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == schMgmt.Jobs) {
    SCH_ERR_LRET(TSDB_CODE_QRY_OUT_OF_MEMORY, "init %d schduler jobs failed", SCHEDULE_DEFAULT_JOB_NUMBER);
  }

  return TSDB_CODE_SUCCESS;
}


int32_t scheduleQueryJob(void *pRpc, SQueryDag* pDag, void** pJob) {
  if (NULL == pDag || NULL == pDag->pSubplans || NULL == pJob) {
    SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t code = 0;
  SQueryJob *job = calloc(1, sizeof(SQueryJob));
  if (NULL == job) {
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SCH_ERR_JRET(schValidateAndBuildJob(pDag, job));

  SCH_ERR_JRET(schJobRun(job));

  *(SQueryJob **)pJob = job;
  
  return TSDB_CODE_SUCCESS;

_return:

  *(SQueryJob **)pJob = NULL;
  scheduleFreeJob(job);
  
  SCH_RET(code);
}

int32_t scheduleFetchRows(void *pRpc, void *pJob, void *data);

int32_t scheduleCancelJob(void *pRpc, void *pJob);

void scheduleFreeJob(void *pJob) {

}

void schedulerDestroy(void) {
  if (schMgmt.Jobs) {
    taosHashCleanup(schMgmt.Jobs); //TBD
    schMgmt.Jobs = NULL;
  }
}


