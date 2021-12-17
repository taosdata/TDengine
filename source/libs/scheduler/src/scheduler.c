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

int32_t schValidateAndBuildJob(SQueryDag *dag, SQueryJob *job) {
  int32_t levelNum = (int32_t)taosArrayGetSize(dag->pSubplans);
  if (levelNum <= 0) {
    qError("invalid level num:%d", levelNum);
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  job->levels = taosArrayInit(levelNum, sizeof(SQueryLevel));
  if (NULL == job->levels) {
    qError("taosArrayInit %d failed", levelNum);
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  job->levelNum = levelNum;
  job->levelIdx = levelNum - 1;
  job->status = SCH_STATUS_NOT_START;

  job->subPlans = dag->pSubplans;

  SQueryLevel level = {0};
  SArray *levelPlans = NULL;
  int32_t levelPlanNum = 0;

  for (int32_t i = 0; i < levelNum; ++i) {
    levelPlans = taosArrayGetP(dag->pSubplans, i);
    if (NULL == levelPlans) {
      qError("no level plans for level %d", i);
      return TSDB_CODE_QRY_INVALID_INPUT;
    }

    levelPlanNum = (int32_t)taosArrayGetSize(levelPlans);
    if (levelPlanNum <= 0) {
      qError("invalid level plans number:%d, level:%d", levelPlanNum, i);
      return TSDB_CODE_QRY_INVALID_INPUT;
    }
    
    for (int32_t n = 0; n < levelPlanNum; ++n) {

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


int32_t scheduleQueryJob(SQueryDag* pDag, void** pJob) {
  if (NULL == pDag || NULL == pDag->pSubplans || NULL == pJob) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }


  SQueryJob *job = calloc(1, sizeof(SQueryJob));
  if (NULL == job) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  schValidateAndBuildJob(pDag, job);





  *(SQueryJob **)pJob = job;




}

int32_t scheduleFetchRows(void *pJob, void *data);

int32_t scheduleCancelJob(void *pJob);

void schedulerDestroy(void) {
  if (schMgmt.Jobs) {
    taosHashCleanup(schMgmt.Jobs); //TBD
    schMgmt.Jobs = NULL;
  }
}


