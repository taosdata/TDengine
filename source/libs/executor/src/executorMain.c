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

#include "os.h"
#include "tref.h"
#include "dataSinkMgt.h"
#include "tmsg.h"
#include "tudf.h"

#include "executor.h"
#include "executorimpl.h"
#include "query.h"

static TdThreadOnce initPoolOnce = PTHREAD_ONCE_INIT;
int32_t exchangeObjRefPool = -1;

static void initRefPool() {
  exchangeObjRefPool = taosOpenRef(1024, doDestroyExchangeOperatorInfo);
}

int32_t qCreateExecTask(SReadHandle* readHandle, int32_t vgId, uint64_t taskId, SSubplan* pSubplan,
                        qTaskInfo_t* pTaskInfo, DataSinkHandle* handle, const char* sql, EOPTR_EXEC_MODEL model) {
  assert(readHandle != NULL && pSubplan != NULL);
  SExecTaskInfo** pTask = (SExecTaskInfo**)pTaskInfo;

  taosThreadOnce(&initPoolOnce, initRefPool);

  int32_t code = createExecTaskInfoImpl(pSubplan, pTask, readHandle, taskId, sql, model);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  SDataSinkMgtCfg cfg = {.maxDataBlockNum = 1000, .maxDataBlockNumPerQuery = 100};
  code = dsDataSinkMgtInit(&cfg);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  
  if (handle) {
    void* pSinkParam = NULL;
    code = createDataSinkParam(pSubplan->pDataSink, &pSinkParam, pTaskInfo);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    
    code = dsCreateDataSinker(pSubplan->pDataSink, handle, pSinkParam);
  }

  _error:
  // if failed to add ref for all tables in this query, abort current query
  return code;
}

#ifdef TEST_IMPL
// wait moment
int waitMoment(SQInfo* pQInfo){
  if(pQInfo->sql) {
    int ms = 0;
    char* pcnt = strstr(pQInfo->sql, " count(*)");
    if(pcnt) return 0;
    
    char* pos = strstr(pQInfo->sql, " t_");
    if(pos){
      pos += 3;
      ms = atoi(pos);
      while(*pos >= '0' && *pos <= '9'){
        pos ++;
      }
      char unit_char = *pos;
      if(unit_char == 'h'){
        ms *= 3600*1000;
      } else if(unit_char == 'm'){
        ms *= 60*1000;
      } else if(unit_char == 's'){
        ms *= 1000;
      }
    }
    if(ms == 0) return 0;
    printf("test wait sleep %dms. sql=%s ...\n", ms, pQInfo->sql);
    
    if(ms < 1000) {
      taosMsleep(ms);
    } else {
      int used_ms = 0;
      while(used_ms < ms) {
        taosMsleep(1000);
        used_ms += 1000;
        if(isTaskKilled(pQInfo)){
          printf("test check query is canceled, sleep break.%s\n", pQInfo->sql);
          break;
        }
      }
    }
  }
  return 1;
}
#endif

int32_t qExecTask(qTaskInfo_t tinfo, SSDataBlock** pRes, uint64_t *useconds) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  int64_t        threadId = taosGetSelfPthreadId();

  *pRes = NULL;
  int64_t curOwner = 0;
  if ((curOwner = atomic_val_compare_exchange_64(&pTaskInfo->owner, 0, threadId)) != 0) {
    qError("%s-%p execTask is now executed by thread:%p", GET_TASKID(pTaskInfo), pTaskInfo,
           (void*)curOwner);
    pTaskInfo->code = TSDB_CODE_QRY_IN_EXEC;
    return pTaskInfo->code;
  }

  if (pTaskInfo->cost.start == 0) {
    pTaskInfo->cost.start = taosGetTimestampMs();
  }

  if (isTaskKilled(pTaskInfo)) {
    qDebug("%s already killed, abort", GET_TASKID(pTaskInfo));
    return TSDB_CODE_SUCCESS;
  }

  // error occurs, record the error code and return to client
  int32_t ret = setjmp(pTaskInfo->env);
  if (ret != TSDB_CODE_SUCCESS) {
    pTaskInfo->code = ret;
    cleanUpUdfs();
    qDebug("%s task abort due to error/cancel occurs, code:%s", GET_TASKID(pTaskInfo), tstrerror(pTaskInfo->code));
    return pTaskInfo->code;
  }

  qDebug("%s execTask is launched", GET_TASKID(pTaskInfo));

  int64_t st = taosGetTimestampUs();
  *pRes = pTaskInfo->pRoot->fpSet.getNextFn(pTaskInfo->pRoot);
  uint64_t el = (taosGetTimestampUs() - st);

  pTaskInfo->cost.elapsedTime += el;
  if (NULL == *pRes) {
    *useconds = pTaskInfo->cost.elapsedTime;
  }

  cleanUpUdfs();

  int32_t current = (*pRes != NULL)? (*pRes)->info.rows:0;
  uint64_t total = pTaskInfo->pRoot->resultInfo.totalRows;

  qDebug("%s task suspended, %d rows returned, total:%" PRId64 " rows, in sinkNode:%d, elapsed:%.2f ms",
         GET_TASKID(pTaskInfo), current, total, 0, el/1000.0);

  atomic_store_64(&pTaskInfo->owner, 0);
  return pTaskInfo->code;
}

int32_t qKillTask(qTaskInfo_t qinfo) {
  SExecTaskInfo *pTaskInfo = (SExecTaskInfo *)qinfo;

  if (pTaskInfo == NULL) {
    return TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  qDebug("%s execTask killed", GET_TASKID(pTaskInfo));
  setTaskKilled(pTaskInfo);

  // Wait for the query executing thread being stopped/
  // Once the query is stopped, the owner of qHandle will be cleared immediately.
  while (pTaskInfo->owner != 0) {
    taosMsleep(100);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qAsyncKillTask(qTaskInfo_t qinfo) {
  SExecTaskInfo *pTaskInfo = (SExecTaskInfo *)qinfo;

  if (pTaskInfo == NULL) {
    return TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  qDebug("%s execTask async killed", GET_TASKID(pTaskInfo));
  setTaskKilled(pTaskInfo);

  return TSDB_CODE_SUCCESS;
}

int32_t qIsTaskCompleted(qTaskInfo_t qinfo) {
  SExecTaskInfo *pTaskInfo = (SExecTaskInfo *)qinfo;

  if (pTaskInfo == NULL) {
    return TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  return isTaskKilled(pTaskInfo);
}

void qDestroyTask(qTaskInfo_t qTaskHandle) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*) qTaskHandle;
  qDebug("%s execTask completed, numOfRows:%"PRId64, GET_TASKID(pTaskInfo), pTaskInfo->pRoot->resultInfo.totalRows);

  queryCostStatis(pTaskInfo);   // print the query cost summary
  doDestroyTask(pTaskInfo);
}

int32_t qGetExplainExecInfo(qTaskInfo_t tinfo, int32_t *resNum, SExplainExecInfo **pRes) {
  SExecTaskInfo *pTaskInfo = (SExecTaskInfo *)tinfo;
  int32_t capacity = 0;

  return getOperatorExplainExecInfo(pTaskInfo->pRoot, pRes, &capacity, resNum);  
}

int32_t qSerializeTaskStatus(qTaskInfo_t tinfo, char** pOutput, int32_t* len) {
  SExecTaskInfo* pTaskInfo = (struct SExecTaskInfo*)tinfo;
  if (pTaskInfo->pRoot == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  return encodeOperator(pTaskInfo->pRoot, pOutput, len);
}

int32_t qDeserializeTaskStatus(qTaskInfo_t tinfo, const char* pInput, int32_t len) {
  SExecTaskInfo* pTaskInfo = (struct SExecTaskInfo*) tinfo;

  if (pTaskInfo == NULL || pInput == NULL || len == 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  return decodeOperator(pTaskInfo->pRoot, pInput, len);
}


