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

#include <tsdb.h>
#include "dataSinkMgt.h"
#include "exception.h"
#include "os.h"
#include "tarray.h"
#include "tcache.h"
#include "tglobal.h"
#include "tmsg.h"

#include "thash.h"
#include "executorimpl.h"
#include "executor.h"
#include "tlosertree.h"
#include "ttypes.h"
#include "query.h"

typedef struct STaskMgmt {
  pthread_mutex_t lock;
  SCacheObj      *qinfoPool;      // query handle pool
  int32_t         vgId;
  bool            closed;
} STaskMgmt;

static void taskMgmtKillTaskFn(void* handle, void* param1) {
  void** fp = (void**)handle;
  qKillTask(*fp);
}

static void freeqinfoFn(void *qhandle) {
  void** handle = qhandle;
  if (handle == NULL || *handle == NULL) {
    return;
  }

  qKillTask(*handle);
  qDestroyTask(*handle);
}

void freeParam(STaskParam *param) {
  tfree(param->sql);
  tfree(param->tagCond);
  tfree(param->tbnameCond);
  tfree(param->pTableIdList);
  taosArrayDestroy(param->pOperator);
  tfree(param->pExprs);
  tfree(param->pSecExprs);

  tfree(param->pExpr);
  tfree(param->pSecExpr);

  tfree(param->pGroupColIndex);
  tfree(param->pTagColumnInfo);
  tfree(param->pGroupbyExpr);
  tfree(param->prevResult);
}

int32_t qCreateExecTask(void* tsdb, int32_t vgId, SSubplan* pSubplan, qTaskInfo_t* pTaskInfo, DataSinkHandle* handle) {
  assert(tsdb != NULL && pSubplan != NULL);
  SExecTaskInfo** pTask = (SExecTaskInfo**)pTaskInfo;

  int32_t         code = 0;
  uint64_t        uid = 0;
  STimeWindow     window = TSWINDOW_INITIALIZER;
  int32_t         tableType = 0;

  SPhyNode   *pPhyNode = pSubplan->pNode;
  if (pPhyNode->info.type == OP_TableScan || pPhyNode->info.type == OP_DataBlocksOptScan) {
    STableScanPhyNode* pTableScanNode = (STableScanPhyNode*)pPhyNode;
    uid       = pTableScanNode->scan.uid;
    window    = pTableScanNode->window;
    tableType = pTableScanNode->scan.tableType;
  } else {
    assert(0);
  }

  STableGroupInfo groupInfo = {0};
  if (tableType == TSDB_SUPER_TABLE) {
    code = tsdbQuerySTableByTagCond(tsdb, uid, window.skey, NULL, 0, 0, NULL, &groupInfo, NULL, 0, pSubplan->id.queryId);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  } else { // Create one table group.
    groupInfo.numOfTables = 1;
    groupInfo.pGroupList = taosArrayInit(1, POINTER_BYTES);

    SArray* pa = taosArrayInit(1, sizeof(STableKeyInfo));

    STableKeyInfo info = {.pTable = NULL, .lastKey = 0, .uid = uid};
    taosArrayPush(pa, &info);
    taosArrayPush(groupInfo.pGroupList, &pa);
  }

  if (groupInfo.numOfTables == 0) {
    code = 0;
//    qDebug("no table qualified for query, reqId:0x%"PRIx64, (*pTask)->id.queryId);
    goto _error;
  }

  code = doCreateExecTaskInfo(pSubplan, pTask, &groupInfo, tsdb);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  SDataSinkMgtCfg cfg = {.maxDataBlockNum = 1000, .maxDataBlockNumPerQuery = 100};
  code = dsDataSinkMgtInit(&cfg);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = dsCreateDataSinker(pSubplan->pDataSink, &(*pTask)->dsHandle);

  *handle = (*pTask)->dsHandle;

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

  // todo: remove it.
  if (tinfo == NULL) {
    return NULL;
  }

  *pRes = NULL;

  int64_t curOwner = 0;
  if ((curOwner = atomic_val_compare_exchange_64(&pTaskInfo->owner, 0, threadId)) != 0) {
    qError("QInfo:0x%" PRIx64 "-%p qhandle is now executed by thread:%p", GET_TASKID(pTaskInfo), pTaskInfo,
           (void*)curOwner);
    pTaskInfo->code = TSDB_CODE_QRY_IN_EXEC;
    return pTaskInfo->code;
  }

  if (pTaskInfo->cost.start == 0) {
    pTaskInfo->cost.start = taosGetTimestampMs();
  }

  if (isTaskKilled(pTaskInfo)) {
    qDebug("QInfo:0x%" PRIx64 " it is already killed, abort", GET_TASKID(pTaskInfo));
    return TSDB_CODE_SUCCESS;
  }

  //  STaskRuntimeEnv* pRuntimeEnv = &pTaskInfo->runtimeEnv;
  //  if (pTaskInfo->tableqinfoGroupInfo.numOfTables == 0) {
  //    qDebug("QInfo:0x%"PRIx64" no table exists for query, abort", GET_TASKID(pTaskInfo));
  //    setTaskStatus(pTaskInfo, TASK_COMPLETED);
  //    return doBuildResCheck(pTaskInfo);
  //  }

  // error occurs, record the error code and return to client
  int32_t ret = setjmp(pTaskInfo->env);
  if (ret != TSDB_CODE_SUCCESS) {
    publishQueryAbortEvent(pTaskInfo, ret);
    pTaskInfo->code = ret;
    qDebug("QInfo:0x%" PRIx64 " query abort due to error/cancel occurs, code:%s", GET_TASKID(pTaskInfo),
           tstrerror(pTaskInfo->code));
    return pTaskInfo->code;
  }

  qDebug("QInfo:0x%" PRIx64 " query task is launched", GET_TASKID(pTaskInfo));

  bool newgroup = false;
  publishOperatorProfEvent(pTaskInfo->pRoot, QUERY_PROF_BEFORE_OPERATOR_EXEC);
  int64_t st = 0;

  st = taosGetTimestampUs();
  *pRes = pTaskInfo->pRoot->exec(pTaskInfo->pRoot, &newgroup);

  pTaskInfo->cost.elapsedTime += (taosGetTimestampUs() - st);
  publishOperatorProfEvent(pTaskInfo->pRoot, QUERY_PROF_AFTER_OPERATOR_EXEC);

  if (NULL == *pRes) {
    *useconds = pTaskInfo->cost.elapsedTime;
  }

  qDebug("QInfo:0x%" PRIx64 " query paused, %d rows returned, total:%" PRId64 " rows, in sinkNode:%d",
         GET_TASKID(pTaskInfo), 0, 0L, 0);

  atomic_store_64(&pTaskInfo->owner, 0);
  return pTaskInfo->code;
}

int32_t qRetrieveQueryResultInfo(qTaskInfo_t qinfo, bool* buildRes, void* pRspContext) {
  SQInfo *pQInfo = (SQInfo *)qinfo;

  if (pQInfo == NULL || !isValidQInfo(pQInfo)) {
    qError("QInfo invalid qhandle");
    return TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  *buildRes = false;
  if (IS_QUERY_KILLED(pQInfo)) {
    qDebug("QInfo:0x%"PRIx64" query is killed, code:0x%08x", pQInfo->qId, pQInfo->code);
    return pQInfo->code;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  if (tsRetrieveBlockingModel) {
    pQInfo->rspContext = pRspContext;
    tsem_wait(&pQInfo->ready);
    *buildRes = true;
    code = pQInfo->code;
  } else {
    STaskRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
    STaskAttr *pQueryAttr = pQInfo->runtimeEnv.pQueryAttr;

    pthread_mutex_lock(&pQInfo->lock);

    assert(pQInfo->rspContext == NULL);
    if (pQInfo->dataReady == QUERY_RESULT_READY) {
      *buildRes = true;
      qDebug("QInfo:0x%"PRIx64" retrieve result info, rowsize:%d, rows:%d, code:%s", pQInfo->qId, pQueryAttr->resultRowSize,
             GET_NUM_OF_RESULTS(pRuntimeEnv), tstrerror(pQInfo->code));
    } else {
      *buildRes = false;
      qDebug("QInfo:0x%"PRIx64" retrieve req set query return result after paused", pQInfo->qId);
      pQInfo->rspContext = pRspContext;
      assert(pQInfo->rspContext != NULL);
    }

    code = pQInfo->code;
    pthread_mutex_unlock(&pQInfo->lock);
  }

  return code;
}

void* qGetResultRetrieveMsg(qTaskInfo_t qinfo) {
  SQInfo* pQInfo = (SQInfo*) qinfo;
  assert(pQInfo != NULL);

  return pQInfo->rspContext;
}

int32_t qKillTask(qTaskInfo_t qinfo) {
  SQInfo *pQInfo = (SQInfo *)qinfo;

  if (pQInfo == NULL || !isValidQInfo(pQInfo)) {
    return TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  qDebug("QInfo:0x%"PRIx64" query killed", pQInfo->qId);
  setQueryKilled(pQInfo);

  // Wait for the query executing thread being stopped/
  // Once the query is stopped, the owner of qHandle will be cleared immediately.
  while (pQInfo->owner != 0) {
    taosMsleep(100);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qIsTaskCompleted(qTaskInfo_t qinfo) {
  SExecTaskInfo *pTaskInfo = (SExecTaskInfo *)qinfo;

  if (pTaskInfo == NULL /*|| !isValidQInfo(pTaskInfo)*/) {
    return TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  return isTaskKilled(pTaskInfo) || Q_STATUS_EQUAL(pTaskInfo->status, TASK_OVER);
}

void qDestroyTask(qTaskInfo_t qHandle) {
  SQInfo* pQInfo = (SQInfo*) qHandle;
  if (!isValidQInfo(pQInfo)) {
    return;
  }

  qDebug("QInfo:0x%"PRIx64" query completed", pQInfo->qId);
  queryCostStatis(pQInfo);   // print the query cost summary
  doDestroyTask(pQInfo);
}

void* qOpenTaskMgmt(int32_t vgId) {
  const int32_t refreshHandleInterval = 30; // every 30 seconds, refresh handle pool

  char cacheName[128] = {0};
  sprintf(cacheName, "qhandle_%d", vgId);

  STaskMgmt* pTaskMgmt = calloc(1, sizeof(STaskMgmt));
  if (pTaskMgmt == NULL) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return NULL;
  }

  pTaskMgmt->qinfoPool = taosCacheInit(TSDB_CACHE_PTR_KEY, refreshHandleInterval, true, freeqinfoFn, cacheName);
  pTaskMgmt->closed    = false;
  pTaskMgmt->vgId      = vgId;

  pthread_mutex_init(&pTaskMgmt->lock, NULL);

  qDebug("vgId:%d, open queryTaskMgmt success", vgId);
  return pTaskMgmt;
}

void qTaskMgmtNotifyClosing(void* pQMgmt) {
  if (pQMgmt == NULL) {
    return;
  }

  STaskMgmt* pQueryMgmt = pQMgmt;
  qInfo("vgId:%d, set querymgmt closed, wait for all queries cancelled", pQueryMgmt->vgId);

  pthread_mutex_lock(&pQueryMgmt->lock);
  pQueryMgmt->closed = true;
  pthread_mutex_unlock(&pQueryMgmt->lock);

  taosCacheRefresh(pQueryMgmt->qinfoPool, taskMgmtKillTaskFn, NULL);
}

void qQueryMgmtReOpen(void *pQMgmt) {
  if (pQMgmt == NULL) {
    return;
  }

  STaskMgmt *pQueryMgmt = pQMgmt;
  qInfo("vgId:%d, set querymgmt reopen", pQueryMgmt->vgId);

  pthread_mutex_lock(&pQueryMgmt->lock);
  pQueryMgmt->closed = false;
  pthread_mutex_unlock(&pQueryMgmt->lock);
}

void qCleanupTaskMgmt(void* pQMgmt) {
  if (pQMgmt == NULL) {
    return;
  }

  STaskMgmt* pQueryMgmt = pQMgmt;
  int32_t vgId = pQueryMgmt->vgId;

  assert(pQueryMgmt->closed);

  SCacheObj* pqinfoPool = pQueryMgmt->qinfoPool;
  pQueryMgmt->qinfoPool = NULL;

  taosCacheCleanup(pqinfoPool);
  pthread_mutex_destroy(&pQueryMgmt->lock);
  tfree(pQueryMgmt);

  qDebug("vgId:%d, queryMgmt cleanup completed", vgId);
}

void** qRegisterTask(void* pMgmt, uint64_t qId, void *qInfo) {
  if (pMgmt == NULL) {
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
    return NULL;
  }

  STaskMgmt *pQueryMgmt = pMgmt;
  if (pQueryMgmt->qinfoPool == NULL) {
    qError("QInfo:0x%"PRIx64"-%p failed to add qhandle into qMgmt, since qMgmt is closed", qId, (void*)qInfo);
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
    return NULL;
  }

  pthread_mutex_lock(&pQueryMgmt->lock);
  if (pQueryMgmt->closed) {
    pthread_mutex_unlock(&pQueryMgmt->lock);
    qError("QInfo:0x%"PRIx64"-%p failed to add qhandle into cache, since qMgmt is colsing", qId, (void*)qInfo);
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
    return NULL;
  } else {
    void** handle = taosCachePut(pQueryMgmt->qinfoPool, &qId, sizeof(qId), &qInfo, sizeof(TSDB_CACHE_PTR_TYPE),
                                 (getMaximumIdleDurationSec()*1000));
    pthread_mutex_unlock(&pQueryMgmt->lock);

    return handle;
  }
}

void** qAcquireTask(void* pMgmt, uint64_t _key) {
  STaskMgmt *pQueryMgmt = pMgmt;

  if (pQueryMgmt->closed) {
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
    return NULL;
  }

  if (pQueryMgmt->qinfoPool == NULL) {
    terrno = TSDB_CODE_QRY_INVALID_QHANDLE;
    return NULL;
  }

  void** handle = taosCacheAcquireByKey(pQueryMgmt->qinfoPool, &_key, sizeof(_key));
  if (handle == NULL || *handle == NULL) {
    terrno = TSDB_CODE_QRY_INVALID_QHANDLE;
    return NULL;
  } else {
    return handle;
  }
}

void** qReleaseTask(void* pMgmt, void* pQInfo, bool freeHandle) {
  STaskMgmt *pQueryMgmt = pMgmt;
  if (pQueryMgmt->qinfoPool == NULL) {
    return NULL;
  }

  taosCacheRelease(pQueryMgmt->qinfoPool, pQInfo, freeHandle);
  return 0;
}

#if 0
//kill by qid
int32_t qKillQueryByQId(void* pMgmt, int64_t qId, int32_t waitMs, int32_t waitCount) {
  int32_t error = TSDB_CODE_SUCCESS;
  void** handle = qAcquireTask(pMgmt, qId);
  if(handle == NULL) return terrno;

  SQInfo* pQInfo = (SQInfo*)(*handle);
  if (pQInfo == NULL || !isValidQInfo(pQInfo)) {
    return TSDB_CODE_QRY_INVALID_QHANDLE;
  }
  qWarn("QId:0x%"PRIx64" be killed(no memory commit).", pQInfo->qId);
  setQueryKilled(pQInfo);

  // wait query stop
  int32_t loop = 0;
  while (pQInfo->owner != 0) {
    taosMsleep(waitMs);
    if(loop++ > waitCount){
      error = TSDB_CODE_FAILED;
      break;
    }
  }

  qReleaseTask(pMgmt, (void **)&handle, true);
  return error;
}

#endif