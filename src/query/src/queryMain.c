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
#include "taosmsg.h"
#include "tcache.h"
#include "tglobal.h"

#include "exception.h"
#include "hash.h"
#include "texpr.h"
#include "qExecutor.h"
#include "qUtil.h"
#include "query.h"
#include "queryLog.h"
#include "tlosertree.h"
#include "ttype.h"

typedef struct SQueryMgmt {
  pthread_mutex_t lock;
  SCacheObj      *qinfoPool;      // query handle pool
  int32_t         vgId;
  bool            closed;
} SQueryMgmt;

static void queryMgmtKillQueryFn(void* handle, void* param1) {
  void** fp = (void**)handle;
  qKillQuery(*fp);
}

static void freeqinfoFn(void *qhandle) {
  void** handle = qhandle;
  if (handle == NULL || *handle == NULL) {
    return;
  }

  qKillQuery(*handle);
  qDestroyQueryInfo(*handle);
}

void freeParam(SQueryParam *param) {
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

int32_t qCreateQueryInfo(void* tsdb, int32_t vgId, SQueryTableMsg* pQueryMsg, qinfo_t* pQInfo, uint64_t qId) {
  assert(pQueryMsg != NULL && tsdb != NULL);

  int32_t code = TSDB_CODE_SUCCESS;

  SQueryParam param = {0};
  code = convertQueryMsg(pQueryMsg, &param);
  if (code != TSDB_CODE_SUCCESS) {
    goto _over;
  }

  if (pQueryMsg->numOfTables <= 0) {
    qError("Invalid number of tables to query, numOfTables:%d", pQueryMsg->numOfTables);
    code = TSDB_CODE_QRY_INVALID_MSG;
    goto _over;
  }

  if (param.pTableIdList == NULL || taosArrayGetSize(param.pTableIdList) == 0) {
    qError("qmsg:%p, SQueryTableMsg wrong format", pQueryMsg);
    code = TSDB_CODE_QRY_INVALID_MSG;
    goto _over;
  }

  SQueriedTableInfo info = { .numOfTags = pQueryMsg->numOfTags, .numOfCols = pQueryMsg->numOfCols, .colList = pQueryMsg->tableCols};
  if ((code = createQueryFunc(&info, pQueryMsg->numOfOutput, &param.pExprs, param.pExpr, param.pTagColumnInfo,
                              pQueryMsg->queryType, pQueryMsg, param.pUdfInfo)) != TSDB_CODE_SUCCESS) {
    goto _over;
  }

  if (param.pSecExpr != NULL) {
    if ((code = createIndirectQueryFuncExprFromMsg(pQueryMsg, pQueryMsg->secondStageOutput, &param.pSecExprs, param.pSecExpr, param.pExprs, param.pUdfInfo)) != TSDB_CODE_SUCCESS) {
      goto _over;
    }
  }

  param.pGroupbyExpr = createGroupbyExprFromMsg(pQueryMsg, param.pGroupColIndex, &code);
  if ((param.pGroupbyExpr == NULL && pQueryMsg->numOfGroupCols != 0) || code != TSDB_CODE_SUCCESS) {
    goto _over;
  }

  bool isSTableQuery = false;
  STableGroupInfo tableGroupInfo = {0};
  int64_t st = taosGetTimestampUs();

  if (TSDB_QUERY_HAS_TYPE(pQueryMsg->queryType, TSDB_QUERY_TYPE_TABLE_QUERY)) {
    STableIdInfo *id = taosArrayGet(param.pTableIdList, 0);

    qDebug("qmsg:%p query normal table, uid:%"PRId64", tid:%d", pQueryMsg, id->uid, id->tid);
    if ((code = tsdbGetOneTableGroup(tsdb, id->uid, pQueryMsg->window.skey, &tableGroupInfo)) != TSDB_CODE_SUCCESS) {
      goto _over;
    }
  } else if (TSDB_QUERY_HAS_TYPE(pQueryMsg->queryType, TSDB_QUERY_TYPE_MULTITABLE_QUERY|TSDB_QUERY_TYPE_STABLE_QUERY)) {
    isSTableQuery = true;

    // also note there's possibility that only one table in the super table
    if (!TSDB_QUERY_HAS_TYPE(pQueryMsg->queryType, TSDB_QUERY_TYPE_MULTITABLE_QUERY)) {
      STableIdInfo *id = taosArrayGet(param.pTableIdList, 0);

      // group by normal column, do not pass the group by condition to tsdb to group table into different group
      int32_t numOfGroupByCols = pQueryMsg->numOfGroupCols;
      if (pQueryMsg->numOfGroupCols == 1 && !TSDB_COL_IS_TAG(param.pGroupColIndex->flag)) {
        numOfGroupByCols = 0;
      }

      qDebug("qmsg:%p query stable, uid:%"PRIu64", tid:%d", pQueryMsg, id->uid, id->tid);
      code = tsdbQuerySTableByTagCond(tsdb, id->uid, pQueryMsg->window.skey, param.tagCond, pQueryMsg->tagCondLen,
                                      pQueryMsg->tagNameRelType, param.tbnameCond, &tableGroupInfo, param.pGroupColIndex, numOfGroupByCols);

      if (code != TSDB_CODE_SUCCESS) {
        qError("qmsg:%p failed to query stable, reason: %s", pQueryMsg, tstrerror(code));
        goto _over;
      }
    } else {
      code = tsdbGetTableGroupFromIdList(tsdb, param.pTableIdList, &tableGroupInfo);
      if (code != TSDB_CODE_SUCCESS) {
        goto _over;
      }

      qDebug("qmsg:%p query on %u tables in one group from client", pQueryMsg, tableGroupInfo.numOfTables);
    }

    int64_t el = taosGetTimestampUs() - st;
    qDebug("qmsg:%p tag filter completed, numOfTables:%u, elapsed time:%"PRId64"us", pQueryMsg, tableGroupInfo.numOfTables, el);
  } else {
    assert(0);
  }

  code = checkForQueryBuf(tableGroupInfo.numOfTables);
  if (code != TSDB_CODE_SUCCESS) {  // not enough query buffer, abort
    goto _over;
  }

  assert(pQueryMsg->stableQuery == isSTableQuery);
  (*pQInfo) = createQInfoImpl(pQueryMsg, param.pGroupbyExpr, param.pExprs, param.pSecExprs, &tableGroupInfo,
                              param.pTagColumnInfo, vgId, param.sql, qId, param.pUdfInfo);

  param.sql    = NULL;
  param.pExprs = NULL;
  param.pSecExprs = NULL;
  param.pGroupbyExpr = NULL;
  param.pTagColumnInfo = NULL;
  param.pUdfInfo = NULL;

  code = initQInfo(&pQueryMsg->tsBuf, tsdb, NULL, *pQInfo, &param, (char*)pQueryMsg, pQueryMsg->prevResultLen, NULL);

  _over:
  if (param.pGroupbyExpr != NULL) {
    taosArrayDestroy(param.pGroupbyExpr->columnInfo);
  }

  destroyUdfInfo(param.pUdfInfo);

  taosArrayDestroy(param.pTableIdList);
  param.pTableIdList = NULL;

  freeParam(&param);

  for (int32_t i = 0; i < pQueryMsg->numOfCols; i++) {
    SColumnInfo* column = pQueryMsg->tableCols + i;
    freeColumnFilterInfo(column->flist.filterInfo, column->flist.numOfFilters);
  }

  //pQInfo already freed in initQInfo, but *pQInfo may not pointer to null;
  if (code != TSDB_CODE_SUCCESS) {
    *pQInfo = NULL;
  }

  // if failed to add ref for all tables in this query, abort current query
  return code;
}


bool qTableQuery(qinfo_t qinfo, uint64_t *qId) {
  SQInfo *pQInfo = (SQInfo *)qinfo;
  assert(pQInfo && pQInfo->signature == pQInfo);
  int64_t threadId = taosGetSelfPthreadId();

  int64_t curOwner = 0;
  if ((curOwner = atomic_val_compare_exchange_64(&pQInfo->owner, 0, threadId)) != 0) {
    qError("QInfo:0x%"PRIx64"-%p qhandle is now executed by thread:%p", pQInfo->qId, pQInfo, (void*) curOwner);
    pQInfo->code = TSDB_CODE_QRY_IN_EXEC;
    return false;
  }

  *qId = pQInfo->qId;
  if(pQInfo->startExecTs == 0) {
    pQInfo->startExecTs = taosGetTimestampMs();
    pQInfo->lastRetrieveTs = pQInfo->startExecTs;
  }

  if (isQueryKilled(pQInfo)) {
    qDebug("QInfo:0x%"PRIx64" it is already killed, abort", pQInfo->qId);
    setQueryKilled(pQInfo);
    pQInfo->runtimeEnv.outputBuf = NULL;
    return doBuildResCheck(pQInfo);
  }

  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
  if (pRuntimeEnv->tableqinfoGroupInfo.numOfTables == 0) {
    qDebug("QInfo:0x%"PRIx64" no table exists for query, abort", pQInfo->qId);
    setQueryStatus(pRuntimeEnv, QUERY_COMPLETED);
    return doBuildResCheck(pQInfo);
  }

  // error occurs, record the error code and return to client
  int32_t ret = setjmp(pQInfo->runtimeEnv.env);
  if (ret != TSDB_CODE_SUCCESS) {
    publishQueryAbortEvent(pQInfo, ret);
    pQInfo->code = ret;
    qDebug("QInfo:0x%"PRIx64" query abort due to error/cancel occurs, code:%s", pQInfo->qId, tstrerror(pQInfo->code));
    return doBuildResCheck(pQInfo);
  }

  qDebug("QInfo:0x%"PRIx64" query task is launched", pQInfo->qId);

  bool newgroup = false;
  publishOperatorProfEvent(pRuntimeEnv->proot, QUERY_PROF_BEFORE_OPERATOR_EXEC);
  pRuntimeEnv->outputBuf = pRuntimeEnv->proot->exec(pRuntimeEnv->proot, &newgroup);
  publishOperatorProfEvent(pRuntimeEnv->proot, QUERY_PROF_AFTER_OPERATOR_EXEC);
  pRuntimeEnv->resultInfo.total += GET_NUM_OF_RESULTS(pRuntimeEnv);

  if (isQueryKilled(pQInfo)) {
    qDebug("QInfo:0x%"PRIx64" query is killed", pQInfo->qId);
  } else if (GET_NUM_OF_RESULTS(pRuntimeEnv) == 0) {
    qDebug("QInfo:0x%"PRIx64" over, %u tables queried, total %"PRId64" rows returned", pQInfo->qId, pRuntimeEnv->tableqinfoGroupInfo.numOfTables,
           pRuntimeEnv->resultInfo.total);
  } else {
    qDebug("QInfo:0x%"PRIx64" query paused, %d rows returned, total:%" PRId64 " rows", pQInfo->qId,
        GET_NUM_OF_RESULTS(pRuntimeEnv), pRuntimeEnv->resultInfo.total);
  }

  return doBuildResCheck(pQInfo);
}

int32_t qRetrieveQueryResultInfo(qinfo_t qinfo, bool* buildRes, void* pRspContext) {
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
    SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;
    SQueryAttr *pQueryAttr = pQInfo->runtimeEnv.pQueryAttr;

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

int32_t qDumpRetrieveResult(qinfo_t qinfo, SRetrieveTableRsp **pRsp, int32_t *contLen, bool* continueExec) {
  SQInfo *pQInfo = (SQInfo *)qinfo;

  if (pQInfo == NULL || !isValidQInfo(pQInfo)) {
    return TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  SQueryAttr *pQueryAttr = pQInfo->runtimeEnv.pQueryAttr;
  SQueryRuntimeEnv* pRuntimeEnv = &pQInfo->runtimeEnv;

  int32_t s = GET_NUM_OF_RESULTS(pRuntimeEnv);
  size_t size = pQueryAttr->resultRowSize * s;
  size += sizeof(int32_t);
  size += sizeof(STableIdInfo) * taosHashGetSize(pRuntimeEnv->pTableRetrieveTsMap);

  *contLen = (int32_t)(size + sizeof(SRetrieveTableRsp));

  // current solution only avoid crash, but cannot return error code to client
  *pRsp = (SRetrieveTableRsp *)rpcMallocCont(*contLen);
  if (*pRsp == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  (*pRsp)->numOfRows = htonl((int32_t)s);

  if (pQInfo->code == TSDB_CODE_SUCCESS) {
    (*pRsp)->offset   = htobe64(pQInfo->runtimeEnv.currentOffset);
    (*pRsp)->useconds = htobe64(pQInfo->summary.elapsedTime);
  } else {
    (*pRsp)->offset   = 0;
    (*pRsp)->useconds = htobe64(pQInfo->summary.elapsedTime);
  }

  (*pRsp)->precision = htons(pQueryAttr->precision);
  if (GET_NUM_OF_RESULTS(&(pQInfo->runtimeEnv)) > 0 && pQInfo->code == TSDB_CODE_SUCCESS) {
    doDumpQueryResult(pQInfo, (*pRsp)->data);
  } else {
    setQueryStatus(pRuntimeEnv, QUERY_OVER);
  }

  RESET_NUM_OF_RESULTS(&(pQInfo->runtimeEnv));
  pQInfo->lastRetrieveTs = taosGetTimestampMs();

  pQInfo->rspContext = NULL;
  pQInfo->dataReady  = QUERY_RESULT_NOT_READY;

  if (IS_QUERY_KILLED(pQInfo) || Q_STATUS_EQUAL(pRuntimeEnv->status, QUERY_OVER)) {
    // here current thread hold the refcount, so it is safe to free tsdbQueryHandle.
    *continueExec = false;
    (*pRsp)->completed = 1;  // notify no more result to client
    qDebug("QInfo:0x%"PRIx64" no more results to retrieve", pQInfo->qId);
  } else {
    *continueExec = true;
    qDebug("QInfo:0x%"PRIx64" has more results to retrieve", pQInfo->qId);
  }

  // the memory should be freed if the code of pQInfo is not TSDB_CODE_SUCCESS
  if (pQInfo->code != TSDB_CODE_SUCCESS) {
    rpcFreeCont(*pRsp);
    *pRsp = NULL;
  }

  return pQInfo->code;
}

void* qGetResultRetrieveMsg(qinfo_t qinfo) {
  SQInfo* pQInfo = (SQInfo*) qinfo;
  assert(pQInfo != NULL);

  return pQInfo->rspContext;
}

int32_t qKillQuery(qinfo_t qinfo) {
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

int32_t qQueryCompleted(qinfo_t qinfo) {
  SQInfo *pQInfo = (SQInfo *)qinfo;

  if (pQInfo == NULL || !isValidQInfo(pQInfo)) {
    return TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  return isQueryKilled(pQInfo) || Q_STATUS_EQUAL(pQInfo->runtimeEnv.status, QUERY_OVER);
}

void qDestroyQueryInfo(qinfo_t qHandle) {
  SQInfo* pQInfo = (SQInfo*) qHandle;
  if (!isValidQInfo(pQInfo)) {
    return;
  }

  qDebug("QInfo:0x%"PRIx64" query completed", pQInfo->qId);
  queryCostStatis(pQInfo);   // print the query cost summary
  freeQInfo(pQInfo);
}

void* qOpenQueryMgmt(int32_t vgId) {
  const int32_t refreshHandleInterval = 30; // every 30 seconds, refresh handle pool

  char cacheName[128] = {0};
  sprintf(cacheName, "qhandle_%d", vgId);

  SQueryMgmt* pQueryMgmt = calloc(1, sizeof(SQueryMgmt));
  if (pQueryMgmt == NULL) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return NULL;
  }

  pQueryMgmt->qinfoPool = taosCacheInit(TSDB_CACHE_PTR_KEY, refreshHandleInterval, true, freeqinfoFn, cacheName);
  pQueryMgmt->closed    = false;
  pQueryMgmt->vgId      = vgId;

  pthread_mutex_init(&pQueryMgmt->lock, NULL);

  qDebug("vgId:%d, open querymgmt success", vgId);
  return pQueryMgmt;
}

void qQueryMgmtNotifyClosed(void* pQMgmt) {
  if (pQMgmt == NULL) {
    return;
  }

  SQueryMgmt* pQueryMgmt = pQMgmt;
  qInfo("vgId:%d, set querymgmt closed, wait for all queries cancelled", pQueryMgmt->vgId);

  pthread_mutex_lock(&pQueryMgmt->lock);
  pQueryMgmt->closed = true;
  pthread_mutex_unlock(&pQueryMgmt->lock);

  taosCacheRefresh(pQueryMgmt->qinfoPool, queryMgmtKillQueryFn, NULL);
}

void qQueryMgmtReOpen(void *pQMgmt) {
  if (pQMgmt == NULL) {
    return;
  }

  SQueryMgmt *pQueryMgmt = pQMgmt;
  qInfo("vgId:%d, set querymgmt reopen", pQueryMgmt->vgId);

  pthread_mutex_lock(&pQueryMgmt->lock);
  pQueryMgmt->closed = false;
  pthread_mutex_unlock(&pQueryMgmt->lock);
}

void qCleanupQueryMgmt(void* pQMgmt) {
  if (pQMgmt == NULL) {
    return;
  }

  SQueryMgmt* pQueryMgmt = pQMgmt;
  int32_t vgId = pQueryMgmt->vgId;

  assert(pQueryMgmt->closed);

  SCacheObj* pqinfoPool = pQueryMgmt->qinfoPool;
  pQueryMgmt->qinfoPool = NULL;

  taosCacheCleanup(pqinfoPool);
  pthread_mutex_destroy(&pQueryMgmt->lock);
  tfree(pQueryMgmt);

  qDebug("vgId:%d, queryMgmt cleanup completed", vgId);
}

void** qRegisterQInfo(void* pMgmt, uint64_t qId, void *qInfo) {
  if (pMgmt == NULL) {
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
    return NULL;
  }

  SQueryMgmt *pQueryMgmt = pMgmt;
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

void** qAcquireQInfo(void* pMgmt, uint64_t _key) {
  SQueryMgmt *pQueryMgmt = pMgmt;

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

void** qReleaseQInfo(void* pMgmt, void* pQInfo, bool freeHandle) {
  SQueryMgmt *pQueryMgmt = pMgmt;
  if (pQueryMgmt->qinfoPool == NULL) {
    return NULL;
  }

  taosCacheRelease(pQueryMgmt->qinfoPool, pQInfo, freeHandle);
  return 0;
}

//kill by qid
int32_t qKillQueryByQId(void* pMgmt, int64_t qId, int32_t waitMs, int32_t waitCount) {
  int32_t err = TSDB_CODE_SUCCESS;
  void** handle = qAcquireQInfo(pMgmt, qId);
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
      err = TSDB_CODE_FAILED;
      break;
    }
  }

  qReleaseQInfo(pMgmt, (void **)&handle, true);
  return err;
}

// local struct
typedef struct {
  int64_t qId;
  int64_t startExecTs;
} SLongQuery;

// callbark for sort compare 
static int compareLongQuery(const void* p1, const void* p2) {
  // sort desc 
  SLongQuery* plq1 = *(SLongQuery**)p1;
  SLongQuery* plq2 = *(SLongQuery**)p2;
  if(plq1->startExecTs == plq2->startExecTs) {
    return 0;
  } else if(plq1->startExecTs > plq2->startExecTs) {
    return 1;
  } else {
    return -1;
  }
}

// callback for taosCacheRefresh
static void cbFoundItem(void* handle, void* param1) {
  SQInfo * qInfo = *(SQInfo**) handle;
  if(qInfo == NULL) return ;
  SArray* qids = (SArray*) param1;
  if(qids == NULL) return ;

  bool usedMem = true;
  bool usedIMem = true;
  SMemTable* mem = qInfo->query.memRef.snapshot.omem;
  SMemTable* imem = qInfo->query.memRef.snapshot.imem;
  if(mem == NULL || T_REF_VAL_GET(mem) == 0)  
     usedMem = false;
  if(imem == NULL || T_REF_VAL_GET(mem) == 0) 
     usedIMem = false ;

  if(!usedMem && !usedIMem) 
     return ;

  // push to qids
  SLongQuery* plq = (SLongQuery*)malloc(sizeof(SLongQuery));
  plq->qId = qInfo->qId;
  plq->startExecTs = qInfo->startExecTs; 
  taosArrayPush(qids, &plq);
}

// longquery
void* qObtainLongQuery(void* param){
  SQueryMgmt* qMgmt =  (SQueryMgmt*)param;
  if(qMgmt == NULL || qMgmt->qinfoPool == NULL) 
    return NULL;
  SArray* qids = taosArrayInit(4, sizeof(int64_t*));
  if(qids == NULL) return NULL;
  // Get each item
  taosCacheRefresh(qMgmt->qinfoPool, cbFoundItem, qids);

  size_t cnt = taosArrayGetSize(qids);
  if(cnt == 0) {
    taosArrayDestroy(qids);
    return NULL;
  } 
  if(cnt > 1)
    taosArraySort(qids, compareLongQuery);

  return qids;   
}

//solve tsdb no block to commit
bool qFixedNoBlock(void* pRepo, void* pMgmt, int32_t longQueryMs) {
  SQueryMgmt *pQueryMgmt = pMgmt;
  bool fixed = false;

  // qid top list
  SArray *qids = (SArray*)qObtainLongQuery(pQueryMgmt);
  if(qids == NULL) return false;

  // kill Query
  int64_t now = taosGetTimestampMs();
  size_t cnt = taosArrayGetSize(qids);
  size_t i;
  SLongQuery* plq;
  for(i=0; i < cnt; i++) {
    plq = (SLongQuery* )taosArrayGetP(qids, i);
    if(plq->startExecTs > now) continue;
    if(now - plq->startExecTs >= longQueryMs) {
      qKillQueryByQId(pMgmt, plq->qId, 500, 10); // wait 50*100 ms 
      if(tsdbNoProblem(pRepo)) {
        fixed = true;
        qWarn("QId:0x%"PRIx64" fixed problem after kill this query.", plq->qId);
        break;
      }
    }
  }

  // free qids
  for(i=0; i < cnt; i++) {
    free(taosArrayGetP(qids, i));
  }
  taosArrayDestroy(qids);
  return fixed;
}

//solve tsdb no block to commit
bool qSolveCommitNoBlock(void* pRepo, void* pMgmt) {
  qWarn("pRepo=%p start solve problem.", pRepo);
  if(qFixedNoBlock(pRepo, pMgmt, 10*60*1000)) {
    return true;
  }
  if(qFixedNoBlock(pRepo, pMgmt, 2*60*1000)){
    return true;
  }
  if(qFixedNoBlock(pRepo, pMgmt, 30*1000)){
    return true;
  }
  qWarn("pRepo=%p solve problem failed.", pRepo);
  return false;
}
