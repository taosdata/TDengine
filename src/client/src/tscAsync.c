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

#include <stdatomic.h>
#include "os.h"
#include "tutil.h"

#include "qTableMeta.h"
#include "tnote.h"
#include "tqueue.h"
#include "trpc.h"
#include "tscLog.h"
#include "tscSubquery.h"
#include "tscUtil.h"
#include "tsched.h"
#include "tsclient.h"

static void tscAsyncQueryRowsForNextVnode(void *param, TAOS_RES *tres, int numOfRows);

/*
 * Proxy function to perform sequentially query&retrieve operation.
 * If sql queries upon a super table and two-stage merge procedure is not involved (when employ the projection
 * query), it will sequentially query&retrieve data for all vnodes
 */
static void tscAsyncFetchRowsProxy(void *param, TAOS_RES *tres, int numOfRows);

// The async auto batch feature.
static bool asyncBatchEnable;
// The queue store the async insertion statements
static taos_queue insertionQueue;
// The number of statements in the insertion queue.
static int currentBatchLen;
// The maximum auto batch len.
static int asyncBatchLen;
// The batch timeout in milliseconds.
static int64_t asyncBatchTimeout;
// The state of the insertion queue. While executing timeout task, the queue will set exclusive for writing
// in order to make sure the statements will be sent to vnodes no more than `timeout` milliseconds.
static int exclusiveState;
// The background thread to manage statement auto batch timeout.
static pthread_t background;

/**
 * Return the error result to the callback function, and release the sql object.
 * 
 * @param pSql  the sql object.
 * @param code  the error code of the error result.
 */
static void tscReturnsError(SSqlObj* pSql, int code) {
  if (pSql == NULL) {
    return;
  }
  
  pSql->res.code = code;
  tscAsyncResultOnError(pSql);
}

/**
 * Represents the callback function and its context.
 */
typedef struct {
  __async_cb_func_t fp;
  void *param;
} Runnable;

/**
 * The context of `tscMergedStatementsCallBack`.
 */
typedef struct {
  size_t count;
  Runnable runnable[];
} BatchCallBackContext;

/**
 * Proxy function to perform sequentially insert operation.
 * 
 * @param param     the context of `tscMergedStatementsCallBack`.
 * @param tres      the result object.
 * @param code      the error code.
 */
static void tscMergedStatementsCallBack(void *param, TAOS_RES *tres, int32_t code) {
  BatchCallBackContext* context = param;
  SSqlObj* res = tres;
  
  // handle corner case [context == null].
  if (context == NULL) {
    tscError("context in `tscMergedStatementsCallBack` is null, which should not happen");
    if (tres) {
      taosReleaseRef(tscObjRef, res->self);
    }
    return;
  }
  
  // handle corner case [res == null].
  if (res == NULL) {
    tscError("tres in `tscMergedStatementsCallBack` is null, which should not happen");
    free(context);
    return;
  }
  
  // handle results.
  tscDebug("async batch result callback, number of item: %zu", context->count);
  for (int i = 0; i < context->count ; ++i) {
    // the result object is shared by many sql objects.
    // therefore, we need to increase the ref count.
    taosAcquireRef(tscObjRef, res->self);
    
    Runnable* runnable = &context->runnable[i];
    runnable->fp(runnable->param, res, res == NULL ? code : taos_errno(res));
  }
  
  taosReleaseRef(tscObjRef, res->self);
  free(param);
}

/**
 * Merge the statements into single SSqlObj.
 * 
 * @param fp            the callback of SSqlObj.
 * @param param         the parameters of the callback.
 * @param statements    the sql statements represents in SArray<SSqlObj*>.
 * @return              the merged SSqlObj.
 */
static int32_t tscMergeStatements(SArray* statements, SSqlObj** result) {
  if (statements == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  
  size_t count = taosArrayGetSize(statements);
  if (count == 0) {
    return TSDB_CODE_SUCCESS;
  }
  
  // create the callback context.
  BatchCallBackContext* context = calloc(1, sizeof(BatchCallBackContext) + count * sizeof(Runnable));
  if (context == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  
  tscDebug("create batch call back context: %p", context);
  
  // initialize the callback context.
  context->count = count;
  for (size_t i = 0; i < count; ++i) {
    SSqlObj* statement = *((SSqlObj ** )taosArrayGet(statements, i));
    Runnable * callback = &context->runnable[i];
    
    callback->fp = statement->fp;
    callback->param = statement->param;
  }
  
  // merge the statements into single one.
  tscDebug("start to merge %zu sql objs", count);
  int32_t code = tscMergeKVPayLoadSqlObj(statements, result);
  if (code != TSDB_CODE_SUCCESS) {
    const char* msg = tstrerror(code);
    tscDebug("failed to merge sql objects: %s", msg);
    free(context);
  } else {
    // set the merged sql object callback.
    (*result)->fp = tscMergedStatementsCallBack;
    (*result)->fetchFp = (*result)->fp;
    (*result)->param = context;
  }
  return code;
}


/**
 * Fetch all the statements in the insertion queue, clean the insertion queue, and sent the statements to the vnodes.
 */
static void tscPollThenSendAsyncQueue() {
  // get the number of the items in the queue.
  int sizeOfQueue = taosGetQueueItemsNumber(insertionQueue);
  if (sizeOfQueue == 0) {
    return;
  }
  
  int32_t code = TSDB_CODE_SUCCESS;
  SArray* statements = taosArrayInit(0, sizeof(SSqlObj *));
  
  // out of memory.
  if (statements == NULL) {
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto cleanup;
  }
  
  // get the sql statements from the queue.
  for (int i = 0; i < sizeOfQueue; ++i) {
    // get a queue node from the queue.
    int type;
    void* node;
    if (!taosReadQitem(insertionQueue, &type, &node)) {
      break;
    }
    
    // get the SSqlObj* from the queue node.
    SSqlObj* item = *((SSqlObj **) node);
    taosFreeQitem(node);
    atomic_fetch_sub(&currentBatchLen, item->cmd.insertParam.numOfRows);
    
    // out of memory.
    if (!taosArrayPush(statements, &item)) {
      code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      tscReturnsError(item, code);
      goto cleanup;
    }
  }
  
  // no item in the queue (items has been taken by other threads).
  if (taosArrayGetSize(statements) == 0) {
    goto cleanup;
  }
  
  // merge the statements into single one.
  SSqlObj* merged = NULL;
  code = tscMergeStatements(statements, &merged);
  if (code == TSDB_CODE_SUCCESS) {
    tscDebug("merging %zu sql objs into %p", taosArrayGetSize(statements), merged);
    tscHandleMultivnodeInsert(merged);
    taosArrayDestroy(&statements);
    return;
  }

cleanup:
  if (code != TSDB_CODE_SUCCESS) {
    tscError("send async batch sql obj failed, reason: %s", tstrerror(code));
  }
  // handling the failures.
  if (statements) {
    for (int i = 0; i < taosArrayGetSize(statements); ++i) {
      SSqlObj* item = *((SSqlObj **)taosArrayGet(statements, i));
      tscReturnsError(item, code);
    }
    taosArrayDestroy(&statements);
  }
}

/**
 * The background thread to manage statement batch timeout.
 */
static void* tscAsyncBackGroundThread(void* args) {
  const int64_t timeoutUs = asyncBatchTimeout * 1000L;
  setThreadName("tscBackground");
  
  while (atomic_load(&asyncBatchEnable)) {
    // set the exclusive state.
    atomic_fetch_or(&exclusiveState, 0x1);
    
    int64_t t0 = taosGetTimestampNs();
    tscPollThenSendAsyncQueue();
    int64_t t1 = taosGetTimestampNs();
    
    // unset the exclusive state.
    atomic_fetch_and(&exclusiveState, ~0x1);
    
    int64_t durationUs = (t1 - t0) / 1000L;
    // Similar to scheduleAtFixedRate in Java, if the execution time of `tscPollThenSendAsyncQueue` exceed
    // `asyncBatchTimeout` milliseconds, then there will be no sleep.
    if (durationUs < timeoutUs) {
      usleep(timeoutUs - durationUs);
    }
  }
  return args;
}

/**
 * The initialization of async insertion auto batch feature.
 * 
 * @param batchLen  When user submit an insert statement to `taos_query_ra`, the statement will be buffered
 *                  asynchronously in the execution queue instead of executing it. If the number of the buffered
 *                  statements reach batchLen, all the statements in the queue will be merged and sent to vnodes.
 * @param timeout   The statements will be sent to vnodes no more than timeout milliseconds. But the actual time
 *                  vnodes received the statements depends on the network quality.
 */
void tscInitAsyncDispatcher(int32_t batchLen, int64_t timeout) {
  atomic_init(&asyncBatchEnable, true);

  asyncBatchLen = batchLen;
  asyncBatchTimeout = timeout;
  
  // init the queue
  insertionQueue = taosOpenQueue();
  if (insertionQueue == NULL) {
    atomic_store(&asyncBatchEnable, false);
    return;
  }
  
  // init the state.
  atomic_init(&exclusiveState, 0);
  atomic_init(&currentBatchLen, 0);
  
  // init background thread.
  if (pthread_create(&background, NULL, tscAsyncBackGroundThread, NULL)) {
    atomic_store(&asyncBatchEnable, false);
    taosCloseQueue(insertionQueue);
    return;
  }
}

/**
 * Destroy the async auto batch dispatcher.
 */
void tscDestroyAsyncDispatcher() {
  atomic_init(&asyncBatchEnable, false);
  
  // poll and send all the statements in the queue.
  while (taosGetQueueItemsNumber(insertionQueue) != 0) {
    tscPollThenSendAsyncQueue();
  }
  // clear the state.
  atomic_store(&exclusiveState, 0);
  
  // make sure the thread exit.
  pthread_join(background, NULL);
  
  // destroy the queue.
  taosCloseQueue(insertionQueue);
}

/**
 * Check if the current sql object supports auto batch.
 * 1. auto batch feature on the sql object must be enabled.
 * 2. must be an `insert into ... value ...` statement.
 * 3. the payload type must be kv payload.
 * 
 * @param pSql the sql object to check.
 * @return returns true if the sql object supports auto batch.
 */
bool tscSupportAutoBatch(SSqlObj* pSql) {
  if (pSql == NULL || !pSql->enableBatch) {
    return false;
  }
  
  SSqlCmd* pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd);
  
  
  // only support insert statement.
  if (!TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_INSERT)) {
    return false;
  }
  
  SInsertStatementParam* pInsertParam = &pCmd->insertParam;
  
  // file insert not support.
  if (TSDB_QUERY_HAS_TYPE(pInsertParam->insertType, TSDB_QUERY_TYPE_FILE_INSERT)) {
    return false;
  }
  
  // only support kv payload.
  if (pInsertParam->payloadType != PAYLOAD_TYPE_KV) {
    return false;
  }
  
  return true;
}

/**
 * Try to offer the insert statement to the queue. If the number of row reach `asyncBatchSize`, the function
 * will merge the statements in the queue and send them to the vnodes.
 * 
 * @param pSql the insert statement to offer.
 * @return     if offer success, returns true.
 */
bool tscTryOfferInsertStatements(SSqlObj* pSql) {
  if (!atomic_load(&asyncBatchEnable)) {
    return false;
  }
  
  // the sql object doesn't support auto batch.
  if (!tscSupportAutoBatch(pSql)) {
    return false;
  }
  
  // the queue is full or reach batch size.
  if (atomic_load(&currentBatchLen) >= asyncBatchLen) {
    return false;
  }
  
  // the queue is exclusive for writing.
  if (atomic_load(&exclusiveState) & 0x1) {
    return false;
  }
  
  // allocate the queue node.
  void* node = taosAllocateQitem(sizeof(SSqlObj *));
  if (node == NULL) {
    return false;
  }
  
  // offer the node to the queue.
  memcpy(node, &pSql, sizeof(SSqlObj *));
  taosWriteQitem(insertionQueue, 0, node);
  
  tscDebug("sql obj %p has been write to insert queue", pSql);
  
  // reach the batch size.
  int numsOfRows = pSql->cmd.insertParam.numOfRows;
  int batchLen = atomic_fetch_add(&currentBatchLen, numsOfRows) + numsOfRows;
  if (batchLen >= asyncBatchLen) {
    tscPollThenSendAsyncQueue();
  }
  
  return true;
}

void doAsyncQuery(STscObj* pObj, SSqlObj* pSql, __async_cb_func_t fp, void* param, const char* sqlstr, size_t sqlLen) {
  SSqlCmd* pCmd = &pSql->cmd;

  pSql->signature = pSql;
  pSql->param     = param;
  pSql->pTscObj   = pObj;
  pSql->parseRetry= 0;
  pSql->maxRetry  = TSDB_MAX_REPLICA;
  pSql->fp        = fp;
  pSql->fetchFp   = fp;
  pSql->rootObj   = pSql;

  pthread_mutex_init(&pSql->mtxSubs, NULL);

  registerSqlObj(pSql);

  pSql->sqlstr = calloc(1, sqlLen + 1);
  if (pSql->sqlstr == NULL) {
    tscError("0x%"PRIx64" failed to malloc sql string buffer", pSql->self);
    pSql->res.code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscAsyncResultOnError(pSql);
    return;
  }

  strntolower(pSql->sqlstr, sqlstr, (int32_t)sqlLen);

  tscDebugL("0x%"PRIx64" SQL: %s", pSql->self, pSql->sqlstr);
  pCmd->resColumnId = TSDB_RES_COL_ID;

  taosAcquireRef(tscObjRef, pSql->self);
  int32_t code = tsParseSql(pSql, true);

  if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
    taosReleaseRef(tscObjRef, pSql->self);
    return;
  }
  
  if (code != TSDB_CODE_SUCCESS) {
    tscReturnsError(pSql, code);
    taosReleaseRef(tscObjRef, pSql->self);
    return;
  }
  
  if (tscTryOfferInsertStatements(pSql)) {
    taosReleaseRef(tscObjRef, pSql->self);
    tscDebug("sql obj %p has been buffer in insert queue", pSql);
    return;
  }
  
  SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd);
  executeQuery(pSql, pQueryInfo);
  taosReleaseRef(tscObjRef, pSql->self);
}

// TODO return the correct error code to client in tscQueueAsyncError
void taos_query_a(TAOS *taos, const char *sqlstr, __async_cb_func_t fp, void *param) {
  taos_query_ra(taos, sqlstr, fp, param, true);
}

TAOS_RES * taos_query_ra(TAOS *taos, const char *sqlstr, __async_cb_func_t fp, void *param, bool enableBatch) {
  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    tscError("pObj:%p is NULL or freed", pObj);
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    tscQueueAsyncError(fp, param, TSDB_CODE_TSC_DISCONNECTED);
    return NULL;
  }
  
  int32_t sqlLen = (int32_t)strlen(sqlstr);
  if (sqlLen > tsMaxSQLStringLen) {
    tscError("sql string exceeds max length:%d", tsMaxSQLStringLen);
    terrno = TSDB_CODE_TSC_EXCEED_SQL_LIMIT;
    tscQueueAsyncError(fp, param, terrno);
    return NULL;
  }
  
  nPrintTsc("%s", sqlstr);
  
  SSqlObj *pSql = (SSqlObj *)calloc(1, sizeof(SSqlObj));
  if (pSql == NULL) {
    tscError("failed to malloc sqlObj");
    tscQueueAsyncError(fp, param, TSDB_CODE_TSC_OUT_OF_MEMORY);
    return NULL;
  }
  
  pSql->enableBatch = enableBatch;
  
  doAsyncQuery(pObj, pSql, fp, param, sqlstr, sqlLen);

  return pSql;
}


static void tscAsyncFetchRowsProxy(void *param, TAOS_RES *tres, int numOfRows) {
  if (tres == NULL) {
    return;
  }

  SSqlObj *pSql = (SSqlObj *)tres;
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  if (numOfRows == 0) {
    if (hasMoreVnodesToTry(pSql)) { // sequentially retrieve data from remain vnodes.
      tscTryQueryNextVnode(pSql, tscAsyncQueryRowsForNextVnode);
    } else {
      /*
       * all available virtual node has been checked already, now we need to check
       * for the next subclause queries
       */
      if (pCmd->active->sibling != NULL) {
        pCmd->active = pCmd->active->sibling;
        tscTryQueryNextClause(pSql, tscAsyncQueryRowsForNextVnode);
        return;
      }

      /*
       * 1. has reach the limitation
       * 2. no remain virtual nodes to be retrieved anymore
       */
      (*pSql->fetchFp)(param, pSql, 0);
    }
    
    return;
  }
  
  // local merge has handle this situation during super table non-projection query.
  if (pCmd->command != TSDB_SQL_RETRIEVE_GLOBALMERGE) {
    pRes->numOfClauseTotal += pRes->numOfRows;
  }

  (*pSql->fetchFp)(param, tres, numOfRows);
}

// actual continue retrieve function with user-specified callback function
static void tscProcessAsyncRetrieveImpl(void *param, TAOS_RES *tres, int numOfRows, __async_cb_func_t fp) {
  SSqlObj *pSql = (SSqlObj *)tres;
  if (pSql == NULL) {  // error
    tscError("sql object is NULL");
    return;
  }

  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  if ((pRes->qId == 0 || numOfRows != 0) && pCmd->command < TSDB_SQL_LOCAL) {
    if (pRes->qId == 0 && numOfRows != 0) {
      tscError("qhandle is NULL");
    } else {
      pRes->code = numOfRows;
    }
    if (pRes->code == TSDB_CODE_SUCCESS) {
      pRes->code = TSDB_CODE_TSC_INVALID_QHANDLE;           
    }

    tscAsyncResultOnError(pSql);
    return;
  }

  pSql->fp = fp;
  if (pCmd->command != TSDB_SQL_RETRIEVE_GLOBALMERGE && pCmd->command < TSDB_SQL_LOCAL) {
    pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
  }

  if (pCmd->command == TSDB_SQL_TABLE_JOIN_RETRIEVE) {
    tscFetchDatablockForSubquery(pSql);
  } else {
    tscBuildAndSendRequest(pSql, NULL);
  }
}

/*
 * retrieve callback for fetch rows proxy.
 * The below two functions both serve as the callback function of query virtual node.
 * query callback first, and then followed by retrieve callback
 */
static void tscAsyncQueryRowsForNextVnode(void *param, TAOS_RES *tres, int numOfRows) {
  // query completed, continue to retrieve
  tscProcessAsyncRetrieveImpl(param, tres, numOfRows, tscAsyncFetchRowsProxy);
}

void taos_fetch_rows_a(TAOS_RES *tres, __async_cb_func_t fp, void *param) {
  SSqlObj *pSql = (SSqlObj *)tres;
  if (pSql == NULL || pSql->signature != pSql) {
    tscError("sql object is NULL");
    tscQueueAsyncError(fp, param, TSDB_CODE_TSC_DISCONNECTED);
    return;
  }

  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  // user-defined callback function is stored in fetchFp
  pSql->fetchFp = fp;
  pSql->fp      = tscAsyncFetchRowsProxy;
  pSql->param   = param;

  tscResetForNextRetrieve(pRes);
  
  // handle outer query based on the already retrieved nest query results.
  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);
  if (pQueryInfo->pUpstream != NULL && taosArrayGetSize(pQueryInfo->pUpstream) > 0) {
    SSchedMsg schedMsg = {0};
    schedMsg.fp      = doRetrieveSubqueryData;
    schedMsg.ahandle = (void *)pSql;
    schedMsg.thandle = (void *)1;
    schedMsg.msg     = 0;
    taosScheduleTask(tscQhandle, &schedMsg);
    return;
  }

  if (pRes->qId == 0 && pSql->cmd.command != TSDB_SQL_RETRIEVE_EMPTY_RESULT) {
    tscError("qhandle is invalid");
    pRes->code = TSDB_CODE_TSC_INVALID_QHANDLE;
    tscAsyncResultOnError(pSql);
    return;
  }

  if (pCmd->command == TSDB_SQL_TABLE_JOIN_RETRIEVE) {
    tscFetchDatablockForSubquery(pSql);
  } else if (pRes->completed) {
    if(pCmd->command == TSDB_SQL_FETCH || (pCmd->command >= TSDB_SQL_SERV_STATUS && pCmd->command <= TSDB_SQL_CURRENT_USER)) {
      if (hasMoreVnodesToTry(pSql)) {  // sequentially retrieve data from remain vnodes.
        tscTryQueryNextVnode(pSql, tscAsyncQueryRowsForNextVnode);
      } else {
        /*
         * all available virtual nodes in current clause has been checked already, now try the
         * next one in the following union subclause
         */
        if (pCmd->active->sibling != NULL) {
          pCmd->active = pCmd->active->sibling;  // todo refactor
          tscTryQueryNextClause(pSql, tscAsyncQueryRowsForNextVnode);
          return;
        }

        /*
         * 1. has reach the limitation
         * 2. no remain virtual nodes to be retrieved anymore
         */
        (*pSql->fetchFp)(param, pSql, 0);
      }

      return;
    } else if (pCmd->command == TSDB_SQL_RETRIEVE || pCmd->command == TSDB_SQL_RETRIEVE_GLOBALMERGE) {
      // in case of show command, return no data
      (*pSql->fetchFp)(param, pSql, 0);
    } else {
      assert(0);
    }
  } else { // current query is not completed, continue retrieve from node
    if (pCmd->command != TSDB_SQL_RETRIEVE_GLOBALMERGE && pCmd->command < TSDB_SQL_LOCAL) {
      pCmd->command = (pCmd->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
    }

    SQueryInfo* pQueryInfo1 = tscGetQueryInfo(&pSql->cmd);
    tscBuildAndSendRequest(pSql, pQueryInfo1);
  }
}

// this function will be executed by queue task threads, so the terrno is not valid
static void tscProcessAsyncError(SSchedMsg *pMsg) {
  void (*fp)() = pMsg->ahandle;
  terrno = *(int32_t*) pMsg->msg;
  tfree(pMsg->msg);
  (*fp)(pMsg->thandle, NULL, terrno);
}

void tscQueueAsyncError(void(*fp), void *param, int32_t code) {
  int32_t* c = malloc(sizeof(int32_t));
  *c = code;
  
  SSchedMsg schedMsg = {0};
  schedMsg.fp = tscProcessAsyncError;
  schedMsg.ahandle = fp;
  schedMsg.thandle = param;
  schedMsg.msg = c;
  taosScheduleTask(tscQhandle, &schedMsg);
}

static void tscAsyncResultCallback(SSchedMsg *pMsg) {
  SSqlObj* pSql = (SSqlObj*)taosAcquireRef(tscObjRef, (int64_t)pMsg->ahandle);
  if (pSql == NULL || pSql->signature != pSql) {
    tscDebug("%p SqlObj is freed, not add into queue async res", pMsg->ahandle);
    return;
  }

  // probe send error , but result be responsed by server async
  if(pSql->res.code == TSDB_CODE_SUCCESS) {
    return ;
  }
  
  if (tsShortcutFlag && (pSql->res.code == TSDB_CODE_RPC_SHORTCUT)) {
    tscDebug("0x%" PRIx64 " async result callback, code:%s", pSql->self, tstrerror(pSql->res.code));
    pSql->res.code = TSDB_CODE_SUCCESS;
  } else {
    tscError("0x%" PRIx64 " async result callback, code:%s", pSql->self, tstrerror(pSql->res.code));
  }

  SSqlRes *pRes = &pSql->res;
  if (pSql->fp == NULL || pSql->fetchFp == NULL){
    taosReleaseRef(tscObjRef, pSql->self);
    return;
  }

  pSql->fp = pSql->fetchFp;
  (*pSql->fp)(pSql->param, pSql, pRes->code);
  taosReleaseRef(tscObjRef, pSql->self);
}

void tscAsyncResultOnError(SSqlObj* pSql) {
  SSchedMsg schedMsg = {0};
  schedMsg.fp = tscAsyncResultCallback;
  schedMsg.ahandle = (void *)pSql->self;
  schedMsg.thandle = (void *)1;
  schedMsg.msg = 0;
  taosScheduleTask(tscQhandle, &schedMsg);
}

int tscSendMsgToServer(SSqlObj *pSql);
void tscClearTableMeta(SSqlObj *pSql);

static void freeElem(void* p) {
  tfree(*(char**)p);
}

void tscTableMetaCallBack(void *param, TAOS_RES *res, int code) {
  SSqlObj* pSql = (SSqlObj*)taosAcquireRef(tscObjRef, (int64_t)param);
  if (pSql == NULL) return;

  assert(pSql->signature == pSql && (int64_t)param == pSql->self);

  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;
  pRes->code = code;

  SSqlObj *sub = (SSqlObj*) res;
  const char* msg = (sub->cmd.command == TSDB_SQL_STABLEVGROUP)? "vgroup-list":"multi-tableMeta";
  if (code != TSDB_CODE_SUCCESS) {
    tscError("0x%"PRIx64" get %s failed, code:%s", pSql->self, msg, tstrerror(code));
    if (code == TSDB_CODE_RPC_FQDN_ERROR) {
      size_t sz = strlen(tscGetErrorMsgPayload(&sub->cmd));
      tscAllocPayload(&pSql->cmd, (int)sz + 1); 
      memcpy(tscGetErrorMsgPayload(&pSql->cmd), tscGetErrorMsgPayload(&sub->cmd), sz);
    } else if (code == TSDB_CODE_MND_INVALID_TABLE_NAME) {
      if (sub->cmd.command == TSDB_SQL_MULTI_META && pSql->cmd.hashedTableNames) {
        tscClearTableMeta(pSql);
        taosArrayDestroyEx(&pSql->cmd.hashedTableNames, freeElem);
        pSql->cmd.hashedTableNames = NULL;
      }
    }
    goto _error;
  }

  if (sub->cmd.command == TSDB_SQL_MULTI_META) {
    if (pSql->cmd.hashedTableNames) {
      taosArrayDestroyEx(&pSql->cmd.hashedTableNames, freeElem);
      pSql->cmd.hashedTableNames = NULL;
    }
  }

  tscDebug("0x%"PRIx64" get %s successfully", pSql->self, msg);
  if (pSql->pStream == NULL) {
    SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd);

    if (pQueryInfo != NULL && TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_INSERT)) {
      tscDebug("0x%" PRIx64 " continue parse sql after get table-meta", pSql->self);

      code = tsParseSql(pSql, false);
      if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
        taosReleaseRef(tscObjRef, pSql->self);
        return;
      } else if (code != TSDB_CODE_SUCCESS) {
        goto _error;
      }

      if (TSDB_QUERY_HAS_TYPE(pCmd->insertParam.insertType, TSDB_QUERY_TYPE_STMT_INSERT)) {  // stmt insert
        (*pSql->fp)(pSql->param, pSql, code);
      } else if (TSDB_QUERY_HAS_TYPE(pCmd->insertParam.insertType, TSDB_QUERY_TYPE_FILE_INSERT)) { // file insert
        tscImportDataFromFile(pSql);
      } else {  // sql string insert
        tscHandleMultivnodeInsert(pSql);
      }
    } else {
      if (pSql->retryReason != TSDB_CODE_SUCCESS) {
        tscDebug("0x%" PRIx64 " update cached table-meta, re-validate sql statement and send query again", pSql->self);
        pSql->retryReason = TSDB_CODE_SUCCESS;
      } else {
        tscDebug("0x%" PRIx64 " cached table-meta, continue validate sql statement and send query", pSql->self);
      }

      code = tsParseSql(pSql, true);
      if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
        taosReleaseRef(tscObjRef, pSql->self);
        return;
      } else if (code != TSDB_CODE_SUCCESS) {
        goto _error;
      }

      SQueryInfo *pQueryInfo1 = tscGetQueryInfo(pCmd);
      executeQuery(pSql, pQueryInfo1);
    }

    taosReleaseRef(tscObjRef, pSql->self);
    return;
  } else {  // stream computing
    tscDebug("0x%"PRIx64" stream:%p meta is updated, start new query, command:%d", pSql->self, pSql->pStream, pCmd->command);

    SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);
    if (tscNumOfExprs(pQueryInfo) == 0) {
      tsParseSql(pSql, false);
    }

    (*pSql->fp)(pSql->param, pSql, code);
    taosReleaseRef(tscObjRef, pSql->self);
    return;
  }

  _error:
  pRes->code = code;
  tscAsyncResultOnError(pSql);
  taosReleaseRef(tscObjRef, pSql->self);
}

void tscClearTableMeta(SSqlObj *pSql) {
  SSqlCmd* pCmd = &pSql->cmd;

  int32_t n = taosArrayGetSize(pCmd->hashedTableNames);
  for (int32_t i = 0; i < n; i++) {
    char *t = taosArrayGetP(pCmd->hashedTableNames, i);
    taosHashRemove(UTIL_GET_TABLEMETA(pSql), t, strlen(t));
  }
}
