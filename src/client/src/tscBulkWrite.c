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

#include "osAtomic.h"

#include "tscDataBlockMerge.h"
#include "tscBulkWrite.h"
#include "tscLog.h"
#include "tscSubquery.h"
#include "tsclient.h"

/**
 * Represents the callback function and its context.
 */
typedef struct {
  __async_cb_func_t fp;
  void*             param;
} Runnable;

/**
 * The context of `batchResultCallback`.
 */
typedef struct {
  size_t   count;
  Runnable runnable[];
} BatchCallBackContext;

/**
 * Get the number of insertion row in the sql statement.
 *
 * @param pSql      the sql statement.
 * @return int32_t  the number of insertion row.
 */
inline static int32_t statementGetInsertionRows(SSqlObj* pSql) { return pSql->cmd.insertParam.numOfRows; }

/**
 * Return the error result to the callback function, and release the sql object.
 *
 * @param pSql  the sql object.
 * @param code  the error code of the error result.
 */
inline static void tscReturnsError(SSqlObj* pSql, int code) {
  if (pSql == NULL) {
    return;
  }

  pSql->res.code = code;
  if (pSql->fp) {
    pSql->fp(pSql->param, pSql, code);
  }
}

/**
 * Proxy function to perform sequentially insert operation.
 *
 * @param param     the context of `batchResultCallback`.
 * @param tres      the result object.
 * @param code      the error code.
 */
static void batchResultCallback(void* param, TAOS_RES* tres, int32_t code) {
  BatchCallBackContext* context = param;
  SSqlObj*              res = tres;

  // handle corner case [context == null].
  if (context == NULL) {
    tscError("context in `batchResultCallback` is null, which should not happen");
    if (tres) {
      taosReleaseRef(tscObjRef, res->self);
    }
    return;
  }

  // handle corner case [res == null].
  if (res == NULL) {
    tscError("tres in `batchResultCallback` is null, which should not happen");
    free(context);
    return;
  }

  // handle results.
  tscDebug("async batch result callback, number of item: %zu", context->count);
  for (int i = 0; i < context->count; ++i) {
    // the result object is shared by many sql objects.
    // therefore, we need to increase the ref count.
    taosAcquireRef(tscObjRef, res->self);

    Runnable* runnable = &context->runnable[i];
    runnable->fp(runnable->param, res, res == NULL ? code : taos_errno(res));
  }

  taosReleaseRef(tscObjRef, res->self);
  free(context);
}

int32_t dispatcherStatementMerge(SArray* statements, SSqlObj** result) {
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
    SSqlObj* statement = taosArrayGetP(statements, i);
    context->runnable[i].fp = statement->fp;
    context->runnable[i].param = statement->param;
  }

  // merge the statements into single one.
  tscDebug("start to merge %zu sql objs", count);
  SSqlObj *pFirst = taosArrayGetP(statements, 0);
  int32_t code = tscMergeKVPayLoadSqlObj(statements, pFirst);
  if (code != TSDB_CODE_SUCCESS) {
    const char* msg = tstrerror(code);
    tscDebug("failed to merge sql objects: %s", msg);
    free(context);
    taosReleaseRef(tscObjRef, pFirst->self);
    return code;
  }
  
  pFirst->fp = batchResultCallback;
  pFirst->param = context;
  pFirst->fetchFp = pFirst->fp;
  *result = pFirst;
  
  for (int i = 1; i < count; ++i) {
    SSqlObj *pSql = taosArrayGetP(statements, i);
    taosReleaseRef(tscObjRef, pSql->self);
  }
  return code;
}

/**
 * Poll all the SSqlObj* in the dispatcher's buffer (No Lock). After call this function,
 * you need to notify dispatcher->notFull by yourself.
 *
 * @param dispatcher    the dispatcher.
 * @return              the items in the dispatcher, SArray<SSqlObj*>.
 */
inline static SArray* dispatcherPollAll(SAsyncBulkWriteDispatcher* dispatcher) {
  if (!taosArrayGetSize(dispatcher->buffer)) {
    return NULL;
  }
  
  SArray* statements = taosArrayDup(dispatcher->buffer);
  if (statements == NULL) {
    tscError("failed to poll all items: out of memory");
    return NULL;
  }
  
  dispatcher->currentSize = 0;
  taosArrayClear(dispatcher->buffer);
  return statements;
}

/**
 * Poll all the SSqlObj* in the dispatcher's buffer.
 *
 * @param dispatcher    the dispatcher.
 * @return              the items in the dispatcher, SArray<SSqlObj*>.
 */
inline static SArray* dispatcherLockPollAll(SAsyncBulkWriteDispatcher* dispatcher) {
  pthread_mutex_lock(&dispatcher->bufferMutex);
  SArray* statements = dispatcherPollAll(dispatcher);
  pthread_cond_broadcast(&dispatcher->notFull);
  pthread_mutex_unlock(&dispatcher->bufferMutex);
  return statements;
}

/**
 * @brief Try to offer the SSqlObj* to the dispatcher.
 *
 * @param dispatcher  the async bulk write dispatcher.
 * @param pSql        the sql object to offer.
 * @return            return whether offer success.
 */
inline static bool dispatcherTryOffer(SAsyncBulkWriteDispatcher* dispatcher, SSqlObj* pSql) {
  pthread_mutex_lock(&dispatcher->bufferMutex);
  
  // if dispatcher is shutdown, must fail back to normal insertion.
  // usually not happen, unless taos_query_a(...) after taos_close(...).
  if (atomic_load_8(&dispatcher->shutdown)) {
    pthread_mutex_unlock(&dispatcher->bufferMutex);
    return false;
  }
  
  // the buffer is full.
  while (dispatcher->currentSize >= dispatcher->batchSize) {
    if (pthread_cond_wait(&dispatcher->notFull, &dispatcher->bufferMutex)) {
      pthread_mutex_unlock(&dispatcher->bufferMutex);
      return false;
    }
  }

  taosArrayPush(dispatcher->buffer, pSql);
  dispatcher->currentSize += statementGetInsertionRows(pSql);
  tscDebug("sql obj %p has been write to insert buffer", pSql);
  
  // the dispatcher has been shutdown or reach batch size.
  if (atomic_load_8(&dispatcher->shutdown) || dispatcher->currentSize >= dispatcher->batchSize) {
    SArray* statements = dispatcherPollAll(dispatcher);
    dispatcherExecute(statements);
    taosArrayDestroy(&statements);
    pthread_cond_broadcast(&dispatcher->notFull);
  }
  pthread_mutex_unlock(&dispatcher->bufferMutex);
  return true;
}

void dispatcherExecute(SArray* statements) {
  int32_t code = TSDB_CODE_SUCCESS;
  // no item in the buffer (items has been taken by other threads).
  if (!statements || !taosArrayGetSize(statements)) {
    return;
  }

  // merge the statements into single one.
  SSqlObj* merged = NULL;
  code = dispatcherStatementMerge(statements, &merged);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  tscDebug("merging %zu sql objs into %p", taosArrayGetSize(statements), merged);
  tscHandleMultivnodeInsert(merged);
  return;
_error:
  tscError("send async batch sql obj failed, reason: %s", tstrerror(code));

  // handling the failures.
  for (size_t i = 0; i < taosArrayGetSize(statements); ++i) {
    SSqlObj* item = taosArrayGetP(statements, i);
    tscReturnsError(item, code);
  }
}

/**
 * Get the timespec after `millis` ms
 *
 * @param t         the timespec.
 * @param millis    the duration in milliseconds.
 * @return          the timespec after `millis` ms.
 */
static inline void afterMillis(struct timespec *t, int32_t millis) {
  t->tv_nsec += millis * 1000000L;
  t->tv_sec += t->tv_nsec / 1000000000L;
  t->tv_nsec %= 1000000000L;
}

/**
 * Sleep until `timeout` timespec. When dispatcherShutdown(...) called, the function will return immediately.
 * 
 * @param dispatcher the dispatcher thread to sleep.
 * @param timeout    the timeout in CLOCK_REALTIME.
 */
inline static void dispatcherSleepUntil(SAsyncBulkWriteDispatcher* dispatcher, struct timespec* timeout) {
  pthread_mutex_lock(&dispatcher->sleepMutex);
  while (true) {
    // notified by dispatcherShutdown(...).
    if (atomic_load_8(&dispatcher->shutdown)) {
      break;
    }
    if (pthread_cond_timedwait(&dispatcher->timeout, &dispatcher->sleepMutex, timeout)) {
      fflush(stdout);
      break;
    }
  }
  pthread_mutex_unlock(&dispatcher->sleepMutex);
}

/**
 * The thread to manage batching timeout.
 */
static void* dispatcherTimeoutCallback(void* arg) {
  SAsyncBulkWriteDispatcher* dispatcher = arg;
  setThreadName("tscAsyncBackground");

  while (!atomic_load_8(&dispatcher->shutdown)) {
    struct timespec timeout;
    clock_gettime(CLOCK_REALTIME, &timeout);
    afterMillis(&timeout, dispatcher->timeoutMs);
    
    SArray* statements = dispatcherLockPollAll(dispatcher);
    dispatcherExecute(statements);
    taosArrayDestroy(&statements);
    
    // Similar to scheduleAtFixedRate in Java, if the execution time exceed
    // `timeoutMs` milliseconds, then there will be no sleep.
    dispatcherSleepUntil(dispatcher, &timeout);
  }
  return NULL;
}

SAsyncBulkWriteDispatcher* createAsyncBulkWriteDispatcher(int32_t batchSize, int32_t timeoutMs) {
  SAsyncBulkWriteDispatcher* dispatcher = calloc(1, sizeof(SAsyncBulkWriteDispatcher));
  if (!dispatcher) {
    return NULL;
  }
  
  dispatcher->currentSize = 0;
  dispatcher->batchSize = batchSize;
  dispatcher->timeoutMs = timeoutMs;
  
  atomic_store_8(&dispatcher->shutdown, false);

  // init the buffer.
  dispatcher->buffer = taosArrayInit(batchSize, sizeof(SSqlObj*));
  if (!dispatcher->buffer) {
    tfree(dispatcher);
    return NULL;
  }

  // init the mutex and the cond.
  pthread_mutex_init(&dispatcher->bufferMutex, NULL);
  pthread_mutex_init(&dispatcher->sleepMutex, NULL);
  pthread_cond_init(&dispatcher->timeout, NULL);
  pthread_cond_init(&dispatcher->notFull, NULL);

  // init background thread.
  if (pthread_create(&dispatcher->background, NULL, dispatcherTimeoutCallback, dispatcher)) {
    pthread_mutex_destroy(&dispatcher->bufferMutex);
    pthread_mutex_destroy(&dispatcher->sleepMutex);
    pthread_cond_destroy(&dispatcher->timeout);
    pthread_cond_destroy(&dispatcher->notFull);
    taosArrayDestroy(&dispatcher->buffer);
    tfree(dispatcher);
    return NULL;
  }

  return dispatcher;
}

/**
 * Shutdown the dispatcher and join the timeout thread.
 * 
 * @param dispatcher the dispatcher.
 */
inline static void dispatcherShutdown(SAsyncBulkWriteDispatcher* dispatcher) {
  // mark shutdown, signal shutdown to timeout thread.
  pthread_mutex_lock(&dispatcher->sleepMutex);
  atomic_store_8(&dispatcher->shutdown, true);
  pthread_cond_broadcast(&dispatcher->timeout);
  pthread_mutex_unlock(&dispatcher->sleepMutex);
  
  // make sure the timeout thread exit.
  pthread_join(dispatcher->background, NULL);
}

void destroyAsyncDispatcher(SAsyncBulkWriteDispatcher* dispatcher) {
  if (dispatcher == NULL) {
    return;
  }

  dispatcherShutdown(dispatcher);

  // poll and send all the statements in the buffer.
  while (true) {
    SArray* statements = dispatcherLockPollAll(dispatcher);
    if (!statements) {
      break ;
    }
    
    dispatcherExecute(statements);
    taosArrayDestroy(&statements);
  }

  // destroy the buffer.
  taosArrayDestroy(&dispatcher->buffer);

  // destroy the mutex.
  pthread_mutex_destroy(&dispatcher->bufferMutex);
  pthread_mutex_destroy(&dispatcher->sleepMutex);
  pthread_cond_destroy(&dispatcher->timeout);
  pthread_cond_destroy(&dispatcher->notFull);

  free(dispatcher);
}

bool tscSupportBulkInsertion(SAsyncBulkWriteDispatcher* dispatcher, SSqlObj* pSql) {
  if (pSql == NULL || !pSql->enableBatch) {
    return false;
  }

  SSqlCmd*    pCmd = &pSql->cmd;
  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);

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
  
  // no schema attached.
  if (pInsertParam->schemaAttached) {
    return false;
  }
  
  // too many insertion rows, fail back to normal insertion.
  if (statementGetInsertionRows(pSql) >= dispatcher->batchSize) {
    return false;
  }

  return true;
}

bool dispatcherTryDispatch(SAsyncBulkWriteDispatcher* dispatcher, SSqlObj* pSql) {
  if (atomic_load_8(&dispatcher->shutdown)) {
    return false;
  }

  // the sql object doesn't support bulk insertion.
  if (!tscSupportBulkInsertion(dispatcher, pSql)) {
    return false;
  }

  // try to offer pSql to the buffer.
  return dispatcherTryOffer(dispatcher, pSql);
}

/**
 * Destroy the SAsyncBulkWriteDispatcher create by SDispatcherHolder.
 * @param arg the thread local SAsyncBulkWriteDispatcher.
 */
static void destroyDispatcher(void* arg) {
  SAsyncBulkWriteDispatcher* dispatcher = arg;
  if (!dispatcher) {
    return;
  }

  destroyAsyncDispatcher(dispatcher);
}

SDispatcherHolder* createDispatcherHolder(int32_t batchSize, int32_t timeoutMs, bool isThreadLocal) {
  SDispatcherHolder* dispatcher = calloc(1, sizeof(SDispatcherHolder));
  if (!dispatcher) {
    return NULL;
  }

  dispatcher->batchSize = batchSize;
  dispatcher->timeoutMs = timeoutMs;
  dispatcher->isThreadLocal = isThreadLocal;

  if (isThreadLocal) {
    if (pthread_key_create(&dispatcher->key, destroyDispatcher)) {
      free(dispatcher);
      return NULL;
    }
  } else {
    dispatcher->global = createAsyncBulkWriteDispatcher(batchSize, timeoutMs);
    if (!dispatcher->global) {
      free(dispatcher);
      return NULL;
    }
  }
  return dispatcher;
}

SAsyncBulkWriteDispatcher* dispatcherAcquire(SDispatcherHolder* holder) {
  if (!holder->isThreadLocal) {
    return holder->global;
  }

  SAsyncBulkWriteDispatcher* value = pthread_getspecific(holder->key);
  if (value) {
    return value;
  }

  value = createAsyncBulkWriteDispatcher(holder->batchSize, holder->timeoutMs);
  if (value) {
    pthread_setspecific(holder->key, value);
    return value;
  }

  return NULL;
}

void destroyDispatcherHolder(SDispatcherHolder* holder) {
  if (holder) {
    if (holder->isThreadLocal) {
      pthread_key_delete(holder->key);
    } else {
      destroyAsyncDispatcher(holder->global);
    }
    free(holder);
  }
}
