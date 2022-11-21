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

#include "tscBatchMerge.h"
#include "tscBatchWrite.h"
#include "tscLog.h"
#include "tscSubquery.h"
#include "tsclient.h"

/**
 * Represents the callback function and its context.
 */
typedef struct {
  __async_cb_func_t fp;
  void*             param;
} SCallbackHandler;

/**
 * The context of `batchResultCallback`.
 */
typedef struct {
  size_t           nHandlers;
  SCallbackHandler handler[];
} SBatchCallbackContext;

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
  tscAsyncResultOnError(pSql);
}

/**
 * Proxy function to perform sequentially insert operation.
 *
 * @param param     the context of `batchResultCallback`.
 * @param tres      the result object.
 * @param code      the error code.
 */
static void batchResultCallback(void* param, TAOS_RES* tres, int32_t code) {
  SBatchCallbackContext* context = param;
  SSqlObj*               res = tres;

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
  tscDebug("async batch result callback, number of item: %zu", context->nHandlers);
  for (int i = 0; i < context->nHandlers; ++i) {
    // the result object is shared by many sql objects.
    // therefore, we need to increase the ref count.
    taosAcquireRef(tscObjRef, res->self);

    SCallbackHandler* handler = &context->handler[i];
    handler->fp(handler->param, res, code);
  }

  taosReleaseRef(tscObjRef, res->self);
  free(context);
}

int32_t dispatcherBatchBuilder(SBatchRequest* pRequest, SSqlObj** batch) {
  assert(pRequest);
  assert(pRequest->pRequests);
  assert(pRequest->nRequests);

  // create the callback context.
  SBatchCallbackContext* context =
      calloc(1, sizeof(SBatchCallbackContext) + pRequest->nRequests * sizeof(SCallbackHandler));
  if (context == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  tscDebug("create batch call back context: %p", context);

  // initialize the callback context.
  context->nHandlers = pRequest->nRequests;
  for (size_t i = 0; i < pRequest->nRequests; ++i) {
    SSqlObj* pSql = pRequest->pRequests[i];
    context->handler[i].fp = pSql->fp;
    context->handler[i].param = pSql->param;
  }

  // merge the statements into single one.
  tscDebug("start to merge %zu sql objs", pRequest->nRequests);
  SSqlObj* pFirst = pRequest->pRequests[0];
  int32_t  code = tscMergeSSqlObjs(pRequest->pRequests, pRequest->nRequests, pFirst);
  if (code != TSDB_CODE_SUCCESS) {
    const char* msg = tstrerror(code);
    tscDebug("failed to merge sql objects: %s", msg);
    free(context);
    return code;
  }

  pFirst->fp = batchResultCallback;
  pFirst->param = context;
  pFirst->fetchFp = pFirst->fp;
  taosAcquireRef(tscObjRef, pFirst->self);
  *batch = pFirst;

  for (int i = 0; i < pRequest->nRequests; ++i) {
    SSqlObj* pSql = pRequest->pRequests[i];
    taosReleaseRef(tscObjRef, pSql->self);
  }
  return code;
}

/**
 * Poll all the SSqlObj* in the dispatcher's buffer (No Lock). After call this function,
 * you need to notify dispatcher->notFull by yourself.
 *
 * @param dispatcher    the dispatcher.
 * @param nPolls        the number of polled SSqlObj*.
 * @return              all the SSqlObj* in the buffer.
 */
inline static SBatchRequest* dispatcherPollAll(SAsyncBatchWriteDispatcher* dispatcher) {
  if (!dispatcher->bufferSize) {
    return NULL;
  }

  SBatchRequest* pRequest = malloc(sizeof(SBatchRequest) + sizeof(SSqlObj*) * dispatcher->bufferSize);
  if (pRequest == NULL) {
    tscError("failed to poll all items: out of memory");
    return NULL;
  }
  
  memcpy(pRequest->pRequests, dispatcher->buffer, sizeof(SSqlObj*) * dispatcher->bufferSize);
  pRequest->nRequests = dispatcher->bufferSize;
  dispatcher->currentSize = 0;
  dispatcher->bufferSize = 0;
  return pRequest;
}

/**
 * Poll all the SSqlObj* in the dispatcher's buffer.
 *
 * @param dispatcher    the dispatcher.
 * @return              all the SSqlObj* in the buffer.
 */
inline static SBatchRequest* dispatcherLockPollAll(SAsyncBatchWriteDispatcher* dispatcher) {
  SBatchRequest* pRequest = NULL;
  pthread_mutex_lock(&dispatcher->bufferMutex);
  pRequest = dispatcherPollAll(dispatcher);
  pthread_cond_broadcast(&dispatcher->notFull);
  pthread_mutex_unlock(&dispatcher->bufferMutex);
  return pRequest;
}

/**
 * @brief Try to offer the SSqlObj* to the dispatcher.
 *
 * @param dispatcher  the async bulk write dispatcher.
 * @param pSql        the sql object to offer.
 * @return            return whether offer success.
 */
inline static bool dispatcherTryOffer(SAsyncBatchWriteDispatcher* dispatcher, SSqlObj* pSql) {
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

  dispatcher->buffer[dispatcher->bufferSize++] = pSql;
  dispatcher->currentSize += statementGetInsertionRows(pSql);
  tscDebug("sql obj %p has been write to insert buffer", pSql);

  if (dispatcher->currentSize < dispatcher->batchSize) {
    pthread_mutex_unlock(&dispatcher->bufferMutex);
    return true;
  }

  // the dispatcher reaches batch size.
  SBatchRequest* pRequest = dispatcherPollAll(dispatcher);
  pthread_cond_broadcast(&dispatcher->notFull);
  pthread_mutex_unlock(&dispatcher->bufferMutex);

  if (pRequest) {
    dispatcherAsyncExecute(pRequest);
  }
  return true;
}

void dispatcherExecute(SBatchRequest* pRequest) {
  int32_t code = TSDB_CODE_SUCCESS;
  // no item in the buffer (items has been taken by other threads).
  if (!pRequest) {
    return;
  }

  assert(pRequest->pRequests);
  assert(pRequest->nRequests);

  // merge the statements into single one.
  SSqlObj* pSql = NULL;
  code = dispatcherBatchBuilder(pRequest, &pSql);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  tscDebug("merging %zu sql objs into %p", pRequest->nRequests, pSql);
  tscHandleMultivnodeInsert(pSql);
  return;
_error:
  tscError("send async batch sql obj failed, reason: %s", tstrerror(code));

  // handling the failures.
  for (size_t i = 0; i < pRequest->nRequests; ++i) {
    SSqlObj* item = pRequest->pRequests[i];
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
static inline void afterMillis(struct timespec* t, int32_t millis) {
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
inline static void timeoutManagerSleepUntil(SDispatcherTimeoutManager* manager, struct timespec* timeout) {
  pthread_mutex_lock(&manager->sleepMutex);
  while (true) {
    // notified by dispatcherShutdown(...).
    if (isShutdownSDispatcherTimeoutManager(manager)) {
      break;
    }
    if (pthread_cond_timedwait(&manager->timeout, &manager->sleepMutex, timeout)) {
      fflush(stdout);
      break;
    }
  }
  pthread_mutex_unlock(&manager->sleepMutex);
}

/**
 * The thread to manage batching timeout.
 */
static void* timeoutManagerCallback(void* arg) {
  SDispatcherTimeoutManager* manager = arg;
  setThreadName("tscAsyncBackground");

  while (!isShutdownSDispatcherTimeoutManager(manager)) {
    struct timespec timeout;
    clock_gettime(CLOCK_REALTIME, &timeout);
    afterMillis(&timeout, manager->timeoutMs);

    SBatchRequest* pRequest = dispatcherLockPollAll(manager->dispatcher);
    if (pRequest) {
      dispatcherAsyncExecute(pRequest);
    }

    // Similar to scheduleAtFixedRate in Java, if the execution time exceed
    // `timeoutMs` milliseconds, then there will be no sleep.
    timeoutManagerSleepUntil(manager, &timeout);
  }
  return NULL;
}

SAsyncBatchWriteDispatcher* createSAsyncBatchWriteDispatcher(STscObj* pClient, int32_t batchSize, int32_t timeoutMs) {
  SAsyncBatchWriteDispatcher* dispatcher = calloc(1, sizeof(SAsyncBatchWriteDispatcher) + batchSize * sizeof(SSqlObj*));
  if (!dispatcher) {
    return NULL;
  }

  assert(pClient != NULL);

  dispatcher->pClient = pClient;
  dispatcher->currentSize = 0;
  dispatcher->bufferSize = 0;
  dispatcher->batchSize = batchSize;
  atomic_store_8(&dispatcher->shutdown, false);

  // init the mutex and the cond.
  pthread_mutex_init(&dispatcher->bufferMutex, NULL);
  pthread_cond_init(&dispatcher->notFull, NULL);

  // init timeout manager.
  dispatcher->timeoutManager = createSDispatcherTimeoutManager(dispatcher, timeoutMs);
  if (!dispatcher->timeoutManager) {
    pthread_mutex_destroy(&dispatcher->bufferMutex);
    pthread_cond_destroy(&dispatcher->notFull);
    free(dispatcher);
    return NULL;
  }

  return dispatcher;
}

/**
 * Shutdown the dispatcher and join the timeout thread.
 *
 * @param dispatcher the dispatcher.
 */
inline static void dispatcherShutdown(SAsyncBatchWriteDispatcher* dispatcher) {
  atomic_store_8(&dispatcher->shutdown, true);
  if (dispatcher->timeoutManager) {
    shutdownSDispatcherTimeoutManager(dispatcher->timeoutManager);
  }
}

void destroySAsyncBatchWriteDispatcher(SAsyncBatchWriteDispatcher* dispatcher) {
  if (dispatcher == NULL) {
    return;
  }

  dispatcherShutdown(dispatcher);

  // poll and send all the statements in the buffer.
  while (true) {
    SBatchRequest* pRequest = dispatcherLockPollAll(dispatcher);
    if (!pRequest) {
      break;
    }
    dispatcherExecute(pRequest);
    free(pRequest);
  }
  // destroy the timeout manager.
  destroySDispatcherTimeoutManager(dispatcher->timeoutManager);

  // destroy the mutex.
  pthread_mutex_destroy(&dispatcher->bufferMutex);
  pthread_cond_destroy(&dispatcher->notFull);

  free(dispatcher);
}

bool dispatcherCanDispatch(SAsyncBatchWriteDispatcher* dispatcher, SSqlObj* pSql) {
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

bool dispatcherTryDispatch(SAsyncBatchWriteDispatcher* dispatcher, SSqlObj* pSql) {
  if (atomic_load_8(&dispatcher->shutdown)) {
    return false;
  }

  // the sql object doesn't support bulk insertion.
  if (!dispatcherCanDispatch(dispatcher, pSql)) {
    return false;
  }

  // try to offer pSql to the buffer.
  return dispatcherTryOffer(dispatcher, pSql);
}

/**
 * Destroy the SAsyncBatchWriteDispatcher create by SDispatcherManager.
 * @param arg the thread local SAsyncBatchWriteDispatcher.
 */
static void destroyDispatcher(void* arg) {
  SAsyncBatchWriteDispatcher* dispatcher = arg;
  if (!dispatcher) {
    return;
  }

  destroySAsyncBatchWriteDispatcher(dispatcher);
}

SDispatcherManager* createDispatcherManager(STscObj* pClient, int32_t batchSize, int32_t timeoutMs,
                                            bool isThreadLocal) {
  SDispatcherManager* dispatcher = calloc(1, sizeof(SDispatcherManager));
  if (!dispatcher) {
    return NULL;
  }

  assert(pClient != NULL);

  dispatcher->pClient = pClient;
  dispatcher->batchSize = batchSize;
  dispatcher->timeoutMs = timeoutMs;
  dispatcher->isThreadLocal = isThreadLocal;

  if (isThreadLocal) {
    if (pthread_key_create(&dispatcher->key, destroyDispatcher)) {
      free(dispatcher);
      return NULL;
    }
  } else {
    dispatcher->pGlobal = createSAsyncBatchWriteDispatcher(pClient, batchSize, timeoutMs);
    if (!dispatcher->pGlobal) {
      free(dispatcher);
      return NULL;
    }
  }
  return dispatcher;
}

SAsyncBatchWriteDispatcher* dispatcherAcquire(SDispatcherManager* manager) {
  if (!manager->isThreadLocal) {
    return manager->pGlobal;
  }

  SAsyncBatchWriteDispatcher* value = pthread_getspecific(manager->key);
  if (value) {
    return value;
  }

  value = createSAsyncBatchWriteDispatcher(manager->pClient, manager->batchSize, manager->timeoutMs);
  if (value) {
    pthread_setspecific(manager->key, value);
    return value;
  }

  return NULL;
}

void destroyDispatcherManager(SDispatcherManager* manager) {
  if (manager) {
    if (manager->isThreadLocal) {
      pthread_key_delete(manager->key);
    }

    if (manager->pGlobal) {
      destroySAsyncBatchWriteDispatcher(manager->pGlobal);
    }
    free(manager);
  }
}

SDispatcherTimeoutManager* createSDispatcherTimeoutManager(SAsyncBatchWriteDispatcher* dispatcher, int32_t timeoutMs) {
  SDispatcherTimeoutManager* manager = calloc(1, sizeof(SDispatcherTimeoutManager));
  if (!manager) {
    return NULL;
  }

  manager->timeoutMs = timeoutMs;
  manager->dispatcher = dispatcher;
  atomic_store_8(&manager->shutdown, false);

  pthread_mutex_init(&manager->sleepMutex, NULL);
  pthread_cond_init(&manager->timeout, NULL);

  // init background thread.
  if (pthread_create(&manager->background, NULL, timeoutManagerCallback, manager)) {
    pthread_mutex_destroy(&manager->sleepMutex);
    pthread_cond_destroy(&manager->timeout);
    free(manager);
    return NULL;
  }
  return manager;
}

void destroySDispatcherTimeoutManager(SDispatcherTimeoutManager* manager) {
  if (!manager) {
    return;
  }

  shutdownSDispatcherTimeoutManager(manager);
  manager->dispatcher->timeoutManager = NULL;

  pthread_mutex_destroy(&manager->sleepMutex);
  pthread_cond_destroy(&manager->timeout);
  free(manager);
}

void shutdownSDispatcherTimeoutManager(SDispatcherTimeoutManager* manager) {
  // mark shutdown, signal shutdown to timeout thread.
  pthread_mutex_lock(&manager->sleepMutex);
  atomic_store_8(&manager->shutdown, true);
  pthread_cond_broadcast(&manager->timeout);
  pthread_mutex_unlock(&manager->sleepMutex);

  // make sure the timeout thread exit.
  pthread_join(manager->background, NULL);
}

bool isShutdownSDispatcherTimeoutManager(SDispatcherTimeoutManager* manager) {
  if (!manager) {
    return true;
  }
  return atomic_load_8(&manager->shutdown);
}

/**
 * The proxy function to call `dispatcherExecute`.
 *
 * @param pMsg the schedule message.
 */
static void dispatcherExecuteProxy(struct SSchedMsg* pMsg) {
  SBatchRequest* pRequest = pMsg->ahandle;
  if (!pRequest) {
    return;
  }
  
  pMsg->ahandle = NULL;
  dispatcherExecute(pRequest);
  free(pRequest);
}

void dispatcherAsyncExecute(SBatchRequest* pRequest) {
  if (!pRequest) {
    return;
  }

  assert(pRequest->pRequests);
  assert(pRequest->nRequests);

  SSchedMsg schedMsg = {0};
  schedMsg.fp = dispatcherExecuteProxy;
  schedMsg.ahandle = (void*) pRequest;
  schedMsg.thandle = (void*) 1;
  schedMsg.msg = 0;
  taosScheduleTask(tscQhandle, &schedMsg);
}
