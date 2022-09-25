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

#ifndef TDENGINE_TSCBATCHWRITE_H
#define TDENGINE_TSCBATCHWRITE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tthread.h"

// forward declaration.
typedef struct SSqlObj                   SSqlObj;
typedef struct SDispatcherTimeoutManager SDispatcherTimeoutManager;

/**
 * SAsyncBatchWriteDispatcher is an async batching write dispatcher (ABWD). ABWD accepts the recent SQL requests and put
 * them in a queue waiting to be scheduled. When the number of requests in the queue reaches batch_size, it merges the
 * requests in the queue and sends them to the server, thus reducing the network overhead caused by multiple
 * communications to the server and directly improving the throughput of small object asynchronous writes.
 */
typedef struct SAsyncBatchWriteDispatcher {
  // the timeout manager.
  SDispatcherTimeoutManager* timeoutManager;

  // the mutex to protect the dispatcher.
  pthread_mutex_t bufferMutex;

  // the cond to signal when buffer not full.
  pthread_cond_t notFull;

  // the maximum number of insertion rows in a batch.
  int32_t batchSize;

  // the number of insertion rows in the buffer.
  int32_t currentSize;

  // the number of items in the buffer.
  int32_t bufferSize;

  // whether the dispatcher is shutdown.
  volatile bool shutdown;

  SSqlObj* buffer[];
} SAsyncBatchWriteDispatcher;

/**
 * The manager of SAsyncBatchWriteDispatcher. Call dispatcherAcquire(...) to get the SAsyncBatchWriteDispatcher
 * instance. SDispatcherManager will manage the life cycle of SAsyncBatchWriteDispatcher.
 */
typedef struct SDispatcherManager {
  pthread_key_t key;

  // the maximum number of insertion rows in a batch.
  int32_t batchSize;

  // the batching timeout in milliseconds.
  int32_t timeoutMs;

  // specifies whether the dispatcher is thread local, if the dispatcher is not
  // thread local, we will use the global dispatcher below.
  bool isThreadLocal;

  // the global dispatcher, if thread local enabled, global will be set to NULL.
  SAsyncBatchWriteDispatcher* global;

} SDispatcherManager;

/**
 * Control the timeout of the dispatcher queue.
 */
typedef struct SDispatcherTimeoutManager {
  // the dispatcher that timeout manager belongs to.
  SAsyncBatchWriteDispatcher* dispatcher;

  // the background thread.
  pthread_t background;

  // the mutex to sleep the background thread.
  pthread_mutex_t sleepMutex;

  // the cond to signal to background thread.
  pthread_cond_t timeout;

  // the batching timeout in milliseconds.
  int32_t timeoutMs;

  // whether the timeout manager is shutdown.
  volatile bool shutdown;
} SDispatcherTimeoutManager;

/**
 * Create the dispatcher timeout manager.
 */
SDispatcherTimeoutManager* createSDispatcherTimeoutManager(SAsyncBatchWriteDispatcher* dispatcher, int32_t timeoutMs);

/**
 * Destroy the dispatcher timeout manager.
 */
void destroySDispatcherTimeoutManager(SDispatcherTimeoutManager* manager);

/**
 * Check if the timeout manager is shutdown.
 * @param manager   the timeout manager.
 * @return          whether the timeout manager is shutdown.
 */
bool isShutdownSDispatcherTimeoutManager(SDispatcherTimeoutManager* manager);

/**
 * Shutdown the SDispatcherTimeoutManager.
 * @param manager the SDispatcherTimeoutManager.
 */
void shutdownSDispatcherTimeoutManager(SDispatcherTimeoutManager* manager);

/**
 * Merge SSqlObjs into single SSqlObj.
 *
 * @param polls  the array of SSqlObj*.
 * @param nPolls the number of SSqlObj* in the array.
 * @param batch  the merged SSqlObj*.
 * @return       the merged SSqlObj.
 */
int32_t dispatcherBatchBuilder(SSqlObj** polls, size_t nPolls, SSqlObj** batch);

/**
 * Merge the sql statements and execute the merged sql statement.
 *
 * @param polls  the array of SSqlObj*.
 * @param nPolls the number of SSqlObj* in the array.
 */
void dispatcherExecute(SSqlObj** polls, size_t nPolls);

/**
 * Create the async batch write dispatcher.
 *
 * @param batchSize When user submit an insert sql to `taos_query_a`, the SSqlObj* will be buffered instead of executing
 * it. If the number of the buffered rows reach `batchSize`, all the SSqlObj* will be merged and sent to vnodes.
 * @param timeout   The SSqlObj* will be sent to vnodes no more than `timeout` milliseconds. But the actual time
 *                  vnodes received the SSqlObj* depends on the network quality.
 */
SAsyncBatchWriteDispatcher* createSAsyncBatchWriteDispatcher(int32_t batchSize, int32_t timeoutMs);

/**
 * Destroy the async auto batch dispatcher.
 */
void destroySAsyncBatchWriteDispatcher(SAsyncBatchWriteDispatcher* dispatcher);

/**
 * Check if the current sql object can be dispatch by ABWD.
 * 1. auto batch feature on the sql object must be enabled.
 * 2. must be an `insert into ... value ...` statement.
 * 3. the payload type must be kv payload.
 * 4. no schema attached.
 *
 * @param dispatcher    the dispatcher.
 * @param pSql          the sql object to check.
 * @return              returns true if the sql object can be dispatch by ABWD.
 */
bool dispatcherCanDispatch(SAsyncBatchWriteDispatcher* dispatcher, SSqlObj* pSql);

/**
 * Try to offer the SSqlObj* to the dispatcher. If the number of row reach `batchSize`, the function
 * will merge the SSqlObj* in the buffer and send them to the vnodes.
 *
 * @param pSql the insert statement to offer.
 * @return     if offer success, returns true.
 */
bool dispatcherTryDispatch(SAsyncBatchWriteDispatcher* dispatcher, SSqlObj* pSql);

/**
 * Create the manager of SAsyncBatchWriteDispatcher.
 *
 * @param batchSize     the batchSize of SAsyncBatchWriteDispatcher.
 * @param timeoutMs     the timeoutMs of SAsyncBatchWriteDispatcher.
 * @param isThreadLocal specifies whether the dispatcher is thread local.
 * @return the SAsyncBatchWriteDispatcher manager.
 */
SDispatcherManager* createDispatcherManager(int32_t batchSize, int32_t timeoutMs, bool isThreadLocal);

/**
 * Destroy the SDispatcherManager.
 * (will destroy all the instances of SAsyncBatchWriteDispatcher in the thread local variable)
 *
 * @param manager  the SDispatcherManager.
 */
void destroyDispatcherManager(SDispatcherManager* manager);

/**
 * Get an instance of SAsyncBatchWriteDispatcher.
 *
 * @param manager   the SDispatcherManager.
 * @return          the SAsyncBatchWriteDispatcher instance.
 */
SAsyncBatchWriteDispatcher* dispatcherAcquire(SDispatcherManager* manager);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCBATCHWRITE_H
