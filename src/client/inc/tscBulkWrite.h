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

#ifndef TDENGINE_TSCBULKWRITE_H
#define TDENGINE_TSCBULKWRITE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tarray.h"
#include "tthread.h"

// forward declaration.
typedef struct SSqlObj SSqlObj;

/**
 * SAsyncBulkWriteDispatcher is an async batching dispatcher(for writing), it can buffer insertion statements, batch
 * and merge them into single statement.
 */
typedef struct SAsyncBulkWriteDispatcher {
  // the buffer to store the insertion statements. equivalent to SArray<SSqlObj*>.
  SArray* buffer;

  // the mutex to protect the dispatcher.
  pthread_mutex_t mutex;
  
  // the cond to signal to background thread.
  pthread_cond_t timeout;
  
  // the cond to signal to background thread.
  pthread_cond_t notFull;

  // the background thread to manage batching timeout.
  pthread_t background;

  // the maximum number of insertion rows in a batch.
  int32_t batchSize;

  // the batching timeout in milliseconds.
  int32_t timeoutMs;
  
  // the number of insertion rows in the buffer.
  int32_t currentSize;

  // whether the dispatcher is shutdown.
  volatile bool shutdown;
  
} SAsyncBulkWriteDispatcher;

// forward declaration.
typedef struct SSqlObj SSqlObj;

/**
 * Merge the statements into single SSqlObj.
 *
 * @param fp            the callback of SSqlObj.
 * @param param         the parameters of the callback.
 * @param statements    the sql statements represents in SArray<SSqlObj*>.
 * @return              the merged SSqlObj.
 */
int32_t dispatcherStatementMerge(SArray* statements, SSqlObj** result);

/**
 * Merge the sql statements and execute the merged sql statement.
 *
 * @param statements the array of sql statement. a.k.a SArray<SSqlObj*>.
 */
void dispatcherExecute(SArray* statements);

/**
 * Create the async bulk write dispatcher.
 *
 * @param batchSize When user submit an insert statement to `taos_query_ra`, the statement will be buffered
 *                  asynchronously in the buffer instead of executing it. If the number of the buffered
 *                  statements reach batchLen, all the statements in the buffer will be merged and sent to vnodes.
 * @param timeout   The statements will be sent to vnodes no more than timeout milliseconds. But the actual time
 *                  vnodes received the statements depends on the network quality.
 */
SAsyncBulkWriteDispatcher* createAsyncBulkWriteDispatcher(int32_t batchSize, int32_t timeoutMs);

/**
 * Destroy the async auto batch dispatcher.
 */
void destroyAsyncDispatcher(SAsyncBulkWriteDispatcher* dispatcher);

/**
 * Check if the current sql object supports bulk insertion.
 * 1. auto batch feature on the sql object must be enabled.
 * 2. must be an `insert into ... value ...` statement.
 * 3. the payload type must be kv payload.
 * 
 * @param dispatcher the async dispatcher.
 * @param pSql the sql object to check.
 * @return returns true if the sql object supports auto batch.
 */
bool tscSupportBulkInsertion(SAsyncBulkWriteDispatcher* dispatcher, SSqlObj* pSql);

/**
 * Try to offer the SSqlObj* to the dispatcher. If the number of row reach `batchSize`, the function
 * will merge the SSqlObj* in the buffer and send them to the vnodes.
 *
 * @param pSql the insert statement to offer.
 * @return     if offer success, returns true.
 */
bool dispatcherTryDispatch(SAsyncBulkWriteDispatcher* dispatcher, SSqlObj* pSql);

/**
 * A holder of SAsyncBulkWriteDispatcher. Call dispatcherAcquire(...) to get the SAsyncBulkWriteDispatcher
 * instance. This holder will manage the life cycle of SAsyncBulkWriteDispatcher.
 */
typedef struct SDispatcherHolder {
  pthread_key_t key;

  // the maximum number of insertion rows in a batch.
  int32_t batchSize;

  // the batching timeout in milliseconds.
  int32_t timeoutMs;
  
  // specifies whether the dispatcher is thread local, if the dispatcher is not
  // thread local, we will use the global dispatcher below.
  bool isThreadLocal;
  
  // the global dispatcher, if thread local enabled, global will be set to NULL.
  SAsyncBulkWriteDispatcher * global;

} SDispatcherHolder;

/**
 * Create a holder of SAsyncBulkWriteDispatcher.
 *
 * @param batchSize     the batchSize of SAsyncBulkWriteDispatcher.
 * @param timeoutMs     the timeoutMs of SAsyncBulkWriteDispatcher.
 * @param isThreadLocal specifies whether the dispatcher is thread local.
 * @return the SAsyncBulkWriteDispatcher holder.
 */
SDispatcherHolder* createDispatcherHolder(int32_t batchSize, int32_t timeoutMs, bool isThreadLocal);

/**
 * Destroy the holder of SAsyncBulkWriteDispatcher.
 * (will destroy all the instances of SAsyncBulkWriteDispatcher in the thread local variable)
 *
 * @param holder  the holder of SAsyncBulkWriteDispatcher.
 */
void destroyDispatcherHolder(SDispatcherHolder* holder);

/**
 * Get an instance of SAsyncBulkWriteDispatcher.
 * 
 * @param holder  the holder of SAsyncBulkWriteDispatcher.
 * @return the SAsyncBulkWriteDispatcher instance.
 */
SAsyncBulkWriteDispatcher* dispatcherAcquire(SDispatcherHolder* holder);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCBULKWRITE_H
