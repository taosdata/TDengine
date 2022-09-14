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

#ifndef _TD_UTIL_SCHED_H_
#define _TD_UTIL_SCHED_H_

#include "os.h"
#include "tdef.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SSchedMsg {
  void (*fp)(struct SSchedMsg *);
  void (*tfp)(void *, void *);
  void *msg;
  void *ahandle;
  void *thandle;
} SSchedMsg;

typedef struct {
  char          label[TSDB_LABEL_LEN];
  tsem_t        emptySem;
  tsem_t        fullSem;
  TdThreadMutex queueMutex;
  int32_t       fullSlot;
  int32_t       emptySlot;
  int32_t       queueSize;
  int32_t       numOfThreads;
  TdThread     *qthread;
  SSchedMsg    *queue;
  int8_t        stop;
  void         *pTmrCtrl;
  void         *pTimer;
} SSchedQueue;

/**
 * Create a thread-safe ring-buffer based task queue and return the instance. A thread
 * pool will be created to consume the messages in the queue.
 * @param capacity the queue capacity
 * @param numOfThreads the number of threads for the thread pool
 * @param label the label of the queue
 * @return the created queue scheduler
 */
void *taosInitScheduler(int32_t capacity, int32_t numOfThreads, const char *label, SSchedQueue *pSched);

/**
 * Create a thread-safe ring-buffer based task queue and return the instance.
 * Same as taosInitScheduler, and it also print the queue status every 1 minite.
 * @param capacity the queue capacity
 * @param numOfThreads the number of threads for the thread pool
 * @param label the label of the queue
 * @param tmrCtrl the timer controller, tmr_ctrl_t*
 * @return the created queue scheduler
 */
void *taosInitSchedulerWithInfo(int32_t capacity, int32_t numOfThreads, const char *label, void *tmrCtrl);

/**
 * Clean up the queue scheduler instance and free the memory.
 * @param queueScheduler the queue scheduler to free
 */
void taosCleanUpScheduler(void *queueScheduler);

/**
 * Schedule a new task to run, the task is described by pMsg.
 * The function may be blocked if no thread is available to execute the task.
 * That may happen when all threads are busy.
 * @param queueScheduler the queue scheduler instance
 * @param pMsg the message for the task
 */
int taosScheduleTask(void *queueScheduler, SSchedMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_SCHED_H_*/
