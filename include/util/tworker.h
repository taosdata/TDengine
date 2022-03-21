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

#ifndef _TD_UTIL_WORKER_H_
#define _TD_UTIL_WORKER_H_

#include "tqueue.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SQWorkerPool SQWorkerPool;
typedef struct SWWorkerPool SWWorkerPool;

typedef struct SQWorker {
  int32_t       id;      // worker ID
  TdThread     thread;  // thread
  SQWorkerPool *pool;
} SQWorker, SFWorker;

typedef struct SQWorkerPool {
  int32_t         max;  // max number of workers
  int32_t         min;  // min number of workers
  int32_t         num;  // current number of workers
  STaosQset      *qset;
  const char     *name;
  SQWorker       *workers;
  TdThreadMutex mutex;
} SQWorkerPool, SFWorkerPool;

typedef struct SWWorker {
  int32_t       id;      // worker id
  TdThread     thread;  // thread
  STaosQall    *qall;
  STaosQset    *qset;  // queue set
  SWWorkerPool *pool;
} SWWorker;

typedef struct SWWorkerPool {
  int32_t         max;     // max number of workers
  int32_t         nextId;  // from 0 to max-1, cyclic
  const char     *name;
  SWWorker       *workers;
  TdThreadMutex mutex;
} SWWorkerPool;

int32_t     tQWorkerInit(SQWorkerPool *pool);
void        tQWorkerCleanup(SQWorkerPool *pool);
STaosQueue *tQWorkerAllocQueue(SQWorkerPool *pool, void *ahandle, FItem fp);
void        tQWorkerFreeQueue(SQWorkerPool *pool, STaosQueue *queue);

int32_t     tFWorkerInit(SFWorkerPool *pool);
void        tFWorkerCleanup(SFWorkerPool *pool);
STaosQueue *tFWorkerAllocQueue(SFWorkerPool *pool, void *ahandle, FItem fp);
void        tFWorkerFreeQueue(SFWorkerPool *pool, STaosQueue *queue);

int32_t     tWWorkerInit(SWWorkerPool *pool);
void        tWWorkerCleanup(SWWorkerPool *pool);
STaosQueue *tWWorkerAllocQueue(SWWorkerPool *pool, void *ahandle, FItems fp);
void        tWWorkerFreeQueue(SWWorkerPool *pool, STaosQueue *queue);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_WORKER_H_*/
