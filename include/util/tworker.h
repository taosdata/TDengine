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

#ifndef _TD_UTIL_WORKER_H
#define _TD_UTIL_WORKER_H

#include "tqueue.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SWorker {
  int32_t             id;      // worker ID
  pthread_t           thread;  // thread
  struct SWorkerPool *pool;
} SWorker;

typedef struct SWorkerPool {
  int32_t         max;  // max number of workers
  int32_t         min;  // min number of workers
  int32_t         num;  // current number of workers
  taos_qset       qset;
  const char     *name;
  SWorker        *workers;
  pthread_mutex_t mutex;
} SWorkerPool;

typedef struct SMWorker {
  int32_t              id;      // worker id
  pthread_t            thread;  // thread
  taos_qall            qall;
  taos_qset            qset;  // queue set
  struct SMWorkerPool *pool;
} SMWorker;

typedef struct SMWorkerPool {
  int32_t         max;     // max number of workers
  int32_t         nextId;  // from 0 to max-1, cyclic
  const char     *name;
  SMWorker       *workers;
  pthread_mutex_t mutex;
} SMWorkerPool;

int32_t    tWorkerInit(SWorkerPool *pool);
void       tWorkerCleanup(SWorkerPool *pool);
taos_queue tWorkerAllocQueue(SWorkerPool *pool, void *ahandle, FProcessItem fp);
void       tWorkerFreeQueue(SWorkerPool *pool, taos_queue queue);

int32_t    tMWorkerInit(SMWorkerPool *pool);
void       tMWorkerCleanup(SMWorkerPool *pool);
taos_queue tMWorkerAllocQueue(SMWorkerPool *pool, void *ahandle, FProcessItems fp);
void       tMWorkerFreeQueue(SMWorkerPool *pool, taos_queue queue);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_WORKER_H*/
