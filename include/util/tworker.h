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

#ifdef __cplusplus
extern "C" {
#endif

typedef int32_t (*ProcessReqFp)(void *ahandle, void *msg);
typedef void (*SendRspFp)(void *ahandle, void *msg, int32_t qtype, int32_t code);

struct SWorkerPool;

typedef struct {
  pthread_t           thread;  // thread
  int32_t             id;      // worker ID
  struct SWorkerPool *pool;
} SWorker;

typedef struct SWorkerPool {
  int32_t         max;  // max number of workers
  int32_t         min;  // min number of workers
  int32_t         num;  // current number of workers
  void *          qset;
  const char *    name;
  SWorker *       workers;
  ProcessReqFp    reqFp;
  SendRspFp       rspFp;
  pthread_mutex_t mutex;
} SWorkerPool;

int32_t tWorkerInit(SWorkerPool *pPool);
void    tWorkerCleanup(SWorkerPool *pPool);
void *  tWorkerAllocQueue(SWorkerPool *pPool, void *ahandle);
void    tWorkerFreeQueue(SWorkerPool *pPool, void *pQueue);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_WORKER_H*/
