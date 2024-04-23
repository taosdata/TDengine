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

#ifndef _TD_UTIL_QUEUE_H_
#define _TD_UTIL_QUEUE_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

/*

This set of API for queue is designed specially for vnode/mnode. The main purpose is to
consume all the items instead of one item from a queue by one single read. Also, it can
combine multiple queues into a queue set, a consumer thread can consume a queue set via
a single API instead of looping every queue by itself.

Notes:
1: taosOpenQueue/taosCloseQueue, taosOpenQset/taosCloseQset is NOT multi-thread safe
2: after taosCloseQueue/taosCloseQset is called, read/write operation APIs are not safe.
3: read/write operation APIs are multi-thread safe

To remove the limitation and make this set of queue APIs multi-thread safe, REF(tref.c)
shall be used to set up the protection.

*/

typedef struct STaosQueue STaosQueue;
typedef struct STaosQset  STaosQset;
typedef struct STaosQall  STaosQall;
typedef struct {
  void   *ahandle;
  void   *fp;
  void   *queue;
  int32_t workerId;
  int32_t threadNum;
  int64_t timestamp;
} SQueueInfo;

typedef enum {
  DEF_QITEM = 0,
  RPC_QITEM = 1,
} EQItype;

typedef void (*FItem)(SQueueInfo *pInfo, void *pItem);
typedef void (*FItems)(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfItems);

typedef struct STaosQnode STaosQnode;

struct STaosQnode {
  STaosQnode *next;
  STaosQueue *queue;
  int64_t     timestamp;
  int64_t     dataSize;
  int32_t     size;
  int8_t      itype;
  int8_t      reserved[3];
  char        item[];
};

STaosQueue *taosOpenQueue();
void        taosCloseQueue(STaosQueue *queue);
void        taosSetQueueFp(STaosQueue *queue, FItem itemFp, FItems itemsFp);
void       *taosAllocateQitem(int32_t size, EQItype itype, int64_t dataSize);
void        taosFreeQitem(void *pItem);
int32_t     taosWriteQitem(STaosQueue *queue, void *pItem);
int32_t     taosReadQitem(STaosQueue *queue, void **ppItem);
bool        taosQueueEmpty(STaosQueue *queue);
void        taosUpdateItemSize(STaosQueue *queue, int32_t items);
int32_t     taosQueueItemSize(STaosQueue *queue);
int64_t     taosQueueMemorySize(STaosQueue *queue);
void        taosSetQueueCapacity(STaosQueue *queue, int64_t size);
void        taosSetQueueMemoryCapacity(STaosQueue *queue, int64_t mem);

STaosQall *taosAllocateQall();
void       taosFreeQall(STaosQall *qall);
int32_t    taosReadAllQitems(STaosQueue *queue, STaosQall *qall);
int32_t    taosGetQitem(STaosQall *qall, void **ppItem);
void       taosResetQitems(STaosQall *qall);
int32_t    taosQallItemSize(STaosQall *qall);
int64_t    taosQallMemSize(STaosQall *qll);
int64_t    taosQallUnAccessedItemSize(STaosQall *qall);
int64_t    taosQallUnAccessedMemSize(STaosQall *qall);

STaosQset *taosOpenQset();
void       taosCloseQset(STaosQset *qset);
void       taosQsetThreadResume(STaosQset *qset);
int32_t    taosAddIntoQset(STaosQset *qset, STaosQueue *queue, void *ahandle);
void       taosRemoveFromQset(STaosQset *qset, STaosQueue *queue);
int32_t    taosGetQueueNumber(STaosQset *qset);

int32_t taosReadQitemFromQset(STaosQset *qset, void **ppItem, SQueueInfo *qinfo);
int32_t taosReadAllQitemsFromQset(STaosQset *qset, STaosQall *qall, SQueueInfo *qinfo);
void    taosResetQsetThread(STaosQset *qset, void *pItem);
void    taosQueueSetThreadId(STaosQueue *pQueue, int64_t threadId);
int64_t taosQueueGetThreadId(STaosQueue *pQueue);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_QUEUE_H_*/
