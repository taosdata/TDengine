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

#ifndef _TD_UTIL_OBJPOOL_H_
#define _TD_UTIL_OBJPOOL_H_

#include "talgo.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TOBJPOOL_INVALID_IDX (-1)

typedef struct SObjPoolNode {
  int32_t nextIdx;
  int32_t prevIdx;
} SObjPoolNode;

#define TOBJPOOL_NODE_GET_OBJ(node) ((void *)((char *)(node) + sizeof(SObjPoolNode)))
#define TOBJPOOL_OBJ_GET_NODE(obj)  ((SObjPoolNode *)((char *)(obj) - sizeof(SObjPoolNode)))

typedef struct SObjPool {
  void   *pData;
  int64_t nodeSize;
  int64_t freeHeadIdx;
  int64_t freeTailIdx;
  int64_t size;
  int64_t capacity;
} SObjPool;

#define TOBJPOOL_GET_NODE(pool, idx) ((SObjPoolNode *)((char *)((pool)->pData) + (idx) * (pool)->nodeSize))
#define TOBJPOOL_GET_IDX(pool, node) (POINTER_DISTANCE(node, (pool)->pData) / (pool)->nodeSize)

int32_t taosObjPoolInit(SObjPool *pPool, int64_t cap, size_t objSize);
int32_t taosObjPoolEnsureCap(SObjPool *pPool, int64_t newCap);
void    taosObjPoolDestroy(SObjPool *pPool);

typedef struct SObjList {
  SObjPool *pPool;
  int64_t   neles;
  int64_t   headIdx;
  int64_t   tailIdx;
} SObjList;

typedef enum { TOBJLIST_ITER_FORWARD, TOBJLIST_ITER_BACKWARD } EObjListIterDirection;

typedef struct SObjListIter {
  SObjPool             *pPool;
  int64_t               nextIdx;
  EObjListIterDirection direction;
} SObjListIter;

int32_t taosObjListInit(SObjList *pList, SObjPool *pPool);
void    taosObjListClear(SObjList *pList);
void    taosObjListClearEx(SObjList *pList, FDelete fp);
int32_t taosObjListPrepend(SObjList *pList, const void *pData);
int32_t taosObjListAppend(SObjList *pList, const void *pData);
void    taosObjListPopHead(SObjList *pList);
void    taosObjListPopHeadEx(SObjList *pList, FDelete fp);
void    taosObjListPopTail(SObjList *pList);
void    taosObjListPopTailEx(SObjList *pList, FDelete fp);
void    taosObjListPopObj(SObjList *pList, void *pObj);
void    taosObjListPopObjEx(SObjList *pList, void *pObj, FDelete fp);

void *taosObjListGetHead(SObjList *pList);
void *taosObjListGetTail(SObjList *pList);
void  taosObjListInitIter(SObjList *pList, SObjListIter *pIter, EObjListIterDirection direction);
void *taosObjListIterNext(SObjListIter *pIter);

#ifdef __cplusplus
}
#endif

#endif /* _TD_UTIL_OBJPOOL_H_ */
