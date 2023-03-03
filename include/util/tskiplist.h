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

#ifndef _TD_UTIL_SKILIST_H
#define _TD_UTIL_SKILIST_H

#include "os.h"
#include "taos.h"
#include "tarray.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_SKIP_LIST_LEVEL          15
#define SKIP_LIST_RECORD_PERFORMANCE 0

// For key property setting
#define SL_ALLOW_DUP_KEY   (uint8_t)0x0  // Allow duplicate key exists (for tag index usage)
#define SL_DISCARD_DUP_KEY (uint8_t)0x1  // Discard duplicate key (for data update=0 case)
#define SL_UPDATE_DUP_KEY  (uint8_t)0x2  // Update duplicate key by remove/insert (for data update!=0 case)

// For thread safety setting
#define SL_THREAD_SAFE (uint8_t)0x4

typedef char *SSkipListKey;
typedef char *(*__sl_key_fn_t)(const void *);

typedef void (*sl_patch_row_fn_t)(void *pDst, const void *pSrc);
typedef void *(*iter_next_fn_t)(void *iter);

typedef struct SSkipListNode {
  uint8_t               level;
  void                 *pData;
  struct SSkipListNode *forwards[];
} SSkipListNode;

#define SL_GET_NODE_DATA(n)                (n)->pData
#define SL_NODE_GET_FORWARD_POINTER(n, l)  (n)->forwards[(l)]
#define SL_NODE_GET_BACKWARD_POINTER(n, l) (n)->forwards[(n)->level + (l)]

typedef enum { SSkipListPutSuccess = 0, SSkipListPutEarlyStop = 1, SSkipListPutSkipOne = 2 } SSkipListPutStatus;

typedef struct SSkipList {
  uint32_t           seed;
  uint16_t           len;
  __compar_fn_t      comparFn;
  __sl_key_fn_t      keyFn;
  TdThreadRwlock    *lock;
  uint8_t            maxLevel;
  uint8_t            flags;
  uint8_t            type;  // static info above
  uint8_t            level;
  uint32_t           size;
  SSkipListNode     *pHead;  // point to the first element
  SSkipListNode     *pTail;  // point to the last element
} SSkipList;

typedef struct SSkipListIterator {
  SSkipList     *pSkipList;
  SSkipListNode *cur;
  int32_t        step;   // the number of nodes that have been checked already
  int32_t        order;  // order of the iterator
  SSkipListNode *next;   // next points to the true qualified node in skiplist
} SSkipListIterator;

#define SL_IS_THREAD_SAFE(s)  (((s)->flags) & SL_THREAD_SAFE)
#define SL_DUP_MODE(s)        (((s)->flags) & ((((uint8_t)1) << 2) - 1))
#define SL_GET_NODE_KEY(s, n) ((s)->keyFn((n)->pData))
#define SL_GET_MIN_KEY(s)     SL_GET_NODE_KEY(s, SL_NODE_GET_FORWARD_POINTER((s)->pHead, 0))
#define SL_GET_MAX_KEY(s)     SL_GET_NODE_KEY((s), SL_NODE_GET_BACKWARD_POINTER((s)->pTail, 0))
#define SL_SIZE(s)            (s)->size

SSkipList *tSkipListCreate(uint8_t maxLevel, uint8_t keyType, uint16_t keyLen, __compar_fn_t comparFn, uint8_t flags,
                           __sl_key_fn_t fn);
void       tSkipListDestroy(SSkipList *pSkipList);
SSkipListNode     *tSkipListPut(SSkipList *pSkipList, void *pData);
void               tSkipListPutBatchByIter(SSkipList *pSkipList, void *iter, iter_next_fn_t iterate);
SArray            *tSkipListGet(SSkipList *pSkipList, SSkipListKey pKey);
void               tSkipListPrint(SSkipList *pSkipList, int16_t nlevel);
SSkipListIterator *tSkipListCreateIter(SSkipList *pSkipList);
SSkipListIterator *tSkipListCreateIterFromVal(SSkipList *pSkipList, const char *val, int32_t type, int32_t order);
bool               tSkipListIterNext(SSkipListIterator *iter);
SSkipListNode     *tSkipListIterGet(SSkipListIterator *iter);
void              *tSkipListDestroyIter(SSkipListIterator *iter);
uint32_t           tSkipListRemove(SSkipList *pSkipList, SSkipListKey key);
void               tSkipListRemoveNode(SSkipList *pSkipList, SSkipListNode *pNode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_SKILIST_H*/
