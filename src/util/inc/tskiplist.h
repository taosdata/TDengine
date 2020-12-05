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

#ifndef TDENGINE_TSKIPLIST_H
#define TDENGINE_TSKIPLIST_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "taosdef.h"
#include "tarray.h"

#define MAX_SKIP_LIST_LEVEL 15
#define SKIP_LIST_RECORD_PERFORMANCE 0

// For key property setting
#define SL_ALLOW_DUP_KEY (uint8_t)0x0    // Allow duplicate key exists (for tag index usage)
#define SL_DISCARD_DUP_KEY (uint8_t)0x1  // Discard duplicate key (for data update=0 case)
#define SL_UPDATE_DUP_KEY (uint8_t)0x2   // Update duplicate key by remove/insert (for data update=1 case)
// For thread safety setting
#define SL_THREAD_SAFE (uint8_t)0x4

typedef char *SSkipListKey;
typedef char *(*__sl_key_fn_t)(const void *);

typedef struct SSkipListNode {
  uint8_t        level;
  void *         pData;
  struct SSkipListNode *forwards[];
} SSkipListNode;

#define SL_GET_NODE_DATA(n) (n)->pData
#define SL_NODE_GET_FORWARD_POINTER(n, l) (n)->forwards[(l)]
#define SL_NODE_GET_BACKWARD_POINTER(n, l) (n)->forwards[(n)->level + (l)]

/*
 * @version 0.3
 * @date   2017/11/12
 * the simple version of skip list.
 *
 * for multi-thread safe purpose, we employ pthread_rwlock_t to guarantee to generate
 * deterministic result. Later, we will remove the lock in SkipList to further enhance the performance.
 * In this case, one should use the concurrent skip list (by using michael-scott algorithm) instead of
 * this simple version in a multi-thread environment, to achieve higher performance of read/write operations.
 *
 * Note: Duplicated primary key situation.
 * In case of duplicated primary key, two ways can be employed to handle this situation:
 * 1. add as normal insertion without special process.
 * 2. add an overflow pointer at each list node, all nodes with the same key will be added in the overflow pointer.
 *    In this case, the total steps of each search will be reduced significantly.
 *    Currently, we implement the skip list in a line with the first means, maybe refactor it soon.
 *
 *    Memory consumption: the memory alignment causes many memory wasted. So, employ a memory
 *    pool will significantly reduce the total memory consumption, as well as the calloc/malloc operation costs.
 *
 */

// state struct, record following information:
// number of links in each level.
// avg search steps, for latest 1000 queries
// avg search rsp time, for latest 1000 queries
// total memory size
typedef struct tSkipListState {
  // in bytes, sizeof(SSkipList)+sizeof(SSkipListNode)*SSkipList->nSize
  uint64_t nTotalMemSize;
  uint64_t nLevelNodeCnt[MAX_SKIP_LIST_LEVEL];
  uint64_t queryCount;  // total query count

  /*
   * only record latest 1000 queries
   * when the value==1000, = 0,
   * nTotalStepsForQueries = 0,
   * nTotalElapsedTimeForQueries = 0
   */
  uint64_t nRecQueries;
  uint16_t nTotalStepsForQueries;
  uint64_t nTotalElapsedTimeForQueries;

  uint16_t nInsertObjs;
  uint16_t nTotalStepsForInsert;
  uint64_t nTotalElapsedTimeForInsert;
} tSkipListState;

typedef struct SSkipList {
  __compar_fn_t     comparFn;
  __sl_key_fn_t     keyFn;
  pthread_rwlock_t *lock;
  uint16_t          len;
  uint8_t           maxLevel;
  uint8_t           flags;
  uint8_t           type;  // static info above
  uint8_t           level;
  uint32_t          size;
  SSkipListNode *   pHead;  // point to the first element
  SSkipListNode *   pTail;  // point to the last element
#if SKIP_LIST_RECORD_PERFORMANCE
  tSkipListState state;  // skiplist state
#endif
} SSkipList;

typedef struct SSkipListIterator {
  SSkipList *    pSkipList;
  SSkipListNode *cur;
  int32_t        step;          // the number of nodes that have been checked already
  int32_t        order;         // order of the iterator
  SSkipListNode *next;          // next points to the true qualified node in skip list
} SSkipListIterator;

#define SL_IS_THREAD_SAFE(s) (((s)->flags) & SL_THREAD_SAFE)
#define SL_DUP_MODE(s) (((s)->flags) & ((((uint8_t)1) << 2) - 1))
#define SL_GET_NODE_KEY(s, n) ((s)->keyFn((n)->pData))
#define SL_GET_MIN_KEY(s) SL_GET_NODE_KEY(s, SL_NODE_GET_FORWARD_POINTER((s)->pHead, 0))
#define SL_GET_MAX_KEY(s) SL_GET_NODE_KEY((s), SL_NODE_GET_BACKWARD_POINTER((s)->pTail, 0))
#define SL_SIZE(s) (s)->size

SSkipList *tSkipListCreate(uint8_t maxLevel, uint8_t keyType, uint16_t keyLen, __compar_fn_t comparFn, uint8_t flags,
                           __sl_key_fn_t fn);
void       tSkipListDestroy(SSkipList *pSkipList);
SSkipListNode *    tSkipListPut(SSkipList *pSkipList, void *pData);
void               tSkipListPutBatch(SSkipList *pSkipList, void **ppData, int ndata);
SArray *           tSkipListGet(SSkipList *pSkipList, SSkipListKey pKey);
void               tSkipListPrint(SSkipList *pSkipList, int16_t nlevel);
SSkipListIterator *tSkipListCreateIter(SSkipList *pSkipList);
SSkipListIterator *tSkipListCreateIterFromVal(SSkipList *pSkipList, const char *val, int32_t type, int32_t order);
bool               tSkipListIterNext(SSkipListIterator *iter);
SSkipListNode *    tSkipListIterGet(SSkipListIterator *iter);
void *             tSkipListDestroyIter(SSkipListIterator *iter);
uint32_t           tSkipListRemove(SSkipList *pSkipList, SSkipListKey key);
void               tSkipListRemoveNode(SSkipList *pSkipList, SSkipListNode *pNode);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSKIPLIST_H
