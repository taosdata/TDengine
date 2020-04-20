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

typedef char *SSkipListKey;
typedef char *(*__sl_key_fn_t)(const void *);

/**
 * the skip list node is located in a consecutive memory area,
 * the format of skip list node is as follows:
 * +------------+-----------------------+------------------------+-----+------+
 * | node level | forward pointer array | backward pointer array | key | data |
 * +------------+-----------------------+------------------------+-----+------+
 */
typedef struct SSkipListNode {
  uint8_t level;
} SSkipListNode;

#define SL_NODE_HEADER_SIZE(_l) (sizeof(SSkipListNode) + ((_l) << 1u) * POINTER_BYTES)

#define SL_GET_FORWARD_POINTER(n, _l) ((SSkipListNode **)((char *)(n) + sizeof(SSkipListNode)))[(_l)]
#define SL_GET_BACKWARD_POINTER(n, _l) \
  ((SSkipListNode **)((char *)(n) + sizeof(SSkipListNode) + ((n)->level) * POINTER_BYTES))[(_l)]

#define SL_GET_NODE_DATA(n) ((char *)(n) + SL_NODE_HEADER_SIZE((n)->level))
#define SL_GET_NODE_KEY(s, n) ((s)->keyFn(SL_GET_NODE_DATA(n)))

#define SL_GET_SL_MIN_KEY(s) (SL_GET_NODE_KEY((s), SL_GET_FORWARD_POINTER((s)->pHead, 0)))

#define SL_GET_NODE_LEVEL(n) *(uint8_t *)((n))

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

typedef struct SSkipListKeyInfo {
  uint8_t dupKey : 2;  // if allow duplicated key in the skip list
  uint8_t type : 4;    // key type
  uint8_t freeNode:2;  // free node when destroy the skiplist
  uint8_t len;         // maximum key length, used in case of string key
} SSkipListKeyInfo;

typedef struct SSkipList {
  __compar_fn_t     comparFn;
  __sl_key_fn_t     keyFn;
  uint32_t          size;
  uint8_t           maxLevel;
  uint8_t           level;
  SSkipListKeyInfo  keyInfo;
  pthread_rwlock_t *lock;
  SSkipListNode *   pHead;    // point to the first element
  SSkipListNode *   pTail;    // point to the last element
  void *            lastKey;  // last key in the skiplist
#if SKIP_LIST_RECORD_PERFORMANCE
  tSkipListState state;  // skiplist state
#endif
} SSkipList;

/*
 * iterate the skiplist
 * this will cause the multi-thread problem, when the skiplist is destroyed, the iterate may
 * continue iterating the skiplist, so add the reference count for skiplist
 * TODO add the ref for skip list when one iterator is created
 */
typedef struct SSkipListIterator {
  SSkipList *    pSkipList;
  SSkipListNode *cur;
  int32_t        step;          // the number of nodes that have been checked already
  int32_t        order;         // order of the iterator
} SSkipListIterator;

/**
 *
 * @param nMaxLevel   maximum skip list level
 * @param keyType     type of key
 * @param dupKey      allow the duplicated key in the skip list
 * @return
 */
SSkipList *tSkipListCreate(uint8_t nMaxLevel, uint8_t keyType, uint8_t keyLen, uint8_t dupKey, uint8_t threadsafe,
    uint8_t freeNode, __sl_key_fn_t fn);

/**
 *
 * @param pSkipList
 * @return                NULL will always be returned
 */
void *tSkipListDestroy(SSkipList *pSkipList);

/**
 *
 * @param pSkipList
 * @param level
 * @param headSize
 */
void tSkipListNewNodeInfo(SSkipList *pSkipList, int32_t *level, int32_t *headSize);

/**
 * put the skip list node into the skip list.
 * If failed, NULL will be returned, otherwise, the pNode will be returned.
 *
 * @param pSkipList
 * @param pNode
 * @return
 */
SSkipListNode *tSkipListPut(SSkipList *pSkipList, SSkipListNode *pNode);

/**
 * get only *one* node of which key is equalled to pKey, even there are more than one nodes are of the same key
 *
 * @param pSkipList
 * @param pKey
 * @param keyType
 * @return
 */
SArray *tSkipListGet(SSkipList *pSkipList, SSkipListKey pKey, int16_t keyType);

/**
 * get the size of skip list
 * @param pSkipList
 * @return
 */
size_t tSkipListGetSize(const SSkipList *pSkipList);

/**
 * display skip list of the given level, for debug purpose only
 * @param pSkipList
 * @param nlevel
 */
void tSkipListPrint(SSkipList *pSkipList, int16_t nlevel);

/**
 * create skiplist iterator
 * @param pSkipList
 * @return
 */
SSkipListIterator *tSkipListCreateIter(SSkipList *pSkipList);

/**
 * create skip list iterator from the given node and specified the order
 * @param pSkipList
 * @param pNode     start position, instead of the first node in skip list
 * @param order     traverse order of the iterator
 * @return
 */
SSkipListIterator *tSkipListCreateIterFromVal(SSkipList* pSkipList, const char* val, int32_t type, int32_t order);

/**
 * forward the skip list iterator
 * @param iter
 * @return
 */
bool tSkipListIterNext(SSkipListIterator *iter);

/**
 * get the element of skip list node
 * @param iter
 * @return
 */
SSkipListNode *tSkipListIterGet(SSkipListIterator *iter);

/**
 * destroy the skip list node
 * @param iter
 * @return
 */
void *tSkipListDestroyIter(SSkipListIterator *iter);

/*
 * remove only one node of the pKey value.
 * If more than one node has the same value, any one will be removed
 *
 * @Return
 * true: one node has been removed
 * false: no node has been removed
 */
bool tSkipListRemove(SSkipList *pSkipList, SSkipListKey *pKey);

/*
 * remove the specified node in parameters
 */
void tSkipListRemoveNode(SSkipList *pSkipList, SSkipListNode *pNode);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSKIPLIST_H
