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

#ifndef TBASE_TSKIPLIST_H
#define TBASE_TSKIPLIST_H

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_SKIP_LIST_LEVEL 20

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

#include "ttypes.h"

/*
 * generate random data with uniform&skewed distribution, extracted from levelDB
 */
typedef struct SRandom {
  uint32_t s;

  uint32_t (*rand)(struct SRandom *, int32_t n);
} SRandom;

/*
 * key of each node
 * todo move to as the global structure in all search codes...
 */

const static size_t SKIP_LIST_STR_KEY_LENGTH_THRESHOLD = 15;
typedef tVariant    tSkipListKey;

typedef enum tSkipListPointQueryType {
  INCLUDE_POINT_QUERY,
  EXCLUDE_POINT_QUERY,
} tSkipListPointQueryType;

typedef struct tSkipListNode {
  uint16_t     nLevel;
  char *       pData;
  tSkipListKey key;

  struct tSkipListNode **pForward;
  struct tSkipListNode **pBackward;
} tSkipListNode;

/*
 * @version 0.2
 * @date   2017/11/12
 * @author liaohj
 * the simple version of SkipList.
 * for multi-thread safe purpose, we employ pthread_rwlock_t to guarantee to
 * generate
 * deterministic result. Later, we will remove the lock in SkipList to further
 * enhance the performance. In this case, one should use the concurrent skip
 * list (by
 * using michael-scott algorithm) instead of this simple version in a
 * multi-thread
 * environment, to achieve higher performance of read/write operations.
 *
 * Note: Duplicated primary key situation.
 * In case of duplicated primary key, two ways can be employed to handle this
 * situation:
 * 1. add as normal insertion with out special process.
 * 2. add an overflow pointer at each list node, all nodes with the same key
 * will be added
 *    in the overflow pointer. In this case, the total steps of each search will
 * be reduced significantly.
 *    Currently, we implement the skip list in a line with the first means,
 * maybe refactor it soon.
 * Memory consumption: the memory alignment causes many memory wasted. So,
 * employ a memory
 * pool will significantly reduce the total memory consumption, as well as the
 * calloc/malloc
 * operation costs.
 *
 * 3. use the iterator pattern to refactor all routines to make it more clean
 */

// state struct, record following information:
// number of links in each level.
// avg search steps, for latest 1000 queries
// avg search rsp time, for latest 1000 queries
// total memory size
typedef struct tSkipListState {
  // in bytes, sizeof(tSkipList)+sizeof(tSkipListNode)*tSkipList->nSize
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

typedef struct tSkipList {
  tSkipListNode pHead;
  uint64_t      nSize;

  uint16_t nMaxLevel;
  uint16_t nLevel;

  uint16_t keyType;
  uint16_t nMaxKeyLen;

  __compar_fn_t    comparator;
  pthread_rwlock_t lock;  // will be removed soon

  // random generator
  SRandom r;

  // skiplist state
  tSkipListState state;
} tSkipList;

/*
 * query condition structure to denote the range query
 * //todo merge the point query cond with range query condition
 */
typedef struct tSKipListQueryCond {
  // when the upper bounding == lower bounding, it is a point query
  tSkipListKey lowerBnd;
  tSkipListKey upperBnd;

  int32_t lowerBndRelOptr;  // relation operator to denote if lower bound is
  // included or not
  int32_t upperBndRelOptr;
} tSKipListQueryCond;

int32_t tSkipListCreate(tSkipList **pSkipList, int16_t nMaxLevel, int16_t keyType, int16_t nMaxKeyLen,
                        int32_t (*funcp)());

void tSkipListDestroy(tSkipList **pSkipList);

// create skip list key
tSkipListKey tSkipListCreateKey(int32_t type, char *val, size_t keyLength);

// destroy skip list key
void tSkipListDestroyKey(tSkipListKey *pKey);

// put data into skiplist
tSkipListNode *tSkipListPut(tSkipList *pSkipList, void *pData, tSkipListKey *pKey, int32_t insertIdenticalKey);

/*
 * get only *one* node of which key is equalled to pKey, even there are more
 * than
 * one nodes are of the same key
 */
tSkipListNode *tSkipListGetOne(tSkipList *pSkipList, tSkipListKey *pKey);

/*
 * get all data with the same keys
 */
int32_t tSkipListGets(tSkipList *pSkipList, tSkipListKey *pKey, tSkipListNode ***pRes);

int32_t tSkipListIterateList(tSkipList *pSkipList, tSkipListNode ***pRes, bool (*fp)(tSkipListNode *, void *),
                             void *param);

/*
 * remove only one node of the pKey value.
 * If more than one node has the same value, any one will be removed
 *
 * @Return
 * true: one node has been removed
 * false: no node has been removed
 */
bool tSkipListRemove(tSkipList *pSkipList, tSkipListKey *pKey);

/*
 * remove the specified node in parameters
 */
void tSkipListRemoveNode(tSkipList *pSkipList, tSkipListNode *pNode);

int32_t tSkipListDefaultCompare(tSkipList *pSkipList, tSkipListKey *a, tSkipListKey *b);

// for debug purpose only
void tSkipListPrint(tSkipList *pSkipList, int16_t nlevel);

/*
 * range query & single point query function
 */
int32_t tSkipListQuery(tSkipList *pSkipList, tSKipListQueryCond *pQueryCond, tSkipListNode ***pResult);

/*
 * include/exclude point query
 */
int32_t tSkipListPointQuery(tSkipList *pSkipList, tSkipListKey *pKey, int32_t numOfKey, tSkipListPointQueryType type,
                            tSkipListNode ***pResult);

void removeNodeEachLevel(tSkipList *pSkipList, int32_t nLevel);

// todo move to utility
void tInitMatrix(double *x, double *y, int32_t length, double p[2][3]);

int32_t tCompute(double p[2][3]);

#ifdef __cplusplus
}
#endif

#endif  // TBASE_TSKIPLIST_H
