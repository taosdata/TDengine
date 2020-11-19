/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
#include "tskiplist.h"
#include "os.h"
#include "tcompare.h"
#include "tulog.h"
#include "tutil.h"

static int                initForwardBackwardPtr(SSkipList *pSkipList);
static SSkipListNode *    getPriorNode(SSkipList *pSkipList, const char *val, int32_t order, SSkipListNode **pCur);
static void               tSkipListRemoveNodeImpl(SSkipList *pSkipList, SSkipListNode *pNode);
static void               tSkipListCorrectLevel(SSkipList *pSkipList);
static SSkipListIterator *doCreateSkipListIterator(SSkipList *pSkipList, int32_t order);
static void tSkipListDoInsert(SSkipList *pSkipList, SSkipListNode **direction, SSkipListNode *pNode, bool isForward);
static bool tSkipListGetPosToPut(SSkipList *pSkipList, SSkipListNode **backward, void *pData);
static SSkipListNode *tSkipListNewNode(uint8_t level);
#define tSkipListFreeNode(n) tfree((n))
static SSkipListNode *tSkipListPutImpl(SSkipList *pSkipList, void *pData, SSkipListNode **direction, bool isForward,
                                       bool hasDup);

static FORCE_INLINE int     tSkipListWLock(SSkipList *pSkipList);
static FORCE_INLINE int     tSkipListRLock(SSkipList *pSkipList);
static FORCE_INLINE int     tSkipListUnlock(SSkipList *pSkipList);
static FORCE_INLINE int32_t getSkipListRandLevel(SSkipList *pSkipList);

SSkipList *tSkipListCreate(uint8_t maxLevel, uint8_t keyType, uint16_t keyLen, __compar_fn_t comparFn, uint8_t flags,
                           __sl_key_fn_t fn) {
  SSkipList *pSkipList = (SSkipList *)calloc(1, sizeof(SSkipList));
  if (pSkipList == NULL) return NULL;

  if (maxLevel > MAX_SKIP_LIST_LEVEL) {
    maxLevel = MAX_SKIP_LIST_LEVEL;
  }

  pSkipList->maxLevel = maxLevel;
  pSkipList->type = keyType;
  pSkipList->len = keyLen;
  pSkipList->flags = flags;
  pSkipList->keyFn = fn;
  if (comparFn == NULL) {
    pSkipList->comparFn = getKeyComparFunc(keyType);
  } else {
    pSkipList->comparFn = comparFn;
  }

  if (initForwardBackwardPtr(pSkipList) < 0) {
    tSkipListDestroy(pSkipList);
    return NULL;
  }

  if (SL_IS_THREAD_SAFE(pSkipList)) {
    pSkipList->lock = (pthread_rwlock_t *)calloc(1, sizeof(pthread_rwlock_t));
    if (pSkipList->lock == NULL) {
      tSkipListDestroy(pSkipList);
      return NULL;
    }

    if (pthread_rwlock_init(pSkipList->lock, NULL) != 0) {
      tSkipListDestroy(pSkipList);
      return NULL;
    }
  }

  srand((uint32_t)time(NULL));

#if SKIP_LIST_RECORD_PERFORMANCE
  pSkipList->state.nTotalMemSize += sizeof(SSkipList);
#endif

  return pSkipList;
}

void tSkipListDestroy(SSkipList *pSkipList) {
  if (pSkipList == NULL) return;

  tSkipListWLock(pSkipList);

  SSkipListNode *pNode = SL_NODE_GET_FORWARD_POINTER(pSkipList->pHead, 0);

  while (pNode != pSkipList->pTail) {
    SSkipListNode *pTemp = pNode;
    pNode = SL_NODE_GET_FORWARD_POINTER(pNode, 0);
    tSkipListFreeNode(pTemp);
  }

  tSkipListUnlock(pSkipList);
  if (pSkipList->lock != NULL) {
    pthread_rwlock_destroy(pSkipList->lock);
    tfree(pSkipList->lock);
  }

  tSkipListFreeNode(pSkipList->pHead);
  tSkipListFreeNode(pSkipList->pTail);
  tfree(pSkipList);
}

SSkipListNode *tSkipListPut(SSkipList *pSkipList, void *pData) {
  if (pSkipList == NULL || pData == NULL) return NULL;

  SSkipListNode *backward[MAX_SKIP_LIST_LEVEL] = {0};
  SSkipListNode *pNode = NULL;

  tSkipListWLock(pSkipList);

  bool hasDup = tSkipListGetPosToPut(pSkipList, backward, pData);
  pNode = tSkipListPutImpl(pSkipList, pData, backward, false, hasDup);

  tSkipListUnlock(pSkipList);

  return pNode;
}

// Put a batch of data into skiplist. The batch of data must be in ascending order
void tSkipListPutBatch(SSkipList *pSkipList, void **ppData, int ndata) {
  SSkipListNode *backward[MAX_SKIP_LIST_LEVEL] = {0};
  SSkipListNode *forward[MAX_SKIP_LIST_LEVEL] = {0};
  bool           hasDup = false;
  char *         pKey = NULL;
  char *         pDataKey = NULL;
  int            compare = 0;

  tSkipListWLock(pSkipList);

  // backward to put the first data
  hasDup = tSkipListGetPosToPut(pSkipList, backward, ppData[0]);
  tSkipListPutImpl(pSkipList, ppData[0], backward, false, hasDup);

  for (int level = 0; level < pSkipList->maxLevel; level++) {
    forward[level] = SL_NODE_GET_BACKWARD_POINTER(backward[level], level);
  }

  // forward to put the rest of data
  for (int idata = 1; idata < ndata; idata++) {
    pDataKey = pSkipList->keyFn(ppData[idata]);
    hasDup = false;

    // Compare max key
    pKey = SL_GET_MAX_KEY(pSkipList);
    compare = pSkipList->comparFn(pDataKey, pKey);
    if (compare > 0) {
      for (int i = 0; i < pSkipList->maxLevel; i++) {
        forward[i] = SL_NODE_GET_BACKWARD_POINTER(pSkipList->pTail, i);
      }
    } else {
      SSkipListNode *px = pSkipList->pHead;
      for (int i = pSkipList->maxLevel - 1; i >= 0; --i) {
        if (i < pSkipList->level) {
          // set new px
          if (forward[i] != pSkipList->pHead) {
            if (px == pSkipList->pHead ||
                pSkipList->comparFn(SL_GET_NODE_KEY(pSkipList, forward[i]), SL_GET_NODE_KEY(pSkipList, px)) > 0) {
              px = forward[i];
            }
          }

          SSkipListNode *p = SL_NODE_GET_FORWARD_POINTER(px, i);
          while (p != pSkipList->pTail) {
            pKey = SL_GET_NODE_KEY(pSkipList, p);

            compare = pSkipList->comparFn(pKey, pDataKey);
            if (compare >= 0) {
              if (compare == 0 && !hasDup) hasDup = true;
              break;
            } else {
              px = p;
              p = SL_NODE_GET_FORWARD_POINTER(px, i);
            }
          }
        }

        forward[i] = px;
      }
    }

    tSkipListPutImpl(pSkipList, ppData[idata], forward, true, hasDup);
  }

  tSkipListUnlock(pSkipList);
}

uint32_t tSkipListRemove(SSkipList *pSkipList, SSkipListKey key) {
  uint32_t count = 0;

  tSkipListWLock(pSkipList);

  SSkipListNode *pNode = getPriorNode(pSkipList, key, TSDB_ORDER_ASC, NULL);
  while (1) {
    SSkipListNode *p = SL_NODE_GET_FORWARD_POINTER(pNode, 0);
    if (p == pSkipList->pTail) {
      break;
    }
    if (pSkipList->comparFn(key, SL_GET_NODE_KEY(pSkipList, p)) != 0) {
      break;
    }

    tSkipListRemoveNodeImpl(pSkipList, p);

    ++count;
  }

  tSkipListCorrectLevel(pSkipList);

  tSkipListUnlock(pSkipList);

  return count;
}

SArray *tSkipListGet(SSkipList *pSkipList, SSkipListKey key) {
  SArray *sa = taosArrayInit(1, POINTER_BYTES);

  tSkipListRLock(pSkipList);

  SSkipListNode *pNode = getPriorNode(pSkipList, key, TSDB_ORDER_ASC, NULL);
  while (1) {
    SSkipListNode *p = SL_NODE_GET_FORWARD_POINTER(pNode, 0);
    if (p == pSkipList->pTail) {
      break;
    }
    if (pSkipList->comparFn(key, SL_GET_NODE_KEY(pSkipList, p)) != 0) {
      break;
    }
    taosArrayPush(sa, &p);
    pNode = p;
  }

  tSkipListUnlock(pSkipList);

  return sa;
}

void tSkipListRemoveNode(SSkipList *pSkipList, SSkipListNode *pNode) {
  tSkipListWLock(pSkipList);
  tSkipListRemoveNodeImpl(pSkipList, pNode);
  tSkipListCorrectLevel(pSkipList);
  tSkipListUnlock(pSkipList);
}

SSkipListIterator *tSkipListCreateIter(SSkipList *pSkipList) {
  if (pSkipList == NULL) return NULL;

  return doCreateSkipListIterator(pSkipList, TSDB_ORDER_ASC);
}

SSkipListIterator *tSkipListCreateIterFromVal(SSkipList *pSkipList, const char *val, int32_t type, int32_t order) {
  ASSERT(order == TSDB_ORDER_ASC || order == TSDB_ORDER_DESC);
  ASSERT(pSkipList != NULL);

  SSkipListIterator *iter = doCreateSkipListIterator(pSkipList, order);
  if (val == NULL) {
    return iter;
  }

  tSkipListRLock(pSkipList);

  iter->cur = getPriorNode(pSkipList, val, order, &(iter->next));

  tSkipListUnlock(pSkipList);

  return iter;
}

bool tSkipListIterNext(SSkipListIterator *iter) {
  if (iter->pSkipList == NULL) return false;

  SSkipList *pSkipList = iter->pSkipList;

  tSkipListRLock(pSkipList);

  if (iter->order == TSDB_ORDER_ASC) {
    if (iter->cur == pSkipList->pTail) return false;
    iter->cur = SL_NODE_GET_FORWARD_POINTER(iter->cur, 0);

    // a new node is inserted into between iter->cur and iter->next, ignore it
    if (iter->cur != iter->next && (iter->next != NULL)) {
      iter->cur = iter->next;
    }

    iter->next = SL_NODE_GET_FORWARD_POINTER(iter->cur, 0);
    iter->step++;
  } else {
    if (iter->cur == pSkipList->pHead) return false;
    iter->cur = SL_NODE_GET_BACKWARD_POINTER(iter->cur, 0);

    // a new node is inserted into between iter->cur and iter->next, ignore it
    if (iter->cur != iter->next && (iter->next != NULL)) {
      iter->cur = iter->next;
    }

    iter->next = SL_NODE_GET_BACKWARD_POINTER(iter->cur, 0);
    iter->step++;
  }

  tSkipListUnlock(pSkipList);

  return (iter->order == TSDB_ORDER_ASC) ? (iter->cur != pSkipList->pTail) : (iter->cur != pSkipList->pHead);
}

SSkipListNode *tSkipListIterGet(SSkipListIterator *iter) {
  if (iter == NULL || iter->cur == iter->pSkipList->pTail || iter->cur == iter->pSkipList->pHead) {
    return NULL;
  } else {
    return iter->cur;
  }
}

void *tSkipListDestroyIter(SSkipListIterator *iter) {
  if (iter == NULL) {
    return NULL;
  }

  tfree(iter);
  return NULL;
}

void tSkipListPrint(SSkipList *pSkipList, int16_t nlevel) {
  if (pSkipList == NULL || pSkipList->level < nlevel || nlevel <= 0) {
    return;
  }

  SSkipListNode *p = SL_NODE_GET_FORWARD_POINTER(pSkipList->pHead, nlevel - 1);

  int32_t id = 1;
  char *  prev = NULL;

  while (p != pSkipList->pTail) {
    char *key = SL_GET_NODE_KEY(pSkipList, p);
    if (prev != NULL) {
      ASSERT(pSkipList->comparFn(prev, key) < 0);
    }

    switch (pSkipList->type) {
      case TSDB_DATA_TYPE_INT:
        fprintf(stdout, "%d: %d\n", id++, *(int32_t *)key);
        break;
      case TSDB_DATA_TYPE_SMALLINT:
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_BIGINT:
        fprintf(stdout, "%d: %" PRId64 " \n", id++, *(int64_t *)key);
        break;
      case TSDB_DATA_TYPE_BINARY:
        fprintf(stdout, "%d: %s \n", id++, key);
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        fprintf(stdout, "%d: %lf \n", id++, *(double *)key);
        break;
      default:
        fprintf(stdout, "\n");
    }

    prev = SL_GET_NODE_KEY(pSkipList, p);

    p = SL_NODE_GET_FORWARD_POINTER(p, nlevel - 1);
  }
}

static void tSkipListDoInsert(SSkipList *pSkipList, SSkipListNode **direction, SSkipListNode *pNode, bool isForward) {
  for (int32_t i = 0; i < pNode->level; ++i) {
    SSkipListNode *x = direction[i];
    if (isForward) {
      SL_NODE_GET_BACKWARD_POINTER(pNode, i) = x;

      SSkipListNode *next = SL_NODE_GET_FORWARD_POINTER(x, i);
      SL_NODE_GET_BACKWARD_POINTER(next, i) = pNode;

      SL_NODE_GET_FORWARD_POINTER(pNode, i) = next;
      SL_NODE_GET_FORWARD_POINTER(x, i) = pNode;
    } else {
      SL_NODE_GET_FORWARD_POINTER(pNode, i) = x;

      SSkipListNode *prev = SL_NODE_GET_BACKWARD_POINTER(x, i);
      SL_NODE_GET_FORWARD_POINTER(prev, i) = pNode;

      SL_NODE_GET_BACKWARD_POINTER(pNode, i) = prev;
      SL_NODE_GET_BACKWARD_POINTER(x, i) = pNode;
    }
  }

  if (pSkipList->level < pNode->level) pSkipList->level = pNode->level;

  pSkipList->size += 1;
}

static SSkipListIterator *doCreateSkipListIterator(SSkipList *pSkipList, int32_t order) {
  SSkipListIterator *iter = calloc(1, sizeof(SSkipListIterator));

  iter->pSkipList = pSkipList;
  iter->order = order;
  if (order == TSDB_ORDER_ASC) {
    iter->cur = pSkipList->pHead;
    iter->next = SL_NODE_GET_FORWARD_POINTER(iter->cur, 0);
  } else {
    iter->cur = pSkipList->pTail;
    iter->next = SL_NODE_GET_BACKWARD_POINTER(iter->cur, 0);
  }

  return iter;
}

static FORCE_INLINE int tSkipListWLock(SSkipList *pSkipList) {
  if (pSkipList->lock) {
    return pthread_rwlock_wrlock(pSkipList->lock);
  }
  return 0;
}

static FORCE_INLINE int tSkipListRLock(SSkipList *pSkipList) {
  if (pSkipList->lock) {
    return pthread_rwlock_rdlock(pSkipList->lock);
  }
  return 0;
}

static FORCE_INLINE int tSkipListUnlock(SSkipList *pSkipList) {
  if (pSkipList->lock) {
    return pthread_rwlock_unlock(pSkipList->lock);
  }
  return 0;
}

static bool tSkipListGetPosToPut(SSkipList *pSkipList, SSkipListNode **backward, void *pData) {
  int     compare = 0;
  bool    hasDupKey = false;
  char *  pDataKey = pSkipList->keyFn(pData);

  if (pSkipList->size == 0) {
    for (int i = 0; i < pSkipList->maxLevel; i++) {
      backward[i] = pSkipList->pTail;
    }
  } else {
    char *pKey = NULL;

    // Compare max key
    pKey = SL_GET_MAX_KEY(pSkipList);
    compare = pSkipList->comparFn(pDataKey, pKey);
    if (compare >= 0) {
      for (int i = 0; i < pSkipList->maxLevel; i++) {
        backward[i] = pSkipList->pTail;
      }

      return (compare == 0);
    }

    // Compare min key
    pKey = SL_GET_MIN_KEY(pSkipList);
    compare = pSkipList->comparFn(pDataKey, pKey);
    if (compare < 0) {
      for (int i = 0; i < pSkipList->maxLevel; i++) {
        backward[i] = SL_NODE_GET_FORWARD_POINTER(pSkipList->pHead, i);
      }

      return (compare == 0);
    }

    SSkipListNode *px = pSkipList->pTail;
    for (int i = pSkipList->maxLevel - 1; i >= 0; --i) {
      if (i < pSkipList->level) {
        SSkipListNode *p = SL_NODE_GET_BACKWARD_POINTER(px, i);
        while (p != pSkipList->pHead) {
          pKey = SL_GET_NODE_KEY(pSkipList, p);

          compare = pSkipList->comparFn(pKey, pDataKey);
          if (compare <= 0) {
            if (compare == 0 && !hasDupKey) hasDupKey = true;
            break;
          } else {
            px = p;
            p = SL_NODE_GET_BACKWARD_POINTER(px, i);
          }
        }
      }

      backward[i] = px;
    }
  }

  return hasDupKey;
}

static void tSkipListRemoveNodeImpl(SSkipList *pSkipList, SSkipListNode *pNode) {
  int32_t level = pNode->level;
  uint8_t dupMode = SL_DUP_MODE(pSkipList);
  ASSERT(dupMode != SL_DISCARD_DUP_KEY && dupMode != SL_UPDATE_DUP_KEY);

  for (int32_t j = level - 1; j >= 0; --j) {
    SSkipListNode *prev = SL_NODE_GET_BACKWARD_POINTER(pNode, j);
    SSkipListNode *next = SL_NODE_GET_FORWARD_POINTER(pNode, j);

    SL_NODE_GET_FORWARD_POINTER(prev, j) = next;
    SL_NODE_GET_BACKWARD_POINTER(next, j) = prev;
  }

  tSkipListFreeNode(pNode);
  pSkipList->size--;
}

// Function must be called after calling tSkipListRemoveNodeImpl() function
static void tSkipListCorrectLevel(SSkipList *pSkipList) {
  while (pSkipList->level > 0 && SL_NODE_GET_FORWARD_POINTER(pSkipList->pHead, pSkipList->level - 1) == pSkipList->pTail) {
    pSkipList->level -= 1;
  }
}

UNUSED_FUNC static FORCE_INLINE void recordNodeEachLevel(SSkipList *pSkipList,
                                                         int32_t    level) {  // record link count in each level
#if SKIP_LIST_RECORD_PERFORMANCE
  for (int32_t i = 0; i < level; ++i) {
    pSkipList->state.nLevelNodeCnt[i]++;
  }
#endif
}

UNUSED_FUNC static FORCE_INLINE void removeNodeEachLevel(SSkipList *pSkipList, int32_t level) {
#if SKIP_LIST_RECORD_PERFORMANCE
  for (int32_t i = 0; i < level; ++i) {
    pSkipList->state.nLevelNodeCnt[i]--;
  }
#endif
}

static FORCE_INLINE int32_t getSkipListNodeRandomHeight(SSkipList *pSkipList) {
  const uint32_t factor = 4;

  int32_t n = 1;
  while ((rand() % factor) == 0 && n <= pSkipList->maxLevel) {
    n++;
  }

  return n;
}

static FORCE_INLINE int32_t getSkipListRandLevel(SSkipList *pSkipList) {
  int32_t level = 0;
  if (pSkipList->size == 0) {
    level = 1;
  } else {
    level = getSkipListNodeRandomHeight(pSkipList);
    if (level > pSkipList->level) {
      if (pSkipList->level < pSkipList->maxLevel) {
        level = pSkipList->level + 1;
      } else {
        level = pSkipList->level;
      }
    }
  }

  ASSERT(level <= pSkipList->maxLevel);
  return level;
}

// when order is TSDB_ORDER_ASC, return the last node with key less than val
// when order is TSDB_ORDER_DESC, return the first node with key large than val
static SSkipListNode *getPriorNode(SSkipList *pSkipList, const char *val, int32_t order, SSkipListNode **pCur) {
  __compar_fn_t  comparFn = pSkipList->comparFn;
  SSkipListNode *pNode = NULL;
  if (pCur != NULL) {
    *pCur = NULL;
  }

  if (order == TSDB_ORDER_ASC) {
    pNode = pSkipList->pHead;
    for (int32_t i = pSkipList->level - 1; i >= 0; --i) {
      SSkipListNode *p = SL_NODE_GET_FORWARD_POINTER(pNode, i);
      while (p != pSkipList->pTail) {
        char *key = SL_GET_NODE_KEY(pSkipList, p);
        if (comparFn(key, val) < 0) {
          pNode = p;
          p = SL_NODE_GET_FORWARD_POINTER(p, i);
        } else {
          if (pCur != NULL) {
            *pCur = p;
          }
          break;
        }
      }
    }
  } else {
    pNode = pSkipList->pTail;
    for (int32_t i = pSkipList->level - 1; i >= 0; --i) {
      SSkipListNode *p = SL_NODE_GET_BACKWARD_POINTER(pNode, i);
      while (p != pSkipList->pHead) {
        char *key = SL_GET_NODE_KEY(pSkipList, p);
        if (comparFn(key, val) > 0) {
          pNode = p;
          p = SL_NODE_GET_BACKWARD_POINTER(p, i);
        } else {
          if (pCur != NULL) {
            *pCur = p;
          }
          break;
        }
      }
    }
  }

  return pNode;
}

static int initForwardBackwardPtr(SSkipList *pSkipList) {
  uint32_t maxLevel = pSkipList->maxLevel;

  // head info
  pSkipList->pHead = tSkipListNewNode(maxLevel);
  if (pSkipList->pHead == NULL) return -1;

  // tail info
  pSkipList->pTail = tSkipListNewNode(maxLevel);
  if (pSkipList->pTail == NULL) {
    tSkipListFreeNode(pSkipList->pHead);
    return -1;
  }

  for (uint32_t i = 0; i < maxLevel; ++i) {
    SL_NODE_GET_FORWARD_POINTER(pSkipList->pHead, i) = pSkipList->pTail;
    SL_NODE_GET_BACKWARD_POINTER(pSkipList->pTail, i) = pSkipList->pHead;
  }

  return 0;
}

static SSkipListNode *tSkipListNewNode(uint8_t level) {
  int32_t tsize = sizeof(SSkipListNode) + sizeof(SSkipListNode *) * level * 2;

  SSkipListNode *pNode = (SSkipListNode *)calloc(1, tsize);
  if (pNode == NULL) return NULL;

  pNode->level = level;
  return pNode;
}

static SSkipListNode *tSkipListPutImpl(SSkipList *pSkipList, void *pData, SSkipListNode **direction, bool isForward,
                                       bool hasDup) {
  uint8_t        dupMode = SL_DUP_MODE(pSkipList);
  SSkipListNode *pNode = NULL;

  if (hasDup && (dupMode == SL_DISCARD_DUP_KEY || dupMode == SL_UPDATE_DUP_KEY)) {
    if (dupMode == SL_UPDATE_DUP_KEY) {
      if (isForward) {
        pNode = SL_NODE_GET_FORWARD_POINTER(direction[0], 0);
      } else {
        pNode = SL_NODE_GET_BACKWARD_POINTER(direction[0], 0);
      }
      atomic_store_ptr(&(pNode->pData), pData);
    }
  } else {
    pNode = tSkipListNewNode(getSkipListRandLevel(pSkipList));
    if (pNode != NULL) {
      pNode->pData = pData;

      tSkipListDoInsert(pSkipList, direction, pNode, isForward);
    }
  }

  return pNode;
}

// static int32_t tSkipListEndParQuery(SSkipList *pSkipList, SSkipListNode *pStartNode, SSkipListKey *pEndKey,
//                                    int32_t cond, SSkipListNode ***pRes) {
//  pthread_rwlock_rdlock(&pSkipList->lock);
//  SSkipListNode *p = pStartNode;
//  int32_t        numOfRes = 0;
//
//  __compar_fn_t filterComparFn = getComparFunc(pSkipList, pEndKey->nType);
//  while (p != NULL) {
//    int32_t ret = filterComparFn(&p->key, pEndKey);
//    if (ret > 0) {
//      break;
//    }
//
//    if (ret < 0) {
//      numOfRes++;
//      p = p->pForward[0];
//    } else if (ret == 0) {
//      if (cond == TSDB_RELATION_LESS_EQUAL) {
//        numOfRes++;
//        p = p->pForward[0];
//      } else {
//        break;
//      }
//    }
//  }
//
//  (*pRes) = (SSkipListNode **)malloc(POINTER_BYTES * numOfRes);
//  for (int32_t i = 0; i < numOfRes; ++i) {
//    (*pRes)[i] = pStartNode;
//    pStartNode = pStartNode->pForward[0];
//  }
//  pthread_rwlock_unlock(&pSkipList->lock);
//
//  return numOfRes;
//}
//
///*
// * maybe return the copy of SSkipListNode would be better
// */
// int32_t tSkipListGets(SSkipList *pSkipList, SSkipListKey *pKey, SSkipListNode ***pRes) {
//  (*pRes) = NULL;
//
//  SSkipListNode *pNode = tSkipListGet(pSkipList, pKey);
//  if (pNode == NULL) {
//    return 0;
//  }
//
//  __compar_fn_t filterComparFn = getComparFunc(pSkipList, pKey->nType);
//
//  // backward check if previous nodes are with the same value.
//  SSkipListNode *pPrev = pNode->pBackward[0];
//  while ((pPrev != &pSkipList->pHead) && filterComparFn(&pPrev->key, pKey) == 0) {
//    pPrev = pPrev->pBackward[0];
//  }
//
//  return tSkipListEndParQuery(pSkipList, pPrev->pForward[0], &pNode->key, TSDB_RELATION_LESS_EQUAL, pRes);
//}
//
// static SSkipListNode *tSkipListParQuery(SSkipList *pSkipList, SSkipListKey *pKey, int32_t cond) {
//  int32_t sLevel = pSkipList->level - 1;
//  int32_t ret = -1;
//
//  SSkipListNode *x = &pSkipList->pHead;
//  __compar_fn_t  filterComparFn = getComparFunc(pSkipList, pKey->nType);
//
//  pthread_rwlock_rdlock(&pSkipList->lock);
//
//  if (cond == TSDB_RELATION_GREATER_EQUAL || cond == TSDB_RELATION_GREATER) {
//    for (int32_t i = sLevel; i >= 0; --i) {
//      while (x->pForward[i] != NULL && (ret = filterComparFn(&x->pForward[i]->key, pKey)) < 0) {
//        x = x->pForward[i];
//      }
//    }
//
//    // backward check if previous nodes are with the same value.
//    if (cond == TSDB_RELATION_GREATER_EQUAL && ret == 0) {
//      SSkipListNode *pNode = x->pForward[0];
//      while ((pNode->pBackward[0] != &pSkipList->pHead) && (filterComparFn(&pNode->pBackward[0]->key, pKey) == 0)) {
//        pNode = pNode->pBackward[0];
//      }
//      pthread_rwlock_unlock(&pSkipList->lock);
//      return pNode;
//    }
//
//    if (ret > 0 || cond == TSDB_RELATION_GREATER_EQUAL) {
//      pthread_rwlock_unlock(&pSkipList->lock);
//      return x->pForward[0];
//    } else {  // cond == TSDB_RELATION_GREATER && ret == 0
//      SSkipListNode *pn = x->pForward[0];
//      while (pn != NULL && filterComparFn(&pn->key, pKey) == 0) {
//        pn = pn->pForward[0];
//      }
//      pthread_rwlock_unlock(&pSkipList->lock);
//      return pn;
//    }
//  }
//
//  pthread_rwlock_unlock(&pSkipList->lock);
//  return NULL;
//}
//
//
// static bool removeSupport(SSkipList *pSkipList, SSkipListNode **forward, SSkipListKey *pKey) {
//  __compar_fn_t filterComparFn = getComparFunc(pSkipList, pKey->nType);
//
//  if (filterComparFn(&forward[0]->pForward[0]->key, pKey) == 0) {
//    SSkipListNode *p = forward[0]->pForward[0];
//    doRemove(pSkipList, p, forward);
//  } else {  // failed to find the node of specified value,abort
//    return false;
//  }
//
//  // compress the minimum level of skip list
//  while (pSkipList->level > 0 && SL_NODE_GET_FORWARD_POINTER(pSkipList->pHead, pSkipList->level - 1) == NULL) {
//    pSkipList->level -= 1;
//  }
//
//  return true;
//}
//
// bool tSkipListRemove(SSkipList *pSkipList, SSkipListKey *pKey) {
//  SSkipListNode *forward[MAX_SKIP_LIST_LEVEL] = {0};
//  __compar_fn_t  filterComparFn = getComparFunc(pSkipList, pKey->nType);
//
//  pthread_rwlock_rdlock(&pSkipList->lock);
//
//  SSkipListNode *x = &pSkipList->pHead;
//  for (int32_t i = pSkipList->level - 1; i >= 0; --i) {
//    while (x->pForward[i] != NULL && (filterComparFn(&x->pForward[i]->key, pKey) < 0)) {
//      x = x->pForward[i];
//    }
//    forward[i] = x;
//  }
//
//  bool ret = removeSupport(pSkipList, forward, pKey);
//  pthread_rwlock_unlock(&pSkipList->lock);
//
//  return ret;
//}
