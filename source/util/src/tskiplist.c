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

#define _DEFAULT_SOURCE
#include "tskiplist.h"
#include "tcompare.h"
#include "tlog.h"
#include "tutil.h"

static int32_t            initForwardBackwardPtr(SSkipList *pSkipList);
static SSkipListNode *    getPriorNode(SSkipList *pSkipList, const char *val, int32_t order, SSkipListNode **pCur);
static void               tSkipListRemoveNodeImpl(SSkipList *pSkipList, SSkipListNode *pNode);
static void               tSkipListCorrectLevel(SSkipList *pSkipList);
static SSkipListIterator *doCreateSkipListIterator(SSkipList *pSkipList, int32_t order);
static void tSkipListDoInsert(SSkipList *pSkipList, SSkipListNode **direction, SSkipListNode *pNode, bool isForward);
static bool tSkipListGetPosToPut(SSkipList *pSkipList, SSkipListNode **backward, void *pData);
static SSkipListNode *tSkipListNewNode(uint8_t level);
#define tSkipListFreeNode(n) taosMemoryFreeClear((n))
static SSkipListNode *tSkipListPutImpl(SSkipList *pSkipList, void *pData, SSkipListNode **direction, bool isForward,
                                       bool hasDup);

static FORCE_INLINE int32_t tSkipListWLock(SSkipList *pSkipList);
static FORCE_INLINE int32_t tSkipListRLock(SSkipList *pSkipList);
static FORCE_INLINE int32_t tSkipListUnlock(SSkipList *pSkipList);
static FORCE_INLINE int32_t getSkipListRandLevel(SSkipList *pSkipList);

SSkipList *tSkipListCreate(uint8_t maxLevel, uint8_t keyType, uint16_t keyLen, __compar_fn_t comparFn, uint8_t flags,
                           __sl_key_fn_t fn) {
  SSkipList *pSkipList = (SSkipList *)taosMemoryCalloc(1, sizeof(SSkipList));
  if (pSkipList == NULL) return NULL;

  if (maxLevel > MAX_SKIP_LIST_LEVEL) {
    maxLevel = MAX_SKIP_LIST_LEVEL;
  }

  pSkipList->maxLevel = maxLevel;
  pSkipList->type = keyType;
  pSkipList->len = keyLen;
  pSkipList->flags = flags;
  pSkipList->keyFn = fn;
  pSkipList->seed = taosRand();

#if 0 
  // the function getkeycomparfunc is defined in common
  if (comparFn == NULL) {
    pSkipList->comparFn = getKeyComparFunc(keyType, TSDB_ORDER_ASC);
  } else {
    pSkipList->comparFn = comparFn;
  }
#else
  pSkipList->comparFn = comparFn;
#endif

  if (initForwardBackwardPtr(pSkipList) < 0) {
    tSkipListDestroy(pSkipList);
    return NULL;
  }

  if (SL_IS_THREAD_SAFE(pSkipList)) {
    pSkipList->lock = (TdThreadRwlock *)taosMemoryCalloc(1, sizeof(TdThreadRwlock));
    if (pSkipList->lock == NULL) {
      tSkipListDestroy(pSkipList);
      return NULL;
    }

    if (taosThreadRwlockInit(pSkipList->lock, NULL) != 0) {
      tSkipListDestroy(pSkipList);
      return NULL;
    }
  }

  taosSeedRand((uint32_t)taosGetTimestampSec());

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
    taosThreadRwlockDestroy(pSkipList->lock);
    taosMemoryFreeClear(pSkipList->lock);
  }

  tSkipListFreeNode(pSkipList->pHead);
  tSkipListFreeNode(pSkipList->pTail);
  taosMemoryFreeClear(pSkipList);
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

#ifdef BUILD_NO_CALL

void tSkipListPutBatchByIter(SSkipList *pSkipList, void *iter, iter_next_fn_t iterate) {
  SSkipListNode *backward[MAX_SKIP_LIST_LEVEL] = {0};
  SSkipListNode *forward[MAX_SKIP_LIST_LEVEL] = {0};
  bool           hasDup = false;
  char *         pKey = NULL;
  char *         pDataKey = NULL;
  int32_t        compare = 0;

  tSkipListWLock(pSkipList);

  void *pData = iterate(iter);
  if (pData == NULL) {
    tSkipListUnlock(pSkipList);
    return;
  }

  // backward to put the first data
  hasDup = tSkipListGetPosToPut(pSkipList, backward, pData);

  tSkipListPutImpl(pSkipList, pData, backward, false, hasDup);

  for (int32_t level = 0; level < pSkipList->maxLevel; level++) {
    forward[level] = SL_NODE_GET_BACKWARD_POINTER(backward[level], level);
  }

  // forward to put the rest of data
  while ((pData = iterate(iter)) != NULL) {
    pDataKey = pSkipList->keyFn(pData);
    hasDup = false;

    // Compare max key
    pKey = SL_GET_MAX_KEY(pSkipList);
    compare = pSkipList->comparFn(pDataKey, pKey);
    if (compare > 0) {
      for (int32_t i = 0; i < pSkipList->maxLevel; i++) {
        forward[i] = SL_NODE_GET_BACKWARD_POINTER(pSkipList->pTail, i);
      }
    } else {
      SSkipListNode *px = pSkipList->pHead;
      for (int32_t i = pSkipList->maxLevel - 1; i >= 0; --i) {
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
            if (compare > 0) {
              break;
            } else {
              if (compare == 0 && !hasDup) hasDup = true;
              px = p;
              p = SL_NODE_GET_FORWARD_POINTER(px, i);
            }
          }
        }

        forward[i] = px;
      }
    }

    tSkipListPutImpl(pSkipList, pData, forward, true, hasDup);
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
#endif

SSkipListIterator *tSkipListCreateIter(SSkipList *pSkipList) {
  if (pSkipList == NULL) return NULL;

  return doCreateSkipListIterator(pSkipList, TSDB_ORDER_ASC);
}

SSkipListIterator *tSkipListCreateIterFromVal(SSkipList *pSkipList, const char *val, int32_t type, int32_t order) {
  if (order != TSDB_ORDER_ASC && order != TSDB_ORDER_DESC) {
    return NULL;
  }

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
    // no data in the skip list
    if (iter->cur == pSkipList->pTail || iter->next == NULL) {
      iter->cur = pSkipList->pTail;
      tSkipListUnlock(pSkipList);
      return false;
    }

    iter->cur = SL_NODE_GET_FORWARD_POINTER(iter->cur, 0);

    // a new node is inserted into between iter->cur and iter->next, ignore it
    if (iter->cur != iter->next && (iter->next != NULL)) {
      iter->cur = iter->next;
    }

    iter->next = SL_NODE_GET_FORWARD_POINTER(iter->cur, 0);
    iter->step++;
  } else {
    if (iter->cur == pSkipList->pHead) {
      iter->cur = pSkipList->pHead;
      tSkipListUnlock(pSkipList);
      return false;
    }

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

  taosMemoryFreeClear(iter);
  return NULL;
}

#ifdef BUILD_NO_CALL
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
      case TSDB_DATA_TYPE_VARBINARY:
      case TSDB_DATA_TYPE_GEOMETRY:
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
#endif

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
  SSkipListIterator *iter = taosMemoryCalloc(1, sizeof(SSkipListIterator));

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

static FORCE_INLINE int32_t tSkipListWLock(SSkipList *pSkipList) {
  if (pSkipList->lock) {
    return taosThreadRwlockWrlock(pSkipList->lock);
  }
  return 0;
}

static FORCE_INLINE int32_t tSkipListRLock(SSkipList *pSkipList) {
  if (pSkipList->lock) {
    return taosThreadRwlockRdlock(pSkipList->lock);
  }
  return 0;
}

static FORCE_INLINE int32_t tSkipListUnlock(SSkipList *pSkipList) {
  if (pSkipList->lock) {
    return taosThreadRwlockUnlock(pSkipList->lock);
  }
  return 0;
}

static bool tSkipListGetPosToPut(SSkipList *pSkipList, SSkipListNode **backward, void *pData) {
  int32_t compare = 0;
  bool    hasDupKey = false;
  char *  pDataKey = pSkipList->keyFn(pData);

  if (pSkipList->size == 0) {
    for (int32_t i = 0; i < pSkipList->maxLevel; i++) {
      backward[i] = pSkipList->pTail;
    }
  } else {
    char *pKey = NULL;

    // Compare max key
    pKey = SL_GET_MAX_KEY(pSkipList);
    compare = pSkipList->comparFn(pDataKey, pKey);
    if (compare >= 0) {
      for (int32_t i = 0; i < pSkipList->maxLevel; i++) {
        backward[i] = pSkipList->pTail;
      }

      return (compare == 0);
    }

    // Compare min key
    pKey = SL_GET_MIN_KEY(pSkipList);
    compare = pSkipList->comparFn(pDataKey, pKey);
    if (compare < 0) {
      for (int32_t i = 0; i < pSkipList->maxLevel; i++) {
        backward[i] = SL_NODE_GET_FORWARD_POINTER(pSkipList->pHead, i);
      }

      return (compare == 0);
    }

    SSkipListNode *px = pSkipList->pTail;
    for (int32_t i = pSkipList->maxLevel - 1; i >= 0; --i) {
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

#ifdef BUILD_NO_CALL
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
  while (pSkipList->level > 0 &&
         SL_NODE_GET_FORWARD_POINTER(pSkipList->pHead, pSkipList->level - 1) == pSkipList->pTail) {
    pSkipList->level -= 1;
  }
}
#endif

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
#ifdef WINDOWS
  while ((taosRand() % factor) == 0 && n <= pSkipList->maxLevel) {
#else
  while ((taosRandR(&(pSkipList->seed)) % factor) == 0 && n <= pSkipList->maxLevel) {
#endif
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

static int32_t initForwardBackwardPtr(SSkipList *pSkipList) {
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

  SSkipListNode *pNode = (SSkipListNode *)taosMemoryCalloc(1, tsize);
  if (pNode == NULL) return NULL;

  pNode->level = level;
  return pNode;
}

static SSkipListNode *tSkipListPutImpl(SSkipList *pSkipList, void *pData, SSkipListNode **direction, bool isForward,
                                       bool hasDup) {
  uint8_t        dupMode = SL_DUP_MODE(pSkipList);
  SSkipListNode *pNode = NULL;

  if (hasDup && (dupMode != SL_ALLOW_DUP_KEY)) {
    if (dupMode == SL_UPDATE_DUP_KEY) {
      if (isForward) {
        pNode = SL_NODE_GET_FORWARD_POINTER(direction[0], 0);
      } else {
        pNode = SL_NODE_GET_BACKWARD_POINTER(direction[0], 0);
      }
      if (pData) {
        atomic_store_ptr(&(pNode->pData), pData);
      }
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
