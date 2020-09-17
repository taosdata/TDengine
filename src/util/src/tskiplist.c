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

#define DO_MEMSET_PTR_AREA(n)                  \
  do {                                         \
    int32_t _l = (n)->level;                   \
    memset(pNode, 0, SL_NODE_HEADER_SIZE(_l)); \
    (n)->level = _l;                           \
  } while (0)

static int initForwardBackwardPtr(SSkipList *pSkipList);
static SSkipListNode *getPriorNode(SSkipList *pSkipList, const char *val, int32_t order);
static void tSkipListRemoveNodeImpl(SSkipList *pSkipList, SSkipListNode *pNode);
static void tSkipListCorrectLevel(SSkipList *pSkipList);
static SSkipListIterator *doCreateSkipListIterator(SSkipList *pSkipList, int32_t order);

static FORCE_INLINE int     tSkipListWLock(SSkipList *pSkipList);
static FORCE_INLINE int     tSkipListRLock(SSkipList *pSkipList);
static FORCE_INLINE int     tSkipListUnlock(SSkipList *pSkipList);
static FORCE_INLINE int32_t getSkipListRandLevel(SSkipList *pSkipList);
static FORCE_INLINE SSkipListNode *tSkipListPutNodeImpl(SSkipList *pSkipList, SSkipListNode *pNode);

SSkipList *tSkipListCreate(uint8_t maxLevel, uint8_t keyType, uint16_t keyLen, uint8_t flags, __sl_key_fn_t fn) {
  SSkipList *pSkipList = (SSkipList *)calloc(1, sizeof(SSkipList));
  if (pSkipList == NULL) {
    return NULL;
  }

  if (maxLevel > MAX_SKIP_LIST_LEVEL) {
    maxLevel = MAX_SKIP_LIST_LEVEL;
  }

  pSkipList->maxLevel = maxLevel;
  pSkipList->type = keyType;
  pSkipList->len = keyLen;
  pSkipList->flags = flags;
  pSkipList->keyFn = fn;
  pSkipList->comparFn = getKeyComparFunc(keyType);

  // pSkipList->level = 1; // TODO: check if 1 is valid
  if (initForwardBackwardPtr(pSkipList) < 0) {
    taosTFree(pSkipList);
    return NULL;
  }

  if (SL_IS_THREAD_SAFE(flags)) {
    pSkipList->lock = (pthread_rwlock_t *)calloc(1, sizeof(pthread_rwlock_t));

    if (pthread_rwlock_init(pSkipList->lock, NULL) != 0) {
      taosTFree(pSkipList->pHead);
      taosTFree(pSkipList);

      return NULL;
    }
  }

  srand((uint32_t)time(NULL));

#if SKIP_LIST_RECORD_PERFORMANCE
  pSkipList->state.nTotalMemSize += sizeof(SSkipList);
#endif

  return pSkipList;
}

void *tSkipListDestroy(SSkipList *pSkipList) {
  if (pSkipList == NULL) return NULL;

  tSkipListWLock(pSkipList);

  SSkipListNode *pNode = SL_GET_FORWARD_POINTER(pSkipList->pHead, 0);

  while (pNode != pSkipList->pTail) {
    SSkipListNode *pTemp = pNode;
    pNode = SL_GET_FORWARD_POINTER(pNode, 0);
    taosTFree(pTemp);
  }

  tSkipListUnlock(pSkipList);
  if (pSkipList->lock != NULL) {
    pthread_rwlock_destroy(pSkipList->lock);
    taosTFree(pSkipList->lock);
  }

  taosTFree(pSkipList->pHead);
  taosTFree(pSkipList);
  return NULL;
}

void tSkipListNewNodeInfo(SSkipList *pSkipList, int32_t *level, int32_t *headSize) {
  if (pSkipList == NULL) {
    *level = 1;
    *headSize = SL_NODE_HEADER_SIZE(*level);
    return;
  }

  *level = getSkipListRandLevel(pSkipList);
  *headSize = SL_NODE_HEADER_SIZE(*level);
}

SSkipListNode *tSkipListPut(SSkipList *pSkipList, void *pData, int dataLen) {
  if (pSkipList == NULL || pData == NULL) return NULL;

  tSkipListWLock(pSkipList);

  int32_t level = getSkipListRandLevel(pSkipList);

  SSkipListNode *pNode = (SSkipListNode *)malloc(SL_NODE_HEADER_SIZE(level) + dataLen);
  if (pNode == NULL) {
    tSkipListUnlock(pSkipList);
    return NULL;
  }

  pNode->level = level;
  memcpy(SL_GET_NODE_DATA(pNode), pData, dataLen);

  if (tSkipListPutNodeImpl(pSkipList, pNode) == NULL) {
    tSkipListUnlock(pSkipList);
    taosTFree(pNode);
    return NULL;
  }

  tSkipListUnlock(pSkipList);

  return pNode;
}

SSkipListNode *tSkipListPutNode(SSkipList *pSkipList, SSkipListNode *pNode) {
  SSkipListNode *pRetNode = NULL;

  if (pSkipList == NULL || pNode == NULL) return NULL;

  tSkipListWLock(pSkipList);
  pRetNode = tSkipListPutNodeImpl(pSkipList, pNode);
  tSkipListUnlock(pSkipList);

  return pRetNode;
}

SArray *tSkipListGet(SSkipList *pSkipList, SSkipListKey key) {
  SArray *sa = taosArrayInit(1, POINTER_BYTES);

  tSkipListRLock(pSkipList);

  SSkipListNode *pNode = getPriorNode(pSkipList, key, TSDB_ORDER_ASC);
  while (1) {
    SSkipListNode *p = SL_GET_FORWARD_POINTER(pNode, 0);
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

uint32_t tSkipListRemove(SSkipList *pSkipList, SSkipListKey key) {
  uint32_t count = 0;

  tSkipListWLock(pSkipList);

  SSkipListNode *pNode = getPriorNode(pSkipList, key, TSDB_ORDER_ASC);
  while (1) {
    SSkipListNode *p = SL_GET_FORWARD_POINTER(pNode, 0);
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
  assert(order == TSDB_ORDER_ASC || order == TSDB_ORDER_DESC);
  assert(pSkipList != NULL);

  SSkipListIterator *iter = doCreateSkipListIterator(pSkipList, order);
  if (val == NULL) {
    return iter;
  }

  tSkipListRLock(pSkipList);

  iter->cur = getPriorNode(pSkipList, val, order);

  tSkipListUnlock(pSkipList);

  return iter;
}

bool tSkipListIterNext(SSkipListIterator *iter) {
  if (iter->pSkipList == NULL) return false;

  SSkipList *pSkipList = iter->pSkipList;
  uint8_t    dupMod = SL_DUP_MODE(pSkipList->flags);

  tSkipListRLock(pSkipList);

  if (iter->order == TSDB_ORDER_ASC) {  // ascending order iterate
    if (dupMod == SL_APPEND_DUP_KEY) {
      if (iter->cur == pSkipList->pHead) {
        iter->cur = SL_GET_FORWARD_POINTER(iter->cur, 0);
        iter->step++;
      } else {
        while (true) {
          iter->step++;
          SSkipListNode *pNode = iter->cur;
          iter->cur = SL_GET_FORWARD_POINTER(pNode, 0);

          if (iter->cur == pSkipList->pTail) break;
          if (pSkipList->comparFn(SL_GET_NODE_KEY(pSkipList, pNode), SL_GET_NODE_KEY(pSkipList, iter->cur)) == 0) {
            continue;
          } else {
            break;
          }
        }
      }

      if (iter->cur != pSkipList->pTail) {
        while (true) {
          SSkipListNode *pNode = SL_GET_FORWARD_POINTER(iter->cur, 0);
          if (pNode == pSkipList->pTail) break;
          if (pSkipList->comparFn(SL_GET_NODE_KEY(pSkipList, pNode), SL_GET_NODE_KEY(pSkipList, iter->cur)) == 0) {
            iter->step++;
            iter->cur = pNode;
          } else {
            break;
          }
        }
      }
    } else {
      iter->cur = SL_GET_FORWARD_POINTER(iter->cur, 0);
      iter->step++;
    }
  } else {  // descending order iterate
    if (dupMod == SL_APPEND_DUP_KEY) {
      if (iter->cur == pSkipList->pTail) {
        iter->cur = SL_GET_BACKWARD_POINTER(iter->cur, 0);
        iter->step++;
      } else {
        while (true) {
          SSkipListNode *pNode = iter->cur;
          iter->cur = SL_GET_BACKWARD_POINTER(pNode, 0);

          if (iter->cur == pSkipList->pHead) break;
          if (pSkipList->comparFn(SL_GET_NODE_KEY(pSkipList, pNode), SL_GET_NODE_KEY(pSkipList, iter->cur)) == 0) {
            iter->cur = pNode;
            continue;
          } else {
            break;
          }
        }
      }
    } else {
      iter->cur = SL_GET_BACKWARD_POINTER(iter->cur, 0);
      iter->step++;
    }
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

  taosTFree(iter);
  return NULL;
}

void tSkipListPrint(SSkipList *pSkipList, int16_t nlevel) {
  if (pSkipList == NULL || pSkipList->level < nlevel || nlevel <= 0) {
    return;
  }

  SSkipListNode *p = SL_GET_FORWARD_POINTER(pSkipList->pHead, nlevel - 1);

  int32_t id = 1;
  char *  prev = NULL;

  while (p != pSkipList->pTail) {
    char *key = SL_GET_NODE_KEY(pSkipList, p);
    if (prev != NULL) {
      assert(pSkipList->comparFn(prev, key) < 0);
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

    p = SL_GET_FORWARD_POINTER(p, nlevel - 1);
  }
}

static void tSkipListDoInsert(SSkipList *pSkipList, SSkipListNode **forward, SSkipListNode *pNode, bool hasDupKey) {
  uint8_t dupMode = SL_DUP_MODE(pSkipList->flags);

  // FIXME: this may cause the level of skiplist change
  if (dupMode == SL_UPDATA_DUP_KEY && hasDupKey) {
    tSkipListRemoveNodeImpl(pSkipList, forward[0]);
    tSkipListCorrectLevel(pSkipList);
  }

  DO_MEMSET_PTR_AREA(pNode);

  for (int32_t i = 0; i < pNode->level; ++i) {
    SSkipListNode *x = forward[i];
    SL_GET_BACKWARD_POINTER(pNode, i) = x;

    SSkipListNode *next = SL_GET_FORWARD_POINTER(x, i);
    SL_GET_BACKWARD_POINTER(next, i) = pNode;

    SL_GET_FORWARD_POINTER(pNode, i) = next;
    SL_GET_FORWARD_POINTER(x, i) = pNode;
  }

  if (!(dupMode == SL_APPEND_DUP_KEY && hasDupKey)) {
    pSkipList->size += 1;
  }
  pSkipList->tsize += 1;
}

static SSkipListIterator *doCreateSkipListIterator(SSkipList *pSkipList, int32_t order) {
  SSkipListIterator *iter = calloc(1, sizeof(SSkipListIterator));

  iter->pSkipList = pSkipList;
  iter->order = order;
  if (order == TSDB_ORDER_ASC) {
    iter->cur = pSkipList->pHead;
  } else {
    iter->cur = pSkipList->pTail;
  }

  return iter;
}

static FORCE_INLINE int tSkipListWLock(SSkipList *pSkipList) {
  if (SL_IS_THREAD_SAFE(pSkipList->flags)) {
    return pthread_rwlock_wrlock(pSkipList->lock);
  }
  return 0;
}

static FORCE_INLINE int tSkipListRLock(SSkipList *pSkipList) {
  if (SL_IS_THREAD_SAFE(pSkipList->flags)) {
    return pthread_rwlock_rdlock(pSkipList->lock);
  }
  return 0;
}

static FORCE_INLINE int tSkipListUnlock(SSkipList *pSkipList) {
  if (SL_IS_THREAD_SAFE(pSkipList->flags)) {
    return pthread_rwlock_unlock(pSkipList->lock);
  }
  return 0;
}

static bool tSkipListGetPosToPut(SSkipList *pSkipList, SSkipListNode **forward, SSkipListNode *pNode) {
  int     compare = 1;
  bool    hasDupKey = false;
  char *  pNodeKey = SL_GET_NODE_KEY(pSkipList, pNode);
  uint8_t dupMode = SL_DUP_MODE(pSkipList->flags);

  if (pSkipList->size == 0) {
    for (int i = 0; i < pNode->level; i++) {
      forward[i] = pSkipList->pHead;
    }
  } else {
    char *pKey = NULL;

    // Compare min key
    pKey = SL_GET_SL_MIN_KEY(pSkipList);
    compare = pSkipList->comparFn(pNodeKey, pKey);
    if ((dupMode == SL_APPEND_DUP_KEY && compare < 0) || (dupMode != SL_APPEND_DUP_KEY && compare <= 0)) {
      for (int i = 0; i < pNode->level; i++) {
        forward[i] = pSkipList->pHead;
      }
      return (compare == 0);
    }

    // Compare max key
    pKey = SL_GET_SL_MAX_KEY(pSkipList);
    compare = pSkipList->comparFn(pNodeKey, pKey);
    if ((dupMode == SL_DISCARD_DUP_KEY && compare > 0) || (dupMode != SL_DISCARD_DUP_KEY && compare >= 0)) {
      for (int i = 0; i < pNode->level; i++) {
        forward[i] = SL_GET_BACKWARD_POINTER(pSkipList->pTail, i);
      }

      return (compare == 0);
    }

    SSkipListNode *px = pSkipList->pHead;
    for (int i = pNode->level - 1; i >= 0; --i) {
      if (i >= pSkipList->level) {
        forward[i] = pSkipList->pHead;
        continue;
      }

      SSkipListNode *p = SL_GET_FORWARD_POINTER(px, i);
      while (p != pSkipList->pTail) {
        pKey = SL_GET_NODE_KEY(pSkipList, p);

        compare = pSkipList->comparFn(pKey, pNodeKey);
        if (compare == 0 && hasDupKey == false) hasDupKey = true;
        if ((dupMode == SL_APPEND_DUP_KEY && compare > 0) || (dupMode != SL_APPEND_DUP_KEY && compare >= 0)) {
          break;
        } else {
          px = p;
          p = SL_GET_FORWARD_POINTER(px, i);
        }
      }

      forward[i] = px;
    }
  }

  return hasDupKey;
}

static bool tSkipListIsNodeDup(SSkipList *pSkipList, SSkipListNode *pNode) {
  SSkipListNode *pPrevNode = SL_GET_BACKWARD_POINTER(pNode, 0);
  SSkipListNode *pNextNode = SL_GET_FORWARD_POINTER(pNode, 0);
  char *         pNodeKey = SL_GET_NODE_KEY(pSkipList, pNode);
  char *         pPrevNodeKey = (pPrevNode == pSkipList->pHead) ? NULL : SL_GET_NODE_KEY(pSkipList, pPrevNode);
  char *         pNextNodeKey = (pNextNode == pSkipList->pTail) ? NULL : SL_GET_NODE_KEY(pSkipList, pNextNode);

  return ((pPrevNodeKey != NULL && pSkipList->comparFn(pNodeKey, pPrevNodeKey) == 0) ||
          (pNextNodeKey != NULL && pSkipList->comparFn(pNodeKey, pNextNodeKey) == 0));
}

static void tSkipListRemoveNodeImpl(SSkipList *pSkipList, SSkipListNode *pNode) {
  int32_t level = pNode->level;
  uint8_t dupMode = SL_DUP_MODE(pSkipList->flags);

  bool sizeReduce = !(dupMode == SL_APPEND_DUP_KEY && tSkipListIsNodeDup(pSkipList, pNode));

  for (int32_t j = level - 1; j >= 0; --j) {
    SSkipListNode *prev = SL_GET_BACKWARD_POINTER(pNode, j);
    SSkipListNode *next = SL_GET_FORWARD_POINTER(pNode, j);

    SL_GET_FORWARD_POINTER(prev, j) = next;
    SL_GET_BACKWARD_POINTER(next, j) = prev;
  }

  taosTFree(pNode);

  if (sizeReduce) pSkipList->size--;
  pSkipList->tsize--;
}

// Function must be called after calling tSkipListRemoveNodeImpl() function
static void tSkipListCorrectLevel(SSkipList *pSkipList) {
  while (pSkipList->level > 0 && SL_GET_FORWARD_POINTER(pSkipList->pHead, pSkipList->level - 1) == pSkipList->pTail) {
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

  assert(level <= pSkipList->maxLevel);
  return level;
}

// when order is TSDB_ORDER_ASC, return the last node with key less than val
// when order is TSDB_ORDER_DESC, return the first node with key large than val
static SSkipListNode *getPriorNode(SSkipList *pSkipList, const char *val, int32_t order) {
  __compar_fn_t  comparFn = pSkipList->comparFn;
  SSkipListNode *pNode = NULL;

  if (order == TSDB_ORDER_ASC) {
    pNode = pSkipList->pHead;
    for (int32_t i = pSkipList->level - 1; i >= 0; --i) {
      SSkipListNode *p = SL_GET_FORWARD_POINTER(pNode, i);
      while (p != pSkipList->pTail) {
        char *key = SL_GET_NODE_KEY(pSkipList, p);
        if (comparFn(key, val) < 0) {
          pNode = p;
          p = SL_GET_FORWARD_POINTER(p, i);
        } else {
          break;
        }
      }
    }
  } else {
    pNode = pSkipList->pTail;
    for (int32_t i = pSkipList->level - 1; i >= 0; --i) {
      SSkipListNode *p = SL_GET_BACKWARD_POINTER(pNode, i);
      while (p != pSkipList->pHead) {
        char *key = SL_GET_NODE_KEY(pSkipList, p);
        if (comparFn(key, val) > 0) {
          pNode = p;
          p = SL_GET_BACKWARD_POINTER(p, i);
        } else {
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
  pSkipList->pHead = (SSkipListNode *)malloc(SL_NODE_HEADER_SIZE(maxLevel) * 2);
  if (pSkipList->pHead == NULL) {
    return -1;
  }

  pSkipList->pHead->level = maxLevel;

  // tail info
  pSkipList->pTail = (SSkipListNode *)POINTER_SHIFT(pSkipList->pHead, SL_NODE_HEADER_SIZE(maxLevel));
  pSkipList->pTail->level = maxLevel;

  for (uint32_t i = 0; i < maxLevel; ++i) {
    SL_GET_FORWARD_POINTER(pSkipList->pHead, i) = pSkipList->pTail;
    SL_GET_BACKWARD_POINTER(pSkipList->pTail, i) = pSkipList->pHead;
  }

  return 0;
}

static FORCE_INLINE SSkipListNode *tSkipListPutNodeImpl(SSkipList *pSkipList, SSkipListNode *pNode) {
  SSkipListNode *pRetNode = NULL;
  SSkipListNode *forward[MAX_SKIP_LIST_LEVEL] = {0};

  int hasDupKey = tSkipListGetPosToPut(pSkipList, forward, pNode);
  if (SL_DUP_MODE(pSkipList->flags) == SL_DISCARD_DUP_KEY && hasDupKey) {
    pRetNode = NULL;
  } else {
    pRetNode = pNode;
    tSkipListDoInsert(pSkipList, forward, pNode, hasDupKey);

    if (pNode->level > pSkipList->level) pSkipList->level = pNode->level;
  }

  return pRetNode;
}

// static void tSkipListSeek(SSkipList *pSkipList, char *key, int order) {
//   // TODO
// }

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
//  while (pSkipList->level > 0 && SL_GET_FORWARD_POINTER(pSkipList->pHead, pSkipList->level - 1) == NULL) {
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