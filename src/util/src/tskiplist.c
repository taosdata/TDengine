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
#include "os.h"

#include "tlog.h"
#include "tskiplist.h"
#include "tutil.h"
#include "tcompare.h"

__attribute__ ((unused)) static FORCE_INLINE void recordNodeEachLevel(SSkipList *pSkipList, int32_t level) {  // record link count in each level
#if SKIP_LIST_RECORD_PERFORMANCE
  for (int32_t i = 0; i < level; ++i) {
    pSkipList->state.nLevelNodeCnt[i]++;
  }
#endif
}

__attribute__ ((unused)) static FORCE_INLINE void removeNodeEachLevel(SSkipList *pSkipList, int32_t level) {
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
  int32_t level = getSkipListNodeRandomHeight(pSkipList);
  if (pSkipList->size == 0) {
    level = 1;
    pSkipList->level = 1;
  } else {
    if (level > pSkipList->level) {
      if (pSkipList->level < pSkipList->maxLevel) {
        level = (++pSkipList->level);
      } else {
        level = pSkipList->level;
      }
    }
  }
  
  assert(level <= pSkipList->maxLevel);
  return level;
}

#define DO_MEMSET_PTR_AREA(n) do {\
int32_t _l = (n)->level;\
memset(pNode, 0, SL_NODE_HEADER_SIZE(_l));\
(n)->level = _l;\
} while(0)

static void tSkipListDoInsert(SSkipList *pSkipList, SSkipListNode **forward, SSkipListNode *pNode);
static SSkipListNode* tSkipListDoAppend(SSkipList *pSkipList, SSkipListNode *pNode);
static SSkipListIterator* doCreateSkipListIterator(SSkipList *pSkipList, int32_t order);

static bool initForwardBackwardPtr(SSkipList* pSkipList) {
  uint32_t maxLevel = pSkipList->maxLevel;
  
  // head info
  pSkipList->pHead = (SSkipListNode *)calloc(1, SL_NODE_HEADER_SIZE(maxLevel) * 2);
  if (pSkipList->pHead == NULL) {
    return false;
  }
  
  pSkipList->pHead->level = pSkipList->maxLevel;
  
  // tail info
  pSkipList->pTail = (SSkipListNode*) ((char*) pSkipList->pHead + SL_NODE_HEADER_SIZE(maxLevel));
  pSkipList->pTail->level = pSkipList->maxLevel;
  
  for(int32_t i = 0; i < maxLevel; ++i) {
    SL_GET_FORWARD_POINTER(pSkipList->pHead, i) = pSkipList->pTail;
    SL_GET_BACKWARD_POINTER(pSkipList->pTail, i) = pSkipList->pHead;
  }
  
  return true;
}
SSkipList *tSkipListCreate(uint8_t maxLevel, uint8_t keyType, uint8_t keyLen, uint8_t dupKey, uint8_t lock,
    uint8_t freeNode, __sl_key_fn_t fn) {
  SSkipList *pSkipList = (SSkipList *)calloc(1, sizeof(SSkipList));
  if (pSkipList == NULL) {
    return NULL;
  }

  if (maxLevel > MAX_SKIP_LIST_LEVEL) {
    maxLevel = MAX_SKIP_LIST_LEVEL;
  }

  pSkipList->keyInfo  = (SSkipListKeyInfo){.type = keyType, .len = keyLen, .dupKey = dupKey, .freeNode = freeNode};
  pSkipList->keyFn    = fn;
  pSkipList->comparFn = getKeyComparFunc(keyType);
  pSkipList->maxLevel = maxLevel;
  pSkipList->level    = 1;
  
  if (!initForwardBackwardPtr(pSkipList)) {
    tfree(pSkipList);
    return NULL;
  }
  
  if (lock) {
    pSkipList->lock = calloc(1, sizeof(pthread_rwlock_t));

    if (pthread_rwlock_init(pSkipList->lock, NULL) != 0) {
      tfree(pSkipList->pHead);
      tfree(pSkipList);
      
      return NULL;
    }
  }

  srand(time(NULL));

#if SKIP_LIST_RECORD_PERFORMANCE
  pSkipList->state.nTotalMemSize += sizeof(SSkipList);
#endif

  return pSkipList;
}

// static void doRemove(SSkipList *pSkipList, SSkipListNode *pNode, SSkipListNode *forward[]) {
//  int32_t level = pNode->level;
//  for (int32_t j = level - 1; j >= 0; --j) {
//    if ((forward[j]->pForward[j] != NULL) && (forward[j]->pForward[j]->pForward[j])) {
//      forward[j]->pForward[j]->pForward[j]->pBackward[j] = forward[j];
//    }
//
//    if (forward[j]->pForward[j] != NULL) {
//      forward[j]->pForward[j] = forward[j]->pForward[j]->pForward[j];
//    }
//  }
//
//  pSkipList->state.nTotalMemSize -= (sizeof(SSkipListNode) + POINTER_BYTES * pNode->level * 2);
//  removeNodeEachLevel(pSkipList, pNode->level);
//
//  tfree(pNode);
//  --pSkipList->size;
//}

void *tSkipListDestroy(SSkipList *pSkipList) {
  if (pSkipList == NULL) {
    return NULL;
  }

  if (pSkipList->lock) {
    pthread_rwlock_wrlock(pSkipList->lock);
  }

  SSkipListNode *pNode = SL_GET_FORWARD_POINTER(pSkipList->pHead, 0);

  while (pNode != pSkipList->pTail) {
    SSkipListNode *pTemp = pNode;
    pNode = SL_GET_FORWARD_POINTER(pNode, 0);
    
    if (pSkipList->keyInfo.freeNode) {
      tfree(pTemp);
    }
  }

  if (pSkipList->lock) {
    pthread_rwlock_unlock(pSkipList->lock);
    pthread_rwlock_destroy(pSkipList->lock);

    tfree(pSkipList->lock);
  }

  tfree(pSkipList->pHead);
  tfree(pSkipList);
  return NULL;
}

void tSkipListNewNodeInfo(SSkipList *pSkipList, int32_t *level, int32_t *headSize) {
  if (pSkipList == NULL) {
    return;
  }

  *level = getSkipListRandLevel(pSkipList);
  *headSize = SL_NODE_HEADER_SIZE(*level);
}

SSkipListNode *tSkipListPut(SSkipList *pSkipList, SSkipListNode *pNode) {
  if (pSkipList == NULL || pNode == NULL) {
    return NULL;
  }

  if (pSkipList->lock) {
    pthread_rwlock_wrlock(pSkipList->lock);
  }
  
  // the new key is greater than the last key of skiplist append it at last position
  char *newDatakey = SL_GET_NODE_KEY(pSkipList, pNode);
  if (pSkipList->size == 0 || pSkipList->comparFn(pSkipList->lastKey, newDatakey) < 0) {
    return tSkipListDoAppend(pSkipList, pNode);
  }
  
  // find the appropriated position to insert data
  SSkipListNode *px = pSkipList->pHead;
  SSkipListNode *forward[MAX_SKIP_LIST_LEVEL] = {0};

  int32_t ret = -1;
  for (int32_t i = pSkipList->level - 1; i >= 0; --i) {
    SSkipListNode *p = SL_GET_FORWARD_POINTER(px, i);
    while (p != pSkipList->pTail) {
      char *key = SL_GET_NODE_KEY(pSkipList, p);

      // if the forward element is less than the specified key, forward one step
      ret = pSkipList->comparFn(key, newDatakey);
      if (ret < 0) {
        px = p;
        p = SL_GET_FORWARD_POINTER(px, i);
      } else {
        break;
      }
    }
    
    forward[i] = px;
  }

  // if the skip list does not allowed identical key inserted, the new data will be discarded.
  if (pSkipList->keyInfo.dupKey == 0 && ret == 0) {
    if (pSkipList->lock) {
      pthread_rwlock_unlock(pSkipList->lock);
    }

    return forward[0];
  }
  
  tSkipListDoInsert(pSkipList, forward, pNode);
  return pNode;
}

void tSkipListDoInsert(SSkipList *pSkipList, SSkipListNode **forward, SSkipListNode *pNode) {
  DO_MEMSET_PTR_AREA(pNode);
  
  for (int32_t i = 0; i < pNode->level; ++i) {
    SSkipListNode *x = forward[i];
    SL_GET_BACKWARD_POINTER(pNode, i) = x;

    SSkipListNode *next = SL_GET_FORWARD_POINTER(x, i);
    SL_GET_BACKWARD_POINTER(next, i) = pNode;

    SL_GET_FORWARD_POINTER(pNode, i) = next;
    SL_GET_FORWARD_POINTER(x, i) = pNode;
  }
  
  atomic_add_fetch_32(&pSkipList->size, 1);
  if (pSkipList->lock) {
    pthread_rwlock_unlock(pSkipList->lock);
  }
}

SSkipListNode* tSkipListDoAppend(SSkipList *pSkipList, SSkipListNode *pNode) {
  // do clear pointer area
  DO_MEMSET_PTR_AREA(pNode);
  
  for(int32_t i = 0; i < pNode->level; ++i) {
    SSkipListNode* prev = SL_GET_BACKWARD_POINTER(pSkipList->pTail, i);
    SL_GET_FORWARD_POINTER(prev, i) = pNode;
    SL_GET_FORWARD_POINTER(pNode, i) = pSkipList->pTail;
    
    SL_GET_BACKWARD_POINTER(pNode, i) = prev;
    SL_GET_BACKWARD_POINTER(pSkipList->pTail, i) = pNode;
  }
  
  pSkipList->lastKey = SL_GET_NODE_KEY(pSkipList, pNode);
  
  atomic_add_fetch_32(&pSkipList->size, 1);
  if (pSkipList->lock) {
    pthread_rwlock_unlock(pSkipList->lock);
  }
  
  return pNode;
}

SArray* tSkipListGet(SSkipList *pSkipList, SSkipListKey pKey, int16_t keyType) {
  int32_t sLevel = pSkipList->level - 1;
  
  // result list
  SArray* sa = taosArrayInit(1, POINTER_BYTES);
  SSkipListNode *pNode = pSkipList->pHead;
 
  if (pSkipList->lock) {
    pthread_rwlock_rdlock(pSkipList->lock);
  }

#if SKIP_LIST_RECORD_PERFORMANCE
  pSkipList->state.queryCount++;
#endif

  __compar_fn_t filterComparFn = getComparFunc(pSkipList->keyInfo.type, keyType);
  int32_t ret = -1;
  for (int32_t i = sLevel; i >= 0; --i) {
    SSkipListNode *p = SL_GET_FORWARD_POINTER(pNode, i);
    while (p != pSkipList->pTail) {
      char *key = SL_GET_NODE_KEY(pSkipList, p);
      
      if ((ret = filterComparFn(key, pKey)) < 0) {
        pNode = p;
        p = SL_GET_FORWARD_POINTER(p, i);
      } else {
        break;
      }
    }

    // find the qualified key
    if (ret == 0) {
      SSkipListNode* pResult = SL_GET_FORWARD_POINTER(pNode, i);
      taosArrayPush(sa, &pResult);
      
      // skip list does not allowed duplicated key, abort further retrieve data
      if (!pSkipList->keyInfo.dupKey) {
        break;
      }
    }
  }

  if (pSkipList->lock) {
    pthread_rwlock_unlock(pSkipList->lock);
  }

  return sa;
}

size_t tSkipListGetSize(const SSkipList* pSkipList) {
  if (pSkipList == NULL) {
    return 0;
  }
  
  return pSkipList->size;
}

SSkipListIterator* tSkipListCreateIter(SSkipList *pSkipList) {
  if (pSkipList == NULL) {
    return NULL;
  }
  
  return doCreateSkipListIterator(pSkipList, TSDB_ORDER_ASC);
}

SSkipListIterator *tSkipListCreateIterFromVal(SSkipList* pSkipList, const char* val, int32_t type, int32_t order) {
  if (pSkipList == NULL) {
    return NULL;
  }
  
  assert(order == TSDB_ORDER_ASC || order == TSDB_ORDER_DESC);
  
  if (val == NULL) {
    return doCreateSkipListIterator(pSkipList, order);
  } else {
  
    SSkipListNode *forward[MAX_SKIP_LIST_LEVEL] = {0};
  
    int32_t ret = -1;
    __compar_fn_t filterComparFn = getComparFunc(pSkipList->keyInfo.type, type);
    SSkipListNode* pNode = pSkipList->pHead;
    
    for (int32_t i = pSkipList->level - 1; i >= 0; --i) {
      SSkipListNode *p = SL_GET_FORWARD_POINTER(pNode, i);
      while (p != pSkipList->pTail) {
        char *key = SL_GET_NODE_KEY(pSkipList, p);
      
        if ((ret = filterComparFn(key, val)) < 0) {
          pNode = p;
          p = SL_GET_FORWARD_POINTER(p, i);
        } else {
          break;
        }
      }
    
      forward[i] = pNode;
    }
    
    SSkipListIterator* iter = doCreateSkipListIterator(pSkipList, order);
    
    // set the initial position
    if (order == TSDB_ORDER_ASC) {
      iter->cur = forward[0]; // greater equals than the value
    } else {
      iter->cur = SL_GET_FORWARD_POINTER(forward[0], 0);

      if (ret == 0) {
        assert(iter->cur != pSkipList->pTail);
        iter->cur = SL_GET_FORWARD_POINTER(iter->cur, 0);
      }
    }
    
    return iter;
  }
}

bool tSkipListIterNext(SSkipListIterator *iter) {
  if (iter->pSkipList == NULL) {
    return false;
  }
  
  SSkipList *pSkipList = iter->pSkipList;
  
  if (pSkipList->lock) {
    pthread_rwlock_rdlock(pSkipList->lock);
  }
  
  if (iter->order == TSDB_ORDER_ASC) {  // ascending order iterate
    if (iter->cur == NULL) {
      iter->cur = SL_GET_FORWARD_POINTER(pSkipList->pHead, 0);
    } else {
      iter->cur = SL_GET_FORWARD_POINTER(iter->cur, 0);
    }
  } else { // descending order iterate
    if (iter->cur == NULL) {
      iter->cur = SL_GET_BACKWARD_POINTER(pSkipList->pTail, 0);
    } else {
      iter->cur = SL_GET_BACKWARD_POINTER(iter->cur, 0);
    }
  }
  
  if (pSkipList->lock) {
    pthread_rwlock_unlock(pSkipList->lock);
  }
  
  return (iter->order == TSDB_ORDER_ASC)? (iter->cur != pSkipList->pTail) : (iter->cur != pSkipList->pHead);
}

SSkipListNode *tSkipListIterGet(SSkipListIterator *iter) { 
  if (iter == NULL || iter->cur == iter->pSkipList->pTail || iter->cur == iter->pSkipList->pHead) {
     return NULL;
  } else {
    return iter->cur;
  }
}

void* tSkipListDestroyIter(SSkipListIterator* iter) {
  if (iter == NULL) {
    return NULL;
  }
  
  tfree(iter);
  return NULL;
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
//  while (pSkipList->level > 0 && pSkipList->pHead.pForward[pSkipList->level - 1] == NULL) {
//    pSkipList->level -= 1;
//  }
//
//  return true;
//}
//
// void tSkipListRemoveNode(SSkipList *pSkipList, SSkipListNode *pNode) {
//  SSkipListNode *forward[MAX_SKIP_LIST_LEVEL] = {0};
//
//  pthread_rwlock_rdlock(&pSkipList->lock);
//  for (int32_t i = 0; i < pNode->level; ++i) {
//    forward[i] = pNode->pBackward[i];
//  }
//
//  removeSupport(pSkipList, forward, &pNode->key);
//  pthread_rwlock_unlock(&pSkipList->lock);
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

void tSkipListPrint(SSkipList *pSkipList, int16_t nlevel) {
  if (pSkipList == NULL || pSkipList->level < nlevel || nlevel <= 0) {
    return;
  }

  SSkipListNode *p = SL_GET_FORWARD_POINTER(pSkipList->pHead, nlevel - 1);
  
  int32_t id = 1;
  char* prev = NULL;
  
  while (p != pSkipList->pTail) {
    char *key = SL_GET_NODE_KEY(pSkipList, p);
    if (prev != NULL) {
      assert(pSkipList->comparFn(prev, key) < 0);
    }
    
    switch (pSkipList->keyInfo.type) {
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

SSkipListIterator* doCreateSkipListIterator(SSkipList *pSkipList, int32_t order) {
  SSkipListIterator* iter = calloc(1, sizeof(SSkipListIterator));
  
  iter->pSkipList = pSkipList;
  iter->order = order;
  
  return iter;
}
