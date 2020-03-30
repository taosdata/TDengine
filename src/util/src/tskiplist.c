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

static FORCE_INLINE void recordNodeEachLevel(SSkipList *pSkipList, int32_t level) {  // record link count in each level
#if SKIP_LIST_RECORD_PERFORMANCE
  for (int32_t i = 0; i < level; ++i) {
    pSkipList->state.nLevelNodeCnt[i]++;
  }
#endif
}

static FORCE_INLINE void removeNodeEachLevel(SSkipList *pSkipList, int32_t level) {
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
    if (level > pSkipList->level && pSkipList->level < pSkipList->maxLevel) {
      level = (++pSkipList->level);
    }
  }
  return level;
}

static void tSkipListDoInsert(SSkipList *pSkipList, SSkipListNode **forward, int32_t level, SSkipListNode *pNode);

void tSkipListDoRecordPut(SSkipList *pSkipList) {
#if SKIP_LIST_RECORD_PERFORMANCE
  const int32_t MAX_RECORD_NUM = 1000;

  if (pSkipList->state.nInsertObjs == MAX_RECORD_NUM) {
    pSkipList->state.nInsertObjs = 1;
    pSkipList->state.nTotalStepsForInsert = 0;
    pSkipList->state.nTotalElapsedTimeForInsert = 0;
  } else {
    pSkipList->state.nInsertObjs++;
  }
#endif
}

int32_t compareInt32Val(const void *pLeft, const void *pRight) {
  int32_t ret = GET_INT32_VAL(pLeft) - GET_INT32_VAL(pRight);
  if (ret == 0) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

int32_t compareInt64Val(const void *pLeft, const void *pRight) {
  int32_t ret = GET_INT64_VAL(pLeft) - GET_INT64_VAL(pRight);
  if (ret == 0) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

int32_t compareInt16Val(const void *pLeft, const void *pRight) {
  int32_t ret = GET_INT16_VAL(pLeft) - GET_INT16_VAL(pRight);
  if (ret == 0) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

int32_t compareInt8Val(const void *pLeft, const void *pRight) {
  int32_t ret = GET_INT8_VAL(pLeft) - GET_INT8_VAL(pRight);
  if (ret == 0) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

int32_t compareIntDoubleVal(const void *pLeft, const void *pRight) {
  //  int64_t lhs = ((SSkipListKey *)pLeft)->i64Key;
  //  double  rhs = ((SSkipListKey *)pRight)->dKey;
  //  if (fabs(lhs - rhs) < FLT_EPSILON) {
  //    return 0;
  //  } else {
  //    return (lhs > rhs) ? 1 : -1;
  //  }
  return 0;
}

int32_t compareDoubleIntVal(const void *pLeft, const void *pRight) {
  //  double  lhs = ((SSkipListKey *)pLeft)->dKey;
  //  int64_t rhs = ((SSkipListKey *)pRight)->i64Key;
  //  if (fabs(lhs - rhs) < FLT_EPSILON) {
  //    return 0;
  //  } else {
  //    return (lhs > rhs) ? 1 : -1;
  //  }
  return 0;
}

int32_t compareDoubleVal(const void *pLeft, const void *pRight) {
  double ret = GET_DOUBLE_VAL(pLeft) - GET_DOUBLE_VAL(pRight);
  if (fabs(ret) < FLT_EPSILON) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

int32_t compareStrVal(const void *pLeft, const void *pRight) {
  //  SSkipListKey *pL = (SSkipListKey *)pLeft;
  //  SSkipListKey *pR = (SSkipListKey *)pRight;
  //
  //  if (pL->nLen == 0 && pR->nLen == 0) {
  //    return 0;
  //  }
  //
  //  // handle only one-side bound compare situation, there is only lower bound or only upper bound
  //  if (pL->nLen == -1) {
  //    return 1;  // no lower bound, lower bound is minimum, always return -1;
  //  } else if (pR->nLen == -1) {
  //    return -1;  // no upper bound, upper bound is maximum situation, always return 1;
  //  }
  //
  //  int32_t ret = strcmp(((SSkipListKey *)pLeft)->pz, ((SSkipListKey *)pRight)->pz);
  //
  //  if (ret == 0) {
  //    return 0;
  //  } else {
  //    return ret > 0 ? 1 : -1;
  //  }
  return 0;
}

int32_t compareWStrVal(const void *pLeft, const void *pRight) {
  //  SSkipListKey *pL = (SSkipListKey *)pLeft;
  //  SSkipListKey *pR = (SSkipListKey *)pRight;
  //
  //  if (pL->nLen == 0 && pR->nLen == 0) {
  //    return 0;
  //  }
  //
  //  // handle only one-side bound compare situation, there is only lower bound or only upper bound
  //  if (pL->nLen == -1) {
  //    return 1;  // no lower bound, lower bound is minimum, always return -1;
  //  } else if (pR->nLen == -1) {
  //    return -1;  // no upper bound, upper bound is maximum situation, always return 1;
  //  }
  //
  //  int32_t ret = wcscmp(((SSkipListKey *)pLeft)->wpz, ((SSkipListKey *)pRight)->wpz);
  //
  //  if (ret == 0) {
  //    return 0;
  //  } else {
  //    return ret > 0 ? 1 : -1;
  //  }
  return 0;
}

static __compar_fn_t getKeyFilterComparator(SSkipList *pSkipList, int32_t filterDataType) {
  __compar_fn_t comparFn = NULL;

  switch (pSkipList->keyInfo.type) {
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT: {
      if (filterDataType == TSDB_DATA_TYPE_BIGINT) {
        comparFn = compareInt64Val;
        break;
      }
    }
    case TSDB_DATA_TYPE_BOOL: {
      if (filterDataType >= TSDB_DATA_TYPE_BOOL && filterDataType <= TSDB_DATA_TYPE_BIGINT) {
        comparFn = compareInt32Val;
      } else if (filterDataType >= TSDB_DATA_TYPE_FLOAT && filterDataType <= TSDB_DATA_TYPE_DOUBLE) {
        comparFn = compareIntDoubleVal;
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE: {
//      if (filterDataType >= TSDB_DATA_TYPE_BOOL && filterDataType <= TSDB_DATA_TYPE_BIGINT) {
//        comparFn = compareDoubleIntVal;
//      } else if (filterDataType >= TSDB_DATA_TYPE_FLOAT && filterDataType <= TSDB_DATA_TYPE_DOUBLE) {
//        comparFn = compareDoubleVal;
//      }
      if (filterDataType == TSDB_DATA_TYPE_DOUBLE) {
        comparFn = compareDoubleVal;
      }
      break;
    }
    case TSDB_DATA_TYPE_BINARY:
      comparFn = compareStrVal;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      comparFn = compareWStrVal;
      break;
    default:
      comparFn = compareInt32Val;
      break;
  }

  return comparFn;
}

static __compar_fn_t getKeyComparator(int32_t keyType) {
  __compar_fn_t comparFn = NULL;

  switch (keyType) {
    case TSDB_DATA_TYPE_TINYINT:
      comparFn = compareInt8Val;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      comparFn = compareInt16Val;
      break;
    case TSDB_DATA_TYPE_INT:
      comparFn = compareInt32Val;
      break;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      comparFn = compareInt64Val;
      break;
    case TSDB_DATA_TYPE_BOOL:
      comparFn = compareInt32Val;
      break;

    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      comparFn = compareDoubleVal;
      break;

    case TSDB_DATA_TYPE_BINARY:
      comparFn = compareStrVal;
      break;

    case TSDB_DATA_TYPE_NCHAR:
      comparFn = compareWStrVal;
      break;

    default:
      comparFn = compareInt32Val;
      break;
  }

  return comparFn;
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

  pSkipList->keyInfo = (SSkipListKeyInfo){.type = keyType, .len = keyLen, .dupKey = dupKey, .freeNode = freeNode};
  pSkipList->keyFn = fn;
  pSkipList->comparFn = getKeyComparator(keyType);
  pSkipList->maxLevel = maxLevel;
  pSkipList->level = 1;

  pSkipList->pHead = (SSkipListNode *)calloc(1, SL_NODE_HEADER_SIZE(maxLevel));
  pSkipList->pHead->level = pSkipList->maxLevel;

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

  while (pNode) {
    SSkipListNode *pTemp = pNode;
    pNode = SL_GET_FORWARD_POINTER(pNode, 0);
    
    if (pSkipList->keyInfo.freeNode) {
      tfree(pTemp);
    }
  }

  tfree(pSkipList->pHead);

  if (pSkipList->lock) {
    pthread_rwlock_unlock(pSkipList->lock);
    pthread_rwlock_destroy(pSkipList->lock);

    tfree(pSkipList->lock);
  }

  tfree(pSkipList->pHead);
  tfree(pSkipList);
  return NULL;
}

void tSkipListRandNodeInfo(SSkipList *pSkipList, int32_t *level, int32_t *headSize) {
  if (pSkipList == NULL) {
    return;
  }

  *level = getSkipListRandLevel(pSkipList);
  *headSize = SL_NODE_HEADER_SIZE(*level);
}

SSkipListNode *tSkipListPut(SSkipList *pSkipList, SSkipListNode *pNode) {
  if (pSkipList == NULL) {
    return NULL;
  }

  if (pSkipList->lock) {
    pthread_rwlock_wrlock(pSkipList->lock);
  }

  // record one node is put into skiplist
  tSkipListDoRecordPut(pSkipList);

  SSkipListNode *px = pSkipList->pHead;
  SSkipListNode *forward[MAX_SKIP_LIST_LEVEL] = {0};

  bool identical = false;
  for (int32_t i = pSkipList->level - 1; i >= 0; --i) {
    SSkipListNode *p = SL_GET_FORWARD_POINTER(px, i);
    while (p != NULL) {
      char *key = SL_GET_NODE_KEY(pSkipList, p);
      char *newDatakey = SL_GET_NODE_KEY(pSkipList, pNode);

      // if the forward element is less than the specified key, forward one step
      int32_t ret = pSkipList->comparFn(key, newDatakey);
      if (ret < 0) {
        px = p;

        p = SL_GET_FORWARD_POINTER(px, i);
      } else {
        if (identical == false) {
          identical = (ret == 0);
        }
        
        break;
      }
    }

#if SKIP_LIST_RECORD_PERFORMANCE
    pSkipList->state.nTotalStepsForInsert++;
#endif
    forward[i] = px;
  }

  // if the skip list does not allowed identical key inserted, the new data will be discarded.
  if (pSkipList->keyInfo.dupKey == 0 && identical) {
    if (pSkipList->lock) {
      pthread_rwlock_unlock(pSkipList->lock);
    }

    return forward[0];
  }

#if SKIP_LIST_RECORD_PERFORMANCE
  recordNodeEachLevel(pSkipList, level);
#endif

  // clear pointer area
  int32_t level = SL_GET_NODE_LEVEL(pNode);
  memset(pNode, 0, SL_NODE_HEADER_SIZE(pNode->level));
  pNode->level = level;
  
  tSkipListDoInsert(pSkipList, forward, level, pNode);

  atomic_add_fetch_32(&pSkipList->size, 1);

#if SKIP_LIST_RECORD_PERFORMANCE
  pSkipList->state.nTotalMemSize += getOneNodeSize(pKey, level);
#endif

  if (pSkipList->lock) {
    pthread_rwlock_unlock(pSkipList->lock);
  }

  return pNode;
}

void tSkipListDoInsert(SSkipList *pSkipList, SSkipListNode **forward, int32_t level, SSkipListNode *pNode) {
  for (int32_t i = 0; i < level; ++i) {
    SSkipListNode *x = forward[i];
    if (x != NULL) {
      SL_GET_BACKWARD_POINTER(pNode, i) = x;

      SSkipListNode *pForward = SL_GET_FORWARD_POINTER(x, i);
      if (pForward) {
        SL_GET_BACKWARD_POINTER(pForward, i) = pNode;
      }

      SL_GET_FORWARD_POINTER(pNode, i) = SL_GET_FORWARD_POINTER(x, i);
      SL_GET_FORWARD_POINTER(x, i) = pNode;
    } else {
      SL_GET_FORWARD_POINTER(pSkipList->pHead, i) = pNode;
      SL_GET_BACKWARD_POINTER(pSkipList->pHead, i) = (pSkipList->pHead);
    }
  }
}

SArray* tSkipListGet(SSkipList *pSkipList, SSkipListKey pKey, int16_t keyType) {
  int32_t sLevel = pSkipList->level - 1;
  int32_t ret = -1;

  // result list
  SArray* sa = taosArrayInit(1, POINTER_BYTES);
  
  SSkipListNode *pNode = pSkipList->pHead;
 
  if (pSkipList->lock) {
    pthread_rwlock_rdlock(pSkipList->lock);
  }

#if SKIP_LIST_RECORD_PERFORMANCE
  pSkipList->state.queryCount++;
#endif

  __compar_fn_t filterComparFn = getKeyFilterComparator(pSkipList, keyType);

  for (int32_t i = sLevel; i >= 0; --i) {
    SSkipListNode *pNext = SL_GET_FORWARD_POINTER(pNode, i);
    while (pNext != NULL) {
      char *key = SL_GET_NODE_KEY(pSkipList, pNext);
      if ((ret = filterComparFn(key, pKey)) < 0) {
        pNode = pNext;
        pNext = SL_GET_FORWARD_POINTER(pNext, i);
      } else {
        break;
      }
    }

    // find the qualified key
    if (ret == 0) {
      if (pSkipList->lock) {
        pthread_rwlock_unlock(pSkipList->lock);
      }

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
  
  SSkipListIterator* iter = calloc(1, sizeof(SSkipListIterator));
  
  iter->pSkipList = pSkipList;
  if (pSkipList->lock) {
    pthread_rwlock_rdlock(pSkipList->lock);
  }
  
  iter->cur = NULL;
  iter->num = pSkipList->size;
  
  if (pSkipList->lock) {
    pthread_rwlock_unlock(pSkipList->lock);
  }
  
  return iter;
}

bool tSkipListIterNext(SSkipListIterator *iter) {
  if (iter->num == 0 || iter->pSkipList == NULL) {
    return false;
  }
  
  SSkipList *pSkipList = iter->pSkipList;
  
  if (pSkipList->lock) {
    pthread_rwlock_rdlock(pSkipList->lock);
  }
  
  if (iter->cur == NULL) {
    iter->cur = SL_GET_FORWARD_POINTER(pSkipList->pHead, 0);
  } else {
    iter->cur = SL_GET_FORWARD_POINTER(iter->cur, 0);
  }
  
  if (pSkipList->lock) {
    pthread_rwlock_unlock(pSkipList->lock);
  }
  
  return iter->cur != NULL;
}

SSkipListNode *tSkipListIterGet(SSkipListIterator *iter) { return (iter == NULL)? NULL:iter->cur; }

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
//  __compar_fn_t filterComparFn = getKeyFilterComparator(pSkipList, pEndKey->nType);
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
//  __compar_fn_t filterComparFn = getKeyFilterComparator(pSkipList, pKey->nType);
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
//  __compar_fn_t  filterComparFn = getKeyFilterComparator(pSkipList, pKey->nType);
//
//  pthread_rwlock_rdlock(&pSkipList->lock);
//
//  if (cond == TSDB_RELATION_LARGE_EQUAL || cond == TSDB_RELATION_LARGE) {
//    for (int32_t i = sLevel; i >= 0; --i) {
//      while (x->pForward[i] != NULL && (ret = filterComparFn(&x->pForward[i]->key, pKey)) < 0) {
//        x = x->pForward[i];
//      }
//    }
//
//    // backward check if previous nodes are with the same value.
//    if (cond == TSDB_RELATION_LARGE_EQUAL && ret == 0) {
//      SSkipListNode *pNode = x->pForward[0];
//      while ((pNode->pBackward[0] != &pSkipList->pHead) && (filterComparFn(&pNode->pBackward[0]->key, pKey) == 0)) {
//        pNode = pNode->pBackward[0];
//      }
//      pthread_rwlock_unlock(&pSkipList->lock);
//      return pNode;
//    }
//
//    if (ret > 0 || cond == TSDB_RELATION_LARGE_EQUAL) {
//      pthread_rwlock_unlock(&pSkipList->lock);
//      return x->pForward[0];
//    } else {  // cond == TSDB_RELATION_LARGE && ret == 0
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
// int32_t tSkipListRangeQuery(SSkipList *pSkipList, tSKipListQueryCond *pCond, SSkipListNode ***pRes) {
//  pSkipList->state.queryCount++;
//  SSkipListNode *pStart = tSkipListParQuery(pSkipList, &pCond->lowerBnd, pCond->lowerBndRelOptr);
//  if (pStart == 0) {
//    *pRes = NULL;
//    return 0;
//  }
//
//  return tSkipListEndParQuery(pSkipList, pStart, &pCond->upperBnd, pCond->upperBndRelOptr, pRes);
//}
//
// static bool removeSupport(SSkipList *pSkipList, SSkipListNode **forward, SSkipListKey *pKey) {
//  __compar_fn_t filterComparFn = getKeyFilterComparator(pSkipList, pKey->nType);
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
//  __compar_fn_t  filterComparFn = getKeyFilterComparator(pSkipList, pKey->nType);
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

  while (p) {
    char *key = SL_GET_NODE_KEY(pSkipList, p);
    switch (pSkipList->keyInfo.type) {
      case TSDB_DATA_TYPE_INT:
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

    p = SL_GET_FORWARD_POINTER(p, nlevel - 1);
    //    p = p->pForward[nlevel - 1];
  }
}
