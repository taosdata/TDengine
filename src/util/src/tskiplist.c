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

#include <float.h>
#include <math.h>
#include <stdbool.h>
#include <stdlib.h>

#include "tlog.h"
#include "tsdb.h"
#include "tskiplist.h"
#include "tutil.h"

static uint32_t doGetRand(SRandom *pRand, int32_t n) {
  const uint32_t val = 2147483647L;
  uint64_t       p = pRand->s * 16807;

  pRand->s = (uint32_t)((p >> 31) + (p & val));
  if (pRand->s > val) {
    pRand->s -= val;
  }

  return (pRand->s % n);
}

static SRandom getRand(uint32_t s) {
  uint32_t seed = s & 0x7FFFFFFF;
  if (seed == 0 || seed == INT32_MAX) {
    seed = 1;
  }

  struct SRandom r = {seed, doGetRand};
  return r;
}

void recordNodeEachLevel(tSkipList *pSkipList, int32_t nLevel);

int32_t getSkipListNodeLevel(tSkipList *pSkipList);

void tSkipListDoInsert(tSkipList *pSkipList, tSkipListNode **forward, int32_t nLevel, tSkipListNode *pNode);

static int32_t getSkipListNodeRandomHeight(tSkipList *pSkipList) {
  const uint32_t factor = 4;

  int32_t n = 1;
  while ((pSkipList->r.rand(&pSkipList->r, MAX_SKIP_LIST_LEVEL) % factor) == 0 && n <= MAX_SKIP_LIST_LEVEL) {
    n++;
  }

  return n;
}

void tSkipListDoRecordPutNode(tSkipList *pSkipList) {
  const int32_t MAX_RECORD_NUM = 1000;

  if (pSkipList->state.nInsertObjs == MAX_RECORD_NUM) {
    pSkipList->state.nInsertObjs = 1;
    pSkipList->state.nTotalStepsForInsert = 0;
    pSkipList->state.nTotalElapsedTimeForInsert = 0;
  } else {
    pSkipList->state.nInsertObjs++;
  }
}

int32_t compareIntVal(const void *pLeft, const void *pRight) {
  int64_t lhs = ((tSkipListKey *)pLeft)->i64Key;
  int64_t rhs = ((tSkipListKey *)pRight)->i64Key;

  DEFAULT_COMP(lhs, rhs);
}

int32_t compareIntDoubleVal(const void *pLeft, const void *pRight) {
  int64_t lhs = ((tSkipListKey *)pLeft)->i64Key;
  double  rhs = ((tSkipListKey *)pRight)->dKey;
  if (fabs(lhs - rhs) < FLT_EPSILON) {
    return 0;
  } else {
    return (lhs > rhs) ? 1 : -1;
  }
}

int32_t compareDoubleIntVal(const void *pLeft, const void *pRight) {
  double  lhs = ((tSkipListKey *)pLeft)->dKey;
  int64_t rhs = ((tSkipListKey *)pRight)->i64Key;
  if (fabs(lhs - rhs) < FLT_EPSILON) {
    return 0;
  } else {
    return (lhs > rhs) ? 1 : -1;
  }
}

int32_t compareDoubleVal(const void *pLeft, const void *pRight) {
  double ret = (((tSkipListKey *)pLeft)->dKey - ((tSkipListKey *)pRight)->dKey);
  if (fabs(ret) < FLT_EPSILON) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

int32_t compareStrVal(const void *pLeft, const void *pRight) {
  tSkipListKey *pL = (tSkipListKey *)pLeft;
  tSkipListKey *pR = (tSkipListKey *)pRight;

  if (pL->nLen == 0 && pR->nLen == 0) {
    return 0;
  }

  /*
   * handle only one-side bound compare situation, there is only lower bound or
   * only
   * upper bound
   */
  if (pL->nLen == -1) {
    return 1;  // no lower bound, lower bound is minimum, always return -1;
  } else if (pR->nLen == -1) {
    return -1;  // no upper bound, upper bound is maximum situation, always
                // return 1;
  }

  int32_t ret = strcmp(((tSkipListKey *)pLeft)->pz, ((tSkipListKey *)pRight)->pz);

  if (ret == 0) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

int32_t compareWStrVal(const void *pLeft, const void *pRight) {
  tSkipListKey *pL = (tSkipListKey *)pLeft;
  tSkipListKey *pR = (tSkipListKey *)pRight;

  if (pL->nLen == 0 && pR->nLen == 0) {
    return 0;
  }

  /*
   * handle only one-side bound compare situation,
   * there is only lower bound or only upper bound
   */
  if (pL->nLen == -1) {
    return 1;  // no lower bound, lower bound is minimum, always return -1;
  } else if (pR->nLen == -1) {
    return -1;  // no upper bound, upper bound is maximum situation, always
                // return 1;
  }

  int32_t ret = wcscmp(((tSkipListKey *)pLeft)->wpz, ((tSkipListKey *)pRight)->wpz);

  if (ret == 0) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

static __compar_fn_t getKeyFilterComparator(tSkipList *pSkipList, int32_t filterDataType) {
  __compar_fn_t comparator = NULL;

  switch (pSkipList->keyType) {
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_BOOL: {
      if (filterDataType >= TSDB_DATA_TYPE_BOOL && filterDataType <= TSDB_DATA_TYPE_BIGINT) {
        comparator = compareIntVal;
      } else if (filterDataType >= TSDB_DATA_TYPE_FLOAT && filterDataType <= TSDB_DATA_TYPE_DOUBLE) {
        comparator = compareIntDoubleVal;
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE: {
      if (filterDataType >= TSDB_DATA_TYPE_BOOL && filterDataType <= TSDB_DATA_TYPE_BIGINT) {
        comparator = compareDoubleIntVal;
      } else if (filterDataType >= TSDB_DATA_TYPE_FLOAT && filterDataType <= TSDB_DATA_TYPE_DOUBLE) {
        comparator = compareDoubleVal;
      }
      break;
    }
    case TSDB_DATA_TYPE_BINARY:
      comparator = compareStrVal;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      comparator = compareWStrVal;
      break;
    default:
      comparator = compareIntVal;
      break;
  }

  return comparator;
}

static __compar_fn_t getKeyComparator(int32_t keyType) {
  __compar_fn_t comparator = NULL;

  switch (keyType) {
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_BOOL:
      comparator = compareIntVal;
      break;

    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      comparator = compareDoubleVal;
      break;

    case TSDB_DATA_TYPE_BINARY:
      comparator = compareStrVal;
      break;

    case TSDB_DATA_TYPE_NCHAR:
      comparator = compareWStrVal;
      break;

    default:
      comparator = compareIntVal;
      break;
  }

  return comparator;
}

int32_t tSkipListCreate(tSkipList **pSkipList, int16_t nMaxLevel, int16_t keyType, int16_t nMaxKeyLen,
                        int32_t (*funcp)()) {
  (*pSkipList) = (tSkipList *)calloc(1, sizeof(tSkipList));
  if ((*pSkipList) == NULL) {
    return -1;
  }

  (*pSkipList)->keyType = keyType;

  (*pSkipList)->comparator = getKeyComparator(keyType);
  (*pSkipList)->pHead.pForward = (tSkipListNode **)calloc(1, POINTER_BYTES * MAX_SKIP_LIST_LEVEL);

  (*pSkipList)->nMaxLevel = MAX_SKIP_LIST_LEVEL;
  (*pSkipList)->nLevel = 1;

  (*pSkipList)->nMaxKeyLen = nMaxKeyLen;
  (*pSkipList)->nMaxLevel = nMaxLevel;

  if (pthread_rwlock_init(&(*pSkipList)->lock, NULL) != 0) {
    return -1;
  }

  srand(time(NULL));
  (*pSkipList)->r = getRand(time(NULL));

  (*pSkipList)->state.nTotalMemSize += sizeof(tSkipList);
  return 0;
}

static void doRemove(tSkipList *pSkipList, tSkipListNode *pNode, tSkipListNode *forward[]) {
  int32_t level = pNode->nLevel;
  for (int32_t j = level - 1; j >= 0; --j) {
    if ((forward[j]->pForward[j] != NULL) && (forward[j]->pForward[j]->pForward[j])) {
      forward[j]->pForward[j]->pForward[j]->pBackward[j] = forward[j];
    }

    if (forward[j]->pForward[j] != NULL) {
      forward[j]->pForward[j] = forward[j]->pForward[j]->pForward[j];
    }
  }

  pSkipList->state.nTotalMemSize -= (sizeof(tSkipListNode) + POINTER_BYTES * pNode->nLevel * 2);
  removeNodeEachLevel(pSkipList, pNode->nLevel);

  tfree(pNode);
  --pSkipList->nSize;
}

static size_t getOneNodeSize(const tSkipListKey *pKey, int32_t nLevel) {
  size_t size = sizeof(tSkipListNode) + sizeof(intptr_t) * (nLevel << 1);
  if (pKey->nType == TSDB_DATA_TYPE_BINARY) {
    size += pKey->nLen + 1;
  } else if (pKey->nType == TSDB_DATA_TYPE_NCHAR) {
    size += (pKey->nLen + 1) * TSDB_NCHAR_SIZE;
  }

  return size;
}

static tSkipListNode *tSkipListCreateNode(void *pData, const tSkipListKey *pKey, int32_t nLevel) {
  size_t         nodeSize = getOneNodeSize(pKey, nLevel);
  tSkipListNode *pNode = (tSkipListNode *)calloc(1, nodeSize);

  pNode->pForward = (tSkipListNode **)(&pNode[1]);
  pNode->pBackward = (pNode->pForward + nLevel);

  pNode->pData = pData;

  pNode->key = *pKey;
  if (pKey->nType == TSDB_DATA_TYPE_BINARY) {
    pNode->key.pz = (char *)(pNode->pBackward + nLevel);

    strcpy(pNode->key.pz, pKey->pz);
    pNode->key.pz[pKey->nLen] = 0;
  } else if (pKey->nType == TSDB_DATA_TYPE_NCHAR) {
    pNode->key.wpz = (wchar_t *)(pNode->pBackward + nLevel);
    wcsncpy(pNode->key.wpz, pKey->wpz, pKey->nLen);
    pNode->key.wpz[pKey->nLen] = 0;
  }

  pNode->nLevel = nLevel;
  return pNode;
}

tSkipListKey tSkipListCreateKey(int32_t type, char *val, size_t keyLength) {
  tSkipListKey k;
  k.nType = (uint8_t)type;

  switch (type) {
    case TSDB_DATA_TYPE_INT: {
      k.i64Key = *(int32_t *)val;
      return k;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      k.i64Key = *(int64_t *)val;
      return k;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      k.dKey = *(double *)val;
      return k;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      k.dKey = *(float *)val;
      return k;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      k.i64Key = *(int16_t *)val;
      return k;
    }
    case TSDB_DATA_TYPE_TINYINT: {
      k.i64Key = *(int8_t *)val;
      return k;
    }
    case TSDB_DATA_TYPE_BOOL: {
      k.i64Key = *(int8_t *)val;
      return k;
    }
    case TSDB_DATA_TYPE_BINARY: {
      k.pz = malloc(keyLength + 1);
      k.nLen = keyLength;
      memcpy(k.pz, val, keyLength);
      k.pz[keyLength] = 0;
      return k;
    }
    case TSDB_DATA_TYPE_NCHAR: {
      k.pz = malloc(keyLength + TSDB_NCHAR_SIZE);
      k.nLen = keyLength / TSDB_NCHAR_SIZE;

      wcsncpy(k.wpz, (wchar_t *)val, k.nLen);
      k.wpz[k.nLen] = 0;

      return k;
    }
    default:
      return k;
  }
}

void tSkipListDestroyKey(tSkipListKey *pKey) { tVariantDestroy(pKey); }

void tSkipListDestroy(tSkipList **pSkipList) {
  if ((*pSkipList) == NULL) {
    return;
  }

  pthread_rwlock_wrlock(&(*pSkipList)->lock);
  tSkipListNode *pNode = (*pSkipList)->pHead.pForward[0];
  while (pNode) {
    tSkipListNode *pTemp = pNode;
    pNode = pNode->pForward[0];
    tfree(pTemp);
  }

  tfree((*pSkipList)->pHead.pForward);
  pthread_rwlock_unlock(&(*pSkipList)->lock);
  tfree(*pSkipList);
}

tSkipListNode *tSkipListPut(tSkipList *pSkipList, void *pData, tSkipListKey *pKey, int32_t insertIdenticalKey) {
  if (pSkipList == NULL) {
    return NULL;
  }

  pthread_rwlock_wrlock(&pSkipList->lock);

  // record one node is put into skiplist
  tSkipListDoRecordPutNode(pSkipList);

  tSkipListNode *px = &pSkipList->pHead;

  tSkipListNode *forward[MAX_SKIP_LIST_LEVEL] = {0};
  for (int32_t i = pSkipList->nLevel - 1; i >= 0; --i) {
    while (px->pForward[i] != NULL && (pSkipList->comparator(&px->pForward[i]->key, pKey) < 0)) {
      px = px->pForward[i];
    }

    pSkipList->state.nTotalStepsForInsert++;
    forward[i] = px;
  }

  if ((insertIdenticalKey == 0) && forward[0] != &pSkipList->pHead &&
      (pSkipList->comparator(&forward[0]->key, pKey) == 0)) {
    /* ignore identical key*/
    pthread_rwlock_unlock(&pSkipList->lock);
    return forward[0];
  }

  int32_t nLevel = getSkipListNodeLevel(pSkipList);
  recordNodeEachLevel(pSkipList, nLevel);

  tSkipListNode *pNode = tSkipListCreateNode(pData, pKey, nLevel);
  tSkipListDoInsert(pSkipList, forward, nLevel, pNode);

  pSkipList->nSize += 1;

  //    char tmpstr[512] = {0};
  //    tVariantToString(&pNode->key, tmpstr);
  //    pTrace("skiplist:%p, node added, key:%s, total list len:%d", pSkipList,
  //    tmpstr, pSkipList->nSize);

  pSkipList->state.nTotalMemSize += getOneNodeSize(pKey, nLevel);
  pthread_rwlock_unlock(&pSkipList->lock);

  return pNode;
}

void tSkipListDoInsert(tSkipList *pSkipList, tSkipListNode **forward, int32_t nLevel, tSkipListNode *pNode) {
  for (int32_t i = 0; i < nLevel; ++i) {
    tSkipListNode *x = forward[i];
    if (x != NULL) {
      pNode->pBackward[i] = x;
      if (x->pForward[i]) x->pForward[i]->pBackward[i] = pNode;

      pNode->pForward[i] = x->pForward[i];
      x->pForward[i] = pNode;
    } else {
      pSkipList->pHead.pForward[i] = pNode;
      pNode->pBackward[i] = &(pSkipList->pHead);
    }
  }
}

int32_t getSkipListNodeLevel(tSkipList *pSkipList) {
  int32_t nLevel = getSkipListNodeRandomHeight(pSkipList);
  if (pSkipList->nSize == 0) {
    nLevel = 1;
    pSkipList->nLevel = 1;
  } else {
    if (nLevel > pSkipList->nLevel && pSkipList->nLevel < pSkipList->nMaxLevel) {
      nLevel = (++pSkipList->nLevel);
    }
  }
  return nLevel;
}

void recordNodeEachLevel(tSkipList *pSkipList, int32_t nLevel) {  // record link count in each level
  for (int32_t i = 0; i < nLevel; ++i) {
    pSkipList->state.nLevelNodeCnt[i]++;
  }
}

void removeNodeEachLevel(tSkipList *pSkipList, int32_t nLevel) {
  for (int32_t i = 0; i < nLevel; ++i) {
    pSkipList->state.nLevelNodeCnt[i]--;
  }
}

tSkipListNode *tSkipListGetOne(tSkipList *pSkipList, tSkipListKey *pKey) {
  int32_t sLevel = pSkipList->nLevel - 1;
  int32_t ret = -1;

  tSkipListNode *x = &pSkipList->pHead;

  pthread_rwlock_rdlock(&pSkipList->lock);
  pSkipList->state.queryCount++;

  __compar_fn_t filterComparator = getKeyFilterComparator(pSkipList, pKey->nType);

  for (int32_t i = sLevel; i >= 0; --i) {
    while (x->pForward[i] != NULL && (ret = filterComparator(&x->pForward[i]->key, pKey)) < 0) {
      x = x->pForward[i];
    }

    if (ret == 0) {
      pthread_rwlock_unlock(&pSkipList->lock);
      return x->pForward[i];
    }
  }

  pthread_rwlock_unlock(&pSkipList->lock);
  return NULL;
}

static int32_t tSkipListEndParQuery(tSkipList *pSkipList, tSkipListNode *pStartNode, tSkipListKey *pEndKey,
                                    int32_t cond, tSkipListNode ***pRes) {
  pthread_rwlock_rdlock(&pSkipList->lock);
  tSkipListNode *p = pStartNode;
  int32_t        numOfRes = 0;

  __compar_fn_t filterComparator = getKeyFilterComparator(pSkipList, pEndKey->nType);
  while (p != NULL) {
    int32_t ret = filterComparator(&p->key, pEndKey);
    if (ret > 0) {
      break;
    }

    if (ret < 0) {
      numOfRes++;
      p = p->pForward[0];
    } else if (ret == 0) {
      if (cond == TSDB_RELATION_LESS_EQUAL) {
        numOfRes++;
        p = p->pForward[0];
      } else {
        break;
      }
    }
  }

  (*pRes) = (tSkipListNode **)malloc(POINTER_BYTES * numOfRes);
  for (int32_t i = 0; i < numOfRes; ++i) {
    (*pRes)[i] = pStartNode;
    pStartNode = pStartNode->pForward[0];
  }
  pthread_rwlock_unlock(&pSkipList->lock);

  return numOfRes;
}

/*
 * maybe return the copy of tSkipListNode would be better
 */
int32_t tSkipListGets(tSkipList *pSkipList, tSkipListKey *pKey, tSkipListNode ***pRes) {
  (*pRes) = NULL;

  tSkipListNode *pNode = tSkipListGetOne(pSkipList, pKey);
  if (pNode == NULL) {
    return 0;
  }

  __compar_fn_t filterComparator = getKeyFilterComparator(pSkipList, pKey->nType);

  // backward check if previous nodes are with the same value.
  tSkipListNode *pPrev = pNode->pBackward[0];
  while ((pPrev != &pSkipList->pHead) && filterComparator(&pPrev->key, pKey) == 0) {
    pPrev = pPrev->pBackward[0];
  }

  return tSkipListEndParQuery(pSkipList, pPrev->pForward[0], &pNode->key, TSDB_RELATION_LESS_EQUAL, pRes);
}

static tSkipListNode *tSkipListParQuery(tSkipList *pSkipList, tSkipListKey *pKey, int32_t cond) {
  int32_t sLevel = pSkipList->nLevel - 1;
  int32_t ret = -1;

  tSkipListNode *x = &pSkipList->pHead;
  __compar_fn_t  filterComparator = getKeyFilterComparator(pSkipList, pKey->nType);

  pthread_rwlock_rdlock(&pSkipList->lock);

  if (cond == TSDB_RELATION_LARGE_EQUAL || cond == TSDB_RELATION_LARGE) {
    for (int32_t i = sLevel; i >= 0; --i) {
      while (x->pForward[i] != NULL && (ret = filterComparator(&x->pForward[i]->key, pKey)) < 0) {
        x = x->pForward[i];
      }
    }

    // backward check if previous nodes are with the same value.
    if (cond == TSDB_RELATION_LARGE_EQUAL && ret == 0) {
      tSkipListNode *pNode = x->pForward[0];
      while ((pNode->pBackward[0] != &pSkipList->pHead) && (filterComparator(&pNode->pBackward[0]->key, pKey) == 0)) {
        pNode = pNode->pBackward[0];
      }
      pthread_rwlock_unlock(&pSkipList->lock);
      return pNode;
    }

    if (ret > 0 || cond == TSDB_RELATION_LARGE_EQUAL) {
      pthread_rwlock_unlock(&pSkipList->lock);
      return x->pForward[0];
    } else {  // cond == TSDB_RELATION_LARGE && ret == 0
      tSkipListNode *pn = x->pForward[0];
      while (pn != NULL && filterComparator(&pn->key, pKey) == 0) {
        pn = pn->pForward[0];
      }
      pthread_rwlock_unlock(&pSkipList->lock);
      return pn;
    }
  }

  pthread_rwlock_unlock(&pSkipList->lock);
  return NULL;
}

int32_t tSkipListIterateList(tSkipList *pSkipList, tSkipListNode ***pRes, bool (*fp)(tSkipListNode *, void *),
                             void *param) {
  pthread_rwlock_rdlock(&pSkipList->lock);

  (*pRes) = (tSkipListNode **)malloc(POINTER_BYTES * pSkipList->nSize);
  tSkipListNode *pStartNode = pSkipList->pHead.pForward[0];
  int32_t        num = 0;
  for (int32_t i = 0; i < pSkipList->nSize; ++i) {
    if (pStartNode == NULL) {
      pError("error skiplist %p, required length:%d, actual length:%d", pSkipList, pSkipList->nSize, i - 1);
#ifdef _DEBUG_VIEW
      tSkipListPrint(pSkipList, 1);
#endif
      break;
    }

    if (fp == NULL || (fp != NULL && fp(pStartNode, param) == true)) {
      (*pRes)[num++] = pStartNode;
    }

    pStartNode = pStartNode->pForward[0];
  }
  pthread_rwlock_unlock(&pSkipList->lock);
  return num;
}

int32_t tSkipListRangeQuery(tSkipList *pSkipList, tSKipListQueryCond *pCond, tSkipListNode ***pRes) {
  pSkipList->state.queryCount++;
  tSkipListNode *pStart = tSkipListParQuery(pSkipList, &pCond->lowerBnd, pCond->lowerBndRelOptr);
  if (pStart == 0) {
    *pRes = NULL;
    return 0;
  }

  return tSkipListEndParQuery(pSkipList, pStart, &pCond->upperBnd, pCond->upperBndRelOptr, pRes);
}

static bool removeSupport(tSkipList *pSkipList, tSkipListNode **forward, tSkipListKey *pKey) {
  __compar_fn_t filterComparator = getKeyFilterComparator(pSkipList, pKey->nType);

  if (filterComparator(&forward[0]->pForward[0]->key, pKey) == 0) {
    tSkipListNode *p = forward[0]->pForward[0];
    doRemove(pSkipList, p, forward);
  } else {  // failed to find the node of specified value,abort
    return false;
  }

  // compress the minimum level of skip list
  while (pSkipList->nLevel > 0 && pSkipList->pHead.pForward[pSkipList->nLevel - 1] == NULL) {
    pSkipList->nLevel -= 1;
  }

  return true;
}

void tSkipListRemoveNode(tSkipList *pSkipList, tSkipListNode *pNode) {
  tSkipListNode *forward[MAX_SKIP_LIST_LEVEL] = {0};

  pthread_rwlock_rdlock(&pSkipList->lock);
  for (int32_t i = 0; i < pNode->nLevel; ++i) {
    forward[i] = pNode->pBackward[i];
  }

  removeSupport(pSkipList, forward, &pNode->key);
  pthread_rwlock_unlock(&pSkipList->lock);
}

bool tSkipListRemove(tSkipList *pSkipList, tSkipListKey *pKey) {
  tSkipListNode *forward[MAX_SKIP_LIST_LEVEL] = {0};
  __compar_fn_t  filterComparator = getKeyFilterComparator(pSkipList, pKey->nType);

  pthread_rwlock_rdlock(&pSkipList->lock);

  tSkipListNode *x = &pSkipList->pHead;
  for (int32_t i = pSkipList->nLevel - 1; i >= 0; --i) {
    while (x->pForward[i] != NULL && (filterComparator(&x->pForward[i]->key, pKey) < 0)) {
      x = x->pForward[i];
    }
    forward[i] = x;
  }

  bool ret = removeSupport(pSkipList, forward, pKey);
  pthread_rwlock_unlock(&pSkipList->lock);

  return ret;
}

void tSkipListPrint(tSkipList *pSkipList, int16_t nlevel) {
  if (pSkipList == NULL || pSkipList->nLevel < nlevel || nlevel <= 0) {
    return;
  }

  tSkipListNode *p = pSkipList->pHead.pForward[nlevel - 1];
  int32_t        id = 1;
  while (p) {
    switch (pSkipList->keyType) {
      case TSDB_DATA_TYPE_INT:
      case TSDB_DATA_TYPE_SMALLINT:
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_BIGINT:
        fprintf(stdout, "%d: %ld \n", id++, p->key.i64Key);
        break;
      case TSDB_DATA_TYPE_BINARY:
        fprintf(stdout, "%d: %s \n", id++, p->key.pz);
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        fprintf(stdout, "%d: %lf \n", id++, p->key.dKey);
        break;
      default:
        fprintf(stdout, "\n");
    }
    p = p->pForward[nlevel - 1];
  }
}

/*
 * query processor based on query condition
 */
int32_t tSkipListQuery(tSkipList *pSkipList, tSKipListQueryCond *pQueryCond, tSkipListNode ***pResult) {
  // query condition check
  int32_t       rel = 0;
  __compar_fn_t comparator = getKeyComparator(pQueryCond->lowerBnd.nType);

  if (pSkipList == NULL || pQueryCond == NULL || pSkipList->nSize == 0 ||
      (((rel = comparator(&pQueryCond->lowerBnd, &pQueryCond->upperBnd)) > 0 &&
        pQueryCond->lowerBnd.nType != TSDB_DATA_TYPE_NCHAR && pQueryCond->lowerBnd.nType != TSDB_DATA_TYPE_BINARY))) {
    (*pResult) = NULL;
    return 0;
  }

  if (rel == 0) {
    /*
     * 0 means: pQueryCond->lowerBnd == pQueryCond->upperBnd
     * point query
     */
    if (pQueryCond->lowerBndRelOptr == TSDB_RELATION_LARGE_EQUAL &&
        pQueryCond->upperBndRelOptr == TSDB_RELATION_LESS_EQUAL) {  // point query
      return tSkipListGets(pSkipList, &pQueryCond->lowerBnd, pResult);
    } else {
      (*pResult) = NULL;
      return 0;
    }
  } else {
    /* range query, query operation code check */
    return tSkipListRangeQuery(pSkipList, pQueryCond, pResult);
  }
}

typedef struct MultipleQueryResult {
  int32_t         len;
  tSkipListNode **pData;
} MultipleQueryResult;

static int32_t mergeQueryResult(MultipleQueryResult *pResults, int32_t numOfResSet, tSkipListNode ***pRes) {
  int32_t total = 0;
  for (int32_t i = 0; i < numOfResSet; ++i) {
    total += pResults[i].len;
  }

  (*pRes) = malloc(POINTER_BYTES * total);
  int32_t idx = 0;

  for (int32_t i = 0; i < numOfResSet; ++i) {
    MultipleQueryResult *pOneResult = &pResults[i];
    for (int32_t j = 0; j < pOneResult->len; ++j) {
      (*pRes)[idx++] = pOneResult->pData[j];
    }
  }

  return total;
}

static void removeDuplicateKey(tSkipListKey *pKey, int32_t *numOfKey, __compar_fn_t comparator) {
  if (*numOfKey == 1) {
    return;
  }

  qsort(pKey, *numOfKey, sizeof(pKey[0]), comparator);
  int32_t i = 0, j = 1;

  while (i < (*numOfKey) && j < (*numOfKey)) {
    int32_t ret = comparator(&pKey[i], &pKey[j]);
    if (ret == 0) {
      j++;
    } else {
      pKey[i + 1] = pKey[j];
      i++;
      j++;
    }
  }

  (*numOfKey) = i + 1;
}

int32_t mergeResult(const tSkipListKey *pKey, int32_t numOfKey, tSkipListNode ***pRes, __compar_fn_t comparator,
                    tSkipListNode *pNode) {
  int32_t i = 0, j = 0;
  // merge two sorted arrays in O(n) time
  while (i < numOfKey && pNode != NULL) {
    int32_t ret = comparator(&pNode->key, &pKey[i]);
    if (ret < 0) {
      (*pRes)[j++] = pNode;
      pNode = pNode->pForward[0];
    } else if (ret == 0) {
      pNode = pNode->pForward[0];
    } else {  // pNode->key > pkey[i]
      i++;
    }
  }

  while (pNode != NULL) {
    (*pRes)[j++] = pNode;
    pNode = pNode->pForward[0];
  }
  return j;
}

int32_t tSkipListPointQuery(tSkipList *pSkipList, tSkipListKey *pKey, int32_t numOfKey, tSkipListPointQueryType type,
                            tSkipListNode ***pRes) {
  if (numOfKey == 0 || pKey == NULL || pSkipList == NULL || pSkipList->nSize == 0 ||
      (type != INCLUDE_POINT_QUERY && type != EXCLUDE_POINT_QUERY)) {
    (*pRes) = NULL;
    return 0;
  }

  __compar_fn_t comparator = getKeyComparator(pKey->nType);
  removeDuplicateKey(pKey, &numOfKey, comparator);

  if (type == INCLUDE_POINT_QUERY) {
    if (numOfKey == 1) {
      return tSkipListGets(pSkipList, &pKey[0], pRes);
    } else {
      MultipleQueryResult *pTempResult = (MultipleQueryResult *)malloc(sizeof(MultipleQueryResult) * numOfKey);
      for (int32_t i = 0; i < numOfKey; ++i) {
        pTempResult[i].len = tSkipListGets(pSkipList, &pKey[i], &pTempResult[i].pData);
      }
      int32_t num = mergeQueryResult(pTempResult, numOfKey, pRes);

      for (int32_t i = 0; i < numOfKey; ++i) {
        free(pTempResult[i].pData);
      }
      free(pTempResult);
      return num;
    }
  } else {  // exclude query
    *pRes = malloc(POINTER_BYTES * pSkipList->nSize);

    __compar_fn_t filterComparator = getKeyFilterComparator(pSkipList, pKey->nType);

    tSkipListNode *pNode = pSkipList->pHead.pForward[0];
    int32_t        retLen = mergeResult(pKey, numOfKey, pRes, filterComparator, pNode);

    if (retLen < pSkipList->nSize) {
      (*pRes) = realloc(*pRes, POINTER_BYTES * retLen);
    }
    return retLen;
  }
}

int32_t tSkipListDefaultCompare(tSkipList *pSkipList, tSkipListKey *a, tSkipListKey *b) {
  switch (pSkipList->keyType) {
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_BOOL: {
      if (a->i64Key == b->i64Key) {
        return 0;
      } else {
        return a->i64Key > b->i64Key ? 1 : -1;
      }
    };
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE: {
      if (fabs(a->dKey - b->dKey) < FLT_EPSILON) {
        return 0;
      } else {
        return a->dKey > b->dKey ? 1 : -1;
      }
    };
    case TSDB_DATA_TYPE_BINARY: {
      if (a->nLen == b->nLen) {
        int32_t ret = strncmp(a->pz, b->pz, a->nLen);
        if (ret == 0) {
          return 0;
        } else {
          return ret > 0 ? 1 : -1;
        }
      } else {
        return a->nLen > b->nLen ? 1 : -1;
      }
    };
  }

  return 0;
}
