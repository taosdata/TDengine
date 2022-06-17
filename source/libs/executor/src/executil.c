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
#include "tmsg.h"
#include "thash.h"

#include "executil.h"
#include "executorimpl.h"
#include "tcompression.h"
#include "tlosertree.h"

typedef struct SCompSupporter {
  STableQueryInfo **pTableQueryInfo;
  int32_t          *rowIndex;
  int32_t           order;
} SCompSupporter;

int32_t initResultRowInfo(SResultRowInfo *pResultRowInfo, int32_t size) {
  pResultRowInfo->size       = 0;
  pResultRowInfo->cur.pageId = -1;
  return TSDB_CODE_SUCCESS;
}

void cleanupResultRowInfo(SResultRowInfo *pResultRowInfo) {
  if (pResultRowInfo == NULL) {
    return;
  }

  for(int32_t i = 0; i < pResultRowInfo->size; ++i) {
//    if (pResultRowInfo->pResult[i]) {
//      taosMemoryFreeClear(pResultRowInfo->pResult[i]->key);
//    }
  }
}

void resetResultRowInfo(STaskRuntimeEnv *pRuntimeEnv, SResultRowInfo *pResultRowInfo) {
  for (int32_t i = 0; i < pResultRowInfo->size; ++i) {
//    SResultRow *pWindowRes = pResultRowInfo->pResult[i];
//    clearResultRow(pRuntimeEnv, pWindowRes);

    int32_t groupIndex = 0;
    int64_t uid = 0;

    SET_RES_WINDOW_KEY(pRuntimeEnv->keyBuf, &groupIndex, sizeof(groupIndex), uid);
    taosHashRemove(pRuntimeEnv->pResultRowHashTable, (const char *)pRuntimeEnv->keyBuf, GET_RES_WINDOW_KEY_LEN(sizeof(groupIndex)));
  }

  pResultRowInfo->size     = 0;
}

void closeAllResultRows(SResultRowInfo *pResultRowInfo) {
// do nothing
}

bool isResultRowClosed(SResultRow* pRow) {
  return (pRow->closed == true);
}

void closeResultRow(SResultRow* pResultRow) {
  pResultRow->closed = true;
}

// TODO refactor: use macro
SResultRowEntryInfo* getResultCell(const SResultRow* pRow, int32_t index, const int32_t* offset) {
  assert(index >= 0 && offset != NULL);
  return (SResultRowEntryInfo*)((char*) pRow->pEntryInfo + offset[index]);
}

size_t getResultRowSize(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  int32_t rowSize = (numOfOutput * sizeof(SResultRowEntryInfo)) + sizeof(SResultRow);

  for(int32_t i = 0; i < numOfOutput; ++i) {
    rowSize += pCtx[i].resDataInfo.interBufSize;
  }

  return rowSize;
}

void cleanupGroupResInfo(SGroupResInfo* pGroupResInfo) {
  assert(pGroupResInfo != NULL);

  taosArrayDestroy(pGroupResInfo->pRows);
  pGroupResInfo->pRows     = NULL;
  pGroupResInfo->index     = 0;
}

static int32_t resultrowComparAsc(const void* p1, const void* p2) {
  SResKeyPos* pp1 = *(SResKeyPos**) p1;
  SResKeyPos* pp2 = *(SResKeyPos**) p2;

  if (pp1->groupId == pp2->groupId) {
    int64_t pts1 = *(int64_t*) pp1->key;
    int64_t pts2 = *(int64_t*) pp2->key;

    if (pts1 == pts2) {
      return 0;
    } else {
      return pts1 < pts2? -1:1;
    }
  } else {
    return pp1->groupId < pp2->groupId? -1:1;
  }
}

static int32_t resultrowComparDesc(const void* p1, const void* p2) {
  return resultrowComparAsc(p2, p1);
}

void initGroupedResultInfo(SGroupResInfo* pGroupResInfo, SHashObj* pHashmap, int32_t order) {
  if (pGroupResInfo->pRows != NULL) {
    taosArrayDestroy(pGroupResInfo->pRows);
  }

  // extract the result rows information from the hash map
  void* pData = NULL;
  pGroupResInfo->pRows = taosArrayInit(10, POINTER_BYTES);

  size_t keyLen = 0;
  while((pData = taosHashIterate(pHashmap, pData)) != NULL) {
    void* key = taosHashGetKey(pData, &keyLen);

    SResKeyPos* p = taosMemoryMalloc(keyLen + sizeof(SResultRowPosition));

    p->groupId = *(uint64_t*) key;
    p->pos = *(SResultRowPosition*) pData;
    memcpy(p->key, (char*)key + sizeof(uint64_t), keyLen - sizeof(uint64_t));

    taosArrayPush(pGroupResInfo->pRows, &p);
  }

  if (order == TSDB_ORDER_ASC || order == TSDB_ORDER_DESC) {
    __compar_fn_t fn = (order == TSDB_ORDER_ASC)? resultrowComparAsc:resultrowComparDesc;
    qsort(pGroupResInfo->pRows->pData, taosArrayGetSize(pGroupResInfo->pRows), POINTER_BYTES, fn);
  }

  pGroupResInfo->index = 0;
  assert(pGroupResInfo->index <= getNumOfTotalRes(pGroupResInfo));
}

void initMultiResInfoFromArrayList(SGroupResInfo* pGroupResInfo, SArray* pArrayList) {
  if (pGroupResInfo->pRows != NULL) {
    taosArrayDestroyP(pGroupResInfo->pRows, taosMemoryFree);
  }

  pGroupResInfo->pRows = pArrayList;
  pGroupResInfo->index = 0;
  ASSERT(pGroupResInfo->index <= getNumOfTotalRes(pGroupResInfo));
}

bool hashRemainDataInGroupInfo(SGroupResInfo* pGroupResInfo) {
  if (pGroupResInfo->pRows == NULL) {
    return false;
  }

  return pGroupResInfo->index < taosArrayGetSize(pGroupResInfo->pRows);
}

int32_t getNumOfTotalRes(SGroupResInfo* pGroupResInfo) {
  assert(pGroupResInfo != NULL);
  if (pGroupResInfo->pRows == 0) {
    return 0;
  }

  return (int32_t) taosArrayGetSize(pGroupResInfo->pRows);
}

static int32_t tableResultComparFn(const void *pLeft, const void *pRight, void *param) {
  int32_t left  = *(int32_t *)pLeft;
  int32_t right = *(int32_t *)pRight;

  SCompSupporter *  supporter = (SCompSupporter *)param;

  int32_t leftPos  = supporter->rowIndex[left];
  int32_t rightPos = supporter->rowIndex[right];

  /* left source is exhausted */
  if (leftPos == -1) {
    return 1;
  }

  /* right source is exhausted*/
  if (rightPos == -1) {
    return -1;
  }

  ASSERT(0);
  STableQueryInfo** pList = supporter->pTableQueryInfo;
//  SResultRow* pWindowRes1 = pList[left]->resInfo.pResult[leftPos];
//  SResultRow * pWindowRes1 = getResultRow(&(pList[left]->resInfo), leftPos);
//  TSKEY leftTimestamp = pWindowRes1->win.skey;

//  SResultRowInfo *pWindowResInfo2 = &(pList[right]->resInfo);
//  SResultRow * pWindowRes2 = getResultRow(pWindowResInfo2, rightPos);
//  SResultRow* pWindowRes2 = pList[right]->resInfo.pResult[rightPos];
//  TSKEY rightTimestamp = pWindowRes2->win.skey;

//  if (leftTimestamp == rightTimestamp) {
    return 0;
//  }

//  if (supporter->order == TSDB_ORDER_ASC) {
//    return (leftTimestamp > rightTimestamp)? 1:-1;
//  } else {
//    return (leftTimestamp < rightTimestamp)? 1:-1;
//  }
}

int32_t tsAscOrder(const void* p1, const void* p2) {
  SResultRowCell* pc1 = (SResultRowCell*) p1;
  SResultRowCell* pc2 = (SResultRowCell*) p2;

  if (pc1->groupId == pc2->groupId) {
    ASSERT(0);
//    if (pc1->pRow->win.skey == pc2->pRow->win.skey) {
//      return 0;
//    } else {
//      return (pc1->pRow->win.skey < pc2->pRow->win.skey)? -1:1;
//    }
  } else {
    return (pc1->groupId < pc2->groupId)? -1:1;
  }
}

int32_t tsDescOrder(const void* p1, const void* p2) {
  SResultRowCell* pc1 = (SResultRowCell*) p1;
  SResultRowCell* pc2 = (SResultRowCell*) p2;

  if (pc1->groupId == pc2->groupId) {
    ASSERT(0);
//    if (pc1->pRow->win.skey == pc2->pRow->win.skey) {
//      return 0;
//    } else {
//      return (pc1->pRow->win.skey < pc2->pRow->win.skey)? 1:-1;
//    }
  } else {
    return (pc1->groupId < pc2->groupId)? -1:1;
  }
}

void orderTheResultRows(STaskRuntimeEnv* pRuntimeEnv) {
  __compar_fn_t  fn = NULL;
//  if (pRuntimeEnv->pQueryAttr->order.order == TSDB_ORDER_ASC) {
//    fn = tsAscOrder;
//  } else {
//    fn = tsDescOrder;
//  }

  taosArraySort(pRuntimeEnv->pResultRowArrayList, fn);
}
