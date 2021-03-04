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
#include "taosmsg.h"
#include "hash.h"

#include "qExecutor.h"
#include "qUtil.h"
#include "tbuffer.h"
#include "tlosertree.h"
#include "queryLog.h"

typedef struct SCompSupporter {
  STableQueryInfo **pTableQueryInfo;
  int32_t          *rowIndex;
  int32_t           order;
} SCompSupporter;

int32_t getOutputInterResultBufSize(SQuery* pQuery) {
  int32_t size = 0;

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    size += pQuery->pExpr1[i].interBytes;
  }

  assert(size >= 0);
  return size;
}

int32_t initResultRowInfo(SResultRowInfo *pResultRowInfo, int32_t size, int16_t type) {
  pResultRowInfo->capacity = size;

  pResultRowInfo->type = type;
  pResultRowInfo->curIndex = -1;
  pResultRowInfo->size     = 0;
  pResultRowInfo->prevSKey = TSKEY_INITIAL_VAL;

  pResultRowInfo->pResult = calloc(pResultRowInfo->capacity, POINTER_BYTES);
  if (pResultRowInfo->pResult == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

void cleanupResultRowInfo(SResultRowInfo *pResultRowInfo) {
  if (pResultRowInfo == NULL) {
    return;
  }

  if (pResultRowInfo->capacity == 0) {
    assert(pResultRowInfo->pResult == NULL);
    return;
  }

  if (pResultRowInfo->type == TSDB_DATA_TYPE_BINARY || pResultRowInfo->type == TSDB_DATA_TYPE_NCHAR) {
    for(int32_t i = 0; i < pResultRowInfo->size; ++i) {
      tfree(pResultRowInfo->pResult[i]->key);
    }
  }
  
  tfree(pResultRowInfo->pResult);
}

void resetResultRowInfo(SQueryRuntimeEnv *pRuntimeEnv, SResultRowInfo *pResultRowInfo) {
  if (pResultRowInfo == NULL || pResultRowInfo->capacity == 0) {
    return;
  }

  for (int32_t i = 0; i < pResultRowInfo->size; ++i) {
    SResultRow *pWindowRes = pResultRowInfo->pResult[i];
    clearResultRow(pRuntimeEnv, pWindowRes, pResultRowInfo->type);

    int32_t groupIndex = 0;
    int64_t uid = 0;

    SET_RES_WINDOW_KEY(pRuntimeEnv->keyBuf, &groupIndex, sizeof(groupIndex), uid);
    taosHashRemove(pRuntimeEnv->pResultRowHashTable, (const char *)pRuntimeEnv->keyBuf, GET_RES_WINDOW_KEY_LEN(sizeof(groupIndex)));
  }
  
  pResultRowInfo->curIndex = -1;
  pResultRowInfo->size = 0;
  pResultRowInfo->prevSKey = TSKEY_INITIAL_VAL;
}

int32_t numOfClosedResultRows(SResultRowInfo *pResultRowInfo) {
  int32_t i = 0;
  while (i < pResultRowInfo->size && pResultRowInfo->pResult[i]->closed) {
    ++i;
  }
  
  return i;
}

void closeAllResultRows(SResultRowInfo *pResultRowInfo) {
  assert(pResultRowInfo->size >= 0 && pResultRowInfo->capacity >= pResultRowInfo->size);
  
  for (int32_t i = 0; i < pResultRowInfo->size; ++i) {
    SResultRow* pRow = pResultRowInfo->pResult[i];
    if (pRow->closed) {
      continue;
    }
    
    pRow->closed = true;
  }
}

bool isResultRowClosed(SResultRowInfo *pResultRowInfo, int32_t slot) {
  return (getResultRow(pResultRowInfo, slot)->closed == true);
}

void closeResultRow(SResultRowInfo *pResultRowInfo, int32_t slot) {
  getResultRow(pResultRowInfo, slot)->closed = true;
}

void clearResultRow(SQueryRuntimeEnv *pRuntimeEnv, SResultRow *pResultRow, int16_t type) {
  if (pResultRow == NULL) {
    return;
  }

  // the result does not put into the SDiskbasedResultBuf, ignore it.
  if (pResultRow->pageId >= 0) {
    tFilePage *page = getResBufPage(pRuntimeEnv->pResultBuf, pResultRow->pageId);

    int16_t offset = 0;
    for (int32_t i = 0; i < pRuntimeEnv->pQuery->numOfOutput; ++i) {
      SResultRowCellInfo *pResultInfo = &pResultRow->pCellInfo[i];

      size_t size = pRuntimeEnv->pQuery->pExpr1[i].bytes;
      char * s = getPosInResultPage(pRuntimeEnv->pQuery, page, pResultRow->offset, offset);
      memset(s, 0, size);

      offset += size;
      RESET_RESULT_INFO(pResultInfo);
    }
  }

  pResultRow->numOfRows = 0;
  pResultRow->pageId = -1;
  pResultRow->offset = -1;
  pResultRow->closed = false;

  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    tfree(pResultRow->key);
  } else {
    pResultRow->win = TSWINDOW_INITIALIZER;
  }
}

// TODO refactor: use macro
SResultRowCellInfo* getResultCell(const SResultRow* pRow, int32_t index, int32_t* offset) {
  assert(index >= 0 && offset != NULL);
  return (SResultRowCellInfo*)((char*) pRow->pCellInfo + offset[index]);
}

size_t getResultRowSize(SQueryRuntimeEnv* pRuntimeEnv) {
  SQuery* pQuery = pRuntimeEnv->pQuery;
  return (pQuery->numOfOutput * sizeof(SResultRowCellInfo)) + pQuery->interBufSize + sizeof(SResultRow);
}

SResultRowPool* initResultRowPool(size_t size) {
  SResultRowPool* p = calloc(1, sizeof(SResultRowPool));
  if (p == NULL) {
    return NULL;
  }

  p->numOfElemPerBlock = 128;

  p->elemSize = (int32_t) size;
  p->blockSize = p->numOfElemPerBlock * p->elemSize;
  p->position.pos = 0;

  p->pData = taosArrayInit(8, POINTER_BYTES);
  return p;
}

SResultRow* getNewResultRow(SResultRowPool* p) {
  if (p == NULL) {
    return NULL;
  }

  void* ptr = NULL;
  if (p->position.pos == 0) {
    ptr = calloc(1, p->blockSize);
    taosArrayPush(p->pData, &ptr);

  } else {
    size_t last = taosArrayGetSize(p->pData);

    void** pBlock = taosArrayGet(p->pData, last - 1);
    ptr = ((char*) (*pBlock)) + p->elemSize * p->position.pos;
  }

  p->position.pos = (p->position.pos + 1)%p->numOfElemPerBlock;
  initResultRow(ptr);

  return ptr;
}

int64_t getResultRowPoolMemSize(SResultRowPool* p) {
  if (p == NULL) {
    return 0;
  }

  return taosArrayGetSize(p->pData) * p->blockSize;
}

int32_t getNumOfAllocatedResultRows(SResultRowPool* p) {
  return (int32_t) taosArrayGetSize(p->pData) * p->numOfElemPerBlock;
}

int32_t getNumOfUsedResultRows(SResultRowPool* p) {
  return getNumOfAllocatedResultRows(p) - p->numOfElemPerBlock + p->position.pos;
}

void* destroyResultRowPool(SResultRowPool* p) {
  if (p == NULL) {
    return NULL;
  }

  size_t size = taosArrayGetSize(p->pData);
  for(int32_t i = 0; i < size; ++i) {
    void** ptr = taosArrayGet(p->pData, i);
    tfree(*ptr);
  }

  taosArrayDestroy(p->pData);

  tfree(p);
  return NULL;
}

void interResToBinary(SBufferWriter* bw, SArray* pRes, int32_t tagLen) {
  uint32_t numOfGroup = (uint32_t) taosArrayGetSize(pRes);
  tbufWriteUint32(bw, numOfGroup);
  tbufWriteUint16(bw, tagLen);

  for(int32_t i = 0; i < numOfGroup; ++i) {
    SInterResult* pOne = taosArrayGet(pRes, i);
    if (tagLen > 0) {
      tbufWriteBinary(bw, pOne->tags, tagLen);
    }

    uint32_t numOfCols = (uint32_t) taosArrayGetSize(pOne->pResult);
    tbufWriteUint32(bw, numOfCols);
    for(int32_t j = 0; j < numOfCols; ++j) {
      SStddevInterResult* p = taosArrayGet(pOne->pResult, j);
      uint32_t numOfRows = (uint32_t) taosArrayGetSize(p->pResult);

      tbufWriteUint16(bw, p->colId);
      tbufWriteUint32(bw, numOfRows);

      for(int32_t k = 0; k < numOfRows; ++k) {
        SResPair v = *(SResPair*) taosArrayGet(p->pResult, k);
        tbufWriteDouble(bw, v.avg);
        tbufWriteInt64(bw, v.key);
      }
    }
  }
}

SArray* interResFromBinary(const char* data, int32_t len) {
  SBufferReader br = tbufInitReader(data, len, false);
  uint32_t numOfGroup = tbufReadUint32(&br);
  uint16_t tagLen = tbufReadUint16(&br);

  char* tag = NULL;
  if (tagLen > 0) {
    tag = calloc(1, tagLen);
  }

  SArray* pResult = taosArrayInit(4, sizeof(SInterResult));

  for(int32_t i = 0; i < numOfGroup; ++i) {
    if (tagLen > 0) {
      memset(tag, 0, tagLen);
      tbufReadToBinary(&br, tag, tagLen);
    }

    uint32_t numOfCols = tbufReadUint32(&br);

    SArray* p = taosArrayInit(numOfCols, sizeof(SStddevInterResult));
    for(int32_t j = 0; j < numOfCols; ++j) {
      int16_t colId = tbufReadUint16(&br);
      int32_t numOfRows = tbufReadUint32(&br);

      SStddevInterResult interRes = {.colId = colId, .pResult = taosArrayInit(4, sizeof(struct SResPair)),};
      for(int32_t k = 0; k < numOfRows; ++k) {
        SResPair px = {0};
        px.avg = tbufReadDouble(&br);
        px.key = tbufReadInt64(&br);

        taosArrayPush(interRes.pResult, &px);
      }

      taosArrayPush(p, &interRes);
    }

    char* p1 = NULL;
    if (tagLen > 0) {
      p1 = malloc(tagLen);
      memcpy(p1, tag, tagLen);
    }

    SInterResult d = {.pResult = p, .tags = p1,};
    taosArrayPush(pResult, &d);
  }

  tfree(tag);
  return pResult;
}

void freeInterResult(void* param) {
  SInterResult* pResult = (SInterResult*) param;
  tfree(pResult->tags);

  int32_t numOfCols = (int32_t) taosArrayGetSize(pResult->pResult);
  for(int32_t i = 0; i < numOfCols; ++i) {
    SStddevInterResult *p = taosArrayGet(pResult->pResult, i);
    taosArrayDestroy(p->pResult);
  }

  taosArrayDestroy(pResult->pResult);
}

void cleanupGroupResInfo(SGroupResInfo* pGroupResInfo) {
  assert(pGroupResInfo != NULL);

  taosArrayDestroy(pGroupResInfo->pRows);
  pGroupResInfo->pRows     = NULL;
  pGroupResInfo->index     = 0;
}

void initGroupResInfo(SGroupResInfo* pGroupResInfo, SResultRowInfo* pResultInfo) {
  if (pGroupResInfo->pRows != NULL) {
    taosArrayDestroy(pGroupResInfo->pRows);
  }

  pGroupResInfo->pRows = taosArrayFromList(pResultInfo->pResult, pResultInfo->size, POINTER_BYTES);
  pGroupResInfo->index = 0;

  assert(pGroupResInfo->index <= getNumOfTotalRes(pGroupResInfo));
}

bool hasRemainDataInCurrentGroup(SGroupResInfo* pGroupResInfo) {
  if (pGroupResInfo->pRows == NULL) {
    return false;
  }

  return pGroupResInfo->index < taosArrayGetSize(pGroupResInfo->pRows);
}

bool hasRemainData(SGroupResInfo* pGroupResInfo) {
  if (hasRemainDataInCurrentGroup(pGroupResInfo)) {
    return true;
  }

  return pGroupResInfo->currentGroup < pGroupResInfo->totalGroup;
}

bool incNextGroup(SGroupResInfo* pGroupResInfo) {
  return (++pGroupResInfo->currentGroup) < pGroupResInfo->totalGroup;
}

int32_t getNumOfTotalRes(SGroupResInfo* pGroupResInfo) {
  assert(pGroupResInfo != NULL);
  if (pGroupResInfo->pRows == 0) {
    return 0;
  }

  return taosArrayGetSize(pGroupResInfo->pRows);
}

static int64_t getNumOfResultWindowRes(SQueryRuntimeEnv* pRuntimeEnv, SResultRow *pResultRow, int32_t* rowCellInfoOffset) {
  SQuery* pQuery = pRuntimeEnv->pQuery;

  for (int32_t j = 0; j < pQuery->numOfOutput; ++j) {
    int32_t functionId = pQuery->pExpr1[j].base.functionId;

    /*
     * ts, tag, tagprj function can not decide the output number of current query
     * the number of output result is decided by main output
     */
    if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TAGPRJ) {
      continue;
    }

    SResultRowCellInfo *pResultInfo = getResultCell(pResultRow, j, rowCellInfoOffset);
    assert(pResultInfo != NULL);

    if (pResultInfo->numOfRes > 0) {
      return pResultInfo->numOfRes;
    }
  }

  return 0;
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

  STableQueryInfo** pList = supporter->pTableQueryInfo;

  SResultRowInfo *pWindowResInfo1 = &(pList[left]->resInfo);
  SResultRow * pWindowRes1 = getResultRow(pWindowResInfo1, leftPos);
  TSKEY leftTimestamp = pWindowRes1->win.skey;

  SResultRowInfo *pWindowResInfo2 = &(pList[right]->resInfo);
  SResultRow * pWindowRes2 = getResultRow(pWindowResInfo2, rightPos);
  TSKEY rightTimestamp = pWindowRes2->win.skey;

  if (leftTimestamp == rightTimestamp) {
    return 0;
  }

  if (supporter->order == TSDB_ORDER_ASC) {
    return (leftTimestamp > rightTimestamp)? 1:-1;
  } else {
    return (leftTimestamp < rightTimestamp)? 1:-1;
  }
}

static int32_t mergeIntoGroupResultImpl(SQueryRuntimeEnv *pRuntimeEnv, SGroupResInfo* pGroupResInfo, SArray *pTableList,
    int32_t* rowCellInfoOffset) {
  bool ascQuery = QUERY_IS_ASC_QUERY(pRuntimeEnv->pQuery);

  int32_t code = TSDB_CODE_SUCCESS;

  int32_t *posList = NULL;
  SLoserTreeInfo *pTree = NULL;
  STableQueryInfo **pTableQueryInfoList = NULL;

  size_t size = taosArrayGetSize(pTableList);
  if (pGroupResInfo->pRows == NULL) {
    pGroupResInfo->pRows = taosArrayInit(100, POINTER_BYTES);
  }

  posList = calloc(size, sizeof(int32_t));
  pTableQueryInfoList = malloc(POINTER_BYTES * size);

  if (pTableQueryInfoList == NULL || posList == NULL || pGroupResInfo->pRows == NULL || pGroupResInfo->pRows == NULL) {
    qError("QInfo:%p failed alloc memory", pRuntimeEnv->qinfo);
    code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    goto _end;
  }

  int32_t numOfTables = 0;
  for (int32_t i = 0; i < size; ++i) {
    STableQueryInfo *item = taosArrayGetP(pTableList, i);
    if (item->resInfo.size > 0) {
      pTableQueryInfoList[numOfTables++] = item;
    }
  }

  // there is no data in current group
  // no need to merge results since only one table in each group
  if (numOfTables == 0) {
    goto _end;
  }

  SCompSupporter cs = {pTableQueryInfoList, posList, pRuntimeEnv->pQuery->order.order};

  int32_t ret = tLoserTreeCreate(&pTree, numOfTables, &cs, tableResultComparFn);
  if (ret != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    goto _end;
  }

  int64_t lastTimestamp = ascQuery? INT64_MIN:INT64_MAX;
  int64_t startt = taosGetTimestampMs();

  while (1) {
    int32_t tableIndex = pTree->pNode[0].index;

    SResultRowInfo *pWindowResInfo = &pTableQueryInfoList[tableIndex]->resInfo;
    SResultRow  *pWindowRes = getResultRow(pWindowResInfo, cs.rowIndex[tableIndex]);

    int64_t num = getNumOfResultWindowRes(pRuntimeEnv, pWindowRes, rowCellInfoOffset);
    if (num <= 0) {
      cs.rowIndex[tableIndex] += 1;

      if (cs.rowIndex[tableIndex] >= pWindowResInfo->size) {
        cs.rowIndex[tableIndex] = -1;
        if (--numOfTables == 0) { // all input sources are exhausted
          break;
        }
      }
    } else {
      assert((pWindowRes->win.skey >= lastTimestamp && ascQuery) || (pWindowRes->win.skey <= lastTimestamp && !ascQuery));

      if (pWindowRes->win.skey != lastTimestamp) {
        taosArrayPush(pGroupResInfo->pRows, &pWindowRes);
        pWindowRes->numOfRows = (uint32_t) num;
      }

      lastTimestamp = pWindowRes->win.skey;

      // move to the next row of current entry
      if ((++cs.rowIndex[tableIndex]) >= pWindowResInfo->size) {
        cs.rowIndex[tableIndex] = -1;

        // all input sources are exhausted
        if ((--numOfTables) == 0) {
          break;
        }
      }
    }

    tLoserTreeAdjust(pTree, tableIndex + pTree->numOfEntries);
  }

  int64_t endt = taosGetTimestampMs();

  qDebug("QInfo:%p result merge completed for group:%d, elapsed time:%" PRId64 " ms", pRuntimeEnv->qinfo,
         pGroupResInfo->currentGroup, endt - startt);

  _end:
  tfree(pTableQueryInfoList);
  tfree(posList);
  tfree(pTree);

  return code;
}

int32_t mergeIntoGroupResult(SGroupResInfo* pGroupResInfo, SQueryRuntimeEnv* pRuntimeEnv, int32_t* offset) {
  int64_t st = taosGetTimestampUs();

  while (pGroupResInfo->currentGroup < pGroupResInfo->totalGroup) {
    SArray *group = GET_TABLEGROUP(pRuntimeEnv, pGroupResInfo->currentGroup);

    int32_t ret = mergeIntoGroupResultImpl(pRuntimeEnv, pGroupResInfo, group, offset);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    // this group generates at least one result, return results
    if (taosArrayGetSize(pGroupResInfo->pRows) > 0) {
      break;
    }

    qDebug("QInfo:%p no result in group %d, continue", pRuntimeEnv->qinfo, pGroupResInfo->currentGroup);
    cleanupGroupResInfo(pGroupResInfo);
    incNextGroup(pGroupResInfo);
  }

//  if (pGroupResInfo->currentGroup >= pGroupResInfo->totalGroup && !hasRemainData(pGroupResInfo)) {
//    SET_STABLE_QUERY_OVER(pRuntimeEnv);
//  }

  int64_t elapsedTime = taosGetTimestampUs() - st;
  qDebug("QInfo:%p merge res data into group, index:%d, total group:%d, elapsed time:%" PRId64 "us", pRuntimeEnv->qinfo,
         pGroupResInfo->currentGroup, pGroupResInfo->totalGroup, elapsedTime);

//  pQInfo->summary.firstStageMergeTime += elapsedTime;
  return TSDB_CODE_SUCCESS;
}
