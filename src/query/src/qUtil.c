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

static int32_t getResultRowKeyInfo(SResultRow* pResult, int16_t type, char** key, int16_t* bytes) {
  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    *key   = varDataVal(pResult->key);
    *bytes = varDataLen(pResult->key);
  } else {
    *key = (char*) &pResult->win.skey;
    *bytes = tDataTypeDesc[type].nSize;
  }

  return 0;
}

int32_t getOutputInterResultBufSize(SQuery* pQuery) {
  int32_t size = 0;

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    size += pQuery->pExpr1[i].interBytes;
  }

  assert(size >= 0);
  return size;
}

int32_t initWindowResInfo(SResultRowInfo *pResultRowInfo, int32_t size, int16_t type) {
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

void cleanupTimeWindowInfo(SResultRowInfo *pResultRowInfo) {
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

void resetTimeWindowInfo(SQueryRuntimeEnv *pRuntimeEnv, SResultRowInfo *pResultRowInfo) {
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
  
  pResultRowInfo->startTime = TSKEY_INITIAL_VAL;
  pResultRowInfo->prevSKey = TSKEY_INITIAL_VAL;
}

void clearFirstNWindowRes(SQueryRuntimeEnv *pRuntimeEnv, int32_t num) {
  SResultRowInfo *pResultRowInfo = &pRuntimeEnv->windowResInfo;
  if (pResultRowInfo == NULL || pResultRowInfo->capacity == 0 || pResultRowInfo->size == 0 || num == 0) {
    return;
  }
  
  int32_t numOfClosed = numOfClosedTimeWindow(pResultRowInfo);
  assert(num >= 0 && num <= numOfClosed);

  int16_t type = pResultRowInfo->type;
  int64_t uid = getResultInfoUId(pRuntimeEnv);

  char    *key  = NULL;
  int16_t  bytes = -1;

  for (int32_t i = 0; i < num; ++i) {
    SResultRow *pResult = pResultRowInfo->pResult[i];
    if (pResult->closed) {  // remove the window slot from hash table
      getResultRowKeyInfo(pResult, type, &key, &bytes);
      SET_RES_WINDOW_KEY(pRuntimeEnv->keyBuf, key, bytes, uid);
      taosHashRemove(pRuntimeEnv->pResultRowHashTable, (const char *)pRuntimeEnv->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes));
    } else {
      break;
    }
  }
  
  int32_t remain = pResultRowInfo->size - num;
  
  // clear all the closed windows from the window list
  for (int32_t k = 0; k < remain; ++k) {
    copyResultRow(pRuntimeEnv, pResultRowInfo->pResult[k], pResultRowInfo->pResult[num + k], type);
  }
  
  // move the unclosed window in the front of the window list
  for (int32_t k = remain; k < pResultRowInfo->size; ++k) {
    SResultRow *pWindowRes = pResultRowInfo->pResult[k];
    clearResultRow(pRuntimeEnv, pWindowRes, pResultRowInfo->type);
  }
  
  pResultRowInfo->size = remain;

  for (int32_t k = 0; k < pResultRowInfo->size; ++k) {
    SResultRow *pResult = pResultRowInfo->pResult[k];
    getResultRowKeyInfo(pResult, type, &key, &bytes);
    SET_RES_WINDOW_KEY(pRuntimeEnv->keyBuf, key, bytes, uid);

    int32_t *p = (int32_t *)taosHashGet(pRuntimeEnv->pResultRowHashTable, (const char *)pRuntimeEnv->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes));
    assert(p != NULL); 

    int32_t  v = (*p - num);
    assert(v >= 0 && v <= pResultRowInfo->size);

    SET_RES_WINDOW_KEY(pRuntimeEnv->keyBuf, key, bytes, uid);
    taosHashPut(pRuntimeEnv->pResultRowHashTable, pRuntimeEnv->keyBuf, GET_RES_WINDOW_KEY_LEN(bytes), (char *)&v, sizeof(int32_t));
  }
  
  pResultRowInfo->curIndex = -1;
}

void clearClosedTimeWindow(SQueryRuntimeEnv *pRuntimeEnv) {
  SResultRowInfo *pResultRowInfo = &pRuntimeEnv->windowResInfo;
  if (pResultRowInfo == NULL || pResultRowInfo->capacity == 0 || pResultRowInfo->size == 0) {
    return;
  }
  
  int32_t numOfClosed = numOfClosedTimeWindow(pResultRowInfo);
  clearFirstNWindowRes(pRuntimeEnv, numOfClosed);
}

int32_t numOfClosedTimeWindow(SResultRowInfo *pResultRowInfo) {
  int32_t i = 0;
  while (i < pResultRowInfo->size && pResultRowInfo->pResult[i]->closed) {
    ++i;
  }
  
  return i;
}

void closeAllTimeWindow(SResultRowInfo *pResultRowInfo) {
  assert(pResultRowInfo->size >= 0 && pResultRowInfo->capacity >= pResultRowInfo->size);
  
  for (int32_t i = 0; i < pResultRowInfo->size; ++i) {
    if (pResultRowInfo->pResult[i]->closed) {
      continue;
    }
    
    pResultRowInfo->pResult[i]->closed = true;
  }
}

/*
 * remove the results that are not the FIRST time window that spreads beyond the
 * the last qualified time stamp in case of sliding query, which the sliding time is not equalled to the interval time.
 * NOTE: remove redundant, only when the result set order equals to traverse order
 */
void removeRedundantWindow(SResultRowInfo *pResultRowInfo, TSKEY lastKey, int32_t order) {
  assert(pResultRowInfo->size >= 0 && pResultRowInfo->capacity >= pResultRowInfo->size);
  if (pResultRowInfo->size <= 1) {
    return;
  }

  // get the result order
  int32_t resultOrder = (pResultRowInfo->pResult[0]->win.skey < pResultRowInfo->pResult[1]->win.skey)? 1:-1;
  if (order != resultOrder) {
    return;
  }

  int32_t i = 0;
  if (order == QUERY_ASC_FORWARD_STEP) {
    TSKEY ekey = pResultRowInfo->pResult[i]->win.ekey;
    while (i < pResultRowInfo->size && (ekey < lastKey)) {
      ++i;
    }
  } else if (order == QUERY_DESC_FORWARD_STEP) {
    while (i < pResultRowInfo->size && (pResultRowInfo->pResult[i]->win.skey > lastKey)) {
      ++i;
    }
  }

  if (i < pResultRowInfo->size) {
    pResultRowInfo->size = (i + 1);
  }
}

bool isWindowResClosed(SResultRowInfo *pResultRowInfo, int32_t slot) {
  return (getResultRow(pResultRowInfo, slot)->closed == true);
}

void closeTimeWindow(SResultRowInfo *pResultRowInfo, int32_t slot) {
  getResultRow(pResultRowInfo, slot)->closed = true;
}

void clearResultRow(SQueryRuntimeEnv *pRuntimeEnv, SResultRow *pWindowRes, int16_t type) {
  if (pWindowRes == NULL) {
    return;
  }

  // the result does not put into the SDiskbasedResultBuf, ignore it.
  if (pWindowRes->pageId >= 0) {
    tFilePage *page = getResBufPage(pRuntimeEnv->pResultBuf, pWindowRes->pageId);

    for (int32_t i = 0; i < pRuntimeEnv->pQuery->numOfOutput; ++i) {
      SResultRowCellInfo *pResultInfo = &pWindowRes->pCellInfo[i];

      char * s = getPosInResultPage(pRuntimeEnv, i, pWindowRes, page);
      size_t size = pRuntimeEnv->pQuery->pExpr1[i].bytes;
      memset(s, 0, size);

      RESET_RESULT_INFO(pResultInfo);
    }
  }

  pWindowRes->numOfRows = 0;
  pWindowRes->pageId = -1;
  pWindowRes->rowId = -1;
  pWindowRes->closed = false;

  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    tfree(pWindowRes->key);
  } else {
    pWindowRes->win = TSWINDOW_INITIALIZER;
  }
}

/**
 * The source window result pos attribution of the source window result does not assign to the destination,
 * since the attribute of "Pos" is bound to each window result when the window result is created in the
 * disk-based result buffer.
 */
void copyResultRow(SQueryRuntimeEnv *pRuntimeEnv, SResultRow *dst, const SResultRow *src, int16_t type) {
  dst->numOfRows = src->numOfRows;

  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    dst->key = realloc(dst->key, varDataTLen(src->key));
    varDataCopy(dst->key, src->key);
  } else {
    dst->win   = src->win;
  }
  dst->closed = src->closed;
  
  int32_t nOutputCols = pRuntimeEnv->pQuery->numOfOutput;
  
  for (int32_t i = 0; i < nOutputCols; ++i) {
    SResultRowCellInfo *pDst = getResultCell(pRuntimeEnv, dst, i);
    SResultRowCellInfo *pSrc = getResultCell(pRuntimeEnv, src, i);
    
//    char *buf = pDst->interResultBuf;
    memcpy(pDst, pSrc, sizeof(SResultRowCellInfo) + pRuntimeEnv->pCtx[i].interBufBytes);
//    pDst->interResultBuf = buf;  // restore the allocated buffer
    
    // copy the result info struct
//    memcpy(pDst->interResultBuf, pSrc->interResultBuf, pRuntimeEnv->pCtx[i].interBufBytes);
    
    // copy the output buffer data from src to dst, the position info keep unchanged
    tFilePage *dstpage = getResBufPage(pRuntimeEnv->pResultBuf, dst->pageId);
    char * dstBuf = getPosInResultPage(pRuntimeEnv, i, dst, dstpage);

    tFilePage *srcpage = getResBufPage(pRuntimeEnv->pResultBuf, src->pageId);
    char * srcBuf = getPosInResultPage(pRuntimeEnv, i, (SResultRow *)src, srcpage);
    size_t s = pRuntimeEnv->pQuery->pExpr1[i].bytes;
    
    memcpy(dstBuf, srcBuf, s);
  }
}

SResultRowCellInfo* getResultCell(SQueryRuntimeEnv* pRuntimeEnv, const SResultRow* pRow, int32_t index) {
  assert(index >= 0 && index < pRuntimeEnv->pQuery->numOfOutput);
  return (SResultRowCellInfo*)((char*) pRow->pCellInfo + pRuntimeEnv->rowCellInfoOffset[index]);
}

size_t getWindowResultSize(SQueryRuntimeEnv* pRuntimeEnv) {
  return (pRuntimeEnv->pQuery->numOfOutput * sizeof(SResultRowCellInfo)) + pRuntimeEnv->interBufSize + sizeof(SResultRow);
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

uint64_t getResultInfoUId(SQueryRuntimeEnv* pRuntimeEnv) {
  if (!pRuntimeEnv->stableQuery) {
    return 0;  // for simple table query, the uid is always set to be 0;
  }

  SQuery* pQuery = pRuntimeEnv->pQuery;
  if (pQuery->interval.interval == 0 || isPointInterpoQuery(pQuery) || pRuntimeEnv->groupbyNormalCol) {
    return 0;
  }

  STableId* id = TSDB_TABLEID(pRuntimeEnv->pQuery->current->pTable);
  return id->uid;
}