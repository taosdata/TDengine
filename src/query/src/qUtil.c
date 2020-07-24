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

#include "hash.h"
#include "taosmsg.h"
#include "ttime.h"

#include "qExecutor.h"
#include "qUtil.h"

int32_t getOutputInterResultBufSize(SQuery* pQuery) {
  int32_t size = 0;

  for (int32_t i = 0; i < pQuery->numOfOutput; ++i) {
    size += pQuery->pSelectExpr[i].interBytes;
  }

  assert(size > 0);
  return size;
}

int32_t initWindowResInfo(SWindowResInfo *pWindowResInfo, SQueryRuntimeEnv *pRuntimeEnv, int32_t size,
                          int32_t threshold, int16_t type) {
  pWindowResInfo->capacity = size;
  pWindowResInfo->threshold = threshold;
  
  pWindowResInfo->type = type;
  _hash_fn_t fn = taosGetDefaultHashFunction(type);
  pWindowResInfo->hashList = taosHashInit(threshold, fn, false);
  if (pWindowResInfo->hashList == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }
  
  pWindowResInfo->curIndex = -1;
  pWindowResInfo->size     = 0;
  pWindowResInfo->prevSKey = TSKEY_INITIAL_VAL;
  
  // use the pointer arraylist
  pWindowResInfo->pResult = calloc(threshold, sizeof(SWindowResult));
  if (pWindowResInfo->pResult == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < pWindowResInfo->capacity; ++i) {
    int32_t code = createQueryResultInfo(pRuntimeEnv->pQuery, &pWindowResInfo->pResult[i], pRuntimeEnv->stableQuery, pRuntimeEnv->interBufSize);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  
  return TSDB_CODE_SUCCESS;
}

void destroyTimeWindowRes(SWindowResult *pWindowRes) {
  if (pWindowRes == NULL) {
    return;
  }

  free(pWindowRes->resultInfo);
}

void cleanupTimeWindowInfo(SWindowResInfo *pWindowResInfo) {
  if (pWindowResInfo == NULL) {
    return;
  }
  if (pWindowResInfo->capacity == 0) {
    assert(pWindowResInfo->hashList == NULL && pWindowResInfo->pResult == NULL);
    return;
  }
  
  if (pWindowResInfo->pResult != NULL) {
    for (int32_t i = 0; i < pWindowResInfo->capacity; ++i) {
      destroyTimeWindowRes(&pWindowResInfo->pResult[i]);
    }
  }
  
  taosHashCleanup(pWindowResInfo->hashList);
  tfree(pWindowResInfo->pResult);
}

void resetTimeWindowInfo(SQueryRuntimeEnv *pRuntimeEnv, SWindowResInfo *pWindowResInfo) {
  if (pWindowResInfo == NULL || pWindowResInfo->capacity == 0) {
    return;
  }
  
  for (int32_t i = 0; i < pWindowResInfo->size; ++i) {
    SWindowResult *pWindowRes = &pWindowResInfo->pResult[i];
    clearTimeWindowResBuf(pRuntimeEnv, pWindowRes);
  }
  
  pWindowResInfo->curIndex = -1;
  taosHashCleanup(pWindowResInfo->hashList);
  pWindowResInfo->size = 0;
  
  _hash_fn_t fn = taosGetDefaultHashFunction(pWindowResInfo->type);
  pWindowResInfo->hashList = taosHashInit(pWindowResInfo->capacity, fn, false);
  
  pWindowResInfo->startTime = TSKEY_INITIAL_VAL;
  pWindowResInfo->prevSKey = TSKEY_INITIAL_VAL;
}

void clearFirstNTimeWindow(SQueryRuntimeEnv *pRuntimeEnv, int32_t num) {
  SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;
  if (pWindowResInfo == NULL || pWindowResInfo->capacity == 0 || pWindowResInfo->size == 0 || num == 0) {
    return;
  }
  
  int32_t numOfClosed = numOfClosedTimeWindow(pWindowResInfo);
  assert(num >= 0 && num <= numOfClosed);
  
  for (int32_t i = 0; i < num; ++i) {
    SWindowResult *pResult = &pWindowResInfo->pResult[i];
    if (pResult->status.closed) {  // remove the window slot from hash table
      taosHashRemove(pWindowResInfo->hashList, (const char *)&pResult->window.skey, pWindowResInfo->type);
    } else {
      break;
    }
  }
  
  int32_t remain = pWindowResInfo->size - num;
  
  // clear all the closed windows from the window list
  for (int32_t k = 0; k < remain; ++k) {
    copyTimeWindowResBuf(pRuntimeEnv, &pWindowResInfo->pResult[k], &pWindowResInfo->pResult[num + k]);
  }
  
  // move the unclosed window in the front of the window list
  for (int32_t k = remain; k < pWindowResInfo->size; ++k) {
    SWindowResult *pWindowRes = &pWindowResInfo->pResult[k];
    clearTimeWindowResBuf(pRuntimeEnv, pWindowRes);
  }
  
  pWindowResInfo->size = remain;
  for (int32_t k = 0; k < pWindowResInfo->size; ++k) {
    SWindowResult *pResult = &pWindowResInfo->pResult[k];
    int32_t *p = (int32_t *)taosHashGet(pWindowResInfo->hashList, (const char *)&pResult->window.skey,
        tDataTypeDesc[pWindowResInfo->type].nSize);
    assert(p != NULL); 
    int32_t  v = (*p - num);
    assert(v >= 0 && v <= pWindowResInfo->size);
    taosHashPut(pWindowResInfo->hashList, (char *)&pResult->window.skey, tDataTypeDesc[pWindowResInfo->type].nSize,
        (char *)&v, sizeof(int32_t));
  }
  
  pWindowResInfo->curIndex = -1;
}

void clearClosedTimeWindow(SQueryRuntimeEnv *pRuntimeEnv) {
  SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;
  if (pWindowResInfo == NULL || pWindowResInfo->capacity == 0 || pWindowResInfo->size == 0) {
    return;
  }
  
  int32_t numOfClosed = numOfClosedTimeWindow(pWindowResInfo);
  clearFirstNTimeWindow(pRuntimeEnv, numOfClosed);
}

int32_t numOfClosedTimeWindow(SWindowResInfo *pWindowResInfo) {
  int32_t i = 0;
  while (i < pWindowResInfo->size && pWindowResInfo->pResult[i].status.closed) {
    ++i;
  }
  
  return i;
}

void closeAllTimeWindow(SWindowResInfo *pWindowResInfo) {
  assert(pWindowResInfo->size >= 0 && pWindowResInfo->capacity >= pWindowResInfo->size);
  
  for (int32_t i = 0; i < pWindowResInfo->size; ++i) {
    if (pWindowResInfo->pResult[i].status.closed) {
      continue;
    }
    
    pWindowResInfo->pResult[i].status.closed = true;
  }
}

/*
 * remove the results that are not the FIRST time window that spreads beyond the
 * the last qualified time stamp in case of sliding query, which the sliding time is not equalled to the interval time.
 * NOTE: remove redundant, only when the result set order equals to traverse order
 */
void removeRedundantWindow(SWindowResInfo *pWindowResInfo, TSKEY lastKey, int32_t order) {
  assert(pWindowResInfo->size >= 0 && pWindowResInfo->capacity >= pWindowResInfo->size);
  if (pWindowResInfo->size <= 1) {
    return;
  }

  // get the result order
  int32_t resultOrder = (pWindowResInfo->pResult[0].window.skey < pWindowResInfo->pResult[1].window.skey)? 1:-1;

  if (order != resultOrder) {
    return;
  }

  int32_t i = 0;
  if (order == QUERY_ASC_FORWARD_STEP) {
    while (i < pWindowResInfo->size && (pWindowResInfo->pResult[i].window.ekey < lastKey)) {
      ++i;
    }
  } else if (order == QUERY_DESC_FORWARD_STEP) {
    while (i < pWindowResInfo->size && (pWindowResInfo->pResult[i].window.skey > lastKey)) {
      ++i;
    }
  }

  if (i < pWindowResInfo->size) {
    pWindowResInfo->size = (i + 1);
  }
}

bool isWindowResClosed(SWindowResInfo *pWindowResInfo, int32_t slot) {
  return (getWindowResult(pWindowResInfo, slot)->status.closed == true);
}

void closeTimeWindow(SWindowResInfo *pWindowResInfo, int32_t slot) {
  getWindowResult(pWindowResInfo, slot)->status.closed = true;
}

void clearTimeWindowResBuf(SQueryRuntimeEnv *pRuntimeEnv, SWindowResult *pWindowRes) {
  if (pWindowRes == NULL) {
    return;
  }
  
  for (int32_t i = 0; i < pRuntimeEnv->pQuery->numOfOutput; ++i) {
    SResultInfo *pResultInfo = &pWindowRes->resultInfo[i];
    
    char * s = getPosInResultPage(pRuntimeEnv, i, pWindowRes);
    size_t size = pRuntimeEnv->pQuery->pSelectExpr[i].bytes;
    memset(s, 0, size);
    
    RESET_RESULT_INFO(pResultInfo);
  }
  
  pWindowRes->numOfRows = 0;
  pWindowRes->pos = (SPosInfo){-1, -1};
  pWindowRes->status.closed = false;
  pWindowRes->window = TSWINDOW_INITIALIZER;
}

/**
 * The source window result pos attribution of the source window result does not assign to the destination,
 * since the attribute of "Pos" is bound to each window result when the window result is created in the
 * disk-based result buffer.
 */
void copyTimeWindowResBuf(SQueryRuntimeEnv *pRuntimeEnv, SWindowResult *dst, const SWindowResult *src) {
  dst->numOfRows = src->numOfRows;
  dst->window = src->window;
  dst->status = src->status;
  
  int32_t nOutputCols = pRuntimeEnv->pQuery->numOfOutput;
  
  for (int32_t i = 0; i < nOutputCols; ++i) {
    SResultInfo *pDst = &dst->resultInfo[i];
    SResultInfo *pSrc = &src->resultInfo[i];
    
    char *buf = pDst->interResultBuf;
    memcpy(pDst, pSrc, sizeof(SResultInfo));
    pDst->interResultBuf = buf;  // restore the allocated buffer
    
    // copy the result info struct
    memcpy(pDst->interResultBuf, pSrc->interResultBuf, pDst->bufLen);
    
    // copy the output buffer data from src to dst, the position info keep unchanged
    char * dstBuf = getPosInResultPage(pRuntimeEnv, i, dst);
    char * srcBuf = getPosInResultPage(pRuntimeEnv, i, (SWindowResult *)src);
    size_t s = pRuntimeEnv->pQuery->pSelectExpr[i].bytes;
    
    memcpy(dstBuf, srcBuf, s);
  }
}

