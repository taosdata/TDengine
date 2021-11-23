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
#include "tscompression.h"

typedef struct SCompSupporter {
  STableQueryInfo **pTableQueryInfo;
  int32_t          *rowIndex;
  int32_t           order;
} SCompSupporter;

int32_t getRowNumForMultioutput(SQueryAttr* pQueryAttr, bool topBottomQuery, bool stable) {
  if (pQueryAttr && (!stable)) {
    for (int16_t i = 0; i < pQueryAttr->numOfOutput; ++i) {
      if (pQueryAttr->pExpr1[i].base.functionId == TSDB_FUNC_TOP || pQueryAttr->pExpr1[i].base.functionId == TSDB_FUNC_BOTTOM) {
        return (int32_t)pQueryAttr->pExpr1[i].base.param[0].i64;
      }
    }
  }

  return 1;
}

int32_t getOutputInterResultBufSize(SQueryAttr* pQueryAttr) {
  int32_t size = 0;

  for (int32_t i = 0; i < pQueryAttr->numOfOutput; ++i) {
    size += pQueryAttr->pExpr1[i].base.interBytes;
  }

  assert(size >= 0);
  return size;
}

int32_t initResultRowInfo(SResultRowInfo *pResultRowInfo, int32_t size, int16_t type) {
  pResultRowInfo->type     = type;
  pResultRowInfo->size     = 0;
  pResultRowInfo->curPos  = -1;
  pResultRowInfo->capacity = size;

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

  for(int32_t i = 0; i < pResultRowInfo->size; ++i) {
    if (pResultRowInfo->pResult[i]) {
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

  pResultRowInfo->size     = 0;
  pResultRowInfo->curPos  = -1;
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
    for (int32_t i = 0; i < pRuntimeEnv->pQueryAttr->numOfOutput; ++i) {
      SResultRowCellInfo *pResultInfo = &pResultRow->pCellInfo[i];

      int16_t size = pRuntimeEnv->pQueryAttr->pExpr1[i].base.resType;
      char * s = getPosInResultPage(pRuntimeEnv->pQueryAttr, page, pResultRow->offset, offset);
      memset(s, 0, size);

      offset += size;
      RESET_RESULT_INFO(pResultInfo);
    }
  }

  pResultRow->numOfRows = 0;
  pResultRow->pageId = -1;
  pResultRow->offset = -1;
  pResultRow->closed = false;

  tfree(pResultRow->key);
  pResultRow->win = TSWINDOW_INITIALIZER;
}

// TODO refactor: use macro
SResultRowCellInfo* getResultCell(const SResultRow* pRow, int32_t qry_index, int32_t* offset) {
  assert(qry_index >= 0 && offset != NULL);
  return (SResultRowCellInfo*)((char*) pRow->pCellInfo + offset[qry_index]);
}

size_t getResultRowSize(SQueryRuntimeEnv* pRuntimeEnv) {
  SQueryAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;
  return (pQueryAttr->numOfOutput * sizeof(SResultRowCellInfo)) + pQueryAttr->interBufSize + sizeof(SResultRow);
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

  return (int32_t) taosArrayGetSize(pGroupResInfo->pRows);
}

static int64_t getNumOfResultWindowRes(SQueryRuntimeEnv* pRuntimeEnv, SResultRow *pResultRow, int32_t* rowCellInfoOffset) {
  SQueryAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;

  for (int32_t j = 0; j < pQueryAttr->numOfOutput; ++j) {
    int32_t functionId = pQueryAttr->pExpr1[j].base.functionId;

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

int32_t tsAscOrder(const void* p1, const void* p2) {
  SResultRowCell* pc1 = (SResultRowCell*) p1;
  SResultRowCell* pc2 = (SResultRowCell*) p2;

  if (pc1->groupId == pc2->groupId) {
    if (pc1->pRow->win.skey == pc2->pRow->win.skey) {
      return 0;
    } else {
      return (pc1->pRow->win.skey < pc2->pRow->win.skey)? -1:1;
    }
  } else {
    return (pc1->groupId < pc2->groupId)? -1:1;
  }
}

int32_t tsDescOrder(const void* p1, const void* p2) {
  SResultRowCell* pc1 = (SResultRowCell*) p1;
  SResultRowCell* pc2 = (SResultRowCell*) p2;

  if (pc1->groupId == pc2->groupId) {
    if (pc1->pRow->win.skey == pc2->pRow->win.skey) {
      return 0;
    } else {
      return (pc1->pRow->win.skey < pc2->pRow->win.skey)? 1:-1;
    }
  } else {
    return (pc1->groupId < pc2->groupId)? -1:1;
  }
}

void orderTheResultRows(SQueryRuntimeEnv* pRuntimeEnv) {
  __compar_fn_t  fn = NULL;
  if (pRuntimeEnv->pQueryAttr->order.order == TSDB_ORDER_ASC) {
    fn = tsAscOrder;
  } else {
    fn = tsDescOrder;
  }

  taosArraySort(pRuntimeEnv->pResultRowArrayList, fn);
}

static int32_t mergeIntoGroupResultImplRv(SQueryRuntimeEnv *pRuntimeEnv, SGroupResInfo* pGroupResInfo, uint64_t groupId, int32_t* rowCellInfoOffset) {
  if (!pGroupResInfo->ordered) {
    orderTheResultRows(pRuntimeEnv);
    pGroupResInfo->ordered = true;
  }

  if (pGroupResInfo->pRows == NULL) {
    pGroupResInfo->pRows = taosArrayInit(100, POINTER_BYTES);
  }

  size_t len = taosArrayGetSize(pRuntimeEnv->pResultRowArrayList);
  for(; pGroupResInfo->position < len; ++pGroupResInfo->position) {

    SResultRowCell* pResultRowCell = taosArrayGet(pRuntimeEnv->pResultRowArrayList, pGroupResInfo->position);
    if (pResultRowCell->groupId != groupId) {
      break;
    }

    int64_t num = getNumOfResultWindowRes(pRuntimeEnv, pResultRowCell->pRow, rowCellInfoOffset);
    if (num <= 0) {
      continue;
    }

    taosArrayPush(pGroupResInfo->pRows, &pResultRowCell->pRow);
    pResultRowCell->pRow->numOfRows = (uint32_t) num;

  }

  return TSDB_CODE_SUCCESS;
}

int32_t mergeIntoGroupResult(SGroupResInfo* pGroupResInfo, SQueryRuntimeEnv* pRuntimeEnv, int32_t* offset) {
  int64_t st = taosGetTimestampUs();

  while (pGroupResInfo->currentGroup < pGroupResInfo->totalGroup) {
    mergeIntoGroupResultImplRv(pRuntimeEnv, pGroupResInfo, pGroupResInfo->currentGroup, offset);

    // this group generates at least one result, return results
    if (taosArrayGetSize(pGroupResInfo->pRows) > 0) {
      break;
    }

    qDebug("QInfo:%"PRIu64" no result in group %d, continue", GET_QID(pRuntimeEnv), pGroupResInfo->currentGroup);
    cleanupGroupResInfo(pGroupResInfo);
    incNextGroup(pGroupResInfo);
  }

  int64_t elapsedTime = taosGetTimestampUs() - st;
  qDebug("QInfo:%"PRIu64" merge res data into group, index:%d, total group:%d, elapsed time:%" PRId64 "us", GET_QID(pRuntimeEnv),
         pGroupResInfo->currentGroup, pGroupResInfo->totalGroup, elapsedTime);

  return TSDB_CODE_SUCCESS;
}

void blockDistInfoToBinary(STableBlockDist* pDist, struct SBufferWriter* bw) {
  tbufWriteUint32(bw, pDist->numOfTables);
  tbufWriteUint16(bw, pDist->numOfFiles);
  tbufWriteUint64(bw, pDist->totalSize);
  tbufWriteUint64(bw, pDist->totalRows);
  tbufWriteInt32(bw, pDist->maxRows);
  tbufWriteInt32(bw, pDist->minRows);
  tbufWriteUint32(bw, pDist->numOfRowsInMemTable);
  tbufWriteUint32(bw, pDist->numOfSmallBlocks);
  tbufWriteUint64(bw, taosArrayGetSize(pDist->dataBlockInfos));

  // compress the binary string
  char* p = TARRAY_GET_START(pDist->dataBlockInfos);

  // compress extra bytes
  size_t x = taosArrayGetSize(pDist->dataBlockInfos) * pDist->dataBlockInfos->elemSize;
  char* tmp = malloc(x + 2);

  bool comp = false;
  int32_t len = tsCompressString(p, (int32_t)x, 1, tmp, (int32_t)x, ONE_STAGE_COMP, NULL, 0);
  if (len == -1 || len >= x) { // compress failed, do not compress this binary data
    comp = false;
    len = (int32_t)x;
  } else {
    comp = true;
  }

  tbufWriteUint8(bw, comp);
  tbufWriteUint32(bw, len);
  if (comp) {
    tbufWriteBinary(bw, tmp, len);
  } else {
    tbufWriteBinary(bw, p, len);
  }
  tfree(tmp);
}

void blockDistInfoFromBinary(const char* data, int32_t len, STableBlockDist* pDist) {
  SBufferReader br = tbufInitReader(data, len, false);

  pDist->numOfTables = tbufReadUint32(&br);
  pDist->numOfFiles  = tbufReadUint16(&br);
  pDist->totalSize   = tbufReadUint64(&br);
  pDist->totalRows   = tbufReadUint64(&br);
  pDist->maxRows     = tbufReadInt32(&br);
  pDist->minRows     = tbufReadInt32(&br);
  pDist->numOfRowsInMemTable = tbufReadUint32(&br);
  pDist->numOfSmallBlocks = tbufReadUint32(&br);
  int64_t numSteps = tbufReadUint64(&br);

  bool comp = tbufReadUint8(&br);
  uint32_t compLen = tbufReadUint32(&br);

  size_t originalLen = (size_t) (numSteps *sizeof(SFileBlockInfo));

  char* outputBuf = NULL;
  if (comp) {
    outputBuf = malloc(originalLen);

    size_t actualLen = compLen;
    const char* compStr = tbufReadBinary(&br, &actualLen);

    int32_t orignalLen = tsDecompressString(compStr, compLen, 1, outputBuf,
                                            (int32_t)originalLen , ONE_STAGE_COMP, NULL, 0);
    assert(orignalLen == numSteps *sizeof(SFileBlockInfo));
  } else {
    outputBuf = (char*) tbufReadBinary(&br, &originalLen);
  }

  pDist->dataBlockInfos = taosArrayFromList(outputBuf, (uint32_t)numSteps, sizeof(SFileBlockInfo));
  if (comp) {
    tfree(outputBuf);
  }
}

