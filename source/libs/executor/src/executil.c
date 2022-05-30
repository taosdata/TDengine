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

int32_t getRowNumForMultioutput(STaskAttr* pQueryAttr, bool topBottomQuery, bool stable) {
  if (pQueryAttr && (!stable)) {
    for (int16_t i = 0; i < pQueryAttr->numOfOutput; ++i) {
//      if (pQueryAttr->pExpr1[i].base. == FUNCTION_TOP || pQueryAttr->pExpr1[i].base.functionId == FUNCTION_BOTTOM) {
//        return (int32_t)pQueryAttr->pExpr1[i].base.param[0].i;
//      }
    }
  }

  return 1;
}

int32_t getOutputInterResultBufSize(STaskAttr* pQueryAttr) {
  int32_t size = 0;

  for (int32_t i = 0; i < pQueryAttr->numOfOutput; ++i) {
//    size += pQueryAttr->pExpr1[i].base.interBytes;
  }

  assert(size >= 0);
  return size;
}

int32_t initResultRowInfo(SResultRowInfo *pResultRowInfo, int32_t size) {
  pResultRowInfo->size       = 0;
  pResultRowInfo->capacity   = size;
  pResultRowInfo->cur.pageId = -1;
  
  pResultRowInfo->pPosition = taosMemoryCalloc(pResultRowInfo->capacity, sizeof(SResultRowPosition));
  if (pResultRowInfo->pPosition == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

void cleanupResultRowInfo(SResultRowInfo *pResultRowInfo) {
  if (pResultRowInfo == NULL) {
    return;
  }

  if (pResultRowInfo->capacity == 0) {
//    assert(pResultRowInfo->pResult == NULL);
    return;
  }

  for(int32_t i = 0; i < pResultRowInfo->size; ++i) {
//    if (pResultRowInfo->pResult[i]) {
//      taosMemoryFreeClear(pResultRowInfo->pResult[i]->key);
//    }
  }
  
  taosMemoryFreeClear(pResultRowInfo->pPosition);
}

void resetResultRowInfo(STaskRuntimeEnv *pRuntimeEnv, SResultRowInfo *pResultRowInfo) {
  if (pResultRowInfo == NULL || pResultRowInfo->capacity == 0) {
    return;
  }

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

int32_t numOfClosedResultRows(SResultRowInfo *pResultRowInfo) {
  int32_t i = 0;
//  while (i < pResultRowInfo->size && pResultRowInfo->pResult[i]->closed) {
//    ++i;
//  }
  
  return i;
}

void closeAllResultRows(SResultRowInfo *pResultRowInfo) {
  assert(pResultRowInfo->size >= 0 && pResultRowInfo->capacity >= pResultRowInfo->size);
  
  for (int32_t i = 0; i < pResultRowInfo->size; ++i) {
  }
}

bool isResultRowClosed(SResultRow* pRow) {
  return (pRow->closed == true);
}

void closeResultRow(SResultRow* pResultRow) {
  pResultRow->closed = true;
}

void clearResultRow(STaskRuntimeEnv *pRuntimeEnv, SResultRow *pResultRow) {
  if (pResultRow == NULL) {
    return;
  }

  // the result does not put into the SDiskbasedBuf, ignore it.
  if (pResultRow->pageId >= 0) {
    SFilePage *page = getBufPage(pRuntimeEnv->pResultBuf, pResultRow->pageId);

    int16_t offset = 0;
    for (int32_t i = 0; i < pRuntimeEnv->pQueryAttr->numOfOutput; ++i) {
      struct SResultRowEntryInfo *pEntryInfo = NULL;//pResultRow->pEntryInfo[i];

//      int16_t size = pRuntimeEnv->pQueryAttr->pExpr1[i].base.resSchema.bytes;
//      char * s = getPosInResultPage(pRuntimeEnv->pQueryAttr, page, pResultRow->offset, offset);
//      memset(s, 0, size);

//      offset += size;
      cleanupResultRowEntry(pEntryInfo);
    }
  }

  pResultRow->numOfRows = 0;
  pResultRow->pageId = -1;
  pResultRow->offset = -1;
  pResultRow->closed = false;
  pResultRow->win = TSWINDOW_INITIALIZER;
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

static int64_t getNumOfResultWindowRes(STaskRuntimeEnv* pRuntimeEnv, SResultRowPosition *pos, int32_t* rowCellInfoOffset) {
  STaskAttr* pQueryAttr = pRuntimeEnv->pQueryAttr;
  ASSERT(0);

  for (int32_t j = 0; j < pQueryAttr->numOfOutput; ++j) {
    int32_t functionId = 0;//pQueryAttr->pExpr1[j].base.functionId;

    /*
     * ts, tag, tagprj function can not decide the output number of current query
     * the number of output result is decided by main output
     */
    if (functionId == FUNCTION_TS || functionId == FUNCTION_TAG || functionId == FUNCTION_TAGPRJ) {
      continue;
    }

//    SResultRowEntryInfo *pResultInfo = getResultCell(pResultRow, j, rowCellInfoOffset);
//    assert(pResultInfo != NULL);
//
//    if (pResultInfo->numOfRes > 0) {
//      return pResultInfo->numOfRes;
//    }
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

static int32_t mergeIntoGroupResultImplRv(STaskRuntimeEnv *pRuntimeEnv, SGroupResInfo* pGroupResInfo, uint64_t groupId, int32_t* rowCellInfoOffset) {
  if (pGroupResInfo->pRows == NULL) {
    pGroupResInfo->pRows = taosArrayInit(100, POINTER_BYTES);
  }

  size_t len = taosArrayGetSize(pRuntimeEnv->pResultRowArrayList);
  for(; pGroupResInfo->position < len; ++pGroupResInfo->position) {
    SResultRowCell* pResultRowCell = taosArrayGet(pRuntimeEnv->pResultRowArrayList, pGroupResInfo->position);
    if (pResultRowCell->groupId != groupId) {
      break;
    }


    int64_t num = getNumOfResultWindowRes(pRuntimeEnv, &pResultRowCell->pos, rowCellInfoOffset);
    if (num <= 0) {
      continue;
    }

    taosArrayPush(pGroupResInfo->pRows, &pResultRowCell->pos);
//    pResultRowCell->pRow->numOfRows = (uint32_t) num;
  }

  return TSDB_CODE_SUCCESS;
}

static UNUSED_FUNC int32_t mergeIntoGroupResultImpl(STaskRuntimeEnv *pRuntimeEnv, SGroupResInfo* pGroupResInfo, SArray *pTableList,
    int32_t* rowCellInfoOffset) {
  bool ascQuery = true;
#if 0
  int32_t code = TSDB_CODE_SUCCESS;

  int32_t *posList = NULL;
  SMultiwayMergeTreeInfo *pTree = NULL;
  STableQueryInfo **pTableQueryInfoList = NULL;

  size_t size = taosArrayGetSize(pTableList);
  if (pGroupResInfo->pRows == NULL) {
    pGroupResInfo->pRows = taosArrayInit(100, POINTER_BYTES);
  }

  posList = taosMemoryCalloc(size, sizeof(int32_t));
  pTableQueryInfoList = taosMemoryMalloc(POINTER_BYTES * size);

  if (pTableQueryInfoList == NULL || posList == NULL || pGroupResInfo->pRows == NULL || pGroupResInfo->pRows == NULL) {
//    qError("QInfo:%"PRIu64" failed alloc memory", GET_TASKID(pRuntimeEnv));
    code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    goto _end;
  }

  int32_t numOfTables = 0;
  for (int32_t i = 0; i < size; ++i) {
    STableQueryInfo *item = taosArrayGetP(pTableList, i);
//    if (item->resInfo.size > 0) {
//      pTableQueryInfoList[numOfTables++] = item;
//    }
  }

  // there is no data in current group
  // no need to merge results since only one table in each group
//  if (numOfTables == 0) {
//    goto _end;
//  }

  int32_t order = TSDB_ORDER_ASC;
  SCompSupporter cs = {pTableQueryInfoList, posList, order};

  int32_t ret = tMergeTreeCreate(&pTree, numOfTables, &cs, tableResultComparFn);
  if (ret != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    goto _end;
  }

  int64_t lastTimestamp = ascQuery? INT64_MIN:INT64_MAX;
  int64_t startt = taosGetTimestampMs();

  while (1) {
    int32_t tableIndex = tMergeTreeGetChosenIndex(pTree);

    SResultRowInfo *pWindowResInfo = &pTableQueryInfoList[tableIndex]->resInfo;
    ASSERT(0);
    SResultRow  *pWindowRes = NULL;//getResultRow(pBuf, pWindowResInfo, cs.rowIndex[tableIndex]);

    int64_t num = 0;//getNumOfResultWindowRes(pRuntimeEnv, pWindowRes, rowCellInfoOffset);
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

    tMergeTreeAdjust(pTree, tMergeTreeGetAdjustIndex(pTree));
  }

  int64_t endt = taosGetTimestampMs();

//  qDebug("QInfo:%"PRIx64" result merge completed for group:%d, elapsed time:%" PRId64 " ms", GET_TASKID(pRuntimeEnv),
//         pGroupResInfo->currentGroup, endt - startt);

  _end:
  taosMemoryFreeClear(pTableQueryInfoList);
  taosMemoryFreeClear(posList);
  taosMemoryFreeClear(pTree);

  return code;
}

int32_t mergeIntoGroupResult(SGroupResInfo* pGroupResInfo, STaskRuntimeEnv* pRuntimeEnv, int32_t* offset) {
  int64_t st = taosGetTimestampUs();

  while (pGroupResInfo->currentGroup < pGroupResInfo->totalGroup) {
    mergeIntoGroupResultImplRv(pRuntimeEnv, pGroupResInfo, pGroupResInfo->currentGroup, offset);

    // this group generates at least one result, return results
    if (taosArrayGetSize(pGroupResInfo->pRows) > 0) {
      break;
    }

//    qDebug("QInfo:%"PRIu64" no result in group %d, continue", GET_TASKID(pRuntimeEnv), pGroupResInfo->currentGroup);
    cleanupGroupResInfo(pGroupResInfo);
    incNextGroup(pGroupResInfo);
  }

//  int64_t elapsedTime = taosGetTimestampUs() - st;
//  qDebug("QInfo:%"PRIu64" merge res data into group, index:%d, total group:%d, elapsed time:%" PRId64 "us", GET_TASKID(pRuntimeEnv),
//         pGroupResInfo->currentGroup, pGroupResInfo->totalGroup, elapsedTime);
#endif

  return TSDB_CODE_SUCCESS;
}

//void blockDistInfoToBinary(STableBlockDist* pDist, struct SBufferWriter* bw) {
//  tbufWriteUint32(bw, pDist->numOfTables);
//  tbufWriteUint16(bw, pDist->numOfFiles);
//  tbufWriteUint64(bw, pDist->totalSize);
//  tbufWriteUint64(bw, pDist->totalRows);
//  tbufWriteInt32(bw, pDist->maxRows);
//  tbufWriteInt32(bw, pDist->minRows);
//  tbufWriteUint32(bw, pDist->numOfRowsInMemTable);
//  tbufWriteUint32(bw, pDist->numOfSmallBlocks);
//  tbufWriteUint64(bw, taosArrayGetSize(pDist->dataBlockInfos));
//
//  // compress the binary string
//  char* p = TARRAY_GET_START(pDist->dataBlockInfos);
//
//  // compress extra bytes
//  size_t x = taosArrayGetSize(pDist->dataBlockInfos) * pDist->dataBlockInfos->elemSize;
//  char* tmp = taosMemoryMalloc(x + 2);
//
//  bool comp = false;
//  int32_t len = tsCompressString(p, (int32_t)x, 1, tmp, (int32_t)x, ONE_STAGE_COMP, NULL, 0);
//  if (len == -1 || len >= x) { // compress failed, do not compress this binary data
//    comp = false;
//    len = (int32_t)x;
//  } else {
//    comp = true;
//  }
//
//  tbufWriteUint8(bw, comp);
//  tbufWriteUint32(bw, len);
//  if (comp) {
//    tbufWriteBinary(bw, tmp, len);
//  } else {
//    tbufWriteBinary(bw, p, len);
//  }
//  taosMemoryFreeClear(tmp);
//}

//void blockDistInfoFromBinary(const char* data, int32_t len, STableBlockDist* pDist) {
//  SBufferReader br = tbufInitReader(data, len, false);
//
//  pDist->numOfTables = tbufReadUint32(&br);
//  pDist->numOfFiles  = tbufReadUint16(&br);
//  pDist->totalSize   = tbufReadUint64(&br);
//  pDist->totalRows   = tbufReadUint64(&br);
//  pDist->maxRows     = tbufReadInt32(&br);
//  pDist->minRows     = tbufReadInt32(&br);
//  pDist->numOfRowsInMemTable = tbufReadUint32(&br);
//  pDist->numOfSmallBlocks = tbufReadUint32(&br);
//  int64_t numSteps = tbufReadUint64(&br);
//
//  bool comp = tbufReadUint8(&br);
//  uint32_t compLen = tbufReadUint32(&br);
//
//  size_t originalLen = (size_t) (numSteps *sizeof(SFileBlockInfo));
//
//  char* outputBuf = NULL;
//  if (comp) {
//    outputBuf = taosMemoryMalloc(originalLen);
//
//    size_t actualLen = compLen;
//    const char* compStr = tbufReadBinary(&br, &actualLen);
//
//    int32_t orignalLen = tsDecompressString(compStr, compLen, 1, outputBuf,
//                                            (int32_t)originalLen , ONE_STAGE_COMP, NULL, 0);
//    assert(orignalLen == numSteps *sizeof(SFileBlockInfo));
//  } else {
//    outputBuf = (char*) tbufReadBinary(&br, &originalLen);
//  }
//
//  pDist->dataBlockInfos = taosArrayFromList(outputBuf, (uint32_t)numSteps, sizeof(SFileBlockInfo));
//  if (comp) {
//    taosMemoryFreeClear(outputBuf);
//  }
//}

