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

#include "tcommon.h"
#include "query.h"

#include "tdatablock.h"
#include "tdef.h"
#include "tlosertree.h"
#include "tpagedbuf.h"
#include "tsort.h"
#include "tutil.h"

typedef struct STupleHandle {
  SSDataBlock* pBlock;
  int32_t      rowIndex;
} STupleHandle;

typedef struct SSortHandle {
  int32_t           type;

  int32_t           pageSize;
  int32_t           numOfPages;
  SDiskbasedBuf    *pBuf;

  SArray           *pOrderInfo;
  bool              nullFirst;
  SArray           *pOrderedSource;

  _sort_fetch_block_fn_t  fetchfp;
  _sort_merge_compar_fn_t comparFn;

  void             *pParam;
  SMultiwayMergeTreeInfo *pMergeTree;
  int32_t           numOfCols;

  int64_t           startTs;
  uint64_t          sortElapsed;
  uint64_t          totalElapsed;

  int32_t           sourceId;
  SSDataBlock      *pDataBlock;
  SMsortComparParam cmpParam;
  int32_t           numOfCompletedSources;
  bool              opened;
  const char       *idStr;

  bool              inMemSort;
  bool              needAdjust;
  STupleHandle      tupleHandle;
} SSortHandle;

static int32_t msortComparFn(const void *pLeft, const void *pRight, void *param);

static SSDataBlock* createDataBlock_rv(SSchema* pSchema, int32_t numOfCols) {
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  pBlock->pDataBlock = taosArrayInit(numOfCols, sizeof(SColumnInfoData));
  pBlock->info.numOfCols = numOfCols;

  for(int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData  colInfo = {0};

    colInfo.info.type  = pSchema[i].type;
    colInfo.info.bytes = pSchema[i].bytes;
    colInfo.info.colId = pSchema[i].colId;
    taosArrayPush(pBlock->pDataBlock, &colInfo);

    if (IS_VAR_DATA_TYPE(colInfo.info.type)) {
      pBlock->info.hasVarCol = true;
    }
  }

  return pBlock;
}

/**
 *
 * @param type
 * @return
 */
SSortHandle* tsortCreateSortHandle(SArray* pOrderInfo, bool nullFirst, int32_t type, int32_t pageSize, int32_t numOfPages, SSchema* pSchema, int32_t numOfCols, const char* idstr) {
  SSortHandle* pSortHandle = taosMemoryCalloc(1, sizeof(SSortHandle));

  pSortHandle->type       = type;
  pSortHandle->pageSize   = pageSize;
  pSortHandle->numOfPages = numOfPages;
  pSortHandle->pOrderedSource = taosArrayInit(4, POINTER_BYTES);
  pSortHandle->pOrderInfo = pOrderInfo;
  pSortHandle->nullFirst  = nullFirst;
  pSortHandle->cmpParam.orderInfo = pOrderInfo;

  pSortHandle->pDataBlock = createDataBlock_rv(pSchema, numOfCols);
  tsortSetComparFp(pSortHandle, msortComparFn);

  if (idstr != NULL) {
    pSortHandle->idStr    = strdup(idstr);
  }

  return pSortHandle;
}

void tsortDestroySortHandle(SSortHandle* pSortHandle) {
  tsortClose(pSortHandle);
  if (pSortHandle->pMergeTree != NULL) {
    tMergeTreeDestroy(pSortHandle->pMergeTree);
  }

  destroyDiskbasedBuf(pSortHandle->pBuf);
  taosMemoryFreeClear(pSortHandle->idStr);
  taosMemoryFreeClear(pSortHandle);
}

int32_t tsortAddSource(SSortHandle* pSortHandle, void* pSource) {
  taosArrayPush(pSortHandle->pOrderedSource, &pSource);
}

static int32_t doAddNewExternalMemSource(SDiskbasedBuf *pBuf, SArray* pAllSources, SSDataBlock* pBlock, int32_t* sourceId) {
  SExternalMemSource* pSource = taosMemoryCalloc(1, sizeof(SExternalMemSource));
  if (pSource == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  pSource->pageIdList = getDataBufPagesIdList(pBuf, (*sourceId));
  pSource->src.pBlock = pBlock;

  taosArrayPush(pAllSources, &pSource);

  (*sourceId) += 1;

  int32_t rowSize = blockDataGetSerialRowSize(pSource->src.pBlock);
  int32_t numOfRows = (getBufPageSize(pBuf) - blockDataGetSerialMetaSize(pBlock))/rowSize;

  return blockDataEnsureCapacity(pSource->src.pBlock, numOfRows);
}

static int32_t doAddToBuf(SSDataBlock* pDataBlock, SSortHandle* pHandle) {
  int32_t start = 0;

  if (pHandle->pBuf == NULL) {
    int32_t code = createDiskbasedBuf(&pHandle->pBuf, pHandle->pageSize, pHandle->numOfPages * pHandle->pageSize, 0, "/tmp");
    dBufSetPrintInfo(pHandle->pBuf);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  while(start < pDataBlock->info.rows) {
    int32_t stop = 0;
    blockDataSplitRows(pDataBlock, pDataBlock->info.hasVarCol, start, &stop, pHandle->pageSize);
    SSDataBlock* p = blockDataExtractBlock(pDataBlock, start, stop - start + 1);
    if (p == NULL) {
      return terrno;
    }

    int32_t pageId = -1;
    SFilePage* pPage = getNewBufPage(pHandle->pBuf, pHandle->sourceId, &pageId);
    if (pPage == NULL) {
      return terrno;
    }

    int32_t size = blockDataGetSize(p) + sizeof(int32_t)  + p->info.numOfCols * sizeof(int32_t);
    assert(size <= getBufPageSize(pHandle->pBuf));

    blockDataToBuf(pPage->data, p);

    setBufPageDirty(pPage, true);
    releaseBufPage(pHandle->pBuf, pPage);

    blockDataDestroy(p);
    start = stop + 1;
  }

  blockDataCleanup(pDataBlock);

  SSDataBlock* pBlock = createOneDataBlock(pDataBlock);
  int32_t code = doAddNewExternalMemSource(pHandle->pBuf, pHandle->pOrderedSource, pBlock, &pHandle->sourceId);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
}

static int32_t sortComparInit(SMsortComparParam* cmpParam, SArray* pSources, int32_t startIndex, int32_t endIndex, SSortHandle* pHandle) {
  cmpParam->pSources  = taosArrayGet(pSources, startIndex);
  cmpParam->numOfSources = (endIndex - startIndex + 1);

  int32_t code = 0;

  if (pHandle->type == SORT_SINGLESOURCE_SORT) {
    for (int32_t i = 0; i < cmpParam->numOfSources; ++i) {
      SExternalMemSource* pSource = cmpParam->pSources[i];
      SPageInfo*          pPgInfo = *(SPageInfo**)taosArrayGet(pSource->pageIdList, pSource->pageIndex);

      SFilePage* pPage = getBufPage(pHandle->pBuf, getPageId(pPgInfo));
      code = blockDataFromBuf(pSource->src.pBlock, pPage->data);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      releaseBufPage(pHandle->pBuf, pPage);
    }
  } else {
    // multi-pass internal merge sort is required
    if (pHandle->pBuf == NULL) {
      code = createDiskbasedBuf(&pHandle->pBuf, pHandle->pageSize, pHandle->numOfPages * pHandle->pageSize, 0, "/tmp");
      dBufSetPrintInfo(pHandle->pBuf);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    for (int32_t i = 0; i < cmpParam->numOfSources; ++i) {
      SGenericSource* pSource = cmpParam->pSources[i];
      pSource->src.pBlock = pHandle->fetchfp(pSource->param);
    }
  }

  return code;
}

static int32_t sortComparClearup(SMsortComparParam* cmpParam) {
  for(int32_t i = 0; i < cmpParam->numOfSources; ++i) {
    SExternalMemSource* pSource = cmpParam->pSources[i];
    blockDataDestroy(pSource->src.pBlock);
    taosMemoryFreeClear(pSource);
  }

  cmpParam->numOfSources = 0;
}

static void appendOneRowToDataBlock(SSDataBlock *pBlock, const SSDataBlock* pSource, int32_t* rowIndex) {
  for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);

    SColumnInfoData* pSrcColInfo = taosArrayGet(pSource->pDataBlock, i);
    bool isNull = colDataIsNull(pSrcColInfo, pSource->info.rows, *rowIndex, NULL);

    if (isNull) {
      colDataAppend(pColInfo, pBlock->info.rows, NULL, true);
    } else {
      char* pData = colDataGetData(pSrcColInfo, *rowIndex);
      colDataAppend(pColInfo, pBlock->info.rows, pData, false);
    }
  }

  pBlock->info.rows += 1;
  *rowIndex += 1;
}

static int32_t adjustMergeTreeForNextTuple(SExternalMemSource *pSource, SMultiwayMergeTreeInfo *pTree, SSortHandle *pHandle, int32_t* numOfCompleted) {
  /*
   * load a new SDataBlock into memory of a given intermediate data-set source,
   * since it's last record in buffer has been chosen to be processed, as the winner of loser-tree
   */
  if (pSource->src.rowIndex >= pSource->src.pBlock->info.rows) {
    pSource->src.rowIndex = 0;

    if (pHandle->type == SORT_SINGLESOURCE_SORT) {
      if (pSource->pageIndex >= taosArrayGetSize(pSource->pageIdList)) {
        (*numOfCompleted) += 1;
        pSource->src.rowIndex = -1;
        pSource->pageIndex = -1;
        pSource->src.pBlock = blockDataDestroy(pSource->src.pBlock);
      } else {
        SPageInfo* pPgInfo = *(SPageInfo**)taosArrayGet(pSource->pageIdList, pSource->pageIndex);

        SFilePage* pPage = getBufPage(pHandle->pBuf, getPageId(pPgInfo));
        int32_t    code = blockDataFromBuf(pSource->src.pBlock, pPage->data);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }

        releaseBufPage(pHandle->pBuf, pPage);
      }
    } else {
      pSource->src.pBlock = pHandle->fetchfp(((SGenericSource*)pSource)->param);
      if (pSource->src.pBlock == NULL) {
        (*numOfCompleted) += 1;
        pSource->src.rowIndex = -1;
      }
    }
  }

  /*
   * Adjust loser tree otherwise, according to new candidate data
   * if the loser tree is rebuild completed, we do not need to adjust
   */
  int32_t leafNodeIndex = tMergeTreeGetAdjustIndex(pTree);

#ifdef _DEBUG_VIEW
  printf("before adjust:\t");
  tMergeTreePrint(pTree);
#endif

  tMergeTreeAdjust(pTree, leafNodeIndex);

#ifdef _DEBUG_VIEW
  printf("\nafter adjust:\t");
  tMergeTreePrint(pTree);
#endif
}

static SSDataBlock* getSortedBlockData(SSortHandle* pHandle, SMsortComparParam* cmpParam, int32_t capacity) {
  blockDataCleanup(pHandle->pDataBlock);

  while(1) {
    if (cmpParam->numOfSources == pHandle->numOfCompletedSources) {
      break;
    }

    int32_t index = tMergeTreeGetChosenIndex(pHandle->pMergeTree);

    SExternalMemSource *pSource = (*cmpParam).pSources[index];
    appendOneRowToDataBlock(pHandle->pDataBlock, pSource->src.pBlock, &pSource->src.rowIndex);

    int32_t code = adjustMergeTreeForNextTuple(pSource, pHandle->pMergeTree, pHandle, &pHandle->numOfCompletedSources);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
      return NULL;
    }

    if (pHandle->pDataBlock->info.rows >= capacity) {
      return pHandle->pDataBlock;
    }
  }

  return (pHandle->pDataBlock->info.rows > 0)? pHandle->pDataBlock:NULL;
}

int32_t msortComparFn(const void *pLeft, const void *pRight, void *param) {
  int32_t pLeftIdx  = *(int32_t *)pLeft;
  int32_t pRightIdx = *(int32_t *)pRight;

  SMsortComparParam *pParam = (SMsortComparParam *)param;

  SArray *pInfo = pParam->orderInfo;

  SExternalMemSource* pLeftSource  = pParam->pSources[pLeftIdx];
  SExternalMemSource* pRightSource = pParam->pSources[pRightIdx];

  // this input is exhausted, set the special value to denote this
  if (pLeftSource->src.rowIndex == -1) {
    return 1;
  }

  if (pRightSource->src.rowIndex == -1) {
    return -1;
  }

  SSDataBlock* pLeftBlock = pLeftSource->src.pBlock;
  SSDataBlock* pRightBlock = pRightSource->src.pBlock;

  for(int32_t i = 0; i < pInfo->size; ++i) {
    SBlockOrderInfo* pOrder = TARRAY_GET_ELEM(pInfo, i);

    SColumnInfoData* pLeftColInfoData = TARRAY_GET_ELEM(pLeftBlock->pDataBlock, pOrder->colIndex);

    bool leftNull  = false;
    if (pLeftColInfoData->hasNull) {
      leftNull = colDataIsNull(pLeftColInfoData, pLeftBlock->info.rows, pLeftSource->src.rowIndex, pLeftBlock->pBlockAgg);
    }

    SColumnInfoData* pRightColInfoData = TARRAY_GET_ELEM(pRightBlock->pDataBlock, pOrder->colIndex);
    bool rightNull = false;
    if (pRightColInfoData->hasNull) {
      rightNull = colDataIsNull(pRightColInfoData, pRightBlock->info.rows, pRightSource->src.rowIndex, pRightBlock->pBlockAgg);
    }

    if (leftNull && rightNull) {
      continue; // continue to next slot
    }

    if (rightNull) {
      return pParam->nullFirst? 1:-1;
    }

    if (leftNull) {
      return pParam->nullFirst? -1:1;
    }

    void* left1  = colDataGetData(pLeftColInfoData, pLeftSource->src.rowIndex);
    void* right1 = colDataGetData(pRightColInfoData, pRightSource->src.rowIndex);

    switch(pLeftColInfoData->info.type) {
      case TSDB_DATA_TYPE_INT: {
        int32_t leftv = *(int32_t*)left1;
        int32_t rightv = *(int32_t*)right1;

        if (leftv == rightv) {
          break;
        } else {
          if (pOrder->order == TSDB_ORDER_ASC) {
            return leftv < rightv? -1 : 1;
          } else {
            return leftv < rightv? 1 : -1;
          }
        }
      }
      default:
        assert(0);
    }
  }
}

static int32_t doInternalMergeSort(SSortHandle* pHandle) {
  size_t numOfSources = taosArrayGetSize(pHandle->pOrderedSource);

  // Calculate the I/O counts to complete the data sort.
  double sortPass = floorl(log2(numOfSources) / log2(pHandle->numOfPages));

  pHandle->totalElapsed = taosGetTimestampUs() - pHandle->startTs;
  qDebug("%s %d rounds mergesort required to complete the sort, first-round sorted data size:%"PRIzu", sort:%"PRId64", total elapsed:%"PRId64,
         pHandle->idStr, (int32_t) (sortPass + 1), getTotalBufSize(pHandle->pBuf), pHandle->sortElapsed, pHandle->totalElapsed);

  size_t pgSize = pHandle->pageSize;
  int32_t numOfRows = (pgSize - blockDataGetSerialMetaSize(pHandle->pDataBlock))/ blockDataGetSerialRowSize(pHandle->pDataBlock);

  blockDataEnsureCapacity(pHandle->pDataBlock, numOfRows);

  size_t numOfSorted = taosArrayGetSize(pHandle->pOrderedSource);
  for(int32_t t = 0; t < sortPass; ++t) {
    int64_t st = taosGetTimestampUs();

    SArray* pResList = taosArrayInit(4, POINTER_BYTES);

    int32_t numOfInputSources = pHandle->numOfPages;
    int32_t sortGroup = (numOfSorted + numOfInputSources - 1) / numOfInputSources;

    // Only *numOfInputSources* can be loaded into buffer to perform the external sort.
    for(int32_t i = 0; i < sortGroup; ++i) {
      pHandle->sourceId += 1;

      int32_t end = (i + 1) * numOfInputSources - 1;
      if (end > numOfSorted - 1) {
        end = numOfSorted - 1;
      }

      pHandle->cmpParam.numOfSources = end - i * numOfInputSources + 1;

      int32_t code = sortComparInit(&pHandle->cmpParam, pHandle->pOrderedSource, i * numOfInputSources, end, pHandle);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      code = tMergeTreeCreate(&pHandle->pMergeTree, pHandle->cmpParam.numOfSources, &pHandle->cmpParam, pHandle->comparFn);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      while (1) {
        SSDataBlock* pDataBlock = getSortedBlockData(pHandle, &pHandle->cmpParam, numOfRows);
        if (pDataBlock == NULL) {
          break;
        }

        int32_t pageId = -1;
        SFilePage* pPage = getNewBufPage(pHandle->pBuf, pHandle->sourceId, &pageId);
        if (pPage == NULL) {
          return terrno;
        }

        int32_t size = blockDataGetSize(pDataBlock) + sizeof(int32_t) + pDataBlock->info.numOfCols * sizeof(int32_t);
        assert(size <= getBufPageSize(pHandle->pBuf));

        blockDataToBuf(pPage->data, pDataBlock);

        setBufPageDirty(pPage, true);
        releaseBufPage(pHandle->pBuf, pPage);

        blockDataCleanup(pDataBlock);
      }

      tMergeTreeDestroy(pHandle->pMergeTree);
      pHandle->numOfCompletedSources = 0;

      SSDataBlock* pBlock = createOneDataBlock(pHandle->pDataBlock);
      code = doAddNewExternalMemSource(pHandle->pBuf, pResList, pBlock, &pHandle->sourceId);
      if (code != 0) {
        return code;
      }
    }

    sortComparClearup(&pHandle->cmpParam);

    taosArrayClear(pHandle->pOrderedSource);
    taosArrayAddAll(pHandle->pOrderedSource, pResList);
    taosArrayDestroy(pResList);

    numOfSorted = taosArrayGetSize(pHandle->pOrderedSource);

    int64_t el = taosGetTimestampUs() - st;
    pHandle->totalElapsed += el;

    SDiskbasedBufStatis statis = getDBufStatis(pHandle->pBuf);
    qDebug("%s %d round mergesort, elapsed:%"PRId64" readDisk:%.2f Kb, flushDisk:%.2f Kb", pHandle->idStr, t + 1, el, statis.loadBytes/1024.0,
           statis.flushBytes/1024.0);

    if (pHandle->type == SORT_MULTISOURCE_MERGE) {
      pHandle->type = SORT_SINGLESOURCE_SORT;
      pHandle->comparFn = msortComparFn;
    }
  }

  pHandle->cmpParam.numOfSources = taosArrayGetSize(pHandle->pOrderedSource);
  return 0;
}

static int32_t createInitialSortedMultiSources(SSortHandle* pHandle) {
  size_t sortBufSize = pHandle->numOfPages * pHandle->pageSize;

  if (pHandle->type == SORT_SINGLESOURCE_SORT) {
    SGenericSource* source = taosArrayGetP(pHandle->pOrderedSource, 0);
    taosArrayClear(pHandle->pOrderedSource);

    while (1) {
      SSDataBlock* pBlock = pHandle->fetchfp(source->param);
      if (pBlock == NULL) {
        break;
      }

      if (pHandle->pDataBlock == NULL) {
        pHandle->pDataBlock = createOneDataBlock(pBlock);
      }

      int32_t code = blockDataMerge(pHandle->pDataBlock, pBlock);
      if (code != 0) {
        return code;
      }

      size_t size = blockDataGetSize(pHandle->pDataBlock);
      if (size > sortBufSize) {
        // Perform the in-memory sort and then flush data in the buffer into disk.
        int64_t p = taosGetTimestampUs();
        blockDataSort(pHandle->pDataBlock, pHandle->pOrderInfo, pHandle->nullFirst);

        int64_t el = taosGetTimestampUs() - p;
        pHandle->sortElapsed += el;

        doAddToBuf(pHandle->pDataBlock, pHandle);
      }
    }

    if (pHandle->pDataBlock->info.rows > 0) {
      size_t size = blockDataGetSize(pHandle->pDataBlock);

      // Perform the in-memory sort and then flush data in the buffer into disk.
      blockDataSort(pHandle->pDataBlock, pHandle->pOrderInfo, pHandle->nullFirst);

      // All sorted data can fit in memory, external memory sort is not needed. Return to directly
      if (size <= sortBufSize) {
        pHandle->cmpParam.numOfSources = 1;
        pHandle->inMemSort = true;

        pHandle->tupleHandle.rowIndex  = -1;
        pHandle->tupleHandle.pBlock = pHandle->pDataBlock;
        return 0;
      } else {
        doAddToBuf(pHandle->pDataBlock, pHandle);
      }
    }

    taosMemoryFreeClear(source);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tsortOpen(SSortHandle* pHandle) {
  if (pHandle->opened) {
    return 0;
  }

  if (pHandle->fetchfp == NULL || pHandle->comparFn == NULL) {
    return -1;
  }

  pHandle->opened = true;

  int32_t code = createInitialSortedMultiSources(pHandle);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // do internal sort
  code = doInternalMergeSort(pHandle);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int32_t numOfSources = taosArrayGetSize(pHandle->pOrderedSource);
  if (pHandle->pBuf != NULL) {
    ASSERT(numOfSources <= getNumOfInMemBufPages(pHandle->pBuf));
  }

  code = sortComparInit(&pHandle->cmpParam, pHandle->pOrderedSource, 0, numOfSources - 1, pHandle);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = tMergeTreeCreate(&pHandle->pMergeTree, pHandle->cmpParam.numOfSources, &pHandle->cmpParam, pHandle->comparFn);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
}

int32_t tsortClose(SSortHandle* pHandle) {
  // do nothing
}

int32_t tsortSetFetchRawDataFp(SSortHandle* pHandle, _sort_fetch_block_fn_t fp) {
  pHandle->fetchfp = fp;
}

int32_t tsortSetComparFp(SSortHandle* pHandle, _sort_merge_compar_fn_t fp) {
  pHandle->comparFn = fp;
}

STupleHandle* tsortNextTuple(SSortHandle* pHandle) {
  if (pHandle->cmpParam.numOfSources == pHandle->numOfCompletedSources) {
    return NULL;
  }

  // All the data are hold in the buffer, no external sort is invoked.
  if (pHandle->inMemSort) {
    pHandle->tupleHandle.rowIndex += 1;
    if (pHandle->tupleHandle.rowIndex == pHandle->pDataBlock->info.rows) {
      pHandle->numOfCompletedSources = 1;
      return NULL;
    }

    return &pHandle->tupleHandle;
  }

  int32_t index = tMergeTreeGetChosenIndex(pHandle->pMergeTree);
  SExternalMemSource *pSource = pHandle->cmpParam.pSources[index];

  if (pHandle->needAdjust) {
    int32_t code = adjustMergeTreeForNextTuple(pSource, pHandle->pMergeTree, pHandle, &pHandle->numOfCompletedSources);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
      return NULL;
    }
  }

  if (pHandle->cmpParam.numOfSources == pHandle->numOfCompletedSources) {
    return NULL;
  }

  // Get the adjusted value after the loser tree is updated.
  index = tMergeTreeGetChosenIndex(pHandle->pMergeTree);
  pSource = pHandle->cmpParam.pSources[index];

  assert(pSource->src.pBlock != NULL);

  pHandle->tupleHandle.rowIndex = pSource->src.rowIndex;
  pHandle->tupleHandle.pBlock = pSource->src.pBlock;

  pHandle->needAdjust = true;
  pSource->src.rowIndex += 1;

  return &pHandle->tupleHandle;
}

bool tsortIsNullVal(STupleHandle* pVHandle, int32_t colIndex) {
  return false;
}

void* tsortGetValue(STupleHandle* pVHandle, int32_t colIndex) {
  SColumnInfoData* pColInfo = TARRAY_GET_ELEM(pVHandle->pBlock->pDataBlock, colIndex);
  return colDataGetData(pColInfo, pVHandle->rowIndex);
}
