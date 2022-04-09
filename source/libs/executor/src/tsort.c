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
#include "tcompare.h"

struct STupleHandle {
  SSDataBlock* pBlock;
  int32_t      rowIndex;
};

struct SSortHandle {
  int32_t           type;

  int32_t           pageSize;
  int32_t           numOfPages;
  SDiskbasedBuf    *pBuf;

  SArray           *pSortInfo;
  SArray           *pIndexMap;
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
};

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
SSortHandle* tsortCreateSortHandle(SArray* pSortInfo, SArray* pIndexMap, int32_t type, int32_t pageSize, int32_t numOfPages, SSDataBlock* pBlock, const char* idstr) {
  SSortHandle* pSortHandle = taosMemoryCalloc(1, sizeof(SSortHandle));

  pSortHandle->type       = type;
  pSortHandle->pageSize   = pageSize;
  pSortHandle->numOfPages = numOfPages;
  pSortHandle->pSortInfo  = pSortInfo;
  pSortHandle->pIndexMap  = pIndexMap;
  pSortHandle->pDataBlock = createOneDataBlock(pBlock);

  pSortHandle->pOrderedSource     = taosArrayInit(4, POINTER_BYTES);
  pSortHandle->cmpParam.orderInfo = pSortInfo;

  tsortSetComparFp(pSortHandle, msortComparFn);

  if (idstr != NULL) {
    pSortHandle->idStr    = strdup(idstr);
  }

  return pSortHandle;
}

static int32_t sortComparClearup(SMsortComparParam* cmpParam) {
  for(int32_t i = 0; i < cmpParam->numOfSources; ++i) {
    SSortSource* pSource = cmpParam->pSources[i];    // NOTICE: pSource may be SGenericSource *, if it is SORT_MULTISOURCE_MERGE
    blockDataDestroy(pSource->src.pBlock);
    taosMemoryFreeClear(pSource);
  }

  cmpParam->numOfSources = 0;
  return TSDB_CODE_SUCCESS;
}

void tsortDestroySortHandle(SSortHandle* pSortHandle) {
  tsortClose(pSortHandle);
  if (pSortHandle->pMergeTree != NULL) {
    tMergeTreeDestroy(pSortHandle->pMergeTree);
  }

  destroyDiskbasedBuf(pSortHandle->pBuf);
  taosMemoryFreeClear(pSortHandle->idStr);
  blockDataDestroy(pSortHandle->pDataBlock);
  for (size_t i = 0; i < taosArrayGetSize(pSortHandle->pOrderedSource); i++){
    SSortSource** pSource = taosArrayGet(pSortHandle->pOrderedSource, i);
    blockDataDestroy((*pSource)->src.pBlock);
    taosMemoryFreeClear(*pSource);
  }
  taosArrayDestroy(pSortHandle->pOrderedSource);
  taosMemoryFreeClear(pSortHandle);
}

int32_t tsortAddSource(SSortHandle* pSortHandle, void* pSource) {
  taosArrayPush(pSortHandle->pOrderedSource, &pSource);
  return TSDB_CODE_SUCCESS;
}

static int32_t doAddNewExternalMemSource(SDiskbasedBuf *pBuf, SArray* pAllSources, SSDataBlock* pBlock, int32_t* sourceId) {
  SSortSource* pSource = taosMemoryCalloc(1, sizeof(SSortSource));
  if (pSource == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  pSource->pageIdList = getDataBufPagesIdList(pBuf, (*sourceId));
  pSource->src.pBlock = pBlock;

  taosArrayPush(pAllSources, &pSource);

  (*sourceId) += 1;

  int32_t rowSize = blockDataGetSerialRowSize(pSource->src.pBlock);
  int32_t numOfRows = (getBufPageSize(pBuf) - blockDataGetSerialMetaSize(pBlock))/rowSize;  // The value of numOfRows must be greater than 0, which is guaranteed by the previous memory allocation
  ASSERT(numOfRows > 0);
  return blockDataEnsureCapacity(pSource->src.pBlock, numOfRows);
}

static int32_t doAddToBuf(SSDataBlock* pDataBlock, SSortHandle* pHandle) {
  int32_t start = 0;

  if (pHandle->pBuf == NULL) {
    int32_t code = createDiskbasedBuf(&pHandle->pBuf, pHandle->pageSize, pHandle->numOfPages * pHandle->pageSize, "doAddToBuf", "/tmp");
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
    void* pPage = getNewBufPage(pHandle->pBuf, pHandle->sourceId, &pageId);
    if (pPage == NULL) {
      blockDataDestroy(p);
      return terrno;
    }

    int32_t size = blockDataGetSize(p) + sizeof(int32_t)  + p->info.numOfCols * sizeof(int32_t);
    assert(size <= getBufPageSize(pHandle->pBuf));

    blockDataToBuf(pPage, p);

    setBufPageDirty(pPage, true);
    releaseBufPage(pHandle->pBuf, pPage);

    blockDataDestroy(p);
    start = stop + 1;
  }

  blockDataCleanup(pDataBlock);

  SSDataBlock* pBlock = createOneDataBlock(pDataBlock);
  return doAddNewExternalMemSource(pHandle->pBuf, pHandle->pOrderedSource, pBlock, &pHandle->sourceId);
}

static int32_t sortComparInit(SMsortComparParam* cmpParam, SArray* pSources, int32_t startIndex, int32_t endIndex, SSortHandle* pHandle) {
  cmpParam->pSources  = taosArrayGet(pSources, startIndex);
  cmpParam->numOfSources = (endIndex - startIndex + 1);

  int32_t code = 0;

  if (pHandle->type == SORT_SINGLESOURCE_SORT) {
    for (int32_t i = 0; i < cmpParam->numOfSources; ++i) {
      SSortSource* pSource = cmpParam->pSources[i];
      SPageInfo*          pPgInfo = *(SPageInfo**)taosArrayGet(pSource->pageIdList, pSource->pageIndex);

      void* pPage = getBufPage(pHandle->pBuf, getPageId(pPgInfo));
      code = blockDataFromBuf(pSource->src.pBlock, pPage);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      releaseBufPage(pHandle->pBuf, pPage);
    }
  } else {
    // multi-pass internal merge sort is required
    if (pHandle->pBuf == NULL) {
      code = createDiskbasedBuf(&pHandle->pBuf, pHandle->pageSize, pHandle->numOfPages * pHandle->pageSize, "sortComparInit", "/tmp");
      dBufSetPrintInfo(pHandle->pBuf);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    for (int32_t i = 0; i < cmpParam->numOfSources; ++i) {
      SSortSource* pSource = cmpParam->pSources[i];
      pSource->src.pBlock = pHandle->fetchfp(pSource->param);
    }
  }

  return code;
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

static int32_t adjustMergeTreeForNextTuple(SSortSource *pSource, SMultiwayMergeTreeInfo *pTree, SSortHandle *pHandle, int32_t* numOfCompleted) {
  /*
   * load a new SDataBlock into memory of a given intermediate data-set source,
   * since it's last record in buffer has been chosen to be processed, as the winner of loser-tree
   */
  if (pSource->src.rowIndex >= pSource->src.pBlock->info.rows) {
    pSource->src.rowIndex = 0;

    if (pHandle->type == SORT_SINGLESOURCE_SORT) {
      pSource->pageIndex ++;
      if (pSource->pageIndex >= taosArrayGetSize(pSource->pageIdList)) {
        (*numOfCompleted) += 1;
        pSource->src.rowIndex = -1;
        pSource->pageIndex = -1;
        pSource->src.pBlock = blockDataDestroy(pSource->src.pBlock);
      } else {
        SPageInfo* pPgInfo = *(SPageInfo**)taosArrayGet(pSource->pageIdList, pSource->pageIndex);

        void* pPage = getBufPage(pHandle->pBuf, getPageId(pPgInfo));
        int32_t    code = blockDataFromBuf(pSource->src.pBlock, pPage);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }

        releaseBufPage(pHandle->pBuf, pPage);
      }
    } else {
      pSource->src.pBlock = pHandle->fetchfp(((SSortSource*)pSource)->param);
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
  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* getSortedBlockDataInner(SSortHandle* pHandle, SMsortComparParam* cmpParam, int32_t capacity) {
  blockDataCleanup(pHandle->pDataBlock);

  while(1) {
    if (cmpParam->numOfSources == pHandle->numOfCompletedSources) {
      break;
    }

    int32_t index = tMergeTreeGetChosenIndex(pHandle->pMergeTree);

    SSortSource *pSource = (*cmpParam).pSources[index];
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

  SSortSource* pLeftSource  = pParam->pSources[pLeftIdx];
  SSortSource* pRightSource = pParam->pSources[pRightIdx];

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

    SColumnInfoData* pLeftColInfoData = TARRAY_GET_ELEM(pLeftBlock->pDataBlock, pOrder->slotId);

    bool leftNull  = false;
    if (pLeftColInfoData->hasNull) {
      leftNull = colDataIsNull(pLeftColInfoData, pLeftBlock->info.rows, pLeftSource->src.rowIndex, pLeftBlock->pBlockAgg);
    }

    SColumnInfoData* pRightColInfoData = TARRAY_GET_ELEM(pRightBlock->pDataBlock, pOrder->slotId);
    bool rightNull = false;
    if (pRightColInfoData->hasNull) {
      rightNull = colDataIsNull(pRightColInfoData, pRightBlock->info.rows, pRightSource->src.rowIndex, pRightBlock->pBlockAgg);
    }

    if (leftNull && rightNull) {
      continue; // continue to next slot
    }

    if (rightNull) {
      return pOrder->nullFirst? 1:-1;
    }

    if (leftNull) {
      return pOrder->nullFirst? -1:1;
    }

    void* left1  = colDataGetData(pLeftColInfoData, pLeftSource->src.rowIndex);
    void* right1 = colDataGetData(pRightColInfoData, pRightSource->src.rowIndex);

    __compar_fn_t fn = getKeyComparFunc(pLeftColInfoData->info.type, pOrder->order);

    int ret = fn(left1, right1);
    if (ret == 0) {
      continue;
    } else {
      return ret;
    }
  }
  return 0;
}

static int32_t doInternalMergeSort(SSortHandle* pHandle) {
  size_t numOfSources = taosArrayGetSize(pHandle->pOrderedSource);
  if (numOfSources == 0) {
    return 0;
  }

  // Calculate the I/O counts to complete the data sort.
  double sortPass = floorl(log2(numOfSources) / log2(pHandle->numOfPages));

  pHandle->totalElapsed = taosGetTimestampUs() - pHandle->startTs;
  qDebug("%s %d rounds mergesort required to complete the sort, first-round sorted data size:%"PRIzu", sort elapsed:%"PRId64", total elapsed:%"PRId64,
         pHandle->idStr, (int32_t) (sortPass + 1), getTotalBufSize(pHandle->pBuf), pHandle->sortElapsed, pHandle->totalElapsed);

  int32_t numOfRows = blockDataGetCapacityInRow(pHandle->pDataBlock, pHandle->pageSize);
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
        SSDataBlock* pDataBlock = getSortedBlockDataInner(pHandle, &pHandle->cmpParam, numOfRows);
        if (pDataBlock == NULL) {
          break;
        }

        int32_t pageId = -1;
        void* pPage = getNewBufPage(pHandle->pBuf, pHandle->sourceId, &pageId);
        if (pPage == NULL) {
          return terrno;
        }

        int32_t size = blockDataGetSize(pDataBlock) + sizeof(int32_t) + pDataBlock->info.numOfCols * sizeof(int32_t);
        assert(size <= getBufPageSize(pHandle->pBuf));

        blockDataToBuf(pPage, pDataBlock);

        setBufPageDirty(pPage, true);
        releaseBufPage(pHandle->pBuf, pPage);

        blockDataCleanup(pDataBlock);
      }

      sortComparClearup(&pHandle->cmpParam);
      tMergeTreeDestroy(pHandle->pMergeTree);
      pHandle->numOfCompletedSources = 0;

      SSDataBlock* pBlock = createOneDataBlock(pHandle->pDataBlock);
      code = doAddNewExternalMemSource(pHandle->pBuf, pResList, pBlock, &pHandle->sourceId);
      if (code != 0) {
        return code;
      }
    }

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
    SSortSource* source = taosArrayGetP(pHandle->pOrderedSource, 0);
    taosArrayClear(pHandle->pOrderedSource);
    while (1) {
      SSDataBlock* pBlock = pHandle->fetchfp(source->param);
      if (pBlock == NULL) {
        break;
      }

      if (pHandle->pDataBlock == NULL) {
        pHandle->pDataBlock = createOneDataBlock(pBlock);
      }

      int32_t code = blockDataMerge(pHandle->pDataBlock, pBlock, pHandle->pIndexMap);
      if (code != 0) {
        return code;
      }

      size_t size = blockDataGetSize(pHandle->pDataBlock);
      if (size > sortBufSize) {
        // Perform the in-memory sort and then flush data in the buffer into disk.
        int64_t p = taosGetTimestampUs();
        blockDataSort(pHandle->pDataBlock, pHandle->pSortInfo);

        int64_t el = taosGetTimestampUs() - p;
        pHandle->sortElapsed += el;

        doAddToBuf(pHandle->pDataBlock, pHandle);
      }
    }

    if (pHandle->pDataBlock->info.rows > 0) {
      size_t size = blockDataGetSize(pHandle->pDataBlock);

      // Perform the in-memory sort and then flush data in the buffer into disk.
      int64_t p = taosGetTimestampUs();

      blockDataSort(pHandle->pDataBlock, pHandle->pSortInfo);

      int64_t el = taosGetTimestampUs() - p;
      pHandle->sortElapsed += el;

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

  if (numOfSources == 0) {
    return 0;
  }

  code = sortComparInit(&pHandle->cmpParam, pHandle->pOrderedSource, 0, numOfSources - 1, pHandle);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  return tMergeTreeCreate(&pHandle->pMergeTree, pHandle->cmpParam.numOfSources, &pHandle->cmpParam, pHandle->comparFn);
}

int32_t tsortClose(SSortHandle* pHandle) {
  // do nothing
  return TSDB_CODE_SUCCESS;
}

int32_t tsortSetFetchRawDataFp(SSortHandle* pHandle, _sort_fetch_block_fn_t fp) {
  pHandle->fetchfp = fp;
  return TSDB_CODE_SUCCESS;
}

int32_t tsortSetComparFp(SSortHandle* pHandle, _sort_merge_compar_fn_t fp) {
  pHandle->comparFn = fp;
  return TSDB_CODE_SUCCESS;
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
  SSortSource *pSource = pHandle->cmpParam.pSources[index];

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
  SColumnInfoData* pColInfoSrc = taosArrayGet(pVHandle->pBlock->pDataBlock, colIndex);
  return colDataIsNull(pColInfoSrc, 0, pVHandle->rowIndex, NULL);
}

void* tsortGetValue(STupleHandle* pVHandle, int32_t colIndex) {
  SColumnInfoData* pColInfo = TARRAY_GET_ELEM(pVHandle->pBlock->pDataBlock, colIndex);
  return colDataGetData(pColInfo, pVHandle->rowIndex);
}
