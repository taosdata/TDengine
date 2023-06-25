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

#include "query.h"
#include "tcommon.h"

#include "tcompare.h"
#include "tdatablock.h"
#include "tdef.h"
#include "tlosertree.h"
#include "tpagedbuf.h"
#include "tsort.h"
#include "tutil.h"

struct STupleHandle {
  SSDataBlock* pBlock;
  int32_t      rowIndex;
};

struct SSortHandle {
  int32_t        type;
  int32_t        pageSize;
  int32_t        numOfPages;
  SDiskbasedBuf* pBuf;
  SArray*        pSortInfo;
  SArray*        pOrderedSource;
  int32_t        loops;
  uint64_t       sortElapsed;
  int64_t        startTs;
  uint64_t       totalElapsed;

  int32_t           sourceId;
  SSDataBlock*      pDataBlock;
  SMsortComparParam cmpParam;
  int32_t           numOfCompletedSources;
  bool              opened;
  const char*       idStr;
  bool              inMemSort;
  bool              needAdjust;
  STupleHandle      tupleHandle;
  void*             param;
  void (*beforeFp)(SSDataBlock* pBlock, void* param);

  _sort_fetch_block_fn_t  fetchfp;
  _sort_merge_compar_fn_t comparFn;
  SMultiwayMergeTreeInfo* pMergeTree;
};

static int32_t msortComparFn(const void* pLeft, const void* pRight, void* param);

SSDataBlock* tsortGetSortedDataBlock(const SSortHandle* pSortHandle) {
  return createOneDataBlock(pSortHandle->pDataBlock, false);
}

/**
 *
 * @param type
 * @return
 */
SSortHandle* tsortCreateSortHandle(SArray* pSortInfo, int32_t type, int32_t pageSize, int32_t numOfPages,
                                   SSDataBlock* pBlock, const char* idstr) {
  SSortHandle* pSortHandle = taosMemoryCalloc(1, sizeof(SSortHandle));

  pSortHandle->type = type;
  pSortHandle->pageSize = pageSize;
  pSortHandle->numOfPages = numOfPages;
  pSortHandle->pSortInfo = pSortInfo;
  pSortHandle->loops = 0;

  if (pBlock != NULL) {
    pSortHandle->pDataBlock = createOneDataBlock(pBlock, false);
  }

  pSortHandle->pOrderedSource = taosArrayInit(4, POINTER_BYTES);
  pSortHandle->cmpParam.orderInfo = pSortInfo;
  pSortHandle->cmpParam.cmpGroupId = false;

  tsortSetComparFp(pSortHandle, msortComparFn);

  if (idstr != NULL) {
    pSortHandle->idStr = taosStrdup(idstr);
  }

  return pSortHandle;
}

static int32_t sortComparCleanup(SMsortComparParam* cmpParam) {
  // NOTICE: pSource may be, if it is SORT_MULTISOURCE_MERGE
  for (int32_t i = 0; i < cmpParam->numOfSources; ++i) {
    SSortSource* pSource = cmpParam->pSources[i];
    blockDataDestroy(pSource->src.pBlock);
    if (pSource->pageIdList) {
      taosArrayDestroy(pSource->pageIdList);
    }
    taosMemoryFreeClear(pSource);
    cmpParam->pSources[i] = NULL;
  }

  cmpParam->numOfSources = 0;
  return TSDB_CODE_SUCCESS;
}

void tsortClearOrderdSource(SArray* pOrderedSource, int64_t *fetchUs, int64_t *fetchNum) {
  for (size_t i = 0; i < taosArrayGetSize(pOrderedSource); i++) {
    SSortSource** pSource = taosArrayGet(pOrderedSource, i);
    if (NULL == *pSource) {
      continue;
    }

    if (fetchUs) {
      *fetchUs += (*pSource)->fetchUs;
      *fetchNum += (*pSource)->fetchNum;
    }
    
    // release pageIdList
    if ((*pSource)->pageIdList) {
      taosArrayDestroy((*pSource)->pageIdList);
      (*pSource)->pageIdList = NULL;
    }
    if ((*pSource)->param && !(*pSource)->onlyRef) {
      taosMemoryFree((*pSource)->param);
      (*pSource)->param = NULL;
    }

    if (!(*pSource)->onlyRef && (*pSource)->src.pBlock) {
      blockDataDestroy((*pSource)->src.pBlock);
      (*pSource)->src.pBlock = NULL;
    }

    taosMemoryFreeClear(*pSource);
  }

  taosArrayClear(pOrderedSource);
}

void tsortDestroySortHandle(SSortHandle* pSortHandle) {
  if (pSortHandle == NULL) {
    return;
  }

  tsortClose(pSortHandle);
  if (pSortHandle->pMergeTree != NULL) {
    tMergeTreeDestroy(pSortHandle->pMergeTree);
  }

  destroyDiskbasedBuf(pSortHandle->pBuf);
  taosMemoryFreeClear(pSortHandle->idStr);
  blockDataDestroy(pSortHandle->pDataBlock);

  int64_t fetchUs = 0, fetchNum = 0;
  tsortClearOrderdSource(pSortHandle->pOrderedSource, &fetchUs, &fetchNum);
  qDebug("all source fetch time: %" PRId64 "us num:%" PRId64 " %s", fetchUs, fetchNum, pSortHandle->idStr);
  
  taosArrayDestroy(pSortHandle->pOrderedSource);
  taosMemoryFreeClear(pSortHandle);
}

int32_t tsortAddSource(SSortHandle* pSortHandle, void* pSource) {
  taosArrayPush(pSortHandle->pOrderedSource, &pSource);
  return TSDB_CODE_SUCCESS;
}

static int32_t doAddNewExternalMemSource(SDiskbasedBuf* pBuf, SArray* pAllSources, SSDataBlock* pBlock,
                                         int32_t* sourceId, SArray* pPageIdList) {
  SSortSource* pSource = taosMemoryCalloc(1, sizeof(SSortSource));
  if (pSource == NULL) {
    taosArrayDestroy(pPageIdList);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSource->src.pBlock = pBlock;
  pSource->pageIdList = pPageIdList;
  taosArrayPush(pAllSources, &pSource);

  (*sourceId) += 1;

  int32_t rowSize = blockDataGetSerialRowSize(pSource->src.pBlock);

  // The value of numOfRows must be greater than 0, which is guaranteed by the previous memory allocation
  int32_t numOfRows =
      (getBufPageSize(pBuf) - blockDataGetSerialMetaSize(taosArrayGetSize(pBlock->pDataBlock))) / rowSize;
  ASSERT(numOfRows > 0);

  return blockDataEnsureCapacity(pSource->src.pBlock, numOfRows);
}

static int32_t doAddToBuf(SSDataBlock* pDataBlock, SSortHandle* pHandle) {
  int32_t start = 0;

  if (pHandle->pBuf == NULL) {
    if (!osTempSpaceAvailable()) {
      terrno = TSDB_CODE_NO_DISKSPACE;
      qError("Add to buf failed since %s, tempDir:%s", terrstr(), tsTempDir);
      return terrno;
    }

    int32_t code = createDiskbasedBuf(&pHandle->pBuf, pHandle->pageSize, pHandle->numOfPages * pHandle->pageSize,
                                      "sortExternalBuf", tsTempDir);
    dBufSetPrintInfo(pHandle->pBuf);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  SArray* pPageIdList = taosArrayInit(4, sizeof(int32_t));
  while (start < pDataBlock->info.rows) {
    int32_t stop = 0;
    blockDataSplitRows(pDataBlock, pDataBlock->info.hasVarCol, start, &stop, pHandle->pageSize);
    SSDataBlock* p = blockDataExtractBlock(pDataBlock, start, stop - start + 1);
    if (p == NULL) {
      taosArrayDestroy(pPageIdList);
      return terrno;
    }

    int32_t pageId = -1;
    void*   pPage = getNewBufPage(pHandle->pBuf, &pageId);
    if (pPage == NULL) {
      taosArrayDestroy(pPageIdList);
      blockDataDestroy(p);
      return terrno;
    }

    taosArrayPush(pPageIdList, &pageId);

    int32_t size = blockDataGetSize(p) + sizeof(int32_t) + taosArrayGetSize(p->pDataBlock) * sizeof(int32_t);
    ASSERT(size <= getBufPageSize(pHandle->pBuf));

    blockDataToBuf(pPage, p);

    setBufPageDirty(pPage, true);
    releaseBufPage(pHandle->pBuf, pPage);

    blockDataDestroy(p);
    start = stop + 1;
  }

  blockDataCleanup(pDataBlock);

  SSDataBlock* pBlock = createOneDataBlock(pDataBlock, false);
  return doAddNewExternalMemSource(pHandle->pBuf, pHandle->pOrderedSource, pBlock, &pHandle->sourceId, pPageIdList);
}

static void setCurrentSourceDone(SSortSource* pSource, SSortHandle* pHandle) {
  pSource->src.rowIndex = -1;
  ++pHandle->numOfCompletedSources;
}

static int32_t sortComparInit(SMsortComparParam* pParam, SArray* pSources, int32_t startIndex, int32_t endIndex,
                              SSortHandle* pHandle) {
  pParam->pSources = taosArrayGet(pSources, startIndex);
  pParam->numOfSources = (endIndex - startIndex + 1);

  int32_t code = 0;

  // multi-pass internal merge sort is required
  if (pHandle->pBuf == NULL) {
    if (!osTempSpaceAvailable()) {
      code = terrno = TSDB_CODE_NO_DISKSPACE;
      qError("Sort compare init failed since %s, tempDir:%s, idStr:%s", terrstr(), tsTempDir, pHandle->idStr);
      return code;
    }

    code = createDiskbasedBuf(&pHandle->pBuf, pHandle->pageSize, pHandle->numOfPages * pHandle->pageSize,
                              "sortComparInit", tsTempDir);
    dBufSetPrintInfo(pHandle->pBuf);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
      return code;
    }
  }

  if (pHandle->type == SORT_SINGLESOURCE_SORT) {
    for (int32_t i = 0; i < pParam->numOfSources; ++i) {
      SSortSource* pSource = pParam->pSources[i];

      // set current source is done
      if (taosArrayGetSize(pSource->pageIdList) == 0) {
        setCurrentSourceDone(pSource, pHandle);
        continue;
      }

      int32_t* pPgId = taosArrayGet(pSource->pageIdList, pSource->pageIndex);

      void* pPage = getBufPage(pHandle->pBuf, *pPgId);
      if (NULL == pPage) {
        return terrno;
      }
      
      code = blockDataFromBuf(pSource->src.pBlock, pPage);
      if (code != TSDB_CODE_SUCCESS) {
        terrno = code;
        return code;
      }

      releaseBufPage(pHandle->pBuf, pPage);
    }
  } else {
    qDebug("start init for the multiway merge sort, %s", pHandle->idStr);
    int64_t st = taosGetTimestampUs();

    for (int32_t i = 0; i < pParam->numOfSources; ++i) {
      SSortSource* pSource = pParam->pSources[i];
      pSource->src.pBlock = pHandle->fetchfp(pSource->param);

      // set current source is done
      if (pSource->src.pBlock == NULL) {
        setCurrentSourceDone(pSource, pHandle);
      }
    }

    int64_t et = taosGetTimestampUs();
    qDebug("init for merge sort completed, elapsed time:%.2f ms, %s", (et - st) / 1000.0, pHandle->idStr);
  }

  return code;
}

static void appendOneRowToDataBlock(SSDataBlock* pBlock, const SSDataBlock* pSource, int32_t* rowIndex) {
  for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);

    SColumnInfoData* pSrcColInfo = taosArrayGet(pSource->pDataBlock, i);
    bool             isNull = colDataIsNull(pSrcColInfo, pSource->info.rows, *rowIndex, NULL);

    if (isNull) {
      colDataSetVal(pColInfo, pBlock->info.rows, NULL, true);
    } else {
      char* pData = colDataGetData(pSrcColInfo, *rowIndex);
      colDataSetVal(pColInfo, pBlock->info.rows, pData, false);
    }
  }

  pBlock->info.rows += 1;
  *rowIndex += 1;
}

static int32_t adjustMergeTreeForNextTuple(SSortSource* pSource, SMultiwayMergeTreeInfo* pTree, SSortHandle* pHandle,
                                           int32_t* numOfCompleted) {
  /*
   * load a new SDataBlock into memory of a given intermediate data-set source,
   * since it's last record in buffer has been chosen to be processed, as the winner of loser-tree
   */
  if (pSource->src.rowIndex >= pSource->src.pBlock->info.rows) {
    pSource->src.rowIndex = 0;

    if (pHandle->type == SORT_SINGLESOURCE_SORT) {
      pSource->pageIndex++;
      if (pSource->pageIndex >= taosArrayGetSize(pSource->pageIdList)) {
        (*numOfCompleted) += 1;
        pSource->src.rowIndex = -1;
        pSource->pageIndex = -1;
        pSource->src.pBlock = blockDataDestroy(pSource->src.pBlock);
      } else {
        int32_t* pPgId = taosArrayGet(pSource->pageIdList, pSource->pageIndex);

        void*   pPage = getBufPage(pHandle->pBuf, *pPgId);
        if (pPage == NULL) {
          qError("failed to get buffer, code:%s", tstrerror(terrno));
          return terrno;
        }

        int32_t code = blockDataFromBuf(pSource->src.pBlock, pPage);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }

        releaseBufPage(pHandle->pBuf, pPage);
      }
    } else {
      int64_t st = taosGetTimestampUs();      
      pSource->src.pBlock = pHandle->fetchfp(((SSortSource*)pSource)->param);
      pSource->fetchUs += taosGetTimestampUs() - st;
      pSource->fetchNum++;
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

  while (1) {
    if (cmpParam->numOfSources == pHandle->numOfCompletedSources) {
      break;
    }

    int32_t index = tMergeTreeGetChosenIndex(pHandle->pMergeTree);

    SSortSource* pSource = (*cmpParam).pSources[index];
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

  return (pHandle->pDataBlock->info.rows > 0) ? pHandle->pDataBlock : NULL;
}

int32_t msortComparFn(const void* pLeft, const void* pRight, void* param) {
  int32_t pLeftIdx = *(int32_t*)pLeft;
  int32_t pRightIdx = *(int32_t*)pRight;

  SMsortComparParam* pParam = (SMsortComparParam*)param;

  SArray* pInfo = pParam->orderInfo;

  SSortSource* pLeftSource = pParam->pSources[pLeftIdx];
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

  if (pParam->cmpGroupId) {
    if (pLeftBlock->info.id.groupId != pRightBlock->info.id.groupId) {
      return pLeftBlock->info.id.groupId < pRightBlock->info.id.groupId ? -1 : 1;
    }
  }

  for (int32_t i = 0; i < pInfo->size; ++i) {
    SBlockOrderInfo* pOrder = TARRAY_GET_ELEM(pInfo, i);
    SColumnInfoData* pLeftColInfoData = TARRAY_GET_ELEM(pLeftBlock->pDataBlock, pOrder->slotId);

    bool leftNull = false;
    if (pLeftColInfoData->hasNull) {
      if (pLeftBlock->pBlockAgg == NULL) {
        leftNull = colDataIsNull_s(pLeftColInfoData, pLeftSource->src.rowIndex);
      } else {
        leftNull =
            colDataIsNull(pLeftColInfoData, pLeftBlock->info.rows, pLeftSource->src.rowIndex, pLeftBlock->pBlockAgg[i]);
      }
    }

    SColumnInfoData* pRightColInfoData = TARRAY_GET_ELEM(pRightBlock->pDataBlock, pOrder->slotId);
    bool             rightNull = false;
    if (pRightColInfoData->hasNull) {
      if (pRightBlock->pBlockAgg == NULL) {
        rightNull = colDataIsNull_s(pRightColInfoData, pRightSource->src.rowIndex);
      } else {
        rightNull = colDataIsNull(pRightColInfoData, pRightBlock->info.rows, pRightSource->src.rowIndex,
                                  pRightBlock->pBlockAgg[i]);
      }
    }

    if (leftNull && rightNull) {
      continue;  // continue to next slot
    }

    if (rightNull) {
      return pOrder->nullFirst ? 1 : -1;
    }

    if (leftNull) {
      return pOrder->nullFirst ? -1 : 1;
    }

    void* left1 = colDataGetData(pLeftColInfoData, pLeftSource->src.rowIndex);
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

  if (sortPass > 0) {
    size_t s = pHandle->pBuf ? getTotalBufSize(pHandle->pBuf) : 0;
    qDebug("%s %d rounds mergesort required to complete the sort, first-round sorted data size:%" PRIzu
           ", sort elapsed:%" PRId64 ", total elapsed:%" PRId64,
           pHandle->idStr, (int32_t)(sortPass + 1), s, pHandle->sortElapsed, pHandle->totalElapsed);
  } else {
    qDebug("%s ordered source:%" PRIzu ", available buf:%d, no need internal sort", pHandle->idStr, numOfSources,
           pHandle->numOfPages);
  }

  int32_t numOfRows = blockDataGetCapacityInRow(pHandle->pDataBlock, pHandle->pageSize,
                                                blockDataGetSerialMetaSize(taosArrayGetSize(pHandle->pDataBlock->pDataBlock)));
  blockDataEnsureCapacity(pHandle->pDataBlock, numOfRows);

  // the initial pass + sortPass + final mergePass
  pHandle->loops = sortPass + 2;

  size_t numOfSorted = taosArrayGetSize(pHandle->pOrderedSource);
  for (int32_t t = 0; t < sortPass; ++t) {
    int64_t st = taosGetTimestampUs();

    SArray* pResList = taosArrayInit(4, POINTER_BYTES);

    int32_t numOfInputSources = pHandle->numOfPages;
    int32_t sortGroup = (numOfSorted + numOfInputSources - 1) / numOfInputSources;

    // Only *numOfInputSources* can be loaded into buffer to perform the external sort.
    for (int32_t i = 0; i < sortGroup; ++i) {
      pHandle->sourceId += 1;

      int32_t end = (i + 1) * numOfInputSources - 1;
      if (end > numOfSorted - 1) {
        end = numOfSorted - 1;
      }

      pHandle->cmpParam.numOfSources = end - i * numOfInputSources + 1;

      int32_t code = sortComparInit(&pHandle->cmpParam, pHandle->pOrderedSource, i * numOfInputSources, end, pHandle);
      if (code != TSDB_CODE_SUCCESS) {
        taosArrayDestroy(pResList);
        return code;
      }

      code =
          tMergeTreeCreate(&pHandle->pMergeTree, pHandle->cmpParam.numOfSources, &pHandle->cmpParam, pHandle->comparFn);
      if (code != TSDB_CODE_SUCCESS) {
        taosArrayDestroy(pResList);
        return code;
      }

      SArray* pPageIdList = taosArrayInit(4, sizeof(int32_t));
      while (1) {
        SSDataBlock* pDataBlock = getSortedBlockDataInner(pHandle, &pHandle->cmpParam, numOfRows);
        if (pDataBlock == NULL) {
          break;
        }

        int32_t pageId = -1;
        void*   pPage = getNewBufPage(pHandle->pBuf, &pageId);
        if (pPage == NULL) {
          taosArrayDestroy(pResList);
          taosArrayDestroy(pPageIdList);
          return terrno;
        }

        taosArrayPush(pPageIdList, &pageId);

        int32_t size =
            blockDataGetSize(pDataBlock) + sizeof(int32_t) + taosArrayGetSize(pDataBlock->pDataBlock) * sizeof(int32_t);
        ASSERT(size <= getBufPageSize(pHandle->pBuf));

        blockDataToBuf(pPage, pDataBlock);

        setBufPageDirty(pPage, true);
        releaseBufPage(pHandle->pBuf, pPage);

        blockDataCleanup(pDataBlock);
      }

      sortComparCleanup(&pHandle->cmpParam);
      tMergeTreeDestroy(pHandle->pMergeTree);
      pHandle->numOfCompletedSources = 0;

      SSDataBlock* pBlock = createOneDataBlock(pHandle->pDataBlock, false);
      code = doAddNewExternalMemSource(pHandle->pBuf, pResList, pBlock, &pHandle->sourceId, pPageIdList);
      if (code != TSDB_CODE_SUCCESS) {
        taosArrayDestroy(pResList);
        return code;
      }
    }

    tsortClearOrderdSource(pHandle->pOrderedSource, NULL, NULL);
    taosArrayAddAll(pHandle->pOrderedSource, pResList);
    taosArrayDestroy(pResList);

    numOfSorted = taosArrayGetSize(pHandle->pOrderedSource);

    int64_t el = taosGetTimestampUs() - st;
    pHandle->totalElapsed += el;

    SDiskbasedBufStatis statis = getDBufStatis(pHandle->pBuf);
    qDebug("%s %d round mergesort, elapsed:%" PRId64 " readDisk:%.2f Kb, flushDisk:%.2f Kb", pHandle->idStr, t + 1, el,
           statis.loadBytes / 1024.0, statis.flushBytes / 1024.0);

    if (pHandle->type == SORT_MULTISOURCE_MERGE) {
      pHandle->type = SORT_SINGLESOURCE_SORT;
      pHandle->comparFn = msortComparFn;
    }
  }

  pHandle->cmpParam.numOfSources = taosArrayGetSize(pHandle->pOrderedSource);
  return 0;
}

// get sort page size
int32_t getProperSortPageSize(size_t rowSize, uint32_t numOfCols) {
  uint32_t pgSize = rowSize * 4 + blockDataGetSerialMetaSize(numOfCols);
  if (pgSize < DEFAULT_PAGESIZE) {
    return DEFAULT_PAGESIZE;
  }

  return pgSize;
}

static int32_t createInitialSources(SSortHandle* pHandle) {
  size_t sortBufSize = pHandle->numOfPages * pHandle->pageSize;
  int32_t code = 0;

  if (pHandle->type == SORT_SINGLESOURCE_SORT) {
    SSortSource** pSource = taosArrayGet(pHandle->pOrderedSource, 0);
    SSortSource*  source = *pSource;
    *pSource = NULL;

    tsortClearOrderdSource(pHandle->pOrderedSource, NULL, NULL);

    while (1) {
      SSDataBlock* pBlock = pHandle->fetchfp(source->param);
      if (pBlock == NULL) {
        break;
      }

      if (pHandle->pDataBlock == NULL) {
        uint32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
        pHandle->pageSize = getProperSortPageSize(blockDataGetRowSize(pBlock), numOfCols);

        // todo, number of pages are set according to the total available sort buffer
        pHandle->numOfPages = 1024;
        sortBufSize = pHandle->numOfPages * pHandle->pageSize;
        pHandle->pDataBlock = createOneDataBlock(pBlock, false);
      }

      if (pHandle->beforeFp != NULL) {
        pHandle->beforeFp(pBlock, pHandle->param);
      }

      code = blockDataMerge(pHandle->pDataBlock, pBlock);
      if (code != TSDB_CODE_SUCCESS) {
        if (source->param && !source->onlyRef) {
          taosMemoryFree(source->param);
        }
        if (!source->onlyRef && source->src.pBlock) {
          blockDataDestroy(source->src.pBlock);
          source->src.pBlock = NULL;
        }
        taosMemoryFree(source);
        return code;
      }

      size_t size = blockDataGetSize(pHandle->pDataBlock);
      if (size > sortBufSize) {
        // Perform the in-memory sort and then flush data in the buffer into disk.
        int64_t p = taosGetTimestampUs();
        code = blockDataSort(pHandle->pDataBlock, pHandle->pSortInfo);
        if (code != 0) {
          if (source->param && !source->onlyRef) {
            taosMemoryFree(source->param);
          }
          if (!source->onlyRef && source->src.pBlock) {
            blockDataDestroy(source->src.pBlock);
            source->src.pBlock = NULL;
          }

          taosMemoryFree(source);
          return code;
        }

        int64_t el = taosGetTimestampUs() - p;
        pHandle->sortElapsed += el;

        code = doAddToBuf(pHandle->pDataBlock, pHandle);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
    }

    if (source->param && !source->onlyRef) {
      taosMemoryFree(source->param);
    }

    taosMemoryFree(source);

    if (pHandle->pDataBlock != NULL && pHandle->pDataBlock->info.rows > 0) {
      size_t size = blockDataGetSize(pHandle->pDataBlock);

      // Perform the in-memory sort and then flush data in the buffer into disk.
      int64_t p = taosGetTimestampUs();

      code = blockDataSort(pHandle->pDataBlock, pHandle->pSortInfo);
      if (code != 0) {
        return code;
      }

      int64_t el = taosGetTimestampUs() - p;
      pHandle->sortElapsed += el;

      // All sorted data can fit in memory, external memory sort is not needed. Return to directly
      if (size <= sortBufSize && pHandle->pBuf == NULL) {
        pHandle->cmpParam.numOfSources = 1;
        pHandle->inMemSort = true;

        pHandle->loops = 1;
        pHandle->tupleHandle.rowIndex = -1;
        pHandle->tupleHandle.pBlock = pHandle->pDataBlock;
        return 0;
      } else {
        code = doAddToBuf(pHandle->pDataBlock, pHandle);
      }
    }
  }

  return code;
}

int32_t tsortOpen(SSortHandle* pHandle) {
  if (pHandle->opened) {
    return 0;
  }

  if (pHandle->fetchfp == NULL || pHandle->comparFn == NULL) {
    return -1;
  }

  pHandle->opened = true;

  int32_t code = createInitialSources(pHandle);
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

int32_t tsortSetFetchRawDataFp(SSortHandle* pHandle, _sort_fetch_block_fn_t fetchFp, void (*fp)(SSDataBlock*, void*),
                               void* param) {
  pHandle->fetchfp = fetchFp;
  pHandle->beforeFp = fp;
  pHandle->param = param;
  return TSDB_CODE_SUCCESS;
}

int32_t tsortSetComparFp(SSortHandle* pHandle, _sort_merge_compar_fn_t fp) {
  pHandle->comparFn = fp;
  return TSDB_CODE_SUCCESS;
}

int32_t tsortSetCompareGroupId(SSortHandle* pHandle, bool compareGroupId) {
  pHandle->cmpParam.cmpGroupId = compareGroupId;
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

  int32_t      index = tMergeTreeGetChosenIndex(pHandle->pMergeTree);
  SSortSource* pSource = pHandle->cmpParam.pSources[index];

  if (pHandle->needAdjust) {
    int32_t code = adjustMergeTreeForNextTuple(pSource, pHandle->pMergeTree, pHandle, &pHandle->numOfCompletedSources);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
      return NULL;
    }
  }

  // all sources are completed.
  if (pHandle->cmpParam.numOfSources == pHandle->numOfCompletedSources) {
    return NULL;
  }

  // Get the adjusted value after the loser tree is updated.
  index = tMergeTreeGetChosenIndex(pHandle->pMergeTree);
  pSource = pHandle->cmpParam.pSources[index];

  ASSERT(pSource->src.pBlock != NULL);

  pHandle->tupleHandle.rowIndex = pSource->src.rowIndex;
  pHandle->tupleHandle.pBlock = pSource->src.pBlock;

  pHandle->needAdjust = true;
  pSource->src.rowIndex += 1;

  return &pHandle->tupleHandle;
}

bool tsortIsNullVal(STupleHandle* pVHandle, int32_t colIndex) {
  SColumnInfoData* pColInfoSrc = taosArrayGet(pVHandle->pBlock->pDataBlock, colIndex);
  return colDataIsNull_s(pColInfoSrc, pVHandle->rowIndex);
}

void* tsortGetValue(STupleHandle* pVHandle, int32_t colIndex) {
  SColumnInfoData* pColInfo = TARRAY_GET_ELEM(pVHandle->pBlock->pDataBlock, colIndex);
  if (pColInfo->pData == NULL) {
    return NULL;
  } else {
    return colDataGetData(pColInfo, pVHandle->rowIndex);
  }
}

uint64_t tsortGetGroupId(STupleHandle* pVHandle) { return pVHandle->pBlock->info.id.groupId; }
void*    tsortGetBlockInfo(STupleHandle* pVHandle) { return &pVHandle->pBlock->info; }

SSortExecInfo tsortGetSortExecInfo(SSortHandle* pHandle) {
  SSortExecInfo info = {0};

  if (pHandle == NULL) {
    info.sortMethod = SORT_QSORT_T;  // by default
    info.sortBuffer = 2 * 1048576;   // 2mb by default
  } else {
    info.sortBuffer = pHandle->pageSize * pHandle->numOfPages;
    info.sortMethod = pHandle->inMemSort ? SORT_QSORT_T : SORT_SPILLED_MERGE_SORT_T;
    info.loops = pHandle->loops;

    if (pHandle->pBuf != NULL) {
      SDiskbasedBufStatis st = getDBufStatis(pHandle->pBuf);
      info.writeBytes = st.flushBytes;
      info.readBytes = st.loadBytes;
    }
  }

  return info;
}
