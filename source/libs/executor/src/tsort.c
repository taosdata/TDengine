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
#include "theap.h"
#include "tlosertree.h"
#include "tpagedbuf.h"
#include "tsort.h"
#include "tutil.h"
#include "tsimplehash.h"
#include "executil.h"

struct STupleHandle {
  SSDataBlock* pBlock;
  int32_t      rowIndex;
};

typedef struct SSortMemFileRegion {
  int64_t fileOffset;
  int32_t regionSize;

  int32_t bufRegOffset;
  int32_t bufLen;
  char*   buf;
} SSortMemFileRegion;

typedef struct SSortMemFile {
  char*   writeBuf;
  int32_t writeBufSize;
  int64_t writeFileOffset;

  int32_t currRegionId;
  int32_t currRegionOffset;
  bool    bRegionDirty;

  SArray* aFileRegions;
  int32_t cacheSize;
  int32_t blockSize;

  FILE* pTdFile;
  char  memFilePath[PATH_MAX];
} SSortMemFile;

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

  uint64_t         pqMaxRows;
  uint32_t         pqMaxTupleLength;
  uint32_t         pqSortBufSize;
  bool             forceUsePQSort;
  BoundedQueue*    pBoundedQueue;
  uint32_t         tmpRowIdx;

  int64_t          mergeLimit;
  int64_t          currMergeLimitTs;          

  int32_t           sourceId;
  SSDataBlock*      pDataBlock;
  SMsortComparParam cmpParam;
  int32_t           numOfCompletedSources;
  bool              opened;
  int8_t            closed;
  const char*       idStr;
  bool              inMemSort;
  bool              needAdjust;
  STupleHandle      tupleHandle;
  void*             param;
  void (*beforeFp)(SSDataBlock* pBlock, void* param);

  _sort_fetch_block_fn_t  fetchfp;
  _sort_merge_compar_fn_t comparFn;
  SMultiwayMergeTreeInfo* pMergeTree;

  bool singleTableMerge;

  bool (*abortCheckFn)(void* param);
  void* abortCheckParam;

  bool           bSortByRowId;
  SSortMemFile* pExtRowsMemFile;
  int32_t        extRowBytes;
  int32_t        extRowsPageSize;
  int32_t        extRowsMemSize;
  int32_t        srcTsSlotId;
  SArray*         aExtRowsOrders;
  bool            bSortPk;
  void (*mergeLimitReachedFn)(uint64_t tableUid, void* param);
  void* mergeLimitReachedParam;
};

static int32_t destroySortMemFile(SSortHandle* pHandle);
static int32_t getRowBufFromExtMemFile(SSortHandle* pHandle, int32_t regionId, int32_t tupleOffset, int32_t rowLen,
                                       char** ppRow, bool* pFreeRow);
void tsortSetSingleTableMerge(SSortHandle* pHandle) {
  pHandle->singleTableMerge = true;
}

void tsortSetAbortCheckFn(SSortHandle *pHandle, bool (*checkFn)(void *), void* param) {
  pHandle->abortCheckFn = checkFn;
  pHandle->abortCheckParam = param;
}

static int32_t msortComparFn(const void* pLeft, const void* pRight, void* param);

// | offset[0] | offset[1] |....| nullbitmap | data |...|
static void* createTuple(uint32_t columnNum, uint32_t tupleLen) {
  uint32_t totalLen = sizeof(uint32_t) * columnNum + BitmapLen(columnNum) + tupleLen;
  return taosMemoryCalloc(1, totalLen);
}
static void destoryAllocatedTuple(void* t) { taosMemoryFree(t); }

#define tupleOffset(tuple, colIdx) ((uint32_t*)(tuple + sizeof(uint32_t) * colIdx))
#define tupleSetOffset(tuple, colIdx, offset) (*tupleOffset(tuple, colIdx) = offset)
#define tupleSetNull(tuple, colIdx, colNum) colDataSetNull_f((char*)tuple + sizeof(uint32_t) * colNum, colIdx)
#define tupleColIsNull(tuple, colIdx, colNum) colDataIsNull_f((char*)tuple + sizeof(uint32_t) * colNum, colIdx)
#define tupleGetDataStartOffset(colNum) (sizeof(uint32_t) * colNum + BitmapLen(colNum))
#define tupleSetData(tuple, offset, data, length) memcpy(tuple + offset, data, length)

/**
 * @param t the tuple pointer addr, if realloced, *t is changed to the new addr
 * @param offset copy data into pTuple start from offset
 * @param colIndex the columnIndex, for setting null bitmap
 * @return the next offset to add field
 * */
static inline size_t tupleAddField(char** t, uint32_t colNum, uint32_t offset, uint32_t colIdx, void* data, size_t length,
                            bool isNull, uint32_t tupleLen) {
  tupleSetOffset(*t, colIdx, offset);
  if (isNull) {
    tupleSetNull(*t, colIdx, colNum);
  } else {
    if (offset + length > tupleLen + tupleGetDataStartOffset(colNum)) {
      *t = taosMemoryRealloc(*t, offset + length);
    }
    tupleSetData(*t, offset, data, length);
  }
  return offset + length;
}

static void* tupleGetField(char* t, uint32_t colIdx, uint32_t colNum) {
  if (tupleColIsNull(t, colIdx, colNum)) return NULL;
  return t + *tupleOffset(t, colIdx);
}

SSDataBlock* tsortGetSortedDataBlock(const SSortHandle* pSortHandle) {
  return createOneDataBlock(pSortHandle->pDataBlock, false);
}

#define AllocatedTupleType 0
#define ReferencedTupleType 1 // tuple references to one row in pDataBlock
typedef struct TupleDesc {
  uint8_t type;
  char*   data; // if type is AllocatedTuple, then points to the created tuple, otherwise points to the DataBlock
} TupleDesc;

typedef struct ReferencedTuple {
  TupleDesc desc;
  size_t    rowIndex;
} ReferencedTuple;

static TupleDesc* createAllocatedTuple(SSDataBlock* pBlock, size_t colNum, uint32_t tupleLen, size_t rowIdx) {
  TupleDesc* t = taosMemoryCalloc(1, sizeof(TupleDesc));
  void*      pTuple = createTuple(colNum, tupleLen);
  if (!pTuple) {
    taosMemoryFree(t);
    return NULL;
  }
  size_t   colLen = 0;
  uint32_t offset = tupleGetDataStartOffset(colNum);
  for (size_t colIdx = 0; colIdx < colNum; ++colIdx) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, colIdx);
    if (colDataIsNull_s(pCol, rowIdx)) {
      offset = tupleAddField((char**)&pTuple, colNum, offset, colIdx, 0, 0, true, tupleLen);
    } else {
      colLen = colDataGetRowLength(pCol, rowIdx);
      offset =
          tupleAddField((char**)&pTuple, colNum, offset, colIdx, colDataGetData(pCol, rowIdx), colLen, false, tupleLen);
    }
  }
  t->type = AllocatedTupleType;
  t->data = pTuple;
  return t;
}

void* tupleDescGetField(const TupleDesc* pDesc, int32_t colIdx, uint32_t colNum) {
  if (pDesc->type == ReferencedTupleType) {
    ReferencedTuple* pRefTuple = (ReferencedTuple*)pDesc;
    SColumnInfoData* pCol = taosArrayGet(((SSDataBlock*)pDesc->data)->pDataBlock, colIdx);
    if (colDataIsNull_s(pCol, pRefTuple->rowIndex)) return NULL;
    return colDataGetData(pCol, pRefTuple->rowIndex);
  } else {
    return tupleGetField(pDesc->data, colIdx, colNum);
  }
}

void destroyTuple(void* t) {
  TupleDesc* pDesc = t;
  if (pDesc->type == AllocatedTupleType) {
    destoryAllocatedTuple(pDesc->data);
    taosMemoryFree(pDesc);
  }
}


/**
 *
 * @param type
 * @return
 */
SSortHandle* tsortCreateSortHandle(SArray* pSortInfo, int32_t type, int32_t pageSize, int32_t numOfPages,
                                   SSDataBlock* pBlock, const char* idstr, uint64_t pqMaxRows, uint32_t pqMaxTupleLength,
                                   uint32_t pqSortBufSize) {
  SSortHandle* pSortHandle = taosMemoryCalloc(1, sizeof(SSortHandle));

  pSortHandle->type = type;
  pSortHandle->pageSize = pageSize;
  pSortHandle->numOfPages = numOfPages;
  pSortHandle->pSortInfo = taosArrayDup(pSortInfo, NULL);
  pSortHandle->loops = 0;

  pSortHandle->pqMaxTupleLength = pqMaxTupleLength;
  if (pqMaxRows != 0) {
    pSortHandle->pqSortBufSize = pqSortBufSize;
    pSortHandle->pqMaxRows = pqMaxRows;
  }
  pSortHandle->forceUsePQSort = false;

  if (pBlock != NULL) {
    pSortHandle->pDataBlock = createOneDataBlock(pBlock, false);
  }

  pSortHandle->mergeLimit = -1;

  pSortHandle->pOrderedSource = taosArrayInit(4, POINTER_BYTES);
  pSortHandle->cmpParam.orderInfo = pSortInfo;
  pSortHandle->cmpParam.cmpGroupId = false;
  pSortHandle->cmpParam.sortType = type;

  if (type == SORT_BLOCK_TS_MERGE) {
    SBlockOrderInfo* pTsOrder = TARRAY_GET_ELEM(pSortInfo, 0);
    pSortHandle->cmpParam.tsSlotId = pTsOrder->slotId;
    pSortHandle->cmpParam.tsOrder = pTsOrder->order;
    pSortHandle->cmpParam.cmpTsFn = pTsOrder->compFn;
    if (taosArrayGetSize(pSortHandle->pSortInfo) == 2) {
      pSortHandle->cmpParam.pPkOrder = taosArrayGet(pSortHandle->pSortInfo, 1);
      pSortHandle->bSortPk = true;
    } else {
      pSortHandle->cmpParam.pPkOrder = NULL;
      pSortHandle->bSortPk = false;
    }
  }
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
    tMergeTreeDestroy(&pSortHandle->pMergeTree);
  }

  destroyDiskbasedBuf(pSortHandle->pBuf);
  taosMemoryFreeClear(pSortHandle->idStr);
  blockDataDestroy(pSortHandle->pDataBlock);
  if (pSortHandle->pBoundedQueue) destroyBoundedQueue(pSortHandle->pBoundedQueue);

  int64_t fetchUs = 0, fetchNum = 0;
  tsortClearOrderdSource(pSortHandle->pOrderedSource, &fetchUs, &fetchNum);
  qDebug("all source fetch time: %" PRId64 "us num:%" PRId64 " %s", fetchUs, fetchNum, pSortHandle->idStr);
  
  taosArrayDestroy(pSortHandle->pOrderedSource);
  if (pSortHandle->pExtRowsMemFile != NULL) {
    destroySortMemFile(pSortHandle);
  }
  taosArrayDestroy(pSortHandle->pSortInfo);  
  taosArrayDestroy(pSortHandle->aExtRowsOrders);
  pSortHandle->aExtRowsOrders = NULL;
  taosMemoryFreeClear(pSortHandle);
}

int32_t tsortAddSource(SSortHandle* pSortHandle, void* pSource) {
  void* px = taosArrayPush(pSortHandle->pOrderedSource, &pSource);
  if (px == NULL) {
    taosMemoryFree(pSource);
  }
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

    bool isNull = colDataIsNull(pSrcColInfo, pSource->info.rows, *rowIndex, NULL);
    if (isNull) {
      colDataSetVal(pColInfo, pBlock->info.rows, NULL, true);
    } else {
      if (!pSrcColInfo->pData) continue;
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
        qDebug("adjust merge tree. %d source completed %d", *numOfCompleted, pSource->pageIndex);
        (*numOfCompleted) += 1;
        pSource->src.rowIndex = -1;
        pSource->pageIndex = -1;
        pSource->src.pBlock = blockDataDestroy(pSource->src.pBlock);
      } else {
        if (pSource->pageIndex % 512 == 0) {
          qDebug("begin source %p page %d", pSource, pSource->pageIndex);
        }

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
        qDebug("adjust merge tree. %d source completed", *numOfCompleted);
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

// TODO: improve this function performance

int32_t tsortComparBlockCell(SSDataBlock* pLeftBlock, SSDataBlock* pRightBlock,
                      int32_t leftRowIndex, int32_t rightRowIndex, void* pCompareOrder) {
  SBlockOrderInfo* pOrder = pCompareOrder;
  SColumnInfoData* pLeftColInfoData = TARRAY_GET_ELEM(pLeftBlock->pDataBlock, pOrder->slotId);
  SColumnInfoData* pRightColInfoData = TARRAY_GET_ELEM(pRightBlock->pDataBlock, pOrder->slotId);

  bool isVarType = IS_VAR_DATA_TYPE(pLeftColInfoData->info.type);
  if (pLeftColInfoData->hasNull || pRightColInfoData->hasNull) {
    bool leftNull = false;
    if (pLeftColInfoData->hasNull) {
      if (pLeftBlock->pBlockAgg == NULL) {
        leftNull = colDataIsNull_t(pLeftColInfoData, leftRowIndex, isVarType);
      } else {
        leftNull =
            colDataIsNull(pLeftColInfoData, pLeftBlock->info.rows, leftRowIndex, &pLeftBlock->pBlockAgg[pOrder->slotId]);
      }
    }

    bool rightNull = false;
    if (pRightColInfoData->hasNull) {
      if (pRightBlock->pBlockAgg == NULL) {
        rightNull = colDataIsNull_t(pRightColInfoData, rightRowIndex, isVarType);
      } else {
        rightNull = colDataIsNull(pRightColInfoData, pRightBlock->info.rows, rightRowIndex,
                                  &pRightBlock->pBlockAgg[pOrder->slotId]);
      }
    }

    if (leftNull && rightNull) {
      return 0;
    }

    if (rightNull) {
      return pOrder->nullFirst ? 1 : -1;
    }

    if (leftNull) {
      return pOrder->nullFirst ? -1 : 1;
    }
  }

  void *left1, *right1;
  left1 = colDataGetData(pLeftColInfoData, leftRowIndex);
  right1 = colDataGetData(pRightColInfoData, rightRowIndex);
  __compar_fn_t fn = pOrder->compFn;
  int32_t ret = fn(left1, right1);
  return ret;
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

  if (pParam->sortType == SORT_BLOCK_TS_MERGE) {
    SColumnInfoData* pLeftTsCol = TARRAY_GET_ELEM(pLeftBlock->pDataBlock, pParam->tsSlotId);
    SColumnInfoData* pRightTsCol = TARRAY_GET_ELEM(pRightBlock->pDataBlock, pParam->tsSlotId);
    int64_t*            leftTs = (int64_t*)(pLeftTsCol->pData) + pLeftSource->src.rowIndex;
    int64_t*            rightTs =  (int64_t*)(pRightTsCol->pData) + pRightSource->src.rowIndex;

    int32_t ret = pParam->cmpTsFn(leftTs, rightTs);
    if (ret == 0 && pParam->pPkOrder) {
      ret = tsortComparBlockCell(pLeftBlock, pRightBlock, 
                              pLeftSource->src.rowIndex, pRightSource->src.rowIndex, (SBlockOrderInfo*)pParam->pPkOrder);
    }
    return ret;
  } else {
    bool isVarType;
    for (int32_t i = 0; i < pInfo->size; ++i) {
      SBlockOrderInfo* pOrder = TARRAY_GET_ELEM(pInfo, i);
      SColumnInfoData* pLeftColInfoData = TARRAY_GET_ELEM(pLeftBlock->pDataBlock, pOrder->slotId);
      SColumnInfoData* pRightColInfoData = TARRAY_GET_ELEM(pRightBlock->pDataBlock, pOrder->slotId);
      isVarType = IS_VAR_DATA_TYPE(pLeftColInfoData->info.type);

      if (pLeftColInfoData->hasNull || pRightColInfoData->hasNull) {
        bool leftNull = false;
        if (pLeftColInfoData->hasNull) {
          if (pLeftBlock->pBlockAgg == NULL) {
            leftNull = colDataIsNull_t(pLeftColInfoData, pLeftSource->src.rowIndex, isVarType);
          } else {
            leftNull = colDataIsNull(pLeftColInfoData, pLeftBlock->info.rows, pLeftSource->src.rowIndex,
                                     &pLeftBlock->pBlockAgg[i]);
          }
        }

        bool rightNull = false;
        if (pRightColInfoData->hasNull) {
          if (pRightBlock->pBlockAgg == NULL) {
            rightNull = colDataIsNull_t(pRightColInfoData, pRightSource->src.rowIndex, isVarType);
          } else {
            rightNull = colDataIsNull(pRightColInfoData, pRightBlock->info.rows, pRightSource->src.rowIndex,
                                      &pRightBlock->pBlockAgg[i]);
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
      }

      void* left1, *right1;
      if (isVarType) {
        left1 = colDataGetVarData(pLeftColInfoData, pLeftSource->src.rowIndex);
        right1 = colDataGetVarData(pRightColInfoData, pRightSource->src.rowIndex);
      } else {
        left1 = colDataGetNumData(pLeftColInfoData, pLeftSource->src.rowIndex);
        right1 = colDataGetNumData(pRightColInfoData, pRightSource->src.rowIndex);
      }

      __compar_fn_t fn = pOrder->compFn;
      if (!fn) {
        fn = getKeyComparFunc(pLeftColInfoData->info.type, pOrder->order);
        pOrder->compFn = fn;
      }

      int32_t ret = fn(left1, right1);
      if (ret == 0) {
        continue;
      } else {
        return ret;
      }
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
      qDebug("internal merge sort pass %d group %d. num input sources %d ", t, i, numOfInputSources);
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

      int32_t nMergedRows = 0;

      SArray* pPageIdList = taosArrayInit(4, sizeof(int32_t));
      while (1) {
        if (tsortIsClosed(pHandle) || (pHandle->abortCheckFn && pHandle->abortCheckFn(pHandle->abortCheckParam))) {
          code = terrno = TSDB_CODE_TSC_QUERY_CANCELLED;
          taosArrayDestroy(pPageIdList);
          return code;
        }

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
        nMergedRows += pDataBlock->info.rows;

        blockDataCleanup(pDataBlock);
        if ((pHandle->mergeLimit != -1) && (nMergedRows >= pHandle->mergeLimit)) {
          break;
        }        
      }

      sortComparCleanup(&pHandle->cmpParam);
      tMergeTreeDestroy(&pHandle->pMergeTree);
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

static int32_t createPageBuf(SSortHandle* pHandle) {
  if (pHandle->pBuf == NULL) {
    if (!osTempSpaceAvailable()) {
      terrno = TSDB_CODE_NO_DISKSPACE;
      qError("create page buf failed since %s, tempDir:%s", terrstr(), tsTempDir);
      return terrno;
    }

    int32_t code = createDiskbasedBuf(&pHandle->pBuf, pHandle->pageSize, pHandle->numOfPages * pHandle->pageSize,
                                      "tableBlocksBuf", tsTempDir);
    dBufSetPrintInfo(pHandle->pBuf);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return 0;
}

void tsortAppendTupleToBlock(SSortHandle* pHandle, SSDataBlock* pBlock, STupleHandle* pTupleHandle) {
  if (pHandle->bSortByRowId) {
    int32_t regionId = *(int32_t*)tsortGetValue(pTupleHandle, 1);
    int32_t offset = *(int32_t*)tsortGetValue(pTupleHandle, 2);
    int32_t length = *(int32_t*)tsortGetValue(pTupleHandle, 3);
    
    char* buf = NULL;
    bool bFreeRow = false;
    getRowBufFromExtMemFile(pHandle, regionId, offset, length, &buf, &bFreeRow);
    int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
    char*   isNull = (char*)buf;
    char*   pStart = (char*)buf + sizeof(int8_t) * numOfCols;
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);

      if (!isNull[i]) {
        colDataSetVal(pColInfo, pBlock->info.rows, pStart, false);
        if (pColInfo->info.type == TSDB_DATA_TYPE_JSON) {
          int32_t dataLen = getJsonValueLen(pStart);
          pStart += dataLen;
        } else if (IS_VAR_DATA_TYPE(pColInfo->info.type)) {
          pStart += varDataTLen(pStart);
        } else {
          int32_t bytes = pColInfo->info.bytes;
          pStart += bytes;
        }
      } else {
        colDataSetNULL(pColInfo, pBlock->info.rows);
      }
    }

    if (*(int32_t*)pStart != pStart - buf) {
      qError("table merge scan row buf deserialization. length error %d != %d ", *(int32_t*)pStart,
             (int32_t)(pStart - buf));
    }

    if (bFreeRow) {
      taosMemoryFree(buf);
    }

    pBlock->info.dataLoad = 1;
    pBlock->info.scanFlag = ((SDataBlockInfo*)tsortGetBlockInfo(pTupleHandle))->scanFlag;
    pBlock->info.rows += 1;

  } else {
    for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); ++i) {
      SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);
      bool             isNull = tsortIsNullVal(pTupleHandle, i);
      if (isNull) {
        colDataSetNULL(pColInfo, pBlock->info.rows);
      } else {
        char* pData = tsortGetValue(pTupleHandle, i);
        if (pData != NULL) {
          colDataSetVal(pColInfo, pBlock->info.rows, pData, false);
        }
      }
    }

    pBlock->info.dataLoad = 1;
    pBlock->info.scanFlag = ((SDataBlockInfo*)tsortGetBlockInfo(pTupleHandle))->scanFlag;
    pBlock->info.rows += 1;
  }
}

static int32_t blockRowToBuf(SSDataBlock* pBlock, int32_t rowIdx, char* buf) {
  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);

  char* isNull = (char*)buf;
  char* pStart = (char*)buf + sizeof(int8_t) * numOfCols;
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, i);
    if (colDataIsNull_s(pCol, rowIdx)) {
      isNull[i] = 1;
      continue;
    }

    isNull[i] = 0;
    char* pData = colDataGetData(pCol, rowIdx);
    if (pCol->info.type == TSDB_DATA_TYPE_JSON) {
      if (pCol->pData) {
        int32_t dataLen = getJsonValueLen(pData);
        memcpy(pStart, pData, dataLen);
        pStart += dataLen;
      } else {
        // the column that is pre-allocated has no data and has offset
        *pStart = 0;
        pStart += 1;
      }
    } else if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      if (pCol->pData) {
        varDataCopy(pStart, pData);
        pStart += varDataTLen(pData);
      } else {
        // the column that is pre-allocated has no data and has offset
        *(VarDataLenT*)(pStart) = 0;
        pStart += VARSTR_HEADER_SIZE;
      }
    } else {
      int32_t bytes = pCol->info.bytes;
      memcpy(pStart, pData, bytes);
      pStart += bytes;
    }
  }
  *(int32_t*)pStart = (char*)pStart - (char*)buf;
  pStart += sizeof(int32_t);
  return (int32_t)(pStart - (char*)buf);
}

static int32_t getRowBufFromExtMemFile(SSortHandle* pHandle, int32_t regionId, int32_t tupleOffset, int32_t rowLen,
                                       char** ppRow, bool* pFreeRow) {
  SSortMemFile* pMemFile = pHandle->pExtRowsMemFile;
  SSortMemFileRegion* pRegion = taosArrayGet(pMemFile->aFileRegions, regionId);
  if (pRegion->buf == NULL) {
    pRegion->bufRegOffset = 0;
    pRegion->buf = taosMemoryMalloc(pMemFile->blockSize);
    if (pRegion->buf == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    taosSeekCFile(pMemFile->pTdFile, pRegion->fileOffset, SEEK_SET);
    int32_t readBytes = TMIN(pMemFile->blockSize, pRegion->regionSize);
    int32_t ret = taosReadFromCFile(pRegion->buf, readBytes, 1, pMemFile->pTdFile);
    if (ret != 1) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return terrno;
    }
    pRegion->bufLen = readBytes;
  }
  ASSERT(pRegion->bufRegOffset <= tupleOffset);
  if (pRegion->bufRegOffset + pRegion->bufLen >= tupleOffset + rowLen) {
    *pFreeRow = false;
    *ppRow = pRegion->buf + tupleOffset - pRegion->bufRegOffset;
  } else {
    *ppRow = taosMemoryMalloc(rowLen);
    if (*ppRow == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    int32_t szThisBlock = pRegion->bufLen - (tupleOffset - pRegion->bufRegOffset);
    memcpy(*ppRow, pRegion->buf + tupleOffset - pRegion->bufRegOffset, szThisBlock);
    taosSeekCFile(pMemFile->pTdFile, pRegion->fileOffset + pRegion->bufRegOffset + pRegion->bufLen, SEEK_SET);
    int32_t readBytes = TMIN(pMemFile->blockSize, pRegion->regionSize - (pRegion->bufRegOffset + pRegion->bufLen));
    int32_t     ret = taosReadFromCFile(pRegion->buf, readBytes, 1, pMemFile->pTdFile);
    if (ret != 1) {
      taosMemoryFreeClear(*ppRow);
      terrno = TAOS_SYSTEM_ERROR(errno);
      return terrno;
    }
    memcpy(*ppRow + szThisBlock, pRegion->buf, rowLen - szThisBlock);
    *pFreeRow = true;
    pRegion->bufRegOffset += pRegion->bufLen;
    pRegion->bufLen = readBytes;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t createSortMemFile(SSortHandle* pHandle) {
  if (pHandle->pExtRowsMemFile != NULL) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t       code = TSDB_CODE_SUCCESS;
  SSortMemFile* pMemFile = taosMemoryCalloc(1, sizeof(SSortMemFile));
  if (pMemFile == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }
  if (code == TSDB_CODE_SUCCESS) {
    taosGetTmpfilePath(tsTempDir, "sort-ext-mem", pMemFile->memFilePath);
    pMemFile->pTdFile = taosOpenCFile(pMemFile->memFilePath, "w+b");
    if (pMemFile->pTdFile == NULL) {
      code = terrno = TAOS_SYSTEM_ERROR(errno);
    }
  }
  if (code == TSDB_CODE_SUCCESS) {
    taosSetAutoDelFile(pMemFile->memFilePath);

    pMemFile->currRegionId = -1;
    pMemFile->currRegionOffset = -1;

    pMemFile->writeBufSize = 4 * 1024 * 1024;
    pMemFile->writeFileOffset = -1;
    pMemFile->bRegionDirty = false;
    
    pMemFile->writeBuf = taosMemoryMalloc(pMemFile->writeBufSize);
    if (pMemFile->writeBuf == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  if (code == TSDB_CODE_SUCCESS) {
    pMemFile->cacheSize = pHandle->extRowsMemSize;
    pMemFile->aFileRegions = taosArrayInit(64, sizeof(SSortMemFileRegion));
    if (pMemFile->aFileRegions == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  if (code == TSDB_CODE_SUCCESS) {
    pHandle->pExtRowsMemFile = pMemFile;
  } else {
    if (pMemFile) {
      if (pMemFile->aFileRegions) taosMemoryFreeClear(pMemFile->aFileRegions);
      if (pMemFile->writeBuf) taosMemoryFreeClear(pMemFile->writeBuf);
      if (pMemFile->pTdFile) {
        taosCloseCFile(pMemFile->pTdFile);
        pMemFile->pTdFile = NULL;
      }
      taosMemoryFreeClear(pMemFile);
    }
  }
  return code;
}

static int32_t destroySortMemFile(SSortHandle* pHandle) {
  if (pHandle->pExtRowsMemFile == NULL) return TSDB_CODE_SUCCESS;

  SSortMemFile* pMemFile = pHandle->pExtRowsMemFile;
  for (int32_t i = 0; i < taosArrayGetSize(pMemFile->aFileRegions); ++i) {
    SSortMemFileRegion* pRegion = taosArrayGet(pMemFile->aFileRegions, i);
    taosMemoryFree(pRegion->buf);
  }
  taosArrayDestroy(pMemFile->aFileRegions);
  pMemFile->aFileRegions = NULL;

  taosMemoryFree(pMemFile->writeBuf);
  pMemFile->writeBuf = NULL;

  taosCloseCFile(pMemFile->pTdFile);
  pMemFile->pTdFile = NULL;
  taosRemoveFile(pMemFile->memFilePath);
  taosMemoryFree(pMemFile);
  pHandle->pExtRowsMemFile = NULL;
  return TSDB_CODE_SUCCESS;
}

static int32_t tsortOpenRegion(SSortHandle* pHandle) {
  SSortMemFile* pMemFile = pHandle->pExtRowsMemFile;
  if (pMemFile->currRegionId == -1) {
    SSortMemFileRegion region = {0};
    region.fileOffset = 0;
    region.bufRegOffset = 0;
    taosArrayPush(pMemFile->aFileRegions, &region);
    pMemFile->currRegionId = 0;
    pMemFile->currRegionOffset = 0;
    pMemFile->writeFileOffset = 0;
  } else {
    SSortMemFileRegion regionNew = {0};
    SSortMemFileRegion* pRegion = taosArrayGet(pMemFile->aFileRegions, pMemFile->currRegionId);
    regionNew.fileOffset = pRegion->fileOffset + pRegion->regionSize;
    regionNew.bufRegOffset = 0;
    taosArrayPush(pMemFile->aFileRegions, &regionNew);
    ++pMemFile->currRegionId;
    pMemFile->currRegionOffset = 0;
    pMemFile->writeFileOffset = regionNew.fileOffset;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t tsortCloseRegion(SSortHandle* pHandle) {
  SSortMemFile* pMemFile = pHandle->pExtRowsMemFile;
  SSortMemFileRegion* pRegion = taosArrayGet(pMemFile->aFileRegions, pMemFile->currRegionId);
  pRegion->regionSize = pMemFile->currRegionOffset;
  int32_t writeBytes = pRegion->regionSize - (pMemFile->writeFileOffset - pRegion->fileOffset);
  if (writeBytes > 0) {
    int32_t ret = fwrite(pMemFile->writeBuf, writeBytes, 1, pMemFile->pTdFile);
    if (ret != 1) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return terrno;
    }
    pMemFile->bRegionDirty = false;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t tsortFinalizeRegions(SSortHandle* pHandle) {
  SSortMemFile* pMemFile = pHandle->pExtRowsMemFile;
  size_t numRegions = taosArrayGetSize(pMemFile->aFileRegions);
  ASSERT(numRegions == (pMemFile->currRegionId + 1));
  if (numRegions == 0) return TSDB_CODE_SUCCESS;
  int32_t blockReadBytes = (pMemFile->cacheSize / numRegions + 4095) & ~4095;
  pMemFile->blockSize = blockReadBytes;

  for (int32_t i = 0; i < numRegions; ++i) {
    SSortMemFileRegion* pRegion = taosArrayGet(pMemFile->aFileRegions, i);
    pRegion->bufRegOffset = 0;
  }
  taosMemoryFree(pMemFile->writeBuf);
  pMemFile->writeBuf = NULL;
  return TSDB_CODE_SUCCESS;
}

static int32_t saveBlockRowToExtRowsMemFile(SSortHandle* pHandle, SSDataBlock* pBlock, int32_t rowIdx,
                                            int32_t* pRegionId, int32_t* pOffset, int32_t* pLength) {

  SSortMemFile* pMemFile = pHandle->pExtRowsMemFile;
  SSortMemFileRegion* pRegion = taosArrayGet(pMemFile->aFileRegions, pMemFile->currRegionId);
  {
    if (pMemFile->currRegionOffset + pHandle->extRowBytes >= pMemFile->writeBufSize) {
      int32_t writeBytes = pMemFile->currRegionOffset - (pMemFile->writeFileOffset - pRegion->fileOffset);
      int32_t ret = fwrite(pMemFile->writeBuf, writeBytes, 1, pMemFile->pTdFile);
      if (ret !=  1) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        return terrno;
      }
      pMemFile->writeFileOffset = pRegion->fileOffset + pMemFile->currRegionOffset;
    }
  }

  *pRegionId = pMemFile->currRegionId;
  *pOffset = pMemFile->currRegionOffset;
  int32_t writeBufOffset = pMemFile->currRegionOffset - (pMemFile->writeFileOffset - pRegion->fileOffset);
  int32_t blockLen = blockRowToBuf(pBlock, rowIdx, pMemFile->writeBuf + writeBufOffset);
  *pLength = blockLen;

  pMemFile->currRegionOffset += blockLen;
  pMemFile->bRegionDirty = true;
  return TSDB_CODE_SUCCESS;
}

static void appendToRowIndexDataBlock(SSortHandle* pHandle, SSDataBlock* pSource, int32_t* rowIndex) {
  int32_t pageId = -1;
  int32_t offset = -1;
  int32_t length = -1;
  saveBlockRowToExtRowsMemFile(pHandle, pSource, *rowIndex, &pageId, &offset, &length);

  SSDataBlock* pBlock = pHandle->pDataBlock;
  SBlockOrderInfo* extRowsTsOrder = taosArrayGet(pHandle->aExtRowsOrders, 0);
  SColumnInfoData* pSrcTsCol = taosArrayGet(pSource->pDataBlock, extRowsTsOrder->slotId);
  SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, 0);
  char* pData = colDataGetData(pSrcTsCol, *rowIndex);
  colDataSetVal(pTsCol, pBlock->info.rows, pData, false);

  SColumnInfoData* pRegionIdCol = taosArrayGet(pBlock->pDataBlock, 1);
  colDataSetInt32(pRegionIdCol, pBlock->info.rows, &pageId);

  SColumnInfoData* pOffsetCol = taosArrayGet(pBlock->pDataBlock, 2);
  colDataSetInt32(pOffsetCol, pBlock->info.rows, &offset);

  SColumnInfoData* pLengthCol = taosArrayGet(pBlock->pDataBlock, 3);
  colDataSetInt32(pLengthCol, pBlock->info.rows, &length);

  if (pHandle->bSortPk) {
    SBlockOrderInfo* extRowsPkOrder = taosArrayGet(pHandle->aExtRowsOrders, 1);
    SColumnInfoData* pSrcPkCol = taosArrayGet(pSource->pDataBlock, extRowsPkOrder->slotId);
    SColumnInfoData* pPkCol = taosArrayGet(pBlock->pDataBlock, 4);
    if (colDataIsNull_s(pSrcPkCol, *rowIndex)) {
      colDataSetNULL(pPkCol, pBlock->info.rows);
    } else {
      char* pPkData = colDataGetData(pSrcPkCol, *rowIndex);
      colDataSetVal(pPkCol, pBlock->info.rows, pPkData, false);
    }
  }

  pBlock->info.rows += 1;
  *rowIndex += 1;
}

static void initRowIdSort(SSortHandle* pHandle) {
  SBlockOrderInfo* pkOrder = (pHandle->bSortPk) ? taosArrayGet(pHandle->aExtRowsOrders, 1) : NULL;
  SColumnInfoData* extPkCol = (pHandle->bSortPk) ? taosArrayGet(pHandle->pDataBlock->pDataBlock, pkOrder->slotId) : NULL;
  SColumnInfoData pkCol = {0};

  SSDataBlock* pSortInput = createDataBlock();
  SColumnInfoData tsCol = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, 8, 1);
  blockDataAppendColInfo(pSortInput, &tsCol);
  SColumnInfoData regionIdCol = createColumnInfoData(TSDB_DATA_TYPE_INT, 4, 2);
  blockDataAppendColInfo(pSortInput, &regionIdCol);
  SColumnInfoData  offsetCol = createColumnInfoData(TSDB_DATA_TYPE_INT, 4, 3);
  blockDataAppendColInfo(pSortInput, &offsetCol);
  SColumnInfoData  lengthCol = createColumnInfoData(TSDB_DATA_TYPE_INT, 4, 4);
  blockDataAppendColInfo(pSortInput, &lengthCol);

  if (pHandle->bSortPk) {
    pkCol = createColumnInfoData(extPkCol->info.type, extPkCol->info.bytes, 5);
    blockDataAppendColInfo(pSortInput, &pkCol);
  }

  blockDataDestroy(pHandle->pDataBlock);
  pHandle->pDataBlock = pSortInput;

//  int32_t  rowSize = blockDataGetRowSize(pHandle->pDataBlock);
//  size_t nCols = taosArrayGetSize(pHandle->pDataBlock->pDataBlock);
  pHandle->pageSize = 256 * 1024; // 256k
  pHandle->numOfPages = 256;

  SArray* pOrderInfoList = taosArrayInit(1, sizeof(SBlockOrderInfo));

  int32_t tsOrder = ((SBlockOrderInfo*)taosArrayGet(pHandle->pSortInfo, 0))->order;

  SBlockOrderInfo biTs = {0};
  biTs.order = tsOrder;
  biTs.slotId = 0;
  biTs.nullFirst = (biTs.order == TSDB_ORDER_ASC);
  biTs.compFn = getKeyComparFunc(TSDB_DATA_TYPE_TIMESTAMP, biTs.order);
  taosArrayPush(pOrderInfoList, &biTs);

  if (pHandle->bSortPk) {
    SBlockOrderInfo biPk = {0};
    biPk.order = pkOrder->order;
    biPk.slotId = 4;
    biPk.nullFirst = (biPk.order == TSDB_ORDER_ASC);
    biPk.compFn = getKeyComparFunc(pkCol.info.type, biPk.order);
    taosArrayPush(pOrderInfoList, &biPk);
  }

  taosArrayDestroy(pHandle->pSortInfo);
  pHandle->pSortInfo = pOrderInfoList;
  pHandle->cmpParam.pPkOrder = (pHandle->bSortPk) ? taosArrayGet(pHandle->pSortInfo, 1) : NULL;
}

int32_t tsortSetSortByRowId(SSortHandle* pHandle, int32_t extRowsMemSize) {
  pHandle->extRowBytes = blockDataGetRowSize(pHandle->pDataBlock) + taosArrayGetSize(pHandle->pDataBlock->pDataBlock) + sizeof(int32_t);
  pHandle->extRowsMemSize = extRowsMemSize;
  pHandle->aExtRowsOrders = taosArrayDup(pHandle->pSortInfo, NULL);
  initRowIdSort(pHandle);
  if (!osTempSpaceAvailable()) {
    terrno = TSDB_CODE_NO_DISKSPACE;
    qError("create sort mem file failed since %s, tempDir:%s", terrstr(), tsTempDir);
    return terrno;
  }
  int32_t code = createSortMemFile(pHandle);
  pHandle->bSortByRowId = true;
  return code;
}

typedef struct SBlkMergeSupport {
  int64_t** aTs;
  int32_t* aRowIdx;
  int32_t tsOrder;

  SBlockOrderInfo* pPkOrder;
  SSDataBlock** aBlks;
} SBlkMergeSupport;

static int32_t blockCompareTsFn(const void* pLeft, const void* pRight, void* param) {
  int32_t left = *(int32_t*)pLeft;
  int32_t right = *(int32_t*)pRight;

  SBlkMergeSupport* pSup = (SBlkMergeSupport*)param;
  if (pSup->aRowIdx[left] == -1) {
    return 1;
  } else if (pSup->aRowIdx[right] == -1) {
    return -1;
  }

  int64_t leftTs = pSup->aTs[left][pSup->aRowIdx[left]];
  int64_t rightTs = pSup->aTs[right][pSup->aRowIdx[right]];

  int32_t ret = leftTs>rightTs ? 1 : ((leftTs < rightTs) ? -1 : 0);
  if (pSup->tsOrder == TSDB_ORDER_DESC) {
    ret = -1 * ret;
  }
  return ret;
}

static int32_t blockCompareTsPkFn(const void* pLeft, const void* pRight, void* param) {
  int32_t left = *(int32_t*)pLeft;
  int32_t right = *(int32_t*)pRight;

  SBlkMergeSupport* pSup = (SBlkMergeSupport*)param;
  if (pSup->aRowIdx[left] == -1) {
    return 1;
  } else if (pSup->aRowIdx[right] == -1) {
    return -1;
  }

  int64_t leftTs = pSup->aTs[left][pSup->aRowIdx[left]];
  int64_t rightTs = pSup->aTs[right][pSup->aRowIdx[right]];

  int32_t ret = leftTs>rightTs ? 1 : ((leftTs < rightTs) ? -1 : 0);
  if (pSup->tsOrder == TSDB_ORDER_DESC) {
    ret = -1 * ret;
  }
  if (ret == 0 && pSup->pPkOrder) {
    ret = tsortComparBlockCell(pSup->aBlks[left], pSup->aBlks[right], pSup->aRowIdx[left], pSup->aRowIdx[right], pSup->pPkOrder);
  }
  return ret;  
}

static int32_t appendDataBlockToPageBuf(SSortHandle* pHandle, SSDataBlock* blk, SArray* aPgId) {
  int32_t pageId = -1;
  void*   pPage = getNewBufPage(pHandle->pBuf, &pageId);
  if (pPage == NULL) {
    return terrno;
  }
  taosArrayPush(aPgId, &pageId);

  int32_t size = blockDataGetSize(blk) + sizeof(int32_t) + taosArrayGetSize(blk->pDataBlock) * sizeof(int32_t);
  ASSERT(size <= getBufPageSize(pHandle->pBuf));
  
  blockDataToBuf(pPage, blk);

  setBufPageDirty(pPage, true);
  releaseBufPage(pHandle->pBuf, pPage);

  return 0;
}

static int32_t getPageBufIncForRow(SSDataBlock* pSrcBlock, int32_t srcRowIndex, int32_t dstRowIndex) {
  int32_t size = 0;
  int32_t numCols = taosArrayGetSize(pSrcBlock->pDataBlock);

  if (!pSrcBlock->info.hasVarCol) {
    size += numCols * ((dstRowIndex & 0x7) == 0 ? 1: 0);
    size += blockDataGetRowSize(pSrcBlock);
  } else {
    for (int32_t i = 0; i < numCols; ++i) {
      SColumnInfoData* pColInfoData = TARRAY_GET_ELEM(pSrcBlock->pDataBlock, i);
      if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
        if ((pColInfoData->varmeta.offset[srcRowIndex] != -1) && (pColInfoData->pData)) {
          char* p = colDataGetData(pColInfoData, srcRowIndex);

          if (pColInfoData->info.type == TSDB_DATA_TYPE_JSON) {
            size += getJsonValueLen(p);
          } else {
            size += varDataTLen(p);
          }
        }

        size += sizeof(pColInfoData->varmeta.offset[0]);
      } else {
        size += pColInfoData->info.bytes;

        if (((dstRowIndex) & 0x07) == 0) {
          size += 1; // bitmap
        }
      }
    }    
  }

  return size;
}

static int32_t getPageBufIncForRowIdSort(SSDataBlock* pDstBlock, int32_t srcRowIndex, int32_t dstRowIndex,
                                         SColumnInfoData* pPkCol) {
  int32_t size = 0;
  int32_t numOfCols = blockDataGetNumOfCols(pDstBlock);

  if (pPkCol == NULL) { // no var column
    ASSERT((numOfCols == 4) && (!pDstBlock->info.hasVarCol));

    size += numOfCols * ((dstRowIndex & 0x7) == 0 ? 1: 0);
    size += blockDataGetRowSize(pDstBlock);
  } else {
    ASSERT(numOfCols == 5);

    size += (numOfCols - 1) * (((dstRowIndex & 0x7) == 0)? 1:0);
    for(int32_t i = 0; i < numOfCols - 1; ++i) {
      SColumnInfoData* pColInfo = TARRAY_GET_ELEM(pDstBlock->pDataBlock, i);
      size += pColInfo->info.bytes;
    }

    // handle the pk column, the last column, may be the var char column
    if (IS_VAR_DATA_TYPE(pPkCol->info.type)) {
      if ((pPkCol->varmeta.offset[srcRowIndex] != -1) && (pPkCol->pData)) {
        char* p = colDataGetData(pPkCol, srcRowIndex);
        size += varDataTLen(p);
      }

      size += sizeof(pPkCol->varmeta.offset[0]);
    } else {
      size += pPkCol->info.bytes;
      if (((dstRowIndex) & 0x07) == 0) {
        size += 1; // bitmap
      }
    }
  }

  return size;
}

static int32_t getBufIncForNewRow(SSortHandle* pHandle, int32_t dstRowIndex, SSDataBlock* pSrcBlock,
                                  int32_t srcRowIndex) {
  int32_t inc = 0;

  if (pHandle->bSortByRowId) {
    SColumnInfoData* pPkCol = NULL;

    // there may be varchar column exists, so we need to get the pk info, and then calculate the row length
    if (pHandle->bSortPk) {
      SBlockOrderInfo* extRowsPkOrder = taosArrayGet(pHandle->aExtRowsOrders, 1);
      pPkCol = taosArrayGet(pSrcBlock->pDataBlock, extRowsPkOrder->slotId);
    }

    inc = getPageBufIncForRowIdSort(pHandle->pDataBlock, srcRowIndex, dstRowIndex, pPkCol);
  } else {
    inc = getPageBufIncForRow(pSrcBlock, srcRowIndex, dstRowIndex);
  }

  return inc;
}

static int32_t initMergeSup(SBlkMergeSupport* pSup, SArray* pBlockList, int32_t tsOrder, int32_t tsSlotId, SBlockOrderInfo* pPkOrderInfo) {
  memset(pSup, 0, sizeof(SBlkMergeSupport));

  int32_t numOfBlocks = taosArrayGetSize(pBlockList);

  pSup->aRowIdx = taosMemoryCalloc(numOfBlocks, sizeof(int32_t));
  pSup->aTs = taosMemoryCalloc(numOfBlocks, sizeof(int64_t*));
  pSup->tsOrder = tsOrder;
  pSup->aBlks = taosMemoryCalloc(numOfBlocks, sizeof(SSDataBlock*));

  for (int32_t i = 0; i < numOfBlocks; ++i) {
    SSDataBlock*     pBlock = taosArrayGetP(pBlockList, i);
    SColumnInfoData* col = taosArrayGet(pBlock->pDataBlock, tsSlotId);
    pSup->aTs[i] = (int64_t*)col->pData;
    pSup->aRowIdx[i] = 0;
    pSup->aBlks[i] = pBlock;
  }

  pSup->pPkOrder = pPkOrderInfo;
  return TSDB_CODE_SUCCESS;
}

static void cleanupMergeSup(SBlkMergeSupport* pSup) {
  taosMemoryFree(pSup->aRowIdx);
  taosMemoryFree(pSup->aTs);
  taosMemoryFree(pSup->aBlks);
}

static int32_t getTotalRows(SArray* pBlockList) {
  int32_t totalRows = 0;

  for (int32_t i = 0; i < taosArrayGetSize(pBlockList); ++i) {
    SSDataBlock* blk = taosArrayGetP(pBlockList, i);
    totalRows += blk->info.rows;
  }

  return totalRows;
}

static int32_t sortBlocksToExtSource(SSortHandle* pHandle, SArray* aBlk, SArray* aExtSrc) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t pageHeaderSize = sizeof(int32_t) + sizeof(int32_t) * blockDataGetNumOfCols(pHandle->pDataBlock);
  int32_t rowCap = blockDataGetCapacityInRow(pHandle->pDataBlock, pHandle->pageSize, pageHeaderSize);
  
  blockDataEnsureCapacity(pHandle->pDataBlock, rowCap);
  blockDataCleanup(pHandle->pDataBlock);

  SBlkMergeSupport sup = {0};

  SBlockOrderInfo* pOrigBlockTsOrder =
      (!pHandle->bSortByRowId) ? taosArrayGet(pHandle->pSortInfo, 0) : taosArrayGet(pHandle->aExtRowsOrders, 0);
  
  SBlockOrderInfo* pHandleBlockTsOrder = taosArrayGet(pHandle->pSortInfo, 0);

  SBlockOrderInfo* pOrigBlockPkOrder = NULL;
  if (pHandle->bSortPk) {
    pOrigBlockPkOrder =
        (!pHandle->bSortByRowId) ? taosArrayGet(pHandle->pSortInfo, 1) : taosArrayGet(pHandle->aExtRowsOrders, 1);
  }

  initMergeSup(&sup, aBlk, pOrigBlockTsOrder->order, pOrigBlockTsOrder->slotId, pOrigBlockPkOrder);

  int32_t totalRows = getTotalRows(aBlk);

  SMultiwayMergeTreeInfo* pTree = NULL;
  __merge_compare_fn_t    mergeCompareFn = (!pHandle->bSortPk) ? blockCompareTsFn : blockCompareTsPkFn;

  code = tMergeTreeCreate(&pTree, taosArrayGetSize(aBlk), &sup, mergeCompareFn);
  if (TSDB_CODE_SUCCESS != code) {
    cleanupMergeSup(&sup);
    return code;
  }

  SArray* aPgId = taosArrayInit(8, sizeof(int32_t));
  int32_t nRows = 0;
  int32_t nMergedRows = 0;
  bool    mergeLimitReached = false;
  size_t  blkPgSz = pageHeaderSize;
  int64_t lastPageBufTs = (pHandleBlockTsOrder->order == TSDB_ORDER_ASC) ? INT64_MAX : INT64_MIN;

  while (nRows < totalRows) {
    int32_t      minIdx = tMergeTreeGetChosenIndex(pTree);
    SSDataBlock* minBlk = taosArrayGetP(aBlk, minIdx);
    int32_t      minRow = sup.aRowIdx[minIdx];

    int32_t bufInc = getBufIncForNewRow(pHandle, pHandle->pDataBlock->info.rows, minBlk, minRow);

    if (blkPgSz <= pHandle->pageSize && blkPgSz + bufInc > pHandle->pageSize) {
      SColumnInfoData* tsCol = taosArrayGet(pHandle->pDataBlock->pDataBlock, pHandleBlockTsOrder->slotId);
      lastPageBufTs = ((int64_t*)tsCol->pData)[pHandle->pDataBlock->info.rows - 1];
      code = appendDataBlockToPageBuf(pHandle, pHandle->pDataBlock, aPgId);
      if (code != TSDB_CODE_SUCCESS) {
        taosMemoryFree(pTree);
        taosArrayDestroy(aPgId);
        cleanupMergeSup(&sup);
        return code;
      }

      nMergedRows += pHandle->pDataBlock->info.rows;
      blockDataCleanup(pHandle->pDataBlock);
      blkPgSz = pageHeaderSize;

      bufInc = getBufIncForNewRow(pHandle, 0, minBlk, minRow);

      if ((pHandle->mergeLimit != -1) && (nMergedRows >= pHandle->mergeLimit)) {
        mergeLimitReached = true;
        if ((lastPageBufTs < pHandle->currMergeLimitTs && pHandleBlockTsOrder->order == TSDB_ORDER_ASC) ||
            (lastPageBufTs > pHandle->currMergeLimitTs && pHandleBlockTsOrder->order == TSDB_ORDER_DESC)) {
          pHandle->currMergeLimitTs = lastPageBufTs;
        }

        break;
      }
    }

    blockDataEnsureCapacity(pHandle->pDataBlock, pHandle->pDataBlock->info.rows + 1);
    if (pHandle->bSortByRowId) {
      appendToRowIndexDataBlock(pHandle, minBlk, &minRow);
    } else {
      appendOneRowToDataBlock(pHandle->pDataBlock, minBlk, &minRow);
    }

    blkPgSz += bufInc;
    ASSERT(blkPgSz == blockDataGetSize(pHandle->pDataBlock) + pageHeaderSize);

    ++nRows;

    if (sup.aRowIdx[minIdx] == minBlk->info.rows - 1) {
      sup.aRowIdx[minIdx] = -1;
    } else {
      ++sup.aRowIdx[minIdx];
    }
    tMergeTreeAdjust(pTree, tMergeTreeGetAdjustIndex(pTree));
  }

  if (pHandle->pDataBlock->info.rows > 0) {
    if (!mergeLimitReached) {
      SColumnInfoData* tsCol = taosArrayGet(pHandle->pDataBlock->pDataBlock, pHandleBlockTsOrder->slotId);
      lastPageBufTs = ((int64_t*)tsCol->pData)[pHandle->pDataBlock->info.rows - 1];
      code = appendDataBlockToPageBuf(pHandle, pHandle->pDataBlock, aPgId);
      if (code != TSDB_CODE_SUCCESS) {
        taosArrayDestroy(aPgId);
        taosMemoryFree(pTree);
        cleanupMergeSup(&sup);
        return code;
      }
      nMergedRows += pHandle->pDataBlock->info.rows;
      if ((pHandle->mergeLimit != -1) && (nMergedRows >= pHandle->mergeLimit)) {
        mergeLimitReached = true;
        if ((lastPageBufTs < pHandle->currMergeLimitTs && pHandleBlockTsOrder->order == TSDB_ORDER_ASC) ||
            (lastPageBufTs > pHandle->currMergeLimitTs && pHandleBlockTsOrder->order == TSDB_ORDER_DESC)) {
          pHandle->currMergeLimitTs = lastPageBufTs;
        }
      }
    }
    blockDataCleanup(pHandle->pDataBlock);
  }

  SSDataBlock* pMemSrcBlk = createOneDataBlock(pHandle->pDataBlock, false);
  doAddNewExternalMemSource(pHandle->pBuf, aExtSrc, pMemSrcBlk, &pHandle->sourceId, aPgId);

  cleanupMergeSup(&sup);
  tMergeTreeDestroy(&pTree);

  return 0;
}

static SSDataBlock* getRowsBlockWithinMergeLimit(const SSortHandle* pHandle, SSHashObj* mTableNumRows, SSDataBlock* pOrigBlk, bool* pExtractedBlock, bool *pSkipBlock) {
  int64_t nRows = 0;
  int64_t prevRows = 0;
  void*   pNum = tSimpleHashGet(mTableNumRows, &pOrigBlk->info.id.uid, sizeof(pOrigBlk->info.id.uid));
  if (pNum == NULL) {
    prevRows = 0;
    nRows = pOrigBlk->info.rows;
    tSimpleHashPut(mTableNumRows, &pOrigBlk->info.id.uid, sizeof(pOrigBlk->info.id.uid), &nRows, sizeof(nRows));
  } else {
    prevRows = *(int64_t*)pNum;
    *(int64_t*)pNum = *(int64_t*)pNum + pOrigBlk->info.rows;
    nRows = *(int64_t*)pNum;
  }

  int64_t keepRows = pOrigBlk->info.rows;
  if (nRows >= pHandle->mergeLimit) {
    if (pHandle->mergeLimitReachedFn) {
      pHandle->mergeLimitReachedFn(pOrigBlk->info.id.uid, pHandle->mergeLimitReachedParam);
    }
    keepRows = pHandle->mergeLimit > prevRows ? (pHandle->mergeLimit - prevRows) : 0;
  }
 
  if (keepRows == 0) {
    *pSkipBlock = true;
    return pOrigBlk; 
  }

  *pSkipBlock = false;
  SSDataBlock* pBlock = NULL;
  if (keepRows != pOrigBlk->info.rows) {
    pBlock = blockDataExtractBlock(pOrigBlk, 0, keepRows);
    *pExtractedBlock = true;
  } else {
    *pExtractedBlock = false;
    pBlock = pOrigBlk;
  }
  return pBlock;
}

static int32_t createBlocksMergeSortInitialSources(SSortHandle* pHandle) {
  size_t  nSrc = taosArrayGetSize(pHandle->pOrderedSource);
  SArray* aExtSrc = taosArrayInit(nSrc, POINTER_BYTES);

  size_t maxBufSize = (pHandle->bSortByRowId) ? pHandle->extRowsMemSize : (pHandle->numOfPages * pHandle->pageSize);

  int32_t code = createPageBuf(pHandle);
  if (code != TSDB_CODE_SUCCESS) {
    taosArrayDestroy(aExtSrc);
    return code;
  }

  SSortSource* pSrc = taosArrayGetP(pHandle->pOrderedSource, 0);
  int32_t      szSort = 0;

  SBlockOrderInfo* pOrigTsOrder = (!pHandle->bSortByRowId) ? 
          taosArrayGet(pHandle->pSortInfo, 0) : taosArrayGet(pHandle->aExtRowsOrders, 0);
  if (pOrigTsOrder->order == TSDB_ORDER_ASC) {
    pHandle->currMergeLimitTs = INT64_MAX;
  } else {
    pHandle->currMergeLimitTs = INT64_MIN;
  }

  SSHashObj* mTableNumRows = tSimpleHashInit(8192, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT));
  SArray* aBlkSort = taosArrayInit(8, POINTER_BYTES);
  SSHashObj* mUidBlk = tSimpleHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT));
  while (1) {
    SSDataBlock* pBlk = pHandle->fetchfp(pSrc->param);

    bool bExtractedBlock = false;
    bool bSkipBlock = false;
    if (pBlk != NULL && pHandle->mergeLimit > 0) {
      pBlk = getRowsBlockWithinMergeLimit(pHandle, mTableNumRows, pBlk, &bExtractedBlock, &bSkipBlock);
      if (bSkipBlock) {
        continue;
      }
    }

    if (pBlk != NULL) {
      SColumnInfoData* tsCol = taosArrayGet(pBlk->pDataBlock, pOrigTsOrder->slotId);
      int64_t          firstRowTs = *(int64_t*)tsCol->pData;
      if ((pOrigTsOrder->order == TSDB_ORDER_ASC && firstRowTs > pHandle->currMergeLimitTs) ||
          (pOrigTsOrder->order == TSDB_ORDER_DESC && firstRowTs < pHandle->currMergeLimitTs)) {
        if (bExtractedBlock) {
          blockDataDestroy(pBlk);
        }
        continue;
      }
    }

    if (pBlk != NULL) {
      szSort += blockDataGetSize(pBlk);
      void* ppBlk = tSimpleHashGet(mUidBlk, &pBlk->info.id.uid, sizeof(pBlk->info.id.uid));
      if (ppBlk != NULL) {
        SSDataBlock* tBlk = *(SSDataBlock**)(ppBlk);
        blockDataMerge(tBlk, pBlk);
        if (bExtractedBlock) {
          blockDataDestroy(pBlk);
        }
      } else {
        SSDataBlock* tBlk = (bExtractedBlock) ? pBlk : createOneDataBlock(pBlk, true);
        tSimpleHashPut(mUidBlk, &pBlk->info.id.uid, sizeof(pBlk->info.id.uid), &tBlk, POINTER_BYTES);
        taosArrayPush(aBlkSort, &tBlk);
      }
    }

    if ((pBlk != NULL && szSort > maxBufSize) || (pBlk == NULL && szSort > 0)) {
      tSimpleHashClear(mUidBlk);

      int64_t p = taosGetTimestampUs();
      if (pHandle->bSortByRowId) {
        tsortOpenRegion(pHandle);
      }
      code = sortBlocksToExtSource(pHandle, aBlkSort, aExtSrc);

      if (code != TSDB_CODE_SUCCESS) {
        for (int32_t i = 0; i < taosArrayGetSize(aBlkSort); ++i) {
          blockDataDestroy(taosArrayGetP(aBlkSort, i));
        }
        taosArrayClear(aBlkSort);
        break;	
      }
      if (pHandle->bSortByRowId) {
        tsortCloseRegion(pHandle);
      }
      int64_t el = taosGetTimestampUs() - p;
      pHandle->sortElapsed += el;

      for (int32_t i = 0; i < taosArrayGetSize(aBlkSort); ++i) {
        blockDataDestroy(taosArrayGetP(aBlkSort, i));
      }
      taosArrayClear(aBlkSort);
      szSort = 0;
      qDebug("source %zu created", taosArrayGetSize(aExtSrc));
    }

    if (pBlk == NULL) {
      break;
    }

    if (tsortIsClosed(pHandle)) {
      tSimpleHashClear(mUidBlk);
      for (int32_t i = 0; i < taosArrayGetSize(aBlkSort); ++i) {
        blockDataDestroy(taosArrayGetP(aBlkSort, i));
      }
      taosArrayClear(aBlkSort);
      break;
    }
  }

  tSimpleHashCleanup(mUidBlk);
  for (int32_t i = 0; i < taosArrayGetSize(aBlkSort); ++i) {
    blockDataDestroy(taosArrayGetP(aBlkSort, i));
  }
  taosArrayDestroy(aBlkSort);
  tsortClearOrderdSource(pHandle->pOrderedSource, NULL, NULL);
  if (!tsortIsClosed(pHandle)) {
    taosArrayAddAll(pHandle->pOrderedSource, aExtSrc);
  }
  taosArrayDestroy(aExtSrc);
  tSimpleHashCleanup(mTableNumRows);
  if (pHandle->bSortByRowId) {
    tsortFinalizeRegions(pHandle);
  }
  pHandle->type = SORT_SINGLESOURCE_SORT;
  return code;
}

static void freeSSortSource(SSortSource* source) {
  if (NULL == source) return;
  if (source->param && !source->onlyRef) {
    taosMemoryFree(source->param);
  }
  if (!source->onlyRef && source->src.pBlock) {
    blockDataDestroy(source->src.pBlock);
    source->src.pBlock = NULL;
  }
  taosMemoryFree(source);
}

static int32_t createBlocksQuickSortInitialSources(SSortHandle* pHandle) {
  int32_t code = 0;
  size_t  sortBufSize = pHandle->numOfPages * pHandle->pageSize;

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
      freeSSortSource(source);
      return code;
    }

    size_t size = blockDataGetSize(pHandle->pDataBlock);
    if (size > sortBufSize) {
      // Perform the in-memory sort and then flush data in the buffer into disk.
      int64_t p = taosGetTimestampUs();
      code = blockDataSort(pHandle->pDataBlock, pHandle->pSortInfo);
      if (code != 0) {
        freeSSortSource(source);
        return code;
      }

      int64_t el = taosGetTimestampUs() - p;
      pHandle->sortElapsed += el;
      if (pHandle->pqMaxRows > 0) blockDataKeepFirstNRows(pHandle->pDataBlock, pHandle->pqMaxRows);
      code = doAddToBuf(pHandle->pDataBlock, pHandle);
      if (code != TSDB_CODE_SUCCESS) {
        freeSSortSource(source);
        return code;
      }
    }
  }

  freeSSortSource(source);

  if (pHandle->pDataBlock != NULL && pHandle->pDataBlock->info.rows > 0) {
    size_t size = blockDataGetSize(pHandle->pDataBlock);

    // Perform the in-memory sort and then flush data in the buffer into disk.
    int64_t p = taosGetTimestampUs();

    code = blockDataSort(pHandle->pDataBlock, pHandle->pSortInfo);
    if (code != 0) {
      return code;
    }

    if (pHandle->pqMaxRows > 0) blockDataKeepFirstNRows(pHandle->pDataBlock, pHandle->pqMaxRows);
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
  return code;
}

static int32_t createInitialSources(SSortHandle* pHandle) {
  int32_t code = 0;

  if (pHandle->type == SORT_SINGLESOURCE_SORT) {
    code = createBlocksQuickSortInitialSources(pHandle);
  } else if (pHandle->type == SORT_BLOCK_TS_MERGE) {
    code = createBlocksMergeSortInitialSources(pHandle);
  }
  qDebug("%zu sources created", taosArrayGetSize(pHandle->pOrderedSource));
  return code;
}

static bool tsortOpenForBufMergeSort(SSortHandle* pHandle) {
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
  atomic_val_compare_exchange_8(&pHandle->closed, 0, 1);
  return TSDB_CODE_SUCCESS;
}

bool tsortIsClosed(SSortHandle* pHandle) {
  return atomic_val_compare_exchange_8(&pHandle->closed, 1, 2);
}

void tsortSetClosed(SSortHandle* pHandle) {
  atomic_store_8(&pHandle->closed, 2);
}

void tsortSetMergeLimit(SSortHandle* pHandle, int64_t mergeLimit) {
  pHandle->mergeLimit = mergeLimit;
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

static STupleHandle* tsortBufMergeSortNextTuple(SSortHandle* pHandle) {
  if (tsortIsClosed(pHandle)) {
    return NULL;
  }
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

static bool tsortIsForceUsePQSort(SSortHandle* pHandle) {
  return pHandle->forceUsePQSort == true;
}

void tsortSetForceUsePQSort(SSortHandle* pHandle) {
  pHandle->forceUsePQSort = true;
}

static bool tsortIsPQSortApplicable(SSortHandle* pHandle) {
  if (pHandle->type != SORT_SINGLESOURCE_SORT) return false;
  if (tsortIsForceUsePQSort(pHandle)) return true;
  uint64_t maxRowsFitInMemory = pHandle->pqSortBufSize / (pHandle->pqMaxTupleLength + sizeof(char*));
  return maxRowsFitInMemory > pHandle->pqMaxRows;
}

static bool tsortPQCompFn(void* a, void* b, void* param) {
  SSortHandle* pHandle = param;
  int32_t res = pHandle->comparFn(a, b, param);
  if (res < 0) return 1;
  return 0;
}

static bool tsortPQComFnReverse(void*a, void* b, void* param) {
  SSortHandle* pHandle = param;
  int32_t res = pHandle->comparFn(a, b, param);
  if (res > 0) return 1;
  return 0;
}

static int32_t tupleComparFn(const void* pLeft, const void* pRight, void* param) {
  TupleDesc* pLeftDesc = (TupleDesc*)pLeft;
  TupleDesc* pRightDesc = (TupleDesc*)pRight;

  SSortHandle* pHandle = (SSortHandle*)param;
  SArray* orderInfo = (SArray*)pHandle->pSortInfo;
  uint32_t colNum = blockDataGetNumOfCols(pHandle->pDataBlock);
  for (int32_t i = 0; i < orderInfo->size; ++i) {
    SBlockOrderInfo* pOrder = TARRAY_GET_ELEM(orderInfo, i);
    void *lData = tupleDescGetField(pLeftDesc, pOrder->slotId, colNum);
    void *rData = tupleDescGetField(pRightDesc, pOrder->slotId, colNum);
    if (!lData && !rData) continue;
    if (!lData) return pOrder->nullFirst ? -1 : 1;
    if (!rData) return pOrder->nullFirst ? 1 : -1;

    int32_t       type = ((SColumnInfoData*)taosArrayGet(pHandle->pDataBlock->pDataBlock, pOrder->slotId))->info.type;
    __compar_fn_t fn = getKeyComparFunc(type, pOrder->order);

    int32_t ret = fn(lData, rData);
    if (ret == 0) {
      continue;
    } else {
      return ret;
    }
  }
  return 0;
}

static int32_t tsortOpenForPQSort(SSortHandle* pHandle) {
  pHandle->pBoundedQueue = createBoundedQueue(pHandle->pqMaxRows, tsortPQCompFn, destroyTuple, pHandle);
  if (NULL == pHandle->pBoundedQueue) return TSDB_CODE_OUT_OF_MEMORY;
  tsortSetComparFp(pHandle, tupleComparFn);

  SSortSource** pSource = taosArrayGet(pHandle->pOrderedSource, 0);
  SSortSource*  source = *pSource;

  pHandle->pDataBlock = NULL;
  uint32_t tupleLen = 0;
  PriorityQueueNode pqNode;
  while (1) {
    // fetch data
    SSDataBlock* pBlock = pHandle->fetchfp(source->param);
    if (NULL == pBlock) break;

    if (pHandle->beforeFp != NULL) {
      pHandle->beforeFp(pBlock, pHandle->param);
    }
    if (pHandle->pDataBlock == NULL) {
      pHandle->pDataBlock = createOneDataBlock(pBlock, false);
    }
    if (pHandle->pDataBlock == NULL) return TSDB_CODE_OUT_OF_MEMORY;

    size_t colNum = blockDataGetNumOfCols(pBlock);

    if (tupleLen == 0) {
      for (size_t colIdx = 0; colIdx < colNum; ++colIdx) {
        SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, colIdx);
        tupleLen += pCol->info.bytes;
        if (IS_VAR_DATA_TYPE(pCol->info.type)) {
          tupleLen += sizeof(VarDataLenT);
        }
      }
    }
    ReferencedTuple refTuple = {.desc.data = (char*)pBlock, .desc.type = ReferencedTupleType, .rowIndex = 0};
    for (size_t rowIdx = 0; rowIdx < pBlock->info.rows; ++rowIdx) {
      refTuple.rowIndex = rowIdx;
      pqNode.data = &refTuple;
      PriorityQueueNode* pPushedNode = taosBQPush(pHandle->pBoundedQueue, &pqNode);
      if (!pPushedNode) {
        // do nothing if push failed
      } else {
        pPushedNode->data = createAllocatedTuple(pBlock, colNum, tupleLen, rowIdx);
        if (pPushedNode->data == NULL) return TSDB_CODE_OUT_OF_MEMORY;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

static STupleHandle* tsortPQSortNextTuple(SSortHandle* pHandle) {
  if (pHandle->pDataBlock == NULL) { // when no input stream datablock
    return NULL;
  }
  blockDataCleanup(pHandle->pDataBlock);
  blockDataEnsureCapacity(pHandle->pDataBlock, 1);
  // abandon the top tuple if queue size bigger than max size
  if (taosBQSize(pHandle->pBoundedQueue) == taosBQMaxSize(pHandle->pBoundedQueue) + 1) {
    taosBQPop(pHandle->pBoundedQueue);
  }
  if (pHandle->tmpRowIdx == 0) {
    // sort the results
    taosBQSetFn(pHandle->pBoundedQueue, tsortPQComFnReverse);
    taosBQBuildHeap(pHandle->pBoundedQueue);
  }
  if (taosBQSize(pHandle->pBoundedQueue) > 0) {
    uint32_t           colNum = blockDataGetNumOfCols(pHandle->pDataBlock);
    PriorityQueueNode* node = taosBQTop(pHandle->pBoundedQueue);
    char*              pTuple = ((TupleDesc*)node->data)->data;

    for (uint32_t i = 0; i < colNum; ++i) {
      void* pData = tupleGetField(pTuple, i, colNum);
      if (!pData) {
        colDataSetNULL(bdGetColumnInfoData(pHandle->pDataBlock, i), 0);
      } else {
        colDataSetVal(bdGetColumnInfoData(pHandle->pDataBlock, i), 0, pData, false);
      }
    }
    pHandle->pDataBlock->info.rows++;
    pHandle->tmpRowIdx++;
    taosBQPop(pHandle->pBoundedQueue);
  }
  if (pHandle->pDataBlock->info.rows == 0) return NULL;
  pHandle->tupleHandle.pBlock = pHandle->pDataBlock;
  return &pHandle->tupleHandle;
}

static STupleHandle* tsortSingleTableMergeNextTuple(SSortHandle* pHandle) {
  if (1 == pHandle->numOfCompletedSources) return NULL;
  if (pHandle->tupleHandle.pBlock && pHandle->tupleHandle.rowIndex + 1 < pHandle->tupleHandle.pBlock->info.rows) {
    pHandle->tupleHandle.rowIndex++;
  } else {
    if (pHandle->tupleHandle.rowIndex == -1) return NULL;
    SSortSource** pSource = taosArrayGet(pHandle->pOrderedSource, 0);
    SSortSource*  source = *pSource;
    SSDataBlock*  pBlock = pHandle->fetchfp(source->param);
    if (!pBlock || pBlock->info.rows == 0) {
      setCurrentSourceDone(source, pHandle);
      pHandle->tupleHandle.pBlock = NULL;
      return NULL;
    }
    pHandle->tupleHandle.pBlock = pBlock;
    pHandle->tupleHandle.rowIndex = 0;
  }
  return &pHandle->tupleHandle;
}

int32_t tsortOpen(SSortHandle* pHandle) {
  if (pHandle->opened) {
    return 0;
  }

  if (pHandle->fetchfp == NULL || pHandle->comparFn == NULL) {
    return -1;
  }

  pHandle->opened = true;
  if (tsortIsPQSortApplicable(pHandle))
    return tsortOpenForPQSort(pHandle);
  else
    return tsortOpenForBufMergeSort(pHandle);
}

STupleHandle* tsortNextTuple(SSortHandle* pHandle) {
  if (pHandle->singleTableMerge)
    return tsortSingleTableMergeNextTuple(pHandle);
  else if (pHandle->pBoundedQueue)
    return tsortPQSortNextTuple(pHandle);
  else
    return tsortBufMergeSortNextTuple(pHandle);
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

int32_t tsortCompAndBuildKeys(const SArray* pSortCols, char* keyBuf, int32_t* keyLen,
                              const STupleHandle* pTuple) {
  int32_t ret;
  if (0 == compKeys(pSortCols, keyBuf, *keyLen, pTuple->pBlock, pTuple->rowIndex)) {
    ret = 0;
  } else {
    *keyLen = buildKeys(keyBuf, pSortCols, pTuple->pBlock, pTuple->rowIndex);
    ret = 1;
  }
  return ret;
}

void tsortSetMergeLimitReachedFp(SSortHandle* pHandle, void (*mergeLimitReachedCb)(uint64_t tableUid, void* param), void* param) {
  pHandle->mergeLimitReachedFn = mergeLimitReachedCb;
  pHandle->mergeLimitReachedParam = param;
}
