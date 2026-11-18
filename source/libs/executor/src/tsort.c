/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include "executil.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tdef.h"
#include "theap.h"
#include "tlosertree.h"
#include "tpagedbuf.h"
#include "tsimplehash.h"
#include "tsort.h"
#include "tutil.h"

#define AllocatedTupleType  0
#define ReferencedTupleType 1  // tuple references to one row in pDataBlock

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

  TdFilePtr pTdFile;
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

  uint64_t      pqMaxRows;
  uint32_t      pqMaxTupleLength;
  uint32_t      pqSortBufSize;
  bool          forceUsePQSort;
  bool          tsPkSort;
  bool          inputOrderedBySortKeys;
  // First field-read failure seen by tupleComparFn. A comparator can only
  // return an order verdict, so it records the error here and the PQ sort
  // loop surfaces it after the push that triggered the comparison.
  int32_t       comparErr;
  BoundedQueue* pBoundedQueue;
  uint32_t      tmpRowIdx;
  int64_t       mergeLimit;
  int64_t       currMergeLimitTs;
  // Live cross-table threshold for SORT_BLOCK_TS_MERGE with a merge limit.
  // pTsHeap keeps the mergeLimit best (in sort order) timestamps admitted so
  // far during the fetch loop; its worst element is an upper bound of the
  // mergeLimit-th best ts, which tightens currMergeLimitTs block by block.
  // It is only enabled when mergeLimit fits in TSORT_MERGE_LIMIT_TS_HEAP_CAP:
  // a smaller heap would expose the k-th best ts as the root, which is a
  // tighter bound than the N-th best and would prune rows that still belong
  // to the first mergeLimit rows.
  SArray*        pTsHeap;
  int32_t        tsHeapCap;

  int32_t           sourceId;
  SSDataBlock*      pDataBlock;
  SMsortComparParam cmpParam;
  int32_t           numOfCompletedSources;
  /* Cumulative time spent in fetchfp (raw-data pull from sources). */
  int64_t           fetchTotalUs;
  bool              opened;
  int32_t           openCode;
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

  bool          bSortByRowId;
  SSortMemFile* pExtRowsMemFile;
  // reused block-sort scratch (tuple index + gather column buffers) owned by
  // this handle; keeps steady-state block sorts allocation-free
  SBlockSortScratch* pBlkSortScratch;
  int32_t       extRowBytes;
  int32_t       extRowsPageSize;
  int32_t       extRowsMemSize;
  int32_t       srcTsSlotId;
  SArray*       aExtRowsOrders;
  bool          bSortPk;
  void (*mergeLimitReachedFn)(uint64_t tableUid, void* param);
  void* mergeLimitReachedParam;
};

static void    destroySortMemFile(SSortHandle* pHandle);
static int32_t getRowBufFromExtMemFile(SSortHandle* pHandle, int32_t regionId, int32_t tupleOffset, int32_t rowLen,
                                       char** ppRow, bool* pFreeRow);
void           tsortSetSingleTableMerge(SSortHandle* pHandle) { pHandle->singleTableMerge = true; }

void tsortSetAbortCheckFn(SSortHandle* pHandle, bool (*checkFn)(void*), void* param) {
  pHandle->abortCheckFn = checkFn;
  pHandle->abortCheckParam = param;
}

static int32_t msortComparFn(const void* pLeft, const void* pRight, void* param);

// | offset[0] | offset[1] |....| nullbitmap | data |...|
static void* createTuple(uint32_t columnNum, uint32_t tupleLen) {
  uint64_t totalLen = (uint64_t)sizeof(uint32_t) * columnNum + ((uint64_t)columnNum + 7) / 8 + tupleLen;
  if (totalLen > UINT32_MAX) {
    terrno = TSDB_CODE_INVALID_MSG;
    return NULL;
  }
  return taosMemoryCalloc(1, (size_t)totalLen);
}

static void destoryAllocatedTuple(void* t) { taosMemoryFree(t); }

#define tupleOffset(tuple, colIdx)                ((uint32_t*)(tuple + sizeof(uint32_t) * colIdx))
#define tupleSetOffset(tuple, colIdx, offset)     (*tupleOffset(tuple, colIdx) = offset)
#define tupleSetNull(tuple, colIdx, colNum)       colDataSetNull_f((char*)tuple + sizeof(uint32_t) * colNum, colIdx)
#define tupleColIsNull(tuple, colIdx, colNum)     BMIsNull((char*)tuple + sizeof(uint32_t) * colNum, colIdx)
#define tupleGetDataStartOffset(colNum)           (sizeof(uint32_t) * colNum + BitmapLen(colNum))
#define tupleSetData(tuple, offset, data, length) memcpy(tuple + offset, data, length)

/**
 * @param t the tuple pointer addr, if realloced, *t is changed to the new addr
 * @param offset copy data into pTuple start from offset
 * @param colIndex the columnIndex, for setting null bitmap
 * @return the next offset to add field
 * */
static inline int32_t tupleAddField(char** t, uint32_t colNum, uint32_t offset, uint32_t colIdx, void* data,
                                    size_t length, bool isNull, uint32_t tupleLen, uint32_t* pOffset) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (length > UINT32_MAX - offset) return TSDB_CODE_INVALID_MSG;
  tupleSetOffset(*t, colIdx, offset);

  if (isNull) {
    tupleSetNull(*t, colIdx, colNum);
  } else {
    uint64_t initialLen = (uint64_t)tupleLen + tupleGetDataStartOffset(colNum);
    if ((uint64_t)offset + length > initialLen) {
      void* px = taosMemoryRealloc(*t, (size_t)offset + length);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);

      *t = px;
    }
    tupleSetData(*t, offset, data, length);
  }

  (*pOffset) = offset + (uint32_t)length;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void* tupleGetField(char* t, uint32_t colIdx, uint32_t colNum) {
  if (tupleColIsNull(t, colIdx, colNum)) {
    return NULL;
  }

  return t + *tupleOffset(t, colIdx);
}

int32_t tsortGetSortedDataBlock(const SSortHandle* pSortHandle, SSDataBlock** pBlock) {
  *pBlock = NULL;
  if (pSortHandle->pDataBlock == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  return createOneDataBlock(pSortHandle->pDataBlock, false, pBlock);
}

typedef struct TupleDesc {
  uint8_t type;
  char*   data;  // if type is AllocatedTuple, then points to the created tuple, otherwise points to the DataBlock
} TupleDesc;

typedef struct ReferencedTuple {
  TupleDesc desc;
  size_t    rowIndex;
} ReferencedTuple;

static int32_t createAllocatedTuple(SSDataBlock* pBlock, size_t colNum, uint32_t tupleLen, size_t rowIdx,
                                    TupleDesc** pDesc) {
  int32_t    code = TSDB_CODE_SUCCESS;
  TupleDesc* t = taosMemoryCalloc(1, sizeof(TupleDesc));
  if (t == NULL) {
    return terrno;
  }

  if (colNum > UINT32_MAX || (uint64_t)sizeof(uint32_t) * colNum + ((uint64_t)colNum + 7) / 8 + tupleLen > UINT32_MAX) {
    taosMemoryFree(t);
    return TSDB_CODE_INVALID_MSG;
  }
  void* pTuple = createTuple(colNum, tupleLen);
  if (!pTuple) {
    taosMemoryFree(t);
    return terrno;
  }

  size_t   colLen = 0;
  uint32_t offset = (uint32_t)((uint64_t)sizeof(uint32_t) * colNum + ((uint64_t)colNum + 7) / 8);
  for (size_t colIdx = 0; colIdx < colNum; ++colIdx) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, colIdx);
    if (pCol == NULL) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      taosMemoryFree(pTuple);
      taosMemoryFree(t);
      return terrno;
    }

    if (colDataIsNull_s(pCol, rowIdx)) {
      code = tupleAddField((char**)&pTuple, colNum, offset, colIdx, 0, 0, true, tupleLen, &offset);
    } else {
      colLen = colDataGetRowLength(pCol, rowIdx);
      code = tupleAddField((char**)&pTuple, colNum, offset, colIdx, colDataGetData(pCol, rowIdx), colLen, false,
                           tupleLen, &offset);
    }
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      taosMemoryFree(pTuple);
      taosMemoryFree(t);
      return code;
    }
  }

  t->type = AllocatedTupleType;
  t->data = pTuple;

  *pDesc = t;
  return code;
}

int32_t tupleDescGetField(const TupleDesc* pDesc, int32_t colIdx, uint32_t colNum, void** pResult) {
  *pResult = NULL;

  if (pDesc->type == ReferencedTupleType) {
    ReferencedTuple* pRefTuple = (ReferencedTuple*)pDesc;
    SColumnInfoData* pCol = taosArrayGet(((SSDataBlock*)pDesc->data)->pDataBlock, colIdx);
    if (pCol == NULL) {
      return terrno;
    }

    if (colDataIsNull_s(pCol, pRefTuple->rowIndex)) {
      return TSDB_CODE_SUCCESS;
    }

    *pResult = colDataGetData(pCol, pRefTuple->rowIndex);
  } else {
    *pResult = tupleGetField(pDesc->data, colIdx, colNum);
  }

  return 0;
}

void destroyTuple(void* t) {
  TupleDesc* pDesc = t;
  if (pDesc != NULL && pDesc->type == AllocatedTupleType) {
    destoryAllocatedTuple(pDesc->data);
    taosMemoryFree(pDesc);
  }
}

/**
 *
 * @param type
 * @return
 */
int32_t tsortCreateSortHandle(SArray* pSortInfo, int32_t type, int32_t pageSize, int32_t numOfPages,
                              SSDataBlock* pBlock, const char* idstr, uint64_t pqMaxRows, uint32_t pqMaxTupleLength,
                              uint32_t pqSortBufSize, SSortHandle** pHandle) {
  int32_t code = 0;
  int32_t lino = 0;

  QRY_PARAM_CHECK(pHandle);
  SSortHandle* pSortHandle = taosMemoryCalloc(1, sizeof(SSortHandle));
  QUERY_CHECK_NULL(pSortHandle, code, lino, _err, terrno);

  pSortHandle->type = type;
  pSortHandle->pageSize = pageSize;
  pSortHandle->numOfPages = numOfPages;
  pSortHandle->pSortInfo = taosArrayDup(pSortInfo, NULL);
  QUERY_CHECK_NULL(pSortHandle->pSortInfo, code, lino, _err, terrno);

  pSortHandle->loops = 0;
  pSortHandle->pqMaxTupleLength = pqMaxTupleLength;
  if (pqMaxRows != 0) {
    pSortHandle->pqSortBufSize = pqSortBufSize;
    pSortHandle->pqMaxRows = pqMaxRows;
  }

  pSortHandle->forceUsePQSort = false;
  if (pBlock != NULL) {
    code = createOneDataBlock(pBlock, false, &pSortHandle->pDataBlock);
    QUERY_CHECK_CODE(code, lino, _err);
  }

  pSortHandle->mergeLimit = -1;

  pSortHandle->pOrderedSource = taosArrayInit(4, POINTER_BYTES);
  QUERY_CHECK_NULL(pSortHandle->pOrderedSource, code, lino, _err, terrno);

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
    QUERY_CHECK_NULL(pSortHandle->idStr, code, lino, _err, terrno);
  }

  *pHandle = pSortHandle;
  return code;

_err:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  if (pSortHandle) {
    tsortDestroySortHandle(pSortHandle);
  }
  return code;
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

void tsortClearOrderedSource(SArray* pOrderedSource, int64_t* fetchUs, int64_t* fetchNum) {
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
  tsortClearOrderedSource(pSortHandle->pOrderedSource, &fetchUs, &fetchNum);
  qDebug("all source fetch time: %" PRId64 "us num:%" PRId64 " %s", fetchUs, fetchNum, pSortHandle->idStr);

  taosArrayDestroy(pSortHandle->pOrderedSource);
  blockDataSortFreeScratch(&pSortHandle->pBlkSortScratch);
  if (pSortHandle->pExtRowsMemFile != NULL) {
    destroySortMemFile(pSortHandle);
  }

  taosArrayDestroy(pSortHandle->pSortInfo);
  taosArrayDestroy(pSortHandle->aExtRowsOrders);
  pSortHandle->aExtRowsOrders = NULL;
  // Normally released at the end of createBlocksMergeSortInitialSources; kept
  // here so an aborted open cannot leak it.
  taosArrayDestroy(pSortHandle->pTsHeap);
  pSortHandle->pTsHeap = NULL;
  taosMemoryFreeClear(pSortHandle);
}

int32_t tsortAddSource(SSortHandle* pSortHandle, void* pSource) {
  void* p = taosArrayPush(pSortHandle->pOrderedSource, &pSource);
  return (p != NULL) ? TSDB_CODE_SUCCESS : terrno;
}

static int32_t doAddNewExternalMemSource(SDiskbasedBuf* pBuf, SArray* pAllSources, SSDataBlock* pBlock,
                                         int32_t* sourceId, SArray* pPageIdList) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SSortSource* pSource = taosMemoryCalloc(1, sizeof(SSortSource));
  QUERY_CHECK_NULL(pSource, code, lino, _err, terrno);

  pSource->src.pBlock = pBlock;
  pSource->pageIdList = pPageIdList;

  SSortSource** p = taosArrayPush(pAllSources, &pSource);
  QUERY_CHECK_NULL(p, code, lino, _err, terrno);
  pSource = NULL;

  (*sourceId) += 1;

  int32_t rowSize = blockDataGetSerialRowSize((*p)->src.pBlock);

  // The value of numOfRows must be greater than 0, which is guaranteed by the previous memory allocation
  int32_t numOfRows =
      (getBufPageSize(pBuf) - blockDataGetSerialMetaSize(taosArrayGetSize(pBlock->pDataBlock))) / rowSize;
  QUERY_CHECK_CONDITION((numOfRows > 0), code, lino, _err, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);

  code = blockDataEnsureCapacity((*p)->src.pBlock, numOfRows);
  QUERY_CHECK_CODE(code, lino, _err);

  return code;

_err:
  if (pSource) taosMemoryFree(pSource);
  qError("sort failed at %s:%d since %s", __func__, lino, tstrerror(code));
  return code;
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
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    dBufSetPrintInfo(pHandle->pBuf);
  }

  SArray* pPageIdList = taosArrayInit(4, sizeof(int32_t));
  if (pPageIdList == NULL) {
    return terrno;
  }

  while (start < pDataBlock->info.rows) {
    int32_t stop = 0;

    int32_t code = blockDataSplitRows(pDataBlock, pDataBlock->info.hasVarCol, start, &stop, pHandle->pageSize);
    if (code) {
      taosArrayDestroy(pPageIdList);
      return code;
    }

    SSDataBlock* p = NULL;
    code = blockDataExtractBlock(pDataBlock, start, stop - start + 1, &p);
    if (code) {
      taosArrayDestroy(pPageIdList);
      return code;
    }

    int32_t pageId = -1;
    void*   pPage = getNewBufPage(pHandle->pBuf, &pageId);
    if (pPage == NULL) {
      taosArrayDestroy(pPageIdList);
      blockDataDestroy(p);
      return terrno;
    }

    void* px = taosArrayPush(pPageIdList, &pageId);
    if (px == NULL) {
      taosArrayDestroy(pPageIdList);
      blockDataDestroy(p);
      return terrno;
    }

    int32_t size = blockDataGetSize(p) + sizeof(int32_t) +
                   taosArrayGetSize(p->pDataBlock) * (sizeof(int32_t) + 2 * sizeof(uint8_t));
    if (size > getBufPageSize(pHandle->pBuf)) {
      qError("sort failed at: %s:%d", __func__, __LINE__);
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    code = blockDataToBuf(pPage, p);
    if (code) {
      return code;
    }

    setBufPageDirty(pPage, true);
    releaseBufPage(pHandle->pBuf, pPage);

    blockDataDestroy(p);
    start = stop + 1;
  }

  blockDataCleanup(pDataBlock);

  SSDataBlock* pBlock = NULL;
  int32_t      code = createOneDataBlock(pDataBlock, false, &pBlock);
  if (code) {
    return code;
  }

  code = doAddNewExternalMemSource(pHandle->pBuf, pHandle->pOrderedSource, pBlock, &pHandle->sourceId, pPageIdList);
  if (code) {
    blockDataDestroy(pBlock);
    taosArrayDestroy(pPageIdList);
  }
  return code;
}

static void setCurrentSourceDone(SSortSource* pSource, SSortHandle* pHandle) {
  pSource->src.rowIndex = -1;
  ++pHandle->numOfCompletedSources;
}

static int32_t sortComparInit(SMsortComparParam* pParam, SArray* pSources, int32_t startIndex, int32_t endIndex,
                              SSortHandle* pHandle) {
  pParam->pSources = taosArrayGet(pSources, startIndex);
  if (pParam->pSources == NULL) {
    return terrno;
  }

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
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    } else {
      dBufSetPrintInfo(pHandle->pBuf);
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
      if (pPgId == NULL) {
        return terrno;
      }

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
      TAOS_CHECK_RETURN(pHandle->fetchfp(pSource->param, &pSource->src.pBlock));

      // set current source is done. An empty (0-row) block must be treated the same way:
      // the loser-tree init compares element [0] of each source, which reads past the
      // (1-byte placeholder) var-offset array of an empty block -- heap-buffer-overflow
      // under ASan, garbage ordering otherwise.
      if (pSource->src.pBlock == NULL || pSource->src.pBlock->info.rows == 0) {
        setCurrentSourceDone(pSource, pHandle);
      }
    }

    int64_t et = taosGetTimestampUs();
    pHandle->fetchTotalUs += (et - st);
    qDebug("init for merge sort completed, elapsed time:%.2f ms, %s", (et - st) / 1000.0, pHandle->idStr);
  }

  return code;
}

static int32_t appendOneRowToDataBlock(SSDataBlock* pBlock, const SSDataBlock* pSource, int32_t* rowIndex) {
  int32_t code = 0;

  for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);
    if (pColInfo == NULL) {
      return terrno;
    }

    SColumnInfoData* pSrcColInfo = taosArrayGet(pSource->pDataBlock, i);
    if (pSrcColInfo == NULL) {
      return terrno;
    }

    // A fixed-width source column without NULLs needs one memcpy per row; the
    // generic colDataSetVal path re-dispatches on the type for every value.
    // Both sides must be the same fixed-width type: nullbitmap and varmeta are
    // a union, so treating a varlen destination as bitmap/rows would corrupt
    // its offset array.
    if (colDataCanCopyFixed(pColInfo, pSrcColInfo)) {
      if (pColInfo->nullbitmap != NULL) {
        // a reused destination can still carry a stale bit for this row
        colDataClearNull_f(pColInfo->nullbitmap, pBlock->info.rows);
      } else {
        // No bitmap means no row ever carried a NULL bit (writing one requires
        // a bitmap), so a set hasNull flag is stale and must not outlive the
        // non-NULL value written below.
        pColInfo->hasNull = false;
      }
      memcpy(pColInfo->pData + (size_t)pBlock->info.rows * pColInfo->info.bytes,
             pSrcColInfo->pData + (size_t)(*rowIndex) * pColInfo->info.bytes, pColInfo->info.bytes);
      continue;
    }

    // Keep the previous semantics exactly: colDataIsNull already returns false
    // when hasNull is clear, and an unmaterialized source column (pData == NULL)
    // is skipped below rather than turned into an explicit NULL.
    bool isNull = colDataIsNull(pSrcColInfo, pSource->info.rows, *rowIndex, NULL);
    if (isNull) {
      code = colDataSetVal(pColInfo, pBlock->info.rows, NULL, true);
      if (code) {
        return code;
      }
    } else {
      // Unmaterialized source column: keep the historical semantics of leaving
      // the destination cell untouched instead of emitting an explicit NULL.
      if (!pSrcColInfo->pData) continue;
      char* pData = colDataGetData(pSrcColInfo, *rowIndex);
      code = colDataSetVal(pColInfo, pBlock->info.rows, pData, false);
      if (code) {
        return code;
      }
    }
  }

  pBlock->info.rows += 1;
  *rowIndex += 1;
  return code;
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
        blockDataDestroy(pSource->src.pBlock);
        pSource->src.pBlock = NULL;
      } else {
        if (pSource->pageIndex % 512 == 0) {
          qDebug("begin source %p page %d", pSource, pSource->pageIndex);
        }

        int32_t* pPgId = taosArrayGet(pSource->pageIdList, pSource->pageIndex);
        if (pPgId == NULL) {
          return terrno;
        }

        void* pPage = getBufPage(pHandle->pBuf, *pPgId);
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
      TAOS_CHECK_RETURN(pHandle->fetchfp(((SSortSource*)pSource)->param, &pSource->src.pBlock));
      int64_t elapsed = taosGetTimestampUs() - st;
      pSource->fetchUs += elapsed;
      pSource->fetchNum++;
      pHandle->fetchTotalUs += elapsed;
      if (pSource->src.pBlock == NULL || pSource->src.pBlock->info.rows == 0) {
        // same empty-block rule as the init loop above: a 0-row block must complete the
        // source -- the loser tree would otherwise compare element [0] of it and read
        // past the empty block's var-offset placeholder
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

  int32_t code = tMergeTreeAdjust(pTree, leafNodeIndex);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

#ifdef _DEBUG_VIEW
  printf("\nafter adjust:\t");
  tMergeTreePrint(pTree);
#endif
  return TSDB_CODE_SUCCESS;
}

static int32_t getSortedBlockDataInner(SSortHandle* pHandle, SMsortComparParam* cmpParam, int32_t capacity,
                                       SSDataBlock** pRes) {
  *pRes = NULL;

  int32_t code = 0;
  blockDataCleanup(pHandle->pDataBlock);

  while (1) {
    if (cmpParam->numOfSources == pHandle->numOfCompletedSources) {
      break;
    }

    int32_t index = tMergeTreeGetChosenIndex(pHandle->pMergeTree);

    SSortSource* pSource = (*cmpParam).pSources[index];
    code = appendOneRowToDataBlock(pHandle->pDataBlock, pSource->src.pBlock, &pSource->src.rowIndex);
    if (code) {
      return code;
    }

    code = adjustMergeTreeForNextTuple(pSource, pHandle->pMergeTree, pHandle, &pHandle->numOfCompletedSources);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (pHandle->pDataBlock->info.rows >= capacity) {
      *pRes = pHandle->pDataBlock;
      return code;
    }
  }

  *pRes = (pHandle->pDataBlock->info.rows > 0) ? pHandle->pDataBlock : NULL;
  return code;
}

// TODO: improve this function performance

int32_t tsortComparBlockCell(SSDataBlock* pLeftBlock, SSDataBlock* pRightBlock, int32_t leftRowIndex,
                             int32_t rightRowIndex, void* pCompareOrder) {
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
        leftNull = colDataIsNull(pLeftColInfoData, pLeftBlock->info.rows, leftRowIndex,
                                 &pLeftBlock->pBlockAgg[pOrder->slotId]);
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
  int32_t       ret = fn(left1, right1);
  return ret;
}

// Typed comparison kinds for the single-key merge fast path.  Float and double
// stay on the library comparator so NaN and signed-zero ordering do not change.
enum {
  TSORT_KEY_CMP_NONE = 0,
  TSORT_KEY_CMP_I8,
  TSORT_KEY_CMP_I16,
  TSORT_KEY_CMP_I32,
  TSORT_KEY_CMP_I64,
  TSORT_KEY_CMP_U8,
  TSORT_KEY_CMP_U16,
  TSORT_KEY_CMP_U32,
  TSORT_KEY_CMP_U64,
};

#define TSORT_CMP3(lv, rv) ((lv) > (rv) ? 1 : ((lv) < (rv) ? -1 : 0))

static int8_t tsortKeyCmpKind(int8_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_BOOL:
      return TSORT_KEY_CMP_I8;
    case TSDB_DATA_TYPE_UTINYINT:
      return TSORT_KEY_CMP_U8;
    case TSDB_DATA_TYPE_SMALLINT:
      return TSORT_KEY_CMP_I16;
    case TSDB_DATA_TYPE_USMALLINT:
      return TSORT_KEY_CMP_U16;
    case TSDB_DATA_TYPE_INT:
      return TSORT_KEY_CMP_I32;
    case TSDB_DATA_TYPE_UINT:
      return TSORT_KEY_CMP_U32;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      return TSORT_KEY_CMP_I64;
    case TSDB_DATA_TYPE_UBIGINT:
      return TSORT_KEY_CMP_U64;
    default:
      return TSORT_KEY_CMP_NONE;
  }
}

// Payload stride implied by a comparison kind. 0 means "no typed comparison",
// which keeps the fast path off for an unknown kind.
static int32_t tsortKeyCmpWidth(int8_t kind) {
  switch (kind) {
    case TSORT_KEY_CMP_I8:
    case TSORT_KEY_CMP_U8:
      return 1;
    case TSORT_KEY_CMP_I16:
    case TSORT_KEY_CMP_U16:
      return 2;
    case TSORT_KEY_CMP_I32:
    case TSORT_KEY_CMP_U32:
      return 4;
    case TSORT_KEY_CMP_I64:
    case TSORT_KEY_CMP_U64:
      return 8;
    default:
      return 0;
  }
}

static int32_t tsortComparFixedKeyVal(const SColumnInfoData* pLeft, int32_t leftRow, const SColumnInfoData* pRight,
                                      int32_t rightRow, int8_t kind) {
  const char* l = pLeft->pData;
  const char* r = pRight->pData;

  switch (kind) {
    case TSORT_KEY_CMP_I8:
      return TSORT_CMP3(*(const int8_t*)(l + leftRow), *(const int8_t*)(r + rightRow));
    case TSORT_KEY_CMP_U8:
      return TSORT_CMP3(*(const uint8_t*)(l + leftRow), *(const uint8_t*)(r + rightRow));
    case TSORT_KEY_CMP_I16:
      return TSORT_CMP3(*(const int16_t*)(l + leftRow * 2), *(const int16_t*)(r + rightRow * 2));
    case TSORT_KEY_CMP_U16:
      return TSORT_CMP3(*(const uint16_t*)(l + leftRow * 2), *(const uint16_t*)(r + rightRow * 2));
    case TSORT_KEY_CMP_I32:
      return TSORT_CMP3(*(const int32_t*)(l + leftRow * 4), *(const int32_t*)(r + rightRow * 4));
    case TSORT_KEY_CMP_U32:
      return TSORT_CMP3(*(const uint32_t*)(l + leftRow * 4), *(const uint32_t*)(r + rightRow * 4));
    case TSORT_KEY_CMP_I64:
      return TSORT_CMP3(*(const int64_t*)(l + (int64_t)leftRow * 8), *(const int64_t*)(r + (int64_t)rightRow * 8));
    case TSORT_KEY_CMP_U64:
      return TSORT_CMP3(*(const uint64_t*)(l + (int64_t)leftRow * 8), *(const uint64_t*)(r + (int64_t)rightRow * 8));
    default:
      // Unreachable: the caller only enters the fast path with a kind whose
      // tsortKeyCmpWidth() matches the column width, so every value produced by
      // tsortKeyCmpKind() has a case above. Returning 0 here would report
      // "equal" and silently corrupt the merge order, so keep the caller gate
      // authoritative and state the invariant in one place.
      return 0;
  }
}

int32_t msortComparFn(const void* pLeft, const void* pRight, void* param) {
  int32_t pLeftIdx = *(int32_t*)pLeft;
  int32_t pRightIdx = *(int32_t*)pRight;

  SMsortComparParam* pParam = (SMsortComparParam*)param;

  SArray* pInfo = pParam->orderInfo;

  SSortSource* pLeftSource = pParam->pSources[pLeftIdx];
  SSortSource* pRightSource = pParam->pSources[pRightIdx];

  // this input is exhausted, set the special value to denote this
  if (UNLIKELY(pLeftSource->src.rowIndex == -1)) {
    return 1;
  }

  if (UNLIKELY(pRightSource->src.rowIndex == -1)) {
    return -1;
  }

  SSDataBlock* pLeftBlock = pLeftSource->src.pBlock;
  SSDataBlock* pRightBlock = pRightSource->src.pBlock;

  if (pParam->cmpGroupId) {
    if (pLeftBlock->info.id.baseGId != pRightBlock->info.id.baseGId) {
      return pLeftBlock->info.id.baseGId < pRightBlock->info.id.baseGId ? -1 : 1;
    }
    if (pLeftBlock->info.id.groupId != pRightBlock->info.id.groupId) {
      return pLeftBlock->info.id.groupId < pRightBlock->info.id.groupId ? -1 : 1;
    }
  }

  if (pParam->sortType == SORT_BLOCK_TS_MERGE) {
    SColumnInfoData* pLeftTsCol = TARRAY_GET_ELEM(pLeftBlock->pDataBlock, pParam->tsSlotId);
    SColumnInfoData* pRightTsCol = TARRAY_GET_ELEM(pRightBlock->pDataBlock, pParam->tsSlotId);
    int64_t*         leftTs = (int64_t*)(pLeftTsCol->pData) + pLeftSource->src.rowIndex;
    int64_t*         rightTs = (int64_t*)(pRightTsCol->pData) + pRightSource->src.rowIndex;

    int32_t ret = pParam->cmpTsFn(leftTs, rightTs);
    if (ret == 0 && pParam->pPkOrder) {
      ret = tsortComparBlockCell(pLeftBlock, pRightBlock, pLeftSource->src.rowIndex, pRightSource->src.rowIndex,
                                 (SBlockOrderInfo*)pParam->pPkOrder);
    }
    return ret;
  } else if (pParam->sortType == SORT_MULTISOURCE_TS_MERGE) {
    SBlockOrderInfo* pOrder = TARRAY_GET_ELEM(pInfo, 0);
    SColumnInfoData* pLeftTsCol = TARRAY_GET_ELEM(pLeftBlock->pDataBlock, pOrder->slotId);
    SColumnInfoData* pRightTsCol = TARRAY_GET_ELEM(pRightBlock->pDataBlock, pOrder->slotId);

    bool isVarType = IS_VAR_DATA_TYPE(pLeftTsCol->info.type);
    if (isVarType) {
      bool leftNull = (pLeftTsCol->pData == NULL) || colDataIsNull_var(pLeftTsCol, pLeftSource->src.rowIndex);
      bool rightNull = (pRightTsCol->pData == NULL) || colDataIsNull_var(pRightTsCol, pRightSource->src.rowIndex);
      if (leftNull && rightNull) return 0;
      if (leftNull) return pOrder->nullFirst ? -1 : 1;
      if (rightNull) return pOrder->nullFirst ? 1 : -1;
      void* left1 = colDataGetVarData(pLeftTsCol, pLeftSource->src.rowIndex);
      void* right1 = colDataGetVarData(pRightTsCol, pRightSource->src.rowIndex);
      __compar_fn_t fn = pOrder->compFn;
      if (!fn) {
        fn = getKeyComparFunc(pLeftTsCol->info.type, pOrder->order);
        pOrder->compFn = fn;
      }
      return fn(left1, right1);
    }

    if (pLeftTsCol->hasNull || pRightTsCol->hasNull || pLeftTsCol->pData == NULL || pRightTsCol->pData == NULL) {
      bool leftNull = (pLeftTsCol->pData == NULL);
      if (!leftNull && pLeftTsCol->hasNull) {
        leftNull = colDataIsNull_t(pLeftTsCol, pLeftSource->src.rowIndex, false);
      }

      bool rightNull = (pRightTsCol->pData == NULL);
      if (!rightNull && pRightTsCol->hasNull) {
        rightNull = colDataIsNull_t(pRightTsCol, pRightSource->src.rowIndex, false);
      }

      if (leftNull && rightNull) return 0;
      if (leftNull) return pOrder->nullFirst ? -1 : 1;
      if (rightNull) return pOrder->nullFirst ? 1 : -1;
    }

    int64_t*         leftTs = (int64_t*)(pLeftTsCol->pData) + pLeftSource->src.rowIndex;
    int64_t*         rightTs = (int64_t*)(pRightTsCol->pData) + pRightSource->src.rowIndex;

    __compar_fn_t fn = pOrder->compFn;
    if (!fn) {
      fn = getKeyComparFunc(pLeftTsCol->info.type, pOrder->order);
      pOrder->compFn = fn;
    }

    return fn(leftTs, rightTs);
  } else {
    // Single fixed-length key without NULLs: compare the values directly and
    // skip the generic per-key loop that re-reads the order array and calls a
    // function pointer for every comparison.
    if (pParam->fastKeyState != TSORT_FASTKEY_DISABLED) {
      if (pParam->fastKeyState == TSORT_FASTKEY_UNKNOWN) {
        // Resolve the slot and comparison kind once per sort handle. The
        // candidate is rejected only when the shape is definitively
        // ineligible; when the first compared block cannot answer yet (column
        // not materialized, slot absent from this block), the state stays
        // UNKNOWN so a later comparison still gets a chance. fastKeyKind is
        // only ever a hint: the per-comparison checks below re-derive the kind
        // from the current column and require it to match, so a stale hint can
        // only disable the fast path, never misread data.
        // ENABLED is not terminal either. A pair whose key is NULL, whose block
        // carries pBlockAgg, or whose slot is missing from one side simply
        // takes the generic per-key path for that comparison while the state
        // stays ENABLED, so a clean pair later in the same merge still uses the
        // typed compare. Only the shape checks below (slot/order drift) flip
        // the state to DISABLED.
        if (taosArrayGetSize(pInfo) != 1) {
          pParam->fastKeyState = TSORT_FASTKEY_DISABLED;
        } else {
          SBlockOrderInfo* pOrder0 = TARRAY_GET_ELEM(pInfo, 0);
          if (pOrder0 == NULL || pOrder0->compFn != NULL) {
            pParam->fastKeyState = TSORT_FASTKEY_DISABLED;
          } else if (pOrder0->slotId >= 0 &&
                     pOrder0->slotId < (int32_t)taosArrayGetSize(pLeftBlock->pDataBlock)) {
            SColumnInfoData* pCol0 = TARRAY_GET_ELEM(pLeftBlock->pDataBlock, pOrder0->slotId);
            if (pCol0 != NULL && pCol0->pData != NULL) {
              int8_t kind = tsortKeyCmpKind(pCol0->info.type);
              if (IS_VAR_DATA_TYPE(pCol0->info.type) || kind == TSORT_KEY_CMP_NONE) {
                pParam->fastKeyState = TSORT_FASTKEY_DISABLED;
              } else {
                pParam->fastKeySlotId = pOrder0->slotId;
                pParam->fastKeyKind = kind;
                pParam->fastKeyDesc = (pOrder0->order == TSDB_ORDER_DESC);
                pParam->fastKeyState = TSORT_FASTKEY_ENABLED;
              }
            }
            // else: the column is not materialized yet; do not decide.
          }
          // else: this block does not carry the slot; do not decide.
        }
      }

      if (pParam->fastKeyState == TSORT_FASTKEY_ENABLED) {
        // Re-derive the direction from the live order array on every
        // comparison instead of trusting the cached fastKeyDesc forever: if
        // the sort key slot or the direction changes, comparing with the
        // cached direction would emit the merge output in the wrong order.
        // Drop the fast path in that case and let the generic loop resolve
        // the current order.
        SBlockOrderInfo* pOrder0 = (taosArrayGetSize(pInfo) == 1) ? TARRAY_GET_ELEM(pInfo, 0) : NULL;
        if (pOrder0 == NULL || pOrder0->slotId != pParam->fastKeySlotId ||
            (pOrder0->order == TSDB_ORDER_DESC) != pParam->fastKeyDesc) {
          pParam->fastKeyState = TSORT_FASTKEY_DISABLED;
        }
      }

      // The cached slot must exist in both blocks, otherwise fall through to
      // the generic per-key loop instead of comparing garbage. The column
      // shape is re-checked on every comparison rather than cached per block
      // address: a merge frees and rebuilds source blocks, so the same address
      // can come back carrying a different type or width (ABA). The checks read
      // fields of the SColumnInfoData already touched below, so they are cheap.
      bool fastUsable = pParam->fastKeyState == TSORT_FASTKEY_ENABLED && pParam->fastKeySlotId >= 0 &&
                        pParam->fastKeySlotId < (int32_t)taosArrayGetSize(pLeftBlock->pDataBlock) &&
                        pParam->fastKeySlotId < (int32_t)taosArrayGetSize(pRightBlock->pDataBlock);
      if (fastUsable) {
        SColumnInfoData* pLeftKey = TARRAY_GET_ELEM(pLeftBlock->pDataBlock, pParam->fastKeySlotId);
        SColumnInfoData* pRightKey = TARRAY_GET_ELEM(pRightBlock->pDataBlock, pParam->fastKeySlotId);
        if (pLeftKey != NULL && pRightKey != NULL && pLeftKey->pData != NULL && pRightKey->pData != NULL &&
            !pLeftKey->hasNull && !pRightKey->hasNull && pLeftBlock->pBlockAgg == NULL &&
            pRightBlock->pBlockAgg == NULL && pLeftKey->info.type == pRightKey->info.type &&
            pLeftKey->info.bytes == pRightKey->info.bytes &&
            tsortKeyCmpKind(pLeftKey->info.type) == pParam->fastKeyKind &&
            // the typed load advances by the kind's width: refuse a column whose
            // declared width does not match it instead of reading at the wrong
            // offset (a zero width also means an unknown kind)
            tsortKeyCmpWidth(pParam->fastKeyKind) == pLeftKey->info.bytes &&
            // the row indices come from the merge state; keep the typed load
            // inside the block the column actually holds
            pLeftSource->src.rowIndex >= 0 && pLeftSource->src.rowIndex < (int32_t)pLeftBlock->info.rows &&
            pRightSource->src.rowIndex >= 0 && pRightSource->src.rowIndex < (int32_t)pRightBlock->info.rows) {
          int32_t ret = tsortComparFixedKeyVal(pLeftKey, pLeftSource->src.rowIndex, pRightKey,
                                               pRightSource->src.rowIndex, pParam->fastKeyKind);
          return pParam->fastKeyDesc ? -ret : ret;
        }
      }
    }

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

      void *left1, *right1;
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

static int32_t doSortForEachGroup(SSortHandle* pHandle, int32_t sortTimes, int32_t numOfSorted,
                                  int32_t numOfInputSources, SArray* pResList, int32_t sortGroup, int32_t numOfRows) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* pPageIdList = NULL;

  for (int32_t i = 0; i < sortGroup; ++i) {
    qDebug("internal merge sort pass %d group %d. num input sources %d ", sortTimes, i, numOfInputSources);
    pHandle->sourceId += 1;

    int32_t end = (i + 1) * numOfInputSources - 1;
    if (end > numOfSorted - 1) {
      end = numOfSorted - 1;
    }

    pHandle->cmpParam.numOfSources = end - i * numOfInputSources + 1;

    code = sortComparInit(&pHandle->cmpParam, pHandle->pOrderedSource, i * numOfInputSources, end, pHandle);
    QUERY_CHECK_CODE(code, lino, _err);

    code =
        tMergeTreeCreate(&pHandle->pMergeTree, pHandle->cmpParam.numOfSources, &pHandle->cmpParam, pHandle->comparFn);
    QUERY_CHECK_CODE(code, lino, _err);

    int32_t nMergedRows = 0;
    pPageIdList = taosArrayInit(4, sizeof(int32_t));
    QUERY_CHECK_NULL(pPageIdList, code, lino, _err, terrno);

    while (1) {
      if (tsortIsClosed(pHandle) || (pHandle->abortCheckFn && pHandle->abortCheckFn(pHandle->abortCheckParam))) {
        code = TSDB_CODE_TSC_QUERY_CANCELLED;
        goto _err;
      }

      SSDataBlock* pDataBlock = NULL;
      code = getSortedBlockDataInner(pHandle, &pHandle->cmpParam, numOfRows, &pDataBlock);
      if (pDataBlock == NULL || code != 0) {
        break;
      }

      int32_t pageId = -1;
      void*   pPage = getNewBufPage(pHandle->pBuf, &pageId);
      QUERY_CHECK_NULL(pPage, code, lino, _err, terrno);

      void* px = taosArrayPush(pPageIdList, &pageId);
      QUERY_CHECK_NULL(px, code, lino, _err, terrno);

      int32_t size =
          blockDataGetSize(pDataBlock) + sizeof(int32_t) +
          taosArrayGetSize(pDataBlock->pDataBlock) * (sizeof(int32_t) + 2 * sizeof(uint8_t));
      if (size > getBufPageSize(pHandle->pBuf)) {
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        goto _err;
      }

      code = blockDataToBuf(pPage, pDataBlock);
      QUERY_CHECK_CODE(code, lino, _err);

      setBufPageDirty(pPage, true);
      releaseBufPage(pHandle->pBuf, pPage);
      nMergedRows += pDataBlock->info.rows;

      blockDataCleanup(pDataBlock);
      if ((pHandle->mergeLimit != -1) && (nMergedRows >= pHandle->mergeLimit)) {
        break;
      }
    }

    code = sortComparCleanup(&pHandle->cmpParam);
    QUERY_CHECK_CODE(code, lino, _err);

    tMergeTreeDestroy(&pHandle->pMergeTree);
    pHandle->numOfCompletedSources = 0;

    SSDataBlock* pBlock = NULL;
    code = createOneDataBlock(pHandle->pDataBlock, false, &pBlock);
    QUERY_CHECK_CODE(code, lino, _err);

    code = doAddNewExternalMemSource(pHandle->pBuf, pResList, pBlock, &pHandle->sourceId, pPageIdList);
    if (code != TSDB_CODE_SUCCESS) {
      blockDataDestroy(pBlock);
    }
    QUERY_CHECK_CODE(code, lino, _err);
  }

  return code;

_err:
  taosArrayDestroy(pPageIdList);
  qError("%s error happens:%s line:%d, code:%s", pHandle->idStr, __func__, lino, tstrerror(code));
  return code;
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

  int32_t size = (int32_t)blockDataGetSerialMetaSize(taosArrayGetSize(pHandle->pDataBlock->pDataBlock));
  size_t  rowSize = blockDataGetRowSize(pHandle->pDataBlock);
  int32_t numOfRows = blockDataGetCapacityInRow(pHandle->pDataBlock, pHandle->pageSize, size);
  if (numOfRows < 0) {
    return terrno;
  }

  int32_t code = blockDataEnsureCapacity(pHandle->pDataBlock, numOfRows);
  if (code) {
    return code;
  }

  // the initial pass + sortPass + final mergePass
  pHandle->loops = sortPass + 2;

  size_t numOfSorted = taosArrayGetSize(pHandle->pOrderedSource);
  for (int32_t t = 0; t < sortPass; ++t) {
    int64_t st = taosGetTimestampUs();
    SArray* pResList = taosArrayInit(4, POINTER_BYTES);
    if (pResList == NULL) {
      return terrno;
    }

    int32_t numOfInputSources = pHandle->numOfPages;
    int32_t sortGroup = (numOfSorted + numOfInputSources - 1) / numOfInputSources;

    // Only *numOfInputSources* can be loaded into buffer to perform the external sort.
    code = doSortForEachGroup(pHandle, t, numOfSorted, numOfInputSources, pResList, sortGroup, numOfRows);
    if (code != 0) {
      tsortClearOrderedSource(pResList, NULL, NULL);
      taosArrayDestroy(pResList);
      return code;
    }

    tsortClearOrderedSource(pHandle->pOrderedSource, NULL, NULL);
    void* px = taosArrayAddAll(pHandle->pOrderedSource, pResList);
    if (px == NULL) {
      tsortClearOrderedSource(pResList, NULL, NULL);
      taosArrayDestroy(pResList);
      return terrno;
    }

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
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    } else {
      dBufSetPrintInfo(pHandle->pBuf);
    }
  }
  return 0;
}

int32_t tsortAppendTupleToBlock(SSortHandle* pHandle, SSDataBlock* pBlock, STupleHandle* pTupleHandle) {
  int32_t code = 0;

  if (pHandle->bSortByRowId) {
    int32_t *p1, *p2, *p3;
    tsortGetValue(pTupleHandle, 1, (void**)&p1);
    tsortGetValue(pTupleHandle, 2, (void**)&p2);
    tsortGetValue(pTupleHandle, 3, (void**)&p3);

    int32_t regionId = *p1;
    int32_t offset = *p2;
    int32_t length = *p3;

    char* buf = NULL;
    bool  bFreeRow = false;

    code = getRowBufFromExtMemFile(pHandle, regionId, offset, length, &buf, &bFreeRow);
    if (code) {
      return code;
    }

    int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
    char*   isNull = (char*)buf;
    char*   pStart = (char*)buf + sizeof(int8_t) * numOfCols;
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);
      if (pColInfo == NULL) {
        return terrno;
      }

      if (!isNull[i]) {
        code = colDataSetVal(pColInfo, pBlock->info.rows, pStart, false);
        if (code) {
          return code;
        }

        if (pColInfo->info.type == TSDB_DATA_TYPE_JSON) {
          int32_t dataLen = getJsonValueLen(pStart);
          pStart += dataLen;
        } else if (IS_VAR_DATA_TYPE(pColInfo->info.type)) {
          if (IS_STR_DATA_BLOB(pColInfo->info.type)) {
            pStart += blobDataTLen(pStart);
          } else {
            pStart += varDataTLen(pStart);
          }
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

    SDataBlockInfo info = {0};
    tsortGetBlockInfo(pTupleHandle, &info);

    pBlock->info.scanFlag = info.scanFlag;
    pBlock->info.scanPhase = info.scanPhase;
    pBlock->info.rows += 1;

  } else {
    for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); ++i) {
      SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);
      if (pColInfo == NULL) {
        return terrno;
      }

      bool isNull = tsortIsNullVal(pTupleHandle, i);
      if (isNull) {
        colDataSetNULL(pColInfo, pBlock->info.rows);
      } else {
        char* pData = NULL;
        tsortGetValue(pTupleHandle, i, (void**)&pData);
        if (pData != NULL) {
          code = colDataSetVal(pColInfo, pBlock->info.rows, pData, false);
          if (code) {
            return code;
          }
        }
      }
    }

    pBlock->info.dataLoad = 1;
    SDataBlockInfo info = {0};
    tsortGetBlockInfo(pTupleHandle, &info);

    pBlock->info.scanFlag = info.scanFlag;
    pBlock->info.scanPhase = info.scanPhase;
    pBlock->info.rows += 1;
  }

  return code;
}

static int32_t blockRowToBuf(SSDataBlock* pBlock, int32_t rowIdx, char* buf) {
  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);

  char* isNull = (char*)buf;
  char* pStart = (char*)buf + sizeof(int8_t) * numOfCols;
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, i);
    if (pCol == NULL) {
      return terrno;
    }

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
      if (IS_STR_DATA_BLOB(pCol->info.type)) {
        if (pCol->pData) {
          blobDataCopy(pStart, pData);
          pStart += blobDataTLen(pData);
        } else {
          // the column that is pre-allocated has no data and has offset
          *(BlobDataLenT*)(pStart) = 0;
          pStart += BLOBSTR_HEADER_SIZE;
        }
      } else {
        if (pCol->pData) {
          varDataCopy(pStart, pData);
          pStart += varDataTLen(pData);
        } else {
          // the column that is pre-allocated has no data and has offset
          *(VarDataLenT*)(pStart) = 0;
          pStart += VARSTR_HEADER_SIZE;
        }
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
  SSortMemFile*       pMemFile = pHandle->pExtRowsMemFile;
  SSortMemFileRegion* pRegion = taosArrayGet(pMemFile->aFileRegions, regionId);
  if (pRegion == NULL) {
    return terrno;
  }

  if (pRegion->buf == NULL) {
    pRegion->bufRegOffset = 0;
    pRegion->buf = taosMemoryMalloc(pMemFile->blockSize);
    if (pRegion->buf == NULL) {
      return terrno;
    }

    if (taosLSeekFile(pMemFile->pTdFile, pRegion->fileOffset, SEEK_SET) < 0) {
      return terrno;
    }

    int32_t readBytes = TMIN(pMemFile->blockSize, pRegion->regionSize);
    int64_t ret = taosReadFile(pMemFile->pTdFile, pRegion->buf, readBytes);
    if (ret != readBytes) {
      return ret < 0 ? terrno : TSDB_CODE_FILE_CORRUPTED;
    }
    pRegion->bufLen = readBytes;
  }
  if (pRegion->bufRegOffset > tupleOffset) {
    qError("sort failed at: %s:%d", __func__, __LINE__);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  if (pRegion->bufRegOffset + pRegion->bufLen >= tupleOffset + rowLen) {
    *pFreeRow = false;
    *ppRow = pRegion->buf + tupleOffset - pRegion->bufRegOffset;
  } else {
    *ppRow = taosMemoryMalloc(rowLen);
    if (*ppRow == NULL) {
      return terrno;
    }
    int32_t szThisBlock = pRegion->bufLen - (tupleOffset - pRegion->bufRegOffset);
    memcpy(*ppRow, pRegion->buf + tupleOffset - pRegion->bufRegOffset, szThisBlock);

    // todo
    int64_t offset = pRegion->fileOffset + pRegion->bufRegOffset +
                     pRegion->bufLen;
    if (taosLSeekFile(pMemFile->pTdFile, offset, SEEK_SET) < 0) {
      return terrno;
    }
    int32_t readBytes = TMIN(pMemFile->blockSize, pRegion->regionSize - (pRegion->bufRegOffset + pRegion->bufLen));
    int64_t ret = taosReadFile(pMemFile->pTdFile, pRegion->buf, readBytes);
    if (ret != readBytes) {
      taosMemoryFreeClear(*ppRow);
      return ret < 0 ? terrno : TSDB_CODE_FILE_CORRUPTED;
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
    code = terrno;
  }
  if (code == TSDB_CODE_SUCCESS) {
    pMemFile->pTdFile = taosOpenTempFile(tsTempDir, "sort-ext-mem");
    if (pMemFile->pTdFile == NULL) {
      code = terrno;
    }
  }
  if (code == TSDB_CODE_SUCCESS) {
    pMemFile->currRegionId = -1;
    pMemFile->currRegionOffset = -1;

    pMemFile->writeBufSize = 4 * 1024 * 1024;
    pMemFile->writeFileOffset = -1;
    pMemFile->bRegionDirty = false;

    pMemFile->writeBuf = taosMemoryMalloc(pMemFile->writeBufSize);
    if (pMemFile->writeBuf == NULL) {
      code = terrno;
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    pMemFile->cacheSize = pHandle->extRowsMemSize;
    pMemFile->aFileRegions = taosArrayInit(64, sizeof(SSortMemFileRegion));
    if (pMemFile->aFileRegions == NULL) {
      code = terrno;
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    pHandle->pExtRowsMemFile = pMemFile;
  } else {
    if (pMemFile) {
      if (pMemFile->aFileRegions) taosMemoryFreeClear(pMemFile->aFileRegions);
      if (pMemFile->writeBuf) taosMemoryFreeClear(pMemFile->writeBuf);
      if (pMemFile->pTdFile) {
        (void)taosCloseFile(&pMemFile->pTdFile);
        pMemFile->pTdFile = NULL;
      }
      taosMemoryFreeClear(pMemFile);
    }
  }
  return code;
}

static void destroySortMemFile(SSortHandle* pHandle) {
  if (pHandle->pExtRowsMemFile == NULL) {
    return;
  }

  SSortMemFile* pMemFile = pHandle->pExtRowsMemFile;
  for (int32_t i = 0; i < taosArrayGetSize(pMemFile->aFileRegions); ++i) {
    SSortMemFileRegion* pRegion = taosArrayGet(pMemFile->aFileRegions, i);
    if (pRegion == NULL) {
      continue;
    }

    taosMemoryFree(pRegion->buf);
  }

  taosArrayDestroy(pMemFile->aFileRegions);
  pMemFile->aFileRegions = NULL;

  taosMemoryFree(pMemFile->writeBuf);
  pMemFile->writeBuf = NULL;

  (void)taosCloseFile(&pMemFile->pTdFile);
  pMemFile->pTdFile = NULL;
  taosMemoryFree(pMemFile);
  pHandle->pExtRowsMemFile = NULL;
}

static int32_t tsortOpenRegion(SSortHandle* pHandle) {
  SSortMemFile* pMemFile = pHandle->pExtRowsMemFile;
  int32_t       code = 0;

  if (pMemFile->currRegionId == -1) {
    SSortMemFileRegion region = {0};
    region.fileOffset = 0;
    region.bufRegOffset = 0;
    void* px = taosArrayPush(pMemFile->aFileRegions, &region);
    if (px == NULL) {
      code = terrno;
    }

    pMemFile->currRegionId = 0;
    pMemFile->currRegionOffset = 0;
    pMemFile->writeFileOffset = 0;
  } else {
    SSortMemFileRegion  regionNew = {0};
    SSortMemFileRegion* pRegion = taosArrayGet(pMemFile->aFileRegions, pMemFile->currRegionId);
    if (pRegion == NULL) {
      return terrno;
    }

    regionNew.fileOffset = pRegion->fileOffset + pRegion->regionSize;
    regionNew.bufRegOffset = 0;

    void* px = taosArrayPush(pMemFile->aFileRegions, &regionNew);
    if (px == NULL) {
      code = terrno;
    }
    ++pMemFile->currRegionId;
    pMemFile->currRegionOffset = 0;
    pMemFile->writeFileOffset = regionNew.fileOffset;
  }
  return code;
}

static int32_t tsortCloseRegion(SSortHandle* pHandle) {
  SSortMemFile*       pMemFile = pHandle->pExtRowsMemFile;
  SSortMemFileRegion* pRegion = taosArrayGet(pMemFile->aFileRegions, pMemFile->currRegionId);
  if (pRegion == NULL) {
    return terrno;
  }

  pRegion->regionSize = pMemFile->currRegionOffset;
  int32_t writeBytes = pRegion->regionSize - (pMemFile->writeFileOffset - pRegion->fileOffset);
  if (writeBytes > 0) {
    int64_t ret = taosWriteFile(pMemFile->pTdFile, pMemFile->writeBuf,
                                writeBytes);
    if (ret != writeBytes) {
      return ret < 0 ? terrno : TSDB_CODE_APP_ERROR;
    }
    pMemFile->bRegionDirty = false;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t tsortFinalizeRegions(SSortHandle* pHandle) {
  SSortMemFile* pMemFile = pHandle->pExtRowsMemFile;
  size_t        numRegions = taosArrayGetSize(pMemFile->aFileRegions);
  if (numRegions != (pMemFile->currRegionId + 1)) {
    qError("sort failed at: %s:%d", __func__, __LINE__);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  if (numRegions == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t blockReadBytes = (pMemFile->cacheSize / numRegions + 4095) & ~4095;
  pMemFile->blockSize = blockReadBytes;

  for (int32_t i = 0; i < numRegions; ++i) {
    SSortMemFileRegion* pRegion = taosArrayGet(pMemFile->aFileRegions, i);
    if (pRegion == NULL) {
      return terrno;
    }

    pRegion->bufRegOffset = 0;
  }

  taosMemoryFree(pMemFile->writeBuf);
  pMemFile->writeBuf = NULL;
  return TSDB_CODE_SUCCESS;
}

static int32_t saveBlockRowToExtRowsMemFile(SSortHandle* pHandle, SSDataBlock* pBlock, int32_t rowIdx,
                                            int32_t* pRegionId, int32_t* pOffset, int32_t* pLength) {
  SSortMemFile*       pMemFile = pHandle->pExtRowsMemFile;
  SSortMemFileRegion* pRegion = taosArrayGet(pMemFile->aFileRegions, pMemFile->currRegionId);
  if (pRegion == NULL) {
    return terrno;
  }

  {
    if (pMemFile->currRegionOffset + pHandle->extRowBytes >= pMemFile->writeBufSize) {
      int32_t writeBytes = pMemFile->currRegionOffset - (pMemFile->writeFileOffset - pRegion->fileOffset);
      int64_t ret = taosWriteFile(pMemFile->pTdFile, pMemFile->writeBuf,
                                  writeBytes);
      if (ret != writeBytes) {
        return ret < 0 ? terrno : TSDB_CODE_APP_ERROR;
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

static int32_t appendToRowIndexDataBlock(SSortHandle* pHandle, SSDataBlock* pSource, int32_t* rowIndex) {
  int32_t pageId = -1;
  int32_t offset = -1;
  int32_t length = -1;
  int32_t code = 0;

  code = saveBlockRowToExtRowsMemFile(pHandle, pSource, *rowIndex, &pageId, &offset, &length);
  if (code) {
    return code;
  }

  SSDataBlock*     pBlock = pHandle->pDataBlock;
  SBlockOrderInfo* extRowsTsOrder = taosArrayGet(pHandle->aExtRowsOrders, 0);
  if (extRowsTsOrder == NULL) {
    return terrno;
  }

  SColumnInfoData* pSrcTsCol = taosArrayGet(pSource->pDataBlock, extRowsTsOrder->slotId);
  if (pSrcTsCol == NULL) {
    return terrno;
  }

  SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, 0);
  if (pTsCol == NULL) {
    return terrno;
  }

  char* pData = colDataGetData(pSrcTsCol, *rowIndex);
  code = colDataSetVal(pTsCol, pBlock->info.rows, pData, false);
  if (code) {
    return code;
  }

  SColumnInfoData* pRegionIdCol = taosArrayGet(pBlock->pDataBlock, 1);
  if (pRegionIdCol == NULL) {
    return terrno;
  }

  colDataSetInt32(pRegionIdCol, pBlock->info.rows, &pageId);

  SColumnInfoData* pOffsetCol = taosArrayGet(pBlock->pDataBlock, 2);
  if (pOffsetCol == NULL) {
    return terrno;
  }

  colDataSetInt32(pOffsetCol, pBlock->info.rows, &offset);

  SColumnInfoData* pLengthCol = taosArrayGet(pBlock->pDataBlock, 3);
  if (pLengthCol == NULL) {
    return terrno;
  }

  colDataSetInt32(pLengthCol, pBlock->info.rows, &length);

  if (pHandle->bSortPk) {
    SBlockOrderInfo* extRowsPkOrder = taosArrayGet(pHandle->aExtRowsOrders, 1);
    if (extRowsPkOrder == NULL) {
      return terrno;
    }

    SColumnInfoData* pSrcPkCol = taosArrayGet(pSource->pDataBlock, extRowsPkOrder->slotId);
    if (pSrcPkCol == NULL) {
      return terrno;
    }

    SColumnInfoData* pPkCol = taosArrayGet(pBlock->pDataBlock, 4);
    if (pPkCol == NULL) {
      return terrno;
    }

    if (colDataIsNull_s(pSrcPkCol, *rowIndex)) {
      colDataSetNULL(pPkCol, pBlock->info.rows);
    } else {
      char* pPkData = colDataGetData(pSrcPkCol, *rowIndex);
      code = colDataSetVal(pPkCol, pBlock->info.rows, pPkData, false);
      if (code) {
        return code;
      }
    }
  }

  pBlock->info.rows += 1;
  *rowIndex += 1;
  return code;
}

static int32_t initRowIdSort(SSortHandle* pHandle) {
  SBlockOrderInfo* pkOrder = (pHandle->bSortPk) ? taosArrayGet(pHandle->aExtRowsOrders, 1) : NULL;
  SColumnInfoData* extPkCol =
      (pHandle->bSortPk) ? taosArrayGet(pHandle->pDataBlock->pDataBlock, pkOrder->slotId) : NULL;

  SColumnInfoData pkCol = {0};
  SSDataBlock*    pSortInput = NULL;
  int32_t         code = createDataBlock(&pSortInput);
  if (code) {
    return code;
  }

  SColumnInfoData tsCol = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, 8, 1);
  code = blockDataAppendColInfo(pSortInput, &tsCol);
  if (code) {
    blockDataDestroy(pSortInput);
    return code;
  }

  SColumnInfoData regionIdCol = createColumnInfoData(TSDB_DATA_TYPE_INT, 4, 2);
  code = blockDataAppendColInfo(pSortInput, &regionIdCol);
  if (code) {
    blockDataDestroy(pSortInput);
    return code;
  }

  SColumnInfoData offsetCol = createColumnInfoData(TSDB_DATA_TYPE_INT, 4, 3);
  code = blockDataAppendColInfo(pSortInput, &offsetCol);
  if (code) {
    blockDataDestroy(pSortInput);
    return code;
  }

  SColumnInfoData lengthCol = createColumnInfoData(TSDB_DATA_TYPE_INT, 4, 4);
  code = blockDataAppendColInfo(pSortInput, &lengthCol);
  if (code) {
    blockDataDestroy(pSortInput);
    return code;
  }

  if (pHandle->bSortPk) {
    pkCol = createColumnInfoData(extPkCol->info.type, extPkCol->info.bytes, 5);
    code = blockDataAppendColInfo(pSortInput, &pkCol);
    if (code) {
      blockDataDestroy(pSortInput);
      return code;
    }
  }

  blockDataDestroy(pHandle->pDataBlock);
  pHandle->pDataBlock = pSortInput;

  //  int32_t  rowSize = blockDataGetRowSize(pHandle->pDataBlock);
  //  size_t nCols = taosArrayGetSize(pHandle->pDataBlock->pDataBlock);
  pHandle->pageSize = 256 * 1024;  // 256k
  pHandle->numOfPages = 256;

  SArray* pOrderInfoList = taosArrayInit(1, sizeof(SBlockOrderInfo));
  if (pOrderInfoList == NULL) {
    return terrno;
  }

  int32_t tsOrder = ((SBlockOrderInfo*)taosArrayGet(pHandle->pSortInfo, 0))->order;

  SBlockOrderInfo biTs = {0};
  biTs.order = tsOrder;
  biTs.slotId = 0;
  biTs.nullFirst = (biTs.order == TSDB_ORDER_ASC);
  biTs.compFn = getKeyComparFunc(TSDB_DATA_TYPE_TIMESTAMP, biTs.order);
  void* p = taosArrayPush(pOrderInfoList, &biTs);
  if (p == NULL) {
    taosArrayDestroy(pOrderInfoList);
    return terrno;
  }

  if (pHandle->bSortPk) {
    SBlockOrderInfo biPk = {0};
    biPk.order = pkOrder->order;
    biPk.slotId = 4;
    biPk.nullFirst = (biPk.order == TSDB_ORDER_ASC);
    biPk.compFn = getKeyComparFunc(pkCol.info.type, biPk.order);

    void* px = taosArrayPush(pOrderInfoList, &biPk);
    if (px == NULL) {
      taosArrayDestroy(pOrderInfoList);
      return terrno;
    }
  }

  taosArrayDestroy(pHandle->pSortInfo);
  pHandle->pSortInfo = pOrderInfoList;
  pHandle->cmpParam.pPkOrder = (pHandle->bSortPk) ? taosArrayGet(pHandle->pSortInfo, 1) : NULL;
  return TSDB_CODE_SUCCESS;
}

int32_t tsortSetSortByRowId(SSortHandle* pHandle, int32_t extRowsMemSize) {
  pHandle->extRowBytes =
      blockDataGetRowSize(pHandle->pDataBlock) + taosArrayGetSize(pHandle->pDataBlock->pDataBlock) + sizeof(int32_t);
  pHandle->extRowsMemSize = extRowsMemSize;
  pHandle->aExtRowsOrders = taosArrayDup(pHandle->pSortInfo, NULL);
  if (pHandle->aExtRowsOrders == NULL) {
    return terrno;
  }

  int32_t code = initRowIdSort(pHandle);
  if (code) {
    return code;
  }

  if (!osTempSpaceAvailable()) {
    qError("create sort mem file failed since %s, tempDir:%s", terrstr(), tsTempDir);
    return TSDB_CODE_NO_DISKSPACE;
  }

  code = createSortMemFile(pHandle);
  pHandle->bSortByRowId = true;
  return code;
}

typedef struct SBlkMergeSupport {
  int64_t** aTs;
  int32_t*  aRowIdx;
  int32_t   tsOrder;

  SBlockOrderInfo* pPkOrder;
  SSDataBlock**    aBlks;
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

  int32_t ret = leftTs > rightTs ? 1 : ((leftTs < rightTs) ? -1 : 0);
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

  int32_t ret = leftTs > rightTs ? 1 : ((leftTs < rightTs) ? -1 : 0);
  if (pSup->tsOrder == TSDB_ORDER_DESC) {
    ret = -1 * ret;
  }
  if (ret == 0 && pSup->pPkOrder) {
    ret = tsortComparBlockCell(pSup->aBlks[left], pSup->aBlks[right], pSup->aRowIdx[left], pSup->aRowIdx[right],
                               pSup->pPkOrder);
  }
  return ret;
}

static int32_t appendDataBlockToPageBuf(SSortHandle* pHandle, SSDataBlock* blk, SArray* aPgId) {
  int32_t pageId = -1;
  void*   pPage = getNewBufPage(pHandle->pBuf, &pageId);
  if (pPage == NULL) {
    return terrno;
  }

  void* px = taosArrayPush(aPgId, &pageId);
  if (px == NULL) {
    return terrno;
  }

  int32_t size = blockDataGetSize(blk) + sizeof(int32_t) +
                 taosArrayGetSize(blk->pDataBlock) * (sizeof(int32_t) + 2 * sizeof(uint8_t));
  if (size > getBufPageSize(pHandle->pBuf)) {
    qError("sort failed at: %s:%d", __func__, __LINE__);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int32_t code = blockDataToBuf(pPage, blk);

  setBufPageDirty(pPage, true);
  releaseBufPage(pHandle->pBuf, pPage);

  return code;
}

static int32_t getPageBufIncForRow(SSDataBlock* pSrcBlock, int32_t srcRowIndex, int32_t dstRowIndex) {
  int32_t size = 0;
  int32_t numCols = taosArrayGetSize(pSrcBlock->pDataBlock);

  if (!pSrcBlock->info.hasVarCol) {
    size += numCols * ((dstRowIndex & 0x7) == 0 ? 1 : 0);
    size += blockDataGetRowSize(pSrcBlock);
  } else {
    for (int32_t i = 0; i < numCols; ++i) {
      SColumnInfoData* pColInfoData = TARRAY_GET_ELEM(pSrcBlock->pDataBlock, i);
      if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
        if ((pColInfoData->varmeta.offset[srcRowIndex] != -1) && (pColInfoData->pData)) {
          char* p = colDataGetData(pColInfoData, srcRowIndex);

          size += calcStrBytesByType(pColInfoData->info.type, p);
          // if (pColInfoData->info.type == TSDB_DATA_TYPE_JSON) {
          //   size += getJsonValueLen(p);
          // } else {
          //   size += varDataTLen(p);
          // }
        }

        size += sizeof(pColInfoData->varmeta.offset[0]);
      } else {
        size += pColInfoData->info.bytes;

        if (((dstRowIndex)&0x07) == 0) {
          size += 1;  // bitmap
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

  if (pPkCol == NULL) {  // no var column
    if (!((numOfCols == 4) && (!pDstBlock->info.hasVarCol))) {
      qError("sort failed at: %s:%d", __func__, __LINE__);
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    size += numOfCols * ((dstRowIndex & 0x7) == 0 ? 1 : 0);
    size += blockDataGetRowSize(pDstBlock);
  } else {
    if (numOfCols != 5) {
      qError("sort failed at: %s:%d", __func__, __LINE__);
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    size += (numOfCols - 1) * (((dstRowIndex & 0x7) == 0) ? 1 : 0);
    for (int32_t i = 0; i < numOfCols - 1; ++i) {
      SColumnInfoData* pColInfo = TARRAY_GET_ELEM(pDstBlock->pDataBlock, i);
      size += pColInfo->info.bytes;
    }

    // handle the pk column, the last column, may be the var char column
    if (IS_VAR_DATA_TYPE(pPkCol->info.type)) {
      if ((pPkCol->varmeta.offset[srcRowIndex] != -1) && (pPkCol->pData)) {
        char* p = colDataGetData(pPkCol, srcRowIndex);
        if (IS_STR_DATA_BLOB(pPkCol->info.type)) {
          size += blobDataTLen(p);
        } else {
          size += varDataTLen(p);
        }
      }

      size += sizeof(pPkCol->varmeta.offset[0]);
    } else {
      size += pPkCol->info.bytes;
      if (((dstRowIndex)&0x07) == 0) {
        size += 1;  // bitmap
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

static int32_t initMergeSup(SBlkMergeSupport* pSup, SArray* pBlockList, int32_t tsOrder, int32_t tsSlotId,
                            SBlockOrderInfo* pPkOrderInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  memset(pSup, 0, sizeof(SBlkMergeSupport));

  int32_t numOfBlocks = taosArrayGetSize(pBlockList);

  pSup->aRowIdx = taosMemoryCalloc(numOfBlocks, sizeof(int32_t));
  QUERY_CHECK_NULL(pSup->aRowIdx, code, lino, _end, terrno);

  pSup->aTs = taosMemoryCalloc(numOfBlocks, sizeof(int64_t*));
  QUERY_CHECK_NULL(pSup->aTs, code, lino, _end, terrno);

  pSup->tsOrder = tsOrder;
  pSup->aBlks = taosMemoryCalloc(numOfBlocks, sizeof(SSDataBlock*));
  QUERY_CHECK_NULL(pSup->aBlks, code, lino, _end, terrno);

  for (int32_t i = 0; i < numOfBlocks; ++i) {
    SSDataBlock*     pBlock = taosArrayGetP(pBlockList, i);
    SColumnInfoData* col = taosArrayGet(pBlock->pDataBlock, tsSlotId);
    pSup->aTs[i] = (int64_t*)col->pData;
    pSup->aRowIdx[i] = 0;
    pSup->aBlks[i] = pBlock;
  }

  pSup->pPkOrder = pPkOrderInfo;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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
  int32_t pageHeaderSize =
      sizeof(int32_t) + (sizeof(int32_t) + 2 * sizeof(uint8_t)) * blockDataGetNumOfCols(pHandle->pDataBlock);
  int32_t rowCap = blockDataGetCapacityInRow(pHandle->pDataBlock, pHandle->pageSize, pageHeaderSize);
  if (rowCap < 0) {
    return terrno;
  }

  code = blockDataEnsureCapacity(pHandle->pDataBlock, rowCap);
  if (code) {
    return code;
  }

  blockDataCleanup(pHandle->pDataBlock);
  SBlkMergeSupport sup = {0};

  SBlockOrderInfo* pOrigBlockTsOrder =
      (!pHandle->bSortByRowId) ? taosArrayGet(pHandle->pSortInfo, 0) : taosArrayGet(pHandle->aExtRowsOrders, 0);
  if (pOrigBlockTsOrder == NULL) {
    return terrno;
  }

  SBlockOrderInfo* pHandleBlockTsOrder = taosArrayGet(pHandle->pSortInfo, 0);
  if (pHandleBlockTsOrder == NULL) {
    return terrno;
  }

  SBlockOrderInfo* pOrigBlockPkOrder = NULL;
  if (pHandle->bSortPk) {
    pOrigBlockPkOrder =
        (!pHandle->bSortByRowId) ? taosArrayGet(pHandle->pSortInfo, 1) : taosArrayGet(pHandle->aExtRowsOrders, 1);
    if (pOrigBlockPkOrder == NULL) {
      return terrno;
    }
  }

  code = initMergeSup(&sup, aBlk, pOrigBlockTsOrder->order, pOrigBlockTsOrder->slotId, pOrigBlockPkOrder);
  if (code) {
    return code;
  }

  int32_t totalRows = getTotalRows(aBlk);

  SMultiwayMergeTreeInfo* pTree = NULL;
  __merge_compare_fn_t    mergeCompareFn = (!pHandle->bSortPk) ? blockCompareTsFn : blockCompareTsPkFn;

  code = tMergeTreeCreate(&pTree, taosArrayGetSize(aBlk), &sup, mergeCompareFn);
  if (TSDB_CODE_SUCCESS != code) {
    cleanupMergeSup(&sup);
    return code;
  }

  SArray* aPgId = taosArrayInit(8, sizeof(int32_t));
  if (aPgId == NULL) {
    goto _error;
  }

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
        goto _error;
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

    code = blockDataEnsureCapacity(pHandle->pDataBlock, pHandle->pDataBlock->info.rows + 1);
    if (code) {
      goto _error;
    }

    if (pHandle->bSortByRowId) {
      code = appendToRowIndexDataBlock(pHandle, minBlk, &minRow);
    } else {
      code = appendOneRowToDataBlock(pHandle->pDataBlock, minBlk, &minRow);
    }

    if (code) {
      goto _error;
    }

    blkPgSz += bufInc;
    if (blkPgSz != blockDataGetSize(pHandle->pDataBlock) + pageHeaderSize) {
      qError("sort failed at: %s:%d", __func__, __LINE__);
      goto _error;
    }

    ++nRows;

    if (sup.aRowIdx[minIdx] == minBlk->info.rows - 1) {
      sup.aRowIdx[minIdx] = -1;
    } else {
      ++sup.aRowIdx[minIdx];
    }
    code = tMergeTreeAdjust(pTree, tMergeTreeGetAdjustIndex(pTree));
    if (TSDB_CODE_SUCCESS != code) {
      goto _error;
    }
  }

  if (pHandle->pDataBlock->info.rows > 0) {
    if (!mergeLimitReached) {
      SColumnInfoData* tsCol = taosArrayGet(pHandle->pDataBlock->pDataBlock, pHandleBlockTsOrder->slotId);
      lastPageBufTs = ((int64_t*)tsCol->pData)[pHandle->pDataBlock->info.rows - 1];
      code = appendDataBlockToPageBuf(pHandle, pHandle->pDataBlock, aPgId);
      if (code != TSDB_CODE_SUCCESS) {
        goto _error;
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

  SSDataBlock* pMemSrcBlk = NULL;
  code = createOneDataBlock(pHandle->pDataBlock, false, &pMemSrcBlk);
  if (code) goto _error;

  code = doAddNewExternalMemSource(pHandle->pBuf, aExtSrc, pMemSrcBlk, &pHandle->sourceId, aPgId);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pMemSrcBlk);
    goto _error;
  }

  cleanupMergeSup(&sup);
  tMergeTreeDestroy(&pTree);

  return code;

_error:
  tMergeTreeDestroy(&pTree);
  cleanupMergeSup(&sup);
  if (aPgId) taosArrayDestroy(aPgId);
  return code;
}

// Per-table admission state of the merge-limit fetch loop. Each table of the
// merge scan is a ts-ordered stream, so the rows of one table that can still
// reach the first mergeLimit rows of the merged output are exactly the rows
// whose ts is not beyond the ts of that table's mergeLimit-th row.
typedef struct SMergeLimitTblState {
  int64_t nRows;   // rows of this table admitted into the merge so far
  int64_t cutTs;   // ts of this table's mergeLimit-th row, valid when cutSet
  bool    cutSet;  // whether the row budget is exhausted and cutTs is set
} SMergeLimitTblState;

/*
 * Decide how many leading rows of pOrigBlk may still enter the merge.
 *
 * Correctness: the merged output is ordered by (ts [, pk]) with ts first, and
 * every per-table stream is ts-ordered, so the first mergeLimit rows of the
 * untruncated merge can only contain table rows that are not beyond the
 * table's own mergeLimit-th row in ts. All rows kept here have ts equal to or
 * better than cutTs, and every row dropped is strictly beyond cutTs in ts,
 * which also keeps whole tie groups at the cut boundary: the tie selection of
 * the merge is identical to an untruncated run.
 *
 * *pTableDone reports that the remaining rows of this table are provably
 * beyond the cut value (the block was trimmed), so the caller may stop
 * fetching the table. When a block ends inside the cut tie group the flag
 * stays false because later blocks may still carry tie rows.
 *
 * Known cost: the per-table stream is only guaranteed to be ts-ordered, not
 * (ts, pk)-ordered, so the whole tie group at cutTs must be kept. A table
 * whose cut lands inside a very large equal-ts group therefore keeps every
 * row of that group (the whole table in the degenerate all-equal-ts case).
 * This is the price of not dropping tied rows; the cross-table threshold can
 * still prune that table only with a strict ts comparison, so ties are never
 * pruned early.
 */
static int32_t getRowsBlockWithinMergeLimit(const SSortHandle* pHandle, SSHashObj* mTableStates,
                                            const SColumnInfoData* pTsCol, int32_t tsOrder, SSDataBlock* pOrigBlk,
                                            int32_t* pKeepRows, bool* pTableDone) {
  int32_t code = 0;
  uint64_t uid = pOrigBlk->info.id.uid;

  SMergeLimitTblState* pState = tSimpleHashGet(mTableStates, &uid, sizeof(uid));
  if (pState == NULL) {
    SMergeLimitTblState init = {0};
    code = tSimpleHashPut(mTableStates, &uid, sizeof(uid), &init, sizeof(init));
    if (code) {
      return code;
    }
    pState = tSimpleHashGet(mTableStates, &uid, sizeof(uid));
    if (pState == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  bool    asc = (tsOrder == TSDB_ORDER_ASC);
  int32_t rows = pOrigBlk->info.rows;
  int32_t keep = 0;
  *pTableDone = false;

  const int64_t* ts = (const int64_t*)pTsCol->pData;
  if (pState->cutSet) {
    // Budget exhausted earlier: only the tie group at cutTs may still enter.
    while (keep < rows && ts[keep] == pState->cutTs) {
      ++keep;
    }
    *pTableDone = (keep < rows);
  } else if (rows <= pHandle->mergeLimit - pState->nRows) {
    // The whole block fits into the remaining row budget.
    keep = rows;
    pState->nRows += rows;
    if (pState->nRows == pHandle->mergeLimit) {
      // The budget ends exactly at this block's last row; the tie group may
      // continue into later blocks, so the table is not marked done here.
      pState->cutTs = ts[rows - 1];
      pState->cutSet = true;
    }
  } else {
    // This block fills the budget; keep the tie group at the cut row as well.
    keep = (int32_t)(pHandle->mergeLimit - pState->nRows);
    if (keep <= 0) {
      // Defensive: the branch above always marks cutSet once the budget is
      // exhausted, so this is unreachable while that invariant holds. Never
      // index ts[keep - 1] with a non-positive keep if it is ever broken.
      pState->nRows = pHandle->mergeLimit;
      pState->cutSet = true;
      *pKeepRows = 0;
      return code;
    }
    pState->nRows = pHandle->mergeLimit;
    pState->cutTs = ts[keep - 1];
    pState->cutSet = true;
    while (keep < rows && ts[keep] == pState->cutTs) {
      ++keep;
    }
    *pTableDone = (keep < rows);
  }

  *pKeepRows = keep;
  return code;
}

// Upper bound of the live threshold heap. The heap must hold the full
// mergeLimit best timestamps: only the mergeLimit-th best ts may prune a
// table. A smaller heap would expose a better (k-th) ts as the root and could
// prune rows that still belong to the first mergeLimit rows, so this live heap
// is disabled when LIMIT+OFFSET exceeds the cap; the per-table ts cut and the
// page-bound threshold update keep working.
#define TSORT_MERGE_LIMIT_TS_HEAP_CAP 4096

// Sift helpers of the threshold heap. asc keeps the mergeLimit best (smallest)
// ts values in a max-heap whose root is the worst of them; !asc mirrors it.
static void tsortTsHeapSiftUp(int64_t* heap, int32_t idx, bool asc) {
  while (idx > 0) {
    int32_t parent = (idx - 1) >> 1;
    bool    parentNotAbove = asc ? (heap[parent] < heap[idx]) : (heap[parent] > heap[idx]);
    if (!parentNotAbove) {
      break;
    }
    int64_t tmp = heap[parent];
    heap[parent] = heap[idx];
    heap[idx] = tmp;
    idx = parent;
  }
}

static void tsortTsHeapSiftDown(int64_t* heap, int32_t size, int32_t idx, bool asc) {
  while (true) {
    int32_t child = 2 * idx + 1;
    if (child >= size) {
      break;
    }
    int32_t candidate = child;
    child += 1;
    if (child < size) {
      // pick the child that is "larger" in heap order
      bool rightLarger = asc ? (heap[child] > heap[candidate]) : (heap[child] < heap[candidate]);
      if (rightLarger) {
        candidate = child;
      }
    }
    bool childAbove = asc ? (heap[candidate] > heap[idx]) : (heap[candidate] < heap[idx]);
    if (!childAbove) {
      break;
    }
    int64_t tmp = heap[candidate];
    heap[candidate] = heap[idx];
    heap[idx] = tmp;
    idx = candidate;
  }
}

/*
 * Track the mergeLimit best timestamps admitted to the merge. The heap is only
 * created when tsHeapCap == mergeLimit (see the create site), so once it is
 * full its root is an upper bound of the mergeLimit-th best ts of the whole
 * input: any row strictly worse than the root cannot belong to the first
 * mergeLimit rows of the merged output, because the heap already holds
 * mergeLimit admitted rows that are all at least as good. The bound tightens
 * monotonically as better rows arrive.
 */
static void tsortMergeLimitTrackTs(SSortHandle* pHandle, bool asc, const int64_t* ts, int32_t numOfRows) {
  if (pHandle->pTsHeap == NULL || numOfRows <= 0) {
    return;
  }
  for (int32_t i = 0; i < numOfRows; ++i) {
    int64_t v = ts[i];
    int32_t size = (int32_t)taosArrayGetSize(pHandle->pTsHeap);
    if (size < pHandle->tsHeapCap) {
      if (taosArrayPush(pHandle->pTsHeap, &v) == NULL) {
        return;
      }
      tsortTsHeapSiftUp((int64_t*)TARRAY_DATA(pHandle->pTsHeap), size, asc);
      continue;
    }
    int64_t* heap = (int64_t*)TARRAY_DATA(pHandle->pTsHeap);
    bool     better = asc ? (v < heap[0]) : (v > heap[0]);
    if (better) {
      heap[0] = v;
      tsortTsHeapSiftDown(heap, size, 0, asc);
    }
  }
  if (taosArrayGetSize(pHandle->pTsHeap) == (size_t)pHandle->tsHeapCap) {
    int64_t* heap = (int64_t*)TARRAY_DATA(pHandle->pTsHeap);
    if ((asc && heap[0] < pHandle->currMergeLimitTs) || (!asc && heap[0] > pHandle->currMergeLimitTs)) {
      pHandle->currMergeLimitTs = heap[0];
    }
  }
}

static void freeHelp(void* param) {
  SSDataBlock** ptr = param;
  if (*ptr != NULL) {
    blockDataDestroy(*ptr);
  }
}

static int32_t createBlocksMergeSortInitialSources(SSortHandle* pHandle) {
  int32_t          szSort = 0;
  int32_t          code = 0;
  int32_t          lino = 0;
  size_t           nSrc = taosArrayGetSize(pHandle->pOrderedSource);
  SArray*          aExtSrc = NULL;
  SArray*          aBlkSort = NULL;
  SSHashObj*       mTableStates = NULL;
  SSHashObj*       mUidBlk = NULL;
  SBlockOrderInfo* pOrigTsOrder = NULL;

  aExtSrc = taosArrayInit(nSrc, POINTER_BYTES);
  QUERY_CHECK_NULL(aExtSrc, code, lino, _err, terrno);

  mTableStates = tSimpleHashInit(8192, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT));
  QUERY_CHECK_NULL(mTableStates, code, lino, _err, terrno);

  aBlkSort = taosArrayInit(8, POINTER_BYTES);
  QUERY_CHECK_NULL(aBlkSort, code, lino, _err, terrno);

  mUidBlk = tSimpleHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT));
  QUERY_CHECK_NULL(mUidBlk, code, lino, _err, terrno);

  size_t maxBufSize = (pHandle->bSortByRowId) ? pHandle->extRowsMemSize : (pHandle->numOfPages * pHandle->pageSize);
  code = createPageBuf(pHandle);
  QUERY_CHECK_CODE(code, lino, _err);

  SSortSource* pSrc = taosArrayGetP(pHandle->pOrderedSource, 0);
  QUERY_CHECK_NULL(pSrc, code, lino, _err, terrno);

  pOrigTsOrder =
      (!pHandle->bSortByRowId) ? taosArrayGet(pHandle->pSortInfo, 0) : taosArrayGet(pHandle->aExtRowsOrders, 0);
  QUERY_CHECK_NULL(pOrigTsOrder, code, lino, _err, terrno);

  pHandle->currMergeLimitTs = (pOrigTsOrder->order == TSDB_ORDER_ASC) ? INT64_MAX : INT64_MIN;

  int64_t fetchedRows = 0;
  int64_t admittedRows = 0;
  int32_t prunedBlocks = 0;

  if (pHandle->mergeLimit > 0 && !pHandle->bSortByRowId &&
      pHandle->mergeLimit <= (int64_t)TSORT_MERGE_LIMIT_TS_HEAP_CAP) {
    // Live threshold for the fetch loop; see tsortMergeLimitTrackTs. Sorted by
    // row id is disabled whenever a merge limit is set, but keep the guard so
    // the two settings can never combine unnoticed. The heap only serves as a
    // prune bound when it holds the full mergeLimit best timestamps, so a
    // limit above the cap keeps the per-table ts cut and the page-bound
    // threshold update but leaves this live heap disabled.
    pHandle->tsHeapCap = (int32_t)pHandle->mergeLimit;
    pHandle->pTsHeap = taosArrayInit(pHandle->tsHeapCap, sizeof(int64_t));
    QUERY_CHECK_NULL(pHandle->pTsHeap, code, lino, _err, terrno);
  } else {
    pHandle->tsHeapCap = 0;
  }

  while (1) {
    bool         bExtractedBlock = false;
    bool         bTableDone = false;
    SSDataBlock* pBlk = NULL;

    code = pHandle->fetchfp(pSrc->param, &pBlk);
    QUERY_CHECK_CODE(code, lino, _err);

    if (pBlk != NULL && pHandle->mergeLimit > 0) {
      fetchedRows += pBlk->info.rows;
    }

    if (pBlk != NULL && pBlk->info.rows > 0 && pHandle->mergeLimit > 0) {
      SColumnInfoData* tsCol = taosArrayGet(pBlk->pDataBlock, pOrigTsOrder->slotId);
      QUERY_CHECK_NULL(tsCol, code, lino, _err, terrno);
      QUERY_CHECK_NULL(tsCol->pData, code, lino, _err, terrno);

      bool    asc = (pOrigTsOrder->order == TSDB_ORDER_ASC);
      int64_t firstRowTs = *(int64_t*)tsCol->pData;
      if ((asc && firstRowTs > pHandle->currMergeLimitTs) || (!asc && firstRowTs < pHandle->currMergeLimitTs)) {
        // Cross-table pruning: the threshold already holds mergeLimit admitted
        // rows strictly better than every row of this table, and per-table
        // streams are ts-ordered, so the whole rest of this table is beyond
        // the final output. Tell the reader to skip the table. Rows equal to
        // the threshold ts are never pruned here, so ties are kept.
        if (pHandle->mergeLimitReachedFn) {
          pHandle->mergeLimitReachedFn(pBlk->info.id.uid, pHandle->mergeLimitReachedParam);
        }
        ++prunedBlocks;
        continue;
      }

      int32_t keepRows = 0;
      code = getRowsBlockWithinMergeLimit(pHandle, mTableStates, tsCol, pOrigTsOrder->order, pBlk, &keepRows,
                                          &bTableDone);
      QUERY_CHECK_CODE(code, lino, _err);

      if (bTableDone && pHandle->mergeLimitReachedFn) {
        // The block was trimmed at the table's cut ts, so every later row of
        // this table is strictly beyond it and the reader may skip the table.
        pHandle->mergeLimitReachedFn(pBlk->info.id.uid, pHandle->mergeLimitReachedParam);
      }

      // Track the admitted rows before trimming so the threshold reflects
      // exactly the rows that continue into the merge.
      tsortMergeLimitTrackTs(pHandle, asc, (const int64_t*)tsCol->pData, keepRows);
      admittedRows += keepRows;

      if (keepRows == 0) {
        continue;
      }
      if (keepRows != pBlk->info.rows) {
        SSDataBlock* p = NULL;
        code = blockDataExtractBlock(pBlk, 0, keepRows, &p);
        QUERY_CHECK_CODE(code, lino, _err);
        pBlk = p;
        bExtractedBlock = true;
      }
    }

    if (pBlk != NULL) {
      szSort += blockDataGetSize(pBlk);
      void* ppBlk = tSimpleHashGet(mUidBlk, &pBlk->info.id.uid, sizeof(pBlk->info.id.uid));
      if (ppBlk != NULL) {
        SSDataBlock* tBlk = *(SSDataBlock**)(ppBlk);
        code = blockDataMerge(tBlk, pBlk);
        if (code != TSDB_CODE_SUCCESS && bExtractedBlock) {
          // The extracted block never reached aBlkSort/mUidBlk, so the freeHelp
          // cleanup of aBlkSort cannot release it.
          blockDataDestroy(pBlk);
        }
        QUERY_CHECK_CODE(code, lino, _err);

        if (bExtractedBlock) {
          blockDataDestroy(pBlk);
        }
      } else {
        SSDataBlock* tBlk = NULL;
        if (bExtractedBlock) {
          tBlk = pBlk;
        } else {
          code = createOneDataBlock(pBlk, true, &tBlk);
          QUERY_CHECK_CODE(code, lino, _err);
        }

        code = tSimpleHashPut(mUidBlk, &pBlk->info.id.uid, sizeof(pBlk->info.id.uid), &tBlk, POINTER_BYTES);
        if (code != TSDB_CODE_SUCCESS) {
          blockDataDestroy(tBlk);
        }
        QUERY_CHECK_CODE(code, lino, _err);

        void* px = taosArrayPush(aBlkSort, &tBlk);
        if (px == NULL) {
          blockDataDestroy(tBlk);
        }
        QUERY_CHECK_NULL(px, code, lino, _err, terrno);
      }
    }

    if ((pBlk != NULL && szSort > maxBufSize) || (pBlk == NULL && szSort > 0)) {
      tSimpleHashClear(mUidBlk);

      int64_t p = taosGetTimestampUs();
      if (pHandle->bSortByRowId) {
        code = tsortOpenRegion(pHandle);
        QUERY_CHECK_CODE(code, lino, _err);
      }

      code = sortBlocksToExtSource(pHandle, aBlkSort, aExtSrc);
      QUERY_CHECK_CODE(code, lino, _err);

      if (pHandle->bSortByRowId) {
        code = tsortCloseRegion(pHandle);  // ignore this error code
      }

      int64_t el = taosGetTimestampUs() - p;
      pHandle->sortElapsed += el;
      taosArrayClearEx(aBlkSort, freeHelp);

      szSort = 0;
      qDebug("%s source %zu created", pHandle->idStr, taosArrayGetSize(aExtSrc));
    }

    if (pBlk == NULL) {
      break;
    }

    if (tsortIsClosed(pHandle)) {
      break;
    }
  }

  tsortClearOrderedSource(pHandle->pOrderedSource, NULL, NULL);
  if (!tsortIsClosed(pHandle)) {
    void* px = taosArrayAddAll(pHandle->pOrderedSource, aExtSrc);
    QUERY_CHECK_NULL(px, code, lino, _err, terrno);
  }

  if (pHandle->bSortByRowId) {
    code = tsortFinalizeRegions(pHandle);
  }

  pHandle->type = SORT_SINGLESOURCE_SORT;

  if (pHandle->mergeLimit > 0) {
    // Fetch-side LIMIT accounting: fetched rows entered the fetch loop,
    // admitted rows survived the per-table cut and the cross-table threshold,
    // and pruned blocks were dropped whole because their table starts beyond
    // the live threshold.
    qDebug("%s merge limit fetch done, limit:%" PRId64 " fetched rows:%" PRId64 " admitted rows:%" PRId64
           " pruned blocks:%" PRId64,
           pHandle->idStr, pHandle->mergeLimit, fetchedRows, admittedRows, (int64_t)prunedBlocks);
  }

_err:
  if (code) {
    qError("%s %s failed at line %d since %s", pHandle->idStr, __func__, lino, tstrerror(code));
  }

  if (aExtSrc) {
    taosArrayDestroy(aExtSrc);
  }
  if (aBlkSort) {
    taosArrayDestroyEx(aBlkSort, freeHelp);
  }
  if (mTableStates) {
    tSimpleHashCleanup(mTableStates);
  }
  if (mUidBlk) {
    tSimpleHashCleanup(mUidBlk);
  }
  taosArrayDestroy(pHandle->pTsHeap);
  pHandle->pTsHeap = NULL;
  pHandle->tsHeapCap = 0;
  return code;
}

static void freeSortSource(void* p) {
  SSortSource** pSource = (SSortSource**)p;
  if (NULL == pSource || NULL == *pSource) {
    return;
  }

  if ((*pSource)->pageIdList) {
    taosArrayDestroy((*pSource)->pageIdList);
  }

  if (!(*pSource)->onlyRef) {
    if ((*pSource)->param) {
      taosMemoryFree((*pSource)->param);
    }
    if ((*pSource)->src.pBlock) {
      blockDataDestroy((*pSource)->src.pBlock);
    }
  }

  taosMemoryFreeClear(*pSource);
}

static int32_t createBlocksQuickSortInitialSources(SSortHandle* pHandle) {
  int32_t       code = 0;
  int32_t       lino = 0;
  size_t        sortBufSize = pHandle->numOfPages * pHandle->pageSize;
  SSortSource** p = taosArrayGet(pHandle->pOrderedSource, 0);
  if (p == NULL) {
    return terrno;
  }

  SSortSource* pSource = *p;
  size_t       origSourceCount = taosArrayGetSize(pHandle->pOrderedSource);

  while (1) {
    SSDataBlock* pBlock = NULL;
    code = pHandle->fetchfp(pSource->param, &pBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pBlock == NULL) {
      break;
    }

    if (pHandle->pDataBlock == NULL) {
      uint32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
      pHandle->pageSize = getProperSortPageSize(blockDataGetRowSize(pBlock), numOfCols);

      // todo, number of pages are set according to the total available sort buffer
      pHandle->numOfPages = 1024;
      sortBufSize = pHandle->numOfPages * pHandle->pageSize;
      code = createOneDataBlock(pBlock, false, &pHandle->pDataBlock);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pHandle->beforeFp != NULL) {
      pHandle->beforeFp(pBlock, pHandle->param);
    }

    code = blockDataMerge(pHandle->pDataBlock, pBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    size_t size = blockDataGetSize(pHandle->pDataBlock);
    if (size > sortBufSize) {
      // Perform the in-memory sort and then flush data in the buffer into disk.
      int64_t st = taosGetTimestampUs();
      code = blockDataSortEx(pHandle->pDataBlock, pHandle->pSortInfo, &pHandle->pBlkSortScratch);
      QUERY_CHECK_CODE(code, lino, _end);

      pHandle->sortElapsed += (taosGetTimestampUs() - st);

      if (pHandle->pqMaxRows > 0) blockDataKeepFirstNRows(pHandle->pDataBlock, pHandle->pqMaxRows);
      code = doAddToBuf(pHandle->pDataBlock, pHandle);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  if (pHandle->pDataBlock != NULL && pHandle->pDataBlock->info.rows > 0) {
    size_t size = blockDataGetSize(pHandle->pDataBlock);

    // Perform the in-memory sort and then flush data in the buffer into disk.
    int64_t st = taosGetTimestampUs();
    code = blockDataSortEx(pHandle->pDataBlock, pHandle->pSortInfo, &pHandle->pBlkSortScratch);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pHandle->pqMaxRows > 0) blockDataKeepFirstNRows(pHandle->pDataBlock, pHandle->pqMaxRows);
    pHandle->sortElapsed += (taosGetTimestampUs() - st);

    // All sorted data can fit in memory, external memory sort is not needed. Return to directly
    if (size <= sortBufSize && pHandle->pBuf == NULL) {
      pHandle->cmpParam.numOfSources = 1;
      pHandle->inMemSort = true;

      pHandle->loops = 1;
      pHandle->tupleHandle.rowIndex = -1;
      pHandle->tupleHandle.pBlock = pHandle->pDataBlock;
    } else {
      code = doAddToBuf(pHandle->pDataBlock, pHandle);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  taosArrayRemoveBatch(pHandle->pOrderedSource, 0, origSourceCount, freeSortSource);
  return code;
}

static int32_t createInitialSources(SSortHandle* pHandle) {
  int32_t code = 0;

  if (pHandle->type == SORT_SINGLESOURCE_SORT) {
    code = createBlocksQuickSortInitialSources(pHandle);
  } else if (pHandle->type == SORT_BLOCK_TS_MERGE) {
    code = createBlocksMergeSortInitialSources(pHandle);
  }

  qDebug("%s %zu sources created", pHandle->idStr, taosArrayGetSize(pHandle->pOrderedSource));
  return code;
}

static int32_t tsortOpenForBufMergeSort(SSortHandle* pHandle) {
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
    if (numOfSources > getNumOfInMemBufPages(pHandle->pBuf)) {
      qError("sort failed at: %s:%d", __func__, __LINE__);
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
  }

  if (numOfSources == 0) {
    return 0;
  }

  code = sortComparInit(&pHandle->cmpParam, pHandle->pOrderedSource, 0, numOfSources - 1, pHandle);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = tMergeTreeCreate(&pHandle->pMergeTree, pHandle->cmpParam.numOfSources, &pHandle->cmpParam, pHandle->comparFn);
  return code;
}

// `closed` state machine, shared by the three accessors below:
//   0 open            -> nothing closed the handle
//   1 close requested -> tsortClose() (0 -> 1)
//   2 closed/reaped   -> tsortSetClosed(), or the first consumer-side read here
// Only the 1 -> 2 transition is consumed by tsortIsClosed(); state 0 is never
// modified by a read, exactly as with the earlier bare compare-exchange against
// 1. Callers only poll the boolean, so no one depends on a 0 -> 1 side effect.
void tsortClose(SSortHandle* pHandle) { (void)atomic_val_compare_exchange_8(&pHandle->closed, 0, 1); }

bool tsortIsClosed(SSortHandle* pHandle) {
  // Same truth table as the original compare-exchange, which returned the
  // previous state: 0 -> false, 1 -> true (and consumes the flag), 2 -> true
  // (already consumed or explicitly closed by tsortSetClosed). The plain load
  // avoids the locked instruction for the common, never-closed case and does
  // not alter state 0.
  int8_t state = atomic_load_8(&pHandle->closed);
  if (state == 0) {
    return false;
  }
  if (state == 1) {
    return atomic_val_compare_exchange_8(&pHandle->closed, 1, 2) != 0;
  }
  return true;
}

void tsortSetClosed(SSortHandle* pHandle) { atomic_store_8(&pHandle->closed, 2); }

void tsortSetMergeLimit(SSortHandle* pHandle, int64_t mergeLimit) { pHandle->mergeLimit = mergeLimit; }

void tsortSetFetchRawDataFp(SSortHandle* pHandle, _sort_fetch_block_fn_t fetchFp, void (*fp)(SSDataBlock*, void*),
                            void* param) {
  pHandle->fetchfp = fetchFp;
  pHandle->beforeFp = fp;
  pHandle->param = param;
}

void tsortSetComparFp(SSortHandle* pHandle, _sort_merge_compar_fn_t fp) {
  if (pHandle) {
    pHandle->comparFn = fp;
  }
}

void tsortSetCompareGroupId(SSortHandle* pHandle, bool compareGroupId) {
  if (pHandle) {
    pHandle->cmpParam.cmpGroupId = compareGroupId;
  }
}

static int32_t tsortBufMergeSortNextTuple(SSortHandle* pHandle, STupleHandle** pTupleHandle) {
  *pTupleHandle = NULL;
  int32_t code = 0;

  if (tsortIsClosed(pHandle)) {
    return code;
  }

  if (pHandle->cmpParam.numOfSources == pHandle->numOfCompletedSources) {
    return code;
  }

  // All the data are hold in the buffer, no external sort is invoked.
  if (pHandle->inMemSort) {
    pHandle->tupleHandle.rowIndex += 1;
    if (pHandle->tupleHandle.rowIndex == pHandle->pDataBlock->info.rows) {
      pHandle->numOfCompletedSources = 1;
      return code;
    }

    *pTupleHandle = &pHandle->tupleHandle;
    return code;
  }

  int32_t      index = tMergeTreeGetChosenIndex(pHandle->pMergeTree);
  SSortSource* pSource = pHandle->cmpParam.pSources[index];

  if (pHandle->needAdjust) {
    code = adjustMergeTreeForNextTuple(pSource, pHandle->pMergeTree, pHandle, &pHandle->numOfCompletedSources);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  // all sources are completed.
  if (pHandle->cmpParam.numOfSources == pHandle->numOfCompletedSources) {
    return code;
  }

  // Get the adjusted value after the loser tree is updated.
  index = tMergeTreeGetChosenIndex(pHandle->pMergeTree);
  pSource = pHandle->cmpParam.pSources[index];

  if (UNLIKELY(pSource->src.pBlock == NULL)) {
    qError("sort failed at: %s:%d", __func__, __LINE__);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  pHandle->tupleHandle.rowIndex = pSource->src.rowIndex;
  pHandle->tupleHandle.pBlock = pSource->src.pBlock;

  pHandle->needAdjust = true;
  pSource->src.rowIndex += 1;

  *pTupleHandle = &pHandle->tupleHandle;
  return code;
}

static bool tsortIsForceUsePQSort(SSortHandle* pHandle) { return pHandle->forceUsePQSort == true; }

void tsortSetForceUsePQSort(SSortHandle* pHandle) {
  if (pHandle == NULL) {
    return;
  }
  pHandle->forceUsePQSort = true;
}

// Declares the sort-key SHAPE only: exactly two ascending keys, the first a
// primary timestamp and the second a primary key column. It says nothing about
// the order of the input; that needs tsortSetInputOrdered() (a plan proof) or
// the runtime threshold path. Keep the two flags independent.
void tsortSetTsPkSort(SSortHandle* pHandle) {
  if (pHandle == NULL) {
    return;
  }
  pHandle->tsPkSort = true;
}

void tsortSetInputOrdered(SSortHandle* pHandle) {
  if (pHandle == NULL) {
    return;
  }
  pHandle->inputOrderedBySortKeys = true;
}

static bool tsortIsPQSortApplicable(SSortHandle* pHandle) {
  if (pHandle->type != SORT_SINGLESOURCE_SORT) return false;
  if (tsortIsForceUsePQSort(pHandle)) return true;
  uint64_t maxRowsFitInMemory = pHandle->pqSortBufSize / (pHandle->pqMaxTupleLength + sizeof(char*));
  return maxRowsFitInMemory > pHandle->pqMaxRows;
}

static bool tsortPQCompFn(void* a, void* b, void* param) {
  SSortHandle* pHandle = param;
  int32_t      res = pHandle->comparFn(a, b, param);
  if (res < 0) return 1;
  return 0;
}

static bool tsortPQComFnReverse(void* a, void* b, void* param) {
  SSortHandle* pHandle = param;
  int32_t      res = pHandle->comparFn(a, b, param);
  if (res > 0) return 1;
  return 0;
}

static int32_t tupleComparFn(const void* pLeft, const void* pRight, void* param) {
  TupleDesc* pLeftDesc = (TupleDesc*)pLeft;
  TupleDesc* pRightDesc = (TupleDesc*)pRight;

  SSortHandle* pHandle = (SSortHandle*)param;
  SArray*      orderInfo = (SArray*)pHandle->pSortInfo;
  uint32_t     colNum = blockDataGetNumOfCols(pHandle->pDataBlock);
  for (int32_t i = 0; i < orderInfo->size; ++i) {
    SBlockOrderInfo* pOrder = TARRAY_GET_ELEM(orderInfo, i);
    void *           lData = NULL, *rData = NULL;

    int32_t ret1 = tupleDescGetField(pLeftDesc, pOrder->slotId, colNum, &lData);
    int32_t ret2 = tupleDescGetField(pRightDesc, pOrder->slotId, colNum, &rData);
    if (ret1 || ret2) {
      // A comparator result is only interpreted as an order verdict (negative,
      // zero or positive), so an error code must not be returned here: a
      // positive code would read as "left is greater" and reject a good row.
      // Record the failure on the handle instead; the PQ sort loop surfaces it
      // right after the push that triggered this comparison, so the query
      // fails instead of silently treating unreadable keys as equal.
      int32_t err = ret1 ? ret1 : ret2;
      pHandle->comparErr = (err != TSDB_CODE_SUCCESS) ? err : TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      qError("sort comparator failed to read key field, slotId:%d code:%d/%d", pOrder->slotId, ret1, ret2);
      return 0;
    }

    if ((!lData) && (!rData)) {
      continue;
    }

    if (!lData) return pOrder->nullFirst ? -1 : 1;
    if (!rData) return pOrder->nullFirst ? 1 : -1;

    SColumnInfoData* p = (SColumnInfoData*)taosArrayGet(pHandle->pDataBlock->pDataBlock, pOrder->slotId);
    if (p == NULL) {
      // Same policy as the key-field read failure above: record it so the PQ
      // loop surfaces the error instead of treating the unreadable key as an
      // equal one and silently returning an unordered result.
      pHandle->comparErr = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      qError("sort comparator failed to get key column, slotId:%d", pOrder->slotId);
      return 0;
    }

    __compar_fn_t fn = getKeyComparFunc(p->info.type, pOrder->order);

    int32_t ret = fn(lData, rData);
    if (ret == 0) {
      continue;
    } else {
      return ret;
    }
  }

  return 0;
}

static const char* tsortSafeIdStr(const SSortHandle* pHandle) {
  // Placeholder instead of an empty string so log lines never start with a
  // blank field when the handle has no id.
  return (pHandle != NULL && pHandle->idStr != NULL) ? pHandle->idStr : "-";
}

// Stack key buffers used by the P1-B short-circuit path. Eligibility admits
// only a single key or the two-key (ts, pk) shape; the path re-checks the bound
// before filling the buffers so widening the predicate cannot overflow them.
#define TSORT_SHORT_CIRCUIT_MAX_KEYS 2

// Runtime short-circuit eligibility for inputs whose order the plan did not
// prove. Keep the preconditions of the previous suffix validator: one
// TIMESTAMP first key, or the composite tsPk shape, and no variable-width key
// (the conservative cost choice kept from the earlier review).
static bool tsortShortCircuitEligible(const SSortHandle* pHandle, const SSDataBlock* pBlock) {
  if (pHandle == NULL || pHandle->pDataBlock == NULL || pHandle->pDataBlock->pDataBlock == NULL) {
    return false;
  }
  size_t keyNum = taosArrayGetSize(pHandle->pSortInfo);
  if (keyNum != 1 && !(keyNum == 2 && pHandle->tsPkSort)) {
    return false;
  }
  if (pBlock == NULL || pBlock->pDataBlock == NULL ||
      blockDataGetNumOfCols(pBlock) != blockDataGetNumOfCols(pHandle->pDataBlock)) {
    return false;
  }

  int32_t colNum = (int32_t)taosArrayGetSize(pBlock->pDataBlock);
  for (size_t keyIdx = 0; keyIdx < keyNum; ++keyIdx) {
    SBlockOrderInfo* pOrder = TARRAY_GET_ELEM(pHandle->pSortInfo, keyIdx);
    if (pOrder->slotId < 0 || pOrder->slotId >= colNum) {
      return false;
    }
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, pOrder->slotId);
    if (pCol == NULL || pCol->pData == NULL) {
      return false;
    }
    if (keyIdx == 0 && pCol->info.type != TSDB_DATA_TYPE_TIMESTAMP) {
      // The planner may mark the shape from the parse tree only; the runtime
      // column behind the sort slot can differ (e.g. after slot rewriting).
      // Stay safe on the row-wise path and log why the optimization was lost.
      qTrace("%s suffix fallback, sort key 0 is not timestamp, type:%d", tsortSafeIdStr(pHandle), pCol->info.type);
      return false;
    }
    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      qTrace("%s suffix fallback, variable-width key, keyIdx:%d type:%d", tsortSafeIdStr(pHandle), (int32_t)keyIdx,
             pCol->info.type);
      return false;
    }
  }

  return true;
}

// Lightweight runtime re-check of the plan's ordered-input proof. The flag is
// trusted in release builds (the per-row suffix check below only runs at trace
// level), so a planner or slot-rewrite bug would drop the block suffix without
// warning. Validate the key shape the proof depends on against the actual
// block: exactly the two (ts, pk) keys, first key TIMESTAMP, both fixed-width,
// and the same column layout as the first input block. A mismatch falls back
// to the row-wise path instead of trusting the contract.
static bool tsortPlanOrderStillValid(const SSortHandle* pHandle, const SSDataBlock* pBlock) {
  if (pHandle == NULL || pHandle->pDataBlock == NULL || pHandle->pDataBlock->pDataBlock == NULL ||
      pBlock == NULL || pBlock->pDataBlock == NULL) {
    return false;
  }
  if (taosArrayGetSize(pHandle->pSortInfo) != 2) {
    return false;
  }
  int32_t colNum = (int32_t)taosArrayGetSize(pBlock->pDataBlock);
  if (colNum != (int32_t)taosArrayGetSize(pHandle->pDataBlock->pDataBlock)) {
    return false;
  }
  for (int32_t k = 0; k < 2; ++k) {
    SBlockOrderInfo* pOrder = TARRAY_GET_ELEM(pHandle->pSortInfo, k);
    if (pOrder->slotId < 0 || pOrder->slotId >= colNum) {
      return false;
    }
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, pOrder->slotId);
    SColumnInfoData* pRef = taosArrayGet(pHandle->pDataBlock->pDataBlock, pOrder->slotId);
    if (pCol == NULL || pRef == NULL || pCol->info.type != pRef->info.type) {
      return false;
    }
    if (k == 0 && pCol->info.type != TSDB_DATA_TYPE_TIMESTAMP) {
      return false;
    }
    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      return false;
    }
  }
  return true;
}

// Diagnostic check of the plan's full-key input order contract. The flag is
// carried through the plan and never re-derived at runtime, so a planner or a
// slot-rewrite bug would otherwise drop the block suffix without warning. The
// check compares every adjacent key pair in the suffix, which costs a little
// per row, so it only runs when trace logging is enabled; release builds keep
// the zero-cost fast path. A false result keeps the caller on the row-wise
// path instead of trusting the contract.
static bool tsortSuffixIsOrdered(const SSortHandle* pHandle, const SSDataBlock* pBlock, size_t startRow) {
  if ((qDebugFlag & DEBUG_TRACE) == 0) {
    return true;
  }
  if (pBlock == NULL || pBlock->pDataBlock == NULL) {
    return false;
  }

  size_t  keyNum = taosArrayGetSize(pHandle->pSortInfo);
  int32_t colNum = (int32_t)taosArrayGetSize(pBlock->pDataBlock);
  for (size_t row = startRow + 1; row < pBlock->info.rows; ++row) {
    for (size_t k = 0; k < keyNum; ++k) {
      SBlockOrderInfo* pOrder = TARRAY_GET_ELEM(pHandle->pSortInfo, k);
      if (pOrder->slotId < 0 || pOrder->slotId >= colNum) {
        qError("%s input order check failed, invalid sort slot:%d", tsortSafeIdStr(pHandle), pOrder->slotId);
        return false;
      }

      SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, pOrder->slotId);
      if (pCol == NULL || pCol->pData == NULL) {
        qError("%s input order check failed, missing key column, slotId:%d", tsortSafeIdStr(pHandle),
               pOrder->slotId);
        return false;
      }

      bool    prevNull = colDataIsNull_s(pCol, (uint32_t)(row - 1));
      bool    curNull = colDataIsNull_s(pCol, (uint32_t)row);
      int32_t cmp = 0;
      if (prevNull && curNull) {
        continue;
      } else if (prevNull) {
        cmp = pOrder->nullFirst ? -1 : 1;
      } else if (curNull) {
        cmp = pOrder->nullFirst ? 1 : -1;
      } else {
        __compar_fn_t fn = getKeyComparFunc(pCol->info.type, pOrder->order);
        if (fn == NULL) {
          return false;
        }
        cmp = fn(colDataGetData(pCol, (uint32_t)(row - 1)), colDataGetData(pCol, (uint32_t)row));
      }

      if (cmp > 0) {
        qError("%s input order contract violated, key:%d row:%zu", tsortSafeIdStr(pHandle), (int32_t)k, row);
        return false;
      }
      if (cmp < 0) {
        break;
      }
    }
  }

  return true;
}

static int32_t tsortOpenForPQSort(SSortHandle* pHandle) {
  pHandle->pBoundedQueue = createBoundedQueue(pHandle->pqMaxRows, tsortPQCompFn, destroyTuple, pHandle);
  if (NULL == pHandle->pBoundedQueue) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  tsortSetComparFp(pHandle, tupleComparFn);

  SSortSource** pSource = taosArrayGet(pHandle->pOrderedSource, 0);
  if (pSource == NULL) {
    return terrno;
  }

  SSortSource*      source = *pSource;
  uint32_t          tupleLen = 0;
  PriorityQueueNode pqNode;
  pHandle->pDataBlock = NULL;

  while (1) {
    // fetch data
    SSDataBlock* pBlock = NULL;
    TAOS_CHECK_RETURN(pHandle->fetchfp(source->param, &pBlock));
    if (NULL == pBlock) {
      break;
    }

    if (pHandle->beforeFp != NULL) {
      pHandle->beforeFp(pBlock, pHandle->param);
    }

    if (pHandle->pDataBlock == NULL) {
      int32_t code = createOneDataBlock(pBlock, false, &pHandle->pDataBlock);
      if (code) {
        return code;
      }
    }

    size_t colNum = blockDataGetNumOfCols(pBlock);

    if (tupleLen == 0) {
      for (size_t colIdx = 0; colIdx < colNum; ++colIdx) {
        SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, colIdx);
        if (pCol == NULL) {
          return terrno;
        }

        if (pCol->info.bytes <= 0 || (uint64_t)tupleLen + (uint32_t)pCol->info.bytes > UINT32_MAX) {
          return TSDB_CODE_INVALID_MSG;
        }
        tupleLen += (uint32_t)pCol->info.bytes;
        if (IS_VAR_DATA_TYPE(pCol->info.type)) {
          uint32_t headerLen = IS_STR_DATA_BLOB(pCol->info.type) ? sizeof(BlobDataLenT) : sizeof(VarDataLenT);
          if (tupleLen > UINT32_MAX - headerLen) return TSDB_CODE_INVALID_MSG;
          tupleLen += headerLen;
        }
      }
    }

    ReferencedTuple refTuple = {.desc.data = (char*)pBlock, .desc.type = ReferencedTupleType, .rowIndex = 0};
    bool shortCircuitChecked = false;
    for (size_t rowIdx = 0; rowIdx < pBlock->info.rows; ++rowIdx) {
      refTuple.rowIndex = rowIdx;
      pqNode.data = &refTuple;
      PriorityQueueNode* pPushedNode = taosBQPush(pHandle->pBoundedQueue, &pqNode);
      if (pHandle->comparErr != TSDB_CODE_SUCCESS) {
        // tupleComparFn could not read a key field while sifting this node in.
        // Fail the sort instead of keeping a tuple it could not compare.
        return pHandle->comparErr;
      }
      if (pPushedNode) {
        pPushedNode->data = NULL;
        int32_t code = createAllocatedTuple(pBlock, colNum, tupleLen, rowIdx, (TupleDesc**)&pPushedNode->data);
        if (code) {
          return code;
        }
        continue;
      }

      if (shortCircuitChecked) {
        continue;
      }

      // A push is rejected only when the queue is at max+1 and the row is not
      // better than the current threshold. Only then can the block suffix be
      // short-circuited; later blocks are still processed normally.
      if (taosBQSize(pHandle->pBoundedQueue) != taosBQMaxSize(pHandle->pBoundedQueue) + 1) {
        // taosBQPush returns NULL either for a threshold rejection or for a
        // failed backing-array allocation. The rejection case always leaves
        // the queue at max+1, so this branch is an allocation failure: keep
        // the row from being dropped silently.
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      shortCircuitChecked = true;

      if (pHandle->inputOrderedBySortKeys) {
        // P1-A: the planner proved that the physical input is ordered by the
        // complete sort key list, so every remaining row of this block follows
        // the rejected row and is strictly worse than the threshold. Skip the
        // block suffix. A rejected last row leaves an empty suffix, so there is
        // nothing to skip or verify.
        if (rowIdx + 1 >= pBlock->info.rows) {
          continue;
        }
        // Re-check the columns the proof depends on against the actual block,
        // then re-validate the order itself when trace logging is on. Either
        // check failing keeps the remaining rows on the row-wise path.
        if (!tsortPlanOrderStillValid(pHandle, pBlock)) {
          qTrace("%s suffix fallback, plan order contract does not match the block", tsortSafeIdStr(pHandle));
          continue;
        }
        if (!tsortSuffixIsOrdered(pHandle, pBlock, rowIdx)) {
          continue;
        }
        qTrace("%s suffix skip, ordered rows after rejected row, reason:plan skipped:%" PRId64
               " block rows:%" PRId64 " start row:%zu",
               tsortSafeIdStr(pHandle), (int64_t)pBlock->info.rows - (int64_t)rowIdx, pBlock->info.rows, rowIdx);
        break;
      }

      if (!tsortShortCircuitEligible(pHandle, pBlock)) {
        // Conservative fallback: keep the row-wise PQ for the rest of the block.
        continue;
      }

      // P1-B: the order is not proven. Short-circuit the rest of the block
      // against the live heap threshold, skipping only rows that the PQ
      // comparator would reject. The comparison reuses the per-key comparator
      // function and NULL ordering of tupleComparFn(), but reads the threshold
      // tuple's key fields once per heap change instead of unpacking both
      // tuples on every row. A row that may enter resumes the normal PQ
      // insertion; the threshold is re-read after every accepted row because
      // the heap changes.
      size_t keyNum = taosArrayGetSize(pHandle->pSortInfo);
      if (keyNum > TSORT_SHORT_CIRCUIT_MAX_KEYS) {
        // Cannot happen while eligibility stays in sync with the buffers, but
        // never index past them if the predicate is widened later. This is an
        // internal-invariant diagnostic, not a query failure: warn and fall
        // back to the row-wise path.
        qWarn("%s suffix fallback, too many sort keys:%zu", tsortSafeIdStr(pHandle), keyNum);
        continue;
      }

      SColumnInfoData* keyCols[TSORT_SHORT_CIRCUIT_MAX_KEYS] = {NULL, NULL};
      __compar_fn_t    keyFns[TSORT_SHORT_CIRCUIT_MAX_KEYS] = {NULL, NULL};
      bool             keyReady = true;
      for (size_t k = 0; k < keyNum; ++k) {
        SBlockOrderInfo* pOrder = TARRAY_GET_ELEM(pHandle->pSortInfo, k);
        keyCols[k] = taosArrayGet(pBlock->pDataBlock, pOrder->slotId);
        if (keyCols[k] == NULL) {
          keyReady = false;
          break;
        }
        keyFns[k] = getKeyComparFunc(keyCols[k]->info.type, pOrder->order);
        if (keyFns[k] == NULL) {
          keyReady = false;
          break;
        }
      }
      if (!keyReady) {
        continue;
      }

      int64_t  skipped = 1;  // the rejected row itself
      // tupleDescGetField returns non-zero on failure and only leaves a NULL
      // field for a genuine SQL NULL. A failed read aborts this block before
      // the comparison loop, so NULL entries here mean NULL keys, never a
      // read failure.
      void*    topKeyData[TSORT_SHORT_CIRCUIT_MAX_KEYS] = {NULL, NULL};
      bool     topKeysValid = false;
      bool     fallbackRowWise = false;
      for (size_t next = rowIdx + 1; next < pBlock->info.rows; ++next) {
        refTuple.rowIndex = next;
        if (!topKeysValid) {
          // Re-read the threshold keys from the live heap top after every
          // change. taosBQPush frees the replaced tuple and pqHeapify may
          // reorder the heap, so caching a pointer into the old tuple can
          // dangle or alias a recycled allocation.
          PriorityQueueNode* top = taosBQTop(pHandle->pBoundedQueue);
          if (top == NULL || top->data == NULL) {
            // The queue is full here, so this only happens on an allocation
            // failure. Do not dereference it: resume the row-wise path for the
            // rest of the block instead of dropping rows. Both this and the
            // unreadable-key case below are recoverable, so warn rather than
            // raise an error-level diagnostic.
            qWarn("%s suffix fallback, empty heap top", tsortSafeIdStr(pHandle));
            fallbackRowWise = true;
            rowIdx = next - 1;
            break;
          }
          bool topReadFailed = false;
          for (size_t k = 0; k < keyNum; ++k) {
            SBlockOrderInfo* pOrder = TARRAY_GET_ELEM(pHandle->pSortInfo, k);
            int32_t rc = tupleDescGetField((TupleDesc*)top->data, pOrder->slotId, colNum, &topKeyData[k]);
            if (rc) {
              // Same policy as the other failures in this path: fall back to
              // the row-wise PQ for the rest of the block instead of failing
              // the query. The row-wise comparator reports the same failure.
              qWarn("%s suffix fallback, cannot read heap top key:%d slotId:%d code:%d", tsortSafeIdStr(pHandle),
                    (int32_t)k, pOrder->slotId, rc);
              topReadFailed = true;
              break;
            }
          }
          if (topReadFailed) {
            fallbackRowWise = true;
            rowIdx = next - 1;
            break;
          }
          topKeysValid = true;
        }

        int32_t cmp = 0;
        for (size_t k = 0; k < keyNum; ++k) {
          SBlockOrderInfo* pOrder = TARRAY_GET_ELEM(pHandle->pSortInfo, k);
          bool topNull = (topKeyData[k] == NULL);
          bool rowNull = colDataIsNull_s(keyCols[k], next);
          void* rowData = rowNull ? NULL : colDataGetData(keyCols[k], next);
          if (topNull && rowNull) {
            continue;
          } else if (topNull) {
            cmp = pOrder->nullFirst ? -1 : 1;
          } else if (rowNull) {
            cmp = pOrder->nullFirst ? 1 : -1;
          } else {
            cmp = keyFns[k](topKeyData[k], rowData);
          }
          if (cmp != 0) {
            break;
          }
        }

        // The bounded queue rejects a row exactly when the threshold is
        // strictly better, i.e. comparFn(threshold, row) < 0. Equal keys are
        // accepted: tsortPQCompFn returns false for cmp == 0, and theap.c then
        // replaces the threshold tuple, so only strict rejections are counted.
        if (cmp < 0) {
          ++skipped;
          continue;
        }

        pqNode.data = &refTuple;
        pPushedNode = taosBQPush(pHandle->pBoundedQueue, &pqNode);
        if (pHandle->comparErr != TSDB_CODE_SUCCESS) {
          return pHandle->comparErr;
        }
        if (pPushedNode) {
          pPushedNode->data = NULL;
          int32_t code = createAllocatedTuple(pBlock, colNum, tupleLen, next, (TupleDesc**)&pPushedNode->data);
          if (code) {
            return code;
          }
          // The heap top may have moved or its tuple been replaced.
          topKeysValid = false;
        }
      }

      if (fallbackRowWise) {
        // rowIdx already points at the last row handled before the fallback,
        // so the outer loop resumes with the row that was not compared yet.
        continue;
      }

      qTrace("%s suffix skip, ordered rows after rejected row, reason:threshold skipped:%" PRId64
             " block rows:%" PRId64 " start row:%zu",
             tsortSafeIdStr(pHandle), skipped, pBlock->info.rows, rowIdx);
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tsortPQSortNextTuple(SSortHandle* pHandle, STupleHandle** pTupleHandle) {
  int32_t code = 0;

  *pTupleHandle = NULL;
  if (pHandle->pDataBlock == NULL) {  // when no input stream datablock
    return code;
  }

  blockDataCleanup(pHandle->pDataBlock);
  code = blockDataEnsureCapacity(pHandle->pDataBlock, 1);
  if (code) {
    return code;
  }

  // abandon the top tuple if queue size bigger than max size
  if (taosBQSize(pHandle->pBoundedQueue) == taosBQMaxSize(pHandle->pBoundedQueue) + 1) {
    taosBQPop(pHandle->pBoundedQueue);
  }
  if (pHandle->tmpRowIdx == 0) {
    // sort the results
    taosBQSetFn(pHandle->pBoundedQueue, tsortPQComFnReverse);
    taosBQBuildHeap(pHandle->pBoundedQueue);
    if (pHandle->comparErr != TSDB_CODE_SUCCESS) {
      // The reverse comparator hit an unreadable key field while building the
      // output heap. Surface it instead of emitting a partially ordered block.
      return pHandle->comparErr;
    }
  }
  if (taosBQSize(pHandle->pBoundedQueue) > 0) {
    uint32_t           colNum = blockDataGetNumOfCols(pHandle->pDataBlock);
    PriorityQueueNode* node = taosBQTop(pHandle->pBoundedQueue);
    char*              pTuple = ((TupleDesc*)node->data)->data;

    for (uint32_t i = 0; i < colNum; ++i) {
      void* pData = tupleGetField(pTuple, i, colNum);

      SColumnInfoData* p = NULL;
      TAOS_CHECK_RETURN(bdGetColumnInfoData(pHandle->pDataBlock, i, &p));

      if (!pData) {
        colDataSetNULL(p, 0);
      } else {
        TAOS_CHECK_RETURN(colDataSetVal(p, 0, pData, false));
      }
    }
    pHandle->pDataBlock->info.rows++;
    pHandle->tmpRowIdx++;
    taosBQPop(pHandle->pBoundedQueue);
  }

  if (pHandle->pDataBlock->info.rows == 0) {
    return code;
  }

  pHandle->tupleHandle.pBlock = pHandle->pDataBlock;
  *pTupleHandle = &pHandle->tupleHandle;
  return code;
}

static int32_t tsortSingleTableMergeNextTuple(SSortHandle* pHandle, STupleHandle** pTupleHandle) {
  *pTupleHandle = NULL;
  int32_t code = 0;

  if (1 == pHandle->numOfCompletedSources) {
    return code;
  }

  if (pHandle->tupleHandle.pBlock && pHandle->tupleHandle.rowIndex + 1 < pHandle->tupleHandle.pBlock->info.rows) {
    pHandle->tupleHandle.rowIndex++;
  } else {
    if (pHandle->tupleHandle.rowIndex == -1) {
      return code;
    }

    SSortSource** pSource = taosArrayGet(pHandle->pOrderedSource, 0);
    if (pSource == NULL) {
      return terrno;
    }

    SSortSource* source = *pSource;
    SSDataBlock* pBlock = NULL;
    TAOS_CHECK_RETURN(pHandle->fetchfp(source->param, &pBlock));

    if (!pBlock || pBlock->info.rows == 0) {
      setCurrentSourceDone(source, pHandle);
      pHandle->tupleHandle.pBlock = NULL;
      return code;
    }

    pHandle->tupleHandle.pBlock = pBlock;
    pHandle->tupleHandle.rowIndex = 0;
  }

  *pTupleHandle = &pHandle->tupleHandle;
  return code;
}

int32_t tsortOpen(SSortHandle* pHandle) {
  int32_t code = 0;
  if (pHandle == NULL || pHandle->fetchfp == NULL || pHandle->comparFn == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pHandle->opened) {
    return pHandle->openCode;
  }

  // A reentrant open must not observe the default success code.
  pHandle->openCode = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  pHandle->opened = true;
  if (tsortIsPQSortApplicable(pHandle)) {
    code = tsortOpenForPQSort(pHandle);
  } else {
    code = tsortOpenForBufMergeSort(pHandle);
  }

  pHandle->openCode = code;
  return code;
}

int32_t tsortNextTuple(SSortHandle* pHandle, STupleHandle** pTupleHandle) {
  int32_t code = 0;

  if (pHandle->singleTableMerge) {
    code = tsortSingleTableMergeNextTuple(pHandle, pTupleHandle);
  } else if (pHandle->pBoundedQueue) {
    code = tsortPQSortNextTuple(pHandle, pTupleHandle);
  } else {
    code = tsortBufMergeSortNextTuple(pHandle, pTupleHandle);
  }

  return code;
}

bool tsortIsNullVal(STupleHandle* pVHandle, int32_t colIndex) {
  SColumnInfoData* pColInfoSrc = taosArrayGet(pVHandle->pBlock->pDataBlock, colIndex);
  if (pColInfoSrc == NULL || pColInfoSrc->pData == NULL) {
    return true;
  }

  return colDataIsNull_s(pColInfoSrc, pVHandle->rowIndex);
}

void tsortGetTuplePos(const STupleHandle* pVHandle, const SSDataBlock** ppBlock, int32_t* pRowIndex) {
  // Tolerate a NULL handle: callers use a NULL block as the "no row" signal,
  // which is exactly what an empty handle means.
  if (ppBlock != NULL) {
    *ppBlock = (pVHandle != NULL) ? pVHandle->pBlock : NULL;
  }
  if (pRowIndex != NULL) {
    *pRowIndex = (pVHandle != NULL) ? pVHandle->rowIndex : -1;
  }
}

void tsortGetValue(STupleHandle* pVHandle, int32_t colIndex, void** pVal) {
  *pVal = NULL;
  SColumnInfoData* pColInfo = TARRAY_GET_ELEM(pVHandle->pBlock->pDataBlock, colIndex);
  if (pColInfo->pData != NULL) {
    *pVal = colDataGetData(pColInfo, pVHandle->rowIndex);
  }
}

void tsortGetColumnInfo(STupleHandle* pVHandle, int32_t colIndex, SColumnInfoData** pColInfo) {
  *pColInfo = TARRAY_GET_ELEM(pVHandle->pBlock->pDataBlock, colIndex);
}

size_t   tsortGetColNum(STupleHandle* pVHandle) { return blockDataGetNumOfCols(pVHandle->pBlock); }
void     tsortGetGroupId(STupleHandle* pVHandle, uint64_t* gid, uint64_t* baseGid) { 
  *gid = pVHandle->pBlock->info.id.groupId; 
  *baseGid = pVHandle->pBlock->info.id.baseGId; 
}
int64_t tsortGetBlockId(STupleHandle* pVHandle) { return pVHandle->pBlock->info.id.blockId; }
void tsortGetBlockInfo(STupleHandle* pVHandle, SDataBlockInfo* pBlockInfo) {
  *pBlockInfo = pVHandle->pBlock->info;
  pBlockInfo->pBlockAgg = NULL;
}

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

int64_t tsortGetFetchRawDataTime(SSortHandle* pHandle) {
  return (pHandle == NULL) ? 0 : pHandle->fetchTotalUs;
}

int32_t tsortCompAndBuildKeys(const SArray* pSortCols, char* keyBuf, int32_t* keyLen, const STupleHandle* pTuple) {
  int32_t ret;
  if (0 == compKeys(pSortCols, keyBuf, *keyLen, pTuple->pBlock, pTuple->rowIndex)) {
    ret = 0;
  } else {
    *keyLen = buildKeys(keyBuf, pSortCols, pTuple->pBlock, pTuple->rowIndex);
    // buildKeys returns the (negative) error code when a key value overruns the
    // key buffer; error codes are negative by convention, so pass it through.
    if (*keyLen < 0) {
      return *keyLen;
    }
    ret = 1;
  }
  return ret;
}

void tsortSetMergeLimitReachedFp(SSortHandle* pHandle, void (*mergeLimitReachedCb)(uint64_t tableUid, void* param),
                                 void*        param) {
  pHandle->mergeLimitReachedFn = mergeLimitReachedCb;
  pHandle->mergeLimitReachedParam = param;
}
