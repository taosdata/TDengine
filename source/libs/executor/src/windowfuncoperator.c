/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "executil.h"
#include "executorInt.h"
#include "filter.h"
#include "function.h"
#include "functionMgt.h"
#include "operator.h"
#include "os.h"
#include "querytask.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tpagedbuf.h"
#include "windowfunc.h"

#include <math.h>

typedef struct SWindowFuncOperatorInfo {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
  SExprSupp          scalarSup;
  SExprSupp          funcSupp;
  SNodeList         *pAggFuncs;
  SWindowInputStore *pInputStore;
  SResultRow        *pAggRow;
  int32_t           *pOutputSrcSlots;
  int32_t            numOfOutputSrcSlots;
  int32_t            nextRow;
  int32_t            partitionStart;
  int32_t            partitionEnd;
  int32_t            peerStart;
  int32_t            peerEnd;
  int64_t            denseRank;
  bool               inputBuilt;
} SWindowFuncOperatorInfo;

typedef struct SWindowInputPage {
  int32_t pageId;
  int32_t startRow;
  int32_t rows;
} SWindowInputPage;

struct SWindowInputStore {
  SDiskbasedBuf *pBuf;
  SSDataBlock   *pTemplate;
  SSDataBlock   *pPageBlocks[2];
  SArray        *pPages;
  int32_t        totalRows;
  int32_t        pageSize;
  int32_t        currentPageIndexes[2];
};

static int64_t winMaxI64(int64_t lhs, int64_t rhs) { return lhs > rhs ? lhs : rhs; }

static int64_t winMinI64(int64_t lhs, int64_t rhs) { return lhs < rhs ? lhs : rhs; }

static void winFuncMarkBlockMaterialized(SSDataBlock *pBlock) {
  if (pBlock == NULL) {
    return;
  }

  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData *pCol = taosArrayGet(pBlock->pDataBlock, i);
    if (pCol != NULL) {
      pCol->info.noData = false;
    }
  }
}

static int64_t winSubI64Saturating(int64_t lhs, int64_t rhs) {
  if (rhs > 0 && lhs < INT64_MIN + rhs) {
    return INT64_MIN;
  }
  return lhs - rhs;
}

static int64_t winAddI64Saturating(int64_t lhs, int64_t rhs) {
  if (rhs > 0 && lhs > INT64_MAX - rhs) {
    return INT64_MAX;
  }
  return lhs + rhs;
}

static int32_t winReadOffset(const SNode *pNode, int64_t *pOffset) {
  if (pNode == NULL || pOffset == NULL || nodeType(pNode) != QUERY_NODE_VALUE) {
    return TSDB_CODE_INVALID_PARA;
  }

  const SValueNode *pValue = (const SValueNode *)pNode;
  if (pValue->isNull || pValue->datum.i < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pOffset = pValue->datum.i;
  return TSDB_CODE_SUCCESS;
}

static int32_t winReadRangeBoundOffset(const SSqlWindowBound *pBound, int64_t *pOffset) {
  switch (pBound->boundType) {
    case WINDOW_BOUND_N_PRECEDING:
    case WINDOW_BOUND_N_FOLLOWING:
      return winReadOffset(pBound->pOffset, pOffset);
    case WINDOW_BOUND_CURRENT_ROW:
      *pOffset = 0;
      return TSDB_CODE_SUCCESS;
    default:
      return TSDB_CODE_INVALID_PARA;
  }
}

static int32_t winReadRangeBoundOffsetDouble(const SSqlWindowBound *pBound, double *pOffset) {
  int64_t offset = 0;
  int32_t code = winReadRangeBoundOffset(pBound, &offset);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  *pOffset = (double)offset;
  return TSDB_CODE_SUCCESS;
}

static int32_t winCalcBoundPosition(int64_t rowIndex, int64_t partitionRows, const SSqlWindowBound *pBound,
                                    int64_t *pPosition) {
  switch (pBound->boundType) {
    case WINDOW_BOUND_UNBOUNDED_PRECEDING:
      *pPosition = 0;
      return TSDB_CODE_SUCCESS;
    case WINDOW_BOUND_N_PRECEDING: {
      int64_t offset = 0;
      int32_t code = winReadOffset(pBound->pOffset, &offset);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      *pPosition = rowIndex - offset;
      return TSDB_CODE_SUCCESS;
    }
    case WINDOW_BOUND_CURRENT_ROW:
      *pPosition = rowIndex;
      return TSDB_CODE_SUCCESS;
    case WINDOW_BOUND_N_FOLLOWING: {
      int64_t offset = 0;
      int32_t code = winReadOffset(pBound->pOffset, &offset);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      *pPosition = rowIndex + offset;
      return TSDB_CODE_SUCCESS;
    }
    case WINDOW_BOUND_UNBOUNDED_FOLLOWING:
      *pPosition = partitionRows - 1;
      return TSDB_CODE_SUCCESS;
    default:
      return TSDB_CODE_INVALID_PARA;
  }
}

int32_t winCalcRowsFrame(int64_t rowIndex, int64_t partitionRows, const SWindowFrameNode *pFrame,
                         SSqlWindowFrameRange *pRange) {
  if (pFrame == NULL || pRange == NULL || partitionRows <= 0 || rowIndex < 0 || rowIndex >= partitionRows ||
      pFrame->frameUnit != WINDOW_FRAME_UNIT_ROWS) {
    return TSDB_CODE_INVALID_PARA;
  }

  pRange->start = 0;
  pRange->end = -1;

  int64_t start = 0;
  int64_t end = 0;
  int32_t code = winCalcBoundPosition(rowIndex, partitionRows, &pFrame->start, &start);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = winCalcBoundPosition(rowIndex, partitionRows, &pFrame->end, &end);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  start = winMaxI64(start, 0);
  end = winMinI64(end, partitionRows - 1);
  if (start > end) {
    pRange->start = 0;
    pRange->end = -1;
    return TSDB_CODE_SUCCESS;
  }

  pRange->start = start;
  pRange->end = end;
  return TSDB_CODE_SUCCESS;
}

int32_t winCalcRangeFrameForInt64(const int64_t *values, int64_t rows, int64_t rowIndex, int64_t preceding,
                                  int64_t following, SSqlWindowFrameRange *pRange) {
  if (values == NULL || pRange == NULL || rows <= 0 || rowIndex < 0 || rowIndex >= rows || preceding < 0 ||
      following < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  int64_t current = values[rowIndex];
  int64_t lower = winSubI64Saturating(current, preceding);
  int64_t upper = winAddI64Saturating(current, following);

  pRange->start = 0;
  pRange->end = -1;
  for (int64_t i = 0; i < rows; ++i) {
    if (values[i] < lower || values[i] > upper) {
      continue;
    }

    if (pRange->start > pRange->end) {
      pRange->start = i;
    }
    pRange->end = i;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t winCalcRangeFrameForDouble(const double *values, int64_t rows, int64_t rowIndex, double preceding,
                                   double following, SSqlWindowFrameRange *pRange) {
  if (values == NULL || pRange == NULL || rows <= 0 || rowIndex < 0 || rowIndex >= rows || isnan(preceding) ||
      isnan(following) || preceding < 0 || following < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  double current = values[rowIndex];

  pRange->start = 0;
  pRange->end = -1;
  if (isnan(current)) {
    for (int64_t i = 0; i < rows; ++i) {
      if (!isnan(values[i])) {
        continue;
      }

      if (pRange->start > pRange->end) {
        pRange->start = i;
      }
      pRange->end = i;
    }
    return TSDB_CODE_SUCCESS;
  }

  double lower = current - preceding;
  double upper = current + following;
  if (lower > upper) {
    return TSDB_CODE_SUCCESS;
  }

  for (int64_t i = 0; i < rows; ++i) {
    double value = values[i];
    if (isnan(value) || value < lower || value > upper) {
      continue;
    }

    if (pRange->start > pRange->end) {
      pRange->start = i;
    }
    pRange->end = i;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t winCalcRankValue(int64_t rowIndex, int64_t peerStart, int64_t denseRank, int64_t *pRank) {
  if (pRank == NULL || rowIndex < 0 || peerStart < 0 || peerStart > rowIndex || denseRank <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pRank = peerStart + 1;
  return TSDB_CODE_SUCCESS;
}

int32_t winCalcPercentRank(int64_t rank, int64_t partitionRows, double *pValue) {
  if (pValue == NULL || rank < 1 || partitionRows < 1 || rank > partitionRows) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (partitionRows == 1) {
    *pValue = 0.0;
    return TSDB_CODE_SUCCESS;
  }

  *pValue = (double)(rank - 1) / (double)(partitionRows - 1);
  return TSDB_CODE_SUCCESS;
}

int32_t winCalcCumeDist(int64_t peerEnd, int64_t partitionRows, double *pValue) {
  if (pValue == NULL || peerEnd < 0 || partitionRows < 1 || peerEnd >= partitionRows) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pValue = (double)(peerEnd + 1) / (double)partitionRows;
  return TSDB_CODE_SUCCESS;
}

int32_t winFuncCheckDedicatedFallback(const char *pFuncName) {
  return fmCanUseAsSqlWindowAgg(pFuncName) ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_PARA;
}

int32_t winCalcOutputBatchEnd(int64_t totalRows, int64_t startRow, int64_t capacity, int64_t *pEndRow) {
  if (pEndRow == NULL || totalRows <= 0 || startRow < 0 || startRow >= totalRows || capacity <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pEndRow = winMinI64(totalRows, startRow + capacity);
  return TSDB_CODE_SUCCESS;
}

int32_t winInputStoreCreate(const SSDataBlock *pTemplate, int32_t pageSize, int64_t inMemBufSize, const char *id,
                            SWindowInputStore **ppStore) {
  if (pTemplate == NULL || pageSize <= 0 || ppStore == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  *ppStore = NULL;
  if (!osTempSpaceAvailable()) {
    terrno = TSDB_CODE_NO_DISKSPACE;
    return terrno;
  }

  SWindowInputStore *pStore = taosMemoryCalloc(1, sizeof(SWindowInputStore));
  QUERY_CHECK_NULL(pStore, code, lino, _end, terrno);

  code = createDiskbasedBuf(&pStore->pBuf, pageSize, inMemBufSize, id, tsTempDir);
  QUERY_CHECK_CODE(code, lino, _end);

  pStore->pPages = taosArrayInit(4, sizeof(SWindowInputPage));
  QUERY_CHECK_NULL(pStore->pPages, code, lino, _end, terrno);

  code = createOneDataBlock(pTemplate, false, &pStore->pTemplate);
  QUERY_CHECK_CODE(code, lino, _end);
  winFuncMarkBlockMaterialized(pStore->pTemplate);

  pStore->pageSize = pageSize;
  pStore->currentPageIndexes[0] = -1;
  pStore->currentPageIndexes[1] = -1;
  *ppStore = pStore;
  return TSDB_CODE_SUCCESS;

_end:
  if (pStore != NULL) {
    winInputStoreDestroy(pStore);
  }
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

void winInputStoreDestroy(SWindowInputStore *pStore) {
  if (pStore == NULL) {
    return;
  }

  destroyDiskbasedBuf(pStore->pBuf);
  blockDataDestroy(pStore->pTemplate);
  blockDataDestroy(pStore->pPageBlocks[0]);
  blockDataDestroy(pStore->pPageBlocks[1]);
  taosArrayDestroy(pStore->pPages);
  taosMemoryFreeClear(pStore);
}

int32_t winInputStoreAppendBlock(SWindowInputStore *pStore, SSDataBlock *pBlock) {
  if (pStore == NULL || pBlock == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pBlock->info.rows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t start = 0;

  while (start < pBlock->info.rows) {
    int32_t stop = 0;
    code = blockDataSplitRows(pBlock, pBlock->info.hasVarCol, start, &stop, pStore->pageSize);
    QUERY_CHECK_CODE(code, lino, _end);

    SSDataBlock *pPageBlock = NULL;
    code = blockDataExtractBlock(pBlock, start, stop - start + 1, &pPageBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    int32_t pageId = -1;
    void   *pPage = getNewBufPage(pStore->pBuf, &pageId);
    if (pPage == NULL) {
      blockDataDestroy(pPageBlock);
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    int32_t size =
        blockDataGetSize(pPageBlock) + sizeof(int32_t) + taosArrayGetSize(pPageBlock->pDataBlock) * sizeof(int32_t);
    if (size > getBufPageSize(pStore->pBuf)) {
      releaseBufPage(pStore->pBuf, pPage);
      blockDataDestroy(pPageBlock);
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    code = blockDataToBuf(pPage, pPageBlock);
    if (code != TSDB_CODE_SUCCESS) {
      releaseBufPage(pStore->pBuf, pPage);
      blockDataDestroy(pPageBlock);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    setBufPageDirty(pPage, true);
    releaseBufPage(pStore->pBuf, pPage);

    SWindowInputPage inputPage = {.pageId = pageId, .startRow = pStore->totalRows, .rows = pPageBlock->info.rows};
    void            *p = taosArrayPush(pStore->pPages, &inputPage);
    blockDataDestroy(pPageBlock);
    QUERY_CHECK_NULL(p, code, lino, _end, terrno);

    pStore->totalRows += inputPage.rows;
    start = stop + 1;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t winInputStoreGetRows(const SWindowInputStore *pStore) { return pStore == NULL ? 0 : pStore->totalRows; }

int32_t winInputStoreGetPageCount(const SWindowInputStore *pStore) {
  return pStore == NULL || pStore->pPages == NULL ? 0 : taosArrayGetSize(pStore->pPages);
}

static int32_t winInputStoreGetBlockSlot(SWindowInputStore *pStore, int32_t pageIndex, int32_t slot,
                                         SSDataBlock **ppBlock) {
  if (pStore == NULL || ppBlock == NULL || pageIndex < 0 || pageIndex >= winInputStoreGetPageCount(pStore)) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (slot < 0 || slot >= 2) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (pStore->pPageBlocks[slot] == NULL) {
    int32_t code = createOneDataBlock(pStore->pTemplate, false, &pStore->pPageBlocks[slot]);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  if (pStore->currentPageIndexes[slot] != pageIndex) {
    SWindowInputPage *pPageInfo = taosArrayGet(pStore->pPages, pageIndex);
    if (pPageInfo == NULL) {
      return terrno;
    }

    void *pPage = getBufPage(pStore->pBuf, pPageInfo->pageId);
    if (pPage == NULL) {
      return terrno;
    }

    blockDataCleanup(pStore->pPageBlocks[slot]);
    int32_t code = blockDataFromBuf(pStore->pPageBlocks[slot], pPage);
    releaseBufPage(pStore->pBuf, pPage);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    pStore->currentPageIndexes[slot] = pageIndex;
  }

  *ppBlock = pStore->pPageBlocks[slot];
  return TSDB_CODE_SUCCESS;
}

int32_t winInputStoreGetBlock(SWindowInputStore *pStore, int32_t pageIndex, SSDataBlock **ppBlock) {
  return winInputStoreGetBlockSlot(pStore, pageIndex, 0, ppBlock);
}

static int32_t winInputStoreLocateRow(SWindowInputStore *pStore, int32_t globalRow, int32_t slot, SSDataBlock **ppBlock,
                                      int32_t *pLocalRow) {
  if (pStore == NULL || ppBlock == NULL || pLocalRow == NULL || globalRow < 0 || globalRow >= pStore->totalRows) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t pageCount = winInputStoreGetPageCount(pStore);
  int32_t left = 0;
  int32_t right = pageCount - 1;
  while (left <= right) {
    int32_t           mid = left + (right - left) / 2;
    SWindowInputPage *pPageInfo = taosArrayGet(pStore->pPages, mid);
    if (pPageInfo == NULL) {
      return terrno;
    }

    if (globalRow < pPageInfo->startRow) {
      right = mid - 1;
    } else if (globalRow >= pPageInfo->startRow + pPageInfo->rows) {
      left = mid + 1;
    } else {
      int32_t code = winInputStoreGetBlockSlot(pStore, mid, slot, ppBlock);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      *pLocalRow = globalRow - pPageInfo->startRow;
      return TSDB_CODE_SUCCESS;
    }
  }

  return TSDB_CODE_OUT_OF_RANGE;
}

static int32_t winInputStoreGetPageForRow(SWindowInputStore *pStore, int32_t globalRow, int32_t *pPageIndex,
                                          int32_t *pLocalRow) {
  if (pStore == NULL || pPageIndex == NULL || pLocalRow == NULL || globalRow < 0 || globalRow >= pStore->totalRows) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t pageCount = winInputStoreGetPageCount(pStore);
  int32_t left = 0;
  int32_t right = pageCount - 1;
  while (left <= right) {
    int32_t           mid = left + (right - left) / 2;
    SWindowInputPage *pPageInfo = taosArrayGet(pStore->pPages, mid);
    if (pPageInfo == NULL) {
      return terrno;
    }

    if (globalRow < pPageInfo->startRow) {
      right = mid - 1;
    } else if (globalRow >= pPageInfo->startRow + pPageInfo->rows) {
      left = mid + 1;
    } else {
      *pPageIndex = mid;
      *pLocalRow = globalRow - pPageInfo->startRow;
      return TSDB_CODE_SUCCESS;
    }
  }

  return TSDB_CODE_OUT_OF_RANGE;
}

SDiskbasedBufStatis winInputStoreGetStatis(const SWindowInputStore *pStore) {
  SDiskbasedBufStatis statis = {0};
  if (pStore != NULL && pStore->pBuf != NULL) {
    statis = getDBufStatis(pStore->pBuf);
  }
  return statis;
}

static void destroyWindowFuncOperatorInfo(void *param) {
  SWindowFuncOperatorInfo *pInfo = (SWindowFuncOperatorInfo *)param;
  if (pInfo == NULL) {
    return;
  }

  cleanupBasicInfo(&pInfo->binfo);
  winInputStoreDestroy(pInfo->pInputStore);
  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSup);
  cleanupExprSupp(&pInfo->funcSupp);
  nodesDestroyList(pInfo->pAggFuncs);
  taosMemoryFreeClear(pInfo->pAggRow);
  taosMemoryFreeClear(pInfo->pOutputSrcSlots);
  taosMemoryFreeClear(param);
}

static bool winFuncIsResultSlot(const SWindowFuncOperatorInfo *pInfo, int32_t slotId) {
  for (int32_t i = 0; i < pInfo->funcSupp.numOfExprs; ++i) {
    if (pInfo->funcSupp.pExprInfo[i].base.resSchema.slotId == slotId) {
      return true;
    }
  }
  for (int32_t i = 0; i < pInfo->scalarSup.numOfExprs; ++i) {
    if (pInfo->scalarSup.pExprInfo[i].base.resSchema.slotId == slotId) {
      return true;
    }
  }
  return false;
}

static bool winFuncSlotDescEqual(const SSlotDescNode *pLeft, const SSlotDescNode *pRight) {
  return pLeft->dataType.type == pRight->dataType.type && pLeft->dataType.bytes == pRight->dataType.bytes &&
         pLeft->dataType.precision == pRight->dataType.precision && pLeft->dataType.scale == pRight->dataType.scale &&
         0 == strcmp(pLeft->name, pRight->name);
}

static int32_t winFuncInitOutputSrcSlots(SWindowFuncOperatorInfo *pInfo, const SDataBlockDescNode *pOutputDesc,
                                         const SDataBlockDescNode *pInputDesc) {
  if (pInfo == NULL || pOutputDesc == NULL || pInputDesc == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t outputCols = LIST_LENGTH(pOutputDesc->pSlots);
  int32_t inputCols = LIST_LENGTH(pInputDesc->pSlots);

  pInfo->pOutputSrcSlots = taosMemoryMalloc(sizeof(int32_t) * outputCols);
  if (pInfo->pOutputSrcSlots == NULL) {
    return terrno;
  }
  pInfo->numOfOutputSrcSlots = outputCols;

  for (int32_t slotId = 0; slotId < outputCols; ++slotId) {
    pInfo->pOutputSrcSlots[slotId] = -1;
  }

  for (int32_t dstSlot = 0; dstSlot < outputCols; ++dstSlot) {
    if (winFuncIsResultSlot(pInfo, dstSlot)) {
      continue;
    }

    if (dstSlot < inputCols) {
      pInfo->pOutputSrcSlots[dstSlot] = dstSlot;
      continue;
    }

    SSlotDescNode *pOutputSlot = (SSlotDescNode *)nodesListGetNode(pOutputDesc->pSlots, dstSlot);
    if (pOutputSlot == NULL) {
      return terrno;
    }

    for (int32_t srcSlot = 0; srcSlot < inputCols; ++srcSlot) {
      SSlotDescNode *pInputSlot = (SSlotDescNode *)nodesListGetNode(pInputDesc->pSlots, srcSlot);
      if (pInputSlot == NULL) {
        return terrno;
      }
      if (winFuncSlotDescEqual(pOutputSlot, pInputSlot)) {
        pInfo->pOutputSrcSlots[dstSlot] = srcSlot;
        break;
      }
    }

    if (pInfo->pOutputSrcSlots[dstSlot] < 0) {
      return TSDB_CODE_OUT_OF_RANGE;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static bool winFuncIsSqlWindowAggExpr(const SNode *pNode) {
  if (pNode == NULL || nodeType(pNode) != QUERY_NODE_TARGET) {
    return false;
  }

  const STargetNode *pTarget = (const STargetNode *)pNode;
  if (pTarget->pExpr == NULL || nodeType(pTarget->pExpr) != QUERY_NODE_FUNCTION) {
    return false;
  }

  const SFunctionNode *pFunc = (const SFunctionNode *)pTarget->pExpr;
  return fmCanUseAsSqlWindowAgg(pFunc->functionName);
}

static int32_t winFuncCloneAggFuncs(const SNodeList *pFuncs, SNodeList **ppAggFuncs) {
  if (pFuncs == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SNode  *pNode = NULL;
  FOREACH(pNode, pFuncs) {
    if (!winFuncIsSqlWindowAggExpr(pNode)) {
      continue;
    }

    SNode *pNew = NULL;
    code = nodesCloneNode(pNode, &pNew);
    if (code == TSDB_CODE_SUCCESS) {
      code = nodesListMakeStrictAppend(ppAggFuncs, pNew);
    }
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t winFuncCopyOutputColumns(SSDataBlock *pDst, SWindowInputStore *pStore, int32_t srcRow, int32_t dstRow,
                                        const SWindowFuncOperatorInfo *pInfo) {
  SSDataBlock *pSrc = NULL;
  int32_t      localRow = 0;
  int32_t      code = winInputStoreLocateRow(pStore, srcRow, 0, &pSrc, &localRow);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int32_t outputCols = taosArrayGetSize(pDst->pDataBlock);
  int32_t inputCols = taosArrayGetSize(pSrc->pDataBlock);

  for (int32_t slotId = 0; slotId < outputCols; ++slotId) {
    if (winFuncIsResultSlot(pInfo, slotId)) {
      continue;
    }

    int32_t srcSlotId = -1;
    if (pInfo->pOutputSrcSlots != NULL && slotId < pInfo->numOfOutputSrcSlots) {
      srcSlotId = pInfo->pOutputSrcSlots[slotId];
    } else {
      srcSlotId = slotId;
    }
    if (srcSlotId < 0 || srcSlotId >= inputCols) {
      return TSDB_CODE_OUT_OF_RANGE;
    }

    SColumnInfoData *pSrcCol = taosArrayGet(pSrc->pDataBlock, srcSlotId);
    SColumnInfoData *pDstCol = taosArrayGet(pDst->pDataBlock, slotId);
    if (pSrcCol == NULL || pDstCol == NULL) {
      return TSDB_CODE_OUT_OF_RANGE;
    }

    bool  isNull = colDataIsNull(pSrcCol, pSrc->info.rows, localRow, NULL);
    char *pData = isNull ? NULL : colDataGetData(pSrcCol, localRow);
    code = colDataSetVal(pDstCol, dstRow, pData, isNull);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t winFuncRowsSamePartition(SWindowInputStore *pStore, const SNodeList *pPartitionKeys, int32_t lhs,
                                        int32_t rhs, bool *pSame) {
  *pSame = false;
  if (pPartitionKeys == NULL || LIST_LENGTH(pPartitionKeys) == 0) {
    *pSame = true;
    return TSDB_CODE_SUCCESS;
  }

  SSDataBlock *pLhsBlock = NULL;
  SSDataBlock *pRhsBlock = NULL;
  int32_t      lhsRow = 0;
  int32_t      rhsRow = 0;
  int32_t      code = winInputStoreLocateRow(pStore, lhs, 0, &pLhsBlock, &lhsRow);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = winInputStoreLocateRow(pStore, rhs, 1, &pRhsBlock, &rhsRow);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SNode *pNode = NULL;
  FOREACH(pNode, pPartitionKeys) {
    SNode *pExpr = (nodeType(pNode) == QUERY_NODE_TARGET) ? ((STargetNode *)pNode)->pExpr : pNode;
    if (nodeType(pExpr) != QUERY_NODE_COLUMN) {
      return TSDB_CODE_INVALID_PARA;
    }

    int32_t          slotId = ((SColumnNode *)pExpr)->slotId;
    SColumnInfoData *pLhsCol = taosArrayGet(pLhsBlock->pDataBlock, slotId);
    SColumnInfoData *pRhsCol = taosArrayGet(pRhsBlock->pDataBlock, slotId);
    if (pLhsCol == NULL || pRhsCol == NULL) {
      return TSDB_CODE_OUT_OF_RANGE;
    }

    bool lhsNull = colDataIsNull(pLhsCol, pLhsBlock->info.rows, lhsRow, NULL);
    bool rhsNull = colDataIsNull(pRhsCol, pRhsBlock->info.rows, rhsRow, NULL);
    if (lhsNull != rhsNull) {
      return TSDB_CODE_SUCCESS;
    }
    if (lhsNull) {
      continue;
    }

    char *lhsData = colDataGetData(pLhsCol, lhsRow);
    char *rhsData = colDataGetData(pRhsCol, rhsRow);
    if (IS_VAR_DATA_TYPE(pLhsCol->info.type)) {
      if (varDataTLen(lhsData) != varDataTLen(rhsData) || memcmp(lhsData, rhsData, varDataTLen(lhsData)) != 0) {
        return TSDB_CODE_SUCCESS;
      }
    } else if (memcmp(lhsData, rhsData, pLhsCol->info.bytes) != 0) {
      return TSDB_CODE_SUCCESS;
    }
  }

  *pSame = true;
  return TSDB_CODE_SUCCESS;
}

static int32_t winFuncReadOrderValueAsI64(SColumnInfoData *pCol, int32_t totalRows, int32_t row, int64_t *pValue,
                                          bool *pIsNull) {
  if (colDataIsNull(pCol, totalRows, row, NULL)) {
    *pIsNull = true;
    return TSDB_CODE_SUCCESS;
  }
  *pIsNull = false;

  char *pData = colDataGetData(pCol, row);
  switch (pCol->info.type) {
    case TSDB_DATA_TYPE_TINYINT:
      *pValue = *(int8_t *)pData;
      return TSDB_CODE_SUCCESS;
    case TSDB_DATA_TYPE_SMALLINT:
      *pValue = *(int16_t *)pData;
      return TSDB_CODE_SUCCESS;
    case TSDB_DATA_TYPE_INT:
      *pValue = *(int32_t *)pData;
      return TSDB_CODE_SUCCESS;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      *pValue = *(int64_t *)pData;
      return TSDB_CODE_SUCCESS;
    case TSDB_DATA_TYPE_UTINYINT:
      *pValue = *(uint8_t *)pData;
      return TSDB_CODE_SUCCESS;
    case TSDB_DATA_TYPE_USMALLINT:
      *pValue = *(uint16_t *)pData;
      return TSDB_CODE_SUCCESS;
    case TSDB_DATA_TYPE_UINT:
      *pValue = *(uint32_t *)pData;
      return TSDB_CODE_SUCCESS;
    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t value = *(uint64_t *)pData;
      if (value > INT64_MAX) {
        return TSDB_CODE_INVALID_PARA;
      }
      *pValue = value;
      return TSDB_CODE_SUCCESS;
    }
    default:
      return TSDB_CODE_INVALID_PARA;
  }
}

static int32_t winFuncReadOrderValueAsDouble(SColumnInfoData *pCol, int32_t totalRows, int32_t row, double *pValue,
                                             bool *pIsNull) {
  if (colDataIsNull(pCol, totalRows, row, NULL)) {
    *pIsNull = true;
    return TSDB_CODE_SUCCESS;
  }
  *pIsNull = false;

  char *pData = colDataGetData(pCol, row);
  switch (pCol->info.type) {
    case TSDB_DATA_TYPE_FLOAT:
      *pValue = *(float *)pData;
      return TSDB_CODE_SUCCESS;
    case TSDB_DATA_TYPE_DOUBLE:
      *pValue = *(double *)pData;
      return TSDB_CODE_SUCCESS;
    default:
      return TSDB_CODE_INVALID_PARA;
  }
}

static int32_t winFuncGetColumnByRow(SWindowInputStore *pStore, int32_t globalRow, int32_t slot, int32_t colSlot,
                                     SSDataBlock **ppBlock, SColumnInfoData **ppCol, int32_t *pLocalRow) {
  int32_t code = winInputStoreLocateRow(pStore, globalRow, slot, ppBlock, pLocalRow);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  *ppCol = taosArrayGet((*ppBlock)->pDataBlock, colSlot);
  return *ppCol != NULL ? TSDB_CODE_SUCCESS : TSDB_CODE_OUT_OF_RANGE;
}

static int32_t winFuncReadOrderValueAsI64BySlot(SWindowInputStore *pStore, int32_t orderSlot, int32_t row,
                                                int64_t *pValue, bool *pIsNull) {
  SSDataBlock     *pBlock = NULL;
  SColumnInfoData *pCol = NULL;
  int32_t          localRow = 0;
  int32_t          code = winFuncGetColumnByRow(pStore, row, 0, orderSlot, &pBlock, &pCol, &localRow);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  return winFuncReadOrderValueAsI64(pCol, pBlock->info.rows, localRow, pValue, pIsNull);
}

static int32_t winFuncReadOrderValueAsDoubleBySlot(SWindowInputStore *pStore, int32_t orderSlot, int32_t row,
                                                   double *pValue, bool *pIsNull) {
  SSDataBlock     *pBlock = NULL;
  SColumnInfoData *pCol = NULL;
  int32_t          localRow = 0;
  int32_t          code = winFuncGetColumnByRow(pStore, row, 0, orderSlot, &pBlock, &pCol, &localRow);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  return winFuncReadOrderValueAsDouble(pCol, pBlock->info.rows, localRow, pValue, pIsNull);
}

static int32_t winFuncGetRangeOrderSlot(SWindowInputStore *pStore, const SNodeList *pOrderKeys, int32_t *pSlot,
                                        int32_t *pType, int32_t *pOrder, ENullOrder *pNullOrder) {
  if (pStore == NULL || pOrderKeys == NULL || LIST_LENGTH(pOrderKeys) != 1 || pSlot == NULL || pType == NULL ||
      pOrder == NULL || pNullOrder == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SNode *pNode = nodesListGetNode((SNodeList *)pOrderKeys, 0);
  if (pNode == NULL || nodeType(pNode) != QUERY_NODE_ORDER_BY_EXPR) {
    return TSDB_CODE_INVALID_PARA;
  }

  SOrderByExprNode *pOrderExpr = (SOrderByExprNode *)pNode;
  if ((pOrderExpr->order != ORDER_ASC && pOrderExpr->order != ORDER_DESC) || pOrderExpr->pExpr == NULL ||
      nodeType(pOrderExpr->pExpr) != QUERY_NODE_COLUMN) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t          slotId = ((SColumnNode *)pOrderExpr->pExpr)->slotId;
  SColumnInfoData *pCol = taosArrayGet(pStore->pTemplate->pDataBlock, slotId);
  if (pCol == NULL) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  *pSlot = slotId;
  *pType = pCol->info.type;
  *pOrder = pOrderExpr->order == ORDER_ASC ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
  *pNullOrder = pOrderExpr->nullOrder == NULL_ORDER_DEFAULT
                    ? (pOrderExpr->order == ORDER_ASC ? NULL_ORDER_FIRST : NULL_ORDER_LAST)
                    : pOrderExpr->nullOrder;
  return TSDB_CODE_SUCCESS;
}

static bool winFuncRangeHasOffset(const SWindowFrameNode *pFrame) {
  return pFrame->start.boundType == WINDOW_BOUND_N_PRECEDING || pFrame->start.boundType == WINDOW_BOUND_N_FOLLOWING ||
         pFrame->end.boundType == WINDOW_BOUND_N_PRECEDING || pFrame->end.boundType == WINDOW_BOUND_N_FOLLOWING;
}

static void winFuncSetEmptyRange(SSqlWindowFrameRange *pRange) {
  pRange->start = 0;
  pRange->end = -1;
}

static int32_t winFuncFindNullEdgeRange(SWindowInputStore *pStore, int32_t orderSlot, int32_t partitionStart,
                                        int32_t partitionEnd, bool nullFirst, SSqlWindowFrameRange *pRange) {
  winFuncSetEmptyRange(pRange);

  if (nullFirst) {
    for (int32_t i = partitionStart; i < partitionEnd; ++i) {
      SSDataBlock     *pBlock = NULL;
      SColumnInfoData *pCol = NULL;
      int32_t          localRow = 0;
      int32_t          code = winFuncGetColumnByRow(pStore, i, 0, orderSlot, &pBlock, &pCol, &localRow);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      if (!colDataIsNull(pCol, pBlock->info.rows, localRow, NULL)) {
        break;
      }
      int64_t frameRow = i - partitionStart;
      if (pRange->start > pRange->end) {
        pRange->start = frameRow;
      }
      pRange->end = frameRow;
    }
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = partitionEnd - 1; i >= partitionStart; --i) {
    SSDataBlock     *pBlock = NULL;
    SColumnInfoData *pCol = NULL;
    int32_t          localRow = 0;
    int32_t          code = winFuncGetColumnByRow(pStore, i, 0, orderSlot, &pBlock, &pCol, &localRow);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (!colDataIsNull(pCol, pBlock->info.rows, localRow, NULL)) {
      break;
    }
    int64_t frameRow = i - partitionStart;
    pRange->start = frameRow;
    if (pRange->start > pRange->end) {
      pRange->end = frameRow;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t winFuncFindNanPeerRange(SWindowInputStore *pStore, int32_t orderSlot, int32_t partitionStart,
                                       int32_t partitionEnd, int32_t row, SSqlWindowFrameRange *pRange) {
  double  value = 0;
  bool    valueNull = false;
  int32_t code = winFuncReadOrderValueAsDoubleBySlot(pStore, orderSlot, row, &value, &valueNull);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (valueNull || !isnan(value)) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t peerStart = row;
  while (peerStart > partitionStart) {
    code = winFuncReadOrderValueAsDoubleBySlot(pStore, orderSlot, peerStart - 1, &value, &valueNull);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (valueNull || !isnan(value)) {
      break;
    }
    --peerStart;
  }

  int32_t peerEnd = row;
  while (peerEnd + 1 < partitionEnd) {
    code = winFuncReadOrderValueAsDoubleBySlot(pStore, orderSlot, peerEnd + 1, &value, &valueNull);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (valueNull || !isnan(value)) {
      break;
    }
    ++peerEnd;
  }

  pRange->start = peerStart - partitionStart;
  pRange->end = peerEnd - partitionStart;
  return TSDB_CODE_SUCCESS;
}

static int32_t winFuncSameOrderKeys(SWindowInputStore *pStore, const SNodeList *pOrderKeys, int32_t lhs, int32_t rhs,
                                    bool *pSame) {
  *pSame = false;
  if (pOrderKeys == NULL || LIST_LENGTH(pOrderKeys) == 0) {
    *pSame = true;
    return TSDB_CODE_SUCCESS;
  }

  SSDataBlock *pLhsBlock = NULL;
  SSDataBlock *pRhsBlock = NULL;
  int32_t      lhsRow = 0;
  int32_t      rhsRow = 0;
  int32_t      code = winInputStoreLocateRow(pStore, lhs, 0, &pLhsBlock, &lhsRow);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = winInputStoreLocateRow(pStore, rhs, 1, &pRhsBlock, &rhsRow);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SNode *pNode = NULL;
  FOREACH(pNode, pOrderKeys) {
    if (nodeType(pNode) != QUERY_NODE_ORDER_BY_EXPR) {
      return TSDB_CODE_INVALID_PARA;
    }

    SNode *pExpr = ((SOrderByExprNode *)pNode)->pExpr;
    if (nodeType(pExpr) != QUERY_NODE_COLUMN) {
      return TSDB_CODE_INVALID_PARA;
    }

    int32_t          slotId = ((SColumnNode *)pExpr)->slotId;
    SColumnInfoData *pLhsCol = taosArrayGet(pLhsBlock->pDataBlock, slotId);
    SColumnInfoData *pRhsCol = taosArrayGet(pRhsBlock->pDataBlock, slotId);
    if (pLhsCol == NULL || pRhsCol == NULL) {
      return TSDB_CODE_OUT_OF_RANGE;
    }

    bool lhsNull = colDataIsNull(pLhsCol, pLhsBlock->info.rows, lhsRow, NULL);
    bool rhsNull = colDataIsNull(pRhsCol, pRhsBlock->info.rows, rhsRow, NULL);
    if (lhsNull != rhsNull) {
      return TSDB_CODE_SUCCESS;
    }
    if (lhsNull) {
      continue;
    }

    char   *lhsData = colDataGetData(pLhsCol, lhsRow);
    char   *rhsData = colDataGetData(pRhsCol, rhsRow);
    int32_t order = (((SOrderByExprNode *)pNode)->order == ORDER_ASC) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
    if (getKeyComparFunc(pLhsCol->info.type, order)(lhsData, rhsData) != 0) {
      return TSDB_CODE_SUCCESS;
    }
  }

  *pSame = true;
  return TSDB_CODE_SUCCESS;
}

static int32_t winFuncFindPeerRange(SWindowInputStore *pStore, const SNodeList *pOrderKeys, int32_t partitionStart,
                                    int32_t partitionEnd, int32_t row, int32_t *pPeerStart, int32_t *pPeerEnd) {
  int32_t peerStart = row;
  while (peerStart > partitionStart) {
    bool    same = false;
    int32_t code = winFuncSameOrderKeys(pStore, pOrderKeys, row, peerStart - 1, &same);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (!same) {
      break;
    }
    --peerStart;
  }

  int32_t peerEnd = row;
  while (peerEnd + 1 < partitionEnd) {
    bool    same = false;
    int32_t code = winFuncSameOrderKeys(pStore, pOrderKeys, row, peerEnd + 1, &same);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (!same) {
      break;
    }
    ++peerEnd;
  }

  *pPeerStart = peerStart;
  *pPeerEnd = peerEnd;
  return TSDB_CODE_SUCCESS;
}

static int32_t winFuncCalcRangePeerFrame(SWindowFuncOperatorInfo *pInfo, SWindowFuncPhysiNode *pWindowNode,
                                         int32_t partitionStart, int32_t partitionEnd, int32_t row,
                                         SSqlWindowFrameRange *pRange) {
  if (pWindowNode->pOrderKeys == NULL || LIST_LENGTH(pWindowNode->pOrderKeys) == 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t peerStart = row;
  int32_t peerEnd = row;
  int32_t code = winFuncFindPeerRange(pInfo->pInputStore, pWindowNode->pOrderKeys, partitionStart, partitionEnd, row,
                                      &peerStart, &peerEnd);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SWindowFrameNode *pFrame = (SWindowFrameNode *)pWindowNode->pFrame;
  switch (pFrame->start.boundType) {
    case WINDOW_BOUND_UNBOUNDED_PRECEDING:
      pRange->start = 0;
      break;
    case WINDOW_BOUND_CURRENT_ROW:
      pRange->start = peerStart - partitionStart;
      break;
    default:
      return TSDB_CODE_INVALID_PARA;
  }

  switch (pFrame->end.boundType) {
    case WINDOW_BOUND_CURRENT_ROW:
      pRange->end = peerEnd - partitionStart;
      break;
    case WINDOW_BOUND_UNBOUNDED_FOLLOWING:
      pRange->end = partitionEnd - partitionStart - 1;
      break;
    default:
      return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t winFuncCalcRangeFrame(SWindowFuncOperatorInfo *pInfo, SWindowFuncPhysiNode *pWindowNode,
                                     int32_t partitionStart, int32_t partitionEnd, int32_t row,
                                     SSqlWindowFrameRange *pRange) {
  SWindowFrameNode *pFrame = (SWindowFrameNode *)pWindowNode->pFrame;
  if (!winFuncRangeHasOffset(pFrame)) {
    return winFuncCalcRangePeerFrame(pInfo, pWindowNode, partitionStart, partitionEnd, row, pRange);
  }

  int32_t    orderSlot = -1;
  int32_t    orderType = 0;
  int32_t    order = TSDB_ORDER_ASC;
  ENullOrder nullOrder = NULL_ORDER_FIRST;
  int32_t    code =
      winFuncGetRangeOrderSlot(pInfo->pInputStore, pWindowNode->pOrderKeys, &orderSlot, &orderType, &order, &nullOrder);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (nullOrder != NULL_ORDER_FIRST && nullOrder != NULL_ORDER_LAST) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (IS_FLOAT_TYPE(orderType)) {
    double current = 0;
    bool   currentNull = false;
    code = winFuncReadOrderValueAsDoubleBySlot(pInfo->pInputStore, orderSlot, row, &current, &currentNull);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    bool   lowerUnbounded = false;
    bool   upperUnbounded = false;
    double lower = current;
    double upper = current;
    double offset = 0;

    bool sqlStartUnbounded = pFrame->start.boundType == WINDOW_BOUND_UNBOUNDED_PRECEDING;
    bool sqlEndUnbounded = pFrame->end.boundType == WINDOW_BOUND_UNBOUNDED_FOLLOWING;
    if (currentNull) {
      SSqlWindowFrameRange nullPeers = {0};
      code = winFuncFindNullEdgeRange(pInfo->pInputStore, orderSlot, partitionStart, partitionEnd,
                                      nullOrder == NULL_ORDER_FIRST, &nullPeers);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      int64_t relativeRow = row - partitionStart;
      if (relativeRow < nullPeers.start || relativeRow > nullPeers.end) {
        return TSDB_CODE_INVALID_PARA;
      }

      pRange->start = sqlStartUnbounded ? 0 : nullPeers.start;
      pRange->end = sqlEndUnbounded ? partitionEnd - partitionStart - 1 : nullPeers.end;
      return TSDB_CODE_SUCCESS;
    }
    if (isnan(current)) {
      SSqlWindowFrameRange nanPeers = {0};
      code = winFuncFindNanPeerRange(pInfo->pInputStore, orderSlot, partitionStart, partitionEnd, row, &nanPeers);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      pRange->start = sqlStartUnbounded ? 0 : nanPeers.start;
      pRange->end = sqlEndUnbounded ? partitionEnd - partitionStart - 1 : nanPeers.end;
      return TSDB_CODE_SUCCESS;
    }

    switch (pFrame->start.boundType) {
      case WINDOW_BOUND_UNBOUNDED_PRECEDING:
        lowerUnbounded = true;
        break;
      case WINDOW_BOUND_N_PRECEDING:
        code = winReadRangeBoundOffsetDouble(&pFrame->start, &offset);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        lower = order == TSDB_ORDER_ASC ? current - offset : current + offset;
        break;
      case WINDOW_BOUND_CURRENT_ROW:
        lower = current;
        break;
      case WINDOW_BOUND_N_FOLLOWING:
        code = winReadRangeBoundOffsetDouble(&pFrame->start, &offset);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        lower = order == TSDB_ORDER_ASC ? current + offset : current - offset;
        break;
      default:
        return TSDB_CODE_INVALID_PARA;
    }

    switch (pFrame->end.boundType) {
      case WINDOW_BOUND_CURRENT_ROW:
        upper = current;
        break;
      case WINDOW_BOUND_N_PRECEDING:
        code = winReadRangeBoundOffsetDouble(&pFrame->end, &offset);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        upper = order == TSDB_ORDER_ASC ? current - offset : current + offset;
        break;
      case WINDOW_BOUND_N_FOLLOWING:
        code = winReadRangeBoundOffsetDouble(&pFrame->end, &offset);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        upper = order == TSDB_ORDER_ASC ? current + offset : current - offset;
        break;
      case WINDOW_BOUND_UNBOUNDED_FOLLOWING:
        upperUnbounded = true;
        break;
      default:
        return TSDB_CODE_INVALID_PARA;
    }

    pRange->start = 0;
    pRange->end = -1;
    if (order == TSDB_ORDER_DESC) {
      TSWAP(lower, upper);
      TSWAP(lowerUnbounded, upperUnbounded);
    }
    if (!lowerUnbounded && !upperUnbounded && lower > upper) {
      return TSDB_CODE_SUCCESS;
    }

    for (int32_t i = partitionStart; i < partitionEnd; ++i) {
      double value = 0;
      bool   valueNull = false;
      code = winFuncReadOrderValueAsDoubleBySlot(pInfo->pInputStore, orderSlot, i, &value, &valueNull);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      if (valueNull || isnan(value)) {
        continue;
      }

      if ((!lowerUnbounded && value < lower) || (!upperUnbounded && value > upper)) {
        continue;
      }

      int64_t frameRow = i - partitionStart;
      if (pRange->start > pRange->end) {
        pRange->start = frameRow;
      }
      pRange->end = frameRow;
    }
    if (sqlStartUnbounded && nullOrder == NULL_ORDER_FIRST) {
      if (pRange->start > pRange->end) {
        code = winFuncFindNullEdgeRange(pInfo->pInputStore, orderSlot, partitionStart, partitionEnd, true, pRange);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      } else {
        pRange->start = 0;
      }
    }
    if (sqlEndUnbounded && nullOrder == NULL_ORDER_LAST) {
      if (pRange->start > pRange->end) {
        code = winFuncFindNullEdgeRange(pInfo->pInputStore, orderSlot, partitionStart, partitionEnd, false, pRange);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      } else {
        pRange->end = partitionEnd - partitionStart - 1;
      }
    }

    return TSDB_CODE_SUCCESS;
  }

  int64_t current = 0;
  bool    currentNull = false;
  code = winFuncReadOrderValueAsI64BySlot(pInfo->pInputStore, orderSlot, row, &current, &currentNull);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  bool    lowerUnbounded = false;
  bool    upperUnbounded = false;
  int64_t lower = current;
  int64_t upper = current;
  int64_t offset = 0;

  bool sqlStartUnbounded = pFrame->start.boundType == WINDOW_BOUND_UNBOUNDED_PRECEDING;
  bool sqlEndUnbounded = pFrame->end.boundType == WINDOW_BOUND_UNBOUNDED_FOLLOWING;
  if (currentNull) {
    SSqlWindowFrameRange nullPeers = {0};
    code = winFuncFindNullEdgeRange(pInfo->pInputStore, orderSlot, partitionStart, partitionEnd,
                                    nullOrder == NULL_ORDER_FIRST, &nullPeers);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    int64_t relativeRow = row - partitionStart;
    if (relativeRow < nullPeers.start || relativeRow > nullPeers.end) {
      return TSDB_CODE_INVALID_PARA;
    }

    pRange->start = sqlStartUnbounded ? 0 : nullPeers.start;
    pRange->end = sqlEndUnbounded ? partitionEnd - partitionStart - 1 : nullPeers.end;
    return TSDB_CODE_SUCCESS;
  }

  switch (pFrame->start.boundType) {
    case WINDOW_BOUND_UNBOUNDED_PRECEDING:
      lowerUnbounded = true;
      break;
    case WINDOW_BOUND_N_PRECEDING:
      code = winReadRangeBoundOffset(&pFrame->start, &offset);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      lower = order == TSDB_ORDER_ASC ? winSubI64Saturating(current, offset) : winAddI64Saturating(current, offset);
      break;
    case WINDOW_BOUND_CURRENT_ROW:
      lower = current;
      break;
    case WINDOW_BOUND_N_FOLLOWING:
      code = winReadRangeBoundOffset(&pFrame->start, &offset);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      lower = order == TSDB_ORDER_ASC ? winAddI64Saturating(current, offset) : winSubI64Saturating(current, offset);
      break;
    default:
      return TSDB_CODE_INVALID_PARA;
  }

  switch (pFrame->end.boundType) {
    case WINDOW_BOUND_CURRENT_ROW:
      upper = current;
      break;
    case WINDOW_BOUND_N_PRECEDING:
      code = winReadRangeBoundOffset(&pFrame->end, &offset);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      upper = order == TSDB_ORDER_ASC ? winSubI64Saturating(current, offset) : winAddI64Saturating(current, offset);
      break;
    case WINDOW_BOUND_N_FOLLOWING:
      code = winReadRangeBoundOffset(&pFrame->end, &offset);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      upper = order == TSDB_ORDER_ASC ? winAddI64Saturating(current, offset) : winSubI64Saturating(current, offset);
      break;
    case WINDOW_BOUND_UNBOUNDED_FOLLOWING:
      upperUnbounded = true;
      break;
    default:
      return TSDB_CODE_INVALID_PARA;
  }

  pRange->start = 0;
  pRange->end = -1;
  if (order == TSDB_ORDER_DESC) {
    TSWAP(lower, upper);
    TSWAP(lowerUnbounded, upperUnbounded);
  }
  if (!lowerUnbounded && !upperUnbounded && lower > upper) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = partitionStart; i < partitionEnd; ++i) {
    int64_t value = 0;
    bool    valueNull = false;
    code = winFuncReadOrderValueAsI64BySlot(pInfo->pInputStore, orderSlot, i, &value, &valueNull);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (valueNull) {
      continue;
    }

    if ((!lowerUnbounded && value < lower) || (!upperUnbounded && value > upper)) {
      continue;
    }

    int64_t frameRow = i - partitionStart;
    if (pRange->start > pRange->end) {
      pRange->start = frameRow;
    }
    pRange->end = frameRow;
  }
  if (sqlStartUnbounded && nullOrder == NULL_ORDER_FIRST) {
    if (pRange->start > pRange->end) {
      code = winFuncFindNullEdgeRange(pInfo->pInputStore, orderSlot, partitionStart, partitionEnd, true, pRange);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    } else {
      pRange->start = 0;
    }
  }
  if (sqlEndUnbounded && nullOrder == NULL_ORDER_LAST) {
    if (pRange->start > pRange->end) {
      code = winFuncFindNullEdgeRange(pInfo->pInputStore, orderSlot, partitionStart, partitionEnd, false, pRange);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    } else {
      pRange->end = partitionEnd - partitionStart - 1;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t winFuncCalcFrame(SWindowFuncOperatorInfo *pInfo, SWindowFuncPhysiNode *pWindowNode,
                                int32_t partitionStart, int32_t partitionEnd, int32_t row,
                                SSqlWindowFrameRange *pRange) {
  SWindowFrameNode *pFrame = (SWindowFrameNode *)pWindowNode->pFrame;
  int64_t           partitionRows = partitionEnd - partitionStart;

  if (pFrame->frameUnit == WINDOW_FRAME_UNIT_ROWS) {
    return winCalcRowsFrame(row - partitionStart, partitionRows, pFrame, pRange);
  }
  if (pFrame->frameUnit == WINDOW_FRAME_UNIT_RANGE) {
    return winFuncCalcRangeFrame(pInfo, pWindowNode, partitionStart, partitionEnd, row, pRange);
  }
  return TSDB_CODE_INVALID_PARA;
}

static void winFuncNormalizeAggResultRows(SOperatorInfo *pOperator, SResultRow *pRow) {
  for (int32_t i = 0; i < pOperator->exprSupp.numOfExprs; ++i) {
    SResultRowEntryInfo *pEntryInfo = getResultEntryInfo(pRow, i, pOperator->exprSupp.rowEntryInfoOffset);
    if (pEntryInfo->numOfRes > 1) {
      pEntryInfo->numOfRes = 1;
    }
  }
  pRow->numOfRows = pRow->numOfRows > 0 ? 1 : 0;
}

static void winFuncCleanupAggRow(SOperatorInfo *pOperator, SResultRow *pRow) {
  for (int32_t i = 0; i < pOperator->exprSupp.numOfExprs; ++i) {
    SqlFunctionCtx *pCtx = &pOperator->exprSupp.pCtx[i];
    pCtx->resultInfo = getResultEntryInfo(pRow, i, pOperator->exprSupp.rowEntryInfoOffset);
    if (pCtx->needCleanup && pCtx->fpSet.cleanup != NULL) {
      pCtx->fpSet.cleanup(pCtx);
      pCtx->needCleanup = false;
    }
  }
}

static int32_t winFuncApplyAggOnFramePages(SOperatorInfo *pOperator, SWindowFuncOperatorInfo *pInfo, int32_t start,
                                           int32_t end, int32_t scanFlag) {
  if (start > end) {
    if (pInfo->pInputStore == NULL || winInputStoreGetPageCount(pInfo->pInputStore) == 0) {
      return TSDB_CODE_SUCCESS;
    }

    SSDataBlock *pBlock = NULL;
    int32_t      code = winInputStoreGetBlockSlot(pInfo->pInputStore, 0, 0, &pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    code = setInputDataBlock(&pOperator->exprSupp, pBlock, pInfo->binfo.inputTsOrder, scanFlag, true);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    return applyAggFunctionOnPartialTuples(pOperator->pTaskInfo, pOperator->exprSupp.pCtx, NULL, 0, 0,
                                           pBlock->info.rows, pOperator->exprSupp.numOfExprs);
  }

  int32_t row = start;
  while (row <= end) {
    int32_t pageIndex = -1;
    int32_t localRow = 0;
    int32_t code = winInputStoreGetPageForRow(pInfo->pInputStore, row, &pageIndex, &localRow);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    SSDataBlock *pBlock = NULL;
    code = winInputStoreGetBlockSlot(pInfo->pInputStore, pageIndex, 0, &pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    int32_t rows = TMIN(end - row + 1, pBlock->info.rows - localRow);
    code = setInputDataBlock(&pOperator->exprSupp, pBlock, pInfo->binfo.inputTsOrder, scanFlag, true);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    code = applyAggFunctionOnPartialTuples(pOperator->pTaskInfo, pOperator->exprSupp.pCtx, NULL, localRow, rows,
                                           pBlock->info.rows, pOperator->exprSupp.numOfExprs);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    row += rows;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t winFuncCalcOneFrame(SOperatorInfo *pOperator, SWindowFuncOperatorInfo *pInfo, SSDataBlock *pRes,
                                   int32_t start, int32_t end) {
  if (pOperator->exprSupp.numOfExprs == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SResultRow *pRow = pInfo->pAggRow;
  QUERY_CHECK_NULL(pRow, code, lino, _end, terrno);
  winFuncCleanupAggRow(pOperator, pRow);
  resetResultRow(pRow, pInfo->aggSup.resultRowSize - sizeof(SResultRow));

  code = setResultRowInitCtx(pRow, pOperator->exprSupp.pCtx, pOperator->exprSupp.numOfExprs,
                             pOperator->exprSupp.rowEntryInfoOffset);
  QUERY_CHECK_CODE(code, lino, _end);

  bool hasRepeatScanFunc = false;
  for (int32_t i = 0; i < pOperator->exprSupp.numOfExprs; ++i) {
    if (fmIsRepeatScanFunc(pOperator->exprSupp.pCtx[i].functionId)) {
      hasRepeatScanFunc = true;
      break;
    }
  }
  if (hasRepeatScanFunc) {
    code = winFuncApplyAggOnFramePages(pOperator, pInfo, start, end, PRE_SCAN);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  code = winFuncApplyAggOnFramePages(pOperator, pInfo, start, end, MAIN_SCAN);
  QUERY_CHECK_CODE(code, lino, _end);

  doUpdateNumOfRows(pOperator->exprSupp.pCtx, pRow, pOperator->exprSupp.numOfExprs,
                    pOperator->exprSupp.rowEntryInfoOffset);
  winFuncNormalizeAggResultRows(pOperator, pRow);
  code = copyResultrowToDataBlock(pOperator->exprSupp.pExprInfo, pOperator->exprSupp.numOfExprs, pRow,
                                  pOperator->exprSupp.pCtx, pRes, pOperator->exprSupp.rowEntryInfoOffset,
                                  pOperator->pTaskInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  pRes->info.rows += 1;
  clearResultRowInitFlag(pOperator->exprSupp.pCtx, pOperator->exprSupp.numOfExprs);

_end:
  return code;
}

static int64_t winFuncValueParamI64(const SExprInfo *pExpr, int32_t paramIdx, int64_t defaultVal) {
  if (pExpr->base.numOfParams <= paramIdx || pExpr->base.pParam[paramIdx].type != FUNC_PARAM_TYPE_VALUE) {
    return defaultVal;
  }

  SVariant *pParam = &pExpr->base.pParam[paramIdx].param;
  if (IS_SIGNED_NUMERIC_TYPE(pParam->nType)) {
    return pParam->i;
  }
  if (IS_UNSIGNED_NUMERIC_TYPE(pParam->nType)) {
    return pParam->u;
  }
  return defaultVal;
}

static int32_t winFuncSetNull(SSDataBlock *pRes, const SExprInfo *pExpr, int32_t dstRow) {
  SColumnInfoData *pDstCol = taosArrayGet(pRes->pDataBlock, pExpr->base.resSchema.slotId);
  if (pDstCol == NULL) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  return colDataSetVal(pDstCol, dstRow, NULL, true);
}

static int32_t winFuncCopyValue(SSDataBlock *pRes, const SExprInfo *pExpr, SWindowInputStore *pStore, int32_t srcRow,
                                int32_t dstRow) {
  if (pExpr->base.numOfParams < 1 || pExpr->base.pParam[0].type != FUNC_PARAM_TYPE_COLUMN) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t      srcSlot = pExpr->base.pParam[0].pCol->slotId;
  SSDataBlock *pInput = NULL;
  int32_t      localRow = 0;
  int32_t      code = winInputStoreLocateRow(pStore, srcRow, 0, &pInput, &localRow);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SColumnInfoData *pSrcCol = taosArrayGet(pInput->pDataBlock, srcSlot);
  SColumnInfoData *pDstCol = taosArrayGet(pRes->pDataBlock, pExpr->base.resSchema.slotId);
  if (pSrcCol == NULL || pDstCol == NULL) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  bool  isNull = colDataIsNull(pSrcCol, pInput->info.rows, localRow, NULL);
  char *pData = isNull ? NULL : colDataGetData(pSrcCol, localRow);
  return colDataSetVal(pDstCol, dstRow, pData, isNull);
}

static int32_t winFuncSetDefaultOrNull(SSDataBlock *pRes, const SExprInfo *pExpr, int32_t dstRow) {
  if (pExpr->base.numOfParams < 3 || pExpr->base.pParam[2].type != FUNC_PARAM_TYPE_VALUE) {
    return winFuncSetNull(pRes, pExpr, dstRow);
  }

  SColumnInfoData *pDstCol = taosArrayGet(pRes->pDataBlock, pExpr->base.resSchema.slotId);
  if (pDstCol == NULL) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  SFunctParam *pDefaultParam = &pExpr->base.pParam[2];
  SVariant    *pDefault = &pDefaultParam->param;
  if (pDefault->nType == TSDB_DATA_TYPE_NULL) {
    return colDataSetVal(pDstCol, dstRow, NULL, true);
  }
  return setLagLeadDefaultValueToCol(pDstCol, dstRow, pDefaultParam);
}

static int32_t winFuncSetBigint(SSDataBlock *pRes, const SExprInfo *pExpr, int32_t dstRow, int64_t value) {
  SColumnInfoData *pDstCol = taosArrayGet(pRes->pDataBlock, pExpr->base.resSchema.slotId);
  if (pDstCol == NULL) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  return colDataSetVal(pDstCol, dstRow, (const char *)&value, false);
}

static int32_t winFuncSetDouble(SSDataBlock *pRes, const SExprInfo *pExpr, int32_t dstRow, double value) {
  SColumnInfoData *pDstCol = taosArrayGet(pRes->pDataBlock, pExpr->base.resSchema.slotId);
  if (pDstCol == NULL) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  return colDataSetVal(pDstCol, dstRow, (const char *)&value, false);
}

static int32_t winFuncCalcDedicated(SWindowFuncOperatorInfo *pInfo, SWindowFuncPhysiNode *pWindowNode,
                                    SSDataBlock *pRes, int32_t partitionStart, int32_t partitionEnd, int32_t row,
                                    int32_t dstRow, const SSqlWindowFrameRange *pRange, int64_t peerStart,
                                    int64_t peerEnd, int64_t denseRank) {
  (void)pWindowNode;
  int32_t partitionRows = partitionEnd - partitionStart;
  int32_t rowIndex = row - partitionStart;

  for (int32_t i = 0; i < pInfo->funcSupp.numOfExprs; ++i) {
    SExprInfo    *pExpr = &pInfo->funcSupp.pExprInfo[i];
    int32_t       funcId = pExpr->pExpr->_function.functionId;
    EFunctionType funcType = fmGetFuncTypeFromId(funcId);
    const char   *pFuncName = pExpr->pExpr->_function.functionName;
    int32_t       code = TSDB_CODE_SUCCESS;

    switch (funcType) {
      case FUNCTION_TYPE_ROW_NUMBER: {
        int64_t value = rowIndex + 1;
        code = winFuncSetBigint(pRes, pExpr, dstRow, value);
        break;
      }
      case FUNCTION_TYPE_RANK: {
        int64_t value = 0;
        code = winCalcRankValue(rowIndex, peerStart, denseRank, &value);
        if (code == TSDB_CODE_SUCCESS) {
          code = winFuncSetBigint(pRes, pExpr, dstRow, value);
        }
        break;
      }
      case FUNCTION_TYPE_DENSE_RANK:
        code = winFuncSetBigint(pRes, pExpr, dstRow, denseRank);
        break;
      case FUNCTION_TYPE_PERCENT_RANK: {
        double value = 0;
        code = winCalcPercentRank(peerStart + 1, partitionRows, &value);
        if (code == TSDB_CODE_SUCCESS) {
          code = winFuncSetDouble(pRes, pExpr, dstRow, value);
        }
        break;
      }
      case FUNCTION_TYPE_CUME_DIST: {
        double value = 0;
        code = winCalcCumeDist(peerEnd, partitionRows, &value);
        if (code == TSDB_CODE_SUCCESS) {
          code = winFuncSetDouble(pRes, pExpr, dstRow, value);
        }
        break;
      }
      case FUNCTION_TYPE_LAG: {
        int64_t offset = winFuncValueParamI64(pExpr, 1, 1);
        int64_t target = (int64_t)row - offset;
        code = (target >= partitionStart && target < partitionEnd)
                   ? winFuncCopyValue(pRes, pExpr, pInfo->pInputStore, (int32_t)target, dstRow)
                   : winFuncSetDefaultOrNull(pRes, pExpr, dstRow);
        break;
      }
      case FUNCTION_TYPE_LEAD: {
        int64_t offset = winFuncValueParamI64(pExpr, 1, 1);
        int64_t remainingRows = (int64_t)partitionEnd - row - 1;
        if (offset <= remainingRows) {
          int64_t target = (int64_t)row + offset;
          code = winFuncCopyValue(pRes, pExpr, pInfo->pInputStore, (int32_t)target, dstRow);
        } else {
          code = winFuncSetDefaultOrNull(pRes, pExpr, dstRow);
        }
        break;
      }
      case FUNCTION_TYPE_FIRST_VALUE:
        code = pRange->start <= pRange->end
                   ? winFuncCopyValue(pRes, pExpr, pInfo->pInputStore, partitionStart + pRange->start, dstRow)
                   : winFuncSetNull(pRes, pExpr, dstRow);
        break;
      case FUNCTION_TYPE_LAST_VALUE:
        code = pRange->start <= pRange->end
                   ? winFuncCopyValue(pRes, pExpr, pInfo->pInputStore, partitionStart + pRange->end, dstRow)
                   : winFuncSetNull(pRes, pExpr, dstRow);
        break;
      case FUNCTION_TYPE_NTH_VALUE: {
        int64_t nth = winFuncValueParamI64(pExpr, 1, 1);
        if (pRange->start <= pRange->end && nth <= pRange->end - pRange->start + 1) {
          int64_t target = pRange->start + nth - 1;
          code = winFuncCopyValue(pRes, pExpr, pInfo->pInputStore, partitionStart + target, dstRow);
        } else {
          code = winFuncSetNull(pRes, pExpr, dstRow);
        }
        break;
      }
      default:
        code = winFuncCheckDedicatedFallback(pFuncName);
        break;
    }

    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t winFuncCalcPeerGroup(SWindowInputStore *pStore, const SNodeList *pOrderKeys, int32_t partitionStart,
                                    int32_t partitionEnd, int32_t peerStart, int32_t *pPeerEnd) {
  int32_t peerEnd = peerStart + 1;
  while (peerEnd < partitionEnd) {
    bool    same = false;
    int32_t code = winFuncSameOrderKeys(pStore, pOrderKeys, peerStart, peerEnd, &same);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (!same) {
      break;
    }
    ++peerEnd;
  }

  *pPeerEnd = peerEnd;
  return TSDB_CODE_SUCCESS;
}

static int32_t winFuncBuildInput(SOperatorInfo *pOperator) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SWindowFuncOperatorInfo *pInfo = pOperator->info;

  while (true) {
    SSDataBlock *pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    if (pInfo->scalarSup.pExprInfo != NULL) {
      code = projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx,
                                   pInfo->scalarSup.numOfExprs, NULL, GET_STM_RTINFO(pOperator->pTaskInfo),
                                   pOperator->pTaskInfo);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pInfo->pInputStore == NULL) {
      uint32_t pageSize = 0;
      int64_t  bufSize = 0;
      code = getBufferPgSize(pBlock->info.rowSize, &pageSize, &bufSize);
      QUERY_CHECK_CODE(code, lino, _end);
      code = winInputStoreCreate(pBlock, pageSize, bufSize, pOperator->pTaskInfo->id.str, &pInfo->pInputStore);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    code = winInputStoreAppendBlock(pInfo->pInputStore, pBlock);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pInfo->pInputStore == NULL || winInputStoreGetRows(pInfo->pInputStore) == 0) {
    pInfo->inputBuilt = true;
    return TSDB_CODE_SUCCESS;
  }

  pInfo->nextRow = 0;
  pInfo->partitionStart = 0;
  pInfo->partitionEnd = 0;
  pInfo->peerStart = 0;
  pInfo->peerEnd = 0;
  pInfo->denseRank = 0;
  pInfo->inputBuilt = true;
  return TSDB_CODE_SUCCESS;

_end:
  return code;
}

static int32_t winFuncOpenPartition(SWindowFuncOperatorInfo *pInfo, SWindowFuncPhysiNode *pWindowNode, int32_t row) {
  int32_t totalRows = winInputStoreGetRows(pInfo->pInputStore);
  int32_t partitionEnd = row + 1;
  while (partitionEnd < totalRows) {
    bool    same = false;
    int32_t code = winFuncRowsSamePartition(pInfo->pInputStore, pWindowNode->pPartitionKeys, row, partitionEnd, &same);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (!same) {
      break;
    }
    ++partitionEnd;
  }

  pInfo->partitionStart = row;
  pInfo->partitionEnd = partitionEnd;
  pInfo->peerStart = row;
  pInfo->peerEnd = row;
  pInfo->denseRank = 0;
  return TSDB_CODE_SUCCESS;
}

static int32_t winFuncBuildResultBatch(SOperatorInfo *pOperator) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SWindowFuncOperatorInfo *pInfo = pOperator->info;
  SWindowFuncPhysiNode    *pWindowNode = (SWindowFuncPhysiNode *)pOperator->pPhyNode;
  SSDataBlock             *pRes = pInfo->binfo.pRes;

  blockDataCleanup(pRes);

  int32_t totalRows = winInputStoreGetRows(pInfo->pInputStore);
  if (pInfo->pInputStore == NULL || pInfo->nextRow >= totalRows) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t batchEnd = 0;
  code = winCalcOutputBatchEnd(totalRows, pInfo->nextRow, pOperator->resultInfo.capacity, &batchEnd);
  QUERY_CHECK_CODE(code, lino, _end);

  code = blockDataEnsureCapacity(pRes, batchEnd - pInfo->nextRow);
  QUERY_CHECK_CODE(code, lino, _end);

  while (pInfo->nextRow < batchEnd) {
    int32_t row = pInfo->nextRow;
    if (row >= pInfo->partitionEnd) {
      code = winFuncOpenPartition(pInfo, pWindowNode, row);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (row >= pInfo->peerEnd) {
      pInfo->peerStart = row;
      code = winFuncCalcPeerGroup(pInfo->pInputStore, pWindowNode->pOrderKeys, pInfo->partitionStart,
                                  pInfo->partitionEnd, pInfo->peerStart, &pInfo->peerEnd);
      QUERY_CHECK_CODE(code, lino, _end);
      pInfo->denseRank += 1;
    }

    int32_t dstRow = pRes->info.rows;
    code = winFuncCopyOutputColumns(pRes, pInfo->pInputStore, row, dstRow, pInfo);
    QUERY_CHECK_CODE(code, lino, _end);

    SSqlWindowFrameRange range = {0};
    code = winFuncCalcFrame(pInfo, pWindowNode, pInfo->partitionStart, pInfo->partitionEnd, row, &range);
    QUERY_CHECK_CODE(code, lino, _end);

    code = winFuncCalcOneFrame(pOperator, pInfo, pRes, pInfo->partitionStart + range.start,
                               pInfo->partitionStart + range.end);
    QUERY_CHECK_CODE(code, lino, _end);

    code = winFuncCalcDedicated(pInfo, pWindowNode, pRes, pInfo->partitionStart, pInfo->partitionEnd, row, dstRow,
                                &range, pInfo->peerStart - pInfo->partitionStart,
                                pInfo->peerEnd - pInfo->partitionStart - 1, pInfo->denseRank);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pOperator->exprSupp.numOfExprs == 0) {
      pRes->info.rows += 1;
    }
    pInfo->nextRow += 1;
  }

  code = doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL);
  QUERY_CHECK_CODE(code, lino, _end);

  return TSDB_CODE_SUCCESS;

_end:
  return code;
}

static int32_t winFuncNext(SOperatorInfo *pOperator, SSDataBlock **ppRes) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SWindowFuncOperatorInfo *pInfo = pOperator->info;

  if (!pInfo->inputBuilt) {
    code = winFuncBuildInput(pOperator);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  while (true) {
    code = winFuncBuildResultBatch(pOperator);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pInfo->binfo.pRes->info.rows > 0 || pInfo->pInputStore == NULL ||
        pInfo->nextRow >= winInputStoreGetRows(pInfo->pInputStore)) {
      break;
    }
  }

  if (pInfo->binfo.pRes->info.rows == 0) {
    setOperatorCompleted(pOperator);
    *ppRes = NULL;
  } else {
    *ppRes = pInfo->binfo.pRes;
  }
  return TSDB_CODE_SUCCESS;

_end:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  pOperator->pTaskInfo->code = code;
  T_LONG_JMP(pOperator->pTaskInfo->env, code);
  return code;
}

int32_t createWindowFuncOperatorInfo(SOperatorInfo *downstream, SWindowFuncPhysiNode *pWindowNode,
                                     SExecTaskInfo *pTaskInfo, SOperatorInfo **pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SWindowFuncOperatorInfo *pInfo = taosMemoryCalloc(1, sizeof(SWindowFuncOperatorInfo));
  SOperatorInfo           *pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  SExprInfo               *pExprInfo = NULL;
  SExprInfo               *pFuncExprInfo = NULL;
  SExprInfo               *pScalarExprInfo = NULL;
  int32_t                  numOfExprs = 0;
  int32_t                  numOfFuncExprs = 0;
  int32_t                  numOfScalarExprs = 0;

  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  if (downstream == NULL || pWindowNode == NULL || pWindowNode->pFrame == NULL ||
      (((SWindowFrameNode *)pWindowNode->pFrame)->frameUnit != WINDOW_FRAME_UNIT_ROWS &&
       ((SWindowFrameNode *)pWindowNode->pFrame)->frameUnit != WINDOW_FRAME_UNIT_RANGE)) {
    code = TSDB_CODE_INVALID_PARA;
    goto _error;
  }

  initOperatorCostInfo(pOperator);
  pOperator->pPhyNode = pWindowNode;
  pOperator->pTaskInfo = pTaskInfo;
  pOperator->exprSupp.hasWindowOrGroup = true;
  pOperator->exprSupp.hasWindow = true;

  code = filterInitFromNode((SNode *)pWindowNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);
  filterSetExecContext(pOperator->exprSupp.pFilterInfo, pTaskInfo, isTaskKilled);

  SSDataBlock *pResBlock = createDataBlockFromDescNode(pWindowNode->node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pInfo->binfo, pResBlock);
  pInfo->binfo.inputTsOrder = pWindowNode->node.inputTsOrder;
  pInfo->binfo.outputTsOrder = pWindowNode->node.outputTsOrder;

  initResultSizeInfo(&pOperator->resultInfo, 4096);

  if (pWindowNode->pExprs != NULL) {
    code = createExprInfo(pWindowNode->pExprs, NULL, &pScalarExprInfo, &numOfScalarExprs);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalarExprs, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  code = createExprInfo(pWindowNode->pFuncs, NULL, &pFuncExprInfo, &numOfFuncExprs);
  QUERY_CHECK_CODE(code, lino, _error);
  pInfo->funcSupp.pExprInfo = pFuncExprInfo;
  pInfo->funcSupp.numOfExprs = numOfFuncExprs;
  pFuncExprInfo = NULL;

  code = winFuncCloneAggFuncs(pWindowNode->pFuncs, &pInfo->pAggFuncs);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pInfo->pAggFuncs != NULL) {
    code = createExprInfo(pInfo->pAggFuncs, NULL, &pExprInfo, &numOfExprs);
    QUERY_CHECK_CODE(code, lino, _error);

    size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
    code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, numOfExprs, keyBufSize, pTaskInfo->id.str, NULL,
                      &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);

    pInfo->pAggRow = taosMemoryCalloc(1, pInfo->aggSup.resultRowSize);
    QUERY_CHECK_NULL(pInfo->pAggRow, code, lino, _error, terrno);
  }

  SPhysiNode *pChildNode = (SPhysiNode *)nodesListGetNode(pWindowNode->node.pChildren, 0);
  if (pChildNode == NULL) {
    code = terrno;
    goto _error;
  }
  code = winFuncInitOutputSrcSlots(pInfo, pWindowNode->node.pOutputDataBlockDesc, pChildNode->pOutputDataBlockDesc);
  QUERY_CHECK_CODE(code, lino, _error);

  setOperatorInfo(pOperator, "WindowFuncOperator", QUERY_NODE_PHYSICAL_PLAN_WINDOW_FUNC, true, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, winFuncNext, NULL, destroyWindowFuncOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  if (pInfo != NULL) {
    destroyWindowFuncOperatorInfo(pInfo);
  }
  if (pFuncExprInfo != NULL) {
    destroyExprInfo(pFuncExprInfo, numOfFuncExprs);
    taosMemoryFreeClear(pFuncExprInfo);
  }
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}
