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

#include "streamVtableMerge.h"

#include "query.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tlosertree.h"

typedef struct SVMBufPageInfo {
  int32_t pageId;
  int64_t expireTimeMs;
} SVMBufPageInfo;

typedef struct SStreamVtableMergeSource {
  SDiskbasedBuf* pBuf;         // buffer for storing data
  int32_t*       pTotalPages;  // total pages of all sources in the buffer
  int32_t        primaryTsIndex;

  SSDataBlock* pInputDataBlock;      // data block to be written to the buffer
  int64_t      currentExpireTimeMs;  // expire time of the input data block

  SList*       pageInfoList;      // info of current source's page in the buffer
  SSDataBlock* pOutputDataBlock;  // data block read from the buffer
  int32_t      rowIndex;          // current row index of the output data block

  int64_t  latestTs;         // latest timestamp of the source
  int64_t* pGlobalLatestTs;  // global latest timestamp of all sources
} SStreamVtableMergeSource;

typedef struct SStreamVtableMergeHandle {
  SDiskbasedBuf* pBuf;
  int32_t        numOfPages;
  int32_t        numPageLimit;
  int32_t        primaryTsIndex;

  int64_t      vuid;
  int32_t      nSrcTbls;
  SHashObj*    pSources;
  SSDataBlock* datablock;  // Does not store data, only used to save the schema of input/output data blocks

  SMultiwayMergeTreeInfo* pMergeTree;
  int32_t                 numEmptySources;
  int64_t                 globalLatestTs;
} SStreamVtableMergeHandle;

typedef enum StreamVirtualMergeMode {
  STREAM_VIRTUAL_MERGE_WAIT_FOREVER,
  STREAM_VIRTUAL_MERGE_MAX_DELAY,
  STREAM_VIRTUAL_MERGE_MAX_MEMORY,
} StreamVirtualMergeMode;

static void svmSourceDestroy(void* ptr) {
  SStreamVtableMergeSource** ppSource = ptr;
  if (ppSource == NULL || *ppSource == NULL) {
    return;
  }

  SStreamVtableMergeSource* pSource = *ppSource;
  if (pSource->pInputDataBlock) {
    blockDataDestroy(pSource->pInputDataBlock);
    pSource->pInputDataBlock = NULL;
  }
  pSource->pageInfoList = tdListFree(pSource->pageInfoList);
  if (pSource->pOutputDataBlock) {
    blockDataDestroy(pSource->pOutputDataBlock);
    pSource->pOutputDataBlock = NULL;
  }

  taosMemoryFreeClear(*ppSource);
}

static int32_t svmSourceFlushInput(SStreamVtableMergeSource* pSource, const char* idstr) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SSDataBlock*   pBlock = NULL;
  SVMBufPageInfo pageInfo = {0};
  void*          page = NULL;

  QUERY_CHECK_NULL(pSource, code, lino, _end, TSDB_CODE_INVALID_PARA);

  // check data block size
  pBlock = pSource->pInputDataBlock;
  if (blockDataGetNumOfRows(pBlock) == 0) {
    goto _end;
  }
  int32_t size = blockDataGetSize(pBlock) + sizeof(int32_t) + taosArrayGetSize(pBlock->pDataBlock) * sizeof(int32_t);
  QUERY_CHECK_CONDITION(size <= getBufPageSize(pSource->pBuf), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

  // allocate a new page and write data to it
  page = getNewBufPage(pSource->pBuf, &pageInfo.pageId);
  QUERY_CHECK_NULL(page, code, lino, _end, terrno);
  code = blockDataToBuf(page, pBlock);
  QUERY_CHECK_CODE(code, lino, _end);
  setBufPageDirty(page, true);

  // save page info
  pageInfo.expireTimeMs = pSource->currentExpireTimeMs;
  code = tdListAppend(pSource->pageInfoList, &pageInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  releaseBufPage(pSource->pBuf, page);
  page = NULL;
  blockDataCleanup(pBlock);
  (*pSource->pTotalPages)++;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
  }
  if (page != NULL) {
    dBufSetBufPageRecycled(pSource->pBuf, page);
  }
  return code;
}

static int32_t svmSourceAddBlock(SStreamVtableMergeSource* pSource, SSDataBlock* pDataBlock, const char* idstr) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pInputDataBlock = NULL;

  QUERY_CHECK_NULL(pDataBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pSource, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pInputDataBlock = pSource->pInputDataBlock;
  QUERY_CHECK_CONDITION(taosArrayGetSize(pDataBlock->pDataBlock) >= taosArrayGetSize(pInputDataBlock->pDataBlock), code,
                        lino, _end, TSDB_CODE_INVALID_PARA);

  int32_t start = 0;
  int32_t nrows = blockDataGetNumOfRows(pDataBlock);
  int32_t pageSize =
      getBufPageSize(pSource->pBuf) - sizeof(int32_t) - taosArrayGetSize(pInputDataBlock->pDataBlock) * sizeof(int32_t);
  while (start < nrows) {
    int32_t holdSize = blockDataGetSize(pInputDataBlock);
    QUERY_CHECK_CONDITION(holdSize < pageSize, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    int32_t stop = start;
    code = blockDataSplitRows(pDataBlock, pDataBlock->info.hasVarCol, start, &stop, pageSize - holdSize);
    if (stop == start - 1) {
      // If pInputDataBlock cannot hold new rows, ignore the error and write pInputDataBlock to the buffer
    } else {
      QUERY_CHECK_CODE(code, lino, _end);
      // append new rows to pInputDataBlock
      if (blockDataGetNumOfRows(pInputDataBlock) == 0) {
        // set expires time for the first block
        pSource->currentExpireTimeMs = taosGetTimestampMs() + tsStreamVirtualMergeMaxDelayMs;
      }
      int32_t numOfRows = stop - start + 1;
      code = blockDataEnsureCapacity(pInputDataBlock, pInputDataBlock->info.rows + numOfRows);
      QUERY_CHECK_CODE(code, lino, _end);
      code = blockDataMergeNRows(pInputDataBlock, pDataBlock, start, numOfRows);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    start = stop + 1;
    if (stop == nrows - 1) {
      break;
    } else {
      // pInputDataBlock is full, write it to the buffer
      code = svmSourceFlushInput(pSource, idstr);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
  }
  return code;
}

static bool svmSourceIsEmpty(SStreamVtableMergeSource* pSource) { return listNEles(pSource->pageInfoList) == 0; }

static int64_t svmSourceGetExpireTime(SStreamVtableMergeSource* pSource) {
  SListNode* pn = tdListGetHead(pSource->pageInfoList);
  if (pn != NULL) {
    SVMBufPageInfo* pageInfo = (SVMBufPageInfo*)pn->data;
    if (pageInfo != NULL) {
      return pageInfo->expireTimeMs;
    }
  }
  return INT64_MAX;
}

static int32_t svmSourceReadBuf(SStreamVtableMergeSource* pSource, const char* idstr) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SListNode*      pn = NULL;
  SVMBufPageInfo* pageInfo = NULL;
  void*           page = NULL;

  QUERY_CHECK_NULL(pSource, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(!svmSourceIsEmpty(pSource), code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pSource->pOutputDataBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);

  blockDataCleanup(pSource->pOutputDataBlock);
  int32_t numOfCols = taosArrayGetSize(pSource->pOutputDataBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; i++) {
    SColumnInfoData* pCol = taosArrayGet(pSource->pOutputDataBlock->pDataBlock, i);
    pCol->hasNull = true;
  }

  pn = tdListGetHead(pSource->pageInfoList);
  QUERY_CHECK_NULL(pn, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  pageInfo = (SVMBufPageInfo*)pn->data;
  QUERY_CHECK_NULL(pageInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

  page = getBufPage(pSource->pBuf, pageInfo->pageId);
  QUERY_CHECK_NULL(page, code, lino, _end, terrno);
  code = blockDataFromBuf(pSource->pOutputDataBlock, page);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
  }
  if (page) {
    releaseBufPage(pSource->pBuf, page);
  }
  return code;
}

static int32_t svmSourceCurrentTs(SStreamVtableMergeSource* pSource, const char* idstr, int64_t* pTs) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SColumnInfoData* tsCol = NULL;

  QUERY_CHECK_NULL(pSource, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pSource->pOutputDataBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(pSource->rowIndex >= 0 && pSource->rowIndex < blockDataGetNumOfRows(pSource->pOutputDataBlock),
                        code, lino, _end, TSDB_CODE_INVALID_PARA);

  tsCol = taosArrayGet(pSource->pOutputDataBlock->pDataBlock, pSource->primaryTsIndex);
  QUERY_CHECK_NULL(tsCol, code, lino, _end, terrno);
  QUERY_CHECK_CONDITION(tsCol->info.type == TSDB_DATA_TYPE_TIMESTAMP, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pTs = ((int64_t*)tsCol->pData)[pSource->rowIndex];

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
  }
  return code;
}

static int32_t svmSourceMoveNext(SStreamVtableMergeSource* pSource, const char* idstr) {
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    lino = 0;
  SListNode* pn = NULL;
  void*      page = NULL;

  QUERY_CHECK_NULL(pSource, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pSource->pOutputDataBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);

  while (true) {
    if (pSource->rowIndex >= 0) {
      QUERY_CHECK_CONDITION(pSource->rowIndex < blockDataGetNumOfRows(pSource->pOutputDataBlock), code, lino, _end,
                            TSDB_CODE_INVALID_PARA);
      pSource->rowIndex++;
      if (pSource->rowIndex >= blockDataGetNumOfRows(pSource->pOutputDataBlock)) {
        // Pop the page from the list and recycle it
        pn = tdListPopHead(pSource->pageInfoList);
        QUERY_CHECK_NULL(pn, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        QUERY_CHECK_NULL(pn->data, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        SVMBufPageInfo* pageInfo = (SVMBufPageInfo*)pn->data;
        page = getBufPage(pSource->pBuf, pageInfo->pageId);
        QUERY_CHECK_NULL(page, code, lino, _end, terrno);
        code = dBufSetBufPageRecycled(pSource->pBuf, page);
        QUERY_CHECK_CODE(code, lino, _end);
        (*pSource->pTotalPages)--;
        taosMemoryFreeClear(pn);
        pSource->rowIndex = -1;
      }
    }

    if (pSource->rowIndex == -1) {
      if (svmSourceIsEmpty(pSource)) {
        break;
      }
      // Read the first page from the list
      code = svmSourceReadBuf(pSource, idstr);
      QUERY_CHECK_CODE(code, lino, _end);
      pSource->rowIndex = 0;
    }

    // Check the timestamp of the current row
    int64_t currentTs = INT64_MIN;
    code = svmSourceCurrentTs(pSource, idstr, &currentTs);
    if (currentTs > pSource->latestTs) {
      pSource->latestTs = currentTs;
      if (currentTs >= *pSource->pGlobalLatestTs) {
        break;
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
  }
  return code;
}

static int32_t svmSourceCompare(const void* pLeft, const void* pRight, void* param) {
  int32_t left = *(int32_t*)pLeft;
  int32_t right = *(int32_t*)pRight;
  int32_t code = TSDB_CODE_SUCCESS;

  SArray*                   pValidSources = param;
  SStreamVtableMergeSource* pLeftSource = *(SStreamVtableMergeSource**)taosArrayGet(pValidSources, left);
  SStreamVtableMergeSource* pRightSource = *(SStreamVtableMergeSource**)taosArrayGet(pValidSources, right);

  if (svmSourceIsEmpty(pLeftSource)) {
    return 1;
  } else if (svmSourceIsEmpty(pRightSource)) {
    return -1;
  }

  int64_t leftTs = 0;
  code = svmSourceCurrentTs(pLeftSource, "", &leftTs);
  if (code != TSDB_CODE_SUCCESS) {
    return -1;
  }
  int64_t rightTs = 0;
  code = svmSourceCurrentTs(pRightSource, "", &rightTs);
  if (code != TSDB_CODE_SUCCESS) {
    return 1;
  }

  if (leftTs < rightTs) {
    return -1;
  } else if (leftTs > rightTs) {
    return 1;
  } else {
    return 0;
  }
}

static SStreamVtableMergeSource* svmAddSource(SStreamVtableMergeHandle* pHandle, int64_t uid, const char* idstr) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SStreamVtableMergeSource* pSource = NULL;

  pSource = taosMemoryCalloc(1, sizeof(SStreamVtableMergeSource));
  QUERY_CHECK_NULL(pSource, code, lino, _end, terrno);
  pSource->pBuf = pHandle->pBuf;
  pSource->pTotalPages = &pHandle->numOfPages;
  pSource->primaryTsIndex = pHandle->primaryTsIndex;
  code = createOneDataBlock(pHandle->datablock, false, &pSource->pInputDataBlock);
  QUERY_CHECK_CODE(code, lino, _end);
  pSource->pageInfoList = tdListNew(sizeof(SVMBufPageInfo));
  QUERY_CHECK_NULL(pSource->pageInfoList, code, lino, _end, terrno);
  code = createOneDataBlock(pHandle->datablock, false, &pSource->pOutputDataBlock);
  QUERY_CHECK_CODE(code, lino, _end);
  pSource->rowIndex = -1;
  pSource->latestTs = INT64_MIN;
  pSource->pGlobalLatestTs = &pHandle->globalLatestTs;
  code = taosHashPut(pHandle->pSources, &uid, sizeof(uid), &pSource, POINTER_BYTES);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
    svmSourceDestroy(&pSource);
    pSource = NULL;
    terrno = code;
  }
  return pSource;
}

static void svmDestroyTree(void* ptr) {
  SMultiwayMergeTreeInfo** ppTree = ptr;
  if (ppTree == NULL || *ppTree == NULL) {
    return;
  }
  SMultiwayMergeTreeInfo* pTree = *ppTree;

  if (pTree->param != NULL) {
    taosArrayDestroy(pTree->param);
    pTree->param = NULL;
  }

  tMergeTreeDestroy(ppTree);
}

static int32_t svmBuildTree(SStreamVtableMergeHandle* pHandle, SVM_NEXT_RESULT* pRes, const char* idstr) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SArray* pReadySources = NULL;
  void*   pIter = NULL;
  void*   px = NULL;
  int64_t globalExpireTimeMs = INT64_MAX;

  QUERY_CHECK_NULL(pHandle, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pRes, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pRes = SVM_NEXT_NOT_READY;

  // find all non-empty sources
  pReadySources = taosArrayInit(pHandle->nSrcTbls, POINTER_BYTES);
  pIter = taosHashIterate(pHandle->pSources, NULL);
  while (pIter != NULL) {
    SStreamVtableMergeSource* pSource = *(SStreamVtableMergeSource**)pIter;
    code = svmSourceFlushInput(pSource, idstr);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pSource->rowIndex == -1) {
      code = svmSourceMoveNext(pSource, idstr);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    if (!svmSourceIsEmpty(pSource)) {
      px = taosArrayPush(pReadySources, &pSource);
      QUERY_CHECK_NULL(px, code, lino, _end, terrno);
      globalExpireTimeMs = TMIN(globalExpireTimeMs, svmSourceGetExpireTime(pSource));
    }
    pIter = taosHashIterate(pHandle->pSources, pIter);
  }

  if (taosArrayGetSize(pReadySources) == 0) {
    // no available sources
    goto _end;
  }

  if (taosArrayGetSize(pReadySources) < pHandle->nSrcTbls) {
    // some sources are still empty
    switch (tsStreamVirtualMergeWaitMode) {
      case STREAM_VIRTUAL_MERGE_WAIT_FOREVER:
        goto _end;  // wait forever

      case STREAM_VIRTUAL_MERGE_MAX_DELAY:
        if (taosGetTimestampMs() < globalExpireTimeMs) {
          goto _end;  // wait for max delay
        }
        break;

      case STREAM_VIRTUAL_MERGE_MAX_MEMORY:
        if (pHandle->numOfPages < pHandle->numPageLimit) {
          goto _end;  // wait for max memory
        }
        break;
    };
  }

  void* param = NULL;
  code = tMergeTreeCreate(&pHandle->pMergeTree, taosArrayGetSize(pReadySources), pReadySources, svmSourceCompare);
  QUERY_CHECK_CODE(code, lino, _end);
  pHandle->numEmptySources = 0;
  pReadySources = NULL;
  *pRes = SVM_NEXT_FOUND;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
  }
  if (pReadySources != NULL) {
    taosArrayDestroy(pReadySources);
  }
  if (pIter != NULL) {
    taosHashCancelIterate(pHandle->pSources, pIter);
  }
  return code;
}

int32_t streamVtableMergeAddBlock(SStreamVtableMergeHandle* pHandle, SSDataBlock* pDataBlock, const char* idstr) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  int64_t                   pTbUid = 0;
  void*                     px = 0;
  SStreamVtableMergeSource* pSource = NULL;

  QUERY_CHECK_NULL(pHandle, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pDataBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pTbUid = pDataBlock->info.id.groupId;
  px = taosHashGet(pHandle->pSources, &pTbUid, sizeof(int64_t));

  if (px == NULL) {
    if (taosHashGetSize(pHandle->pSources) >= pHandle->nSrcTbls) {
      qError("Number of source tables exceeded the limit %d, table uid: %" PRId64, pHandle->nSrcTbls, pTbUid);
      code = TSDB_CODE_INTERNAL_ERROR;
      QUERY_CHECK_CODE(code, lino, _end);
    }
    // try to allocate a new source
    pSource = svmAddSource(pHandle, pTbUid, idstr);
    QUERY_CHECK_NULL(pSource, code, lino, _end, terrno);
  } else {
    pSource = *(SStreamVtableMergeSource**)px;
    QUERY_CHECK_NULL(pSource, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  }

  code = svmSourceAddBlock(pSource, pDataBlock, idstr);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
  }
  return code;
}

int32_t streamVtableMergeCurrent(SStreamVtableMergeHandle* pHandle, SSDataBlock** ppDataBlock, int32_t* pRowIdx,
                                 const char* idstr) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  QUERY_CHECK_NULL(pHandle, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pHandle->pMergeTree, code, lino, _end, TSDB_CODE_INVALID_PARA);

  int32_t idx = tMergeTreeGetChosenIndex(pHandle->pMergeTree);
  SArray* pReadySources = pHandle->pMergeTree->param;
  void*   px = taosArrayGet(pReadySources, idx);
  QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  SStreamVtableMergeSource* pSource = *(SStreamVtableMergeSource**)px;
  QUERY_CHECK_NULL(pSource, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  *ppDataBlock = pSource->pOutputDataBlock;
  *pRowIdx = pSource->rowIndex;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
  }
  return code;
}

int32_t streamVtableMergeMoveNext(SStreamVtableMergeHandle* pHandle, SVM_NEXT_RESULT* pRes, const char* idstr) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  void*                     px = NULL;
  SArray*                   pReadySources = NULL;
  SStreamVtableMergeSource* pSource = NULL;

  QUERY_CHECK_NULL(pHandle, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pRes, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pRes = SVM_NEXT_NOT_READY;
  if (pHandle->pMergeTree == NULL) {
    code = svmBuildTree(pHandle, pRes, idstr);
    QUERY_CHECK_CODE(code, lino, _end);
    goto _end;
  }

  int32_t idx = tMergeTreeGetChosenIndex(pHandle->pMergeTree);
  pReadySources = pHandle->pMergeTree->param;
  px = taosArrayGet(pReadySources, idx);
  QUERY_CHECK_NULL(px, code, lino, _end, terrno);
  pSource = *(SStreamVtableMergeSource**)px;
  QUERY_CHECK_CODE(code, lino, _end);
  pHandle->globalLatestTs = TMAX(pSource->latestTs, pHandle->globalLatestTs);

  int32_t origNumOfPages = pHandle->numOfPages;
  code = svmSourceMoveNext(pSource, idstr);
  QUERY_CHECK_CODE(code, lino, _end);

  if (svmSourceIsEmpty(pSource)) {
    ++pHandle->numEmptySources;
  }

  bool needDestroy = false;
  if (pHandle->numEmptySources == taosArrayGetSize(pReadySources)) {
    // all sources are empty
    needDestroy = true;
  } else {
    code = tMergeTreeAdjust(pHandle->pMergeTree, tMergeTreeGetAdjustIndex(pHandle->pMergeTree));
    QUERY_CHECK_CODE(code, lino, _end);
    if (pHandle->numEmptySources > 0) {
      if (tsStreamVirtualMergeWaitMode == STREAM_VIRTUAL_MERGE_WAIT_FOREVER) {
        idx = tMergeTreeGetChosenIndex(pHandle->pMergeTree);
        px = taosArrayGet(pReadySources, idx);
        QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        pSource = *(SStreamVtableMergeSource**)px;
        QUERY_CHECK_NULL(pSource, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        int64_t currentTs = INT64_MIN;
        code = svmSourceCurrentTs(pSource, idstr, &currentTs);
        QUERY_CHECK_CODE(code, lino, _end);
        needDestroy = currentTs > pHandle->globalLatestTs;
      } else if (pHandle->numOfPages != origNumOfPages) {
        // The original data for this portion is incomplete. Its merge was forcibly triggered by certain conditions, so
        // we must recheck if those conditions are still met.
        if (tsStreamVirtualMergeWaitMode == STREAM_VIRTUAL_MERGE_MAX_DELAY) {
          int64_t globalExpireTimeMs = INT64_MAX;
          for (int32_t i = 0; i < taosArrayGetSize(pReadySources); ++i) {
            px = taosArrayGet(pReadySources, i);
            QUERY_CHECK_NULL(px, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
            pSource = *(SStreamVtableMergeSource**)px;
            globalExpireTimeMs = TMIN(globalExpireTimeMs, svmSourceGetExpireTime(pSource));
          }
          needDestroy = taosGetTimestampMs() < globalExpireTimeMs;
        } else if (tsStreamVirtualMergeWaitMode == STREAM_VIRTUAL_MERGE_MAX_MEMORY) {
          needDestroy = pHandle->numOfPages < pHandle->numPageLimit;
        } else {
          code = TSDB_CODE_INTERNAL_ERROR;
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }
  }
  if (needDestroy) {
    svmDestroyTree(&pHandle->pMergeTree);
  } else {
    *pRes = SVM_NEXT_FOUND;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
  }
  return code;
}

int32_t streamVtableMergeCreateHandle(SStreamVtableMergeHandle** ppHandle, int64_t vuid, int32_t nSrcTbls,
                                      int32_t numPageLimit, int32_t primaryTsIndex, SDiskbasedBuf* pBuf,
                                      SSDataBlock* pResBlock, const char* idstr) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SStreamVtableMergeHandle* pHandle = NULL;

  QUERY_CHECK_NULL(ppHandle, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(nSrcTbls > 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pBuf, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *ppHandle = NULL;
  pHandle = taosMemoryCalloc(1, sizeof(SStreamVtableMergeHandle));
  QUERY_CHECK_NULL(pHandle, code, lino, _end, terrno);

  pHandle->pBuf = pBuf;
  pHandle->numPageLimit = numPageLimit;
  pHandle->primaryTsIndex = primaryTsIndex;
  pHandle->vuid = vuid;
  pHandle->nSrcTbls = nSrcTbls;
  pHandle->pSources = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  QUERY_CHECK_NULL(pHandle->pSources, code, lino, _end, terrno);
  taosHashSetFreeFp(pHandle->pSources, svmSourceDestroy);
  code = createOneDataBlock(pResBlock, false, &pHandle->datablock);
  QUERY_CHECK_CODE(code, lino, _end);
  pHandle->globalLatestTs = INT64_MIN;

  *ppHandle = pHandle;
  pHandle = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
  }
  if (pHandle != NULL) {
    streamVtableMergeDestroyHandle(&pHandle);
  }
  return code;
}

void streamVtableMergeDestroyHandle(void* ptr) {
  SStreamVtableMergeHandle** ppHandle = ptr;
  if (ppHandle == NULL || *ppHandle == NULL) {
    return;
  }
  SStreamVtableMergeHandle* pHandle = *ppHandle;

  if (pHandle->pSources != NULL) {
    taosHashCleanup(pHandle->pSources);
    pHandle->pSources = NULL;
  }
  blockDataDestroy(pHandle->datablock);
  svmDestroyTree(&pHandle->pMergeTree);

  taosMemoryFreeClear(*ppHandle);
}

int64_t streamVtableMergeHandleGetVuid(SStreamVtableMergeHandle* pHandle) {
  if (pHandle != NULL) {
    return pHandle->vuid;
  } else {
    return 0;
  }
}
