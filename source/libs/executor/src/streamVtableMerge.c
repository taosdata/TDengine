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
#include "tlosertree.h"

typedef struct SVMBufPageInfo {
  int32_t pageId;
  int64_t expireTimeMs;
} SVMBufPageInfo;

typedef struct SStreamVtableMergeSource {
  SDiskbasedBuf* pBuf;
  int32_t*       pTotalPages;

  SSDataBlock* pInputDataBlock;
  int64_t      currentExpireTimeMs;

  SList*       pageInfoList;
  SSDataBlock* pOutDataBlock;
  int32_t      rowIndex;

  int64_t latestTs;
} SStreamVtableMergeSource;

typedef struct SStreamVtableMergeHandle {
  SDiskbasedBuf* pBuf;
  int32_t        numOfPages;
  int32_t        numPageLimit;

  int32_t   nSrcTbls;
  SHashObj* pSources;

  SArray*                 pMergeNodes;
  SMultiwayMergeTreeInfo* pMergeTree;
} SStreamVtableMergeHandle;

int32_t tsStreamMergeMaxDelayMs = 10 * 1000;  // 10s
int32_t tsStreamMergeMaxMemKb = 256 * 1024;   // 256MB
int32_t tsStreamMergeWaitMode = 0;            // 0 wait forever, 1 wait for max delay, 2 wait for max mem

static void svmSourceDestroy(SStreamVtableMergeSource* pSource) {
  if (pSource == NULL) {
    return;
  }

  if (pSource->pInputDataBlock) {
    blockDataDestroy(pSource->pInputDataBlock);
    pSource->pInputDataBlock = NULL;
  }
  pSource->pageInfoList = tdListFree(pSource->pageInfoList);
  if (pSource->pOutDataBlock) {
    blockDataDestroy(pSource->pOutDataBlock);
    pSource->pOutDataBlock = NULL;
  }
  taosMemoryFree(pSource);
}

static SStreamVtableMergeSource* svmAddSource(SStreamVtableMergeHandle* pHandle, int64_t uid, const char* idstr) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SStreamVtableMergeSource* pSource = NULL;

  pSource = taosMemoryCalloc(1, sizeof(SStreamVtableMergeSource));
  QUERY_CHECK_NULL(pSource, code, lino, _end, terrno);
  pSource->pBuf = pHandle->pBuf;
  pSource->pTotalPages = &pHandle->numOfPages;
  pSource->pageInfoList = tdListNew(sizeof(SVMBufPageInfo));
  QUERY_CHECK_NULL(pSource->pageInfoList, code, lino, _end, terrno);
  pSource->latestTs = INT64_MIN;
  code = taosHashPut(pHandle->pSources, &uid, sizeof(uid), &pSource, POINTER_BYTES);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
    svmSourceDestroy(pSource);
    pSource = NULL;
    terrno = code;
  }
  return pSource;
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
  int32_t size = blockDataGetSize(pBlock) + sizeof(int32_t) + taosArrayGetSize(pBlock->pDataBlock) * sizeof(int32_t);
  QUERY_CHECK_CONDITION(size <= getBufPageSize(pSource->pBuf), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

  page = getNewBufPage(pSource->pBuf, &pageInfo.pageId);
  QUERY_CHECK_NULL(page, code, lino, _end, terrno);
  pageInfo.expireTimeMs = pSource->currentExpireTimeMs;
  code = tdListAppend(pSource->pageInfoList, &pageInfo);
  QUERY_CHECK_CODE(code, lino, _end);
  code = blockDataToBuf(page, pBlock);
  QUERY_CHECK_CODE(code, lino, _end);
  setBufPageDirty(page, true);
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
  int32_t      pageSize = 0;
  int32_t      holdSize = 0;
  SSDataBlock* pInputDataBlock = NULL;

  QUERY_CHECK_NULL(pDataBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pSource, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pInputDataBlock = pSource->pInputDataBlock;
  if (pInputDataBlock == NULL) {
    code = createOneDataBlock(pDataBlock, false, &pInputDataBlock);
    QUERY_CHECK_CODE(code, lino, _end);
    pSource->pInputDataBlock = pInputDataBlock;
  }

  int32_t start = 0;
  int32_t nrows = blockDataGetNumOfRows(pDataBlock);
  while (start < nrows) {
    int32_t holdSize = blockDataGetSize(pInputDataBlock);
    QUERY_CHECK_CONDITION(holdSize < pageSize, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    int32_t stop = 0;
    code = blockDataSplitRows(pDataBlock, pDataBlock->info.hasVarCol, start, &stop, pageSize - holdSize);
    if (stop == start - 1) {
      // If pInputDataBlock cannot hold new rows, ignore the error and write pInputDataBlock to the buffer
    } else {
      QUERY_CHECK_CODE(code, lino, _end);
      // append new rows to pInputDataBlock
      int32_t numOfRows = stop - start + 1;
      code = blockDataMergeNRows(pInputDataBlock, pDataBlock, start, numOfRows);
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

int32_t streamVtableMergeAddBlock(SStreamVtableMergeHandle* pHandle, SSDataBlock* pDataBlock, const char* idstr) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  int64_t                   pTbUid = 0;
  void*                     px = 0;
  SStreamVtableMergeSource* pSource = NULL;

  QUERY_CHECK_NULL(pHandle, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pDataBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pTbUid = pDataBlock->info.id.uid;
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

static bool svmSourceIsEmpty(SStreamVtableMergeSource* pSource) { return listNEles(pSource->pageInfoList) == 0; }

static int32_t svmSourceReadBuf(SStreamVtableMergeSource* pSource, const char* idstr) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SListNode*      pn = NULL;
  SVMBufPageInfo* pageInfo = NULL;
  void*           page = NULL;

  QUERY_CHECK_NULL(pSource, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(!svmSourceIsEmpty(pSource), code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pSource->pOutDataBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);

  blockDataCleanup(pSource->pOutDataBlock);

  pn = tdListGetHead(pSource->pageInfoList);
  QUERY_CHECK_NULL(pn, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  pageInfo = (SVMBufPageInfo*)pn->data;
  QUERY_CHECK_NULL(pageInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

  page = getBufPage(pSource->pBuf, pageInfo->pageId);
  QUERY_CHECK_NULL(page, code, lino, _end, terrno);
  code = blockDataFromBuf(pSource->pOutDataBlock, page);
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
  QUERY_CHECK_CONDITION(!svmSourceIsEmpty(pSource), code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pSource->pOutDataBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (blockDataGetNumOfRows(pSource->pOutDataBlock) == 0) {
    code = svmSourceReadBuf(pSource, idstr);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  QUERY_CHECK_CONDITION(pSource->rowIndex < blockDataGetNumOfRows(pSource->pOutDataBlock), code, lino, _end,
                        TSDB_CODE_INVALID_PARA);

  tsCol = taosArrayGet(pSource->pOutDataBlock->pDataBlock, 0);
  QUERY_CHECK_NULL(tsCol, code, lino, _end, terrno);

  *pTs = ((int64_t*)tsCol->pData)[pSource->rowIndex];
  if (*pTs > pSource->latestTs) {
    pSource->latestTs = *pTs;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
  }
  return code;
}

static int32_t svmSourceMoveNext(SStreamVtableMergeSource* pSource, const char* idstr, SVM_NEXT_RESULT* pRes) {
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    lino = 0;
  SListNode* pn = NULL;
  void*      page = NULL;
  int64_t    latestTs = 0;

  QUERY_CHECK_NULL(pSource, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pSource->pOutDataBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pRes = SVM_NEXT_NOT_READY;
  latestTs = pSource->latestTs;

  while (true) {
    if (svmSourceIsEmpty(pSource)) {
      pSource->rowIndex = 0;
      break;
    }

    if (blockDataGetNumOfRows(pSource->pOutDataBlock) == 0) {
      code = svmSourceReadBuf(pSource, idstr);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    QUERY_CHECK_CONDITION(pSource->rowIndex < blockDataGetNumOfRows(pSource->pOutDataBlock), code, lino, _end,
                          TSDB_CODE_INVALID_PARA);

    pSource->rowIndex++;
    if (pSource->rowIndex >= blockDataGetNumOfRows(pSource->pOutDataBlock)) {
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
      pSource->rowIndex = 0;
    }

    if (svmSourceIsEmpty(pSource)) {
      pSource->rowIndex = 0;
      break;
    }

    int64_t ts = 0;
    code = svmSourceCurrentTs(pSource, idstr, &ts);
    QUERY_CHECK_CODE(code, lino, _end);
    if (ts > latestTs) {
      *pRes = SVM_NEXT_FOUND;
      break;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
  }
  return code;
}

static int32_t svmBuildMergeTree(SStreamVtableMergeHandle* pHandle, SVM_NEXT_RESULT* pRes, const char* idstr) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void*   pIter = NULL;

  QUERY_CHECK_NULL(pHandle, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pRes, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pRes = SVM_NEXT_NOT_READY;

  if (pHandle->pMergeNodes == NULL) {
    pHandle->pMergeNodes = taosArrayInit(pHandle->nSrcTbls, POINTER_BYTES);
    QUERY_CHECK_NULL(pHandle->pMergeNodes, code, lino, _end, terrno);
  }
  taosArrayClear(pHandle->pMergeNodes);

  pIter = taosHashIterate(pHandle->pSources, NULL);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
  }
  return code;
}
