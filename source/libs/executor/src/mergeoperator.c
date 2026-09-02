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

#include "executorInt.h"
#include "filter.h"
#include "operator.h"
#include "querytask.h"
#include "tdatablock.h"
#include "tcompare.h"

typedef struct SSortMergeInfo {
  SArray*        pSortInfo;
  SSortHandle*   pSortHandle;
  SArray*        pSourceParams;
  STimeWindow    inputTimeWindow;  // union of source block ranges, preserved across sort materialization
  STupleHandle*  prefetchedTuple;
  int32_t        bufPageSize;
  uint32_t       sortBufSize;  // max buffer size for in-memory sort
  SSDataBlock*   pIntermediateBlock;   // to hold the intermediate result
  SSDataBlock*   pInputBlock;
  SColMatchInfo  matchInfo;
} SSortMergeInfo;

typedef struct SSortMergeSourceParam {
  SOperatorInfo* pOperator;
  STimeWindow*   pInputTimeWindow;
} SSortMergeSourceParam;

typedef struct SNonSortMergeInfo {
  int32_t  lastSourceIdx;
  int32_t  sourceWorkIdx;
  int32_t  sourceNum;
  int32_t* pSourceStatus;
  bool     detachParams;
} SNonSortMergeInfo;

typedef struct SColsMergeInfo {
  SNodeList* pTargets;
  size_t     sourceNum;
  int64_t*   srcBlkIds;
} SColsMergeInfo;

typedef struct SMultiwayMergeOperatorInfo {
  SOptrBasicInfo binfo;
  EMergeType     type;
  union {
    SSortMergeInfo    sortMergeInfo;
    SNonSortMergeInfo nsortMergeInfo;
    SColsMergeInfo    colsMergeInfo;
  };
  SLimitInfo     limitInfo;
  bool           groupMerge;
  bool           ignoreGroupId;
  uint64_t       groupId;
  uint64_t       baseGroupId;
  bool           groupInitialized;
  bool           inputWithGroupId;
} SMultiwayMergeOperatorInfo;

static int32_t doSortMerge(SOperatorInfo* pOperator, SSDataBlock** pResBlock);
static int32_t doMultiwayMerge(SOperatorInfo* pOperator, SSDataBlock** pResBlock);
static int32_t doNonSortMerge(SOperatorInfo* pOperator, SSDataBlock** pResBlock);
static int32_t resetMultiwayMergeOperState(SOperatorInfo* pOper);
static int32_t doColsMerge(SOperatorInfo* pOperator, SSDataBlock** pResBlock);
static int32_t sortMergeloadNextDataBlock(void* param, SSDataBlock** ppBlock);

static void destroySortMergeSourceParams(SSortMergeInfo* pInfo) {
  if (pInfo->pSourceParams == NULL) {
    return;
  }

  size_t numOfParams = taosArrayGetSize(pInfo->pSourceParams);
  for (size_t i = 0; i < numOfParams; ++i) {
    SSortMergeSourceParam** ppParam = taosArrayGet(pInfo->pSourceParams, i);
    if (ppParam != NULL) {
      taosMemoryFree(*ppParam);
    }
  }

  taosArrayDestroy(pInfo->pSourceParams);
  pInfo->pSourceParams = NULL;
}

int32_t sortMergeloadNextDataBlock(void* param, SSDataBlock** ppBlock) {
  SSortMergeSourceParam* pSourceParam = param;
  SOperatorInfo*         pOperator = pSourceParam->pOperator;
  int32_t                code = TSDB_CODE_SUCCESS;
  int32_t                lino = 0;

  code = pOperator->fpSet.getNextFn(pOperator, ppBlock);
  QUERY_CHECK_CODE(code, lino, _return);

  code = blockDataCheck(*ppBlock);
  QUERY_CHECK_CODE(code, lino, _return);

  if (*ppBlock != NULL && (*ppBlock)->info.window.skey <= (*ppBlock)->info.window.ekey) {
    pSourceParam->pInputTimeWindow->skey =
        TMIN(pSourceParam->pInputTimeWindow->skey, (*ppBlock)->info.window.skey);
    pSourceParam->pInputTimeWindow->ekey =
        TMAX(pSourceParam->pInputTimeWindow->ekey, (*ppBlock)->info.window.ekey);
  }

  return code;
_return:
  qError("%s failed to load next data block from upstream, %s line: %d, code:%s", __func__, GET_TASKID(pOperator->pTaskInfo),
         lino, tstrerror(code));
  return code;
}

int32_t openSortMergeOperator(SOperatorInfo* pOperator) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SSortMergeInfo*             pSortMergeInfo = &pInfo->sortMergeInfo;
  int32_t                     numOfBufPage = (int32_t)pSortMergeInfo->sortBufSize / pSortMergeInfo->bufPageSize;

  pSortMergeInfo->pSortHandle = NULL;
  pSortMergeInfo->inputTimeWindow = (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN};
  pSortMergeInfo->pSourceParams = taosArrayInit(pOperator->numOfDownstream, POINTER_BYTES);
  QUERY_CHECK_NULL(pSortMergeInfo->pSourceParams, code, lino, _return, terrno);

  code = tsortCreateSortHandle(pSortMergeInfo->pSortInfo, SORT_MULTISOURCE_MERGE, pSortMergeInfo->bufPageSize,
                               numOfBufPage, pSortMergeInfo->pInputBlock, pTaskInfo->id.str, 0, 0, 0, &pSortMergeInfo->pSortHandle);
  QUERY_CHECK_CODE(code, lino, _return);

  tsortSetFetchRawDataFp(pSortMergeInfo->pSortHandle, sortMergeloadNextDataBlock, NULL, NULL);
  const SStreamRuntimeFuncInfo* pStream = GET_STM_RTINFO(pTaskInfo);
  bool compareGroupId = pInfo->groupMerge || (pInfo->inputWithGroupId && pStream != NULL && pStream->isMultiGroupCalc);
  tsortSetCompareGroupId(pSortMergeInfo->pSortHandle, compareGroupId);

  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SOperatorInfo* pDownstream = pOperator->pDownstream[i];
    if (NULL != pOperator->pOperatorGetParam && NULL != pOperator->pDownstreamGetParams &&
        NULL == pOperator->pDownstreamGetParams[i]) {
      continue;
    }
    if (pDownstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
      code = pDownstream->fpSet._openFn(pDownstream);
      QUERY_CHECK_CODE(code, lino, _return);
      if (pOperator->pDownstreamGetParams && pOperator->pDownstreamGetParams[i]) {
        pDownstream->pOperatorGetParam = pOperator->pDownstreamGetParams[i];
        pOperator->pDownstreamGetParams[i] = NULL;
      }
    } else if (pOperator->pDownstreamGetParams && pOperator->pDownstreamGetParams[i]) {
      code = setOperatorParams(pDownstream, pOperator->pDownstreamGetParams[i], OP_GET_PARAM);
      QUERY_CHECK_CODE(code, lino, _return);
      pOperator->pDownstreamGetParams[i] = NULL;
    }

    SSortMergeSourceParam* pSourceParam = taosMemoryCalloc(1, sizeof(SSortMergeSourceParam));
    QUERY_CHECK_NULL(pSourceParam, code, lino, _return, terrno);
    pSourceParam->pOperator = pDownstream;
    pSourceParam->pInputTimeWindow = &pSortMergeInfo->inputTimeWindow;
    if (taosArrayPush(pSortMergeInfo->pSourceParams, &pSourceParam) == NULL) {
      taosMemoryFree(pSourceParam);
      code = terrno;
      lino = __LINE__;
      goto _return;
    }

    SSortSource* ps = taosMemoryCalloc(1, sizeof(SSortSource));
    QUERY_CHECK_NULL(ps, code, lino, _return, terrno);

    ps->param = pSourceParam;
    ps->onlyRef = true;

    code = tsortAddSource(pSortMergeInfo->pSortHandle, ps);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(ps);
    }
    QUERY_CHECK_CODE(code, lino, _return);
  }

  code = tsortOpen(pSortMergeInfo->pSortHandle);
  QUERY_CHECK_CODE(code, lino, _return);

_return:
  if (code) {
    qError("%s failed to open sort merge operator, %s at line %d, code:%s", __func__, GET_TASKID(pTaskInfo), lino, tstrerror(code));
  }
  return code;
}

static int32_t doGetSortedBlockData(SMultiwayMergeOperatorInfo* pInfo, SSortHandle* pHandle, int32_t capacity,
                                 SSDataBlock* p, bool* newgroup) {
  SSortMergeInfo* pSortMergeInfo = &pInfo->sortMergeInfo;
  *newgroup = false;
  int32_t code = 0;

  while (1) {
    STupleHandle* pTupleHandle = NULL;
    if (pInfo->groupMerge || pInfo->inputWithGroupId) {
      if (pSortMergeInfo->prefetchedTuple == NULL) {
        pTupleHandle = NULL;
        code = tsortNextTuple(pHandle, &pTupleHandle);
        if (code) {
          // todo handle error
        }
      } else {
        pTupleHandle = pSortMergeInfo->prefetchedTuple;
        pSortMergeInfo->prefetchedTuple = NULL;
        uint64_t gid = 0, baseGid = 0;
        tsortGetGroupId(pTupleHandle, &gid, &baseGid);
        if (!pInfo->groupInitialized || gid != pInfo->groupId || baseGid != pInfo->baseGroupId) {
          *newgroup = true;
          pInfo->groupId = gid;
          pInfo->baseGroupId = baseGid;
          pInfo->groupInitialized = true;
        }
      }
    } else {
      code = tsortNextTuple(pHandle, &pTupleHandle);
      pInfo->groupId = 0;
    }

    if (pTupleHandle == NULL || (code != 0)) {
      break;
    }

    if (pInfo->groupMerge || pInfo->inputWithGroupId) {
      uint64_t tupleGroupId = 0, tupleBaseGid = 0;
      tsortGetGroupId(pTupleHandle, &tupleGroupId, &tupleBaseGid);
      if (!pInfo->groupInitialized || (pInfo->groupId == tupleGroupId && pInfo->baseGroupId == tupleBaseGid)) {
        code = appendOneRowToDataBlock(p, pTupleHandle);
        if (code) {
          return code;
        }

        p->info.id.groupId = tupleGroupId;
        p->info.id.baseGId = tupleBaseGid;
        pInfo->groupId = tupleGroupId;
        pInfo->baseGroupId = tupleBaseGid;
        pInfo->groupInitialized = true;
      } else {
        if (p->info.rows == 0) {
          code = appendOneRowToDataBlock(p, pTupleHandle);
          if (code) {
            return code;
          }

          p->info.id.groupId = pInfo->groupId = tupleGroupId;
          p->info.id.baseGId = tupleBaseGid;
          pInfo->baseGroupId = tupleBaseGid;
          pInfo->groupInitialized = true;
        } else {
          pSortMergeInfo->prefetchedTuple = pTupleHandle;
          break;
        }
      }
    } else {
      code = appendOneRowToDataBlock(p, pTupleHandle);
      if (code) {
        return code;
      }
    }

    if (p->info.rows >= capacity) {
      break;
    }
  }

  return code;
}

int32_t doSortMerge(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SSortMergeInfo*             pSortMergeInfo = &pInfo->sortMergeInfo;
  SSortHandle*                pHandle = pSortMergeInfo->pSortHandle;
  SSDataBlock*                pDataBlock = pInfo->binfo.pRes;
  SArray*                     pColMatchInfo = pSortMergeInfo->matchInfo.pList;
  int32_t                     capacity = pOperator->resultInfo.capacity;
  int32_t                     code = 0;
  bool                        newgroup = false;

  qDebug("start to merge final sorted rows, %s", GET_TASKID(pTaskInfo));
  blockDataCleanup(pDataBlock);

  if (pSortMergeInfo->pIntermediateBlock == NULL) {
    pSortMergeInfo->pIntermediateBlock = NULL;
    code = tsortGetSortedDataBlock(pHandle, &pSortMergeInfo->pIntermediateBlock);
    if (pSortMergeInfo->pIntermediateBlock == NULL || code != 0) {
      return code;
    }

    code = blockDataEnsureCapacity(pSortMergeInfo->pIntermediateBlock, capacity);
    if (code) {
      return code;
    }

  } else {
    blockDataCleanup(pSortMergeInfo->pIntermediateBlock);
  }

  SSDataBlock* p = pSortMergeInfo->pIntermediateBlock;
  while (1) {
    code = doGetSortedBlockData(pInfo, pHandle, capacity, p, &newgroup);
    if (code) {
      return code;
    }

    if (p->info.rows == 0) {
      break;
    }

    if (newgroup) {
      resetLimitInfoForNextGroup(&pInfo->limitInfo);
    }

    bool limitReached = applyLimitOffset(&pInfo->limitInfo, p, pTaskInfo);

    if (p->info.rows > 0) {
      break;
    }
  }

  if (p->info.rows > 0) {
    int32_t numOfCols = taosArrayGetSize(pColMatchInfo);
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColMatchItem* pmInfo = taosArrayGet(pColMatchInfo, i);
      if (pmInfo == NULL) {
        code = terrno;
        return code;
      }

      SColumnInfoData* pSrc = getDataBlockColBySlotId(p, pmInfo->srcSlotId, NULL);
      if (pSrc == NULL) {
        code = terrno;
        return code;
      }

      SColumnInfoData* pDst = getDataBlockColBySlotId(pDataBlock, pmInfo->dstSlotId, NULL);
      if (pDst == NULL) {
        code = terrno;
        return code;
      }

      code = colDataAssign(pDst, pSrc, p->info.rows, &pDataBlock->info);
      if (code) {
        return code;
      }
    }

    pDataBlock->info.rows = p->info.rows;
    pDataBlock->info.scanFlag = p->info.scanFlag;
    if (pInfo->ignoreGroupId) {
      pDataBlock->info.id.groupId = 0;
      pDataBlock->info.id.baseGId = p->info.id.baseGId;
    } else {
      pDataBlock->info.id.groupId = pInfo->groupId;
      pDataBlock->info.id.baseGId = p->info.id.baseGId;
    }
    pDataBlock->info.dataLoad = 1;
    if (pSortMergeInfo->inputTimeWindow.skey <= pSortMergeInfo->inputTimeWindow.ekey) {
      pDataBlock->info.window = pSortMergeInfo->inputTimeWindow;
    }
  }

  qDebug("%s get sorted block, groupId:0x%" PRIx64 " rows:%" PRId64 , GET_TASKID(pTaskInfo), pDataBlock->info.id.groupId,
         pDataBlock->info.rows);

  *pResBlock = (pDataBlock->info.rows > 0) ? pDataBlock : NULL;
  return code;
}

int32_t getSortMergeExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  SSortExecInfo* pSortExecInfo = taosMemoryCalloc(1, sizeof(SSortExecInfo));
  if (pSortExecInfo == NULL) {
    pOptr->pTaskInfo->code = terrno;
    return terrno;
  }

  SMultiwayMergeOperatorInfo* pInfo = (SMultiwayMergeOperatorInfo*)pOptr->info;
  SSortMergeInfo* pSortMergeInfo = &pInfo->sortMergeInfo;

  *pSortExecInfo = tsortGetSortExecInfo(pSortMergeInfo->pSortHandle);
  *pOptrExplain = pSortExecInfo;

  *len = sizeof(SSortExecInfo);
  return TSDB_CODE_SUCCESS;
}


void destroySortMergeOperatorInfo(void* param) {
  SSortMergeInfo* pSortMergeInfo = param;
  blockDataDestroy(pSortMergeInfo->pInputBlock);
  pSortMergeInfo->pInputBlock = NULL;

  blockDataDestroy(pSortMergeInfo->pIntermediateBlock);
  pSortMergeInfo->pIntermediateBlock = NULL;

  taosArrayDestroy(pSortMergeInfo->matchInfo.pList);

  tsortDestroySortHandle(pSortMergeInfo->pSortHandle);
  destroySortMergeSourceParams(pSortMergeInfo);
  taosArrayDestroy(pSortMergeInfo->pSortInfo);
}

#define NON_SORT_NEXT_SRC(_info, _idx) ((++(_idx) >= (_info)->sourceNum) ? ((_info)->sourceWorkIdx) : (_idx))

int32_t openNonSortMergeOperator(SOperatorInfo* pOperator) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SNonSortMergeInfo*          pNonSortMergeInfo = &pInfo->nsortMergeInfo;

  pNonSortMergeInfo->sourceWorkIdx = 0;
  SMergeOperatorParam* pMergeParam =
      NULL != pOperator->pOperatorGetParam ? pOperator->pOperatorGetParam->value : NULL;
  bool dynamicParam = NULL != pMergeParam && pMergeParam->activeSourcesOnly;
  pNonSortMergeInfo->detachParams = dynamicParam;
  pNonSortMergeInfo->sourceNum = 0;
  pNonSortMergeInfo->lastSourceIdx = -1;
  int32_t activeSources = dynamicParam ? 0 : pOperator->numOfDownstream;
  if (dynamicParam) {
    for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
      if (NULL != pOperator->pDownstreamGetParams &&
          NULL != pOperator->pDownstreamGetParams[i]) {
        ++activeSources;
      }
    }
  }
  if (activeSources > 0) {
    pNonSortMergeInfo->pSourceStatus =
        taosMemoryCalloc(activeSources, sizeof(*pNonSortMergeInfo->pSourceStatus));
    QUERY_CHECK_NULL(pNonSortMergeInfo->pSourceStatus, code, lino, _return, terrno);
  }

  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    if (dynamicParam &&
        (NULL == pOperator->pDownstreamGetParams ||
         NULL == pOperator->pDownstreamGetParams[i])) {
      continue;
    }
    pNonSortMergeInfo->pSourceStatus[pNonSortMergeInfo->sourceNum++] = i;
  }

  return code;

_return:
  qError("%s %s failed at line %d since %s", GET_TASKID(pTaskInfo), __func__, lino, tstrerror(code));
  pTaskInfo->code = code;
  return code;
}

int32_t doNonSortMerge(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  QRY_PARAM_CHECK(pResBlock);

  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SNonSortMergeInfo*          pNonSortMerge = &pInfo->nsortMergeInfo;
  SSDataBlock*                pBlock = NULL;
  SSDataBlock*                pRes = pInfo->binfo.pRes;
  int32_t                     code = 0;

  qDebug("start to merge no sorted rows, %s", GET_TASKID(pTaskInfo));

  int32_t idx = NON_SORT_NEXT_SRC(pNonSortMerge, pNonSortMerge->lastSourceIdx);
  while (idx < pNonSortMerge->sourceNum) {
    pBlock = pNonSortMerge->detachParams
                 ? getNextBlockFromDownstreamRemainDetach(pOperator, pNonSortMerge->pSourceStatus[idx])
                 : getNextBlockFromDownstream(pOperator, pNonSortMerge->pSourceStatus[idx]);
    if (NULL == pBlock) {
      TSWAP(pNonSortMerge->pSourceStatus[pNonSortMerge->sourceWorkIdx], pNonSortMerge->pSourceStatus[idx]);
      pNonSortMerge->sourceWorkIdx++;
      idx = NON_SORT_NEXT_SRC(pNonSortMerge, pNonSortMerge->lastSourceIdx);
      continue;
    }

    pNonSortMerge->lastSourceIdx = idx - 1;
    break;
  }

  if (!pBlock) {  // null data
    return code;
  }

  code = copyDataBlock(pRes, pBlock);
  *pResBlock = pRes;
  return code;
}

void destroyNonSortMergeOperatorInfo(void* param) {
  SNonSortMergeInfo* pNonSortMerge = param;
  taosMemoryFree(pNonSortMerge->pSourceStatus);
}

int32_t getNonSortMergeExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  return TSDB_CODE_SUCCESS;
}


int32_t openColsMergeOperator(SOperatorInfo* pOperator) {
  return TSDB_CODE_SUCCESS;
}

/*
 * Dynamic external windows emit one partial row for every requested window,
 * including a NULL placeholder when a source has no data in that window.
 * Interleave those already-aligned rows by window so the merge ExternalWindow
 * can consume all partial states for one window contiguously without a sort.
 */
static int32_t appendVtableAlignedPartialRows(SSDataBlock* pDst, SSDataBlock** pSources,
                                              int32_t sourceNum, int32_t windowNum) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  QUERY_CHECK_NULL(pDst, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pSources, code, lino, _return, TSDB_CODE_INVALID_PARA);

  int32_t activeSourceNum = 0;
  size_t  outputColNum = taosArrayGetSize(pDst->pDataBlock);
  for (int32_t sourceIdx = 0; sourceIdx < sourceNum; ++sourceIdx) {
    SSDataBlock* pSource = pSources[sourceIdx];
    if (pSource == NULL) {
      continue;
    }
    QUERY_CHECK_CONDITION(pSource->info.rows == windowNum, code, lino, _return,
                          TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
    QUERY_CHECK_CONDITION(taosArrayGetSize(pSource->pDataBlock) == outputColNum,
                          code, lino, _return, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
    for (int32_t colIdx = 0; colIdx < outputColNum; ++colIdx) {
      SColumnInfoData* pDstCol = taosArrayGet(pDst->pDataBlock, colIdx);
      SColumnInfoData* pSrcCol = taosArrayGet(pSource->pDataBlock, colIdx);
      QUERY_CHECK_NULL(pDstCol, code, lino, _return, terrno);
      QUERY_CHECK_NULL(pSrcCol, code, lino, _return, terrno);
      QUERY_CHECK_CONDITION(pDstCol->info.type == pSrcCol->info.type &&
                                pDstCol->info.bytes == pSrcCol->info.bytes,
                            code, lino, _return, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
    }
    ++activeSourceNum;
  }

  int64_t outputRowNum = (int64_t)windowNum * activeSourceNum;
  QUERY_CHECK_CONDITION(outputRowNum <= INT32_MAX, code, lino, _return,
                        TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  code = blockDataEnsureCapacity(pDst, (int32_t)outputRowNum);
  QUERY_CHECK_CODE(code, lino, _return);

  for (int32_t windowIdx = 0; windowIdx < windowNum; ++windowIdx) {
    for (int32_t sourceIdx = 0; sourceIdx < sourceNum; ++sourceIdx) {
      SSDataBlock* pSource = pSources[sourceIdx];
      if (pSource == NULL) {
        continue;
      }
      code = blockDataMergeNRows(pDst, pSource, windowIdx, 1);
      QUERY_CHECK_CODE(code, lino, _return);
    }
  }
  return code;

_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

typedef struct SVtableFedPrefetchTask {
  SOperatorInfo*  pOperator;
  SOperatorParam* pParam;
  TdThread        thread;
  int32_t         code;
  bool            started;
} SVtableFedPrefetchTask;

static bool getVtableFedPrefetchInput(SOperatorInfo* pExtWin, SOperatorParam* pExtParam,
                                      SOperatorInfo** ppFedScan, SOperatorParam** ppFedParam) {
  if (pExtWin == NULL || pExtParam == NULL || pExtWin->numOfDownstream != 1 ||
      pExtWin->pDownstream[0] == NULL ||
      pExtWin->pDownstream[0]->operatorType != QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN ||
      pExtParam->pChildren == NULL || taosArrayGetSize(pExtParam->pChildren) != 1) {
    return false;
  }

  SOperatorParam* pFedParam = taosArrayGetP(pExtParam->pChildren, 0);
  if (pFedParam == NULL || pFedParam->opType != QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN ||
      pFedParam->downstreamIdx == -1 || pFedParam->pChildren != NULL || pFedParam->value == NULL ||
      ((SForeignScanOperatorParam*)pFedParam->value)->paramType != FOREIGN_SCAN_PARAM_BATCH) {
    return false;
  }

  *ppFedScan = pExtWin->pDownstream[0];
  *ppFedParam = pFedParam;
  return true;
}

static void* vtableFedPrefetchThread(void* pData) {
  SVtableFedPrefetchTask* pTask = pData;
  pTask->code = federatedScanPrefetch(pTask->pOperator, pTask->pParam);
  return NULL;
}

static int32_t prefetchVtableFederatedSources(SOperatorInfo* pOperator) {
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SColsMergeInfo*             pColsMerge = &pInfo->colsMergeInfo;
  SVtableFedPrefetchTask*     pTasks = NULL;
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     candidateNum = 0;

  pTasks = taosMemoryCalloc(pColsMerge->sourceNum, sizeof(*pTasks));
  if (pTasks == NULL) {
    return terrno;
  }

  for (int32_t i = 0; i < pColsMerge->sourceNum; ++i) {
    if (pOperator->pDownstreamGetParams == NULL || pOperator->pDownstreamGetParams[i] == NULL) {
      continue;
    }
    if (getVtableFedPrefetchInput(pOperator->pDownstream[i],
                                  pOperator->pDownstreamGetParams[i],
                                  &pTasks[i].pOperator, &pTasks[i].pParam)) {
      ++candidateNum;
    }
  }

  if (candidateNum <= 1) {
    taosMemoryFree(pTasks);
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < pColsMerge->sourceNum; ++i) {
    if (pTasks[i].pOperator == NULL) {
      continue;
    }
    pTasks[i].code = taosThreadCreate(&pTasks[i].thread, NULL, vtableFedPrefetchThread, &pTasks[i]);
    if (pTasks[i].code != TSDB_CODE_SUCCESS) {
      code = pTasks[i].code;
      break;
    }
    pTasks[i].started = true;
  }

  for (int32_t i = 0; i < pColsMerge->sourceNum; ++i) {
    if (!pTasks[i].started) {
      continue;
    }
    int32_t joinCode = taosThreadJoin(pTasks[i].thread, NULL);
    if (code == TSDB_CODE_SUCCESS && joinCode != TSDB_CODE_SUCCESS) {
      code = joinCode;
    }
    if (code == TSDB_CODE_SUCCESS && pTasks[i].code != TSDB_CODE_SUCCESS) {
      code = pTasks[i].code;
      federatedScanPropagatePrefetchError(pTasks[i].pOperator, code);
    }
  }

  taosMemoryFree(pTasks);
  return code;
}

int32_t doColsMerge(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  QRY_PARAM_CHECK(pResBlock);

  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SSDataBlock*                pBlock = NULL;
  SColsMergeInfo*             pColsMerge = &pInfo->colsMergeInfo;
  int32_t                     code = 0;
  int32_t                     numOfRows = 0;
  int32_t                     lino = 0;
  STimeWindow                 timeWindow = {.skey = INT64_MAX, .ekey = INT64_MIN};
  bool                        allNull = true;
  bool                        isVtableMerge = false;
  bool                        mergeSameCols = false;
  SSDataBlock**               pSourceBlocks = NULL;

  if (pOperator->pOperatorGetParam) {
    if (pOperator->status == OP_EXEC_DONE) {
      pOperator->status = OP_OPENED;
    }
    SMergeOperatorParam* pParam = pOperator->pOperatorGetParam->value;
    numOfRows = pParam->winNum;
    mergeSameCols = pParam->mergeSameCols;
    freeOperatorParam(pOperator->pOperatorGetParam, OP_GET_PARAM);
    pOperator->pOperatorGetParam = NULL;
    isVtableMerge = true;
  } else {
    numOfRows = 1;
    isVtableMerge = false;
  }

  qDebug("start to merge columns, %s", GET_TASKID(pTaskInfo));
  blockDataCleanup(pInfo->binfo.pRes);
  int64_t maxOutputRows = mergeSameCols ? (int64_t)numOfRows * pColsMerge->sourceNum : numOfRows;
  QUERY_CHECK_CONDITION(maxOutputRows <= INT32_MAX, code, lino, _return,
                        TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  code = blockDataEnsureCapacity(pInfo->binfo.pRes, (int32_t)maxOutputRows);
  QUERY_CHECK_CODE(code, lino, _return);
  if (mergeSameCols) {
    pSourceBlocks = taosMemoryCalloc(pColsMerge->sourceNum, POINTER_BYTES);
    QUERY_CHECK_NULL(pSourceBlocks, code, lino, _return, terrno);
    code = prefetchVtableFederatedSources(pOperator);
    QUERY_CHECK_CODE(code, lino, _return);
  }

  for (int32_t i = 0; i < pColsMerge->sourceNum; ++i) {
    pBlock = NULL;
    if (isVtableMerge) {
      if (pOperator->pDownstreamGetParams != NULL && pOperator->pDownstreamGetParams[i] == NULL) {
        continue;
      }
      SOperatorInfo*  pExtWinOp = pOperator->pDownstream[i];
      SOperatorParam* pParam = pOperator->pDownstreamGetParams[i];
      code = pExtWinOp->fpSet.getNextExtFn(pExtWinOp, pParam, &pBlock);
      setOperatorCompleted(pExtWinOp);
      pOperator->pDownstreamGetParams[i] = NULL;
      QUERY_CHECK_CODE(code, lino, _return);
      if (pBlock) {
        printDataBlock(pBlock, __func__, "colsMerge", pTaskInfo->id.queryId);
        if (mergeSameCols) {
          pSourceBlocks[i] = pBlock;
        } else {
          code = copyColumnsValue(pColsMerge->pTargets, pColsMerge->srcBlkIds[i],
                                  pInfo->binfo.pRes, pBlock, numOfRows);
          QUERY_CHECK_CODE(code, lino, _return);
        }

        timeWindow.skey = TMIN(timeWindow.skey, pBlock->info.window.skey);
        timeWindow.ekey = TMAX(timeWindow.ekey, pBlock->info.window.ekey);
        if (allNull) {
          pInfo->binfo.pRes->info.id.groupId = pBlock->info.id.groupId;
          pInfo->binfo.pRes->info.id.baseGId = pBlock->info.id.baseGId;
          pInfo->binfo.pRes->info.scanFlag = pBlock->info.scanFlag;
        }
        allNull = false;
      } else if (!mergeSameCols) {
        code = copyColumnsValue(pColsMerge->pTargets, pColsMerge->srcBlkIds[i],
                                pInfo->binfo.pRes, NULL, numOfRows);
        QUERY_CHECK_CODE(code, lino, _return);
      }
    } else {
      pBlock = getNextBlockFromDownstream(pOperator, i);
      if (pBlock) {
        code = copyColumnsValue(pColsMerge->pTargets, pColsMerge->srcBlkIds[i], pInfo->binfo.pRes, pBlock, numOfRows);
        QUERY_CHECK_CODE(code, lino, _return);
        allNull = false;
      }
    }
  }

  setOperatorCompleted(pOperator);
  if (allNull) {
    taosMemoryFree(pSourceBlocks);
    return code;
  }

  if (mergeSameCols) {
    code = appendVtableAlignedPartialRows(pInfo->binfo.pRes, pSourceBlocks,
                                          pColsMerge->sourceNum, numOfRows);
    QUERY_CHECK_CODE(code, lino, _return);
  } else {
    pInfo->binfo.pRes->info.rows = numOfRows;
  }

  pInfo->binfo.pRes->info.window.skey = timeWindow.skey;
  pInfo->binfo.pRes->info.window.ekey = timeWindow.ekey;
  pInfo->binfo.pRes->info.dataLoad = 1;
  *pResBlock = pInfo->binfo.pRes;
  taosMemoryFree(pSourceBlocks);

  return code;
_return:
  taosMemoryFree(pSourceBlocks);
  qError("failed to merge columns, line:%d code:%s", lino, tstrerror(code));
  return code;
}

void destroyColsMergeOperatorInfo(void* param) {
  SColsMergeInfo* pColsMergeInfo = param;
  taosMemoryFreeClear(pColsMergeInfo->srcBlkIds);
}

int32_t getColsMergeExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  return TSDB_CODE_SUCCESS;
}


SOperatorFpSet gMultiwayMergeFps[MERGE_TYPE_MAX_VALUE] = {
  {0},
  {._openFn = openSortMergeOperator, .getNextFn = doSortMerge, .closeFn = destroySortMergeOperatorInfo, .getExplainFn = getSortMergeExplainExecInfo},
  {._openFn = openNonSortMergeOperator, .getNextFn = doNonSortMerge, .closeFn = destroyNonSortMergeOperatorInfo, .getExplainFn = getNonSortMergeExplainExecInfo},
  {._openFn = openColsMergeOperator, .getNextFn = doColsMerge, .closeFn = destroyColsMergeOperatorInfo, .getExplainFn = getColsMergeExplainExecInfo},
};


int32_t openMultiwayMergeOperator(SOperatorInfo* pOperator) {
  int32_t code = 0;
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;

  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  if (NULL != gMultiwayMergeFps[pInfo->type]._openFn) {
    code = (*gMultiwayMergeFps[pInfo->type]._openFn)(pOperator);
  }

  pOperator->status = OP_RES_TO_RETURN;

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, terrno);
  }

  OPTR_SET_OPENED(pOperator);
  return code;
}

int32_t doMultiwayMerge(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  QRY_PARAM_CHECK(pResBlock);
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (pOperator->status == OP_EXEC_DONE && !pOperator->pOperatorGetParam) {
    return 0;
  }

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SMergeOperatorParam* pMergeParam =
      NULL != pOperator->pOperatorGetParam ? pOperator->pOperatorGetParam->value : NULL;
  bool scopedRowMerge = MERGE_TYPE_NON_SORT == pInfo->type && NULL != pMergeParam &&
                        pMergeParam->activeSourcesOnly;

  if (scopedRowMerge && OPTR_IS_OPENED(pOperator)) {
    code = resetMultiwayMergeOperState(pOperator);
    QUERY_CHECK_CODE(code, lino, _return);
  }

  code = pOperator->fpSet._openFn(pOperator);
  QUERY_CHECK_CODE(code, lino, _return);

  if (scopedRowMerge) {
    freeOperatorParam(pOperator->pOperatorGetParam, OP_GET_PARAM);
    pOperator->pOperatorGetParam = NULL;
  }

  if (NULL != gMultiwayMergeFps[pInfo->type].getNextFn) {
    code = (*gMultiwayMergeFps[pInfo->type].getNextFn)(pOperator, pResBlock);
    QUERY_CHECK_CODE(code, lino, _return);
  }

  /*
    Sort-merge pulls its input through the sort handle's fetch callback
    (sortMergeloadNextDataBlock), which bypasses getNextBlockFromDownstream and
    therefore never records input_wait the way other operators do.  Reclassify
    the time spent fetching raw data from downstream as input_wait, and remove it
    from this operator's own compute (execElapsed) so the metrics stay consistent
    with the rest of the pipeline.  recordOpExecEnd (called by the wrapper after
    this function returns) will add this call's full slice to execElapsed, so the
    subtraction here nets out to "compute excluding downstream fetch".

    Since this block is the only writer of inputWaitElapsed for the sort-merge
    type, the counter always equals the last-seen cumulative fetch time — no
    separate baseline needs to be stored, and the accounting stays correct
    across an operator reset (sort handle recreated and cost cleared together).
  */
  if (pInfo->type == MERGE_TYPE_SORT && QUERY_ENABLE_EXPLAIN(pTaskInfo)) {
    int64_t cur = tsortGetFetchRawDataTime(pInfo->sortMergeInfo.pSortHandle);
    int64_t delta = cur - pOperator->cost.inputWaitElapsed;
    pOperator->cost.inputWaitElapsed = cur;
    pOperator->cost.execElapsed -= delta;
  }

  if ((*pResBlock) != NULL) {
    code = blockDataCheck(*pResBlock);
    QUERY_CHECK_CODE(code, lino, _return);
  } else {
    setOperatorCompleted(pOperator);
  }

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", GET_TASKID(pTaskInfo), __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

void destroyMultiwayMergeOperatorInfo(void* param) {
  SMultiwayMergeOperatorInfo* pInfo = (SMultiwayMergeOperatorInfo*)param;
  blockDataDestroy(pInfo->binfo.pRes);
  pInfo->binfo.pRes = NULL;

  if (NULL != gMultiwayMergeFps[pInfo->type].closeFn) {
    (*gMultiwayMergeFps[pInfo->type].closeFn)(&pInfo->sortMergeInfo);
  }

  taosMemoryFreeClear(param);
}

int32_t getMultiwayMergeExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  int32_t code = 0;
  SMultiwayMergeOperatorInfo* pInfo = (SMultiwayMergeOperatorInfo*)pOptr->info;

  if (NULL != gMultiwayMergeFps[pInfo->type].getExplainFn) {
    code = (*gMultiwayMergeFps[pInfo->type].getExplainFn)(pOptr, pOptrExplain, len);
  }

  return code;
}

static int32_t resetMultiwayMergeOperState(SOperatorInfo* pOper) {
  SMultiwayMergeOperatorInfo* pInfo = pOper->info;
  SExecTaskInfo*           pTaskInfo = pOper->pTaskInfo;
  SMergePhysiNode* pPhynode = (SMergePhysiNode*)pOper->pPhyNode;
  pOper->status = OP_NOT_OPENED;

  resetBasicOperatorState(&pInfo->binfo);
  pInfo->groupId = 0;
  pInfo->baseGroupId = 0;
  pInfo->groupInitialized = false;

  OPTR_CLR_OPENED(pOper);

  switch (pInfo->type) {
    case MERGE_TYPE_SORT: {

      SSortMergeInfo* pSortMergeInfo = &pInfo->sortMergeInfo;

      blockDataCleanup(pSortMergeInfo->pInputBlock);

      blockDataDestroy(pSortMergeInfo->pIntermediateBlock);
      pSortMergeInfo->pIntermediateBlock = NULL;

      tsortDestroySortHandle(pSortMergeInfo->pSortHandle);
      pSortMergeInfo->pSortHandle = NULL;
      destroySortMergeSourceParams(pSortMergeInfo);
      pSortMergeInfo->prefetchedTuple = NULL;

      pInfo->limitInfo = (SLimitInfo){0};
      initLimitInfo(pPhynode->node.pLimit, pPhynode->node.pSlimit, &pInfo->limitInfo);
      break;
    }
    case MERGE_TYPE_NON_SORT: {
      taosMemoryFree(pInfo->nsortMergeInfo.pSourceStatus);
      pInfo->nsortMergeInfo = (SNonSortMergeInfo){0};
      break;
    }
    case MERGE_TYPE_COLUMNS: {
      break;
    }
    default:
      qError("Invalid merge type: %d", pInfo->type);
  }

  return 0;
}

int32_t createMultiwayMergeOperatorInfo(SOperatorInfo** downStreams, size_t numStreams, SMergePhysiNode* pMergePhyNode,
                                        SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  SPhysiNode*                 pPhyNode = (SPhysiNode*)pMergePhyNode;
  int32_t                     lino = 0;
  int32_t                     code = TSDB_CODE_SUCCESS;
  SMultiwayMergeOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SMultiwayMergeOperatorInfo));
  SOperatorInfo*              pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  SDataBlockDescNode*         pDescNode = pPhyNode->pOutputDataBlockDesc;
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }
  initOperatorCostInfo(pOperator);

  pOperator->pPhyNode = pPhyNode;
  pInfo->groupMerge = pMergePhyNode->groupSort;
  pInfo->ignoreGroupId = pMergePhyNode->ignoreGroupId;
  setOptrBasicInfoOrder(&pInfo->binfo, &pMergePhyNode->node);
  pInfo->inputWithGroupId = pMergePhyNode->inputWithGroupId;

  pInfo->type = pMergePhyNode->type;
  switch (pInfo->type) {
    case MERGE_TYPE_SORT: {
      SSortMergeInfo* pSortMergeInfo = &pInfo->sortMergeInfo;
      initLimitInfo(pMergePhyNode->node.pLimit, pMergePhyNode->node.pSlimit, &pInfo->limitInfo);
      pInfo->binfo.pRes = createDataBlockFromDescNode(pDescNode);
      TSDB_CHECK_NULL(pInfo->binfo.pRes, code, lino, _error, terrno);

      SPhysiNode*  pChildNode = (SPhysiNode*)nodesListGetNode(pPhyNode->pChildren, 0);
      SSDataBlock* pInputBlock = createDataBlockFromDescNode(pChildNode->pOutputDataBlockDesc);
      TSDB_CHECK_NULL(pInputBlock, code, lino, _error, terrno);
      pSortMergeInfo->pInputBlock = pInputBlock;

      initResultSizeInfo(&pOperator->resultInfo, 1024);
      code = blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
      TSDB_CHECK_CODE(code, lino, _error);

      size_t  numOfCols = taosArrayGetSize(pInfo->binfo.pRes->pDataBlock);
      int32_t rowSize = pInfo->binfo.pRes->info.rowSize;
      int32_t numOfOutputCols = 0;
      pSortMergeInfo->pSortInfo = createSortInfo(pMergePhyNode->pMergeKeys);
      pSortMergeInfo->bufPageSize = getProperSortPageSize(rowSize, numOfCols);
      pSortMergeInfo->sortBufSize =
          pSortMergeInfo->bufPageSize * (numStreams + 1);  // one additional is reserved for merged result.
      code = extractColMatchInfo(pMergePhyNode->pTargets, pDescNode, &numOfOutputCols, COL_MATCH_FROM_SLOT_ID,
                                 &pSortMergeInfo->matchInfo);
      if (code != TSDB_CODE_SUCCESS) {
        goto _error;
      }
      break;
    }
    case MERGE_TYPE_NON_SORT: {
      SNonSortMergeInfo* pNonSortMerge = &pInfo->nsortMergeInfo;
      pInfo->binfo.pRes = createDataBlockFromDescNode(pDescNode);
      TSDB_CHECK_NULL(pInfo->binfo.pRes, code, lino, _error, terrno);

      initResultSizeInfo(&pOperator->resultInfo, 1024);
      code = blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
      TSDB_CHECK_CODE(code, lino, _error);

      break;
    }
    case MERGE_TYPE_COLUMNS: {
      SColsMergeInfo* pColsMerge = &pInfo->colsMergeInfo;
      pInfo->binfo.pRes = createDataBlockFromDescNode(pDescNode);
      TSDB_CHECK_NULL(pInfo->binfo.pRes, code, lino, _error, terrno);

      initResultSizeInfo(&pOperator->resultInfo, 1);
      code = blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
      TSDB_CHECK_CODE(code, lino, _error);

      pColsMerge->pTargets = pMergePhyNode->pTargets;
      pColsMerge->sourceNum = numStreams;
      pColsMerge->srcBlkIds = taosMemoryCalloc(numStreams, sizeof(int64_t));
      for (size_t i = 0; i < numStreams; ++i) {
        pColsMerge->srcBlkIds[i] = getOperatorResultBlockId(downStreams[i], 0);
      }
      break;
    }
    default:
      qError("Invalid merge type: %d", pInfo->type);
      code = TSDB_CODE_INVALID_PARA;
      goto _error;
  }

  setOperatorInfo(pOperator, "MultiwayMergeOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet =
      createOperatorFpSet(openMultiwayMergeOperator, doMultiwayMerge, NULL, destroyMultiwayMergeOperatorInfo,
                          optrDefaultBufFn, getMultiwayMergeExplainExecInfo, optrDefaultGetNextExtFn, NULL);

  setOperatorResetStateFn(pOperator, resetMultiwayMergeOperState);
  code = appendDownstream(pOperator, downStreams, numStreams);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) {
    destroyMultiwayMergeOperatorInfo(pInfo);
  }
  pTaskInfo->code = code;
  destroyOperatorAndDownstreams(pOperator, downStreams, numStreams);
  return code;
}
