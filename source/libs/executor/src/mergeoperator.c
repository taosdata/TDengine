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

typedef struct SSortMergeSource {
  SOperatorInfo* pOperator;
  int32_t        sourceIndex;
  uint64_t       srcOuterGroupId;
  int8_t         srcPhase;
  SSDataBlock*   pHeldBlock;
  SSDataBlock*   pActiveHeldBlock;
  bool           exhausted;
} SSortMergeSource;

typedef struct SSortMergeInfo {
  SArray*           pSortInfo;
  SSortHandle*      pSortHandle;
  STupleHandle*     prefetchedTuple;
  int32_t           bufPageSize;
  uint32_t          sortBufSize;  // max buffer size for in-memory sort
  SSDataBlock*      pIntermediateBlock;
  SSDataBlock*      pInputBlock;
  SColMatchInfo     matchInfo;
  SSortMergeSource* pSources;
  int32_t           numOfSources;
  uint64_t          currentOuterGroupId;
  int8_t            currentPhase;
  bool              hasCurrentOuterGroup;
  STimeWindow       inputTimeWindow;
  int64_t           fetchRawDataTime;
  SSortExecInfo     sortExecInfo;
} SSortMergeInfo;

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
  bool           outerGroupPhaseMode;
} SMultiwayMergeOperatorInfo;

static int32_t doSortMerge(SOperatorInfo* pOperator, SSDataBlock** pResBlock);
static int32_t doMultiwayMerge(SOperatorInfo* pOperator, SSDataBlock** pResBlock);
static int32_t doNonSortMerge(SOperatorInfo* pOperator, SSDataBlock** pResBlock);
static int32_t resetMultiwayMergeOperState(SOperatorInfo* pOper);
static int32_t doColsMerge(SOperatorInfo* pOperator, SSDataBlock** pResBlock);
static int32_t sortMergeloadNextDataBlock(void* param, SSDataBlock** ppBlock);

static int32_t sortMergeCompareCursor(uint64_t leftOuterGroupId,
                                      int8_t leftPhase,
                                      uint64_t rightOuterGroupId,
                                      int8_t rightPhase) {
  if (leftOuterGroupId < rightOuterGroupId) {
    return -1;
  }
  if (leftOuterGroupId > rightOuterGroupId) {
    return 1;
  }
  if (leftPhase < rightPhase) {
    return -1;
  }
  if (leftPhase > rightPhase) {
    return 1;
  }
  return 0;
}

static int32_t sortMergeloadNextDataBlock(void* param, SSDataBlock** ppBlock) {
  SSortMergeSource* pSource = param;
  if (pSource == NULL || pSource->pOperator == NULL || ppBlock == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SOperatorInfo*              pOperator = pSource->pOperator;
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SSortMergeInfo*             pMerge = &pInfo->sortMergeInfo;
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  *ppBlock = NULL;

  blockDataDestroy(pSource->pActiveHeldBlock);
  pSource->pActiveHeldBlock = NULL;

  if (pSource->pHeldBlock != NULL) {
    int8_t phase = pSource->pHeldBlock->info.scanPhase;
    if (pInfo->outerGroupPhaseMode) {
      if (!pMerge->hasCurrentOuterGroup) {
        // The initial sort open only seeds one held cursor per source. The
        // smallest cursor is selected before a phase handle consumes it.
        return code;
      }
      uint64_t outerGroupId =
          pSource->pHeldBlock->info.id.groupId;
      int32_t compare = sortMergeCompareCursor(
          outerGroupId, phase, pMerge->currentOuterGroupId,
          pMerge->currentPhase);
      if (compare < 0) {
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        lino = __LINE__;
        goto _return;
      }
      if (compare > 0) {
        return code;
      }
    } else {
      if (phase < pMerge->currentPhase) {
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        lino = __LINE__;
        goto _return;
      }
      if (phase > pMerge->currentPhase) {
        return code;
      }
    }

    pSource->pActiveHeldBlock = pSource->pHeldBlock;
    pSource->pHeldBlock = NULL;
    *ppBlock = pSource->pActiveHeldBlock;
    return code;
  }

  if (pSource->exhausted) {
    return code;
  }

  SOperatorInfo* pDownstream = pOperator->pDownstream[pSource->sourceIndex];
  if (pOperator->pDownstreamGetParams != NULL &&
      pOperator->pDownstreamGetParams[pSource->sourceIndex] != NULL) {
    SOperatorParam* pGetParam =
        pOperator->pDownstreamGetParams[pSource->sourceIndex];
    pOperator->pDownstreamGetParams[pSource->sourceIndex] = NULL;
    pGetParam->reUse = false;
    if (pDownstream->fpSet.getNextExtFn == NULL) {
      freeOperatorParam(pGetParam, OP_GET_PARAM);
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      lino = __LINE__;
      goto _return;
    }
    code = pDownstream->fpSet.getNextExtFn(pDownstream, pGetParam, ppBlock);
  } else {
    code = pDownstream->fpSet.getNextFn(pDownstream, ppBlock);
  }
  QUERY_CHECK_CODE(code, lino, _return);
  if (*ppBlock == NULL) {
    pSource->exhausted = true;
    return code;
  }

  code = blockDataCheck(*ppBlock);
  QUERY_CHECK_CODE(code, lino, _return);

  if ((*ppBlock)->info.window.skey <= (*ppBlock)->info.window.ekey) {
    pMerge->inputTimeWindow.skey =
        TMIN(pMerge->inputTimeWindow.skey, (*ppBlock)->info.window.skey);
    pMerge->inputTimeWindow.ekey =
        TMAX(pMerge->inputTimeWindow.ekey, (*ppBlock)->info.window.ekey);
  }

  int8_t phase = (*ppBlock)->info.scanPhase;
  if (phase < SCAN_PHASE_MAIN || phase > SCAN_PHASE_PREV) {
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    lino = __LINE__;
    goto _return;
  }

  bool holdBlock = false;
  if (pInfo->outerGroupPhaseMode) {
    uint64_t outerGroupId = (*ppBlock)->info.id.groupId;
    // srcPhase starts at -1, so the first block of a source never regresses.
    if (sortMergeCompareCursor(outerGroupId, phase,
                               pSource->srcOuterGroupId,
                               pSource->srcPhase) < 0) {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      lino = __LINE__;
      goto _return;
    }
    pSource->srcOuterGroupId = outerGroupId;
    pSource->srcPhase = phase;

    if (!pMerge->hasCurrentOuterGroup) {
      holdBlock = true;
    } else {
      int32_t compare = sortMergeCompareCursor(
          outerGroupId, phase, pMerge->currentOuterGroupId,
          pMerge->currentPhase);
      if (compare < 0) {
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        lino = __LINE__;
        goto _return;
      }
      holdBlock = compare > 0;
    }
  } else {
    if (phase < pSource->srcPhase || phase < pMerge->currentPhase) {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      lino = __LINE__;
      goto _return;
    }
    pSource->srcPhase = phase;
    holdBlock = phase > pMerge->currentPhase;
  }

  if (holdBlock) {
    code = createOneDataBlock(*ppBlock, true, &pSource->pHeldBlock);
    QUERY_CHECK_CODE(code, lino, _return);
    qDebug("%s merge source:%d holds group:0x%" PRIx64
           " phase:%d at group:0x%" PRIx64 " phase:%d",
           GET_TASKID(pOperator->pTaskInfo), pSource->sourceIndex,
           (*ppBlock)->info.id.groupId, phase,
           pMerge->currentOuterGroupId, pMerge->currentPhase);
    *ppBlock = NULL;
  }

  return code;

_return:
  qError("%s failed to load source:%d for %s at line:%d, code:%s",
         __func__, pSource->sourceIndex,
         GET_TASKID(pOperator->pTaskInfo), lino, tstrerror(code));
  return code;
}

static void accumulateSortExecInfo(SSortExecInfo* pDst,
                                   const SSortExecInfo* pSrc) {
  pDst->sortMethod = pSrc->sortMethod;
  pDst->sortBuffer = pSrc->sortBuffer;
  pDst->loops += pSrc->loops;
  pDst->writeBytes += pSrc->writeBytes;
  pDst->readBytes += pSrc->readBytes;
}

static void finishSortMergeHandle(SSortMergeInfo* pMerge) {
  if (pMerge->pSortHandle == NULL) {
    return;
  }

  SSortExecInfo info = tsortGetSortExecInfo(pMerge->pSortHandle);
  accumulateSortExecInfo(&pMerge->sortExecInfo, &info);
  pMerge->fetchRawDataTime +=
      tsortGetFetchRawDataTime(pMerge->pSortHandle);
  tsortDestroySortHandle(pMerge->pSortHandle);
  pMerge->pSortHandle = NULL;
}

static int32_t rebuildSortMergeInfo(SOperatorInfo* pOperator) {
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SSortMergeInfo*             pMerge = &pInfo->sortMergeInfo;
  SMergePhysiNode*            pMergeNode =
      (SMergePhysiNode*)pOperator->pPhyNode;
  SArray*                     pSortInfo =
      createSortInfo(pMergeNode->pMergeKeys);
  if (pSortInfo == NULL) {
    return terrno;
  }

  if (pMerge->currentPhase == SCAN_PHASE_PREV) {
    for (int32_t i = 0; i < taosArrayGetSize(pSortInfo); ++i) {
      SBlockOrderInfo* pOrder = taosArrayGet(pSortInfo, i);
      if (pOrder == NULL) {
        taosArrayDestroy(pSortInfo);
        return terrno;
      }
      pOrder->order = TSDB_ORDER_DESC;
      pOrder->nullFirst = false;
      pOrder->compFn = NULL;
    }
  }

  taosArrayDestroy(pMerge->pSortInfo);
  pMerge->pSortInfo = pSortInfo;
  return TSDB_CODE_SUCCESS;
}

static int32_t createSortMergePhaseHandle(SOperatorInfo* pOperator) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SSortMergeInfo*             pMerge = &pInfo->sortMergeInfo;
  int32_t numOfBufPage =
      (int32_t)pMerge->sortBufSize / pMerge->bufPageSize;

  code = tsortCreateSortHandle(
      pMerge->pSortInfo, SORT_MULTISOURCE_MERGE, pMerge->bufPageSize,
      numOfBufPage, pMerge->pInputBlock, pTaskInfo->id.str, 0, 0, 0,
      &pMerge->pSortHandle);
  QUERY_CHECK_CODE(code, lino, _return);

  tsortSetFetchRawDataFp(pMerge->pSortHandle,
                         sortMergeloadNextDataBlock, NULL, NULL);
  const SStreamRuntimeFuncInfo* pStream = GET_STM_RTINFO(pTaskInfo);
  bool compareGroupId =
      pInfo->groupMerge ||
      (pInfo->inputWithGroupId && pStream != NULL &&
       pStream->isMultiGroupCalc);
  tsortSetCompareGroupId(pMerge->pSortHandle, compareGroupId);

  for (int32_t i = 0; i < pMerge->numOfSources; ++i) {
    SSortSource* pSortSource =
        taosMemoryCalloc(1, sizeof(SSortSource));
    QUERY_CHECK_NULL(pSortSource, code, lino, _return, terrno);
    pSortSource->param = &pMerge->pSources[i];
    pSortSource->onlyRef = true;

    code = tsortAddSource(pMerge->pSortHandle, pSortSource);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(pSortSource);
      lino = __LINE__;
      goto _return;
    }
  }

  code = tsortOpen(pMerge->pSortHandle);
  QUERY_CHECK_CODE(code, lino, _return);
  SBlockOrderInfo* pOrder = taosArrayGet(pMerge->pSortInfo, 0);
  const char* order = pOrder != NULL &&
                              pOrder->order == TSDB_ORDER_DESC
                          ? "desc"
                          : "asc";
  qDebug("%s opened merge phase:%d order:%s sources:%d",
         GET_TASKID(pTaskInfo), pMerge->currentPhase, order,
         pMerge->numOfSources);

_return:
  if (code != TSDB_CODE_SUCCESS) {
    finishSortMergeHandle(pMerge);
    qError("%s failed to open merge phase:%d for %s at line:%d, code:%s",
           __func__, pMerge->currentPhase, GET_TASKID(pTaskInfo), lino,
           tstrerror(code));
  }
  return code;
}

int32_t openSortMergeOperator(SOperatorInfo* pOperator) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SSortMergeInfo*             pMerge = &pInfo->sortMergeInfo;

  pMerge->inputTimeWindow =
      (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MIN};

  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SOperatorInfo*    pDownstream = pOperator->pDownstream[i];
    SSortMergeSource* pSource = &pMerge->pSources[i];
    if (NULL != pOperator->pOperatorGetParam &&
        NULL != pOperator->pDownstreamGetParams &&
        NULL == pOperator->pDownstreamGetParams[i]) {
      pSource->exhausted = true;
      continue;
    }

    pSource->exhausted = false;
    if (pDownstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
      code = pDownstream->fpSet._openFn(pDownstream);
      QUERY_CHECK_CODE(code, lino, _return);
      if (pOperator->pDownstreamGetParams && pOperator->pDownstreamGetParams[i]) {
        pDownstream->pOperatorGetParam = pOperator->pDownstreamGetParams[i];
        pOperator->pDownstreamGetParams[i] = NULL;
      }
    } else if (pOperator->pDownstreamGetParams &&
               pOperator->pDownstreamGetParams[i]) {
      code = setOperatorParams(pDownstream,
                               pOperator->pDownstreamGetParams[i],
                               OP_GET_PARAM);
      QUERY_CHECK_CODE(code, lino, _return);
      pOperator->pDownstreamGetParams[i] = NULL;
    }
  }

  code = createSortMergePhaseHandle(pOperator);
  QUERY_CHECK_CODE(code, lino, _return);

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed to open sort merge for %s at line:%d, code:%s",
           __func__, GET_TASKID(pTaskInfo), lino, tstrerror(code));
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
          // The previous group may end exactly at the output block boundary.
          *newgroup = true;
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

static int32_t doSortMergePhase(SOperatorInfo* pOperator,
                                SSDataBlock** pResBlock) {
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
    pDataBlock->info.scanPhase = pSortMergeInfo->currentPhase;
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

  qDebug("%s merged phase:%d groupId:0x%" PRIx64 " rows:%" PRId64,
         GET_TASKID(pTaskInfo), pDataBlock->info.scanPhase,
         pDataBlock->info.id.groupId, pDataBlock->info.rows);

  *pResBlock = (pDataBlock->info.rows > 0) ? pDataBlock : NULL;
  return code;
}

static bool sortMergeHasHeldBlock(const SSortMergeInfo* pMerge) {
  for (int32_t i = 0; i < pMerge->numOfSources; ++i) {
    if (pMerge->pSources[i].pHeldBlock != NULL) {
      return true;
    }
  }
  return false;
}

static bool sortMergeFindNextHeldCursor(const SSortMergeInfo* pMerge,
                                        uint64_t* pOuterGroupId,
                                        int8_t* pPhase) {
  bool found = false;
  for (int32_t i = 0; i < pMerge->numOfSources; ++i) {
    SSDataBlock* pHeld = pMerge->pSources[i].pHeldBlock;
    if (pHeld == NULL) {
      continue;
    }
    uint64_t outerGroupId = pHeld->info.id.groupId;
    int8_t phase = pHeld->info.scanPhase;
    if (!found || sortMergeCompareCursor(
                      outerGroupId, phase, *pOuterGroupId, *pPhase) < 0) {
      *pOuterGroupId = outerGroupId;
      *pPhase = phase;
      found = true;
    }
  }
  return found;
}

static int32_t advanceSortMergePhase(SOperatorInfo* pOperator,
                                     bool* pAdvanced) {
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SSortMergeInfo*             pMerge = &pInfo->sortMergeInfo;
  int32_t                     code = TSDB_CODE_SUCCESS;
  *pAdvanced = false;

  uint64_t nextOuterGroupId = pMerge->currentOuterGroupId;
  int8_t nextPhase = pMerge->currentPhase;
  if (pInfo->outerGroupPhaseMode) {
    if (!sortMergeFindNextHeldCursor(
            pMerge, &nextOuterGroupId, &nextPhase)) {
      return code;
    }
    if (pMerge->hasCurrentOuterGroup &&
        sortMergeCompareCursor(nextOuterGroupId, nextPhase,
                               pMerge->currentOuterGroupId,
                               pMerge->currentPhase) <= 0) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
  } else {
    if (pMerge->currentPhase >= SCAN_PHASE_PREV ||
        !sortMergeHasHeldBlock(pMerge)) {
      return code;
    }
    nextPhase = (int8_t)(pMerge->currentPhase + 1);
  }

  // Only PREV reads DESC, so the sort order is rebuilt when the cursor
  // enters or leaves PREV. Without a current group the old side is ASC.
  bool wasDesc = pMerge->hasCurrentOuterGroup &&
                 pMerge->currentPhase == SCAN_PHASE_PREV;
  bool rebuildOrder = wasDesc != (nextPhase == SCAN_PHASE_PREV);

  finishSortMergeHandle(pMerge);
  blockDataDestroy(pMerge->pIntermediateBlock);
  pMerge->pIntermediateBlock = NULL;
  pMerge->prefetchedTuple = NULL;
  pInfo->groupId = 0;
  pMerge->currentOuterGroupId = nextOuterGroupId;
  pMerge->currentPhase = nextPhase;
  pMerge->hasCurrentOuterGroup = true;

  if (rebuildOrder) {
    code = rebuildSortMergeInfo(pOperator);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  code = createSortMergePhaseHandle(pOperator);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  *pAdvanced = true;
  return code;
}

int32_t doSortMerge(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  int32_t code = TSDB_CODE_SUCCESS;
  *pResBlock = NULL;

  while (true) {
    code = doSortMergePhase(pOperator, pResBlock);
    if (code != TSDB_CODE_SUCCESS || *pResBlock != NULL) {
      return code;
    }

    bool advanced = false;
    code = advanceSortMergePhase(pOperator, &advanced);
    if (code != TSDB_CODE_SUCCESS || !advanced) {
      if (code == TSDB_CODE_SUCCESS) {
        qDebug("%s merge phases complete final-null phase:%d",
               GET_TASKID(pOperator->pTaskInfo),
               pInfo->sortMergeInfo.currentPhase);
      }
      return code;
    }
  }
}

int32_t getSortMergeExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  SSortExecInfo* pSortExecInfo = taosMemoryCalloc(1, sizeof(SSortExecInfo));
  if (pSortExecInfo == NULL) {
    pOptr->pTaskInfo->code = terrno;
    return terrno;
  }

  SMultiwayMergeOperatorInfo* pInfo = (SMultiwayMergeOperatorInfo*)pOptr->info;
  SSortMergeInfo* pSortMergeInfo = &pInfo->sortMergeInfo;

  *pSortExecInfo = pSortMergeInfo->sortExecInfo;
  if (pSortMergeInfo->pSortHandle != NULL) {
    SSortExecInfo current =
        tsortGetSortExecInfo(pSortMergeInfo->pSortHandle);
    accumulateSortExecInfo(pSortExecInfo, &current);
  }
  *pOptrExplain = pSortExecInfo;

  *len = sizeof(SSortExecInfo);
  return TSDB_CODE_SUCCESS;
}


void destroySortMergeOperatorInfo(void* param) {
  SSortMergeInfo* pSortMergeInfo = param;
  finishSortMergeHandle(pSortMergeInfo);

  for (int32_t i = 0; i < pSortMergeInfo->numOfSources; ++i) {
    blockDataDestroy(pSortMergeInfo->pSources[i].pHeldBlock);
    blockDataDestroy(pSortMergeInfo->pSources[i].pActiveHeldBlock);
  }
  taosMemoryFreeClear(pSortMergeInfo->pSources);

  blockDataDestroy(pSortMergeInfo->pInputBlock);
  pSortMergeInfo->pInputBlock = NULL;

  blockDataDestroy(pSortMergeInfo->pIntermediateBlock);
  pSortMergeInfo->pIntermediateBlock = NULL;

  taosArrayDestroy(pSortMergeInfo->matchInfo.pList);

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

  detachOperatorRoutingParam(pOperator);
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
    type, the counter always equals the last-seen cumulative fetch time  -  no
    separate baseline needs to be stored, and the accounting stays correct
    across an operator reset (sort handle recreated and cost cleared together).
  */
  if (pInfo->type == MERGE_TYPE_SORT && QUERY_ENABLE_EXPLAIN(pTaskInfo)) {
    int64_t cur = pInfo->sortMergeInfo.fetchRawDataTime +
                  tsortGetFetchRawDataTime(
                      pInfo->sortMergeInfo.pSortHandle);
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
  SMergePhysiNode* pPhynode = (SMergePhysiNode*)pOper->pPhyNode;
  int32_t code = TSDB_CODE_SUCCESS;
  pOper->status = OP_NOT_OPENED;

  resetBasicOperatorState(&pInfo->binfo);
  pInfo->groupId = 0;
  pInfo->baseGroupId = 0;
  pInfo->groupInitialized = false;

  OPTR_CLR_OPENED(pOper);

  switch (pInfo->type) {
    case MERGE_TYPE_SORT: {
      SSortMergeInfo* pSortMergeInfo = &pInfo->sortMergeInfo;
      bool rebuildOrder =
          pSortMergeInfo->currentPhase == SCAN_PHASE_PREV;

      blockDataCleanup(pSortMergeInfo->pInputBlock);

      blockDataDestroy(pSortMergeInfo->pIntermediateBlock);
      pSortMergeInfo->pIntermediateBlock = NULL;

      finishSortMergeHandle(pSortMergeInfo);
      pSortMergeInfo->prefetchedTuple = NULL;
      for (int32_t i = 0; i < pSortMergeInfo->numOfSources; ++i) {
        SSortMergeSource* pSource = &pSortMergeInfo->pSources[i];
        blockDataDestroy(pSource->pHeldBlock);
        blockDataDestroy(pSource->pActiveHeldBlock);
        pSource->pHeldBlock = NULL;
        pSource->pActiveHeldBlock = NULL;
        pSource->srcOuterGroupId = 0;
        pSource->srcPhase = -1;
        pSource->exhausted = false;
      }
      pSortMergeInfo->currentOuterGroupId = 0;
      pSortMergeInfo->currentPhase = SCAN_PHASE_MAIN;
      pSortMergeInfo->hasCurrentOuterGroup = false;
      pSortMergeInfo->fetchRawDataTime = 0;
      pSortMergeInfo->sortExecInfo = (SSortExecInfo){0};

      if (rebuildOrder) {
        code = rebuildSortMergeInfo(pOper);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }

      pInfo->limitInfo = (SLimitInfo){0};
      initLimitInfo(pPhynode->node.pLimit, pPhynode->node.pSlimit,
                    &pInfo->limitInfo);
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

  return code;
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
  pInfo->outerGroupPhaseMode = pMergePhyNode->outerGroupPhaseMode;
  if (pInfo->outerGroupPhaseMode &&
      (!pInfo->inputWithGroupId || pInfo->groupMerge ||
       pMergePhyNode->type != MERGE_TYPE_SORT)) {
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    goto _error;
  }

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
      TSDB_CHECK_NULL(pSortMergeInfo->pSortInfo, code, lino, _error,
                      terrno);
      pSortMergeInfo->bufPageSize = getProperSortPageSize(rowSize, numOfCols);
      pSortMergeInfo->sortBufSize =
          pSortMergeInfo->bufPageSize * (numStreams + 1);  // one additional is reserved for merged result.
      pSortMergeInfo->currentPhase = SCAN_PHASE_MAIN;
      pSortMergeInfo->hasCurrentOuterGroup = false;
      pSortMergeInfo->numOfSources = (int32_t)numStreams;
      if (numStreams > 0) {
        pSortMergeInfo->pSources = taosMemoryCalloc(
            numStreams, sizeof(SSortMergeSource));
        TSDB_CHECK_NULL(pSortMergeInfo->pSources, code, lino, _error,
                        terrno);
        for (int32_t i = 0; i < pSortMergeInfo->numOfSources; ++i) {
          pSortMergeInfo->pSources[i].pOperator = pOperator;
          pSortMergeInfo->pSources[i].sourceIndex = i;
          pSortMergeInfo->pSources[i].srcPhase = -1;
        }
      }
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
