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
#include "function.h"
#include "functionMgt.h"
#include "interpResultStore.h"
#include "operator.h"
#include "querytask.h"
#include "storageapi.h"
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tfill.h"
#include "ttime.h"

// Tracks timestamp ordering independently for each input timeline.
typedef struct STimelineState {
  uint64_t uid;        // source table UID, or groupId when UID is unavailable
  int64_t  prevTs;     // last accepted timestamp in the current direction
  bool     prevTsSet;  // whether prevTs contains an accepted timestamp
} STimelineState;

// Query-level interpolation configuration shared by every result group.
typedef struct SInterpQueryCtx {
  STimeWindow          win;              // RANGE, rewritten per stream window
  SNode*               pWin;             // cloned RANGE; NULL for batch
  SInterval            interval;         // EVERY units, precision, and timezone
  int32_t              fillType;         // TSDB_FILL_* policy
  SColumn              tsCol;            // interpolation timeline column
  struct SFillColInfo* pFillColInfo;      // owned output fill metadata
  int64_t              surroundingTime;  // max distance; <= 0 is unlimited
  int8_t               timelineSource;   // duplicate-ts semantics by source
  bool                 igNull;            // ignore NULL interpolation values
  bool                 requireMainMembership;
} SInterpQueryCtx;

// Mutable interpolation state for the group currently being consumed.
typedef struct SGroupInterpState {
  uint64_t groupId;          // current group cursor; 0 is the initial sentinel
  int64_t  current;          // next interpolation timestamp to generate
  bool     isPrevRowSet;     // whether pPrevRow holds a valid reference row
  bool     isNextRowSet;     // whether pNextRow holds a valid reference row
  bool     mainSeen;         // whether this group has a row inside RANGE
  SArray*  pPrevRow;         // Previous references by input column
  SArray*  pNextRow;         // Next references by input column
  SArray*  pLinearInfo;      // LINEAR endpoints by input column
  SArray*  pTimelineStates;  // MAIN/NEXT ASC cursor per source timeline
  SArray*  pDescTimelineStates;  // PREV DESC cursor per source timeline
  SArray*  pPrevGroupKeys;   // Last group keys copied to output
  STimeWindow leadHole;      // MAIN prefix gap awaiting a PREV reference
  STimeWindow trailHole;     // MAIN suffix gap awaiting a NEXT reference
  SArray*    pExtPrevRow;    // PREV reference for leadHole
  SArray*    pExtNextRow;    // NEXT reference for trailHole
  bool       leadHoleSet;    // whether leadHole is an active range
  bool       trailHoleSet;   // whether trailHole is an active range
  bool       extPrevRowSet;  // whether pExtPrevRow contains a valid row
  bool       extNextRowSet;  // whether pExtNextRow contains a valid row
} SGroupInterpState;

// MAIN_FIRST keeps one interpolation state per group until both required
// side phases are resolved and the group has been emitted.
typedef struct SMainFirstGroupState {
  SGroupInterpState interp;
  SArray*           pFirstMainRow;
  SArray*           pLastMainRow;
  SArray*           pGroupRow;
  SBlockID          blockId;
  uint8_t           scanFlag;
  uint8_t           sideNotified;
  bool              firstMainRowSet;
  bool              groupRowSet;
  bool              mainFinalized;
  bool              queued;
  bool              hasStoredResults;
} SMainFirstGroupState;

typedef enum EInterpOutputStage {
  INTERP_OUTPUT_NONE = 0,
  INTERP_OUTPUT_LEAD,
  INTERP_OUTPUT_STORE,
  INTERP_OUTPUT_TRAIL,
} EInterpOutputStage;

typedef enum EInterpEmittedGroupFlag {
  INTERP_GROUP_EMITTED = 1,
  INTERP_GROUP_NEXT_NOTIFIED = 1 << 1,
  INTERP_GROUP_PREV_NOTIFIED = 1 << 2,
} EInterpEmittedGroupFlag;

typedef struct SInterpGroupOutput {
  SMainFirstGroupState* pGroup;
  SSDataBlock*          pStoredBlock;
  int64_t               storedRowOffset;
  EInterpOutputStage    stage;
  bool                  edgePrepared;
  bool                  edgeDone;
  bool                  edgeAbandoned;
  bool                  storeOpened;
} SInterpGroupOutput;

// Runtime state owned by one time-slice operator instance.
typedef struct STimeSliceOperatorInfo {
  SSDataBlock*      pRes;           // Reusable output block
  SSDataBlock*      pMergedRes;
  SInterpQueryCtx   queryCtx;       // Query configuration and RANGE
  SGroupInterpState groupState;     // Current group state
  SExprSupp         scalarSup;      // Optional scalar projection
  SSDataBlock*      pNextGroupRes;  // Lookahead block for the next group
  SSDataBlock*      pRemainRes;     // Partially consumed input block
  int32_t           remainIndex;    // next row to consume from pRemainRes
  bool              remainFill;    // Resume a gap before remainIndex
  bool              prevNotified;   // PREV early-stop notification sent
  bool              nextNotified;   // NEXT early-stop notification sent
  int8_t            interpMode;     // finalized EInterpMode from the plan
  int8_t            curPhase;       // monotonic EScanPhase input cursor
  SHashObj*          pGroupStates;   // groupId -> SMainFirstGroupState*
  SHashObj*          pEmittedGroups;
  SArray*            pCompletedGroups;
  SInterpResultStore* pResultStore;
  SInterpGroupOutput output;
  uint64_t           inputGroupId;
  uint64_t           outerEpoch;
  int32_t            completedIndex;
  bool               inputGroupSet;
  bool               outerEpochSet;
  bool               outerEpochDraining;
  bool               inputDone;
  bool               phaseClosed[SCAN_PHASE_PREV + 1];
} STimeSliceOperatorInfo;

static void destroyTimeSliceOperatorInfo(void* param);

bool interpGroupNeedsPrev(int32_t fillType, bool hasLeadHole) {
  return hasLeadHole && (fillType == TSDB_FILL_PREV ||
                         fillType == TSDB_FILL_LINEAR ||
                         fillType == TSDB_FILL_NEAR);
}

bool interpGroupNeedsNext(int32_t fillType, bool hasTrailHole) {
  return hasTrailHole && (fillType == TSDB_FILL_NEXT ||
                          fillType == TSDB_FILL_LINEAR ||
                          fillType == TSDB_FILL_NEAR);
}

bool interpTimestampInMainRange(const STimeWindow* pWin, int64_t ts) {
  return ts >= pWin->skey && ts <= pWin->ekey;
}

bool interpLegacyGroupCanEmit(bool requireMainMembership, bool mainSeen) {
  return !requireMainMembership || mainSeen;
}

bool interpLegacyApplyGroupMembership(bool requireMainMembership,
                                      bool mainSeen,
                                      SSDataBlock* pResult) {
  bool canEmit = interpLegacyGroupCanEmit(requireMainMembership, mainSeen);
  if (!canEmit) {
    blockDataCleanup(pResult);
  }
  return canEmit;
}

bool interpModeUsesPhasedProtocol(int8_t interpMode) {
  return interpMode == INTERP_MODE_MAIN_FIRST ||
         interpMode == INTERP_MODE_GROUP_FIRST ||
         interpMode == INTERP_MODE_MULTI_GROUP;
}

bool interpModeCanEarlyStop(int8_t interpMode) {
  return interpMode == INTERP_MODE_LEGACY ||
         interpModeUsesPhasedProtocol(interpMode);
}

// MAIN_FIRST notifies each side group individually as it resolves;
// MULTI_GROUP instead waits and sends one phase-wide notification
// (see notifyMultiGroupSidePhaseResolved).
static bool interpModeNotifiesPerGroup(int8_t interpMode) {
  return interpMode == INTERP_MODE_MAIN_FIRST ||
         interpMode == INTERP_MODE_GROUP_FIRST;
}

bool interpMainFirstGroupIsReady(const SInterpMainFirstReadiness* pReadiness) {
  bool hasFullHole = !pReadiness->firstMainRowSet;
  bool needsPrev = interpGroupNeedsPrev(pReadiness->fillType,
                                        hasFullHole ||
                                            pReadiness->leadHoleSet);
  bool needsNext = interpGroupNeedsNext(pReadiness->fillType,
                                        hasFullHole ||
                                            pReadiness->trailHoleSet);
  bool pendingPrev = needsPrev && !pReadiness->extPrevRowSet &&
                     !pReadiness->prevPhaseClosed;
  bool pendingNext = needsNext && !pReadiness->extNextRowSet &&
                     !pReadiness->nextPhaseClosed;

  return !pReadiness->queued && pReadiness->groupRowSet &&
         pReadiness->mainFinalized && !pendingPrev && !pendingNext;
}

bool interpMainFirstNextCompletedGroup(const SArray* pGroups,
                                       int32_t* pIndex,
                                       uint64_t* pGroupId) {
  if (NULL == pGroups || NULL == pIndex || NULL == pGroupId ||
      *pIndex < 0 || *pIndex >= taosArrayGetSize(pGroups)) {
    return false;
  }

  uint64_t* pQueuedGroup = taosArrayGet(pGroups, (*pIndex)++);
  if (NULL == pQueuedGroup) {
    return false;
  }
  *pGroupId = *pQueuedGroup;
  return true;
}

bool interpMainFirstEdgeCanProduce(int32_t fillType, bool linearReady,
                                   bool prevRowSet, bool nextRowSet) {
  switch (fillType) {
    case TSDB_FILL_LINEAR:
      return linearReady;
    case TSDB_FILL_PREV:
      return prevRowSet;
    case TSDB_FILL_NEXT:
      return nextRowSet;
    case TSDB_FILL_NEAR:
      return prevRowSet || nextRowSet;
    case TSDB_FILL_NULL:
    case TSDB_FILL_NULL_F:
    case TSDB_FILL_SET_VALUE:
    case TSDB_FILL_SET_VALUE_F:
      return true;
    default:
      return false;
  }
}

bool interpTimestampFollows(int32_t order, int64_t previous,
                            int64_t current) {
  if (TSDB_ORDER_ASC == order) {
    return current > previous;
  }
  if (TSDB_ORDER_DESC == order) {
    return current < previous;
  }
  return false;
}

int32_t interpValidatePhaseTransition(int8_t currentPhase,
                                      int8_t inputPhase) {
  if (currentPhase < SCAN_PHASE_MAIN || currentPhase > SCAN_PHASE_PREV ||
      inputPhase < SCAN_PHASE_MAIN || inputPhase > SCAN_PHASE_PREV ||
      inputPhase < currentPhase) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  return TSDB_CODE_SUCCESS;
}

// Copy one column value. Var-length types carry their length in the data
// itself; fixed-length types use the column-defined byte width.
static FORCE_INLINE void copyColDataVal(void* dst, char* val, int8_t type,
                                        int32_t fixedBytes) {
  int32_t bytes = IS_VAR_DATA_TYPE(type) ?
                  calcStrBytesByType(type, val) : fixedBytes;
  memcpy(dst, val, bytes);
}

static void doKeepRows(SArray* pRowKeeper, const SSDataBlock* pBlock,
                       int32_t rowIndex) {
  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);

    SGroupKeys* pkey = taosArrayGet(pRowKeeper, i);
    if (!colDataIsNull_s(pColInfoData, rowIndex)) {
      pkey->isNull = false;
      copyColDataVal(pkey->pData, colDataGetData(pColInfoData, rowIndex),
                     pkey->type, pkey->bytes);
    } else {
      pkey->isNull = true;
    }
  }
}

static void doKeepPrevRows(SGroupInterpState* pState,
                           const SSDataBlock* pBlock, int32_t rowIndex) {
  doKeepRows(pState->pPrevRow, pBlock, rowIndex);
  pState->isPrevRowSet = true;
}

static void doKeepNextRows(SGroupInterpState* pState,
                           const SSDataBlock* pBlock, int32_t rowIndex) {
  doKeepRows(pState->pNextRow, pBlock, rowIndex);
  pState->isNextRowSet = true;
}

static void doKeepLinearInfo(const SInterpQueryCtx* pCtx,
                             SGroupInterpState* pState,
                             const SSDataBlock* pBlock, int32_t rowIndex) {
  int32_t          numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock,
                                         pCtx->tsCol.slotId);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
    SFillLinearInfo* pLinearInfo = taosArrayGet(pState->pLinearInfo, i);

    if (!IS_MATHABLE_TYPE(pColInfoData->info.type)) {
      continue;
    }

    // null value is represented by using key = INT64_MIN for now.
    // TODO: optimize to ignore null values for linear interpolation.
    if (!pLinearInfo->isStartSet) {
      if (!colDataIsNull_s(pColInfoData, rowIndex)) {
        pLinearInfo->start.key = *(int64_t*)colDataGetData(pTsCol, rowIndex);
        copyColDataVal(pLinearInfo->start.val, colDataGetData(pColInfoData, rowIndex),
                       pColInfoData->info.type, pLinearInfo->bytes);
      }
      pLinearInfo->isStartSet = true;
    } else if (!pLinearInfo->isEndSet) {
      if (!colDataIsNull_s(pColInfoData, rowIndex)) {
        pLinearInfo->end.key = *(int64_t*)colDataGetData(pTsCol, rowIndex);
        copyColDataVal(pLinearInfo->end.val, colDataGetData(pColInfoData, rowIndex),
                       pColInfoData->info.type, pLinearInfo->bytes);
      }
      pLinearInfo->isEndSet = true;
    } else {
      pLinearInfo->start.key = pLinearInfo->end.key;
      memcpy(pLinearInfo->start.val, pLinearInfo->end.val, pLinearInfo->bytes);

      if (!colDataIsNull_s(pColInfoData, rowIndex)) {
        pLinearInfo->end.key = *(int64_t*)colDataGetData(pTsCol, rowIndex);
        copyColDataVal(pLinearInfo->end.val, colDataGetData(pColInfoData, rowIndex),
                       pColInfoData->info.type, pLinearInfo->bytes);
      } else {
        pLinearInfo->end.key = INT64_MIN;
      }
    }
  }
}

static FORCE_INLINE int32_t timeSliceEnsureBlockCapacity(
    const SInterpQueryCtx* pCtx, SSDataBlock* pBlock) {
  if (pBlock->info.rows < pBlock->info.capacity) {
    return TSDB_CODE_SUCCESS;
  }

  // Interval is non-zero here: checkEvery() rejects month/year-style calendar
  // units (n/q/y) and a zero interval at translate time. Day/week EVERY values
  // can be DST-sensitive; this tick-based count is only a capacity-growth
  // estimate and does not drive timestamp stepping.
  uint32_t winNum = (pCtx->win.ekey - pCtx->win.skey) / pCtx->interval.interval;
  uint32_t newRowsNum = pBlock->info.rows + TMIN(winNum / 4 + 1, 1048576);
  int32_t  code = blockDataEnsureCapacity(pBlock, newRowsNum);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

static bool isIrowtsPseudoColumn(SExprInfo* pExprInfo) {
  char* name = pExprInfo->pExpr->_function.functionName;
  return (IS_TIMESTAMP_TYPE(pExprInfo->base.resSchema.type) && strcasecmp(name, "_irowts") == 0);
}

static bool isIsfilledPseudoColumn(SExprInfo* pExprInfo) {
  char* name = pExprInfo->pExpr->_function.functionName;
  return (IS_BOOLEAN_TYPE(pExprInfo->base.resSchema.type) && strcasecmp(name, "_isfilled") == 0);
}

static bool isIrowtsOriginPseudoColumn(SExprInfo* pExprInfo) {
  const char* name = pExprInfo->pExpr->_function.functionName;
  return (IS_TIMESTAMP_TYPE(pExprInfo->base.resSchema.type) && strcasecmp(name, "_irowts_origin") == 0);
}

typedef enum {
  INVALID_TIMESTAMP_REASON_NONE = 0,
  INVALID_TIMESTAMP_REASON_PREV_TS_EQUAL = 1,
  INVALID_TIMESTAMP_REASON_PREV_TS_SMALLER = 2,
  INVALID_TIMESTAMP_REASON_DUPLICATE =
      INVALID_TIMESTAMP_REASON_PREV_TS_EQUAL,
  INVALID_TIMESTAMP_REASON_OUT_OF_ORDER =
      INVALID_TIMESTAMP_REASON_PREV_TS_SMALLER,
} EInvalidTimestampReason;

static int32_t getTimelineState(SArray** ppTimelineStates, uint64_t uid,
                                STimelineState** ppState) {
  if (NULL == *ppTimelineStates) {
    *ppTimelineStates = taosArrayInit(8, sizeof(STimelineState));
    if (NULL == *ppTimelineStates) {
      return terrno;
    }
  }

  int32_t num = taosArrayGetSize(*ppTimelineStates);
  for (int32_t i = 0; i < num; ++i) {
    STimelineState* pState = taosArrayGet(*ppTimelineStates, i);
    if (pState->uid == uid) {
      *ppState = pState;
      return TSDB_CODE_SUCCESS;
    }
  }

  STimelineState state = {.uid = uid, .prevTs = INT64_MIN, .prevTsSet = false};
  STimelineState* pState = taosArrayPush(*ppTimelineStates, &state);
  if (pState == NULL) {
    return terrno;
  }

  *ppState = pState;
  return TSDB_CODE_SUCCESS;
}

static uint64_t getTimelineId(const SSDataBlock* pBlock) {
  return (pBlock->info.id.uid != 0) ? pBlock->info.id.uid : pBlock->info.id.groupId;
}

static bool allowPrimaryTimelineDuplicateTimestamp(
    const SInterpQueryCtx* pCtx) {
  return pCtx->timelineSource == TIME_LINE_SOURCE_PRIMARY_TS;
}

// A timestamp is invalid if it does not follow the previous timestamp in the
// requested direction for the same source timeline. Primary timestamp input
// keeps the legacy table semantics: the caller may skip equal timestamps,
// while degraded timelines reject them. A valid timestamp advances the
// cursor, so this function must run exactly once per accepted row. MAIN and
// NEXT share the ASC state; MAIN_FIRST PREV uses a separate DESC state.
static int32_t isInvalidTimestamp(SArray** ppTimelineStates, uint64_t uid,
                                  int64_t currentTs, int32_t order,
                                  EInvalidTimestampReason* pReason) {
  *pReason = INVALID_TIMESTAMP_REASON_NONE;

  STimelineState* pState = NULL;
  int32_t code = getTimelineState(ppTimelineStates, uid, &pState);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (pState->prevTsSet &&
      !interpTimestampFollows(order, pState->prevTs, currentTs)) {
    if (currentTs == pState->prevTs) {
      *pReason = INVALID_TIMESTAMP_REASON_DUPLICATE;
    } else {
      *pReason = INVALID_TIMESTAMP_REASON_OUT_OF_ORDER;
    }
    return TSDB_CODE_SUCCESS;
  }

  pState->prevTsSet = true;
  pState->prevTs = currentTs;
  return TSDB_CODE_SUCCESS;
}

static bool isInterpFunc(SExprInfo* pExprInfo) {
  int32_t functionType = pExprInfo->pExpr->_function.functionType;
  return (functionType == FUNCTION_TYPE_INTERP);
}

static bool isGroupKeyFunc(SExprInfo* pExprInfo) {
  int32_t functionType = pExprInfo->pExpr->_function.functionType;
  return (functionType == FUNCTION_TYPE_GROUP_KEY);
}

static bool isSelectGroupConstValueFunc(SExprInfo* pExprInfo) {
  int32_t functionType = pExprInfo->pExpr->_function.functionType;
  return (functionType == FUNCTION_TYPE_GROUP_CONST_VALUE);
}

static bool getIgnoreNullRes(SExprSupp* pExprSup) {
  for (int32_t i = 0; i < pExprSup->numOfExprs; ++i) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[i];

    if (isInterpFunc(pExprInfo)) {
      for (int32_t j = 0; j < pExprInfo->base.numOfParams; ++j) {
        SFunctParam* pFuncParam = &pExprInfo->base.pParam[j];
        if (pFuncParam->type == FUNC_PARAM_TYPE_VALUE) {
          return pFuncParam->param.i ? true : false;
        }
      }
    }
  }

  return false;
}

static bool checkNullRow(SExprSupp* pExprSup, SSDataBlock* pSrcBlock, int32_t index, bool ignoreNull) {
  if (!ignoreNull) {
    return false;
  }

  for (int32_t j = 0; j < pExprSup->numOfExprs; ++j) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[j];

    if (isInterpFunc(pExprInfo)) {
      int32_t          srcSlot = pExprInfo->base.pParam[0].pCol->slotId;
      SColumnInfoData* pSrc = taosArrayGet(pSrcBlock->pDataBlock, srcSlot);

      if (colDataIsNull_s(pSrc, index)) {
        return true;
      }
    }
  }

  return false;
}

static int32_t interpColSetKey(SColumnInfoData* pDst, int32_t rowNum, SGroupKeys* pKey) {
  int32_t code = 0;
  if (pKey->isNull == false) {
    code = colDataSetVal(pDst, rowNum, pKey->pData, false);
  } else {
    colDataSetNULL(pDst, rowNum);
  }
  return code;
}

/**
  @brief Check if the time difference between the fill reference row and
  target row exceeds surroundingTime, if so, set the fill row to NULL and use
  fillVal to fill.
*/
static void checkSurroundingTime(const SInterpQueryCtx* pCtx,
                                 const SGroupInterpState* pState,
                                 SArray** ppFillRow, SArray* pFillRefRow,
                                 int64_t fillRefRowTs) {
  *ppFillRow = NULL;
  uint64_t diff = safe_abs_diff_i64(fillRefRowTs, pState->current);
  if (pCtx->surroundingTime > 0 && diff > (uint64_t)pCtx->surroundingTime) {
    return;
  }
  *ppFillRow = pFillRefRow;
}

static bool interpDetermineNearFillRow(const SInterpQueryCtx* pCtx,
                                       const SGroupInterpState* pState,
                                       SArray** ppNearRow) {
  if (!pState->isPrevRowSet && !pState->isNextRowSet) {
    *ppNearRow = NULL;
    return false;
  }
  SGroupKeys *pPrevTsKey = NULL, *pNextTsKey = NULL;
  int64_t    *pPrevTs = NULL, *pNextTs = NULL;
  if (pState->isPrevRowSet) {
    pPrevTsKey = taosArrayGet(pState->pPrevRow, pCtx->tsCol.slotId);
    pPrevTs = (int64_t*)pPrevTsKey->pData;
  }
  if (pState->isNextRowSet) {
    pNextTsKey = taosArrayGet(pState->pNextRow, pCtx->tsCol.slotId);
    pNextTs = (int64_t*)pNextTsKey->pData;
  }
  if (!pPrevTsKey) {
    checkSurroundingTime(pCtx, pState, ppNearRow, pState->pNextRow, *pNextTs);
  } else if (!pNextTsKey) {
    checkSurroundingTime(pCtx, pState, ppNearRow, pState->pPrevRow, *pPrevTs);
  } else if (llabs(pState->current - *pPrevTs) <=
             llabs(*pNextTs - pState->current)) {
    // Take PREV when both references are equally near.
    checkSurroundingTime(pCtx, pState, ppNearRow, pState->pPrevRow, *pPrevTs);
  } else {
    checkSurroundingTime(pCtx, pState, ppNearRow, pState->pNextRow, *pNextTs);
  }
  return true;
}

static bool interpDetermineFillRefRow(const SInterpQueryCtx* pCtx,
                                      const SGroupInterpState* pState,
                                      SArray** ppOutRow) {
  bool needFill = false;
  if (pCtx->fillType == TSDB_FILL_PREV) {
    if (pState->isPrevRowSet) {
      SGroupKeys* pTsCol = taosArrayGet(pState->pPrevRow, pCtx->tsCol.slotId);
      checkSurroundingTime(pCtx, pState, ppOutRow, pState->pPrevRow,
                           *(int64_t*)pTsCol->pData);
      needFill = true;
    }
  } else if (pCtx->fillType == TSDB_FILL_NEXT) {
    if (pState->isNextRowSet) {
      SGroupKeys* pTsCol = taosArrayGet(pState->pNextRow, pCtx->tsCol.slotId);
      checkSurroundingTime(pCtx, pState, ppOutRow, pState->pNextRow,
                           *(int64_t*)pTsCol->pData);
      needFill = true;
    }
  } else if (pCtx->fillType == TSDB_FILL_NEAR) {
    needFill = interpDetermineNearFillRow(pCtx, pState, ppOutRow);
  } else {
    needFill = true;
  }
  return needFill;
}

static bool genInterpolationResult(const SInterpQueryCtx* pCtx,
                                   const SGroupInterpState* pState,
                                   SExprSupp* pExprSup, SSDataBlock* pResBlock,
                                   SSDataBlock* pSrcBlock, int32_t index,
                                   bool beforeTs, SExecTaskInfo* pTaskInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t rows = pResBlock->info.rows;
  code = timeSliceEnsureBlockCapacity(pCtx, pResBlock);
  QUERY_CHECK_CODE(code, lino, _end);
  // todo set the correct primary timestamp column

  // output the result
  int32_t fillColIndex = 0;
  int32_t groupKeyIndex = 0;
  bool    hasInterp = true;
  SArray* pFillRefRow = NULL;
  bool    needFill = interpDetermineFillRefRow(pCtx, pState, &pFillRefRow);
  for (int32_t j = 0; j < pExprSup->numOfExprs; ++j) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[j];

    int32_t          dstSlot = pExprInfo->base.resSchema.slotId;
    SColumnInfoData* pDst = taosArrayGet(pResBlock->pDataBlock, dstSlot);

    if (isIrowtsPseudoColumn(pExprInfo)) {
      code = colDataSetVal(pDst, rows, (char*)&pState->current, false);
      QUERY_CHECK_CODE(code, lino, _end);
      continue;
    } else if (isIsfilledPseudoColumn(pExprInfo)) {
      bool isFilled = true;
      code = colDataSetVal(pDst, pResBlock->info.rows, (char*)&isFilled, false);
      QUERY_CHECK_CODE(code, lino, _end);
      continue;
    } else if (!isInterpFunc(pExprInfo) && !isIrowtsOriginPseudoColumn(pExprInfo)) {
      if (isGroupKeyFunc(pExprInfo) || isSelectGroupConstValueFunc(pExprInfo)) {
        if (pSrcBlock != NULL) {
          int32_t          srcSlot = pExprInfo->base.pParam[0].pCol->slotId;
          SColumnInfoData* pSrc = taosArrayGet(pSrcBlock->pDataBlock, srcSlot);

          if (colDataIsNull_s(pSrc, index)) {
            colDataSetNULL(pDst, pResBlock->info.rows);
            continue;
          }

          char* v = colDataGetData(pSrc, index);
          code = colDataSetVal(pDst, pResBlock->info.rows, v, false);
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (!isSelectGroupConstValueFunc(pExprInfo)) {
          // use stored group key
          SGroupKeys* pkey = taosArrayGet(pState->pPrevGroupKeys,
                                          groupKeyIndex);
          QUERY_CHECK_NULL(pkey, code, lino, _end, terrno);
          groupKeyIndex++;
          if (pkey->isNull == false) {
            code = colDataSetVal(pDst, rows, pkey->pData, false);
            QUERY_CHECK_CODE(code, lino, _end);
          } else {
            colDataSetNULL(pDst, rows);
          }
        } else {
          int32_t     srcSlot = pExprInfo->base.pParam[0].pCol->slotId;
          SGroupKeys* pkey = taosArrayGet(pState->pPrevRow, srcSlot);
          if (pkey->isNull == false) {
            code = colDataSetVal(pDst, rows, pkey->pData, false);
            QUERY_CHECK_CODE(code, lino, _end);
          } else {
            colDataSetNULL(pDst, rows);
          }
        }
      }
      continue;
    }

    int32_t srcSlot = isIrowtsOriginPseudoColumn(pExprInfo)
                          ? pCtx->tsCol.slotId
                          : pExprInfo->base.pParam[0].pCol->slotId;
    switch (pCtx->fillType) {
      case TSDB_FILL_NULL:
      case TSDB_FILL_NULL_F: {
        colDataSetNULL(pDst, rows);
        break;
      }

      case TSDB_FILL_PREV:
      case TSDB_FILL_NEAR:
      case TSDB_FILL_NEXT: {
        if (!needFill) {
          hasInterp = false;
          break;
        }
        if (pFillRefRow) {
          code = interpColSetKey(pDst, rows, taosArrayGet(pFillRefRow, srcSlot));
          QUERY_CHECK_CODE(code, lino, _end);
          break;
        }
        // no fillRefRow, fall through to fill specified values
        if (srcSlot == pCtx->tsCol.slotId) {
          // if is _irowts_origin, there is no value to fill, just set to null
          colDataSetNULL(pDst, rows);
          break;
        }
      }
      case TSDB_FILL_SET_VALUE:
      case TSDB_FILL_SET_VALUE_F: {
        SVariant* pVar = &pCtx->pFillColInfo[fillColIndex].fillVal;

        bool isNull = (TSDB_DATA_TYPE_NULL == pVar->nType) ? true : false;
        if (pDst->info.type == TSDB_DATA_TYPE_FLOAT) {
          float v = 0;
          if (!IS_VAR_DATA_TYPE(pVar->nType)) {
            GET_TYPED_DATA(v, float, pVar->nType, &pVar->f, 0);
          } else {
            v = taosStr2Float(varDataVal(pVar->pz), NULL);
          }
          code = colDataSetVal(pDst, rows, (char*)&v, isNull);
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (pDst->info.type == TSDB_DATA_TYPE_DOUBLE) {
          double v = 0;
          if (!IS_VAR_DATA_TYPE(pVar->nType)) {
            GET_TYPED_DATA(v, double, pVar->nType, &pVar->d, 0);
          } else {
            v = taosStr2Double(varDataVal(pVar->pz), NULL);
          }
          code = colDataSetVal(pDst, rows, (char*)&v, isNull);
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (IS_SIGNED_NUMERIC_TYPE(pDst->info.type)) {
          int64_t v = 0;
          if (!IS_VAR_DATA_TYPE(pVar->nType)) {
            GET_TYPED_DATA(v, int64_t, pVar->nType, &pVar->i, 0);
          } else {
            v = taosStr2Int64(varDataVal(pVar->pz), NULL, 10);
          }
          code = colDataSetVal(pDst, rows, (char*)&v, isNull);
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (IS_UNSIGNED_NUMERIC_TYPE(pDst->info.type)) {
          uint64_t v = 0;
          if (!IS_VAR_DATA_TYPE(pVar->nType)) {
            GET_TYPED_DATA(v, uint64_t, pVar->nType, &pVar->u, 0);
          } else {
            v = taosStr2UInt64(varDataVal(pVar->pz), NULL, 10);
          }
          code = colDataSetVal(pDst, rows, (char*)&v, isNull);
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (IS_BOOLEAN_TYPE(pDst->info.type)) {
          bool v = false;
          if (!IS_VAR_DATA_TYPE(pVar->nType)) {
            GET_TYPED_DATA(v, bool, pVar->nType, &pVar->i, 0);
          } else {
            v = taosStr2Int8(varDataVal(pVar->pz), NULL, 10);
          }
          code = colDataSetVal(pDst, rows, (char*)&v, isNull);
          QUERY_CHECK_CODE(code, lino, _end);
        }

        ++fillColIndex;
        break;
      }

      case TSDB_FILL_LINEAR: {
        SFillLinearInfo* pLinearInfo =
            taosArrayGet(pState->pLinearInfo, srcSlot);

        SPoint start = pLinearInfo->start;
        SPoint end = pLinearInfo->end;
        SPoint current = {.key = pState->current};

        // do not interpolate before ts range, only increate pState->current
        if (beforeTs && !pLinearInfo->isEndSet) {
          return true;
        }

        if (!pLinearInfo->isStartSet || !pLinearInfo->isEndSet) {
          hasInterp = false;
          break;
        }

        if (end.key != INT64_MIN && end.key < pState->current) {
          hasInterp = false;
          break;
        }

        if (start.key == INT64_MIN || end.key == INT64_MIN) {
          colDataSetNULL(pDst, rows);
          break;
        }

        current.val = taosMemoryCalloc(pLinearInfo->bytes, 1);
        QUERY_CHECK_NULL(current.val, code, lino, _end, terrno);
        taosGetLinearInterpolationVal(&current, pLinearInfo->type, &start, &end, pLinearInfo->type,
                                      typeGetTypeModFromColInfo(&pDst->info));
        code = colDataSetVal(pDst, rows, (char*)current.val, false);
        QUERY_CHECK_CODE(code, lino, _end);

        taosMemoryFree(current.val);
        break;
      }
      case TSDB_FILL_NONE:
      default:
        break;
    }
  }

  if (hasInterp) {
    pResBlock->info.rows += 1;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return hasInterp;
}

static int32_t addCurrentRowToResult(const SInterpQueryCtx* pCtx,
                                     const SGroupInterpState* pState,
                                     SExprSupp* pExprSup,
                                     SSDataBlock* pResBlock,
                                     SSDataBlock* pSrcBlock, int32_t index) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  code = timeSliceEnsureBlockCapacity(pCtx, pResBlock);
  QUERY_CHECK_CODE(code, lino, _end);
  for (int32_t j = 0; j < pExprSup->numOfExprs; ++j) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[j];

    int32_t          dstSlot = pExprInfo->base.resSchema.slotId;
    SColumnInfoData* pDst = taosArrayGet(pResBlock->pDataBlock, dstSlot);

    if (isIrowtsPseudoColumn(pExprInfo) || isIrowtsOriginPseudoColumn(pExprInfo)) {
      code = colDataSetVal(pDst, pResBlock->info.rows,
                           (char*)&pState->current, false);
      QUERY_CHECK_CODE(code, lino, _end);
    } else if (isIsfilledPseudoColumn(pExprInfo)) {
      bool isFilled = false;
      code = colDataSetVal(pDst, pResBlock->info.rows, (char*)&isFilled, false);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      int32_t          srcSlot = pExprInfo->base.pParam[0].pCol->slotId;
      SColumnInfoData* pSrc = taosArrayGet(pSrcBlock->pDataBlock, srcSlot);

      if (colDataIsNull_s(pSrc, index)) {
        colDataSetNULL(pDst, pResBlock->info.rows);
        continue;
      }

      char* v = colDataGetData(pSrc, index);
      code = colDataSetVal(pDst, pResBlock->info.rows, v, false);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  pResBlock->info.rows += 1;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// @brief Create the row keeper array if absent and clear the row-set flag.
// When the keeper already exists (subsequent blocks), both the array and the
// flag are left untouched.
static int32_t initRowsKeeper(SArray** ppRows, bool* pIsRowSet,
                              const SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (*ppRows != NULL) {
    return TSDB_CODE_SUCCESS;
  }

  *ppRows = taosArrayInit(4, sizeof(SGroupKeys));
  if (*ppRows == NULL) {
    return terrno;
  }

  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);

    SGroupKeys key = {0};
    key.bytes = pColInfo->info.bytes;
    key.type = pColInfo->info.type;
    key.isNull = false;
    key.pData = taosMemoryCalloc(1, pColInfo->info.bytes);
    QUERY_CHECK_NULL(key.pData, code, lino, _end, terrno);
    void* tmp = taosArrayPush(*ppRows, &key);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  }

  *pIsRowSet = false;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t initFillLinearInfo(SGroupInterpState* pState,
                                  SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pState->pLinearInfo != NULL) {
    return TSDB_CODE_SUCCESS;
  }

  pState->pLinearInfo = taosArrayInit(4, sizeof(SFillLinearInfo));
  if (pState->pLinearInfo == NULL) {
    return terrno;
  }

  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);

    SFillLinearInfo linearInfo = {0};
    linearInfo.start.key = INT64_MIN;
    linearInfo.end.key = INT64_MIN;
    linearInfo.start.val = taosMemoryCalloc(1, pColInfo->info.bytes);
    QUERY_CHECK_NULL(linearInfo.start.val, code, lino, _end, terrno);

    linearInfo.end.val = taosMemoryCalloc(1, pColInfo->info.bytes);
    QUERY_CHECK_NULL(linearInfo.end.val, code, lino, _end, terrno);
    linearInfo.isStartSet = false;
    linearInfo.isEndSet = false;
    linearInfo.type = pColInfo->info.type;
    linearInfo.bytes = pColInfo->info.bytes;
    void* tmp = taosArrayPush(pState->pLinearInfo, &linearInfo);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void destroyGroupKey(void* pKey) {
  SGroupKeys* key = (SGroupKeys*)pKey;
  if (key->pData != NULL) {
    taosMemoryFreeClear(key->pData);
  }
}

static int32_t initGroupKeyKeeper(SGroupInterpState* pState,
                                  SExprSupp* pExprSup) {
  if (pState->pPrevGroupKeys != NULL) {
    return TSDB_CODE_SUCCESS;
  }

  pState->pPrevGroupKeys = taosArrayInit(pExprSup->numOfExprs,
                                         sizeof(SGroupKeys));
  if (pState->pPrevGroupKeys == NULL) {
    return terrno;
  }

  for (int32_t i = 0; i < pExprSup->numOfExprs; ++i) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[i];

    if (isGroupKeyFunc(pExprInfo)) {
      SGroupKeys key = {.bytes = pExprInfo->base.resSchema.bytes,
                        .type = pExprInfo->base.resSchema.type,
                        .isNull = false,
                        .pData = taosMemoryCalloc(1, pExprInfo->base.resSchema.bytes)};
      if (!key.pData) {
        taosArrayDestroyEx(pState->pPrevGroupKeys, destroyGroupKey);
        pState->pPrevGroupKeys = NULL;
        return terrno;
      }
      if (NULL == taosArrayPush(pState->pPrevGroupKeys, &key)) {
        taosMemoryFree(key.pData);
        taosArrayDestroyEx(pState->pPrevGroupKeys, destroyGroupKey);
        pState->pPrevGroupKeys = NULL;
        return terrno;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t initKeeperInfo(SGroupInterpState* pState,
                              SSDataBlock* pBlock, SExprSupp* pExprSup) {
  int32_t code = initRowsKeeper(&pState->pPrevRow,
                                &pState->isPrevRowSet, pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  }

  code = initRowsKeeper(&pState->pNextRow, &pState->isNextRowSet, pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  }

  code = initFillLinearInfo(pState, pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  }

  code = initGroupKeyKeeper(pState, pExprSup);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t initMainFirstKeeperInfo(SMainFirstGroupState* pGroup,
                                       SSDataBlock* pBlock,
                                       SExprSupp* pExprSup) {
  int32_t code = initKeeperInfo(&pGroup->interp, pBlock, pExprSup);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = initRowsKeeper(&pGroup->interp.pExtPrevRow,
                        &pGroup->interp.extPrevRowSet, pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = initRowsKeeper(&pGroup->interp.pExtNextRow,
                        &pGroup->interp.extNextRowSet, pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = initRowsKeeper(&pGroup->pFirstMainRow,
                        &pGroup->firstMainRowSet, pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  // pLastMainRow's content is driven entirely by firstMainRowSet (both flip
  // together on the first valid MAIN row); the keeper only needs its array
  // allocated here, so the set-flag output is discarded.
  bool lastMainRowSet = false;
  code = initRowsKeeper(&pGroup->pLastMainRow, &lastMainRowSet, pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  return initRowsKeeper(&pGroup->pGroupRow, &pGroup->groupRowSet,
                        pBlock);
}

static void copyKeeperRow(SArray* pDst, const SArray* pSrc) {
  int32_t numOfCols = taosArrayGetSize(pSrc);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SGroupKeys* pDstKey = taosArrayGet(pDst, i);
    SGroupKeys* pSrcKey = taosArrayGet(pSrc, i);
    pDstKey->isNull = pSrcKey->isNull;
    if (!pSrcKey->isNull) {
      copyColDataVal(pDstKey->pData, pSrcKey->pData, pSrcKey->type,
                     pSrcKey->bytes);
    }
  }
}

static void resetRowsKeeper(SArray* pRows, bool* pIsRowSet) {
  *pIsRowSet = false;
  if (pRows == NULL) {
    return;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pRows); ++i) {
    SGroupKeys* pKey = taosArrayGet(pRows, i);
    pKey->isNull = false;
  }
}

static void destroyRowsKeeper(SArray** ppRows) {
  taosArrayDestroyEx(*ppRows, destroyGroupKey);
  *ppRows = NULL;
}

static void destroyFillLinearInfo(SArray** ppLinearInfo) {
  for (int32_t i = 0; i < taosArrayGetSize(*ppLinearInfo); ++i) {
    SFillLinearInfo* pLinearInfo = taosArrayGet(*ppLinearInfo, i);
    taosMemoryFree(pLinearInfo->start.val);
    taosMemoryFree(pLinearInfo->end.val);
  }
  taosArrayDestroy(*ppLinearInfo);
  *ppLinearInfo = NULL;
}

static void resetFillLinearInfo(SGroupInterpState* pState) {
  if (pState->pLinearInfo == NULL) {
    return;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pState->pLinearInfo); ++i) {
    SFillLinearInfo* pLinearInfo = taosArrayGet(pState->pLinearInfo, i);
    pLinearInfo->start.key = INT64_MIN;
    pLinearInfo->end.key = INT64_MIN;
    pLinearInfo->isStartSet = false;
    pLinearInfo->isEndSet = false;
  }
}

static void resetKeeperInfo(SGroupInterpState* pState) {
  resetRowsKeeper(pState->pPrevRow, &pState->isPrevRowSet);
  resetRowsKeeper(pState->pNextRow, &pState->isNextRowSet);
  resetRowsKeeper(pState->pExtPrevRow, &pState->extPrevRowSet);
  resetRowsKeeper(pState->pExtNextRow, &pState->extNextRowSet);
  resetFillLinearInfo(pState);
}

static int32_t interpGroupStateInit(SGroupInterpState* pState,
                                    int64_t startTs) {
  pState->current = startTs;
  return TSDB_CODE_SUCCESS;
}

// Preserve groupId across group switches: the caller has already advanced it
// to the incoming group. A full operator reset clears groupId separately.
// Edge keepers remain lazy but their validity and hole ranges are reset with
// the rest of the group state.
static void interpGroupStateReset(const SInterpQueryCtx* pCtx,
                                  SGroupInterpState* pState) {
  pState->current = pCtx->win.skey;
  pState->mainSeen = false;
  taosArrayClear(pState->pTimelineStates);
  taosArrayClear(pState->pDescTimelineStates);
  resetKeeperInfo(pState);
  memset(&pState->leadHole, 0, sizeof(pState->leadHole));
  memset(&pState->trailHole, 0, sizeof(pState->trailHole));
  pState->leadHoleSet = false;
  pState->trailHoleSet = false;
}

static void interpGroupStateClearKeepers(SGroupInterpState* pState) {
  destroyRowsKeeper(&pState->pPrevRow);
  destroyRowsKeeper(&pState->pNextRow);
  destroyFillLinearInfo(&pState->pLinearInfo);
  destroyRowsKeeper(&pState->pExtPrevRow);
  destroyRowsKeeper(&pState->pExtNextRow);
  taosArrayDestroyEx(pState->pPrevGroupKeys, destroyGroupKey);
  pState->pPrevGroupKeys = NULL;
}

static void interpGroupStateDestroy(SGroupInterpState* pState) {
  if (pState == NULL) {
    return;
  }

  interpGroupStateClearKeepers(pState);
  taosArrayDestroy(pState->pTimelineStates);
  taosArrayDestroy(pState->pDescTimelineStates);
  memset(pState, 0, sizeof(*pState));
}

static void mainFirstGroupStateDestroy(SMainFirstGroupState* pGroup) {
  if (NULL == pGroup) {
    return;
  }

  destroyRowsKeeper(&pGroup->pFirstMainRow);
  destroyRowsKeeper(&pGroup->pLastMainRow);
  destroyRowsKeeper(&pGroup->pGroupRow);
  interpGroupStateDestroy(&pGroup->interp);
  taosMemoryFree(pGroup);
}

static SMainFirstGroupState* getMainFirstGroup(
    const STimeSliceOperatorInfo* pInfo, uint64_t groupId) {
  SMainFirstGroupState** ppGroup =
      taosHashGet(pInfo->pGroupStates, &groupId, sizeof(groupId));
  return NULL == ppGroup ? NULL : *ppGroup;
}

bool interpMainFirstGroupWasEmitted(SHashObj* pEmittedGroups,
                                    uint64_t groupId) {
  return NULL != taosHashGet(pEmittedGroups, &groupId,
                             sizeof(groupId));
}

bool interpMainFirstEmittedGroupNeedsNotify(SHashObj* pEmittedGroups,
                                            uint64_t groupId, int8_t phase) {
  uint8_t* pFlags = taosHashGet(pEmittedGroups, &groupId, sizeof(groupId));
  if (pFlags == NULL) {
    return false;
  }

  uint8_t flag = 0;
  if (phase == SCAN_PHASE_NEXT) {
    flag = INTERP_GROUP_NEXT_NOTIFIED;
  } else if (phase == SCAN_PHASE_PREV) {
    flag = INTERP_GROUP_PREV_NOTIFIED;
  } else {
    return false;
  }

  if ((*pFlags & flag) != 0) {
    return false;
  }
  *pFlags |= flag;
  return true;
}

EInterpSideGroupAction interpSideGroupAction(int8_t interpMode,
                                             bool groupExists,
                                             bool groupEmitted) {
  if (!interpModeUsesPhasedProtocol(interpMode)) {
    return INTERP_SIDE_GROUP_DROP;
  }
  if (groupEmitted) {
    return interpModeNotifiesPerGroup(interpMode)
               ? INTERP_SIDE_GROUP_NOTIFY
               : INTERP_SIDE_GROUP_DROP;
  }
  if (groupExists) {
    return INTERP_SIDE_GROUP_PROCESS;
  }
  return interpModeNotifiesPerGroup(interpMode)
             ? INTERP_SIDE_GROUP_CREATE
             : INTERP_SIDE_GROUP_DROP;
}

static int32_t createMainFirstGroup(STimeSliceOperatorInfo* pInfo,
                                    SOperatorInfo* pOperator,
                                    SSDataBlock* pBlock,
                                    SMainFirstGroupState** ppGroup) {
  uint64_t groupId = pBlock->info.id.groupId;
  SMainFirstGroupState* pGroup = getMainFirstGroup(pInfo, groupId);
  if (NULL != pGroup) {
    *ppGroup = pGroup;
    return TSDB_CODE_SUCCESS;
  }

  pGroup = taosMemoryCalloc(1, sizeof(SMainFirstGroupState));
  if (NULL == pGroup) {
    return terrno;
  }

  int32_t code = interpGroupStateInit(&pGroup->interp,
                                      pInfo->queryCtx.win.skey);
  if (code == TSDB_CODE_SUCCESS) {
    pGroup->interp.groupId = groupId;
    code = initMainFirstKeeperInfo(pGroup, pBlock,
                                   &pOperator->exprSupp);
  }
  if (code != TSDB_CODE_SUCCESS) {
    mainFirstGroupStateDestroy(pGroup);
    return code;
  }

  // Input metadata is stable within a group, as in LEGACY. Capture it once
  // and reuse it for every stored or edge block emitted for this group.
  pGroup->blockId = pBlock->info.id;
  pGroup->scanFlag = pBlock->info.scanFlag;
  code = taosHashPut(pInfo->pGroupStates, &groupId, sizeof(groupId),
                     &pGroup, sizeof(pGroup));
  if (code != TSDB_CODE_SUCCESS) {
    mainFirstGroupStateDestroy(pGroup);
    return code;
  }

  *ppGroup = pGroup;
  return TSDB_CODE_SUCCESS;
}

static void clearMainFirstGroupStates(STimeSliceOperatorInfo* pInfo) {
  if (NULL == pInfo->pGroupStates) {
    return;
  }

  void* pIter = NULL;
  while (NULL != (pIter = taosHashIterate(pInfo->pGroupStates, pIter))) {
    SMainFirstGroupState* pGroup =
        *(SMainFirstGroupState**)pIter;
    mainFirstGroupStateDestroy(pGroup);
  }
  taosHashClear(pInfo->pGroupStates);
}

static int32_t initMainFirstRuntime(STimeSliceOperatorInfo* pInfo) {
  pInfo->pGroupStates =
      taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT),
                   false, HASH_NO_LOCK);
  if (NULL == pInfo->pGroupStates) {
    return terrno;
  }

  pInfo->pEmittedGroups =
      taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT),
                   false, HASH_NO_LOCK);
  if (NULL == pInfo->pEmittedGroups) {
    return terrno;
  }

  pInfo->pCompletedGroups = taosArrayInit(16, sizeof(uint64_t));
  if (NULL == pInfo->pCompletedGroups) {
    return terrno;
  }
  return interpResultStoreCreate(&pInfo->pResultStore);
}

static void resetMainFirstRuntime(STimeSliceOperatorInfo* pInfo) {
  clearMainFirstGroupStates(pInfo);
  taosHashClear(pInfo->pEmittedGroups);
  taosArrayClear(pInfo->pCompletedGroups);
  interpResultStoreReset(pInfo->pResultStore);
  memset(&pInfo->output, 0, sizeof(pInfo->output));
  memset(pInfo->phaseClosed, 0, sizeof(pInfo->phaseClosed));
  pInfo->inputGroupId = 0;
  pInfo->completedIndex = 0;
  pInfo->inputGroupSet = false;
  pInfo->inputDone = false;
  pInfo->remainFill = false;
  pInfo->prevNotified = false;
  pInfo->nextNotified = false;
  pInfo->outerEpoch = 0;
  pInfo->outerEpochSet = false;
  pInfo->outerEpochDraining = false;
}

// Same runtime reset as a fresh operator, then adopt the next outer epoch
// and restart its phase sequence at MAIN.
static void resetMainFirstOuterEpoch(STimeSliceOperatorInfo* pInfo,
                                     uint64_t outerEpoch) {
  resetMainFirstRuntime(pInfo);
  pInfo->curPhase = SCAN_PHASE_MAIN;
  pInfo->outerEpoch = outerEpoch;
  pInfo->outerEpochSet = true;
}

static void destroyMainFirstRuntime(STimeSliceOperatorInfo* pInfo) {
  clearMainFirstGroupStates(pInfo);
  taosHashCleanup(pInfo->pGroupStates);
  taosHashCleanup(pInfo->pEmittedGroups);
  taosArrayDestroy(pInfo->pCompletedGroups);
  interpResultStoreDestroy(pInfo->pResultStore);
  pInfo->pGroupStates = NULL;
  pInfo->pEmittedGroups = NULL;
  pInfo->pCompletedGroups = NULL;
  pInfo->pResultStore = NULL;
}

static void interpQueryCtxDestroy(SInterpQueryCtx* pCtx) {
  if (pCtx->pFillColInfo != NULL) {
    for (int32_t i = 0; i < pCtx->pFillColInfo->numOfFillExpr; ++i) {
      taosVariantDestroy(&pCtx->pFillColInfo[i].fillVal);
    }
    taosMemoryFreeClear(pCtx->pFillColInfo);
  }
  nodesDestroyNode(pCtx->pWin);
  pCtx->pWin = NULL;
}

static bool checkThresholdReached(STimeSliceOperatorInfo* pSliceInfo, int32_t threshold) {
  SSDataBlock* pResBlock = pSliceInfo->pRes;
  if (pResBlock->info.rows > threshold) {
    return true;
  }

  return false;
}

static bool checkWindowBoundReached(const SInterpQueryCtx* pCtx,
                                    const SGroupInterpState* pState) {
  return pState->current > pCtx->win.ekey;
}

static void saveBlockStatus(STimeSliceOperatorInfo* pSliceInfo, SSDataBlock* pBlock, int32_t curIndex) {
  if (curIndex < pBlock->info.rows - 1) {
    pSliceInfo->pRemainRes = pBlock;
    pSliceInfo->remainIndex = curIndex + 1;
    return;
  }

  // all data in remaining block processed
  pSliceInfo->pRemainRes = NULL;
}

static bool isInterpMainFirstNotifyTarget(int32_t operatorType) {
  return operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN ||
         operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN ||
         operatorType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE ||
         operatorType == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN;
}

SOperatorInfo* interpMainFirstResolveNotifyTarget(SOperatorInfo* pDownstream) {
  if (pDownstream == NULL) {
    return NULL;
  }
  if (isInterpMainFirstNotifyTarget(pDownstream->operatorType)) {
    return pDownstream;
  }
  if (pDownstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_PROJECT ||
      pDownstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_PARTITION) {
    if (pDownstream->numOfDownstream != 1 ||
        pDownstream->pDownstream == NULL) {
      return NULL;
    }
    return interpMainFirstResolveNotifyTarget(
        pDownstream->pDownstream[0]);
  }
  if (pDownstream->operatorType != QUERY_NODE_PHYSICAL_PLAN_MERGE ||
      pDownstream->numOfDownstream <= 0 ||
      pDownstream->pDownstream == NULL) {
    return NULL;
  }

  for (int32_t i = 0; i < pDownstream->numOfDownstream; ++i) {
    SOperatorInfo* pTarget = interpMainFirstResolveNotifyTarget(
        pDownstream->pDownstream[i]);
    if (pTarget == NULL ||
        pTarget->operatorType != QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
      return NULL;
    }
  }
  return pDownstream;
}

static int32_t interpMainFirstNotifyTargetHops(
    SOperatorInfo* pDownstream, const SOperatorInfo* pTarget) {
  int32_t hops = 0;
  while (pDownstream != pTarget && pDownstream != NULL &&
         (pDownstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_PROJECT ||
          pDownstream->operatorType ==
              QUERY_NODE_PHYSICAL_PLAN_PARTITION) &&
         pDownstream->numOfDownstream == 1 &&
         pDownstream->pDownstream != NULL) {
    pDownstream = pDownstream->pDownstream[0];
    ++hops;
  }
  return pDownstream == pTarget ? hops : 0;
}

static int32_t createInterpMainFirstNotifyLeaf(int32_t operatorType,
                                               int32_t downstreamIdx,
                                               SOperatorParam** ppParam) {
  SOperatorParam* pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  if (pParam == NULL) {
    return terrno;
  }

  size_t valueSize = 0;
  if (operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN ||
      operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN) {
    valueSize = sizeof(STableScanOperatorParam);
  } else if (operatorType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
    valueSize = sizeof(SExchangeOperatorParam);
  } else {
    taosMemoryFree(pParam);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  pParam->value = taosMemoryCalloc(1, valueSize);
  if (pParam->value == NULL) {
    taosMemoryFree(pParam);
    return terrno;
  }
  pParam->opType = operatorType;
  pParam->downstreamIdx = downstreamIdx;
  pParam->reUse = false;
  *ppParam = pParam;
  return TSDB_CODE_SUCCESS;
}

static void setInterpNotifyParamReuse(SOperatorParam* pParam, bool reUse) {
  if (pParam == NULL) {
    return;
  }
  pParam->reUse = reUse;
  int32_t numOfChildren = taosArrayGetSize(pParam->pChildren);
  for (int32_t i = 0; i < numOfChildren; ++i) {
    setInterpNotifyParamReuse(taosArrayGetP(pParam->pChildren, i),
                              reUse);
  }
}

void interpPrepareNotifyParamForDispatch(SOperatorParam* pParam) {
  if (pParam == NULL) {
    return;
  }
  pParam->reUse = true;
  if (pParam->opType == QUERY_NODE_PHYSICAL_PLAN_MERGE) {
    return;
  }
  int32_t numOfChildren = taosArrayGetSize(pParam->pChildren);
  for (int32_t i = 0; i < numOfChildren; ++i) {
    interpPrepareNotifyParamForDispatch(
        taosArrayGetP(pParam->pChildren, i));
  }
}

static void destroyInterpNotifyParam(SOperatorParam* pParam) {
  setInterpNotifyParamReuse(pParam, false);
  freeOperatorParam(pParam, OP_GET_PARAM);
}

static int32_t setInterpNotifyParamValue(
    SOperatorParam* pParam, uint64_t notifyGroupId,
    int8_t notifyPhase, uint64_t notifyScanGroupId,
    int32_t exchangeSrcOpType, TSKEY notifyTs) {
  if (pParam == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (exchangeSrcOpType != QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN &&
      exchangeSrcOpType !=
          QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pParam->opType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN ||
      pParam->opType == QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN) {
    STableScanOperatorParam* pScan = pParam->value;
    if (pScan == NULL) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    pScan->paramType = NOTIFY_TYPE_SCAN_PARAM;
    pScan->groupid = notifyGroupId;
    pScan->notifyToProcess = true;
    pScan->notifyTs = notifyTs;
    pScan->notifyPhase = notifyPhase;
    pScan->notifyScanGroupId = notifyScanGroupId;
    return TSDB_CODE_SUCCESS;
  }
  if (pParam->opType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
    SExchangeOperatorParam* pExchange = pParam->value;
    if (pExchange == NULL) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    pExchange->multiParams = false;
    pExchange->basic.paramType = NOTIFY_TYPE_EXCHANGE_PARAM;
    pExchange->basic.srcOpType = exchangeSrcOpType;
    pExchange->basic.groupid = notifyGroupId;
    pExchange->basic.notifyTs = notifyTs;
    pExchange->basic.notifyPhase = notifyPhase;
    pExchange->basic.notifyScanGroupId = notifyScanGroupId;
    return TSDB_CODE_SUCCESS;
  }

  int32_t numOfChildren = taosArrayGetSize(pParam->pChildren);
  if (numOfChildren <= 0) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  for (int32_t i = 0; i < numOfChildren; ++i) {
    SOperatorParam* pChild = taosArrayGetP(pParam->pChildren, i);
    if (pChild == NULL) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    int32_t code = setInterpNotifyParamValue(
        pChild, notifyGroupId, notifyPhase, notifyScanGroupId,
        exchangeSrcOpType, notifyTs);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t buildInterpNotifyParamTree(SOperatorInfo* pOperator,
                                          int32_t downstreamIdx,
                                          SOperatorParam** ppParam) {
  *ppParam = NULL;
  SOperatorInfo* pTarget = interpMainFirstResolveNotifyTarget(pOperator);
  if (pTarget == NULL) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  if (pTarget->operatorType == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN) {
    return TSDB_CODE_SUCCESS;
  }
  if (isInterpMainFirstNotifyTarget(pOperator->operatorType)) {
    return createInterpMainFirstNotifyLeaf(
        pOperator->operatorType, downstreamIdx, ppParam);
  }

  SOperatorParam* pRoot = taosMemoryCalloc(1, sizeof(SOperatorParam));
  if (pRoot == NULL) {
    return terrno;
  }
  pRoot->opType = pOperator->operatorType;
  pRoot->downstreamIdx = downstreamIdx;
  pRoot->reUse = false;
  pRoot->pChildren = taosArrayInit(pOperator->numOfDownstream,
                                   POINTER_BYTES);
  if (pRoot->pChildren == NULL) {
    destroyInterpNotifyParam(pRoot);
    return terrno;
  }

  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SOperatorParam* pChild = NULL;
    int32_t code = buildInterpNotifyParamTree(
        pOperator->pDownstream[i], i, &pChild);
    if (code != TSDB_CODE_SUCCESS || pChild == NULL ||
        taosArrayPush(pRoot->pChildren, &pChild) == NULL) {
      destroyInterpNotifyParam(pChild);
      destroyInterpNotifyParam(pRoot);
      return code != TSDB_CODE_SUCCESS
                 ? code
                 : TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
  }
  *ppParam = pRoot;
  return TSDB_CODE_SUCCESS;
}

static int32_t buildInterpNotifyParam(
    SOperatorInfo* pDownstream, int32_t downstreamIdx,
    uint64_t notifyGroupId, int8_t notifyPhase,
    uint64_t notifyScanGroupId, int32_t exchangeSrcOpType,
    TSKEY notifyTs,
    SOperatorParam** ppParam) {
  if (pDownstream == NULL || ppParam == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  *ppParam = NULL;
  if (notifyPhase < SCAN_PHASE_MAIN ||
      notifyPhase > SCAN_PHASE_PREV) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = buildInterpNotifyParamTree(
      pDownstream, downstreamIdx, ppParam);
  if (code != TSDB_CODE_SUCCESS || *ppParam == NULL) {
    return code;
  }
  code = setInterpNotifyParamValue(
      *ppParam, notifyGroupId, notifyPhase, notifyScanGroupId,
      exchangeSrcOpType, notifyTs);
  if (code != TSDB_CODE_SUCCESS) {
    destroyInterpNotifyParam(*ppParam);
    *ppParam = NULL;
    return code;
  }
  (*ppParam)->reUse = true;
  return code;
}

int32_t interpMainFirstBuildNotifyParam(
    SOperatorInfo* pDownstream, int32_t downstreamIdx,
    uint64_t notifyGroupId, int8_t notifyPhase,
    uint64_t notifyScanGroupId, TSKEY notifyTs,
    SOperatorParam** ppParam) {
  return buildInterpNotifyParam(
      pDownstream, downstreamIdx, notifyGroupId, notifyPhase,
      notifyScanGroupId,
      QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN, notifyTs,
      ppParam);
}

// The get-param follows the normal pull path because applying an early-stop
// changes which block the downstream scan returns next.
static int32_t setDownstreamOpGetParam(SOperatorInfo* pOperator,
                                       TSKEY notifyTs,
                                       const SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  STimeSliceOperatorInfo* pInfo = pOperator->info;
  if (!interpModeCanEarlyStop(pInfo->interpMode)) {
    return TSDB_CODE_SUCCESS;
  }
  bool multiGroup = pInfo->interpMode == INTERP_MODE_MULTI_GROUP;
  if (multiGroup && pInfo->curPhase == SCAN_PHASE_MAIN) {
    return TSDB_CODE_SUCCESS;
  }

  uint64_t notifyScanGroupId =
      multiGroup ? 0 : pBlock->info.id.groupId;
  uint64_t notifyOuterEpoch =
      multiGroup ? pBlock->info.id.baseGId : 0;

  if (pOperator->pDownstreamGetParams == NULL) {
    pOperator->pDownstreamGetParams =
      taosMemoryCalloc(pOperator->numOfDownstream, POINTER_BYTES);
    QUERY_CHECK_NULL(pOperator->pDownstreamGetParams, code, lino, _end,
                     terrno);
  }

  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SOperatorInfo* pDownstream = pOperator->pDownstream[i];
    SOperatorInfo* pTarget =
        interpMainFirstResolveNotifyTarget(pDownstream);
    if (pTarget == NULL) {
      qWarn("%s, %s unsupported notify downstream %d, skip",
             GET_TASKID(pOperator->pTaskInfo), __func__,
             pDownstream->operatorType);
      continue;
    }

    // Any get-param restarts FederatedScan, so its notification is a no-op.
    if (pTarget->operatorType == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN) {
      continue;
    }

    // A scan-typed target dictates its own srcOpType; any other target
    // (e.g. MERGE, EXCHANGE) falls back to the per-mode default.
    int32_t exchangeSrcOpType;
    if (pTarget->operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN ||
        pTarget->operatorType ==
            QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN) {
      exchangeSrcOpType = pTarget->operatorType;
    } else {
      exchangeSrcOpType =
          interpModeUsesPhasedProtocol(pInfo->interpMode)
              ? QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN
              : QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
    }

    SOperatorParam* pRoot = pOperator->pDownstreamGetParams[i];
    if (pRoot == NULL) {
      code = buildInterpNotifyParam(
          pDownstream, i, notifyOuterEpoch, pInfo->curPhase,
          notifyScanGroupId, exchangeSrcOpType, notifyTs,
          &pRoot);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      code = setInterpNotifyParamValue(
          pRoot, notifyOuterEpoch, pInfo->curPhase, notifyScanGroupId,
          exchangeSrcOpType, notifyTs);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    pOperator->pDownstreamGetParams[i] = pRoot;

    if (interpModeUsesPhasedProtocol(pInfo->interpMode) &&
        (qDebugFlag & DEBUG_DEBUG)) {
      int32_t hops = interpMainFirstNotifyTargetHops(
          pDownstream, pTarget);
      qDebug("%s TimeSlice MAIN_FIRST early-stop phase=%d target=%s "
             "hops=%d notifyTs=%" PRId64 " scanGroupId=%" PRIu64
             " outerEpoch=%" PRIu64,
             GET_TASKID(pOperator->pTaskInfo), pInfo->curPhase,
             interpNotifyTargetOpName(pTarget->operatorType),
             hops, notifyTs, notifyScanGroupId, notifyOuterEpoch);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static SSDataBlock* getNextInterpBlock(SOperatorInfo* pOperator,
                                       bool detachParam) {
  SOperatorParam* pParam = pOperator->pDownstreamGetParams == NULL
                               ? NULL
                               : pOperator->pDownstreamGetParams[0];
  STimeSliceOperatorInfo* pInfo = pOperator->info;
  if (interpModeUsesPhasedProtocol(pInfo->interpMode)) {
    // Pending trees own their children. Lend each node only while routing the
    // one-shot pull, so an undispatched tree remains recursively freeable.
    interpPrepareNotifyParamForDispatch(pParam);
  }
  if (detachParam) {
    return getNextBlockFromDownstreamRemainDetach(pOperator, 0);
  }

  SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
  SOperatorInfo* pDownstream = pOperator->pDownstream[0];
  if (pParam != NULL && pDownstream->pOperatorGetParam == pParam) {
    // Some legacy wrappers route the child without consuming their empty
    // reusable shell. The one-shot pull has completed, so release it here.
    pDownstream->pOperatorGetParam = NULL;
    pParam->reUse = false;
    freeOperatorParam(pParam, OP_GET_PARAM);
  }
  return pBlock;
}

static int64_t getNextTimestamp(int64_t current, const SInterval* pInterval) {
  return taosTimeAdd(current, pInterval->interval,
                     pInterval->intervalUnit, pInterval->precision,
                     pInterval->timezone);
}

int32_t interpMainFirstGetLeadHole(int64_t startTs, int64_t firstTs,
                                   const SInterval* pInterval,
                                   STimeWindow* pHole, int64_t* pNextTs,
                                   bool* pHasHole) {
  if (NULL == pInterval || NULL == pHole || NULL == pNextTs ||
      NULL == pHasHole || pInterval->interval <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  memset(pHole, 0, sizeof(*pHole));
  *pNextTs = startTs;
  *pHasHole = false;
  if (firstTs <= startTs) {
    return TSDB_CODE_SUCCESS;
  }

  bool calendarInterval =
      IS_CALENDAR_TIME_DURATION(pInterval->intervalUnit) ||
      IS_CALENDAR_DAY_DURATION(pInterval->intervalUnit);
  if (!calendarInterval) {
    // Fixed-duration grids are affine. Locate both adjacent points directly
    // so a wide range does not turn into one iteration per EVERY tick.
    uint64_t distance = (uint64_t)firstTs - (uint64_t)startTs;
    uint64_t interval = (uint64_t)pInterval->interval;
    int64_t back = (int64_t)((distance - 1) % interval + 1);
    int64_t holeEnd = firstTs - back;
    if (holeEnd > INT64_MAX - pInterval->interval) {
      return TSDB_CODE_TIME_ERROR;
    }
    int64_t nextTs = holeEnd + pInterval->interval;

    pHole->skey = startTs;
    pHole->ekey = holeEnd;
    *pNextTs = nextTs;
    *pHasHole = true;
    return TSDB_CODE_SUCCESS;
  }

  // Calendar units are coarse, and their addition is cumulative: month-end
  // clamping and a DST gap can change later wall-clock points. Preserve
  // taosTimeAdd's exact sequence instead of using a fixed-tick shortcut.
  int64_t current = startTs;
  int64_t holeEnd = startTs;
  while (current < firstTs) {
    holeEnd = current;
    int64_t nextTs = getNextTimestamp(current, pInterval);
    if (nextTs <= current) {
      return TSDB_CODE_TIME_ERROR;
    }
    current = nextTs;
  }

  pHole->skey = startTs;
  pHole->ekey = holeEnd;
  *pNextTs = current;
  *pHasHole = true;
  return TSDB_CODE_SUCCESS;
}

static void doTimesliceImpl(SOperatorInfo* pOperator,
                            STimeSliceOperatorInfo* pSliceInfo,
                            const SInterpQueryCtx* pCtx,
                            SGroupInterpState* pState, SSDataBlock* pBlock,
                            SExecTaskInfo* pTaskInfo) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SSDataBlock*     pResBlock = pSliceInfo->pRes;
  const SInterval* pInterval = &pCtx->interval;
  SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock,
                                         pCtx->tsCol.slotId);

  qDebug("%s TimeSlice: begin block rows=%d tsPrecision=%d "
         "interval=%" PRId64 " intervalUnit=%d intervalPrecision=%d "
         "current=%" PRId64 " win=[%" PRId64 ",%" PRId64 "] "
         "fillType=%d remainRes=%p remainIndex=%d dataLoad=%d",
         GET_TASKID(pTaskInfo), (int)pBlock->info.rows,
         (int)pTsCol->info.precision, pInterval->interval,
         (int)pInterval->intervalUnit, (int)pInterval->precision,
         pState->current, pCtx->win.skey, pCtx->win.ekey,
         (int)pCtx->fillType, (void*)pSliceInfo->pRemainRes,
         pSliceInfo->remainIndex, (int)pBlock->info.dataLoad);

  int32_t i = (pSliceInfo->pRemainRes == NULL) ? 0 : pSliceInfo->remainIndex;
  for (; i < pBlock->info.rows; ++i) {
    bool resumeFill = pSliceInfo->remainFill;
    pSliceInfo->remainFill = false;
    if (pTsCol->hasNull && colDataIsNull_f(pTsCol, i)) {
      code = TSDB_CODE_FUNC_INVALID_TIMELINE;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    int64_t ts = *(int64_t*)colDataGetData(pTsCol, i);

    if (pCtx->requireMainMembership &&
        interpTimestampInMainRange(&pCtx->win, ts)) {
      pState->mainSeen = true;
    }

    if (checkNullRow(&pOperator->exprSupp, pBlock, i, pCtx->igNull)) {
      continue;
    }

    if ((!pSliceInfo->prevNotified && ts < pCtx->win.skey) ||
        (!pSliceInfo->nextNotified && ts > pCtx->win.ekey)) {
      code = setDownstreamOpGetParam(pOperator, ts, pBlock);
      QUERY_CHECK_CODE(code, lino, _end);
      if (ts < pCtx->win.skey) {
        pSliceInfo->prevNotified = true;
      } else {
        pSliceInfo->nextNotified = true;
      }
    }

    EInvalidTimestampReason invalidReason = INVALID_TIMESTAMP_REASON_NONE;
    bool allowDuplicateTs = allowPrimaryTimelineDuplicateTimestamp(pCtx);
    if (!resumeFill) {
      code = isInvalidTimestamp(&pState->pTimelineStates, getTimelineId(pBlock),
                                ts, TSDB_ORDER_ASC, &invalidReason);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    qDebug("%s TimeSlice: row=%d ts=%" PRId64 " current=%" PRId64
           " invalidReason=%d cmp=%d",
           GET_TASKID(pTaskInfo), i, ts, pState->current,
           (int)invalidReason,
           (ts < pState->current) ? -1 : ((ts > pState->current) ? 1 : 0));
    if (invalidReason != INVALID_TIMESTAMP_REASON_NONE) {
      if (invalidReason == INVALID_TIMESTAMP_REASON_DUPLICATE &&
          allowDuplicateTs) {
        continue;
      }
      if (invalidReason == INVALID_TIMESTAMP_REASON_OUT_OF_ORDER &&
          ts < pCtx->win.skey) {
        break;
      }
      code = (invalidReason == INVALID_TIMESTAMP_REASON_DUPLICATE)
                 ? TSDB_CODE_FUNC_DUP_TIMESTAMP
                 : TSDB_CODE_FUNC_INVALID_TIMELINE;
      QUERY_CHECK_CODE(code, lino, _end);
    }
    if (ts == pState->current) {
      code = addCurrentRowToResult(pCtx, pState, &pOperator->exprSupp,
                                   pResBlock, pBlock, i);
      QUERY_CHECK_CODE(code, lino, _end);
      qDebug("%s TimeSlice: add-current row=%d ts=%" PRId64 " resRows=%d",
             GET_TASKID(pTaskInfo), i, ts, (int)pResBlock->info.rows);

      doKeepPrevRows(pState, pBlock, i);
      if (!resumeFill) {
        doKeepLinearInfo(pCtx, pState, pBlock, i);
      }

      pState->current = getNextTimestamp(pState->current, pInterval);
      if (checkWindowBoundReached(pCtx, pState)) {
        break;
      }
      if (checkThresholdReached(pSliceInfo, pOperator->resultInfo.threshold)) {
        saveBlockStatus(pSliceInfo, pBlock, i);
        return;
      }
    } else if (ts < pState->current) {
      qDebug("%s TimeSlice: keep-prev row=%d ts=%" PRId64
             " current=%" PRId64,
             GET_TASKID(pTaskInfo), i, ts, pState->current);
      doKeepPrevRows(pState, pBlock, i);
      if (!resumeFill) {
        doKeepLinearInfo(pCtx, pState, pBlock, i);
      }
    } else {  // ts > pState->current
      qDebug("%s TimeSlice: keep-next row=%d ts=%" PRId64
             " current=%" PRId64,
             GET_TASKID(pTaskInfo), i, ts, pState->current);
      if (!resumeFill) {
        doKeepNextRows(pState, pBlock, i);
        doKeepLinearInfo(pCtx, pState, pBlock, i);
      }

      while (pState->current < ts && pState->current <= pCtx->win.ekey) {
        bool generated = genInterpolationResult(
            pCtx, pState, &pOperator->exprSupp, pResBlock, pBlock, i, true,
            pTaskInfo);
        qDebug("%s TimeSlice: gen-interp current=%" PRId64
               " targetTs=%" PRId64 " generated=%d resRows=%d "
               "fillType=%d",
               GET_TASKID(pTaskInfo), pState->current, ts, (int)generated,
               (int)pResBlock->info.rows, (int)pCtx->fillType);
        if (!generated && pCtx->fillType == TSDB_FILL_LINEAR) {
          break;
        }
        pState->current = getNextTimestamp(pState->current, pInterval);
        if (pSliceInfo->interpMode == INTERP_MODE_GROUP_FIRST &&
            checkThresholdReached(pSliceInfo,
                                  pOperator->resultInfo.threshold) &&
            !checkWindowBoundReached(pCtx, pState)) {
          // Keep the validated reference row until its gap is fully emitted.
          pSliceInfo->pRemainRes = pBlock;
          pSliceInfo->remainIndex = i;
          pSliceInfo->remainFill = true;
          return;
        }
      }

      // add current row if timestamp matches
      if (ts == pState->current && pState->current <= pCtx->win.ekey) {
        code = addCurrentRowToResult(pCtx, pState, &pOperator->exprSupp,
                                     pResBlock, pBlock, i);
        QUERY_CHECK_CODE(code, lino, _end);
        qDebug("%s TimeSlice: add-current-after-interp row=%d ts=%" PRId64 " resRows=%d",
               GET_TASKID(pTaskInfo), i, ts, (int)pResBlock->info.rows);

        pState->current = getNextTimestamp(pState->current, pInterval);
      }
      doKeepPrevRows(pState, pBlock, i);

      if (checkWindowBoundReached(pCtx, pState)) {
        break;
      }
      if (checkThresholdReached(pSliceInfo, pOperator->resultInfo.threshold)) {
        saveBlockStatus(pSliceInfo, pBlock, i);
        return;
      }
    }
  }

  // if reached here, meaning block processing finished naturally,
  // or interpolation reach window upper bound
  pSliceInfo->pRemainRes = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

static void genInterpAfterDataBlock(const SInterpQueryCtx* pCtx,
                                    SGroupInterpState* pState,
                                    STimeSliceOperatorInfo* pSliceInfo,
                                    SOperatorInfo* pOperator, int32_t index) {
  if (pCtx->fillType == TSDB_FILL_NEXT ||
      pCtx->fillType == TSDB_FILL_LINEAR ||
      pState->pPrevGroupKeys == NULL) {
    return;
  }

  while (pState->current <= pCtx->win.ekey) {
    (void)genInterpolationResult(pCtx, pState, &pOperator->exprSupp,
                                 pSliceInfo->pRes, NULL, index, false,
                                 pOperator->pTaskInfo);
    pState->current = getNextTimestamp(pState->current, &pCtx->interval);
  }
}

static int32_t copyPrevGroupKey(SExprSupp* pExprSup, SArray* pGroupKeys, SSDataBlock* pSrcBlock) {
  int32_t groupKeyIdx = 0;
  for (int32_t j = 0; j < pExprSup->numOfExprs; ++j) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[j];

    if (isGroupKeyFunc(pExprInfo)) {
      int32_t     srcSlot = pExprInfo->base.pParam[0].pCol->slotId;
      SGroupKeys* pGroupKey = taosArrayGet(pGroupKeys, groupKeyIdx);
      if (pGroupKey == NULL) {
        return terrno;
      }
      groupKeyIdx++;
      SColumnInfoData* pSrc = taosArrayGet(pSrcBlock->pDataBlock, srcSlot);

      // All rows in one upstream block belong to a single group, so row 0 is
      // representative for the group key columns.
      if (colDataIsNull_s(pSrc, 0)) {
        pGroupKey->isNull = true;
        break;
      }

      copyColDataVal(pGroupKey->pData, colDataGetData(pSrc, 0), pGroupKey->type,
                     pGroupKey->bytes);
      pGroupKey->isNull = false;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t applyTimeSliceScalar(SOperatorInfo* pOperator,
                                    STimeSliceOperatorInfo* pInfo,
                                    SSDataBlock* pBlock) {
  if (NULL == pInfo->scalarSup.pExprInfo) {
    return TSDB_CODE_SUCCESS;
  }

  SExprSupp* pExprSup = &pInfo->scalarSup;
  return projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock,
                               pExprSup->pCtx, pExprSup->numOfExprs,
                               NULL,
                               GET_STM_RTINFO(pOperator->pTaskInfo),
                               pOperator->pTaskInfo);
}

static int32_t setMainFirstLeadHole(const SInterpQueryCtx* pCtx,
                                    SMainFirstGroupState* pGroup,
                                    int64_t firstTs) {
  SGroupInterpState* pState = &pGroup->interp;
  bool hasHole = false;
  int32_t code = interpMainFirstGetLeadHole(pState->current, firstTs,
                                             &pCtx->interval,
                                             &pState->leadHole,
                                             &pState->current, &hasHole);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (hasHole) {
    pState->leadHoleSet = true;
  }
  return TSDB_CODE_SUCCESS;
}

// The first block seen for a group (MAIN or side) supplies the PREV-hole
// fallback row shared by both phases; capture it once, whichever phase
// arrives first.
static void captureMainFirstGroupRow(SMainFirstGroupState* pGroup,
                                     const SSDataBlock* pBlock) {
  if (!pGroup->groupRowSet && pBlock->info.rows > 0) {
    doKeepRows(pGroup->pGroupRow, pBlock, 0);
    pGroup->groupRowSet = true;
  }
}

static int32_t captureMainFirstRows(SOperatorInfo* pOperator,
                                    STimeSliceOperatorInfo* pInfo,
                                    SMainFirstGroupState* pGroup,
                                    SSDataBlock* pBlock) {
  SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock,
                                         pInfo->queryCtx.tsCol.slotId);
  captureMainFirstGroupRow(pGroup, pBlock);

  int32_t lastValidRow = -1;
  for (int32_t i = 0; i < pBlock->info.rows; ++i) {
    if (pTsCol->hasNull && colDataIsNull_f(pTsCol, i)) {
      return TSDB_CODE_FUNC_INVALID_TIMELINE;
    }
    if (checkNullRow(&pOperator->exprSupp, pBlock, i,
                     pInfo->queryCtx.igNull)) {
      continue;
    }

    int64_t ts = *(int64_t*)colDataGetData(pTsCol, i);
    if (!pGroup->firstMainRowSet) {
      doKeepRows(pGroup->pFirstMainRow, pBlock, i);
      pGroup->firstMainRowSet = true;
      if (pInfo->interpMode != INTERP_MODE_GROUP_FIRST ||
          interpGroupNeedsPrev(pInfo->queryCtx.fillType, true)) {
        int32_t code = setMainFirstLeadHole(&pInfo->queryCtx, pGroup, ts);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
    }
    lastValidRow = i;
    if (pInfo->interpMode == INTERP_MODE_GROUP_FIRST &&
        !pInfo->queryCtx.igNull && !pTsCol->hasNull) {
      // Every row is eligible; the interpolation kernel validates ordering.
      lastValidRow = pBlock->info.rows - 1;
      break;
    }
  }
  // Only the last valid row's snapshot survives; keep it once instead of
  // re-copying on every row in the block.
  if (lastValidRow >= 0) {
    doKeepRows(pGroup->pLastMainRow, pBlock, lastValidRow);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t appendMainFirstResult(STimeSliceOperatorInfo* pInfo,
                                     SMainFirstGroupState* pGroup) {
  if (pInfo->pRes->info.rows <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  pInfo->pRes->info.id = pGroup->blockId;
  pInfo->pRes->info.id.groupId = pGroup->interp.groupId;
  pInfo->pRes->info.scanFlag = pGroup->scanFlag;
  int32_t code = interpResultStoreAppend(pInfo->pResultStore,
                                         pGroup->interp.groupId,
                                         pInfo->pRes);
  if (code == TSDB_CODE_SUCCESS) {
    pGroup->hasStoredResults = true;
  }
  return code;
}

// The scan knows its physical phase window, but not when TimeSlice has
// produced every interpolation point for this group. Notify it so the
// remaining MAIN rows can be skipped instead of read to natural exhaustion.
static int32_t notifyMainFirstMainResolved(SOperatorInfo* pOperator,
                                            const SSDataBlock* pBlock,
                                            int64_t endTs) {
  // There is no representable timestamp beyond INT64_MAX. In that case the
  // optimization must yield to natural reader exhaustion.
  if (endTs == INT64_MAX) {
    return TSDB_CODE_SUCCESS;
  }
  return setDownstreamOpGetParam(pOperator, endTs + 1, pBlock);
}

static void initMainFirstOutputInfo(SSDataBlock* pBlock,
                                   const SBlockID* pBlockId,
                                   uint8_t scanFlag, int8_t scanPhase);

static int32_t processMainFirstMainBlock(SOperatorInfo* pOperator,
                                         SSDataBlock* pBlock) {
  STimeSliceOperatorInfo* pInfo = pOperator->info;
  uint64_t groupId = pBlock->info.id.groupId;
  if (interpMainFirstGroupWasEmitted(pInfo->pEmittedGroups, groupId)) {
    return TSDB_CODE_SUCCESS;
  }

  SMainFirstGroupState* pGroup = NULL;
  int32_t code = createMainFirstGroup(pInfo, pOperator, pBlock, &pGroup);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (pBlock->info.blankFill) {
    return copyPrevGroupKey(&pOperator->exprSupp,
                            pGroup->interp.pPrevGroupKeys, pBlock);
  }

  if (pInfo->pRemainRes != pBlock) {
    code = applyTimeSliceScalar(pOperator, pInfo, pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    code = copyPrevGroupKey(&pOperator->exprSupp,
                           pGroup->interp.pPrevGroupKeys, pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    code = captureMainFirstRows(pOperator, pInfo, pGroup, pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  SGroupInterpState* pState = &pGroup->interp;
  if (checkWindowBoundReached(&pInfo->queryCtx, pState)) {
    pInfo->pRemainRes = NULL;
    return notifyMainFirstMainResolved(
        pOperator, pBlock, pInfo->queryCtx.win.ekey);
  }

  code = setInputDataBlock(&pOperator->exprSupp, pBlock, TSDB_ORDER_ASC,
                           MAIN_SCAN, true);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (pInfo->interpMode == INTERP_MODE_GROUP_FIRST &&
      pGroup->firstMainRowSet && !pState->leadHoleSet) {
    if (pInfo->pRes->info.rows == 0) {
      initMainFirstOutputInfo(pInfo->pRes, &pGroup->blockId,
                             pGroup->scanFlag, SCAN_PHASE_MAIN);
    }
    doTimesliceImpl(pOperator, pInfo, &pInfo->queryCtx, pState,
                   pBlock, pOperator->pTaskInfo);
    if (checkWindowBoundReached(&pInfo->queryCtx, pState)) {
      return notifyMainFirstMainResolved(
          pOperator, pBlock, pInfo->queryCtx.win.ekey);
    }
    return TSDB_CODE_SUCCESS;
  }

  do {
    blockDataCleanup(pInfo->pRes);
    doTimesliceImpl(pOperator, pInfo, &pInfo->queryCtx, pState,
                    pBlock, pOperator->pTaskInfo);
    code = appendMainFirstResult(pInfo, pGroup);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  } while (pInfo->pRemainRes == pBlock);

  blockDataCleanup(pInfo->pRes);
  if (checkWindowBoundReached(&pInfo->queryCtx, pState)) {
    return notifyMainFirstMainResolved(
        pOperator, pBlock, pInfo->queryCtx.win.ekey);
  }
  return TSDB_CODE_SUCCESS;
}

// A group is ready to emit once MAIN has finalized it, it produced at least
// one row, and neither side phase it still needs is outstanding. pendingPrev/
// pendingNext are recomputed fresh on every call (nothing reads a value
// carried over from a previous call), so they stay local rather than fields.
static int32_t refreshMainFirstGroupReadiness(STimeSliceOperatorInfo* pInfo,
                                              SMainFirstGroupState* pGroup) {
  SGroupInterpState* pState = &pGroup->interp;
  SInterpMainFirstReadiness readiness = {
      .fillType = pInfo->queryCtx.fillType,
      .firstMainRowSet = pGroup->firstMainRowSet,
      .groupRowSet = pGroup->groupRowSet,
      .mainFinalized = pGroup->mainFinalized,
      .queued = pGroup->queued,
      .leadHoleSet = pState->leadHoleSet,
      .trailHoleSet = pState->trailHoleSet,
      .extPrevRowSet = pState->extPrevRowSet,
      .extNextRowSet = pState->extNextRowSet,
      .prevPhaseClosed = pInfo->phaseClosed[SCAN_PHASE_PREV],
      .nextPhaseClosed = pInfo->phaseClosed[SCAN_PHASE_NEXT],
  };
  if (!interpMainFirstGroupIsReady(&readiness)) {
    return TSDB_CODE_SUCCESS;
  }

  uint64_t groupId = pGroup->interp.groupId;
  if (NULL == taosArrayPush(pInfo->pCompletedGroups, &groupId)) {
    return terrno;
  }
  pGroup->queued = true;
  return TSDB_CODE_SUCCESS;
}

static int32_t finalizeMainFirstGroup(STimeSliceOperatorInfo* pInfo,
                                      SMainFirstGroupState* pGroup) {
  if (NULL == pGroup || pGroup->mainFinalized) {
    return TSDB_CODE_SUCCESS;
  }

  SGroupInterpState* pState = &pGroup->interp;
  if (pGroup->firstMainRowSet &&
      pState->current <= pInfo->queryCtx.win.ekey) {
    pState->trailHole.skey = pState->current;
    pState->trailHole.ekey = pInfo->queryCtx.win.ekey;
    pState->trailHoleSet = true;
  }
  pGroup->mainFinalized = true;
  return refreshMainFirstGroupReadiness(pInfo, pGroup);
}

static int32_t finalizeMainFirstInputGroup(STimeSliceOperatorInfo* pInfo) {
  if (!pInfo->inputGroupSet || pInfo->curPhase != SCAN_PHASE_MAIN) {
    return TSDB_CODE_SUCCESS;
  }

  SMainFirstGroupState* pGroup =
      getMainFirstGroup(pInfo, pInfo->inputGroupId);
  return finalizeMainFirstGroup(pInfo, pGroup);
}

static int32_t refreshAllMainFirstGroups(STimeSliceOperatorInfo* pInfo) {
  void* pIter = NULL;
  while (NULL != (pIter = taosHashIterate(pInfo->pGroupStates, pIter))) {
    SMainFirstGroupState* pGroup =
        *(SMainFirstGroupState**)pIter;
    int32_t code = refreshMainFirstGroupReadiness(pInfo, pGroup);
    if (code != TSDB_CODE_SUCCESS) {
      taosHashCancelIterate(pInfo->pGroupStates, pIter);
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t closeMainFirstPhase(STimeSliceOperatorInfo* pInfo,
                                   int8_t phase) {
  if (pInfo->phaseClosed[phase]) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  if (phase == SCAN_PHASE_MAIN) {
    code = finalizeMainFirstInputGroup(pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  pInfo->phaseClosed[phase] = true;
  pInfo->inputGroupSet = false;
  return refreshAllMainFirstGroups(pInfo);
}

static int32_t advanceMainFirstPhase(STimeSliceOperatorInfo* pInfo,
                                     int8_t inputPhase) {
  int32_t code = interpValidatePhaseTransition(pInfo->curPhase,
                                               inputPhase);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  while (pInfo->curPhase < inputPhase) {
    code = closeMainFirstPhase(pInfo, pInfo->curPhase);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pInfo->curPhase += 1;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t finishAllMainFirstPhases(STimeSliceOperatorInfo* pInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  while (pInfo->curPhase <= SCAN_PHASE_PREV) {
    code = closeMainFirstPhase(pInfo, pInfo->curPhase);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pInfo->curPhase += 1;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t validateMainFirstSideTimestamp(const SInterpQueryCtx* pCtx,
                                              SMainFirstGroupState* pGroup,
                                              const SSDataBlock* pBlock,
                                              int64_t ts, int8_t phase,
                                             EInvalidTimestampReason* pReason) {
  SArray** ppTimelineStates = phase == SCAN_PHASE_PREV
                                  ? &pGroup->interp.pDescTimelineStates
                                  : &pGroup->interp.pTimelineStates;
  int32_t order = phase == SCAN_PHASE_PREV ? TSDB_ORDER_DESC
                                            : TSDB_ORDER_ASC;
  int32_t code = isInvalidTimestamp(ppTimelineStates,
                                    getTimelineId(pBlock), ts, order,
                                    pReason);
  if (code != TSDB_CODE_SUCCESS ||
      *pReason == INVALID_TIMESTAMP_REASON_NONE) {
    return code;
  }
  if (*pReason == INVALID_TIMESTAMP_REASON_DUPLICATE &&
      allowPrimaryTimelineDuplicateTimestamp(pCtx)) {
    return TSDB_CODE_SUCCESS;
  }
  return *pReason == INVALID_TIMESTAMP_REASON_DUPLICATE
             ? TSDB_CODE_FUNC_DUP_TIMESTAMP
             : TSDB_CODE_FUNC_INVALID_TIMELINE;
}

static bool isMainFirstGroupSideResolved(
    const STimeSliceOperatorInfo* pInfo,
    const SMainFirstGroupState* pGroup) {
  if (!pGroup->mainFinalized) {
    return false;
  }

  const SGroupInterpState* pState = &pGroup->interp;
  bool fullHole = !pGroup->firstMainRowSet;
  if (pInfo->curPhase == SCAN_PHASE_NEXT) {
    return !interpGroupNeedsNext(pInfo->queryCtx.fillType,
                                 fullHole || pState->trailHoleSet) ||
           pState->extNextRowSet;
  }
  if (pInfo->curPhase == SCAN_PHASE_PREV) {
    return !interpGroupNeedsPrev(pInfo->queryCtx.fillType,
                                 fullHole || pState->leadHoleSet) ||
           pState->extPrevRowSet;
  }
  return false;
}

static bool isMultiGroupSidePhaseResolved(
    const STimeSliceOperatorInfo* pInfo) {
  int8_t phase = pInfo->curPhase;
  if (phase != SCAN_PHASE_NEXT && phase != SCAN_PHASE_PREV) {
    return false;
  }
  if (!pInfo->phaseClosed[SCAN_PHASE_MAIN]) {
    return false;
  }

  void* pIter = NULL;
  while (NULL != (pIter = taosHashIterate(pInfo->pGroupStates, pIter))) {
    SMainFirstGroupState* pGroup =
        *(SMainFirstGroupState**)pIter;
    if (!isMainFirstGroupSideResolved(pInfo, pGroup)) {
      taosHashCancelIterate(pInfo->pGroupStates, pIter);
      return false;
    }
  }
  return true;
}

static bool getMainFirstSideNotifyTs(const STimeSliceOperatorInfo* pInfo,
                                     const SSDataBlock* pBlock,
                                     int64_t* pNotifyTs) {
  if (pBlock == NULL || pNotifyTs == NULL || pBlock->info.blankFill ||
      pBlock->info.rows <= 0) {
    return false;
  }

  SColumnInfoData* pTsCol = taosArrayGet(
      pBlock->pDataBlock, pInfo->queryCtx.tsCol.slotId);
  if (pTsCol == NULL) {
    return false;
  }
  for (int32_t i = 0; i < pBlock->info.rows; ++i) {
    if (!pTsCol->hasNull || !colDataIsNull_f(pTsCol, i)) {
      *pNotifyTs = *(int64_t*)colDataGetData(pTsCol, i);
      return true;
    }
  }
  return false;
}

static int32_t notifyMultiGroupSidePhaseResolved(
    SOperatorInfo* pOperator, const SSDataBlock* pBlock,
    bool foundRef, int64_t refTs) {
  STimeSliceOperatorInfo* pInfo = pOperator->info;
  if (pInfo->interpMode != INTERP_MODE_MULTI_GROUP) {
    return TSDB_CODE_SUCCESS;
  }

  bool* pNotified = pInfo->curPhase == SCAN_PHASE_NEXT
                        ? &pInfo->nextNotified
                        : &pInfo->prevNotified;
  if (*pNotified) {
    return TSDB_CODE_SUCCESS;
  }
  if (!isMultiGroupSidePhaseResolved(pInfo)) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t notifyTs = refTs;
  if (!foundRef && !getMainFirstSideNotifyTs(pInfo, pBlock, &notifyTs)) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t code = setDownstreamOpGetParam(
      pOperator, notifyTs, pBlock);
  if (code == TSDB_CODE_SUCCESS) {
    *pNotified = true;
  }
  return code;
}

static int32_t notifyMainFirstGroupSideResolved(
    SOperatorInfo* pOperator, SMainFirstGroupState* pGroup,
    const SSDataBlock* pBlock, bool foundRef, int64_t refTs) {
  STimeSliceOperatorInfo* pInfo = pOperator->info;
  if (pInfo->interpMode == INTERP_MODE_MULTI_GROUP) {
    return notifyMultiGroupSidePhaseResolved(pOperator, pBlock, foundRef,
                                             refTs);
  }

  if (!isMainFirstGroupSideResolved(pInfo, pGroup)) {
    return TSDB_CODE_SUCCESS;
  }
  uint8_t flag = pInfo->curPhase == SCAN_PHASE_NEXT
                     ? INTERP_GROUP_NEXT_NOTIFIED
                     : INTERP_GROUP_PREV_NOTIFIED;
  if (pGroup->sideNotified & flag) {
    return TSDB_CODE_SUCCESS;
  }
  int64_t notifyTs = refTs;
  if (!foundRef && !getMainFirstSideNotifyTs(pInfo, pBlock, &notifyTs)) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t code = setDownstreamOpGetParam(pOperator, notifyTs, pBlock);
  if (code == TSDB_CODE_SUCCESS) {
    pGroup->sideNotified |= flag;
  }
  return code;
}

static int32_t notifyEmittedMainFirstSideGroup(
    SOperatorInfo* pOperator, const SSDataBlock* pBlock) {
  if (pBlock->info.rows <= 0 || pBlock->info.blankFill) {
    return TSDB_CODE_SUCCESS;
  }

  STimeSliceOperatorInfo* pInfo = pOperator->info;
  SColumnInfoData* pTsCol = taosArrayGet(
      pBlock->pDataBlock, pInfo->queryCtx.tsCol.slotId);
  if (pTsCol == NULL ||
      (pTsCol->hasNull && colDataIsNull_f(pTsCol, 0))) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t refTs = *(int64_t*)colDataGetData(pTsCol, 0);
  if (!interpMainFirstEmittedGroupNeedsNotify(
          pInfo->pEmittedGroups, pBlock->info.id.groupId,
          pInfo->curPhase)) {
    return TSDB_CODE_SUCCESS;
  }
  return setDownstreamOpGetParam(pOperator, refTs, pBlock);
}

static int32_t processMainFirstSideBlock(SOperatorInfo* pOperator,
                                         SSDataBlock* pBlock) {
  STimeSliceOperatorInfo* pInfo = pOperator->info;
  uint64_t groupId = pBlock->info.id.groupId;
  bool emitted = interpMainFirstGroupWasEmitted(pInfo->pEmittedGroups,
                                                groupId);
  SMainFirstGroupState* pGroup = emitted
                                     ? NULL
                                     : getMainFirstGroup(pInfo, groupId);
  EInterpSideGroupAction action = interpSideGroupAction(
      pInfo->interpMode, pGroup != NULL, emitted);
  if (action == INTERP_SIDE_GROUP_DROP) {
    return notifyMultiGroupSidePhaseResolved(
        pOperator, pBlock, false, 0);
  }
  if (action == INTERP_SIDE_GROUP_NOTIFY) {
    // The group needs no more values, but its active side reader still needs
    // the boundary notification before this block can be discarded.
    return notifyEmittedMainFirstSideGroup(pOperator, pBlock);
  }

  int32_t code = TSDB_CODE_SUCCESS;
  if (action == INTERP_SIDE_GROUP_CREATE) {
    code = createMainFirstGroup(pInfo, pOperator, pBlock, &pGroup);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  code = applyTimeSliceScalar(pOperator, pInfo, pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = copyPrevGroupKey(&pOperator->exprSupp,
                          pGroup->interp.pPrevGroupKeys, pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (pBlock->info.blankFill) {
    return notifyMultiGroupSidePhaseResolved(
        pOperator, pBlock, false, 0);
  }

  captureMainFirstGroupRow(pGroup, pBlock);

  code = finalizeMainFirstGroup(pInfo, pGroup);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // The other side may still block output. This side's unused values need
  // no timeline validation; its block only identifies the active reader.
  if (isMainFirstGroupSideResolved(pInfo, pGroup)) {
    return notifyMainFirstGroupSideResolved(pOperator, pGroup, pBlock, false,
                                            0);
  }

  SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock,
                                         pInfo->queryCtx.tsCol.slotId);
  bool foundRef = false;
  int64_t refTs = 0;
  for (int32_t i = 0; i < pBlock->info.rows; ++i) {
    if (pTsCol->hasNull && colDataIsNull_f(pTsCol, i)) {
      return TSDB_CODE_FUNC_INVALID_TIMELINE;
    }
    if (checkNullRow(&pOperator->exprSupp, pBlock, i,
                     pInfo->queryCtx.igNull)) {
      continue;
    }

    int64_t ts = *(int64_t*)colDataGetData(pTsCol, i);
    EInvalidTimestampReason reason = INVALID_TIMESTAMP_REASON_NONE;
    code = validateMainFirstSideTimestamp(&pInfo->queryCtx, pGroup,
                                          pBlock, ts, pInfo->curPhase,
                                          &reason);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (reason == INVALID_TIMESTAMP_REASON_DUPLICATE) {
      continue;
    }

    if (pInfo->curPhase == SCAN_PHASE_NEXT &&
        !pGroup->interp.extNextRowSet) {
      doKeepRows(pGroup->interp.pExtNextRow, pBlock, i);
      pGroup->interp.extNextRowSet = true;
      foundRef = true;
      refTs = ts;
    } else if (pInfo->curPhase == SCAN_PHASE_PREV &&
               !pGroup->interp.extPrevRowSet) {
      doKeepRows(pGroup->interp.pExtPrevRow, pBlock, i);
      pGroup->interp.extPrevRowSet = true;
      foundRef = true;
      refTs = ts;
    }
    if (foundRef) {
      break;
    }
  }

  code = refreshMainFirstGroupReadiness(pInfo, pGroup);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  return notifyMainFirstGroupSideResolved(pOperator, pGroup, pBlock, foundRef,
                                         refTs);
}

static void setMainFirstLinearEndpoint(const SInterpQueryCtx* pCtx,
                                       SGroupInterpState* pState,
                                       const SArray* pRow, bool start) {
  SGroupKeys* pTsKey = taosArrayGet(pRow, pCtx->tsCol.slotId);
  int64_t ts = *(int64_t*)pTsKey->pData;
  int32_t numOfCols = taosArrayGetSize(pState->pLinearInfo);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SFillLinearInfo* pLinear = taosArrayGet(pState->pLinearInfo, i);
    SGroupKeys* pKey = taosArrayGet(pRow, i);
    if (!IS_MATHABLE_TYPE(pKey->type)) {
      continue;
    }

    SPoint* pPoint = start ? &pLinear->start : &pLinear->end;
    pPoint->key = pKey->isNull ? INT64_MIN : ts;
    if (!pKey->isNull) {
      copyColDataVal(pPoint->val, pKey->pData, pKey->type,
                     pKey->bytes);
    }
    if (start) {
      pLinear->isStartSet = true;
    } else {
      pLinear->isEndSet = true;
    }
  }
}

static void prepareMainFirstEdgeRefs(const SInterpQueryCtx* pCtx,
                                     SMainFirstGroupState* pGroup,
                                     EInterpOutputStage stage) {
  SGroupInterpState* pState = &pGroup->interp;
  const SArray* pPrev = NULL;
  const SArray* pNext = NULL;

  // A group with no MAIN row at all (fully empty) treats both edge stages as
  // the whole-window hole; firstMainRowSet doubles as that "no data" flag
  // since it's frozen once MAIN finalizes the group.
  if (!pGroup->firstMainRowSet) {
    if (pState->extPrevRowSet) {
      pPrev = pState->pExtPrevRow;
    }
    if (pState->extNextRowSet) {
      pNext = pState->pExtNextRow;
    }
  } else if (stage == INTERP_OUTPUT_LEAD) {
    if (pState->extPrevRowSet) {
      pPrev = pState->pExtPrevRow;
    }
    pNext = pGroup->pFirstMainRow;
  } else {
    pPrev = pGroup->pLastMainRow;
    if (pState->extNextRowSet) {
      pNext = pState->pExtNextRow;
    }
  }

  resetRowsKeeper(pState->pPrevRow, &pState->isPrevRowSet);
  resetRowsKeeper(pState->pNextRow, &pState->isNextRowSet);
  resetFillLinearInfo(pState);
  if (NULL != pPrev) {
    copyKeeperRow(pState->pPrevRow, pPrev);
    pState->isPrevRowSet = true;
    setMainFirstLinearEndpoint(pCtx, pState, pPrev, true);
  } else if (pGroup->groupRowSet) {
    copyKeeperRow(pState->pPrevRow, pGroup->pGroupRow);
  }
  if (NULL != pNext) {
    copyKeeperRow(pState->pNextRow, pNext);
    pState->isNextRowSet = true;
    setMainFirstLinearEndpoint(pCtx, pState, pNext, false);
  }
}

static bool mainFirstLinearRefsReady(const SInterpQueryCtx* pCtx,
                                     const SGroupInterpState* pState,
                                     SExprSupp* pExprSup) {
  if (NULL == pState->pLinearInfo) {
    return false;
  }

  for (int32_t i = 0; i < pExprSup->numOfExprs; ++i) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[i];
    if (!isInterpFunc(pExprInfo) &&
        !isIrowtsOriginPseudoColumn(pExprInfo)) {
      continue;
    }

    int32_t srcSlot = isIrowtsOriginPseudoColumn(pExprInfo)
                          ? pCtx->tsCol.slotId
                          : pExprInfo->base.pParam[0].pCol->slotId;
    SFillLinearInfo* pLinear =
        taosArrayGet(pState->pLinearInfo, srcSlot);
    if (NULL == pLinear || !IS_MATHABLE_TYPE(pLinear->type)) {
      continue;
    }
    if (!pLinear->isStartSet || !pLinear->isEndSet) {
      return false;
    }
  }
  return true;
}

static bool mainFirstEdgeCanProduce(const SInterpQueryCtx* pCtx,
                                    const SGroupInterpState* pState,
                                    SExprSupp* pExprSup) {
  bool linearReady = pCtx->fillType != TSDB_FILL_LINEAR ||
                     mainFirstLinearRefsReady(pCtx, pState, pExprSup);
  return interpMainFirstEdgeCanProduce(pCtx->fillType, linearReady,
                                       pState->isPrevRowSet,
                                       pState->isNextRowSet);
}

static bool getMainFirstEdgeRange(const SInterpQueryCtx* pCtx,
                                  const SMainFirstGroupState* pGroup,
                                  EInterpOutputStage stage,
                                  STimeWindow* pRange) {
  if (stage == INTERP_OUTPUT_LEAD) {
    if (!pGroup->firstMainRowSet) {
      *pRange = pCtx->win;
      return true;
    }
    if (pGroup->interp.leadHoleSet) {
      *pRange = pGroup->interp.leadHole;
      return true;
    }
    return false;
  }

  if (stage == INTERP_OUTPUT_TRAIL &&
      pGroup->firstMainRowSet && pGroup->interp.trailHoleSet) {
    *pRange = pGroup->interp.trailHole;
    return true;
  }
  return false;
}

static void clearMainFirstOutputPks(SDataBlockInfo* pInfo) {
  for (int32_t i = 0; i < 2; ++i) {
    if (IS_VAR_DATA_TYPE(pInfo->pks[i].type)) {
      taosMemoryFreeClear(pInfo->pks[i].pData);
    }
    memset(&pInfo->pks[i], 0, sizeof(pInfo->pks[i]));
  }
}

static void initMainFirstOutputInfo(SSDataBlock* pBlock,
                                    const SBlockID* pBlockId,
                                    uint8_t scanFlag,
                                    int8_t scanPhase) {
  pBlock->info.window = (STimeWindow){0};
  pBlock->info.id = *pBlockId;
  pBlock->info.dataLoad = 0;
  pBlock->info.scanFlag = scanFlag;
  pBlock->info.scanPhase = scanPhase;
  pBlock->info.blankFill = false;
  clearMainFirstOutputPks(&pBlock->info);
  pBlock->info.pBlockAgg = NULL;
  memset(pBlock->info.parTbName, 0,
         sizeof(pBlock->info.parTbName));
}

static bool mainFirstOutputInfoMatches(const SSDataBlock* pBlock,
                                       const SBlockID* pBlockId,
                                       uint8_t scanFlag,
                                       int8_t scanPhase) {
  const SBlockID* pCurrent = &pBlock->info.id;
  return pCurrent->uid == pBlockId->uid &&
         pCurrent->blockId == pBlockId->blockId &&
         pCurrent->groupId == pBlockId->groupId &&
         pCurrent->baseGId == pBlockId->baseGId &&
         pBlock->info.scanFlag == scanFlag &&
         pBlock->info.scanPhase == scanPhase;
}

static int32_t appendMainFirstStoredBlock(
    STimeSliceOperatorInfo* pInfo) {
  SInterpGroupOutput* pOutput = &pInfo->output;
  SSDataBlock* pDest = pInfo->pRes;
  SSDataBlock* pSrc = pOutput->pStoredBlock;
  if (pSrc == NULL || pOutput->storedRowOffset < 0 ||
      pOutput->storedRowOffset > pSrc->info.rows ||
      pDest->info.rows < 0 ||
      pDest->info.rows > pDest->info.capacity) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int64_t srcRemain = pSrc->info.rows - pOutput->storedRowOffset;
  if (srcRemain == 0) {
    pOutput->pStoredBlock = NULL;
    pOutput->storedRowOffset = 0;
    return TSDB_CODE_SUCCESS;
  }

  int64_t available =
      (int64_t)pDest->info.capacity - pDest->info.rows;
  if (available <= 0) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  if (pDest->info.rows == 0 && pOutput->storedRowOffset == 0 &&
      srcRemain <= available && srcRemain <= INT32_MAX) {
    int64_t version = pDest->info.version;
    int32_t childId = pDest->info.childId;
    STimeWindow calWin = pDest->info.calWin;
    TSKEY watermark = pDest->info.watermark;
    int32_t code = copyDataBlock(pDest, pSrc);
    pDest->info.version = version;
    pDest->info.childId = childId;
    pDest->info.calWin = calWin;
    pDest->info.watermark = watermark;
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    initMainFirstOutputInfo(pDest, &pSrc->info.id,
                            pSrc->info.scanFlag,
                            pSrc->info.scanPhase);
    pOutput->pStoredBlock = NULL;
    pOutput->storedRowOffset = 0;
    return TSDB_CODE_SUCCESS;
  }

  int64_t copyRows = TMIN(srcRemain, available);
  if (pOutput->storedRowOffset > INT32_MAX ||
      copyRows > INT32_MAX) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  if (pDest->info.rows == 0) {
    initMainFirstOutputInfo(pDest, &pSrc->info.id,
                            pSrc->info.scanFlag,
                            pSrc->info.scanPhase);
  }

  int32_t code = blockDataMergeNRows(
      pDest, pSrc, (int32_t)pOutput->storedRowOffset,
      (int32_t)copyRows);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pOutput->storedRowOffset += copyRows;
  if (pOutput->storedRowOffset == pSrc->info.rows) {
    pOutput->pStoredBlock = NULL;
    pOutput->storedRowOffset = 0;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t produceMainFirstEdge(SOperatorInfo* pOperator,
                                    STimeWindow range) {
  STimeSliceOperatorInfo* pInfo = pOperator->info;
  SInterpGroupOutput* pOutput = &pInfo->output;
  SMainFirstGroupState* pGroup = pOutput->pGroup;
  SGroupInterpState* pState = &pGroup->interp;

  if (!pOutput->edgePrepared) {
    prepareMainFirstEdgeRefs(&pInfo->queryCtx, pGroup,
                             pOutput->stage);
    pState->current = range.skey;
    pOutput->edgePrepared = true;
    pOutput->edgeDone = false;
    pOutput->edgeAbandoned = false;

    // Edge references are frozen until this stage advances. If they cannot
    // produce one point, no later point in the same range can become ready.
    if (!mainFirstEdgeCanProduce(&pInfo->queryCtx, pState,
                                 &pOperator->exprSupp)) {
      pOutput->edgeAbandoned = true;
      return TSDB_CODE_SUCCESS;
    }
  }

  if (pInfo->pRes->info.rows == 0) {
    initMainFirstOutputInfo(pInfo->pRes, &pGroup->blockId,
                            pGroup->scanFlag, SCAN_PHASE_MAIN);
  }
  bool beforeTs = pOutput->stage == INTERP_OUTPUT_LEAD;
  while (!pOutput->edgeDone && pState->current <= range.ekey) {
    (void)genInterpolationResult(&pInfo->queryCtx, pState,
                                 &pOperator->exprSupp, pInfo->pRes,
                                 NULL, 0, beforeTs,
                                 pOperator->pTaskInfo);
    if (pState->current == range.ekey) {
      pOutput->edgeDone = true;
      break;
    }

    bool calendarInterval =
        IS_CALENDAR_TIME_DURATION(pInfo->queryCtx.interval.intervalUnit) ||
        IS_CALENDAR_DAY_DURATION(pInfo->queryCtx.interval.intervalUnit);
    int64_t nextTs = 0;
    if (!calendarInterval) {
      int64_t interval = pInfo->queryCtx.interval.interval;
      if (interval <= 0) {
        return TSDB_CODE_TIME_ERROR;
      }
      if (pState->current > INT64_MAX - interval) {
        pOutput->edgeDone = true;
        break;
      }
      nextTs = pState->current + interval;
      if (nextTs > range.ekey) {
        pOutput->edgeDone = true;
        break;
      }
    } else {
      nextTs = getNextTimestamp(pState->current,
                                &pInfo->queryCtx.interval);
    }
    if (nextTs <= pState->current) {
      return TSDB_CODE_TIME_ERROR;
    }
    pState->current = nextTs;
    if (pState->current > range.ekey) {
      pOutput->edgeDone = true;
    }
    if (checkThresholdReached(pInfo,
                              pOperator->resultInfo.threshold)) {
      break;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t finishMainFirstOutputGroup(STimeSliceOperatorInfo* pInfo) {
  SMainFirstGroupState* pGroup = pInfo->output.pGroup;
  uint64_t groupId = pGroup->interp.groupId;
  uint8_t emitted = INTERP_GROUP_EMITTED | pGroup->sideNotified;
  int32_t code = taosHashPut(pInfo->pEmittedGroups, &groupId,
                             sizeof(groupId), &emitted,
                             sizeof(emitted));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = taosHashRemove(pInfo->pGroupStates, &groupId,
                        sizeof(groupId));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  mainFirstGroupStateDestroy(pGroup);
  memset(&pInfo->output, 0, sizeof(pInfo->output));
  return TSDB_CODE_SUCCESS;
}

static bool startNextMainFirstOutput(STimeSliceOperatorInfo* pInfo) {
  int32_t numOfGroups = taosArrayGetSize(pInfo->pCompletedGroups);
  uint64_t groupId = 0;
  while (interpMainFirstNextCompletedGroup(pInfo->pCompletedGroups,
                                            &pInfo->completedIndex,
                                            &groupId)) {
    SMainFirstGroupState* pGroup =
        getMainFirstGroup(pInfo, groupId);
    if (NULL == pGroup) {
      continue;
    }

    pInfo->output.pGroup = pGroup;
    pInfo->output.stage = INTERP_OUTPUT_LEAD;
    return true;
  }

  if (pInfo->completedIndex == numOfGroups && numOfGroups > 0) {
    taosArrayClear(pInfo->pCompletedGroups);
    pInfo->completedIndex = 0;
  }
  return false;
}

// LEAD moves on to the buffered STORE results; TRAIL (or a group with no
// edge range at all) is the last stage before the group is fully emitted.
static void advanceMainFirstEdgeStage(SInterpGroupOutput* pOutput) {
  pOutput->edgePrepared = false;
  pOutput->edgeDone = false;
  pOutput->edgeAbandoned = false;
  pOutput->stage = pOutput->stage == INTERP_OUTPUT_LEAD
                       ? INTERP_OUTPUT_STORE
                       : INTERP_OUTPUT_NONE;
}

// Filters pInfo->pRes in place and reports whether it still has rows to
// emit. Shared by the edge-row and stored-result output paths so both apply
// the same empty/filtered-to-nothing handling.
static int32_t filterMainFirstOutput(SOperatorInfo* pOperator,
                                     bool* pProduced) {
  STimeSliceOperatorInfo* pInfo = pOperator->info;
  if (pInfo->pRes->info.rows <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo,
                          NULL, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  *pProduced = pInfo->pRes->info.rows > 0;
  return TSDB_CODE_SUCCESS;
}

static int32_t produceMainFirstGroupOutput(SOperatorInfo* pOperator,
                                           bool* pProduced) {
  STimeSliceOperatorInfo* pInfo = pOperator->info;
  SInterpGroupOutput* pOutput = &pInfo->output;
  *pProduced = false;

  while (NULL != pOutput->pGroup || startNextMainFirstOutput(pInfo)) {
    STimeWindow edgeRange = {0};
    if (pOutput->stage == INTERP_OUTPUT_LEAD ||
        pOutput->stage == INTERP_OUTPUT_TRAIL) {
      if (getMainFirstEdgeRange(&pInfo->queryCtx, pOutput->pGroup,
                                pOutput->stage, &edgeRange)) {
        SMainFirstGroupState* pGroup = pOutput->pGroup;
        if (pInfo->pRes->info.rows > 0 &&
            !mainFirstOutputInfoMatches(
                pInfo->pRes, &pGroup->blockId, pGroup->scanFlag,
                SCAN_PHASE_MAIN)) {
          int32_t code = filterMainFirstOutput(pOperator, pProduced);
          if (code != TSDB_CODE_SUCCESS || *pProduced) {
            return code;
          }
          continue;
        }

        int32_t code = produceMainFirstEdge(pOperator, edgeRange);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        if (pOutput->edgeDone || pOutput->edgeAbandoned) {
          advanceMainFirstEdgeStage(pOutput);
        }
        if (checkThresholdReached(
                pInfo, pOperator->resultInfo.threshold)) {
          code = filterMainFirstOutput(pOperator, pProduced);
          if (code != TSDB_CODE_SUCCESS || *pProduced) {
            return code;
          }
        }
        continue;
      }
      advanceMainFirstEdgeStage(pOutput);
    }

    if (pOutput->stage == INTERP_OUTPUT_STORE) {
      if (pOutput->pGroup->hasStoredResults) {
        if (!pOutput->storeOpened) {
          int32_t code = interpResultStoreOpenRead(pInfo->pResultStore,
                                                   pOutput->pGroup->interp
                                                       .groupId);
          if (code != TSDB_CODE_SUCCESS) {
            return code;
          }
          pOutput->storeOpened = true;
        }

        if (pOutput->pStoredBlock == NULL) {
          int32_t code = interpResultStoreNextBlock(
              pInfo->pResultStore, &pOutput->pStoredBlock);
          if (code != TSDB_CODE_SUCCESS) {
            return code;
          }
          pOutput->storedRowOffset = 0;
        }

        SSDataBlock* pStored = pOutput->pStoredBlock;
        if (pStored != NULL) {
          if (pInfo->pRes->info.rows > 0 &&
              !mainFirstOutputInfoMatches(
                  pInfo->pRes, &pStored->info.id,
                  pStored->info.scanFlag,
                  pStored->info.scanPhase)) {
            int32_t code = filterMainFirstOutput(
                pOperator, pProduced);
            if (code != TSDB_CODE_SUCCESS || *pProduced) {
              return code;
            }
            continue;
          }

          int32_t code = appendMainFirstStoredBlock(pInfo);
          if (code != TSDB_CODE_SUCCESS) {
            return code;
          }
          if (checkThresholdReached(
                  pInfo, pOperator->resultInfo.threshold) ||
              pInfo->pRes->info.rows ==
                  pInfo->pRes->info.capacity) {
            code = filterMainFirstOutput(pOperator, pProduced);
            if (code != TSDB_CODE_SUCCESS || *pProduced) {
              return code;
            }
          }
          continue;
        }

        int32_t code = interpResultStoreDropGroup(
            pInfo->pResultStore,
            pOutput->pGroup->interp.groupId);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      pOutput->stage = INTERP_OUTPUT_TRAIL;
      pOutput->edgePrepared = false;
      pOutput->edgeDone = false;
      pOutput->edgeAbandoned = false;
      continue;
    }

    if (pOutput->stage == INTERP_OUTPUT_NONE) {
      if (pInfo->pRes->info.rows > 0) {
        int32_t code = filterMainFirstOutput(pOperator, pProduced);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        if (*pProduced) {
          return TSDB_CODE_SUCCESS;
        }
      }

      int32_t code = finishMainFirstOutputGroup(pInfo);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }
  return pInfo->pRes->info.rows == 0
             ? TSDB_CODE_SUCCESS
             : TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
}

static void resetTimesliceInfo(STimeSliceOperatorInfo* pSliceInfo) {
  interpGroupStateReset(&pSliceInfo->queryCtx, &pSliceInfo->groupState);
  pSliceInfo->prevNotified = false;
  pSliceInfo->nextNotified = false;
}

static void doHandleTimeslice(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  STimeSliceOperatorInfo* pSliceInfo = pOperator->info;
  SInterpQueryCtx*        pCtx = &pSliceInfo->queryCtx;
  SGroupInterpState*      pState = &pSliceInfo->groupState;
  SExprSupp*              pSup = &pOperator->exprSupp;
  int32_t                 order = TSDB_ORDER_ASC;

  pSliceInfo->pRes->info.id.groupId = pBlock->info.id.groupId;

  if (pSup->pFilterInfo != NULL) {
    filterSetExecContext(pSup->pFilterInfo, pTaskInfo, isTaskKilled);
  }

  if (checkWindowBoundReached(pCtx, pState)) {
    code = setDownstreamOpGetParam(
        pOperator, pCtx->win.ekey + 1, pBlock);
    QUERY_CHECK_CODE(code, lino, _end);
    goto _end;
  }

  code = initKeeperInfo(pState, pBlock, &pOperator->exprSupp);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pSliceInfo->scalarSup.pExprInfo != NULL) {
    SExprSupp* pExprSup = &pSliceInfo->scalarSup;
    code = projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock,
                                 pExprSup->pCtx, pExprSup->numOfExprs, NULL,
                                 GET_STM_RTINFO(pOperator->pTaskInfo), pOperator->pTaskInfo);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  // the pDataBlock are always the same one, no need to call this again
  code = setInputDataBlock(pSup, pBlock, order, MAIN_SCAN, true);
  QUERY_CHECK_CODE(code, lino, _end);
  doTimesliceImpl(pOperator, pSliceInfo, pCtx, pState, pBlock, pTaskInfo);
  code = copyPrevGroupKey(&pOperator->exprSupp, pState->pPrevGroupKeys, pBlock);
  QUERY_CHECK_CODE(code, lino, _end);

  // Legacy input is PREV->MAIN->NEXT. PREV only keeps references, so
  // discarded results cannot later gain MAIN membership.
  interpLegacyApplyGroupMembership(pCtx->requireMainMembership,
                                   pState->mainSeen,
                                   pSliceInfo->pRes);

  if (checkWindowBoundReached(pCtx, pState)) {
    // All interpolation points are produced. Arm the step-done notification
    // right away so the very next fetch lets the reader skip the remaining
    // main-window data instead of wasting one more block read.
    code = setDownstreamOpGetParam(
        pOperator, pCtx->win.ekey + 1, pBlock);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

static int32_t validateTimeSliceInputPhase(STimeSliceOperatorInfo* pInfo,
                                           const SSDataBlock* pBlock) {
  if (INTERP_MODE_LEGACY == pInfo->interpMode) {
    return pBlock->info.scanPhase == SCAN_PHASE_MAIN
               ? TSDB_CODE_SUCCESS
               : TSDB_CODE_PLAN_INTERNAL_ERROR;
  }
  if (!interpModeUsesPhasedProtocol(pInfo->interpMode)) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  int32_t code = interpValidatePhaseTransition(pInfo->curPhase,
                                               pBlock->info.scanPhase);
  if (TSDB_CODE_SUCCESS == code) {
    pInfo->curPhase = pBlock->info.scanPhase;
  }
  return code;
}

static int32_t prepareMainFirstInputBlock(STimeSliceOperatorInfo* pInfo,
                                          const SSDataBlock* pBlock,
                                          bool* pDeferred) {
  *pDeferred = false;
  int8_t inputPhase = pBlock->info.scanPhase;
  bool groupFirst = pInfo->interpMode == INTERP_MODE_GROUP_FIRST;
  uint64_t inputOuterEpoch =
      groupFirst ? pBlock->info.id.groupId :
      pInfo->interpMode == INTERP_MODE_MULTI_GROUP
          ? pBlock->info.id.baseGId
          : 0;

  if (!pInfo->outerEpochSet) {
    pInfo->outerEpoch = inputOuterEpoch;
    pInfo->outerEpochSet = true;
  } else if (!groupFirst && inputOuterEpoch < pInfo->outerEpoch) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  } else if (inputOuterEpoch != pInfo->outerEpoch) {
    int32_t code = interpValidatePhaseTransition(SCAN_PHASE_MAIN,
                                                 inputPhase);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    code = finishAllMainFirstPhases(pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pInfo->outerEpochDraining = true;
    *pDeferred = true;
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = interpValidatePhaseTransition(pInfo->curPhase,
                                               inputPhase);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (inputPhase > pInfo->curPhase) {
    code = advanceMainFirstPhase(pInfo, inputPhase);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pInfo->inputGroupId = pBlock->info.id.groupId;
    pInfo->inputGroupSet = true;
    *pDeferred = true;
    return TSDB_CODE_SUCCESS;
  }

  uint64_t groupId = pBlock->info.id.groupId;
  if (!pInfo->inputGroupSet) {
    pInfo->inputGroupId = groupId;
    pInfo->inputGroupSet = true;
  } else if (pInfo->inputGroupId != groupId) {
    // mainFirstGroupSeqTableScan() in scanoperator.c drains one per-group
    // reader before advancing, so groupId changes delimit complete groups.
    if (pInfo->curPhase == SCAN_PHASE_MAIN) {
      code = finalizeMainFirstInputGroup(pInfo);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      *pDeferred = true;
    }
    pInfo->inputGroupId = groupId;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t doTimesliceMainFirstNext(SOperatorInfo* pOperator,
                                        SSDataBlock** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  STimeSliceOperatorInfo* pInfo = pOperator->info;
  if (pOperator->status == OP_EXEC_DONE) {
    *ppRes = NULL;
    return TSDB_CODE_SUCCESS;
  }
  blockDataCleanup(pInfo->pRes);

  while (true) {
    bool produced = false;
    if (pInfo->pRes->info.rows == 0) {
      code = produceMainFirstGroupOutput(pOperator, &produced);
      QUERY_CHECK_CODE(code, lino, _finished);
      if (produced) {
        break;
      }
    }

    if (pInfo->outerEpochDraining) {
      if (pInfo->pNextGroupRes == NULL) {
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        QUERY_CHECK_CODE(code, lino, _finished);
      }
      resetMainFirstOuterEpoch(
          pInfo, pInfo->interpMode == INTERP_MODE_GROUP_FIRST
                     ? pInfo->pNextGroupRes->info.id.groupId
                     : pInfo->pNextGroupRes->info.id.baseGId);
      continue;
    }

    if (pInfo->inputDone) {
      clearMainFirstGroupStates(pInfo);
      setOperatorCompleted(pOperator);
      break;
    }

    SSDataBlock* pBlock = pInfo->pRemainRes != NULL
                             ? pInfo->pRemainRes : pInfo->pNextGroupRes;
    if (NULL == pBlock) {
      pBlock = getNextInterpBlock(pOperator, true);
      if (NULL == pBlock) {
        qDebug("%s TimeSlice MAIN_FIRST natural EOF",
               GET_TASKID(pOperator->pTaskInfo));
        code = finishAllMainFirstPhases(pInfo);
        QUERY_CHECK_CODE(code, lino, _finished);
        pInfo->inputDone = true;
        code = filterMainFirstOutput(pOperator, &produced);
        QUERY_CHECK_CODE(code, lino, _finished);
        if (produced) {
          break;
        }
        continue;
      }
    }

    // Flush the streamed prefix before the shared result block is reused
    // by phase closing or by another group's interpolation state.
    if (pInfo->pRes->info.rows > 0 &&
        (pBlock->info.scanPhase != SCAN_PHASE_MAIN ||
         pBlock->info.id.groupId != pInfo->inputGroupId)) {
      pInfo->pNextGroupRes = pBlock;
      code = filterMainFirstOutput(pOperator, &produced);
      QUERY_CHECK_CODE(code, lino, _finished);
      if (produced) {
        break;
      }
    }

    bool deferred = false;
    code = prepareMainFirstInputBlock(pInfo, pBlock, &deferred);
    QUERY_CHECK_CODE(code, lino, _finished);
    if (deferred) {
      pInfo->pNextGroupRes = pBlock;
      continue;
    }

    printDataBlock(pBlock, "doTimesliceMainFirstNext",
                   GET_TASKID(pOperator->pTaskInfo),
                   pOperator->pTaskInfo->id.queryId);
    qDebug("%s TimeSlice MAIN_FIRST input phase=%d groupId=%" PRIu64
           " rows=%" PRId64,
           GET_TASKID(pOperator->pTaskInfo), pInfo->curPhase,
           pBlock->info.id.groupId, pBlock->info.rows);
    if (pInfo->curPhase == SCAN_PHASE_MAIN) {
      code = processMainFirstMainBlock(pOperator, pBlock);
    } else {
      code = processMainFirstSideBlock(pOperator, pBlock);
    }
    QUERY_CHECK_CODE(code, lino, _finished);
    pInfo->pNextGroupRes = NULL;
    if (checkThresholdReached(pInfo, pOperator->resultInfo.threshold)) {
      code = filterMainFirstOutput(pOperator, &produced);
      QUERY_CHECK_CODE(code, lino, _finished);
      if (produced) {
        break;
      }
    }
  }

_finished:
  setTaskStatus(pTaskInfo, TASK_COMPLETED);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino,
           tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }

  *ppRes = pInfo->pRes->info.rows == 0 ? NULL : pInfo->pRes;
  return TSDB_CODE_SUCCESS;
}

static int32_t doTimesliceNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;
  STimeSliceOperatorInfo* pSliceInfo = pOperator->info;
  SInterpQueryCtx*        pCtx = &pSliceInfo->queryCtx;
  SGroupInterpState*      pState = &pSliceInfo->groupState;
  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  } else if (pOperator->status == OP_NOT_OPENED && pCtx->pWin) {
    code = streamCalcCurrWinTimeRange((STimeRangeNode*)pCtx->pWin,
                                       &pTaskInfo->pStreamRuntimeInfo->funcInfo,
                                       &pCtx->win, NULL, 3);
    QUERY_CHECK_CODE(code, lino, _finished);
    OPTR_SET_OPENED(pOperator);    
    pState->current = pCtx->win.skey;
  }

  SSDataBlock* pResBlock = pSliceInfo->pRes;
  blockDataCleanup(pResBlock);

  if (IS_STREAM_MODE(pTaskInfo)) {
    /**
      For stream calculation, the interp operator is triggered by the window,
      so we need to reset the notified status for each window.
    */
    pSliceInfo->prevNotified = false;
    pSliceInfo->nextNotified = false;
  }

  while (1) {
    if (pSliceInfo->pNextGroupRes != NULL) {
      doHandleTimeslice(pOperator, pSliceInfo->pNextGroupRes);
      if (checkWindowBoundReached(pCtx, pState) ||
          checkThresholdReached(pSliceInfo, pOperator->resultInfo.threshold)) {
        code = doFilter(pResBlock, pOperator->exprSupp.pFilterInfo, NULL, NULL);
        QUERY_CHECK_CODE(code, lino, _finished);
        if (pSliceInfo->pRemainRes == NULL) {
          pSliceInfo->pNextGroupRes = NULL;
        }
        if (pResBlock->info.rows != 0) {
          goto _finished;
        } else {
          // after fillter if result block has 0 rows, go back to
          // process pNextGroupRes again for unfinished data
          continue;
        }
      }
      pSliceInfo->pNextGroupRes = NULL;
    }

    while (1) {
      SSDataBlock* pBlock = pSliceInfo->pRemainRes
                                ? pSliceInfo->pRemainRes
                                : getNextInterpBlock(pOperator, false);
      if (pBlock == NULL) {
        setOperatorCompleted(pOperator);
        break;
      }
      code = validateTimeSliceInputPhase(pSliceInfo, pBlock);
      QUERY_CHECK_CODE(code, lino, _finished);
      printDataBlock(pBlock, "doTimesliceNext",
                    GET_TASKID(pOperator->pTaskInfo),
                    pOperator->pTaskInfo->id.queryId);

      pResBlock->info.scanFlag = pBlock->info.scanFlag;
      if (pState->groupId == 0 && pBlock->info.id.groupId != 0) {
        pState->groupId = pBlock->info.id.groupId;
      } else {
        if (pState->groupId != pBlock->info.id.groupId) {
          pState->groupId = pBlock->info.id.groupId;
          pSliceInfo->pNextGroupRes = pBlock;
          break;
        }
      }

      doHandleTimeslice(pOperator, pBlock);
      if (checkWindowBoundReached(pCtx, pState) ||
          checkThresholdReached(pSliceInfo, pOperator->resultInfo.threshold)) {
        code = doFilter(pResBlock, pOperator->exprSupp.pFilterInfo, NULL, NULL);
        QUERY_CHECK_CODE(code, lino, _finished);
        if (pResBlock->info.rows != 0) {
          goto _finished;
        }
      }
    }
    // post work for a specific group

    // check if need to interpolate after last datablock
    // except for fill(next), fill(linear)
    genInterpAfterDataBlock(pCtx, pState, pSliceInfo, pOperator, 0);

    interpLegacyApplyGroupMembership(pCtx->requireMainMembership,
                                     pState->mainSeen, pResBlock);

    code = doFilter(pResBlock, pOperator->exprSupp.pFilterInfo, NULL, NULL);
    QUERY_CHECK_CODE(code, lino, _finished);
    if (pOperator->status == OP_EXEC_DONE) {
      break;
    }

    // restore initial value for next group
    resetTimesliceInfo(pSliceInfo);
    if (pResBlock->info.rows != 0) {
      break;
    }
  }

_finished:
  // restore the value
  setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
  if (pResBlock->info.rows == 0) {
    pOperator->status = OP_EXEC_DONE;
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }

  (*ppRes) = pResBlock->info.rows == 0 ? NULL : pResBlock;
  return code;
}

static int32_t doTimesliceNextMerged(SOperatorInfo* pOperator,
                                     SSDataBlock** ppRes) {
  STimeSliceOperatorInfo* pInfo = pOperator->info;
  SSDataBlock* pMerged = pInfo->pMergedRes;
  __optr_fn_t nextFn = interpModeUsesPhasedProtocol(pInfo->interpMode)
                          ? doTimesliceMainFirstNext
                          : doTimesliceNext;

  *ppRes = NULL;
  blockDataCleanup(pMerged);
  while (pMerged->info.rows < pOperator->resultInfo.threshold) {
    SSDataBlock* pBlock = NULL;
    int32_t code = nextFn(pOperator, &pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (pBlock == NULL) {
      break;
    }
    if (pMerged->info.rows == 0 &&
        pBlock->info.rows >= pOperator->resultInfo.threshold) {
      pBlock->info.id.groupId = 0;
      *ppRes = pBlock;
      return TSDB_CODE_SUCCESS;
    }

    code = blockDataMerge(pMerged, pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pMerged->info.scanFlag = pBlock->info.scanFlag;
    pMerged->info.version = pBlock->info.version;
  }

  pMerged->info.id.groupId = 0;
  *ppRes = pMerged->info.rows == 0 ? NULL : pMerged;
  return TSDB_CODE_SUCCESS;
}

static int32_t resetTimeSliceOperState(SOperatorInfo* pOper) {
  STimeSliceOperatorInfo* pInfo = pOper->info;
  SExecTaskInfo*           pTaskInfo = pOper->pTaskInfo;
  SInterpFuncPhysiNode* pPhynode = (SInterpFuncPhysiNode*)pOper->pPhyNode;
  pOper->status = OP_NOT_OPENED;

  setTaskStatus(pOper->pTaskInfo, TASK_NOT_COMPLETED);

  int32_t code = resetExprSupp(&pOper->exprSupp, pTaskInfo, pPhynode->pFuncs,
                               NULL, &pTaskInfo->storageAPI.functionStore);
  if (code == 0) {
    code = resetExprSupp(&pInfo->scalarSup, pTaskInfo, pPhynode->pExprs, NULL,
                         &pTaskInfo->storageAPI.functionStore);
  }

  interpGroupStateReset(&pInfo->queryCtx, &pInfo->groupState);
  pInfo->groupState.groupId = 0;
  pInfo->pNextGroupRes = NULL;
  pInfo->pRemainRes = NULL;
  pInfo->remainIndex = 0;
  pInfo->curPhase = SCAN_PHASE_MAIN;
  pInfo->remainFill = false;
  blockDataCleanup(pInfo->pRes);
  blockDataCleanup(pInfo->pMergedRes);

  if (interpModeUsesPhasedProtocol(pInfo->interpMode)) {
    resetMainFirstRuntime(pInfo);
  }

  // These keepers are built lazily from the input block layout. Drop them on
  // reset so the next execution rebuilds them from fresh input state.
  interpGroupStateClearKeepers(&pInfo->groupState);

  return code;
}

int32_t createTimeSliceOperatorInfo(SOperatorInfo* downstream,
                                    SPhysiNode* pPhyNode,
                                    SExecTaskInfo* pTaskInfo,
                                    SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                 code = 0;
  int32_t                 lino = 0;
  STimeSliceOperatorInfo* pInfo =
    taosMemoryCalloc(1, sizeof(STimeSliceOperatorInfo));
  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

  if (pOperator == NULL || pInfo == NULL) {
    code = terrno;
    goto _error;
  }
  initOperatorCostInfo(pOperator);

  pOperator->pPhyNode = pPhyNode;
  SInterpFuncPhysiNode* pInterpPhyNode = (SInterpFuncPhysiNode*)pPhyNode;
  if (INTERP_MODE_LEGACY != pInterpPhyNode->interpMode &&
      !interpModeUsesPhasedProtocol(pInterpPhyNode->interpMode)) {
    code = TSDB_CODE_PLAN_INTERNAL_ERROR;
    lino = __LINE__;
    goto _error;
  }

  SInterpQueryCtx*      pCtx = &pInfo->queryCtx;
  SExprSupp*            pSup = &pOperator->exprSupp;
  pInfo->interpMode = pInterpPhyNode->interpMode;
  pInfo->curPhase = SCAN_PHASE_MAIN;

  int32_t scanType = NULL == downstream || NULL == downstream->pPhyNode
                         ? -1
                         : nodeType(downstream->pPhyNode);
  bool tableScan = scanType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
  bool tableMergeScan =
      scanType == QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN;
  const char* scanKind = tableScan
                             ? "table"
                             : tableMergeScan ? "table_merge" : "other";
  STableScanPhysiNode* pScan = tableScan || tableMergeScan
                                   ? (STableScanPhysiNode*)
                                         downstream->pPhyNode
                                   : NULL;
  int32_t scanInterpMode = NULL == pScan ? -1 : pScan->interpMode;
  qDebug("%s TimeSlice runtime interpMode=%d scanKind=%s "
         "scanInterpMode=%d",
         GET_TASKID(pTaskInfo), pInfo->interpMode, scanKind,
         scanInterpMode);

  int32_t    numOfExprs = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pInterpPhyNode->pFuncs, NULL, &pExprInfo, &numOfExprs);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initExprSupp(pSup, pExprInfo, numOfExprs,
                      &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pInterpPhyNode->pExprs != NULL) {
    int32_t    num = 0;
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pInterpPhyNode->pExprs, NULL, &pScalarExprInfo, &num);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, num,
                        &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  code = filterInitFromNode((SNode*)pInterpPhyNode->node.pConditions,
                            &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  pCtx->tsCol =
      extractColumnFromColumnNode((SColumnNode*)pInterpPhyNode->pTimeSeries);
  pCtx->fillType = convertFillType(pInterpPhyNode->fillMode);
  pCtx->igNull = getIgnoreNullRes(pSup);
  pCtx->requireMainMembership =
      pInterpPhyNode->requireMainMembership;
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  pCtx->pFillColInfo = createFillColInfo(pExprInfo, numOfExprs, NULL, 0,
                                         NULL, 0,
                                   (SNodeListNode*)pInterpPhyNode->pFillValues);
  QUERY_CHECK_NULL(pCtx->pFillColInfo, code, lino, _error, terrno);

  pInfo->pRes = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);
  if (pInterpPhyNode->mergeDataBlock &&
      pTaskInfo->execModel == OPTR_EXEC_MODEL_BATCH) {
    code = createOneDataBlock(pInfo->pRes, false, &pInfo->pMergedRes);
    QUERY_CHECK_CODE(code, lino, _error);
  }
  pCtx->win = pInterpPhyNode->timeRange;
  code = nodesCloneNode(pInterpPhyNode->pTimeRange, &pCtx->pWin);
  QUERY_CHECK_CODE(code, lino, _error);
  pCtx->interval.interval = pInterpPhyNode->interval;
  pCtx->interval.precision = pInterpPhyNode->precision;
  pCtx->interval.timezone = pInterpPhyNode->timezone;
  /*
   * Default to fixed-duration stepping: a 0 unit makes IS_CALENDAR_DAY_DURATION
   * /IS_CALENDAR_TIME_DURATION false, so calendar mode stays off. pInfo is
   * calloc'd so these are already 0, but set them explicitly so correctness does
   * not silently depend on the allocator zeroing the struct.
   */
  pCtx->interval.intervalUnit = 0;
  pCtx->interval.slidingUnit = 0;
  /*
   * Enable calendar/DST-aware EVERY stepping (d/w) only when the session
   * timezone was propagated by the client plan. Plans serialized by pre-upgrade
   * clients carry no timezone (and no precision), so we keep the historical
   * fixed-duration stepping by leaving intervalUnit unset -- this keeps a new
   * server bit-for-bit compatible with old-client plans during a rolling
   * upgrade, and avoids using a defaulted precision in the calendar-day math.
   * EVERY(24h) stays fixed regardless.
   */
  if (pInterpPhyNode->timezone != NULL) {
    pCtx->interval.intervalUnit = pInterpPhyNode->intervalUnit;
    pCtx->interval.slidingUnit = pInterpPhyNode->intervalUnit;
  }
  code = interpGroupStateInit(&pInfo->groupState, pCtx->win.skey);
  QUERY_CHECK_CODE(code, lino, _error);
  if (interpModeUsesPhasedProtocol(pInfo->interpMode)) {
    code = initMainFirstRuntime(pInfo);
    QUERY_CHECK_CODE(code, lino, _error);
  }
  pInfo->pNextGroupRes = NULL;
  pInfo->pRemainRes = NULL;
  pInfo->remainIndex = 0;
  pCtx->surroundingTime = pInterpPhyNode->surroundingTime;
  pCtx->timelineSource = pInterpPhyNode->timelineSource;

  setOperatorInfo(pOperator, "TimeSliceOperator",
                  QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  __optr_fn_t nextFn = interpModeUsesPhasedProtocol(pInfo->interpMode)
                           ? doTimesliceMainFirstNext
                           : doTimesliceNext;
  if (pInfo->pMergedRes != NULL) {
    nextFn = doTimesliceNextMerged;
  }
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, nextFn, NULL,
                                         destroyTimeSliceOperatorInfo,
                                         optrDefaultBufFn, NULL,
                                         optrDefaultGetNextExtFn, NULL);

  code = blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);
  if (pInfo->pMergedRes != NULL) {
    // The merged block accumulates one source block at a time, and
    // colDataMergeCol reallocs to the exact new size, so without a hint every
    // group grows it again. Pre-size it for a typical full round (threshold
    // plus one more source block) to take the fixed-length columns and the
    // var-length offset arrays out of that path. This is only a hint, not a
    // bound: gap and tail interpolation can emit larger blocks, and
    // blockDataMerge still grows the block when it is exceeded. Note the
    // var-length payloads are NOT covered -- colDataReserve is private to
    // tdatablock.c, so those still realloc per group.
    code = blockDataEnsureCapacity(pInfo->pMergedRes,
                                   pOperator->resultInfo.threshold +
                                     pOperator->resultInfo.capacity);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  setOperatorResetStateFn(pOperator, resetTimeSliceOperState);

  code = appendDownstream(pOperator, &downstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (pInfo != NULL) destroyTimeSliceOperatorInfo(pInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

void destroyTimeSliceOperatorInfo(void* param) {
  STimeSliceOperatorInfo* pInfo = (STimeSliceOperatorInfo*)param;

  blockDataDestroy(pInfo->pRes);
  pInfo->pRes = NULL;
  blockDataDestroy(pInfo->pMergedRes);

  destroyMainFirstRuntime(pInfo);
  interpGroupStateDestroy(&pInfo->groupState);
  cleanupExprSupp(&pInfo->scalarSup);
  interpQueryCtxDestroy(&pInfo->queryCtx);
  taosMemoryFreeClear(param);
}

#if defined(BUILD_TEST)
struct SInterpMainFirstOutputTestCtx {
  SOperatorInfo          op;
  STimeSliceOperatorInfo info;
};

void interpTestDestroyMainFirstOutput(
    SInterpMainFirstOutputTestCtx* pCtx) {
  if (pCtx == NULL) {
    return;
  }

  blockDataDestroy(pCtx->info.pRes);
  pCtx->info.pRes = NULL;
  filterFreeInfo(pCtx->op.exprSupp.pFilterInfo);
  pCtx->op.exprSupp.pFilterInfo = NULL;
  destroyMainFirstRuntime(&pCtx->info);
  taosMemoryFree(pCtx);
}

int32_t interpTestCreateMainFirstOutput(
    SSDataBlock* const* ppBlocks, int32_t numBlocks,
    int32_t capacity, int32_t threshold,
    SInterpMainFirstOutputTestCtx** ppCtx) {
  if (ppBlocks == NULL || numBlocks <= 0 || ppBlocks[0] == NULL ||
      ppCtx == NULL ||
      capacity <= 0 || threshold < 0 || threshold >= capacity) {
    return TSDB_CODE_INVALID_PARA;
  }
  *ppCtx = NULL;

  SInterpMainFirstOutputTestCtx* pCtx =
      taosMemoryCalloc(1, sizeof(*pCtx));
  if (pCtx == NULL) {
    return terrno;
  }

  int32_t code = createOneDataBlock(ppBlocks[0], false,
                                    &pCtx->info.pRes);
  if (code == TSDB_CODE_SUCCESS) {
    code = blockDataEnsureCapacity(pCtx->info.pRes, capacity);
  }
  if (code == TSDB_CODE_SUCCESS) {
    code = initMainFirstRuntime(&pCtx->info);
  }
  if (code != TSDB_CODE_SUCCESS) {
    interpTestDestroyMainFirstOutput(pCtx);
    return code;
  }

  for (int32_t i = 0; i < numBlocks; ++i) {
    SSDataBlock* pBlock = ppBlocks[i];
    if (pBlock == NULL || pBlock->info.rows <= 0) {
      interpTestDestroyMainFirstOutput(pCtx);
      return TSDB_CODE_INVALID_PARA;
    }

    uint64_t groupId = pBlock->info.id.groupId;
    SMainFirstGroupState* pGroup =
        getMainFirstGroup(&pCtx->info, groupId);
    if (pGroup == NULL) {
      pGroup = taosMemoryCalloc(1, sizeof(*pGroup));
      if (pGroup == NULL) {
        code = terrno;
        interpTestDestroyMainFirstOutput(pCtx);
        return code;
      }
      pGroup->interp.groupId = groupId;
      pGroup->blockId = pBlock->info.id;
      pGroup->scanFlag = pBlock->info.scanFlag;
      pGroup->firstMainRowSet = true;
      code = taosHashPut(pCtx->info.pGroupStates, &groupId,
                         sizeof(groupId), &pGroup,
                         sizeof(pGroup));
      if (code != TSDB_CODE_SUCCESS) {
        mainFirstGroupStateDestroy(pGroup);
        interpTestDestroyMainFirstOutput(pCtx);
        return code;
      }
      if (taosArrayPush(pCtx->info.pCompletedGroups,
                        &groupId) == NULL) {
        code = terrno;
        interpTestDestroyMainFirstOutput(pCtx);
        return code;
      }
    }

    code = interpResultStoreAppend(pCtx->info.pResultStore, groupId,
                                   pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      interpTestDestroyMainFirstOutput(pCtx);
      return code;
    }
    pGroup->hasStoredResults = true;
  }

  pCtx->op.info = &pCtx->info;
  pCtx->op.resultInfo.capacity = capacity;
  pCtx->op.resultInfo.threshold = threshold;
  *ppCtx = pCtx;
  return TSDB_CODE_SUCCESS;
}

int32_t interpTestNextMainFirstOutput(
    SInterpMainFirstOutputTestCtx* pCtx, SSDataBlock** ppBlock) {
  if (pCtx == NULL || ppBlock == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  blockDataCleanup(pCtx->info.pRes);
  bool produced = false;
  int32_t code = produceMainFirstGroupOutput(&pCtx->op, &produced);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  *ppBlock = produced ? pCtx->info.pRes : NULL;
  return TSDB_CODE_SUCCESS;
}

int32_t interpTestSetMainFirstOutputFilterFalse(
    SInterpMainFirstOutputTestCtx* pCtx) {
  if (pCtx == NULL || pCtx->op.exprSupp.pFilterInfo != NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SValueNode* pFalse = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_VALUE,
                               (SNode**)&pFalse);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pFalse->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pFalse->node.resType.bytes = sizeof(bool);
  bool value = false;
  code = nodesSetValueNodeValue(pFalse, &value);
  if (code == TSDB_CODE_SUCCESS) {
    pFalse->translate = true;
    code = filterInitFromNode(
        (SNode*)pFalse, &pCtx->op.exprSupp.pFilterInfo, 0, NULL);
  }
  nodesDestroyNode((SNode*)pFalse);
  return code;
}

void interpTestResetMainFirstOutput(
    SInterpMainFirstOutputTestCtx* pCtx) {
  if (pCtx == NULL) {
    return;
  }
  blockDataCleanup(pCtx->info.pRes);
  resetMainFirstRuntime(&pCtx->info);
}

int64_t interpTestMainFirstOutputStoreBytes(
    const SInterpMainFirstOutputTestCtx* pCtx) {
  return pCtx == NULL
             ? 0
             : interpResultStoreBytesUsed(pCtx->info.pResultStore);
}
#endif

STrueForInfo* getTrueForInfo(struct SOperatorInfo* pOperator) {
  if (pOperator == NULL) {
    return NULL;
  }

  switch (pOperator->operatorType) {
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE:
      return &((SStateWindowOperatorInfo*)pOperator->info)->trueForInfo;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_EVENT:
      return &((SEventWindowOperatorInfo*)pOperator->info)->trueForInfo;
    default:
      return NULL;
  }
}
