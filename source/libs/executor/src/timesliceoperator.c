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
#include "executorimpl.h"
#include "filter.h"
#include "function.h"
#include "functionMgt.h"
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tfill.h"
#include "ttime.h"

typedef struct STimeSliceOperatorInfo {
  SSDataBlock*         pRes;
  STimeWindow          win;
  SInterval            interval;
  int64_t              current;
  SArray*              pPrevRow;     // SArray<SGroupValue>
  SArray*              pNextRow;     // SArray<SGroupValue>
  SArray*              pLinearInfo;  // SArray<SFillLinearInfo>
  bool                 isPrevRowSet;
  bool                 isNextRowSet;
  int32_t              fillType;      // fill type
  SColumn              tsCol;         // primary timestamp column
  SExprSupp            scalarSup;     // scalar calculation
  struct SFillColInfo* pFillColInfo;  // fill column info
} STimeSliceOperatorInfo;

static void destroyTimeSliceOperatorInfo(void* param);

static void doKeepPrevRows(STimeSliceOperatorInfo* pSliceInfo, const SSDataBlock* pBlock, int32_t rowIndex) {
  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);

    SGroupKeys* pkey = taosArrayGet(pSliceInfo->pPrevRow, i);
    if (!colDataIsNull_s(pColInfoData, rowIndex)) {
      pkey->isNull = false;
      char* val = colDataGetData(pColInfoData, rowIndex);
      if (IS_VAR_DATA_TYPE(pkey->type)) {
        memcpy(pkey->pData, val, varDataLen(val));
      } else {
        memcpy(pkey->pData, val, pkey->bytes);
      }
    } else {
      pkey->isNull = true;
    }
  }

  pSliceInfo->isPrevRowSet = true;
}

static void doKeepNextRows(STimeSliceOperatorInfo* pSliceInfo, const SSDataBlock* pBlock, int32_t rowIndex) {
  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);

    SGroupKeys* pkey = taosArrayGet(pSliceInfo->pNextRow, i);
    if (!colDataIsNull_s(pColInfoData, rowIndex)) {
      pkey->isNull = false;
      char* val = colDataGetData(pColInfoData, rowIndex);
      if (!IS_VAR_DATA_TYPE(pkey->type)) {
        memcpy(pkey->pData, val, pkey->bytes);
      } else {
        memcpy(pkey->pData, val, varDataLen(val));
      }
    } else {
      pkey->isNull = true;
    }
  }

  pSliceInfo->isNextRowSet = true;
}

static void doKeepLinearInfo(STimeSliceOperatorInfo* pSliceInfo, const SSDataBlock* pBlock, int32_t rowIndex) {
  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
    SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pSliceInfo->tsCol.slotId);
    SFillLinearInfo* pLinearInfo = taosArrayGet(pSliceInfo->pLinearInfo, i);


    if (!IS_MATHABLE_TYPE(pColInfoData->info.type)) {
      continue;
    }

    // null value is represented by using key = INT64_MIN for now.
    // TODO: optimize to ignore null values for linear interpolation.
    if (!pLinearInfo->isStartSet) {
      if (!colDataIsNull_s(pColInfoData, rowIndex)) {

        pLinearInfo->start.key = *(int64_t*)colDataGetData(pTsCol, rowIndex);
        char* p = colDataGetData(pColInfoData, rowIndex);
        if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
          ASSERT(varDataTLen(p) <= pColInfoData->info.bytes);
          memcpy(pLinearInfo->start.val, p, varDataTLen(p));
        } else {
          memcpy(pLinearInfo->start.val, p, pLinearInfo->bytes);
        }
      }
      pLinearInfo->isStartSet = true;
    } else if (!pLinearInfo->isEndSet) {
      if (!colDataIsNull_s(pColInfoData, rowIndex)) {
        pLinearInfo->end.key = *(int64_t*)colDataGetData(pTsCol, rowIndex);

        char* p = colDataGetData(pColInfoData, rowIndex);
        if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
          ASSERT(varDataTLen(p) <= pColInfoData->info.bytes);
          memcpy(pLinearInfo->end.val, p, varDataTLen(p));
        } else {
          memcpy(pLinearInfo->end.val, p, pLinearInfo->bytes);
        }
      }
      pLinearInfo->isEndSet = true;
    } else {
      pLinearInfo->start.key = pLinearInfo->end.key;
      memcpy(pLinearInfo->start.val, pLinearInfo->end.val, pLinearInfo->bytes);

      if (!colDataIsNull_s(pColInfoData, rowIndex)) {
        pLinearInfo->end.key = *(int64_t*)colDataGetData(pTsCol, rowIndex);

        char* p = colDataGetData(pColInfoData, rowIndex);
        if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
          ASSERT(varDataTLen(p) <= pColInfoData->info.bytes);
          memcpy(pLinearInfo->end.val, p, varDataTLen(p));
        } else {
          memcpy(pLinearInfo->end.val, p, pLinearInfo->bytes);
        }

      } else {
        pLinearInfo->end.key = INT64_MIN;
      }
    }
  }

}


static FORCE_INLINE int32_t timeSliceEnsureBlockCapacity(STimeSliceOperatorInfo* pSliceInfo, SSDataBlock* pBlock) {
  if (pBlock->info.rows < pBlock->info.capacity) {
    return TSDB_CODE_SUCCESS;
  }

  uint32_t winNum = (pSliceInfo->win.ekey - pSliceInfo->win.skey) / pSliceInfo->interval.interval;
  uint32_t newRowsNum = pBlock->info.rows + TMIN(winNum / 4 + 1, 1048576);
  blockDataEnsureCapacity(pBlock, newRowsNum);

  return TSDB_CODE_SUCCESS;
}


static bool genInterpolationResult(STimeSliceOperatorInfo* pSliceInfo, SExprSupp* pExprSup, SSDataBlock* pResBlock, bool beforeTs) {
  int32_t rows = pResBlock->info.rows;
  timeSliceEnsureBlockCapacity(pSliceInfo, pResBlock);
  // todo set the correct primary timestamp column

  // output the result
  bool hasInterp = true;
  for (int32_t j = 0; j < pExprSup->numOfExprs; ++j) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[j];

    int32_t          dstSlot = pExprInfo->base.resSchema.slotId;
    SColumnInfoData* pDst = taosArrayGet(pResBlock->pDataBlock, dstSlot);

    if (IS_TIMESTAMP_TYPE(pExprInfo->base.resSchema.type)) {
      colDataAppend(pDst, rows, (char*)&pSliceInfo->current, false);
      continue;
    }

    int32_t srcSlot = pExprInfo->base.pParam[0].pCol->slotId;
    switch (pSliceInfo->fillType) {
      case TSDB_FILL_NULL:
      case TSDB_FILL_NULL_F: {
        colDataAppendNULL(pDst, rows);
        break;
      }

      case TSDB_FILL_SET_VALUE:
      case TSDB_FILL_SET_VALUE_F: {
        SVariant* pVar = &pSliceInfo->pFillColInfo[j].fillVal;

        if (pDst->info.type == TSDB_DATA_TYPE_FLOAT) {
          float v = 0;
          GET_TYPED_DATA(v, float, pVar->nType, &pVar->i);
          colDataAppend(pDst, rows, (char*)&v, false);
        } else if (pDst->info.type == TSDB_DATA_TYPE_DOUBLE) {
          double v = 0;
          GET_TYPED_DATA(v, double, pVar->nType, &pVar->i);
          colDataAppend(pDst, rows, (char*)&v, false);
        } else if (IS_SIGNED_NUMERIC_TYPE(pDst->info.type)) {
          int64_t v = 0;
          GET_TYPED_DATA(v, int64_t, pVar->nType, &pVar->i);
          colDataAppend(pDst, rows, (char*)&v, false);
        }
        break;
      }

      case TSDB_FILL_LINEAR: {
        SFillLinearInfo* pLinearInfo = taosArrayGet(pSliceInfo->pLinearInfo, srcSlot);

        SPoint start = pLinearInfo->start;
        SPoint end = pLinearInfo->end;
        SPoint current = {.key = pSliceInfo->current};

        // do not interpolate before ts range, only increate pSliceInfo->current
        if (beforeTs && !pLinearInfo->isEndSet) {
          return true;
        }

        if (!pLinearInfo->isStartSet || !pLinearInfo->isEndSet) {
          hasInterp = false;
          break;
        }

        if (start.key == INT64_MIN || end.key == INT64_MIN) {
          colDataAppendNULL(pDst, rows);
          break;
        }

        current.val = taosMemoryCalloc(pLinearInfo->bytes, 1);
        taosGetLinearInterpolationVal(&current, pLinearInfo->type, &start, &end, pLinearInfo->type);
        colDataAppend(pDst, rows, (char*)current.val, false);

        taosMemoryFree(current.val);
        break;
      }
      case TSDB_FILL_PREV: {
        if (!pSliceInfo->isPrevRowSet) {
          hasInterp = false;
          break;
        }

        SGroupKeys* pkey = taosArrayGet(pSliceInfo->pPrevRow, srcSlot);
        if (pkey->isNull == false) {
          colDataAppend(pDst, rows, pkey->pData, false);
        } else {
          colDataAppendNULL(pDst, rows);
        }
        break;
      }

      case TSDB_FILL_NEXT: {
        if (!pSliceInfo->isNextRowSet) {
          hasInterp = false;
          break;
        }

        SGroupKeys* pkey = taosArrayGet(pSliceInfo->pNextRow, srcSlot);
        if (pkey->isNull == false) {
          colDataAppend(pDst, rows, pkey->pData, false);
        } else {
          colDataAppendNULL(pDst, rows);
        }
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

  return hasInterp;
}

static void addCurrentRowToResult(STimeSliceOperatorInfo* pSliceInfo, SExprSupp* pExprSup, SSDataBlock* pResBlock,
                                  SSDataBlock* pSrcBlock, int32_t index) {
  timeSliceEnsureBlockCapacity(pSliceInfo, pResBlock);
  for (int32_t j = 0; j < pExprSup->numOfExprs; ++j) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[j];

    int32_t          dstSlot = pExprInfo->base.resSchema.slotId;
    SColumnInfoData* pDst = taosArrayGet(pResBlock->pDataBlock, dstSlot);

    if (IS_TIMESTAMP_TYPE(pExprInfo->base.resSchema.type)) {
      colDataAppend(pDst, pResBlock->info.rows, (char*)&pSliceInfo->current, false);
    } else {
      int32_t          srcSlot = pExprInfo->base.pParam[0].pCol->slotId;
      SColumnInfoData* pSrc = taosArrayGet(pSrcBlock->pDataBlock, srcSlot);

      if (colDataIsNull_s(pSrc, index)) {
        colDataAppendNULL(pDst, pResBlock->info.rows);
        continue;
      }

      char* v = colDataGetData(pSrc, index);
      colDataAppend(pDst, pResBlock->info.rows, v, false);
    }
  }

  pResBlock->info.rows += 1;
  return;
}


static int32_t initPrevRowsKeeper(STimeSliceOperatorInfo* pInfo, SSDataBlock* pBlock) {
  if (pInfo->pPrevRow != NULL) {
    return TSDB_CODE_SUCCESS;
  }

  pInfo->pPrevRow = taosArrayInit(4, sizeof(SGroupKeys));
  if (pInfo->pPrevRow == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);

    SGroupKeys key = {0};
    key.bytes = pColInfo->info.bytes;
    key.type = pColInfo->info.type;
    key.isNull = false;
    key.pData = taosMemoryCalloc(1, pColInfo->info.bytes);
    taosArrayPush(pInfo->pPrevRow, &key);
  }

  pInfo->isPrevRowSet = false;

  return TSDB_CODE_SUCCESS;
}

static int32_t initNextRowsKeeper(STimeSliceOperatorInfo* pInfo, SSDataBlock* pBlock) {
  if (pInfo->pNextRow != NULL) {
    return TSDB_CODE_SUCCESS;
  }

  pInfo->pNextRow = taosArrayInit(4, sizeof(SGroupKeys));
  if (pInfo->pNextRow == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);

    SGroupKeys key = {0};
    key.bytes = pColInfo->info.bytes;
    key.type = pColInfo->info.type;
    key.isNull = false;
    key.pData = taosMemoryCalloc(1, pColInfo->info.bytes);
    taosArrayPush(pInfo->pNextRow, &key);
  }

  pInfo->isNextRowSet = false;

  return TSDB_CODE_SUCCESS;
}

static int32_t initFillLinearInfo(STimeSliceOperatorInfo* pInfo, SSDataBlock* pBlock) {
  if (pInfo->pLinearInfo != NULL) {
    return TSDB_CODE_SUCCESS;
  }

  pInfo->pLinearInfo = taosArrayInit(4, sizeof(SFillLinearInfo));
  if (pInfo->pLinearInfo == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);

    SFillLinearInfo linearInfo = {0};
    linearInfo.start.key = INT64_MIN;
    linearInfo.end.key = INT64_MIN;
    linearInfo.start.val = taosMemoryCalloc(1, pColInfo->info.bytes);
    linearInfo.end.val = taosMemoryCalloc(1, pColInfo->info.bytes);
    linearInfo.isStartSet = false;
    linearInfo.isEndSet = false;
    linearInfo.type = pColInfo->info.type;
    linearInfo.bytes = pColInfo->info.bytes;
    taosArrayPush(pInfo->pLinearInfo, &linearInfo);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t initKeeperInfo(STimeSliceOperatorInfo* pInfo, SSDataBlock* pBlock) {
  int32_t code;
  code = initPrevRowsKeeper(pInfo, pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  }

  code = initNextRowsKeeper(pInfo, pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  }

  code = initFillLinearInfo(pInfo, pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  }

  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* doTimeslice(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  STimeSliceOperatorInfo* pSliceInfo = pOperator->info;
  SSDataBlock*            pResBlock = pSliceInfo->pRes;
  SExprSupp*              pSup = &pOperator->exprSupp;

  int32_t        order = TSDB_ORDER_ASC;
  SInterval*     pInterval = &pSliceInfo->interval;
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  blockDataCleanup(pResBlock);

  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }

    if (pSliceInfo->scalarSup.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pSliceInfo->scalarSup;
      projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    }

    int32_t code = initKeeperInfo(pSliceInfo, pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pSup, pBlock, order, MAIN_SCAN, true);

    SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pSliceInfo->tsCol.slotId);
    for (int32_t i = 0; i < pBlock->info.rows; ++i) {
      int64_t ts = *(int64_t*)colDataGetData(pTsCol, i);

      if (pSliceInfo->current > pSliceInfo->win.ekey) {
        setOperatorCompleted(pOperator);
        break;
      }

      if (ts == pSliceInfo->current) {
        addCurrentRowToResult(pSliceInfo, &pOperator->exprSupp, pResBlock, pBlock, i);

        doKeepPrevRows(pSliceInfo, pBlock, i);
        doKeepLinearInfo(pSliceInfo, pBlock, i);

        pSliceInfo->current =
            taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
        if (pSliceInfo->current > pSliceInfo->win.ekey) {
          setOperatorCompleted(pOperator);
          break;
        }
      } else if (ts < pSliceInfo->current) {
        // in case of interpolation window starts and ends between two datapoints, fill(prev) need to interpolate
        doKeepPrevRows(pSliceInfo, pBlock, i);
        doKeepLinearInfo(pSliceInfo, pBlock, i);

        if (i < pBlock->info.rows - 1) {
          // in case of interpolation window starts and ends between two datapoints, fill(next) need to interpolate
          doKeepNextRows(pSliceInfo, pBlock, i + 1);
          int64_t nextTs = *(int64_t*)colDataGetData(pTsCol, i + 1);
          if (nextTs > pSliceInfo->current) {
            while (pSliceInfo->current < nextTs && pSliceInfo->current <= pSliceInfo->win.ekey) {
              if (!genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pResBlock, false) && pSliceInfo->fillType == TSDB_FILL_LINEAR) {
                break;
              } else {
                pSliceInfo->current =
                    taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
              }
            }

            if (pSliceInfo->current > pSliceInfo->win.ekey) {
              setOperatorCompleted(pOperator);
              break;
            }
          } else {
            // ignore current row, and do nothing
          }
        } else {  // it is the last row of current block
          doKeepPrevRows(pSliceInfo, pBlock, i);
        }
      } else {  // ts > pSliceInfo->current
        // in case of interpolation window starts and ends between two datapoints, fill(next) need to interpolate
        doKeepNextRows(pSliceInfo, pBlock, i);
        doKeepLinearInfo(pSliceInfo, pBlock, i);

        while (pSliceInfo->current < ts && pSliceInfo->current <= pSliceInfo->win.ekey) {
          if (!genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pResBlock, true) && pSliceInfo->fillType == TSDB_FILL_LINEAR) {
            break;
          } else {
            pSliceInfo->current =
                taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
          }
        }

        // add current row if timestamp match
        if (ts == pSliceInfo->current && pSliceInfo->current <= pSliceInfo->win.ekey) {
          addCurrentRowToResult(pSliceInfo, &pOperator->exprSupp, pResBlock, pBlock, i);

          pSliceInfo->current =
              taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
        }
        doKeepPrevRows(pSliceInfo, pBlock, i);

        if (pSliceInfo->current > pSliceInfo->win.ekey) {
          setOperatorCompleted(pOperator);
          break;
        }
      }
    }
  }

  // check if need to interpolate after last datablock
  // except for fill(next), fill(linear)
  while (pSliceInfo->current <= pSliceInfo->win.ekey && pSliceInfo->fillType != TSDB_FILL_NEXT &&
         pSliceInfo->fillType != TSDB_FILL_LINEAR) {
    genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pResBlock, false);
    pSliceInfo->current =
        taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
  }

  doFilter(pResBlock, pOperator->exprSupp.pFilterInfo, NULL);

  // restore the value
  setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
  if (pResBlock->info.rows == 0) {
    pOperator->status = OP_EXEC_DONE;
  }

  return pResBlock->info.rows == 0 ? NULL : pResBlock;
}

SOperatorInfo* createTimeSliceOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo) {
  STimeSliceOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(STimeSliceOperatorInfo));
  SOperatorInfo*          pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pOperator == NULL || pInfo == NULL) {
    goto _error;
  }

  SInterpFuncPhysiNode* pInterpPhyNode = (SInterpFuncPhysiNode*)pPhyNode;
  SExprSupp*            pSup = &pOperator->exprSupp;

  int32_t    numOfExprs = 0;
  SExprInfo* pExprInfo = createExprInfo(pInterpPhyNode->pFuncs, NULL, &numOfExprs);
  int32_t    code = initExprSupp(pSup, pExprInfo, numOfExprs);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (pInterpPhyNode->pExprs != NULL) {
    int32_t    num = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pInterpPhyNode->pExprs, NULL, &num);
    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, num);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  code = filterInitFromNode((SNode*)pInterpPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->tsCol = extractColumnFromColumnNode((SColumnNode*)pInterpPhyNode->pTimeSeries);
  pInfo->fillType = convertFillType(pInterpPhyNode->fillMode);
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  pInfo->pFillColInfo = createFillColInfo(pExprInfo, numOfExprs, NULL, 0, (SNodeListNode*)pInterpPhyNode->pFillValues);
  pInfo->pLinearInfo = NULL;
  pInfo->pRes = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  pInfo->win = pInterpPhyNode->timeRange;
  pInfo->interval.interval = pInterpPhyNode->interval;
  pInfo->current = pInfo->win.skey;

  if (downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    STableScanInfo* pScanInfo = (STableScanInfo*)downstream->info;
    pScanInfo->base.cond.twindows = pInfo->win;
    pScanInfo->base.cond.type = TIMEWINDOW_RANGE_EXTERNAL;
  }

  setOperatorInfo(pOperator, "TimeSliceOperator", QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doTimeslice, NULL, destroyTimeSliceOperatorInfo, optrDefaultBufFn, NULL);

  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);

  code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

  _error:
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

void destroyTimeSliceOperatorInfo(void* param) {
  STimeSliceOperatorInfo* pInfo = (STimeSliceOperatorInfo*)param;

  pInfo->pRes = blockDataDestroy(pInfo->pRes);

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pPrevRow); ++i) {
    SGroupKeys* pKey = taosArrayGet(pInfo->pPrevRow, i);
    taosMemoryFree(pKey->pData);
  }
  taosArrayDestroy(pInfo->pPrevRow);

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pNextRow); ++i) {
    SGroupKeys* pKey = taosArrayGet(pInfo->pNextRow, i);
    taosMemoryFree(pKey->pData);
  }
  taosArrayDestroy(pInfo->pNextRow);

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pLinearInfo); ++i) {
    SFillLinearInfo* pKey = taosArrayGet(pInfo->pLinearInfo, i);
    taosMemoryFree(pKey->start.val);
    taosMemoryFree(pKey->end.val);
  }
  taosArrayDestroy(pInfo->pLinearInfo);
  cleanupExprSupp(&pInfo->scalarSup);

  taosMemoryFree(pInfo->pFillColInfo);
  taosMemoryFreeClear(param);
}
