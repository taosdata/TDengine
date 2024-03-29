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
#include "operator.h"
#include "querytask.h"
#include "storageapi.h"
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
  int64_t              prevTs;
  bool                 prevTsSet;
  uint64_t             groupId;
  SGroupKeys*          pPrevGroupKey;
  SSDataBlock*         pNextGroupRes;
  SSDataBlock*         pRemainRes;    // save block unfinished processing
  int32_t              remainIndex;     // the remaining index in the block to be processed
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

static bool isIrowtsPseudoColumn(SExprInfo* pExprInfo) {
  char *name = pExprInfo->pExpr->_function.functionName;
  return (IS_TIMESTAMP_TYPE(pExprInfo->base.resSchema.type) && strcasecmp(name, "_irowts") == 0);
}

static bool isIsfilledPseudoColumn(SExprInfo* pExprInfo) {
  char *name = pExprInfo->pExpr->_function.functionName;
  return (IS_BOOLEAN_TYPE(pExprInfo->base.resSchema.type) && strcasecmp(name, "_isfilled") == 0);
}

static bool checkDuplicateTimestamps(STimeSliceOperatorInfo* pSliceInfo, SColumnInfoData* pTsCol,
                                     int32_t curIndex, int32_t rows) {


  int64_t currentTs = *(int64_t*)colDataGetData(pTsCol, curIndex);
  if (currentTs > pSliceInfo->win.ekey) {
    return false;
  }

  if ((pSliceInfo->prevTsSet == true) && (currentTs == pSliceInfo->prevTs)) {
    return true;
  }

  pSliceInfo->prevTsSet = true;
  pSliceInfo->prevTs = currentTs;

  if (currentTs == pSliceInfo->win.ekey && curIndex < rows - 1) {
    int64_t nextTs = *(int64_t*)colDataGetData(pTsCol, curIndex + 1);
    if (currentTs == nextTs) {
      return true;
    }
  }

  return false;
}

static bool isInterpFunc(SExprInfo* pExprInfo) {
  int32_t functionType = pExprInfo->pExpr->_function.functionType;
  return (functionType == FUNCTION_TYPE_INTERP);
}

static bool isGroupKeyFunc(SExprInfo* pExprInfo) {
  int32_t functionType = pExprInfo->pExpr->_function.functionType;
  return (functionType == FUNCTION_TYPE_GROUP_KEY);
}

static bool getIgoreNullRes(SExprSupp* pExprSup) {
  for (int32_t i = 0; i < pExprSup->numOfExprs; ++i) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[i];

    if (isInterpFunc(pExprInfo)) {
      for (int32_t j = 0; j < pExprInfo->base.numOfParams; ++j) {
        SFunctParam *pFuncParam = &pExprInfo->base.pParam[j];
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
      int32_t       srcSlot = pExprInfo->base.pParam[0].pCol->slotId;
      SColumnInfoData* pSrc = taosArrayGet(pSrcBlock->pDataBlock, srcSlot);

      if (colDataIsNull_s(pSrc, index)) {
        return true;
      }
    }
  }

  return false;
}


static bool genInterpolationResult(STimeSliceOperatorInfo* pSliceInfo, SExprSupp* pExprSup, SSDataBlock* pResBlock,
                                   SSDataBlock* pSrcBlock, int32_t index, bool beforeTs) {
  int32_t rows = pResBlock->info.rows;
  timeSliceEnsureBlockCapacity(pSliceInfo, pResBlock);
  // todo set the correct primary timestamp column


  // output the result
  int32_t fillColIndex = 0;
  bool       hasInterp = true;
  for (int32_t j = 0; j < pExprSup->numOfExprs; ++j) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[j];

    int32_t       dstSlot = pExprInfo->base.resSchema.slotId;
    SColumnInfoData* pDst = taosArrayGet(pResBlock->pDataBlock, dstSlot);

    if (isIrowtsPseudoColumn(pExprInfo)) {
      colDataSetVal(pDst, rows, (char*)&pSliceInfo->current, false);
      continue;
    } else if (isIsfilledPseudoColumn(pExprInfo)) {
      bool isFilled = true;
      colDataSetVal(pDst, pResBlock->info.rows, (char*)&isFilled, false);
      continue;
    } else if (!isInterpFunc(pExprInfo)) {
      if (isGroupKeyFunc(pExprInfo)) {
        if (pSrcBlock != NULL) {
          int32_t       srcSlot = pExprInfo->base.pParam[0].pCol->slotId;
          SColumnInfoData* pSrc = taosArrayGet(pSrcBlock->pDataBlock, srcSlot);

          if (colDataIsNull_s(pSrc, index)) {
            colDataSetNULL(pDst, pResBlock->info.rows);
            continue;
          }

          char* v = colDataGetData(pSrc, index);
          colDataSetVal(pDst, pResBlock->info.rows, v, false);
        } else {
          // use stored group key
          SGroupKeys* pkey = pSliceInfo->pPrevGroupKey;
          if (pkey->isNull == false) {
            colDataSetVal(pDst, rows, pkey->pData, false);
          } else {
            colDataSetNULL(pDst, rows);
          }
        }
      }
      continue;
    }

    int32_t srcSlot = pExprInfo->base.pParam[0].pCol->slotId;
    switch (pSliceInfo->fillType) {
      case TSDB_FILL_NULL:
      case TSDB_FILL_NULL_F: {
        colDataSetNULL(pDst, rows);
        break;
      }

      case TSDB_FILL_SET_VALUE:
      case TSDB_FILL_SET_VALUE_F: {
        SVariant* pVar = &pSliceInfo->pFillColInfo[fillColIndex].fillVal;

        bool isNull = (TSDB_DATA_TYPE_NULL == pVar->nType) ? true : false;
        if (pDst->info.type == TSDB_DATA_TYPE_FLOAT) {
          float v = 0;
          if (!IS_VAR_DATA_TYPE(pVar->nType)) {
            GET_TYPED_DATA(v, float, pVar->nType, &pVar->f);
          } else {
            v = taosStr2Float(varDataVal(pVar->pz), NULL);
          }
          colDataSetVal(pDst, rows, (char*)&v, isNull);
        } else if (pDst->info.type == TSDB_DATA_TYPE_DOUBLE) {
          double v = 0;
          if (!IS_VAR_DATA_TYPE(pVar->nType)) {
            GET_TYPED_DATA(v, double, pVar->nType, &pVar->d);
          } else {
            v = taosStr2Double(varDataVal(pVar->pz), NULL);
          }
          colDataSetVal(pDst, rows, (char*)&v, isNull);
        } else if (IS_SIGNED_NUMERIC_TYPE(pDst->info.type)) {
          int64_t v = 0;
          if (!IS_VAR_DATA_TYPE(pVar->nType)) {
            GET_TYPED_DATA(v, int64_t, pVar->nType, &pVar->i);
          } else {
            v = taosStr2Int64(varDataVal(pVar->pz), NULL, 10);
          }
          colDataSetVal(pDst, rows, (char*)&v, isNull);
        } else if (IS_UNSIGNED_NUMERIC_TYPE(pDst->info.type)) {
          uint64_t v = 0;
          if (!IS_VAR_DATA_TYPE(pVar->nType)) {
            GET_TYPED_DATA(v, uint64_t, pVar->nType, &pVar->u);
          } else {
            v = taosStr2UInt64(varDataVal(pVar->pz), NULL, 10);
          }
          colDataSetVal(pDst, rows, (char*)&v, isNull);
        } else if (IS_BOOLEAN_TYPE(pDst->info.type)) {
          bool v = false;
          if (!IS_VAR_DATA_TYPE(pVar->nType)) {
            GET_TYPED_DATA(v, bool, pVar->nType, &pVar->i);
          } else {
            v = taosStr2Int8(varDataVal(pVar->pz), NULL, 10);
          }
          colDataSetVal(pDst, rows, (char*)&v, isNull);
        }

        ++fillColIndex;
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

        if (end.key != INT64_MIN && end.key < pSliceInfo->current) {
          hasInterp = false;
          break;
        }

        if (start.key == INT64_MIN || end.key == INT64_MIN) {
          colDataSetNULL(pDst, rows);
          break;
        }

        current.val = taosMemoryCalloc(pLinearInfo->bytes, 1);
        taosGetLinearInterpolationVal(&current, pLinearInfo->type, &start, &end, pLinearInfo->type);
        colDataSetVal(pDst, rows, (char*)current.val, false);

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
          colDataSetVal(pDst, rows, pkey->pData, false);
        } else {
          colDataSetNULL(pDst, rows);
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
          colDataSetVal(pDst, rows, pkey->pData, false);
        } else {
          colDataSetNULL(pDst, rows);
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

    if (isIrowtsPseudoColumn(pExprInfo)) {
      colDataSetVal(pDst, pResBlock->info.rows, (char*)&pSliceInfo->current, false);
    } else if (isIsfilledPseudoColumn(pExprInfo)) {
      bool isFilled = false;
      colDataSetVal(pDst, pResBlock->info.rows, (char*)&isFilled, false);
    } else {
      int32_t       srcSlot = pExprInfo->base.pParam[0].pCol->slotId;
      SColumnInfoData* pSrc = taosArrayGet(pSrcBlock->pDataBlock, srcSlot);

      if (colDataIsNull_s(pSrc, index)) {
        colDataSetNULL(pDst, pResBlock->info.rows);
        continue;
      }

      char* v = colDataGetData(pSrc, index);
      colDataSetVal(pDst, pResBlock->info.rows, v, false);
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

static int32_t initGroupKeyKeeper(STimeSliceOperatorInfo* pInfo, SExprSupp* pExprSup) {
  if (pInfo->pPrevGroupKey != NULL) {
    return TSDB_CODE_SUCCESS;
  }

  pInfo->pPrevGroupKey = taosMemoryCalloc(1, sizeof(SGroupKeys));
  if (pInfo->pPrevGroupKey == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < pExprSup->numOfExprs; ++i) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[i];

    if (isGroupKeyFunc(pExprInfo)) {
      pInfo->pPrevGroupKey->bytes = pExprInfo->base.resSchema.bytes;
      pInfo->pPrevGroupKey->type = pExprInfo->base.resSchema.type;
      pInfo->pPrevGroupKey->isNull = false;
      pInfo->pPrevGroupKey->pData = taosMemoryCalloc(1, pInfo->pPrevGroupKey->bytes);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t initKeeperInfo(STimeSliceOperatorInfo* pInfo, SSDataBlock* pBlock, SExprSupp* pExprSup) {
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

  code = initGroupKeyKeeper(pInfo, pExprSup);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  }


  return TSDB_CODE_SUCCESS;
}

static int32_t resetPrevRowsKeeper(STimeSliceOperatorInfo* pInfo) {
  if (pInfo->pPrevRow == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pLinearInfo); ++i) {
    SGroupKeys *pKey = taosArrayGet(pInfo->pPrevRow, i);
    pKey->isNull = false;
  }

  pInfo->isPrevRowSet = false;

  return TSDB_CODE_SUCCESS;
}

static int32_t resetNextRowsKeeper(STimeSliceOperatorInfo* pInfo) {
  if (pInfo->pNextRow == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pLinearInfo); ++i) {
    SGroupKeys *pKey = taosArrayGet(pInfo->pPrevRow, i);
    pKey->isNull = false;
  }

  pInfo->isNextRowSet = false;

  return TSDB_CODE_SUCCESS;
}

static int32_t resetFillLinearInfo(STimeSliceOperatorInfo* pInfo) {
  if (pInfo->pLinearInfo == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pLinearInfo); ++i) {
    SFillLinearInfo *pLinearInfo = taosArrayGet(pInfo->pLinearInfo, i);
    pLinearInfo->start.key = INT64_MIN;
    pLinearInfo->end.key = INT64_MIN;
    pLinearInfo->isStartSet = false;
    pLinearInfo->isEndSet = false;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t resetKeeperInfo(STimeSliceOperatorInfo* pInfo) {
  resetPrevRowsKeeper(pInfo);
  resetNextRowsKeeper(pInfo);
  resetFillLinearInfo(pInfo);

  return TSDB_CODE_SUCCESS;
}

static bool checkThresholdReached(STimeSliceOperatorInfo* pSliceInfo, int32_t threshold) {
  SSDataBlock* pResBlock = pSliceInfo->pRes;
  if (pResBlock->info.rows > threshold) {
    return true;
  }

  return false;
}

static bool checkWindowBoundReached(STimeSliceOperatorInfo* pSliceInfo) {
  if (pSliceInfo->current > pSliceInfo->win.ekey) {
    return true;
  }

  return false;
}

static void saveBlockStatus(STimeSliceOperatorInfo* pSliceInfo, SSDataBlock* pBlock, int32_t curIndex) {
  SSDataBlock* pResBlock = pSliceInfo->pRes;

  SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pSliceInfo->tsCol.slotId);
  if (curIndex < pBlock->info.rows - 1) {
    pSliceInfo->pRemainRes = pBlock;
    pSliceInfo->remainIndex = curIndex + 1;
    return;
  }

  // all data in remaining block processed
  pSliceInfo->pRemainRes = NULL;

}

static void doTimesliceImpl(SOperatorInfo* pOperator, STimeSliceOperatorInfo* pSliceInfo, SSDataBlock* pBlock,
                            SExecTaskInfo* pTaskInfo, bool ignoreNull) {
  SSDataBlock* pResBlock = pSliceInfo->pRes;
  SInterval*   pInterval = &pSliceInfo->interval;

  SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pSliceInfo->tsCol.slotId);

  int32_t i = (pSliceInfo->pRemainRes == NULL) ? 0 : pSliceInfo->remainIndex;
  for (; i < pBlock->info.rows; ++i) {
    int64_t ts = *(int64_t*)colDataGetData(pTsCol, i);

    // check for duplicate timestamps
    if (checkDuplicateTimestamps(pSliceInfo, pTsCol, i, pBlock->info.rows)) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_FUNC_DUP_TIMESTAMP);
    }

    if (checkNullRow(&pOperator->exprSupp, pBlock, i, ignoreNull)) {
      continue;
    }

    if (ts == pSliceInfo->current) {
      addCurrentRowToResult(pSliceInfo, &pOperator->exprSupp, pResBlock, pBlock, i);

      doKeepPrevRows(pSliceInfo, pBlock, i);
      doKeepLinearInfo(pSliceInfo, pBlock, i);

      pSliceInfo->current =
          taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);

      if (checkWindowBoundReached(pSliceInfo)) {
        break;
      }
      if (checkThresholdReached(pSliceInfo, pOperator->resultInfo.threshold)) {
        saveBlockStatus(pSliceInfo, pBlock, i);
        return;
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
            if (!genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pResBlock, pBlock, i, false) &&
                pSliceInfo->fillType == TSDB_FILL_LINEAR) {
              break;
            } else {
              pSliceInfo->current = taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit,
                                                pInterval->precision);
            }
          }

          if (checkWindowBoundReached(pSliceInfo)) {
            break;
          }
          if (checkThresholdReached(pSliceInfo, pOperator->resultInfo.threshold)) {
            saveBlockStatus(pSliceInfo, pBlock, i);
            return;
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
        if (!genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pResBlock, pBlock, i, true) &&
            pSliceInfo->fillType == TSDB_FILL_LINEAR) {
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

      if (checkWindowBoundReached(pSliceInfo)) {
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

}

static void genInterpAfterDataBlock(STimeSliceOperatorInfo* pSliceInfo, SOperatorInfo* pOperator, int32_t index) {
  SSDataBlock* pResBlock = pSliceInfo->pRes;
  SInterval*   pInterval = &pSliceInfo->interval;

  while (pSliceInfo->current <= pSliceInfo->win.ekey && pSliceInfo->fillType != TSDB_FILL_NEXT &&
         pSliceInfo->fillType != TSDB_FILL_LINEAR) {
    genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pResBlock, NULL, index, false);
    pSliceInfo->current =
        taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
  }
}

static void copyPrevGroupKey(SExprSupp* pExprSup, SGroupKeys* pGroupKey, SSDataBlock* pSrcBlock) {
  for (int32_t j = 0; j < pExprSup->numOfExprs; ++j) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[j];

    if (isGroupKeyFunc(pExprInfo)) {
      int32_t       srcSlot = pExprInfo->base.pParam[0].pCol->slotId;
      SColumnInfoData* pSrc = taosArrayGet(pSrcBlock->pDataBlock, srcSlot);

      if (colDataIsNull_s(pSrc, 0)) {
        pGroupKey->isNull = true;
        break;
      }

      char* v = colDataGetData(pSrc, 0);
      if (IS_VAR_DATA_TYPE(pGroupKey->type)) {
        memcpy(pGroupKey->pData, v, varDataTLen(v));
      } else {
        memcpy(pGroupKey->pData, v, pGroupKey->bytes);
      }

      pGroupKey->isNull = false;
      break;
    }
  }
}

static void resetTimesliceInfo(STimeSliceOperatorInfo* pSliceInfo) {
  pSliceInfo->current = pSliceInfo->win.skey;
  pSliceInfo->prevTsSet = false;
  resetKeeperInfo(pSliceInfo);
}

static void doHandleTimeslice(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  STimeSliceOperatorInfo* pSliceInfo = pOperator->info;
  SExprSupp*              pSup = &pOperator->exprSupp;
  bool                    ignoreNull = getIgoreNullRes(pSup);
  int32_t                 order = TSDB_ORDER_ASC;

  if (checkWindowBoundReached(pSliceInfo)) {
    return;
  }

  int32_t code = initKeeperInfo(pSliceInfo, pBlock, &pOperator->exprSupp);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  if (pSliceInfo->scalarSup.pExprInfo != NULL) {
    SExprSupp* pExprSup = &pSliceInfo->scalarSup;
    projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
  }

  // the pDataBlock are always the same one, no need to call this again
  setInputDataBlock(pSup, pBlock, order, MAIN_SCAN, true);
  doTimesliceImpl(pOperator, pSliceInfo, pBlock, pTaskInfo, ignoreNull);
  copyPrevGroupKey(&pOperator->exprSupp, pSliceInfo->pPrevGroupKey, pBlock);
}

static SSDataBlock* doTimeslice(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;
  STimeSliceOperatorInfo* pSliceInfo = pOperator->info;
  SSDataBlock*            pResBlock = pSliceInfo->pRes;

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  blockDataCleanup(pResBlock);

  while (1) {
    if (pSliceInfo->pNextGroupRes != NULL) {
      doHandleTimeslice(pOperator, pSliceInfo->pNextGroupRes);
      if (checkWindowBoundReached(pSliceInfo) || checkThresholdReached(pSliceInfo, pOperator->resultInfo.threshold)) {
        doFilter(pResBlock, pOperator->exprSupp.pFilterInfo, NULL);
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
      SSDataBlock* pBlock = pSliceInfo->pRemainRes ? pSliceInfo->pRemainRes : getNextBlockFromDownstream(pOperator, 0);
      if (pBlock == NULL) {
        setOperatorCompleted(pOperator);
        break;
      }

      pResBlock->info.scanFlag = pBlock->info.scanFlag;
      if (pSliceInfo->groupId == 0 && pBlock->info.id.groupId != 0) {
        pSliceInfo->groupId = pBlock->info.id.groupId;
      } else {
        if (pSliceInfo->groupId != pBlock->info.id.groupId) {
          pSliceInfo->groupId = pBlock->info.id.groupId;
          pSliceInfo->pNextGroupRes = pBlock;
          break;
        }
      }

      doHandleTimeslice(pOperator, pBlock);
      if (checkWindowBoundReached(pSliceInfo) || checkThresholdReached(pSliceInfo, pOperator->resultInfo.threshold)) {
        doFilter(pResBlock, pOperator->exprSupp.pFilterInfo, NULL);
        if (pResBlock->info.rows != 0) {
          goto _finished;
        }
      }
    }
    // post work for a specific group

    // check if need to interpolate after last datablock
    // except for fill(next), fill(linear)
    genInterpAfterDataBlock(pSliceInfo, pOperator, 0);

    doFilter(pResBlock, pOperator->exprSupp.pFilterInfo, NULL);
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
  int32_t    code = initExprSupp(pSup, pExprInfo, numOfExprs, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (pInterpPhyNode->pExprs != NULL) {
    int32_t    num = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pInterpPhyNode->pExprs, NULL, &num);
    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, num, &pTaskInfo->storageAPI.functionStore);
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
  pInfo->prevTsSet = false;
  pInfo->prevTs = 0;
  pInfo->groupId = 0;
  pInfo->pPrevGroupKey = NULL;
  pInfo->pNextGroupRes = NULL;
  pInfo->pRemainRes = NULL;
  pInfo->remainIndex = 0;

  if (downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    STableScanInfo* pScanInfo = (STableScanInfo*)downstream->info;
    pScanInfo->base.cond.twindows = pInfo->win;
    pScanInfo->base.cond.type = TIMEWINDOW_RANGE_EXTERNAL;
  }

  setOperatorInfo(pOperator, "TimeSliceOperator", QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet =
      createOperatorFpSet(optrDummyOpenFn, doTimeslice, NULL, destroyTimeSliceOperatorInfo, optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

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

  if (pInfo->pPrevGroupKey) {
    taosMemoryFree(pInfo->pPrevGroupKey->pData);
    taosMemoryFree(pInfo->pPrevGroupKey);
  }

  cleanupExprSupp(&pInfo->scalarSup);

  for (int32_t i = 0; i < pInfo->pFillColInfo->numOfFillExpr; ++i) {
    taosVariantDestroy(&pInfo->pFillColInfo[i].fillVal);
  }
  taosMemoryFree(pInfo->pFillColInfo);
  taosMemoryFreeClear(param);
}
