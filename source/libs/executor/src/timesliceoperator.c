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
  SRowKey              prevKey;
  bool                 prevTsSet;
  uint64_t             groupId;
  SGroupKeys*          pPrevGroupKey;
  SSDataBlock*         pNextGroupRes;
  SSDataBlock*         pRemainRes;   // save block unfinished processing
  int32_t              remainIndex;  // the remaining index in the block to be processed
  bool                 hasPk;
  SColumn              pkCol;
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

static void tRowGetKeyFromColData(int64_t ts, SColumnInfoData* pPkCol, int32_t rowIndex, SRowKey* pKey) {
  pKey->ts = ts;
  pKey->numOfPKs = 1;

  int8_t t = pPkCol->info.type;

  pKey->pks[0].type = t;
  if (IS_NUMERIC_TYPE(t)) {
    GET_TYPED_DATA(pKey->pks[0].val, int64_t, t, colDataGetNumData(pPkCol, rowIndex));
  } else {
    char* p = colDataGetVarData(pPkCol, rowIndex);
    pKey->pks[0].pData = (uint8_t*)varDataVal(p);
    pKey->pks[0].nData = varDataLen(p);
  }
}

// only the timestamp is needed to complete the duplicated timestamp check.
static bool checkDuplicateTimestamps(STimeSliceOperatorInfo* pSliceInfo, SColumnInfoData* pTsCol,
                                     SColumnInfoData* pPkCol, int32_t curIndex, int32_t rows) {
  int64_t currentTs = *(int64_t*)colDataGetData(pTsCol, curIndex);
  if (currentTs > pSliceInfo->win.ekey) {
    return false;
  }

  SRowKey cur = {.ts = currentTs, .numOfPKs = (pPkCol != NULL) ? 1 : 0};
  if (pPkCol != NULL) {
    cur.pks[0].type = pPkCol->info.type;
    if (IS_VAR_DATA_TYPE(pPkCol->info.type)) {
      cur.pks[0].pData = (uint8_t*)colDataGetVarData(pPkCol, curIndex);
    } else {
      memcpy(&cur.pks[0].val, colDataGetData(pPkCol, curIndex), pPkCol->info.bytes);
    }
  }

  // let's discard the duplicated ts
  if ((pSliceInfo->prevTsSet == true) && (currentTs == pSliceInfo->prevKey.ts)) {
    return true;
  }

  pSliceInfo->prevTsSet = true;
  tRowKeyAssign(&pSliceInfo->prevKey, &cur);

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

static bool isSelectGroupConstValueFunc(SExprInfo* pExprInfo) {
  int32_t functionType = pExprInfo->pExpr->_function.functionType;
  return (functionType == FUNCTION_TYPE_GROUP_CONST_VALUE);
}

static bool getIgoreNullRes(SExprSupp* pExprSup) {
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

static bool genInterpolationResult(STimeSliceOperatorInfo* pSliceInfo, SExprSupp* pExprSup, SSDataBlock* pResBlock,
                                   SSDataBlock* pSrcBlock, int32_t index, bool beforeTs, SExecTaskInfo* pTaskInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t rows = pResBlock->info.rows;
  code = timeSliceEnsureBlockCapacity(pSliceInfo, pResBlock);
  QUERY_CHECK_CODE(code, lino, _end);
  // todo set the correct primary timestamp column

  // output the result
  int32_t fillColIndex = 0;
  bool    hasInterp = true;
  for (int32_t j = 0; j < pExprSup->numOfExprs; ++j) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[j];

    int32_t          dstSlot = pExprInfo->base.resSchema.slotId;
    SColumnInfoData* pDst = taosArrayGet(pResBlock->pDataBlock, dstSlot);

    if (isIrowtsPseudoColumn(pExprInfo)) {
      code = colDataSetVal(pDst, rows, (char*)&pSliceInfo->current, false);
      QUERY_CHECK_CODE(code, lino, _end);
      continue;
    } else if (isIsfilledPseudoColumn(pExprInfo)) {
      bool isFilled = true;
      code = colDataSetVal(pDst, pResBlock->info.rows, (char*)&isFilled, false);
      QUERY_CHECK_CODE(code, lino, _end);
      continue;
    } else if (!isInterpFunc(pExprInfo)) {
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
          SGroupKeys* pkey = pSliceInfo->pPrevGroupKey;
          if (pkey->isNull == false) {
            code = colDataSetVal(pDst, rows, pkey->pData, false);
            QUERY_CHECK_CODE(code, lino, _end);
          } else {
            colDataSetNULL(pDst, rows);
          }
        } else {
          int32_t     srcSlot = pExprInfo->base.pParam[0].pCol->slotId;
          SGroupKeys* pkey = taosArrayGet(pSliceInfo->pPrevRow, srcSlot);
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
          code = colDataSetVal(pDst, rows, (char*)&v, isNull);
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (pDst->info.type == TSDB_DATA_TYPE_DOUBLE) {
          double v = 0;
          if (!IS_VAR_DATA_TYPE(pVar->nType)) {
            GET_TYPED_DATA(v, double, pVar->nType, &pVar->d);
          } else {
            v = taosStr2Double(varDataVal(pVar->pz), NULL);
          }
          code = colDataSetVal(pDst, rows, (char*)&v, isNull);
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (IS_SIGNED_NUMERIC_TYPE(pDst->info.type)) {
          int64_t v = 0;
          if (!IS_VAR_DATA_TYPE(pVar->nType)) {
            GET_TYPED_DATA(v, int64_t, pVar->nType, &pVar->i);
          } else {
            v = taosStr2Int64(varDataVal(pVar->pz), NULL, 10);
          }
          code = colDataSetVal(pDst, rows, (char*)&v, isNull);
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (IS_UNSIGNED_NUMERIC_TYPE(pDst->info.type)) {
          uint64_t v = 0;
          if (!IS_VAR_DATA_TYPE(pVar->nType)) {
            GET_TYPED_DATA(v, uint64_t, pVar->nType, &pVar->u);
          } else {
            v = taosStr2UInt64(varDataVal(pVar->pz), NULL, 10);
          }
          code = colDataSetVal(pDst, rows, (char*)&v, isNull);
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (IS_BOOLEAN_TYPE(pDst->info.type)) {
          bool v = false;
          if (!IS_VAR_DATA_TYPE(pVar->nType)) {
            GET_TYPED_DATA(v, bool, pVar->nType, &pVar->i);
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
        QUERY_CHECK_NULL(current.val, code, lino, _end, terrno);
        taosGetLinearInterpolationVal(&current, pLinearInfo->type, &start, &end, pLinearInfo->type);
        code = colDataSetVal(pDst, rows, (char*)current.val, false);
        QUERY_CHECK_CODE(code, lino, _end);

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
          code = colDataSetVal(pDst, rows, pkey->pData, false);
          QUERY_CHECK_CODE(code, lino, _end);
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
          code = colDataSetVal(pDst, rows, pkey->pData, false);
          QUERY_CHECK_CODE(code, lino, _end);
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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return hasInterp;
}

static int32_t addCurrentRowToResult(STimeSliceOperatorInfo* pSliceInfo, SExprSupp* pExprSup, SSDataBlock* pResBlock,
                                     SSDataBlock* pSrcBlock, int32_t index) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  code = timeSliceEnsureBlockCapacity(pSliceInfo, pResBlock);
  QUERY_CHECK_CODE(code, lino, _end);
  for (int32_t j = 0; j < pExprSup->numOfExprs; ++j) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[j];

    int32_t          dstSlot = pExprInfo->base.resSchema.slotId;
    SColumnInfoData* pDst = taosArrayGet(pResBlock->pDataBlock, dstSlot);

    if (isIrowtsPseudoColumn(pExprInfo)) {
      code = colDataSetVal(pDst, pResBlock->info.rows, (char*)&pSliceInfo->current, false);
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

static int32_t initPrevRowsKeeper(STimeSliceOperatorInfo* pInfo, SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
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
    QUERY_CHECK_NULL(key.pData, code, lino, _end, terrno);
    void* tmp = taosArrayPush(pInfo->pPrevRow, &key);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  }

  pInfo->isPrevRowSet = false;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t initNextRowsKeeper(STimeSliceOperatorInfo* pInfo, SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
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
    QUERY_CHECK_NULL(key.pData, code, lino, _end, terrno);

    void* tmp = taosArrayPush(pInfo->pNextRow, &key);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  }

  pInfo->isNextRowSet = false;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t initFillLinearInfo(STimeSliceOperatorInfo* pInfo, SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
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
    QUERY_CHECK_NULL(linearInfo.start.val, code, lino, _end, terrno);

    linearInfo.end.val = taosMemoryCalloc(1, pColInfo->info.bytes);
    QUERY_CHECK_NULL(linearInfo.end.val, code, lino, _end, terrno);
    linearInfo.isStartSet = false;
    linearInfo.isEndSet = false;
    linearInfo.type = pColInfo->info.type;
    linearInfo.bytes = pColInfo->info.bytes;
    void* tmp = taosArrayPush(pInfo->pLinearInfo, &linearInfo);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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
      if (!pInfo->pPrevGroupKey->pData) {
        return terrno;
      }
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

static void resetPrevRowsKeeper(STimeSliceOperatorInfo* pInfo) {
  if (pInfo->pPrevRow == NULL) {
    return;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pLinearInfo); ++i) {
    SGroupKeys* pKey = taosArrayGet(pInfo->pPrevRow, i);
    pKey->isNull = false;
  }

  pInfo->isPrevRowSet = false;

  return;
}

static void resetNextRowsKeeper(STimeSliceOperatorInfo* pInfo) {
  if (pInfo->pNextRow == NULL) {
    return;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pLinearInfo); ++i) {
    SGroupKeys* pKey = taosArrayGet(pInfo->pPrevRow, i);
    pKey->isNull = false;
  }

  pInfo->isNextRowSet = false;

  return;
}

static void resetFillLinearInfo(STimeSliceOperatorInfo* pInfo) {
  if (pInfo->pLinearInfo == NULL) {
    return;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pLinearInfo); ++i) {
    SFillLinearInfo* pLinearInfo = taosArrayGet(pInfo->pLinearInfo, i);
    pLinearInfo->start.key = INT64_MIN;
    pLinearInfo->end.key = INT64_MIN;
    pLinearInfo->isStartSet = false;
    pLinearInfo->isEndSet = false;
  }

  return;
}

static void resetKeeperInfo(STimeSliceOperatorInfo* pInfo) {
  resetPrevRowsKeeper(pInfo);
  resetNextRowsKeeper(pInfo);
  resetFillLinearInfo(pInfo);
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
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pResBlock = pSliceInfo->pRes;
  SInterval*   pInterval = &pSliceInfo->interval;

  SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pSliceInfo->tsCol.slotId);
  SColumnInfoData* pPkCol = NULL;

  if (pSliceInfo->hasPk) {
    pPkCol = taosArrayGet(pBlock->pDataBlock, pSliceInfo->pkCol.slotId);
  }

  int32_t i = (pSliceInfo->pRemainRes == NULL) ? 0 : pSliceInfo->remainIndex;
  for (; i < pBlock->info.rows; ++i) {
    int64_t ts = *(int64_t*)colDataGetData(pTsCol, i);

    // check for duplicate timestamps
    if (checkDuplicateTimestamps(pSliceInfo, pTsCol, pPkCol, i, pBlock->info.rows)) {
      continue;
    }

    if (checkNullRow(&pOperator->exprSupp, pBlock, i, ignoreNull)) {
      continue;
    }

    if (ts == pSliceInfo->current) {
      code = addCurrentRowToResult(pSliceInfo, &pOperator->exprSupp, pResBlock, pBlock, i);
      QUERY_CHECK_CODE(code, lino, _end);

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
            if (!genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pResBlock, pBlock, i, false, pTaskInfo) &&
                pSliceInfo->fillType == TSDB_FILL_LINEAR) {
              break;
            } else {
              pSliceInfo->current =
                  taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
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
        if (!genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pResBlock, pBlock, i, true, pTaskInfo) &&
            pSliceInfo->fillType == TSDB_FILL_LINEAR) {
          break;
        } else {
          pSliceInfo->current =
              taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
        }
      }

      // add current row if timestamp match
      if (ts == pSliceInfo->current && pSliceInfo->current <= pSliceInfo->win.ekey) {
        code = addCurrentRowToResult(pSliceInfo, &pOperator->exprSupp, pResBlock, pBlock, i);
        QUERY_CHECK_CODE(code, lino, _end);

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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

static void genInterpAfterDataBlock(STimeSliceOperatorInfo* pSliceInfo, SOperatorInfo* pOperator, int32_t index) {
  SSDataBlock* pResBlock = pSliceInfo->pRes;
  SInterval*   pInterval = &pSliceInfo->interval;

  if (pSliceInfo->fillType == TSDB_FILL_NEXT || pSliceInfo->fillType == TSDB_FILL_LINEAR ||
      pSliceInfo->pPrevGroupKey == NULL) {
    return;
  }

  while (pSliceInfo->current <= pSliceInfo->win.ekey) {
    (void)genInterpolationResult(pSliceInfo, &pOperator->exprSupp, pResBlock, NULL, index, false, pOperator->pTaskInfo);
    pSliceInfo->current =
        taosTimeAdd(pSliceInfo->current, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
  }
}

static void copyPrevGroupKey(SExprSupp* pExprSup, SGroupKeys* pGroupKey, SSDataBlock* pSrcBlock) {
  for (int32_t j = 0; j < pExprSup->numOfExprs; ++j) {
    SExprInfo* pExprInfo = &pExprSup->pExprInfo[j];

    if (isGroupKeyFunc(pExprInfo)) {
      int32_t          srcSlot = pExprInfo->base.pParam[0].pCol->slotId;
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
    code = projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      T_LONG_JMP(pTaskInfo->env, code);
    }
  }

  // the pDataBlock are always the same one, no need to call this again
  code = setInputDataBlock(pSup, pBlock, order, MAIN_SCAN, true);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }
  doTimesliceImpl(pOperator, pSliceInfo, pBlock, pTaskInfo, ignoreNull);
  copyPrevGroupKey(&pOperator->exprSupp, pSliceInfo->pPrevGroupKey, pBlock);
}

static int32_t doTimesliceNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  }

  STimeSliceOperatorInfo* pSliceInfo = pOperator->info;
  SSDataBlock*            pResBlock = pSliceInfo->pRes;

  blockDataCleanup(pResBlock);

  while (1) {
    if (pSliceInfo->pNextGroupRes != NULL) {
      doHandleTimeslice(pOperator, pSliceInfo->pNextGroupRes);
      if (checkWindowBoundReached(pSliceInfo) || checkThresholdReached(pSliceInfo, pOperator->resultInfo.threshold)) {
        code = doFilter(pResBlock, pOperator->exprSupp.pFilterInfo, NULL);
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
        code = doFilter(pResBlock, pOperator->exprSupp.pFilterInfo, NULL);
        QUERY_CHECK_CODE(code, lino, _finished);
        if (pResBlock->info.rows != 0) {
          goto _finished;
        }
      }
    }
    // post work for a specific group

    // check if need to interpolate after last datablock
    // except for fill(next), fill(linear)
    genInterpAfterDataBlock(pSliceInfo, pOperator, 0);

    code = doFilter(pResBlock, pOperator->exprSupp.pFilterInfo, NULL);
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

static SSDataBlock* doTimeslice(SOperatorInfo* pOperator) {
  SSDataBlock* pRes = NULL;
  int32_t code = doTimesliceNext(pOperator, &pRes);
  return pRes;
}

static int32_t extractPkColumnFromFuncs(SNodeList* pFuncs, bool* pHasPk, SColumn* pPkColumn) {
  SNode* pNode;
  FOREACH(pNode, pFuncs) {
    if ((nodeType(pNode) == QUERY_NODE_TARGET) && (nodeType(((STargetNode*)pNode)->pExpr) == QUERY_NODE_FUNCTION)) {
      SFunctionNode* pFunc = (SFunctionNode*)((STargetNode*)pNode)->pExpr;
      if (fmIsInterpFunc(pFunc->funcId) && pFunc->hasPk) {
        SNode* pNode2 = (pFunc->pParameterList->pTail->pNode);
        if ((nodeType(pNode2) == QUERY_NODE_COLUMN) && ((SColumnNode*)pNode2)->isPk) {
          *pHasPk = true;
          *pPkColumn = extractColumnFromColumnNode((SColumnNode*)pNode2);
          break;
        }
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t createTimeSliceOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_OPTR_CHECK(pOptrInfo);

  int32_t code = 0;
  int32_t lino = 0;
  STimeSliceOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(STimeSliceOperatorInfo));
  SOperatorInfo*          pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

  if (pOperator == NULL || pInfo == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  SInterpFuncPhysiNode* pInterpPhyNode = (SInterpFuncPhysiNode*)pPhyNode;
  SExprSupp*            pSup = &pOperator->exprSupp;

  int32_t    numOfExprs = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pInterpPhyNode->pFuncs, NULL, &pExprInfo, &numOfExprs);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initExprSupp(pSup, pExprInfo, numOfExprs, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pInterpPhyNode->pExprs != NULL) {
    int32_t    num = 0;
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pInterpPhyNode->pExprs, NULL, &pScalarExprInfo, &num);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, num, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  code = filterInitFromNode((SNode*)pInterpPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->tsCol = extractColumnFromColumnNode((SColumnNode*)pInterpPhyNode->pTimeSeries);
  code = extractPkColumnFromFuncs(pInterpPhyNode->pFuncs, &pInfo->hasPk, &pInfo->pkCol);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->fillType = convertFillType(pInterpPhyNode->fillMode);
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  pInfo->pFillColInfo = createFillColInfo(pExprInfo, numOfExprs, NULL, 0, (SNodeListNode*)pInterpPhyNode->pFillValues);
  QUERY_CHECK_NULL(pInfo->pFillColInfo, code, lino, _error, terrno);

  pInfo->pLinearInfo = NULL;
  pInfo->pRes = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);
  pInfo->win = pInterpPhyNode->timeRange;
  pInfo->interval.interval = pInterpPhyNode->interval;
  pInfo->current = pInfo->win.skey;
  pInfo->prevTsSet = false;
  pInfo->prevKey.ts = INT64_MIN;
  pInfo->groupId = 0;
  pInfo->pPrevGroupKey = NULL;
  pInfo->pNextGroupRes = NULL;
  pInfo->pRemainRes = NULL;
  pInfo->remainIndex = 0;

  if (pInfo->hasPk) {
    pInfo->prevKey.numOfPKs = 1;
    pInfo->prevKey.ts = INT64_MIN;
    pInfo->prevKey.pks[0].type = pInfo->pkCol.type;

    if (IS_VAR_DATA_TYPE(pInfo->pkCol.type)) {
      pInfo->prevKey.pks[0].pData = taosMemoryCalloc(1, pInfo->pkCol.bytes);
      QUERY_CHECK_NULL(pInfo->prevKey.pks[0].pData, code, lino, _error, terrno);
    }
  }

  if (downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    STableScanInfo* pScanInfo = (STableScanInfo*)downstream->info;
    pScanInfo->base.cond.twindows = pInfo->win;
    pScanInfo->base.cond.type = TIMEWINDOW_RANGE_EXTERNAL;
  }

  setOperatorInfo(pOperator, "TimeSliceOperator", QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doTimeslice, NULL, destroyTimeSliceOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  //  int32_t code = initKeeperInfo(pSliceInfo, pBlock, &pOperator->exprSupp);

  code = appendDownstream(pOperator, &downstream, 1);

  *pOptrInfo = pOperator;
  return code;

_error:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (pInfo != NULL) destroyTimeSliceOperatorInfo(pInfo);
  if (pOperator != NULL) {
    pOperator->info = NULL;
    if (pOperator->pDownstream == NULL && downstream != NULL) {
      destroyOperator(downstream);
    }
    destroyOperator(pOperator);
  }
  pTaskInfo->code = code;
  return code;
}

void destroyTimeSliceOperatorInfo(void* param) {
  STimeSliceOperatorInfo* pInfo = (STimeSliceOperatorInfo*)param;

  blockDataDestroy(pInfo->pRes);
  pInfo->pRes = NULL;

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
  if (pInfo->pFillColInfo != NULL) {
    for (int32_t i = 0; i < pInfo->pFillColInfo->numOfFillExpr; ++i) {
      taosVariantDestroy(&pInfo->pFillColInfo[i].fillVal);
    }
    taosMemoryFree(pInfo->pFillColInfo);
  }
  taosMemoryFreeClear(param);
}
