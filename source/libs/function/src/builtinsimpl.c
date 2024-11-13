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

#include "builtinsimpl.h"
#include "cJSON.h"
#include "function.h"
#include "functionResInfoInt.h"
#include "query.h"
#include "querynodes.h"
#include "tanalytics.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tdigest.h"
#include "tfunctionInt.h"
#include "tglobal.h"
#include "thistogram.h"
#include "tpercentile.h"

bool ignoreNegative(int8_t ignoreOption){
  return (ignoreOption & 0x1) == 0x1;
}
bool ignoreNull(int8_t ignoreOption){
  return (ignoreOption & 0x2) == 0x2;
}

typedef enum {
  APERCT_ALGO_UNKNOWN = 0,
  APERCT_ALGO_DEFAULT,
  APERCT_ALGO_TDIGEST,
} EAPerctAlgoType;

typedef enum { UNKNOWN_BIN = 0, USER_INPUT_BIN, LINEAR_BIN, LOG_BIN } EHistoBinType;

typedef enum {
  STATE_OPER_INVALID = 0,
  STATE_OPER_LT,
  STATE_OPER_GT,
  STATE_OPER_LE,
  STATE_OPER_GE,
  STATE_OPER_NE,
  STATE_OPER_EQ,
} EStateOperType;

#define SET_VAL(_info, numOfElem, res) \
  do {                                 \
    if ((numOfElem) <= 0) {            \
      break;                           \
    }                                  \
    (_info)->numOfRes = (res);         \
  } while (0)

#define GET_TS_LIST(x)    ((TSKEY*)((x)->ptsList))
#define GET_TS_DATA(x, y) (GET_TS_LIST(x)[(y)])

#define DO_UPDATE_SUBSID_RES(ctx, ts)                          \
  do {                                                         \
    for (int32_t _i = 0; _i < (ctx)->subsidiaries.num; ++_i) { \
      SqlFunctionCtx* __ctx = (ctx)->subsidiaries.pCtx[_i];    \
      if (__ctx->functionId == FUNCTION_TS_DUMMY) {            \
        __ctx->tag.i = (ts);                                   \
        __ctx->tag.nType = TSDB_DATA_TYPE_BIGINT;              \
      }                                                        \
      __ctx->fpSet.process(__ctx);                             \
    }                                                          \
  } while (0)

#define UPDATE_DATA(ctx, left, right, num, sign, _ts) \
  do {                                                \
    if (((left) < (right)) ^ (sign)) {                \
      (left) = (right);                               \
      DO_UPDATE_SUBSID_RES(ctx, _ts);                 \
      (num) += 1;                                     \
    }                                                 \
  } while (0)

#define LOOPCHECK_N(val, _col, ctx, _t, _nrow, _start, sign, num)        \
  do {                                                                   \
    _t* d = (_t*)((_col)->pData);                                        \
    for (int32_t i = (_start); i < (_nrow) + (_start); ++i) {            \
      if (((_col)->hasNull) && colDataIsNull_f((_col)->nullbitmap, i)) { \
        continue;                                                        \
      }                                                                  \
      TSKEY ts = (ctx)->ptsList != NULL ? GET_TS_DATA(ctx, i) : 0;       \
      UPDATE_DATA(ctx, val, d[i], num, sign, ts);                        \
    }                                                                    \
  } while (0)

#define LIST_ADD_N(_res, _col, _start, _rows, _t, numOfElem)             \
  do {                                                                   \
    _t* d = (_t*)(_col->pData);                                          \
    for (int32_t i = (_start); i < (_rows) + (_start); ++i) {            \
      if (((_col)->hasNull) && colDataIsNull_f((_col)->nullbitmap, i)) { \
        continue;                                                        \
      };                                                                 \
      (_res) += (d)[i];                                                  \
      (numOfElem)++;                                                     \
    }                                                                    \
  } while (0)

#define LIST_SUB_N(_res, _col, _start, _rows, _t, numOfElem)             \
  do {                                                                   \
    _t* d = (_t*)(_col->pData);                                          \
    for (int32_t i = (_start); i < (_rows) + (_start); ++i) {            \
      if (((_col)->hasNull) && colDataIsNull_f((_col)->nullbitmap, i)) { \
        continue;                                                        \
      };                                                                 \
      (_res) -= (d)[i];                                                  \
      (numOfElem)++;                                                     \
    }                                                                    \
  } while (0)

//#define LIST_AVG_N(sumT, T)                                               \
//  do {                                                                    \
//    T* plist = (T*)pCol->pData;                                           \
//    for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) { \
//      if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {        \
//        continue;                                                         \
//      }                                                                   \
//                                                                          \
//      numOfElem += 1;                                                     \
//      pAvgRes->count -= 1;                                                \
//      sumT -= plist[i];                                                   \
//    }                                                                     \
//  } while (0)

#define LIST_STDDEV_SUB_N(sumT, T)                                 \
  do {                                                             \
    T* plist = (T*)pCol->pData;                                    \
    for (int32_t i = start; i < numOfRows + start; ++i) {          \
      if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) { \
        continue;                                                  \
      }                                                            \
      numOfElem += 1;                                              \
      pStddevRes->count -= 1;                                      \
      sumT -= plist[i];                                            \
      pStddevRes->quadraticISum -= (int64_t)(plist[i] * plist[i]); \
    }                                                              \
  } while (0)

#define LEASTSQR_CAL(p, x, y, index, step) \
  do {                                     \
    (p)[0][0] += (double)(x) * (x);        \
    (p)[0][1] += (double)(x);              \
    (p)[0][2] += (double)(x) * (y)[index]; \
    (p)[1][2] += (y)[index];               \
    (x) += step;                           \
  } while (0)

#define STATE_COMP(_op, _lval, _param) STATE_COMP_IMPL(_op, _lval, GET_STATE_VAL(_param))

#define GET_STATE_VAL(param) ((param.nType == TSDB_DATA_TYPE_BIGINT) ? (param.i) : (param.d))

#define STATE_COMP_IMPL(_op, _lval, _rval) \
  do {                                     \
    switch (_op) {                         \
      case STATE_OPER_LT:                  \
        return ((_lval) < (_rval));        \
        break;                             \
      case STATE_OPER_GT:                  \
        return ((_lval) > (_rval));        \
        break;                             \
      case STATE_OPER_LE:                  \
        return ((_lval) <= (_rval));       \
        break;                             \
      case STATE_OPER_GE:                  \
        return ((_lval) >= (_rval));       \
        break;                             \
      case STATE_OPER_NE:                  \
        return ((_lval) != (_rval));       \
        break;                             \
      case STATE_OPER_EQ:                  \
        return ((_lval) == (_rval));       \
        break;                             \
      default:                             \
        break;                             \
    }                                      \
  } while (0)

#define INIT_INTP_POINT(_p, _k, _v) \
  do {                              \
    (_p).key = (_k);                \
    (_p).val = (_v);                \
  } while (0)

void funcInputUpdate(SqlFunctionCtx* pCtx) {
  SFuncInputRowIter* pIter = &pCtx->rowIter;

  if (!pCtx->bInputFinished) {
    pIter->pInput = &pCtx->input;
    pIter->tsList = (TSKEY*)pIter->pInput->pPTS->pData;
    pIter->pDataCol = pIter->pInput->pData[0];
    pIter->pPkCol = pIter->pInput->pPrimaryKey;
    pIter->rowIndex = pIter->pInput->startRowIndex;
    pIter->inputEndIndex = pIter->rowIndex + pIter->pInput->numOfRows - 1;
    pIter->pSrcBlock = pCtx->pSrcBlock;
    if (!pIter->hasGroupId || pIter->groupId != pIter->pSrcBlock->info.id.groupId) {
      pIter->hasGroupId = true;
      pIter->groupId = pIter->pSrcBlock->info.id.groupId;
      pIter->hasPrev = false;
    }
  } else {
    pIter->finalRow = true;
  }
}

int32_t funcInputGetNextRowDescPk(SFuncInputRowIter* pIter, SFuncInputRow* pRow, bool *res) {
  if (pIter->finalRow) {
    if (pIter->hasPrev) {
      pRow->ts = pIter->prevBlockTsEnd;
      pRow->isDataNull = pIter->prevIsDataNull;
      pRow->pData = pIter->pPrevData;
      pRow->block = pIter->pPrevRowBlock;
      pRow->rowIndex = 0;
      
      pIter->hasPrev = false;
      *res = true;
      return TSDB_CODE_SUCCESS;
    } else {
      *res = false;
      return TSDB_CODE_SUCCESS;
    }
  }
  if (pIter->hasPrev) {
    if (pIter->prevBlockTsEnd == pIter->tsList[pIter->inputEndIndex]) {
      blockDataDestroy(pIter->pPrevRowBlock);
      int32_t code = blockDataExtractBlock(pIter->pSrcBlock, pIter->inputEndIndex, 1, &pIter->pPrevRowBlock);
      if (code) {
        return code;
      }

      pIter->prevIsDataNull = colDataIsNull_f(pIter->pDataCol->nullbitmap, pIter->inputEndIndex);

      pIter->pPrevData = taosMemoryMalloc(pIter->pDataCol->info.bytes);
      if (NULL == pIter->pPrevData) {
        qError("out of memory when function get input row.");
        return terrno;
      }
      char* srcData = colDataGetData(pIter->pDataCol, pIter->inputEndIndex);
      (void)memcpy(pIter->pPrevData, srcData, pIter->pDataCol->info.bytes);

      pIter->pPrevPk = taosMemoryMalloc(pIter->pPkCol->info.bytes);
      if (NULL == pIter->pPrevPk) {
        qError("out of memory when function get input row.");
        taosMemoryFree(pIter->pPrevData);
        return terrno;
      }
      char* pkData = colDataGetData(pIter->pPkCol, pIter->inputEndIndex);
      (void)memcpy(pIter->pPrevPk, pkData, pIter->pPkCol->info.bytes);

      code = blockDataExtractBlock(pIter->pSrcBlock, pIter->inputEndIndex, 1, &pIter->pPrevRowBlock);
      pIter->hasPrev = true;
      *res = false;
      return code;
    } else {
      int32_t idx = pIter->rowIndex;
      while (pIter->tsList[idx] == pIter->prevBlockTsEnd) {
        ++idx;
      }
      pRow->ts = pIter->prevBlockTsEnd;
      if (idx == pIter->pInput->startRowIndex) {
        pRow->isDataNull = pIter->prevIsDataNull;
        pRow->pData = pIter->pPrevData;
        pRow->block = pIter->pPrevRowBlock;
        pRow->rowIndex = 0;
      } else {
        pRow->ts = pIter->tsList[idx - 1];
        pRow->isDataNull = colDataIsNull_f(pIter->pDataCol->nullbitmap, idx - 1);
        pRow->pData = colDataGetData(pIter->pDataCol, idx - 1);
        pRow->pPk = colDataGetData(pIter->pPkCol, idx - 1);
        pRow->block = pIter->pSrcBlock;
        pRow->rowIndex = idx - 1;
      }
      pIter->hasPrev = false;
      pIter->rowIndex = idx;
      *res = true;
      return TSDB_CODE_SUCCESS;
    }
  } else {
    TSKEY tsEnd = pIter->tsList[pIter->inputEndIndex];
    if (pIter->tsList[pIter->rowIndex] != tsEnd) {
      int32_t idx = pIter->rowIndex;
      while (pIter->tsList[idx + 1] == pIter->tsList[pIter->rowIndex]) {
        ++idx;
      }
      pRow->ts = pIter->tsList[idx];
      pRow->isDataNull = colDataIsNull_f(pIter->pDataCol->nullbitmap, idx);
      pRow->pData = colDataGetData(pIter->pDataCol, idx);
      pRow->pPk = colDataGetData(pIter->pPkCol, idx);
      pRow->block = pIter->pSrcBlock;

      pIter->rowIndex = idx + 1;
      *res = true;
      return TSDB_CODE_SUCCESS;
    } else {
      pIter->hasPrev = true;
      pIter->prevBlockTsEnd = tsEnd;
      pIter->prevIsDataNull = colDataIsNull_f(pIter->pDataCol->nullbitmap, pIter->inputEndIndex);
      pIter->pPrevData = taosMemoryMalloc(pIter->pDataCol->info.bytes);
      if (NULL == pIter->pPrevData) {
        qError("out of memory when function get input row.");
        return terrno;
      }
      (void)memcpy(pIter->pPrevData, colDataGetData(pIter->pDataCol, pIter->inputEndIndex), pIter->pDataCol->info.bytes);
      pIter->pPrevPk = taosMemoryMalloc(pIter->pPkCol->info.bytes);
      if (NULL == pIter->pPrevPk) {
        qError("out of memory when function get input row.");
        taosMemoryFree(pIter->pPrevData);
        return terrno;
      }
      (void)memcpy(pIter->pPrevPk, colDataGetData(pIter->pPkCol, pIter->inputEndIndex), pIter->pPkCol->info.bytes);

      int32_t code = blockDataExtractBlock(pIter->pSrcBlock, pIter->inputEndIndex, 1, &pIter->pPrevRowBlock);
      *res = false;
      return code;
    }
  }
}

static void forwardToNextDiffTsRow(SFuncInputRowIter* pIter, int32_t rowIndex) {
  int32_t idx = rowIndex + 1;
  while (idx <= pIter->inputEndIndex && pIter->tsList[idx] == pIter->tsList[rowIndex]) {
    ++idx;
  }
  pIter->rowIndex = idx;
}

static void setInputRowInfo(SFuncInputRow* pRow, SFuncInputRowIter* pIter, int32_t rowIndex, bool setPk) {
  pRow->ts = pIter->tsList[rowIndex];
  pRow->ts = pIter->tsList[rowIndex];
  pRow->isDataNull = colDataIsNull_f(pIter->pDataCol->nullbitmap, rowIndex);
  pRow->pData = colDataGetData(pIter->pDataCol, rowIndex);
  pRow->pPk = setPk? colDataGetData(pIter->pPkCol, rowIndex):NULL;
  pRow->block = pIter->pSrcBlock;
  pRow->rowIndex = rowIndex;
}

bool funcInputGetNextRowAscPk(SFuncInputRowIter *pIter, SFuncInputRow* pRow) {
  if (pIter->hasPrev) {
    if (pIter->prevBlockTsEnd == pIter->tsList[pIter->inputEndIndex]) {
      pIter->hasPrev = true;
      return false;
    } else {
      int32_t idx = pIter->rowIndex;
      while (pIter->tsList[idx] == pIter->prevBlockTsEnd) {
        ++idx;
      }

      pIter->hasPrev = false;
      setInputRowInfo(pRow, pIter, idx, true);
      forwardToNextDiffTsRow(pIter, idx);
      return true;
    }
  } else {
    if (pIter->rowIndex <= pIter->inputEndIndex) { 
      setInputRowInfo(pRow, pIter, pIter->rowIndex, true);

      TSKEY tsEnd = pIter->tsList[pIter->inputEndIndex];
      if (pIter->tsList[pIter->rowIndex] != tsEnd) {
        forwardToNextDiffTsRow(pIter, pIter->rowIndex);
      } else {
        pIter->rowIndex = pIter->inputEndIndex + 1;
      }
      return true;
    } else {
      TSKEY tsEnd = pIter->tsList[pIter->inputEndIndex];
      pIter->hasPrev = true;
      pIter->prevBlockTsEnd = tsEnd;
      return false;
    }
  }
}

bool funcInputGetNextRowNoPk(SFuncInputRowIter *pIter, SFuncInputRow* pRow) {
  if (pIter->rowIndex <= pIter->inputEndIndex) {
    setInputRowInfo(pRow, pIter, pIter->rowIndex, false);
    ++pIter->rowIndex;
    return true;    
  } else {
    return false;    
  }
}

int32_t funcInputGetNextRow(SqlFunctionCtx* pCtx, SFuncInputRow* pRow, bool *res) {
  SFuncInputRowIter* pIter = &pCtx->rowIter;
  if (pCtx->hasPrimaryKey) {
    if (pCtx->order == TSDB_ORDER_ASC) {
      *res = funcInputGetNextRowAscPk(pIter, pRow);
      return TSDB_CODE_SUCCESS;
    } else {
      return funcInputGetNextRowDescPk(pIter, pRow, res);
    }
  } else {
    *res = funcInputGetNextRowNoPk(pIter, pRow);
    return TSDB_CODE_SUCCESS;
  }
  return TSDB_CODE_SUCCESS;
}

// This function append the selectivity to subsidiaries function context directly, without fetching data
// from intermediate disk based buf page
int32_t appendSelectivityCols(SqlFunctionCtx* pCtx, SSDataBlock* pSrcBlock, int32_t rowIndex, int32_t pos) {
  if (pCtx->subsidiaries.num <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t j = 0; j < pCtx->subsidiaries.num; ++j) {
    SqlFunctionCtx* pc = pCtx->subsidiaries.pCtx[j];

    // get data from source col
    SFunctParam* pFuncParam = &pc->pExpr->base.pParam[0];
    int32_t      srcSlotId = pFuncParam->pCol->slotId;

    SColumnInfoData* pSrcCol = taosArrayGet(pSrcBlock->pDataBlock, srcSlotId);
    if (NULL == pSrcCol) {
      return TSDB_CODE_OUT_OF_RANGE;
    }

    char* pData = colDataGetData(pSrcCol, rowIndex);

    // append to dest col
    int32_t dstSlotId = pc->pExpr->base.resSchema.slotId;

    SColumnInfoData* pDstCol = taosArrayGet(pCtx->pDstBlock->pDataBlock, dstSlotId);
    if (NULL == pDstCol) {
      return TSDB_CODE_OUT_OF_RANGE;
    }
    if (colDataIsNull_s(pSrcCol, rowIndex) == true) {
      colDataSetNULL(pDstCol, pos);
    } else {
      int32_t code = colDataSetVal(pDstCol, pos, pData, false);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

bool funcInputGetNextRowIndex(SInputColumnInfoData* pInput, int32_t from, bool firstOccur, int32_t* pRowIndex, int32_t* nextFrom);

static bool firstLastTransferInfoImpl(SFirstLastRes* pInput, SFirstLastRes* pOutput, bool isFirst);

int32_t functionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (pResultInfo->initialized) {
    return TSDB_CODE_SUCCESS;  // already initialized
  }

  if (pCtx->pOutput != NULL) {
    (void)memset(pCtx->pOutput, 0, (size_t)pCtx->resDataInfo.bytes);
  }

  initResultRowEntry(pResultInfo, pCtx->resDataInfo.interBufSize);
  return TSDB_CODE_SUCCESS;
}

int32_t functionFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  if (NULL == pCol) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  pResInfo->isNullRes = (pResInfo->numOfRes == 0) ? 1 : 0;

  char* in = GET_ROWCELL_INTERBUF(pResInfo);
  code = colDataSetVal(pCol, pBlock->info.rows, in, pResInfo->isNullRes);

  return code;
}

int32_t firstCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SFirstLastRes*       pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);
  int32_t              bytes = pDBuf->bytes;

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SFirstLastRes*       pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  pDBuf->hasResult = firstLastTransferInfoImpl(pSBuf, pDBuf, true);

  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
  return TSDB_CODE_SUCCESS;
}

int32_t functionFinalizeWithResultBuf(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, char* finalResult) {
  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  if (NULL == pCol) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  pResInfo->isNullRes = (pResInfo->numOfRes == 0) ? 1 : 0;

  char* in = finalResult;
  int32_t code = colDataSetVal(pCol, pBlock->info.rows, in, pResInfo->isNullRes);

  return code;
}

EFuncDataRequired countDataRequired(SFunctionNode* pFunc, STimeWindow* pTimeWindow) {
  SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
  if (QUERY_NODE_COLUMN == nodeType(pParam) && PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pParam)->colId) {
    return FUNC_DATA_REQUIRED_NOT_LOAD;
  }
  return FUNC_DATA_REQUIRED_SMA_LOAD;
}

bool getCountFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(int64_t);
  return true;
}

static int64_t getNumOfElems(SqlFunctionCtx* pCtx) {
  int64_t numOfElem = 0;

  /*
   * 1. column data missing (schema modified) causes pInputCol->hasNull == true. pInput->colDataSMAIsSet == true;
   * 2. for general non-primary key columns, pInputCol->hasNull may be true or false, pInput->colDataSMAIsSet == true;
   * 3. for primary key column, pInputCol->hasNull always be false, pInput->colDataSMAIsSet == false;
   */
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];
  if(1 == pInput->numOfRows && pInput->blankFill) {
    return 0;
  }
  if (pInput->colDataSMAIsSet && pInput->totalRows == pInput->numOfRows) {
    numOfElem = pInput->numOfRows - pInput->pColumnDataAgg[0]->numOfNull;
  } else {
    if (pInputCol->hasNull) {
      for (int32_t i = pInput->startRowIndex; i < pInput->startRowIndex + pInput->numOfRows; ++i) {
        if (colDataIsNull(pInputCol, pInput->totalRows, i, NULL)) {
          continue;
        }
        numOfElem += 1;
      }
    } else {
      // when counting on the primary time stamp column and no statistics data is presented, use the size value
      // directly.
      numOfElem = pInput->numOfRows;
    }
  }
  return numOfElem;
}

/*
 * count function does need the finalize, if data is missing, the default value, which is 0, is used
 * count function does not use the pCtx->interResBuf to keep the intermediate buffer
 */
int32_t countFunction(SqlFunctionCtx* pCtx) {
  int64_t numOfElem = 0;

  SResultRowEntryInfo*  pResInfo = GET_RES_INFO(pCtx);
  SInputColumnInfoData* pInput = &pCtx->input;

  int32_t type = pInput->pData[0]->info.type;

  char* buf = GET_ROWCELL_INTERBUF(pResInfo);
  if (IS_NULL_TYPE(type)) {
    // select count(NULL) returns 0
    numOfElem = 1;
    *((int64_t*)buf) += 0;
  } else {
    numOfElem = getNumOfElems(pCtx);
    *((int64_t*)buf) += numOfElem;
  }

  if (tsCountAlwaysReturnValue) {
    pResInfo->numOfRes = 1;
  } else {
    SET_VAL(pResInfo, *((int64_t*)buf), 1);
  }

  return TSDB_CODE_SUCCESS;
}

#ifdef BUILD_NO_CALL
int32_t countInvertFunction(SqlFunctionCtx* pCtx) {
  int64_t numOfElem = getNumOfElems(pCtx);

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  char*                buf = GET_ROWCELL_INTERBUF(pResInfo);
  *((int64_t*)buf) -= numOfElem;

  SET_VAL(pResInfo, *((int64_t*)buf), 1);
  return TSDB_CODE_SUCCESS;
}
#endif

int32_t combineFunction(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  char*                pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  char*                pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);
  *((int64_t*)pDBuf) += *((int64_t*)pSBuf);

  SET_VAL(pDResInfo, *((int64_t*)pDBuf), 1);
  return TSDB_CODE_SUCCESS;
}

int32_t sumFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];
  int32_t               type = pInput->pData[0]->info.type;

  SSumRes* pSumRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  pSumRes->type = type;

  if (IS_NULL_TYPE(type)) {
    numOfElem = 0;
    goto _sum_over;
  }

  if (pInput->colDataSMAIsSet) {
    numOfElem = pInput->numOfRows - pAgg->numOfNull;

    if (IS_SIGNED_NUMERIC_TYPE(type)) {
      pSumRes->isum += pAgg->sum;
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      pSumRes->usum += pAgg->sum;
    } else if (IS_FLOAT_TYPE(type)) {
      pSumRes->dsum += GET_DOUBLE_VAL((const char*)&(pAgg->sum));
    }
  } else {  // computing based on the true data block
    SColumnInfoData* pCol = pInput->pData[0];

    int32_t start = pInput->startRowIndex;
    int32_t numOfRows = pInput->numOfRows;

    if (IS_SIGNED_NUMERIC_TYPE(type) || type == TSDB_DATA_TYPE_BOOL) {
      if (type == TSDB_DATA_TYPE_TINYINT || type == TSDB_DATA_TYPE_BOOL) {
        LIST_ADD_N(pSumRes->isum, pCol, start, numOfRows, int8_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_SMALLINT) {
        LIST_ADD_N(pSumRes->isum, pCol, start, numOfRows, int16_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_INT) {
        LIST_ADD_N(pSumRes->isum, pCol, start, numOfRows, int32_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_BIGINT) {
        LIST_ADD_N(pSumRes->isum, pCol, start, numOfRows, int64_t, numOfElem);
      }
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      if (type == TSDB_DATA_TYPE_UTINYINT) {
        LIST_ADD_N(pSumRes->usum, pCol, start, numOfRows, uint8_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_USMALLINT) {
        LIST_ADD_N(pSumRes->usum, pCol, start, numOfRows, uint16_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_UINT) {
        LIST_ADD_N(pSumRes->usum, pCol, start, numOfRows, uint32_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_UBIGINT) {
        LIST_ADD_N(pSumRes->usum, pCol, start, numOfRows, uint64_t, numOfElem);
      }
    } else if (type == TSDB_DATA_TYPE_DOUBLE) {
      LIST_ADD_N(pSumRes->dsum, pCol, start, numOfRows, double, numOfElem);
    } else if (type == TSDB_DATA_TYPE_FLOAT) {
      LIST_ADD_N(pSumRes->dsum, pCol, start, numOfRows, float, numOfElem);
    }
  }

  // check for overflow
  if (IS_FLOAT_TYPE(type) && (isinf(pSumRes->dsum) || isnan(pSumRes->dsum))) {
    numOfElem = 0;
  }

_sum_over:
  if (numOfElem == 0) {
    if (tsCountAlwaysReturnValue && pCtx->pExpr->pExpr->_function.pFunctNode->hasOriginalFunc &&
        fmIsCountLikeFunc(pCtx->pExpr->pExpr->_function.pFunctNode->originalFuncId)) {
      numOfElem = 1;
    }
  }
  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElem, 1);
  return TSDB_CODE_SUCCESS;
}

#ifdef BUILD_NO_CALL
int32_t sumInvertFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];
  int32_t               type = pInput->pData[0]->info.type;

  SSumRes* pSumRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  if (pInput->colDataSMAIsSet) {
    numOfElem = pInput->numOfRows - pAgg->numOfNull;

    if (IS_SIGNED_NUMERIC_TYPE(type)) {
      pSumRes->isum -= pAgg->sum;
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      pSumRes->usum -= pAgg->sum;
    } else if (IS_FLOAT_TYPE(type)) {
      pSumRes->dsum -= GET_DOUBLE_VAL((const char*)&(pAgg->sum));
    }
  } else {  // computing based on the true data block
    SColumnInfoData* pCol = pInput->pData[0];

    int32_t start = pInput->startRowIndex;
    int32_t numOfRows = pInput->numOfRows;

    if (IS_SIGNED_NUMERIC_TYPE(type) || type == TSDB_DATA_TYPE_BOOL) {
      if (type == TSDB_DATA_TYPE_TINYINT || type == TSDB_DATA_TYPE_BOOL) {
        LIST_SUB_N(pSumRes->isum, pCol, start, numOfRows, int8_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_SMALLINT) {
        LIST_SUB_N(pSumRes->isum, pCol, start, numOfRows, int16_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_INT) {
        LIST_SUB_N(pSumRes->isum, pCol, start, numOfRows, int32_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_BIGINT) {
        LIST_SUB_N(pSumRes->isum, pCol, start, numOfRows, int64_t, numOfElem);
      }
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      if (type == TSDB_DATA_TYPE_UTINYINT) {
        LIST_SUB_N(pSumRes->usum, pCol, start, numOfRows, uint8_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_USMALLINT) {
        LIST_SUB_N(pSumRes->usum, pCol, start, numOfRows, uint16_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_UINT) {
        LIST_SUB_N(pSumRes->usum, pCol, start, numOfRows, uint32_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_UBIGINT) {
        LIST_SUB_N(pSumRes->usum, pCol, start, numOfRows, uint64_t, numOfElem);
      }
    } else if (type == TSDB_DATA_TYPE_DOUBLE) {
      LIST_SUB_N(pSumRes->dsum, pCol, start, numOfRows, double, numOfElem);
    } else if (type == TSDB_DATA_TYPE_FLOAT) {
      LIST_SUB_N(pSumRes->dsum, pCol, start, numOfRows, float, numOfElem);
    }
  }

  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElem, 1);
  return TSDB_CODE_SUCCESS;
}
#endif

int32_t sumCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SSumRes*             pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SSumRes*             pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);
  int16_t              type = pDBuf->type == TSDB_DATA_TYPE_NULL ? pSBuf->type : pDBuf->type;

  if (IS_SIGNED_NUMERIC_TYPE(type) || type == TSDB_DATA_TYPE_BOOL) {
    pDBuf->isum += pSBuf->isum;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    pDBuf->usum += pSBuf->usum;
  } else if (type == TSDB_DATA_TYPE_DOUBLE || type == TSDB_DATA_TYPE_FLOAT) {
    pDBuf->dsum += pSBuf->dsum;
  }
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
  return TSDB_CODE_SUCCESS;
}

bool getSumFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SSumRes);
  return true;
}

EFuncDataRequired statisDataRequired(SFunctionNode* pFunc, STimeWindow* pTimeWindow) {
  return FUNC_DATA_REQUIRED_SMA_LOAD;
}

int32_t minmaxFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (pResultInfo->initialized) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS != functionSetup(pCtx, pResultInfo)) {
    return TSDB_CODE_FUNC_SETUP_ERROR;  // not initialized since it has been initialized
  }

  SMinmaxResInfo* buf = GET_ROWCELL_INTERBUF(pResultInfo);
  buf->assign = false;
  buf->tuplePos.pageId = -1;

  buf->nullTupleSaved = false;
  buf->nullTuplePos.pageId = -1;
  buf->str = NULL;
  return TSDB_CODE_SUCCESS;
}

bool getMinmaxFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SMinmaxResInfo);
  return true;
}

int32_t minFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = 0;
  int32_t code = doMinMaxHelper(pCtx, 1, &numOfElems);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  SET_VAL(GET_RES_INFO(pCtx), numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t maxFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = 0;
  int32_t code = doMinMaxHelper(pCtx, 0, &numOfElems);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  SET_VAL(GET_RES_INFO(pCtx), numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

static int32_t setNullSelectivityValue(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t rowIndex);
static int32_t setSelectivityValue(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, const STuplePos* pTuplePos,
                                   int32_t rowIndex);

int32_t minmaxFunctionFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;

  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);
  SMinmaxResInfo*      pRes = GET_ROWCELL_INTERBUF(pEntryInfo);

  int32_t slotId = pCtx->pExpr->base.resSchema.slotId;
  int32_t currentRow = pBlock->info.rows;

  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  if (NULL == pCol) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  pEntryInfo->isNullRes = (pEntryInfo->numOfRes == 0) ? 1 : 0;

  // NOTE: do nothing change it, for performance issue
  if (!pEntryInfo->isNullRes) {
    switch (pCol->info.type) {
      case TSDB_DATA_TYPE_UBIGINT:
      case TSDB_DATA_TYPE_BIGINT:
        ((int64_t*)pCol->pData)[currentRow] = pRes->v;
        break;
      case TSDB_DATA_TYPE_UINT:
      case TSDB_DATA_TYPE_INT:
        colDataSetInt32(pCol, currentRow, (int32_t*)&pRes->v);
        break;
      case TSDB_DATA_TYPE_USMALLINT:
      case TSDB_DATA_TYPE_SMALLINT:
        colDataSetInt16(pCol, currentRow, (int16_t*)&pRes->v);
        break;
      case TSDB_DATA_TYPE_BOOL:
      case TSDB_DATA_TYPE_UTINYINT:
      case TSDB_DATA_TYPE_TINYINT:
        colDataSetInt8(pCol, currentRow, (int8_t*)&pRes->v);
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        colDataSetDouble(pCol, currentRow, (double*)&pRes->v);
        break;
      case TSDB_DATA_TYPE_FLOAT: {
        float v = GET_FLOAT_VAL(&pRes->v);
        colDataSetFloat(pCol, currentRow, &v);
        break;
      }
      case TSDB_DATA_TYPE_VARBINARY:
      case TSDB_DATA_TYPE_VARCHAR:
      case TSDB_DATA_TYPE_NCHAR: {
        code = colDataSetVal(pCol, currentRow, pRes->str, false);
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
        break;
      }
    }
  } else {
    colDataSetNULL(pCol, currentRow);
  }

  taosMemoryFreeClear(pRes->str);
  if (pCtx->subsidiaries.num > 0) {
    if (pEntryInfo->numOfRes > 0) {
      code = setSelectivityValue(pCtx, pBlock, &pRes->tuplePos, currentRow);
    } else {
      code = setSelectivityValue(pCtx, pBlock, &pRes->nullTuplePos, currentRow);
    }
  }

  return code;
}

#ifdef BUILD_NO_CALL
int32_t setNullSelectivityValue(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t rowIndex) {
  if (pCtx->subsidiaries.num <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t j = 0; j < pCtx->subsidiaries.num; ++j) {
    SqlFunctionCtx* pc = pCtx->subsidiaries.pCtx[j];
    int32_t         dstSlotId = pc->pExpr->base.resSchema.slotId;

    SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, dstSlotId);
    colDataSetNULL(pDstCol, rowIndex);
  }

  return TSDB_CODE_SUCCESS;
}
#endif

int32_t setSelectivityValue(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, const STuplePos* pTuplePos, int32_t rowIndex) {
  if (pCtx->subsidiaries.num <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  if ((pCtx->saveHandle.pBuf != NULL && pTuplePos->pageId != -1) ||
      (pCtx->saveHandle.pState && pTuplePos->streamTupleKey.ts > 0)) {
    int32_t     numOfCols = pCtx->subsidiaries.num;
    char* p = NULL;
    int32_t code = loadTupleData(pCtx, pTuplePos, &p);
    if (p == NULL || TSDB_CODE_SUCCESS != code) {
      qError("Load tuple data failed since %s, groupId:%" PRIu64 ", ts:%" PRId64, terrstr(),
             pTuplePos->streamTupleKey.groupId, pTuplePos->streamTupleKey.ts);
      return TSDB_CODE_NOT_FOUND;
    }

    bool* nullList = (bool*)p;
    char* pStart = (char*)(nullList + numOfCols * sizeof(bool));

    // todo set the offset value to optimize the performance.
    for (int32_t j = 0; j < numOfCols; ++j) {
      SqlFunctionCtx* pc = pCtx->subsidiaries.pCtx[j];
      int32_t         dstSlotId = pc->pExpr->base.resSchema.slotId;

      // group_key function has its own process function
      // do not process there
      if (fmIsGroupKeyFunc(pc->functionId)) {
        continue;
      }

      SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, dstSlotId);
      if (NULL == pDstCol) {
        return TSDB_CODE_OUT_OF_RANGE;
      }
      if (nullList[j]) {
        colDataSetNULL(pDstCol, rowIndex);
      } else {
        code = colDataSetVal(pDstCol, rowIndex, pStart, false);
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
      }
      pStart += pDstCol->info.bytes;
    }
  }

  return TSDB_CODE_SUCCESS;
}

// This function append the selectivity to subsidiaries function context directly, without fetching data
// from intermediate disk based buf page
int32_t appendSelectivityValue(SqlFunctionCtx* pCtx, int32_t rowIndex, int32_t pos) {
  if (pCtx->subsidiaries.num <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  for (int32_t j = 0; j < pCtx->subsidiaries.num; ++j) {
    SqlFunctionCtx* pc = pCtx->subsidiaries.pCtx[j];

    // get data from source col
    SFunctParam* pFuncParam = &pc->pExpr->base.pParam[0];
    int32_t      srcSlotId = pFuncParam->pCol->slotId;

    SColumnInfoData* pSrcCol = taosArrayGet(pCtx->pSrcBlock->pDataBlock, srcSlotId);
    if (NULL == pSrcCol) {
      return TSDB_CODE_OUT_OF_RANGE;
    }

    char* pData = colDataGetData(pSrcCol, rowIndex);

    // append to dest col
    int32_t dstSlotId = pc->pExpr->base.resSchema.slotId;

    SColumnInfoData* pDstCol = taosArrayGet(pCtx->pDstBlock->pDataBlock, dstSlotId);
    if (NULL == pDstCol) {
      return TSDB_CODE_OUT_OF_RANGE;
    }

    if (colDataIsNull_s(pSrcCol, rowIndex) == true) {
      colDataSetNULL(pDstCol, pos);
    } else {
      code = colDataSetVal(pDstCol, pos, pData, false);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  }
  return code;
}

void replaceTupleData(STuplePos* pDestPos, STuplePos* pSourcePos) { *pDestPos = *pSourcePos; }

#define COMPARE_MINMAX_DATA(type) (( (*(type*)&pDBuf->v) < (*(type*)&pSBuf->v) ) ^ isMinFunc)
int32_t minMaxCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx, int32_t isMinFunc) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SMinmaxResInfo*      pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SMinmaxResInfo*      pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);
  int16_t              type = pDBuf->type == TSDB_DATA_TYPE_NULL ? pSBuf->type : pDBuf->type;

  switch (type) {
    case TSDB_DATA_TYPE_DOUBLE:
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_BIGINT:
      if (pSBuf->assign && (COMPARE_MINMAX_DATA(int64_t) || !pDBuf->assign)) {
        pDBuf->v = pSBuf->v;
        replaceTupleData(&pDBuf->tuplePos, &pSBuf->tuplePos);
        pDBuf->assign = true;
      }
      break;
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_INT:
      if (pSBuf->assign && (COMPARE_MINMAX_DATA(int32_t) || !pDBuf->assign)) {
        pDBuf->v = pSBuf->v;
        replaceTupleData(&pDBuf->tuplePos, &pSBuf->tuplePos);
        pDBuf->assign = true;
      }
      break;
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_SMALLINT:
      if (pSBuf->assign && (COMPARE_MINMAX_DATA(int16_t) || !pDBuf->assign)) {
        pDBuf->v = pSBuf->v;
        replaceTupleData(&pDBuf->tuplePos, &pSBuf->tuplePos);
        pDBuf->assign = true;
      }
      break;
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_TINYINT:
      if (pSBuf->assign && (COMPARE_MINMAX_DATA(int8_t) || !pDBuf->assign)) {
        pDBuf->v = pSBuf->v;
        replaceTupleData(&pDBuf->tuplePos, &pSBuf->tuplePos);
        pDBuf->assign = true;
      }
      break;
    case TSDB_DATA_TYPE_FLOAT: {
      if (pSBuf->assign && (COMPARE_MINMAX_DATA(double) || !pDBuf->assign)) {
        pDBuf->v = pSBuf->v;
        replaceTupleData(&pDBuf->tuplePos, &pSBuf->tuplePos);
        pDBuf->assign = true;
      }
      break;
    }
    default:
      if (pSBuf->assign && (strcmp((char*)&pDBuf->v, (char*)&pSBuf->v) || !pDBuf->assign)) {
        pDBuf->v = pSBuf->v;
        replaceTupleData(&pDBuf->tuplePos, &pSBuf->tuplePos);
        pDBuf->assign = true;
      }
      break;
  }
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
  return TSDB_CODE_SUCCESS;
}

int32_t minCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  return minMaxCombine(pDestCtx, pSourceCtx, 1);
}
int32_t maxCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  return minMaxCombine(pDestCtx, pSourceCtx, 0);
}

int32_t getStdInfoSize() { return (int32_t)sizeof(SStdRes); }

bool getStdFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SStdRes);
  return true;
}

int32_t stdFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (pResultInfo->initialized) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS != functionSetup(pCtx, pResultInfo)) {
    return TSDB_CODE_FUNC_SETUP_ERROR;
  }

  SStdRes* pRes = GET_ROWCELL_INTERBUF(pResultInfo);
  (void)memset(pRes, 0, sizeof(SStdRes));
  return TSDB_CODE_SUCCESS;
}

int32_t stdFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  int32_t               type = pInput->pData[0]->info.type;

  SStdRes* pStdRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  pStdRes->type = type;

  // computing based on the true data block
  SColumnInfoData* pCol = pInput->pData[0];

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;

  if (IS_NULL_TYPE(type)) {
    numOfElem = 0;
    goto _stddev_over;
  }

  switch (type) {
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t* plist = (int8_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + start; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStdRes->count += 1;
        pStdRes->isum += plist[i];
        pStdRes->quadraticISum += plist[i] * plist[i];
      }

      break;
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t* plist = (int16_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStdRes->count += 1;
        pStdRes->isum += plist[i];
        pStdRes->quadraticISum += plist[i] * plist[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_INT: {
      int32_t* plist = (int32_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStdRes->count += 1;
        pStdRes->isum += plist[i];
        pStdRes->quadraticISum += plist[i] * plist[i];
      }

      break;
    }

    case TSDB_DATA_TYPE_BIGINT: {
      int64_t* plist = (int64_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStdRes->count += 1;
        pStdRes->isum += plist[i];
        pStdRes->quadraticISum += plist[i] * plist[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_UTINYINT: {
      uint8_t* plist = (uint8_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + start; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStdRes->count += 1;
        pStdRes->usum += plist[i];
        pStdRes->quadraticUSum += plist[i] * plist[i];
      }

      break;
    }

    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t* plist = (uint16_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStdRes->count += 1;
        pStdRes->usum += plist[i];
        pStdRes->quadraticUSum += plist[i] * plist[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_UINT: {
      uint32_t* plist = (uint32_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStdRes->count += 1;
        pStdRes->usum += plist[i];
        pStdRes->quadraticUSum += plist[i] * plist[i];
      }

      break;
    }

    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t* plist = (uint64_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStdRes->count += 1;
        pStdRes->usum += plist[i];
        pStdRes->quadraticUSum += plist[i] * plist[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_FLOAT: {
      float* plist = (float*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStdRes->count += 1;
        pStdRes->dsum += plist[i];
        pStdRes->quadraticDSum += plist[i] * plist[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double* plist = (double*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStdRes->count += 1;
        pStdRes->dsum += plist[i];
        pStdRes->quadraticDSum += plist[i] * plist[i];
      }
      break;
    }

    default:
      break;
  }

_stddev_over:
  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElem, 1);
  return TSDB_CODE_SUCCESS;
}

static void stdTransferInfo(SStdRes* pInput, SStdRes* pOutput) {
  if (IS_NULL_TYPE(pInput->type)) {
    return;
  }
  pOutput->type = pInput->type;
  if (IS_SIGNED_NUMERIC_TYPE(pOutput->type)) {
    pOutput->quadraticISum += pInput->quadraticISum;
    pOutput->isum += pInput->isum;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(pOutput->type)) {
    pOutput->quadraticUSum += pInput->quadraticUSum;
    pOutput->usum += pInput->usum;
  } else {
    pOutput->quadraticDSum += pInput->quadraticDSum;
    pOutput->dsum += pInput->dsum;
  }

  pOutput->count += pInput->count;
}

int32_t stdFunctionMerge(SqlFunctionCtx* pCtx) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];

  if (IS_NULL_TYPE(pCol->info.type)) {
    SET_VAL(GET_RES_INFO(pCtx), 0, 1);
    return TSDB_CODE_SUCCESS;
  }

  if (pCol->info.type != TSDB_DATA_TYPE_BINARY) {
    return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }

  SStdRes* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  for (int32_t i = pInput->startRowIndex; i < pInput->startRowIndex + pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pCol, i)) continue;
    char*       data = colDataGetData(pCol, i);
    SStdRes*    pInputInfo = (SStdRes*)varDataVal(data);
    stdTransferInfo(pInputInfo, pInfo);
  }

  SET_VAL(GET_RES_INFO(pCtx), 1, 1);
  return TSDB_CODE_SUCCESS;
}

#ifdef BUILD_NO_CALL
int32_t stdInvertFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  int32_t               type = pInput->pData[0]->info.type;

  SStdRes* pStdRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  // computing based on the true data block
  SColumnInfoData* pCol = pInput->pData[0];

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;

  switch (type) {
    case TSDB_DATA_TYPE_TINYINT: {
      LIST_STDDEV_SUB_N(pStdRes->isum, int8_t);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      LIST_STDDEV_SUB_N(pStdRes->isum, int16_t);
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      LIST_STDDEV_SUB_N(pStdRes->isum, int32_t);
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      LIST_STDDEV_SUB_N(pStdRes->isum, int64_t);
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      LIST_STDDEV_SUB_N(pStdRes->isum, uint8_t);
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      LIST_STDDEV_SUB_N(pStdRes->isum, uint16_t);
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      LIST_STDDEV_SUB_N(pStdRes->isum, uint32_t);
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      LIST_STDDEV_SUB_N(pStdRes->isum, uint64_t);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      LIST_STDDEV_SUB_N(pStdRes->dsum, float);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      LIST_STDDEV_SUB_N(pStdRes->dsum, double);
      break;
    }
    default:
      break;
  }

  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElem, 1);
  return TSDB_CODE_SUCCESS;
}
#endif

int32_t stddevFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SStdRes*              pStddevRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t               type = pStddevRes->type;
  double                avg;

  if (pStddevRes->count == 0) {
    GET_RES_INFO(pCtx)->numOfRes = 0;
    return functionFinalize(pCtx, pBlock);
  }

  if (IS_SIGNED_NUMERIC_TYPE(type)) {
    avg = pStddevRes->isum / ((double)pStddevRes->count);
    pStddevRes->result = sqrt(fabs(pStddevRes->quadraticISum / ((double)pStddevRes->count) - avg * avg));
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    avg = pStddevRes->usum / ((double)pStddevRes->count);
    pStddevRes->result = sqrt(fabs(pStddevRes->quadraticUSum / ((double)pStddevRes->count) - avg * avg));
  } else {
    avg = pStddevRes->dsum / ((double)pStddevRes->count);
    pStddevRes->result = sqrt(fabs(pStddevRes->quadraticDSum / ((double)pStddevRes->count) - avg * avg));
  }

  // check for overflow
  if (isinf(pStddevRes->result) || isnan(pStddevRes->result)) {
    GET_RES_INFO(pCtx)->numOfRes = 0;
  }

  return functionFinalize(pCtx, pBlock);
}

int32_t stdvarFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SStdRes*              pStdvarRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t               type = pStdvarRes->type;
  double                avg;

  if (pStdvarRes->count == 0) {
    GET_RES_INFO(pCtx)->numOfRes = 0;
    return functionFinalize(pCtx, pBlock);
  }

  if (IS_SIGNED_NUMERIC_TYPE(type)) {
    avg = pStdvarRes->isum / ((double)pStdvarRes->count);
    pStdvarRes->result = fabs(pStdvarRes->quadraticISum / ((double)pStdvarRes->count) - avg * avg);
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    avg = pStdvarRes->usum / ((double)pStdvarRes->count);
    pStdvarRes->result = fabs(pStdvarRes->quadraticUSum / ((double)pStdvarRes->count) - avg * avg);
  } else {
    avg = pStdvarRes->dsum / ((double)pStdvarRes->count);
    pStdvarRes->result = fabs(pStdvarRes->quadraticDSum / ((double)pStdvarRes->count) - avg * avg);
  }

  // check for overflow
  if (isinf(pStdvarRes->result) || isnan(pStdvarRes->result)) {
    GET_RES_INFO(pCtx)->numOfRes = 0;
  }

  return functionFinalize(pCtx, pBlock);
}

int32_t stdPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SStdRes*             pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t              resultBytes = getStdInfoSize();
  char*                res = taosMemoryCalloc(resultBytes + VARSTR_HEADER_SIZE, sizeof(char));

  if (NULL == res) {
    return terrno;
  }
  (void)memcpy(varDataVal(res), pInfo, resultBytes);
  varDataSetLen(res, resultBytes);

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  if (NULL == pCol) {
    taosMemoryFree(res);
    return TSDB_CODE_OUT_OF_RANGE;
  }

  int32_t code = colDataSetVal(pCol, pBlock->info.rows, res, false);

  taosMemoryFree(res);
  return code;
}

int32_t stdCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SStdRes*             pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SStdRes*             pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);
  int16_t              type = pDBuf->type == TSDB_DATA_TYPE_NULL ? pSBuf->type : pDBuf->type;

  stdTransferInfo(pSBuf, pDBuf);

  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
  return TSDB_CODE_SUCCESS;
}

bool getLeastSQRFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SLeastSQRInfo);
  return true;
}

int32_t leastSQRFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (pResultInfo->initialized) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS != functionSetup(pCtx, pResultInfo)) {
    return TSDB_CODE_FUNC_SETUP_ERROR;
  }

  SLeastSQRInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);

  GET_TYPED_DATA(pInfo->startVal, double, pCtx->param[1].param.nType, &pCtx->param[1].param.i);
  GET_TYPED_DATA(pInfo->stepVal, double, pCtx->param[2].param.nType, &pCtx->param[2].param.i);
  return TSDB_CODE_SUCCESS;
}

int32_t leastSQRFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = 0;

  SInputColumnInfoData* pInput = &pCtx->input;
  int32_t               type = pInput->pData[0]->info.type;

  SLeastSQRInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  SColumnInfoData* pCol = pInput->pData[0];

  double(*param)[3] = pInfo->matrix;
  double x = pInfo->startVal;

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;

  switch (type) {
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t* plist = (int8_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }
        numOfElem++;
        LEASTSQR_CAL(param, x, plist, i, pInfo->stepVal);
      }
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t* plist = (int16_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem++;
        LEASTSQR_CAL(param, x, plist, i, pInfo->stepVal);
      }
      break;
    }

    case TSDB_DATA_TYPE_INT: {
      int32_t* plist = (int32_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem++;
        LEASTSQR_CAL(param, x, plist, i, pInfo->stepVal);
      }
      break;
    }

    case TSDB_DATA_TYPE_BIGINT: {
      int64_t* plist = (int64_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem++;
        LEASTSQR_CAL(param, x, plist, i, pInfo->stepVal);
      }
      break;
    }

    case TSDB_DATA_TYPE_UTINYINT: {
      uint8_t* plist = (uint8_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }
        numOfElem++;
        LEASTSQR_CAL(param, x, plist, i, pInfo->stepVal);
      }
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t* plist = (uint16_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem++;
        LEASTSQR_CAL(param, x, plist, i, pInfo->stepVal);
      }
      break;
    }

    case TSDB_DATA_TYPE_UINT: {
      uint32_t* plist = (uint32_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem++;
        LEASTSQR_CAL(param, x, plist, i, pInfo->stepVal);
      }
      break;
    }

    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t* plist = (uint64_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem++;
        LEASTSQR_CAL(param, x, plist, i, pInfo->stepVal);
      }
      break;
    }

    case TSDB_DATA_TYPE_FLOAT: {
      float* plist = (float*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem++;
        LEASTSQR_CAL(param, x, plist, i, pInfo->stepVal);
      }
      break;
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double* plist = (double*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem++;
        LEASTSQR_CAL(param, x, plist, i, pInfo->stepVal);
      }
      break;
    }
    case TSDB_DATA_TYPE_NULL: {
      GET_RES_INFO(pCtx)->isNullRes = 1;
      numOfElem = 1;
      break;
    }

    default:
      break;
  }

  pInfo->startVal = x;
  pInfo->num += numOfElem;

  SET_VAL(GET_RES_INFO(pCtx), numOfElem, 1);

  return TSDB_CODE_SUCCESS;
}

int32_t leastSQRFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SLeastSQRInfo*       pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t              slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData*     pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  if (NULL == pCol) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  int32_t currentRow = pBlock->info.rows;

  if (0 == pInfo->num) {
    colDataSetNULL(pCol, currentRow);
    return TSDB_CODE_SUCCESS;
  }

  double(*param)[3] = pInfo->matrix;

  param[1][1] = (double)pInfo->num;
  param[1][0] = param[0][1];

  double param00 = param[0][0] - param[1][0] * (param[0][1] / param[1][1]);
  double param02 = param[0][2] - param[1][2] * (param[0][1] / param[1][1]);

  if (0 == param00) {
    colDataSetNULL(pCol, currentRow);
    return TSDB_CODE_SUCCESS;
  }

  // param[0][1] = 0;
  double param12 = param[1][2] - param02 * (param[1][0] / param00);
  // param[1][0] = 0;
  param02 /= param00;

  param12 /= param[1][1];

  char buf[LEASTSQUARES_BUFF_LENGTH] = {0};
  char slopBuf[64] = {0};
  char interceptBuf[64] = {0};
  int  n = tsnprintf(slopBuf, 64, "%.6lf", param02);
  if (n > LEASTSQUARES_DOUBLE_ITEM_LENGTH) {
    (void)snprintf(slopBuf, 64, "%." DOUBLE_PRECISION_DIGITS, param02);
  }
  n = tsnprintf(interceptBuf, 64, "%.6lf", param12);
  if (n > LEASTSQUARES_DOUBLE_ITEM_LENGTH) {
    (void)snprintf(interceptBuf, 64, "%." DOUBLE_PRECISION_DIGITS, param12);
  }
  size_t len =
      snprintf(varDataVal(buf), sizeof(buf) - VARSTR_HEADER_SIZE, "{slop:%s, intercept:%s}", slopBuf, interceptBuf);
  varDataSetLen(buf, len);

  int32_t code = colDataSetVal(pCol, currentRow, buf, pResInfo->isNullRes);

  return code;
}

int32_t leastSQRCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SLeastSQRInfo*       pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);
  int32_t              type = pDestCtx->input.pData[0]->info.type;
  double(*pDparam)[3] = pDBuf->matrix;

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SLeastSQRInfo*       pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);
  double(*pSparam)[3] = pSBuf->matrix;
  for (int32_t i = 0; i < pSBuf->num; i++) {
    pDparam[0][0] += pDBuf->startVal * pDBuf->startVal;
    pDparam[0][1] += pDBuf->startVal;
    pDBuf->startVal += pDBuf->stepVal;
  }
  pDparam[0][2] += pSparam[0][2] + pDBuf->num * pDBuf->stepVal * pSparam[1][2];
  pDparam[1][2] += pSparam[1][2];
  pDBuf->num += pSBuf->num;
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
  return TSDB_CODE_SUCCESS;
}

bool getPercentileFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SPercentileInfo);
  return true;
}

int32_t percentileFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (pResultInfo->initialized) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS != functionSetup(pCtx, pResultInfo)) {
    return TSDB_CODE_FUNC_SETUP_ERROR;
  }

  // in the first round, get the min-max value of all involved data
  SPercentileInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  SET_DOUBLE_VAL(&pInfo->minval, DBL_MAX);
  SET_DOUBLE_VAL(&pInfo->maxval, -DBL_MAX);
  pInfo->numOfElems = 0;

  return TSDB_CODE_SUCCESS;
}

void percentileFunctionCleanupExt(SqlFunctionCtx* pCtx) {
  if (pCtx == NULL || GET_RES_INFO(pCtx) == NULL || GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx)) == NULL) {
    return;
  }
  SPercentileInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  if (pInfo->pMemBucket != NULL) {
    tMemBucketDestroy(&(pInfo->pMemBucket));
    pInfo->pMemBucket = NULL;
  }
}

int32_t percentileFunction(SqlFunctionCtx* pCtx) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              numOfElems = 0;
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];

  SColumnInfoData* pCol = pInput->pData[0];
  int32_t          type = pCol->info.type;

  SPercentileInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  if (pCtx->scanFlag == MAIN_SCAN && pInfo->stage == 0) {
    pInfo->stage += 1;

    // all data are null, set it completed
    if (pInfo->numOfElems == 0) {
      pResInfo->complete = true;
      return TSDB_CODE_SUCCESS;
    } else {
      code = tMemBucketCreate(pCol->info.bytes, type, pInfo->minval, pInfo->maxval, pCtx->hasWindowOrGroup, &pInfo->pMemBucket, pInfo->numOfElems);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  }

  // the first stage, only acquire the min/max value
  if (pInfo->stage == 0) {
    if (pCtx->input.colDataSMAIsSet) {
      double tmin = 0.0, tmax = 0.0;
      if (IS_SIGNED_NUMERIC_TYPE(type)) {
        tmin = (double)GET_INT64_VAL(&pAgg->min);
        tmax = (double)GET_INT64_VAL(&pAgg->max);
      } else if (IS_FLOAT_TYPE(type)) {
        tmin = GET_DOUBLE_VAL(&pAgg->min);
        tmax = GET_DOUBLE_VAL(&pAgg->max);
      } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
        tmin = (double)GET_UINT64_VAL(&pAgg->min);
        tmax = (double)GET_UINT64_VAL(&pAgg->max);
      }

      if (GET_DOUBLE_VAL(&pInfo->minval) > tmin) {
        SET_DOUBLE_VAL(&pInfo->minval, tmin);
      }

      if (GET_DOUBLE_VAL(&pInfo->maxval) < tmax) {
        SET_DOUBLE_VAL(&pInfo->maxval, tmax);
      }

      pInfo->numOfElems += (pInput->numOfRows - pAgg->numOfNull);
    } else {
      // check the valid data one by one
      int32_t start = pInput->startRowIndex;
      for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
        if (colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        char* data = colDataGetData(pCol, i);

        double v = 0;
        GET_TYPED_DATA(v, double, type, data);
        if (v < GET_DOUBLE_VAL(&pInfo->minval)) {
          SET_DOUBLE_VAL(&pInfo->minval, v);
        }

        if (v > GET_DOUBLE_VAL(&pInfo->maxval)) {
          SET_DOUBLE_VAL(&pInfo->maxval, v);
        }

        pInfo->numOfElems += 1;
      }
    }
  } else {
    // the second stage, calculate the true percentile value
    int32_t start = pInput->startRowIndex;
    for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
      if (colDataIsNull_f(pCol->nullbitmap, i)) {
        continue;
      }

      char* data = colDataGetData(pCol, i);
      numOfElems += 1;
      code = tMemBucketPut(pInfo->pMemBucket, data, 1);
      if (code != TSDB_CODE_SUCCESS) {
        tMemBucketDestroy(&(pInfo->pMemBucket));
        return code;
      }
    }

    SET_VAL(pResInfo, numOfElems, 1);
  }

  pCtx->needCleanup = true;
  return TSDB_CODE_SUCCESS;
}

int32_t percentileFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SPercentileInfo*     ppInfo = (SPercentileInfo*)GET_ROWCELL_INTERBUF(pResInfo);

  int32_t code = 0;
  double  v = 0;

  tMemBucket** pMemBucket = &ppInfo->pMemBucket;
  if ((*pMemBucket) != NULL && (*pMemBucket)->total > 0) {  // check for null
    if (pCtx->numOfParams > 2) {
      char   buf[3200] = {0};
      // max length of double num is 317, e.g. use %.6lf to print -1.0e+308, consider the comma and bracket, 3200 is enough.
      size_t len = 1;

      varDataVal(buf)[0] = '[';
      for (int32_t i = 1; i < pCtx->numOfParams; ++i) {
        SVariant* pVal = &pCtx->param[i].param;

        GET_TYPED_DATA(v, double, pVal->nType, &pVal->i);

        code = getPercentile((*pMemBucket), v, &ppInfo->result);
        if (code != TSDB_CODE_SUCCESS) {
          goto _fin_error;
        }

        if (i == pCtx->numOfParams - 1) {
          len += tsnprintf(varDataVal(buf) + len, sizeof(buf) - VARSTR_HEADER_SIZE - len, "%.6lf]", ppInfo->result);
        } else {
          len += tsnprintf(varDataVal(buf) + len, sizeof(buf) - VARSTR_HEADER_SIZE - len, "%.6lf, ", ppInfo->result);
        }
      }

      int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
      SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
      if (NULL == pCol) {
        code = terrno;
        goto _fin_error;
      }

      varDataSetLen(buf, len);
      code = colDataSetVal(pCol, pBlock->info.rows, buf, false);
      if (code != TSDB_CODE_SUCCESS) {
        goto _fin_error;
      }

      tMemBucketDestroy(pMemBucket);
      return TSDB_CODE_SUCCESS;
    } else {
      SVariant* pVal = &pCtx->param[1].param;

      GET_TYPED_DATA(v, double, pVal->nType, &pVal->i);

      code = getPercentile((*pMemBucket), v, &ppInfo->result);
      if (code != TSDB_CODE_SUCCESS) {
        goto _fin_error;
      }

      tMemBucketDestroy(pMemBucket);
      return functionFinalize(pCtx, pBlock);
    }
  } else {
    return functionFinalize(pCtx, pBlock);
  }

_fin_error:

  tMemBucketDestroy(pMemBucket);
  return code;
}

bool getApercentileFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  int32_t bytesHist =
      (int32_t)(sizeof(SAPercentileInfo) + sizeof(SHistogramInfo) + sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1));
  int32_t bytesDigest = (int32_t)(sizeof(SAPercentileInfo) + TDIGEST_SIZE(COMPRESSION));
  pEnv->calcMemSize = TMAX(bytesHist, bytesDigest);
  return true;
}

int32_t getApercentileMaxSize() {
  int32_t bytesHist =
      (int32_t)(sizeof(SAPercentileInfo) + sizeof(SHistogramInfo) + sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1));
  int32_t bytesDigest = (int32_t)(sizeof(SAPercentileInfo) + TDIGEST_SIZE(COMPRESSION));
  return TMAX(bytesHist, bytesDigest);
}

static int8_t getApercentileAlgo(char* algoStr) {
  int8_t algoType;
  if (strcasecmp(algoStr, "default") == 0) {
    algoType = APERCT_ALGO_DEFAULT;
  } else if (strcasecmp(algoStr, "t-digest") == 0) {
    algoType = APERCT_ALGO_TDIGEST;
  } else {
    algoType = APERCT_ALGO_UNKNOWN;
  }

  return algoType;
}

static void buildHistogramInfo(SAPercentileInfo* pInfo) {
  pInfo->pHisto = (SHistogramInfo*)((char*)pInfo + sizeof(SAPercentileInfo));
  pInfo->pHisto->elems = (SHistBin*)((char*)pInfo->pHisto + sizeof(SHistogramInfo));
}

static void buildTDigestInfo(SAPercentileInfo* pInfo) {
  pInfo->pTDigest = (TDigest*)((char*)pInfo + sizeof(SAPercentileInfo));
}

int32_t apercentileFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (pResultInfo->initialized) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS != functionSetup(pCtx, pResultInfo)) {
    return TSDB_CODE_FUNC_SETUP_ERROR;
  }

  SAPercentileInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);

  SVariant* pVal = &pCtx->param[1].param;
  pInfo->percent = 0;
  GET_TYPED_DATA(pInfo->percent, double, pVal->nType, &pVal->i);

  if (pCtx->numOfParams == 2) {
    pInfo->algo = APERCT_ALGO_DEFAULT;
  } else if (pCtx->numOfParams == 3) {
    pInfo->algo = getApercentileAlgo(varDataVal(pCtx->param[2].param.pz));
    if (pInfo->algo == APERCT_ALGO_UNKNOWN) {
      return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    }
  }

  char* tmp = (char*)pInfo + sizeof(SAPercentileInfo);
  if (pInfo->algo == APERCT_ALGO_TDIGEST) {
    pInfo->pTDigest = tdigestNewFrom(tmp, COMPRESSION);
  } else {
    buildHistogramInfo(pInfo);
    pInfo->pHisto = tHistogramCreateFrom(tmp, MAX_HISTOGRAM_BIN);
    qDebug("%s set up histogram, numOfElems:%" PRId64 ", numOfEntry:%d, pHisto:%p, elems:%p", __FUNCTION__,
           pInfo->pHisto->numOfElems, pInfo->pHisto->numOfEntries, pInfo->pHisto, pInfo->pHisto->elems);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t apercentileFunction(SqlFunctionCtx* pCtx) {
  int32_t               numOfElems = 0;
  SResultRowEntryInfo*  pResInfo = GET_RES_INFO(pCtx);
  SInputColumnInfoData* pInput = &pCtx->input;

  SColumnInfoData* pCol = pInput->pData[0];
  int32_t          type = pCol->info.type;

  SAPercentileInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  int32_t start = pInput->startRowIndex;
  if (pInfo->algo == APERCT_ALGO_TDIGEST) {
    buildTDigestInfo(pInfo);
    tdigestAutoFill(pInfo->pTDigest, COMPRESSION);
    for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
      if (colDataIsNull_f(pCol->nullbitmap, i)) {
        continue;
      }
      numOfElems += 1;
      char* data = colDataGetData(pCol, i);

      double  v = 0;  // value
      int64_t w = 1;  // weigth
      GET_TYPED_DATA(v, double, type, data);
      int32_t code = tdigestAdd(pInfo->pTDigest, v, w);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  } else {
    // might be a race condition here that pHisto can be overwritten or setup function
    // has not been called, need to relink the buffer pHisto points to.
    buildHistogramInfo(pInfo);
    qDebug("%s before add %d elements into histogram, total:%" PRId64 ", numOfEntry:%d, pHisto:%p, elems: %p",
           __FUNCTION__, numOfElems, pInfo->pHisto->numOfElems, pInfo->pHisto->numOfEntries, pInfo->pHisto,
           pInfo->pHisto->elems);
    for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
      if (colDataIsNull_f(pCol->nullbitmap, i)) {
        continue;
      }
      numOfElems += 1;
      char* data = colDataGetData(pCol, i);

      double v = 0;
      GET_TYPED_DATA(v, double, type, data);
      int32_t code = tHistogramAdd(&pInfo->pHisto, v);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    qDebug("%s after add %d elements into histogram, total:%" PRId64 ", numOfEntry:%d, pHisto:%p, elems: %p",
           __FUNCTION__, numOfElems, pInfo->pHisto->numOfElems, pInfo->pHisto->numOfEntries, pInfo->pHisto,
           pInfo->pHisto->elems);
  }

  SET_VAL(pResInfo, numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

static int32_t apercentileTransferInfo(SAPercentileInfo* pInput, SAPercentileInfo* pOutput, bool* hasRes) {
  pOutput->percent = pInput->percent;
  pOutput->algo = pInput->algo;
  if (pOutput->algo == APERCT_ALGO_TDIGEST) {
    buildTDigestInfo(pInput);
    tdigestAutoFill(pInput->pTDigest, COMPRESSION);

    if (pInput->pTDigest->num_centroids == 0 && pInput->pTDigest->num_buffered_pts == 0) {
      return TSDB_CODE_SUCCESS;
    }

    if (hasRes) {
      *hasRes = true;
    }

    buildTDigestInfo(pOutput);
    TDigest* pTDigest = pOutput->pTDigest;
    tdigestAutoFill(pTDigest, COMPRESSION);

    if (pTDigest->num_centroids <= 0 && pTDigest->num_buffered_pts == 0) {
      (void)memcpy(pTDigest, pInput->pTDigest, (size_t)TDIGEST_SIZE(COMPRESSION));
      tdigestAutoFill(pTDigest, COMPRESSION);
    } else {
      int32_t code = tdigestMerge(pTDigest, pInput->pTDigest);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  } else {
    buildHistogramInfo(pInput);
    if (pInput->pHisto->numOfElems <= 0) {
      return TSDB_CODE_SUCCESS;
    }

    if (hasRes) {
      *hasRes = true;
    }

    buildHistogramInfo(pOutput);
    SHistogramInfo* pHisto = pOutput->pHisto;

    if (pHisto->numOfElems <= 0) {
      (void)memcpy(pHisto, pInput->pHisto, sizeof(SHistogramInfo) + sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1));
      pHisto->elems = (SHistBin*)((char*)pHisto + sizeof(SHistogramInfo));

      qDebug("%s merge histo, total:%" PRId64 ", entry:%d, %p", __FUNCTION__, pHisto->numOfElems, pHisto->numOfEntries,
             pHisto);
    } else {
      pHisto->elems = (SHistBin*)((char*)pHisto + sizeof(SHistogramInfo));
      qDebug("%s input histogram, elem:%" PRId64 ", entry:%d, %p", __FUNCTION__, pHisto->numOfElems,
             pHisto->numOfEntries, pInput->pHisto);

      SHistogramInfo* pRes = NULL;
      int32_t code = tHistogramMerge(pHisto, pInput->pHisto, MAX_HISTOGRAM_BIN, &pRes);
      if (TSDB_CODE_SUCCESS != code) {
        tHistogramDestroy(&pRes);
        return code;
      }
      (void)memcpy(pHisto, pRes, sizeof(SHistogramInfo) + sizeof(SHistBin) * MAX_HISTOGRAM_BIN);
      pHisto->elems = (SHistBin*)((char*)pHisto + sizeof(SHistogramInfo));

      qDebug("%s merge histo, total:%" PRId64 ", entry:%d, %p", __FUNCTION__, pHisto->numOfElems, pHisto->numOfEntries,
             pHisto);
      tHistogramDestroy(&pRes);
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t apercentileFunctionMerge(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  SInputColumnInfoData* pInput = &pCtx->input;

  SColumnInfoData* pCol = pInput->pData[0];
  if (pCol->info.type != TSDB_DATA_TYPE_BINARY) {
    return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }

  SAPercentileInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  qDebug("%s total %" PRId64 " rows will merge, %p", __FUNCTION__, pInput->numOfRows, pInfo->pHisto);

  bool hasRes = false;
  int32_t start = pInput->startRowIndex;
  for (int32_t i = start; i < start + pInput->numOfRows; ++i) {
    char* data = colDataGetData(pCol, i);

    SAPercentileInfo* pInputInfo = (SAPercentileInfo*)varDataVal(data);
    int32_t code = apercentileTransferInfo(pInputInfo, pInfo, &hasRes);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  if (pInfo->algo != APERCT_ALGO_TDIGEST) {
    buildHistogramInfo(pInfo);
    qDebug("%s after merge, total:%" PRId64 ", numOfEntry:%d, %p", __FUNCTION__, pInfo->pHisto->numOfElems,
           pInfo->pHisto->numOfEntries, pInfo->pHisto);
  }

  SET_VAL(pResInfo, hasRes ? 1 : 0, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t apercentileFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SAPercentileInfo*    pInfo = (SAPercentileInfo*)GET_ROWCELL_INTERBUF(pResInfo);

  if (pInfo->algo == APERCT_ALGO_TDIGEST) {
    buildTDigestInfo(pInfo);
    tdigestAutoFill(pInfo->pTDigest, COMPRESSION);
    if (pInfo->pTDigest->size > 0) {
      pInfo->result = tdigestQuantile(pInfo->pTDigest, pInfo->percent / 100);
    } else {  // no need to free
      // setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
      return TSDB_CODE_SUCCESS;
    }
  } else {
    buildHistogramInfo(pInfo);
    if (pInfo->pHisto->numOfElems > 0) {
      qDebug("%s get the final res, elements:%" PRId64 ", numOfEntry:%d, pHisto:%p, elems:%p", __FUNCTION__,
             pInfo->pHisto->numOfElems, pInfo->pHisto->numOfEntries, pInfo->pHisto, pInfo->pHisto->elems);

      double  ratio[] = {pInfo->percent};
      double* res = NULL;
      int32_t code = tHistogramUniform(pInfo->pHisto, ratio, 1, &res);
      if (TSDB_CODE_SUCCESS != code) {
        taosMemoryFree(res);
        return code;
      }
      pInfo->result = *res;
      // memcpy(pCtx->pOutput, res, sizeof(double));
      taosMemoryFree(res);
    } else {  // no need to free
      // setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
      // return TSDB_CODE_SUCCESS;
      qDebug("%s get the final res, elements:%" PRId64 ", numOfEntry:%d. result is null", __FUNCTION__,
             pInfo->pHisto->numOfElems, pInfo->pHisto->numOfEntries);
    }
  }

  return functionFinalize(pCtx, pBlock);
}

int32_t apercentilePartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SAPercentileInfo*    pInfo = (SAPercentileInfo*)GET_ROWCELL_INTERBUF(pResInfo);

  int32_t resultBytes = getApercentileMaxSize();
  char*   res = taosMemoryCalloc(resultBytes + VARSTR_HEADER_SIZE, sizeof(char));
  if (NULL == res) {
    return terrno;
  }

  if (pInfo->algo == APERCT_ALGO_TDIGEST) {
    (void)memcpy(varDataVal(res), pInfo, resultBytes);
    varDataSetLen(res, resultBytes);
  } else {
    (void)memcpy(varDataVal(res), pInfo, resultBytes);
    varDataSetLen(res, resultBytes);
  }

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  if (NULL == pCol) {
    taosMemoryFree(res);
    return TSDB_CODE_OUT_OF_RANGE;
  }

  int32_t code = colDataSetVal(pCol, pBlock->info.rows, res, false);

  taosMemoryFree(res);
  return code;
}

int32_t apercentileCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SAPercentileInfo*    pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SAPercentileInfo*    pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  qDebug("%s start to combine apercentile, %p", __FUNCTION__, pDBuf->pHisto);

  int32_t code = apercentileTransferInfo(pSBuf, pDBuf, NULL);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
  return TSDB_CODE_SUCCESS;
}

// TODO: change this function when block data info pks changed
static int32_t comparePkDataWithSValue(int8_t pkType, char* pkData, SValue* pVal, int32_t order) {
  char numVal[8] = {0};
  switch (pkType) {
    case TSDB_DATA_TYPE_INT:
      *(int32_t*)numVal = (int32_t)pVal->val;
      break;
    case TSDB_DATA_TYPE_UINT:
      *(uint32_t*)numVal = (uint32_t)pVal->val;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      *(int64_t*)numVal = (int64_t)pVal->val;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      *(uint64_t*)numVal = (uint64_t)pVal->val;
      break;
    default:
      break;
  }
  char*         blockData = (IS_NUMERIC_TYPE(pkType)) ? (char*) numVal : (char*)pVal->pData;
  __compar_fn_t fn = getKeyComparFunc(pkType, order);
  return fn(pkData, blockData);
}

EFuncDataRequired firstDynDataReq(void* pRes, SDataBlockInfo* pBlockInfo) {
  SResultRowEntryInfo* pEntry = (SResultRowEntryInfo*)pRes;

  // not initialized yet, data is required
  if (pEntry == NULL) {
    return FUNC_DATA_REQUIRED_DATA_LOAD;
  }

  SFirstLastRes* pResult = GET_ROWCELL_INTERBUF(pEntry);
  if (pResult->hasResult) {
    if (pResult->pkBytes > 0) {
      pResult->pkData = pResult->buf + pResult->bytes;
    } else {
      pResult->pkData = NULL;
    }    
    if (pResult->ts < pBlockInfo->window.skey) {
      return FUNC_DATA_REQUIRED_NOT_LOAD;
    } else if (pResult->ts == pBlockInfo->window.skey) {
      if (NULL == pResult->pkData) {
        return FUNC_DATA_REQUIRED_NOT_LOAD;
      }
      if (comparePkDataWithSValue(pResult->pkType, pResult->pkData, pBlockInfo->pks + 0, TSDB_ORDER_ASC) < 0) {
        return FUNC_DATA_REQUIRED_NOT_LOAD;
      }
    }
    return FUNC_DATA_REQUIRED_DATA_LOAD;
  } else {
    return FUNC_DATA_REQUIRED_DATA_LOAD;
  }
}

EFuncDataRequired lastDynDataReq(void* pRes, SDataBlockInfo* pBlockInfo) {
  SResultRowEntryInfo* pEntry = (SResultRowEntryInfo*)pRes;

  // not initialized yet, data is required
  if (pEntry == NULL) {
    return FUNC_DATA_REQUIRED_DATA_LOAD;
  }

  SFirstLastRes* pResult = GET_ROWCELL_INTERBUF(pEntry);
  if (pResult->hasResult) {
    if (pResult->pkBytes > 0) {
      pResult->pkData = pResult->buf + pResult->bytes;
    } else {
      pResult->pkData = NULL;
    }
    if (pResult->ts > pBlockInfo->window.ekey) {
      return FUNC_DATA_REQUIRED_NOT_LOAD;
    } else if (pResult->ts == pBlockInfo->window.ekey && pResult->pkData) {
      if (comparePkDataWithSValue(pResult->pkType, pResult->pkData, pBlockInfo->pks + 1, TSDB_ORDER_DESC) < 0) {
        return FUNC_DATA_REQUIRED_NOT_LOAD;
      }
    }
    return FUNC_DATA_REQUIRED_DATA_LOAD;
  } else {
    return FUNC_DATA_REQUIRED_DATA_LOAD;
  }
}

//TODO modify it to include primary key bytes
int32_t getFirstLastInfoSize(int32_t resBytes, int32_t pkBytes) { return sizeof(SFirstLastRes) + resBytes + pkBytes; }

bool getFirstLastFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  SColumnNode* pNode = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
  //TODO: change SFunctionNode to add pk info
  int32_t pkBytes = (pFunc->hasPk) ? pFunc->pkBytes : 0;
  pEnv->calcMemSize = getFirstLastInfoSize(pNode->node.resType.bytes, pkBytes);
  return true;
}

bool getSelectivityFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  SColumnNode* pNode = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
  pEnv->calcMemSize = pNode->node.resType.bytes;
  return true;
}

bool getGroupKeyFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  SColumnNode* pNode = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
  pEnv->calcMemSize = sizeof(SGroupKeyInfo) + pNode->node.resType.bytes;
  return true;
}

static FORCE_INLINE TSKEY getRowPTs(SColumnInfoData* pTsColInfo, int32_t rowIndex) {
  if (pTsColInfo == NULL || pTsColInfo->pData == NULL) {
    return 0;
  }

  return *(TSKEY*)colDataGetData(pTsColInfo, rowIndex);
}

int32_t firstLastFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo) {
  if (pResInfo->initialized) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS != functionSetup(pCtx, pResInfo)) {
    return TSDB_CODE_FUNC_SETUP_ERROR;
  }

  SFirstLastRes *       pRes = GET_ROWCELL_INTERBUF(pResInfo);
  SInputColumnInfoData* pInput = &pCtx->input;

  pRes->nullTupleSaved = false;
  pRes->nullTuplePos.pageId = -1;
  return TSDB_CODE_SUCCESS;
}

static int32_t prepareBuf(SqlFunctionCtx* pCtx) {
  if (pCtx->subsidiaries.rowLen == 0) {
    int32_t rowLen = 0;
    for (int32_t j = 0; j < pCtx->subsidiaries.num; ++j) {
      SqlFunctionCtx* pc = pCtx->subsidiaries.pCtx[j];
      rowLen += pc->pExpr->base.resSchema.bytes;
    }

    pCtx->subsidiaries.rowLen = rowLen + pCtx->subsidiaries.num * sizeof(bool);
    pCtx->subsidiaries.buf = taosMemoryMalloc(pCtx->subsidiaries.rowLen);
    if (NULL == pCtx->subsidiaries.buf) {
      return terrno;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t firstlastSaveTupleData(const SSDataBlock* pSrcBlock, int32_t rowIndex, SqlFunctionCtx* pCtx,
                                      SFirstLastRes* pInfo, bool noElements) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (pCtx->subsidiaries.num <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (!pInfo->hasResult) {
    code = saveTupleData(pCtx, rowIndex, pSrcBlock, noElements ? &pInfo->nullTuplePos : &pInfo->pos);
  } else {
    code = updateTupleData(pCtx, rowIndex, pSrcBlock, &pInfo->pos);
  }

  return code;
}

static int32_t doSaveCurrentVal(SqlFunctionCtx* pCtx, int32_t rowIndex, int64_t currentTs, char* pkData, int32_t type, char* pData) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SFirstLastRes*       pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  if (IS_VAR_DATA_TYPE(type)) {
    pInfo->bytes = varDataTLen(pData);
  }

  (void)memcpy(pInfo->buf, pData, pInfo->bytes);
  if (pkData != NULL) {
    if (IS_VAR_DATA_TYPE(pInfo->pkType)) {
      pInfo->pkBytes = varDataTLen(pkData);
    }
    (void)memcpy(pInfo->buf + pInfo->bytes, pkData, pInfo->pkBytes);
    pInfo->pkData = pInfo->buf + pInfo->bytes;
  }

  pInfo->ts = currentTs;
  int32_t code = firstlastSaveTupleData(pCtx->pSrcBlock, rowIndex, pCtx, pInfo, false);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pInfo->hasResult = true;
  return TSDB_CODE_SUCCESS;
}

// This ordinary first function does not care if current scan is ascending order or descending order scan
// the OPTIMIZED version of first function will only handle the ascending order scan
int32_t firstFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = 0;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SFirstLastRes*       pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  pInfo->bytes = pInputCol->info.bytes;

  if (IS_NULL_TYPE(pInputCol->info.type)) {
    return TSDB_CODE_SUCCESS;
  }

  SColumnInfoData* pkCol = pInput->pPrimaryKey;
  pInfo->pkType = -1;
  __compar_fn_t  pkCompareFn = NULL;
  if (pCtx->hasPrimaryKey) {
    pInfo->pkType = pkCol->info.type;
    pInfo->pkBytes = pkCol->info.bytes;
    pkCompareFn = getKeyComparFunc(pInfo->pkType, TSDB_ORDER_ASC);
  }

  // All null data column, return directly.
  if (pInput->colDataSMAIsSet && (pInput->pColumnDataAgg[0]->numOfNull == pInput->totalRows) &&
      pInputCol->hasNull == true) {
    // save selectivity value for column consisted of all null values
    int32_t code = firstlastSaveTupleData(pCtx->pSrcBlock, pInput->startRowIndex, pCtx, pInfo, !pInfo->nullTupleSaved);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pInfo->nullTupleSaved = true;
    return TSDB_CODE_SUCCESS;
  }

  SColumnDataAgg* pColAgg = (pInput->colDataSMAIsSet) ? pInput->pColumnDataAgg[0] : NULL;

  TSKEY startKey = getRowPTs(pInput->pPTS, 0);
  TSKEY endKey = getRowPTs(pInput->pPTS, pInput->totalRows - 1);

  int32_t blockDataOrder = (startKey <= endKey) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;

  //  please ref. to the comment in lastRowFunction for the reason why disabling the opt version of last/first
  //  function. we will use this opt implementation in an new version that is only available in scan subplan
#if 0
  if (blockDataOrder == TSDB_ORDER_ASC) {
    // filter according to current result firstly
    if (pResInfo->numOfRes > 0) {
      if (pInfo->ts < startKey) {
        return TSDB_CODE_SUCCESS;
      }
    }

    for (int32_t i = pInput->startRowIndex; i < pInput->startRowIndex + pInput->numOfRows; ++i) {
      if (pInputCol->hasNull && colDataIsNull(pInputCol, pInput->totalRows, i, pColAgg)) {
        continue;
      }

      numOfElems++;

      char* data = colDataGetData(pInputCol, i);
      TSKEY cts = getRowPTs(pInput->pPTS, i);
      if (pResInfo->numOfRes == 0 || pInfo->ts > cts) {
        doSaveCurrentVal(pCtx, i, cts, pInputCol->info.type, data);
        break;
      }
    }
  } else {
    // in case of descending order time stamp serial, which usually happens as the results of the nest query,
    // all data needs to be check.
    if (pResInfo->numOfRes > 0) {
      if (pInfo->ts < endKey) {
        return TSDB_CODE_SUCCESS;
      }
    }

    for (int32_t i = pInput->numOfRows + pInput->startRowIndex - 1; i >= pInput->startRowIndex; --i) {
      if (pInputCol->hasNull && colDataIsNull(pInputCol, pInput->totalRows, i, pColAgg)) {
        continue;
      }

      numOfElems++;

      char* data = colDataGetData(pInputCol, i);
      TSKEY cts = getRowPTs(pInput->pPTS, i);

      if (pResInfo->numOfRes == 0 || pInfo->ts > cts) {
        doSaveCurrentVal(pCtx, i, cts, pInputCol->info.type, data);
        break;
      }
    }
  }
#else
  int64_t* pts = (int64_t*)pInput->pPTS->pData;

  int from = -1;
  int32_t i = -1;
  while (funcInputGetNextRowIndex(pInput, from, true, &i, &from)) {
    if (pInputCol->hasNull && colDataIsNull(pInputCol, pInput->totalRows, i, pColAgg)) {
      continue;
    }

    numOfElems++;
    char* data = colDataGetData(pInputCol, i);
    char* pkData = NULL;
    if (pCtx->hasPrimaryKey) {
      pkData = colDataGetData(pkCol, i);
    }
    TSKEY cts = pts[i];
    if (pResInfo->numOfRes == 0 || pInfo->ts > cts || 
         (pInfo->ts == cts && pkCompareFn && pkCompareFn(pkData, pInfo->pkData) < 0)) {
      int32_t code = doSaveCurrentVal(pCtx, i, cts, pkData, pInputCol->info.type, data);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      pResInfo->numOfRes = 1;
    }
  }
#endif

  if (numOfElems == 0) {
    // save selectivity value for column consisted of all null values
    int32_t code = firstlastSaveTupleData(pCtx->pSrcBlock, pInput->startRowIndex, pCtx, pInfo, !pInfo->nullTupleSaved);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pInfo->nullTupleSaved = true;
  }
  SET_VAL(pResInfo, numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t lastFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = 0;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SFirstLastRes*       pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  int32_t type = pInputCol->info.type;
  int32_t bytes = pInputCol->info.bytes;
  pInfo->bytes = bytes;

  if (IS_NULL_TYPE(type)) {
    return TSDB_CODE_SUCCESS;
  }

  SColumnInfoData* pkCol = pInput->pPrimaryKey;
  pInfo->pkType = -1;
  __compar_fn_t  pkCompareFn = NULL;
  if (pCtx->hasPrimaryKey) {
    pInfo->pkType = pkCol->info.type;
    pInfo->pkBytes = pkCol->info.bytes;
    pkCompareFn = getKeyComparFunc(pInfo->pkType, TSDB_ORDER_DESC);
  }

  // All null data column, return directly.
  if (pInput->colDataSMAIsSet && (pInput->pColumnDataAgg[0]->numOfNull == pInput->totalRows) &&
      pInputCol->hasNull == true) {
    // save selectivity value for column consisted of all null values
    int32_t code = firstlastSaveTupleData(pCtx->pSrcBlock, pInput->startRowIndex, pCtx, pInfo, !pInfo->nullTupleSaved);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pInfo->nullTupleSaved = true;
    return TSDB_CODE_SUCCESS;
  }

  SColumnDataAgg* pColAgg = (pInput->colDataSMAIsSet) ? pInput->pColumnDataAgg[0] : NULL;

  TSKEY startKey = getRowPTs(pInput->pPTS, 0);
  TSKEY endKey = getRowPTs(pInput->pPTS, pInput->totalRows - 1);

  int32_t blockDataOrder = (startKey <= endKey) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;

  //  please ref. to the comment in lastRowFunction for the reason why disabling the opt version of last/first function.
#if 0
  if (blockDataOrder == TSDB_ORDER_ASC) {
    for (int32_t i = pInput->numOfRows + pInput->startRowIndex - 1; i >= pInput->startRowIndex; --i) {
      if (pInputCol->hasNull && colDataIsNull(pInputCol, pInput->totalRows, i, pColAgg)) {
        continue;
      }

      numOfElems++;

      char* data = colDataGetData(pInputCol, i);
      TSKEY cts = getRowPTs(pInput->pPTS, i);
      if (pResInfo->numOfRes == 0 || pInfo->ts < cts) {
        doSaveCurrentVal(pCtx, i, cts, type, data);
      }

      break;
    }
  } else {  // descending order
    for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
      if (pInputCol->hasNull && colDataIsNull(pInputCol, pInput->totalRows, i, pColAgg)) {
        continue;
      }

      numOfElems++;

      char* data = colDataGetData(pInputCol, i);
      TSKEY cts = getRowPTs(pInput->pPTS, i);
      if (pResInfo->numOfRes == 0 || pInfo->ts < cts) {
        doSaveCurrentVal(pCtx, i, cts, type, data);
      }
      break;
    }
  }
#else
  int64_t* pts = (int64_t*)pInput->pPTS->pData;

#if 0
    for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
      if (pInputCol->hasNull && colDataIsNull(pInputCol, pInput->totalRows, i, pColAgg)) {
        continue;
      }

      numOfElems++;
      if (pResInfo->numOfRes == 0 || pInfo->ts < pts[i]) {
        char* data = colDataGetData(pInputCol, i);
        doSaveCurrentVal(pCtx, i, pts[i], type, data);
        pResInfo->numOfRes = 1;
      }
    }
#else

// todo refactor
  if (!pInputCol->hasNull && !pCtx->hasPrimaryKey) {
    numOfElems = 1;

    int32_t round = pInput->numOfRows >> 2;
    int32_t reminder = pInput->numOfRows & 0x03;

    for (int32_t i = pInput->startRowIndex, tick = 0; tick < round; i += 4, tick += 1) {
      int64_t cts = pts[i];
      int32_t chosen = i;

      if (cts < pts[i + 1]) {
        cts = pts[i + 1];
        chosen = i + 1;
      }

      if (cts < pts[i + 2]) {
        cts = pts[i + 2];
        chosen = i + 2;
      }

      if (cts < pts[i + 3]) {
        cts = pts[i + 3];
        chosen = i + 3;
      }

      if (pResInfo->numOfRes == 0 || pInfo->ts < cts) {
        char*   data = colDataGetData(pInputCol, chosen);
        int32_t code = doSaveCurrentVal(pCtx, i, cts, NULL, type, data);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        pResInfo->numOfRes = 1;
      }
    }

    for (int32_t i = pInput->startRowIndex + round * 4; i < pInput->startRowIndex + pInput->numOfRows; ++i) {
      if (pResInfo->numOfRes == 0 || pInfo->ts < pts[i]) {
        char*   data = colDataGetData(pInputCol, i);
        int32_t code = doSaveCurrentVal(pCtx, i, pts[i], NULL, type, data);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        pResInfo->numOfRes = 1;
      }
    }
  } else {
    int from = -1;
    int32_t i = -1;
    while (funcInputGetNextRowIndex(pInput, from, false, &i, &from)) {
      if (colDataIsNull(pInputCol, pInput->totalRows, i, pColAgg)) {
        continue;
      }

      numOfElems++;
      char* pkData = NULL;
      if (pCtx->hasPrimaryKey) {
        pkData = colDataGetData(pkCol, i);
      }
      if (pResInfo->numOfRes == 0 || pInfo->ts < pts[i] ||
          (pInfo->ts == pts[i] && pkCompareFn && pkCompareFn(pkData, pInfo->pkData) < 0)) {
        char*   data = colDataGetData(pInputCol, i);
        int32_t code = doSaveCurrentVal(pCtx, i, pts[i], pkData, type, data);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        pResInfo->numOfRes = 1;
      }
    }
  }
#endif

#endif

  // save selectivity value for column consisted of all null values
  if (numOfElems == 0) {
    int32_t code = firstlastSaveTupleData(pCtx->pSrcBlock, pInput->startRowIndex, pCtx, pInfo, !pInfo->nullTupleSaved);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pInfo->nullTupleSaved = true;
  }

  return TSDB_CODE_SUCCESS;
}

static bool firstLastTransferInfoImpl(SFirstLastRes* pInput, SFirstLastRes* pOutput, bool isFirst) {
  if (!pInput->hasResult) {
    return false;
  }
  __compar_fn_t pkCompareFn = NULL;
  if (pInput->pkData) {
    pkCompareFn = getKeyComparFunc(pInput->pkType, (isFirst) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC);
  }
  if (pOutput->hasResult) {
    if (isFirst) {
      if (pInput->ts > pOutput->ts ||
          (pInput->ts == pOutput->ts && pkCompareFn && pkCompareFn(pInput->pkData, pOutput->pkData) > 0)) {
        return false;
      }
    } else {
      if (pInput->ts < pOutput->ts ||
          (pInput->ts == pOutput->ts && pkCompareFn && pkCompareFn(pInput->pkData, pOutput->pkData) > 0)) {
        return false;
      }
    }
  }

  pOutput->isNull = pInput->isNull;
  pOutput->ts = pInput->ts;
  pOutput->bytes = pInput->bytes;
  pOutput->pkType = pInput->pkType;

  (void)memcpy(pOutput->buf, pInput->buf, pOutput->bytes);
  if (pInput->pkData) {
    pOutput->pkBytes = pInput->pkBytes;
    (void)memcpy(pOutput->buf + pOutput->bytes, pInput->pkData, pOutput->pkBytes);
    pOutput->pkData = pOutput->buf + pOutput->bytes;
  }
  return true;
}

static int32_t firstLastTransferInfo(SqlFunctionCtx* pCtx, SFirstLastRes* pInput, SFirstLastRes* pOutput, bool isFirst,
                                     int32_t rowIndex) {
  if (firstLastTransferInfoImpl(pInput, pOutput, isFirst)) {
    int32_t code = firstlastSaveTupleData(pCtx->pSrcBlock, rowIndex, pCtx, pOutput, pOutput->nullTupleSaved);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    pOutput->hasResult = true;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t firstLastFunctionMergeImpl(SqlFunctionCtx* pCtx, bool isFirstQuery) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];

  if (IS_NULL_TYPE(pCol->info.type)) {
    SET_VAL(GET_RES_INFO(pCtx), 0, 1);
    return TSDB_CODE_SUCCESS;
  }

  if (pCol->info.type != TSDB_DATA_TYPE_BINARY) {
    return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }

  SFirstLastRes* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  int32_t start = pInput->startRowIndex;
  int32_t numOfElems = 0;

  for (int32_t i = start; i < start + pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pCol, i)) {
      continue;
    }
    char*          data = colDataGetData(pCol, i);
    SFirstLastRes* pInputInfo = (SFirstLastRes*)varDataVal(data);
    if (pCtx->hasPrimaryKey) {
      pInputInfo->pkData = pInputInfo->buf + pInputInfo->bytes;
    } else {
      pInputInfo->pkData = NULL;
    }

    int32_t code = firstLastTransferInfo(pCtx, pInputInfo, pInfo, isFirstQuery, i);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (!numOfElems) {
      numOfElems = pInputInfo->hasResult ? 1 : 0;
    }
  }

  if (numOfElems == 0) {
    int32_t code = firstlastSaveTupleData(pCtx->pSrcBlock, pInput->startRowIndex, pCtx, pInfo, !pInfo->nullTupleSaved);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pInfo->nullTupleSaved = true;
  }

  SET_VAL(GET_RES_INFO(pCtx), numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t firstFunctionMerge(SqlFunctionCtx* pCtx) { return firstLastFunctionMergeImpl(pCtx, true); }

int32_t lastFunctionMerge(SqlFunctionCtx* pCtx) { return firstLastFunctionMergeImpl(pCtx, false); }

int32_t firstLastFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  if (NULL == pCol) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  pResInfo->isNullRes = (pResInfo->numOfRes == 0) ? 1 : 0;

  SFirstLastRes* pRes = GET_ROWCELL_INTERBUF(pResInfo);

  if (pResInfo->isNullRes) {
    colDataSetNULL(pCol, pBlock->info.rows);
    return setSelectivityValue(pCtx, pBlock, &pRes->nullTuplePos, pBlock->info.rows);
  }
  code = colDataSetVal(pCol, pBlock->info.rows, pRes->buf, pRes->isNull || pResInfo->isNullRes);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  // handle selectivity
  code = setSelectivityValue(pCtx, pBlock, &pRes->pos, pBlock->info.rows);

  return code;
}

int32_t firstLastPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;

  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);
  SFirstLastRes*       pRes = GET_ROWCELL_INTERBUF(pEntryInfo);

  int32_t resultBytes = getFirstLastInfoSize(pRes->bytes, pRes->pkBytes);

  // todo check for failure
  char* res = taosMemoryCalloc(resultBytes + VARSTR_HEADER_SIZE, sizeof(char));
  if (NULL == res) {
    return terrno;
  }
  (void)memcpy(varDataVal(res), pRes, resultBytes);

  varDataSetLen(res, resultBytes);

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  if (NULL == pCol) {
    taosMemoryFree(res);
    return TSDB_CODE_OUT_OF_RANGE;
  }

  if (pEntryInfo->numOfRes == 0) {
    colDataSetNULL(pCol, pBlock->info.rows);
    code = setSelectivityValue(pCtx, pBlock, &pRes->nullTuplePos, pBlock->info.rows);
  } else {
    code = colDataSetVal(pCol, pBlock->info.rows, res, false);
    if (TSDB_CODE_SUCCESS != code) {
      taosMemoryFree(res);
      return code;
    }
    code = setSelectivityValue(pCtx, pBlock, &pRes->pos, pBlock->info.rows);
  }
  taosMemoryFree(res);
  return code;
}

int32_t lastCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SFirstLastRes*       pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);
  int32_t              bytes = pDBuf->bytes;

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SFirstLastRes*       pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  pDBuf->hasResult = firstLastTransferInfoImpl(pSBuf, pDBuf, false);
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
  return TSDB_CODE_SUCCESS;
}

static int32_t doSaveLastrow(SqlFunctionCtx* pCtx, char* pData, int32_t rowIndex, int64_t cts, SFirstLastRes* pInfo) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];
  SColumnInfoData* pkCol = pInput->pPrimaryKey;


  if (colDataIsNull_s(pInputCol, rowIndex)) {
    pInfo->isNull = true;
  } else {
    pInfo->isNull = false;

    if (IS_VAR_DATA_TYPE(pInputCol->info.type)) {
      pInfo->bytes = varDataTLen(pData);
    }

    (void)memcpy(pInfo->buf, pData, pInfo->bytes);
  }

  if (pCtx->hasPrimaryKey && !colDataIsNull_s(pkCol, rowIndex)) {
    char* pkData = colDataGetData(pkCol, rowIndex);
    if (IS_VAR_DATA_TYPE(pInfo->pkType)) {
      pInfo->pkBytes = varDataTLen(pkData);
    }
    (void)memcpy(pInfo->buf + pInfo->bytes, pkData, pInfo->pkBytes);
    pInfo->pkData = pInfo->buf + pInfo->bytes;
  }
  pInfo->ts = cts;
  int32_t code = firstlastSaveTupleData(pCtx->pSrcBlock, rowIndex, pCtx, pInfo, false);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pInfo->hasResult = true;

  return TSDB_CODE_SUCCESS;
}

int32_t lastRowFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = 0;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SFirstLastRes*       pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  int32_t type = pInputCol->info.type;
  int32_t bytes = pInputCol->info.bytes;
  pInfo->bytes = bytes;

  if (IS_NULL_TYPE(type)) {
    return TSDB_CODE_SUCCESS;
  }
  SColumnInfoData* pkCol = pInput->pPrimaryKey;
  pInfo->pkType = -1;
  __compar_fn_t  pkCompareFn = NULL;
  if (pCtx->hasPrimaryKey) {
    pInfo->pkType = pkCol->info.type;
    pInfo->pkBytes = pkCol->info.bytes;
    pkCompareFn = getKeyComparFunc(pInfo->pkType, TSDB_ORDER_DESC);
  }
  TSKEY startKey = getRowPTs(pInput->pPTS, 0);
  TSKEY endKey = getRowPTs(pInput->pPTS, pInput->totalRows - 1);

  if (pCtx->order == TSDB_ORDER_ASC && !pCtx->hasPrimaryKey) {
    for (int32_t i = pInput->numOfRows + pInput->startRowIndex - 1; i >= pInput->startRowIndex; --i) {
      char* data = colDataGetData(pInputCol, i);
      TSKEY cts = getRowPTs(pInput->pPTS, i);
      numOfElems++;

      if (pResInfo->numOfRes == 0 || pInfo->ts < cts) {
        int32_t code = doSaveLastrow(pCtx, data, i, cts, pInfo);
        if (code != TSDB_CODE_SUCCESS) return code;
      }

      break;
    }
  } else if (!pCtx->hasPrimaryKey && pCtx->order == TSDB_ORDER_DESC) {
    // the optimized version only valid if all tuples in one block are monotonious increasing or descreasing.
    // this assumption is NOT always works if project operator exists in downstream.
    for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
      char* data = colDataGetData(pInputCol, i);
      TSKEY cts = getRowPTs(pInput->pPTS, i);
      numOfElems++;

      if (pResInfo->numOfRes == 0 || pInfo->ts < cts) {
        int32_t code = doSaveLastrow(pCtx, data, i, cts, pInfo);
        if (code != TSDB_CODE_SUCCESS) return code;
      }
      break;
    }
  } else {
    int64_t* pts = (int64_t*)pInput->pPTS->pData;
    int from = -1;
    int32_t i = -1;
    while (funcInputGetNextRowIndex(pInput, from, false, &i, &from)) {
      bool  isNull = colDataIsNull(pInputCol, pInput->numOfRows, i, NULL);
      char* data = isNull ? NULL : colDataGetData(pInputCol, i);
      TSKEY cts = pts[i];

      numOfElems++;
      char* pkData = NULL;
      if (pCtx->hasPrimaryKey) {
        pkData = colDataGetData(pkCol, i);
      }
      if (pResInfo->numOfRes == 0 || pInfo->ts < cts ||
          (pInfo->ts == pts[i] && pkCompareFn && pkCompareFn(pkData, pInfo->pkData) < 0)) {
        int32_t code = doSaveLastrow(pCtx, data, i, cts, pInfo);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        pResInfo->numOfRes = 1;
      }
    }

  }

  SET_VAL(pResInfo, numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

bool getDiffFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SDiffInfo);
  return true;
}

int32_t diffFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo) {
  if (pResInfo->initialized) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS != functionSetup(pCtx, pResInfo)) {
    return TSDB_CODE_FUNC_SETUP_ERROR;
  }
  SDiffInfo* pDiffInfo = GET_ROWCELL_INTERBUF(pResInfo);
  pDiffInfo->hasPrev = false;
  pDiffInfo->isFirstRow = true;
  pDiffInfo->prev.i64 = 0;
  pDiffInfo->prevTs = -1;
  if (pCtx->numOfParams > 1) {
    pDiffInfo->ignoreOption = pCtx->param[1].param.i;  // TODO set correct param
  } else {
    pDiffInfo->ignoreOption = 0;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t doSetPrevVal(SDiffInfo* pDiffInfo, int32_t type, const char* pv, int64_t ts) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
      pDiffInfo->prev.i64 = *(bool*)pv ? 1 : 0;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_TINYINT:
      pDiffInfo->prev.i64 = *(int8_t*)pv;
      break;
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_INT:
      pDiffInfo->prev.i64 = *(int32_t*)pv;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_SMALLINT:
      pDiffInfo->prev.i64 = *(int16_t*)pv;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_BIGINT:
      pDiffInfo->prev.i64 = *(int64_t*)pv;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      pDiffInfo->prev.d64 = *(float*)pv;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      pDiffInfo->prev.d64 = *(double*)pv;
      break;
    default:
      return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }
  pDiffInfo->prevTs = ts;
  pDiffInfo->hasPrev = true;
  return TSDB_CODE_SUCCESS;
}

static bool diffIsNegtive(SDiffInfo* pDiffInfo, int32_t type, const char* pv) {
  switch (type) {
    case TSDB_DATA_TYPE_UINT: {
      int64_t v = *(uint32_t*)pv;
      return v < pDiffInfo->prev.i64;
    }
    case TSDB_DATA_TYPE_INT: {
      int64_t v = *(int32_t*)pv;
      return v < pDiffInfo->prev.i64;
    }
    case TSDB_DATA_TYPE_BOOL: {
      int64_t v = *(bool*)pv;
      return v < pDiffInfo->prev.i64;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      int64_t v = *(uint8_t*)pv;
      return v < pDiffInfo->prev.i64;
    }
    case TSDB_DATA_TYPE_TINYINT: {
      int64_t v = *(int8_t*)pv;
      return v < pDiffInfo->prev.i64;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      int64_t v = *(uint16_t*)pv;
      return v < pDiffInfo->prev.i64;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      int64_t v = *(int16_t*)pv;
      return v < pDiffInfo->prev.i64;
    }
    case TSDB_DATA_TYPE_UBIGINT:{
      uint64_t v = *(uint64_t*)pv;
      return v < (uint64_t)pDiffInfo->prev.i64;
    }
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t v = *(int64_t*)pv;
      return v < pDiffInfo->prev.i64;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float v = *(float*)pv;
      return v < pDiffInfo->prev.d64;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double v = *(double*)pv;
      return v < pDiffInfo->prev.d64;
    }
    default:
      return false;
  }

  return false;
}

static void tryToSetInt64(SDiffInfo* pDiffInfo, int32_t type, SColumnInfoData* pOutput, int64_t v, int32_t pos) {
  bool isNegative = v < pDiffInfo->prev.i64;
  if(type == TSDB_DATA_TYPE_UBIGINT){
    isNegative = (uint64_t)v < (uint64_t)pDiffInfo->prev.i64;
  }
  int64_t delta = v - pDiffInfo->prev.i64;
  if (isNegative && ignoreNegative(pDiffInfo->ignoreOption)) {
    colDataSetNull_f_s(pOutput, pos);
    pOutput->hasNull = true;
  } else {
    colDataSetInt64(pOutput, pos, &delta);
  }
  pDiffInfo->prev.i64 = v;
}

static void tryToSetDouble(SDiffInfo* pDiffInfo, SColumnInfoData* pOutput, double v, int32_t pos) {
  double delta = v - pDiffInfo->prev.d64;
  if (delta < 0 && ignoreNegative(pDiffInfo->ignoreOption)) {
    colDataSetNull_f_s(pOutput, pos);
  } else {
    colDataSetDouble(pOutput, pos, &delta);
  }
  pDiffInfo->prev.d64 = v;
}

static int32_t doHandleDiff(SDiffInfo* pDiffInfo, int32_t type, const char* pv, SColumnInfoData* pOutput, int32_t pos,
                            int64_t ts) {
  if (!pDiffInfo->hasPrev) {
    colDataSetNull_f_s(pOutput, pos);
    return doSetPrevVal(pDiffInfo, type, pv, ts);
  }
  pDiffInfo->prevTs = ts;
  switch (type) {
    case TSDB_DATA_TYPE_UINT: {
      int64_t v = *(uint32_t*)pv;
      tryToSetInt64(pDiffInfo, type, pOutput, v, pos);
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      int64_t v = *(int32_t*)pv;
      tryToSetInt64(pDiffInfo, type, pOutput, v, pos);
      break;
    }
    case TSDB_DATA_TYPE_BOOL: {
      int64_t v = *(bool*)pv;
      tryToSetInt64(pDiffInfo, type, pOutput, v, pos);
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      int64_t v = *(uint8_t*)pv;
      tryToSetInt64(pDiffInfo, type, pOutput, v, pos);
      break;
    }
    case TSDB_DATA_TYPE_TINYINT: {
      int64_t v = *(int8_t*)pv;
      tryToSetInt64(pDiffInfo, type, pOutput, v, pos);
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT:{
      int64_t v = *(uint16_t*)pv;
      tryToSetInt64(pDiffInfo, type, pOutput, v, pos);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      int64_t v = *(int16_t*)pv;
      tryToSetInt64(pDiffInfo, type, pOutput, v, pos);
      break;
    }
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t v = *(int64_t*)pv;
      tryToSetInt64(pDiffInfo, type, pOutput, v, pos);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      double v = *(float*)pv;
      tryToSetDouble(pDiffInfo, pOutput, v, pos);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double v = *(double*)pv;
      tryToSetDouble(pDiffInfo, pOutput, v, pos);
      break;
    }
    default:
      return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }
  pDiffInfo->hasPrev = true;
  return TSDB_CODE_SUCCESS;
}

//TODO: the primary key compare can be skipped for ordered pk if knonwn before
//TODO: for desc ordered, pk shall select the smallest one for one ts. if across block boundaries.
bool funcInputGetNextRowIndex(SInputColumnInfoData* pInput, int32_t from, bool firstOccur, int32_t* pRowIndex, int32_t* nextFrom) {
  if (pInput->pPrimaryKey == NULL) {
    if (from == -1) {
      from = pInput->startRowIndex;
    } else if (from >= pInput->numOfRows + pInput->startRowIndex) {
      return false;
    }
    *pRowIndex = from;
    *nextFrom = from + 1;
    return true;
  } else {
    if (from == -1) {
      from = pInput->startRowIndex;
    } else if (from >= pInput->numOfRows + pInput->startRowIndex) {
      return false;
    }
    TSKEY* tsList = (int64_t*)pInput->pPTS->pData;
    SColumnInfoData* pkCol = pInput->pPrimaryKey;
    int8_t pkType = pkCol->info.type;
    int32_t order = (firstOccur) ? TSDB_ORDER_ASC: TSDB_ORDER_DESC;
    __compar_fn_t compareFunc = getKeyComparFunc(pkType, order);
    int32_t select = from;
    char* val = colDataGetData(pkCol, select);
    while (from < pInput->numOfRows + pInput->startRowIndex - 1  &&  tsList[from + 1] == tsList[from]) {
      char* val1 = colDataGetData(pkCol, from + 1);
      if (compareFunc(val1, val) < 0)  {
        select = from + 1;
        val = val1;
      }
      from = from + 1;
    }
    *pRowIndex = select;
    *nextFrom = from + 1;
    return true;
  }
}

bool getForecastConfEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(float);
  return true;
}

int32_t diffResultIsNull(SqlFunctionCtx* pCtx, SFuncInputRow* pRow){
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SDiffInfo*           pDiffInfo = GET_ROWCELL_INTERBUF(pResInfo);

  if (pRow->isDataNull || !pDiffInfo->hasPrev ) {
    return true;
  }  else if (ignoreNegative(pDiffInfo->ignoreOption)){
    return diffIsNegtive(pDiffInfo, pCtx->input.pData[0]->info.type, pRow->pData);
  }
  return false;
}

bool isFirstRow(SqlFunctionCtx* pCtx, SFuncInputRow* pRow) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SDiffInfo*           pDiffInfo = GET_ROWCELL_INTERBUF(pResInfo);
  return pDiffInfo->isFirstRow;
}

int32_t trySetPreVal(SqlFunctionCtx* pCtx, SFuncInputRow* pRow) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SDiffInfo*           pDiffInfo = GET_ROWCELL_INTERBUF(pResInfo);
  pDiffInfo->isFirstRow = false;
  if (pRow->isDataNull) {
    return TSDB_CODE_SUCCESS;
  }

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];
  int8_t                inputType = pInputCol->info.type;

  char*   pv = pRow->pData;
  return doSetPrevVal(pDiffInfo, inputType, pv, pRow->ts);
}

int32_t setDoDiffResult(SqlFunctionCtx* pCtx, SFuncInputRow* pRow, int32_t pos) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SDiffInfo*           pDiffInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];
  int8_t                inputType = pInputCol->info.type;
  SColumnInfoData*      pOutput = (SColumnInfoData*)pCtx->pOutput;
  int32_t               code = TSDB_CODE_SUCCESS;
  if (pRow->isDataNull) {
    colDataSetNull_f_s(pOutput, pos);
    pOutput->hasNull = true;

    // handle selectivity
    if (pCtx->subsidiaries.num > 0) {
      code = appendSelectivityCols(pCtx, pRow->block, pRow->rowIndex, pos);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  char* pv = pRow->pData;

  if (pRow->ts == pDiffInfo->prevTs) {
    return TSDB_CODE_FUNC_DUP_TIMESTAMP;
  }
  code = doHandleDiff(pDiffInfo, inputType, pv, pOutput, pos, pRow->ts);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  // handle selectivity
  if (pCtx->subsidiaries.num > 0) {
    code = appendSelectivityCols(pCtx, pRow->block, pRow->rowIndex, pos);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t diffFunction(SqlFunctionCtx* pCtx) {
  return TSDB_CODE_SUCCESS;
}

int32_t diffFunctionByRow(SArray* pCtxArray) {
  int32_t code = TSDB_CODE_SUCCESS;
  int diffColNum = pCtxArray->size;
  if(diffColNum == 0) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t numOfElems = 0;

  SArray*  pRows = taosArrayInit_s(sizeof(SFuncInputRow), diffColNum);
  if (NULL == pRows) {
    return terrno;
  }

  bool keepNull = false;
  for (int i = 0; i < diffColNum; ++i) {
    SqlFunctionCtx* pCtx = *(SqlFunctionCtx**)taosArrayGet(pCtxArray, i);
    if (NULL == pCtx) {
      code = terrno;
      goto _exit;
    }
    funcInputUpdate(pCtx);
    SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
    SDiffInfo*           pDiffInfo = GET_ROWCELL_INTERBUF(pResInfo);
    if (!ignoreNull(pDiffInfo->ignoreOption)) {
      keepNull = true;
    }
  }

  SqlFunctionCtx* pCtx0 = *(SqlFunctionCtx**)taosArrayGet(pCtxArray, 0);
  SFuncInputRow* pRow0 = (SFuncInputRow*)taosArrayGet(pRows, 0);
  if (NULL == pCtx0 || NULL == pRow0) {
    code = terrno;
    goto _exit;
  }
  int32_t startOffset = pCtx0->offset;
  bool    result = false;
  while (1) {
    code = funcInputGetNextRow(pCtx0, pRow0, &result);
    if (TSDB_CODE_SUCCESS != code) {
      goto _exit;
    }
    if (!result) {
      break;
    }
    bool hasNotNullValue = !diffResultIsNull(pCtx0, pRow0);
    for (int i = 1; i < diffColNum; ++i) {
      SqlFunctionCtx* pCtx = *(SqlFunctionCtx**)taosArrayGet(pCtxArray, i);
      SFuncInputRow* pRow = (SFuncInputRow*)taosArrayGet(pRows, i);
      if (NULL == pCtx || NULL == pRow) {
        code = terrno;
        goto _exit;
      }
      code = funcInputGetNextRow(pCtx, pRow, &result);
      if (TSDB_CODE_SUCCESS != code) {
        goto _exit;
      }
      if (!result) {
        // rows are not equal
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        goto _exit;
      }
      if (!diffResultIsNull(pCtx, pRow)) {
        hasNotNullValue = true;
      }
    }
    int32_t pos = startOffset + numOfElems;

    bool newRow = false;
    for (int i = 0; i < diffColNum; ++i) {
      SqlFunctionCtx* pCtx = *(SqlFunctionCtx**)taosArrayGet(pCtxArray, i);
      SFuncInputRow*  pRow = (SFuncInputRow*)taosArrayGet(pRows, i);
      if (NULL == pCtx || NULL == pRow) {
        code = terrno;
        goto _exit;
      }
      if ((keepNull || hasNotNullValue) && !isFirstRow(pCtx, pRow)){
        code = setDoDiffResult(pCtx, pRow, pos);
        if (code != TSDB_CODE_SUCCESS) {
          goto _exit;
        }
        newRow = true;
      } else {
        code = trySetPreVal(pCtx, pRow);
        if (code != TSDB_CODE_SUCCESS) {
          goto _exit;
        } 
      }
    }
    if (newRow) ++numOfElems;
  }

  for (int i = 0; i < diffColNum; ++i) {
    SqlFunctionCtx*      pCtx = *(SqlFunctionCtx**)taosArrayGet(pCtxArray, i);
    if (NULL == pCtx) {
      code = terrno;
      goto _exit;
    }
    SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
    pResInfo->numOfRes = numOfElems;
  }

_exit:
  if (pRows) {
    taosArrayDestroy(pRows);
    pRows = NULL;
  }
  return code;
}

int32_t getTopBotInfoSize(int64_t numOfItems) { return sizeof(STopBotRes) + numOfItems * sizeof(STopBotResItem); }

bool getTopBotFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  SValueNode* pkNode = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);
  pEnv->calcMemSize = sizeof(STopBotRes) + pkNode->datum.i * sizeof(STopBotResItem);
  return true;
}

int32_t topBotFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo) {
  if (pResInfo->initialized) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS != functionSetup(pCtx, pResInfo)) {
    return TSDB_CODE_FUNC_SETUP_ERROR;
  }

  STopBotRes*           pRes = GET_ROWCELL_INTERBUF(pResInfo);
  SInputColumnInfoData* pInput = &pCtx->input;

  pRes->maxSize = pCtx->param[1].param.i;

  pRes->nullTupleSaved = false;
  pRes->nullTuplePos.pageId = -1;
  return TSDB_CODE_SUCCESS;
}

static STopBotRes* getTopBotOutputInfo(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  STopBotRes*          pRes = GET_ROWCELL_INTERBUF(pResInfo);
  pRes->pItems = (STopBotResItem*)((char*)pRes + sizeof(STopBotRes));

  return pRes;
}

static int32_t doAddIntoResult(SqlFunctionCtx* pCtx, void* pData, int32_t rowIndex, SSDataBlock* pSrcBlock,
                               uint16_t type, uint64_t uid, SResultRowEntryInfo* pEntryInfo, bool isTopQuery);

static int32_t addResult(SqlFunctionCtx* pCtx, STopBotResItem* pSourceItem, int16_t type, bool isTopQuery);

int32_t topFunction(SqlFunctionCtx* pCtx) {
  int32_t              numOfElems = 0;
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];

  STopBotRes* pRes = getTopBotOutputInfo(pCtx);
  pRes->type = pInput->pData[0]->info.type;

  int32_t start = pInput->startRowIndex;
  for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
    if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
      continue;
    }

    numOfElems++;
    char*   data = colDataGetData(pCol, i);
    int32_t code = doAddIntoResult(pCtx, data, i, pCtx->pSrcBlock, pRes->type, pInput->uid, pResInfo, true);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  if (numOfElems == 0 && pCtx->subsidiaries.num > 0 && !pRes->nullTupleSaved) {
    int32_t code = saveTupleData(pCtx, pInput->startRowIndex, pCtx->pSrcBlock, &pRes->nullTuplePos);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pRes->nullTupleSaved = true;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t bottomFunction(SqlFunctionCtx* pCtx) {
  int32_t              numOfElems = 0;
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];

  STopBotRes* pRes = getTopBotOutputInfo(pCtx);
  pRes->type = pInput->pData[0]->info.type;

  int32_t start = pInput->startRowIndex;
  for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
    if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
      continue;
    }

    numOfElems++;
    char*   data = colDataGetData(pCol, i);
    int32_t code = doAddIntoResult(pCtx, data, i, pCtx->pSrcBlock, pRes->type, pInput->uid, pResInfo, false);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  if (numOfElems == 0 && pCtx->subsidiaries.num > 0 && !pRes->nullTupleSaved) {
    int32_t code = saveTupleData(pCtx, pInput->startRowIndex, pCtx->pSrcBlock, &pRes->nullTuplePos);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pRes->nullTupleSaved = true;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t topBotResComparFn(const void* p1, const void* p2, const void* param) {
  uint16_t type = *(uint16_t*)param;

  STopBotResItem* val1 = (STopBotResItem*)p1;
  STopBotResItem* val2 = (STopBotResItem*)p2;

  if (IS_SIGNED_NUMERIC_TYPE(type)) {
    if (val1->v.i == val2->v.i) {
      return 0;
    }

    return (val1->v.i > val2->v.i) ? 1 : -1;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    if (val1->v.u == val2->v.u) {
      return 0;
    }

    return (val1->v.u > val2->v.u) ? 1 : -1;
  } else if (TSDB_DATA_TYPE_FLOAT == type) {
    if (val1->v.f == val2->v.f) {
      return 0;
    }

    return (val1->v.f > val2->v.f) ? 1 : -1;
  }

  if (val1->v.d == val2->v.d) {
    return 0;
  }

  return (val1->v.d > val2->v.d) ? 1 : -1;
}

int32_t doAddIntoResult(SqlFunctionCtx* pCtx, void* pData, int32_t rowIndex, SSDataBlock* pSrcBlock, uint16_t type,
                        uint64_t uid, SResultRowEntryInfo* pEntryInfo, bool isTopQuery) {
  STopBotRes* pRes = getTopBotOutputInfo(pCtx);
  int32_t     code = TSDB_CODE_SUCCESS;

  SVariant val = {0};
  TAOS_CHECK_RETURN(taosVariantCreateFromBinary(&val, pData, tDataTypes[type].bytes, type));

  STopBotResItem* pItems = pRes->pItems;

  // not full yet
  if (pEntryInfo->numOfRes < pRes->maxSize) {
    STopBotResItem* pItem = &pItems[pEntryInfo->numOfRes];
    pItem->v = val;
    pItem->uid = uid;

    // save the data of this tuple
    if (pCtx->subsidiaries.num > 0) {
      code = saveTupleData(pCtx, rowIndex, pSrcBlock, &pItem->tuplePos);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
#ifdef BUF_PAGE_DEBUG
    qDebug("page_saveTuple i:%d, item:%p,pageId:%d, offset:%d\n", pEntryInfo->numOfRes, pItem, pItem->tuplePos.pageId,
           pItem->tuplePos.offset);
#endif
    // allocate the buffer and keep the data of this row into the new allocated buffer
    pEntryInfo->numOfRes++;
    code = taosheapsort((void*)pItems, sizeof(STopBotResItem), pEntryInfo->numOfRes, (const void*)&type,
                        topBotResComparFn, !isTopQuery);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  } else {  // replace the minimum value in the result
    if ((isTopQuery && ((IS_SIGNED_NUMERIC_TYPE(type) && val.i > pItems[0].v.i) ||
                        (IS_UNSIGNED_NUMERIC_TYPE(type) && val.u > pItems[0].v.u) ||
                        (TSDB_DATA_TYPE_FLOAT == type && val.f > pItems[0].v.f) ||
                        (TSDB_DATA_TYPE_DOUBLE == type && val.d > pItems[0].v.d))) ||
        (!isTopQuery && ((IS_SIGNED_NUMERIC_TYPE(type) && val.i < pItems[0].v.i) ||
                         (IS_UNSIGNED_NUMERIC_TYPE(type) && val.u < pItems[0].v.u) ||
                         (TSDB_DATA_TYPE_FLOAT == type && val.f < pItems[0].v.f) ||
                         (TSDB_DATA_TYPE_DOUBLE == type && val.d < pItems[0].v.d)))) {
      // replace the old data and the coresponding tuple data
      STopBotResItem* pItem = &pItems[0];
      pItem->v = val;
      pItem->uid = uid;

      // save the data of this tuple by over writing the old data
      if (pCtx->subsidiaries.num > 0) {
        code = updateTupleData(pCtx, rowIndex, pSrcBlock, &pItem->tuplePos);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
#ifdef BUF_PAGE_DEBUG
      qDebug("page_copyTuple pageId:%d, offset:%d", pItem->tuplePos.pageId, pItem->tuplePos.offset);
#endif
      code = taosheapadjust((void*)pItems, sizeof(STopBotResItem), 0, pEntryInfo->numOfRes - 1, (const void*)&type,
                     topBotResComparFn, NULL, !isTopQuery);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

/*
 * +------------------------------------+--------------+--------------+
 * |            null bitmap             |              |              |
 * |(n columns, one bit for each column)| src column #1| src column #2|
 * +------------------------------------+--------------+--------------+
 */
int32_t serializeTupleData(const SSDataBlock* pSrcBlock, int32_t rowIndex, SSubsidiaryResInfo* pSubsidiaryies,
                         char* buf, char** res) {
  char* nullList = buf;
  char* pStart = (char*)(nullList + sizeof(bool) * pSubsidiaryies->num);

  int32_t offset = 0;
  for (int32_t i = 0; i < pSubsidiaryies->num; ++i) {
    SqlFunctionCtx* pc = pSubsidiaryies->pCtx[i];

    // group_key function has its own process function
    // do not process there
    if (fmIsGroupKeyFunc(pc->functionId)) {
      continue;
    }

    SFunctParam* pFuncParam = &pc->pExpr->base.pParam[0];
    int32_t      srcSlotId = pFuncParam->pCol->slotId;

    SColumnInfoData* pCol = taosArrayGet(pSrcBlock->pDataBlock, srcSlotId);
    if (NULL == pCol) {
      return TSDB_CODE_OUT_OF_RANGE;
    }
    if ((nullList[i] = colDataIsNull_s(pCol, rowIndex)) == true) {
      offset += pCol->info.bytes;
      continue;
    }

    char* p = colDataGetData(pCol, rowIndex);
    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      (void)memcpy(pStart + offset, p, (pCol->info.type == TSDB_DATA_TYPE_JSON) ? getJsonValueLen(p) : varDataTLen(p));
    } else {
      (void)memcpy(pStart + offset, p, pCol->info.bytes);
    }

    offset += pCol->info.bytes;
  }

  *res = buf;
  return TSDB_CODE_SUCCESS;
}

static int32_t doSaveTupleData(SSerializeDataHandle* pHandle, const void* pBuf, size_t length, SWinKey* key,
                               STuplePos* pPos, SFunctionStateStore* pStore) {
  STuplePos p = {0};
  if (pHandle->pBuf != NULL) {
    SFilePage* pPage = NULL;

    if (pHandle->currentPage == -1) {
      pPage = getNewBufPage(pHandle->pBuf, &pHandle->currentPage);
      if (pPage == NULL) {
        return terrno;
      }
      pPage->num = sizeof(SFilePage);
    } else {
      pPage = getBufPage(pHandle->pBuf, pHandle->currentPage);
      if (pPage == NULL) {
        return terrno;
      }
      if (pPage->num + length > getBufPageSize(pHandle->pBuf)) {
        // current page is all used, let's prepare a new buffer page
        releaseBufPage(pHandle->pBuf, pPage);
        pPage = getNewBufPage(pHandle->pBuf, &pHandle->currentPage);
        if (pPage == NULL) {
          return terrno;
        }
        pPage->num = sizeof(SFilePage);
      }
    }

    p = (STuplePos){.pageId = pHandle->currentPage, .offset = pPage->num};
    (void)memcpy(pPage->data + pPage->num, pBuf, length);

    pPage->num += length;
    setBufPageDirty(pPage, true);
    releaseBufPage(pHandle->pBuf, pPage);
  } else { // other tuple save policy
    if (pStore->streamStateFuncPut(pHandle->pState, key, pBuf, length) >= 0) {
      p.streamTupleKey = *key;
    }
  }

  *pPos = p;
  return TSDB_CODE_SUCCESS;
}

int32_t saveTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STuplePos* pPos) {
  int32_t code = prepareBuf(pCtx);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  SWinKey key = {0};
  if (pCtx->saveHandle.pBuf == NULL) {
    SColumnInfoData* pColInfo = taosArrayGet(pSrcBlock->pDataBlock, pCtx->saveHandle.pState->tsIndex);
    if (NULL == pColInfo) {
      return TSDB_CODE_OUT_OF_RANGE;
    }
    if (pColInfo->info.type != TSDB_DATA_TYPE_TIMESTAMP) {
      return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
    }
    key.groupId = pSrcBlock->info.id.groupId;
    key.ts = *(int64_t*)colDataGetData(pColInfo, rowIndex);
  }

  char* buf = NULL;
  code = serializeTupleData(pSrcBlock, rowIndex, &pCtx->subsidiaries, pCtx->subsidiaries.buf, &buf);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  return doSaveTupleData(&pCtx->saveHandle, buf, pCtx->subsidiaries.rowLen, &key, pPos, pCtx->pStore);
}

static int32_t doUpdateTupleData(SSerializeDataHandle* pHandle, const void* pBuf, size_t length, STuplePos* pPos, SFunctionStateStore* pStore) {
  if (pHandle->pBuf != NULL) {
    SFilePage* pPage = getBufPage(pHandle->pBuf, pPos->pageId);
    if (pPage == NULL) {
      return terrno;
    }
    (void)memcpy(pPage->data + pPos->offset, pBuf, length);
    setBufPageDirty(pPage, true);
    releaseBufPage(pHandle->pBuf, pPage);
  } else {
    int32_t code = pStore->streamStateFuncPut(pHandle->pState, &pPos->streamTupleKey, pBuf, length);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t updateTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STuplePos* pPos) {
  int32_t code = prepareBuf(pCtx);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  char* buf = NULL;
  code = serializeTupleData(pSrcBlock, rowIndex, &pCtx->subsidiaries, pCtx->subsidiaries.buf, &buf);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  return doUpdateTupleData(&pCtx->saveHandle, buf, pCtx->subsidiaries.rowLen, pPos, pCtx->pStore);
}

static int32_t doLoadTupleData(SSerializeDataHandle* pHandle, const STuplePos* pPos, SFunctionStateStore* pStore, char** value) {
  if (pHandle->pBuf != NULL) {
    SFilePage* pPage = getBufPage(pHandle->pBuf, pPos->pageId);
    if (pPage == NULL) {
      *value = NULL;
      return terrno;
    }
    *value = pPage->data + pPos->offset;
    releaseBufPage(pHandle->pBuf, pPage);
    return TSDB_CODE_SUCCESS;
  } else {
    *value = NULL;
    int32_t vLen;
    int32_t code = pStore->streamStateFuncGet(pHandle->pState, &pPos->streamTupleKey, (void **)(value), &vLen);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    return TSDB_CODE_SUCCESS;
  }
}

int32_t loadTupleData(SqlFunctionCtx* pCtx, const STuplePos* pPos, char** value) {
  return doLoadTupleData(&pCtx->saveHandle, pPos, pCtx->pStore, value);
}

int32_t topBotFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;

  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);
  STopBotRes*          pRes = getTopBotOutputInfo(pCtx);

  int16_t type = pCtx->pExpr->base.resSchema.type;
  int32_t slotId = pCtx->pExpr->base.resSchema.slotId;

  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  if (NULL == pCol) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  // todo assign the tag value and the corresponding row data
  int32_t currentRow = pBlock->info.rows;
  if (pEntryInfo->numOfRes <= 0) {
    colDataSetNULL(pCol, currentRow);
    code = setSelectivityValue(pCtx, pBlock, &pRes->nullTuplePos, currentRow);
    return code;
  }
  for (int32_t i = 0; i < pEntryInfo->numOfRes; ++i) {
    STopBotResItem* pItem = &pRes->pItems[i];
    code = colDataSetVal(pCol, currentRow, (const char*)&pItem->v.i, false);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
#ifdef BUF_PAGE_DEBUG
    qDebug("page_finalize i:%d,item:%p,pageId:%d, offset:%d\n", i, pItem, pItem->tuplePos.pageId,
           pItem->tuplePos.offset);
#endif
    code = setSelectivityValue(pCtx, pBlock, &pRes->pItems[i].tuplePos, currentRow);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    currentRow += 1;
  }

  return code;
}

int32_t addResult(SqlFunctionCtx* pCtx, STopBotResItem* pSourceItem, int16_t type, bool isTopQuery) {
  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);
  STopBotRes*          pRes = getTopBotOutputInfo(pCtx);
  STopBotResItem*      pItems = pRes->pItems;
  int32_t              code = TSDB_CODE_SUCCESS;

  // not full yet
  if (pEntryInfo->numOfRes < pRes->maxSize) {
    STopBotResItem* pItem = &pItems[pEntryInfo->numOfRes];
    pItem->v = pSourceItem->v;
    pItem->uid = pSourceItem->uid;
    pItem->tuplePos.pageId = -1;
    replaceTupleData(&pItem->tuplePos, &pSourceItem->tuplePos);
    pEntryInfo->numOfRes++;
   code = taosheapsort((void*)pItems, sizeof(STopBotResItem), pEntryInfo->numOfRes, (const void*)&type,
                        topBotResComparFn, !isTopQuery);
   if (TSDB_CODE_SUCCESS != code) {
     return code;
   }
  } else {  // replace the minimum value in the result
    if ((isTopQuery && ((IS_SIGNED_NUMERIC_TYPE(type) && pSourceItem->v.i > pItems[0].v.i) ||
                        (IS_UNSIGNED_NUMERIC_TYPE(type) && pSourceItem->v.u > pItems[0].v.u) ||
                        (TSDB_DATA_TYPE_FLOAT == type && pSourceItem->v.f > pItems[0].v.f) ||
                        (TSDB_DATA_TYPE_DOUBLE == type && pSourceItem->v.d > pItems[0].v.d))) ||
        (!isTopQuery && ((IS_SIGNED_NUMERIC_TYPE(type) && pSourceItem->v.i < pItems[0].v.i) ||
                         (IS_UNSIGNED_NUMERIC_TYPE(type) && pSourceItem->v.u < pItems[0].v.u) ||
                         (TSDB_DATA_TYPE_FLOAT == type && pSourceItem->v.f < pItems[0].v.f) ||
                         (TSDB_DATA_TYPE_DOUBLE == type && pSourceItem->v.d < pItems[0].v.d)))) {
      // replace the old data and the coresponding tuple data
      STopBotResItem* pItem = &pItems[0];
      pItem->v = pSourceItem->v;
      pItem->uid = pSourceItem->uid;

      // save the data of this tuple by over writing the old data
      replaceTupleData(&pItem->tuplePos, &pSourceItem->tuplePos);
      code = taosheapadjust((void*)pItems, sizeof(STopBotResItem), 0, pEntryInfo->numOfRes - 1, (const void*)&type,
                            topBotResComparFn, NULL, !isTopQuery);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  }
  return code;
}

int32_t topCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  STopBotRes*          pSBuf = getTopBotOutputInfo(pSourceCtx);
  int16_t              type = pSBuf->type;
  int32_t              code = TSDB_CODE_SUCCESS;
  for (int32_t i = 0; i < pSResInfo->numOfRes; i++) {
    code = addResult(pDestCtx, pSBuf->pItems + i, type, true);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t bottomCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  STopBotRes*          pSBuf = getTopBotOutputInfo(pSourceCtx);
  int16_t              type = pSBuf->type;
  int32_t              code = TSDB_CODE_SUCCESS;
  for (int32_t i = 0; i < pSResInfo->numOfRes; i++) {
    code = addResult(pDestCtx, pSBuf->pItems + i, type, false);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t getSpreadInfoSize() { return (int32_t)sizeof(SSpreadInfo); }

bool getSpreadFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SSpreadInfo);
  return true;
}

int32_t spreadFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (pResultInfo->initialized) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS != functionSetup(pCtx, pResultInfo)) {
    return TSDB_CODE_FUNC_SETUP_ERROR;
  }

  SSpreadInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  SET_DOUBLE_VAL(&pInfo->min, DBL_MAX);
  SET_DOUBLE_VAL(&pInfo->max, -DBL_MAX);
  pInfo->hasResult = false;
  return TSDB_CODE_SUCCESS;
}

int32_t spreadFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];
  int32_t               type = pInput->pData[0]->info.type;

  SSpreadInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  if (pInput->colDataSMAIsSet) {
    numOfElems = pInput->numOfRows - pAgg->numOfNull;
    if (numOfElems == 0) {
      goto _spread_over;
    }
    double tmin = 0.0, tmax = 0.0;
    if (IS_SIGNED_NUMERIC_TYPE(type) || IS_TIMESTAMP_TYPE(type)) {
      tmin = (double)GET_INT64_VAL(&pAgg->min);
      tmax = (double)GET_INT64_VAL(&pAgg->max);
    } else if (IS_FLOAT_TYPE(type)) {
      tmin = GET_DOUBLE_VAL(&pAgg->min);
      tmax = GET_DOUBLE_VAL(&pAgg->max);
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      tmin = (double)GET_UINT64_VAL(&pAgg->min);
      tmax = (double)GET_UINT64_VAL(&pAgg->max);
    }

    if (GET_DOUBLE_VAL(&pInfo->min) > tmin) {
      SET_DOUBLE_VAL(&pInfo->min, tmin);
    }

    if (GET_DOUBLE_VAL(&pInfo->max) < tmax) {
      SET_DOUBLE_VAL(&pInfo->max, tmax);
    }

  } else {  // computing based on the true data block
    SColumnInfoData* pCol = pInput->pData[0];

    int32_t start = pInput->startRowIndex;
    // check the valid data one by one
    for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
      if (colDataIsNull_f(pCol->nullbitmap, i)) {
        continue;
      }

      char* data = colDataGetData(pCol, i);

      double v = 0;
      GET_TYPED_DATA(v, double, type, data);
      if (v < GET_DOUBLE_VAL(&pInfo->min)) {
        SET_DOUBLE_VAL(&pInfo->min, v);
      }

      if (v > GET_DOUBLE_VAL(&pInfo->max)) {
        SET_DOUBLE_VAL(&pInfo->max, v);
      }

      numOfElems += 1;
    }
  }

_spread_over:
  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElems, 1);
  if (numOfElems > 0) {
    pInfo->hasResult = true;
  }

  return TSDB_CODE_SUCCESS;
}

static void spreadTransferInfo(SSpreadInfo* pInput, SSpreadInfo* pOutput) {
  pOutput->hasResult = pInput->hasResult;
  if (pInput->max > pOutput->max) {
    pOutput->max = pInput->max;
  }

  if (pInput->min < pOutput->min) {
    pOutput->min = pInput->min;
  }
}

int32_t spreadFunctionMerge(SqlFunctionCtx* pCtx) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];

  if (IS_NULL_TYPE(pCol->info.type)) {
    SET_VAL(GET_RES_INFO(pCtx), 0, 1);
    return TSDB_CODE_SUCCESS;
  }

  if (pCol->info.type != TSDB_DATA_TYPE_BINARY) {
    return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }

  SSpreadInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  int32_t start = pInput->startRowIndex;
  for (int32_t i = start; i < start + pInput->numOfRows; ++i) {
    if(colDataIsNull_s(pCol, i)) continue;
    char*        data = colDataGetData(pCol, i);
    SSpreadInfo* pInputInfo = (SSpreadInfo*)varDataVal(data);
    if (pInputInfo->hasResult) {
      spreadTransferInfo(pInputInfo, pInfo);
    }
  }

  if (pInfo->hasResult) {
    GET_RES_INFO(pCtx)->numOfRes = 1;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t spreadFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SSpreadInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  if (pInfo->hasResult == true) {
    SET_DOUBLE_VAL(&pInfo->result, pInfo->max - pInfo->min);
  } else {
    GET_RES_INFO(pCtx)->isNullRes = 1;
  }
  return functionFinalize(pCtx, pBlock);
}

int32_t spreadPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SSpreadInfo*         pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t              resultBytes = getSpreadInfoSize();
  char*                res = taosMemoryCalloc(resultBytes + VARSTR_HEADER_SIZE, sizeof(char));

  if (NULL == res) {
    return terrno;
  }
  (void)memcpy(varDataVal(res), pInfo, resultBytes);
  varDataSetLen(res, resultBytes);

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  int32_t          code = TSDB_CODE_SUCCESS;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  if (NULL == pCol) {
    code = terrno;
    goto _exit;
  }

  code = colDataSetVal(pCol, pBlock->info.rows, res, false);
  if (TSDB_CODE_SUCCESS != code) {
    goto _exit;
  }

_exit:
  taosMemoryFree(res);
  return code;
}

int32_t spreadCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SSpreadInfo*         pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SSpreadInfo*         pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);
  spreadTransferInfo(pSBuf, pDBuf);
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
  return TSDB_CODE_SUCCESS;
}

int32_t getElapsedInfoSize() { return (int32_t)sizeof(SElapsedInfo); }

bool getElapsedFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SElapsedInfo);
  return true;
}

int32_t elapsedFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (pResultInfo->initialized) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS != functionSetup(pCtx, pResultInfo)) {
    return TSDB_CODE_FUNC_SETUP_ERROR;
  }

  SElapsedInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  pInfo->result = 0;
  pInfo->min = TSKEY_MAX;
  pInfo->max = 0;

  if (pCtx->numOfParams > 1) {
    pInfo->timeUnit = pCtx->param[1].param.i;
  } else {
    pInfo->timeUnit = 1;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t elapsedFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];

  SElapsedInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  numOfElems = pInput->numOfRows;  // since this is the primary timestamp, no need to exclude NULL values
  if (numOfElems == 0) {
    // for stream
    if (pCtx->end.key != INT64_MIN) {
      pInfo->max = pCtx->end.key + 1;
    }
    goto _elapsed_over;
  }

  if (pInput->colDataSMAIsSet) {
    if (pInfo->min == TSKEY_MAX) {
      pInfo->min = GET_INT64_VAL(&pAgg->min);
      pInfo->max = GET_INT64_VAL(&pAgg->max);
    } else {
      if (pCtx->order == TSDB_ORDER_ASC) {
        pInfo->max = GET_INT64_VAL(&pAgg->max);
      } else {
        pInfo->min = GET_INT64_VAL(&pAgg->min);
      }
    }
  } else {  // computing based on the true data block
    if (0 == pInput->numOfRows) {
      if (pCtx->order == TSDB_ORDER_DESC) {
        if (pCtx->end.key != INT64_MIN) {
          pInfo->min = pCtx->end.key;
        }
      } else {
        if (pCtx->end.key != INT64_MIN) {
          pInfo->max = pCtx->end.key + 1;
        }
      }
      goto _elapsed_over;
    }

    SColumnInfoData* pCol = pInput->pData[0];

    int32_t start = pInput->startRowIndex;
    TSKEY*  ptsList = (int64_t*)colDataGetData(pCol, 0);
    if (pCtx->order == TSDB_ORDER_DESC) {
      if (pCtx->start.key == INT64_MIN) {
        pInfo->max = (pInfo->max < ptsList[start]) ? ptsList[start] : pInfo->max;
      } else {
        pInfo->max = pCtx->start.key + 1;
      }

      if (pCtx->end.key == INT64_MIN) {
        pInfo->min =
            (pInfo->min > ptsList[start + pInput->numOfRows - 1]) ? ptsList[start + pInput->numOfRows - 1] : pInfo->min;
      } else {
        pInfo->min = pCtx->end.key;
      }
    } else {
      if (pCtx->start.key == INT64_MIN) {
        pInfo->min = (pInfo->min > ptsList[start]) ? ptsList[start] : pInfo->min;
      } else {
        pInfo->min = pCtx->start.key;
      }

      if (pCtx->end.key == INT64_MIN) {
        pInfo->max =
            (pInfo->max < ptsList[start + pInput->numOfRows - 1]) ? ptsList[start + pInput->numOfRows - 1] : pInfo->max;
      } else {
        pInfo->max = pCtx->end.key + 1;
      }
    }
  }

_elapsed_over:
  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElems, 1);

  return TSDB_CODE_SUCCESS;
}

static void elapsedTransferInfo(SElapsedInfo* pInput, SElapsedInfo* pOutput) {
  pOutput->timeUnit = pInput->timeUnit;
  if (pOutput->min > pInput->min) {
    pOutput->min = pInput->min;
  }

  if (pOutput->max < pInput->max) {
    pOutput->max = pInput->max;
  }
}

int32_t elapsedFunctionMerge(SqlFunctionCtx* pCtx) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];
  if (pCol->info.type != TSDB_DATA_TYPE_BINARY) {
    return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }

  SElapsedInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  int32_t start = pInput->startRowIndex;

  for (int32_t i = start; i < start + pInput->numOfRows; ++i) {
    char*         data = colDataGetData(pCol, i);
    SElapsedInfo* pInputInfo = (SElapsedInfo*)varDataVal(data);
    elapsedTransferInfo(pInputInfo, pInfo);
  }

  SET_VAL(GET_RES_INFO(pCtx), 1, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t elapsedFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SElapsedInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  double        result = (double)pInfo->max - (double)pInfo->min;
  result = (result >= 0) ? result : -result;
  pInfo->result = result / pInfo->timeUnit;
  return functionFinalize(pCtx, pBlock);
}

int32_t elapsedPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SElapsedInfo*        pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t              resultBytes = getElapsedInfoSize();
  char*                res = taosMemoryCalloc(resultBytes + VARSTR_HEADER_SIZE, sizeof(char));

  if (NULL == res) {
    return terrno;
  }
  (void)memcpy(varDataVal(res), pInfo, resultBytes);
  varDataSetLen(res, resultBytes);

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  int32_t          code = TSDB_CODE_SUCCESS;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  if (NULL == pCol) {
    code = terrno;
    goto _exit;
  }

  code = colDataSetVal(pCol, pBlock->info.rows, res, false);
  if (TSDB_CODE_SUCCESS != code) {
    goto _exit;
  }
_exit:
  taosMemoryFree(res);
  return code;
}

int32_t elapsedCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SElapsedInfo*        pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SElapsedInfo*        pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  elapsedTransferInfo(pSBuf, pDBuf);
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
  return TSDB_CODE_SUCCESS;
}

int32_t getHistogramInfoSize() {
  return (int32_t)sizeof(SHistoFuncInfo) + HISTOGRAM_MAX_BINS_NUM * sizeof(SHistoFuncBin);
}

bool getHistogramFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SHistoFuncInfo) + HISTOGRAM_MAX_BINS_NUM * sizeof(SHistoFuncBin);
  return true;
}

static int8_t getHistogramBinType(char* binTypeStr) {
  int8_t binType;
  if (strcasecmp(binTypeStr, "user_input") == 0) {
    binType = USER_INPUT_BIN;
  } else if (strcasecmp(binTypeStr, "linear_bin") == 0) {
    binType = LINEAR_BIN;
  } else if (strcasecmp(binTypeStr, "log_bin") == 0) {
    binType = LOG_BIN;
  } else {
    binType = UNKNOWN_BIN;
  }

  return binType;
}

static int32_t getHistogramBinDesc(SHistoFuncInfo* pInfo, char* binDescStr, int8_t binType, bool normalized) {
  cJSON*  binDesc = cJSON_Parse(binDescStr);
  int32_t numOfBins;
  double* intervals;
  if (cJSON_IsObject(binDesc)) { /* linaer/log bins */
    int32_t numOfParams = cJSON_GetArraySize(binDesc);
    int32_t startIndex;
    if (numOfParams != 4) {
      cJSON_Delete(binDesc);
      return TSDB_CODE_FAILED;
    }

    cJSON* start = cJSON_GetObjectItem(binDesc, "start");
    cJSON* factor = cJSON_GetObjectItem(binDesc, "factor");
    cJSON* width = cJSON_GetObjectItem(binDesc, "width");
    cJSON* count = cJSON_GetObjectItem(binDesc, "count");
    cJSON* infinity = cJSON_GetObjectItem(binDesc, "infinity");

    if (!cJSON_IsNumber(start) || !cJSON_IsNumber(count) || !cJSON_IsBool(infinity)) {
      cJSON_Delete(binDesc);
      return TSDB_CODE_FAILED;
    }

    if (count->valueint <= 0 || count->valueint > 1000) {  // limit count to 1000
      cJSON_Delete(binDesc);
      return TSDB_CODE_FAILED;
    }

    if (isinf(start->valuedouble) || (width != NULL && isinf(width->valuedouble)) ||
        (factor != NULL && isinf(factor->valuedouble)) || (count != NULL && isinf(count->valuedouble))) {
      cJSON_Delete(binDesc);
      return TSDB_CODE_FAILED;
    }

    int32_t counter = (int32_t)count->valueint;
    if (infinity->valueint == false) {
      startIndex = 0;
      numOfBins = counter + 1;
    } else {
      startIndex = 1;
      numOfBins = counter + 3;
    }

    intervals = taosMemoryCalloc(numOfBins, sizeof(double));
    if (NULL == intervals) {
      cJSON_Delete(binDesc);
      qError("histogram function out of memory");
      return terrno;
    }
    if (cJSON_IsNumber(width) && factor == NULL && binType == LINEAR_BIN) {
      // linear bin process
      if (width->valuedouble == 0) {
        taosMemoryFree(intervals);
        cJSON_Delete(binDesc);
        return TSDB_CODE_FAILED;
      }
      for (int i = 0; i < counter + 1; ++i) {
        intervals[startIndex] = start->valuedouble + i * width->valuedouble;
        if (isinf(intervals[startIndex])) {
          taosMemoryFree(intervals);
          cJSON_Delete(binDesc);
          return TSDB_CODE_FAILED;
        }
        startIndex++;
      }
    } else if (cJSON_IsNumber(factor) && width == NULL && binType == LOG_BIN) {
      // log bin process
      if (start->valuedouble == 0) {
        taosMemoryFree(intervals);
        cJSON_Delete(binDesc);
        return TSDB_CODE_FAILED;
      }
      if (factor->valuedouble < 0 || factor->valuedouble == 0 || factor->valuedouble == 1) {
        taosMemoryFree(intervals);
        cJSON_Delete(binDesc);
        return TSDB_CODE_FAILED;
      }
      for (int i = 0; i < counter + 1; ++i) {
        intervals[startIndex] = start->valuedouble * pow(factor->valuedouble, i * 1.0);
        if (isinf(intervals[startIndex])) {
          taosMemoryFree(intervals);
          cJSON_Delete(binDesc);
          return TSDB_CODE_FAILED;
        }
        startIndex++;
      }
    } else {
      taosMemoryFree(intervals);
      cJSON_Delete(binDesc);
      return TSDB_CODE_FAILED;
    }

    if (infinity->valueint == true) {
      intervals[0] = -INFINITY;
      intervals[numOfBins - 1] = INFINITY;
      // in case of desc bin orders, -inf/inf should be swapped
      if (numOfBins < 4) {
        return TSDB_CODE_FAILED;
      }
      if (intervals[1] > intervals[numOfBins - 2]) {
        TSWAP(intervals[0], intervals[numOfBins - 1]);
      }
    }
  } else if (cJSON_IsArray(binDesc)) { /* user input bins */
    if (binType != USER_INPUT_BIN) {
      cJSON_Delete(binDesc);
      return TSDB_CODE_FAILED;
    }
    numOfBins = cJSON_GetArraySize(binDesc);
    intervals = taosMemoryCalloc(numOfBins, sizeof(double));
    if (NULL == intervals) {
      cJSON_Delete(binDesc);
      qError("histogram function out of memory");
      return terrno;
    }
    cJSON* bin = binDesc->child;
    if (bin == NULL) {
      taosMemoryFree(intervals);
      cJSON_Delete(binDesc);
      return TSDB_CODE_FAILED;
    }
    int i = 0;
    while (bin) {
      intervals[i] = bin->valuedouble;
      if (!cJSON_IsNumber(bin)) {
        taosMemoryFree(intervals);
        cJSON_Delete(binDesc);
        return TSDB_CODE_FAILED;
      }
      if (i != 0 && intervals[i] <= intervals[i - 1]) {
        taosMemoryFree(intervals);
        cJSON_Delete(binDesc);
        return TSDB_CODE_FAILED;
      }
      bin = bin->next;
      i++;
    }
  } else {
    cJSON_Delete(binDesc);
    return TSDB_CODE_FAILED;
  }

  pInfo->numOfBins = numOfBins - 1;
  pInfo->normalized = normalized;
  for (int32_t i = 0; i < pInfo->numOfBins; ++i) {
    pInfo->bins[i].lower = intervals[i] < intervals[i + 1] ? intervals[i] : intervals[i + 1];
    pInfo->bins[i].upper = intervals[i + 1] > intervals[i] ? intervals[i + 1] : intervals[i];
    pInfo->bins[i].count = 0;
  }

  taosMemoryFree(intervals);
  cJSON_Delete(binDesc);

  return TSDB_CODE_SUCCESS;
}

int32_t histogramFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (pResultInfo->initialized) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS != functionSetup(pCtx, pResultInfo)) {
    return TSDB_CODE_FUNC_SETUP_ERROR;
  }

  SHistoFuncInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  pInfo->numOfBins = 0;
  pInfo->totalCount = 0;
  pInfo->normalized = 0;

  char*  binTypeStr = taosStrndup(varDataVal(pCtx->param[1].param.pz), varDataLen(pCtx->param[1].param.pz));
  if (binTypeStr == NULL) {
    return terrno;
  }
  int8_t binType = getHistogramBinType(binTypeStr);
  taosMemoryFree(binTypeStr);

  if (binType == UNKNOWN_BIN) {
    return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
  }
  char*   binDesc = taosStrndup(varDataVal(pCtx->param[2].param.pz), varDataLen(pCtx->param[2].param.pz));
  if (binDesc == NULL) {
    return terrno;
  }
  int64_t normalized = pCtx->param[3].param.i;
  if (normalized != 0 && normalized != 1) {
    taosMemoryFree(binDesc);
    return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
  }
  int32_t code = getHistogramBinDesc(pInfo, binDesc, binType, (bool)normalized);
  if (TSDB_CODE_SUCCESS != code) {
    taosMemoryFree(binDesc);
    return code;
  }
  taosMemoryFree(binDesc);

  return TSDB_CODE_SUCCESS;
}

static int32_t histogramFunctionImpl(SqlFunctionCtx* pCtx, bool isPartial) {
  SHistoFuncInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];

  int32_t type = pInput->pData[0]->info.type;

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;

  int32_t numOfElems = 0;
  for (int32_t i = start; i < numOfRows + start; ++i) {
    if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
      continue;
    }

    numOfElems++;

    char*  data = colDataGetData(pCol, i);
    double v;
    GET_TYPED_DATA(v, double, type, data);

    for (int32_t k = 0; k < pInfo->numOfBins; ++k) {
      if (v > pInfo->bins[k].lower && v <= pInfo->bins[k].upper) {
        pInfo->bins[k].count++;
        pInfo->totalCount++;
        break;
      }
    }
  }

  if (!isPartial) {
    GET_RES_INFO(pCtx)->numOfRes = pInfo->numOfBins;
  } else {
    GET_RES_INFO(pCtx)->numOfRes = 1;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t histogramFunction(SqlFunctionCtx* pCtx) { return histogramFunctionImpl(pCtx, false); }

int32_t histogramFunctionPartial(SqlFunctionCtx* pCtx) { return histogramFunctionImpl(pCtx, true); }

static void histogramTransferInfo(SHistoFuncInfo* pInput, SHistoFuncInfo* pOutput) {
  pOutput->normalized = pInput->normalized;
  pOutput->numOfBins = pInput->numOfBins;
  pOutput->totalCount += pInput->totalCount;
  for (int32_t k = 0; k < pOutput->numOfBins; ++k) {
    pOutput->bins[k].lower = pInput->bins[k].lower;
    pOutput->bins[k].upper = pInput->bins[k].upper;
    pOutput->bins[k].count += pInput->bins[k].count;
  }
}

int32_t histogramFunctionMerge(SqlFunctionCtx* pCtx) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];
  if (pCol->info.type != TSDB_DATA_TYPE_BINARY) {
    return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }

  SHistoFuncInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  int32_t start = pInput->startRowIndex;

  for (int32_t i = start; i < start + pInput->numOfRows; ++i) {
    char*           data = colDataGetData(pCol, i);
    SHistoFuncInfo* pInputInfo = (SHistoFuncInfo*)varDataVal(data);
    histogramTransferInfo(pInputInfo, pInfo);
  }

  SET_VAL(GET_RES_INFO(pCtx), pInfo->numOfBins, pInfo->numOfBins);
  return TSDB_CODE_SUCCESS;
}

int32_t histogramFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SHistoFuncInfo*      pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t              slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData*     pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  int32_t              code = TSDB_CODE_SUCCESS;

  int32_t currentRow = pBlock->info.rows;
  if (NULL == pCol) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  if (pInfo->normalized) {
    for (int32_t k = 0; k < pResInfo->numOfRes; ++k) {
      if (pInfo->totalCount != 0) {
        pInfo->bins[k].percentage = pInfo->bins[k].count / (double)pInfo->totalCount;
      } else {
        pInfo->bins[k].percentage = 0;
      }
    }
  }

  for (int32_t i = 0; i < pResInfo->numOfRes; ++i) {
    int32_t len;
    char    buf[512] = {0};
    if (!pInfo->normalized) {
      len = tsnprintf(varDataVal(buf), sizeof(buf) - VARSTR_HEADER_SIZE, "{\"lower_bin\":%g, \"upper_bin\":%g, \"count\":%" PRId64 "}",
                    pInfo->bins[i].lower, pInfo->bins[i].upper, pInfo->bins[i].count);
    } else {
      len = tsnprintf(varDataVal(buf), sizeof(buf) - VARSTR_HEADER_SIZE, "{\"lower_bin\":%g, \"upper_bin\":%g, \"count\":%lf}", pInfo->bins[i].lower,
                    pInfo->bins[i].upper, pInfo->bins[i].percentage);
    }
    varDataSetLen(buf, len);
    code = colDataSetVal(pCol, currentRow, buf, false);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    currentRow++;
  }

  return code;
}

int32_t histogramPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SHistoFuncInfo*      pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t              resultBytes = getHistogramInfoSize();
  char*                res = taosMemoryCalloc(resultBytes + VARSTR_HEADER_SIZE, sizeof(char));

  if (NULL == res) {
    return terrno;
  }
  (void)memcpy(varDataVal(res), pInfo, resultBytes);
  varDataSetLen(res, resultBytes);

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  int32_t          code = TSDB_CODE_SUCCESS;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  if (NULL == pCol) {
    code = terrno;
    goto _exit;
  }
  code = colDataSetVal(pCol, pBlock->info.rows, res, false);

_exit:
  taosMemoryFree(res);
  return code;
}

int32_t histogramCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SHistoFuncInfo*      pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SHistoFuncInfo*      pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  histogramTransferInfo(pSBuf, pDBuf);
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
  return TSDB_CODE_SUCCESS;
}

int32_t getHLLInfoSize() { return (int32_t)sizeof(SHLLInfo); }

bool getHLLFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SHLLInfo);
  return true;
}

static uint8_t hllCountNum(void* data, int32_t bytes, int32_t* buk) {
  uint64_t hash = MurmurHash3_64(data, bytes);
  int32_t  index = hash & HLL_BUCKET_MASK;
  hash >>= HLL_BUCKET_BITS;
  hash |= ((uint64_t)1 << HLL_DATA_BITS);
  uint64_t bit = 1;
  uint8_t  count = 1;
  while ((hash & bit) == 0) {
    count++;
    bit <<= 1;
  }
  *buk = index;
  return count;
}

static void hllBucketHisto(uint8_t* buckets, int32_t* bucketHisto) {
  uint64_t* word = (uint64_t*)buckets;
  uint8_t*  bytes;

  for (int32_t j = 0; j < HLL_BUCKETS >> 3; j++) {
    if (*word == 0) {
      bucketHisto[0] += 8;
    } else {
      bytes = (uint8_t*)word;
      bucketHisto[bytes[0]]++;
      bucketHisto[bytes[1]]++;
      bucketHisto[bytes[2]]++;
      bucketHisto[bytes[3]]++;
      bucketHisto[bytes[4]]++;
      bucketHisto[bytes[5]]++;
      bucketHisto[bytes[6]]++;
      bucketHisto[bytes[7]]++;
    }
    word++;
  }
}
static double hllTau(double x) {
  if (x == 0. || x == 1.) return 0.;
  double zPrime;
  double y = 1.0;
  double z = 1 - x;
  do {
    x = sqrt(x);
    zPrime = z;
    y *= 0.5;
    z -= pow(1 - x, 2) * y;
  } while (zPrime != z);
  return z / 3;
}

static double hllSigma(double x) {
  if (x == 1.0) return INFINITY;
  double zPrime;
  double y = 1;
  double z = x;
  do {
    x *= x;
    zPrime = z;
    z += x * y;
    y += y;
  } while (zPrime != z);
  return z;
}

// estimate the cardinality, the algorithm refer this paper: "New cardinality estimation algorithms for HyperLogLog
// sketches"
static uint64_t hllCountCnt(uint8_t* buckets) {
  double  m = HLL_BUCKETS;
  int32_t buckethisto[64] = {0};
  hllBucketHisto(buckets, buckethisto);

  double z = m * hllTau((m - buckethisto[HLL_DATA_BITS + 1]) / (double)m);
  for (int j = HLL_DATA_BITS; j >= 1; --j) {
    z += buckethisto[j];
    z *= 0.5;
  }

  z += m * hllSigma(buckethisto[0] / (double)m);
  double E = (double)llroundl(HLL_ALPHA_INF * m * m / z);

  return (uint64_t)E;
}

int32_t hllFunction(SqlFunctionCtx* pCtx) {
  SHLLInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];

  int32_t type = pCol->info.type;
  int32_t bytes = pCol->info.bytes;

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;

  int32_t numOfElems = 0;
  if (IS_NULL_TYPE(type)) {
    goto _hll_over;
  }

  for (int32_t i = start; i < numOfRows + start; ++i) {
    if (pCol->hasNull && colDataIsNull_s(pCol, i)) {
      continue;
    }

    numOfElems++;

    char* data = colDataGetData(pCol, i);
    if (IS_VAR_DATA_TYPE(type)) {
      bytes = varDataLen(data);
      data = varDataVal(data);
    }

    int32_t index = 0;
    uint8_t count = hllCountNum(data, bytes, &index);
    uint8_t oldcount = pInfo->buckets[index];
    if (count > oldcount) {
      pInfo->buckets[index] = count;
    }
  }

_hll_over:
  pInfo->totalCount += numOfElems;

  if (pInfo->totalCount == 0 && !tsCountAlwaysReturnValue) {
    SET_VAL(GET_RES_INFO(pCtx), 0, 1);
  } else {
    SET_VAL(GET_RES_INFO(pCtx), 1, 1);
  }

  return TSDB_CODE_SUCCESS;
}

static void hllTransferInfo(SHLLInfo* pInput, SHLLInfo* pOutput) {
  for (int32_t k = 0; k < HLL_BUCKETS; ++k) {
    if (pOutput->buckets[k] < pInput->buckets[k]) {
      pOutput->buckets[k] = pInput->buckets[k];
    }
  }
  pOutput->totalCount += pInput->totalCount;
}

int32_t hllFunctionMerge(SqlFunctionCtx* pCtx) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];

  if (IS_NULL_TYPE(pCol->info.type)) {
    SET_VAL(GET_RES_INFO(pCtx), 0, 1);
    return TSDB_CODE_SUCCESS;
  }

  if (pCol->info.type != TSDB_DATA_TYPE_BINARY) {
    return TSDB_CODE_SUCCESS;
  }

  SHLLInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  int32_t start = pInput->startRowIndex;

  for (int32_t i = start; i < start + pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pCol, i)) continue;
    char*     data = colDataGetData(pCol, i);
    SHLLInfo* pInputInfo = (SHLLInfo*)varDataVal(data);
    hllTransferInfo(pInputInfo, pInfo);
  }

  if (pInfo->totalCount == 0 && !tsCountAlwaysReturnValue) {
    SET_VAL(GET_RES_INFO(pCtx), 0, 1);
  } else {
    SET_VAL(GET_RES_INFO(pCtx), 1, 1);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t hllFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pInfo = GET_RES_INFO(pCtx);

  SHLLInfo* pHllInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  pHllInfo->result = hllCountCnt(pHllInfo->buckets);
  if (tsCountAlwaysReturnValue && pHllInfo->result == 0) {
    pInfo->numOfRes = 1;
  }

  return functionFinalize(pCtx, pBlock);
}

int32_t hllPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SHLLInfo*            pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t              resultBytes = getHLLInfoSize();
  char*                res = taosMemoryCalloc(resultBytes + VARSTR_HEADER_SIZE, sizeof(char));

  if (NULL == res) {
    return terrno;
  }
  (void)memcpy(varDataVal(res), pInfo, resultBytes);
  varDataSetLen(res, resultBytes);

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  int32_t          code = TSDB_CODE_SUCCESS;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  if (NULL == pCol) {
    code = terrno;
    goto _exit;
  }

  code = colDataSetVal(pCol, pBlock->info.rows, res, false);

_exit:
  taosMemoryFree(res);
  return code;
}

int32_t hllCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SHLLInfo*            pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SHLLInfo*            pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  hllTransferInfo(pSBuf, pDBuf);
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
  return TSDB_CODE_SUCCESS;
}

bool getStateFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SStateInfo);
  return true;
}

static int8_t getStateOpType(char* opStr) {
  int8_t opType;
  if (strncasecmp(opStr, "LT", 2) == 0) {
    opType = STATE_OPER_LT;
  } else if (strncasecmp(opStr, "GT", 2) == 0) {
    opType = STATE_OPER_GT;
  } else if (strncasecmp(opStr, "LE", 2) == 0) {
    opType = STATE_OPER_LE;
  } else if (strncasecmp(opStr, "GE", 2) == 0) {
    opType = STATE_OPER_GE;
  } else if (strncasecmp(opStr, "NE", 2) == 0) {
    opType = STATE_OPER_NE;
  } else if (strncasecmp(opStr, "EQ", 2) == 0) {
    opType = STATE_OPER_EQ;
  } else {
    opType = STATE_OPER_INVALID;
  }

  return opType;
}

static bool checkStateOp(int8_t op, SColumnInfoData* pCol, int32_t index, SVariant param) {
  char* data = colDataGetData(pCol, index);
  switch (pCol->info.type) {
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t v = *(int8_t*)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      uint8_t v = *(uint8_t*)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t v = *(int16_t*)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t v = *(uint16_t*)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      int32_t v = *(int32_t*)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      uint32_t v = *(uint32_t*)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t v = *(int64_t*)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t v = *(uint64_t*)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float v = *(float*)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double v = *(double*)data;
      STATE_COMP(op, v, param);
      break;
    }
    default: {
      return false;
    }
  }
  return false;
}

int32_t stateCountFunction(SqlFunctionCtx* pCtx) {
  int32_t              code = TSDB_CODE_SUCCESS;
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SStateInfo*          pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  TSKEY*                tsList = (int64_t*)pInput->pPTS->pData;

  SColumnInfoData* pInputCol = pInput->pData[0];

  int32_t          numOfElems = 0;
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  int8_t op = getStateOpType(varDataVal(pCtx->param[1].param.pz));
  if (STATE_OPER_INVALID == op) {
    return 0;
  }

  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    if (pInfo->isPrevTsSet == true && tsList[i] == pInfo->prevTs) {
      return TSDB_CODE_FUNC_DUP_TIMESTAMP;
    } else {
      pInfo->prevTs = tsList[i];
    }

    pInfo->isPrevTsSet = true;
    numOfElems++;

    if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
      colDataSetNULL(pOutput, i);
      // handle selectivity
      if (pCtx->subsidiaries.num > 0) {
        code = appendSelectivityValue(pCtx, i, pCtx->offset + numOfElems - 1);
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
      }
      continue;
    }

    bool ret = checkStateOp(op, pInputCol, i, pCtx->param[2].param);

    int64_t output = -1;
    if (ret) {
      output = ++pInfo->count;
    } else {
      pInfo->count = 0;
    }
    code = colDataSetVal(pOutput, pCtx->offset + numOfElems - 1, (char*)&output, false);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }

    // handle selectivity
    if (pCtx->subsidiaries.num > 0) {
      code = appendSelectivityValue(pCtx, i, pCtx->offset + numOfElems - 1);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  }

  pResInfo->numOfRes = numOfElems;
  return TSDB_CODE_SUCCESS;
}

int32_t stateDurationFunction(SqlFunctionCtx* pCtx) {
  int32_t              code = TSDB_CODE_SUCCESS;
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SStateInfo*          pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  TSKEY*                tsList = (int64_t*)pInput->pPTS->pData;

  SColumnInfoData* pInputCol = pInput->pData[0];

  int32_t          numOfElems = 0;
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  // TODO: process timeUnit for different db precisions
  int32_t timeUnit = 1;
  if (pCtx->numOfParams == 5) {  // TODO: param number incorrect
    timeUnit = pCtx->param[3].param.i;
  }

  int8_t op = getStateOpType(varDataVal(pCtx->param[1].param.pz));
  if (STATE_OPER_INVALID == op) {
    return TSDB_CODE_INVALID_PARA;
  }

  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    if (pInfo->isPrevTsSet == true && tsList[i] == pInfo->prevTs) {
      return TSDB_CODE_FUNC_DUP_TIMESTAMP;
    } else {
      pInfo->prevTs = tsList[i];
    }

    pInfo->isPrevTsSet = true;
    numOfElems++;

    if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
      colDataSetNULL(pOutput, i);
      // handle selectivity
      if (pCtx->subsidiaries.num > 0) {
        code = appendSelectivityValue(pCtx, i, pCtx->offset + numOfElems - 1);
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
      }
      continue;
    }

    bool    ret = checkStateOp(op, pInputCol, i, pCtx->param[2].param);
    int64_t output = -1;
    if (ret) {
      if (pInfo->durationStart == 0) {
        output = 0;
        pInfo->durationStart = tsList[i];
      } else {
        output = (tsList[i] - pInfo->durationStart) / timeUnit;
      }
    } else {
      pInfo->durationStart = 0;
    }
    code = colDataSetVal(pOutput, pCtx->offset + numOfElems - 1, (char*)&output, false);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }

    // handle selectivity
    if (pCtx->subsidiaries.num > 0) {
      code = appendSelectivityValue(pCtx, i, pCtx->offset + numOfElems - 1);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  }

  pResInfo->numOfRes = numOfElems;
  return TSDB_CODE_SUCCESS;
}

bool getCsumFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SSumRes);
  return true;
}

int32_t csumFunction(SqlFunctionCtx* pCtx) {
  int32_t              code = TSDB_CODE_SUCCESS;
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SSumRes*             pSumRes = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  TSKEY*                tsList = (int64_t*)pInput->pPTS->pData;

  SColumnInfoData* pInputCol = pInput->pData[0];
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  int32_t numOfElems = 0;
  int32_t type = pInputCol->info.type;
  int32_t startOffset = pCtx->offset;
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    if (pSumRes->isPrevTsSet == true && tsList[i] == pSumRes->prevTs) {
      return TSDB_CODE_FUNC_DUP_TIMESTAMP;
    } else {
      pSumRes->prevTs = tsList[i];
    }
    pSumRes->isPrevTsSet = true;

    int32_t pos = startOffset + numOfElems;
    if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
      // colDataSetNULL(pOutput, i);
      continue;
    }

    char* data = colDataGetData(pInputCol, i);
    if (IS_SIGNED_NUMERIC_TYPE(type)) {
      int64_t v;
      GET_TYPED_DATA(v, int64_t, type, data);
      pSumRes->isum += v;
      code = colDataSetVal(pOutput, pos, (char*)&pSumRes->isum, false);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      uint64_t v;
      GET_TYPED_DATA(v, uint64_t, type, data);
      pSumRes->usum += v;
      code = colDataSetVal(pOutput, pos, (char*)&pSumRes->usum, false);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    } else if (IS_FLOAT_TYPE(type)) {
      double v;
      GET_TYPED_DATA(v, double, type, data);
      pSumRes->dsum += v;
      // check for overflow
      if (isinf(pSumRes->dsum) || isnan(pSumRes->dsum)) {
        colDataSetNULL(pOutput, pos);
      } else {
        code = colDataSetVal(pOutput, pos, (char*)&pSumRes->dsum, false);
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
      }
    }

    // handle selectivity
    if (pCtx->subsidiaries.num > 0) {
      code = appendSelectivityValue(pCtx, i, pos);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }

    numOfElems++;
  }

  pResInfo->numOfRes = numOfElems;
  return TSDB_CODE_SUCCESS;
}

bool getMavgFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SMavgInfo) + MAVG_MAX_POINTS_NUM * sizeof(double);
  return true;
}

int32_t mavgFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (pResultInfo->initialized) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS != functionSetup(pCtx, pResultInfo)) {
    return TSDB_CODE_FUNC_SETUP_ERROR;
  }

  SMavgInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  pInfo->pos = 0;
  pInfo->sum = 0;
  pInfo->prevTs = -1;
  pInfo->isPrevTsSet = false;
  pInfo->numOfPoints = pCtx->param[1].param.i;
  if (pInfo->numOfPoints < 1 || pInfo->numOfPoints > MAVG_MAX_POINTS_NUM) {
    return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
  }
  pInfo->pointsMeet = false;

  return TSDB_CODE_SUCCESS;
}

int32_t mavgFunction(SqlFunctionCtx* pCtx) {
  int32_t              code = TSDB_CODE_SUCCESS;
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SMavgInfo*           pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  TSKEY*                tsList = (int64_t*)pInput->pPTS->pData;

  SColumnInfoData* pInputCol = pInput->pData[0];
  SColumnInfoData* pTsOutput = pCtx->pTsOutput;
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  int32_t numOfElems = 0;
  int32_t type = pInputCol->info.type;
  int32_t startOffset = pCtx->offset;
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    if (pInfo->isPrevTsSet == true && tsList[i] == pInfo->prevTs) {
      return TSDB_CODE_FUNC_DUP_TIMESTAMP;
    } else {
      pInfo->prevTs = tsList[i];
    }
    pInfo->isPrevTsSet = true;

    int32_t pos = startOffset + numOfElems;
    if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
      // colDataSetNULL(pOutput, i);
      continue;
    }

    char*  data = colDataGetData(pInputCol, i);
    double v;
    GET_TYPED_DATA(v, double, type, data);

    if (!pInfo->pointsMeet && (pInfo->pos < pInfo->numOfPoints - 1)) {
      pInfo->points[pInfo->pos] = v;
      pInfo->sum += v;
    } else {
      if (!pInfo->pointsMeet && (pInfo->pos == pInfo->numOfPoints - 1)) {
        pInfo->sum += v;
        pInfo->pointsMeet = true;
      } else {
        pInfo->sum = pInfo->sum + v - pInfo->points[pInfo->pos];
      }

      pInfo->points[pInfo->pos] = v;
      double result = pInfo->sum / pInfo->numOfPoints;
      // check for overflow
      if (isinf(result) || isnan(result)) {
        colDataSetNULL(pOutput, pos);
      } else {
        code = colDataSetVal(pOutput, pos, (char*)&result, false);
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
      }

      // handle selectivity
      if (pCtx->subsidiaries.num > 0) {
        code = appendSelectivityValue(pCtx, i, pos);
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
      }

      numOfElems++;
    }

    pInfo->pos++;
    if (pInfo->pos == pInfo->numOfPoints) {
      pInfo->pos = 0;
    }
  }

  pResInfo->numOfRes = numOfElems;
  return TSDB_CODE_SUCCESS;
}

static SSampleInfo* getSampleOutputInfo(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SSampleInfo*         pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  pInfo->data = (char*)pInfo + sizeof(SSampleInfo);
  pInfo->tuplePos = (STuplePos*)((char*)pInfo + sizeof(SSampleInfo) + pInfo->samples * pInfo->colBytes);

  return pInfo;
}

bool getSampleFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  SColumnNode* pCol = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
  SValueNode*  pVal = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);
  int32_t      numOfSamples = pVal->datum.i;
  pEnv->calcMemSize = sizeof(SSampleInfo) + numOfSamples * (pCol->node.resType.bytes + sizeof(STuplePos));
  return true;
}

int32_t sampleFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (pResultInfo->initialized) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS != functionSetup(pCtx, pResultInfo)) {
    return TSDB_CODE_FUNC_SETUP_ERROR;
  }

  taosSeedRand(taosSafeRand());

  SSampleInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  pInfo->samples = pCtx->param[1].param.i;
  pInfo->totalPoints = 0;
  pInfo->numSampled = 0;
  pInfo->colType = pCtx->resDataInfo.type;
  pInfo->colBytes = pCtx->resDataInfo.bytes;
  pInfo->nullTuplePos.pageId = -1;
  pInfo->nullTupleSaved = false;
  pInfo->data = (char*)pInfo + sizeof(SSampleInfo);
  pInfo->tuplePos = (STuplePos*)((char*)pInfo + sizeof(SSampleInfo) + pInfo->samples * pInfo->colBytes);

  return TSDB_CODE_SUCCESS;
}

static void sampleAssignResult(SSampleInfo* pInfo, char* data, int32_t index) {
  assignVal(pInfo->data + index * pInfo->colBytes, data, pInfo->colBytes, pInfo->colType);
}

static int32_t doReservoirSample(SqlFunctionCtx* pCtx, SSampleInfo* pInfo, char* data, int32_t index) {
  pInfo->totalPoints++;
  if (pInfo->numSampled < pInfo->samples) {
    sampleAssignResult(pInfo, data, pInfo->numSampled);
    if (pCtx->subsidiaries.num > 0) {
      int32_t code = saveTupleData(pCtx, index, pCtx->pSrcBlock, &pInfo->tuplePos[pInfo->numSampled]);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    pInfo->numSampled++;
  } else {
    int32_t j = taosRand() % (pInfo->totalPoints);
    if (j < pInfo->samples) {
      sampleAssignResult(pInfo, data, j);
      if (pCtx->subsidiaries.num > 0) {
        int32_t code = updateTupleData(pCtx, index, pCtx->pSrcBlock, &pInfo->tuplePos[j]);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t sampleFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SSampleInfo*         pInfo = getSampleOutputInfo(pCtx);

  SInputColumnInfoData* pInput = &pCtx->input;

  SColumnInfoData* pInputCol = pInput->pData[0];
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    if (colDataIsNull_s(pInputCol, i)) {
      continue;
    }

    char*   data = colDataGetData(pInputCol, i);
    int32_t code = doReservoirSample(pCtx, pInfo, data, i);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  if (pInfo->numSampled == 0 && pCtx->subsidiaries.num > 0 && !pInfo->nullTupleSaved) {
    int32_t code = saveTupleData(pCtx, pInput->startRowIndex, pCtx->pSrcBlock, &pInfo->nullTuplePos);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pInfo->nullTupleSaved = true;
  }

  SET_VAL(pResInfo, pInfo->numSampled, pInfo->numSampled);
  return TSDB_CODE_SUCCESS;
}

int32_t sampleFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t              code = TSDB_CODE_SUCCESS;
  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);

  SSampleInfo* pInfo = getSampleOutputInfo(pCtx);
  pEntryInfo->complete = true;

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  if (NULL == pCol) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  int32_t currentRow = pBlock->info.rows;
  if (pInfo->numSampled == 0) {
    colDataSetNULL(pCol, currentRow);
    code = setSelectivityValue(pCtx, pBlock, &pInfo->nullTuplePos, currentRow);
    return code;
  }
  for (int32_t i = 0; i < pInfo->numSampled; ++i) {
    code = colDataSetVal(pCol, currentRow + i, pInfo->data + i * pInfo->colBytes, false);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    code = setSelectivityValue(pCtx, pBlock, &pInfo->tuplePos[i], currentRow + i);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  return code;
}

bool getTailFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
#if 0
  SColumnNode* pCol = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
  SValueNode*  pVal = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);
  int32_t      numOfPoints = pVal->datum.i;
  pEnv->calcMemSize = sizeof(STailInfo) + numOfPoints * (POINTER_BYTES + sizeof(STailItem) + pCol->node.resType.bytes);
#endif
  return true;
}

int32_t tailFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
#if 0
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  STailInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  pInfo->numAdded = 0;
  pInfo->numOfPoints = pCtx->param[1].param.i;
  if (pCtx->numOfParams == 4) {
    pInfo->offset = pCtx->param[2].param.i;
  } else {
    pInfo->offset = 0;
  }
  pInfo->colType = pCtx->resDataInfo.type;
  pInfo->colBytes = pCtx->resDataInfo.bytes;
  if ((pInfo->numOfPoints < 1 || pInfo->numOfPoints > TAIL_MAX_POINTS_NUM) ||
      (pInfo->numOfPoints < 0 || pInfo->numOfPoints > TAIL_MAX_OFFSET)) {
    return false;
  }

  pInfo->pItems = (STailItem**)((char*)pInfo + sizeof(STailInfo));
  char* pItem = (char*)pInfo->pItems + pInfo->numOfPoints * POINTER_BYTES;

  size_t unitSize = sizeof(STailItem) + pInfo->colBytes;
  for (int32_t i = 0; i < pInfo->numOfPoints; ++i) {
    pInfo->pItems[i] = (STailItem*)(pItem + i * unitSize);
    pInfo->pItems[i]->isNull = false;
  }
#endif

  return TSDB_CODE_SUCCESS;
}

static void tailAssignResult(STailItem* pItem, char* data, int32_t colBytes, TSKEY ts, bool isNull) {
#if 0
  pItem->timestamp = ts;
  if (isNull) {
    pItem->isNull = true;
  } else {
    pItem->isNull = false;
    memcpy(pItem->data, data, colBytes);
  }
#endif
}

#if 0
static int32_t tailCompFn(const void* p1, const void* p2, const void* param) {
  STailItem* d1 = *(STailItem**)p1;
  STailItem* d2 = *(STailItem**)p2;
  return compareInt64Val(&d1->timestamp, &d2->timestamp);
}

static void doTailAdd(STailInfo* pInfo, char* data, TSKEY ts, bool isNull) {
  STailItem** pList = pInfo->pItems;
  if (pInfo->numAdded < pInfo->numOfPoints) {
    tailAssignResult(pList[pInfo->numAdded], data, pInfo->colBytes, ts, isNull);
    taosheapsort((void*)pList, sizeof(STailItem**), pInfo->numAdded + 1, NULL, tailCompFn, 0);
    pInfo->numAdded++;
  } else if (pList[0]->timestamp < ts) {
    tailAssignResult(pList[0], data, pInfo->colBytes, ts, isNull);
    taosheapadjust((void*)pList, sizeof(STailItem**), 0, pInfo->numOfPoints - 1, NULL, tailCompFn, NULL, 0);
  }
}
#endif

int32_t tailFunction(SqlFunctionCtx* pCtx) {
#if 0
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  STailInfo*           pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  TSKEY*                tsList = (int64_t*)pInput->pPTS->pData;

  SColumnInfoData* pInputCol = pInput->pData[0];
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  int32_t startOffset = pCtx->offset;
  if (pInfo->offset >= pInput->numOfRows) {
    return 0;
  } else {
    pInfo->numOfPoints = TMIN(pInfo->numOfPoints, pInput->numOfRows - pInfo->offset);
  }
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex - pInfo->offset; i += 1) {
    char* data = colDataGetData(pInputCol, i);
    doTailAdd(pInfo, data, tsList[i], colDataIsNull_s(pInputCol, i));
  }

  taosqsort(pInfo->pItems, pInfo->numOfPoints, POINTER_BYTES, NULL, tailCompFn);

  for (int32_t i = 0; i < pInfo->numOfPoints; ++i) {
    int32_t    pos = startOffset + i;
    STailItem* pItem = pInfo->pItems[i];
    if (pItem->isNull) {
      colDataSetNULL(pOutput, pos);
    } else {
      colDataSetVal(pOutput, pos, pItem->data, false);
    }
  }

  return pInfo->numOfPoints;
#endif
  return 0;
}

int32_t tailFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
#if 0
  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);
  STailInfo*           pInfo = GET_ROWCELL_INTERBUF(pEntryInfo);
  pEntryInfo->complete = true;

  int32_t type = pCtx->input.pData[0]->info.type;
  int32_t slotId = pCtx->pExpr->base.resSchema.slotId;

  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  // todo assign the tag value and the corresponding row data
  int32_t currentRow = pBlock->info.rows;
  for (int32_t i = 0; i < pEntryInfo->numOfRes; ++i) {
    STailItem* pItem = pInfo->pItems[i];
    colDataSetVal(pCol, currentRow, pItem->data, false);
    currentRow += 1;
  }

  return pEntryInfo->numOfRes;
#endif
  return 0;
}

bool getUniqueFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
#if 0
  pEnv->calcMemSize = sizeof(SUniqueInfo) + UNIQUE_MAX_RESULT_SIZE;
#endif
  return true;
}

int32_t uniqueFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo) {
#if 0
  if (!functionSetup(pCtx, pResInfo)) {
    return false;
  }

  SUniqueInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  pInfo->numOfPoints = 0;
  pInfo->colType = pCtx->resDataInfo.type;
  pInfo->colBytes = pCtx->resDataInfo.bytes;
  if (pInfo->pHash != NULL) {
    taosHashClear(pInfo->pHash);
  } else {
    pInfo->pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  }
#endif
  return TSDB_CODE_SUCCESS;
}

#if 0
static void doUniqueAdd(SUniqueInfo* pInfo, char* data, TSKEY ts, bool isNull) {
  // handle null elements
  if (isNull == true) {
    int32_t      size = sizeof(SUniqueItem) + pInfo->colBytes;
    SUniqueItem* pItem = (SUniqueItem*)(pInfo->pItems + pInfo->numOfPoints * size);
    if (pInfo->hasNull == false && pItem->isNull == false) {
      pItem->timestamp = ts;
      pItem->isNull = true;
      pInfo->numOfPoints++;
      pInfo->hasNull = true;
    } else if (pItem->timestamp > ts && pItem->isNull == true) {
      pItem->timestamp = ts;
    }
    return;
  }

  int32_t      hashKeyBytes = IS_VAR_DATA_TYPE(pInfo->colType) ? varDataTLen(data) : pInfo->colBytes;
  SUniqueItem* pHashItem = taosHashGet(pInfo->pHash, data, hashKeyBytes);
  if (pHashItem == NULL) {
    int32_t      size = sizeof(SUniqueItem) + pInfo->colBytes;
    SUniqueItem* pItem = (SUniqueItem*)(pInfo->pItems + pInfo->numOfPoints * size);
    pItem->timestamp = ts;
    memcpy(pItem->data, data, pInfo->colBytes);

    taosHashPut(pInfo->pHash, data, hashKeyBytes, (char*)pItem, sizeof(SUniqueItem*));
    pInfo->numOfPoints++;
  } else if (pHashItem->timestamp > ts) {
    pHashItem->timestamp = ts;
  }
}
#endif

int32_t uniqueFunction(SqlFunctionCtx* pCtx) {
#if 0
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SUniqueInfo*         pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  TSKEY*                tsList = (int64_t*)pInput->pPTS->pData;

  SColumnInfoData* pInputCol = pInput->pData[0];
  SColumnInfoData* pTsOutput = pCtx->pTsOutput;
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  int32_t startOffset = pCtx->offset;
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
    char* data = colDataGetData(pInputCol, i);
    doUniqueAdd(pInfo, data, tsList[i], colDataIsNull_s(pInputCol, i));

    if (sizeof(SUniqueInfo) + pInfo->numOfPoints * (sizeof(SUniqueItem) + pInfo->colBytes) >= UNIQUE_MAX_RESULT_SIZE) {
      taosHashCleanup(pInfo->pHash);
      return 0;
    }
  }

  for (int32_t i = 0; i < pInfo->numOfPoints; ++i) {
    SUniqueItem* pItem = (SUniqueItem*)(pInfo->pItems + i * (sizeof(SUniqueItem) + pInfo->colBytes));
    if (pItem->isNull == true) {
      colDataSetNULL(pOutput, i);
    } else {
      colDataSetVal(pOutput, i, pItem->data, false);
    }
    if (pTsOutput != NULL) {
      colDataSetInt64(pTsOutput, i, &pItem->timestamp);
    }
  }

  return pInfo->numOfPoints;
#endif
  return 0;
}

bool getModeFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SModeInfo);
  return true;
}

int32_t modeFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo) {
  if (pResInfo->initialized) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS != functionSetup(pCtx, pResInfo)) {
    return TSDB_CODE_FUNC_SETUP_ERROR;
  }

  SModeInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  pInfo->colType = pCtx->resDataInfo.type;
  pInfo->colBytes = pCtx->resDataInfo.bytes;
  if (pInfo->pHash != NULL) {
    taosHashClear(pInfo->pHash);
  } else {
    pInfo->pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
    if (NULL == pInfo->pHash) {
      return terrno;
    }
  }
  pInfo->nullTupleSaved = false;
  pInfo->nullTuplePos.pageId = -1;

  pInfo->buf = taosMemoryMalloc(pInfo->colBytes);
  if (NULL == pInfo->buf) {
    taosHashCleanup(pInfo->pHash);
    pInfo->pHash = NULL;
    return terrno;
  }
  pCtx->needCleanup = true;
  return TSDB_CODE_SUCCESS;
}

static void modeFunctionCleanup(SModeInfo * pInfo) {
  taosHashCleanup(pInfo->pHash);
  pInfo->pHash = NULL;
  taosMemoryFreeClear(pInfo->buf);
}

void modeFunctionCleanupExt(SqlFunctionCtx* pCtx) {
  if (pCtx == NULL || GET_RES_INFO(pCtx) == NULL || GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx)) == NULL) {
    return;
  }
  modeFunctionCleanup(GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx)));
}

static int32_t saveModeTupleData(SqlFunctionCtx* pCtx, char* data, SModeInfo *pInfo, STuplePos* pPos) {
  if (IS_VAR_DATA_TYPE(pInfo->colType)) {
    (void)memcpy(pInfo->buf, data, varDataTLen(data));
  } else {
    (void)memcpy(pInfo->buf, data, pInfo->colBytes);
  }

  return doSaveTupleData(&pCtx->saveHandle, pInfo->buf, pInfo->colBytes, NULL, pPos, pCtx->pStore);
}

static int32_t doModeAdd(SModeInfo* pInfo, int32_t rowIndex, SqlFunctionCtx* pCtx, char* data) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t hashKeyBytes = IS_STR_DATA_TYPE(pInfo->colType) ? varDataTLen(data) : pInfo->colBytes;

  SModeItem* pHashItem = (SModeItem *)taosHashGet(pInfo->pHash, data, hashKeyBytes);
  if (pHashItem == NULL) {
    int32_t    size = sizeof(SModeItem);
    SModeItem  item = {0};

    item.count += 1;
    code = saveModeTupleData(pCtx, data, pInfo, &item.dataPos);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (pCtx->subsidiaries.num > 0) {
      code = saveTupleData(pCtx, rowIndex, pCtx->pSrcBlock, &item.tuplePos);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    code = taosHashPut(pInfo->pHash, data, hashKeyBytes, &item, sizeof(SModeItem));
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  } else {
    pHashItem->count += 1;
    if (pCtx->subsidiaries.num > 0) {
      code = updateTupleData(pCtx, rowIndex, pCtx->pSrcBlock, &pHashItem->tuplePos);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }

  return code;
}

int32_t modeFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SModeInfo*           pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;

  SColumnInfoData* pInputCol = pInput->pData[0];
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  int32_t numOfElems = 0;
  int32_t startOffset = pCtx->offset;
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
    if (colDataIsNull_s(pInputCol, i)) {
      continue;
    }
    numOfElems++;

    char*   data = colDataGetData(pInputCol, i);
    int32_t code = doModeAdd(pInfo, i, pCtx, data);
    if (code != TSDB_CODE_SUCCESS) {
      modeFunctionCleanup(pInfo);
      return code;
    }
  }

  if (numOfElems == 0 && pCtx->subsidiaries.num > 0 && !pInfo->nullTupleSaved) {
    int32_t code = saveTupleData(pCtx, pInput->startRowIndex, pCtx->pSrcBlock, &pInfo->nullTuplePos);
    if (code != TSDB_CODE_SUCCESS) {
      modeFunctionCleanup(pInfo);
      return code;
    }
    pInfo->nullTupleSaved = true;
  }

  SET_VAL(pResInfo, numOfElems, 1);

  return TSDB_CODE_SUCCESS;
}

int32_t modeFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t              code = TSDB_CODE_SUCCESS;
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SModeInfo*           pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  int32_t              slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData*     pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  int32_t              currentRow = pBlock->info.rows;
  if (NULL == pCol) {
    modeFunctionCleanup(pInfo);
    return TSDB_CODE_OUT_OF_RANGE;
  }

  STuplePos resDataPos, resTuplePos;
  int32_t maxCount = 0;

  void *pIter = taosHashIterate(pInfo->pHash, NULL);
  while (pIter != NULL) {
    SModeItem *pItem = (SModeItem *)pIter;
    if (pItem->count >= maxCount) {
      maxCount = pItem->count;
      resDataPos = pItem->dataPos;
      resTuplePos = pItem->tuplePos;
    }

    pIter = taosHashIterate(pInfo->pHash, pIter);
  }

  if (maxCount != 0) {
    char* pData = NULL;
    code = loadTupleData(pCtx, &resDataPos, &pData);
    if (pData == NULL || TSDB_CODE_SUCCESS != code) {
      code = terrno = TSDB_CODE_NOT_FOUND;
      qError("Load tuple data failed since %s, groupId:%" PRIu64 ", ts:%" PRId64, terrstr(),
             resDataPos.streamTupleKey.groupId, resDataPos.streamTupleKey.ts);
      modeFunctionCleanup(pInfo);
      return code;
    }

    code = colDataSetVal(pCol, currentRow, pData, false);
    if (TSDB_CODE_SUCCESS != code) {
      modeFunctionCleanup(pInfo);
     return code;
    }
    code = setSelectivityValue(pCtx, pBlock, &resTuplePos, currentRow);
  } else {
    colDataSetNULL(pCol, currentRow);
    code = setSelectivityValue(pCtx, pBlock, &pInfo->nullTuplePos, currentRow);
  }

  modeFunctionCleanup(pInfo);

  return code;
}

bool getTwaFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(STwaInfo);
  return true;
}

int32_t twaFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (pResultInfo->initialized) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS != functionSetup(pCtx, pResultInfo)) {
    return TSDB_CODE_FUNC_SETUP_ERROR;
  }

  STwaInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  pInfo->numOfElems = 0;
  pInfo->p.key = INT64_MIN;
  pInfo->win = TSWINDOW_INITIALIZER;
  return TSDB_CODE_SUCCESS;
}

static double twa_get_area(SPoint1 s, SPoint1 e) {
  if (e.key == INT64_MAX || s.key == INT64_MIN) {
    return 0;
  }

  if ((s.val >= 0 && e.val >= 0) || (s.val <= 0 && e.val <= 0)) {
    return (s.val + e.val) * (e.key - s.key) / 2;
  }

  double x = (s.key * e.val - e.key * s.val) / (e.val - s.val);
  double val = (s.val * (x - s.key) + e.val * (e.key - x)) / 2;
  return val;
}

int32_t twaFunction(SqlFunctionCtx* pCtx) {
  int32_t               code = TSDB_CODE_SUCCESS;
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  STwaInfo*            pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  SPoint1*             last = &pInfo->p;

  if (IS_NULL_TYPE(pInputCol->info.type)) {
    pInfo->numOfElems = 0;
    goto _twa_over;
  }

  funcInputUpdate(pCtx);
  SFuncInputRow row = {0};
  bool          result = false;
  if (pCtx->start.key != INT64_MIN && last->key == INT64_MIN) {
    while (1) {
      code = funcInputGetNextRow(pCtx, &row, &result);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      if (!result) {
        break;
      }
      if (row.isDataNull) {
        continue;
      }

      last->key = row.ts;

      GET_TYPED_DATA(last->val, double, pInputCol->info.type, row.pData);

      pInfo->dOutput += twa_get_area(pCtx->start, *last);
      pInfo->win.skey = pCtx->start.key;
      pInfo->numOfElems++;
      break;
    }
  } else if (pInfo->p.key == INT64_MIN) {
    while (1) {
      code = funcInputGetNextRow(pCtx, &row, &result);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      if (!result) {
        break;
      }
      if (row.isDataNull) {
        continue;
      }

      last->key = row.ts;

      GET_TYPED_DATA(last->val, double, pInputCol->info.type, row.pData);

      pInfo->win.skey = last->key;
      pInfo->numOfElems++;
      break;
    }
  }

  SPoint1 st = {0};

  // calculate the value of
  while (1) {
    code = funcInputGetNextRow(pCtx, &row, &result);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    if (!result) {
      break;
    }
    if (row.isDataNull) {
      continue;
    }
    pInfo->numOfElems++;
    switch (pInputCol->info.type) {
      case TSDB_DATA_TYPE_TINYINT: {
        INIT_INTP_POINT(st, row.ts, *(int8_t*)row.pData);
        break;
      }
      case TSDB_DATA_TYPE_SMALLINT: {
        INIT_INTP_POINT(st, row.ts, *(int16_t*)row.pData);
        break;
      }
      case TSDB_DATA_TYPE_INT: {
        INIT_INTP_POINT(st, row.ts, *(int32_t*)row.pData);
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        INIT_INTP_POINT(st, row.ts, *(int64_t*)row.pData);
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        INIT_INTP_POINT(st, row.ts, *(float_t*)row.pData);
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        INIT_INTP_POINT(st, row.ts, *(double*)row.pData);
        break;
      }
      case TSDB_DATA_TYPE_UTINYINT: {
        INIT_INTP_POINT(st, row.ts, *(uint8_t*)row.pData);
        break;
      }
      case TSDB_DATA_TYPE_USMALLINT: {
        INIT_INTP_POINT(st, row.ts, *(uint16_t*)row.pData);
        break;
      }
      case TSDB_DATA_TYPE_UINT: {
        INIT_INTP_POINT(st, row.ts, *(uint32_t*)row.pData);
        break;
      }
      case TSDB_DATA_TYPE_UBIGINT: {
        INIT_INTP_POINT(st, row.ts, *(uint64_t*)row.pData);
        break;
      }
      default: {
        return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
      }
    }
    if (pInfo->p.key == st.key) {
      return TSDB_CODE_FUNC_DUP_TIMESTAMP;
    }

    pInfo->dOutput += twa_get_area(pInfo->p, st);
    pInfo->p = st;
  }

  // the last interpolated time window value
  if (pCtx->end.key != INT64_MIN) {
    pInfo->dOutput += twa_get_area(pInfo->p, pCtx->end);
    pInfo->p = pCtx->end;
    pInfo->numOfElems += 1;
  }

  pInfo->win.ekey = pInfo->p.key;

_twa_over:
  SET_VAL(pResInfo, 1, 1);
  return TSDB_CODE_SUCCESS;
}

/*
 * To copy the input to interResBuf to avoid the input buffer space be over writen
 * by next input data. The TWA function only applies to each table, so no merge procedure
 * is required, we simply copy to the resut ot interResBuffer.
 */
// void twa_function_copy(SQLFunctionCtx *pCtx) {
//   SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
//
//   memcpy(GET_ROWCELL_INTERBUF(pResInfo), pCtx->pInput, (size_t)pCtx->inputBytes);
//   pResInfo->hasResult = ((STwaInfo *)pCtx->pInput)->hasResult;
// }

int32_t twaFinalize(struct SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  STwaInfo* pInfo = (STwaInfo*)GET_ROWCELL_INTERBUF(pResInfo);
  if (pInfo->numOfElems == 0) {
    pResInfo->numOfRes = 0;
  } else {
    if (pInfo->win.ekey == pInfo->win.skey) {
      pInfo->dTwaRes = pInfo->p.val;
    } else if (pInfo->win.ekey == INT64_MAX || pInfo->win.skey == INT64_MIN) {  // no data in timewindow
      pInfo->dTwaRes = 0;
    } else {
      pInfo->dTwaRes = pInfo->dOutput / (pInfo->win.ekey - pInfo->win.skey);
    }

    pResInfo->numOfRes = 1;
  }

  return functionFinalize(pCtx, pBlock);
}

int32_t blockDistSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (pResultInfo->initialized) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS != functionSetup(pCtx, pResultInfo)) {
    return TSDB_CODE_FUNC_SETUP_ERROR;
  }

  STableBlockDistInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  pInfo->minRows = INT32_MAX;
  return TSDB_CODE_SUCCESS;
}

int32_t blockDistFunction(SqlFunctionCtx* pCtx) {
  const int32_t BLOCK_DIST_RESULT_ROWS = 25;

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];
  SResultRowEntryInfo*  pResInfo = GET_RES_INFO(pCtx);
  STableBlockDistInfo*  pDistInfo = GET_ROWCELL_INTERBUF(pResInfo);

  STableBlockDistInfo p1 = {0};
  if (tDeserializeBlockDistInfo(varDataVal(pInputCol->pData), varDataLen(pInputCol->pData), &p1) < 0) {
    qError("failed to deserialize block dist info");
    return TSDB_CODE_FAILED;
  }

  pDistInfo->numOfBlocks += p1.numOfBlocks;
  pDistInfo->numOfTables += p1.numOfTables;
  pDistInfo->numOfInmemRows += p1.numOfInmemRows;
  pDistInfo->numOfSttRows += p1.numOfSttRows;
  pDistInfo->totalSize += p1.totalSize;
  pDistInfo->totalRows += p1.totalRows;
  pDistInfo->numOfFiles += p1.numOfFiles;

  pDistInfo->defMinRows = p1.defMinRows;
  pDistInfo->defMaxRows = p1.defMaxRows;
  pDistInfo->rowSize = p1.rowSize;

  if (pDistInfo->minRows > p1.minRows) {
    pDistInfo->minRows = p1.minRows;
  }
  if (pDistInfo->maxRows < p1.maxRows) {
    pDistInfo->maxRows = p1.maxRows;
  }
  pDistInfo->numOfVgroups += (p1.numOfTables != 0 ? 1 : 0);
  for (int32_t i = 0; i < tListLen(pDistInfo->blockRowsHisto); ++i) {
    pDistInfo->blockRowsHisto[i] += p1.blockRowsHisto[i];
  }

  pResInfo->numOfRes = BLOCK_DIST_RESULT_ROWS;  // default output rows
  return TSDB_CODE_SUCCESS;
}

int32_t tSerializeBlockDistInfo(void* buf, int32_t bufLen, const STableBlockDistInfo* pInfo) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeU32(&encoder, pInfo->rowSize));

  TAOS_CHECK_EXIT(tEncodeU16(&encoder, pInfo->numOfFiles));
  TAOS_CHECK_EXIT(tEncodeU32(&encoder, pInfo->numOfBlocks));
  TAOS_CHECK_EXIT(tEncodeU32(&encoder, pInfo->numOfTables));

  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pInfo->totalSize));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pInfo->totalRows));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pInfo->maxRows));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pInfo->minRows));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pInfo->defMaxRows));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pInfo->defMinRows));
  TAOS_CHECK_EXIT(tEncodeU32(&encoder, pInfo->numOfInmemRows));
  TAOS_CHECK_EXIT(tEncodeU32(&encoder, pInfo->numOfSttRows));
  TAOS_CHECK_EXIT(tEncodeU32(&encoder, pInfo->numOfVgroups));

  for (int32_t i = 0; i < tListLen(pInfo->blockRowsHisto); ++i) {
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pInfo->blockRowsHisto[i]));
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeBlockDistInfo(void* buf, int32_t bufLen, STableBlockDistInfo* pInfo) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeU32(&decoder, &pInfo->rowSize));

  TAOS_CHECK_EXIT(tDecodeU16(&decoder, &pInfo->numOfFiles));
  TAOS_CHECK_EXIT(tDecodeU32(&decoder, &pInfo->numOfBlocks));
  TAOS_CHECK_EXIT(tDecodeU32(&decoder, &pInfo->numOfTables));

  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pInfo->totalSize));
  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pInfo->totalRows));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pInfo->maxRows));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pInfo->minRows));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pInfo->defMaxRows));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pInfo->defMinRows));
  TAOS_CHECK_EXIT(tDecodeU32(&decoder, &pInfo->numOfInmemRows));
  TAOS_CHECK_EXIT(tDecodeU32(&decoder, &pInfo->numOfSttRows));
  TAOS_CHECK_EXIT(tDecodeU32(&decoder, &pInfo->numOfVgroups));

  for (int32_t i = 0; i < tListLen(pInfo->blockRowsHisto); ++i) {
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pInfo->blockRowsHisto[i]));
  }

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t blockDistFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  STableBlockDistInfo* pData = GET_ROWCELL_INTERBUF(pResInfo);

  SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, 0);
  if (NULL == pColInfo) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  if (pData->totalRows == 0) {
    pData->minRows = 0;
  }

  int32_t row = 0;
  char    st[256] = {0};
  double  averageSize = 0;
  if (pData->numOfBlocks != 0) {
    averageSize = ((double)pData->totalSize) / pData->numOfBlocks;
  }
  uint64_t totalRawSize = pData->totalRows * pData->rowSize;
  double   compRatio = 0;
  if (totalRawSize != 0) {
    compRatio = pData->totalSize * 100 / (double)totalRawSize;
  }

  int32_t len = tsnprintf(varDataVal(st), sizeof(st) - VARSTR_HEADER_SIZE,
                        "Total_Blocks=[%d] Total_Size=[%.2f KiB] Average_size=[%.2f KiB] Compression_Ratio=[%.2f %c]",
                        pData->numOfBlocks, pData->totalSize / 1024.0, averageSize / 1024.0, compRatio, '%');

  varDataSetLen(st, len);
  int32_t code = colDataSetVal(pColInfo, row++, st, false);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  int64_t avgRows = 0;
  if (pData->numOfBlocks > 0) {
    avgRows = pData->totalRows / pData->numOfBlocks;
  }

  len = tsnprintf(varDataVal(st), sizeof(st) - VARSTR_HEADER_SIZE, "Block_Rows=[%" PRId64 "] MinRows=[%d] MaxRows=[%d] AvgRows=[%" PRId64 "]",
                pData->totalRows, pData->minRows, pData->maxRows, avgRows);
  varDataSetLen(st, len);
  code = colDataSetVal(pColInfo, row++, st, false);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  len = tsnprintf(varDataVal(st), sizeof(st) - VARSTR_HEADER_SIZE, "Inmem_Rows=[%d] Stt_Rows=[%d] ", pData->numOfInmemRows, pData->numOfSttRows);
  varDataSetLen(st, len);
  code = colDataSetVal(pColInfo, row++, st, false);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  len = tsnprintf(varDataVal(st), sizeof(st) - VARSTR_HEADER_SIZE, "Total_Tables=[%d] Total_Filesets=[%d] Total_Vgroups=[%d]", pData->numOfTables,
                pData->numOfFiles, pData->numOfVgroups);

  varDataSetLen(st, len);
  code = colDataSetVal(pColInfo, row++, st, false);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  len = tsnprintf(varDataVal(st), sizeof(st) - VARSTR_HEADER_SIZE,
                "--------------------------------------------------------------------------------");
  varDataSetLen(st, len);
  code = colDataSetVal(pColInfo, row++, st, false);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  int32_t maxVal = 0;
  int32_t minVal = INT32_MAX;
  for (int32_t i = 0; i < tListLen(pData->blockRowsHisto); ++i) {
    if (maxVal < pData->blockRowsHisto[i]) {
      maxVal = pData->blockRowsHisto[i];
    }

    if (minVal > pData->blockRowsHisto[i]) {
      minVal = pData->blockRowsHisto[i];
    }
  }

  // maximum number of step is 80
  double factor = pData->numOfBlocks / 80.0;

  int32_t numOfBuckets = sizeof(pData->blockRowsHisto) / sizeof(pData->blockRowsHisto[0]);
  int32_t bucketRange = ceil(((double) (pData->defMaxRows - pData->defMinRows)) / numOfBuckets);

  for (int32_t i = 0; i < tListLen(pData->blockRowsHisto); ++i) {
    len = tsnprintf(varDataVal(st), sizeof(st) - VARSTR_HEADER_SIZE, "%04d |", pData->defMinRows + bucketRange * (i + 1));

    int32_t num = 0;
    if (pData->blockRowsHisto[i] > 0) {
      num = (pData->blockRowsHisto[i]) / factor;
    }

    for (int32_t j = 0; j < num; ++j) {
      int32_t x = tsnprintf(varDataVal(st) + len, sizeof(st) - VARSTR_HEADER_SIZE - len, "%c", '|');
      len += x;
    }

    if (pData->blockRowsHisto[i] > 0) {
      double v = pData->blockRowsHisto[i] * 100.0 / pData->numOfBlocks;
      len += tsnprintf(varDataVal(st) + len, sizeof(st) - VARSTR_HEADER_SIZE - len, "  %d (%.2f%c)", pData->blockRowsHisto[i], v, '%');
    }

    varDataSetLen(st, len);
    code = colDataSetVal(pColInfo, row++, st, false);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

bool getDerivativeFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SDerivInfo);
  return true;
}

int32_t derivativeFuncSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo) {
  if (pResInfo->initialized) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS != functionSetup(pCtx, pResInfo)) {
    return TSDB_CODE_FUNC_SETUP_ERROR;
  }

  SDerivInfo* pDerivInfo = GET_ROWCELL_INTERBUF(pResInfo);

  pDerivInfo->ignoreNegative = pCtx->param[2].param.i;
  pDerivInfo->prevTs = -1;
  pDerivInfo->tsWindow = pCtx->param[1].param.i;
  pDerivInfo->valueSet = false;
  return TSDB_CODE_SUCCESS;
}

int32_t derivativeFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SDerivInfo*          pDerivInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  int32_t          numOfElems = 0;
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;
  SColumnInfoData* pTsOutput = pCtx->pTsOutput;
  int32_t          code = TSDB_CODE_SUCCESS;

  funcInputUpdate(pCtx);

  double v = 0;
  if (pCtx->order == TSDB_ORDER_ASC) {
    SFuncInputRow row = {0};
    bool result = false;
    while (1) {
      code = funcInputGetNextRow(pCtx, &row, &result);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      if (!result) {
        break;
      }
      if (row.isDataNull) {
        continue;
      }

      char* d = row.pData;
      GET_TYPED_DATA(v, double, pInputCol->info.type, d);

      int32_t pos = pCtx->offset + numOfElems;
      if (!pDerivInfo->valueSet) {  // initial value is not set yet
        pDerivInfo->valueSet = true;
      } else {
        if (row.ts == pDerivInfo->prevTs) {
          return TSDB_CODE_FUNC_DUP_TIMESTAMP;
        }
        double r = ((v - pDerivInfo->prevValue) * pDerivInfo->tsWindow) / (row.ts - pDerivInfo->prevTs);
        if (pDerivInfo->ignoreNegative && r < 0) {
        } else {
          if (isinf(r) || isnan(r)) {
            colDataSetNULL(pOutput, pos);
          } else {
            code = colDataSetVal(pOutput, pos, (const char*)&r, false);
            if (code != TSDB_CODE_SUCCESS) {
              return code;
            }
          }

          if (pTsOutput != NULL) {
            colDataSetInt64(pTsOutput, pos, &row.ts);
          }

          // handle selectivity
          if (pCtx->subsidiaries.num > 0) {
            code = appendSelectivityCols(pCtx, row.block, row.rowIndex, pos);
            if (code != TSDB_CODE_SUCCESS) {
              return code;
            }
          }

          numOfElems++;
        }
      }

      pDerivInfo->prevValue = v;
      pDerivInfo->prevTs = row.ts;
    }
  } else {
    SFuncInputRow row = {0};
    bool          result = false;
    while (1) {
      code = funcInputGetNextRow(pCtx, &row, &result);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      if (!result) {
        break;
      }
      if (row.isDataNull) {
        continue;
      }

      char* d = row.pData;
      GET_TYPED_DATA(v, double, pInputCol->info.type, d);

      int32_t pos = pCtx->offset + numOfElems;
      if (!pDerivInfo->valueSet) {  // initial value is not set yet
        pDerivInfo->valueSet = true;
      } else {
        if (row.ts == pDerivInfo->prevTs) {
          return TSDB_CODE_FUNC_DUP_TIMESTAMP;
        }
        double r = ((pDerivInfo->prevValue - v) * pDerivInfo->tsWindow) / (pDerivInfo->prevTs - row.ts);
        if (pDerivInfo->ignoreNegative && r < 0) {
        } else {
          if (isinf(r) || isnan(r)) {
            colDataSetNULL(pOutput, pos);
          } else {
            code = colDataSetVal(pOutput, pos, (const char*)&r, false);
            if (code != TSDB_CODE_SUCCESS) {
              return code;
            }
          }

          if (pTsOutput != NULL) {
            colDataSetInt64(pTsOutput, pos, &pDerivInfo->prevTs);
          }

          // handle selectivity
          if (pCtx->subsidiaries.num > 0) {
            code = appendSelectivityCols(pCtx, row.block, row.rowIndex, pos);
            if (code != TSDB_CODE_SUCCESS) {
              return code;
            }
          }
          numOfElems++;
        }
      }

      pDerivInfo->prevValue = v;
      pDerivInfo->prevTs = row.ts;
    }
  }

  pResInfo->numOfRes = numOfElems;

  return TSDB_CODE_SUCCESS;
}

int32_t getIrateInfoSize(int32_t pkBytes) { return (int32_t)sizeof(SRateInfo) + 2 * pkBytes; }

bool getIrateFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  int32_t pkBytes = (pFunc->hasPk) ? pFunc->pkBytes : 0;
  pEnv->calcMemSize = getIrateInfoSize(pkBytes);
  return true;
}

int32_t irateFuncSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo) {
  if (pResInfo->initialized) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS != functionSetup(pCtx, pResInfo)) {
    return TSDB_CODE_FUNC_SETUP_ERROR;
  }

  SRateInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  pInfo->firstKey = INT64_MIN;
  pInfo->lastKey = INT64_MIN;
  pInfo->firstValue = (double)INT64_MIN;
  pInfo->lastValue = (double)INT64_MIN;

  pInfo->hasResult = 0;
  return TSDB_CODE_SUCCESS;
}

static void doSaveRateInfo(SRateInfo* pRateInfo, bool isFirst, int64_t ts, char* pk, double v) {
  if (isFirst) {
    pRateInfo->firstValue = v;
    pRateInfo->firstKey = ts;
    if (pRateInfo->firstPk) {
      int32_t pkBytes = IS_VAR_DATA_TYPE(pRateInfo->pkType) ? varDataTLen(pk) : pRateInfo->pkBytes;
      (void)memcpy(pRateInfo->firstPk, pk, pkBytes);
    }
  } else {
    pRateInfo->lastValue = v;
    pRateInfo->lastKey = ts;
    if (pRateInfo->lastPk) {
      int32_t pkBytes = IS_VAR_DATA_TYPE(pRateInfo->pkType) ? varDataTLen(pk) : pRateInfo->pkBytes;
      (void)memcpy(pRateInfo->lastPk, pk, pkBytes);
    }
  }
}

static void initializeRateInfo(SqlFunctionCtx* pCtx, SRateInfo* pRateInfo, bool isMerge) {
  if (pCtx->hasPrimaryKey) {
    if (!isMerge) {
      pRateInfo->pkType = pCtx->input.pPrimaryKey->info.type;
      pRateInfo->pkBytes = pCtx->input.pPrimaryKey->info.bytes;
      pRateInfo->firstPk = pRateInfo->pkData;
      pRateInfo->lastPk = pRateInfo->pkData + pRateInfo->pkBytes;
    } else {
      pRateInfo->firstPk = pRateInfo->pkData;
      pRateInfo->lastPk = pRateInfo->pkData + pRateInfo->pkBytes;
    }
  } else {
    pRateInfo->firstPk = NULL;
    pRateInfo->lastPk = NULL;
  }  
}

int32_t irateFunction(SqlFunctionCtx* pCtx) {
  int32_t              code = TSDB_CODE_SUCCESS;
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SRateInfo*           pRateInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  funcInputUpdate(pCtx);
  
  initializeRateInfo(pCtx, pRateInfo, false);

  int32_t numOfElems = 0;
  int32_t type = pInputCol->info.type;
  SFuncInputRow row = {0};
  bool          result = false;
  while (1)  {
    code = funcInputGetNextRow(pCtx, &row, &result);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    if (!result) {
      break;
    }
    if (row.isDataNull) {
      continue;
    }

    char*  data = row.pData;
    double v = 0;
    GET_TYPED_DATA(v, double, type, data);

    if (INT64_MIN == pRateInfo->lastKey) {
      doSaveRateInfo(pRateInfo, false, row.ts, row.pPk, v);
      pRateInfo->hasResult = 1;
      continue;
    }

    if (row.ts > pRateInfo->lastKey) {
      if ((INT64_MIN == pRateInfo->firstKey) || pRateInfo->lastKey > pRateInfo->firstKey) {
        doSaveRateInfo(pRateInfo, true, pRateInfo->lastKey, pRateInfo->lastPk, pRateInfo->lastValue);
      }
      doSaveRateInfo(pRateInfo, false, row.ts, row.pPk, v);
      continue;
    } else if (row.ts == pRateInfo->lastKey) {
        return TSDB_CODE_FUNC_DUP_TIMESTAMP;
    }
    

    if ((INT64_MIN == pRateInfo->firstKey) || row.ts > pRateInfo->firstKey) {
      doSaveRateInfo(pRateInfo, true, row.ts, row.pPk, v);    
    } else if (row.ts == pRateInfo->firstKey) {
      return TSDB_CODE_FUNC_DUP_TIMESTAMP;
    }
  }

  numOfElems++;
  
  SET_VAL(pResInfo, numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

static double doCalcRate(const SRateInfo* pRateInfo, double tickPerSec) {
  if ((INT64_MIN == pRateInfo->lastKey) || (INT64_MIN == pRateInfo->firstKey) ||
      (pRateInfo->firstKey >= pRateInfo->lastKey)) {
    return 0.0;
  }

  double diff = 0;
  // If the previous value of the last is greater than the last value, only keep the last point instead of the delta
  // value between two values.
  diff = pRateInfo->lastValue;
  if (diff >= pRateInfo->firstValue) {
    diff -= pRateInfo->firstValue;
  }

  int64_t duration = pRateInfo->lastKey - pRateInfo->firstKey;
  if (duration == 0) {
    return 0;
  }

  return (duration > 0) ? ((double)diff) / (duration / tickPerSec) : 0.0;
}

static void irateTransferInfoImpl(TSKEY inputKey, SRateInfo* pInput, SRateInfo* pOutput, bool isFirstKey) {
  if (inputKey > pOutput->lastKey) {
    doSaveRateInfo(pOutput, true, pOutput->lastKey, pOutput->lastPk, pOutput->lastValue);
    if (isFirstKey) {
      doSaveRateInfo(pOutput, false, pInput->firstKey, pInput->firstPk, pInput->firstValue);
    } else {
      doSaveRateInfo(pOutput, false, pInput->lastKey, pInput->lastPk, pInput->lastValue);
    }
  } else if ((inputKey < pOutput->lastKey) && (inputKey > pOutput->firstKey)) {
    if (isFirstKey) {
      doSaveRateInfo(pOutput, true, pInput->firstKey, pInput->firstPk, pInput->firstValue);
    } else {
      doSaveRateInfo(pOutput, true, pInput->lastKey, pInput->lastPk, pInput->lastValue);
    }
  } else {
    // inputKey < pOutput->firstKey
  }
}

static void irateCopyInfo(SRateInfo* pInput, SRateInfo* pOutput) {
  doSaveRateInfo(pOutput, true, pInput->firstKey, pInput->firstPk, pInput->firstValue);
  doSaveRateInfo(pOutput, false, pInput->lastKey, pInput->lastPk, pInput->lastValue);
}

static int32_t irateTransferInfo(SRateInfo* pInput, SRateInfo* pOutput) {
  if ((pInput->firstKey != INT64_MIN && (pInput->firstKey == pOutput->firstKey || pInput->firstKey == pOutput->lastKey)) ||
      (pInput->lastKey != INT64_MIN && (pInput->lastKey  == pOutput->firstKey || pInput->lastKey  == pOutput->lastKey))) {
    return TSDB_CODE_FUNC_DUP_TIMESTAMP;
  }

  if (pOutput->hasResult == 0) {
    irateCopyInfo(pInput, pOutput);
    pOutput->hasResult = pInput->hasResult;
    return TSDB_CODE_SUCCESS;
  }

  if (pInput->firstKey != INT64_MIN) {
    irateTransferInfoImpl(pInput->firstKey, pInput, pOutput, true);
  }

  if (pInput->lastKey != INT64_MIN) {
    irateTransferInfoImpl(pInput->lastKey, pInput, pOutput, false);
  }

  pOutput->hasResult = pInput->hasResult;
  return TSDB_CODE_SUCCESS;
}

int32_t irateFunctionMerge(SqlFunctionCtx* pCtx) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];
  if (pCol->info.type != TSDB_DATA_TYPE_BINARY) {
    return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }

  SRateInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  initializeRateInfo(pCtx, pInfo, true);

  int32_t start = pInput->startRowIndex;
  for (int32_t i = start; i < start + pInput->numOfRows; ++i) {
    char*        data = colDataGetData(pCol, i);
    SRateInfo*   pInputInfo = (SRateInfo*)varDataVal(data);
    initializeRateInfo(pCtx, pInfo, true);
    if (pInputInfo->hasResult) {
      int32_t code = irateTransferInfo(pInputInfo, pInfo);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }

  if (pInfo->hasResult) {
    GET_RES_INFO(pCtx)->numOfRes = 1;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t iratePartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SRateInfo*           pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t              resultBytes = getIrateInfoSize(pInfo->pkBytes);
  char*                res = taosMemoryCalloc(resultBytes + VARSTR_HEADER_SIZE, sizeof(char));

  if (NULL == res) {
    return terrno;
  }
  (void)memcpy(varDataVal(res), pInfo, resultBytes);
  varDataSetLen(res, resultBytes);

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  if (NULL == pCol) {
      taosMemoryFree(res);
      return TSDB_CODE_OUT_OF_RANGE;
  }

  int32_t code = colDataSetVal(pCol, pBlock->info.rows, res, false);

  taosMemoryFree(res);
  return code;
}

int32_t irateFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  if (NULL == pCol) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  pResInfo->isNullRes = (pResInfo->numOfRes == 0) ? 1 : 0;

  SRateInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  double     result = doCalcRate(pInfo, (double)TSDB_TICK_PER_SECOND(pCtx->param[1].param.i));
  int32_t code = colDataSetVal(pCol, pBlock->info.rows, (const char*)&result, pResInfo->isNullRes);

  return code;
}

int32_t groupConstValueFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SGroupKeyInfo*       pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  int32_t startIndex = pInput->startRowIndex;

  // escape rest of data blocks to avoid first entry to be overwritten.
  if (pInfo->hasResult) {
    goto _group_value_over;
  }

  if (pInputCol->pData == NULL || colDataIsNull_s(pInputCol, startIndex)) {
    pInfo->isNull = true;
    pInfo->hasResult = true;
    goto _group_value_over;
  }

  char* data = colDataGetData(pInputCol, startIndex);
  if (IS_VAR_DATA_TYPE(pInputCol->info.type)) {
    (void)memcpy(pInfo->data, data,
                 (pInputCol->info.type == TSDB_DATA_TYPE_JSON) ? getJsonValueLen(data) : varDataTLen(data));
  } else {
    (void)memcpy(pInfo->data, data, pInputCol->info.bytes);
  }
  pInfo->hasResult = true;

_group_value_over:

  SET_VAL(pResInfo, 1, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t groupKeyFunction(SqlFunctionCtx* pCtx) {
  return groupConstValueFunction(pCtx);
}

int32_t groupConstValueFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  int32_t          code = TSDB_CODE_SUCCESS;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  if (NULL == pCol) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  SGroupKeyInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  if (pInfo->hasResult) {
    int32_t currentRow = pBlock->info.rows;
    for (; currentRow < pBlock->info.rows + pResInfo->numOfRes; ++currentRow) {
      code = colDataSetVal(pCol, currentRow, pInfo->data, pInfo->isNull ? true : false);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  } else {
    pResInfo->numOfRes = 0;
  }

  return code;
}

int32_t groupKeyFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock){
  return groupConstValueFinalize(pCtx, pBlock);
}

int32_t groupKeyCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SGroupKeyInfo*       pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SGroupKeyInfo*       pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  // escape rest of data blocks to avoid first entry to be overwritten.
  if (pDBuf->hasResult) {
    goto _group_key_over;
  }

  if (pSBuf->isNull) {
    pDBuf->isNull = true;
    pDBuf->hasResult = true;
    goto _group_key_over;
  }

  if (IS_VAR_DATA_TYPE(pSourceCtx->resDataInfo.type)) {
    (void)memcpy(pDBuf->data, pSBuf->data,
           (pSourceCtx->resDataInfo.type == TSDB_DATA_TYPE_JSON) ? getJsonValueLen(pSBuf->data) : varDataTLen(pSBuf->data));
  } else {
    (void)memcpy(pDBuf->data, pSBuf->data, pSourceCtx->resDataInfo.bytes);
  }

  pDBuf->hasResult = true;

_group_key_over:

  SET_VAL(pDResInfo, 1, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t cachedLastRowFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = 0;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SFirstLastRes*       pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  int32_t bytes = pInputCol->info.bytes;
  pInfo->bytes = bytes;

  SColumnInfoData* pkCol = pInput->pPrimaryKey;
  pInfo->pkType = -1;
  __compar_fn_t  pkCompareFn = NULL;
  if (pCtx->hasPrimaryKey) {
    pInfo->pkType = pkCol->info.type;
    pInfo->pkBytes = pkCol->info.bytes;
    pkCompareFn = getKeyComparFunc(pInfo->pkType, TSDB_ORDER_DESC);
  }

  // TODO it traverse the different way.
  // last_row function does not ignore the null value
  for (int32_t i = pInput->numOfRows + pInput->startRowIndex - 1; i >= pInput->startRowIndex; --i) {
    numOfElems++;

    bool  isNull = colDataIsNull(pInputCol, pInput->numOfRows, i, NULL);
    char* data = isNull ? NULL : colDataGetData(pInputCol, i);

    TSKEY cts = getRowPTs(pInput->pPTS, i);
    if (pResInfo->numOfRes == 0 || pInfo->ts < cts) {
      int32_t code = doSaveLastrow(pCtx, data, i, cts, pInfo);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      pResInfo->numOfRes = 1;
    }
  }

  SET_VAL(pResInfo, numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}
