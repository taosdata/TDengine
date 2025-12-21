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

#include "filter.h"
#include "os.h"
#include "query.h"
#include "taosdef.h"
#include "tmsg.h"
#include "ttypes.h"

#include "executorInt.h"
#include "tcommon.h"
#include "thash.h"
#include "ttime.h"

#include "executorInt.h"
#include "function.h"
#include "querynodes.h"
#include "querytask.h"
#include "tdatablock.h"
#include "tfill.h"

#define FILL_IS_ASC_FILL(_f) ((_f)->order == TSDB_ORDER_ASC)
#define DO_INTERPOLATION(_v1, _v2, _k1, _k2, _k) \
  ((_v1) + ((_v2) - (_v1)) * (((double)(_k)) - ((double)(_k1))) / (((double)(_k2)) - ((double)(_k1))))

static int32_t doSetVal(SColumnInfoData* pDstColInfoData, int32_t rowIndex, const SGroupKeys* pKey);

static bool setNotFillColumn(SFillInfo* pFillInfo, SColumnInfoData* pDstColInfo, int32_t rowIndex, int32_t colIdx) {
  SFillColInfo* pCol = &pFillInfo->pFillCol[colIdx];
  if (pCol->fillNull) {
    colDataSetNULL(pDstColInfo, rowIndex);
  } else {
    SRowVal* p = NULL;
    bool     ascNext = false, descPrev = false;
    if (pFillInfo->type == TSDB_FILL_NEXT) {
      p = FILL_IS_ASC_FILL(pFillInfo) ? &pFillInfo->next : &pFillInfo->prev;
      if (FILL_IS_ASC_FILL(pFillInfo)) ascNext = true;
    } else {
      p = FILL_IS_ASC_FILL(pFillInfo) ? &pFillInfo->prev : &pFillInfo->next;
      if (!FILL_IS_ASC_FILL(pFillInfo)) descPrev = true;
    }

    const bool* pNullValueFlag = taosArrayGet(p->pNullValueFlag, colIdx);
    if (*pNullValueFlag && pFillInfo->numOfRows > 0) {
      if (ascNext || descPrev) return true;
    }

    SGroupKeys* pKey = taosArrayGet(p->pRowVal, colIdx);
    if (!pKey) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      T_LONG_JMP(pFillInfo->pTaskInfo->env, terrno);
    }
    int32_t code = doSetVal(pDstColInfo, rowIndex, pKey);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      T_LONG_JMP(pFillInfo->pTaskInfo->env, code);
    }
  }
  return false;
}

static void setNullCol(SSDataBlock* pBlock, SFillInfo* pFillInfo, int32_t rowIdx, int32_t colIdx) {
  SFillColInfo*    pCol = &pFillInfo->pFillCol[colIdx];
  int32_t          dstSlotId = GET_DEST_SLOT_ID(pCol);
  SColumnInfoData* pDstColInfo = taosArrayGet(pBlock->pDataBlock, dstSlotId);
  if (pCol->notFillCol) {
    bool filled = fillIfWindowPseudoColumn(pFillInfo, pCol, pDstColInfo, rowIdx);
    if (!filled) {
      TAOS_UNUSED(setNotFillColumn(pFillInfo, pDstColInfo, rowIdx, colIdx));
    }
  } else {
    colDataSetNULL(pDstColInfo, rowIdx);
  }
}

static void setNullRow(SSDataBlock* pBlock, SFillInfo* pFillInfo, int32_t rowIndex) {
  for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    setNullCol(pBlock, pFillInfo, rowIndex, i);
  }
}

static int32_t doSetUserSpecifiedValue(SColumnInfoData* pDst, SVariant* pVar, int32_t rowIndex, int64_t currentKey) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  bool    isNull = (TSDB_DATA_TYPE_NULL == pVar->nType) ? true : false;
  if (pDst->info.type == TSDB_DATA_TYPE_FLOAT) {
    float v = 0;
    GET_TYPED_DATA(v, float, pVar->nType, &pVar->f, typeGetTypeModFromColInfo(&pDst->info));
    code = colDataSetVal(pDst, rowIndex, (char*)&v, isNull);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (pDst->info.type == TSDB_DATA_TYPE_DOUBLE) {
    double v = 0;
    GET_TYPED_DATA(v, double, pVar->nType, &pVar->d, typeGetTypeModFromColInfo(&pDst->info));
    code = colDataSetVal(pDst, rowIndex, (char*)&v, isNull);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (IS_SIGNED_NUMERIC_TYPE(pDst->info.type) || pDst->info.type == TSDB_DATA_TYPE_BOOL) {
    int64_t v = 0;
    GET_TYPED_DATA(v, int64_t, pVar->nType, &pVar->i, typeGetTypeModFromColInfo(&pDst->info));
    code = colDataSetVal(pDst, rowIndex, (char*)&v, isNull);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (IS_UNSIGNED_NUMERIC_TYPE(pDst->info.type)) {
    uint64_t v = 0;
    GET_TYPED_DATA(v, uint64_t, pVar->nType, &pVar->u, typeGetTypeModFromColInfo(&pDst->info));
    code = colDataSetVal(pDst, rowIndex, (char*)&v, isNull);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (pDst->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
    int64_t v = 0;
    GET_TYPED_DATA(v, int64_t, pVar->nType, &pVar->u, typeGetTypeModFromColInfo(&pDst->info));
    code = colDataSetVal(pDst, rowIndex, (const char*)&v, isNull);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (pDst->info.type == TSDB_DATA_TYPE_NCHAR || pDst->info.type == TSDB_DATA_TYPE_VARCHAR ||
             pDst->info.type == TSDB_DATA_TYPE_VARBINARY) {
    code = colDataSetVal(pDst, rowIndex, pVar->pz, isNull);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (pDst->info.type == TSDB_DATA_TYPE_DECIMAL64) {
    code = colDataSetVal(pDst, rowIndex, (char*)&pVar->i, isNull);
  } else if (pDst->info.type == TSDB_DATA_TYPE_DECIMAL) {
    code = colDataSetVal(pDst, rowIndex, (char*)pVar->pz, isNull);
  } else {  // others data
    colDataSetNULL(pDst, rowIndex);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// fill windows pseudo column, _wstart, _wend, _wduration and return true, otherwise return false
bool fillIfWindowPseudoColumn(SFillInfo* pFillInfo, SFillColInfo* pCol, SColumnInfoData* pDstColInfoData,
                              int32_t rowIndex) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (!pCol->notFillCol) {
    return false;
  }
  if (pCol->pExpr->pExpr->nodeType == QUERY_NODE_COLUMN) {
    if (pCol->pExpr->base.numOfParams != 1) {
      return false;
    }
    if (pCol->pExpr->base.pParam[0].pCol->colType == COLUMN_TYPE_WINDOW_START) {
      code = colDataSetVal(pDstColInfoData, rowIndex, (const char*)&pFillInfo->currentKey, false);
      QUERY_CHECK_CODE(code, lino, _end);
      return true;
    } else if (pCol->pExpr->base.pParam[0].pCol->colType == COLUMN_TYPE_WINDOW_END) {
      // TODO: include endpoint
      SInterval* pInterval = &pFillInfo->interval;
      int64_t    windowEnd =
          taosTimeAdd(pFillInfo->currentKey, pInterval->interval, pInterval->intervalUnit, pInterval->precision, NULL);
      code = colDataSetVal(pDstColInfoData, rowIndex, (const char*)&windowEnd, false);
      QUERY_CHECK_CODE(code, lino, _end);
      return true;
    } else if (pCol->pExpr->base.pParam[0].pCol->colType == COLUMN_TYPE_WINDOW_DURATION) {
      // TODO: include endpoint
      code = colDataSetVal(pDstColInfoData, rowIndex, (const char*)&pFillInfo->interval.interval, false);
      QUERY_CHECK_CODE(code, lino, _end);
      return true;
    } else if (pCol->pExpr->base.pParam[0].pCol->colType == COLUMN_TYPE_IS_WINDOW_FILLED) {
      code = colDataSetVal(pDstColInfoData, rowIndex, (const char*)&pFillInfo->isFilled, false);
      QUERY_CHECK_CODE(code, lino, _end);
      return true;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pFillInfo->pTaskInfo->env, code);
  }
  return false;
}

static void doFillOneRow(SFillInfo* pFillInfo, SSDataBlock* pBlock, SSDataBlock* pSrcBlock, int64_t ts,
                         bool outOfBound) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SPoint  point1, point2, point;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pFillInfo->order);

  // set the primary timestamp column value
  int32_t index = pBlock->info.rows;

  // set the other values
  if (pFillInfo->type == TSDB_FILL_PREV) {
    for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
      SFillColInfo* pCol = &pFillInfo->pFillCol[i];

      SColumnInfoData* pDstColInfoData = taosArrayGet(pBlock->pDataBlock, GET_DEST_SLOT_ID(pCol));
      bool             filled = fillIfWindowPseudoColumn(pFillInfo, pCol, pDstColInfoData, index);
      if (!filled) {
        TAOS_UNUSED(setNotFillColumn(pFillInfo, pDstColInfoData, index, i));
      }
    }
  } else if (pFillInfo->type == TSDB_FILL_NEXT) {
    // todo  refactor: start from 0 not 1
    for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
      SFillColInfo*    pCol = &pFillInfo->pFillCol[i];
      SColumnInfoData* pDstColInfoData = taosArrayGet(pBlock->pDataBlock, GET_DEST_SLOT_ID(pCol));
      bool             filled = fillIfWindowPseudoColumn(pFillInfo, pCol, pDstColInfoData, index);
      if (!filled) {
        TAOS_UNUSED(setNotFillColumn(pFillInfo, pDstColInfoData, index, i));
      }
    }
  } else if (pFillInfo->type == TSDB_FILL_LINEAR) {
    // TODO : linear interpolation supports NULL value
    if (outOfBound) {
      setNullRow(pBlock, pFillInfo, index);
    } else {
      for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
        SFillColInfo* pCol = &pFillInfo->pFillCol[i];

        int32_t          dstSlotId = GET_DEST_SLOT_ID(pCol);
        SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, dstSlotId);
        int16_t          type = pDstCol->info.type;

        if (pCol->notFillCol) {
          bool filled = fillIfWindowPseudoColumn(pFillInfo, pCol, pDstCol, index);
          if (!filled) {
            TAOS_UNUSED(setNotFillColumn(pFillInfo, pDstCol, index, i));
          }
        } else {
          SRowVal*    pRVal = &pFillInfo->prev;
          SGroupKeys* pKey = taosArrayGet(pRVal->pRowVal, i);
          if (IS_VAR_DATA_TYPE(type) || type == TSDB_DATA_TYPE_BOOL || pKey->isNull) {
            colDataSetNULL(pDstCol, index);
            continue;
          }

          SGroupKeys* pKey1 = taosArrayGet(pRVal->pRowVal, pFillInfo->tsSlotId);

          int64_t prevTs = *(int64_t*)pKey1->pData;
          int32_t srcSlotId = GET_DEST_SLOT_ID(pCol);

          SColumnInfoData* pSrcCol = taosArrayGet(pSrcBlock->pDataBlock, srcSlotId);
          char*            data = colDataGetData(pSrcCol, pFillInfo->index);

          point1 = (SPoint){.key = prevTs, .val = pKey->pData};
          point2 = (SPoint){.key = ts, .val = data};

          int64_t out = 0;
          point = (SPoint){.key = pFillInfo->currentKey, .val = &out};
          taosGetLinearInterpolationVal(&point, type, &point1, &point2, type,
                                        typeGetTypeModFromColInfo(&pDstCol->info));

          code = colDataSetVal(pDstCol, index, (const char*)&out, false);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }
  } else if (pFillInfo->type == TSDB_FILL_NULL || pFillInfo->type == TSDB_FILL_NULL_F) {  // fill with NULL
    setNullRow(pBlock, pFillInfo, index);
  } else {  // fill with user specified value for each column
    for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
      SFillColInfo* pCol = &pFillInfo->pFillCol[i];

      int32_t          slotId = GET_DEST_SLOT_ID(pCol);
      SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, slotId);

      if (pCol->notFillCol) {
        bool filled = fillIfWindowPseudoColumn(pFillInfo, pCol, pDst, index);
        if (!filled) {
          TAOS_UNUSED(setNotFillColumn(pFillInfo, pDst, index, i));
        }
      } else {
        SVariant* pVar = &pFillInfo->pFillCol[i].fillVal;
        code = doSetUserSpecifiedValue(pDst, pVar, index, pFillInfo->currentKey);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  }

  //  setTagsValue(pFillInfo, data, index);
  SInterval* pInterval = &pFillInfo->interval;
  pFillInfo->currentKey =
      taosTimeAdd(pFillInfo->currentKey, pInterval->sliding * step, pInterval->slidingUnit, pInterval->precision, NULL);
  pBlock->info.rows += 1;
  pFillInfo->numOfCurrent++;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pFillInfo->pTaskInfo->env, code);
  }
}

int32_t doSetVal(SColumnInfoData* pDstCol, int32_t rowIndex, const SGroupKeys* pKey) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pKey->isNull) {
    colDataSetNULL(pDstCol, rowIndex);
  } else {
    code = colDataSetVal(pDstCol, rowIndex, pKey->pData, false);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t initBeforeAfterDataBuf(SFillInfo* pFillInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (taosArrayGetSize(pFillInfo->next.pRowVal) > 0) {
    goto _end;
  }

  for (int i = 0; i < pFillInfo->numOfCols; i++) {
    SFillColInfo* pCol = &pFillInfo->pFillCol[i];

    SGroupKeys  key = {0};
    SResSchema* pSchema = &pCol->pExpr->base.resSchema;
    key.pData = taosMemoryMalloc(pSchema->bytes);
    QUERY_CHECK_NULL(key.pData, code, lino, _end, terrno);
    key.isNull = true;
    key.bytes = pSchema->bytes;
    key.type = pSchema->type;
    bool nullValueFlag = false;

    void* tmp = taosArrayPush(pFillInfo->next.pRowVal, &key);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

    tmp = taosArrayPush(pFillInfo->next.pNullValueFlag, &nullValueFlag);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

    key.pData = taosMemoryMalloc(pSchema->bytes);
    QUERY_CHECK_NULL(key.pData, code, lino, _end, terrno);

    tmp = taosArrayPush(pFillInfo->prev.pRowVal, &key);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

    tmp = taosArrayPush(pFillInfo->prev.pNullValueFlag, &nullValueFlag);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void saveColData(SArray* rowBuf, int32_t columnIndex, const char* src, bool isNull);

static int32_t copyCurrentRowIntoBuf(SFillInfo* pFillInfo, int32_t rowIndex, SRowVal* pRowVal, bool reset) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SColumnInfoData* pTsCol = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, pFillInfo->srcTsSlotId);
  QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
  pRowVal->key = ((int64_t*)pTsCol->pData)[rowIndex];

  for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    int32_t type = pFillInfo->pFillCol[i].pExpr->pExpr->nodeType;
    if (type == QUERY_NODE_COLUMN || type == QUERY_NODE_OPERATOR || type == QUERY_NODE_FUNCTION) {
      if (!pFillInfo->pFillCol[i].notFillCol) {
        if (FILL_IS_ASC_FILL(pFillInfo) && pFillInfo->type != TSDB_FILL_NEXT) continue;
        if (!FILL_IS_ASC_FILL(pFillInfo) && pFillInfo->type != TSDB_FILL_PREV) continue;
      }
      int32_t srcSlotId = GET_DEST_SLOT_ID(&pFillInfo->pFillCol[i]);

      if (srcSlotId == pFillInfo->srcTsSlotId && pFillInfo->type == TSDB_FILL_LINEAR) {
        continue;
      }

      SColumnInfoData* pSrcCol = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, srcSlotId);
      QUERY_CHECK_NULL(pSrcCol, code, lino, _end, terrno);

      bool  isNull = colDataIsNull_s(pSrcCol, rowIndex);
      char* p = colDataGetData(pSrcCol, rowIndex);

      saveColData(pRowVal->pRowVal, i, p, reset ? true : isNull);
    } else {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t fillResultImpl(SFillInfo* pFillInfo, SSDataBlock* pBlock, int32_t outputRows) {
  pFillInfo->numOfCurrent = 0;
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SColumnInfoData* pTsCol = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, pFillInfo->srcTsSlotId);

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pFillInfo->order);
  bool    ascFill = FILL_IS_ASC_FILL(pFillInfo);

  while (pFillInfo->numOfCurrent < outputRows) {
    int64_t ts = ((int64_t*)pTsCol->pData)[pFillInfo->index];

    // set the next value for interpolation
    if (pFillInfo->currentKey < ts && ascFill) {
      SRowVal* pRVal = pFillInfo->type == TSDB_FILL_NEXT ? &pFillInfo->next : &pFillInfo->prev;
      code = copyCurrentRowIntoBuf(pFillInfo, pFillInfo->index, pRVal, false);
      QUERY_CHECK_CODE(code, lino, _end);
    } else if (pFillInfo->currentKey > ts && !ascFill) {
      SRowVal* pRVal = pFillInfo->type == TSDB_FILL_NEXT ? &pFillInfo->prev : &pFillInfo->next;
      code = copyCurrentRowIntoBuf(pFillInfo, pFillInfo->index, pRVal, false);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (((pFillInfo->currentKey < ts && ascFill) || (pFillInfo->currentKey > ts && !ascFill)) &&
        pFillInfo->numOfCurrent < outputRows) {
      // fill the gap between two input rows
      while (((pFillInfo->currentKey < ts && ascFill) || (pFillInfo->currentKey > ts && !ascFill)) &&
             pFillInfo->numOfCurrent < outputRows) {
        doFillOneRow(pFillInfo, pBlock, pFillInfo->pSrcBlock, ts, false);
      }

      // output buffer is full, abort
      if (pFillInfo->numOfCurrent == outputRows) {
        pFillInfo->numOfTotal += pFillInfo->numOfCurrent;
        goto _end;
      }
    } else {
      QUERY_CHECK_CONDITION((pFillInfo->currentKey == ts), code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
      int32_t index = pBlock->info.rows;

      int32_t nextRowIndex = pFillInfo->index + 1;
      if (pFillInfo->type == TSDB_FILL_NEXT) {
        if ((pFillInfo->index + 1) < pFillInfo->numOfRows) {
          code = copyCurrentRowIntoBuf(pFillInfo, nextRowIndex, &pFillInfo->next, false);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          // reset to null after last row
          code = copyCurrentRowIntoBuf(pFillInfo, nextRowIndex, &pFillInfo->next, true);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
      if (pFillInfo->type == TSDB_FILL_PREV) {
        if (nextRowIndex + 1 >= pFillInfo->numOfRows && !FILL_IS_ASC_FILL(pFillInfo)) {
          code = copyCurrentRowIntoBuf(pFillInfo, nextRowIndex, &pFillInfo->next, true);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }

      // copy rows to dst buffer
      for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
        SFillColInfo* pCol = &pFillInfo->pFillCol[i];

        int32_t dstSlotId = GET_DEST_SLOT_ID(pCol);

        SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, dstSlotId);
        SColumnInfoData* pSrc = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, dstSlotId);

        char* src = colDataGetData(pSrc, pFillInfo->index);
        if (!colDataIsNull_s(pSrc, pFillInfo->index)) {
          code = colDataSetVal(pDst, index, src, false);
          QUERY_CHECK_CODE(code, lino, _end);
          SRowVal* pRVal = &pFillInfo->prev;
          saveColData(pRVal->pRowVal, i, src, false);
          if (pFillInfo->srcTsSlotId == dstSlotId) {
            pRVal->key = *(int64_t*)src;
          }
        } else {  // the value is null
          if (pDst->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
            code = colDataSetVal(pDst, index, (const char*)&pFillInfo->currentKey, false);
            QUERY_CHECK_CODE(code, lino, _end);
          } else {  // i > 0 and data is null , do interpolation
            if (pFillInfo->type == TSDB_FILL_PREV) {
              SArray*     p = FILL_IS_ASC_FILL(pFillInfo) ? pFillInfo->prev.pRowVal : pFillInfo->next.pRowVal;
              SGroupKeys* pKey = taosArrayGet(p, i);
              QUERY_CHECK_NULL(pKey, code, lino, _end, terrno);
              code = doSetVal(pDst, index, pKey);
              QUERY_CHECK_CODE(code, lino, _end);
            } else if (pFillInfo->type == TSDB_FILL_LINEAR) {
              bool isNull = colDataIsNull_s(pSrc, pFillInfo->index);
              code = colDataSetVal(pDst, index, src, isNull);
              QUERY_CHECK_CODE(code, lino, _end);

              SArray* p = pFillInfo->prev.pRowVal;
              saveColData(p, i, src, isNull);  // todo:
            } else if (pFillInfo->type == TSDB_FILL_NULL || pFillInfo->type == TSDB_FILL_NULL_F) {
              colDataSetNULL(pDst, index);
            } else if (pFillInfo->type == TSDB_FILL_NEXT) {
              SArray*     p = FILL_IS_ASC_FILL(pFillInfo) ? pFillInfo->next.pRowVal : pFillInfo->prev.pRowVal;
              SGroupKeys* pKey = taosArrayGet(p, i);
              QUERY_CHECK_NULL(pKey, code, lino, _end, terrno);
              code = doSetVal(pDst, index, pKey);
              QUERY_CHECK_CODE(code, lino, _end);
            } else {
              SVariant* pVar = &pFillInfo->pFillCol[i].fillVal;
              code = doSetUserSpecifiedValue(pDst, pVar, index, pFillInfo->currentKey);
              QUERY_CHECK_CODE(code, lino, _end);
            }
          }
        }
      }

      // set the tag value for final result
      SInterval* pInterval = &pFillInfo->interval;
      pFillInfo->currentKey = taosTimeAdd(pFillInfo->currentKey, pInterval->sliding * step, pInterval->slidingUnit,
                                          pInterval->precision, NULL);

      pBlock->info.rows += 1;
      pFillInfo->index += 1;
      pFillInfo->numOfCurrent += 1;
    }

    if (pFillInfo->index >= pFillInfo->numOfRows || pFillInfo->numOfCurrent >= outputRows) {
      pFillInfo->numOfTotal += pFillInfo->numOfCurrent;
      goto _end;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void saveColData(SArray* rowBuf, int32_t columnIndex, const char* src, bool isNull) {
  SGroupKeys* pKey = taosArrayGet(rowBuf, columnIndex);
  if (isNull) {
    pKey->isNull = true;
  } else {
    if (IS_VAR_DATA_TYPE(pKey->type)) {
      int32_t bytes = calcStrBytesByType(pKey->type, (char*)src);
      memcpy(pKey->pData, src, bytes);
    } else {
      memcpy(pKey->pData, src, pKey->bytes);
    }
    pKey->isNull = false;
  }
}

static int32_t appendFilledResult(SFillInfo* pFillInfo, SSDataBlock* pBlock, int64_t resultCapacity) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  /*
   * These data are generated according to fill strategy, since the current timestamp is out of the time window of
   * real result set. Note that we need to keep the direct previous result rows, to generated the filled data.
   */
  pFillInfo->numOfCurrent = 0;
  while (pFillInfo->numOfCurrent < resultCapacity) {
    doFillOneRow(pFillInfo, pBlock, pFillInfo->pSrcBlock, pFillInfo->start, true);
  }

  pFillInfo->numOfTotal += pFillInfo->numOfCurrent;

  QUERY_CHECK_CONDITION((pFillInfo->numOfCurrent == resultCapacity), code, lino, _end,
                        TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t taosNumOfRemainRows(SFillInfo* pFillInfo) {
  if (pFillInfo->numOfRows == 0 || (pFillInfo->numOfRows > 0 && pFillInfo->index >= pFillInfo->numOfRows)) {
    return 0;
  }

  return pFillInfo->numOfRows - pFillInfo->index;
}

int32_t taosCreateFillInfo(TSKEY skey, int32_t numOfFillCols, int32_t numOfNotFillCols, int32_t fillNullCols,
                           int32_t capacity, SInterval* pInterval, int32_t fillType, struct SFillColInfo* pCol,
                           int32_t primaryTsSlotId, int32_t order, const char* id, SExecTaskInfo* pTaskInfo,
                           SFillInfo** ppFillInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (fillType == TSDB_FILL_NONE) {
    (*ppFillInfo) = NULL;
    return code;
  }

  SFillInfo* pFillInfo = taosMemoryCalloc(1, sizeof(SFillInfo));
  QUERY_CHECK_NULL(pFillInfo, code, lino, _end, terrno);

  pFillInfo->order = order;
  pFillInfo->srcTsSlotId = primaryTsSlotId;

  for (int32_t i = 0; i < numOfNotFillCols; ++i) {
    SFillColInfo* p = &pCol[i + numOfFillCols];
    int32_t       srcSlotId = GET_DEST_SLOT_ID(p);
    if (srcSlotId == primaryTsSlotId) {
      pFillInfo->tsSlotId = i + numOfFillCols;
      break;
    }
  }

  taosResetFillInfo(pFillInfo, skey);

  pFillInfo->type = fillType;
  pFillInfo->pFillCol = pCol;
  pFillInfo->numOfCols = numOfFillCols + numOfNotFillCols + fillNullCols;
  pFillInfo->alloc = capacity;
  pFillInfo->id = id;
  pFillInfo->interval = *pInterval;

  pFillInfo->next.pRowVal = taosArrayInit(pFillInfo->numOfCols, sizeof(SGroupKeys));
  QUERY_CHECK_NULL(pFillInfo->next.pRowVal, code, lino, _end, terrno);

  pFillInfo->next.pNullValueFlag = taosArrayInit(pFillInfo->numOfCols, sizeof(bool));
  QUERY_CHECK_NULL(pFillInfo->next.pNullValueFlag, code, lino, _end, terrno);

  pFillInfo->prev.pRowVal = taosArrayInit(pFillInfo->numOfCols, sizeof(SGroupKeys));
  QUERY_CHECK_NULL(pFillInfo->prev.pRowVal, code, lino, _end, terrno);

  pFillInfo->prev.pNullValueFlag = taosArrayInit(pFillInfo->numOfCols, sizeof(bool));
  QUERY_CHECK_NULL(pFillInfo->prev.pNullValueFlag, code, lino, _end, terrno);

  code = initBeforeAfterDataBuf(pFillInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  pFillInfo->pTaskInfo = pTaskInfo;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pFillInfo = taosDestroyFillInfo(pFillInfo);
  }
  (*ppFillInfo) = pFillInfo;
  return code;
}

void taosResetFillInfo(SFillInfo* pFillInfo, TSKEY startTimestamp) {
  pFillInfo->start = startTimestamp;
  pFillInfo->currentKey = startTimestamp;
  pFillInfo->end = startTimestamp;
  pFillInfo->index = -1;
  pFillInfo->numOfRows = 0;
  pFillInfo->numOfCurrent = 0;
  pFillInfo->numOfTotal = 0;
}

void* taosDestroyFillInfo(SFillInfo* pFillInfo) {
  if (pFillInfo == NULL) {
    return NULL;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pFillInfo->prev.pRowVal); ++i) {
    SGroupKeys* pKey = taosArrayGet(pFillInfo->prev.pRowVal, i);
    if (pKey) taosMemoryFree(pKey->pData);
  }
  taosArrayDestroy(pFillInfo->prev.pNullValueFlag);
  taosArrayDestroy(pFillInfo->prev.pRowVal);
  for (int32_t i = 0; i < taosArrayGetSize(pFillInfo->next.pRowVal); ++i) {
    SGroupKeys* pKey = taosArrayGet(pFillInfo->next.pRowVal, i);
    if (pKey) taosMemoryFree(pKey->pData);
  }
  taosArrayDestroy(pFillInfo->next.pNullValueFlag);
  taosArrayDestroy(pFillInfo->next.pRowVal);

  //  for (int32_t i = 0; i < pFillInfo->numOfTags; ++i) {
  //    taosMemoryFreeClear(pFillInfo->pTags[i].tagVal);
  //  }

  // free pFillCol
  if (pFillInfo->pFillCol) {
    for (int32_t i = 0; i < pFillInfo->numOfCols; i++) {
      SFillColInfo* pCol = &pFillInfo->pFillCol[i];
      if (!pCol->notFillCol) {
        if (pCol->fillVal.nType == TSDB_DATA_TYPE_VARBINARY || pCol->fillVal.nType == TSDB_DATA_TYPE_VARCHAR ||
            pCol->fillVal.nType == TSDB_DATA_TYPE_NCHAR || pCol->fillVal.nType == TSDB_DATA_TYPE_JSON ||
            pCol->fillVal.nType == TSDB_DATA_TYPE_DECIMAL) {
          if (pCol->fillVal.pz) {
            taosMemoryFree(pCol->fillVal.pz);
            pCol->fillVal.pz = NULL;
          }
        }
      }
    }
  }

  taosMemoryFreeClear(pFillInfo->pTags);
  taosMemoryFreeClear(pFillInfo->pFillCol);
  taosArrayDestroy(pFillInfo->pColFillProgress);
  tdListFreeP(pFillInfo->pFillSavedBlockList, destroyFillBlock);
  taosMemoryFreeClear(pFillInfo);
  return NULL;
}

void taosFillSetStartInfo(SFillInfo* pFillInfo, int32_t numOfRows, TSKEY endKey) {
  if (pFillInfo->type == TSDB_FILL_NONE) {
    return;
  }

  // the endKey is now the aligned time window value. truncate time window isn't correct.
  pFillInfo->end = endKey;
  pFillInfo->index = 0;
  pFillInfo->numOfRows = numOfRows;
}

void taosFillSetInputDataBlock(SFillInfo* pFillInfo, const SSDataBlock* pInput) {
  pFillInfo->pSrcBlock = (SSDataBlock*)pInput;
}

void taosFillUpdateStartTimestampInfo(SFillInfo* pFillInfo, int64_t ts) {
  pFillInfo->start = ts;
  pFillInfo->currentKey = ts;
}

bool taosFillNotStarted(const SFillInfo* pFillInfo) { return pFillInfo->start == pFillInfo->currentKey; }

bool taosFillHasMoreResults(SFillInfo* pFillInfo) {
  int32_t remain = taosNumOfRemainRows(pFillInfo);
  if (remain > 0) {
    return true;
  }

  bool ascFill = FILL_IS_ASC_FILL(pFillInfo);
  if (pFillInfo->numOfTotal > 0 &&
      (((pFillInfo->end > pFillInfo->start) && ascFill) || (pFillInfo->end < pFillInfo->start && !ascFill))) {
    return getNumOfResultsAfterFillGap(pFillInfo, pFillInfo->end, 4096) > 0;
  }

  return false;
}

int64_t getNumOfResultsAfterFillGap(SFillInfo* pFillInfo, TSKEY ekey, int32_t maxNumOfRows) {
  int32_t numOfRows = taosNumOfRemainRows(pFillInfo);

  TSKEY ekey1 = ekey;

  int64_t numOfRes = -1;
  if (numOfRows > 0) {  // still fill gap within current data block, not generating data after the result set.
    SColumnInfoData* pCol = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, pFillInfo->srcTsSlotId);
    int64_t*         tsList = (int64_t*)pCol->pData;
    TSKEY            lastKey = tsList[pFillInfo->numOfRows - 1];
    numOfRes =
        taosTimeCountIntervalForFill(lastKey, pFillInfo->currentKey, pFillInfo->interval.sliding,
                                     pFillInfo->interval.slidingUnit, pFillInfo->interval.precision, pFillInfo->order);
  } else {  // reach the end of data
    if ((ekey1 < pFillInfo->currentKey && FILL_IS_ASC_FILL(pFillInfo)) ||
        (ekey1 > pFillInfo->currentKey && !FILL_IS_ASC_FILL(pFillInfo))) {
      return 0;
    }

    numOfRes =
        taosTimeCountIntervalForFill(ekey1, pFillInfo->currentKey, pFillInfo->interval.sliding,
                                     pFillInfo->interval.slidingUnit, pFillInfo->interval.precision, pFillInfo->order);
  }

  return (numOfRes > maxNumOfRows) ? maxNumOfRows : numOfRes;
}

void taosGetLinearInterpolationVal(SPoint* point, int32_t outputType, SPoint* point1, SPoint* point2, int32_t inputType,
                                   STypeMod inputTypeMod) {
  double v1 = -1, v2 = -1;
  GET_TYPED_DATA(v1, double, inputType, point1->val, inputTypeMod);
  GET_TYPED_DATA(v2, double, inputType, point2->val, inputTypeMod);

  double r = 0;
  if (!IS_BOOLEAN_TYPE(inputType)) {
    r = DO_INTERPOLATION(v1, v2, point1->key, point2->key, point->key);
  } else {
    r = (v1 < 1 || v2 < 1) ? 0 : 1;
  }
  SET_TYPED_DATA(point->val, outputType, r);
}

int32_t taosFillResultDataBlock(SFillInfo* pFillInfo, SSDataBlock* p, int32_t capacity) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t remain = taosNumOfRemainRows(pFillInfo);

  int64_t numOfRes = getNumOfResultsAfterFillGap(pFillInfo, pFillInfo->end, capacity);
  QUERY_CHECK_CONDITION((numOfRes <= capacity), code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);

  // no data existed for fill operation now, append result according to the fill strategy
  if (remain == 0) {
    code = appendFilledResult(pFillInfo, p, numOfRes);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    code = fillResultImpl(pFillInfo, p, (int32_t)numOfRes);
    QUERY_CHECK_CODE(code, lino, _end);
    QUERY_CHECK_CONDITION((numOfRes == pFillInfo->numOfCurrent), code, lino, _end,
                          TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }

  qDebug("fill:%p, generated fill result, src block:%d, index:%d, brange:%" PRId64 "-%" PRId64 ", currentKey:%" PRId64
         ", current : % d, total : % d, %s",
         pFillInfo, pFillInfo->numOfRows, pFillInfo->index, pFillInfo->start, pFillInfo->end, pFillInfo->currentKey,
         pFillInfo->numOfCurrent, pFillInfo->numOfTotal, pFillInfo->id);
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int64_t getFillInfoStart(struct SFillInfo* pFillInfo) { return pFillInfo->start; }

SFillColInfo* createFillColInfo(SExprInfo* pExpr, int32_t numOfFillExpr, SExprInfo* pNotFillExpr,
                                int32_t numOfNoFillExpr, SExprInfo* pFillNullExpr, int32_t numOfFillNullExpr,
                                const struct SNodeListNode* pValNode) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       lino = 0;
  SFillColInfo* pFillCol = taosMemoryCalloc(numOfFillExpr + numOfNoFillExpr + numOfFillNullExpr, sizeof(SFillColInfo));
  if (pFillCol == NULL) {
    return NULL;
  }

  size_t len = (pValNode != NULL) ? LIST_LENGTH(pValNode->pNodeList) : 0;
  for (int32_t i = 0; i < numOfFillExpr; ++i) {
    SExprInfo* pExprInfo = &pExpr[i];
    pFillCol[i].pExpr = pExprInfo;
    pFillCol[i].notFillCol = false;

    // todo refactor
    if (len > 0) {
      // if the user specified value is less than the column, alway use the last one as the fill value
      int32_t index = (i >= len) ? (len - 1) : i;

      SValueNode* pv = (SValueNode*)nodesListGetNode(pValNode->pNodeList, index);
      QUERY_CHECK_NULL(pv, code, lino, _end, terrno);
      code = nodesValueNodeToVariant(pv, &pFillCol[i].fillVal);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    if (TSDB_CODE_SUCCESS != code) {
      goto _end;
    }
  }
  pFillCol->numOfFillExpr = numOfFillExpr;

  for (int32_t i = 0; i < numOfNoFillExpr; ++i) {
    SExprInfo* pExprInfo = &pNotFillExpr[i];
    pFillCol[i + numOfFillExpr].pExpr = pExprInfo;
    pFillCol[i + numOfFillExpr].notFillCol = true;
  }

  for (int32_t i = 0; i < numOfFillNullExpr; ++i) {
    SExprInfo* pExprInfo = &pFillNullExpr[i];
    pFillCol[i + numOfFillExpr + numOfNoFillExpr].pExpr = pExprInfo;
    pFillCol[i + numOfFillExpr + numOfNoFillExpr].notFillCol = true;
    pFillCol[i + numOfFillExpr + numOfNoFillExpr].fillNull = true;
  }

  return pFillCol;

_end:
  for (int32_t i = 0; i < numOfFillExpr; ++i) {
    taosVariantDestroy(&pFillCol[i].fillVal);
  }
  taosMemoryFree(pFillCol);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return NULL;
}

static bool fillShouldPause(SFillInfo* pFillInfo, const SSDataBlock* pDstBlock) {
  if (pFillInfo->pSrcBlock && pFillInfo->index >= pFillInfo->pSrcBlock->info.rows) return true;
  if (pDstBlock->info.rows > 0) return true;
  if (pFillInfo->numOfRows == 0) return true;
  if (pFillInfo->order == TSDB_ORDER_ASC && pFillInfo->currentKey > pFillInfo->end) return true;
  if (pFillInfo->order == TSDB_ORDER_DESC && pFillInfo->currentKey < pFillInfo->end) return true;
  if (pDstBlock->info.rows > 0) return true;
  return false;
}

static TSKEY getBlockCurTs(const struct SFillInfo* pFillInfo, const SSDataBlock* pBlock, int32_t rowIdx) {
  if (pBlock) {
    SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, pFillInfo->srcTsSlotId);
    return ((TSKEY*)pTsCol->pData)[rowIdx];
  }
  return -1;
}

static int32_t copyCurrentRowIntoBuf2(SFillInfo* pFillInfo, int32_t rowIndex, SRowVal* pRowVal, TSKEY blockCurTs) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  bool             ascFill = FILL_IS_ASC_FILL(pFillInfo);
  int32_t          fillType = pFillInfo->type;
  bool             fillNext = fillType == TSDB_FILL_NEXT, fillPrev = fillType == TSDB_FILL_PREV;
  bool             ascNext = ascFill && fillNext, descPrev = !ascFill && fillPrev;
  SColumnInfoData* pTsCol = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, pFillInfo->srcTsSlotId);
  QUERY_CHECK_NULL(pTsCol, code, lino, _end, terrno);
  pRowVal->key = ((int64_t*)pTsCol->pData)[rowIndex];

  for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    int32_t type = pFillInfo->pFillCol[i].pExpr->pExpr->nodeType;
    if (type == QUERY_NODE_COLUMN || type == QUERY_NODE_OPERATOR || type == QUERY_NODE_FUNCTION) {
      int32_t srcSlotId = GET_DEST_SLOT_ID(&pFillInfo->pFillCol[i]);

      if (blockCurTs != pFillInfo->currentKey) {
        if (!pFillInfo->pFillCol[i].notFillCol) {
          if (!ascNext && !descPrev) continue;
        }
        if (srcSlotId == pFillInfo->srcTsSlotId && pFillInfo->type == TSDB_FILL_LINEAR) {
          continue;
        }
      }

      SColumnInfoData* pSrcCol = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, srcSlotId);
      QUERY_CHECK_NULL(pSrcCol, code, lino, _end, terrno);

      bool  isNull = colDataIsNull_s(pSrcCol, rowIndex);
      char* p = colDataGetData(pSrcCol, rowIndex);
      // only save null value flag for fill prev/next and need fill cols
      if (!pFillInfo->pFillCol[i].notFillCol && pRowVal->pNullValueFlag && (fillNext || fillPrev)) {
        bool* pNullValueFlag = taosArrayGet(pRowVal->pNullValueFlag, i);
        *pNullValueFlag = isNull;
        bool ascPrevOrDescNext = (fillPrev && ascFill) || (fillNext && !ascFill);
        // For ascPrev and descNext, we do not save NULL values into prev/next pRowVal, cause the last prev or last next
        // will be used for later filling, should not use NULL to override the last value
        if (isNull && ascPrevOrDescNext) continue;
      }

      saveColData(pRowVal->pRowVal, i, p, isNull);
    } else {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t fillTrySaveRow(struct SFillInfo* pFillInfo, const SSDataBlock* pBlock, int32_t rowIdx) {
  if (!pBlock) return 0;
  int32_t  code = 0;
  bool     ascFill = FILL_IS_ASC_FILL(pFillInfo);
  TSKEY    blockCurKey = getBlockCurTs(pFillInfo, pBlock, rowIdx);
  int32_t  fillType = pFillInfo->type;
  SRowVal* pFillRow = ascFill ? (fillType == TSDB_FILL_NEXT ? &pFillInfo->next : &pFillInfo->prev)
                              : (fillType == TSDB_FILL_PREV ? &pFillInfo->next : &pFillInfo->prev);
  return copyCurrentRowIntoBuf2(pFillInfo, rowIdx, pFillRow, blockCurKey);
}

static int32_t tIsColFallBehind(struct SFillInfo* pFillInfo, int32_t colIdx) {
  SColumnFillProgress* pColProgress = taosArrayGet(pFillInfo->pColFillProgress, colIdx);
  if (!pColProgress) {
    qError("failed to get col progress for col %d, size: %lu", colIdx, taosArrayGetSize(pFillInfo->pColFillProgress));
    return TSDB_CODE_INTERNAL_ERROR;
  }
  if (pColProgress->pBlockNode) {
    return true;
  }
  return false;
}

static bool tFillTrySaveColProgress(struct SFillInfo* pFillInfo, int32_t colIdx, SListNode* pBlockNode,
                                    int32_t rowIdx) {
  bool ascNext = pFillInfo->type == TSDB_FILL_NEXT && pFillInfo->order == TSDB_ORDER_ASC;
  bool descPrev = pFillInfo->type == TSDB_FILL_PREV && pFillInfo->order == TSDB_ORDER_DESC;
  if (ascNext || descPrev) {
    SColumnFillProgress* pProgress = taosArrayGet(pFillInfo->pColFillProgress, colIdx);
    if (!pProgress->pBlockNode) {
      pProgress->pBlockNode = pBlockNode;
      pProgress->rowIdx = rowIdx;
      SFillBlock*         pFillBlock = (SFillBlock*)pBlockNode->data;
      SBlockFillProgress* pFillProg = taosArrayGet(pFillBlock->pFillProgress, colIdx);
      pFillProg->rowIdx = rowIdx;
    }
    return true;
  }
  return false;
}

static bool doFillOneCol(SFillInfo* pFillInfo, SSDataBlock* pBlock, TSKEY ts, int32_t colIdx, int32_t rowIdx,
                         bool outOfBound) {
  int32_t code = 0;
  int32_t lino = 0;
  bool    saveProgress = false;
  // set the other values
  if (pFillInfo->type == TSDB_FILL_PREV) {
    SFillColInfo* pCol = &pFillInfo->pFillCol[colIdx];

    SColumnInfoData* pDstColInfoData = taosArrayGet(pBlock->pDataBlock, GET_DEST_SLOT_ID(pCol));
    bool             filled = fillIfWindowPseudoColumn(pFillInfo, pCol, pDstColInfoData, rowIdx);
    if (!filled) {
      saveProgress =
          setNotFillColumn(pFillInfo, pDstColInfoData, rowIdx, colIdx) && pFillInfo->order == TSDB_ORDER_DESC;
    }
  } else if (pFillInfo->type == TSDB_FILL_NEXT) {
    // todo  refactor: start from 0 not 1
    SFillColInfo*    pCol = &pFillInfo->pFillCol[colIdx];
    SColumnInfoData* pDstColInfoData = taosArrayGet(pBlock->pDataBlock, GET_DEST_SLOT_ID(pCol));
    bool             filled = fillIfWindowPseudoColumn(pFillInfo, pCol, pDstColInfoData, rowIdx);
    if (!filled) {
      saveProgress = setNotFillColumn(pFillInfo, pDstColInfoData, rowIdx, colIdx) && pFillInfo->order == TSDB_ORDER_ASC;
    }
  } else if (pFillInfo->type == TSDB_FILL_LINEAR) {
    // TODO : linear interpolation supports NULL value
    if (outOfBound) {
      setNullCol(pBlock, pFillInfo, rowIdx, colIdx);
    } else {
      SFillColInfo* pCol = &pFillInfo->pFillCol[colIdx];

      int32_t          dstSlotId = GET_DEST_SLOT_ID(pCol);
      SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, dstSlotId);
      int16_t          type = pDstCol->info.type;

      if (pCol->notFillCol) {
        bool filled = fillIfWindowPseudoColumn(pFillInfo, pCol, pDstCol, rowIdx);
        if (!filled) {
          (void)setNotFillColumn(pFillInfo, pDstCol, rowIdx, colIdx);
        }
      } else {
        SRowVal*         pRVal = &pFillInfo->prev;
        SGroupKeys*      pKey = taosArrayGet(pRVal->pRowVal, colIdx);
        int32_t          srcSlotId = GET_DEST_SLOT_ID(pCol);
        SColumnInfoData* pSrcCol = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, srcSlotId);
        if (IS_VAR_DATA_TYPE(type) || type == TSDB_DATA_TYPE_BOOL || pKey->isNull ||
            colDataIsNull_s(pSrcCol, pFillInfo->index)) {
          colDataSetNULL(pDstCol, rowIdx);
        } else {
          SGroupKeys* pKey1 = taosArrayGet(pRVal->pRowVal, pFillInfo->tsSlotId);

          int64_t prevTs = *(int64_t*)pKey1->pData;
          char*   data = colDataGetData(pSrcCol, pFillInfo->index);
          SPoint  point1, point2, point;

          point1 = (SPoint){.key = prevTs, .val = pKey->pData};
          point2 = (SPoint){.key = ts, .val = data};

          int64_t out = 0;
          point = (SPoint){.key = pFillInfo->currentKey, .val = &out};
          taosGetLinearInterpolationVal(&point, type, &point1, &point2, type,
                                        typeGetTypeModFromColInfo(&pDstCol->info));

          code = colDataSetVal(pDstCol, rowIdx, (const char*)&out, false);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }
  } else if (pFillInfo->type == TSDB_FILL_NULL || pFillInfo->type == TSDB_FILL_NULL_F) {  // fill with NULL
    setNullCol(pBlock, pFillInfo, rowIdx, colIdx);
  } else {  // fill with user specified value for each column
    SFillColInfo* pCol = &pFillInfo->pFillCol[colIdx];

    int32_t          slotId = GET_DEST_SLOT_ID(pCol);
    SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, slotId);

    if (pCol->notFillCol) {
      bool filled = fillIfWindowPseudoColumn(pFillInfo, pCol, pDst, rowIdx);
      if (!filled) {
        (void)setNotFillColumn(pFillInfo, pDst, rowIdx, colIdx);
      }
    } else {
      SVariant* pVar = &pFillInfo->pFillCol[colIdx].fillVal;
      code = doSetUserSpecifiedValue(pDst, pVar, rowIdx, pFillInfo->currentKey);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pFillInfo->pTaskInfo->env, code);
  }
  return saveProgress;
}

static int32_t tFillFromHeadForCol(struct SFillInfo* pFillInfo, TSKEY ts, int32_t colIdx, bool outOfBound) {
  int32_t code = 0;
  // Check the progress of this col, start fill from the start block
  // Here we will always fill till the last row of last block in list. Cause this is always the first time we meet
  // non-null value after fill till current key, we should update it's progress, set no lag for this col
  SColumnFillProgress* pColProgress = taosArrayGet(pFillInfo->pColFillProgress, colIdx);
  if (!pColProgress) {
    qError("failed to get col progress for col %d, size: %lu", colIdx, taosArrayGetSize(pFillInfo->pColFillProgress));
    return TSDB_CODE_INTERNAL_ERROR;
  }
  SListNode* pListNode = pColProgress->pBlockNode;

  while (pListNode) {
    SFillBlock*                pFillBlock = (SFillBlock*)pListNode->data;
    const SColumnFillProgress* pProgress = taosArrayGet(pFillInfo->pColFillProgress, colIdx);
    for (int32_t rowIdx = pProgress->rowIdx; rowIdx < pFillBlock->pBlock->info.rows; ++rowIdx) {
      TAOS_UNUSED(doFillOneCol(pFillInfo, pFillBlock->pBlock, ts, colIdx, rowIdx, outOfBound));
    }
    SBlockFillProgress* pMyBlockProgress = taosArrayGet(pFillBlock->pFillProgress, colIdx);
    pMyBlockProgress->rowIdx = pFillBlock->pBlock->info.rows;
    bool allColFinished = true;
    for (int32_t i = 0; i < taosArrayGetSize(pFillBlock->pFillProgress); ++i) {
      SBlockFillProgress* pBProgress = taosArrayGet(pFillBlock->pFillProgress, i);
      if (pBProgress->rowIdx < pFillBlock->pBlock->info.rows) {
        allColFinished = false;
        break;
      }
    }
    pFillBlock->allColFinished = allColFinished;
    pListNode = TD_DLIST_NODE_NEXT(pListNode);
    // update progress
    pColProgress->pBlockNode = pListNode;
    pColProgress->rowIdx = 0;
  }
  return code;
}

static void doFillOneRow2(SFillInfo* pFillInfo, SSDataBlock* pBlock, SSDataBlock* pSrcBlock, int64_t ts,
                          bool outOfBound) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pFillInfo->order);
  int32_t rowIdx = pBlock->info.rows;

  // set the primary timestamp column value
  for (int32_t colIdx = 0; code == 0 && colIdx < pFillInfo->numOfCols; ++colIdx) {
    if (outOfBound && tIsColFallBehind(pFillInfo, colIdx)) {
      code = tFillFromHeadForCol(pFillInfo, ts, colIdx, true);
      if (code != 0) goto _end;
    }
    bool saveProgress = doFillOneCol(pFillInfo, pBlock, ts, colIdx, rowIdx, outOfBound);
    // if this col meet a null value during fill the first time, save it's progress
    if (saveProgress) {
      SListNode* pFillBlockListNode = tdListGetTail(pFillInfo->pFillSavedBlockList);
      (void)tFillTrySaveColProgress(pFillInfo, colIdx, pFillBlockListNode, rowIdx);
    }
  }
  SInterval* pInterval = &pFillInfo->interval;
  pFillInfo->currentKey =
      taosTimeAdd(pFillInfo->currentKey, pInterval->sliding * step, pInterval->slidingUnit, pInterval->precision, NULL);
  pBlock->info.rows += 1;
  pFillInfo->numOfCurrent++;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pFillInfo->pTaskInfo->env, code);
  }
}

static void tryExtractReadyBlocks(struct SFillInfo* pFillInfo, SSDataBlock* pDstBlock, int32_t capacity) {
  SListNode* pListNode = tdListGetHead(pFillInfo->pFillSavedBlockList);
  bool       allFinished = true;
  bool       noMoreBlocks = pFillInfo->numOfRows == 0;
  if (pListNode) {
    SFillBlock* pFillBlock = (SFillBlock*)pListNode->data;
    if (!noMoreBlocks) {
      if (pFillBlock->pBlock->info.rows < capacity) return;
      for (int32_t colIdx = 0; colIdx < pFillInfo->numOfCols; ++colIdx) {
        SColumnFillProgress* pProg = taosArrayGet(pFillInfo->pColFillProgress, colIdx);
        if (pProg->pBlockNode == pListNode) {
          allFinished = false;
          break;
        }
      }
    }
    if (allFinished || noMoreBlocks) {
      TSWAP(pDstBlock->info.rows, pFillBlock->pBlock->info.rows);
      TSWAP(pDstBlock->pDataBlock, pFillBlock->pBlock->pDataBlock);
      SListNode *tNode = tdListPopNode(pFillInfo->pFillSavedBlockList, pListNode);
      destroyFillBlock(pListNode->data);
      taosMemFreeClear(pListNode);
    }
  }
}

static SSDataBlock* createNewSavedBlock(struct SFillInfo* pFillInfo, SSDataBlock* pDstBlock, int32_t capacity) {
  int32_t      code = 0;
  SSDataBlock* pBlock = NULL;
  code = createOneDataBlock(pDstBlock, false, &pBlock);
  if (code != 0) return NULL;
  code = blockDataEnsureCapacity(pBlock, capacity);
  if (code != 0) {
    blockDataDestroy(pBlock);
    return NULL;
  }
  return pBlock;
}

static int32_t trySaveNewBlock(struct SFillInfo* pFillInfo, SSDataBlock* pDstBlock, int32_t capacity,
                               SFillBlock** ppFillBlock) {
  int32_t      code = 0;
  SSDataBlock* pBlock = createNewSavedBlock(pFillInfo, pDstBlock, capacity);
  if (!pBlock) {
    code = terrno;
    goto _end;
  }
  SArray* pProgress = taosArrayInit(pFillInfo->numOfCols, sizeof(SBlockFillProgress));
  if (!pProgress) {
    code = terrno;
    goto _end;
  }
  SBlockFillProgress prog = {INT32_MAX};
  for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    if (NULL == taosArrayPush(pProgress, &prog)) {
      code = terrno;
      goto _end;
    }
  }
  *ppFillBlock = tFillSaveBlock(pFillInfo, pBlock, pProgress);
  if (!*ppFillBlock) {
    code = terrno;
    goto _end;
  }
  return 0;
_end:
  if (pBlock) blockDataDestroy(pBlock);
  if (pProgress) taosArrayDestroy(pProgress);
  return code;
}

static int32_t fillInitSavedBlockList(struct SFillInfo* pFillInfo, SSDataBlock* pDstBlock, int32_t capacity) {
  int32_t     code = 0;
  SFillBlock* pFillBlock = NULL;
  pFillInfo->pFillSavedBlockList = tdListNew(sizeof(SFillBlock));
  if (!pFillInfo->pFillSavedBlockList) return terrno;
  code = trySaveNewBlock(pFillInfo, pDstBlock, capacity, &pFillBlock);
  if (code != 0) return code;

  pFillInfo->pColFillProgress = taosArrayInit(pFillInfo->numOfCols, sizeof(SColumnFillProgress));
  if (!pFillInfo->pColFillProgress) {
    return terrno;
  }
  SColumnFillProgress prog = {.pBlockNode = NULL, .rowIdx = 0};
  for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    if (NULL == taosArrayPush(pFillInfo->pColFillProgress, &prog)) {
      return terrno;
    }
  }
  return code;
}

static void tryResetColNextPrev(struct SFillInfo* pFillInfo, int32_t colIdx) {
  bool    ascFill = FILL_IS_ASC_FILL(pFillInfo);
  int32_t fillType = pFillInfo->type;
  bool    ascNext = ascFill && fillType == TSDB_FILL_NEXT, descPrev = !ascFill && fillType == TSDB_FILL_PREV;
  if ((ascNext || descPrev) && !pFillInfo->pFillCol[colIdx].notFillCol) {
    SRowVal*    pFillRow = ascFill ? (fillType == TSDB_FILL_NEXT ? &pFillInfo->next : &pFillInfo->prev)
                                   : (fillType == TSDB_FILL_NEXT ? &pFillInfo->prev : &pFillInfo->next);
    SGroupKeys* pKey = taosArrayGet(pFillRow->pRowVal, colIdx);
    pKey->isNull = true;
  }
}

int32_t taosFillResultDataBlock2(struct SFillInfo* pFillInfo, SSDataBlock* pDstBlock, int32_t capacity,
                                 bool* wantMoreBlock) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  bool        ascFill = FILL_IS_ASC_FILL(pFillInfo);
  SFillBlock* pFillBlock = NULL;
  SListNode*  pFillBlockListNode = NULL;
  pFillInfo->numOfCurrent = 0;

  if (!pFillInfo->pFillSavedBlockList) {
    code = fillInitSavedBlockList(pFillInfo, pDstBlock, capacity);
    if (code != 0) goto _end;
  }
  pFillBlockListNode = tdListGetTail(pFillInfo->pFillSavedBlockList);
  pFillBlock = pFillBlockListNode ? (SFillBlock*)pFillBlockListNode->data : NULL;

  // if all blocks are consumed, we have to fill for not filled cols
  if (pFillInfo->numOfRows == 0) {
    if (!pFillBlock) {
      code = trySaveNewBlock(pFillInfo, pDstBlock, capacity, &pFillBlock);
      if (code != 0) goto _end;
      pFillBlockListNode = tdListGetTail(pFillInfo->pFillSavedBlockList);
    }
    bool allFilled = pFillInfo->order == TSDB_ORDER_ASC ? pFillInfo->currentKey > pFillInfo->end
                                                        : pFillInfo->currentKey < pFillInfo->end;
    while (!allFilled && pFillBlock->pBlock->info.rows < capacity) {
      doFillOneRow2(pFillInfo, pFillBlock->pBlock, pFillInfo->pSrcBlock, pFillInfo->start, true);
      allFilled = pFillInfo->order == TSDB_ORDER_ASC ? pFillInfo->currentKey > pFillInfo->end
                                                     : pFillInfo->currentKey < pFillInfo->end;
    }

    for (int32_t colIdx = 0; colIdx < pFillInfo->numOfCols; ++colIdx) {
      if (tIsColFallBehind(pFillInfo, colIdx)) {
        code = tFillFromHeadForCol(pFillInfo, pFillInfo->start, colIdx, true);
        if (code != 0) goto _end;
      }
    }
  }

  // check from list head if we have already filled all rows in blocks, if any block is full, send it out
  tryExtractReadyBlocks(pFillInfo, pDstBlock, capacity);
  TSKEY lastSavedTs = -1;
  while (!fillShouldPause(pFillInfo, pDstBlock)) {
    if (!pFillBlock || pFillBlock->pBlock->info.rows >= capacity) {
      code = trySaveNewBlock(pFillInfo, pDstBlock, capacity, &pFillBlock);
      if (code != 0) goto _end;
      pFillBlockListNode = tdListGetTail(pFillInfo->pFillSavedBlockList);
    }
    TSKEY fillCurTs = pFillInfo->currentKey;
    TSKEY blockCurTs = getBlockCurTs(pFillInfo, pFillInfo->pSrcBlock, pFillInfo->index);
    if (pFillInfo->pSrcBlock && (lastSavedTs != blockCurTs || blockCurTs == fillCurTs))
      code = fillTrySaveRow(pFillInfo, pFillInfo->pSrcBlock, pFillInfo->index);
    lastSavedTs = blockCurTs;
    if (code != 0) goto _end;

    if (blockCurTs != fillCurTs || !pFillInfo->pSrcBlock) {
      doFillOneRow2(pFillInfo, pFillBlock->pBlock, pFillInfo->pSrcBlock, blockCurTs, false);
    } else {
      for (int32_t colIdx = 0; colIdx < pFillInfo->numOfCols; ++colIdx) {
        SFillColInfo*    pCol = &pFillInfo->pFillCol[colIdx];
        int32_t          rowIdx = pFillBlock->pBlock->info.rows;
        int32_t          dstSlotId = GET_DEST_SLOT_ID(pCol);
        SColumnInfoData* pDst = taosArrayGet(pFillBlock->pBlock->pDataBlock, dstSlotId);
        SColumnInfoData* pSrc = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, dstSlotId);

        char* src = colDataGetData(pSrc, pFillInfo->index);
        if (!colDataIsNull_s(pSrc, pFillInfo->index)) {
          if (tIsColFallBehind(pFillInfo, colIdx)) code = tFillFromHeadForCol(pFillInfo, blockCurTs, colIdx, false);
          QUERY_CHECK_CODE(code, lino, _end);
          code = colDataSetVal(pDst, rowIdx, src, false);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          // if col value in block is NULL, skip setting value for this col, save current position, wait till we got
          // non-null data if there is no lag for this col, then we should fill from (pFillBlock, index) when we got
          // non-null value. if this col is already fall behind, do nothing. Cause when we meet non-null value for this
          // col, we will fill till the last row of last block in list.
          bool saved = tFillTrySaveColProgress(pFillInfo, colIdx, pFillBlockListNode, rowIdx);
          if (!saved) {
            TAOS_UNUSED(doFillOneCol(pFillInfo, pFillBlock->pBlock, blockCurTs, colIdx, rowIdx, false));
          }
        }
        tryResetColNextPrev(pFillInfo, colIdx);
      }
      SInterval* pInterval = &pFillInfo->interval;
      pFillInfo->currentKey =
          taosTimeAdd(pFillInfo->currentKey, pInterval->sliding * GET_FORWARD_DIRECTION_FACTOR(pFillInfo->order),
                      pInterval->slidingUnit, pInterval->precision, NULL);
      pFillBlock->pBlock->info.rows += 1;
      pFillInfo->index += 1;
      pFillInfo->numOfCurrent += 1;
    }
    tryExtractReadyBlocks(pFillInfo, pDstBlock, capacity);
  }

_end:
  pFillInfo->numOfTotal += pFillInfo->numOfCurrent;
  if (!isListEmpty(pFillInfo->pFillSavedBlockList)) {
    if (wantMoreBlock) *wantMoreBlock = true;
  } else {
    if (wantMoreBlock) *wantMoreBlock = false;
  }
  return code;
}

void destroyFillBlock(void* p) {
  SFillBlock* pFillBlock = p;
  taosArrayDestroy(pFillBlock->pFillProgress);
  blockDataDestroy(pFillBlock->pBlock);
}

SFillBlock* tFillSaveBlock(SFillInfo* pFill, SSDataBlock* pBlock, SArray* pProgress) {
  SFillBlock block = {.pBlock = pBlock, .pFillProgress = pProgress, .allColFinished = false};
  SListNode* pNode = tdListAdd(pFill->pFillSavedBlockList, &block);
  if (!pNode) {
    return NULL;
  }
  return (SFillBlock*)pNode->data;
}
