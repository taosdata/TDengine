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
#include "tdatablock.h"
#include "tfill.h"

#define FILL_IS_ASC_FILL(_f) ((_f)->order == TSDB_ORDER_ASC)
#define DO_INTERPOLATION(_v1, _v2, _k1, _k2, _k) \
  ((_v1) + ((_v2) - (_v1)) * (((double)(_k)) - ((double)(_k1))) / (((double)(_k2)) - ((double)(_k1))))

static void doSetVal(SColumnInfoData* pDstColInfoData, int32_t rowIndex, const SGroupKeys* pKey);

static void setNotFillColumn(SFillInfo* pFillInfo, SColumnInfoData* pDstColInfo, int32_t rowIndex, int32_t colIdx) {
  SRowVal* p = NULL;
  if (pFillInfo->type == TSDB_FILL_NEXT) {
    p = FILL_IS_ASC_FILL(pFillInfo) ? &pFillInfo->next : &pFillInfo->prev;
  } else {
    p = FILL_IS_ASC_FILL(pFillInfo) ? &pFillInfo->prev : &pFillInfo->next;
  }

  SGroupKeys* pKey = taosArrayGet(p->pRowVal, colIdx);
  doSetVal(pDstColInfo, rowIndex, pKey);
}

static void setNullRow(SSDataBlock* pBlock, SFillInfo* pFillInfo, int32_t rowIndex) {
  for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    SFillColInfo*    pCol = &pFillInfo->pFillCol[i];
    int32_t          dstSlotId = GET_DEST_SLOT_ID(pCol);
    SColumnInfoData* pDstColInfo = taosArrayGet(pBlock->pDataBlock, dstSlotId);
    if (pCol->notFillCol) {
      bool filled = fillIfWindowPseudoColumn(pFillInfo, pCol, pDstColInfo, rowIndex);
      if (!filled) {
        setNotFillColumn(pFillInfo, pDstColInfo, rowIndex, i);
      }
    } else {
      colDataSetNULL(pDstColInfo, rowIndex);
    }
  }
}

static void doSetUserSpecifiedValue(SColumnInfoData* pDst, SVariant* pVar, int32_t rowIndex, int64_t currentKey) {
  bool isNull = (TSDB_DATA_TYPE_NULL == pVar->nType) ? true : false;
  if (pDst->info.type == TSDB_DATA_TYPE_FLOAT) {
    float v = 0;
    GET_TYPED_DATA(v, float, pVar->nType, &pVar->f);
    colDataSetVal(pDst, rowIndex, (char*)&v, isNull);
  } else if (pDst->info.type == TSDB_DATA_TYPE_DOUBLE) {
    double v = 0;
    GET_TYPED_DATA(v, double, pVar->nType, &pVar->d);
    colDataSetVal(pDst, rowIndex, (char*)&v, isNull);
  } else if (IS_SIGNED_NUMERIC_TYPE(pDst->info.type) || pDst->info.type == TSDB_DATA_TYPE_BOOL) {
    int64_t v = 0;
    GET_TYPED_DATA(v, int64_t, pVar->nType, &pVar->i);
    colDataSetVal(pDst, rowIndex, (char*)&v, isNull);
  } else if (IS_UNSIGNED_NUMERIC_TYPE(pDst->info.type)) {
    uint64_t v = 0;
    GET_TYPED_DATA(v, uint64_t, pVar->nType, &pVar->u);
    colDataSetVal(pDst, rowIndex, (char*)&v, isNull);
  } else if (pDst->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
    colDataSetVal(pDst, rowIndex, (const char*)&currentKey, isNull);
  } else if (pDst->info.type == TSDB_DATA_TYPE_NCHAR || pDst->info.type == TSDB_DATA_TYPE_VARCHAR ||
             pDst->info.type == TSDB_DATA_TYPE_VARBINARY) {
    colDataSetVal(pDst, rowIndex, pVar->pz, isNull);
  } else {  // others data
    colDataSetNULL(pDst, rowIndex);
  }
}

// fill windows pseudo column, _wstart, _wend, _wduration and return true, otherwise return false
bool fillIfWindowPseudoColumn(SFillInfo* pFillInfo, SFillColInfo* pCol, SColumnInfoData* pDstColInfoData,
                              int32_t rowIndex) {
  if (!pCol->notFillCol) {
    return false;
  }
  if (pCol->pExpr->pExpr->nodeType == QUERY_NODE_COLUMN) {
    if (pCol->pExpr->base.numOfParams != 1) {
      return false;
    }
    if (pCol->pExpr->base.pParam[0].pCol->colType == COLUMN_TYPE_WINDOW_START) {
      colDataSetVal(pDstColInfoData, rowIndex, (const char*)&pFillInfo->currentKey, false);
      return true;
    } else if (pCol->pExpr->base.pParam[0].pCol->colType == COLUMN_TYPE_WINDOW_END) {
      // TODO: include endpoint
      SInterval* pInterval = &pFillInfo->interval;
      int64_t    windowEnd =
          taosTimeAdd(pFillInfo->currentKey, pInterval->interval, pInterval->intervalUnit, pInterval->precision);
      colDataSetVal(pDstColInfoData, rowIndex, (const char*)&windowEnd, false);
      return true;
    } else if (pCol->pExpr->base.pParam[0].pCol->colType == COLUMN_TYPE_WINDOW_DURATION) {
      // TODO: include endpoint
      colDataSetVal(pDstColInfoData, rowIndex, (const char*)&pFillInfo->interval.sliding, false);
      return true;
    }
  }
  return false;
}

static void doFillOneRow(SFillInfo* pFillInfo, SSDataBlock* pBlock, SSDataBlock* pSrcBlock, int64_t ts,
                         bool outOfBound) {
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
        setNotFillColumn(pFillInfo, pDstColInfoData, index, i);
      }
    }
  } else if (pFillInfo->type == TSDB_FILL_NEXT) {
    // todo  refactor: start from 0 not 1
    for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
      SFillColInfo*    pCol = &pFillInfo->pFillCol[i];
      SColumnInfoData* pDstColInfoData = taosArrayGet(pBlock->pDataBlock, GET_DEST_SLOT_ID(pCol));
      bool             filled = fillIfWindowPseudoColumn(pFillInfo, pCol, pDstColInfoData, index);
      if (!filled) {
        setNotFillColumn(pFillInfo, pDstColInfoData, index, i);
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
            setNotFillColumn(pFillInfo, pDstCol, index, i);
          }
        } else {
          SRowVal* pRVal = FILL_IS_ASC_FILL(pFillInfo) ? &pFillInfo->prev : &pFillInfo->next;
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
          taosGetLinearInterpolationVal(&point, type, &point1, &point2, type);

          colDataSetVal(pDstCol, index, (const char*)&out, false);
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
          setNotFillColumn(pFillInfo, pDst, index, i);
        }
      } else {
        SVariant* pVar = &pFillInfo->pFillCol[i].fillVal;
        doSetUserSpecifiedValue(pDst, pVar, index, pFillInfo->currentKey);
      }
    }
  }

  //  setTagsValue(pFillInfo, data, index);
  SInterval* pInterval = &pFillInfo->interval;
  pFillInfo->currentKey =
      taosTimeAdd(pFillInfo->currentKey, pInterval->sliding * step, pInterval->slidingUnit, pInterval->precision);
  pBlock->info.rows += 1;
  pFillInfo->numOfCurrent++;
}

void doSetVal(SColumnInfoData* pDstCol, int32_t rowIndex, const SGroupKeys* pKey) {
  if (pKey->isNull) {
    colDataSetNULL(pDstCol, rowIndex);
  } else {
    colDataSetVal(pDstCol, rowIndex, pKey->pData, false);
  }
}

static void initBeforeAfterDataBuf(SFillInfo* pFillInfo) {
  if (taosArrayGetSize(pFillInfo->next.pRowVal) > 0) {
    return;
  }

  for (int i = 0; i < pFillInfo->numOfCols; i++) {
    SFillColInfo* pCol = &pFillInfo->pFillCol[i];

    SGroupKeys  key = {0};
    SResSchema* pSchema = &pCol->pExpr->base.resSchema;
    key.pData = taosMemoryMalloc(pSchema->bytes);
    key.isNull = true;
    key.bytes = pSchema->bytes;
    key.type = pSchema->type;

    taosArrayPush(pFillInfo->next.pRowVal, &key);

    key.pData = taosMemoryMalloc(pSchema->bytes);
    taosArrayPush(pFillInfo->prev.pRowVal, &key);
  }
}

static void saveColData(SArray* rowBuf, int32_t columnIndex, const char* src, bool isNull);

static void copyCurrentRowIntoBuf(SFillInfo* pFillInfo, int32_t rowIndex, SRowVal* pRowVal, bool reset) {
  SColumnInfoData* pTsCol = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, pFillInfo->srcTsSlotId);
  pRowVal->key = ((int64_t*)pTsCol->pData)[rowIndex];

  for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    int32_t type = pFillInfo->pFillCol[i].pExpr->pExpr->nodeType;
    if ( type == QUERY_NODE_COLUMN || type == QUERY_NODE_OPERATOR || type == QUERY_NODE_FUNCTION) {
      if (!pFillInfo->pFillCol[i].notFillCol && pFillInfo->type != TSDB_FILL_NEXT) {
        continue;
      }
      int32_t srcSlotId = GET_DEST_SLOT_ID(&pFillInfo->pFillCol[i]);

      if (srcSlotId == pFillInfo->srcTsSlotId && pFillInfo->type == TSDB_FILL_LINEAR) {
        continue;
      }

      SColumnInfoData* pSrcCol = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, srcSlotId);

      bool  isNull = colDataIsNull_s(pSrcCol, rowIndex);
      char* p = colDataGetData(pSrcCol, rowIndex);

      saveColData(pRowVal->pRowVal, i, p, reset ? true : isNull);
    } else {
      ASSERT(0);
    }
  }
}

static int32_t fillResultImpl(SFillInfo* pFillInfo, SSDataBlock* pBlock, int32_t outputRows) {
  pFillInfo->numOfCurrent = 0;

  SColumnInfoData* pTsCol = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, pFillInfo->srcTsSlotId);

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pFillInfo->order);
  bool    ascFill = FILL_IS_ASC_FILL(pFillInfo);

#if 0
  ASSERT(ascFill && (pFillInfo->currentKey >= pFillInfo->start) || (!ascFill && (pFillInfo->currentKey <= pFillInfo->start)));
#endif

  while (pFillInfo->numOfCurrent < outputRows) {
    int64_t ts = ((int64_t*)pTsCol->pData)[pFillInfo->index];

    // set the next value for interpolation
    if (pFillInfo->currentKey < ts && ascFill) {
      SRowVal* pRVal = pFillInfo->type == TSDB_FILL_NEXT ? &pFillInfo->next : &pFillInfo->prev;
      copyCurrentRowIntoBuf(pFillInfo, pFillInfo->index, pRVal, false);
    } else if (pFillInfo->currentKey > ts && !ascFill) {
      SRowVal* pRVal = pFillInfo->type == TSDB_FILL_NEXT ? &pFillInfo->prev : &pFillInfo->next;
      copyCurrentRowIntoBuf(pFillInfo, pFillInfo->index, pRVal, false);
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
        return outputRows;
      }
    } else {
      ASSERT(pFillInfo->currentKey == ts);
      int32_t index = pBlock->info.rows;

      if (pFillInfo->type == TSDB_FILL_NEXT) {
        int32_t nextRowIndex = pFillInfo->index + 1;
        if ((pFillInfo->index + 1) < pFillInfo->numOfRows) {
          copyCurrentRowIntoBuf(pFillInfo, nextRowIndex, &pFillInfo->next, false);
        } else {
          // reset to null after last row
          copyCurrentRowIntoBuf(pFillInfo, nextRowIndex, &pFillInfo->next, true);
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
          colDataSetVal(pDst, index, src, false);
          SRowVal* pRVal = FILL_IS_ASC_FILL(pFillInfo) ? &pFillInfo->prev : &pFillInfo->next;
          saveColData(pRVal->pRowVal, i, src, false);
          if (pFillInfo->srcTsSlotId == dstSlotId) {
            pRVal->key = *(int64_t*)src;
          }
        } else {  // the value is null
          if (pDst->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
            colDataSetVal(pDst, index, (const char*)&pFillInfo->currentKey, false);
          } else {  // i > 0 and data is null , do interpolation
            if (pFillInfo->type == TSDB_FILL_PREV) {
              SArray*     p = FILL_IS_ASC_FILL(pFillInfo) ? pFillInfo->prev.pRowVal : pFillInfo->next.pRowVal;
              SGroupKeys* pKey = taosArrayGet(p, i);
              doSetVal(pDst, index, pKey);
            } else if (pFillInfo->type == TSDB_FILL_LINEAR) {
              bool isNull = colDataIsNull_s(pSrc, pFillInfo->index);
              colDataSetVal(pDst, index, src, isNull);
              SArray* p = FILL_IS_ASC_FILL(pFillInfo) ? pFillInfo->prev.pRowVal : pFillInfo->next.pRowVal;
              saveColData(p, i, src, isNull);  // todo:
            } else if (pFillInfo->type == TSDB_FILL_NULL || pFillInfo->type == TSDB_FILL_NULL_F) {
              colDataSetNULL(pDst, index);
            } else if (pFillInfo->type == TSDB_FILL_NEXT) {
              SArray*     p = FILL_IS_ASC_FILL(pFillInfo) ? pFillInfo->next.pRowVal : pFillInfo->prev.pRowVal;
              SGroupKeys* pKey = taosArrayGet(p, i);
              doSetVal(pDst, index, pKey);
            } else {
              SVariant* pVar = &pFillInfo->pFillCol[i].fillVal;
              doSetUserSpecifiedValue(pDst, pVar, index, pFillInfo->currentKey);
            }
          }
        }
      }

      // set the tag value for final result
      SInterval* pInterval = &pFillInfo->interval;
      pFillInfo->currentKey =
          taosTimeAdd(pFillInfo->currentKey, pInterval->sliding * step, pInterval->slidingUnit, pInterval->precision);

      pBlock->info.rows += 1;
      pFillInfo->index += 1;
      pFillInfo->numOfCurrent += 1;
    }

    if (pFillInfo->index >= pFillInfo->numOfRows || pFillInfo->numOfCurrent >= outputRows) {
      pFillInfo->numOfTotal += pFillInfo->numOfCurrent;
      return pFillInfo->numOfCurrent;
    }
  }

  return pFillInfo->numOfCurrent;
}

static void saveColData(SArray* rowBuf, int32_t columnIndex, const char* src, bool isNull) {
  SGroupKeys* pKey = taosArrayGet(rowBuf, columnIndex);
  if (isNull) {
    pKey->isNull = true;
  } else {
    if (IS_VAR_DATA_TYPE(pKey->type)) {
      memcpy(pKey->pData, src, varDataTLen(src));
    } else {
      memcpy(pKey->pData, src, pKey->bytes);
    }
    pKey->isNull = false;
  }
}

static int64_t appendFilledResult(SFillInfo* pFillInfo, SSDataBlock* pBlock, int64_t resultCapacity) {
  /*
   * These data are generated according to fill strategy, since the current timestamp is out of the time window of
   * real result set. Note that we need to keep the direct previous result rows, to generated the filled data.
   */
  pFillInfo->numOfCurrent = 0;
  while (pFillInfo->numOfCurrent < resultCapacity) {
    doFillOneRow(pFillInfo, pBlock, pFillInfo->pSrcBlock, pFillInfo->start, true);
  }

  pFillInfo->numOfTotal += pFillInfo->numOfCurrent;

  ASSERT(pFillInfo->numOfCurrent == resultCapacity);
  return resultCapacity;
}

static int32_t taosNumOfRemainRows(SFillInfo* pFillInfo) {
  if (pFillInfo->numOfRows == 0 || (pFillInfo->numOfRows > 0 && pFillInfo->index >= pFillInfo->numOfRows)) {
    return 0;
  }

  return pFillInfo->numOfRows - pFillInfo->index;
}

struct SFillInfo* taosCreateFillInfo(TSKEY skey, int32_t numOfFillCols, int32_t numOfNotFillCols, int32_t capacity,
                                     SInterval* pInterval, int32_t fillType, struct SFillColInfo* pCol,
                                     int32_t primaryTsSlotId, int32_t order, const char* id) {
  if (fillType == TSDB_FILL_NONE) {
    return NULL;
  }

  SFillInfo* pFillInfo = taosMemoryCalloc(1, sizeof(SFillInfo));
  if (pFillInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

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
  pFillInfo->numOfCols = numOfFillCols + numOfNotFillCols;
  pFillInfo->alloc = capacity;
  pFillInfo->id = id;
  pFillInfo->interval = *pInterval;

  pFillInfo->next.pRowVal = taosArrayInit(pFillInfo->numOfCols, sizeof(SGroupKeys));
  pFillInfo->prev.pRowVal = taosArrayInit(pFillInfo->numOfCols, sizeof(SGroupKeys));

  initBeforeAfterDataBuf(pFillInfo);
  return pFillInfo;
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
    taosMemoryFree(pKey->pData);
  }
  taosArrayDestroy(pFillInfo->prev.pRowVal);
  for (int32_t i = 0; i < taosArrayGetSize(pFillInfo->next.pRowVal); ++i) {
    SGroupKeys* pKey = taosArrayGet(pFillInfo->next.pRowVal, i);
    taosMemoryFree(pKey->pData);
  }
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
            pCol->fillVal.nType == TSDB_DATA_TYPE_NCHAR || pCol->fillVal.nType == TSDB_DATA_TYPE_JSON) {
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
  taosMemoryFreeClear(pFillInfo);
  return NULL;
}

void taosFillSetStartInfo(SFillInfo* pFillInfo, int32_t numOfRows, TSKEY endKey) {
  if (pFillInfo->type == TSDB_FILL_NONE) {
    return;
  }

  // the endKey is now the aligned time window value. truncate time window isn't correct.
  pFillInfo->end = endKey;

#if 0
  if (pFillInfo->order == TSDB_ORDER_ASC) {
    ASSERT(pFillInfo->start <= pFillInfo->end);
  } else {
    ASSERT(pFillInfo->start >= pFillInfo->end);
  }
#endif

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

bool taosFillNotStarted(const SFillInfo* pFillInfo) {return pFillInfo->start == pFillInfo->currentKey;}

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
  int32_t  numOfRows = taosNumOfRemainRows(pFillInfo);

  TSKEY ekey1 = ekey;

  int64_t numOfRes = -1;
  if (numOfRows > 0) {  // still fill gap within current data block, not generating data after the result set.
    SColumnInfoData* pCol = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, pFillInfo->srcTsSlotId);    
    int64_t* tsList = (int64_t*)pCol->pData;
    TSKEY lastKey = tsList[pFillInfo->numOfRows - 1];
    numOfRes = taosTimeCountIntervalForFill(lastKey, pFillInfo->currentKey, pFillInfo->interval.sliding,
                                     pFillInfo->interval.slidingUnit, pFillInfo->interval.precision, pFillInfo->order);
    ASSERT(numOfRes >= numOfRows);
  } else {  // reach the end of data
    if ((ekey1 < pFillInfo->currentKey && FILL_IS_ASC_FILL(pFillInfo)) ||
        (ekey1 > pFillInfo->currentKey && !FILL_IS_ASC_FILL(pFillInfo))) {
      return 0;
    }

    numOfRes = taosTimeCountIntervalForFill(ekey1, pFillInfo->currentKey, pFillInfo->interval.sliding,
                                     pFillInfo->interval.slidingUnit, pFillInfo->interval.precision, pFillInfo->order);
  }

  return (numOfRes > maxNumOfRows) ? maxNumOfRows : numOfRes;
}

int32_t taosGetLinearInterpolationVal(SPoint* point, int32_t outputType, SPoint* point1, SPoint* point2,
                                      int32_t inputType) {
  double v1 = -1, v2 = -1;
  GET_TYPED_DATA(v1, double, inputType, point1->val);
  GET_TYPED_DATA(v2, double, inputType, point2->val);

  double r = 0;
  if (!IS_BOOLEAN_TYPE(inputType)) {
    r = DO_INTERPOLATION(v1, v2, point1->key, point2->key, point->key);
  } else {
    r = (v1 < 1 || v2 < 1) ? 0 : 1;
  }
  SET_TYPED_DATA(point->val, outputType, r);

  return TSDB_CODE_SUCCESS;
}

int64_t taosFillResultDataBlock(SFillInfo* pFillInfo, SSDataBlock* p, int32_t capacity) {
  int32_t remain = taosNumOfRemainRows(pFillInfo);

  int64_t numOfRes = getNumOfResultsAfterFillGap(pFillInfo, pFillInfo->end, capacity);
  ASSERT(numOfRes <= capacity);

  // no data existed for fill operation now, append result according to the fill strategy
  if (remain == 0) {
    appendFilledResult(pFillInfo, p, numOfRes);
  } else {
    fillResultImpl(pFillInfo, p, (int32_t)numOfRes);
    ASSERT(numOfRes == pFillInfo->numOfCurrent);
  }

  qDebug("fill:%p, generated fill result, src block:%d, index:%d, brange:%" PRId64 "-%" PRId64 ", currentKey:%" PRId64
         ", current : % d, total : % d, %s",
         pFillInfo, pFillInfo->numOfRows, pFillInfo->index, pFillInfo->start, pFillInfo->end, pFillInfo->currentKey,
         pFillInfo->numOfCurrent, pFillInfo->numOfTotal, pFillInfo->id);

  return numOfRes;
}

int64_t getFillInfoStart(struct SFillInfo* pFillInfo) { return pFillInfo->start; }

SFillColInfo* createFillColInfo(SExprInfo* pExpr, int32_t numOfFillExpr, SExprInfo* pNotFillExpr,
                                int32_t numOfNoFillExpr, const struct SNodeListNode* pValNode) {
  SFillColInfo* pFillCol = taosMemoryCalloc(numOfFillExpr + numOfNoFillExpr, sizeof(SFillColInfo));
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
      nodesValueNodeToVariant(pv, &pFillCol[i].fillVal);
    }
  }
  pFillCol->numOfFillExpr = numOfFillExpr;

  for (int32_t i = 0; i < numOfNoFillExpr; ++i) {
    SExprInfo* pExprInfo = &pNotFillExpr[i];
    pFillCol[i + numOfFillExpr].pExpr = pExprInfo;
    pFillCol[i + numOfFillExpr].notFillCol = true;
  }

  return pFillCol;
}
