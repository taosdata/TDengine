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

#include "os.h"
#include "query.h"
#include "taosdef.h"
#include "tmsg.h"
#include "ttypes.h"

#include "executorimpl.h"
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

#define GET_DEST_SLOT_ID(_p) ((_p)->pExpr->base.resSchema.slotId)

#define FILL_POS_INVALID 0
#define FILL_POS_START   1
#define FILL_POS_MID     2
#define FILL_POS_END     3

typedef struct STimeRange {
  TSKEY    skey;
  TSKEY    ekey;
  uint64_t groupId;
} STimeRange;

static void doSetVal(SColumnInfoData* pDstColInfoData, int32_t rowIndex, const SGroupKeys* pKey);
static bool fillIfWindowPseudoColumn(SFillInfo* pFillInfo, SFillColInfo* pCol, SColumnInfoData* pDstColInfoData,
                                     int32_t rowIndex);

static void setNullRow(SSDataBlock* pBlock, SFillInfo* pFillInfo, int32_t rowIndex) {
  for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    SFillColInfo*    pCol = &pFillInfo->pFillCol[i];
    int32_t          dstSlotId = GET_DEST_SLOT_ID(pCol);
    SColumnInfoData* pDstColInfo = taosArrayGet(pBlock->pDataBlock, dstSlotId);
    if (pCol->notFillCol) {
      bool filled = fillIfWindowPseudoColumn(pFillInfo, pCol, pDstColInfo, rowIndex);
      if (!filled) {
        SArray*     p = FILL_IS_ASC_FILL(pFillInfo) ? pFillInfo->prev.pRowVal : pFillInfo->next.pRowVal;
        SGroupKeys* pKey = taosArrayGet(p, i);
        doSetVal(pDstColInfo, rowIndex, pKey);
      }
    } else {
      colDataAppendNULL(pDstColInfo, rowIndex);
    }
  }
}

static void doSetUserSpecifiedValue(SColumnInfoData* pDst, SVariant* pVar, int32_t rowIndex, int64_t currentKey) {
  if (pDst->info.type == TSDB_DATA_TYPE_FLOAT) {
    float v = 0;
    GET_TYPED_DATA(v, float, pVar->nType, &pVar->i);
    colDataAppend(pDst, rowIndex, (char*)&v, false);
  } else if (pDst->info.type == TSDB_DATA_TYPE_DOUBLE) {
    double v = 0;
    GET_TYPED_DATA(v, double, pVar->nType, &pVar->i);
    colDataAppend(pDst, rowIndex, (char*)&v, false);
  } else if (IS_SIGNED_NUMERIC_TYPE(pDst->info.type)) {
    int64_t v = 0;
    GET_TYPED_DATA(v, int64_t, pVar->nType, &pVar->i);
    colDataAppend(pDst, rowIndex, (char*)&v, false);
  } else if (pDst->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
    colDataAppend(pDst, rowIndex, (const char*)&currentKey, false);
  } else {  // varchar/nchar data
    colDataAppendNULL(pDst, rowIndex);
  }
}

// fill windows pseudo column, _wstart, _wend, _wduration and return true, otherwise return false
static bool fillIfWindowPseudoColumn(SFillInfo* pFillInfo, SFillColInfo* pCol, SColumnInfoData* pDstColInfoData,
                                     int32_t rowIndex) {
  if (!pCol->notFillCol) {
    return false;
  }
  if (pCol->pExpr->pExpr->nodeType == QUERY_NODE_COLUMN) {
    if (pCol->pExpr->base.numOfParams != 1) {
      return false;
    }
    if (pCol->pExpr->base.pParam[0].pCol->colType == COLUMN_TYPE_WINDOW_START) {
      colDataAppend(pDstColInfoData, rowIndex, (const char*)&pFillInfo->currentKey, false);
      return true;
    } else if (pCol->pExpr->base.pParam[0].pCol->colType == COLUMN_TYPE_WINDOW_END) {
      // TODO: include endpoint
      SInterval* pInterval = &pFillInfo->interval;
      int32_t    step = (pFillInfo->order == TSDB_ORDER_ASC) ? 1 : -1;
      int64_t    windowEnd =
          taosTimeAdd(pFillInfo->currentKey, pInterval->sliding * step, pInterval->slidingUnit, pInterval->precision);
      colDataAppend(pDstColInfoData, rowIndex, (const char*)&windowEnd, false);
      return true;
    } else if (pCol->pExpr->base.pParam[0].pCol->colType == COLUMN_TYPE_WINDOW_DURATION) {
      // TODO: include endpoint
      colDataAppend(pDstColInfoData, rowIndex, (const char*)&pFillInfo->interval.sliding, false);
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
    SArray* p = FILL_IS_ASC_FILL(pFillInfo) ? pFillInfo->prev.pRowVal : pFillInfo->next.pRowVal;

    for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
      SFillColInfo* pCol = &pFillInfo->pFillCol[i];

      SColumnInfoData* pDstColInfoData = taosArrayGet(pBlock->pDataBlock, GET_DEST_SLOT_ID(pCol));
      bool             filled = fillIfWindowPseudoColumn(pFillInfo, pCol, pDstColInfoData, index);
      if (!filled) {
        SGroupKeys* pKey = taosArrayGet(p, i);
        doSetVal(pDstColInfoData, index, pKey);
      }
    }
  } else if (pFillInfo->type == TSDB_FILL_NEXT) {
    SArray* p = FILL_IS_ASC_FILL(pFillInfo) ? pFillInfo->next.pRowVal : pFillInfo->prev.pRowVal;
    // todo  refactor: start from 0 not 1
    for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
      SFillColInfo*    pCol = &pFillInfo->pFillCol[i];
      SColumnInfoData* pDstColInfoData = taosArrayGet(pBlock->pDataBlock, GET_DEST_SLOT_ID(pCol));
      bool             filled = fillIfWindowPseudoColumn(pFillInfo, pCol, pDstColInfoData, index);
      if (!filled) {
        SGroupKeys* pKey = taosArrayGet(p, i);
        doSetVal(pDstColInfoData, index, pKey);
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
            SArray*     p = FILL_IS_ASC_FILL(pFillInfo) ? pFillInfo->prev.pRowVal : pFillInfo->next.pRowVal;
            SGroupKeys* pKey = taosArrayGet(p, i);
            doSetVal(pDstCol, index, pKey);
          }
        } else {
          SGroupKeys* pKey = taosArrayGet(pFillInfo->prev.pRowVal, i);
          if (IS_VAR_DATA_TYPE(type) || type == TSDB_DATA_TYPE_BOOL || pKey->isNull) {
            colDataAppendNULL(pDstCol, index);
            continue;
          }

          SGroupKeys* pKey1 = taosArrayGet(pFillInfo->prev.pRowVal, pFillInfo->tsSlotId);

          int64_t prevTs = *(int64_t*)pKey1->pData;
          int32_t srcSlotId = GET_DEST_SLOT_ID(pCol);

          SColumnInfoData* pSrcCol = taosArrayGet(pSrcBlock->pDataBlock, srcSlotId);
          char*            data = colDataGetData(pSrcCol, pFillInfo->index);

          point1 = (SPoint){.key = prevTs, .val = pKey->pData};
          point2 = (SPoint){.key = ts, .val = data};

          int64_t out = 0;
          point = (SPoint){.key = pFillInfo->currentKey, .val = &out};
          taosGetLinearInterpolationVal(&point, type, &point1, &point2, type);

          colDataAppend(pDstCol, index, (const char*)&out, false);
        }
      }
    }
  } else if (pFillInfo->type == TSDB_FILL_NULL) {  // fill with NULL
    setNullRow(pBlock, pFillInfo, index);
  } else {  // fill with user specified value for each column
    for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
      SFillColInfo* pCol = &pFillInfo->pFillCol[i];

      int32_t          slotId = GET_DEST_SLOT_ID(pCol);
      SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, slotId);

      if (pCol->notFillCol) {
        bool filled = fillIfWindowPseudoColumn(pFillInfo, pCol, pDst, index);
        if (!filled) {
          SArray*     p = FILL_IS_ASC_FILL(pFillInfo) ? pFillInfo->prev.pRowVal : pFillInfo->next.pRowVal;
          SGroupKeys* pKey = taosArrayGet(p, i);
          doSetVal(pDst, index, pKey);
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
    colDataAppendNULL(pDstCol, rowIndex);
  } else {
    colDataAppend(pDstCol, rowIndex, pKey->pData, false);
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

static void copyCurrentRowIntoBuf(SFillInfo* pFillInfo, int32_t rowIndex, SArray* pRow) {
  for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    int32_t type = pFillInfo->pFillCol[i].pExpr->pExpr->nodeType;
    if (type == QUERY_NODE_COLUMN) {
      int32_t srcSlotId = GET_DEST_SLOT_ID(&pFillInfo->pFillCol[i]);

      SColumnInfoData* pSrcCol = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, srcSlotId);

      bool  isNull = colDataIsNull_s(pSrcCol, rowIndex);
      char* p = colDataGetData(pSrcCol, rowIndex);
      saveColData(pRow, i, p, isNull);
    } else if (type == QUERY_NODE_OPERATOR) {
      SColumnInfoData* pSrcCol = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, i);

      bool  isNull = colDataIsNull_s(pSrcCol, rowIndex);
      char* p = colDataGetData(pSrcCol, rowIndex);
      saveColData(pRow, i, p, isNull);
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
    if ((pFillInfo->currentKey < ts && ascFill) || (pFillInfo->currentKey > ts && !ascFill)) {
      copyCurrentRowIntoBuf(pFillInfo, pFillInfo->index, pFillInfo->next.pRowVal);
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

      if (pFillInfo->type == TSDB_FILL_NEXT && (pFillInfo->index + 1) < pFillInfo->numOfRows) {
        int32_t nextRowIndex = pFillInfo->index + 1;
        copyCurrentRowIntoBuf(pFillInfo, nextRowIndex, pFillInfo->next.pRowVal);
      }

      // copy rows to dst buffer
      for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
        SFillColInfo* pCol = &pFillInfo->pFillCol[i];

        int32_t dstSlotId = GET_DEST_SLOT_ID(pCol);

        SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, dstSlotId);
        SColumnInfoData* pSrc = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, dstSlotId);

        char* src = colDataGetData(pSrc, pFillInfo->index);
        if (!colDataIsNull_s(pSrc, pFillInfo->index)) {
          colDataAppend(pDst, index, src, false);
          saveColData(pFillInfo->prev.pRowVal, i, src, false);
        } else {  // the value is null
          if (pDst->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
            colDataAppend(pDst, index, (const char*)&pFillInfo->currentKey, false);
          } else {  // i > 0 and data is null , do interpolation
            if (pFillInfo->type == TSDB_FILL_PREV) {
              SArray*     p = FILL_IS_ASC_FILL(pFillInfo) ? pFillInfo->prev.pRowVal : pFillInfo->next.pRowVal;
              SGroupKeys* pKey = taosArrayGet(p, i);
              doSetVal(pDst, index, pKey);
            } else if (pFillInfo->type == TSDB_FILL_LINEAR) {
              bool isNull = colDataIsNull_s(pSrc, pFillInfo->index);
              colDataAppend(pDst, index, src, isNull);
              saveColData(pFillInfo->prev.pRowVal, i, src, isNull);  // todo:
            } else if (pFillInfo->type == TSDB_FILL_NULL) {
              colDataAppendNULL(pDst, index);
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
      //      setTagsValue(pFillInfo, data, pFillInfo->numOfCurrent);
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

  assert(pFillInfo->numOfCurrent == resultCapacity);
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

  switch (fillType) {
    case FILL_MODE_NONE:
      pFillInfo->type = TSDB_FILL_NONE;
      break;
    case FILL_MODE_PREV:
      pFillInfo->type = TSDB_FILL_PREV;
      break;
    case FILL_MODE_NULL:
      pFillInfo->type = TSDB_FILL_NULL;
      break;
    case FILL_MODE_LINEAR:
      pFillInfo->type = TSDB_FILL_LINEAR;
      break;
    case FILL_MODE_NEXT:
      pFillInfo->type = TSDB_FILL_NEXT;
      break;
    case FILL_MODE_VALUE:
      pFillInfo->type = TSDB_FILL_SET_VALUE;
      break;
    default:
      terrno = TSDB_CODE_INVALID_PARA;
      return NULL;
  }

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

  taosMemoryFreeClear(pFillInfo->pTags);
  taosMemoryFreeClear(pFillInfo->pFillCol);
  taosMemoryFreeClear(pFillInfo);
  return NULL;
}

void taosFillSetDataOrderInfo(SFillInfo* pFillInfo, int32_t order) {
  if (pFillInfo == NULL || (order != TSDB_ORDER_ASC && order != TSDB_ORDER_DESC)) {
    return;
  }

  pFillInfo->order = order;
}

void taosFillSetStartInfo(SFillInfo* pFillInfo, int32_t numOfRows, TSKEY endKey) {
  if (pFillInfo->type == TSDB_FILL_NONE) {
    return;
  }

  pFillInfo->end = endKey;
  if (!FILL_IS_ASC_FILL(pFillInfo)) {
    pFillInfo->end = taosTimeTruncate(endKey, &pFillInfo->interval, pFillInfo->interval.precision);
  }

  pFillInfo->index = 0;
  pFillInfo->numOfRows = numOfRows;
}

void taosFillSetInputDataBlock(SFillInfo* pFillInfo, const SSDataBlock* pInput) {
  pFillInfo->pSrcBlock = (SSDataBlock*)pInput;
}

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
  SColumnInfoData* pCol = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, pFillInfo->srcTsSlotId);

  int64_t* tsList = (int64_t*)pCol->pData;
  int32_t  numOfRows = taosNumOfRemainRows(pFillInfo);

  TSKEY ekey1 = ekey;
  if (!FILL_IS_ASC_FILL(pFillInfo)) {
    pFillInfo->end = taosTimeTruncate(ekey, &pFillInfo->interval, pFillInfo->interval.precision);
  }

  int64_t numOfRes = -1;
  if (numOfRows > 0) {  // still fill gap within current data block, not generating data after the result set.
    TSKEY lastKey = tsList[pFillInfo->numOfRows - 1];
    numOfRes = taosTimeCountInterval(lastKey, pFillInfo->currentKey, pFillInfo->interval.sliding,
                                     pFillInfo->interval.slidingUnit, pFillInfo->interval.precision);
    numOfRes += 1;
    assert(numOfRes >= numOfRows);
  } else {  // reach the end of data
    if ((ekey1 < pFillInfo->currentKey && FILL_IS_ASC_FILL(pFillInfo)) ||
        (ekey1 > pFillInfo->currentKey && !FILL_IS_ASC_FILL(pFillInfo))) {
      return 0;
    }
    numOfRes = taosTimeCountInterval(ekey1, pFillInfo->currentKey, pFillInfo->interval.sliding,
                                     pFillInfo->interval.slidingUnit, pFillInfo->interval.precision);
    numOfRes += 1;
  }

  return (numOfRes > maxNumOfRows) ? maxNumOfRows : numOfRes;
}

int32_t taosGetLinearInterpolationVal(SPoint* point, int32_t outputType, SPoint* point1, SPoint* point2,
                                      int32_t inputType) {
  double v1 = -1, v2 = -1;
  GET_TYPED_DATA(v1, double, inputType, point1->val);
  GET_TYPED_DATA(v2, double, inputType, point2->val);

  double r = DO_INTERPOLATION(v1, v2, point1->key, point2->key, point->key);
  SET_TYPED_DATA(point->val, outputType, r);

  return TSDB_CODE_SUCCESS;
}

int64_t taosFillResultDataBlock(SFillInfo* pFillInfo, SSDataBlock* p, int32_t capacity) {
  int32_t remain = taosNumOfRemainRows(pFillInfo);

  int64_t numOfRes = getNumOfResultsAfterFillGap(pFillInfo, pFillInfo->end, capacity);
  assert(numOfRes <= capacity);

  // no data existed for fill operation now, append result according to the fill strategy
  if (remain == 0) {
    appendFilledResult(pFillInfo, p, numOfRes);
  } else {
    fillResultImpl(pFillInfo, p, (int32_t)numOfRes);
    assert(numOfRes == pFillInfo->numOfCurrent);
  }

  qDebug("fill:%p, generated fill result, src block:%d, index:%d, brange:%" PRId64 "-%" PRId64 ", currentKey:%" PRId64
         ", current : % d, total : % d, %s",
         pFillInfo, pFillInfo->numOfRows, pFillInfo->index, pFillInfo->start, pFillInfo->end, pFillInfo->currentKey,
         pFillInfo->numOfCurrent, pFillInfo->numOfTotal, pFillInfo->id);

  return numOfRes;
}

int64_t getFillInfoStart(struct SFillInfo* pFillInfo) { return pFillInfo->start; }

SFillColInfo* createFillColInfo(SExprInfo* pExpr, int32_t numOfFillExpr, SExprInfo* pNotFillExpr,
                                int32_t numOfNotFillExpr, const struct SNodeListNode* pValNode) {
  SFillColInfo* pFillCol = taosMemoryCalloc(numOfFillExpr + numOfNotFillExpr, sizeof(SFillColInfo));
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

  for (int32_t i = 0; i < numOfNotFillExpr; ++i) {
    SExprInfo* pExprInfo = &pNotFillExpr[i];
    pFillCol[i + numOfFillExpr].pExpr = pExprInfo;
    pFillCol[i + numOfFillExpr].notFillCol = true;
  }

  return pFillCol;
}

TSKEY getNextWindowTs(TSKEY ts, SInterval* pInterval) {
  STimeWindow win = {.skey = ts, .ekey = ts};
  getNextIntervalWindow(pInterval, &win, TSDB_ORDER_ASC);
  return win.skey;
}

TSKEY getPrevWindowTs(TSKEY ts, SInterval* pInterval) {
  STimeWindow win = {.skey = ts, .ekey = ts};
  getNextIntervalWindow(pInterval, &win, TSDB_ORDER_DESC);
  return win.skey;
}

void setRowCell(SColumnInfoData* pCol, int32_t rowId, const SResultCellData* pCell) {
  colDataAppend(pCol, rowId, pCell->pData, pCell->isNull);
}

SResultCellData* getResultCell(SResultRowData* pRaw, int32_t index) {
  if (!pRaw || !pRaw->pRowVal) {
    return NULL;
  }
  char*            pData = (char*)pRaw->pRowVal;
  SResultCellData* pCell = pRaw->pRowVal;
  for (int32_t i = 0; i < index; i++) {
    pData += (pCell->bytes + sizeof(SResultCellData));
    pCell = (SResultCellData*)pData;
  }
  return pCell;
}

void* destroyFillColumnInfo(SFillColInfo* pFillCol, int32_t start, int32_t end) {
  for (int32_t i = start; i < end; i++) {
    destroyExprInfo(pFillCol[i].pExpr, 1);
    taosMemoryFreeClear(pFillCol[i].pExpr);
    taosVariantDestroy(&pFillCol[i].fillVal);
  }
  taosMemoryFree(pFillCol);
  return NULL;
}

void* destroyStreamFillSupporter(SStreamFillSupporter* pFillSup) {
  pFillSup->pAllColInfo = destroyFillColumnInfo(pFillSup->pAllColInfo, pFillSup->numOfFillCols, pFillSup->numOfAllCols);
  tSimpleHashCleanup(pFillSup->pResMap);
  pFillSup->pResMap = NULL;
  taosMemoryFree(pFillSup);
  return NULL;
}

void* destroyStreamFillLinearInfo(SStreamFillLinearInfo* pFillLinear) {
  taosArrayDestroy(pFillLinear->pDeltaVal);
  taosArrayDestroy(pFillLinear->pNextDeltaVal);
  taosMemoryFree(pFillLinear);
  return NULL;
}
void* destroyStreamFillInfo(SStreamFillInfo* pFillInfo) {
  if (pFillInfo->type == TSDB_FILL_SET_VALUE || pFillInfo->type == TSDB_FILL_NULL) {
    taosMemoryFreeClear(pFillInfo->pResRow->pRowVal);
    taosMemoryFreeClear(pFillInfo->pResRow);
  }
  pFillInfo->pLinearInfo = destroyStreamFillLinearInfo(pFillInfo->pLinearInfo);
  taosMemoryFree(pFillInfo);
  return NULL;
}

void destroyStreamFillOperatorInfo(void* param) {
  SStreamFillOperatorInfo* pInfo = (SStreamFillOperatorInfo*)param;
  pInfo->pFillInfo = destroyStreamFillInfo(pInfo->pFillInfo);
  pInfo->pFillSup = destroyStreamFillSupporter(pInfo->pFillSup);
  pInfo->pRes = blockDataDestroy(pInfo->pRes);
  pInfo->pSrcBlock = blockDataDestroy(pInfo->pSrcBlock);
  pInfo->pColMatchColInfo = taosArrayDestroy(pInfo->pColMatchColInfo);
  taosMemoryFree(pInfo);
}

static void resetFillWindow(SResultRowData* pRowData) {
  pRowData->key = INT64_MIN;
  pRowData->pRowVal = NULL;
}

void resetPrevAndNextWindow(SStreamFillSupporter* pFillSup, SStreamState* pState) {
  resetFillWindow(&pFillSup->prev);
  resetFillWindow(&pFillSup->cur);
  resetFillWindow(&pFillSup->next);
  resetFillWindow(&pFillSup->nextNext);
}

void getCurWindowFromDiscBuf(SOperatorInfo* pOperator, TSKEY ts, uint64_t groupId, SStreamFillSupporter* pFillSup) {
  SStreamState* pState = pOperator->pTaskInfo->streamInfo.pState;
  resetPrevAndNextWindow(pFillSup, pState);

  SWinKey key = {.ts = ts, .groupId = groupId};
  void*   curVal = NULL;
  int32_t curVLen = 0;
  int32_t code = streamStateFillGet(pState, &key, (void**)&curVal, &curVLen);
  ASSERT(code == TSDB_CODE_SUCCESS);
  pFillSup->cur.key = key.ts;
  pFillSup->cur.pRowVal = curVal;
}

void getWindowFromDiscBuf(SOperatorInfo* pOperator, TSKEY ts, uint64_t groupId, SStreamFillSupporter* pFillSup) {
  SStreamState* pState = pOperator->pTaskInfo->streamInfo.pState;
  resetPrevAndNextWindow(pFillSup, pState);

  SWinKey key = {.ts = ts, .groupId = groupId};
  void*   curVal = NULL;
  int32_t curVLen = 0;
  int32_t code = streamStateFillGet(pState, &key, (void**)&curVal, &curVLen);
  ASSERT(code == TSDB_CODE_SUCCESS);
  pFillSup->cur.key = key.ts;
  pFillSup->cur.pRowVal = curVal;

  SStreamStateCur* pCur = streamStateFillSeekKeyPrev(pState, &key);
  SWinKey          preKey = {.groupId = groupId};
  void*            preVal = NULL;
  int32_t          preVLen = 0;
  if (pCur) {
    code = streamStateGetGroupKVByCur(pCur, &preKey, (const void**)&preVal, &preVLen);
  }

  if (pCur && code == TSDB_CODE_SUCCESS) {
    pFillSup->prev.key = preKey.ts;
    pFillSup->prev.pRowVal = preVal;

    code = streamStateCurNext(pState, pCur);
    ASSERT(code == TSDB_CODE_SUCCESS);

    code = streamStateCurNext(pState, pCur);
    if (code != TSDB_CODE_SUCCESS) {
      pCur = NULL;
    }
  } else {
    pCur = streamStateFillSeekKeyNext(pState, &key);
  }

  if (pCur) {
    SWinKey nextKey = {.groupId = groupId};
    void*   nextVal = NULL;
    int32_t nextVLen = 0;
    code = streamStateGetGroupKVByCur(pCur, &nextKey, (const void**)&nextVal, &nextVLen);
    if (code == TSDB_CODE_SUCCESS) {
      pFillSup->next.key = nextKey.ts;
      pFillSup->next.pRowVal = nextVal;
      if (pFillSup->type == TSDB_FILL_PREV || pFillSup->type == TSDB_FILL_NEXT) {
        code = streamStateCurNext(pState, pCur);
        if (code == TSDB_CODE_SUCCESS) {
          SWinKey nextNextKey = {.groupId = groupId};
          void*   nextNextVal = NULL;
          int32_t nextNextVLen = 0;
          code = streamStateGetGroupKVByCur(pCur, &nextNextKey, (const void**)&nextNextVal, &nextNextVLen);
          if (code == TSDB_CODE_SUCCESS) {
            pFillSup->nextNext.key = nextNextKey.ts;
            pFillSup->nextNext.pRowVal = nextNextVal;
          }
        }
      }
    }
  }
}

static bool hasPrevWindow(SStreamFillSupporter* pFillSup) { return pFillSup->prev.key != INT64_MIN; }
static bool hasNextWindow(SStreamFillSupporter* pFillSup) { return pFillSup->next.key != INT64_MIN; }
static bool hasNextNextWindow(SStreamFillSupporter* pFillSup) {
  return pFillSup->nextNext.key != INT64_MIN;
  return false;
}

static void transBlockToResultRow(const SSDataBlock* pBlock, int32_t rowId, TSKEY ts, SResultRowData* pRowVal) {
  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
    SResultCellData* pCell = getResultCell(pRowVal, i);
    if (!colDataIsNull_s(pColData, rowId)) {
      pCell->isNull = false;
      pCell->type = pColData->info.type;
      pCell->bytes = pColData->info.bytes;
      char* val = colDataGetData(pColData, rowId);
      if (IS_VAR_DATA_TYPE(pCell->type)) {
        memcpy(pCell->pData, val, varDataTLen(val));
      } else {
        memcpy(pCell->pData, val, pCell->bytes);
      }
    } else {
      pCell->isNull = true;
    }
  }
  pRowVal->key = ts;
}

static void calcDeltaData(SSDataBlock* pBlock, int32_t rowId, SResultRowData* pRowVal, SArray* pDelta,
                          SFillColInfo* pFillCol, int32_t numOfCol, int32_t winCount, int32_t order) {
  for (int32_t i = 0; i < numOfCol; i++) {
    if (!pFillCol[i].notFillCol) {
      int32_t          slotId = GET_DEST_SLOT_ID(pFillCol + i);
      SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
      char*            var = colDataGetData(pCol, rowId);
      double           start = 0;
      GET_TYPED_DATA(start, double, pCol->info.type, var);
      SResultCellData* pCell = getResultCell(pRowVal, slotId);
      double           end = 0;
      GET_TYPED_DATA(end, double, pCell->type, pCell->pData);
      double delta = 0;
      if (order == TSDB_ORDER_ASC) {
        delta = (end - start) / winCount;
      } else {
        delta = (start - end) / winCount;
      }
      taosArraySet(pDelta, slotId, &delta);
    }
  }
}

static void calcRowDeltaData(SResultRowData* pStartRow, SResultRowData* pEndRow, SArray* pDelta, SFillColInfo* pFillCol,
                             int32_t numOfCol, int32_t winCount) {
  for (int32_t i = 0; i < numOfCol; i++) {
    if (!pFillCol[i].notFillCol) {
      int32_t          slotId = GET_DEST_SLOT_ID(pFillCol + i);
      SResultCellData* pSCell = getResultCell(pStartRow, slotId);
      double           start = 0.0;
      GET_TYPED_DATA(start, double, pSCell->type, pSCell->pData);
      SResultCellData* pECell = getResultCell(pEndRow, slotId);
      double           end = 0.0;
      GET_TYPED_DATA(end, double, pECell->type, pECell->pData);
      double delta = (end - start) / winCount;
      taosArraySet(pDelta, slotId, &delta);
    }
  }
}

static void setFillInfoStart(TSKEY ts, SInterval* pInterval, SStreamFillInfo* pFillInfo) {
  ts = taosTimeAdd(ts, pInterval->sliding, pInterval->slidingUnit, pInterval->precision);
  pFillInfo->start = ts;
}

static void setFillInfoEnd(TSKEY ts, SInterval* pInterval, SStreamFillInfo* pFillInfo) {
  ts = taosTimeAdd(ts, pInterval->sliding * -1, pInterval->slidingUnit, pInterval->precision);
  pFillInfo->end = ts;
}

static void setFillKeyInfo(TSKEY start, TSKEY end, SInterval* pInterval, SStreamFillInfo* pFillInfo) {
  setFillInfoStart(start, pInterval, pFillInfo);
  pFillInfo->current = pFillInfo->start;
  setFillInfoEnd(end, pInterval, pFillInfo);
}

void setDeleteFillValueInfo(TSKEY start, TSKEY end, SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo) {
  if (!hasPrevWindow(pFillSup) || !hasNextWindow(pFillSup)) {
    pFillInfo->needFill = false;
    return;
  }

  pFillInfo->needFill = true;
  pFillInfo->start = start;
  pFillInfo->current = pFillInfo->start;
  pFillInfo->end = end;
  pFillInfo->pos = FILL_POS_INVALID;
  switch (pFillInfo->type) {
    case TSDB_FILL_NULL:
    case TSDB_FILL_SET_VALUE:
      break;
    case TSDB_FILL_PREV:
      pFillInfo->pResRow = &pFillSup->prev;
      break;
    case TSDB_FILL_NEXT:
      pFillInfo->pResRow = &pFillSup->next;
      break;
    case TSDB_FILL_LINEAR: {
      setFillKeyInfo(pFillSup->prev.key, pFillSup->next.key, &pFillSup->interval, pFillInfo);
      pFillInfo->pLinearInfo->hasNext = false;
      pFillInfo->pLinearInfo->nextEnd = INT64_MIN;
      int32_t numOfWins = taosTimeCountInterval(pFillSup->prev.key, pFillSup->next.key, pFillSup->interval.sliding,
                                                pFillSup->interval.slidingUnit, pFillSup->interval.precision);
      calcRowDeltaData(&pFillSup->prev, &pFillSup->next, pFillInfo->pLinearInfo->pDeltaVal, pFillSup->pAllColInfo,
                       pFillSup->numOfAllCols, numOfWins);
      pFillInfo->pResRow = &pFillSup->prev;
      pFillInfo->pLinearInfo->winIndex = 0;
    } break;
    default:
      ASSERT(0);
      break;
  }
}

void setFillValueInfo(SSDataBlock* pBlock, TSKEY ts, int32_t rowId, SStreamFillSupporter* pFillSup,
                      SStreamFillInfo* pFillInfo) {
  pFillInfo->preRowKey = pFillSup->cur.key;
  if (!hasPrevWindow(pFillSup) && !hasNextWindow(pFillSup)) {
    pFillInfo->needFill = false;
    pFillInfo->pos = FILL_POS_START;
    return;
  }
  TSKEY prevWKey = INT64_MIN;
  TSKEY nextWKey = INT64_MIN;
  if (hasPrevWindow(pFillSup)) {
    prevWKey = pFillSup->prev.key;
  }
  if (hasNextWindow(pFillSup)) {
    nextWKey = pFillSup->next.key;
  }

  pFillInfo->needFill = true;
  pFillInfo->pos = FILL_POS_INVALID;
  switch (pFillInfo->type) {
    case TSDB_FILL_NULL:
    case TSDB_FILL_SET_VALUE: {
      if (pFillSup->prev.key == pFillInfo->preRowKey) {
        resetFillWindow(&pFillSup->prev);
      }
      if (hasPrevWindow(pFillSup) && hasNextWindow(pFillSup)) {
        if (pFillSup->next.key == pFillInfo->nextRowKey) {
          pFillInfo->preRowKey = INT64_MIN;
          setFillKeyInfo(prevWKey, ts, &pFillSup->interval, pFillInfo);
          pFillInfo->pos = FILL_POS_END;
        } else {
          pFillInfo->needFill = false;
          pFillInfo->pos = FILL_POS_START;
        }
      } else if (hasPrevWindow(pFillSup)) {
        setFillKeyInfo(prevWKey, ts, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_END;
      } else {
        setFillKeyInfo(ts, nextWKey, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_START;
      }
    } break;
    case TSDB_FILL_PREV: {
      if (hasNextWindow(pFillSup) && ((pFillSup->next.key != pFillInfo->nextRowKey) ||
                                      (pFillSup->next.key == pFillInfo->nextRowKey && hasNextNextWindow(pFillSup)) ||
                                      (pFillSup->next.key == pFillInfo->nextRowKey && !hasPrevWindow(pFillSup)))) {
        setFillKeyInfo(ts, nextWKey, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_START;
        pFillSup->prev.key = pFillSup->cur.key;
        pFillSup->prev.pRowVal = pFillSup->cur.pRowVal;
      } else if (hasPrevWindow(pFillSup)) {
        setFillKeyInfo(prevWKey, ts, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_END;
        pFillInfo->preRowKey = INT64_MIN;
      }
      pFillInfo->pResRow = &pFillSup->prev;
    } break;
    case TSDB_FILL_NEXT: {
      if (hasPrevWindow(pFillSup)) {
        setFillKeyInfo(prevWKey, ts, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_END;
        pFillSup->next.key = pFillSup->cur.key;
        pFillSup->next.pRowVal = pFillSup->cur.pRowVal;
        pFillInfo->preRowKey = INT64_MIN;
      } else {
        ASSERT(hasNextWindow(pFillSup));
        setFillKeyInfo(ts, nextWKey, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_START;
      }
      pFillInfo->pResRow = &pFillSup->next;
    } break;
    case TSDB_FILL_LINEAR: {
      pFillInfo->pLinearInfo->winIndex = 0;
      if (hasPrevWindow(pFillSup) && hasNextWindow(pFillSup)) {
        setFillKeyInfo(prevWKey, ts, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_MID;
        pFillInfo->pLinearInfo->nextEnd = nextWKey;
        int32_t numOfWins = taosTimeCountInterval(prevWKey, ts, pFillSup->interval.sliding,
                                                  pFillSup->interval.slidingUnit, pFillSup->interval.precision);
        calcRowDeltaData(&pFillSup->prev, &pFillSup->cur, pFillInfo->pLinearInfo->pDeltaVal, pFillSup->pAllColInfo,
                         pFillSup->numOfAllCols, numOfWins);
        pFillInfo->pResRow = &pFillSup->prev;

        numOfWins = taosTimeCountInterval(ts, nextWKey, pFillSup->interval.sliding, pFillSup->interval.slidingUnit,
                                          pFillSup->interval.precision);
        calcRowDeltaData(&pFillSup->cur, &pFillSup->next, pFillInfo->pLinearInfo->pNextDeltaVal, pFillSup->pAllColInfo,
                         pFillSup->numOfAllCols, numOfWins);
        pFillInfo->pLinearInfo->hasNext = true;
      } else if (hasPrevWindow(pFillSup)) {
        setFillKeyInfo(prevWKey, ts, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_END;
        pFillInfo->pLinearInfo->nextEnd = INT64_MIN;
        int32_t numOfWins = taosTimeCountInterval(prevWKey, ts, pFillSup->interval.sliding,
                                                  pFillSup->interval.slidingUnit, pFillSup->interval.precision);
        calcRowDeltaData(&pFillSup->prev, &pFillSup->cur, pFillInfo->pLinearInfo->pDeltaVal, pFillSup->pAllColInfo,
                         pFillSup->numOfAllCols, numOfWins);
        pFillInfo->pResRow = &pFillSup->prev;
        pFillInfo->pLinearInfo->hasNext = false;
      } else {
        ASSERT(hasNextWindow(pFillSup));
        setFillKeyInfo(ts, nextWKey, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_START;
        pFillInfo->pLinearInfo->nextEnd = INT64_MIN;
        int32_t numOfWins = taosTimeCountInterval(ts, nextWKey, pFillSup->interval.sliding,
                                                  pFillSup->interval.slidingUnit, pFillSup->interval.precision);
        calcRowDeltaData(&pFillSup->cur, &pFillSup->next, pFillInfo->pLinearInfo->pDeltaVal, pFillSup->pAllColInfo,
                         pFillSup->numOfAllCols, numOfWins);
        pFillInfo->pResRow = &pFillSup->cur;
        pFillInfo->pLinearInfo->hasNext = false;
      }
    } break;
    default:
      ASSERT(0);
      break;
  }
  ASSERT(pFillInfo->pos != FILL_POS_INVALID);
}

static bool checkResult(SStreamFillSupporter* pFillSup, TSKEY ts, uint64_t groupId) {
  SWinKey key = {.groupId = groupId, .ts = ts};
  if (tSimpleHashGet(pFillSup->pResMap, &key, sizeof(SWinKey)) != NULL) {
    return false;
  }
  tSimpleHashPut(pFillSup->pResMap, &key, sizeof(SWinKey), NULL, 0);
  return true;
}

static void buildFillResult(SResultRowData* pResRow, SStreamFillSupporter* pFillSup, TSKEY ts, SSDataBlock* pBlock) {
  uint64_t groupId = pBlock->info.groupId;
  if (pFillSup->hasDelete && !checkResult(pFillSup, ts, groupId)) {
    return;
  }
  for (int32_t i = 0; i < pFillSup->numOfAllCols; ++i) {
    SFillColInfo*    pFillCol = pFillSup->pAllColInfo + i;
    int32_t          slotId = GET_DEST_SLOT_ID(pFillCol);
    SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, slotId);
    SFillInfo        tmpInfo = {
               .currentKey = ts,
               .order = TSDB_ORDER_ASC,
               .interval = pFillSup->interval,
    };
    bool filled = fillIfWindowPseudoColumn(&tmpInfo, pFillCol, pColData, pBlock->info.rows);
    if (!filled) {
      SResultCellData* pCell = getResultCell(pResRow, slotId);
      setRowCell(pColData, pBlock->info.rows, pCell);
    }
  }
  pBlock->info.rows++;
}

static bool hasRemainCalc(SStreamFillInfo* pFillInfo) {
  if (pFillInfo->current != INT64_MIN && pFillInfo->current <= pFillInfo->end) {
    return true;
  }
  return false;
}

static void doStreamFillNormal(SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo, SSDataBlock* pBlock) {
  while (hasRemainCalc(pFillInfo) && pBlock->info.rows < pBlock->info.capacity) {
    buildFillResult(pFillInfo->pResRow, pFillSup, pFillInfo->current, pBlock);
    pFillInfo->current = taosTimeAdd(pFillInfo->current, pFillSup->interval.sliding, pFillSup->interval.slidingUnit,
                                     pFillSup->interval.precision);
  }
}

static void doStreamFillLinear(SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo, SSDataBlock* pBlock) {
  while (hasRemainCalc(pFillInfo) && pBlock->info.rows < pBlock->info.capacity) {
    uint64_t groupId = pBlock->info.groupId;
    SWinKey  key = {.groupId = groupId, .ts = pFillInfo->current};
    if (pFillSup->hasDelete && !checkResult(pFillSup, pFillInfo->current, groupId)) {
      pFillInfo->current = taosTimeAdd(pFillInfo->current, pFillSup->interval.sliding, pFillSup->interval.slidingUnit,
                                       pFillSup->interval.precision);
      pFillInfo->pLinearInfo->winIndex++;
      continue;
    }
    pFillInfo->pLinearInfo->winIndex++;
    for (int32_t i = 0; i < pFillSup->numOfAllCols; ++i) {
      SFillColInfo* pFillCol = pFillSup->pAllColInfo + i;
      SFillInfo     tmp = {
              .currentKey = pFillInfo->current,
              .order = TSDB_ORDER_ASC,
              .interval = pFillSup->interval,
      };

      int32_t          slotId = GET_DEST_SLOT_ID(pFillCol);
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, slotId);
      int16_t          type = pColData->info.type;
      SResultCellData* pCell = getResultCell(pFillInfo->pResRow, slotId);
      int32_t          index = pBlock->info.rows;
      if (pFillCol->notFillCol) {
        bool filled = fillIfWindowPseudoColumn(&tmp, pFillCol, pColData, index);
        if (!filled) {
          setRowCell(pColData, index, pCell);
        }
      } else {
        if (IS_VAR_DATA_TYPE(type) || type == TSDB_DATA_TYPE_BOOL || pCell->isNull) {
          colDataAppendNULL(pColData, index);
          continue;
        }
        double* pDelta = taosArrayGet(pFillInfo->pLinearInfo->pDeltaVal, slotId);
        double  vCell = 0;
        GET_TYPED_DATA(vCell, double, pCell->type, pCell->pData);
        vCell += (*pDelta) * pFillInfo->pLinearInfo->winIndex;
        int64_t result = 0;
        SET_TYPED_DATA(&result, pCell->type, vCell);
        colDataAppend(pColData, index, (const char*)&result, false);
      }
    }
    pFillInfo->current = taosTimeAdd(pFillInfo->current, pFillSup->interval.sliding, pFillSup->interval.slidingUnit,
                                     pFillSup->interval.precision);
    pBlock->info.rows++;
  }
}

static void keepResultInDiscBuf(SOperatorInfo* pOperator, uint64_t groupId, SResultRowData* pRow, int32_t len) {
  SWinKey key = {.groupId = groupId, .ts = pRow->key};
  int32_t code = streamStateFillPut(pOperator->pTaskInfo->streamInfo.pState, &key, pRow->pRowVal, len);
  ASSERT(code == TSDB_CODE_SUCCESS);
}

static void doStreamFillRange(SStreamFillInfo* pFillInfo, SStreamFillSupporter* pFillSup, SSDataBlock* pRes) {
  if (pFillInfo->needFill == false) {
    buildFillResult(&pFillSup->cur, pFillSup, pFillSup->cur.key, pRes);
    return;
  }

  if (pFillInfo->pos == FILL_POS_START) {
    buildFillResult(&pFillSup->cur, pFillSup, pFillSup->cur.key, pRes);
  }
  if (pFillInfo->type != TSDB_FILL_LINEAR) {
    doStreamFillNormal(pFillSup, pFillInfo, pRes);
  } else {
    doStreamFillLinear(pFillSup, pFillInfo, pRes);

    if (pFillInfo->pos == FILL_POS_MID) {
      buildFillResult(&pFillSup->cur, pFillSup, pFillSup->cur.key, pRes);
    }

    if (pFillInfo->current > pFillInfo->end && pFillInfo->pLinearInfo->hasNext) {
      pFillInfo->pLinearInfo->hasNext = false;
      pFillInfo->pLinearInfo->winIndex = 0;
      taosArrayClear(pFillInfo->pLinearInfo->pDeltaVal);
      taosArrayAddAll(pFillInfo->pLinearInfo->pDeltaVal, pFillInfo->pLinearInfo->pNextDeltaVal);
      pFillInfo->pResRow = &pFillSup->cur;
      setFillKeyInfo(pFillSup->cur.key, pFillInfo->pLinearInfo->nextEnd, &pFillSup->interval, pFillInfo);
      doStreamFillLinear(pFillSup, pFillInfo, pRes);
    }
  }
  if (pFillInfo->pos == FILL_POS_END) {
    buildFillResult(&pFillSup->cur, pFillSup, pFillSup->cur.key, pRes);
  }
}

void keepBlockRowInDiscBuf(SOperatorInfo* pOperator, SStreamFillInfo* pFillInfo, SSDataBlock* pBlock, TSKEY* tsCol,
                           int32_t rowId, uint64_t groupId, int32_t rowSize) {
  TSKEY ts = tsCol[rowId];
  pFillInfo->nextRowKey = ts;
  SResultRowData tmpNextRow = {.key = ts};
  tmpNextRow.pRowVal = taosMemoryCalloc(1, rowSize);
  transBlockToResultRow(pBlock, rowId, ts, &tmpNextRow);
  keepResultInDiscBuf(pOperator, groupId, &tmpNextRow, rowSize);
  taosMemoryFreeClear(tmpNextRow.pRowVal);
}

static void doFillResults(SOperatorInfo* pOperator, SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo,
                          SSDataBlock* pBlock, TSKEY* tsCol, int32_t rowId, SSDataBlock* pRes) {
  uint64_t groupId = pBlock->info.groupId;
  getWindowFromDiscBuf(pOperator, tsCol[rowId], groupId, pFillSup);
  if (pFillSup->prev.key == pFillInfo->preRowKey) {
    resetFillWindow(&pFillSup->prev);
  }
  setFillValueInfo(pBlock, tsCol[rowId], rowId, pFillSup, pFillInfo);
  doStreamFillRange(pFillInfo, pFillSup, pRes);
}

static void doStreamFillImpl(SOperatorInfo* pOperator) {
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SStreamFillSupporter*    pFillSup = pInfo->pFillSup;
  SStreamFillInfo*         pFillInfo = pInfo->pFillInfo;
  SSDataBlock*             pBlock = pInfo->pSrcBlock;
  uint64_t                 groupId = pBlock->info.groupId;
  SSDataBlock*             pRes = pInfo->pRes;
  pRes->info.groupId = groupId;
  if (hasRemainCalc(pFillInfo)) {
    doStreamFillRange(pFillInfo, pFillSup, pRes);
  }

  SColumnInfoData* pTsCol = taosArrayGet(pInfo->pSrcBlock->pDataBlock, pInfo->primaryTsCol);
  TSKEY*           tsCol = (TSKEY*)pTsCol->pData;

  if (pInfo->srcRowIndex == 0) {
    keepBlockRowInDiscBuf(pOperator, pFillInfo, pBlock, tsCol, pInfo->srcRowIndex, groupId, pFillSup->rowSize);
    SSDataBlock* preBlock = pInfo->pPrevSrcBlock;
    if (preBlock->info.rows > 0) {
      int              preRowId = preBlock->info.rows - 1;
      SColumnInfoData* pPreTsCol = taosArrayGet(preBlock->pDataBlock, pInfo->primaryTsCol);
      doFillResults(pOperator, pFillSup, pFillInfo, preBlock, (TSKEY*)pPreTsCol->pData, preRowId, pRes);
    }
    pInfo->srcRowIndex++;
  }

  while (pInfo->srcRowIndex < pBlock->info.rows) {
    TSKEY ts = tsCol[pInfo->srcRowIndex];
    keepBlockRowInDiscBuf(pOperator, pFillInfo, pBlock, tsCol, pInfo->srcRowIndex, groupId, pFillSup->rowSize);
    doFillResults(pOperator, pFillSup, pFillInfo, pBlock, tsCol, pInfo->srcRowIndex - 1, pRes);
    if (pInfo->pRes->info.rows == pInfo->pRes->info.capacity) {
      blockDataUpdateTsWindow(pRes, pInfo->primaryTsCol);
      return;
    }
    pInfo->srcRowIndex++;
  }
  blockDataUpdateTsWindow(pRes, pInfo->primaryTsCol);
  blockDataCleanup(pInfo->pPrevSrcBlock);
  copyDataBlock(pInfo->pPrevSrcBlock, pInfo->pSrcBlock);
  blockDataCleanup(pInfo->pSrcBlock);
}

static void buildDeleteRange(TSKEY start, TSKEY end, uint64_t groupId, SSDataBlock* delRes) {
  SSDataBlock*     pBlock = delRes;
  SColumnInfoData* pStartCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pEndCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pUidCol = taosArrayGet(pBlock->pDataBlock, UID_COLUMN_INDEX);
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  SColumnInfoData* pCalStartCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  SColumnInfoData* pCalEndCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  colDataAppend(pStartCol, pBlock->info.rows, (const char*)&start, false);
  colDataAppend(pEndCol, pBlock->info.rows, (const char*)&end, false);
  colDataAppendNULL(pUidCol, pBlock->info.rows);
  colDataAppend(pGroupCol, pBlock->info.rows, (const char*)&groupId, false);
  colDataAppendNULL(pCalStartCol, pBlock->info.rows);
  colDataAppendNULL(pCalEndCol, pBlock->info.rows);
  pBlock->info.rows++;
}

static void buildDeleteResult(SStreamFillSupporter* pFillSup, TSKEY startTs, TSKEY endTs, uint64_t groupId,
                              SSDataBlock* delRes) {
  if (hasPrevWindow(pFillSup)) {
    TSKEY start = getNextWindowTs(pFillSup->prev.key, &pFillSup->interval);
    buildDeleteRange(start, endTs, groupId, delRes);
  } else if (hasNextWindow(pFillSup)) {
    TSKEY end = getPrevWindowTs(pFillSup->next.key, &pFillSup->interval);
    buildDeleteRange(startTs, end, groupId, delRes);
  } else {
    buildDeleteRange(startTs, endTs, groupId, delRes);
  }
}

static void doDeleteFillResultImpl(SOperatorInfo* pOperator, TSKEY startTs, TSKEY endTs, uint64_t groupId) {
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  getWindowFromDiscBuf(pOperator, startTs, groupId, pInfo->pFillSup);
  setDeleteFillValueInfo(startTs, endTs, pInfo->pFillSup, pInfo->pFillInfo);
  SWinKey key = {.ts = startTs, .groupId = groupId};
  if (!pInfo->pFillInfo->needFill) {
    streamStateFillDel(pOperator->pTaskInfo->streamInfo.pState, &key);
    buildDeleteResult(pInfo->pFillSup, startTs, endTs, groupId, pInfo->pDelRes);
  } else {
    STimeRange tw = {
        .skey = startTs,
        .ekey = endTs,
        .groupId = groupId,
    };
    taosArrayPush(pInfo->pFillInfo->delRanges, &tw);
    while (key.ts <= endTs) {
      key.ts = taosTimeAdd(key.ts, pInfo->pFillSup->interval.sliding, pInfo->pFillSup->interval.slidingUnit,
                           pInfo->pFillSup->interval.precision);
      tSimpleHashPut(pInfo->pFillSup->pResMap, &key, sizeof(SWinKey), NULL, 0);
    }
  }
}

static void doDeleteFillFinalize(SOperatorInfo* pOperator) {
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SStreamFillInfo*         pFillInfo = pInfo->pFillInfo;
  int32_t                  size = taosArrayGetSize(pFillInfo->delRanges);
  tSimpleHashClear(pInfo->pFillSup->pResMap);
  for (; pFillInfo->delIndex < size; pFillInfo->delIndex++) {
    STimeRange* range = taosArrayGet(pFillInfo->delRanges, pFillInfo->delIndex);
    if (pInfo->pRes->info.groupId != 0 && pInfo->pRes->info.groupId != range->groupId) {
      return;
    }
    getWindowFromDiscBuf(pOperator, range->skey, range->groupId, pInfo->pFillSup);
    setDeleteFillValueInfo(range->skey, range->ekey, pInfo->pFillSup, pInfo->pFillInfo);
    if (pInfo->pFillInfo->needFill) {
      doStreamFillRange(pInfo->pFillInfo, pInfo->pFillSup, pInfo->pRes);
      pInfo->pRes->info.groupId = range->groupId;
    }
    SWinKey key = {.ts = range->skey, .groupId = range->groupId};
    streamStateFillDel(pOperator->pTaskInfo->streamInfo.pState, &key);
  }
}

static void doDeleteFillResult(SOperatorInfo* pOperator) {
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SStreamFillSupporter*    pFillSup = pInfo->pFillSup;
  SStreamFillInfo*         pFillInfo = pInfo->pFillInfo;
  SSDataBlock*             pBlock = pInfo->pSrcDelBlock;
  SSDataBlock*             pRes = pInfo->pRes;
  SSDataBlock*             pDelRes = pInfo->pDelRes;

  SColumnInfoData* pStartCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           tsStarts = (TSKEY*)pStartCol->pData;
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        groupIds = (uint64_t*)pGroupCol->pData;
  while (pInfo->srcDelRowIndex < pBlock->info.rows) {
    TSKEY            ts = tsStarts[pInfo->srcDelRowIndex];
    TSKEY            endTs = ts;
    uint64_t         groupId = groupIds[pInfo->srcDelRowIndex];
    SWinKey          key = {.ts = ts, .groupId = groupId};
    SStreamStateCur* pCur = streamStateGetAndCheckCur(pOperator->pTaskInfo->streamInfo.pState, &key);
    if (!pCur) {
      pInfo->srcDelRowIndex++;
      continue;
    }

    SWinKey nextKey = {.groupId = groupId, .ts = ts};
    while (pInfo->srcDelRowIndex < pBlock->info.rows) {
      void*    nextVal = NULL;
      int32_t  nextLen = 0;
      TSKEY    delTs = tsStarts[pInfo->srcDelRowIndex];
      uint64_t delGroupId = groupIds[pInfo->srcDelRowIndex];
      int32_t  code = TSDB_CODE_SUCCESS;
      if (groupId != delGroupId) {
        break;
      }
      if (delTs > nextKey.ts) {
        break;
      }
      endTs = delTs;
      SWinKey delKey = {.groupId = delGroupId, .ts = delTs};
      if (delTs == nextKey.ts) {
        code = streamStateCurNext(pOperator->pTaskInfo->streamInfo.pState, pCur);
        if (code == TSDB_CODE_SUCCESS) {
          code = streamStateGetGroupKVByCur(pCur, &nextKey, (const void**)&nextVal, &nextLen);
        }
        if (delTs != ts) {
          streamStateFillDel(pOperator->pTaskInfo->streamInfo.pState, &delKey);
        }
        if (code != TSDB_CODE_SUCCESS) {
          break;
        }
      }
      pInfo->srcDelRowIndex++;
    }
    doDeleteFillResultImpl(pOperator, ts, endTs, groupId);
  }
  pFillInfo->current = pFillInfo->end + 1;
}

static void resetStreamFillInfo(SStreamFillOperatorInfo* pInfo) {
  blockDataCleanup(pInfo->pPrevSrcBlock);
  tSimpleHashClear(pInfo->pFillSup->pResMap);
  pInfo->pFillSup->hasDelete = false;
  taosArrayClear(pInfo->pFillInfo->delRanges);
  pInfo->pFillInfo->delIndex = 0;
}

static void doApplyStreamScalarCalculation(SOperatorInfo* pOperator, SSDataBlock* pSrcBlock, SSDataBlock* pDstBlock) {
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SExprSupp*               pSup = &pOperator->exprSupp;

  blockDataCleanup(pDstBlock);
  blockDataEnsureCapacity(pDstBlock, pSrcBlock->info.rows);
  setInputDataBlock(pOperator, pSup->pCtx, pSrcBlock, TSDB_ORDER_ASC, MAIN_SCAN, false);
  projectApplyFunctions(pSup->pExprInfo, pDstBlock, pSrcBlock, pSup->pCtx, pSup->numOfExprs, NULL);
  pDstBlock->info.groupId = pSrcBlock->info.groupId;

  SColumnInfoData* pDst = taosArrayGet(pDstBlock->pDataBlock, pInfo->primaryTsCol);
  SColumnInfoData* pSrc = taosArrayGet(pSrcBlock->pDataBlock, pInfo->primarySrcSlotId);
  colDataAssign(pDst, pSrc, pDstBlock->info.rows, &pDstBlock->info);

  int32_t numOfNotFill = pInfo->pFillSup->numOfAllCols - pInfo->pFillSup->numOfFillCols;
  for (int32_t i = 0; i < numOfNotFill; ++i) {
    SFillColInfo* pCol = &pInfo->pFillSup->pAllColInfo[i + pInfo->pFillSup->numOfFillCols];
    ASSERT(pCol->notFillCol);

    SExprInfo* pExpr = pCol->pExpr;
    int32_t    srcSlotId = pExpr->base.pParam[0].pCol->slotId;
    int32_t    dstSlotId = pExpr->base.resSchema.slotId;

    SColumnInfoData* pDst1 = taosArrayGet(pDstBlock->pDataBlock, dstSlotId);
    SColumnInfoData* pSrc1 = taosArrayGet(pSrcBlock->pDataBlock, srcSlotId);
    colDataAssign(pDst1, pSrc1, pDstBlock->info.rows, &pDstBlock->info);
  }
  blockDataUpdateTsWindow(pDstBlock, pInfo->primaryTsCol);
}

static SSDataBlock* doStreamFill(SOperatorInfo* pOperator) {
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }
  blockDataCleanup(pInfo->pRes);
  if (pOperator->status == OP_RES_TO_RETURN) {
    if (hasRemainCalc(pInfo->pFillInfo)) {
      doStreamFillRange(pInfo->pFillInfo, pInfo->pFillSup, pInfo->pRes);
      if (pInfo->pRes->info.rows > 0) {
        return pInfo->pRes;
      }
    }
    doDeleteFillFinalize(pOperator);
    if (pInfo->pRes->info.rows > 0) {
      printDataBlock(pInfo->pRes, "stream fill");
      return pInfo->pRes;
    }
    doSetOperatorCompleted(pOperator);
    resetStreamFillInfo(pInfo);
    return NULL;
  }

  SSDataBlock*   fillResult = NULL;
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  while (1) {
    if (pInfo->srcRowIndex >= pInfo->pSrcBlock->info.rows) {
      // If there are delete datablocks, we receive  them first.
      SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
      if (pBlock == NULL) {
        pOperator->status = OP_RES_TO_RETURN;
        SSDataBlock* preBlock = pInfo->pPrevSrcBlock;
        if (preBlock->info.rows > 0) {
          int              preRowId = preBlock->info.rows - 1;
          SColumnInfoData* pPreTsCol = taosArrayGet(preBlock->pDataBlock, pInfo->primaryTsCol);
          doFillResults(pOperator, pInfo->pFillSup, pInfo->pFillInfo, preBlock, (TSKEY*)pPreTsCol->pData, preRowId,
                        pInfo->pRes);
        }
        pInfo->pFillInfo->preRowKey = INT64_MIN;
        if (pInfo->pRes->info.rows > 0) {
          printDataBlock(pInfo->pRes, "stream fill");
          return pInfo->pRes;
        }
        break;
      }
      printDataBlock(pBlock, "stream fill recv");

      switch (pBlock->info.type) {
        case STREAM_RETRIEVE:
          return pBlock;
        case STREAM_DELETE_RESULT: {
          pInfo->pSrcDelBlock = pBlock;
          pInfo->srcDelRowIndex = 0;
          blockDataCleanup(pInfo->pDelRes);
          pInfo->pFillSup->hasDelete = true;
          doDeleteFillResult(pOperator);
          if (pInfo->pDelRes->info.rows > 0) {
            printDataBlock(pInfo->pDelRes, "stream fill delete");
            return pInfo->pDelRes;
          }
          continue;
        } break;
        case STREAM_NORMAL:
        case STREAM_INVALID: {
          doApplyStreamScalarCalculation(pOperator, pBlock, pInfo->pSrcBlock);
          pInfo->srcRowIndex = 0;
        } break;
        default:
          ASSERT(0);
          break;
      }
    }

    doStreamFillImpl(pOperator);
    doFilter(pInfo->pCondition, pInfo->pRes, pInfo->pColMatchColInfo, NULL);
    pOperator->resultInfo.totalRows += pInfo->pRes->info.rows;
    if (pInfo->pRes->info.rows > 0) {
      break;
    }
  }
  if (pOperator->status == OP_RES_TO_RETURN) {
    doDeleteFillFinalize(pOperator);
  }

  if (pInfo->pRes->info.rows == 0) {
    doSetOperatorCompleted(pOperator);
    resetStreamFillInfo(pInfo);
    return NULL;
  }

  pOperator->resultInfo.totalRows += pInfo->pRes->info.rows;
  printDataBlock(pInfo->pRes, "stream fill");
  return pInfo->pRes;
}

static int32_t initResultBuf(SStreamFillSupporter* pFillSup) {
  pFillSup->rowSize = sizeof(SResultCellData) * pFillSup->numOfAllCols;
  for (int i = 0; i < pFillSup->numOfAllCols; i++) {
    SFillColInfo* pCol = &pFillSup->pAllColInfo[i];
    SResSchema*   pSchema = &pCol->pExpr->base.resSchema;
    pFillSup->rowSize += pSchema->bytes;
  }
  pFillSup->next.key = INT64_MIN;
  pFillSup->nextNext.key = INT64_MIN;
  pFillSup->prev.key = INT64_MIN;
  pFillSup->next.pRowVal = NULL;
  pFillSup->nextNext.pRowVal = NULL;
  pFillSup->prev.pRowVal = NULL;
  return TSDB_CODE_SUCCESS;
}

static SStreamFillSupporter* initStreamFillSup(SStreamFillPhysiNode* pPhyFillNode, SInterval* pInterval,
                                               SExprInfo* pFillExprInfo, int32_t numOfFillCols) {
  SStreamFillSupporter* pFillSup = taosMemoryCalloc(1, sizeof(SStreamFillSupporter));
  if (!pFillSup) {
    return NULL;
  }
  pFillSup->numOfFillCols = numOfFillCols;
  int32_t    numOfNotFillCols = 0;
  SExprInfo* pNotFillExprInfo = createExprInfo(pPhyFillNode->pNotFillExprs, NULL, &numOfNotFillCols);
  pFillSup->pAllColInfo = createFillColInfo(pFillExprInfo, pFillSup->numOfFillCols, pNotFillExprInfo, numOfNotFillCols,
                                            (const SNodeListNode*)(pPhyFillNode->pValues));
  pFillSup->type = convertFillType(pPhyFillNode->mode);
  pFillSup->numOfAllCols = pFillSup->numOfFillCols + numOfNotFillCols;
  pFillSup->interval = *pInterval;

  int32_t code = initResultBuf(pFillSup);
  if (code != TSDB_CODE_SUCCESS) {
    destroyStreamFillSupporter(pFillSup);
    return NULL;
  }
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pFillSup->pResMap = tSimpleHashInit(16, hashFn);
  pFillSup->hasDelete = false;
  return pFillSup;
}

SStreamFillInfo* initStreamFillInfo(SStreamFillSupporter* pFillSup, SSDataBlock* pRes) {
  SStreamFillInfo* pFillInfo = taosMemoryCalloc(1, sizeof(SStreamFillInfo));
  pFillInfo->start = INT64_MIN;
  pFillInfo->current = INT64_MIN;
  pFillInfo->end = INT64_MIN;
  pFillInfo->preRowKey = INT64_MIN;
  pFillInfo->needFill = false;
  pFillInfo->pLinearInfo = taosMemoryCalloc(1, sizeof(SStreamFillLinearInfo));
  pFillInfo->pLinearInfo->hasNext = false;
  pFillInfo->pLinearInfo->nextEnd = INT64_MIN;
  pFillInfo->pLinearInfo->pDeltaVal = NULL;
  pFillInfo->pLinearInfo->pNextDeltaVal = NULL;
  if (pFillSup->type == TSDB_FILL_LINEAR) {
    pFillInfo->pLinearInfo->pDeltaVal = taosArrayInit(pFillSup->numOfAllCols, sizeof(double));
    pFillInfo->pLinearInfo->pNextDeltaVal = taosArrayInit(pFillSup->numOfAllCols, sizeof(double));
    for (int32_t i = 0; i < pFillSup->numOfAllCols; i++) {
      double value = 0.0;
      taosArrayPush(pFillInfo->pLinearInfo->pDeltaVal, &value);
      taosArrayPush(pFillInfo->pLinearInfo->pNextDeltaVal, &value);
    }
  }
  pFillInfo->pLinearInfo->winIndex = 0;

  pFillInfo->pResRow = NULL;
  if (pFillSup->type == TSDB_FILL_SET_VALUE || pFillSup->type == TSDB_FILL_NULL) {
    pFillInfo->pResRow = taosMemoryCalloc(1, sizeof(SResultRowData));
    pFillInfo->pResRow->key = INT64_MIN;
    pFillInfo->pResRow->pRowVal = taosMemoryCalloc(1, pFillSup->rowSize);
    for (int32_t i = 0; i < pFillSup->numOfAllCols; ++i) {
      SColumnInfoData* pColData = taosArrayGet(pRes->pDataBlock, i);
      SResultCellData* pCell = getResultCell(pFillInfo->pResRow, i);
      pCell->bytes = pColData->info.bytes;
      pCell->type = pColData->info.type;
    }
  }

  pFillInfo->type = pFillSup->type;
  pFillInfo->delRanges = taosArrayInit(16, sizeof(STimeRange));
  pFillInfo->delIndex = 0;
  return pFillInfo;
}

SOperatorInfo* createStreamFillOperatorInfo(SOperatorInfo* downstream, SStreamFillPhysiNode* pPhyFillNode,
                                            SExecTaskInfo* pTaskInfo) {
  SStreamFillOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamFillOperatorInfo));
  SOperatorInfo*           pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SInterval* pInterval = QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL == downstream->operatorType
                             ? &((SStreamFinalIntervalOperatorInfo*)downstream->info)->interval
                             : &((SStreamIntervalOperatorInfo*)downstream->info)->interval;
  int32_t    numOfFillCols = 0;
  SExprInfo* pFillExprInfo = createExprInfo(pPhyFillNode->pFillExprs, NULL, &numOfFillCols);
  pInfo->pFillSup = initStreamFillSup(pPhyFillNode, pInterval, pFillExprInfo, numOfFillCols);
  if (!pInfo->pFillSup) {
    goto _error;
  }

  SResultInfo* pResultInfo = &pOperator->resultInfo;
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  pInfo->pRes = createResDataBlock(pPhyFillNode->node.pOutputDataBlockDesc);
  pInfo->pSrcBlock = createResDataBlock(pPhyFillNode->node.pOutputDataBlockDesc);
  pInfo->pPrevSrcBlock = createResDataBlock(pPhyFillNode->node.pOutputDataBlockDesc);
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  blockDataEnsureCapacity(pInfo->pSrcBlock, pOperator->resultInfo.capacity);
  blockDataEnsureCapacity(pInfo->pPrevSrcBlock, pOperator->resultInfo.capacity);

  pInfo->pFillInfo = initStreamFillInfo(pInfo->pFillSup, pInfo->pRes);
  if (!pInfo->pFillInfo) {
    goto _error;
  }

  if (pInfo->pFillInfo->type == TSDB_FILL_SET_VALUE) {
    for (int32_t i = 0; i < pInfo->pFillSup->numOfAllCols; ++i) {
      SFillColInfo*    pFillCol = pInfo->pFillSup->pAllColInfo + i;
      int32_t          slotId = GET_DEST_SLOT_ID(pFillCol);
      SResultCellData* pCell = getResultCell(pInfo->pFillInfo->pResRow, slotId);
      SVariant*        pVar = &(pFillCol->fillVal);
      if (pCell->type == TSDB_DATA_TYPE_FLOAT) {
        float v = 0;
        GET_TYPED_DATA(v, float, pVar->nType, &pVar->i);
        SET_TYPED_DATA(pCell->pData, pCell->type, v);
      } else if (pCell->type == TSDB_DATA_TYPE_DOUBLE) {
        double v = 0;
        GET_TYPED_DATA(v, double, pVar->nType, &pVar->i);
        SET_TYPED_DATA(pCell->pData, pCell->type, v);
      } else if (IS_SIGNED_NUMERIC_TYPE(pCell->type)) {
        int64_t v = 0;
        GET_TYPED_DATA(v, int64_t, pVar->nType, &pVar->i);
        SET_TYPED_DATA(pCell->pData, pCell->type, v);
      } else {
        pCell->isNull = true;
      }
    }
  } else if (pInfo->pFillInfo->type == TSDB_FILL_NULL) {
    for (int32_t i = 0; i < pInfo->pFillSup->numOfAllCols; ++i) {
      SFillColInfo*    pFillCol = pInfo->pFillSup->pAllColInfo + i;
      int32_t          slotId = GET_DEST_SLOT_ID(pFillCol);
      SResultCellData* pCell = getResultCell(pInfo->pFillInfo->pResRow, slotId);
      pCell->isNull = true;
    }
  }

  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  blockDataEnsureCapacity(pInfo->pDelRes, pOperator->resultInfo.capacity);

  pInfo->primaryTsCol = ((STargetNode*)pPhyFillNode->pWStartTs)->slotId;
  pInfo->primarySrcSlotId = ((SColumnNode*)((STargetNode*)pPhyFillNode->pWStartTs)->pExpr)->slotId;

  int32_t numOfOutputCols = 0;
  SArray* pColMatchColInfo = extractColMatchInfo(pPhyFillNode->pFillExprs, pPhyFillNode->node.pOutputDataBlockDesc,
                                                 &numOfOutputCols, COL_MATCH_FROM_SLOT_ID);
  pInfo->pCondition = pPhyFillNode->node.pConditions;
  pInfo->pColMatchColInfo = pColMatchColInfo;
  initExprSupp(&pOperator->exprSupp, pFillExprInfo, numOfFillCols);
  pInfo->srcRowIndex = 0;

  pOperator->name = "FillOperator";
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_STREAM_FILL;
  pOperator->info = pInfo;
  pOperator->pTaskInfo = pTaskInfo;
  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doStreamFill, NULL, NULL, destroyStreamFillOperatorInfo,
                                         NULL, NULL, NULL);

  int32_t code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  return pOperator;

_error:
  destroyStreamFillOperatorInfo(pInfo);
  taosMemoryFreeClear(pOperator);
  return NULL;
}
