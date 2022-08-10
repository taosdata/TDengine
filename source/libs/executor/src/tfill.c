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

static void setTagsValue(SFillInfo* pFillInfo, void** data, int32_t genRows) {
  for (int32_t j = 0; j < pFillInfo->numOfCols; ++j) {
    SFillColInfo* pCol = &pFillInfo->pFillCol[j];
    if (TSDB_COL_IS_NORMAL_COL(pCol->flag) || TSDB_COL_IS_UD_COL(pCol->flag)) {
      continue;
    }

    SResSchema* pSchema = &pCol->pExpr->base.resSchema;
    char*       val1 = elePtrAt(data[j], pSchema->bytes, genRows);

    assert(pCol->tagIndex >= 0 && pCol->tagIndex < pFillInfo->numOfTags);
    SFillTagColInfo* pTag = &pFillInfo->pTags[pCol->tagIndex];
    assignVal(val1, pTag->tagVal, pSchema->bytes, pSchema->type);
  }
}

static void setNullRow(SSDataBlock* pBlock, int64_t ts, int32_t rowIndex) {
  // the first are always the timestamp column, so start from the second column.
  for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); ++i) {
    SColumnInfoData* p = taosArrayGet(pBlock->pDataBlock, i);
    if (p->info.type == TSDB_DATA_TYPE_TIMESTAMP) {  // handle timestamp
      colDataAppend(p, rowIndex, (const char*)&ts, false);
    } else {
      colDataAppendNULL(p, rowIndex);
    }
  }
}

#define GET_DEST_SLOT_ID(_p) ((_p)->pExpr->base.resSchema.slotId)
#define GET_SRC_SLOT_ID(_p)  ((_p)->pExpr->base.pParam[0].pCol->slotId)

static void doSetVal(SColumnInfoData* pDstColInfoData, int32_t rowIndex, const SGroupKeys* pKey);

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

static void doFillOneRow(SFillInfo* pFillInfo, SSDataBlock* pBlock, SSDataBlock* pSrcBlock, int64_t ts,
                         bool outOfBound) {
  SPoint  point1, point2, point;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pFillInfo->order);

  // set the primary timestamp column value
  int32_t index = pBlock->info.rows;

  // set the other values
  if (pFillInfo->type == TSDB_FILL_PREV) {
    SArray* p = FILL_IS_ASC_FILL(pFillInfo) ? pFillInfo->prev : pFillInfo->next;

    for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
      SFillColInfo* pCol = &pFillInfo->pFillCol[i];
      if (TSDB_COL_IS_TAG(pCol->flag)) {
        continue;
      }

      SColumnInfoData* pDstColInfoData = taosArrayGet(pBlock->pDataBlock, GET_DEST_SLOT_ID(pCol));

      if (pDstColInfoData->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
        colDataAppend(pDstColInfoData, index, (const char*)&pFillInfo->currentKey, false);
      } else {
        SGroupKeys* pKey = taosArrayGet(p, i);
        doSetVal(pDstColInfoData, index, pKey);
      }
    }
  } else if (pFillInfo->type == TSDB_FILL_NEXT) {
    SArray* p = FILL_IS_ASC_FILL(pFillInfo) ? pFillInfo->next : pFillInfo->prev;
    // todo  refactor: start from 0 not 1
    for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
      SFillColInfo* pCol = &pFillInfo->pFillCol[i];
      if (TSDB_COL_IS_TAG(pCol->flag)) {
        continue;
      }

      SColumnInfoData* pDstColInfoData = taosArrayGet(pBlock->pDataBlock, GET_DEST_SLOT_ID(pCol));

      if (pDstColInfoData->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
        colDataAppend(pDstColInfoData, index, (const char*)&pFillInfo->currentKey, false);
      } else {
        SGroupKeys* pKey = taosArrayGet(p, i);
        doSetVal(pDstColInfoData, index, pKey);
      }
    }
  } else if (pFillInfo->type == TSDB_FILL_LINEAR) {
    // TODO : linear interpolation supports NULL value
    if (outOfBound) {
      setNullRow(pBlock, pFillInfo->currentKey, index);
    } else {
      for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
        SFillColInfo* pCol = &pFillInfo->pFillCol[i];
        if (TSDB_COL_IS_TAG(pCol->flag)) {
          continue;
        }

        int32_t          dstSlotId = GET_DEST_SLOT_ID(pCol);
        SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, dstSlotId);

        int16_t type = pDstCol->info.type;
        if (type == TSDB_DATA_TYPE_TIMESTAMP) {
          colDataAppend(pDstCol, index, (const char*)&pFillInfo->currentKey, false);
          continue;
        }

        SGroupKeys* pKey = taosArrayGet(pFillInfo->prev, i);
        if (IS_VAR_DATA_TYPE(type) || type == TSDB_DATA_TYPE_BOOL || pKey->isNull) {
          colDataAppendNULL(pDstCol, index);
          continue;
        }

        SGroupKeys* pKey1 = taosArrayGet(pFillInfo->prev, pFillInfo->tsSlotId);

        int64_t prevTs = *(int64_t*)pKey1->pData;
        int32_t srcSlotId = GET_SRC_SLOT_ID(pCol);

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
  } else if (pFillInfo->type == TSDB_FILL_NULL) {  // fill with NULL
    setNullRow(pBlock, pFillInfo->currentKey, index);
  } else {  // fill with user specified value for each column
    for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
      SFillColInfo* pCol = &pFillInfo->pFillCol[i];
      if (TSDB_COL_IS_TAG(pCol->flag)) {
        continue;
      }

      SVariant*        pVar = &pFillInfo->pFillCol[i].fillVal;
      SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, i);
      doSetUserSpecifiedValue(pDst, pVar, index, pFillInfo->currentKey);
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
  if (taosArrayGetSize(pFillInfo->next) > 0) {
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

    taosArrayPush(pFillInfo->next, &key);

    key.pData = taosMemoryMalloc(pSchema->bytes);
    taosArrayPush(pFillInfo->prev, &key);
  }
}

static void saveColData(SArray* rowBuf, int32_t columnIndex, const char* src, bool isNull);

static void copyCurrentRowIntoBuf(SFillInfo* pFillInfo, int32_t rowIndex, SArray* pRow) {
  for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    int32_t srcSlotId = GET_SRC_SLOT_ID(&pFillInfo->pFillCol[i]);

    SColumnInfoData* pSrcCol = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, srcSlotId);

    bool  isNull = colDataIsNull_s(pSrcCol, rowIndex);
    char* p = colDataGetData(pSrcCol, rowIndex);
    saveColData(pRow, i, p, isNull);
  }
}

static int32_t fillResultImpl(SFillInfo* pFillInfo, SSDataBlock* pBlock, int32_t outputRows) {
  pFillInfo->numOfCurrent = 0;

  SColumnInfoData* pTsCol = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, pFillInfo->tsSlotId);

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pFillInfo->order);
  bool    ascFill = FILL_IS_ASC_FILL(pFillInfo);

#if 0
  ASSERT(ascFill && (pFillInfo->currentKey >= pFillInfo->start) || (!ascFill && (pFillInfo->currentKey <= pFillInfo->start)));
#endif

  while (pFillInfo->numOfCurrent < outputRows) {
    int64_t ts = ((int64_t*)pTsCol->pData)[pFillInfo->index];

    // set the next value for interpolation
    if ((pFillInfo->currentKey < ts && ascFill) || (pFillInfo->currentKey > ts && !ascFill)) {
      copyCurrentRowIntoBuf(pFillInfo, pFillInfo->index, pFillInfo->next);
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
        copyCurrentRowIntoBuf(pFillInfo, nextRowIndex, pFillInfo->next);
      }

      // assign rows to dst buffer
      for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
        SFillColInfo* pCol = &pFillInfo->pFillCol[i];
        if (TSDB_COL_IS_TAG(pCol->flag) /* || IS_VAR_DATA_TYPE(pCol->schema.type)*/) {
          continue;
        }

        int32_t srcSlotId = GET_SRC_SLOT_ID(pCol);
        int32_t dstSlotId = GET_DEST_SLOT_ID(pCol);

        SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, dstSlotId);
        SColumnInfoData* pSrc = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, srcSlotId);

        char* src = colDataGetData(pSrc, pFillInfo->index);
        if (/*i == 0 || (*/ !colDataIsNull_s(pSrc, pFillInfo->index)) {
          bool isNull = colDataIsNull_s(pSrc, pFillInfo->index);
          colDataAppend(pDst, index, src, isNull);
          saveColData(pFillInfo->prev, i, src, isNull);
        } else {
          if (pDst->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
            colDataAppend(pDst, index, (const char*)&pFillInfo->currentKey, false);
          } else {  // i > 0 and data is null , do interpolation
            if (pFillInfo->type == TSDB_FILL_PREV) {
              SArray*     p = FILL_IS_ASC_FILL(pFillInfo) ? pFillInfo->prev : pFillInfo->next;
              SGroupKeys* pKey = taosArrayGet(p, i);
              doSetVal(pDst, index, pKey);
            } else if (pFillInfo->type == TSDB_FILL_LINEAR) {
              bool isNull = colDataIsNull_s(pSrc, pFillInfo->index);
              colDataAppend(pDst, index, src, isNull);
              saveColData(pFillInfo->prev, i, src, isNull);  // todo:
            } else if (pFillInfo->type == TSDB_FILL_NULL) {
              colDataAppendNULL(pDst, index);
            } else if (pFillInfo->type == TSDB_FILL_NEXT) {
              SArray*     p = FILL_IS_ASC_FILL(pFillInfo) ? pFillInfo->next : pFillInfo->prev;
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
      /* the raw data block is exhausted, next value does not exists */
      //      if (pFillInfo->index >= pFillInfo->numOfRows) {
      //        taosMemoryFreeClear(*next);
      //      }
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
    memcpy(pKey->pData, src, pKey->bytes);
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

// there are no duplicated tags in the SFillTagColInfo list
static int32_t setTagColumnInfo(SFillInfo* pFillInfo, int32_t numOfCols, int32_t capacity) {
  int32_t rowsize = 0;
  int32_t numOfTags = 0;

  int32_t k = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    SFillColInfo* pColInfo = &pFillInfo->pFillCol[i];
    SResSchema*   pSchema = &pColInfo->pExpr->base.resSchema;

    if (TSDB_COL_IS_TAG(pColInfo->flag) || pSchema->type == TSDB_DATA_TYPE_BINARY) {
      numOfTags += 1;

      bool    exists = false;
      int32_t index = -1;
      for (int32_t j = 0; j < k; ++j) {
        if (pFillInfo->pTags[j].col.colId == pSchema->slotId) {
          exists = true;
          index = j;
          break;
        }
      }

      if (!exists) {
        SSchema* pSchema1 = &pFillInfo->pTags[k].col;
        pSchema1->colId = pSchema->slotId;
        pSchema1->type = pSchema->type;
        pSchema1->bytes = pSchema->bytes;

        pFillInfo->pTags[k].tagVal = taosMemoryCalloc(1, pSchema->bytes);
        pColInfo->tagIndex = k;

        k += 1;
      } else {
        pColInfo->tagIndex = index;
      }
    }

    rowsize += pSchema->bytes;
  }

  pFillInfo->numOfTags = numOfTags;

  assert(k <= pFillInfo->numOfTags);
  return rowsize;
}

static int32_t taosNumOfRemainRows(SFillInfo* pFillInfo) {
  if (pFillInfo->numOfRows == 0 || (pFillInfo->numOfRows > 0 && pFillInfo->index >= pFillInfo->numOfRows)) {
    return 0;
  }

  return pFillInfo->numOfRows - pFillInfo->index;
}

struct SFillInfo* taosCreateFillInfo(TSKEY skey, int32_t numOfTags, int32_t capacity, int32_t numOfCols,
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
  pFillInfo->tsSlotId = primaryTsSlotId;
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
  pFillInfo->numOfTags = numOfTags;
  pFillInfo->numOfCols = numOfCols;
  pFillInfo->alloc = capacity;
  pFillInfo->id = id;
  pFillInfo->interval = *pInterval;

  //  if (numOfTags > 0) {
  pFillInfo->pTags = taosMemoryCalloc(numOfCols, sizeof(SFillTagColInfo));
  for (int32_t i = 0; i < numOfCols; ++i) {
    pFillInfo->pTags[i].col.colId = -2;  // TODO
  }
  //  }

  pFillInfo->next = taosArrayInit(numOfCols, sizeof(SGroupKeys));
  pFillInfo->prev = taosArrayInit(numOfCols, sizeof(SGroupKeys));

  initBeforeAfterDataBuf(pFillInfo);

  pFillInfo->rowSize = setTagColumnInfo(pFillInfo, pFillInfo->numOfCols, pFillInfo->alloc);
  assert(pFillInfo->rowSize > 0);
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
  for (int32_t i = 0; i < taosArrayGetSize(pFillInfo->prev); ++i) {
    SGroupKeys* pKey = taosArrayGet(pFillInfo->prev, i);
    taosMemoryFree(pKey->pData);
  }
  taosArrayDestroy(pFillInfo->prev);
  for (int32_t i = 0; i < taosArrayGetSize(pFillInfo->next); ++i) {
    SGroupKeys* pKey = taosArrayGet(pFillInfo->next, i);
    taosMemoryFree(pKey->pData);
  }
  taosArrayDestroy(pFillInfo->next);

  for (int32_t i = 0; i < pFillInfo->numOfTags; ++i) {
    taosMemoryFreeClear(pFillInfo->pTags[i].tagVal);
  }

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
  SColumnInfoData* pCol = taosArrayGet(pFillInfo->pSrcBlock->pDataBlock, 0);

  int64_t* tsList = (int64_t*)pCol->pData;
  int32_t  numOfRows = taosNumOfRemainRows(pFillInfo);

  TSKEY ekey1 = ekey;
  if (!FILL_IS_ASC_FILL(pFillInfo)) {
    pFillInfo->end = taosTimeTruncate(ekey, &pFillInfo->interval, pFillInfo->interval.precision);
  }

  int64_t numOfRes = -1;
  if (numOfRows > 0) {  // still fill gap within current data block, not generating data after the result set.
    TSKEY lastKey = (TSDB_ORDER_ASC == pFillInfo->order ? tsList[pFillInfo->numOfRows - 1] : tsList[0]);
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

SFillColInfo* createFillColInfo(SExprInfo* pExpr, int32_t numOfOutput, const struct SNodeListNode* pValNode) {
  SFillColInfo* pFillCol = taosMemoryCalloc(numOfOutput, sizeof(SFillColInfo));
  if (pFillCol == NULL) {
    return NULL;
  }

  size_t len = (pValNode != NULL) ? LIST_LENGTH(pValNode->pNodeList) : 0;
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SExprInfo* pExprInfo = &pExpr[i];
    pFillCol[i].pExpr = pExprInfo;
    pFillCol[i].tagIndex = -2;

    // todo refactor
    if (len > 0) {
      // if the user specified value is less than the column, alway use the last one as the fill value
      int32_t index = (i >= len) ? (len - 1) : i;

      SValueNode* pv = (SValueNode*)nodesListGetNode(pValNode->pNodeList, index);
      nodesValueNodeToVariant(pv, &pFillCol[i].fillVal);
    }

    if (pExprInfo->base.numOfParams > 0) {
      pFillCol[i].flag = pExprInfo->base.pParam[0].pCol->flag;  // always be the normal column for table query
    }
  }

  return pFillCol;
}
