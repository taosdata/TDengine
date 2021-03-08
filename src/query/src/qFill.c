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

#include "qAggMain.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "ttype.h"

#include "qFill.h"
#include "qExtbuffer.h"
#include "queryLog.h"

#define FILL_IS_ASC_FILL(_f) ((_f)->order == TSDB_ORDER_ASC)
#define DO_INTERPOLATION(_v1, _v2, _k1, _k2, _k) ((_v1) + ((_v2) - (_v1)) * (((double)(_k)) - ((double)(_k1))) / (((double)(_k2)) - ((double)(_k1))))

static void setTagsValue(SFillInfo* pFillInfo, tFilePage** data, int32_t genRows) {
  for(int32_t j = 0; j < pFillInfo->numOfCols; ++j) {
    SFillColInfo* pCol = &pFillInfo->pFillCol[j];
    if (TSDB_COL_IS_NORMAL_COL(pCol->flag)) {
      continue;
    }

    char* val1 = elePtrAt(data[j]->data, pCol->col.bytes, genRows);

    assert(pCol->tagIndex >= 0 && pCol->tagIndex < pFillInfo->numOfTags);
    SFillTagColInfo* pTag = &pFillInfo->pTags[pCol->tagIndex];

    assert (pTag->col.colId == pCol->col.colId);
    assignVal(val1, pTag->tagVal, pCol->col.bytes, pCol->col.type);
  }
}

static void setNullValueForRow(SFillInfo* pFillInfo, tFilePage** data, int32_t numOfCol, int32_t rowIndex) {
  // the first are always the timestamp column, so start from the second column.
  for (int32_t i = 1; i < numOfCol; ++i) {
    SFillColInfo* pCol = &pFillInfo->pFillCol[i];

    char* output = elePtrAt(data[i]->data, pCol->col.bytes, rowIndex);
    setNull(output, pCol->col.type, pCol->col.bytes);
  }
}

static void doFillOneRowResult(SFillInfo* pFillInfo, tFilePage** data, char** srcData, int64_t ts, bool outOfBound) {
  char* prev = pFillInfo->prevValues;
  char* next = pFillInfo->nextValues;

  SPoint point1, point2, point;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pFillInfo->order);

  // set the primary timestamp column value
  int32_t index = pFillInfo->numOfCurrent;
  char* val = elePtrAt(data[0]->data, TSDB_KEYSIZE, index);
  *(TSKEY*) val = pFillInfo->currentKey;

  // set the other values
  if (pFillInfo->type == TSDB_FILL_PREV) {
    char* p = FILL_IS_ASC_FILL(pFillInfo) ? prev : next;

    if (p != NULL) {
      for (int32_t i = 1; i < pFillInfo->numOfCols; ++i) {
        SFillColInfo* pCol = &pFillInfo->pFillCol[i];
        if (TSDB_COL_IS_TAG(pCol->flag)) {
          continue;
        }

        char* output = elePtrAt(data[i]->data, pCol->col.bytes, index);
        assignVal(output, p + pCol->col.offset, pCol->col.bytes, pCol->col.type);
      }
    } else {  // no prev value yet, set the value for NULL
      setNullValueForRow(pFillInfo, data, pFillInfo->numOfCols, index);
    }
  } else if (pFillInfo->type == TSDB_FILL_NEXT) {
    char* p = FILL_IS_ASC_FILL(pFillInfo)? next : prev;

    if (p != NULL) {
      for (int32_t i = 1; i < pFillInfo->numOfCols; ++i) {
        SFillColInfo* pCol = &pFillInfo->pFillCol[i];
        if (TSDB_COL_IS_TAG(pCol->flag)) {
          continue;
        }

        char* output = elePtrAt(data[i]->data, pCol->col.bytes, index);
        assignVal(output, p + pCol->col.offset, pCol->col.bytes, pCol->col.type);
      }
    } else { // no prev value yet, set the value for NULL
      setNullValueForRow(pFillInfo, data, pFillInfo->numOfCols, index);
    }
  } else if (pFillInfo->type == TSDB_FILL_LINEAR) {
    // TODO : linear interpolation supports NULL value
    if (prev != NULL && !outOfBound) {
      for (int32_t i = 1; i < pFillInfo->numOfCols; ++i) {
        SFillColInfo* pCol = &pFillInfo->pFillCol[i];
        if (TSDB_COL_IS_TAG(pCol->flag)) {
          continue;
        }

        int16_t type  = pCol->col.type;
        int16_t bytes = pCol->col.bytes;

        char *val1 = elePtrAt(data[i]->data, pCol->col.bytes, index);
        if (type == TSDB_DATA_TYPE_BINARY|| type == TSDB_DATA_TYPE_NCHAR || type == TSDB_DATA_TYPE_BOOL) {
          setNull(val1, pCol->col.type, bytes);
          continue;
        }

        point1 = (SPoint){.key = *(TSKEY*)(prev), .val = prev + pCol->col.offset};
        point2 = (SPoint){.key = ts, .val = srcData[i] + pFillInfo->index * bytes};
        point  = (SPoint){.key = pFillInfo->currentKey, .val = val1};
        taosGetLinearInterpolationVal(&point, type, &point1, &point2, type);
      }
    } else {
      setNullValueForRow(pFillInfo, data, pFillInfo->numOfCols, index);
    }
  } else { /* fill the default value */
    for (int32_t i = 1; i < pFillInfo->numOfCols; ++i) {
      SFillColInfo* pCol = &pFillInfo->pFillCol[i];
      if (TSDB_COL_IS_TAG(pCol->flag)) {
        continue;
      }

      char* val1 = elePtrAt(data[i]->data, pCol->col.bytes, index);
      assignVal(val1, (char*)&pCol->fillVal.i, pCol->col.bytes, pCol->col.type);
    }
  }

  setTagsValue(pFillInfo, data, index);
  pFillInfo->currentKey = taosTimeAdd(pFillInfo->currentKey, pFillInfo->interval.sliding * step, pFillInfo->interval.slidingUnit, pFillInfo->precision);
  pFillInfo->numOfCurrent++;
}

static void initBeforeAfterDataBuf(SFillInfo* pFillInfo, char** next) {
  if (*next != NULL) {
    return;
  }

  *next = calloc(1, pFillInfo->rowSize);
  for (int i = 1; i < pFillInfo->numOfCols; i++) {
    SFillColInfo* pCol = &pFillInfo->pFillCol[i];
    setNull(*next + pCol->col.offset, pCol->col.type, pCol->col.bytes);
  }
}

static void copyCurrentRowIntoBuf(SFillInfo* pFillInfo, char** srcData, char* buf) {
  int32_t rowIndex = pFillInfo->index;
  for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    SFillColInfo* pCol = &pFillInfo->pFillCol[i];
    memcpy(buf + pCol->col.offset, srcData[i] + rowIndex * pCol->col.bytes, pCol->col.bytes);
  }
}

static int32_t fillResultImpl(SFillInfo* pFillInfo, tFilePage** data, int32_t outputRows) {
  pFillInfo->numOfCurrent = 0;

  char** srcData = pFillInfo->pData;
  char** prev = &pFillInfo->prevValues;
  char** next = &pFillInfo->nextValues;

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pFillInfo->order);

  if (FILL_IS_ASC_FILL(pFillInfo)) {
    assert(pFillInfo->currentKey >= pFillInfo->start);
  } else {
    assert(pFillInfo->currentKey <= pFillInfo->start);
  }

  while (pFillInfo->numOfCurrent < outputRows) {
    int64_t ts = ((int64_t*)pFillInfo->pData[0])[pFillInfo->index];

    // set the next value for interpolation
    if ((pFillInfo->currentKey < ts && FILL_IS_ASC_FILL(pFillInfo)) ||
        (pFillInfo->currentKey > ts && !FILL_IS_ASC_FILL(pFillInfo))) {
      initBeforeAfterDataBuf(pFillInfo, next);
      copyCurrentRowIntoBuf(pFillInfo, srcData, *next);
    }

    if (((pFillInfo->currentKey < ts && FILL_IS_ASC_FILL(pFillInfo)) || (pFillInfo->currentKey > ts && !FILL_IS_ASC_FILL(pFillInfo))) &&
        pFillInfo->numOfCurrent < outputRows) {

      // fill the gap between two actual input rows
      while (((pFillInfo->currentKey < ts && FILL_IS_ASC_FILL(pFillInfo)) ||
              (pFillInfo->currentKey > ts && !FILL_IS_ASC_FILL(pFillInfo))) &&
             pFillInfo->numOfCurrent < outputRows) {
        doFillOneRowResult(pFillInfo, data, srcData, ts, false);
      }

      // output buffer is full, abort
      if (pFillInfo->numOfCurrent == outputRows) {
        pFillInfo->numOfTotal += pFillInfo->numOfCurrent;
        return outputRows;
      }
    } else {
      assert(pFillInfo->currentKey == ts);
      initBeforeAfterDataBuf(pFillInfo, prev);

      // assign rows to dst buffer
      for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
        SFillColInfo* pCol = &pFillInfo->pFillCol[i];
        if (TSDB_COL_IS_TAG(pCol->flag)) {
          continue;
        }

        char* output = elePtrAt(data[i]->data, pCol->col.bytes, pFillInfo->numOfCurrent);
        char* src = elePtrAt(srcData[i], pCol->col.bytes, pFillInfo->index);

        if (i == 0 || (pCol->functionId != TSDB_FUNC_COUNT && !isNull(src, pCol->col.type)) ||
            (pCol->functionId == TSDB_FUNC_COUNT && GET_INT64_VAL(src) != 0)) {
          assignVal(output, src, pCol->col.bytes, pCol->col.type);
          memcpy(*prev + pCol->col.offset, src, pCol->col.bytes);
        } else {  // i > 0 and data is null , do interpolation
          if (pFillInfo->type == TSDB_FILL_PREV) {
            assignVal(output, *prev + pCol->col.offset, pCol->col.bytes, pCol->col.type);
          } else if (pFillInfo->type == TSDB_FILL_LINEAR) {
            assignVal(output, src, pCol->col.bytes, pCol->col.type);
            memcpy(*prev + pCol->col.offset, src, pCol->col.bytes);
          } else {
            assignVal(output, (char*)&pCol->fillVal.i, pCol->col.bytes, pCol->col.type);
          }
        }
      }

      // set the tag value for final result
      setTagsValue(pFillInfo, data, pFillInfo->numOfCurrent);

      pFillInfo->currentKey = taosTimeAdd(pFillInfo->currentKey, pFillInfo->interval.sliding * step,
                                          pFillInfo->interval.slidingUnit, pFillInfo->precision);
      pFillInfo->index += 1;
      pFillInfo->numOfCurrent += 1;
    }

    if (pFillInfo->index >= pFillInfo->numOfRows || pFillInfo->numOfCurrent >= outputRows) {
      /* the raw data block is exhausted, next value does not exists */
      if (pFillInfo->index >= pFillInfo->numOfRows) {
        tfree(*next);
      }

      pFillInfo->numOfTotal += pFillInfo->numOfCurrent;
      return pFillInfo->numOfCurrent;
    }
  }

  return pFillInfo->numOfCurrent;
}

static int64_t appendFilledResult(SFillInfo* pFillInfo, tFilePage** output, int64_t resultCapacity) {
  /*
   * These data are generated according to fill strategy, since the current timestamp is out of the time window of
   * real result set. Note that we need to keep the direct previous result rows, to generated the filled data.
   */
  pFillInfo->numOfCurrent = 0;
  while (pFillInfo->numOfCurrent < resultCapacity) {
    doFillOneRowResult(pFillInfo, output, pFillInfo->pData, pFillInfo->start, true);
  }

  pFillInfo->numOfTotal += pFillInfo->numOfCurrent;

  assert(pFillInfo->numOfCurrent == resultCapacity);
  return resultCapacity;
}

// there are no duplicated tags in the SFillTagColInfo list
static int32_t setTagColumnInfo(SFillInfo* pFillInfo, int32_t numOfCols, int32_t capacity) {
  int32_t rowsize = 0;

  int32_t k = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    SFillColInfo* pColInfo = &pFillInfo->pFillCol[i];
    pFillInfo->pData[i] = calloc(1, pColInfo->col.bytes * capacity);

    if (TSDB_COL_IS_TAG(pColInfo->flag)) {
      bool exists = false;
      int32_t index = -1;
      for (int32_t j = 0; j < k; ++j) {
        if (pFillInfo->pTags[j].col.colId == pColInfo->col.colId) {
          exists = true;
          index = j;
          break;
        }
      }

      if (!exists) {
        SSchema* pSchema = &pFillInfo->pTags[k].col;
        pSchema->colId = pColInfo->col.colId;
        pSchema->type  = pColInfo->col.type;
        pSchema->bytes = pColInfo->col.bytes;

        pFillInfo->pTags[k].tagVal = calloc(1, pColInfo->col.bytes);
        pColInfo->tagIndex = k;

        k += 1;
      } else {
        pColInfo->tagIndex = index;
      }
    }

    rowsize += pColInfo->col.bytes;
  }

  assert(k <= pFillInfo->numOfTags);
  return rowsize;
}

static int32_t taosNumOfRemainRows(SFillInfo* pFillInfo) {
  if (pFillInfo->numOfRows == 0 || (pFillInfo->numOfRows > 0 && pFillInfo->index >= pFillInfo->numOfRows)) {
    return 0;
  }

  return pFillInfo->numOfRows - pFillInfo->index;
}

SFillInfo* taosCreateFillInfo(int32_t order, TSKEY skey, int32_t numOfTags, int32_t capacity, int32_t numOfCols,
                            int64_t slidingTime, int8_t slidingUnit, int8_t precision, int32_t fillType,
                            SFillColInfo* pCol, void* handle) {
  if (fillType == TSDB_FILL_NONE) {
    return NULL;
  }

  SFillInfo* pFillInfo = calloc(1, sizeof(SFillInfo));
  taosResetFillInfo(pFillInfo, skey);

  pFillInfo->order     = order;
  pFillInfo->type      = fillType;
  pFillInfo->pFillCol  = pCol;
  pFillInfo->numOfTags = numOfTags;
  pFillInfo->numOfCols = numOfCols;
  pFillInfo->precision = precision;
  pFillInfo->alloc     = capacity;
  pFillInfo->handle    = handle;

  pFillInfo->interval.interval     = slidingTime;
  pFillInfo->interval.intervalUnit = slidingUnit;
  pFillInfo->interval.sliding      = slidingTime;
  pFillInfo->interval.slidingUnit  = slidingUnit;

  pFillInfo->pData = malloc(POINTER_BYTES * numOfCols);
  if (numOfTags > 0) {
    pFillInfo->pTags = calloc(pFillInfo->numOfTags, sizeof(SFillTagColInfo));
    for (int32_t i = 0; i < numOfTags; ++i) {
      pFillInfo->pTags[i].col.colId = -2;  // TODO
    }
  }

  pFillInfo->rowSize = setTagColumnInfo(pFillInfo, pFillInfo->numOfCols, pFillInfo->alloc);
  assert(pFillInfo->rowSize > 0);

  return pFillInfo;
}

void taosResetFillInfo(SFillInfo* pFillInfo, TSKEY startTimestamp) {
  pFillInfo->start        = startTimestamp;
  pFillInfo->currentKey   = startTimestamp;
  pFillInfo->index        = -1;
  pFillInfo->numOfRows    = 0;
  pFillInfo->numOfCurrent = 0;
  pFillInfo->numOfTotal   = 0;
}

void* taosDestroyFillInfo(SFillInfo* pFillInfo) {
  if (pFillInfo == NULL) {
    return NULL;
  }

  tfree(pFillInfo->prevValues);
  tfree(pFillInfo->nextValues);
  tfree(pFillInfo->pTags);
  
  for(int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    tfree(pFillInfo->pData[i]);
  }
  
  tfree(pFillInfo->pData);
  tfree(pFillInfo->pFillCol);
  
  tfree(pFillInfo);
  return NULL;
}

void taosFillSetStartInfo(SFillInfo* pFillInfo, int32_t numOfRows, TSKEY endKey) {
  if (pFillInfo->type == TSDB_FILL_NONE) {
    return;
  }

  pFillInfo->end = endKey;
  if (!FILL_IS_ASC_FILL(pFillInfo)) {
    pFillInfo->end = taosTimeTruncate(endKey, &pFillInfo->interval, pFillInfo->precision);
  }

  pFillInfo->index     = 0;
  pFillInfo->numOfRows = numOfRows;
  
  // ensure the space
  if (pFillInfo->alloc < numOfRows) {
    for(int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
      char* tmp = realloc(pFillInfo->pData[i], numOfRows*pFillInfo->pFillCol[i].col.bytes);
      assert(tmp != NULL); // todo handle error
      
      memset(tmp, 0, numOfRows*pFillInfo->pFillCol[i].col.bytes);
      pFillInfo->pData[i] = tmp;
    }
  }
}

// copy the data into source data buffer
void taosFillSetDataBlockFromFilePage(SFillInfo* pFillInfo, const tFilePage** pInput) {
  for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    memcpy(pFillInfo->pData[i], pInput[i]->data, pFillInfo->numOfRows * pFillInfo->pFillCol[i].col.bytes);
  }
}

void taosFillCopyInputDataFromOneFilePage(SFillInfo* pFillInfo, const tFilePage* pInput) {
  assert(pFillInfo->numOfRows == pInput->num);

  for(int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    SFillColInfo* pCol = &pFillInfo->pFillCol[i];

    const char* data = pInput->data + pCol->col.offset * pInput->num;
    memcpy(pFillInfo->pData[i], data, (size_t)(pInput->num * pCol->col.bytes));

    if (TSDB_COL_IS_TAG(pCol->flag)) {  // copy the tag value to tag value buffer
      SFillTagColInfo* pTag = &pFillInfo->pTags[pCol->tagIndex];
      assert (pTag->col.colId == pCol->col.colId);
      memcpy(pTag->tagVal, data, pCol->col.bytes);
    }
  }
}

bool taosFillHasMoreResults(SFillInfo* pFillInfo) {
  return taosNumOfRemainRows(pFillInfo) > 0;
}

int64_t getNumOfResultsAfterFillGap(SFillInfo* pFillInfo, TSKEY ekey, int32_t maxNumOfRows) {
  int64_t* tsList = (int64_t*) pFillInfo->pData[0];

  int32_t numOfRows = taosNumOfRemainRows(pFillInfo);

  TSKEY ekey1 = ekey;
  if (!FILL_IS_ASC_FILL(pFillInfo)) {
    pFillInfo->end = taosTimeTruncate(ekey, &pFillInfo->interval, pFillInfo->precision);
  }

  int64_t numOfRes = -1;
  if (numOfRows > 0) {  // still fill gap within current data block, not generating data after the result set.
    TSKEY lastKey = tsList[pFillInfo->numOfRows - 1];
    numOfRes = taosTimeCountInterval(
      lastKey,
      pFillInfo->currentKey,
      pFillInfo->interval.sliding,
      pFillInfo->interval.slidingUnit,
      pFillInfo->precision);
    numOfRes += 1;
    assert(numOfRes >= numOfRows);
  } else { // reach the end of data
    if ((ekey1 < pFillInfo->currentKey && FILL_IS_ASC_FILL(pFillInfo)) ||
        (ekey1 > pFillInfo->currentKey && !FILL_IS_ASC_FILL(pFillInfo))) {
      return 0;
    }
    numOfRes = taosTimeCountInterval(
      ekey1,
      pFillInfo->currentKey,
      pFillInfo->interval.sliding,
      pFillInfo->interval.slidingUnit,
      pFillInfo->precision);
    numOfRes += 1;
  }

  return (numOfRes > maxNumOfRows) ? maxNumOfRows : numOfRes;
}

int32_t taosGetLinearInterpolationVal(SPoint* point, int32_t outputType, SPoint* point1, SPoint* point2, int32_t inputType) {
  double v1 = -1, v2 = -1;
  GET_TYPED_DATA(v1, double, inputType, point1->val);
  GET_TYPED_DATA(v2, double, inputType, point2->val);

  double r = DO_INTERPOLATION(v1, v2, point1->key, point2->key, point->key);
  SET_TYPED_DATA(point->val, outputType, r);

  return TSDB_CODE_SUCCESS;
}

int64_t taosFillResultDataBlock(SFillInfo* pFillInfo, tFilePage** output, int32_t capacity) {
  int32_t remain = taosNumOfRemainRows(pFillInfo);

  int64_t numOfRes = getNumOfResultsAfterFillGap(pFillInfo, pFillInfo->end, capacity);
  assert(numOfRes <= capacity);

  // no data existed for fill operation now, append result according to the fill strategy
  if (remain == 0) {
    appendFilledResult(pFillInfo, output, numOfRes);
  } else {
    fillResultImpl(pFillInfo, output, (int32_t) numOfRes);
    assert(numOfRes == pFillInfo->numOfCurrent);
  }

  qDebug("fill:%p, generated fill result, src block:%d, index:%d, brange:%"PRId64"-%"PRId64", currentKey:%"PRId64", current:%d, total:%d, %p",
      pFillInfo, pFillInfo->numOfRows, pFillInfo->index, pFillInfo->start, pFillInfo->end, pFillInfo->currentKey, pFillInfo->numOfCurrent,
         pFillInfo->numOfTotal, pFillInfo->handle);

  return numOfRes;
}
