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

#include "qFill.h"
#include "os.h"
#include "qExtbuffer.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tsqlfunction.h"

#define FILL_IS_ASC_FILL(_f) ((_f)->order == TSDB_ORDER_ASC)

SFillInfo* taosInitFillInfo(int32_t order, TSKEY skey, int32_t numOfTags, int32_t capacity, int32_t numOfCols,
    int64_t slidingTime, int8_t slidingUnit, int8_t precision, int32_t fillType, SFillColInfo* pFillCol) {
  if (fillType == TSDB_FILL_NONE) {
    return NULL;
  }
  
  SFillInfo* pFillInfo = calloc(1, sizeof(SFillInfo));
  
  taosResetFillInfo(pFillInfo, skey);
  
  pFillInfo->order     = order;
  pFillInfo->fillType  = fillType;
  pFillInfo->pFillCol  = pFillCol;
  pFillInfo->numOfTags = numOfTags;
  pFillInfo->numOfCols = numOfCols;
  pFillInfo->precision = precision;

  pFillInfo->interval.interval = slidingTime;
  pFillInfo->interval.intervalUnit = slidingUnit;
  pFillInfo->interval.sliding = slidingTime;
  pFillInfo->interval.slidingUnit = slidingUnit;

  pFillInfo->pData = malloc(POINTER_BYTES * numOfCols);
  if (numOfTags > 0) {
    pFillInfo->pTags = calloc(pFillInfo->numOfTags, sizeof(SFillTagColInfo));
    for(int32_t i = 0; i < numOfTags; ++i) {
      pFillInfo->pTags[i].col.colId = -2;
    }
  }

  int32_t rowsize = 0;
  int32_t k = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    SFillColInfo* pColInfo = &pFillInfo->pFillCol[i];
    pFillInfo->pData[i] = calloc(1, pColInfo->col.bytes * capacity);

    if (TSDB_COL_IS_TAG(pColInfo->flag)) {
      bool exists = false;
      for(int32_t j = 0; j < k; ++j) {
        if (pFillInfo->pTags[j].col.colId == pColInfo->col.colId) {
          exists = true;
          break;
        }
      }

      if (!exists) {
        pFillInfo->pTags[k].col.colId = pColInfo->col.colId;
        pFillInfo->pTags[k].tagVal = calloc(1, pColInfo->col.bytes);

        k += 1;
      }
    }
    rowsize += pColInfo->col.bytes;
  }

  pFillInfo->rowSize = rowsize;
  pFillInfo->capacityInRows = capacity;
  
  return pFillInfo;
}

void taosResetFillInfo(SFillInfo* pFillInfo, TSKEY startTimestamp) {
  pFillInfo->start        = startTimestamp;
  pFillInfo->rowIdx       = -1;
  pFillInfo->numOfRows    = 0;
  pFillInfo->numOfCurrent = 0;
  pFillInfo->numOfTotal   = 0;
}

void* taosDestroyFillInfo(SFillInfo* pFillInfo) {
  if (pFillInfo == NULL) {
    return NULL;
  }

  taosTFree(pFillInfo->prevValues);
  taosTFree(pFillInfo->nextValues);
  taosTFree(pFillInfo->pTags);
  
  for(int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    taosTFree(pFillInfo->pData[i]);
  }
  
  taosTFree(pFillInfo->pData);
  taosTFree(pFillInfo->pFillCol);
  
  taosTFree(pFillInfo);
  return NULL;
}

void taosFillSetStartInfo(SFillInfo* pFillInfo, int32_t numOfRows, TSKEY endKey) {
  if (pFillInfo->fillType == TSDB_FILL_NONE) {
    return;
  }

  pFillInfo->endKey = endKey;
  if (pFillInfo->order != TSDB_ORDER_ASC) {
    pFillInfo->endKey = taosTimeTruncate(endKey, &pFillInfo->interval, pFillInfo->precision);
  }

  pFillInfo->rowIdx    = 0;
  pFillInfo->numOfRows = numOfRows;
  
  // ensure the space
  if (pFillInfo->capacityInRows < numOfRows) {
    for(int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
      char* tmp = realloc(pFillInfo->pData[i], numOfRows*pFillInfo->pFillCol[i].col.bytes);
      assert(tmp != NULL); // todo handle error
      
      memset(tmp, 0, numOfRows*pFillInfo->pFillCol[i].col.bytes);
      pFillInfo->pData[i] = tmp;
    }
  }
}

void taosFillCopyInputDataFromFilePage(SFillInfo* pFillInfo, tFilePage** pInput) {
  // copy the data into source data buffer
  for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    memcpy(pFillInfo->pData[i], pInput[i]->data, pFillInfo->numOfRows * pFillInfo->pFillCol[i].col.bytes);
  }
}

void taosFillCopyInputDataFromOneFilePage(SFillInfo* pFillInfo, tFilePage* pInput) {
  assert(pFillInfo->numOfRows == pInput->num);

  for(int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
    SFillColInfo* pCol = &pFillInfo->pFillCol[i];

    char* data = pInput->data + pCol->col.offset * pInput->num;
    memcpy(pFillInfo->pData[i], data, (size_t)(pInput->num * pCol->col.bytes));

    if (TSDB_COL_IS_TAG(pCol->flag)) {  // copy the tag value to tag value buffer
      for (int32_t j = 0; j < pFillInfo->numOfTags; ++j) {
        SFillTagColInfo* pTag = &pFillInfo->pTags[j];
        if (pTag->col.colId == pCol->col.colId) {
          memcpy(pTag->tagVal, data, pCol->col.bytes);
          break;
        }
      }
    }
  }
}

int64_t getFilledNumOfRes(SFillInfo* pFillInfo, TSKEY ekey, int32_t maxNumOfRows) {
  int64_t* tsList = (int64_t*) pFillInfo->pData[0];

  int32_t numOfRows = taosNumOfRemainRows(pFillInfo);

  TSKEY ekey1 = ekey;
  if (pFillInfo->order != TSDB_ORDER_ASC) {
    pFillInfo->endKey = taosTimeTruncate(ekey, &pFillInfo->interval, pFillInfo->precision);
  }

  int64_t numOfRes = -1;
  if (numOfRows > 0) {  // still fill gap within current data block, not generating data after the result set.
    TSKEY lastKey = tsList[pFillInfo->numOfRows - 1];
    numOfRes = taosTimeCountInterval(
      lastKey,
      pFillInfo->start,
      pFillInfo->interval.sliding,
      pFillInfo->interval.slidingUnit,
      pFillInfo->precision);
    numOfRes += 1;
    assert(numOfRes >= numOfRows);
  } else { // reach the end of data
    if ((ekey1 < pFillInfo->start && FILL_IS_ASC_FILL(pFillInfo)) ||
        (ekey1 > pFillInfo->start && !FILL_IS_ASC_FILL(pFillInfo))) {
      return 0;
    }
    numOfRes = taosTimeCountInterval(
      ekey1,
      pFillInfo->start,
      pFillInfo->interval.sliding,
      pFillInfo->interval.slidingUnit,
      pFillInfo->precision);
    numOfRes += 1;
  }

  return (numOfRes > maxNumOfRows) ? maxNumOfRows : numOfRes;
}

int32_t taosNumOfRemainRows(SFillInfo* pFillInfo) {
  if (pFillInfo->rowIdx == -1 || pFillInfo->numOfRows == 0) {
    return 0;
  }

  return pFillInfo->numOfRows - pFillInfo->rowIdx;
}

// todo: refactor
static double linearInterpolationImpl(double v1, double v2, double k1, double k2, double k) {
  return v1 + (v2 - v1) * (k - k1) / (k2 - k1);
}

int taosDoLinearInterpolation(int32_t type, SPoint* point1, SPoint* point2, SPoint* point) {
  switch (type) {
    case TSDB_DATA_TYPE_INT: {
      *(int32_t*)point->val = (int32_t)linearInterpolationImpl(*(int32_t*)point1->val, *(int32_t*)point2->val, (double)point1->key,
                                                              (double)point2->key, (double)point->key);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      *(float*)point->val = (float)
        linearInterpolationImpl(*(float*)point1->val, *(float*)point2->val, (double)point1->key, (double)point2->key, (double)point->key);
      break;
    };
    case TSDB_DATA_TYPE_DOUBLE: {
      *(double*)point->val =
        linearInterpolationImpl(*(double*)point1->val, *(double*)point2->val, (double)point1->key, (double)point2->key, (double)point->key);
      break;
    };
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT: {
      *(int64_t*)point->val = (int64_t)linearInterpolationImpl((double)(*(int64_t*)point1->val), (double)(*(int64_t*)point2->val), (double)point1->key,
        (double)point2->key, (double)point->key);
      break;
    };
    case TSDB_DATA_TYPE_SMALLINT: {
      *(int16_t*)point->val = (int16_t)linearInterpolationImpl(*(int16_t*)point1->val, *(int16_t*)point2->val, (double)point1->key,
        (double)point2->key, (double)point->key);
      break;
    };
    case TSDB_DATA_TYPE_TINYINT: {
      *(int8_t*) point->val = (int8_t)
        linearInterpolationImpl(*(int8_t*)point1->val, *(int8_t*)point2->val, (double)point1->key, (double)point2->key, (double)point->key);
      break;
    };
    default: {
      // TODO: Deal with interpolation with bool and strings and timestamp
      return -1;
    }
  }

  return 0;
}

static void setTagsValue(SFillInfo* pFillInfo, tFilePage** data, int32_t num) {
  for(int32_t j = 0; j < pFillInfo->numOfCols; ++j) {
    SFillColInfo* pCol = &pFillInfo->pFillCol[j];
    if (TSDB_COL_IS_NORMAL_COL(pCol->flag)) {
      continue;
    }

    char* val1 = elePtrAt(data[j]->data, pCol->col.bytes, num);

    for(int32_t i = 0; i < pFillInfo->numOfTags; ++i) {
      SFillTagColInfo* pTag = &pFillInfo->pTags[i];
      if (pTag->col.colId == pCol->col.colId) {
        assignVal(val1, pTag->tagVal, pCol->col.bytes, pCol->col.type);
        break;
      }
    }
  }
}

static void doFillResultImpl(SFillInfo* pFillInfo, tFilePage** data, int32_t* num, char** srcData, int64_t ts,
                             bool outOfBound) {
  char* prevValues = pFillInfo->prevValues;
  char* nextValues = pFillInfo->nextValues;

  SPoint point1, point2, point;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pFillInfo->order);

  char* val = elePtrAt(data[0]->data, TSDB_KEYSIZE, *num);
  *(TSKEY*) val = pFillInfo->start;

  int32_t numOfValCols = pFillInfo->numOfCols - pFillInfo->numOfTags;

  // set the other values
  if (pFillInfo->fillType == TSDB_FILL_PREV) {
    char* p = FILL_IS_ASC_FILL(pFillInfo) ? prevValues : nextValues;
    
    if (p != NULL) {
      for (int32_t i = 1; i < numOfValCols; ++i) {
        SFillColInfo* pCol = &pFillInfo->pFillCol[i];
        
        char* val1 = elePtrAt(data[i]->data, pCol->col.bytes, *num);
        if (isNull(p + pCol->col.offset, pCol->col.type)) {
          if (pCol->col.type == TSDB_DATA_TYPE_BINARY || pCol->col.type == TSDB_DATA_TYPE_NCHAR) {
            setVardataNull(val1, pCol->col.type);
          } else {
            setNull(val1, pCol->col.type, pCol->col.bytes);
          }
        } else {
          assignVal(val1, p + pCol->col.offset, pCol->col.bytes, pCol->col.type);
        }
      }
    } else { // no prev value yet, set the value for NULL
      for (int32_t i = 1; i < numOfValCols; ++i) {
        SFillColInfo* pCol = &pFillInfo->pFillCol[i];

        char* val1 = elePtrAt(data[i]->data, pCol->col.bytes, *num);
        if (pCol->col.type == TSDB_DATA_TYPE_BINARY||pCol->col.type == TSDB_DATA_TYPE_NCHAR) {
          setVardataNull(val1, pCol->col.type);
        } else {
          setNull(val1, pCol->col.type, pCol->col.bytes);
        }
      }
    }

    setTagsValue(pFillInfo, data, *num);
  } else if (pFillInfo->fillType == TSDB_FILL_LINEAR) {
    // TODO : linear interpolation supports NULL value
    if (prevValues != NULL && !outOfBound) {
      for (int32_t i = 1; i < numOfValCols; ++i) {
        SFillColInfo* pCol = &pFillInfo->pFillCol[i];
        
        int16_t type  = pCol->col.type;
        int16_t bytes = pCol->col.bytes;
        
        char *val1 = elePtrAt(data[i]->data, pCol->col.bytes, *num);
        if (type == TSDB_DATA_TYPE_BINARY|| type == TSDB_DATA_TYPE_NCHAR) {
          setVardataNull(val1, pCol->col.type);
          continue;
        } else if (type == TSDB_DATA_TYPE_BOOL) {
          setNull(val1, pCol->col.type, bytes);
          continue;
        }

        point1 = (SPoint){.key = *(TSKEY*)(prevValues), .val = prevValues + pCol->col.offset};
        point2 = (SPoint){.key = ts, .val = srcData[i] + pFillInfo->rowIdx * bytes};
        point  = (SPoint){.key = pFillInfo->start, .val = val1};
        taosDoLinearInterpolation(type, &point1, &point2, &point);
      }

      setTagsValue(pFillInfo, data, *num);

    } else {
      for (int32_t i = 1; i < numOfValCols; ++i) {
        SFillColInfo* pCol = &pFillInfo->pFillCol[i];
  
        char* val1 = elePtrAt(data[i]->data, pCol->col.bytes, *num);
  
        if (pCol->col.type == TSDB_DATA_TYPE_BINARY || pCol->col.type == TSDB_DATA_TYPE_NCHAR) {
          setVardataNull(val1, pCol->col.type);
        } else {
          setNull(val1, pCol->col.type, pCol->col.bytes);
        }
      }

      setTagsValue(pFillInfo, data, *num);
  
    }
  } else { /* fill the default value */
    for (int32_t i = 1; i < numOfValCols; ++i) {
      SFillColInfo* pCol = &pFillInfo->pFillCol[i];
      
      char* val1 = elePtrAt(data[i]->data, pCol->col.bytes, *num);
      assignVal(val1, (char*)&pCol->fillVal.i, pCol->col.bytes, pCol->col.type);
    }

    setTagsValue(pFillInfo, data, *num);
  }

  pFillInfo->start = taosTimeAdd(pFillInfo->start, pFillInfo->interval.sliding * step, pFillInfo->interval.slidingUnit, pFillInfo->precision);
  pFillInfo->numOfCurrent++;

  (*num) += 1;
}

static void initBeforeAfterDataBuf(SFillInfo* pFillInfo, char** nextValues) {
  if (*nextValues != NULL) {
    return;
  }
  
  *nextValues = calloc(1, pFillInfo->rowSize);
  for (int i = 1; i < pFillInfo->numOfCols; i++) {
    SFillColInfo* pCol = &pFillInfo->pFillCol[i];
  
    if (pCol->col.type == TSDB_DATA_TYPE_BINARY||pCol->col.type == TSDB_DATA_TYPE_NCHAR) {
      setVardataNull(*nextValues + pCol->col.offset, pCol->col.type);
    } else {
      setNull(*nextValues + pCol->col.offset, pCol->col.type, pCol->col.bytes);
    }
  }
}

int32_t generateDataBlockImpl(SFillInfo* pFillInfo, tFilePage** data, int32_t numOfRows, int32_t outputRows, char** srcData) {
  int32_t num = 0;
  pFillInfo->numOfCurrent = 0;

  char** prevValues = &pFillInfo->prevValues;
  char** nextValues = &pFillInfo->nextValues;

  int32_t numOfTags = pFillInfo->numOfTags;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pFillInfo->order);
  if (numOfRows == 0) {
    /*
     * These data are generated according to fill strategy, since the current timestamp is out of time window of
     * real result set. Note that we need to keep the direct previous result rows, to generated the filled data.
     */
    while (num < outputRows) {
      doFillResultImpl(pFillInfo, data, &num, srcData, pFillInfo->start, true);
    }
    
    pFillInfo->numOfTotal += pFillInfo->numOfCurrent;
    return outputRows;

  } else {
    while (1) {
      int64_t ts = ((int64_t*)pFillInfo->pData[0])[pFillInfo->rowIdx];

      if ((pFillInfo->start < ts && FILL_IS_ASC_FILL(pFillInfo)) ||
          (pFillInfo->start > ts && !FILL_IS_ASC_FILL(pFillInfo))) {
        /* set the next value for interpolation */
        initBeforeAfterDataBuf(pFillInfo, nextValues);
        
        int32_t offset = pFillInfo->rowIdx;
        for (int32_t i = 0; i < pFillInfo->numOfCols - numOfTags; ++i) {
          SFillColInfo* pCol = &pFillInfo->pFillCol[i];
          memcpy(*nextValues + pCol->col.offset, srcData[i] + offset * pCol->col.bytes, pCol->col.bytes);
        }
      }

      if (((pFillInfo->start < ts && FILL_IS_ASC_FILL(pFillInfo)) ||
           (pFillInfo->start > ts && !FILL_IS_ASC_FILL(pFillInfo))) && num < outputRows) {
        
        while (((pFillInfo->start < ts && FILL_IS_ASC_FILL(pFillInfo)) ||
                (pFillInfo->start > ts && !FILL_IS_ASC_FILL(pFillInfo))) && num < outputRows) {
          doFillResultImpl(pFillInfo, data, &num, srcData, ts, false);
        }

        /* output buffer is full, abort */
        if ((num == outputRows && FILL_IS_ASC_FILL(pFillInfo)) || (num < 0 && !FILL_IS_ASC_FILL(pFillInfo))) {
          pFillInfo->numOfTotal += pFillInfo->numOfCurrent;
          return outputRows;
        }
      } else {
        assert(pFillInfo->start == ts);
        initBeforeAfterDataBuf(pFillInfo, prevValues);
        
        // assign rows to dst buffer
        for (int32_t i = 0; i < pFillInfo->numOfCols; ++i) {
          SFillColInfo* pCol = &pFillInfo->pFillCol[i];
          if (TSDB_COL_IS_TAG(pCol->flag)) {
            continue;
          }

          char* val1 = elePtrAt(data[i]->data, pCol->col.bytes, num);
          char* src  = elePtrAt(srcData[i], pCol->col.bytes, pFillInfo->rowIdx);
          
          if (i == 0 ||
              (pCol->functionId != TSDB_FUNC_COUNT && !isNull(src, pCol->col.type)) ||
              (pCol->functionId == TSDB_FUNC_COUNT && GET_INT64_VAL(src) != 0)) {
            assignVal(val1, src, pCol->col.bytes, pCol->col.type);
            memcpy(*prevValues + pCol->col.offset, src, pCol->col.bytes);
          } else {  // i > 0 and data is null , do interpolation
            if (pFillInfo->fillType == TSDB_FILL_PREV) {
              assignVal(val1, *prevValues + pCol->col.offset, pCol->col.bytes, pCol->col.type);
            } else if (pFillInfo->fillType == TSDB_FILL_LINEAR) {
              assignVal(val1, src, pCol->col.bytes, pCol->col.type);
              memcpy(*prevValues + pCol->col.offset, src, pCol->col.bytes);
            } else {
              assignVal(val1, (char*) &pCol->fillVal.i, pCol->col.bytes, pCol->col.type);
            }
          }
        }

        // set the tag value for final result
        setTagsValue(pFillInfo, data, num);

        pFillInfo->start = taosTimeAdd(pFillInfo->start, pFillInfo->interval.sliding*step, pFillInfo->interval.slidingUnit, pFillInfo->precision);
        pFillInfo->rowIdx += 1;

        pFillInfo->numOfCurrent +=1;
        num += 1;
      }

      if ((pFillInfo->rowIdx >= pFillInfo->numOfRows && FILL_IS_ASC_FILL(pFillInfo)) ||
          (pFillInfo->rowIdx < 0 && !FILL_IS_ASC_FILL(pFillInfo)) || num >= outputRows) {
        if (pFillInfo->rowIdx >= pFillInfo->numOfRows || pFillInfo->rowIdx < 0) {
          pFillInfo->rowIdx = -1;
          pFillInfo->numOfRows = 0;

          /* the raw data block is exhausted, next value does not exists */
          taosTFree(*nextValues);
        }

        pFillInfo->numOfTotal += pFillInfo->numOfCurrent;
        return num;
      }
    }
  }
}

int64_t taosGenerateDataBlock(SFillInfo* pFillInfo, tFilePage** output, int32_t capacity) {
  int32_t remain = taosNumOfRemainRows(pFillInfo);  // todo use iterator?

  int32_t rows = (int32_t)getFilledNumOfRes(pFillInfo, pFillInfo->endKey, capacity);
  int32_t numOfRes = generateDataBlockImpl(pFillInfo, output, remain, rows, pFillInfo->pData);
  assert(numOfRes == rows);
  
  return numOfRes;
}
