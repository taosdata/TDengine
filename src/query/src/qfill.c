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

#include "qfill.h"
#include "os.h"
#include "qextbuffer.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tsqlfunction.h"

#define FILL_IS_ASC_FILL(_f) ((_f)->order == TSDB_ORDER_ASC)

int64_t taosGetIntervalStartTimestamp(int64_t startTime, int64_t slidingTime, char timeUnit, int16_t precision) {
  if (slidingTime == 0) {
    return startTime;
  }

  if (timeUnit == 'a' || timeUnit == 'm' || timeUnit == 's' || timeUnit == 'h') {
    return (startTime / slidingTime) * slidingTime;
  } else {
    /*
     * here we revised the start time of day according to the local time zone,
     * but in case of DST, the start time of one day need to be dynamically decided.
     *
     * TODO dynamically decide the start time of a day
     */

    // todo refactor to extract function that is available for Linux/Windows/Mac platform
#if defined(WINDOWS) && _MSC_VER >= 1900
    // see https://docs.microsoft.com/en-us/cpp/c-runtime-library/daylight-dstbias-timezone-and-tzname?view=vs-2019
    int64_t timezone = _timezone;
    int32_t daylight = _daylight;
    char**  tzname = _tzname;
#endif

    int64_t t = (precision == TSDB_TIME_PRECISION_MILLI) ? MILLISECOND_PER_SECOND : MILLISECOND_PER_SECOND * 1000L;

    int64_t revStartime = (startTime / slidingTime) * slidingTime + timezone * t;
    int64_t revEndtime = revStartime + slidingTime - 1;
    if (revEndtime < startTime) {
      revStartime += slidingTime;
    }

    return revStartime;
  }
}

SFillInfo* taosInitFillInfo(int32_t order, TSKEY skey, int32_t numOfTags, int32_t capacity, int32_t numOfCols,
    int64_t slidingTime, int32_t fillType, SFillColInfo* pFillCol) {
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
  pFillInfo->slidingTime = slidingTime;
  
  pFillInfo->pData = malloc(POINTER_BYTES * numOfCols);
  
  int32_t rowsize = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    int32_t bytes = pFillInfo->pFillCol[i].col.bytes;
    pFillInfo->pData[i] = calloc(1, sizeof(tFilePage) + bytes * capacity);
    
    rowsize += bytes;
  }
  
  if (numOfTags > 0) {
    pFillInfo->pTags = calloc(1, pFillInfo->numOfTags * POINTER_BYTES + rowsize);
  }
  
  pFillInfo->rowSize = rowsize;
  return pFillInfo;
}

void taosResetFillInfo(SFillInfo* pFillInfo, TSKEY startTimestamp) {
  pFillInfo->start        = startTimestamp;
  pFillInfo->rowIdx       = -1;
  pFillInfo->numOfRows    = 0;
  pFillInfo->numOfCurrent = 0;
  pFillInfo->numOfTotal   = 0;
}

void taosDestoryFillInfo(SFillInfo* pFillInfo) {
  if (pFillInfo == NULL) {
    return;
  }

  tfree(pFillInfo->prevValues);
  tfree(pFillInfo->nextValues);
  tfree(pFillInfo->pTags);
  tfree(pFillInfo);
}

void taosFillSetStartInfo(SFillInfo* pFillInfo, int32_t numOfRows, TSKEY endKey) {
  if (pFillInfo->fillType == TSDB_FILL_NONE) {
    return;
  }

  pFillInfo->rowIdx = 0;
  pFillInfo->numOfRows = numOfRows;
  
  pFillInfo->endKey = endKey;
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
    
    char* s = pInput->data + pCol->col.offset * pInput->num;
    memcpy(pFillInfo->pData[i], s, pInput->num * pCol->col.bytes);
    
    if (pCol->flag == TSDB_COL_TAG) {  // copy the tag value
      memcpy(pFillInfo->pTags[i], pFillInfo->pData[i], pCol->col.bytes);
    }
  }
}

TSKEY taosGetRevisedEndKey(TSKEY ekey, int32_t order, int64_t timeInterval, int8_t slidingTimeUnit, int8_t precision) {
  if (order == TSDB_ORDER_ASC) {
    return ekey;
  } else {
    return taosGetIntervalStartTimestamp(ekey, timeInterval, slidingTimeUnit, precision);
  }
}

static int32_t taosGetTotalNumOfFilledRes(SFillInfo* pFillInfo, const TSKEY* tsArray, int32_t remain,
    int64_t nInterval, int64_t ekey) {
  
  if (remain > 0) {  // still fill gap within current data block, not generating data after the result set.
    TSKEY lastKey = tsArray[pFillInfo->numOfRows - 1];
    int32_t total = (int32_t)(labs(lastKey - pFillInfo->start) / nInterval) + 1;

    assert(total >= remain);
    return total;
  } else { // reach the end of data
    if ((ekey < pFillInfo->start && FILL_IS_ASC_FILL(pFillInfo)) ||
        (ekey > pFillInfo->start && !FILL_IS_ASC_FILL(pFillInfo))) {
      return 0;
    } else {
      return (int32_t)(labs(ekey - pFillInfo->start) / nInterval) + 1;
    }
  }
}

int32_t taosGetNumOfResultWithFill(SFillInfo* pFillInfo, int32_t numOfRows, int64_t ekey, int32_t maxNumOfRows) {
  int32_t numOfRes = taosGetTotalNumOfFilledRes(pFillInfo, (int64_t*) pFillInfo->pData[0], numOfRows,
                                                pFillInfo->slidingTime, ekey);
  return (numOfRes > maxNumOfRows) ? maxNumOfRows : numOfRes;
}

int32_t taosNumOfRemainRows(SFillInfo* pFillInfo) {
  if (pFillInfo->rowIdx == -1 || pFillInfo->numOfRows == 0) {
    return 0;
  }

  return FILL_IS_ASC_FILL(pFillInfo) ? (pFillInfo->numOfRows - pFillInfo->rowIdx)
                                                : pFillInfo->rowIdx + 1;
}

// todo: refactor
static double linearInterpolationImpl(double v1, double v2, double k1, double k2, double k) {
  return v1 + (v2 - v1) * (k - k1) / (k2 - k1);
}

int taosDoLinearInterpolation(int32_t type, SPoint* point1, SPoint* point2, SPoint* point) {
  switch (type) {
    case TSDB_DATA_TYPE_INT: {
      *(int32_t*)point->val = linearInterpolationImpl(*(int32_t*)point1->val, *(int32_t*)point2->val, point1->key,
                                                        point2->key, point->key);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      *(float*)point->val =
          linearInterpolationImpl(*(float*)point1->val, *(float*)point2->val, point1->key, point2->key, point->key);
      break;
    };
    case TSDB_DATA_TYPE_DOUBLE: {
      *(double*)point->val =
          linearInterpolationImpl(*(double*)point1->val, *(double*)point2->val, point1->key, point2->key, point->key);
      break;
    };
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT: {
      *(int64_t*)point->val = linearInterpolationImpl(*(int64_t*)point1->val, *(int64_t*)point2->val, point1->key,
                                                        point2->key, point->key);
      break;
    };
    case TSDB_DATA_TYPE_SMALLINT: {
      *(int16_t*)point->val = linearInterpolationImpl(*(int16_t*)point1->val, *(int16_t*)point2->val, point1->key,
                                                        point2->key, point->key);
      break;
    };
    case TSDB_DATA_TYPE_TINYINT: {
      *(int8_t*)point->val =
          linearInterpolationImpl(*(int8_t*)point1->val, *(int8_t*)point2->val, point1->key, point2->key, point->key);
      break;
    };
    default: {
      // TODO: Deal with interpolation with bool and strings and timestamp
      return -1;
    }
  }

  return 0;
}

static void setTagsValue(SFillInfo* pColInfo, tFilePage** data, char** pTags, int32_t start, int32_t num) {
  for (int32_t j = 0, i = start; i < pColInfo->numOfCols + pColInfo->numOfTags; ++i, ++j) {
    SFillColInfo* pCol = &pColInfo->pFillCol[i];
    
    char* val1 = elePtrAt(data[i]->data, pCol->col.bytes, num);
    assignVal(val1, pTags[j], pCol->col.bytes, pCol->col.type);
  }
}

static void doInterpoResultImpl(SFillInfo* pFillInfo, tFilePage** data, int32_t* num, char** srcData,
                                int64_t ts, char** pTags, bool outOfBound) {
  char** prevValues = &pFillInfo->prevValues;
  char** nextValues = &pFillInfo->nextValues;

  SPoint point1, point2, point;

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pFillInfo->order);

  char* val = elePtrAt(data[0]->data, TSDB_KEYSIZE, *num);
  *(TSKEY*) val = pFillInfo->start;

  int32_t numOfValCols = pFillInfo->numOfCols - pFillInfo->numOfTags;

  // set the other values
  if (pFillInfo->fillType == TSDB_FILL_PREV) {
    char* pInterpolationData = FILL_IS_ASC_FILL(pFillInfo) ? *prevValues : *nextValues;
    if (pInterpolationData != NULL) {
      for (int32_t i = 1; i < numOfValCols; ++i) {
        SFillColInfo* pCol = &pFillInfo->pFillCol[i];
        
        char* val1 = elePtrAt(data[i]->data, pCol->col.bytes, *num);
        if (isNull(pInterpolationData + pCol->col.offset, pCol->col.type)) {
          setNull(val1, pCol->col.type, pCol->col.bytes);
        } else {
          assignVal(val1, pInterpolationData + pCol->col.offset, pCol->col.bytes, pCol->col.type);
        }
      }
    } else { // no prev value yet, set the value for NULL
      for (int32_t i = 1; i < numOfValCols; ++i) {
        SFillColInfo* pCol = &pFillInfo->pFillCol[i];

        char* val1 = elePtrAt(data[i]->data, pCol->col.bytes, *num);
        setNull(val1, pCol->col.type, pCol->col.bytes);
      }
    }

    setTagsValue(pFillInfo, data, pTags, numOfValCols, *num);
  } else if (pFillInfo->fillType == TSDB_FILL_LINEAR) {
    // TODO : linear interpolation supports NULL value
    if (*prevValues != NULL && !outOfBound) {
      for (int32_t i = 1; i < numOfValCols; ++i) {
        SFillColInfo* pCol = &pFillInfo->pFillCol[i];
        
        int16_t type  = pCol->col.type;
        int16_t bytes = pCol->col.bytes;
        
        char *val1 = elePtrAt(data[i]->data, pCol->col.bytes, *num);
        if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR || type == TSDB_DATA_TYPE_BOOL) {
          setNull(val1, pCol->col.type, bytes);
          continue;
        }

        point1 = (SPoint){.key = *(TSKEY*)(*prevValues), .val = *prevValues + pCol->col.offset};
        point2 = (SPoint){.key = ts, .val = srcData[i] + pFillInfo->rowIdx * bytes};
        point = (SPoint){.key = pFillInfo->start, .val = val1};
        taosDoLinearInterpolation(type, &point1, &point2, &point);
      }

      setTagsValue(pFillInfo, data, pTags, numOfValCols, *num);

    } else {
      for (int32_t i = 1; i < numOfValCols; ++i) {
        SFillColInfo* pCol = &pFillInfo->pFillCol[i];
  
        char* val1 = elePtrAt(data[i]->data, pCol->col.bytes, *num);
        setNull(val1, pCol->col.type, pCol->col.bytes);
      }

      setTagsValue(pFillInfo, data, pTags, numOfValCols, *num);
  
    }
  } else { /* default value interpolation */
    for (int32_t i = 1; i < numOfValCols; ++i) {
      SFillColInfo* pCol = &pFillInfo->pFillCol[i];
      
      char* val1 = elePtrAt(data[i]->data, pCol->col.bytes, *num);
      assignVal(val1, (char*)&pCol->defaultVal.i, pCol->col.bytes, pCol->col.type);
    }

    setTagsValue(pFillInfo, data, pTags, numOfValCols, *num);
  }

  pFillInfo->start += (pFillInfo->slidingTime * step);
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
    setNull(*nextValues + pCol->col.offset, pCol->col.type, pCol->col.bytes);
  }
}

int32_t taosDoInterpoResult(SFillInfo* pFillInfo, tFilePage** data, int32_t numOfRows, int32_t outputRows, char** srcData) {
  int32_t num = 0;
  pFillInfo->numOfCurrent = 0;

  char** prevValues = &pFillInfo->prevValues;
  char** nextValues = &pFillInfo->nextValues;

  int32_t numOfTags = pFillInfo->numOfTags;
  char**  pTags = pFillInfo->pTags;

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pFillInfo->order);

  if (numOfRows == 0) {
    /*
     * we need to rebuild whole result set
     * NOTE:we need to keep the last saved data, to generated the filled data
     */
    while (num < outputRows) {
      doInterpoResultImpl(pFillInfo, data, &num, srcData, pFillInfo->start, pTags, true);
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
          doInterpoResultImpl(pFillInfo, data, &num, srcData, pFillInfo->start, pTags, false);
        }

        /* output buffer is full, abort */
        if ((num == outputRows && FILL_IS_ASC_FILL(pFillInfo)) ||
            (num < 0 && !FILL_IS_ASC_FILL(pFillInfo))) {
          pFillInfo->numOfTotal += pFillInfo->numOfCurrent;
          return outputRows;
        }
      } else {
        assert(pFillInfo->start == ts);
        initBeforeAfterDataBuf(pFillInfo, prevValues);
        
        // assign rows to dst buffer
        int32_t i = 0;
        for (; i < pFillInfo->numOfCols - numOfTags; ++i) {
          SFillColInfo* pCol = &pFillInfo->pFillCol[i];
          
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
              assignVal(val1, (char*) &pCol->defaultVal.i, pCol->col.bytes, pCol->col.type);
            }
          }
        }

        // set the tag value for final result
        setTagsValue(pFillInfo, data, pTags, pFillInfo->numOfCols - numOfTags, num);

        pFillInfo->start += (pFillInfo->slidingTime * step);
        pFillInfo->rowIdx += 1;
        num += 1;
      }

      if ((pFillInfo->rowIdx >= pFillInfo->numOfRows && FILL_IS_ASC_FILL(pFillInfo)) ||
          (pFillInfo->rowIdx < 0 && !FILL_IS_ASC_FILL(pFillInfo)) || num >= outputRows) {
        if (pFillInfo->rowIdx >= pFillInfo->numOfRows || pFillInfo->rowIdx < 0) {
          pFillInfo->rowIdx = -1;
          pFillInfo->numOfRows = 0;

          /* the raw data block is exhausted, next value does not exists */
          tfree(*nextValues);
        }

        pFillInfo->numOfTotal += pFillInfo->numOfCurrent;
        return num;
      }
    }
  }
}

void taosGenerateDataBlock(SFillInfo* pFillInfo, tFilePage** output, int64_t* outputRows, int32_t capacity) {
    int32_t remain = taosNumOfRemainRows(pFillInfo);  // todo use iterator?
    
//    TSKEY ekey = taosGetRevisedEndKey(pQuery->window.ekey, pQuery->order.order, pQuery->slidingTime,
//                                      pQuery->slidingTimeUnit, pQuery->precision);
//    if (QUERY_IS_ASC_QUERY(pQuery)) {
//      assert(ekey >= pQuery->window.ekey);
//    } else {
//      assert(ekey <= pQuery->window.ekey);
//    }
    
    int32_t rows = taosGetNumOfResultWithFill(pFillInfo, remain, pFillInfo->endKey, capacity);
  
    int32_t numOfRes = taosDoInterpoResult(pFillInfo, output, remain, rows, pFillInfo->pData);
    *outputRows = rows;
    
    assert(numOfRes == rows);
}
