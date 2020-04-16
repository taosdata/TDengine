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
#include "taosmsg.h"
#include "textbuffer.h"
#include "tinterpolation.h"
#include "tsqlfunction.h"
#include "ttypes.h"

#define INTERPOL_IS_ASC_INTERPOL(interp) ((interp)->order == TSQL_SO_ASC)

int64_t taosGetIntervalStartTimestamp(int64_t startTime, int64_t timeRange, char slidingTimeUnit, int16_t precision) {
  if (timeRange == 0) {
    return startTime;
  }

  if (slidingTimeUnit == 'a' || slidingTimeUnit == 'm' || slidingTimeUnit == 's' || slidingTimeUnit == 'h' || slidingTimeUnit == 'u') {
    return (startTime / timeRange) * timeRange;
  } else {
    /*
     * here we revised the start time of day according to the local time zone,
     * but in case of DST, the start time of one day need to be dynamically decided.
     *
     * TODO dynamically decide the start time of a day
     */

#if defined(WINDOWS) && _MSC_VER >= 1900
    // see https://docs.microsoft.com/en-us/cpp/c-runtime-library/daylight-dstbias-timezone-and-tzname?view=vs-2019
    int64_t timezone = _timezone;
    int32_t daylight = _daylight;
    char**  tzname = _tzname;
#endif

    int64_t t = (precision == TSDB_TIME_PRECISION_MILLI) ? MILLISECOND_PER_SECOND : MILLISECOND_PER_SECOND * 1000L;

    int64_t revStartime = (startTime / timeRange) * timeRange + timezone * t;
    int64_t revEndtime = revStartime + timeRange - 1;
    if (revEndtime < startTime) {
      revStartime += timeRange;
    }

    return revStartime;
  }
}

void taosInitInterpoInfo(SInterpolationInfo* pInterpoInfo, int32_t order, int64_t startTimestamp,
                         int32_t numOfGroupbyTags, int32_t rowSize) {
  pInterpoInfo->startTimestamp = startTimestamp;
  pInterpoInfo->rowIdx = -1;
  pInterpoInfo->numOfRawDataInRows = 0;
  pInterpoInfo->numOfCurrentInterpo = 0;
  pInterpoInfo->numOfTotalInterpo = 0;
  pInterpoInfo->order = order;

  pInterpoInfo->numOfTags = numOfGroupbyTags;
  if (pInterpoInfo->pTags == NULL && numOfGroupbyTags > 0) {
    pInterpoInfo->pTags = calloc(1, numOfGroupbyTags * POINTER_BYTES + rowSize);
  }

  // set the previous value to be null
  tfree(pInterpoInfo->prevValues);
}

// the SInterpolationInfo itself will not be released
void taosDestoryInterpoInfo(SInterpolationInfo* pInterpoInfo) {
  if (pInterpoInfo == NULL) {
    return;
  }

  tfree(pInterpoInfo->prevValues);
  tfree(pInterpoInfo->nextValues);

  tfree(pInterpoInfo->pTags);
}

void taosInterpoSetStartInfo(SInterpolationInfo* pInterpoInfo, int32_t numOfRawDataInRows, int32_t type) {
  if (type == TSDB_INTERPO_NONE) {
    return;
  }

  pInterpoInfo->rowIdx = 0;
  pInterpoInfo->numOfRawDataInRows = numOfRawDataInRows;
}

TSKEY taosGetRevisedEndKey(TSKEY ekey, int32_t order, int32_t timeInterval, int8_t slidingTimeUnit, int8_t precision) {
  if (order == TSQL_SO_ASC) {
    return ekey;
  } else {
    return taosGetIntervalStartTimestamp(ekey, timeInterval, slidingTimeUnit, precision);
  }
}

int32_t taosGetNumOfResultWithInterpo(SInterpolationInfo* pInterpoInfo, TSKEY* pPrimaryKeyArray,
                                      int32_t numOfRawDataInRows, int64_t nInterval, int64_t ekey,
                                      int32_t maxNumOfRows) {
  int32_t numOfRes = taosGetNumOfResWithoutLimit(pInterpoInfo, pPrimaryKeyArray, numOfRawDataInRows, nInterval, ekey);
  return (numOfRes > maxNumOfRows) ? maxNumOfRows : numOfRes;
}

int32_t taosGetNumOfResWithoutLimit(SInterpolationInfo* pInterpoInfo, int64_t* pPrimaryKeyArray,
                                    int32_t numOfAvailRawData, int64_t nInterval, int64_t ekey) {
  if (numOfAvailRawData > 0) {
    int32_t finalNumOfResult = 0;

    // get last timestamp, calculate the result size
    int64_t lastKey = pPrimaryKeyArray[pInterpoInfo->numOfRawDataInRows - 1];
    finalNumOfResult = (int32_t)(labs(lastKey - pInterpoInfo->startTimestamp) / nInterval) + 1;

    assert(finalNumOfResult >= numOfAvailRawData);
    return finalNumOfResult;
  } else {
    /* reach the end of data */
    if ((ekey < pInterpoInfo->startTimestamp && INTERPOL_IS_ASC_INTERPOL(pInterpoInfo)) ||
        (ekey > pInterpoInfo->startTimestamp && !INTERPOL_IS_ASC_INTERPOL(pInterpoInfo))) {
      return 0;
    } else {
      return (int32_t)(labs(ekey - pInterpoInfo->startTimestamp) / nInterval) + 1;
    }
  }
}

bool taosHasRemainsDataForInterpolation(SInterpolationInfo* pInterpoInfo) {
  return taosNumOfRemainPoints(pInterpoInfo) > 0;
}

int32_t taosNumOfRemainPoints(SInterpolationInfo* pInterpoInfo) {
  if (pInterpoInfo->rowIdx == -1 || pInterpoInfo->numOfRawDataInRows == 0) {
    return 0;
  }

  return INTERPOL_IS_ASC_INTERPOL(pInterpoInfo) ? (pInterpoInfo->numOfRawDataInRows - pInterpoInfo->rowIdx)
                                                : pInterpoInfo->rowIdx + 1;
}

static double doLinearInterpolationImpl(double v1, double v2, double k1, double k2, double k) {
  return v1 + (v2 - v1) * (k - k1) / (k2 - k1);
}

int taosDoLinearInterpolation(int32_t type, SPoint* point1, SPoint* point2, SPoint* point) {
  switch (type) {
    case TSDB_DATA_TYPE_INT: {
      *(int32_t*)point->val = doLinearInterpolationImpl(*(int32_t*)point1->val, *(int32_t*)point2->val, point1->key,
                                                        point2->key, point->key);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      *(float*)point->val =
          doLinearInterpolationImpl(*(float*)point1->val, *(float*)point2->val, point1->key, point2->key, point->key);
      break;
    };
    case TSDB_DATA_TYPE_DOUBLE: {
      *(double*)point->val =
          doLinearInterpolationImpl(*(double*)point1->val, *(double*)point2->val, point1->key, point2->key, point->key);
      break;
    };
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT: {
      *(int64_t*)point->val = doLinearInterpolationImpl(*(int64_t*)point1->val, *(int64_t*)point2->val, point1->key,
                                                        point2->key, point->key);
      break;
    };
    case TSDB_DATA_TYPE_SMALLINT: {
      *(int16_t*)point->val = doLinearInterpolationImpl(*(int16_t*)point1->val, *(int16_t*)point2->val, point1->key,
                                                        point2->key, point->key);
      break;
    };
    case TSDB_DATA_TYPE_TINYINT: {
      *(int8_t*)point->val =
          doLinearInterpolationImpl(*(int8_t*)point1->val, *(int8_t*)point2->val, point1->key, point2->key, point->key);
      break;
    };
    default: {
      // TODO: Deal with interpolation with bool and strings and timestamp
      return -1;
    }
  }

  return 0;
}

int taosDoLinearInterpolationD(int32_t type, SPoint* point1, SPoint* point2, SPoint* point) {
  switch (type) {
    case TSDB_DATA_TYPE_INT: {
      *(double*) point->val = doLinearInterpolationImpl(*(int32_t*)point1->val, *(int32_t*)point2->val, point1->key,
                                                        point2->key, point->key);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      *(double*)point->val =
          doLinearInterpolationImpl(*(float*)point1->val, *(float*)point2->val, point1->key, point2->key, point->key);
      break;
    };
    case TSDB_DATA_TYPE_DOUBLE: {
      *(double*)point->val =
          doLinearInterpolationImpl(*(double*)point1->val, *(double*)point2->val, point1->key, point2->key, point->key);
      break;
    };
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT: {
      *(double*)point->val = doLinearInterpolationImpl(*(int64_t*)point1->val, *(int64_t*)point2->val, point1->key,
                                                        point2->key, point->key);
      break;
    };
    case TSDB_DATA_TYPE_SMALLINT: {
      *(double*)point->val = doLinearInterpolationImpl(*(int16_t*)point1->val, *(int16_t*)point2->val, point1->key,
                                                        point2->key, point->key);
      break;
    };
    case TSDB_DATA_TYPE_TINYINT: {
      *(double*)point->val =
          doLinearInterpolationImpl(*(int8_t*)point1->val, *(int8_t*)point2->val, point1->key, point2->key, point->key);
      break;
    };
    default: {
      // TODO: Deal with interpolation with bool and strings and timestamp
      return -1;
    }
  }
  
  return 0;
}


static char* getPos(char* data, int32_t bytes, int32_t index) { return data + index * bytes; }

static void setTagsValueInInterpolation(tFilePage** data, char** pTags, SColumnModel* pModel, int32_t order,
                                        int32_t start, int32_t capacity, int32_t num) {
  for (int32_t j = 0, i = start; i < pModel->numOfCols; ++i, ++j) {
    SSchema* pSchema = getColumnModelSchema(pModel, i);

    char* val1 = getPos(data[i]->data, pSchema->bytes, num);
    assignVal(val1, pTags[j], pSchema->bytes, pSchema->type);
  }
}

static void doInterpoResultImpl(SInterpolationInfo* pInterpoInfo, int16_t interpoType, tFilePage** data,
                                SColumnModel* pModel, int32_t* num, char** srcData, int64_t nInterval,
                                int64_t* defaultVal, int64_t currentTimestamp, int32_t capacity, int32_t numOfTags,
                                char** pTags, bool outOfBound) {
  char** prevValues = &pInterpoInfo->prevValues;
  char** nextValues = &pInterpoInfo->nextValues;

  SPoint point1, point2, point;

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pInterpoInfo->order);

  char* val = getPos(data[0]->data, TSDB_KEYSIZE, *num);
  *(TSKEY*)val = pInterpoInfo->startTimestamp;

  int32_t numOfValCols = pModel->numOfCols - numOfTags;

  // set the other values
  if (interpoType == TSDB_INTERPO_PREV) {
    char* pInterpolationData = INTERPOL_IS_ASC_INTERPOL(pInterpoInfo) ? *prevValues : *nextValues;
    if (pInterpolationData != NULL) {
      for (int32_t i = 1; i < numOfValCols; ++i) {
        SSchema* pSchema = getColumnModelSchema(pModel, i);
        int16_t  offset = getColumnModelOffset(pModel, i);

        char* val1 = getPos(data[i]->data, pSchema->bytes, *num);

        if (isNull(pInterpolationData + offset, pSchema->type)) {
          setNull(val1, pSchema->type, pSchema->bytes);
        } else {
          assignVal(val1, pInterpolationData + offset, pSchema->bytes, pSchema->type);
        }
      }
    } else { /* no prev value yet, set the value for null */
      for (int32_t i = 1; i < numOfValCols; ++i) {
        SSchema* pSchema = getColumnModelSchema(pModel, i);

        char* val1 = getPos(data[i]->data, pSchema->bytes, *num);
        setNull(val1, pSchema->type, pSchema->bytes);
      }
    }

    setTagsValueInInterpolation(data, pTags, pModel, pInterpoInfo->order, numOfValCols, capacity, *num);
  } else if (interpoType == TSDB_INTERPO_LINEAR) {
    // TODO : linear interpolation supports NULL value
    if (*prevValues != NULL && !outOfBound) {
      for (int32_t i = 1; i < numOfValCols; ++i) {
        SSchema* pSchema = getColumnModelSchema(pModel, i);
        int16_t  offset = getColumnModelOffset(pModel, i);

        int16_t type = pSchema->type;
        char*   val1 = getPos(data[i]->data, pSchema->bytes, *num);

        if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR || type == TSDB_DATA_TYPE_BOOL) {
          setNull(val1, type, pSchema->bytes);
          continue;
        }

        point1 = (SPoint){.key = *(TSKEY*)(*prevValues), .val = *prevValues + offset};
        point2 = (SPoint){.key = currentTimestamp, .val = srcData[i] + pInterpoInfo->rowIdx * pSchema->bytes};
        point = (SPoint){.key = pInterpoInfo->startTimestamp, .val = val1};
        taosDoLinearInterpolation(type, &point1, &point2, &point);
      }

      setTagsValueInInterpolation(data, pTags, pModel, pInterpoInfo->order, numOfValCols, capacity, *num);

    } else {
      for (int32_t i = 1; i < numOfValCols; ++i) {
        SSchema* pSchema = getColumnModelSchema(pModel, i);

        char* val1 = getPos(data[i]->data, pSchema->bytes, *num);
        setNull(val1, pSchema->type, pSchema->bytes);
      }

      setTagsValueInInterpolation(data, pTags, pModel, pInterpoInfo->order, numOfValCols, capacity, *num);
    }
  } else { /* default value interpolation */
    for (int32_t i = 1; i < numOfValCols; ++i) {
      SSchema* pSchema = getColumnModelSchema(pModel, i);

      char* val1 = getPos(data[i]->data, pSchema->bytes, *num);
      assignVal(val1, (char*)&defaultVal[i], pSchema->bytes, pSchema->type);
    }

    setTagsValueInInterpolation(data, pTags, pModel, pInterpoInfo->order, numOfValCols, capacity, *num);
  }

  pInterpoInfo->startTimestamp += (nInterval * step);
  pInterpoInfo->numOfCurrentInterpo++;

  (*num) += 1;
}

static void initBeforeAfterDataBuf(SColumnModel* pModel, char** nextValues) {
  if (*nextValues != NULL) {
    return;
  }
  
  *nextValues = calloc(1, pModel->rowSize);
  for (int i = 1; i < pModel->numOfCols; i++) {
    int16_t  offset = getColumnModelOffset(pModel, i);
    SSchema* pSchema = getColumnModelSchema(pModel, i);
    
    setNull(*nextValues + offset, pSchema->type, pSchema->bytes);
  }
}

int32_t taosDoInterpoResult(SInterpolationInfo* pInterpoInfo, int16_t interpoType, tFilePage** data,
                            int32_t numOfRawDataInRows, int32_t outputRows, int64_t nInterval,
                            const int64_t* pPrimaryKeyArray, SColumnModel* pModel, char** srcData, int64_t* defaultVal,
                            const int32_t* functionIDs, int32_t bufSize) {
  int32_t num = 0;
  pInterpoInfo->numOfCurrentInterpo = 0;

  char** prevValues = &pInterpoInfo->prevValues;
  char** nextValues = &pInterpoInfo->nextValues;

  int32_t numOfTags = pInterpoInfo->numOfTags;
  char**  pTags = pInterpoInfo->pTags;

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pInterpoInfo->order);

  if (numOfRawDataInRows == 0) {
    /*
     * we need to rebuild whole data
     * NOTE:we need to keep the last saved data, to satisfy the interpolation
     */
    while (num < outputRows) {
      doInterpoResultImpl(pInterpoInfo, interpoType, data, pModel, &num, srcData, nInterval, defaultVal,
                          pInterpoInfo->startTimestamp, bufSize, numOfTags, pTags, true);
    }
    pInterpoInfo->numOfTotalInterpo += pInterpoInfo->numOfCurrentInterpo;
    return outputRows;

  } else {
    while (1) {
      int64_t currentTimestamp = pPrimaryKeyArray[pInterpoInfo->rowIdx];

      if ((pInterpoInfo->startTimestamp < currentTimestamp && INTERPOL_IS_ASC_INTERPOL(pInterpoInfo)) ||
          (pInterpoInfo->startTimestamp > currentTimestamp && !INTERPOL_IS_ASC_INTERPOL(pInterpoInfo))) {
        /* set the next value for interpolation */
        initBeforeAfterDataBuf(pModel, nextValues);
        
        int32_t offset = pInterpoInfo->rowIdx;
        for (int32_t tlen = 0, i = 0; i < pModel->numOfCols - numOfTags; ++i) {
          SSchema* pSchema = getColumnModelSchema(pModel, i);

          memcpy(*nextValues + tlen, srcData[i] + offset * pSchema->bytes, pSchema->bytes);
          tlen += pSchema->bytes;
        }
      }

      if (((pInterpoInfo->startTimestamp < currentTimestamp && INTERPOL_IS_ASC_INTERPOL(pInterpoInfo)) ||
           (pInterpoInfo->startTimestamp > currentTimestamp && !INTERPOL_IS_ASC_INTERPOL(pInterpoInfo))) &&
          num < outputRows) {
        while (((pInterpoInfo->startTimestamp < currentTimestamp && INTERPOL_IS_ASC_INTERPOL(pInterpoInfo)) ||
                (pInterpoInfo->startTimestamp > currentTimestamp && !INTERPOL_IS_ASC_INTERPOL(pInterpoInfo))) &&
               num < outputRows) {
          doInterpoResultImpl(pInterpoInfo, interpoType, data, pModel, &num, srcData, nInterval, defaultVal,
                              currentTimestamp, bufSize, numOfTags, pTags, false);
        }

        /* output buffer is full, abort */
        if ((num == outputRows && INTERPOL_IS_ASC_INTERPOL(pInterpoInfo)) ||
            (num < 0 && !INTERPOL_IS_ASC_INTERPOL(pInterpoInfo))) {
          pInterpoInfo->numOfTotalInterpo += pInterpoInfo->numOfCurrentInterpo;
          return outputRows;
        }
      } else {
        assert(pInterpoInfo->startTimestamp == currentTimestamp);
        
        initBeforeAfterDataBuf(pModel, prevValues);
        
        // assign rows to dst buffer
        int32_t i = 0;
        for (int32_t tlen = 0; i < pModel->numOfCols - numOfTags; ++i) {
          int16_t  offset = getColumnModelOffset(pModel, i);
          SSchema* pSchema = getColumnModelSchema(pModel, i);

          char* val1 = getPos(data[i]->data, pSchema->bytes, num);
          char* src = srcData[i] + pInterpoInfo->rowIdx * pSchema->bytes;
          
          if (i == 0 ||
              (functionIDs[i] != TSDB_FUNC_COUNT && !isNull(src, pSchema->type)) ||
              (functionIDs[i] == TSDB_FUNC_COUNT && *(int64_t*)(src) != 0)) {
            assignVal(val1, src, pSchema->bytes, pSchema->type);
            memcpy(*prevValues + tlen, src, pSchema->bytes);
          } else {  // i > 0 and data is null , do interpolation
            if (interpoType == TSDB_INTERPO_PREV) {
              assignVal(val1, *prevValues + offset, pSchema->bytes, pSchema->type);
            } else if (interpoType == TSDB_INTERPO_LINEAR) {
              assignVal(val1, src, pSchema->bytes, pSchema->type);
              memcpy(*prevValues + tlen, src, pSchema->bytes);
            } else {
              assignVal(val1, (char*)&defaultVal[i], pSchema->bytes, pSchema->type);
            }
          }
          tlen += pSchema->bytes;
        }

        /* set the tag value for final result */
        setTagsValueInInterpolation(data, pTags, pModel, pInterpoInfo->order, pModel->numOfCols - numOfTags, bufSize,
                                    num);

        pInterpoInfo->startTimestamp += (nInterval * step);
        pInterpoInfo->rowIdx += 1;
        num += 1;
      }

      if ((pInterpoInfo->rowIdx >= pInterpoInfo->numOfRawDataInRows && INTERPOL_IS_ASC_INTERPOL(pInterpoInfo)) ||
          (pInterpoInfo->rowIdx < 0 && !INTERPOL_IS_ASC_INTERPOL(pInterpoInfo)) || num >= outputRows) {
        if (pInterpoInfo->rowIdx >= pInterpoInfo->numOfRawDataInRows || pInterpoInfo->rowIdx < 0) {
          pInterpoInfo->rowIdx = -1;
          pInterpoInfo->numOfRawDataInRows = 0;

          /* the raw data block is exhausted, next value does not exists */
          tfree(*nextValues);
        }

        pInterpoInfo->numOfTotalInterpo += pInterpoInfo->numOfCurrentInterpo;
        return num;
      }
    }
  }
}
