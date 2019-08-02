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

#include <assert.h>
#include <stdint.h>

#include "taosmsg.h"
#include "textbuffer.h"
#include "tinterpolation.h"
#include "tsqlfunction.h"
#include "ttypes.h"

#define INTERPOL_IS_ASC_INTERPOL(interp) ((interp)->order == TSQL_SO_ASC)

int64_t taosGetIntervalStartTimestamp(int64_t startTime, int64_t timeRange, char intervalTimeUnit) {
  if (timeRange == 0) {
    return startTime;
  }

  if (intervalTimeUnit == 'a' || intervalTimeUnit == 'm' || intervalTimeUnit == 's' || intervalTimeUnit == 'h') {
    return (startTime / timeRange) * timeRange;
  } else {
    /*
     * here we revised the start time of day according to the local time zone,
     * but in case of DST, the start time of one day need to be dynamically decided.
     *
     * TODO dynmaically decide the start time of a day
     */
    int64_t revStartime = (startTime / timeRange) * timeRange + timezone * MILLISECOND_PER_SECOND;
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

void taosInterpoSetStartInfo(SInterpolationInfo* pInterpoInfo, int32_t numOfRawDataInRows, int32_t type) {
  if (type == TSDB_INTERPO_NONE) {
    return;
  }

  pInterpoInfo->rowIdx = INTERPOL_IS_ASC_INTERPOL(pInterpoInfo) ? 0 : numOfRawDataInRows - 1;
  pInterpoInfo->numOfRawDataInRows = numOfRawDataInRows;
}

TSKEY taosGetRevisedEndKey(TSKEY ekey, int32_t order, int32_t timeInterval, int8_t intervalTimeUnit) {
  if (order == TSQL_SO_ASC) {
    return ekey;
  } else {
    return taosGetIntervalStartTimestamp(ekey, timeInterval, intervalTimeUnit);
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

    if (pInterpoInfo->order == TSQL_SO_ASC) {
      // get last timestamp, calculate the result size
      int64_t lastKey = pPrimaryKeyArray[pInterpoInfo->numOfRawDataInRows - 1];
      finalNumOfResult = (int32_t)((lastKey - pInterpoInfo->startTimestamp) / nInterval) + 1;
    } else {  // todo error less than one!!!
      TSKEY lastKey = pPrimaryKeyArray[0];
      finalNumOfResult = (int32_t)((pInterpoInfo->startTimestamp - lastKey) / nInterval) + 1;
    }

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

bool taosHasRemainsDataForInterpolation(SInterpolationInfo* pInterpoInfo) { return taosNumOfRemainPoints(pInterpoInfo) > 0; }

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

static char* getPos(char* data, int32_t bytes, int32_t order, int32_t capacity, int32_t index) {
  if (order == TSQL_SO_ASC) {
    return data + index * bytes;
  } else {
    return data + (capacity - index - 1) * bytes;
  }
}

static void setTagsValueInInterpolation(tFilePage** data, char** pTags, tColModel* pModel, int32_t order, int32_t start,
                                        int32_t capacity, int32_t num) {
  for (int32_t j = 0, i = start; i < pModel->numOfCols; ++i, ++j) {
    char* val1 = getPos(data[i]->data, pModel->pFields[i].bytes, order, capacity, num);
    assignVal(val1, pTags[j], pModel->pFields[i].bytes, pModel->pFields[i].type);
  }
}

static void doInterpoResultImpl(SInterpolationInfo* pInterpoInfo, int16_t interpoType, tFilePage** data,
                                tColModel* pModel, int32_t* num, char** srcData, int64_t nInterval, int64_t* defaultVal,
                                int64_t currentTimestamp, int32_t capacity, int32_t numOfTags, char** pTags,
                                bool outOfBound) {
  char** prevValues = &pInterpoInfo->prevValues;
  char** nextValues = &pInterpoInfo->nextValues;

  SPoint point1, point2, point;

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pInterpoInfo->order);

  char* val = getPos(data[0]->data, TSDB_KEYSIZE, pInterpoInfo->order, capacity, *num);
  *(TSKEY*)val = pInterpoInfo->startTimestamp;

  int32_t numOfValCols = pModel->numOfCols - numOfTags;

  // set the other values
  if (interpoType == TSDB_INTERPO_PREV) {
    char* pInterpolationData = INTERPOL_IS_ASC_INTERPOL(pInterpoInfo) ? *prevValues : *nextValues;
    if (pInterpolationData != NULL) {
      for (int32_t i = 1; i < numOfValCols; ++i) {
        char* val1 = getPos(data[i]->data, pModel->pFields[i].bytes, pInterpoInfo->order, capacity, *num);

        if (isNull(pInterpolationData + pModel->colOffset[i], pModel->pFields[i].type)) {
          setNull(val1, pModel->pFields[i].type, pModel->pFields[i].bytes);
        } else {
          assignVal(val1, pInterpolationData + pModel->colOffset[i], pModel->pFields[i].bytes, pModel->pFields[i].type);
        }
      }
    } else { /* no prev value yet, set the value for null */
      for (int32_t i = 1; i < numOfValCols; ++i) {
        char* val1 = getPos(data[i]->data, pModel->pFields[i].bytes, pInterpoInfo->order, capacity, *num);
        setNull(val1, pModel->pFields[i].type, pModel->pFields[i].bytes);
      }
    }

    setTagsValueInInterpolation(data, pTags, pModel, pInterpoInfo->order, numOfValCols, capacity, *num);
  } else if (interpoType == TSDB_INTERPO_LINEAR) {
    // TODO : linear interpolation supports NULL value
    if (*prevValues != NULL && !outOfBound) {
      for (int32_t i = 1; i < numOfValCols; ++i) {
        int32_t type = pModel->pFields[i].type;
        char*   val1 = getPos(data[i]->data, pModel->pFields[i].bytes, pInterpoInfo->order, capacity, *num);

        if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR || type == TSDB_DATA_TYPE_BOOL) {
          setNull(val1, pModel->pFields[i].type, pModel->pFields[i].bytes);
          continue;
        }

        point1 = (SPoint){.key = *(TSKEY*)(*prevValues), .val = *prevValues + pModel->colOffset[i]};
        point2 = (SPoint){.key = currentTimestamp, .val = srcData[i] + pInterpoInfo->rowIdx * pModel->pFields[i].bytes};
        point = (SPoint){.key = pInterpoInfo->startTimestamp, .val = val1};
        taosDoLinearInterpolation(pModel->pFields[i].type, &point1, &point2, &point);
      }

      setTagsValueInInterpolation(data, pTags, pModel, pInterpoInfo->order, numOfValCols, capacity, *num);

    } else {
      for (int32_t i = 1; i < numOfValCols; ++i) {
        char* val1 = getPos(data[i]->data, pModel->pFields[i].bytes, pInterpoInfo->order, capacity, *num);
        setNull(val1, pModel->pFields[i].type, pModel->pFields[i].bytes);
      }

      setTagsValueInInterpolation(data, pTags, pModel, pInterpoInfo->order, numOfValCols, capacity, *num);
    }
  } else { /* default value interpolation */
    for (int32_t i = 1; i < numOfValCols; ++i) {
      char* val1 = getPos(data[i]->data, pModel->pFields[i].bytes, pInterpoInfo->order, capacity, *num);
      assignVal(val1, (char*)&defaultVal[i], pModel->pFields[i].bytes, pModel->pFields[i].type);
    }

    setTagsValueInInterpolation(data, pTags, pModel, pInterpoInfo->order, numOfValCols, capacity, *num);
  }

  pInterpoInfo->startTimestamp += (nInterval * step);
  pInterpoInfo->numOfCurrentInterpo++;

  (*num) += 1;
}

int32_t taosDoInterpoResult(SInterpolationInfo* pInterpoInfo, int16_t interpoType, tFilePage** data,
                            int32_t numOfRawDataInRows, int32_t outputRows, int64_t nInterval,
                            int64_t* pPrimaryKeyArray, tColModel* pModel, char** srcData, int64_t* defaultVal,
                            int32_t* functionIDs, int32_t bufSize) {
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
        if (*nextValues == NULL) {
          *nextValues =
              calloc(1, pModel->colOffset[pModel->numOfCols - 1] + pModel->pFields[pModel->numOfCols - 1].bytes);
          for (int i = 1; i < pModel->numOfCols; i++) {
            setNull(*nextValues + pModel->colOffset[i], pModel->pFields[i].type, pModel->pFields[i].bytes);
          }
        }

        int32_t offset = pInterpoInfo->rowIdx;
        for (int32_t tlen = 0, i = 0; i < pModel->numOfCols - numOfTags; ++i) {
          memcpy(*nextValues + tlen, srcData[i] + offset * pModel->pFields[i].bytes, pModel->pFields[i].bytes);
          tlen += pModel->pFields[i].bytes;
        }
      }

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

      if (pInterpoInfo->startTimestamp == currentTimestamp) {
        if (*prevValues == NULL) {
          *prevValues =
              calloc(1, pModel->colOffset[pModel->numOfCols - 1] + pModel->pFields[pModel->numOfCols - 1].bytes);
          for (int i = 1; i < pModel->numOfCols; i++) {
            setNull(*prevValues + pModel->colOffset[i], pModel->pFields[i].type, pModel->pFields[i].bytes);
          }
        }

        // assign rows to dst buffer
        int32_t i = 0;
        for (int32_t tlen = 0; i < pModel->numOfCols - numOfTags; ++i) {
          char* val1 = getPos(data[i]->data, pModel->pFields[i].bytes, pInterpoInfo->order, bufSize, num);

          if (i == 0 ||
              (functionIDs[i] != TSDB_FUNC_COUNT &&
               !isNull(srcData[i] + pInterpoInfo->rowIdx * pModel->pFields[i].bytes, pModel->pFields[i].type)) ||
              (functionIDs[i] == TSDB_FUNC_COUNT &&
               *(int64_t*)(srcData[i] + pInterpoInfo->rowIdx * pModel->pFields[i].bytes) != 0)) {
            assignVal(val1, srcData[i] + pInterpoInfo->rowIdx * pModel->pFields[i].bytes, pModel->pFields[i].bytes,
                      pModel->pFields[i].type);
            memcpy(*prevValues + tlen, srcData[i] + pInterpoInfo->rowIdx * pModel->pFields[i].bytes,
                   pModel->pFields[i].bytes);
          } else {  // i > 0 and isNULL, do interpolation
            if (interpoType == TSDB_INTERPO_PREV) {
              assignVal(val1, *prevValues + pModel->colOffset[i], pModel->pFields[i].bytes, pModel->pFields[i].type);
            } else if (interpoType == TSDB_INTERPO_LINEAR) {
              // TODO:
            } else {
              assignVal(val1, (char*)&defaultVal[i], pModel->pFields[i].bytes, pModel->pFields[i].type);
            }
          }
          tlen += pModel->pFields[i].bytes;
        }

        /* set the tag value for final result */
        setTagsValueInInterpolation(data, pTags, pModel, pInterpoInfo->order, pModel->numOfCols - numOfTags, bufSize,
                                    num);
      }

      pInterpoInfo->startTimestamp += (nInterval * step);
      pInterpoInfo->rowIdx += step;
      num += 1;

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
