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

#ifndef TDENGINE_TINTERPOLATION_H
#define TDENGINE_TINTERPOLATION_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SInterpolationInfo {
  int64_t startTimestamp;
  int32_t order;                // order [asc/desc]
  int32_t numOfRawDataInRows;   // number of points in pQuery->sdata
  int32_t rowIdx;               // rowIdx in pQuery->sdata
  int32_t numOfTotalInterpo;    // number of interpolated rows in one round
  int32_t numOfCurrentInterpo;  // number of interpolated rows in current results
  char *  prevValues;           // previous row of data
  char *  nextValues;           // next row of data
  int32_t numOfTags;
  char ** pTags;  // tags value for current interoplation
} SInterpolationInfo;

typedef struct SPoint {
  int64_t key;
  void *  val;
} SPoint;

typedef void (*__interpo_callback_fn_t)(void *param);

int64_t taosGetIntervalStartTimestamp(int64_t startTime, int64_t timeRange, char intervalTimeUnit);

void taosInitInterpoInfo(SInterpolationInfo *pInterpoInfo, int32_t order, int64_t startTimeStamp, int32_t numOfTags,
                         int32_t rowSize);

void taosInterpoSetStartInfo(SInterpolationInfo *pInterpoInfo, int32_t numOfRawDataInRows, int32_t type);

TSKEY taosGetRevisedEndKey(TSKEY ekey, int32_t order, int32_t timeInterval, int8_t intervalTimeUnit);

/**
 *
 * @param pInterpoInfo
 * @param pPrimaryKeyArray
 * @param numOfRows
 * @param nInterval
 * @param ekey
 * @param maxNumOfRows
 * @return
 */
int32_t taosGetNumOfResultWithInterpo(SInterpolationInfo *pInterpoInfo, int64_t *pPrimaryKeyArray, int32_t numOfRows,
                                      int64_t nInterval, int64_t ekey, int32_t maxNumOfRows);

int32_t taosGetNumOfResWithoutLimit(SInterpolationInfo *pInterpoInfo, int64_t *pPrimaryKeyArray,
                                    int32_t numOfRawDataInRows, int64_t nInterval, int64_t ekey);
/**
 *
 * @param pInterpoInfo
 * @return
 */
bool taosHasRemainsDataForInterpolation(SInterpolationInfo *pInterpoInfo);

int32_t taosNumOfRemainPoints(SInterpolationInfo *pInterpoInfo);

/**
 *
 */
int32_t taosDoInterpoResult(SInterpolationInfo *pInterpoInfo, int16_t interpoType, tFilePage **data,
                            int32_t numOfRawDataInRows, int32_t outputRows, int64_t nInterval,
                            int64_t *pPrimaryKeyArray, tColModel *pModel, char **srcData, int64_t *defaultVal,
                            int32_t *functionIDs, int32_t bufSize);

int taosDoLinearInterpolation(int32_t type, SPoint *point1, SPoint *point2, SPoint *point);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TINTERPOLATION_H
