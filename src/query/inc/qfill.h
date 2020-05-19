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

#ifndef TDENGINE_QFILL_H
#define TDENGINE_QFILL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "taosdef.h"
#include "qextbuffer.h"

typedef struct {
  STColumn col;             // column info
  int16_t  functionId;      // sql function id
  int16_t  flag;            // column flag: TAG COLUMN|NORMAL COLUMN
  union {int64_t i; double d;} defaultVal;
} SFillColInfo;
  
typedef struct SFillInfo {
  TSKEY   start;                // start timestamp
  TSKEY   endKey;               // endKey for fill
  int32_t order;                // order [TSDB_ORDER_ASC|TSDB_ORDER_DESC]
  int32_t fillType;             // fill type
  int32_t numOfRows;            // number of rows in the input data block
  int32_t rowIdx;               // rowIdx
  int32_t numOfTotal;           // number of filled rows in one round
  int32_t numOfCurrent;         // number of filled rows in current results

  int32_t numOfTags;            // number of tags
  int32_t numOfCols;            // number of columns, including the tags columns
  int32_t rowSize;              // size of each row
  char ** pTags;                // tags value for current interpolation
  
  int64_t slidingTime;         // sliding value to determine the number of result for a given time window
  char *  prevValues;           // previous row of data, to generate the interpolation results
  char *  nextValues;           // next row of data
  SFillColInfo* pFillCol;       // column info for fill operations
  char** pData;                 // original result data block involved in filling data
} SFillInfo;

typedef struct SPoint {
  int64_t key;
  void *  val;
} SPoint;

int64_t taosGetIntervalStartTimestamp(int64_t startTime, int64_t slidingTime, char timeUnit, int16_t precision);

SFillInfo* taosInitFillInfo(int32_t order, TSKEY skey, int32_t numOfTags, int32_t capacity,
                            int32_t numOfCols, int64_t slidingTime, int32_t fillType, SFillColInfo* pFillCol);

void taosResetFillInfo(SFillInfo* pFillInfo, TSKEY startTimestamp);

void taosDestoryFillInfo(SFillInfo *pFillInfo);

void taosFillSetStartInfo(SFillInfo* pFillInfo, int32_t numOfRows, TSKEY endKey);

void taosFillCopyInputDataFromFilePage(SFillInfo* pFillInfo, tFilePage** pInput);

void taosFillCopyInputDataFromOneFilePage(SFillInfo* pFillInfo, tFilePage* pInput);

TSKEY taosGetRevisedEndKey(TSKEY ekey, int32_t order, int64_t timeInterval, int8_t slidingTimeUnit, int8_t precision);

int32_t taosGetNumOfResultWithFill(SFillInfo* pFillInfo, int32_t numOfRows, int64_t ekey, int32_t maxNumOfRows);

int32_t taosNumOfRemainRows(SFillInfo *pFillInfo);

int32_t taosDoInterpoResult(SFillInfo* pFillInfo, tFilePage** data, int32_t numOfRows, int32_t outputRows, char** srcData);
  
int taosDoLinearInterpolation(int32_t type, SPoint *point1, SPoint *point2, SPoint *point);

void taosGenerateDataBlock(SFillInfo* pFillInfo, tFilePage** output, int64_t* outputRows, int32_t capacity);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_QFILL_H
