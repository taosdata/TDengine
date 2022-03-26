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

#ifndef TDENGINE_TFILL_H
#define TDENGINE_TFILL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "taosdef.h"

struct SSDataBlock;

typedef struct SFillColInfo {
  STColumn col;             // column info
  int16_t  functionId;      // sql function id
  int16_t  flag;            // column flag: TAG COLUMN|NORMAL COLUMN
  int16_t  tagIndex;        // index of current tag in SFillTagColInfo array list
  union {int64_t i; double d;} fillVal;
} SFillColInfo;

typedef struct {
  SSchema col;
  char*   tagVal;
} SFillTagColInfo;
  
typedef struct SFillInfo {
  TSKEY     start;                // start timestamp
  TSKEY     end;                  // endKey for fill
  TSKEY     currentKey;           // current active timestamp, the value may be changed during the fill procedure.
  int32_t   order;                // order [TSDB_ORDER_ASC|TSDB_ORDER_DESC]
  int32_t   type;                 // fill type
  int32_t   numOfRows;            // number of rows in the input data block
  int32_t   index;                // active row index
  int32_t   numOfTotal;           // number of filled rows in one round
  int32_t   numOfCurrent;         // number of filled rows in current results

  int32_t   numOfTags;            // number of tags
  int32_t   numOfCols;            // number of columns, including the tags columns
  int32_t   rowSize;              // size of each row
  SInterval interval;
  char *    prevValues;           // previous row of data, to generate the interpolation results
  char *    nextValues;           // next row of data
  char**    pData;                // original result data block involved in filling data
  int32_t   alloc;                // data buffer size in rows
  int8_t    precision;            // time resoluation

  SFillColInfo* pFillCol;         // column info for fill operations
  SFillTagColInfo* pTags;         // tags value for filling gap
  const char* id;
} SFillInfo;

int64_t getNumOfResultsAfterFillGap(SFillInfo* pFillInfo, int64_t ekey, int32_t maxNumOfRows);





#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TFILL_H
