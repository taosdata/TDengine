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

#ifndef TDENGINE_COMMON_H
#define TDENGINE_COMMON_H

#include "taosdef.h"
#include "taosmsg.h"
#include "tarray.h"

//typedef struct STimeWindow {
//  TSKEY skey;
//  TSKEY ekey;
//} STimeWindow;

//typedef struct {
//  int32_t dataLen;
//  char    name[TSDB_TABLE_FNAME_LEN];
//  char   *data;
//} STagData;

//typedef struct SSchema {
//  uint8_t type;
//  char    name[TSDB_COL_NAME_LEN];
//  int16_t colId;
//  int16_t bytes;
//} SSchema;

typedef struct SColumnDataAgg {
  int16_t colId;
  int64_t sum;
  int64_t max;
  int64_t min;
  int16_t maxIndex;
  int16_t minIndex;
  int16_t numOfNull;
} SColumnDataAgg;

typedef struct SDataBlockInfo {
  STimeWindow window;
  int32_t     rows;
  int32_t     numOfCols;
  int64_t     uid;
} SDataBlockInfo;

typedef struct SSDataBlock {
  SColumnDataAgg *pBlockAgg;
  SArray         *pDataBlock;   // SArray<SColumnInfoData>
  SDataBlockInfo info;
} SSDataBlock;

typedef struct SColumnInfoData {
  SColumnInfo info;     // TODO filter info needs to be removed
  char       *pData;    // the corresponding block data in memory
} SColumnInfoData;

#endif  // TDENGINE_COMMON_H
