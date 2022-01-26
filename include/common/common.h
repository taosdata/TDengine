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
#include "tmsg.h"
#include "tarray.h"
#include "tvariant.h"
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

typedef struct {
  uint32_t  numOfTables;
  SArray   *pGroupList;
  SHashObj *map;  // speedup acquire the tableQueryInfo by table uid
} STableGroupInfo;

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

typedef struct SConstantItem {
  SColumnInfo info;
  int32_t     startRow;    // run-length-encoding to save the space for multiple rows
  int32_t     endRow;
  SVariant    value;
} SConstantItem;

// info.numOfCols = taosArrayGetSize(pDataBlock) + taosArrayGetSize(pConstantList);
typedef struct SSDataBlock {
  SColumnDataAgg *pBlockAgg;
  SArray         *pDataBlock;    // SArray<SColumnInfoData>
  SArray         *pConstantList; // SArray<SConstantItem>, it is a constant/tags value of the corresponding result value.
  SDataBlockInfo  info;
} SSDataBlock;

// pBlockAgg->numOfNull == info.rows, all data are null
// pBlockAgg->numOfNull == 0, no data are null.
typedef struct SColumnInfoData {
  SColumnInfo info;      // TODO filter info needs to be removed
  char       *nullbitmap;//
  char       *pData;     // the corresponding block data in memory
} SColumnInfoData;

//======================================================================================================================
// the following structure shared by parser and executor
typedef struct SColumn {
  uint64_t     uid;
  char         name[TSDB_COL_NAME_LEN];
  int8_t       flag;      // column type: normal column, tag, or user-input column (integer/float/string)
  SColumnInfo  info;
} SColumn;

typedef struct SLimit {
  int64_t   limit;
  int64_t   offset;
} SLimit;

typedef struct SOrder {
  uint32_t  order;
  SColumn   col;
} SOrder;

typedef struct SGroupbyExpr {
  SArray*  columnInfo;  // SArray<SColIndex>, group by columns information
  bool     groupbyTag;  // group by tag or column
} SGroupbyExpr;

// the structure for sql function in select clause
typedef struct SSqlExpr {
  char      token[TSDB_COL_NAME_LEN];      // original token
  SSchema   resSchema;

  int32_t   numOfCols;
  SColumn*  pColumns;       // data columns that are required by query
  int32_t   interBytes;     // inter result buffer size
  int16_t   numOfParams;    // argument value of each function
  SVariant  param[3];       // parameters are not more than 3
} SSqlExpr;

typedef struct SExprInfo {
  struct SSqlExpr    base;
  struct tExprNode  *pExpr;
} SExprInfo;

typedef struct SStateWindow {
  SColumn col;
} SStateWindow;

typedef struct SSessionWindow {
  int64_t gap;             // gap between two session window(in microseconds)
  SColumn col;
} SSessionWindow;

#define QUERY_ASC_FORWARD_STEP   1
#define QUERY_DESC_FORWARD_STEP -1

#define GET_FORWARD_DIRECTION_FACTOR(ord) (((ord) == TSDB_ORDER_ASC) ? QUERY_ASC_FORWARD_STEP : QUERY_DESC_FORWARD_STEP)

#endif  // TDENGINE_COMMON_H
