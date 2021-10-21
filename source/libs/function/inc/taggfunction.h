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

#ifndef TDENGINE_TAGGFUNCTION_H
#define TDENGINE_TAGGFUNCTION_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"

#include "tname.h"
#include "taosdef.h"
#include "tvariant.h"
#include "function.h"
#include "tudf.h"

extern SAggFunctionInfo aggFunc[34];

typedef struct SResultRowCellInfo {
  int8_t   hasResult;       // result generated, not NULL value
  bool     initialized;     // output buffer has been initialized
  bool     complete;        // query has completed
  uint32_t numOfRes;        // num of output result in current buffer
} SResultRowCellInfo;

#define FUNCSTATE_SO           0x0u
#define FUNCSTATE_MO           0x1u    // dynamic number of output, not multinumber of output e.g., TOP/BOTTOM
#define FUNCSTATE_STREAM       0x2u    // function avail for stream
#define FUNCSTATE_STABLE       0x4u    // function avail for super table
#define FUNCSTATE_NEED_TS      0x8u    // timestamp is required during query processing
#define FUNCSTATE_SELECTIVITY  0x10u   // selectivity functions, can exists along with tag columns

#define BASIC_FUNC_SO FUNCSTATE_SO | FUNCSTATE_STREAM | FUNCSTATE_STABLE
#define BASIC_FUNC_MO FUNCSTATE_MO | FUNCSTATE_STREAM | FUNCSTATE_STABLE

#define AVG_FUNCTION_INTER_BUFFER_SIZE 50

#define DATA_SET_FLAG ','  // to denote the output area has data, not null value
#define DATA_SET_FLAG_SIZE sizeof(DATA_SET_FLAG)

#define QUERY_ASC_FORWARD_STEP   1
#define QUERY_DESC_FORWARD_STEP -1

#define GET_FORWARD_DIRECTION_FACTOR(ord) (((ord) == TSDB_ORDER_ASC) ? QUERY_ASC_FORWARD_STEP : QUERY_DESC_FORWARD_STEP)

#define MAX_INTERVAL_TIME_WINDOW 1000000  // maximum allowed time windows in final results
#define TOP_BOTTOM_QUERY_LIMIT   100

enum {
  MASTER_SCAN   = 0x0u,
  REVERSE_SCAN  = 0x1u,
  REPEAT_SCAN   = 0x2u,  //repeat scan belongs to the master scan
  MERGE_STAGE   = 0x20u,
};

#define QUERY_IS_STABLE_QUERY(type)      (((type)&TSDB_QUERY_TYPE_STABLE_QUERY) != 0)
#define QUERY_IS_JOIN_QUERY(type)        (TSDB_QUERY_HAS_TYPE(type, TSDB_QUERY_TYPE_JOIN_QUERY))
#define QUERY_IS_PROJECTION_QUERY(type)  (((type)&TSDB_QUERY_TYPE_PROJECTION_QUERY) != 0)
#define QUERY_IS_FREE_RESOURCE(type)     (((type)&TSDB_QUERY_TYPE_FREE_RESOURCE) != 0)

typedef struct SArithmeticSupport {
  SExprInfo   *pExprInfo;
  int32_t      numOfCols;
  SColumnInfo *colList;
  void        *exprList;   // client side used
  int32_t      offset;
  char**       data;
} SArithmeticSupport;

typedef struct SInterpInfoDetail {
  TSKEY  ts;  // interp specified timestamp
  int8_t type;
  int8_t primaryCol;
} SInterpInfoDetail;

#define GET_ROWCELL_INTERBUF(_c) ((void*) ((char*)(_c) + sizeof(SResultRowCellInfo)))

#define GET_RES_INFO(ctx) ((ctx)->resultInfo)

#define IS_STREAM_QUERY_VALID(x)  (((x)&TSDB_FUNCSTATE_STREAM) != 0)
#define IS_MULTIOUTPUT(x)         (((x)&TSDB_FUNCSTATE_MO) != 0)

// determine the real data need to calculated the result
enum {
  BLK_DATA_NO_NEEDED     = 0x0,
  BLK_DATA_STATIS_NEEDED = 0x1,
  BLK_DATA_ALL_NEEDED    = 0x3,
  BLK_DATA_DISCARD       = 0x4,   // discard current data block since it is not qualified for filter
};

typedef struct STwaInfo {
  int8_t      hasResult;  // flag to denote has value
  double      dOutput;
  SPoint1     p;
  STimeWindow win;
} STwaInfo;

extern int32_t functionCompatList[]; // compatible check array list

bool topbot_datablock_filter(SQLFunctionCtx *pCtx, const char *minval, const char *maxval);

/**
 * the numOfRes should be kept, since it may be used later
 * and allow the ResultInfo to be re initialized
 */
#define RESET_RESULT_INFO(_r)  \
  do {                         \
    (_r)->initialized = false; \
  } while (0)

static FORCE_INLINE void initResultInfo(SResultRowCellInfo *pResInfo, int32_t bufLen) {
  pResInfo->initialized = true;  // the this struct has been initialized flag
  
  pResInfo->complete  = false;
  pResInfo->hasResult = false;
  pResInfo->numOfRes  = 0;
  
  memset(GET_ROWCELL_INTERBUF(pResInfo), 0, bufLen);
}

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TAGGFUNCTION_H
