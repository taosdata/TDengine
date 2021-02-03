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

#ifndef TDENGINE_QAGGMAIN_H
#define TDENGINE_QAGGMAIN_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"

#include "tname.h"
#include "taosdef.h"
#include "trpc.h"
#include "tvariant.h"

#define TSDB_FUNC_INVALID_ID  -1
#define TSDB_FUNC_COUNT        0
#define TSDB_FUNC_SUM          1
#define TSDB_FUNC_AVG          2
#define TSDB_FUNC_MIN          3
#define TSDB_FUNC_MAX          4
#define TSDB_FUNC_STDDEV       5
#define TSDB_FUNC_PERCT        6
#define TSDB_FUNC_APERCT       7
#define TSDB_FUNC_FIRST        8
#define TSDB_FUNC_LAST         9
#define TSDB_FUNC_LAST_ROW     10
#define TSDB_FUNC_TOP          11
#define TSDB_FUNC_BOTTOM       12
#define TSDB_FUNC_SPREAD       13
#define TSDB_FUNC_TWA          14
#define TSDB_FUNC_LEASTSQR     15

#define TSDB_FUNC_TS           16
#define TSDB_FUNC_TS_DUMMY     17
#define TSDB_FUNC_TAG_DUMMY    18
#define TSDB_FUNC_TS_COMP      19

#define TSDB_FUNC_TAG          20
#define TSDB_FUNC_PRJ          21

#define TSDB_FUNC_TAGPRJ       22
#define TSDB_FUNC_ARITHM       23
#define TSDB_FUNC_DIFF         24

#define TSDB_FUNC_FIRST_DST    25
#define TSDB_FUNC_LAST_DST     26
#define TSDB_FUNC_STDDEV_DST   27
#define TSDB_FUNC_INTERP       28

#define TSDB_FUNC_RATE         29
#define TSDB_FUNC_IRATE        30
#define TSDB_FUNC_SUM_RATE     31
#define TSDB_FUNC_SUM_IRATE    32
#define TSDB_FUNC_AVG_RATE     33
#define TSDB_FUNC_AVG_IRATE    34

#define TSDB_FUNC_TID_TAG      35
#define TSDB_FUNC_HISTOGRAM    36
#define TSDB_FUNC_HLL          37
#define TSDB_FUNC_MODE         38
#define TSDB_FUNC_SAMPLE       39
#define TSDB_FUNC_CEIL         40
#define TSDB_FUNC_FLOOR        41
#define TSDB_FUNC_ROUND        42
#define TSDB_FUNC_MAVG         43
#define TSDB_FUNC_CSUM         44


#define TSDB_FUNCSTATE_SO           0x1u    // single output
#define TSDB_FUNCSTATE_MO           0x2u    // dynamic number of output, not multinumber of output e.g., TOP/BOTTOM
#define TSDB_FUNCSTATE_STREAM       0x4u    // function avail for stream
#define TSDB_FUNCSTATE_STABLE       0x8u    // function avail for metric
#define TSDB_FUNCSTATE_OF           0x10u   // outer forward
#define TSDB_FUNCSTATE_NEED_TS      0x20u   // timestamp is required during query processing
#define TSDB_FUNCSTATE_SELECTIVITY  0x40u   // selectivity functions, can exists along with tag columns

#define TSDB_BASE_FUNC_SO TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_STREAM | TSDB_FUNCSTATE_STABLE | TSDB_FUNCSTATE_OF
#define TSDB_BASE_FUNC_MO TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_STREAM | TSDB_FUNCSTATE_STABLE | TSDB_FUNCSTATE_OF

#define TSDB_FUNCTIONS_NAME_MAX_LENGTH 16
#define TSDB_AVG_FUNCTION_INTER_BUFFER_SIZE 50

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
  SExprInfo   *pArithExpr;
  int32_t      numOfCols;
  SColumnInfo *colList;
  void        *exprList;   // client side used
  int32_t      offset;
  char**       data;
} SArithmeticSupport;

typedef struct SQLPreAggVal {
  bool        isSet;             // statistics info set or not
  bool        dataBlockLoaded;   // data block is loaded or not
  SDataStatis statis;
} SQLPreAggVal;

typedef struct SInterpInfoDetail {
  TSKEY  ts;  // interp specified timestamp
  int8_t type;
  int8_t primaryCol;
} SInterpInfoDetail;

typedef struct SResultRowCellInfo {
  int8_t   hasResult;       // result generated, not NULL value
  bool     initialized;     // output buffer has been initialized
  bool     complete;        // query has completed
  uint32_t numOfRes;        // num of output result in current buffer
} SResultRowCellInfo;

typedef struct SPoint1 {
  int64_t key;
  union{double  val; char* ptr;};
} SPoint1;

#define GET_ROWCELL_INTERBUF(_c) ((void*) ((char*)(_c) + sizeof(SResultRowCellInfo)))

struct SQLFunctionCtx;

/**
 * for selectivity query, the corresponding tag value is assigned if the data is qualified
 */
typedef struct SExtTagsInfo {
  int16_t                 tagsLen;      // keep the tags data for top/bottom query result
  int16_t                 numOfTagCols;
  struct SQLFunctionCtx **pTagCtxList;
} SExtTagsInfo;

// sql function runtime context
typedef struct SQLFunctionCtx {
  int32_t      startOffset;  // todo remove it
  int32_t      size;      // number of rows
  void *       pInput;    //
  uint32_t     order;     // asc|desc
  int16_t      inputType;
  int16_t      inputBytes;
  
  int16_t      outputType;
  int16_t      outputBytes;   // size of results, determined by function and input column data type
  int32_t      interBufBytes; // internal buffer size
  bool         hasNull;       // null value exist in current block
  bool         requireNull;   // require null in some function
  bool         stableQuery;
  int16_t      functionId;    // function id
  char *       pOutput;       // final result output buffer, point to sdata->data
  uint8_t      currentStage;  // record current running step, default: 0
  int64_t      startTs;       // timestamp range of current query when function is executed on a specific data block
  int32_t      numOfParams;
  tVariant     param[4];      // input parameter, e.g., top(k, 20), the number of results for top query is kept in param */
  int64_t     *ptsList;       // corresponding timestamp array list
  void        *ptsOutputBuf;  // corresponding output buffer for timestamp of each result, e.g., top/bottom*/
  SQLPreAggVal preAggVals;
  tVariant     tag;

  SResultRowCellInfo *resultInfo;

  SExtTagsInfo tagInfo;
  SPoint1      start;
  SPoint1      end;
} SQLFunctionCtx;

typedef struct SAggFunctionInfo {
  char     name[TSDB_FUNCTIONS_NAME_MAX_LENGTH];
  uint8_t  index;       // index of function in aAggs
  int8_t   stableFuncId;  // transfer function for super table query
  uint16_t status;

  bool (*init)(SQLFunctionCtx *pCtx);  // setup the execute environment

  void (*xFunction)(SQLFunctionCtx *pCtx);                     // blocks version function
  void (*xFunctionF)(SQLFunctionCtx *pCtx, int32_t position);  // single-row function version, todo merge with blockwise function

  // some sql function require scan data twice or more, e.g.,stddev, percentile
  void (*xNextStep)(SQLFunctionCtx *pCtx);

  // finalizer must be called after all xFunction has been executed to generated final result.
  void (*xFinalize)(SQLFunctionCtx *pCtx);
  void (*mergeFunc)(SQLFunctionCtx *pCtx);

  int32_t (*dataReqFunc)(SQLFunctionCtx *pCtx, TSKEY start, TSKEY end, int32_t colId);
} SAggFunctionInfo;

#define GET_RES_INFO(ctx) ((ctx)->resultInfo)

int32_t getResultDataInfo(int32_t dataType, int32_t dataBytes, int32_t functionId, int32_t param, int16_t *type,
                          int16_t *len, int32_t *interBytes, int16_t extLength, bool isSuperTable);

#define IS_STREAM_QUERY_VALID(x)  (((x)&TSDB_FUNCSTATE_STREAM) != 0)
#define IS_MULTIOUTPUT(x)         (((x)&TSDB_FUNCSTATE_MO) != 0)
#define IS_SINGLEOUTPUT(x)        (((x)&TSDB_FUNCSTATE_SO) != 0)
#define IS_OUTER_FORWARD(x)       (((x)&TSDB_FUNCSTATE_OF) != 0)

/* determine the real data need to calculated the result */
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

/* global sql function array */
extern struct SAggFunctionInfo aAggs[];

extern int32_t functionCompatList[]; // compatible check array list

bool topbot_datablock_filter(SQLFunctionCtx *pCtx, int32_t functionId, const char *minval, const char *maxval);

/**
 * the numOfRes should be kept, since it may be used later
 * and allow the ResultInfo to be re initialized
 */
#define RESET_RESULT_INFO(_r)  \
  do {                         \
    (_r)->initialized = false; \
  } while (0)

static FORCE_INLINE void initResultInfo(SResultRowCellInfo *pResInfo, uint32_t bufLen) {
  pResInfo->initialized = true;  // the this struct has been initialized flag
  
  pResInfo->complete = false;
  pResInfo->hasResult = false;
  pResInfo->numOfRes = 0;
  
  memset(GET_ROWCELL_INTERBUF(pResInfo), 0, (size_t)bufLen);
}

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_QAGGMAIN_H
