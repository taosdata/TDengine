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

#ifndef TDENGINE_TSQLFUNCTION_H
#define TDENGINE_TSQLFUNCTION_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"

#include "../../common/inc/tname.h"
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
#define TSDB_FUNC_INTERP       27

#define TSDB_FUNC_RATE         28
#define TSDB_FUNC_IRATE        29
#define TSDB_FUNC_SUM_RATE     30
#define TSDB_FUNC_SUM_IRATE    31
#define TSDB_FUNC_AVG_RATE     32
#define TSDB_FUNC_AVG_IRATE    33

#define TSDB_FUNC_TID_TAG      34

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

#define QUERY_COND_REL_PREFIX_IN "IN|"
#define QUERY_COND_REL_PREFIX_LIKE "LIKE|"

#define QUERY_COND_REL_PREFIX_IN_LEN 3
#define QUERY_COND_REL_PREFIX_LIKE_LEN 5

#define QUERY_ASC_FORWARD_STEP 1
#define QUERY_DESC_FORWARD_STEP -1

#define GET_FORWARD_DIRECTION_FACTOR(ord) (((ord) == TSDB_ORDER_ASC) ? QUERY_ASC_FORWARD_STEP : QUERY_DESC_FORWARD_STEP)

#define MAX_RETRIEVE_ROWS_IN_INTERVAL_QUERY 10000000
#define TOP_BOTTOM_QUERY_LIMIT 100

enum {
  MASTER_SCAN           = 0x0u,
  REVERSE_SCAN          = 0x1u,
  REPEAT_SCAN           = 0x2u,  //repeat scan belongs to the master scan
  FIRST_STAGE_MERGE     = 0x10u,
  SECONDARY_STAGE_MERGE = 0x20u,
};

#define QUERY_IS_STABLE_QUERY(type)      (((type)&TSDB_QUERY_TYPE_STABLE_QUERY) != 0)
#define QUERY_IS_JOIN_QUERY(type)        (TSDB_QUERY_HAS_TYPE(type, TSDB_QUERY_TYPE_JOIN_QUERY))
#define QUERY_IS_PROJECTION_QUERY(type) (((type)&TSDB_QUERY_TYPE_PROJECTION_QUERY) != 0)
#define QUERY_IS_FREE_RESOURCE(type)     (((type)&TSDB_QUERY_TYPE_FREE_RESOURCE) != 0)

typedef struct SArithmeticSupport {
  SExprInfo   *pArithExpr;
  int32_t      numOfCols;
  SColumnInfo *colList;
  SArray*      exprList;   // client side used
  int32_t      offset;
  char**       data;
} SArithmeticSupport;

typedef struct SQLPreAggVal {
  bool    isSet;
  SDataStatis statis;
} SQLPreAggVal;

typedef struct SInterpInfoDetail {
  TSKEY  ts;  // interp specified timestamp
  int8_t hasResult;
  int8_t type;
  int8_t primaryCol;
} SInterpInfoDetail;

typedef struct SInterpInfo { SInterpInfoDetail *pInterpDetail; } SInterpInfo;

typedef struct SResultInfo {
  int8_t  hasResult;       // result generated, not NULL value
  bool    initialized;     // output buffer has been initialized
  bool    complete;        // query has completed
  bool    superTableQ;     // is super table query
  int32_t numOfRes;        // num of output result in current buffer
  int32_t bufLen;          // buffer size
  void *  interResultBuf;  // output result buffer
} SResultInfo;

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
  int32_t  startOffset;
  int32_t  size;      // number of rows
  uint32_t order;     // asc|desc
  uint32_t scanFlag;  // TODO merge with currentStage

  int16_t inputType;
  int16_t inputBytes;

  int16_t  outputType;
  int16_t  outputBytes;  // size of results, determined by function and input column data type
  bool     hasNull;      // null value exist in current block
  int16_t  functionId;   // function id
  void *   aInputElemBuf;
  char *   aOutputBuf;            // final result output buffer, point to sdata->data
  uint8_t  currentStage;          // record current running step, default: 0
  int64_t  nStartQueryTimestamp;  // timestamp range of current query when function is executed on a specific data block
  int32_t  numOfParams;
  tVariant param[4];      // input parameter, e.g., top(k, 20), the number of results for top query is kept in param */
  int64_t *ptsList;       // corresponding timestamp array list
  void *   ptsOutputBuf;  // corresponding output buffer for timestamp of each result, e.g., top/bottom*/
  SQLPreAggVal preAggVals;
  tVariant     tag;
  SResultInfo *resultInfo;

  SExtTagsInfo tagInfo;
} SQLFunctionCtx;

typedef struct SQLAggFuncElem {
  char aName[TSDB_FUNCTIONS_NAME_MAX_LENGTH];

  uint8_t  nAggIdx;       // index of function in aAggs
  int8_t   stableFuncId;  // transfer function for super table query
  uint16_t nStatus;

  bool (*init)(SQLFunctionCtx *pCtx);  // setup the execute environment

  void (*xFunction)(SQLFunctionCtx *pCtx);                     // blocks version function
  void (*xFunctionF)(SQLFunctionCtx *pCtx, int32_t position);  // single-row function version

  // some sql function require scan data twice or more, e.g.,stddev
  void (*xNextStep)(SQLFunctionCtx *pCtx);

  /*
   * finalizer must be called after all xFunction has been executed to
   * generated final result. Otherwise, the value in aOutputBuf is a intern result.
   */
  void (*xFinalize)(SQLFunctionCtx *pCtx);

  void (*distMergeFunc)(SQLFunctionCtx *pCtx);

  void (*distSecondaryMergeFunc)(SQLFunctionCtx *pCtx);

  int32_t (*dataReqFunc)(SQLFunctionCtx *pCtx, TSKEY start, TSKEY end, int32_t colId);
} SQLAggFuncElem;

#define GET_RES_INFO(ctx) ((ctx)->resultInfo)

int32_t getResultDataInfo(int32_t dataType, int32_t dataBytes, int32_t functionId, int32_t param, int16_t *type,
                          int16_t *len, int16_t *interBytes, int16_t extLength, bool isSuperTable);

#define IS_STREAM_QUERY_VALID(x)  (((x)&TSDB_FUNCSTATE_STREAM) != 0)
#define IS_MULTIOUTPUT(x)         (((x)&TSDB_FUNCSTATE_MO) != 0)
#define IS_SINGLEOUTPUT(x)        (((x)&TSDB_FUNCSTATE_SO) != 0)
#define IS_OUTER_FORWARD(x)       (((x)&TSDB_FUNCSTATE_OF) != 0)

/*
 * the status of one block, used in metric query. all blocks are mixed together,
 * we need the status to decide if one block is a first/end/inter block of one meter
 */
enum {
  BLK_FILE_BLOCK = 0x1,
  BLK_BLOCK_LOADED = 0x2,
  BLK_CACHE_BLOCK = 0x4,  // in case of cache block, block must be loaded
};

/* determine the real data need to calculated the result */
enum {
  BLK_DATA_NO_NEEDED = 0x0,
  BLK_DATA_FILEDS_NEEDED = 0x1,
  BLK_DATA_ALL_NEEDED = 0x3,
};

#define SET_DATA_BLOCK_NOT_LOADED(x) ((x) &= (~BLK_BLOCK_LOADED));

typedef struct STwaInfo {
  TSKEY   lastKey;
  int8_t  hasResult;  // flag to denote has value
  int16_t type;       // source data type
  TSKEY   SKey;
  TSKEY   EKey;

  union {
    double  dOutput;
    int64_t iOutput;
  };

  union {
    double  dLastValue;
    int64_t iLastValue;
  };
} STwaInfo;

/* global sql function array */
extern struct SQLAggFuncElem aAggs[];

/* compatible check array list */
extern int32_t funcCompatDefList[];

void getStatistics(char *priData, char *data, int32_t size, int32_t numOfRow, int32_t type, int64_t *min, int64_t *max,
                   int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int32_t *numOfNull);

bool top_bot_datablock_filter(SQLFunctionCtx *pCtx, int32_t functionId, char *minval, char *maxval);

bool stableQueryFunctChanged(int32_t funcId);

void resetResultInfo(SResultInfo *pResInfo);
void initResultInfo(SResultInfo *pResInfo);
void setResultInfoBuf(SResultInfo *pResInfo, int32_t size, bool superTable);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSQLFUNCTION_H
