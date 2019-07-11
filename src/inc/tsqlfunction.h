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

#include <stdbool.h>
#include <stdint.h>

#include "trpc.h"
#include "tsql.h"
#include "ttypes.h"

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
#define TSDB_FUNC_LEASTSQR     11
#define TSDB_FUNC_TOP          12
#define TSDB_FUNC_BOTTOM       13
#define TSDB_FUNC_SPREAD       14
#define TSDB_FUNC_WAVG         15
#define TSDB_FUNC_TS           16
#define TSDB_FUNC_TS_DUMMY     17

#define TSDB_FUNC_TAG          18
#define TSDB_FUNC_PRJ          19

#define TSDB_FUNC_TAGPRJ       20
#define TSDB_FUNC_ARITHM       21
#define TSDB_FUNC_DIFF         22

#define TSDB_FUNC_SUM_DST      23
#define TSDB_FUNC_AVG_DST      24
#define TSDB_FUNC_MIN_DST      25
#define TSDB_FUNC_MAX_DST      26

#define TSDB_FUNC_FIRST_DST    27
#define TSDB_FUNC_LAST_DST     28
#define TSDB_FUNC_LAST_ROW_DST 29
#define TSDB_FUNC_SPREAD_DST   30

#define TSDB_FUNC_WAVG_DST     31
#define TSDB_FUNC_TOP_DST      32
#define TSDB_FUNC_BOTTOM_DST   33
#define TSDB_FUNC_APERCT_DST   34
#define TSDB_FUNC_INTERP       35

#define TSDB_FUNCSTATE_SO      0x1  // single output
#define TSDB_FUNCSTATE_MO      0x2  // dynamic number of output, not multinumber of output e.g., TOP/BOTTOM
#define TSDB_FUNCSTATE_STREAM  0x4  // function avail for stream
#define TSDB_FUNCSTATE_METRIC  0x8  // function avail for metric
#define TSDB_FUNCSTATE_OF      0x10 // outer forward
#define TSDB_FUNCSTATE_NEED_TS 0x20

#define TSDB_BASE_FUNC_SO TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_STREAM | TSDB_FUNCSTATE_METRIC | TSDB_FUNCSTATE_OF
#define TSDB_BASE_FUNC_MO TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_STREAM | TSDB_FUNCSTATE_METRIC | TSDB_FUNCSTATE_OF

#define TSDB_PATTERN_MATCH           0
#define TSDB_PATTERN_NOMATCH         1
#define TSDB_PATTERN_NOWILDCARDMATCH 2
#define TSDB_PATTERN_STRING_MAX_LEN 20

#define TSDB_FUNCTIONS_NAME_MAX_LENGTH      16
#define TSDB_AVG_FUNCTION_INTER_BUFFER_SIZE 50

#define PATTERN_COMPARE_INFO_INITIALIZER \
  { '%', '_' }

#define DATA_SET_FLAG ','  // to denote the output area has data, not null value
#define DATA_SET_FLAG_SIZE sizeof(char)

#define QUERY_ASC_FORWARD_STEP   1
#define QUERY_DESC_FORWARD_STEP -1
#define GET_FORWARD_DIRECTION_FACTOR(ord) (((ord) == TSQL_SO_ASC) ? QUERY_ASC_FORWARD_STEP : QUERY_DESC_FORWARD_STEP)

enum {
  MASTER_SCAN = 0x0,
  SUPPLEMENTARY_SCAN = 0x1,
  SECONDARY_STAGE_MERGE = 0x10,
};

typedef struct {
  SSqlFunctionExpr *pExpr;
  int32_t           elemSize[TSDB_MAX_COLUMNS];
  int32_t           numOfCols;
  int32_t           offset;
  char *            data[TSDB_MAX_COLUMNS];
} SArithmeticSupport;

typedef struct SQLPreAggVal {
  bool    isSet;
  int32_t numOfNullPoints;
  int64_t wsum;
  int64_t sum;
  int64_t min;
  int64_t max;
} SQLPreAggVal;

/* sql function runtime context */
typedef struct SQLFunctionCtx {
  int32_t startOffset;
  int32_t size;
  int32_t order;
  int32_t scanFlag;

  int16_t inputType;
  int16_t inputBytes;

  int16_t outputType;
  int16_t outputBytes; /* size of results, determined by function and input
                          column data type */

  bool    hasNullValue; /* null value exist in current block */
  int32_t blockStatus;  /* Indicate if data is loaded, it is first/last/internal
                           block. Only for file blocks */

  void *  aInputElemBuf;
  char *  aOutputBuf;         /* final result output buffer, point to sdata->data */
  int64_t numOfIteratedElems; /* total scanned points in processing, used for
                                 complex query process */
  int32_t numOfOutputElems;

  int32_t currentStage; /* record current running step, default: 0 */

  int64_t nStartQueryTimestamp; /* timestamp range of current query when
                                   function is executed on a specific data block
                                   */
  tVariant intermediateBuf[4];  /* to hold intermediate result */

  int32_t  numOfParams;
  tVariant param[4];     /* input parameter, current support only one element */
  int64_t *ptsList;      /* additional array list */
  void *   ptsOutputBuf; /* output buffer for the corresponding timestamp of each
                            result, e.g., top/bottom*/

  SQLPreAggVal preAggVals;
} SQLFunctionCtx;

typedef struct SQLAggFuncElem {
  char aName[TSDB_FUNCTIONS_NAME_MAX_LENGTH];

  uint8_t  nAggIdx;      /* index of function in aAggs         */
  int8_t   stableFuncId; /* transfer function for metric query */
  uint16_t nStatus;

  /* setup the execute environment */
  void (*init)(SQLFunctionCtx *pCtx);

  /* main execution function */
  bool (*xFunction)(SQLFunctionCtx *pCtx);

  /* filter version */
  bool (*xFunctionF)(SQLFunctionCtx *pCtx, int32_t position);

  /*
   * some sql function require scan data twice or more in case of no index
   * existing.
   * e.g., stddev, percentile[disk based process for extremely large dataset]
   * @param pCtx
   */
  bool (*xNextStep)(SQLFunctionCtx *pCtx);

  /*
   * finalizer must be called after all xFunction has been executed to
   * generated final result. Otherwise, the value in aOutputBuf is a intern
   * result.
   */
  void (*xFinalize)(SQLFunctionCtx *pCtx);

  void (*distMergeFunc)(SQLFunctionCtx *pCtx);

  void (*distSecondaryMergeFunc)(SQLFunctionCtx *pCtx);

  int32_t (*dataReqFunc)(SQLFunctionCtx *pCtx, TSKEY start, TSKEY end, int32_t colId, int32_t blockStatus);
} SQLAggFuncElem;

typedef struct SPatternCompareInfo {
  char matchAll;  // symbol for match all wildcard, default: '%'
  char matchOne;  // symbol for match one wildcard, default: '_'
} SPatternCompareInfo;

void function_finalize(SQLFunctionCtx *pCtx);

void getResultInfo(int32_t dataType, int32_t dataBytes, int32_t functionId, int32_t param, int16_t *type, int16_t *len);

int patternMatch(const char *zPattern, const char *zString, size_t size, const SPatternCompareInfo *pInfo);

int WCSPatternMatch(const wchar_t *zPattern, const wchar_t *zString, size_t size,
                    const struct SPatternCompareInfo *pInfo);

#define IS_STREAM_QUERY_VALID(x) (((x)&TSDB_FUNCSTATE_STREAM) != 0)
#define IS_MULTIOUTPUT(x) (((x)&TSDB_FUNCSTATE_MO) != 0)
#define IS_SINGLEOUTPUT(x) (((x)&TSDB_FUNCSTATE_SO) != 0)
#define IS_OUTER_FORWARD(x) (((x)&TSDB_FUNCSTATE_OF) != 0)

/*
 * the status of one block, used in metric query. all blocks are mixed together,
 * we need the status to decide
 * if one block is a first/end/inter block of one meter
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

#define IS_FILE_BLOCK(x) (((x)&BLK_FILE_BLOCK) != 0)

#define SET_FILE_BLOCK_FLAG(x) \
  do {                         \
    (x) &= (~BLK_CACHE_BLOCK); \
    (x) |= BLK_FILE_BLOCK;     \
  } while (0);

#define SET_CACHE_BLOCK_FLAG(x) ((x) = BLK_CACHE_BLOCK | BLK_BLOCK_LOADED);

#define SET_DATA_BLOCK_NOT_LOADED(x) ((x) &= (~BLK_BLOCK_LOADED));

#define SET_DATA_BLOCK_LOADED(x) ((x) |= BLK_BLOCK_LOADED);
#define IS_DATA_BLOCK_LOADED(x) (((x)&BLK_BLOCK_LOADED) != 0)

typedef struct SWavgRuntime {
  int8_t  valFlag;  // flag to denote has value
  int16_t type;     // source data type
  int64_t lastKey;
  int64_t sKey;
  int64_t eKey;

  union {
    double  dOutput;
    int64_t iOutput;
  };

  union {
    double  dLastValue;
    int64_t iLastValue;
  };
} SWavgRuntime;

typedef struct SSumRuntime {
  union {
    double  dOutput;
    int64_t iOutput;
  };
  int8_t valFlag;
} SSumRuntime;

typedef struct SAvgRuntime {
  double  sum;
  int64_t num;
  int8_t  valFlag;
} SAvgRuntime;

/* global sql function array */
extern struct SQLAggFuncElem aAggs[36];

/* compatible check array list */
extern int32_t funcCompatList[36];

void getStatistics(char *priData, char *data, int32_t size, int32_t numOfRow, int32_t type, int64_t *min, int64_t *max,
                   int64_t *sum, int64_t *wsum, int32_t *numOfNull);

bool top_bot_datablock_filter(SQLFunctionCtx *pCtx, int32_t functionId, char *minval, char *maxval);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSQLFUNCTION_H
