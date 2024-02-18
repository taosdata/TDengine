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

#ifndef _TD_FUNCTION_MGT_H_
#define _TD_FUNCTION_MGT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "function.h"
#include "querynodes.h"

#define FUNC_AGGREGATE_UDF_ID 5001
#define FUNC_SCALAR_UDF_ID    5002

typedef enum EFunctionType {
  // aggregate function
  FUNCTION_TYPE_APERCENTILE = 1,
  FUNCTION_TYPE_AVG,
  FUNCTION_TYPE_COUNT,
  FUNCTION_TYPE_ELAPSED,
  FUNCTION_TYPE_IRATE,
  FUNCTION_TYPE_LAST_ROW,
  FUNCTION_TYPE_MAX,
  FUNCTION_TYPE_MIN,
  FUNCTION_TYPE_MODE,
  FUNCTION_TYPE_PERCENTILE,
  FUNCTION_TYPE_SPREAD,
  FUNCTION_TYPE_STDDEV,
  FUNCTION_TYPE_LEASTSQUARES,
  FUNCTION_TYPE_SUM,
  FUNCTION_TYPE_TWA,
  FUNCTION_TYPE_HISTOGRAM,
  FUNCTION_TYPE_HYPERLOGLOG,

  // nonstandard SQL function
  FUNCTION_TYPE_BOTTOM = 500,
  FUNCTION_TYPE_CSUM,
  FUNCTION_TYPE_DERIVATIVE,
  FUNCTION_TYPE_DIFF,
  FUNCTION_TYPE_FIRST,
  FUNCTION_TYPE_INTERP,
  FUNCTION_TYPE_LAST,
  FUNCTION_TYPE_MAVG,
  FUNCTION_TYPE_SAMPLE,
  FUNCTION_TYPE_TAIL,
  FUNCTION_TYPE_TOP,
  FUNCTION_TYPE_UNIQUE,
  FUNCTION_TYPE_STATE_COUNT,
  FUNCTION_TYPE_STATE_DURATION,

  // math function
  FUNCTION_TYPE_ABS = 1000,
  FUNCTION_TYPE_LOG,
  FUNCTION_TYPE_POW,
  FUNCTION_TYPE_SQRT,
  FUNCTION_TYPE_CEIL,
  FUNCTION_TYPE_FLOOR,
  FUNCTION_TYPE_ROUND,

  FUNCTION_TYPE_SIN,
  FUNCTION_TYPE_COS,
  FUNCTION_TYPE_TAN,
  FUNCTION_TYPE_ASIN,
  FUNCTION_TYPE_ACOS,
  FUNCTION_TYPE_ATAN,

  // string function
  FUNCTION_TYPE_LENGTH = 1500,
  FUNCTION_TYPE_CHAR_LENGTH,
  FUNCTION_TYPE_CONCAT,
  FUNCTION_TYPE_CONCAT_WS,
  FUNCTION_TYPE_LOWER,
  FUNCTION_TYPE_UPPER,
  FUNCTION_TYPE_LTRIM,
  FUNCTION_TYPE_RTRIM,
  FUNCTION_TYPE_SUBSTR,

  // conversion function
  FUNCTION_TYPE_CAST = 2000,
  FUNCTION_TYPE_TO_ISO8601,
  FUNCTION_TYPE_TO_UNIXTIMESTAMP,
  FUNCTION_TYPE_TO_JSON,
  FUNCTION_TYPE_TO_TIMESTAMP,
  FUNCTION_TYPE_TO_CHAR,

  // date and time function
  FUNCTION_TYPE_NOW = 2500,
  FUNCTION_TYPE_TIMEDIFF,
  FUNCTION_TYPE_TIMETRUNCATE,
  FUNCTION_TYPE_TIMEZONE,
  FUNCTION_TYPE_TODAY,

  // system function
  FUNCTION_TYPE_DATABASE = 3000,
  FUNCTION_TYPE_CLIENT_VERSION,
  FUNCTION_TYPE_SERVER_VERSION,
  FUNCTION_TYPE_SERVER_STATUS,
  FUNCTION_TYPE_CURRENT_USER,
  FUNCTION_TYPE_USER,

  // pseudo column function
  FUNCTION_TYPE_ROWTS = 3500,
  FUNCTION_TYPE_TBNAME,
  FUNCTION_TYPE_QSTART,
  FUNCTION_TYPE_QEND,
  FUNCTION_TYPE_QDURATION,
  FUNCTION_TYPE_WSTART,
  FUNCTION_TYPE_WEND,
  FUNCTION_TYPE_WDURATION,
  FUNCTION_TYPE_IROWTS,
  FUNCTION_TYPE_ISFILLED,
  FUNCTION_TYPE_TAGS,
  FUNCTION_TYPE_TBUID,
  FUNCTION_TYPE_VGID,
  FUNCTION_TYPE_VGVER,

  // internal function
  FUNCTION_TYPE_SELECT_VALUE = 3750,
  FUNCTION_TYPE_BLOCK_DIST,       // block distribution aggregate function
  FUNCTION_TYPE_BLOCK_DIST_INFO,  // block distribution pseudo column function
  FUNCTION_TYPE_TO_COLUMN,
  FUNCTION_TYPE_GROUP_KEY,
  FUNCTION_TYPE_CACHE_LAST_ROW,
  FUNCTION_TYPE_CACHE_LAST,
  FUNCTION_TYPE_TABLE_COUNT,

  // distributed splitting functions
  FUNCTION_TYPE_APERCENTILE_PARTIAL = 4000,
  FUNCTION_TYPE_APERCENTILE_MERGE,
  FUNCTION_TYPE_SPREAD_PARTIAL,
  FUNCTION_TYPE_SPREAD_MERGE,
  FUNCTION_TYPE_HISTOGRAM_PARTIAL,
  FUNCTION_TYPE_HISTOGRAM_MERGE,
  FUNCTION_TYPE_HYPERLOGLOG_PARTIAL,
  FUNCTION_TYPE_HYPERLOGLOG_MERGE,
  FUNCTION_TYPE_ELAPSED_PARTIAL,
  FUNCTION_TYPE_ELAPSED_MERGE,

  FUNCTION_TYPE_TOP_PARTIAL,
  FUNCTION_TYPE_TOP_MERGE,
  FUNCTION_TYPE_BOTTOM_PARTIAL,
  FUNCTION_TYPE_BOTTOM_MERGE,
  FUNCTION_TYPE_FIRST_PARTIAL,
  FUNCTION_TYPE_FIRST_MERGE,
  FUNCTION_TYPE_LAST_PARTIAL,
  FUNCTION_TYPE_LAST_MERGE,
  FUNCTION_TYPE_AVG_PARTIAL,
  FUNCTION_TYPE_AVG_MERGE,
  FUNCTION_TYPE_STDDEV_PARTIAL,
  FUNCTION_TYPE_STDDEV_MERGE,
  FUNCTION_TYPE_IRATE_PARTIAL,
  FUNCTION_TYPE_IRATE_MERGE,

  // geometry functions
  FUNCTION_TYPE_GEOM_FROM_TEXT = 4250,
  FUNCTION_TYPE_AS_TEXT,
  FUNCTION_TYPE_MAKE_POINT,
  FUNCTION_TYPE_INTERSECTS,
  FUNCTION_TYPE_EQUALS,
  FUNCTION_TYPE_TOUCHES,
  FUNCTION_TYPE_COVERS,
  FUNCTION_TYPE_CONTAINS,
  FUNCTION_TYPE_CONTAINS_PROPERLY,

  // user defined funcion
  FUNCTION_TYPE_UDF = 10000
} EFunctionType;

typedef enum EFuncReturnRows {
  FUNC_RETURN_ROWS_NORMAL = 1,
  FUNC_RETURN_ROWS_INDEFINITE,
  FUNC_RETURN_ROWS_N,
  FUNC_RETURN_ROWS_N_MINUS_1
} EFuncReturnRows;

struct SqlFunctionCtx;
struct SResultRowEntryInfo;
struct STimeWindow;

int32_t fmFuncMgtInit();

void fmFuncMgtDestroy();

int32_t fmGetFuncInfo(SFunctionNode* pFunc, char* pMsg, int32_t msgLen);

EFuncReturnRows fmGetFuncReturnRows(SFunctionNode* pFunc);

bool          fmIsBuiltinFunc(const char* pFunc);
EFunctionType fmGetFuncType(const char* pFunc);

bool fmIsAggFunc(int32_t funcId);
bool fmIsScalarFunc(int32_t funcId);
bool fmIsVectorFunc(int32_t funcId);
bool fmIsIndefiniteRowsFunc(int32_t funcId);
bool fmIsStringFunc(int32_t funcId);
bool fmIsDateTimeFunc(int32_t funcId);
bool fmIsSelectFunc(int32_t funcId);
bool fmIsTimelineFunc(int32_t funcId);
bool fmIsTimeorderFunc(int32_t funcId);
bool fmIsPseudoColumnFunc(int32_t funcId);
bool fmIsScanPseudoColumnFunc(int32_t funcId);
bool fmIsWindowPseudoColumnFunc(int32_t funcId);
bool fmIsWindowClauseFunc(int32_t funcId);
bool fmIsSpecialDataRequiredFunc(int32_t funcId);
bool fmIsDynamicScanOptimizedFunc(int32_t funcId);
bool fmIsMultiResFunc(int32_t funcId);
bool fmIsRepeatScanFunc(int32_t funcId);
bool fmIsUserDefinedFunc(int32_t funcId);
bool fmIsDistExecFunc(int32_t funcId);
bool fmIsForbidFillFunc(int32_t funcId);
bool fmIsForbidStreamFunc(int32_t funcId);
bool fmIsForbidSysTableFunc(int32_t funcId);
bool fmIsIntervalInterpoFunc(int32_t funcId);
bool fmIsInterpFunc(int32_t funcId);
bool fmIsLastRowFunc(int32_t funcId);
bool fmIsNotNullOutputFunc(int32_t funcId);
bool fmIsSelectValueFunc(int32_t funcId);
bool fmIsSystemInfoFunc(int32_t funcId);
bool fmIsImplicitTsFunc(int32_t funcId);
bool fmIsClientPseudoColumnFunc(int32_t funcId);
bool fmIsMultiRowsFunc(int32_t funcId);
bool fmIsKeepOrderFunc(int32_t funcId);
bool fmIsCumulativeFunc(int32_t funcId);
bool fmIsInterpPseudoColumnFunc(int32_t funcId);
bool fmIsGroupKeyFunc(int32_t funcId);
bool fmIsBlockDistFunc(int32_t funcId);
bool fmIsConstantResFunc(SFunctionNode* pFunc);
bool fmIsSkipScanCheckFunc(int32_t funcId);

void getLastCacheDataType(SDataType* pType);
SFunctionNode* createFunction(const char* pName, SNodeList* pParameterList);

int32_t fmGetDistMethod(const SFunctionNode* pFunc, SFunctionNode** pPartialFunc, SFunctionNode** pMidFunc, SFunctionNode** pMergeFunc);

typedef enum EFuncDataRequired {
  FUNC_DATA_REQUIRED_DATA_LOAD = 1,
  FUNC_DATA_REQUIRED_SMA_LOAD,
  FUNC_DATA_REQUIRED_NOT_LOAD,
  FUNC_DATA_REQUIRED_FILTEROUT,
  FUNC_DATA_REQUIRED_ALL_FILTEROUT,
} EFuncDataRequired;

EFuncDataRequired fmFuncDataRequired(SFunctionNode* pFunc, STimeWindow* pTimeWindow);
EFuncDataRequired fmFuncDynDataRequired(int32_t funcId, void* pRes, STimeWindow* pTimeWindow);

int32_t fmGetFuncExecFuncs(int32_t funcId, SFuncExecFuncs* pFpSet);
int32_t fmGetScalarFuncExecFuncs(int32_t funcId, SScalarFuncExecFuncs* pFpSet);
int32_t fmGetUdafExecFuncs(int32_t funcId, SFuncExecFuncs* pFpSet);

#ifdef BUILD_NO_CALL
int32_t fmSetInvertFunc(int32_t funcId, SFuncExecFuncs* pFpSet);
int32_t fmSetNormalFunc(int32_t funcId, SFuncExecFuncs* pFpSet);
bool    fmIsInvertible(int32_t funcId);
#endif

char*   fmGetFuncName(int32_t funcId);

#ifdef __cplusplus
}
#endif

#endif  // _TD_FUNCTION_MGT_H_
