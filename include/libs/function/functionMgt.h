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

#include "querynodes.h"
#include "function.h"

typedef enum EFunctionType {
  // aggregate function
  FUNCTION_TYPE_APERCENTILE = 1,
  FUNCTION_TYPE_AVG,
  FUNCTION_TYPE_COUNT,
  FUNCTION_TYPE_ELAPSED,
  FUNCTION_TYPE_IRATE,
  FUNCTION_TYPE_LAST_ROW,
  FUNCTION_TYPE_LEASTSQUARES,
  FUNCTION_TYPE_MAX,
  FUNCTION_TYPE_MIN,
  FUNCTION_TYPE_MODE,
  FUNCTION_TYPE_PERCENTILE,
  FUNCTION_TYPE_SPREAD,
  FUNCTION_TYPE_STDDEV,
  FUNCTION_TYPE_SUM,
  FUNCTION_TYPE_TWA,

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
  FUNCTION_TYPE_CHAR_LENGTH = 1500,
  FUNCTION_TYPE_CONCAT,
  FUNCTION_TYPE_CONCAT_WS,
  FUNCTION_TYPE_LENGTH,

  // conversion function
  FUNCTION_TYPE_CAST = 2000,
  FUNCTION_TYPE_TO_ISO8601,
  FUNCTION_TYPE_TO_JSON,
  FUNCTION_TYPE_UNIXTIMESTAMP,

  // date and time function
  FUNCTION_TYPE_NOW = 2500,
  FUNCTION_TYPE_TIMEDIFF,
  FUNCTION_TYPE_TIMETRUNCATE,
  FUNCTION_TYPE_TIMEZONE,
  FUNCTION_TYPE_TODAY,

  // system function
  FUNCTION_TYPE_DATABASE = 3000,
  FUNCTION_TYPE_CLIENT_VERSION,
  FUNCTION_TYPE_SERVER_SERSION,
  FUNCTION_TYPE_SERVER_STATUS,
  FUNCTION_TYPE_CURRENT_USER,
  FUNCTION_TYPE_USER
} EFunctionType;

struct SqlFunctionCtx;
struct SResultRowEntryInfo;
struct STimeWindow;

typedef int32_t (*FScalarExecProcess)(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);

typedef struct SScalarFuncExecFuncs {
  FScalarExecProcess process;
} SScalarFuncExecFuncs;


int32_t fmFuncMgtInit();

void fmFuncMgtDestroy();

int32_t fmGetFuncInfo(const char* pFuncName, int32_t* pFuncId, int32_t* pFuncType);

int32_t fmGetFuncResultType(SFunctionNode* pFunc);

bool fmIsAggFunc(int32_t funcId);
bool fmIsScalarFunc(int32_t funcId);
bool fmIsNonstandardSQLFunc(int32_t funcId);
bool fmIsStringFunc(int32_t funcId);
bool fmIsDatetimeFunc(int32_t funcId);
bool fmIsTimelineFunc(int32_t funcId);
bool fmIsTimeorderFunc(int32_t funcId);

int32_t fmFuncScanType(int32_t funcId);

int32_t fmGetFuncExecFuncs(int32_t funcId, SFuncExecFuncs* pFpSet);
int32_t fmGetScalarFuncExecFuncs(int32_t funcId, SScalarFuncExecFuncs* pFpSet);

#ifdef __cplusplus
}
#endif

#endif  // _TD_FUNCTION_MGT_H_
