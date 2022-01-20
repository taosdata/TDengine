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

#ifndef _TD_FUNCTION_MGT_INT_H_
#define _TD_FUNCTION_MGT_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "functionMgt.h"

#define FUNC_MGT_DATA_TYPE_MASK(n)    (1 << n)

#define FUNC_MGT_DATA_TYPE_NULL       0
#define FUNC_MGT_DATA_TYPE_BOOL       FUNC_MGT_DATA_TYPE_MASK(0)
#define FUNC_MGT_DATA_TYPE_TINYINT    FUNC_MGT_DATA_TYPE_MASK(1)
#define FUNC_MGT_DATA_TYPE_SMALLINT   FUNC_MGT_DATA_TYPE_MASK(2)
#define FUNC_MGT_DATA_TYPE_INT        FUNC_MGT_DATA_TYPE_MASK(3)
#define FUNC_MGT_DATA_TYPE_BIGINT     FUNC_MGT_DATA_TYPE_MASK(4)
#define FUNC_MGT_DATA_TYPE_FLOAT      FUNC_MGT_DATA_TYPE_MASK(5)
#define FUNC_MGT_DATA_TYPE_DOUBLE     FUNC_MGT_DATA_TYPE_MASK(6)
#define FUNC_MGT_DATA_TYPE_BINARY     FUNC_MGT_DATA_TYPE_MASK(7)
#define FUNC_MGT_DATA_TYPE_TIMESTAMP  FUNC_MGT_DATA_TYPE_MASK(8)
#define FUNC_MGT_DATA_TYPE_NCHAR      FUNC_MGT_DATA_TYPE_MASK(9)
#define FUNC_MGT_DATA_TYPE_UTINYINT   FUNC_MGT_DATA_TYPE_MASK(10)
#define FUNC_MGT_DATA_TYPE_USMALLINT  FUNC_MGT_DATA_TYPE_MASK(11)
#define FUNC_MGT_DATA_TYPE_UINT       FUNC_MGT_DATA_TYPE_MASK(12)
#define FUNC_MGT_DATA_TYPE_UBIGINT    FUNC_MGT_DATA_TYPE_MASK(13)
#define FUNC_MGT_DATA_TYPE_VARCHAR    FUNC_MGT_DATA_TYPE_MASK(14)
#define FUNC_MGT_DATA_TYPE_VARBINARY  FUNC_MGT_DATA_TYPE_MASK(15)
#define FUNC_MGT_DATA_TYPE_JSON       FUNC_MGT_DATA_TYPE_MASK(16)
#define FUNC_MGT_DATA_TYPE_DECIMAL    FUNC_MGT_DATA_TYPE_MASK(17)
#define FUNC_MGT_DATA_TYPE_BLOB       FUNC_MGT_DATA_TYPE_MASK(18)

#define FUNC_MGT_EXACT_NUMERIC_DATA_TYPE \
  (FUNC_MGT_DATA_TYPE_TINYINT | FUNC_MGT_DATA_TYPE_SMALLINT | FUNC_MGT_DATA_TYPE_INT | FUNC_MGT_DATA_TYPE_BIGINT \
      | FUNC_MGT_DATA_TYPE_UTINYINT | FUNC_MGT_DATA_TYPE_USMALLINT | FUNC_MGT_DATA_TYPE_UINT | FUNC_MGT_DATA_TYPE_UBIGINT)

#define FUNC_MGT_APPRO_NUMERIC_DATA_TYPE (FUNC_MGT_DATA_TYPE_FLOAT | FUNC_MGT_DATA_TYPE_DOUBLE)

#define FUNC_MGT_NUMERIC_DATA_TYPE (FUNC_MGT_EXACT_NUMERIC_DATA_TYPE | FUNC_MGT_APPRO_NUMERIC_DATA_TYPE)

typedef void* FuncDef;

typedef struct SFuncElement {
  FuncDef (*defineFunc)();
} SFuncElement;

extern const SFuncElement gBuiltinFuncs[];

FuncDef createFuncDef(const char* name, int32_t maxNumOfParams);
FuncDef setOneParamSignature(FuncDef def, int64_t resDataType, int64_t paramDataType);
FuncDef setTwoParamsSignature(FuncDef def, int64_t resDataType, int64_t p1DataType, int64_t p2DataType);
FuncDef setFollowParamSignature(FuncDef def, int64_t paramDataType);
FuncDef setFollowParamsSignature(FuncDef def, int64_t p1DataType, int64_t p2DataType, int32_t followNo);

FuncDef setExecFuncs(FuncDef def, FExecGetEnv getEnv, FExecInit init, FExecProcess process, FExecFinalize finalize);

#ifdef __cplusplus
}
#endif

#endif  // _TD_FUNCTION_MGT_INT_H_
