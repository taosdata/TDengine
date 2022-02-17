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

#include "builtins.h"
#include "taoserror.h"

int32_t stubCheckAndGetResultType(SFunctionNode* pFunc);

const SBuiltinFuncDefinition funcMgtBuiltins[] = {
  {
    .name = "count",
    .type = FUNCTION_TYPE_COUNT,
    .classification = FUNC_MGT_AGG_FUNC,
    .checkFunc = stubCheckAndGetResultType,
    .getEnvFunc = NULL,
    .initFunc = NULL,
    .processFunc = NULL,
    .finalizeFunc = NULL
  },
  {
    .name = "concat",
    .type = FUNCTION_TYPE_CONCAT,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_STRING_FUNC,
    .checkFunc = stubCheckAndGetResultType,
    .getEnvFunc = NULL,
    .initFunc = NULL,
    .processFunc = NULL,
    .finalizeFunc = NULL
  }
};

const int funcMgtBuiltinsNum = (sizeof(funcMgtBuiltins) / sizeof(SBuiltinFuncDefinition));

int32_t stubCheckAndGetResultType(SFunctionNode* pFunc) {
  return TSDB_CODE_SUCCESS;
}
