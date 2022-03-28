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
#include "builtinsimpl.h"
#include "scalar.h"
#include "taoserror.h"
#include "tdatablock.h"

int32_t stubCheckAndGetResultType(SFunctionNode* pFunc);

const SBuiltinFuncDefinition funcMgtBuiltins[] = {
  {
    .name = "count",
    .type = FUNCTION_TYPE_COUNT,
    .classification = FUNC_MGT_AGG_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = getCountFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = countFunction,
    .finalizeFunc = functionFinalize
  },
  {
    .name = "sum",
    .type = FUNCTION_TYPE_SUM,
    .classification = FUNC_MGT_AGG_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = getSumFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = sumFunction,
    .finalizeFunc = functionFinalize
  },
  {
    .name = "min",
    .type = FUNCTION_TYPE_MIN,
    .classification = FUNC_MGT_AGG_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = getMinmaxFuncEnv,
    .initFunc     = minFunctionSetup,
    .processFunc  = minFunction,
    .finalizeFunc = functionFinalize
  },
  {
    .name = "max",
    .type = FUNCTION_TYPE_MAX,
    .classification = FUNC_MGT_AGG_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = getMinmaxFuncEnv,
    .initFunc     = maxFunctionSetup,
    .processFunc  = maxFunction,
    .finalizeFunc = functionFinalize
  },
  {
      .name = "stddev",
      .type = FUNCTION_TYPE_STDDEV,
      .classification = FUNC_MGT_AGG_FUNC,
      .checkFunc    = stubCheckAndGetResultType,
      .getEnvFunc   = getStddevFuncEnv,
      .initFunc     = maxFunctionSetup,
      .processFunc  = maxFunction,
      .finalizeFunc = functionFinalize
  },
  {
      .name = "percentile",
      .type = FUNCTION_TYPE_PERCENTILE,
      .classification = FUNC_MGT_AGG_FUNC,
      .checkFunc    = stubCheckAndGetResultType,
      .getEnvFunc   = getMinmaxFuncEnv,
      .initFunc     = maxFunctionSetup,
      .processFunc  = maxFunction,
      .finalizeFunc = functionFinalize
  },
  {
      .name = "apercentile",
      .type = FUNCTION_TYPE_APERCENTILE,
      .classification = FUNC_MGT_AGG_FUNC,
      .checkFunc    = stubCheckAndGetResultType,
      .getEnvFunc   = getMinmaxFuncEnv,
      .initFunc     = maxFunctionSetup,
      .processFunc  = maxFunction,
      .finalizeFunc = functionFinalize
  },
  {
      .name = "top",
      .type = FUNCTION_TYPE_TOP,
      .classification = FUNC_MGT_AGG_FUNC,
      .checkFunc    = stubCheckAndGetResultType,
      .getEnvFunc   = getMinmaxFuncEnv,
      .initFunc     = maxFunctionSetup,
      .processFunc  = maxFunction,
      .finalizeFunc = functionFinalize
  },
  {
      .name = "bottom",
      .type = FUNCTION_TYPE_BOTTOM,
      .classification = FUNC_MGT_AGG_FUNC,
      .checkFunc    = stubCheckAndGetResultType,
      .getEnvFunc   = getMinmaxFuncEnv,
      .initFunc     = maxFunctionSetup,
      .processFunc  = maxFunction,
      .finalizeFunc = functionFinalize
  },
  {
      .name = "spread",
      .type = FUNCTION_TYPE_SPREAD,
      .classification = FUNC_MGT_AGG_FUNC,
      .checkFunc    = stubCheckAndGetResultType,
      .getEnvFunc   = getMinmaxFuncEnv,
      .initFunc     = maxFunctionSetup,
      .processFunc  = maxFunction,
      .finalizeFunc = functionFinalize
  },
  {
      .name = "last_row",
      .type = FUNCTION_TYPE_LAST_ROW,
      .classification = FUNC_MGT_AGG_FUNC,
      .checkFunc    = stubCheckAndGetResultType,
      .getEnvFunc   = getMinmaxFuncEnv,
      .initFunc     = maxFunctionSetup,
      .processFunc  = maxFunction,
      .finalizeFunc = functionFinalize
  },
  {
    .name = "first",
    .type = FUNCTION_TYPE_FIRST,
    .classification = FUNC_MGT_AGG_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = getFirstLastFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = firstFunction,
    .finalizeFunc = functionFinalize
  },
  {
    .name = "last",
    .type = FUNCTION_TYPE_LAST,
    .classification = FUNC_MGT_AGG_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = getFirstLastFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = lastFunction,
    .finalizeFunc = functionFinalize
  },
  {
    .name = "abs",
    .type = FUNCTION_TYPE_ABS,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = absFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "log",
    .type = FUNCTION_TYPE_LOG,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = logFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "power",
    .type = FUNCTION_TYPE_POW,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = powFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "sqrt",
    .type = FUNCTION_TYPE_SQRT,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = sqrtFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "ceil",
    .type = FUNCTION_TYPE_CEIL,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = ceilFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "floor",
    .type = FUNCTION_TYPE_FLOOR,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = floorFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "round",
    .type = FUNCTION_TYPE_ROUND,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = roundFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "sin",
    .type = FUNCTION_TYPE_SIN,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = sinFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "cos",
    .type = FUNCTION_TYPE_COS,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = cosFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "tan",
    .type = FUNCTION_TYPE_TAN,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = tanFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "asin",
    .type = FUNCTION_TYPE_ASIN,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = asinFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "acos",
    .type = FUNCTION_TYPE_ACOS,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = acosFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "atan",
    .type = FUNCTION_TYPE_ATAN,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = atanFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "concat",
    .type = FUNCTION_TYPE_CONCAT,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_STRING_FUNC,
    .checkFunc    = stubCheckAndGetResultType,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = NULL,
    .finalizeFunc = NULL
  }
};

const int32_t funcMgtBuiltinsNum = (sizeof(funcMgtBuiltins) / sizeof(SBuiltinFuncDefinition));

int32_t stubCheckAndGetResultType(SFunctionNode* pFunc) {
  switch(pFunc->funcType) {
    case FUNCTION_TYPE_COUNT:
      pFunc->node.resType = (SDataType){.bytes = sizeof(int64_t), .type = TSDB_DATA_TYPE_BIGINT};
      break;
    case FUNCTION_TYPE_SUM: {
      SColumnNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
      int32_t paraType = pParam->node.resType.type;

      int32_t resType  = 0;
      if (IS_SIGNED_NUMERIC_TYPE(paraType)) {
        resType = TSDB_DATA_TYPE_BIGINT;
      } else if (IS_UNSIGNED_NUMERIC_TYPE(paraType)) {
        resType = TSDB_DATA_TYPE_UBIGINT;
      } else if (IS_FLOAT_TYPE(paraType)) {
        resType = TSDB_DATA_TYPE_DOUBLE;
      } else {
        ASSERT(0);
      }

      pFunc->node.resType = (SDataType) { .bytes = tDataTypes[resType].bytes, .type = resType };
      break;
    }
    case FUNCTION_TYPE_FIRST:
    case FUNCTION_TYPE_LAST:
    case FUNCTION_TYPE_MIN:
    case FUNCTION_TYPE_MAX: {
      SColumnNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
      int32_t paraType = pParam->node.resType.type;
      pFunc->node.resType = (SDataType) { .bytes = tDataTypes[paraType].bytes, .type = paraType };
      break;
    }
    case FUNCTION_TYPE_CONCAT:
      // todo
      break;
    default:
      ASSERT(0); // to found the fault ASAP.
  }

  return TSDB_CODE_SUCCESS;
}
