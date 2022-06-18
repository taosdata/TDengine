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
#include "querynodes.h"
#include "scalar.h"
#include "taoserror.h"

static int32_t buildFuncErrMsg(char* pErrBuf, int32_t len, int32_t errCode, const char* pFormat, ...) {
  va_list vArgList;
  va_start(vArgList, pFormat);
  vsnprintf(pErrBuf, len, pFormat, vArgList);
  va_end(vArgList);
  return errCode;
}

static int32_t invaildFuncParaNumErrMsg(char* pErrBuf, int32_t len, const char* pFuncName) {
  return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_PARA_NUM, "Invalid number of parameters : %s", pFuncName);
}

static int32_t invaildFuncParaTypeErrMsg(char* pErrBuf, int32_t len, const char* pFuncName) {
  return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_PARA_TYPE, "Invalid parameter data type : %s", pFuncName);
}

static int32_t invaildFuncParaValueErrMsg(char* pErrBuf, int32_t len, const char* pFuncName) {
  return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_PARA_VALUE, "Invalid parameter value : %s", pFuncName);
}

// There is only one parameter of numeric type, and the return type is parameter type
static int32_t translateInOutNum(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  if (!IS_NUMERIC_TYPE(paraType) && !IS_NULL_TYPE(paraType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  } else if (IS_NULL_TYPE(paraType)) {
    paraType = TSDB_DATA_TYPE_BIGINT;
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[paraType].bytes, .type = paraType};
  return TSDB_CODE_SUCCESS;
}

// There is only one parameter of numeric type, and the return type is double type
static int32_t translateInNumOutDou(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  if (!IS_NUMERIC_TYPE(paraType) && !IS_NULL_TYPE(paraType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};
  return TSDB_CODE_SUCCESS;
}

// There are two parameters of numeric type, and the return type is double type
static int32_t translateIn2NumOutDou(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (2 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t para1Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  uint8_t para2Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type;
  if (!IS_NUMERIC_TYPE(para1Type) || !IS_NUMERIC_TYPE(para2Type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};
  return TSDB_CODE_SUCCESS;
}

// There is only one parameter of string type, and the return type is parameter type
static int32_t translateInOutStr(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  SExprNode* pPara1 = (SExprNode*)nodesListGetNode(pFunc->pParameterList, 0);
  if (!IS_VAR_DATA_TYPE(pPara1->resType.type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = pPara1->resType.bytes, .type = pPara1->resType.type};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateLogarithm(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (1 != numOfParams && 2 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t para1Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  if (!IS_NUMERIC_TYPE(para1Type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (2 == numOfParams) {
    uint8_t para2Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type;
    if (!IS_NUMERIC_TYPE(para2Type)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateCount(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }
  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes, .type = TSDB_DATA_TYPE_BIGINT};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateSum(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  if (!IS_NUMERIC_TYPE(paraType) && !IS_NULL_TYPE(paraType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t resType = 0;
  if (IS_SIGNED_NUMERIC_TYPE(paraType) || TSDB_DATA_TYPE_BOOL == paraType || IS_NULL_TYPE(paraType)) {
    resType = TSDB_DATA_TYPE_BIGINT;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(paraType)) {
    resType = TSDB_DATA_TYPE_UBIGINT;
  } else if (IS_FLOAT_TYPE(paraType)) {
    resType = TSDB_DATA_TYPE_DOUBLE;
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[resType].bytes, .type = resType};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateAvgPartial(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  if (!IS_NUMERIC_TYPE(paraType) && !IS_NULL_TYPE(paraType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = getAvgInfoSize() + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateAvgMerge(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  if (TSDB_DATA_TYPE_BINARY != paraType) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};

  return TSDB_CODE_SUCCESS;
}

static int32_t translateStddevPartial(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  if (!IS_NUMERIC_TYPE(paraType) && !IS_NULL_TYPE(paraType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = getStddevInfoSize() + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateStddevMerge(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  if (TSDB_DATA_TYPE_BINARY != paraType) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};

  return TSDB_CODE_SUCCESS;
}

static int32_t translateWduration(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  // pseudo column do not need to check parameters
  pFunc->node.resType = (SDataType){.bytes = sizeof(int64_t), .type = TSDB_DATA_TYPE_BIGINT};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateTimePseudoColumn(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  // pseudo column do not need to check parameters
  pFunc->node.resType = (SDataType){.bytes = sizeof(int64_t), .type = TSDB_DATA_TYPE_TIMESTAMP};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateTimezone(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  pFunc->node.resType = (SDataType){.bytes = TD_TIMEZONE_LEN, .type = TSDB_DATA_TYPE_BINARY};
  return TSDB_CODE_SUCCESS;
}

static int32_t translatePercentile(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (2 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // param1
  SValueNode* pValue = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);

  if (pValue->datum.i < 0 || pValue->datum.i > 100) {
    return invaildFuncParaValueErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pValue->notReserved = true;

  uint8_t para1Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  uint8_t para2Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type;
  if (!IS_NUMERIC_TYPE(para1Type) || (!IS_SIGNED_NUMERIC_TYPE(para2Type) && !IS_UNSIGNED_NUMERIC_TYPE(para2Type))) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // set result type
  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};
  return TSDB_CODE_SUCCESS;
}

static bool validateApercentileAlgo(const SValueNode* pVal) {
  if (TSDB_DATA_TYPE_BINARY != pVal->node.resType.type) {
    return false;
  }
  return (0 == strcasecmp(varDataVal(pVal->datum.p), "default") ||
          0 == strcasecmp(varDataVal(pVal->datum.p), "t-digest"));
}

static int32_t translateApercentile(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (2 != numOfParams && 3 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // param1
  SNode* pParamNode1 = nodesListGetNode(pFunc->pParameterList, 1);
  if (nodeType(pParamNode1) != QUERY_NODE_VALUE) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  SValueNode* pValue = (SValueNode*)pParamNode1;
  if (pValue->datum.i < 0 || pValue->datum.i > 100) {
    return invaildFuncParaValueErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pValue->notReserved = true;

  uint8_t para1Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  uint8_t para2Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type;
  if (!IS_NUMERIC_TYPE(para1Type) || !IS_INTEGER_TYPE(para2Type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // param2
  if (3 == numOfParams) {
    uint8_t para3Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 2))->resType.type;
    if (!IS_VAR_DATA_TYPE(para3Type)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    SNode* pParamNode2 = nodesListGetNode(pFunc->pParameterList, 2);
    if (QUERY_NODE_VALUE != nodeType(pParamNode2) || !validateApercentileAlgo((SValueNode*)pParamNode2)) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                             "Third parameter algorithm of apercentile must be 'default' or 't-digest'");
    }

    pValue = (SValueNode*)pParamNode2;
    pValue->notReserved = true;
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateApercentileImpl(SFunctionNode* pFunc, char* pErrBuf, int32_t len, bool isPartial) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);

  if (isPartial) {
    if (2 != numOfParams && 3 != numOfParams) {
      return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
    }
    // param1
    SNode* pParamNode1 = nodesListGetNode(pFunc->pParameterList, 1);
    if (nodeType(pParamNode1) != QUERY_NODE_VALUE) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    SValueNode* pValue = (SValueNode*)pParamNode1;
    if (pValue->datum.i < 0 || pValue->datum.i > 100) {
      return invaildFuncParaValueErrMsg(pErrBuf, len, pFunc->functionName);
    }

    pValue->notReserved = true;

    uint8_t para1Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
    uint8_t para2Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type;
    if (!IS_NUMERIC_TYPE(para1Type) || !IS_INTEGER_TYPE(para2Type)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    // param2
    if (3 == numOfParams) {
      uint8_t para3Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 2))->resType.type;
      if (!IS_VAR_DATA_TYPE(para3Type)) {
        return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
      }

      SNode* pParamNode2 = nodesListGetNode(pFunc->pParameterList, 2);
      if (QUERY_NODE_VALUE != nodeType(pParamNode2) || !validateApercentileAlgo((SValueNode*)pParamNode2)) {
        return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                               "Third parameter algorithm of apercentile must be 'default' or 't-digest'");
      }

      pValue = (SValueNode*)pParamNode2;
      pValue->notReserved = true;
    }

    pFunc->node.resType =
        (SDataType){.bytes = getApercentileMaxSize() + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY};
  } else {
    if (1 != numOfParams) {
      return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
    }
    uint8_t para1Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
    if (TSDB_DATA_TYPE_BINARY != para1Type) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t translateApercentilePartial(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateApercentileImpl(pFunc, pErrBuf, len, true);
}

static int32_t translateApercentileMerge(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateApercentileImpl(pFunc, pErrBuf, len, false);
}

static int32_t translateTbnameColumn(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  // pseudo column do not need to check parameters
  pFunc->node.resType =
      (SDataType){.bytes = TSDB_TABLE_FNAME_LEN - 1 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateTopBot(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (2 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t para1Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  uint8_t para2Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type;
  if (!IS_NUMERIC_TYPE(para1Type) || !IS_INTEGER_TYPE(para2Type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // param1
  SNode* pParamNode1 = nodesListGetNode(pFunc->pParameterList, 1);
  if (nodeType(pParamNode1) != QUERY_NODE_VALUE) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  SValueNode* pValue = (SValueNode*)pParamNode1;
  if (pValue->node.resType.type != TSDB_DATA_TYPE_BIGINT) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (pValue->datum.i < 1 || pValue->datum.i > 100) {
    return invaildFuncParaValueErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pValue->notReserved = true;

  // set result type
  SDataType* pType = &((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType;
  pFunc->node.resType = (SDataType){.bytes = pType->bytes, .type = pType->type};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateTopBotImpl(SFunctionNode* pFunc, char* pErrBuf, int32_t len, bool isPartial) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);

  if (isPartial) {
    if (2 != numOfParams) {
      return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
    }

    uint8_t para1Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
    uint8_t para2Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type;
    if (!IS_NUMERIC_TYPE(para1Type) || !IS_INTEGER_TYPE(para2Type)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    // param1
    SNode* pParamNode1 = nodesListGetNode(pFunc->pParameterList, 1);
    if (nodeType(pParamNode1) != QUERY_NODE_VALUE) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    SValueNode* pValue = (SValueNode*)pParamNode1;
    if (pValue->node.resType.type != TSDB_DATA_TYPE_BIGINT) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    if (pValue->datum.i < 1 || pValue->datum.i > 100) {
      return invaildFuncParaValueErrMsg(pErrBuf, len, pFunc->functionName);
    }

    pValue->notReserved = true;

    // set result type
    pFunc->node.resType =
        (SDataType){.bytes = getTopBotInfoSize(pValue->datum.i) + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY};
  } else {
    if (1 != numOfParams) {
      return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
    }

    uint8_t para1Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
    if (TSDB_DATA_TYPE_BINARY != para1Type) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    // Do nothing. We can only access output of partial functions as input,
    // so original input type cannot be obtained, resType will be set same
    // as original function input type after merge function created.
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateTopBotPartial(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateTopBotImpl(pFunc, pErrBuf, len, true);
}

static int32_t translateTopBotMerge(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateTopBotImpl(pFunc, pErrBuf, len, false);
}

static int32_t translateSpread(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  if (!IS_NUMERIC_TYPE(paraType) && TSDB_DATA_TYPE_TIMESTAMP != paraType) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateSpreadImpl(SFunctionNode* pFunc, char* pErrBuf, int32_t len, bool isPartial) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  if (isPartial) {
    if (!IS_NUMERIC_TYPE(paraType) && TSDB_DATA_TYPE_TIMESTAMP != paraType) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
    pFunc->node.resType = (SDataType){.bytes = getSpreadInfoSize() + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY};
  } else {
    if (TSDB_DATA_TYPE_BINARY != paraType) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
    pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t translateSpreadPartial(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateSpreadImpl(pFunc, pErrBuf, len, true);
}

static int32_t translateSpreadMerge(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateSpreadImpl(pFunc, pErrBuf, len, false);
}

static int32_t translateElapsed(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (1 != numOfParams && 2 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  if (TSDB_DATA_TYPE_TIMESTAMP != paraType) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // param1
  if (2 == numOfParams) {
    SNode* pParamNode1 = nodesListGetNode(pFunc->pParameterList, 1);
    if (QUERY_NODE_VALUE != nodeType(pParamNode1)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    SValueNode* pValue = (SValueNode*)pParamNode1;

    pValue->notReserved = true;

    paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type;
    if (!IS_INTEGER_TYPE(paraType)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    if (pValue->datum.i == 0) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                             "ELAPSED function time unit parameter should be greater than db precision");
    }
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateElapsedImpl(SFunctionNode* pFunc, char* pErrBuf, int32_t len, bool isPartial) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);

  if (isPartial) {
    if (1 != numOfParams && 2 != numOfParams) {
      return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
    }

    uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
    if (TSDB_DATA_TYPE_TIMESTAMP != paraType) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    // param1
    if (2 == numOfParams) {
      SNode* pParamNode1 = nodesListGetNode(pFunc->pParameterList, 1);
      if (QUERY_NODE_VALUE != nodeType(pParamNode1)) {
        return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
      }

      SValueNode* pValue = (SValueNode*)pParamNode1;

      pValue->notReserved = true;

      paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type;
      if (!IS_INTEGER_TYPE(paraType)) {
        return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
      }

      if (pValue->datum.i == 0) {
        return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                               "ELAPSED function time unit parameter should be greater than db precision");
      }
    }

    pFunc->node.resType =
        (SDataType){.bytes = getElapsedInfoSize() + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY};
  } else {
    if (1 != numOfParams) {
      return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
    }

    uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
    if (TSDB_DATA_TYPE_BINARY != paraType) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
    pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateElapsedPartial(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateElapsedImpl(pFunc, pErrBuf, len, true);
}

static int32_t translateElapsedMerge(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateElapsedImpl(pFunc, pErrBuf, len, false);
}

static int32_t translateLeastSQR(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (3 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  for (int32_t i = 0; i < numOfParams; ++i) {
    SNode* pParamNode = nodesListGetNode(pFunc->pParameterList, i);
    if (i > 0) {  // param1 & param2
      if (QUERY_NODE_VALUE != nodeType(pParamNode)) {
        return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
      }

      SValueNode* pValue = (SValueNode*)pParamNode;

      pValue->notReserved = true;
    }

    uint8_t colType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, i))->resType.type;
    if (!IS_NUMERIC_TYPE(colType)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
  }

  pFunc->node.resType = (SDataType){.bytes = 64, .type = TSDB_DATA_TYPE_BINARY};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateHistogram(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (4 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t colType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  if (!IS_NUMERIC_TYPE(colType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // param1 ~ param3
  for (int32_t i = 1; i < numOfParams; ++i) {
    SNode* pParamNode = nodesListGetNode(pFunc->pParameterList, i);
    if (QUERY_NODE_VALUE != nodeType(pParamNode)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    SValueNode* pValue = (SValueNode*)pParamNode;

    pValue->notReserved = true;
  }

  if (((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type != TSDB_DATA_TYPE_BINARY ||
      ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 2))->resType.type != TSDB_DATA_TYPE_BINARY ||
      ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 3))->resType.type != TSDB_DATA_TYPE_BIGINT) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = 512, .type = TSDB_DATA_TYPE_BINARY};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateHistogramImpl(SFunctionNode* pFunc, char* pErrBuf, int32_t len, bool isPartial) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (isPartial) {
    if (4 != numOfParams) {
      return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
    }

    uint8_t colType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
    if (!IS_NUMERIC_TYPE(colType)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    // param1 ~ param3
    for (int32_t i = 1; i < numOfParams; ++i) {
      SNode* pParamNode = nodesListGetNode(pFunc->pParameterList, i);
      if (QUERY_NODE_VALUE != nodeType(pParamNode)) {
        return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
      }

      SValueNode* pValue = (SValueNode*)pParamNode;

      pValue->notReserved = true;
    }

    if (((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type != TSDB_DATA_TYPE_BINARY ||
        ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 2))->resType.type != TSDB_DATA_TYPE_BINARY ||
        ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 3))->resType.type != TSDB_DATA_TYPE_BIGINT) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    pFunc->node.resType =
        (SDataType){.bytes = getHistogramInfoSize() + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY};
  } else {
    if (1 != numOfParams) {
      return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
    }

    if (((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type != TSDB_DATA_TYPE_BINARY) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    pFunc->node.resType = (SDataType){.bytes = 512, .type = TSDB_DATA_TYPE_BINARY};
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateHistogramPartial(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateHistogramImpl(pFunc, pErrBuf, len, true);
}

static int32_t translateHistogramMerge(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateHistogramImpl(pFunc, pErrBuf, len, false);
}

static int32_t translateHLL(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes, .type = TSDB_DATA_TYPE_BIGINT};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateHLLImpl(SFunctionNode* pFunc, char* pErrBuf, int32_t len, bool isPartial) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (isPartial) {
    pFunc->node.resType =
        (SDataType){.bytes = getHistogramInfoSize() + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY};
  } else {
    pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes, .type = TSDB_DATA_TYPE_BIGINT};
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t translateHLLPartial(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateHLLImpl(pFunc, pErrBuf, len, true);
}

static int32_t translateHLLMerge(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateHLLImpl(pFunc, pErrBuf, len, false);
}

static bool validateStateOper(const SValueNode* pVal) {
  if (TSDB_DATA_TYPE_BINARY != pVal->node.resType.type) {
    return false;
  }
  return (0 == strcasecmp(varDataVal(pVal->datum.p), "GT") || 0 == strcasecmp(varDataVal(pVal->datum.p), "GE") ||
          0 == strcasecmp(varDataVal(pVal->datum.p), "LT") || 0 == strcasecmp(varDataVal(pVal->datum.p), "LE") ||
          0 == strcasecmp(varDataVal(pVal->datum.p), "EQ") || 0 == strcasecmp(varDataVal(pVal->datum.p), "NE"));
}

static int32_t translateStateCount(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (3 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t colType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  if (!IS_NUMERIC_TYPE(colType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // param1 & param2
  for (int32_t i = 1; i < numOfParams; ++i) {
    SNode* pParamNode = nodesListGetNode(pFunc->pParameterList, i);
    if (QUERY_NODE_VALUE != nodeType(pParamNode)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    SValueNode* pValue = (SValueNode*)pParamNode;

    if (i == 1 && !validateStateOper(pValue)) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                             "Second parameter of STATECOUNT function"
                             "must be one of the following: 'GE', 'GT', 'LE', 'LT', 'EQ', 'NE'");
    }

    pValue->notReserved = true;
  }

  if (((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type != TSDB_DATA_TYPE_BINARY ||
      (((SExprNode*)nodesListGetNode(pFunc->pParameterList, 2))->resType.type != TSDB_DATA_TYPE_BIGINT &&
       ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 2))->resType.type != TSDB_DATA_TYPE_DOUBLE)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // set result type
  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes, .type = TSDB_DATA_TYPE_BIGINT};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateStateDuration(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (3 != numOfParams && 4 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t colType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  if (!IS_NUMERIC_TYPE(colType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // param1, param2 & param3
  for (int32_t i = 1; i < numOfParams; ++i) {
    SNode* pParamNode = nodesListGetNode(pFunc->pParameterList, i);
    if (QUERY_NODE_VALUE != nodeType(pParamNode)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    SValueNode* pValue = (SValueNode*)pParamNode;

    if (i == 1 && !validateStateOper(pValue)) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                             "Second parameter of STATEDURATION function"
                             "must be one of the following: 'GE', 'GT', 'LE', 'LT', 'EQ', 'NE'");
    } else if (i == 3 && pValue->datum.i == 0) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                             "STATEDURATION function time unit parameter should be greater than db precision");
    }

    pValue->notReserved = true;
  }

  if (((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type != TSDB_DATA_TYPE_BINARY ||
      (((SExprNode*)nodesListGetNode(pFunc->pParameterList, 2))->resType.type != TSDB_DATA_TYPE_BIGINT &&
       ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 2))->resType.type != TSDB_DATA_TYPE_DOUBLE)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (numOfParams == 4 &&
      ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 3))->resType.type != TSDB_DATA_TYPE_BIGINT) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // set result type
  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes, .type = TSDB_DATA_TYPE_BIGINT};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateCsum(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t colType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  uint8_t resType;
  if (!IS_NUMERIC_TYPE(colType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  } else {
    if (IS_SIGNED_NUMERIC_TYPE(colType)) {
      resType = TSDB_DATA_TYPE_BIGINT;
    } else if (IS_UNSIGNED_NUMERIC_TYPE(colType)) {
      resType = TSDB_DATA_TYPE_UBIGINT;
    } else if (IS_FLOAT_TYPE(colType)) {
      resType = TSDB_DATA_TYPE_DOUBLE;
    } else {
      ASSERT(0);
    }
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[resType].bytes, .type = resType};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateMavg(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (2 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t colType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;

  // param1
  SNode* pParamNode1 = nodesListGetNode(pFunc->pParameterList, 1);
  if (QUERY_NODE_VALUE != nodeType(pParamNode1)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  SValueNode* pValue = (SValueNode*)pParamNode1;
  if (pValue->datum.i < 1 || pValue->datum.i > 1000) {
    return invaildFuncParaValueErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pValue->notReserved = true;

  uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type;
  if (!IS_NUMERIC_TYPE(colType) || !IS_INTEGER_TYPE(paraType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateSample(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (2 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  SExprNode* pCol = (SExprNode*)nodesListGetNode(pFunc->pParameterList, 0);
  uint8_t    colType = pCol->resType.type;

  // param1
  SNode* pParamNode1 = nodesListGetNode(pFunc->pParameterList, 1);
  if (QUERY_NODE_VALUE != nodeType(pParamNode1)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  SValueNode* pValue = (SValueNode*)pParamNode1;
  if (pValue->datum.i < 1 || pValue->datum.i > 1000) {
    return invaildFuncParaValueErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pValue->notReserved = true;

  uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type;
  if (!IS_INTEGER_TYPE(paraType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // set result type
  if (IS_VAR_DATA_TYPE(colType)) {
    pFunc->node.resType = (SDataType){.bytes = pCol->resType.bytes, .type = colType};
  } else {
    pFunc->node.resType = (SDataType){.bytes = tDataTypes[colType].bytes, .type = colType};
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t translateTail(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (2 != numOfParams && 3 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  SExprNode* pCol = (SExprNode*)nodesListGetNode(pFunc->pParameterList, 0);
  uint8_t    colType = pCol->resType.type;

  // param1 & param2
  for (int32_t i = 1; i < numOfParams; ++i) {
    SNode* pParamNode = nodesListGetNode(pFunc->pParameterList, i);
    if (QUERY_NODE_VALUE != nodeType(pParamNode)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    SValueNode* pValue = (SValueNode*)pParamNode;

    if (pValue->datum.i < ((i > 1) ? 0 : 1) || pValue->datum.i > 100) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                             "TAIL function second parameter should be in range [1, 100], "
                             "third parameter should be in range [0, 100]");
    }

    pValue->notReserved = true;

    uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, i))->resType.type;
    if (!IS_INTEGER_TYPE(paraType)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
  }

  // set result type
  if (IS_VAR_DATA_TYPE(colType)) {
    pFunc->node.resType = (SDataType){.bytes = pCol->resType.bytes, .type = colType};
  } else {
    pFunc->node.resType = (SDataType){.bytes = tDataTypes[colType].bytes, .type = colType};
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateLastRow(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  // todo
  return TSDB_CODE_SUCCESS;
}

static int32_t translateDerivative(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (3 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t colType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;

  // param1
  SNode* pParamNode1 = nodesListGetNode(pFunc->pParameterList, 1);
  if (QUERY_NODE_VALUE != nodeType(pParamNode1)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  SValueNode* pValue = (SValueNode*)pParamNode1;
  pValue->notReserved = true;

  if (!IS_NUMERIC_TYPE(colType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  SNode*      pParamNode2 = nodesListGetNode(pFunc->pParameterList, 2);
  SValueNode* pValue2 = (SValueNode*)pParamNode2;
  pValue2->notReserved = true;

  if (pValue2->datum.i != 0 && pValue2->datum.i != 1) {
    return invaildFuncParaValueErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateFirstLast(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  // first(col_list) will be rewritten as first(col)
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pPara = nodesListGetNode(pFunc->pParameterList, 0);
  if (QUERY_NODE_COLUMN != nodeType(pPara)) {
    return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                           "The parameters of first/last can only be columns");
  }

  pFunc->node.resType = ((SExprNode*)pPara)->resType;
  return TSDB_CODE_SUCCESS;
}

static int32_t translateFirstLastImpl(SFunctionNode* pFunc, char* pErrBuf, int32_t len, bool isPartial) {
  // first(col_list) will be rewritten as first(col)
  if (2 != LIST_LENGTH(pFunc->pParameterList)) {  // input has two params c0,ts, is this a bug?
    return TSDB_CODE_SUCCESS;
  }

  SNode*  pPara = nodesListGetNode(pFunc->pParameterList, 0);
  uint8_t paraType = ((SExprNode*)pPara)->resType.type;
  int32_t paraBytes = ((SExprNode*)pPara)->resType.bytes;
  if (isPartial) {
    if (QUERY_NODE_COLUMN != nodeType(pPara)) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                             "The parameters of first/last can only be columns");
    }

    pFunc->node.resType =
        (SDataType){.bytes = getFirstLastInfoSize(paraBytes) + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY};
  } else {
    if (TSDB_DATA_TYPE_BINARY != paraType) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    pFunc->node.resType = ((SExprNode*)pPara)->resType;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateFirstLastPartial(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateFirstLastImpl(pFunc, pErrBuf, len, true);
}

static int32_t translateFirstLastMerge(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateFirstLastImpl(pFunc, pErrBuf, len, false);
}

static int32_t translateUnique(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  SNode* pPara = nodesListGetNode(pFunc->pParameterList, 0);
  if (!nodesExprHasColumn(pPara)) {
    return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR, "The parameters of UNIQUE must contain columns");
  }

  pFunc->node.resType = ((SExprNode*)pPara)->resType;
  return TSDB_CODE_SUCCESS;
}

static int32_t translateDiff(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (numOfParams == 0 || numOfParams > 2) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t colType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  if (!IS_SIGNED_NUMERIC_TYPE(colType) && !IS_FLOAT_TYPE(colType) && TSDB_DATA_TYPE_BOOL != colType) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // param1
  if (numOfParams == 2) {
    uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type;
    if (!IS_INTEGER_TYPE(paraType)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    SNode* pParamNode1 = nodesListGetNode(pFunc->pParameterList, 1);
    if (QUERY_NODE_VALUE != nodeType(pParamNode1)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    SValueNode* pValue = (SValueNode*)pParamNode1;
    if (pValue->datum.i != 0 && pValue->datum.i != 1) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                             "Second parameter of DIFF function should be only 0 or 1");
    }

    pValue->notReserved = true;
  }

  uint8_t resType;
  if (IS_SIGNED_NUMERIC_TYPE(colType) || TSDB_DATA_TYPE_BOOL == colType) {
    resType = TSDB_DATA_TYPE_BIGINT;
  } else {
    resType = TSDB_DATA_TYPE_DOUBLE;
  }
  pFunc->node.resType = (SDataType){.bytes = tDataTypes[resType].bytes, .type = resType};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateLength(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (!IS_VAR_DATA_TYPE(((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes, .type = TSDB_DATA_TYPE_BIGINT};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateConcatImpl(SFunctionNode* pFunc, char* pErrBuf, int32_t len, int32_t minParaNum,
                                   int32_t maxParaNum, bool hasSep) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (numOfParams < minParaNum || numOfParams > maxParaNum) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t resultType = TSDB_DATA_TYPE_BINARY;
  int32_t resultBytes = 0;
  int32_t sepBytes = 0;

  // concat_ws separator should be constant string
  if (hasSep) {
    SNode* pPara = nodesListGetNode(pFunc->pParameterList, 0);
    if (nodeType(pPara) != QUERY_NODE_VALUE) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                             "The first parameter of CONCAT_WS function can only be constant string");
    }
  }

  /* For concat/concat_ws function, if params have NCHAR type, promote the final result to NCHAR */
  for (int32_t i = 0; i < numOfParams; ++i) {
    SNode*  pPara = nodesListGetNode(pFunc->pParameterList, i);
    uint8_t paraType = ((SExprNode*)pPara)->resType.type;
    if (!IS_VAR_DATA_TYPE(paraType) && !IS_NULL_TYPE(paraType)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
    if (TSDB_DATA_TYPE_NCHAR == paraType) {
      resultType = paraType;
    }
  }

  for (int32_t i = 0; i < numOfParams; ++i) {
    SNode*  pPara = nodesListGetNode(pFunc->pParameterList, i);
    uint8_t paraType = ((SExprNode*)pPara)->resType.type;
    int32_t paraBytes = ((SExprNode*)pPara)->resType.bytes;
    int32_t factor = 1;
    if (IS_NULL_TYPE(paraType)) {
      resultType = TSDB_DATA_TYPE_VARCHAR;
      resultBytes = 0;
      sepBytes = 0;
      break;
    }
    if (TSDB_DATA_TYPE_NCHAR == resultType && TSDB_DATA_TYPE_VARCHAR == paraType) {
      factor *= TSDB_NCHAR_SIZE;
    }
    resultBytes += paraBytes * factor;

    if (i == 0) {
      sepBytes = paraBytes * factor;
    }
  }

  if (hasSep) {
    resultBytes += sepBytes * (numOfParams - 3);
  }

  pFunc->node.resType = (SDataType){.bytes = resultBytes, .type = resultType};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateConcat(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateConcatImpl(pFunc, pErrBuf, len, 2, 8, false);
}

static int32_t translateConcatWs(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateConcatImpl(pFunc, pErrBuf, len, 3, 9, true);
}

static int32_t translateSubstr(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (2 != numOfParams && 3 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  SExprNode* pPara0 = (SExprNode*)nodesListGetNode(pFunc->pParameterList, 0);
  SExprNode* p1 = (SExprNode*)nodesListGetNode(pFunc->pParameterList, 1);

  uint8_t para1Type = p1->resType.type;
  if (!IS_VAR_DATA_TYPE(pPara0->resType.type) || !IS_INTEGER_TYPE(para1Type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (((SValueNode*)p1)->datum.i < 1) {
    return invaildFuncParaValueErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (3 == numOfParams) {
    SExprNode* p2 = (SExprNode*)nodesListGetNode(pFunc->pParameterList, 2);
    uint8_t    para2Type = p2->resType.type;
    if (!IS_INTEGER_TYPE(para2Type)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    int64_t v = ((SValueNode*)p1)->datum.i;
    if (v < 0 || v > INT16_MAX) {
      return invaildFuncParaValueErrMsg(pErrBuf, len, pFunc->functionName);
    }
  }

  pFunc->node.resType = (SDataType){.bytes = pPara0->resType.bytes, .type = pPara0->resType.type};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateCast(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  // The number of parameters has been limited by the syntax definition
  uint8_t para1Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  // The function return type has been set during syntax parsing
  uint8_t para2Type = pFunc->node.resType.type;
  if (para2Type != TSDB_DATA_TYPE_BIGINT && para2Type != TSDB_DATA_TYPE_UBIGINT &&
      para2Type != TSDB_DATA_TYPE_VARCHAR && para2Type != TSDB_DATA_TYPE_NCHAR &&
      para2Type != TSDB_DATA_TYPE_TIMESTAMP) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }
  if ((para2Type == TSDB_DATA_TYPE_TIMESTAMP && IS_VAR_DATA_TYPE(para1Type)) ||
      (para2Type == TSDB_DATA_TYPE_BINARY && para1Type == TSDB_DATA_TYPE_NCHAR)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  int32_t para2Bytes = pFunc->node.resType.bytes;
  if (IS_VAR_DATA_TYPE(para2Type)) {
    para2Bytes -= VARSTR_HEADER_SIZE;
  }
  if (para2Bytes <= 0 || para2Bytes > 1000) {  // cast dst var type length limits to 1000
    return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                           "CAST function converted length should be in range [0, 1000]");
  }
  return TSDB_CODE_SUCCESS;
}

/* Following are valid ISO-8601 timezone format:
 * 1 z/Z
 * 2 ±hh:mm
 * 3 ±hhmm
 * 4 ±hh
 *
 */

static bool validateHourRange(int8_t hour) {
  if (hour < 0 || hour > 12) {
    return false;
  }

  return true;
}

static bool validateMinuteRange(int8_t hour, int8_t minute, char sign) {
  if (minute == 0 || (minute == 30 && (hour == 3 || hour == 5) && sign == '+')) {
    return true;
  }

  return false;
}

static bool validateTimezoneFormat(const SValueNode* pVal) {
  if (TSDB_DATA_TYPE_BINARY != pVal->node.resType.type) {
    return false;
  }

  char*   tz = varDataVal(pVal->datum.p);
  int32_t len = varDataLen(pVal->datum.p);

  char   buf[3] = {0};
  int8_t hour = -1, minute = -1;
  if (len == 0) {
    return false;
  } else if (len == 1 && (tz[0] == 'z' || tz[0] == 'Z')) {
    return true;
  } else if ((tz[0] == '+' || tz[0] == '-')) {
    switch (len) {
      case 3:
      case 5: {
        for (int32_t i = 1; i < len; ++i) {
          if (!isdigit(tz[i])) {
            return false;
          }

          if (i == 2) {
            memcpy(buf, &tz[i - 1], 2);
            hour = taosStr2Int8(buf, NULL, 10);
            if (!validateHourRange(hour)) {
              return false;
            }
          } else if (i == 4) {
            memcpy(buf, &tz[i - 1], 2);
            minute = taosStr2Int8(buf, NULL, 10);
            if (!validateMinuteRange(hour, minute, tz[0])) {
              return false;
            }
          }
        }
        break;
      }
      case 6: {
        for (int32_t i = 1; i < len; ++i) {
          if (i == 3) {
            if (tz[i] != ':') {
              return false;
            }
            continue;
          }

          if (!isdigit(tz[i])) {
            return false;
          }

          if (i == 2) {
            memcpy(buf, &tz[i - 1], 2);
            hour = taosStr2Int8(buf, NULL, 10);
            if (!validateHourRange(hour)) {
              return false;
            }
          } else if (i == 5) {
            memcpy(buf, &tz[i - 1], 2);
            minute = taosStr2Int8(buf, NULL, 10);
            if (!validateMinuteRange(hour, minute, tz[0])) {
              return false;
            }
          }
        }
        break;
      }
      default: {
        return false;
      }
    }
  } else {
    return false;
  }

  return true;
}

void static addTimezoneParam(SNodeList* pList) {
  char       buf[6] = {0};
  time_t     t = taosTime(NULL);
  struct tm* tmInfo = taosLocalTime(&t, NULL);
  strftime(buf, sizeof(buf), "%z", tmInfo);
  int32_t len = (int32_t)strlen(buf);

  SValueNode* pVal = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  pVal->literal = strndup(buf, len);
  pVal->isDuration = false;
  pVal->translate = true;
  pVal->node.resType.type = TSDB_DATA_TYPE_BINARY;
  pVal->node.resType.bytes = len + VARSTR_HEADER_SIZE;
  pVal->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  pVal->datum.p = taosMemoryCalloc(1, len + VARSTR_HEADER_SIZE + 1);
  varDataSetLen(pVal->datum.p, len);
  strncpy(varDataVal(pVal->datum.p), pVal->literal, len);

  nodesListAppend(pList, (SNode*)pVal);
}

static int32_t translateToIso8601(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (1 != numOfParams && 2 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // param0
  uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  if (!IS_INTEGER_TYPE(paraType) && TSDB_DATA_TYPE_TIMESTAMP != paraType) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // param1
  if (numOfParams == 2) {
    SValueNode* pValue = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);

    if (!validateTimezoneFormat(pValue)) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR, "Invalid timzone format");
    }
  } else {  // add default client timezone
    addTimezoneParam(pFunc->pParameterList);
  }

  // set result type
  pFunc->node.resType = (SDataType){.bytes = 64, .type = TSDB_DATA_TYPE_BINARY};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateToUnixtimestamp(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (!IS_VAR_DATA_TYPE(((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes, .type = TSDB_DATA_TYPE_BIGINT};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateTimeTruncate(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (2 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t para1Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  uint8_t para2Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type;
  if ((!IS_VAR_DATA_TYPE(para1Type) && !IS_INTEGER_TYPE(para1Type) && TSDB_DATA_TYPE_TIMESTAMP != para1Type) ||
      !IS_INTEGER_TYPE(para2Type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType =
      (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes, .type = TSDB_DATA_TYPE_TIMESTAMP};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateTimeDiff(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (2 != numOfParams && 3 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  for (int32_t i = 0; i < 2; ++i) {
    uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, i))->resType.type;
    if (!IS_VAR_DATA_TYPE(paraType) && !IS_INTEGER_TYPE(paraType) && TSDB_DATA_TYPE_TIMESTAMP != paraType) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
  }

  if (3 == numOfParams) {
    if (!IS_INTEGER_TYPE(((SExprNode*)nodesListGetNode(pFunc->pParameterList, 2))->resType.type)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes, .type = TSDB_DATA_TYPE_BIGINT};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateToJson(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  SExprNode* pPara = (SExprNode*)nodesListGetNode(pFunc->pParameterList, 0);
  if (QUERY_NODE_VALUE != nodeType(pPara) || (!IS_VAR_DATA_TYPE(pPara->resType.type))) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_BINARY].bytes, .type = TSDB_DATA_TYPE_BINARY};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateSelectValue(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  pFunc->node.resType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType;
  return TSDB_CODE_SUCCESS;
}

static int32_t translateBlockDistFunc(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  pFunc->node.resType = (SDataType){.bytes = 128, .type = TSDB_DATA_TYPE_VARCHAR};
  return TSDB_CODE_SUCCESS;
}

static bool getBlockDistFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(STableBlockDistInfo);
  return true;
}

// clang-format off
const SBuiltinFuncDefinition funcMgtBuiltins[] = {
  {
    .name = "count",
    .type = FUNCTION_TYPE_COUNT,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SPECIAL_DATA_REQUIRED,
    .translateFunc = translateCount,
    .dataRequiredFunc = countDataRequired,
    .getEnvFunc   = getCountFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = countFunction,
    .finalizeFunc = functionFinalize,
    .invertFunc   = countInvertFunction,
    .combineFunc  = combineFunction,
    .pPartialFunc = "count",
    .pMergeFunc   = "sum"
  },
  {
    .name = "sum",
    .type = FUNCTION_TYPE_SUM,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SPECIAL_DATA_REQUIRED,
    .translateFunc = translateSum,
    .dataRequiredFunc = statisDataRequired,
    .getEnvFunc   = getSumFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = sumFunction,
    .finalizeFunc = functionFinalize,
    .invertFunc   = sumInvertFunction,
    .combineFunc  = sumCombine,
    .pPartialFunc = "sum",
    .pMergeFunc   = "sum"
  },
  {
    .name = "min",
    .type = FUNCTION_TYPE_MIN,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SPECIAL_DATA_REQUIRED | FUNC_MGT_SELECT_FUNC,
    .translateFunc = translateInOutNum,
    .dataRequiredFunc = statisDataRequired,
    .getEnvFunc   = getMinmaxFuncEnv,
    .initFunc     = minmaxFunctionSetup,
    .processFunc  = minFunction,
    .finalizeFunc = minmaxFunctionFinalize,
    .combineFunc  = minCombine,
    .pPartialFunc = "min",
    .pMergeFunc   = "min"
  },
  {
    .name = "max",
    .type = FUNCTION_TYPE_MAX,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SPECIAL_DATA_REQUIRED | FUNC_MGT_SELECT_FUNC,
    .translateFunc = translateInOutNum,
    .dataRequiredFunc = statisDataRequired,
    .getEnvFunc   = getMinmaxFuncEnv,
    .initFunc     = minmaxFunctionSetup,
    .processFunc  = maxFunction,
    .finalizeFunc = minmaxFunctionFinalize,
    .combineFunc  = maxCombine,
    .pPartialFunc = "max",
    .pMergeFunc   = "max"
  },
  {
    .name = "stddev",
    .type = FUNCTION_TYPE_STDDEV,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateInNumOutDou,
    .getEnvFunc   = getStddevFuncEnv,
    .initFunc     = stddevFunctionSetup,
    .processFunc  = stddevFunction,
    .finalizeFunc = stddevFinalize,
    .invertFunc   = stddevInvertFunction,
    .combineFunc  = stddevCombine,
    .pPartialFunc = "_stddev_partial",
    .pMergeFunc   = "_stddev_merge"
  },
  {
    .name = "_stddev_partial",
    .type = FUNCTION_TYPE_STDDEV_PARTIAL,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateStddevPartial,
    .getEnvFunc   = getStddevFuncEnv,
    .initFunc     = stddevFunctionSetup,
    .processFunc  = stddevFunction,
    .finalizeFunc = stddevPartialFinalize,
    .invertFunc   = stddevInvertFunction,
    .combineFunc  = stddevCombine,
  },
  {
    .name = "_stddev_merge",
    .type = FUNCTION_TYPE_STDDEV_MERGE,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateStddevMerge,
    .getEnvFunc   = getStddevFuncEnv,
    .initFunc     = stddevFunctionSetup,
    .processFunc  = stddevFunctionMerge,
    .finalizeFunc = stddevFinalize,
    .invertFunc   = stddevInvertFunction,
    .combineFunc  = stddevCombine,
  },
  {
    .name = "leastsquares",
    .type = FUNCTION_TYPE_LEASTSQUARES,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateLeastSQR,
    .getEnvFunc   = getLeastSQRFuncEnv,
    .initFunc     = leastSQRFunctionSetup,
    .processFunc  = leastSQRFunction,
    .finalizeFunc = leastSQRFinalize,
    .invertFunc   = NULL,
    .combineFunc  = leastSQRCombine,
  },
  {
    .name = "avg",
    .type = FUNCTION_TYPE_AVG,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateInNumOutDou,
    .dataRequiredFunc = statisDataRequired,
    .getEnvFunc   = getAvgFuncEnv,
    .initFunc     = avgFunctionSetup,
    .processFunc  = avgFunction,
    .finalizeFunc = avgFinalize,
    .invertFunc   = avgInvertFunction,
    .combineFunc  = avgCombine,
    .pPartialFunc = "_avg_partial",
    .pMergeFunc   = "_avg_merge"
  },
  {
    .name = "_avg_partial",
    .type = FUNCTION_TYPE_AVG_PARTIAL,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateAvgPartial,
    .getEnvFunc   = getAvgFuncEnv,
    .initFunc     = avgFunctionSetup,
    .processFunc  = avgFunction,
    .finalizeFunc = avgPartialFinalize,
    .invertFunc   = avgInvertFunction,
    .combineFunc  = avgCombine,
  },
  {
    .name = "_avg_merge",
    .type = FUNCTION_TYPE_AVG_MERGE,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateAvgMerge,
    .getEnvFunc   = getAvgFuncEnv,
    .initFunc     = avgFunctionSetup,
    .processFunc  = avgFunctionMerge,
    .finalizeFunc = avgFinalize,
    .invertFunc   = avgInvertFunction,
    .combineFunc  = avgCombine,
  },
  {
    .name = "percentile",
    .type = FUNCTION_TYPE_PERCENTILE,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_REPEAT_SCAN_FUNC | FUNC_MGT_FORBID_STREAM_FUNC,
    .translateFunc = translatePercentile,
    .getEnvFunc   = getPercentileFuncEnv,
    .initFunc     = percentileFunctionSetup,
    .processFunc  = percentileFunction,
    .finalizeFunc = percentileFinalize,
    .invertFunc   = NULL,
    .combineFunc  = NULL,
  },
  {
    .name = "apercentile",
    .type = FUNCTION_TYPE_APERCENTILE,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateApercentile,
    .getEnvFunc   = getApercentileFuncEnv,
    .initFunc     = apercentileFunctionSetup,
    .processFunc  = apercentileFunction,
    .finalizeFunc = apercentileFinalize,
    .invertFunc   = NULL,
    .combineFunc  = apercentileCombine,
    .pPartialFunc = "_apercentile_partial",
    .pMergeFunc   = "_apercentile_merge"
  },
  {
    .name = "_apercentile_partial",
    .type = FUNCTION_TYPE_APERCENTILE_PARTIAL,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateApercentilePartial,
    .getEnvFunc   = getApercentileFuncEnv,
    .initFunc     = apercentileFunctionSetup,
    .processFunc  = apercentileFunction,
    .finalizeFunc = apercentilePartialFinalize,
    .invertFunc   = NULL,
    .combineFunc = apercentileCombine,
  },
  {
    .name = "_apercentile_merge",
    .type = FUNCTION_TYPE_APERCENTILE_MERGE,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateApercentileMerge,
    .getEnvFunc   = getApercentileFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = apercentileFunctionMerge,
    .finalizeFunc = apercentileFinalize,
    .invertFunc   = NULL,
    .combineFunc = apercentileCombine,
  },
  {
    .name = "top",
    .type = FUNCTION_TYPE_TOP,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_INDEFINITE_ROWS_FUNC | FUNC_MGT_FORBID_STREAM_FUNC,
    .translateFunc = translateTopBot,
    .getEnvFunc   = getTopBotFuncEnv,
    .initFunc     = topBotFunctionSetup,
    .processFunc  = topFunction,
    .finalizeFunc = topBotFinalize,
    .combineFunc  = topCombine,
    .pPartialFunc = "_top_partial",
    .pMergeFunc   = "_top_merge"
  },
  {
    .name = "_top_partial",
    .type = FUNCTION_TYPE_TOP_PARTIAL,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_INDEFINITE_ROWS_FUNC,
    .translateFunc = translateTopBotPartial,
    .getEnvFunc   = getTopBotFuncEnv,
    .initFunc     = topBotFunctionSetup,
    .processFunc  = topFunction,
    .finalizeFunc = topBotPartialFinalize,
    .combineFunc  = topCombine,
  },
  {
    .name = "_top_merge",
    .type = FUNCTION_TYPE_TOP_MERGE,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_INDEFINITE_ROWS_FUNC,
    .translateFunc = translateTopBotMerge,
    .getEnvFunc   = getTopBotMergeFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = topFunctionMerge,
    .finalizeFunc = topBotMergeFinalize,
    .combineFunc  = topCombine,
  },
  {
    .name = "bottom",
    .type = FUNCTION_TYPE_BOTTOM,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_INDEFINITE_ROWS_FUNC | FUNC_MGT_FORBID_STREAM_FUNC,
    .translateFunc = translateTopBot,
    .getEnvFunc   = getTopBotFuncEnv,
    .initFunc     = topBotFunctionSetup,
    .processFunc  = bottomFunction,
    .finalizeFunc = topBotFinalize,
    .combineFunc  = bottomCombine,
    .pPartialFunc = "_bottom_partial",
    .pMergeFunc   = "_bottom_merge"
  },
  {
    .name = "_bottom_partial",
    .type = FUNCTION_TYPE_BOTTOM_PARTIAL,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_INDEFINITE_ROWS_FUNC,
    .translateFunc = translateTopBotPartial,
    .getEnvFunc   = getTopBotFuncEnv,
    .initFunc     = topBotFunctionSetup,
    .processFunc  = bottomFunction,
    .finalizeFunc = topBotPartialFinalize,
    .combineFunc  = bottomCombine,
  },
  {
    .name = "_bottom_merge",
    .type = FUNCTION_TYPE_BOTTOM_MERGE,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_INDEFINITE_ROWS_FUNC,
    .translateFunc = translateTopBotMerge,
    .getEnvFunc   = getTopBotMergeFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = bottomFunctionMerge,
    .finalizeFunc = topBotMergeFinalize,
    .combineFunc  = bottomCombine,
  },
  {
    .name = "spread",
    .type = FUNCTION_TYPE_SPREAD,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateSpread,
    .dataRequiredFunc = statisDataRequired,
    .getEnvFunc   = getSpreadFuncEnv,
    .initFunc     = spreadFunctionSetup,
    .processFunc  = spreadFunction,
    .finalizeFunc = spreadFinalize,
    .invertFunc   = NULL,
    .combineFunc  = spreadCombine,
    .pPartialFunc = "_spread_partial",
    .pMergeFunc   = "_spread_merge"
  },
  {
    .name = "_spread_partial",
    .type = FUNCTION_TYPE_SPREAD_PARTIAL,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateSpreadPartial,
    .dataRequiredFunc = statisDataRequired,
    .getEnvFunc   = getSpreadFuncEnv,
    .initFunc     = spreadFunctionSetup,
    .processFunc  = spreadFunction,
    .finalizeFunc = spreadPartialFinalize,
    .invertFunc   = NULL,
    .combineFunc  = spreadCombine,
  },
  {
    .name = "_spread_merge",
    .type = FUNCTION_TYPE_SPREAD_MERGE,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateSpreadMerge,
    .dataRequiredFunc = statisDataRequired,
    .getEnvFunc   = getSpreadFuncEnv,
    .initFunc     = spreadFunctionSetup,
    .processFunc  = spreadFunctionMerge,
    .finalizeFunc = spreadFinalize,
    .invertFunc   = NULL,
    .combineFunc  = spreadCombine,
  },
  {
    .name = "elapsed",
    .type = FUNCTION_TYPE_ELAPSED,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_TIMELINE_FUNC | FUNC_MGT_INTERVAL_INTERPO_FUNC | FUNC_MGT_FORBID_STREAM_FUNC,
    .dataRequiredFunc = statisDataRequired,
    .translateFunc = translateElapsed,
    .getEnvFunc   = getElapsedFuncEnv,
    .initFunc     = elapsedFunctionSetup,
    .processFunc  = elapsedFunction,
    .finalizeFunc = elapsedFinalize,
    .invertFunc   = NULL,
    .combineFunc  = elapsedCombine,
    //.pPartialFunc = "_elapsed_partial",
    //.pMergeFunc   = "_elapsed_merge"
  },
  {
    .name = "_elapsed_partial",
    .type = FUNCTION_TYPE_ELAPSED,
    .classification = FUNC_MGT_AGG_FUNC,
    .dataRequiredFunc = statisDataRequired,
    .translateFunc = translateElapsedPartial,
    .getEnvFunc   = getElapsedFuncEnv,
    .initFunc     = elapsedFunctionSetup,
    .processFunc  = elapsedFunction,
    .finalizeFunc = elapsedPartialFinalize,
    .invertFunc   = NULL,
    .combineFunc  = elapsedCombine,
  },
  {
    .name = "_elapsed_merge",
    .type = FUNCTION_TYPE_ELAPSED,
    .classification = FUNC_MGT_AGG_FUNC,
    .dataRequiredFunc = statisDataRequired,
    .translateFunc = translateElapsedMerge,
    .getEnvFunc   = getElapsedFuncEnv,
    .initFunc     = elapsedFunctionSetup,
    .processFunc  = elapsedFunctionMerge,
    .finalizeFunc = elapsedFinalize,
    .invertFunc   = NULL,
    .combineFunc  = elapsedCombine,
  },
  {
    .name = "interp",
    .type = FUNCTION_TYPE_INTERP,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_TIMELINE_FUNC | FUNC_MGT_INTERVAL_INTERPO_FUNC,
    .translateFunc = translateFirstLast,
    .getEnvFunc    = getSelectivityFuncEnv,
    .initFunc      = functionSetup,
    .processFunc   = NULL,
    .finalizeFunc  = NULL
  },
  {
    .name = "derivative",
    .type = FUNCTION_TYPE_DERIVATIVE,
    .classification = FUNC_MGT_INDEFINITE_ROWS_FUNC | FUNC_MGT_TIMELINE_FUNC,
    .translateFunc = translateDerivative,
    .getEnvFunc   = getDerivativeFuncEnv,
    .initFunc     = derivativeFuncSetup,
    .processFunc  = derivativeFunction,
    .finalizeFunc = functionFinalize
  },
  {
    .name = "last_row",
    .type = FUNCTION_TYPE_LAST_ROW,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_MULTI_RES_FUNC,
    .translateFunc = translateLastRow,
    .getEnvFunc   = getMinmaxFuncEnv,
    .initFunc     = minmaxFunctionSetup,
    .processFunc  = maxFunction,
    .finalizeFunc = functionFinalize
  },
  {
    .name = "first",
    .type = FUNCTION_TYPE_FIRST,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_MULTI_RES_FUNC | FUNC_MGT_TIMELINE_FUNC,
    .translateFunc = translateFirstLast,
    .getEnvFunc   = getFirstLastFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = firstFunction,
    .finalizeFunc = firstLastFinalize,
    .pPartialFunc = "_first_partial",
    .pMergeFunc   = "_first_merge",
    .combineFunc  = firstCombine,
  },
  {
    .name = "_first_partial",
    .type = FUNCTION_TYPE_FIRST_PARTIAL,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_MULTI_RES_FUNC | FUNC_MGT_TIMELINE_FUNC,
    .translateFunc = translateFirstLastPartial,
    .getEnvFunc   = getFirstLastFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = firstFunction,
    .finalizeFunc = firstLastPartialFinalize,
    .combineFunc  = firstCombine,
  },
  {
    .name = "_first_merge",
    .type = FUNCTION_TYPE_FIRST_MERGE,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_MULTI_RES_FUNC | FUNC_MGT_TIMELINE_FUNC,
    .translateFunc = translateFirstLastMerge,
    .getEnvFunc   = getFirstLastFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = firstFunctionMerge,
    .finalizeFunc = firstLastFinalize,
    .combineFunc  = firstCombine,
  },
  {
    .name = "last",
    .type = FUNCTION_TYPE_LAST,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_MULTI_RES_FUNC | FUNC_MGT_TIMELINE_FUNC,
    .translateFunc = translateFirstLast,
    .getEnvFunc   = getFirstLastFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = lastFunction,
    .finalizeFunc = firstLastFinalize,
    .pPartialFunc = "_last_partial",
    .pMergeFunc   = "_last_merge",
    .combineFunc  = lastCombine,
  },
  {
    .name = "_last_partial",
    .type = FUNCTION_TYPE_LAST_PARTIAL,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_MULTI_RES_FUNC | FUNC_MGT_TIMELINE_FUNC,
    .translateFunc = translateFirstLastPartial,
    .getEnvFunc   = getFirstLastFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = lastFunction,
    .finalizeFunc = firstLastPartialFinalize,
    .combineFunc  = lastCombine,
  },
  {
    .name = "_last_merge",
    .type = FUNCTION_TYPE_LAST_MERGE,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_MULTI_RES_FUNC | FUNC_MGT_TIMELINE_FUNC,
    .translateFunc = translateFirstLastMerge,
    .getEnvFunc   = getFirstLastFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = lastFunctionMerge,
    .finalizeFunc = firstLastFinalize,
    .combineFunc  = lastCombine,
  },
  {
    .name = "twa",
    .type = FUNCTION_TYPE_TWA,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_TIMELINE_FUNC | FUNC_MGT_INTERVAL_INTERPO_FUNC | FUNC_MGT_FORBID_STREAM_FUNC,
    .translateFunc = translateInNumOutDou,
    .dataRequiredFunc = statisDataRequired,
    .getEnvFunc    = getTwaFuncEnv,
    .initFunc      = twaFunctionSetup,
    .processFunc   = twaFunction,
    .finalizeFunc  = twaFinalize
  },
  {
    .name = "histogram",
    .type = FUNCTION_TYPE_HISTOGRAM,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_FORBID_FILL_FUNC,
    .translateFunc = translateHistogram,
    .getEnvFunc   = getHistogramFuncEnv,
    .initFunc     = histogramFunctionSetup,
    .processFunc  = histogramFunction,
    .finalizeFunc = histogramFinalize,
    .invertFunc   = NULL,
    .combineFunc  = histogramCombine,
    .pPartialFunc = "_histogram_partial",
    .pMergeFunc   = "_histogram_merge",
  },
  {
    .name = "_histogram_partial",
    .type = FUNCTION_TYPE_HISTOGRAM_PARTIAL,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateHistogramPartial,
    .getEnvFunc   = getHistogramFuncEnv,
    .initFunc     = histogramFunctionSetup,
    .processFunc  = histogramFunction,
    .finalizeFunc = histogramPartialFinalize,
    .invertFunc   = NULL,
    .combineFunc  = histogramCombine,
  },
  {
    .name = "_histogram_merge",
    .type = FUNCTION_TYPE_HISTOGRAM_MERGE,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateHistogramMerge,
    .getEnvFunc   = getHistogramFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = histogramFunctionMerge,
    .finalizeFunc = histogramFinalize,
    .invertFunc   = NULL,
    .combineFunc  = histogramCombine,
  },
  {
    .name = "hyperloglog",
    .type = FUNCTION_TYPE_HYPERLOGLOG,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateHLL,
    .getEnvFunc   = getHLLFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = hllFunction,
    .finalizeFunc = hllFinalize,
    .invertFunc   = NULL,
    .combineFunc  = hllCombine,
    .pPartialFunc = "_hyperloglog_partial",
    .pMergeFunc   = "_hyperloglog_merge"
  },
  {
    .name = "_hyperloglog_partial",
    .type = FUNCTION_TYPE_HYPERLOGLOG_PARTIAL,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateHLLPartial,
    .getEnvFunc   = getHLLFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = hllFunction,
    .finalizeFunc = hllPartialFinalize,
    .invertFunc   = NULL,
    .combineFunc  = hllCombine,
  },
  {
    .name = "_hyperloglog_merge",
    .type = FUNCTION_TYPE_HYPERLOGLOG_MERGE,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateHLLMerge,
    .getEnvFunc   = getHLLFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = hllFunctionMerge,
    .finalizeFunc = hllFinalize,
    .invertFunc   = NULL,
    .combineFunc  = hllCombine,
  },
  {
    .name = "diff",
    .type = FUNCTION_TYPE_DIFF,
    .classification = FUNC_MGT_INDEFINITE_ROWS_FUNC | FUNC_MGT_TIMELINE_FUNC | FUNC_MGT_FORBID_STREAM_FUNC,
    .translateFunc = translateDiff,
    .getEnvFunc   = getDiffFuncEnv,
    .initFunc     = diffFunctionSetup,
    .processFunc  = diffFunction,
    .finalizeFunc = functionFinalize
  },
  {
    .name = "statecount",
    .type = FUNCTION_TYPE_STATE_COUNT,
    .classification = FUNC_MGT_INDEFINITE_ROWS_FUNC | FUNC_MGT_FORBID_STREAM_FUNC,
    .translateFunc = translateStateCount,
    .getEnvFunc   = getStateFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = stateCountFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "stateduration",
    .type = FUNCTION_TYPE_STATE_DURATION,
    .classification = FUNC_MGT_INDEFINITE_ROWS_FUNC | FUNC_MGT_TIMELINE_FUNC | FUNC_MGT_FORBID_STREAM_FUNC,
    .translateFunc = translateStateDuration,
    .getEnvFunc   = getStateFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = stateDurationFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "csum",
    .type = FUNCTION_TYPE_CSUM,
    .classification = FUNC_MGT_INDEFINITE_ROWS_FUNC | FUNC_MGT_TIMELINE_FUNC | FUNC_MGT_FORBID_STREAM_FUNC,
    .translateFunc = translateCsum,
    .getEnvFunc   = getCsumFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = csumFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "mavg",
    .type = FUNCTION_TYPE_MAVG,
    .classification = FUNC_MGT_INDEFINITE_ROWS_FUNC | FUNC_MGT_TIMELINE_FUNC | FUNC_MGT_FORBID_STREAM_FUNC,
    .translateFunc = translateMavg,
    .getEnvFunc   = getMavgFuncEnv,
    .initFunc     = mavgFunctionSetup,
    .processFunc  = mavgFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "sample",
    .type = FUNCTION_TYPE_SAMPLE,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_INDEFINITE_ROWS_FUNC | FUNC_MGT_FORBID_STREAM_FUNC,
    .translateFunc = translateSample,
    .getEnvFunc   = getSampleFuncEnv,
    .initFunc     = sampleFunctionSetup,
    .processFunc  = sampleFunction,
    .finalizeFunc = sampleFinalize
  },
  {
    .name = "tail",
    .type = FUNCTION_TYPE_TAIL,
    .classification = FUNC_MGT_INDEFINITE_ROWS_FUNC | FUNC_MGT_TIMELINE_FUNC | FUNC_MGT_FORBID_STREAM_FUNC | FUNC_MGT_FORBID_WINDOW_FUNC | FUNC_MGT_FORBID_GROUP_BY_FUNC,
    .translateFunc = translateTail,
    .getEnvFunc   = getTailFuncEnv,
    .initFunc     = tailFunctionSetup,
    .processFunc  = tailFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "unique",
    .type = FUNCTION_TYPE_UNIQUE,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_INDEFINITE_ROWS_FUNC | FUNC_MGT_TIMELINE_FUNC | 
                      FUNC_MGT_FORBID_STREAM_FUNC | FUNC_MGT_FORBID_WINDOW_FUNC | FUNC_MGT_FORBID_GROUP_BY_FUNC,
    .translateFunc = translateUnique,
    .getEnvFunc   = getUniqueFuncEnv,
    .initFunc     = uniqueFunctionSetup,
    .processFunc  = uniqueFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "abs",
    .type = FUNCTION_TYPE_ABS,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateInOutNum,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = absFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "log",
    .type = FUNCTION_TYPE_LOG,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateLogarithm,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = logFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "pow",
    .type = FUNCTION_TYPE_POW,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateIn2NumOutDou,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = powFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "sqrt",
    .type = FUNCTION_TYPE_SQRT,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateInNumOutDou,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = sqrtFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "ceil",
    .type = FUNCTION_TYPE_CEIL,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateInOutNum,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = ceilFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "floor",
    .type = FUNCTION_TYPE_FLOOR,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateInOutNum,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = floorFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "round",
    .type = FUNCTION_TYPE_ROUND,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateInOutNum,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = roundFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "sin",
    .type = FUNCTION_TYPE_SIN,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateInNumOutDou,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = sinFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "cos",
    .type = FUNCTION_TYPE_COS,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateInNumOutDou,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = cosFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "tan",
    .type = FUNCTION_TYPE_TAN,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateInNumOutDou,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = tanFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "asin",
    .type = FUNCTION_TYPE_ASIN,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateInNumOutDou,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = asinFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "acos",
    .type = FUNCTION_TYPE_ACOS,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateInNumOutDou,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = acosFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "atan",
    .type = FUNCTION_TYPE_ATAN,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateInNumOutDou,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = atanFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "length",
    .type = FUNCTION_TYPE_LENGTH,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_STRING_FUNC,
    .translateFunc = translateLength,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = lengthFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "char_length",
    .type = FUNCTION_TYPE_CHAR_LENGTH,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_STRING_FUNC,
    .translateFunc = translateLength,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = charLengthFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "concat",
    .type = FUNCTION_TYPE_CONCAT,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_STRING_FUNC,
    .translateFunc = translateConcat,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = concatFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "concat_ws",
    .type = FUNCTION_TYPE_CONCAT_WS,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_STRING_FUNC,
    .translateFunc = translateConcatWs,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = concatWsFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "lower",
    .type = FUNCTION_TYPE_LOWER,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_STRING_FUNC,
    .translateFunc = translateInOutStr,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = lowerFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "upper",
    .type = FUNCTION_TYPE_UPPER,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_STRING_FUNC,
    .translateFunc = translateInOutStr,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = upperFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "ltrim",
    .type = FUNCTION_TYPE_LTRIM,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_STRING_FUNC,
    .translateFunc = translateInOutStr,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = ltrimFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "rtrim",
    .type = FUNCTION_TYPE_RTRIM,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_STRING_FUNC,
    .translateFunc = translateInOutStr,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = rtrimFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "substr",
    .type = FUNCTION_TYPE_SUBSTR,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_STRING_FUNC,
    .translateFunc = translateSubstr,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = substrFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "cast",
    .type = FUNCTION_TYPE_CAST,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateCast,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = castFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "to_iso8601",
    .type = FUNCTION_TYPE_TO_ISO8601,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateToIso8601,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = toISO8601Function,
    .finalizeFunc = NULL
  },
  {
    .name = "to_unixtimestamp",
    .type = FUNCTION_TYPE_TO_UNIXTIMESTAMP,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateToUnixtimestamp,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = toUnixtimestampFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "timetruncate",
    .type = FUNCTION_TYPE_TIMETRUNCATE,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateTimeTruncate,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = timeTruncateFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "timediff",
    .type = FUNCTION_TYPE_TIMEDIFF,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateTimeDiff,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = timeDiffFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "now",
    .type = FUNCTION_TYPE_NOW,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_DATETIME_FUNC,
    .translateFunc = translateTimePseudoColumn,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = nowFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "today",
    .type = FUNCTION_TYPE_TODAY,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_DATETIME_FUNC,
    .translateFunc = translateTimePseudoColumn,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = todayFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "timezone",
    .type = FUNCTION_TYPE_TIMEZONE,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateTimezone,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = timezoneFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "tbname",
    .type = FUNCTION_TYPE_TBNAME,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_SCAN_PC_FUNC,
    .translateFunc = translateTbnameColumn,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = qTbnameFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "_qstartts",
    .type = FUNCTION_TYPE_QSTARTTS,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_WINDOW_PC_FUNC,
    .translateFunc = translateTimePseudoColumn,
    .getEnvFunc   = getTimePseudoFuncEnv,
    .initFunc     = NULL,
    .sprocessFunc = qStartTsFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "_qendts",
    .type = FUNCTION_TYPE_QENDTS,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_WINDOW_PC_FUNC,
    .translateFunc = translateTimePseudoColumn,
    .getEnvFunc   = getTimePseudoFuncEnv,
    .initFunc     = NULL,
    .sprocessFunc = qEndTsFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "_wstartts",
    .type = FUNCTION_TYPE_WSTARTTS,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_WINDOW_PC_FUNC,
    .translateFunc = translateTimePseudoColumn,
    .getEnvFunc   = getTimePseudoFuncEnv,
    .initFunc     = NULL,
    .sprocessFunc = winStartTsFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "_wendts",
    .type = FUNCTION_TYPE_WENDTS,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_WINDOW_PC_FUNC,
    .translateFunc = translateTimePseudoColumn,
    .getEnvFunc   = getTimePseudoFuncEnv,
    .initFunc     = NULL,
    .sprocessFunc = winEndTsFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "_wduration",
    .type = FUNCTION_TYPE_WDURATION,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_WINDOW_PC_FUNC,
    .translateFunc = translateWduration,
    .getEnvFunc   = getTimePseudoFuncEnv,
    .initFunc     = NULL,
    .sprocessFunc = winDurFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "to_json",
    .type = FUNCTION_TYPE_TO_JSON,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateToJson,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = toJsonFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "_select_value",
    .type = FUNCTION_TYPE_SELECT_VALUE,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC,
    .translateFunc = translateSelectValue,
    .getEnvFunc   = getSelectivityFuncEnv,  // todo remove this function later.
    .initFunc     = functionSetup,
    .processFunc  = NULL,
    .finalizeFunc = NULL
  },
  {
    .name = "_block_dist",
    .type = FUNCTION_TYPE_BLOCK_DIST,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateBlockDistFunc,
    .getEnvFunc   = getBlockDistFuncEnv,
    .processFunc  = blockDistFunction,
    .finalizeFunc = blockDistFinalize
  }
};
// clang-format on

const int32_t funcMgtBuiltinsNum = (sizeof(funcMgtBuiltins) / sizeof(SBuiltinFuncDefinition));
