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
  if (!IS_NUMERIC_TYPE(paraType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
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
  if (!IS_NUMERIC_TYPE(paraType)) {
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
  if (!IS_NUMERIC_TYPE(paraType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t resType = 0;
  if (IS_SIGNED_NUMERIC_TYPE(paraType) || paraType == TSDB_DATA_TYPE_BOOL) {
    resType = TSDB_DATA_TYPE_BIGINT;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(paraType)) {
    resType = TSDB_DATA_TYPE_UBIGINT;
  } else if (IS_FLOAT_TYPE(paraType)) {
    resType = TSDB_DATA_TYPE_DOUBLE;
  }
  pFunc->node.resType = (SDataType){.bytes = tDataTypes[resType].bytes, .type = resType};
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

  uint8_t para1Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  uint8_t para2Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type;
  if (!IS_NUMERIC_TYPE(para1Type) || (!IS_SIGNED_NUMERIC_TYPE(para2Type) && !IS_UNSIGNED_NUMERIC_TYPE(para2Type))) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};
  return TSDB_CODE_SUCCESS;
}

static bool validAperventileAlgo(const SValueNode* pVal) {
  if (TSDB_DATA_TYPE_BINARY != pVal->node.resType.type) {
    return false;
  }
  return (0 == strcasecmp(varDataVal(pVal->datum.p), "default") ||
          0 == strcasecmp(varDataVal(pVal->datum.p), "t-digest"));
}

static int32_t translateApercentile(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t paraNum = LIST_LENGTH(pFunc->pParameterList);
  if (2 != paraNum && 3 != paraNum) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t para1Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  uint8_t para2Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type;
  if (!IS_NUMERIC_TYPE(para1Type) || !IS_INTEGER_TYPE(para2Type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }
  if (3 == paraNum) {
    SNode* pPara3 = nodesListGetNode(pFunc->pParameterList, 2);
    if (QUERY_NODE_VALUE != nodeType(pPara3) || !validAperventileAlgo((SValueNode*)pPara3)) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                             "Third parameter algorithm of apercentile must be 'default' or 't-digest'");
    }
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateTbnameColumn(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  // pseudo column do not need to check parameters
  pFunc->node.resType =
      (SDataType){.bytes = TSDB_TABLE_FNAME_LEN - 1 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateTop(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  SDataType* pType = &((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType;
  pFunc->node.resType = (SDataType){.bytes = pType->bytes, .type = pType->type};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateBottom(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  SDataType* pType = &((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType;
  pFunc->node.resType = (SDataType){.bytes = pType->bytes, .type = pType->type};
  return TSDB_CODE_SUCCESS;
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

static int32_t translateLeastSQR(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (3 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  for (int32_t i = 0; i < numOfParams; ++i) {
    uint8_t colType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, i))->resType.type;
    if (!IS_NUMERIC_TYPE(colType)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
  }

  pFunc->node.resType = (SDataType) { .bytes = 64, .type = TSDB_DATA_TYPE_BINARY };
  return TSDB_CODE_SUCCESS;
}

static int32_t translateHistogram(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (4 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t colType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  if (!IS_NUMERIC_TYPE(colType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type != TSDB_DATA_TYPE_BINARY ||
      ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 2))->resType.type != TSDB_DATA_TYPE_BINARY ||
      ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 3))->resType.type != TSDB_DATA_TYPE_BIGINT) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = 512, .type = TSDB_DATA_TYPE_BINARY};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateLastRow(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  // todo
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

static int32_t translateDiff(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t paraLen = LIST_LENGTH(pFunc->pParameterList);
  if (paraLen == 0 || paraLen > 2) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  SExprNode* p1 = (SExprNode*)nodesListGetNode(pFunc->pParameterList, 0);
  if (!IS_SIGNED_NUMERIC_TYPE(p1->resType.type) && !IS_FLOAT_TYPE(p1->resType.type) &&
      TSDB_DATA_TYPE_BOOL != p1->resType.type) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }
  pFunc->node.resType = p1->resType;
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
  int32_t paraNum = LIST_LENGTH(pFunc->pParameterList);
  if (paraNum < minParaNum || paraNum > maxParaNum) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t resultType = TSDB_DATA_TYPE_BINARY;
  int32_t resultBytes = 0;
  int32_t sepBytes = 0;

  /* For concat/concat_ws function, if params have NCHAR type, promote the final result to NCHAR */
  for (int32_t i = 0; i < paraNum; ++i) {
    SNode*  pPara = nodesListGetNode(pFunc->pParameterList, i);
    uint8_t paraType = ((SExprNode*)pPara)->resType.type;
    if (!IS_VAR_DATA_TYPE(paraType)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
    if (TSDB_DATA_TYPE_NCHAR == paraType) {
      resultType = paraType;
    }
  }

  for (int32_t i = 0; i < paraNum; ++i) {
    SNode*  pPara = nodesListGetNode(pFunc->pParameterList, i);
    uint8_t paraType = ((SExprNode*)pPara)->resType.type;
    int32_t paraBytes = ((SExprNode*)pPara)->resType.bytes;
    int32_t factor = 1;
    if (TSDB_DATA_TYPE_NCHAR == resultType && TSDB_DATA_TYPE_VARCHAR == paraType) {
      factor *= TSDB_NCHAR_SIZE;
    }
    resultBytes += paraBytes * factor;

    if (i == 0) {
      sepBytes = paraBytes * factor;
    }
  }

  if (hasSep) {
    resultBytes += sepBytes * (paraNum - 3);
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
  int32_t paraNum = LIST_LENGTH(pFunc->pParameterList);
  if (2 != paraNum && 3 != paraNum) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  SExprNode* pPara1 = (SExprNode*)nodesListGetNode(pFunc->pParameterList, 0);
  uint8_t    para2Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type;
  if (!IS_VAR_DATA_TYPE(pPara1->resType.type) || !IS_INTEGER_TYPE(para2Type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }
  if (3 == paraNum) {
    uint8_t para3Type = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 1))->resType.type;
    if (!IS_INTEGER_TYPE(para3Type)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
  }

  pFunc->node.resType = (SDataType){.bytes = pPara1->resType.bytes, .type = pPara1->resType.type};
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
  if (para2Bytes <= 0 || para2Bytes > 1000) {  // cast dst var type length limits to 1000
    return invaildFuncParaValueErrMsg(pErrBuf, len, pFunc->functionName);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateToIso8601(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  if (!IS_INTEGER_TYPE(paraType) && TSDB_DATA_TYPE_TIMESTAMP != paraType) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

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
  int32_t paraNum = LIST_LENGTH(pFunc->pParameterList);
  if (2 != paraNum && 3 != paraNum) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  for (int32_t i = 0; i < 2; ++i) {
    uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, i))->resType.type;
    if (!IS_VAR_DATA_TYPE(paraType) && !IS_INTEGER_TYPE(paraType) && TSDB_DATA_TYPE_TIMESTAMP != paraType) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
  }

  if (3 == paraNum) {
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

  SExprNode* pPara = nodesListGetNode(pFunc->pParameterList, 0);
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
    .invertFunc   = countInvertFunction
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
    .invertFunc   = sumInvertFunction
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
    .finalizeFunc = minmaxFunctionFinalize
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
    .finalizeFunc = minmaxFunctionFinalize
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
    .invertFunc   = stddevInvertFunction
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
    .invertFunc   = leastSQRInvertFunction
  },
  {
    .name = "avg",
    .type = FUNCTION_TYPE_AVG,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateInNumOutDou,
    .getEnvFunc   = getAvgFuncEnv,
    .initFunc     = avgFunctionSetup,
    .processFunc  = avgFunction,
    .finalizeFunc = avgFinalize,
    .invertFunc   = avgInvertFunction
  },
  {
    .name = "percentile",
    .type = FUNCTION_TYPE_PERCENTILE,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_REPEAT_SCAN_FUNC,
    .translateFunc = translatePercentile,
    .getEnvFunc   = getPercentileFuncEnv,
    .initFunc     = percentileFunctionSetup,
    .processFunc  = percentileFunction,
    .finalizeFunc = percentileFinalize
  },
  {
    .name = "apercentile",
    .type = FUNCTION_TYPE_APERCENTILE,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateApercentile,
    .getEnvFunc   = getMinmaxFuncEnv,
    .initFunc     = minmaxFunctionSetup,
    .processFunc  = maxFunction,
    .finalizeFunc = functionFinalize
  },
  {
    .name = "top",
    .type = FUNCTION_TYPE_TOP,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC,
    .translateFunc = translateTop,
    .getEnvFunc   = getTopBotFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = topFunction,
    .finalizeFunc = topBotFinalize,
  },
  {
    .name = "bottom",
    .type = FUNCTION_TYPE_BOTTOM,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC,
    .translateFunc = translateBottom,
    .getEnvFunc   = getTopBotFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = bottomFunction,
    .finalizeFunc = topBotFinalize
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
    .finalizeFunc = spreadFinalize
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
    .finalizeFunc = functionFinalize
  },
  {
    .name = "last",
    .type = FUNCTION_TYPE_LAST,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_MULTI_RES_FUNC | FUNC_MGT_TIMELINE_FUNC,
    .translateFunc = translateFirstLast,
    .getEnvFunc   = getFirstLastFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = lastFunction,
    .finalizeFunc = functionFinalize
  },
  {
    .name = "diff",
    .type = FUNCTION_TYPE_DIFF,
    .classification = FUNC_MGT_NONSTANDARD_SQL_FUNC | FUNC_MGT_TIMELINE_FUNC,
    .translateFunc = translateDiff,
    .getEnvFunc   = getDiffFuncEnv,
    .initFunc     = diffFunctionSetup,
    .processFunc  = diffFunction,
    .finalizeFunc = functionFinalize
  },
  {
    .name = "histogram",
    .type = FUNCTION_TYPE_HISTOGRAM,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateHistogram,
    .getEnvFunc   = getHistogramFuncEnv,
    .initFunc     = histogramFunctionSetup,
    .processFunc  = histogramFunction,
    .finalizeFunc = histogramFinalize
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
    .translateFunc = translateIn2NumOutDou,
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
    .name = "_rowts",
    .type = FUNCTION_TYPE_ROWTS,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC,
    .translateFunc = translateTimePseudoColumn,
    .getEnvFunc   = getTimePseudoFuncEnv,
    .initFunc     = NULL,
    .sprocessFunc = NULL,
    .finalizeFunc = NULL
  },
  {
    .name = "_c0",
    .type = FUNCTION_TYPE_ROWTS,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC,
    .translateFunc = translateTimePseudoColumn,
    .getEnvFunc   = getTimePseudoFuncEnv,
    .initFunc     = NULL,
    .sprocessFunc = NULL,
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
    .sprocessFunc = NULL,
    .finalizeFunc = NULL
  }
};
// clang-format on

const int32_t funcMgtBuiltinsNum = (sizeof(funcMgtBuiltins) / sizeof(SBuiltinFuncDefinition));
