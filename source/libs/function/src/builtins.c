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
#include "cJSON.h"
#include "querynodes.h"
#include "scalar.h"
#include "geomFunc.h"
#include "taoserror.h"
#include "ttime.h"

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

#define TIME_UNIT_INVALID   1
#define TIME_UNIT_TOO_SMALL 2

static int32_t validateTimeUnitParam(uint8_t dbPrec, const SValueNode* pVal) {
  if (!pVal->isDuration) {
    return TIME_UNIT_INVALID;
  }

  if (TSDB_TIME_PRECISION_MILLI == dbPrec &&
      (0 == strcasecmp(pVal->literal, "1u") || 0 == strcasecmp(pVal->literal, "1b"))) {
    return TIME_UNIT_TOO_SMALL;
  }

  if (TSDB_TIME_PRECISION_MICRO == dbPrec && 0 == strcasecmp(pVal->literal, "1b")) {
    return TIME_UNIT_TOO_SMALL;
  }

  if (pVal->literal[0] != '1' ||
      (pVal->literal[1] != 'u' && pVal->literal[1] != 'a' && pVal->literal[1] != 's' && pVal->literal[1] != 'm' &&
       pVal->literal[1] != 'h' && pVal->literal[1] != 'd' && pVal->literal[1] != 'w' && pVal->literal[1] != 'b')) {
    return TIME_UNIT_INVALID;
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

static bool validateTimestampDigits(const SValueNode* pVal) {
  if (!IS_INTEGER_TYPE(pVal->node.resType.type)) {
    return false;
  }

  int64_t tsVal = pVal->datum.i;
  char    fraction[20] = {0};
  NUM_TO_STRING(pVal->node.resType.type, &tsVal, sizeof(fraction), fraction);
  int32_t tsDigits = (int32_t)strlen(fraction);

  if (tsDigits > TSDB_TIME_PRECISION_SEC_DIGITS) {
    if (tsDigits == TSDB_TIME_PRECISION_MILLI_DIGITS || tsDigits == TSDB_TIME_PRECISION_MICRO_DIGITS ||
        tsDigits == TSDB_TIME_PRECISION_NANO_DIGITS) {
      return true;
    } else {
      return false;
    }
  }

  return true;
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

static int32_t countTrailingSpaces(const SValueNode* pVal, bool isLtrim) {
  int32_t numOfSpaces = 0;
  int32_t len = varDataLen(pVal->datum.p);
  char*   str = varDataVal(pVal->datum.p);

  int32_t startPos = isLtrim ? 0 : len - 1;
  int32_t step = isLtrim ? 1 : -1;
  for (int32_t i = startPos; i < len || i >= 0; i += step) {
    if (!isspace(str[i])) {
      break;
    }
    numOfSpaces++;
  }

  return numOfSpaces;
}

static int32_t addTimezoneParam(SNodeList* pList) {
  char      buf[6] = {0};
  time_t    t = taosTime(NULL);
  struct tm tmInfo;
  if (taosLocalTime(&t, &tmInfo, buf) != NULL) {
    strftime(buf, sizeof(buf), "%z", &tmInfo);
  }
  int32_t len = (int32_t)strlen(buf);

  SValueNode* pVal = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  if (pVal == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

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
  return TSDB_CODE_SUCCESS;
}

static int32_t addDbPrecisonParam(SNodeList** pList, uint8_t precision) {
  SValueNode* pVal = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  if (pVal == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pVal->literal = NULL;
  pVal->isDuration = false;
  pVal->translate = true;
  pVal->notReserved = true;
  pVal->node.resType.type = TSDB_DATA_TYPE_TINYINT;
  pVal->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_TINYINT].bytes;
  pVal->node.resType.precision = precision;
  pVal->datum.i = (int64_t)precision;
  pVal->typeData = (int64_t)precision;

  nodesListMakeAppend(pList, (SNode*)pVal);
  return TSDB_CODE_SUCCESS;
}

static SDataType* getSDataTypeFromNode(SNode* pNode) {
  if (pNode == NULL) return NULL;
  if (nodesIsExprNode(pNode)) {
    return &((SExprNode*)pNode)->resType;
  } else if (QUERY_NODE_COLUMN_REF == pNode->type) {
    return &((SColumnRefNode*)pNode)->resType;
  } else {
    return NULL;
  }
}

// There is only one parameter of numeric type, and the return type is parameter type
static int32_t translateInOutNum(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
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

  uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
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

  uint8_t para1Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  uint8_t para2Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type;
  if ((!IS_NUMERIC_TYPE(para1Type) && !IS_NULL_TYPE(para1Type)) ||
      (!IS_NUMERIC_TYPE(para2Type) && !IS_NULL_TYPE(para2Type))) {
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

  SDataType* pRestType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0));
  if (TSDB_DATA_TYPE_VARBINARY == pRestType->type || !IS_STR_DATA_TYPE(pRestType->type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = pRestType->bytes, .type = pRestType->type};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateTrimStr(SFunctionNode* pFunc, char* pErrBuf, int32_t len, bool isLtrim) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  SDataType* pRestType1 = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0));
  if (TSDB_DATA_TYPE_VARBINARY == pRestType1->type || !IS_STR_DATA_TYPE(pRestType1->type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  int32_t numOfSpaces = 0;
  SNode*  pParamNode1 = nodesListGetNode(pFunc->pParameterList, 0);
  // for select trim functions with constant value from table,
  // need to set the proper result result schema bytes to avoid
  // trailing garbage characters
  if (nodeType(pParamNode1) == QUERY_NODE_VALUE) {
    SValueNode* pValue = (SValueNode*)pParamNode1;
    numOfSpaces = countTrailingSpaces(pValue, isLtrim);
  }

  int32_t resBytes = pRestType1->bytes - numOfSpaces;
  pFunc->node.resType = (SDataType){.bytes = resBytes, .type = pRestType1->type};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateLtrim(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateTrimStr(pFunc, pErrBuf, len, true);
}

static int32_t translateRtrim(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateTrimStr(pFunc, pErrBuf, len, false);
}

static int32_t translateLogarithm(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (1 != numOfParams && 2 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t para1Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  if (!IS_NUMERIC_TYPE(para1Type) && !IS_NULL_TYPE(para1Type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (2 == numOfParams) {
    uint8_t para2Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type;
    if (!IS_NUMERIC_TYPE(para2Type) && !IS_NULL_TYPE(para2Type)) {
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

  uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
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

  uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  if (!IS_NUMERIC_TYPE(paraType) && !IS_NULL_TYPE(paraType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = getAvgInfoSize() + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateAvgMiddle(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType.type;
  if (TSDB_DATA_TYPE_BINARY != paraType) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = getAvgInfoSize() + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateAvgMerge(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
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

  uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
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

  uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  if (TSDB_DATA_TYPE_BINARY != paraType) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};

  return TSDB_CODE_SUCCESS;
}

static int32_t translateWduration(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  // pseudo column do not need to check parameters
  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes, .type = TSDB_DATA_TYPE_BIGINT,
                                    .precision = pFunc->node.resType.precision};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateNowToday(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  // pseudo column do not need to check parameters

  // add database precision as param
  uint8_t dbPrec = pFunc->node.resType.precision;
  int32_t code = addDbPrecisonParam(&pFunc->pParameterList, dbPrec);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pFunc->node.resType =
      (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes, .type = TSDB_DATA_TYPE_TIMESTAMP};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateTimePseudoColumn(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  // pseudo column do not need to check parameters

  pFunc->node.resType =
      (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes, .type = TSDB_DATA_TYPE_TIMESTAMP,
                  .precision = pFunc->node.resType.precision};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateIsFilledPseudoColumn(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  // pseudo column do not need to check parameters

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes, .type = TSDB_DATA_TYPE_BOOL};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateTimezone(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  pFunc->node.resType = (SDataType){.bytes = TD_TIMEZONE_LEN, .type = TSDB_DATA_TYPE_BINARY};
  return TSDB_CODE_SUCCESS;
}

static int32_t translatePercentile(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (numOfParams < 2 || numOfParams > 11) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t para1Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  if (!IS_NUMERIC_TYPE(para1Type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  for (int32_t i = 1; i < numOfParams; ++i) {
    SValueNode* pValue = (SValueNode*)nodesListGetNode(pFunc->pParameterList, i);
    pValue->notReserved = true;

    uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, i))->type;
    if (!IS_NUMERIC_TYPE(paraType)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    double v = 0;
    if (IS_INTEGER_TYPE(paraType)) {
      v = (double)pValue->datum.i;
    } else {
      v = pValue->datum.d;
    }

    if (v < 0 || v > 100) {
      return invaildFuncParaValueErrMsg(pErrBuf, len, pFunc->functionName);
    }
  }

  // set result type
  if (numOfParams > 2) {
    pFunc->node.resType = (SDataType){.bytes = 512, .type = TSDB_DATA_TYPE_VARCHAR};
  } else {
    pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};
  }
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

  uint8_t para1Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  uint8_t para2Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type;
  if (!IS_NUMERIC_TYPE(para1Type) || !IS_INTEGER_TYPE(para2Type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // param2
  if (3 == numOfParams) {
    uint8_t para3Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 2))->type;
    if (!IS_STR_DATA_TYPE(para3Type)) {
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

    uint8_t para1Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
    uint8_t para2Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type;
    if (!IS_NUMERIC_TYPE(para1Type) || !IS_INTEGER_TYPE(para2Type)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    // param2
    if (3 == numOfParams) {
      uint8_t para3Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 2))->type;
      if (!IS_STR_DATA_TYPE(para3Type)) {
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
    // original percent param is reserved
    if (3 != numOfParams && 2 != numOfParams) {
      return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
    }
    uint8_t para1Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
    uint8_t para2Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type;
    if (TSDB_DATA_TYPE_BINARY != para1Type || !IS_INTEGER_TYPE(para2Type)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    if (3 == numOfParams) {
      uint8_t para3Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 2))->type;
      if (!IS_STR_DATA_TYPE(para3Type)) {
        return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
      }
      
      SNode* pParamNode2 = nodesListGetNode(pFunc->pParameterList, 2);
      if (QUERY_NODE_VALUE != nodeType(pParamNode2) || !validateApercentileAlgo((SValueNode*)pParamNode2)) {
        return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                               "Third parameter algorithm of apercentile must be 'default' or 't-digest'");
      }
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

static int32_t translateTbUidColumn(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  // pseudo column do not need to check parameters
  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes, .type = TSDB_DATA_TYPE_BIGINT};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateVgIdColumn(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  // pseudo column do not need to check parameters
  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes, .type = TSDB_DATA_TYPE_INT};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateVgVerColumn(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes, .type = TSDB_DATA_TYPE_BIGINT};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateTopBot(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (2 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t para1Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  uint8_t para2Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type;
  if (!IS_NUMERIC_TYPE(para1Type) || !IS_INTEGER_TYPE(para2Type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // param1
  SNode* pParamNode1 = nodesListGetNode(pFunc->pParameterList, 1);
  if (nodeType(pParamNode1) != QUERY_NODE_VALUE) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  SValueNode* pValue = (SValueNode*)pParamNode1;
  if (!IS_INTEGER_TYPE(pValue->node.resType.type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (pValue->datum.i < 1 || pValue->datum.i > TOP_BOTTOM_QUERY_LIMIT) {
    return invaildFuncParaValueErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pValue->notReserved = true;

  // set result type
  SDataType* pType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0));
  pFunc->node.resType = (SDataType){.bytes = pType->bytes, .type = pType->type};
  return TSDB_CODE_SUCCESS;
}

static int32_t reserveFirstMergeParam(SNodeList* pRawParameters, SNode* pPartialRes, SNodeList** pParameters) {
  int32_t code = nodesListMakeAppend(pParameters, pPartialRes);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListStrictAppend(*pParameters, nodesCloneNode(nodesListGetNode(pRawParameters, 1)));
  }
  return TSDB_CODE_SUCCESS;
}

int32_t topBotCreateMergeParam(SNodeList* pRawParameters, SNode* pPartialRes, SNodeList** pParameters) {
  return reserveFirstMergeParam(pRawParameters, pPartialRes, pParameters);
}

int32_t apercentileCreateMergeParam(SNodeList* pRawParameters, SNode* pPartialRes, SNodeList** pParameters) {
  int32_t code = reserveFirstMergeParam(pRawParameters, pPartialRes, pParameters);
  if (TSDB_CODE_SUCCESS == code && pRawParameters->length >= 3) {
    code = nodesListStrictAppend(*pParameters, nodesCloneNode(nodesListGetNode(pRawParameters, 2)));
  }
  return code;
}

static int32_t translateSpread(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  if (!IS_NUMERIC_TYPE(paraType) && !IS_TIMESTAMP_TYPE(paraType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateSpreadImpl(SFunctionNode* pFunc, char* pErrBuf, int32_t len, bool isPartial) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  if (isPartial) {
    if (!IS_NUMERIC_TYPE(paraType) && !IS_TIMESTAMP_TYPE(paraType)) {
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

  SNode* pPara1 = nodesListGetNode(pFunc->pParameterList, 0);
  if (QUERY_NODE_COLUMN != nodeType(pPara1) || PRIMARYKEY_TIMESTAMP_COL_ID != ((SColumnNode*)pPara1)->colId) {
    return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                           "The first parameter of the ELAPSED function can only be the timestamp primary key");
  }

  // param1
  if (2 == numOfParams) {
    SNode* pParamNode1 = nodesListGetNode(pFunc->pParameterList, 1);
    if (QUERY_NODE_VALUE != nodeType(pParamNode1)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    SValueNode* pValue = (SValueNode*)pParamNode1;

    pValue->notReserved = true;

    if (!IS_INTEGER_TYPE(pValue->node.resType.type)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    uint8_t dbPrec = pFunc->node.resType.precision;

    int32_t ret = validateTimeUnitParam(dbPrec, (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1));
    if (ret == TIME_UNIT_TOO_SMALL) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                             "ELAPSED function time unit parameter should be greater than db precision");
    } else if (ret == TIME_UNIT_INVALID) {
      return buildFuncErrMsg(
          pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
          "ELAPSED function time unit parameter should be one of the following: [1b, 1u, 1a, 1s, 1m, 1h, 1d, 1w]");
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

    uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
    if (!IS_TIMESTAMP_TYPE(paraType)) {
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

      paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type;
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

    uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
    if (TSDB_DATA_TYPE_BINARY != paraType) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
    pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateElapsedPartial(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
#if 0
  return translateElapsedImpl(pFunc, pErrBuf, len, true);
#endif
  return 0;
}

static int32_t translateElapsedMerge(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
#if 0
  return translateElapsedImpl(pFunc, pErrBuf, len, false);
#endif
  return 0;
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

    uint8_t colType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, i))->type;
    if (!IS_NUMERIC_TYPE(colType)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
  }

  pFunc->node.resType = (SDataType){.bytes = LEASTSQUARES_BUFF_LENGTH, .type = TSDB_DATA_TYPE_BINARY};
  return TSDB_CODE_SUCCESS;
}

typedef enum { UNKNOWN_BIN = 0, USER_INPUT_BIN, LINEAR_BIN, LOG_BIN } EHistoBinType;

static int8_t validateHistogramBinType(char* binTypeStr) {
  int8_t binType;
  if (strcasecmp(binTypeStr, "user_input") == 0) {
    binType = USER_INPUT_BIN;
  } else if (strcasecmp(binTypeStr, "linear_bin") == 0) {
    binType = LINEAR_BIN;
  } else if (strcasecmp(binTypeStr, "log_bin") == 0) {
    binType = LOG_BIN;
  } else {
    binType = UNKNOWN_BIN;
  }

  return binType;
}

static bool validateHistogramBinDesc(char* binDescStr, int8_t binType, char* errMsg, int32_t msgLen) {
  const char* msg1 = "HISTOGRAM function requires four parameters";
  const char* msg3 = "HISTOGRAM function invalid format for binDesc parameter";
  const char* msg4 = "HISTOGRAM function binDesc parameter \"count\" should be in range [1, 1000]";
  const char* msg5 = "HISTOGRAM function bin/parameter should be in range [-DBL_MAX, DBL_MAX]";
  const char* msg6 = "HISTOGRAM function binDesc parameter \"width\" cannot be 0";
  const char* msg7 = "HISTOGRAM function binDesc parameter \"start\" cannot be 0 with \"log_bin\" type";
  const char* msg8 = "HISTOGRAM function binDesc parameter \"factor\" cannot be negative or equal to 0/1";

  cJSON*  binDesc = cJSON_Parse(binDescStr);
  int32_t numOfBins;
  double* intervals;
  if (cJSON_IsObject(binDesc)) { /* linaer/log bins */
    int32_t numOfParams = cJSON_GetArraySize(binDesc);
    int32_t startIndex;
    if (numOfParams != 4) {
      snprintf(errMsg, msgLen, "%s", msg1);
      cJSON_Delete(binDesc);
      return false;
    }

    cJSON* start = cJSON_GetObjectItem(binDesc, "start");
    cJSON* factor = cJSON_GetObjectItem(binDesc, "factor");
    cJSON* width = cJSON_GetObjectItem(binDesc, "width");
    cJSON* count = cJSON_GetObjectItem(binDesc, "count");
    cJSON* infinity = cJSON_GetObjectItem(binDesc, "infinity");

    if (!cJSON_IsNumber(start) || !cJSON_IsNumber(count) || !cJSON_IsBool(infinity)) {
      snprintf(errMsg, msgLen, "%s", msg3);
      cJSON_Delete(binDesc);
      return false;
    }

    if (count->valueint <= 0 || count->valueint > 1000) {  // limit count to 1000
      snprintf(errMsg, msgLen, "%s", msg4);
      cJSON_Delete(binDesc);
      return false;
    }

    if (isinf(start->valuedouble) || (width != NULL && isinf(width->valuedouble)) ||
        (factor != NULL && isinf(factor->valuedouble)) || (count != NULL && isinf(count->valuedouble))) {
      snprintf(errMsg, msgLen, "%s", msg5);
      cJSON_Delete(binDesc);
      return false;
    }

    int32_t counter = (int32_t)count->valueint;
    if (infinity->valueint == false) {
      startIndex = 0;
      numOfBins = counter + 1;
    } else {
      startIndex = 1;
      numOfBins = counter + 3;
    }

    intervals = taosMemoryCalloc(numOfBins, sizeof(double));
    if (cJSON_IsNumber(width) && factor == NULL && binType == LINEAR_BIN) {
      // linear bin process
      if (width->valuedouble == 0) {
        snprintf(errMsg, msgLen, "%s", msg6);
        taosMemoryFree(intervals);
        cJSON_Delete(binDesc);
        return false;
      }
      for (int i = 0; i < counter + 1; ++i) {
        intervals[startIndex] = start->valuedouble + i * width->valuedouble;
        if (isinf(intervals[startIndex])) {
          snprintf(errMsg, msgLen, "%s", msg5);
          taosMemoryFree(intervals);
          cJSON_Delete(binDesc);
          return false;
        }
        startIndex++;
      }
    } else if (cJSON_IsNumber(factor) && width == NULL && binType == LOG_BIN) {
      // log bin process
      if (start->valuedouble == 0) {
        snprintf(errMsg, msgLen, "%s", msg7);
        taosMemoryFree(intervals);
        cJSON_Delete(binDesc);
        return false;
      }
      if (factor->valuedouble < 0 || factor->valuedouble == 0 || factor->valuedouble == 1) {
        snprintf(errMsg, msgLen, "%s", msg8);
        taosMemoryFree(intervals);
        cJSON_Delete(binDesc);
        return false;
      }
      for (int i = 0; i < counter + 1; ++i) {
        intervals[startIndex] = start->valuedouble * pow(factor->valuedouble, i * 1.0);
        if (isinf(intervals[startIndex])) {
          snprintf(errMsg, msgLen, "%s", msg5);
          taosMemoryFree(intervals);
          cJSON_Delete(binDesc);
          return false;
        }
        startIndex++;
      }
    } else {
      snprintf(errMsg, msgLen, "%s", msg3);
      taosMemoryFree(intervals);
      cJSON_Delete(binDesc);
      return false;
    }

    if (infinity->valueint == true) {
      intervals[0] = -INFINITY;
      intervals[numOfBins - 1] = INFINITY;
      // in case of desc bin orders, -inf/inf should be swapped
      if (numOfBins < 4) {
        return false;
      }

      if (intervals[1] > intervals[numOfBins - 2]) {
        TSWAP(intervals[0], intervals[numOfBins - 1]);
      }
    }
  } else if (cJSON_IsArray(binDesc)) { /* user input bins */
    if (binType != USER_INPUT_BIN) {
      snprintf(errMsg, msgLen, "%s", msg3);
      cJSON_Delete(binDesc);
      return false;
    }
    numOfBins = cJSON_GetArraySize(binDesc);
    intervals = taosMemoryCalloc(numOfBins, sizeof(double));
    cJSON* bin = binDesc->child;
    if (bin == NULL) {
      snprintf(errMsg, msgLen, "%s", msg3);
      taosMemoryFree(intervals);
      cJSON_Delete(binDesc);
      return false;
    }
    int i = 0;
    while (bin) {
      intervals[i] = bin->valuedouble;
      if (!cJSON_IsNumber(bin)) {
        snprintf(errMsg, msgLen, "%s", msg3);
        taosMemoryFree(intervals);
        cJSON_Delete(binDesc);
        return false;
      }
      if (i != 0 && intervals[i] <= intervals[i - 1]) {
        snprintf(errMsg, msgLen, "%s", msg3);
        taosMemoryFree(intervals);
        cJSON_Delete(binDesc);
        return false;
      }
      bin = bin->next;
      i++;
    }
  } else {
    snprintf(errMsg, msgLen, "%s", msg3);
    cJSON_Delete(binDesc);
    return false;
  }

  cJSON_Delete(binDesc);
  taosMemoryFree(intervals);
  return true;
}

static int32_t translateHistogram(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (4 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t colType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  if (!IS_NUMERIC_TYPE(colType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // param1 ~ param3
  if (getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type != TSDB_DATA_TYPE_BINARY ||
      getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 2))->type != TSDB_DATA_TYPE_BINARY ||
      !IS_INTEGER_TYPE(getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 3))->type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  int8_t binType;
  char*  binDesc;
  for (int32_t i = 1; i < numOfParams; ++i) {
    SNode* pParamNode = nodesListGetNode(pFunc->pParameterList, i);
    if (QUERY_NODE_VALUE != nodeType(pParamNode)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    SValueNode* pValue = (SValueNode*)pParamNode;

    pValue->notReserved = true;

    if (i == 1) {
      binType = validateHistogramBinType(varDataVal(pValue->datum.p));
      if (binType == UNKNOWN_BIN) {
        return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                               "HISTOGRAM function binType parameter should be "
                               "\"user_input\", \"log_bin\" or \"linear_bin\"");
      }
    }

    if (i == 2) {
      char errMsg[128] = {0};
      binDesc = varDataVal(pValue->datum.p);
      if (!validateHistogramBinDesc(binDesc, binType, errMsg, (int32_t)sizeof(errMsg))) {
        return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR, errMsg);
      }
    }

    if (i == 3 && pValue->datum.i != 1 && pValue->datum.i != 0) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                             "HISTOGRAM function normalized parameter should be 0/1");
    }
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

    uint8_t colType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
    if (!IS_NUMERIC_TYPE(colType)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    // param1 ~ param3
    if (getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type != TSDB_DATA_TYPE_BINARY ||
        getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 2))->type != TSDB_DATA_TYPE_BINARY ||
        !IS_INTEGER_TYPE(getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 3))->type)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    int8_t binType;
    char*  binDesc;
    for (int32_t i = 1; i < numOfParams; ++i) {
      SNode* pParamNode = nodesListGetNode(pFunc->pParameterList, i);
      if (QUERY_NODE_VALUE != nodeType(pParamNode)) {
        return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
      }

      SValueNode* pValue = (SValueNode*)pParamNode;

      pValue->notReserved = true;

      if (i == 1) {
        binType = validateHistogramBinType(varDataVal(pValue->datum.p));
        if (binType == UNKNOWN_BIN) {
          return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                                 "HISTOGRAM function binType parameter should be "
                                 "\"user_input\", \"log_bin\" or \"linear_bin\"");
        }
      }

      if (i == 2) {
        char errMsg[128] = {0};
        binDesc = varDataVal(pValue->datum.p);
        if (!validateHistogramBinDesc(binDesc, binType, errMsg, (int32_t)sizeof(errMsg))) {
          return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR, errMsg);
        }
      }

      if (i == 3 && pValue->datum.i != 1 && pValue->datum.i != 0) {
        return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                               "HISTOGRAM function normalized parameter should be 0/1");
      }
    }

    pFunc->node.resType =
        (SDataType){.bytes = getHistogramInfoSize() + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY};
  } else {
    if (1 != numOfParams) {
      return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
    }

    if (getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type != TSDB_DATA_TYPE_BINARY) {
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
  if (strlen(varDataVal(pVal->datum.p)) == 2) {
    return (
        0 == strncasecmp(varDataVal(pVal->datum.p), "GT", 2) || 0 == strncasecmp(varDataVal(pVal->datum.p), "GE", 2) ||
        0 == strncasecmp(varDataVal(pVal->datum.p), "LT", 2) || 0 == strncasecmp(varDataVal(pVal->datum.p), "LE", 2) ||
        0 == strncasecmp(varDataVal(pVal->datum.p), "EQ", 2) || 0 == strncasecmp(varDataVal(pVal->datum.p), "NE", 2));
  }
  return false;
}

static int32_t translateStateCount(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (3 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t colType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
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

  if (getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type != TSDB_DATA_TYPE_BINARY ||
      (getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 2))->type != TSDB_DATA_TYPE_BIGINT &&
       getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 2))->type != TSDB_DATA_TYPE_DOUBLE)) {
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

  uint8_t colType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
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

  if (getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type != TSDB_DATA_TYPE_BINARY ||
      (getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 2))->type != TSDB_DATA_TYPE_BIGINT &&
       getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 2))->type != TSDB_DATA_TYPE_DOUBLE)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (numOfParams == 4 &&
      getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 3))->type != TSDB_DATA_TYPE_BIGINT) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (numOfParams == 4) {
    uint8_t dbPrec = pFunc->node.resType.precision;

    int32_t ret = validateTimeUnitParam(dbPrec, (SValueNode*)nodesListGetNode(pFunc->pParameterList, 3));
    if (ret == TIME_UNIT_TOO_SMALL) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                             "STATEDURATION function time unit parameter should be greater than db precision");
    } else if (ret == TIME_UNIT_INVALID) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                             "STATEDURATION function time unit parameter should be one of the following: [1b, 1u, 1a, "
                             "1s, 1m, 1h, 1d, 1w]");
    }
  }

  // set result type
  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes, .type = TSDB_DATA_TYPE_BIGINT};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateCsum(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t colType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
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
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[resType].bytes, .type = resType};
  return TSDB_CODE_SUCCESS;
}

static EFuncReturnRows csumEstReturnRows(SFunctionNode* pFunc) { return FUNC_RETURN_ROWS_N; }

static int32_t translateMavg(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (2 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t colType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
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

  uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type;
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

  SDataType* pSDataType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0));
  uint8_t    colType = pSDataType->type;

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

  uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type;
  if (!IS_INTEGER_TYPE(paraType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // set result type
  if (IS_STR_DATA_TYPE(colType)) {
    pFunc->node.resType = (SDataType){.bytes = pSDataType->bytes, .type = colType};
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

  SDataType* pSDataType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0));
  uint8_t    colType = pSDataType->type;

  // param1 & param2
  for (int32_t i = 1; i < numOfParams; ++i) {
    SNode* pParamNode = nodesListGetNode(pFunc->pParameterList, i);
    if (QUERY_NODE_VALUE != nodeType(pParamNode)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    SValueNode* pValue = (SValueNode*)pParamNode;

    if ((IS_SIGNED_NUMERIC_TYPE(pValue->node.resType.type) ? pValue->datum.i : pValue->datum.u) < ((i > 1) ? 0 : 1) ||
        (IS_SIGNED_NUMERIC_TYPE(pValue->node.resType.type) ? pValue->datum.i : pValue->datum.u) > 100) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                             "TAIL function second parameter should be in range [1, 100], "
                             "third parameter should be in range [0, 100]");
    }

    pValue->notReserved = true;

    uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, i))->type;
    if (!IS_INTEGER_TYPE(paraType)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
  }

  // set result type
  if (IS_STR_DATA_TYPE(colType)) {
    pFunc->node.resType = (SDataType){.bytes = pSDataType->bytes, .type = colType};
  } else {
    pFunc->node.resType = (SDataType){.bytes = tDataTypes[colType].bytes, .type = colType};
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateDerivative(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (3 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t colType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;

  // param1
  SNode*      pParamNode1 = nodesListGetNode(pFunc->pParameterList, 1);
  SValueNode* pValue1 = (SValueNode*)pParamNode1;
  if (QUERY_NODE_VALUE != nodeType(pParamNode1)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (pValue1->datum.i <= 0) {
    return invaildFuncParaValueErrMsg(pErrBuf, len, pFunc->functionName);
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

static EFuncReturnRows derivativeEstReturnRows(SFunctionNode* pFunc) {
  return 1 == ((SValueNode*)nodesListGetNode(pFunc->pParameterList, 2))->datum.i ? FUNC_RETURN_ROWS_INDEFINITE
                                                                                 : FUNC_RETURN_ROWS_N_MINUS_1;
}

static int32_t translateIrate(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t colType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;

  if (!IS_NUMERIC_TYPE(colType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // add database precision as param
  uint8_t dbPrec = pFunc->node.resType.precision;
  int32_t code = addDbPrecisonParam(&pFunc->pParameterList, dbPrec);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateIrateImpl(SFunctionNode* pFunc, char* pErrBuf, int32_t len, bool isPartial) {
  uint8_t colType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  if (isPartial) {
    if (3 != LIST_LENGTH(pFunc->pParameterList)) {
      return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
    }
    if (!IS_NUMERIC_TYPE(colType)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
    pFunc->node.resType = (SDataType){.bytes = getIrateInfoSize() + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY};
  } else {
    if (1 != LIST_LENGTH(pFunc->pParameterList)) {
      return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
    }
    if (TSDB_DATA_TYPE_BINARY != colType) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
    pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes, .type = TSDB_DATA_TYPE_DOUBLE};

    // add database precision as param
    uint8_t dbPrec = pFunc->node.resType.precision;
    int32_t code = addDbPrecisonParam(&pFunc->pParameterList, dbPrec);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }


  return TSDB_CODE_SUCCESS;
}

static int32_t translateIratePartial(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateIrateImpl(pFunc, pErrBuf, len, true);
}

static int32_t translateIrateMerge(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateIrateImpl(pFunc, pErrBuf, len, false);
}

static int32_t translateInterp(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  uint8_t dbPrec = pFunc->node.resType.precision;

  if (2 < numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t nodeType = nodeType(nodesListGetNode(pFunc->pParameterList, 0));
  uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  if ((!IS_NUMERIC_TYPE(paraType) && !IS_BOOLEAN_TYPE(paraType)) || QUERY_NODE_VALUE == nodeType) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (2 == numOfParams) {
    nodeType = nodeType(nodesListGetNode(pFunc->pParameterList, 1));
    paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type;
    if (!IS_INTEGER_TYPE(paraType) || QUERY_NODE_VALUE != nodeType) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    SValueNode* pValue = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);
    if (pValue->datum.i != 0 && pValue->datum.i != 1) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                             "INTERP function second parameter should be 0/1");
    }

    pValue->notReserved = true;
  }

#if 0
  if (3 <= numOfParams) {
    int64_t timeVal[2] = {0};
    for (int32_t i = 1; i < 3; ++i) {
      nodeType = nodeType(nodesListGetNode(pFunc->pParameterList, i));
      paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, i))->resType.type;
      if (!IS_STR_DATA_TYPE(paraType) || QUERY_NODE_VALUE != nodeType) {
        return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
      }

      SValueNode* pValue = (SValueNode*)nodesListGetNode(pFunc->pParameterList, i);
      int32_t     ret = convertStringToTimestamp(paraType, pValue->datum.p, dbPrec, &timeVal[i - 1]);
      if (ret != TSDB_CODE_SUCCESS) {
        return invaildFuncParaValueErrMsg(pErrBuf, len, pFunc->functionName);
      }
    }

    if (timeVal[0] > timeVal[1]) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR, "INTERP function invalid time range");
    }
  }

  if (4 == numOfParams) {
    nodeType = nodeType(nodesListGetNode(pFunc->pParameterList, 3));
    paraType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 3))->resType.type;
    if (!IS_INTEGER_TYPE(paraType) || QUERY_NODE_VALUE != nodeType) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    int32_t ret = validateTimeUnitParam(dbPrec, (SValueNode*)nodesListGetNode(pFunc->pParameterList, 3));
    if (ret == TIME_UNIT_TOO_SMALL) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                             "INTERP function time interval parameter should be greater than db precision");
    } else if (ret == TIME_UNIT_INVALID) {
      return buildFuncErrMsg(
          pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
          "INTERP function time interval parameter should be one of the following: [1b, 1u, 1a, 1s, 1m, 1h, 1d, 1w]");
    }
  }
#endif

  pFunc->node.resType = ((SExprNode*)nodesListGetNode(pFunc->pParameterList, 0))->resType;
  return TSDB_CODE_SUCCESS;
}

static EFuncReturnRows interpEstReturnRows(SFunctionNode* pFunc) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (1 < numOfParams && 1 == ((SValueNode*)nodesListGetNode(pFunc->pParameterList, 1))->datum.i) {
    return FUNC_RETURN_ROWS_INDEFINITE;
  } else {
    return FUNC_RETURN_ROWS_N;
  }
}

static int32_t translateFirstLast(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  // forbid null as first/last input, since first(c0, null, 1) may have different number of input
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);

  for (int32_t i = 0; i < numOfParams; ++i) {
    uint8_t nodeType = nodeType(nodesListGetNode(pFunc->pParameterList, i));
    uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, i))->type;
    if (IS_NULL_TYPE(paraType) && QUERY_NODE_VALUE == nodeType) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
  }

  pFunc->node.resType = *getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0));
  return TSDB_CODE_SUCCESS;
}

static int32_t translateFirstLastImpl(SFunctionNode* pFunc, char* pErrBuf, int32_t len, bool isPartial) {
  // first(col_list) will be rewritten as first(col)
  SNode*  pPara = nodesListGetNode(pFunc->pParameterList, 0);
  uint8_t paraType = getSDataTypeFromNode(pPara)->type;
  int32_t paraBytes = getSDataTypeFromNode(pPara)->bytes;
  if (isPartial) {
    int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
    for (int32_t i = 0; i < numOfParams; ++i) {
      uint8_t nodeType = nodeType(nodesListGetNode(pFunc->pParameterList, i));
      uint8_t pType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, i))->type;
      if (IS_NULL_TYPE(pType) && QUERY_NODE_VALUE == nodeType) {
        return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
      }
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

static int32_t translateUniqueMode(SFunctionNode* pFunc, char* pErrBuf, int32_t len, bool isUnique) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  SNode* pPara = nodesListGetNode(pFunc->pParameterList, 0);
  if (!nodesExprHasColumn(pPara)) {
    return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR, "The parameters of %s must contain columns",
                           isUnique ? "UNIQUE" : "MODE");
  }

  pFunc->node.resType = ((SExprNode*)pPara)->resType;
  return TSDB_CODE_SUCCESS;
}

static int32_t translateUnique(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateUniqueMode(pFunc, pErrBuf, len, true);
}

static int32_t translateMode(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  return translateUniqueMode(pFunc, pErrBuf, len, false);
}

static int32_t translateDiff(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (numOfParams > 2) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t colType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  if (!IS_INTEGER_TYPE(colType) && !IS_FLOAT_TYPE(colType) && TSDB_DATA_TYPE_BOOL != colType &&
      !IS_TIMESTAMP_TYPE(colType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // param1
  if (numOfParams == 2) {
    uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type;
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
  if (IS_SIGNED_NUMERIC_TYPE(colType) || IS_TIMESTAMP_TYPE(colType) || TSDB_DATA_TYPE_BOOL == colType) {
    resType = TSDB_DATA_TYPE_BIGINT;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(colType)) {
    resType = TSDB_DATA_TYPE_UBIGINT;
  } else {
    resType = TSDB_DATA_TYPE_DOUBLE;
  }
  pFunc->node.resType = (SDataType){.bytes = tDataTypes[resType].bytes, .type = resType};
  return TSDB_CODE_SUCCESS;
}

static EFuncReturnRows diffEstReturnRows(SFunctionNode* pFunc) {
  if (1 == LIST_LENGTH(pFunc->pParameterList)) {
    return FUNC_RETURN_ROWS_N_MINUS_1;
  }
  return 1 == ((SValueNode*)nodesListGetNode(pFunc->pParameterList, 1))->datum.i ? FUNC_RETURN_ROWS_INDEFINITE
                                                                                 : FUNC_RETURN_ROWS_N_MINUS_1;
}

static int32_t translateLength(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (!IS_STR_DATA_TYPE(getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type)) {
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
    uint8_t paraType = getSDataTypeFromNode(pPara)->type;
    if (TSDB_DATA_TYPE_VARBINARY == paraType) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    if (!IS_STR_DATA_TYPE(paraType) && !IS_NULL_TYPE(paraType)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
    if (TSDB_DATA_TYPE_NCHAR == paraType) {
      resultType = paraType;
    }
  }

  for (int32_t i = 0; i < numOfParams; ++i) {
    SNode*  pPara = nodesListGetNode(pFunc->pParameterList, i);
    uint8_t paraType = getSDataTypeFromNode(pPara)->type;
    int32_t paraBytes = getSDataTypeFromNode(pPara)->bytes;
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
  SExprNode* pPara1 = (SExprNode*)nodesListGetNode(pFunc->pParameterList, 1);

  uint8_t para0Type = pPara0->resType.type;
  uint8_t para1Type = pPara1->resType.type;
  if (TSDB_DATA_TYPE_VARBINARY == para0Type || !IS_STR_DATA_TYPE(para0Type) || !IS_INTEGER_TYPE(para1Type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (((SValueNode*)pPara1)->datum.i == 0) {
    return invaildFuncParaValueErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (3 == numOfParams) {
    SExprNode* pPara2 = (SExprNode*)nodesListGetNode(pFunc->pParameterList, 2);
    uint8_t    para2Type = pPara2->resType.type;
    if (!IS_INTEGER_TYPE(para2Type)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    int64_t v = ((SValueNode*)pPara2)->datum.i;
    if (v < 0) {
      return invaildFuncParaValueErrMsg(pErrBuf, len, pFunc->functionName);
    }
  }

  pFunc->node.resType = (SDataType){.bytes = pPara0->resType.bytes, .type = pPara0->resType.type};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateCast(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  // The number of parameters has been limited by the syntax definition

  SExprNode* pPara0 = (SExprNode*)nodesListGetNode(pFunc->pParameterList, 0);
  uint8_t para0Type = pPara0->resType.type;
  if (TSDB_DATA_TYPE_VARBINARY == para0Type) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // The function return type has been set during syntax parsing
  uint8_t para2Type = pFunc->node.resType.type;

  int32_t para2Bytes = pFunc->node.resType.bytes;
  if (IS_STR_DATA_TYPE(para2Type)) {
    para2Bytes -= VARSTR_HEADER_SIZE;
  }
  if (para2Bytes <= 0 || para2Bytes > 4096) {  // cast dst var type length limits to 4096 bytes
    return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                           "CAST function converted length should be in range (0, 4096] bytes");
  }

  // add database precision as param
  uint8_t dbPrec = pFunc->node.resType.precision;
  int32_t code = addDbPrecisonParam(&pFunc->pParameterList, dbPrec);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t translateToIso8601(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (1 != numOfParams && 2 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  // param0
  uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  if (!IS_INTEGER_TYPE(paraType) && !IS_TIMESTAMP_TYPE(paraType)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (QUERY_NODE_VALUE == nodeType(nodesListGetNode(pFunc->pParameterList, 0))) {
    SValueNode* pValue = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 0);

    if (!validateTimestampDigits(pValue)) {
      pFunc->node.resType = (SDataType){.bytes = 0, .type = TSDB_DATA_TYPE_BINARY};
      return TSDB_CODE_SUCCESS;
    }
  }

  // param1
  if (numOfParams == 2) {
    SValueNode* pValue = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);

    if (!validateTimezoneFormat(pValue)) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR, "Invalid timzone format");
    }
  } else {  // add default client timezone
    int32_t code = addTimezoneParam(pFunc->pParameterList);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  // set result type
  pFunc->node.resType = (SDataType){.bytes = 64, .type = TSDB_DATA_TYPE_BINARY};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateToUnixtimestamp(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  int16_t resType = TSDB_DATA_TYPE_BIGINT;

  if (1 != numOfParams && 2 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t para1Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  if (para1Type == TSDB_DATA_TYPE_VARBINARY || !IS_STR_DATA_TYPE(para1Type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  if (2 == numOfParams) {
    uint8_t para2Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type;
    if (!IS_INTEGER_TYPE(para2Type)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }

    SValueNode* pValue = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);
    if (pValue->datum.i == 1) {
      resType = TSDB_DATA_TYPE_TIMESTAMP;
    } else if (pValue->datum.i == 0) {
      resType = TSDB_DATA_TYPE_BIGINT;
    } else {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                             "TO_UNIXTIMESTAMP function second parameter should be 0/1");
    }
  }

  // add database precision as param
  uint8_t dbPrec = pFunc->node.resType.precision;
  int32_t code = addDbPrecisonParam(&pFunc->pParameterList, dbPrec);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[resType].bytes, .type = resType};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateToTimestamp(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (LIST_LENGTH(pFunc->pParameterList) != 2) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }
  uint8_t para1Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  uint8_t para2Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type;
  if (!IS_STR_DATA_TYPE(para1Type) || !IS_STR_DATA_TYPE(para2Type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }
  pFunc->node.resType =
      (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes, .type = TSDB_DATA_TYPE_TIMESTAMP};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateToChar(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (LIST_LENGTH(pFunc->pParameterList) != 2) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }
  uint8_t para1Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  uint8_t para2Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type;
  // currently only support to_char(timestamp, str)
  if (!IS_STR_DATA_TYPE(para2Type) || !IS_TIMESTAMP_TYPE(para1Type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }
  pFunc->node.resType = (SDataType){.bytes = 4096, .type = TSDB_DATA_TYPE_VARCHAR};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateTimeTruncate(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  int32_t numOfParams = LIST_LENGTH(pFunc->pParameterList);
  if (2 != numOfParams && 3 != numOfParams) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t para1Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  uint8_t para2Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type;
  if ((!IS_STR_DATA_TYPE(para1Type) && !IS_INTEGER_TYPE(para1Type) && !IS_TIMESTAMP_TYPE(para1Type)) ||
      !IS_INTEGER_TYPE(para2Type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t dbPrec = pFunc->node.resType.precision;
  int32_t ret = validateTimeUnitParam(dbPrec, (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1));
  if (ret == TIME_UNIT_TOO_SMALL) {
    return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                           "TIMETRUNCATE function time unit parameter should be greater than db precision");
  } else if (ret == TIME_UNIT_INVALID) {
    return buildFuncErrMsg(
        pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
        "TIMETRUNCATE function time unit parameter should be one of the following: [1b, 1u, 1a, 1s, 1m, 1h, 1d, 1w]");
  }

  if (3 == numOfParams) {
    uint8_t para3Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 2))->type;
    if (!IS_INTEGER_TYPE(para3Type)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
    SValueNode* pValue = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 2);
    if (pValue->datum.i != 0 && pValue->datum.i != 1) {
      return invaildFuncParaValueErrMsg(pErrBuf, len, pFunc->functionName);
    }
  }

  // add database precision as param

  int32_t code = addDbPrecisonParam(&pFunc->pParameterList, dbPrec);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // add client timezone as param
  code = addTimezoneParam(pFunc->pParameterList);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
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
    uint8_t paraType = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, i))->type;
    if (!IS_STR_DATA_TYPE(paraType) && !IS_INTEGER_TYPE(paraType) && !IS_TIMESTAMP_TYPE(paraType)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
  }

  if (3 == numOfParams) {
    if (!IS_INTEGER_TYPE(getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 2))->type)) {
      return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
    }
  }

  // add database precision as param
  uint8_t dbPrec = pFunc->node.resType.precision;

  if (3 == numOfParams) {
    int32_t ret = validateTimeUnitParam(dbPrec, (SValueNode*)nodesListGetNode(pFunc->pParameterList, 2));
    if (ret == TIME_UNIT_TOO_SMALL) {
      return buildFuncErrMsg(pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
                             "TIMEDIFF function time unit parameter should be greater than db precision");
    } else if (ret == TIME_UNIT_INVALID) {
      return buildFuncErrMsg(
          pErrBuf, len, TSDB_CODE_FUNC_FUNTION_ERROR,
          "TIMEDIFF function time unit parameter should be one of the following: [1b, 1u, 1a, 1s, 1m, 1h, 1d, 1w]");
    }
  }

  int32_t code = addDbPrecisonParam(&pFunc->pParameterList, dbPrec);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes, .type = TSDB_DATA_TYPE_BIGINT};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateToJson(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  SExprNode* pPara = (SExprNode*)nodesListGetNode(pFunc->pParameterList, 0);
  if (QUERY_NODE_VALUE != nodeType(pPara) || TSDB_DATA_TYPE_VARBINARY == pPara->resType.type || (!IS_VAR_DATA_TYPE(pPara->resType.type))) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_JSON].bytes, .type = TSDB_DATA_TYPE_JSON};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateInStrOutGeom(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t para1Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  if (!IS_STR_DATA_TYPE(para1Type) && !IS_NULL_TYPE(para1Type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_GEOMETRY].bytes, .type = TSDB_DATA_TYPE_GEOMETRY};

  return TSDB_CODE_SUCCESS;
}

static int32_t translateInGeomOutStr(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t para1Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  if (para1Type != TSDB_DATA_TYPE_GEOMETRY && !IS_NULL_TYPE(para1Type)) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_VARCHAR].bytes, .type = TSDB_DATA_TYPE_VARCHAR};

  return TSDB_CODE_SUCCESS;
}

static int32_t translateIn2NumOutGeom(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (2 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t para1Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  uint8_t para2Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type;
  if ((!IS_NUMERIC_TYPE(para1Type) && !IS_NULL_TYPE(para1Type)) ||
      (!IS_NUMERIC_TYPE(para2Type) && !IS_NULL_TYPE(para2Type))) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_GEOMETRY].bytes, .type = TSDB_DATA_TYPE_GEOMETRY};

  return TSDB_CODE_SUCCESS;
}

static int32_t translateIn2GeomOutBool(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (2 != LIST_LENGTH(pFunc->pParameterList)) {
    return invaildFuncParaNumErrMsg(pErrBuf, len, pFunc->functionName);
  }

  uint8_t para1Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 0))->type;
  uint8_t para2Type = getSDataTypeFromNode(nodesListGetNode(pFunc->pParameterList, 1))->type;
  if ((para1Type != TSDB_DATA_TYPE_GEOMETRY && !IS_NULL_TYPE(para1Type)) ||
      (para2Type != TSDB_DATA_TYPE_GEOMETRY && !IS_NULL_TYPE(para2Type))) {
    return invaildFuncParaTypeErrMsg(pErrBuf, len, pFunc->functionName);
  }

  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes, .type = TSDB_DATA_TYPE_BOOL};

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

static int32_t translateBlockDistInfoFunc(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  pFunc->node.resType = (SDataType){.bytes = 128, .type = TSDB_DATA_TYPE_VARCHAR};
  return TSDB_CODE_SUCCESS;
}

static bool getBlockDistFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(STableBlockDistInfo);
  return true;
}

static int32_t translateGroupKey(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  if (1 != LIST_LENGTH(pFunc->pParameterList)) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pPara = nodesListGetNode(pFunc->pParameterList, 0);
  pFunc->node.resType = ((SExprNode*)pPara)->resType;
  return TSDB_CODE_SUCCESS;
}

static int32_t translateDatabaseFunc(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  pFunc->node.resType = (SDataType){.bytes = TSDB_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateClientVersionFunc(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  pFunc->node.resType = (SDataType){.bytes = TSDB_VERSION_LEN, .type = TSDB_DATA_TYPE_VARCHAR};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateServerVersionFunc(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  pFunc->node.resType = (SDataType){.bytes = TSDB_VERSION_LEN, .type = TSDB_DATA_TYPE_VARCHAR};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateServerStatusFunc(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes, .type = TSDB_DATA_TYPE_INT};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateCurrentUserFunc(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  pFunc->node.resType = (SDataType){.bytes = TSDB_USER_LEN, .type = TSDB_DATA_TYPE_VARCHAR};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateUserFunc(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  pFunc->node.resType = (SDataType){.bytes = TSDB_USER_LEN, .type = TSDB_DATA_TYPE_VARCHAR};
  return TSDB_CODE_SUCCESS;
}

static int32_t translateTagsPseudoColumn(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  // The _tags pseudo-column will be expanded to the actual tags on the client side
  return TSDB_CODE_SUCCESS;
}

static int32_t translateTableCountPseudoColumn(SFunctionNode* pFunc, char* pErrBuf, int32_t len) {
  pFunc->node.resType = (SDataType){.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes, .type = TSDB_DATA_TYPE_BIGINT};
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
    .sprocessFunc = countScalarFunction,
    .finalizeFunc = functionFinalize,
#ifdef BUILD_NO_CALL
    .invertFunc   = countInvertFunction,
#endif
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
    .sprocessFunc = sumScalarFunction,
    .finalizeFunc = functionFinalize,
#ifdef BUILD_NO_CALL
    .invertFunc   = sumInvertFunction,
#endif
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
    .sprocessFunc = minScalarFunction,
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
    .sprocessFunc = maxScalarFunction,
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
    .sprocessFunc = stddevScalarFunction,
    .finalizeFunc = stddevFinalize,
#ifdef BUILD_NO_CALL
    .invertFunc   = stddevInvertFunction,
#endif
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
#ifdef BUILD_NO_CALL
    .invertFunc   = stddevInvertFunction,
#endif
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
#ifdef BUILD_NO_CALL
    .invertFunc   = stddevInvertFunction,
#endif
    .combineFunc  = stddevCombine,
  },
  {
    .name = "leastsquares",
    .type = FUNCTION_TYPE_LEASTSQUARES,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_FORBID_STREAM_FUNC | FUNC_MGT_FORBID_SYSTABLE_FUNC,
    .translateFunc = translateLeastSQR,
    .getEnvFunc   = getLeastSQRFuncEnv,
    .initFunc     = leastSQRFunctionSetup,
    .processFunc  = leastSQRFunction,
    .sprocessFunc = leastSQRScalarFunction,
    .finalizeFunc = leastSQRFinalize,
#ifdef BUILD_NO_CALL
    .invertFunc   = NULL,
#endif
    .combineFunc  = leastSQRCombine,
  },
  {
    .name = "avg",
    .type = FUNCTION_TYPE_AVG,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SPECIAL_DATA_REQUIRED,
    .translateFunc = translateInNumOutDou,
    .dataRequiredFunc = statisDataRequired,
    .getEnvFunc   = getAvgFuncEnv,
    .initFunc     = avgFunctionSetup,
    .processFunc  = avgFunction,
    .sprocessFunc = avgScalarFunction,
    .finalizeFunc = avgFinalize,
#ifdef BUILD_NO_CALL
    .invertFunc   = avgInvertFunction,
#endif
    .combineFunc  = avgCombine,
    .pPartialFunc = "_avg_partial",
    .pMiddleFunc  = "_avg_middle",
    .pMergeFunc   = "_avg_merge"
  },
  {
    .name = "_avg_partial",
    .type = FUNCTION_TYPE_AVG_PARTIAL,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateAvgPartial,
    .dataRequiredFunc = statisDataRequired,
    .getEnvFunc   = getAvgFuncEnv,
    .initFunc     = avgFunctionSetup,
    .processFunc  = avgFunction,
    .finalizeFunc = avgPartialFinalize,
#ifdef BUILD_NO_CALL
    .invertFunc   = avgInvertFunction,
#endif
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
#ifdef BUILD_NO_CALL
    .invertFunc   = avgInvertFunction,
#endif
    .combineFunc  = avgCombine,
  },
  {
    .name = "percentile",
    .type = FUNCTION_TYPE_PERCENTILE,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_REPEAT_SCAN_FUNC | FUNC_MGT_SPECIAL_DATA_REQUIRED | FUNC_MGT_FORBID_STREAM_FUNC,
    .translateFunc = translatePercentile,
    .dataRequiredFunc = statisDataRequired,
    .getEnvFunc   = getPercentileFuncEnv,
    .initFunc     = percentileFunctionSetup,
    .processFunc  = percentileFunction,
    .sprocessFunc = percentileScalarFunction,
    .finalizeFunc = percentileFinalize,
#ifdef BUILD_NO_CALL
    .invertFunc   = NULL,
#endif
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
    .sprocessFunc = apercentileScalarFunction,
    .finalizeFunc = apercentileFinalize,
#ifdef BUILD_NO_CALL
    .invertFunc   = NULL,
#endif
    .combineFunc  = apercentileCombine,
    .pPartialFunc = "_apercentile_partial",
    .pMergeFunc   = "_apercentile_merge",
    .createMergeParaFuc = apercentileCreateMergeParam
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
#ifdef BUILD_NO_CALL
    .invertFunc   = NULL,
#endif
    .combineFunc = apercentileCombine,
  },
  {
    .name = "_apercentile_merge",
    .type = FUNCTION_TYPE_APERCENTILE_MERGE,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateApercentileMerge,
    .getEnvFunc   = getApercentileFuncEnv,
    .initFunc     = apercentileFunctionSetup,
    .processFunc  = apercentileFunctionMerge,
    .finalizeFunc = apercentileFinalize,
#ifdef BUILD_NO_CALL
    .invertFunc   = NULL,
#endif
    .combineFunc = apercentileCombine,
  },
  {
    .name = "top",
    .type = FUNCTION_TYPE_TOP,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_MULTI_ROWS_FUNC | FUNC_MGT_KEEP_ORDER_FUNC |
                      FUNC_MGT_FORBID_STREAM_FUNC | FUNC_MGT_FORBID_FILL_FUNC,
    .translateFunc = translateTopBot,
    .getEnvFunc   = getTopBotFuncEnv,
    .initFunc     = topBotFunctionSetup,
    .processFunc  = topFunction,
    .sprocessFunc = topBotScalarFunction,
    .finalizeFunc = topBotFinalize,
    .combineFunc  = topCombine,
    .pPartialFunc = "top",
    .pMergeFunc   = "top",
    .createMergeParaFuc = topBotCreateMergeParam
  },
  {
    .name = "bottom",
    .type = FUNCTION_TYPE_BOTTOM,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_MULTI_ROWS_FUNC | FUNC_MGT_KEEP_ORDER_FUNC |
                      FUNC_MGT_FORBID_STREAM_FUNC | FUNC_MGT_FORBID_FILL_FUNC,
    .translateFunc = translateTopBot,
    .getEnvFunc   = getTopBotFuncEnv,
    .initFunc     = topBotFunctionSetup,
    .processFunc  = bottomFunction,
    .sprocessFunc = topBotScalarFunction,
    .finalizeFunc = topBotFinalize,
    .combineFunc  = bottomCombine,
    .pPartialFunc = "bottom",
    .pMergeFunc   = "bottom",
    .createMergeParaFuc = topBotCreateMergeParam
  },
  {
    .name = "spread",
    .type = FUNCTION_TYPE_SPREAD,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SPECIAL_DATA_REQUIRED,
    .translateFunc = translateSpread,
    .dataRequiredFunc = statisDataRequired,
    .getEnvFunc   = getSpreadFuncEnv,
    .initFunc     = spreadFunctionSetup,
    .processFunc  = spreadFunction,
    .sprocessFunc = spreadScalarFunction,
    .finalizeFunc = spreadFinalize,
#ifdef BUILD_NO_CALL
    .invertFunc   = NULL,
#endif
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
#ifdef BUILD_NO_CALL
    .invertFunc   = NULL,
#endif
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
#ifdef BUILD_NO_CALL
    .invertFunc   = NULL,
#endif
    .combineFunc  = spreadCombine,
  },
  {
    .name = "elapsed",
    .type = FUNCTION_TYPE_ELAPSED,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_TIMELINE_FUNC | FUNC_MGT_INTERVAL_INTERPO_FUNC | FUNC_MGT_FORBID_STREAM_FUNC |
                      FUNC_MGT_FORBID_SYSTABLE_FUNC | FUNC_MGT_SPECIAL_DATA_REQUIRED,
    .dataRequiredFunc = statisDataRequired,
    .translateFunc = translateElapsed,
    .getEnvFunc   = getElapsedFuncEnv,
    .initFunc     = elapsedFunctionSetup,
    .processFunc  = elapsedFunction,
    .finalizeFunc = elapsedFinalize,
#ifdef BUILD_NO_CALL
    .invertFunc   = NULL,
#endif
    .combineFunc  = elapsedCombine,
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
#ifdef BUILD_NO_CALL
    .invertFunc   = NULL,
#endif
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
  #ifdef BUILD_NO_CALL
    .invertFunc   = NULL,
  #endif
    .combineFunc  = elapsedCombine,
  },
  {
    .name = "interp",
    .type = FUNCTION_TYPE_INTERP,
    .classification = FUNC_MGT_TIMELINE_FUNC | FUNC_MGT_INTERVAL_INTERPO_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC |
                      FUNC_MGT_FORBID_STREAM_FUNC | FUNC_MGT_FORBID_SYSTABLE_FUNC | FUNC_MGT_KEEP_ORDER_FUNC,
    .translateFunc = translateInterp,
    .getEnvFunc    = getSelectivityFuncEnv,
    .initFunc      = functionSetup,
    .processFunc   = NULL,
    .finalizeFunc  = NULL,
    .estimateReturnRowsFunc = interpEstReturnRows
  },
  {
    .name = "derivative",
    .type = FUNCTION_TYPE_DERIVATIVE,
    .classification = FUNC_MGT_INDEFINITE_ROWS_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_TIMELINE_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC |
                      FUNC_MGT_KEEP_ORDER_FUNC | FUNC_MGT_CUMULATIVE_FUNC | FUNC_MGT_FORBID_STREAM_FUNC | FUNC_MGT_FORBID_SYSTABLE_FUNC,
    .translateFunc = translateDerivative,
    .getEnvFunc   = getDerivativeFuncEnv,
    .initFunc     = derivativeFuncSetup,
    .processFunc  = derivativeFunction,
    .sprocessFunc = derivativeScalarFunction,
    .finalizeFunc = functionFinalize,
    .estimateReturnRowsFunc = derivativeEstReturnRows
  },
  {
    .name = "irate",
    .type = FUNCTION_TYPE_IRATE,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_TIMELINE_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC | FUNC_MGT_FORBID_STREAM_FUNC |
                      FUNC_MGT_FORBID_SYSTABLE_FUNC,
    .translateFunc = translateIrate,
    .getEnvFunc   = getIrateFuncEnv,
    .initFunc     = irateFuncSetup,
    .processFunc  = irateFunction,
    .sprocessFunc = irateScalarFunction,
    .finalizeFunc = irateFinalize,
    .pPartialFunc = "_irate_partial",
    .pMergeFunc   = "_irate_merge"
  },
  {
    .name = "_irate_partial",
    .type = FUNCTION_TYPE_IRATE_PARTIAL,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_TIMELINE_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC | FUNC_MGT_FORBID_STREAM_FUNC |
                      FUNC_MGT_FORBID_SYSTABLE_FUNC,
    .translateFunc = translateIratePartial,
    .getEnvFunc   = getIrateFuncEnv,
    .initFunc     = irateFuncSetup,
    .processFunc  = irateFunction,
    .sprocessFunc = irateScalarFunction,
    .finalizeFunc = iratePartialFinalize
  },
  {
    .name = "_irate_merge",
    .type = FUNCTION_TYPE_IRATE_MERGE,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateIrateMerge,
    .getEnvFunc   = getIrateFuncEnv,
    .initFunc     = irateFuncSetup,
    .processFunc  = irateFunctionMerge,
    .sprocessFunc = irateScalarFunction,
    .finalizeFunc = irateFinalize
  },
  {
    .name = "last_row",
    .type = FUNCTION_TYPE_LAST_ROW,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_MULTI_RES_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC |
                      FUNC_MGT_KEEP_ORDER_FUNC | FUNC_MGT_FORBID_SYSTABLE_FUNC,
    .translateFunc = translateFirstLast,
    .dynDataRequiredFunc = lastDynDataReq,
    .getEnvFunc   = getFirstLastFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = lastRowFunction,
    .sprocessFunc = firstLastScalarFunction,
    .pPartialFunc = "_last_row_partial",
    .pMergeFunc   = "_last_row_merge",
    .finalizeFunc = firstLastFinalize,
    .combineFunc  = lastCombine
  },
  {
    .name = "_cache_last_row",
    .type = FUNCTION_TYPE_CACHE_LAST_ROW,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_MULTI_RES_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC |
                      FUNC_MGT_FORBID_STREAM_FUNC | FUNC_MGT_FORBID_SYSTABLE_FUNC,
    .translateFunc = translateFirstLast,
    .getEnvFunc   = getFirstLastFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = cachedLastRowFunction,
    .finalizeFunc = firstLastFinalize
  },
  {
    .name = "_cache_last",
    .type = FUNCTION_TYPE_CACHE_LAST,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_MULTI_RES_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_FORBID_STREAM_FUNC | FUNC_MGT_FORBID_SYSTABLE_FUNC,
    .translateFunc = translateFirstLast,
    .getEnvFunc   = getFirstLastFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = lastFunctionMerge,
    .finalizeFunc = firstLastFinalize
  },
  {
    .name = "_last_row_partial",
    .type = FUNCTION_TYPE_LAST_PARTIAL,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_MULTI_RES_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC |
                      FUNC_MGT_FORBID_SYSTABLE_FUNC,
    .translateFunc = translateFirstLastPartial,
    .dynDataRequiredFunc = lastDynDataReq,
    .getEnvFunc   = getFirstLastFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = lastRowFunction,
    .finalizeFunc = firstLastPartialFinalize,
  },
  {
    .name = "_last_row_merge",
    .type = FUNCTION_TYPE_LAST_MERGE,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_MULTI_RES_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC |
                      FUNC_MGT_FORBID_SYSTABLE_FUNC,
    .translateFunc = translateFirstLastMerge,
    .getEnvFunc   = getFirstLastFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = lastFunctionMerge,
    .finalizeFunc = firstLastFinalize,
  },
  {
    .name = "first",
    .type = FUNCTION_TYPE_FIRST,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_MULTI_RES_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC |
                      FUNC_MGT_KEEP_ORDER_FUNC | FUNC_MGT_FORBID_SYSTABLE_FUNC,
    .translateFunc = translateFirstLast,
    .dynDataRequiredFunc = firstDynDataReq,
    .getEnvFunc   = getFirstLastFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = firstFunction,
    .sprocessFunc = firstLastScalarFunction,
    .finalizeFunc = firstLastFinalize,
    .pPartialFunc = "_first_partial",
    .pMergeFunc   = "_first_merge",
    .combineFunc  = firstCombine,
  },
  {
    .name = "_first_partial",
    .type = FUNCTION_TYPE_FIRST_PARTIAL,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_MULTI_RES_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC |
                      FUNC_MGT_FORBID_SYSTABLE_FUNC,
    .translateFunc = translateFirstLastPartial,
    .dynDataRequiredFunc = firstDynDataReq,
    .getEnvFunc   = getFirstLastFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = firstFunction,
    .finalizeFunc = firstLastPartialFinalize,
    .combineFunc  = firstCombine,
  },
  {
    .name = "_first_merge",
    .type = FUNCTION_TYPE_FIRST_MERGE,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_MULTI_RES_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC |
                      FUNC_MGT_FORBID_SYSTABLE_FUNC,
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
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_MULTI_RES_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC |
                      FUNC_MGT_KEEP_ORDER_FUNC | FUNC_MGT_FORBID_SYSTABLE_FUNC,
    .translateFunc = translateFirstLast,
    .dynDataRequiredFunc = lastDynDataReq,
    .getEnvFunc   = getFirstLastFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = lastFunction,
    .sprocessFunc = firstLastScalarFunction,
    .finalizeFunc = firstLastFinalize,
    .pPartialFunc = "_last_partial",
    .pMergeFunc   = "_last_merge",
    .combineFunc  = lastCombine,
  },
  {
    .name = "_last_partial",
    .type = FUNCTION_TYPE_LAST_PARTIAL,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_MULTI_RES_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC |
                      FUNC_MGT_FORBID_SYSTABLE_FUNC,
    .translateFunc = translateFirstLastPartial,
    .dynDataRequiredFunc = lastDynDataReq,
    .getEnvFunc   = getFirstLastFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = lastFunction,
    .finalizeFunc = firstLastPartialFinalize,
    .combineFunc  = lastCombine,
  },
  {
    .name = "_last_merge",
    .type = FUNCTION_TYPE_LAST_MERGE,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_MULTI_RES_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC |
                      FUNC_MGT_FORBID_SYSTABLE_FUNC,
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
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_TIMELINE_FUNC | FUNC_MGT_INTERVAL_INTERPO_FUNC | FUNC_MGT_FORBID_STREAM_FUNC |
                      FUNC_MGT_IMPLICIT_TS_FUNC | FUNC_MGT_FORBID_SYSTABLE_FUNC,
    .translateFunc = translateInNumOutDou,
    .dataRequiredFunc = statisDataRequired,
    .getEnvFunc    = getTwaFuncEnv,
    .initFunc      = twaFunctionSetup,
    .processFunc   = twaFunction,
    .sprocessFunc  = twaScalarFunction,
    .finalizeFunc  = twaFinalize
  },
  {
    .name = "histogram",
    .type = FUNCTION_TYPE_HISTOGRAM,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_MULTI_ROWS_FUNC | FUNC_MGT_FORBID_FILL_FUNC | FUNC_MGT_FORBID_STREAM_FUNC,
    .translateFunc = translateHistogram,
    .getEnvFunc   = getHistogramFuncEnv,
    .initFunc     = histogramFunctionSetup,
    .processFunc  = histogramFunction,
    .sprocessFunc = histogramScalarFunction,
    .finalizeFunc = histogramFinalize,
#ifdef BUILD_NO_CALL
    .invertFunc   = NULL,
#endif
    .combineFunc  = histogramCombine,
    .pPartialFunc = "_histogram_partial",
    .pMergeFunc   = "_histogram_merge",
  },
  {
    .name = "_histogram_partial",
    .type = FUNCTION_TYPE_HISTOGRAM_PARTIAL,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_MULTI_ROWS_FUNC | FUNC_MGT_FORBID_FILL_FUNC,
    .translateFunc = translateHistogramPartial,
    .getEnvFunc   = getHistogramFuncEnv,
    .initFunc     = histogramFunctionSetup,
    .processFunc  = histogramFunctionPartial,
    .finalizeFunc = histogramPartialFinalize,
#ifdef BUILD_NO_CALL
    .invertFunc   = NULL,
#endif
    .combineFunc  = histogramCombine,
  },
  {
    .name = "_histogram_merge",
    .type = FUNCTION_TYPE_HISTOGRAM_MERGE,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_MULTI_ROWS_FUNC | FUNC_MGT_FORBID_FILL_FUNC,
    .translateFunc = translateHistogramMerge,
    .getEnvFunc   = getHistogramFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = histogramFunctionMerge,
    .finalizeFunc = histogramFinalize,
#ifdef BUILD_NO_CALL
    .invertFunc   = NULL,
#endif
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
    .sprocessFunc = hllScalarFunction,
    .finalizeFunc = hllFinalize,
#ifdef BUILD_NO_CALL
    .invertFunc   = NULL,
#endif
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
#ifdef BUILD_NO_CALL
    .invertFunc   = NULL,
#endif
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
#ifdef BUILD_NO_CALL
    .invertFunc   = NULL,
#endif
    .combineFunc  = hllCombine,
  },
  {
    .name = "diff",
    .type = FUNCTION_TYPE_DIFF,
    .classification = FUNC_MGT_INDEFINITE_ROWS_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_TIMELINE_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC |
                      FUNC_MGT_KEEP_ORDER_FUNC | FUNC_MGT_FORBID_STREAM_FUNC | FUNC_MGT_CUMULATIVE_FUNC | FUNC_MGT_FORBID_SYSTABLE_FUNC,
    .translateFunc = translateDiff,
    .getEnvFunc   = getDiffFuncEnv,
    .initFunc     = diffFunctionSetup,
    .processFunc  = diffFunction,
    .sprocessFunc = diffScalarFunction,
    .finalizeFunc = functionFinalize,
    .estimateReturnRowsFunc = diffEstReturnRows,
  },
  {
    .name = "statecount",
    .type = FUNCTION_TYPE_STATE_COUNT,
    .classification = FUNC_MGT_INDEFINITE_ROWS_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_TIMELINE_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC |
                      FUNC_MGT_FORBID_STREAM_FUNC | FUNC_MGT_FORBID_SYSTABLE_FUNC,
    .translateFunc = translateStateCount,
    .getEnvFunc   = getStateFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = stateCountFunction,
    .sprocessFunc = stateCountScalarFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "stateduration",
    .type = FUNCTION_TYPE_STATE_DURATION,
    .classification = FUNC_MGT_INDEFINITE_ROWS_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_TIMELINE_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC |
                      FUNC_MGT_FORBID_STREAM_FUNC | FUNC_MGT_FORBID_SYSTABLE_FUNC,
    .translateFunc = translateStateDuration,
    .getEnvFunc   = getStateFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = stateDurationFunction,
    .sprocessFunc = stateDurationScalarFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "csum",
    .type = FUNCTION_TYPE_CSUM,
    .classification = FUNC_MGT_INDEFINITE_ROWS_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_TIMELINE_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC |
                      FUNC_MGT_FORBID_STREAM_FUNC | FUNC_MGT_CUMULATIVE_FUNC | FUNC_MGT_KEEP_ORDER_FUNC | FUNC_MGT_FORBID_SYSTABLE_FUNC,
    .translateFunc = translateCsum,
    .getEnvFunc   = getCsumFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = csumFunction,
    .sprocessFunc = csumScalarFunction,
    .finalizeFunc = NULL,
    .estimateReturnRowsFunc = csumEstReturnRows,
  },
  {
    .name = "mavg",
    .type = FUNCTION_TYPE_MAVG,
    .classification = FUNC_MGT_INDEFINITE_ROWS_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_TIMELINE_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC |
                      FUNC_MGT_FORBID_STREAM_FUNC | FUNC_MGT_FORBID_SYSTABLE_FUNC,
    .translateFunc = translateMavg,
    .getEnvFunc   = getMavgFuncEnv,
    .initFunc     = mavgFunctionSetup,
    .processFunc  = mavgFunction,
    .sprocessFunc = mavgScalarFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "sample",
    .type = FUNCTION_TYPE_SAMPLE,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_MULTI_ROWS_FUNC | FUNC_MGT_KEEP_ORDER_FUNC | FUNC_MGT_FORBID_STREAM_FUNC |
                      FUNC_MGT_FORBID_FILL_FUNC,
    .translateFunc = translateSample,
    .getEnvFunc   = getSampleFuncEnv,
    .initFunc     = sampleFunctionSetup,
    .processFunc  = sampleFunction,
    .sprocessFunc = sampleScalarFunction,
    .finalizeFunc = sampleFinalize
  },
  {
    .name = "tail",
    .type = FUNCTION_TYPE_TAIL,
    .classification = FUNC_MGT_SELECT_FUNC | FUNC_MGT_INDEFINITE_ROWS_FUNC | FUNC_MGT_FORBID_STREAM_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC,
    .translateFunc = translateTail,
    .getEnvFunc   = getTailFuncEnv,
    .initFunc     = tailFunctionSetup,
    .processFunc  = tailFunction,
    .sprocessFunc = tailScalarFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "unique",
    .type = FUNCTION_TYPE_UNIQUE,
    .classification = FUNC_MGT_SELECT_FUNC | FUNC_MGT_INDEFINITE_ROWS_FUNC | FUNC_MGT_FORBID_STREAM_FUNC | FUNC_MGT_IMPLICIT_TS_FUNC,
    .translateFunc = translateUnique,
    .getEnvFunc   = getUniqueFuncEnv,
    .initFunc     = uniqueFunctionSetup,
    .processFunc  = uniqueFunction,
    .sprocessFunc = uniqueScalarFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "mode",
    .type = FUNCTION_TYPE_MODE,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_FORBID_STREAM_FUNC,
    .translateFunc = translateMode,
    .getEnvFunc   = getModeFuncEnv,
    .initFunc     = modeFunctionSetup,
    .processFunc  = modeFunction,
    .sprocessFunc = modeScalarFunction,
    .finalizeFunc = modeFinalize,
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
    .translateFunc = translateLtrim,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = ltrimFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "rtrim",
    .type = FUNCTION_TYPE_RTRIM,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_STRING_FUNC,
    .translateFunc = translateRtrim,
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
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_DATETIME_FUNC | FUNC_MGT_KEEP_ORDER_FUNC,
    .translateFunc = translateNowToday,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = nowFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "today",
    .type = FUNCTION_TYPE_TODAY,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_DATETIME_FUNC | FUNC_MGT_KEEP_ORDER_FUNC,
    .translateFunc = translateNowToday,
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
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_SCAN_PC_FUNC | FUNC_MGT_KEEP_ORDER_FUNC,
    .translateFunc = translateTbnameColumn,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = qPseudoTagFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "_qstart",
    .type = FUNCTION_TYPE_QSTART,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_CLIENT_PC_FUNC,
    .translateFunc = translateTimePseudoColumn,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = NULL,
    .finalizeFunc = NULL
  },
  {
    .name = "_qend",
    .type = FUNCTION_TYPE_QEND,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_CLIENT_PC_FUNC,
    .translateFunc = translateTimePseudoColumn,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = NULL,
    .finalizeFunc = NULL
  },
  {
    .name = "_qduration",
    .type = FUNCTION_TYPE_QDURATION,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_CLIENT_PC_FUNC,
    .translateFunc = translateWduration,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = NULL,
    .finalizeFunc = NULL
  },
  {
    .name = "_wstart",
    .type = FUNCTION_TYPE_WSTART,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_WINDOW_PC_FUNC | FUNC_MGT_KEEP_ORDER_FUNC | FUNC_MGT_SKIP_SCAN_CHECK_FUNC,
    .translateFunc = translateTimePseudoColumn,
    .getEnvFunc   = getTimePseudoFuncEnv,
    .initFunc     = NULL,
    .sprocessFunc = winStartTsFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "_wend",
    .type = FUNCTION_TYPE_WEND,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_WINDOW_PC_FUNC | FUNC_MGT_KEEP_ORDER_FUNC | FUNC_MGT_SKIP_SCAN_CHECK_FUNC,
    .translateFunc = translateTimePseudoColumn,
    .getEnvFunc   = getTimePseudoFuncEnv,
    .initFunc     = NULL,
    .sprocessFunc = winEndTsFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "_wduration",
    .type = FUNCTION_TYPE_WDURATION,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_WINDOW_PC_FUNC | FUNC_MGT_KEEP_ORDER_FUNC | FUNC_MGT_SKIP_SCAN_CHECK_FUNC,
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
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_KEEP_ORDER_FUNC,
    .translateFunc = translateSelectValue,
    .getEnvFunc   = getSelectivityFuncEnv,  // todo remove this function later.
    .initFunc     = functionSetup,
    .processFunc  = NULL,
    .finalizeFunc = NULL,
    .pPartialFunc = "_select_value",
    .pMergeFunc   = "_select_value"
  },
  {
    .name = "_block_dist",
    .type = FUNCTION_TYPE_BLOCK_DIST,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_FORBID_STREAM_FUNC,
    .translateFunc = translateBlockDistFunc,
    .getEnvFunc   = getBlockDistFuncEnv,
    .initFunc     = blockDistSetup,
    .processFunc  = blockDistFunction,
    .finalizeFunc = blockDistFinalize
  },
  {
    .name = "_block_dist_info",
    .type = FUNCTION_TYPE_BLOCK_DIST_INFO,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_SCAN_PC_FUNC,
    .translateFunc = translateBlockDistInfoFunc,
  },
  {
    .name = "_group_key",
    .type = FUNCTION_TYPE_GROUP_KEY,
    .classification = FUNC_MGT_AGG_FUNC | FUNC_MGT_SELECT_FUNC | FUNC_MGT_KEEP_ORDER_FUNC | FUNC_MGT_SKIP_SCAN_CHECK_FUNC,
    .translateFunc = translateGroupKey,
    .getEnvFunc   = getGroupKeyFuncEnv,
    .initFunc     = functionSetup,
    .processFunc  = groupKeyFunction,
    .finalizeFunc = groupKeyFinalize,
    .combineFunc  = groupKeyCombine,
    .pPartialFunc = "_group_key",
    .pMergeFunc   = "_group_key"
  },
  {
    .name = "database",
    .type = FUNCTION_TYPE_DATABASE,
    .classification = FUNC_MGT_SYSTEM_INFO_FUNC | FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateDatabaseFunc,
  },
  {
    .name = "client_version",
    .type = FUNCTION_TYPE_CLIENT_VERSION,
    .classification = FUNC_MGT_SYSTEM_INFO_FUNC | FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateClientVersionFunc,
  },
  {
    .name = "server_version",
    .type = FUNCTION_TYPE_SERVER_VERSION,
    .classification = FUNC_MGT_SYSTEM_INFO_FUNC | FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateServerVersionFunc,
  },
  {
    .name = "server_status",
    .type = FUNCTION_TYPE_SERVER_STATUS,
    .classification = FUNC_MGT_SYSTEM_INFO_FUNC | FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateServerStatusFunc,
  },
  {
    .name = "current_user",
    .type = FUNCTION_TYPE_CURRENT_USER,
    .classification = FUNC_MGT_SYSTEM_INFO_FUNC | FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateCurrentUserFunc,
  },
  {
    .name = "user",
    .type = FUNCTION_TYPE_USER,
    .classification = FUNC_MGT_SYSTEM_INFO_FUNC | FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateUserFunc,
  },
  {
    .name = "_irowts",
    .type = FUNCTION_TYPE_IROWTS,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_INTERP_PC_FUNC | FUNC_MGT_KEEP_ORDER_FUNC,
    .translateFunc = translateTimePseudoColumn,
    .getEnvFunc   = getTimePseudoFuncEnv,
    .initFunc     = NULL,
    .sprocessFunc = NULL,
    .finalizeFunc = NULL
  },
  {
    .name = "_isfilled",
    .type = FUNCTION_TYPE_ISFILLED,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_INTERP_PC_FUNC,
    .translateFunc = translateIsFilledPseudoColumn,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = NULL,
    .finalizeFunc = NULL
  },
  {
    .name = "_tags",
    .type = FUNCTION_TYPE_TAGS,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_MULTI_RES_FUNC,
    .translateFunc = translateTagsPseudoColumn,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = NULL,
    .finalizeFunc = NULL
  },
  {
    .name = "_table_count",
    .type = FUNCTION_TYPE_TABLE_COUNT,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_SCAN_PC_FUNC,
    .translateFunc = translateTableCountPseudoColumn,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = NULL,
    .finalizeFunc = NULL
  },
  {
    .name = "st_geomfromtext",
    .type = FUNCTION_TYPE_GEOM_FROM_TEXT,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_GEOMETRY_FUNC,
    .translateFunc = translateInStrOutGeom,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = geomFromTextFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "st_astext",
    .type = FUNCTION_TYPE_AS_TEXT,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_GEOMETRY_FUNC,
    .translateFunc = translateInGeomOutStr,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = asTextFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "st_makepoint",
    .type = FUNCTION_TYPE_MAKE_POINT,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_GEOMETRY_FUNC,
    .translateFunc = translateIn2NumOutGeom,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = makePointFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "st_intersects",
    .type = FUNCTION_TYPE_INTERSECTS,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_GEOMETRY_FUNC,
    .translateFunc = translateIn2GeomOutBool,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = intersectsFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "st_equals",
    .type = FUNCTION_TYPE_EQUALS,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_GEOMETRY_FUNC,
    .translateFunc = translateIn2GeomOutBool,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = equalsFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "st_touches",
    .type = FUNCTION_TYPE_TOUCHES,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_GEOMETRY_FUNC,
    .translateFunc = translateIn2GeomOutBool,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = touchesFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "st_covers",
    .type = FUNCTION_TYPE_COVERS,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_GEOMETRY_FUNC,
    .translateFunc = translateIn2GeomOutBool,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = coversFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "st_contains",
    .type = FUNCTION_TYPE_CONTAINS,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_GEOMETRY_FUNC,
    .translateFunc = translateIn2GeomOutBool,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = containsFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "st_containsproperly",
    .type = FUNCTION_TYPE_CONTAINS_PROPERLY,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_GEOMETRY_FUNC,
    .translateFunc = translateIn2GeomOutBool,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = containsProperlyFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "_tbuid",
    .type = FUNCTION_TYPE_TBUID,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_SCAN_PC_FUNC | FUNC_MGT_KEEP_ORDER_FUNC,
    .translateFunc = translateTbUidColumn,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = qPseudoTagFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "_vgid",
    .type = FUNCTION_TYPE_VGID,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_SCAN_PC_FUNC | FUNC_MGT_KEEP_ORDER_FUNC,
    .translateFunc = translateVgIdColumn,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = qPseudoTagFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "to_timestamp",
    .type = FUNCTION_TYPE_TO_TIMESTAMP,
    .classification = FUNC_MGT_SCALAR_FUNC | FUNC_MGT_DATETIME_FUNC,
    .translateFunc = translateToTimestamp,
    .getEnvFunc = NULL,
    .initFunc = NULL,
    .sprocessFunc = toTimestampFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "to_char",
    .type = FUNCTION_TYPE_TO_CHAR,
    .classification = FUNC_MGT_SCALAR_FUNC,
    .translateFunc = translateToChar,
    .getEnvFunc = NULL,
    .initFunc = NULL,
    .sprocessFunc = toCharFunction,
    .finalizeFunc = NULL
  },
  {
    .name = "_avg_middle",
    .type = FUNCTION_TYPE_AVG_PARTIAL,
    .classification = FUNC_MGT_AGG_FUNC,
    .translateFunc = translateAvgMiddle,
    .dataRequiredFunc = statisDataRequired,
    .getEnvFunc   = getAvgFuncEnv,
    .initFunc     = avgFunctionSetup,
    .processFunc  = avgFunctionMerge,
    .finalizeFunc = avgPartialFinalize,
#ifdef BUILD_NO_CALL
    .invertFunc   = avgInvertFunction,
#endif
    .combineFunc  = avgCombine,
  },
  {
    .name = "_vgver",
    .type = FUNCTION_TYPE_VGVER,
    .classification = FUNC_MGT_PSEUDO_COLUMN_FUNC | FUNC_MGT_SCAN_PC_FUNC | FUNC_MGT_KEEP_ORDER_FUNC,
    .translateFunc = translateVgVerColumn,
    .getEnvFunc   = NULL,
    .initFunc     = NULL,
    .sprocessFunc = qPseudoTagFunction,
    .finalizeFunc = NULL
  }
};
// clang-format on

const int32_t funcMgtBuiltinsNum = (sizeof(funcMgtBuiltins) / sizeof(SBuiltinFuncDefinition));
