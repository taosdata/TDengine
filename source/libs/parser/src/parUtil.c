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

#include "parUtil.h"
#include "cJSON.h"
#include "querynodes.h"

#define USER_AUTH_KEY_MAX_LEN TSDB_USER_LEN + TSDB_TABLE_FNAME_LEN + 2

const void* nullPointer = NULL;

static char* getSyntaxErrFormat(int32_t errCode) {
  switch (errCode) {
    case TSDB_CODE_PAR_SYNTAX_ERROR:
      return "syntax error near \"%s\"";
    case TSDB_CODE_PAR_INCOMPLETE_SQL:
      return "Incomplete SQL statement";
    case TSDB_CODE_PAR_INVALID_COLUMN:
      return "Invalid column name: %s";
    case TSDB_CODE_PAR_TABLE_NOT_EXIST:
      return "Table does not exist: %s";
    case TSDB_CODE_PAR_GET_META_ERROR:
      return "Fail to get table info, error: %s";
    case TSDB_CODE_PAR_AMBIGUOUS_COLUMN:
      return "Column ambiguously defined: %s";
    case TSDB_CODE_PAR_WRONG_VALUE_TYPE:
      return "Invalid value type: %s";
    case TSDB_CODE_PAR_INVALID_VARBINARY:
      return "Invalid varbinary value: %s";
    case TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION:
      return "There mustn't be aggregation";
    case TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT:
      return "ORDER BY item must be the number of a SELECT-list expression";
    case TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION:
      return "Not a GROUP BY expression";
    case TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION:
      return "Not SELECTed expression";
    case TSDB_CODE_PAR_NOT_SINGLE_GROUP:
      return "Not a single-group group function";
    case TSDB_CODE_PAR_TAGS_NOT_MATCHED:
      return "Tags number not matched";
    case TSDB_CODE_PAR_INVALID_TAG_NAME:
      return "Invalid tag name: %s";
    case TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG:
      return "Name or password too long";
    case TSDB_CODE_PAR_PASSWD_EMPTY:
      return "Password can not be empty";
    case TSDB_CODE_PAR_INVALID_PORT:
      return "Port should be an integer that is less than 65535 and greater than 0";
    case TSDB_CODE_PAR_INVALID_ENDPOINT:
      return "Endpoint should be in the format of 'fqdn:port'";
    case TSDB_CODE_PAR_EXPRIE_STATEMENT:
      return "This statement is no longer supported";
    case TSDB_CODE_PAR_INTER_VALUE_TOO_SMALL:
      return "Interval cannot be less than %d %s";
    case TSDB_CODE_PAR_INTER_VALUE_TOO_BIG:
      return "Interval cannot be more than %d %s";
    case TSDB_CODE_PAR_DB_NOT_SPECIFIED:
      return "Database not specified";
    case TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME:
      return "Invalid identifier name: %s";
    case TSDB_CODE_PAR_CORRESPONDING_STABLE_ERR:
      return "Corresponding super table not in this db";
    case TSDB_CODE_PAR_GROUPBY_WINDOW_COEXIST:
      return "GROUP BY and WINDOW-clause can't be used together";
    case TSDB_CODE_PAR_AGG_FUNC_NESTING:
      return "Aggregate functions do not support nesting";
    case TSDB_CODE_PAR_INVALID_STATE_WIN_TYPE:
      return "Only support STATE_WINDOW on integer/bool/varchar column";
    case TSDB_CODE_PAR_INVALID_STATE_WIN_COL:
      return "Not support STATE_WINDOW on tag column";
    case TSDB_CODE_PAR_INVALID_STATE_WIN_TABLE:
      return "STATE_WINDOW not support for super table query";
    case TSDB_CODE_PAR_INTER_SESSION_GAP:
      return "SESSION gap should be fixed time window, and greater than 0";
    case TSDB_CODE_PAR_INTER_SESSION_COL:
      return "Only support SESSION on primary timestamp column";
    case TSDB_CODE_PAR_INTER_OFFSET_NEGATIVE:
      return "Interval offset cannot be negative";
    case TSDB_CODE_PAR_INTER_OFFSET_UNIT:
      return "Cannot use 'year' as offset when interval is 'month'";
    case TSDB_CODE_PAR_INTER_OFFSET_TOO_BIG:
      return "Interval offset should be shorter than interval";
    case TSDB_CODE_PAR_INTER_SLIDING_UNIT:
      return "Does not support sliding when interval is natural month/year";
    case TSDB_CODE_PAR_INTER_SLIDING_TOO_BIG:
      return "sliding value no larger than the interval value";
    case TSDB_CODE_PAR_INTER_SLIDING_TOO_SMALL:
      return "sliding value can not less than 1%% of interval value";
    case TSDB_CODE_PAR_ONLY_ONE_JSON_TAG:
      return "Only one tag if there is a json tag";
    case TSDB_CODE_PAR_INCORRECT_NUM_OF_COL:
      return "Query block has incorrect number of result columns";
    case TSDB_CODE_PAR_INCORRECT_TIMESTAMP_VAL:
      return "Incorrect TIMESTAMP value: %s";
    case TSDB_CODE_PAR_OFFSET_LESS_ZERO:
      return "soffset/offset can not be less than 0";
    case TSDB_CODE_PAR_SLIMIT_LEAK_PARTITION_GROUP_BY:
      return "slimit/soffset only available for PARTITION/GROUP BY query";
    case TSDB_CODE_PAR_INVALID_TOPIC_QUERY:
      return "Invalid topic query";
    case TSDB_CODE_PAR_INVALID_DROP_STABLE:
      return "Cannot drop super table in batch";
    case TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE:
      return "Start(end) time of query range required or time range too large";
    case TSDB_CODE_PAR_DUPLICATED_COLUMN:
      return "Duplicated column names";
    case TSDB_CODE_PAR_INVALID_TAGS_LENGTH:
      return "Tags length exceeds max length %d";
    case TSDB_CODE_PAR_INVALID_ROW_LENGTH:
      return "Row length exceeds max length %d";
    case TSDB_CODE_PAR_INVALID_COLUMNS_NUM:
      return "Illegal number of columns";
    case TSDB_CODE_PAR_TOO_MANY_COLUMNS:
      return "Too many columns";
    case TSDB_CODE_PAR_INVALID_FIRST_COLUMN:
      return "First column must be timestamp";
    case TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN:
      return "Invalid column length for var length type";
    case TSDB_CODE_PAR_INVALID_TAGS_NUM:
      return "Invalid number of tag columns";
    case TSDB_CODE_PAR_INVALID_INTERNAL_PK:
      return "Invalid _c0 or _rowts expression";
    case TSDB_CODE_PAR_INVALID_TIMELINE_FUNC:
      return "Invalid timeline function";
    case TSDB_CODE_PAR_INVALID_PASSWD:
      return "Invalid password";
    case TSDB_CODE_PAR_INVALID_ALTER_TABLE:
      return "Invalid alter table statement";
    case TSDB_CODE_PAR_CANNOT_DROP_PRIMARY_KEY:
      return "Primary timestamp column cannot be dropped";
    case TSDB_CODE_PAR_INVALID_MODIFY_COL:
      return "Only varbinary/binary/nchar/geometry column length could be modified, and the length can only be increased, not decreased";
    case TSDB_CODE_PAR_INVALID_TBNAME:
      return "Invalid tbname pseudo column";
    case TSDB_CODE_PAR_INVALID_FUNCTION_NAME:
      return "Invalid function name";
    case TSDB_CODE_PAR_COMMENT_TOO_LONG:
      return "Comment too long";
    case TSDB_CODE_PAR_NOT_ALLOWED_FUNC:
      return "Some functions are allowed only in the SELECT list of a query. "
             "And, cannot be mixed with other non scalar functions or columns.";
    case TSDB_CODE_PAR_NOT_ALLOWED_WIN_QUERY:
      return "Window query not supported, since the result of subquery not include valid timestamp column";
    case TSDB_CODE_PAR_INVALID_DROP_COL:
      return "No columns can be dropped";
    case TSDB_CODE_PAR_INVALID_COL_JSON:
      return "Only tag can be json type";
    case TSDB_CODE_PAR_VALUE_TOO_LONG:
      return "Value too long for column/tag: %s";
    case TSDB_CODE_PAR_INVALID_DELETE_WHERE:
      return "The DELETE statement must have a definite time window range";
    case TSDB_CODE_PAR_INVALID_REDISTRIBUTE_VG:
      return "The REDISTRIBUTE VGROUP statement only support 1 to 3 dnodes";
    case TSDB_CODE_PAR_FILL_NOT_ALLOWED_FUNC:
      return "%s function is not supported in fill query";
    case TSDB_CODE_PAR_INVALID_WINDOW_PC:
      return "_WSTART, _WEND and _WDURATION can only be used in window query";
    case TSDB_CODE_PAR_INVALID_TAGS_PC:
      return "Tags can only applied to super table and child table";
    case TSDB_CODE_PAR_WINDOW_NOT_ALLOWED_FUNC:
      return "%s function is not supported in time window query";
    case TSDB_CODE_PAR_STREAM_NOT_ALLOWED_FUNC:
      return "%s function is not supported in stream query";
    case TSDB_CODE_PAR_GROUP_BY_NOT_ALLOWED_FUNC:
      return "%s function is not supported in group query";
    case TSDB_CODE_PAR_SYSTABLE_NOT_ALLOWED_FUNC:
      return "%s function is not supported in system table query";
    case TSDB_CODE_PAR_SYSTABLE_NOT_ALLOWED:
      return "%s is not supported in system table query";
    case TSDB_CODE_PAR_INVALID_INTERP_CLAUSE:
      return "Invalid usage of RANGE clause, EVERY clause or FILL clause";
    case TSDB_CODE_PAR_NO_VALID_FUNC_IN_WIN:
      return "No valid function in window query";
    case TSDB_CODE_PAR_INVALID_OPTR_USAGE:
      return "Invalid usage of expr: %s";
    case TSDB_CODE_PAR_INVALID_IP_RANGE:
      return "invalid ip range";
    case TSDB_CODE_OUT_OF_MEMORY:
      return "Out of memory";
    case TSDB_CODE_PAR_ORDERBY_AMBIGUOUS:
      return "ORDER BY \"%s\" is ambiguous";
    case TSDB_CODE_PAR_NOT_SUPPORT_MULTI_RESULT:
      return "Operator not supported multi result: %s";
    default:
      return "Unknown error";
  }
}

int32_t generateSyntaxErrMsg(SMsgBuf* pBuf, int32_t errCode, ...) {
  va_list vArgList;
  va_start(vArgList, errCode);
  vsnprintf(pBuf->buf, pBuf->len, getSyntaxErrFormat(errCode), vArgList);
  va_end(vArgList);
  return errCode;
}

int32_t generateSyntaxErrMsgExt(SMsgBuf* pBuf, int32_t errCode, const char* pFormat, ...) {
  va_list vArgList;
  va_start(vArgList, pFormat);
  vsnprintf(pBuf->buf, pBuf->len, pFormat, vArgList);
  va_end(vArgList);
  return errCode;
}

int32_t buildInvalidOperationMsg(SMsgBuf* pBuf, const char* msg) {
  strncpy(pBuf->buf, msg, pBuf->len);
  return TSDB_CODE_TSC_INVALID_OPERATION;
}

int32_t buildSyntaxErrMsg(SMsgBuf* pBuf, const char* additionalInfo, const char* sourceStr) {
  if (pBuf == NULL) return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
  const char* msgFormat1 = "syntax error near \'%s\'";
  const char* msgFormat2 = "syntax error near \'%s\' (%s)";
  const char* msgFormat3 = "%s";

  const char* prefix = "syntax error";
  if (sourceStr == NULL) {
    snprintf(pBuf->buf, pBuf->len, msgFormat1, additionalInfo);
    return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
  }

  char buf[64] = {0};  // only extract part of sql string
  strncpy(buf, sourceStr, tListLen(buf) - 1);

  if (additionalInfo != NULL) {
    snprintf(pBuf->buf, pBuf->len, msgFormat2, buf, additionalInfo);
  } else {
    const char* msgFormat = (0 == strncmp(sourceStr, prefix, strlen(prefix))) ? msgFormat3 : msgFormat1;
    snprintf(pBuf->buf, pBuf->len, msgFormat, buf);
  }

  return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
}

SSchema* getTableColumnSchema(const STableMeta* pTableMeta) { return (SSchema*)pTableMeta->schema; }

static SSchema* getOneColumnSchema(const STableMeta* pTableMeta, int32_t colIndex) {
  SSchema* pSchema = (SSchema*)pTableMeta->schema;
  return &pSchema[colIndex];
}

SSchema* getTableTagSchema(const STableMeta* pTableMeta) {
  return getOneColumnSchema(pTableMeta, getTableInfo(pTableMeta).numOfColumns);
}

int32_t getNumOfColumns(const STableMeta* pTableMeta) {
  // table created according to super table, use data from super table
  return getTableInfo(pTableMeta).numOfColumns;
}

int32_t getNumOfTags(const STableMeta* pTableMeta) { return getTableInfo(pTableMeta).numOfTags; }

STableComInfo getTableInfo(const STableMeta* pTableMeta) { return pTableMeta->tableInfo; }

int32_t getTableTypeFromTableNode(SNode *pTable) {
  if (NULL == pTable) {
    return -1;
  }
  if (QUERY_NODE_REAL_TABLE != nodeType(pTable)) {
    return -1;
  }
  return ((SRealTableNode *)pTable)->pMeta->tableType;
}


STableMeta* tableMetaDup(const STableMeta* pTableMeta) {
  int32_t numOfFields = TABLE_TOTAL_COL_NUM(pTableMeta);
  if (numOfFields > TSDB_MAX_COLUMNS || numOfFields < TSDB_MIN_COLUMNS) {
    return NULL;
  }

  size_t      size = sizeof(STableMeta) + numOfFields * sizeof(SSchema);
  STableMeta* p = taosMemoryMalloc(size);
  memcpy(p, pTableMeta, size);
  return p;
}

int32_t trimString(const char* src, int32_t len, char* dst, int32_t dlen) {
  if (len <= 0 || dlen <= 0) return 0;

  char    delim = src[0];
  int32_t j = 0;
  for (uint32_t k = 1; k < len - 1; ++k) {
    if (j >= dlen) {
      dst[j - 1] = '\0';
      return j;
    }
    if (src[k] == delim && src[k + 1] == delim) {  // deal with "", ''
      dst[j] = src[k + 1];
      j++;
      k++;
      continue;
    }

    if (src[k] == '\\') {  // deal with escape character
      if (src[k + 1] == 'n') {
        dst[j] = '\n';
      } else if (src[k + 1] == 'r') {
        dst[j] = '\r';
      } else if (src[k + 1] == 't') {
        dst[j] = '\t';
      } else if (src[k + 1] == '\\') {
        dst[j] = '\\';
      } else if (src[k + 1] == '\'') {
        dst[j] = '\'';
      } else if (src[k + 1] == '"') {
        dst[j] = '"';
      } else if (src[k + 1] == '%' || src[k + 1] == '_' || src[k + 1] == 'x') {
        dst[j++] = src[k];
        dst[j] = src[k + 1];
      } else {
        dst[j] = src[k + 1];
      }
      j++;
      k++;
      continue;
    }

    dst[j] = src[k];
    j++;
  }
  if (j >= dlen) j = dlen - 1;
  dst[j] = '\0';
  return j;
}

static bool isValidateTag(char* input) {
  if (!input) return false;
  for (size_t i = 0; i < strlen(input); ++i) {
#ifdef WINDOWS
    if (input[i] < 0x20 || input[i] > 0x7E) return false;
#else
    if (isprint(input[i]) == 0) return false;
#endif
  }
  return true;
}

int32_t parseJsontoTagData(const char* json, SArray* pTagVals, STag** ppTag, void* pMsgBuf) {
  int32_t   retCode = TSDB_CODE_SUCCESS;
  cJSON*    root = NULL;
  SHashObj* keyHash = NULL;
  int32_t   size = 0;
  // set json NULL data
  if (!json || strtrim((char*)json) == 0 || strcasecmp(json, TSDB_DATA_NULL_STR_L) == 0) {
    retCode = TSDB_CODE_SUCCESS;
    goto end;
  }

  // set json real data
  root = cJSON_Parse(json);
  if (root == NULL) {
    retCode = buildSyntaxErrMsg(pMsgBuf, "json parse error", json);
    goto end;
  }

  size = cJSON_GetArraySize(root);
  if (!cJSON_IsObject(root)) {
    retCode = buildSyntaxErrMsg(pMsgBuf, "json error invalide value", json);
    goto end;
  }

  keyHash = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, false);
  for (int32_t i = 0; i < size; i++) {
    cJSON* item = cJSON_GetArrayItem(root, i);
    if (!item) {
      uError("json inner error:%d", i);
      retCode = buildSyntaxErrMsg(pMsgBuf, "json inner error", json);
      goto end;
    }

    char* jsonKey = item->string;
    if (!isValidateTag(jsonKey)) {
      retCode = buildSyntaxErrMsg(pMsgBuf, "json key not validate", jsonKey);
      goto end;
    }
    size_t keyLen = strlen(jsonKey);
    if (keyLen > TSDB_MAX_JSON_KEY_LEN) {
      uError("json key too long error");
      retCode = buildSyntaxErrMsg(pMsgBuf, "json key too long, more than 256", jsonKey);
      goto end;
    }
    if (keyLen == 0 || taosHashGet(keyHash, jsonKey, keyLen) != NULL) {
      continue;
    }
    STagVal val = {0};
    //    strcpy(val.colName, colName);
    val.pKey = jsonKey;
    taosHashPut(keyHash, jsonKey, keyLen, &keyLen,
                CHAR_BYTES);  // add key to hash to remove dumplicate, value is useless

    if (item->type == cJSON_String) {  // add json value  format: type|data
      char*   jsonValue = item->valuestring;
      int32_t valLen = (int32_t)strlen(jsonValue);
      char*   tmp = taosMemoryCalloc(1, valLen * TSDB_NCHAR_SIZE);
      if (!tmp) {
        retCode = TSDB_CODE_OUT_OF_MEMORY;
        goto end;
      }
      val.type = TSDB_DATA_TYPE_NCHAR;
      if (valLen > 0 && !taosMbsToUcs4(jsonValue, valLen, (TdUcs4*)tmp, (int32_t)(valLen * TSDB_NCHAR_SIZE), &valLen)) {
        uError("charset:%s to %s. val:%s, errno:%s, convert failed.", DEFAULT_UNICODE_ENCODEC, tsCharset, jsonValue,
               strerror(errno));
        retCode = buildSyntaxErrMsg(pMsgBuf, "charset convert json error", jsonValue);
        taosMemoryFree(tmp);
        goto end;
      }
      val.nData = valLen;
      val.pData = tmp;
    } else if (item->type == cJSON_Number) {
      if (!isfinite(item->valuedouble)) {
        uError("json value is invalidate");
        retCode = buildSyntaxErrMsg(pMsgBuf, "json value number is illegal", json);
        goto end;
      }
      val.type = TSDB_DATA_TYPE_DOUBLE;
      *((double*)&(val.i64)) = item->valuedouble;
    } else if (item->type == cJSON_True || item->type == cJSON_False) {
      val.type = TSDB_DATA_TYPE_BOOL;
      *((char*)&(val.i64)) = (char)(item->valueint);
    } else if (item->type == cJSON_NULL) {
      val.type = TSDB_DATA_TYPE_NULL;
    } else {
      retCode = buildSyntaxErrMsg(pMsgBuf, "invalidate json value", json);
      goto end;
    }
    taosArrayPush(pTagVals, &val);
  }

end:
  taosHashCleanup(keyHash);
  if (retCode == TSDB_CODE_SUCCESS) {
    retCode = tTagNew(pTagVals, 1, true, ppTag);
  }
  for (int i = 0; i < taosArrayGetSize(pTagVals); ++i) {
    STagVal* p = (STagVal*)taosArrayGet(pTagVals, i);
    if (IS_VAR_DATA_TYPE(p->type)) {
      taosMemoryFreeClear(p->pData);
    }
  }
  cJSON_Delete(root);
  return retCode;
}

static int32_t getInsTagsTableTargetNameFromOp(int32_t acctId, SOperatorNode* pOper, SName* pName) {
  if (OP_TYPE_EQUAL != pOper->opType) {
    return TSDB_CODE_SUCCESS;
  }

  SColumnNode* pCol = NULL;
  SValueNode*  pVal = NULL;
  if (QUERY_NODE_COLUMN == nodeType(pOper->pLeft)) {
    pCol = (SColumnNode*)pOper->pLeft;
  } else if (QUERY_NODE_VALUE == nodeType(pOper->pLeft)) {
    pVal = (SValueNode*)pOper->pLeft;
  }
  if (QUERY_NODE_COLUMN == nodeType(pOper->pRight)) {
    pCol = (SColumnNode*)pOper->pRight;
  } else if (QUERY_NODE_VALUE == nodeType(pOper->pRight)) {
    pVal = (SValueNode*)pOper->pRight;
  }
  if (NULL == pCol || NULL == pVal || NULL == pVal->literal || 0 == strcmp(pVal->literal, "")) {
    return TSDB_CODE_SUCCESS;
  }

  if (0 == strcmp(pCol->colName, "db_name")) {
    return tNameSetDbName(pName, acctId, pVal->literal, strlen(pVal->literal));
  } else if (0 == strcmp(pCol->colName, "table_name")) {
    return tNameAddTbName(pName, pVal->literal, strlen(pVal->literal));
  }

  return TSDB_CODE_SUCCESS;
}

static void getInsTagsTableTargetObjName(int32_t acctId, SNode* pNode, SName* pName) {
  if (QUERY_NODE_OPERATOR == nodeType(pNode)) {
    getInsTagsTableTargetNameFromOp(acctId, (SOperatorNode*)pNode, pName);
  }
}

static int32_t getInsTagsTableTargetNameFromCond(int32_t acctId, SLogicConditionNode* pCond, SName* pName) {
  if (LOGIC_COND_TYPE_AND != pCond->condType) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pCond->pParameterList) { getInsTagsTableTargetObjName(acctId, pNode, pName); }
  if ('\0' == pName->dbname[0]) {
    pName->type = 0;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t getVnodeSysTableTargetName(int32_t acctId, SNode* pWhere, SName* pName) {
  if (NULL == pWhere) {
    return TSDB_CODE_SUCCESS;
  }

  if (QUERY_NODE_OPERATOR == nodeType(pWhere)) {
    int32_t code = getInsTagsTableTargetNameFromOp(acctId, (SOperatorNode*)pWhere, pName);
    if (TSDB_CODE_SUCCESS == code && '\0' == pName->dbname[0]) {
      pName->type = 0;
    }
    return code;
  }

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pWhere)) {
    return getInsTagsTableTargetNameFromCond(acctId, (SLogicConditionNode*)pWhere, pName);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t userAuthToString(int32_t acctId, const char* pUser, const char* pDb, const char* pTable, AUTH_TYPE type,
                                char* pStr, bool isView) {
  return sprintf(pStr, "%s*%d*%s*%s*%d*%d", pUser, acctId, pDb, (NULL == pTable || '\0' == pTable[0]) ? "``" : pTable,
                 type, isView);
}

static int32_t getIntegerFromAuthStr(const char* pStart, char** pNext) {
  char* p = strchr(pStart, '*');
  char  buf[10] = {0};
  if (NULL == p) {
    strcpy(buf, pStart);
    *pNext = NULL;
  } else {
    strncpy(buf, pStart, p - pStart);
    *pNext = ++p;
  }
  return taosStr2Int32(buf, NULL, 10);
}

static void getStringFromAuthStr(const char* pStart, char* pStr, char** pNext) {
  char* p = strchr(pStart, '*');
  if (NULL == p) {
    strcpy(pStr, pStart);
    *pNext = NULL;
  } else {
    strncpy(pStr, pStart, p - pStart);
    *pNext = ++p;
  }
  if (*pStart == '`' && *(pStart + 1) == '`') {
    *pStr = 0;
  }
}

static void stringToUserAuth(const char* pStr, int32_t len, SUserAuthInfo* pUserAuth) {
  char* p = NULL;
  getStringFromAuthStr(pStr, pUserAuth->user, &p);
  pUserAuth->tbName.acctId = getIntegerFromAuthStr(p, &p);
  getStringFromAuthStr(p, pUserAuth->tbName.dbname, &p);
  getStringFromAuthStr(p, pUserAuth->tbName.tname, &p);
  if (pUserAuth->tbName.tname[0]) {
    pUserAuth->tbName.type = TSDB_TABLE_NAME_T;
  } else {
    pUserAuth->tbName.type = TSDB_DB_NAME_T;
  }
  pUserAuth->type = getIntegerFromAuthStr(p, &p);
  pUserAuth->isView = getIntegerFromAuthStr(p, &p);
}

static int32_t buildTableReq(SHashObj* pTablesHash, SArray** pTables) {
  if (NULL != pTablesHash) {
    *pTables = taosArrayInit(taosHashGetSize(pTablesHash), sizeof(SName));
    if (NULL == *pTables) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    void* p = taosHashIterate(pTablesHash, NULL);
    while (NULL != p) {
      size_t len = 0;
      char*  pKey = taosHashGetKey(p, &len);
      char   fullName[TSDB_TABLE_FNAME_LEN] = {0};
      strncpy(fullName, pKey, len);
      SName name = {0};
      tNameFromString(&name, fullName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
      taosArrayPush(*pTables, &name);
      p = taosHashIterate(pTablesHash, p);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t buildDbReq(SHashObj* pDbsHash, SArray** pDbs) {
  if (NULL != pDbsHash) {
    *pDbs = taosArrayInit(taosHashGetSize(pDbsHash), TSDB_DB_FNAME_LEN);
    if (NULL == *pDbs) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    void* p = taosHashIterate(pDbsHash, NULL);
    while (NULL != p) {
      size_t len = 0;
      char*  pKey = taosHashGetKey(p, &len);
      char   fullName[TSDB_DB_FNAME_LEN] = {0};
      strncpy(fullName, pKey, len);
      taosArrayPush(*pDbs, fullName);
      p = taosHashIterate(pDbsHash, p);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t buildTableReqFromDb(SHashObj* pDbsHash, SArray** pDbs) {
  if (NULL != pDbsHash) {
    if (NULL == *pDbs) {
      *pDbs = taosArrayInit(taosHashGetSize(pDbsHash), sizeof(STablesReq));
      if (NULL == *pDbs) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }
    SParseTablesMetaReq* p = taosHashIterate(pDbsHash, NULL);
    while (NULL != p) {
      STablesReq req = {0};
      strcpy(req.dbFName, p->dbFName);
      buildTableReq(p->pTables, &req.pTables);
      taosArrayPush(*pDbs, &req);
      p = taosHashIterate(pDbsHash, p);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t buildUserAuthReq(SHashObj* pUserAuthHash, SArray** pUserAuth) {
  if (NULL != pUserAuthHash) {
    *pUserAuth = taosArrayInit(taosHashGetSize(pUserAuthHash), sizeof(SUserAuthInfo));
    if (NULL == *pUserAuth) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    void* p = taosHashIterate(pUserAuthHash, NULL);
    while (NULL != p) {
      size_t len = 0;
      char*  pKey = taosHashGetKey(p, &len);
      char   key[USER_AUTH_KEY_MAX_LEN] = {0};
      strncpy(key, pKey, len);
      SUserAuthInfo userAuth = {0};
      stringToUserAuth(key, len, &userAuth);
      taosArrayPush(*pUserAuth, &userAuth);
      p = taosHashIterate(pUserAuthHash, p);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t buildUdfReq(SHashObj* pUdfHash, SArray** pUdf) {
  if (NULL != pUdfHash) {
    *pUdf = taosArrayInit(taosHashGetSize(pUdfHash), TSDB_FUNC_NAME_LEN);
    if (NULL == *pUdf) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    void* p = taosHashIterate(pUdfHash, NULL);
    while (NULL != p) {
      size_t len = 0;
      char*  pFunc = taosHashGetKey(p, &len);
      char   func[TSDB_FUNC_NAME_LEN] = {0};
      strncpy(func, pFunc, len);
      taosArrayPush(*pUdf, func);
      p = taosHashIterate(pUdfHash, p);
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t buildCatalogReq(const SParseMetaCache* pMetaCache, SCatalogReq* pCatalogReq) {
  int32_t code = buildTableReqFromDb(pMetaCache->pTableMeta, &pCatalogReq->pTableMeta);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildDbReq(pMetaCache->pDbVgroup, &pCatalogReq->pDbVgroup);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildTableReqFromDb(pMetaCache->pTableVgroup, &pCatalogReq->pTableHash);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildDbReq(pMetaCache->pDbCfg, &pCatalogReq->pDbCfg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildDbReq(pMetaCache->pDbInfo, &pCatalogReq->pDbInfo);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildUserAuthReq(pMetaCache->pUserAuth, &pCatalogReq->pUser);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildUdfReq(pMetaCache->pUdf, &pCatalogReq->pUdf);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildTableReq(pMetaCache->pTableIndex, &pCatalogReq->pTableIndex);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildTableReq(pMetaCache->pTableCfg, &pCatalogReq->pTableCfg);
  }
#ifdef TD_ENTERPRISE
  if (TSDB_CODE_SUCCESS == code) {
    code = buildTableReqFromDb(pMetaCache->pTableMeta, &pCatalogReq->pView);
  }  
#endif  
  pCatalogReq->dNodeRequired = pMetaCache->dnodeRequired;
  return code;
}


SNode* createSelectStmtImpl(bool isDistinct, SNodeList* pProjectionList, SNode* pTable, SNodeList* pHint) {
  SSelectStmt* select = (SSelectStmt*)nodesMakeNode(QUERY_NODE_SELECT_STMT);
  if (NULL == select) {
    return NULL;
  }
  select->isDistinct = isDistinct;
  select->pProjectionList = pProjectionList;
  select->pFromTable = pTable;
  sprintf(select->stmtName, "%p", select);
  select->timeLineResMode = select->isDistinct ? TIME_LINE_NONE : TIME_LINE_GLOBAL;
  select->onlyHasKeepOrderFunc = true;
  select->timeRange = TSWINDOW_INITIALIZER;
  select->pHint = pHint;
  return (SNode*)select;
}

static int32_t putMetaDataToHash(const char* pKey, int32_t len, const SArray* pData, int32_t index, SHashObj** pHash) {
  if (NULL == *pHash) {
    *pHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
    if (NULL == *pHash) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  SMetaRes* pRes = taosArrayGet(pData, index);
  return taosHashPut(*pHash, pKey, len, &pRes, POINTER_BYTES);
}

int32_t getMetaDataFromHash(const char* pKey, int32_t len, SHashObj* pHash, void** pOutput) {
  SMetaRes** pRes = taosHashGet(pHash, pKey, len);
  if (NULL == pRes || NULL == *pRes) {
    return TSDB_CODE_PAR_INTERNAL_ERROR;
  }
  if (TSDB_CODE_SUCCESS == (*pRes)->code) {
    *pOutput = (*pRes)->pRes;
  }
  return (*pRes)->code;
}

static int32_t putTableDataToCache(const SArray* pTableReq, const SArray* pTableData, SHashObj** pTable) {
  int32_t ntables = taosArrayGetSize(pTableReq);
  for (int32_t i = 0; i < ntables; ++i) {
    char fullName[TSDB_TABLE_FNAME_LEN];
    tNameExtractFullName(taosArrayGet(pTableReq, i), fullName);
    if (TSDB_CODE_SUCCESS != putMetaDataToHash(fullName, strlen(fullName), pTableData, i, pTable)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t putDbDataToCache(const SArray* pDbReq, const SArray* pDbData, SHashObj** pDb) {
  int32_t nvgs = taosArrayGetSize(pDbReq);
  for (int32_t i = 0; i < nvgs; ++i) {
    char* pDbFName = taosArrayGet(pDbReq, i);
    if (TSDB_CODE_SUCCESS != putMetaDataToHash(pDbFName, strlen(pDbFName), pDbData, i, pDb)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t putDbTableDataToCache(const SArray* pDbReq, const SArray* pTableData, SHashObj** pTable) {
  int32_t ndbs = taosArrayGetSize(pDbReq);
  int32_t tableNo = 0;
  for (int32_t i = 0; i < ndbs; ++i) {
    STablesReq* pReq = taosArrayGet(pDbReq, i);
    int32_t     ntables = taosArrayGetSize(pReq->pTables);
    for (int32_t j = 0; j < ntables; ++j) {
      char fullName[TSDB_TABLE_FNAME_LEN];
      tNameExtractFullName(taosArrayGet(pReq->pTables, j), fullName);
      if (TSDB_CODE_SUCCESS != putMetaDataToHash(fullName, strlen(fullName), pTableData, tableNo, pTable)) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      ++tableNo;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t putUserAuthToCache(const SArray* pUserAuthReq, const SArray* pUserAuthData, SHashObj** pUserAuth) {
  int32_t nvgs = taosArrayGetSize(pUserAuthReq);
  for (int32_t i = 0; i < nvgs; ++i) {
    SUserAuthInfo* pUser = taosArrayGet(pUserAuthReq, i);
    char           key[USER_AUTH_KEY_MAX_LEN] = {0};
    int32_t        len = userAuthToString(pUser->tbName.acctId, pUser->user, pUser->tbName.dbname, pUser->tbName.tname,
                                          pUser->type, key, pUser->isView);
    if (TSDB_CODE_SUCCESS != putMetaDataToHash(key, len, pUserAuthData, i, pUserAuth)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t putUdfToCache(const SArray* pUdfReq, const SArray* pUdfData, SHashObj** pUdf) {
  int32_t num = taosArrayGetSize(pUdfReq);
  for (int32_t i = 0; i < num; ++i) {
    char* pFunc = taosArrayGet(pUdfReq, i);
    if (TSDB_CODE_SUCCESS != putMetaDataToHash(pFunc, strlen(pFunc), pUdfData, i, pUdf)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t putMetaDataToCache(const SCatalogReq* pCatalogReq, const SMetaData* pMetaData, SParseMetaCache* pMetaCache) {
  int32_t code = putDbTableDataToCache(pCatalogReq->pTableMeta, pMetaData->pTableMeta, &pMetaCache->pTableMeta);
  if (TSDB_CODE_SUCCESS == code) {
    code = putDbDataToCache(pCatalogReq->pDbVgroup, pMetaData->pDbVgroup, &pMetaCache->pDbVgroup);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = putDbTableDataToCache(pCatalogReq->pTableHash, pMetaData->pTableHash, &pMetaCache->pTableVgroup);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = putDbDataToCache(pCatalogReq->pDbCfg, pMetaData->pDbCfg, &pMetaCache->pDbCfg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = putDbDataToCache(pCatalogReq->pDbInfo, pMetaData->pDbInfo, &pMetaCache->pDbInfo);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = putUserAuthToCache(pCatalogReq->pUser, pMetaData->pUser, &pMetaCache->pUserAuth);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = putUdfToCache(pCatalogReq->pUdf, pMetaData->pUdfList, &pMetaCache->pUdf);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = putTableDataToCache(pCatalogReq->pTableIndex, pMetaData->pTableIndex, &pMetaCache->pTableIndex);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = putTableDataToCache(pCatalogReq->pTableCfg, pMetaData->pTableCfg, &pMetaCache->pTableCfg);
  }
#ifdef TD_ENTERPRISE
  if (TSDB_CODE_SUCCESS == code) {
    code = putDbTableDataToCache(pCatalogReq->pView, pMetaData->pView, &pMetaCache->pViews);
  }
#endif  
  pMetaCache->pDnodes = pMetaData->pDnodeList;
  return code;
}

static int32_t reserveTableReqInCacheImpl(const char* pTbFName, int32_t len, SHashObj** pTables) {
  if (NULL == *pTables) {
    *pTables = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
    if (NULL == *pTables) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return taosHashPut(*pTables, pTbFName, len, &nullPointer, POINTER_BYTES);
}

static int32_t reserveTableReqInCache(int32_t acctId, const char* pDb, const char* pTable, SHashObj** pTables) {
  char    fullName[TSDB_TABLE_FNAME_LEN];
  int32_t len = snprintf(fullName, sizeof(fullName), "%d.%s.%s", acctId, pDb, pTable);
  return reserveTableReqInCacheImpl(fullName, len, pTables);
}

static int32_t reserveTableReqInDbCacheImpl(int32_t acctId, const char* pDb, const char* pTable, SHashObj* pDbs) {
  SParseTablesMetaReq req = {0};
  int32_t             len = snprintf(req.dbFName, sizeof(req.dbFName), "%d.%s", acctId, pDb);
  int32_t             code = reserveTableReqInCache(acctId, pDb, pTable, &req.pTables);
  if (TSDB_CODE_SUCCESS == code) {
    code = taosHashPut(pDbs, req.dbFName, len, &req, sizeof(SParseTablesMetaReq));
  }
  return code;
}

static int32_t reserveTableReqInDbCache(int32_t acctId, const char* pDb, const char* pTable, SHashObj** pDbs) {
  if (NULL == *pDbs) {
    *pDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
    if (NULL == *pDbs) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  char                 fullName[TSDB_DB_FNAME_LEN];
  int32_t              len = snprintf(fullName, sizeof(fullName), "%d.%s", acctId, pDb);
  SParseTablesMetaReq* pReq = taosHashGet(*pDbs, fullName, len);
  if (NULL == pReq) {
    return reserveTableReqInDbCacheImpl(acctId, pDb, pTable, *pDbs);
  }
  return reserveTableReqInCache(acctId, pDb, pTable, &pReq->pTables);
}

int32_t reserveTableMetaInCache(int32_t acctId, const char* pDb, const char* pTable, SParseMetaCache* pMetaCache) {
  return reserveTableReqInDbCache(acctId, pDb, pTable, &pMetaCache->pTableMeta);
}

int32_t reserveTableMetaInCacheExt(const SName* pName, SParseMetaCache* pMetaCache) {
  return reserveTableReqInDbCache(pName->acctId, pName->dbname, pName->tname, &pMetaCache->pTableMeta);
}

int32_t getTableMetaFromCache(SParseMetaCache* pMetaCache, const SName* pName, STableMeta** pMeta) {
  char fullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pName, fullName);
  STableMeta* pTableMeta = NULL;
  int32_t     code = getMetaDataFromHash(fullName, strlen(fullName), pMetaCache->pTableMeta, (void**)&pTableMeta);
  if (TSDB_CODE_SUCCESS == code) {
    *pMeta = tableMetaDup(pTableMeta);
    if (NULL == *pMeta) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return code;
}

int32_t buildTableMetaFromViewMeta(STableMeta** pMeta, SViewMeta* pViewMeta) {
  *pMeta = taosMemoryCalloc(1, sizeof(STableMeta) + pViewMeta->numOfCols * sizeof(SSchema));
  if (NULL == *pMeta) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  (*pMeta)->uid = pViewMeta->viewId;
  (*pMeta)->vgId = MNODE_HANDLE;
  (*pMeta)->tableType = TSDB_VIEW_TABLE;
  (*pMeta)->sversion = pViewMeta->version;
  (*pMeta)->tversion = pViewMeta->version;
  (*pMeta)->tableInfo.precision = pViewMeta->precision;
  (*pMeta)->tableInfo.numOfColumns = pViewMeta->numOfCols;
  memcpy((*pMeta)->schema, pViewMeta->pSchema, sizeof(SSchema) * pViewMeta->numOfCols);
  
  for (int32_t i = 0; i < pViewMeta->numOfCols; ++i) {
    (*pMeta)->tableInfo.rowSize += (*pMeta)->schema[i].bytes;
  }    
  return TSDB_CODE_SUCCESS;
}

int32_t getViewMetaFromCache(SParseMetaCache* pMetaCache, const SName* pName, STableMeta** pMeta) {
  char fullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pName, fullName);
  SViewMeta* pViewMeta = NULL;
  int32_t     code = getMetaDataFromHash(fullName, strlen(fullName), pMetaCache->pViews, (void**)&pViewMeta);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildTableMetaFromViewMeta(pMeta, pViewMeta);
  }
  return code;
}


static int32_t reserveDbReqInCache(int32_t acctId, const char* pDb, SHashObj** pDbs) {
  if (NULL == *pDbs) {
    *pDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
    if (NULL == *pDbs) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  char    fullName[TSDB_TABLE_FNAME_LEN];
  int32_t len = snprintf(fullName, sizeof(fullName), "%d.%s", acctId, pDb);
  return taosHashPut(*pDbs, fullName, len, &nullPointer, POINTER_BYTES);
}

int32_t reserveDbVgInfoInCache(int32_t acctId, const char* pDb, SParseMetaCache* pMetaCache) {
  return reserveDbReqInCache(acctId, pDb, &pMetaCache->pDbVgroup);
}

int32_t getDbVgInfoFromCache(SParseMetaCache* pMetaCache, const char* pDbFName, SArray** pVgInfo) {
  SArray* pVgList = NULL;
  int32_t code = getMetaDataFromHash(pDbFName, strlen(pDbFName), pMetaCache->pDbVgroup, (void**)&pVgList);
  // pVgList is null, which is a legal value, indicating that the user DB has not been created
  if (TSDB_CODE_SUCCESS == code && NULL != pVgList) {
    *pVgInfo = taosArrayDup(pVgList, NULL);
    if (NULL == *pVgInfo) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return code;
}

int32_t reserveTableVgroupInCache(int32_t acctId, const char* pDb, const char* pTable, SParseMetaCache* pMetaCache) {
  return reserveTableReqInDbCache(acctId, pDb, pTable, &pMetaCache->pTableVgroup);
}

int32_t reserveTableVgroupInCacheExt(const SName* pName, SParseMetaCache* pMetaCache) {
  return reserveTableReqInDbCache(pName->acctId, pName->dbname, pName->tname, &pMetaCache->pTableVgroup);
}

int32_t getTableVgroupFromCache(SParseMetaCache* pMetaCache, const SName* pName, SVgroupInfo* pVgroup) {
  char fullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pName, fullName);
  SVgroupInfo* pVg = NULL;
  int32_t      code = getMetaDataFromHash(fullName, strlen(fullName), pMetaCache->pTableVgroup, (void**)&pVg);
  if (TSDB_CODE_SUCCESS == code) {
    memcpy(pVgroup, pVg, sizeof(SVgroupInfo));
  }
  return code;
}

int32_t reserveDbVgVersionInCache(int32_t acctId, const char* pDb, SParseMetaCache* pMetaCache) {
  return reserveDbReqInCache(acctId, pDb, &pMetaCache->pDbInfo);
}

int32_t getDbVgVersionFromCache(SParseMetaCache* pMetaCache, const char* pDbFName, int32_t* pVersion, int64_t* pDbId,
                                int32_t* pTableNum, int64_t* pStateTs) {
  SDbInfo* pDbInfo = NULL;
  int32_t  code = getMetaDataFromHash(pDbFName, strlen(pDbFName), pMetaCache->pDbInfo, (void**)&pDbInfo);
  if (TSDB_CODE_SUCCESS == code) {
    *pVersion = pDbInfo->vgVer;
    *pDbId = pDbInfo->dbId;
    *pTableNum = pDbInfo->tbNum;
    *pStateTs = pDbInfo->stateTs;
  }
  return code;
}

int32_t reserveDbCfgInCache(int32_t acctId, const char* pDb, SParseMetaCache* pMetaCache) {
  return reserveDbReqInCache(acctId, pDb, &pMetaCache->pDbCfg);
}

int32_t getDbCfgFromCache(SParseMetaCache* pMetaCache, const char* pDbFName, SDbCfgInfo* pInfo) {
  SDbCfgInfo* pDbCfg = NULL;
  int32_t     code = getMetaDataFromHash(pDbFName, strlen(pDbFName), pMetaCache->pDbCfg, (void**)&pDbCfg);
  if (TSDB_CODE_SUCCESS == code) {
    memcpy(pInfo, pDbCfg, sizeof(SDbCfgInfo));
  }
  return code;
}

static int32_t reserveUserAuthInCacheImpl(const char* pKey, int32_t len, SParseMetaCache* pMetaCache) {
  if (NULL == pMetaCache->pUserAuth) {
    pMetaCache->pUserAuth = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
    if (NULL == pMetaCache->pUserAuth) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return taosHashPut(pMetaCache->pUserAuth, pKey, len, &nullPointer, POINTER_BYTES);
}

int32_t reserveUserAuthInCache(int32_t acctId, const char* pUser, const char* pDb, const char* pTable, AUTH_TYPE type,
                               SParseMetaCache* pMetaCache) {
  char    key[USER_AUTH_KEY_MAX_LEN] = {0};
  int32_t len = userAuthToString(acctId, pUser, pDb, pTable, type, key, false);
  return reserveUserAuthInCacheImpl(key, len, pMetaCache);
}

int32_t reserveViewUserAuthInCache(int32_t acctId, const char* pUser, const char* pDb, const char* pTable, AUTH_TYPE type,
                              SParseMetaCache* pMetaCache) {
 char    key[USER_AUTH_KEY_MAX_LEN] = {0};
 int32_t len = userAuthToString(acctId, pUser, pDb, pTable, type, key, true);
 return reserveUserAuthInCacheImpl(key, len, pMetaCache);
}



int32_t getUserAuthFromCache(SParseMetaCache* pMetaCache, SUserAuthInfo* pAuthReq, SUserAuthRes* pAuthRes) {
  char          key[USER_AUTH_KEY_MAX_LEN] = {0};
  int32_t       len = userAuthToString(pAuthReq->tbName.acctId, pAuthReq->user, pAuthReq->tbName.dbname,
                                       pAuthReq->tbName.tname, pAuthReq->type, key, pAuthReq->isView);
  SUserAuthRes* pAuth = NULL;
  int32_t       code = getMetaDataFromHash(key, len, pMetaCache->pUserAuth, (void**)&pAuth);
  if (TSDB_CODE_SUCCESS == code) {
    memcpy(pAuthRes, pAuth, sizeof(SUserAuthRes));
  }
  return code;
}

int32_t reserveUdfInCache(const char* pFunc, SParseMetaCache* pMetaCache) {
  if (NULL == pMetaCache->pUdf) {
    pMetaCache->pUdf = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
    if (NULL == pMetaCache->pUdf) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return taosHashPut(pMetaCache->pUdf, pFunc, strlen(pFunc), &nullPointer, POINTER_BYTES);
}

int32_t getUdfInfoFromCache(SParseMetaCache* pMetaCache, const char* pFunc, SFuncInfo* pInfo) {
  SFuncInfo* pFuncInfo = NULL;
  int32_t    code = getMetaDataFromHash(pFunc, strlen(pFunc), pMetaCache->pUdf, (void**)&pFuncInfo);
  if (TSDB_CODE_SUCCESS == code) {
    memcpy(pInfo, pFuncInfo, sizeof(SFuncInfo));
  }
  return code;
}

static void destroySmaIndex(void* p) { taosMemoryFree(((STableIndexInfo*)p)->expr); }

static SArray* smaIndexesDup(SArray* pSrc) {
  SArray* pDst = taosArrayDup(pSrc, NULL);
  if (NULL == pDst) {
    return NULL;
  }
  int32_t size = taosArrayGetSize(pDst);
  for (int32_t i = 0; i < size; ++i) {
    ((STableIndexInfo*)taosArrayGet(pDst, i))->expr = NULL;
  }
  for (int32_t i = 0; i < size; ++i) {
    STableIndexInfo* pIndex = taosArrayGet(pDst, i);
    pIndex->expr = taosStrdup(((STableIndexInfo*)taosArrayGet(pSrc, i))->expr);
    if (NULL == pIndex->expr) {
      taosArrayDestroyEx(pDst, destroySmaIndex);
      return NULL;
    }
  }
  return pDst;
}

int32_t reserveTableIndexInCache(int32_t acctId, const char* pDb, const char* pTable, SParseMetaCache* pMetaCache) {
  return reserveTableReqInCache(acctId, pDb, pTable, &pMetaCache->pTableIndex);
}

int32_t reserveTableCfgInCache(int32_t acctId, const char* pDb, const char* pTable, SParseMetaCache* pMetaCache) {
  return reserveTableReqInCache(acctId, pDb, pTable, &pMetaCache->pTableCfg);
}

int32_t getTableIndexFromCache(SParseMetaCache* pMetaCache, const SName* pName, SArray** pIndexes) {
  char fullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pName, fullName);
  SArray* pSmaIndexes = NULL;
  int32_t code = getMetaDataFromHash(fullName, strlen(fullName), pMetaCache->pTableIndex, (void**)&pSmaIndexes);
  if (TSDB_CODE_SUCCESS == code && NULL != pSmaIndexes) {
    *pIndexes = smaIndexesDup(pSmaIndexes);
    if (NULL == *pIndexes) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return code;
}

STableCfg* tableCfgDup(STableCfg* pCfg) {
  STableCfg* pNew = taosMemoryMalloc(sizeof(*pNew));

  memcpy(pNew, pCfg, sizeof(*pNew));
  if (NULL != pNew->pComment) {
    pNew->pComment = taosMemoryCalloc(pNew->commentLen + 1, 1);
    memcpy(pNew->pComment, pCfg->pComment, pNew->commentLen);
  }
  if (NULL != pNew->pFuncs) {
    pNew->pFuncs = taosArrayDup(pNew->pFuncs, NULL);
  }
  if (NULL != pNew->pTags) {
    pNew->pTags = taosMemoryCalloc(pNew->tagsLen + 1, 1);
    memcpy(pNew->pTags, pCfg->pTags, pNew->tagsLen);
  }

  int32_t schemaSize = (pCfg->numOfColumns + pCfg->numOfTags) * sizeof(SSchema);

  SSchema* pSchema = taosMemoryMalloc(schemaSize);
  memcpy(pSchema, pCfg->pSchemas, schemaSize);

  pNew->pSchemas = pSchema;

  return pNew;
}

int32_t getTableCfgFromCache(SParseMetaCache* pMetaCache, const SName* pName, STableCfg** pOutput) {
  char fullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pName, fullName);
  STableCfg* pCfg = NULL;
  int32_t    code = getMetaDataFromHash(fullName, strlen(fullName), pMetaCache->pTableCfg, (void**)&pCfg);
  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = tableCfgDup(pCfg);
    if (NULL == *pOutput) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return code;
}

int32_t reserveDnodeRequiredInCache(SParseMetaCache* pMetaCache) {
  pMetaCache->dnodeRequired = true;
  return TSDB_CODE_SUCCESS;
}

int32_t getDnodeListFromCache(SParseMetaCache* pMetaCache, SArray** pDnodes) {
  SMetaRes* pRes = taosArrayGet(pMetaCache->pDnodes, 0);
  if (TSDB_CODE_SUCCESS != pRes->code) {
    return pRes->code;
  }

  *pDnodes = taosArrayDup((SArray*)pRes->pRes, NULL);
  if (NULL == *pDnodes) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

void destoryParseTablesMetaReqHash(SHashObj* pHash) {
  SParseTablesMetaReq* p = taosHashIterate(pHash, NULL);
  while (NULL != p) {
    taosHashCleanup(p->pTables);
    p = taosHashIterate(pHash, p);
  }
  taosHashCleanup(pHash);
}

void destoryParseMetaCache(SParseMetaCache* pMetaCache, bool request) {
  if (request) {
    destoryParseTablesMetaReqHash(pMetaCache->pTableMeta);
    destoryParseTablesMetaReqHash(pMetaCache->pTableVgroup);
    destoryParseTablesMetaReqHash(pMetaCache->pViews);
  } else {
    taosHashCleanup(pMetaCache->pTableMeta);
    taosHashCleanup(pMetaCache->pTableVgroup);
    taosHashCleanup(pMetaCache->pViews);
  }
  taosHashCleanup(pMetaCache->pDbVgroup);
  taosHashCleanup(pMetaCache->pDbCfg);
  taosHashCleanup(pMetaCache->pDbInfo);
  taosHashCleanup(pMetaCache->pUserAuth);
  taosHashCleanup(pMetaCache->pUdf);
  taosHashCleanup(pMetaCache->pTableIndex);
  taosHashCleanup(pMetaCache->pTableCfg);
}

int64_t int64SafeSub(int64_t a, int64_t b) {
  int64_t res = (uint64_t)a - (uint64_t)b;

  if (a >= 0 && b < 0) {
    if ((uint64_t)res > (uint64_t)INT64_MAX) {
      // overflow
      res = INT64_MAX;
    }
  } else if (a < 0 && b > 0 && res >= 0) {
    // underflow
    res = INT64_MIN;
  }
  return res;
}


