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

#define USER_AUTH_KEY_MAX_LEN TSDB_USER_LEN + TSDB_DB_FNAME_LEN + 2

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
    case TSDB_CODE_PAR_AMBIGUOUS_COLUMN:
      return "Column ambiguously defined: %s";
    case TSDB_CODE_PAR_WRONG_VALUE_TYPE:
      return "Invalid value type: %s";
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
      return "Interval cannot be less than %d us";
    case TSDB_CODE_PAR_DB_NOT_SPECIFIED:
      return "Database not specified";
    case TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME:
      return "Invalid identifier name: %s";
    case TSDB_CODE_PAR_CORRESPONDING_STABLE_ERR:
      return "Corresponding super table not in this db";
    case TSDB_CODE_PAR_INVALID_RANGE_OPTION:
      return "Invalid option %s: %" PRId64 " valid range: [%d, %d]";
    case TSDB_CODE_PAR_INVALID_STR_OPTION:
      return "Invalid option %s: %s";
    case TSDB_CODE_PAR_INVALID_ENUM_OPTION:
      return "Invalid option %s: %" PRId64 ", only %d, %d allowed";
    case TSDB_CODE_PAR_INVALID_KEEP_NUM:
      return "Invalid number of keep options";
    case TSDB_CODE_PAR_INVALID_KEEP_ORDER:
      return "Invalid keep value, should be keep0 <= keep1 <= keep2";
    case TSDB_CODE_PAR_INVALID_KEEP_VALUE:
      return "Invalid option keep: %d, %d, %d valid range: [%d, %d]";
    case TSDB_CODE_PAR_INVALID_COMMENT_OPTION:
      return "Invalid option comment, length cannot exceed %d";
    case TSDB_CODE_PAR_INVALID_F_RANGE_OPTION:
      return "Invalid option %s: %f valid range: [%d, %d]";
    case TSDB_CODE_PAR_INVALID_ROLLUP_OPTION:
      return "Invalid option rollup: only one function is allowed";
    case TSDB_CODE_PAR_INVALID_RETENTIONS_OPTION:
      return "Invalid option retentions";
    case TSDB_CODE_PAR_GROUPBY_WINDOW_COEXIST:
      return "GROUP BY and WINDOW-clause can't be used together";
    case TSDB_CODE_PAR_INVALID_OPTION_UNIT:
      return "Invalid option %s unit: %c, only m, h, d allowed";
    case TSDB_CODE_PAR_INVALID_KEEP_UNIT:
      return "Invalid option keep unit: %c, only m, h, d allowed";
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
      return "sliding value can not less than 1% of interval value";
    case TSDB_CODE_PAR_ONLY_ONE_JSON_TAG:
      return "Only one tag if there is a json tag";
    case TSDB_CODE_PAR_INCORRECT_NUM_OF_COL:
      return "Query block has incorrect number of result columns";
    case TSDB_CODE_PAR_INCORRECT_TIMESTAMP_VAL:
      return "Incorrect TIMESTAMP value: %s";
    case TSDB_CODE_PAR_INVALID_DAYS_VALUE:
      return "Invalid days value, should be keep2 >= keep1 >= keep0 >= days";
    case TSDB_CODE_PAR_OFFSET_LESS_ZERO:
      return "soffset/offset can not be less than 0";
    case TSDB_CODE_PAR_SLIMIT_LEAK_PARTITION_BY:
      return "slimit/soffset only available for PARTITION BY query";
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
      return "Invalid binary/nchar column length";
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
      return "Only binary/nchar column length could be modified";
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
    case TSDB_CODE_OUT_OF_MEMORY:
      return "Out of memory";
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

int32_t buildInvalidOperationMsg(SMsgBuf* pBuf, const char* msg) {
  strncpy(pBuf->buf, msg, pBuf->len);
  return TSDB_CODE_TSC_INVALID_OPERATION;
}

int32_t buildSyntaxErrMsg(SMsgBuf* pBuf, const char* additionalInfo, const char* sourceStr) {
  const char* msgFormat1 = "syntax error near \'%s\'";
  const char* msgFormat2 = "syntax error near \'%s\' (%s)";
  const char* msgFormat3 = "%s";

  const char* prefix = "syntax error";
  if (sourceStr == NULL) {
    assert(additionalInfo != NULL);
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

SSchema* getTableColumnSchema(const STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);
  return (SSchema*)pTableMeta->schema;
}

static SSchema* getOneColumnSchema(const STableMeta* pTableMeta, int32_t colIndex) {
  assert(pTableMeta != NULL && pTableMeta->schema != NULL && colIndex >= 0 &&
         colIndex < (getNumOfColumns(pTableMeta) + getNumOfTags(pTableMeta)));

  SSchema* pSchema = (SSchema*)pTableMeta->schema;
  return &pSchema[colIndex];
}

SSchema* getTableTagSchema(const STableMeta* pTableMeta) {
  assert(pTableMeta != NULL &&
         (pTableMeta->tableType == TSDB_SUPER_TABLE || pTableMeta->tableType == TSDB_CHILD_TABLE));
  return getOneColumnSchema(pTableMeta, getTableInfo(pTableMeta).numOfColumns);
}

int32_t getNumOfColumns(const STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);
  // table created according to super table, use data from super table
  return getTableInfo(pTableMeta).numOfColumns;
}

int32_t getNumOfTags(const STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);
  return getTableInfo(pTableMeta).numOfTags;
}

STableComInfo getTableInfo(const STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);
  return pTableMeta->tableInfo;
}

STableMeta* tableMetaDup(const STableMeta* pTableMeta) {
  size_t size = TABLE_META_SIZE(pTableMeta);

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
      } else if (src[k + 1] == '%' || src[k + 1] == '_') {
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
  dst[j] = '\0';
  return j;
}

static bool isValidateTag(char* input) {
  if (!input) return false;
  for (size_t i = 0; i < strlen(input); ++i) {
    if (isprint(input[i]) == 0) return false;
  }
  return true;
}

int32_t parseJsontoTagData(const char* json, SKVRowBuilder* kvRowBuilder, SMsgBuf* pMsgBuf, int16_t startColId) {
  // set json NULL data
  uint8_t jsonNULL = TSDB_DATA_TYPE_NULL;
  int32_t jsonIndex = startColId + 1;
  if (!json || strtrim((char*)json) == 0 || strcasecmp(json, TSDB_DATA_NULL_STR_L) == 0) {
    tdAddColToKVRow(kvRowBuilder, jsonIndex, &jsonNULL, CHAR_BYTES);
    return TSDB_CODE_SUCCESS;
  }

  // set json real data
  cJSON* root = cJSON_Parse(json);
  if (root == NULL) {
    return buildSyntaxErrMsg(pMsgBuf, "json parse error", json);
  }

  int32_t size = cJSON_GetArraySize(root);
  if (!cJSON_IsObject(root)) {
    return buildSyntaxErrMsg(pMsgBuf, "json error invalide value", json);
  }

  int32_t   retCode = 0;
  char*     tagKV = NULL;
  SHashObj* keyHash = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, false);
  for (int32_t i = 0; i < size; i++) {
    cJSON* item = cJSON_GetArrayItem(root, i);
    if (!item) {
      qError("json inner error:%d", i);
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
      qError("json key too long error");
      retCode = buildSyntaxErrMsg(pMsgBuf, "json key too long, more than 256", jsonKey);
      goto end;
    }
    if (keyLen == 0 || taosHashGet(keyHash, jsonKey, keyLen) != NULL) {
      continue;
    }
    // key: keyLen + VARSTR_HEADER_SIZE, value type: CHAR_BYTES, value reserved: DOUBLE_BYTES
    tagKV = taosMemoryCalloc(keyLen + VARSTR_HEADER_SIZE + CHAR_BYTES + DOUBLE_BYTES, 1);
    if (!tagKV) {
      retCode = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto end;
    }
    strncpy(varDataVal(tagKV), jsonKey, keyLen);
    varDataSetLen(tagKV, keyLen);
    if (taosHashGetSize(keyHash) == 0) {
      uint8_t jsonNotNULL = TSDB_DATA_TYPE_JSON;
      tdAddColToKVRow(kvRowBuilder, jsonIndex++, &jsonNotNULL, CHAR_BYTES);  // add json type
    }
    taosHashPut(keyHash, jsonKey, keyLen, &keyLen,
                CHAR_BYTES);  // add key to hash to remove dumplicate, value is useless

    if (item->type == cJSON_String) {  // add json value  format: type|data
      char*   jsonValue = item->valuestring;
      int32_t valLen = (int32_t)strlen(jsonValue);
      int32_t totalLen = keyLen + VARSTR_HEADER_SIZE + valLen * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE + CHAR_BYTES;
      char*   tmp = taosMemoryRealloc(tagKV, totalLen);
      if (!tmp) {
        retCode = TSDB_CODE_TSC_OUT_OF_MEMORY;
        goto end;
      }
      tagKV = tmp;
      char* valueType = POINTER_SHIFT(tagKV, keyLen + VARSTR_HEADER_SIZE);
      char* valueData = POINTER_SHIFT(tagKV, keyLen + VARSTR_HEADER_SIZE + CHAR_BYTES);
      *valueType = TSDB_DATA_TYPE_NCHAR;
      if (valLen > 0 && !taosMbsToUcs4(jsonValue, valLen, (TdUcs4*)varDataVal(valueData),
                                       (int32_t)(valLen * TSDB_NCHAR_SIZE), &valLen)) {
        qError("charset:%s to %s. val:%s, errno:%s, convert failed.", DEFAULT_UNICODE_ENCODEC, tsCharset, jsonValue,
               strerror(errno));
        retCode = buildSyntaxErrMsg(pMsgBuf, "charset convert json error", jsonValue);
        goto end;
      }

      varDataSetLen(valueData, valLen);
      tdAddColToKVRow(kvRowBuilder, jsonIndex++, tagKV, totalLen);
    } else if (item->type == cJSON_Number) {
      if (!isfinite(item->valuedouble)) {
        qError("json value is invalidate");
        retCode = buildSyntaxErrMsg(pMsgBuf, "json value number is illegal", json);
        goto end;
      }
      char* valueType = POINTER_SHIFT(tagKV, keyLen + VARSTR_HEADER_SIZE);
      char* valueData = POINTER_SHIFT(tagKV, keyLen + VARSTR_HEADER_SIZE + CHAR_BYTES);
      *valueType = TSDB_DATA_TYPE_DOUBLE;
      *((double*)valueData) = item->valuedouble;
      tdAddColToKVRow(kvRowBuilder, jsonIndex++, tagKV, keyLen + VARSTR_HEADER_SIZE + CHAR_BYTES + DOUBLE_BYTES);
    } else if (item->type == cJSON_True || item->type == cJSON_False) {
      char* valueType = POINTER_SHIFT(tagKV, keyLen + VARSTR_HEADER_SIZE);
      char* valueData = POINTER_SHIFT(tagKV, keyLen + VARSTR_HEADER_SIZE + CHAR_BYTES);
      *valueType = TSDB_DATA_TYPE_BOOL;
      *valueData = (char)(item->valueint);
      tdAddColToKVRow(kvRowBuilder, jsonIndex++, tagKV, keyLen + VARSTR_HEADER_SIZE + CHAR_BYTES + CHAR_BYTES);
    } else if (item->type == cJSON_NULL) {
      char* valueType = POINTER_SHIFT(tagKV, keyLen + VARSTR_HEADER_SIZE);
      *valueType = TSDB_DATA_TYPE_NULL;
      tdAddColToKVRow(kvRowBuilder, jsonIndex++, tagKV, keyLen + VARSTR_HEADER_SIZE + CHAR_BYTES);
    } else {
      retCode = buildSyntaxErrMsg(pMsgBuf, "invalidate json value", json);
      goto end;
    }
  }

  if (taosHashGetSize(keyHash) == 0) {  // set json NULL true
    tdAddColToKVRow(kvRowBuilder, jsonIndex, &jsonNULL, CHAR_BYTES);
  }

end:
  taosMemoryFree(tagKV);
  taosHashCleanup(keyHash);
  cJSON_Delete(root);
  return retCode;
}

static int32_t userAuthToString(int32_t acctId, const char* pUser, const char* pDb, AUTH_TYPE type, char* pStr) {
  return sprintf(pStr, "%s*%d.%s*%d", pUser, acctId, pDb, type);
}

static int32_t userAuthToStringExt(const char* pUser, const char* pDbFName, AUTH_TYPE type, char* pStr) {
  return sprintf(pStr, "%s*%s*%d", pUser, pDbFName, type);
}

static void stringToUserAuth(const char* pStr, int32_t len, SUserAuthInfo* pUserAuth) {
  char* p1 = strchr(pStr, '*');
  strncpy(pUserAuth->user, pStr, p1 - pStr);
  ++p1;
  char* p2 = strchr(p1, '*');
  strncpy(pUserAuth->dbFName, p1, p2 - p1);
  ++p2;
  char buf[10] = {0};
  strncpy(buf, p2, len - (p2 - pStr));
  pUserAuth->type = taosStr2Int32(buf, NULL, 10);
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

static int32_t buildTableMetaReq(SHashObj* pTableMetaHash, SArray** pTableMeta) {
  return buildTableReq(pTableMetaHash, pTableMeta);
}

static int32_t buildDbVgroupReq(SHashObj* pDbVgroupHash, SArray** pDbVgroup) {
  return buildDbReq(pDbVgroupHash, pDbVgroup);
}

static int32_t buildTableVgroupReq(SHashObj* pTableVgroupHash, SArray** pTableVgroup) {
  return buildTableReq(pTableVgroupHash, pTableVgroup);
}

static int32_t buildDbCfgReq(SHashObj* pDbCfgHash, SArray** pDbCfg) { return buildDbReq(pDbCfgHash, pDbCfg); }

static int32_t buildUserAuthReq(SHashObj* pUserAuthHash, SArray** pUserAuth) {
  if (NULL != pUserAuthHash) {
    *pUserAuth = taosArrayInit(taosHashGetSize(pUserAuthHash), sizeof(SUserAuthInfo));
    if (NULL == *pUserAuth) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    void* p = taosHashIterate(pUserAuthHash, NULL);
    while (NULL != p) {
      size_t        len = 0;
      char*         pKey = taosHashGetKey(p, &len);
      SUserAuthInfo userAuth = {0};
      stringToUserAuth(pKey, len, &userAuth);
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
  int32_t code = buildTableMetaReq(pMetaCache->pTableMeta, &pCatalogReq->pTableMeta);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildDbVgroupReq(pMetaCache->pDbVgroup, &pCatalogReq->pDbVgroup);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildTableVgroupReq(pMetaCache->pTableVgroup, &pCatalogReq->pTableHash);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildDbCfgReq(pMetaCache->pDbCfg, &pCatalogReq->pDbCfg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildUserAuthReq(pMetaCache->pUserAuth, &pCatalogReq->pUser);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildUdfReq(pMetaCache->pUdf, &pCatalogReq->pUdf);
  }
  return code;
}

static int32_t putTableMetaToCache(const SArray* pTableMetaReq, const SArray* pTableMetaData, SHashObj* pTableMeta) {
  int32_t ntables = taosArrayGetSize(pTableMetaReq);
  for (int32_t i = 0; i < ntables; ++i) {
    char fullName[TSDB_TABLE_FNAME_LEN];
    tNameExtractFullName(taosArrayGet(pTableMetaReq, i), fullName);
    if (TSDB_CODE_SUCCESS !=
        taosHashPut(pTableMeta, fullName, strlen(fullName), taosArrayGet(pTableMetaData, i), POINTER_BYTES)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t putDbVgroupToCache(const SArray* pDbVgroupReq, const SArray* pDbVgroupData, SHashObj* pDbVgroup) {
  int32_t nvgs = taosArrayGetSize(pDbVgroupReq);
  for (int32_t i = 0; i < nvgs; ++i) {
    char* pDbFName = taosArrayGet(pDbVgroupReq, i);
    if (TSDB_CODE_SUCCESS !=
        taosHashPut(pDbVgroup, pDbFName, strlen(pDbFName), taosArrayGet(pDbVgroupData, i), POINTER_BYTES)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t putTableVgroupToCache(const SArray* pTableVgroupReq, const SArray* pTableVgroupData,
                                     SHashObj* pTableVgroup) {
  int32_t ntables = taosArrayGetSize(pTableVgroupReq);
  for (int32_t i = 0; i < ntables; ++i) {
    char fullName[TSDB_TABLE_FNAME_LEN];
    tNameExtractFullName(taosArrayGet(pTableVgroupReq, i), fullName);
    SVgroupInfo* pInfo = taosArrayGet(pTableVgroupData, i);
    if (TSDB_CODE_SUCCESS != taosHashPut(pTableVgroup, fullName, strlen(fullName), &pInfo, POINTER_BYTES)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t putDbCfgToCache(const SArray* pDbCfgReq, const SArray* pDbCfgData, SHashObj* pDbCfg) {
  int32_t nvgs = taosArrayGetSize(pDbCfgReq);
  for (int32_t i = 0; i < nvgs; ++i) {
    char*       pDbFName = taosArrayGet(pDbCfgReq, i);
    SDbCfgInfo* pInfo = taosArrayGet(pDbCfgData, i);
    if (TSDB_CODE_SUCCESS != taosHashPut(pDbCfg, pDbFName, strlen(pDbFName), &pInfo, POINTER_BYTES)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t putUserAuthToCache(const SArray* pUserAuthReq, const SArray* pUserAuthData, SHashObj* pUserAuth) {
  int32_t nvgs = taosArrayGetSize(pUserAuthReq);
  for (int32_t i = 0; i < nvgs; ++i) {
    SUserAuthInfo* pUser = taosArrayGet(pUserAuthReq, i);
    char           key[USER_AUTH_KEY_MAX_LEN] = {0};
    int32_t        len = userAuthToStringExt(pUser->user, pUser->dbFName, pUser->type, key);
    if (TSDB_CODE_SUCCESS != taosHashPut(pUserAuth, key, len, taosArrayGet(pUserAuthData, i), sizeof(bool))) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t putUdfToCache(const SArray* pUdfReq, const SArray* pUdfData, SHashObj* pUdf) {
  int32_t num = taosArrayGetSize(pUdfReq);
  for (int32_t i = 0; i < num; ++i) {
    char*      pFunc = taosArrayGet(pUdfReq, i);
    SFuncInfo* pInfo = taosArrayGet(pUdfData, i);
    if (TSDB_CODE_SUCCESS != taosHashPut(pUdf, pFunc, strlen(pFunc), &pInfo, POINTER_BYTES)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t putMetaDataToCache(const SCatalogReq* pCatalogReq, const SMetaData* pMetaData, SParseMetaCache* pMetaCache) {
  int32_t code = putTableMetaToCache(pCatalogReq->pTableMeta, pMetaData->pTableMeta, pMetaCache->pTableMeta);
  if (TSDB_CODE_SUCCESS == code) {
    code = putDbVgroupToCache(pCatalogReq->pDbVgroup, pMetaData->pDbVgroup, pMetaCache->pDbVgroup);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = putTableVgroupToCache(pCatalogReq->pTableHash, pMetaData->pTableHash, pMetaCache->pTableVgroup);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = putDbCfgToCache(pCatalogReq->pDbCfg, pMetaData->pDbCfg, pMetaCache->pDbCfg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = putUserAuthToCache(pCatalogReq->pUser, pMetaData->pUser, pMetaCache->pUserAuth);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = putUdfToCache(pCatalogReq->pUdf, pMetaData->pUdfList, pMetaCache->pUdf);
  }
  return code;
}

static int32_t reserveTableReqInCache(int32_t acctId, const char* pDb, const char* pTable, SHashObj** pTables) {
  if (NULL == *pTables) {
    *pTables = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
    if (NULL == *pTables) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  char    fullName[TSDB_TABLE_FNAME_LEN];
  int32_t len = snprintf(fullName, sizeof(fullName), "%d.%s.%s", acctId, pDb, pTable);
  return taosHashPut(*pTables, fullName, len, &pTables, POINTER_BYTES);
}

int32_t reserveTableMetaInCache(int32_t acctId, const char* pDb, const char* pTable, SParseMetaCache* pMetaCache) {
  return reserveTableReqInCache(acctId, pDb, pTable, &pMetaCache->pTableMeta);
}

int32_t getTableMetaFromCache(SParseMetaCache* pMetaCache, const SName* pName, STableMeta** pMeta) {
  char fullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pName, fullName);
  STableMeta** pRes = taosHashGet(pMetaCache->pTableMeta, fullName, strlen(fullName));
  if (NULL == pRes || NULL == *pRes) {
    return TSDB_CODE_PAR_INTERNAL_ERROR;
  }
  *pMeta = tableMetaDup(*pRes);
  if (NULL == *pMeta) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
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
  return taosHashPut(*pDbs, fullName, len, &pDbs, POINTER_BYTES);
}

int32_t reserveDbVgInfoInCache(int32_t acctId, const char* pDb, SParseMetaCache* pMetaCache) {
  return reserveDbReqInCache(acctId, pDb, &pMetaCache->pDbVgroup);
}

int32_t getDbVgInfoFromCache(SParseMetaCache* pMetaCache, const char* pDbFName, SArray** pVgInfo) {
  SArray** pRes = taosHashGet(pMetaCache->pDbVgroup, pDbFName, strlen(pDbFName));
  if (NULL == pRes) {
    return TSDB_CODE_PAR_INTERNAL_ERROR;
  }
  // *pRes is null, which is a legal value, indicating that the user DB has not been created
  if (NULL != *pRes) {
    *pVgInfo = taosArrayDup(*pRes);
    if (NULL == *pVgInfo) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t reserveTableVgroupInCache(int32_t acctId, const char* pDb, const char* pTable, SParseMetaCache* pMetaCache) {
  return reserveTableReqInCache(acctId, pDb, pTable, &pMetaCache->pTableVgroup);
}

int32_t getTableVgroupFromCache(SParseMetaCache* pMetaCache, const SName* pName, SVgroupInfo* pVgroup) {
  char fullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pName, fullName);
  SVgroupInfo** pRes = taosHashGet(pMetaCache->pTableVgroup, fullName, strlen(fullName));
  if (NULL == pRes || NULL == *pRes) {
    return TSDB_CODE_PAR_INTERNAL_ERROR;
  }
  memcpy(pVgroup, *pRes, sizeof(SVgroupInfo));
  return TSDB_CODE_SUCCESS;
}

int32_t reserveDbVgVersionInCache(int32_t acctId, const char* pDb, SParseMetaCache* pMetaCache) {
  return reserveDbReqInCache(acctId, pDb, &pMetaCache->pDbCfg);
}

int32_t getDbVgVersionFromCache(SParseMetaCache* pMetaCache, const char* pDbFName, int32_t* pVersion, int64_t* pDbId,
                                int32_t* pTableNum) {
  SDbInfo** pRes = taosHashGet(pMetaCache->pDbCfg, pDbFName, strlen(pDbFName));
  if (NULL == pRes || NULL == *pRes) {
    return TSDB_CODE_PAR_INTERNAL_ERROR;
  }
  *pVersion = (*pRes)->vgVer;
  *pDbId = (*pRes)->dbId;
  *pTableNum = (*pRes)->tbNum;
  return TSDB_CODE_SUCCESS;
}

int32_t reserveDbCfgInCache(int32_t acctId, const char* pDb, SParseMetaCache* pMetaCache) {
  return reserveDbReqInCache(acctId, pDb, &pMetaCache->pDbCfg);
}

int32_t getDbCfgFromCache(SParseMetaCache* pMetaCache, const char* pDbFName, SDbCfgInfo* pInfo) {
  SDbCfgInfo** pRes = taosHashGet(pMetaCache->pDbCfg, pDbFName, strlen(pDbFName));
  if (NULL == pRes || NULL == *pRes) {
    return TSDB_CODE_PAR_INTERNAL_ERROR;
  }
  memcpy(pInfo, *pRes, sizeof(SDbCfgInfo));
  return TSDB_CODE_SUCCESS;
}

int32_t reserveUserAuthInCache(int32_t acctId, const char* pUser, const char* pDb, AUTH_TYPE type,
                               SParseMetaCache* pMetaCache) {
  if (NULL == pMetaCache->pUserAuth) {
    pMetaCache->pUserAuth = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
    if (NULL == pMetaCache->pUserAuth) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  char    key[USER_AUTH_KEY_MAX_LEN] = {0};
  int32_t len = userAuthToString(acctId, pUser, pDb, type, key);
  bool    pass = false;
  return taosHashPut(pMetaCache->pUserAuth, key, len, &pass, sizeof(pass));
}

int32_t getUserAuthFromCache(SParseMetaCache* pMetaCache, const char* pUser, const char* pDbFName, AUTH_TYPE type,
                             bool* pPass) {
  char    key[USER_AUTH_KEY_MAX_LEN] = {0};
  int32_t len = userAuthToStringExt(pUser, pDbFName, type, key);
  bool*   pRes = taosHashGet(pMetaCache->pUserAuth, key, len);
  if (NULL == pRes) {
    return TSDB_CODE_PAR_INTERNAL_ERROR;
  }
  *pPass = *pRes;
  return TSDB_CODE_SUCCESS;
}

int32_t reserveUdfInCache(const char* pFunc, SParseMetaCache* pMetaCache) {
  if (NULL == pMetaCache->pUdf) {
    pMetaCache->pUdf = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
    if (NULL == pMetaCache->pUdf) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return taosHashPut(pMetaCache->pUdf, pFunc, strlen(pFunc), &pMetaCache, POINTER_BYTES);
}

int32_t getUdfInfoFromCache(SParseMetaCache* pMetaCache, const char* pFunc, SFuncInfo* pInfo) {
  SFuncInfo** pRes = taosHashGet(pMetaCache->pUdf, pFunc, strlen(pFunc));
  if (NULL == pRes || NULL == *pRes) {
    return TSDB_CODE_PAR_INTERNAL_ERROR;
  }
  memcpy(pInfo, *pRes, sizeof(SFuncInfo));
  return TSDB_CODE_SUCCESS;
}
