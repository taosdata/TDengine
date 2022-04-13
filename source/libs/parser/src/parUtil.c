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

static char* getSyntaxErrFormat(int32_t errCode) {
  switch (errCode) {
    case TSDB_CODE_PAR_SYNTAX_ERROR:
      return "syntax error near \"%s\"";
    case TSDB_CODE_PAR_INCOMPLETE_SQL:
      return "Incomplete SQL statement";
    case TSDB_CODE_PAR_INVALID_COLUMN:
      return "Invalid column name : %s";
    case TSDB_CODE_PAR_TABLE_NOT_EXIST:
      return "Table does not exist : %s";
    case TSDB_CODE_PAR_AMBIGUOUS_COLUMN:
      return "Column ambiguously defined : %s";
    case TSDB_CODE_PAR_WRONG_VALUE_TYPE:
      return "Invalid value type : %s";
    case TSDB_CODE_PAR_INVALID_FUNTION:
      return "Invalid function name : %s";
    case TSDB_CODE_PAR_FUNTION_PARA_NUM:
      return "Invalid number of arguments : %s";
    case TSDB_CODE_PAR_FUNTION_PARA_TYPE:
      return "Inconsistent datatypes : %s";
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
      return "Invalid tag name : %s";
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
    case TSDB_CODE_PAR_INTERVAL_VALUE_TOO_SMALL:
      return "This interval value is too small : %s";
    case TSDB_CODE_PAR_DB_NOT_SPECIFIED:
      return "Database not specified";
    case TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME:
      return "Invalid identifier name : %s";
    case TSDB_CODE_PAR_CORRESPONDING_STABLE_ERR:
      return "Corresponding super table not in this db";
    case TSDB_CODE_PAR_INVALID_RANGE_OPTION:
      return "Invalid option %s: %"PRId64" valid range: [%d, %d]";
    case TSDB_CODE_PAR_INVALID_STR_OPTION:
      return "Invalid option %s: %s";
    case TSDB_CODE_PAR_INVALID_ENUM_OPTION:
      return "Invalid option %s: %"PRId64", only %d, %d allowed";
    case TSDB_CODE_PAR_INVALID_TTL_OPTION:
      return "Invalid option ttl: %"PRId64", should be greater than or equal to %d";
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
      return "Invalid option keep unit: %c, %c, %c, only m, h, d allowed";
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
  terrno = errCode;
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

static uint32_t getTableMetaSize(const STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);

  int32_t totalCols = 0;
  if (pTableMeta->tableInfo.numOfColumns >= 0) {
    totalCols = pTableMeta->tableInfo.numOfColumns + pTableMeta->tableInfo.numOfTags;
  }

  return sizeof(STableMeta) + totalCols * sizeof(SSchema);
}

STableMeta* tableMetaDup(const STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);
  size_t size = getTableMetaSize(pTableMeta);

  STableMeta* p = taosMemoryMalloc(size);
  memcpy(p, pTableMeta, size);
  return p;
}

SSchema *getTableColumnSchema(const STableMeta *pTableMeta) {
  assert(pTableMeta != NULL);
  return (SSchema*) pTableMeta->schema;
}

static SSchema* getOneColumnSchema(const STableMeta* pTableMeta, int32_t colIndex) {
  assert(pTableMeta != NULL && pTableMeta->schema != NULL && colIndex >= 0 && colIndex < (getNumOfColumns(pTableMeta) + getNumOfTags(pTableMeta)));

  SSchema* pSchema = (SSchema*) pTableMeta->schema;
  return &pSchema[colIndex];
}

SSchema* getTableTagSchema(const STableMeta* pTableMeta) {
  assert(pTableMeta != NULL && (pTableMeta->tableType == TSDB_SUPER_TABLE || pTableMeta->tableType == TSDB_CHILD_TABLE));
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

int32_t trimString(const char* src, int32_t len, char* dst, int32_t dlen) {
  // delete escape character: \\, \', \"
  char delim = src[0];
  int32_t cnt = 0;
  int32_t j = 0;
  for (uint32_t k = 1; k < len - 1; ++k) {
    if (j >= dlen) {
      break;
    }
    if (src[k] == '\\' || (src[k] == delim && src[k + 1] == delim)) {
      dst[j] = src[k + 1];
      cnt++;
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
