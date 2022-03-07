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

#include "parserUtil.h"

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

  STableMeta* p = malloc(size);
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
