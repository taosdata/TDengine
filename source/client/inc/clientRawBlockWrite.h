/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#ifndef CLIENT_RAW_BLOCK_WRITE_H
#define CLIENT_RAW_BLOCK_WRITE_H

#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#include "clientInt.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common macros shared across clientRawBlockWrite.c and clientRawBlockJson.c

#define RAW_LOG_END                                                           \
  if (code != 0) {                                                            \
    uError("%s failed at line:%d since:%s", __func__, lino, tstrerror(code)); \
  } else {                                                                    \
    uDebug("%s return success", __func__);                                    \
  }

#define RAW_LOG_START uDebug("%s start", __func__);

#define RAW_NULL_CHECK(c) \
  do {                    \
    if (c == NULL) {      \
      lino = __LINE__;    \
      code = terrno;      \
      goto end;           \
    }                     \
  } while (0)

#define RAW_FALSE_CHECK(c)           \
  do {                               \
    if (!(c)) {                      \
      code = TSDB_CODE_INVALID_PARA; \
      lino = __LINE__;               \
      goto end;                      \
    }                                \
  } while (0)

#define RAW_RETURN_CHECK(c) \
  do {                      \
    code = c;               \
    if (code != 0) {        \
      lino = __LINE__;      \
      goto end;             \
    }                       \
  } while (0)

#define LOG_ID_TAG   "connId:0x%" PRIx64 ", QID:0x%" PRIx64 ",func:%s"
#define LOG_ID_VALUE *(int64_t*)taos, pRequest->requestId, __func__

int32_t tmqGetRawDataFromRes(void* pRetrieve, uint32_t retrieveLen, void** ppRawData);

#define PROCESS_TABLE_NOT_EXIST(code, tbName)                               \
  if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {                              \
    uWarn(LOG_ID_TAG " %s does not exist, skipping", LOG_ID_VALUE, tbName); \
    code = TSDB_CODE_SUCCESS;                                               \
    continue;                                                               \
  }                                                                         \
  RAW_RETURN_CHECK(code);

#define RAW_BLOCK_DELETE_SQL_LEN (2 * TSDB_TABLE_FNAME_LEN + 4 * TSDB_COL_NAME_LEN + 128)

// A quoted identifier may itself contain backticks. Double them instead of
// rejecting legal names or letting server-supplied text terminate the quote.
static inline bool rawBlockQuoteIdent(const char* name, char* output, size_t capacity) {
  if (name == NULL || name[0] == '\0' || capacity < 3) return false;
  size_t length = 0;
  output[length++] = '`';
  for (const char* p = name; *p != '\0'; ++p) {
    size_t bytes = *p == '`' ? 2 : 1;
    if (bytes > capacity - length - 2) return false;
    output[length++] = *p;
    if (*p == '`') output[length++] = '`';
  }
  output[length++] = '`';
  output[length] = '\0';
  return true;
}

static inline int32_t rawBlockBuildDeleteSql(char* sql, size_t capacity, const SDeleteRes* req) {
  char table[2 * TSDB_TABLE_FNAME_LEN + 3];
  char column[2 * TSDB_COL_NAME_LEN + 3];
  if (sql == NULL || req == NULL || !rawBlockQuoteIdent(req->tableFName, table, sizeof(table)) ||
      !rawBlockQuoteIdent(req->tsColName, column, sizeof(column))) {
    return TSDB_CODE_INVALID_MSG;
  }
  int32_t length = snprintf(sql, capacity, "delete from %s where %s >= %" PRId64 " and %s <= %" PRId64,
                            table, column, req->skey, column, req->ekey);
  return length < 0 || (size_t)length >= capacity ? TSDB_CODE_OUT_OF_BUFFER : TSDB_CODE_SUCCESS;
}

#ifdef __cplusplus
}
#endif

#endif  // CLIENT_RAW_BLOCK_WRITE_H
