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

#ifndef CLIENT_RAW_BLOCK_WRITE_H
#define CLIENT_RAW_BLOCK_WRITE_H

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

#define PROCESS_TABLE_NOT_EXIST(code, tbName) \
  if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) { \
    uWarn(LOG_ID_TAG " %s not exists, skip", LOG_ID_VALUE, tbName); \
    code = TSDB_CODE_SUCCESS; \
    continue; \
  } \
  RAW_RETURN_CHECK(code);

#ifdef __cplusplus
}
#endif

#endif  // CLIENT_RAW_BLOCK_WRITE_H
