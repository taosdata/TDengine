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

#ifndef _TD_UTIL_LRUCACHE_H_
#define _TD_UTIL_LRUCACHE_H_

#include "thash.h"

#ifdef __cplusplus
extern "C" {
#endif

#define CACHE_CHECK_RET_GOTO(CMD) \
  code = (CMD);                    \
  if (code != TSDB_CODE_SUCCESS) { \
    lino = __LINE__;               \
    goto end;                      \
  }

#define CACHE_CHECK_NULL_GOTO(CMD, ret) \
  if ((CMD) == NULL) {                   \
    code = ret;                          \
    lino = __LINE__;                     \
    goto end;                            \
  }

#define CACHE_CHECK_CONDITION_GOTO(CMD, ret) \
  if (CMD) {                                  \
    code = ret;                               \
    lino = __LINE__;                          \
    goto end;                                 \
  }

#define PRINT_LOG_END(code, lino)                                              \
  if (code != 0) {                                                             \
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code)); \
  } else {                                                                     \
    stDebug("%s done success", __func__);                                      \
  }

int32_t getFromPlanCache(char* user, UserPriority priority, char* query, void** value);
int32_t putToPlanCache(char* user, UserPriority priority, int32_t max, char* query, void* value);

#ifdef __cplusplus
}
#endif

#endif  // _TD_UTIL_LRUCACHE_H_
