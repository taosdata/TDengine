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
#include "trpc.h"
#include "cJSON.h"

#ifdef __cplusplus
extern "C" {
#endif

#define CACHE_CHECK_RET_GOTO(CMD)  \
  code = (CMD);                    \
  if (code != TSDB_CODE_SUCCESS) { \
    lino = __LINE__;               \
    goto end;                      \
  }

#define CACHE_CHECK_NULL_GOTO(CMD, ret) \
  if ((CMD) == NULL) {                  \
    code = ret;                         \
    lino = __LINE__;                    \
    goto end;                           \
  }

#define CACHE_CHECK_CONDITION_GOTO(CMD, ret) \
  if (CMD) {                                 \
    code = ret;                              \
    lino = __LINE__;                         \
    goto end;                                \
  }

#define PRINT_LOG_END(code, lino)                                             \
  if (code != 0) {                                                            \
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code)); \
  } else {                                                                    \
    uDebug("%s done success", __func__);                                      \
  }

#define MAX_PLAN_CACHE_SIZE              10
#define MAX_PLAN_CACHE_SIZE_LOW_LEVEL    ((int32_t)(0.5 * MAX_PLAN_CACHE_SIZE))
#define MAX_PLAN_CACHE_SIZE_MEDIUM_LEVEL ((int32_t)(0.7 * MAX_PLAN_CACHE_SIZE))
#define MAX_PLAN_CACHE_SIZE_HIGH_LEVEL   ((int32_t)(0.9 * MAX_PLAN_CACHE_SIZE))

#define QUERY_STRING_MAX_LEN 128
typedef enum {
  PLAN_CACHE_PRIORITY_HIGH = 0,
  PLAN_CACHE_PRIORITY_MEDIUM = 1,
  PLAN_CACHE_PRIORITY_LOW = 2,
  PLAN_CACHE_PRIORITY_NUM = 3,
} UserPriority;

typedef struct SCachedPlan {
  char    user[24];
  char   sql[256];
  int64_t cache_hit;
  char   created_at[128];
  char   last_accessed_at[128];
} SCachedPlan;

typedef struct SUserCachedPlan {
  char    user[24];
  int32_t plans;
  int32_t quota;
  char   last_updated_at[128];
} SUserCachedPlan;

int32_t getFromPlanCache(char* user, UserPriority priority, char* query, void** value);
int32_t putToPlanCache(char* user, UserPriority priority, int32_t max, char* query, void* value, void* pTrans, SEpSet* epset);
int32_t clientRetrieveCachedPlans(SArray** ppRes); // SCachedPlan
int32_t clientRetrieveUserCachedPlans(SArray** ppRes); // SUserCachedPlan

#ifdef __cplusplus
}
#endif

#endif  // _TD_UTIL_LRUCACHE_H_
