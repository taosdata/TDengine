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

#ifndef _TD_RATE_LIMIT_MGR_H
#define _TD_RATE_LIMIT_MGR_H

#ifdef __cplusplus
extern "C" {
#endif

#include "../inc/rateLimit.h"
#include "os.h"

#define RLM_MAX_IP_LEN     46
#define RLM_MAX_USER_LEN   64
#define RLM_MAX_DB_LEN     64
#define RLM_MAX_IP_ENTRIES 1000

typedef enum {
  RLM_TARGET_GLOBAL = 0,
  RLM_TARGET_IP = 1,
  RLM_TARGET_USER = 2,
  RLM_TARGET_DB = 3,
} ERLimitTarget;

typedef struct SIpLimiter   SIpLimiter;
typedef struct SUserLimiter SUserLimiter;
typedef struct SDbLimiter   SDbLimiter;

typedef struct {
  SRateLimiter* connLimiter;
  SRateLimiter* queryLimiter;
  SRateLimiter* writeLimiter;
  int64_t       maxConnections;
  int64_t       currentConnections;
} SGlobalLimiters;

struct SIpLimiter {
  char          ip[RLM_MAX_IP_LEN];
  SRateLimiter* connLimiter;
  SRateLimiter* queryLimiter;
  int64_t       activeConnections;
  SIpLimiter*   next;
};

struct SUserLimiter {
  char          user[RLM_MAX_USER_LEN];
  SRateLimiter* queryLimiter;
  SRateLimiter* writeLimiter;
  int64_t       activeConnections;
  SUserLimiter* next;
};

struct SDbLimiter {
  char          db[RLM_MAX_DB_LEN];
  SRateLimiter* queryLimiter;
  SRateLimiter* writeLimiter;
  SDbLimiter*   next;
};

typedef struct {
  SGlobalLimiters* global;
  SIpLimiter*      ipLimiters;
  SUserLimiter*    userLimiters;
  SDbLimiter*      dbLimiters;
  TdThreadRwlock   lock;
} SRateLimitMgr;

int32_t rlmInit(int64_t globalConnRate, int64_t globalQueryRate, int64_t globalWriteRate);
void    rlmCleanup();
int32_t rlmAllowConnection(const char* ip);
void    rlmConnectionClosed(const char* ip);
int32_t rlmAllowQuery(const char* user, const char* db, const char* ip);
int32_t rlmAllowWrite(const char* user, const char* db, const char* ip);
void    rlmResetLimiter(ERLimitTarget target, const char* identifier);
void    rlmGetStats(ERLimitTarget target, const char* identifier, int64_t* allowed, int64_t* rejected);

#ifdef __cplusplus
}
#endif

#endif
