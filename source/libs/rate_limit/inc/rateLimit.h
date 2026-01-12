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

#ifndef _TD_RATE_LIMIT_H
#define _TD_RATE_LIMIT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

#define RL_MAX_BUCKETS   60
#define RL_MIN_RATE      1
#define RL_MAX_RATE      10000000
#define RL_NAME_LEN      64
#define TAOS_MAX_SQL_LEN 1048576

typedef enum {
  RL_TYPE_TOKEN_BUCKET = 0,
  RL_TYPE_LEAKY_BUCKET = 1,
  RL_TYPE_SLIDING_WINDOW = 2,
} ERateLimitType;

typedef struct {
  int64_t          capacity;
  int64_t          tokens;
  int64_t          rate;
  int64_t          lastRefillTime;
  pthread_rwlock_t lock;
} STokenBucket;

typedef struct {
  int64_t          capacity;
  int64_t          waterLevel;
  int64_t          leakRate;
  int64_t          lastLeakTime;
  pthread_rwlock_t lock;
} SLeakyBucket;

typedef struct {
  int64_t          windowSizeMs;
  int64_t          maxCount;
  int64_t          buckets[RL_MAX_BUCKETS];
  int64_t          bucketSizeMs;
  int64_t          lastBucketTime;
  int32_t          currentBucket;
  pthread_rwlock_t lock;
} SSlidingWindow;

typedef struct {
  ERateLimitType   type;
  void*            impl;
  char             name[RL_NAME_LEN];
  int64_t          totalAllowed;
  int64_t          totalRejected;
  pthread_rwlock_t statsLock;
} SRateLimiter;

STokenBucket* rlCreateTokenBucket(int64_t capacity, int64_t rate);
void          rlDestroyTokenBucket(STokenBucket* pBucket);
bool          rlTokenBucketAllow(STokenBucket* pBucket, int64_t count);
void          rlTokenBucketReset(STokenBucket* pBucket);
void          rlTokenBucketUpdateRate(STokenBucket* pBucket, int64_t newRate);

SLeakyBucket* rlCreateLeakyBucket(int64_t capacity, int64_t leakRate);
void          rlDestroyLeakyBucket(SLeakyBucket* pBucket);
bool          rlLeakyBucketAllow(SLeakyBucket* pBucket, int64_t count);
void          rlLeakyBucketReset(SLeakyBucket* pBucket);

SSlidingWindow* rlCreateSlidingWindow(int64_t windowSizeMs, int32_t numBuckets, int64_t maxCount);
void            rlDestroySlidingWindow(SSlidingWindow* pWindow);
bool            rlSlidingWindowAllow(SSlidingWindow* pWindow, int64_t count);
void            rlSlidingWindowReset(SSlidingWindow* pWindow);

SRateLimiter* rlCreateLimiter(const char* name, ERateLimitType type, void* impl);
void          rlDestroyLimiter(SRateLimiter* pLimiter);
bool          rlAllowRequest(SRateLimiter* pLimiter, int64_t count);
void          rlGetLimiterStats(SRateLimiter* pLimiter, int64_t* totalAllowed, int64_t* totalRejected);
void          rlResetLimiter(SRateLimiter* pLimiter);

#ifdef __cplusplus
}
#endif

#endif
