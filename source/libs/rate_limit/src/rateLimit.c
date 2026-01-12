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

#include "../inc/rateLimit.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

#define MILLISECOND_PER_SECOND 1000LL

static int64_t getCurrentTimeMs() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (int64_t)ts.tv_sec * MILLISECOND_PER_SECOND + ts.tv_nsec / 1000000;
}

STokenBucket* rlCreateTokenBucket(int64_t capacity, int64_t rate) {
  if (capacity <= 0 || rate < RL_MIN_RATE || rate > RL_MAX_RATE) {
    return NULL;
  }

  STokenBucket* pBucket = (STokenBucket*)calloc(1, sizeof(STokenBucket));
  if (pBucket == NULL) {
    return NULL;
  }

  pBucket->capacity = capacity;
  pBucket->tokens = capacity;
  pBucket->rate = rate;
  pBucket->lastRefillTime = getCurrentTimeMs();
  pthread_rwlock_init(&pBucket->lock, NULL);

  return pBucket;
}

void rlDestroyTokenBucket(STokenBucket* pBucket) {
  if (pBucket == NULL) {
    return;
  }

  pthread_rwlock_destroy(&pBucket->lock);
  free(pBucket);
}

bool rlTokenBucketAllow(STokenBucket* pBucket, int64_t count) {
  if (pBucket == NULL || count <= 0) {
    return false;
  }

  pthread_rwlock_wrlock(&pBucket->lock);

  int64_t now = getCurrentTimeMs();
  int64_t elapsedMs = now - pBucket->lastRefillTime;

  if (elapsedMs > 0) {
    int64_t tokensToAdd = (pBucket->rate * elapsedMs) / MILLISECOND_PER_SECOND;
    pBucket->tokens += tokensToAdd;
    if (pBucket->tokens > pBucket->capacity) {
      pBucket->tokens = pBucket->capacity;
    }
    pBucket->lastRefillTime = now;
  }

  bool allowed = (pBucket->tokens >= count);
  if (allowed) {
    pBucket->tokens -= count;
  }

  pthread_rwlock_unlock(&pBucket->lock);
  return allowed;
}

void rlTokenBucketReset(STokenBucket* pBucket) {
  if (pBucket == NULL) {
    return;
  }

  pthread_rwlock_wrlock(&pBucket->lock);
  pBucket->tokens = pBucket->capacity;
  pBucket->lastRefillTime = getCurrentTimeMs();
  pthread_rwlock_unlock(&pBucket->lock);
}

void rlTokenBucketUpdateRate(STokenBucket* pBucket, int64_t newRate) {
  if (pBucket == NULL || newRate < RL_MIN_RATE || newRate > RL_MAX_RATE) {
    return;
  }

  pthread_rwlock_wrlock(&pBucket->lock);
  pBucket->rate = newRate;
  pthread_rwlock_unlock(&pBucket->lock);
}

SLeakyBucket* rlCreateLeakyBucket(int64_t capacity, int64_t leakRate) {
  if (capacity <= 0 || leakRate < RL_MIN_RATE || leakRate > RL_MAX_RATE) {
    return NULL;
  }

  SLeakyBucket* pBucket = (SLeakyBucket*)calloc(1, sizeof(SLeakyBucket));
  if (pBucket == NULL) {
    return NULL;
  }

  pBucket->capacity = capacity;
  pBucket->waterLevel = 0;
  pBucket->leakRate = leakRate;
  pBucket->lastLeakTime = getCurrentTimeMs();
  pthread_rwlock_init(&pBucket->lock, NULL);

  return pBucket;
}

void rlDestroyLeakyBucket(SLeakyBucket* pBucket) {
  if (pBucket == NULL) {
    return;
  }

  pthread_rwlock_destroy(&pBucket->lock);
  free(pBucket);
}

bool rlLeakyBucketAllow(SLeakyBucket* pBucket, int64_t count) {
  if (pBucket == NULL || count <= 0) {
    return false;
  }

  pthread_rwlock_wrlock(&pBucket->lock);

  int64_t now = getCurrentTimeMs();
  int64_t elapsedMs = now - pBucket->lastLeakTime;

  if (elapsedMs > 0) {
    int64_t leaked = (pBucket->leakRate * elapsedMs) / MILLISECOND_PER_SECOND;
    pBucket->waterLevel -= leaked;
    if (pBucket->waterLevel < 0) {
      pBucket->waterLevel = 0;
    }
    pBucket->lastLeakTime = now;
  }

  bool allowed = (pBucket->waterLevel + count <= pBucket->capacity);
  if (allowed) {
    pBucket->waterLevel += count;
  }

  pthread_rwlock_unlock(&pBucket->lock);
  return allowed;
}

void rlLeakyBucketReset(SLeakyBucket* pBucket) {
  if (pBucket == NULL) {
    return;
  }

  pthread_rwlock_wrlock(&pBucket->lock);
  pBucket->waterLevel = 0;
  pBucket->lastLeakTime = getCurrentTimeMs();
  pthread_rwlock_unlock(&pBucket->lock);
}

SSlidingWindow* rlCreateSlidingWindow(int64_t windowSizeMs, int32_t numBuckets, int64_t maxCount) {
  if (windowSizeMs <= 0 || numBuckets <= 0 || numBuckets > RL_MAX_BUCKETS || maxCount <= 0) {
    return NULL;
  }

  SSlidingWindow* pWindow = (SSlidingWindow*)calloc(1, sizeof(SSlidingWindow));
  if (pWindow == NULL) {
    return NULL;
  }

  pWindow->windowSizeMs = windowSizeMs;
  pWindow->maxCount = maxCount;
  pWindow->bucketSizeMs = windowSizeMs / numBuckets;
  pWindow->currentBucket = 0;
  pWindow->lastBucketTime = getCurrentTimeMs();
  pthread_rwlock_init(&pWindow->lock, NULL);

  return pWindow;
}

void rlDestroySlidingWindow(SSlidingWindow* pWindow) {
  if (pWindow == NULL) {
    return;
  }

  pthread_rwlock_destroy(&pWindow->lock);
  free(pWindow);
}

static void rotateBuckets(SSlidingWindow* pWindow) {
  int64_t now = getCurrentTimeMs();
  int64_t elapsedMs = now - pWindow->lastBucketTime;

  if (elapsedMs < pWindow->bucketSizeMs) {
    return;
  }

  int32_t bucketsToRotate = (int32_t)(elapsedMs / pWindow->bucketSizeMs);
  int32_t numBuckets = (int32_t)(pWindow->windowSizeMs / pWindow->bucketSizeMs);

  for (int32_t i = 0; i < bucketsToRotate && i < numBuckets; i++) {
    pWindow->currentBucket = (pWindow->currentBucket + 1) % numBuckets;
    pWindow->buckets[pWindow->currentBucket] = 0;
  }

  pWindow->lastBucketTime += bucketsToRotate * pWindow->bucketSizeMs;
}

bool rlSlidingWindowAllow(SSlidingWindow* pWindow, int64_t count) {
  if (pWindow == NULL || count <= 0) {
    return false;
  }

  pthread_rwlock_wrlock(&pWindow->lock);

  rotateBuckets(pWindow);

  int64_t totalCount = 0;
  int32_t numBuckets = (int32_t)(pWindow->windowSizeMs / pWindow->bucketSizeMs);
  for (int32_t i = 0; i < numBuckets; i++) {
    totalCount += pWindow->buckets[i];
  }

  bool allowed = (totalCount + count <= pWindow->maxCount);
  if (allowed) {
    pWindow->buckets[pWindow->currentBucket] += count;
  }

  pthread_rwlock_unlock(&pWindow->lock);
  return allowed;
}

void rlSlidingWindowReset(SSlidingWindow* pWindow) {
  if (pWindow == NULL) {
    return;
  }

  pthread_rwlock_wrlock(&pWindow->lock);
  memset(pWindow->buckets, 0, sizeof(pWindow->buckets));
  pWindow->currentBucket = 0;
  pWindow->lastBucketTime = getCurrentTimeMs();
  pthread_rwlock_unlock(&pWindow->lock);
}

SRateLimiter* rlCreateLimiter(const char* name, ERateLimitType type, void* impl) {
  if (name == NULL || impl == NULL) {
    return NULL;
  }

  SRateLimiter* pLimiter = (SRateLimiter*)calloc(1, sizeof(SRateLimiter));
  if (pLimiter == NULL) {
    return NULL;
  }

  pLimiter->type = type;
  pLimiter->impl = impl;
  strncpy(pLimiter->name, name, RL_NAME_LEN - 1);
  pLimiter->name[RL_NAME_LEN - 1] = '\0';
  pLimiter->totalAllowed = 0;
  pLimiter->totalRejected = 0;
  pthread_rwlock_init(&pLimiter->statsLock, NULL);

  return pLimiter;
}

void rlDestroyLimiter(SRateLimiter* pLimiter) {
  if (pLimiter == NULL) {
    return;
  }

  switch (pLimiter->type) {
    case RL_TYPE_TOKEN_BUCKET:
      rlDestroyTokenBucket((STokenBucket*)pLimiter->impl);
      break;
    case RL_TYPE_LEAKY_BUCKET:
      rlDestroyLeakyBucket((SLeakyBucket*)pLimiter->impl);
      break;
    case RL_TYPE_SLIDING_WINDOW:
      rlDestroySlidingWindow((SSlidingWindow*)pLimiter->impl);
      break;
    default:
      free(pLimiter->impl);
      break;
  }

  pthread_rwlock_destroy(&pLimiter->statsLock);
  free(pLimiter);
}

bool rlAllowRequest(SRateLimiter* pLimiter, int64_t count) {
  if (pLimiter == NULL || count <= 0) {
    return false;
  }

  bool allowed = false;

  switch (pLimiter->type) {
    case RL_TYPE_TOKEN_BUCKET:
      allowed = rlTokenBucketAllow((STokenBucket*)pLimiter->impl, count);
      break;
    case RL_TYPE_LEAKY_BUCKET:
      allowed = rlLeakyBucketAllow((SLeakyBucket*)pLimiter->impl, count);
      break;
    case RL_TYPE_SLIDING_WINDOW:
      allowed = rlSlidingWindowAllow((SSlidingWindow*)pLimiter->impl, count);
      break;
    default:
      allowed = false;
      break;
  }

  pthread_rwlock_wrlock(&pLimiter->statsLock);
  if (allowed) {
    pLimiter->totalAllowed++;
  } else {
    pLimiter->totalRejected++;
  }
  pthread_rwlock_unlock(&pLimiter->statsLock);

  return allowed;
}

void rlGetLimiterStats(SRateLimiter* pLimiter, int64_t* totalAllowed, int64_t* totalRejected) {
  if (pLimiter == NULL) {
    if (totalAllowed) *totalAllowed = 0;
    if (totalRejected) *totalRejected = 0;
    return;
  }

  pthread_rwlock_rdlock(&pLimiter->statsLock);
  if (totalAllowed) *totalAllowed = pLimiter->totalAllowed;
  if (totalRejected) *totalRejected = pLimiter->totalRejected;
  pthread_rwlock_unlock(&pLimiter->statsLock);
}

void rlResetLimiter(SRateLimiter* pLimiter) {
  if (pLimiter == NULL) {
    return;
  }

  switch (pLimiter->type) {
    case RL_TYPE_TOKEN_BUCKET:
      rlTokenBucketReset((STokenBucket*)pLimiter->impl);
      break;
    case RL_TYPE_LEAKY_BUCKET:
      rlLeakyBucketReset((SLeakyBucket*)pLimiter->impl);
      break;
    case RL_TYPE_SLIDING_WINDOW:
      rlSlidingWindowReset((SSlidingWindow*)pLimiter->impl);
      break;
    default:
      break;
  }

  pthread_rwlock_wrlock(&pLimiter->statsLock);
  pLimiter->totalAllowed = 0;
  pLimiter->totalRejected = 0;
  pthread_rwlock_unlock(&pLimiter->statsLock);
}
