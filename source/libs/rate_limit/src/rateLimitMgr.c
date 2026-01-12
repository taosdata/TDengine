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

#include "../inc/rateLimitMgr.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DEFAULT_MAX_IP_ENTRIES 1000
#define DEFAULT_CONN_RATE      10000
#define DEFAULT_QUERY_RATE     10000
#define DEFAULT_WRITE_RATE     10000

static SRateLimitMgr*  gRateLimitMgr = NULL;
static int32_t         gInitialized = 0;
static pthread_mutex_t gInitMutex = PTHREAD_MUTEX_INITIALIZER;

static SIpLimiter* findOrCreateIpLimiter(const char* ip) {
  if (gRateLimitMgr == NULL || ip == NULL) {
    return NULL;
  }

  pthread_rwlock_wrlock(&gRateLimitMgr->lock);

  SIpLimiter* pLimiter = gRateLimitMgr->ipLimiters;
  SIpLimiter* pPrev = NULL;

  while (pLimiter != NULL) {
    if (strcmp(pLimiter->ip, ip) == 0) {
      pthread_rwlock_unlock(&gRateLimitMgr->lock);
      return pLimiter;
    }
    pPrev = pLimiter;
    pLimiter = pLimiter->next;
  }

  pLimiter = (SIpLimiter*)calloc(1, sizeof(SIpLimiter));
  if (pLimiter == NULL) {
    pthread_rwlock_unlock(&gRateLimitMgr->lock);
    return NULL;
  }

  strncpy(pLimiter->ip, ip, RLM_MAX_IP_LEN - 1);
  pLimiter->ip[RLM_MAX_IP_LEN - 1] = '\0';
  pLimiter->connLimiter =
      rlCreateLimiter("ip_conn", RL_TYPE_TOKEN_BUCKET, rlCreateTokenBucket(DEFAULT_CONN_RATE, DEFAULT_CONN_RATE));
  pLimiter->queryLimiter =
      rlCreateLimiter("ip_query", RL_TYPE_SLIDING_WINDOW, rlCreateSlidingWindow(60000, 60, DEFAULT_QUERY_RATE));
  pLimiter->activeConnections = 0;
  pLimiter->next = gRateLimitMgr->ipLimiters;
  gRateLimitMgr->ipLimiters = pLimiter;

  pthread_rwlock_unlock(&gRateLimitMgr->lock);
  return pLimiter;
}

static SUserLimiter* findOrCreateUserLimiter(const char* user) {
  if (gRateLimitMgr == NULL || user == NULL) {
    return NULL;
  }

  pthread_rwlock_wrlock(&gRateLimitMgr->lock);

  SUserLimiter* pLimiter = gRateLimitMgr->userLimiters;
  SUserLimiter* pPrev = NULL;

  while (pLimiter != NULL) {
    if (strcmp(pLimiter->user, user) == 0) {
      pthread_rwlock_unlock(&gRateLimitMgr->lock);
      return pLimiter;
    }
    pPrev = pLimiter;
    pLimiter = pLimiter->next;
  }

  pLimiter = (SUserLimiter*)calloc(1, sizeof(SUserLimiter));
  if (pLimiter == NULL) {
    pthread_rwlock_unlock(&gRateLimitMgr->lock);
    return NULL;
  }

  strncpy(pLimiter->user, user, RLM_MAX_USER_LEN - 1);
  pLimiter->user[RLM_MAX_USER_LEN - 1] = '\0';
  pLimiter->queryLimiter =
      rlCreateLimiter("user_query", RL_TYPE_SLIDING_WINDOW, rlCreateSlidingWindow(60000, 60, DEFAULT_QUERY_RATE));
  pLimiter->writeLimiter =
      rlCreateLimiter("user_write", RL_TYPE_SLIDING_WINDOW, rlCreateSlidingWindow(60000, 60, DEFAULT_WRITE_RATE));
  pLimiter->activeConnections = 0;
  pLimiter->next = gRateLimitMgr->userLimiters;
  gRateLimitMgr->userLimiters = pLimiter;

  pthread_rwlock_unlock(&gRateLimitMgr->lock);
  return pLimiter;
}

int32_t rlmInit(int64_t globalConnRate, int64_t globalQueryRate, int64_t globalWriteRate) {
  pthread_mutex_lock(&gInitMutex);
  if (gInitialized) {
    pthread_mutex_unlock(&gInitMutex);
    return 0;
  }

  gRateLimitMgr = (SRateLimitMgr*)calloc(1, sizeof(SRateLimitMgr));
  if (gRateLimitMgr == NULL) {
    pthread_mutex_unlock(&gInitMutex);
    return -1;
  }

  gRateLimitMgr->global = (SGlobalLimiters*)calloc(1, sizeof(SGlobalLimiters));
  if (gRateLimitMgr->global == NULL) {
    free(gRateLimitMgr);
    gRateLimitMgr = NULL;
    pthread_mutex_unlock(&gInitMutex);
    return -1;
  }

  int64_t connRate = (globalConnRate > 0) ? globalConnRate : DEFAULT_CONN_RATE;
  int64_t queryRate = (globalQueryRate > 0) ? globalQueryRate : DEFAULT_QUERY_RATE;
  int64_t writeRate = (globalWriteRate > 0) ? globalWriteRate : DEFAULT_WRITE_RATE;

  gRateLimitMgr->global->connLimiter =
      rlCreateLimiter("global_conn", RL_TYPE_TOKEN_BUCKET, rlCreateTokenBucket(connRate, connRate));
  gRateLimitMgr->global->queryLimiter =
      rlCreateLimiter("global_query", RL_TYPE_SLIDING_WINDOW, rlCreateSlidingWindow(60000, 60, queryRate));
  gRateLimitMgr->global->writeLimiter =
      rlCreateLimiter("global_write", RL_TYPE_SLIDING_WINDOW, rlCreateSlidingWindow(60000, 60, writeRate));
  gRateLimitMgr->global->maxConnections = 50000;
  gRateLimitMgr->global->currentConnections = 0;

  pthread_rwlock_init(&gRateLimitMgr->lock, NULL);
  gInitialized = 1;

  pthread_mutex_unlock(&gInitMutex);
  return 0;
}

void rlmCleanup() {
  pthread_mutex_lock(&gInitMutex);
  if (!gInitialized) {
    pthread_mutex_unlock(&gInitMutex);
    return;
  }

  pthread_rwlock_wrlock(&gRateLimitMgr->lock);

  SIpLimiter* ipLimiter = gRateLimitMgr->ipLimiters;
  while (ipLimiter != NULL) {
    SIpLimiter* next = ipLimiter->next;
    rlDestroyLimiter(ipLimiter->connLimiter);
    rlDestroyLimiter(ipLimiter->queryLimiter);
    free(ipLimiter);
    ipLimiter = next;
  }

  SUserLimiter* userLimiter = gRateLimitMgr->userLimiters;
  while (userLimiter != NULL) {
    SUserLimiter* next = userLimiter->next;
    rlDestroyLimiter(userLimiter->queryLimiter);
    rlDestroyLimiter(userLimiter->writeLimiter);
    free(userLimiter);
    userLimiter = next;
  }

  SDbLimiter* dbLimiter = gRateLimitMgr->dbLimiters;
  while (dbLimiter != NULL) {
    SDbLimiter* next = dbLimiter->next;
    rlDestroyLimiter(dbLimiter->queryLimiter);
    rlDestroyLimiter(dbLimiter->writeLimiter);
    free(dbLimiter);
    dbLimiter = next;
  }

  if (gRateLimitMgr->global != NULL) {
    rlDestroyLimiter(gRateLimitMgr->global->connLimiter);
    rlDestroyLimiter(gRateLimitMgr->global->queryLimiter);
    rlDestroyLimiter(gRateLimitMgr->global->writeLimiter);
    free(gRateLimitMgr->global);
  }

  pthread_rwlock_unlock(&gRateLimitMgr->lock);
  pthread_rwlock_destroy(&gRateLimitMgr->lock);
  free(gRateLimitMgr);
  gRateLimitMgr = NULL;
  gInitialized = 0;

  pthread_mutex_unlock(&gInitMutex);
}

int32_t rlmAllowConnection(const char* ip) {
  if (gRateLimitMgr == NULL || ip == NULL) {
    return -1;
  }

  if (!rlAllowRequest(gRateLimitMgr->global->connLimiter, 1)) {
    return -1;
  }

  pthread_rwlock_wrlock(&gRateLimitMgr->lock);
  if (gRateLimitMgr->global->currentConnections >= gRateLimitMgr->global->maxConnections) {
    pthread_rwlock_unlock(&gRateLimitMgr->lock);
    return -2;
  }
  pthread_rwlock_unlock(&gRateLimitMgr->lock);

  SIpLimiter* ipLimiter = findOrCreateIpLimiter(ip);
  if (ipLimiter == NULL) {
    return -3;
  }

  if (!rlAllowRequest(ipLimiter->connLimiter, 1)) {
    return -4;
  }

  pthread_rwlock_wrlock(&gRateLimitMgr->lock);
  gRateLimitMgr->global->currentConnections++;
  ipLimiter->activeConnections++;
  pthread_rwlock_unlock(&gRateLimitMgr->lock);

  return 0;
}

void rlmConnectionClosed(const char* ip) {
  if (gRateLimitMgr == NULL || ip == NULL) {
    return;
  }

  pthread_rwlock_wrlock(&gRateLimitMgr->lock);
  gRateLimitMgr->global->currentConnections--;

  SIpLimiter* pLimiter = gRateLimitMgr->ipLimiters;
  while (pLimiter != NULL) {
    if (strcmp(pLimiter->ip, ip) == 0) {
      pLimiter->activeConnections--;
      break;
    }
    pLimiter = pLimiter->next;
  }

  pthread_rwlock_unlock(&gRateLimitMgr->lock);
}

int32_t rlmAllowQuery(const char* user, const char* db, const char* ip) {
  if (gRateLimitMgr == NULL) {
    return -1;
  }

  if (!rlAllowRequest(gRateLimitMgr->global->queryLimiter, 1)) {
    return -2;
  }

  if (ip != NULL) {
    SIpLimiter* ipLimiter = findOrCreateIpLimiter(ip);
    if (ipLimiter != NULL && !rlAllowRequest(ipLimiter->queryLimiter, 1)) {
      return -3;
    }
  }

  if (user != NULL) {
    SUserLimiter* userLimiter = findOrCreateUserLimiter(user);
    if (userLimiter != NULL && !rlAllowRequest(userLimiter->queryLimiter, 1)) {
      return -4;
    }
  }

  return 0;
}

int32_t rlmAllowWrite(const char* user, const char* db, const char* ip) {
  if (gRateLimitMgr == NULL) {
    return -1;
  }

  if (!rlAllowRequest(gRateLimitMgr->global->writeLimiter, 1)) {
    return -2;
  }

  if (user != NULL) {
    SUserLimiter* userLimiter = findOrCreateUserLimiter(user);
    if (userLimiter != NULL && !rlAllowRequest(userLimiter->writeLimiter, 1)) {
      return -4;
    }
  }

  return 0;
}

void rlmResetLimiter(ERLimitTarget target, const char* identifier) {
  if (gRateLimitMgr == NULL) {
    return;
  }

  switch (target) {
    case RLM_TARGET_GLOBAL:
      rlResetLimiter(gRateLimitMgr->global->connLimiter);
      rlResetLimiter(gRateLimitMgr->global->queryLimiter);
      rlResetLimiter(gRateLimitMgr->global->writeLimiter);
      break;
    case RLM_TARGET_IP:
      if (identifier != NULL) {
        pthread_rwlock_rdlock(&gRateLimitMgr->lock);
        SIpLimiter* pLimiter = gRateLimitMgr->ipLimiters;
        while (pLimiter != NULL) {
          if (strcmp(pLimiter->ip, identifier) == 0) {
            rlResetLimiter(pLimiter->connLimiter);
            rlResetLimiter(pLimiter->queryLimiter);
            break;
          }
          pLimiter = pLimiter->next;
        }
        pthread_rwlock_unlock(&gRateLimitMgr->lock);
      }
      break;
    case RLM_TARGET_USER:
      if (identifier != NULL) {
        pthread_rwlock_rdlock(&gRateLimitMgr->lock);
        SUserLimiter* pLimiter = gRateLimitMgr->userLimiters;
        while (pLimiter != NULL) {
          if (strcmp(pLimiter->user, identifier) == 0) {
            rlResetLimiter(pLimiter->queryLimiter);
            rlResetLimiter(pLimiter->writeLimiter);
            break;
          }
          pLimiter = pLimiter->next;
        }
        pthread_rwlock_unlock(&gRateLimitMgr->lock);
      }
      break;
    case RLM_TARGET_DB:
      break;
    default:
      break;
  }
}

void rlmGetStats(ERLimitTarget target, const char* identifier, int64_t* allowed, int64_t* rejected) {
  if (allowed != NULL) *allowed = 0;
  if (rejected != NULL) *rejected = 0;

  if (gRateLimitMgr == NULL) {
    return;
  }

  switch (target) {
    case RLM_TARGET_GLOBAL:
      rlGetLimiterStats(gRateLimitMgr->global->queryLimiter, allowed, rejected);
      break;
    case RLM_TARGET_IP:
      if (identifier != NULL) {
        pthread_rwlock_rdlock(&gRateLimitMgr->lock);
        SIpLimiter* pLimiter = gRateLimitMgr->ipLimiters;
        while (pLimiter != NULL) {
          if (strcmp(pLimiter->ip, identifier) == 0) {
            rlGetLimiterStats(pLimiter->queryLimiter, allowed, rejected);
            break;
          }
          pLimiter = pLimiter->next;
        }
        pthread_rwlock_unlock(&gRateLimitMgr->lock);
      }
      break;
    case RLM_TARGET_USER:
      if (identifier != NULL) {
        pthread_rwlock_rdlock(&gRateLimitMgr->lock);
        SUserLimiter* pLimiter = gRateLimitMgr->userLimiters;
        while (pLimiter != NULL) {
          if (strcmp(pLimiter->user, identifier) == 0) {
            rlGetLimiterStats(pLimiter->queryLimiter, allowed, rejected);
            break;
          }
          pLimiter = pLimiter->next;
        }
        pthread_rwlock_unlock(&gRateLimitMgr->lock);
      }
      break;
    case RLM_TARGET_DB:
      break;
    default:
      break;
  }
}
