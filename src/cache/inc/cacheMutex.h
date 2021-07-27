/*
 * Copyright (c) 2019 TAOS Data, Inc. <cli@taosdata.com>
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

#ifndef TDENGINE_MUTEX_H
#define TDENGINE_MUTEX_H

#include <pthread.h>
#include "cacheTypes.h"
#include "osDef.h"

#ifdef __cplusplus
extern "C" {
#endif

struct cacheMutex {
  pthread_mutex_t mutex;
};

static FORCE_INLINE int cacheMutexInit(cacheMutex* mutex) {
  return pthread_mutex_init(&(mutex->mutex), NULL);
}

static FORCE_INLINE int cacheMutexLock(cacheMutex* mutex) {
  return pthread_mutex_lock(&(mutex->mutex));
}

static FORCE_INLINE int cacheMutexUnlock(cacheMutex* mutex) {
  return pthread_mutex_unlock(&(mutex->mutex));
}

static FORCE_INLINE int cacheMutexTryLock(cacheMutex* mutex) {
  return pthread_mutex_trylock(&(mutex->mutex));
}

static FORCE_INLINE int cacheMutexTryUnlock(cacheMutex* mutex) {
  return pthread_mutex_unlock(&(mutex->mutex));
}

static FORCE_INLINE int cacheMutexDestroy(cacheMutex* mutex) {
  return pthread_mutex_destroy(&(mutex->mutex));
}

#ifdef __cplusplus
}
#endif

#endif /* TDENGINE_MUTEX_H */