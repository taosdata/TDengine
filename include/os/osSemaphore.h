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

#ifndef _TD_OS_SEMPHONE_H_
#define _TD_OS_SEMPHONE_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <semaphore.h>

#if defined(_TD_DARWIN_64)
#include <dispatch/dispatch.h>
// typedef struct tsem_s *tsem_t;
typedef dispatch_semaphore_t tsem_t;

int32_t tsem_init(tsem_t *sem, int pshared, unsigned int value);
int32_t tsem_wait(tsem_t *sem);
int32_t tsem_timewait(tsem_t *sim, int64_t milis);
int32_t tsem_post(tsem_t *sem);
int32_t tsem_destroy(tsem_t *sem);

#define tsem2_t        tsem_t
#define tsem2_init     tsem_init
#define tsem2_wait     tsem_wait
#define tsem2_timewait tsem_timewait
#define tsem2_post     tsem_post
#define tsem2_destroy  tsem_destroy

#elif defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
#include <windows.h>

#define tsem_t HANDLE

int32_t tsem_init(tsem_t *sem, int pshared, unsigned int value);
int32_t tsem_wait(tsem_t *sem);
int32_t tsem_timewait(tsem_t *sim, int64_t milis);
int32_t tsem_post(tsem_t *sem);
int32_t tsem_destroy(tsem_t *sem);

#define tsem2_t        tsem_t
#define tsem2_init     tsem_init
#define tsem2_wait     tsem_wait
#define tsem2_timewait tsem_timewait
#define tsem2_post     tsem_post
#define tsem2_destroy  tsem_destroy

#else

#define tsem_t       sem_t
int32_t tsem_init(tsem_t *sem, int pshared, unsigned int value);
int32_t tsem_wait(tsem_t *sem);
int32_t tsem_timewait(tsem_t *sim, int64_t milis);
int32_t tsem_post(tsem_t *sem);
int32_t tsem_destroy(tsem_t *sem);

typedef struct tsem2_t {
  TdThreadMutex    mutex;
  TdThreadCond     cond;
  TdThreadCondAttr attr;
  int              count;
} tsem2_t;

// #define tsem2_t       sem_t
int tsem2_init(tsem2_t* sem, int pshared, unsigned int value);
int tsem2_wait(tsem2_t* sem);
int tsem2_timewait(tsem2_t* sem, int64_t milis);
int tsem2_post(tsem2_t* sem);
int tsem2_destroy(tsem2_t* sem);

#endif

#if defined(_TD_DARWIN_64)
//  #define TdThreadRwlock TdThreadMutex
//  #define taosThreadRwlockInit(lock, NULL) taosThreadMutexInit(lock, NULL)
//  #define taosThreadRwlockDestroy(lock) taosThreadMutexDestroy(lock)
// #define taosThreadRwlockWrlock(lock) taosThreadMutexLock(lock)
//  #define taosThreadRwlockRdlock(lock) taosThreadMutexLock(lock)
//  #define taosThreadRwlockUnlock(lock) taosThreadMutexUnlock(lock)

// #define TdThreadSpinlock               TdThreadMutex
// #define taosThreadSpinInit(lock, NULL) taosThreadMutexInit(lock, NULL)
// #define taosThreadSpinDestroy(lock)    taosThreadMutexDestroy(lock)
// #define taosThreadSpinLock(lock)       taosThreadMutexLock(lock)
// #define taosThreadSpinUnlock(lock)     taosThreadMutexUnlock(lock)
#endif

bool    taosCheckPthreadValid(TdThread thread);
int64_t taosGetSelfPthreadId();
int64_t taosGetPthreadId(TdThread thread);
void    taosResetPthread(TdThread *thread);
bool    taosComparePthread(TdThread first, TdThread second);
int32_t taosGetPId();
int32_t taosGetAppName(char *name, int32_t *len);

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_SEMPHONE_H_*/
