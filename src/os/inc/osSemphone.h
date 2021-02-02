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

#ifndef TDENGINE_OS_SEMPHONE_H
#define TDENGINE_OS_SEMPHONE_H

#ifdef __cplusplus
extern "C" {
#endif

#ifndef TAOS_OS_FUNC_SEMPHONE
  #define tsem_t sem_t
  #define tsem_init sem_init
  int tsem_wait(tsem_t* sem);
  #define tsem_post sem_post
  #define tsem_destroy sem_destroy
#endif

#ifdef TAOS_OS_FUNC_PTHREAD_RWLOCK
  #define pthread_rwlock_t pthread_mutex_t
  #define pthread_rwlock_init(lock, NULL) pthread_mutex_init(lock, NULL)
  #define pthread_rwlock_destroy(lock) pthread_mutex_destroy(lock)
  #define pthread_rwlock_wrlock(lock) pthread_mutex_lock(lock)
  #define pthread_rwlock_rdlock(lock) pthread_mutex_lock(lock)
  #define pthread_rwlock_unlock(lock) pthread_mutex_unlock(lock)

  #define pthread_spinlock_t pthread_mutex_t
  #define pthread_spin_init(lock, NULL) pthread_mutex_init(lock, NULL)
  #define pthread_spin_destroy(lock) pthread_mutex_destroy(lock)
  #define pthread_spin_lock(lock) pthread_mutex_lock(lock)
  #define pthread_spin_unlock(lock) pthread_mutex_unlock(lock)
#endif

// TAOS_OS_FUNC_SEMPHONE_PTHREAD
bool    taosCheckPthreadValid(pthread_t thread);
int64_t taosGetSelfPthreadId();
int64_t taosGetPthreadId(pthread_t thread);
void    taosResetPthread(pthread_t* thread);
bool    taosComparePthread(pthread_t first, pthread_t second);
int32_t taosGetPId();
int32_t taosGetCurrentAPPName(char* name, int32_t* len);

#ifdef __cplusplus
}
#endif

#endif
