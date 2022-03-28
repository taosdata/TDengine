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

#ifndef _TD_OS_THREAD_H_
#define _TD_OS_THREAD_H_

#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef pthread_t TdThread;
typedef pthread_spinlock_t TdThreadSpinlock;
typedef pthread_mutex_t TdThreadMutex;
typedef pthread_mutexattr_t TdThreadMutexAttr;
typedef pthread_rwlock_t TdThreadRwlock;
typedef pthread_attr_t TdThreadAttr;
typedef pthread_once_t TdThreadOnce;
typedef pthread_rwlockattr_t TdThreadRwlockAttr;
typedef pthread_cond_t TdThreadCond;
typedef pthread_condattr_t TdThreadCondAttr;

#define taosThreadCleanupPush pthread_cleanup_push
#define taosThreadCleanupPop pthread_cleanup_pop

// If the error is in a third-party library, place this header file under the third-party library header file.
// When you want to use this feature, you should find or add the same function in the following section.
#ifndef ALLOW_FORBID_FUNC
    #define pthread_t PTHREAD_T_TYPE_TAOS_FORBID
    #define pthread_spinlock_t PTHREAD_SPINLOCK_T_TYPE_TAOS_FORBID
    #define pthread_mutex_t PTHREAD_MUTEX_T_TYPE_TAOS_FORBID
    #define pthread_mutexattr_t PTHREAD_MUTEXATTR_T_TYPE_TAOS_FORBID
    #define pthread_rwlock_t PTHREAD_RWLOCK_T_TYPE_TAOS_FORBID
    #define pthread_attr_t PTHREAD_ATTR_T_TYPE_TAOS_FORBID
    #define pthread_once_t PTHREAD_ONCE_T_TYPE_TAOS_FORBID
    #define pthread_rwlockattr_t PTHREAD_RWLOCKATTR_T_TYPE_TAOS_FORBID
    #define pthread_cond_t PTHREAD_COND_T_TYPE_TAOS_FORBID
    #define pthread_condattr_t PTHREAD_CONDATTR_T_TYPE_TAOS_FORBID
    #define pthread_spin_init PTHREAD_SPIN_INIT_FUNC_TAOS_FORBID
    #define pthread_mutex_init PTHREAD_MUTEX_INIT_FUNC_TAOS_FORBID
    #define pthread_spin_destroy PTHREAD_SPIN_DESTROY_FUNC_TAOS_FORBID
    #define pthread_mutex_destroy PTHREAD_MUTEX_DESTROY_FUNC_TAOS_FORBID
    #define pthread_spin_lock PTHREAD_SPIN_LOCK_FUNC_TAOS_FORBID
    #define pthread_mutex_lock PTHREAD_MUTEX_LOCK_FUNC_TAOS_FORBID
    #define pthread_spin_unlock PTHREAD_SPIN_UNLOCK_FUNC_TAOS_FORBID
    #define pthread_mutex_unlock PTHREAD_MUTEX_UNLOCK_FUNC_TAOS_FORBID
    #define pthread_rwlock_rdlock PTHREAD_RWLOCK_RDLOCK_FUNC_TAOS_FORBID
    #define pthread_rwlock_wrlock PTHREAD_RWLOCK_WRLOCK_FUNC_TAOS_FORBID
    #define pthread_rwlock_unlock PTHREAD_RWLOCK_UNLOCK_FUNC_TAOS_FORBID
    #define pthread_testcancel PTHREAD_TESTCANCEL_FUNC_TAOS_FORBID
    #define pthread_attr_init PTHREAD_ATTR_INIT_FUNC_TAOS_FORBID
    #define pthread_create PTHREAD_CREATE_FUNC_TAOS_FORBID
    #define pthread_once PTHREAD_ONCE_FUNC_TAOS_FORBID
    #define pthread_attr_setdetachstate PTHREAD_ATTR_SETDETACHSTATE_FUNC_TAOS_FORBID
    #define pthread_attr_destroy PTHREAD_ATTR_DESTROY_FUNC_TAOS_FORBID
    #define pthread_join PTHREAD_JOIN_FUNC_TAOS_FORBID
    #define pthread_rwlock_init PTHREAD_RWLOCK_INIT_FUNC_TAOS_FORBID
    #define pthread_rwlock_destroy PTHREAD_RWLOCK_DESTROY_FUNC_TAOS_FORBID
    #define pthread_cond_signal PTHREAD_COND_SIGNAL_FUNC_TAOS_FORBID
    #define pthread_cond_init PTHREAD_COND_INIT_FUNC_TAOS_FORBID
    #define pthread_cond_broadcast PTHREAD_COND_BROADCAST_FUNC_TAOS_FORBID
    #define pthread_cond_destroy PTHREAD_COND_DESTROY_FUNC_TAOS_FORBID
    #define pthread_cond_wait PTHREAD_COND_WAIT_FUNC_TAOS_FORBID
    #define pthread_self PTHREAD_SELF_FUNC_TAOS_FORBID
    #define pthread_equal PTHREAD_EQUAL_FUNC_TAOS_FORBID
    #define pthread_sigmask PTHREAD_SIGMASK_FUNC_TAOS_FORBID
    #define pthread_cancel PTHREAD_CANCEL_FUNC_TAOS_FORBID
    #define pthread_kill PTHREAD_KILL_FUNC_TAOS_FORBID
#endif

int32_t taosThreadSpinInit(TdThreadSpinlock *lock, int pshared);
int32_t taosThreadMutexInit(TdThreadMutex *mutex, const TdThreadMutexAttr *attr);
int32_t taosThreadSpinDestroy(TdThreadSpinlock *lock);
int32_t taosThreadMutexDestroy(TdThreadMutex * mutex);
int32_t taosThreadSpinLock(TdThreadSpinlock *lock);
int32_t taosThreadMutexLock(TdThreadMutex *mutex);
int32_t taosThreadRwlockRdlock(TdThreadRwlock *rwlock);
int32_t taosThreadSpinUnlock(TdThreadSpinlock *lock);
int32_t taosThreadMutexUnlock(TdThreadMutex *mutex);
int32_t taosThreadRwlockWrlock(TdThreadRwlock *rwlock);
int32_t taosThreadRwlockUnlock(TdThreadRwlock *rwlock);
void taosThreadTestCancel(void);
int32_t taosThreadAttrInit(TdThreadAttr *attr);
int32_t taosThreadCreate(TdThread *tid, const TdThreadAttr *attr, void*(*start)(void*), void *arg);
int32_t taosThreadOnce(TdThreadOnce *onceControl, void(*initRoutine)(void));
int32_t taosThreadAttrSetDetachState(TdThreadAttr *attr, int32_t detachState);
int32_t taosThreadAttrDestroy(TdThreadAttr *attr);
int32_t taosThreadJoin(TdThread thread, void **pValue);
int32_t taosThreadRwlockInit(TdThreadRwlock *rwlock, const TdThreadRwlockAttr *attr);
int32_t taosThreadRwlockDestroy(TdThreadRwlock *rwlock);
int32_t taosThreadCondSignal(TdThreadCond *cond);
int32_t taosThreadCondInit(TdThreadCond *cond, const TdThreadCondAttr *attr);
int32_t taosThreadCondBroadcast(TdThreadCond *cond);
int32_t taosThreadCondDestroy(TdThreadCond *cond);
int32_t taosThreadCondWait(TdThreadCond *cond, TdThreadMutex *mutex);
TdThread taosThreadSelf(void);
int32_t taosThreadEqual(TdThread t1, TdThread t2);
int32_t taosThreadSigmask(int how, sigset_t const *set, sigset_t *oset);
int32_t taosThreadCancel(TdThread thread);
int32_t taosThreadKill(TdThread thread, int sig);
#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_THREAD_H_*/
