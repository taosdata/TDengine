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

#if defined(WINDOWS) && !defined(__USE_PTHREAD)
#include <windows.h>
#define __USE_WIN_THREAD
// https://learn.microsoft.com/en-us/windows/win32/winprog/using-the-windows-headers
// #ifndef _WIN32_WINNT
// #define _WIN32_WINNT 0x0600
// #endif
#endif

#if !defined(WINDOWS) && !defined(_ALPINE)
#ifndef __USE_XOPEN2K
#define TD_USE_SPINLOCK_AS_MUTEX
typedef pthread_mutex_t pthread_spinlock_t;
#endif
#endif

#ifdef __USE_WIN_THREAD
typedef pthread_t          TdThread;           // pthread api
typedef pthread_spinlock_t TdThreadSpinlock;   // pthread api
typedef CRITICAL_SECTION   TdThreadMutex;      // windows api
typedef HANDLE             TdThreadMutexAttr;  // windows api
typedef struct {
  SRWLOCK lock;
  int8_t  excl;
} TdThreadRwlock;                               // windows api
typedef pthread_attr_t     TdThreadAttr;        // pthread api
typedef pthread_once_t     TdThreadOnce;        // pthread api
typedef HANDLE             TdThreadRwlockAttr;  // windows api
typedef CONDITION_VARIABLE TdThreadCond;        // windows api
typedef HANDLE             TdThreadCondAttr;    // windows api
typedef pthread_key_t      TdThreadKey;         // pthread api
#else
typedef pthread_t            TdThread;
typedef pthread_spinlock_t   TdThreadSpinlock;
typedef pthread_mutex_t      TdThreadMutex;
typedef pthread_mutexattr_t  TdThreadMutexAttr;
typedef pthread_rwlock_t     TdThreadRwlock;
typedef pthread_attr_t       TdThreadAttr;
typedef pthread_once_t       TdThreadOnce;
typedef pthread_rwlockattr_t TdThreadRwlockAttr;
typedef pthread_cond_t       TdThreadCond;
typedef pthread_condattr_t   TdThreadCondAttr;
typedef pthread_key_t        TdThreadKey;
#endif

#define taosThreadCleanupPush pthread_cleanup_push
#define taosThreadCleanupPop  pthread_cleanup_pop
#if !defined(WINDOWS)
#if defined(_TD_DARWIN_64)  // MACOS
#define taosThreadRwlockAttrSetKindNP(A, B) ((void)0)
#else  // LINUX
#if _XOPEN_SOURCE >= 500 || _POSIX_C_SOURCE >= 200809L
#define taosThreadRwlockAttrSetKindNP(A, B) pthread_rwlockattr_setkind_np(A, B)
#else
#define taosThreadRwlockAttrSetKindNP(A, B) ((void)0)
#endif
#endif
#else  // WINDOWS
#define taosThreadRwlockAttrSetKindNP(A, B) ((void)0)
#endif

#if defined(WINDOWS) && !defined(__USE_PTHREAD)
#define TD_PTHREAD_MUTEX_INITIALIZER PTHREAD_MUTEX_INITIALIZER_FORBID
#elif defined(WINDOWS)
#define TD_PTHREAD_MUTEX_INITIALIZER (TdThreadMutex)(-1)
#else
#define TD_PTHREAD_MUTEX_INITIALIZER PTHREAD_MUTEX_INITIALIZER
#endif

// If the error is in a third-party library, place this header file under the third-party library header file.
// When you want to use this feature, you should find or add the same function in the following section.
#ifndef ALLOW_FORBID_FUNC
#define pthread_t                      PTHREAD_T_TYPE_TAOS_FORBID
#define pthread_spinlock_t             PTHREAD_SPINLOCK_T_TYPE_TAOS_FORBID
#define pthread_mutex_t                PTHREAD_MUTEX_T_TYPE_TAOS_FORBID
#define pthread_mutexattr_t            PTHREAD_MUTEXATTR_T_TYPE_TAOS_FORBID
#define pthread_rwlock_t               PTHREAD_RWLOCK_T_TYPE_TAOS_FORBID
#define pthread_attr_t                 PTHREAD_ATTR_T_TYPE_TAOS_FORBID
#define pthread_once_t                 PTHREAD_ONCE_T_TYPE_TAOS_FORBID
#define pthread_rwlockattr_t           PTHREAD_RWLOCKATTR_T_TYPE_TAOS_FORBID
#define pthread_cond_t                 PTHREAD_COND_T_TYPE_TAOS_FORBID
#define pthread_condattr_t             PTHREAD_CONDATTR_T_TYPE_TAOS_FORBID
#define pthread_key_t                  PTHREAD_KEY_T_TYPE_TAOS_FORBID
#define pthread_barrier_t              PTHREAD_BARRIER_T_TYPE_TAOS_FORBID
#define pthread_barrierattr_t          PTHREAD_BARRIERATTR_T_TYPE_TAOS_FORBID
#define pthread_create                 PTHREAD_CREATE_FUNC_TAOS_FORBID
#define pthread_attr_destroy           PTHREAD_ATTR_DESTROY_FUNC_TAOS_FORBID
#define pthread_attr_getdetachstate    PTHREAD_ATTR_GETDETACHSTATE_FUNC_TAOS_FORBID
#define pthread_attr_getinheritsched   PTHREAD_ATTR_GETINHERITSCHED_FUNC_TAOS_FORBID
#define pthread_attr_getschedparam     PTHREAD_ATTR_GETSCHEDPARAM_FUNC_TAOS_FORBID
#define pthread_attr_getschedpolicy    PTHREAD_ATTR_GETSCHEDPOLICY_FUNC_TAOS_FORBID
#define pthread_attr_getscope          PTHREAD_ATTR_GETSCOPE_FUNC_TAOS_FORBID
#define pthread_attr_getstacksize      PTHREAD_ATTR_GETSTACKSIZE_FUNC_TAOS_FORBID
#define pthread_attr_init              PTHREAD_ATTR_INIT_FUNC_TAOS_FORBID
#define pthread_attr_setdetachstate    PTHREAD_ATTR_SETDETACHSTATE_FUNC_TAOS_FORBID
#define pthread_attr_setinheritsched   PTHREAD_ATTR_SETINHERITSCHED_FUNC_TAOS_FORBID
#define pthread_attr_setschedparam     PTHREAD_ATTR_SETSCHEDPARAM_FUNC_TAOS_FORBID
#define pthread_attr_setschedpolicy    PTHREAD_ATTR_SETSCHEDPOLICY_FUNC_TAOS_FORBID
#define pthread_attr_setscope          PTHREAD_ATTR_SETSCOPE_FUNC_TAOS_FORBID
#define pthread_attr_setstacksize      PTHREAD_ATTR_SETSTACKSIZE_FUNC_TAOS_FORBID
#define pthread_barrier_destroy        PTHREAD_BARRIER_DESTROY_FUNC_TAOS_FORBID
#define pthread_barrier_init           PTHREAD_BARRIER_INIT_FUNC_TAOS_FORBID
#define pthread_barrier_wait           PTHREAD_BARRIER_WAIT_FUNC_TAOS_FORBID
#define pthread_barrierattr_destroy    PTHREAD_BARRIERATTR_DESTROY_FUNC_TAOS_FORBID
#define pthread_barrierattr_getpshared PTHREAD_BARRIERATTR_GETPSHARED_FUNC_TAOS_FORBID
#define pthread_barrierattr_init       PTHREAD_BARRIERATTR_INIT_FUNC_TAOS_FORBID
#define pthread_barrierattr_setpshared PTHREAD_BARRIERATTR_SETPSHARED_FUNC_TAOS_FORBID
#define pthread_cancel                 PTHREAD_CANCEL_FUNC_TAOS_FORBID
#define pthread_cond_destroy           PTHREAD_COND_DESTROY_FUNC_TAOS_FORBID
#define pthread_cond_init              PTHREAD_COND_INIT_FUNC_TAOS_FORBID
#define pthread_cond_signal            PTHREAD_COND_SIGNAL_FUNC_TAOS_FORBID
#define pthread_cond_broadcast         PTHREAD_COND_BROADCAST_FUNC_TAOS_FORBID
#define pthread_cond_wait              PTHREAD_COND_WAIT_FUNC_TAOS_FORBID
#define pthread_cond_timedwait         PTHREAD_COND_TIMEDWAIT_FUNC_TAOS_FORBID
#define pthread_condattr_destroy       PTHREAD_CONDATTR_DESTROY_FUNC_TAOS_FORBID
#define pthread_condattr_getpshared    PTHREAD_CONDATTR_GETPSHARED_FUNC_TAOS_FORBID
#define pthread_condattr_init          PTHREAD_CONDATTR_INIT_FUNC_TAOS_FORBID
#define pthread_condattr_setpshared    PTHREAD_CONDATTR_SETPSHARED_FUNC_TAOS_FORBID
#define pthread_detach                 PTHREAD_DETACH_FUNC_TAOS_FORBID

#if !defined(_ALPINE)
#define pthread_equal                  PTHREAD_EQUAL_FUNC_TAOS_FORBID
#endif

#define pthread_exit                   PTHREAD_EXIT_FUNC_TAOS_FORBID
#define pthread_getschedparam          PTHREAD_GETSCHEDPARAM_FUNC_TAOS_FORBID
#define pthread_getspecific            PTHREAD_GETSPECIFIC_FUNC_TAOS_FORBID
#define pthread_join                   PTHREAD_JOIN_FUNC_TAOS_FORBID
#define pthread_key_create             PTHREAD_KEY_CREATE_FUNC_TAOS_FORBID
#define pthread_key_delete             PTHREAD_KEY_DELETE_FUNC_TAOS_FORBID
#define pthread_kill                   PTHREAD_KILL_FUNC_TAOS_FORBID
#define pthread_mutex_consistent       PTHREAD_MUTEX_CONSISTENT_FUNC_TAOS_FORBID
#define pthread_mutex_destroy          PTHREAD_MUTEX_DESTROY_FUNC_TAOS_FORBID
#define pthread_mutex_init             PTHREAD_MUTEX_INIT_FUNC_TAOS_FORBID
#define pthread_mutex_lock             PTHREAD_MUTEX_LOCK_FUNC_TAOS_FORBID
#define pthread_mutex_timedlock        PTHREAD_MUTEX_TIMEDLOCK_FUNC_TAOS_FORBID
#define pthread_mutex_trylock          PTHREAD_MUTEX_TRYLOCK_FUNC_TAOS_FORBID
#define pthread_mutex_unlock           PTHREAD_MUTEX_UNLOCK_FUNC_TAOS_FORBID
#define pthread_mutexattr_destroy      PTHREAD_MUTEXATTR_DESTROY_FUNC_TAOS_FORBID
#define pthread_mutexattr_getpshared   PTHREAD_MUTEXATTR_GETPSHARED_FUNC_TAOS_FORBID
#define pthread_mutexattr_getrobust    PTHREAD_MUTEXATTR_GETROBUST_FUNC_TAOS_FORBID
#define pthread_mutexattr_gettype      PTHREAD_MUTEXATTR_GETTYPE_FUNC_TAOS_FORBID
#define pthread_mutexattr_init         PTHREAD_MUTEXATTR_INIT_FUNC_TAOS_FORBID
#define pthread_mutexattr_setpshared   PTHREAD_MUTEXATTR_SETPSHARED_FUNC_TAOS_FORBID
#define pthread_mutexattr_setrobust    PTHREAD_MUTEXATTR_SETROBUST_FUNC_TAOS_FORBID
#define pthread_mutexattr_settype      PTHREAD_MUTEXATTR_SETTYPE_FUNC_TAOS_FORBID
#define pthread_once                   PTHREAD_ONCE_FUNC_TAOS_FORBID
#define pthread_rwlock_destroy         PTHREAD_RWLOCK_DESTROY_FUNC_TAOS_FORBID
#define pthread_rwlock_init            PTHREAD_RWLOCK_INIT_FUNC_TAOS_FORBID
#define pthread_rwlock_rdlock          PTHREAD_RWLOCK_RDLOCK_FUNC_TAOS_FORBID
#define pthread_rwlock_timedrdlock     PTHREAD_RWLOCK_TIMEDRDLOCK_FUNC_TAOS_FORBID
#define pthread_rwlock_timedwrlock     PTHREAD_RWLOCK_TIMEDWRLOCK_FUNC_TAOS_FORBID
#define pthread_rwlock_tryrdlock       PTHREAD_RWLOCK_TRYRDLOCK_FUNC_TAOS_FORBID
#define pthread_rwlock_trywrlock       PTHREAD_RWLOCK_TRYWRLOCK_FUNC_TAOS_FORBID
#define pthread_rwlock_unlock          PTHREAD_RWLOCK_UNLOCK_FUNC_TAOS_FORBID
#define pthread_rwlock_wrlock          PTHREAD_RWLOCK_WRLOCK_FUNC_TAOS_FORBID
#define pthread_rwlockattr_destroy     PTHREAD_RWLOCKATTR_DESTROY_FUNC_TAOS_FORBID
#define pthread_rwlockattr_getpshared  PTHREAD_RWLOCKATTR_GETPSHARED_FUNC_TAOS_FORBID
#define pthread_rwlockattr_init        PTHREAD_RWLOCKATTR_INIT_FUNC_TAOS_FORBID
#define pthread_rwlockattr_setpshared  PTHREAD_RWLOCKATTR_SETPSHARED_FUNC_TAOS_FORBID
#define pthread_self                   PTHREAD_SELF_FUNC_TAOS_FORBID
#define pthread_setcancelstate         PTHREAD_SETCANCELSTATE_FUNC_TAOS_FORBID
#define pthread_setcanceltype          PTHREAD_SETCANCELTYPE_FUNC_TAOS_FORBID
#define pthread_setschedparam          PTHREAD_SETSCHEDPARAM_FUNC_TAOS_FORBID
#define pthread_setspecific            PTHREAD_SETSPECIFIC_FUNC_TAOS_FORBID
#define pthread_spin_destroy           PTHREAD_SPIN_DESTROY_FUNC_TAOS_FORBID
#define pthread_spin_init              PTHREAD_SPIN_INIT_FUNC_TAOS_FORBID
#define pthread_spin_lock              PTHREAD_SPIN_LOCK_FUNC_TAOS_FORBID
#define pthread_spin_trylock           PTHREAD_SPIN_TRYLOCK_FUNC_TAOS_FORBID
#define pthread_spin_unlock            PTHREAD_SPIN_UNLOCK_FUNC_TAOS_FORBID
#define pthread_testcancel             PTHREAD_TESTCANCEL_FUNC_TAOS_FORBID
#define pthread_sigmask                PTHREAD_SIGMASK_FUNC_TAOS_FORBID
#define sigwait                        SIGWAIT_FUNC_TAOS_FORBID
#endif

int32_t taosThreadCreate(TdThread *tid, const TdThreadAttr *attr, void *(*start)(void *), void *arg);
int32_t taosThreadAttrDestroy(TdThreadAttr *attr);
int32_t taosThreadAttrGetDetachState(const TdThreadAttr *attr, int32_t *detachstate);
int32_t taosThreadAttrGetInheritSched(const TdThreadAttr *attr, int32_t *inheritsched);
int32_t taosThreadAttrGetSchedParam(const TdThreadAttr *attr, struct sched_param *param);
int32_t taosThreadAttrGetSchedPolicy(const TdThreadAttr *attr, int32_t *policy);
int32_t taosThreadAttrGetScope(const TdThreadAttr *attr, int32_t *contentionscope);
int32_t taosThreadAttrGetStackSize(const TdThreadAttr *attr, size_t *stacksize);
int32_t taosThreadAttrInit(TdThreadAttr *attr);
int32_t taosThreadAttrSetDetachState(TdThreadAttr *attr, int32_t detachstate);
int32_t taosThreadAttrSetInheritSched(TdThreadAttr *attr, int32_t inheritsched);
int32_t taosThreadAttrSetSchedParam(TdThreadAttr *attr, const struct sched_param *param);
int32_t taosThreadAttrSetSchedPolicy(TdThreadAttr *attr, int32_t policy);
int32_t taosThreadAttrSetScope(TdThreadAttr *attr, int32_t contentionscope);
int32_t taosThreadAttrSetStackSize(TdThreadAttr *attr, size_t stacksize);
int32_t taosThreadCancel(TdThread thread);
int32_t taosThreadCondDestroy(TdThreadCond *cond);
int32_t taosThreadCondInit(TdThreadCond *cond, const TdThreadCondAttr *attr);
int32_t taosThreadCondSignal(TdThreadCond *cond);
int32_t taosThreadCondBroadcast(TdThreadCond *cond);
int32_t taosThreadCondWait(TdThreadCond *cond, TdThreadMutex *mutex);
int32_t taosThreadCondTimedWait(TdThreadCond *cond, TdThreadMutex *mutex, const struct timespec *abstime);
int32_t taosThreadCondAttrDestroy(TdThreadCondAttr *attr);
int32_t taosThreadCondAttrGetPshared(const TdThreadCondAttr *attr, int32_t *pshared);
int32_t taosThreadCondAttrInit(TdThreadCondAttr *attr);
int32_t taosThreadCondAttrSetPshared(TdThreadCondAttr *attr, int32_t pshared);
int32_t taosThreadDetach(TdThread thread);
int32_t taosThreadEqual(TdThread t1, TdThread t2);
void    taosThreadExit(void *valuePtr);
int32_t taosThreadGetSchedParam(TdThread thread, int32_t *policy, struct sched_param *param);
void   *taosThreadGetSpecific(TdThreadKey key);
int32_t taosThreadJoin(TdThread thread, void **valuePtr);
int32_t taosThreadKeyCreate(TdThreadKey *key, void (*destructor)(void *));
int32_t taosThreadKeyDelete(TdThreadKey key);
int32_t taosThreadKill(TdThread thread, int32_t sig);
// int32_t taosThreadMutexConsistent(TdThreadMutex* mutex);
int32_t taosThreadMutexDestroy(TdThreadMutex *mutex);
int32_t taosThreadMutexInit(TdThreadMutex *mutex, const TdThreadMutexAttr *attr);
int32_t taosThreadMutexLock(TdThreadMutex *mutex);
// int32_t taosThreadMutexTimedLock(TdThreadMutex * mutex, const struct timespec *abstime);
int32_t taosThreadMutexTryLock(TdThreadMutex *mutex);
int32_t taosThreadMutexUnlock(TdThreadMutex *mutex);
int32_t taosThreadMutexAttrDestroy(TdThreadMutexAttr *attr);
int32_t taosThreadMutexAttrGetPshared(const TdThreadMutexAttr *attr, int32_t *pshared);
// int32_t taosThreadMutexAttrGetRobust(const TdThreadMutexAttr * attr, int32_t * robust);
int32_t taosThreadMutexAttrGetType(const TdThreadMutexAttr *attr, int32_t *kind);
int32_t taosThreadMutexAttrInit(TdThreadMutexAttr *attr);
int32_t taosThreadMutexAttrSetPshared(TdThreadMutexAttr *attr, int32_t pshared);
// int32_t taosThreadMutexAttrSetRobust(TdThreadMutexAttr * attr, int32_t robust);
int32_t taosThreadMutexAttrSetType(TdThreadMutexAttr *attr, int32_t kind);
int32_t taosThreadOnce(TdThreadOnce *onceControl, void (*initRoutine)(void));
int32_t taosThreadRwlockDestroy(TdThreadRwlock *rwlock);
int32_t taosThreadRwlockInit(TdThreadRwlock *rwlock, const TdThreadRwlockAttr *attr);
int32_t taosThreadRwlockRdlock(TdThreadRwlock *rwlock);
// int32_t taosThreadRwlockTimedRdlock(TdThreadRwlock * rwlock, const struct timespec *abstime);
// int32_t taosThreadRwlockTimedWrlock(TdThreadRwlock * rwlock, const struct timespec *abstime);
int32_t  taosThreadRwlockTryRdlock(TdThreadRwlock *rwlock);
int32_t  taosThreadRwlockTryWrlock(TdThreadRwlock *rwlock);
int32_t  taosThreadRwlockUnlock(TdThreadRwlock *rwlock);
int32_t  taosThreadRwlockWrlock(TdThreadRwlock *rwlock);
int32_t  taosThreadRwlockAttrDestroy(TdThreadRwlockAttr *attr);
int32_t  taosThreadRwlockAttrGetPshared(const TdThreadRwlockAttr *attr, int32_t *pshared);
int32_t  taosThreadRwlockAttrInit(TdThreadRwlockAttr *attr);
int32_t  taosThreadRwlockAttrSetPshared(TdThreadRwlockAttr *attr, int32_t pshared);
TdThread taosThreadSelf(void);
int32_t  taosThreadSetCancelState(int32_t state, int32_t *oldstate);
int32_t  taosThreadSetCancelType(int32_t type, int32_t *oldtype);
int32_t  taosThreadSetSchedParam(TdThread thread, int32_t policy, const struct sched_param *param);
int32_t  taosThreadSetSpecific(TdThreadKey key, const void *value);
int32_t  taosThreadSpinDestroy(TdThreadSpinlock *lock);
int32_t  taosThreadSpinInit(TdThreadSpinlock *lock, int32_t pshared);
int32_t  taosThreadSpinLock(TdThreadSpinlock *lock);
int32_t  taosThreadSpinTrylock(TdThreadSpinlock *lock);
int32_t  taosThreadSpinUnlock(TdThreadSpinlock *lock);
void     taosThreadTestCancel(void);
void     taosThreadClear(TdThread *thread);

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_THREAD_H_*/
