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

#define ALLOW_FORBID_FUNC
#include <pthread.h>
#include "os.h"

int32_t taosThreadCreate(TdThread *tid, const TdThreadAttr *attr, void *(*start)(void *), void *arg) {
  return pthread_create(tid, attr, start, arg);
}

int32_t taosThreadAttrDestroy(TdThreadAttr *attr) { return pthread_attr_destroy(attr); }

int32_t taosThreadAttrGetDetachState(const TdThreadAttr *attr, int32_t *detachstate) {
  return pthread_attr_getdetachstate(attr, detachstate);
}

int32_t taosThreadAttrGetInheritSched(const TdThreadAttr *attr, int32_t *inheritsched) {
  return pthread_attr_getinheritsched(attr, inheritsched);
}

int32_t taosThreadAttrGetSchedParam(const TdThreadAttr *attr, struct sched_param *param) {
  return pthread_attr_getschedparam(attr, param);
}

int32_t taosThreadAttrGetSchedPolicy(const TdThreadAttr *attr, int32_t *policy) {
  return pthread_attr_getschedpolicy(attr, policy);
}

int32_t taosThreadAttrGetScope(const TdThreadAttr *attr, int32_t *contentionscope) {
  return pthread_attr_getscope(attr, contentionscope);
}

int32_t taosThreadAttrGetStackSize(const TdThreadAttr *attr, size_t *stacksize) {
  return pthread_attr_getstacksize(attr, stacksize);
}

int32_t taosThreadAttrInit(TdThreadAttr *attr) { return pthread_attr_init(attr); }

int32_t taosThreadAttrSetDetachState(TdThreadAttr *attr, int32_t detachstate) {
  return pthread_attr_setdetachstate(attr, detachstate);
}

int32_t taosThreadAttrSetInheritSched(TdThreadAttr *attr, int32_t inheritsched) {
  return pthread_attr_setinheritsched(attr, inheritsched);
}

int32_t taosThreadAttrSetSchedParam(TdThreadAttr *attr, const struct sched_param *param) {
  return pthread_attr_setschedparam(attr, param);
}

int32_t taosThreadAttrSetSchedPolicy(TdThreadAttr *attr, int32_t policy) {
  return pthread_attr_setschedpolicy(attr, policy);
}

int32_t taosThreadAttrSetScope(TdThreadAttr *attr, int32_t contentionscope) {
  return pthread_attr_setscope(attr, contentionscope);
}

int32_t taosThreadAttrSetStackSize(TdThreadAttr *attr, size_t stacksize) {
  return pthread_attr_setstacksize(attr, stacksize);
}

int32_t taosThreadCancel(TdThread thread) { return pthread_cancel(thread); }

int32_t taosThreadCondDestroy(TdThreadCond *cond) { return pthread_cond_destroy(cond); }

int32_t taosThreadCondInit(TdThreadCond *cond, const TdThreadCondAttr *attr) { return pthread_cond_init(cond, attr); }

int32_t taosThreadCondSignal(TdThreadCond *cond) { return pthread_cond_signal(cond); }

int32_t taosThreadCondBroadcast(TdThreadCond *cond) { return pthread_cond_broadcast(cond); }

int32_t taosThreadCondWait(TdThreadCond *cond, TdThreadMutex *mutex) { return pthread_cond_wait(cond, mutex); }

int32_t taosThreadCondTimedWait(TdThreadCond *cond, TdThreadMutex *mutex, const struct timespec *abstime) {
  return pthread_cond_timedwait(cond, mutex, abstime);
}

int32_t taosThreadCondAttrDestroy(TdThreadCondAttr *attr) { return pthread_condattr_destroy(attr); }

int32_t taosThreadCondAttrGetPshared(const TdThreadCondAttr *attr, int32_t *pshared) {
  return pthread_condattr_getpshared(attr, pshared);
}

int32_t taosThreadCondAttrInit(TdThreadCondAttr *attr) { return pthread_condattr_init(attr); }

int32_t taosThreadCondAttrSetPshared(TdThreadCondAttr *attr, int32_t pshared) {
  return pthread_condattr_setpshared(attr, pshared);
}

int32_t taosThreadDetach(TdThread thread) { return pthread_detach(thread); }

int32_t taosThreadEqual(TdThread t1, TdThread t2) { return pthread_equal(t1, t2); }

void taosThreadExit(void *valuePtr) { return pthread_exit(valuePtr); }

int32_t taosThreadGetSchedParam(TdThread thread, int32_t *policy, struct sched_param *param) {
  return pthread_getschedparam(thread, policy, param);
}

void *taosThreadGetSpecific(TdThreadKey key) { return pthread_getspecific(key); }

int32_t taosThreadJoin(TdThread thread, void **valuePtr) { return pthread_join(thread, valuePtr); }

int32_t taosThreadKeyCreate(TdThreadKey *key, void (*destructor)(void *)) {
  return pthread_key_create(key, destructor);
}

int32_t taosThreadKeyDelete(TdThreadKey key) { return pthread_key_delete(key); }

int32_t taosThreadKill(TdThread thread, int32_t sig) { return pthread_kill(thread, sig); }

// int32_t taosThreadMutexConsistent(TdThreadMutex* mutex) {
//   return pthread_mutex_consistent(mutex);
// }

int32_t taosThreadMutexDestroy(TdThreadMutex *mutex) { return pthread_mutex_destroy(mutex); }

int32_t taosThreadMutexInit(TdThreadMutex *mutex, const TdThreadMutexAttr *attr) {
  return pthread_mutex_init(mutex, attr);
}

int32_t taosThreadMutexLock(TdThreadMutex *mutex) { return pthread_mutex_lock(mutex); }

// int32_t taosThreadMutexTimedLock(TdThreadMutex * mutex, const struct timespec *abstime) {
//   return pthread_mutex_timedlock(mutex, abstime);
// }

int32_t taosThreadMutexTryLock(TdThreadMutex *mutex) { return pthread_mutex_trylock(mutex); }

int32_t taosThreadMutexUnlock(TdThreadMutex *mutex) { return pthread_mutex_unlock(mutex); }

int32_t taosThreadMutexAttrDestroy(TdThreadMutexAttr *attr) { return pthread_mutexattr_destroy(attr); }

int32_t taosThreadMutexAttrGetPshared(const TdThreadMutexAttr *attr, int32_t *pshared) {
  return pthread_mutexattr_getpshared(attr, pshared);
}

// int32_t taosThreadMutexAttrGetRobust(const TdThreadMutexAttr * attr, int32_t * robust) {
//   return pthread_mutexattr_getrobust(attr, robust);
// }

int32_t taosThreadMutexAttrGetType(const TdThreadMutexAttr *attr, int32_t *kind) {
  return pthread_mutexattr_gettype(attr, kind);
}

int32_t taosThreadMutexAttrInit(TdThreadMutexAttr *attr) { return pthread_mutexattr_init(attr); }

int32_t taosThreadMutexAttrSetPshared(TdThreadMutexAttr *attr, int32_t pshared) {
  return pthread_mutexattr_setpshared(attr, pshared);
}

// int32_t taosThreadMutexAttrSetRobust(TdThreadMutexAttr * attr, int32_t robust) {
//   return pthread_mutexattr_setrobust(attr, robust);
// }

int32_t taosThreadMutexAttrSetType(TdThreadMutexAttr *attr, int32_t kind) {
  return pthread_mutexattr_settype(attr, kind);
}

int32_t taosThreadOnce(TdThreadOnce *onceControl, void (*initRoutine)(void)) {
  return pthread_once(onceControl, initRoutine);
}

int32_t taosThreadRwlockDestroy(TdThreadRwlock *rwlock) { return pthread_rwlock_destroy(rwlock); }

int32_t taosThreadRwlockInit(TdThreadRwlock *rwlock, const TdThreadRwlockAttr *attr) {
  return pthread_rwlock_init(rwlock, attr);
}

int32_t taosThreadRwlockRdlock(TdThreadRwlock *rwlock) { return pthread_rwlock_rdlock(rwlock); }

// int32_t taosThreadRwlockTimedRdlock(TdThreadRwlock * rwlock, const struct timespec *abstime) {
//   return pthread_rwlock_timedrdlock(rwlock, abstime);
// }

// int32_t taosThreadRwlockTimedWrlock(TdThreadRwlock * rwlock, const struct timespec *abstime) {
//   return pthread_rwlock_timedwrlock(rwlock, abstime);
// }

int32_t taosThreadRwlockTryRdlock(TdThreadRwlock *rwlock) { return pthread_rwlock_tryrdlock(rwlock); }

int32_t taosThreadRwlockTryWrlock(TdThreadRwlock *rwlock) { return pthread_rwlock_trywrlock(rwlock); }

int32_t taosThreadRwlockUnlock(TdThreadRwlock *rwlock) { return pthread_rwlock_unlock(rwlock); }

int32_t taosThreadRwlockWrlock(TdThreadRwlock *rwlock) { return pthread_rwlock_wrlock(rwlock); }

int32_t taosThreadRwlockAttrDestroy(TdThreadRwlockAttr *attr) { return pthread_rwlockattr_destroy(attr); }

int32_t taosThreadRwlockAttrGetPshared(const TdThreadRwlockAttr *attr, int32_t *pshared) {
  return pthread_rwlockattr_getpshared(attr, pshared);
}

int32_t taosThreadRwlockAttrInit(TdThreadRwlockAttr *attr) { return pthread_rwlockattr_init(attr); }

int32_t taosThreadRwlockAttrSetPshared(TdThreadRwlockAttr *attr, int32_t pshared) {
  return pthread_rwlockattr_setpshared(attr, pshared);
}

TdThread taosThreadSelf(void) { return pthread_self(); }

int32_t taosThreadSetCancelState(int32_t state, int32_t *oldstate) { return pthread_setcancelstate(state, oldstate); }

int32_t taosThreadSetCancelType(int32_t type, int32_t *oldtype) { return pthread_setcanceltype(type, oldtype); }

int32_t taosThreadSetSchedParam(TdThread thread, int32_t policy, const struct sched_param *param) {
  return pthread_setschedparam(thread, policy, param);
}

int32_t taosThreadSetSpecific(TdThreadKey key, const void *value) { return pthread_setspecific(key, value); }

int32_t taosThreadSpinDestroy(TdThreadSpinlock *lock) {
#ifdef TD_USE_SPINLOCK_AS_MUTEX
  return pthread_mutex_destroy((pthread_mutex_t *)lock);
#else
  return pthread_spin_destroy((pthread_spinlock_t *)lock);
#endif
}

int32_t taosThreadSpinInit(TdThreadSpinlock *lock, int32_t pshared) {
#ifdef TD_USE_SPINLOCK_AS_MUTEX
  assert(pshared == 0);
  return pthread_mutex_init((pthread_mutex_t *)lock, NULL);
#else
  return pthread_spin_init((pthread_spinlock_t *)lock, pshared);
#endif
}

int32_t taosThreadSpinLock(TdThreadSpinlock *lock) {
#ifdef TD_USE_SPINLOCK_AS_MUTEX
  return pthread_mutex_lock((pthread_mutex_t *)lock);
#else
  return pthread_spin_lock((pthread_spinlock_t *)lock);
#endif
}

int32_t taosThreadSpinTrylock(TdThreadSpinlock *lock) {
#ifdef TD_USE_SPINLOCK_AS_MUTEX
  return pthread_mutex_trylock((pthread_mutex_t *)lock);
#else
  return pthread_spin_trylock((pthread_spinlock_t *)lock);
#endif
}

int32_t taosThreadSpinUnlock(TdThreadSpinlock *lock) {
#ifdef TD_USE_SPINLOCK_AS_MUTEX
  return pthread_mutex_unlock((pthread_mutex_t *)lock);
#else
  return pthread_spin_unlock((pthread_spinlock_t *)lock);
#endif
}

void taosThreadTestCancel(void) { return pthread_testcancel(); }

void taosThreadClear(TdThread *thread) { memset(thread, 0, sizeof(TdThread)); }