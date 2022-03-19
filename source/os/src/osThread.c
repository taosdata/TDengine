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
#include "os.h"

// int32_t taosThreadSetnameNp(TdThread thread, const char *name) {
//     return pthread_setname_np(thread,name);
// }

int32_t taosThreadSpinInit(TdThreadSpinlock *lock, int pshared) {
    return pthread_spin_init(lock, pshared);
}

int32_t taosThreadMutexInit(TdThreadMutex *mutex, const TdThreadMutexAttr *attr) {
    return pthread_mutex_init(mutex, attr);
}

int32_t taosThreadSpinDestroy(TdThreadSpinlock *lock) {
    return pthread_spin_destroy(lock);
}

int32_t taosThreadMutexDestroy(TdThreadMutex * mutex) {
    return pthread_mutex_destroy(mutex);
}

int32_t taosThreadSpinLock(TdThreadSpinlock *lock) {
    return pthread_spin_lock(lock);
}

int32_t taosThreadMutexLock(TdThreadMutex *mutex) {
    return pthread_mutex_lock(mutex);
}

int32_t taosThreadSpinUnlock(TdThreadSpinlock *lock) {
    return pthread_spin_unlock(lock);
}

int32_t taosThreadMutexUnlock(TdThreadMutex *mutex) {
    return pthread_mutex_unlock(mutex);
}

int32_t taosThreadRwlockRdlock(TdThreadRwlock *rwlock) {
    return pthread_rwlock_rdlock(rwlock);
}

int32_t taosThreadRwlockWrlock(TdThreadRwlock *rwlock) {
    return pthread_rwlock_wrlock(rwlock);
}

int32_t taosThreadRwlockUnlock(TdThreadRwlock *rwlock) {
    return pthread_rwlock_unlock(rwlock);
}

void taosThreadTestCancel(void) {
    return pthread_testcancel();
}

int32_t taosThreadAttrInit(TdThreadAttr *attr) {
    return pthread_attr_init(attr);
}

int32_t taosThreadCreate(TdThread *tid, const TdThreadAttr *attr, void*(*start)(void*), void *arg) {
    return pthread_create(tid, attr, start, arg);
}

int32_t taosThreadOnce(TdThreadOnce *onceControl, void(*initRoutine)(void)) {
    return pthread_once(onceControl, initRoutine);
}

int32_t taosThreadAttrSetDetachState(TdThreadAttr *attr, int32_t detachState) {
    return pthread_attr_setdetachstate(attr, detachState);
}

int32_t taosThreadAttrDestroy(TdThreadAttr *attr) {
    return pthread_attr_destroy(attr);
}

int32_t taosThreadJoin(TdThread thread, void **pValue) {
    return pthread_join(thread, pValue);
}

int32_t taosThreadRwlockInit(TdThreadRwlock *rwlock, const TdThreadRwlockAttr *attr) {
    return pthread_rwlock_init(rwlock, attr);
}

int32_t taosThreadRwlockDestroy(TdThreadRwlock *rwlock) {
    return pthread_rwlock_destroy(rwlock);
}

int32_t taosThreadCondSignal(TdThreadCond *cond) {
    return pthread_cond_signal(cond);
}

int32_t taosThreadCondInit(TdThreadCond *cond, const TdThreadCondAttr *attr) {
    return pthread_cond_init(cond, attr);
}

int32_t taosThreadCondBroadcast(TdThreadCond *cond) {
    return pthread_cond_broadcast(cond);
}

int32_t taosThreadCondDestroy(TdThreadCond *cond) {
    return pthread_cond_destroy(cond);
}

int32_t taosThreadCondWait(TdThreadCond *cond, TdThreadMutex *mutex) {
    return pthread_cond_wait(cond, mutex);
}

TdThread taosThreadSelf(void) {
    return pthread_self();
}

// int32_t taosThreadGetW32ThreadIdNp(TdThread thread) {
//     return pthread_getw32threadid_np(thread);
// }

int32_t taosThreadEqual(TdThread t1, TdThread t2) {
    return pthread_equal(t1, t2);
}

int32_t taosThreadSigmask(int how, sigset_t const *set, sigset_t *oset) {
    return pthread_sigmask(how, set, oset);
}

int32_t taosThreadCancel(TdThread thread) {
    return pthread_cancel(thread);
}

int32_t taosThreadKill(TdThread thread, int sig) {
    return pthread_kill(thread, sig);
}