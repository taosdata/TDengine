/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
  OS_PARAM_CHECK(tid);
  OS_PARAM_CHECK(start);
#ifdef TD_ASTRA
  int32_t code = 0;
  if (!attr) {
    pthread_attr_t threadAttr;
    pthread_attr_init(&threadAttr);
    pthread_attr_setstacksize(&threadAttr, STACK_SIZE_DEFAULT);
    code = pthread_create(tid, &threadAttr, start, arg);
    pthread_attr_destroy(&threadAttr);
  } else {
    int32_t stackSize = 0;
    pthread_attr_getstacksize(attr, &stackSize);
    if (stackSize == 0) {
      pthread_attr_setstacksize(attr, STACK_SIZE_DEFAULT);
    }
    code = pthread_create(tid, attr, start, arg);
  }
#else
  int32_t code = pthread_create(tid, attr, start, arg);
#endif
  if (code) {
    taosThreadClear(tid);
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadAttrDestroy(TdThreadAttr *attr) {
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_attr_destroy(attr);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadAttrGetDetachState(const TdThreadAttr *attr, int32_t *detachstate) {
  OS_PARAM_CHECK(attr);
  OS_PARAM_CHECK(detachstate);
  int32_t code = pthread_attr_getdetachstate(attr, detachstate);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadAttrGetInheritSched(const TdThreadAttr *attr, int32_t *inheritsched) {
  OS_PARAM_CHECK(attr);
  OS_PARAM_CHECK(inheritsched);
  int32_t code = pthread_attr_getinheritsched(attr, inheritsched);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadAttrGetSchedParam(const TdThreadAttr *attr, struct sched_param *param) {
  OS_PARAM_CHECK(attr);
  OS_PARAM_CHECK(param);
  int32_t code = pthread_attr_getschedparam(attr, param);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadAttrGetSchedPolicy(const TdThreadAttr *attr, int32_t *policy) {
  OS_PARAM_CHECK(attr);
  OS_PARAM_CHECK(policy);
  int32_t code = pthread_attr_getschedpolicy(attr, policy);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadAttrGetScope(const TdThreadAttr *attr, int32_t *contentionscope) {
  OS_PARAM_CHECK(attr);
  OS_PARAM_CHECK(contentionscope);
  int32_t code = pthread_attr_getscope(attr, contentionscope);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadAttrGetStackSize(const TdThreadAttr *attr, size_t *stacksize) {
  OS_PARAM_CHECK(attr);
  OS_PARAM_CHECK(stacksize);
  int32_t code = pthread_attr_getstacksize(attr, stacksize);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadAttrInit(TdThreadAttr *attr) {
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_attr_init(attr);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadAttrSetDetachState(TdThreadAttr *attr, int32_t detachstate) {
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_attr_setdetachstate(attr, detachstate);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadAttrSetInheritSched(TdThreadAttr *attr, int32_t inheritsched) {
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_attr_setinheritsched(attr, inheritsched);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadAttrSetSchedParam(TdThreadAttr *attr, const struct sched_param *param) {
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_attr_setschedparam(attr, param);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadAttrSetSchedPolicy(TdThreadAttr *attr, int32_t policy) {
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_attr_setschedpolicy(attr, policy);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadAttrSetScope(TdThreadAttr *attr, int32_t contentionscope) {
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_attr_setscope(attr, contentionscope);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadAttrSetStackSize(TdThreadAttr *attr, size_t stacksize) {
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_attr_setstacksize(attr, stacksize);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadCancel(TdThread thread) {
  int32_t code = pthread_cancel(thread);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadCondDestroy(TdThreadCond *cond) {
  OS_PARAM_CHECK(cond);
#ifdef __USE_WIN_THREAD
  return 0;
#else
  int32_t code = pthread_cond_destroy(cond);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadCondInit(TdThreadCond *cond, const TdThreadCondAttr *attr) {
  OS_PARAM_CHECK(cond);
#ifdef __USE_WIN_THREAD
  InitializeConditionVariable(cond);
  return 0;
#else
  int32_t code = pthread_cond_init(cond, attr);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadCondSignal(TdThreadCond *cond) {
  OS_PARAM_CHECK(cond);
#ifdef __USE_WIN_THREAD
  WakeConditionVariable(cond);
  return 0;
#else
  int32_t code = pthread_cond_signal(cond);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadCondBroadcast(TdThreadCond *cond) {
  OS_PARAM_CHECK(cond);
#ifdef __USE_WIN_THREAD
  WakeAllConditionVariable(cond);
  return 0;
#else
  int32_t code = pthread_cond_broadcast(cond);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadCondWait(TdThreadCond *cond, TdThreadMutex *mutex) {
  OS_PARAM_CHECK(cond);
  OS_PARAM_CHECK(mutex);
#ifdef __USE_WIN_THREAD
  if (!SleepConditionVariableCS(cond, mutex, INFINITE)) {
    return EINVAL;
  }
  return 0;
#else
  int32_t code = pthread_cond_wait(cond, mutex);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadCondTimedWait(TdThreadCond *cond, TdThreadMutex *mutex, const struct timespec *abstime) {
  if (!abstime) return 0;
  OS_PARAM_CHECK(cond);
  OS_PARAM_CHECK(mutex);
#ifdef __USE_WIN_THREAD
  if (SleepConditionVariableCS(cond, mutex, (DWORD)(abstime->tv_sec * 1e3 + abstime->tv_nsec / 1e6))) return 0;
  DWORD error = GetLastError();
  if (error == ERROR_TIMEOUT) {
    return TSDB_CODE_TIMEOUT_ERROR;
  }
  return TAOS_SYSTEM_WINAPI_ERROR(error);
#else
  int32_t code = pthread_cond_timedwait(cond, mutex, abstime);
  if (code == ETIMEDOUT) {
    return TSDB_CODE_TIMEOUT_ERROR;
  } else if (code) {
    return TAOS_SYSTEM_ERROR(code);
  } else {
    return 0;
  }
#endif
}

int32_t taosThreadCondAttrDestroy(TdThreadCondAttr *attr) {
#ifdef __USE_WIN_THREAD
  return 0;
#else
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_condattr_destroy(attr);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadCondAttrGetPshared(const TdThreadCondAttr *attr, int32_t *pshared) {
  OS_PARAM_CHECK(attr);
  OS_PARAM_CHECK(pshared);
#ifdef __USE_WIN_THREAD
  if (pshared) *pshared = PTHREAD_PROCESS_PRIVATE;
  return 0;
#else
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_condattr_getpshared(attr, pshared);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadCondAttrInit(TdThreadCondAttr *attr) {
#ifdef __USE_WIN_THREAD
  return 0;
#else
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_condattr_init(attr);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadCondAttrSetclock(TdThreadCondAttr *attr, int clockId) {
#ifdef __USE_WIN_THREAD
  return 0;
#elif defined(__APPLE__)
  return 0;
#else
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_condattr_setclock(attr, clockId);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadCondAttrSetPshared(TdThreadCondAttr *attr, int32_t pshared) {
  OS_PARAM_CHECK(attr);
#ifdef __USE_WIN_THREAD
  return 0;
#else
  int32_t code = pthread_condattr_setpshared(attr, pshared);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadDetach(TdThread thread) {
  int32_t code = pthread_detach(thread);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadEqual(TdThread t1, TdThread t2) { return pthread_equal(t1, t2); }

void taosThreadExit(void *valuePtr) {
  if (valuePtr) return pthread_exit(valuePtr);
}

int32_t taosThreadGetSchedParam(TdThread thread, int32_t *policy, struct sched_param *param) {
  OS_PARAM_CHECK(policy);
  OS_PARAM_CHECK(param);
  int32_t code = pthread_getschedparam(thread, policy, param);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

void *taosThreadGetSpecific(TdThreadKey key) { return pthread_getspecific(key); }

int32_t taosThreadJoin(TdThread thread, void **valuePtr) {
  int32_t code = pthread_join(thread, valuePtr);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadKeyCreate(TdThreadKey *key, void (*destructor)(void *)) {
  OS_PARAM_CHECK(key);
  int32_t code = pthread_key_create(key, destructor);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadKeyDelete(TdThreadKey key) {
  int32_t code = pthread_key_delete(key);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadKill(TdThread thread, int32_t sig) {
  int32_t code = pthread_kill(thread, sig);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

// int32_t taosThreadMutexConsistent(TdThreadMutex* mutex) {
//   return pthread_mutex_consistent(mutex);
// }

int32_t taosThreadMutexDestroy(TdThreadMutex *mutex) {
  OS_PARAM_CHECK(mutex);
#ifdef __USE_WIN_THREAD
  DeleteCriticalSection(mutex);
  return 0;
#else
  int32_t code = pthread_mutex_destroy(mutex);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadMutexInit(TdThreadMutex *mutex, const TdThreadMutexAttr *attr) {
  OS_PARAM_CHECK(mutex);
#ifdef __USE_WIN_THREAD
  /**
   * Windows Server 2003 and Windows XP:  In low memory situations, InitializeCriticalSection can raise a
   * STATUS_NO_MEMORY exception. Starting with Windows Vista, this exception was eliminated and
   * InitializeCriticalSection always succeeds, even in low memory situations.
   */
  InitializeCriticalSection(mutex);
  return 0;
#else
  int32_t code = pthread_mutex_init(mutex, attr);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadMutexLock(TdThreadMutex *mutex) {
  OS_PARAM_CHECK(mutex);
#ifdef __USE_WIN_THREAD
  EnterCriticalSection(mutex);
  return 0;
#else
  int32_t code = pthread_mutex_lock(mutex);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

// int32_t taosThreadMutexTimedLock(TdThreadMutex * mutex, const struct timespec *abstime) {
//   return pthread_mutex_timedlock(mutex, abstime);
// }

int32_t taosThreadMutexTryLock(TdThreadMutex *mutex) {
  OS_PARAM_CHECK(mutex);
#ifdef __USE_WIN_THREAD
  if (TryEnterCriticalSection(mutex)) return 0;
  return EBUSY;
#else
  int32_t code = pthread_mutex_trylock(mutex);
  if (code && code != EBUSY) {
    code = TAOS_SYSTEM_ERROR(code);
  }
  return code;
#endif
}

int32_t taosThreadMutexUnlock(TdThreadMutex *mutex) {
  OS_PARAM_CHECK(mutex);
#ifdef __USE_WIN_THREAD
  LeaveCriticalSection(mutex);
  return 0;
#else
  int32_t code = pthread_mutex_unlock(mutex);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadMutexAttrDestroy(TdThreadMutexAttr *attr) {
#ifdef __USE_WIN_THREAD
  return 0;
#else
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_mutexattr_destroy(attr);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadMutexAttrGetPshared(const TdThreadMutexAttr *attr, int32_t *pshared) {
  OS_PARAM_CHECK(pshared);
#ifdef __USE_WIN_THREAD
  if (pshared) *pshared = PTHREAD_PROCESS_PRIVATE;
  return 0;
#else
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_mutexattr_getpshared(attr, pshared);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

// int32_t taosThreadMutexAttrGetRobust(const TdThreadMutexAttr * attr, int32_t * robust) {
//   return pthread_mutexattr_getrobust(attr, robust);
// }

int32_t taosThreadMutexAttrGetType(const TdThreadMutexAttr *attr, int32_t *kind) {
  OS_PARAM_CHECK(kind);
#ifdef __USE_WIN_THREAD
  if (kind) *kind = PTHREAD_MUTEX_NORMAL;
  return 0;
#else
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_mutexattr_gettype(attr, kind);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadMutexAttrInit(TdThreadMutexAttr *attr) {
#ifdef __USE_WIN_THREAD
  return 0;
#else
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_mutexattr_init(attr);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadMutexAttrSetPshared(TdThreadMutexAttr *attr, int32_t pshared) {
#ifdef __USE_WIN_THREAD
  return 0;
#else
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_mutexattr_setpshared(attr, pshared);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

// int32_t taosThreadMutexAttrSetRobust(TdThreadMutexAttr * attr, int32_t robust) {
//   return pthread_mutexattr_setrobust(attr, robust);
// }

int32_t taosThreadMutexAttrSetType(TdThreadMutexAttr *attr, int32_t kind) {
#ifdef __USE_WIN_THREAD
  return 0;
#else
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_mutexattr_settype(attr, kind);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadOnce(TdThreadOnce *onceControl, void (*initRoutine)(void)) {
  int32_t code = pthread_once(onceControl, initRoutine);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

#ifdef __USE_WIN_THREAD
/* glibc default (PTHREAD_RWLOCK_PREFER_READER_NP): a queued writer does not
 * stop a new reader. Writer-prefer (preferWriter != 0) does, matching
 * PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP used by meta/wal/bse.
 * Not recursive. Unlock of a lock this thread does not hold is undefined,
 * same as pthread_rwlock_unlock.
 * No logging here. uDebug with asyncLog=0 writes the log file under this same
 * rwlock, and a log call while gate is held recurses until the stack overflows.
 * gate is an SRWLOCK used only in exclusive mode so a zero-filled lock (the
 * sessMgt pattern, and extConnector lock-after-destroy) stays valid. */
static int32_t taosWinRwlockReaderBlocked(const TdThreadRwlock *rwlock) {
  if (rwlock->state & TD_WIN_RWLOCK_WRITER_BIT) return 1;
  if (rwlock->preferWriter && rwlock->nWriterWait > 0) return 1;
  return 0;
}

static int32_t taosWinRwlockWait(TdThreadRwlock *rwlock, CONDITION_VARIABLE *cv) {
  if (!SleepConditionVariableSRW(cv, &rwlock->gate, INFINITE, 0)) {
    return EINVAL;
  }
  return 0;
}

int32_t taosWinThreadRwlockAttrSetKindNP(TdThreadRwlockAttr *attr, int32_t kind) {
  OS_PARAM_CHECK(attr);
  /* 0 is reader-prefer (Linux default). Any other glibc kind is writer-prefer. */
  *attr = (kind == 0) ? 0 : 1;
  return 0;
}
#endif

int32_t taosThreadRwlockDestroy(TdThreadRwlock *rwlock) {
#ifdef __USE_WIN_THREAD
  /* SRWLOCK and CONDITION_VARIABLE have no destroy. Keep this a no-op so a
   * later Rdlock/Wrlock on a zeroed lock (extConnector) is still defined. */
  OS_PARAM_CHECK(rwlock);
  return 0;
#else
  OS_PARAM_CHECK(rwlock);
  int32_t code = pthread_rwlock_destroy(rwlock);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadRwlockInit(TdThreadRwlock *rwlock, const TdThreadRwlockAttr *attr) {
  OS_PARAM_CHECK(rwlock);
#ifdef __USE_WIN_THREAD
  int32_t preferWriter = 0;
  /* Read preferWriter before the memset. Callers pass attr pointing at a field
   * inside rwlock (see bse/meta/wal), so reading it afterwards is always 0. */
  if (attr != NULL && *attr != 0) {
    preferWriter = 1;
  }
  memset(rwlock, 0, sizeof(*rwlock));
  /* NULL attr is the Linux default: reader-prefer. SRWLOCK_INIT and
   * CONDITION_VARIABLE_INIT are all-zero, so a lock that skips Init stays valid. */
  InitializeSRWLock(&rwlock->gate);
  InitializeConditionVariable(&rwlock->readerCv);
  InitializeConditionVariable(&rwlock->writerCv);
  rwlock->preferWriter = preferWriter;
  return 0;
#else
  int32_t code = pthread_rwlock_init(rwlock, attr);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadRwlockRdlock(TdThreadRwlock *rwlock) {
  OS_PARAM_CHECK(rwlock);
#ifdef __USE_WIN_THREAD
  int32_t code = 0;
  AcquireSRWLockExclusive(&rwlock->gate);
  while (taosWinRwlockReaderBlocked(rwlock)) {
    code = taosWinRwlockWait(rwlock, &rwlock->readerCv);
    if (code != 0) {
      ReleaseSRWLockExclusive(&rwlock->gate);
      return code;
    }
  }
  rwlock->state += 1;
  ReleaseSRWLockExclusive(&rwlock->gate);
  return 0;
#else
  int32_t code = pthread_rwlock_rdlock(rwlock);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

// int32_t taosThreadRwlockTimedRdlock(TdThreadRwlock * rwlock, const struct timespec *abstime) {
//   return pthread_rwlock_timedrdlock(rwlock, abstime);
// }

// int32_t taosThreadRwlockTimedWrlock(TdThreadRwlock * rwlock, const struct timespec *abstime) {
//   return pthread_rwlock_timedwrlock(rwlock, abstime);
// }

int32_t taosThreadRwlockTryRdlock(TdThreadRwlock *rwlock) {
  OS_PARAM_CHECK(rwlock);
#ifdef __USE_WIN_THREAD
  if (!TryAcquireSRWLockExclusive(&rwlock->gate)) {
    return EBUSY;
  }
  if (taosWinRwlockReaderBlocked(rwlock)) {
    ReleaseSRWLockExclusive(&rwlock->gate);
    return EBUSY;
  }
  rwlock->state += 1;
  ReleaseSRWLockExclusive(&rwlock->gate);
  return 0;
#else
  int32_t code = pthread_rwlock_tryrdlock(rwlock);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadRwlockTryWrlock(TdThreadRwlock *rwlock) {
  OS_PARAM_CHECK(rwlock);
#ifdef __USE_WIN_THREAD
  if (!TryAcquireSRWLockExclusive(&rwlock->gate)) {
    return EBUSY;
  }
  if (rwlock->state != 0) {
    ReleaseSRWLockExclusive(&rwlock->gate);
    return EBUSY;
  }
  rwlock->state = TD_WIN_RWLOCK_WRITER_BIT;
  ReleaseSRWLockExclusive(&rwlock->gate);
  return 0;
#else
  int32_t code = pthread_rwlock_trywrlock(rwlock);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadRwlockUnlock(TdThreadRwlock *rwlock) {
  OS_PARAM_CHECK(rwlock);
#ifdef __USE_WIN_THREAD
  AcquireSRWLockExclusive(&rwlock->gate);
  if (rwlock->state & TD_WIN_RWLOCK_WRITER_BIT) {
    rwlock->state &= ~TD_WIN_RWLOCK_WRITER_BIT;
  } else if ((rwlock->state & TD_WIN_RWLOCK_READER_MASK) > 0) {
    rwlock->state -= 1;
  }
  /* Reader-prefer: wake every reader that is now allowed in. A waiting writer
   * is woken only when no reader remains, so it cannot cut ahead of readers
   * that arrived while it was queued. Writer-prefer skips the reader wake
   * while nWriterWait > 0. */
  if (!taosWinRwlockReaderBlocked(rwlock)) {
    WakeAllConditionVariable(&rwlock->readerCv);
  }
  if (rwlock->state == 0 && rwlock->nWriterWait > 0) {
    WakeConditionVariable(&rwlock->writerCv);
  }
  ReleaseSRWLockExclusive(&rwlock->gate);
  return 0;
#else
  int32_t code = pthread_rwlock_unlock(rwlock);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadRwlockWrlock(TdThreadRwlock *rwlock) {
  OS_PARAM_CHECK(rwlock);
#ifdef __USE_WIN_THREAD
  int32_t code = 0;
  AcquireSRWLockExclusive(&rwlock->gate);
  if (rwlock->state != 0) {
    rwlock->nWriterWait += 1;
    while (rwlock->state != 0) {
      code = taosWinRwlockWait(rwlock, &rwlock->writerCv);
      if (code != 0) {
        rwlock->nWriterWait -= 1;
        ReleaseSRWLockExclusive(&rwlock->gate);
        return code;
      }
    }
    rwlock->nWriterWait -= 1;
  }
  rwlock->state |= TD_WIN_RWLOCK_WRITER_BIT;
  ReleaseSRWLockExclusive(&rwlock->gate);
  return 0;
#else
  int32_t code = pthread_rwlock_wrlock(rwlock);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadRwlockAttrDestroy(TdThreadRwlockAttr *attr) {
#ifdef __USE_WIN_THREAD
  return 0;
#else
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_rwlockattr_destroy(attr);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadRwlockAttrGetPshared(const TdThreadRwlockAttr *attr, int32_t *pshared) {
  OS_PARAM_CHECK(attr);
  OS_PARAM_CHECK(pshared);
#ifdef __USE_WIN_THREAD
  if (pshared) *pshared = PTHREAD_PROCESS_PRIVATE;
  return 0;
#else
  int32_t code = pthread_rwlockattr_getpshared(attr, pshared);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadRwlockAttrInit(TdThreadRwlockAttr *attr) {
#ifdef __USE_WIN_THREAD
  OS_PARAM_CHECK(attr);
  /* Same default as pthread_rwlockattr_init: reader-prefer. */
  *attr = 0;
  return 0;
#else
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_rwlockattr_init(attr);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadRwlockAttrSetPshared(TdThreadRwlockAttr *attr, int32_t pshared) {
#ifdef __USE_WIN_THREAD
  return 0;
#else
  OS_PARAM_CHECK(attr);
  int32_t code = pthread_rwlockattr_setpshared(attr, pshared);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

TdThread taosThreadSelf(void) { return pthread_self(); }

int32_t taosThreadSetCancelState(int32_t state, int32_t *oldstate) {
  int32_t code = pthread_setcancelstate(state, oldstate);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadSetCancelType(int32_t type, int32_t *oldtype) {
  int32_t code = pthread_setcanceltype(type, oldtype);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadSetSchedParam(TdThread thread, int32_t policy, const struct sched_param *param) {
  OS_PARAM_CHECK(param);
  int32_t code = pthread_setschedparam(thread, policy, param);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadSetSpecific(TdThreadKey key, const void *value) {
  OS_PARAM_CHECK(value);
  int32_t code = pthread_setspecific(key, value);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

int32_t taosThreadSpinDestroy(TdThreadSpinlock *lock) {
  OS_PARAM_CHECK(lock);
#ifdef TD_USE_SPINLOCK_AS_MUTEX
  return pthread_mutex_destroy((pthread_mutex_t *)lock);
#else
  int32_t code = pthread_spin_destroy((pthread_spinlock_t *)lock);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadSpinInit(TdThreadSpinlock *lock, int32_t pshared) {
  OS_PARAM_CHECK(lock);
#ifdef TD_USE_SPINLOCK_AS_MUTEX
  if (pshared != 0) return TSDB_CODE_INVALID_PARA;
  return pthread_mutex_init((pthread_mutex_t *)lock, NULL);
#else
  int32_t code = pthread_spin_init((pthread_spinlock_t *)lock, pshared);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadSpinLock(TdThreadSpinlock *lock) {
  OS_PARAM_CHECK(lock);
#ifdef TD_USE_SPINLOCK_AS_MUTEX
  return pthread_mutex_lock((pthread_mutex_t *)lock);
#else
  int32_t code = pthread_spin_lock((pthread_spinlock_t *)lock);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

int32_t taosThreadSpinTrylock(TdThreadSpinlock *lock) {
  OS_PARAM_CHECK(lock);
#ifdef TD_USE_SPINLOCK_AS_MUTEX
  return pthread_mutex_trylock((pthread_mutex_t *)lock);
#else
  int32_t code = pthread_spin_trylock((pthread_spinlock_t *)lock);
  if (code && code != EBUSY) {
    code = TAOS_SYSTEM_ERROR(code);
  }
  return code;
#endif
}

int32_t taosThreadSpinUnlock(TdThreadSpinlock *lock) {
  OS_PARAM_CHECK(lock);
#ifdef TD_USE_SPINLOCK_AS_MUTEX
  return pthread_mutex_unlock((pthread_mutex_t *)lock);
#else
  int32_t code = pthread_spin_unlock((pthread_spinlock_t *)lock);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
#endif
}

void taosThreadTestCancel(void) { return pthread_testcancel(); }

void taosThreadClear(TdThread *thread) {
  if (!thread) return;
  (void)memset(thread, 0, sizeof(TdThread));
}

#ifdef WINDOWS
bool taosThreadIsMain() {
  DWORD curProcessId = GetCurrentProcessId();
  DWORD curThreadId = GetCurrentThreadId();
  DWORD dwThreadId = -1;

  HANDLE hThreadSnapshot = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
  if (hThreadSnapshot == INVALID_HANDLE_VALUE) {
    return false;
  }

  THREADENTRY32 te32;
  te32.dwSize = sizeof(THREADENTRY32);

  if (!Thread32First(hThreadSnapshot, &te32)) {
    CloseHandle(hThreadSnapshot);
    return false;
  }

  do {
    if (te32.th32OwnerProcessID == curProcessId) {
      dwThreadId = te32.th32ThreadID;
      break;
    }
  } while (Thread32Next(hThreadSnapshot, &te32));

  CloseHandle(hThreadSnapshot);

  return curThreadId == dwThreadId;
}
#endif
