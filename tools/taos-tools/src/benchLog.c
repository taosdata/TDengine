/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#include "bench.h"
#include "benchLog.h"


//
// -----------  log -----------
//


// --------  OS IMPL
#ifdef WINDOWS
typedef CRITICAL_SECTION     TdThreadMutex;      // windows api
typedef HANDLE               TdThreadMutexAttr;  // windows api
#else
typedef pthread_mutex_t      TdThreadMutex;
typedef pthread_mutexattr_t  TdThreadMutexAttr;
#endif


int32_t taosThreadMutexInit(TdThreadMutex *mutex, const TdThreadMutexAttr *attr) {
#ifdef WINDOWS
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
    terrno = TAOS_SYSTEM_ERROR(code);
    return terrno;
  }
  return code;
#endif
}

int32_t taosThreadMutexDestroy(TdThreadMutex *mutex) {
#ifdef WINDOWS
  DeleteCriticalSection(mutex);
  return 0;
#else
  int32_t code = pthread_mutex_destroy(mutex);
  if (code) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return terrno;
  }
  return code;
#endif
}


int32_t taosThreadMutexLock(TdThreadMutex *mutex) {
#ifdef WINDOWS
  EnterCriticalSection(mutex);
  return 0;
#else
  int32_t code = pthread_mutex_lock(mutex);
  if (code) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return terrno;
  }
  return code;
#endif
}

int32_t taosThreadMutexUnlock(TdThreadMutex *mutex) {
#ifdef WINDOWS
  LeaveCriticalSection(mutex);
  return 0;
#else
  int32_t code = pthread_mutex_unlock(mutex);
  if (code) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return terrno;
  }
  return code;
#endif
}

//  ------- interface api ---------
TdThreadMutex mutexs[LOG_COUNT];

// init log
bool initLog() {
    for (int32_t i = 0; i < LOG_COUNT; i++) {
        if (taosThreadMutexInit(&mutexs[i], NULL) != 0) {
            printf("taosThreadMutexInit i=%d failed.\n", i);
        }
    }
    return true;
}

// exit log
void exitLog() {
    for (int32_t i = 0; i < LOG_COUNT; i++) {
        taosThreadMutexDestroy(&mutexs[i]);
    }
}

// lock
void lockLog(int8_t idx) {
    taosThreadMutexLock(&mutexs[idx]);
}
// unlock
void unlockLog(int8_t idx) {
    taosThreadMutexUnlock(&mutexs[idx]);
}