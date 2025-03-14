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
#define _DEFAULT_SOURCE
#include "os.h"
#include "pthread.h"
#include "tdef.h"

#ifdef WINDOWS

/*
 * windows implementation
 */

#include <windows.h>

bool taosCheckPthreadValid(TdThread thread) { return thread.p != NULL; }

void taosResetPthread(TdThread* thread) {
  if (thread != NULL) {
    thread->p = NULL;
  }
}

int64_t taosGetPthreadId(TdThread thread) {
#ifdef PTW32_VERSION
  return pthread_getw32threadid_np(thread);
#else
  return (int64_t)thread;
#endif
}

int64_t taosGetSelfPthreadId() { return GetCurrentThreadId(); }

bool taosComparePthread(TdThread first, TdThread second) { return first.p == second.p; }

int32_t taosGetPId() { return GetCurrentProcessId(); }

int32_t taosGetAppName(char* name, int32_t* len) {
  OS_PARAM_CHECK(name);
  char filepath[1024] = {0};

  if (GetModuleFileName(NULL, filepath, MAX_PATH) == 0) {
    terrno = TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
    return terrno;
  }
  char* sub = strrchr(filepath, '.');
  if (sub != NULL) {
    *sub = '\0';
  }
  char* end = strrchr(filepath, TD_DIRSEP[0]);
  if (end == NULL) {
    end = filepath;
  } else {
    end += 1;
  }

  tstrncpy(name, end, TSDB_APP_NAME_LEN);

  if (len != NULL) {
    *len = (int32_t)strlen(end);
  }

  return 0;
}

int32_t taosGetPIdByName(const char* name, int32_t* pPId) { return -1;}

int32_t tsem_wait(tsem_t* sem) {
  OS_PARAM_CHECK(sem);
  OS_PARAM_CHECK(*sem);
  DWORD ret = WaitForSingleObject(*sem, INFINITE);
  if (ret == WAIT_OBJECT_0) {
    return 0;
  } else {
    return TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
  }
}

int32_t tsem_timewait(tsem_t* sem, int64_t timeout_ms) {
  OS_PARAM_CHECK(sem);
  OS_PARAM_CHECK(*sem);
  DWORD result = WaitForSingleObject(*sem, timeout_ms);
  if (result == WAIT_OBJECT_0) {
    return 0;  // Semaphore acquired
  } else if (result == WAIT_TIMEOUT) {
    return TSDB_CODE_TIMEOUT_ERROR;  // Timeout reached
  } else {
    return TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
  }
}

// Inter-process sharing is not currently supported. The pshared parameter is invalid.
int32_t tsem_init(tsem_t* sem, int pshared, unsigned int value) {
  OS_PARAM_CHECK(sem);
  *sem = CreateSemaphore(NULL, value, LONG_MAX, NULL);
  return (*sem != NULL) ? 0 : TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
}

int32_t tsem_post(tsem_t* sem) {
  OS_PARAM_CHECK(sem);
  OS_PARAM_CHECK(*sem);
  if (ReleaseSemaphore(*sem, 1, NULL)) return 0;
  return TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
}

int32_t tsem_destroy(tsem_t* sem) {
  OS_PARAM_CHECK(sem);
  OS_PARAM_CHECK(*sem);
  if (CloseHandle(*sem)) return 0;
  return TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
}

#elif defined(_TD_DARWIN_64)

#include <libproc.h>

int32_t tsem_init(tsem_t *psem, int flags, unsigned int count) {
  OS_PARAM_CHECK(psem);
  *psem = dispatch_semaphore_create(count);
  if (*psem == NULL) return TAOS_SYSTEM_ERROR(ERRNO);
  return 0;
}

int32_t tsem_destroy(tsem_t *psem) {
  // if (psem == NULL || *psem == NULL) return -1;
  // dispatch_release(*psem);
  // *psem = NULL;
  return 0;
}

int32_t tsem_post(tsem_t *psem) {
  if (psem == NULL || *psem == NULL) return TSDB_CODE_INVALID_PARA;
  (void)dispatch_semaphore_signal(*psem);
  return 0;
}

int32_t tsem_wait(tsem_t *psem) {
  if (psem == NULL || *psem == NULL) return TSDB_CODE_INVALID_PARA;
  dispatch_semaphore_wait(*psem, DISPATCH_TIME_FOREVER);
  return 0;
}

int32_t tsem_timewait(tsem_t *psem, int64_t milis) {
  if (psem == NULL || *psem == NULL) return TSDB_CODE_INVALID_PARA;
  dispatch_time_t time = dispatch_time(DISPATCH_TIME_NOW, (int64_t)(milis * USEC_PER_SEC));
  if (dispatch_semaphore_wait(*psem, time) == 0) {
    return 0;
  } else {
    return TSDB_CODE_TIMEOUT_ERROR;
  }
}

bool taosCheckPthreadValid(TdThread thread) { return thread != 0; }

int64_t taosGetSelfPthreadId() {
  TdThread thread = taosThreadSelf();
  return (int64_t)thread;
}

int64_t taosGetPthreadId(TdThread thread) { return (int64_t)thread; }

void taosResetPthread(TdThread *thread) {
  if (thread) {
    *thread = NULL;
  }
}

bool taosComparePthread(TdThread first, TdThread second) { return taosThreadEqual(first, second) ? true : false; }

int32_t taosGetPId() { return (int32_t)getpid(); }

int32_t taosGetAppName(char *name, int32_t *len) {
  OS_PARAM_CHECK(name);
  char buf[PATH_MAX + 1];
  buf[0] = '\0';
  proc_name(getpid(), buf, sizeof(buf) - 1);
  buf[PATH_MAX] = '\0';
  size_t n = strlen(buf);
  if (len) *len = n;
  if (name) tstrncpy(name, buf, TSDB_APP_NAME_LEN);
  return 0;
}

int32_t taosGetPIdByName(const char* name, int32_t* pPId) {return -1;}

#else

/*
 * linux implementation
 */
#ifndef TD_ASTRA
#include <sys/syscall.h>
#endif
#include <unistd.h>

bool taosCheckPthreadValid(TdThread thread) { return thread != 0; }

int64_t taosGetSelfPthreadId() {
  static __thread int64_t id = 0;
  if (id != 0) return id;
#ifndef TD_ASTRA
  id = syscall(SYS_gettid);
#else
  id = (int64_t) taosThreadSelf();
#endif
  return id;
}

int64_t taosGetPthreadId(TdThread thread) { return (int64_t)thread; }
void    taosResetPthread(TdThread* thread) {
  if (thread) {
    *thread = 0;
  }
}
bool    taosComparePthread(TdThread first, TdThread second) { return first == second; }

int32_t taosGetPId() {
  static int32_t pid;
  if (pid != 0) return pid;
#ifndef TD_ASTRA
  pid = getpid();
#else
  pid = (int32_t)taosThreadSelf(); // TD_ASTRA_TODO
#endif
  return pid;
}

int32_t taosGetAppName(char* name, int32_t* len) {
#ifndef TD_ASTRA
  OS_PARAM_CHECK(name);
  const char* self = "/proc/self/exe";
  char        path[PATH_MAX] = {0};

  if (-1 == readlink(self, path, PATH_MAX)) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }

  path[PATH_MAX - 1] = 0;
  char* end = strrchr(path, '/');
  if (end == NULL) {
    end = path;
  } else {
    ++end;
  }

  tstrncpy(name, end, TSDB_APP_NAME_LEN);
#else
  tstrncpy(name, "tdastra", TSDB_APP_NAME_LEN); // TD_ASTRA_TODO
#endif
  if (len != NULL) {
    *len = strlen(name);
  }

  return 0;
}

int32_t taosGetPIdByName(const char* name, int32_t* pPId) {
#ifndef TD_ASTRA
  OS_PARAM_CHECK(name);
  OS_PARAM_CHECK(pPId);
  DIR*           dir = NULL;
  struct dirent* ptr = NULL;
  FILE*          fp = NULL;
  char           filepath[512];
  char           bufx[50];
  char           buf[1024] = {0};

  *pPId = -1;
  dir = opendir("/proc");
  if (dir == NULL) {
    return TAOS_SYSTEM_ERROR(ERRNO);
  }

  while ((ptr = readdir(dir)) != NULL) {
    if ((strcmp(ptr->d_name, ".") == 0) || (strcmp(ptr->d_name, "..") == 0)) {
      continue;
    }

    if (DT_DIR != ptr->d_type) {
      continue;
    }

    int32_t ret = tsnprintf(filepath, tListLen(filepath), "/proc/%s/status", ptr->d_name);
    if (ret == -1) {
      continue;
    }

    fp = fopen(filepath, "r");
    if (NULL != fp) {
      if (fgets(buf, tListLen(buf) - 1, fp) == NULL) {
        TAOS_UNUSED(fclose(fp));
        continue;
      }

      ret = sscanf(buf, "%*s %s", bufx);
      if (!strcmp(bufx, name)) {
        char* end = NULL;
        *pPId = taosStr2Int32(ptr->d_name, &end, 10);
      }
      TAOS_UNUSED(fclose(fp));
    }
  }

  TAOS_UNUSED(closedir(dir));

  if ((*pPId) == -1) {
    return TAOS_SYSTEM_ERROR(ESRCH);
  } else {
    return TSDB_CODE_SUCCESS;
  }
#else
  return TSDB_CODE_APP_ERROR;
#endif
}

int32_t tsem_init(tsem_t* psem, int flags, unsigned int count) {
  if (sem_init(psem, flags, count) == 0) {
    return 0;
  } else {
    return terrno = TAOS_SYSTEM_ERROR(ERRNO);
  }
}

int32_t tsem_timewait(tsem_t* sem, int64_t ms) {
  OS_PARAM_CHECK(sem);
  int ret = 0;

  struct timespec ts = {0};

  if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }

  ts.tv_nsec += ms * 1000000;
  ts.tv_sec += ts.tv_nsec / 1000000000;
  ts.tv_nsec %= 1000000000;

  while ((ret = sem_timedwait(sem, &ts)) == -1) {
    if (ERRNO == EINTR) {
      continue;
    } else if (ERRNO == ETIMEDOUT) {
      return TSDB_CODE_TIMEOUT_ERROR;
    } else {
      terrno = TAOS_SYSTEM_ERROR(ERRNO);
      return terrno;
    }
  }

  return 0;
}

int32_t tsem_wait(tsem_t* sem) {
  OS_PARAM_CHECK(sem);
  int ret = 0;
  do {
    ret = sem_wait(sem);
  } while (-1 == ret && ERRNO == EINTR);

  if (-1 == ret) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }

  return ret;
}

int tsem2_init(tsem2_t* sem, int pshared, unsigned int value) {
  OS_PARAM_CHECK(sem);
  int ret = taosThreadMutexInit(&sem->mutex, NULL);
  if (ret != 0) return ret;

  ret = taosThreadCondAttrInit(&sem->attr);
  if (ret != 0) {
    (void)taosThreadMutexDestroy(&sem->mutex);
    return ret;
  }

  ret = taosThreadCondAttrSetclock(&sem->attr, CLOCK_MONOTONIC);
  if (ret != 0) {
    (void)taosThreadMutexDestroy(&sem->mutex);
    (void)taosThreadCondAttrDestroy(&sem->attr);
    return ret;
  }

  ret = taosThreadCondInit(&sem->cond, &sem->attr);
  if (ret != 0) {
    (void)taosThreadMutexDestroy(&sem->mutex);
    (void)taosThreadCondAttrDestroy(&sem->attr);
    return ret;
  }

  sem->count = value;

  return 0;
}

int32_t tsem_post(tsem_t* psem) {
  OS_PARAM_CHECK(psem);
  if (sem_post(psem) == 0) {
    return 0;
  } else {
    return TAOS_SYSTEM_ERROR(ERRNO);
  }
}

int32_t tsem_destroy(tsem_t* sem) {
  OS_PARAM_CHECK(sem);
  if (sem_destroy(sem) == 0) {
    return 0;
  } else {
    return TAOS_SYSTEM_ERROR(ERRNO);
  }
}

int tsem2_post(tsem2_t* sem) {
  OS_PARAM_CHECK(sem);
  int32_t code = taosThreadMutexLock(&sem->mutex);
  if (code) {
    return code;
  }

  sem->count++;
  code = taosThreadCondSignal(&sem->cond);
  if (code) {
    return code;
  }

  code = taosThreadMutexUnlock(&sem->mutex);
  if (code) {
    return code;
  }

  return 0;
}

int tsem2_destroy(tsem2_t* sem) {
  OS_PARAM_CHECK(sem);
  (void)taosThreadMutexDestroy(&sem->mutex);
  (void)taosThreadCondDestroy(&sem->cond);
  (void)taosThreadCondAttrDestroy(&sem->attr);

  return 0;
}

int32_t tsem2_wait(tsem2_t* sem) {
  OS_PARAM_CHECK(sem);
  int32_t code = taosThreadMutexLock(&sem->mutex);
  if (code) {
    return code;
  }

  while (sem->count <= 0) {
    int ret = taosThreadCondWait(&sem->cond, &sem->mutex);
    if (0 == ret) {
      continue;
    } else {
      (void)taosThreadMutexUnlock(&sem->mutex);
      return ret;
    }
  }
  sem->count--;

  code = taosThreadMutexUnlock(&sem->mutex);
  if (code) {
    return code;
  }

  return 0;
}

int32_t tsem2_timewait(tsem2_t* sem, int64_t ms) {
  OS_PARAM_CHECK(sem);
  int32_t code = 0;

  code = taosThreadMutexLock(&sem->mutex);
  if (code) {
    return code;
  }

  if (sem->count <= 0) {
    struct timespec ts = {0};
    if (clock_gettime(CLOCK_MONOTONIC, &ts) == -1) {
      code = TAOS_SYSTEM_ERROR(ERRNO);
      (void)taosThreadMutexUnlock(&sem->mutex);
      terrno = code;
      return code;
    }

    ts.tv_sec += ms / 1000;
    ts.tv_nsec += (ms % 1000) * 1000000;
    ts.tv_sec += ts.tv_nsec / 1000000000;
    ts.tv_nsec %= 1000000000;

    while (sem->count <= 0) {
      code = taosThreadCondTimedWait(&sem->cond, &sem->mutex, &ts);
      if (code != 0) {
        (void)taosThreadMutexUnlock(&sem->mutex);
        return code;
      }
    }
  }

  sem->count--;
  
  code = taosThreadMutexUnlock(&sem->mutex);
  return code;
}

#endif
