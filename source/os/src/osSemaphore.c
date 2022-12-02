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

void taosResetPthread(TdThread* thread) { thread->p = 0; }

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
  char filepath[1024] = {0};

  GetModuleFileName(NULL, filepath, MAX_PATH);
  char* sub = strrchr(filepath, '.');
  if (sub != NULL) {
    *sub = '\0';
  }
  char* end = strrchr(filepath, TD_DIRSEP[0]);
  if (end == NULL) {
    end = filepath;
  }

  tstrncpy(name, end, TSDB_APP_NAME_LEN);

  if (len != NULL) {
    *len = (int32_t)strlen(end);
  }

  return 0;
}

int32_t tsem_wait(tsem_t* sem) {
  int ret = 0;
  do {
    ret = sem_wait(sem);
  } while (ret != 0 && errno == EINTR);
  return ret;
}

int32_t tsem_timewait(tsem_t* sem, int64_t ms) {
  struct timespec ts;
  taosClockGetTime(0, &ts);

  ts.tv_nsec += ms * 1000000;
  ts.tv_sec += ts.tv_nsec / 1000000000;
  ts.tv_nsec %= 1000000000;
  int rc;
  while ((rc = sem_timedwait(sem, &ts)) == -1 && errno == EINTR) continue;
  return rc;
  /* This should have timed out */
  // assert(errno == ETIMEDOUT);
  // assert(rc != 0);
  // GetSystemTimeAsFileTime(&ft_after);
  // // We specified a non-zero wait. Time must advance.
  // if (ft_before.dwLowDateTime == ft_after.dwLowDateTime && ft_before.dwHighDateTime == ft_after.dwHighDateTime)
  //   {
  //     printf("nanoseconds: %d, rc: %d, code:0x%x. before filetime: %d, %d; after filetime: %d, %d\n",
  //         nanosecs, rc, errno,
  //         (int)ft_before.dwLowDateTime, (int)ft_before.dwHighDateTime,
  //         (int)ft_after.dwLowDateTime, (int)ft_after.dwHighDateTime);
  //     printf("time must advance during sem_timedwait.");
  //     return 1;
  //   }
}

#elif defined(_TD_DARWIN_64)

#include <libproc.h>

int tsem_init(tsem_t *psem, int flags, unsigned int count) {
  *psem = dispatch_semaphore_create(count);
  if (*psem == NULL) return -1;
  return 0;
}

int tsem_destroy(tsem_t *psem) {
  if (psem == NULL || *psem == NULL) return -1;
  // dispatch_release(*psem);
  // *psem = NULL;
  return 0;
}

int tsem_post(tsem_t *psem) {
  if (psem == NULL || *psem == NULL) return -1;
  dispatch_semaphore_signal(*psem);
  return 0;
}

int tsem_wait(tsem_t *psem) {
  if (psem == NULL || *psem == NULL) return -1;
  dispatch_semaphore_wait(*psem, DISPATCH_TIME_FOREVER);
  return 0;
}

int tsem_timewait(tsem_t *psem, int64_t milis) {
  if (psem == NULL || *psem == NULL) return -1;
  dispatch_semaphore_wait(*psem, milis * 1000 * 1000);
  return 0;
}

bool taosCheckPthreadValid(TdThread thread) { return thread != 0; }

int64_t taosGetSelfPthreadId() {
  TdThread thread = taosThreadSelf();
  return (int64_t)thread;
}

int64_t taosGetPthreadId(TdThread thread) { return (int64_t)thread; }

void taosResetPthread(TdThread *thread) { *thread = NULL; }

bool taosComparePthread(TdThread first, TdThread second) { return taosThreadEqual(first, second) ? true : false; }

int32_t taosGetPId() { return (int32_t)getpid(); }

int32_t taosGetAppName(char *name, int32_t *len) {
  char buf[PATH_MAX + 1];
  buf[0] = '\0';
  proc_name(getpid(), buf, sizeof(buf) - 1);
  buf[PATH_MAX] = '\0';
  size_t n = strlen(buf);
  if (len) *len = n;
  if (name) tstrncpy(name, buf, TSDB_APP_NAME_LEN);
  return 0;
}

#else

/*
 * linux implementation
 */

#include <sys/syscall.h>
#include <unistd.h>

bool taosCheckPthreadValid(TdThread thread) { return thread != 0; }

int64_t taosGetSelfPthreadId() {
  static __thread int id = 0;
  if (id != 0) return id;
  id = syscall(SYS_gettid);
  return id;
}

int64_t taosGetPthreadId(TdThread thread) { return (int64_t)thread; }
void    taosResetPthread(TdThread* thread) { *thread = 0; }
bool    taosComparePthread(TdThread first, TdThread second) { return first == second; }

int32_t taosGetPId() {
  static int32_t pid;
  if (pid != 0) return pid;
  pid = getpid();
  return pid;
}

int32_t taosGetAppName(char* name, int32_t* len) {
  const char* self = "/proc/self/exe";
  char        path[PATH_MAX] = {0};

  if (readlink(self, path, PATH_MAX) <= 0) {
    return -1;
  }

  path[PATH_MAX - 1] = 0;
  char* end = strrchr(path, '/');
  if (end == NULL) {
    return -1;
  }

  ++end;

  tstrncpy(name, end, TSDB_APP_NAME_LEN);

  if (len != NULL) {
    *len = strlen(name);
  }

  return 0;
}

int32_t tsem_wait(tsem_t* sem) {
  int ret = 0;
  do {
    ret = sem_wait(sem);
  } while (ret != 0 && errno == EINTR);
  return ret;
}

int32_t tsem_timewait(tsem_t* sem, int64_t ms) {
  int ret = 0;

  struct timespec ts = {0};

  if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
    return -1;
  }

  ts.tv_nsec += ms * 1000000;
  ts.tv_sec += ts.tv_nsec / 1000000000;
  ts.tv_nsec %= 1000000000;

  while ((ret = sem_timedwait(sem, &ts)) == -1 && errno == EINTR) continue;

  return ret;
}

#endif
