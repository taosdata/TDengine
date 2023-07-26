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

#define _DEFAULT_SOURCE
#include "os.h"

#if !defined(_TD_DARWIN_64)

int32_t tsem_wait(tsem_t* sem) {
  int ret = 0;
  do {
    ret = sem_wait(sem);
  } while (ret != 0 && errno == EINTR);
  return ret;
}

#endif

#if !(defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32) || defined(_TD_DARWIN_64))

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

bool taosCheckPthreadValid(pthread_t thread) { return thread != 0; }

int64_t taosGetSelfPthreadId() {
  static __thread int id = 0;
  if (id != 0) return id;
  id = syscall(SYS_gettid);
  return id;
}

int64_t taosGetPthreadId(pthread_t thread) { return (int64_t)thread; }
void    taosResetPthread(pthread_t* thread) { *thread = 0; }
bool    taosComparePthread(pthread_t first, pthread_t second) { return first == second; }
int32_t taosGetPId() { return getpid(); }

int32_t taosGetCurrentAPPName(char* name, int32_t* len) {
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

  strcpy(name, end);

  if (len != NULL) {
    *len = strlen(name);
  }

  return 0;
}

#endif

#if (defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32))
int32_t tsem_timewait(tsem_t* sem, int64_t ms) {
  return tsem_wait(sem);
  // struct timespec ts;
  // taosClockGetTime(0, &ts);

  // ts.tv_nsec += ms * 1000000;
  // ts.tv_sec += ts.tv_nsec / 1000000000;
  // ts.tv_nsec %= 1000000000;
  // int rc;
  // while ((rc = sem_timedwait(sem, &ts)) == -1 && errno == EINTR) continue;
  // return rc;
  // /* This should have timed out */
  // // ASSERT(errno == ETIMEDOUT);
  // // ASSERT(rc != 0);
  // // GetSystemTimeAsFileTime(&ft_after);
  // // // We specified a non-zero wait. Time must advance.
  // // if (ft_before.dwLowDateTime == ft_after.dwLowDateTime && ft_before.dwHighDateTime == ft_after.dwHighDateTime)
  // //   {
  // //     printf("nanoseconds: %d, rc: %d, code:0x%x. before filetime: %d, %d; after filetime: %d, %d\n",
  // //         nanosecs, rc, errno,
  // //         (int)ft_before.dwLowDateTime, (int)ft_before.dwHighDateTime,
  // //         (int)ft_after.dwLowDateTime, (int)ft_after.dwHighDateTime);
  // //     printf("time must advance during sem_timedwait.");
  // //     return 1;
  // //   }
}

#endif

// #if defined(_TD_DARWIN_64)

// int tsem_timewait(tsem_t* psem, int64_t milis) {
//   if (psem == NULL || *psem == NULL) return -1;
//   dispatch_time_t time = dispatch_time(DISPATCH_TIME_NOW, (int64_t)(milis * USEC_PER_SEC));
//   dispatch_semaphore_wait(*psem, time);
//   return 0;
// }

// #endif