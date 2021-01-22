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

#ifndef TAOS_OS_FUNC_SEMPHONE

#ifdef BUG_DEMO
static __thread char zeros[sizeof(sem_t)] = {0};
int tsem_init(tsem_t *sem, int pshared, unsigned int value) {
  fprintf(stderr, "==%s[%d]%s()sem:[%p]==\n", basename(__FILE__), __LINE__, __func__, sem);
  if (memcmp(sem, zeros, sizeof(zeros))) {
    fprintf(stderr, "==%s[%d]%s()sem:[%p]==buffer not initialized\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  int e = 0;
  int r = sem_init(sem, pshared, value);
  e = errno;
  if (r) {
    fprintf(stderr, "==%s[%d]%s()sem:[%p]==failed\n", basename(__FILE__), __LINE__, __func__, sem);
  }
  errno = e;
  return r ? -1 : 0;
}

int tsem_post(tsem_t *sem) {
  return sem_post(sem);
}

int tsem_destroy(tsem_t *sem) {
  fprintf(stderr, "==%s[%d]%s()sem:[%p]==\n", basename(__FILE__), __LINE__, __func__, sem);
  if (0==memcmp(sem, zeros, sizeof(zeros))) {
    fprintf(stderr, "==%s[%d]%s()sem:[%p]==already destroyed\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  int e = 0;
  int r = sem_destroy(sem);
  e = errno;
  if (r) {
    fprintf(stderr, "==%s[%d]%s()sem:[%p]==failed\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  memset(sem, 0, sizeof(*sem));
  errno = e;
  return r ? -1 : 0;
}
#endif // BUG_DEMO

int tsem_wait(tsem_t* sem) {
  int ret = 0;
  do {
    ret = sem_wait(sem);
  } while (ret != 0 && errno == EINTR);
  return ret;
}

#endif

#ifndef TAOS_OS_FUNC_SEMPHONE_PTHREAD

bool    taosCheckPthreadValid(pthread_t thread) { return thread != 0; }
int64_t taosGetSelfPthreadId() { return (int64_t)pthread_self(); }
int64_t taosGetPthreadId(pthread_t thread) { return (int64_t)thread; }
void    taosResetPthread(pthread_t *thread) { *thread = 0; }
bool    taosComparePthread(pthread_t first, pthread_t second) { return first == second; }
int32_t taosGetPId() { return getpid(); }

int32_t taosGetCurrentAPPName(char *name, int32_t* len) {
  const char* self = "/proc/self/exe";
  char path[PATH_MAX] = {0};

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
