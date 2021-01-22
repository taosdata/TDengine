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
struct tsem_s {
  sem_t                  sem;
  volatile unsigned int  valid:1;
};

int tsem_init(tsem_t *sem, int pshared, unsigned int value) {
  fprintf(stderr, "==%s[%d]%s()sem:[%p]==\n", basename(__FILE__), __LINE__, __func__, sem);
  if (*sem) {
    fprintf(stderr, "==%s[%d]%s()sem:[%p]==buffer not initialized\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  int e = 0;
  int r = 0;
  struct tsem_s *p = (struct tsem_s *)calloc(1, sizeof(*p));
  if (!p) {
    fprintf(stderr, "==%s[%d]%s()sem:[%p]==out of memory\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  r = sem_init(&p->sem, pshared, value);
  e = errno;
  if (r) {
    fprintf(stderr, "==%s[%d]%s()sem:[%p]==failed\n", basename(__FILE__), __LINE__, __func__, sem);
    free(p);
    p = NULL;
  } else {
    p->valid = 1;
    *sem = p;
  }
  errno = e;
  return r ? -1 : 0;
}

int tsem_post(tsem_t *sem) {
  fprintf(stderr, "==%s[%d]%s()sem:[%p]==\n", basename(__FILE__), __LINE__, __func__, sem);
  struct tsem_s *p = *sem;
  if (!p) {
    fprintf(stderr, "==%s[%d]%s()sem:[%p]==not initialized\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  if (!p->valid) {
    fprintf(stderr, "==%s[%d]%s()sem:[%p]==already destroyed\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  return sem_post(&p->sem);
}

int tsem_wait(tsem_t* sem) {
  fprintf(stderr, "==%s[%d]%s()sem:[%p]==\n", basename(__FILE__), __LINE__, __func__, sem);
  struct tsem_s *p = *sem;
  if (!p) {
    fprintf(stderr, "==%s[%d]%s()sem:[%p]==not initialized\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  if (!p->valid) {
    fprintf(stderr, "==%s[%d]%s()sem:[%p]==already destroyed\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  int ret = 0;
  do {
    ret = sem_wait(&p->sem);
  } while (ret != 0 && errno == EINTR);
  return ret;
}

int tsem_timedwait(tsem_t *sem, const struct timespec *abs_timeout) {
  fprintf(stderr, "==%s[%d]%s()sem:[%p]==\n", basename(__FILE__), __LINE__, __func__, sem);
  struct tsem_s *p = *sem;
  if (!p) {
    fprintf(stderr, "==%s[%d]%s()sem:[%p]==not initialized\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  if (!p->valid) {
    fprintf(stderr, "==%s[%d]%s()sem:[%p]==already destroyed\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  int ret = 0;
  do {
    ret = sem_timedwait(&p->sem, abs_timeout);
    // EINTR: we don't adjust abs_timeout, because we are demon bug for the moment!!!
  } while (ret != 0 && errno == EINTR);
  return ret;
}

int tsem_destroy(tsem_t *sem) {
  fprintf(stderr, "==%s[%d]%s()sem:[%p]==\n", basename(__FILE__), __LINE__, __func__, sem);
  struct tsem_s *p = *sem;
  if (!p) {
    fprintf(stderr, "==%s[%d]%s()sem:[%p]==not initialized\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  if (!p->valid) {
    fprintf(stderr, "==%s[%d]%s()sem:[%p]==already destroyed\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  int e = 0;
  int r = sem_destroy(&p->sem);
  e = errno;
  if (r) {
    fprintf(stderr, "==%s[%d]%s()sem:[%p]==failed\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  p->valid = 0;
  free(p);
  *sem = NULL;
  errno = e;
  return r ? -1 : 0;
}
#else // BUG_DEMO
int tsem_wait(tsem_t* sem) {
  int ret = 0;
  do {
    ret = sem_wait(sem);
  } while (ret != 0 && errno == EINTR);
  return ret;
}
#endif // BUG_DEMO

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
