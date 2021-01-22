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

// fail-fast or let-it-crash philosophy
// https://en.wikipedia.org/wiki/Fail-fast
// https://stackoverflow.com/questions/4393197/erlangs-let-it-crash-philosophy-applicable-elsewhere
// experimentally, we follow log-and-crash here

#define _DEFAULT_SOURCE
#include "os.h"

// #define SEM_USE_PTHREAD
// #define SEM_USE_POSIX
#define SEM_USE_SEM

#ifdef SEM_USE_SEM
#include <mach/mach_init.h>
#include <mach/mach_error.h>
#include <mach/semaphore.h>
#include <mach/task.h>

static pthread_t                 sem_thread;
static pthread_once_t            sem_once;
static task_t                    sem_port;
static volatile int              sem_inited = 0;
static semaphore_t               sem_exit;

static void* sem_thread_routine(void *arg) {
  (void)arg;
  sem_port = mach_task_self();
  kern_return_t ret = semaphore_create(sem_port, &sem_exit, SYNC_POLICY_FIFO, 0);
  if (ret != KERN_SUCCESS) {
    fprintf(stderr, "==%s[%d]%s()==failed to create sem_exit\n", basename(__FILE__), __LINE__, __func__);
    sem_inited = -1;
    return NULL;
  }
  sem_inited = 1;
  semaphore_wait(sem_exit);
  return NULL;
}

static void once_init(void) {
  int r = 0;
  r = pthread_create(&sem_thread, NULL, sem_thread_routine, NULL);
  if (r) {
    fprintf(stderr, "==%s[%d]%s()==failed to create thread\n", basename(__FILE__), __LINE__, __func__);
    return;
  }
  while (sem_inited==0) {
    ;
  }
}
#endif

struct tsem_s {
#ifdef SEM_USE_PTHREAD
  pthread_mutex_t              lock;
  pthread_cond_t               cond;
  volatile int64_t             val;
#elif defined(SEM_USE_POSIX)
  size_t                       id;
  sem_t                       *sem;
#elif defined(SEM_USE_SEM)
  semaphore_t                  sem;
#else // SEM_USE_PTHREAD
  dispatch_semaphore_t         sem;
#endif // SEM_USE_PTHREAD

  volatile unsigned int        valid:1;
};

int tsem_init(tsem_t *sem, int pshared, unsigned int value) {
  // fprintf(stderr, "==%s[%d]%s():[%p]==creating\n", basename(__FILE__), __LINE__, __func__, sem);
  if (*sem) {
    fprintf(stderr, "==%s[%d]%s():[%p]==already initialized\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  struct tsem_s *p = (struct tsem_s*)calloc(1, sizeof(*p));
  if (!p) {
    fprintf(stderr, "==%s[%d]%s():[%p]==out of memory\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }

#ifdef SEM_USE_PTHREAD
  int r = pthread_mutex_init(&p->lock, NULL);
  do {
    if (r) break;
    r = pthread_cond_init(&p->cond, NULL);
    if (r) {
      pthread_mutex_destroy(&p->lock);
      break;
    }
    p->val = value;
  } while (0);
  if (r) {
    fprintf(stderr, "==%s[%d]%s():[%p]==not created\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
#elif defined(SEM_USE_POSIX)
  static size_t tick = 0;
  do {
    size_t id = atomic_add_fetch_64(&tick, 1);
    if (id==SEM_VALUE_MAX) {
      atomic_store_64(&tick, 0);
      id = 0;
    }
    char name[NAME_MAX-4];
    snprintf(name, sizeof(name), "/t%ld", id);
    p->sem = sem_open(name, O_CREAT|O_EXCL, pshared, value);
    p->id  = id;
    if (p->sem!=SEM_FAILED) break;
    int e = errno;
    if (e==EEXIST) continue;
    if (e==EINTR) continue;
    fprintf(stderr, "==%s[%d]%s():[%p]==not created[%d]%s\n", basename(__FILE__), __LINE__, __func__, sem, e, strerror(e));
    abort();
  } while (p->sem==SEM_FAILED);
#elif defined(SEM_USE_SEM)
  pthread_once(&sem_once, once_init);
  if (sem_inited!=1) {
    fprintf(stderr, "==%s[%d]%s():[%p]==internal resource init failed\n", basename(__FILE__), __LINE__, __func__, sem);
    errno = ENOMEM;
    return -1;
  }
  kern_return_t ret = semaphore_create(sem_port, &p->sem, SYNC_POLICY_FIFO, value);
  if (ret != KERN_SUCCESS) {
    fprintf(stderr, "==%s[%d]%s():[%p]==semophore_create failed\n", basename(__FILE__), __LINE__, __func__, sem);
    // we fail-fast here, because we have less-doc about semaphore_create for the moment
    abort();
  }
#else // SEM_USE_PTHREAD
  p->sem = dispatch_semaphore_create(value);
  if (p->sem == NULL) {
    fprintf(stderr, "==%s[%d]%s():[%p]==not created\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
#endif // SEM_USE_PTHREAD

  p->valid = 1;

  *sem = p;

  return 0;
}

int tsem_wait(tsem_t *sem) {
  if (!*sem) {
    fprintf(stderr, "==%s[%d]%s():[%p]==not initialized\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  struct tsem_s *p = *sem;
  if (!p->valid) {
    fprintf(stderr, "==%s[%d]%s():[%p]==already destroyed\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
#ifdef SEM_USE_PTHREAD
  if (pthread_mutex_lock(&p->lock)) {
    fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  p->val -= 1;
  if (p->val < 0) {
    if (pthread_cond_wait(&p->cond, &p->lock)) {
      fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", basename(__FILE__), __LINE__, __func__, sem);
      abort();
    }
  }
  if (pthread_mutex_unlock(&p->lock)) {
    fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  return 0;
#elif defined(SEM_USE_POSIX)
  return sem_wait(p->sem);
#elif defined(SEM_USE_SEM)
  return semaphore_wait(p->sem);
#else // SEM_USE_PTHREAD
  return dispatch_semaphore_wait(p->sem, DISPATCH_TIME_FOREVER);
#endif // SEM_USE_PTHREAD
}

int tsem_post(tsem_t *sem) {
  if (!*sem) {
    fprintf(stderr, "==%s[%d]%s():[%p]==not initialized\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  struct tsem_s *p = *sem;
  if (!p->valid) {
    fprintf(stderr, "==%s[%d]%s():[%p]==already destroyed\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
#ifdef SEM_USE_PTHREAD
  if (pthread_mutex_lock(&p->lock)) {
    fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  p->val += 1;
  if (p->val <= 0) {
    if (pthread_cond_signal(&p->cond)) {
      fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", basename(__FILE__), __LINE__, __func__, sem);
      abort();
    }
  }
  if (pthread_mutex_unlock(&p->lock)) {
    fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  return 0;
#elif defined(SEM_USE_POSIX)
  return sem_post(p->sem);
#elif defined(SEM_USE_SEM)
  return semaphore_signal(p->sem);
#else // SEM_USE_PTHREAD
  return dispatch_semaphore_signal(p->sem);
#endif // SEM_USE_PTHREAD
}

int tsem_destroy(tsem_t *sem) {
  // fprintf(stderr, "==%s[%d]%s():[%p]==destroying\n", basename(__FILE__), __LINE__, __func__, sem);
  if (!*sem) {
    // fprintf(stderr, "==%s[%d]%s():[%p]==not initialized\n", basename(__FILE__), __LINE__, __func__, sem);
    // abort();
    return 0;
  }
  struct tsem_s *p = *sem;
  if (!p->valid) {
    // fprintf(stderr, "==%s[%d]%s():[%p]==already destroyed\n", basename(__FILE__), __LINE__, __func__, sem);
    // abort();
    return 0;
  }
#ifdef SEM_USE_PTHREAD
  if (pthread_mutex_lock(&p->lock)) {
    fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  p->valid = 0;
  if (pthread_cond_destroy(&p->cond)) {
    fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  if (pthread_mutex_unlock(&p->lock)) {
    fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  if (pthread_mutex_destroy(&p->lock)) {
    fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", basename(__FILE__), __LINE__, __func__, sem);
    abort();
  }
#elif defined(SEM_USE_POSIX)
  char name[NAME_MAX-4];
  snprintf(name, sizeof(name), "/t%ld", p->id);
  int r = sem_unlink(name);
  if (r) {
    int e = errno;
    fprintf(stderr, "==%s[%d]%s():[%p]==unlink failed[%d]%s\n", basename(__FILE__), __LINE__, __func__, sem, e, strerror(e));
    abort();
  }
#elif defined(SEM_USE_SEM)
  semaphore_destroy(sem_port, p->sem);
#else // SEM_USE_PTHREAD
#endif // SEM_USE_PTHREAD

  p->valid = 0;
  free(p);

  *sem = NULL;
  return 0;
}

