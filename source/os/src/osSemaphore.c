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

  strcpy(name, end);

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

int32_t tsem_timewait(tsem_t* sem, int64_t nanosecs) {
  struct timespec ts, rel;
  FILETIME ft_before, ft_after;
  int rc;

  rel.tv_sec = 0;
  rel.tv_nsec = nanosecs;

  GetSystemTimeAsFileTime(&ft_before);
  errno = 0;
  rc = sem_timedwait(&sem, pthread_win32_getabstime_np(&ts, &rel));

  /* This should have timed out */
  assert(errno == ETIMEDOUT);
  assert(rc != 0);
  GetSystemTimeAsFileTime(&ft_after);
  // We specified a non-zero wait. Time must advance.
  if (ft_before.dwLowDateTime == ft_after.dwLowDateTime && ft_before.dwHighDateTime == ft_after.dwHighDateTime)
    {
      printf("nanoseconds: %d, rc: %d, errno: %d. before filetime: %d, %d; after filetime: %d, %d\n",
          nanosecs, rc, errno,
          (int)ft_before.dwLowDateTime, (int)ft_before.dwHighDateTime,
          (int)ft_after.dwLowDateTime, (int)ft_after.dwHighDateTime);
      printf("time must advance during sem_timedwait.");
      return 1;
    }
  return 0;
}

#elif defined(_TD_DARWIN_64)

/*
 * darwin implementation
 */

#include <libproc.h>

// #define SEM_USE_PTHREAD
// #define SEM_USE_POSIX
#define SEM_USE_SEM

#ifdef SEM_USE_SEM
#include <mach/mach_error.h>
#include <mach/mach_init.h>
#include <mach/semaphore.h>
#include <mach/task.h>

static TdThread     sem_thread;
static TdThreadOnce sem_once;
static task_t       sem_port;
static volatile int sem_inited = 0;
static semaphore_t  sem_exit;

static void *sem_thread_routine(void *arg) {
  (void)arg;
  setThreadName("sem_thrd");

  sem_port = mach_task_self();
  kern_return_t ret = semaphore_create(sem_port, &sem_exit, SYNC_POLICY_FIFO, 0);
  if (ret != KERN_SUCCESS) {
    fprintf(stderr, "==%s[%d]%s()==failed to create sem_exit\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__);
    sem_inited = -1;
    return NULL;
  }
  sem_inited = 1;
  semaphore_wait(sem_exit);
  return NULL;
}

static void once_init(void) {
  int r = 0;
  r = taosThreadCreate(&sem_thread, NULL, sem_thread_routine, NULL);
  if (r) {
    fprintf(stderr, "==%s[%d]%s()==failed to create thread\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__);
    return;
  }
  while (sem_inited == 0) {
    ;
  }
}
#endif

struct tsem_s {
#ifdef SEM_USE_PTHREAD
  TdThreadMutex    lock;
  TdThreadCond     cond;
  volatile int64_t val;
#elif defined(SEM_USE_POSIX)
  size_t        id;
  sem_t        *sem;
#elif defined(SEM_USE_SEM)
  semaphore_t sem;
#else   // SEM_USE_PTHREAD
  dispatch_semaphore_t sem;
#endif  // SEM_USE_PTHREAD

  volatile unsigned int valid : 1;
};

int tsem_init(tsem_t *sem, int pshared, unsigned int value) {
  // fprintf(stderr, "==%s[%d]%s():[%p]==creating\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__, sem);
  if (*sem) {
    fprintf(stderr, "==%s[%d]%s():[%p]==already initialized\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__,
            sem);
    abort();
  }
  struct tsem_s *p = (struct tsem_s *)taosMemoryCalloc(1, sizeof(*p));
  if (!p) {
    fprintf(stderr, "==%s[%d]%s():[%p]==out of memory\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__, sem);
    abort();
  }

#ifdef SEM_USE_PTHREAD
  int r = taosThreadMutexInit(&p->lock, NULL);
  do {
    if (r) break;
    r = taosThreadCondInit(&p->cond, NULL);
    if (r) {
      taosThreadMutexDestroy(&p->lock);
      break;
    }
    p->val = value;
  } while (0);
  if (r) {
    fprintf(stderr, "==%s[%d]%s():[%p]==not created\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__, sem);
    abort();
  }
#elif defined(SEM_USE_POSIX)
  static size_t tick = 0;
  do {
    size_t id = atomic_add_fetch_64(&tick, 1);
    if (id == SEM_VALUE_MAX) {
      atomic_store_64(&tick, 0);
      id = 0;
    }
    char name[NAME_MAX - 4];
    snprintf(name, sizeof(name), "/t%ld", id);
    p->sem = sem_open(name, O_CREAT | O_EXCL, pshared, value);
    p->id = id;
    if (p->sem != SEM_FAILED) break;
    int e = errno;
    if (e == EEXIST) continue;
    if (e == EINTR) continue;
    fprintf(stderr, "==%s[%d]%s():[%p]==not created[%d]%s\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__, sem,
            e, strerror(e));
    abort();
  } while (p->sem == SEM_FAILED);
#elif defined(SEM_USE_SEM)
  taosThreadOnce(&sem_once, once_init);
  if (sem_inited != 1) {
    fprintf(stderr, "==%s[%d]%s():[%p]==internal resource init failed\n", taosDirEntryBaseName(__FILE__), __LINE__,
            __func__, sem);
    errno = ENOMEM;
    return -1;
  }
  kern_return_t ret = semaphore_create(sem_port, &p->sem, SYNC_POLICY_FIFO, value);
  if (ret != KERN_SUCCESS) {
    fprintf(stderr, "==%s[%d]%s():[%p]==semophore_create failed\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__,
            sem);
    // we fail-fast here, because we have less-doc about semaphore_create for the moment
    abort();
  }
#else   // SEM_USE_PTHREAD
  p->sem = dispatch_semaphore_create(value);
  if (p->sem == NULL) {
    fprintf(stderr, "==%s[%d]%s():[%p]==not created\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__, sem);
    abort();
  }
#endif  // SEM_USE_PTHREAD

  p->valid = 1;

  *sem = p;

  return 0;
}

int tsem_wait(tsem_t *sem) {
  if (!*sem) {
    fprintf(stderr, "==%s[%d]%s():[%p]==not initialized\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  struct tsem_s *p = *sem;
  if (!p->valid) {
    fprintf(stderr, "==%s[%d]%s():[%p]==already destroyed\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__, sem);
    abort();
  }
#ifdef SEM_USE_PTHREAD
  if (taosThreadMutexLock(&p->lock)) {
    fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__,
            sem);
    abort();
  }
  p->val -= 1;
  if (p->val < 0) {
    if (taosThreadCondWait(&p->cond, &p->lock)) {
      fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__,
              sem);
      abort();
    }
  }
  if (taosThreadMutexUnlock(&p->lock)) {
    fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__,
            sem);
    abort();
  }
  return 0;
#elif defined(SEM_USE_POSIX)
  return sem_wait(p->sem);
#elif defined(SEM_USE_SEM)
  return semaphore_wait(p->sem);
#else   // SEM_USE_PTHREAD
  return dispatch_semaphore_wait(p->sem, DISPATCH_TIME_FOREVER);
#endif  // SEM_USE_PTHREAD
}

int tsem_post(tsem_t *sem) {
  if (!*sem) {
    fprintf(stderr, "==%s[%d]%s():[%p]==not initialized\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__, sem);
    abort();
  }
  struct tsem_s *p = *sem;
  if (!p->valid) {
    fprintf(stderr, "==%s[%d]%s():[%p]==already destroyed\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__, sem);
    abort();
  }
#ifdef SEM_USE_PTHREAD
  if (taosThreadMutexLock(&p->lock)) {
    fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__,
            sem);
    abort();
  }
  p->val += 1;
  if (p->val <= 0) {
    if (taosThreadCondSignal(&p->cond)) {
      fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__,
              sem);
      abort();
    }
  }
  if (taosThreadMutexUnlock(&p->lock)) {
    fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__,
            sem);
    abort();
  }
  return 0;
#elif defined(SEM_USE_POSIX)
  return sem_post(p->sem);
#elif defined(SEM_USE_SEM)
  return semaphore_signal(p->sem);
#else   // SEM_USE_PTHREAD
  return dispatch_semaphore_signal(p->sem);
#endif  // SEM_USE_PTHREAD
}

int tsem_destroy(tsem_t *sem) {
  // fprintf(stderr, "==%s[%d]%s():[%p]==destroying\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__, sem);
  if (!*sem) {
    // fprintf(stderr, "==%s[%d]%s():[%p]==not initialized\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__, sem);
    // abort();
    return 0;
  }
  struct tsem_s *p = *sem;
  if (!p->valid) {
    // fprintf(stderr, "==%s[%d]%s():[%p]==already destroyed\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__,
    // sem); abort();
    return 0;
  }
#ifdef SEM_USE_PTHREAD
  if (taosThreadMutexLock(&p->lock)) {
    fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__,
            sem);
    abort();
  }
  p->valid = 0;
  if (taosThreadCondDestroy(&p->cond)) {
    fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__,
            sem);
    abort();
  }
  if (taosThreadMutexUnlock(&p->lock)) {
    fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__,
            sem);
    abort();
  }
  if (taosThreadMutexDestroy(&p->lock)) {
    fprintf(stderr, "==%s[%d]%s():[%p]==internal logic error\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__,
            sem);
    abort();
  }
#elif defined(SEM_USE_POSIX)
  char name[NAME_MAX - 4];
  snprintf(name, sizeof(name), "/t%ld", p->id);
  int r = sem_unlink(name);
  if (r) {
    int e = errno;
    fprintf(stderr, "==%s[%d]%s():[%p]==unlink failed[%d]%s\n", taosDirEntryBaseName(__FILE__), __LINE__, __func__, sem,
            e, strerror(e));
    abort();
  }
#elif defined(SEM_USE_SEM)
  semaphore_destroy(sem_port, p->sem);
#else   // SEM_USE_PTHREAD
#endif  // SEM_USE_PTHREAD

  p->valid = 0;
  taosMemoryFree(p);

  *sem = NULL;
  return 0;
}

bool taosCheckPthreadValid(TdThread thread) {
  uint64_t id = 0;
  int      r = TdThreadhreadid_np(thread, &id);
  return r ? false : true;
}

int64_t taosGetSelfPthreadId() {
  uint64_t id;
  TdThreadhreadid_np(0, &id);
  return (int64_t)id;
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
  if (name) strcpy(name, buf);
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
int32_t taosGetPId() { return getpid(); }

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

  strcpy(name, end);

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

int32_t tsem_timewait(tsem_t* sem, int64_t nanosecs) {
  int ret = 0;

  struct timespec tv = {
      .tv_sec = 0,
      .tv_nsec = nanosecs,
  };

  while ((ret = sem_timedwait(sem, &tv)) == -1 && errno == EINTR) continue;

  return ret;
}

#endif
