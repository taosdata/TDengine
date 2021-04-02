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

#if 1
#include <sys/event.h>

static void (*timer_callback)(int);
static int           timer_ms = 0;
static pthread_t     timer_thread;
static int           timer_kq = -1;
static volatile int  timer_stop = 0;

static void* timer_routine(void *arg) {
  (void)arg;

  int r = 0;
  struct timespec to = {0};
  to.tv_sec    = timer_ms / 1000;
  to.tv_nsec   = (timer_ms % 1000) * 1000000;
  while (!timer_stop) {
    struct kevent64_s kev[10] = {0};
    r = kevent64(timer_kq, NULL, 0, kev, sizeof(kev)/sizeof(kev[0]), 0, &to);
    if (r!=0) {
      fprintf(stderr, "==%s[%d]%s()==kevent64 failed\n", basename(__FILE__), __LINE__, __func__);
      abort();
    }
    timer_callback(SIGALRM); // just mock
  }

  return NULL;
}

int taosInitTimer(void (*callback)(int), int ms) {
  int r = 0;
  timer_ms       = ms;
  timer_callback = callback;

  timer_kq = kqueue();
  if (timer_kq==-1) {
    fprintf(stderr, "==%s[%d]%s()==failed to create timer kq\n", basename(__FILE__), __LINE__, __func__);
    // since no caller of this func checks the return value for the moment
    abort();
  }

  r = pthread_create(&timer_thread, NULL, timer_routine, NULL);
  if (r) {
    fprintf(stderr, "==%s[%d]%s()==failed to create timer thread\n", basename(__FILE__), __LINE__, __func__);
    // since no caller of this func checks the return value for the moment
    abort();
  }
  return 0;
}

void taosUninitTimer() {
  int r = 0;
  timer_stop = 1;
  r = pthread_join(timer_thread, NULL);
  if (r) {
    fprintf(stderr, "==%s[%d]%s()==failed to join timer thread\n", basename(__FILE__), __LINE__, __func__);
    // since no caller of this func checks the return value for the moment
    abort();
  }
  close(timer_kq);
  timer_kq = -1;
}

void taos_block_sigalrm(void) {
  // we don't know if there's any specific API for SIGALRM to deliver to specific thread
  // this implementation relies on kqueue rather than SIGALRM
}
#else
int taosInitTimer(void (*callback)(int), int ms) {
  signal(SIGALRM, callback);

  struct itimerval tv;
  tv.it_interval.tv_sec = 0;  /* my timer resolution */
  tv.it_interval.tv_usec = 1000 * ms;  // resolution is in msecond
  tv.it_value = tv.it_interval;

  setitimer(ITIMER_REAL, &tv, NULL);

  return 0;
}

void taosUninitTimer() {
  struct itimerval tv = { 0 };
  setitimer(ITIMER_REAL, &tv, NULL);
}

void taos_block_sigalrm(void) {
  // since SIGALRM has been used
  // consideration: any better solution?
  static __thread int already_set = 0;
  if (!already_set) {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL);
    already_set = 1;
  }
}
#endif

