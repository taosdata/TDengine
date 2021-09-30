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

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)

/*
 * windows implementation
 */


#include <Windows.h>
#include <Mmsystem.h>
#include <stdio.h>
#include <stdint.h>

#pragma warning( disable : 4244 )

typedef void (*win_timer_f)(int signo);

void WINAPI taosWinOnTimer(UINT wTimerID, UINT msg, DWORD_PTR dwUser, DWORD_PTR dwl, DWORD_PTR dw2) {
  win_timer_f callback = *((win_timer_f *)&dwUser);
  if (callback != NULL) {
    callback(0);
  }
}

static MMRESULT timerId;
int taosInitTimer(win_timer_f callback, int ms) {
  DWORD_PTR param = *((int64_t *) & callback);

  timerId = timeSetEvent(ms, 1, (LPTIMECALLBACK)taosWinOnTimer, param, TIME_PERIODIC);
  if (timerId == 0) {
    return -1;
  }
  return 0;
}

void taosUninitTimer() {
  timeKillEvent(timerId);
}

#elif defined(_TD_DARWIN_64)

/*
 * darwin implementation
 */

#include <sys/syscall.h>
#include <sys/event.h>

static void (*timer_callback)(int);
static int           timer_ms = 0;
static pthread_t     timer_thread;
static int           timer_kq = -1;
static volatile int  timer_stop = 0;

static void* timer_routine(void *arg) {
  (void)arg;
  setThreadName("timer");

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

/*
 * linux implementation
 */

#include <sys/syscall.h>

static void taosDeleteTimer(void *tharg) {
  timer_t *pTimer = tharg;
  timer_delete(*pTimer);
}

static pthread_t timerThread;
static timer_t         timerId;
static volatile bool stopTimer = false;
static void *taosProcessAlarmSignal(void *tharg) {
  // Block the signal
  sigset_t sigset;
  sigemptyset(&sigset);
  sigaddset(&sigset, SIGALRM);
  sigprocmask(SIG_BLOCK, &sigset, NULL);
  void (*callback)(int) = tharg;

  struct sigevent sevent = {{0}};

  setThreadName("tmr");

  #ifdef _ALPINE
    sevent.sigev_notify = SIGEV_THREAD;
    sevent.sigev_value.sival_int = syscall(__NR_gettid);
  #else
    sevent.sigev_notify = SIGEV_THREAD_ID;
    sevent._sigev_un._tid = syscall(__NR_gettid);
  #endif
  
  sevent.sigev_signo = SIGALRM;

  if (timer_create(CLOCK_REALTIME, &sevent, &timerId) == -1) {
    printf("Failed to create timer");
  }

  pthread_cleanup_push(taosDeleteTimer, &timerId);

  struct itimerspec ts;
  ts.it_value.tv_sec = 0;
  ts.it_value.tv_nsec = 1000000 * MSECONDS_PER_TICK;
  ts.it_interval.tv_sec = 0;
  ts.it_interval.tv_nsec = 1000000 * MSECONDS_PER_TICK;

  if (timer_settime(timerId, 0, &ts, NULL)) {
    printf("Failed to init timer");
    return NULL;
  }

  int signo;
  while (!stopTimer) {
    if (sigwait(&sigset, &signo)) {
      printf("Failed to wait signal: number %d", signo);
      continue;
    }
    /* printf("Signal handling: number %d ......\n", signo); */

    callback(0);
  }
  
  pthread_cleanup_pop(1);

  return NULL;
}

int taosInitTimer(void (*callback)(int), int ms) {
  pthread_attr_t tattr;
  pthread_attr_init(&tattr);
  int code = pthread_create(&timerThread, &tattr, taosProcessAlarmSignal, callback);
  pthread_attr_destroy(&tattr);
  if (code != 0) {
    printf("failed to create timer thread");
    return -1;
  } else {
    printf("timer thread:0x%08" PRIx64 " is created", taosGetPthreadId(timerThread));
  }

  return 0;
}

void taosUninitTimer() {
  stopTimer = true;

  printf("join timer thread:0x%08" PRIx64, taosGetPthreadId(timerThread));
  pthread_join(timerThread, NULL);
}

#endif
