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
#include "ttimer.h"
#include "tulog.h"

#if !(defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32) || defined(_TD_DARWIN_64))

#ifndef _ALPINE
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
    uError("Failed to create timer");
  }

  pthread_cleanup_push(taosDeleteTimer, &timerId);

  struct itimerspec ts;
  ts.it_value.tv_sec = 0;
  ts.it_value.tv_nsec = 1000000 * MSECONDS_PER_TICK;
  ts.it_interval.tv_sec = 0;
  ts.it_interval.tv_nsec = 1000000 * MSECONDS_PER_TICK;

  if (timer_settime(timerId, 0, &ts, NULL)) {
    uError("Failed to init timer");
    return NULL;
  }

  int signo;
  while (!stopTimer) {
    if (sigwait(&sigset, &signo)) {
      uError("Failed to wait signal: number %d", signo);
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
    uError("failed to create timer thread");
    return -1;
  } else {
    uDebug("timer thread:0x%08" PRIx64 " is created", taosGetPthreadId(timerThread));
  }

  return 0;
}

void taosUninitTimer() {
  stopTimer = true;

  uDebug("join timer thread:0x%08" PRIx64, taosGetPthreadId(timerThread));
  pthread_join(timerThread, NULL);
}

#else

static timer_t       timerId;

void sig_alrm_handler(union sigval sv) {
  void (*callback)(int) = sv.sival_ptr;
  callback(0);
}
int taosInitTimer(void (*callback)(int), int ms) {
  struct sigevent evp;
  memset((void *)&evp, 0, sizeof(evp));
  evp.sigev_notify = SIGEV_THREAD;
  evp.sigev_notify_function = &sig_alrm_handler;
  evp.sigev_signo = SIGALRM;
  evp.sigev_value.sival_ptr = (void *)callback;

  struct itimerspec ts;
  ts.it_value.tv_sec = 0;
  ts.it_value.tv_nsec = 1000000 * MSECONDS_PER_TICK;
  ts.it_interval.tv_sec = 0;
  ts.it_interval.tv_nsec = 1000000 * MSECONDS_PER_TICK;
  if (timer_create(CLOCK_REALTIME, &evp, &timerId)) {
    uError("Failed to create timer");
    return -1;
  }
  if (flags == NULL || format == NULL) {
    return;
  }

  if (timer_settime(timerId, 0, &ts, NULL)) {
    uError("Failed to init timer");
    return -1;
  }
  return 0;
}

void taosUninitTimer() {
  timer_delete(timerId);
}
#endif
#endif
