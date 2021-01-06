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

#ifndef TAOS_OS_FUNC_TIMER

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

#endif

#ifndef TAOS_OS_FUNC_TIMER_SLEEP
/*
  to make taosMsleep work,
   signal SIGALRM shall be blocked in the calling thread,

  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGALRM);
  pthread_sigmask(SIG_BLOCK, &set, NULL);
*/
void taosMsleep(int mseconds) {
#if 1
  usleep(mseconds * 1000);
#else
  struct timeval timeout;
  int            seconds, useconds;

  seconds = mseconds / 1000;
  useconds = (mseconds % 1000) * 1000;
  timeout.tv_sec = seconds;
  timeout.tv_usec = useconds;

  /* sigset_t set; */
  /* sigemptyset(&set); */
  /* sigaddset(&set, SIGALRM); */
  /* pthread_sigmask(SIG_BLOCK, &set, NULL); */

  select(0, NULL, NULL, NULL, &timeout);

/* pthread_sigmask(SIG_UNBLOCK, &set, NULL); */
#endif
}

#endif