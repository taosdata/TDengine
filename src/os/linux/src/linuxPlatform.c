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
#include "taosdef.h"
#include "tglobal.h"
#include "ttimer.h"
#include "tulog.h"
#include "tutil.h"

int64_t str2int64(char *str) {
  char *endptr = NULL;
  return strtoll(str, &endptr, 10);
}

/*
  to make taosMsleep work,
   signal SIGALRM shall be blocked in the calling thread,

  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGALRM);
  pthread_sigmask(SIG_BLOCK, &set, NULL);
*/
void taosMsleep(int mseconds) {
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
}

bool taosCheckPthreadValid(pthread_t thread) { return thread != 0; }

int64_t taosGetPthreadId() { return (int64_t)pthread_self(); }

int taosSetNonblocking(int sock, int on) {
  int flags = 0;
  if ((flags = fcntl(sock, F_GETFL, 0)) < 0) {
    uError("fcntl(F_GETFL) error: %d (%s)\n", errno, strerror(errno));
    return 1;
  }

  if (on)
    flags |= O_NONBLOCK;
  else
    flags &= ~O_NONBLOCK;

  if ((flags = fcntl(sock, F_SETFL, flags)) < 0) {
    uError("fcntl(F_SETFL) error: %d (%s)\n", errno, strerror(errno));
    return 1;
  }

  return 0;
}

int taosSetSockOpt(int socketfd, int level, int optname, void *optval, int optlen) {
  return setsockopt(socketfd, level, optname, optval, (socklen_t)optlen);
}
static void taosDeleteTimer(void *tharg) {
  timer_t *pTimer = tharg;
  timer_delete(*pTimer);
}

static pthread_t timerThread;
static timer_t         timerId;
static volatile bool stopTimer = false;

void *taosProcessAlarmSignal(void *tharg) {
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
  }
  return 0;
}

void taosUninitTimer() {
  stopTimer = true;
  pthread_join(timerThread, NULL);
}

ssize_t tread(int fd, void *buf, size_t count) {
  size_t  leftbytes = count;
  ssize_t readbytes;
  char *  tbuf = (char *)buf;

  while (leftbytes > 0) {
    readbytes = read(fd, (void *)tbuf, leftbytes);
    if (readbytes < 0) {
      if (errno == EINTR) {
        continue;
      } else {
        return -1;
      }
    } else if (readbytes == 0) {
      return (ssize_t)(count - leftbytes);
    }

    leftbytes -= readbytes;
    tbuf += readbytes;
  }

  return (ssize_t)count;
}

ssize_t tsendfile(int dfd, int sfd, off_t *offset, size_t size) {
  size_t  leftbytes = size;
  ssize_t sentbytes;

  while (leftbytes > 0) {
    /*
     * TODO : Think to check if file is larger than 1GB
     */
    //if (leftbytes > 1000000000) leftbytes = 1000000000;
    sentbytes = sendfile(dfd, sfd, offset, leftbytes);
    if (sentbytes == -1) {
      if (errno == EINTR) {
        continue;
      }
      else {
        return -1;
      }
    } else if (sentbytes == 0) {
      return (ssize_t)(size - leftbytes);
    }

    leftbytes -= sentbytes;
  }

  return size;
}

ssize_t twrite(int fd, void *buf, size_t n) {
  size_t nleft = n; 
  ssize_t nwritten = 0;
  char *tbuf = (char *)buf;

  while (nleft > 0) {
    nwritten = write(fd, (void *)tbuf, nleft);
    if (nwritten < 0) {
      if (errno == EINTR) {
        continue;
      }
      return -1;
    }
    nleft -= nwritten;
    tbuf += nwritten;
  }

  return n;
}

void taosBlockSIGPIPE() {
  sigset_t signal_mask;
  sigemptyset(&signal_mask);
  sigaddset(&signal_mask, SIGPIPE);
  int rc = pthread_sigmask(SIG_BLOCK, &signal_mask, NULL);
  if (rc != 0) {
    uError("failed to block SIGPIPE");
  }
}

int tSystem(const char * cmd) 
{ 
  FILE * fp; 
  int res; 
  char buf[1024]; 
  if (cmd == NULL) { 
    uError("tSystem cmd is NULL!\n");
    return -1;
  } 
  
  if ((fp = popen(cmd, "r") ) == NULL) { 
    uError("popen cmd:%s error: %s/n", cmd, strerror(errno)); 
    return -1; 
  } else {
    while(fgets(buf, sizeof(buf), fp))  { 
      uDebug("popen result:%s", buf); 
    } 

    if ((res = pclose(fp)) == -1) { 
      uError("close popen file pointer fp error!\n");
    } else { 
      uDebug("popen res is :%d\n", res);
    } 

    return res;
  }
}

