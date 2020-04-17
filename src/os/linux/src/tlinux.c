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

#include <errno.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>
#include <sys/utsname.h>

#include "tglobalcfg.h"
#include "tlog.h"
#include "taosdef.h"
#include "tutil.h"
#include "ttimer.h"

char configDir[TSDB_FILENAME_LEN] = "/etc/taos";
char tsVnodeDir[TSDB_FILENAME_LEN] = {0};
char tsDnodeDir[TSDB_FILENAME_LEN] = {0};
char tsMnodeDir[TSDB_FILENAME_LEN] = {0};
char dataDir[TSDB_FILENAME_LEN] = "/var/lib/taos";
char logDir[TSDB_FILENAME_LEN] = "/var/log/taos";
char scriptDir[TSDB_FILENAME_LEN] = "/etc/taos";
char osName[] = "Linux";

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

void taosResetPthread(pthread_t *thread) { *thread = 0; }

int64_t taosGetPthreadId() { return (int64_t)pthread_self(); }

/*
* Function to get the private ip address of current machine. If get IP
* successfully, return 0, else, return -1. The return values is ip.
*
* Use:
* if (taosGetPrivateIp(ip) != 0) {
*     perror("Fail to get private IP address\n");
*     exit(EXIT_FAILURE);
* }
*/
int taosGetPrivateIp(char *const ip) {
  bool hasLoCard = false;

  struct ifaddrs *ifaddr, *ifa;
  int             family, s;
  char            host[NI_MAXHOST];

  if (getifaddrs(&ifaddr) == -1) {
    return -1;
  }

  /* Walk through linked list, maintaining head pointer so we can free list later */
  int flag = 0;
  for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr == NULL) continue;

    family = ifa->ifa_addr->sa_family;
    if (strcmp("lo", ifa->ifa_name) == 0) {
      hasLoCard = true;
      continue;
    }

    if (family == AF_INET) {
      /* printf("%-8s", ifa->ifa_name); */
      s = getnameinfo(ifa->ifa_addr, (family == AF_INET) ? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6),
                      host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST);
      if (s != 0) {
        freeifaddrs(ifaddr);
        return -1;
      }

      strcpy(ip, host);
      flag = 1;
      break;
    }
  }

  freeifaddrs(ifaddr);
  if (flag) {
    return 0;
  } else {
    if (hasLoCard) {
      pPrint("no net card was found, use lo:127.0.0.1 as default");
      strcpy(ip, "127.0.0.1");
      return 0;
    }
    return -1;
  }
}

int taosSetNonblocking(int sock, int on) {
  int flags = 0;
  if ((flags = fcntl(sock, F_GETFL, 0)) < 0) {
    pError("fcntl(F_GETFL) error: %d (%s)\n", errno, strerror(errno));
    return 1;
  }

  if (on)
    flags |= O_NONBLOCK;
  else
    flags &= ~O_NONBLOCK;

  if ((flags = fcntl(sock, F_SETFL, flags)) < 0) {
    pError("fcntl(F_SETFL) error: %d (%s)\n", errno, strerror(errno));
    return 1;
  }

  return 0;
}

int taosSetSockOpt(int socketfd, int level, int optname, void *optval, int optlen) {
  return setsockopt(socketfd, level, optname, optval, (socklen_t)optlen);
}

int taosOpenUDClientSocket(char *ip, uint16_t port) {
  int                sockFd = 0;
  struct sockaddr_un serverAddr;
  int                ret;
  char               name[128];
  sprintf(name, "%s.%hu", ip, port);

  sockFd = socket(AF_UNIX, SOCK_STREAM, 0);

  if (sockFd < 0) {
    pError("failed to open the UD socket:%s, reason:%s", name, strerror(errno));
    return -1;
  }

  memset((char *)&serverAddr, 0, sizeof(serverAddr));
  serverAddr.sun_family = AF_UNIX;
  strcpy(serverAddr.sun_path + 1, name);

  ret = connect(sockFd, (struct sockaddr *)&serverAddr, sizeof(serverAddr));

  if (ret != 0) {
    pError("failed to connect UD socket, name:%d, reason: %s", name, strerror(errno));
    sockFd = -1;
  }

  return sockFd;
}

int taosOpenUDServerSocket(char *ip, uint16_t port) {
  struct sockaddr_un serverAdd;
  int                sockFd;
  char               name[128];

  pTrace("open ud socket:%s", name);
  sprintf(name, "%s.%hu", ip, port);

  bzero((char *)&serverAdd, sizeof(serverAdd));
  serverAdd.sun_family = AF_UNIX;
  strcpy(serverAdd.sun_path + 1, name);
  unlink(name);

  if ((sockFd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
    pError("failed to open UD socket:%s, reason:%s", name, strerror(errno));
    return -1;
  }

  /* bind socket to server address */
  if (bind(sockFd, (struct sockaddr *)&serverAdd, sizeof(serverAdd)) < 0) {
    pError("bind socket:%s failed, reason:%s", name, strerror(errno));
    tclose(sockFd);
    return -1;
  }

  if (listen(sockFd, 10) < 0) {
    pError("listen socket:%s failed, reason:%s", name, strerror(errno));
    return -1;
  }

  return sockFd;
}

static void taosDeleteTimer(void *tharg) {
  timer_t *pTimer = tharg;
  timer_delete(*pTimer);
}

void *taosProcessAlarmSignal(void *tharg) {
  // Block the signal
  sigset_t sigset;
  sigemptyset(&sigset);
  sigaddset(&sigset, SIGALRM);
  sigprocmask(SIG_BLOCK, &sigset, NULL);
  void (*callback)(int) = tharg;

  static timer_t         timerId;
  struct sigevent sevent = {0};

  #ifdef _ALPINE
    sevent.sigev_notify = SIGEV_THREAD;
    sevent.sigev_value.sival_int = syscall(__NR_gettid);
  #else
    sevent.sigev_notify = SIGEV_THREAD_ID;
    sevent._sigev_un._tid = syscall(__NR_gettid);
  #endif
  
  sevent.sigev_signo = SIGALRM;

  if (timer_create(CLOCK_REALTIME, &sevent, &timerId) == -1) {
    tmrError("Failed to create timer");
  }

  pthread_cleanup_push(taosDeleteTimer, &timerId);

  struct itimerspec ts;
  ts.it_value.tv_sec = 0;
  ts.it_value.tv_nsec = 1000000 * MSECONDS_PER_TICK;
  ts.it_interval.tv_sec = 0;
  ts.it_interval.tv_nsec = 1000000 * MSECONDS_PER_TICK;

  if (timer_settime(timerId, 0, &ts, NULL)) {
    tmrError("Failed to init timer");
    return NULL;
  }

  int signo;
  while (1) {
    if (sigwait(&sigset, &signo)) {
      tmrError("Failed to wait signal: number %d", signo);
      continue;
    }
    /* printf("Signal handling: number %d ......\n", signo); */

    callback(0);
  }
  
  pthread_cleanup_pop(1);

  return NULL;
}

static pthread_t timerThread;

int taosInitTimer(void (*callback)(int), int ms) {
  pthread_attr_t tattr;
  pthread_attr_init(&tattr);
  int code = pthread_create(&timerThread, &tattr, taosProcessAlarmSignal, callback);
  pthread_attr_destroy(&tattr);
  if (code != 0) {
    tmrError("failed to create timer thread");
    return -1;
  }
  return 0;
}

void taosUninitTimer() {
  pthread_cancel(timerThread);
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

bool taosSkipSocketCheck() {
  struct utsname buf;
  if (uname(&buf)) {
    pPrint("can't fetch os info");
    return false;
  }

  if (strstr(buf.release, "Microsoft") != 0) {
    pPrint("using WSLv1");
    return true;
  }

  return false;
}

void taosBlockSIGPIPE() {
  sigset_t signal_mask;
  sigemptyset(&signal_mask);
  sigaddset(&signal_mask, SIGPIPE);
  int rc = pthread_sigmask(SIG_BLOCK, &signal_mask, NULL);
  if (rc != 0) {
    pError("failed to block SIGPIPE");
  }
}
