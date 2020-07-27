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
#include "os.h"
#include "taosdef.h"
#include "tglobal.h"
#include "tconfig.h"
#include "ttimer.h"
#include "tulog.h"
#include "tutil.h"

int64_t tsosStr2int64(char *str) {
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
      uInfo("no net card was found, use lo:127.0.0.1 as default");
      strcpy(ip, "127.0.0.1");
      return 0;
    }
    return -1;
  }
}

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
  if (level == SOL_SOCKET && optname == SO_SNDBUF) {
    return 0;
  }

  if (level == SOL_SOCKET && optname == SO_RCVBUF) {
    return 0;
  }

  return setsockopt(socketfd, level, optname, optval, (socklen_t)optlen);
}

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

void taosGetSystemTimezone() {
  // get and set default timezone
  SGlobalCfg *cfg_timezone = taosGetConfigOption("timezone");
  if (cfg_timezone && cfg_timezone->cfgStatus < TAOS_CFG_CSTATUS_DEFAULT) {
    char *tz = getenv("TZ");
    if (tz == NULL || strlen(tz) == 0) {
      strcpy(tsTimezone, "not configured");
    }
    else {
      strcpy(tsTimezone, tz);
    }
    cfg_timezone->cfgStatus = TAOS_CFG_CSTATUS_DEFAULT;
    uInfo("timezone not configured, use default");
  }
}

void taosGetSystemLocale() {
  // get and set default locale
  SGlobalCfg *cfg_locale = taosGetConfigOption("locale");
  if (cfg_locale && cfg_locale->cfgStatus < TAOS_CFG_CSTATUS_DEFAULT) {
    char *locale = setlocale(LC_CTYPE, "chs");
    if (locale != NULL) {
      tstrncpy(tsLocale, locale, sizeof(tsLocale));
      cfg_locale->cfgStatus = TAOS_CFG_CSTATUS_DEFAULT;
      uInfo("locale not configured, set to default:%s", tsLocale);
    }
  }

  SGlobalCfg *cfg_charset = taosGetConfigOption("charset");
  if (cfg_charset && cfg_charset->cfgStatus < TAOS_CFG_CSTATUS_DEFAULT) {
    strcpy(tsCharset, "cp936");
    cfg_charset->cfgStatus = TAOS_CFG_CSTATUS_DEFAULT;
    uInfo("charset not configured, set to default:%s", tsCharset);
  }
}


void taosPrintOsInfo() {}

void taosKillSystem() {
  uError("function taosKillSystem, exit!");
  exit(0);
}

bool taosGetDisk() {
  return true;
}

void taosGetSystemInfo() {
  taosGetSystemTimezone();
  taosGetSystemLocale();
}

void *taosInitTcpClient(char *ip, uint16_t port, char *flabel, int num, void *fp, void *shandle) {
  uError("function taosInitTcpClient is not implemented in darwin system, exit!");
  exit(0);
}

void taosCloseTcpClientConnection(void *chandle) {
  uError("function taosCloseTcpClientConnection is not implemented in darwin system, exit!");
  exit(0);
}

void *taosOpenTcpClientConnection(void *shandle, void *thandle, char *ip, uint16_t port) {
  uError("function taosOpenTcpClientConnection is not implemented in darwin system, exit!");
  exit(0);
}

int taosSendTcpClientData(unsigned int ip, uint16_t port, char *data, int len, void *chandle) {
  uError("function taosSendTcpClientData is not implemented in darwin system, exit!");
  exit(0);
}

void taosCleanUpTcpClient(void *chandle) {
  uError("function taosCleanUpTcpClient is not implemented in darwin system, exit!");
  exit(0);
}

void taosCloseTcpServerConnection(void *chandle) {
  uError("function taosCloseTcpServerConnection is not implemented in darwin system, exit!");
  exit(0);
}

void taosCleanUpTcpServer(void *handle) {
  uError("function taosCleanUpTcpServer is not implemented in darwin system, exit!");
  exit(0);
}

void *taosInitTcpServer(char *ip, uint16_t port, char *label, int numOfThreads, void *fp, void *shandle) {
  uError("function taosInitTcpServer is not implemented in darwin system, exit!");
  exit(0);
}

int taosSendTcpServerData(unsigned int ip, uint16_t port, char *data, int len, void *chandle) {
  uError("function taosSendTcpServerData is not implemented in darwin system, exit!");
  exit(0);
}

void taosFreeMsgHdr(void *hdr) {
  uError("function taosFreeMsgHdr is not implemented in darwin system, exit!");
  exit(0);
}

int taosMsgHdrSize(void *hdr) {
  uError("function taosMsgHdrSize is not implemented in darwin system, exit!");
  exit(0);
}

void taosSendMsgHdr(void *hdr, int fd) {
  uError("function taosSendMsgHdr is not implemented in darwin system, exit!");
  exit(0);
}

void taosInitMsgHdr(void **hdr, void *dest, int maxPkts) {
  uError("function taosInitMsgHdr is not implemented in darwin system, exit!");
  exit(0);
}

void taosSetMsgHdrData(void *hdr, char *data, int dataLen) {
  uError("function taosSetMsgHdrData is not implemented in darwin system, exit!");
  exit(0);
}

bool taosSkipSocketCheck() {
  return true;
}

int tsem_init(dispatch_semaphore_t *sem, int pshared, unsigned int value) {
  *sem = dispatch_semaphore_create(value);
  if (*sem == NULL) {
    return -1;
  } else {
    return 0;
  }
}

int tsem_wait(dispatch_semaphore_t *sem) {
  dispatch_semaphore_wait(*sem, DISPATCH_TIME_FOREVER);
  return 0;
}

int tsem_post(dispatch_semaphore_t *sem) {
  dispatch_semaphore_signal(*sem);
  return 0;
}

int tsem_destroy(dispatch_semaphore_t *sem) {
  return 0;
}

int32_t __sync_val_load_32(int32_t *ptr) {
  return __atomic_load_n(ptr, __ATOMIC_ACQUIRE);
}

void __sync_val_restore_32(int32_t *ptr, int32_t newval) {
  __atomic_store_n(ptr, newval, __ATOMIC_RELEASE);
}

#define _SEND_FILE_STEP_ 1000

int fsendfile(FILE* out_file, FILE* in_file, int64_t* offset, int32_t count) {
  fseek(in_file, (int32_t)(*offset), 0);
  int writeLen = 0;
  uint8_t buffer[_SEND_FILE_STEP_] = { 0 };

  for (int len = 0; len < (count - _SEND_FILE_STEP_); len += _SEND_FILE_STEP_) {
    size_t rlen = fread(buffer, 1, _SEND_FILE_STEP_, in_file);
    if (rlen <= 0) {
      return writeLen;
    }
    else if (rlen < _SEND_FILE_STEP_) {
      fwrite(buffer, 1, rlen, out_file);
      return (int)(writeLen + rlen);
    }
    else {
      fwrite(buffer, 1, _SEND_FILE_STEP_, in_file);
      writeLen += _SEND_FILE_STEP_;
    }
  }

  int remain = count - writeLen;
  if (remain > 0) {
    size_t rlen = fread(buffer, 1, remain, in_file);
    if (rlen <= 0) {
      return writeLen;
    }
    else {
      fwrite(buffer, 1, remain, out_file);
      writeLen += remain;
    }
  }

  return writeLen;
}

void taosSetCoreDump() {}
