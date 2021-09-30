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
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <netinet/udp.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

void taosShutDownSocketRD(SOCKET fd) {
#ifdef WINDOWS
  closesocket(fd);
#elif __APPLE__
  close(fd);
#else
  shutdown(fd, SHUT_RD);
#endif
}

void taosShutDownSocketWR(SOCKET fd) {
#ifdef WINDOWS
  closesocket(fd);
#elif __APPLE__
  close(fd);
#else
  shutdown(fd, SHUT_WR);
#endif
}

#if !(defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32))

int32_t taosSetNonblocking(SOCKET sock, int32_t on) {
  int32_t flags = 0;
  if ((flags = fcntl(sock, F_GETFL, 0)) < 0) {
    printf("fcntl(F_GETFL) error: %d (%s)\n", errno, strerror(errno));
    return 1;
  }

  if (on)
    flags |= O_NONBLOCK;
  else
    flags &= ~O_NONBLOCK;

  if ((flags = fcntl(sock, F_SETFL, flags)) < 0) {
    printf("fcntl(F_SETFL) error: %d (%s)\n", errno, strerror(errno));
    return 1;
  }

  return 0;
}

void taosIgnSIGPIPE() { signal(SIGPIPE, SIG_IGN); }

void taosBlockSIGPIPE() {
  sigset_t signal_mask;
  sigemptyset(&signal_mask);
  sigaddset(&signal_mask, SIGPIPE);
  int32_t rc = pthread_sigmask(SIG_BLOCK, &signal_mask, NULL);
  if (rc != 0) {
    printf("failed to block SIGPIPE");
  }
}

void taosSetMaskSIGPIPE() {
  sigset_t signal_mask;
  sigemptyset(&signal_mask);
  sigaddset(&signal_mask, SIGPIPE);
  int32_t rc = pthread_sigmask(SIG_SETMASK, &signal_mask, NULL);
  if (rc != 0) {
    printf("failed to setmask SIGPIPE");
  }
}

#endif

#if !(defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32) || defined(_TD_DARWIN_32))

int32_t taosSetSockOpt(SOCKET socketfd, int32_t level, int32_t optname, void *optval, int32_t optlen) {
  return setsockopt(socketfd, level, optname, optval, (socklen_t)optlen);
}

int32_t taosGetSockOpt(SOCKET socketfd, int32_t level, int32_t optname, void *optval, int32_t *optlen) {
  return getsockopt(socketfd, level, optname, optval, (socklen_t *)optlen);
}

#endif

#if !((defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)) && defined(_MSC_VER))

uint32_t taosInetAddr(char *ipAddr) { return inet_addr(ipAddr); }

const char *taosInetNtoa(struct in_addr ipInt) { return inet_ntoa(ipInt); }

#endif

#if defined(_TD_DARWIN_64)

/*
 * darwin implementation
 */

int taosSetSockOpt(SOCKET socketfd, int level, int optname, void *optval, int optlen) {
  if (level == SOL_SOCKET && optname == SO_SNDBUF) {
    return 0;
  }

  if (level == SOL_SOCKET && optname == SO_RCVBUF) {
    return 0;
  }

  return setsockopt(socketfd, level, optname, optval, (socklen_t)optlen);
}
#endif

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)

/*
 * windows implementation
 */

#include <IPHlpApi.h>
#include <WS2tcpip.h>
#include <stdio.h>
#include <string.h>
#include <tchar.h>
#include <winsock2.h>
#include <ws2def.h>

void taosWinSocketInit() {
  static char flag = 0;
  if (flag == 0) {
    WORD    wVersionRequested;
    WSADATA wsaData;
    wVersionRequested = MAKEWORD(1, 1);
    if (WSAStartup(wVersionRequested, &wsaData) == 0) {
      flag = 1;
    }
  }
}

int32_t taosSetNonblocking(SOCKET sock, int32_t on) {
  u_long mode;
  if (on) {
    mode = 1;
    ioctlsocket(sock, FIONBIO, &mode);
  } else {
    mode = 0;
    ioctlsocket(sock, FIONBIO, &mode);
  }
  return 0;
}

void taosIgnSIGPIPE() {}
void taosBlockSIGPIPE() {}
void taosSetMaskSIGPIPE() {}

int32_t taosSetSockOpt(SOCKET socketfd, int32_t level, int32_t optname, void *optval, int32_t optlen) {
  if (level == SOL_SOCKET && optname == TCP_KEEPCNT) {
    return 0;
  }

  if (level == SOL_TCP && optname == TCP_KEEPIDLE) {
    return 0;
  }

  if (level == SOL_TCP && optname == TCP_KEEPINTVL) {
    return 0;
  }

  if (level == SOL_TCP && optname == TCP_KEEPCNT) {
    return 0;
  }

  return setsockopt(socketfd, level, optname, optval, optlen);
}

#ifdef _MSC_VER
//#if _MSC_VER >= 1900

uint32_t taosInetAddr(char *ipAddr) {
  uint32_t value;
  int32_t  ret = inet_pton(AF_INET, ipAddr, &value);
  if (ret <= 0) {
    return INADDR_NONE;
  } else {
    return value;
  }
}

const char *taosInetNtoa(struct in_addr ipInt) {
  // not thread safe, only for debug usage while print log
  static char tmpDstStr[16];
  return inet_ntop(AF_INET, &ipInt, tmpDstStr, INET6_ADDRSTRLEN);
}

//#endif
#endif

#if defined(_TD_GO_DLL_)

uint64_t htonll(uint64_t val) { return (((uint64_t)htonl(val)) << 32) + htonl(val >> 32); }

#endif

#endif