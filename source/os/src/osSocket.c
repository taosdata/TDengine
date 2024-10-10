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
#define ALLOW_FORBID_FUNC
#include "os.h"

#if defined(WINDOWS)
#include <IPHlpApi.h>
#include <WS2tcpip.h>
#include <Winsock2.h>
#include <stdio.h>
#include <string.h>
#include <tchar.h>
#include <winbase.h>
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <netinet/udp.h>
#include <sys/socket.h>
#include <unistd.h>

#if defined(DARWIN)
#include <dispatch/dispatch.h>
#include "osEok.h"
#else
#include <sys/epoll.h>
#endif
#endif

#ifndef INVALID_SOCKET
#define INVALID_SOCKET -1
#endif

typedef struct TdSocket {
#if SOCKET_WITH_LOCK
  TdThreadRwlock rwlock;
#endif
  int      refId;
  SocketFd fd;
} * TdSocketPtr, TdSocket;

typedef struct TdSocketServer {
#if SOCKET_WITH_LOCK
  TdThreadRwlock rwlock;
#endif
  int      refId;
  SocketFd fd;
} * TdSocketServerPtr, TdSocketServer;

typedef struct TdEpoll {
#if SOCKET_WITH_LOCK
  TdThreadRwlock rwlock;
#endif
  int     refId;
  EpollFd fd;
} * TdEpollPtr, TdEpoll;

int32_t taosCloseSocketNoCheck1(SocketFd fd) {
#ifdef WINDOWS
  int ret = closesocket(fd);
  if (ret == SOCKET_ERROR) {
    int errorCode = WSAGetLastError();
    return terrno = TAOS_SYSTEM_WINSOCKET_ERROR(errorCode);
  }
  return 0;
#else
  int32_t code = close(fd);
  if (-1 == code) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return terrno;
  }
  return code;
#endif
}

int32_t taosCloseSocket(TdSocketPtr *ppSocket) {
  int32_t code;
  if (ppSocket == NULL || *ppSocket == NULL || (*ppSocket)->fd < 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  code = taosCloseSocketNoCheck1((*ppSocket)->fd);
  (*ppSocket)->fd = -1;
  taosMemoryFree(*ppSocket);

  return code;
}

int32_t taosSetSockOpt(TdSocketPtr pSocket, int32_t level, int32_t optname, void *optval, int32_t optlen) {
  if (pSocket == NULL || pSocket->fd < 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

#ifdef WINDOWS
#ifdef TCP_KEEPCNT
  if (level == SOL_SOCKET && optname == TCP_KEEPCNT) {
    return 0;
  }
#endif

#ifdef TCP_KEEPIDLE
  if (level == SOL_TCP && optname == TCP_KEEPIDLE) {
    return 0;
  }
#endif

#ifdef TCP_KEEPINTVL
  if (level == SOL_TCP && optname == TCP_KEEPINTVL) {
    return 0;
  }
#endif

#ifdef TCP_KEEPCNT
  if (level == SOL_TCP && optname == TCP_KEEPCNT) {
    return 0;
  }
#endif

  int ret = setsockopt(pSocket->fd, level, optname, optval, optlen);
  if (ret == SOCKET_ERROR) {
    int errorCode = WSAGetLastError();
    return terrno = TAOS_SYSTEM_WINSOCKET_ERROR(errorCode);
  }
#else
  int32_t code = setsockopt(pSocket->fd, level, optname, optval, (int)optlen);
  if (-1 == code) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return terrno;
  }
  return 0;
#endif
}

const char *taosInetNtoa(struct in_addr ipInt, char *dstStr, int32_t len) {
  const char *r = inet_ntop(AF_INET, &ipInt, dstStr, len);
  if (NULL == r) {
    terrno = TAOS_SYSTEM_ERROR(errno);
  }

  return r;
}

#ifndef SIGPIPE
#define SIGPIPE EPIPE
#endif

#define TCP_CONN_TIMEOUT 3000  // conn timeout

bool taosValidIpAndPort(uint32_t ip, uint16_t port) {
  struct sockaddr_in serverAdd;
  SocketFd           fd;
  int32_t            reuse;
  int32_t            code = 0;

  // printf("open tcp server socket:0x%x:%hu", ip, port);

  bzero((char *)&serverAdd, sizeof(serverAdd));
  serverAdd.sin_family = AF_INET;
#ifdef WINDOWS
  serverAdd.sin_addr.s_addr = INADDR_ANY;
#else
  serverAdd.sin_addr.s_addr = ip;
#endif
  serverAdd.sin_port = (uint16_t)htons(port);

  fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (-1 == fd) {  // exception
    terrno = TAOS_SYSTEM_ERROR(errno);
    return false;
  }

  TdSocketPtr pSocket = (TdSocketPtr)taosMemoryMalloc(sizeof(TdSocket));
  if (pSocket == NULL) {
    TAOS_SKIP_ERROR(taosCloseSocketNoCheck1(fd));
    return false;
  }
  pSocket->refId = 0;
  pSocket->fd = fd;

  /* set REUSEADDR option, so the portnumber can be re-used */
  reuse = 1;
  if (taosSetSockOpt(pSocket, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse, sizeof(reuse)) < 0) {
    TAOS_SKIP_ERROR(taosCloseSocket(&pSocket));
    return false;
  }

  /* bind socket to server address */
  if (-1 == bind(pSocket->fd, (struct sockaddr *)&serverAdd, sizeof(serverAdd))) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    TAOS_SKIP_ERROR(taosCloseSocket(&pSocket));
    return false;
  }

  TAOS_SKIP_ERROR(taosCloseSocket(&pSocket));

  return true;
}

int32_t taosBlockSIGPIPE() {
#ifdef WINDOWS
  return 0;
#else
  sigset_t signal_mask;
  (void)sigemptyset(&signal_mask);
  (void)sigaddset(&signal_mask, SIGPIPE);
  int32_t rc = pthread_sigmask(SIG_BLOCK, &signal_mask, NULL);
  if (rc != 0) {
    terrno = TAOS_SYSTEM_ERROR(rc);
    return terrno;
  }

  return 0;
#endif
}

int32_t taosGetIpv4FromFqdn(const char *fqdn, uint32_t *ip) {
#ifdef WINDOWS
  // Initialize Winsock
  WSADATA wsaData;
  int     iResult;
  iResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
  if (iResult != 0) {
    return TAOS_SYSTEM_WINSOCKET_ERROR(WSAGetLastError());
  }
#endif

#if defined(LINUX)
  struct addrinfo hints = {0};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  struct addrinfo *result = NULL;
  bool             inRetry = false;

  while (true) {
    int32_t ret = getaddrinfo(fqdn, NULL, &hints, &result);
    if (ret) {
      if (EAI_AGAIN == ret && !inRetry) {
        inRetry = true;
        continue;
      } else if (EAI_SYSTEM == ret) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        return terrno;
      }

      terrno = TAOS_SYSTEM_ERROR(errno);
      return terrno;
    }

    struct sockaddr    *sa = result->ai_addr;
    struct sockaddr_in *si = (struct sockaddr_in *)sa;
    struct in_addr      ia = si->sin_addr;

    *ip = ia.s_addr;

    freeaddrinfo(result);

    return 0;
  }
#else
  struct addrinfo hints = {0};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  struct addrinfo *result = NULL;

  int32_t ret = getaddrinfo(fqdn, NULL, &hints, &result);
  if (result) {
    struct sockaddr    *sa = result->ai_addr;
    struct sockaddr_in *si = (struct sockaddr_in *)sa;
    struct in_addr      ia = si->sin_addr;
    *ip = ia.s_addr;
    freeaddrinfo(result);
    return 0;
  } else {
#ifdef EAI_SYSTEM
    if (ret == EAI_SYSTEM) {
      // printf("failed to get the ip address, fqdn:%s, errno:%d, since:%s", fqdn, errno, strerror(errno));
    } else {
      // printf("failed to get the ip address, fqdn:%s, ret:%d, since:%s", fqdn, ret, gai_strerror(ret));
    }
#else
    // printf("failed to get the ip address, fqdn:%s, ret:%d, since:%s", fqdn, ret, gai_strerror(ret));
#endif

    *ip = 0xFFFFFFFF;
    return TSDB_CODE_RPC_FQDN_ERROR;
  }
#endif
}

int32_t taosGetFqdn(char *fqdn) {
#ifdef WINDOWS
  // Initialize Winsock
  WSADATA wsaData;
  int     iResult;
  iResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
  if (iResult != 0) {
    // printf("WSAStartup failed: %d\n", iResult);
    return TAOS_SYSTEM_WINSOCKET_ERROR(WSAGetLastError());
  }
#endif
  char hostname[1024];
  hostname[1023] = '\0';
  int32_t code = taosGetlocalhostname(hostname, 1023);
  if (code) {
    return code;
  }

#ifdef __APPLE__
  // on macosx, hostname -f has the form of xxx.local
  // which will block getaddrinfo for a few seconds if AI_CANONNAME is set
  // thus, we choose AF_INET (ipv4 for the moment) to make getaddrinfo return
  // immediately
  // hints.ai_family = AF_INET;
  strcpy(fqdn, hostname);
  strcpy(fqdn + strlen(hostname), ".local");
#else  // linux

#endif  // linux

#if defined(LINUX)

  struct addrinfo  hints = {0};
  struct addrinfo *result = NULL;
  hints.ai_flags = AI_CANONNAME;

  while (true) {
    int32_t ret = getaddrinfo(hostname, NULL, &hints, &result);
    if (ret) {
      if (EAI_AGAIN == ret) {
        continue;
      } else if (EAI_SYSTEM == ret) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        return terrno;
      }

      terrno = TAOS_SYSTEM_ERROR(ret);
      return terrno;
    }

    break;
  }

  (void)strcpy(fqdn, result->ai_canonname);

  freeaddrinfo(result);

#elif WINDOWS
  struct addrinfo  hints = {0};
  struct addrinfo *result = NULL;
  hints.ai_flags = AI_CANONNAME;

  int32_t ret = getaddrinfo(hostname, NULL, &hints, &result);
  if (!result) {
    // fprintf(stderr, "failed to get fqdn, code:%d, hostname:%s, reason:%s\n", ret, hostname, gai_strerror(ret));
    return TAOS_SYSTEM_WINSOCKET_ERROR(WSAGetLastError());
  }
  strcpy(fqdn, result->ai_canonname);
  freeaddrinfo(result);

#endif

  return 0;
}

void tinet_ntoa(char *ipstr, uint32_t ip) {
  (void)sprintf(ipstr, "%d.%d.%d.%d", ip & 0xFF, (ip >> 8) & 0xFF, (ip >> 16) & 0xFF, ip >> 24);
}

int32_t taosIgnSIGPIPE() {
  sighandler_t h = signal(SIGPIPE, SIG_IGN);
  if (SIG_ERR == h) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return terrno;
  }

  return 0;
}

/*
 * Set TCP connection timeout per-socket level.
 * ref [https://github.com/libuv/help/issues/54]
 */
int32_t taosCreateSocketWithTimeout(uint32_t timeout) {
#if defined(WINDOWS)
  SOCKET fd;
#else
  int fd;
#endif

  if ((fd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) == INVALID_SOCKET) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return terrno;
  }

#if defined(WINDOWS)
  if (0 != setsockopt(fd, IPPROTO_TCP, TCP_MAXRT, (char *)&timeout, sizeof(timeout))) {
    taosCloseSocketNoCheck1(fd);
    return TAOS_SYSTEM_WINSOCKET_ERROR(WSAGetLastError());
  }
#elif defined(_TD_DARWIN_64)
  // invalid config
  // uint32_t conn_timeout_ms = timeout * 1000;
  // if (0 != setsockopt(fd, IPPROTO_TCP, TCP_CONNECTIONTIMEOUT, (char *)&conn_timeout_ms, sizeof(conn_timeout_ms))) {
  //  taosCloseSocketNoCheck1(fd);
  //  return -1;
  //}
#else  // Linux like systems
  uint32_t conn_timeout_ms = timeout;
  if (-1 == setsockopt(fd, IPPROTO_TCP, TCP_USER_TIMEOUT, (char *)&conn_timeout_ms, sizeof(conn_timeout_ms))) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    TAOS_SKIP_ERROR(taosCloseSocketNoCheck1(fd));
    return terrno;
  }
#endif

  return (int)fd;
}

int32_t taosWinSocketInit() {
#ifdef WINDOWS
  static int8_t flag = 0;
  if (atomic_val_compare_exchange_8(&flag, 0, 1) == 0) {
    WORD    wVersionRequested;
    WSADATA wsaData;
    wVersionRequested = MAKEWORD(1, 1);
    if (WSAStartup(wVersionRequested, &wsaData) != 0) {
      atomic_store_8(&flag, 0);
      int errorCode = WSAGetLastError();
      return terrno = TAOS_SYSTEM_WINSOCKET_ERROR(errorCode);
    }
  }
  return 0;
#else
#endif
  return 0;
}

uint64_t taosHton64(uint64_t val) {
#if defined(WINDOWS) || defined(DARWIN)
  return ((val & 0x00000000000000ff) << 7 * 8) | ((val & 0x000000000000ff00) << 5 * 8) |
         ((val & 0x0000000000ff0000) << 3 * 8) | ((val & 0x00000000ff000000) << 1 * 8) |
         ((val & 0x000000ff00000000) >> 1 * 8) | ((val & 0x0000ff0000000000) >> 3 * 8) |
         ((val & 0x00ff000000000000) >> 5 * 8) | ((val & 0xff00000000000000) >> 7 * 8);
#else
  if (__BYTE_ORDER == __LITTLE_ENDIAN) {
    return (((uint64_t)htonl((int)((val << 32) >> 32))) << 32) | (unsigned int)htonl((int)(val >> 32));
  } else if (__BYTE_ORDER == __BIG_ENDIAN) {
    return val;
  }
#endif
}

uint64_t taosNtoh64(uint64_t val) {
#if defined(WINDOWS) || defined(DARWIN)
  return taosHton64(val);
#else
  if (__BYTE_ORDER == __LITTLE_ENDIAN) {
    return (((uint64_t)htonl((int)((val << 32) >> 32))) << 32) | (unsigned int)htonl((int)(val >> 32));
  } else if (__BYTE_ORDER == __BIG_ENDIAN) {
    return val;
  }
#endif
}
