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
#include <winsock2.h>
#include <ws2def.h>
#include "winsock2.h"
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

#if defined(DARWIN)
  #include <dispatch/dispatch.h>
  #include "osEok.h"
#else
  #include <sys/epoll.h>
#endif
#endif

typedef int32_t SocketFd;
typedef SocketFd  EpollFd;

typedef struct TdSocketServer {
#if SOCKET_WITH_LOCK
  TdThreadRwlock rwlock;
#endif
  int      refId;
  SocketFd fd;
} *TdSocketServerPtr, TdSocketServer;

typedef struct TdSocket {
#if SOCKET_WITH_LOCK
  TdThreadRwlock rwlock;
#endif
  int      refId;
  SocketFd fd;
} *TdSocketPtr, TdSocket;

typedef struct TdEpoll {
#if SOCKET_WITH_LOCK
  TdThreadRwlock rwlock;
#endif
  int      refId;
  EpollFd  fd;
} *TdEpollPtr, TdEpoll;

int32_t taosSendto(TdSocketPtr pSocket, void *buf, int len, unsigned int flags, const struct sockaddr *dest_addr,
                    int addrlen) {
  if (pSocket == NULL || pSocket->fd < 0) {
    return -1;
  }
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return sendto(pSocket->fd, buf, len, flags, dest_addr, addrlen);
#else
  return sendto(pSocket->fd, buf, len, flags, dest_addr, addrlen);
#endif
}
int32_t taosWriteSocket(TdSocketPtr pSocket, void *buf, int len) {
  if (pSocket == NULL || pSocket->fd < 0) {
    return -1;
  }
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return send(pSocket->fd, buf, len, 0);
  ;
#else
  return write(pSocket->fd, buf, len);
#endif
}
int32_t taosReadSocket(TdSocketPtr pSocket, void *buf, int len) {
  if (pSocket == NULL || pSocket->fd < 0) {
    return -1;
  }
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return recv(pSocket->fd, buf, len, 0);
  ;
#else
  return read(pSocket->fd, buf, len);
#endif
}

int32_t taosReadFromSocket(TdSocketPtr pSocket, void *buf, int32_t len, int32_t flags, struct sockaddr *destAddr, socklen_t *addrLen) {
  if (pSocket == NULL || pSocket->fd < 0) {
    return -1;
  }
  return recvfrom(pSocket->fd, buf, len, flags, destAddr, addrLen);
}
int32_t taosCloseSocketNoCheck1(SocketFd fd) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return closesocket(fd);
#else
  return close(fd);
#endif
}
int32_t taosCloseSocket(TdSocketPtr *ppSocket) {
  int32_t code;
  if (ppSocket == NULL || *ppSocket == NULL || (*ppSocket)->fd < 0) {
    return -1;
  }
  code = taosCloseSocketNoCheck1((*ppSocket)->fd);
  (*ppSocket)->fd = -1;
  free(*ppSocket);
  return code;
}
int32_t taosCloseSocketServer(TdSocketServerPtr *ppSocketServer) {
  int32_t code;
  if (ppSocketServer == NULL || *ppSocketServer == NULL || (*ppSocketServer)->fd < 0) {
    return -1;
  }
  code = taosCloseSocketNoCheck1((*ppSocketServer)->fd);
  (*ppSocketServer)->fd = -1;
  free(*ppSocketServer);
  return code;
}

int32_t taosShutDownSocketRD(TdSocketPtr pSocket) {
  if (pSocket == NULL || pSocket->fd < 0) {
    return -1;
  }
#ifdef WINDOWS
  return closesocket(pSocket->fd);
#elif __APPLE__
  return close(pSocket->fd);
#else
  return shutdown(pSocket->fd, SHUT_RD);
#endif
}
int32_t taosShutDownSocketServerRD(TdSocketServerPtr pSocketServer) {
  if (pSocketServer == NULL || pSocketServer->fd < 0) {
    return -1;
  }
#ifdef WINDOWS
  return closesocket(pSocketServer->fd);
#elif __APPLE__
  return close(pSocketServer->fd);
#else
  return shutdown(pSocketServer->fd, SHUT_RD);
#endif
}

int32_t taosShutDownSocketWR(TdSocketPtr pSocket) {
  if (pSocket == NULL || pSocket->fd < 0) {
    return -1;
  }
#ifdef WINDOWS
  return closesocket(pSocket->fd);
#elif __APPLE__
  return close(pSocket->fd);
#else
  return shutdown(pSocket->fd, SHUT_WR);
#endif
}
int32_t taosShutDownSocketServerWR(TdSocketServerPtr pSocketServer) {
  if (pSocketServer == NULL || pSocketServer->fd < 0) {
    return -1;
  }
#ifdef WINDOWS
  return closesocket(pSocketServer->fd);
#elif __APPLE__
  return close(pSocketServer->fd);
#else
  return shutdown(pSocketServer->fd, SHUT_WR);
#endif
}
int32_t taosShutDownSocketRDWR(TdSocketPtr pSocket) {
  if (pSocket == NULL || pSocket->fd < 0) {
    return -1;
  }
#ifdef WINDOWS
  return closesocket(pSocket->fd);
#elif __APPLE__
  return close(pSocket->fd);
#else
  return shutdown(pSocket->fd, SHUT_RDWR);
#endif
}
int32_t taosShutDownSocketServerRDWR(TdSocketServerPtr pSocketServer) {
  if (pSocketServer == NULL || pSocketServer->fd < 0) {
    return -1;
  }
#ifdef WINDOWS
  return closesocket(pSocketServer->fd);
#elif __APPLE__
  return close(pSocketServer->fd);
#else
  return shutdown(pSocketServer->fd, SHUT_RDWR);
#endif
}

void taosWinSocketInit1() {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  static char flag = 0;
  if (flag == 0) {
    WORD    wVersionRequested;
    WSADATA wsaData;
    wVersionRequested = MAKEWORD(1, 1);
    if (WSAStartup(wVersionRequested, &wsaData) == 0) {
      flag = 1;
    }
  }
#else
#endif
}
int32_t taosSetNonblocking(TdSocketPtr pSocket, int32_t on) {
  if (pSocket == NULL || pSocket->fd < 0) {
    return -1;
  }
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  u_long mode;
  if (on) {
    mode = 1;
    ioctlsocket(pSocket->fd, FIONBIO, &mode);
  } else {
    mode = 0;
    ioctlsocket(pSocket->fd, FIONBIO, &mode);
  }
#else
  int32_t flags = 0;
  if ((flags = fcntl(pSocket->fd, F_GETFL, 0)) < 0) {
    // printf("fcntl(F_GETFL) error: %d (%s)\n", errno, strerror(errno));
    return 1;
  }

  if (on)
    flags |= O_NONBLOCK;
  else
    flags &= ~O_NONBLOCK;

  if ((flags = fcntl(pSocket->fd, F_SETFL, flags)) < 0) {
    // printf("fcntl(F_SETFL) error: %d (%s)\n", errno, strerror(errno));
    return 1;
  }
#endif
  return 0;
}
int32_t taosSetSockOpt(TdSocketPtr pSocket, int32_t level, int32_t optname, void *optval, int32_t optlen) {
  if (pSocket == NULL || pSocket->fd < 0) {
    return -1;
  }
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
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

  return setsockopt(pSocket->fd, level, optname, optval, optlen);
#else
  return setsockopt(pSocket->fd, level, optname, optval, (socklen_t)optlen);
#endif
}
int32_t taosGetSockOpt(TdSocketPtr pSocket, int32_t level, int32_t optname, void *optval, int32_t *optlen) {
  if (pSocket == NULL || pSocket->fd < 0) {
    return -1;
  }
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return 0;
#else
  return getsockopt(pSocket->fd, level, optname, optval, (socklen_t *)optlen);
#endif
}
uint32_t taosInetAddr(const char *ipAddr) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  uint32_t value;
  int32_t  ret = inet_pton(AF_INET, ipAddr, &value);
  if (ret <= 0) {
    return INADDR_NONE;
  } else {
    return value;
  }
#else
  return inet_addr(ipAddr);
#endif
}
const char *taosInetNtoa(struct in_addr ipInt) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  // not thread safe, only for debug usage while print log
  static char tmpDstStr[16];
  return inet_ntop(AF_INET, &ipInt, tmpDstStr, INET6_ADDRSTRLEN);
#else
  return inet_ntoa(ipInt);
#endif
}

#ifndef SIGPIPE
#define SIGPIPE EPIPE
#endif

#define TCP_CONN_TIMEOUT 3000  // conn timeout

int32_t taosWriteMsg(TdSocketPtr pSocket, void *buf, int32_t nbytes) {
  if (pSocket == NULL || pSocket->fd < 0) {
    return -1;
  }
  int32_t nleft, nwritten;
  char   *ptr = (char *)buf;

  nleft = nbytes;

  while (nleft > 0) {
    nwritten = taosWriteSocket(pSocket, (char *)ptr, (size_t)nleft);
    if (nwritten <= 0) {
      if (errno == EINTR /* || errno == EAGAIN || errno == EWOULDBLOCK */)
        continue;
      else
        return -1;
    } else {
      nleft -= nwritten;
      ptr += nwritten;
    }

    if (errno == SIGPIPE || errno == EPIPE) {
      return -1;
    }
  }

  return (nbytes - nleft);
}

int32_t taosReadMsg(TdSocketPtr pSocket, void *buf, int32_t nbytes) {
  if (pSocket == NULL || pSocket->fd < 0) {
    return -1;
  }
  int32_t nleft, nread;
  char   *ptr = (char *)buf;

  nleft = nbytes;

  while (nleft > 0) {
    nread = taosReadSocket(pSocket, ptr, (size_t)nleft);
    if (nread == 0) {
      break;
    } else if (nread < 0) {
      if (errno == EINTR /* || errno == EAGAIN || errno == EWOULDBLOCK*/) {
        continue;
      } else {
        return -1;
      }
    } else {
      nleft -= nread;
      ptr += nread;
    }

    if (errno == SIGPIPE || errno == EPIPE) {
      return -1;
    }
  }

  return (nbytes - nleft);
}

int32_t taosNonblockwrite(TdSocketPtr pSocket, char *ptr, int32_t nbytes) {
  if (pSocket == NULL || pSocket->fd < 0) {
    return -1;
  }
  taosSetNonblocking(pSocket, 1);

  int32_t        nleft, nwritten, nready;
  fd_set         fset;
  struct timeval tv;

  nleft = nbytes;
  while (nleft > 0) {
    tv.tv_sec = 30;
    tv.tv_usec = 0;
    FD_ZERO(&fset);
    FD_SET(pSocket->fd, &fset);
    if ((nready = select((SocketFd)(pSocket->fd + 1), NULL, &fset, NULL, &tv)) == 0) {
      errno = ETIMEDOUT;
      // printf("fd %d timeout, no enough space to write", fd);
      break;

    } else if (nready < 0) {
      if (errno == EINTR) continue;

      // printf("select error, %d (%s)", errno, strerror(errno));
      return -1;
    }

    nwritten = (int32_t)send(pSocket->fd, ptr, (size_t)nleft, MSG_NOSIGNAL);
    if (nwritten <= 0) {
      if (errno == EAGAIN || errno == EINTR) continue;

      // printf("write error, %d (%s)", errno, strerror(errno));
      return -1;
    }

    nleft -= nwritten;
    ptr += nwritten;
  }

  taosSetNonblocking(pSocket, 0);

  return (nbytes - nleft);
}

TdSocketPtr taosOpenUdpSocket(uint32_t ip, uint16_t port) {
  struct sockaddr_in localAddr;
  SocketFd           fd;
  int32_t            bufSize = 1024000;

  // printf("open udp socket:0x%x:%hu", ip, port);

  memset((char *)&localAddr, 0, sizeof(localAddr));
  localAddr.sin_family = AF_INET;
  localAddr.sin_addr.s_addr = ip;
  localAddr.sin_port = (uint16_t)htons(port);

  if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) <= 2) {
    // printf("failed to open udp socket: %d (%s)", errno, strerror(errno));
    taosCloseSocketNoCheck1(fd);
    return NULL;
  }

  TdSocketPtr pSocket = (TdSocketPtr)malloc(sizeof(TdSocket));
  if (pSocket == NULL) {
    taosCloseSocketNoCheck1(fd);
    return NULL;
  }
  pSocket->fd = fd;
  pSocket->refId = 0;

  if (taosSetSockOpt(pSocket, SOL_SOCKET, SO_SNDBUF, (void *)&bufSize, sizeof(bufSize)) != 0) {
    // printf("failed to set the send buffer size for UDP socket\n");
    taosCloseSocket(&pSocket);
    return NULL;
  }

  if (taosSetSockOpt(pSocket, SOL_SOCKET, SO_RCVBUF, (void *)&bufSize, sizeof(bufSize)) != 0) {
    // printf("failed to set the receive buffer size for UDP socket\n");
    taosCloseSocket(&pSocket);
    return NULL;
  }

  /* bind socket to local address */
  if (bind(pSocket->fd, (struct sockaddr *)&localAddr, sizeof(localAddr)) < 0) {
    // printf("failed to bind udp socket: %d (%s), 0x%x:%hu", errno, strerror(errno), ip, port);
    taosCloseSocket(&pSocket);
    return NULL;
  }

  return pSocket;
}

TdSocketPtr taosOpenTcpClientSocket(uint32_t destIp, uint16_t destPort, uint32_t clientIp) {
  SocketFd           fd = -1;
  int32_t            ret;
  struct sockaddr_in serverAddr, clientAddr;
  int32_t            bufSize = 1024 * 1024;

  fd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);

  if (fd <= 2) {
    // printf("failed to open the socket: %d (%s)", errno, strerror(errno));
    if (fd >= 0) taosCloseSocketNoCheck1(fd);
    return NULL;
  }

  TdSocketPtr pSocket = (TdSocketPtr)malloc(sizeof(TdSocket));
  if (pSocket == NULL) {
    taosCloseSocketNoCheck1(fd);
    return NULL;
  }
  pSocket->fd = fd;
  pSocket->refId = 0;

  /* set REUSEADDR option, so the portnumber can be re-used */
  int32_t reuse = 1;
  if (taosSetSockOpt(pSocket, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse, sizeof(reuse)) < 0) {
    // printf("setsockopt SO_REUSEADDR failed: %d (%s)", errno, strerror(errno));
    taosCloseSocket(&pSocket);
    return NULL;
  }

  if (taosSetSockOpt(pSocket, SOL_SOCKET, SO_SNDBUF, (void *)&bufSize, sizeof(bufSize)) != 0) {
    // printf("failed to set the send buffer size for TCP socket\n");
    taosCloseSocket(&pSocket);
    return NULL;
  }

  if (taosSetSockOpt(pSocket, SOL_SOCKET, SO_RCVBUF, (void *)&bufSize, sizeof(bufSize)) != 0) {
    // printf("failed to set the receive buffer size for TCP socket\n");
    taosCloseSocket(&pSocket);
    return NULL;
  }

  if (clientIp != 0) {
    memset((char *)&clientAddr, 0, sizeof(clientAddr));
    clientAddr.sin_family = AF_INET;
    clientAddr.sin_addr.s_addr = clientIp;
    clientAddr.sin_port = 0;

    /* bind socket to client address */
    if (bind(pSocket->fd, (struct sockaddr *)&clientAddr, sizeof(clientAddr)) < 0) {
      // printf("bind tcp client socket failed, client(0x%x:0), dest(0x%x:%d), reason:(%s)", clientIp, destIp, destPort,
      //        strerror(errno));
      taosCloseSocket(&pSocket);
      return NULL;
    }
  }

  memset((char *)&serverAddr, 0, sizeof(serverAddr));
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_addr.s_addr = destIp;
  serverAddr.sin_port = (uint16_t)htons((uint16_t)destPort);

#ifdef _TD_LINUX
  taosSetNonblocking(pSocket, 1);
  ret = connect(pSocket->fd, (struct sockaddr *)&serverAddr, sizeof(serverAddr));
  if (ret == -1) {
    if (errno == EHOSTUNREACH) {
      // printf("failed to connect socket, ip:0x%x, port:%hu(%s)", destIp, destPort, strerror(errno));
      taosCloseSocket(&pSocket);
      return -1;
    } else if (errno == EINPROGRESS || errno == EAGAIN || errno == EWOULDBLOCK) {
      struct pollfd wfd[1];

      wfd[0].fd = pSocket->fd;
      wfd[0].events = POLLOUT;

      int res = poll(wfd, 1, TCP_CONN_TIMEOUT);
      if (res == -1 || res == 0) {
        // printf("failed to connect socket, ip:0x%x, port:%hu(poll error/conn timeout)", destIp, destPort);
        taosCloseSocket(&pSocket);  //
        return -1;
      }
      int optVal = -1, optLen = sizeof(int);
      if ((0 != taosGetSockOpt(pSocket, SOL_SOCKET, SO_ERROR, &optVal, &optLen)) || (optVal != 0)) {
        // printf("failed to connect socket, ip:0x%x, port:%hu(connect host error)", destIp, destPort);
        taosCloseSocket(&pSocket);  //
        return -1;
      }
      ret = 0;
    } else {  // Other error
      // printf("failed to connect socket, ip:0x%x, port:%hu(target host cannot be reached)", destIp, destPort);
      taosCloseSocket(&pSocket);  //
      return -1;
    }
  }
  taosSetNonblocking(pSocket, 0);

#else
  ret = connect(pSocket->fd, (struct sockaddr *)&serverAddr, sizeof(serverAddr));
#endif

  if (ret != 0) {
    // printf("failed to connect socket, ip:0x%x, port:%hu(%s)", destIp, destPort, strerror(errno));
    taosCloseSocket(&pSocket);
    return NULL;
  } else {
    taosKeepTcpAlive(pSocket);
  }

  return pSocket;
}

int32_t taosKeepTcpAlive(TdSocketPtr pSocket) {
  if (pSocket == NULL || pSocket->fd < 0) {
    return -1;
  }
  int32_t alive = 1;
  if (taosSetSockOpt(pSocket, SOL_SOCKET, SO_KEEPALIVE, (void *)&alive, sizeof(alive)) < 0) {
    // printf("fd:%d setsockopt SO_KEEPALIVE failed: %d (%s)", sockFd, errno, strerror(errno));
    taosCloseSocket(&pSocket);
    return -1;
  }

#ifndef __APPLE__
  // all fails on macosx
  int32_t probes = 3;
  if (taosSetSockOpt(pSocket, SOL_TCP, TCP_KEEPCNT, (void *)&probes, sizeof(probes)) < 0) {
    // printf("fd:%d setsockopt SO_KEEPCNT failed: %d (%s)", sockFd, errno, strerror(errno));
    taosCloseSocket(&pSocket);
    return -1;
  }

  int32_t alivetime = 10;
  if (taosSetSockOpt(pSocket, SOL_TCP, TCP_KEEPIDLE, (void *)&alivetime, sizeof(alivetime)) < 0) {
    // printf("fd:%d setsockopt SO_KEEPIDLE failed: %d (%s)", sockFd, errno, strerror(errno));
    taosCloseSocket(&pSocket);
    return -1;
  }

  int32_t interval = 3;
  if (taosSetSockOpt(pSocket, SOL_TCP, TCP_KEEPINTVL, (void *)&interval, sizeof(interval)) < 0) {
    // printf("fd:%d setsockopt SO_KEEPINTVL failed: %d (%s)", sockFd, errno, strerror(errno));
    taosCloseSocket(&pSocket);
    return -1;
  }
#endif  // __APPLE__

  int32_t nodelay = 1;
  if (taosSetSockOpt(pSocket, IPPROTO_TCP, TCP_NODELAY, (void *)&nodelay, sizeof(nodelay)) < 0) {
    // printf("fd:%d setsockopt TCP_NODELAY failed %d (%s)", sockFd, errno, strerror(errno));
    taosCloseSocket(&pSocket);
    return -1;
  }

  struct linger linger = {0};
  linger.l_onoff = 1;
  linger.l_linger = 3;
  if (taosSetSockOpt(pSocket, SOL_SOCKET, SO_LINGER, (void *)&linger, sizeof(linger)) < 0) {
    // printf("setsockopt SO_LINGER failed: %d (%s)", errno, strerror(errno));
    taosCloseSocket(&pSocket);
    return -1;
  }

  return 0;
}

TdSocketServerPtr taosOpenTcpServerSocket(uint32_t ip, uint16_t port) {
  struct sockaddr_in serverAdd;
  SocketFd           fd;
  int32_t            reuse;

  // printf("open tcp server socket:0x%x:%hu", ip, port);

  bzero((char *)&serverAdd, sizeof(serverAdd));
  serverAdd.sin_family = AF_INET;
  serverAdd.sin_addr.s_addr = ip;
  serverAdd.sin_port = (uint16_t)htons(port);

  if ((fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) <= 2) {
    // printf("failed to open TCP socket: %d (%s)", errno, strerror(errno));
    taosCloseSocketNoCheck1(fd);
    return NULL;
  }

  TdSocketPtr pSocket = (TdSocketPtr)malloc(sizeof(TdSocket));
  if (pSocket == NULL) {
    taosCloseSocketNoCheck1(fd);
    return NULL;
  }
  pSocket->refId = 0;
  pSocket->fd = fd;

  /* set REUSEADDR option, so the portnumber can be re-used */
  reuse = 1;
  if (taosSetSockOpt(pSocket, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse, sizeof(reuse)) < 0) {
    // printf("setsockopt SO_REUSEADDR failed: %d (%s)", errno, strerror(errno));
    taosCloseSocket(&pSocket);
    return NULL;
  }

  /* bind socket to server address */
  if (bind(pSocket->fd, (struct sockaddr *)&serverAdd, sizeof(serverAdd)) < 0) {
    // printf("bind tcp server socket failed, 0x%x:%hu(%s)", ip, port, strerror(errno));
    taosCloseSocket(&pSocket);
    return NULL;
  }

  if (taosKeepTcpAlive(pSocket) < 0) {
    // printf("failed to set tcp server keep-alive option, 0x%x:%hu(%s)", ip, port, strerror(errno));
    taosCloseSocket(&pSocket);
    return NULL;
  }

  if (listen(pSocket->fd, 1024) < 0) {
    // printf("listen tcp server socket failed, 0x%x:%hu(%s)", ip, port, strerror(errno));
    taosCloseSocket(&pSocket);
    return NULL;
  }

  return (TdSocketServerPtr)pSocket;
}

TdSocketPtr taosAcceptTcpConnectSocket(TdSocketServerPtr pServerSocket, struct sockaddr *destAddr,
                                       socklen_t *addrLen) {
  if (pServerSocket == NULL || pServerSocket->fd < 0) {
    return NULL;
  }
  SocketFd fd = accept(pServerSocket->fd, destAddr, addrLen);
  if (fd == -1) {
    // tError("TCP accept failure(%s)", strerror(errno));
    return NULL;
  }

  TdSocketPtr pSocket = (TdSocketPtr)malloc(sizeof(TdSocket));
  if (pSocket == NULL) {
    taosCloseSocketNoCheck1(fd);
    return NULL;
  }
  pSocket->fd = fd;
  pSocket->refId = 0;
  return pSocket;
}
#define COPY_SIZE 32768
// sendfile shall be used

int64_t taosCopyFds(TdSocketPtr pSrcSocket, TdSocketPtr pDestSocket, int64_t len) {
  if (pSrcSocket == NULL || pSrcSocket->fd < 0 || pDestSocket == NULL || pDestSocket->fd < 0) {
    return -1;
  }
  int64_t leftLen;
  int64_t readLen, writeLen;
  char    temp[COPY_SIZE];

  leftLen = len;

  while (leftLen > 0) {
    if (leftLen < COPY_SIZE)
      readLen = leftLen;
    else
      readLen = COPY_SIZE;  // 4K

    int64_t retLen = taosReadMsg(pSrcSocket, temp, (int32_t)readLen);
    if (readLen != retLen) {
      // printf("read error, readLen:%" PRId64 " retLen:%" PRId64 " len:%" PRId64 " leftLen:%" PRId64 ", reason:%s",
      //        readLen, retLen, len, leftLen, strerror(errno));
      return -1;
    }

    writeLen = taosWriteMsg(pDestSocket, temp, (int32_t)readLen);

    if (readLen != writeLen) {
      // printf("copy error, readLen:%" PRId64 " writeLen:%" PRId64 " len:%" PRId64 " leftLen:%" PRId64 ", reason:%s",
      //        readLen, writeLen, len, leftLen, strerror(errno));
      return -1;
    }

    leftLen -= readLen;
  }

  return len;
}

void taosBlockSIGPIPE() {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
#else
  sigset_t signal_mask;
  sigemptyset(&signal_mask);
  sigaddset(&signal_mask, SIGPIPE);
  int32_t rc = taosThreadSigmask(SIG_BLOCK, &signal_mask, NULL);
  if (rc != 0) {
    // printf("failed to block SIGPIPE");
  }
#endif
}

uint32_t taosGetIpv4FromFqdn(const char *fqdn) {
  struct addrinfo hints = {0};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  struct addrinfo *result = NULL;

  int32_t ret = getaddrinfo(fqdn, NULL, &hints, &result);
  if (result) {
    struct sockaddr    *sa = result->ai_addr;
    struct sockaddr_in *si = (struct sockaddr_in *)sa;
    struct in_addr      ia = si->sin_addr;
    uint32_t            ip = ia.s_addr;
    freeaddrinfo(result);
    return ip;
  } else {
#ifdef EAI_SYSTEM
    if (ret == EAI_SYSTEM) {
      // printf("failed to get the ip address, fqdn:%s, since:%s", fqdn, strerror(errno));
    } else {
      // printf("failed to get the ip address, fqdn:%s, since:%s", fqdn, gai_strerror(ret));
    }
#else
    // printf("failed to get the ip address, fqdn:%s, since:%s", fqdn, gai_strerror(ret));
#endif
    return 0xFFFFFFFF;
  }
}

int32_t taosGetFqdn(char *fqdn) {
  char hostname[1024];
  hostname[1023] = '\0';
  if (gethostname(hostname, 1023) == -1) {
    printf("failed to get hostname, reason:%s", strerror(errno));
    assert(0);
    return -1;
  }

  struct addrinfo  hints = {0};
  struct addrinfo *result = NULL;
#ifdef __APPLE__
  // on macosx, hostname -f has the form of xxx.local
  // which will block getaddrinfo for a few seconds if AI_CANONNAME is set
  // thus, we choose AF_INET (ipv4 for the moment) to make getaddrinfo return
  // immediately
  hints.ai_family = AF_INET;
#else   // __APPLE__
  hints.ai_flags = AI_CANONNAME;
#endif  // __APPLE__
  int32_t ret = getaddrinfo(hostname, NULL, &hints, &result);
  if (!result) {
    printf("failed to get fqdn, code:%d, reason:%s", ret, gai_strerror(ret));
    assert(0);
    return -1;
  }

#ifdef __APPLE__
  // refer to comments above
  strcpy(fqdn, hostname);
#else   // __APPLE__
  strcpy(fqdn, result->ai_canonname);
#endif  // __APPLE__
  freeaddrinfo(result);
  return 0;
}

// Function converting an IP address string to an uint32_t.
uint32_t ip2uint(const char *const ip_addr) {
  char ip_addr_cpy[20];
  char ip[5];

  tstrncpy(ip_addr_cpy, ip_addr, sizeof(ip_addr_cpy));

  char *s_start, *s_end;
  s_start = ip_addr_cpy;
  s_end = ip_addr_cpy;

  int32_t k;

  for (k = 0; *s_start != '\0'; s_start = s_end) {
    for (s_end = s_start; *s_end != '.' && *s_end != '\0'; s_end++) {
    }
    if (*s_end == '.') {
      *s_end = '\0';
      s_end++;
    }
    ip[k++] = (char)atoi(s_start);
  }

  ip[k] = '\0';

  return *((uint32_t *)ip);
}

void tinet_ntoa(char *ipstr, uint32_t ip) {
  sprintf(ipstr, "%d.%d.%d.%d", ip & 0xFF, (ip >> 8) & 0xFF, (ip >> 16) & 0xFF, ip >> 24);
}

void taosIgnSIGPIPE() {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
#else
  signal(SIGPIPE, SIG_IGN);
#endif
}

void taosSetMaskSIGPIPE() {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
#else
  sigset_t signal_mask;
  sigemptyset(&signal_mask);
  sigaddset(&signal_mask, SIGPIPE);
  int32_t rc = taosThreadSigmask(SIG_SETMASK, &signal_mask, NULL);
  if (rc != 0) {
    // printf("failed to setmask SIGPIPE");
  }
#endif
}

int32_t taosGetSocketName(TdSocketPtr pSocket, struct sockaddr *destAddr, socklen_t *addrLen) {
  if (pSocket == NULL || pSocket->fd < 0) {
    return -1;
  }
  return getsockname(pSocket->fd, destAddr, addrLen);
}


TdEpollPtr taosCreateEpoll(int32_t size) {
  EpollFd fd = -1;
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
#else
  fd = epoll_create(size);
#endif
  if (fd < 0) {
    return NULL;
  }

  TdEpollPtr pEpoll = (TdEpollPtr)malloc(sizeof(TdEpoll));
  if (pEpoll == NULL) {
    taosCloseSocketNoCheck1(fd);
    return NULL;
  }
  pEpoll->fd = fd;
  pEpoll->refId = 0;
  return pEpoll;
}
int32_t taosCtlEpoll(TdEpollPtr pEpoll, int32_t epollOperate, TdSocketPtr pSocket, struct epoll_event *event) {
  int32_t code = -1;
  if (pEpoll == NULL || pEpoll->fd < 0) {
    return -1;
  }
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
#else
  code = epoll_ctl(pEpoll->fd, epollOperate, pSocket->fd, event);
#endif
  return code;
}
int32_t taosWaitEpoll(TdEpollPtr pEpoll, struct epoll_event *event, int32_t maxEvents, int32_t timeout) {
  int32_t code = -1;
  if (pEpoll == NULL || pEpoll->fd < 0) {
    return -1;
  }
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
#else
  code = epoll_wait(pEpoll->fd, event, maxEvents, timeout);
#endif
  return code;
}
int32_t taosCloseEpoll(TdEpollPtr *ppEpoll) {
  int32_t code;
  if (ppEpoll == NULL || *ppEpoll == NULL || (*ppEpoll)->fd < 0) {
    return -1;
  }
  code = taosCloseSocketNoCheck1((*ppEpoll)->fd);
  (*ppEpoll)->fd = -1;
  free(*ppEpoll);
  return code;
}
