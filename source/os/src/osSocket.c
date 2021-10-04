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
  #include "winsock2.h"
  #include <WS2tcpip.h>
  #include <winbase.h>
  #include <Winsock2.h>
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

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)

#define taosSend(sockfd, buf, len, flags) send((SOCKET)sockfd, buf, len, flags)
int32_t taosSendto(SocketFd fd, void *buf, int len, unsigned int flags, const struct sockaddr *to, int tolen) {
  return sendto((SOCKET)sockfd, buf, len, flags, dest_addr, addrlen);
}
int32_t taosWriteSocket(SocketFd fd, void *buf, int len) { return send((SOCKET)fd, buf, len, 0); }
int32_t taosReadSocket(SocketFd fd, void *buf, int len) { return recv((SOCKET)fd, buf, len, 0)(); }
int32_t taosCloseSocketNoCheck(SocketFd fd) { return closesocket((SOCKET)fd); }
int32_t taosCloseSocket(SocketFd fd) { closesocket((SOCKET)fd) }

#else

  #define taosSend(sockfd, buf, len, flags) send(sockfd, buf, len, flags)
  int32_t taosSendto(SocketFd fd, void * buf, int len, unsigned int flags, const struct sockaddr * dest_addr, int addrlen) {
    return sendto(fd, buf, len, flags, dest_addr, addrlen);
  }

  int32_t taosWriteSocket(SocketFd fd, void *buf, int len) {
    return write(fd, buf, len);
  }

  int32_t taosReadSocket(SocketFd fd, void *buf, int len) {
    return read(fd, buf, len);
  }

  int32_t taosCloseSocketNoCheck(SocketFd fd) {
    return close(fd);
  }

  int32_t taosCloseSocket(SocketFd fd) {
    if (fd > -1) {
      close(fd);
    }
  }
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

#ifndef SIGPIPE
  #define SIGPIPE EPIPE
#endif

#define TCP_CONN_TIMEOUT 3000  // conn timeout

int32_t taosGetFqdn(char *fqdn) {
  char hostname[1024];
  hostname[1023] = '\0';
  if (gethostname(hostname, 1023) == -1) {
    printf("failed to get hostname, reason:%s", strerror(errno));
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
#else // __APPLE__
  hints.ai_flags = AI_CANONNAME;
#endif // __APPLE__
  int32_t ret = getaddrinfo(hostname, NULL, &hints, &result);
  if (!result) {
    printf("failed to get fqdn, code:%d, reason:%s", ret, gai_strerror(ret));
    return -1;
  }

#ifdef __APPLE__
  // refer to comments above
  strcpy(fqdn, hostname);
#else // __APPLE__
  strcpy(fqdn, result->ai_canonname);
#endif // __APPLE__
  freeaddrinfo(result);
  return 0;
}

uint32_t taosGetIpv4FromFqdn(const char *fqdn) {
  struct addrinfo hints = {0};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  struct addrinfo *result = NULL;

  int32_t ret = getaddrinfo(fqdn, NULL, &hints, &result);
  if (result) {
    struct sockaddr *   sa = result->ai_addr;
    struct sockaddr_in *si = (struct sockaddr_in *)sa;
    struct in_addr      ia = si->sin_addr;
    uint32_t            ip = ia.s_addr;
    freeaddrinfo(result);
    return ip;
  } else {
#ifdef EAI_SYSTEM
    if (ret == EAI_SYSTEM) {
      printf("failed to get the ip address, fqdn:%s, since:%s", fqdn, strerror(errno));
    } else {
      printf("failed to get the ip address, fqdn:%s, since:%s", fqdn, gai_strerror(ret));
    }
#else
    printf("failed to get the ip address, fqdn:%s, since:%s", fqdn, gai_strerror(ret));
#endif
    return 0xFFFFFFFF;
  }
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

int32_t taosWriteMsg(SOCKET fd, void *buf, int32_t nbytes) {
  int32_t nleft, nwritten;
  char *  ptr = (char *)buf;

  nleft = nbytes;

  while (nleft > 0) {
    nwritten = (int32_t)taosWriteSocket(fd, (char *)ptr, (size_t)nleft);
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

int32_t taosReadMsg(SOCKET fd, void *buf, int32_t nbytes) {
  int32_t nleft, nread;
  char *  ptr = (char *)buf;

  nleft = nbytes;

  if (fd < 0) return -1;

  while (nleft > 0) {
    nread = (int32_t)taosReadSocket(fd, ptr, (size_t)nleft);
    if (nread == 0) {
      break;
    } else if (nread < 0) {
      if (errno == EINTR/* || errno == EAGAIN || errno == EWOULDBLOCK*/) {
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

int32_t taosNonblockwrite(SOCKET fd, char *ptr, int32_t nbytes) {
  taosSetNonblocking(fd, 1);

  int32_t nleft, nwritten, nready;
  fd_set  fset;
  struct timeval tv;

  nleft = nbytes;
  while (nleft > 0) {
    tv.tv_sec = 30;
    tv.tv_usec = 0;
    FD_ZERO(&fset);
    FD_SET(fd, &fset);
    if ((nready = select((int32_t)(fd + 1), NULL, &fset, NULL, &tv)) == 0) {
      errno = ETIMEDOUT;
      printf("fd %d timeout, no enough space to write", fd);
      break;

    } else if (nready < 0) {
      if (errno == EINTR) continue;

      printf("select error, %d (%s)", errno, strerror(errno));
      return -1;
    }

    nwritten = (int32_t)taosSend(fd, ptr, (size_t)nleft, MSG_NOSIGNAL);
    if (nwritten <= 0) {
      if (errno == EAGAIN || errno == EINTR) continue;

      printf("write error, %d (%s)", errno, strerror(errno));
      return -1;
    }

    nleft -= nwritten;
    ptr += nwritten;
  }

  taosSetNonblocking(fd, 0);

  return (nbytes - nleft);
}

int32_t taosReadn(SOCKET fd, char *ptr, int32_t nbytes) {
  int32_t nread, nready, nleft = nbytes;

  fd_set fset;
  struct timeval tv;

  while (nleft > 0) {
    tv.tv_sec = 30;
    tv.tv_usec = 0;
    FD_ZERO(&fset);
    FD_SET(fd, &fset);
    if ((nready = select((int32_t)(fd + 1), NULL, &fset, NULL, &tv)) == 0) {
      errno = ETIMEDOUT;
      printf("fd %d timeout\n", fd);
      break;
    } else if (nready < 0) {
      if (errno == EINTR) continue;
      printf("select error, %d (%s)", errno, strerror(errno));
      return -1;
    }

    if ((nread = (int32_t)taosReadSocket(fd, ptr, (size_t)nleft)) < 0) {
      if (errno == EINTR) continue;
      printf("read error, %d (%s)", errno, strerror(errno));
      return -1;

    } else if (nread == 0) {
      printf("fd %d EOF", fd);
      break;  // EOF
    }

    nleft -= nread;
    ptr += nread;
  }

  return (nbytes - nleft);
}

SOCKET taosOpenUdpSocket(uint32_t ip, uint16_t port) {
  struct sockaddr_in localAddr;
  SOCKET sockFd;
  int32_t bufSize = 1024000;

  printf("open udp socket:0x%x:%hu", ip, port);

  memset((char *)&localAddr, 0, sizeof(localAddr));
  localAddr.sin_family = AF_INET;
  localAddr.sin_addr.s_addr = ip;
  localAddr.sin_port = (uint16_t)htons(port);

  if ((sockFd = socket(AF_INET, SOCK_DGRAM, 0)) <= 2) {
    printf("failed to open udp socket: %d (%s)", errno, strerror(errno));
    taosCloseSocketNoCheck(sockFd);
    return -1;
  }

  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_SNDBUF, (void *)&bufSize, sizeof(bufSize)) != 0) {
    printf("failed to set the send buffer size for UDP socket\n");
    taosCloseSocket(sockFd);
    return -1;
  }

  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_RCVBUF, (void *)&bufSize, sizeof(bufSize)) != 0) {
    printf("failed to set the receive buffer size for UDP socket\n");
    taosCloseSocket(sockFd);
    return -1;
  }

  /* bind socket to local address */
  if (bind(sockFd, (struct sockaddr *)&localAddr, sizeof(localAddr)) < 0) {
    printf("failed to bind udp socket: %d (%s), 0x%x:%hu", errno, strerror(errno), ip, port);
    taosCloseSocket(sockFd);
    return -1;
  }

  return sockFd;
}

SOCKET taosOpenTcpClientSocket(uint32_t destIp, uint16_t destPort, uint32_t clientIp) {
  SOCKET sockFd = 0;
  int32_t ret;
  struct sockaddr_in serverAddr, clientAddr;
  int32_t bufSize = 1024 * 1024;

  sockFd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);

  if (sockFd <= 2) {
    printf("failed to open the socket: %d (%s)", errno, strerror(errno));
    taosCloseSocketNoCheck(sockFd);
    return -1;
  }

  /* set REUSEADDR option, so the portnumber can be re-used */
  int32_t reuse = 1;
  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse, sizeof(reuse)) < 0) {
    printf("setsockopt SO_REUSEADDR failed: %d (%s)", errno, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }

  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_SNDBUF, (void *)&bufSize, sizeof(bufSize)) != 0) {
    printf("failed to set the send buffer size for TCP socket\n");
    taosCloseSocket(sockFd);
    return -1;
  }

  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_RCVBUF, (void *)&bufSize, sizeof(bufSize)) != 0) {
    printf("failed to set the receive buffer size for TCP socket\n");
    taosCloseSocket(sockFd);
    return -1;
  }

  if (clientIp != 0) {
    memset((char *)&clientAddr, 0, sizeof(clientAddr));
    clientAddr.sin_family = AF_INET;
    clientAddr.sin_addr.s_addr = clientIp;
    clientAddr.sin_port = 0;

    /* bind socket to client address */
    if (bind(sockFd, (struct sockaddr *)&clientAddr, sizeof(clientAddr)) < 0) {
      printf("bind tcp client socket failed, client(0x%x:0), dest(0x%x:%d), reason:(%s)", clientIp, destIp, destPort,
             strerror(errno));
      taosCloseSocket(sockFd);
      return -1;
    }
  }

  memset((char *)&serverAddr, 0, sizeof(serverAddr));
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_addr.s_addr = destIp;
  serverAddr.sin_port = (uint16_t)htons((uint16_t)destPort);

#ifdef _TD_LINUX 
  taosSetNonblocking(sockFd, 1);   
  ret = connect(sockFd, (struct sockaddr *)&serverAddr, sizeof(serverAddr)); 
  if (ret == -1) {
    if (errno == EHOSTUNREACH) {
      printf("failed to connect socket, ip:0x%x, port:%hu(%s)", destIp, destPort, strerror(errno));
      taosCloseSocket(sockFd);
      return -1; 
    } else if (errno == EINPROGRESS || errno == EAGAIN || errno == EWOULDBLOCK) {
      struct pollfd wfd[1]; 

      wfd[0].fd = sockFd;
      wfd[0].events = POLLOUT;
    
      int res = poll(wfd, 1, TCP_CONN_TIMEOUT);
      if (res == -1 || res == 0) {
        printf("failed to connect socket, ip:0x%x, port:%hu(poll error/conn timeout)", destIp, destPort);
        taosCloseSocket(sockFd); //  
        return -1;
      }
      int optVal = -1, optLen = sizeof(int);  
      if ((0 != taosGetSockOpt(sockFd, SOL_SOCKET, SO_ERROR, &optVal, &optLen)) || (optVal != 0)) {
        printf("failed to connect socket, ip:0x%x, port:%hu(connect host error)", destIp, destPort);
        taosCloseSocket(sockFd); //  
        return -1;
      }
      ret = 0;
    } else { // Other error
      printf("failed to connect socket, ip:0x%x, port:%hu(target host cannot be reached)", destIp, destPort);
      taosCloseSocket(sockFd); //  
      return -1; 
    } 
  }
  taosSetNonblocking(sockFd, 0);   

#else
  ret = connect(sockFd, (struct sockaddr *)&serverAddr, sizeof(serverAddr)); 
#endif

  if (ret != 0) {
    printf("failed to connect socket, ip:0x%x, port:%hu(%s)", destIp, destPort, strerror(errno));
    taosCloseSocket(sockFd);
    sockFd = -1;
  } else {
    taosKeepTcpAlive(sockFd);
  }

  return sockFd;
}

int32_t taosKeepTcpAlive(SOCKET sockFd) {
  int32_t alive = 1;
  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_KEEPALIVE, (void *)&alive, sizeof(alive)) < 0) {
    printf("fd:%d setsockopt SO_KEEPALIVE failed: %d (%s)", sockFd, errno, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }

#ifndef __APPLE__
  // all fails on macosx
  int32_t probes = 3;
  if (taosSetSockOpt(sockFd, SOL_TCP, TCP_KEEPCNT, (void *)&probes, sizeof(probes)) < 0) {
    printf("fd:%d setsockopt SO_KEEPCNT failed: %d (%s)", sockFd, errno, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }

  int32_t alivetime = 10;
  if (taosSetSockOpt(sockFd, SOL_TCP, TCP_KEEPIDLE, (void *)&alivetime, sizeof(alivetime)) < 0) {
    printf("fd:%d setsockopt SO_KEEPIDLE failed: %d (%s)", sockFd, errno, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }

  int32_t interval = 3;
  if (taosSetSockOpt(sockFd, SOL_TCP, TCP_KEEPINTVL, (void *)&interval, sizeof(interval)) < 0) {
    printf("fd:%d setsockopt SO_KEEPINTVL failed: %d (%s)", sockFd, errno, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }
#endif // __APPLE__

  int32_t nodelay = 1;
  if (taosSetSockOpt(sockFd, IPPROTO_TCP, TCP_NODELAY, (void *)&nodelay, sizeof(nodelay)) < 0) {
    printf("fd:%d setsockopt TCP_NODELAY failed %d (%s)", sockFd, errno, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }

  struct linger linger = {0};
  linger.l_onoff = 1;
  linger.l_linger = 3;
  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_LINGER, (void *)&linger, sizeof(linger)) < 0) {
    printf("setsockopt SO_LINGER failed: %d (%s)", errno, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }

  return 0;
}

SOCKET taosOpenTcpServerSocket(uint32_t ip, uint16_t port) {
  struct sockaddr_in serverAdd;
  SOCKET             sockFd;
  int32_t            reuse;

  printf("open tcp server socket:0x%x:%hu", ip, port);

  bzero((char *)&serverAdd, sizeof(serverAdd));
  serverAdd.sin_family = AF_INET;
  serverAdd.sin_addr.s_addr = ip;
  serverAdd.sin_port = (uint16_t)htons(port);

  if ((sockFd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) <= 2) {
    printf("failed to open TCP socket: %d (%s)", errno, strerror(errno));
    taosCloseSocketNoCheck(sockFd);
    return -1;
  }

  /* set REUSEADDR option, so the portnumber can be re-used */
  reuse = 1;
  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse, sizeof(reuse)) < 0) {
    printf("setsockopt SO_REUSEADDR failed: %d (%s)", errno, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }

  /* bind socket to server address */
  if (bind(sockFd, (struct sockaddr *)&serverAdd, sizeof(serverAdd)) < 0) {
    printf("bind tcp server socket failed, 0x%x:%hu(%s)", ip, port, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }

  if (taosKeepTcpAlive(sockFd) < 0) {
    printf("failed to set tcp server keep-alive option, 0x%x:%hu(%s)", ip, port, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }

  if (listen(sockFd, 1024) < 0) {
    printf("listen tcp server socket failed, 0x%x:%hu(%s)", ip, port, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }

  return sockFd;
}

void tinet_ntoa(char *ipstr, uint32_t ip) {
  sprintf(ipstr, "%d.%d.%d.%d", ip & 0xFF, (ip >> 8) & 0xFF, (ip >> 16) & 0xFF, ip >> 24);
}

#define COPY_SIZE 32768
// sendfile shall be used

int64_t taosCopyFds(SOCKET sfd, int32_t dfd, int64_t len) {
  int64_t leftLen;
  int64_t readLen, writeLen;
  char    temp[COPY_SIZE];

  leftLen = len;

  while (leftLen > 0) {
    if (leftLen < COPY_SIZE)
      readLen = leftLen;
    else
      readLen = COPY_SIZE;  // 4K

    int64_t retLen = taosReadMsg(sfd, temp, (int32_t)readLen);
    if (readLen != retLen) {
      printf("read error, readLen:%" PRId64 " retLen:%" PRId64 " len:%" PRId64 " leftLen:%" PRId64 ", reason:%s",
             readLen, retLen, len, leftLen, strerror(errno));
      return -1;
    }

    writeLen = taosWriteMsg(dfd, temp, (int32_t)readLen);

    if (readLen != writeLen) {
      printf("copy error, readLen:%" PRId64 " writeLen:%" PRId64 " len:%" PRId64 " leftLen:%" PRId64 ", reason:%s",
             readLen, writeLen, len, leftLen, strerror(errno));
      return -1;
    }

    leftLen -= readLen;
  }

  return len;
}
