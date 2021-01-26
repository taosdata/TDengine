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

#include "os.h"
#include "tulog.h"
#include "tsocket.h"
#include "taoserror.h"

#ifndef SIGPIPE
  #define SIGPIPE EPIPE
#endif

int32_t taosGetFqdn(char *fqdn) {
  char hostname[1024];
  hostname[1023] = '\0';
  if (gethostname(hostname, 1023) == -1) {
    uError("failed to get hostname, reason:%s", strerror(errno));
    return -1;
  }

  struct addrinfo  hints = {0};
  struct addrinfo *result = NULL;
  hints.ai_flags = AI_CANONNAME;
  int32_t ret = getaddrinfo(hostname, NULL, &hints, &result);
  if (!result) {
    uError("failed to get fqdn, code:%d, reason:%s", ret, gai_strerror(ret));
    return -1;
  }

  strcpy(fqdn, result->ai_canonname);
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
      uError("failed to get the ip address, fqdn:%s, since:%s", fqdn, strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
    } else {
      uError("failed to get the ip address, fqdn:%s, since:%s", fqdn, gai_strerror(ret));
    }
#else
    uError("failed to get the ip address, fqdn:%s, since:%s", fqdn, gai_strerror(ret));
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
      uError("fd %d timeout, no enough space to write", fd);
      break;

    } else if (nready < 0) {
      if (errno == EINTR) continue;

      uError("select error, %d (%s)", errno, strerror(errno));
      return -1;
    }

    nwritten = (int32_t)taosSend(fd, ptr, (size_t)nleft, MSG_NO_SIGNAL);
    if (nwritten <= 0) {
      if (errno == EAGAIN || errno == EINTR) continue;

      uError("write error, %d (%s)", errno, strerror(errno));
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
      uError("fd %d timeout\n", fd);
      break;
    } else if (nready < 0) {
      if (errno == EINTR) continue;
      uError("select error, %d (%s)", errno, strerror(errno));
      return -1;
    }

    if ((nread = (int32_t)taosReadSocket(fd, ptr, (size_t)nleft)) < 0) {
      if (errno == EINTR) continue;
      uError("read error, %d (%s)", errno, strerror(errno));
      return -1;

    } else if (nread == 0) {
      uError("fd %d EOF", fd);
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

  uDebug("open udp socket:0x%x:%hu", ip, port);

  memset((char *)&localAddr, 0, sizeof(localAddr));
  localAddr.sin_family = AF_INET;
  localAddr.sin_addr.s_addr = ip;
  localAddr.sin_port = (uint16_t)htons(port);

  if ((sockFd = socket(AF_INET, SOCK_DGRAM, 0)) <= 2) {
    uError("failed to open udp socket: %d (%s)", errno, strerror(errno));
    taosCloseSocketNoCheck(sockFd);
    return -1;
  }

  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_SNDBUF, (void *)&bufSize, sizeof(bufSize)) != 0) {
    uError("failed to set the send buffer size for UDP socket\n");
    taosCloseSocket(sockFd);
    return -1;
  }

  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_RCVBUF, (void *)&bufSize, sizeof(bufSize)) != 0) {
    uError("failed to set the receive buffer size for UDP socket\n");
    taosCloseSocket(sockFd);
    return -1;
  }

  /* bind socket to local address */
  if (bind(sockFd, (struct sockaddr *)&localAddr, sizeof(localAddr)) < 0) {
    uError("failed to bind udp socket: %d (%s), 0x%x:%hu", errno, strerror(errno), ip, port);
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
    uError("failed to open the socket: %d (%s)", errno, strerror(errno));
    taosCloseSocketNoCheck(sockFd);
    return -1;
  }

  /* set REUSEADDR option, so the portnumber can be re-used */
  int32_t reuse = 1;
  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse, sizeof(reuse)) < 0) {
    uError("setsockopt SO_REUSEADDR failed: %d (%s)", errno, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }

  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_SNDBUF, (void *)&bufSize, sizeof(bufSize)) != 0) {
    uError("failed to set the send buffer size for TCP socket\n");
    taosCloseSocket(sockFd);
    return -1;
  }

  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_RCVBUF, (void *)&bufSize, sizeof(bufSize)) != 0) {
    uError("failed to set the receive buffer size for TCP socket\n");
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
      uError("bind tcp client socket failed, client(0x%x:0), dest(0x%x:%d), reason:(%s)", clientIp, destIp, destPort,
             strerror(errno));
      taosCloseSocket(sockFd);
      return -1;
    }
  }

  memset((char *)&serverAddr, 0, sizeof(serverAddr));
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_addr.s_addr = destIp;
  serverAddr.sin_port = (uint16_t)htons((uint16_t)destPort);

  ret = connect(sockFd, (struct sockaddr *)&serverAddr, sizeof(serverAddr));

  if (ret != 0) {
    // uError("failed to connect socket, ip:0x%x, port:%hu(%s)", destIp, destPort, strerror(errno));
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
    uError("fd:%d setsockopt SO_KEEPALIVE failed: %d (%s)", sockFd, errno, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }

#ifndef __APPLE__
  // all fails on macosx
  int32_t probes = 3;
  if (taosSetSockOpt(sockFd, SOL_TCP, TCP_KEEPCNT, (void *)&probes, sizeof(probes)) < 0) {
    uError("fd:%d setsockopt SO_KEEPCNT failed: %d (%s)", sockFd, errno, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }

  int32_t alivetime = 10;
  if (taosSetSockOpt(sockFd, SOL_TCP, TCP_KEEPIDLE, (void *)&alivetime, sizeof(alivetime)) < 0) {
    uError("fd:%d setsockopt SO_KEEPIDLE failed: %d (%s)", sockFd, errno, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }

  int32_t interval = 3;
  if (taosSetSockOpt(sockFd, SOL_TCP, TCP_KEEPINTVL, (void *)&interval, sizeof(interval)) < 0) {
    uError("fd:%d setsockopt SO_KEEPINTVL failed: %d (%s)", sockFd, errno, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }
#endif // __APPLE__

  int32_t nodelay = 1;
  if (taosSetSockOpt(sockFd, IPPROTO_TCP, TCP_NODELAY, (void *)&nodelay, sizeof(nodelay)) < 0) {
    uError("fd:%d setsockopt TCP_NODELAY failed %d (%s)", sockFd, errno, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }

  struct linger linger = {0};
  linger.l_onoff = 1;
  linger.l_linger = 3;
  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_LINGER, (void *)&linger, sizeof(linger)) < 0) {
    uError("setsockopt SO_LINGER failed: %d (%s)", errno, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }

  return 0;
}

SOCKET taosOpenTcpServerSocket(uint32_t ip, uint16_t port) {
  struct sockaddr_in serverAdd;
  SOCKET             sockFd;
  int32_t            reuse;

  uDebug("open tcp server socket:0x%x:%hu", ip, port);

  bzero((char *)&serverAdd, sizeof(serverAdd));
  serverAdd.sin_family = AF_INET;
  serverAdd.sin_addr.s_addr = ip;
  serverAdd.sin_port = (uint16_t)htons(port);

  if ((sockFd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) <= 2) {
    uError("failed to open TCP socket: %d (%s)", errno, strerror(errno));
    taosCloseSocketNoCheck(sockFd);
    return -1;
  }

  /* set REUSEADDR option, so the portnumber can be re-used */
  reuse = 1;
  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse, sizeof(reuse)) < 0) {
    uError("setsockopt SO_REUSEADDR failed: %d (%s)", errno, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }

  /* bind socket to server address */
  if (bind(sockFd, (struct sockaddr *)&serverAdd, sizeof(serverAdd)) < 0) {
    uError("bind tcp server socket failed, 0x%x:%hu(%s)", ip, port, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }

  if (taosKeepTcpAlive(sockFd) < 0) {
    uError("failed to set tcp server keep-alive option, 0x%x:%hu(%s)", ip, port, strerror(errno));
    taosCloseSocket(sockFd);
    return -1;
  }

  if (listen(sockFd, 10) < 0) {
    uError("listen tcp server socket failed, 0x%x:%hu(%s)", ip, port, strerror(errno));
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

int32_t taosCopyFds(SOCKET sfd, int32_t dfd, int64_t len) {
  int64_t leftLen;
  int32_t readLen, writeLen;
  char    temp[COPY_SIZE];

  leftLen = len;

  while (leftLen > 0) {
    if (leftLen < COPY_SIZE)
      readLen = (int32_t)leftLen;
    else
      readLen = COPY_SIZE;  // 4K

    int32_t retLen = taosReadMsg(sfd, temp, (int32_t)readLen);
    if (readLen != retLen) {
      uError("read error, readLen:%d retLen:%d len:%" PRId64 " leftLen:%" PRId64 ", reason:%s", readLen, retLen, len,
             leftLen, strerror(errno));
      return -1;
    }

    writeLen = taosWriteMsg(dfd, temp, readLen);

    if (readLen != writeLen) {
      uError("copy error, readLen:%d writeLen:%d len:%" PRId64 " leftLen:%" PRId64 ", reason:%s", readLen, writeLen,
             len, leftLen, strerror(errno));
      return -1;
    }

    leftLen -= readLen;
  }

  return 0;
}
