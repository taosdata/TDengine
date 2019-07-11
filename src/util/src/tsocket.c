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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <ctype.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdarg.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include "tglobalcfg.h"
#include "tlog.h"
#include "tsocket.h"
#include "tutil.h"

unsigned int ip2uint(const char *const ip_addr);
int taosSetNonblocking(int sock, int on);
int taosSetSockOpt(int socketfd, int level, int optname, void *optval, int optlen);

/*
 * Function to get the public ip address of current machine. If get IP
 * successfully, return 0, else, return -1. The return values is ip.
 *
 * Use:
 * if (taosGetPublicIp(ip) != 0) {
 *     perror("Fail to get public IP address\n");
 *     exit(EXIT_FAILURE);
 * }
 */
int taosGetPublicIp(char *const ip) {
  /* bool flag; */
  int                flag;
  int                sock;
  char **            pptr = NULL;
  struct sockaddr_in destAddr;
  struct hostent *   ptr = NULL;
  char               destIP[128];
  char               szBuffer[] = {
      "GET / HTTP/1.1\nHost: ident.me\nUser-Agent: curl/7.47.0\nAccept: "
      "*/*\n\n"};
  char res[1024];

  // Create socket
  sock = (int)socket(AF_INET, SOCK_STREAM, 0);
  if (sock == -1) {
    return -1;
  }

  bzero((void *)&destAddr, sizeof(destAddr));
  destAddr.sin_family = AF_INET;
  destAddr.sin_port = htons(80);

  ptr = gethostbyname("ident.me");
  if (ptr == NULL) {
    return -1;
  }

  // Loop to find a valid IP address
  for (flag = 0, pptr = ptr->h_addr_list; NULL != *pptr; ++pptr) {
    inet_ntop(ptr->h_addrtype, *pptr, destIP, sizeof(destIP));
    destAddr.sin_addr.s_addr = inet_addr(destIP);
    if (connect(sock, (struct sockaddr *)&destAddr, sizeof(struct sockaddr)) != -1) {
      flag = 1;
      break;
    }
  }

  // Check if the host is available.
  if (flag == 0) {
    return -1;
  }

  // Check send.
  if (strlen(szBuffer) != taosWriteSocket(sock, szBuffer, (size_t)strlen(szBuffer))) {
    return -1;
  }

  // Receive response.
  if (taosReadSocket(sock, res, 1024) == -1) {
    return -1;
  }

  // Extract the IP address from the response.
  int c_start = 0, c_end = 0;
  for (; c_start < (int)strlen(res); c_start = c_end + 1) {
    for (c_end = c_start; c_end < (int)strlen(res) && res[c_end] != '\n'; c_end++) {
    }

    if (c_end >= (int)strlen(res)) {
      return -1;
    }

    if (res[c_start] >= '0' && res[c_start] <= '9') {
      strncpy(ip, res + c_start, (size_t)(c_end - c_start));
      ip[c_end - c_start] = '\0';
      break;
    }
  }

  return 0;
}

// Function converting an IP address string to an unsigned int.
unsigned int ip2uint(const char *const ip_addr) {
  char ip_addr_cpy[20];
  char ip[5];

  strcpy(ip_addr_cpy, ip_addr);

  char *s_start, *s_end;
  s_start = ip_addr_cpy;
  s_end = ip_addr_cpy;

  int k;

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

  return *((unsigned int *)ip);
}

int taosWriteMsg(int fd, void *buf, int nbytes) {
  int   nleft, nwritten;
  char *ptr = (char *)buf;

  nleft = nbytes;

  while (nleft > 0) {
    nwritten = (int)taosWriteSocket(fd, (char *)ptr, (size_t)nleft);
    if (nwritten <= 0) {
      if (errno == EINTR)
        continue;
      else
        return -1;
    } else {
      nleft -= nwritten;
      ptr += nwritten;
    }
  }

  return (nbytes - nleft);
}

int taosReadMsg(int fd, void *buf, int nbytes) {
  int   nleft, nread;
  char *ptr = (char *)buf;

  nleft = nbytes;

  if (fd < 0) return -1;

  while (nleft > 0) {
    nread = (int)taosReadSocket(fd, ptr, (size_t)nleft);
    if (nread == 0) {
      break;
    } else if (nread < 0) {
      if (errno == EINTR) {
        continue;
      } else {
        return -1;
      }
    } else {
      nleft -= nread;
      ptr += nread;
    }
  }

  return (nbytes - nleft);
}

int taosNonblockwrite(int fd, char *ptr, int nbytes) {
  taosSetNonblocking(fd, 1);

  int            nleft, nwritten, nready;
  fd_set         fset;
  struct timeval tv;

  nleft = nbytes;
  while (nleft > 0) {
    tv.tv_sec = 30;
    tv.tv_usec = 0;
    FD_ZERO(&fset);
    FD_SET(fd, &fset);
    if ((nready = select(fd + 1, NULL, &fset, NULL, &tv)) == 0) {
      errno = ETIMEDOUT;
      pError("fd %d timeout, no enough space to write", fd);
      break;

    } else if (nready < 0) {
      if (errno == EINTR) continue;

      pError("select error, %d (%s)", errno, strerror(errno));
      return -1;
    }

    nwritten = (int)send(fd, ptr, (size_t)nleft, MSG_NOSIGNAL);
    if (nwritten <= 0) {
      if (errno == EAGAIN || errno == EINTR) continue;

      pError("write error, %d (%s)", errno, strerror(errno));
      return -1;
    }

    nleft -= nwritten;
    ptr += nwritten;
  }

  taosSetNonblocking(fd, 0);

  return (nbytes - nleft);
}

int taosReadn(int fd, char *ptr, int nbytes) {
  int nread, nready, nleft = nbytes;

  fd_set         fset;
  struct timeval tv;

  while (nleft > 0) {
    tv.tv_sec = 30;
    tv.tv_usec = 0;
    FD_ZERO(&fset);
    FD_SET(fd, &fset);
    if ((nready = select(fd + 1, NULL, &fset, NULL, &tv)) == 0) {
      errno = ETIMEDOUT;
      pError("fd %d timeout\n", fd);
      break;
    } else if (nready < 0) {
      if (errno == EINTR) continue;
      pError("select error, %d (%s)", errno, strerror(errno));
      return -1;
    }

    if ((nread = (int)taosReadSocket(fd, ptr, (size_t)nleft)) < 0) {
      if (errno == EINTR) continue;
      pError("read error, %d (%s)", errno, strerror(errno));
      return -1;

    } else if (nread == 0) {
      pError("fd %d EOF", fd);
      break;  // EOF
    }

    nleft -= nread;
    ptr += nread;
  }

  return (nbytes - nleft);
}

int taosOpenUdpSocket(char *ip, short port) {
  struct sockaddr_in localAddr;
  int                sockFd;
  int                ttl = 128;
  int                reuse, nocheck;
  int                bufSize = 8192000;

  pTrace("open udp socket:%s:%d", ip, port);
  // if (tsAllowLocalhost) ip = "0.0.0.0";

  memset((char *)&localAddr, 0, sizeof(localAddr));
  localAddr.sin_family = AF_INET;
  localAddr.sin_addr.s_addr = inet_addr(ip);
  localAddr.sin_port = (uint16_t)htons((uint16_t)port);

  if ((sockFd = (int)socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    pError("failed to open udp socket: %d (%s)", errno, strerror(errno));
    return -1;
  }

  reuse = 1;
  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse, sizeof(reuse)) < 0) {
    pError("setsockopt SO_REUSEADDR failed): %d (%s)", errno, strerror(errno));
    close(sockFd);
    return -1;
  };

  nocheck = 1;
  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_NO_CHECK, (void *)&nocheck, sizeof(nocheck)) < 0) {
    pError("setsockopt SO_NO_CHECK failed: %d (%s)", errno, strerror(errno));
    close(sockFd);
    return -1;
  }

  ttl = 128;
  if (taosSetSockOpt(sockFd, IPPROTO_IP, IP_TTL, (void *)&ttl, sizeof(ttl)) < 0) {
    pError("setsockopt IP_TTL failed: %d (%s)", errno, strerror(errno));
    close(sockFd);
    return -1;
  }

  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_SNDBUF, (void *)&bufSize, sizeof(bufSize)) != 0) {
    pError("failed to set the send buffer size for UDP socket\n");
    close(sockFd);
    return -1;
  }

  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_RCVBUF, (void *)&bufSize, sizeof(bufSize)) != 0) {
    pError("failed to set the receive buffer size for UDP socket\n");
    close(sockFd);
    return -1;
  }

  /* bind socket to local address */
  if (bind(sockFd, (struct sockaddr *)&localAddr, sizeof(localAddr)) < 0) {
    pError("failed to bind udp socket: %d (%s), %s:%d", errno, strerror(errno), ip, port);
    taosCloseSocket(sockFd);
    return -1;
  }

  return sockFd;
}

int taosOpenTcpClientSocket(char *destIp, short destPort, char *clientIp) {
  int                sockFd = 0;
  struct sockaddr_in serverAddr, clientAddr;
  int                ret;

  pTrace("open tcp client socket:%s:%d", destIp, destPort);
  // if (tsAllowLocalhost) destIp = "0.0.0.0";

  sockFd = (int)socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);

  if (sockFd < 0) {
    pError("failed to open the socket: %d (%s)", errno, strerror(errno));
    return -1;
  }

  if (clientIp && clientIp[0] && clientIp[0] != '0') {
    memset((char *)&clientAddr, 0, sizeof(clientAddr));
    clientAddr.sin_family = AF_INET;
    clientAddr.sin_addr.s_addr = inet_addr(clientIp);
    clientAddr.sin_port = 0;

    /* bind socket to client address */
    if (bind(sockFd, (struct sockaddr *)&clientAddr, sizeof(clientAddr)) < 0) {
      pError("bind tcp client socket failed, client(%s:0), dest(%s:%d), reason:%d(%s)",
             clientIp, destIp, destPort, errno, strerror(errno));
      close(sockFd);
      return -1;
    }
  }

  memset((char *)&serverAddr, 0, sizeof(serverAddr));
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_addr.s_addr = inet_addr(destIp);
  serverAddr.sin_port = (uint16_t)htons((uint16_t)destPort);

  ret = connect(sockFd, (struct sockaddr *)&serverAddr, sizeof(serverAddr));

  if (ret != 0) {
    pError("failed to connect socket, ip:%s, port:%d, reason: %s", destIp, destPort, strerror(errno));
    taosCloseSocket(sockFd);
    sockFd = -1;
  }

  return sockFd;
}

void taosCloseTcpSocket(int sockFd) {
  struct linger linger;
  linger.l_onoff = 1;
  linger.l_linger = 0;
  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_LINGER, (void *)&linger, sizeof(linger)) < 0) {
    pError("setsockopt SO_LINGER failed: %d (%s)", errno, strerror(errno));
  }

  taosCloseSocket(sockFd);
}

int taosKeepTcpAlive(int sockFd) {
  int alive = 1;
  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_KEEPALIVE, (void *)&alive, sizeof(alive)) < 0) {
    pError("fd:%d setsockopt SO_KEEPALIVE failed: %d (%s)", sockFd, errno, strerror(errno));
    close(sockFd);
    return -1;
  }

  int probes = 3;
  if (taosSetSockOpt(sockFd, SOL_TCP, TCP_KEEPCNT, (void *)&probes, sizeof(probes)) < 0) {
    pError("fd:%d setsockopt SO_KEEPCNT failed: %d (%s)", sockFd, errno, strerror(errno));
    close(sockFd);
    return -1;
  }

  int alivetime = 10;
  if (taosSetSockOpt(sockFd, SOL_TCP, TCP_KEEPIDLE, (void *)&alivetime, sizeof(alivetime)) < 0) {
    pError("fd:%d setsockopt SO_KEEPIDLE failed: %d (%s)", sockFd, errno, strerror(errno));
    close(sockFd);
    return -1;
  }

  int interval = 3;
  if (taosSetSockOpt(sockFd, SOL_TCP, TCP_KEEPINTVL, (void *)&interval, sizeof(interval)) < 0) {
    pError("fd:%d setsockopt SO_KEEPINTVL failed: %d (%s)", sockFd, errno, strerror(errno));
    close(sockFd);
    return -1;
  }

  int nodelay = 1;
  if (taosSetSockOpt(sockFd, IPPROTO_TCP, TCP_NODELAY, (void *)&nodelay, sizeof(nodelay)) < 0) {
    pError("fd:%d setsockopt TCP_NODELAY failed %d (%s)", sockFd, errno, strerror(errno));
    close(sockFd);
    return -1;
  }

  return 0;
}

int taosOpenTcpServerSocket(char *ip, short port) {
  struct sockaddr_in serverAdd;
  int                sockFd;
  int                reuse;

  pTrace("open tcp server socket:%s:%d", ip, port);
  // if (tsAllowLocalhost) ip = "0.0.0.0";

  bzero((char *)&serverAdd, sizeof(serverAdd));
  serverAdd.sin_family = AF_INET;
  serverAdd.sin_addr.s_addr = inet_addr(ip);
  serverAdd.sin_port = (uint16_t)htons((uint16_t)port);

  if ((sockFd = (int)socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
    pError("failed to open TCP socket: %d (%s)", errno, strerror(errno));
    return -1;
  }

  /* set REUSEADDR option, so the portnumber can be re-used */
  reuse = 1;
  if (taosSetSockOpt(sockFd, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse, sizeof(reuse)) < 0) {
    pError("setsockopt SO_REUSEADDR failed: %d (%s)", errno, strerror(errno));
    close(sockFd);
    return -1;
  };

  /* bind socket to server address */
  if (bind(sockFd, (struct sockaddr *)&serverAdd, sizeof(serverAdd)) < 0) {
    pError("bind tcp server socket failed, %s:%d, reason:%d(%s)", ip, port, errno, strerror(errno));
    close(sockFd);
    return -1;
  }

  if (taosKeepTcpAlive(sockFd) < 0) return -1;

  if (listen(sockFd, 10) < 0) {
    pError("listen tcp server socket failed, %s:%d, reason:%d(%s)", ip, port, errno, strerror(errno));
    return -1;
  }

  return sockFd;
}

int taosOpenRawSocket(char *ip) {
  int                fd, hold;
  struct sockaddr_in rawAdd;

  pTrace("open udp raw socket:%s", ip);
  // if (tsAllowLocalhost) ip = "0.0.0.0";

  fd = (int)socket(AF_INET, SOCK_RAW, IPPROTO_UDP);
  if (fd < 0) {
    pError("failed to open raw socket: %d (%s)", errno, strerror(errno));
    return -1;
  }

  hold = 1;
  if (taosSetSockOpt(fd, IPPROTO_IP, IP_HDRINCL, (void *)&hold, sizeof(hold)) < 0) {
    pError("failed to set hold option: %d (%s)", errno, strerror(errno));
    close(fd);
    return -1;
  }

  bzero((char *)&rawAdd, sizeof(rawAdd));
  rawAdd.sin_family = AF_INET;
  rawAdd.sin_addr.s_addr = inet_addr(ip);

  if (bind(fd, (struct sockaddr *)&rawAdd, sizeof(rawAdd)) < 0) {
    pError("failed to bind RAW socket: %d (%s)", errno, strerror(errno));
    close(fd);
    return -1;
  }

  return fd;
}

void tinet_ntoa(char *ipstr, unsigned int ip) {
  sprintf(ipstr, "%d.%d.%d.%d", ip & 0xFF, (ip >> 8) & 0xFF, (ip >> 16) & 0xFF, ip >> 24);
}

#define COPY_SIZE 32768
// sendfile shall be used

int taosCopyFds(int sfd, int dfd, int64_t len) {
  int64_t leftLen;
  int     readLen, writeLen;
  char    temp[COPY_SIZE];

  leftLen = len;

  while (leftLen > 0) {
    if (leftLen < COPY_SIZE)
      readLen = (int)leftLen;
    else
      readLen = COPY_SIZE;  // 4K

    int retLen = taosReadMsg(sfd, temp, (int)readLen);
    if (readLen != retLen) {
      pError("read error, readLen:%d retLen:%d len:%ld leftLen:%ld, reason:%s", readLen, retLen, len, leftLen,
             strerror(errno));
      return -1;
    }

    writeLen = taosWriteMsg(dfd, temp, readLen);

    if (readLen != writeLen) {
      pError("copy error, readLen:%d writeLen:%d len:%ld leftLen:%ld, reason:%s", readLen, writeLen, len, leftLen,
             strerror(errno));
      return -1;
    }

    leftLen -= readLen;
  }

  return 0;
}
