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

#ifndef TDENGINE_OS_SOCKET_H
#define TDENGINE_OS_SOCKET_H

#ifdef __cplusplus
extern "C" {
#endif

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  #include "winsock2.h"
  #include <WS2tcpip.h>
  #include <winbase.h>
  #include <Winsock2.h>
#else
  #include <netinet/in.h>
  #include <netinet/ip.h>
  #include <netinet/tcp.h>
  #include <netinet/udp.h>
  #include <unistd.h>
#endif

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  #define taosSend(sockfd, buf, len, flags) send((SOCKET)sockfd, buf, len, flags)
  #define taosSendto(sockfd, buf, len, flags, dest_addr, addrlen) sendto((SOCKET)sockfd, buf, len, flags, dest_addr, addrlen)
  #define taosWriteSocket(fd, buf, len) send((SOCKET)fd, buf, len, 0)
  #define taosReadSocket(fd, buf, len) recv((SOCKET)fd, buf, len, 0)
  #define taosCloseSocketNoCheck(fd) closesocket((SOCKET)fd)
  #define taosCloseSocket(fd) closesocket((SOCKET)fd)
#else
  #define taosSend(sockfd, buf, len, flags) send(sockfd, buf, len, flags)
  #define taosSendto(sockfd, buf, len, flags, dest_addr, addrlen) sendto(sockfd, buf, len, flags, dest_addr, addrlen)
  #define taosReadSocket(fd, buf, len) read(fd, buf, len)
  #define taosWriteSocket(fd, buf, len) write(fd, buf, len)
  #define taosCloseSocketNoCheck(x) close(x)
  #define taosCloseSocket(x) \
    {                        \
      if ((x) > -1) {        \
        close(x);            \
        x = FD_INITIALIZER;  \
      }                      \
    }
#endif

#define TAOS_EPOLL_WAIT_TIME 500 
typedef int32_t SOCKET;
typedef SOCKET EpollFd;
#define EpollClose(pollFd) taosCloseSocket(pollFd)

void taosShutDownSocketRD(SOCKET fd);
void taosShutDownSocketWR(SOCKET fd);

int32_t taosSetNonblocking(SOCKET sock, int32_t on);
void    taosIgnSIGPIPE();
void    taosBlockSIGPIPE();
void    taosSetMaskSIGPIPE();
int32_t taosSetSockOpt(SOCKET socketfd, int32_t level, int32_t optname, void *optval, int32_t optlen);
int32_t taosGetSockOpt(SOCKET socketfd, int32_t level, int32_t optname, void *optval, int32_t* optlen);

uint32_t    taosInetAddr(char *ipAddr);
const char *taosInetNtoa(struct in_addr ipInt);

#if (defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)) 
  #define htobe64 htonll
  #if defined(_TD_GO_DLL_)
    uint64_t htonll(uint64_t val);
  #endif
#endif

#if defined(_TD_DARWIN_64)
  #define htobe64 htonll
#endif

#ifdef __cplusplus
}
#endif

#endif
