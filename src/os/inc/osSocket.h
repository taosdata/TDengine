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
      if (FD_VALID(x)) {     \
        close(x);            \
        x = FD_INITIALIZER;  \
      }                      \
    }
#endif

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  #define TAOS_EPOLL_WAIT_TIME 100
  typedef SOCKET eventfd_t; 
  #define eventfd(a, b) -1
  typedef SOCKET EpollFd; 
  #define EpollClose(pollFd) epoll_close(pollFd) 
  #ifndef EPOLLWAKEUP
    #define EPOLLWAKEUP (1u << 29)
  #endif
#elif defined(_TD_DARWIN_64)
  #define TAOS_EPOLL_WAIT_TIME 500
  typedef int32_t SOCKET;
  typedef SOCKET EpollFd;
  #define EpollClose(pollFd) epoll_close(pollFd)
#else
  #define TAOS_EPOLL_WAIT_TIME 500 
  typedef int32_t SOCKET;
  typedef SOCKET EpollFd;
  #define EpollClose(pollFd) taosCloseSocket(pollFd)
#endif  

#ifdef TAOS_RANDOM_NETWORK_FAIL
  #ifdef TAOS_RANDOM_NETWORK_FAIL_TEST
    int64_t taosSendRandomFail(int32_t sockfd, const void *buf, size_t len, int32_t flags);
    int64_t taosSendToRandomFail(int32_t sockfd, const void *buf, size_t len, int32_t flags, const struct sockaddr *dest_addr, socklen_t addrlen);
    int64_t taosReadSocketRandomFail(int32_t fd, void *buf, size_t count);
    int64_t taosWriteSocketRandomFail(int32_t fd, const void *buf, size_t count);
    #undef taosSend
    #undef taosSendto
    #undef taosReadSocket
    #undef taosWriteSocket
    #define taosSend(sockfd, buf, len, flags) taosSendRandomFail(sockfd, buf, len, flags)
    #define taosSendto(sockfd, buf, len, flags, dest_addr, addrlen) taosSendToRandomFail(sockfd, buf, len, flags, dest_addr, addrlen)
    #define taosReadSocket(fd, buf, len) taosReadSocketRandomFail(fd, buf, len)
    #define taosWriteSocket(fd, buf, len) taosWriteSocketRandomFail(fd, buf, len)
  #endif  
#endif

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
