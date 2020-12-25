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

#ifndef TAOS_OS_FUNC_SOCKET_OP
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
  typedef int32_t SOCKET;
#endif

#ifndef TAOS_OS_DEF_EPOLL
  #define TAOS_EPOLL_WAIT_TIME 500 
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

// TAOS_OS_FUNC_SOCKET
int32_t taosSetNonblocking(SOCKET sock, int32_t on);
void    taosIgnSIGPIPE();
void    taosBlockSIGPIPE();

// TAOS_OS_FUNC_SOCKET_SETSOCKETOPT
int32_t taosSetSockOpt(SOCKET socketfd, int32_t level, int32_t optname, void *optval, int32_t optlen);

// TAOS_OS_FUNC_SOCKET_INET
uint32_t    taosInetAddr(char *ipAddr);
const char *taosInetNtoa(struct in_addr ipInt);

#ifdef __cplusplus
}
#endif

#endif
