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

#ifndef _TD_OS_SOCKET_H_
#define _TD_OS_SOCKET_H_

// If the error is in a third-party library, place this header file under the third-party library header file.
// When you want to use this feature, you should find or add the same function in the following section.
#ifndef ALLOW_FORBID_FUNC
#define socket       SOCKET_FUNC_TAOS_FORBID
#define bind         BIND_FUNC_TAOS_FORBID
#define listen       LISTEN_FUNC_TAOS_FORBID
#define accept       ACCEPT_FUNC_TAOS_FORBID
#define epoll_create EPOLL_CREATE_FUNC_TAOS_FORBID
#define epoll_ctl    EPOLL_CTL_FUNC_TAOS_FORBID
#define epoll_wait   EPOLL_WAIT_FUNC_TAOS_FORBID
#define inet_addr    INET_ADDR_FUNC_TAOS_FORBID
#define inet_ntoa    INET_NTOA_FUNC_TAOS_FORBID
#endif

#if defined(WINDOWS)
#if BYTE_ORDER == LITTLE_ENDIAN
#include <stdlib.h>
#define htobe16(x) _byteswap_ushort(x)
#define htole16(x) (x)
#define be16toh(x) _byteswap_ushort(x)
#define le16toh(x) (x)

#define htobe32(x) _byteswap_ulong(x)
#define htole32(x) (x)
#define be32toh(x) _byteswap_ulong(x)
#define le32toh(x) (x)

#define htobe64(x) _byteswap_uint64(x)
#define htole64(x) (x)
#define be64toh(x) _byteswap_uint64(x)
#define le64toh(x) (x)
#else
#error byte order not supported
#endif

#define __BYTE_ORDER    BYTE_ORDER
#define __BIG_ENDIAN    BIG_ENDIAN
#define __LITTLE_ENDIAN LITTLE_ENDIAN
#define __PDP_ENDIAN    PDP_ENDIAN

#else
#include <netinet/in.h>
#include <sys/socket.h>

#if defined(_TD_DARWIN_64)
#include <osEok.h>
#else
#include <netinet/in.h>
#endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

#if defined(WINDOWS)
typedef int socklen_t;
#define TAOS_EPOLL_WAIT_TIME 100
typedef SOCKET eventfd_t;
#define eventfd(a, b) -1
#ifndef EPOLLWAKEUP
#define EPOLLWAKEUP (1u << 29)
#endif
#elif defined(_TD_DARWIN_64)
#define TAOS_EPOLL_WAIT_TIME 500
typedef int32_t SOCKET;
#else
#define TAOS_EPOLL_WAIT_TIME 500
typedef int32_t SOCKET;
typedef SOCKET  EpollFd;
#define EpollClose(pollFd)   taosCloseSocket(pollFd)
#endif

#if defined(_TD_DARWIN_64)
//  #define htobe64 htonll

#include <libkern/OSByteOrder.h>

#define htobe16(x) OSSwapHostToBigInt16(x)
#define htole16(x) OSSwapHostToLittleInt16(x)
#define be16toh(x) OSSwapBigToHostInt16(x)
#define le16toh(x) OSSwapLittleToHostInt16(x)

#define htobe32(x) OSSwapHostToBigInt32(x)
#define htole32(x) OSSwapHostToLittleInt32(x)
#define be32toh(x) OSSwapBigToHostInt32(x)
#define le32toh(x) OSSwapLittleToHostInt32(x)

#define htobe64(x) OSSwapHostToBigInt64(x)
#define htole64(x) OSSwapHostToLittleInt64(x)
#define be64toh(x) OSSwapBigToHostInt64(x)
#define le64toh(x) OSSwapLittleToHostInt64(x)

#define __BYTE_ORDER    BYTE_ORDER
#define __BIG_ENDIAN    BIG_ENDIAN
#define __LITTLE_ENDIAN LITTLE_ENDIAN
#define __PDP_ENDIAN    PDP_ENDIAN
#endif

typedef int32_t  SocketFd;
typedef SocketFd EpollFd;

typedef struct TdSocketServer *TdSocketServerPtr;
typedef struct TdSocket       *TdSocketPtr;
typedef struct TdEpoll        *TdEpollPtr;

int32_t taosSendto(TdSocketPtr pSocket, void *msg, int len, unsigned int flags, const struct sockaddr *to, int tolen);
int32_t taosWriteSocket(TdSocketPtr pSocket, void *msg, int len);
int32_t taosReadSocket(TdSocketPtr pSocket, void *msg, int len);
int32_t taosReadFromSocket(TdSocketPtr pSocket, void *buf, int32_t len, int32_t flags, struct sockaddr *destAddr,
                           int *addrLen);
int32_t taosCloseSocketNoCheck1(SocketFd fd);
int32_t taosCloseSocket(TdSocketPtr *ppSocket);
int32_t taosCloseSocketServer(TdSocketServerPtr *ppSocketServer);
int32_t taosShutDownSocketRD(TdSocketPtr pSocket);
int32_t taosShutDownSocketServerRD(TdSocketServerPtr pSocketServer);
int32_t taosShutDownSocketWR(TdSocketPtr pSocket);
int32_t taosShutDownSocketServerWR(TdSocketServerPtr pSocketServer);
int32_t taosShutDownSocketRDWR(TdSocketPtr pSocket);
int32_t taosShutDownSocketServerRDWR(TdSocketServerPtr pSocketServer);
int32_t taosSetNonblocking(TdSocketPtr pSocket, int32_t on);
int32_t taosSetSockOpt(TdSocketPtr pSocket, int32_t level, int32_t optname, void *optval, int32_t optlen);
int32_t taosGetSockOpt(TdSocketPtr pSocket, int32_t level, int32_t optname, void *optval, int32_t *optlen);
int32_t taosWriteMsg(TdSocketPtr pSocket, void *ptr, int32_t nbytes);
int32_t taosReadMsg(TdSocketPtr pSocket, void *ptr, int32_t nbytes);
int32_t taosNonblockwrite(TdSocketPtr pSocket, char *ptr, int32_t nbytes);
int64_t taosCopyFds(TdSocketPtr pSrcSocket, TdSocketPtr pDestSocket, int64_t len);
void    taosWinSocketInit();

/*
 * set timeout(ms)
 */
int32_t taosCreateSocketWithTimeout(uint32_t timeout);

TdSocketPtr       taosOpenUdpSocket(uint32_t localIp, uint16_t localPort);
TdSocketPtr       taosOpenTcpClientSocket(uint32_t ip, uint16_t port, uint32_t localIp);
bool              taosValidIpAndPort(uint32_t ip, uint16_t port);
TdSocketServerPtr taosOpenTcpServerSocket(uint32_t ip, uint16_t port);
int32_t           taosKeepTcpAlive(TdSocketPtr pSocket);
TdSocketPtr       taosAcceptTcpConnectSocket(TdSocketServerPtr pServerSocket, struct sockaddr *destAddr, int *addrLen);

int32_t taosGetSocketName(TdSocketPtr pSocket, struct sockaddr *destAddr, int *addrLen);

void        taosBlockSIGPIPE();
uint32_t    taosGetIpv4FromFqdn(const char *);
int32_t     taosGetFqdn(char *);
void        tinet_ntoa(char *ipstr, uint32_t ip);
uint32_t    ip2uint(const char *const ip_addr);
void        taosIgnSIGPIPE();
void        taosSetMaskSIGPIPE();
uint32_t    taosInetAddr(const char *ipAddr);
const char *taosInetNtoa(struct in_addr ipInt, char *dstStr, int32_t len);

uint64_t taosHton64(uint64_t val);
uint64_t taosNtoh64(uint64_t val);

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_SOCKET_H_*/
