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

#ifndef TDENGINE_TSOCKET_H
#define TDENGINE_TSOCKET_H

#ifdef __cplusplus
extern "C" {
#endif

#ifdef WINDOWS
#include "wepoll.h"
#endif

int32_t taosReadn(SOCKET sock, char *buffer, int32_t len);
int32_t taosWriteMsg(SOCKET fd, void *ptr, int32_t nbytes);
int32_t taosReadMsg(SOCKET fd, void *ptr, int32_t nbytes);
int32_t taosNonblockwrite(SOCKET fd, char *ptr, int32_t nbytes);
int64_t taosCopyFds(SOCKET sfd, int32_t dfd, int64_t len);
int32_t taosSetNonblocking(SOCKET sock, int32_t on);

SOCKET  taosOpenUdpSocket(uint32_t localIp, uint16_t localPort);
SOCKET  taosOpenTcpClientSocket(uint32_t ip, uint16_t port, uint32_t localIp);
SOCKET  taosOpenTcpServerSocket(uint32_t ip, uint16_t port);
int32_t taosKeepTcpAlive(SOCKET sockFd);

int32_t  taosGetFqdn(char *);
uint32_t taosGetIpv4FromFqdn(const char *);
void     tinet_ntoa(char *ipstr, uint32_t ip);
uint32_t ip2uint(const char *const ip_addr);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSOCKET_H
