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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>

int taosNonblockwrite(int fd, char *ptr, int nbytes);

int taosReadn(int sock, char *buffer, int len);

int taosWriteMsg(int fd, void *ptr, int nbytes);

int taosReadMsg(int fd, void *ptr, int nbytes);

int taosOpenUdpSocket(char *ip, short port);

int taosOpenTcpClientSocket(char *ip, short port, char *localIp);

int taosOpenTcpServerSocket(char *ip, short port);

int taosKeepTcpAlive(int sockFd);

void taosCloseTcpSocket(int sockFd);

int taosOpenUDServerSocket(char *ip, short port);

int taosOpenUDClientSocket(char *ip, short port);

int taosOpenRawSocket(char *ip);

int taosCopyFds(int sfd, int dfd, int64_t len);

int taosGetPublicIp(char *const ip);

int taosGetPrivateIp(char *const ip);

void tinet_ntoa(char *ipstr, unsigned int ip);

int taosSetNonblocking(int sock, int on);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSOCKET_H
