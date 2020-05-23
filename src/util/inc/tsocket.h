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

int taosReadn(int sock, char *buffer, int len);
int taosWriteMsg(int fd, void *ptr, int nbytes);
int taosReadMsg(int fd, void *ptr, int nbytes);
int taosNonblockwrite(int fd, char *ptr, int nbytes);
int taosCopyFds(int sfd, int dfd, int64_t len);
int taosSetNonblocking(int sock, int on);

int  taosOpenUdpSocket(uint32_t localIp, uint16_t localPort);
int  taosOpenTcpClientSocket(uint32_t ip, uint16_t port, uint32_t localIp);
int  taosOpenTcpServerSocket(uint32_t ip, uint16_t port);
int  taosKeepTcpAlive(int sockFd);

int      taosGetFqdn(char *);
uint32_t taosGetIpFromFqdn(const char *);
void     tinet_ntoa(char *ipstr, unsigned int ip);
uint32_t ip2uint(const char *const ip_addr);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSOCKET_H
