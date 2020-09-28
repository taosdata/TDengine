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

#ifndef _rpc_tcp_header_
#define _rpc_tcp_header_

#ifdef __cplusplus
extern "C" {
#endif

void *taosInitTcpServer(uint32_t ip, uint16_t port, char *label, int numOfThreads, void *fp, void *shandle);
void taosStopTcpServer(void *param);
void taosCleanUpTcpServer(void *param);

void *taosInitTcpClient(uint32_t ip, uint16_t port, char *label, int num, void *fp, void *shandle);
void taosStopTcpClient(void *chandle);
void taosCleanUpTcpClient(void *chandle);
void *taosOpenTcpClientConnection(void *shandle, void *thandle, uint32_t ip, uint16_t port);

void taosCloseTcpConnection(void *chandle);
int  taosSendTcpData(uint32_t ip, uint16_t port, void *data, int len, void *chandle);

#ifdef __cplusplus
}
#endif

#endif
