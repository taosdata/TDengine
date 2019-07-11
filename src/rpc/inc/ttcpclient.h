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

#ifndef _taos_tcp_client_header_
#define _taos_tcp_client_header_

#include "tsdb.h"

void *taosInitTcpClient(char *ip, short port, char *label, int num, void *fp, void *shandle);
void taosCleanUpTcpClient(void *chandle);
void *taosOpenTcpClientConnection(void *shandle, void *thandle, char *ip, short port);
void taosCloseTcpClientConnection(void *chandle);
int taosSendTcpClientData(uint32_t ip, short port, char *data, int len, void *chandle);

#endif
