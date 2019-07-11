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

#ifndef _taos_tcp_server_header_
#define _taos_tcp_server_header_

#include "tsdb.h"

void *taosInitTcpServer(char *ip, short port, char *label, int numOfThreads, void *fp, void *shandle);
void taosCleanUpTcpServer(void *param);
void taosCloseTcpServerConnection(void *param);
int taosSendTcpServerData(uint32_t ip, short port, char *data, int len, void *chandle);

#endif
