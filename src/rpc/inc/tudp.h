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

#ifndef _taos_udp_header_
#define _taos_udp_header_

#include "tsdb.h"

void *taosInitUdpServer(char *ip, short port, char *label, int, void *fp, void *shandle);
void *taosInitUdpClient(char *ip, short port, char *label, int, void *fp, void *shandle);
void taosCleanUpUdpConnection(void *handle);
int taosSendUdpData(uint32_t ip, short port, char *data, int dataLen, void *chandle);
void *taosOpenUdpConnection(void *shandle, void *thandle, char *ip, short port);

void taosFreeMsgHdr(void *hdr);
int taosMsgHdrSize(void *hdr);
void taosSendMsgHdr(void *hdr, int fd);
void taosInitMsgHdr(void **hdr, void *dest, int maxPkts);
void taosSetMsgHdrData(void *hdr, char *data, int dataLen);

#endif
