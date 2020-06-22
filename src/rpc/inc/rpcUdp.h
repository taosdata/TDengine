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

#ifndef _rpc_udp_header_
#define _rpc_udp_header_

#ifdef __cplusplus
extern "C" {
#endif

#include "taosdef.h"

void *taosInitUdpConnection(uint32_t ip, uint16_t port, char *label, int, void *fp, void *shandle);
void  taosStopUdpConnection(void *handle);
void  taosCleanUpUdpConnection(void *handle);
int   taosSendUdpData(uint32_t ip, uint16_t port, void *data, int dataLen, void *chandle);
void *taosOpenUdpConnection(void *shandle, void *thandle, uint32_t ip, uint16_t port);

void  taosFreeMsgHdr(void *hdr);
int   taosMsgHdrSize(void *hdr);
void  taosSendMsgHdr(void *hdr, int fd);
void  taosInitMsgHdr(void **hdr, void *dest, int maxPkts);
void  taosSetMsgHdrData(void *hdr, char *data, int dataLen);

#ifdef __cplusplus
}
#endif

#endif
