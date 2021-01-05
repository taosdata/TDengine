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

#ifndef TDENGINE_SYNC_TCP_POOL_H
#define TDENGINE_SYNC_TCP_POOL_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  int32_t   numOfThreads;
  uint32_t  serverIp;
  int16_t   port;
  int32_t   bufferSize;
  void    (*processBrokenLink)(int64_t handleId);
  int32_t (*processIncomingMsg)(int64_t handleId, void *buffer);
  void    (*processIncomingConn)(int32_t fd, uint32_t ip);
} SPoolInfo;

void *syncOpenTcpThreadPool(SPoolInfo *pInfo);
void  syncCloseTcpThreadPool(void *);
void *syncAllocateTcpConn(void *, int64_t rid, int32_t connFd);
void  syncFreeTcpConn(void *);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TCP_POOL_H
                                   
