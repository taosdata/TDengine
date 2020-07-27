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

#ifndef TDENGINE_TCP_POOL_H
#define TDENGINE_TCP_POOL_H

#ifdef __cplusplus
extern "C" {
#endif

typedef void* ttpool_h;
typedef void* tthread_h;

typedef struct {
  int       numOfThreads;
  uint32_t  serverIp;
  short     port;
  int       bufferSize;
  void     (*processBrokenLink)(void *ahandle);
  int      (*processIncomingMsg)(void *ahandle, void *buffer);
  void     (*processIncomingConn)(int fd, uint32_t ip);
} SPoolInfo;

ttpool_h   taosOpenTcpThreadPool(SPoolInfo *pInfo);
void       taosCloseTcpThreadPool(ttpool_h);
void      *taosAllocateTcpConn(void *, void *ahandle, int connFd);
void       taosFreeTcpConn(void *);


#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TCP_POOL_H
                                   
