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

#ifndef _TD_LIBS_IO_H
#define _TD_LIBS_IO_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "os.h"
#include "syncInt.h"
#include "taosdef.h"
#include "tqueue.h"
#include "trpc.h"

typedef struct SSyncIO {
  STaosQueue *pMsgQ;
  STaosQset * pQset;
  pthread_t   consumerTid;

  void * serverRpc;
  void * clientRpc;
  SEpSet myAddr;

  void *ioTimerTickQ;
  void *ioTimerTickPing;
  void *ioTimerManager;

  void *pSyncNode;
  int32_t (*FpOnSyncPing)(SSyncNode *pSyncNode, SyncPing *pMsg);
  int32_t (*FpOnSyncPingReply)(SSyncNode *pSyncNode, SyncPingReply *pMsg);
  int32_t (*FpOnSyncRequestVote)(SSyncNode *pSyncNode, SyncRequestVote *pMsg);
  int32_t (*FpOnSyncRequestVoteReply)(SSyncNode *pSyncNode, SyncRequestVoteReply *pMsg);
  int32_t (*FpOnSyncAppendEntries)(SSyncNode *pSyncNode, SyncAppendEntries *pMsg);
  int32_t (*FpOnSyncAppendEntriesReply)(SSyncNode *pSyncNode, SyncAppendEntriesReply *pMsg);

  int8_t isStart;

} SSyncIO;

extern SSyncIO *gSyncIO;

int32_t syncIOStart(char *host, uint16_t port);
int32_t syncIOStop();
int32_t syncIOSendMsg(void *clientRpc, const SEpSet *pEpSet, SRpcMsg *pMsg);
int32_t syncIOTickQ();
int32_t syncIOTickPing();

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_IO_H*/
