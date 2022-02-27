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
  void *      serverRpc;
  void *      clientRpc;
  STaosQueue *pMsgQ;
  STaosQset * pQset;
  pthread_t   tid;
  int8_t      isStart;

  SEpSet epSet;

  void *syncTimer;
  void *syncTimerManager;

  int32_t (*start)(struct SSyncIO *ths);
  int32_t (*stop)(struct SSyncIO *ths);
  int32_t (*ping)(struct SSyncIO *ths);

  int32_t (*onMsg)(struct SSyncIO *ths, void *pParent, SRpcMsg *pMsg, SEpSet *pEpSet);
  int32_t (*destroy)(struct SSyncIO *ths);

  void *pSyncNode;
  int32_t (*FpOnPing)(struct SSyncNode *ths, SyncPing *pMsg);

} SSyncIO;

extern SSyncIO *gSyncIO;

int32_t  syncIOStart();
int32_t  syncIOStop();
int32_t  syncIOSendMsg(void *handle, const SEpSet *pEpSet, SRpcMsg *pMsg);
SSyncIO *syncIOCreate();

static int32_t doSyncIOStart(SSyncIO *io);
static int32_t doSyncIOStop(SSyncIO *io);
static int32_t doSyncIOPing(SSyncIO *io);
static int32_t doSyncIOOnMsg(struct SSyncIO *io, void *pParent, SRpcMsg *pMsg, SEpSet *pEpSet);
static int32_t doSyncIODestroy(SSyncIO *io);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_IO_H*/
