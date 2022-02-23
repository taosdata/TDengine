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

  int32_t (*start)(struct SSyncIO *ths);
  int32_t (*stop)(struct SSyncIO *ths);
  int32_t (*ping)(struct SSyncIO *ths);
  int32_t (*onMessage)(struct SSyncIO *ths, void *pParent, SRpcMsg *pMsg, SEpSet *pEpSet);
  int32_t (*destroy)(struct SSyncIO *ths);

} SSyncIO;

SSyncIO * syncIOCreate();

static int32_t syncIOStart(SSyncIO *io);
static int32_t syncIOStop(SSyncIO *io);
static int32_t syncIOPing(SSyncIO *io);
static int32_t syncIOOnMessage(struct SSyncIO *io, void *pParent, SRpcMsg *pMsg, SEpSet *pEpSet);
static int32_t syncIODestroy(SSyncIO *io);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_IO_H*/
