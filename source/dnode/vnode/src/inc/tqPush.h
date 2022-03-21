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

#ifndef _TQ_PUSH_H_
#define _TQ_PUSH_H_

#include "executor.h"
#include "thash.h"
#include "trpc.h"
#include "ttimer.h"
#include "vnode.h"

#ifdef __cplusplus
extern "C" {
#endif

enum {
  TQ_PUSHER_TYPE__CLIENT = 1,
  TQ_PUSHER_TYPE__STREAM,
};

typedef struct {
  int8_t   type;
  int8_t   reserved[3];
  int32_t  ttl;
  int64_t  consumerId;
  SRpcMsg* pMsg;
  // SMqPollRsp* rsp;
} STqClientPusher;

typedef struct {
  int8_t      type;
  int8_t      nodeType;
  int8_t      reserved[6];
  int64_t     streamId;
  qTaskInfo_t task;
  // TODO sync function
} STqStreamPusher;

typedef struct {
  int8_t type;  // mq or stream
} STqPusher;

typedef struct {
  SHashObj* pHash;  // <id, STqPush*>
} STqPushMgr;

typedef struct {
  int8_t inited;
  tmr_h  timer;
} STqPushMgmt;

static STqPushMgmt tqPushMgmt;

int32_t tqPushMgrInit();
void    tqPushMgrCleanUp();

STqPushMgr* tqPushMgrOpen();
void        tqPushMgrClose(STqPushMgr* pushMgr);

STqClientPusher* tqAddClientPusher(STqPushMgr* pushMgr, SRpcMsg* pMsg, int64_t consumerId, int64_t ttl);
STqStreamPusher* tqAddStreamPusher(STqPushMgr* pushMgr, int64_t streamId, SEpSet* pEpSet);

#ifdef __cplusplus
}
#endif

#endif /*_TQ_PUSH_H_*/
