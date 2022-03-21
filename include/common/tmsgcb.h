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

#ifndef _TD_COMMON_MSG_CB_H_
#define _TD_COMMON_MSG_CB_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

struct SRpcMsg;
struct SEpSet;
struct SMgmtWrapper;
typedef struct SMgmtWrapper SMgmtWrapper;

typedef int32_t (*PutToQueueFp)(struct SMgmtWrapper* pWrapper, struct SRpcMsg* pReq);
typedef int32_t (*SendReqFp)(struct SMgmtWrapper* pWrapper, struct SEpSet* epSet, struct SRpcMsg* pReq);
typedef int32_t (*SendMnodeReqFp)(struct SMgmtWrapper* pWrapper, struct SRpcMsg* pReq);
typedef void (*SendRspFp)(struct SMgmtWrapper* pWrapper, struct SRpcMsg* pRsp);

typedef enum { QUERY_QUEUE, FETCH_QUEUE, WRITE_QUEUE, APPLY_QUEUE, SYNC_QUEUE, QUEUE_MAX } EMsgQueueType;

typedef struct {
  struct SMgmtWrapper* pWrapper;
  PutToQueueFp         queueFps[QUEUE_MAX];
  SendReqFp            sendReqFp;
  SendMnodeReqFp       sendMnodeReqFp;
  SendRspFp            sendRspFp;
} SMsgCb;

int32_t tmsgPutToQueue(const SMsgCb* pMsgCb, EMsgQueueType qtype, struct SRpcMsg* pReq);
int32_t tmsgSendReq(const SMsgCb* pMsgCb, struct SEpSet* epSet, struct SRpcMsg* pReq);
int32_t tmsgSendMnodeReq(const SMsgCb* pMsgCb, struct SRpcMsg* pReq);
void    tmsgSendRsp(const SMsgCb* pMsgCb, struct SRpcMsg* pRsp);

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_MSG_CB_H_*/
