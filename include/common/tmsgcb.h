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

typedef struct SRpcMsg      SRpcMsg;
typedef struct SEpSet       SEpSet;
typedef struct SMgmtWrapper SMgmtWrapper;

typedef enum {
  QUERY_QUEUE,
  FETCH_QUEUE,
  READ_QUEUE,
  WRITE_QUEUE,
  APPLY_QUEUE,
  SYNC_QUEUE,
  MERGE_QUEUE,
  QUEUE_MAX,
} EQueueType;

typedef int32_t (*PutToQueueFp)(SMgmtWrapper* pWrapper, SRpcMsg* pReq);
typedef int32_t (*GetQueueSizeFp)(SMgmtWrapper* pWrapper, int32_t vgId, EQueueType qtype);
typedef int32_t (*SendReqFp)(SMgmtWrapper* pWrapper, const SEpSet* epSet, SRpcMsg* pReq);
typedef int32_t (*SendMnodeReqFp)(SMgmtWrapper* pWrapper, SRpcMsg* pReq);
typedef void (*SendRspFp)(SMgmtWrapper* pWrapper, const SRpcMsg* pRsp);
typedef void (*RegisterBrokenLinkArgFp)(SMgmtWrapper* pWrapper, SRpcMsg* pMsg);
typedef void (*ReleaseHandleFp)(SMgmtWrapper* pWrapper, void* handle, int8_t type);
typedef void (*ReportStartup)(SMgmtWrapper* pWrapper, const char* name, const char* desc);

typedef struct {
  SMgmtWrapper*           pWrapper;
  PutToQueueFp            queueFps[QUEUE_MAX];
  GetQueueSizeFp          qsizeFp;
  SendReqFp               sendReqFp;
  SendRspFp               sendRspFp;
  RegisterBrokenLinkArgFp registerBrokenLinkArgFp;
  ReleaseHandleFp         releaseHandleFp;
  ReportStartup           reportStartupFp;
} SMsgCb;

void    tmsgSetDefaultMsgCb(const SMsgCb* pMsgCb);
int32_t tmsgPutToQueue(const SMsgCb* pMsgCb, EQueueType qtype, SRpcMsg* pReq);
int32_t tmsgGetQueueSize(const SMsgCb* pMsgCb, int32_t vgId, EQueueType qtype);
int32_t tmsgSendReq(const SMsgCb* pMsgCb, const SEpSet* epSet, SRpcMsg* pReq);
void    tmsgSendRsp(const SRpcMsg* pRsp);
void    tmsgRegisterBrokenLinkArg(const SMsgCb* pMsgCb, SRpcMsg* pMsg);
void    tmsgReleaseHandle(void* handle, int8_t type);
void    tmsgReportStartup(const char* name, const char* desc);

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_MSG_CB_H_*/
