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

typedef struct SRpcMsg        SRpcMsg;
typedef struct SEpSet         SEpSet;
typedef struct SMgmtWrapper   SMgmtWrapper;
typedef struct SRpcHandleInfo SRpcHandleInfo;

typedef enum {
  QUERY_QUEUE,
  FETCH_QUEUE,
  READ_QUEUE,
  WRITE_QUEUE,
  APPLY_QUEUE,
  SYNC_QUEUE,
  SYNC_RD_QUEUE,
  STREAM_QUEUE,
  QUEUE_MAX,
} EQueueType;

typedef bool (*UpdateDnodeInfoFp)(void* pData, int32_t* dnodeId, int64_t* clusterId, char* fqdn, uint16_t* port);
typedef int32_t (*PutToQueueFp)(void* pMgmt, EQueueType qtype, SRpcMsg* pMsg);
typedef int32_t (*GetQueueSizeFp)(void* pMgmt, int32_t vgId, EQueueType qtype);
typedef int32_t (*SendReqFp)(const SEpSet* pEpSet, SRpcMsg* pMsg);
typedef void (*SendRspFp)(SRpcMsg* pMsg);
typedef void (*RegisterBrokenLinkArgFp)(SRpcMsg* pMsg);
typedef void (*ReleaseHandleFp)(SRpcHandleInfo* pHandle, int8_t type);
typedef void (*ReportStartup)(const char* name, const char* desc);

typedef struct {
  void*                   data;
  void*                   mgmt;
  void*                   clientRpc;
  void*                   serverRpc;
  void*                   statusRpc;
  void*                   syncRpc;
  PutToQueueFp            putToQueueFp;
  GetQueueSizeFp          qsizeFp;
  SendReqFp               sendReqFp;
  SendReqFp               sendSyncReqFp;
  SendRspFp               sendRspFp;
  RegisterBrokenLinkArgFp registerBrokenLinkArgFp;
  ReleaseHandleFp         releaseHandleFp;
  ReportStartup           reportStartupFp;
  UpdateDnodeInfoFp       updateDnodeInfoFp;
} SMsgCb;

void    tmsgSetDefault(const SMsgCb* msgcb);
int32_t tmsgPutToQueue(const SMsgCb* msgcb, EQueueType qtype, SRpcMsg* pMsg);
int32_t tmsgGetQueueSize(const SMsgCb* msgcb, int32_t vgId, EQueueType qtype);
int32_t tmsgSendReq(const SEpSet* epSet, SRpcMsg* pMsg);
int32_t tmsgSendSyncReq(const SEpSet* epSet, SRpcMsg* pMsg);
void    tmsgSendRsp(SRpcMsg* pMsg);
void    tmsgRegisterBrokenLinkArg(SRpcMsg* pMsg);
void    tmsgReleaseHandle(SRpcHandleInfo* pHandle, int8_t type);
void    tmsgReportStartup(const char* name, const char* desc);
bool    tmsgUpdateDnodeInfo(int32_t* dnodeId, int64_t* clusterId, char* fqdn, uint16_t* port);
void    tmsgUpdateDnodeEpSet(SEpSet* epset);

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_MSG_CB_H_*/
