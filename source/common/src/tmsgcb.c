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

#define _DEFAULT_SOURCE
#include "tmsgcb.h"
#include "taoserror.h"

static SMsgCb tsDefaultMsgCb;

void tmsgSetDefaultMsgCb(const SMsgCb* pMsgCb) { tsDefaultMsgCb = *pMsgCb; }

int32_t tmsgPutToQueue(const SMsgCb* pMsgCb, EQueueType qtype, SRpcMsg* pMsg) {
  PutToQueueFp fp = pMsgCb->queueFps[qtype];
  return (*fp)(pMsgCb->mgmt, pMsg);
}

int32_t tmsgGetQueueSize(const SMsgCb* pMsgCb, int32_t vgId, EQueueType qtype) {
  GetQueueSizeFp fp = pMsgCb->qsizeFp;
  return (*fp)(pMsgCb->mgmt, vgId, qtype);
}

int32_t tmsgSendReq(const SEpSet* epSet, SRpcMsg* pMsg) {
  SendReqFp fp = tsDefaultMsgCb.sendReqFp;
  return (*fp)(epSet, pMsg);
}

void tmsgSendRsp(SRpcMsg* pMsg) {
  SendRspFp fp = tsDefaultMsgCb.sendRspFp;
  return (*fp)(pMsg);
}

void tmsgSendRedirectRsp(SRpcMsg* pMsg, const SEpSet* pNewEpSet) {
  SendRedirectRspFp fp = tsDefaultMsgCb.sendRedirectRspFp;
  (*fp)(pMsg, pNewEpSet);
}

void tmsgRegisterBrokenLinkArg(SRpcMsg* pMsg) {
  RegisterBrokenLinkArgFp fp = tsDefaultMsgCb.registerBrokenLinkArgFp;
  (*fp)(pMsg);
}

void tmsgReleaseHandle(SRpcHandleInfo* pHandle, int8_t type) {
  ReleaseHandleFp fp = tsDefaultMsgCb.releaseHandleFp;
  (*fp)(pHandle, type);
}

void tmsgReportStartup(const char* name, const char* desc) {
  ReportStartup fp = tsDefaultMsgCb.reportStartupFp;
  (*fp)(name, desc);
}