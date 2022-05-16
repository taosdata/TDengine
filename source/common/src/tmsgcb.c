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

int32_t tmsgPutToQueue(const SMsgCb* pMsgCb, EQueueType qtype, SRpcMsg* pReq) {
  // cannot be empty, but not checked for faster detect
  PutToQueueFp fp = pMsgCb->queueFps[qtype];
  return (*fp)(pMsgCb->pMgmt, pReq);
}

int32_t tmsgGetQueueSize(const SMsgCb* pMsgCb, int32_t vgId, EQueueType qtype) {
  // cannot be empty, but not checked for faster detect
  GetQueueSizeFp fp = pMsgCb->qsizeFp;
  return (*fp)(pMsgCb->pMgmt, vgId, qtype);
}

int32_t tmsgSendReq(const SMsgCb* pMsgCb, const SEpSet* epSet, SRpcMsg* pReq) {
  // cannot be empty, but not checked for faster detect
  SendReqFp fp = pMsgCb->sendReqFp;
  return (*fp)(pMsgCb->pWrapper, epSet, pReq);
}

void tmsgSendRsp(SRpcMsg* pMsg) {
  // cannot be empty, but not checked for faster detect
  SendRspFp fp = tsDefaultMsgCb.sendRspFp;
  return (*fp)(pMsg);
}

void tmsgSendRedirectRsp(SRpcMsg* pRsp, const SEpSet* pNewEpSet) {
  // cannot be empty, but not checked for faster detect
  SendRedirectRspFp fp = tsDefaultMsgCb.sendRedirectRspFp;
  (*fp)(pRsp, pNewEpSet);
}

void tmsgRegisterBrokenLinkArg(const SMsgCb* pMsgCb, SRpcMsg* pMsg) {
  RegisterBrokenLinkArgFp fp = pMsgCb->registerBrokenLinkArgFp;
  if (fp != NULL) {
    (*fp)(pMsgCb->pWrapper, pMsg);
  } else {
    terrno = TSDB_CODE_INVALID_PTR;
  }
}

void tmsgReleaseHandle(void* handle, int8_t type) {
  ReleaseHandleFp fp = tsDefaultMsgCb.releaseHandleFp;
  if (fp != NULL) {
    (*fp)(tsDefaultMsgCb.pWrapper, handle, type);
  } else {
    terrno = TSDB_CODE_INVALID_PTR;
  }
}

void tmsgReportStartup(const char* name, const char* desc) {
  ReportStartup fp = tsDefaultMsgCb.reportStartupFp;
  (*fp)(name, desc);
}