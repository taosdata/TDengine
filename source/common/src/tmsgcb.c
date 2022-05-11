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
  PutToQueueFp fp = pMsgCb->queueFps[qtype];
  if (fp != NULL) {
    return (*fp)(pMsgCb->pMgmt, pReq);
  } else {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }
}

int32_t tmsgGetQueueSize(const SMsgCb* pMsgCb, int32_t vgId, EQueueType qtype) {
  GetQueueSizeFp fp = pMsgCb->qsizeFp;
  if (fp != NULL) {
    return (*fp)(pMsgCb->pMgmt, vgId, qtype);
  } else {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }
}

int32_t tmsgSendReq(const SMsgCb* pMsgCb, const SEpSet* epSet, SRpcMsg* pReq) {
  SendReqFp fp = pMsgCb->sendReqFp;
  if (fp != NULL) {
    return (*fp)(pMsgCb->pWrapper, epSet, pReq);
  } else {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }
}

void tmsgSendRsp(const SRpcMsg* pRsp) {
  SendRspFp fp = tsDefaultMsgCb.sendRspFp;
  if (fp != NULL) {
    return (*fp)(tsDefaultMsgCb.pWrapper, pRsp);
  } else {
    terrno = TSDB_CODE_INVALID_PTR;
  }
}

void tmsgSendRedirectRsp(const SRpcMsg* pRsp, const SEpSet* pNewEpSet) {
  SendRedirectRspFp fp = tsDefaultMsgCb.sendRedirectRspFp;
  if (fp != NULL) {
    (*fp)(tsDefaultMsgCb.pWrapper, pRsp, pNewEpSet);
  } else {
    terrno = TSDB_CODE_INVALID_PTR;
  }
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
  if (fp != NULL && tsDefaultMsgCb.pWrapper != NULL) {
    (*fp)(tsDefaultMsgCb.pWrapper, name, desc);
  } else {
    terrno = TSDB_CODE_INVALID_PTR;
  }
}