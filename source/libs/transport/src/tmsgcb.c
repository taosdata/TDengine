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
#include "transLog.h"
#include "trpc.h"

static SMsgCb defaultMsgCb;

void tmsgSetDefault(const SMsgCb* msgcb) { defaultMsgCb = *msgcb; }

int32_t tmsgPutToQueue(const SMsgCb* msgcb, EQueueType qtype, SRpcMsg* pMsg) {
  int32_t code = (*msgcb->putToQueueFp)(msgcb->mgmt, qtype, pMsg);
  if (code != 0) {
    SRpcMsg rsp = {.code = code, .info = pMsg->info};
    if (rsp.info.handle != NULL) {
      tmsgSendRsp(&rsp);
    }
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
  }
  return code;
}

int32_t tmsgGetQueueSize(const SMsgCb* msgcb, int32_t vgId, EQueueType qtype) {
  return (*msgcb->qsizeFp)(msgcb->mgmt, vgId, qtype);
}

int32_t tmsgSendReq(const SEpSet* epSet, SRpcMsg* pMsg) {
  int32_t code = (*defaultMsgCb.sendReqFp)(epSet, pMsg);
  if (code != 0) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
  }
  return code;
}
int32_t tmsgSendSyncReq(const SEpSet* epSet, SRpcMsg* pMsg) {
  int32_t code = (*defaultMsgCb.sendSyncReqFp)(epSet, pMsg);
  if (code != 0) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
  }
  return code;
}

void tmsgSendRsp(SRpcMsg* pMsg) {
#if 1
  rpcSendResponse(pMsg);
#else
  return (*defaultMsgCb.sendRspFp)(pMsg);
#endif
}

void tmsgRegisterBrokenLinkArg(SRpcMsg* pMsg) { (*defaultMsgCb.registerBrokenLinkArgFp)(pMsg); }

void tmsgReleaseHandle(SRpcHandleInfo* pHandle, int8_t type) { (*defaultMsgCb.releaseHandleFp)(pHandle, type); }

void tmsgReportStartup(const char* name, const char* desc) { (*defaultMsgCb.reportStartupFp)(name, desc); }

bool tmsgUpdateDnodeInfo(int32_t* dnodeId, int64_t* clusterId, char* fqdn, uint16_t* port) {
  if (defaultMsgCb.updateDnodeInfoFp) {
    return (*defaultMsgCb.updateDnodeInfoFp)(defaultMsgCb.data, dnodeId, clusterId, fqdn, port);
  } else {
    return false;
  }
}

void tmsgUpdateDnodeEpSet(SEpSet* epset) {
  for (int32_t i = 0; i < epset->numOfEps; ++i) {
    tmsgUpdateDnodeInfo(NULL, NULL, epset->eps[i].fqdn, &epset->eps[i].port);
  }
}
