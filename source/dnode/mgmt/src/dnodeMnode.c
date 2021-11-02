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
#include "dnodeMnode.h"
#include "dnodeDnode.h"
#include "dnodeTransport.h"
#include "mnode.h"

int32_t dnodeInitMnode() {
  SMnodePara para;
  para.fp.GetDnodeEp = dnodeGetDnodeEp;
  para.fp.SendMsgToDnode = dnodeSendMsgToDnode;
  para.fp.SendMsgToMnode = dnodeSendMsgToMnode;
  para.fp.SendRedirectMsg = dnodeSendRedirectMsg;
  para.dnodeId = dnodeGetDnodeId();
  para.clusterId = dnodeGetClusterId();

  return mnodeInit(para);
}

void dnodeCleanupMnode() { mnodeCleanup(); }

static int32_t dnodeStartMnode(SRpcMsg *pMsg) {
  SCreateMnodeMsg *pCfg = pMsg->pCont;
  pCfg->dnodeId = htonl(pCfg->dnodeId);
  pCfg->mnodeNum = htonl(pCfg->mnodeNum);
  for (int32_t i = 0; i < pCfg->mnodeNum; ++i) {
    pCfg->mnodeEps[i].dnodeId = htonl(pCfg->mnodeEps[i].dnodeId);
    pCfg->mnodeEps[i].dnodePort = htons(pCfg->mnodeEps[i].dnodePort);
  }

  if (pCfg->dnodeId != dnodeGetDnodeId()) {
    dDebug("dnode:%d, in create meps msg is not equal with saved dnodeId:%d", pCfg->dnodeId, dnodeGetDnodeId());
    return TSDB_CODE_MND_DNODE_ID_NOT_CONFIGURED;
  }

  if (mnodeGetStatus() == MN_STATUS_READY) return 0;

  return mnodeDeploy();
}

void dnodeProcessCreateMnodeReq(SRpcMsg *pMsg) {
  int32_t code = dnodeStartMnode(pMsg);

  SRpcMsg rspMsg = {.handle = pMsg->handle, .pCont = NULL, .contLen = 0, .code = code};

  rpcSendResponse(&rspMsg);
  rpcFreeCont(pMsg->pCont);
}

void dnodeProcessDropMnodeReq(SRpcMsg *pMsg) {
  int32_t code = dnodeStartMnode(pMsg);

  SRpcMsg rspMsg = {.handle = pMsg->handle, .pCont = NULL, .contLen = 0, .code = code};

  rpcSendResponse(&rspMsg);
  rpcFreeCont(pMsg->pCont);
}

void dnodeProcessMnodeMsg(SRpcMsg *pMsg, SEpSet *pEpSet) {
  switch (pMsg->msgType) {
    case TSDB_MSG_TYPE_CREATE_MNODE_IN:
      dnodeProcessCreateMnodeReq(pMsg);
      break;
    case TSDB_MSG_TYPE_DROP_MNODE_IN:
      dnodeProcessDropMnodeReq(pMsg);
      break;
    default:
      mnodeProcessMsg(pMsg);
  }
}

int32_t dnodeGetUserAuthFromMnode(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  return mnodeRetriveAuth(user, spi, encrypt, secret, ckey);
}