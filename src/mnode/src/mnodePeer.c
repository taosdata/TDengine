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
#include "os.h"
#include "taoserror.h"
#include "tsched.h"
#include "tsystem.h"
#include "tutil.h"
#include "tgrant.h"
#include "tbn.h"
#include "tglobal.h"
#include "mnode.h"
#include "dnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeDb.h"
#include "mnodeMnode.h"
#include "mnodeProfile.h"
#include "mnodeShow.h"
#include "mnodeSdb.h"
#include "mnodeTable.h"
#include "mnodeVgroup.h"

static int32_t (*tsMnodeProcessPeerMsgFp[TSDB_MSG_TYPE_MAX])(SMnodeMsg *);
static void (*tsMnodeProcessPeerRspFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *);

void mnodeAddPeerMsgHandle(uint8_t msgType, int32_t (*fp)(SMnodeMsg *mnodeMsg)) {
  tsMnodeProcessPeerMsgFp[msgType] = fp;
}

void mnodeAddPeerRspHandle(uint8_t msgType, void (*fp)(SRpcMsg *rpcMsg)) {
  tsMnodeProcessPeerRspFp[msgType] = fp;
}

int32_t mnodeProcessPeerReq(SMnodeMsg *pMsg) {
  if (pMsg->rpcMsg.pCont == NULL) {
    mError("msg:%p, ahandle:%p type:%s in mpeer queue, content is null", pMsg, pMsg->rpcMsg.ahandle, taosMsg[pMsg->rpcMsg.msgType]);
    return TSDB_CODE_MND_INVALID_MSG_LEN;
  }

  if (!sdbIsMaster()) {
    SMnodeRsp *rpcRsp = &pMsg->rpcRsp;
    SRpcEpSet *epSet = rpcMallocCont(sizeof(SRpcEpSet));
    mnodeGetMnodeEpSetForPeer(epSet);
    rpcRsp->rsp = epSet;
    rpcRsp->len = sizeof(SRpcEpSet);

    mDebug("msg:%p, ahandle:%p type:%s in mpeer queue is redirected, numOfEps:%d inUse:%d", pMsg, pMsg->rpcMsg.ahandle,
           taosMsg[pMsg->rpcMsg.msgType], epSet->numOfEps, epSet->inUse);

    return TSDB_CODE_RPC_REDIRECT;
  }

  if (tsMnodeProcessPeerMsgFp[pMsg->rpcMsg.msgType] == NULL) {
    mError("msg:%p, ahandle:%p type:%s in mpeer queue, not processed", pMsg, pMsg->rpcMsg.ahandle,
           taosMsg[pMsg->rpcMsg.msgType]);
    return TSDB_CODE_MND_MSG_NOT_PROCESSED;
  }

  return (*tsMnodeProcessPeerMsgFp[pMsg->rpcMsg.msgType])(pMsg);
}

void mnodeProcessPeerRsp(SRpcMsg *pMsg) {
  if (!sdbIsMaster()) {
    mError("msg:%p, ahandle:%p type:%s  is not processed for it is not master", pMsg, pMsg->ahandle,
           taosMsg[pMsg->msgType]);
    return;
  }

  if (tsMnodeProcessPeerRspFp[pMsg->msgType]) {
    (*tsMnodeProcessPeerRspFp[pMsg->msgType])(pMsg);
  } else {
    mError("msg:%p, ahandle:%p type:%s is not processed", pMsg, pMsg->ahandle, taosMsg[pMsg->msgType]);
  }
}
