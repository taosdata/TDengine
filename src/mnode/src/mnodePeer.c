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
#include "tbalance.h"
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
    mError("%p, msg:%s in mpeer queue, content is null", pMsg->rpcMsg.ahandle, taosMsg[pMsg->rpcMsg.msgType]);
    return TSDB_CODE_INVALID_MSG_LEN;
  }

  if (!sdbIsMaster()) {
    SMnodeRsp *rpcRsp = &pMsg->rpcRsp;
    SRpcIpSet *ipSet = rpcMallocCont(sizeof(SRpcIpSet));
    mnodeGetMnodeIpSetForPeer(ipSet);
    rpcRsp->rsp = ipSet;
    rpcRsp->len = sizeof(SRpcIpSet);

    mTrace("%p, msg:%s in mpeer queue, will be redireced inUse:%d", pMsg->rpcMsg.ahandle, taosMsg[pMsg->rpcMsg.msgType], ipSet->inUse);
    for (int32_t i = 0; i < ipSet->numOfIps; ++i) {
      mTrace("mnode index:%d ip:%s:%d", i, ipSet->fqdn[i], htons(ipSet->port[i]));
    }

    return TSDB_CODE_REDIRECT;
  }

  if (tsMnodeProcessPeerMsgFp[pMsg->rpcMsg.msgType] == NULL) {
    mError("%p, msg:%s in mpeer queue, not processed", pMsg->rpcMsg.ahandle, taosMsg[pMsg->rpcMsg.msgType]);
    return TSDB_CODE_MSG_NOT_PROCESSED;
  }

  return (*tsMnodeProcessPeerMsgFp[pMsg->rpcMsg.msgType])(pMsg);
}

void mnodeProcessPeerRsp(SRpcMsg *pMsg) {
  if (tsMnodeProcessPeerRspFp[pMsg->msgType]) {
    (*tsMnodeProcessPeerRspFp[pMsg->msgType])(pMsg);
  } else {
    mError("%p, msg:%s is not processed", pMsg->ahandle, taosMsg[pMsg->msgType]);
  }
}
