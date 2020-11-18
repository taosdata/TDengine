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
    return TSDB_CODE_MND_INVALID_MSG_LEN;
  }

  if (!sdbIsMaster()) {
    SMnodeRsp *rpcRsp = &pMsg->rpcRsp;
    SRpcEpSet *epSet = rpcMallocCont(sizeof(SRpcEpSet));
    mnodeGetMnodeEpSetForPeer(epSet);
    rpcRsp->rsp = epSet;
    rpcRsp->len = sizeof(SRpcEpSet);

    mDebug("%p, msg:%s in mpeer queue will be redirected, numOfEps:%d inUse:%d", pMsg->rpcMsg.ahandle,
           taosMsg[pMsg->rpcMsg.msgType], epSet->numOfEps, epSet->inUse);
    for (int32_t i = 0; i < epSet->numOfEps; ++i) {
      if (strcmp(epSet->fqdn[i], tsLocalFqdn) == 0 && htons(epSet->port[i]) == tsServerPort + TSDB_PORT_DNODEDNODE) {
        epSet->inUse = (i + 1) % epSet->numOfEps;
        mDebug("mpeer:%d ep:%s:%u, set inUse to %d", i, epSet->fqdn[i], htons(epSet->port[i]), epSet->inUse);
      } else {
        mDebug("mpeer:%d ep:%s:%u", i, epSet->fqdn[i], htons(epSet->port[i]));
      }
    }

    return TSDB_CODE_RPC_REDIRECT;
  }

  if (tsMnodeProcessPeerMsgFp[pMsg->rpcMsg.msgType] == NULL) {
    mError("%p, msg:%s in mpeer queue, not processed", pMsg->rpcMsg.ahandle, taosMsg[pMsg->rpcMsg.msgType]);
    return TSDB_CODE_MND_MSG_NOT_PROCESSED;
  }

  return (*tsMnodeProcessPeerMsgFp[pMsg->rpcMsg.msgType])(pMsg);
}

void mnodeProcessPeerRsp(SRpcMsg *pMsg) {
  if (!sdbIsMaster()) {
    mError("%p, msg:%s is not processed for it is not master", pMsg->ahandle, taosMsg[pMsg->msgType]);
    return;
  }

  if (tsMnodeProcessPeerRspFp[pMsg->msgType]) {
    (*tsMnodeProcessPeerRspFp[pMsg->msgType])(pMsg);
  } else {
    mError("%p, msg:%s is not processed", pMsg->ahandle, taosMsg[pMsg->msgType]);
  }
}
