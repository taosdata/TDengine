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
#include "dndInt.h"

static int32_t dndBuildMsg(SNodeMsg *pMsg, SRpcMsg *pRpc, SEpSet *pEpSet) {
  SRpcConnInfo connInfo = {0};
  if ((pRpc->msgType & 1U) && rpcGetConnInfo(pRpc->handle, &connInfo) != 0) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    dError("failed to build msg since %s, app:%p RPC:%p", terrstr(), pRpc->ahandle, pRpc->handle);
    return -1;
  }

  memcpy(pMsg->user, connInfo.user, TSDB_USER_LEN);
  pMsg->rpcMsg = *pRpc;

  return 0;
}

void dndProcessRpcMsg(SMgmtWrapper *pWrapper, SRpcMsg *pRpc, SEpSet *pEpSet) {
  if (pEpSet && pEpSet->numOfEps > 0 && pRpc->msgType == TDMT_MND_STATUS_RSP) {
    dmUpdateMnodeEpSet(dndAcquireWrapper(pWrapper->pDnode, DNODE)->pMgmt, pEpSet);
  }

  int32_t   code = -1;
  SNodeMsg *pMsg = NULL;

  NodeMsgFp msgFp = pWrapper->msgFps[TMSG_INDEX(pRpc->msgType)];
  if (msgFp == NULL) {
    terrno = TSDB_CODE_MSG_NOT_PROCESSED;
    goto _OVER;
  }

  pMsg = taosAllocateQitem(sizeof(SNodeMsg));
  if (pMsg == NULL) {
    goto _OVER;
  }

  if (dndBuildMsg(pMsg, pRpc, pEpSet) != 0) {
    goto _OVER;
  }

  dTrace("msg:%p, is created, app:%p user:%s", pMsg, pRpc->ahandle, pMsg->user);

  if (pWrapper->procType == PROC_SINGLE) {
    code = (*msgFp)(pWrapper->pMgmt, pMsg);
  } else if (pWrapper->procType == PROC_PARENT) {
    code = taosProcPutToChildQueue(pWrapper->pProc, pMsg, sizeof(SNodeMsg), pRpc->pCont, pRpc->contLen);
  } else {
    terrno = TSDB_CODE_MEMORY_CORRUPTED;
    dError("msg:%p, won't be processed for it is child process", pMsg);
  }

_OVER:

  if (code == 0) {
    if (pWrapper->procType == PROC_PARENT) {
      dTrace("msg:%p, is freed", pMsg);
      taosFreeQitem(pMsg);
      rpcFreeCont(pRpc->pCont);
    }
  } else {
    dError("msg:%p, failed to process since %s", pMsg, terrstr());
    bool isReq = (pRpc->msgType & 1U);
    if (isReq) {
      SRpcMsg rsp = {.handle = pRpc->handle, .ahandle = pRpc->ahandle, .code = terrno};
      dndSendRsp(pWrapper, &rsp);
    }
    dTrace("msg:%p, is freed", pMsg);
    taosFreeQitem(pMsg);
    rpcFreeCont(pRpc->pCont);
  }
}

static SMgmtWrapper *dndGetWrapperFromMsg(SDnode *pDnode, SNodeMsg *pMsg) {
  SMgmtWrapper *pWrapper = NULL;
  switch (pMsg->rpcMsg.msgType) {
    case TDMT_DND_CREATE_MNODE:
      return dndAcquireWrapper(pDnode, MNODE);
    case TDMT_DND_CREATE_QNODE:
      return dndAcquireWrapper(pDnode, QNODE);
    case TDMT_DND_CREATE_SNODE:
      return dndAcquireWrapper(pDnode, SNODE);
    case TDMT_DND_CREATE_BNODE:
      return dndAcquireWrapper(pDnode, BNODE);
    default:
      return NULL;
  }
}

int32_t dndProcessCreateNodeMsg(SDnode *pDnode, SNodeMsg *pMsg) {
  SMgmtWrapper *pWrapper = dndGetWrapperFromMsg(pDnode, pMsg);
  if (pWrapper->procType == PROC_SINGLE) {
    switch (pMsg->rpcMsg.msgType) {
      case TDMT_DND_CREATE_MNODE:
        return mmProcessCreateReq(pWrapper->pMgmt, pMsg);
      case TDMT_DND_CREATE_QNODE:
        return qmProcessCreateReq(pWrapper->pMgmt, pMsg);
      case TDMT_DND_CREATE_SNODE:
        return smProcessCreateReq(pWrapper->pMgmt, pMsg);
      case TDMT_DND_CREATE_BNODE:
        return bmProcessCreateReq(pWrapper->pMgmt, pMsg);
      default:
        terrno = TSDB_CODE_MSG_NOT_PROCESSED;
        return -1;
    }
  } else {
    terrno = TSDB_CODE_MSG_NOT_PROCESSED;
    return -1;
  }
}

int32_t dndProcessDropNodeMsg(SDnode *pDnode, SNodeMsg *pMsg) {
  SMgmtWrapper *pWrapper = dndGetWrapperFromMsg(pDnode, pMsg);
  switch (pMsg->rpcMsg.msgType) {
    case TDMT_DND_DROP_MNODE:
      return mmProcessDropReq(pWrapper->pMgmt, pMsg);
    case TDMT_DND_DROP_QNODE:
      return qmProcessDropReq(pWrapper->pMgmt, pMsg);
    case TDMT_DND_DROP_SNODE:
      return smProcessDropReq(pWrapper->pMgmt, pMsg);
    case TDMT_DND_DROP_BNODE:
      return bmProcessDropReq(pWrapper->pMgmt, pMsg);
    default:
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      return -1;
  }
}