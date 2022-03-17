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

static void dndUpdateMnodeEpSet(SDnode *pDnode, SEpSet *pEpSet) {
  SMgmtWrapper *pWrapper = dndAcquireWrapper(pDnode, DNODE);
  if (pWrapper != NULL) {
    dmUpdateMnodeEpSet(pWrapper->pMgmt, pEpSet);
  }
  dndReleaseWrapper(pWrapper);
}

static inline NodeMsgFp dndGetMsgFp(SMgmtWrapper *pWrapper, SRpcMsg *pRpc) {
  NodeMsgFp msgFp = pWrapper->msgFps[TMSG_INDEX(pRpc->msgType)];
  if (msgFp == NULL) {
    terrno = TSDB_CODE_MSG_NOT_PROCESSED;
  }

  return msgFp;
}

static inline int32_t dndBuildMsg(SNodeMsg *pMsg, SRpcMsg *pRpc) {
  SRpcConnInfo connInfo = {0};
  if ((pRpc->msgType & 1U) && rpcGetConnInfo(pRpc->handle, &connInfo) != 0) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    dError("failed to build msg since %s, app:%p RPC:%p", terrstr(), pRpc->ahandle, pRpc->handle);
    return -1;
  }

  memcpy(pMsg->user, connInfo.user, TSDB_USER_LEN);
  memcpy(&pMsg->rpcMsg, pRpc, sizeof(SRpcMsg));

  return 0;
}

void dndProcessRpcMsg(SMgmtWrapper *pWrapper, SRpcMsg *pRpc, SEpSet *pEpSet) {
  if (pEpSet && pEpSet->numOfEps > 0 && pRpc->msgType == TDMT_MND_STATUS_RSP) {
    dndUpdateMnodeEpSet(pWrapper->pDnode, pEpSet);
  }

  int32_t   code = -1;
  SNodeMsg *pMsg = NULL;
  NodeMsgFp msgFp = NULL;

  if (dndMarkWrapper(pWrapper) != 0) goto _OVER;
  if ((msgFp = dndGetMsgFp(pWrapper, pRpc)) == NULL) goto _OVER;
  if ((pMsg = taosAllocateQitem(sizeof(SNodeMsg))) == NULL) goto _OVER;
  if (dndBuildMsg(pMsg, pRpc) != 0) goto _OVER;

  dTrace("msg:%p, is created, app:%p user:%s", pMsg, pRpc->ahandle, pMsg->user);
  if (pWrapper->procType == PROC_SINGLE) {
    code = (*msgFp)(pWrapper->pMgmt, pMsg);
  } else if (pWrapper->procType == PROC_PARENT) {
    code = taosProcPutToChildQueue(pWrapper->pProc, pMsg, sizeof(SNodeMsg), pRpc->pCont, pRpc->contLen);
  } else {
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
    if (pRpc->msgType & 1U) {
      SRpcMsg rsp = {.handle = pRpc->handle, .ahandle = pRpc->ahandle, .code = terrno};
      dndSendRsp(pWrapper, &rsp);
    }
    dTrace("msg:%p, is freed", pMsg);
    taosFreeQitem(pMsg);
    rpcFreeCont(pRpc->pCont);
  }

  dndReleaseWrapper(pWrapper);
}

int32_t dndProcessNodeMsg(SDnode *pDnode, SNodeMsg *pMsg) {
  switch (pMsg->rpcMsg.msgType) {
    case TDMT_DND_CREATE_MNODE:
      return dndOpenNode(pDnode, MNODE);
    case TDMT_DND_DROP_MNODE:
      return dndCloseNode(pDnode, MNODE);
    case TDMT_DND_CREATE_QNODE:
      return dndOpenNode(pDnode, QNODE);
    case TDMT_DND_DROP_QNODE:
      return dndCloseNode(pDnode, QNODE);
    case TDMT_DND_CREATE_SNODE:
      return dndOpenNode(pDnode, SNODE);
    case TDMT_DND_DROP_SNODE:
      return dndCloseNode(pDnode, MNODE);
    case TDMT_DND_CREATE_BNODE:
      return dndOpenNode(pDnode, BNODE);
    case TDMT_DND_DROP_BNODE:
      return dndCloseNode(pDnode, BNODE);

    default:
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      return -1;
  }
}