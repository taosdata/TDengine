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
#include "dmMgmt.h"
#include "qworker.h"

static inline void dmSendRsp(SRpcMsg *pMsg) {
  SMgmtWrapper *pWrapper = pMsg->info.wrapper;
  if (InChildProc(pWrapper)) {
    dmPutToProcPQueue(&pWrapper->proc, pMsg, DND_FUNC_RSP);
  } else {
    rpcSendResponse(pMsg);
  }
}

static inline void dmBuildMnodeRedirectRsp(SDnode *pDnode, SRpcMsg *pMsg) {
  SEpSet epSet = {0};
  dmGetMnodeEpSetForRedirect(&pDnode->data, pMsg, &epSet);

  const int32_t contLen = tSerializeSEpSet(NULL, 0, &epSet);
  pMsg->pCont = rpcMallocCont(contLen);
  if (pMsg->pCont == NULL) {
    pMsg->code = TSDB_CODE_OUT_OF_MEMORY;
  } else {
    tSerializeSEpSet(pMsg->pCont, contLen, &epSet);
    pMsg->contLen = contLen;
  }
}

static inline void dmSendRedirectRsp(SRpcMsg *pMsg, const SEpSet *pNewEpSet) {
  pMsg->info.hasEpSet = 1;
  SRpcMsg rsp = {.code = TSDB_CODE_RPC_REDIRECT, .info = pMsg->info, .msgType = pMsg->msgType};
  int32_t contLen = tSerializeSEpSet(NULL, 0, pNewEpSet);

  rsp.pCont = rpcMallocCont(contLen);
  if (rsp.pCont == NULL) {
    pMsg->code = TSDB_CODE_OUT_OF_MEMORY;
  } else {
    tSerializeSEpSet(rsp.pCont, contLen, pNewEpSet);
    rsp.contLen = contLen;
  }
  dmSendRsp(&rsp);
  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = NULL;
}

int32_t dmProcessNodeMsg(SMgmtWrapper *pWrapper, SRpcMsg *pMsg) {
  NodeMsgFp msgFp = pWrapper->msgFps[TMSG_INDEX(pMsg->msgType)];
  if (msgFp == NULL) {
    terrno = TSDB_CODE_MSG_NOT_PROCESSED;
    return -1;
  }

  const STraceId *trace = &pMsg->info.traceId;
  dGTrace("msg:%p, will be processed by %s", pMsg, pWrapper->name);
  pMsg->info.wrapper = pWrapper;
  return (*msgFp)(pWrapper->pMgmt, pMsg);
}

static void dmProcessRpcMsg(SDnode *pDnode, SRpcMsg *pRpc, SEpSet *pEpSet) {
  SDnodeTrans  *pTrans = &pDnode->trans;
  int32_t       code = -1;
  SRpcMsg      *pMsg = NULL;
  SMgmtWrapper *pWrapper = NULL;
  SDnodeHandle *pHandle = &pTrans->msgHandles[TMSG_INDEX(pRpc->msgType)];

  const STraceId *trace = &pRpc->info.traceId;
  dGTrace("msg:%s is received, handle:%p len:%d code:0x%x app:%p refId:%" PRId64, TMSG_INFO(pRpc->msgType),
          pRpc->info.handle, pRpc->contLen, pRpc->code, pRpc->info.ahandle, pRpc->info.refId);

  switch (pRpc->msgType) {
    case TDMT_DND_NET_TEST:
      dmProcessNetTestReq(pDnode, pRpc);
      return;
    case TDMT_MND_SYSTABLE_RETRIEVE_RSP:
    case TDMT_DND_SYSTABLE_RETRIEVE_RSP:
    case TDMT_SCH_FETCH_RSP:
    case TDMT_SCH_MERGE_FETCH_RSP:
    case TDMT_VND_SUBMIT_RSP:
      qWorkerProcessRspMsg(NULL, NULL, pRpc, 0);
      return;
    case TDMT_MND_STATUS_RSP:
      if (pEpSet != NULL) {
        dmSetMnodeEpSet(&pDnode->data, pEpSet);
      }
      break;
    default:
      break;
  }

  if (pDnode->status != DND_STAT_RUNNING) {
    if (pRpc->msgType == TDMT_DND_SERVER_STATUS) {
      dmProcessServerStartupStatus(pDnode, pRpc);
      return;
    } else {
      terrno = TSDB_CODE_APP_NOT_READY;
      goto _OVER;
    }
  }

  if (IsReq(pRpc) && pRpc->pCont == NULL) {
    terrno = TSDB_CODE_INVALID_MSG_LEN;
    goto _OVER;
  }

  if (pHandle->defaultNtype == NODE_END) {
    terrno = TSDB_CODE_MSG_NOT_PROCESSED;
    goto _OVER;
  }

  pWrapper = &pDnode->wrappers[pHandle->defaultNtype];
  if (pHandle->needCheckVgId) {
    if (pRpc->contLen > 0) {
      const SMsgHead *pHead = pRpc->pCont;
      const int32_t   vgId = ntohl(pHead->vgId);
      switch (vgId) {
        case QNODE_HANDLE:
          pWrapper = &pDnode->wrappers[QNODE];
          break;
        case SNODE_HANDLE:
          pWrapper = &pDnode->wrappers[SNODE];
          break;
        case MNODE_HANDLE:
          pWrapper = &pDnode->wrappers[MNODE];
          break;
        default:
          break;
      }
    } else {
      terrno = TSDB_CODE_INVALID_MSG_LEN;
      goto _OVER;
    }
  }

  if (dmMarkWrapper(pWrapper) != 0) {
    pWrapper = NULL;
    goto _OVER;
  }

  pRpc->info.wrapper = pWrapper;
  pMsg = taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM);
  if (pMsg == NULL) goto _OVER;

  memcpy(pMsg, pRpc, sizeof(SRpcMsg));
  dGTrace("msg:%p, is created, type:%s handle:%p", pMsg, TMSG_INFO(pRpc->msgType), pMsg->info.handle);

  if (InParentProc(pWrapper)) {
    code = dmPutToProcCQueue(&pWrapper->proc, pMsg, DND_FUNC_REQ);
  } else {
    code = dmProcessNodeMsg(pWrapper, pMsg);
  }

_OVER:
  if (code != 0) {
    if (terrno != 0) code = terrno;
    dGTrace("msg:%p, failed to process since %s", pMsg, terrstr());

    if (IsReq(pRpc)) {
      SRpcMsg rsp = {.code = code, .info = pRpc->info};
      if ((code == TSDB_CODE_NODE_NOT_DEPLOYED || code == TSDB_CODE_APP_NOT_READY) && pRpc->msgType > TDMT_MND_MSG &&
          pRpc->msgType < TDMT_VND_MSG) {
        dmBuildMnodeRedirectRsp(pDnode, &rsp);
      }

      if (pWrapper != NULL) {
        dmSendRsp(&rsp);
      } else {
        rpcSendResponse(&rsp);
      }
    }

    if (pMsg != NULL) {
      dGTrace("msg:%p, is freed", pMsg);
      taosFreeQitem(pMsg);
    }
    rpcFreeCont(pRpc->pCont);
    pRpc->pCont = NULL;
  }

  dmReleaseWrapper(pWrapper);
}

int32_t dmInitMsgHandle(SDnode *pDnode) {
  SDnodeTrans *pTrans = &pDnode->trans;

  for (EDndNodeType ntype = DNODE; ntype < NODE_END; ++ntype) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[ntype];
    SArray       *pArray = (*pWrapper->func.getHandlesFp)();
    if (pArray == NULL) return -1;

    for (int32_t i = 0; i < taosArrayGetSize(pArray); ++i) {
      SMgmtHandle  *pMgmt = taosArrayGet(pArray, i);
      SDnodeHandle *pHandle = &pTrans->msgHandles[TMSG_INDEX(pMgmt->msgType)];
      if (pMgmt->needCheckVgId) {
        pHandle->needCheckVgId = pMgmt->needCheckVgId;
      }
      if (!pMgmt->needCheckVgId) {
        pHandle->defaultNtype = ntype;
      }
      pWrapper->msgFps[TMSG_INDEX(pMgmt->msgType)] = pMgmt->msgFp;
    }

    taosArrayDestroy(pArray);
  }

  return 0;
}

static inline int32_t dmSendReq(const SEpSet *pEpSet, SRpcMsg *pMsg) {
  SDnode *pDnode = dmInstance();
  if (pDnode->status != DND_STAT_RUNNING && pMsg->msgType < TDMT_SYNC_MSG) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
    terrno = TSDB_CODE_NODE_OFFLINE;
    dError("failed to send rpc msg:%s since %s, handle:%p", TMSG_INFO(pMsg->msgType), terrstr(), pMsg->info.handle);
    return -1;
  } else {
    rpcSendRequest(pDnode->trans.clientRpc, pEpSet, pMsg, NULL);
    return 0;
  }
}

static inline void dmRegisterBrokenLinkArg(SRpcMsg *pMsg) {
  SMgmtWrapper *pWrapper = pMsg->info.wrapper;
  if (InChildProc(pWrapper)) {
    dmPutToProcPQueue(&pWrapper->proc, pMsg, DND_FUNC_REGIST);
  } else {
    rpcRegisterBrokenLinkArg(pMsg);
  }
}

static inline void dmReleaseHandle(SRpcHandleInfo *pHandle, int8_t type) {
  SMgmtWrapper *pWrapper = pHandle->wrapper;
  if (InChildProc(pWrapper)) {
    SRpcMsg msg = {.code = type, .info = *pHandle};
    dmPutToProcPQueue(&pWrapper->proc, &msg, DND_FUNC_RELEASE);
  } else {
    rpcReleaseHandle(pHandle, type);
  }
}

static bool rpcRfp(int32_t code, tmsg_t msgType) {
  if (code == TSDB_CODE_RPC_REDIRECT || code == TSDB_CODE_RPC_NETWORK_UNAVAIL || code == TSDB_CODE_NODE_NOT_DEPLOYED ||
      code == TSDB_CODE_SYN_NOT_LEADER || code == TSDB_CODE_APP_NOT_READY || code == TSDB_CODE_RPC_BROKEN_LINK) {
    if (msgType == TDMT_SCH_QUERY || msgType == TDMT_SCH_MERGE_QUERY || msgType == TDMT_SCH_FETCH ||
        msgType == TDMT_SCH_MERGE_FETCH) {
      return false;
    }
    return true;
  } else {
    return false;
  }
}

int32_t dmInitClient(SDnode *pDnode) {
  SDnodeTrans *pTrans = &pDnode->trans;

  SRpcInit rpcInit = {0};
  rpcInit.label = "DND-C";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = (RpcCfp)dmProcessRpcMsg;
  rpcInit.sessions = 1024;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.parent = pDnode;
  rpcInit.rfp = rpcRfp;

  pTrans->clientRpc = rpcOpen(&rpcInit);
  if (pTrans->clientRpc == NULL) {
    dError("failed to init dnode rpc client");
    return -1;
  }

  dDebug("dnode rpc client is initialized");
  return 0;
}

void dmCleanupClient(SDnode *pDnode) {
  SDnodeTrans *pTrans = &pDnode->trans;
  if (pTrans->clientRpc) {
    rpcClose(pTrans->clientRpc);
    pTrans->clientRpc = NULL;
    dDebug("dnode rpc client is closed");
  }
}

int32_t dmInitServer(SDnode *pDnode) {
  SDnodeTrans *pTrans = &pDnode->trans;

  SRpcInit rpcInit = {0};
  strncpy(rpcInit.localFqdn, tsLocalFqdn, strlen(tsLocalFqdn));
  rpcInit.localPort = tsServerPort;
  rpcInit.label = "DND-S";
  rpcInit.numOfThreads = tsNumOfRpcThreads;
  rpcInit.cfp = (RpcCfp)dmProcessRpcMsg;
  rpcInit.sessions = tsMaxShellConns;
  rpcInit.connType = TAOS_CONN_SERVER;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.parent = pDnode;

  pTrans->serverRpc = rpcOpen(&rpcInit);
  if (pTrans->serverRpc == NULL) {
    dError("failed to init dnode rpc server");
    return -1;
  }

  dDebug("dnode rpc server is initialized");
  return 0;
}

void dmCleanupServer(SDnode *pDnode) {
  SDnodeTrans *pTrans = &pDnode->trans;
  if (pTrans->serverRpc) {
    rpcClose(pTrans->serverRpc);
    pTrans->serverRpc = NULL;
    dDebug("dnode rpc server is closed");
  }
}

SMsgCb dmGetMsgcb(SDnode *pDnode) {
  SMsgCb msgCb = {
      .clientRpc = pDnode->trans.clientRpc,
      .sendReqFp = dmSendReq,
      .sendRspFp = dmSendRsp,
      .sendRedirectRspFp = dmSendRedirectRsp,
      .registerBrokenLinkArgFp = dmRegisterBrokenLinkArg,
      .releaseHandleFp = dmReleaseHandle,
      .reportStartupFp = dmReportStartup,
  };
  return msgCb;
}
