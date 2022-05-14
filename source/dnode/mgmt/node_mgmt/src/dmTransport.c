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

#define INTERNAL_USER   "_dnd"
#define INTERNAL_CKEY   "_key"
#define INTERNAL_SECRET "_pwd"

static void dmGetMnodeEpSet(SDnode *pDnode, SEpSet *pEpSet) {
  taosRLockLatch(&pDnode->latch);
  *pEpSet = pDnode->mnodeEps;
  taosRUnLockLatch(&pDnode->latch);
}

static void dmSetMnodeEpSet(SDnode *pDnode, SEpSet *pEpSet) {
  dInfo("mnode is changed, num:%d use:%d", pEpSet->numOfEps, pEpSet->inUse);

  taosWLockLatch(&pDnode->latch);
  pDnode->mnodeEps = *pEpSet;
  for (int32_t i = 0; i < pEpSet->numOfEps; ++i) {
    dInfo("mnode index:%d %s:%u", i, pEpSet->eps[i].fqdn, pEpSet->eps[i].port);
  }

  taosWUnLockLatch(&pDnode->latch);
}

static inline NodeMsgFp dmGetMsgFp(SMgmtWrapper *pWrapper, SRpcMsg *pRpc) {
  NodeMsgFp msgFp = pWrapper->msgFps[TMSG_INDEX(pRpc->msgType)];
  if (msgFp == NULL) {
    terrno = TSDB_CODE_MSG_NOT_PROCESSED;
  }

  return msgFp;
}

static inline int32_t dmBuildNodeMsg(SNodeMsg *pMsg, SRpcMsg *pRpc) {
  SRpcConnInfo connInfo = {0};
  if ((pRpc->msgType & 1U) && rpcGetConnInfo(pRpc->handle, &connInfo) != 0) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    dError("failed to build msg since %s, app:%p handle:%p", terrstr(), pRpc->ahandle, pRpc->handle);
    return -1;
  }

  memcpy(pMsg->user, connInfo.user, TSDB_USER_LEN);
  pMsg->clientIp = connInfo.clientIp;
  pMsg->clientPort = connInfo.clientPort;
  memcpy(&pMsg->rpcMsg, pRpc, sizeof(SRpcMsg));
  if ((pRpc->msgType & 1u)) {
    assert(pRpc->refId != 0);
  }

  return 0;
}

int32_t dmProcessNodeMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SRpcMsg *pRpc = &pMsg->rpcMsg;

  if (InParentProc(pWrapper->proc.ptype)) {
    dTrace("msg:%p, created and put into child queue, type:%s handle:%p user:%s code:0x%04x contLen:%d", pMsg,
           TMSG_INFO(pRpc->msgType), pRpc->handle, pMsg->user, pRpc->code & 0XFFFF, pRpc->contLen);
    return dmPutToProcCQueue(&pWrapper->proc, pMsg, sizeof(SNodeMsg), pRpc->pCont, pRpc->contLen,
                             ((pRpc->msgType & 1U) && (pRpc->code == 0)) ? pRpc->handle : NULL, pRpc->refId,
                             PROC_FUNC_REQ);
  } else {
    dTrace("msg:%p, created, type:%s handle:%p user:%s", pMsg, TMSG_INFO(pRpc->msgType), pRpc->handle, pMsg->user);
    NodeMsgFp msgFp = dmGetMsgFp(pWrapper, &pMsg->rpcMsg);
    if (msgFp == NULL) return -1;
    return (*msgFp)(pWrapper->pMgmt, pMsg);
  }
}

static void dmProcessRpcMsg(SMgmtWrapper *pWrapper, SRpcMsg *pRpc, SEpSet *pEpSet) {
  int32_t   code = -1;
  SNodeMsg *pMsg = NULL;
  uint16_t  msgType = pRpc->msgType;
  bool      needRelease = false;
  bool      isReq = msgType & 1U;

  if (dmMarkWrapper(pWrapper) != 0) goto _OVER;
  needRelease = true;

  if ((pMsg = taosAllocateQitem(sizeof(SNodeMsg), RPC_QITEM)) == NULL) goto _OVER;
  if (dmBuildNodeMsg(pMsg, pRpc) != 0) goto _OVER;

  code = dmProcessNodeMsg(pWrapper, pMsg);

_OVER:
  if (code == 0) {
    if (InParentProc(pWrapper->proc.ptype)) {
      dTrace("msg:%p, freed in parent process", pMsg);
      taosFreeQitem(pMsg);
      rpcFreeCont(pRpc->pCont);
    }
  } else {
    dError("msg:%p, type:%s handle:%p failed to process since 0x%04x:%s", pMsg, TMSG_INFO(msgType), pRpc->handle,
           code & 0XFFFF, terrstr());
    if (isReq) {
      if (terrno != 0) code = terrno;
      if (code == TSDB_CODE_NODE_NOT_DEPLOYED || code == TSDB_CODE_NODE_OFFLINE) {
        if (msgType > TDMT_MND_MSG && msgType < TDMT_VND_MSG) {
          code = TSDB_CODE_NODE_REDIRECT;
        }
      }

      SRpcMsg rsp = {.handle = pRpc->handle, .ahandle = pRpc->ahandle, .code = code, .refId = pRpc->refId};
      tmsgSendRsp(&rsp);
    }
    dTrace("msg:%p, is freed", pMsg);
    taosFreeQitem(pMsg);
    rpcFreeCont(pRpc->pCont);
  }

  if (needRelease) {
    dmReleaseWrapper(pWrapper);
  }
}

static void dmProcessMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SDnodeTrans  *pTrans = &pDnode->trans;
  tmsg_t        msgType = pMsg->msgType;
  bool          isReq = msgType & 1u;
  SMsgHandle   *pHandle = &pTrans->msgHandles[TMSG_INDEX(msgType)];
  SMgmtWrapper *pWrapper = NULL;

  switch (msgType) {
    case TDMT_DND_SERVER_STATUS:
      if (pDnode->status != DND_STAT_RUNNING) {
        dTrace("server status req will be processed, handle:%p, app:%p", pMsg->handle, pMsg->ahandle);
        dmProcessServerStartupStatus(pDnode, pMsg);
        return;
      } else {
        break;
      }
    case TDMT_DND_NET_TEST:
      dTrace("net test req will be processed, handle:%p, app:%p", pMsg->handle, pMsg->ahandle);
      dmProcessNetTestReq(pDnode, pMsg);
      return;
    case TDMT_MND_SYSTABLE_RETRIEVE_RSP:
    case TDMT_VND_FETCH_RSP:
      dTrace("retrieve rsp is received");
      qWorkerProcessFetchRsp(NULL, NULL, pMsg);
      pMsg->pCont = NULL;  // already freed in qworker
      return;
  }

  if (pDnode->status != DND_STAT_RUNNING) {
    dError("msg:%s ignored since dnode not running, handle:%p app:%p", TMSG_INFO(msgType), pMsg->handle, pMsg->ahandle);
    if (isReq) {
      SRpcMsg rspMsg = {
          .handle = pMsg->handle, .code = TSDB_CODE_APP_NOT_READY, .ahandle = pMsg->ahandle, .refId = pMsg->refId};
      rpcSendResponse(&rspMsg);
    }
    rpcFreeCont(pMsg->pCont);
    return;
  }

  if (isReq && pMsg->pCont == NULL) {
    dError("req:%s not processed since its empty, handle:%p app:%p", TMSG_INFO(msgType), pMsg->handle, pMsg->ahandle);
    SRpcMsg rspMsg = {
        .handle = pMsg->handle, .code = TSDB_CODE_INVALID_MSG_LEN, .ahandle = pMsg->ahandle, .refId = pMsg->refId};
    rpcSendResponse(&rspMsg);
    return;
  }

  if (pHandle->defaultNtype == NODE_END) {
    dError("msg:%s not processed since no handle, handle:%p app:%p", TMSG_INFO(msgType), pMsg->handle, pMsg->ahandle);
    if (isReq) {
      SRpcMsg rspMsg = {
          .handle = pMsg->handle, .code = TSDB_CODE_MSG_NOT_PROCESSED, .ahandle = pMsg->ahandle, .refId = pMsg->refId};
      rpcSendResponse(&rspMsg);
    }
    rpcFreeCont(pMsg->pCont);
    return;
  }

  pWrapper = &pDnode->wrappers[pHandle->defaultNtype];
  if (pHandle->needCheckVgId) {
    SMsgHead *pHead = pMsg->pCont;
    int32_t   vgId = ntohl(pHead->vgId);
    if (vgId == QNODE_HANDLE) {
      pWrapper = &pDnode->wrappers[QNODE];
    } else if (vgId == MNODE_HANDLE) {
      pWrapper = &pDnode->wrappers[MNODE];
    } else {
    }
  }

  dTrace("msg:%s will be processed by %s, app:%p", TMSG_INFO(msgType), pWrapper->name, pMsg->ahandle);
  if (isReq) {
    assert(pMsg->refId != 0);
  }
  dmProcessRpcMsg(pWrapper, pMsg, pEpSet);
}

int32_t dmInitMsgHandle(SDnode *pDnode) {
  SDnodeTrans *pTrans = &pDnode->trans;

  for (EDndNodeType ntype = DNODE; ntype < NODE_END; ++ntype) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[ntype];
    SArray       *pArray = (*pWrapper->func.getHandlesFp)();
    if (pArray == NULL) return -1;

    for (int32_t i = 0; i < taosArrayGetSize(pArray); ++i) {
      SMgmtHandle *pMgmt = taosArrayGet(pArray, i);
      SMsgHandle  *pHandle = &pTrans->msgHandles[TMSG_INDEX(pMgmt->msgType)];
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

static void dmSendRpcRedirectRsp(SDnode *pDnode, const SRpcMsg *pReq) {
  SEpSet epSet = {0};
  dmGetMnodeEpSet(pDnode, &epSet);

  dDebug("RPC %p, req is redirected, num:%d use:%d", pReq->handle, epSet.numOfEps, epSet.inUse);
  for (int32_t i = 0; i < epSet.numOfEps; ++i) {
    dDebug("mnode index:%d %s:%u", i, epSet.eps[i].fqdn, epSet.eps[i].port);
    if (strcmp(epSet.eps[i].fqdn, pDnode->input.localFqdn) == 0 && epSet.eps[i].port == pDnode->input.serverPort) {
      epSet.inUse = (i + 1) % epSet.numOfEps;
    }

    epSet.eps[i].port = htons(epSet.eps[i].port);
  }
  SRpcMsg resp;
  SMEpSet msg = {.epSet = epSet};
  int32_t len = tSerializeSMEpSet(NULL, 0, &msg);
  resp.pCont = rpcMallocCont(len);
  resp.contLen = len;
  tSerializeSMEpSet(resp.pCont, len, &msg);

  resp.code = TSDB_CODE_RPC_REDIRECT;
  resp.handle = pReq->handle;
  resp.refId = pReq->refId;
  rpcSendResponse(&resp);
}

static inline void dmSendRpcRsp(SDnode *pDnode, const SRpcMsg *pRsp) {
  if (pRsp->code == TSDB_CODE_NODE_REDIRECT) {
    dmSendRpcRedirectRsp(pDnode, pRsp);
  } else {
    rpcSendResponse(pRsp);
  }
}

static inline void dmSendRecv(SDnode *pDnode, SEpSet *pEpSet, SRpcMsg *pReq, SRpcMsg *pRsp) {
  if (pDnode->status != DND_STAT_RUNNING) {
    pRsp->code = TSDB_CODE_NODE_OFFLINE;
    rpcFreeCont(pReq->pCont);
    pReq->pCont = NULL;
  } else {
    rpcSendRecv(pDnode->trans.clientRpc, pEpSet, pReq, pRsp);
  }
}

static inline void dmSendToMnodeRecv(SMgmtWrapper *pWrapper, SRpcMsg *pReq, SRpcMsg *pRsp) {
  SEpSet epSet = {0};
  dmGetMnodeEpSet(pWrapper->pDnode, &epSet);
  dmSendRecv(pWrapper->pDnode, &epSet, pReq, pRsp);
}

static inline int32_t dmSendReq(SMgmtWrapper *pWrapper, const SEpSet *pEpSet, SRpcMsg *pReq) {
  SDnode *pDnode = pWrapper->pDnode;
  if (pDnode->status != DND_STAT_RUNNING || pDnode->trans.clientRpc == NULL) {
    rpcFreeCont(pReq->pCont);
    pReq->pCont = NULL;
    terrno = TSDB_CODE_NODE_OFFLINE;
    dError("failed to send rpc msg since %s, handle:%p", terrstr(), pReq->handle);
    return -1;
  }

  rpcSendRequest(pDnode->trans.clientRpc, pEpSet, pReq, NULL);
  return 0;
}

static inline void dmSendRsp(SMgmtWrapper *pWrapper, const SRpcMsg *pRsp) {
  if (!InChildProc(pWrapper->proc.ptype)) {
    dmSendRpcRsp(pWrapper->pDnode, pRsp);
  } else {
    dmPutToProcPQueue(&pWrapper->proc, pRsp, sizeof(SRpcMsg), pRsp->pCont, pRsp->contLen, PROC_FUNC_RSP);
  }
}

static inline void dmSendRedirectRsp(SMgmtWrapper *pWrapper, const SRpcMsg *pRsp, const SEpSet *pNewEpSet) {
  if (!InChildProc(pWrapper->proc.ptype)) {
    SRpcMsg resp = {0};
    SMEpSet msg = {.epSet = *pNewEpSet};
    int32_t len = tSerializeSMEpSet(NULL, 0, &msg);
    resp.pCont = rpcMallocCont(len);
    resp.contLen = len;
    tSerializeSMEpSet(resp.pCont, len, &msg);

    resp.code = TSDB_CODE_RPC_REDIRECT;
    resp.handle = pRsp->handle;
    resp.refId = pRsp->refId;
    rpcSendResponse(&resp);
  } else {
    dmPutToProcPQueue(&pWrapper->proc, pRsp, sizeof(SRpcMsg), pRsp->pCont, pRsp->contLen, PROC_FUNC_RSP);
  }
}

static inline void dmRegisterBrokenLinkArg(SMgmtWrapper *pWrapper, SRpcMsg *pMsg) {
  if (!InChildProc(pWrapper->proc.ptype)) {
    rpcRegisterBrokenLinkArg(pMsg);
  } else {
    dmPutToProcPQueue(&pWrapper->proc, pMsg, sizeof(SRpcMsg), pMsg->pCont, pMsg->contLen, PROC_FUNC_REGIST);
  }
}

static inline void dmReleaseHandle(SMgmtWrapper *pWrapper, void *handle, int8_t type) {
  if (!InChildProc(pWrapper->proc.ptype)) {
    rpcReleaseHandle(handle, type);
  } else {
    SRpcMsg msg = {.handle = handle, .code = type};
    dmPutToProcPQueue(&pWrapper->proc, &msg, sizeof(SRpcMsg), NULL, 0, PROC_FUNC_RELEASE);
  }
}

static bool rpcRfp(int32_t code) {
  if (code == TSDB_CODE_RPC_REDIRECT) {
    return true;
  } else {
    return false;
  }
}

int32_t dmInitClient(SDnode *pDnode) {
  SDnodeTrans *pTrans = &pDnode->trans;

  SRpcInit rpcInit = {0};
  rpcInit.label = "DND";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = (RpcCfp)dmProcessMsg;
  rpcInit.sessions = 1024;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.user = INTERNAL_USER;
  rpcInit.ckey = INTERNAL_CKEY;
  rpcInit.spi = 1;
  rpcInit.parent = pDnode;
  rpcInit.rfp = rpcRfp;

  char pass[TSDB_PASSWORD_LEN + 1] = {0};
  taosEncryptPass_c((uint8_t *)(INTERNAL_SECRET), strlen(INTERNAL_SECRET), pass);
  rpcInit.secret = pass;

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

static inline int32_t dmGetHideUserAuth(SDnode *pDnode, char *user, char *spi, char *encrypt, char *secret,
                                        char *ckey) {
  int32_t code = 0;
  char    pass[TSDB_PASSWORD_LEN + 1] = {0};

  if (strcmp(user, INTERNAL_USER) == 0) {
    taosEncryptPass_c((uint8_t *)(INTERNAL_SECRET), strlen(INTERNAL_SECRET), pass);
  } else if (strcmp(user, TSDB_NETTEST_USER) == 0) {
    taosEncryptPass_c((uint8_t *)(TSDB_NETTEST_USER), strlen(TSDB_NETTEST_USER), pass);
  } else {
    code = -1;
  }

  if (code == 0) {
    memcpy(secret, pass, TSDB_PASSWORD_LEN);
    *spi = 1;
    *encrypt = 0;
    *ckey = 0;
  }

  return code;
}

static inline int32_t dmRetrieveUserAuthInfo(SDnode *pDnode, char *user, char *spi, char *encrypt, char *secret,
                                             char *ckey) {
  if (dmGetHideUserAuth(pDnode, user, spi, encrypt, secret, ckey) == 0) {
    dTrace("user:%s, get auth from mnode, spi:%d encrypt:%d", user, *spi, *encrypt);
    return 0;
  }

  SAuthReq authReq = {0};
  tstrncpy(authReq.user, user, TSDB_USER_LEN);
  int32_t contLen = tSerializeSAuthReq(NULL, 0, &authReq);
  void   *pReq = rpcMallocCont(contLen);
  tSerializeSAuthReq(pReq, contLen, &authReq);

  SRpcMsg rpcMsg = {.pCont = pReq, .contLen = contLen, .msgType = TDMT_MND_AUTH, .ahandle = (void *)9528};
  SRpcMsg rpcRsp = {0};
  SEpSet  epSet = {0};
  dTrace("user:%s, send user auth req to other mnodes, spi:%d encrypt:%d", user, authReq.spi, authReq.encrypt);
  dmGetMnodeEpSet(pDnode, &epSet);
  dmSendRecv(pDnode, &epSet, &rpcMsg, &rpcRsp);

  if (rpcRsp.code != 0) {
    terrno = rpcRsp.code;
    dError("user:%s, failed to get user auth from other mnodes since %s", user, terrstr());
  } else {
    SAuthRsp authRsp = {0};
    tDeserializeSAuthReq(rpcRsp.pCont, rpcRsp.contLen, &authRsp);
    memcpy(secret, authRsp.secret, TSDB_PASSWORD_LEN);
    memcpy(ckey, authRsp.ckey, TSDB_PASSWORD_LEN);
    *spi = authRsp.spi;
    *encrypt = authRsp.encrypt;
    dTrace("user:%s, success to get user auth from other mnodes, spi:%d encrypt:%d", user, authRsp.spi,
           authRsp.encrypt);
  }

  rpcFreeCont(rpcRsp.pCont);
  return rpcRsp.code;
}

int32_t dmInitServer(SDnode *pDnode) {
  SDnodeTrans *pTrans = &pDnode->trans;

  SRpcInit rpcInit = {0};

  strncpy(rpcInit.localFqdn, pDnode->input.localFqdn, strlen(pDnode->input.localFqdn));
  rpcInit.localPort = pDnode->input.serverPort;
  rpcInit.label = "DND";
  rpcInit.numOfThreads = tsNumOfRpcThreads;
  rpcInit.cfp = (RpcCfp)dmProcessMsg;
  rpcInit.sessions = tsMaxShellConns;
  rpcInit.connType = TAOS_CONN_SERVER;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.afp = (RpcAfp)dmRetrieveUserAuthInfo;
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

SMsgCb dmGetMsgcb(SMgmtWrapper *pWrapper) {
  SMsgCb msgCb = {
      .pWrapper = pWrapper,
      .clientRpc = pWrapper->pDnode->trans.clientRpc,
      .sendReqFp = dmSendReq,
      .sendRspFp = dmSendRsp,
      .sendMnodeRecvFp = dmSendToMnodeRecv,
      .sendRedirectRspFp = dmSendRedirectRsp,
      .registerBrokenLinkArgFp = dmRegisterBrokenLinkArg,
      .releaseHandleFp = dmReleaseHandle,
      .reportStartupFp = dmReportStartupByWrapper,
  };
  return msgCb;
}
