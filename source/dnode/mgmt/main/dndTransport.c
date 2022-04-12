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

#define INTERNAL_USER   "_dnd"
#define INTERNAL_CKEY   "_key"
#define INTERNAL_SECRET "_pwd"

static void dndUpdateMnodeEpSet(SDnode *pDnode, SEpSet *pEpSet) {
  SMgmtWrapper *pWrapper = &pDnode->wrappers[DNODE];
  dmUpdateMnodeEpSet(pWrapper->pMgmt, pEpSet);
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
    dError("failed to build msg since %s, app:%p handle:%p", terrstr(), pRpc->ahandle, pRpc->handle);
    return -1;
  }

  memcpy(pMsg->user, connInfo.user, TSDB_USER_LEN);
  pMsg->clientIp = connInfo.clientIp;
  pMsg->clientPort = connInfo.clientPort;
  memcpy(&pMsg->rpcMsg, pRpc, sizeof(SRpcMsg));
  return 0;
}

static void dndProcessRpcMsg(SMgmtWrapper *pWrapper, SRpcMsg *pRpc, SEpSet *pEpSet) {
  int32_t   code = -1;
  SNodeMsg *pMsg = NULL;
  NodeMsgFp msgFp = NULL;
  uint16_t msgType = pRpc->msgType;

  if (pEpSet && pEpSet->numOfEps > 0 && msgType == TDMT_MND_STATUS_RSP) {
    dndUpdateMnodeEpSet(pWrapper->pDnode, pEpSet);
  }

  if (dndMarkWrapper(pWrapper) != 0) goto _OVER;
  if ((msgFp = dndGetMsgFp(pWrapper, pRpc)) == NULL) goto _OVER;
  if ((pMsg = taosAllocateQitem(sizeof(SNodeMsg))) == NULL) goto _OVER;
  if (dndBuildMsg(pMsg, pRpc) != 0) goto _OVER;

  if (pWrapper->procType == PROC_SINGLE) {
    dTrace("msg:%p, is created, handle:%p user:%s", pMsg, pRpc->handle, pMsg->user);
    code = (*msgFp)(pWrapper, pMsg);
  } else if (pWrapper->procType == PROC_PARENT) {
    dTrace("msg:%p, is created and put into child queue, handle:%p user:%s", pMsg, pRpc->handle, pMsg->user);
    code = taosProcPutToChildQ(pWrapper->pProc, pMsg, sizeof(SNodeMsg), pRpc->pCont, pRpc->contLen, pRpc->handle,
                               PROC_REQ);
  } else {
    dTrace("msg:%p, should not processed in child process, handle:%p user:%s", pMsg, pRpc->handle, pMsg->user);
    ASSERT(1);
  }

_OVER:
  if (code == 0) {
    if (pWrapper->procType == PROC_PARENT) {
      dTrace("msg:%p, is freed in parent process", pMsg);
      taosFreeQitem(pMsg);
      rpcFreeCont(pRpc->pCont);
    }
  } else {
    dError("msg:%p, failed to process since 0x%04x:%s", pMsg, code & 0XFFFF, terrstr());
    if (msgType & 1U) {
      if (terrno != 0) code = terrno;
      if (code == TSDB_CODE_NODE_NOT_DEPLOYED || code == TSDB_CODE_NODE_OFFLINE) {
        if (msgType > TDMT_MND_MSG && msgType < TDMT_VND_MSG) {
          code = TSDB_CODE_NODE_REDIRECT;
        }
      }

      SRpcMsg rsp = {.handle = pRpc->handle, .ahandle = pRpc->ahandle, .code = code};
      tmsgSendRsp(&rsp);
    }
    dTrace("msg:%p, is freed", pMsg);
    taosFreeQitem(pMsg);
    rpcFreeCont(pRpc->pCont);
  }

  dndReleaseWrapper(pWrapper);
}

static void dndProcessMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  STransMgmt   *pMgmt = &pDnode->trans;
  tmsg_t        msgType = pMsg->msgType;
  bool          isReq = msgType & 1u;
  SMsgHandle   *pHandle = &pMgmt->msgHandles[TMSG_INDEX(msgType)];
  SMgmtWrapper *pWrapper = pHandle->pWrapper;

  if (msgType == TDMT_DND_NETWORK_TEST) {
    dTrace("network test req will be processed, handle:%p, app:%p", pMsg->handle, pMsg->ahandle);
    dndProcessStartupReq(pDnode, pMsg);
    return;
  }

  if (dndGetStatus(pDnode) != DND_STAT_RUNNING) {
    dError("msg:%s ignored since dnode not running, handle:%p app:%p", TMSG_INFO(msgType), pMsg->handle, pMsg->ahandle);
    if (isReq) {
      SRpcMsg rspMsg = {.handle = pMsg->handle, .code = TSDB_CODE_APP_NOT_READY, .ahandle = pMsg->ahandle};
      rpcSendResponse(&rspMsg);
    }
    rpcFreeCont(pMsg->pCont);
    return;
  }

  if (isReq && pMsg->pCont == NULL) {
    dError("req:%s not processed since its empty, handle:%p app:%p", TMSG_INFO(msgType), pMsg->handle, pMsg->ahandle);
    SRpcMsg rspMsg = {.handle = pMsg->handle, .code = TSDB_CODE_INVALID_MSG_LEN, .ahandle = pMsg->ahandle};
    rpcSendResponse(&rspMsg);
    return;
  }

  if (pWrapper == NULL) {
    dError("msg:%s not processed since no handle, handle:%p app:%p", TMSG_INFO(msgType), pMsg->handle, pMsg->ahandle);
    if (isReq) {
      SRpcMsg rspMsg = {.handle = pMsg->handle, .code = TSDB_CODE_MSG_NOT_PROCESSED, .ahandle = pMsg->ahandle};
      rpcSendResponse(&rspMsg);
    }
    rpcFreeCont(pMsg->pCont);
  }

  if (pHandle->pMndWrapper != NULL || pHandle->pQndWrapper != NULL) {
    SMsgHead *pHead = pMsg->pCont;
    int32_t   vgId = ntohl(pHead->vgId);
    if (vgId == QNODE_HANDLE) {
      pWrapper = pHandle->pQndWrapper;
    } else if (vgId == MNODE_HANDLE) {
      pWrapper = pHandle->pMndWrapper;
    } else {
    }
  }

  dTrace("msg:%s will be processed by %s, app:%p", TMSG_INFO(msgType), pWrapper->name, pMsg->ahandle);
  dndProcessRpcMsg(pWrapper, pMsg, pEpSet);
}

static int32_t dndInitClient(SDnode *pDnode) {
  STransMgmt *pMgmt = &pDnode->trans;

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.label = "DND";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = (RpcCfp)dndProcessMsg;
  rpcInit.sessions = 1024;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.user = INTERNAL_USER;
  rpcInit.ckey = INTERNAL_CKEY;
  rpcInit.spi = 1;
  rpcInit.parent = pDnode;

  char pass[TSDB_PASSWORD_LEN + 1] = {0};
  taosEncryptPass_c((uint8_t *)(INTERNAL_SECRET), strlen(INTERNAL_SECRET), pass);
  rpcInit.secret = pass;

  pMgmt->clientRpc = rpcOpen(&rpcInit);
  if (pMgmt->clientRpc == NULL) {
    dError("failed to init dnode rpc client");
    return -1;
  }

  dDebug("dnode rpc client is initialized");
  return 0;
}

static void dndCleanupClient(SDnode *pDnode) {
  STransMgmt *pMgmt = &pDnode->trans;
  if (pMgmt->clientRpc) {
    rpcClose(pMgmt->clientRpc);
    pMgmt->clientRpc = NULL;
    dDebug("dnode rpc client is closed");
  }
}

static inline void dndSendMsgToMnodeRecv(SDnode *pDnode, SRpcMsg *pReq, SRpcMsg *pRsp) {
  SEpSet        epSet = {0};
  SMgmtWrapper *pWrapper = &pDnode->wrappers[DNODE];
  dmGetMnodeEpSet(pWrapper->pMgmt, &epSet);
  rpcSendRecv(pDnode->trans.clientRpc, &epSet, pReq, pRsp);
}

static inline int32_t dndGetHideUserAuth(SDnode *pDnode, char *user, char *spi, char *encrypt, char *secret,
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

static int32_t dndRetrieveUserAuthInfo(SDnode *pDnode, char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  if (dndGetHideUserAuth(pDnode, user, spi, encrypt, secret, ckey) == 0) {
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
  dTrace("user:%s, send user auth req to other mnodes, spi:%d encrypt:%d", user, authReq.spi, authReq.encrypt);
  dndSendMsgToMnodeRecv(pDnode, &rpcMsg, &rpcRsp);

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

static int32_t dndInitServer(SDnode *pDnode) {
  STransMgmt *pMgmt = &pDnode->trans;

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort = pDnode->serverPort;
  rpcInit.label = "DND";
  rpcInit.numOfThreads = tsNumOfRpcThreads;
  rpcInit.cfp = (RpcCfp)dndProcessMsg;
  rpcInit.sessions = tsMaxShellConns;
  rpcInit.connType = TAOS_CONN_SERVER;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.afp = (RpcAfp)dndRetrieveUserAuthInfo;
  rpcInit.parent = pDnode;

  pMgmt->serverRpc = rpcOpen(&rpcInit);
  if (pMgmt->serverRpc == NULL) {
    dError("failed to init dnode rpc server");
    return -1;
  }

  dDebug("dnode rpc server is initialized");
  return 0;
}

static void dndCleanupServer(SDnode *pDnode) {
  STransMgmt *pMgmt = &pDnode->trans;
  if (pMgmt->serverRpc) {
    rpcClose(pMgmt->serverRpc);
    pMgmt->serverRpc = NULL;
    dDebug("dnode rpc server is closed");
  }
}

int32_t dndInitTrans(SDnode *pDnode) {
  if (dndInitServer(pDnode) != 0) return -1;
  if (dndInitClient(pDnode) != 0) return -1;
  return 0;
}

void dndCleanupTrans(SDnode *pDnode) {
  dndCleanupServer(pDnode);
  dndCleanupClient(pDnode);
}

int32_t dndInitMsgHandle(SDnode *pDnode) {
  STransMgmt *pMgmt = &pDnode->trans;

  for (EDndType n = 0; n < NODE_MAX; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];

    for (int32_t msgIndex = 0; msgIndex < TDMT_MAX; ++msgIndex) {
      NodeMsgFp msgFp = pWrapper->msgFps[msgIndex];
      int8_t    vgId = pWrapper->msgVgIds[msgIndex];
      if (msgFp == NULL) continue;

      SMsgHandle *pHandle = &pMgmt->msgHandles[msgIndex];
      if (vgId == QNODE_HANDLE) {
        if (pHandle->pQndWrapper != NULL) {
          dError("msg:%s has multiple process nodes", tMsgInfo[msgIndex]);
          return -1;
        }
        pHandle->pQndWrapper = pWrapper;
      } else if (vgId == MNODE_HANDLE) {
        if (pHandle->pMndWrapper != NULL) {
          dError("msg:%s has multiple process nodes", tMsgInfo[msgIndex]);
          return -1;
        }
        pHandle->pMndWrapper = pWrapper;
      } else {
        if (pHandle->pWrapper != NULL) {
          dError("msg:%s has multiple process nodes", tMsgInfo[msgIndex]);
          return -1;
        }
        pHandle->pWrapper = pWrapper;
      }
    }
  }

  return 0;
}

static int32_t dndSendRpcReq(STransMgmt *pMgmt, const SEpSet *pEpSet, SRpcMsg *pReq) {
  if (pMgmt->clientRpc == NULL) {
    terrno = TSDB_CODE_NODE_OFFLINE;
    return -1;
  }

  rpcSendRequest(pMgmt->clientRpc, pEpSet, pReq, NULL);
  return 0;
}

static void dndSendRpcRsp(SMgmtWrapper *pWrapper, const SRpcMsg *pRsp) {
  if (pRsp->code == TSDB_CODE_NODE_REDIRECT) {
    dmSendRedirectRsp(pWrapper->pMgmt, pRsp);
  } else {
    rpcSendResponse(pRsp);
  }
}

static int32_t dndSendReq(SMgmtWrapper *pWrapper, const SEpSet *pEpSet, SRpcMsg *pReq) {
  if (dndGetStatus(pWrapper->pDnode) != DND_STAT_RUNNING) {
    terrno = TSDB_CODE_NODE_OFFLINE;
    dError("failed to send rpc msg since %s, handle:%p", terrstr(), pReq->handle);
    return -1;
  }

  if (pWrapper->procType != PROC_CHILD) {
    return dndSendRpcReq(&pWrapper->pDnode->trans, pEpSet, pReq);
  } else {
    char *pHead = taosMemoryMalloc(sizeof(SRpcMsg) + sizeof(SEpSet));
    if (pHead == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    memcpy(pHead, pReq, sizeof(SRpcMsg));
    memcpy(pHead + sizeof(SRpcMsg), pEpSet, sizeof(SEpSet));
    taosProcPutToParentQ(pWrapper->pProc, pHead, sizeof(SRpcMsg) + sizeof(SEpSet), pReq->pCont, pReq->contLen,
                         PROC_REQ);
    taosMemoryFree(pHead);
    return 0;
  }
}

static void dndSendRsp(SMgmtWrapper *pWrapper, const SRpcMsg *pRsp) {
  if (pWrapper->procType != PROC_CHILD) {
    dndSendRpcRsp(pWrapper, pRsp);
  } else {
    taosProcPutToParentQ(pWrapper->pProc, pRsp, sizeof(SRpcMsg), pRsp->pCont, pRsp->contLen, PROC_RSP);
  }
}

static void dndRegisterBrokenLinkArg(SMgmtWrapper *pWrapper, SRpcMsg *pMsg) {
  if (pWrapper->procType != PROC_CHILD) {
    rpcRegisterBrokenLinkArg(pMsg);
  } else {
    taosProcPutToParentQ(pWrapper->pProc, pMsg, sizeof(SRpcMsg), pMsg->pCont, pMsg->contLen, PROC_REGIST);
  }
}

static void dndReleaseHandle(SMgmtWrapper *pWrapper, void *handle, int8_t type) {
  if (pWrapper->procType != PROC_CHILD) {
    rpcReleaseHandle(handle, type);
  } else {
    SRpcMsg msg = {.handle = handle, .code = type};
    taosProcPutToParentQ(pWrapper->pProc, &msg, sizeof(SRpcMsg), NULL, 0, PROC_RELEASE);
  }
}

SMsgCb dndCreateMsgcb(SMgmtWrapper *pWrapper) {
  SMsgCb msgCb = {
      .pWrapper = pWrapper,
      .sendReqFp = dndSendReq,
      .sendRspFp = dndSendRsp,
      .registerBrokenLinkArgFp = dndRegisterBrokenLinkArg,
      .releaseHandleFp = dndReleaseHandle,
  };
  return msgCb;
}

static void dndConsumeChildQueue(SMgmtWrapper *pWrapper, SNodeMsg *pMsg, int16_t msgLen, void *pCont, int32_t contLen,
                                 ProcFuncType ftype) {
  SRpcMsg *pRpc = &pMsg->rpcMsg;
  pRpc->pCont = pCont;
  dTrace("msg:%p, get from child queue, handle:%p app:%p", pMsg, pRpc->handle, pRpc->ahandle);

  NodeMsgFp msgFp = pWrapper->msgFps[TMSG_INDEX(pRpc->msgType)];
  int32_t   code = (*msgFp)(pWrapper, pMsg);

  if (code != 0) {
    dError("msg:%p, failed to process since code:0x%04x:%s", pMsg, code & 0XFFFF, tstrerror(code));
    if (pRpc->msgType & 1U) {
      SRpcMsg rsp = {.handle = pRpc->handle, .ahandle = pRpc->ahandle, .code = terrno};
      dndSendRsp(pWrapper, &rsp);
    }

    dTrace("msg:%p, is freed", pMsg);
    taosFreeQitem(pMsg);
    rpcFreeCont(pCont);
  }
}

static void dndConsumeParentQueue(SMgmtWrapper *pWrapper, SRpcMsg *pMsg, int16_t msgLen, void *pCont, int32_t contLen,
                                  ProcFuncType ftype) {
  pMsg->pCont = pCont;
  dTrace("msg:%p, get from parent queue, ftype:%d handle:%p code:0x%04x mtype:%d, app:%p", pMsg, ftype, pMsg->handle,
         pMsg->code & 0xFFFF, pMsg->msgType, pMsg->ahandle);

  switch (ftype) {
    case PROC_REGIST:
      rpcRegisterBrokenLinkArg(pMsg);
      break;
    case PROC_RELEASE:
      taosProcRemoveHandle(pWrapper->pProc, pMsg->handle);
      rpcReleaseHandle(pMsg->handle, (int8_t)pMsg->code);
      rpcFreeCont(pCont);
      break;
    case PROC_REQ:
      dndSendRpcReq(&pWrapper->pDnode->trans, (SEpSet *)((char *)pMsg + sizeof(SRpcMsg)), pMsg);
      break;
    case PROC_RSP:
      taosProcRemoveHandle(pWrapper->pProc, pMsg->handle);
      dndSendRpcRsp(pWrapper, pMsg);
      break;
    default:
      break;
  }
  taosMemoryFree(pMsg);
}

SProcCfg dndGenProcCfg(SMgmtWrapper *pWrapper) {
  SProcCfg cfg = {.childConsumeFp = (ProcConsumeFp)dndConsumeChildQueue,
                  .childMallocHeadFp = (ProcMallocFp)taosAllocateQitem,
                  .childFreeHeadFp = (ProcFreeFp)taosFreeQitem,
                  .childMallocBodyFp = (ProcMallocFp)rpcMallocCont,
                  .childFreeBodyFp = (ProcFreeFp)rpcFreeCont,
                  .parentConsumeFp = (ProcConsumeFp)dndConsumeParentQueue,
                  .parentMallocHeadFp = (ProcMallocFp)taosMemoryMalloc,
                  .parentFreeHeadFp = (ProcFreeFp)taosMemoryFree,
                  .parentMallocBodyFp = (ProcMallocFp)rpcMallocCont,
                  .parentFreeBodyFp = (ProcFreeFp)rpcFreeCont,
                  .shm = pWrapper->shm,
                  .parent = pWrapper,
                  .name = pWrapper->name};
  return cfg;
}

void dndSendRecv(SDnode *pDnode, SEpSet *pEpSet, SRpcMsg *pReq, SRpcMsg *pRsp) {
  rpcSendRecv(pDnode->trans.clientRpc, pEpSet, pReq, pRsp);
}