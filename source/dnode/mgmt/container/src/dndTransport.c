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

static inline void dndProcessQVnodeRpcMsg(SMsgHandle *pHandle, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SMsgHead *pHead = pMsg->pCont;
  int32_t   vgId = htonl(pHead->vgId);

  SMgmtWrapper *pWrapper = pHandle->pWrapper;
  if (vgId == pHandle->vgId && pHandle->pVgIdWrapper != NULL) {
    pWrapper = pHandle->pVgIdWrapper;
  }

  dTrace("msg:%s will be processed by %s, handle:%p app:%p vgId:%d", TMSG_INFO(pMsg->msgType), pWrapper->name,
         pMsg->handle, pMsg->ahandle, vgId);
  dndProcessRpcMsg(pWrapper, pMsg, pEpSet);
}

static void dndProcessResponse(void *parent, SRpcMsg *pRsp, SEpSet *pEpSet) {
  SDnode     *pDnode = parent;
  STransMgmt *pMgmt = &pDnode->trans;
  tmsg_t      msgType = pRsp->msgType;

  if (dndGetStatus(pDnode) != DND_STAT_RUNNING) {
    dTrace("rsp:%s ignored since dnode not running, handle:%p app:%p", TMSG_INFO(msgType), pRsp->handle, pRsp->ahandle);
    rpcFreeCont(pRsp->pCont);
    return;
  }

  SMsgHandle *pHandle = &pMgmt->msgHandles[TMSG_INDEX(msgType)];
  if (pHandle->msgFp != NULL) {
    if (pHandle->vgId == 0) {
      dTrace("rsp:%s will be processed by %s, handle:%p app:%p code:0x%04x:%s", TMSG_INFO(msgType),
             pHandle->pWrapper->name, pRsp->handle, pRsp->ahandle, pRsp->code & 0XFFFF, tstrerror(pRsp->code));
      dndProcessRpcMsg(pHandle->pWrapper, pRsp, pEpSet);
    } else {
      dndProcessQVnodeRpcMsg(pHandle, pRsp, pEpSet);
    }
  } else {
    dError("rsp:%s not processed since no handle, handle:%p app:%p", TMSG_INFO(msgType), pRsp->handle, pRsp->ahandle);
    rpcFreeCont(pRsp->pCont);
  }
}

int32_t dndInitClient(SDnode *pDnode) {
  STransMgmt *pMgmt = &pDnode->trans;

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.label = "DND";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = dndProcessResponse;
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

void dndCleanupClient(SDnode *pDnode) {
  STransMgmt *pMgmt = &pDnode->trans;
  if (pMgmt->clientRpc) {
    rpcClose(pMgmt->clientRpc);
    pMgmt->clientRpc = NULL;
    dDebug("dnode rpc client is closed");
  }
}

static void dndProcessRequest(void *param, SRpcMsg *pReq, SEpSet *pEpSet) {
  SDnode     *pDnode = param;
  STransMgmt *pMgmt = &pDnode->trans;
  tmsg_t      msgType = pReq->msgType;

  if (msgType == TDMT_DND_NETWORK_TEST) {
    dTrace("network test req will be processed, handle:%p, app:%p", pReq->handle, pReq->ahandle);
    dndProcessStartupReq(pDnode, pReq);
    return;
  }

  if (dndGetStatus(pDnode) != DND_STAT_RUNNING) {
    dError("req:%s ignored since dnode not running, handle:%p app:%p", TMSG_INFO(msgType), pReq->handle, pReq->ahandle);
    SRpcMsg rspMsg = {.handle = pReq->handle, .code = TSDB_CODE_APP_NOT_READY, .ahandle = pReq->ahandle};
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pReq->pCont);
    return;
  }

  if (pReq->pCont == NULL) {
    dTrace("req:%s not processed since its empty, handle:%p app:%p", TMSG_INFO(msgType), pReq->handle, pReq->ahandle);
    SRpcMsg rspMsg = {.handle = pReq->handle, .code = TSDB_CODE_DND_INVALID_MSG_LEN, .ahandle = pReq->ahandle};
    rpcSendResponse(&rspMsg);
    return;
  }

  SMsgHandle *pHandle = &pMgmt->msgHandles[TMSG_INDEX(msgType)];
  if (pHandle->msgFp != NULL) {
    if (pHandle->vgId == 0) {
      dTrace("req:%s will be processed by %s, handle:%p app:%p", TMSG_INFO(msgType), pHandle->pWrapper->name,
             pReq->handle, pReq->ahandle);
      dndProcessRpcMsg(pHandle->pWrapper, pReq, pEpSet);
    } else {
      dndProcessQVnodeRpcMsg(pHandle, pReq, pEpSet);
    }
  } else {
    dError("req:%s not processed since no handle, handle:%p app:%p", TMSG_INFO(msgType), pReq->handle, pReq->ahandle);
    SRpcMsg rspMsg = {.handle = pReq->handle, .code = TSDB_CODE_MSG_NOT_PROCESSED, .ahandle = pReq->ahandle};
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pReq->pCont);
  }
}

static void dndSendMsgToMnodeRecv(SDnode *pDnode, SRpcMsg *pRpcMsg, SRpcMsg *pRpcRsp) {
  STransMgmt *pMgmt = &pDnode->trans;

  SEpSet epSet = {0};
  dmGetMnodeEpSet(dndAcquireWrapper(pDnode, DNODE)->pMgmt, &epSet);
  rpcSendRecv(pMgmt->clientRpc, &epSet, pRpcMsg, pRpcRsp);
}

static int32_t dndGetHideUserAuth(SDnode *pDnode, char *user, char *spi, char *encrypt, char *secret, char *ckey) {
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

static int32_t dndRetrieveUserAuthInfo(void *parent, char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  SDnode *pDnode = parent;

  if (dndGetHideUserAuth(parent, user, spi, encrypt, secret, ckey) == 0) {
    dTrace("user:%s, get auth from mnode, spi:%d encrypt:%d", user, *spi, *encrypt);
    return 0;
  }

  if (mmGetUserAuth(dndAcquireWrapper(pDnode, MNODE), user, spi, encrypt, secret, ckey) == 0) {
    dTrace("user:%s, get auth from mnode, spi:%d encrypt:%d", user, *spi, *encrypt);
    return 0;
  }

  if (terrno != TSDB_CODE_APP_NOT_READY) {
    dTrace("failed to get user auth from mnode since %s", terrstr());
    return -1;
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

int32_t dndInitServer(SDnode *pDnode) {
  STransMgmt *pMgmt = &pDnode->trans;

  int32_t numOfThreads = (int32_t)((tsNumOfCores * tsNumOfThreadsPerCore) / 2.0);
  if (numOfThreads < 1) {
    numOfThreads = 1;
  }

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort = pDnode->serverPort;
  rpcInit.label = "DND";
  rpcInit.numOfThreads = numOfThreads;
  rpcInit.cfp = dndProcessRequest;
  rpcInit.sessions = tsMaxShellConns;
  rpcInit.connType = TAOS_CONN_SERVER;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.afp = dndRetrieveUserAuthInfo;
  rpcInit.parent = pDnode;

  pMgmt->serverRpc = rpcOpen(&rpcInit);
  if (pMgmt->serverRpc == NULL) {
    dError("failed to init dnode rpc server");
    return -1;
  }

  dDebug("dnode rpc server is initialized");
  return 0;
}

void dndCleanupServer(SDnode *pDnode) {
  STransMgmt *pMgmt = &pDnode->trans;
  if (pMgmt->serverRpc) {
    rpcClose(pMgmt->serverRpc);
    pMgmt->serverRpc = NULL;
    dDebug("dnode rpc server is closed");
  }
}

int32_t dndInitMsgHandle(SDnode *pDnode) {
  STransMgmt *pMgmt = &pDnode->trans;

  for (ENodeType n = 0; n < NODE_MAX; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];

    for (int32_t msgIndex = 0; msgIndex < TDMT_MAX; ++msgIndex) {
      NodeMsgFp msgFp = pWrapper->msgFps[msgIndex];
      int32_t   vgId = pWrapper->msgVgIds[msgIndex];
      if (msgFp == NULL) continue;

      SMsgHandle *pHandle = &pMgmt->msgHandles[msgIndex];
      if (pHandle->msgFp != NULL && pHandle->vgId == vgId) {
        dError("msg:%s has multiple process nodes, prev node:%s:%d, curr node:%s:%d", tMsgInfo[msgIndex],
               pHandle->pWrapper->name, pHandle->pWrapper->msgVgIds[msgIndex], pWrapper->name, vgId);
        return -1;
      } else {
        dTrace("msg:%s will be processed by %s, vgId:%d", tMsgInfo[msgIndex], pWrapper->name, vgId);
        if (vgId == 0) {
          pHandle->msgFp = msgFp;
          pHandle->pWrapper = pWrapper;
        } else {
          pHandle->vgId = vgId;
          pHandle->vgIdMsgFp = msgFp;
          pHandle->pVgIdWrapper = pWrapper;
        }
      }
    }
  }

  return 0;
}

static int32_t dndSendRpcReq(STransMgmt *pMgmt, SEpSet *pEpSet, SRpcMsg *pReq) {
  if (pMgmt->clientRpc == NULL) {
    terrno = TSDB_CODE_DND_OFFLINE;
    return -1;
  }

  rpcSendRequest(pMgmt->clientRpc, pEpSet, pReq, NULL);
  return 0;
}

int32_t dndSendReqToDnode(SMgmtWrapper *pWrapper, SEpSet *pEpSet, SRpcMsg *pReq) {
  if (pWrapper->procType == PROC_CHILD) {
  } else {
    SDnode *pDnode = pWrapper->pDnode;
    if (dndGetStatus(pDnode) != DND_STAT_RUNNING) {
      terrno = TSDB_CODE_DND_OFFLINE;
      dError("failed to send rpc msg since %s, handle:%p", terrstr(), pReq->handle);
      return -1;
    }
    return dndSendRpcReq(&pDnode->trans, pEpSet, pReq);
  }
}

int32_t dndSendReqToMnode(SMgmtWrapper *pWrapper, SRpcMsg *pReq) {
  if (pWrapper->procType == PROC_CHILD) {
  } else {
    SDnode     *pDnode = pWrapper->pDnode;
    STransMgmt *pTrans = &pDnode->trans;
    SEpSet      epSet = {0};
    dmGetMnodeEpSet(dndAcquireWrapper(pDnode, DNODE)->pMgmt, &epSet);
    return dndSendRpcReq(pTrans, &epSet, pReq);
  }
}

void dndSendRpcRsp(SMgmtWrapper *pWrapper, SRpcMsg *pRsp) {
  if (pRsp->code == TSDB_CODE_APP_NOT_READY) {
    SMgmtWrapper *pDnodeWrapper = dndAcquireWrapper(pWrapper->pDnode, DNODE);
    dmSendRedirectRsp(pDnodeWrapper->pMgmt, pRsp);
  } else {
    rpcSendResponse(pRsp);
  }
}

void dndSendRsp(SMgmtWrapper *pWrapper, SRpcMsg *pRsp) {
  if (pWrapper->procType == PROC_CHILD) {
    int32_t code = -1;
    do {
      code = taosProcPutToParentQueue(pWrapper->pProc, pRsp, sizeof(SRpcMsg), pRsp->pCont, pRsp->contLen);
      if (code != 0) {
        taosMsleep(10);
      }
    } while (code != 0);
  } else {
    dndSendRpcRsp(pWrapper, pRsp);
  }
}
