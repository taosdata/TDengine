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

/* this file is mainly responsible for the communication between DNODEs. Each
 * dnode works as both server and client. Dnode may send status, grant, config
 * messages to mnode, mnode may send create/alter/drop table/vnode messages
 * to dnode. All theses messages are handled from here
 */

#define _DEFAULT_SOURCE
#include "dndTransport.h"
#include "dndDnode.h"
#include "dndMnode.h"
#include "dndVnodes.h"

static void dndInitMsgFp(STransMgmt *pMgmt) {
  // msg from client to dnode
  pMgmt->msgFp[TSDB_MSG_TYPE_SUBMIT] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_QUERY] = dndProcessVnodeQueryMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_FETCH] = dndProcessVnodeFetchMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_CREATE_TABLE] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_DROP_TABLE] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_ALTER_TABLE] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_UPDATE_TAG_VAL] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_TABLE_META] = dndProcessVnodeQueryMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_TABLES_META] = dndProcessVnodeQueryMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_MQ_QUERY] = dndProcessVnodeQueryMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_MQ_CONSUME] = dndProcessVnodeQueryMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_MQ_CONNECT] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_MQ_DISCONNECT] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_MQ_ACK] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_MQ_RESET] = dndProcessVnodeWriteMsg;

  // msg from client to mnode
  pMgmt->msgFp[TSDB_MSG_TYPE_CONNECT] = dndProcessMnodeReadMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_CREATE_ACCT] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_ALTER_ACCT] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_DROP_ACCT] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_CREATE_USER] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_ALTER_USER] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_DROP_USER] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_CREATE_DNODE] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_CONFIG_DNODE] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_DROP_DNODE] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_CREATE_DB] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_DROP_DB] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_USE_DB] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_ALTER_DB] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_SYNC_DB] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_CREATE_TOPIC] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_DROP_TOPIC] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_ALTER_TOPIC] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_CREATE_FUNCTION] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_ALTER_FUNCTION] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_DROP_FUNCTION] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_CREATE_STABLE] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_ALTER_STABLE] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_DROP_STABLE] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_STABLE_VGROUP] = dndProcessMnodeReadMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_KILL_QUERY] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_KILL_CONN] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_HEARTBEAT] = dndProcessMnodeReadMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_SHOW] = dndProcessMnodeReadMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_SHOW_RETRIEVE] = dndProcessMnodeReadMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_SHOW_RETRIEVE_FUNC] = dndProcessMnodeReadMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_COMPACT_VNODE] = dndProcessMnodeWriteMsg;

  // message from client to dnode
  pMgmt->msgFp[TSDB_MSG_TYPE_NETWORK_TEST] = dndProcessDnodeReq;

  // message from mnode to vnode
  pMgmt->msgFp[TSDB_MSG_TYPE_CREATE_STABLE_IN] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_CREATE_STABLE_IN_RSP] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_ALTER_STABLE_IN] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_ALTER_STABLE_IN_RSP] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_DROP_STABLE_IN] = dndProcessVnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_DROP_STABLE_IN_RSP] = dndProcessMnodeWriteMsg;

  // message from mnode to dnode
  pMgmt->msgFp[TSDB_MSG_TYPE_CREATE_VNODE_IN] = dndProcessVnodeMgmtMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_CREATE_VNODE_IN_RSP] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_ALTER_VNODE_IN] = dndProcessVnodeMgmtMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_ALTER_VNODE_IN_RSP] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_DROP_VNODE_IN] = dndProcessVnodeMgmtMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_DROP_VNODE_IN_RSP] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_SYNC_VNODE_IN] = dndProcessVnodeMgmtMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_SYNC_VNODE_IN_RSP] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_AUTH_VNODE_IN] = dndProcessVnodeMgmtMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_AUTH_VNODE_IN_RSP] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_COMPACT_VNODE_IN] = dndProcessVnodeMgmtMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_COMPACT_VNODE_IN_RSP] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_CREATE_MNODE_IN] = dndProcessMnodeMgmtMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_CREATE_MNODE_IN_RSP] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_ALTER_MNODE_IN] = dndProcessMnodeMgmtMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_ALTER_MNODE_IN_RSP] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_DROP_MNODE_IN] = dndProcessMnodeMgmtMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_DROP_MNODE_IN_RSP] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_CONFIG_DNODE_IN] = dndProcessDnodeReq;
  pMgmt->msgFp[TSDB_MSG_TYPE_CONFIG_DNODE_IN_RSP] = dndProcessMnodeWriteMsg;

  // message from dnode to mnode
  pMgmt->msgFp[TSDB_MSG_TYPE_AUTH] = dndProcessMnodeReadMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_AUTH_RSP] = dndProcessDnodeRsp;
  pMgmt->msgFp[TSDB_MSG_TYPE_GRANT] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_GRANT_RSP] = dndProcessDnodeRsp;
  pMgmt->msgFp[TSDB_MSG_TYPE_STATUS] = dndProcessMnodeWriteMsg;
  pMgmt->msgFp[TSDB_MSG_TYPE_STATUS_RSP] = dndProcessDnodeRsp;
}

static void dndProcessResponse(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SDnode     *pDnode = parent;
  STransMgmt *pMgmt = &pDnode->t;

  int32_t msgType = pMsg->msgType;

  if (dndGetStat(pDnode) == DND_STAT_STOPPED) {
    if (pMsg == NULL || pMsg->pCont == NULL) return;
    dTrace("RPC %p, rsp:%s is ignored since dnode is stopping", pMsg->handle, taosMsg[msgType]);
    rpcFreeCont(pMsg->pCont);
    return;
  }

  DndMsgFp fp = pMgmt->msgFp[msgType];
  if (fp != NULL) {
    dTrace("RPC %p, rsp:%s will be processed, code:%s", pMsg->handle, taosMsg[msgType], tstrerror(pMsg->code));
    (*fp)(pDnode, pMsg, pEpSet);
  } else {
    dError("RPC %p, rsp:%s not processed", pMsg->handle, taosMsg[msgType]);
    rpcFreeCont(pMsg->pCont);
  }
}

static int32_t dndInitClient(SDnode *pDnode) {
  STransMgmt *pMgmt = &pDnode->t;

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.label = "DND-C";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = dndProcessResponse;
  rpcInit.sessions = TSDB_MAX_VNODES << 4;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.idleTime = pDnode->opt.shellActivityTimer * 1000;
  rpcInit.user = "-internal";
  rpcInit.ckey = "-key";
  rpcInit.secret = "-secret";

  pMgmt->clientRpc = rpcOpen(&rpcInit);
  if (pMgmt->clientRpc == NULL) {
    dError("failed to init rpc client");
    return -1;
  }

  return 0;
}

static void dndCleanupClient(SDnode *pDnode) {
  STransMgmt *pMgmt = &pDnode->t;
  if (pMgmt->clientRpc) {
    rpcClose(pMgmt->clientRpc);
    pMgmt->clientRpc = NULL;
    dInfo("dnode peer rpc client is closed");
  }
}

static void dndProcessRequest(void *param, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SDnode *pDnode = param;
  STransMgmt *pMgmt = &pDnode->t;

  int32_t msgType = pMsg->msgType;
  if (msgType == TSDB_MSG_TYPE_NETWORK_TEST) {
    dndProcessDnodeReq(pDnode, pMsg, pEpSet);
    return;
  }

  if (dndGetStat(pDnode) == DND_STAT_STOPPED) {
    dError("RPC %p, req:%s is ignored since dnode exiting", pMsg->handle, taosMsg[msgType]);
    SRpcMsg rspMsg = {.handle = pMsg->handle, .code = TSDB_CODE_DND_EXITING};
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
    return;
  } else if (dndGetStat(pDnode) != DND_STAT_RUNNING) {
    dError("RPC %p, req:%s is ignored since dnode not running", pMsg->handle, taosMsg[msgType]);
    SRpcMsg rspMsg = {.handle = pMsg->handle, .code = TSDB_CODE_APP_NOT_READY};
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
    return;
  }

  if (pMsg->pCont == NULL) {
    SRpcMsg rspMsg = {.handle = pMsg->handle, .code = TSDB_CODE_DND_INVALID_MSG_LEN};
    rpcSendResponse(&rspMsg);
    return;
  }

  DndMsgFp fp = pMgmt->msgFp[msgType];
  if (fp != NULL) {
    dTrace("RPC %p, req:%s will be processed", pMsg->handle, taosMsg[msgType]);
    (*fp)(pDnode, pMsg, pEpSet);
  } else {
    dError("RPC %p, req:%s is not processed", pMsg->handle, taosMsg[msgType]);
    SRpcMsg rspMsg = {.handle = pMsg->handle, .code = TSDB_CODE_MSG_NOT_PROCESSED};
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
  }
}

static void dndSendMsgToMnodeRecv(SDnode *pDnode, SRpcMsg *pRpcMsg, SRpcMsg *pRpcRsp) {
  STransMgmt *pMgmt = &pDnode->t;

  SEpSet epSet = {0};
  dndGetMnodeEpSet(pDnode, &epSet);
  rpcSendRecv(pMgmt->clientRpc, &epSet, pRpcMsg, pRpcRsp);
}

static int32_t dndRetrieveUserAuthInfo(void *parent, char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  SDnode *pDnode = parent;

  if (dndGetUserAuthFromMnode(pDnode, user, spi, encrypt, secret, ckey) != 0) {
    if (terrno != TSDB_CODE_APP_NOT_READY) {
      dTrace("failed to get user auth from mnode since %s", terrstr());
      return -1;
    }
  }

  dDebug("user:%s, send auth msg to mnodes", user);

  SAuthMsg *pMsg = rpcMallocCont(sizeof(SAuthMsg));
  tstrncpy(pMsg->user, user, TSDB_USER_LEN);

  SRpcMsg rpcMsg = {.pCont = pMsg, .contLen = sizeof(SAuthMsg), .msgType = TSDB_MSG_TYPE_AUTH};
  SRpcMsg rpcRsp = {0};
  dndSendMsgToMnodeRecv(pDnode, &rpcMsg, &rpcRsp);

  if (rpcRsp.code != 0) {
    terrno = rpcRsp.code;
    dError("user:%s, failed to get user auth from mnodes since %s", user, terrstr());
  } else {
    SAuthRsp *pRsp = rpcRsp.pCont;
    memcpy(secret, pRsp->secret, TSDB_KEY_LEN);
    memcpy(ckey, pRsp->ckey, TSDB_KEY_LEN);
    *spi = pRsp->spi;
    *encrypt = pRsp->encrypt;
    dDebug("user:%s, success to get user auth from mnodes", user);
  }

  rpcFreeCont(rpcRsp.pCont);
  return rpcRsp.code;
}

static int32_t dndInitServer(SDnode *pDnode) {
  STransMgmt *pMgmt = &pDnode->t;
  dndInitMsgFp(pMgmt);

  int32_t numOfThreads = (int32_t)((pDnode->opt.numOfCores * pDnode->opt.numOfThreadsPerCore) / 2.0);
  if (numOfThreads < 1) {
    numOfThreads = 1;
  }

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort = pDnode->opt.serverPort;
  rpcInit.label = "DND-S";
  rpcInit.numOfThreads = numOfThreads;
  rpcInit.cfp = dndProcessRequest;
  rpcInit.sessions = pDnode->opt.maxShellConns;
  rpcInit.connType = TAOS_CONN_SERVER;
  rpcInit.idleTime = pDnode->opt.shellActivityTimer * 1000;
  rpcInit.afp = dndRetrieveUserAuthInfo;

  pMgmt->serverRpc = rpcOpen(&rpcInit);
  if (pMgmt->serverRpc == NULL) {
    dError("failed to init rpc server");
    return -1;
  }

  return 0;
}

static void dndCleanupServer(SDnode *pDnode) {
  STransMgmt *pMgmt = &pDnode->t;
  if (pMgmt->serverRpc) {
    rpcClose(pMgmt->serverRpc);
    pMgmt->serverRpc = NULL;
  }
}

int32_t dndInitTrans(SDnode *pDnode) {
  if (dndInitClient(pDnode) != 0) {
    return -1;
  }

  if (dndInitServer(pDnode) != 0) {
    return -1;
  }

  dInfo("dnode-transport is initialized");
  return 0;
}

void dndCleanupTrans(SDnode *pDnode) {
  dndCleanupServer(pDnode);
  dndCleanupClient(pDnode);
  dInfo("dnode-transport is cleaned up");
}

void dndSendMsgToDnode(SDnode *pDnode, SEpSet *pEpSet, SRpcMsg *pMsg) {
  STransMgmt *pMgmt = &pDnode->t;
  rpcSendRequest(pMgmt->clientRpc, pEpSet, pMsg, NULL);
}

void dndSendMsgToMnode(SDnode *pDnode, SRpcMsg *pMsg) {
  SEpSet epSet = {0};
  dndGetMnodeEpSet(pDnode, &epSet);
  dndSendMsgToDnode(pDnode, &epSet, pMsg);
}