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
#include "dnodeTransport.h"
#include "dnodeConfig.h"
#include "dnodeDnode.h"
#include "mnode.h"
#include "vnode.h"

typedef void (*MsgFp)(SRpcMsg *pMsg);

static struct {
  void *serverRpc;
  void *clientRpc;
  void *shellRpc;
  MsgFp msgFp[TSDB_MSG_TYPE_MAX];
} tsTrans;

static void dnodeInitMsgFp() {
  // msg from client to dnode
  tsTrans.msgFp[TSDB_MSG_TYPE_SUBMIT] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_QUERY] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_FETCH] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_TABLE] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_TABLE] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_TABLE] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_UPDATE_TAG_VAL] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_TABLE_META] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_TABLES_META] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_STABLE_VGROUP] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_MQ_QUERY] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_MQ_CONSUME] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_MQ_CONNECT] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_MQ_DISCONNECT] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_MQ_ACK] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_MQ_RESET] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_NETWORK_TEST] = dnodeProcessStartupReq;

  // msg from client to mnode
  tsTrans.msgFp[TSDB_MSG_TYPE_CONNECT] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_ACCT] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_ACCT] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_ACCT] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_USER] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_USER] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_USER] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_DNODE] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CONFIG_DNODE] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_DNODE] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_DB] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_DB] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_USE_DB] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_DB] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_SYNC_DB] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_TOPIC] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_TOPIC] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_TOPIC] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_FUNCTION] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_FUNCTION] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_FUNCTION] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_STABLE] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_STABLE] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_STABLE] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_KILL_QUERY] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_KILL_CONN] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_HEARTBEAT] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_SHOW] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_SHOW_RETRIEVE] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_SHOW_RETRIEVE_FUNC] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_COMPACT_VNODE] = mnodeProcessMsg;

  // message from mnode to dnode
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_STABLE_IN] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_STABLE_IN_RSP] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_STABLE_IN] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_STABLE_IN_RSP] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_STABLE_IN] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_STABLE_IN_RSP] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_VNODE_IN] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_VNODE_IN_RSP] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_VNODE_IN] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_VNODE_IN_RSP] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_VNODE_IN] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_VNODE_IN_RSP] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_SYNC_VNODE_IN] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_SYNC_VNODE_IN_RSP] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_COMPACT_VNODE_IN] = vnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_COMPACT_VNODE_IN_RSP] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_MNODE_IN] = dnodeProcessCreateMnodeReq;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_MNODE_IN_RSP] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_MNODE_IN] = NULL;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_MNODE_IN_RSP] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CONFIG_DNODE_IN] = NULL;
  tsTrans.msgFp[TSDB_MSG_TYPE_CONFIG_DNODE_IN_RSP] = mnodeProcessMsg;

  // message from dnode to mnode
  tsTrans.msgFp[TSDB_MSG_TYPE_DM_AUTH] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DM_AUTH_RSP] = NULL;
  tsTrans.msgFp[TSDB_MSG_TYPE_DM_GRANT] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DM_GRANT_RSP] = NULL;
  tsTrans.msgFp[TSDB_MSG_TYPE_DM_STATUS] = mnodeProcessMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DM_STATUS_RSP] = dnodeProcessStatusRsp;
}

static void dnodeProcessPeerReq(SRpcMsg *pMsg, SRpcEpSet *pEpSet) {
  SRpcMsg rspMsg = {.handle = pMsg->handle, .pCont = NULL, .contLen = 0};
  int32_t msgType = pMsg->msgType;

  if (msgType == TSDB_MSG_TYPE_NETWORK_TEST) {
    dnodeProcessStartupReq(pMsg);
    return;
  }

  if (dnodeGetRunStat() != DN_RUN_STAT_RUNNING) {
    rspMsg.code = TSDB_CODE_APP_NOT_READY;
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
    dTrace("RPC %p, peer req:%s is ignored since dnode not running", pMsg->handle, taosMsg[msgType]);
    return;
  }

  if (pMsg->pCont == NULL) {
    rspMsg.code = TSDB_CODE_DND_INVALID_MSG_LEN;
    rpcSendResponse(&rspMsg);
    return;
  }

  MsgFp fp = tsTrans.msgFp[msgType];
  if (fp != NULL) {
    dTrace("RPC %p, peer req:%s will be processed", pMsg->handle, taosMsg[msgType]);
    (*fp)(pMsg);
  } else {
    dError("RPC %p, peer req:%s not processed", pMsg->handle, taosMsg[msgType]);
    rspMsg.code = TSDB_CODE_DND_MSG_NOT_PROCESSED;
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
  }
}

static int32_t dnodeInitServer() {
  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort = tsDnodeDnodePort;
  rpcInit.label = "DND-S";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = dnodeProcessPeerReq;
  rpcInit.sessions = TSDB_MAX_VNODES << 4;
  rpcInit.connType = TAOS_CONN_SERVER;
  rpcInit.idleTime = tsShellActivityTimer * 1000;

  tsTrans.serverRpc = rpcOpen(&rpcInit);
  if (tsTrans.serverRpc == NULL) {
    dError("failed to init peer rpc server");
    return -1;
  }

  dInfo("dnode peer rpc server is initialized");
  return 0;
}

static void dnodeCleanupServer() {
  if (tsTrans.serverRpc) {
    rpcClose(tsTrans.serverRpc);
    tsTrans.serverRpc = NULL;
    dInfo("dnode peer server is closed");
  }
}

static void dnodeProcessPeerRsp(SRpcMsg *pMsg, SRpcEpSet *pEpSet) {
  int32_t msgType = pMsg->msgType;

  if (dnodeGetRunStat() == DN_RUN_STAT_STOPPED) {
    if (pMsg == NULL || pMsg->pCont == NULL) return;
    dTrace("RPC %p, peer rsp:%s is ignored since dnode is stopping", pMsg->handle, taosMsg[msgType]);
    rpcFreeCont(pMsg->pCont);
    return;
  }

  if (msgType == TSDB_MSG_TYPE_DM_STATUS_RSP && pEpSet) {
    dnodeUpdateMnodeEps(pEpSet);
  }

  MsgFp fp = tsTrans.msgFp[msgType];
  if (fp != NULL) {
    dTrace("RPC %p, peer rsp:%s will be processed", pMsg->handle, taosMsg[msgType]);
    (*fp)(pMsg);
  } else {
    dDebug("RPC %p, peer rsp:%s not processed", pMsg->handle, taosMsg[msgType]);
  }

  rpcFreeCont(pMsg->pCont);
}

static int32_t dnodeInitClient() {
  char     secret[TSDB_KEY_LEN] = "secret";
  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.label = "DND-C";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = dnodeProcessPeerRsp;
  rpcInit.sessions = TSDB_MAX_VNODES << 4;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.user = "t";
  rpcInit.ckey = "key";
  rpcInit.secret = secret;

  tsTrans.clientRpc = rpcOpen(&rpcInit);
  if (tsTrans.clientRpc == NULL) {
    dError("failed to init peer rpc client");
    return -1;
  }

  dInfo("dnode peer rpc client is initialized");
  return 0;
}

static void dnodeCleanupClient() {
  if (tsTrans.clientRpc) {
    rpcClose(tsTrans.clientRpc);
    tsTrans.clientRpc = NULL;
    dInfo("dnode peer rpc client is closed");
  }
}

static void dnodeProcessShellReq(SRpcMsg *pMsg, SRpcEpSet *pEpSet) {
  SRpcMsg rspMsg = {.handle = pMsg->handle, .pCont = NULL, .contLen = 0};
  int32_t msgType = pMsg->msgType;

  if (dnodeGetRunStat() == DN_RUN_STAT_STOPPED) {
    dError("RPC %p, shell req:%s is ignored since dnode exiting", pMsg->handle, taosMsg[msgType]);
    rspMsg.code = TSDB_CODE_DND_EXITING;
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
    return;
  } else if (dnodeGetRunStat() != DN_RUN_STAT_RUNNING) {
    dError("RPC %p, shell req:%s is ignored since dnode not running", pMsg->handle, taosMsg[msgType]);
    rspMsg.code = TSDB_CODE_APP_NOT_READY;
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
    return;
  }

  if (pMsg->pCont == NULL) {
    rspMsg.code = TSDB_CODE_DND_INVALID_MSG_LEN;
    rpcSendResponse(&rspMsg);
    return;
  }

  MsgFp fp = tsTrans.msgFp[msgType];
  if (fp != NULL) {
    dTrace("RPC %p, shell req:%s will be processed", pMsg->handle, taosMsg[msgType]);
    (*fp)(pMsg);
  } else {
    dError("RPC %p, shell req:%s is not processed", pMsg->handle, taosMsg[msgType]);
    rspMsg.code = TSDB_CODE_DND_MSG_NOT_PROCESSED;
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
  }
}

void dnodeSendMsgToDnode(SRpcEpSet *epSet, SRpcMsg *rpcMsg) { rpcSendRequest(tsTrans.clientRpc, epSet, rpcMsg, NULL); }

void dnodeSendMsgToMnode(SRpcMsg *rpcMsg) {
  SRpcEpSet epSet = {0};
  dnodeGetEpSetForPeer(&epSet);
  dnodeSendMsgToDnode(&epSet, rpcMsg);
}

static void dnodeSendMsgToMnodeRecv(SRpcMsg *rpcMsg, SRpcMsg *rpcRsp) {
  SRpcEpSet epSet = {0};
  dnodeGetEpSetForPeer(&epSet);
  rpcSendRecv(tsTrans.clientRpc, &epSet, rpcMsg, rpcRsp);
}

static int32_t dnodeRetrieveUserAuthInfo(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  int32_t code = mnodeRetriveAuth(user, spi, encrypt, secret, ckey);
  if (code != TSDB_CODE_APP_NOT_READY) return code;

  SAuthMsg *pMsg = rpcMallocCont(sizeof(SAuthMsg));
  tstrncpy(pMsg->user, user, sizeof(pMsg->user));

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pMsg;
  rpcMsg.contLen = sizeof(SAuthMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_DM_AUTH;

  dDebug("user:%s, send auth msg to mnodes", user);
  SRpcMsg rpcRsp = {0};
  dnodeSendMsgToMnodeRecv(&rpcMsg, &rpcRsp);

  if (rpcRsp.code != 0) {
    dError("user:%s, auth msg received from mnodes, error:%s", user, tstrerror(rpcRsp.code));
  } else {
    SAuthRsp *pRsp = rpcRsp.pCont;
    dDebug("user:%s, auth msg received from mnodes", user);
    memcpy(secret, pRsp->secret, TSDB_KEY_LEN);
    memcpy(ckey, pRsp->ckey, TSDB_KEY_LEN);
    *spi = pRsp->spi;
    *encrypt = pRsp->encrypt;
  }

  rpcFreeCont(rpcRsp.pCont);
  return rpcRsp.code;
}

static int32_t dnodeInitShell() {
  int32_t numOfThreads = (int32_t)((tsNumOfCores * tsNumOfThreadsPerCore) / 2.0);
  if (numOfThreads < 1) {
    numOfThreads = 1;
  }

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort = tsDnodeShellPort;
  rpcInit.label = "SHELL";
  rpcInit.numOfThreads = numOfThreads;
  rpcInit.cfp = dnodeProcessShellReq;
  rpcInit.sessions = tsMaxShellConns;
  rpcInit.connType = TAOS_CONN_SERVER;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.afp = dnodeRetrieveUserAuthInfo;

  tsTrans.shellRpc = rpcOpen(&rpcInit);
  if (tsTrans.shellRpc == NULL) {
    dError("failed to init shell rpc server");
    return -1;
  }

  dInfo("dnode shell rpc server is initialized");
  return 0;
}

static void dnodeCleanupShell() {
  if (tsTrans.shellRpc) {
    rpcClose(tsTrans.shellRpc);
    tsTrans.shellRpc = NULL;
  }
}

int32_t dnodeInitTrans() {
  if (dnodeInitClient() != 0) {
    return -1;
  }

  if (dnodeInitServer() != 0) {
    return -1;
  }

  if (dnodeInitShell() != 0) {
    return -1;
  }

  return 0;
}

void dnodeCleanupTrans() {
  dnodeCleanupShell();
  dnodeCleanupServer();
  dnodeCleanupClient();
}
