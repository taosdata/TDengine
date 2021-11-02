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
#include "dnodeDnode.h"
#include "dnodeMnode.h"
#include "dnodeVnodes.h"

static struct {
  void *peerRpc;
  void *shellRpc;
  void *clientRpc;
  MsgFp msgFp[TSDB_MSG_TYPE_MAX];
} tsTrans;

static void dnodeInitMsgFp() {
  // msg from client to dnode
  tsTrans.msgFp[TSDB_MSG_TYPE_SUBMIT] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_QUERY] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_FETCH] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_TABLE] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_TABLE] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_TABLE] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_UPDATE_TAG_VAL] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_TABLE_META] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_TABLES_META] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_STABLE_VGROUP] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_MQ_QUERY] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_MQ_CONSUME] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_MQ_CONNECT] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_MQ_DISCONNECT] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_MQ_ACK] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_MQ_RESET] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_NETWORK_TEST] = dnodeProcessDnodeMsg;

  // msg from client to mnode
  tsTrans.msgFp[TSDB_MSG_TYPE_CONNECT] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_ACCT] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_ACCT] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_ACCT] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_USER] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_USER] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_USER] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_DNODE] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CONFIG_DNODE] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_DNODE] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_DB] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_DB] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_USE_DB] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_DB] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_SYNC_DB] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_TOPIC] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_TOPIC] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_TOPIC] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_FUNCTION] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_FUNCTION] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_FUNCTION] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_STABLE] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_STABLE] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_STABLE] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_KILL_QUERY] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_KILL_CONN] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_HEARTBEAT] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_SHOW] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_SHOW_RETRIEVE] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_SHOW_RETRIEVE_FUNC] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_COMPACT_VNODE] = dnodeProcessMnodeMsg;

  // message from mnode to dnode
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_STABLE_IN] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_STABLE_IN_RSP] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_STABLE_IN] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_STABLE_IN_RSP] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_STABLE_IN] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_STABLE_IN_RSP] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_VNODE_IN] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_VNODE_IN_RSP] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_VNODE_IN] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_ALTER_VNODE_IN_RSP] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_VNODE_IN] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_VNODE_IN_RSP] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_SYNC_VNODE_IN] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_SYNC_VNODE_IN_RSP] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_COMPACT_VNODE_IN] = dnodeProcessVnodesMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_COMPACT_VNODE_IN_RSP] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_MNODE_IN] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CREATE_MNODE_IN_RSP] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_MNODE_IN] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_DROP_MNODE_IN_RSP] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CONFIG_DNODE_IN] = dnodeProcessDnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_CONFIG_DNODE_IN_RSP] = dnodeProcessMnodeMsg;

  // message from dnode to mnode
  tsTrans.msgFp[TSDB_MSG_TYPE_AUTH] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_AUTH_RSP] = dnodeProcessDnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_GRANT] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_GRANT_RSP] = dnodeProcessDnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_STATUS] = dnodeProcessMnodeMsg;
  tsTrans.msgFp[TSDB_MSG_TYPE_STATUS_RSP] = dnodeProcessDnodeMsg;
}

static void dnodeProcessPeerReq(SRpcMsg *pMsg, SEpSet *pEpSet) {
  SRpcMsg rspMsg = {.handle = pMsg->handle};
  int32_t msgType = pMsg->msgType;

  if (msgType == TSDB_MSG_TYPE_NETWORK_TEST) {
    dnodeProcessDnodeMsg(pMsg, pEpSet);
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
    (*fp)(pMsg, pEpSet);
  } else {
    dError("RPC %p, peer req:%s not processed", pMsg->handle, taosMsg[msgType]);
    rspMsg.code = TSDB_CODE_DND_MSG_NOT_PROCESSED;
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
  }
}

static int32_t dnodeInitPeerServer() {
  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort = tsDnodeDnodePort;
  rpcInit.label = "DND-S";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = dnodeProcessPeerReq;
  rpcInit.sessions = TSDB_MAX_VNODES << 4;
  rpcInit.connType = TAOS_CONN_SERVER;
  rpcInit.idleTime = tsShellActivityTimer * 1000;

  tsTrans.peerRpc = rpcOpen(&rpcInit);
  if (tsTrans.peerRpc == NULL) {
    dError("failed to init peer rpc server");
    return -1;
  }

  dInfo("dnode peer rpc server is initialized");
  return 0;
}

static void dnodeCleanupPeerServer() {
  if (tsTrans.peerRpc) {
    rpcClose(tsTrans.peerRpc);
    tsTrans.peerRpc = NULL;
    dInfo("dnode peer server is closed");
  }
}

static void dnodeProcessPeerRsp(SRpcMsg *pMsg, SEpSet *pEpSet) {
  int32_t msgType = pMsg->msgType;

  if (dnodeGetRunStat() == DN_RUN_STAT_STOPPED) {
    if (pMsg == NULL || pMsg->pCont == NULL) return;
    dTrace("RPC %p, peer rsp:%s is ignored since dnode is stopping", pMsg->handle, taosMsg[msgType]);
    rpcFreeCont(pMsg->pCont);
    return;
  }

  MsgFp fp = tsTrans.msgFp[msgType];
  if (fp != NULL) {
    dTrace("RPC %p, peer rsp:%s will be processed, code:%s", pMsg->handle, taosMsg[msgType], tstrerror(pMsg->code));
    (*fp)(pMsg, pEpSet);
  } else {
    dDebug("RPC %p, peer rsp:%s not processed", pMsg->handle, taosMsg[msgType]);
  }

  rpcFreeCont(pMsg->pCont);
}

static int32_t dnodeInitClient() {
  char secret[TSDB_KEY_LEN] = "secret";

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

static void dnodeProcessShellReq(SRpcMsg *pMsg, SEpSet *pEpSet) {
  SRpcMsg rspMsg = {.handle = pMsg->handle};
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
    (*fp)(pMsg, pEpSet);
  } else {
    dError("RPC %p, shell req:%s is not processed", pMsg->handle, taosMsg[msgType]);
    rspMsg.code = TSDB_CODE_DND_MSG_NOT_PROCESSED;
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
  }
}

static void dnodeSendMsgToMnodeRecv(SRpcMsg *rpcMsg, SRpcMsg *rpcRsp) {
  SEpSet epSet = {0};
  dnodeGetMnodeEpSetForPeer(&epSet);
  rpcSendRecv(tsTrans.clientRpc, &epSet, rpcMsg, rpcRsp);
}

static int32_t dnodeRetrieveUserAuthInfo(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  int32_t code = dnodeGetUserAuthFromMnode(user, spi, encrypt, secret, ckey);
  if (code != TSDB_CODE_APP_NOT_READY) return code;

  SAuthMsg *pMsg = rpcMallocCont(sizeof(SAuthMsg));
  tstrncpy(pMsg->user, user, sizeof(pMsg->user));

  dDebug("user:%s, send auth msg to mnodes", user);
  SRpcMsg rpcMsg = {.pCont = pMsg, .contLen = sizeof(SAuthMsg), .msgType = TSDB_MSG_TYPE_AUTH};
  SRpcMsg rpcRsp = {0};
  dnodeSendMsgToMnodeRecv(&rpcMsg, &rpcRsp);

  if (rpcRsp.code != 0) {
    dError("user:%s, auth msg received from mnodes, error:%s", user, tstrerror(rpcRsp.code));
  } else {
    dDebug("user:%s, auth msg received from mnodes", user);
    SAuthRsp *pRsp = rpcRsp.pCont;
    memcpy(secret, pRsp->secret, TSDB_KEY_LEN);
    memcpy(ckey, pRsp->ckey, TSDB_KEY_LEN);
    *spi = pRsp->spi;
    *encrypt = pRsp->encrypt;
  }

  rpcFreeCont(rpcRsp.pCont);
  return rpcRsp.code;
}

static int32_t dnodeInitShellServer() {
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

static void dnodeCleanupShellServer() {
  if (tsTrans.shellRpc) {
    rpcClose(tsTrans.shellRpc);
    tsTrans.shellRpc = NULL;
  }
}

int32_t dnodeInitTrans() {
  if (dnodeInitClient() != 0) {
    return -1;
  }

  if (dnodeInitPeerServer() != 0) {
    return -1;
  }

  if (dnodeInitShellServer() != 0) {
    return -1;
  }

  return 0;
}

void dnodeCleanupTrans() {
  dnodeCleanupShellServer();
  dnodeCleanupPeerServer();
  dnodeCleanupClient();
}

void dnodeSendMsgToDnode(SEpSet *epSet, SRpcMsg *rpcMsg) { rpcSendRequest(tsTrans.clientRpc, epSet, rpcMsg, NULL); }

void dnodeSendMsgToMnode(SRpcMsg *rpcMsg) {
  SEpSet epSet = {0};
  dnodeGetMnodeEpSetForPeer(&epSet);
  dnodeSendMsgToDnode(&epSet, rpcMsg);
}