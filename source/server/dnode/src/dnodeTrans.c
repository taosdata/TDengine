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
#include "dnodeTrans.h"
#include "dnodeEps.h"
#include "dnodeMsg.h"
#include "mnode.h"
#include "vnode.h"

typedef void (*RpcMsgFp)(SRpcMsg *pMsg);

static struct {
  void    *serverRpc;
  void    *clientRpc;
  void    *shellRpc;
  int32_t  queryReqNum;
  int32_t  submitReqNum;
  RpcMsgFp peerMsgFp[TSDB_MSG_TYPE_MAX];
  RpcMsgFp shellMsgFp[TSDB_MSG_TYPE_MAX];
} tsTrans;

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

  RpcMsgFp fp = tsTrans.peerMsgFp[msgType];
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
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_CREATE_TABLE] = vnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_DROP_TABLE] = vnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_ALTER_TABLE] = vnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_DROP_STABLE] = vnodeProcessMsg;

  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE] = vnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_ALTER_VNODE] = vnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_SYNC_VNODE] = vnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_COMPACT_VNODE] = vnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_DROP_VNODE] = vnodeProcessMsg;

  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_ALTER_STREAM] = vnodeProcessMsg;

  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_CONFIG_DNODE] = dnodeProcessConfigDnodeReq;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_CREATE_MNODE] = dnodeProcessCreateMnodeReq;

  tsTrans.peerMsgFp[TSDB_MSG_TYPE_DM_CONFIG_TABLE] = mnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_DM_CONFIG_VNODE] = mnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_DM_AUTH] = mnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_DM_GRANT] = mnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_DM_STATUS] = mnodeProcessMsg;

  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MQ_CONNECT] = vnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MQ_CONSUME] = vnodeProcessMsg;

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

  RpcMsgFp fp = tsTrans.peerMsgFp[msgType];
  if (fp != NULL) {
    (*fp)(pMsg);
  } else {
    dDebug("RPC %p, peer rsp:%s not processed", pMsg->handle, taosMsg[msgType]);
  }

  rpcFreeCont(pMsg->pCont);
}

static int32_t dnodeInitClient() {
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_CREATE_TABLE_RSP] = mnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_DROP_TABLE_RSP] = mnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_ALTER_TABLE_RSP] = mnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_DROP_STABLE_RSP] = mnodeProcessMsg;

  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE_RSP] = mnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_ALTER_VNODE_RSP] = mnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_SYNC_VNODE_RSP] = mnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_COMPACT_VNODE_RSP] = mnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_DROP_VNODE_RSP] = mnodeProcessMsg;

  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_ALTER_STREAM_RSP] = mnodeProcessMsg;

  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_CONFIG_DNODE_RSP] = mnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_MD_CREATE_MNODE_RSP] = mnodeProcessMsg;

  tsTrans.peerMsgFp[TSDB_MSG_TYPE_DM_CONFIG_TABLE_RSP] = mnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_DM_CONFIG_VNODE_RSP] = mnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_DM_AUTH_RSP] = mnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_DM_GRANT_RSP] = mnodeProcessMsg;
  tsTrans.peerMsgFp[TSDB_MSG_TYPE_DM_STATUS_RSP] = dnodeProcessStatusRsp;

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

  if (msgType == TSDB_MSG_TYPE_QUERY) {
    atomic_fetch_add_32(&tsTrans.queryReqNum, 1);
  } else if (msgType == TSDB_MSG_TYPE_SUBMIT) {
    atomic_fetch_add_32(&tsTrans.submitReqNum, 1);
  } else {
  }

  RpcMsgFp fp = tsTrans.shellMsgFp[msgType];
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
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_SUBMIT] = vnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_QUERY] = vnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_FETCH] = vnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_UPDATE_TAG_VAL] = vnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_MQ_QUERY] = vnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_MQ_CONSUME] = vnodeProcessMsg;

  // the following message shall be treated as mnode write
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_CREATE_ACCT] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_ALTER_ACCT] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_DROP_ACCT] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_CREATE_USER] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_ALTER_USER] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_DROP_USER] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_CREATE_DNODE] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_DROP_DNODE] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_CREATE_DB] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_CREATE_TP] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_CREATE_FUNCTION] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_DROP_DB] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_SYNC_DB] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_DROP_TP] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_DROP_FUNCTION] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_ALTER_DB] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_ALTER_TP] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_CREATE_TABLE] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_DROP_TABLE] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_ALTER_TABLE] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_ALTER_STREAM] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_KILL_QUERY] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_KILL_STREAM] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_KILL_CONN] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_CONFIG_DNODE] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_COMPACT_VNODE] = mnodeProcessMsg;

  // the following message shall be treated as mnode query
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_HEARTBEAT] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_CONNECT] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_USE_DB] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_TABLE_META] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_STABLE_VGROUP] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_TABLES_META] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_SHOW] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_RETRIEVE] = mnodeProcessMsg;
  tsTrans.shellMsgFp[TSDB_MSG_TYPE_CM_RETRIEVE_FUNC] = mnodeProcessMsg;

  tsTrans.shellMsgFp[TSDB_MSG_TYPE_NETWORK_TEST] = dnodeProcessStartupReq;

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
