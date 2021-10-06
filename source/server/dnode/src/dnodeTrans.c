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
#include "os.h"
#include "tglobal.h"
#include "dnodeMain.h"
#include "dnodeMnodeEps.h"
#include "dnodeStatus.h"
#include "dnodeTrans.h"
#include "vnode.h"
#include "mnode.h"

static void dnodeProcessPeerReq(SRpcMsg *pMsg, SRpcEpSet *pEpSet) {
  Dnode * dnode = dnodeInst();
  SRpcMsg rspMsg = {.handle = pMsg->handle, .pCont = NULL, .contLen = 0};

  if (pMsg->pCont == NULL) return;
  if (pMsg->msgType == TSDB_MSG_TYPE_NETWORK_TEST) {
    dnodeProcessStartupReq(pMsg);
    return;
  }

  if (dnode->main->runStatus != TD_RUN_STAT_RUNNING) {
    rspMsg.code = TSDB_CODE_APP_NOT_READY;
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
    dTrace("RPC %p, msg:%s is ignored since dnode not running", pMsg->handle, taosMsg[pMsg->msgType]);
    return;
  }

  if (pMsg->pCont == NULL) {
    rspMsg.code = TSDB_CODE_DND_INVALID_MSG_LEN;
    rpcSendResponse(&rspMsg);
    return;
  }

  RpcMsgFp fp = dnode->trans->peerMsgFp[pMsg->msgType];
  if (fp != NULL) {
    (*fp)(pMsg);
  } else {
    dDebug("RPC %p, peer req:%s not processed", pMsg->handle, taosMsg[pMsg->msgType]);
    rspMsg.code = TSDB_CODE_DND_MSG_NOT_PROCESSED;
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
  }
}

int32_t dnodeInitServer(DnTrans *trans) {
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_CREATE_TABLE]  = vnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_DROP_TABLE]    = vnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_ALTER_TABLE]   = vnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_DROP_STABLE]   = vnodeProcessMsg;

  trans->peerMsgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE]  = vnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_ALTER_VNODE]   = vnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_SYNC_VNODE]    = vnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_COMPACT_VNODE] = vnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_DROP_VNODE]    = vnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_ALTER_STREAM]  = vnodeProcessMsg;

  trans->peerMsgFp[TSDB_MSG_TYPE_MD_CONFIG_DNODE]  = dnodeProcessConfigDnodeReq;
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_CREATE_MNODE]  = dnodeProcessCreateMnodeReq;

  trans->peerMsgFp[TSDB_MSG_TYPE_DM_CONFIG_TABLE]  = mnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_DM_CONFIG_VNODE]  = mnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_DM_AUTH]          = mnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_DM_GRANT]         = mnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_DM_STATUS]        = mnodeProcessMsg;

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort = tsDnodeDnodePort;
  rpcInit.label = "DND-S";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = dnodeProcessPeerReq;
  rpcInit.sessions = TSDB_MAX_VNODES << 4;
  rpcInit.connType = TAOS_CONN_SERVER;
  rpcInit.idleTime = tsShellActivityTimer * 1000;

  trans->serverRpc = rpcOpen(&rpcInit);
  if (trans->serverRpc == NULL) {
    dError("failed to init peer rpc server");
    return -1;
  }

  dInfo("dnode peer rpc server is initialized");
  return 0;
}

void dnodeCleanupServer(DnTrans *trans) {
  if (trans->serverRpc) {
    rpcClose(trans->serverRpc);
    trans->serverRpc = NULL;
    dInfo("dnode peer server is closed");
  }
}

static void dnodeProcessRspFromPeer(SRpcMsg *pMsg, SRpcEpSet *pEpSet) {
  Dnode *dnode = dnodeInst();
  if (dnode->main->runStatus == TD_RUN_STAT_STOPPED) {
    if (pMsg == NULL || pMsg->pCont == NULL) return;
    dTrace("msg:%p is ignored since dnode is stopping", pMsg);
    rpcFreeCont(pMsg->pCont);
    return;
  }

  if (pMsg->msgType == TSDB_MSG_TYPE_DM_STATUS_RSP && pEpSet) {
    dnodeUpdateMnodeFromPeer(dnode->meps, pEpSet);
  }

  RpcMsgFp fp = dnode->trans->peerMsgFp[pMsg->msgType];
  if (fp != NULL) {
    (*fp)(pMsg);
  } else {
    dDebug("RPC %p, peer rsp:%s not processed", pMsg->handle, taosMsg[pMsg->msgType]);
    SRpcMsg rspMsg = {.handle = pMsg->handle, .pCont = NULL, .contLen = 0};
    rspMsg.code = TSDB_CODE_DND_MSG_NOT_PROCESSED;
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
  }

  rpcFreeCont(pMsg->pCont);
}

int32_t dnodeInitClient(DnTrans *trans) {
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_CREATE_TABLE_RSP]  = mnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_DROP_TABLE_RSP]    = mnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_ALTER_TABLE_RSP]   = mnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_DROP_STABLE_RSP]   = mnodeProcessMsg;

  trans->peerMsgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE_RSP]  = mnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_ALTER_VNODE_RSP]   = mnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_SYNC_VNODE_RSP]    = mnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_COMPACT_VNODE_RSP] = mnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_DROP_VNODE_RSP]    = mnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_ALTER_STREAM_RSP]  = mnodeProcessMsg;

  trans->peerMsgFp[TSDB_MSG_TYPE_MD_CONFIG_DNODE_RSP]  = mnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_MD_CREATE_MNODE_RSP]  = mnodeProcessMsg;

  trans->peerMsgFp[TSDB_MSG_TYPE_DM_CONFIG_TABLE_RSP]  = mnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_DM_CONFIG_VNODE_RSP]  = mnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_DM_AUTH_RSP]          = mnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_DM_GRANT_RSP]         = mnodeProcessMsg;
  trans->peerMsgFp[TSDB_MSG_TYPE_DM_STATUS_RSP]        = dnodeProcessStatusRsp;

  char secret[TSDB_KEY_LEN] = "secret";
  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.label        = "DND-C";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = dnodeProcessRspFromPeer;
  rpcInit.sessions     = TSDB_MAX_VNODES << 4;
  rpcInit.connType     = TAOS_CONN_CLIENT;
  rpcInit.idleTime     = tsShellActivityTimer * 1000;
  rpcInit.user         = "t";
  rpcInit.ckey         = "key";
  rpcInit.secret       = secret;

  trans->clientRpc = rpcOpen(&rpcInit);
  if (trans->clientRpc == NULL) {
    dError("failed to init peer rpc client");
    return -1;
  }

  dInfo("dnode peer rpc client is initialized");
  return 0;
}

void dnodeCleanupClient(DnTrans *trans) {
  if (trans->clientRpc) {
    rpcClose(trans->clientRpc);
    trans->clientRpc = NULL;
    dInfo("dnode peer rpc client is closed");
  }
}

static void dnodeProcessMsgFromShell(SRpcMsg *pMsg, SRpcEpSet *pEpSet) {
  Dnode * dnode = dnodeInst();
  SRpcMsg rpcMsg = {.handle = pMsg->handle, .pCont = NULL, .contLen = 0};

  if (pMsg->pCont == NULL) return;
  if (dnode->main->runStatus == TD_RUN_STAT_STOPPED) {
    dError("RPC %p, shell msg:%s is ignored since dnode exiting", pMsg->handle, taosMsg[pMsg->msgType]);
    rpcMsg.code = TSDB_CODE_DND_EXITING;
    rpcSendResponse(&rpcMsg);
    rpcFreeCont(pMsg->pCont);
    return;
  } else if (dnode->main->runStatus != TD_RUN_STAT_RUNNING) {
    dError("RPC %p, shell msg:%s is ignored since dnode not running", pMsg->handle, taosMsg[pMsg->msgType]);
    rpcMsg.code = TSDB_CODE_APP_NOT_READY;
    rpcSendResponse(&rpcMsg);
    rpcFreeCont(pMsg->pCont);
    return;
  }

  DnTrans *trans = dnode->trans;
  if (pMsg->msgType == TSDB_MSG_TYPE_QUERY) {
    atomic_fetch_add_32(&trans->queryReqNum, 1);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_SUBMIT) {
    atomic_fetch_add_32(&trans->submitReqNum, 1);
  } else {}

  RpcMsgFp fp = trans->shellMsgFp[pMsg->msgType];
  if (fp != NULL) {
    (*fp)(pMsg);
  } else {
    dError("RPC %p, shell req:%s is not processed", pMsg->handle, taosMsg[pMsg->msgType]);
    rpcMsg.code = TSDB_CODE_DND_MSG_NOT_PROCESSED;
    rpcSendResponse(&rpcMsg);
    rpcFreeCont(pMsg->pCont);
  }
}

static int32_t dnodeAuthNetTest(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  if (strcmp(user, "nettestinternal") == 0) {
    char pass[32] = {0};
    taosEncryptPass((uint8_t *)user, strlen(user), pass);
    *spi = 0;
    *encrypt = 0;
    *ckey = 0;
    memcpy(secret, pass, TSDB_KEY_LEN);
    dTrace("nettest user is authorized");
    return 0;
  }

  return -1;
}

void dnodeSendMsgToDnode(SRpcEpSet *epSet, SRpcMsg *rpcMsg) {
  Dnode *dnode = dnodeInst();
  rpcSendRequest(dnode->trans->clientRpc, epSet, rpcMsg, NULL);
}

void dnodeSendMsgToMnode(SRpcMsg *rpcMsg) {
  Dnode *   dnode = dnodeInst();
  SRpcEpSet epSet = {0};
  dnodeGetEpSetForPeer(dnode->meps, &epSet);
  dnodeSendMsgToDnode(&epSet, rpcMsg);
}

void dnodeSendMsgToMnodeRecv(SRpcMsg *rpcMsg, SRpcMsg *rpcRsp) {
  Dnode *   dnode = dnodeInst();
  SRpcEpSet epSet = {0};
  dnodeGetEpSetForPeer(dnode->meps, &epSet);
  rpcSendRecv(dnode->trans->clientRpc, &epSet, rpcMsg, rpcRsp);
}

void dnodeSendMsgToDnodeRecv(SRpcMsg *rpcMsg, SRpcMsg *rpcRsp, SRpcEpSet *epSet) {
  Dnode *dnode = dnodeInst();
  rpcSendRecv(dnode->trans->clientRpc, epSet, rpcMsg, rpcRsp);
}

static int32_t dnodeRetrieveUserAuthInfo(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  if (dnodeAuthNetTest(user, spi, encrypt, secret, ckey) == 0) return 0;

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

int32_t dnodeInitShell(DnTrans *trans) {
  trans->shellMsgFp[TSDB_MSG_TYPE_SUBMIT] = vnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_QUERY]  = vnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_FETCH]  = vnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_UPDATE_TAG_VAL] = vnodeProcessMsg;

  // the following message shall be treated as mnode write
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_CREATE_ACCT]     = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_ALTER_ACCT]      = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_DROP_ACCT]       = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_CREATE_USER]     = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_ALTER_USER]      = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_DROP_USER]       = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_CREATE_DNODE]    = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_DROP_DNODE]      = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_CREATE_DB]       = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_CREATE_TP]       = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_CREATE_FUNCTION] = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_DROP_DB]         = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_SYNC_DB]         = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_DROP_TP]         = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_DROP_FUNCTION]   = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_ALTER_DB]        = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_ALTER_TP]        = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_CREATE_TABLE]    = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_DROP_TABLE]      = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_ALTER_TABLE]     = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_ALTER_STREAM]    = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_KILL_QUERY]      = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_KILL_STREAM]     = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_KILL_CONN]       = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_CONFIG_DNODE]    = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_COMPACT_VNODE]   = mnodeProcessMsg;

  // the following message shall be treated as mnode query
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_HEARTBEAT]     = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_CONNECT]       = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_USE_DB]        = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_TABLE_META]    = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_STABLE_VGROUP] = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_TABLES_META]   = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_SHOW]          = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_RETRIEVE]      = mnodeProcessMsg;
  trans->shellMsgFp[TSDB_MSG_TYPE_CM_RETRIEVE_FUNC] = mnodeProcessMsg;

  trans->shellMsgFp[TSDB_MSG_TYPE_NETWORK_TEST]     = dnodeProcessStartupReq;

  int32_t numOfThreads = (int32_t)((tsNumOfCores * tsNumOfThreadsPerCore) / 2.0);
  if (numOfThreads < 1) {
    numOfThreads = 1;
  }

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort    = tsDnodeShellPort;
  rpcInit.label        = "SHELL";
  rpcInit.numOfThreads = numOfThreads;
  rpcInit.cfp          = dnodeProcessMsgFromShell;
  rpcInit.sessions     = tsMaxShellConns;
  rpcInit.connType     = TAOS_CONN_SERVER;
  rpcInit.idleTime     = tsShellActivityTimer * 1000;
  rpcInit.afp          = dnodeRetrieveUserAuthInfo;

  trans->shellRpc = rpcOpen(&rpcInit);
  if (trans->shellRpc == NULL) {
    dError("failed to init shell rpc server");
    return -1;
  }

  dInfo("dnode shell rpc server is initialized");
  return 0;
}

void dnodeCleanupShell(DnTrans *trans) {
  if (trans->shellRpc) {
    rpcClose(trans->shellRpc);
    trans->shellRpc = NULL;
  }
}

int32_t dnodeInitTrans(DnTrans **out) {
  DnTrans *trans = calloc(1, sizeof(DnTrans));
  if (trans == NULL) return -1;

  *out = trans;

  if (dnodeInitClient(trans) != 0) {
    return -1;
  }

  if (dnodeInitServer(trans) != 0) {
    return -1;
  }

  if (dnodeInitShell(trans) != 0) {
    return -1;
  }

  return 0;
}

void dnodeCleanupTrans(DnTrans **out) {
  DnTrans* trans = *out;
  *out = NULL;

  dnodeCleanupShell(trans);
  dnodeCleanupServer(trans);
  dnodeCleanupClient(trans);

  free(trans);
}
