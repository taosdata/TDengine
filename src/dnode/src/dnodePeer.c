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
#include "taosmsg.h"
#include "tglobal.h"
#include "mnode.h"
#include "dnode.h"
#include "dnodeInt.h"
#include "dnodeMgmt.h"
#include "dnodeVWrite.h"
#include "dnodeMPeer.h"
#include "dnodeMInfos.h"

static void (*dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *);
static void dnodeProcessReqMsgFromDnode(SRpcMsg *pMsg, SRpcEpSet *);
static void (*dnodeProcessRspMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *rpcMsg);
static void dnodeProcessRspFromDnode(SRpcMsg *pMsg, SRpcEpSet *pEpSet);
static void *tsServerRpc = NULL;
static void *tsClientRpc = NULL;

int32_t dnodeInitServer() {
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MD_CREATE_TABLE] = dnodeDispatchToVWriteQueue;
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MD_DROP_TABLE]   = dnodeDispatchToVWriteQueue; 
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MD_ALTER_TABLE]  = dnodeDispatchToVWriteQueue;
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MD_DROP_STABLE]  = dnodeDispatchToVWriteQueue;

  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE] = dnodeDispatchToMgmtQueue; 
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MD_ALTER_VNODE]  = dnodeDispatchToMgmtQueue; 
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MD_DROP_VNODE]   = dnodeDispatchToMgmtQueue;
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MD_ALTER_STREAM] = dnodeDispatchToMgmtQueue;
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MD_CONFIG_DNODE] = dnodeDispatchToMgmtQueue;
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MD_CREATE_MNODE] = dnodeDispatchToMgmtQueue;

  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_DM_CONFIG_TABLE] = dnodeDispatchToMPeerQueue;
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_DM_CONFIG_VNODE] = dnodeDispatchToMPeerQueue;
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_DM_AUTH]         = dnodeDispatchToMPeerQueue;
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_DM_GRANT]        = dnodeDispatchToMPeerQueue;
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_DM_STATUS]       = dnodeDispatchToMPeerQueue;
  
  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort    = tsDnodeDnodePort;
  rpcInit.label        = "DND-S";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = dnodeProcessReqMsgFromDnode;
  rpcInit.sessions     = TSDB_MAX_VNODES;
  rpcInit.connType     = TAOS_CONN_SERVER;
  rpcInit.idleTime     = tsShellActivityTimer * 1000;

  tsServerRpc = rpcOpen(&rpcInit);
  if (tsServerRpc == NULL) {
    dError("failed to init inter-dnodes RPC server");
    return -1;
  }

  dInfo("dnode inter-dnodes RPC server is initialized");
  return 0;
}

void dnodeCleanupServer() {
  if (tsServerRpc) {
    rpcClose(tsServerRpc);
    tsServerRpc = NULL;
    dInfo("inter-dnodes RPC server is closed");
  }
}

static void dnodeProcessReqMsgFromDnode(SRpcMsg *pMsg, SRpcEpSet *pEpSet) {
  SRpcMsg rspMsg = {
    .handle  = pMsg->handle,
    .pCont   = NULL,
    .contLen = 0
  };
  
  if (pMsg->pCont == NULL) return;

  if (dnodeGetRunStatus() != TSDB_RUN_STATUS_RUNING) {
    rspMsg.code = TSDB_CODE_APP_NOT_READY;
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
    dDebug("RPC %p, msg:%s is ignored since dnode not running", pMsg->handle, taosMsg[pMsg->msgType]);
    return;
  }

  if (pMsg->pCont == NULL) {
    rspMsg.code = TSDB_CODE_DND_INVALID_MSG_LEN;
    rpcSendResponse(&rspMsg);
    return;
  }

  if (dnodeProcessReqMsgFp[pMsg->msgType]) {
    (*dnodeProcessReqMsgFp[pMsg->msgType])(pMsg);
  } else {
    dDebug("RPC %p, message:%s not processed", pMsg->handle, taosMsg[pMsg->msgType]);
    rspMsg.code = TSDB_CODE_DND_MSG_NOT_PROCESSED;
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
  }
}

int32_t dnodeInitClient() {
  char secret[TSDB_KEY_LEN] = "secret";
  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.label        = "DND-C";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = dnodeProcessRspFromDnode;
  rpcInit.sessions     = TSDB_MAX_VNODES;
  rpcInit.connType     = TAOS_CONN_CLIENT;
  rpcInit.idleTime     = tsShellActivityTimer * 1000;
  rpcInit.user         = "t";
  rpcInit.ckey         = "key";
  rpcInit.secret       = secret;

  tsClientRpc = rpcOpen(&rpcInit);
  if (tsClientRpc == NULL) {
    dError("failed to init mnode rpc client");
    return -1;
  }

  dInfo("dnode inter-dnodes rpc client is initialized");
  return 0;
}

void dnodeCleanupClient() {
  if (tsClientRpc) {
    rpcClose(tsClientRpc);
    tsClientRpc = NULL;
    dInfo("dnode inter-dnodes rpc client is closed");
  }
}

static void dnodeProcessRspFromDnode(SRpcMsg *pMsg, SRpcEpSet *pEpSet) {
  if (pMsg->msgType == TSDB_MSG_TYPE_DM_STATUS_RSP && pEpSet) {
    dnodeUpdateEpSetForPeer(pEpSet);
  }

  if (dnodeProcessRspMsgFp[pMsg->msgType]) {    
    (*dnodeProcessRspMsgFp[pMsg->msgType])(pMsg);
  } else {
    mnodeProcessPeerRsp(pMsg);
  }

  rpcFreeCont(pMsg->pCont);
}

void dnodeAddClientRspHandle(uint8_t msgType, void (*fp)(SRpcMsg *rpcMsg)) {
  dnodeProcessRspMsgFp[msgType] = fp;
}

void dnodeSendMsgToDnode(SRpcEpSet *epSet, SRpcMsg *rpcMsg) {
  rpcSendRequest(tsClientRpc, epSet, rpcMsg, NULL);
}

void dnodeSendMsgToMnodeRecv(SRpcMsg *rpcMsg, SRpcMsg *rpcRsp) {
  SRpcEpSet epSet = {0};
  dnodeGetEpSetForPeer(&epSet);
  rpcSendRecv(tsClientRpc, &epSet, rpcMsg, rpcRsp);
}

void dnodeSendMsgToDnodeRecv(SRpcMsg *rpcMsg, SRpcMsg *rpcRsp, SRpcEpSet *epSet) {
  rpcSendRecv(tsClientRpc, epSet, rpcMsg, rpcRsp);
}
