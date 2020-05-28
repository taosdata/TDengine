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

#include "os.h"
#include "taosmsg.h"
#include "tglobal.h"
#include "mnode.h"
#include "dnode.h"
#include "dnodeInt.h"
#include "dnodeMgmt.h"
#include "dnodeVWrite.h"
#include "dnodeMPeer.h"

extern void dnodeUpdateMnodeIpSetForPeer(SRpcIpSet *pIpSet);
static void (*dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *);
static void dnodeProcessReqMsgFromDnode(SRpcMsg *pMsg, SRpcIpSet *);
static void (*dnodeProcessRspMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *rpcMsg);
static void dnodeProcessRspFromDnode(SRpcMsg *pMsg, SRpcIpSet *pIpSet);
static void *tsDnodeServerRpc = NULL;
static void *tsDnodeClientRpc = NULL;

int32_t dnodeInitServer() {
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MD_CREATE_TABLE] = dnodeDispatchToVnodeWriteQueue;
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MD_DROP_TABLE]   = dnodeDispatchToVnodeWriteQueue; 
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MD_ALTER_TABLE]  = dnodeDispatchToVnodeWriteQueue;
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MD_DROP_STABLE]  = dnodeDispatchToVnodeWriteQueue;

  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE] = dnodeDispatchToDnodeMgmt; 
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MD_DROP_VNODE]   = dnodeDispatchToDnodeMgmt;
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MD_ALTER_STREAM] = dnodeDispatchToDnodeMgmt;
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_MD_CONFIG_DNODE] = dnodeDispatchToDnodeMgmt;

  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_DM_CONFIG_TABLE] = dnodeDispatchToMnodePeerQueue;
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_DM_CONFIG_VNODE] = dnodeDispatchToMnodePeerQueue;
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_DM_AUTH]         = dnodeDispatchToMnodePeerQueue;
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_DM_GRANT]        = dnodeDispatchToMnodePeerQueue;
  dnodeProcessReqMsgFp[TSDB_MSG_TYPE_DM_STATUS]       = dnodeDispatchToMnodePeerQueue;
  
  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort    = tsDnodeDnodePort;
  rpcInit.label        = "DND-S";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = dnodeProcessReqMsgFromDnode;
  rpcInit.sessions     = 100;
  rpcInit.connType     = TAOS_CONN_SERVER;
  rpcInit.idleTime     = tsShellActivityTimer * 1000;

  tsDnodeServerRpc = rpcOpen(&rpcInit);
  if (tsDnodeServerRpc == NULL) {
    dError("failed to init inter-dnodes RPC server");
    return -1;
  }

  dPrint("inter-dnodes RPC server is opened");
  return 0;
}

void dnodeCleanupServer() {
  if (tsDnodeServerRpc) {
    rpcClose(tsDnodeServerRpc);
    tsDnodeServerRpc = NULL;
    dPrint("inter-dnodes RPC server is closed");
  }
}

static void dnodeProcessReqMsgFromDnode(SRpcMsg *pMsg, SRpcIpSet *pIpSet) {
  SRpcMsg rspMsg;
  rspMsg.handle  = pMsg->handle;
  rspMsg.pCont   = NULL;
  rspMsg.contLen = 0;

  if (dnodeGetRunStatus() != TSDB_DNODE_RUN_STATUS_RUNING) {
    rspMsg.code = TSDB_CODE_NOT_READY;
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
    dTrace("RPC %p, msg:%s is ignored since dnode not running", pMsg->handle, taosMsg[pMsg->msgType]);
    return;
  }

  if (pMsg->pCont == NULL) {
    rspMsg.code = TSDB_CODE_INVALID_MSG_LEN;
    rpcSendResponse(&rspMsg);
    return;
  }

  if (dnodeProcessReqMsgFp[pMsg->msgType]) {
    (*dnodeProcessReqMsgFp[pMsg->msgType])(pMsg);
  } else {
    dTrace("RPC %p, message:%s not processed", pMsg->handle, taosMsg[pMsg->msgType]);
    rspMsg.code = TSDB_CODE_MSG_NOT_PROCESSED;
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
  }
}

int32_t dnodeInitClient() {
  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.label        = "DND-C";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = dnodeProcessRspFromDnode;
  rpcInit.sessions     = 100;
  rpcInit.connType     = TAOS_CONN_CLIENT;
  rpcInit.idleTime     = tsShellActivityTimer * 1000;
  rpcInit.user         = "t";
  rpcInit.ckey         = "key";
  rpcInit.secret       = "secret";

  tsDnodeClientRpc = rpcOpen(&rpcInit);
  if (tsDnodeClientRpc == NULL) {
    dError("failed to init mnode rpc client");
    return -1;
  }

  dPrint("inter-dnodes rpc client is opened");
  return 0;
}

void dnodeCleanupClient() {
  if (tsDnodeClientRpc) {
    rpcClose(tsDnodeClientRpc);
    tsDnodeClientRpc = NULL;
    dPrint("inter-dnodes rpc client is closed");
  }
}

static void dnodeProcessRspFromDnode(SRpcMsg *pMsg, SRpcIpSet *pIpSet) {
  if (pMsg->msgType == TSDB_MSG_TYPE_DM_STATUS_RSP && pIpSet) {
    dnodeUpdateMnodeIpSetForPeer(pIpSet);
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

void dnodeSendMsgToDnode(SRpcIpSet *ipSet, SRpcMsg *rpcMsg) {
  rpcSendRequest(tsDnodeClientRpc, ipSet, rpcMsg);
}

void dnodeSendMsgToDnodeRecv(SRpcMsg *rpcMsg, SRpcMsg *rpcRsp) {
  SRpcIpSet ipSet = {0};
  dnodeGetMnodeIpSetForPeer(&ipSet);
  rpcSendRecv(tsDnodeClientRpc, &ipSet, rpcMsg, rpcRsp);
}
