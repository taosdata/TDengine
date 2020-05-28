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
#include "os.h"
#include "taoserror.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tglobal.h"
#include "http.h"
#include "mnode.h"
#include "dnode.h"
#include "dnodeInt.h"
#include "dnodeVRead.h"
#include "dnodeVWrite.h"
#include "dnodeMRead.h"
#include "dnodeMWrite.h"
#include "dnodeShell.h"

static void  (*dnodeProcessShellMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *);
static void    dnodeProcessMsgFromShell(SRpcMsg *pMsg, SRpcIpSet *);
static int     dnodeRetrieveUserAuthInfo(char *user, char *spi, char *encrypt, char *secret, char *ckey);
static void  * tsDnodeShellRpc = NULL;
static int32_t tsDnodeQueryReqNum  = 0;
static int32_t tsDnodeSubmitReqNum = 0;

int32_t dnodeInitShell() {
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_SUBMIT] = dnodeDispatchToVnodeWriteQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_QUERY]  = dnodeDispatchToVnodeReadQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_FETCH]  = dnodeDispatchToVnodeReadQueue;

  // the following message shall be treated as mnode write
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_CREATE_ACCT] = dnodeDispatchToMnodeWriteQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_ALTER_ACCT]  = dnodeDispatchToMnodeWriteQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_DROP_ACCT]   = dnodeDispatchToMnodeWriteQueue; 
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_CREATE_USER] = dnodeDispatchToMnodeWriteQueue; 
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_ALTER_USER]  = dnodeDispatchToMnodeWriteQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_DROP_USER]   = dnodeDispatchToMnodeWriteQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_CREATE_DNODE]= dnodeDispatchToMnodeWriteQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_DROP_DNODE]  = dnodeDispatchToMnodeWriteQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_CREATE_DB]   = dnodeDispatchToMnodeWriteQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_DROP_DB]     = dnodeDispatchToMnodeWriteQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_ALTER_DB]    = dnodeDispatchToMnodeWriteQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_CREATE_TABLE]= dnodeDispatchToMnodeWriteQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_DROP_TABLE]  = dnodeDispatchToMnodeWriteQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_ALTER_TABLE] = dnodeDispatchToMnodeWriteQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_ALTER_STREAM]= dnodeDispatchToMnodeWriteQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_KILL_QUERY]  = dnodeDispatchToMnodeWriteQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_KILL_STREAM] = dnodeDispatchToMnodeWriteQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_KILL_CONN]   = dnodeDispatchToMnodeWriteQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_CONFIG_DNODE]= dnodeDispatchToMnodeWriteQueue;
  
  // the following message shall be treated as mnode query 
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_HEARTBEAT]   = dnodeDispatchToMnodeReadQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_CONNECT]     = dnodeDispatchToMnodeReadQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_USE_DB]      = dnodeDispatchToMnodeReadQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_TABLE_META]  = dnodeDispatchToMnodeReadQueue; 
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_STABLE_VGROUP]= dnodeDispatchToMnodeReadQueue;   
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_TABLES_META] = dnodeDispatchToMnodeReadQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_SHOW]        = dnodeDispatchToMnodeReadQueue;
  dnodeProcessShellMsgFp[TSDB_MSG_TYPE_CM_RETRIEVE]    = dnodeDispatchToMnodeReadQueue;

  int32_t numOfThreads = tsNumOfCores * tsNumOfThreadsPerCore;
  numOfThreads = (int32_t) ((1.0 - tsRatioOfQueryThreads) * numOfThreads / 2.0);
  if (numOfThreads < 1) {
    numOfThreads = 1;
  }

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort    = tsDnodeShellPort;
  rpcInit.label        = "SHELL";
  rpcInit.numOfThreads = numOfThreads;
  rpcInit.cfp          = dnodeProcessMsgFromShell;
  rpcInit.sessions     = TSDB_SESSIONS_PER_DNODE;
  rpcInit.connType     = TAOS_CONN_SERVER;
  rpcInit.idleTime     = tsShellActivityTimer * 1000;
  rpcInit.afp          = dnodeRetrieveUserAuthInfo;

  tsDnodeShellRpc = rpcOpen(&rpcInit);
  if (tsDnodeShellRpc == NULL) {
    dError("failed to init shell rpc server");
    return -1;
  }

  dPrint("shell rpc server is opened");
  return 0;
}

void dnodeCleanupShell() {
  if (tsDnodeShellRpc) {
    rpcClose(tsDnodeShellRpc);
    tsDnodeShellRpc = NULL;
  }
}

void dnodeProcessMsgFromShell(SRpcMsg *pMsg, SRpcIpSet *pIpSet) {
  SRpcMsg rpcMsg;
  rpcMsg.handle = pMsg->handle;
  rpcMsg.pCont = NULL;
  rpcMsg.contLen = 0;

  if (dnodeGetRunStatus() != TSDB_DNODE_RUN_STATUS_RUNING) {
    dError("RPC %p, shell msg:%s is ignored since dnode not running", pMsg->handle, taosMsg[pMsg->msgType]);
    rpcMsg.code = TSDB_CODE_NOT_READY;
    rpcSendResponse(&rpcMsg);
    rpcFreeCont(pMsg->pCont);
    return;
  }

  if (pMsg->msgType == TSDB_MSG_TYPE_QUERY) {
    atomic_fetch_add_32(&tsDnodeQueryReqNum, 1);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_SUBMIT) {
    atomic_fetch_add_32(&tsDnodeSubmitReqNum, 1);
  } else {}

  if ( dnodeProcessShellMsgFp[pMsg->msgType] ) {
    (*dnodeProcessShellMsgFp[pMsg->msgType])(pMsg);
  } else {
    dError("RPC %p, shell msg:%s is not processed", pMsg->handle, taosMsg[pMsg->msgType]);
    rpcMsg.code = TSDB_CODE_MSG_NOT_PROCESSED;
    rpcSendResponse(&rpcMsg);
    rpcFreeCont(pMsg->pCont);
    return;
  }
}

static int dnodeRetrieveUserAuthInfo(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  int code = mnodeRetriveAuth(user, spi, encrypt, secret, ckey);
  if (code != TSDB_CODE_NOT_READY) return code;

  SDMAuthMsg *pMsg = rpcMallocCont(sizeof(SDMAuthMsg));
  strcpy(pMsg->user, user);

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pMsg;
  rpcMsg.contLen = sizeof(SDMAuthMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_DM_AUTH;
  
  dTrace("user:%s, send auth msg to mnode", user);
  SRpcMsg rpcRsp = {0};
  dnodeSendMsgToDnodeRecv(&rpcMsg, &rpcRsp);

  if (rpcRsp.code != 0) {
    dError("user:%s, auth msg received from mnode, error:%s", user, tstrerror(rpcRsp.code));
  } else {
    SDMAuthRsp *pRsp = rpcRsp.pCont;
    dTrace("user:%s, auth msg received from mnode", user);
    memcpy(secret, pRsp->secret, TSDB_KEY_LEN);
    memcpy(ckey, pRsp->ckey, TSDB_KEY_LEN);
    *spi = pRsp->spi;
    *encrypt = pRsp->encrypt;
  }

  rpcFreeCont(rpcRsp.pCont);
  return rpcRsp.code;
}

SDnodeStatisInfo dnodeGetStatisInfo() {
  SDnodeStatisInfo info = {0};
  if (dnodeGetRunStatus() == TSDB_DNODE_RUN_STATUS_RUNING) {
    info.httpReqNum   = httpGetReqCount();
    info.queryReqNum  = atomic_exchange_32(&tsDnodeQueryReqNum, 0);
    info.submitReqNum = atomic_exchange_32(&tsDnodeSubmitReqNum, 0);
  }

  return info;
}
