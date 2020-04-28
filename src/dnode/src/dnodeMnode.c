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

#include "os.h"
#include "taosmsg.h"
#include "tglobal.h"
#include "trpc.h"
#include "dnode.h"
#include "dnodeLog.h"
#include "dnodeMgmt.h"
#include "dnodeWrite.h"

static void (*dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *);
static void   dnodeProcessMsgFromMnode(SRpcMsg *pMsg);
static void  *tsDnodeMnodeRpc = NULL;

int32_t dnodeInitMnode() {
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_CREATE_TABLE] = dnodeWrite;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_DROP_TABLE]   = dnodeWrite;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_ALTER_TABLE]  = dnodeWrite;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_DROP_STABLE]  = dnodeWrite;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE] = dnodeMgmt;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_DROP_VNODE]   = dnodeMgmt;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_ALTER_STREAM] = dnodeMgmt;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_CONFIG_DNODE] = dnodeMgmt;

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort    = tsDnodeMnodePort;
  rpcInit.label        = "DND-MS";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = dnodeProcessMsgFromMnode;
  rpcInit.sessions     = 100;
  rpcInit.connType     = TAOS_CONN_SERVER;
  rpcInit.idleTime     = tsShellActivityTimer * 2000;

  tsDnodeMnodeRpc = rpcOpen(&rpcInit);
  if (tsDnodeMnodeRpc == NULL) {
    dError("failed to init mnode rpc server");
    return -1;
  }

  dPrint("mnode rpc server is opened");
  return 0;
}

void dnodeCleanupMnode() {
  if (tsDnodeMnodeRpc) {
    rpcClose(tsDnodeMnodeRpc);
    tsDnodeMnodeRpc = NULL;
    dPrint("mnode rpc server is closed");
  }
}

static void dnodeProcessMsgFromMnode(SRpcMsg *pMsg) {
  SRpcMsg rspMsg;
  rspMsg.handle  = pMsg->handle;
  rspMsg.pCont   = NULL;
  rspMsg.contLen = 0;

  if (dnodeGetRunStatus() != TSDB_DNODE_RUN_STATUS_RUNING) {
    rspMsg.code = TSDB_CODE_NOT_READY;
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
    dTrace("thandle:%p, query msg is ignored since dnode not running", pMsg->handle);
    return;
  }

  if (pMsg->pCont == NULL) {
    rspMsg.code = TSDB_CODE_INVALID_MSG_LEN;
    rpcSendResponse(&rspMsg);
    return;
  }

  if (dnodeProcessMgmtMsgFp[pMsg->msgType]) {
    (*dnodeProcessMgmtMsgFp[pMsg->msgType])(pMsg);
  } else {
    dError("%s is not processed in dnode mserver", taosMsg[pMsg->msgType]);
    rspMsg.code = TSDB_CODE_MSG_NOT_PROCESSED;
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
  }
}


