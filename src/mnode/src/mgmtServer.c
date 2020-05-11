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
#include "trpc.h"
#include "tsched.h"
#include "tsystem.h"
#include "tutil.h"
#include "tgrant.h"
#include "tbalance.h"
#include "tglobal.h"
#include "dnode.h"
#include "mgmtDef.h"
#include "mgmtInt.h"
#include "mgmtDb.h"
#include "mgmtMnode.h"
#include "mgmtProfile.h"
#include "mgmtShell.h"
#include "mgmtSdb.h"
#include "mgmtTable.h"
#include "mgmtVgroup.h"

static void (*mgmtProcessDnodeMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *rpcMsg);
static void  *tsMgmtServerQhandle = NULL;

int32_t mgmtInitServer() {

  tsMgmtServerQhandle = taosInitScheduler(tsMaxShellConns, 1, "MS");

  mPrint("server connection to dnode is opened");
  return 0;
}

void mgmtCleanupServer() {
  if (tsMgmtServerQhandle) {
    taosCleanUpScheduler(tsMgmtServerQhandle);
    tsMgmtServerQhandle = NULL;
  }
}

void dnodeAddServerMsgHandle(uint8_t msgType, void (*fp)(SRpcMsg *rpcMsg)) {
  mgmtProcessDnodeMsgFp[msgType] = fp;
}

static void mgmtProcessRequestFromDnode(SSchedMsg *sched) {
  SRpcMsg *pMsg = sched->msg;
  (*mgmtProcessDnodeMsgFp[pMsg->msgType])(pMsg);
  rpcFreeCont(pMsg->pCont);
  free(pMsg);
}

static void mgmtAddToServerQueue(SRpcMsg *pMsg) {
  SSchedMsg schedMsg;
  schedMsg.msg = pMsg;
  schedMsg.fp  = mgmtProcessRequestFromDnode;
  taosScheduleTask(tsMgmtServerQhandle, &schedMsg);
}

void mgmtProcessReqMsgFromDnode(SRpcMsg *rpcMsg) {
  if (mgmtProcessDnodeMsgFp[rpcMsg->msgType] == NULL) {
    mError("%s is not processed in mnode", taosMsg[rpcMsg->msgType]);
    mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_MSG_NOT_PROCESSED);
    rpcFreeCont(rpcMsg->pCont);
  }

  if (rpcMsg->pCont == NULL) {
    mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_INVALID_MSG_LEN);
    return;
  }

  if (!sdbIsMaster()) {
    SRpcConnInfo connInfo;
    rpcGetConnInfo(rpcMsg->handle, &connInfo);
    
    SRpcIpSet ipSet = {0};
    dnodeGetMnodeDnodeIpSet(&ipSet);
    for (int i = 0; i < ipSet.numOfIps; ++i) 
      ipSet.port[i] = htons(ipSet.port[i]);
    
    mTrace("conn from dnode ip:%s user:%s redirect msg, inUse:%d", taosIpStr(connInfo.clientIp), connInfo.user, ipSet.inUse);
    for (int32_t i = 0; i < ipSet.numOfIps; ++i) {
      mTrace("mnode index:%d %s:%d", i, ipSet.fqdn[i], htons(ipSet.port[i]));
    }
    rpcSendRedirectRsp(rpcMsg->handle, &ipSet);
    return;
  }
  
  SRpcMsg *pMsg = malloc(sizeof(SRpcMsg));
  memcpy(pMsg, rpcMsg, sizeof(SRpcMsg));
  mgmtAddToServerQueue(pMsg);
}

