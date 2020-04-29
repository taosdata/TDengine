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
#include "mgmtLog.h"
#include "mgmtDb.h"
#include "mgmtDServer.h"
#include "mgmtMnode.h"
#include "mgmtProfile.h"
#include "mgmtShell.h"
#include "mgmtSdb.h"
#include "mgmtTable.h"
#include "mgmtVgroup.h"

static void   mgmtProcessMsgFromDnode(SRpcMsg *rpcMsg);
static int    mgmtDServerRetrieveAuth(char *user, char *spi, char *encrypt, char *secret, char *ckey);
static void (*mgmtProcessDnodeMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *rpcMsg);
static void  *tsMgmtDServerRpc;
static void  *tsMgmtDServerQhandle = NULL;

int32_t mgmtInitDServer() {
  SRpcInit rpcInit = {0};
  rpcInit.localPort    = tsMnodeDnodePort;
  rpcInit.label        = "MND-DS";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = mgmtProcessMsgFromDnode;
  rpcInit.sessions     = 100;
  rpcInit.connType     = TAOS_CONN_SERVER;
  rpcInit.idleTime     = tsShellActivityTimer * 1000;
  rpcInit.afp          = mgmtDServerRetrieveAuth;

  tsMgmtDServerRpc = rpcOpen(&rpcInit);
  if (tsMgmtDServerRpc == NULL) {
    mError("failed to init server connection to dnode");
    return -1;
  }

  tsMgmtDServerQhandle = taosInitScheduler(tsMaxShellConns, 1, "MS");

  mPrint("server connection to dnode is opened");
  return 0;
}

void mgmtCleanupDServer() {
  if (tsMgmtDServerQhandle) {
    taosCleanUpScheduler(tsMgmtDServerQhandle);
    tsMgmtDServerQhandle = NULL;
  }

  if (tsMgmtDServerRpc) {
    rpcClose(tsMgmtDServerRpc);
    tsMgmtDServerRpc = NULL;
    mPrint("server connection to dnode is closed");
  }
}

void mgmtAddDServerMsgHandle(uint8_t msgType, void (*fp)(SRpcMsg *rpcMsg)) {
  mgmtProcessDnodeMsgFp[msgType] = fp;
}

static void mgmtProcessDServerRequest(SSchedMsg *sched) {
  SRpcMsg *pMsg = sched->msg;
  (*mgmtProcessDnodeMsgFp[pMsg->msgType])(pMsg);
  rpcFreeCont(pMsg->pCont);
  free(pMsg);
}

static void mgmtAddToDServerQueue(SRpcMsg *pMsg) {
  SSchedMsg schedMsg;
  schedMsg.msg = pMsg;
  schedMsg.fp  = mgmtProcessDServerRequest;
  taosScheduleTask(tsMgmtDServerQhandle, &schedMsg);
}

static void mgmtProcessMsgFromDnode(SRpcMsg *rpcMsg) {
  if (rpcMsg->pCont == NULL) {
    mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_INVALID_MSG_LEN);
    return;
  }

  if (!sdbIsMaster()) {
    SRpcConnInfo connInfo;
    rpcGetConnInfo(rpcMsg->handle, &connInfo);
    
    SRpcIpSet ipSet = {0};
    dnodeGetMnodeDnodeIpSet(&ipSet);
    
    mTrace("conn from dnode ip:%s user:%s redirect msg, inUse:%d", taosIpStr(connInfo.clientIp), connInfo.user, ipSet.inUse);
    for (int32_t i = 0; i < ipSet.numOfIps; ++i) {
      mTrace("index:%d %s:%d", i, ipSet.fqdn[i], ipSet.port[i]);
    }
    rpcSendRedirectRsp(rpcMsg->handle, &ipSet);
    return;
  }
  
  if (mgmtProcessDnodeMsgFp[rpcMsg->msgType]) {
    SRpcMsg *pMsg = malloc(sizeof(SRpcMsg));
    memcpy(pMsg, rpcMsg, sizeof(SRpcMsg));
    mgmtAddToDServerQueue(pMsg);
  } else {
    mError("%s is not processed in mgmt dserver", taosMsg[rpcMsg->msgType]);
    mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_MSG_NOT_PROCESSED);
    rpcFreeCont(rpcMsg->pCont);
  }
}

static int mgmtDServerRetrieveAuth(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  return TSDB_CODE_SUCCESS;
}
