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
#include "tsched.h"
#include "tsystem.h"
#include "tutil.h"
#include "tglobal.h"
#include "dnode.h"
#include "tgrant.h"
#include "mgmtDef.h"
#include "mgmtLog.h"
#include "mgmtMnode.h"
#include "mgmtDb.h"
#include "mgmtDnode.h"
#include "mgmtProfile.h"
#include "mgmtShell.h"
#include "mgmtTable.h"
#include "mgmtVgroup.h"

static void   mgmtProcessRspFromDnode(SRpcMsg *rpcMsg);
static void (*mgmtProcessDnodeRspFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *rpcMsg);
static void  *tsMgmtDClientRpc = NULL;

int32_t mgmtInitDClient() {
  SRpcInit rpcInit = {0};
  rpcInit.localIp      = tsAnyIp ? "0.0.0.0" : tsPrivateIp;
  rpcInit.localPort    = 0;
  rpcInit.label        = "MND-DC";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = mgmtProcessRspFromDnode;
  rpcInit.sessions     = 100;
  rpcInit.connType     = TAOS_CONN_CLIENT;
  rpcInit.idleTime     = tsShellActivityTimer * 1000;
  rpcInit.user         = "mgmtDClient";
  rpcInit.ckey         = "key";
  rpcInit.secret       = "secret";

  tsMgmtDClientRpc = rpcOpen(&rpcInit);
  if (tsMgmtDClientRpc == NULL) {
    mError("failed to init client connection to dnode");
    return -1;
  }

  mPrint("client connection to dnode is opened");
  return 0;
}

void mgmtCleanupDClient() {
  if (tsMgmtDClientRpc) {
    rpcClose(tsMgmtDClientRpc);
    tsMgmtDClientRpc = NULL;
  }
}

void mgmtAddDClientRspHandle(uint8_t msgType, void (*fp)(SRpcMsg *rpcMsg)) {
  mgmtProcessDnodeRspFp[msgType] = fp;
}

void mgmtSendMsgToDnode(SRpcIpSet *ipSet, SRpcMsg *rpcMsg) {
  rpcSendRequest(tsMgmtDClientRpc, ipSet, rpcMsg);
}

static void mgmtProcessRspFromDnode(SRpcMsg *rpcMsg) {
  if (mgmtProcessDnodeRspFp[rpcMsg->msgType]) {
    (*mgmtProcessDnodeRspFp[rpcMsg->msgType])(rpcMsg);
  } else {
    mError("%s is not processed in dclient", taosMsg[rpcMsg->msgType]);
  }

  rpcFreeCont(rpcMsg->pCont);
}
