/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "os.h"
#include "taosmsg.h"
#include "trpc.h"
#include "tutil.h"
#include "tglobal.h"
#include "dnode.h"
#include "dnodeLog.h"
#include "dnodeMgmt.h"

static void *tsDnodeClientRpc;
static void (*dnodeProcessDnodeRspFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *rpcMsg);
static void dnodeProcessRspFromDnode(SRpcMsg *pMsg);
extern void dnodeUpdateIpSet(void *ahandle, SRpcIpSet *pIpSet);

int32_t dnodeInitClient() {
  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.label        = "DND-C";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = dnodeProcessRspFromDnode;
  rpcInit.ufp          = dnodeUpdateIpSet;
  rpcInit.sessions     = 100;
  rpcInit.connType     = TAOS_CONN_CLIENT;
  rpcInit.idleTime     = tsShellActivityTimer * 2000;
  rpcInit.user         = "t";
  rpcInit.ckey         = "key";
  rpcInit.secret       = "secret";

  tsDnodeClientRpc = rpcOpen(&rpcInit);
  if (tsDnodeClientRpc == NULL) {
    dError("failed to init mnode rpc client");
    return -1;
  }

  dPrint("inter-dndoes rpc client is opened");
  return 0;
}

void dnodeCleanupClient() {
  if (tsDnodeClientRpc) {
    rpcClose(tsDnodeClientRpc);
    tsDnodeClientRpc = NULL;
    dPrint("inter-dnodes rpc client is closed");
  }
}

static void dnodeProcessRspFromDnode(SRpcMsg *pMsg) {
  if (dnodeProcessDnodeRspFp[pMsg->msgType]) {
    (*dnodeProcessDnodeRspFp[pMsg->msgType])(pMsg);
  } else {
    dError("%s is not processed", taosMsg[pMsg->msgType]);
  }
  rpcFreeCont(pMsg->pCont);
}

void dnodeAddClientRspHandle(uint8_t msgType, void (*fp)(SRpcMsg *rpcMsg)) {
  dnodeProcessDnodeRspFp[msgType] = fp;
}

void dnodeSendMsgToDnode(SRpcIpSet *ipSet, SRpcMsg *rpcMsg) {
  rpcSendRequest(tsDnodeClientRpc, ipSet, rpcMsg);
}
