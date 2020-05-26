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

static void (*tsMnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MAX])(SMnodeMsg *);

void mnodeAddMgmtMsgHandle(uint8_t msgType, void (*fp)(SMnodeMsg *pMsg)) {
  tsMnodeProcessMgmtMsgFp[msgType] = fp;
}

int32_t mnodeProcessMgmt(SMnodeMsg *pMsg) {
  SRpcMsg *rpcMsg = &pMsg->rpcMsg;
  if (rpcMsg->pCont == NULL) {
    mError("%p, msg:%s content is null", rpcMsg->ahandle, taosMsg[rpcMsg->msgType]);
    return TSDB_CODE_INVALID_MSG_LEN;
  }

  if (!sdbIsMaster()) {
    SMnodeRsp *rpcRsp = &pMsg->rpcRsp;
    SRpcIpSet *ipSet = rpcMallocCont(sizeof(SRpcIpSet));
    mgmtGetMnodeIpSetForPeer(ipSet);
    rpcRsp->rsp = ipSet;
    rpcRsp->len = sizeof(SRpcIpSet);

    mTrace("%p, msg:%s will be redireced, inUse:%d", rpcMsg->ahandle, taosMsg[rpcMsg->msgType], ipSet->inUse);
    for (int32_t i = 0; i < ipSet->numOfIps; ++i) {
      mTrace("mnode index:%d ip:%s:%d", i, ipSet->fqdn[i], htons(ipSet->port[i]));
    }

    return TSDB_CODE_REDIRECT;
  }

  if (tsMnodeProcessMgmtMsgFp[rpcMsg->msgType] == NULL) {
    mError("%p, msg:%s not processed, no handle exist", rpcMsg->ahandle, taosMsg[rpcMsg->msgType]);
    return TSDB_CODE_MSG_NOT_PROCESSED;
  }

  return (*tsMnodeProcessMgmtMsgFp[rpcMsg->msgType])(rpcMsg, );
}
