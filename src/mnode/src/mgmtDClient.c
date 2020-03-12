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
#include "tstatus.h"
#include "tsystem.h"
#include "tutil.h"
#include "dnode.h"
#include "mnode.h"
#include "mgmtBalance.h"
#include "mgmtDb.h"
#include "mgmtDnode.h"
#include "mgmtGrant.h"
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
  rpcInit.sessions     = tsMaxDnodes * 5;
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
    dError("%s is not processed", taosMsg[rpcMsg->msgType]);
  }

  rpcFreeCont(rpcMsg->pCont);
}

//static void mgmtProcessDropTableRsp(SRpcMsg *rpcMsg) {
//  mTrace("drop table rsp received, handle:%p code:%d", rpcMsg->handle, rpcMsg->code);
//}
//
//static void mgmtProcessAlterTableRsp(SRpcMsg *rpcMsg) {
//  mTrace("alter table rsp received, handle:%p code:%d", rpcMsg->handle, rpcMsg->code);
//}
//
//
//static void mgmtProcessDropVnodeRsp(SRpcMsg *rpcMsg) {
//  mTrace("drop vnode rsp received, handle:%p code:%d", rpcMsg->handle, rpcMsg->code);
//}
//
//static void mgmtProcessAlterVnodeRsp(SRpcMsg *rpcMsg) {
//  mTrace("alter vnode rsp received, handle:%p code:%d", rpcMsg->handle, rpcMsg->code);
//}
//
//static void mgmtProcessDropStableRsp(SRpcMsg *rpcMsg) {
//  mTrace("drop stable rsp received, handle:%p code:%d", rpcMsg->handle, rpcMsg->code);
//}
//
//static void mgmtProcessAlterStreamRsp(SRpcMsg *rpcMsg) {
//  mTrace("alter stream rsp received, handle:%p code:%d", rpcMsg->handle, rpcMsg->code);
//}
//
//static void mgmtProcessConfigDnodeRsp(SRpcMsg *rpcMsg) {
//  mTrace("config dnode rsp received, handle:%p code:%d", rpcMsg->handle, rpcMsg->code);
//}
//


//
//void mgmtSendAlterStreamMsg(STableInfo *pTable, SRpcIpSet *ipSet, void *ahandle) {
//  mTrace("table:%s, send alter stream msg, ahandle:%p", pTable->tableId, pTable->sid, ahandle);
//}
//
//void mgmtSendDropVnodeMsg(int32_t vgId, int32_t vnode, SRpcIpSet *ipSet, void *ahandle) {
//  mTrace("vnode:%d send free vnode msg, ahandle:%p", vnode, ahandle);
//  SMDDropVnodeMsg *pDrop = rpcMallocCont(sizeof(SMDDropVnodeMsg));
//  SRpcMsg rpcMsg = {
//      .handle  = ahandle,
//      .pCont   = pDrop,
//      .contLen = pDrop ? sizeof(SMDDropVnodeMsg) : 0,
//      .code    = 0,
//      .msgType = TSDB_MSG_TYPE_MD_DROP_VNODE
//  };
//  rpcSendRequest(tsMgmtDClientRpc, ipSet, &rpcMsg);
//}
//

////
////int32_t mgmtCfgDynamicOptions(SDnodeObj *pDnode, char *msg) {
////  char *option, *value;
////  int32_t   olen, valen;
////
////  paGetToken(msg, &option, &olen);
////  if (strncasecmp(option, "unremove", 8) == 0) {
////    mgmtSetDnodeUnRemove(pDnode);
////    return TSDB_CODE_SUCCESS;
////  } else if (strncasecmp(option, "score", 5) == 0) {
////    paGetToken(option + olen + 1, &value, &valen);
////    if (valen > 0) {
////      int32_t score = atoi(value);
////      mTrace("dnode:%s, custom score set from:%d to:%d", taosIpStr(pDnode->privateIp), pDnode->customScore, score);
////      pDnode->customScore = score;
////      mgmtUpdateDnode(pDnode);
////      //mgmtStartBalanceTimer(15);
////    }
////    return TSDB_CODE_INVALID_SQL;
////  } else if (strncasecmp(option, "bandwidth", 9) == 0) {
////    paGetToken(msg, &value, &valen);
////    if (valen > 0) {
////      int32_t bandwidthMb = atoi(value);
////      if (bandwidthMb >= 0 && bandwidthMb < 10000000) {
////        mTrace("dnode:%s, bandwidth(Mb) set from:%d to:%d", taosIpStr(pDnode->privateIp), pDnode->bandwidthMb, bandwidthMb);
////        pDnode->bandwidthMb = bandwidthMb;
////        mgmtUpdateDnode(pDnode);
////        return TSDB_CODE_SUCCESS;
////      }
////    }
////    return TSDB_CODE_INVALID_SQL;
////  }
////
////  return -1;
////}
////
