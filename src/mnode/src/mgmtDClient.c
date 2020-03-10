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

static void mgmtProcessRspFromDnode(SRpcMsg *rpcMsg) {
  if (mgmtProcessDnodeRspFp[rpcMsg->msgType]) {
    (*mgmtProcessDnodeRspFp[rpcMsg->msgType])(rpcMsg);
  } else {
    dError("%s is not processed", taosMsg[rpcMsg->msgType]);
  }

  rpcFreeCont(rpcMsg->pCont);
}

//static void   mgmtProcessCreateTableRsp(SRpcMsg *rpcMsg);
//static void   mgmtProcessDropTableRsp(SRpcMsg *rpcMsg);
//static void   mgmtProcessAlterTableRsp(SRpcMsg *rpcMsg);
//static void   mgmtProcessCreateVnodeRsp(SRpcMsg *rpcMsg);
//static void   mgmtProcessDropVnodeRsp(SRpcMsg *rpcMsg);
//static void   mgmtProcessAlterVnodeRsp(SRpcMsg *rpcMsg);
//static void   mgmtProcessDropStableRsp(SRpcMsg *rpcMsg);
//static void   mgmtProcessAlterStreamRsp(SRpcMsg *rpcMsg);
//static void   mgmtProcessConfigDnodeRsp(SRpcMsg *rpcMsg);

//
//static void mgmtProcessCreateTableRsp(SRpcMsg *rpcMsg) {
//  mTrace("create table rsp received, handle:%p code:%d", rpcMsg->handle, rpcMsg->code);
//  if (rpcMsg->handle == NULL) return;
//
//  SProcessInfo *info = rpcMsg->handle;
//  assert(info->type == TSDB_PROCESS_CREATE_TABLE || info->type == TSDB_PROCESS_CREATE_TABLE_GET_META);
//
//  STableInfo *pTable = info->ahandle;
//  if (rpcMsg->code != TSDB_CODE_SUCCESS) {
//    mError("table:%s, failed to create in dnode, code:%d, set it dirty", pTable->tableId, rpcMsg->code);
//    mgmtSetTableDirty(pTable, true);
//  } else {
//    mTrace("table:%s, created in dnode", pTable->tableId);
//    mgmtSetTableDirty(pTable, false);
//  }
//
//  if (rpcMsg->code != TSDB_CODE_SUCCESS) {
//    SRpcMsg rpcRsp = {.handle = info->thandle, .pCont = NULL, .contLen = 0, .code = rpcMsg->code, .msgType = 0};
//    rpcSendResponse(&rpcMsg);
//  } else {
//    if (info->type == TSDB_PROCESS_CREATE_TABLE_GET_META) {
//      mTrace("table:%s, start to process get meta", pTable->tableId);
//      mgmtProcessGetTableMeta(pTable, rpcMsg->handle);
//    } else {
//      SRpcMsg rpcRsp = {.handle = info->thandle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//      rpcSendResponse(&rpcMsg);
//    }
//  }
//
//  free(info);
//}
//
//static void mgmtProcessDropTableRsp(SRpcMsg *rpcMsg) {
//  mTrace("drop table rsp received, handle:%p code:%d", rpcMsg->handle, rpcMsg->code);
//}
//
//static void mgmtProcessAlterTableRsp(SRpcMsg *rpcMsg) {
//  mTrace("alter table rsp received, handle:%p code:%d", rpcMsg->handle, rpcMsg->code);
//}
//
//static void mgmtProcessCreateVnodeRsp(SRpcMsg *rpcMsg) {
//  mTrace("create vnode rsp received, handle:%p code:%d", rpcMsg->handle, rpcMsg->code);
//  if (rpcMsg->handle == NULL) return;
//
//  SProcessInfo *info = rpcMsg->handle;
//  assert(info->type == TSDB_PROCESS_CREATE_VGROUP || info->type == TSDB_PROCESS_CREATE_VGROUP_GET_META);
//
//  info->received++;
//  SVgObj *pVgroup = info->ahandle;
//
//  bool isGetMeta = false;
//  if (info->type == TSDB_PROCESS_CREATE_VGROUP_GET_META) {
//    isGetMeta = true;
//  }
//
//  mTrace("vgroup:%d, received:%d numOfVnodes:%d", pVgroup->vgId, info->received, pVgroup->numOfVnodes);
//  if (info->received == pVgroup->numOfVnodes) {
//    mgmtProcessCreateTable(pVgroup, info->cont, info->contLen, info->thandle, isGetMeta);
//    free(info);
//  }
//}
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
//void mgmtSendCreateTableMsg(SDMCreateTableMsg *pCreate, SRpcIpSet *ipSet, void *ahandle) {
//  mTrace("table:%s, send create table msg, ahandle:%p", pCreate->tableId, ahandle);
//  SRpcMsg rpcMsg = {
//    .handle  = ahandle,
//    .pCont   = pCreate,
//    .contLen = htonl(pCreate->contLen),
//    .code    = 0,
//    .msgType = TSDB_MSG_TYPE_MD_CREATE_TABLE
//  };
//  rpcSendRequest(tsMgmtDClientRpc, ipSet, &rpcMsg);
//}
//
//void mgmtSendDropTableMsg(SMDDropTableMsg *pDrop, SRpcIpSet *ipSet, void *ahandle) {
//  mTrace("table:%s, send drop table msg, ahandle:%p", pDrop->tableId, ahandle);
//  SRpcMsg rpcMsg = {
//    .handle  = ahandle,
//    .pCont   = pDrop,
//    .contLen = sizeof(SMDDropTableMsg),
//    .code    = 0,
//    .msgType = TSDB_MSG_TYPE_MD_DROP_TABLE
//  };
//  rpcSendRequest(tsMgmtDClientRpc, ipSet, &rpcMsg);
//}
//
//void mgmtSendCreateVnodeMsg(SVgObj *pVgroup, int32_t vnode, SRpcIpSet *ipSet, void *ahandle) {
//  mTrace("vgroup:%d, send create vnode:%d msg, ahandle:%p", pVgroup->vgId, vnode, ahandle);
//  SMDCreateVnodeMsg *pCreate = mgmtBuildCreateVnodeMsg(pVgroup, vnode);
//  SRpcMsg rpcMsg = {
//      .handle  = ahandle,
//      .pCont   = pCreate,
//      .contLen = pCreate ? sizeof(SMDCreateVnodeMsg) : 0,
//      .code    = 0,
//      .msgType = TSDB_MSG_TYPE_MD_CREATE_VNODE
//  };
//  rpcSendRequest(tsMgmtDClientRpc, ipSet, &rpcMsg);
//}
//
//void mgmtSendCreateVgroupMsg(SVgObj *pVgroup, void *ahandle) {
//  mTrace("vgroup:%d, send create all vnodes msg, handle:%p", pVgroup->vgId, ahandle);
//  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
//    SRpcIpSet ipSet = mgmtGetIpSetFromIp(pVgroup->vnodeGid[i].ip);
//    mgmtSendCreateVnodeMsg(pVgroup, pVgroup->vnodeGid[i].vnode, &ipSet, ahandle);
//  }
//}
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
//void mgmtSendDropVgroupMsg(SVgObj *pVgroup, void *ahandle) {
//  mTrace("vgroup:%d send free vgroup msg, ahandle:%p", pVgroup->vgId, ahandle);
//  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
//    SRpcIpSet ipSet = mgmtGetIpSetFromIp(pVgroup->vnodeGid[i].ip);
//    mgmtSendDropVnodeMsg(pVgroup->vgId, pVgroup->vnodeGid[i].vnode, &ipSet, ahandle);
//  }
//}
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
////int32_t mgmtSendCfgDnodeMsg(char *cont) {
////  SDnodeObj *pDnode;
////  SCfgDnodeMsg *  pCfg = (SCfgDnodeMsg *)cont;
////  uint32_t   ip;
////
////  ip = inet_addr(pCfg->ip);
////  pDnode = mgmtGetDnode(ip);
////  if (pDnode == NULL) {
////    mError("dnode ip:%s not configured", pCfg->ip);
////    return TSDB_CODE_NOT_CONFIGURED;
////  }
////
////  mTrace("dnode:%s, dynamic option received, content:%s", taosIpStr(pDnode->privateIp), pCfg->config);
////  int32_t code = mgmtCfgDynamicOptions(pDnode, pCfg->config);
////  if (code != -1) {
////    return code;
////  }
////
////#ifdef CLUSTER
////  pStart = taosBuildReqMsg(pDnode->thandle, TSDB_MSG_TYPE_MD_CONFIG_DNODE);
////  if (pStart == NULL) return TSDB_CODE_NODE_OFFLINE;
////  pMsg = pStart;
////
////  memcpy(pMsg, cont, sizeof(SCfgDnodeMsg));
////  pMsg += sizeof(SCfgDnodeMsg);
////
////  msgLen = pMsg - pStart;
////  mgmtSendMsgToDnode(pDnode, pStart, msgLen);
////#else
////  (void)tsCfgDynamicOptions(pCfg->config);
////#endif
////  return 0;
////}
