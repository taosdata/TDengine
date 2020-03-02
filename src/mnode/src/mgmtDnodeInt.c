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
#include "mgmtDnodeInt.h"
#include "mgmtProfile.h"
#include "mgmtShell.h"
#include "mgmtTable.h"
#include "mgmtVgroup.h"

int32_t (*mgmtInitDnodeIntFp)() = NULL;
void (*mgmtCleanUpDnodeIntFp)() = NULL;

void (*mgmtSendMsgToDnodeFp)(SRpcIpSet *ipSet, int8_t msgType, void *pCont, int32_t contLen, void *ahandle) = NULL;
void (*mgmtSendRspToDnodeFp)(void *handle, int32_t code, void *pCont, int32_t contLen) = NULL;
void *mgmtStatusTimer = NULL;

static void mgmtProcessDnodeStatus(int8_t msgType, void *pCont, int32_t contLen, void *pConn, int32_t code);

static void mgmtSendMsgToDnodeQueueFp(SSchedMsg *sched) {
  int32_t contLen  = *(int32_t *) (sched->msg - 4);
  int32_t code     = *(int32_t *) (sched->msg - 8);
  int8_t  msgType  = *(int8_t *) (sched->msg - 9);
  void    *ahandle = sched->ahandle;
  int8_t  *pCont   = sched->msg;

  dnodeProcessMsgFromMgmt(msgType, pCont, contLen, ahandle, code);
}

void mgmtSendMsgToDnode(SRpcIpSet *ipSet, int8_t msgType, void *pCont, int32_t contLen, void *ahandle) {
  mTrace("msg:%d:%s is sent to dnode, ahandle:%p", msgType, taosMsg[msgType], ahandle);
  if (mgmtSendMsgToDnodeFp) {
    mgmtSendMsgToDnodeFp(ipSet, msgType, pCont, contLen, ahandle);
  } else {
    if (pCont == NULL) {
      pCont = rpcMallocCont(1);
      contLen = 0;
    }
    SSchedMsg schedMsg = {0};
    schedMsg.fp      = mgmtSendMsgToDnodeQueueFp;
    schedMsg.msg     = pCont;
    schedMsg.ahandle = ahandle;
    *(int32_t *) (pCont - 4) = contLen;
    *(int32_t *) (pCont - 8) = TSDB_CODE_SUCCESS;
    *(int8_t *)  (pCont - 9) = msgType;
    taosScheduleTask(tsDnodeMgmtQhandle, &schedMsg);
  }
}

void mgmtSendRspToDnode(void *pConn, int8_t msgType, int32_t code, void *pCont, int32_t contLen) {
  mTrace("rsp:%d:%s is sent to dnode", msgType, taosMsg[msgType]);
  if (mgmtSendRspToDnodeFp) {
    mgmtSendRspToDnodeFp(pConn, code, pCont, contLen);
  } else {
    if (pCont == NULL) {
      pCont = rpcMallocCont(1);
      contLen = 0;
    }
    SSchedMsg schedMsg = {0};
    schedMsg.fp  = mgmtSendMsgToDnodeQueueFp;
    schedMsg.msg = pCont;
    *(int32_t *) (pCont - 4) = contLen;
    *(int32_t *) (pCont - 8) = code;
    *(int8_t *)  (pCont - 9) = msgType;
    taosScheduleTask(tsDnodeMgmtQhandle, &schedMsg);
  }
}

static void mgmtProcessTableCfgMsg(int8_t msgType, int8_t *pCont, int32_t contLen, void *thandle) {
  STableCfgMsg *pCfg = (STableCfgMsg *) pCont;
  pCfg->dnode = htonl(pCfg->dnode);
  pCfg->vnode = htonl(pCfg->vnode);
  pCfg->sid   = htonl(pCfg->sid);
  mTrace("dnode:%s, vnode:%d, sid:%d, receive table config msg", taosIpStr(pCfg->dnode), pCfg->vnode, pCfg->sid);

  if (!sdbMaster) {
    mError("dnode:%s, vnode:%d, sid:%d, not master, redirect it", taosIpStr(pCfg->dnode), pCfg->vnode, pCfg->sid);
    mgmtSendRspToDnode(thandle, msgType + 1, TSDB_CODE_REDIRECT, NULL, 0);
    return;
  }

  STableInfo *pTable = mgmtGetTableByPos(pCfg->dnode, pCfg->vnode, pCfg->sid);
  if (pTable == NULL) {
    mError("dnode:%s, vnode:%d, sid:%d, table not found", taosIpStr(pCfg->dnode), pCfg->vnode, pCfg->sid);
    mgmtSendRspToDnode(thandle, msgType + 1, TSDB_CODE_INVALID_TABLE, NULL, 0);
    return;
  }

  mgmtSendRspToDnode(thandle, msgType + 1, TSDB_CODE_SUCCESS, NULL, 0);

  //TODO
  SRpcIpSet ipSet = mgmtGetIpSetFromIp(pCfg->dnode);
  mgmtSendCreateTableMsg(NULL, &ipSet, NULL);
}

static void mgmtProcessVnodeCfgMsg(int8_t msgType, int8_t *pCont, int32_t contLen, void *pConn) {
  if (!sdbMaster) {
    mgmtSendRspToDnode(pConn, msgType + 1, TSDB_CODE_REDIRECT, NULL, 0);
    return;
  }

  SVpeerCfgMsg *pCfg = (SVpeerCfgMsg *) pCont;
  pCfg->dnode = htonl(pCfg->dnode);
  pCfg->vnode = htonl(pCfg->vnode);

  SVgObj *pVgroup = mgmtGetVgroupByVnode(pCfg->dnode, pCfg->vnode);
  if (pVgroup == NULL) {
    mTrace("dnode:%s, vnode:%d, no vgroup info", taosIpStr(pCfg->dnode), pCfg->vnode);
    mgmtSendRspToDnode(pConn, msgType + 1, TSDB_CODE_NOT_ACTIVE_VNODE, NULL, 0);
    return;
  }

  mgmtSendRspToDnode(pConn, msgType + 1, TSDB_CODE_SUCCESS, NULL, 0);

  SRpcIpSet ipSet = mgmtGetIpSetFromIp(pCfg->dnode);
  mgmtSendCreateVnodeMsg(pVgroup, pCfg->vnode, &ipSet, NULL);
}

static void mgmtProcessCreateTableRsp(int8_t msgType, int8_t *pCont, int32_t contLen, void *thandle, int32_t code) {
  mTrace("create table rsp received, thandle:%p code:%d", thandle, code);
  if (thandle == NULL) return;

  SProcessInfo *info = thandle;
  assert(info->type == TSDB_PROCESS_CREATE_TABLE || info->type == TSDB_PROCESS_CREATE_TABLE_GET_META);
  STableInfo *pTable = info->ahandle;

  if (code != TSDB_CODE_SUCCESS) {
    mError("table:%s, failed to create in dnode, code:%d, set it dirty", pTable->tableId);
    mgmtSetTableDirty(pTable, true);
  } else {
    mTrace("table:%s, created in dnode", pTable->tableId);
    mgmtSetTableDirty(pTable, false);
  }

  if (code != TSDB_CODE_SUCCESS) {
    rpcSendResponse(info->thandle, code, NULL, 0);
  } else {
    if (info->type == TSDB_PROCESS_CREATE_TABLE_GET_META) {
      mTrace("table:%s, start to process get meta", pTable->tableId);
      mgmtProcessGetTableMeta(pTable, thandle);
    } else {
      rpcSendResponse(info->thandle, code, NULL, 0);
    }
  }

  free(info);
}

void mgmtSendCreateTableMsg(SDCreateTableMsg *pCreate, SRpcIpSet *ipSet, void *ahandle) {
  mTrace("table:%s, send create table msg, ahandle:%p", pCreate->tableId, ahandle);
  mgmtSendMsgToDnode(ipSet, TSDB_MSG_TYPE_DNODE_CREATE_TABLE, pCreate, htonl(pCreate->contLen), ahandle);
}

static void mgmtProcessRemoveTableRsp(int8_t msgType, int8_t *pCont, int32_t contLen, void *thandle, int32_t code) {
  mTrace("remove table rsp received, thandle:%p code:%d", thandle, code);
}

void mgmtSendRemoveTableMsg(SDRemoveTableMsg *pRemove, SRpcIpSet *ipSet, void *ahandle) {
  mTrace("table:%s, sid:%d send remove table msg, ahandle:%p", pRemove->tableId, htonl(pRemove->sid), ahandle);
  if (pRemove != NULL) {
    mgmtSendMsgToDnode(ipSet, TSDB_MSG_TYPE_DNODE_REMOVE_TABLE, pRemove, sizeof(SDRemoveTableMsg), ahandle);
  }
}

static void mgmtProcessFreeVnodeRsp(int8_t msgType, int8_t *pCont, int32_t contLen, void *thandle, int32_t code) {
  mTrace("free vnode rsp received, thandle:%p code:%d", thandle, code);
}

static void mgmtProcessDropStableRsp(int8_t msgType, int8_t *pCont, int32_t contLen, void *thandle, int32_t code) {
  mTrace("drop stable rsp received, thandle:%p code:%d", thandle, code);
}

static void mgmtProcessCreateVnodeRsp(int8_t msgType, int8_t *pCont, int32_t contLen, void *thandle, int32_t code) {
  mTrace("create vnode rsp received, thandle:%p code:%d", thandle, code);
  if (thandle == NULL) return;

  SProcessInfo *info = thandle;
  assert(info->type == TSDB_PROCESS_CREATE_VGROUP || info->type == TSDB_PROCESS_CREATE_VGROUP_GET_META);
  info->received++;
  SVgObj *pVgroup = info->ahandle;

  bool isGetMeta = false;
  if (info->type == TSDB_PROCESS_CREATE_VGROUP_GET_META) {
    isGetMeta = true;
  }

  mTrace("vgroup:%d, received:%d numOfVnodes:%d", pVgroup->vgId, info->received, pVgroup->numOfVnodes);
  if (info->received == pVgroup->numOfVnodes) {
    mgmtProcessCreateTable(pVgroup, info->cont, info->contLen, info->thandle, isGetMeta);
    free(info);
  }
}

void mgmtSendCreateVgroupMsg(SVgObj *pVgroup, void *ahandle) {
  mTrace("vgroup:%d, send create all vnodes msg, ahandle:%p", pVgroup->vgId, ahandle);
  for (int i = 0; i < pVgroup->numOfVnodes; ++i) {
    SRpcIpSet ipSet = mgmtGetIpSetFromIp(pVgroup->vnodeGid[i].ip);
    mgmtSendCreateVnodeMsg(pVgroup, pVgroup->vnodeGid[i].vnode, &ipSet, ahandle);
  }
}

void mgmtSendCreateVnodeMsg(SVgObj *pVgroup, int32_t vnode, SRpcIpSet *ipSet, void *ahandle) {
  mTrace("vgroup:%d, send create vnode:%d msg, ahandle:%p", pVgroup->vgId, vnode, ahandle);
  SCreateVnodeMsg *pVpeer = mgmtBuildVpeersMsg(pVgroup, vnode);
  if (pVpeer != NULL) {
    mgmtSendMsgToDnode(ipSet, TSDB_MSG_TYPE_CREATE_VNODE, pVpeer, sizeof(SCreateVnodeMsg), ahandle);
  }
}

void mgmtProcessMsgFromDnode(char msgType, void *pCont, int32_t contLen, void *pConn, int32_t code) {
  if (msgType < 0 || msgType >= TSDB_MSG_TYPE_MAX) {
    mError("invalid msg type:%d", msgType);
    return;
  }

  mTrace("msg:%d:%s is received from dnode, pConn:%p", msgType, taosMsg[(int8_t)msgType], pConn);

  if (msgType == TSDB_MSG_TYPE_TABLE_CFG) {
    mgmtProcessTableCfgMsg(msgType, pCont, contLen, pConn);
  } else if (msgType == TSDB_MSG_TYPE_VNODE_CFG) {
    mgmtProcessVnodeCfgMsg(msgType, pCont, contLen, pConn);
  } else if (msgType == TSDB_MSG_TYPE_DNODE_CREATE_TABLE_RSP) {
    mgmtProcessCreateTableRsp(msgType, pCont, contLen, pConn, code);
  } else if (msgType == TSDB_MSG_TYPE_DNODE_REMOVE_TABLE_RSP) {
    mgmtProcessRemoveTableRsp(msgType, pCont, contLen, pConn, code);
  } else if (msgType == TSDB_MSG_TYPE_CREATE_VNODE_RSP) {
    mgmtProcessCreateVnodeRsp(msgType, pCont, contLen, pConn, code);
  } else if (msgType == TSDB_MSG_TYPE_FREE_VNODE_RSP) {
    mgmtProcessFreeVnodeRsp(msgType, pCont, contLen, pConn, code);
  } else if (msgType == TSDB_MSG_TYPE_DROP_STABLE) {
    mgmtProcessDropStableRsp(msgType, pCont, contLen, pConn, code);
  } else if (msgType == TSDB_MSG_TYPE_DNODE_CFG_RSP) {
  } else if (msgType == TSDB_MSG_TYPE_ALTER_STREAM_RSP) {
  } else if (msgType == TSDB_MSG_TYPE_STATUS) {
    mgmtProcessDnodeStatus(msgType, pConn, contLen, pConn, code);
  } else if (msgType == TSDB_MSG_TYPE_GRANT) {
    mgmtProcessDropStableRsp(msgType, pCont, contLen, pConn, code);
  } else {
    mError("%s from dnode is not processed", taosMsg[(int8_t)msgType]);
  }

  //rpcFreeCont(pCont);
}

void mgmtSendAlterStreamMsg(STableInfo *pTable, SRpcIpSet *ipSet, void *ahandle) {
  mTrace("table:%s, sid:%d send alter stream msg, ahandle:%p", pTable->tableId, pTable->sid, ahandle);
}

void mgmtSendOneFreeVnodeMsg(int32_t vnode, SRpcIpSet *ipSet, void *ahandle) {
  mTrace("vnode:%d send free vnode msg, ahandle:%p", vnode, ahandle);

  SFreeVnodeMsg *pFreeVnode = rpcMallocCont(sizeof(SFreeVnodeMsg));
  if (pFreeVnode != NULL) {
    pFreeVnode->vnode = htonl(vnode);
    mgmtSendMsgToDnode(ipSet, TSDB_MSG_TYPE_FREE_VNODE, pFreeVnode, sizeof(SFreeVnodeMsg), ahandle);
  }
}

void mgmtSendRemoveVgroupMsg(SVgObj *pVgroup, void *ahandle) {
  mTrace("vgroup:%d send free vgroup msg, ahandle:%p", pVgroup->vgId, ahandle);

  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SRpcIpSet ipSet = mgmtGetIpSetFromIp(pVgroup->vnodeGid[i].ip);
    mgmtSendOneFreeVnodeMsg(pVgroup->vnodeGid[i].vnode, &ipSet, ahandle);
  }
}

int32_t mgmtCfgDynamicOptions(SDnodeObj *pDnode, char *msg) {
  char *option, *value;
  int32_t   olen, valen;

  paGetToken(msg, &option, &olen);
  if (strncasecmp(option, "unremove", 8) == 0) {
    mgmtSetDnodeUnRemove(pDnode);
    return TSDB_CODE_SUCCESS;
  } else if (strncasecmp(option, "score", 5) == 0) {
    paGetToken(option + olen + 1, &value, &valen);
    if (valen > 0) {
      int32_t score = atoi(value);
      mTrace("dnode:%s, custom score set from:%d to:%d", taosIpStr(pDnode->privateIp), pDnode->customScore, score);
      pDnode->customScore = score;
      mgmtUpdateDnode(pDnode);
      mgmtStartBalanceTimer(15);
    }
    return TSDB_CODE_INVALID_SQL;
  } else if (strncasecmp(option, "bandwidth", 9) == 0) {
    paGetToken(msg, &value, &valen);
    if (valen > 0) {
      int32_t bandwidthMb = atoi(value);
      if (bandwidthMb >= 0 && bandwidthMb < 10000000) {
        mTrace("dnode:%s, bandwidth(Mb) set from:%d to:%d", taosIpStr(pDnode->privateIp), pDnode->bandwidthMb, bandwidthMb);
        pDnode->bandwidthMb = bandwidthMb;
        mgmtUpdateDnode(pDnode);
        return TSDB_CODE_SUCCESS;
      }
    }
    return TSDB_CODE_INVALID_SQL;
  }

  return -1;
}

int32_t mgmtSendCfgDnodeMsg(char *cont) {
//#ifdef CLUSTER
//  char *     pMsg, *pStart;
//  int32_t        msgLen = 0;
//#endif
//
//  SDnodeObj *pDnode;
//  SCfgDnodeMsg *  pCfg = (SCfgDnodeMsg *)cont;
//  uint32_t   ip;
//
//  ip = inet_addr(pCfg->ip);
//  pDnode = mgmtGetDnode(ip);
//  if (pDnode == NULL) {
//    mError("dnode ip:%s not configured", pCfg->ip);
//    return TSDB_CODE_NOT_CONFIGURED;
//  }
//
//  mTrace("dnode:%s, dynamic option received, content:%s", taosIpStr(pDnode->privateIp), pCfg->config);
//  int32_t code = mgmtCfgDynamicOptions(pDnode, pCfg->config);
//  if (code != -1) {
//    return code;
//  }
//
//#ifdef CLUSTER
//  pStart = taosBuildReqMsg(pDnode->thandle, TSDB_MSG_TYPE_DNODE_CFG);
//  if (pStart == NULL) return TSDB_CODE_NODE_OFFLINE;
//  pMsg = pStart;
//
//  memcpy(pMsg, cont, sizeof(SCfgDnodeMsg));
//  pMsg += sizeof(SCfgDnodeMsg);
//
//  msgLen = pMsg - pStart;
//  mgmtSendMsgToDnode(pDnode, pStart, msgLen);
//#else
//  (void)tsCfgDynamicOptions(pCfg->config);
//#endif
//  return 0;
}

int32_t mgmtInitDnodeInt() {
 if (mgmtInitDnodeIntFp) {
   return mgmtInitDnodeIntFp();
 } else {
   return 0;
 }
}

void mgmtCleanUpDnodeInt() {
  if (mgmtCleanUpDnodeIntFp) {
    mgmtCleanUpDnodeIntFp();
  }
}

void mgmtProcessDnodeStatus(int8_t msgType, void *pCont, int32_t contLen, void *pConn, int32_t code) {
  SStatusMsg *pStatus = (SStatusMsg *)pCont;

  SDnodeObj *pObj = mgmtGetDnode(htonl(pStatus->privateIp));
  if (pObj == NULL) {
    mError("dnode:%s not exist", taosIpStr(pObj->privateIp));
    mgmtSendRspToDnode(pConn, msgType + 1, TSDB_CODE_DNODE_NOT_EXIST, NULL, 0);
    return;
  }

  pObj->lastReboot       = htonl(pStatus->lastReboot);
  pObj->numOfTotalVnodes = htons(pStatus->numOfTotalVnodes);
  pObj->openVnodes       = htons(pStatus->openVnodes);
  pObj->numOfCores       = htons(pStatus->numOfCores);
  pObj->diskAvailable = pStatus->diskAvailable;
  pObj->alternativeRole  = pStatus->alternativeRole;
//
//  if (mgmtProcessDnodeStatusFp) {
//    mgmtProcessDnodeStatusFp(pStatus, pObj, pConn);
//    return;
//  }

  pObj->status = TSDB_DN_STATUS_READY;

//  // wait vnode dropped
//  for (int32_t vnode = 0; vnode < pObj->numOfVnodes; ++vnode) {
//    SVnodeLoad *pVload = &(pObj->vload[vnode]);
//    if (pVload->dropStatus == TSDB_VN_DROP_STATUS_DROPPING) {
//      bool existInDnode = false;
//      for (int32_t j = 0; j < pObj->openVnodes; ++j) {
//        if (htonl(pStatus->load[j].vnode) == vnode) {
//          existInDnode = true;
//          break;
//        }
//      }
//
//      if (!existInDnode) {
//        pVload->dropStatus = TSDB_VN_DROP_STATUS_READY;
//        pVload->status = TSDB_VN_STATUS_OFFLINE;
//        mgmtUpdateDnode(pObj);
//        mPrint("dnode:%s, vid:%d, drop finished", taosIpStr(pObj->privateIp), vnode);
//        taosTmrStart(mgmtMonitorDbDrop, 10000, NULL, tsMgmtTmr);
//      }
//    } else if (pVload->vgId == 0) {
//      /*
//       * In some cases, vnode information may be reported abnormally, recover it
//       */
//      if (pVload->dropStatus != TSDB_VN_DROP_STATUS_READY || pVload->status != TSDB_VN_STATUS_OFFLINE) {
//        mPrint("dnode:%s, vid:%d, vgroup:%d status:%s dropStatus:%s, set it to avail status",
//               taosIpStr(pObj->privateIp), vnode, pVload->vgId, taosGetVnodeStatusStr(pVload->status),
//               taosGetVnodeDropStatusStr(pVload->dropStatus));
//        pVload->dropStatus = TSDB_VN_DROP_STATUS_READY;
//        pVload->status = TSDB_VN_STATUS_OFFLINE;
//        mgmtUpdateDnode(pObj);
//      }
//    }
//  }
}
