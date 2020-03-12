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
#include "tstatus.h"
#include "tsystem.h"
#include "tutil.h"
#include "dnode.h"
#include "mnode.h"
#include "mgmtBalance.h"
#include "mgmtDb.h"
#include "mgmtDServer.h"
#include "mgmtGrant.h"
#include "mgmtProfile.h"
#include "mgmtShell.h"
#include "mgmtTable.h"
#include "mgmtVgroup.h"


static void   mgmtProcessMsgFromDnode(SRpcMsg *rpcMsg);
static int    mgmtDServerRetrieveAuth(char *user, char *spi, char *encrypt, char *secret, char *ckey);
static void (*mgmtProcessDnodeMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *rpcMsg);
static void  *tsMgmtDServerRpc;

int32_t mgmtInitDServer() {
  SRpcInit rpcInit = {0};
  rpcInit.localIp = tsAnyIp ? "0.0.0.0" : tsPrivateIp;;
  rpcInit.localPort    = tsMnodeDnodePort;
  rpcInit.label        = "MND-DS";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = mgmtProcessMsgFromDnode;
  rpcInit.sessions     = tsMaxDnodes * 5;
  rpcInit.connType     = TAOS_CONN_SERVER;
  rpcInit.idleTime     = tsShellActivityTimer * 1000;
  rpcInit.afp          = mgmtDServerRetrieveAuth;

  tsMgmtDServerRpc = rpcOpen(&rpcInit);
  if (tsMgmtDServerRpc == NULL) {
    mError("failed to init server connection to dnode");
    return -1;
  }

  mPrint("server connection to dnode is opened");
  return 0;
}

void mgmtCleanupDServer() {
  if (tsMgmtDServerRpc) {
    rpcClose(tsMgmtDServerRpc);
    tsMgmtDServerRpc = NULL;
    mPrint("server connection to dnode is closed");
  }
}

void mgmtAddDServerMsgHandle(uint8_t msgType, void (*fp)(SRpcMsg *rpcMsg)) {
  mgmtProcessDnodeMsgFp[msgType] = fp;
}

static void mgmtProcessMsgFromDnode(SRpcMsg *rpcMsg) {
  if (mgmtProcessDnodeMsgFp[rpcMsg->msgType]) {
    (*mgmtProcessDnodeMsgFp[rpcMsg->msgType])(rpcMsg);
  } else {
    mError("%s is not processed in dserver", taosMsg[rpcMsg->msgType]);
  }

  rpcFreeCont(rpcMsg->pCont);
}

static int mgmtDServerRetrieveAuth(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  return TSDB_CODE_SUCCESS;
}

//
//
//static void mgmtProcessTableCfgMsg(int8_t msgType, int8_t *pCont, int32_t contLen, void *thandle) {
//  SDMConfigTableMsg *pCfg = (SDMConfigTableMsg *) pCont;
//  pCfg->dnode = htonl(pCfg->dnode);
//  pCfg->vnode = htonl(pCfg->vnode);
//  pCfg->sid   = htonl(pCfg->sid);
//  mTrace("dnode:%s, vnode:%d, sid:%d, receive table config msg", taosIpStr(pCfg->dnode), pCfg->vnode, pCfg->sid);
//
//  if (!sdbMaster) {
//    mError("dnode:%s, vnode:%d, sid:%d, not master, redirect it", taosIpStr(pCfg->dnode), pCfg->vnode, pCfg->sid);
//    mgmtSendRspToDnode(thandle, msgType + 1, TSDB_CODE_REDIRECT, NULL, 0);
//    return;
//  }
//
//  STableInfo *pTable = mgmtGetTableByPos(pCfg->dnode, pCfg->vnode, pCfg->sid);
//  if (pTable == NULL) {
//    mError("dnode:%s, vnode:%d, sid:%d, table not found", taosIpStr(pCfg->dnode), pCfg->vnode, pCfg->sid);
//    mgmtSendRspToDnode(thandle, msgType + 1, TSDB_CODE_INVALID_TABLE, NULL, 0);
//    return;
//  }
//
//  mgmtSendRspToDnode(thandle, msgType + 1, TSDB_CODE_SUCCESS, NULL, 0);
//
//  //TODO
//  SRpcIpSet ipSet = mgmtGetIpSetFromIp(pCfg->dnode);
//  mgmtSendCreateTableMsg(NULL, &ipSet, NULL);
//}
//
//static void mgmtProcessVnodeCfgMsg(int8_t msgType, int8_t *pCont, int32_t contLen, void *pConn) {
//  if (!sdbMaster) {
//    mgmtSendRspToDnode(pConn, msgType + 1, TSDB_CODE_REDIRECT, NULL, 0);
//    return;
//  }
//
//  SDMConfigVnodeMsg *pCfg = (SDMConfigVnodeMsg *) pCont;
//  pCfg->dnode = htonl(pCfg->dnode);
//  pCfg->vnode = htonl(pCfg->vnode);
//
//  SVgObj *pVgroup = mgmtGetVgroupByVnode(pCfg->dnode, pCfg->vnode);
//  if (pVgroup == NULL) {
//    mTrace("dnode:%s, vnode:%d, no vgroup info", taosIpStr(pCfg->dnode), pCfg->vnode);
//    mgmtSendRspToDnode(pConn, msgType + 1, TSDB_CODE_NOT_ACTIVE_VNODE, NULL, 0);
//    return;
//  }
//
//  mgmtSendRspToDnode(pConn, msgType + 1, TSDB_CODE_SUCCESS, NULL, 0);
//
//  SRpcIpSet ipSet = mgmtGetIpSetFromIp(pCfg->dnode);
//  mgmtSendCreateVnodeMsg(pVgroup, pCfg->vnode, &ipSet, NULL);
//}
//
//static void mgmtProcessCreateTableRsp(int8_t msgType, int8_t *pCont, int32_t contLen, void *thandle, int32_t code) {
//  mTrace("create table rsp received, thandle:%p code:%d", thandle, code);
//  if (thandle == NULL) return;
//
//  SProcessInfo *info = thandle;
//  assert(info->type == TSDB_PROCESS_CREATE_TABLE || info->type == TSDB_PROCESS_CREATE_TABLE_GET_META);
//  STableInfo *pTable = info->ahandle;
//
//  if (code != TSDB_CODE_SUCCESS) {
//    mError("table:%s, failed to create in dnode, code:%d, set it dirty", pTable->tableId);
//    mgmtSetTableDirty(pTable, true);
//  } else {
//    mTrace("table:%s, created in dnode", pTable->tableId);
//    mgmtSetTableDirty(pTable, false);
//  }
//
//  if (code != TSDB_CODE_SUCCESS) {
//    SRpcMsg rpcMsg = {0};
//    rpcMsg.code = code;
//    rpcMsg.handle = info->thandle;
//    rpcSendResponse(&rpcMsg);
//  } else {
//    if (info->type == TSDB_PROCESS_CREATE_TABLE_GET_META) {
//      mTrace("table:%s, start to process get meta", pTable->tableId);
//      mgmtProcessGetTableMeta(pTable, thandle);
//    } else {
//      SRpcMsg rpcMsg = {0};
//      rpcMsg.code = code;
//      rpcMsg.handle = info->thandle;
//      rpcSendResponse(&rpcMsg);
//    }
//  }
//
//  free(info);
//}
//

//static void mgmtProcessRemoveTableRsp(int8_t msgType, int8_t *pCont, int32_t contLen, void *thandle, int32_t code) {
//  mTrace("remove table rsp received, thandle:%p code:%d", thandle, code);
//}
//

//
//static void mgmtProcessDropVnodeRsp(int8_t msgType, int8_t *pCont, int32_t contLen, void *thandle, int32_t code) {
//  mTrace("free vnode rsp received, thandle:%p code:%d", thandle, code);
//}
//
//static void mgmtProcessDropStableRsp(int8_t msgType, int8_t *pCont, int32_t contLen, void *thandle, int32_t code) {
//  mTrace("drop stable rsp received, thandle:%p code:%d", thandle, code);
//}
//
//static void mgmtProcessCreateVnodeRsp(int8_t msgType, int8_t *pCont, int32_t contLen, void *thandle, int32_t code) {
//  mTrace("create vnode rsp received, thandle:%p code:%d", thandle, code);
//  if (thandle == NULL) return;
//
//  SProcessInfo *info = thandle;
//  assert(info->type == TSDB_PROCESS_CREATE_VGROUP || info->type == TSDB_PROCESS_CREATE_VGROUP_GET_META);
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
//void mgmtProcessMsgFromDnode(char msgType, void *pCont, int32_t contLen, void *pConn, int32_t code) {
//  if (msgType < 0 || msgType >= TSDB_MSG_TYPE_MAX) {
//    mError("invalid msg type:%d", msgType);
//    return;
//  }
//
//  mTrace("msg:%d:%s is received from dnode, pConn:%p", msgType, taosMsg[(int8_t)msgType], pConn);
//
//  if (msgType == TSDB_MSG_TYPE_DM_CONFIG_TABLE) {
//    mgmtProcessTableCfgMsg(msgType, pCont, contLen, pConn);
//  } else if (msgType == TSDB_MSG_TYPE_DM_CONFIG_VNODE) {
//    mgmtProcessVnodeCfgMsg(msgType, pCont, contLen, pConn);
//  } else if (msgType == TSDB_MSG_TYPE_MD_CREATE_TABLE_RSP) {
//    mgmtProcessCreateTableRsp(msgType, pCont, contLen, pConn, code);
//  } else if (msgType == TSDB_MSG_TYPE_MD_DROP_TABLE_RSP) {
//    mgmtProcessRemoveTableRsp(msgType, pCont, contLen, pConn, code);
//  } else if (msgType == TSDB_MSG_TYPE_MD_CREATE_VNODE_RSP) {
//    mgmtProcessCreateVnodeRsp(msgType, pCont, contLen, pConn, code);
//  } else if (msgType == TSDB_MSG_TYPE_MD_DROP_VNODE_RSP) {
//    mgmtProcessDropVnodeRsp(msgType, pCont, contLen, pConn, code);
//  } else if (msgType == TSDB_MSG_TYPE_MD_DROP_STABLE) {
//    mgmtProcessDropStableRsp(msgType, pCont, contLen, pConn, code);
//  } else if (msgType == TSDB_MSG_TYPE_MD_CONFIG_DNODE_RSP) {
//  } else if (msgType == TSDB_MSG_TYPE_CM_ALTER_STREAM_RSP) {
//  } else if (msgType == TSDB_MSG_TYPE_DM_STATUS) {
//    mgmtProcessDnodeStatus(msgType, pCont, contLen, pConn, code);
//  }  else {
//    mError("%s from dnode is not processed", taosMsg[(int8_t)msgType]);
//  }
//
//  //rpcFreeCont(pCont);
//}
//
//void mgmtSendAlterStreamMsg(STableInfo *pTable, SRpcIpSet *ipSet, void *ahandle) {
//  mTrace("table:%s, sid:%d send alter stream msg, ahandle:%p", pTable->tableId, pTable->sid, ahandle);
//}
//
//void mgmtSendDropVnodeMsg(int32_t vnode, SRpcIpSet *ipSet, void *ahandle) {
//  mTrace("vnode:%d send free vnode msg, ahandle:%p", vnode, ahandle);
//
//  SMDDropVnodeMsg *pFreeVnode = rpcMallocCont(sizeof(SMDDropVnodeMsg));
//  if (pFreeVnode != NULL) {
//    pFreeVnode->vnode = htonl(vnode);
//    mgmtSendMsgToDnode(ipSet, TSDB_MSG_TYPE_MD_DROP_VNODE, pFreeVnode, sizeof(SMDDropVnodeMsg), ahandle);
//  }
//}
//

//int32_t mgmtCfgDynamicOptions(SDnodeObj *pDnode, char *msg) {
//  char *option, *value;
//  int32_t   olen, valen;
//
//  paGetToken(msg, &option, &olen);
//  if (strncasecmp(option, "unremove", 8) == 0) {
//    mgmtSetDnodeUnRemove(pDnode);
//    return TSDB_CODE_SUCCESS;
//  } else if (strncasecmp(option, "score", 5) == 0) {
//    paGetToken(option + olen + 1, &value, &valen);
//    if (valen > 0) {
//      int32_t score = atoi(value);
//      mTrace("dnode:%s, custom score set from:%d to:%d", taosIpStr(pDnode->privateIp), pDnode->customScore, score);
//      pDnode->customScore = score;
//      mgmtUpdateDnode(pDnode);
//      //mgmtStartBalanceTimer(15);
//    }
//    return TSDB_CODE_INVALID_SQL;
//  } else if (strncasecmp(option, "bandwidth", 9) == 0) {
//    paGetToken(msg, &value, &valen);
//    if (valen > 0) {
//      int32_t bandwidthMb = atoi(value);
//      if (bandwidthMb >= 0 && bandwidthMb < 10000000) {
//        mTrace("dnode:%s, bandwidth(Mb) set from:%d to:%d", taosIpStr(pDnode->privateIp), pDnode->bandwidthMb, bandwidthMb);
//        pDnode->bandwidthMb = bandwidthMb;
//        mgmtUpdateDnode(pDnode);
//        return TSDB_CODE_SUCCESS;
//      }
//    }
//    return TSDB_CODE_INVALID_SQL;
//  }
//
//  return -1;
//}
//
//
//void mgmtCleanUpDnodeInt() {
//  if (mgmtCleanUpDnodeIntFp) {
//    mgmtCleanUpDnodeIntFp();
//  }
//}
//
//void mgmtProcessDnodeStatus(int8_t msgType, void *pCont, int32_t contLen, void *pConn, int32_t code) {
//  SDMStatusMsg *pStatus = (SDMStatusMsg *)pCont;
//
//  SDnodeObj *pObj = mgmtGetDnode(htonl(pStatus->privateIp));
//  if (pObj == NULL) {
//    mError("dnode:%s not exist", taosIpStr(pObj->privateIp));
//    mgmtSendRspToDnode(pConn, msgType + 1, TSDB_CODE_DNODE_NOT_EXIST, NULL, 0);
//    return;
//  }
//
//  pObj->lastReboot       = htonl(pStatus->lastReboot);
//  pObj->numOfTotalVnodes = htons(pStatus->numOfTotalVnodes);
//  pObj->openVnodes       = htons(pStatus->openVnodes);
//  pObj->numOfCores       = htons(pStatus->numOfCores);
//  pObj->diskAvailable = pStatus->diskAvailable;
//  pObj->alternativeRole  = pStatus->alternativeRole;
////
////  if (mgmtProcessDnodeStatusFp) {
////    mgmtProcessDnodeStatusFp(pStatus, pObj, pConn);
////    return;
////  }
//
//  pObj->status = TSDB_DN_STATUS_READY;
//
////  // wait vnode dropped
////  for (int32_t vnode = 0; vnode < pObj->numOfVnodes; ++vnode) {
////    SVnodeLoad *pVload = &(pObj->vload[vnode]);
////    if (pVload->dropStatus == TSDB_VN_DROP_STATUS_DROPPING) {
////      bool existInDnode = false;
////      for (int32_t j = 0; j < pObj->openVnodes; ++j) {
////        if (htonl(pStatus->load[j].vnode) == vnode) {
////          existInDnode = true;
////          break;
////        }
////      }
////
////      if (!existInDnode) {
////        pVload->dropStatus = TSDB_VN_DROP_STATUS_READY;
////        pVload->status = TSDB_VN_STATUS_OFFLINE;
////        mgmtUpdateDnode(pObj);
////        mPrint("dnode:%s, vid:%d, drop finished", taosIpStr(pObj->privateIp), vnode);
////        taosTmrStart(mgmtMonitorDbDrop, 10000, NULL, tsMgmtTmr);
////      }
////    } else if (pVload->vgId == 0) {
////      /*
////       * In some cases, vnode information may be reported abnormally, recover it
////       */
////      if (pVload->dropStatus != TSDB_VN_DROP_STATUS_READY || pVload->status != TSDB_VN_STATUS_OFFLINE) {
////        mPrint("dnode:%s, vid:%d, vgroup:%d status:%s dropStatus:%s, set it to avail status",
////               taosIpStr(pObj->privateIp), vnode, pVload->vgId, taosGetVnodeStatusStr(pVload->status),
////               taosGetVnodeDropStatusStr(pVload->dropStatus));
////        pVload->dropStatus = TSDB_VN_DROP_STATUS_READY;
////        pVload->status = TSDB_VN_STATUS_OFFLINE;
////        mgmtUpdateDnode(pObj);
////      }
////    }
////  }
//}
