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
#include "tmodule.h"
#include "tstatus.h"
#include "mgmtBalance.h"
#include "mgmtDnode.h"
#include "mgmtDClient.h"
#include "mgmtMnode.h"
#include "mgmtShell.h"
#include "mgmtDServer.h"
#include "mgmtUser.h"
#include "mgmtVgroup.h"

static void    mgmtProcessCfgDnodeMsg(SQueuedMsg *pMsg);
static void    mgmtProcessCfgDnodeMsgRsp(SRpcMsg *rpcMsg) ;
static void    mgmtProcessDnodeStatusMsg(SRpcMsg *rpcMsg);
extern int32_t clusterInit();
extern void    clusterCleanUp();
extern int32_t clusterGetDnodesNum();
extern SDnodeObj* clusterGetDnode(int32_t dnodeId);
extern SDnodeObj* clusterGetDnodeByIp(uint32_t ip);
static SDnodeObj  tsDnodeObj = {0};

int32_t mgmtInitDnodes() {
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_CONFIG_DNODE, mgmtProcessCfgDnodeMsg);
  mgmtAddDClientRspHandle(TSDB_MSG_TYPE_MD_CONFIG_DNODE_RSP, mgmtProcessCfgDnodeMsgRsp);
  mgmtAddDServerMsgHandle(TSDB_MSG_TYPE_DM_STATUS, mgmtProcessDnodeStatusMsg);

#ifdef _CLUSTER
  return clusterInit();
#else
  tsDnodeObj.dnodeId          = 1;
  tsDnodeObj.privateIp        = inet_addr(tsPrivateIp);
  tsDnodeObj.publicIp         = inet_addr(tsPublicIp);
  tsDnodeObj.createdTime      = taosGetTimestampMs();
  tsDnodeObj.numOfTotalVnodes = tsNumOfTotalVnodes;
  tsDnodeObj.status           = TSDB_DN_STATUS_OFFLINE;
  tsDnodeObj.lastReboot       = taosGetTimestampSec();
  sprintf(tsDnodeObj.dnodeName, "%d", tsDnodeObj.dnodeId);

  tsDnodeObj.moduleStatus |= (1 << TSDB_MOD_MGMT);
  if (tsEnableHttpModule) {
    tsDnodeObj.moduleStatus |= (1 << TSDB_MOD_HTTP);
  }
  if (tsEnableMonitorModule) {
    tsDnodeObj.moduleStatus |= (1 << TSDB_MOD_MONITOR);
  }
  return 0;
#endif
}

void mgmtCleanUpDnodes() {
#ifdef _CLUSTER
  clusterCleanUp();
#endif
}

SDnodeObj *mgmtGetDnode(int32_t dnodeId) {
#ifdef _CLUSTER
  return clusterGetDnode(dnodeId);
#else
  if (dnodeId == 1) {
    return &tsDnodeObj;
  } else {
    return NULL;
  }
#endif
}

SDnodeObj *mgmtGetDnodeByIp(uint32_t ip) {
#ifdef _CLUSTER
  return clusterGetDnodeByIp(ip);
#else
  return &tsDnodeObj;
#endif
}

int32_t mgmtGetDnodesNum() {
#ifdef _CLUSTER
  return clusterGetDnodesNum();
#else
  return 1;
#endif
}

void mgmtProcessCfgDnodeMsg(SQueuedMsg *pMsg) {
  SRpcMsg rpcRsp = {.handle = pMsg->thandle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  if (mgmtCheckRedirect(pMsg->thandle)) return;

  SCMCfgDnodeMsg *pCmCfgDnode = pMsg->pCont;
  if (pCmCfgDnode->ip[0] == 0) {
    strcpy(pCmCfgDnode->ip, tsPrivateIp);
  } else {
    strcpy(pCmCfgDnode->ip, pCmCfgDnode->ip);
  }
  uint32_t dnodeIp = inet_addr(pCmCfgDnode->ip);

  if (strcmp(pMsg->pUser->pAcct->user, "root") != 0) {
    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
  } else {
    SRpcIpSet ipSet = mgmtGetIpSetFromIp(dnodeIp);
    SMDCfgDnodeMsg *pMdCfgDnode = rpcMallocCont(sizeof(SMDCfgDnodeMsg));
    strcpy(pMdCfgDnode->ip, pCmCfgDnode->ip);
    strcpy(pMdCfgDnode->config, pCmCfgDnode->config);
    SRpcMsg rpcMdCfgDnodeMsg = {
        .handle = 0,
        .code = 0,
        .msgType = TSDB_MSG_TYPE_MD_CONFIG_DNODE,
        .pCont = pMdCfgDnode,
        .contLen = sizeof(SMDCfgDnodeMsg)
    };
    mgmtSendMsgToDnode(&ipSet, &rpcMdCfgDnodeMsg);
    rpcRsp.code = TSDB_CODE_SUCCESS;
  }

  if (rpcRsp.code == TSDB_CODE_SUCCESS) {
    mTrace("dnode:%s is configured by %s", pCmCfgDnode->ip, pMsg->pUser->user);
  }

  rpcSendResponse(&rpcRsp);
}

static void mgmtProcessCfgDnodeMsgRsp(SRpcMsg *rpcMsg) {
  mTrace("cfg vnode rsp is received");
}

void mgmtProcessDnodeStatusMsg(SRpcMsg *rpcMsg) {
  if (mgmtCheckRedirect(rpcMsg->handle)) return;

  SDMStatusMsg *pStatus = rpcMsg->pCont;
  pStatus->dnodeId = htonl(pStatus->dnodeId);
  pStatus->privateIp = htonl(pStatus->privateIp);
  pStatus->publicIp = htonl(pStatus->publicIp);
  pStatus->lastReboot = htonl(pStatus->lastReboot);
  pStatus->numOfCores = htons(pStatus->numOfCores);
  pStatus->numOfTotalVnodes = htons(pStatus->numOfTotalVnodes);

  SDnodeObj *pDnode = NULL;
  if (pStatus->dnodeId == 0) {
    pDnode = mgmtGetDnodeByIp(pStatus->privateIp);
    if (pDnode == NULL) {
      mTrace("dnode not created, privateIp:%s", taosIpStr(pStatus->privateIp));
      mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_DNODE_NOT_EXIST);
      return;
    }
  } else {
    pDnode = mgmtGetDnode(pStatus->dnodeId);
    if (pDnode == NULL) {
      mError("dnode:%d, not exist, privateIp:%s", pStatus->dnodeId, taosIpStr(pStatus->privateIp));
      mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_DNODE_NOT_EXIST);
      return;
    }
  }

  uint32_t version = htonl(pStatus->version);
  if (version != tsVersion) {
    mError("dnode:%d, status msg version:%d not equal with mnode:%d", pDnode->dnodeId, version, tsVersion);
    mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_INVALID_MSG_VERSION);
    return ;
  }
  
  pDnode->privateIp        = pStatus->privateIp;
  pDnode->publicIp         = pStatus->publicIp;
  pDnode->lastReboot       = pStatus->lastReboot;
  pDnode->numOfCores       = pStatus->numOfCores;
  pDnode->diskAvailable    = pStatus->diskAvailable;
  pDnode->alternativeRole  = pStatus->alternativeRole;
  pDnode->numOfTotalVnodes = pStatus->numOfTotalVnodes; 
  
  if (pStatus->dnodeId == 0) {
    mTrace("dnode:%d, first access, privateIp:%s, name:%s", pDnode->dnodeId, taosIpStr(pDnode->privateIp), pDnode->dnodeName);
  }
 
  int32_t openVnodes = htons(pStatus->openVnodes);
  for (int32_t j = 0; j < openVnodes; ++j) {
    pDnode->vload[j].vgId          = htonl(pStatus->load[j].vgId);
    pDnode->vload[j].totalStorage  = htobe64(pStatus->load[j].totalStorage);
    pDnode->vload[j].compStorage   = htobe64(pStatus->load[j].compStorage);
    pDnode->vload[j].pointsWritten = htobe64(pStatus->load[j].pointsWritten);
    
    SVgObj *pVgroup = mgmtGetVgroup(pDnode->vload[j].vgId);
    if (pVgroup == NULL) {
      SRpcIpSet ipSet = mgmtGetIpSetFromIp(pDnode->privateIp);
      mPrint("dnode:%d, vgroup:%d not exist in mnode, drop it", pDnode->dnodeId, pDnode->vload[j].vgId);
      mgmtSendDropVnodeMsg(pDnode->vload[j].vgId, &ipSet, NULL);
    }
  }

  if (pDnode->status != TSDB_DN_STATUS_READY) {
    mTrace("dnode:%d, from offline to online", pDnode->dnodeId);
    pDnode->status = TSDB_DN_STATUS_READY;
    mgmtStartBalanceTimer(200);
  }

  int32_t contLen = sizeof(SDMStatusRsp) + TSDB_MAX_VNODES * sizeof(SVnodeAccess);
  SDMStatusRsp *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return;
  }

  mgmtGetMnodePrivateIpList(&pRsp->ipList);

  pRsp->dnodeState.dnodeId = htonl(pDnode->dnodeId);
  pRsp->dnodeState.moduleStatus = htonl(pDnode->moduleStatus);
  pRsp->dnodeState.createdTime  = htonl(pDnode->createdTime / 1000);
  pRsp->dnodeState.numOfVnodes = 0;
  
  contLen = sizeof(SDMStatusRsp);

  //TODO: set vnode access
  
  SRpcMsg rpcRsp = {
    .handle  = rpcMsg->handle,
    .code    = TSDB_CODE_SUCCESS,
    .pCont   = pRsp,
    .contLen = contLen
  };

  rpcSendResponse(&rpcRsp);
}
