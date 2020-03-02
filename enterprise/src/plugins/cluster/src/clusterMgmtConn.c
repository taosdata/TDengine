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
#include "trpc.h"
#include "tutil.h"
#include "mnode.h"
#include "mgmtDnode.h"
#include "mgmtDnodeInt.h"

#include "dnodeSystem.h"
#include "mgmtBalance.h"
#include "dnodeModule.h"

static void *tsMgmtConnServer = NULL;
static void *tsMgmtConnClient = NULL;
extern void *tsVgroupSdb;
extern int32_t tsVgUpdateSize;

static int mgmtDnodeIntRetrieveUserAuthInfo(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  return TSDB_CODE_SUCCESS;
}

void mgmtSendMsgToDnodeImp(SRpcIpSet *ipSet, int8_t msgType, void *pCont, int32_t contLen, void *ahandle) {
  rpcSendRequest(tsMgmtConnClient, ipSet, msgType, pCont, contLen, ahandle);
}

void mgmtSendRspToDnodeImp(void *pConn, int8_t msgType, int32_t code, void *pCont, int32_t contLen) {
  rpcSendResponse(pConn, code, pCont, contLen);
}

int32_t mgmtInitDnodeIntImp() {
  SRpcInit rpcInit;

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp = tsAnyIp ? "0.0.0.0" : tsPrivateIp;;
  rpcInit.localPort    = tsMgmtDnodePort;
  rpcInit.label        = "MND-dnode-s";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = mgmtProcessMsgFromDnode;
  rpcInit.sessions     = tsMaxDnodes * 5;
  rpcInit.connType     = TAOS_CONN_SERVER;
  rpcInit.idleTime     = tsShellActivityTimer * 2000;
  rpcInit.afp          = mgmtDnodeIntRetrieveUserAuthInfo;

  tsMgmtConnServer = rpcOpen(&rpcInit);
  if (tsMgmtConnServer == NULL) {
    mError("failed to init connection of mgmt server");
    return -1;
  }

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp      = tsAnyIp ? "0.0.0.0" : tsPrivateIp;
  rpcInit.localPort    = 0;
  rpcInit.label        = "MND-dnode-c";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = mgmtProcessMsgFromDnode;
  rpcInit.sessions     = tsMaxDnodes * 5;
  rpcInit.connType     = TAOS_CONN_CLIENT;
  rpcInit.idleTime     = tsShellActivityTimer * 2000;
  rpcInit.user         = "t";
  rpcInit.ckey         = "key";
  rpcInit.secret       = "secret";

  tsMgmtConnClient = rpcOpen(&rpcInit);
  if (tsMgmtConnClient == NULL) {
    tscError("failed to init connection of mgmt client");
    return -1;
  }

  return 0;
}

void mgmtCleanUpDnodeIntImp() {
  if (tsMgmtConnServer) {
    rpcClose(tsMgmtConnServer);
    tsMgmtConnServer = NULL;
  }

  if (tsMgmtConnClient) {
    rpcClose(tsMgmtConnClient);
    tsMgmtConnClient = NULL;
  }
}

static int32_t mgmtSendStatusRspMsg(int8_t type, void *pConn, SStatusRsp *pRsp, int32_t rspLen) {
  pRsp->code        = htonl(pRsp->code);
  pRsp->ipList      = *pSdbIpList;
  pRsp->ipList.port = htons(pRsp->ipList.port);
  pRsp->numOfVnodes = htonl(pRsp->numOfVnodes);

  for (int i = 0; i < pRsp->ipList.numOfIps; ++i) {
    pRsp->ipList.ip[i] = htonl(pRsp->ipList.ip[i]);
  }

  pRsp->dnodeState.moduleStatus = htonl(pObj->moduleStatus);
  pRsp->dnodeState.createdTime = htonl(pObj->createdTime / 1000);

  mgmtSendRspToDnode(pConn, msgType + 1, TSDB_CODE_SUCCESS, pRsp, rspLen);
}

static void mgmtUpdateVgroupPublicIp(uint32_t privateIp, uint32_t oldPublicIp, uint32_t newPublicIp) {
  void *  pNode = NULL;
  SVgObj *pVgroup = NULL;
  while (1) {
    pNode = sdbFetchRow(tsVgroupSdb, pNode, (void **)&pVgroup);
    if (pVgroup == NULL) break;

    for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
      SVnodeGid *vnodeGid = pVgroup->vnodeGid + i;
      if (vnodeGid->ip == privateIp) {
        mPrint("vgroup:%d, index:%d vnode:%d ip:%s change publicIp from %s to %s", pVgroup->vgId, i, vnodeGid->vnode,
               taosIpStr(privateIp), taosIpStr(vnodeGid->publicIp), taosIpStr(newPublicIp));
        vnodeGid->publicIp = newPublicIp;
        sdbUpdateRow(tsVgroupSdb, pVgroup, tsVgUpdateSize, 1);
      }
    }
  }
}

static void mgmtUpdateMnodePublicIp(uint32_t privateIp, uint32_t oldPublicIp, uint32_t newPublicIp) {
  void *  pNode = NULL;
  SSdbPeer *pMnode = NULL;
  while (1) {
    pNode = sdbFetchRow(mnodeSdb, pNode, (void **)&pMnode);
    if (pMnode == NULL) break;

    if (pMnode->ip == privateIp) {
      mPrint("mnode:%s, change public ip from %s to %s",
              taosIpStr(pMnode->ip), taosIpStr(pMnode->publicIp), taosIpStr(newPublicIp));
      pMnode->publicIp = newPublicIp;
      sdbUpdateRow(mnodeSdb, pMnode, tsMnodeUpdateSize, 1);
    }
  }
  sdbUpdateIpList();
}

void mgmtProcessDnodeStatusImp(SStatusMsg *pStatus, SDnodeObj *pObj, int32_t msgType, void *pConn) {
  if (pStatus->version != tsVersion) {
    mError("dnode:%s status msg version:%d not equal with master:%d", taosIpStr(pObj->privateIp), pStatus->version, tsVersion);
    mgmtSendRspToDnode(pConn, msgType + 1, TSDB_CODE_INVALID_MSG_VERSION, NULL, 0);
    return;
  }

  if (!sdbMaster) {
    mError("dnode:%s status msg received, redirect the message", taosIpStr(pObj->privateIp));
    SStatusRsp *pRsp = rpcMallocCont(sizeof(SStatusRsp));
    if (pRsp == NULL) {
      mError("dnode:%s, no enough memory to alloc status rsp", taosIpStr(pObj->privateIp));
      mgmtSendRspToDnode(pConn, msgType + 1, TSDB_CODE_SERV_OUT_OF_MEMORY, NULL, 0);
      return;
    }

    pRsp->code = TSDB_CODE_REDIRECT;
    mgmtSendStatusRspMsg(msgType + 1, pConn, pRsp, sizeof(SStatusRsp));
    return;
  }

  /*
   * When publicIp changes, update the publicIP of all vnodes
   */

  if (pObj->publicIp != pStatus->publicIp) {
    mPrint("dnode:%s, change publicIp from %s to %s", taosIpStr(pObj->privateIp), taosIpStr(pObj->publicIp),
           taosIpStr(pStatus->publicIp));
    mgmtUpdateVgroupPublicIp(pObj->privateIp, pObj->publicIp, pStatus->publicIp);
    pObj->publicIp = pStatus->publicIp;
    mgmtUpdateDnode(pObj);
    mgmtUpdateMnodePublicIp(pObj->privateIp, pObj->publicIp, pStatus->publicIp);
  }

  pObj->lastReboot       = pStatus->lastReboot;
  pObj->numOfTotalVnodes = pStatus->numOfTotalVnodes;
  pObj->openVnodes       = pStatus->openVnodes;
  pObj->numOfCores       = pStatus->numOfCores;
  pObj->diskAvailable    = pStatus->diskAvailable;
  pObj->alternativeRole  = pStatus->alternativeRole;

  if (pObj->numOfVnodes == TSDB_INVALID_VNODE_NUM) {
    int32_t oldVnodes = pObj->numOfVnodes;
    mgmtSetDnodeMaxVnodes(pObj);
    mPrint("dnode:%s, first access, set total vnodes from %d to %d", taosIpStr(pObj->privateIp), oldVnodes, pObj->numOfVnodes);
  }


  int32_t rspLen = sizeof(SStatusRsp) + sizeof(SVnodeAccess) * pObj->openVnodes;
  SStatusRsp *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    mError("dnode:%s, no enough memory to alloc status rsp", taosIpStr(pObj->privateIp));
    mgmtSendRspToDnode(pConn, msgType + 1, TSDB_CODE_SERV_OUT_OF_MEMORY, NULL, 0);
    return;
  }

//  SVnodeAccess *pAccess = pRsp->vnodeAccess;
//  for (int32_t i = 0; i < pObj->openVnodes; ++i) {
//    int32_t vnode = htonl(pStatus->load[i].vnode);
//    if (vnode < 0 || vnode >= pObj->numOfVnodes) {
//      mError("dnode:%s vid:%d out of range(0, %d)", taosIpStr(pObj->privateIp), vnode, pObj->numOfVnodes - 1);
//      continue;
//    }
//
//    SVnodeLoad *pVload = &(pObj->vload[vnode]);
//    pVload->vnode = vnode;
//    pVload->status = pStatus->load[i].status;
//    pVload->syncStatus = pStatus->load[i].syncStatus;
//
//    int64_t  totalStorage = htobe64(pStatus->load[i].totalStorage);
//    int64_t  compStorage = htobe64(pStatus->load[i].compStorage);
//    int64_t  pointsWritten = htobe64(pStatus->load[i].pointsWritten);
//    uint32_t vgId = htonl(pStatus->load[i].vgId);
//
//    SVgObj *pVgroup = mgmtGetVgroup(vgId);
//    if (pVgroup == NULL) {
//      mError("vgroup:%d is not there, but vnode %d on dnode 0x%x still exists, drop it!", vgId, vnode, pObj->privateIp);
//
//      // drop vnode from dnode
//      SVnodeGid vnodeGid = {pObj->privateIp, pObj->publicIp, vnode};
//      mgmtSendOneFreeVnodeMsg(&vnodeGid);
//      continue;
//    }
//
//    SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
//    if (pDb == NULL) {
//      mError("vgroup:%d not belongs to any database, vnode:%d dnode:0x%x", vgId, pStatus->load[i].vnode,
//             pObj->privateIp);
//      continue;
//    }
//
//    SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
//    if (pAcct == NULL) {
//      mError("db:%s not belongs to any account", pDb->name);
//      continue;
//    }
//
//    // Check access status;
//    char accessState = TSDB_VN_ALL_ACCCESS;
//    if (pAcct->acctInfo.totalStorage > pAcct->cfg.maxStorage) {
//      accessState &= (~TSDB_VN_WRITE_ACCCESS);
//    }
//
//    if (grantCheckStorage() != 0) {
//      accessState &= (~TSDB_VN_WRITE_ACCCESS);
//    }
//
//    if (pAcct->acctInfo.queryTime > pAcct->cfg.maxQueryTime) {
//      accessState &= (~TSDB_VN_READ_ACCCESS);
//    }
//
//    accessState &= pAcct->cfg.accessState;
//
//    pAcct->acctInfo.accessState = accessState;
//
//    // Check if accessState is changed
//    if (pAcct->acctInfo.accessState != pStatus->load[i].accessState) {
//      pAccess->vnode = htonl(vnode);
//      pAccess->accessState = pAcct->acctInfo.accessState;
//      pAccess++;
//      pRsp->numOfVnodes ++;
//    }
//
//    pVload->totalStorage = totalStorage > 0 ? totalStorage : 0;
//    pVload->compStorage = compStorage > 0 ? compStorage : 0;
//    pVload->pointsWritten = pointsWritten > 0 ? pointsWritten : 0;
//
//    if (pVload->vgId == 0 || pVload->dropStatus == TSDB_VN_DROP_STATUS_DROPPING) {
//      mPrint("dnode:%s, vid:%d, mgmt not exist, drop it", taosIpStr(pObj->privateIp), vnode);
//      SVnodeGid pVnodeGid;
//      pVnodeGid.ip = pObj->privateIp;
//      pVnodeGid.vnode = vnode;
//      mgmtSendOneFreeVnodeMsg(&pVnodeGid);
//      memset(pVload, 0, sizeof(SVnodeLoad));
//
//      // if dnode not receive drop-vnode-msg, set the vnode to dropping state
//      pVload->dropStatus = TSDB_VN_DROP_STATUS_DROPPING;
//    }
//  }
//
//  /*
//   * In some unusual cases, the dnode is likely to have openVnodes 0.
//   */
//  //if (pObj->status != TSDB_DN_STATUS_READY && pObj->openVnodes == 0) {
//  if (pObj->status != TSDB_DN_STATUS_READY) {
//    mTrace("dnode:%s, from offline to online", taosIpStr(pObj->privateIp));
//    mgmtStartBalanceTimer(200);
//  }
//
//  pObj->lastAccess = mgmtAccessSquence;
//  pObj->status = TSDB_DN_STATUS_READY;
//
//  mgmtSendStatusRspMsg(msgType + 1, pConn, pRsp, rspLen);
}

