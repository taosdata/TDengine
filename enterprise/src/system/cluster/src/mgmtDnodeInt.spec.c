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
#include <arpa/inet.h>
#include <endian.h>

#include "dnodeSystem.h"
#include "mgmt.h"
#include "mgmtBalance.h"
#include "tutil.h"

void *     pDnodeConn = NULL;
SDnodeObj *dnodeList = NULL;
void *     dnodeHash;
void *     dnodeIdPool;
uint32_t   tsPrivateIp4;

void *mgmtProcessMsgFromDnodeSpec(char *msg, void *ahandle, void *thandle);
int   mgmtSendVPeersMsg(SVgObj *pVgroup);
char *mgmtBuildVpeersIe(char *pMsg, SVgObj *pVgroup, int vnode);
char *mgmtBuildCreateMeterIe(STabObj *pMeter, char *pMsg, int vnode);
void  mgmtProcessMsgFromDnode(char *content, int msgLen, int msgType, SDnodeObj *pObj);

/*
 * Communication function between dnode and mnode
 * Cluster version via network communication
 */
char *taosBuildRspMsgToDnodeWithSize(SDnodeObj *pObj, char type, int size) {
  return taosBuildRspMsgWithSize(pObj->thandle, type, size);
}

char *taosBuildReqMsgToDnodeWithSize(SDnodeObj *pObj, char type, int size) {
  return taosBuildReqMsgWithSize(pObj->thandle, type, size);
}

char *taosBuildRspMsgToDnode(SDnodeObj *pObj, char type) {
  return taosBuildRspMsgToDnodeWithSize(pObj, type, 256);
}

char *taosBuildReqMsgToDnode(SDnodeObj *pObj, char type) {
  return taosBuildReqMsgToDnodeWithSize(pObj, type, 256);
}

int taosSendSimpleRspToDnode(SDnodeObj *pObj, char rsptype, char code) {
  return taosSendSimpleRsp(pObj->thandle, rsptype, code);
}

int taosSendMsgToDnode(SDnodeObj *pObj, char *msg, int msgLen) {
  return taosSendMsgToPeer(pObj->thandle, msg, msgLen);
}

int mgmtInitDnodeInt() {
  SRpcInit rpcInit;

  int numOfThreads = tsNumOfCores * tsNumOfThreadsPerCore / 4.0;
  if (numOfThreads < 1) numOfThreads = 1;

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp = tsPrivateIp;
  rpcInit.localPort = tsMgmtVnodePort;
  rpcInit.label = "MND-dnode";
  rpcInit.numOfThreads = numOfThreads;
  rpcInit.fp = mgmtProcessMsgFromDnodeSpec;
  rpcInit.bits = 20;
  rpcInit.numOfChanns = 1;
  rpcInit.sessionsPerChann = tsMaxDnodes;
  rpcInit.idMgmt = TAOS_ID_FREE;
  rpcInit.connType = TAOS_CONN_SOCKET_TYPE_S();
  rpcInit.idleTime = tsStatusInterval * 3000;
  rpcInit.qhandle = mgmtQhandle;

  pDnodeConn = taosOpenRpc(&rpcInit);
  if (pDnodeConn == NULL) {
    mError("failed to init tcp connection to vnode");
    return -1;
  }

  tsPrivateIp4 = inet_addr(tsPrivateIp);
  return 0;
}

void mgmtCleanUpDnodeInt() {
  if (pDnodeConn) {
    taosCloseRpc(pDnodeConn);
  }
  pDnodeConn = NULL;
}

int mgmtSendStatusRspMsg(SDnodeObj *pObj, char *pVMsg, int ssize) {
  char *    pMsg, *pStart;
  int       msgLen;
  STaosRsp *pRsp;

  // if SDB is not ready, dont send response
  if (pSdbIpList == NULL) return 0;

  pStart = taosBuildRspMsgWithSize(pObj->thandle, TSDB_MSG_TYPE_STATUS_RSP, 512 + ssize);
  if (pStart == NULL) {
    taosSendSimpleRsp(pObj->thandle, TSDB_MSG_TYPE_STATUS_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return 0;
  }
  pMsg = pStart;

  pRsp = (STaosRsp *)pMsg;
  pRsp->code = sdbMaster ? 0 : TSDB_CODE_REDIRECT;
  pMsg = pRsp->more;

  int size = sizeof(SIpList) + pSdbIpList->numOfIps * 4;
  memcpy(pMsg, pSdbIpList, size);
  pMsg += size;
  memcpy(pMsg, pSdbPublicIpList, size);
  pMsg += size;

  *pMsg = TSDB_IE_TYPE_DNODE_STATE;
  pMsg++;
  SDnodeState *pState = (SDnodeState *)pMsg;
  pState->moduleStatus = htonl(pObj->moduleStatus);
  pState->createdTime = htonl(pObj->createdTime / 1000);
  pMsg += sizeof(SDnodeState);

  if (ssize > 0) memcpy(pMsg, pVMsg, ssize);
  pMsg += ssize;

  msgLen = pMsg - pStart;
  taosSendMsgToPeer(pObj->thandle, pStart, msgLen);

  return msgLen;
}

void mgmtUpdateVgroupPublicIp(uint32_t privateIp, uint32_t oldPublicIp, uint32_t newPublicIp) {
  void *  pNode = NULL;
  SVgObj *pVgroup = NULL;
  while (1) {
    pNode = sdbFetchRow(vgSdb, pNode, (void **)&pVgroup);
    if (pVgroup == NULL) break;

    for (int i = 0; i < pVgroup->numOfVnodes; ++i) {
      SVnodeGid *vnodeGid = pVgroup->vnodeGid + i;
      if (vnodeGid->ip == privateIp) {
        mPrint("vgroup:%d, index:%d vnode:%d ip:%s change publicIp from %s to %s", pVgroup->vgId, i, vnodeGid->vnode,
               taosIpStr(privateIp), taosIpStr(vnodeGid->publicIp), taosIpStr(newPublicIp));
        vnodeGid->publicIp = newPublicIp;
        sdbUpdateRow(vgSdb, pVgroup, tsVgUpdateSize, 1);
      }
    }
  }
}

void mgmtUpdateMnodePublicIp(uint32_t privateIp, uint32_t oldPublicIp, uint32_t newPublicIp) {
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

int mgmtProcessDnodeStatus(unsigned char *pMsg, int msgLen, SDnodeObj *pObj) {
  SStatusMsg *pStatus = (SStatusMsg *)pMsg;
  char *      pVMsg = NULL;

  uint32_t version = htonl(pStatus->version);
  if (version != tsVersion) {
    mError("dnode:%s status msg version:%d not equal with master:%d", taosIpStr(pObj->privateIp), version, tsVersion);
    return 0;
  }

  if (!sdbMaster) {
    mError("dnode:%s status msg received, redirect the message", taosIpStr(pObj->privateIp));
    mgmtSendStatusRspMsg(pObj, NULL, 0);
    return 0;
  }

  uint32_t pubicIp = htonl(pStatus->publicIp);

  /*
   * When publicIp changes, update the publicIP of all vnodes
   */
  if (pObj->publicIp != pubicIp) {
    mPrint("dnode:%s, change publicIp from %s to %s", taosIpStr(pObj->privateIp), taosIpStr(pObj->publicIp), taosIpStr(pubicIp));
    mgmtUpdateVgroupPublicIp(pObj->privateIp, pObj->publicIp, pubicIp);
    pObj->publicIp = pubicIp;
    mgmtUpdateDnode(pObj);
    mgmtUpdateMnodePublicIp(pObj->privateIp, pObj->publicIp, pubicIp);
  }

  pObj->lastReboot = htonl(pStatus->lastReboot);
  pObj->numOfCores = htons(pStatus->numOfCores);
  pObj->alternativeRole = pStatus->alternativeRole;
  pObj->numOfTotalVnodes = htons(pStatus->numOfTotalVnodes);
  pObj->diskAvailable = pStatus->diskAvailable;
  pObj->openVnodes = htonl(pStatus->openVnodes);

  if (pObj->numOfVnodes == TSDB_INVALID_VNODE_NUM) {
    int oldVnodes = pObj->numOfVnodes;
    mgmtSetDnodeMaxVnodes(pObj);
    mPrint("dnode:%s, first access, set total vnodes from %d to %d", taosIpStr(pObj->privateIp), oldVnodes, pObj->numOfVnodes);
  }

  // wait vnode dropped
  for (int vnode = 0; vnode < pObj->numOfVnodes; ++vnode) {
    SVnodeLoad *pVload = &(pObj->vload[vnode]);
    if (pVload->dropStatus == TSDB_VN_DROP_STATUS_DROPPING) {
      bool existInDnode = false;
      for (int j = 0; j < pObj->openVnodes; ++j) {
        if (htonl(pStatus->load[j].vnode) == vnode) {
          existInDnode = true;
          break;
        }
      }

      if (!existInDnode) {
        pVload->dropStatus = TSDB_VN_DROP_STATUS_READY;
        pVload->status = TSDB_VN_STATUS_OFFLINE;
        mgmtUpdateDnode(pObj);
        mPrint("dnode:%s, vid:%d, drop finished", taosIpStr(pObj->privateIp), vnode);
        taosTmrStart(mgmtMonitorDbDrop, 10000, NULL, mgmtTmr);
      }
    } else if (pVload->vgId == 0) {
      /*
       * In some cases, vnode information may be reported abnormally, recover it
       */
      if (pVload->dropStatus != TSDB_VN_DROP_STATUS_READY || pVload->status != TSDB_VN_STATUS_OFFLINE) {
        mPrint("dnode:%s, vid:%d, vgroup:%d status:%s dropStatus:%s, set it to avail status",
                taosIpStr(pObj->privateIp), vnode, pVload->vgId, taosGetVnodeStatusStr(pVload->status),
                taosGetVnodeDropStatusStr(pVload->dropStatus));
        pVload->dropStatus = TSDB_VN_DROP_STATUS_READY;
        pVload->status = TSDB_VN_STATUS_OFFLINE;
        mgmtUpdateDnode(pObj);
      }
    }
  }

  // set vnode status
  pVMsg = (char *)malloc(sizeof(SVnodeAccess) * TSDB_MAX_VNODES);
  SVnodeAccess *pAccess = (SVnodeAccess *)pVMsg;
  for (int i = 0; i < pObj->openVnodes; ++i) {
    int vnode = htonl(pStatus->load[i].vnode);
    if (vnode < 0 || vnode >= pObj->numOfVnodes) {
      mError("dnode:%s vid:%d out of range(0, %d)", taosIpStr(pObj->privateIp), vnode, pObj->numOfVnodes - 1);
      continue;
    }

    SVnodeLoad *pVload = &(pObj->vload[vnode]);
    pVload->vnode = vnode;
    pVload->status = pStatus->load[i].status;
    pVload->syncStatus = pStatus->load[i].syncStatus;

    int64_t  totalStorage = htobe64(pStatus->load[i].totalStorage);
    int64_t  compStorage = htobe64(pStatus->load[i].compStorage);
    int64_t  pointsWritten = htobe64(pStatus->load[i].pointsWritten);
    uint32_t vgId = htonl(pStatus->load[i].vgId);

    SVgObj *pVgroup = mgmtGetVgroup(vgId);
    if (pVgroup == NULL) {
      mError("vgroup:%d is not there, but vnode %d on dnode 0x%x still exists, drop it!", vgId, vnode, pObj->privateIp);

      // drop vnode from dnode
      SVnodeGid vnodeGid = {pObj->privateIp, pObj->publicIp, vnode};
      mgmtSendOneFreeVnodeMsg(&vnodeGid);
      continue;
    }

    SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
    if (pDb == NULL) {
      mError("vgroup:%d not belongs to any database, vnode:%d dnode:0x%x", vgId, pStatus->load[i].vnode,
             pObj->privateIp);
      continue;
    }

    SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
    if (pAcct == NULL) {
      mError("db:%s not belongs to any account", pDb->name);
      continue;
    }

    // Check access status;
    char accessState = TSDB_VN_ALL_ACCCESS;
    if (pAcct->acctInfo.totalStorage > pAcct->cfg.maxStorage) {
      accessState &= (~TSDB_VN_WRITE_ACCCESS);
    }

    if (grantCheckStorage() != 0) {
      accessState &= (~TSDB_VN_WRITE_ACCCESS);
    }

    if (pAcct->acctInfo.queryTime > pAcct->cfg.maxQueryTime) {
      accessState &= (~TSDB_VN_READ_ACCCESS);
    }

    accessState &= pAcct->cfg.accessState;

    pAcct->acctInfo.accessState = accessState;

    // Check if accessState is changed
    if (pAcct->acctInfo.accessState != pStatus->load[i].accessState) {
      pAccess->vnode = htonl(vnode);
      pAccess->accessState = pAcct->acctInfo.accessState;
      pAccess++;
    }

    pVload->totalStorage = totalStorage > 0 ? totalStorage : 0;
    pVload->compStorage = compStorage > 0 ? compStorage : 0;
    pVload->pointsWritten = pointsWritten > 0 ? pointsWritten : 0;
    
    if (pVload->vgId == 0 || pVload->dropStatus == TSDB_VN_DROP_STATUS_DROPPING) {
      mPrint("dnode:%s, vid:%d, mgmt not exist, drop it", taosIpStr(pObj->privateIp), vnode);
      SVnodeGid pVnodeGid;
      pVnodeGid.ip = pObj->privateIp;
      pVnodeGid.vnode = vnode;
      mgmtSendOneFreeVnodeMsg(&pVnodeGid);
      memset(pVload, 0, sizeof(SVnodeLoad));

      // if dnode not receive drop-vnode-msg, set the vnode to dropping state
      pVload->dropStatus = TSDB_VN_DROP_STATUS_DROPPING;
    }
  }

  /*
   * In some unusual cases, the dnode is likely to have openVnodes 0.
   */
  //if (pObj->status != TSDB_DN_STATUS_READY && pObj->openVnodes == 0) {
  if (pObj->status != TSDB_DN_STATUS_READY) {
    mTrace("dnode:%s, from offline to online", taosIpStr(pObj->privateIp));
    mgmtStartBalanceTimer(200);
  }

  pObj->lastAccess = mgmtAccessSquence;
  pObj->status = TSDB_DN_STATUS_READY;
  mgmtSendStatusRspMsg(pObj, pVMsg, (((char *)pAccess) - pVMsg));

  tfree(pVMsg);

  return 0;
}

int mgmtProcessDnodeGrantMsg(unsigned char *pMsg, int msgLen, SDnodeObj *pObj) {
  grantUpdate(pMsg);

  int code = (sdbMaster == 1 ? 0 : TSDB_CODE_REDIRECT);

  taosSendSimpleRsp(pObj->thandle, TSDB_MSG_TYPE_GRANT_RSP, code);

  return 0;
}

SDnodeObj *mgmtProcessNewConnection(char *msg) {
  uint32_t   ip;
  SIntMsg *  pMsg = (SIntMsg *)msg;
  SDnodeObj *pObj;

  ip = inet_addr(pMsg->meterId);
  pObj = mgmtGetDnode(ip);

  if (pObj == NULL) {
    if (ip == tsPrivateIp4) {
      mgmtCreateDnode(ip);
      pObj = mgmtGetDnode(ip);
      if (pObj == NULL) {
        mTrace("no resource for connection from:%s", pMsg->meterId);
      } else {
        pObj->numOfVnodes = TSDB_INVALID_VNODE_NUM;
        pObj->numOfFreeVnodes = TSDB_INVALID_VNODE_NUM;
        pObj->moduleStatus |= 1 << TSDB_MOD_MGMT;
        mgmtUpdateDnode(pObj);
      }
    } else {
      mTrace("ip:%s not configured", pMsg->meterId);
    }
  } else {
    /*
        if ( numOfVnodes != pObj->numOfVnodes ) {
          mgmtDropDnode (pObj);
          pObj = mgmtCreateDnode(ip, numOfVnodes);
        }
    */
    if (pObj->thandle) {
      taosCloseRpcConn(pObj->thandle);
      __sync_fetch_and_sub(&mgmtDnodeConns, 1);
      __sync_fetch_and_sub(&sdbExtConns, 1);
      pObj->thandle = NULL;
      mTrace(
          "dnode:%s, connection is already there, close it first, "
          "connections:%d",
          pMsg->meterId, mgmtDnodeConns, pMsg->msgType);
    }
  }

  return pObj;
}

void *mgmtProcessMsgFromDnodeSpec(char *msg, void *ahandle, void *thandle) {
  SDnodeObj *pObj = (SDnodeObj *)ahandle;
  SIntMsg *  pMsg = (SIntMsg *)msg;

  if (msg == NULL) {
    if (pObj) {
      pObj->thandle = NULL;
      if (pObj->status != TSDB_DN_STATUS_OFFLINE) {
        pObj->status = TSDB_DN_STATUS_OFFLINE;
        __sync_fetch_and_sub(&mgmtDnodeConns, 1);
        __sync_fetch_and_sub(&sdbExtConns, 1);
      }
      mTrace("dnode:%s, connection is down, connections:%d", taosIpStr(pObj->privateIp), mgmtDnodeConns);
    }
    return NULL;
  }

  if (pObj == NULL) {
    pObj = mgmtProcessNewConnection(msg);
    if (pObj == NULL) {
      taosSendSimpleRsp(thandle, TSDB_MSG_TYPE_STATUS_RSP, TSDB_CODE_NO_RIGHTS);
      return NULL;
    }

    pObj->thandle = thandle;
    __sync_fetch_and_add(&mgmtDnodeConns, 1);
    __sync_fetch_and_add(&sdbExtConns, 1);
    mTrace("dnode:%s, connection is up, connections:%d, msgType:%d", pMsg->meterId, mgmtDnodeConns, pMsg->msgType);
  }

  // reset the timer
  if (mgmtGetDnode(pObj->privateIp) == NULL) {
    pObj->thandle = NULL;
    __sync_fetch_and_sub(&mgmtDnodeConns, 1);
    __sync_fetch_and_sub(&sdbExtConns, 1);
    mWarn("dnode:%s shall be dropped since not configured, connections:%d", pMsg->meterId, mgmtDnodeConns);
    return NULL;
  }

  if (pMsg->msgType == TSDB_MSG_TYPE_STATUS) {
    mgmtProcessDnodeStatus(pMsg->content, pMsg->msgLen - sizeof(SIntMsg), pObj);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_GRANT) {
    mgmtProcessDnodeGrantMsg(pMsg->content, pMsg->msgLen - sizeof(SIntMsg), pObj);
  } else {
    mgmtProcessMsgFromDnode((char*)pMsg->content, pMsg->msgLen - sizeof(SIntMsg), pMsg->msgType, pObj);
  }

  return pObj;
}