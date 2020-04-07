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
#include "tmodule.h"
#include "tstatus.h"
#include "tutil.h"
#include "mgmtDb.h"
#include "mgmtDnode.h"
#include "mgmtMnode.h"
#include "mgmtSdb.h"
#include "mgmtShell.h"
#include "mgmtVgroup.h"
#include "balance.h"
#include "balanceModule.h"
#include "dnodeMClient.h"

extern void *  tsVgroupSdb;
extern void *  tsDnodeSdb;
extern int32_t tsVgUpdateSize;
extern int32_t tsDnodeUpdateSize;

/*
 * once sdb work as mater, then balanceAccessSquence reset to zero
 * increase balanceAccessSquence every balance interval
 */
static uint32_t        balanceAccessSquence = 0;
static void *          tsBalanceTimer = NULL;
static void *          tsBalanceMonitorTimer = NULL;
static int32_t         tsBalanceDnodeListSize = 0;
static SDnodeObj **    tsBalanceDnodeList = NULL;
static int32_t         tsBalanceDnodesListMallocSize = 0;
static pthread_mutex_t tsBalanceMutex;

static void  balanceStartTimer(int64_t mseconds);
// static void  balanceMonitorVgroups();
// static void  balanceMonitorDnodes();
static void  balanceInitDnodeList();
static void  balanceMakeDnodeList();
static void  balanceReleaseDnodeList();
static void  balanceCalcSystemScore();
// static float balanceTryCalcDnodeScore(SDnodeObj *pDnode, int32_t extraVnode);
static bool  balanceCheckDnodeInRemoveState(SDnodeObj *pDnode);
static bool  balanceCheckDnodeInOfflineState(SDnodeObj *pDnode);

static int32_t balanceGetScoresMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t balanceRetrieveScores(SShowObj *pShow, char *data, int32_t rows, void *pConn);

static void balanceLock() {
  pthread_mutex_lock(&tsBalanceMutex);
}

static void balanceUnLock() {
  pthread_mutex_unlock(&tsBalanceMutex);
}

static void balanceUpdateDnodeState(SDnodeObj *pDnode, int32_t lbStatus) {
  SSdbOperDesc oper = {
    .type = SDB_OPER_TYPE_GLOBAL,
    .table = tsDnodeSdb,
    .pObj = pDnode,
    .rowSize = tsDnodeUpdateSize
  };

  sdbUpdateRow(&oper);
}

UNUSED_FUNC
static void balanceUpdateVgroupState(SVgObj *pVgroup, int32_t lbStatus, int32_t srcIp) {
  pVgroup->lbTime = taosGetTimestampSec();
  pVgroup->lbStatus = lbStatus;
  pVgroup->lbIp = srcIp;

  SSdbOperDesc oper = {
    .type = SDB_OPER_TYPE_GLOBAL,
    .table = tsVgroupSdb,
    .pObj = pVgroup,
    .rowSize = tsVgUpdateSize
  };

  sdbUpdateRow(&oper);
}

/**
 * check if can alloc a vnode from this dnode
 **/
static bool balanceCheckDnodeFree(SDnodeObj *pDnode) {
  mTrace("dnode:%d, try alloc vnode, status:%s lbstatus:%s openVnodes:%d totalVnodes", pDnode->dnodeId,
         taosGetDnodeStatusStr(pDnode->status), taosGetDnodeLbStatusStr(pDnode->lbStatus), pDnode->openVnodes,
         pDnode->numOfTotalVnodes);

  if (balanceCheckDnodeInRemoveState(pDnode)) {
    return false;
  }

  if (balanceCheckDnodeInOfflineState(pDnode)) {
    return false;
  }

  if (pDnode->numOfTotalVnodes <= pDnode->openVnodes) {
    return false;
  }

  if (pDnode->diskAvailable <= tsMinimalDataDirGB) {
    mError("dnode:%d, no disk space to alloc vnode, available:%fGB", pDnode->dnodeId, pDnode->diskAvailable);
    return false;
  }

  return true;
}

/**
 * check if can balance a vnode into this dnode
 **/
UNUSED_FUNC
static bool balanceCheckDnodeCanBalanceIn(SDnodeObj *pDnode) {
  if (pDnode->lbStatus != TSDB_DN_LB_STATUS_BALANCED) {
    return false;
  }

  if (balanceCheckDnodeInOfflineState(pDnode)) {
    return false;
  }

  if (pDnode->numOfTotalVnodes <= pDnode->openVnodes) {
    return false;
  }

  return true;
}

/**
 * check if can balance a vnode out of this dnode
 **/
UNUSED_FUNC
static bool balanceCheckDnodeCanBalanceOut(SDnodeObj *pDnode) {
  if (pDnode->lbStatus != TSDB_DN_LB_STATUS_BALANCED) {
    return false;
  }

  if (balanceCheckDnodeInOfflineState(pDnode)) {
    return false;
  }

  if (pDnode->openVnodes <= 0) {
    return false;
  }

  return true;
}

// UNUSED_FUNC
// static bool balanceCheckVgroupHaveRemovingDnode(SVgObj *pVgroup) {
//   for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
//     SVnodeGid *pVnodeGid = pVgroup->vnodeGid + i;
//     SDnodeObj *pDnode = mgmtGetDnode(pVnodeGid->dnodeId);
//     if (pDnode != NULL && balanceCheckDnodeInRemoveState(pDnode)) {
//       return false;
//     }
//   }

//   return true;
// }

// /**
//  * for each vnode in this vgroup
//  * if vnode.ip equal to pDnode.privateIp, return true
//  * otherwist false
//  **/
// static bool balanceCheckDnodeInVgroup(SDnodeObj *pDnode, SVgObj *pVgroup) {
//   for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
//     SVnodeGid *pVnodeGid = pVgroup->vnodeGid + i;
//     if (pVnodeGid->dnodeId == pDnode->dnodeId) {
//       return true;
//     }
//   }

//   return false;
// }


// /**
//  * remove one vnode from the vgroup
//  **/
// static void balanceDiscardVnode(SVgObj *pVgroup, SVnodeGid *pVnodeGid) {
//   mTrace("dnode:%s, vgroup:%d, vnode:%d is dropping", taosIpStr(pVnodeGid->ip), pVgroup->vgId, pVnodeGid->vnode);

//   SVnodeGid pBackupVnodeGid = *pVnodeGid;

//   SVnodeGid vnodeGid[TSDB_VNODES_SUPPORT] = {0};
//   int32_t       numOfVnodes = 0;
//   for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
//     SVnodeGid *pTmpVodeGid = pVgroup->vnodeGid + i;
//     if (pTmpVodeGid == pVnodeGid) {
//       continue;
//     }
//     vnodeGid[numOfVnodes] = *pTmpVodeGid;
//     ++numOfVnodes;
//   }
//   memcpy(pVgroup->vnodeGid, vnodeGid, TSDB_VNODES_SUPPORT * sizeof(SVnodeGid));
//   pVgroup->numOfVnodes = numOfVnodes;

//   SDnodeObj *pDnode = mgmtGetDnode(pBackupVnodeGid.ip);
//   if (pDnode) {
//     SVnodeLoad *pVload = pDnode->vload + pBackupVnodeGid.vnode;
//     memset(pVload, 0, sizeof(SVnodeLoad));
//     mgmtCalcNumOfFreeVnodes(pDnode);
//     mgmtUpdateDnode(pDnode);
//   } else {
//     mError("dnode:%s, not in dnode DB!!!", taosIpStr(pBackupVnodeGid.ip));
//   }

//   for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
//     mTrace("dnode:%s, vgroup:%d, vnode:%d exist after drop", taosIpStr(pVgroup->vnodeGid[i].ip), pVgroup->vgId, pVgroup->vnodeGid[i].vnode);
//   }

//   sdbUpdateRow(tsVgroupSdb, pVgroup, tsVgUpdateSize, 1);

//   SRpcIpSet ipSet = mgmtGetIpSetFromIp(pBackupVnodeGid.ip);
//   mgmtSendDropVnodeMsg(pBackupVnodeGid.vnode, &ipSet, NULL);

//   mgmtSendCreateVgroupMsg(pVgroup, NULL);
// }

// /**
//  * add one vnode to the vgroup
//  **/
// static void balanceAppendVnode(SVgObj *pVgroup, SVnodeGid *pVnodeGid) {
//   mTrace("dnode:%s, vgroup:%d, vnode:%d is adding", taosIpStr(pVnodeGid->ip), pVgroup->vgId, pVnodeGid->vnode);

//   if (pVgroup->numOfVnodes < TSDB_VNODES_SUPPORT) {
//     pVgroup->vnodeGid[pVgroup->numOfVnodes] = *pVnodeGid;
//     pVgroup->numOfVnodes++;
//   }

//   SDnodeObj *pDnode = mgmtGetDnode(pVnodeGid->ip);
//   if (pDnode) {
//     SVnodeLoad *pVload = pDnode->vload + pVnodeGid->vnode;
//     memset(pVload, 0, sizeof(SVnodeLoad));
//     pVload->vnode = pVnodeGid->vnode;
//     pVload->vgId = pVgroup->vgId;
//     mgmtCalcNumOfFreeVnodes(pDnode);
//     mgmtUpdateDnode(pDnode);
//   } else {
//     mError("dnode:%s, not in dnode DB!!!", taosIpStr(pVnodeGid->ip));
//   }

//   sdbUpdateRow(tsVgroupSdb, pVgroup, tsVgUpdateSize, 1);

//   for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
//     mTrace("%d-dnode:%s, vgroup:%d, vnode:%d exist after addition", i, taosIpStr(pVgroup->vnodeGid[i].ip), pVgroup->vgId, pVgroup->vnodeGid[i].vnode);
//   }

//   mgmtSendCreateVgroupMsg(pVgroup, NULL);
// }

static void balanceSwapVnodeGid(SVnodeGid *pVnodeGid1, SVnodeGid *pVnodeGid2) {
  SVnodeGid tmp = *pVnodeGid1;
  *pVnodeGid1 = *pVnodeGid2;
  *pVnodeGid2 = tmp;
}

/**
 * while create a new vgroup, we should fill the vgroup
 * 1. the numOfVnodes is equal to the db replica
 * 2. find dnodes use balance score
 * 2.1 if not offline, create a vnode in the dnode
 * 2.2 then add the vnode to the vgroup
 * 3 if filledVnodes euqal to numOfVnodes, success
 * return
 * 0 - success
 * other - failure
 **/
int32_t balanceAllocVnodes(SVgObj *pVgroup) {
  int32_t dnode = 0;
  int32_t vnodes = 0;

  balanceLock();

  balanceMakeDnodeList();

  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    for (; dnode < tsBalanceDnodeListSize; ++dnode) {
      SDnodeObj *pDnode = tsBalanceDnodeList[dnode];
      if (balanceCheckDnodeFree(pDnode)) {
        SVnodeGid *pVnodeGid = pVgroup->vnodeGid + i;
        pVnodeGid->dnodeId = pDnode->dnodeId;
        pVnodeGid->privateIp = pDnode->privateIp;
        pVnodeGid->publicIp = pDnode->publicIp;

        dnode++;
        vnodes++;
        break;
      }
    }
  }

  if (vnodes != pVgroup->numOfVnodes) {
    mTrace("vgroup:%d, db:%s need vnodes:%d, but alloc:%d, free them", pVgroup->vgId, pVgroup->dbName,
           pVgroup->numOfVnodes, vnodes);
    balanceUnLock();
    return -1;
  }

  /*
   * make the choice more random.
   * replica 1: no choice
   * replica 2: there are 2 combinations
   * replica 3 or larger: there are 6 combinations
   */
  if (pVgroup->numOfVnodes == 1) {
  } else if (pVgroup->numOfVnodes == 2) {
    //if (rand() % 2 == 0) {
      balanceSwapVnodeGid(pVgroup->vnodeGid, pVgroup->vnodeGid + 1);
    //}
  } else {
    int32_t randVal = rand() % 6;
    if (randVal == 1) {  // 1, 0, 2
      balanceSwapVnodeGid(pVgroup->vnodeGid + 0, pVgroup->vnodeGid + 1);
    } else if (randVal == 2) {  // 1, 2, 0
      balanceSwapVnodeGid(pVgroup->vnodeGid + 0, pVgroup->vnodeGid + 1);
      balanceSwapVnodeGid(pVgroup->vnodeGid + 1, pVgroup->vnodeGid + 2);
    } else if (randVal == 3) {  // 2, 1, 0
      balanceSwapVnodeGid(pVgroup->vnodeGid + 0, pVgroup->vnodeGid + 2);
    } else if (randVal == 4) {  // 2, 0, 1
      balanceSwapVnodeGid(pVgroup->vnodeGid + 0, pVgroup->vnodeGid + 2);
      balanceSwapVnodeGid(pVgroup->vnodeGid + 1, pVgroup->vnodeGid + 2);
    }
    if (randVal == 5) {  // 0, 2, 1
      balanceSwapVnodeGid(pVgroup->vnodeGid + 1, pVgroup->vnodeGid + 2);
    } else {
    }  // 0, 1, 2
  }

  balanceUnLock();
  return 0;
}

// /**
//  * desc: check vnode is ready (synced)
//  **/
// static bool balanceCheckVnodeReady(SDnodeObj *pDnode, SVgObj *pVgroup, SVnodeGid *pVnode) {
//   if (pDnode == NULL) {
//     pDnode = mgmtGetDnode(pVnode->ip);
//     if (pDnode == NULL) {
//       mError("dnode:%s, vgroup:%d, vnode:%d dnode not exist", taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode);
//       return false;
//     }
//   }

//   if (balanceCheckDnodeInOfflineState(pDnode)) {
//     mTrace("dnode:%s, vgroup:%d, vnode:%d dnode is offline", taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode);
//     return false;
//   }

//   SVnodeLoad *vload = pDnode->vload + pVnode->vnode;
//   if (vload->vgId != pVgroup->vgId || vload->vnode != pVnode->vnode) {
//     mError("dnode:%s, vgroup:%d, vnode:%d not same with dnode vgroup:%d vnode:%d",
//             taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode, vload->vgId, vload->vnode);
//     return false;
//   }

//   mTrace("dnode:%s, vgroup:%d, vnode:%d, status:%s, syncstatus:%s",
//           taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode,
//           taosGetVnodeStatusStr(vload->status), taosGetVnodeSyncStatusStr(vload->syncStatus));
//   return vload->status == TSDB_VN_STATUS_SLAVE || vload->status == TSDB_VN_STATUS_MASTER;
// }

// /**
//  * desc: remove one vnode from vgroup
//  * all vnodes in vgroup should in ready state, except the balancing one
//  **/
// static void balanceRemoveOneRedundantVnode(SVgObj *pVgroup) {
//   if (pVgroup->numOfVnodes <= 1) return;

//   SVnodeGid *pRmVnode = NULL;
//   SVnodeGid *pSelVnode = NULL;
//   int32_t    maxScore = 0;
//   bool       allReady = false;

//   for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
//     SVnodeGid *pVnode = &(pVgroup->vnodeGid[i]);
//     SDnodeObj *pDnode = mgmtGetDnode(pVnode->ip);

//     if (pDnode == NULL) {
//       mError("dnode:%s, vgroup:%d, vnode:%d dnode not exist, remove it from vgroup",
//               taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode);
//       pRmVnode = pVnode;
//       allReady = true;
//       break;
//     }

//     if (pDnode->lbStatus == TSDB_DN_LB_STATE_SHELL_REMOVING) {
//       mTrace("dnode:%s, vgroup:%d, vnode:%d, dnode in shell removing state",
//               taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode);
//       pRmVnode = pVnode;
//       if (balanceCheckDnodeInOfflineState(pDnode) && (balanceAccessSquence - pDnode->lastAccess) > 5 * tsStatusInterval) {
//         mTrace("dnode:%s, vgroup:%d, vnode:%d, dnode offline:%d seconds, remove it from vgroup",
//                 taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode, balanceAccessSquence - pDnode->lastAccess);
//         allReady = true;
//       }
//       break;
//     }

//     if (pVnode->ip == pVgroup->lbIp) {
//       mTrace("dnode:%s, vgroup:%d, vnode:%d is updating", taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode);
//       pRmVnode = pVnode;
//       continue;
//     }
//   }

//   if (pRmVnode != NULL && allReady) {
//     mTrace("vgroup:%d is ready", pVgroup->vgId);
//     balanceDiscardVnode(pVgroup, pRmVnode);
//     balanceStartTimer(1000);
//     return;
//   }

//   allReady = true;
//   for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
//     SVnodeGid *pVnode = &(pVgroup->vnodeGid[i]);
//     SDnodeObj *pDnode = mgmtGetDnode(pVnode->ip);
//     if (pDnode == NULL) continue;

//     if (pVnode == pRmVnode) {
//       continue;
//     }

//     if (!balanceCheckVnodeReady(pDnode, pVgroup, pVnode)) {
//       allReady = false;
//       break;
//     }

//     if (pSelVnode == NULL) {
//       pSelVnode = pVnode;
//       maxScore = pDnode->lbScore;
//     } else {
//       if (maxScore < pDnode->lbScore) {
//         pSelVnode = pVnode;
//         maxScore = pDnode->lbScore;
//       }
//     }
//   }

//   if (!allReady) {
//     mTrace("vgroup:%d is not ready", pVgroup->vgId);
//   } else {
//     mTrace("vgroup:%d is ready", pVgroup->vgId);
//     if (pRmVnode != NULL) {
//       pSelVnode = pRmVnode;
//     }
//     if (pSelVnode != NULL) {
//       balanceDiscardVnode(pVgroup, pSelVnode);
//       balanceStartTimer(1001);
//     }
//   }
// }

// /**
//  * desc: add vnode to vgroup, find a new one if dest dnode is null
//  **/
// static bool balanceAddVnode(SVgObj *pVgroup, SDnodeObj *pSrcDnode, SDnodeObj *pDestDnode) {
//   if (pDestDnode == NULL) {
//     for (int32_t i = 0; i < tsBalanceDnodeListSize; ++i) {
//       SDnodeObj *pDnode = tsBalanceDnodeList[i];

//       if (pDnode == pSrcDnode) {
//         continue;
//       }

//       if (!balanceCheckDnodeFree(pDnode)) {
//         continue;
//       }

//       if (balanceCheckDnodeInVgroup(pDnode, pVgroup)) {
//         continue;
//       }

//       pDestDnode = pDnode;
//       mTrace("vgroup:%d, add vnode to dnode:%s", pVgroup->vgId, taosIpStr(pDnode->privateIp));
//       break;
//     }
//   }

//   if (pDestDnode == NULL) {
//     return false;
//   }

//   SVnodeGid pVnodeGid;
//   if (!balanceAllocVnode(pVgroup, &pVnodeGid, pDestDnode)) {
//     return false;
//   }

//   uint32_t srcIp = (pSrcDnode == NULL ? 0 : pSrcDnode->privateIp);
//   balanceUpdateVgroupState(pVgroup, TSDB_VG_LB_STATUS_UPDATE, srcIp);
//   balanceAppendVnode(pVgroup, &pVnodeGid);
//   balanceStartTimer(1002);

//   return true;
// }

// static void balanceMonitorDnodeBalanced(int32_t mseconds) {
//   if (mseconds == 0) {
//     mTrace("balance function is scheduled by schedule, dnodes:%d", tsBalanceDnodeListSize);
//   } else {
//     mTrace("balance function is scheduled by event for %d mseconds arrived, dnodes:%d", mseconds, tsBalanceDnodeListSize);
//   }

//   if (tsBalanceDnodeListSize < 2) {
//     mTrace("dnodes:%d not enough, stop balance", tsBalanceDnodeListSize);
//     return;
//   }

//   for (int32_t src = tsBalanceDnodeListSize - 1; src >= 0; --src) {
//     SDnodeObj *pDnode = tsBalanceDnodeList[src];
//     mTrace("%d-dnode:%s, state:%s, lbstatus:%s, lbScore:%.1f, totalVnodes:%d, freeVnodes:%d, openVnodes:%d",
//             tsBalanceDnodeListSize - src - 1, taosIpStr(pDnode->privateIp), taosGetDnodeStatusStr(pDnode->status),
//             taosGetDnodeLbStatusStr(pDnode->lbStatus),
//             pDnode->lbScore, pDnode->numOfVnodes, pDnode->numOfFreeVnodes, pDnode->openVnodes
//     );
//   }

//   if ((tsBalanceDnodeList[tsBalanceDnodeListSize - 1]->lbScore - tsBalanceDnodeList[0]->lbScore) < 2) {
//     mTrace("all dnodes:%d is already balanced", tsBalanceDnodeListSize);
//     return;
//   }

//   for (int32_t src = tsBalanceDnodeListSize - 1; src > 0; --src) {
//     SDnodeObj *pSrcDnode = tsBalanceDnodeList[src];
//     if (!balanceCheckDnodeCanBalanceOut(pSrcDnode)) {
//       continue;
//     }

//     float srcScore = balanceTryCalcDnodeScore(pSrcDnode, -1);

//     for (int32_t i = 0; i < pSrcDnode->numOfVnodes; ++i) {
//       SVnodeLoad *pVload = pSrcDnode->vload + i;
//       if (pVload->vgId == 0) continue;

//       SVgObj *pVgroup = mgmtGetVgroup(pVload->vgId);
//       if (pVgroup == NULL) continue;
//       if (pVgroup->lbStatus != TSDB_VG_LB_STATUS_READY) continue;

//       for (int32_t dest = 0; dest < src; dest++) {
//         SDnodeObj *pDestDnode = tsBalanceDnodeList[dest];
//         if (!balanceCheckDnodeCanBalanceIn(pDestDnode)) {
//           continue;
//         }

//         float destScore = balanceTryCalcDnodeScore(pDestDnode, 1);
//         if (srcScore + 0.0001 < destScore) {
//           continue;
//         }

//         if (balanceCheckDnodeInVgroup(pDestDnode, pVgroup)) {
//           continue;
//         }

//         // if (pVgroup->numOfVnodes > 1 &&
//         // balanceCheckVgroupHaveRemovingDnode(pVgroup)) {
//         //  continue;
//         //}

//         mTrace("dnode:%s, vgroup:%d begin balancing to dnode:%s, srcScore:%.1f:%.1f, destScore:%.1f:%.1f",
//             taosIpStr(pSrcDnode->privateIp), pVgroup->vgId, taosIpStr(pDestDnode->privateIp),
//             pSrcDnode->lbScore, srcScore, pDestDnode->lbScore, destScore);
//         if (balanceAddVnode(pVgroup, pSrcDnode, pDestDnode)) {
//           balanceUpdateDnodeState(pSrcDnode, TSDB_DN_LB_STATUS_BALANCING);
//           return;
//         }
//       }
//     }
//   }
// }

// // if mgmt changed to master
// // 1. reset balanceAccessSquence to zero
// // 2. reset state of dnodes to offline
// // 3. reset lastAccess of dnodes to zero
// UNUSED_FUNC
// static void balanceSetDnodeOfflineOnSdbChanged() {
//   mPrint("work as master, set sequence:%d to 0", balanceAccessSquence);

//   void *     pNode = NULL;
//   SDnodeObj *pDnode = NULL;
//   while (1) {
//     pNode = sdbFetchRow(tsDnodeSdb, pNode, (void **)&pDnode);
//     if (pDnode == NULL) break;

//     mPrint("dnode:%s set access:%d to 0", taosIpStr(pDnode->privateIp), pDnode->lastAccess);
//     pDnode->lastAccess = 0;
//     pDnode->status = TSDB_DN_STATUS_OFFLINE;  // while master change, should reset dnode to offline
//   }

//   balanceAccessSquence = 0;
// }

static void balanceStart(int64_t mseconds) {
  if (!mgmtIsMaster()) return;

  // static uint32_t lastTime = 0;

  balanceLock();
  balanceMakeDnodeList();
  // balanceMonitorDnodes();
  // balanceMonitorVgroups();
  // if (mseconds != 0 || (taosGetTimestampSec() - lastTime) > tsBalanceStartInterval) {
  //   balanceMonitorDnodeBalanced(mseconds);
  //   lastTime = taosGetTimestampSec();
  // }
  balanceUnLock();

  balanceCalcSystemScore();
}

static void balanceProcessBalanceTimer(void *handle, void *tmrId) {
  if (handle == NULL) {
    balanceAccessSquence += tsBalanceMonitorInterval;
  }

  tsBalanceTimer = NULL;
  balanceStart((int64_t)handle);
  if (tsBalanceTimer == NULL) {
    taosTmrReset(balanceProcessBalanceTimer, tsBalanceMonitorInterval * 1000, NULL, tsMgmtTmr, &tsBalanceTimer);
  }
}

static void balanceStartTimer(int64_t mseconds) {
  mTrace("balance function will be called after %d mseconds", mseconds);
  taosTmrReset((TAOS_TMR_CALLBACK)balanceProcessBalanceTimer, mseconds, (void *)mseconds, tsMgmtTmr, &tsBalanceTimer);
}

void balanceNotify() { 
  balanceStartTimer(200); 
}

// static void balanceMonitorVgroups() {
//   void *  pNode = NULL;
//   SVgObj *pVgroup = NULL;
//   SDbObj *pDb = NULL;
//   int64_t curTime = time(NULL);

//   while (1) {
//     pNode = sdbFetchRow(tsVgroupSdb, pNode, (void **)&pVgroup);
//     if (pVgroup == NULL) break;
//     if (pVgroup->lbStatus == TSDB_VG_LB_STATUS_READY) continue;
//     if (pVgroup->lbTime + 5 * tsStatusInterval >= curTime) continue;

//     pDb = mgmtGetDb(pVgroup->dbName);
//     if (pDb == NULL) {
//       mError("vgroup:%d, db:%s is not exist", pVgroup->vgId, pVgroup->dbName);
//       continue;
//     }

//     int32_t dbReplica = pDb->cfg.replications;
//     int32_t vgReplica = pVgroup->numOfVnodes;
//     mTrace("vgroup:%d, db:%s is updating, replica:%d lbIp:%s, db replica:%d", pVgroup->vgId, pVgroup->dbName,
//            vgReplica, taosIpStr(pVgroup->lbIp), dbReplica);

//     if (vgReplica > dbReplica) {
//       balanceRemoveOneRedundantVnode(pVgroup);
//     } else if (vgReplica == dbReplica) {
//       mTrace("vgroup:%d, db:%s update success", pVgroup->vgId, pVgroup->dbName);
//       balanceUpdateVgroupState(pVgroup, TSDB_VG_LB_STATUS_READY, 0);
//       balanceStartTimer(1003);
//     } else {
//       balanceAddVnode(pVgroup, NULL, NULL);
//     }
//   }
// }

// /**
//  * if one dnode offline larger than OFFLINE_INTERVAL, remove it
//  **/
// static void balanceMontiorDnodeOffline(SDnodeObj *pDnode) {
//   if (!balanceCheckDnodeInOfflineState(pDnode)) return;
//   if (balanceCheckDnodeInRemoveState(pDnode)) return;
//   if (pDnode->lastAccess + tsOfflineThreshold > balanceAccessSquence) return;
//   if (pDnode->privateIp == dnodeGetMnodeMasteIp()) return;
//   if (mgmtGetDnodesNum() <= 1) return;

//   mLPrint("dnode:%d set to removing state for it offline:%d seconds", pDnode->dnodeId,
//           balanceAccessSquence - pDnode->lastAccess);

//   balanceUpdateDnodeState(pDnode, TSDB_DN_LB_STATUS_OFFLINE_REMOVING);
//   balanceStartTimer(1004);
// }

// static void balanceMonitorDnodeBalancing(SDnodeObj *pDnode) {
//   mTrace("dnode:%d, in balancing state", pDnode->dnodeId);

//   int32_t numOfUpdateVgroups = 0;
//   for (int32_t i = 0; i < pDnode->numOfVnodes; ++i) {
//     SVnodeLoad *pVload = pDnode->vload + i;
//     if (pVload->vgId == 0) continue;

//     SVgObj *pVgroup = mgmtGetVgroup(pVload->vgId);
//     if (pVgroup == NULL) continue;
//     mgmtDecVgroupRef(pVgroup);

//     if (pVgroup->lbStatus == TSDB_VG_LB_STATUS_READY) continue;
//     if (pVgroup->lbIp != pDnode->privateIp) continue;

//     numOfUpdateVgroups++;
//     mTrace("dnode:%s, vgroup:%d is updating", taosIpStr(pDnode->privateIp), pVgroup->vgId);
//     break;
//   }

//   if (numOfUpdateVgroups == 0) {
//     mPrint("dnode:%s, set to balanced state", taosIpStr(pDnode->privateIp));
//     balanceUpdateDnodeState(pDnode, TSDB_DN_LB_STATUS_BALANCED);
//     balanceStartTimer(1005);
//   }
// }

// static void balanceMonitorDnodeRemoving(SDnodeObj *pDnode) {
//   mTrace("dnode:%s, in removing state", taosIpStr(pDnode->privateIp));

//   for (int32_t i = 0; i < pDnode->numOfVnodes; ++i) {
//     SVnodeLoad *pVload = pDnode->vload + i;
//     if (pVload->vgId == 0) continue;

//     SVgObj *pVgroup = mgmtGetVgroup(pVload->vgId);
//     if (pVgroup == NULL) continue;
//     mgmtDecVgroupRef(pVgroup);

//     SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
//     if (pDb == NULL) {
//       mError("dnode:%s, vgroup:%d db:%s not exist", taosIpStr(pDnode->privateIp), pVgroup->vgId, pVgroup->dbName);
//       continue;
//     }
//     mgmtDecDbRef(pDb);

//     if (pDb->cfg.replications >= pVgroup->numOfVnodes) {
//       if (!balanceAddVnode(pVgroup, pDnode, NULL)) {
//         mError("dnode:%s, vgroup:%d no enough dnode for remove operation", taosIpStr(pDnode->privateIp), pVgroup->vgId);
//       } else {
//         mTrace("dnode:%s, vgroup:%d set to updating state", taosIpStr(pDnode->privateIp), pVgroup->vgId);
//       }
//     } else {
//       if (pVgroup->lbIp != pDnode->privateIp) {
//         mTrace("dnode:%s, vgroup:%d set to updating state, change lbIp:%s to %s",
//             taosIpStr(pDnode->privateIp), pVgroup->vgId, taosIpStr(pVgroup->lbIp), taosIpStr(pDnode->privateIp));
//         balanceUpdateVgroupState(pVgroup, TSDB_VG_LB_STATUS_UPDATE, pDnode->privateIp);
//       } else {
//         mTrace("dnode:%s, vgroup:%d wait update over", taosIpStr(pDnode->privateIp), pVgroup->vgId);
//       }
//     }

//     if (pVgroup->lbStatus == TSDB_VG_LB_STATUS_UPDATE) {
//       break;
//     }
//   }

//   if (pDnode->numOfVnodes == pDnode->numOfFreeVnodes) {
//     mPrint("dnode:%s, dropped for all vnodes are moving to other dnodes", taosIpStr(pDnode->privateIp));
//     mgmtDropDnode(pDnode);
//     balanceStartTimer(1005);
//   }
// }

// static void balanceMonitorDnodes() {
//   void *     pNode = NULL;
//   SDnodeObj *pDnode = NULL;
//   bool       hasRemovingDnode = false;

//   while (1) {
//     mgmtDecDnodeRef(pDnode);
//     pNode = sdbFetchRow(tsDnodeSdb, pNode, (void **)&pDnode);
//     if (pDnode == NULL) break;

//     balanceMontiorDnodeOffline(pDnode);

//     switch (pDnode->lbStatus) {
//       case TSDB_DN_LB_STATUS_BALANCED:
//         break;
//       case TSDB_DN_LB_STATUS_BALANCING:
//         balanceMonitorDnodeBalancing(pDnode);
//         break;
//       case TSDB_DN_LB_STATUS_OFFLINE_REMOVING:
//       case TSDB_DN_LB_STATE_SHELL_REMOVING:
//         if (hasRemovingDnode) break;
//         hasRemovingDnode = true;
//         balanceMonitorDnodeRemoving(pDnode);
//         break;
//       default:
//         balanceUpdateDnodeState(pDnode, TSDB_DN_LB_STATUS_BALANCED);
//         break;
//     }
//   }
//   mgmtDecDnodeRef(pDnode);
// }

/**
 * should be called at system init function
 **/
int32_t balanceInit() {
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_SCORES, balanceGetScoresMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_SCORES, balanceRetrieveScores);
  
  if (tsBalanceTimer == NULL && tsBalanceMonitorTimer == NULL) {
    pthread_mutex_init(&tsBalanceMutex, NULL);
    balanceInitDnodeList();
    balanceStartTimer(3000);
    mTrace("balance start fp:%p initialized", balanceProcessBalanceTimer);
  }

  return 0;
}

/**
 * should be called at system release function
 **/
void balanceCleanUp() {
  if (tsBalanceTimer != NULL) {
    taosTmrStopA(&tsBalanceTimer);
    pthread_mutex_destroy(&tsBalanceMutex);
    balanceReleaseDnodeList();
    mTrace("stop balance timer");
  }
}

void balanceSetDnodeUnRemoveState(SDnodeObj *pDnode) {
  mPrint("dnode:%d, set to unremove state", pDnode->dnodeId);
  if (balanceCheckDnodeInRemoveState(pDnode)) {
    balanceUpdateDnodeState(pDnode, TSDB_DN_LB_STATUS_BALANCED);
    balanceStartTimer(11);
  }
}

// int32_t balanceSetDnodeRemoveState(SDnodeObj *pDnode) {
//   int32_t numOfVnodes = pDnode->numOfVnodes - pDnode->numOfFreeVnodes;
//   int32_t numOfTotalFreeVnodes = 0;

//   void *pNode = NULL;
//   SDnodeObj *pTempDnode = NULL;
//   while (1) {
//     mgmtDecDnodeRef(pDnode);
//     pNode = sdbFetchRow(tsDnodeSdb, pNode, (void **) &pTempDnode);
//     if (pTempDnode == NULL) break;
//     if (pTempDnode == pDnode) continue;

//     switch (pTempDnode->lbStatus) {
//       case TSDB_DN_LB_STATUS_OFFLINE_REMOVING:
//       case TSDB_DN_LB_STATE_SHELL_REMOVING:
//         break;
//       default:
//         numOfTotalFreeVnodes += pTempDnode->numOfFreeVnodes;
//     }
//     mgmtDecDnodeRef(pDnode);
//   }

//   if (numOfVnodes > numOfTotalFreeVnodes) {
//     mError("dnode:%s, numOfVnodes:%d, no enough dnode for remove dnode operation, numOfTotalFreeVnodes:%d",
//            taosIpStr(pDnode->privateIp), numOfVnodes, numOfTotalFreeVnodes);
//     return TSDB_CODE_NO_ENOUGH_DNODES;
//   }

//   balanceUpdateDnodeState(pDnode, TSDB_DN_LB_STATE_SHELL_REMOVING);
//   mPrint("dnode:%s, set to shell removing state", taosIpStr(pDnode->privateIp));

//   balanceStartTimer(12);

//   return 0;
// }

bool balanceCheckDnodeInRemoveState(SDnodeObj *pDnode) {
  return pDnode->lbStatus == TSDB_DN_LB_STATUS_OFFLINE_REMOVING || pDnode->lbStatus == TSDB_DN_LB_STATE_SHELL_REMOVING;
}

bool balanceCheckDnodeInOfflineState(SDnodeObj *pDnode) {
  return pDnode->status == TSDB_DN_STATUS_OFFLINE;
}

static int32_t balanceCalcCpuScore(SDnodeObj *pDnode) {
  if (pDnode->cpuAvgUsage < 80)
    return 0;
  else if (pDnode->cpuAvgUsage < 90)
    return 10;
  else
    return 50;
}

static int32_t balanceCalcMemoryScore(SDnodeObj *pDnode) {
  if (pDnode->memoryAvgUsage < 80)
    return 0;
  else if (pDnode->memoryAvgUsage < 90)
    return 10;
  else
    return 50;
}

static int32_t balanceCalcDiskScore(SDnodeObj *pDnode) {
  if (pDnode->diskAvgUsage < 80)
    return 0;
  else if (pDnode->diskAvgUsage < 90)
    return 10;
  else
    return 50;
}

static int32_t balanceCalcBandwidthScore(SDnodeObj *pDnode) {
  if (pDnode->bandwidthUsage < 30)
    return 0;
  else if (pDnode->bandwidthUsage < 80)
    return 10;
  else
    return 50;
}

static float balanceCalcModuleScore(SDnodeObj *pDnode) {
  if (mgmtCheckModuleInDnode(pDnode, TSDB_MOD_MGMT)) {
    return (float)tsModule[TSDB_MOD_MGMT].equalVnodeNum / pDnode->numOfTotalVnodes * 100;
  }
  return 0;
}

static float balanceCalcVnodeScore(SDnodeObj *pDnode, int32_t extra) {
  if (pDnode->numOfTotalVnodes <= 1) return 0;
  return (float)(pDnode->openVnodes + extra) / pDnode->numOfTotalVnodes * 100;
}

/**
 * calc singe score, such as cpu/memory/disk/bandwitdh/vnode
 * 1. get the score config
 * 2. if the value is out of range, use border data
 * 3. otherwise use interpolation method
 **/
void balanceCalcDnodeScore(SDnodeObj *pDnode) {
  pDnode->lbScore = balanceCalcCpuScore(pDnode) + balanceCalcMemoryScore(pDnode) + balanceCalcDiskScore(pDnode) +
                    balanceCalcBandwidthScore(pDnode) + balanceCalcModuleScore(pDnode) + balanceCalcVnodeScore(pDnode, 0) +
                    pDnode->customScore;
}

float balanceTryCalcDnodeScore(SDnodeObj *pDnode, int32_t extra) {
  return balanceCalcCpuScore(pDnode) + balanceCalcMemoryScore(pDnode) + balanceCalcDiskScore(pDnode) +
         balanceCalcBandwidthScore(pDnode) + balanceCalcModuleScore(pDnode) + balanceCalcVnodeScore(pDnode, extra) +
         pDnode->customScore;
}

void balanceInitDnodeList() {
  if (tsBalanceDnodeList != NULL) {
    free(tsBalanceDnodeList);
    tsBalanceDnodeList = NULL;
  }

  if (tsBalanceDnodesListMallocSize <= 0) tsBalanceDnodesListMallocSize = 4;
  tsBalanceDnodeList = (SDnodeObj **)malloc(tsBalanceDnodesListMallocSize * sizeof(SDnodeObj *));
  memset(tsBalanceDnodeList, 0, tsBalanceDnodesListMallocSize * sizeof(SDnodeObj *));
}

void balanceCalcSystemScore() {
  if (!tsEnableMonitorModule) return;
  if (!tsBalancePolicy) return;

  static uint32_t lastTime = 0;
  if (lastTime == 0) {
    lastTime = taosGetTimestampSec();
    return;
  }

  uint32_t ts = taosGetTimestampSec();
  if (ts - lastTime > 86400) {
    lastTime = ts;
    // fetch system paramete from sys.cpu and so on
  }
}

void balanceReleaseDnodeList() {
  if (tsBalanceDnodeList != NULL) {
    free(tsBalanceDnodeList);
    tsBalanceDnodeList = NULL;
  }
}

static void balanceAllocDnodeOrderList() {
  tsBalanceDnodeListSize = sdbGetNumOfRows(tsDnodeSdb);

  if (tsBalanceDnodesListMallocSize <= tsBalanceDnodeListSize) {
    tsBalanceDnodesListMallocSize = tsBalanceDnodeListSize * 2;
    if (tsBalanceDnodesListMallocSize <= 0) tsBalanceDnodesListMallocSize = 4;
    balanceReleaseDnodeList();
    tsBalanceDnodeList = (SDnodeObj **)malloc(tsBalanceDnodesListMallocSize * sizeof(SDnodeObj *));
    memset(tsBalanceDnodeList, 0, tsBalanceDnodesListMallocSize * sizeof(SDnodeObj *));
  }
}

/**
 * create a dnode list based on the balance score in asscending order
 * the balance score is calculate here
 * for every operation may change the score
 **/
void balanceMakeDnodeList() {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;

  balanceAllocDnodeOrderList();

  // fill and order
  int32_t dnodeIndex = 0;
  while (dnodeIndex < tsBalanceDnodeListSize) {
    pNode = sdbFetchRow(tsDnodeSdb, pNode, (void **)&pDnode);
    if (pDnode == NULL) break;
    balanceCalcDnodeScore(pDnode);

    int32_t orderIndex;
    for (orderIndex = dnodeIndex; orderIndex > 0; --orderIndex) {
      if (pDnode->lbScore > tsBalanceDnodeList[orderIndex - 1]->lbScore) {
        break;
      }
      tsBalanceDnodeList[orderIndex] = tsBalanceDnodeList[orderIndex - 1];
    }
    tsBalanceDnodeList[orderIndex] = pDnode;
    dnodeIndex++;
    mgmtDecDnodeRef(pDnode);
  }
}

static int32_t balanceGetScoresMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 16;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "IP");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "system scores");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "custom scores");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "module scores");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "vnode scores");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "total scores");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "open vnodes");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "total vnodes");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 18;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "balance state");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = sdbGetNumOfRows(tsDnodeSdb);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pNode = NULL;

  return 0;
}

static int32_t balanceRetrieveScores(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t    numOfRows = 0;
  SDnodeObj *pDnode = NULL;
  char *     pWrite;
  int32_t    cols = 0;
  char       ipstr[20];

  while (numOfRows < rows) {
    pShow->pNode = sdbFetchRow(tsDnodeSdb, pShow->pNode, (void **)&pDnode);
    if (pDnode == NULL) break;

    int32_t systemScore = balanceCalcCpuScore(pDnode) + balanceCalcMemoryScore(pDnode) + balanceCalcDiskScore(pDnode) +
                      balanceCalcBandwidthScore(pDnode);
    float moduleScore = balanceCalcModuleScore(pDnode);
    float vnodeScore = balanceCalcVnodeScore(pDnode, 0);

    cols = 0;

    tinet_ntoa(ipstr, pDnode->privateIp);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, ipstr);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = systemScore;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDnode->customScore;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = (int32_t)moduleScore;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = (int32_t)vnodeScore;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = (int32_t)(vnodeScore + moduleScore + pDnode->customScore + systemScore);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDnode->openVnodes;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDnode->numOfTotalVnodes;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, taosGetDnodeLbStatusStr(pDnode->lbStatus));
    cols++;

    numOfRows++;
    mgmtDecDnodeRef(pDnode);
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}
