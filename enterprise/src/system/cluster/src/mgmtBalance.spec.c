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
#include "mgmtBalance.h"
#include "vnodeStatus.h"

/*
 * once sdb work as mater, then mgmtAccessSquence reset to zero, increase mgmtAccessSquence every balance interval
 */
uint32_t        mgmtAccessSquence = 0;
pthread_mutex_t balanceMutex;
void *          balanceTimer = NULL;
void *          balanceMonitorTimer = NULL;

void mgmtLockBalance() { pthread_mutex_lock(&balanceMutex); }

void mgmtUnLockBalance() { pthread_mutex_unlock(&balanceMutex); }

void mgmtUpdateDnodeState(SDnodeObj *pDnode, int lbStatus) {
  pDnode->lbStatus = lbStatus;
  sdbUpdateRow(dnodeSdb, pDnode, tsDnodeUpdateSize, 1);
}

void mgmtUpdateVgroupState(SVgObj *pVgroup, int lbStatus, int srcIp) {
  pVgroup->lbTime = taosGetTimestampSec();
  pVgroup->lbStatus = lbStatus;
  pVgroup->lbIp = srcIp;
  sdbUpdateRow(vgSdb, pVgroup, tsVgUpdateSize, 1);
}

/**
 * check if a dnode in remove state
 **/
bool mgmtCheckDnodeInRemoveState(SDnodeObj *pDnode) {
  return pDnode->lbStatus == TSDB_DN_LB_STATUS_OFFLINE_REMOVING || pDnode->lbStatus == TSDB_DN_LB_STATE_SHELL_REMOVING;
}

/**
 * check if a dnode in offline state
 **/
bool mgmtCheckDnodeInOfflineState(SDnodeObj *pDnode) { return pDnode->status == TSDB_DN_STATUS_OFFLINE; }

/**
 * check if can alloc a vnode from this dnode
 **/
bool mgmtCheckDnodeFree(SDnodeObj *pDnode) {
  mTrace("dnode:%s, try alloc vnode, status:%s, lbstatus:%s, numOfFreeVnodes:%d",
         taosIpStr(pDnode->privateIp), taosGetDnodeStatusStr(pDnode->status),
         taosGetDnodeLbStatusStr(pDnode->lbStatus), pDnode->numOfFreeVnodes);
  for (int vnode = 0; vnode < pDnode->numOfVnodes; vnode++) {
    if (pDnode->vload[vnode].vgId != 0) {
      mTrace("dnode:%s, try alloc vnode, vnode:%d already exist, vgroup:%d, vnodestatus:%s, dropstatus:%s, syncstatus:%s",
             taosIpStr(pDnode->privateIp), vnode, pDnode->vload[vnode].vgId,
             taosGetVnodeStatusStr(pDnode->vload[vnode].status),
             taosGetVnodeDropStatusStr(pDnode->vload[vnode].dropStatus),
             taosGetVnodeSyncStatusStr(pDnode->vload[vnode].syncStatus));
    }
  }

  if (mgmtCheckDnodeInRemoveState(pDnode)) {
    return false;
  }

  if (mgmtCheckDnodeInOfflineState(pDnode)) {
    return false;
  }

  if (pDnode->numOfFreeVnodes <= 0) {
    return false;
  }

  if (pDnode->diskAvailable <= tsMinimalDataDirGB) {
    mError("dnode:%s, no disk space to alloc vnode, available:%fGB", taosIpStr(pDnode->privateIp), pDnode->diskAvailable);
    return false;
  }

  return true;
}

/**
 * check if can balance a vnode into this dnode
 **/
bool mgmtCheckDnodeCanBalanceIn(SDnodeObj *pDnode) {
  if (pDnode->lbStatus != TSDB_DN_LB_STATUS_BALANCED) {
    return false;
  }

  if (mgmtCheckDnodeInOfflineState(pDnode)) {
    return false;
  }

  if (pDnode->numOfFreeVnodes <= 0) {
    return false;
  }

  return true;
}

/**
 * check if can balance a vnode out of this dnode
 **/
bool mgmtCheckDnodeCanBalanceOut(SDnodeObj *pDnode) {
  if (pDnode->lbStatus != TSDB_DN_LB_STATUS_BALANCED) {
    return false;
  }

  if (mgmtCheckDnodeInOfflineState(pDnode)) {
    return false;
  }

  if (pDnode->openVnodes <= 0) {
    return false;
  }

  return true;
}

bool mgmtCheckVgroupHaveRemovingDnode(SVgObj *pVgroup) {
  for (int i = 0; i < pVgroup->numOfVnodes; ++i) {
    SVnodeGid *pVnodeGid = pVgroup->vnodeGid + i;
    SDnodeObj *pDnode = mgmtGetDnode(pVnodeGid->ip);
    if (pDnode != NULL && mgmtCheckDnodeInRemoveState(pDnode)) {
      return false;
    }
  }

  return true;
}

/**
 * for each vnode in this vgroup
 * if vnode.ip equal to pDnode.privateIp, return true
 * otherwist false
 **/
bool mgmtCheckDnodeInVgroup(SDnodeObj *pDnode, SVgObj *pVgroup) {
  for (int i = 0; i < pVgroup->numOfVnodes; ++i) {
    SVnodeGid *pVnodeGid = pVgroup->vnodeGid + i;
    if (pVnodeGid->ip == pDnode->privateIp) {
      return true;
    }
  }

  return false;
}

/**
 * find a free vnode from given dnode
 **/
bool mgmtAllocVnode(SVgObj *pVgroup, SVnodeGid *pVnodeGid, SDnodeObj *pDnode) {
  int selectedVnode = -1;
  for (int i = 0; i < pDnode->numOfVnodes; i++) {
    int vnode = (i + pDnode->lastAllocVnode) % pDnode->numOfVnodes;
    if (pDnode->vload[vnode].vgId == 0 && pDnode->vload[vnode].status == TSDB_VN_STATUS_OFFLINE) {
      selectedVnode = vnode;
      break;
    }
  }

  if (selectedVnode == -1) {
    if (pVgroup->vgId != 0) {
      mError("dnode:%s, alloc vnode to vgroup:%d failed, free vnodes:%d",
             taosIpStr(pDnode->privateIp), pVgroup->vgId, pDnode->numOfFreeVnodes);
    } else {
      mError("dnode:%s, alloc vnode to new vgroup failed, free vnodes:%d",
             taosIpStr(pDnode->privateIp), pDnode->numOfFreeVnodes);
    }

    return false;
  } else {
    if (pVgroup->vgId != 0) {
      mTrace("dnode:%s, vnode:%d allocated to vgroup:%d, last alloc vnode:%d",
             taosIpStr(pDnode->privateIp), selectedVnode, pVgroup->vgId, pDnode->lastAllocVnode);
    } else {
      mTrace("dnode:%s, vnode:%d allocated to new vgroup, last alloc vnode:%d",
             taosIpStr(pDnode->privateIp), selectedVnode, pDnode->lastAllocVnode);
    }

    pVnodeGid->ip = pDnode->privateIp;
    pVnodeGid->publicIp = pDnode->publicIp;
    pVnodeGid->vnode = selectedVnode;
    pDnode->lastAllocVnode = selectedVnode + 1;
    if (pDnode->lastAllocVnode >= pDnode->numOfVnodes) pDnode->lastAllocVnode = 0;
    return true;
  }
}

/**
 * remove one vnode from the vgroup
 **/
void mgmtDiscardVnode(SVgObj *pVgroup, SVnodeGid *pVnodeGid) {
  mTrace("dnode:%s, vgroup:%d, vnode:%d is dropping", taosIpStr(pVnodeGid->ip), pVgroup->vgId, pVnodeGid->vnode);

  SVnodeGid pBackupVnodeGid = *pVnodeGid;

  SVnodeGid vnodeGid[TSDB_VNODES_SUPPORT] = {0};
  int       numOfVnodes = 0;
  for (int i = 0; i < pVgroup->numOfVnodes; ++i) {
    SVnodeGid *pTmpVodeGid = pVgroup->vnodeGid + i;
    if (pTmpVodeGid == pVnodeGid) {
      continue;
    }
    vnodeGid[numOfVnodes] = *pTmpVodeGid;
    ++numOfVnodes;
  }
  memcpy(pVgroup->vnodeGid, vnodeGid, TSDB_VNODES_SUPPORT * sizeof(SVnodeGid));
  pVgroup->numOfVnodes = numOfVnodes;

  SDnodeObj *pDnode = mgmtGetDnode(pBackupVnodeGid.ip);
  if (pDnode) {
    SVnodeLoad *pVload = pDnode->vload + pBackupVnodeGid.vnode;
    memset(pVload, 0, sizeof(SVnodeLoad));
    mgmtCalcNumOfFreeVnodes(pDnode);
    mgmtUpdateDnode(pDnode);
  } else {
    mError("dnode:%s, not in dnode DB!!!", taosIpStr(pBackupVnodeGid.ip));
  }

  for (int i = 0; i < pVgroup->numOfVnodes; ++i) {
    mTrace("dnode:%s, vgroup:%d, vnode:%d exist after drop", taosIpStr(pVgroup->vnodeGid[i].ip), pVgroup->vgId, pVgroup->vnodeGid[i].vnode);
  }

  sdbUpdateRow(vgSdb, pVgroup, tsVgUpdateSize, 1);

  mgmtSendOneFreeVnodeMsg(&pBackupVnodeGid);
  mgmtSendVPeersMsg(pVgroup);
}

/**
 * add one vnode to the vgroup
 **/
void mgmtAppendVnode(SVgObj *pVgroup, SVnodeGid *pVnodeGid) {
  mTrace("dnode:%s, vgroup:%d, vnode:%d is adding", taosIpStr(pVnodeGid->ip), pVgroup->vgId, pVnodeGid->vnode);

  if (pVgroup->numOfVnodes < TSDB_VNODES_SUPPORT) {
    pVgroup->vnodeGid[pVgroup->numOfVnodes] = *pVnodeGid;
    pVgroup->numOfVnodes++;
  }

  SDnodeObj *pDnode = mgmtGetDnode(pVnodeGid->ip);
  if (pDnode) {
    SVnodeLoad *pVload = pDnode->vload + pVnodeGid->vnode;
    memset(pVload, 0, sizeof(SVnodeLoad));
    pVload->vnode = pVnodeGid->vnode;
    pVload->vgId = pVgroup->vgId;
    mgmtCalcNumOfFreeVnodes(pDnode);
    mgmtUpdateDnode(pDnode);
  } else {
    mError("dnode:%s, not in dnode DB!!!", taosIpStr(pVnodeGid->ip));
  }

  sdbUpdateRow(vgSdb, pVgroup, tsVgUpdateSize, 1);

  for (int i = 0; i < pVgroup->numOfVnodes; ++i) {
    mTrace("%d-dnode:%s, vgroup:%d, vnode:%d exist after addition", i, taosIpStr(pVgroup->vnodeGid[i].ip), pVgroup->vgId, pVgroup->vnodeGid[i].vnode);
  }

  mgmtSendVPeersMsg(pVgroup);
}

void mgmtSwapVnodeGid(SVnodeGid *pVnodeGid1, SVnodeGid *pVnodeGid2) {
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
int mgmtAllocVnodes(SVgObj *pVgroup) {
  int dnode = 0;
  int vnodes = 0;

  mgmtLockBalance();

  mgmtMakeDnodeOrderList();

  for (int i = 0; i < pVgroup->numOfVnodes; ++i) {
    SVnodeGid *pVnodeGid = pVgroup->vnodeGid + i;
    for (; dnode < mgmtOrderedDnodesSize; ++dnode) {
      SDnodeObj *pDnode = mgmtOrderedDnodes[dnode];
      if (!mgmtCheckDnodeFree(pDnode)) {
        continue;
      }

      if (mgmtAllocVnode(pVgroup, pVnodeGid, pDnode)) {
        dnode++;
        vnodes++;
        break;
      }
    }
  }

  if (vnodes != pVgroup->numOfVnodes) {
    mTrace("vgroup:%d, db:%s need vnodes:%d, but alloc:%d, free them", pVgroup->vgId, pVgroup->dbName,
           pVgroup->numOfVnodes, vnodes);
    mgmtUnLockBalance();
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
    if (rand() % 2 == 0) {
      mgmtSwapVnodeGid(pVgroup->vnodeGid, pVgroup->vnodeGid + 1);
    }
  } else {
    int randVal = rand() % 6;
    if (randVal == 1) {  // 1, 0, 2
      mgmtSwapVnodeGid(pVgroup->vnodeGid + 0, pVgroup->vnodeGid + 1);
    } else if (randVal == 2) {  // 1, 2, 0
      mgmtSwapVnodeGid(pVgroup->vnodeGid + 0, pVgroup->vnodeGid + 1);
      mgmtSwapVnodeGid(pVgroup->vnodeGid + 1, pVgroup->vnodeGid + 2);
    } else if (randVal == 3) {  // 2, 1, 0
      mgmtSwapVnodeGid(pVgroup->vnodeGid + 0, pVgroup->vnodeGid + 2);
    } else if (randVal == 4) {  // 2, 0, 1
      mgmtSwapVnodeGid(pVgroup->vnodeGid + 0, pVgroup->vnodeGid + 2);
      mgmtSwapVnodeGid(pVgroup->vnodeGid + 1, pVgroup->vnodeGid + 2);
    }
    if (randVal == 5) {  // 0, 2, 1
      mgmtSwapVnodeGid(pVgroup->vnodeGid + 1, pVgroup->vnodeGid + 2);
    } else {
    }  // 0, 1, 2
  }

  mgmtUnLockBalance();
  return 0;
}

char *mgmtGetVnodeStatus(SVgObj *pVgroup, SVnodeGid *pVnode) {
  SDnodeObj *pDnode = mgmtGetDnode(pVnode->ip);
  if (pDnode == NULL) {
    mError("dnode:%s, vgroup:%d, vnode:%d dnode not exist", taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode);
    return "null";
  }

  SVnodeLoad *vload = pDnode->vload + pVnode->vnode;
  if (vload->vgId != pVgroup->vgId || vload->vnode != pVnode->vnode) {
    mError("dnode:%s, vgroup:%d, vnode:%d not same with dnode vgroup:%d vnode:%d",
           taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode, vload->vgId, vload->vnode);
    return "null";
  }

  return taosGetVnodeStatusStr(vload->status);
}

/**
 * desc: check vnode is ready (synced)
 **/
bool mgmtCheckVnodeReady(SDnodeObj *pDnode, SVgObj *pVgroup, SVnodeGid *pVnode) {
  if (pDnode == NULL) {
    pDnode = mgmtGetDnode(pVnode->ip);
    if (pDnode == NULL) {
      mError("dnode:%s, vgroup:%d, vnode:%d dnode not exist", taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode);
      return false;
    }
  }

  if (mgmtCheckDnodeInOfflineState(pDnode)) {
    mTrace("dnode:%s, vgroup:%d, vnode:%d dnode is offline", taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode);
    return false;
  }

  SVnodeLoad *vload = pDnode->vload + pVnode->vnode;
  if (vload->vgId != pVgroup->vgId || vload->vnode != pVnode->vnode) {
    mError("dnode:%s, vgroup:%d, vnode:%d not same with dnode vgroup:%d vnode:%d",
            taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode, vload->vgId, vload->vnode);
    return false;
  }

  mTrace("dnode:%s, vgroup:%d, vnode:%d, status:%s, syncstatus:%s",
          taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode,
          taosGetVnodeStatusStr(vload->status), taosGetVnodeSyncStatusStr(vload->syncStatus));
  return vload->status == TSDB_VN_STATUS_SLAVE || vload->status == TSDB_VN_STATUS_MASTER;
}

/**
 * desc: remove one vnode from vgroup
 * all vnodes in vgroup should in ready state, except the balancing one
 **/
void mgmtRemoveOneRedundantVnode(SVgObj *pVgroup) {
  if (pVgroup->numOfVnodes <= 1) return;

  SVnodeGid *pRmVnode = NULL;
  SVnodeGid *pSelVnode = NULL;
  int32_t    maxScore = 0;
  bool       allReady = false;

  for (int i = 0; i < pVgroup->numOfVnodes; ++i) {
    SVnodeGid *pVnode = &(pVgroup->vnodeGid[i]);
    SDnodeObj *pDnode = mgmtGetDnode(pVnode->ip);

    if (pDnode == NULL) {
      mError("dnode:%s, vgroup:%d, vnode:%d dnode not exist, remove it from vgroup",
              taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode);
      pRmVnode = pVnode;
      allReady = true;
      break;
    }

    if (pDnode->lbStatus == TSDB_DN_LB_STATE_SHELL_REMOVING) {
      mTrace("dnode:%s, vgroup:%d, vnode:%d, dnode in shell removing state",
              taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode);
      pRmVnode = pVnode;
      if (mgmtCheckDnodeInOfflineState(pDnode) && (mgmtAccessSquence - pDnode->lastAccess) > 5 * tsStatusInterval) {
        mTrace("dnode:%s, vgroup:%d, vnode:%d, dnode offline:%d seconds, remove it from vgroup",
                taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode, mgmtAccessSquence - pDnode->lastAccess);
        allReady = true;
      }
      break;
    }

    if (pVnode->ip == pVgroup->lbIp) {
      mTrace("dnode:%s, vgroup:%d, vnode:%d is updating", taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode);
      pRmVnode = pVnode;
      continue;
    }
  }

  if (pRmVnode != NULL && allReady) {
    mTrace("vgroup:%d is ready", pVgroup->vgId);
    mgmtDiscardVnode(pVgroup, pRmVnode);
    mgmtStartBalanceTimer(1000);
    return;
  }

  allReady = true;
  for (int i = 0; i < pVgroup->numOfVnodes; ++i) {
    SVnodeGid *pVnode = &(pVgroup->vnodeGid[i]);
    SDnodeObj *pDnode = mgmtGetDnode(pVnode->ip);
    if (pDnode == NULL) continue;

    if (pVnode == pRmVnode) {
      continue;
    }

    if (!mgmtCheckVnodeReady(pDnode, pVgroup, pVnode)) {
      allReady = false;
      break;
    }

    if (pSelVnode == NULL) {
      pSelVnode = pVnode;
      maxScore = pDnode->lbScore;
    } else {
      if (maxScore < pDnode->lbScore) {
        pSelVnode = pVnode;
        maxScore = pDnode->lbScore;
      }
    }
  }

  if (!allReady) {
    mTrace("vgroup:%d is not ready", pVgroup->vgId);
  } else {
    mTrace("vgroup:%d is ready", pVgroup->vgId);
    if (pRmVnode != NULL) {
      pSelVnode = pRmVnode;
    }
    if (pSelVnode != NULL) {
      mgmtDiscardVnode(pVgroup, pSelVnode);
      mgmtStartBalanceTimer(1001);
    }
  }
}

/**
 * desc: add vnode to vgroup, find a new one if dest dnode is null
 **/
bool mgmtAddVnode(SVgObj *pVgroup, SDnodeObj *pSrcDnode, SDnodeObj *pDestDnode) {
  if (pDestDnode == NULL) {
    for (int i = 0; i < mgmtOrderedDnodesSize; ++i) {
      SDnodeObj *pDnode = mgmtOrderedDnodes[i];

      if (pDnode == pSrcDnode) {
        continue;
      }

      if (!mgmtCheckDnodeFree(pDnode)) {
        continue;
      }

      if (mgmtCheckDnodeInVgroup(pDnode, pVgroup)) {
        continue;
      }

      pDestDnode = pDnode;
      mTrace("vgroup:%d, add vnode to dnode:%s", pVgroup->vgId, taosIpStr(pDnode->privateIp));
      break;
    }
  }

  if (pDestDnode == NULL) {
    return false;
  }

  SVnodeGid pVnodeGid;
  if (!mgmtAllocVnode(pVgroup, &pVnodeGid, pDestDnode)) {
    return false;
  }

  uint32_t srcIp = (pSrcDnode == NULL ? 0 : pSrcDnode->privateIp);
  mgmtUpdateVgroupState(pVgroup, TSDB_VG_LB_STATUS_UPDATE, srcIp);
  mgmtAppendVnode(pVgroup, &pVnodeGid);
  mgmtStartBalanceTimer(1002);

  return true;
}

void mgmtMonitorDnodeBalanced(int mseconds) {
  if (mseconds == 0) {
    mTrace("balance function is scheduled by schedule, dnodes:%d", mgmtOrderedDnodesSize);
  } else {
    mTrace("balance function is scheduled by event for %d mseconds arrived, dnodes:%d", mseconds, mgmtOrderedDnodesSize);
  }

  if (mgmtOrderedDnodesSize < 2) {
    mTrace("dnodes:%d not enough, stop balance", mgmtOrderedDnodesSize);
    return;
  }

  for (int src = mgmtOrderedDnodesSize - 1; src >= 0; --src) {
    SDnodeObj *pDnode = mgmtOrderedDnodes[src];
    mTrace("%d-dnode:%s, state:%s, lbstatus:%s, lbScore:%.1f, totalVnodes:%d, freeVnodes:%d, openVnodes:%d",
            mgmtOrderedDnodesSize - src - 1, taosIpStr(pDnode->privateIp), taosGetDnodeStatusStr(pDnode->status),
            taosGetDnodeLbStatusStr(pDnode->lbStatus),
            pDnode->lbScore, pDnode->numOfVnodes, pDnode->numOfFreeVnodes, pDnode->openVnodes
    );
  }

  if ((mgmtOrderedDnodes[mgmtOrderedDnodesSize - 1]->lbScore - mgmtOrderedDnodes[0]->lbScore) < 2) {
    mTrace("all dnodes:%d is already balanced", mgmtOrderedDnodesSize);
    return;
  }

  for (int src = mgmtOrderedDnodesSize - 1; src > 0; --src) {
    SDnodeObj *pSrcDnode = mgmtOrderedDnodes[src];
    if (!mgmtCheckDnodeCanBalanceOut(pSrcDnode)) {
      continue;
    }

    float srcScore = mgmtTryCalcDnodeScore(pSrcDnode, -1);

    for (int i = 0; i < pSrcDnode->numOfVnodes; ++i) {
      SVnodeLoad *pVload = pSrcDnode->vload + i;
      if (pVload->vgId == 0) continue;

      SVgObj *pVgroup = mgmtGetVgroup(pVload->vgId);
      if (pVgroup == NULL) continue;
      if (pVgroup->lbStatus != TSDB_VG_LB_STATUS_READY) continue;

      for (int dest = 0; dest < src; dest++) {
        SDnodeObj *pDestDnode = mgmtOrderedDnodes[dest];
        if (!mgmtCheckDnodeCanBalanceIn(pDestDnode)) {
          continue;
        }

        float destScore = mgmtTryCalcDnodeScore(pDestDnode, 1);
        if (srcScore + 0.0001 < destScore) {
          continue;
        }

        if (mgmtCheckDnodeInVgroup(pDestDnode, pVgroup)) {
          continue;
        }

        // if (pVgroup->numOfVnodes > 1 &&
        // mgmtCheckVgroupHaveRemovingDnode(pVgroup)) {
        //  continue;
        //}

        mTrace("dnode:%s, vgroup:%d begin balancing to dnode:%s, srcScore:%.1f:%.1f, destScore:%.1f:%.1f",
            taosIpStr(pSrcDnode->privateIp), pVgroup->vgId, taosIpStr(pDestDnode->privateIp),
            pSrcDnode->lbScore, srcScore, pDestDnode->lbScore, destScore);
        if (mgmtAddVnode(pVgroup, pSrcDnode, pDestDnode)) {
          mgmtUpdateDnodeState(pSrcDnode, TSDB_DN_LB_STATUS_BALANCING);
          return;
        }
      }
    }
  }
}

// if mgmt changed to master
// 1. reset mgmtAccessSquence to zero
// 2. reset state of dnodes to offline
// 3. reset lastAccess of dnodes to zero
void mgmtSetDnodeOfflineOnSdbChanged() {
  mPrint("work as master at %d set sequence:%d to 0", sdbMasterStartTime, mgmtAccessSquence);

  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;
  while (1) {
    pNode = sdbFetchRow(dnodeSdb, pNode, (void **)&pDnode);
    if (pDnode == NULL) break;

    mPrint("dnode:%s set access:%d to 0", taosIpStr(pDnode->privateIp), pDnode->lastAccess);
    pDnode->lastAccess = 0;
    pDnode->status = TSDB_DN_STATUS_OFFLINE;  // while master change, should reset dnode to offline
  }

  mgmtAccessSquence = 0;
}

void mgmtStartBalance(int64_t mseconds) {
  if (!sdbMaster) return;

  static uint32_t lastTime = 0;

  mgmtLockBalance();
  mgmtMakeDnodeOrderList();
  mgmtMonitorDnodes();
  mgmtMonitorVgroups();
  if (mseconds != 0 || (taosGetTimestampSec() - lastTime) > tsBalanceStartInterval) {
    mgmtMonitorDnodeBalanced(mseconds);
    mgmtMonitorDnodeModule();
    lastTime = taosGetTimestampSec();
  }
  mgmtUnLockBalance();

  mgmtCalcSystemScore();
}

void mgmtProcessBalanceTimer(void *handle, void *tmrId) {
  if (handle == NULL) {
    mgmtAccessSquence += tsBalanceMonitorInterval;
  }

  balanceTimer = NULL;
  mgmtStartBalance((int64_t)handle);
  if (balanceTimer == NULL) {
    taosTmrReset(mgmtProcessBalanceTimer, tsBalanceMonitorInterval * 1000, NULL, mgmtTmr, &balanceTimer);
  }
}

void mgmtStartBalanceTimer(int64_t mseconds) {
  mTrace("balance function will be called after %d mseconds", mseconds);
  taosTmrReset((TAOS_TMR_CALLBACK)mgmtProcessBalanceTimer, mseconds, (void *)mseconds, mgmtTmr, &balanceTimer);
}

void mgmtMonitorVgroups() {
  void *  pNode = NULL;
  SVgObj *pVgroup = NULL;
  SDbObj *pDb = NULL;
  int64_t curTime = time(NULL);

  while (1) {
    pNode = sdbFetchRow(vgSdb, pNode, (void **)&pVgroup);
    if (pVgroup == NULL) break;
    if (pVgroup->lbStatus == TSDB_VG_LB_STATUS_READY) continue;
    if (pVgroup->lbTime + 5 * tsStatusInterval >= curTime) continue;

    pDb = mgmtGetDb(pVgroup->dbName);
    if (pDb == NULL) {
      mError("vgroup:%d, db:%s is not exist", pVgroup->vgId, pVgroup->dbName);
      continue;
    }

    int dbReplica = pDb->cfg.replications;
    int vgReplica = pVgroup->numOfVnodes;
    mTrace("vgroup:%d, db:%s is updating, replica:%d lbIp:%s, db replica:%d", pVgroup->vgId, pVgroup->dbName,
           vgReplica, taosIpStr(pVgroup->lbIp), dbReplica);

    if (vgReplica > dbReplica) {
      mgmtRemoveOneRedundantVnode(pVgroup);
    } else if (vgReplica == dbReplica) {
      mTrace("vgroup:%d, db:%s update success", pVgroup->vgId, pVgroup->dbName);
      mgmtUpdateVgroupState(pVgroup, TSDB_VG_LB_STATUS_READY, 0);
      mgmtStartBalanceTimer(1003);
    } else {
      mgmtAddVnode(pVgroup, NULL, NULL);
    }
  }
}

/**
 * if one dnode offline larger than OFFLINE_INTERVAL, remove it
 **/
void mgmtMontiorDnodeOffline(SDnodeObj *pDnode) {
  if (!mgmtCheckDnodeInOfflineState(pDnode)) return;
  if (mgmtCheckDnodeInRemoveState(pDnode)) return;
  if (pDnode->lastAccess + tsOfflineThreshold > mgmtAccessSquence) return;
  if (pDnode->privateIp == mgmtIpList.ip[0]) return;
  if (sdbGetNumOfRows(dnodeSdb) <= 1) return;

  mLPrint("dnode:%s set to removing state for it offline:%d seconds",
          taosIpStr(pDnode->privateIp), mgmtAccessSquence - pDnode->lastAccess);

  mgmtUpdateDnodeState(pDnode, TSDB_DN_LB_STATUS_OFFLINE_REMOVING);
  mgmtStartBalanceTimer(1004);
}

void mgmtMonitorDnodeBalancing(SDnodeObj *pDnode) {
  mTrace("dnode:%s, in balancing state", taosIpStr(pDnode->privateIp));

  int numOfUpdateVgroups = 0;
  for (int i = 0; i < pDnode->numOfVnodes; ++i) {
    SVnodeLoad *pVload = pDnode->vload + i;
    if (pVload->vgId == 0) continue;

    SVgObj *pVgroup = mgmtGetVgroup(pVload->vgId);
    if (pVgroup == NULL) continue;
    if (pVgroup->lbStatus == TSDB_VG_LB_STATUS_READY) continue;
    if (pVgroup->lbIp != pDnode->privateIp) continue;

    numOfUpdateVgroups++;
    mTrace("dnode:%s, vgroup:%d is updating", taosIpStr(pDnode->privateIp), pVgroup->vgId);
    break;
  }

  if (numOfUpdateVgroups == 0) {
    mPrint("dnode:%s, set to balanced state", taosIpStr(pDnode->privateIp));
    mgmtUpdateDnodeState(pDnode, TSDB_DN_LB_STATUS_BALANCED);
    mgmtStartBalanceTimer(1005);
  }
}

void mgmtMonitorDnodeRemoving(SDnodeObj *pDnode) {
  mTrace("dnode:%s, in removing state", taosIpStr(pDnode->privateIp));

  for (int i = 0; i < pDnode->numOfVnodes; ++i) {
    SVnodeLoad *pVload = pDnode->vload + i;
    if (pVload->vgId == 0) continue;

    SVgObj *pVgroup = mgmtGetVgroup(pVload->vgId);
    if (pVgroup == NULL) continue;

    SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
    if (pDb == NULL) {
      mError("dnode:%s, vgroup:%d db:%s not exist", taosIpStr(pDnode->privateIp), pVgroup->vgId, pVgroup->dbName);
      continue;
    }

    if (pDb->cfg.replications >= pVgroup->numOfVnodes) {
      if (!mgmtAddVnode(pVgroup, pDnode, NULL)) {
        mError("dnode:%s, vgroup:%d no enough dnode for remove operation", taosIpStr(pDnode->privateIp), pVgroup->vgId);
      } else {
        mTrace("dnode:%s, vgroup:%d set to updating state", taosIpStr(pDnode->privateIp), pVgroup->vgId);
      }
    } else {
      if (pVgroup->lbIp != pDnode->privateIp) {
        mTrace("dnode:%s, vgroup:%d set to updating state, change lbIp:%s to %s",
            taosIpStr(pDnode->privateIp), pVgroup->vgId, taosIpStr(pVgroup->lbIp), taosIpStr(pDnode->privateIp));
        mgmtUpdateVgroupState(pVgroup, TSDB_VG_LB_STATUS_UPDATE, pDnode->privateIp);
      } else {
        mTrace("dnode:%s, vgroup:%d wait update over", taosIpStr(pDnode->privateIp), pVgroup->vgId);
      }
    }

    if (pVgroup->lbStatus == TSDB_VG_LB_STATUS_UPDATE) {
      break;
    }
  }

  if (pDnode->numOfVnodes == pDnode->numOfFreeVnodes) {
    mPrint("dnode:%s, dropped for all vnodes are moving to other dnodes", taosIpStr(pDnode->privateIp));
    mgmtDropDnode(pDnode);
    mgmtStartBalanceTimer(1005);
  }
}

void mgmtMonitorDnodes() {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;
  bool       hasRemovingDnode = false;

  while (1) {
    pNode = sdbFetchRow(dnodeSdb, pNode, (void **)&pDnode);
    if (pDnode == NULL) break;

    mgmtMontiorDnodeOffline(pDnode);

    switch (pDnode->lbStatus) {
      case TSDB_DN_LB_STATUS_BALANCED:
        break;
      case TSDB_DN_LB_STATUS_BALANCING:
        mgmtMonitorDnodeBalancing(pDnode);
        break;
      case TSDB_DN_LB_STATUS_OFFLINE_REMOVING:
      case TSDB_DN_LB_STATE_SHELL_REMOVING:
        if (hasRemovingDnode) break;
        hasRemovingDnode = true;
        mgmtMonitorDnodeRemoving(pDnode);
        break;
      default:
        mgmtUpdateDnodeState(pDnode, TSDB_DN_LB_STATUS_BALANCED);
        break;
    }
  }
}

/**
 * should be called at system init function
 **/
int mgmtInitBalance() {
  if (balanceTimer == NULL && balanceMonitorTimer == NULL) {
    pthread_mutex_init(&balanceMutex, NULL);
    mgmtCreateDnodeOrderList();
    mgmtStartBalanceTimer(3000);
    mTrace("balance start fp:%p initialized", mgmtProcessBalanceTimer);
  }

  return 0;
}

/**
 * should be called at system release function
 **/
void mgmtCleanupBalance() {
  if (balanceTimer != NULL) {
    taosTmrStopA(&balanceTimer);
    pthread_mutex_destroy(&balanceMutex);
    mgmtReleaseDnodeOrderList();
    mTrace("stop balance timer");
  }
}

void mgmtSetDnodeUnRemove(SDnodeObj *pDnode) {
  mPrint("dnode:%s, set to unremove state", taosIpStr(pDnode->privateIp));
  if (mgmtCheckDnodeInRemoveState(pDnode)) {
    mgmtUpdateDnodeState(pDnode, TSDB_DN_LB_STATUS_BALANCED);
    mgmtStartBalanceTimer(11);
  }
}

void mgmtSetDnodeShellRemoving(SDnodeObj *pDnode) {
  mPrint("dnode:%s, set to shell removing state", taosIpStr(pDnode->privateIp));
  if (pDnode->privateIp == mgmtIpList.ip[0]) return;
  mgmtUpdateDnodeState(pDnode, TSDB_DN_LB_STATE_SHELL_REMOVING);
  mgmtStartBalanceTimer(12);
}

