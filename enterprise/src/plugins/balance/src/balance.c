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
#include "tutil.h"
#include "tbalance.h"
#include "tsync.h"
#include "ttime.h"
#include "ttimer.h"
#include "tglobal.h"
#include "tdataformat.h"
#include "dnode.h"
#include "mnode.h"
#include "mgmtDef.h"
#include "mgmtInt.h"
#include "mgmtDnode.h"
#include "mgmtDb.h"
#include "mgmtMnode.h"
#include "mgmtSdb.h"
#include "mgmtShell.h"
#include "mgmtUser.h"
#include "mgmtVgroup.h"

/*
 * once sdb work as mater, then tsAccessSquence reset to zero
 * increase tsAccessSquence every balance interval
 */
extern void *       tsMgmtTmr;
static void *       tsBalanceTimer = NULL;
static int32_t      tsBalanceDnodeListSize = 0;
static SDnodeObj ** tsBalanceDnodeList = NULL;
static int32_t      tsBalanceDnodeListMallocSize = 16;
static pthread_mutex_t tsBalanceMutex;

static void  balanceStartTimer(int64_t mseconds);
static void  balanceInitDnodeList();
static void  balanceCleanupDnodeList();
static void  balanceAccquireDnodeList();
static void  balanceReleaseDnodeList();
static void  balanceMonitorDnodeModule();
static float balanceTryCalcDnodeScore(SDnodeObj *pDnode, int32_t extraVnode);
static int32_t balanceGetScoresMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t balanceRetrieveScores(SShowObj *pShow, char *data, int32_t rows, void *pConn);

static void balanceLock() {
  pthread_mutex_lock(&tsBalanceMutex);
}

static void balanceUnLock() {
  pthread_mutex_unlock(&tsBalanceMutex);
}

static bool balanceCheckFree(SDnodeObj *pDnode) {
  if (pDnode->status == TAOS_DN_STATUS_DROPPING || pDnode->status == TAOS_DN_STATUS_OFFLINE) {
    mError("dnode:%d, status:%s not available", pDnode->dnodeId, mgmtGetDnodeStatusStr(pDnode->status));
    return false;
  }
  
  if (pDnode->totalVnodes <= pDnode->openVnodes) {
    mError("dnode:%d, openVnodes:%d totalVnodes:%d not available", pDnode->dnodeId, pDnode->openVnodes, pDnode->totalVnodes);
    return false;
  }

  if (pDnode->diskAvailable <= tsMinimalDataDirGB) {
    mError("dnode:%d, disk space:%fGB, not available", pDnode->dnodeId, pDnode->diskAvailable);
    return false;
  }

  return true;
}

static void balanceDiscardVnode(SVgObj *pVgroup, SVnodeGid *pVnodeGid) {
  mTrace("vgId:%d, dnode:%d is dropping", pVgroup->vgId, pVnodeGid->dnodeId);

  SDnodeObj *pDnode = mgmtGetDnode(pVnodeGid->dnodeId);
  if (pDnode != NULL) {
    atomic_sub_fetch_32(&pDnode->openVnodes, 1);
    mgmtDecDnodeRef(pDnode);
  }

  SVnodeGid vnodeGid[TSDB_MAX_REPLICA] = {0};
  int32_t   numOfVnodes = 0;
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SVnodeGid *pTmpVodeGid = pVgroup->vnodeGid + i;
    if (pTmpVodeGid == pVnodeGid) {
      continue;
    }
    vnodeGid[numOfVnodes] = *pTmpVodeGid;
    ++numOfVnodes;
  }
  memcpy(pVgroup->vnodeGid, vnodeGid, TSDB_MAX_REPLICA * sizeof(SVnodeGid));
  pVgroup->numOfVnodes = numOfVnodes;

  mgmtUpdateVgroup(pVgroup);
}

static void balanceSwapVnodeGid(SVnodeGid *pVnodeGid1, SVnodeGid *pVnodeGid2) {
  // SVnodeGid tmp = *pVnodeGid1;
  // *pVnodeGid1 = *pVnodeGid2;
  // *pVnodeGid2 = tmp;
}

int32_t balanceAllocVnodes(SVgObj *pVgroup) {
  int32_t dnode = 0;
  int32_t vnodes = 0;

  balanceLock();

  balanceAccquireDnodeList();

  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    for (; dnode < tsBalanceDnodeListSize; ++dnode) {
      SDnodeObj *pDnode = tsBalanceDnodeList[dnode];
      if (balanceCheckFree(pDnode)) {
        SVnodeGid *pVnodeGid = pVgroup->vnodeGid + i;
        pVnodeGid->dnodeId = pDnode->dnodeId;
        pVnodeGid->pDnode = pDnode;
        dnode++;
        vnodes++;
        break;
      }
    }
  }

  if (vnodes != pVgroup->numOfVnodes) {
    mTrace("vgId:%d, db:%s need vnodes:%d, but alloc:%d, free them", pVgroup->vgId, pVgroup->dbName,
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
    if (rand() % 2 == 0) {
      balanceSwapVnodeGid(pVgroup->vnodeGid, pVgroup->vnodeGid + 1);
    }
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

static bool balanceCheckVgroupReady(SVgObj *pVgroup, SVnodeGid *pRmVnode) {
  if (pVgroup->lbTime + 5 * tsStatusInterval > tsAccessSquence) {
    return false;
  }

  bool isReady = false;
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SVnodeGid *pVnode = pVgroup->vnodeGid + i;
    if (pVnode == pRmVnode) continue;

    if (pVnode->pDnode->status == TAOS_DN_STATUS_DROPPING) continue;
    if (pVnode->pDnode->status == TAOS_DN_STATUS_OFFLINE) continue;

    if (pVnode->role == TAOS_SYNC_ROLE_SLAVE || pVnode->role == TAOS_SYNC_ROLE_MASTER) {
      isReady = true;
    }
  }

  return isReady;
}

/**
 * desc: remove one vnode from vgroup
 * all vnodes in vgroup should in ready state, except the balancing one
 **/
static void balanceRemoveVnode(SVgObj *pVgroup) {
  if (pVgroup->numOfVnodes <= 1) return;

  SVnodeGid *pRmVnode = NULL;
  SVnodeGid *pSelVnode = NULL;
  int32_t    maxScore = 0;

  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SVnodeGid *pVnode = &(pVgroup->vnodeGid[i]);
    SDnodeObj *pDnode = mgmtGetDnode(pVnode->dnodeId);

    if (pDnode == NULL) {
      mError("vgId:%d, dnode:%d not exist, remove it", pVgroup->vgId, pVnode->dnodeId);
      pRmVnode = pVnode;
      break;
    }

    if (pDnode->status == TAOS_DN_STATUS_DROPPING) {
      mTrace("vgId:%d, dnode:%d in dropping state", pVgroup->vgId, pVnode->dnodeId);
      pRmVnode = pVnode;
    } else if (pVnode->dnodeId == pVgroup->lbDnodeId) {
      mTrace("vgId:%d, dnode:%d is updating", pVgroup->vgId, pVnode->dnodeId);
      pRmVnode = pVnode;
    } else {
      if (pSelVnode == NULL) {
        pSelVnode = pVnode;
        maxScore = pDnode->score;
      } else {
        if (maxScore < pDnode->score) {
          pSelVnode = pVnode;
          maxScore = pDnode->score;
        }
      }
    }

    mgmtDecDnodeRef(pDnode);
  }

  if (pRmVnode != NULL) {
    pSelVnode = pRmVnode;
  }

  if (!balanceCheckVgroupReady(pVgroup, pSelVnode)) {
    mTrace("vgId:%d, is not ready", pVgroup->vgId);
  } else {
    mTrace("vgId:%d, is ready, discard dnode:%d", pVgroup->vgId, pSelVnode->dnodeId);
    balanceDiscardVnode(pVgroup, pSelVnode);
  }
}

static bool balanceCheckDnodeInVgroup(SDnodeObj *pDnode, SVgObj *pVgroup) {
 for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SVnodeGid *pGid = &pVgroup->vnodeGid[i];
    if (pGid->dnodeId == 0) break;
    if (pGid->dnodeId == pDnode->dnodeId) {
      return true;
    }
  }

  return false;
}

/**
 * desc: add vnode to vgroup, find a new one if dest dnode is null
 **/
static bool balanceAddVnode(SVgObj *pVgroup, SDnodeObj *pSrcDnode, SDnodeObj *pDestDnode) {
  if (pDestDnode == NULL) {
    for (int32_t i = 0; i < tsBalanceDnodeListSize; ++i) {
      SDnodeObj *pDnode = tsBalanceDnodeList[i];
      if (pDnode == pSrcDnode) continue;
      if (balanceCheckDnodeInVgroup(pDnode, pVgroup)) continue;
      if (!balanceCheckFree(pDnode)) continue;
      
      pDestDnode = pDnode;
      mTrace("vgId:%d, add vnode to dnode:%d", pVgroup->vgId, pDnode->dnodeId);
      break;
    }
  }

  if (pDestDnode == NULL) return false;

  SVnodeGid *pVnodeGid = pVgroup->vnodeGid + pVgroup->numOfVnodes;
  pVnodeGid->dnodeId = pDestDnode->dnodeId;
  pVnodeGid->pDnode = pDestDnode;
  pVgroup->numOfVnodes++;

  pVgroup->status = TSDB_VG_STATUS_READY;
  if (pSrcDnode != NULL) {
    pVgroup->lbDnodeId = pSrcDnode->dnodeId;
  }

  atomic_add_fetch_32(&pDestDnode->openVnodes, 1);

  mgmtUpdateVgroup(pVgroup);
  return true;
}

static bool balanceMonitorBalance() {
  if (tsBalanceDnodeListSize < 2) return false;

  for (int32_t src = tsBalanceDnodeListSize - 1; src >= 0; --src) {
    SDnodeObj *pDnode = tsBalanceDnodeList[src];
    mTrace("%d-dnode:%d, state:%s, score:%.1f, totalVnodes:%d, openVnodes:%d", tsBalanceDnodeListSize - src - 1,
           pDnode->dnodeId, mgmtGetDnodeStatusStr(pDnode->status), pDnode->score, pDnode->totalVnodes,
           pDnode->openVnodes);
  }

  if ((tsBalanceDnodeList[tsBalanceDnodeListSize - 1]->score - tsBalanceDnodeList[0]->score) < 2) {
    mTrace("all dnodes:%d is already balanced", tsBalanceDnodeListSize);
    return false;
  }

  for (int32_t src = tsBalanceDnodeListSize - 1; src > 0; --src) {
    SDnodeObj *pSrcDnode = tsBalanceDnodeList[src];
    float srcScore = balanceTryCalcDnodeScore(pSrcDnode, -1);

    void *pIter = NULL;
    while (1) {
      SVgObj *pVgroup;
      pIter = mgmtGetNextVgroup(pIter, &pVgroup);
      if (pVgroup == NULL) break;

      if (balanceCheckDnodeInVgroup(pSrcDnode, pVgroup)) {
        for (int32_t dest = 0; dest < src; dest++) {
          SDnodeObj *pDestDnode = tsBalanceDnodeList[dest];
          if (balanceCheckDnodeInVgroup(pDestDnode, pVgroup)) continue;

          float destScore = balanceTryCalcDnodeScore(pDestDnode, 1);
          if (srcScore + 0.0001 < destScore) continue;
          
          mTrace("vgId:%d, balance from dnode:%d to dnode:%d, srcScore:%.1f:%.1f, destScore:%.1f:%.1f",
                 pVgroup->vgId, pSrcDnode->dnodeId, pDestDnode->dnodeId, pSrcDnode->score,
                 srcScore, pDestDnode->score, destScore);
          balanceAddVnode(pVgroup, pSrcDnode, pDestDnode);
          mgmtDecVgroupRef(pVgroup);
          return true;
        }
      }

      mgmtDecVgroupRef(pVgroup);
    }

    sdbFreeIter(pIter);
  }

  return false;
}

// if mgmt changed to master
// 1. reset balanceAccessSquence to zero
// 2. reset state of dnodes to offline
// 3. reset lastAccess of dnodes to zero
void balanceReset() {
  void *     pIter = NULL;
  SDnodeObj *pDnode = NULL;
  while (1) {
    pIter = mgmtGetNextDnode(pIter, &pDnode);
    if (pDnode == NULL) break;

    // while master change, should reset dnode to offline
    mPrint("dnode:%d set access:%d to 0", pDnode->dnodeId, pDnode->lastAccess);
    pDnode->lastAccess = 0;
    if (pDnode->status != TAOS_DN_STATUS_DROPPING) {
      pDnode->status = TAOS_DN_STATUS_OFFLINE;
    }

    mgmtDecDnodeRef(pDnode);
  }

  sdbFreeIter(pIter);

  tsAccessSquence = 0;
}

static int32_t balanceMonitorVgroups() {
  void *  pIter = NULL;
  SVgObj *pVgroup = NULL;
  bool    hasUpdatingVgroup = false;

  while (1) {
    pIter = mgmtGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) break;

    int32_t dbReplica = pVgroup->pDb->cfg.replications;
    int32_t vgReplica = pVgroup->numOfVnodes;
    
    if (vgReplica == dbReplica) {
      if (pVgroup->status != TSDB_VG_STATUS_READY) {
        pVgroup->status = TSDB_VG_STATUS_READY;
        pVgroup->lbTime = tsAccessSquence;
        pVgroup->lbDnodeId = 0;
        mgmtUpdateVgroup(pVgroup);
        hasUpdatingVgroup = true;
        mPrint("vgId:%d, set to ready state", pVgroup->vgId);
      }
    } else if (vgReplica > dbReplica) {
      mPrint("vgId:%d, replica:%d numOfVnodes:%d, try remove one vnode", pVgroup->vgId, dbReplica, vgReplica);
      hasUpdatingVgroup = true;
      balanceRemoveVnode(pVgroup);
    } else {
      mPrint("vgId:%d, replica:%d numOfVnodes:%d, try add one vnode", pVgroup->vgId, dbReplica, vgReplica);
      hasUpdatingVgroup = true;
      balanceAddVnode(pVgroup, NULL, NULL);
    }

    mgmtDecVgroupRef(pVgroup);
  }

  sdbFreeIter(pIter);

  return hasUpdatingVgroup;
}

static bool balanceMonitorDnodeDropping(SDnodeObj *pDnode) {
  mTrace("dnode:%d, in dropping state", pDnode->dnodeId);

  void *  pIter = NULL;
  bool    hasThisDnode = false;
  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = mgmtGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) break;

    hasThisDnode = balanceCheckDnodeInVgroup(pDnode, pVgroup);
    mgmtDecVgroupRef(pVgroup);

    if (hasThisDnode) break;
  }

  sdbFreeIter(pIter);

  if (!hasThisDnode) {
    mPrint("dnode:%d, dropped for all vnodes are moving to other dnodes", pDnode->dnodeId);
    mgmtDropDnode(pDnode);
    return true;
  }

  return false;
}

static bool balanceMontiorDropping() {
  void *pIter = NULL;
  SDnodeObj *pDnode = NULL;

  while (1) {
    mgmtDecDnodeRef(pDnode);
    pIter = mgmtGetNextDnode(pIter, &pDnode);
    if (pDnode == NULL) break;

    if (pDnode->status == TAOS_DN_STATUS_OFFLINE) {
      if (pDnode->lastAccess + tsOfflineThreshold > tsAccessSquence) continue;
      if (strcmp(pDnode->dnodeEp, dnodeGetMnodeMasterEp()) == 0) continue; 
      if (mgmtGetDnodesNum() <= 1) continue;

      mLPrint("dnode:%d, set to removing state for it offline:%d seconds", pDnode->dnodeId,
              tsAccessSquence - pDnode->lastAccess);

      pDnode->status = TAOS_DN_STATUS_DROPPING;
      mgmtUpdateDnode(pDnode);
      mgmtDecDnodeRef(pDnode);
      return true;
    }

    if (pDnode->status == TAOS_DN_STATUS_DROPPING) {
      bool ret = balanceMonitorDnodeDropping(pDnode);
      mgmtDecDnodeRef(pDnode);
      return ret;
    }
  }

  sdbFreeIter(pIter);

  return false;
}

static bool balanceStart() {
  if (!sdbIsMaster()) return false;

  balanceLock();

  balanceAccquireDnodeList();

  balanceMonitorDnodeModule();

  bool updateSoon = balanceMontiorDropping();

  if (!updateSoon) {
    updateSoon = balanceMonitorVgroups();
  }

  if (!updateSoon) {
    updateSoon = balanceMonitorBalance();
  }
 
  balanceReleaseDnodeList();

  balanceUnLock();

  return updateSoon;
}

static void balanceSetVgroupOffline(SDnodeObj* pDnode) {
  void *pIter = NULL;
  while (1) {
    SVgObj *pVgroup;
    pIter = mgmtGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) break;

    for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
      if (pVgroup->vnodeGid[i].pDnode == pDnode) {
        pVgroup->vnodeGid[i].role = TAOS_SYNC_ROLE_OFFLINE;
      }
    }
    mgmtDecVgroupRef(pVgroup);
  }

  sdbFreeIter(pIter);
}

static void balanceCheckDnodeAccess() {
  void *     pIter = NULL;
  SDnodeObj *pDnode = NULL;

  while (1) {
    pIter = mgmtGetNextDnode(pIter, &pDnode);
    if (pDnode == NULL) break;
    if (tsAccessSquence - pDnode->lastAccess > 3) {
      if (pDnode->status != TAOS_DN_STATUS_DROPPING && pDnode->status != TAOS_DN_STATUS_OFFLINE) {
        pDnode->status = TAOS_DN_STATUS_OFFLINE;
        mPrint("dnode:%d, set to offline state", pDnode->dnodeId);
        balanceSetVgroupOffline(pDnode);
      }
    }
    mgmtDecDnodeRef(pDnode);
  }

  sdbFreeIter(pIter);
}

static void balanceProcessBalanceTimer(void *handle, void *tmrId) {
  if (!sdbIsMaster()) return;

  tsBalanceTimer = NULL;
  tsAccessSquence ++;

  balanceCheckDnodeAccess();  
  bool updateSoon = false;

  if (handle == NULL) {
    if (tsAccessSquence % tsBalanceInterval == 0) {
      mTrace("balance function is scheduled by timer");
      updateSoon = balanceStart();
    }
  } else {
    int64_t mseconds = (int64_t)handle;
    mTrace("balance function is scheduled by event for %d mseconds arrived", mseconds);
    updateSoon = balanceStart();
  }

  if (updateSoon) {
    balanceStartTimer(1000);
  } else {
    taosTmrReset(balanceProcessBalanceTimer, tsStatusInterval * 1000, NULL, tsMgmtTmr, &tsBalanceTimer);
  }
}

static void balanceStartTimer(int64_t mseconds) {
  taosTmrReset(balanceProcessBalanceTimer, mseconds, (void *)mseconds, tsMgmtTmr, &tsBalanceTimer);
}

void balanceUpdateMgmt() {
  if (sdbIsMaster()) {
    balanceLock();
    balanceAccquireDnodeList();
    balanceMonitorDnodeModule();
    balanceReleaseDnodeList();
    balanceUnLock();
  }
}

void balanceNotify() {
  balanceStartTimer(500); 
}

int32_t balanceInit() {
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_SCORES, balanceGetScoresMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_SCORES, balanceRetrieveScores);
  
  pthread_mutex_init(&tsBalanceMutex, NULL);
  balanceInitDnodeList();
  balanceStartTimer(2000);
  mTrace("balance start fp:%p initialized", balanceProcessBalanceTimer);

  balanceReset();
  
  return 0;
}

void balanceCleanUp() {
  if (tsBalanceTimer != NULL) {
    taosTmrStopA(&tsBalanceTimer);
    pthread_mutex_destroy(&tsBalanceMutex);
    balanceCleanupDnodeList();
    mTrace("stop balance timer");
  }
}

int32_t balanceDropDnode(SDnodeObj *pDnode) {
  int32_t    totalFreeVnodes = 0;
  void *     pIter = NULL;
  SDnodeObj *pTempDnode = NULL;

  while (1) {
    pIter = mgmtGetNextDnode(pIter, &pTempDnode);
    if (pTempDnode == NULL) break;

    if (pTempDnode != pDnode && balanceCheckFree(pTempDnode)) {
      totalFreeVnodes += (pTempDnode->totalVnodes - pTempDnode->openVnodes);
    }

    mgmtDecDnodeRef(pTempDnode);
  }

  sdbFreeIter(pIter);

  if (pDnode->openVnodes > totalFreeVnodes) {
    mError("dnode:%d, openVnodes:%d totalFreeVnodes:%d no enough dnodes", pDnode->dnodeId, pDnode->openVnodes, totalFreeVnodes);
    return TSDB_CODE_NO_ENOUGH_DNODES;
  }

  pDnode->status = TAOS_DN_STATUS_DROPPING;
  mgmtUpdateDnode(pDnode);
  
  balanceStartTimer(1100);

  return TSDB_CODE_SUCCESS;
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
  if (pDnode->totalVnodes <= 1) return 0;
  if (pDnode->isMgmt) {
     return (float)tsMgmtEqualVnodeNum / pDnode->totalVnodes * 100;
  }
  return 0;
}

static float balanceCalcVnodeScore(SDnodeObj *pDnode, int32_t extra) {
  if (pDnode->status == TAOS_DN_STATUS_DROPPING || pDnode->status == TAOS_DN_STATUS_OFFLINE) return 1000;
  if (pDnode->totalVnodes <= 1) return 0;
  return (float)(pDnode->openVnodes + extra) / pDnode->totalVnodes * 100;
}

/**
 * calc singe score, such as cpu/memory/disk/bandwitdh/vnode
 * 1. get the score config
 * 2. if the value is out of range, use border data
 * 3. otherwise use interpolation method
 **/
void balanceCalcDnodeScore(SDnodeObj *pDnode) {
  pDnode->score = balanceCalcCpuScore(pDnode) + balanceCalcMemoryScore(pDnode) + balanceCalcDiskScore(pDnode) +
                  balanceCalcBandwidthScore(pDnode) + balanceCalcModuleScore(pDnode) +
                  balanceCalcVnodeScore(pDnode, 0) + pDnode->customScore;
}

float balanceTryCalcDnodeScore(SDnodeObj *pDnode, int32_t extra) {
  int32_t systemScore = balanceCalcCpuScore(pDnode) + balanceCalcMemoryScore(pDnode) + balanceCalcDiskScore(pDnode) +
                        balanceCalcBandwidthScore(pDnode);
  float moduleScore = balanceCalcModuleScore(pDnode);
  float vnodeScore = balanceCalcVnodeScore(pDnode, extra);

  float score = systemScore + moduleScore + vnodeScore +  pDnode->customScore;
  return score;
}

static void balanceInitDnodeList() {
  tsBalanceDnodeList = calloc(tsBalanceDnodeListMallocSize, sizeof(SDnodeObj *));
}

static void balanceCleanupDnodeList() {
  if (tsBalanceDnodeList != NULL) {
    free(tsBalanceDnodeList);
    tsBalanceDnodeList = NULL;
  }
}

static void balanceCheckDnodeListSize(int32_t dnodesNum) {
  if (tsBalanceDnodeListMallocSize <= dnodesNum) {
    tsBalanceDnodeListMallocSize = dnodesNum * 2;
    tsBalanceDnodeList = realloc(tsBalanceDnodeList, tsBalanceDnodeListMallocSize * sizeof(SDnodeObj *));
  }
}

void balanceAccquireDnodeList() {
  int32_t dnodesNum = mgmtGetDnodesNum();
  balanceCheckDnodeListSize(dnodesNum);

  void *     pIter = NULL;
  SDnodeObj *pDnode = NULL;
  int32_t    dnodeIndex = 0;

  while (1) {  
    if (dnodeIndex >= dnodesNum) break;
    pIter = mgmtGetNextDnode(pIter, &pDnode);
    if (pDnode == NULL) break;
    if (pDnode->status == TAOS_DN_STATUS_OFFLINE) {
      mgmtDecDnodeRef(pDnode);
      continue;
    }

    balanceCalcDnodeScore(pDnode);
    
    int32_t orderIndex = dnodeIndex;
    for (; orderIndex > 0; --orderIndex) {
      if (pDnode->score > tsBalanceDnodeList[orderIndex - 1]->score) {
        break;
      }
      tsBalanceDnodeList[orderIndex] = tsBalanceDnodeList[orderIndex - 1];
    }
    tsBalanceDnodeList[orderIndex] = pDnode;
    dnodeIndex++;
  }

  sdbFreeIter(pIter);

  tsBalanceDnodeListSize = dnodeIndex;
}

void balanceReleaseDnodeList() {
  for (int32_t i = 0; i < tsBalanceDnodeListSize; ++i) {
    SDnodeObj *pDnode = tsBalanceDnodeList[i];
    if (pDnode != NULL) {
      mgmtDecDnodeRef(pDnode);
    }
  }
}

static int32_t balanceGetScoresMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mgmtGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->pAcct->user, "root") != 0)  {
    mgmtDecUserRef(pUser);
    return TSDB_CODE_NO_RIGHTS;
  }

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "id");
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

  pShow->bytes[cols] = 18 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "balance state");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = mgmtGetDnodesNum();
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pIter = NULL;

  mgmtDecUserRef(pUser);

  return 0;
}

static int32_t balanceRetrieveScores(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t    numOfRows = 0;
  SDnodeObj *pDnode = NULL;
  char *     pWrite;
  int32_t    cols = 0;
  
  while (numOfRows < rows) {
    pShow->pIter = mgmtGetNextDnode(pShow->pIter, &pDnode);
    if (pDnode == NULL) break;

    int32_t systemScore = balanceCalcCpuScore(pDnode) + balanceCalcMemoryScore(pDnode) + balanceCalcDiskScore(pDnode) +
                      balanceCalcBandwidthScore(pDnode);
    float moduleScore = balanceCalcModuleScore(pDnode);
    float vnodeScore = balanceCalcVnodeScore(pDnode, 0);

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDnode->dnodeId;
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
    *(int32_t *)pWrite = pDnode->totalVnodes;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, mgmtGetDnodeStatusStr(pDnode->status));
    cols++;

    numOfRows++;
    mgmtDecDnodeRef(pDnode);
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static void balanceMonitorDnodeModule() {
  int32_t numOfMnodes = mgmtGetMnodesNum();
  if (numOfMnodes >= tsNumOfMPeers) return;

  for (int32_t i = 0; i < tsBalanceDnodeListSize; ++i) {
    SDnodeObj *pDnode = tsBalanceDnodeList[i];
    if (pDnode == NULL) break;

    if (pDnode->isMgmt || pDnode->status == TAOS_DN_STATUS_DROPPING || pDnode->status == TAOS_DN_STATUS_OFFLINE) {
      continue;
    }

    mLPrint("dnode:%d, numOfMnodes:%d expect:%d, add mnode in this dnode", pDnode->dnodeId, numOfMnodes, tsNumOfMPeers);
    mgmtAddMnode(pDnode->dnodeId);
    
    numOfMnodes = mgmtGetMnodesNum();
    if (numOfMnodes >= tsNumOfMPeers) return;
  }
}
