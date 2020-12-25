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
#include "tref.h"
#include "tsync.h"
#include "tglobal.h"
#include "dnode.h"
#include "bnInt.h"
#include "bnScore.h"
#include "bnThread.h"
#include "mnodeDb.h"
#include "mnodeMnode.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeUser.h"
#include "mnodeVgroup.h"

extern int64_t tsDnodeRid;
extern int64_t tsSdbRid;
static SBnMgmt tsBnMgmt;
static void  bnMonitorDnodeModule();

static void bnLock() {
  pthread_mutex_lock(&tsBnMgmt.mutex);
}

static void bnUnLock() {
  pthread_mutex_unlock(&tsBnMgmt.mutex);
}

static bool bnCheckFree(SDnodeObj *pDnode) {
  if (pDnode->status == TAOS_DN_STATUS_DROPPING || pDnode->status == TAOS_DN_STATUS_OFFLINE) {
    mError("dnode:%d, status:%s not available", pDnode->dnodeId, dnodeStatus[pDnode->status]);
    return false;
  }
  
  if (pDnode->openVnodes >= TSDB_MAX_VNODES) {
    mError("dnode:%d, openVnodes:%d maxVnodes:%d not available", pDnode->dnodeId, pDnode->openVnodes, TSDB_MAX_VNODES);
    return false;
  }

  if (pDnode->diskAvailable <= tsMinimalDataDirGB) {
    mError("dnode:%d, disk space:%fGB, not available", pDnode->dnodeId, pDnode->diskAvailable);
    return false;
  }

  if (pDnode->alternativeRole == TAOS_DN_ALTERNATIVE_ROLE_MNODE) {
    mDebug("dnode:%d, alternative role is master, can't alloc vnodes in this dnode", pDnode->dnodeId);
    return false;
  }

  return true;
}

static void bnDiscardVnode(SVgObj *pVgroup, SVnodeGid *pVnodeGid) {
  mDebug("vgId:%d, dnode:%d is dropping", pVgroup->vgId, pVnodeGid->dnodeId);

  SDnodeObj *pDnode = mnodeGetDnode(pVnodeGid->dnodeId);
  if (pDnode != NULL) {
    atomic_sub_fetch_32(&pDnode->openVnodes, 1);
    mnodeDecDnodeRef(pDnode);
  }

  SVnodeGid vnodeGid[TSDB_MAX_REPLICA]; memset(vnodeGid, 0, sizeof(vnodeGid)); /* = {0}; */
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

  mnodeUpdateVgroup(pVgroup);
}

static void bnSwapVnodeGid(SVnodeGid *pVnodeGid1, SVnodeGid *pVnodeGid2) {
  SVnodeGid tmp = *pVnodeGid1;
  *pVnodeGid1 = *pVnodeGid2;
  *pVnodeGid2 = tmp;
}

int32_t bnAllocVnodes(SVgObj *pVgroup) {
  int32_t dnode = 0;
  int32_t vnodes = 0;

  bnLock();
  bnAccquireDnodes();

  mDebug("db:%s, try alloc %d vnodes to vgroup, dnodes total:%d, avail:%d", pVgroup->dbName, pVgroup->numOfVnodes,
         mnodeGetDnodesNum(), tsBnDnodes.size);
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    for (; dnode < tsBnDnodes.size; ++dnode) {
      SDnodeObj *pDnode = tsBnDnodes.list[dnode];
      if (bnCheckFree(pDnode)) {
        SVnodeGid *pVnodeGid = pVgroup->vnodeGid + i;
        pVnodeGid->dnodeId = pDnode->dnodeId;
        pVnodeGid->pDnode = pDnode;
        dnode++;
        vnodes++;
        mDebug("dnode:%d, is selected, vnodeIndex:%d", pDnode->dnodeId, i);
        break;
      } else {
        mDebug("dnode:%d, is not selected, status:%s vnodes:%d disk:%fGB role:%d", pDnode->dnodeId,
               dnodeStatus[pDnode->status], pDnode->openVnodes, pDnode->diskAvailable, pDnode->alternativeRole);
      }
    }
  }

  if (vnodes != pVgroup->numOfVnodes) {
    bnReleaseDnodes();
    bnUnLock();

    mDebug("db:%s, need vnodes:%d, but alloc:%d", pVgroup->dbName, pVgroup->numOfVnodes, vnodes);

    void *     pIter = NULL;
    SDnodeObj *pDnode = NULL;
    while (1) {
      pIter = mnodeGetNextDnode(pIter, &pDnode);
      if (pDnode == NULL) break;
      mDebug("dnode:%d, status:%s vnodes:%d disk:%fGB role:%d", pDnode->dnodeId, dnodeStatus[pDnode->status],
             pDnode->openVnodes, pDnode->diskAvailable, pDnode->alternativeRole);
      mnodeDecDnodeRef(pDnode);
    }

    if (mnodeGetOnlineDnodesNum() == 0) {
      return TSDB_CODE_MND_NOT_READY;
    } else {
      return TSDB_CODE_MND_NO_ENOUGH_DNODES;
    }
  }

  bnReleaseDnodes();
  bnUnLock();
  return TSDB_CODE_SUCCESS;
}

static bool bnCheckVgroupReady(SVgObj *pVgroup, SVnodeGid *pRmVnode) {
  if (pVgroup->lbTime + 5 * tsStatusInterval > tsAccessSquence) {
    return false;
  }

  bool isReady = false;
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SVnodeGid *pVnode = pVgroup->vnodeGid + i;
    if (pVnode == pRmVnode) continue;

    mTrace("vgId:%d, check vgroup status, dnode:%d status:%d, vnode role:%s", pVgroup->vgId, pVnode->pDnode->dnodeId,
           pVnode->pDnode->status, syncRole[pVnode->role]);
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
static int32_t bnRemoveVnode(SVgObj *pVgroup) {
  if (pVgroup->numOfVnodes <= 1) return -1;

  SVnodeGid *pSelVnode = &pVgroup->vnodeGid[pVgroup->numOfVnodes - 1];
  mDebug("vgId:%d, vnode in dnode:%d will be dropped", pVgroup->vgId, pSelVnode->dnodeId);

  if (!bnCheckVgroupReady(pVgroup, pSelVnode)) {
    mDebug("vgId:%d, is not ready", pVgroup->vgId);
    return -1;
  } else {
    mDebug("vgId:%d, is ready, discard dnode:%d", pVgroup->vgId, pSelVnode->dnodeId);
    bnDiscardVnode(pVgroup, pSelVnode);
    return TSDB_CODE_SUCCESS;
  }
}

static bool bnCheckDnodeInVgroup(SDnodeObj *pDnode, SVgObj *pVgroup) {
 for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SVnodeGid *pGid = &pVgroup->vnodeGid[i];
    if (pGid->dnodeId == 0) break;
    if (pGid->dnodeId == pDnode->dnodeId) {
      return true;
    }
  }

  return false;
}

static SDnodeObj *bnGetAvailDnode(SVgObj *pVgroup) {
  for (int32_t i = 0; i < tsBnDnodes.size; ++i) {
    SDnodeObj *pDnode = tsBnDnodes.list[i];
    if (bnCheckDnodeInVgroup(pDnode, pVgroup)) continue;
    if (!bnCheckFree(pDnode)) continue;

    mDebug("vgId:%d, add vnode to dnode:%d", pVgroup->vgId, pDnode->dnodeId);
    return pDnode;
  }

  return NULL;
}

static int32_t bnAddVnode(SVgObj *pVgroup, SDnodeObj *pSrcDnode, SDnodeObj *pDestDnode) {
  if (pDestDnode == NULL || pSrcDnode == pDestDnode) {
    return TSDB_CODE_MND_DNODE_NOT_EXIST;
  }

  SVnodeGid vnodeGids[TSDB_MAX_REPLICA];
  memcpy(&vnodeGids, &pVgroup->vnodeGid, sizeof(SVnodeGid) * TSDB_MAX_REPLICA);

  int32_t numOfVnodes = pVgroup->numOfVnodes;
  vnodeGids[numOfVnodes].dnodeId = pDestDnode->dnodeId;
  vnodeGids[numOfVnodes].pDnode = pDestDnode;
  numOfVnodes++;

  for (int32_t v = 0; v < numOfVnodes; ++v) {
    if (pSrcDnode != NULL && pSrcDnode->dnodeId == vnodeGids[v].dnodeId) {
      bnSwapVnodeGid(&vnodeGids[v], &vnodeGids[numOfVnodes - 1]);
      pVgroup->lbDnodeId = pSrcDnode->dnodeId;
      break;
    }
  }

  memcpy(&pVgroup->vnodeGid, &vnodeGids, sizeof(SVnodeGid) * TSDB_MAX_REPLICA);
  pVgroup->numOfVnodes = numOfVnodes;
  atomic_add_fetch_32(&pDestDnode->openVnodes, 1);

  mnodeUpdateVgroup(pVgroup);

  return TSDB_CODE_SUCCESS;
}

static bool bnMonitorBalance() {
  if (tsBnDnodes.size < 2) return false;

  mDebug("monitor dnodes for balance, avail:%d", tsBnDnodes.size);
  for (int32_t src = tsBnDnodes.size - 1; src >= 0; --src) {
    SDnodeObj *pDnode = tsBnDnodes.list[src];
    mDebug("%d-dnode:%d, state:%s, score:%.1f, cores:%d, vnodes:%d", tsBnDnodes.size - src - 1, pDnode->dnodeId,
           dnodeStatus[pDnode->status], pDnode->score, pDnode->numOfCores, pDnode->openVnodes);
  }

  float scoresDiff = tsBnDnodes.list[tsBnDnodes.size - 1]->score - tsBnDnodes.list[0]->score;
  if (scoresDiff < 0.01) {
    mDebug("all dnodes:%d is already balanced, scoreDiff:%.1f", tsBnDnodes.size, scoresDiff);
    return false;
  }

  for (int32_t src = tsBnDnodes.size - 1; src > 0; --src) {
    SDnodeObj *pSrcDnode = tsBnDnodes.list[src];
    float srcScore = bnTryCalcDnodeScore(pSrcDnode, -1);
    if (tsEnableBalance == 0 && pSrcDnode->status != TAOS_DN_STATUS_DROPPING) {
      continue;
    }

    void *pIter = NULL;
    while (1) {
      SVgObj *pVgroup;
      pIter = mnodeGetNextVgroup(pIter, &pVgroup);
      if (pVgroup == NULL) break;

      if (bnCheckDnodeInVgroup(pSrcDnode, pVgroup)) {
        for (int32_t dest = 0; dest < src; dest++) {
          SDnodeObj *pDestDnode = tsBnDnodes.list[dest];
          if (bnCheckDnodeInVgroup(pDestDnode, pVgroup)) continue;

          float destScore = bnTryCalcDnodeScore(pDestDnode, 1);
          if (srcScore + 0.0001 < destScore) continue;
          if (!bnCheckFree(pDestDnode)) continue;
          
          mDebug("vgId:%d, balance from dnode:%d to dnode:%d, srcScore:%.1f:%.1f, destScore:%.1f:%.1f",
                 pVgroup->vgId, pSrcDnode->dnodeId, pDestDnode->dnodeId, pSrcDnode->score,
                 srcScore, pDestDnode->score, destScore);
          bnAddVnode(pVgroup, pSrcDnode, pDestDnode);
          mnodeDecVgroupRef(pVgroup);
          mnodeCancelGetNextVgroup(pIter);
          return true;
        }
      }

      mnodeDecVgroupRef(pVgroup);
    }
  }

  return false;
}

// if mgmt changed to master
// 1. reset balanceAccessSquence to zero
// 2. reset state of dnodes to offline
// 3. reset lastAccess of dnodes to zero
void bnReset() {
  void *     pIter = NULL;
  SDnodeObj *pDnode = NULL;
  while (1) {
    pIter = mnodeGetNextDnode(pIter, &pDnode);
    if (pDnode == NULL) break;

    // while master change, should reset dnode to offline
    mInfo("dnode:%d set access:%d to 0", pDnode->dnodeId, pDnode->lastAccess);
    pDnode->lastAccess = 0;
    if (pDnode->status != TAOS_DN_STATUS_DROPPING) {
      pDnode->status = TAOS_DN_STATUS_OFFLINE;
      pDnode->offlineReason = TAOS_DN_OFF_STATUS_NOT_RECEIVED;
    }

    mnodeDecDnodeRef(pDnode);
  }

  tsAccessSquence = 0;
}

static bool bnMonitorVgroups() {
  void *  pIter = NULL;
  SVgObj *pVgroup = NULL;
  bool    hasUpdatingVgroup = false;

  while (1) {
    pIter = mnodeGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) break;

    int32_t dbReplica = pVgroup->pDb->cfg.replications;
    int32_t vgReplica = pVgroup->numOfVnodes;
    int32_t code = -1;
    
    if (vgReplica > dbReplica) {
      mInfo("vgId:%d, replica:%d numOfVnodes:%d, try remove one vnode", pVgroup->vgId, dbReplica, vgReplica);
      hasUpdatingVgroup = true;
      code = bnRemoveVnode(pVgroup);
    } else if (vgReplica < dbReplica) {
      mInfo("vgId:%d, replica:%d numOfVnodes:%d, try add one vnode", pVgroup->vgId, dbReplica, vgReplica);
      hasUpdatingVgroup = true;

      SDnodeObj *pAvailDnode = bnGetAvailDnode(pVgroup);
      if (pAvailDnode == NULL) {
        code = TSDB_CODE_MND_DNODE_NOT_EXIST;
      } else {
        code = bnAddVnode(pVgroup, NULL, pAvailDnode);
      }
    }

    mnodeDecVgroupRef(pVgroup);
    if (code == TSDB_CODE_SUCCESS) {
      mnodeCancelGetNextVgroup(pIter);
      break;
    }
  }

  return hasUpdatingVgroup;
}

static bool bnMonitorDnodeDropping(SDnodeObj *pDnode) {
  mDebug("dnode:%d, in dropping state", pDnode->dnodeId);

  void *  pIter = NULL;
  bool    hasThisDnode = false;
  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = mnodeGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) break;

    hasThisDnode = bnCheckDnodeInVgroup(pDnode, pVgroup);
    mnodeDecVgroupRef(pVgroup);

    if (hasThisDnode) {
      mnodeCancelGetNextVgroup(pIter);
      break;
    }
  }

  if (!hasThisDnode) {
    mInfo("dnode:%d, dropped for all vnodes are moving to other dnodes", pDnode->dnodeId);
    mnodeDropDnode(pDnode, NULL);
    return true;
  }

  return false;
}

static bool bnMontiorDropping() {
  void *pIter = NULL;
  SDnodeObj *pDnode = NULL;

  while (1) {
    mnodeDecDnodeRef(pDnode);
    pIter = mnodeGetNextDnode(pIter, &pDnode);
    if (pDnode == NULL) break;

    if (pDnode->status == TAOS_DN_STATUS_OFFLINE) {
      if (pDnode->lastAccess + tsOfflineThreshold > tsAccessSquence) continue;
      if (dnodeIsMasterEp(pDnode->dnodeEp)) continue; 
      if (mnodeGetDnodesNum() <= 1) continue;

      mLInfo("dnode:%d, set to removing state for it offline:%d seconds", pDnode->dnodeId,
              tsAccessSquence - pDnode->lastAccess);

      pDnode->status = TAOS_DN_STATUS_DROPPING;
      mnodeUpdateDnode(pDnode);
      mnodeDecDnodeRef(pDnode);
      mnodeCancelGetNextDnode(pIter);
      return true;
    }

    if (pDnode->status == TAOS_DN_STATUS_DROPPING) {
      bool ret = bnMonitorDnodeDropping(pDnode);
      mnodeDecDnodeRef(pDnode);
      mnodeCancelGetNextDnode(pIter);
      return ret;
    }
  }

  return false;
}

bool bnStart() {
  if (!sdbIsMaster()) return false;

  bnLock();
  bnAccquireDnodes();

  bnMonitorDnodeModule();

  bool updateSoon = bnMontiorDropping();

  if (!updateSoon) {
    updateSoon = bnMonitorVgroups();
  }

  if (!updateSoon) {
    updateSoon = bnMonitorBalance();
  }
 
  bnReleaseDnodes();
  bnUnLock();

  return updateSoon;
}

static void bnSetVgroupOffline(SDnodeObj* pDnode) {
  void *pIter = NULL;
  while (1) {
    SVgObj *pVgroup;
    pIter = mnodeGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) break;

    for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
      if (pVgroup->vnodeGid[i].pDnode == pDnode) {
        pVgroup->vnodeGid[i].role = TAOS_SYNC_ROLE_OFFLINE;
      }
    }
    mnodeDecVgroupRef(pVgroup);
  }
}

void bnCheckStatus() {
  void *     pIter = NULL;
  SDnodeObj *pDnode = NULL;

  void *dnodeSdb = taosAcquireRef(tsSdbRid, tsDnodeRid);
  if (dnodeSdb == NULL) return;

  while (1) {
    pIter = mnodeGetNextDnode(pIter, &pDnode);
    if (pDnode == NULL) break;
    if (tsAccessSquence - pDnode->lastAccess > 3) {
      if (pDnode->status != TAOS_DN_STATUS_DROPPING && pDnode->status != TAOS_DN_STATUS_OFFLINE) {
        pDnode->status = TAOS_DN_STATUS_OFFLINE;
        pDnode->offlineReason = TAOS_DN_OFF_STATUS_MSG_TIMEOUT;
        mInfo("dnode:%d, set to offline state, access seq:%d last seq:%d laststat:%d", pDnode->dnodeId, tsAccessSquence,
              pDnode->lastAccess, pDnode->status);
        bnSetVgroupOffline(pDnode);
        bnStartTimer(3000);
      }
    }
    mnodeDecDnodeRef(pDnode);
  }

  taosReleaseRef(tsSdbRid, tsDnodeRid);
}

void bnCheckModules() {
  if (sdbIsMaster()) {
    bnLock();
    bnAccquireDnodes();
    bnMonitorDnodeModule();
    bnReleaseDnodes();
    bnUnLock();
  }
}

int32_t bnInit() {
  pthread_mutex_init(&tsBnMgmt.mutex, NULL);
  bnInitDnodes();
  bnInitThread();
  bnReset();
  
  return 0;
}

void bnCleanUp() {
  bnCleanupThread();
  bnCleanupDnodes();
  pthread_mutex_destroy(&tsBnMgmt.mutex);
}

int32_t bnDropDnode(SDnodeObj *pDnode) {
  int32_t    totalFreeVnodes = 0;
  void *     pIter = NULL;
  SDnodeObj *pTempDnode = NULL;

  while (1) {
    pIter = mnodeGetNextDnode(pIter, &pTempDnode);
    if (pTempDnode == NULL) break;

    if (pTempDnode != pDnode && bnCheckFree(pTempDnode)) {
      totalFreeVnodes += (TSDB_MAX_VNODES - pTempDnode->openVnodes);
    }

    mnodeDecDnodeRef(pTempDnode);
  }

  if (pDnode->openVnodes > totalFreeVnodes) {
    mError("dnode:%d, openVnodes:%d totalFreeVnodes:%d no enough dnodes", pDnode->dnodeId, pDnode->openVnodes, totalFreeVnodes);
    return TSDB_CODE_MND_NO_ENOUGH_DNODES;
  }

  pDnode->status = TAOS_DN_STATUS_DROPPING;
  mnodeUpdateDnode(pDnode);
  
  bnStartTimer(1100);

  return TSDB_CODE_SUCCESS;
}

static void bnMonitorDnodeModule() {
  int32_t numOfMnodes = mnodeGetMnodesNum();
  if (numOfMnodes >= tsNumOfMnodes) return;

  for (int32_t i = 0; i < tsBnDnodes.size; ++i) {
    SDnodeObj *pDnode = tsBnDnodes.list[i];
    if (pDnode == NULL) break;

    if (pDnode->isMgmt || pDnode->status == TAOS_DN_STATUS_DROPPING || pDnode->status == TAOS_DN_STATUS_OFFLINE) {
      continue;
    }

    if (pDnode->alternativeRole == TAOS_DN_ALTERNATIVE_ROLE_VNODE) {
      continue;
    }

    mLInfo("dnode:%d, numOfMnodes:%d expect:%d, create mnode in this dnode", pDnode->dnodeId, numOfMnodes, tsNumOfMnodes);
    mnodeCreateMnode(pDnode->dnodeId, pDnode->dnodeEp, true);

#if 0
    // Only create one mnode each time
    return;
#else
    numOfMnodes = mnodeGetMnodesNum();
    if (numOfMnodes >= tsNumOfMnodes) return;
#endif
  }
}

int32_t bnAlterDnode(struct SDnodeObj *pSrcDnode, int32_t vnodeId, int32_t dnodeId) {
  if (!sdbIsMaster()) {
    mError("dnode:%d, failed to alter vgId:%d to dnode:%d, for self not master", pSrcDnode->dnodeId, vnodeId, dnodeId);
    return TSDB_CODE_MND_DNODE_NOT_EXIST;
  }

  if (tsEnableBalance != 0) {
    mError("dnode:%d, failed to alter vgId:%d to dnode:%d, for balance enabled", pSrcDnode->dnodeId, vnodeId, dnodeId);
    return TSDB_CODE_MND_BALANCE_ENABLED;
  }

  SVgObj *pVgroup = mnodeGetVgroup(vnodeId);
  if (pVgroup == NULL) {
    mError("dnode:%d, failed to alter vgId:%d to dnode:%d, for vgroup not exist", pSrcDnode->dnodeId, vnodeId, dnodeId);
    return TSDB_CODE_MND_VGROUP_NOT_EXIST;
  }

  SDnodeObj *pDestDnode = mnodeGetDnode(dnodeId);
  if (pDestDnode == NULL) {
    mnodeDecVgroupRef(pVgroup);
    mError("dnode:%d, failed to alter vgId:%d to dnode:%d, for dnode not exist", pSrcDnode->dnodeId, vnodeId, dnodeId);
    return TSDB_CODE_MND_DNODE_NOT_EXIST;
  }

  bnLock();
  bnAccquireDnodes();

  int32_t code = TSDB_CODE_SUCCESS;
  if (!bnCheckDnodeInVgroup(pSrcDnode, pVgroup)) {
    mError("dnode:%d, failed to alter vgId:%d to dnode:%d, vgroup not in dnode:%d", pSrcDnode->dnodeId, vnodeId,
           dnodeId, pSrcDnode->dnodeId);
    code = TSDB_CODE_MND_VGROUP_NOT_IN_DNODE;
  } else if (bnCheckDnodeInVgroup(pDestDnode, pVgroup)) {
    mError("dnode:%d, failed to alter vgId:%d to dnode:%d, vgroup already in dnode:%d", pSrcDnode->dnodeId, vnodeId,
           dnodeId, dnodeId);
    code = TSDB_CODE_MND_VGROUP_ALREADY_IN_DNODE;
  } else if (!bnCheckFree(pDestDnode)) {
    mError("dnode:%d, failed to alter vgId:%d to dnode:%d, for dnode:%d not free", pSrcDnode->dnodeId, vnodeId, dnodeId,
           dnodeId);
    code = TSDB_CODE_MND_DNODE_NOT_FREE;
  } else {
    code = bnAddVnode(pVgroup, pSrcDnode, pDestDnode);
    mInfo("dnode:%d, alter vgId:%d to dnode:%d, result:%s", pSrcDnode->dnodeId, vnodeId, dnodeId, tstrerror(code));
  }

  bnReleaseDnodes();
  bnUnLock();

  mnodeDecVgroupRef(pVgroup);
  mnodeDecDnodeRef(pDestDnode);

  return code;
}