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
#include "tstep.h"
#include "vnodeStatus.h"
#include "vnodeRead.h"
#include "vnodeWrite.h"
#include "vnodeMain.h"

static SHashObj *tsVnodesHash = NULL;

static int32_t vnodeInitHash(void);
static void    vnodeCleanupHash(void);
static void    vnodeIncRef(void *ptNode);

static SStep tsVnodeSteps[] = {
  {"vsync",  syncInit,            syncCleanUp},
  {"vwrite", vnodeInitWrite,      vnodeCleanupWrite},
  {"vread",  vnodeInitRead,       vnodeCleanupRead},
  {"vhash",  vnodeInitHash,       vnodeCleanupHash},
  {"vqueue", tsdbInitCommitQueue, tsdbDestroyCommitQueue}
};

int32_t vnodeInitMgmt() {
  int32_t stepSize = sizeof(tsVnodeSteps) / sizeof(SStep);
  return taosStepInit(tsVnodeSteps, stepSize);
}

void vnodeCleanupMgmt() {
  int32_t stepSize = sizeof(tsVnodeSteps) / sizeof(SStep);
  taosStepCleanup(tsVnodeSteps, stepSize);
}

static int32_t vnodeInitHash() {
  tsVnodesHash = taosHashInit(TSDB_MIN_VNODES, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (tsVnodesHash == NULL) {
    vError("failed to init vnode mgmt");
    return -1;
  }

  return 0;
}

static void vnodeCleanupHash() {
  if (tsVnodesHash != NULL) {
    vDebug("vnode mgmt is cleanup");
    taosHashCleanup(tsVnodesHash);
    tsVnodesHash = NULL;
  }
}

void *vnodeGetWal(void *pVnode) {
  return ((SVnodeObj *)pVnode)->wal;
}

void vnodeAddIntoHash(SVnodeObj *pVnode) {
  taosHashPut(tsVnodesHash, &pVnode->vgId, sizeof(int32_t), &pVnode, sizeof(SVnodeObj *));
}

void vnodeRemoveFromHash(SVnodeObj *pVnode) { 
  taosHashRemove(tsVnodesHash, &pVnode->vgId, sizeof(int32_t));
}

static void vnodeIncRef(void *ptNode) {
  assert(ptNode != NULL);

  SVnodeObj **ppVnode = (SVnodeObj **)ptNode;
  assert(ppVnode);
  assert(*ppVnode);

  SVnodeObj *pVnode = *ppVnode;
  atomic_add_fetch_32(&pVnode->refCount, 1);
  vTrace("vgId:%d, get vnode, refCount:%d pVnode:%p", pVnode->vgId, pVnode->refCount, pVnode);
}

void *vnodeAcquire(int32_t vgId) {
  SVnodeObj **ppVnode = taosHashGetCB(tsVnodesHash, &vgId, sizeof(int32_t), vnodeIncRef, NULL, sizeof(void *));

  if (ppVnode == NULL || *ppVnode == NULL) {
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
    vDebug("vgId:%d, not exist", vgId);
    return NULL;
  }

  return *ppVnode;
}

void vnodeRelease(void *vparam) {
  SVnodeObj *pVnode = vparam;
  if (vparam == NULL) return;

  int32_t refCount = atomic_sub_fetch_32(&pVnode->refCount, 1);
  vTrace("vgId:%d, release vnode, refCount:%d pVnode:%p", pVnode->vgId, refCount, pVnode);
  assert(refCount >= 0);

  if (refCount > 0) {
    if (vnodeInResetStatus(pVnode) && refCount <= 3) {
      tsem_post(&pVnode->sem);
    }
  } else {
    vDebug("vgId:%d, vnode will be destroyed, refCount:%d pVnode:%p", pVnode->vgId, refCount, pVnode);
    vnodeDestroy(pVnode);
    int32_t count = taosHashGetSize(tsVnodesHash);
    vDebug("vgId:%d, vnode is destroyed, vnodes:%d", pVnode->vgId, count);
  }
}

static void vnodeBuildVloadMsg(SVnodeObj *pVnode, SStatusMsg *pStatus) {
  int64_t totalStorage = 0;
  int64_t compStorage = 0;
  int64_t pointsWritten = 0;

  if (!vnodeInReadyStatus(pVnode)) return;
  if (pStatus->openVnodes >= TSDB_MAX_VNODES) return;

  if (pVnode->tsdb) {
    tsdbReportStat(pVnode->tsdb, &pointsWritten, &totalStorage, &compStorage);
  }

  SVnodeLoad *pLoad = &pStatus->load[pStatus->openVnodes++];
  pLoad->vgId = htonl(pVnode->vgId);
  pLoad->cfgVersion = htonl(pVnode->cfgVersion);
  pLoad->totalStorage = htobe64(totalStorage);
  pLoad->compStorage = htobe64(compStorage);
  pLoad->pointsWritten = htobe64(pointsWritten);
  pLoad->status = pVnode->status;
  pLoad->role = pVnode->role;
  pLoad->replica = pVnode->syncCfg.replica;  
}

int32_t vnodeGetVnodeList(int32_t vnodeList[], int32_t *numOfVnodes) {
  void *pIter = taosHashIterate(tsVnodesHash, NULL);
  while (pIter) {
    SVnodeObj **pVnode = pIter;
    if (*pVnode) {

    (*numOfVnodes)++;
    if (*numOfVnodes >= TSDB_MAX_VNODES) {
      vError("vgId:%d, too many open vnodes, exist:%d max:%d", (*pVnode)->vgId, *numOfVnodes, TSDB_MAX_VNODES);
      continue;
    } else {
      vnodeList[*numOfVnodes - 1] = (*pVnode)->vgId;
    }

    }

    pIter = taosHashIterate(tsVnodesHash, pIter);    
  }
  return TSDB_CODE_SUCCESS;
}

void vnodeBuildStatusMsg(void *param) {
  SStatusMsg *pStatus = param;

  void *pIter = taosHashIterate(tsVnodesHash, NULL);
  while (pIter) {
    SVnodeObj **pVnode = pIter;
    if (*pVnode) {
      vnodeBuildVloadMsg(*pVnode, pStatus);
    }
    pIter = taosHashIterate(tsVnodesHash, pIter);
  }
}

void vnodeSetAccess(SVgroupAccess *pAccess, int32_t numOfVnodes) {
  for (int32_t i = 0; i < numOfVnodes; ++i) {
    pAccess[i].vgId = htonl(pAccess[i].vgId);
    SVnodeObj *pVnode = vnodeAcquire(pAccess[i].vgId);
    if (pVnode != NULL) {
      pVnode->accessState = pAccess[i].accessState;
      if (pVnode->accessState != TSDB_VN_ALL_ACCCESS) {
        vDebug("vgId:%d, access state is set to %d", pAccess[i].vgId, pVnode->accessState);
      }
      vnodeRelease(pVnode);
    }
  }
}
