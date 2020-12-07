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
#include "taosmsg.h"
#include "tglobal.h"
// #include "tfs.h"
#include "query.h"
#include "dnode.h"
#include "vnodeCfg.h"
#include "vnodeStatus.h"
#include "vnodeSync.h"
#include "vnodeVersion.h"
#include "vnodeCancel.h"

static SHashObj*tsVnodesHash;
static void     vnodeCleanUp(SVnodeObj *pVnode);
static int32_t  vnodeProcessTsdbStatus(void *arg, int32_t status, int32_t eno);
static uint32_t vnodeGetFileInfo(int32_t vgId, char *name, uint32_t *index, uint32_t eindex, int64_t *size, uint64_t *fversion);
static int32_t  vnodeGetWalInfo(int32_t vgId, char *fileName, int64_t *fileId);
static void     vnodeNotifyRole(int32_t vgId, int8_t role);
static void     vnodeCtrlFlow(int32_t vgId, int32_t level);
static int32_t  vnodeNotifyFileSynced(int32_t vgId, uint64_t fversion);
static void     vnodeConfirmForard(int32_t vgId, void *wparam, int32_t code);
static int32_t  vnodeWriteToCache(int32_t vgId, void *wparam, int32_t qtype, void *rparam);
static int32_t  vnodeGetVersion(int32_t vgId, uint64_t *fver, uint64_t *wver);

#ifndef _SYNC
int64_t syncStart(const SSyncInfo *info) { return NULL; }
int32_t syncForwardToPeer(int64_t rid, void *pHead, void *mhandle, int32_t qtype) { return 0; }
void    syncStop(int64_t rid) {}
int32_t syncReconfig(int64_t rid, const SSyncCfg *cfg) { return 0; }
int32_t syncGetNodesRole(int64_t rid, SNodesRole *cfg) { return 0; }
void    syncConfirmForward(int64_t rid, uint64_t version, int32_t code) {}
#endif

char* vnodeStatus[] = {
  "init",
  "ready",
  "closing",
  "updating",
  "reset"
};

int32_t vnodeInitResources() {
  int32_t code = syncInit();
  if (code != 0) return code;

  vnodeInitWriteFp();
  vnodeInitReadFp();
  vnodeInitCWorker();

  tsVnodesHash = taosHashInit(TSDB_MIN_VNODES, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (tsVnodesHash == NULL) {
    vError("failed to init vnode list");
    return TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  if (tsdbInitCommitQueue(tsNumOfCommitThreads) < 0) {
    vError("failed to init vnode commit queue");
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

void vnodeCleanupResources() {
  vnodeCleanupCWorker();
  tsdbDestroyCommitQueue();

  if (tsVnodesHash != NULL) {
    vDebug("vnode list is cleanup");
    taosHashCleanup(tsVnodesHash);
    tsVnodesHash = NULL;
  }

  syncCleanUp();
}

int32_t vnodeCreate(SCreateVnodeMsg *pVnodeCfg) {
  int32_t code;

  SVnodeObj *pVnode = vnodeAcquire(pVnodeCfg->cfg.vgId);
  if (pVnode != NULL) {
    vDebug("vgId:%d, vnode already exist, refCount:%d pVnode:%p", pVnodeCfg->cfg.vgId, pVnode->refCount, pVnode);
    vnodeRelease(pVnode);
    return TSDB_CODE_SUCCESS;
  }

  if (mkdir(tsVnodeDir, 0755) != 0 && errno != EEXIST) {
    vError("vgId:%d, failed to create vnode, reason:%s dir:%s", pVnodeCfg->cfg.vgId, strerror(errno), tsVnodeDir);
    if (errno == EACCES) {
      return TSDB_CODE_VND_NO_DISK_PERMISSIONS;
    } else if (errno == ENOSPC) {
      return TSDB_CODE_VND_NO_DISKSPACE;
    } else if (errno == ENOENT) {
      return TSDB_CODE_VND_NO_SUCH_FILE_OR_DIR;
    } else {
      return TSDB_CODE_VND_INIT_FAILED;
    }
  }

  char rootDir[TSDB_FILENAME_LEN] = {0};
  sprintf(rootDir, "%s/vnode%d", tsVnodeDir, pVnodeCfg->cfg.vgId);
  if (mkdir(rootDir, 0755) != 0 && errno != EEXIST) {
    vError("vgId:%d, failed to create vnode, reason:%s dir:%s", pVnodeCfg->cfg.vgId, strerror(errno), rootDir);
    if (errno == EACCES) {
      return TSDB_CODE_VND_NO_DISK_PERMISSIONS;
    } else if (errno == ENOSPC) {
      return TSDB_CODE_VND_NO_DISKSPACE;
    } else if (errno == ENOENT) {
      return TSDB_CODE_VND_NO_SUCH_FILE_OR_DIR;
    } else {
      return TSDB_CODE_VND_INIT_FAILED;
    }
  }

  code = vnodeWriteCfg(pVnodeCfg);
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to save vnode cfg, reason:%s", pVnodeCfg->cfg.vgId, tstrerror(code));
    return code;
  }

  STsdbCfg tsdbCfg = {0};
  tsdbCfg.tsdbId              = pVnodeCfg->cfg.vgId;
  tsdbCfg.cacheBlockSize      = pVnodeCfg->cfg.cacheBlockSize;
  tsdbCfg.totalBlocks         = pVnodeCfg->cfg.totalBlocks;
  tsdbCfg.daysPerFile         = pVnodeCfg->cfg.daysPerFile;
  tsdbCfg.keep                = pVnodeCfg->cfg.daysToKeep;
  tsdbCfg.minRowsPerFileBlock = pVnodeCfg->cfg.minRowsPerFileBlock;
  tsdbCfg.maxRowsPerFileBlock = pVnodeCfg->cfg.maxRowsPerFileBlock;
  tsdbCfg.precision           = pVnodeCfg->cfg.precision;
  tsdbCfg.compression         = pVnodeCfg->cfg.compression;
  tsdbCfg.update              = pVnodeCfg->cfg.update;

  char tsdbDir[TSDB_FILENAME_LEN] = {0};
  sprintf(tsdbDir, "%s/vnode%d/tsdb", tsVnodeDir, pVnodeCfg->cfg.vgId);
  if (tsdbCreateRepo(tsdbDir, &tsdbCfg) < 0) {
    vError("vgId:%d, failed to create tsdb in vnode, reason:%s", pVnodeCfg->cfg.vgId, tstrerror(terrno));
    return TSDB_CODE_VND_INIT_FAILED;
  }

  vInfo("vgId:%d, vnode dir is created, walLevel:%d fsyncPeriod:%d", pVnodeCfg->cfg.vgId, pVnodeCfg->cfg.walLevel,
        pVnodeCfg->cfg.fsyncPeriod);
  code = vnodeOpen(pVnodeCfg->cfg.vgId);

  return code;
}

int32_t vnodeDrop(int32_t vgId) {
  SVnodeObj *pVnode = vnodeAcquire(vgId);
  if (pVnode == NULL) {
    vDebug("vgId:%d, failed to drop, vnode not find", vgId);
    return TSDB_CODE_VND_INVALID_VGROUP_ID;
  }

  vInfo("vgId:%d, vnode will be dropped, refCount:%d pVnode:%p", pVnode->vgId, pVnode->refCount, pVnode);
  pVnode->dropped = 1;

  vnodeRelease(pVnode);
  vnodeCleanUp(pVnode);

  return TSDB_CODE_SUCCESS;
}

static int32_t vnodeAlterImp(SVnodeObj *pVnode, SCreateVnodeMsg *pVnodeCfg) {
  int32_t code = vnodeWriteCfg(pVnodeCfg);
  if (code != TSDB_CODE_SUCCESS) {
    return code; 
  }

  code = vnodeReadCfg(pVnode);
  if (code != TSDB_CODE_SUCCESS) {
    return code; 
  }

  code = walAlter(pVnode->wal, &pVnode->walCfg);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = syncReconfig(pVnode->sync, &pVnode->syncCfg);
  if (code != TSDB_CODE_SUCCESS) {
    return code; 
  }

  if (pVnode->tsdb) {
    code = tsdbConfigRepo(pVnode->tsdb, &pVnode->tsdbCfg);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return 0;
}

int32_t vnodeAlter(void *vparam, SCreateVnodeMsg *pVnodeCfg) {
  SVnodeObj *pVnode = vparam;

  // vnode in non-ready state and still needs to return success instead of TSDB_CODE_VND_INVALID_STATUS
  // cfgVersion can be corrected by status msg
  if (!vnodeSetUpdatingStatus(pVnode)) {
    vDebug("vgId:%d, vnode is not ready, do alter operation later", pVnode->vgId);
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = vnodeAlterImp(pVnode, pVnodeCfg);
  vnodeSetReadyStatus(pVnode);

  if (code != 0) {
    vError("vgId:%d, failed to alter vnode, code:0x%x", pVnode->vgId, code);
  } else {
    vDebug("vgId:%d, vnode is altered", pVnode->vgId);
  }

  return code;
}

int32_t vnodeOpen(int32_t vgId) {
  char temp[TSDB_FILENAME_LEN * 3];
  char rootDir[TSDB_FILENAME_LEN * 2];
  snprintf(rootDir, TSDB_FILENAME_LEN * 2, "%s/vnode%d", tsVnodeDir, vgId);

  SVnodeObj *pVnode = calloc(sizeof(SVnodeObj), 1);
  if (pVnode == NULL) {
    vError("vgId:%d, failed to open vnode since no enough memory", vgId);
    return TAOS_SYSTEM_ERROR(errno);
  }

  atomic_add_fetch_32(&pVnode->refCount, 1);

  pVnode->vgId     = vgId;
  pVnode->fversion = 0;
  pVnode->version  = 0;  
  pVnode->tsdbCfg.tsdbId = pVnode->vgId;
  pVnode->rootDir = strdup(rootDir);
  pVnode->accessState = TSDB_VN_ALL_ACCCESS;
  tsem_init(&pVnode->sem, 0, 0);
  pthread_mutex_init(&pVnode->statusMutex, NULL);
  vnodeSetInitStatus(pVnode);

  int32_t code = vnodeReadCfg(pVnode);
  if (code != TSDB_CODE_SUCCESS) {
    vnodeCleanUp(pVnode);
    return code;
  } 

  code = vnodeReadVersion(pVnode);
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to read version, generate it from data file", pVnode->vgId);
    // Allow vnode start even when read version fails, set version as walVersion or zero
    // vnodeCleanUp(pVnode);
    // return code;
  }

  pVnode->fversion = pVnode->version;
  
  pVnode->wqueue = dnodeAllocVWriteQueue(pVnode);
  pVnode->rqueue = dnodeAllocVReadQueue(pVnode);
  if (pVnode->wqueue == NULL || pVnode->rqueue == NULL) {
    vnodeCleanUp(pVnode);
    return terrno;
  }

  if (tsEnableStream) {
    SCqCfg cqCfg = {0};
    sprintf(cqCfg.user, "_root");
    strcpy(cqCfg.pass, tsInternalPass);
    strcpy(cqCfg.db, pVnode->db);
    cqCfg.vgId = vgId;
    cqCfg.cqWrite = vnodeWriteToCache;
    pVnode->cq = cqOpen(pVnode, &cqCfg);
    if (pVnode->cq == NULL) {
      vnodeCleanUp(pVnode);
      return terrno;
    }
  }

  STsdbAppH appH = {0};
  appH.appH = (void *)pVnode;
  appH.notifyStatus = vnodeProcessTsdbStatus;
  appH.cqH = pVnode->cq;
  appH.cqCreateFunc = cqCreate;
  appH.cqDropFunc = cqDrop;
  sprintf(temp, "%s/tsdb", rootDir);

  terrno = 0;
  pVnode->tsdb = tsdbOpenRepo(temp, &appH);
  if (pVnode->tsdb == NULL) {
    vnodeCleanUp(pVnode);
    return terrno;
  } else if (terrno != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to open tsdb, replica:%d reason:%s", pVnode->vgId, pVnode->syncCfg.replica,
           tstrerror(terrno));
    if (pVnode->syncCfg.replica <= 1) {
      vnodeCleanUp(pVnode);
      return terrno;
    } else {
      pVnode->fversion = 0;
      pVnode->version = 0;
    }
  }

  sprintf(temp, "%s/wal", rootDir);
  pVnode->walCfg.vgId = pVnode->vgId;
  pVnode->wal = walOpen(temp, &pVnode->walCfg);
  if (pVnode->wal == NULL) { 
    vnodeCleanUp(pVnode);
    return terrno;
  }

  walRestore(pVnode->wal, pVnode, vnodeProcessWrite);
  if (pVnode->version == 0) {
    pVnode->fversion = 0;
    pVnode->version = walGetVersion(pVnode->wal);
  }

  code = tsdbSyncCommit(pVnode->tsdb);
  if (code != 0) {
    vError("vgId:%d, failed to commit after restore from wal since %s", pVnode->vgId, tstrerror(code));
    vnodeCleanUp(pVnode);
    return code;
  }

  walRemoveAllOldFiles(pVnode->wal);
  walRenew(pVnode->wal);

  pVnode->qMgmt = qOpenQueryMgmt(pVnode->vgId);
  if (pVnode->qMgmt == NULL) {
    vnodeCleanUp(pVnode);
    return terrno;
  }

  pVnode->events = NULL;

  vDebug("vgId:%d, vnode is opened in %s, pVnode:%p", pVnode->vgId, rootDir, pVnode);
  tsdbIncCommitRef(pVnode->vgId);

  vnodeAddIntoHash(pVnode);

  SSyncInfo syncInfo;
  syncInfo.vgId = pVnode->vgId;
  syncInfo.version = pVnode->version;
  syncInfo.syncCfg = pVnode->syncCfg;
  tstrncpy(syncInfo.path, rootDir, TSDB_FILENAME_LEN);
  syncInfo.getWalInfo = vnodeGetWalInfo;
  syncInfo.getFileInfo = vnodeGetFileInfo;
  syncInfo.writeToCache = vnodeWriteToCache;
  syncInfo.confirmForward = vnodeConfirmForard; 
  syncInfo.notifyRole = vnodeNotifyRole;
  syncInfo.notifyFlowCtrl = vnodeCtrlFlow;
  syncInfo.notifyFileSynced = vnodeNotifyFileSynced;
  syncInfo.getVersion = vnodeGetVersion;
  pVnode->sync = syncStart(&syncInfo);

  if (pVnode->sync <= 0) {
    vError("vgId:%d, failed to open sync, replica:%d reason:%s", pVnode->vgId, pVnode->syncCfg.replica,
           tstrerror(terrno));
    vnodeCleanUp(pVnode);
    return terrno;
  }

  vnodeSetReadyStatus(pVnode);
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeClose(int32_t vgId) {
  SVnodeObj *pVnode = vnodeAcquire(vgId);
  if (pVnode == NULL) return 0;

  vDebug("vgId:%d, vnode will be closed, pVnode:%p", pVnode->vgId, pVnode);
  vnodeRelease(pVnode);
  vnodeCleanUp(pVnode);

  return 0;
}

void vnodeDestroy(SVnodeObj *pVnode) {
  int32_t code = 0;
  int32_t vgId = pVnode->vgId;

  if (pVnode->qMgmt) {
    qCleanupQueryMgmt(pVnode->qMgmt);
    pVnode->qMgmt = NULL;
  }

  if (pVnode->wal) {
    walStop(pVnode->wal);
  }

  if (pVnode->tsdb) {
    code = tsdbCloseRepo(pVnode->tsdb, 1);
    pVnode->tsdb = NULL;
  }

  // stop continuous query
  if (pVnode->cq) {
    void *cq = pVnode->cq;
    pVnode->cq = NULL;
    cqClose(cq);
  }

  if (pVnode->wal) {
    if (code != 0) {
      vError("vgId:%d, failed to commit while close tsdb repo, keep wal", pVnode->vgId);
    } else {
      walRemoveAllOldFiles(pVnode->wal);
    }
    walClose(pVnode->wal);
    pVnode->wal = NULL;
  }

  if (pVnode->wqueue) {
    dnodeFreeVWriteQueue(pVnode->wqueue);
    pVnode->wqueue = NULL;
  }

  if (pVnode->rqueue) {
    dnodeFreeVReadQueue(pVnode->rqueue);
    pVnode->rqueue = NULL;
  }

  tfree(pVnode->rootDir);

  if (pVnode->dropped) {
    char rootDir[TSDB_FILENAME_LEN] = {0};    
    char newDir[TSDB_FILENAME_LEN] = {0};
    sprintf(rootDir, "%s/vnode%d", tsVnodeDir, vgId);
    sprintf(newDir, "%s/vnode%d", tsVnodeBakDir, vgId);

    if (0 == tsEnableVnodeBak) {
      vInfo("vgId:%d, vnode backup not enabled", pVnode->vgId);
    } else {
      taosRemoveDir(newDir);
      taosRename(rootDir, newDir);
    }

    taosRemoveDir(rootDir);
    dnodeSendStatusMsgToMnode();
  }

  tsem_destroy(&pVnode->sem);
  pthread_mutex_destroy(&pVnode->statusMutex);
  free(pVnode);
  tsdbDecCommitRef(vgId);
}


static void vnodeCleanUp(SVnodeObj *pVnode) {
  // remove from hash, so new messages wont be consumed
  vnodeRemoveFromHash(pVnode);

  if (!vnodeInInitStatus(pVnode)) {
    // it may be in updateing or reset state, then it shall wait
    int32_t i = 0;
    while (!vnodeSetClosingStatus(pVnode)) {
      if (++i % 1000 == 0) {
        sched_yield();
      }
    }
  }

  // stop replication module
  if (pVnode->sync > 0) {
    int64_t sync = pVnode->sync;
    pVnode->sync = -1;
    syncStop(sync);
  }

  vDebug("vgId:%d, vnode will cleanup, refCount:%d pVnode:%p", pVnode->vgId, pVnode->refCount, pVnode);

  // release local resources only after cutting off outside connections
  qQueryMgmtNotifyClosed(pVnode->qMgmt);
  vnodeRelease(pVnode);
}

static int32_t vnodeProcessTsdbStatus(void *arg, int32_t status, int32_t eno) {
  SVnodeObj *pVnode = arg;

  if (eno != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to commit since %s, fver:%" PRIu64 " vver:%" PRIu64, pVnode->vgId, tstrerror(eno),
           pVnode->fversion, pVnode->version);
    pVnode->isCommiting = 0;
    pVnode->isFull = 1;
    return 0;
  }

  if (status == TSDB_STATUS_COMMIT_START) {
    pVnode->isCommiting = 1;
    pVnode->fversion = pVnode->version;
    vDebug("vgId:%d, start commit, fver:%" PRIu64 " vver:%" PRIu64, pVnode->vgId, pVnode->fversion, pVnode->version);
    if (!vnodeInInitStatus(pVnode)) {
      return walRenew(pVnode->wal);
    }
    return 0;
  }

  if (status == TSDB_STATUS_COMMIT_OVER) {
    vDebug("vgId:%d, commit over, fver:%" PRIu64 " vver:%" PRIu64, pVnode->vgId, pVnode->fversion, pVnode->version);
    pVnode->isCommiting = 0;
    pVnode->isFull = 0;
    if (!vnodeInInitStatus(pVnode)) {
      walRemoveOneOldFile(pVnode->wal);
    }
    return vnodeSaveVersion(pVnode);
  }

  return 0;
}

int32_t vnodeReset(SVnodeObj *pVnode) {
  char rootDir[128] = "\0";
  sprintf(rootDir, "%s/tsdb", pVnode->rootDir);

  if (!vnodeSetResetStatus(pVnode)) {
    return -1;
  }

  void *tsdb = pVnode->tsdb;
  pVnode->tsdb = NULL;
  
  // acquire vnode
  int32_t refCount = atomic_add_fetch_32(&pVnode->refCount, 1);

  if (refCount > 3) {
    tsem_wait(&pVnode->sem);
  }

  // close tsdb, then open tsdb
  tsdbCloseRepo(tsdb, 0);
  STsdbAppH appH = {0};
  appH.appH = (void *)pVnode;
  appH.notifyStatus = vnodeProcessTsdbStatus;
  appH.cqH = pVnode->cq;
  appH.cqCreateFunc = cqCreate;
  appH.cqDropFunc = cqDrop;
  pVnode->tsdb = tsdbOpenRepo(rootDir, &appH);

  vnodeSetReadyStatus(pVnode);
  vnodeRelease(pVnode);

  return 0;
}
