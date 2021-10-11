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
#include "ttimer.h"
#include "thash.h"
// #include "query.h"
#include "vnodeCfg.h"
#include "vnodeMain.h"
#include "vnodeMgmt.h"
#include "vnodeRead.h"
#include "vnodeStatus.h"
#include "vnodeVersion.h"
#include "vnodeWorker.h"
#include "vnodeWrite.h"

typedef struct {
  pthread_t thread;
  int32_t   threadIndex;
  int32_t   failed;
  int32_t   opened;
  int32_t   vnodeNum;
  int32_t * vnodeList;
} SOpenVnodeThread;

static struct {
  void *    timer;
  SHashObj *hash;
  int32_t   openVnodes;
  int32_t   totalVnodes;
  void (*msgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *);
} tsVmain;

static void vnodeIncRef(void *ptNode) {
  assert(ptNode != NULL);

  SVnode **ppVnode = (SVnode **)ptNode;
  assert(ppVnode);
  assert(*ppVnode);

  SVnode *pVnode = *ppVnode;
  atomic_add_fetch_32(&pVnode->refCount, 1);
  vTrace("vgId:%d, get vnode, refCount:%d pVnode:%p", pVnode->vgId, pVnode->refCount, pVnode);
}

SVnode *vnodeAcquire(int32_t vgId) {
  SVnode *pVnode = NULL;

#if 0
  taosHashGetClone(tsVmain.hash, &vgId, sizeof(int32_t), vnodeIncRef, &pVnode);
#endif 
  if (pVnode == NULL) {
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
    vDebug("vgId:%d, not exist", vgId);
    return NULL;
  }

  return pVnode;
}

SVnode *vnodeAcquireNotClose(int32_t vgId) {
  SVnode *pVnode = vnodeAcquire(vgId);
  if (pVnode != NULL && pVnode->preClose == 1) {
    vnodeRelease(pVnode);
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
    vDebug("vgId:%d, not exist, pre closing", vgId);
    return NULL;
  }

  return pVnode;
}

void vnodeRelease(SVnode *pVnode) {
  if (pVnode == NULL) return;

  int32_t refCount = atomic_sub_fetch_32(&pVnode->refCount, 1);
  int32_t vgId = pVnode->vgId;

  vTrace("vgId:%d, release vnode, refCount:%d pVnode:%p", vgId, refCount, pVnode);
  assert(refCount >= 0);

  if (refCount <= 0) {
    vDebug("vgId:%d, vnode will be destroyed, refCount:%d pVnode:%p", vgId, refCount, pVnode);
    vnodeProcessDestroyTask(pVnode);
    int32_t count = taosHashGetSize(tsVmain.hash);
    vDebug("vgId:%d, vnode is destroyed, vnodes:%d", vgId, count);
  }
}

static int32_t vnodeProcessTsdbStatus(void *arg, int32_t status, int32_t eno);

int32_t vnodeCreate(SCreateVnodeMsg *pVnodeCfg) {
  int32_t code;

  SVnode *pVnode = vnodeAcquire(pVnodeCfg->cfg.vgId);
  if (pVnode != NULL) {
    vDebug("vgId:%d, vnode already exist, refCount:%d pVnode:%p", pVnodeCfg->cfg.vgId, pVnode->refCount, pVnode);
    vnodeRelease(pVnode);
    return TSDB_CODE_SUCCESS;
  }

#if 0
  if (tfsMkdir("vnode") < 0) {
    vError("vgId:%d, failed to create vnode dir, reason:%s", pVnodeCfg->cfg.vgId, tstrerror(terrno));
    return terrno;
  }

  char vnodeDir[TSDB_FILENAME_LEN] = "\0";
  snprintf(vnodeDir, TSDB_FILENAME_LEN, "/vnode/vnode%d", pVnodeCfg->cfg.vgId);
  if (tfsMkdir(vnodeDir) < 0) {
    vError("vgId:%d, failed to create vnode dir %s, reason:%s", pVnodeCfg->cfg.vgId, vnodeDir, strerror(errno));
    return terrno;
  }

  code = vnodeWriteCfg(pVnodeCfg);
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to save vnode cfg, reason:%s", pVnodeCfg->cfg.vgId, tstrerror(code));
    return code;
  }

  if (tsdbCreateRepo(pVnodeCfg->cfg.vgId) < 0) {
    vError("vgId:%d, failed to create tsdb in vnode, reason:%s", pVnodeCfg->cfg.vgId, tstrerror(terrno));
    return TSDB_CODE_VND_INIT_FAILED;
  }
#endif
  vInfo("vgId:%d, vnode dir is created, walLevel:%d fsyncPeriod:%d", pVnodeCfg->cfg.vgId, pVnodeCfg->cfg.walLevel,
        pVnodeCfg->cfg.fsyncPeriod);
  code = vnodeOpen(pVnodeCfg->cfg.vgId);

  return code;
}

int32_t vnodeSync(int32_t vgId) {
#if 0  
  SVnode *pVnode = vnodeAcquireNotClose(vgId);
  if (pVnode == NULL) {
    vDebug("vgId:%d, failed to sync, vnode not find", vgId);
    return TSDB_CODE_VND_INVALID_VGROUP_ID;
  }

  if (pVnode->role == TAOS_SYNC_ROLE_SLAVE) {
    vInfo("vgId:%d, vnode will sync, refCount:%d pVnode:%p", pVnode->vgId, pVnode->refCount, pVnode);

    pVnode->version = 0;
    pVnode->fversion = 0;
    walResetVersion(pVnode->wal, pVnode->fversion);

    syncRecover(pVnode->sync);
  }

  vnodeRelease(pVnode);
#endif
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeDrop(int32_t vgId) {
  SVnode *pVnode = vnodeAcquireNotClose(vgId);
  if (pVnode == NULL) {
    vDebug("vgId:%d, failed to drop, vnode not find", vgId);
    return TSDB_CODE_VND_INVALID_VGROUP_ID;
  }
  if (pVnode->dropped) {
    vnodeRelease(pVnode);
    return TSDB_CODE_SUCCESS;
  }

  vInfo("vgId:%d, vnode will be dropped, refCount:%d pVnode:%p", pVnode->vgId, pVnode->refCount, pVnode);
  pVnode->dropped = 1;

  vnodeRelease(pVnode);
  vnodeProcessCleanupTask(pVnode);

  return TSDB_CODE_SUCCESS;
}

int32_t vnodeCompact(int32_t vgId) {
#if 0  
  SVnode *pVnode = vnodeAcquire(vgId);
  if (pVnode != NULL) {
    vDebug("vgId:%d, compact vnode msg is received", vgId);
    // not care success or not
    tsdbCompact(((SVnode *)pVnode)->tsdb);
    vnodeRelease(pVnode);
  } else {
    vInfo("vgId:%d, vnode not exist, can't compact it", vgId);
    return TSDB_CODE_VND_INVALID_VGROUP_ID;
  }
#endif   
  return TSDB_CODE_SUCCESS;
}

static int32_t vnodeAlterImp(SVnode *pVnode, SCreateVnodeMsg *pVnodeCfg) {
#if 0  
  STsdbCfg tsdbCfg = pVnode->tsdbCfg;
  SSyncCfg syncCfg = pVnode->syncCfg;
  int32_t  dbCfgVersion = pVnode->dbCfgVersion;
  int32_t  vgCfgVersion = pVnode->vgCfgVersion;

  int32_t code = vnodeWriteCfg(pVnodeCfg);
  if (code != TSDB_CODE_SUCCESS) {
    pVnode->dbCfgVersion = dbCfgVersion;
    pVnode->vgCfgVersion = vgCfgVersion;
    pVnode->syncCfg = syncCfg;
    pVnode->tsdbCfg = tsdbCfg;
    return code;
  }

  code = vnodeReadCfg(pVnode);
  if (code != TSDB_CODE_SUCCESS) {
    pVnode->dbCfgVersion = dbCfgVersion;
    pVnode->vgCfgVersion = vgCfgVersion;
    pVnode->syncCfg = syncCfg;
    pVnode->tsdbCfg = tsdbCfg;
    return code;
  }

  code = walAlter(pVnode->wal, &pVnode->walCfg);
  if (code != TSDB_CODE_SUCCESS) {
    pVnode->dbCfgVersion = dbCfgVersion;
    pVnode->vgCfgVersion = vgCfgVersion;
    pVnode->syncCfg = syncCfg;
    pVnode->tsdbCfg = tsdbCfg;
    return code;
  }

  bool tsdbCfgChanged = (memcmp(&tsdbCfg, &pVnode->tsdbCfg, sizeof(STsdbCfg)) != 0);
  bool syncCfgChanged = (memcmp(&syncCfg, &pVnode->syncCfg, sizeof(SSyncCfg)) != 0);

  vDebug("vgId:%d, tsdbchanged:%d syncchanged:%d while alter vnode", pVnode->vgId, tsdbCfgChanged, syncCfgChanged);

  if (tsdbCfgChanged || syncCfgChanged) {
    // vnode in non-ready state and still needs to return success instead of TSDB_CODE_VND_INVALID_STATUS
    // dbCfgVersion can be corrected by status msg
    if (syncCfgChanged) {
      if (!vnodeSetUpdatingStatus(pVnode)) {
        vDebug("vgId:%d, vnode is not ready, do alter operation later", pVnode->vgId);
        pVnode->dbCfgVersion = dbCfgVersion;
        pVnode->vgCfgVersion = vgCfgVersion;
        pVnode->syncCfg = syncCfg;
        pVnode->tsdbCfg = tsdbCfg;
        return TSDB_CODE_SUCCESS;
      }

      code = syncReconfig(pVnode->sync, &pVnode->syncCfg);
      if (code != TSDB_CODE_SUCCESS) {
        pVnode->dbCfgVersion = dbCfgVersion;
        pVnode->vgCfgVersion = vgCfgVersion;
        pVnode->syncCfg = syncCfg;
        pVnode->tsdbCfg = tsdbCfg;
        vnodeSetReadyStatus(pVnode);
        return code;
      }
    }

    if (tsdbCfgChanged && pVnode->tsdb) {
      code = tsdbConfigRepo(pVnode->tsdb, &pVnode->tsdbCfg);
      if (code != TSDB_CODE_SUCCESS) {
        pVnode->dbCfgVersion = dbCfgVersion;
        pVnode->vgCfgVersion = vgCfgVersion;
        pVnode->syncCfg = syncCfg;
        pVnode->tsdbCfg = tsdbCfg;
        vnodeSetReadyStatus(pVnode);
        return code;
      }
    }

    vnodeSetReadyStatus(pVnode);
  }
#endif
  return 0;
}

int32_t vnodeAlter(SVnode *pVnode, SCreateVnodeMsg *pVnodeCfg) {
  vDebug("vgId:%d, current dbCfgVersion:%d vgCfgVersion:%d, input dbCfgVersion:%d vgCfgVersion:%d", pVnode->vgId,
         pVnode->dbCfgVersion, pVnode->vgCfgVersion, pVnodeCfg->cfg.dbCfgVersion, pVnodeCfg->cfg.vgCfgVersion);

  if (pVnode->dbCfgVersion == pVnodeCfg->cfg.dbCfgVersion && pVnode->vgCfgVersion == pVnodeCfg->cfg.vgCfgVersion) {
    vDebug("vgId:%d, cfg not change", pVnode->vgId);
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = vnodeAlterImp(pVnode, pVnodeCfg);

  if (code != 0) {
    vError("vgId:%d, failed to alter vnode, code:0x%x", pVnode->vgId, code);
  } else {
    vDebug("vgId:%d, vnode is altered", pVnode->vgId);
  }

  return code;
}

static void vnodeFindWalRootDir(int32_t vgId, char *walRootDir) {
#if 0  
  char vnodeDir[TSDB_FILENAME_LEN] = "\0";
  snprintf(vnodeDir, TSDB_FILENAME_LEN, "/vnode/vnode%d/wal", vgId);

  TDIR *tdir = tfsOpendir(vnodeDir);
  if (!tdir) return;

  const TFILE *tfile = tfsReaddir(tdir);
  if (!tfile) {
    tfsClosedir(tdir);
    return;
  }

  sprintf(walRootDir, "%s/vnode/vnode%d", TFS_DISK_PATH(tfile->level, tfile->id), vgId);

  tfsClosedir(tdir);
#endif  
}

int32_t vnodeOpen(int32_t vgId) {
#if 0  
  char temp[TSDB_FILENAME_LEN * 3];
  char rootDir[TSDB_FILENAME_LEN * 2];
  char walRootDir[TSDB_FILENAME_LEN * 2] = {0};
  snprintf(rootDir, TSDB_FILENAME_LEN * 2, "%s/vnode%d", tsVnodeDir, vgId);

  SVnode *pVnode = calloc(sizeof(SVnode), 1);
  if (pVnode == NULL) {
    vError("vgId:%d, failed to open vnode since no enough memory", vgId);
    return TAOS_SYSTEM_ERROR(errno);
  }

  atomic_add_fetch_32(&pVnode->refCount, 1);

  pVnode->vgId = vgId;
  pVnode->fversion = 0;
  pVnode->version = 0;
  pVnode->tsdbCfg.tsdbId = pVnode->vgId;
  pVnode->rootDir = strdup(rootDir);
  pVnode->accessState = TSDB_VN_ALL_ACCCESS;
  tsem_init(&pVnode->sem, 0, 0);
  pthread_mutex_init(&pVnode->statusMutex, NULL);
  vnodeSetInitStatus(pVnode);

  tsdbIncCommitRef(pVnode->vgId);

  int32_t code = vnodeReadCfg(pVnode);
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to read config file, set cfgVersion to 0", pVnode->vgId);
    vnodeCleanUp(pVnode);
    return 0;
  }

  code = vnodeReadVersion(pVnode);
  if (code != TSDB_CODE_SUCCESS) {
    pVnode->version = 0;
    vError("vgId:%d, failed to read file version, generate it from data file", pVnode->vgId);
    // Allow vnode start even when read file version fails, set file version as wal version or zero
    // vnodeCleanUp(pVnode);
    // return code;
  }

  pVnode->fversion = pVnode->version;

  pVnode->wqueue = vnodeAllocWriteQueue(pVnode);
  pVnode->qqueue = vnodeAllocQueryQueue(pVnode);
  pVnode->fqueue = vnodeAllocFetchQueue(pVnode);
  if (pVnode->wqueue == NULL || pVnode->qqueue == NULL || pVnode->fqueue == NULL) {
    vnodeCleanUp(pVnode);
    return terrno;
  }

  STsdbAppH appH = {0};
  appH.appH = (void *)pVnode;
  appH.notifyStatus = vnodeProcessTsdbStatus;
  appH.cqH = pVnode->cq;
  appH.cqCreateFunc = cqCreate;
  appH.cqDropFunc = cqDrop;

  terrno = 0;
  pVnode->tsdb = tsdbOpenRepo(&(pVnode->tsdbCfg), &appH);
  if (pVnode->tsdb == NULL) {
    vnodeCleanUp(pVnode);
    return terrno;
  } else if (tsdbGetState(pVnode->tsdb) != TSDB_STATE_OK) {
    vError("vgId:%d, failed to open tsdb(state: %d), replica:%d reason:%s", pVnode->vgId, tsdbGetState(pVnode->tsdb),
           pVnode->syncCfg.replica, tstrerror(terrno));
    if (pVnode->syncCfg.replica <= 1) {
      vnodeCleanUp(pVnode);
      return TSDB_CODE_VND_INVALID_TSDB_STATE;
    } else {
      pVnode->fversion = 0;
      pVnode->version = 0;
    }
  }

  // walRootDir for wal & syncInfo.path (not empty dir of /vnode/vnode{pVnode->vgId}/wal)
  vnodeFindWalRootDir(pVnode->vgId, walRootDir);
  if (walRootDir[0] == 0) {
    int level = -1, id = -1;

    tfsAllocDisk(TFS_PRIMARY_LEVEL, &level, &id);
    if (level < 0 || id < 0) {
      vnodeCleanUp(pVnode);
      return terrno;
    }

    sprintf(walRootDir, "%s/vnode/vnode%d", TFS_DISK_PATH(level, id), vgId);
  }

  sprintf(temp, "%s/wal", walRootDir);
  pVnode->walCfg.vgId = pVnode->vgId;
  pVnode->wal = walOpen(temp, &pVnode->walCfg);
  if (pVnode->wal == NULL) {
    vnodeCleanUp(pVnode);
    return terrno;
  }

  walRestore(pVnode->wal, pVnode, (FWalWrite)vnodeProcessWalMsg);
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

  vDebug("vgId:%d, vnode is opened in %s - %s, pVnode:%p", pVnode->vgId, rootDir, walRootDir, pVnode);

  taosHashPut(tsVmain.hash, &pVnode->vgId, sizeof(int32_t), &pVnode, sizeof(SVnode *));

  vnodeSetReadyStatus(pVnode);
  pVnode->role = TAOS_SYNC_ROLE_MASTER;
#endif  
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeClose(int32_t vgId) {
  SVnode *pVnode = vnodeAcquireNotClose(vgId);
  if (pVnode == NULL) return 0;
  if (pVnode->dropped) {
    vnodeRelease(pVnode);
    return 0;
  }

  pVnode->preClose = 1;

  vDebug("vgId:%d, vnode will be closed, pVnode:%p", pVnode->vgId, pVnode);
  vnodeRelease(pVnode);
  vnodeCleanUp(pVnode);

  return 0;
}

void vnodeDestroy(SVnode *pVnode) {
#if 0  
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
    // the deleted vnode does not need to commit, so as to speed up the deletion
    int toCommit = 1;
    if (pVnode->dropped) toCommit = 0;

    code = tsdbCloseRepo(pVnode->tsdb, toCommit);
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
    vnodeFreeWriteQueue(pVnode->wqueue);
    pVnode->wqueue = NULL;
  }

  if (pVnode->qqueue) {
    vnodeFreeQueryQueue(pVnode->qqueue);
    pVnode->qqueue = NULL;
  }

  if (pVnode->fqueue) {
    vnodeFreeFetchQueue(pVnode->fqueue);
    pVnode->fqueue = NULL;
  }

  tfree(pVnode->rootDir);

  if (pVnode->dropped) {
    char rootDir[TSDB_FILENAME_LEN] = {0};
    char stagingDir[TSDB_FILENAME_LEN] = {0};
    sprintf(rootDir, "%s/vnode%d", "vnode", vgId);
    sprintf(stagingDir, "%s/.staging/vnode%d", "vnode_bak", vgId);

    tfsRename(rootDir, stagingDir);

    vnodeProcessBackupTask(pVnode);

    // dnodeSendStatusMsgToMnode();
  }

  tsem_destroy(&pVnode->sem);
  pthread_mutex_destroy(&pVnode->statusMutex);
  free(pVnode);
  tsdbDecCommitRef(vgId);
#endif  
}

void vnodeCleanUp(SVnode *pVnode) {
#if 0  
  vDebug("vgId:%d, vnode will cleanup, refCount:%d pVnode:%p", pVnode->vgId, pVnode->refCount, pVnode);

  vnodeSetClosingStatus(pVnode);

  taosHashRemove(tsVmain.hash, &pVnode->vgId, sizeof(int32_t));

  // stop replication module
  if (pVnode->sync > 0) {
    int64_t sync = pVnode->sync;
    pVnode->sync = -1;
    syncStop(sync);
  }

  vDebug("vgId:%d, vnode is cleaned, refCount:%d pVnode:%p", pVnode->vgId, pVnode->refCount, pVnode);
  vnodeRelease(pVnode);
#endif  
}

#if 0
static int32_t vnodeProcessTsdbStatus(void *arg, int32_t status, int32_t eno) {
  SVnode *pVnode = arg;

  if (eno != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to commit since %s, fver:%" PRIu64 " vver:%" PRIu64, pVnode->vgId, tstrerror(eno),
           pVnode->fversion, pVnode->version);
    pVnode->isCommiting = 0;
    pVnode->isFull = 1;
    return 0;
  }

  if (status == TSDB_STATUS_COMMIT_START) {
    pVnode->isCommiting = 1;
    pVnode->cversion = pVnode->version;
    vInfo("vgId:%d, start commit, fver:%" PRIu64 " vver:%" PRIu64, pVnode->vgId, pVnode->fversion, pVnode->version);
    if (!vnodeInInitStatus(pVnode)) {
      return walRenew(pVnode->wal);
    }
    return 0;
  }

  if (status == TSDB_STATUS_COMMIT_OVER) {
    pVnode->isCommiting = 0;
    pVnode->isFull = 0;
    pVnode->fversion = pVnode->cversion;
    vInfo("vgId:%d, commit over, fver:%" PRIu64 " vver:%" PRIu64, pVnode->vgId, pVnode->fversion, pVnode->version);
    if (!vnodeInInitStatus(pVnode)) {
      walRemoveOneOldFile(pVnode->wal);
    }
    return vnodeSaveVersion(pVnode);
  }

  // timer thread callback
  if (status == TSDB_STATUS_COMMIT_NOBLOCK) {
    qSolveCommitNoBlock(pVnode->tsdb, pVnode->qMgmt);
  }

  return 0;
}
#endif

static void *vnodeOpenVnode(void *param) {
  SOpenVnodeThread *pThread = param;

  vDebug("thread:%d, start to open %d vnodes", pThread->threadIndex, pThread->vnodeNum);
  setThreadName("vnodeOpenVnode");

  for (int32_t v = 0; v < pThread->vnodeNum; ++v) {
    int32_t vgId = pThread->vnodeList[v];

    char stepDesc[TSDB_STEP_DESC_LEN] = {0};
    snprintf(stepDesc, TSDB_STEP_DESC_LEN, "vgId:%d, start to restore, %d of %d have been opened", vgId,
             tsVmain.openVnodes, tsVmain.totalVnodes);
    // (*vnodeInst()->fp.ReportStartup)("open-vnodes", stepDesc);

    if (vnodeOpen(vgId) < 0) {
      vError("vgId:%d, failed to open vnode by thread:%d", vgId, pThread->threadIndex);
      pThread->failed++;
    } else {
      vDebug("vgId:%d, is opened by thread:%d", vgId, pThread->threadIndex);
      pThread->opened++;
    }

    atomic_add_fetch_32(&tsVmain.openVnodes, 1);
  }

  vDebug("thread:%d, total vnodes:%d, opened:%d failed:%d", pThread->threadIndex, pThread->vnodeNum, pThread->opened,
         pThread->failed);
  return NULL;
}

static int32_t vnodeGetVnodeListFromDisk(int32_t vnodeList[], int32_t *numOfVnodes) {
#if 0
  DIR *dir = opendir(tsVnodeDir);
  if (dir == NULL) return TSDB_CODE_DND_NO_WRITE_ACCESS;

  *numOfVnodes = 0;
  struct dirent *de = NULL;
  while ((de = readdir(dir)) != NULL) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
    if (de->d_type & DT_DIR) {
      if (strncmp("vnode", de->d_name, 5) != 0) continue;
      int32_t vnode = atoi(de->d_name + 5);
      if (vnode == 0) continue;

      (*numOfVnodes)++;

      if (*numOfVnodes >= TSDB_MAX_VNODES) {
        vError("vgId:%d, too many vnode directory in disk, exist:%d max:%d", vnode, *numOfVnodes, TSDB_MAX_VNODES);
        closedir(dir);
        return TSDB_CODE_DND_TOO_MANY_VNODES;
      } else {
        vnodeList[*numOfVnodes - 1] = vnode;
      }
    }
  }
  closedir(dir);
#endif
  return TSDB_CODE_SUCCESS;
}

static int32_t vnodeOpenVnodes() {
  int32_t vnodeList[TSDB_MAX_VNODES] = {0};
  int32_t numOfVnodes = 0;
  int32_t status = vnodeGetVnodeListFromDisk(vnodeList, &numOfVnodes);

  if (status != TSDB_CODE_SUCCESS) {
    vInfo("failed to get vnode list from disk since code:%d", status);
    return status;
  }

  tsVmain.totalVnodes = numOfVnodes;

  int32_t threadNum = tsNumOfCores;
  int32_t vnodesPerThread = numOfVnodes / threadNum + 1;

  SOpenVnodeThread *threads = calloc(threadNum, sizeof(SOpenVnodeThread));
  for (int32_t t = 0; t < threadNum; ++t) {
    threads[t].threadIndex = t;
    threads[t].vnodeList = calloc(vnodesPerThread, sizeof(int32_t));
  }

  for (int32_t v = 0; v < numOfVnodes; ++v) {
    int32_t           t = v % threadNum;
    SOpenVnodeThread *pThread = &threads[t];
    pThread->vnodeList[pThread->vnodeNum++] = vnodeList[v];
  }

  vInfo("start %d threads to open %d vnodes", threadNum, numOfVnodes);

  for (int32_t t = 0; t < threadNum; ++t) {
    SOpenVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum == 0) continue;

    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);
    if (pthread_create(&pThread->thread, &thAttr, vnodeOpenVnode, pThread) != 0) {
      vError("thread:%d, failed to create thread to open vnode, reason:%s", pThread->threadIndex, strerror(errno));
    }

    pthread_attr_destroy(&thAttr);
  }

  int32_t openVnodes = 0;
  int32_t failedVnodes = 0;
  for (int32_t t = 0; t < threadNum; ++t) {
    SOpenVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum > 0 && taosCheckPthreadValid(pThread->thread)) {
      pthread_join(pThread->thread, NULL);
    }
    openVnodes += pThread->opened;
    failedVnodes += pThread->failed;
    free(pThread->vnodeList);
  }

  free(threads);
  vInfo("there are total vnodes:%d, opened:%d", numOfVnodes, openVnodes);

  if (failedVnodes != 0) {
    vError("there are total vnodes:%d, failed:%d", numOfVnodes, failedVnodes);
    return -1;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t vnodeGetVnodeList(int32_t vnodeList[], int32_t *numOfVnodes) {
  void *pIter = taosHashIterate(tsVmain.hash, NULL);
  while (pIter) {
    SVnode **pVnode = pIter;
    if (*pVnode) {
      (*numOfVnodes)++;
      if (*numOfVnodes >= TSDB_MAX_VNODES) {
        vError("vgId:%d, too many open vnodes, exist:%d max:%d", (*pVnode)->vgId, *numOfVnodes, TSDB_MAX_VNODES);
        continue;
      } else {
        vnodeList[*numOfVnodes - 1] = (*pVnode)->vgId;
      }
    }

    pIter = taosHashIterate(tsVmain.hash, pIter);
  }

  return TSDB_CODE_SUCCESS;
}

static void vnodeCleanupVnodes() {
  int32_t vnodeList[TSDB_MAX_VNODES] = {0};
  int32_t numOfVnodes = 0;

  int32_t code = vnodeGetVnodeList(vnodeList, &numOfVnodes);

  if (code != TSDB_CODE_SUCCESS) {
    vInfo("failed to get dnode list since code %d", code);
    return;
  }

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    vnodeClose(vnodeList[i]);
  }

  vInfo("total vnodes:%d are all closed", numOfVnodes);
}

static void vnodeInitMsgFp() {
  tsVmain.msgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE] = vnodeProcessMgmtMsg;
  tsVmain.msgFp[TSDB_MSG_TYPE_MD_ALTER_VNODE] = vnodeProcessMgmtMsg;
  tsVmain.msgFp[TSDB_MSG_TYPE_MD_SYNC_VNODE] = vnodeProcessMgmtMsg;
  tsVmain.msgFp[TSDB_MSG_TYPE_MD_COMPACT_VNODE] = vnodeProcessMgmtMsg;
  tsVmain.msgFp[TSDB_MSG_TYPE_MD_DROP_VNODE] = vnodeProcessMgmtMsg;
  tsVmain.msgFp[TSDB_MSG_TYPE_MD_ALTER_STREAM] = vnodeProcessMgmtMsg;
  tsVmain.msgFp[TSDB_MSG_TYPE_MD_CREATE_TABLE] = vnodeProcessWriteMsg;
  tsVmain.msgFp[TSDB_MSG_TYPE_MD_DROP_TABLE] = vnodeProcessWriteMsg;
  tsVmain.msgFp[TSDB_MSG_TYPE_MD_ALTER_TABLE] = vnodeProcessWriteMsg;
  tsVmain.msgFp[TSDB_MSG_TYPE_MD_DROP_STABLE] = vnodeProcessWriteMsg;
  tsVmain.msgFp[TSDB_MSG_TYPE_SUBMIT] = vnodeProcessWriteMsg;
  tsVmain.msgFp[TSDB_MSG_TYPE_UPDATE_TAG_VAL] = vnodeProcessWriteMsg;
  tsVmain.msgFp[TSDB_MSG_TYPE_QUERY] = vnodeProcessReadMsg;
  tsVmain.msgFp[TSDB_MSG_TYPE_FETCH] = vnodeProcessReadMsg;
}

void vnodeProcessMsg(SRpcMsg *pMsg) {
  if (tsVmain.msgFp[pMsg->msgType]) {
    (*tsVmain.msgFp[pMsg->msgType])(pMsg);
  } else {
    assert(0);
  }
}

int32_t vnodeInitMain() {
  vnodeInitMsgFp();

  tsVmain.timer = taosTmrInit(100, 200, 60000, "VND-TIMER");
  if (tsVmain.timer == NULL) {
    vError("failed to init vnode timer");
    return -1;
  }

  tsVmain.hash = taosHashInit(TSDB_MIN_VNODES, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (tsVmain.hash == NULL) {
    taosTmrCleanUp(tsVmain.timer);
    vError("failed to init vnode mgmt");
    return -1;
  }

  vInfo("vnode main is initialized");
  return vnodeOpenVnodes();
}

void vnodeCleanupMain() {
  taosTmrCleanUp(tsVmain.timer);
  tsVmain.timer = NULL;

  vnodeCleanupVnodes();

  taosHashCleanup(tsVmain.hash);
  tsVmain.hash = NULL;
}

static void vnodeBuildVloadMsg(SVnode *pVnode, SStatusMsg *pStatus) {
#if 0  
  int64_t totalStorage = 0;
  int64_t compStorage = 0;
  int64_t pointsWritten = 0;

  if (vnodeInClosingStatus(pVnode)) return;
  if (pStatus->openVnodes >= TSDB_MAX_VNODES) return;

  if (pVnode->tsdb) {
    tsdbReportStat(pVnode->tsdb, &pointsWritten, &totalStorage, &compStorage);
  }

  SVnodeLoad *pLoad = &pStatus->load[pStatus->openVnodes++];
  pLoad->vgId = htonl(pVnode->vgId);
  pLoad->dbCfgVersion = htonl(pVnode->dbCfgVersion);
  pLoad->vgCfgVersion = htonl(pVnode->vgCfgVersion);
  pLoad->totalStorage = htobe64(totalStorage);
  pLoad->compStorage = htobe64(compStorage);
  pLoad->pointsWritten = htobe64(pointsWritten);
  pLoad->vnodeVersion = htobe64(pVnode->version);
  pLoad->status = pVnode->status;
  pLoad->role = pVnode->role;
  pLoad->replica = pVnode->syncCfg.replica;
  pLoad->compact = (pVnode->tsdb != NULL) ? tsdbGetCompactState(pVnode->tsdb) : 0;
#endif  
}

void vnodeGetStatus(struct SStatusMsg *pStatus) {
  void *pIter = taosHashIterate(tsVmain.hash, NULL);
  while (pIter) {
    SVnode **pVnode = pIter;
    if (*pVnode) {
      vnodeBuildVloadMsg(*pVnode, pStatus);
    }
    pIter = taosHashIterate(tsVmain.hash, pIter);
  }
}

void vnodeSetAccess(struct SVgroupAccess *pAccess, int32_t numOfVnodes) {
  for (int32_t i = 0; i < numOfVnodes; ++i) {
    pAccess[i].vgId = htonl(pAccess[i].vgId);
    SVnode *pVnode = vnodeAcquireNotClose(pAccess[i].vgId);
    if (pVnode != NULL) {
      pVnode->accessState = pAccess[i].accessState;
      if (pVnode->accessState != TSDB_VN_ALL_ACCCESS) {
        vDebug("vgId:%d, access state is set to %d", pAccess[i].vgId, pVnode->accessState);
      }
      vnodeRelease(pVnode);
    }
  }
}

void vnodeBackup(int32_t vgId) {
  char newDir[TSDB_FILENAME_LEN] = {0};
  char stagingDir[TSDB_FILENAME_LEN] = {0};

  sprintf(newDir, "%s/vnode%d", "vnode_bak", vgId);
  sprintf(stagingDir, "%s/.staging/vnode%d", "vnode_bak", vgId);

#if 0
  if (tsEnableVnodeBak) {
    tfsRmdir(newDir);
    tfsRename(stagingDir, newDir);
  } else {
    vInfo("vgId:%d, vnode backup not enabled", vgId);

    tfsRmdir(stagingDir);
  }
#endif  
}
