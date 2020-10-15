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

#include "tcache.h"
#include "cJSON.h"
#include "dnode.h"
#include "hash.h"
#include "taoserror.h"
#include "taosmsg.h"
#include "tglobal.h"
#include "trpc.h"
#include "tsdb.h"
#include "ttimer.h"
#include "tutil.h"
#include "vnode.h"
#include "vnodeInt.h"
#include "query.h"
#include "dnode.h"

#define TSDB_VNODE_VERSION_CONTENT_LEN 31

static SHashObj*tsDnodeVnodesHash;
static void     vnodeCleanUp(SVnodeObj *pVnode);
static int32_t  vnodeSaveCfg(SMDCreateVnodeMsg *pVnodeCfg);
static int32_t  vnodeReadCfg(SVnodeObj *pVnode);
static int32_t  vnodeSaveVersion(SVnodeObj *pVnode);
static int32_t  vnodeReadVersion(SVnodeObj *pVnode);
static int      vnodeProcessTsdbStatus(void *arg, int status);
static uint32_t vnodeGetFileInfo(void *ahandle, char *name, uint32_t *index, uint32_t eindex, int64_t *size, uint64_t *fversion);
static int      vnodeGetWalInfo(void *ahandle, char *name, uint32_t *index);
static void     vnodeNotifyRole(void *ahandle, int8_t role);
static void     vnodeCtrlFlow(void *handle, int32_t mseconds); 
static int      vnodeNotifyFileSynced(void *ahandle, uint64_t fversion);

#ifndef _SYNC
tsync_h syncStart(const SSyncInfo *info) { return NULL; }
int32_t syncForwardToPeer(tsync_h shandle, void *pHead, void *mhandle, int qtype) { return 0; }
void    syncStop(tsync_h shandle) {}
int32_t syncReconfig(tsync_h shandle, const SSyncCfg * cfg) { return 0; }
int     syncGetNodesRole(tsync_h shandle, SNodesRole * cfg) { return 0; }
void    syncConfirmForward(tsync_h shandle, uint64_t version, int32_t code) {}
#endif

int32_t vnodeInitResources() {
  int code = syncInit();
  if (code != 0) return code;

  vnodeInitWriteFp();
  vnodeInitReadFp();

  tsDnodeVnodesHash = taosHashInit(TSDB_MIN_VNODES, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, true);
  if (tsDnodeVnodesHash == NULL) {
    vError("failed to init vnode list");
    return TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

void vnodeCleanupResources() {
  if (tsDnodeVnodesHash != NULL) {
    taosHashCleanup(tsDnodeVnodesHash);
    tsDnodeVnodesHash = NULL;
  }

  syncCleanUp();
}

int32_t vnodeCreate(SMDCreateVnodeMsg *pVnodeCfg) {
  int32_t code;

  SVnodeObj *pTemp = (SVnodeObj *)taosHashGet(tsDnodeVnodesHash, (const char *)&pVnodeCfg->cfg.vgId, sizeof(int32_t));
  if (pTemp != NULL) {
    vInfo("vgId:%d, vnode already exist, pVnode:%p", pVnodeCfg->cfg.vgId, pTemp);
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

  code = vnodeSaveCfg(pVnodeCfg);
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to save vnode cfg, reason:%s", pVnodeCfg->cfg.vgId, tstrerror(code));
    return code;
  }

  STsdbCfg tsdbCfg = {0};
  tsdbCfg.tsdbId              = pVnodeCfg->cfg.vgId;
  tsdbCfg.cacheBlockSize      = pVnodeCfg->cfg.cacheBlockSize;
  tsdbCfg.totalBlocks         = pVnodeCfg->cfg.totalBlocks;
  // tsdbCfg.maxTables           = pVnodeCfg->cfg.maxTables;
  tsdbCfg.daysPerFile         = pVnodeCfg->cfg.daysPerFile;
  tsdbCfg.keep                = pVnodeCfg->cfg.daysToKeep;
  tsdbCfg.minRowsPerFileBlock = pVnodeCfg->cfg.minRowsPerFileBlock;
  tsdbCfg.maxRowsPerFileBlock = pVnodeCfg->cfg.maxRowsPerFileBlock;
  tsdbCfg.precision           = pVnodeCfg->cfg.precision;
  tsdbCfg.compression         = pVnodeCfg->cfg.compression;

  char tsdbDir[TSDB_FILENAME_LEN] = {0};
  sprintf(tsdbDir, "%s/vnode%d/tsdb", tsVnodeDir, pVnodeCfg->cfg.vgId);
  if (tsdbCreateRepo(tsdbDir, &tsdbCfg) < 0) {
    vError("vgId:%d, failed to create tsdb in vnode, reason:%s", pVnodeCfg->cfg.vgId, tstrerror(terrno));
    return TSDB_CODE_VND_INIT_FAILED;
  }

  vInfo("vgId:%d, vnode is created, walLevel:%d fsyncPeriod:%d", pVnodeCfg->cfg.vgId, pVnodeCfg->cfg.walLevel, pVnodeCfg->cfg.fsyncPeriod);
  code = vnodeOpen(pVnodeCfg->cfg.vgId, rootDir);

  return code;
}

int32_t vnodeDrop(int32_t vgId) {
  SVnodeObj **ppVnode = (SVnodeObj **)taosHashGet(tsDnodeVnodesHash, (const char *)&vgId, sizeof(int32_t));
  if (ppVnode == NULL || *ppVnode == NULL) {
    vDebug("vgId:%d, failed to drop, vgId not find", vgId);
    return TSDB_CODE_VND_INVALID_VGROUP_ID;
  }

  SVnodeObj *pVnode = *ppVnode;
  vTrace("vgId:%d, vnode will be dropped, refCount:%d", pVnode->vgId, pVnode->refCount);
  pVnode->dropped = 1;
  vnodeCleanUp(pVnode);

  return TSDB_CODE_SUCCESS;
}

int32_t vnodeAlter(void *param, SMDCreateVnodeMsg *pVnodeCfg) {
  SVnodeObj *pVnode = param;

  // vnode in non-ready state and still needs to return success instead of TSDB_CODE_VND_INVALID_STATUS
  // cfgVersion can be corrected by status msg
  if (atomic_val_compare_exchange_8(&pVnode->status, TAOS_VN_STATUS_READY, TAOS_VN_STATUS_UPDATING) != TAOS_VN_STATUS_READY) {
    vDebug("vgId:%d, vnode is not ready, do alter operation later", pVnode->vgId);
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = vnodeSaveCfg(pVnodeCfg);
  if (code != TSDB_CODE_SUCCESS) {
    pVnode->status = TAOS_VN_STATUS_READY;
    return code; 
  }

  code = vnodeReadCfg(pVnode);
  if (code != TSDB_CODE_SUCCESS) {
    pVnode->status = TAOS_VN_STATUS_READY;
    return code; 
  }

  code = walAlter(pVnode->wal, &pVnode->walCfg);
  if (code != TSDB_CODE_SUCCESS) {
    pVnode->status = TAOS_VN_STATUS_READY;
    return code;
  }

  code = syncReconfig(pVnode->sync, &pVnode->syncCfg);
  if (code != TSDB_CODE_SUCCESS) {
    pVnode->status = TAOS_VN_STATUS_READY;
    return code; 
  } 

  if (pVnode->tsdb) {
    code = tsdbConfigRepo(pVnode->tsdb, &pVnode->tsdbCfg);
    if (code != TSDB_CODE_SUCCESS) {
      pVnode->status = TAOS_VN_STATUS_READY;
      return code; 
    }
  }

  pVnode->status = TAOS_VN_STATUS_READY;
  vDebug("vgId:%d, vnode is altered", pVnode->vgId);

  return TSDB_CODE_SUCCESS;
}

int32_t vnodeOpen(int32_t vnode, char *rootDir) {
  char temp[TSDB_FILENAME_LEN];

  SVnodeObj *pVnode = calloc(sizeof(SVnodeObj), 1);
  if (pVnode == NULL) {
    vError("vgId:%d, failed to open vnode since no enough memory", vnode);
    return TAOS_SYSTEM_ERROR(errno);
  }

  atomic_add_fetch_32(&pVnode->refCount, 1);

  pVnode->vgId     = vnode;
  pVnode->status   = TAOS_VN_STATUS_INIT;
  pVnode->version  = 0;  
  pVnode->tsdbCfg.tsdbId = pVnode->vgId;
  pVnode->rootDir = strdup(rootDir);
  pVnode->accessState = TSDB_VN_ALL_ACCCESS;
  tsem_init(&pVnode->sem, 0, 0);

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
  
  pVnode->wqueue = dnodeAllocateVnodeWqueue(pVnode);
  pVnode->rqueue = dnodeAllocateVnodeRqueue(pVnode);
  if (pVnode->wqueue == NULL || pVnode->rqueue == NULL) {
    vnodeCleanUp(pVnode);
    return terrno;
  }

  SCqCfg cqCfg = {0};
  sprintf(cqCfg.user, "_root");
  strcpy(cqCfg.pass, tsInternalPass);
  strcpy(cqCfg.db, pVnode->db);
  cqCfg.vgId = vnode;
  cqCfg.cqWrite = vnodeWriteToQueue;
  pVnode->cq = cqOpen(pVnode, &cqCfg);
  if (pVnode->cq == NULL) {
    vnodeCleanUp(pVnode);
    return terrno;
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
  } else if (terrno != TSDB_CODE_SUCCESS && pVnode->syncCfg.replica <= 1) {
    vError("vgId:%d, failed to open tsdb, replica:%d reason:%s", pVnode->vgId, pVnode->syncCfg.replica,
           tstrerror(terrno));
    vnodeCleanUp(pVnode);
    return terrno;
  }

  sprintf(temp, "%s/wal", rootDir);
  pVnode->wal = walOpen(temp, &pVnode->walCfg);
  if (pVnode->wal == NULL) { 
    vnodeCleanUp(pVnode);
    return terrno;
  }

  walRestore(pVnode->wal, pVnode, vnodeWriteToQueue);
  if (pVnode->version == 0) {
    pVnode->version = walGetVersion(pVnode->wal);
  }

  SSyncInfo syncInfo;
  syncInfo.vgId = pVnode->vgId;
  syncInfo.version = pVnode->version;
  syncInfo.syncCfg = pVnode->syncCfg;
  sprintf(syncInfo.path, "%s", rootDir);
  syncInfo.ahandle = pVnode;
  syncInfo.getWalInfo = vnodeGetWalInfo;
  syncInfo.getFileInfo = vnodeGetFileInfo;
  syncInfo.writeToCache = vnodeWriteToQueue;
  syncInfo.confirmForward = dnodeSendRpcVnodeWriteRsp; 
  syncInfo.notifyRole = vnodeNotifyRole;
  syncInfo.notifyFlowCtrl = vnodeCtrlFlow;
  syncInfo.notifyFileSynced = vnodeNotifyFileSynced;
  pVnode->sync = syncStart(&syncInfo);

#ifndef _SYNC
  pVnode->role = TAOS_SYNC_ROLE_MASTER;
#else
  if (pVnode->sync == NULL) {
    vError("vgId:%d, failed to open sync module, replica:%d reason:%s", pVnode->vgId, pVnode->syncCfg.replica,
           tstrerror(terrno));
    vnodeCleanUp(pVnode);
    return terrno;
  }
#endif  

  pVnode->qMgmt = qOpenQueryMgmt(pVnode->vgId);
  if (pVnode->qMgmt == NULL) {
    vnodeCleanUp(pVnode);
    return terrno;
  }

  pVnode->events = NULL;
  pVnode->status = TAOS_VN_STATUS_READY;
  vDebug("vgId:%d, vnode is opened in %s, pVnode:%p", pVnode->vgId, rootDir, pVnode);

  taosHashPut(tsDnodeVnodesHash, (const char *)&pVnode->vgId, sizeof(int32_t), (char *)(&pVnode), sizeof(SVnodeObj *));

  return TSDB_CODE_SUCCESS;
}

int32_t vnodeClose(int32_t vgId) {
  SVnodeObj **ppVnode = (SVnodeObj **)taosHashGet(tsDnodeVnodesHash, (const char *)&vgId, sizeof(int32_t));
  if (ppVnode == NULL || *ppVnode == NULL) return 0;

  SVnodeObj *pVnode = *ppVnode;
  vDebug("vgId:%d, vnode will be closed", pVnode->vgId);
  vnodeCleanUp(pVnode);

  return 0;
}

void vnodeRelease(void *pVnodeRaw) {
  SVnodeObj *pVnode = pVnodeRaw;
  int32_t    vgId = pVnode->vgId;

  int32_t refCount = atomic_sub_fetch_32(&pVnode->refCount, 1);
  assert(refCount >= 0);

  if (refCount > 0) {
    vDebug("vgId:%d, release vnode, refCount:%d", vgId, refCount);
    if (pVnode->status == TAOS_VN_STATUS_RESET && refCount == 2) 
      tsem_post(&pVnode->sem);
    return;
  }

  qCleanupQueryMgmt(pVnode->qMgmt);
  pVnode->qMgmt = NULL;

  if (pVnode->tsdb)
    tsdbCloseRepo(pVnode->tsdb, 1);
  pVnode->tsdb = NULL;

  // stop continuous query
  if (pVnode->cq) {
    void *cq = pVnode->cq;
    pVnode->cq = NULL;
    cqClose(cq);
  }

  if (pVnode->wal) 
    walClose(pVnode->wal);
  pVnode->wal = NULL;

  if (pVnode->wqueue) 
    dnodeFreeVnodeWqueue(pVnode->wqueue);
  pVnode->wqueue = NULL;

  if (pVnode->rqueue) 
    dnodeFreeVnodeRqueue(pVnode->rqueue);
  pVnode->rqueue = NULL;
 
  taosTFree(pVnode->rootDir);

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
  free(pVnode);

  int32_t count = taosHashGetSize(tsDnodeVnodesHash);
  vDebug("vgId:%d, vnode is released, vnodes:%d", vgId, count);
}

void *vnodeAcquire(int32_t vgId) {
  SVnodeObj **ppVnode = (SVnodeObj **)taosHashGet(tsDnodeVnodesHash, (const char *)&vgId, sizeof(int32_t));
  if (ppVnode == NULL || *ppVnode == NULL) {
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
    vInfo("vgId:%d, not exist", vgId);
    return NULL;
  }

  SVnodeObj *pVnode = *ppVnode;
  atomic_add_fetch_32(&pVnode->refCount, 1);
  vDebug("vgId:%d, get vnode, refCount:%d", pVnode->vgId, pVnode->refCount);

  return pVnode;
}

void *vnodeAcquireRqueue(int32_t vgId) {
  SVnodeObj *pVnode = vnodeAcquire(vgId);
  if (pVnode == NULL) return NULL;

  if (pVnode->status == TAOS_VN_STATUS_RESET) {           
    terrno = TSDB_CODE_APP_NOT_READY;
    vInfo("vgId:%d, status is in reset", vgId);
    vnodeRelease(pVnode);
    return NULL;
  }

  return pVnode->rqueue;
}

void *vnodeAcquireWqueue(int32_t vgId) {
  SVnodeObj *pVnode = vnodeAcquire(vgId);
  if (pVnode == NULL) return NULL;

  if (pVnode->status == TAOS_VN_STATUS_RESET) {           
    terrno = TSDB_CODE_APP_NOT_READY;
    vInfo("vgId:%d, status is in reset", vgId);
    vnodeRelease(pVnode);
    return NULL;
  }
  
  return pVnode->wqueue;
}

void *vnodeGetWal(void *pVnode) {
  return ((SVnodeObj *)pVnode)->wal;
}

static void vnodeBuildVloadMsg(SVnodeObj *pVnode, SDMStatusMsg *pStatus) {
  int64_t totalStorage = 0;
  int64_t compStorage = 0;
  int64_t pointsWritten = 0;

  if (pVnode->status != TAOS_VN_STATUS_READY) return;
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
  SHashMutableIterator *pIter = taosHashCreateIter(tsDnodeVnodesHash);
  while (taosHashIterNext(pIter)) {
    SVnodeObj **pVnode = taosHashIterGet(pIter);
    if (pVnode == NULL) continue;
    if (*pVnode == NULL) continue;

    (*numOfVnodes)++;
    if (*numOfVnodes >= TSDB_MAX_VNODES) {
      vError("vgId:%d, too many open vnodes, exist:%d max:%d", (*pVnode)->vgId, *numOfVnodes, TSDB_MAX_VNODES);
      continue;
    } else {
      vnodeList[*numOfVnodes - 1] = (*pVnode)->vgId;
    }
  }

  taosHashDestroyIter(pIter);
  return TSDB_CODE_SUCCESS;
}

void vnodeBuildStatusMsg(void *param) {
  SDMStatusMsg *pStatus = param;
  SHashMutableIterator *pIter = taosHashCreateIter(tsDnodeVnodesHash);

  while (taosHashIterNext(pIter)) {
    SVnodeObj **pVnode = taosHashIterGet(pIter);
    if (pVnode == NULL) continue;
    if (*pVnode == NULL) continue;

    vnodeBuildVloadMsg(*pVnode, pStatus);
  }

  taosHashDestroyIter(pIter);
}

void vnodeSetAccess(SDMVgroupAccess *pAccess, int32_t numOfVnodes) {
  for (int32_t i = 0; i < numOfVnodes; ++i) {
    pAccess[i].vgId = htonl(pAccess[i].vgId);
    SVnodeObj *pVnode = vnodeAcquire(pAccess[i].vgId);
    if (pVnode != NULL) {
      pVnode->accessState = pAccess[i].accessState;
      if (pVnode->accessState != TSDB_VN_ALL_ACCCESS) {
        vDebug("vgId:%d, access state is set to %d", pAccess[i].vgId, pVnode->accessState)
      }
      vnodeRelease(pVnode);
    }
  }
}

static void vnodeCleanUp(SVnodeObj *pVnode) {
  // remove from hash, so new messages wont be consumed
  taosHashRemove(tsDnodeVnodesHash, (const char *)&pVnode->vgId, sizeof(int32_t));
  int i = 0;

  if (pVnode->status != TAOS_VN_STATUS_INIT) {
    // it may be in updateing or reset state, then it shall wait
    while (atomic_val_compare_exchange_8(&pVnode->status, TAOS_VN_STATUS_READY, TAOS_VN_STATUS_CLOSING) != TAOS_VN_STATUS_READY) {
      if (++i % 1000 == 0) {
        sched_yield();
      }
    }
  }

  // stop replication module
  if (pVnode->sync) {
    void *sync = pVnode->sync;
    pVnode->sync = NULL;
    syncStop(sync);
  }

  vTrace("vgId:%d, vnode will cleanup, refCount:%d", pVnode->vgId, pVnode->refCount);

  // release local resources only after cutting off outside connections
  qQueryMgmtNotifyClosed(pVnode->qMgmt);
  vnodeRelease(pVnode);
}

// TODO: this is a simple implement
static int vnodeProcessTsdbStatus(void *arg, int status) {
  SVnodeObj *pVnode = arg;

  if (status == TSDB_STATUS_COMMIT_START) {
    pVnode->fversion = pVnode->version; 
    return walRenew(pVnode->wal);
  }

  if (status == TSDB_STATUS_COMMIT_OVER)
    return vnodeSaveVersion(pVnode);

  return 0; 
}

static uint32_t vnodeGetFileInfo(void *ahandle, char *name, uint32_t *index, uint32_t eindex, int64_t *size, uint64_t *fversion) {
  SVnodeObj *pVnode = ahandle;
  *fversion = pVnode->fversion;
  return tsdbGetFileInfo(pVnode->tsdb, name, index, eindex, size);
}

static int vnodeGetWalInfo(void *ahandle, char *name, uint32_t *index) {
  SVnodeObj *pVnode = ahandle;
  return walGetWalFile(pVnode->wal, name, index);
}

static void vnodeNotifyRole(void *ahandle, int8_t role) {
  SVnodeObj *pVnode = ahandle;
  vInfo("vgId:%d, sync role changed from %s to %s", pVnode->vgId, syncRole[pVnode->role], syncRole[role]);
  pVnode->role = role;
  dnodeSendStatusMsgToMnode();

  if (pVnode->role == TAOS_SYNC_ROLE_MASTER)
    cqStart(pVnode->cq);
  else
    cqStop(pVnode->cq);
}

static void vnodeCtrlFlow(void *ahandle, int32_t mseconds) {
  SVnodeObj *pVnode = ahandle;
  if (pVnode->delay != mseconds) 
    vInfo("vgId:%d, sync flow control, mseconds:%d", pVnode->vgId, mseconds);
  pVnode->delay = mseconds;
}

static int vnodeResetTsdb(SVnodeObj *pVnode)
{
  char rootDir[128] = "\0";
  sprintf(rootDir, "%s/tsdb", pVnode->rootDir);

  if (atomic_val_compare_exchange_8(&pVnode->status, TAOS_VN_STATUS_READY, TAOS_VN_STATUS_RESET) != TAOS_VN_STATUS_READY)
    return -1;

  void *tsdb = pVnode->tsdb;
  pVnode->tsdb = NULL;

  // acquire vnode
  int32_t refCount = atomic_add_fetch_32(&pVnode->refCount, 1); 

  if (refCount > 2) 
    tsem_wait(&pVnode->sem);

  // close tsdb, then open tsdb
  tsdbCloseRepo(tsdb, 0);
  STsdbAppH appH = {0};
  appH.appH = (void *)pVnode;
  appH.notifyStatus = vnodeProcessTsdbStatus;
  appH.cqH = pVnode->cq;
  appH.cqCreateFunc = cqCreate;
  appH.cqDropFunc = cqDrop;
  pVnode->tsdb = tsdbOpenRepo(rootDir, &appH);

  pVnode->status = TAOS_VN_STATUS_READY;
  vnodeRelease(pVnode);  

  return 0;
}

static int vnodeNotifyFileSynced(void *ahandle, uint64_t fversion) {
  SVnodeObj *pVnode = ahandle;
  vDebug("vgId:%d, data file is synced, fversion:%" PRId64, pVnode->vgId, fversion);

  pVnode->fversion = fversion;
  pVnode->version = fversion;
  vnodeSaveVersion(pVnode);

  return vnodeResetTsdb(pVnode);
}

static int32_t vnodeSaveCfg(SMDCreateVnodeMsg *pVnodeCfg) {
  char cfgFile[TSDB_FILENAME_LEN + 30] = {0};
  sprintf(cfgFile, "%s/vnode%d/config.json", tsVnodeDir, pVnodeCfg->cfg.vgId);
  FILE *fp = fopen(cfgFile, "w");
  if (!fp) {
    vError("vgId:%d, failed to open vnode cfg file for write, file:%s error:%s", pVnodeCfg->cfg.vgId, cfgFile,
           strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return terrno;
  }

  int32_t len = 0;
  int32_t maxLen = 1000;
  char *  content = calloc(1, maxLen + 1);
  if (content == NULL) {
    fclose(fp);
    return TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"db\": \"%s\",\n", pVnodeCfg->db);
  len += snprintf(content + len, maxLen - len, "  \"cfgVersion\": %d,\n", pVnodeCfg->cfg.cfgVersion);
  len += snprintf(content + len, maxLen - len, "  \"cacheBlockSize\": %d,\n", pVnodeCfg->cfg.cacheBlockSize);
  len += snprintf(content + len, maxLen - len, "  \"totalBlocks\": %d,\n", pVnodeCfg->cfg.totalBlocks);
  // len += snprintf(content + len, maxLen - len, "  \"maxTables\": %d,\n", pVnodeCfg->cfg.maxTables);
  len += snprintf(content + len, maxLen - len, "  \"daysPerFile\": %d,\n", pVnodeCfg->cfg.daysPerFile);
  len += snprintf(content + len, maxLen - len, "  \"daysToKeep\": %d,\n", pVnodeCfg->cfg.daysToKeep);
  len += snprintf(content + len, maxLen - len, "  \"daysToKeep1\": %d,\n", pVnodeCfg->cfg.daysToKeep1);
  len += snprintf(content + len, maxLen - len, "  \"daysToKeep2\": %d,\n", pVnodeCfg->cfg.daysToKeep2);
  len += snprintf(content + len, maxLen - len, "  \"minRowsPerFileBlock\": %d,\n", pVnodeCfg->cfg.minRowsPerFileBlock);
  len += snprintf(content + len, maxLen - len, "  \"maxRowsPerFileBlock\": %d,\n", pVnodeCfg->cfg.maxRowsPerFileBlock);
  // len += snprintf(content + len, maxLen - len, "  \"commitTime\": %d,\n", pVnodeCfg->cfg.commitTime);
  len += snprintf(content + len, maxLen - len, "  \"precision\": %d,\n", pVnodeCfg->cfg.precision);
  len += snprintf(content + len, maxLen - len, "  \"compression\": %d,\n", pVnodeCfg->cfg.compression);
  len += snprintf(content + len, maxLen - len, "  \"walLevel\": %d,\n", pVnodeCfg->cfg.walLevel);
  len += snprintf(content + len, maxLen - len, "  \"fsync\": %d,\n", pVnodeCfg->cfg.fsyncPeriod);
  len += snprintf(content + len, maxLen - len, "  \"replica\": %d,\n", pVnodeCfg->cfg.replications);
  len += snprintf(content + len, maxLen - len, "  \"wals\": %d,\n", pVnodeCfg->cfg.wals);
  len += snprintf(content + len, maxLen - len, "  \"quorum\": %d,\n", pVnodeCfg->cfg.quorum);

  len += snprintf(content + len, maxLen - len, "  \"nodeInfos\": [{\n");

  vInfo("vgId:%d, save vnode cfg, replica:%d", pVnodeCfg->cfg.vgId, pVnodeCfg->cfg.replications);
  for (int32_t i = 0; i < pVnodeCfg->cfg.replications; i++) {
    len += snprintf(content + len, maxLen - len, "    \"nodeId\": %d,\n", pVnodeCfg->nodes[i].nodeId);
    len += snprintf(content + len, maxLen - len, "    \"nodeEp\": \"%s\"\n", pVnodeCfg->nodes[i].nodeEp);
    vInfo("vgId:%d, save vnode cfg, nodeId:%d nodeEp:%s", pVnodeCfg->cfg.vgId, pVnodeCfg->nodes[i].nodeId,
          pVnodeCfg->nodes[i].nodeEp);

    if (i < pVnodeCfg->cfg.replications - 1) {
      len += snprintf(content + len, maxLen - len, "  },{\n");
    } else {
      len += snprintf(content + len, maxLen - len, "  }]\n");
    }
  }
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  fflush(fp);
  fclose(fp);
  free(content);

  vInfo("vgId:%d, save vnode cfg successed", pVnodeCfg->cfg.vgId);

  return TSDB_CODE_SUCCESS;
}

static int32_t vnodeReadCfg(SVnodeObj *pVnode) {
  cJSON  *root = NULL;
  char   *content = NULL;
  char    cfgFile[TSDB_FILENAME_LEN + 30] = {0};
  int     maxLen = 1000;

  terrno = TSDB_CODE_VND_APP_ERROR;
  sprintf(cfgFile, "%s/vnode%d/config.json", tsVnodeDir, pVnode->vgId);
  FILE *fp = fopen(cfgFile, "r");
  if (!fp) {
    vError("vgId:%d, failed to open vnode cfg file:%s to read, error:%s", pVnode->vgId,
           cfgFile, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto PARSE_OVER;
  }

  content = calloc(1, maxLen + 1);
  if (content == NULL) goto PARSE_OVER;
  int len = fread(content, 1, maxLen, fp);
  if (len <= 0) {
    vError("vgId:%d, failed to read vnode cfg, content is null", pVnode->vgId);
    free(content);
    fclose(fp);
    return errno;
  }

  root = cJSON_Parse(content);
  if (root == NULL) {
    vError("vgId:%d, failed to read vnode cfg, invalid json format", pVnode->vgId);
    goto PARSE_OVER;
  }

  cJSON *db = cJSON_GetObjectItem(root, "db");
  if (!db || db->type != cJSON_String || db->valuestring == NULL) {
    vError("vgId:%d, failed to read vnode cfg, db not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  strcpy(pVnode->db, db->valuestring);
  
  cJSON *cfgVersion = cJSON_GetObjectItem(root, "cfgVersion");
  if (!cfgVersion || cfgVersion->type != cJSON_Number) {
    vError("vgId:%d, failed to read vnode cfg, cfgVersion not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->cfgVersion = cfgVersion->valueint;

  cJSON *cacheBlockSize = cJSON_GetObjectItem(root, "cacheBlockSize");
  if (!cacheBlockSize || cacheBlockSize->type != cJSON_Number) {
    vError("vgId:%d, failed to read vnode cfg, cacheBlockSize not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.cacheBlockSize = cacheBlockSize->valueint;

  cJSON *totalBlocks = cJSON_GetObjectItem(root, "totalBlocks");
  if (!totalBlocks || totalBlocks->type != cJSON_Number) {
    vError("vgId:%d, failed to read vnode cfg, totalBlocks not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.totalBlocks = totalBlocks->valueint;

  // cJSON *maxTables = cJSON_GetObjectItem(root, "maxTables");
  // if (!maxTables || maxTables->type != cJSON_Number) {
  //   vError("vgId:%d, failed to read vnode cfg, maxTables not found", pVnode->vgId);
  //   goto PARSE_OVER;
  // }
  // pVnode->tsdbCfg.maxTables = maxTables->valueint;

  cJSON *daysPerFile = cJSON_GetObjectItem(root, "daysPerFile");
  if (!daysPerFile || daysPerFile->type != cJSON_Number) {
    vError("vgId:%d, failed to read vnode cfg, daysPerFile not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.daysPerFile = daysPerFile->valueint;

  cJSON *daysToKeep = cJSON_GetObjectItem(root, "daysToKeep");
  if (!daysToKeep || daysToKeep->type != cJSON_Number) {
    vError("vgId:%d, failed to read vnode cfg, daysToKeep not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.keep = daysToKeep->valueint;

  cJSON *daysToKeep1 = cJSON_GetObjectItem(root, "daysToKeep1");
  if (!daysToKeep1 || daysToKeep1->type != cJSON_Number) {
    vError("vgId:%d, failed to read vnode cfg, daysToKeep1 not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.keep1 = daysToKeep1->valueint;

  cJSON *daysToKeep2 = cJSON_GetObjectItem(root, "daysToKeep2");
  if (!daysToKeep2 || daysToKeep2->type != cJSON_Number) {
    vError("vgId:%d, failed to read vnode cfg, daysToKeep2 not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.keep2 = daysToKeep2->valueint;

  cJSON *minRowsPerFileBlock = cJSON_GetObjectItem(root, "minRowsPerFileBlock");
  if (!minRowsPerFileBlock || minRowsPerFileBlock->type != cJSON_Number) {
    vError("vgId:%d, failed to read vnode cfg, minRowsPerFileBlock not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.minRowsPerFileBlock = minRowsPerFileBlock->valueint;

  cJSON *maxRowsPerFileBlock = cJSON_GetObjectItem(root, "maxRowsPerFileBlock");
  if (!maxRowsPerFileBlock || maxRowsPerFileBlock->type != cJSON_Number) {
    vError("vgId:%d, failed to read vnode cfg, maxRowsPerFileBlock not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.maxRowsPerFileBlock = maxRowsPerFileBlock->valueint;

  // cJSON *commitTime = cJSON_GetObjectItem(root, "commitTime");
  // if (!commitTime || commitTime->type != cJSON_Number) {
  //   vError("vgId:%d, failed to read vnode cfg, commitTime not found", pVnode->vgId);
  //   goto PARSE_OVER;
  // }
  // pVnode->tsdbCfg.commitTime = (int8_t)commitTime->valueint;

  cJSON *precision = cJSON_GetObjectItem(root, "precision");
  if (!precision || precision->type != cJSON_Number) {
    vError("vgId:%d, failed to read vnode cfg, precision not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.precision = (int8_t)precision->valueint;

  cJSON *compression = cJSON_GetObjectItem(root, "compression");
  if (!compression || compression->type != cJSON_Number) {
    vError("vgId:%d, failed to read vnode cfg, compression not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.compression = (int8_t)compression->valueint;

  cJSON *walLevel = cJSON_GetObjectItem(root, "walLevel");
  if (!walLevel || walLevel->type != cJSON_Number) {
    vError("vgId:%d, failed to read vnode cfg, walLevel not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->walCfg.walLevel = (int8_t) walLevel->valueint;

  cJSON *fsyncPeriod = cJSON_GetObjectItem(root, "fsync");
  if (!walLevel || walLevel->type != cJSON_Number) {
    vError("vgId:%d, failed to read vnode cfg, fsyncPeriod not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->walCfg.fsyncPeriod = fsyncPeriod->valueint;

  cJSON *wals = cJSON_GetObjectItem(root, "wals");
  if (!wals || wals->type != cJSON_Number) {
    vError("vgId:%d, failed to read vnode cfg, wals not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->walCfg.wals = (int8_t)wals->valueint;
  pVnode->walCfg.keep = 0;

  cJSON *replica = cJSON_GetObjectItem(root, "replica");
  if (!replica || replica->type != cJSON_Number) {
    vError("vgId:%d, failed to read vnode cfg, replica not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->syncCfg.replica = (int8_t)replica->valueint;

  cJSON *quorum = cJSON_GetObjectItem(root, "quorum");
  if (!quorum || quorum->type != cJSON_Number) {
    vError("vgId: %d, failed to read vnode cfg, quorum not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->syncCfg.quorum = (int8_t)quorum->valueint;

  cJSON *nodeInfos = cJSON_GetObjectItem(root, "nodeInfos");
  if (!nodeInfos || nodeInfos->type != cJSON_Array) {
    vError("vgId:%d, failed to read vnode cfg, nodeInfos not found", pVnode->vgId);
    goto PARSE_OVER;
  }

  int size = cJSON_GetArraySize(nodeInfos);
  if (size != pVnode->syncCfg.replica) {
    vError("vgId:%d, failed to read vnode cfg, nodeInfos size not matched", pVnode->vgId);
    goto PARSE_OVER;
  }

  for (int i = 0; i < size; ++i) {
    cJSON *nodeInfo = cJSON_GetArrayItem(nodeInfos, i);
    if (nodeInfo == NULL) continue;

    cJSON *nodeId = cJSON_GetObjectItem(nodeInfo, "nodeId");
    if (!nodeId || nodeId->type != cJSON_Number) {
      vError("vgId:%d, failed to read vnode cfg, nodeId not found", pVnode->vgId);
      goto PARSE_OVER;
    }
    pVnode->syncCfg.nodeInfo[i].nodeId = nodeId->valueint;

    cJSON *nodeEp = cJSON_GetObjectItem(nodeInfo, "nodeEp");
    if (!nodeEp || nodeEp->type != cJSON_String || nodeEp->valuestring == NULL) {
      vError("vgId:%d, failed to read vnode cfg, nodeFqdn not found", pVnode->vgId);
      goto PARSE_OVER;
    }

    taosGetFqdnPortFromEp(nodeEp->valuestring, pVnode->syncCfg.nodeInfo[i].nodeFqdn, &pVnode->syncCfg.nodeInfo[i].nodePort);
    pVnode->syncCfg.nodeInfo[i].nodePort += TSDB_PORT_SYNC;
  }

  terrno = TSDB_CODE_SUCCESS;

  vInfo("vgId:%d, read vnode cfg successfully, replcia:%d", pVnode->vgId, pVnode->syncCfg.replica);
  for (int32_t i = 0; i < pVnode->syncCfg.replica; i++) {
    vInfo("vgId:%d, dnode:%d, %s:%d", pVnode->vgId, pVnode->syncCfg.nodeInfo[i].nodeId,
           pVnode->syncCfg.nodeInfo[i].nodeFqdn, pVnode->syncCfg.nodeInfo[i].nodePort);
  }

PARSE_OVER:
  taosTFree(content);
  cJSON_Delete(root);
  if (fp) fclose(fp);
  return terrno;
}

static int32_t vnodeSaveVersion(SVnodeObj *pVnode) {
  char versionFile[TSDB_FILENAME_LEN + 30] = {0};
  sprintf(versionFile, "%s/vnode%d/version.json", tsVnodeDir, pVnode->vgId);
  FILE *fp = fopen(versionFile, "w");
  if (!fp) {
    vError("vgId:%d, failed to open vnode version file for write, file:%s error:%s", pVnode->vgId,
           versionFile, strerror(errno));
    return TAOS_SYSTEM_ERROR(errno); 
  }

  int32_t len = 0;
  int32_t maxLen = 30;
  char content[TSDB_VNODE_VERSION_CONTENT_LEN] = {0};

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"version\": %" PRId64 "\n", pVnode->fversion);
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  fflush(fp);
  fclose(fp);

  vInfo("vgId:%d, save vnode version:%" PRId64 " succeed", pVnode->vgId, pVnode->fversion);

  return TSDB_CODE_SUCCESS;
}

static int32_t vnodeReadVersion(SVnodeObj *pVnode) {
  char    versionFile[TSDB_FILENAME_LEN + 30] = {0};
  char   *content = NULL;
  cJSON  *root = NULL;
  int     maxLen = 100;

  terrno = TSDB_CODE_VND_INVALID_VRESION_FILE;
  sprintf(versionFile, "%s/vnode%d/version.json", tsVnodeDir, pVnode->vgId);
  FILE *fp = fopen(versionFile, "r");
  if (!fp) {
    if (errno != ENOENT) {
      vError("vgId:%d, failed to open version file:%s error:%s", pVnode->vgId, versionFile, strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
    } else {
      terrno = TSDB_CODE_SUCCESS;
    }
    goto PARSE_OVER;
  }

  content = calloc(1, maxLen + 1);
  int len = fread(content, 1, maxLen, fp);
  if (len <= 0) {
    vError("vgId:%d, failed to read vnode version, content is null", pVnode->vgId);
    goto PARSE_OVER;
  }

  root = cJSON_Parse(content);
  if (root == NULL) {
    vError("vgId:%d, failed to read vnode version, invalid json format", pVnode->vgId);
    goto PARSE_OVER;
  }

  cJSON *ver = cJSON_GetObjectItem(root, "version");
  if (!ver || ver->type != cJSON_Number) {
    vError("vgId:%d, failed to read vnode version, version not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->version = ver->valueint;

  terrno = TSDB_CODE_SUCCESS;
  vInfo("vgId:%d, read vnode version successfully, version:%" PRId64, pVnode->vgId, pVnode->version);

PARSE_OVER:
  taosTFree(content);
  cJSON_Delete(root);
  if (fp) fclose(fp);
  return terrno;
}
