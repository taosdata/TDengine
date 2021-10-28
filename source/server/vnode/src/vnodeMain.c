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
#include "thash.h"
#include "tthread.h"
#include "vnodeFile.h"
#include "vnodeMain.h"
#include "vnodeMgmt.h"
#include "vnodeRead.h"
#include "vnodeWrite.h"

typedef enum _VN_STATUS {
  TAOS_VN_STATUS_INIT = 0,
  TAOS_VN_STATUS_READY = 1,
  TAOS_VN_STATUS_CLOSING = 2,
  TAOS_VN_STATUS_UPDATING = 3
} EVnodeStatus;

char *vnodeStatus[] = {"init", "ready", "closing", "updating"};

typedef struct {
  pthread_t *threadId;
  int32_t    threadIndex;
  int32_t    failed;
  int32_t    opened;
  int32_t    vnodeNum;
  int32_t   *vnodeList;
} SOpenVnodeThread;

static struct {
  SHashObj *hash;
  int32_t   openVnodes;
  int32_t   totalVnodes;
  void (*msgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *);
} tsVnode;

static bool vnodeSetInitStatus(SVnode *pVnode) {
  pthread_mutex_lock(&pVnode->statusMutex);
  pVnode->status = TAOS_VN_STATUS_INIT;
  pthread_mutex_unlock(&pVnode->statusMutex);
  return true;
}

static bool vnodeSetReadyStatus(SVnode *pVnode) {
  bool set = false;
  pthread_mutex_lock(&pVnode->statusMutex);

  if (pVnode->status == TAOS_VN_STATUS_INIT || pVnode->status == TAOS_VN_STATUS_UPDATING) {
    pVnode->status = TAOS_VN_STATUS_READY;
    set = true;
  }

  pthread_mutex_unlock(&pVnode->statusMutex);
  return set;
}

static bool vnodeSetUpdatingStatus(SVnode *pVnode) {
  bool set = false;
  pthread_mutex_lock(&pVnode->statusMutex);

  if (pVnode->status == TAOS_VN_STATUS_READY) {
    pVnode->status = TAOS_VN_STATUS_UPDATING;
    set = true;
  }

  pthread_mutex_unlock(&pVnode->statusMutex);
  return set;
}

static bool vnodeSetClosingStatus(SVnode *pVnode) {
  bool set = false;
  pthread_mutex_lock(&pVnode->statusMutex);

  if (pVnode->status == TAOS_VN_STATUS_INIT || pVnode->status == TAOS_VN_STATUS_READY) {
    pVnode->status = TAOS_VN_STATUS_CLOSING;
    set = true;
  }

  pthread_mutex_unlock(&pVnode->statusMutex);
  return set;
}

static bool vnodeInStatus(SVnode *pVnode, EVnodeStatus status) {
  bool in = false;
  pthread_mutex_lock(&pVnode->statusMutex);

  if (pVnode->status == status) {
    in = true;
  }

  pthread_mutex_unlock(&pVnode->statusMutex);
  return in;
}

static void vnodeDestroyVnode(SVnode *pVnode) {
  int32_t code = 0;
  int32_t vgId = pVnode->vgId;

  if (pVnode->pQuery) {
    // todo
  }

  if (pVnode->pMeta) {
    // todo
  }

  if (pVnode->pTsdb) {
    // todo
  }

  if (pVnode->pTQ) {
    // todo
  }

  if (pVnode->pWal) {
    // todo
  }

  if (pVnode->allocator) {
    // todo
  }

  if (pVnode->pWriteQ) {
    vnodeFreeWriteQueue(pVnode->pWriteQ);
    pVnode->pWriteQ = NULL;
  }

  if (pVnode->pQueryQ) {
    vnodeFreeQueryQueue(pVnode->pQueryQ);
    pVnode->pQueryQ = NULL;
  }

  if (pVnode->pFetchQ) {
    vnodeFreeFetchQueue(pVnode->pFetchQ);
    pVnode->pFetchQ = NULL;
  }

  if (pVnode->dropped) {
    // todo
  }

  pthread_mutex_destroy(&pVnode->statusMutex);
  free(pVnode);
}

static void vnodeCleanupVnode(SVnode *pVnode) {
  vnodeSetClosingStatus(pVnode);
  taosHashRemove(tsVnode.hash, &pVnode->vgId, sizeof(int32_t));
  vnodeRelease(pVnode);
}

static int32_t vnodeOpenVnode(int32_t vgId) {
  int32_t code = 0;

  SVnode *pVnode = calloc(sizeof(SVnode), 1);
  if (pVnode == NULL) {
    vError("vgId:%d, failed to open vnode since no enough memory", vgId);
    return TAOS_SYSTEM_ERROR(errno);
  }

  pVnode->vgId = vgId;
  pVnode->accessState = TAOS_VN_STATUS_INIT;
  pVnode->status = TSDB_VN_ALL_ACCCESS;
  pVnode->refCount = 1;
  pVnode->role = TAOS_SYNC_ROLE_CANDIDATE;
  pthread_mutex_init(&pVnode->statusMutex, NULL);

  code = vnodeReadCfg(vgId, &pVnode->cfg);
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to read config file, set cfgVersion to 0", pVnode->vgId);
    pVnode->cfg.dropped = 1;
    vnodeCleanupVnode(pVnode);
    return 0;
  }

  code = vnodeReadTerm(vgId, &pVnode->term);
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to read term file since %s", pVnode->vgId, tstrerror(code));
    pVnode->cfg.dropped = 1;
    vnodeCleanupVnode(pVnode);
    return code;
  }

  pVnode->pWriteQ = vnodeAllocWriteQueue(pVnode);
  pVnode->pQueryQ = vnodeAllocQueryQueue(pVnode);
  pVnode->pFetchQ = vnodeAllocFetchQueue(pVnode);
  if (pVnode->pWriteQ == NULL || pVnode->pQueryQ == NULL || pVnode->pFetchQ == NULL) {
    vnodeCleanupVnode(pVnode);
    return terrno;
  }

  char path[PATH_MAX + 20];
  snprintf(path, sizeof(path), "%s/vnode%d/wal", tsVnodeDir, vgId);
  pVnode->pWal = walOpen(path, &pVnode->cfg.wal);
  if (pVnode->pWal == NULL) {
    vnodeCleanupVnode(pVnode);
    return terrno;
  }

  vDebug("vgId:%d, vnode is opened", pVnode->vgId);
  taosHashPut(tsVnode.hash, &pVnode->vgId, sizeof(int32_t), &pVnode, sizeof(SVnode *));

  vnodeSetReadyStatus(pVnode);
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeCreateVnode(int32_t vgId, SVnodeCfg *pCfg) {
  int32_t code = 0;
  char    path[PATH_MAX + 20] = {0};

  snprintf(path, sizeof(path), "%s/vnode%d", tsVnodeDir, vgId);
  if (taosMkDir(path) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    vError("vgId:%d, failed to create since %s", vgId, tstrerror(code));
    return code;
  }

  snprintf(path, sizeof(path), "%s/vnode%d/cfg", tsVnodeDir, vgId);
  if (taosMkDir(path) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    vError("vgId:%d, failed to create since %s", vgId, tstrerror(code));
    return code;
  }

  snprintf(path, sizeof(path), "%s/vnode%d/wal", tsVnodeDir, vgId);
  if (taosMkDir(path) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    vError("vgId:%d, failed to create since %s", vgId, tstrerror(code));
    return code;
  }

  snprintf(path, sizeof(path), "%s/vnode%d/tq", tsVnodeDir, vgId);
  if (taosMkDir(path) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    vError("vgId:%d, failed to create since %s", vgId, tstrerror(code));
    return code;
  }

  snprintf(path, sizeof(path), "%s/vnode%d/tsdb", tsVnodeDir, vgId);
  if (taosMkDir(path) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    vError("vgId:%d, failed to create since %s", vgId, tstrerror(code));
    return code;
  }

  snprintf(path, sizeof(path), "%s/vnode%d/meta", tsVnodeDir, vgId);
  if (taosMkDir(path) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    vError("vgId:%d, failed to create since %s", vgId, tstrerror(code));
    return code;
  }

  code = vnodeWriteCfg(vgId, pCfg);
  if (code != 0) {
    vError("vgId:%d, failed to save vnode cfg since %s", vgId, tstrerror(code));
    return code;
  }

  return vnodeOpenVnode(vgId);
}

int32_t vnodeAlterVnode(SVnode * pVnode, SVnodeCfg *pCfg) {
  int32_t code = 0;
  int32_t vgId = pVnode->vgId;

  bool walChanged = (memcmp(&pCfg->wal, &pVnode->cfg.wal, sizeof(SWalCfg)) != 0);
  bool tsdbChanged = (memcmp(&pCfg->tsdb, &pVnode->cfg.tsdb, sizeof(STsdbCfg)) != 0);
  bool metaChanged = (memcmp(&pCfg->meta, &pVnode->cfg.meta, sizeof(SMetaCfg)) != 0);
  bool syncChanged = (memcmp(&pCfg->sync, &pVnode->cfg.sync, sizeof(SSyncCluster)) != 0);

  if (!walChanged && !tsdbChanged && !metaChanged && !syncChanged) {
    vDebug("vgId:%d, nothing changed", vgId);
    vnodeRelease(pVnode);
    return code;
  }

  code = vnodeWriteCfg(pVnode->vgId, pCfg);
  if (code != 0) {
    vError("vgId:%d, failed to write alter msg to file since %s", vgId, tstrerror(code));
    vnodeRelease(pVnode);
    return code;
  }

  pVnode->cfg = *pCfg;

  if (walChanged) {
    code = walAlter(pVnode->pWal, &pVnode->cfg.wal);
    if (code != 0) {
      vDebug("vgId:%d, failed to alter wal since %s", vgId, tstrerror(code));
      vnodeRelease(pVnode);
      return code;
    }
  }

  if (tsdbChanged) {
    // todo
  }

  if (metaChanged) {
    // todo
  }

  if (syncChanged) {
    // todo
  }

  vnodeRelease(pVnode);
  return code;
}

int32_t vnodeDropVnode(SVnode *pVnode) {
  if (pVnode->cfg.dropped) {
    vInfo("vgId:%d, already set drop flag, ref:%d", pVnode->vgId, pVnode->refCount);
    vnodeRelease(pVnode);
    return TSDB_CODE_SUCCESS;
  }

  pVnode->cfg.dropped = 1;
  int32_t code = vnodeWriteCfg(pVnode->vgId, &pVnode->cfg);
  if (code == 0) {
    vInfo("vgId:%d, set drop flag, ref:%d", pVnode->vgId, pVnode->refCount);
    vnodeCleanupVnode(pVnode);
  } else {
    vError("vgId:%d, failed to set drop flag since %s", pVnode->vgId, tstrerror(code));
    pVnode->cfg.dropped = 0;
  }

  vnodeRelease(pVnode);
  return code;
}

int32_t vnodeSyncVnode(SVnode *pVnode) {
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeCompactVnode(SVnode *pVnode) {
  return TSDB_CODE_SUCCESS;
}

static void *vnodeOpenVnodeFunc(void *param) {
  SOpenVnodeThread *pThread = param;

  vDebug("thread:%d, start to open %d vnodes", pThread->threadIndex, pThread->vnodeNum);
  setThreadName("vnodeOpenVnode");

  for (int32_t v = 0; v < pThread->vnodeNum; ++v) {
    int32_t vgId = pThread->vnodeList[v];

    char stepDesc[TSDB_STEP_DESC_LEN] = {0};
    snprintf(stepDesc, TSDB_STEP_DESC_LEN, "vgId:%d, start to restore, %d of %d have been opened", vgId,
             tsVnode.openVnodes, tsVnode.totalVnodes);
    // (*vnodeInst()->fp.ReportStartup)("open-vnodes", stepDesc);

    if (vnodeOpenVnode(vgId) < 0) {
      vError("vgId:%d, failed to open vnode by thread:%d", vgId, pThread->threadIndex);
      pThread->failed++;
    } else {
      vDebug("vgId:%d, is opened by thread:%d", vgId, pThread->threadIndex);
      pThread->opened++;
    }

    atomic_add_fetch_32(&tsVnode.openVnodes, 1);
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

  tsVnode.totalVnodes = numOfVnodes;

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

    pThread->threadId = taosCreateThread(vnodeOpenVnodeFunc, pThread);
    if (pThread->threadId == NULL) {
      vError("thread:%d, failed to create thread to open vnode, reason:%s", pThread->threadIndex, strerror(errno));
    }
  }

  int32_t openVnodes = 0;
  int32_t failedVnodes = 0;
  for (int32_t t = 0; t < threadNum; ++t) {
    SOpenVnodeThread *pThread = &threads[t];
    taosDestoryThread(pThread->threadId);
    pThread->threadId = NULL;

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

static int32_t vnodeGetVnodeList(SVnode *vnodeList[], int32_t *numOfVnodes) {
  void *pIter = taosHashIterate(tsVnode.hash, NULL);
  while (pIter) {
    SVnode **pVnode = pIter;
    if (*pVnode) {
      (*numOfVnodes)++;
      if (*numOfVnodes >= TSDB_MAX_VNODES) {
        vError("vgId:%d, too many open vnodes, exist:%d max:%d", (*pVnode)->vgId, *numOfVnodes, TSDB_MAX_VNODES);
        continue;
      } else {
        vnodeList[*numOfVnodes - 1] = (*pVnode);
      }
    }

    pIter = taosHashIterate(tsVnode.hash, pIter);
  }

  return TSDB_CODE_SUCCESS;
}

static void vnodeCleanupVnodes() {
  SVnode* vnodeList[TSDB_MAX_VNODES] = {0};
  int32_t numOfVnodes = 0;

  int32_t code = vnodeGetVnodeList(vnodeList, &numOfVnodes);

  if (code != TSDB_CODE_SUCCESS) {
    vInfo("failed to get dnode list since code %d", code);
    return;
  }

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    vnodeCleanupVnode(vnodeList[i]);
  }

  vInfo("total vnodes:%d are all closed", numOfVnodes);
}

static void vnodeIncRef(void *ptNode) {
  assert(ptNode != NULL);

  SVnode **ppVnode = (SVnode **)ptNode;
  assert(ppVnode);
  assert(*ppVnode);

  SVnode *pVnode = *ppVnode;
  atomic_add_fetch_32(&pVnode->refCount, 1);
  vTrace("vgId:%d, get vnode, refCount:%d pVnode:%p", pVnode->vgId, pVnode->refCount, pVnode);
}

SVnode *vnodeAcquireInAllState(int32_t vgId) {
  SVnode *pVnode = NULL;

  // taosHashGetClone(tsVnode.hash, &vgId, sizeof(int32_t), vnodeIncRef, (void*)&pVnode);
  if (pVnode == NULL) {
    vDebug("vgId:%d, can't accquire since not exist", vgId);
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
    return NULL;
  }

  return pVnode;
}

SVnode *vnodeAcquire(int32_t vgId) {
  SVnode *pVnode = vnodeAcquireInAllState(vgId);
  if (pVnode == NULL) return NULL;

  if (vnodeInStatus(pVnode, TAOS_VN_STATUS_READY)) {
    return pVnode;
  } else {
    vDebug("vgId:%d, can't accquire since not in ready status", vgId);
    vnodeRelease(pVnode);
    terrno = TSDB_CODE_VND_INVALID_TSDB_STATE;
    return NULL;
  }
}

void vnodeRelease(SVnode *pVnode) {
  if (pVnode == NULL) return;

  int32_t refCount = atomic_sub_fetch_32(&pVnode->refCount, 1);
  int32_t vgId = pVnode->vgId;

  vTrace("vgId:%d, release vnode, refCount:%d pVnode:%p", vgId, refCount, pVnode);
  assert(refCount >= 0);

  if (refCount <= 0) {
    vDebug("vgId:%d, vnode will be destroyed, refCount:%d pVnode:%p", vgId, refCount, pVnode);
    vnodeDestroyVnode(pVnode);
    int32_t count = taosHashGetSize(tsVnode.hash);
    vDebug("vgId:%d, vnode is destroyed, vnodes:%d", vgId, count);
  }
}

static void vnodeInitMsgFp() {
  tsVnode.msgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE] = vnodeProcessMgmtMsg;
  tsVnode.msgFp[TSDB_MSG_TYPE_MD_ALTER_VNODE] = vnodeProcessMgmtMsg;
  tsVnode.msgFp[TSDB_MSG_TYPE_MD_SYNC_VNODE] = vnodeProcessMgmtMsg;
  tsVnode.msgFp[TSDB_MSG_TYPE_MD_COMPACT_VNODE] = vnodeProcessMgmtMsg;
  tsVnode.msgFp[TSDB_MSG_TYPE_MD_DROP_VNODE] = vnodeProcessMgmtMsg;
  tsVnode.msgFp[TSDB_MSG_TYPE_MD_ALTER_STREAM] = vnodeProcessMgmtMsg;
  tsVnode.msgFp[TSDB_MSG_TYPE_MD_CREATE_TABLE] = vnodeProcessWriteMsg;
  tsVnode.msgFp[TSDB_MSG_TYPE_MD_DROP_TABLE] = vnodeProcessWriteMsg;
  tsVnode.msgFp[TSDB_MSG_TYPE_MD_ALTER_TABLE] = vnodeProcessWriteMsg;
  tsVnode.msgFp[TSDB_MSG_TYPE_MD_DROP_STABLE] = vnodeProcessWriteMsg;
  tsVnode.msgFp[TSDB_MSG_TYPE_SUBMIT] = vnodeProcessWriteMsg;
  tsVnode.msgFp[TSDB_MSG_TYPE_UPDATE_TAG_VAL] = vnodeProcessWriteMsg;
  // mq related
  tsVnode.msgFp[TSDB_MSG_TYPE_MQ_CONNECT] = vnodeProcessWriteMsg;
  tsVnode.msgFp[TSDB_MSG_TYPE_MQ_DISCONNECT] = vnodeProcessWriteMsg;
  tsVnode.msgFp[TSDB_MSG_TYPE_MQ_ACK] = vnodeProcessWriteMsg;
  tsVnode.msgFp[TSDB_MSG_TYPE_MQ_RESET] = vnodeProcessWriteMsg;
  tsVnode.msgFp[TSDB_MSG_TYPE_MQ_QUERY] = vnodeProcessReadMsg;
  tsVnode.msgFp[TSDB_MSG_TYPE_MQ_CONSUME] = vnodeProcessReadMsg;
  // mq related end
  tsVnode.msgFp[TSDB_MSG_TYPE_QUERY] = vnodeProcessReadMsg;
  tsVnode.msgFp[TSDB_MSG_TYPE_FETCH] = vnodeProcessReadMsg;
}

int32_t vnodeInitMain() {
  vnodeInitMsgFp();

  tsVnode.hash = taosHashInit(TSDB_MIN_VNODES, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (tsVnode.hash == NULL) {
    vError("failed to init vnode mgmt");
    return -1;
  }

  vInfo("vnode main is initialized");
  return vnodeOpenVnodes();
}

void vnodeCleanupMain() {
  vnodeCleanupVnodes();
  taosHashCleanup(tsVnode.hash);
  tsVnode.hash = NULL;
}

static void vnodeBuildVloadMsg(SVnode *pVnode, SStatusMsg *pStatus) {
  int64_t totalStorage = 0;
  int64_t compStorage = 0;
  int64_t pointsWritten = 0;

  if (pStatus->openVnodes >= TSDB_MAX_VNODES) return;

  // if (pVnode->tsdb) {
  //   tsdbReportStat(pVnode->tsdb, &pointsWritten, &totalStorage, &compStorage);
  // }

  SVnodeLoad *pLoad = &pStatus->load[pStatus->openVnodes++];
  pLoad->vgId = htonl(pVnode->vgId);
  pLoad->totalStorage = htobe64(totalStorage);
  pLoad->compStorage = htobe64(compStorage);
  pLoad->pointsWritten = htobe64(pointsWritten);
  pLoad->status = pVnode->status;
  pLoad->role = pVnode->role;
}

void vnodeGetStatus(SStatusMsg *pStatus) {
  void *pIter = taosHashIterate(tsVnode.hash, NULL);
  while (pIter) {
    SVnode **pVnode = pIter;
    if (*pVnode) {
      vnodeBuildVloadMsg(*pVnode, pStatus);
    }
    pIter = taosHashIterate(tsVnode.hash, pIter);
  }
}

void vnodeSetAccess(SVgroupAccess *pAccess, int32_t numOfVnodes) {
  for (int32_t i = 0; i < numOfVnodes; ++i) {
    pAccess[i].vgId = htonl(pAccess[i].vgId);
    SVnode *pVnode = vnodeAcquire(pAccess[i].vgId);
    if (pVnode != NULL) {
      pVnode->accessState = pAccess[i].accessState;
      if (pVnode->accessState != TSDB_VN_ALL_ACCCESS) {
        vDebug("vgId:%d, access state is set to %d", pAccess[i].vgId, pVnode->accessState);
      }
      vnodeRelease(pVnode);
    }
  }
}

void vnodeProcessMsg(SRpcMsg *pMsg) {
  if (tsVnode.msgFp[pMsg->msgType]) {
    (*tsVnode.msgFp[pMsg->msgType])(pMsg);
  } else {
    assert(0);
  }
}
