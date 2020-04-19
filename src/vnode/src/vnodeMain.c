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
#include "ihash.h"
#include "taoserror.h"
#include "taosmsg.h"
#include "trpc.h"
#include "tsdb.h"
#include "ttime.h"
#include "ttimer.h"
#include "twal.h"
#include "tglobal.h"
#include "dnode.h"
#include "vnode.h"
#include "vnodeInt.h"
#include "vnodeLog.h"

static int32_t  tsOpennedVnodes;
static void    *tsDnodeVnodesHash;
static void     vnodeCleanUp(SVnodeObj *pVnode);
static void     vnodeBuildVloadMsg(char *pNode, void * param);
static int      vnodeWalCallback(void *arg);
static int32_t  vnodeSaveCfg(SMDCreateVnodeMsg *pVnodeCfg);
static int32_t  vnodeReadCfg(SVnodeObj *pVnode);
static int      vnodeWalCallback(void *arg);
static uint32_t vnodeGetFileInfo(void *ahandle, char *name, uint32_t *index, int32_t *size);
static int      vnodeGetWalInfo(void *ahandle, char *name, uint32_t *index);
static void     vnodeNotifyRole(void *ahandle, int8_t role);

static pthread_once_t  vnodeModuleInit = PTHREAD_ONCE_INIT;

#ifndef _SYNC
tsync_h syncStart(const SSyncInfo *info) { return NULL; }
int     syncForwardToPeer(tsync_h shandle, void *pHead, void *mhandle) { return 0; }
void    syncStop(tsync_h shandle) {}
int     syncReconfig(tsync_h shandle, const SSyncCfg * cfg) { return 0; }
int     syncGetNodesRole(tsync_h shandle, SNodesRole * cfg) { return 0; }
#endif

static void vnodeInit() {
  vnodeInitWriteFp();
  vnodeInitReadFp();

  tsDnodeVnodesHash = taosInitIntHash(TSDB_MAX_VNODES, sizeof(SVnodeObj *), taosHashInt);
  if (tsDnodeVnodesHash == NULL) {
    dError("failed to init vnode list");
  }
}

int32_t vnodeCreate(SMDCreateVnodeMsg *pVnodeCfg) {
  int32_t code;
  pthread_once(&vnodeModuleInit, vnodeInit);

  SVnodeObj *pTemp = (SVnodeObj *)taosGetIntHashData(tsDnodeVnodesHash, pVnodeCfg->cfg.vgId);
  if (pTemp != NULL) {
    dPrint("vgId:%d, vnode already exist, pVnode:%p", pVnodeCfg->cfg.vgId, pTemp);
    return TSDB_CODE_SUCCESS;
  }

  char rootDir[TSDB_FILENAME_LEN] = {0};
  sprintf(rootDir, "%s/vnode%d", tsVnodeDir, pVnodeCfg->cfg.vgId);
  if (mkdir(rootDir, 0755) != 0) {
    if (errno == EACCES) {
      return TSDB_CODE_NO_DISK_PERMISSIONS;
    } else if (errno == ENOSPC) {
      return TSDB_CODE_SERV_NO_DISKSPACE;
    } else if (errno == EEXIST) {
    } else {
      return TSDB_CODE_VG_INIT_FAILED;
    }
  }

  code = vnodeSaveCfg(pVnodeCfg);
  if (code != TSDB_CODE_SUCCESS) {
    dError("vgId:%d, failed to save vnode cfg, reason:%s", pVnodeCfg->cfg.vgId, tstrerror(code));
    return code;
  }

  STsdbCfg tsdbCfg = {0};
  tsdbCfg.precision           = pVnodeCfg->cfg.precision;
  tsdbCfg.compression         = -1;
  tsdbCfg.tsdbId              = pVnodeCfg->cfg.vgId;
  tsdbCfg.maxTables           = pVnodeCfg->cfg.maxSessions;
  tsdbCfg.daysPerFile         = pVnodeCfg->cfg.daysPerFile;
  tsdbCfg.minRowsPerFileBlock = -1;
  tsdbCfg.maxRowsPerFileBlock = -1;
  tsdbCfg.keep                = -1;
  tsdbCfg.maxCacheSize        = -1;

  char tsdbDir[TSDB_FILENAME_LEN] = {0};
  sprintf(tsdbDir, "%s/vnode%d/tsdb", tsVnodeDir, pVnodeCfg->cfg.vgId);
  code = tsdbCreateRepo(tsdbDir, &tsdbCfg, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    dError("vgId:%d, failed to create tsdb in vnode, reason:%s", pVnodeCfg->cfg.vgId, tstrerror(terrno));
    return terrno;
  }

  dPrint("vgId:%d, vnode is created, clog:%d", pVnodeCfg->cfg.vgId, pVnodeCfg->cfg.commitLog);
  code = vnodeOpen(pVnodeCfg->cfg.vgId, rootDir);

  return code;
}

int32_t vnodeDrop(int32_t vgId) {
  SVnodeObj **ppVnode = (SVnodeObj **)taosGetIntHashData(tsDnodeVnodesHash, vgId);
  if (ppVnode == NULL || *ppVnode == NULL) {
    dTrace("vgId:%d, failed to drop, vgId not exist", vgId);
    return TSDB_CODE_INVALID_VGROUP_ID;
  }

  SVnodeObj *pVnode = *ppVnode;
  dTrace("pVnode:%p vgId:%d, vnode will be dropped", pVnode, pVnode->vgId);
  pVnode->status = TAOS_VN_STATUS_DELETING;
  vnodeCleanUp(pVnode);
 
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeOpen(int32_t vnode, char *rootDir) {
  char temp[TSDB_FILENAME_LEN];
  pthread_once(&vnodeModuleInit, vnodeInit);

  SVnodeObj *pVnode = calloc(sizeof(SVnodeObj), 1);
  pVnode->vgId     = vnode;
  pVnode->status   = TAOS_VN_STATUS_INIT;
  pVnode->refCount = 1;
  pVnode->version  = 0;  
  taosAddIntHash(tsDnodeVnodesHash, pVnode->vgId, (char *)(&pVnode));

  int32_t code = vnodeReadCfg(pVnode);
  if (code != TSDB_CODE_SUCCESS) {
    dError("pVnode:%p vgId:%d, failed to read cfg file", pVnode, pVnode->vgId);
    taosDeleteIntHash(tsDnodeVnodesHash, pVnode->vgId);
    return code;
  }

  pVnode->wqueue = dnodeAllocateWqueue(pVnode);
  pVnode->rqueue = dnodeAllocateRqueue(pVnode);

  sprintf(temp, "%s/wal", rootDir);
  pVnode->wal      = walOpen(temp, &pVnode->walCfg);

  SSyncInfo syncInfo;
  syncInfo.vgId = pVnode->vgId;
  syncInfo.version = pVnode->version;
  syncInfo.syncCfg = pVnode->syncCfg;
  sprintf(syncInfo.path, "%s/tsdb/", rootDir);
  syncInfo.ahandle = pVnode;
  syncInfo.getWalInfo = vnodeGetWalInfo;
  syncInfo.getFileInfo = vnodeGetFileInfo;
  syncInfo.writeToCache = vnodeWriteToQueue;
  syncInfo.confirmForward = dnodeSendRpcWriteRsp; 
  syncInfo.notifyRole = vnodeNotifyRole;
  pVnode->sync     = syncStart(&syncInfo);

  pVnode->events   = NULL;
  pVnode->cq       = NULL;

  STsdbAppH appH = {0};
  appH.appH = (void *)pVnode;
  appH.walCallBack = vnodeWalCallback;

  sprintf(temp, "%s/tsdb", rootDir);
  void *pTsdb = tsdbOpenRepo(temp, &appH);
  if (pTsdb == NULL) {
    dError("pVnode:%p vgId:%d, failed to open tsdb at %s(%s)", pVnode, pVnode->vgId, temp, tstrerror(terrno));
    taosDeleteIntHash(tsDnodeVnodesHash, pVnode->vgId);
    return terrno;
  }

  pVnode->tsdb = pTsdb;

  walRestore(pVnode->wal, pVnode, vnodeWriteToQueue);

  pVnode->status = TAOS_VN_STATUS_READY;
  dTrace("pVnode:%p vgId:%d, vnode is opened in %s", pVnode, pVnode->vgId, rootDir);

  atomic_add_fetch_32(&tsOpennedVnodes, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeClose(int32_t vgId) {
  SVnodeObj **ppVnode = (SVnodeObj **)taosGetIntHashData(tsDnodeVnodesHash, vgId);
  if (ppVnode == NULL || *ppVnode == NULL) return 0;

  SVnodeObj *pVnode = *ppVnode;
  dTrace("pVnode:%p vgId:%d, vnode will be closed", pVnode, pVnode->vgId);
  pVnode->status = TAOS_VN_STATUS_CLOSING;
  vnodeCleanUp(pVnode);

  return 0;
}

void vnodeRelease(void *pVnodeRaw) {
  SVnodeObj *pVnode = pVnodeRaw;
  int32_t    vgId = pVnode->vgId;

  int32_t refCount = atomic_sub_fetch_32(&pVnode->refCount, 1);
  assert(refCount >= 0);

  if (refCount > 0) {
    dTrace("pVnode:%p vgId:%d, release vnode, refCount:%d", pVnode, vgId, refCount);
    return;
  }

  // remove read queue
  dnodeFreeRqueue(pVnode->rqueue);
  pVnode->rqueue = NULL;

  // remove write queue
  dnodeFreeWqueue(pVnode->wqueue);
  pVnode->wqueue = NULL;

  if (pVnode->status == TAOS_VN_STATUS_DELETING) {
    // remove the whole directory
  }

  free(pVnode);

  int32_t count = atomic_sub_fetch_32(&tsOpennedVnodes, 1);
  dTrace("pVnode:%p vgId:%d, vnode is released, vnodes:%d", pVnode, vgId, count);

  if (count <= 0) {
    taosCleanUpIntHash(tsDnodeVnodesHash);
    vnodeModuleInit = PTHREAD_ONCE_INIT;
    tsDnodeVnodesHash = NULL;
  }
}

void *vnodeGetVnode(int32_t vgId) {
  SVnodeObj **ppVnode = (SVnodeObj **)taosGetIntHashData(tsDnodeVnodesHash, vgId);
  if (ppVnode == NULL || *ppVnode == NULL) {
    terrno = TSDB_CODE_INVALID_VGROUP_ID;
    assert(false);
  }

  return *ppVnode;
}

void *vnodeAccquireVnode(int32_t vgId) {
  SVnodeObj *pVnode = vnodeGetVnode(vgId);
  if (pVnode == NULL) return pVnode;

  atomic_add_fetch_32(&pVnode->refCount, 1);
  dTrace("pVnode:%p vgId:%d, get vnode, refCount:%d", pVnode, pVnode->vgId, pVnode->refCount);

  return pVnode;
}

void *vnodeGetRqueue(void *pVnode) {
  return ((SVnodeObj *)pVnode)->rqueue; 
}

void *vnodeGetWqueue(int32_t vgId) {
  SVnodeObj *pVnode = vnodeAccquireVnode(vgId);
  if (pVnode == NULL) return NULL;
  return pVnode->wqueue;
} 

void *vnodeGetWal(void *pVnode) {
  return ((SVnodeObj *)pVnode)->wal; 
}

void vnodeBuildStatusMsg(void *param) {
  SDMStatusMsg *pStatus = param;
  taosVisitIntHashWithFp(tsDnodeVnodesHash, vnodeBuildVloadMsg, pStatus);
}

static void vnodeBuildVloadMsg(char *pNode, void * param) {
  SVnodeObj *pVnode = *(SVnodeObj **) pNode;
  if (pVnode->status == TAOS_VN_STATUS_DELETING) return;

  SDMStatusMsg *pStatus = param;
  if (pStatus->openVnodes >= TSDB_MAX_VNODES) return;

  SVnodeLoad *pLoad = &pStatus->load[pStatus->openVnodes++];
  pLoad->vgId = htonl(pVnode->vgId);
  pLoad->status = pVnode->status;
  pLoad->role = pVnode->role;
}

static void vnodeCleanUp(SVnodeObj *pVnode) {
  
  taosDeleteIntHash(tsDnodeVnodesHash, pVnode->vgId);

  //syncStop(pVnode->sync);
  tsdbCloseRepo(pVnode->tsdb);
  walClose(pVnode->wal);

  vnodeRelease(pVnode);
}

// TODO: this is a simple implement
static int vnodeWalCallback(void *arg) {
  SVnodeObj *pVnode = arg;
  return walRenew(pVnode->wal);
}

static uint32_t vnodeGetFileInfo(void *ahandle, char *name, uint32_t *index, int32_t *size) {
  // SVnodeObj *pVnode = ahandle;
  //tsdbGetFileInfo(pVnode->tsdb, name, index, size);
  return 0;
}

static int vnodeGetWalInfo(void *ahandle, char *name, uint32_t *index) {
  SVnodeObj *pVnode = ahandle;
  return walGetWalFile(pVnode->wal, name, index);
}

static void vnodeNotifyRole(void *ahandle, int8_t role) {
  SVnodeObj *pVnode = ahandle;
  pVnode->role = role;
}

static int32_t vnodeSaveCfg(SMDCreateVnodeMsg *pVnodeCfg) {
  char cfgFile[TSDB_FILENAME_LEN * 2] = {0};
  sprintf(cfgFile, "%s/vnode%d/config", tsVnodeDir, pVnodeCfg->cfg.vgId);

  FILE *fp = fopen(cfgFile, "w");
  if (!fp) return errno;

  fprintf(fp, "commitLog %d\n", pVnodeCfg->cfg.commitLog);
  fprintf(fp, "wals %d\n", 3);
  fprintf(fp, "arbitratorIp %d\n", pVnodeCfg->vpeerDesc[0].ip);
  fprintf(fp, "quorum %d\n", 1);
  fprintf(fp, "replica %d\n", pVnodeCfg->cfg.replications);
  for (int32_t i = 0; i < pVnodeCfg->cfg.replications; i++) {
    fprintf(fp, "index%d nodeId %d nodeIp %u name n%d\n", i, pVnodeCfg->vpeerDesc[i].dnodeId, pVnodeCfg->vpeerDesc[i].ip, pVnodeCfg->vpeerDesc[i].dnodeId);
  }

  fclose(fp);
  dTrace("vgId:%d, save vnode cfg successed", pVnodeCfg->cfg.vgId);

  return TSDB_CODE_SUCCESS;
}

// TODO: this is a simple implement
static int32_t vnodeReadCfg(SVnodeObj *pVnode) {
  char option[5][16] = {0};
  char cfgFile[TSDB_FILENAME_LEN * 2] = {0};
  sprintf(cfgFile, "%s/vnode%d/config", tsVnodeDir, pVnode->vgId);

  FILE *fp = fopen(cfgFile, "r");
  if (!fp) return errno;

  int32_t commitLog = -1;
  int32_t num = fscanf(fp, "%s %d", option[0], &commitLog);
  if (num != 2) return TSDB_CODE_INVALID_FILE_FORMAT;
  if (strcmp(option[0], "commitLog") != 0) return TSDB_CODE_INVALID_FILE_FORMAT;
  if (commitLog == -1) return TSDB_CODE_INVALID_FILE_FORMAT;
  pVnode->walCfg.commitLog = (int8_t)commitLog;

  int32_t wals = -1;
  num = fscanf(fp, "%s %d", option[0], &wals);
  if (num != 2) return TSDB_CODE_INVALID_FILE_FORMAT;
  if (strcmp(option[0], "wals") != 0) return TSDB_CODE_INVALID_FILE_FORMAT;
  if (wals == -1) return TSDB_CODE_INVALID_FILE_FORMAT;
  pVnode->walCfg.wals = (int8_t)wals;
  pVnode->walCfg.keep = 0;

  int32_t arbitratorIp = -1;
  num = fscanf(fp, "%s %u", option[0], &arbitratorIp);
  if (num != 2) return TSDB_CODE_INVALID_FILE_FORMAT;
  if (strcmp(option[0], "arbitratorIp") != 0) return TSDB_CODE_INVALID_FILE_FORMAT;
  if (arbitratorIp == -1) return TSDB_CODE_INVALID_FILE_FORMAT;
  pVnode->syncCfg.arbitratorIp = arbitratorIp;

  int32_t quorum = -1;
  num = fscanf(fp, "%s %d", option[0], &quorum);
  if (num != 2) return TSDB_CODE_INVALID_FILE_FORMAT;
  if (strcmp(option[0], "quorum") != 0) return TSDB_CODE_INVALID_FILE_FORMAT;
  if (quorum == -1) return TSDB_CODE_INVALID_FILE_FORMAT;
  pVnode->syncCfg.quorum = (int8_t)quorum;

  int32_t replica = -1;
  num = fscanf(fp, "%s %d", option[0], &replica);
  if (num != 2) return TSDB_CODE_INVALID_FILE_FORMAT;
  if (strcmp(option[0], "replica") != 0) return TSDB_CODE_INVALID_FILE_FORMAT;
  if (replica == -1) return TSDB_CODE_INVALID_FILE_FORMAT;
  pVnode->syncCfg.replica = (int8_t)replica;

  for (int32_t i = 0; i < replica; ++i) {
    int32_t  dnodeId = -1;
    uint32_t dnodeIp = -1;
    num = fscanf(fp, "%s %s %d %s %u %s %s", option[0], option[1], &dnodeId, option[2], &dnodeIp, option[3], pVnode->syncCfg.nodeInfo[i].name);
    if (num != 7) return TSDB_CODE_INVALID_FILE_FORMAT;
    if (strcmp(option[1], "nodeId") != 0) return TSDB_CODE_INVALID_FILE_FORMAT;
    if (strcmp(option[2], "nodeIp") != 0) return TSDB_CODE_INVALID_FILE_FORMAT;
    if (strcmp(option[3], "name") != 0) return TSDB_CODE_INVALID_FILE_FORMAT;
    if (dnodeId == -1) return TSDB_CODE_INVALID_FILE_FORMAT;
    if (dnodeIp == -1) return TSDB_CODE_INVALID_FILE_FORMAT;
    pVnode->syncCfg.nodeInfo[i].nodeId = dnodeId;
    pVnode->syncCfg.nodeInfo[i].nodeIp = dnodeIp;
  }

  fclose(fp);
  dTrace("pVnode:%p vgId:%d, read vnode cfg successed", pVnode, pVnode->vgId);

  return TSDB_CODE_SUCCESS;
}
