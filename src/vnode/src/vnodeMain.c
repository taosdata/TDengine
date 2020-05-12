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
#include "tutil.h"
#include "trpc.h"
#include "tsdb.h"
#include "ttime.h"
#include "ttimer.h"
#include "cJSON.h"
#include "tglobal.h"
#include "dnode.h"
#include "vnode.h"
#include "vnodeInt.h"

static int32_t  tsOpennedVnodes;
static void    *tsDnodeVnodesHash;
static void     vnodeCleanUp(SVnodeObj *pVnode);
static void     vnodeBuildVloadMsg(char *pNode, void * param);
static int      vnodeWalCallback(void *arg);
static int32_t  vnodeSaveCfg(SMDCreateVnodeMsg *pVnodeCfg);
static int32_t  vnodeReadCfg(SVnodeObj *pVnode);
static int32_t  vnodeSaveVersion(SVnodeObj *pVnode);
static bool     vnodeReadVersion(SVnodeObj *pVnode);
static int      vnodeWalCallback(void *arg);
static uint32_t vnodeGetFileInfo(void *ahandle, char *name, uint32_t *index, int32_t *size);
static int      vnodeGetWalInfo(void *ahandle, char *name, uint32_t *index);
static void     vnodeNotifyRole(void *ahandle, int8_t role);
static void     vnodeNotifyFileSynced(void *ahandle);

static pthread_once_t  vnodeModuleInit = PTHREAD_ONCE_INIT;

#ifndef _SYNC
tsync_h syncStart(const SSyncInfo *info) { return NULL; }
int     syncForwardToPeer(tsync_h shandle, void *pHead, void *mhandle) { return 0; }
void    syncStop(tsync_h shandle) {}
int     syncReconfig(tsync_h shandle, const SSyncCfg * cfg) { return 0; }
int     syncGetNodesRole(tsync_h shandle, SNodesRole * cfg) { return 0; }
void    syncConfirmForward(tsync_h shandle, uint64_t version, int32_t code) {}
#endif

static void vnodeInit() {
  vnodeInitWriteFp();
  vnodeInitReadFp();

  tsDnodeVnodesHash = taosInitIntHash(TSDB_MAX_VNODES, sizeof(SVnodeObj *), taosHashInt);
  if (tsDnodeVnodesHash == NULL) {
    vError("failed to init vnode list");
  }
}

int32_t vnodeCreate(SMDCreateVnodeMsg *pVnodeCfg) {
  int32_t code;
  pthread_once(&vnodeModuleInit, vnodeInit);

  SVnodeObj *pTemp = (SVnodeObj *)taosGetIntHashData(tsDnodeVnodesHash, pVnodeCfg->cfg.vgId);
  if (pTemp != NULL) {
    vPrint("vgId:%d, vnode already exist, pVnode:%p", pVnodeCfg->cfg.vgId, pTemp);
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
    vError("vgId:%d, failed to save vnode cfg, reason:%s", pVnodeCfg->cfg.vgId, tstrerror(code));
    return code;
  }

  STsdbCfg tsdbCfg = {0};
  tsdbCfg.tsdbId              = pVnodeCfg->cfg.vgId;
  tsdbCfg.cacheBlockSize      = pVnodeCfg->cfg.cacheBlockSize;
  tsdbCfg.totalBlocks         = pVnodeCfg->cfg.totalBlocks;
  tsdbCfg.maxTables           = pVnodeCfg->cfg.maxTables;
  tsdbCfg.daysPerFile         = pVnodeCfg->cfg.daysPerFile;
  tsdbCfg.keep                = pVnodeCfg->cfg.daysToKeep;
  tsdbCfg.minRowsPerFileBlock = pVnodeCfg->cfg.minRowsPerFileBlock;
  tsdbCfg.maxRowsPerFileBlock = pVnodeCfg->cfg.maxRowsPerFileBlock;
  tsdbCfg.precision           = pVnodeCfg->cfg.precision;
  tsdbCfg.compression         = pVnodeCfg->cfg.compression;;
  
  char tsdbDir[TSDB_FILENAME_LEN] = {0};
  sprintf(tsdbDir, "%s/vnode%d/tsdb", tsVnodeDir, pVnodeCfg->cfg.vgId);
  code = tsdbCreateRepo(tsdbDir, &tsdbCfg, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to create tsdb in vnode, reason:%s", pVnodeCfg->cfg.vgId, tstrerror(code));
    return TSDB_CODE_VG_INIT_FAILED;
  }

  vPrint("vgId:%d, vnode is created, clog:%d", pVnodeCfg->cfg.vgId, pVnodeCfg->cfg.walLevel);
  code = vnodeOpen(pVnodeCfg->cfg.vgId, rootDir);

  return code;
}

int32_t vnodeDrop(int32_t vgId) {
  SVnodeObj **ppVnode = (SVnodeObj **)taosGetIntHashData(tsDnodeVnodesHash, vgId);
  if (ppVnode == NULL || *ppVnode == NULL) {
    vTrace("vgId:%d, failed to drop, vgId not exist", vgId);
    return TSDB_CODE_INVALID_VGROUP_ID;
  }

  SVnodeObj *pVnode = *ppVnode;
  vTrace("vgId:%d, vnode will be dropped", pVnode->vgId);
  pVnode->status = TAOS_VN_STATUS_DELETING;
  vnodeCleanUp(pVnode);
 
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeAlter(void *param, SMDCreateVnodeMsg *pVnodeCfg) {
  SVnodeObj *pVnode = param;
  pVnode->status = TAOS_VN_STATUS_UPDATING;

  int32_t code = vnodeSaveCfg(pVnodeCfg);
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to save vnode cfg, reason:%s", pVnodeCfg->cfg.vgId, tstrerror(code));
    return code;
  }

  code = vnodeReadCfg(pVnode);
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to read cfg file", pVnode->vgId);
    taosDeleteIntHash(tsDnodeVnodesHash, pVnode->vgId);
    return code;
  }

  code = syncReconfig(pVnode->sync, &pVnode->syncCfg);
  if (code != TSDB_CODE_SUCCESS) {
    vTrace("vgId:%d, failed to alter vnode, canot reconfig sync, result:%s", pVnode->vgId,
           tstrerror(code));
    return code;
  }

  code = tsdbConfigRepo(pVnode->tsdb, &pVnode->tsdbCfg);
  if (code != TSDB_CODE_SUCCESS) {
    vTrace("vgId:%d, failed to alter vnode, canot reconfig tsdb, result:%s", pVnode->vgId,
           tstrerror(code));
    return code;
  }

  pVnode->status = TAOS_VN_STATUS_READY;

  vTrace("vgId:%d, vnode is altered", pVnode->vgId);
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
  pVnode->tsdbCfg.tsdbId = pVnode->vgId;
  taosAddIntHash(tsDnodeVnodesHash, pVnode->vgId, (char *)(&pVnode));
  
  int32_t code = vnodeReadCfg(pVnode);
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to read cfg file", pVnode->vgId);
    taosDeleteIntHash(tsDnodeVnodesHash, pVnode->vgId);
    return code;
  }

  vnodeReadVersion(pVnode);
  
  pVnode->wqueue = dnodeAllocateWqueue(pVnode);
  pVnode->rqueue = dnodeAllocateRqueue(pVnode);

  SCqCfg cqCfg = {0};
  sprintf(cqCfg.user, "root");
  strcpy(cqCfg.pass, tsInternalPass);
  cqCfg.vgId = vnode;
  cqCfg.cqWrite = vnodeWriteToQueue;
  pVnode->cq = cqOpen(pVnode, &cqCfg);

  STsdbAppH appH = {0};
  appH.appH = (void *)pVnode;
  appH.walCallBack = vnodeWalCallback;
  appH.cqH = pVnode->cq;

  sprintf(temp, "%s/tsdb", rootDir);
  pVnode->tsdb = tsdbOpenRepo(temp, &appH);
  if (pVnode->tsdb == NULL) {
    vError("vgId:%d, failed to open tsdb at %s(%s)", pVnode->vgId, temp, tstrerror(terrno));
    taosDeleteIntHash(tsDnodeVnodesHash, pVnode->vgId);
    return terrno;
  }

  sprintf(temp, "%s/wal", rootDir);
  pVnode->wal = walOpen(temp, &pVnode->walCfg);
  walRestore(pVnode->wal, pVnode, vnodeWriteToQueue);

  SSyncInfo syncInfo;
  syncInfo.vgId = pVnode->vgId;
  syncInfo.version = pVnode->version;
  syncInfo.syncCfg = pVnode->syncCfg;
  sprintf(syncInfo.path, "%s", rootDir);
  syncInfo.ahandle = pVnode;
  syncInfo.getWalInfo = vnodeGetWalInfo;
  syncInfo.getFileInfo = vnodeGetFileInfo;
  syncInfo.writeToCache = vnodeWriteToQueue;
  syncInfo.confirmForward = dnodeSendRpcWriteRsp; 
  syncInfo.notifyRole = vnodeNotifyRole;
  syncInfo.notifyFileSynced = vnodeNotifyFileSynced;
  pVnode->sync = syncStart(&syncInfo);

  // start continuous query
  if (pVnode->role == TAOS_SYNC_ROLE_MASTER) 
    cqStart(pVnode->cq);

  pVnode->events = NULL;

  pVnode->status = TAOS_VN_STATUS_READY;
  vTrace("vgId:%d, vnode is opened in %s, pVnode:%p", pVnode->vgId, rootDir, pVnode);

  atomic_add_fetch_32(&tsOpennedVnodes, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeClose(int32_t vgId) {
  SVnodeObj **ppVnode = (SVnodeObj **)taosGetIntHashData(tsDnodeVnodesHash, vgId);
  if (ppVnode == NULL || *ppVnode == NULL) return 0;

  SVnodeObj *pVnode = *ppVnode;
  vTrace("vgId:%d, vnode will be closed", pVnode->vgId);
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
    vTrace("vgId:%d, release vnode, refCount:%d", vgId, refCount);
    return;
  }

  // remove read queue
  dnodeFreeRqueue(pVnode->rqueue);
  pVnode->rqueue = NULL;

  // remove write queue
  dnodeFreeWqueue(pVnode->wqueue);
  pVnode->wqueue = NULL;

  if (pVnode->status == TAOS_VN_STATUS_DELETING) {
    char rootDir[TSDB_FILENAME_LEN] = {0};
    sprintf(rootDir, "%s/vnode%d", tsVnodeDir, vgId);
    taosRemoveDir(rootDir);
  }

  free(pVnode);

  int32_t count = atomic_sub_fetch_32(&tsOpennedVnodes, 1);
  vTrace("vgId:%d, vnode is released, vnodes:%d", pVnode, vgId, count);

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
    vPrint("vgId:%d, not exist", vgId);
    return NULL;
  }

  return *ppVnode;
}

void *vnodeAccquireVnode(int32_t vgId) {
  SVnodeObj *pVnode = vnodeGetVnode(vgId);
  if (pVnode == NULL) return pVnode;

  atomic_add_fetch_32(&pVnode->refCount, 1);
  vTrace("vgId:%d, get vnode, refCount:%d", pVnode->vgId, pVnode->refCount);

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
  pLoad->cfgVersion = htonl(pVnode->cfgVersion);
  pLoad->totalStorage = htobe64(pLoad->totalStorage);
  pLoad->compStorage = htobe64(pLoad->compStorage);
  pLoad->pointsWritten = htobe64(pLoad->pointsWritten);
  pLoad->status = pVnode->status;
  pLoad->role = pVnode->role;
  pLoad->replica = pVnode->syncCfg.replica;
}

static void vnodeCleanUp(SVnodeObj *pVnode) {
  taosDeleteIntHash(tsDnodeVnodesHash, pVnode->vgId);

  if (pVnode->sync) {
    syncStop(pVnode->sync);
    pVnode->sync = NULL;
  }

  cqClose(pVnode->cq);
  pVnode->cq = NULL;

  tsdbCloseRepo(pVnode->tsdb);
  pVnode->tsdb = NULL;

  walClose(pVnode->wal);
  pVnode->wal = NULL;

  vnodeSaveVersion(pVnode);
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

  if (pVnode->role == TAOS_SYNC_ROLE_MASTER) 
    cqStart(pVnode->cq);
  else 
    cqStop(pVnode->cq);
}

static void vnodeNotifyFileSynced(void *ahandle) {
  SVnodeObj *pVnode = ahandle;
  vTrace("vgId:%d, data file is synced", pVnode->vgId);

  // close tsdb, then open tsdb
}

static int32_t vnodeSaveCfg(SMDCreateVnodeMsg *pVnodeCfg) {
  char cfgFile[TSDB_FILENAME_LEN + 30] = {0};
  sprintf(cfgFile, "%s/vnode%d/config.json", tsVnodeDir, pVnodeCfg->cfg.vgId);
  FILE *fp = fopen(cfgFile, "w");
  if (!fp) {
    vError("vgId:%d, failed to open vnode cfg file for write, file:%s error:%s", pVnodeCfg->cfg.vgId, cfgFile,
           strerror(errno));
    return errno;
  }

  int32_t len = 0;
  int32_t maxLen = 1000;
  char *  content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");

  len += snprintf(content + len, maxLen - len, "  \"cfgVersion\": %d,\n", pVnodeCfg->cfg.cfgVersion);
  len += snprintf(content + len, maxLen - len, "  \"cacheBlockSize\": %d,\n", pVnodeCfg->cfg.cacheBlockSize);
  len += snprintf(content + len, maxLen - len, "  \"totalBlocks\": %d,\n", pVnodeCfg->cfg.totalBlocks);
  len += snprintf(content + len, maxLen - len, "  \"maxTables\": %d,\n", pVnodeCfg->cfg.maxTables);
  len += snprintf(content + len, maxLen - len, "  \"daysPerFile\": %d,\n", pVnodeCfg->cfg.daysPerFile);
  len += snprintf(content + len, maxLen - len, "  \"daysToKeep\": %d,\n", pVnodeCfg->cfg.daysToKeep);
  len += snprintf(content + len, maxLen - len, "  \"daysToKeep1\": %d,\n", pVnodeCfg->cfg.daysToKeep1);
  len += snprintf(content + len, maxLen - len, "  \"daysToKeep2\": %d,\n", pVnodeCfg->cfg.daysToKeep2);
  len += snprintf(content + len, maxLen - len, "  \"minRowsPerFileBlock\": %d,\n", pVnodeCfg->cfg.minRowsPerFileBlock);
  len += snprintf(content + len, maxLen - len, "  \"maxRowsPerFileBlock\": %d,\n", pVnodeCfg->cfg.maxRowsPerFileBlock);
  len += snprintf(content + len, maxLen - len, "  \"commitTime\": %d,\n", pVnodeCfg->cfg.commitTime);  
  len += snprintf(content + len, maxLen - len, "  \"precision\": %d,\n", pVnodeCfg->cfg.precision);
  len += snprintf(content + len, maxLen - len, "  \"compression\": %d,\n", pVnodeCfg->cfg.compression);
  len += snprintf(content + len, maxLen - len, "  \"walLevel\": %d,\n", pVnodeCfg->cfg.walLevel);
  len += snprintf(content + len, maxLen - len, "  \"replica\": %d,\n", pVnodeCfg->cfg.replications);
  len += snprintf(content + len, maxLen - len, "  \"wals\": %d,\n", pVnodeCfg->cfg.wals);
  len += snprintf(content + len, maxLen - len, "  \"quorum\": %d,\n", pVnodeCfg->cfg.quorum);
  
  len += snprintf(content + len, maxLen - len, "  \"nodeInfos\": [{\n");
  for (int32_t i = 0; i < pVnodeCfg->cfg.replications; i++) {
    len += snprintf(content + len, maxLen - len, "    \"nodeId\": %d,\n", pVnodeCfg->nodes[i].nodeId);
    len += snprintf(content + len, maxLen - len, "    \"nodeEp\": \"%s\"\n", pVnodeCfg->nodes[i].nodeEp);

    if (i < pVnodeCfg->cfg.replications - 1) {
      len += snprintf(content + len, maxLen - len, "  },{\n");
    } else {
      len += snprintf(content + len, maxLen - len, "  }]\n");
    }
  }
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  fclose(fp);
  free(content);

  vPrint("vgId:%d, save vnode cfg successed", pVnodeCfg->cfg.vgId);

  return 0;
}

static int32_t vnodeReadCfg(SVnodeObj *pVnode) {
  char cfgFile[TSDB_FILENAME_LEN + 30] = {0};
  sprintf(cfgFile, "%s/vnode%d/config.json", tsVnodeDir, pVnode->vgId);
  FILE *fp = fopen(cfgFile, "r");
  if (!fp) {
    vError("vgId:%d, failed to open vnode cfg file for read, file:%s, error:%s", pVnode->vgId,
           cfgFile, strerror(errno));
    return errno;
  }

  int   ret = TSDB_CODE_OTHERS;
  int   maxLen = 1000;
  char *content = calloc(1, maxLen + 1);
  int   len = fread(content, 1, maxLen, fp);
  if (len <= 0) {
    free(content);
    fclose(fp);
    vError("vgId:%d, failed to read vnode cfg, content is null", pVnode->vgId);
    return false;
  }

  cJSON *root = cJSON_Parse(content);
  if (root == NULL) {
    vError("vgId:%d, failed to read vnode cfg, invalid json format", pVnode->vgId);
    goto PARSE_OVER;
  }

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

  cJSON *maxTables = cJSON_GetObjectItem(root, "maxTables");
  if (!maxTables || maxTables->type != cJSON_Number) {
    vError("vgId:%d, failed to read vnode cfg, maxTables not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.maxTables = maxTables->valueint;

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

  cJSON *commitTime = cJSON_GetObjectItem(root, "commitTime");
  if (!commitTime || commitTime->type != cJSON_Number) {
    vError("vgId:%d, failed to read vnode cfg, commitTime not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->tsdbCfg.commitTime = (int8_t)commitTime->valueint;

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
    vError("failed to read vnode cfg, quorum not found", pVnode->vgId);
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

  ret = 0;

  vPrint("vgId:%d, read vnode cfg successed, replcia:%d", pVnode->vgId, pVnode->syncCfg.replica);
  for (int32_t i = 0; i < pVnode->syncCfg.replica; i++) {
    vPrint("vgId:%d, dnode:%d, %s:%d", pVnode->vgId, pVnode->syncCfg.nodeInfo[i].nodeId,
           pVnode->syncCfg.nodeInfo[i].nodeFqdn, pVnode->syncCfg.nodeInfo[i].nodePort);
  }

PARSE_OVER:
  free(content);
  cJSON_Delete(root);
  fclose(fp);
  return ret;
}

static int32_t vnodeSaveVersion(SVnodeObj *pVnode) {
  char versionFile[TSDB_FILENAME_LEN + 30] = {0};
  sprintf(versionFile, "%s/vnode%d/version.json", tsVnodeDir, pVnode->vgId);
  FILE *fp = fopen(versionFile, "w");
  if (!fp) {
    vError("vgId:%d, failed to open vnode version file for write, file:%s error:%s", pVnode->vgId,
           versionFile, strerror(errno));
    return errno;
  }

  int32_t len = 0;
  int32_t maxLen = 30;
  char *  content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"version\": %" PRId64 "\n", pVnode->version);
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  fclose(fp);
  free(content);

  vPrint("vgId:%d, save vnode version:%" PRId64 " successed", pVnode->vgId, pVnode->version);

  return 0;
}

static bool vnodeReadVersion(SVnodeObj *pVnode) {
  char versionFile[TSDB_FILENAME_LEN + 30] = {0};
  sprintf(versionFile, "%s/vnode%d/version.json", tsVnodeDir, pVnode->vgId);
  FILE *fp = fopen(versionFile, "r");
  if (!fp) {
    if (errno != ENOENT) {
      vError("vgId:%d, failed to open version file:%s error:%s", pVnode->vgId, versionFile, strerror(errno));
    }
    return false;
  }

  bool  ret = false;
  int   maxLen = 100;
  char *content = calloc(1, maxLen + 1);
  int   len = fread(content, 1, maxLen, fp);
  if (len <= 0) {
    free(content);
    fclose(fp);
    vPrint("vgId:%d, failed to read vnode version, content is null", pVnode->vgId);
    return false;
  }

  cJSON *root = cJSON_Parse(content);
  if (root == NULL) {
    vError("vgId:%d, failed to read vnode version, invalid json format", pVnode->vgId);
    goto PARSE_OVER;
  }

  cJSON *version = cJSON_GetObjectItem(root, "version");
  if (!version || version->type != cJSON_Number) {
    vError("vgId:%d, failed to read vnode version, version not found", pVnode->vgId);
    goto PARSE_OVER;
  }
  pVnode->version = version->valueint;

  ret = true;

  vPrint("vgId:%d, read vnode version successed, version:%%" PRId64, pVnode->vgId, pVnode->version);

PARSE_OVER:
  free(content);
  cJSON_Delete(root);
  fclose(fp);
  return ret;
}
