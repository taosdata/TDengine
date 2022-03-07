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
#include "dndBnode.h"
#include "dndMgmt.h"
#include "dndMnode.h"
#include "dndQnode.h"
#include "dndSnode.h"
#include "dndTransport.h"
#include "dndVnodes.h"
#include "monitor.h"
#include "sync.h"
#include "tfs.h"
#include "wal.h"

static int8_t once = DND_ENV_INIT;

EStat dndGetStat(SDnode *pDnode) { return pDnode->stat; }

void dndSetStat(SDnode *pDnode, EStat stat) {
  dDebug("dnode status set from %s to %s", dndStatStr(pDnode->stat), dndStatStr(stat));
  pDnode->stat = stat;
}

const char *dndStatStr(EStat stat) {
  switch (stat) {
    case DND_STAT_INIT:
      return "init";
    case DND_STAT_RUNNING:
      return "running";
    case DND_STAT_STOPPED:
      return "stopped";
    default:
      return "unknown";
  }
}

void dndReportStartup(SDnode *pDnode, char *pName, char *pDesc) {
  SStartupReq *pStartup = &pDnode->startup;
  tstrncpy(pStartup->name, pName, TSDB_STEP_NAME_LEN);
  tstrncpy(pStartup->desc, pDesc, TSDB_STEP_DESC_LEN);
  pStartup->finished = 0;
}

void dndGetStartup(SDnode *pDnode, SStartupReq *pStartup) {
  memcpy(pStartup, &pDnode->startup, sizeof(SStartupReq));
  pStartup->finished = (dndGetStat(pDnode) == DND_STAT_RUNNING);
}

static TdFilePtr dndCheckRunning(char *dataDir) {
  char filepath[PATH_MAX] = {0};
  snprintf(filepath, sizeof(filepath), "%s/.running", dataDir);

  TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_CTEATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to lock file:%s since %s, quit", filepath, terrstr());
    return NULL;
  }

  int32_t ret = taosLockFile(pFile);
  if (ret != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to lock file:%s since %s, quit", filepath, terrstr());
    taosCloseFile(&pFile);
    return NULL;
  }

  return pFile;
}

static int32_t dndInitDir(SDnode *pDnode, SDnodeObjCfg *pCfg) {
  pDnode->pLockFile = dndCheckRunning(pCfg->dataDir);
  if (pDnode->pLockFile == NULL) {
    return -1;
  }

  char path[PATH_MAX + 100];
  snprintf(path, sizeof(path), "%s%smnode", pCfg->dataDir, TD_DIRSEP);
  pDnode->dir.mnode = tstrdup(path);
  snprintf(path, sizeof(path), "%s%svnode", pCfg->dataDir, TD_DIRSEP);
  pDnode->dir.vnodes = tstrdup(path);
  snprintf(path, sizeof(path), "%s%sdnode", pCfg->dataDir, TD_DIRSEP);
  pDnode->dir.dnode = tstrdup(path);
  snprintf(path, sizeof(path), "%s%ssnode", pCfg->dataDir, TD_DIRSEP);
  pDnode->dir.snode = tstrdup(path);
  snprintf(path, sizeof(path), "%s%sbnode", pCfg->dataDir, TD_DIRSEP);
  pDnode->dir.bnode = tstrdup(path);

  if (pDnode->dir.mnode == NULL || pDnode->dir.vnodes == NULL || pDnode->dir.dnode == NULL ||
      pDnode->dir.snode == NULL || pDnode->dir.bnode == NULL) {
    dError("failed to malloc dir object");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (taosMkDir(pDnode->dir.dnode) != 0) {
    dError("failed to create dir:%s since %s", pDnode->dir.dnode, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosMkDir(pDnode->dir.mnode) != 0) {
    dError("failed to create dir:%s since %s", pDnode->dir.mnode, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosMkDir(pDnode->dir.vnodes) != 0) {
    dError("failed to create dir:%s since %s", pDnode->dir.vnodes, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosMkDir(pDnode->dir.snode) != 0) {
    dError("failed to create dir:%s since %s", pDnode->dir.snode, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosMkDir(pDnode->dir.bnode) != 0) {
    dError("failed to create dir:%s since %s", pDnode->dir.bnode, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  memcpy(&pDnode->cfg, pCfg, sizeof(SDnodeObjCfg));
  return 0;
}

static void dndCloseDir(SDnode *pDnode) {
  tfree(pDnode->dir.mnode);
  tfree(pDnode->dir.vnodes);
  tfree(pDnode->dir.dnode);
  tfree(pDnode->dir.snode);
  tfree(pDnode->dir.bnode);

  if (pDnode->pLockFile != NULL) {
    taosUnLockFile(pDnode->pLockFile);
    taosCloseFile(&pDnode->pLockFile);
    pDnode->pLockFile = NULL;
  }
}

SDnode *dndCreate(SDnodeObjCfg *pCfg) {
  dInfo("start to create dnode object");

  SDnode *pDnode = calloc(1, sizeof(SDnode));
  if (pDnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    dError("failed to create dnode object since %s", terrstr());
    return NULL;
  }

  dndSetStat(pDnode, DND_STAT_INIT);

  if (dndInitDir(pDnode, pCfg) != 0) {
    dError("failed to init dnode dir since %s", terrstr());
    dndClose(pDnode);
    return NULL;
  }

  SDiskCfg dCfg = {0};
  tstrncpy(dCfg.dir, pDnode->cfg.dataDir, TSDB_FILENAME_LEN);
  dCfg.level = 0;
  dCfg.primary = 1;
  SDiskCfg *pDisks = pDnode->cfg.pDisks;
  int32_t   numOfDisks = pDnode->cfg.numOfDisks;
  if (numOfDisks <= 0 || pDisks == NULL) {
    pDisks = &dCfg;
    numOfDisks = 1;
  }

  pDnode->pTfs = tfsOpen(pDisks, numOfDisks);
  if (pDnode->pTfs == NULL) {
    dError("failed to init tfs since %s", terrstr());
    dndClose(pDnode);
    return NULL;
  }

  if (dndInitMgmt(pDnode) != 0) {
    dError("failed to init mgmt since %s", terrstr());
    dndClose(pDnode);
    return NULL;
  }

  if (dndInitVnodes(pDnode) != 0) {
    dError("failed to init vnodes since %s", terrstr());
    dndClose(pDnode);
    return NULL;
  }

  if (dndInitQnode(pDnode) != 0) {
    dError("failed to init qnode since %s", terrstr());
    dndClose(pDnode);
    return NULL;
  }

  if (dndInitSnode(pDnode) != 0) {
    dError("failed to init snode since %s", terrstr());
    dndClose(pDnode);
    return NULL;
  }

  if (dndInitBnode(pDnode) != 0) {
    dError("failed to init bnode since %s", terrstr());
    dndClose(pDnode);
    return NULL;
  }

  if (dndInitMnode(pDnode) != 0) {
    dError("failed to init mnode since %s", terrstr());
    dndClose(pDnode);
    return NULL;
  }

  if (dndInitTrans(pDnode) != 0) {
    dError("failed to init transport since %s", terrstr());
    dndClose(pDnode);
    return NULL;
  }

  dndSetStat(pDnode, DND_STAT_RUNNING);
  dndSendStatusReq(pDnode);
  dndReportStartup(pDnode, "TDengine", "initialized successfully");
  dInfo("dnode object is created, data:%p", pDnode);

  return pDnode;
}

void dndClose(SDnode *pDnode) {
  if (pDnode == NULL) return;

  if (dndGetStat(pDnode) == DND_STAT_STOPPED) {
    dError("dnode is shutting down, data:%p", pDnode);
    return;
  }

  dInfo("start to close dnode, data:%p", pDnode);
  dndSetStat(pDnode, DND_STAT_STOPPED);
  dndCleanupTrans(pDnode);
  dndStopMgmt(pDnode);
  dndCleanupMnode(pDnode);
  dndCleanupBnode(pDnode);
  dndCleanupSnode(pDnode);
  dndCleanupQnode(pDnode);
  dndCleanupVnodes(pDnode);
  dndCleanupMgmt(pDnode);
  tfsClose(pDnode->pTfs);

  dndCloseDir(pDnode);
  free(pDnode);
  dInfo("dnode object is closed, data:%p", pDnode);
}

int32_t dndInit() {
  if (atomic_val_compare_exchange_8(&once, DND_ENV_INIT, DND_ENV_READY) != DND_ENV_INIT) {
    terrno = TSDB_CODE_REPEAT_INIT;
    dError("failed to init dnode env since %s", terrstr());
    return -1;
  }

  taosIgnSIGPIPE();
  taosBlockSIGPIPE();
  taosResolveCRC();

  if (rpcInit() != 0) {
    dError("failed to init rpc since %s", terrstr());
    dndCleanup();
    return -1;
  }

  if (walInit() != 0) {
    dError("failed to init wal since %s", terrstr());
    dndCleanup();
    return -1;
  }

  SVnodeOpt vnodeOpt = {
      .nthreads = tsNumOfCommitThreads, .putReqToVQueryQFp = dndPutReqToVQueryQ, .sendReqToDnodeFp = dndSendReqToDnode};

  if (vnodeInit(&vnodeOpt) != 0) {
    dError("failed to init vnode since %s", terrstr());
    dndCleanup();
    return -1;
  }

  SMonCfg monCfg = {.maxLogs = tsMonitorMaxLogs, .port = tsMonitorPort, .server = tsMonitorFqdn, .comp = tsMonitorComp};
  if (monInit(&monCfg) != 0) {
    dError("failed to init monitor since %s", terrstr());
    dndCleanup();
    return -1;
  }

  dInfo("dnode env is initialized");
  return 0;
}

void dndCleanup() {
  if (atomic_val_compare_exchange_8(&once, DND_ENV_READY, DND_ENV_CLEANUP) != DND_ENV_READY) {
    dError("dnode env is already cleaned up");
    return;
  }

  walCleanUp();
  vnodeCleanup();
  rpcCleanup();
  monCleanup();

  taosStopCacheRefreshWorker();
  dInfo("dnode env is cleaned up");
}

int32_t dndGetMonitorDiskInfo(SDnode *pDnode, SMonDiskInfo *pInfo) {
  tstrncpy(pInfo->logdir.name, tsLogDir, sizeof(pInfo->logdir.name));
  pInfo->logdir.size = tsLogSpace.size;
  tstrncpy(pInfo->tempdir.name, tsTempDir, sizeof(pInfo->tempdir.name));
  pInfo->tempdir.size = tsTempSpace.size;

  return tfsGetMonitorInfo(pDnode->pTfs, pInfo);
}