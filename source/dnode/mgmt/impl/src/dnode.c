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
#include "sync.h"
#include "wal.h"

EStat dndGetStat(SDnode *pDnode) { return pDnode->stat; }

void dndSetStat(SDnode *pDnode, EStat stat) {
  dDebug("dnode status set from %s to %s", dndStatStr(pDnode->stat), dndStatStr(stat));
  pDnode->stat = stat;
}

char *dndStatStr(EStat stat) {
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

static FileFd dndCheckRunning(char *dataDir) {
  char filepath[PATH_MAX] = {0};
  snprintf(filepath, sizeof(filepath), "%s/.running", dataDir);

  FileFd fd = taosOpenFileCreateWriteTrunc(filepath);
  if (fd < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to lock file:%s since %s, quit", filepath, terrstr());
    return -1;
  }

  int32_t ret = taosLockFile(fd);
  if (ret != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to lock file:%s since %s, quit", filepath, terrstr());
    taosCloseFile(fd);
    return -1;
  }

  return fd;
}

static int32_t dndInitEnv(SDnode *pDnode, SDnodeOpt *pOption) {
  pDnode->lockFd = dndCheckRunning(pOption->dataDir);
  if (pDnode->lockFd < 0) {
    return -1;
  }

  char path[PATH_MAX + 100];
  snprintf(path, sizeof(path), "%s%smnode", pOption->dataDir, TD_DIRSEP);
  pDnode->dir.mnode = tstrdup(path);
  snprintf(path, sizeof(path), "%s%svnode", pOption->dataDir, TD_DIRSEP);
  pDnode->dir.vnodes = tstrdup(path);
  snprintf(path, sizeof(path), "%s%sdnode", pOption->dataDir, TD_DIRSEP);
  pDnode->dir.dnode = tstrdup(path);
  snprintf(path, sizeof(path), "%s%ssnode", pOption->dataDir, TD_DIRSEP);
  pDnode->dir.snode = tstrdup(path);
  snprintf(path, sizeof(path), "%s%sbnode", pOption->dataDir, TD_DIRSEP);
  pDnode->dir.bnode = tstrdup(path);

  if (pDnode->dir.mnode == NULL || pDnode->dir.vnodes == NULL || pDnode->dir.dnode == NULL) {
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

  memcpy(&pDnode->opt, pOption, sizeof(SDnodeOpt));
  return 0;
}

static void dndCleanupEnv(SDnode *pDnode) {
  tfree(pDnode->dir.mnode);
  tfree(pDnode->dir.vnodes);
  tfree(pDnode->dir.dnode);
  tfree(pDnode->dir.snode);
  tfree(pDnode->dir.bnode);

  if (pDnode->lockFd >= 0) {
    taosUnLockFile(pDnode->lockFd);
    taosCloseFile(pDnode->lockFd);
    pDnode->lockFd = 0;
  }

  taosStopCacheRefreshWorker();
}

SDnode *dndInit(SDnodeOpt *pOption) {
  taosIgnSIGPIPE();
  taosBlockSIGPIPE();
  taosResolveCRC();

  SDnode *pDnode = calloc(1, sizeof(SDnode));
  if (pDnode == NULL) {
    dError("failed to create dnode object");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  dInfo("start to initialize TDengine");
  dndSetStat(pDnode, DND_STAT_INIT);

  if (dndInitEnv(pDnode, pOption) != 0) {
    dError("failed to init env");
    dndCleanup(pDnode);
    return NULL;
  }

  if (rpcInit() != 0) {
    dError("failed to init rpc env");
    dndCleanup(pDnode);
    return NULL;
  }

  if (walInit() != 0) {
    dError("failed to init wal env");
    dndCleanup(pDnode);
    return NULL;
  }

  if (vnodeInit(pDnode->opt.numOfCommitThreads) != 0) {
    dError("failed to init vnode env");
    dndCleanup(pDnode);
    return NULL;
  }

  if (dndInitDnode(pDnode) != 0) {
    dError("failed to init dnode");
    dndCleanup(pDnode);
    return NULL;
  }

  if (dndInitVnodes(pDnode) != 0) {
    dError("failed to init vnodes");
    dndCleanup(pDnode);
    return NULL;
  }

  if (dndInitQnode(pDnode) != 0) {
    dError("failed to init qnode");
    dndCleanup(pDnode);
    return NULL;
  }

  if (dndInitSnode(pDnode) != 0) {
    dError("failed to init snode");
    dndCleanup(pDnode);
    return NULL;
  }

  if (dndInitBnode(pDnode) != 0) {
    dError("failed to init bnode");
    dndCleanup(pDnode);
    return NULL;
  }

  if (dndInitMnode(pDnode) != 0) {
    dError("failed to init mnode");
    dndCleanup(pDnode);
    return NULL;
  }

  if (dndInitTrans(pDnode) != 0) {
    dError("failed to init transport");
    dndCleanup(pDnode);
    return NULL;
  }

  dndSetStat(pDnode, DND_STAT_RUNNING);
  dndSendStatusReq(pDnode);
  dndReportStartup(pDnode, "TDengine", "initialized successfully");
  dInfo("TDengine is initialized successfully, pDnode:%p", pDnode);

  return pDnode;
}

void dndCleanup(SDnode *pDnode) {
  if (pDnode == NULL) return;

  if (dndGetStat(pDnode) == DND_STAT_STOPPED) {
    dError("dnode is shutting down");
    return;
  }

  dInfo("start to cleanup TDengine");
  dndSetStat(pDnode, DND_STAT_STOPPED);
  dndCleanupTrans(pDnode);
  dndCleanupMnode(pDnode);
  dndCleanupBnode(pDnode);
  dndCleanupSnode(pDnode);
  dndCleanupQnode(pDnode);
  dndCleanupVnodes(pDnode);
  dndCleanupDnode(pDnode);
  vnodeClear();
  walCleanUp();
  rpcCleanup();

  dndCleanupEnv(pDnode);
  free(pDnode);
  dInfo("TDengine is cleaned up successfully");
}
