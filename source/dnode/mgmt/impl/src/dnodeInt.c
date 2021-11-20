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
#include "dnodeDnode.h"
#include "dnodeMnode.h"
#include "dnodeTransport.h"
#include "dnodeVnodes.h"
#include "sync.h"
#include "tcache.h"
#include "tconfig.h"
#include "tnote.h"
#include "tstep.h"
#include "wal.h"

EStat dnodeGetStat(SDnode *pDnode) { return pDnode->stat; }

void dnodeSetStat(SDnode *pDnode, EStat stat) {
  dDebug("dnode stat set from %s to %s", dnodeStatStr(pDnode->stat), dnodeStatStr(stat));
  pDnode->stat = stat;
}

char *dnodeStatStr(EStat stat) {
  switch (stat) {
    case DN_STAT_INIT:
      return "init";
    case DN_STAT_RUNNING:
      return "running";
    case DN_STAT_STOPPED:
      return "stopped";
    default:
      return "unknown";
  }
}

void dnodeReportStartup(SDnode *pDnode, char *name, char *desc) {
  SStartupMsg *pStartup = &pDnode->startup;
  tstrncpy(pStartup->name, name, strlen(pStartup->name));
  tstrncpy(pStartup->desc, desc, strlen(pStartup->desc));
  pStartup->finished = 0;
}

void dnodeGetStartup(SDnode *pDnode, SStartupMsg *pStartup) {
  memcpy(pStartup, &pDnode->startup, sizeof(SStartupMsg);
  pStartup->finished = (dnodeGetStat(pDnode) == DN_STAT_RUNNING);
}

static int32_t dnodeCheckRunning(char *dataDir) {
  char filepath[PATH_MAX] = {0};
  snprintf(filepath, sizeof(filepath), "%s/.running", dataDir);

  FileFd fd = taosOpenFileCreateWriteTrunc(filepath);
  if (fd < 0) {
    dError("failed to open lock file:%s since %s, quit", filepath, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  int32_t ret = taosLockFile(fd);
  if (ret != 0) {
    dError("failed to lock file:%s since %s, quit", filepath, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    taosCloseFile(fd);
    return -1;
  }

  return 0;
}

static int32_t dnodeInitDisk(SDnode *pDnode, char *dataDir) {
  char path[PATH_MAX];
  snprintf(path, PATH_MAX, "%s/mnode", dataDir);
  pDnode->dir.mnode = strdup(path);

  sprintf(path, PATH_MAX, "%s/vnode", dataDir);
  pDnode->dir.vnodes = strdup(path);

  sprintf(path, PATH_MAX, "%s/dnode", dataDir);
  pDnode->dir.dnode = strdup(path);

  if (pDnode->dir.mnode == NULL || pDnode->dir.vnodes == NULL || pDnode->dir.dnode == NULL) {
    dError("failed to malloc dir object");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (!taosMkDir(pDnode->dir.dnode)) {
    dError("failed to create dir:%s since %s", pDnode->dir.dnode, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (!taosMkDir(pDnode->dir.mnode)) {
    dError("failed to create dir:%s since %s", pDnode->dir.mnode, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (!taosMkDir(pDnode->dir.vnodes)) {
    dError("failed to create dir:%s since %s", pDnode->dir.vnodes, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (dnodeCheckRunning(dataDir) != 0) {
    return -1;
  }

  return 0;
}

static int32_t dnodeInitEnv(SDnode *pDnode, const char *cfgPath) {
  taosIgnSIGPIPE();
  taosBlockSIGPIPE();
  taosResolveCRC();
  taosInitGlobalCfg();
  taosReadGlobalLogCfg();
  taosSetCoreDump(tsEnableCoreFile);

  if (!taosMkDir(tsLogDir)) {
    printf("failed to create dir: %s, reason: %s\n", tsLogDir, strerror(errno));
    return -1;
  }

  char temp[TSDB_FILENAME_LEN];
  sprintf(temp, "%s/taosdlog", tsLogDir);
  if (taosInitLog(temp, tsNumOfLogLines, 1) < 0) {
    dError("failed to init log file\n");
    return -1;
  }

  if (!taosReadGlobalCfg()) {
    taosPrintGlobalCfg();
    dError("TDengine read global config failed");
    return -1;
  }

  taosInitNotes();

  if (taosCheckGlobalCfg() != 0) {
    dError("TDengine check global config failed");
    return -1;
  }

  if (dnodeInitDisk(pDnode, tsDataDir) != 0) {
    dError("TDengine failed to init directory");
    return -1;
  }

  return 0;
}

static void dnodeCleanupEnv(SDnode *pDnode) {
  if (pDnode->dir.mnode != NULL) {
    tfree(pDnode->dir.mnode);
  }

  if (pDnode->dir.vnodes != NULL) {
    tfree(pDnode->dir.vnodes);
  }

  if (pDnode->dir.dnode != NULL) {
    tfree(pDnode->dir.dnode);
  }

  taosCloseLog();
  taosStopCacheRefreshWorker();
}

SDnode *dnodeInit(const char *cfgPath) {
  SDnode *pDnode = calloc(1, sizeof(pDnode));
  if (pDnode == NULL) {
    dError("failed to create dnode object");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  dInfo("start to initialize TDengine");
  dnodeSetStat(pDnode, DN_STAT_INIT);

  if (dnodeInitEnv(pDnode, cfgPath) != 0) {
    dError("failed to init env");
    dnodeCleanup(pDnode);
    return NULL;
  }

  if (rpcInit() != 0) {
    dError("failed to init rpc env");
    dnodeCleanup(pDnode);
    return NULL;
  }

  if (walInit() != 0) {
    dError("failed to init wal env");
    dnodeCleanup(pDnode);
    return NULL;
  }

  if (dnodeInitDnode(pDnode) != 0) {
    dError("failed to init dnode");
    dnodeCleanup(pDnode);
    return NULL;
  }

  if (dnodeInitVnodes(pDnode) != 0) {
    dError("failed to init vnodes");
    dnodeCleanup(pDnode);
    return NULL;
  }

  if (dnodeInitMnode(pDnode) != 0) {
    dError("failed to init mnode");
    dnodeCleanup(pDnode);
    return NULL;
  }

  if (dnodeInitTrans(pDnode) != 0) {
    dError("failed to init transport");
    dnodeCleanup(pDnode);
    return NULL;
  }

  dnodeSetStat(pDnode, DN_STAT_RUNNING);
  dnodeReportStartup(pDnode, "TDengine", "initialized successfully");
  dInfo("TDengine is initialized successfully");

  return 0;
}

void dnodeCleanup(SDnode *pDnode) {
  if (dnodeGetStat(pDnode) == DN_STAT_STOPPED) {
    dError("dnode is shutting down");
    return;
  }

  dInfo("start to cleanup TDengine");
  dnodeSetStat(pDnode, DN_STAT_STOPPED);
  dnodeCleanupTrans(pDnode);
  dnodeCleanupMnode(pDnode);
  dnodeCleanupVnodes(pDnode);
  dnodeCleanupDnode(pDnode);
  walCleanUp();
  rpcCleanup();

  dInfo("TDengine is cleaned up successfully");
  dnodeCleanupEnv(pDnode);
  free(pDnode);
}
