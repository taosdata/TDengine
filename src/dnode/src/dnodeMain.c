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
#include "taos.h"
#include "tutil.h"
#include "tconfig.h"
#include "tglobal.h"
#include "dnode.h"
#include "dnodeInt.h"
#include "dnodeMgmt.h"
#include "dnodePeer.h"
#include "dnodeModule.h"
#include "dnodeVRead.h"
#include "dnodeShell.h"
#include "dnodeVWrite.h"

static int32_t dnodeInitStorage();
static void dnodeCleanupStorage();
static void dnodeSetRunStatus(SDnodeRunStatus status);
static void dnodeCheckDataDirOpenned(char *dir);
static SDnodeRunStatus tsDnodeRunStatus = TSDB_DNODE_RUN_STATUS_STOPPED;

int32_t dnodeInitSystem() {
  dnodeSetRunStatus(TSDB_DNODE_RUN_STATUS_INITIALIZE);
  tscEmbedded  = 1;
  taosBlockSIGPIPE();
  taosResolveCRC();
  taosInitGlobalCfg();
  taosReadGlobalLogCfg();
  taosSetCoreDump();
  signal(SIGPIPE, SIG_IGN);

  struct stat dirstat;
  if (stat(tsLogDir, &dirstat) < 0) {
    mkdir(tsLogDir, 0755);
  }

  char temp[TSDB_FILENAME_LEN];
  sprintf(temp, "%s/taosdlog", tsLogDir);
  if (taosInitLog(temp, tsNumOfLogLines, 1) < 0) {
    printf("failed to init log file\n");
  }

  if (!taosReadGlobalCfg() || !taosCheckGlobalCfg()) {
    taosPrintGlobalCfg();
    dError("TDengine read global config failed");
    return -1;
  }
  taosPrintGlobalCfg();

  dPrint("start to initialize TDengine on %s", tsLocalEp);

  if (dnodeInitStorage() != 0) return -1;
  if (dnodeInitRead() != 0) return -1;
  if (dnodeInitWrite() != 0) return -1;
  if (dnodeInitClient() != 0) return -1;
  if (dnodeInitServer() != 0) return -1;
  if (dnodeInitMgmt() != 0) return -1;
  if (dnodeInitModules() != 0) return -1;
  if (dnodeInitShell() != 0) return -1;

  dnodeStartModules();
  dnodeSetRunStatus(TSDB_DNODE_RUN_STATUS_RUNING);

  dPrint("TDengine is initialized successfully");

  return 0;
}

void dnodeCleanUpSystem() {
  if (dnodeGetRunStatus() != TSDB_DNODE_RUN_STATUS_STOPPED) {
    dnodeSetRunStatus(TSDB_DNODE_RUN_STATUS_STOPPED);
    dnodeCleanupShell();
    dnodeCleanUpModules();
    dnodeCleanupMgmt();
    dnodeCleanupServer();
    dnodeCleanupClient();
    dnodeCleanupWrite();
    dnodeCleanupRead();
    dnodeCleanupStorage();
    taos_cleanup();
    taosCloseLog();
  }
}

SDnodeRunStatus dnodeGetRunStatus() {
  return tsDnodeRunStatus;
}

static void dnodeSetRunStatus(SDnodeRunStatus status) {
  tsDnodeRunStatus = status;
}

static void dnodeCheckDataDirOpenned(char *dir) {
  char filepath[256] = {0};
  sprintf(filepath, "%s/.running", dir);

  int32_t fd  = open(filepath, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
  int32_t ret = flock(fd, LOCK_EX | LOCK_NB);
  if (ret != 0) {
    dError("failed to lock file:%s ret:%d, database may be running, quit", filepath, ret);
    close(fd);
    exit(0);
  }
}

static int32_t dnodeInitStorage() {
  struct stat dirstat;
  if (stat(tsDataDir, &dirstat) < 0) {
    mkdir(tsDataDir, 0755);
  }

  sprintf(tsMnodeDir, "%s/mnode", tsDataDir);
  sprintf(tsVnodeDir, "%s/vnode", tsDataDir);
  sprintf(tsDnodeDir, "%s/dnode", tsDataDir);
  mkdir(tsVnodeDir, 0755);
  mkdir(tsDnodeDir, 0755);

  dnodeCheckDataDirOpenned(tsDnodeDir);

  dPrint("storage directory is initialized");
  return 0;
}

static void dnodeCleanupStorage() {}

bool  dnodeIsFirstDeploy() {
  return strcmp(tsFirst, tsLocalEp) == 0;
}
