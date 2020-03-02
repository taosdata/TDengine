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
#include "taosdef.h"
#include "taoserror.h"
#include "tcrc32c.h"
#include "tlog.h"
#include "tmodule.h"
#include "tsched.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"
#include "http.h"
#include "dnode.h"
#include "dnodeMgmt.h"
#include "dnodeModule.h"
#include "dnodeShell.h"
#include "dnodeSystem.h"
#include "dnodeVnodeMgmt.h"

#ifdef CLUSTER
//#include "acct.h"
//#include "admin.h"
//#include "cluster.h"
//#include "grant.h"
//#include "replica.h"
//#include "storage.h"
#endif

static pthread_mutex_t tsDnodeMutex;
static SDnodeRunStatus tsDnodeRunStatus = TSDB_DNODE_RUN_STATUS_STOPPED;

static int32_t dnodeInitRpcQHandle();
static int32_t dnodeInitQueryQHandle();
static int32_t dnodeInitTmrCtl();

void     *tsDnodeTmr;
void     **tsRpcQhandle;
void     *tsDnodeMgmtQhandle;
void     *tsQueryQhandle;
int32_t  tsVnodePeers   = TSDB_VNODES_SUPPORT - 1;
int32_t  tsMaxQueues;
uint32_t tsRebootTime;

static void dnodeInitVnodesLock() {
  pthread_mutex_init(&tsDnodeMutex, NULL);
}

void dnodeLockVnodes() {
  pthread_mutex_lock(&tsDnodeMutex);
}

void dnodeUnLockVnodes() {
  pthread_mutex_unlock(&tsDnodeMutex);
}

static void dnodeCleanVnodesLock() {
  pthread_mutex_destroy(&tsDnodeMutex);
}

SDnodeRunStatus dnodeGetRunStatus() {
  return tsDnodeRunStatus;
}

void dnodeSetRunStatus(SDnodeRunStatus status) {
  tsDnodeRunStatus = status;
}

void dnodeCleanUpSystem() {
  tclearModuleStatus(TSDB_MOD_MGMT);

  if (dnodeGetRunStatus() == TSDB_DNODE_RUN_STATUS_STOPPED) {
    return;
  } else {
    dnodeSetRunStatus(TSDB_DNODE_RUN_STATUS_STOPPED);
  }



  dnodeCleanupShell();
  dnodeCleanUpModules();
  dnodeCleanupVnodes();
  taosCloseLogger();
  dnodeCleanupStorage();
  dnodeCleanVnodesLock();
}

void dnodeCheckDataDirOpenned(const char *dir) {
  char filepath[256] = {0};
  sprintf(filepath, "%s/.running", dir);
  int32_t fd  = open(filepath, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
  int32_t ret = flock(fd, LOCK_EX | LOCK_NB);
  if (ret != 0) {
    dError("failed to lock file:%s ret:%d, database may be running, quit", filepath, ret);
    exit(0);
  }
}

void dnodeInitPlugins() {
#ifdef CLUSTER
  acctInit();
#endif
}

int32_t dnodeInitSystem() {
  tsRebootTime = taosGetTimestampSec();
  tscEmbedded  = 1;

  dnodeSetRunStatus(TSDB_DNODE_RUN_STATUS_INITIALIZE);
  taosResolveCRC();

  // Read global configuration.
  tsReadGlobalLogConfig();

  struct stat dirstat;
  if (stat(logDir, &dirstat) < 0) {
    mkdir(logDir, 0755);
  }

  char temp[128];
  sprintf(temp, "%s/taosdlog", logDir);
  if (taosInitLog(temp, tsNumOfLogLines, 1) < 0) {
    printf("failed to init log file\n");
  }

  if (!tsReadGlobalConfig()) {
    tsPrintGlobalConfig();
    dError("TDengine read global config failed");
    return -1;
  }

  if (dnodeInitStorage() != 0) {
    dError("TDengine init tier directory failed");
    return -1;
  }

  dnodeInitMgmtIp();

  tsPrintGlobalConfig();

  dPrint("Server IP address is:%s", tsPrivateIp);

  taosSetCoreDump();

  signal(SIGPIPE, SIG_IGN);

  dnodeAllocModules();

  dnodeInitVnodesLock();

  dPrint("starting to initialize TDengine ...");

  if (dnodeInitRpcQHandle() < 0) {
    dError("failed to init query qhandle, exit");
    return -1;
  }

  if (dnodeCheckSystem() < 0) {
    return -1;
  }

  if (dnodeInitModules() < 0) {
    return -1;
  }

  if (dnodeInitTmrCtl() < 0) {
    dError("failed to init timer, exit");
    return -1;
  }

  if (dnodeInitQueryQHandle() < 0) {
    dError("failed to init query qhandle, exit");
    return -1;
  }

  if (dnodeOpenVnodes() < 0) {
    dError("failed to init vnode storage");
    return -1;
  }

  int32_t numOfThreads = (1.0 - tsRatioOfQueryThreads) * tsNumOfCores * tsNumOfThreadsPerCore / 2.0;
  if (numOfThreads < 1) numOfThreads = 1;
  if (dnodeInitPeers(numOfThreads) < 0) {
    dError("failed to init vnode peer communication");
    return -1;
  }

  if (dnodeInitMgmt() < 0) {
    dError("failed to init communication to mgmt");
    return -1;
  }

  if (dnodeInitShell() < 0) {
    dError("failed to init communication to shell");
    return -1;
  }

  dnodeStartModules();

  dnodeSetRunStatus(TSDB_DNODE_RUN_STATUS_RUNING);

  dPrint("TDengine is initialized successfully");

  return 0;
}

int32_t dnodeInitStorageImp() {
  struct stat dirstat;
  strcpy(tsDirectory, dataDir);
  if (stat(dataDir, &dirstat) < 0) {
    mkdir(dataDir, 0755);
  }

  char fileName[128];

  sprintf(fileName, "%s/tsdb", tsDirectory);
  mkdir(fileName, 0755);

  sprintf(fileName, "%s/data", tsDirectory);
  mkdir(fileName, 0755);

  sprintf(tsMgmtDirectory, "%s/mgmt", tsDirectory);
  sprintf(tsDirectory, "%s/tsdb", dataDir);
  dnodeCheckDataDirOpenned(dataDir);

  return 0;
}

int32_t (*dnodeInitStorage)() = dnodeInitStorageImp;

void dnodeCleanupStorageImp() {}

void (*dnodeCleanupStorage)() = dnodeCleanupStorageImp;

static int32_t dnodeInitQueryQHandle() {
  int32_t numOfThreads = tsRatioOfQueryThreads * tsNumOfCores * tsNumOfThreadsPerCore;
  if (numOfThreads < 1) {
    numOfThreads = 1;
  }

  int32_t maxQueueSize = tsNumOfVnodesPerCore * tsNumOfCores * tsSessionsPerVnode;
  dTrace("query task queue initialized, max slot:%d, task threads:%d", maxQueueSize, numOfThreads);

  tsQueryQhandle = taosInitSchedulerWithInfo(maxQueueSize, numOfThreads, "query", tsDnodeTmr);

  return 0;
}

static int32_t dnodeInitTmrCtl() {
  tsDnodeTmr = taosTmrInit(TSDB_MAX_VNODES * (tsVnodePeers + 10) + tsSessionsPerVnode + 1000, 200, 60000,
                             "DND-vnode");
  if (tsDnodeTmr == NULL) {
    dError("failed to init timer, exit");
    return -1;
  }

  return 0;
}

static int32_t dnodeInitRpcQHandle() {
  tsMaxQueues = (1.0 - tsRatioOfQueryThreads) * tsNumOfCores * tsNumOfThreadsPerCore / 2.0;
  if (tsMaxQueues < 1) {
    tsMaxQueues = 1;
  }

  tsRpcQhandle = malloc(tsMaxQueues * sizeof(void *));

  for (int32_t i = 0; i < tsMaxQueues; ++i) {
    tsRpcQhandle[i] = taosInitScheduler(tsSessionsPerVnode, 1, "dnode");
  }

  tsDnodeMgmtQhandle = taosInitScheduler(tsSessionsPerVnode, 1, "mgmt");

  return 0;
}

int32_t dnodeCheckSystemImp() {
  return 0;
}

int32_t (*dnodeCheckSystem)() = dnodeCheckSystemImp;

void dnodeParseParameterKImp() {}

void (*dnodeParseParameterK)() = dnodeParseParameterKImp;

int32_t dnodeInitPeersImp(int32_t numOfThreads) {
  return 0;
}

int32_t (*dnodeInitPeers)(int32_t numOfThreads) = dnodeInitPeersImp;




