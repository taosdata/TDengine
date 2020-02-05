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
#include "tsdb.h"
#include "tlog.h"
#include "ttimer.h"
#include "dnodeMgmt.h"
#include "dnodeModule.h"
#include "dnodeService.h"
#include "dnodeSystem.h"
#include "monitorSystem.h"
#include "httpSystem.h"
#include "mgmtSystem.h"

#include "vnode.h"

pthread_mutex_t dmutex;
extern int      vnodeSelectReqNum;
extern int      vnodeInsertReqNum;
void *          tsStatusTimer = NULL;
bool            tsDnodeStopping = false;

// internal global, not configurable
void *   vnodeTmrCtrl;
void **  rpcQhandle;
void *   dmQhandle;
void *   queryQhandle;
int      tsVnodePeers = TSDB_VNODES_SUPPORT - 1;
int      tsMaxQueues;
uint32_t tsRebootTime;
int    (*dnodeInitStorage)() = NULL;
void   (*dnodeCleanupStorage)() = NULL;
int    (*dnodeCheckSystem)() = NULL;

int32_t dnodeInitRpcQHandle();
int32_t dnodeInitQueryQHandle();
int32_t dnodeInitTmrCtl();
void dnodeInitPlugin();
void dnodeCountRequestImp(SCountInfo *info);

void dnodeCleanUpSystem() {
  if (tsDnodeStopping) {
    return;
  } else {
    tsDnodeStopping = true;
  }

  if (tsStatusTimer != NULL) {
    taosTmrStopA(&tsStatusTimer);
    tsStatusTimer = NULL;
  }

  dnodeCleanUpModules();

  vnodeCleanUpVnodes();

  taosCloseLogger();

  dnodeCleanupStorage();
}

void dnodeCheckDbRunning(const char* dir) {
  char filepath[256] = {0};
  sprintf(filepath, "%s/.running", dir);
  int fd = open(filepath, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
  int ret = flock(fd, LOCK_EX | LOCK_NB);
  if (ret != 0) {
    dError("failed to lock file:%s ret:%d, database may be running, quit", filepath, ret);
    exit(0);
  }
}

int dnodeInitSystem() {
  char        temp[128];
  struct stat dirstat;

  dnodeInitPlugin();

  taosResolveCRC();

  tsRebootTime = taosGetTimestampSec();
  tscEmbedded = 1;

  // Read global configuration.
  tsReadGlobalLogConfig();

  if (stat(logDir, &dirstat) < 0) {
    mkdir(logDir, 0755);
  }

  sprintf(temp, "%s/taosdlog", logDir);
  if (taosInitLog(temp, tsNumOfLogLines, 1) < 0) {
    printf("failed to init log file\n");
  }

  if (!tsReadGlobalConfig()) {  // TODO : Change this function
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

  pthread_mutex_init(&dmutex, NULL);

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

  if (vnodeInitStore() < 0) {
    dError("failed to init vnode storage");
    return -1;
  }

  int numOfThreads = (1.0 - tsRatioOfQueryThreads) * tsNumOfCores * tsNumOfThreadsPerCore / 2.0;
  if (numOfThreads < 1) numOfThreads = 1;
  if (vnodeInitPeer(numOfThreads) < 0) {
    dError("failed to init vnode peer communication");
    return -1;
  }

  if (dnodeInitMgmtConn() < 0) {
    dError("failed to init communication to mgmt");
    return -1;
  }

  if (vnodeInitShell() < 0) {
    dError("failed to init communication to shell");
    return -1;
  }

  if (vnodeInitVnodes() < 0) {
    dError("failed to init store");
    return -1;
  }

  mnodeCountRequestFp = dnodeCountRequestImp;

  dnodeStartModules();

  dPrint("TDengine is initialized successfully");

  return 0;
}

void dnodeResetSystem() {
  dPrint("reset the system ...");
  for (int vnode = 0; vnode < TSDB_MAX_VNODES; ++vnode) {
    vnodeRemoveVnode(vnode);
  }
  mgmtStopSystem();
}

void dnodeCountRequestImp(SCountInfo *info) {
  httpGetReqCount(&info->httpReqNum);
  info->selectReqNum = atomic_exchange_32(&vnodeSelectReqNum, 0);
  info->insertReqNum = atomic_exchange_32(&vnodeInsertReqNum, 0);
}

int dnodeInitStorageComImp() {
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

  sprintf(mgmtDirectory, "%s/mgmt", tsDirectory);
  sprintf(tsDirectory, "%s/tsdb", dataDir);
  dnodeCheckDbRunning(dataDir);

  return 0;
}

void dnodeCleanupStorageComImp() {}

int32_t dnodeInitQueryQHandle() {
  int numOfThreads = tsRatioOfQueryThreads * tsNumOfCores * tsNumOfThreadsPerCore;
  if (numOfThreads < 1) {
    numOfThreads = 1;
  }

  int32_t maxQueueSize = tsNumOfVnodesPerCore * tsNumOfCores * tsSessionsPerVnode;
  dTrace("query task queue initialized, max slot:%d, task threads:%d", maxQueueSize,numOfThreads);

  queryQhandle = taosInitSchedulerWithInfo(maxQueueSize, numOfThreads, "query", vnodeTmrCtrl);

  return 0;
}

int32_t dnodeInitTmrCtl() {
  vnodeTmrCtrl = taosTmrInit(TSDB_MAX_VNODES * (tsVnodePeers + 10) + tsSessionsPerVnode + 1000, 200, 60000, "DND-vnode");
  if (vnodeTmrCtrl == NULL) {
    dError("failed to init timer, exit");
    return -1;
  }

  return 0;
}

int32_t dnodeInitRpcQHandle() {
  tsMaxQueues = (1.0 - tsRatioOfQueryThreads)*tsNumOfCores*tsNumOfThreadsPerCore / 2.0;
  if (tsMaxQueues < 1) tsMaxQueues = 1;

  rpcQhandle = malloc(tsMaxQueues*sizeof(void *));

  for (int i=0; i< tsMaxQueues; ++i )
    rpcQhandle[i] = taosInitScheduler(tsSessionsPerVnode, 1, "dnode");

  dmQhandle = taosInitScheduler(tsSessionsPerVnode, 1, "mgmt");

  return 0;
}


int dnodeCheckSystemComImp() {
  return 0;
}

void dnodeInitPlugin() {
  dnodeInitMgmtConn = dnodeInitMgmtConnEdgeImp;
  dnodeInitMgmtIp = dnodeInitMgmtIpEdgeImp;

  taosBuildRspMsgToMnodeWithSize = taosBuildRspMsgToMnodeWithSizeEdgeImp;
  taosBuildReqMsgToMnodeWithSize = taosBuildReqMsgToMnodeWithSizeEdgeImp;
  taosBuildRspMsgToMnode = taosBuildRspMsgToMnodeEdgeImp;
  taosBuildReqMsgToMnode = taosBuildReqMsgToMnodeEdgeImp;
  taosSendMsgToMnode = taosSendMsgToMnodeEdgeImp;
  taosSendSimpleRspToMnode = taosSendSimpleRspToMnodeEdgeImp;

  dnodeParseParameterK = dnodeParseParameterKComImp;
  dnodeCheckSystem = dnodeCheckSystemComImp;
  dnodeInitStorage = dnodeInitStorageComImp;
  dnodeCleanupStorage = dnodeCleanupStorageComImp;
  dnodeStartModules = dnodeStartModulesEdgeImp;
}


