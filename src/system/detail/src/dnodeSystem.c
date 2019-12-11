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

#include "mgmt.h"
#include "vnode.h"

#include "dnodeSystem.h"
#include "httpSystem.h"
#include "monitorSystem.h"
#include "tcrc32c.h"
#include "tglobalcfg.h"
#include "vnode.h"

SModule         tsModule[TSDB_MOD_MAX] = {0};
uint32_t        tsModuleStatus = 0;
pthread_mutex_t dmutex;
extern int      vnodeSelectReqNum;
extern int      vnodeInsertReqNum;
void *          tsStatusTimer = NULL;
bool            tsDnodeStopping = false;

int  dnodeCheckConfig();
void dnodeCountRequest(SCountInfo *info);

void dnodeInitModules() {
  tsModule[TSDB_MOD_MGMT].name = "mgmt";
  tsModule[TSDB_MOD_MGMT].initFp = mgmtInitSystem;
  tsModule[TSDB_MOD_MGMT].cleanUpFp = mgmtCleanUpSystem;
  tsModule[TSDB_MOD_MGMT].startFp = mgmtStartSystem;
  tsModule[TSDB_MOD_MGMT].stopFp = mgmtStopSystem;
  tsModule[TSDB_MOD_MGMT].num = tsNumOfMPeers;
  tsModule[TSDB_MOD_MGMT].curNum = 0;
  tsModule[TSDB_MOD_MGMT].equalVnodeNum = tsMgmtEqualVnodeNum;

  tsModule[TSDB_MOD_HTTP].name = "http";
  tsModule[TSDB_MOD_HTTP].initFp = httpInitSystem;
  tsModule[TSDB_MOD_HTTP].cleanUpFp = httpCleanUpSystem;
  tsModule[TSDB_MOD_HTTP].startFp = httpStartSystem;
  tsModule[TSDB_MOD_HTTP].stopFp = httpStopSystem;
  tsModule[TSDB_MOD_HTTP].num = (tsEnableHttpModule == 1) ? -1 : 0;
  tsModule[TSDB_MOD_HTTP].curNum = 0;
  tsModule[TSDB_MOD_HTTP].equalVnodeNum = 0;

  tsModule[TSDB_MOD_MONITOR].name = "monitor";
  tsModule[TSDB_MOD_MONITOR].initFp = monitorInitSystem;
  tsModule[TSDB_MOD_MONITOR].cleanUpFp = monitorCleanUpSystem;
  tsModule[TSDB_MOD_MONITOR].startFp = monitorStartSystem;
  tsModule[TSDB_MOD_MONITOR].stopFp = monitorStopSystem;
  tsModule[TSDB_MOD_MONITOR].num = (tsEnableMonitorModule == 1) ? -1 : 0;
  tsModule[TSDB_MOD_MONITOR].curNum = 0;
  tsModule[TSDB_MOD_MONITOR].equalVnodeNum = 0;
}

void dnodeCleanUpSystem() {
  if (tsDnodeStopping) return;
  tsDnodeStopping = true;

  if (tsStatusTimer != NULL) {
    taosTmrStopA(&tsStatusTimer);
    tsStatusTimer = NULL;
  }

  for (int mod = 1; mod < TSDB_MOD_MAX; ++mod) {
    if (tsModule[mod].num != 0 && tsModule[mod].stopFp) {
      (*tsModule[mod].stopFp)();
    }
    if (tsModule[mod].num != 0 && tsModule[mod].cleanUpFp) {
      (*tsModule[mod].cleanUpFp)();
    }
  }

  if (tsModule[TSDB_MOD_MGMT].num != 0 && tsModule[TSDB_MOD_MGMT].cleanUpFp) {
    (*tsModule[TSDB_MOD_MGMT].cleanUpFp)();
  }

  vnodeCleanUpVnodes();

  taosCloseLogger();
  taosCleanupTier();
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

  taosResolveCRC();

  tsRebootTime = taosGetTimestampSec();
  tscEmbedded = 1;

  // Read global configuration.
  tsReadGlobalLogConfig();

  if (stat(logDir, &dirstat) < 0) mkdir(logDir, 0755);

  sprintf(temp, "%s/taosdlog", logDir);
  if (taosInitLog(temp, tsNumOfLogLines, 1) < 0) printf("failed to init log file\n");

  if (!tsReadGlobalConfig()) {  // TODO : Change this function
    tsPrintGlobalConfig();
    dError("TDengine read global config failed");
    return -1;
  }

  if (taosCreateTierDirectory() != 0) {
    dError("TDengine init tier directory failed");
    return -1;
  }

  vnodeInitMgmtIp();

  tsPrintGlobalConfig();
  dPrint("Server IP address is:%s", tsPrivateIp);

  taosSetCoreDump();

  signal(SIGPIPE, SIG_IGN);

  dnodeInitModules();
  pthread_mutex_init(&dmutex, NULL);

  dPrint("starting to initialize TDengine ...");

  vnodeInitQHandle();
  if (dnodeInitSystemSpec() < 0) {
    return -1;
  }
  
  for (int mod = 0; mod < TSDB_MOD_MAX; ++mod) {
    if (tsModule[mod].num != 0 && tsModule[mod].initFp) {
      if ((*tsModule[mod].initFp)() != 0) {
        dError("TDengine initialization failed");
        return -1;
      }
    }
  }

  if (vnodeInitSystem() != 0) {
    dError("TDengine vnodes initialization failed");
    return -1;
  }

  monitorCountReqFp = dnodeCountRequest;

  dnodeStartModuleSpec();

  dPrint("TDengine is initialized successfully");

  return 0;
}

void dnodeProcessModuleStatus(uint32_t status) {
  if (tsDnodeStopping) return;

  int news = status;
  int olds = tsModuleStatus;

  for (int moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
    int newStatus = news & (1 << moduleType);
    int oldStatus = olds & (1 << moduleType);

    if (oldStatus > 0) {
      if (newStatus == 0) {
        if (tsModule[moduleType].stopFp) {
          dPrint("module:%s is stopped on this node", tsModule[moduleType].name);
          (*tsModule[moduleType].stopFp)();
        }
      }
    } else if (oldStatus == 0) {
      if (newStatus > 0) {
        if (tsModule[moduleType].startFp) {
          dPrint("module:%s is started on this node", tsModule[moduleType].name);
          (*tsModule[moduleType].startFp)();
        }
      }
    } else {
    }
  }
  tsModuleStatus = status;
}

void dnodeResetSystem() {
  dPrint("reset the system ...");
  for (int vnode = 0; vnode < TSDB_MAX_VNODES; ++vnode) vnodeRemoveVnode(vnode);
  mgmtStopSystem();
}

void dnodeCountRequest(SCountInfo *info) {
  httpGetReqCount(&info->httpReqNum);
  info->selectReqNum = atomic_exchange_32(&vnodeSelectReqNum, 0);
  info->insertReqNum = atomic_exchange_32(&vnodeInsertReqNum, 0);
}
