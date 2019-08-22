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

#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

#include "mgmt.h"
#include "vnode.h"

#include "dnodeSystem.h"
#include "httpSystem.h"
#include "monitorSystem.h"
#include "tcrc32c.h"
#include "tglobalcfg.h"
#include "vnode.h"

SModule         tsModule[TSDB_MOD_MAX];
uint32_t        tsModuleStatus;
pthread_mutex_t dmutex;
extern int      vnodeSelectReqNum;
extern int      vnodeInsertReqNum;
bool            tsDnodeStopping = false;

void dnodeCountRequest(SCountInfo *info);

void dnodeInitModules() {
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

  for (int mod = 0; mod < TSDB_MOD_MAX; ++mod) {
    if (tsModule[mod].num != 0 && tsModule[mod].stopFp) (*tsModule[mod].stopFp)();
    if (tsModule[mod].num != 0 && tsModule[mod].cleanUpFp) (*tsModule[mod].cleanUpFp)();
  }

  mgmtCleanUpSystem();
  vnodeCleanUpVnodes();

  taosCloseLogger();
}

void taosCreateTierDirectory() {
  char fileName[128];

  sprintf(fileName, "%s/tsdb", tsDirectory);
  mkdir(fileName, 0755);

  sprintf(fileName, "%s/data", tsDirectory);
  mkdir(fileName, 0755);
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

  strcpy(tsDirectory, dataDir);
  if (stat(dataDir, &dirstat) < 0) {
    mkdir(dataDir, 0755);
  }

  taosCreateTierDirectory();

  sprintf(mgmtDirectory, "%s/mgmt", tsDirectory);
  sprintf(tsDirectory, "%s/tsdb", dataDir);

  tsPrintGlobalConfig();
  dPrint("Server IP address is:%s", tsInternalIp);

  signal(SIGPIPE, SIG_IGN);

  dnodeInitModules();
  pthread_mutex_init(&dmutex, NULL);

  dPrint("starting to initialize TDengine engine ...");

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

  if (mgmtInitSystem() != 0) {
    dError("TDengine mgmt initialization failed");
    return -1;
  }

  monitorCountReqFp = dnodeCountRequest;

  for (int mod = 0; mod < TSDB_MOD_MAX; ++mod) {
    if (tsModule[mod].num != 0 && tsModule[mod].startFp) {
      if ((*tsModule[mod].startFp)() != 0) {
        dError("failed to start TDengine module:%d", mod);
        return -1;
      }
    }
  }

  dPrint("TDengine is initialized successfully");

  return 0;
}

void dnodeCountRequest(SCountInfo *info) {
  httpGetReqCount(&info->httpReqNum);
  info->selectReqNum = __sync_fetch_and_and(&vnodeSelectReqNum, 0);
  info->insertReqNum = __sync_fetch_and_and(&vnodeInsertReqNum, 0);
}
