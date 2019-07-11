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
#include <pwd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <syslog.h>
#include <unistd.h>

#include "tsdb.h"
#include "tsocket.h"
#include "vnode.h"

// internal global, not configurable
void *   vnodeTmrCtrl;
void *   rpcQhandle;
void *   dmQhandle;
void *   queryQhandle;
uint32_t tsRebootTime;

int vnodeInitSystem() {
  int numOfThreads;

  numOfThreads = tsRatioOfQueryThreads * tsNumOfCores * tsNumOfThreadsPerCore;
  if (numOfThreads < 1) numOfThreads = 1;
  queryQhandle = taosInitScheduler(tsNumOfVnodesPerCore * tsNumOfCores * tsSessionsPerVnode, numOfThreads, "query");

  // numOfThreads = (1.0 - tsRatioOfQueryThreads) * tsNumOfCores * tsNumOfThreadsPerCore / 2.0;
  // if (numOfThreads < 1) numOfThreads = 1;
  rpcQhandle = taosInitScheduler(tsNumOfVnodesPerCore * tsNumOfCores * tsSessionsPerVnode, 1, "dnode");

  vnodeTmrCtrl = taosTmrInit(tsSessionsPerVnode + 1000, 200, 60000, "DND-vnode");
  if (vnodeTmrCtrl == NULL) {
    dError("failed to init timer, exit");
    return -1;
  }

  if (vnodeInitStore() < 0) {
    dError("failed to init vnode storage");
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

  dPrint("vnode is initialized successfully");

  return 0;
}

void vnodeInitQHandle() {
  // int numOfThreads = (1.0 - tsRatioOfQueryThreads) * tsNumOfCores * tsNumOfThreadsPerCore / 2.0;
  // if (numOfThreads < 1) numOfThreads = 1;
  rpcQhandle = taosInitScheduler(tsNumOfVnodesPerCore * tsNumOfCores * tsSessionsPerVnode, 1, "dnode");

  dmQhandle = taosInitScheduler(tsSessionsPerVnode, 1, "mgmt");
}
