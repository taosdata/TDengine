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

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pwd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <syslog.h>
#include <unistd.h>

#include "dnodeSystem.h"
#include "mgmt.h"
#include "tsdb.h"
#include "tsystem.h"
#include "vnode.h"

// global, not configurable
char         mgmtDirectory[128];
void *       mgmtTmr;
void *       mgmtQhandle;
void *       mgmtTranQhandle = NULL;
void *       mgmtStatisticTimer = NULL;
void *       mgmtStatusTimer = NULL;
int          mgmtShellConns = 0;
extern void *pShellConn;
extern void *rpcQhandle;

void mgmtCleanUpSystem() {
  mTrace("mgmt is running, clean it up");
  taosTmrStopA(&mgmtStatisticTimer);
  mgmtCleanUpShell();
  mgmtCleanUpMeters();
  mgmtCleanUpVgroups();
  mgmtCleanUpDbs();
  mgmtCleanUpUsers();
  taosTmrCleanUp(mgmtTmr);
  taosCleanUpScheduler(mgmtQhandle);
  taosCleanUpScheduler(mgmtTranQhandle);
}

void mgmtDoStatistic(void *handle, void *tmrId) {}

void mgmtProcessDnodeStatus(void *handle, void *tmrId) {
  SDnodeObj *pObj = &dnodeObj;
  pObj->openVnodes = tsOpenVnodes;
  pObj->status = TSDB_STATUS_READY;

  float memoryUsedMB = 0;
  taosGetSysMemory(&memoryUsedMB);
  pObj->memoryAvailable = tsTotalMemoryMB - memoryUsedMB;

  float diskUsedGB = 0;
  taosGetDisk(&diskUsedGB);
  pObj->diskAvailable = tsTotalDiskGB - diskUsedGB;

  for (int vnode = 0; vnode < pObj->numOfVnodes; ++vnode) {
    SVnodeLoad *pVload = &(pObj->vload[vnode]);
    SVnodeObj * pVnode = vnodeList + vnode;

    // wait vnode dropped
    if (pVload->dropStatus == TSDB_VN_STATUS_DROPPING) {
      if (vnodeList[vnode].cfg.maxSessions <= 0) {
        pVload->dropStatus = TSDB_VN_STATUS_READY;
        pVload->status = TSDB_VN_STATUS_READY;
        mPrint("vid:%d, drop finished", pObj->privateIp, vnode);
        taosTmrStart(mgmtMonitorDbDrop, 10000, NULL, mgmtTmr);
      }
    }

    if (vnodeList[vnode].cfg.maxSessions <= 0) {
      continue;
    }

    pVload->vnode = vnode;
    pVload->status = TSDB_VN_STATUS_READY;
    pVload->totalStorage = pVnode->vnodeStatistic.totalStorage;
    pVload->compStorage = pVnode->vnodeStatistic.compStorage;
    pVload->pointsWritten = pVnode->vnodeStatistic.pointsWritten;
    uint32_t vgId = pVnode->cfg.vgId;

    SVgObj *pVgroup = mgmtGetVgroup(vgId);
    if (pVgroup == NULL) {
      mError("vgroup:%d is not there, but associated with vnode %d", vgId, vnode);
      pVload->dropStatus = TSDB_VN_STATUS_DROPPING;
      continue;
    }

    SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
    if (pDb == NULL) {
      mError("vgroup:%d not belongs to any database, vnode:%d", vgId, vnode);
      continue;
    }

    if (pVload->vgId == 0 || pVload->dropStatus == TSDB_VN_STATUS_DROPPING) {
      mError("vid:%d, mgmt not exist, drop it", vnode);
      pVload->dropStatus = TSDB_VN_STATUS_DROPPING;
    }
  }

  taosTmrReset(mgmtProcessDnodeStatus, tsStatusInterval * 1000, NULL, mgmtTmr, &mgmtStatusTimer);
  if (mgmtStatusTimer == NULL) {
    mError("Failed to start status timer");
  }
}

int mgmtInitSystem() {
  mPrint("starting to initialize TDengine mgmt ...");

  struct stat dirstat;

  if (stat(mgmtDirectory, &dirstat) < 0) mkdir(mgmtDirectory, 0755);

  int numOfThreads = tsNumOfCores * tsNumOfThreadsPerCore / 2.0;
  if (numOfThreads < 1) numOfThreads = 1;
  mgmtQhandle = taosInitScheduler(tsMaxDnodes + tsMaxShellConns, numOfThreads, "mnode");

  mgmtTranQhandle = taosInitScheduler(tsMaxDnodes + tsMaxShellConns, 1, "mnodeT");

  mgmtTmr = taosTmrInit((tsMaxDnodes + tsMaxShellConns) * 3, 200, 3600000, "MND");
  if (mgmtTmr == NULL) {
    mError("failed to init timer, exit");
    return -1;
  }

  dnodeObj.lastReboot = tsRebootTime;
  dnodeObj.numOfCores = (uint16_t)tsNumOfCores;
  if (dnodeObj.numOfVnodes == TSDB_INVALID_VNODE_NUM) {
    mgmtSetDnodeMaxVnodes(&dnodeObj);
    mPrint("first access, set total vnodes:%d", dnodeObj.numOfVnodes);
  }

  if (mgmtInitUsers() < 0) {
    mError("failed to init users");
    return -1;
  }

  if (mgmtInitDbs() < 0) {
    mError("failed to init dbs");
    return -1;
  }

  if (mgmtInitVgroups() < 0) {
    mError("failed to init vgroups");
    return -1;
  }

  if (mgmtInitMeters() < 0) {
    mError("failed to init meters");
    return -1;
  }

  if (mgmtInitShell() < 0) {
    mError("failed to init shell");
    return -1;
  }

  mgmtCheckAcct();

  taosTmrReset(mgmtDoStatistic, tsStatusInterval * 30000, NULL, mgmtTmr, &mgmtStatisticTimer);

  taosTmrReset(mgmtProcessDnodeStatus, 500, NULL, mgmtTmr, &mgmtStatusTimer);

  mPrint("TDengine mgmt is initialized successfully");

  return 0;
}
