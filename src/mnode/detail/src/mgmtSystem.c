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

#include "dnodeSystem.h"
#include "mgmt.h"
#include "tsdb.h"
#include "mgmtSystem.h"

// global, not configurable
char               mgmtDirectory[128];
void *             mgmtTmr;
void *             mgmtQhandle = NULL;
void *             mgmtTranQhandle = NULL;
void *             mgmtStatisticTimer = NULL;
int                mgmtShellConns = 0;
int                mgmtDnodeConns = 0;
extern void *      pShellConn;
extern void **     rpcQhandle;
extern SMgmtIpList mgmtIpList;
extern SMgmtIpList mgmtPublicIpList;
extern char        mgmtIpStr[TSDB_MAX_MGMT_IPS][20];
extern void *      acctSdb;

void mgmtCleanUpSystem() {
  if (tsModuleStatus & (1 << TSDB_MOD_MGMT)) {
    mTrace("mgmt is running, clean it up");
    taosTmrStopA(&mgmtStatisticTimer);
    sdbCleanUpPeers();
    mgmtCleanupBalance();
    mgmtCleanUpDnodeInt();
    mgmtCleanUpShell();
    mgmtCleanUpMeters();
    mgmtCleanUpVgroups();
    mgmtCleanUpDbs();
    mgmtCleanUpDnodes();
    mgmtCleanUpUsers();
    mgmtCleanUpAccts();
    taosTmrCleanUp(mgmtTmr);
    taosCleanUpScheduler(mgmtQhandle);
    taosCleanUpScheduler(mgmtTranQhandle);
  } else {
    mgmtCleanUpRedirect();
  }

  mgmtTmr = NULL;
  mgmtQhandle = NULL;
  mgmtShellConns = 0;
  mgmtDnodeConns = 0;
  tclearModuleStatus(TSDB_MOD_MGMT);
  pShellConn = NULL;

  mTrace("mgmt is cleaned up");
}

int mgmtStartSystem() {
  mPrint("starting to initialize TDengine mgmt ...");

  struct stat dirstat;
  if (stat(mgmtDirectory, &dirstat) < 0) {
    mkdir(mgmtDirectory, 0755);
  }

  if (mgmtStartCheckMgmtRunning() != 0) {
    mPrint("TDengine mgmt module already started...");
    return 0;
  }

  int numOfThreads = tsNumOfCores * tsNumOfThreadsPerCore / 2.0;
  if (numOfThreads < 1) numOfThreads = 1;
  mgmtQhandle = taosInitScheduler(tsMaxDnodes + tsMaxShellConns, numOfThreads, "mnode");

  mgmtTranQhandle = taosInitScheduler(tsMaxDnodes + tsMaxShellConns, 1, "mnodeT");

  mgmtTmr = taosTmrInit((tsMaxDnodes + tsMaxShellConns) * 3, 200, 3600000, "MND");
  if (mgmtTmr == NULL) {
    mError("failed to init timer, exit");
    return -1;
  }

  if (mgmtInitAccts() < 0) {
    mError("failed to init accts");
    return -1;
  }

  if (mgmtInitUsers() < 0) {
    mError("failed to init users");
    return -1;
  }

  if (mgmtInitDnodes() < 0) {
    mError("failed to init dnodes");
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

  if (mgmtInitDnodeInt() < 0) {
    mError("failed to init inter-mgmt communication");
    return -1;
  }

  if (mgmtInitShell() < 0) {
    mError("failed to init shell");
    return -1;
  }

  if (sdbInitPeers(mgmtDirectory) < 0) {
    mError("failed to init peers");
    return -1;
  }

  if (mgmtInitBalance() < 0) {
    mError("failed to init dnode balance")
  }

  mgmtCheckAcct();

  taosTmrReset(mgmtDoStatistic, tsStatusInterval * 30000, NULL, mgmtTmr, &mgmtStatisticTimer);

  mgmtStartMgmtTimer();

  mPrint("TDengine mgmt is initialized successfully");

  return 0;
}
