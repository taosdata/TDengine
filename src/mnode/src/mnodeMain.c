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
#include "tsched.h"
#include "tbalance.h"
#include "tgrant.h"
#include "ttimer.h"
#include "tglobal.h"
#include "dnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeServer.h"
#include "mnodeAcct.h"
#include "mnodeDnode.h"
#include "mnodeMnode.h"
#include "mnodeDb.h"
#include "mnodeSdb.h"
#include "mnodeVgroup.h"
#include "mnodeUser.h"
#include "mnodeTable.h"
#include "mnodeShell.h"

static void *tsMgmtTmr;
static bool tsMgmtIsRunning = false;

static void mnodeInitTimer();
static void mnodeCleanupTimer();
static bool mnodeNeedStart() ;

int32_t mgmtStartSystem() {
  if (tsMgmtIsRunning) {
    mPrint("TDengine mgmt module already started...");
    return 0;
  }

  mPrint("starting to initialize TDengine mgmt ...");
  struct stat dirstat;
  if (stat(tsMnodeDir, &dirstat) < 0) {
    mkdir(tsMnodeDir, 0755);
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

  if (mgmtInitTables() < 0) {
    mError("failed to init tables");
    return -1;
  }

  if (mgmtInitMnodes() < 0) {
    mError("failed to init mnodes");
    return -1;
  }

  if (sdbInit() < 0) {
    mError("failed to init sdb");
    return -1;
  }

  if (balanceInit() < 0) {
    mError("failed to init balance")
  }

  if (grantInit() < 0) {
    mError("failed to init grant");
    return -1;
  }

  if (mnodeInitMgmt() < 0) {
    return -1;
  }

  grantReset(TSDB_GRANT_ALL, 0);
  tsMgmtIsRunning = true;

  mPrint("TDengine mgmt is initialized successfully");

  return 0;
}

int32_t mgmtInitSystem() {
  mnodeInitTimer();
  mnodeInitRead();
  mnodeInitWrite();

  if (mnodeNeedStart()) {
    if (mgmtStartSystem() != 0) {
      return -1;
    }
  }

  return 0;
}

void mgmtCleanUpSystem() {
  mPrint("starting to clean up mgmt");
  tsMgmtIsRunning = false;

  mnodeCleanupTimer();
  mnodeCleanupRead();
  mnodeCleanupWrite();

  mgmtCleanupMgmt();
  grantCleanUp();
  balanceCleanUp();
  sdbCleanUp();
  mgmtCleanupMnodes();
  mgmtCleanUpTables();
  mgmtCleanUpVgroups();
  mgmtCleanUpDbs();
  mgmtCleanupDnodes();
  mgmtCleanUpUsers();
  mgmtCleanUpAccts();
  mPrint("mgmt is cleaned up");
}

void mgmtStopSystem() {
  if (sdbIsMaster()) {
    mTrace("it is a master mgmt node, it could not be stopped");
    return;
  }
  

  mgmtCleanUpSystem();

  mPrint("mgmt file is removed");
  remove(tsMnodeDir);
}



void*   mnodeGetWqueue(int32_t vgId) {

}



static void mnodeInitTimer() {
  if (tsMgmtTmr != NULL) {
    tsMgmtTmr = taosTmrInit((tsMaxShellConns)*3, 200, 3600000, "MND");
  }
}

static void mnodeCleanupTimer() {
  if (tsMgmtTmr != NULL) {
    taosTmrCleanUp(tsMgmtTmr);
    tsMgmtTmr = NULL;
  }
}

static bool mnodeNeedStart() {
  struct stat dirstat;
  bool fileExist = (stat(tsMnodeDir, &dirstat) == 0);
  bool asMaster = (strcmp(tsFirst, tsLocalEp) == 0);

  if (asMaster || fileExist) {
    return true;
  }

  return false;
}
