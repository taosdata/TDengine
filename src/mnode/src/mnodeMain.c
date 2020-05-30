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
#include "mnodeAcct.h"
#include "mnodeDnode.h"
#include "mnodeMnode.h"
#include "mnodeDb.h"
#include "mnodeSdb.h"
#include "mnodeVgroup.h"
#include "mnodeUser.h"
#include "mnodeTable.h"
#include "mnodeShow.h"

void *tsMnodeTmr;
static bool tsMgmtIsRunning = false;

static void mnodeInitTimer();
static void mnodeCleanupTimer();
static bool mnodeNeedStart() ;

int32_t mnodeStartSystem() {
  if (tsMgmtIsRunning) {
    mPrint("mnode module already started...");
    return 0;
  }

  mPrint("starting to initialize mnode ...");
  struct stat dirstat;
  if (stat(tsMnodeDir, &dirstat) < 0) {
    mkdir(tsMnodeDir, 0755);
  }

  dnodeAllocateMnodeWqueue();
  dnodeAllocateMnodeRqueue();
  dnodeAllocateMnodePqueue();

  if (mnodeInitAccts() < 0) {
    mError("failed to init accts");
    return -1;
  }

  if (mnodeInitUsers() < 0) {
    mError("failed to init users");
    return -1;
  }

  if (mnodeInitDnodes() < 0) {
    mError("failed to init dnodes");
    return -1;
  }

  if (mnodeInitDbs() < 0) {
    mError("failed to init dbs");
    return -1;
  }

  if (mnodeInitVgroups() < 0) {
    mError("failed to init vgroups");
    return -1;
  }

  if (mnodeInitTables() < 0) {
    mError("failed to init tables");
    return -1;
  }

  if (mnodeInitMnodes() < 0) {
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

  if (mnodeInitShow() < 0) {
    mError("failed to init show");
    return -1;
  }

  grantReset(TSDB_GRANT_ALL, 0);
  tsMgmtIsRunning = true;

  mPrint("mnode is initialized successfully");

  return 0;
}

int32_t mnodeInitSystem() {
  mnodeInitTimer();
  if (!mnodeNeedStart()) return 0;
  return mnodeStartSystem();
}

void mnodeCleanupSystem() {
  mPrint("starting to clean up mnode");
  tsMgmtIsRunning = false;

  dnodeFreeMnodeWqueue();
  dnodeFreeMnodeRqueue();
  dnodeFreeMnodePqueue();
  mnodeCleanupTimer();
  mnodeCleanUpShow();
  grantCleanUp();
  balanceCleanUp();
  sdbCleanUp();
  mnodeCleanupMnodes();
  mnodeCleanupTables();
  mnodeCleanupVgroups();
  mnodeCleanupDbs();
  mnodeCleanupDnodes();
  mnodeCleanupUsers();
  mnodeCleanupAccts();
  mPrint("mnode is cleaned up");
}

void mnodeStopSystem() {
  if (sdbIsMaster()) {
    mTrace("it is a master mnode, it could not be stopped");
    return;
  }
  
  mnodeCleanupSystem();
  mPrint("mnode file is removed");
  remove(tsMnodeDir);
}

static void mnodeInitTimer() {
  if (tsMnodeTmr == NULL) {
    tsMnodeTmr = taosTmrInit(tsMaxShellConns, 200, 3600000, "MND");
  }
}

static void mnodeCleanupTimer() {
  if (tsMnodeTmr != NULL) {
    taosTmrCleanUp(tsMnodeTmr);
    tsMnodeTmr = NULL;
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

bool mnodeIsRunning() {
  return tsMgmtIsRunning;
}
