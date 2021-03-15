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
#include "tbn.h"
#include "tgrant.h"
#include "ttimer.h"
#include "tglobal.h"
#include "mnode.h"
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
#include "mnodeFunc.h"
#include "mnodeTable.h"
#include "mnodeCluster.h"
#include "mnodeShow.h"
#include "mnodeProfile.h"

void *tsMnodeTmr = NULL;
static bool tsMgmtIsRunning = false;

static SStep tsMnodeSteps[] = {
  {"sdbref",  sdbInitRef,       sdbCleanUpRef},
  {"profile", mnodeInitProfile, mnodeCleanupProfile},
  {"cluster", mnodeInitCluster, mnodeCleanupCluster},
  {"accts",   mnodeInitAccts,   mnodeCleanupAccts},
  {"users",   mnodeInitUsers,   mnodeCleanupUsers},
  {"funcs",   mnodeInitFuncs,   mnodeCleanupFuncs},
  {"dnodes",  mnodeInitDnodes,  mnodeCleanupDnodes},
  {"dbs",     mnodeInitDbs,     mnodeCleanupDbs},
  {"vgroups", mnodeInitVgroups, mnodeCleanupVgroups},
  {"tables",  mnodeInitTables,  mnodeCleanupTables},  
  {"mnodes",  mnodeInitMnodes,  mnodeCleanupMnodes},
  {"sdb",     sdbInit,          sdbCleanUp},
  {"balance", bnInit,           bnCleanUp},
  {"grant",   grantInit,        grantCleanUp},
  {"show",    mnodeInitShow,    mnodeCleanUpShow}
};

static void mnodeInitTimer();
static void mnodeCleanupTimer();
static bool mnodeNeedStart() ;

static void mnodeCleanupComponents() {
  int32_t stepSize = sizeof(tsMnodeSteps) / sizeof(SStep);
  dnodeStepCleanup(tsMnodeSteps, stepSize);
}

static int32_t mnodeInitComponents() {
  int32_t stepSize = sizeof(tsMnodeSteps) / sizeof(SStep);
  return dnodeStepInit(tsMnodeSteps, stepSize);
}

int32_t mnodeStartSystem() {
  if (tsMgmtIsRunning) {
    mInfo("mnode module already started...");
    return TSDB_CODE_SUCCESS;
  }

  mInfo("starting to initialize mnode ...");
  if (mkdir(tsMnodeDir, 0755) != 0 && errno != EEXIST) {
    mError("failed to init mnode dir:%s, reason:%s", tsMnodeDir, strerror(errno));
    return TSDB_CODE_MND_FAILED_TO_CREATE_DIR;
  }

  dnodeAllocMWritequeue();
  dnodeAllocMReadQueue();
  dnodeAllocateMPeerQueue();

  if (mnodeInitComponents() != 0) {
    return TSDB_CODE_MND_FAILED_TO_INIT_STEP;
  }

  dnodeReportStep("mnode-grant", "start to set grant infomation", 0);
  grantReset(TSDB_GRANT_ALL, 0);
  tsMgmtIsRunning = true;

  mInfo("mnode is initialized successfully");

  sdbUpdateSync(NULL);

  return TSDB_CODE_SUCCESS;
}

int32_t mnodeInitSystem() {
  mnodeInitTimer();
  if (mnodeNeedStart()) {
    return mnodeStartSystem();
  }
  return 0;
}

void mnodeCleanupSystem() {
  if (tsMgmtIsRunning) {
    mInfo("starting to clean up mnode");
    tsMgmtIsRunning = false;

    dnodeFreeMWritequeue();
    dnodeFreeMReadQueue();
    dnodeFreeMPeerQueue();
    mnodeCleanupTimer();
    mnodeCleanupComponents();

    mInfo("mnode is cleaned up");
  }
}

void mnodeStopSystem() {
  if (sdbIsMaster()) {
    mDebug("it is a master mnode, it could not be stopped");
    return;
  }
  
  mnodeCleanupSystem();

  if (remove(tsMnodeDir) != 0) {
    mInfo("failed to remove mnode file, reason:%s", strerror(errno));
  } else {
    mInfo("mnode file is removed");
  }
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
  char mnodeFileName[TSDB_FILENAME_LEN * 2] = {0};
  sprintf(mnodeFileName, "%s/wal/wal0", tsMnodeDir);

  bool fileExist = (stat(mnodeFileName, &dirstat) == 0);
  bool asMaster = (strcmp(tsFirst, tsLocalEp) == 0);

  if (asMaster || fileExist) {
    mDebug("mnode module start, asMaster:%d fileExist:%d", asMaster, fileExist);
    return true;
  } else {
    mDebug("mnode module won't start, asMaster:%d fileExist:%d", asMaster, fileExist);
    return false;
  }
}

bool mnodeIsRunning() {
  return tsMgmtIsRunning;
}
