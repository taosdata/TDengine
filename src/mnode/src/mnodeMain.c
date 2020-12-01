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
#include "taoserror.h"
#include "tbalance.h"
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
#include "mnodeTable.h"
#include "mnodeCluster.h"
#include "mnodeShow.h"
#include "mnodeProfile.h"

typedef enum {
  TSDB_MND_STATUS_NOT_RUNNING,
  TSDB_MND_STATUS_INIT,
  TSDB_MND_STATUS_INIT_SDB,
  TSDB_MND_STATUS_INIT_OTHER,
  TSDB_MND_STATUS_READY,
  TSDB_MND_STATUS_CLEANING,
} EMndStatus;

typedef struct {
  const char *const name;
  int               (*init)();
  void              (*cleanup)();
  EMndStatus        status; 
} SMnodeComponent;

void   *tsMnodeTmr = NULL;
static bool       tsMgmtIsRunning = false;
static EMndStatus tsMgmtStatus = TSDB_MND_STATUS_NOT_RUNNING;

static const SMnodeComponent tsMnodeComponents[] = {
  {"sdbref",  sdbInitRef,       sdbCleanUpRef,       TSDB_MND_STATUS_INIT},
  {"profile", mnodeInitProfile, mnodeCleanupProfile, TSDB_MND_STATUS_INIT},
  {"cluster", mnodeInitCluster, mnodeCleanupCluster, TSDB_MND_STATUS_INIT},
  {"accts",   mnodeInitAccts,   mnodeCleanupAccts,   TSDB_MND_STATUS_INIT},
  {"users",   mnodeInitUsers,   mnodeCleanupUsers,   TSDB_MND_STATUS_INIT},
  {"dnodes",  mnodeInitDnodes,  mnodeCleanupDnodes,  TSDB_MND_STATUS_INIT},
  {"dbs",     mnodeInitDbs,     mnodeCleanupDbs,     TSDB_MND_STATUS_INIT},
  {"vgroups", mnodeInitVgroups, mnodeCleanupVgroups, TSDB_MND_STATUS_INIT},
  {"tables",  mnodeInitTables,  mnodeCleanupTables,  TSDB_MND_STATUS_INIT},  
  {"mnodes",  mnodeInitMnodes,  mnodeCleanupMnodes,  TSDB_MND_STATUS_INIT},
  {"sdb",     sdbInit,          sdbCleanUp,          TSDB_MND_STATUS_INIT_SDB},
  {"balance", balanceInit,      balanceCleanUp,      TSDB_MND_STATUS_INIT_OTHER},
  {"grant",   grantInit,        grantCleanUp,        TSDB_MND_STATUS_INIT_OTHER},
  {"show",    mnodeInitShow,    mnodeCleanUpShow,    TSDB_MND_STATUS_INIT_OTHER},
};

static void mnodeInitTimer();
static void mnodeCleanupTimer();
static bool mnodeNeedStart() ;

static void mnodeCleanupComponents(int32_t stepId) {
  for (int32_t i = stepId; i >= 0; i--) {
    tsMnodeComponents[i].cleanup();
  }
}

static int32_t mnodeInitComponents() {
  int32_t code = 0;
  for (int32_t i = 0; i < sizeof(tsMnodeComponents) / sizeof(tsMnodeComponents[0]); i++) {
    tsMgmtStatus = tsMnodeComponents[i].status;
    if (tsMnodeComponents[i].init() != 0) {
      mnodeCleanupComponents(i);
      code = -1;
      break;
    }
    // sleep(3);
  }
  return code;
}

int32_t mnodeStartSystem() {
  if (tsMgmtStatus != TSDB_MND_STATUS_NOT_RUNNING) {
    mInfo("mnode module already started...");
    return 0;
  }

  tsMgmtStatus = TSDB_MND_STATUS_INIT;
  mInfo("starting to initialize mnode ...");
  if (mkdir(tsMnodeDir, 0755) != 0 && errno != EEXIST) {
    mError("failed to init mnode dir:%s, reason:%s", tsMnodeDir, strerror(errno));
    return -1;
  }

  dnodeAllocMWritequeue();
  dnodeAllocMReadQueue();
  dnodeAllocateMPeerQueue();

  if (mnodeInitComponents() != 0) {
    return -1;
  }

  grantReset(TSDB_GRANT_ALL, 0);
  tsMgmtStatus = TSDB_MND_STATUS_READY;

  mInfo("mnode is initialized successfully");

  sdbUpdateSync(NULL);

  return 0;
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
    tsMgmtStatus = TSDB_MND_STATUS_CLEANING;

    dnodeFreeMWritequeue();
    dnodeFreeMReadQueue();
    dnodeFreeMPeerQueue();
    mnodeCleanupTimer();
    mnodeCleanupComponents(sizeof(tsMnodeComponents) / sizeof(tsMnodeComponents[0]) - 1);

    tsMgmtStatus = TSDB_MND_STATUS_NOT_RUNNING;
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
  return (tsMgmtStatus != TSDB_MND_STATUS_NOT_RUNNING && tsMgmtStatus != TSDB_MND_STATUS_CLEANING);
}

bool mnodeIsReady() {
  return (tsMgmtStatus == TSDB_MND_STATUS_READY);
}

int32_t mnodeInitCode() {
  int32_t code = -1;

  switch (tsMgmtStatus) {
    case TSDB_MND_STATUS_INIT:
      code = TSDB_CODE_MND_INIT;
      break;
    case TSDB_MND_STATUS_INIT_SDB:
      code = TSDB_CODE_MND_INIT_SDB;
      break;
    case TSDB_MND_STATUS_INIT_OTHER:
      code = TSDB_CODE_MND_INIT_OTHER;
      break;
    default:
      code = TSDB_CODE_MND_INIT;
  }

  return code;
}
