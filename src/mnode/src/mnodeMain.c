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
#include "mnodeProfile.h"

typedef struct {
  const char *const name;
  int               (*init)();
  void              (*cleanup)();
} SMnodeComponent;

void *tsMnodeTmr;
static bool tsMgmtIsRunning = false;

static const SMnodeComponent tsMnodeComponents[] = {
  {"profile", mnodeInitProfile, mnodeCleanupProfile},
  {"accts",   mnodeInitAccts,   mnodeCleanupAccts},
  {"users",   mnodeInitUsers,   mnodeCleanupUsers},
  {"dnodes",  mnodeInitDnodes,  mnodeCleanupDnodes},
  {"dbs",     mnodeInitDbs,     mnodeCleanupDbs},
  {"vgroups", mnodeInitVgroups, mnodeCleanupVgroups},
  {"tables",  mnodeInitTables,  mnodeCleanupTables},  
  {"mnodes",  mnodeInitMnodes,  mnodeCleanupMnodes},
  {"sdb",     sdbInit,          sdbCleanUp},
  {"balance", balanceInit,      balanceCleanUp},
  {"grant",   grantInit,        grantCleanUp},
  {"show",    mnodeInitShow,    mnodeCleanUpShow}
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
    if (tsMnodeComponents[i].init() != 0) {
      mnodeCleanupComponents(i);
      code = -1;
      break;
    }
  }
  return code;
}

int32_t mnodeStartSystem() {
  if (tsMgmtIsRunning) {
    mPrint("mnode module already started...");
    return 0;
  }

  mPrint("starting to initialize mnode ...");
  if (mkdir(tsMnodeDir, 0755) != 0 && errno != EEXIST) {
    mError("failed to init mnode dir:%s, reason:%s", tsMnodeDir, strerror(errno));
    return -1;
  }

  dnodeAllocateMnodeWqueue();
  dnodeAllocateMnodeRqueue();
  dnodeAllocateMnodePqueue();

  if (mnodeInitComponents() != 0) {
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
  mnodeCleanupComponents(sizeof(tsMnodeComponents) / sizeof(tsMnodeComponents[0]) - 1);

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
