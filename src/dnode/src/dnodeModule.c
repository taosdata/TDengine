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
#include "tglobal.h"
#include "trpc.h"
#include "mnode.h"
#include "http.h"
#include "monitor.h"
#include "dnodeInt.h"
#include "dnodeModule.h"

typedef struct {
  bool      enable;
  char *    name;
  int32_t (*initFp)();
  int32_t (*startFp)();
  void    (*cleanUpFp)();
  void    (*stopFp)();
} SModule;

static SModule  tsModule[TSDB_MOD_MAX] = {{0}};
static uint32_t tsModuleStatus = 0;

static void dnodeSetModuleStatus(int32_t module) {
  tsModuleStatus |= (1 << module);
}

static void dnodeUnSetModuleStatus(int32_t module) {
  tsModuleStatus &= ~(1 << module);
}

static void dnodeAllocModules() {
  tsModule[TSDB_MOD_MGMT].enable       = false;
  tsModule[TSDB_MOD_MGMT].name         = "mgmt";
  tsModule[TSDB_MOD_MGMT].initFp       = mgmtInitSystem;
  tsModule[TSDB_MOD_MGMT].cleanUpFp    = mgmtCleanUpSystem;
  tsModule[TSDB_MOD_MGMT].startFp      = mgmtStartSystem;
  tsModule[TSDB_MOD_MGMT].stopFp       = mgmtStopSystem;

  tsModule[TSDB_MOD_HTTP].enable       = (tsEnableHttpModule == 1);
  tsModule[TSDB_MOD_HTTP].name         = "http";
  tsModule[TSDB_MOD_HTTP].initFp       = httpInitSystem;
  tsModule[TSDB_MOD_HTTP].cleanUpFp    = httpCleanUpSystem;
  tsModule[TSDB_MOD_HTTP].startFp      = httpStartSystem;
  tsModule[TSDB_MOD_HTTP].stopFp       = httpStopSystem;
  if (tsEnableHttpModule) {
    dnodeSetModuleStatus(TSDB_MOD_HTTP);
  }

  tsModule[TSDB_MOD_MONITOR].enable    = (tsEnableMonitorModule == 1);
  tsModule[TSDB_MOD_MONITOR].name      = "monitor";
  tsModule[TSDB_MOD_MONITOR].initFp    = monitorInitSystem;
  tsModule[TSDB_MOD_MONITOR].cleanUpFp = monitorCleanUpSystem;
  tsModule[TSDB_MOD_MONITOR].startFp   = monitorStartSystem;
  tsModule[TSDB_MOD_MONITOR].stopFp    = monitorStopSystem;
  if (tsEnableMonitorModule) {
    dnodeSetModuleStatus(TSDB_MOD_MONITOR);
  }
}

void dnodeCleanUpModules() {
  for (int32_t module = 1; module < TSDB_MOD_MAX; ++module) {
    if (tsModule[module].enable && tsModule[module].stopFp) {
      (*tsModule[module].stopFp)();
    }
    if (tsModule[module].cleanUpFp) {
      (*tsModule[module].cleanUpFp)();
    }
  }

  if (tsModule[TSDB_MOD_MGMT].enable && tsModule[TSDB_MOD_MGMT].cleanUpFp) {
    (*tsModule[TSDB_MOD_MGMT].cleanUpFp)();
  }
}

int32_t dnodeInitModules() {
  dnodeAllocModules();

  for (EModuleType module = 0; module < TSDB_MOD_MAX; ++module) {
    if (tsModule[module].initFp) {
      if ((*tsModule[module].initFp)() != 0) {
        dError("failed to init module:%s", tsModule[module].name);
        return -1;
      }
    }
  }

  return 0;
}

void dnodeStartModules() {
  for (EModuleType module = 1; module < TSDB_MOD_MAX; ++module) {
    if (tsModule[module].enable && tsModule[module].startFp) {
      if ((*tsModule[module].startFp)() != 0) {
        dError("failed to start module:%s", tsModule[module].name);
      }
    }
  }
}

void dnodeProcessModuleStatus(uint32_t moduleStatus) {
  bool enableMgmtModule = moduleStatus & (1 << TSDB_MOD_MGMT);
  if (!tsModule[TSDB_MOD_MGMT].enable && enableMgmtModule) {
    dPrint("module status is received, start mgmt module", tsModuleStatus, moduleStatus);
    tsModule[TSDB_MOD_MGMT].enable = true;
    dnodeSetModuleStatus(TSDB_MOD_MGMT);
    (*tsModule[TSDB_MOD_MGMT].startFp)();
  }

  if (tsModule[TSDB_MOD_MGMT].enable && !enableMgmtModule) {
    dPrint("module status is received, stop mgmt module", tsModuleStatus, moduleStatus);
    tsModule[TSDB_MOD_MGMT].enable = false;
    dnodeUnSetModuleStatus(TSDB_MOD_MGMT);
    (*tsModule[TSDB_MOD_MGMT].stopFp)();
  }
}
