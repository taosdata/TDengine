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
#include "tlog.h"
#include "tmodule.h"
#include "tglobalcfg.h"
#include "mnode.h"
#include "http.h"
#include "monitor.h"
#include "dnodeModule.h"
#include "dnodeSystem.h"

void dnodeAllocModules() {
  tsModule[TSDB_MOD_MGMT].name          = "mgmt";
  tsModule[TSDB_MOD_MGMT].initFp        = mgmtInitSystem;
  tsModule[TSDB_MOD_MGMT].cleanUpFp     = mgmtCleanUpSystem;
  tsModule[TSDB_MOD_MGMT].startFp       = mgmtStartSystem;
  tsModule[TSDB_MOD_MGMT].stopFp        = mgmtStopSystem;
  tsModule[TSDB_MOD_MGMT].num           = tsNumOfMPeers;
  tsModule[TSDB_MOD_MGMT].curNum        = 0;
  tsModule[TSDB_MOD_MGMT].equalVnodeNum = tsMgmtEqualVnodeNum;

  tsModule[TSDB_MOD_HTTP].name          = "http";
  tsModule[TSDB_MOD_HTTP].initFp        = httpInitSystem;
  tsModule[TSDB_MOD_HTTP].cleanUpFp     = httpCleanUpSystem;
  tsModule[TSDB_MOD_HTTP].startFp       = httpStartSystem;
  tsModule[TSDB_MOD_HTTP].stopFp        = httpStopSystem;
  tsModule[TSDB_MOD_HTTP].num           = (tsEnableHttpModule == 1) ? -1 : 0;
  tsModule[TSDB_MOD_HTTP].curNum        = 0;
  tsModule[TSDB_MOD_HTTP].equalVnodeNum = 0;

  tsModule[TSDB_MOD_MONITOR].name          = "monitor";
  tsModule[TSDB_MOD_MONITOR].initFp        = monitorInitSystem;
  tsModule[TSDB_MOD_MONITOR].cleanUpFp     = monitorCleanUpSystem;
  tsModule[TSDB_MOD_MONITOR].startFp       = monitorStartSystem;
  tsModule[TSDB_MOD_MONITOR].stopFp        = monitorStopSystem;
  tsModule[TSDB_MOD_MONITOR].num           = (tsEnableMonitorModule == 1) ? -1 : 0;
  tsModule[TSDB_MOD_MONITOR].curNum        = 0;
  tsModule[TSDB_MOD_MONITOR].equalVnodeNum = 0;
}

void dnodeCleanUpModules() {
  for (int mod = 1; mod < TSDB_MOD_MAX; ++mod) {
    if (tsModule[mod].num != 0 && tsModule[mod].stopFp) {
      (*tsModule[mod].stopFp)();
    }
    if (tsModule[mod].num != 0 && tsModule[mod].cleanUpFp) {
      (*tsModule[mod].cleanUpFp)();
    }
  }

  if (tsModule[TSDB_MOD_MGMT].num != 0 && tsModule[TSDB_MOD_MGMT].cleanUpFp) {
    (*tsModule[TSDB_MOD_MGMT].cleanUpFp)();
  }
}

void dnodeProcessModuleStatus(uint32_t status) {
  if (dnodeGetRunStatus() != TSDB_DNODE_RUN_STATUS_RUNING) {
    return;
  }

  int news = status;
  int olds = tsModuleStatus;

  for (int moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
    int newStatus = news & (1 << moduleType);
    int oldStatus = olds & (1 << moduleType);

    if (oldStatus > 0) {
      if (newStatus == 0) {
        if (tsModule[moduleType].stopFp) {
          dPrint("module:%s is stopped on this node", tsModule[moduleType].name);
          (*tsModule[moduleType].stopFp)();
        }
      }
    } else if (oldStatus == 0) {
      if (newStatus > 0) {
        if (tsModule[moduleType].startFp) {
          dPrint("module:%s is started on this node", tsModule[moduleType].name);
          (*tsModule[moduleType].startFp)();
        }
      }
    } else {
    }
  }
  tsModuleStatus = status;
}

int32_t dnodeInitModules() {
  for (int mod = 0; mod < TSDB_MOD_MAX; ++mod) {
    if (tsModule[mod].num != 0 && tsModule[mod].initFp) {
      if ((*tsModule[mod].initFp)() != 0) {
        dError("TDengine initialization failed");
        return -1;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

void dnodeStartModulesImp() {
  for (int mod = 1; mod < TSDB_MOD_MAX; ++mod) {
    if (tsModule[mod].num != 0 && tsModule[mod].startFp) {
      if ((*tsModule[mod].startFp)() != 0) {
        dError("failed to start module:%d", mod);
      }
    }
  }
}

void (*dnodeStartModules)() = dnodeStartModulesImp;
