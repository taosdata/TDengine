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
#include "dmMgmt.h"

static struct {
  int8_t        once;
  EDndProcType  ptype;
  EDndNodeType  rtype;
  EDndEvent     event;
  EDndRunStatus status;
  SStartupInfo  startup;
  SDnodeTrans   trans;
  SUdfdData     udfdData;
  TdThreadMutex mutex;
  TdFilePtr     lockfile;
  SDnodeData    data;
  SMgmtWrapper  wrappers[NODE_END];
} global;

static int32_t dmCheckRepeatInit() {
  if (atomic_val_compare_exchange_8(&global.once, DND_ENV_INIT, DND_ENV_READY) != DND_ENV_INIT) {
    dError("env is already initialized");
    terrno = TSDB_CODE_REPEAT_INIT;
    return -1;
  }
  return 0;
}

static int32_t dmInitSystem() {
  taosIgnSIGPIPE();
  taosBlockSIGPIPE();
  taosResolveCRC();
  return 0;
}

static int32_t dmInitMonitor() {
  SMonCfg monCfg = {0};
  monCfg.maxLogs = tsMonitorMaxLogs;
  monCfg.port = tsMonitorPort;
  monCfg.server = tsMonitorFqdn;
  monCfg.comp = tsMonitorComp;
  if (monInit(&monCfg) != 0) {
    dError("failed to init monitor since %s", terrstr());
    return -1;
  }
  return 0;
}

int32_t dmInit() {
  dInfo("start to init env");
  if (dmCheckRepeatInit() != 0) return -1;
  if (dmInitSystem() != 0) return -1;
  if (dmInitMonitor() != 0) return -1;
  // if (dmInit)

  dInfo("env is initialized");
  return 0;
}

static int32_t dmCheckRepeatCleanup() {
  if (atomic_val_compare_exchange_8(&global.once, DND_ENV_READY, DND_ENV_CLEANUP) != DND_ENV_READY) {
    dError("env is already cleaned up");
    return -1;
  }
  return 0;
}

void dmCleanup() {
  dDebug("start to cleanup env");
  if (dmCheckRepeatCleanup != 0) return;

  monCleanup();
  syncCleanUp();
  walCleanUp();
  udfcClose();
  udfStopUdfd();
  taosStopCacheRefreshWorker();
  dInfo("env is cleaned up");
}
