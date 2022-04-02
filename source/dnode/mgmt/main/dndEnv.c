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
#include "dndInt.h"
#include "wal.h"

static int8_t once = DND_ENV_INIT;

int32_t dndInit() {
  dDebug("start to init dnode env");
  if (atomic_val_compare_exchange_8(&once, DND_ENV_INIT, DND_ENV_READY) != DND_ENV_INIT) {
    terrno = TSDB_CODE_REPEAT_INIT;
    dError("failed to init dnode env since %s", terrstr());
    return -1;
  }

  taosIgnSIGPIPE();
  taosBlockSIGPIPE();
  taosResolveCRC();

  SMonCfg monCfg = {0};
  monCfg.maxLogs = tsMonitorMaxLogs;
  monCfg.port = tsMonitorPort;
  monCfg.server = tsMonitorFqdn;
  monCfg.comp = tsMonitorComp;
  if (monInit(&monCfg) != 0) {
    dError("failed to init monitor since %s", terrstr());
    return -1;
  }

  dInfo("dnode env is initialized");
  return 0;
}

void dndCleanup() {
  dDebug("start to cleanup dnode env");
  if (atomic_val_compare_exchange_8(&once, DND_ENV_READY, DND_ENV_CLEANUP) != DND_ENV_READY) {
    dError("dnode env is already cleaned up");
    return;
  }

  monCleanup();
  walCleanUp();
  taosStopCacheRefreshWorker();
  dInfo("dnode env is cleaned up");
}

const char *dndStatStr(EDndStatus status) {
  switch (status) {
    case DND_STAT_INIT:
      return "init";
    case DND_STAT_RUNNING:
      return "running";
    case DND_STAT_STOPPED:
      return "stopped";
    default:
      return "UNKNOWN";
  }
}

const char *dndNodeLogStr(ENodeType ntype) {
  switch (ntype) {
    case VNODES:
      return "vnode";
    case QNODE:
      return "qnode";
    case SNODE:
      return "snode";
    case MNODE:
      return "mnode";
    case BNODE:
      return "bnode";
    default:
      return "taosd";
  }
}

const char *dndNodeProcStr(ENodeType ntype) {
  switch (ntype) {
    case VNODES:
      return "taosv";
    case QNODE:
      return "taosq";
    case SNODE:
      return "taoss";
    case MNODE:
      return "taosm";
    case BNODE:
      return "taosb";
    default:
      return "taosd";
  }
}

const char *dndEventStr(EDndEvent ev) {
  switch (ev) {
    case DND_EVENT_START:
      return "start";
    case DND_EVENT_STOP:
      return "stop";
    case DND_EVENT_CHILD:
      return "child";
    default:
      return "UNKNOWN";
  }
}