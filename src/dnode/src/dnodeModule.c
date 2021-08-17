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
#include "mnode.h"
#include "http.h"
#include "tmqtt.h"
#include "monitor.h"
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
  tsModule[TSDB_MOD_MNODE].enable       = false;
  tsModule[TSDB_MOD_MNODE].name         = "mnode";
  tsModule[TSDB_MOD_MNODE].initFp       = mnodeInitSystem;
  tsModule[TSDB_MOD_MNODE].cleanUpFp    = mnodeCleanupSystem;
  tsModule[TSDB_MOD_MNODE].startFp      = mnodeStartSystem;
  tsModule[TSDB_MOD_MNODE].stopFp       = mnodeStopSystem;

  tsModule[TSDB_MOD_HTTP].enable       = (tsEnableHttpModule == 1);
  tsModule[TSDB_MOD_HTTP].name         = "http";
  tsModule[TSDB_MOD_HTTP].initFp       = httpInitSystem;
  tsModule[TSDB_MOD_HTTP].cleanUpFp    = httpCleanUpSystem;
  tsModule[TSDB_MOD_HTTP].startFp      = httpStartSystem;
  tsModule[TSDB_MOD_HTTP].stopFp       = httpStopSystem;
  if (tsEnableHttpModule) {
    dnodeSetModuleStatus(TSDB_MOD_HTTP);
  }

#ifdef _MQTT
  tsModule[TSDB_MOD_MQTT].enable = (tsEnableMqttModule == 1);
  tsModule[TSDB_MOD_MQTT].name = "mqtt";
  tsModule[TSDB_MOD_MQTT].initFp = mqttInitSystem;
  tsModule[TSDB_MOD_MQTT].cleanUpFp = mqttCleanUpSystem;
  tsModule[TSDB_MOD_MQTT].startFp = mqttStartSystem;
  tsModule[TSDB_MOD_MQTT].stopFp = mqttStopSystem;
  if (tsEnableMqttModule) {
    dnodeSetModuleStatus(TSDB_MOD_MQTT);
  }
#endif  

  tsModule[TSDB_MOD_MONITOR].enable    = (tsEnableMonitorModule == 1);
  tsModule[TSDB_MOD_MONITOR].name      = "monitor";
  tsModule[TSDB_MOD_MONITOR].initFp    = monInitSystem;
  tsModule[TSDB_MOD_MONITOR].cleanUpFp = monCleanupSystem;
  tsModule[TSDB_MOD_MONITOR].startFp   = monStartSystem;
  tsModule[TSDB_MOD_MONITOR].stopFp    = monStopSystem;
  if (tsEnableMonitorModule) {
    dnodeSetModuleStatus(TSDB_MOD_MONITOR);
  }
}

void dnodeCleanupModules() {
  for (EModuleType module = 1; module < TSDB_MOD_MAX; ++module) {
    if (tsModule[module].enable && tsModule[module].stopFp) {
      (*tsModule[module].stopFp)();
    }
    if (tsModule[module].cleanUpFp) {
      (*tsModule[module].cleanUpFp)();
    }
  }

  if (tsModule[TSDB_MOD_MNODE].cleanUpFp) {
    (*tsModule[TSDB_MOD_MNODE].cleanUpFp)();
  }
}

static int32_t dnodeStartModules() {
  for (EModuleType module = 1; module < TSDB_MOD_MAX; ++module) {
    if (tsModule[module].enable && tsModule[module].startFp) {
      int32_t code = (*tsModule[module].startFp)();
      if (code != 0) {
        dError("failed to start module:%s, code:%d", tsModule[module].name, code);
        return code;
      }
    }
  }

  return 0;
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

  dInfo("dnode modules is initialized");
  return dnodeStartModules();
}

int32_t dnodeProcessModuleStatus(uint32_t moduleStatus) {
  int32_t code = 0;

  for (int32_t module = TSDB_MOD_MNODE; module < TSDB_MOD_HTTP; ++module) {
    bool enableModule = moduleStatus & (1 << module);
    if (!tsModule[module].enable && enableModule) {
      dInfo("module status:%u is set, start %s module", moduleStatus, tsModule[module].name);
      tsModule[module].enable = true;
      dnodeSetModuleStatus(module);
      code = (*tsModule[module].startFp)();
    }

    if (tsModule[module].enable && !enableModule) {
      dInfo("module status:%u is set, stop %s module", moduleStatus, tsModule[module].name);
      tsModule[module].enable = false;
      dnodeUnSetModuleStatus(module);
      (*tsModule[module].stopFp)();
    }
  }

  return code;
}

int32_t dnodeStartMnode(SMInfos *pMinfos) {
  if (tsModuleStatus & (1 << TSDB_MOD_MNODE)) {
    dDebug("mnode module is already started, module status:%d", tsModuleStatus);
    return 0;
  }

  uint32_t moduleStatus = tsModuleStatus | (1 << TSDB_MOD_MNODE);
  dInfo("start mnode module, module status:%d, new status:%d", tsModuleStatus, moduleStatus);

  int32_t code = dnodeProcessModuleStatus(moduleStatus);
  if (code == 0) {
    code = sdbUpdateSync(pMinfos);
  }

  if (code != 0) {
    dError("failed to start mnode module since %s", tstrerror(code));
    moduleStatus = tsModuleStatus & ~(1 << TSDB_MOD_MNODE);
    dnodeProcessModuleStatus(moduleStatus);
  }

  return code;
}
