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

  if (rpcInit() != 0) {
    dError("failed to init rpc since %s", terrstr());
    dndCleanup();
    return -1;
  }

  SMonCfg monCfg = {0};
  monCfg.maxLogs = tsMonitorMaxLogs;
  monCfg.port = tsMonitorPort;
  monCfg.server = tsMonitorFqdn;
  monCfg.comp = tsMonitorComp;
  if (monInit(&monCfg) != 0) {
    dError("failed to init monitor since %s", terrstr());
    dndCleanup();
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
  rpcCleanup();
  walCleanUp();
  taosStopCacheRefreshWorker();
  dInfo("dnode env is cleaned up");
}

void dndSetMsgHandle(SMgmtWrapper *pWrapper, int32_t msgType, NodeMsgFp nodeMsgFp) {
  pWrapper->msgFps[TMSG_INDEX(msgType)] = nodeMsgFp;
}

EDndStatus dndGetStatus(SDnode *pDnode) { return pDnode->status; }

void dndSetStatus(SDnode *pDnode, EDndStatus status) {
  if (pDnode->status != status) {
    dDebug("dnode status set from %s to %s", dndStatStr(pDnode->status), dndStatStr(status));
    pDnode->status = status;
  }
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
      return "unknown";
  }
}

void dndReportStartup(SDnode *pDnode, char *pName, char *pDesc) {
  SStartupReq *pStartup = &pDnode->startup;
  tstrncpy(pStartup->name, pName, TSDB_STEP_NAME_LEN);
  tstrncpy(pStartup->desc, pDesc, TSDB_STEP_DESC_LEN);
  pStartup->finished = 0;
}

void dndGetStartup(SDnode *pDnode, SStartupReq *pStartup) {
  memcpy(pStartup, &pDnode->startup, sizeof(SStartupReq));
  pStartup->finished = (dndGetStatus(pDnode) == DND_STAT_RUNNING);
}

TdFilePtr dndCheckRunning(char *dataDir) {
  char filepath[PATH_MAX] = {0};
  snprintf(filepath, sizeof(filepath), "%s/.running", dataDir);

  TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_CTEATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to lock file:%s since %s, quit", filepath, terrstr());
    return NULL;
  }

  int32_t ret = taosLockFile(pFile);
  if (ret != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to lock file:%s since %s, quit", filepath, terrstr());
    taosCloseFile(&pFile);
    return NULL;
  }

  dDebug("file:%s is locked", filepath);
  return pFile;
}

void dndProcessStartupReq(SDnode *pDnode, SRpcMsg *pReq) {
  dDebug("startup req is received");

  SStartupReq *pStartup = rpcMallocCont(sizeof(SStartupReq));
  dndGetStartup(pDnode, pStartup);

  dDebug("startup req is sent, step:%s desc:%s finished:%d", pStartup->name, pStartup->desc, pStartup->finished);

  SRpcMsg rpcRsp = {.handle = pReq->handle, .pCont = pStartup, .contLen = sizeof(SStartupReq)};
  rpcSendResponse(&rpcRsp);
}
