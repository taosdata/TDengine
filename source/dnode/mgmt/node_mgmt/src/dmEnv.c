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

static SDnode globalDnode = {0};

SDnode *dmInstance() { return &globalDnode; }

static int32_t dmCheckRepeatInit(SDnode *pDnode) {
  if (atomic_val_compare_exchange_8(&pDnode->once, DND_ENV_INIT, DND_ENV_READY) != DND_ENV_INIT) {
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

static bool dmCheckDiskSpace() {
  osUpdate();
  // sufficiency
  if (!osDataSpaceSufficient()) {
    dWarn("free data disk size: %f GB, not sufficient, expected %f GB at least",
          (double)tsDataSpace.size.avail / 1024.0 / 1024.0 / 1024.0,
          (double)tsDataSpace.reserved / 1024.0 / 1024.0 / 1024.0);
  }
  if (!osLogSpaceSufficient()) {
    dWarn("free log disk size: %f GB, not sufficient, expected %f GB at least",
          (double)tsLogSpace.size.avail / 1024.0 / 1024.0 / 1024.0,
          (double)tsLogSpace.reserved / 1024.0 / 1024.0 / 1024.0);
  }
  if (!osTempSpaceSufficient()) {
    dWarn("free temp disk size: %f GB, not sufficient, expected %f GB at least",
          (double)tsTempSpace.size.avail / 1024.0 / 1024.0 / 1024.0,
          (double)tsTempSpace.reserved / 1024.0 / 1024.0 / 1024.0);
  }
  // availability
  bool ret = true;
  if (!osDataSpaceAvailable()) {
    dError("data disk space unavailable, i.e. %s", tsDataDir);
    terrno = TSDB_CODE_NO_DISKSPACE;
    ret = false;
  }
  if (!osLogSpaceAvailable()) {
    dError("log disk space unavailable, i.e. %s", tsLogDir);
    terrno = TSDB_CODE_NO_DISKSPACE;
    ret = false;
  }
  if (!osTempSpaceAvailable()) {
    dError("temp disk space unavailable, i.e. %s", tsTempDir);
    terrno = TSDB_CODE_NO_DISKSPACE;
    ret = false;
  }
  return ret;
}

static bool dmCheckDataDirVersion() {
  char checkDataDirJsonFileName[PATH_MAX] = {0};
  snprintf(checkDataDirJsonFileName, PATH_MAX, "%s/dnode/dnodeCfg.json", tsDataDir);
  if (taosCheckExistFile(checkDataDirJsonFileName)) {
    dError("The default data directory %s contains old data of tdengine 2.x, please clear it before running!",
           tsDataDir);
    return false;
  }
  return true;
}

int32_t dmInit() {
  dInfo("start to init dnode env");
  if (!dmCheckDataDirVersion()) return -1;
  if (!dmCheckDiskSpace()) return -1;
  if (dmCheckRepeatInit(dmInstance()) != 0) return -1;
  if (dmInitSystem() != 0) return -1;
  if (dmInitMonitor() != 0) return -1;
  if (dmInitDnode(dmInstance()) != 0) return -1;

  dInfo("dnode env is initialized");
  return 0;
}

static int32_t dmCheckRepeatCleanup(SDnode *pDnode) {
  if (atomic_val_compare_exchange_8(&pDnode->once, DND_ENV_READY, DND_ENV_CLEANUP) != DND_ENV_READY) {
    dError("dnode env is already cleaned up");
    return -1;
  }
  return 0;
}

void dmCleanup() {
  dDebug("start to cleanup dnode env");
  SDnode *pDnode = dmInstance();
  if (dmCheckRepeatCleanup(pDnode) != 0) return;
  dmCleanupDnode(pDnode);
  monCleanup();
  syncCleanUp();
  walCleanUp();
  udfcClose();
  udfStopUdfd();
  taosStopCacheRefreshWorker();
  dInfo("dnode env is cleaned up");

  taosCleanupCfg();
  taosCloseLog();
}

void dmStop() {
  SDnode *pDnode = dmInstance();
  pDnode->stop = true;
}

int32_t dmRun() {
  SDnode *pDnode = dmInstance();
  return dmRunDnode(pDnode);
}

static int32_t dmProcessCreateNodeReq(EDndNodeType ntype, SRpcMsg *pMsg) {
  SDnode *pDnode = dmInstance();

  SMgmtWrapper *pWrapper = dmAcquireWrapper(pDnode, ntype);
  if (pWrapper != NULL) {
    dmReleaseWrapper(pWrapper);
    switch (ntype) {
      case MNODE:
        terrno = TSDB_CODE_MNODE_ALREADY_DEPLOYED;
        break;
      case QNODE:
        terrno = TSDB_CODE_QNODE_ALREADY_DEPLOYED;
        break;
      case SNODE:
        terrno = TSDB_CODE_SNODE_ALREADY_DEPLOYED;
        break;
      default:
        terrno = TSDB_CODE_APP_ERROR;
    }
    dError("failed to create node since %s", terrstr());
    return -1;
  }

  pWrapper = &pDnode->wrappers[ntype];
  if (taosMkDir(pWrapper->path) != 0) {
    dmReleaseWrapper(pWrapper);
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to create dir:%s since %s", pWrapper->path, terrstr());
    return -1;
  }

  taosThreadMutexLock(&pDnode->mutex);
  SMgmtInputOpt input = dmBuildMgmtInputOpt(pWrapper);

  dInfo("node:%s, start to create", pWrapper->name);
  int32_t code = (*pWrapper->func.createFp)(&input, pMsg);
  if (code != 0) {
    dError("node:%s, failed to create since %s", pWrapper->name, terrstr());
  } else {
    dInfo("node:%s, has been created", pWrapper->name);
    code = dmOpenNode(pWrapper);
    if (code == 0) {
      code = dmStartNode(pWrapper);
    }
    pWrapper->deployed = true;
    pWrapper->required = true;
  }

  taosThreadMutexUnlock(&pDnode->mutex);
  return code;
}

static int32_t dmProcessDropNodeReq(EDndNodeType ntype, SRpcMsg *pMsg) {
  SDnode *pDnode = dmInstance();

  SMgmtWrapper *pWrapper = dmAcquireWrapper(pDnode, ntype);
  if (pWrapper == NULL) {
    switch (ntype) {
      case MNODE:
        terrno = TSDB_CODE_MNODE_NOT_DEPLOYED;
        break;
      case QNODE:
        terrno = TSDB_CODE_QNODE_NOT_DEPLOYED;
        break;
      case SNODE:
        terrno = TSDB_CODE_SNODE_NOT_DEPLOYED;
        break;
      default:
        terrno = TSDB_CODE_APP_ERROR;
    }

    dError("failed to drop node since %s", terrstr());
    return -1;
  }

  taosThreadMutexLock(&pDnode->mutex);
  SMgmtInputOpt input = dmBuildMgmtInputOpt(pWrapper);

  dInfo("node:%s, start to drop", pWrapper->name);
  int32_t code = (*pWrapper->func.dropFp)(&input, pMsg);
  if (code != 0) {
    dError("node:%s, failed to drop since %s", pWrapper->name, terrstr());
  } else {
    dInfo("node:%s, has been dropped", pWrapper->name);
    pWrapper->required = false;
    pWrapper->deployed = false;
  }

  dmReleaseWrapper(pWrapper);

  if (code == 0) {
    dmStopNode(pWrapper);
    dmCloseNode(pWrapper);
    taosRemoveDir(pWrapper->path);
  }
  taosThreadMutexUnlock(&pDnode->mutex);
  return code;
}

SMgmtInputOpt dmBuildMgmtInputOpt(SMgmtWrapper *pWrapper) {
  SMgmtInputOpt opt = {
      .path = pWrapper->path,
      .name = pWrapper->name,
      .pData = &pWrapper->pDnode->data,
      .processCreateNodeFp = dmProcessCreateNodeReq,
      .processDropNodeFp = dmProcessDropNodeReq,
      .sendMonitorReportFp = dmSendMonitorReport,
      .getVnodeLoadsFp = dmGetVnodeLoads,
      .getMnodeLoadsFp = dmGetMnodeLoads,
      .getQnodeLoadsFp = dmGetQnodeLoads,
  };

  opt.msgCb = dmGetMsgcb(pWrapper->pDnode);
  return opt;
}

void dmReportStartup(const char *pName, const char *pDesc) {
  SStartupInfo *pStartup = &(dmInstance()->startup);
  tstrncpy(pStartup->name, pName, TSDB_STEP_NAME_LEN);
  tstrncpy(pStartup->desc, pDesc, TSDB_STEP_DESC_LEN);
  dDebug("step:%s, %s", pStartup->name, pStartup->desc);
}

int64_t dmGetClusterId() {
  return globalDnode.data.clusterId;
}

