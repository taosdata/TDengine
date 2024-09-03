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
#include "audit.h"

#define STR_CASE_CMP(s, d)   (0 == strcasecmp((s), (d)))
#define STR_STR_CMP(s, d)    (strstr((s), (d)))
#define STR_INT_CMP(s, d, c) (taosStr2Int32(s, 0, 10) c(d))
#define STR_STR_SIGN         ("ia")
#define DM_INIT_MON()                   \
  do {                                  \
    code = (int32_t)(2147483648 | 298); \
    strncpy(stName, tsVersionName, 64); \
    monCfg.maxLogs = tsMonitorMaxLogs;  \
    monCfg.port = tsMonitorPort;        \
    monCfg.server = tsMonitorFqdn;      \
    monCfg.comp = tsMonitorComp;        \
    if (monInit(&monCfg) != 0) {        \
      if (terrno != 0) code = terrno;   \
      goto _exit;                       \
    }                                   \
  } while (0)

#define DM_INIT_AUDIT()                 \
  do {                                  \
    auditCfg.port = tsMonitorPort;        \
    auditCfg.server = tsMonitorFqdn;      \
    auditCfg.comp = tsMonitorComp;      \
    if (auditInit(&auditCfg) != 0) {    \
      return -1;                        \
    }                                   \
  } while (0)

#define DM_ERR_RTN(c) \
  do {                \
    code = (c);       \
    goto _exit;       \
  } while (0)

static SDnode      globalDnode = {0};
static const char *dmOS[10] = {"Ubuntu",  "CentOS Linux", "Red Hat", "Debian GNU", "CoreOS",
                               "FreeBSD", "openSUSE",     "SLES",    "Fedora",     "macOS"};

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
  int32_t code = 0;
  SMonCfg monCfg = {0};
  char    reName[64] = {0};
  char    stName[64] = {0};
  char    ver[64] = {0};

  DM_INIT_MON();

  if (STR_STR_CMP(stName, STR_STR_SIGN)) {
    DM_ERR_RTN(0);
  }
  if (taosGetOsReleaseName(reName, stName, ver, 64) != 0) {
    DM_ERR_RTN(code);
  }
  if (STR_CASE_CMP(stName, dmOS[0])) {
    if (STR_INT_CMP(ver, 17, >)) {
      DM_ERR_RTN(0);
    }
  } else if (STR_CASE_CMP(stName, dmOS[1])) {
    if (STR_INT_CMP(ver, 6, >)) {
      DM_ERR_RTN(0);
    }
  } else if (STR_STR_CMP(stName, dmOS[2]) || STR_STR_CMP(stName, dmOS[3]) || STR_STR_CMP(stName, dmOS[4]) ||
             STR_STR_CMP(stName, dmOS[5]) || STR_STR_CMP(stName, dmOS[6]) || STR_STR_CMP(stName, dmOS[7]) ||
             STR_STR_CMP(stName, dmOS[8]) || STR_STR_CMP(stName, dmOS[9])) {
    DM_ERR_RTN(0);
  }

_exit:
  if (code) terrno = code;
  return code;
}

static int32_t dmInitAudit() {
  SAuditCfg auditCfg = {0};

  DM_INIT_AUDIT();

  return 0;
}

static bool dmDataSpaceAvailable() {
  SDnode *pDnode = dmInstance();
  if (pDnode->pTfs) {
    return tfsDiskSpaceAvailable(pDnode->pTfs, 0);
  }
  if (!osDataSpaceAvailable()) {
    dError("data disk space unavailable, i.e. %s", tsDataDir);
    return false;
  }
  return true;
}

static bool dmCheckDiskSpace() {
  osUpdate();
  // availability
  bool ret = true;
  if (!dmDataSpaceAvailable()) {
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

int32_t dmDiskInit() {
  SDnode  *pDnode = dmInstance();
  SDiskCfg dCfg = {0};
  tstrncpy(dCfg.dir, tsDataDir, TSDB_FILENAME_LEN);
  dCfg.level = 0;
  dCfg.primary = 1;
  SDiskCfg *pDisks = tsDiskCfg;
  int32_t   numOfDisks = tsDiskCfgNum;
  if (numOfDisks <= 0 || pDisks == NULL) {
    pDisks = &dCfg;
    numOfDisks = 1;
  }

  pDnode->pTfs = tfsOpen(pDisks, numOfDisks);
  if (pDnode->pTfs == NULL) {
    dError("failed to init tfs since %s", terrstr());
    return -1;
  }
  return 0;
}

int32_t dmDiskClose() {
  SDnode *pDnode = dmInstance();
  tfsClose(pDnode->pTfs);
  pDnode->pTfs = NULL;
  return 0;
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
  if (dmDiskInit() != 0) return -1;
  if (!dmCheckDataDirVersion()) return -1;
  if (!dmCheckDiskSpace()) return -1;
  if (dmCheckRepeatInit(dmInstance()) != 0) return -1;
  if (dmInitSystem() != 0) return -1;
  if (dmInitMonitor() != 0) return -1;
  if (dmInitAudit() != 0) return -1;
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
  auditCleanup();
  syncCleanUp();
  walCleanUp();
  udfcClose();
  udfStopUdfd();
  taosStopCacheRefreshWorker();
  dmDiskClose();
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

  dInfo("start to process create-node-request");

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

static int32_t dmProcessAlterNodeTypeReq(EDndNodeType ntype, SRpcMsg *pMsg) {
  SDnode *pDnode = dmInstance();

  SMgmtWrapper *pWrapper = dmAcquireWrapper(pDnode, ntype);
  if (pWrapper == NULL) {
    dError("fail to process alter node type since node not exist");
    return -1;
  }
  dmReleaseWrapper(pWrapper);

  dInfo("node:%s, start to process alter-node-type-request", pWrapper->name);

  pWrapper = &pDnode->wrappers[ntype];

  if(pWrapper->func.nodeRoleFp != NULL){
    ESyncRole role = (*pWrapper->func.nodeRoleFp)(pWrapper->pMgmt);
    dInfo("node:%s, checking node role:%d", pWrapper->name, role);
    if(role == TAOS_SYNC_ROLE_VOTER){
      dError("node:%s, failed to alter node type since node already is role:%d", pWrapper->name, role);
      terrno = TSDB_CODE_MNODE_ALREADY_IS_VOTER;
      return -1;
    }
  }

  if(pWrapper->func.isCatchUpFp != NULL){
    dInfo("node:%s, checking node catch up", pWrapper->name);
    if((*pWrapper->func.isCatchUpFp)(pWrapper->pMgmt) != 1){
      terrno = TSDB_CODE_MNODE_NOT_CATCH_UP;
      return -1;
    }
  }

  dInfo("node:%s, catched up leader, continue to process alter-node-type-request", pWrapper->name);

  taosThreadMutexLock(&pDnode->mutex);

  dInfo("node:%s, stopping node", pWrapper->name);
  dmStopNode(pWrapper);
  dInfo("node:%s, closing node", pWrapper->name);
  dmCloseNode(pWrapper);

  pWrapper = &pDnode->wrappers[ntype];
  if (taosMkDir(pWrapper->path) != 0) {
    taosThreadMutexUnlock(&pDnode->mutex);
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to create dir:%s since %s", pWrapper->path, terrstr());
    return -1;
  }

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
      .pTfs = pWrapper->pDnode->pTfs,
      .pData = &pWrapper->pDnode->data,
      .processCreateNodeFp = dmProcessCreateNodeReq,
      .processAlterNodeTypeFp = dmProcessAlterNodeTypeReq,
      .processDropNodeFp = dmProcessDropNodeReq,
      .sendMonitorReportFp = dmSendMonitorReport,
      .monitorCleanExpiredSamplesFp = dmMonitorCleanExpiredSamples,
      .sendAuditRecordFp = auditSendRecordsInBatch,
      .getVnodeLoadsFp = dmGetVnodeLoads,
      .getVnodeLoadsLiteFp = dmGetVnodeLoadsLite,
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

