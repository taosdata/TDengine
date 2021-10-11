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
#include "ttime.h"
#include "ttimer.h"
#include "tglobal.h"
#include "dnodeCfg.h"
#include "dnodeEps.h"
#include "dnodeMnodeEps.h"
#include "dnodeStatus.h"
#include "dnodeMain.h"
#include "vnode.h"

static struct {
  void *   dnodeTimer;
  void *   statusTimer;
  uint32_t rebootTime;
} tsStatus;

static void dnodeSendStatusMsg(void *handle, void *tmrId) {
  if (tsStatus.statusTimer == NULL) {
    taosTmrReset(dnodeSendStatusMsg, tsStatusInterval * 1000, NULL, tsStatus.dnodeTimer, &tsStatus.statusTimer);
    dError("failed to start status timer");
    return;
  }

  int32_t     contLen = sizeof(SStatusMsg) + TSDB_MAX_VNODES * sizeof(SVnodeLoad);
  SStatusMsg *pStatus = rpcMallocCont(contLen);
  if (pStatus == NULL) {
    taosTmrReset(dnodeSendStatusMsg, tsStatusInterval * 1000, NULL, tsStatus.dnodeTimer, &tsStatus.statusTimer);
    dError("failed to malloc status message");
    return;
  }

  dnodeGetCfg(&pStatus->dnodeId, pStatus->clusterId);
  pStatus->dnodeId = htonl(dnodeGetDnodeId());
  pStatus->version = htonl(tsVersion);
  pStatus->lastReboot = htonl(tsStatus.rebootTime);
  pStatus->numOfCores = htons((uint16_t)tsNumOfCores);
  pStatus->diskAvailable = tsAvailDataDirGB;
  pStatus->alternativeRole = tsAlternativeRole;
  tstrncpy(pStatus->dnodeEp, tsLocalEp, TSDB_EP_LEN);

  // fill cluster cfg parameters
  pStatus->clusterCfg.numOfMnodes = htonl(tsNumOfMnodes);
  pStatus->clusterCfg.mnodeEqualVnodeNum = htonl(tsMnodeEqualVnodeNum);
  pStatus->clusterCfg.offlineThreshold = htonl(tsOfflineThreshold);
  pStatus->clusterCfg.statusInterval = htonl(tsStatusInterval);
  pStatus->clusterCfg.maxtablesPerVnode = htonl(tsMaxTablePerVnode);
  pStatus->clusterCfg.maxVgroupsPerDb = htonl(tsMaxVgroupsPerDb);
  tstrncpy(pStatus->clusterCfg.arbitrator, tsArbitrator, TSDB_EP_LEN);
  tstrncpy(pStatus->clusterCfg.timezone, tsTimezone, 64);
  pStatus->clusterCfg.checkTime = 0;
  char timestr[32] = "1970-01-01 00:00:00.00";
  (void)taosParseTime(timestr, &pStatus->clusterCfg.checkTime, (int32_t)strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);
  tstrncpy(pStatus->clusterCfg.locale, tsLocale, TSDB_LOCALE_LEN);
  tstrncpy(pStatus->clusterCfg.charset, tsCharset, TSDB_LOCALE_LEN);

  pStatus->clusterCfg.enableBalance = tsEnableBalance;
  pStatus->clusterCfg.flowCtrl = tsEnableFlowCtrl;
  pStatus->clusterCfg.slaveQuery = tsEnableSlaveQuery;
  pStatus->clusterCfg.adjustMaster = tsEnableAdjustMaster;

  vnodeGetStatus(pStatus);
  contLen = sizeof(SStatusMsg) + pStatus->openVnodes * sizeof(SVnodeLoad);
  pStatus->openVnodes = htons(pStatus->openVnodes);

  SRpcMsg rpcMsg = {.ahandle = NULL, .pCont = pStatus, .contLen = contLen, .msgType = TSDB_MSG_TYPE_DM_STATUS};

  dnodeSendMsgToMnode(&rpcMsg);
}

void dnodeProcessStatusRsp(SRpcMsg *pMsg) {
  if (pMsg->code != TSDB_CODE_SUCCESS) {
    dError("status rsp is received, error:%s", tstrerror(pMsg->code));
    if (pMsg->code == TSDB_CODE_MND_DNODE_NOT_EXIST) {
      char clusterId[TSDB_CLUSTER_ID_LEN];
      dnodeGetClusterId(clusterId);
      if (clusterId[0] != '\0') {
        dnodeSetDropped();
        dError("exit zombie dropped dnode");
        exit(EXIT_FAILURE);
      }
    }

    taosTmrReset(dnodeSendStatusMsg, tsStatusInterval * 1000, NULL, tsStatus.dnodeTimer, &tsStatus.statusTimer);
    return;
  }

  SStatusRsp *pStatusRsp = pMsg->pCont;
  SMInfos *   minfos = &pStatusRsp->mnodes;
  dnodeUpdateMnodeFromStatus(minfos);

  SDnodeCfg *pCfg = &pStatusRsp->dnodeCfg;
  pCfg->numOfVnodes = htonl(pCfg->numOfVnodes);
  pCfg->moduleStatus = htonl(pCfg->moduleStatus);
  pCfg->dnodeId = htonl(pCfg->dnodeId);
  dnodeUpdateCfg(pCfg);

  vnodeSetAccess(pStatusRsp->vgAccess, pCfg->numOfVnodes);

  SDnodeEps *pEps = (SDnodeEps *)((char *)pStatusRsp->vgAccess + pCfg->numOfVnodes * sizeof(SVgroupAccess));
  dnodeUpdateEps(pEps);

  taosTmrReset(dnodeSendStatusMsg, tsStatusInterval * 1000, NULL, tsStatus.dnodeTimer, &tsStatus.statusTimer);
}

int32_t dnodeInitStatus() {
  tsStatus.statusTimer = NULL;
  tsStatus.dnodeTimer = dnodeGetTimer();
  tsStatus.rebootTime = taosGetTimestampSec();
  taosTmrReset(dnodeSendStatusMsg, 500, NULL, tsStatus.dnodeTimer, &tsStatus.statusTimer);
  dInfo("dnode status timer is initialized");
  return TSDB_CODE_SUCCESS;
}

void dnodeCleanupStatus() {
  if (tsStatus.statusTimer != NULL) {
    taosTmrStopA(&tsStatus.statusTimer);
    tsStatus.statusTimer = NULL;
  }
}
