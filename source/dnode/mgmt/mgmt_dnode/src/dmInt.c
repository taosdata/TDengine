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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "dmInt.h"
#include "libs/function/tudf.h"

static int32_t dmStartMgmt(SDnodeMgmt *pMgmt) {
  if (dmStartStatusThread(pMgmt) != 0) {
    return -1;
  }
#if defined(TD_ENTERPRISE)
  if (dmStartNotifyThread(pMgmt) != 0) {
    return -1;
  }
#endif
  if (dmStartMonitorThread(pMgmt) != 0) {
    return -1;
  }
  if (dmStartAuditThread(pMgmt) != 0) {
    return -1;
  }
  if (dmStartCrashReportThread(pMgmt) != 0) {
    return -1;
  }
  return 0;
}

static void dmStopMgmt(SDnodeMgmt *pMgmt) {
  pMgmt->pData->stopped = true;
  dmStopMonitorThread(pMgmt);
  dmStopAuditThread(pMgmt);
  dmStopStatusThread(pMgmt);
#if defined(TD_ENTERPRISE)
  dmStopNotifyThread(pMgmt);
#endif
  dmStopCrashReportThread(pMgmt);
}

static int32_t dmOpenMgmt(SMgmtInputOpt *pInput, SMgmtOutputOpt *pOutput) {
  SDnodeMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SDnodeMgmt));
  if (pMgmt == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pMgmt->pData = pInput->pData;
  pMgmt->msgCb = pInput->msgCb;
  pMgmt->path = pInput->path;
  pMgmt->name = pInput->name;
  pMgmt->processCreateNodeFp = pInput->processCreateNodeFp;
  pMgmt->processAlterNodeTypeFp = pInput->processAlterNodeTypeFp;
  pMgmt->processDropNodeFp = pInput->processDropNodeFp;
  pMgmt->sendMonitorReportFp = pInput->sendMonitorReportFp;
  pMgmt->sendAuditRecordsFp = pInput->sendAuditRecordFp;
  pMgmt->sendMonitorReportFpBasic = pInput->sendMonitorReportFpBasic;
  pMgmt->getVnodeLoadsFp = pInput->getVnodeLoadsFp;
  pMgmt->getVnodeLoadsLiteFp = pInput->getVnodeLoadsLiteFp;
  pMgmt->getMnodeLoadsFp = pInput->getMnodeLoadsFp;
  pMgmt->getQnodeLoadsFp = pInput->getQnodeLoadsFp;

  // pMgmt->pData->ipWhiteVer = 0;
  if (dmStartWorker(pMgmt) != 0) {
    return -1;
  }

  if (udfStartUdfd(pMgmt->pData->dnodeId) != 0) {
    dError("failed to start udfd");
  }

  pOutput->pMgmt = pMgmt;
  return 0;
}

static void dmCloseMgmt(SDnodeMgmt *pMgmt) {
  dmStopWorker(pMgmt);
  taosMemoryFree(pMgmt);
}

static int32_t dmRequireMgmt(const SMgmtInputOpt *pInput, bool *required) {
  *required = true;
  return 0;
}

SMgmtFunc dmGetMgmtFunc() {
  SMgmtFunc mgmtFunc = {0};
  mgmtFunc.openFp = dmOpenMgmt;
  mgmtFunc.closeFp = (NodeCloseFp)dmCloseMgmt;
  mgmtFunc.startFp = (NodeStartFp)dmStartMgmt;
  mgmtFunc.stopFp = (NodeStopFp)dmStopMgmt;
  mgmtFunc.requiredFp = dmRequireMgmt;
  mgmtFunc.getHandlesFp = dmGetMsgHandles;

  return mgmtFunc;
}
