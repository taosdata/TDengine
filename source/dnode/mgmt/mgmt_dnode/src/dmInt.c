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
#include "tanal.h"

static int32_t dmStartMgmt(SDnodeMgmt *pMgmt) {
  int32_t code = 0;
  if ((code = dmStartStatusThread(pMgmt)) != 0) {
    return code;
  }
  if ((code = dmStartStatusInfoThread(pMgmt)) != 0) {
    return code;
  }
#if defined(TD_ENTERPRISE)
  if ((code = dmStartNotifyThread(pMgmt)) != 0) {
    return code;
  }
#endif
  if ((code = dmStartMonitorThread(pMgmt)) != 0) {
    return code;
  }
  if ((code = dmStartAuditThread(pMgmt)) != 0) {
    return code;
  }
  if ((code = dmStartCrashReportThread(pMgmt)) != 0) {
    return code;
  }
  return 0;
}

static void dmStopMgmt(SDnodeMgmt *pMgmt) {
  pMgmt->pData->stopped = true;
  dmStopMonitorThread(pMgmt);
  dmStopAuditThread(pMgmt);
  dmStopStatusThread(pMgmt);
  dmStopStatusInfoThread(pMgmt);
#if defined(TD_ENTERPRISE)
  dmStopNotifyThread(pMgmt);
#endif
  dmStopCrashReportThread(pMgmt);
}

static int32_t dmOpenMgmt(SMgmtInputOpt *pInput, SMgmtOutputOpt *pOutput) {
  int32_t     code = 0;
  SDnodeMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SDnodeMgmt));
  if (pMgmt == NULL) {
    return terrno;
  }

  pMgmt->pData = pInput->pData;
  pMgmt->msgCb = pInput->msgCb;
  pMgmt->path = pInput->path;
  pMgmt->name = pInput->name;
  pMgmt->processCreateNodeFp = pInput->processCreateNodeFp;
  pMgmt->processAlterNodeTypeFp = pInput->processAlterNodeTypeFp;
  pMgmt->processDropNodeFp = pInput->processDropNodeFp;
  pMgmt->sendMonitorReportFp = pInput->sendMonitorReportFp;
  pMgmt->monitorCleanExpiredSamplesFp = pInput->monitorCleanExpiredSamplesFp;
  pMgmt->sendAuditRecordsFp = pInput->sendAuditRecordFp;
  pMgmt->getVnodeLoadsFp = pInput->getVnodeLoadsFp;
  pMgmt->getVnodeLoadsLiteFp = pInput->getVnodeLoadsLiteFp;
  pMgmt->getMnodeLoadsFp = pInput->getMnodeLoadsFp;
  pMgmt->getQnodeLoadsFp = pInput->getQnodeLoadsFp;

  if ((code = dmStartWorker(pMgmt)) != 0) {
    return code;
  }

  if ((code = udfStartUdfd(pMgmt->pData->dnodeId)) != 0) {
    dError("failed to start udfd since %s", tstrerror(code));
  }

  if ((code = taosAnalInit()) != 0) {
    dError("failed to init analysis env since %s", tstrerror(code));
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
