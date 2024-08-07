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
#include "qmInt.h"
#include "libs/function/tudf.h"

static int32_t qmRequire(const SMgmtInputOpt *pInput, bool *required) {
  return dmReadFile(pInput->path, pInput->name, required);
}

static void qmInitOption(SQnodeMgmt *pMgmt, SQnodeOpt *pOption) { pOption->msgCb = pMgmt->msgCb; }

static void qmClose(SQnodeMgmt *pMgmt) {
  if (pMgmt->pQnode != NULL) {
    qmStopWorker(pMgmt);
    qndClose(pMgmt->pQnode);
    pMgmt->pQnode = NULL;
  }

  taosMemoryFree(pMgmt);
}

static int32_t qndOpenWrapper(SQnodeOpt *pOption, SQnode **pQnode) {
  int32_t code = qndOpen(pOption, pQnode);
  return code;
}
static int32_t qmOpen(SMgmtInputOpt *pInput, SMgmtOutputOpt *pOutput) {
  int32_t     code = 0;
  SQnodeMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SQnodeMgmt));
  if (pMgmt == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pMgmt->pData = pInput->pData;
  pMgmt->path = pInput->path;
  pMgmt->name = pInput->name;
  pMgmt->msgCb = pInput->msgCb;
  pMgmt->msgCb.putToQueueFp = (PutToQueueFp)qmPutRpcMsgToQueue;
  pMgmt->msgCb.qsizeFp = (GetQueueSizeFp)qmGetQueueSize;
  pMgmt->msgCb.mgmt = pMgmt;

  SQnodeOpt option = {0};
  qmInitOption(pMgmt, &option);

  code = qndOpenWrapper(&option, &pMgmt->pQnode);
  if (code != 0) {
    dError("failed to open qnode since %s", tstrerror(code));
    qmClose(pMgmt);
    return code;
  }
  tmsgReportStartup("qnode-impl", "initialized");

  if ((code = udfcOpen()) != 0) {
    dError("qnode can not open udfc");
    qmClose(pMgmt);
    return code;
  }

  if ((code = qmStartWorker(pMgmt)) != 0) {
    dError("failed to start qnode worker since %s", tstrerror(code));
    qmClose(pMgmt);
    return code;
  }
  tmsgReportStartup("qnode-worker", "initialized");

  pOutput->pMgmt = pMgmt;
  return code;
}

SMgmtFunc qmGetMgmtFunc() {
  SMgmtFunc mgmtFunc = {0};
  mgmtFunc.openFp = qmOpen;
  mgmtFunc.closeFp = (NodeCloseFp)qmClose;
  mgmtFunc.createFp = (NodeCreateFp)qmProcessCreateReq;
  mgmtFunc.dropFp = (NodeDropFp)qmProcessDropReq;
  mgmtFunc.requiredFp = qmRequire;
  mgmtFunc.getHandlesFp = qmGetMsgHandles;

  return mgmtFunc;
}
