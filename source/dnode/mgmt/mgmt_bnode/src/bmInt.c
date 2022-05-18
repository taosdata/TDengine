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
#include "bmInt.h"

static int32_t bmRequire(const SMgmtInputOpt *pInput, bool *required) {
  return dmReadFile(pInput->path, pInput->name, required);
}

static void bmInitOption(SBnodeMgmt *pMgmt, SBnodeOpt *pOption) { pOption->msgCb = pMgmt->msgCb; }

static void bmClose(SBnodeMgmt *pMgmt) {
  if (pMgmt->pBnode != NULL) {
    bmStopWorker(pMgmt);
    bndClose(pMgmt->pBnode);
    pMgmt->pBnode = NULL;
  }

  taosMemoryFree(pMgmt);
}

int32_t bmOpen(SMgmtInputOpt *pInput, SMgmtOutputOpt *pOutput) {
  SBnodeMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SBnodeMgmt));
  if (pMgmt == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pMgmt->pData = pInput->pData;
  pMgmt->path = pInput->path;
  pMgmt->name = pInput->name;
  pMgmt->msgCb = pInput->msgCb;
  pMgmt->msgCb.mgmt = pMgmt;

  SBnodeOpt option = {0};
  bmInitOption(pMgmt, &option);
  pMgmt->pBnode = bndOpen(pMgmt->path, &option);
  if (pMgmt->pBnode == NULL) {
    dError("failed to open bnode since %s", terrstr());
    bmClose(pMgmt);
    return -1;
  }
  tmsgReportStartup("bnode-impl", "initialized");

  if (bmStartWorker(pMgmt) != 0) {
    dError("failed to start bnode worker since %s", terrstr());
    bmClose(pMgmt);
    return -1;
  }
  tmsgReportStartup("bnode-worker", "initialized");

  pOutput->pMgmt = pMgmt;
  return 0;
}

SMgmtFunc bmGetMgmtFunc() {
  SMgmtFunc mgmtFunc = {0};
  mgmtFunc.openFp = bmOpen;
  mgmtFunc.closeFp = (NodeCloseFp)bmClose;
  mgmtFunc.createFp = (NodeCreateFp)bmProcessCreateReq;
  mgmtFunc.dropFp = (NodeDropFp)bmProcessDropReq;
  mgmtFunc.requiredFp = bmRequire;
  mgmtFunc.getHandlesFp = bmGetMsgHandles;

  return mgmtFunc;
}
