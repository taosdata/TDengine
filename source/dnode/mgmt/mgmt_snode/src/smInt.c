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
#include "smInt.h"
#include "libs/function/function.h"

static int32_t smRequire(const SMgmtInputOpt *pInput, bool *required) {
  return dmReadFile(pInput->path, pInput->name, required);
}

static void smInitOption(SSnodeMgmt *pMgmt, SSnodeOpt *pOption) { pOption->msgCb = pMgmt->msgCb; }

static void smClose(SSnodeMgmt *pMgmt) {
  dInfo("snode-mgmt start to cleanup");
  if (pMgmt->pSnode != NULL) {
    smStopWorker(pMgmt);
    sndClose(pMgmt->pSnode);
    pMgmt->pSnode = NULL;
  }

  taosMemoryFree(pMgmt);
  dInfo("snode-mgmt is cleaned up");
}

int32_t smOpen(const SMgmtInputOpt *pInput, SMgmtOutputOpt *pOutput) {
  dInfo("snode-mgmt start to init");
  SSnodeMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SSnodeMgmt));
  if (pMgmt == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pMgmt->path = pInput->path;
  pMgmt->name = pInput->name;
  pMgmt->dnodeId = pInput->dnodeId;
  pMgmt->msgCb = pInput->msgCb;
  pMgmt->msgCb.pMgmt = pMgmt;

  SSnodeOpt option = {0};
  smInitOption(pMgmt, &option);
  pMgmt->pSnode = sndOpen(pMgmt->path, &option);
  if (pMgmt->pSnode == NULL) {
    dError("failed to open snode since %s", terrstr());
    smClose(pMgmt);
    return -1;
  }
  tmsgReportStartup("snode-impl", "initialized");

  if (smStartWorker(pMgmt) != 0) {
    dError("failed to start snode worker since %s", terrstr());
    smClose(pMgmt);
    return -1;
  }
  tmsgReportStartup("snode-worker", "initialized");

  if (udfcOpen() != 0) {
    dError("failed to open udfc in snode");
    smClose(pMgmt);
    return -1;
  }

  pOutput->pMgmt = pMgmt;
  return 0;
}

SMgmtFunc smGetMgmtFunc() {
  SMgmtFunc mgmtFunc = {0};
  mgmtFunc.openFp = smOpen;
  mgmtFunc.closeFp = (NodeCloseFp)smClose;
  mgmtFunc.createFp = (NodeCreateFp)smProcessCreateReq;
  mgmtFunc.dropFp = (NodeDropFp)smProcessDropReq;
  mgmtFunc.requiredFp = smRequire;
  mgmtFunc.getHandlesFp = smGetMsgHandles;

  return mgmtFunc;
}
