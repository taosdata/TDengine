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

static int32_t smRequire(SMgmtWrapper *pWrapper, bool *required) { return dmReadFile(pWrapper, required); }

static void smInitOption(SSnodeMgmt *pMgmt, SSnodeOpt *pOption) {
  SMsgCb msgCb = pMgmt->pDnode->data.msgCb;
  msgCb.pWrapper = pMgmt->pWrapper;
  pOption->msgCb = msgCb;
}

static void smClose(SMgmtWrapper *pWrapper) {
  SSnodeMgmt *pMgmt = pWrapper->pMgmt;
  if (pMgmt == NULL) return;

  dInfo("snode-mgmt start to cleanup");
  if (pMgmt->pSnode != NULL) {
    smStopWorker(pMgmt);
    sndClose(pMgmt->pSnode);
    pMgmt->pSnode = NULL;
  }

  pWrapper->pMgmt = NULL;
  taosMemoryFree(pMgmt);
  dInfo("snode-mgmt is cleaned up");
}

int32_t smOpen(SMgmtWrapper *pWrapper) {
  dInfo("snode-mgmt start to init");
  SSnodeMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SSnodeMgmt));
  if (pMgmt == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pMgmt->path = pWrapper->path;
  pMgmt->pDnode = pWrapper->pDnode;
  pMgmt->pWrapper = pWrapper;
  pWrapper->pMgmt = pMgmt;

  SSnodeOpt option = {0};
  smInitOption(pMgmt, &option);
  pMgmt->pSnode = sndOpen(pMgmt->path, &option);
  if (pMgmt->pSnode == NULL) {
    dError("failed to open snode since %s", terrstr());
    return -1;
  }
  dmReportStartup(pWrapper->pDnode, "snode-impl", "initialized");

  if (smStartWorker(pMgmt) != 0) {
    dError("failed to start snode worker since %s", terrstr());
    return -1;
  }
  dmReportStartup(pWrapper->pDnode, "snode-worker", "initialized");

  return 0;
}

void smSetMgmtFp(SMgmtWrapper *pWrapper) {
  SMgmtFp mgmtFp = {0};
  mgmtFp.openFp = smOpen;
  mgmtFp.closeFp = smClose;
  mgmtFp.createFp = smProcessCreateReq;
  mgmtFp.dropFp = smProcessDropReq;
  mgmtFp.requiredFp = smRequire;

  smInitMsgHandle(pWrapper);
  pWrapper->name = "snode";
  pWrapper->fp = mgmtFp;
}
