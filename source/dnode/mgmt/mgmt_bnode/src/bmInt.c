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

static int32_t bmRequire(SMgmtWrapper *pWrapper, bool *required) { return dmReadFile(pWrapper, required); }

static void bmInitOption(SBnodeMgmt *pMgmt, SBnodeOpt *pOption) {
  SMsgCb msgCb = pMgmt->pDnode->data.msgCb;
  msgCb.pWrapper = pMgmt->pWrapper;
  pOption->msgCb = msgCb;
}

static void bmClose(SMgmtWrapper *pWrapper) {
  SBnodeMgmt *pMgmt = pWrapper->pMgmt;
  if (pMgmt == NULL) return;

  dInfo("bnode-mgmt start to cleanup");
  if (pMgmt->pBnode != NULL) {
    bmStopWorker(pMgmt);
    bndClose(pMgmt->pBnode);
    pMgmt->pBnode = NULL;
  }

  pWrapper->pMgmt = NULL;
  taosMemoryFree(pMgmt);
  dInfo("bnode-mgmt is cleaned up");
}

int32_t bmOpen(SMgmtWrapper *pWrapper) {
  dInfo("bnode-mgmt start to init");
  SBnodeMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SBnodeMgmt));
  if (pMgmt == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pMgmt->path = pWrapper->path;
  pMgmt->pDnode = pWrapper->pDnode;
  pMgmt->pWrapper = pWrapper;
  pWrapper->pMgmt = pMgmt;

  SBnodeOpt option = {0};
  bmInitOption(pMgmt, &option);
  pMgmt->pBnode = bndOpen(pMgmt->path, &option);
  if (pMgmt->pBnode == NULL) {
    dError("failed to open bnode since %s", terrstr());
    bmClose(pWrapper);
    return -1;
  }
  dmReportStartup(pWrapper->pDnode, "bnode-impl", "initialized");

  if (bmStartWorker(pMgmt) != 0) {
    dError("failed to start bnode worker since %s", terrstr());
    bmClose(pWrapper);
    return -1;
  }
  dmReportStartup(pWrapper->pDnode, "bnode-worker", "initialized");

  return 0;
}

void bmSetMgmtFp(SMgmtWrapper *pWrapper) {
  SMgmtFp mgmtFp = {0};
  mgmtFp.openFp = bmOpen;
  mgmtFp.closeFp = bmClose;
  mgmtFp.createFp = bmProcessCreateReq;
  mgmtFp.dropFp = bmProcessDropReq;
  mgmtFp.requiredFp = bmRequire;

  bmInitMsgHandle(pWrapper);
  pWrapper->name = "bnode";
  pWrapper->fp = mgmtFp;
}
