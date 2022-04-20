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

static int32_t qmRequire(SMgmtWrapper *pWrapper, bool *required) { return dmReadFile(pWrapper, required); }

static void qmInitOption(SQnodeMgmt *pMgmt, SQnodeOpt *pOption) {
  SMsgCb msgCb = pMgmt->pDnode->data.msgCb;
  msgCb.pWrapper = pMgmt->pWrapper;
  msgCb.queueFps[QUERY_QUEUE] = qmPutMsgToQueryQueue;
  msgCb.queueFps[FETCH_QUEUE] = qmPutMsgToFetchQueue;
  msgCb.qsizeFp = qmGetQueueSize;
  pOption->msgCb = msgCb;
}

static void qmClose(SMgmtWrapper *pWrapper) {
  SQnodeMgmt *pMgmt = pWrapper->pMgmt;
  if (pMgmt == NULL) return;

  dInfo("qnode-mgmt start to cleanup");
  if (pMgmt->pQnode != NULL) {
    qmStopWorker(pMgmt);
    qndClose(pMgmt->pQnode);
    pMgmt->pQnode = NULL;
  }

  pWrapper->pMgmt = NULL;
  taosMemoryFree(pMgmt);
  dInfo("qnode-mgmt is cleaned up");
}

static int32_t qmOpen(SMgmtWrapper *pWrapper) {
  dInfo("qnode-mgmt start to init");
  SQnodeMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SQnodeMgmt));
  if (pMgmt == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pMgmt->path = pWrapper->path;
  pMgmt->pDnode = pWrapper->pDnode;
  pMgmt->pWrapper = pWrapper;
  pWrapper->pMgmt = pMgmt;

  SQnodeOpt option = {0};
  qmInitOption(pMgmt, &option);
  pMgmt->pQnode = qndOpen(&option);
  if (pMgmt->pQnode == NULL) {
    dError("failed to open qnode since %s", terrstr());
    qmClose(pWrapper);
    return -1;
  }
  dmReportStartup(pWrapper->pDnode, "qnode-impl", "initialized");

  if (qmStartWorker(pMgmt) != 0) {
    dError("failed to start qnode worker since %s", terrstr());
    qmClose(pWrapper);
    return -1;
  }
  dmReportStartup(pWrapper->pDnode, "qnode-worker", "initialized");

  dInfo("qnode-mgmt is initialized");
  return 0;
}

void qmSetMgmtFp(SMgmtWrapper *pWrapper) {
  SMgmtFp mgmtFp = {0};
  mgmtFp.openFp = qmOpen;
  mgmtFp.closeFp = qmClose;
  mgmtFp.createFp = qmProcessCreateReq;
  mgmtFp.dropFp = qmProcessDropReq;
  mgmtFp.requiredFp = qmRequire;

  qmInitMsgHandle(pWrapper);
  pWrapper->name = "qnode";
  pWrapper->fp = mgmtFp;
}
