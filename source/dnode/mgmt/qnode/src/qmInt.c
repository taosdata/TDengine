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

static int32_t qmRequire(SMgmtWrapper *pWrapper, bool *required) { return dndReadFile(pWrapper, required); }

static void qmInitOption(SQnodeMgmt *pMgmt, SQnodeOpt *pOption) {
  SMsgCb msgCb = {0};
  msgCb.pWrapper = pMgmt->pWrapper;
  msgCb.queueFps[QUERY_QUEUE] = qmPutMsgToQueryQueue;
  msgCb.queueFps[FETCH_QUEUE] = qmPutMsgToFetchQueue;
  msgCb.sendReqFp = dndSendReqToDnode;
  msgCb.sendMnodeReqFp = dndSendReqToMnode;
  msgCb.sendRspFp = dndSendRsp;
  pOption->msgCb = msgCb;
}

static int32_t qmOpenImp(SQnodeMgmt *pMgmt) {
  SQnodeOpt option = {0};
  qmInitOption(pMgmt, &option);

  pMgmt->pQnode = qndOpen(&option);
  if (pMgmt->pQnode == NULL) {
    dError("failed to open qnode since %s", terrstr());
    return -1;
  }

  if (qmStartWorker(pMgmt) != 0) {
    dError("failed to start qnode worker since %s", terrstr());
    return -1;
  }

  bool deployed = true;
  if (dndWriteFile(pMgmt->pWrapper, deployed) != 0) {
    dError("failed to write qnode file since %s", terrstr());
    return -1;
  }

  return 0;
}

static void qmCloseImp(SQnodeMgmt *pMgmt) {
  if (pMgmt->pQnode != NULL) {
    qmStopWorker(pMgmt);
    qndClose(pMgmt->pQnode);
    pMgmt->pQnode = NULL;
  }
}

int32_t qmDrop(SMgmtWrapper *pWrapper) {
  SQnodeMgmt *pMgmt = pWrapper->pMgmt;
  if (pMgmt == NULL) return 0;

  dInfo("qnode-mgmt start to drop");
  bool deployed = false;
  if (dndWriteFile(pWrapper, deployed) != 0) {
    dError("failed to drop qnode since %s", terrstr());
    return -1;
  }

  qmCloseImp(pMgmt);
  taosRemoveDir(pMgmt->path);
  pWrapper->pMgmt = NULL;
  free(pMgmt);
  dInfo("qnode-mgmt is dropped");
  return 0;
}

static void qmClose(SMgmtWrapper *pWrapper) {
  SQnodeMgmt *pMgmt = pWrapper->pMgmt;
  if (pMgmt == NULL) return;

  dInfo("qnode-mgmt start to cleanup");
  qmCloseImp(pMgmt);
  pWrapper->pMgmt = NULL;
  free(pMgmt);
  dInfo("qnode-mgmt is cleaned up");
}

int32_t qmOpen(SMgmtWrapper *pWrapper) {
  dInfo("qnode-mgmt start to init");
  SQnodeMgmt *pMgmt = calloc(1, sizeof(SQnodeMgmt));
  if (pMgmt == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pMgmt->path = pWrapper->path;
  pMgmt->pDnode = pWrapper->pDnode;
  pMgmt->pWrapper = pWrapper;
  pWrapper->pMgmt = pMgmt;

  int32_t code = qmOpenImp(pMgmt);
  if (code != 0) {
    dError("failed to init qnode-mgmt since %s", terrstr());
    qmClose(pWrapper);
  } else {
    dInfo("qnode-mgmt is initialized");
  }

  return code;
}

void qmGetMgmtFp(SMgmtWrapper *pWrapper) {
  SMgmtFp mgmtFp = {0};
  mgmtFp.openFp = qmOpen;
  mgmtFp.closeFp = qmClose;
  mgmtFp.createMsgFp = qmProcessCreateReq;
  mgmtFp.dropMsgFp = qmProcessDropReq;
  mgmtFp.requiredFp = qmRequire;

  qmInitMsgHandles(pWrapper);
  pWrapper->name = "qnode";
  pWrapper->fp = mgmtFp;
}
