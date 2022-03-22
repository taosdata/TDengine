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

static int32_t smRequire(SMgmtWrapper *pWrapper, bool *required) { return dndReadFile(pWrapper, required); }

static void smInitOption(SSnodeMgmt *pMgmt, SSnodeOpt *pOption) {
  SMsgCb msgCb = {0};
  msgCb.pWrapper = pMgmt->pWrapper;
  msgCb.sendReqFp = dndSendReqToDnode;
  msgCb.sendMnodeReqFp = dndSendReqToMnode;
  msgCb.sendRspFp = dndSendRsp;
  pOption->msgCb = msgCb;
}

static int32_t smOpenImp(SSnodeMgmt *pMgmt) {
  SSnodeOpt option = {0};
  smInitOption(pMgmt, &option);

  pMgmt->pSnode = sndOpen(pMgmt->path, &option);
  if (pMgmt->pSnode == NULL) {
    dError("failed to open snode since %s", terrstr());
    return -1;
  }

  if (smStartWorker(pMgmt) != 0) {
    dError("failed to start snode worker since %s", terrstr());
    return -1;
  }

  bool deployed = true;
  if (dndWriteFile(pMgmt->pWrapper, deployed) != 0) {
    dError("failed to write snode file since %s", terrstr());
    return -1;
  }

  return 0;
}

static void smCloseImp(SSnodeMgmt *pMgmt) {
  if (pMgmt->pSnode != NULL) {
    smStopWorker(pMgmt);
    sndClose(pMgmt->pSnode);
    pMgmt->pSnode = NULL;
  }
}

int32_t smDrop(SMgmtWrapper *pWrapper) {
  SSnodeMgmt *pMgmt = pWrapper->pMgmt;
  if (pMgmt == NULL) return 0;

  dInfo("snode-mgmt start to drop");
  bool deployed = false;
  if (dndWriteFile(pWrapper, deployed) != 0) {
    dError("failed to drop snode since %s", terrstr());
    return -1;
  }

  smCloseImp(pMgmt);
  taosRemoveDir(pMgmt->path);
  pWrapper->pMgmt = NULL;
  free(pMgmt);
  dInfo("snode-mgmt is dropped");
  return 0;
}

static void smClose(SMgmtWrapper *pWrapper) {
  SSnodeMgmt *pMgmt = pWrapper->pMgmt;
  if (pMgmt == NULL) return;

  dInfo("snode-mgmt start to cleanup");
  smCloseImp(pMgmt);
  pWrapper->pMgmt = NULL;
  free(pMgmt);
  dInfo("snode-mgmt is cleaned up");
}

int32_t smOpen(SMgmtWrapper *pWrapper) {
  dInfo("snode-mgmt start to init");
  SSnodeMgmt *pMgmt = calloc(1, sizeof(SSnodeMgmt));
  if (pMgmt == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pMgmt->path = pWrapper->path;
  pMgmt->pDnode = pWrapper->pDnode;
  pMgmt->pWrapper = pWrapper;
  pWrapper->pMgmt = pMgmt;

  int32_t code = smOpenImp(pMgmt);
  if (code != 0) {
    dError("failed to init snode-mgmt since %s", terrstr());
    smClose(pWrapper);
  } else {
    dInfo("snode-mgmt is initialized");
  }

  return code;
}

void smGetMgmtFp(SMgmtWrapper *pWrapper) {
  SMgmtFp mgmtFp = {0};
  mgmtFp.openFp = smOpen;
  mgmtFp.closeFp = smClose;
  mgmtFp.createMsgFp = smProcessCreateReq;
  mgmtFp.dropMsgFp = smProcessDropReq;
  mgmtFp.requiredFp = smRequire;

  smInitMsgHandles(pWrapper);
  pWrapper->name = "snode";
  pWrapper->fp = mgmtFp;
}
