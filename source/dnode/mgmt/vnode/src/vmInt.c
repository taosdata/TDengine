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
#include "vmInt.h"
#include "vmMsg.h"
#include "vmMgmt.h"

static int32_t vmInit(SMgmtWrapper *pWrapper) {
  //     SDiskCfg dCfg = {0};
  // tstrncpy(dCfg.dir, pDnode->cfg.dataDir, TSDB_FILENAME_LEN);
  // dCfg.level = 0;
  // dCfg.primary = 1;
  // SDiskCfg *pDisks = pDnode->cfg.pDisks;
  // int32_t   numOfDisks = pDnode->cfg.numOfDisks;
  // if (numOfDisks <= 0 || pDisks == NULL) {
  //   pDisks = &dCfg;
  //   numOfDisks = 1;
  // }

  // pDnode->pTfs = tfsOpen(pDisks, numOfDisks);
  // if (pDnode->pTfs == NULL) {
  //   dError("failed to init tfs since %s", terrstr());
  //   return -1;
  // }
  if (walInit() != 0) {
    dError("failed to init wal since %s", terrstr());
    dndCleanup();
    return -1;
  }

  SVnodeOpt vnodeOpt = {0};
  vnodeOpt.nthreads = tsNumOfCommitThreads;
  vnodeOpt.putReqToVQueryQFp = dndPutReqToVQueryQ;
  vnodeOpt.sendReqToDnodeFp = dndSendReqToDnode;
  if (vnodeInit(&vnodeOpt) != 0) {
    dError("failed to init vnode since %s", terrstr());
    dndCleanup();
    return -1;
  }

  return 0;
}

static void vmCleanup(SMgmtWrapper *pWrapper) {
  vnodeCleanup();
}

static bool vmRequire(SMgmtWrapper *pWrapper) { return false; }

void vmGetMgmtFp(SMgmtWrapper *pWrapper) {
  SMgmtFp mgmtFp = {0};
  mgmtFp.openFp = vmInit;
  mgmtFp.closeFp = vmCleanup;
  mgmtFp.requiredFp = vmRequire;

  vmInitMsgHandles(pWrapper);
  pWrapper->name = "vnodes";
  pWrapper->fp = mgmtFp;
}
