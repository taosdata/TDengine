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
#include "dmHandle.h"
#include "dmMgmt.h"

int32_t dmGetDnodeId(SDnode *pDnode) {
  SMgmtWrapper *pWrapper = dndGetWrapper(pDnode, DNODE);
  SDnodeMgmt   *pMgmt = pWrapper->pMgmt;

  taosRLockLatch(&pMgmt->latch);
  int32_t dnodeId = pMgmt->dnodeId;
  taosRUnLockLatch(&pMgmt->latch);
  return dnodeId;
}

int64_t dmGetClusterId(SDnode *pDnode) {
  SMgmtWrapper *pWrapper = dndGetWrapper(pDnode, DNODE);
  SDnodeMgmt   *pMgmt = pWrapper->pMgmt;

  taosRLockLatch(&pMgmt->latch);
  int64_t clusterId = pMgmt->clusterId;
  taosRUnLockLatch(&pMgmt->latch);
  return clusterId;
}

SMgmtFp dmGetMgmtFp() {
  SMgmtFp mgmtFp = {0};
  mgmtFp.openFp = dmInit;
  mgmtFp.closeFp = dmCleanup;
  mgmtFp.requiredFp = dmRequire;
  mgmtFp.getMsgHandleFp = dmGetMsgHandle;
  return mgmtFp;
}
