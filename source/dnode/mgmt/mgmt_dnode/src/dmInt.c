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

static int32_t dmStartMgmt(SDnodeMgmt *pMgmt) {
  if (dmStartStatusThread(pMgmt) != 0) {
    return -1;
  }
  if (dmStartMonitorThread(pMgmt) != 0) {
    return -1;
  }
  return 0;
}

static void dmStopMgmt(SDnodeMgmt *pMgmt) {
  dmStopMonitorThread(pMgmt);
  dmStopStatusThread(pMgmt);
}

static int32_t dmOpenMgmt(const SMgmtInputOpt *pInput, SMgmtOutputOpt *pOutput) {
  dInfo("dnode-mgmt start to init");
  SDnodeMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SDnodeMgmt));
  if (pMgmt == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pMgmt->data.dnodeId = 0;
  pMgmt->data.clusterId = 0;
  pMgmt->data.dnodeVer = 0;
  pMgmt->data.updateTime = 0;
  pMgmt->data.rebootTime = taosGetTimestampMs();
  pMgmt->data.dropped = 0;
  pMgmt->data.localEp = pInput->localEp;
  pMgmt->data.localFqdn = pInput->localFqdn;
  pMgmt->data.firstEp = pInput->firstEp;
  pMgmt->data.secondEp = pInput->secondEp;
  pMgmt->data.supportVnodes = pInput->supportVnodes;
  pMgmt->data.serverPort = pInput->serverPort;
  pMgmt->pDnode = pInput->pDnode;
  pMgmt->msgCb = pInput->msgCb;
  pMgmt->path = pInput->path;
  pMgmt->name = pInput->name;
  pMgmt->processCreateNodeFp = pInput->processCreateNodeFp;
  pMgmt->processDropNodeFp = pInput->processDropNodeFp;
  pMgmt->isNodeDeployedFp = pInput->isNodeDeployedFp;
  taosInitRWLatch(&pMgmt->data.latch);

  pMgmt->data.dnodeHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (pMgmt->data.dnodeHash == NULL) {
    dError("failed to init dnode hash");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (dmReadEps(pMgmt) != 0) {
    dError("failed to read file since %s", terrstr());
    return -1;
  }

  if (pMgmt->data.dropped) {
    dError("dnode will not start since its already dropped");
    return -1;
  }

  if (dmStartWorker(pMgmt) != 0) {
    return -1;
  }

  if (udfStartUdfd(pMgmt->data.dnodeId) != 0) {
    dError("failed to start udfd");
  }

  pOutput->pMgmt = pMgmt;
  dInfo("dnode-mgmt is initialized");
  return 0;
}

static void dmCloseMgmt(SDnodeMgmt *pMgmt) {
  dInfo("dnode-mgmt start to clean up");
  dmStopWorker(pMgmt);

  taosWLockLatch(&pMgmt->data.latch);
  if (pMgmt->data.dnodeEps != NULL) {
    taosArrayDestroy(pMgmt->data.dnodeEps);
    pMgmt->data.dnodeEps = NULL;
  }
  if (pMgmt->data.dnodeHash != NULL) {
    taosHashCleanup(pMgmt->data.dnodeHash);
    pMgmt->data.dnodeHash = NULL;
  }
  taosWUnLockLatch(&pMgmt->data.latch);
  taosMemoryFree(pMgmt);

  dInfo("dnode-mgmt is cleaned up");
}

static int32_t dmRequireMgmt(const SMgmtInputOpt *pInput, bool *required) {
  *required = true;
  return 0;
}

SMgmtFunc dmGetMgmtFunc() {
  SMgmtFunc mgmtFunc = {0};
  mgmtFunc.openFp = dmOpenMgmt;
  mgmtFunc.closeFp = (NodeCloseFp)dmCloseMgmt;
  mgmtFunc.startFp = (NodeStartFp)dmStartMgmt;
  mgmtFunc.stopFp = (NodeStopFp)dmStopMgmt;
  mgmtFunc.requiredFp = dmRequireMgmt;
  mgmtFunc.getHandlesFp = dmGetMsgHandles;

  return mgmtFunc;
}
