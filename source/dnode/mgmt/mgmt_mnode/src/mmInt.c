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
#include "mmInt.h"
#include "wal.h"

static bool mmDeployRequired(const SMgmtInputOpt *pInput) {
  if (pInput->pData->dnodeId > 0) return false;
  if (pInput->pData->clusterId > 0) return false;
  if (strcmp(tsLocalEp, tsFirst) != 0) return false;
  return true;
}

static int32_t mmRequire(const SMgmtInputOpt *pInput, bool *required) {
  SMnodeMgmt mgmt = {0};
  mgmt.path = pInput->path;
  if (mmReadFile(&mgmt, required) != 0) {
    return -1;
  }

  if (!(*required)) {
    *required = mmDeployRequired(pInput);
  }

  return 0;
}

static void mmBuildOptionForDeploy(SMnodeMgmt *pMgmt, const SMgmtInputOpt *pInput, SMnodeOpt *pOption) {
  pOption->standby = false;
  pOption->deploy = true;
  pOption->msgCb = pMgmt->msgCb;
  pOption->dnodeId = pMgmt->pData->dnodeId;

  pOption->replica = 1;
  pOption->selfIndex = 0;

  SReplica *pReplica = &pOption->replicas[0];
  pReplica->id = 1;
  pReplica->port = tsServerPort;
  tstrncpy(pReplica->fqdn, tsLocalFqdn, TSDB_FQDN_LEN);
}

static void mmBuildOptionForOpen(SMnodeMgmt *pMgmt, SMnodeOpt *pOption) {
  pOption->deploy = false;
  pOption->standby = false;
  pOption->msgCb = pMgmt->msgCb;
  pOption->dnodeId = pMgmt->pData->dnodeId;

  if (pMgmt->replica > 0) {
    pOption->standby = true;
    pOption->replica = 1;
    pOption->selfIndex = 0;
    SReplica *pReplica = &pOption->replicas[0];
    for (int32_t i = 0; i < pMgmt->replica; ++i) {
      if (pMgmt->replicas[i].id != pMgmt->pData->dnodeId) continue;
      pReplica->id = pMgmt->replicas[i].id;
      pReplica->port = pMgmt->replicas[i].port;
      memcpy(pReplica->fqdn, pMgmt->replicas[i].fqdn, TSDB_FQDN_LEN);
    }
  }
}

static void mmClose(SMnodeMgmt *pMgmt) {
  if (pMgmt->pMnode != NULL) {
    mmStopWorker(pMgmt);
    mndClose(pMgmt->pMnode);
    taosThreadRwlockDestroy(&pMgmt->lock);
    pMgmt->pMnode = NULL;
  }

  taosMemoryFree(pMgmt);
}

static int32_t mmOpen(SMgmtInputOpt *pInput, SMgmtOutputOpt *pOutput) {
  if (walInit() != 0) {
    dError("failed to init wal since %s", terrstr());
    return -1;
  }

  if (syncInit() != 0) {
    dError("failed to init sync since %s", terrstr());
    return -1;
  }

  SMnodeMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SMnodeMgmt));
  if (pMgmt == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pMgmt->pData = pInput->pData;
  pMgmt->path = pInput->path;
  pMgmt->name = pInput->name;
  pMgmt->msgCb = pInput->msgCb;
  pMgmt->msgCb.queueFps[QUERY_QUEUE] = (PutToQueueFp)mmPutRpcMsgToQueryQueue;
  pMgmt->msgCb.queueFps[READ_QUEUE] = (PutToQueueFp)mmPutRpcMsgToReadQueue;
  pMgmt->msgCb.queueFps[WRITE_QUEUE] = (PutToQueueFp)mmPutRpcMsgToWriteQueue;
  pMgmt->msgCb.queueFps[SYNC_QUEUE] = (PutToQueueFp)mmPutRpcMsgToSyncQueue;
  pMgmt->msgCb.mgmt = pMgmt;
  taosThreadRwlockInit(&pMgmt->lock, NULL);

  bool deployed = false;
  if (mmReadFile(pMgmt, &deployed) != 0) {
    dError("failed to read file since %s", terrstr());
    mmClose(pMgmt);
    return -1;
  }

  SMnodeOpt option = {0};
  if (!deployed) {
    dInfo("mnode start to deploy");
    pMgmt->pData->dnodeId = 1;
    mmBuildOptionForDeploy(pMgmt, pInput, &option);
  } else {
    dInfo("mnode start to open");
    mmBuildOptionForOpen(pMgmt, &option);
  }

  pMgmt->pMnode = mndOpen(pMgmt->path, &option);
  if (pMgmt->pMnode == NULL) {
    dError("failed to open mnode since %s", terrstr());
    mmClose(pMgmt);
    return -1;
  }
  tmsgReportStartup("mnode-impl", "initialized");

  if (mmStartWorker(pMgmt) != 0) {
    dError("failed to start mnode worker since %s", terrstr());
    mmClose(pMgmt);
    return -1;
  }
  tmsgReportStartup("mnode-worker", "initialized");

  if (!deployed || pMgmt->replica > 0) {
    pMgmt->replica = 0;
    deployed = true;
    if (mmWriteFile(pMgmt, NULL, deployed) != 0) {
      dError("failed to write mnode file since %s", terrstr());
      return -1;
    }
  }

  pInput->pData->dnodeId = pMgmt->pData->dnodeId;
  pOutput->pMgmt = pMgmt;
  return 0;
}

static int32_t mmStart(SMnodeMgmt *pMgmt) {
  dDebug("mnode-mgmt start to run");
  return mndStart(pMgmt->pMnode);
}

static void mmStop(SMnodeMgmt *pMgmt) {
  dDebug("mnode-mgmt start to stop");
  mndStop(pMgmt->pMnode);
}

SMgmtFunc mmGetMgmtFunc() {
  SMgmtFunc mgmtFunc = {0};
  mgmtFunc.openFp = mmOpen;
  mgmtFunc.closeFp = (NodeCloseFp)mmClose;
  mgmtFunc.startFp = (NodeStartFp)mmStart;
  mgmtFunc.stopFp = (NodeStopFp)mmStop;
  mgmtFunc.createFp = (NodeCreateFp)mmProcessCreateReq;
  mgmtFunc.dropFp = (NodeDropFp)mmProcessDropReq;
  mgmtFunc.requiredFp = mmRequire;
  mgmtFunc.getHandlesFp = mmGetMsgHandles;

  return mgmtFunc;
}

int32_t mmAcquire(SMnodeMgmt *pMgmt) {
  int32_t code = 0;

  taosThreadRwlockRdlock(&pMgmt->lock);
  if (pMgmt->stopped) {
    code = -1;
  } else {
    atomic_add_fetch_32(&pMgmt->refCount, 1);
  }
  taosThreadRwlockUnlock(&pMgmt->lock);
  return code;
}

void mmRelease(SMnodeMgmt *pMgmt) {
  taosThreadRwlockRdlock(&pMgmt->lock);
  atomic_sub_fetch_32(&pMgmt->refCount, 1);
  taosThreadRwlockUnlock(&pMgmt->lock);
}