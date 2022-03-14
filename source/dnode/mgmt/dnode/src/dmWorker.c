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
#include "dmWorker.h"
#include "dndWorker.h"
#include "dmHandle.h"

static void *dnodeThreadRoutine(void *param) {
  SDnodeMgmt *pMgmt = param;
  SDnode     *pDnode = pMgmt->pDnode;
  int64_t     lastStatusTime = taosGetTimestampMs();
  int64_t     lastMonitorTime = lastStatusTime;

  setThreadName("dnode-hb");

  while (true) {
    pthread_testcancel();
    taosMsleep(200);
    if (dndGetStatus(pDnode) != DND_STAT_RUNNING || pMgmt->dropped) {
      continue;
    }

    int64_t curTime = taosGetTimestampMs();

    float statusInterval = (curTime - lastStatusTime) / 1000.0f;
    if (statusInterval >= tsStatusInterval && !pMgmt->statusSent) {
      dndSendStatusReq(pDnode);
      lastStatusTime = curTime;
    }

    // float monitorInterval = (curTime - lastMonitorTime) / 1000.0f;
    // if (monitorInterval >= tsMonitorInterval) {
    //   dndSendMonitorReport(pDnode);
    //   lastMonitorTime = curTime;
    // }
  }
}

static void dndProcessMgmtQueue(SDnode *pDnode, SRpcMsg *pMsg) {
  int32_t code = 0;

#if 0
  switch (pMsg->msgType) {
    case TDMT_DND_CREATE_MNODE:
      code = mmProcessCreateMnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_ALTER_MNODE:
      code = mmProcessAlterMnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_MNODE:
      code = mmProcessDropMnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_CREATE_QNODE:
      code = dndProcessCreateQnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_QNODE:
      code = dndProcessDropQnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_CREATE_SNODE:
      code = dndProcessCreateSnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_SNODE:
      code = dndProcessDropSnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_CREATE_BNODE:
      code = dndProcessCreateBnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_BNODE:
      code = dndProcessDropBnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_CONFIG_DNODE:
      code = dndProcessConfigDnodeReq(pDnode, pMsg);
      break;
    case TDMT_MND_STATUS_RSP:
      dndProcessStatusRsp(pDnode, pMsg);
      break;
    case TDMT_MND_AUTH_RSP:
      dndProcessAuthRsp(pDnode, pMsg);
      break;
    case TDMT_MND_GRANT_RSP:
      dndProcessGrantRsp(pDnode, pMsg);
      break;
    case TDMT_DND_CREATE_VNODE:
      code = dndProcessCreateVnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_ALTER_VNODE:
      code = dndProcessAlterVnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_VNODE:
      code = dndProcessDropVnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_SYNC_VNODE:
      code = dndProcessSyncVnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_COMPACT_VNODE:
      code = dndProcessCompactVnodeReq(pDnode, pMsg);
      break;
    default:
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      code = -1;
      dError("RPC %p, dnode msg:%s not processed", pMsg->handle, TMSG_INFO(pMsg->msgType));
      break;
  }
#endif
  if (pMsg->msgType & 1u) {
    if (code != 0) code = terrno;
    SRpcMsg rsp = {.code = code, .handle = pMsg->handle, .ahandle = pMsg->ahandle};
    rpcSendResponse(&rsp);
  }

  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = NULL;
  taosFreeQitem(pMsg);
}

int32_t dmStartWorker(SDnodeMgmt *pMgmt) {
  if (dndInitWorker(NULL, &pMgmt->mgmtWorker, DND_WORKER_SINGLE, "dnode-mgmt", 1, 1, dndProcessMgmtQueue) != 0) {
    dError("failed to start dnode mgmt worker since %s", terrstr());
    return -1;
  }

  if (dndInitWorker(NULL, &pMgmt->statusWorker, DND_WORKER_SINGLE, "dnode-status", 1, 1, dndProcessMgmtQueue) != 0) {
    dError("failed to start dnode mgmt worker since %s", terrstr());
    return -1;
  }

//   pMgmt->threadId = taosCreateThread(dnodeThreadRoutine, pDnode);
//   if (pMgmt->threadId == NULL) {
//     dError("failed to init dnode thread");
//     terrno = TSDB_CODE_OUT_OF_MEMORY;
//     return -1;
//   }

  return 0;
}

void dmStopWorker(SDnodeMgmt *pMgmt) {
    #if 0
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

  taosWLockLatch(&pMgmt->latch);
  pMgmt->deployed = 0;
  taosWUnLockLatch(&pMgmt->latch);

  while (pMgmt->refCount > 1) {
    taosMsleep(10);
  }

  dndCleanupWorker(&pMgmt->readWorker);
  dndCleanupWorker(&pMgmt->writeWorker);
  dndCleanupWorker(&pMgmt->syncWorker);
  #endif
}
