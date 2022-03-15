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
#include "dmMsg.h"

#include "bmInt.h"
#include "mmInt.h"
#include "qmInt.h"
#include "smInt.h"
#include "vmInt.h"

static void *dmThreadRoutine(void *param) {
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
      dmSendStatusReq(pMgmt);
      lastStatusTime = curTime;
    }

    float monitorInterval = (curTime - lastMonitorTime) / 1000.0f;
    if (monitorInterval >= tsMonitorInterval) {
      dndSendMonitorReport(pDnode);
      lastMonitorTime = curTime;
    }
  }
}

static void dmProcessMgmtQueue(SDnode *pDnode, SNodeMsg *pNodeMsg) {
  int32_t  code = 0;
  SRpcMsg *pMsg = &pNodeMsg->rpcMsg;
  dTrace("msg:%p, will be processed in mgmt queue", pNodeMsg);

  switch (pMsg->msgType) {
    case TDMT_DND_CREATE_MNODE:
      code = mmProcessCreateReq(pDnode, pMsg);
      break;
    case TDMT_DND_ALTER_MNODE:
      code = mmProcessAlterReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_MNODE:
      code = mmProcessDropReq(pDnode, pMsg);
      break;
    case TDMT_DND_CREATE_QNODE:
      code = qmProcessCreateReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_QNODE:
      code = qmProcessDropReq(pDnode, pMsg);
      break;
    case TDMT_DND_CREATE_SNODE:
      code = smProcessCreateReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_SNODE:
      code = smProcessDropReq(pDnode, pMsg);
      break;
    case TDMT_DND_CREATE_BNODE:
      code = bmProcessCreateReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_BNODE:
      code = bmProcessDropReq(pDnode, pMsg);
      break;
    case TDMT_DND_CONFIG_DNODE:
      code = dmProcessConfigReq(pDnode, pMsg);
      break;
    case TDMT_MND_STATUS_RSP:
      dmProcessStatusRsp(pDnode, pMsg);
      break;
    case TDMT_MND_AUTH_RSP:
      dmProcessAuthRsp(pDnode, pMsg);
      break;
    case TDMT_MND_GRANT_RSP:
      dmProcessGrantRsp(pDnode, pMsg);
      break;
    case TDMT_DND_CREATE_VNODE:
      code = vmProcessCreateVnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_ALTER_VNODE:
      code = vmProcessAlterVnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_VNODE:
      code = vmProcessDropVnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_SYNC_VNODE:
      code = vmProcessSyncVnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_COMPACT_VNODE:
      code = vmProcessCompactVnodeReq(pDnode, pMsg);
      break;
    default:
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      code = -1;
      dError("RPC %p, dnode msg:%s not processed", pMsg->handle, TMSG_INFO(pMsg->msgType));
      break;
  }

  if (pMsg->msgType & 1u) {
    if (code != 0) code = terrno;
    SRpcMsg rsp = {.code = code, .handle = pMsg->handle, .ahandle = pMsg->ahandle};
    rpcSendResponse(&rsp);
  }

  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = NULL;
  taosFreeQitem(pNodeMsg);
}

int32_t dmStartWorker(SDnodeMgmt *pMgmt) {
  if (dndInitWorker(pMgmt->pDnode, &pMgmt->mgmtWorker, DND_WORKER_SINGLE, "dnode-mgmt", 1, 1, dmProcessMgmtQueue) !=
      0) {
    dError("failed to start dnode mgmt worker since %s", terrstr());
    return -1;
  }

  if (dndInitWorker(pMgmt->pDnode, &pMgmt->statusWorker, DND_WORKER_SINGLE, "dnode-status", 1, 1,
                    dmProcessMgmtQueue) != 0) {
    dError("failed to start dnode mgmt worker since %s", terrstr());
    return -1;
  }

  pMgmt->threadId = taosCreateThread(dmThreadRoutine, pMgmt);
  if (pMgmt->threadId == NULL) {
    dError("failed to init dnode thread");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

void dmStopWorker(SDnodeMgmt *pMgmt) {
  dndCleanupWorker(&pMgmt->mgmtWorker);
  dndCleanupWorker(&pMgmt->statusWorker);

  if (pMgmt->threadId != NULL) {
    taosDestoryThread(pMgmt->threadId);
    pMgmt->threadId = NULL;
  }
}

int32_t dmProcessMgmtMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SDnodeMgmt *pMgmt = pWrapper->pMgmt;

  SDnodeWorker *pWorker = &pMgmt->mgmtWorker;
  if (pMsg->rpcMsg.msgType == TDMT_MND_STATUS_RSP) {
    pWorker = &pMgmt->statusWorker;
  }

  dTrace("msg:%p, will be written to worker %s", pMsg, pWorker->name);
  return dndWriteMsgToWorker(pWorker, pMsg, sizeof(SNodeMsg));
}