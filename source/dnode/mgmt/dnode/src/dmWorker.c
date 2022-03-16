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

#include "bmInt.h"
#include "mm.h"
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
    if (dndGetStatus(pDnode) != DND_STAT_RUNNING || pDnode->dropped) {
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

static void dmProcessQueue(SDnode *pDnode, SNodeMsg *pMsg) {
  int32_t code = -1;
  tmsg_t  msgType = pMsg->rpcMsg.msgType;
  dTrace("msg:%p, will be processed", pMsg);

  switch (msgType) {
    case TDMT_DND_CREATE_MNODE:
      code = mmProcessCreateReq(dndGetWrapper(pDnode, MNODE)->pMgmt, pMsg);
      break;
    case TDMT_DND_DROP_MNODE:
      code = mmProcessDropReq(dndGetWrapper(pDnode, MNODE)->pMgmt, pMsg);
      break;
    case TDMT_DND_CREATE_QNODE:
      code = qmProcessCreateReq(dndGetWrapper(pDnode, QNODE)->pMgmt, pMsg);
      break;
    case TDMT_DND_DROP_QNODE:
      code = qmProcessDropReq(dndGetWrapper(pDnode, QNODE)->pMgmt, pMsg);
      break;
    case TDMT_DND_CREATE_SNODE:
      code = smProcessCreateReq(dndGetWrapper(pDnode, SNODE)->pMgmt, pMsg);
      break;
    case TDMT_DND_DROP_SNODE:
      code = smProcessDropReq(dndGetWrapper(pDnode, SNODE)->pMgmt, pMsg);
      break;
    case TDMT_DND_CREATE_BNODE:
      code = bmProcessCreateReq(dndGetWrapper(pDnode, BNODE)->pMgmt, pMsg);
      break;
    case TDMT_DND_DROP_BNODE:
      code = bmProcessDropReq(dndGetWrapper(pDnode, BNODE)->pMgmt, pMsg);
      break;
    case TDMT_DND_CONFIG_DNODE:
      code = dmProcessConfigReq(dndGetWrapper(pDnode, DNODE)->pMgmt, pMsg);
      break;
    case TDMT_MND_STATUS_RSP:
      code = dmProcessStatusRsp(dndGetWrapper(pDnode, DNODE)->pMgmt, pMsg);
      break;
    case TDMT_MND_AUTH_RSP:
      code = dmProcessAuthRsp(dndGetWrapper(pDnode, DNODE)->pMgmt, pMsg);
      break;
    case TDMT_MND_GRANT_RSP:
      code = dmProcessGrantRsp(dndGetWrapper(pDnode, DNODE)->pMgmt, pMsg);
      break;
    default:
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      code = -1;
      dError("RPC %p, dnode msg:%s not processed", pMsg->rpcMsg.handle, TMSG_INFO(msgType));
      break;
  }

  if (msgType & 1u) {
    if (code != 0) code = terrno;
    SRpcMsg rsp = {.code = code, .handle = pMsg->rpcMsg.handle, .ahandle = pMsg->rpcMsg.ahandle};
    rpcSendResponse(&rsp);
  }

  rpcFreeCont(pMsg->rpcMsg.pCont);
  pMsg->rpcMsg.pCont = NULL;
  taosFreeQitem(pMsg);
  dTrace("msg:%p, is freed", pMsg);
}

int32_t dmStartWorker(SDnodeMgmt *pMgmt) {
  SDnode *pDnode = pMgmt->pDnode;

  if (dndInitWorker(pDnode, &pMgmt->mgmtWorker, DND_WORKER_SINGLE, "dnode-mgmt", 1, 1, dmProcessQueue) != 0) {
    dError("failed to start dnode mgmt worker since %s", terrstr());
    return -1;
  }

  if (dndInitWorker(pDnode, &pMgmt->statusWorker, DND_WORKER_SINGLE, "dnode-status", 1, 1, dmProcessQueue) != 0) {
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

int32_t dmProcessMgmtMsg(SDnodeMgmt *pMgmt, SNodeMsg *pMsg) {
  SDnodeWorker *pWorker = &pMgmt->mgmtWorker;
  if (pMsg->rpcMsg.msgType == TDMT_MND_STATUS_RSP) {
    pWorker = &pMgmt->statusWorker;
  }

  dTrace("msg:%p, will be written to worker %s", pMsg, pWorker->name);
  return dndWriteMsgToWorker(pWorker, pMsg, 0);
}