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

static void *dmStatusThreadFp(void *param) {
  SDnodeMgmt *pMgmt = param;
  int64_t     lastTime = taosGetTimestampMs();

  setThreadName("dnode-status");

  while (1) {
    taosMsleep(200);
    taosThreadTestCancel();
    if (pMgmt->data.dropped) continue;

    int64_t curTime = taosGetTimestampMs();
    float   interval = (curTime - lastTime) / 1000.0f;
    if (interval >= tsStatusInterval) {
      dmSendStatusReq(pMgmt);
      lastTime = curTime;
    }
  }

  return NULL;
}

static void *dmMonitorThreadFp(void *param) {
  SDnodeMgmt *pMgmt = param;
  int64_t     lastTime = taosGetTimestampMs();

  setThreadName("dnode-monitor");

  while (1) {
    taosMsleep(200);
    taosThreadTestCancel();
    if (pMgmt->data.dropped) continue;

    int64_t curTime = taosGetTimestampMs();
    float   interval = (curTime - lastTime) / 1000.0f;
    if (interval >= tsMonitorInterval) {
      dmSendMonitorReport(pMgmt);
      lastTime = curTime;
    }
  }

  return NULL;
}

int32_t dmStartStatusThread(SDnodeMgmt *pMgmt) {
  pMgmt->statusThreadId = taosCreateThread(dmStatusThreadFp, pMgmt);
  if (pMgmt->statusThreadId == NULL) {
    dError("failed to init dnode status thread");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tmsgReportStartup("dnode-status", "initialized");
  return 0;
}

void dmStopStatusThread(SDnodeMgmt *pMgmt) {
  if (pMgmt->statusThreadId != NULL) {
    taosDestoryThread(pMgmt->statusThreadId);
    pMgmt->statusThreadId = NULL;
  }
}

int32_t dmStartMonitorThread(SDnodeMgmt *pMgmt) {
  pMgmt->monitorThreadId = taosCreateThread(dmMonitorThreadFp, pMgmt);
  if (pMgmt->monitorThreadId == NULL) {
    dError("failed to init dnode monitor thread");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tmsgReportStartup("dnode-monitor", "initialized");
  return 0;
}

void dmStopMonitorThread(SDnodeMgmt *pMgmt) {
  if (pMgmt->monitorThreadId != NULL) {
    taosDestoryThread(pMgmt->monitorThreadId);
    pMgmt->monitorThreadId = NULL;
  }
}

static void dmProcessMgmtQueue(SQueueInfo *pInfo, SNodeMsg *pMsg) {
  SDnodeMgmt *pMgmt = pInfo->ahandle;

  int32_t code = -1;
  tmsg_t  msgType = pMsg->rpcMsg.msgType;
  dTrace("msg:%p, will be processed in dnode-mgmt queue", pMsg);

  switch (msgType) {
    case TDMT_DND_CONFIG_DNODE:
      code = dmProcessConfigReq(pMgmt, pMsg);
      break;
    case TDMT_MND_AUTH_RSP:
      code = dmProcessAuthRsp(pMgmt, pMsg);
      break;
    case TDMT_MND_GRANT_RSP:
      code = dmProcessGrantRsp(pMgmt, pMsg);
      break;
    case TDMT_DND_CREATE_MNODE:
      code = (*pMgmt->processCreateNodeFp)(pMgmt->pDnode, MNODE, pMsg);
      break;
    case TDMT_DND_DROP_MNODE:
      code = (*pMgmt->processDropNodeFp)(pMgmt->pDnode, MNODE, pMsg);
      break;
    case TDMT_DND_CREATE_QNODE:
      code = (*pMgmt->processCreateNodeFp)(pMgmt->pDnode, QNODE, pMsg);
      break;
    case TDMT_DND_DROP_QNODE:
      code = (*pMgmt->processDropNodeFp)(pMgmt->pDnode, QNODE, pMsg);
      break;
    case TDMT_DND_CREATE_SNODE:
      code = (*pMgmt->processCreateNodeFp)(pMgmt->pDnode, SNODE, pMsg);
      break;
    case TDMT_DND_DROP_SNODE:
      code = (*pMgmt->processDropNodeFp)(pMgmt->pDnode, SNODE, pMsg);
      break;
    case TDMT_DND_CREATE_BNODE:
      code = (*pMgmt->processCreateNodeFp)(pMgmt->pDnode, BNODE, pMsg);
      break;
    case TDMT_DND_DROP_BNODE:
      code = (*pMgmt->processDropNodeFp)(pMgmt->pDnode, BNODE, pMsg);
      break;
    default:
      break;
  }

  if (msgType & 1u) {
    if (code != 0 && terrno != 0) code = terrno;
    SRpcMsg rsp = {
        .handle = pMsg->rpcMsg.handle,
        .ahandle = pMsg->rpcMsg.ahandle,
        .code = code,
        .refId = pMsg->rpcMsg.refId,
    };
    rpcSendResponse(&rsp);
  }

  dTrace("msg:%p, is freed, result:0x%04x:%s", pMsg, code & 0XFFFF, tstrerror(code));
  rpcFreeCont(pMsg->rpcMsg.pCont);
  taosFreeQitem(pMsg);
}

int32_t dmStartWorker(SDnodeMgmt *pMgmt) {
  SSingleWorkerCfg cfg = {
      .min = 1,
      .max = 1,
      .name = "dnode-mgmt",
      .fp = (FItem)dmProcessMgmtQueue,
      .param = pMgmt,
  };
  if (tSingleWorkerInit(&pMgmt->mgmtWorker, &cfg) != 0) {
    dError("failed to start dnode-mgmt worker since %s", terrstr());
    return -1;
  }

  dDebug("dnode workers are initialized");
  return 0;
}

void dmStopWorker(SDnodeMgmt *pMgmt) {
  tSingleWorkerCleanup(&pMgmt->mgmtWorker);
  dDebug("dnode workers are closed");
}

int32_t dmPutNodeMsgToMgmtQueue(SDnodeMgmt *pMgmt, SNodeMsg *pMsg) {
  SSingleWorker *pWorker = &pMgmt->mgmtWorker;
  dTrace("msg:%p, put into worker %s", pMsg, pWorker->name);
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}
