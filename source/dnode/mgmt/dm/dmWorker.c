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

static void *dmThreadRoutine(void *param) {
  SDnodeMgmt *pMgmt = param;
  SDnode     *pDnode = pMgmt->pDnode;
  int64_t     lastStatusTime = taosGetTimestampMs();
  int64_t     lastMonitorTime = lastStatusTime;

  setThreadName("dnode-hb");

  while (true) {
    taosThreadTestCancel();
    taosMsleep(200);
    if (dndGetStatus(pDnode) != DND_STAT_RUNNING || pDnode->dropped) {
      continue;
    }

    int64_t curTime = taosGetTimestampMs();
    float   statusInterval = (curTime - lastStatusTime) / 1000.0f;
    if (statusInterval >= tsStatusInterval && !pMgmt->statusSent) {
      dmSendStatusReq(pMgmt);
      lastStatusTime = curTime;
    }

    float monitorInterval = (curTime - lastMonitorTime) / 1000.0f;
    if (monitorInterval >= tsMonitorInterval) {
      dmSendMonitorReport(pDnode);
      lastMonitorTime = curTime;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t dmStartThread(SDnodeMgmt *pMgmt) {
  pMgmt->threadId = taosCreateThread(dmThreadRoutine, pMgmt);
  if (pMgmt->threadId == NULL) {
    dError("failed to init dnode thread");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static void dmProcessQueue(SQueueInfo *pInfo, SNodeMsg *pMsg) {
  SDnodeMgmt *pMgmt = pInfo->ahandle;

  SDnode  *pDnode = pMgmt->pDnode;
  SRpcMsg *pRpc = &pMsg->rpcMsg;
  int32_t  code = -1;
  dTrace("msg:%p, will be processed in dnode queue", pMsg);

  switch (pRpc->msgType) {
    case TDMT_DND_CONFIG_DNODE:
      code = dmProcessConfigReq(pMgmt, pMsg);
      break;
    case TDMT_MND_STATUS_RSP:
      code = dmProcessStatusRsp(pMgmt, pMsg);
      break;
    case TDMT_MND_AUTH_RSP:
      code = dmProcessAuthRsp(pMgmt, pMsg);
      break;
    case TDMT_MND_GRANT_RSP:
      code = dmProcessGrantRsp(pMgmt, pMsg);
      break;
    default:
      code = dmProcessCDnodeReq(pMgmt->pDnode, pMsg);
      break;
  }

  if (pRpc->msgType & 1u) {
    if (code != 0) code = terrno;
    SRpcMsg rsp = {.handle = pRpc->handle, .ahandle = pRpc->ahandle, .code = code};
    rpcSendResponse(&rsp);
  }

  dTrace("msg:%p, is freed, result:0x%04x:%s", pMsg, code & 0XFFFF, tstrerror(code));
  rpcFreeCont(pMsg->rpcMsg.pCont);
  taosFreeQitem(pMsg);
}

int32_t dmStartWorker(SDnodeMgmt *pMgmt) {
  SSingleWorkerCfg mcfg = {.min = 1, .max = 1, .name = "dnode-mgmt", .fp = (FItem)dmProcessQueue, .param = pMgmt};
  if (tSingleWorkerInit(&pMgmt->mgmtWorker, &mcfg) != 0) {
    dError("failed to start dnode mgmt worker since %s", terrstr());
    return -1;
  }

  SSingleWorkerCfg scfg = {.min = 1, .max = 1, .name = "dnode-monitor", .fp = (FItem)dmProcessQueue, .param = pMgmt};
  if (tSingleWorkerInit(&pMgmt->monitorWorker, &scfg) != 0) {
    dError("failed to start dnode monitor worker since %s", terrstr());
    return -1;
  }

  dDebug("dnode workers are initialized");
  return 0;
}

void dmStopWorker(SDnodeMgmt *pMgmt) {
  tSingleWorkerCleanup(&pMgmt->mgmtWorker);
  tSingleWorkerCleanup(&pMgmt->monitorWorker);

  if (pMgmt->threadId != NULL) {
    taosDestoryThread(pMgmt->threadId);
    pMgmt->threadId = NULL;
  }
  dDebug("dnode workers are closed");
}

int32_t dmProcessMgmtMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SDnodeMgmt    *pMgmt = pWrapper->pMgmt;
  SSingleWorker *pWorker = &pMgmt->mgmtWorker;

  dTrace("msg:%p, put into worker %s", pMsg, pWorker->name);
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t dmProcessMonitorMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SDnodeMgmt    *pMgmt = pWrapper->pMgmt;
  SSingleWorker *pWorker = &pMgmt->monitorWorker;

  dTrace("msg:%p, put into worker %s", pMsg, pWorker->name);
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}
