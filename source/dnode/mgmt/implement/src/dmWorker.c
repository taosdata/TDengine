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
#include "dmImp.h"

static void *dmStatusThreadFp(void *param) {
  SDnode *pDnode = param;
  int64_t lastTime = taosGetTimestampMs();

  setThreadName("dnode-status");

  while (1) {
    taosThreadTestCancel();
    taosMsleep(200);

    if (pDnode->status != DND_STAT_RUNNING || pDnode->data.dropped) {
      continue;
    }

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
  SDnode *pDnode = param;
  int64_t lastTime = taosGetTimestampMs();

  setThreadName("dnode-monitor");

  while (1) {
    taosThreadTestCancel();
    taosMsleep(200);

    if (pDnode->status != DND_STAT_RUNNING || pDnode->data.dropped) {
      continue;
    }

    int64_t curTime = taosGetTimestampMs();
    float   interval = (curTime - lastTime) / 1000.0f;
    if (interval >= tsMonitorInterval) {
      dmSendMonitorReport(pDnode);
      lastTime = curTime;
    }
  }

  return NULL;
}

int32_t dmStartStatusThread(SDnode *pDnode) {
  pDnode->statusThreadId = taosCreateThread(dmStatusThreadFp, pDnode);
  if (pDnode->statusThreadId == NULL) {
    dError("failed to init dnode status thread");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

void dmStopStatusThread(SDnode *pDnode) {
  if (pDnode->statusThreadId != NULL) {
    taosDestoryThread(pDnode->statusThreadId);
    pDnode->statusThreadId = NULL;
  }
}

int32_t dmStartMonitorThread(SDnode *pDnode) {
  pDnode->monitorThreadId = taosCreateThread(dmMonitorThreadFp, pDnode);
  if (pDnode->monitorThreadId == NULL) {
    dError("failed to init dnode monitor thread");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

void dmStopMonitorThread(SDnode *pDnode) {
  if (pMgmt->monitorThreadId != NULL) {
    taosDestoryThread(pMgmt->monitorThreadId);
    pMgmt->monitorThreadId = NULL;
  }
}

static void dmProcessMgmtQueue(SQueueInfo *pInfo, SNodeMsg *pMsg) {
  SDnode  *pDnode = pInfo->ahandle;
  SRpcMsg *pRpc = &pMsg->rpcMsg;
  int32_t  code = -1;
  dTrace("msg:%p, will be processed in dnode-mgmt queue", pMsg);

  switch (pRpc->msgType) {
    case TDMT_DND_CONFIG_DNODE:
      code = dmProcessConfigReq(pMgmt, pMsg);
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

int32_t dmStartWorker(SDnodeData *pMgmt) {
  SSingleWorkerCfg cfg = {.min = 1, .max = 1, .name = "dnode-mgmt", .fp = (FItem)dmProcessMgmtQueue, .param = pMgmt};
  if (tSingleWorkerInit(&pMgmt->mgmtWorker, &cfg) != 0) {
    dError("failed to start dnode-mgmt worker since %s", terrstr());
    return -1;
  }

  dDebug("dnode workers are initialized");
  return 0;
}

void dmStopWorker(SDnodeData *pMgmt) {
  tSingleWorkerCleanup(&pMgmt->mgmtWorker);
  dDebug("dnode workers are closed");
}

int32_t dmProcessMgmtMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SDnodeData    *pMgmt = pWrapper->pMgmt;
  SSingleWorker *pWorker = &pMgmt->mgmtWorker;

  dTrace("msg:%p, put into worker %s", pMsg, pWorker->name);
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t dmProcessStatusMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SDnodeData    *pMgmt = pWrapper->pMgmt;
  SSingleWorker *pWorker = &pMgmt->monitorWorker;

  dTrace("msg:%p, put into worker %s", pMsg, pWorker->name);
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}
