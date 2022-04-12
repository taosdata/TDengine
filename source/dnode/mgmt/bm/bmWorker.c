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
#include "bmInt.h"

static void bmSendErrorRsp(SNodeMsg *pMsg, int32_t code) {
  SRpcMsg rpcRsp = {.handle = pMsg->rpcMsg.handle, .ahandle = pMsg->rpcMsg.ahandle, .code = code};
  tmsgSendRsp(&rpcRsp);

  dTrace("msg:%p, is freed", pMsg);
  rpcFreeCont(pMsg->rpcMsg.pCont);
  taosFreeQitem(pMsg);
}

static void bmSendErrorRsps(STaosQall *qall, int32_t numOfMsgs, int32_t code) {
  for (int32_t i = 0; i < numOfMsgs; ++i) {
    SNodeMsg *pMsg = NULL;
    taosGetQitem(qall, (void **)&pMsg);
    if (pMsg != NULL) {
      bmSendErrorRsp(pMsg, code);
    }
  }
}

static inline void bmSendRsp(SNodeMsg *pMsg, int32_t code) {
  SRpcMsg rsp = {.handle = pMsg->rpcMsg.handle,
                 .ahandle = pMsg->rpcMsg.ahandle,
                 .code = code,
                 .pCont = pMsg->pRsp,
                 .contLen = pMsg->rspLen};
  tmsgSendRsp(&rsp);
}

static void bmProcessMonitorQueue(SQueueInfo *pInfo, SNodeMsg *pMsg) {
  SBnodeMgmt *pMgmt = pInfo->ahandle;

  dTrace("msg:%p, get from bnode-monitor queue", pMsg);
  SRpcMsg *pRpc = &pMsg->rpcMsg;
  int32_t  code = -1;

  if (pMsg->rpcMsg.msgType == TDMT_MON_BM_INFO) {
    code = bmProcessGetMonBmInfoReq(pMgmt->pWrapper, pMsg);
  } else {
    terrno = TSDB_CODE_MSG_NOT_PROCESSED;
  }

  if (pRpc->msgType & 1U) {
    if (code != 0 && terrno != 0) code = terrno;
    bmSendRsp(pMsg, code);
  }

  dTrace("msg:%p, is freed, result:0x%04x:%s", pMsg, code & 0XFFFF, tstrerror(code));
  rpcFreeCont(pRpc->pCont);
  taosFreeQitem(pMsg);
}

static void bmProcessWriteQueue(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SBnodeMgmt *pMgmt = pInfo->ahandle;

  SArray *pArray = taosArrayInit(numOfMsgs, sizeof(SNodeMsg *));
  if (pArray == NULL) {
    bmSendErrorRsps(qall, numOfMsgs, TSDB_CODE_OUT_OF_MEMORY);
    return;
  }

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    SNodeMsg *pMsg = NULL;
    taosGetQitem(qall, (void **)&pMsg);
    if (pMsg != NULL) {
      dTrace("msg:%p, get from bnode-write queue", pMsg);
      if (taosArrayPush(pArray, &pMsg) == NULL) {
        bmSendErrorRsp(pMsg, TSDB_CODE_OUT_OF_MEMORY);
      }
    }
  }

  bndProcessWMsgs(pMgmt->pBnode, pArray);

  for (size_t i = 0; i < numOfMsgs; i++) {
    SNodeMsg *pMsg = *(SNodeMsg **)taosArrayGet(pArray, i);
    if (pMsg != NULL) {
      dTrace("msg:%p, is freed", pMsg);
      rpcFreeCont(pMsg->rpcMsg.pCont);
      taosFreeQitem(pMsg);
    }
  }
  taosArrayDestroy(pArray);
}

int32_t bmProcessWriteMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SBnodeMgmt   *pMgmt = pWrapper->pMgmt;
  SMultiWorker *pWorker = &pMgmt->writeWorker;

  dTrace("msg:%p, put into worker:%s", pMsg, pWorker->name);
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t bmProcessMonitorMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SBnodeMgmt    *pMgmt = pWrapper->pMgmt;
  SSingleWorker *pWorker = &pMgmt->monitorWorker;

  dTrace("msg:%p, put into worker:%s", pMsg, pWorker->name);
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t bmStartWorker(SBnodeMgmt *pMgmt) {
  SMultiWorkerCfg cfg = {.max = 1, .name = "bnode-write", .fp = (FItems)bmProcessWriteQueue, .param = pMgmt};
  if (tMultiWorkerInit(&pMgmt->writeWorker, &cfg) != 0) {
    dError("failed to start bnode-write worker since %s", terrstr());
    return -1;
  }

  if (tsMultiProcess) {
    SSingleWorkerCfg mCfg = {
        .min = 1, .max = 1, .name = "bnode-monitor", .fp = (FItem)bmProcessMonitorQueue, .param = pMgmt};
    if (tSingleWorkerInit(&pMgmt->monitorWorker, &mCfg) != 0) {
      dError("failed to start bnode-monitor worker since %s", terrstr());
      return -1;
    }
  }

  dDebug("bnode workers are initialized");
  return 0;
}

void bmStopWorker(SBnodeMgmt *pMgmt) {
  tSingleWorkerCleanup(&pMgmt->monitorWorker);
  tMultiWorkerCleanup(&pMgmt->writeWorker);
  dDebug("bnode workers are closed");
}
