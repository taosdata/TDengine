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
#include "smInt.h"

static inline void smSendRsp(SRpcMsg *pMsg, int32_t code) {
  SRpcMsg rsp = {
      .code = code,
      .pCont = pMsg->info.rsp,
      .contLen = pMsg->info.rspLen,
      .info = pMsg->info,
  };
  tmsgSendRsp(&rsp);
}

static void smProcessWriteQueue(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SSnodeMgmt *pMgmt = pInfo->ahandle;

  for (int32_t i = 0; i < numOfMsgs; i++) {
    SRpcMsg *pMsg = NULL;
    taosGetQitem(qall, (void **)&pMsg);
    const STraceId *trace = &pMsg->info.traceId;

    dTrace("msg:%p, get from snode-write queue", pMsg);
    int32_t code = sndProcessWriteMsg(pMgmt->pSnode, pMsg, NULL);
    if (code < 0) {
      dGError("snd, msg:%p failed to process write since %s", pMsg, terrstr(code));
      if (pMsg->info.handle != NULL) {
        tmsgSendRsp(pMsg);
      }
    } else {
      smSendRsp(pMsg, 0);
    }

    dTrace("msg:%p, is freed", pMsg);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
}

static void smProcessStreamQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SSnodeMgmt     *pMgmt = pInfo->ahandle;
  const STraceId *trace = &pMsg->info.traceId;

  dTrace("msg:%p, get from snode-stream queue", pMsg);
  int32_t code = sndProcessStreamMsg(pMgmt->pSnode, pMsg);
  if (code < 0) {
    dGError("snd, msg:%p failed to process stream msg %s since %s", pMsg, TMSG_INFO(pMsg->msgType), terrstr(code));
    smSendRsp(pMsg, terrno);
  }

  dTrace("msg:%p, is freed", pMsg);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

int32_t smStartWorker(SSnodeMgmt *pMgmt) {
  pMgmt->writeWroker = taosArrayInit(0, sizeof(SMultiWorker *));
  if (pMgmt->writeWroker == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < tsNumOfSnodeWriteThreads; i++) {
    SMultiWorker *pWriteWorker = taosMemoryMalloc(sizeof(SMultiWorker));
    if (pWriteWorker == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    SMultiWorkerCfg cfg = {
        .max = 1,
        .name = "snode-write",
        .fp = smProcessWriteQueue,
        .param = pMgmt,
    };
    if (tMultiWorkerInit(pWriteWorker, &cfg) != 0) {
      dError("failed to start snode-unique worker since %s", terrstr());
      return -1;
    }
    if (taosArrayPush(pMgmt->writeWroker, &pWriteWorker) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  SSingleWorkerCfg cfg = {
      .min = tsNumOfSnodeStreamThreads,
      .max = tsNumOfSnodeStreamThreads,
      .name = "snode-stream",
      .fp = (FItem)smProcessStreamQueue,
      .param = pMgmt,
  };

  if (tSingleWorkerInit(&pMgmt->streamWorker, &cfg)) {
    dError("failed to start snode shared-worker since %s", terrstr());
    return -1;
  }

  dDebug("snode workers are initialized");
  return 0;
}

void smStopWorker(SSnodeMgmt *pMgmt) {
  for (int32_t i = 0; i < taosArrayGetSize(pMgmt->writeWroker); i++) {
    SMultiWorker *pWorker = taosArrayGetP(pMgmt->writeWroker, i);
    tMultiWorkerCleanup(pWorker);
    taosMemoryFree(pWorker);
  }
  taosArrayDestroy(pMgmt->writeWroker);
  tSingleWorkerCleanup(&pMgmt->streamWorker);
  dDebug("snode workers are closed");
}

int32_t smPutMsgToQueue(SSnodeMgmt *pMgmt, EQueueType qtype, SRpcMsg *pRpc) {
  SRpcMsg *pMsg = taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM, pRpc->contLen);
  if (pMsg == NULL) {
    rpcFreeCont(pRpc->pCont);
    pRpc->pCont = NULL;
    return -1;
  }

  SSnode *pSnode = pMgmt->pSnode;
  if (pSnode == NULL) {
    dError("msg:%p failed to put into snode queue since %s, type:%s qtype:%d len:%d", pMsg, terrstr(),
           TMSG_INFO(pMsg->msgType), qtype, pRpc->contLen);
    taosFreeQitem(pMsg);
    rpcFreeCont(pRpc->pCont);
    pRpc->pCont = NULL;
    return -1;
  }

  SMsgHead *pHead = pRpc->pCont;
  pHead->contLen = htonl(pHead->contLen);
  pHead->vgId = SNODE_HANDLE;
  memcpy(pMsg, pRpc, sizeof(SRpcMsg));
  pRpc->pCont = NULL;

  switch (qtype) {
    case STREAM_QUEUE:
      smPutNodeMsgToStreamQueue(pMgmt, pMsg);
      break;
    case WRITE_QUEUE:
      smPutNodeMsgToWriteQueue(pMgmt, pMsg);
      break;
    default:
      terrno = TSDB_CODE_INVALID_PARA;
      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
      return -1;
  }
  return 0;
}

int32_t smPutNodeMsgToMgmtQueue(SSnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SMultiWorker *pWorker = taosArrayGetP(pMgmt->writeWroker, 0);
  if (pWorker == NULL) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  dTrace("msg:%p, put into worker %s", pMsg, pWorker->name);
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t smPutNodeMsgToWriteQueue(SSnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SMultiWorker *pWorker = taosArrayGetP(pMgmt->writeWroker, 0);
  if (pWorker == NULL) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  dTrace("msg:%p, put into worker %s", pMsg, pWorker->name);
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t smPutNodeMsgToStreamQueue(SSnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SSingleWorker *pWorker = &pMgmt->streamWorker;

  dTrace("msg:%p, put into worker %s", pMsg, pWorker->name);
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}
