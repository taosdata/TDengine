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
  (void)tmsgSendRsp(&rsp);
}

static void smProcessWriteQueue(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SSnodeMgmt *pMgmt = pInfo->ahandle;

  for (int32_t i = 0; i < numOfMsgs; i++) {
    SRpcMsg *pMsg = NULL;
    (void)taosGetQitem(qall, (void **)&pMsg);
    const STraceId *trace = &pMsg->info.traceId;

    dTrace("msg:%p, get from snode-write queue", pMsg);
    int32_t code = sndProcessWriteMsg(pMgmt->pSnode, pMsg, NULL);
    if (code < 0) {
      dGError("snd, msg:%p failed to process write since %s", pMsg, tstrerror(code));
      smSendRsp(pMsg, code);
      // if (pMsg->info.handle != NULL) {
      //   tmsgSendRsp(pMsg);
      // }
    } else {
      // smSendRsp(pMsg, 0);
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
    dGError("snd, msg:%p failed to process stream msg %s since %s", pMsg, TMSG_INFO(pMsg->msgType), tstrerror(code));
    smSendRsp(pMsg, terrno);
  }

  dTrace("msg:%p, is freed", pMsg);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

int32_t smStartWorker(SSnodeMgmt *pMgmt) {
  int32_t code = 0;
  pMgmt->writeWroker = taosArrayInit(0, sizeof(SMultiWorker *));
  if (pMgmt->writeWroker == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    return code;
  }

  for (int32_t i = 0; i < tsNumOfSnodeWriteThreads; i++) {
    SMultiWorker *pWriteWorker = taosMemoryMalloc(sizeof(SMultiWorker));
    if (pWriteWorker == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      return code;
    }

    SMultiWorkerCfg cfg = {
        .max = 1,
        .name = "snode-write",
        .fp = smProcessWriteQueue,
        .param = pMgmt,
    };
    if ((code = tMultiWorkerInit(pWriteWorker, &cfg)) != 0) {
      dError("failed to start snode-unique worker since %s", tstrerror(code));
      return code;
    }
    if (taosArrayPush(pMgmt->writeWroker, &pWriteWorker) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      return code;
    }
  }

  SSingleWorkerCfg cfg = {
      .min = tsNumOfSnodeStreamThreads,
      .max = tsNumOfSnodeStreamThreads,
      .name = "snode-stream",
      .fp = (FItem)smProcessStreamQueue,
      .param = pMgmt,
  };

  if ((code = tSingleWorkerInit(&pMgmt->streamWorker, &cfg)) != 0) {
    dError("failed to start snode shared-worker since %s", tstrerror(code));
    return code;
  }

  dDebug("snode workers are initialized");
  return code;
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
  int32_t  code;
  SRpcMsg *pMsg;

  code = taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM, pRpc->contLen, (void **)&pMsg);
  if (code) {
    rpcFreeCont(pRpc->pCont);
    pRpc->pCont = NULL;
    return code = terrno;
  }

  SSnode *pSnode = pMgmt->pSnode;
  if (pSnode == NULL) {
    code = terrno;
    dError("msg:%p failed to put into snode queue since %s, type:%s qtype:%d len:%d", pMsg, tstrerror(code),
           TMSG_INFO(pMsg->msgType), qtype, pRpc->contLen);
    taosFreeQitem(pMsg);
    rpcFreeCont(pRpc->pCont);
    pRpc->pCont = NULL;
    return code;
  }

  SMsgHead *pHead = pRpc->pCont;
  pHead->contLen = htonl(pHead->contLen);
  pHead->vgId = SNODE_HANDLE;
  memcpy(pMsg, pRpc, sizeof(SRpcMsg));
  pRpc->pCont = NULL;

  switch (qtype) {
    case STREAM_QUEUE:
      code = smPutNodeMsgToStreamQueue(pMgmt, pMsg);
      break;
    case WRITE_QUEUE:
      code = smPutNodeMsgToWriteQueue(pMgmt, pMsg);
      break;
    default:
      code = TSDB_CODE_INVALID_PARA;
      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
      return code;
  }
  return code;
}

int32_t smPutNodeMsgToMgmtQueue(SSnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  int32_t       code = 0;
  SMultiWorker *pWorker = taosArrayGetP(pMgmt->writeWroker, 0);
  if (pWorker == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }

  dTrace("msg:%p, put into worker %s", pMsg, pWorker->name);
  return taosWriteQitem(pWorker->queue, pMsg);
}

int32_t smPutNodeMsgToWriteQueue(SSnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SMultiWorker *pWorker = taosArrayGetP(pMgmt->writeWroker, 0);
  if (pWorker == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }

  dTrace("msg:%p, put into worker %s", pMsg, pWorker->name);
  return taosWriteQitem(pWorker->queue, pMsg);
}

int32_t smPutNodeMsgToStreamQueue(SSnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SSingleWorker *pWorker = &pMgmt->streamWorker;

  dTrace("msg:%p, put into worker %s", pMsg, pWorker->name);
  return taosWriteQitem(pWorker->queue, pMsg);
}
