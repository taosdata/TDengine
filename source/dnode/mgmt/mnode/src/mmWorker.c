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

static void mmProcessQueue(SQueueInfo *pInfo, SNodeMsg *pMsg) {
  SMnodeMgmt *pMgmt = pInfo->ahandle;

  dTrace("msg:%p, will be processed in mnode queue", pMsg);
  SRpcMsg *pRpc = &pMsg->rpcMsg;
  int32_t  code = -1;

  if (pMsg->rpcMsg.msgType != TDMT_DND_ALTER_MNODE) {
    pMsg->pNode = pMgmt->pMnode;
    code = mndProcessMsg(pMsg);
  } else {
    code = mmProcessAlterReq(pMgmt, pMsg);
  }

  if (pRpc->msgType & 1U) {
    if (pRpc->handle == NULL) return;
    if (code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
      if (code != 0) code = terrno;
      SRpcMsg rsp = {.handle = pRpc->handle, .code = code, .contLen = pMsg->rspLen, .pCont = pMsg->pRsp};
      dndSendRsp(pMgmt->pWrapper, &rsp);
    }
  }

  dTrace("msg:%p, is freed, result:0x%04x:%s", pMsg, code & 0XFFFF, tstrerror(code));
  rpcFreeCont(pRpc->pCont);
  taosFreeQitem(pMsg);
}

static int32_t mmPutMsgToWorker(SMnodeMgmt *pMgmt, SSingleWorker *pWorker, SNodeMsg *pMsg) {
  dTrace("msg:%p, put into worker %s", pMsg, pWorker->name);
  return taosWriteQitem(pWorker->queue, pMsg);
}

int32_t mmProcessWriteMsg(SMnodeMgmt *pMgmt, SNodeMsg *pMsg) {
  return mmPutMsgToWorker(pMgmt, &pMgmt->writeWorker, pMsg);
}

int32_t mmProcessSyncMsg(SMnodeMgmt *pMgmt, SNodeMsg *pMsg) {
  return mmPutMsgToWorker(pMgmt, &pMgmt->syncWorker, pMsg);
}

int32_t mmProcessReadMsg(SMnodeMgmt *pMgmt, SNodeMsg *pMsg) {
  return mmPutMsgToWorker(pMgmt, &pMgmt->readWorker, pMsg);
}

static int32_t mmPutRpcMsgToWorker(SMnodeMgmt *pMgmt, SSingleWorker *pWorker, SRpcMsg *pRpc) {
  SNodeMsg *pMsg = taosAllocateQitem(sizeof(SNodeMsg));
  if (pMsg == NULL) {
    return -1;
  }

  dTrace("msg:%p, is created and put into worker:%s, type:%s", pMsg, pWorker->name, TMSG_INFO(pRpc->msgType));
  pMsg->rpcMsg = *pRpc;

  int32_t code = taosWriteQitem(pWorker->queue, pMsg);
  if (code != 0) {
    dTrace("msg:%p, is freed", pMsg);
    taosFreeQitem(pMsg);
    rpcFreeCont(pRpc->pCont);
  }

  return code;
}

int32_t mmPutMsgToWriteQueue(SMgmtWrapper *pWrapper, SRpcMsg *pRpc) {
  SMnodeMgmt *pMgmt = pWrapper->pMgmt;
  return mmPutRpcMsgToWorker(pMgmt, &pMgmt->writeWorker, pRpc);
}

int32_t mmPutMsgToReadQueue(SMgmtWrapper *pWrapper, SRpcMsg *pRpc) {
  SMnodeMgmt *pMgmt = pWrapper->pMgmt;
  return mmPutRpcMsgToWorker(pMgmt, &pMgmt->readWorker, pRpc);
}

int32_t mmStartWorker(SMnodeMgmt *pMgmt) {
  SSingleWorkerCfg cfg = {.minNum = 0, .maxNum = 1, .name = "mnode-read", .fp = (FItem)mmProcessQueue, .param = pMgmt};

  if (tSingleWorkerInit(&pMgmt->readWorker, &cfg) != 0) {
    dError("failed to start mnode-read worker since %s", terrstr());
    return -1;
  }

  if (tSingleWorkerInit(&pMgmt->writeWorker, &cfg) != 0) {
    dError("failed to start mnode-write worker since %s", terrstr());
    return -1;
  }

  if (tSingleWorkerInit(&pMgmt->syncWorker, &cfg) != 0) {
    dError("failed to start mnode sync-worker since %s", terrstr());
    return -1;
  }

  dDebug("mnode workers are initialized");
  return 0;
}

void mmStopWorker(SMnodeMgmt *pMgmt) {
  tSingleWorkerCleanup(&pMgmt->readWorker);
  tSingleWorkerCleanup(&pMgmt->writeWorker);
  tSingleWorkerCleanup(&pMgmt->syncWorker);
  dDebug("mnode workers are closed");
}
