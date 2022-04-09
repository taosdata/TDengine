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

  dTrace("msg:%p, get from mnode queue", pMsg);
  SRpcMsg *pRpc = &pMsg->rpcMsg;
  int32_t  code = -1;

  if (pMsg->rpcMsg.msgType == TDMT_DND_ALTER_MNODE) {
    code = mmProcessAlterReq(pMgmt, pMsg);
  } else if (pMsg->rpcMsg.msgType == TDMT_MON_MM_INFO) {
    code = mmProcessGetMonMmInfoReq(pMgmt->pWrapper, pMsg);
  } else {
    pMsg->pNode = pMgmt->pMnode;
    code = mndProcessMsg(pMsg);
  }

  if (pRpc->msgType & 1U) {
    if (pRpc->handle != NULL && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
      if (code != 0 && terrno != 0) code = terrno;
      SRpcMsg rsp = {.handle = pRpc->handle, .code = code, .contLen = pMsg->rspLen, .pCont = pMsg->pRsp};
      tmsgSendRsp(&rsp);
    }
  }

  dTrace("msg:%p, is freed, result:0x%04x:%s", pMsg, code & 0XFFFF, tstrerror(code));
  rpcFreeCont(pRpc->pCont);
  taosFreeQitem(pMsg);
}

static void mmProcessQueryQueue(SQueueInfo *pInfo, SNodeMsg *pMsg) {
  SMnodeMgmt *pMgmt = pInfo->ahandle;

  dTrace("msg:%p, get from mnode query queue", pMsg);
  SRpcMsg *pRpc = &pMsg->rpcMsg;
  int32_t  code = -1;

  pMsg->pNode = pMgmt->pMnode;
  code = mndProcessMsg(pMsg);

  if (pRpc->msgType & 1U) {
    if (pRpc->handle != NULL && code != 0) {
      dError("msg:%p, failed to process since %s", pMsg, terrstr());
      SRpcMsg rsp = {.handle = pRpc->handle, .code = code, .ahandle = pRpc->ahandle};
      tmsgSendRsp(&rsp);
    }
  }

  dTrace("msg:%p, is freed, result:0x%04x:%s", pMsg, code & 0XFFFF, tstrerror(code));
  rpcFreeCont(pRpc->pCont);
  taosFreeQitem(pMsg);
}

static void mmPutMsgToWorker(SSingleWorker *pWorker, SNodeMsg *pMsg) {
  dTrace("msg:%p, put into worker %s", pMsg, pWorker->name);
  taosWriteQitem(pWorker->queue, pMsg);
}

int32_t mmProcessWriteMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SMnodeMgmt *pMgmt = pWrapper->pMgmt;
  mmPutMsgToWorker(&pMgmt->writeWorker, pMsg);
  return 0;
}

int32_t mmProcessSyncMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SMnodeMgmt *pMgmt = pWrapper->pMgmt;
  mmPutMsgToWorker(&pMgmt->syncWorker, pMsg);
  return 0;
}

int32_t mmProcessReadMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SMnodeMgmt *pMgmt = pWrapper->pMgmt;
  mmPutMsgToWorker(&pMgmt->readWorker, pMsg);
  return 0;
}

int32_t mmProcessQueryMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SMnodeMgmt *pMgmt = pWrapper->pMgmt;
  mmPutMsgToWorker(&pMgmt->queryWorker, pMsg);
  return 0;
}

int32_t mmProcessMonitorMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SMnodeMgmt    *pMgmt = pWrapper->pMgmt;
  SSingleWorker *pWorker = &pMgmt->monitorWorker;

  dTrace("msg:%p, put into worker:%s", pMsg, pWorker->name);
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

static int32_t mmPutRpcMsgToWorker(SSingleWorker *pWorker, SRpcMsg *pRpc) {
  SNodeMsg *pMsg = taosAllocateQitem(sizeof(SNodeMsg));
  if (pMsg == NULL) return -1;

  dTrace("msg:%p, is created and put into worker:%s, type:%s", pMsg, pWorker->name, TMSG_INFO(pRpc->msgType));
  pMsg->rpcMsg = *pRpc;
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t mmPutMsgToQueryQueue(SMgmtWrapper *pWrapper, SRpcMsg *pRpc) {
  SMnodeMgmt *pMgmt = pWrapper->pMgmt;
  return mmPutRpcMsgToWorker(&pMgmt->queryWorker, pRpc);
}

int32_t mmPutMsgToWriteQueue(SMgmtWrapper *pWrapper, SRpcMsg *pRpc) {
  SMnodeMgmt *pMgmt = pWrapper->pMgmt;
  return mmPutRpcMsgToWorker(&pMgmt->writeWorker, pRpc);
}

int32_t mmPutMsgToReadQueue(SMgmtWrapper *pWrapper, SRpcMsg *pRpc) {
  SMnodeMgmt *pMgmt = pWrapper->pMgmt;
  return mmPutRpcMsgToWorker(&pMgmt->readWorker, pRpc);
}

int32_t mmPutMsgToSyncQueue(SMgmtWrapper *pWrapper, SRpcMsg *pRpc) {
  SMnodeMgmt *pMgmt = pWrapper->pMgmt;
  return mmPutRpcMsgToWorker(&pMgmt->syncWorker, pRpc);
}

int32_t mmStartWorker(SMnodeMgmt *pMgmt) {
  SSingleWorkerCfg qCfg = {.min = tsNumOfMnodeQueryThreads,
                           .max = tsNumOfMnodeQueryThreads,
                           .name = "mnode-query",
                           .fp = (FItem)mmProcessQueryQueue,
                           .param = pMgmt};
  if (tSingleWorkerInit(&pMgmt->queryWorker, &qCfg) != 0) {
    dError("failed to start mnode-query worker since %s", terrstr());
    return -1;
  }

  SSingleWorkerCfg rCfg = {.min = tsNumOfMnodeReadThreads,
                           .max = tsNumOfMnodeReadThreads,
                           .name = "mnode-read",
                           .fp = (FItem)mmProcessQueue,
                           .param = pMgmt};
  if (tSingleWorkerInit(&pMgmt->readWorker, &rCfg) != 0) {
    dError("failed to start mnode-read worker since %s", terrstr());
    return -1;
  }

  SSingleWorkerCfg wCfg = {.min = 1, .max = 1, .name = "mnode-write", .fp = (FItem)mmProcessQueue, .param = pMgmt};
  if (tSingleWorkerInit(&pMgmt->writeWorker, &wCfg) != 0) {
    dError("failed to start mnode-write worker since %s", terrstr());
    return -1;
  }

  SSingleWorkerCfg sCfg = {.min = 1, .max = 1, .name = "mnode-sync", .fp = (FItem)mmProcessQueue, .param = pMgmt};
  if (tSingleWorkerInit(&pMgmt->syncWorker, &sCfg) != 0) {
    dError("failed to start mnode mnode-sync worker since %s", terrstr());
    return -1;
  }

  if (tsMultiProcess) {
    SSingleWorkerCfg mCfg = {.min = 1, .max = 1, .name = "mnode-monitor", .fp = (FItem)mmProcessQueue, .param = pMgmt};
    if (tSingleWorkerInit(&pMgmt->monitorWorker, &mCfg) != 0) {
      dError("failed to start mnode mnode-monitor worker since %s", terrstr());
      return -1;
    }
  }

  dDebug("mnode workers are initialized");
  return 0;
}

void mmStopWorker(SMnodeMgmt *pMgmt) {
  tSingleWorkerCleanup(&pMgmt->monitorWorker);
  tSingleWorkerCleanup(&pMgmt->queryWorker);
  tSingleWorkerCleanup(&pMgmt->readWorker);
  tSingleWorkerCleanup(&pMgmt->writeWorker);
  tSingleWorkerCleanup(&pMgmt->syncWorker);
  dDebug("mnode workers are closed");
}
