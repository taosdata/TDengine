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
#include "qmInt.h"

static void qmProcessQueue(SQnodeMgmt *pMgmt, SNodeMsg *pMsg) {
  dTrace("msg:%p, will be processed in qnode queue", pMsg);
  SRpcMsg *pRsp = NULL;
  SRpcMsg *pRpc = &pMsg->rpcMsg;
  int32_t  code = qndProcessMsg(pMgmt->pQnode, pRpc, &pRsp);

  if (pRpc->msgType & 1u) {
    if (pRsp != NULL) {
      pRsp->ahandle = pRpc->ahandle;
      dndSendRsp(pMgmt->pWrapper, pRsp);
      free(pRsp);
    } else {
      if (code != 0) code = terrno;
      SRpcMsg rpcRsp = {.handle = pRpc->handle, .ahandle = pRpc->ahandle, .code = code};
      dndSendRsp(pMgmt->pWrapper, &rpcRsp);
    }
  }

  dTrace("msg:%p, is freed", pMsg);
  rpcFreeCont(pRpc->pCont);
  taosFreeQitem(pMsg);
}

int32_t qmProcessQueryMsg(SQnodeMgmt *pMgmt, SNodeMsg *pMsg) {
  SDnodeWorker *pWorker = &pMgmt->queryWorker;

  dTrace("msg:%p, put into worker:%s", pMsg, pWorker->name);
  return dndWriteMsgToWorker(pWorker, pMsg);
}

int32_t qmProcessFetchMsg(SQnodeMgmt *pMgmt, SNodeMsg *pMsg) {
  SDnodeWorker *pWorker = &pMgmt->fetchWorker;

  dTrace("msg:%p, put into worker:%s", pMsg, pWorker->name);
  return dndWriteMsgToWorker(pWorker, pMsg);
}

int32_t qmStartWorker(SQnodeMgmt *pMgmt) {
  if (dndInitWorker(pMgmt, &pMgmt->queryWorker, DND_WORKER_SINGLE, "qnode-query", 0, 1, qmProcessQueue) != 0) {
    dError("failed to start qnode query worker since %s", terrstr());
    return -1;
  }

  if (dndInitWorker(pMgmt, &pMgmt->fetchWorker, DND_WORKER_SINGLE, "qnode-fetch", 0, 1, qmProcessQueue) != 0) {
    dError("failed to start qnode fetch worker since %s", terrstr());
    return -1;
  }

  return 0;
}

void qmStopWorker(SQnodeMgmt *pMgmt) {
  dndCleanupWorker(&pMgmt->queryWorker);
  dndCleanupWorker(&pMgmt->fetchWorker);
}
