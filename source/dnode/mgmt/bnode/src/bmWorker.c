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
// #include "dndBnode.h"
// #include "dndTransport.h"
// #include "dndWorker.h"

#if 0
static void dndProcessBnodeQueue(SDnode *pDnode, STaosQall *qall, int32_t numOfMsgs);


static int32_t bmStartWorker(SDnode *pDnode) {
  SBnodeMgmt *pMgmt = &pDnode->bmgmt;
  if (dndInitWorker(pDnode, &pMgmt->writeWorker, DND_WORKER_MULTI, "bnode-write", 0, 1, dndProcessBnodeQueue) != 0) {
    dError("failed to start bnode write worker since %s", terrstr());
    return -1;
  }

  return 0;
}

static void bmStopWorker(SDnode *pDnode) {
  SBnodeMgmt *pMgmt = &pDnode->bmgmt;

  taosWLockLatch(&pMgmt->latch);
  pMgmt->deployed = 0;
  taosWUnLockLatch(&pMgmt->latch);

  while (pMgmt->refCount > 0) {
    taosMsleep(10);
  }

  dndCleanupWorker(&pMgmt->writeWorker);
}

static void dndSendBnodeErrorRsp(SRpcMsg *pMsg, int32_t code) {
  SRpcMsg rpcRsp = {.handle = pMsg->handle, .ahandle = pMsg->ahandle, .code = code};
  rpcSendResponse(&rpcRsp);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void dndSendBnodeErrorRsps(STaosQall *qall, int32_t numOfMsgs, int32_t code) {
  for (int32_t i = 0; i < numOfMsgs; ++i) {
    SRpcMsg *pMsg = NULL;
    taosGetQitem(qall, (void **)&pMsg);
    dndSendBnodeErrorRsp(pMsg, code);
  }
}

static void dndProcessBnodeQueue(SDnode *pDnode, STaosQall *qall, int32_t numOfMsgs) {
  SBnode *pBnode = bmAcquire(pDnode);
  if (pBnode == NULL) {
    dndSendBnodeErrorRsps(qall, numOfMsgs, TSDB_CODE_OUT_OF_MEMORY);
    return;
  }

  SArray *pArray = taosArrayInit(numOfMsgs, sizeof(SRpcMsg *));
  if (pArray == NULL) {
    bmRelease(pDnode, pBnode);
    dndSendBnodeErrorRsps(qall, numOfMsgs, TSDB_CODE_OUT_OF_MEMORY);
    return;
  }

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    SRpcMsg *pMsg = NULL;
    taosGetQitem(qall, (void **)&pMsg);
    void *ptr = taosArrayPush(pArray, &pMsg);
    if (ptr == NULL) {
      dndSendBnodeErrorRsp(pMsg, TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  bndProcessWMsgs(pBnode, pArray);

  for (size_t i = 0; i < numOfMsgs; i++) {
    SRpcMsg *pMsg = *(SRpcMsg **)taosArrayGet(pArray, i);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
  taosArrayDestroy(pArray);
  bmRelease(pDnode, pBnode);
}

static void dndWriteBnodeMsgToWorker(SDnode *pDnode, SDnodeWorker *pWorker, SRpcMsg *pMsg) {
  int32_t code = TSDB_CODE_DND_BNODE_NOT_DEPLOYED;

  SBnode *pBnode = bmAcquire(pDnode);
  if (pBnode != NULL) {
    code = dndWriteMsgToWorker(pWorker, pMsg, sizeof(SRpcMsg));
  }
  bmRelease(pDnode, pBnode);

  if (code != 0) {
    if (pMsg->msgType & 1u) {
      SRpcMsg rsp = {.handle = pMsg->handle, .ahandle = pMsg->ahandle, .code = code};
      rpcSendResponse(&rsp);
    }
    rpcFreeCont(pMsg->pCont);
  }
}

void dndProcessBnodeWriteMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  dndWriteBnodeMsgToWorker(pDnode, &pDnode->bmgmt.writeWorker, pMsg);
}

int32_t dndInitBnode(SDnode *pDnode) {
  SBnodeMgmt *pMgmt = &pDnode->bmgmt;
  taosInitRWLatch(&pMgmt->latch);

  if (dndReadBnodeFile(pDnode) != 0) {
    return -1;
  }

  if (pMgmt->dropped) {
    dInfo("bnode has been deployed and needs to be deleted");
    bndDestroy(pDnode->dir.bnode);
    return 0;
  }

  if (!pMgmt->deployed) return 0;

  return bmOpen(pDnode);
}

void dndCleanupBnode(SDnode *pDnode) {
  SBnodeMgmt *pMgmt = &pDnode->bmgmt;
  if (pMgmt->pBnode) {
    bmStopWorker(pDnode);
    bndClose(pMgmt->pBnode);
    pMgmt->pBnode = NULL;
  }
}

#endif