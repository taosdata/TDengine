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


static void dndProcessSnodeSharedQueue(SDnode *pDnode, SRpcMsg *pMsg);

static void dndProcessSnodeUniqueQueue(SDnode *pDnode, STaosQall *qall, int32_t numOfMsgs);

static int32_t dndStartSnodeWorker(SDnode *pDnode) {
  SSnodeMgmt *pMgmt = &pDnode->smgmt;
  pMgmt->uniqueWorkers = taosArrayInit(0, sizeof(void *));
  for (int32_t i = 0; i < SND_UNIQUE_THREAD_NUM; i++) {
    SDnodeWorker *pUniqueWorker = malloc(sizeof(SDnodeWorker));
    if (pUniqueWorker == NULL) {
      return -1;
    }
    if (dndInitWorker(pDnode, pUniqueWorker, DND_WORKER_MULTI, "snode-unique", 1, 1, dndProcessSnodeSharedQueue) != 0) {
      dError("failed to start snode unique worker since %s", terrstr());
      return -1;
    }
    taosArrayPush(pMgmt->uniqueWorkers, &pUniqueWorker);
  }
  if (dndInitWorker(pDnode, &pMgmt->sharedWorker, DND_WORKER_SINGLE, "snode-shared", SND_SHARED_THREAD_NUM,
                    SND_SHARED_THREAD_NUM, dndProcessSnodeSharedQueue)) {
    dError("failed to start snode shared worker since %s", terrstr());
    return -1;
  }

  return 0;
}

static void dndStopSnodeWorker(SDnode *pDnode) {
  SSnodeMgmt *pMgmt = &pDnode->smgmt;

  taosWLockLatch(&pMgmt->latch);
  pMgmt->deployed = 0;
  taosWUnLockLatch(&pMgmt->latch);

  while (pMgmt->refCount > 0) {
    taosMsleep(10);
  }

  for (int32_t i = 0; i < taosArrayGetSize(pMgmt->uniqueWorkers); i++) {
    SDnodeWorker *worker = taosArrayGetP(pMgmt->uniqueWorkers, i);
    dndCleanupWorker(worker);
  }
  taosArrayDestroy(pMgmt->uniqueWorkers);
}

static void dndProcessSnodeUniqueQueue(SDnode *pDnode, STaosQall *qall, int32_t numOfMsgs) {
  /*SSnodeMgmt *pMgmt = &pDnode->smgmt;*/
  int32_t code = TSDB_CODE_NODE_NOT_DEPLOYED;

  SSnode *pSnode = dndAcquireSnode(pDnode);
  if (pSnode != NULL) {
    for (int32_t i = 0; i < numOfMsgs; i++) {
      SRpcMsg *pMsg = NULL;
      taosGetQitem(qall, (void **)&pMsg);

      sndProcessUMsg(pSnode, pMsg);

      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
    }
    dndReleaseSnode(pDnode, pSnode);
  } else {
    for (int32_t i = 0; i < numOfMsgs; i++) {
      SRpcMsg *pMsg = NULL;
      taosGetQitem(qall, (void **)&pMsg);
      SRpcMsg rpcRsp = {.handle = pMsg->handle, .ahandle = pMsg->ahandle, .code = code};
      rpcSendResponse(&rpcRsp);

      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
    }
  }
}

static void dndProcessSnodeSharedQueue(SDnode *pDnode, SRpcMsg *pMsg) {
  /*SSnodeMgmt *pMgmt = &pDnode->smgmt;*/
  int32_t code = TSDB_CODE_NODE_NOT_DEPLOYED;

  SSnode *pSnode = dndAcquireSnode(pDnode);
  if (pSnode != NULL) {
    sndProcessSMsg(pSnode, pMsg);
    dndReleaseSnode(pDnode, pSnode);
  } else {
    SRpcMsg rpcRsp = {.handle = pMsg->handle, .ahandle = pMsg->ahandle, .code = code};
    rpcSendResponse(&rpcRsp);
  }

#if 0
  if (pMsg->msgType & 1u) {
    if (pRsp != NULL) {
      pRsp->ahandle = pMsg->ahandle;
      rpcSendResponse(pRsp);
      free(pRsp);
    } else {
      if (code != 0) code = terrno;
      SRpcMsg rpcRsp = {.handle = pMsg->handle, .ahandle = pMsg->ahandle, .code = code};
      rpcSendResponse(&rpcRsp);
    }
  }
#endif

  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static FORCE_INLINE int32_t dndGetSWIdFromMsg(SRpcMsg *pMsg) {
  SMsgHead *pHead = pMsg->pCont;
  pHead->streamTaskId = htonl(pHead->streamTaskId);
  return pHead->streamTaskId % SND_UNIQUE_THREAD_NUM;
}

static void dndWriteSnodeMsgToWorkerByMsg(SDnode *pDnode, SRpcMsg *pMsg) {
  int32_t code = TSDB_CODE_NODE_NOT_DEPLOYED;

  SSnode *pSnode = dndAcquireSnode(pDnode);
  if (pSnode != NULL) {
    int32_t       index = dndGetSWIdFromMsg(pMsg);
    SDnodeWorker *pWorker = taosArrayGetP(pDnode->smgmt.uniqueWorkers, index);
    code = dndWriteMsgToWorker(pWorker, pMsg, sizeof(SRpcMsg));
  }

  dndReleaseSnode(pDnode, pSnode);

  if (code != 0) {
    if (pMsg->msgType & 1u) {
      SRpcMsg rsp = {.handle = pMsg->handle, .ahandle = pMsg->ahandle, .code = code};
      rpcSendResponse(&rsp);
    }
    rpcFreeCont(pMsg->pCont);
  }
}

static void dndWriteSnodeMsgToMgmtWorker(SDnode *pDnode, SRpcMsg *pMsg) {
  int32_t code = TSDB_CODE_NODE_NOT_DEPLOYED;

  SSnode *pSnode = dndAcquireSnode(pDnode);
  if (pSnode != NULL) {
    SDnodeWorker *pWorker = taosArrayGet(pDnode->smgmt.uniqueWorkers, 0);
    code = dndWriteMsgToWorker(pWorker, pMsg, sizeof(SRpcMsg));
  }
  dndReleaseSnode(pDnode, pSnode);

  if (code != 0) {
    if (pMsg->msgType & 1u) {
      SRpcMsg rsp = {.handle = pMsg->handle, .ahandle = pMsg->ahandle, .code = code};
      rpcSendResponse(&rsp);
    }
    rpcFreeCont(pMsg->pCont);
  }
}

static void dndWriteSnodeMsgToWorker(SDnode *pDnode, SDnodeWorker *pWorker, SRpcMsg *pMsg) {
  int32_t code = TSDB_CODE_NODE_NOT_DEPLOYED;

  SSnode *pSnode = dndAcquireSnode(pDnode);
  if (pSnode != NULL) {
    code = dndWriteMsgToWorker(pWorker, pMsg, sizeof(SRpcMsg));
  }
  dndReleaseSnode(pDnode, pSnode);

  if (code != 0) {
    if (pMsg->msgType & 1u) {
      SRpcMsg rsp = {.handle = pMsg->handle, .ahandle = pMsg->ahandle, .code = code};
      rpcSendResponse(&rsp);
    }
    rpcFreeCont(pMsg->pCont);
  }
}

void dndProcessSnodeMgmtMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  dndWriteSnodeMsgToMgmtWorker(pDnode, pMsg);
}

void dndProcessSnodeUniqueMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  dndWriteSnodeMsgToWorkerByMsg(pDnode, pMsg);
}

void dndProcessSnodeSharedMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  dndWriteSnodeMsgToWorker(pDnode, &pDnode->smgmt.sharedWorker, pMsg);
}
