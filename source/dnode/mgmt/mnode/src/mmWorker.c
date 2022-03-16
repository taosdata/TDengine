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

#include "dmInt.h"

static void mmProcessQueue(SMnodeMgmt *pMgmt, SNodeMsg *pMsg) {
  dTrace("msg:%p, will be processed", pMsg);
  SMnode  *pMnode = mmAcquire(pMgmt);
  SRpcMsg *pRpc = &pMsg->rpcMsg;
  bool     isReq = (pRpc->msgType & 1U);
  int32_t  code = -1;

  if (pMnode != NULL) {
    pMsg->pNode = pMnode;
    code = mndProcessMsg(pMsg);
    mmRelease(pMgmt, pMnode);
  }

  if (isReq) {
    if (pMsg->rpcMsg.handle == NULL) return;
    if (code == 0) {
      SRpcMsg rsp = {.handle = pRpc->handle, .contLen = pMsg->rspLen, .pCont = pMsg->pRsp};
      dndSendRsp(pMgmt->pWrapper, &rsp);
    } else {
      if (terrno != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
        SRpcMsg rsp = {.handle = pRpc->handle, .contLen = pMsg->rspLen, .pCont = pMsg->pRsp, .code = terrno};
        dndSendRsp(pMgmt->pWrapper, &rsp);
      }
    }
  }

  dTrace("msg:%p, is freed", pMsg);
  rpcFreeCont(pRpc->pCont);
  taosFreeQitem(pMsg);
}

int32_t mmStartWorker(SMnodeMgmt *pMgmt) {
  if (dndInitWorker(pMgmt, &pMgmt->readWorker, DND_WORKER_SINGLE, "mnode-read", 0, 1, mmProcessQueue) != 0) {
    dError("failed to start mnode read worker since %s", terrstr());
    return -1;
  }

  if (dndInitWorker(pMgmt, &pMgmt->writeWorker, DND_WORKER_SINGLE, "mnode-write", 0, 1, mmProcessQueue) != 0) {
    dError("failed to start mnode write worker since %s", terrstr());
    return -1;
  }

  if (dndInitWorker(pMgmt, &pMgmt->syncWorker, DND_WORKER_SINGLE, "mnode-sync", 0, 1, mmProcessQueue) != 0) {
    dError("failed to start mnode sync worker since %s", terrstr());
    return -1;
  }

  return 0;
}

void mmStopWorker(SMnodeMgmt *pMgmt) {
  taosWLockLatch(&pMgmt->latch);
  pMgmt->deployed = 0;
  taosWUnLockLatch(&pMgmt->latch);

  while (pMgmt->refCount > 1) {
    taosMsleep(10);
  }

  dndCleanupWorker(&pMgmt->readWorker);
  dndCleanupWorker(&pMgmt->writeWorker);
  dndCleanupWorker(&pMgmt->syncWorker);
}

static int32_t mmPutMsgToWorker(SMnodeMgmt *pMgmt, SDnodeWorker *pWorker, SNodeMsg *pMsg) {
  SMnode *pMnode = mmAcquire(pMgmt);
  if (pMnode == NULL) return -1;

  dTrace("msg:%p, put into worker %s", pMsg, pWorker->name);
  int32_t code = dndWriteMsgToWorker(pWorker, pMsg, 0);
  mmRelease(pMgmt, pMnode);
  return code;
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

static int32_t mmPutRpcMsgToWorker(SMgmtWrapper *pWrapper, SDnodeWorker *pWorker, SRpcMsg *pRpc) {
  SNodeMsg *pMsg = taosAllocateQitem(sizeof(SNodeMsg));
  if (pMsg == NULL) {
    return -1;
  }

  dTrace("msg:%p, is created", pMsg);
  pMsg->rpcMsg = *pRpc;

  int32_t code = mmPutMsgToWorker(pWrapper->pMgmt, pWorker, pMsg);
  if (code != 0) {
    dTrace("msg:%p, is freed", pMsg);
    taosFreeQitem(pMsg);
    rpcFreeCont(pRpc->pCont);
  }

  return code;
}

int32_t mmPutMsgToWriteQueue(void *wrapper, SRpcMsg *pRpc) {
  // SMgmtWrapper *pWrapper = dndGetWrapper(pDnode, MNODE);
  // SMnodeMgmt   *pMgmt = pWrapper->pMgmt;
  // return mmPutRpcMsgToWorker(pWrapper, &pMgmt->writeWorker, pRpc);
  return 0;
}

int32_t mmPutMsgToReadQueue(void *wrapper, SRpcMsg *pRpc) {
  // SMgmtWrapper *pWrapper = dndGetWrapper(pDnode, MNODE);
  // SMnodeMgmt   *pMgmt = pWrapper->pMgmt;
  // return mmPutRpcMsgToWorker(pWrapper, &pMgmt->readWorker, pRpc);
  return 0;
}
