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
#include "dndTransport.h"
#include "dndWorker.h"

#if 0
static int32_t mmProcessWriteMsg(SDnode *pDnode, SMndMsg *pMsg);
static int32_t mmProcessSyncMsg(SDnode *pDnode, SMndMsg *pMsg);
static int32_t mmProcessReadMsg(SDnode *pDnode, SMndMsg *pMsg);
static int32_t mmPutMndMsgToWorker(SDnode *pDnode, SDnodeWorker *pWorker, SMndMsg *pMsg);
static int32_t mmPutRpcMsgToWorker(SDnode *pDnode, SDnodeWorker *pWorker, SRpcMsg *pRpc);
static void    mmConsumeMsgQueue(SDnode *pDnode, SMndMsg *pMsg);

int32_t mmStartWorker(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  if (dndInitWorker(pDnode, &pMgmt->readWorker, DND_WORKER_SINGLE, "mnode-read", 0, 1, mmConsumeMsgQueue) != 0) {
    dError("failed to start mnode read worker since %s", terrstr());
    return -1;
  }

  if (dndInitWorker(pDnode, &pMgmt->writeWorker, DND_WORKER_SINGLE, "mnode-write", 0, 1, mmConsumeMsgQueue) != 0) {
    dError("failed to start mnode write worker since %s", terrstr());
    return -1;
  }

  if (dndInitWorker(pDnode, &pMgmt->syncWorker, DND_WORKER_SINGLE, "mnode-sync", 0, 1, mmConsumeMsgQueue) != 0) {
    dError("failed to start mnode sync worker since %s", terrstr());
    return -1;
  }

  return 0;
}

void mmStopWorker(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

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

void mmInitMsgFp(SMnodeMgmt *pMgmt) {
  
}

static void mmSendRpcRsp(SDnode *pDnode, SRpcMsg *pRpc) {
  if (pRpc->code == TSDB_CODE_DND_MNODE_NOT_DEPLOYED || pRpc->code == TSDB_CODE_APP_NOT_READY) {
    dmSendRedirectRsp(pDnode, pRpc);
  } else {
    rpcSendResponse(pRpc);
  }
}

static int32_t mmBuildMsg(SMndMsg *pMsg, SRpcMsg *pRpc) {
  SRpcConnInfo connInfo = {0};
  if ((pRpc->msgType & 1U) && rpcGetConnInfo(pRpc->handle, &connInfo) != 0) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    dError("failed to create msg since %s, app:%p RPC:%p", terrstr(), pRpc->ahandle, pRpc->handle);
    return -1;
  }

  memcpy(pMsg->user, connInfo.user, TSDB_USER_LEN);
  pMsg->rpcMsg = *pRpc;
  pMsg->createdTime = taosGetTimestampSec();

  return 0;
}

void mmProcessRpcMsg(SDnode *pDnode, SRpcMsg *pRpc, SEpSet *pEpSet) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  int32_t   code = -1;
  SMndMsg  *pMsg = NULL;

  MndMsgFp msgFp = pMgmt->msgFp[TMSG_INDEX(pRpc->msgType)];
  if (msgFp == NULL) {
    terrno = TSDB_CODE_MSG_NOT_PROCESSED;
    goto _OVER;
  }

  pMsg = taosAllocateQitem(sizeof(SMndMsg));
  if (pMsg == NULL) {
    goto _OVER;
  }

  if (mmBuildMsg(pMsg, pRpc) != 0) {
    goto _OVER;
  }

  dTrace("msg:%p, is created, app:%p RPC:%p user:%s", pMsg, pRpc->ahandle, pRpc->handle, pMsg->user);

  if (pMgmt->singleProc) {
    code = (*msgFp)(pDnode, pMsg);
  } else {
    code = taosProcPutToChildQueue(pMgmt->pProcess, pMsg, sizeof(SMndMsg), pRpc->pCont, pRpc->contLen);
  }

_OVER:

  if (code == 0) {
    if (!pMgmt->singleProc) {
      dTrace("msg:%p, is freed", pMsg);
      taosFreeQitem(pMsg);
      rpcFreeCont(pRpc->pCont);
    }
  } else {
    bool isReq = (pRpc->msgType & 1U);
    if (isReq) {
      SRpcMsg rsp = {.handle = pRpc->handle, .ahandle = pRpc->ahandle, .code = terrno};
      mmSendRpcRsp(pDnode, &rsp);
    }
    dTrace("msg:%p, is freed", pMsg);
    taosFreeQitem(pMsg);
    rpcFreeCont(pRpc->pCont);
  }
}

int32_t mmProcessWriteMsg(SDnode *pDnode, SMndMsg *pMsg) {
  return mmPutMndMsgToWorker(pDnode, &pDnode->mmgmt.writeWorker, pMsg);
}

int32_t mmProcessSyncMsg(SDnode *pDnode, SMndMsg *pMsg) {
  return mmPutMndMsgToWorker(pDnode, &pDnode->mmgmt.syncWorker, pMsg);
}

int32_t mmProcessReadMsg(SDnode *pDnode, SMndMsg *pMsg) {
  return mmPutMndMsgToWorker(pDnode, &pDnode->mmgmt.readWorker, pMsg);
}

int32_t mmPutMsgToWriteQueue(SDnode *pDnode, SRpcMsg *pRpc) {
  return mmPutRpcMsgToWorker(pDnode, &pDnode->mmgmt.writeWorker, pRpc);
}

int32_t mmPutMsgToReadQueue(SDnode *pDnode, SRpcMsg *pRpc) {
  return mmPutRpcMsgToWorker(pDnode, &pDnode->mmgmt.readWorker, pRpc);
}

static int32_t mmPutMndMsgToWorker(SDnode *pDnode, SDnodeWorker *pWorker, SMndMsg *pMsg) {
  SMnode *pMnode = mmAcquire(pDnode);
  if (pMnode == NULL) return -1;

  dTrace("msg:%p, put into worker %s", pMsg, pWorker->name);
  int32_t code = dndWriteMsgToWorker(pWorker, pMsg, 0);
  mmRelease(pDnode, pMnode);
  return code;
}

static int32_t mmPutRpcMsgToWorker(SDnode *pDnode, SDnodeWorker *pWorker, SRpcMsg *pRpc) {
  SMndMsg *pMsg = taosAllocateQitem(sizeof(SMndMsg));
  if (pMsg == NULL) {
    return -1;
  }

  dTrace("msg:%p, is created", pMsg);
  pMsg->rpcMsg = *pRpc;
  pMsg->createdTime = taosGetTimestampSec();

  int32_t code = mmPutMndMsgToWorker(pDnode, pWorker, pMsg);
  if (code != 0) {
    dTrace("msg:%p, is freed", pMsg);
    taosFreeQitem(pMsg);
    rpcFreeCont(pRpc->pCont);
  }

  return code;
}

void mmPutRpcRspToWorker(SDnode *pDnode, SRpcMsg *pRpc) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  int32_t   code = -1;

  if (pMgmt->singleProc) {
    mmSendRpcRsp(pDnode, pRpc);
  } else {
    do {
      code = taosProcPutToParentQueue(pMgmt->pProcess, pRpc, sizeof(SRpcMsg), pRpc->pCont, pRpc->contLen);
      if (code != 0) {
        taosMsleep(10);
      }
    } while (code != 0);
  }
}

void mmConsumeChildQueue(SDnode *pDnode, SMndMsg *pMsg, int32_t msgLen, void *pCont, int32_t contLen) {
  dTrace("msg:%p, get from child queue", pMsg);
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

  SRpcMsg *pRpc = &pMsg->rpcMsg;
  pRpc->pCont = pCont;

  MndMsgFp msgFp = pMgmt->msgFp[TMSG_INDEX(pRpc->msgType)];
  int32_t  code = (*msgFp)(pDnode, pMsg);

  if (code != 0) {
    bool isReq = (pRpc->msgType & 1U);
    if (isReq) {
      SRpcMsg rsp = {.handle = pRpc->handle, .ahandle = pRpc->ahandle, .code = terrno};
      mmPutRpcRspToWorker(pDnode, &rsp);
    }

    dTrace("msg:%p, is freed", pMsg);
    taosFreeQitem(pMsg);
    rpcFreeCont(pCont);
  }
}

void mmConsumeParentQueue(SDnode *pDnode, SRpcMsg *pMsg, int32_t msgLen, void *pCont, int32_t contLen) {
  dTrace("msg:%p, get from parent queue", pMsg);
  pMsg->pCont = pCont;
  mmSendRpcRsp(pDnode, pMsg);
  free(pMsg);
}

static void mmConsumeMsgQueue(SDnode *pDnode, SMndMsg *pMsg) {
  dTrace("msg:%p, get from msg queue", pMsg);
  SMnode  *pMnode = mmAcquire(pDnode);
  SRpcMsg *pRpc = &pMsg->rpcMsg;
  bool     isReq = (pRpc->msgType & 1U);
  int32_t  code = -1;

  if (pMnode != NULL) {
    pMsg->pMnode = pMnode;
    code = mndProcessMsg(pMsg);
    mmRelease(pDnode, pMnode);
  }

  if (isReq) {
    if (pMsg->rpcMsg.handle == NULL) return;
    if (code == 0) {
      SRpcMsg rsp = {.handle = pRpc->handle, .contLen = pMsg->contLen, .pCont = pMsg->pCont};
      mmPutRpcRspToWorker(pDnode, &rsp);
    } else {
      if (terrno != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
        SRpcMsg rsp = {.handle = pRpc->handle, .contLen = pMsg->contLen, .pCont = pMsg->pCont, .code = terrno};
        mmPutRpcRspToWorker(pDnode, &rsp);
      }
    }
  }

  dTrace("msg:%p, is freed", pMsg);
  rpcFreeCont(pRpc->pCont);
  taosFreeQitem(pMsg);
}

#endif

int32_t mmProcessWriteMsg( SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {return 0;}
int32_t mmProcessSyncMsg( SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {return 0;}
int32_t mmProcessReadMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  terrno = TSDB_CODE_MSG_NOT_PROCESSED;
  return -1;
}