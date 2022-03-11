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
#include "mm.h"

#include "dndMgmt.h"
#include "dndTransport.h"
#include "dndWorker.h"

static int32_t mmProcessWriteMsg(SDnode *pDnode, SMndMsg *pMsg);
static int32_t mmProcessSyncMsg(SDnode *pDnode, SMndMsg *pMsg);
static int32_t mmProcessReadMsg(SDnode *pDnode, SMndMsg *pMsg);
static int32_t mmPutMndMsgToWorker(SDnode *pDnode, SDnodeWorker *pWorker, SMndMsg *pMsg);
static int32_t mmPutRpcMsgToWorker(SDnode *pDnode, SDnodeWorker *pWorker, SRpcMsg *pRpc);
static void    mmConsumeQueue(SDnode *pDnode, SMndMsg *pMsg);

int32_t mmStartWorker(SDnode *pDnode) {
  SMndMgmt *pMgmt = &pDnode->mmgmt;
  if (dndInitWorker(pDnode, &pMgmt->readWorker, DND_WORKER_SINGLE, "mnode-read", 0, 1, mmConsumeQueue) != 0) {
    dError("failed to start mnode read worker since %s", terrstr());
    return -1;
  }

  if (dndInitWorker(pDnode, &pMgmt->writeWorker, DND_WORKER_SINGLE, "mnode-write", 0, 1, mmConsumeQueue) != 0) {
    dError("failed to start mnode write worker since %s", terrstr());
    return -1;
  }

  if (dndInitWorker(pDnode, &pMgmt->syncWorker, DND_WORKER_SINGLE, "mnode-sync", 0, 1, mmConsumeQueue) != 0) {
    dError("failed to start mnode sync worker since %s", terrstr());
    return -1;
  }

  return 0;
}

void mmStopWorker(SDnode *pDnode) {
  SMndMgmt *pMgmt = &pDnode->mmgmt;

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

void mmInitMsgFp(SMndMgmt *pMgmt) {
  // Requests handled by DNODE
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_CREATE_MNODE_RSP)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_ALTER_MNODE_RSP)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_DROP_MNODE_RSP)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_CREATE_QNODE_RSP)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_DROP_QNODE_RSP)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_CREATE_SNODE_RSP)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_DROP_SNODE_RSP)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_CREATE_BNODE_RSP)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_DROP_BNODE_RSP)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_CREATE_VNODE_RSP)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_ALTER_VNODE_RSP)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_DROP_VNODE_RSP)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_SYNC_VNODE_RSP)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_COMPACT_VNODE_RSP)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_DND_CONFIG_DNODE_RSP)] = mmProcessWriteMsg;

  // Requests handled by MNODE
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CONNECT)] = mmProcessReadMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_ACCT)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_ALTER_ACCT)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_ACCT)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_USER)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_ALTER_USER)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_USER)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_GET_USER_AUTH)] = mmProcessReadMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_DNODE)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CONFIG_DNODE)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_DNODE)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_MNODE)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_MNODE)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_QNODE)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_QNODE)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_SNODE)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_SNODE)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_BNODE)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_BNODE)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_DB)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_DB)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_USE_DB)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_ALTER_DB)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_SYNC_DB)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_COMPACT_DB)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_FUNC)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_RETRIEVE_FUNC)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_FUNC)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_STB)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_ALTER_STB)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_STB)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_TABLE_META)] = mmProcessReadMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_VGROUP_LIST)] = mmProcessReadMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_KILL_QUERY)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_KILL_CONN)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_HEARTBEAT)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_SHOW)] = mmProcessReadMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_SHOW_RETRIEVE)] = mmProcessReadMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_STATUS)] = mmProcessReadMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_KILL_TRANS)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_GRANT)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_AUTH)] = mmProcessReadMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_CREATE_TOPIC)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_ALTER_TOPIC)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_DROP_TOPIC)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_SUBSCRIBE)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_MQ_COMMIT_OFFSET)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_MND_GET_SUB_EP)] = mmProcessReadMsg;

  // Requests handled by VNODE
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_MQ_SET_CONN_RSP)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_MQ_REB_RSP)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_CREATE_STB_RSP)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_ALTER_STB_RSP)] = mmProcessWriteMsg;
  pMgmt->msgFp[TMSG_INDEX(TDMT_VND_DROP_STB_RSP)] = mmProcessWriteMsg;
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

  dTrace("msg:%p, is created, app:%p RPC:%p user:%s", pMsg, pRpc->ahandle, pRpc->handle, pMsg->user);
  return 0;
}

void mmProcessRpcMsg(SDnode *pDnode, SRpcMsg *pRpc, SEpSet *pEpSet) {
  SMndMgmt *pMgmt = &pDnode->mmgmt;
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

  if (pMgmt->singleProc) {
    code = (*msgFp)(pDnode, pMsg);
  } else {
    code = taosProcPutToChildQueue(pMgmt->pProcess, pMsg, sizeof(SMndMsg), pRpc->pCont, pRpc->contLen);
  }

_OVER:

  if (code != 0) {
    bool isReq = (pRpc->msgType & 1U);
    if (isReq) {
      if (terrno == TSDB_CODE_DND_MNODE_NOT_DEPLOYED || terrno == TSDB_CODE_APP_NOT_READY) {
        dndSendRedirectRsp(pDnode, pRpc);
      } else {
        SRpcMsg rsp = {.handle = pRpc->handle, .ahandle = pRpc->ahandle, .code = terrno};
        rpcSendResponse(&rsp);
      }
    }
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

  int32_t code = dndWriteMsgToWorker(pWorker, pMsg, 0);
  mmRelease(pDnode, pMnode);
  return code;
}

static int32_t mmPutRpcMsgToWorker(SDnode *pDnode, SDnodeWorker *pWorker, SRpcMsg *pRpc) {
  SMndMsg *pMsg = taosAllocateQitem(sizeof(SMndMsg));
  if (pMsg == NULL) {
    return -1;
  }

  pMsg->rpcMsg = *pRpc;
  pMsg->createdTime = taosGetTimestampSec();

  int32_t code = mmPutMndMsgToWorker(pDnode, pWorker, pMsg);
  if (code != 0) {
    taosFreeQitem(pMsg);
    rpcFreeCont(pRpc->pCont);
  }

  return code;
}

void mmConsumeChildQueue(SDnode *pDnode, SMndMsg *pMsg, int32_t msgLen, void *pCont, int32_t contLen) {
  SMndMgmt *pMgmt = &pDnode->mmgmt;

  SRpcMsg *pRpc = &pMsg->rpcMsg;
  pRpc->pCont = pCont;

  MndMsgFp msgFp = pMgmt->msgFp[TMSG_INDEX(pRpc->msgType)];
  int32_t  code = (*msgFp)(pDnode, pMsg);

  if (code != 0) {
    bool isReq = (pRpc->msgType & 1U);
    if (isReq) {
      if (terrno == TSDB_CODE_DND_MNODE_NOT_DEPLOYED || terrno == TSDB_CODE_APP_NOT_READY) {
        dndSendRedirectRsp(pDnode, pRpc);
      } else {
        SRpcMsg rsp = {.handle = pRpc->handle, .ahandle = pRpc->ahandle, .code = terrno};
        rpcSendResponse(&rsp);
      }
    }
    taosFreeQitem(pMsg);
    rpcFreeCont(pCont);
  }
}

void mmConsumeParentQueue(SDnode *pDnode, SMndMsg *pMsg, int32_t msgLen, void *pCont, int32_t contLen) {}

static void mmConsumeQueue(SDnode *pDnode, SMndMsg *pMsg) {
  SMndMgmt *pMgmt = &pDnode->mmgmt;

  SMnode *pMnode = mmAcquire(pDnode);
  if (pMnode != NULL) {
    pMsg->pMnode = pMnode;
    mndProcessMsg(pMsg);
    mmRelease(pDnode, pMnode);
  } else {
    mndSendRsp(pMsg, terrno);
  }

  taosFreeQitem(pMsg);
  rpcFreeCont(pMsg->rpcMsg.pCont);
}
