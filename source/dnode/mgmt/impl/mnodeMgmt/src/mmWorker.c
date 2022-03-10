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

static int32_t mmProcessWriteMsg(SDnode *pDnode, SMnodeMsg *pMnodeMsg);
static int32_t mmProcessSyncMsg(SDnode *pDnode, SMnodeMsg *pMnodeMsg);
static int32_t mmProcessReadMsg(SDnode *pDnode, SMnodeMsg *pMnodeMsg);
static int32_t mmPutMsgToWorker(SDnode *pDnode, SDnodeWorker *pWorker, SMnodeMsg *pMnodeMsg);
static int32_t mmPutRpcMsgToWorker(SDnode *pDnode, SDnodeWorker *pWorker, SRpcMsg *pRpcMsg);
static void    mmConsumeQueue(SDnode *pDnode, SMnodeMsg *pMsg);

int32_t mmStartWorker(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
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

void mmProcessRpcMsg(SDnode *pDnode, SRpcMsg *pRpcMsg, SEpSet *pEpSet) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  int32_t     code = -1;
  SMnodeMsg  *pMnodeMsg = NULL;

  MndMsgFp msgFp = pMgmt->msgFp[TMSG_INDEX(pRpcMsg->msgType)];
  if (msgFp == NULL) {
    terrno = TSDB_CODE_MSG_NOT_PROCESSED;
    goto _OVER;
  }

  int32_t contLen = sizeof(SMnodeMsg) + pRpcMsg->contLen;
  pMnodeMsg = taosAllocateQitem(contLen);
  if (pMnodeMsg == NULL) {
    goto _OVER;
  }

  if (mndBuildMsg(pMnodeMsg, pRpcMsg) != 0) {
    goto _OVER;
  }

  if (pMgmt->singleProc) {
    code = (*msgFp)(pDnode, pMnodeMsg);
  } else {
    code = taosProcPushChild(pMgmt->pProcess, pMnodeMsg, contLen);
  }

_OVER:

  if (code == 0) {
    if (!pMgmt->singleProc) {
      taosFreeQitem(pMnodeMsg);
    }
  } else {
    bool isReq = (pRpcMsg->msgType & 1U);

    if (isReq) {
      if (terrno == TSDB_CODE_DND_MNODE_NOT_DEPLOYED || terrno == TSDB_CODE_APP_NOT_READY) {
        dndSendRedirectRsp(pDnode, pRpcMsg);
      } else {
        SRpcMsg rsp = {.handle = pRpcMsg->handle, .ahandle = pRpcMsg->ahandle, .code = terrno};
        rpcSendResponse(&rsp);
      }
    }
    taosFreeQitem(pMnodeMsg);
  }

  rpcFreeCont(pRpcMsg->pCont);
}

int32_t mmProcessWriteMsg(SDnode *pDnode, SMnodeMsg *pMnodeMsg) {
  return mmPutMsgToWorker(pDnode, &pDnode->mmgmt.writeWorker, pMnodeMsg);
}

int32_t mmProcessSyncMsg(SDnode *pDnode, SMnodeMsg *pMnodeMsg) {
  return mmPutMsgToWorker(pDnode, &pDnode->mmgmt.syncWorker, pMnodeMsg);
}

int32_t mmProcessReadMsg(SDnode *pDnode, SMnodeMsg *pMnodeMsg) {
  return mmPutMsgToWorker(pDnode, &pDnode->mmgmt.readWorker, pMnodeMsg);
}

int32_t mmPutMsgToWriteQueue(SDnode *pDnode, SRpcMsg *pRpcMsg) {
  return mmPutRpcMsgToWorker(pDnode, &pDnode->mmgmt.writeWorker, pRpcMsg);
}

int32_t mmPutMsgToReadQueue(SDnode *pDnode, SRpcMsg *pRpcMsg) {
  return mmPutRpcMsgToWorker(pDnode, &pDnode->mmgmt.readWorker, pRpcMsg);
}

static int32_t mmPutMsgToWorker(SDnode *pDnode, SDnodeWorker *pWorker, SMnodeMsg *pMnodeMsg) {
  SMnode *pMnode = mmAcquire(pDnode);
  if (pMnode == NULL) return -1;

  pMnodeMsg->pMnode = pMnode;
  int32_t code = dndWriteMsgToWorker(pWorker, pMnodeMsg, 0);

  mmRelease(pDnode, pMnode);
  return code;
}

static int32_t mmPutRpcMsgToWorker(SDnode *pDnode, SDnodeWorker *pWorker, SRpcMsg *pRpcMsg) {
  int32_t    contLen = sizeof(SMnodeMsg) + pRpcMsg->contLen;
  SMnodeMsg *pMnodeMsg = taosAllocateQitem(contLen);
  if (pMnodeMsg == NULL) {
    return -1;
  }

  pMnodeMsg->contLen = pRpcMsg->contLen;
  pMnodeMsg->pCont = (char *)pMnodeMsg + sizeof(SMnodeMsg);
  memcpy(pMnodeMsg->pCont, pRpcMsg->pCont, pRpcMsg->contLen);
  rpcFreeCont(pRpcMsg->pCont);

  int32_t code = mmPutMsgToWorker(pDnode, pWorker, pMnodeMsg);
  if (code != 0) {
    taosFreeQitem(pMnodeMsg);
  }

  return code;
}

void mmConsumeChildQueue(SDnode *pDnode, SBlockItem *pBlock) {
  SMnodeMsg *pMsg = (SMnodeMsg *)pBlock->pCont;

  if (mmPutMsgToWorker(pDnode, &pDnode->mmgmt.writeWorker, pMsg) != 0) {
    // todo
  }
}

void mmConsumeParentQueue(SMnodeMgmt *pMgmt, SBlockItem *pBlock) {}

static void mmConsumeQueue(SDnode *pDnode, SMnodeMsg *pMsg) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

  SMnode *pMnode = mmAcquire(pDnode);
  if (pMnode != NULL) {
    mndProcessMsg(pMsg);
    mmRelease(pDnode, pMnode);
  } else {
    mndSendRsp(pMsg, terrno);
  }

  // mndCleanupMsg(pMsg);
}
