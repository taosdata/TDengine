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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
#include "mndFlush.h"
#include "mndDnode.h"
#include "mndPrivilege.h"
#include "sdb.h"

static int32_t mndProcessFlushMnodeReq(SRpcMsg *pReq);
static int32_t mndProcessFlushMnodePeerReq(SRpcMsg *pReq);

int32_t mndInitFlush(SMnode *pMnode) {
  mndSetMsgHandle(pMnode, TDMT_MND_FLUSH_MNODE,      mndProcessFlushMnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_FLUSH_MNODE_PEER, mndProcessFlushMnodePeerReq);
  return 0;
}

void mndCleanupFlush(SMnode *pMnode) {}

static int32_t mndFlushSelf(SMnode *pMnode) {
  // delta=0: bypass accumulation threshold and flush immediately
  return sdbWriteFile(pMnode->pSdb, 0);
}

static void mndBroadcastFlushToPeers(SMnode *pMnode) {
  SSdb  *pSdb = pMnode->pSdb;
  void  *pIter = NULL;

  while (1) {
    SMnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_MNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    if (pObj->id == pMnode->selfDnodeId) {
      // skip self, already flushed in mndProcessFlushMnodeReq
      sdbRelease(pSdb, pObj);
      continue;
    }

    if (pObj->pDnode == NULL) {
      mWarn("mnode:%d, skip flush-mnode-peer, pDnode is NULL", pObj->id);
      sdbRelease(pSdb, pObj);
      continue;
    }

    SEpSet epSet = mndGetDnodeEpset(pObj->pDnode);

    SMTimerReq timerReq = {0};
    int32_t    contLen = tSerializeSMTimerMsg(NULL, 0, &timerReq);
    if (contLen <= 0) {
      sdbRelease(pSdb, pObj);
      continue;
    }
    void *pCont = rpcMallocCont(contLen);
    if (pCont == NULL) {
      sdbRelease(pSdb, pObj);
      continue;
    }
    tSerializeSMTimerMsg(pCont, contLen, &timerReq);

    SRpcMsg rpcMsg = {
        .msgType = TDMT_MND_FLUSH_MNODE_PEER,
        .pCont   = pCont,
        .contLen = contLen,
    };

    mInfo("mnode:%d, send flush-mnode-peer msg", pObj->id);
    tmsgSendReq(&epSet, &rpcMsg);

    sdbRelease(pSdb, pObj);
  }
}

static int32_t mndProcessFlushMnodeReq(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;

  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_SHOW_VARIBALES) != 0) {
    return -1;
  }

  mInfo("start to flush mnode sdb");

  if (mndFlushSelf(pMnode) != 0) {
    mError("failed to flush mnode sdb since %s", terrstr());
    return -1;
  }
  mInfo("mnode:%d, sdb flushed successfully", pMnode->selfDnodeId);

  mndBroadcastFlushToPeers(pMnode);

  return 0;
}

static int32_t mndProcessFlushMnodePeerReq(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;

  mInfo("mnode:%d, received flush-mnode-peer request", pMnode->selfDnodeId);

  if (mndFlushSelf(pMnode) != 0) {
    mError("mnode:%d, failed to flush sdb since %s", pMnode->selfDnodeId, terrstr());
    return -1;
  }
  mInfo("mnode:%d, sdb flushed by peer request", pMnode->selfDnodeId);
  return 0;
}
