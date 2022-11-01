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

#define _DEFAULT_SOURCE
#include "mndSync.h"
#include "mndTrans.h"

static int32_t mndSyncEqCtrlMsg(const SMsgCb *msgcb, SRpcMsg *pMsg) {
  if (pMsg == NULL || pMsg->pCont == NULL) {
    return -1;
  }

  SMsgHead *pHead = pMsg->pCont;
  pHead->contLen = htonl(pHead->contLen);
  pHead->vgId = htonl(pHead->vgId);

  if (msgcb == NULL || msgcb->putToQueueFp == NULL) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
    return -1;
  }

  int32_t code = tmsgPutToQueue(msgcb, SYNC_CTRL_QUEUE, pMsg);
  if (code != 0) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
  }
  return code;
}

static int32_t mndSyncEqMsg(const SMsgCb *msgcb, SRpcMsg *pMsg) {
  if (pMsg == NULL || pMsg->pCont == NULL) {
    return -1;
  }

  SMsgHead *pHead = pMsg->pCont;
  pHead->contLen = htonl(pHead->contLen);
  pHead->vgId = htonl(pHead->vgId);

  if (msgcb == NULL || msgcb->putToQueueFp == NULL) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
    return -1;
  }

  int32_t code = tmsgPutToQueue(msgcb, SYNC_QUEUE, pMsg);
  if (code != 0) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
  }
  return code;
}

static int32_t mndSyncSendMsg(const SEpSet *pEpSet, SRpcMsg *pMsg) {
  int32_t code = tmsgSendReq(pEpSet, pMsg);
  if (code != 0) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
  }
  return code;
}

void mndSyncCommitMsg(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  SMnode    *pMnode = pFsm->data;
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  SSdbRaw   *pRaw = pMsg->pCont;

  // delete msg handle
  SRpcMsg rpcMsg = {0};
  syncGetAndDelRespRpc(pMnode->syncMgmt.sync, cbMeta.seqNum, &rpcMsg.info);

  int32_t transId = sdbGetIdFromRaw(pMnode->pSdb, pRaw);
  pMgmt->errCode = cbMeta.code;
  mInfo("trans:%d, is proposed, saved:%d code:0x%x, apply index:%" PRId64 " term:%" PRIu64 " config:%" PRId64
        " role:%s raw:%p",
        transId, pMgmt->transId, cbMeta.code, cbMeta.index, cbMeta.term, cbMeta.lastConfigIndex, syncStr(cbMeta.state),
        pRaw);

  if (pMgmt->errCode == 0) {
    sdbWriteWithoutFree(pMnode->pSdb, pRaw);
    sdbSetApplyInfo(pMnode->pSdb, cbMeta.index, cbMeta.term, cbMeta.lastConfigIndex);
  }

  taosWLockLatch(&pMgmt->lock);
  if (transId <= 0) {
    taosWUnLockLatch(&pMgmt->lock);
    mError("trans:%d, invalid commit msg", transId);
  } else if (transId == pMgmt->transId) {
    if (pMgmt->errCode != 0) {
      mError("trans:%d, failed to propose since %s, post sem", transId, tstrerror(pMgmt->errCode));
    } else {
      mInfo("trans:%d, is proposed and post sem", transId);
    }
    pMgmt->transId = 0;
    tsem_post(&pMgmt->syncSem);
    taosWUnLockLatch(&pMgmt->lock);
  } else {
    taosWUnLockLatch(&pMgmt->lock);
    STrans *pTrans = mndAcquireTrans(pMnode, transId);
    if (pTrans != NULL) {
      mInfo("trans:%d, execute in mnode which not leader", transId);
      mndTransExecute(pMnode, pTrans);
      mndReleaseTrans(pMnode, pTrans);
      // sdbWriteFile(pMnode->pSdb, SDB_WRITE_DELTA);
    } else {
      mError("trans:%d, not found while execute in mnode since %s", transId, terrstr());
    }
  }
}

int32_t mndSyncGetSnapshot(struct SSyncFSM *pFsm, SSnapshot *pSnapshot, void *pReaderParam, void **ppReader) {
  mInfo("start to read snapshot from sdb in atomic way");
  SMnode *pMnode = pFsm->data;
  return sdbStartRead(pMnode->pSdb, (SSdbIter **)ppReader, &pSnapshot->lastApplyIndex, &pSnapshot->lastApplyTerm,
                      &pSnapshot->lastConfigIndex);
  return 0;
}

int32_t mndSyncGetSnapshotInfo(struct SSyncFSM *pFsm, SSnapshot *pSnapshot) {
  SMnode *pMnode = pFsm->data;
  sdbGetCommitInfo(pMnode->pSdb, &pSnapshot->lastApplyIndex, &pSnapshot->lastApplyTerm, &pSnapshot->lastConfigIndex);
  return 0;
}

void mndRestoreFinish(struct SSyncFSM *pFsm) {
  SMnode *pMnode = pFsm->data;

  if (!pMnode->deploy) {
    mInfo("vgId:1, sync restore finished, and will handle outstanding transactions");
    mndTransPullup(pMnode);
    mndSetRestored(pMnode, true);
  } else {
    mInfo("vgId:1, sync restore finished");
  }
}

void mndReConfig(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SReConfigCbMeta *cbMeta) {}

int32_t mndSnapshotStartRead(struct SSyncFSM *pFsm, void *pParam, void **ppReader) {
  mInfo("start to read snapshot from sdb");
  SMnode *pMnode = pFsm->data;
  return sdbStartRead(pMnode->pSdb, (SSdbIter **)ppReader, NULL, NULL, NULL);
}

int32_t mndSnapshotStopRead(struct SSyncFSM *pFsm, void *pReader) {
  mInfo("stop to read snapshot from sdb");
  SMnode *pMnode = pFsm->data;
  return sdbStopRead(pMnode->pSdb, pReader);
}

int32_t mndSnapshotDoRead(struct SSyncFSM *pFsm, void *pReader, void **ppBuf, int32_t *len) {
  SMnode *pMnode = pFsm->data;
  return sdbDoRead(pMnode->pSdb, pReader, ppBuf, len);
}

int32_t mndSnapshotStartWrite(struct SSyncFSM *pFsm, void *pParam, void **ppWriter) {
  mInfo("start to apply snapshot to sdb");
  SMnode *pMnode = pFsm->data;
  return sdbStartWrite(pMnode->pSdb, (SSdbIter **)ppWriter);
}

int32_t mndSnapshotStopWrite(struct SSyncFSM *pFsm, void *pWriter, bool isApply, SSnapshot *pSnapshot) {
  mInfo("stop to apply snapshot to sdb, apply:%d, index:%" PRId64 " term:%" PRIu64 " config:%" PRId64, isApply,
        pSnapshot->lastApplyIndex, pSnapshot->lastApplyTerm, pSnapshot->lastConfigIndex);
  SMnode *pMnode = pFsm->data;
  return sdbStopWrite(pMnode->pSdb, pWriter, isApply, pSnapshot->lastApplyIndex, pSnapshot->lastApplyTerm,
                      pSnapshot->lastConfigIndex);
}

int32_t mndSnapshotDoWrite(struct SSyncFSM *pFsm, void *pWriter, void *pBuf, int32_t len) {
  SMnode *pMnode = pFsm->data;
  return sdbDoWrite(pMnode->pSdb, pWriter, pBuf, len);
}

void mndLeaderTransfer(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  SMnode *pMnode = pFsm->data;
  atomic_store_8(&(pMnode->syncMgmt.leaderTransferFinish), 1);
  mInfo("vgId:1, mnode leader transfer finish");
}

static void mndBecomeFollower(struct SSyncFSM *pFsm) {
  SMnode *pMnode = pFsm->data;
  mInfo("vgId:1, become follower");

  taosWLockLatch(&pMnode->syncMgmt.lock);
  if (pMnode->syncMgmt.transId != 0) {
    mInfo("vgId:1, become follower and post sem, trans:%d, failed to propose since not leader",
          pMnode->syncMgmt.transId);
    pMnode->syncMgmt.transId = 0;
    pMnode->syncMgmt.errCode = TSDB_CODE_SYN_NOT_LEADER;
    tsem_post(&pMnode->syncMgmt.syncSem);
  }
  taosWUnLockLatch(&pMnode->syncMgmt.lock);
}

static void mndBecomeLeader(struct SSyncFSM *pFsm) {
  mInfo("vgId:1, become leader");
  SMnode *pMnode = pFsm->data;
}

SSyncFSM *mndSyncMakeFsm(SMnode *pMnode) {
  SSyncFSM *pFsm = taosMemoryCalloc(1, sizeof(SSyncFSM));
  pFsm->data = pMnode;
  pFsm->FpCommitCb = mndSyncCommitMsg;
  pFsm->FpPreCommitCb = NULL;
  pFsm->FpRollBackCb = NULL;
  pFsm->FpRestoreFinishCb = mndRestoreFinish;
  pFsm->FpLeaderTransferCb = mndLeaderTransfer;
  pFsm->FpReConfigCb = mndReConfig;
  pFsm->FpBecomeLeaderCb = mndBecomeLeader;
  pFsm->FpBecomeFollowerCb = mndBecomeFollower;
  pFsm->FpGetSnapshot = mndSyncGetSnapshot;
  pFsm->FpGetSnapshotInfo = mndSyncGetSnapshotInfo;
  pFsm->FpSnapshotStartRead = mndSnapshotStartRead;
  pFsm->FpSnapshotStopRead = mndSnapshotStopRead;
  pFsm->FpSnapshotDoRead = mndSnapshotDoRead;
  pFsm->FpSnapshotStartWrite = mndSnapshotStartWrite;
  pFsm->FpSnapshotStopWrite = mndSnapshotStopWrite;
  pFsm->FpSnapshotDoWrite = mndSnapshotDoWrite;
  return pFsm;
}

int32_t mndInitSync(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  taosInitRWLatch(&pMgmt->lock);
  pMgmt->transId = 0;

  SSyncInfo syncInfo = {
      .snapshotStrategy = SYNC_STRATEGY_STANDARD_SNAPSHOT,
      .batchSize = 1,
      .vgId = 1,
      .pWal = pMnode->pWal,
      .msgcb = NULL,
      .FpSendMsg = mndSyncSendMsg,
      .FpEqMsg = mndSyncEqMsg,
      .FpEqCtrlMsg = mndSyncEqCtrlMsg,
  };

  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s%ssync", pMnode->path, TD_DIRSEP);
  syncInfo.pFsm = mndSyncMakeFsm(pMnode);

  mInfo("vgId:1, start to open sync, replica:%d selfIndex:%d", pMgmt->numOfReplicas, pMgmt->selfIndex);
  SSyncCfg *pCfg = &syncInfo.syncCfg;
  pCfg->replicaNum = pMgmt->numOfReplicas;
  pCfg->myIndex = pMgmt->selfIndex;
  for (int32_t i = 0; i < pMgmt->numOfReplicas; ++i) {
    SNodeInfo *pNode = &pCfg->nodeInfo[i];
    tstrncpy(pNode->nodeFqdn, pMgmt->replicas[i].fqdn, sizeof(pNode->nodeFqdn));
    pNode->nodePort = pMgmt->replicas[i].port;
    mInfo("vgId:1, index:%d ep:%s:%u", i, pNode->nodeFqdn, pNode->nodePort);
  }

  tsem_init(&pMgmt->syncSem, 0, 0);
  pMgmt->sync = syncOpen(&syncInfo);
  if (pMgmt->sync <= 0) {
    mError("failed to open sync since %s", terrstr());
    return -1;
  }

  // decrease election timer
  setPingTimerMS(pMgmt->sync, 5000);
  setElectTimerMS(pMgmt->sync, 3000);
  setHeartbeatTimerMS(pMgmt->sync, 500);

  mInfo("mnode-sync is opened, id:%" PRId64, pMgmt->sync);
  return 0;
}

void mndCleanupSync(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  syncStop(pMgmt->sync);
  mInfo("mnode-sync is stopped, id:%" PRId64, pMgmt->sync);

  tsem_destroy(&pMgmt->syncSem);
  memset(pMgmt, 0, sizeof(SSyncMgmt));
}

int32_t mndSyncPropose(SMnode *pMnode, SSdbRaw *pRaw, int32_t transId) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  SRpcMsg    req = {.msgType = TDMT_MND_APPLY_MSG, .contLen = sdbGetRawTotalSize(pRaw)};
  if (req.contLen <= 0) {
    terrno = TSDB_CODE_APP_ERROR;
    return -1;
  }

  req.pCont = rpcMallocCont(req.contLen);
  if (req.pCont == NULL) return -1;
  memcpy(req.pCont, pRaw, req.contLen);

  pMgmt->errCode = 0;
  taosWLockLatch(&pMgmt->lock);
  if (pMgmt->transId != 0) {
    mError("trans:%d, can't be proposed since trans:%d alrady waiting for confirm", transId, pMgmt->transId);
    taosWUnLockLatch(&pMgmt->lock);
    terrno = TSDB_CODE_APP_NOT_READY;
    return -1;
  } else {
    pMgmt->transId = transId;
    mInfo("trans:%d, will be proposed", pMgmt->transId);
    taosWUnLockLatch(&pMgmt->lock);
  }

  const bool isWeak = false;
  int32_t    code = syncPropose(pMgmt->sync, &req, isWeak);

  if (code == 0) {
    mInfo("trans:%d, is proposing and wait sem", pMgmt->transId);
    tsem_wait(&pMgmt->syncSem);
  } else if (code > 0) {
    mInfo("trans:%d, confirm at once since replica is 1, continue execute", transId);
    taosWLockLatch(&pMgmt->lock);
    pMgmt->transId = 0;
    taosWUnLockLatch(&pMgmt->lock);
    sdbWriteWithoutFree(pMnode->pSdb, pRaw);
    sdbSetApplyInfo(pMnode->pSdb, req.info.conn.applyIndex, req.info.conn.applyTerm, SYNC_INDEX_INVALID);
    code = 0;
  } else {
    taosWLockLatch(&pMgmt->lock);
    mInfo("trans:%d, failed to proposed since %s", transId, terrstr());
    pMgmt->transId = 0;
    taosWUnLockLatch(&pMgmt->lock);
    if (terrno == TSDB_CODE_SYN_NOT_LEADER) {
      terrno = TSDB_CODE_APP_NOT_READY;
    } else {
      terrno = TSDB_CODE_APP_ERROR;
    }
  }

  rpcFreeCont(req.pCont);
  if (code != 0) {
    mError("trans:%d, failed to propose, code:0x%x", pMgmt->transId, code);
    return code;
  }

  if (pMgmt->errCode != 0) terrno = pMgmt->errCode;
  return pMgmt->errCode;
}

void mndSyncStart(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  syncSetMsgCb(pMgmt->sync, &pMnode->msgCb);
  syncStart(pMgmt->sync);
  mInfo("vgId:1, sync started, id:%" PRId64, pMgmt->sync);
}

void mndSyncStop(SMnode *pMnode) {
  taosWLockLatch(&pMnode->syncMgmt.lock);
  if (pMnode->syncMgmt.transId != 0) {
    mInfo("vgId:1, is stopped and post sem, trans:%d", pMnode->syncMgmt.transId);
    pMnode->syncMgmt.transId = 0;
    tsem_post(&pMnode->syncMgmt.syncSem);
  }
  taosWUnLockLatch(&pMnode->syncMgmt.lock);
}

bool mndIsLeader(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;

  if (!syncIsReady(pMgmt->sync)) {
    // get terrno from syncIsReady
    // terrno = TSDB_CODE_SYN_NOT_LEADER;
    return false;
  }

  if (!mndGetRestored(pMnode)) {
    terrno = TSDB_CODE_APP_NOT_READY;
    return false;
  }

  return true;
}
