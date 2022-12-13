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

int32_t mndProcessWriteMsg(const SSyncFSM *pFsm, SRpcMsg *pMsg, const SFsmCbMeta *pMeta) {
  SMnode    *pMnode = pFsm->data;
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  SSdbRaw   *pRaw = pMsg->pCont;

  int32_t transId = sdbGetIdFromRaw(pMnode->pSdb, pRaw);
  pMgmt->errCode = pMeta->code;
  mInfo("trans:%d, is proposed, saved:%d code:0x%x, apply index:%" PRId64 " term:%" PRIu64 " config:%" PRId64
        " role:%s raw:%p",
        transId, pMgmt->transId, pMeta->code, pMeta->index, pMeta->term, pMeta->lastConfigIndex, syncStr(pMeta->state),
        pRaw);

  if (pMgmt->errCode == 0) {
    sdbWriteWithoutFree(pMnode->pSdb, pRaw);
    sdbSetApplyInfo(pMnode->pSdb, pMeta->index, pMeta->term, pMeta->lastConfigIndex);
  }

  taosThreadMutexLock(&pMgmt->lock);
  if (transId <= 0) {
    taosThreadMutexUnlock(&pMgmt->lock);
    mError("trans:%d, invalid commit msg", transId);
  } else if (transId == pMgmt->transId) {
    if (pMgmt->errCode != 0) {
      mError("trans:%d, failed to propose since %s, post sem", transId, tstrerror(pMgmt->errCode));
    } else {
      mInfo("trans:%d, is proposed and post sem", transId);
    }
    pMgmt->transId = 0;
    tsem_post(&pMgmt->syncSem);
    taosThreadMutexUnlock(&pMgmt->lock);
  } else {
    taosThreadMutexUnlock(&pMgmt->lock);
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

  return 0;
}

int32_t mndSyncCommitMsg(const SSyncFSM *pFsm, SRpcMsg *pMsg, const SFsmCbMeta *pMeta) {
  int32_t code = 0;
  if (!syncUtilUserCommit(pMsg->msgType)) {
    goto _out;
  }
  code = mndProcessWriteMsg(pFsm, pMsg, pMeta);

_out:
  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = NULL;
  return code;
}

int32_t mndSyncGetSnapshot(const SSyncFSM *pFsm, SSnapshot *pSnapshot, void *pReaderParam, void **ppReader) {
  mInfo("start to read snapshot from sdb in atomic way");
  SMnode *pMnode = pFsm->data;
  return sdbStartRead(pMnode->pSdb, (SSdbIter **)ppReader, &pSnapshot->lastApplyIndex, &pSnapshot->lastApplyTerm,
                      &pSnapshot->lastConfigIndex);
  return 0;
}

int32_t mndSyncGetSnapshotInfo(const SSyncFSM *pFsm, SSnapshot *pSnapshot) {
  SMnode *pMnode = pFsm->data;
  sdbGetCommitInfo(pMnode->pSdb, &pSnapshot->lastApplyIndex, &pSnapshot->lastApplyTerm, &pSnapshot->lastConfigIndex);
  return 0;
}

void mndRestoreFinish(const SSyncFSM *pFsm) {
  SMnode *pMnode = pFsm->data;

  if (!pMnode->deploy) {
    if (!pMnode->restored) {
      mInfo("vgId:1, sync restore finished, and will handle outstanding transactions");
      mndTransPullup(pMnode);
      mndSetRestored(pMnode, true);
    } else {
      mInfo("vgId:1, sync restore finished, repeat call");
    }
  } else {
    mInfo("vgId:1, sync restore finished");
  }
}

int32_t mndSnapshotStartRead(const SSyncFSM *pFsm, void *pParam, void **ppReader) {
  mInfo("start to read snapshot from sdb");
  SMnode *pMnode = pFsm->data;
  return sdbStartRead(pMnode->pSdb, (SSdbIter **)ppReader, NULL, NULL, NULL);
}

int32_t mndSnapshotStopRead(const SSyncFSM *pFsm, void *pReader) {
  mInfo("stop to read snapshot from sdb");
  SMnode *pMnode = pFsm->data;
  return sdbStopRead(pMnode->pSdb, pReader);
}

int32_t mndSnapshotDoRead(const SSyncFSM *pFsm, void *pReader, void **ppBuf, int32_t *len) {
  SMnode *pMnode = pFsm->data;
  return sdbDoRead(pMnode->pSdb, pReader, ppBuf, len);
}

int32_t mndSnapshotStartWrite(const SSyncFSM *pFsm, void *pParam, void **ppWriter) {
  mInfo("start to apply snapshot to sdb");
  SMnode *pMnode = pFsm->data;
  return sdbStartWrite(pMnode->pSdb, (SSdbIter **)ppWriter);
}

int32_t mndSnapshotStopWrite(const SSyncFSM *pFsm, void *pWriter, bool isApply, SSnapshot *pSnapshot) {
  mInfo("stop to apply snapshot to sdb, apply:%d, index:%" PRId64 " term:%" PRIu64 " config:%" PRId64, isApply,
        pSnapshot->lastApplyIndex, pSnapshot->lastApplyTerm, pSnapshot->lastConfigIndex);
  SMnode *pMnode = pFsm->data;
  return sdbStopWrite(pMnode->pSdb, pWriter, isApply, pSnapshot->lastApplyIndex, pSnapshot->lastApplyTerm,
                      pSnapshot->lastConfigIndex);
}

int32_t mndSnapshotDoWrite(const SSyncFSM *pFsm, void *pWriter, void *pBuf, int32_t len) {
  SMnode *pMnode = pFsm->data;
  return sdbDoWrite(pMnode->pSdb, pWriter, pBuf, len);
}

static void mndBecomeFollower(const SSyncFSM *pFsm) {
  SMnode    *pMnode = pFsm->data;
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  mInfo("vgId:1, become follower");

  taosThreadMutexLock(&pMgmt->lock);
  if (pMgmt->transId != 0) {
    mInfo("vgId:1, become follower and post sem, trans:%d, failed to propose since not leader", pMgmt->transId);
    pMgmt->transId = 0;
    pMgmt->errCode = TSDB_CODE_SYN_NOT_LEADER;
    tsem_post(&pMgmt->syncSem);
  }
  taosThreadMutexUnlock(&pMgmt->lock);
}

static void mndBecomeLeader(const SSyncFSM *pFsm) {
  mInfo("vgId:1, become leader");
  SMnode *pMnode = pFsm->data;
}

static bool mndApplyQueueEmpty(const SSyncFSM *pFsm) {
  SMnode *pMnode = pFsm->data;

  if (pMnode != NULL && pMnode->msgCb.qsizeFp != NULL) {
    int32_t itemSize = tmsgGetQueueSize(&pMnode->msgCb, 1, APPLY_QUEUE);
    return (itemSize == 0);
  } else {
    return true;
  }
}

static int32_t mndApplyQueueItems(const SSyncFSM *pFsm) {
  SMnode *pMnode = pFsm->data;

  if (pMnode != NULL && pMnode->msgCb.qsizeFp != NULL) {
    int32_t itemSize = tmsgGetQueueSize(&pMnode->msgCb, 1, APPLY_QUEUE);
    return itemSize;
  } else {
    return -1;
  }
}

SSyncFSM *mndSyncMakeFsm(SMnode *pMnode) {
  SSyncFSM *pFsm = taosMemoryCalloc(1, sizeof(SSyncFSM));
  pFsm->data = pMnode;
  pFsm->FpCommitCb = mndSyncCommitMsg;
  pFsm->FpPreCommitCb = NULL;
  pFsm->FpRollBackCb = NULL;
  pFsm->FpRestoreFinishCb = mndRestoreFinish;
  pFsm->FpLeaderTransferCb = NULL;
  pFsm->FpApplyQueueEmptyCb = mndApplyQueueEmpty;
  pFsm->FpApplyQueueItems = mndApplyQueueItems;
  pFsm->FpReConfigCb = NULL;
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
  taosThreadMutexInit(&pMgmt->lock, NULL);
  pMgmt->transId = 0;

  SSyncInfo syncInfo = {
      .snapshotStrategy = SYNC_STRATEGY_STANDARD_SNAPSHOT,
      .batchSize = 1,
      .vgId = 1,
      .pWal = pMnode->pWal,
      .msgcb = &pMnode->msgCb,
      .syncSendMSg = mndSyncSendMsg,
      .syncEqMsg = mndSyncEqMsg,
      .syncEqCtrlMsg = mndSyncEqCtrlMsg,
      .pingMs = 5000,
      .electMs = 3000,
      .heartbeatMs = 500,
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

  mInfo("mnode-sync is opened, id:%" PRId64, pMgmt->sync);
  return 0;
}

void mndCleanupSync(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  syncStop(pMgmt->sync);
  mInfo("mnode-sync is stopped, id:%" PRId64, pMgmt->sync);

  tsem_destroy(&pMgmt->syncSem);
  taosThreadMutexDestroy(&pMgmt->lock);
  memset(pMgmt, 0, sizeof(SSyncMgmt));
}

int32_t mndSyncPropose(SMnode *pMnode, SSdbRaw *pRaw, int32_t transId) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  pMgmt->errCode = 0;

  SRpcMsg req = {.msgType = TDMT_MND_APPLY_MSG, .contLen = sdbGetRawTotalSize(pRaw)};
  if (req.contLen <= 0) return -1;

  req.pCont = rpcMallocCont(req.contLen);
  if (req.pCont == NULL) return -1;
  memcpy(req.pCont, pRaw, req.contLen);

  taosThreadMutexLock(&pMgmt->lock);
  if (pMgmt->transId != 0) {
    mError("trans:%d, can't be proposed since trans:%d already waiting for confirm", transId, pMgmt->transId);
    taosThreadMutexUnlock(&pMgmt->lock);
    terrno = TSDB_CODE_MND_LAST_TRANS_NOT_FINISHED;
    return -1;
  }

  mInfo("trans:%d, will be proposed", transId);
  pMgmt->transId = transId;
  taosThreadMutexUnlock(&pMgmt->lock);

  int32_t code = syncPropose(pMgmt->sync, &req, false);
  if (code == 0) {
    mInfo("trans:%d, is proposing and wait sem", transId);
    tsem_wait(&pMgmt->syncSem);
  } else if (code > 0) {
    mInfo("trans:%d, confirm at once since replica is 1, continue execute", transId);
    taosThreadMutexLock(&pMgmt->lock);
    pMgmt->transId = 0;
    taosThreadMutexUnlock(&pMgmt->lock);
    sdbWriteWithoutFree(pMnode->pSdb, pRaw);
    sdbSetApplyInfo(pMnode->pSdb, req.info.conn.applyIndex, req.info.conn.applyTerm, SYNC_INDEX_INVALID);
    code = 0;
  } else {
    mError("trans:%d, failed to proposed since %s", transId, terrstr());
    taosThreadMutexLock(&pMgmt->lock);
    pMgmt->transId = 0;
    taosThreadMutexUnlock(&pMgmt->lock);
    if (terrno == 0) {
      terrno = TSDB_CODE_APP_ERROR;
    }
  }

  rpcFreeCont(req.pCont);
  req.pCont = NULL;
  if (code != 0) {
    mError("trans:%d, failed to propose, code:0x%x", pMgmt->transId, code);
    return code;
  }

  terrno = pMgmt->errCode;
  return pMgmt->errCode;
}

void mndSyncStart(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  if (syncStart(pMgmt->sync) < 0) {
    mError("vgId:1, failed to start sync, id:%" PRId64, pMgmt->sync);
    return;
  }
  mInfo("vgId:1, sync started, id:%" PRId64, pMgmt->sync);
}

void mndSyncStop(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;

  taosThreadMutexLock(&pMgmt->lock);
  if (pMgmt->transId != 0) {
    mInfo("vgId:1, is stopped and post sem, trans:%d", pMgmt->transId);
    pMgmt->transId = 0;
    tsem_post(&pMgmt->syncSem);
  }
  taosThreadMutexUnlock(&pMgmt->lock);
}

bool mndIsLeader(SMnode *pMnode) {
  terrno = 0;
  SSyncState state = syncGetState(pMnode->syncMgmt.sync);

  if (terrno != 0) {
    mDebug("vgId:1, mnode is stopping");
    return false;
  }

  if (state.state != TAOS_SYNC_STATE_LEADER) {
    terrno = TSDB_CODE_SYN_NOT_LEADER;
    mDebug("vgId:1, mnode not leader, state:%s", syncStr(state.state));
    return false;
  }

  if (!state.restored || !pMnode->restored) {
    terrno = TSDB_CODE_SYN_RESTORING;
    mDebug("vgId:1, mnode not restored:%d:%d", state.restored, pMnode->restored);
    return false;
  }

  return true;
}
