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
#include "mndCluster.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndStream.h"

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

  int32_t code = tmsgPutToQueue(msgcb, SYNC_RD_QUEUE, pMsg);
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
  int32_t code = tmsgSendSyncReq(pEpSet, pMsg);
  if (code != 0) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
  }
  return code;
}

static int32_t mndTransValidatePrepareAction(SMnode *pMnode, STrans *pTrans, STransAction *pAction) {
  SSdbRaw *pRaw = pAction->pRaw;
  SSdb    *pSdb = pMnode->pSdb;
  int      code = 0;

  if (pRaw->status != SDB_STATUS_CREATING) goto _OUT;

  SdbValidateFp validateFp = pSdb->validateFps[pRaw->type];
  if (validateFp) {
    code = validateFp(pMnode, pTrans, pRaw);
  }

_OUT:
  return code;
}

static int32_t mndTransValidatePrepareStage(SMnode *pMnode, STrans *pTrans) {
  int32_t code = -1;
  int32_t action = 0;

  int32_t numOfActions = taosArrayGetSize(pTrans->prepareActions);
  if (numOfActions == 0) {
    code = 0;
    goto _OUT;
  }

  mInfo("trans:%d, validate %d prepare actions.", pTrans->id, numOfActions);

  for (action = 0; action < numOfActions; ++action) {
    STransAction *pAction = taosArrayGet(pTrans->prepareActions, action);

    if (pAction->actionType != TRANS_ACTION_RAW) {
      mError("trans:%d, prepare action:%d of unexpected type:%d", pTrans->id, action, pAction->actionType);
      goto _OUT;
    }

    code = mndTransValidatePrepareAction(pMnode, pTrans, pAction);
    if (code != 0) {
      mError("trans:%d, failed to validate prepare action: %d, numOfActions:%d", pTrans->id, action, numOfActions);
      goto _OUT;
    }
  }

  code = 0;
_OUT:
  return code;
}

static int32_t mndTransValidateImp(SMnode *pMnode, STrans *pTrans) {
  int32_t code = 0;
  if (pTrans->stage == TRN_STAGE_PREPARE) {
    if ((code = mndTransCheckConflict(pMnode, pTrans)) < 0) {
      mError("trans:%d, failed to validate trans conflicts.", pTrans->id);
      TAOS_RETURN(code);
    }

    return mndTransValidatePrepareStage(pMnode, pTrans);
  }
  TAOS_RETURN(code);
}

static int32_t mndTransValidate(SMnode *pMnode, SSdbRaw *pRaw) {
  STrans *pTrans = NULL;
  int32_t code = -1;

  SSdbRow *pRow = mndTransDecode(pRaw);
  if (pRow == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OUT;
  }

  pTrans = sdbGetRowObj(pRow);
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OUT;
  }

  code = mndTransValidateImp(pMnode, pTrans);

_OUT:
  if (pTrans) mndTransDropData(pTrans);
  if (pRow) taosMemoryFreeClear(pRow);
  if (code) terrno = (terrno ? terrno : TSDB_CODE_MND_TRANS_CONFLICT);
  TAOS_RETURN(code);
}

int32_t mndProcessWriteMsg(SMnode *pMnode, SRpcMsg *pMsg, SFsmCbMeta *pMeta) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  SSdbRaw   *pRaw = pMsg->pCont;
  STrans    *pTrans = NULL;
  int32_t    code = -1;
  int32_t    transId = sdbGetIdFromRaw(pMnode->pSdb, pRaw);

  if (transId <= 0) {
    mError("trans:%d, invalid commit msg, cache transId:%d seq:%" PRId64, transId, pMgmt->transId, pMgmt->transSeq);
    code = TSDB_CODE_INVALID_MSG;
    goto _OUT;
  }

  mInfo("trans:%d, process sync proposal, saved:%d code:0x%x, apply index:%" PRId64 " term:%" PRIu64 " config:%" PRId64
        " role:%s raw:%p sec:%d seq:%" PRId64,
        transId, pMgmt->transId, pMeta->code, pMeta->index, pMeta->term, pMeta->lastConfigIndex, syncStr(pMeta->state),
        pRaw, pMgmt->transSec, pMgmt->transSeq);

  code = mndTransValidate(pMnode, pRaw);
  if (code != 0) {
    mError("trans:%d, failed to validate requested trans since %s", transId, terrstr());
    // code = 0;
    pMeta->code = code;
    goto _OUT;
  }

  (void)taosThreadMutexLock(&pMnode->pSdb->filelock);
  code = sdbWriteWithoutFree(pMnode->pSdb, pRaw);
  if (code != 0) {
    mError("trans:%d, failed to write to sdb since %s", transId, terrstr());
    // code = 0;
    (void)taosThreadMutexUnlock(&pMnode->pSdb->filelock);
    pMeta->code = code;
    goto _OUT;
  }
  (void)taosThreadMutexUnlock(&pMnode->pSdb->filelock);

  pTrans = mndAcquireTrans(pMnode, transId);
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    mError("trans:%d, not found while execute in mnode since %s", transId, tstrerror(code));
    goto _OUT;
  }

  if (pTrans->stage == TRN_STAGE_PREPARE) {
    bool continueExec = mndTransPerformPrepareStage(pMnode, pTrans, false);
    if (!continueExec) {
      if (terrno != 0) code = terrno;
      goto _OUT;
    }
  }

  mndTransRefresh(pMnode, pTrans);

  sdbSetApplyInfo(pMnode->pSdb, pMeta->index, pMeta->term, pMeta->lastConfigIndex);
  code = sdbWriteFile(pMnode->pSdb, tsMndSdbWriteDelta);

_OUT:
  if (pTrans) mndReleaseTrans(pMnode, pTrans);
  TAOS_RETURN(code);
}

static int32_t mndPostMgmtCode(SMnode *pMnode, int32_t code) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  (void)taosThreadMutexLock(&pMgmt->lock);
  if (pMgmt->transId == 0) {
    goto _OUT;
  }

  int32_t transId = pMgmt->transId;
  pMgmt->transId = 0;
  pMgmt->transSec = 0;
  pMgmt->transSeq = 0;
  pMgmt->errCode = code;
  (void)tsem_post(&pMgmt->syncSem);

  if (pMgmt->errCode != 0) {
    mError("trans:%d, failed to propose since %s, post sem", transId, tstrerror(pMgmt->errCode));
  } else {
    mInfo("trans:%d, is proposed and post sem, seq:%" PRId64, transId, pMgmt->transSeq);
  }

_OUT:
  (void)taosThreadMutexUnlock(&pMgmt->lock);
  return 0;
}

int32_t mndSyncCommitMsg(const SSyncFSM *pFsm, SRpcMsg *pMsg, SFsmCbMeta *pMeta) {
  SMnode *pMnode = pFsm->data;
  int32_t code = pMsg->code;
  if (code != 0) {
    goto _OUT;
  }

  pMsg->info.conn.applyIndex = pMeta->index;
  pMsg->info.conn.applyTerm = pMeta->term;
  pMeta->code = 0;

  atomic_store_64(&pMnode->applied, pMsg->info.conn.applyIndex);

  if (!syncUtilUserCommit(pMsg->msgType)) {
    goto _OUT;
  }

  code = mndProcessWriteMsg(pMnode, pMsg, pMeta);

_OUT:
  mndPostMgmtCode(pMnode, code ? code : pMeta->code);
  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = NULL;
  TAOS_RETURN(code);
}

SyncIndex mndSyncAppliedIndex(const SSyncFSM *pFSM) {
  SMnode *pMnode = pFSM->data;
  return atomic_load_64(&pMnode->applied);
}

int32_t mndSyncGetSnapshot(const SSyncFSM *pFsm, SSnapshot *pSnapshot, void *pReaderParam, void **ppReader) {
  mInfo("start to read snapshot from sdb in atomic way");
  SMnode *pMnode = pFsm->data;
  return sdbStartRead(pMnode->pSdb, (SSdbIter **)ppReader, &pSnapshot->lastApplyIndex, &pSnapshot->lastApplyTerm,
                      &pSnapshot->lastConfigIndex);
  return 0;
}

static int32_t mndSyncGetSnapshotInfo(const SSyncFSM *pFsm, SSnapshot *pSnapshot) {
  SMnode *pMnode = pFsm->data;
  sdbGetCommitInfo(pMnode->pSdb, &pSnapshot->lastApplyIndex, &pSnapshot->lastApplyTerm, &pSnapshot->lastConfigIndex);
  return 0;
}

void mndRestoreFinish(const SSyncFSM *pFsm, const SyncIndex commitIdx) {
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
  (void)mndRefreshUserIpWhiteList(pMnode);

  SyncIndex fsmIndex = mndSyncAppliedIndex(pFsm);
  if (commitIdx != fsmIndex) {
    mError("vgId:1, sync restore finished, but commitIdx:%" PRId64 " is not equal to appliedIdx:%" PRId64, commitIdx,
           fsmIndex);
    mndSetRestored(pMnode, false);
  }
}

int32_t mndSnapshotStartRead(const SSyncFSM *pFsm, void *pParam, void **ppReader) {
  mInfo("start to read snapshot from sdb");
  SMnode *pMnode = pFsm->data;
  return sdbStartRead(pMnode->pSdb, (SSdbIter **)ppReader, NULL, NULL, NULL);
}

static void mndSnapshotStopRead(const SSyncFSM *pFsm, void *pReader) {
  mInfo("stop to read snapshot from sdb");
  SMnode *pMnode = pFsm->data;
  sdbStopRead(pMnode->pSdb, pReader);
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

  (void)taosThreadMutexLock(&pMgmt->lock);
  if (pMgmt->transId != 0) {
    mInfo("vgId:1, become follower and post sem, trans:%d, failed to propose since not leader", pMgmt->transId);
    pMgmt->transId = 0;
    pMgmt->transSec = 0;
    pMgmt->transSeq = 0;
    pMgmt->errCode = TSDB_CODE_SYN_NOT_LEADER;
    (void)tsem_post(&pMgmt->syncSem);
  }
  (void)taosThreadMutexUnlock(&pMgmt->lock);

  mndUpdateStreamExecInfoRole(pMnode, NODE_ROLE_FOLLOWER);
}

static void mndBecomeLearner(const SSyncFSM *pFsm) {
  SMnode    *pMnode = pFsm->data;
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  mInfo("vgId:1, become learner");

  (void)taosThreadMutexLock(&pMgmt->lock);
  if (pMgmt->transId != 0) {
    mInfo("vgId:1, become learner and post sem, trans:%d, failed to propose since not leader", pMgmt->transId);
    pMgmt->transId = 0;
    pMgmt->transSec = 0;
    pMgmt->transSeq = 0;
    pMgmt->errCode = TSDB_CODE_SYN_NOT_LEADER;
    (void)tsem_post(&pMgmt->syncSem);
  }
  (void)taosThreadMutexUnlock(&pMgmt->lock);
}

static void mndBecomeLeader(const SSyncFSM *pFsm) {
  mInfo("vgId:1, become leader");
  SMnode *pMnode = pFsm->data;

  mndUpdateStreamExecInfoRole(pMnode, NODE_ROLE_LEADER);
  mndStreamResetInitTaskListLoadFlag();
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
  pFsm->FpAppliedIndexCb = mndSyncAppliedIndex;
  pFsm->FpPreCommitCb = NULL;
  pFsm->FpRollBackCb = NULL;
  pFsm->FpRestoreFinishCb = mndRestoreFinish;
  pFsm->FpLeaderTransferCb = NULL;
  pFsm->FpApplyQueueEmptyCb = mndApplyQueueEmpty;
  pFsm->FpApplyQueueItems = mndApplyQueueItems;
  pFsm->FpReConfigCb = NULL;
  pFsm->FpBecomeLeaderCb = mndBecomeLeader;
  pFsm->FpBecomeAssignedLeaderCb = NULL;
  pFsm->FpBecomeFollowerCb = mndBecomeFollower;
  pFsm->FpBecomeLearnerCb = mndBecomeLearner;
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
  (void)taosThreadMutexInit(&pMgmt->lock, NULL);
  (void)taosThreadMutexLock(&pMgmt->lock);
  pMgmt->transId = 0;
  pMgmt->transSec = 0;
  pMgmt->transSeq = 0;
  (void)taosThreadMutexUnlock(&pMgmt->lock);

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

  mInfo("vgId:1, start to open mnode sync, replica:%d selfIndex:%d", pMgmt->numOfReplicas, pMgmt->selfIndex);
  SSyncCfg *pCfg = &syncInfo.syncCfg;
  pCfg->totalReplicaNum = pMgmt->numOfTotalReplicas;
  pCfg->replicaNum = pMgmt->numOfReplicas;
  pCfg->myIndex = pMgmt->selfIndex;
  pCfg->lastIndex = pMgmt->lastIndex;
  for (int32_t i = 0; i < pMgmt->numOfTotalReplicas; ++i) {
    SNodeInfo *pNode = &pCfg->nodeInfo[i];
    pNode->nodeId = pMgmt->replicas[i].id;
    pNode->nodePort = pMgmt->replicas[i].port;
    tstrncpy(pNode->nodeFqdn, pMgmt->replicas[i].fqdn, sizeof(pNode->nodeFqdn));
    pNode->nodeRole = pMgmt->nodeRoles[i];
    (void)tmsgUpdateDnodeInfo(&pNode->nodeId, &pNode->clusterId, pNode->nodeFqdn, &pNode->nodePort);
    mInfo("vgId:1, index:%d ep:%s:%u dnode:%d cluster:%" PRId64, i, pNode->nodeFqdn, pNode->nodePort, pNode->nodeId,
          pNode->clusterId);
  }

  int32_t code = 0;
  (void)tsem_init(&pMgmt->syncSem, 0, 0);
  pMgmt->sync = syncOpen(&syncInfo, 1); // always check
  if (pMgmt->sync <= 0) {
    if (terrno != 0) code = terrno;
    mError("failed to open sync since %s", tstrerror(code));
    TAOS_RETURN(code);
  }
  pMnode->pSdb->sync = pMgmt->sync;

  mInfo("vgId:1, mnode sync is opened, id:%" PRId64, pMgmt->sync);
  TAOS_RETURN(code);
}

void mndCleanupSync(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  syncStop(pMgmt->sync);
  mInfo("mnode-sync is stopped, id:%" PRId64, pMgmt->sync);

  (void)tsem_destroy(&pMgmt->syncSem);
  (void)taosThreadMutexDestroy(&pMgmt->lock);
  memset(pMgmt, 0, sizeof(SSyncMgmt));
}

void mndSyncCheckTimeout(SMnode *pMnode) {
  mTrace("check sync timeout");
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  (void)taosThreadMutexLock(&pMgmt->lock);
  if (pMgmt->transId != 0) {
    int32_t curSec = taosGetTimestampSec();
    int32_t delta = curSec - pMgmt->transSec;
    if (delta > MNODE_TIMEOUT_SEC) {
      mError("trans:%d, failed to propose since timeout, start:%d cur:%d delta:%d seq:%" PRId64, pMgmt->transId,
             pMgmt->transSec, curSec, delta, pMgmt->transSeq);
      pMgmt->transId = 0;
      pMgmt->transSec = 0;
      pMgmt->transSeq = 0;
      terrno = TSDB_CODE_SYN_TIMEOUT;
      pMgmt->errCode = TSDB_CODE_SYN_TIMEOUT;
      (void)tsem_post(&pMgmt->syncSem);
    } else {
      mDebug("trans:%d, waiting for sync confirm, start:%d cur:%d delta:%d seq:%" PRId64, pMgmt->transId,
             pMgmt->transSec, curSec, curSec - pMgmt->transSec, pMgmt->transSeq);
    }
  } else {
    // mTrace("check sync timeout msg, no trans waiting for confirm");
  }
  (void)taosThreadMutexUnlock(&pMgmt->lock);
}

int32_t mndSyncPropose(SMnode *pMnode, SSdbRaw *pRaw, int32_t transId) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;

  SRpcMsg req = {.msgType = TDMT_MND_APPLY_MSG, .contLen = sdbGetRawTotalSize(pRaw)};
  if (req.contLen <= 0) return TSDB_CODE_OUT_OF_MEMORY;

  req.pCont = rpcMallocCont(req.contLen);
  if (req.pCont == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  memcpy(req.pCont, pRaw, req.contLen);

  (void)taosThreadMutexLock(&pMgmt->lock);
  pMgmt->errCode = 0;

  if (pMgmt->transId != 0) {
    mError("trans:%d, can't be proposed since trans:%d already waiting for confirm", transId, pMgmt->transId);
    (void)taosThreadMutexUnlock(&pMgmt->lock);
    rpcFreeCont(req.pCont);
    TAOS_RETURN(TSDB_CODE_MND_LAST_TRANS_NOT_FINISHED);
  }

  mInfo("trans:%d, will be proposed", transId);
  pMgmt->transId = transId;
  pMgmt->transSec = taosGetTimestampSec();

  int64_t seq = 0;
  int32_t code = syncPropose(pMgmt->sync, &req, false, &seq);
  if (code == 0) {
    mInfo("trans:%d, is proposing and wait sem, seq:%" PRId64, transId, seq);
    pMgmt->transSeq = seq;
    (void)taosThreadMutexUnlock(&pMgmt->lock);
    (void)tsem_wait(&pMgmt->syncSem);
  } else if (code > 0) {
    mInfo("trans:%d, confirm at once since replica is 1, continue execute", transId);
    pMgmt->transId = 0;
    pMgmt->transSec = 0;
    pMgmt->transSeq = 0;
    (void)taosThreadMutexUnlock(&pMgmt->lock);
    code = sdbWriteWithoutFree(pMnode->pSdb, pRaw);
    if (code == 0) {
      sdbSetApplyInfo(pMnode->pSdb, req.info.conn.applyIndex, req.info.conn.applyTerm, SYNC_INDEX_INVALID);
    }
  } else {
    mError("trans:%d, failed to proposed since %s", transId, terrstr());
    pMgmt->transId = 0;
    pMgmt->transSec = 0;
    pMgmt->transSeq = 0;
    (void)taosThreadMutexUnlock(&pMgmt->lock);
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
  return terrno;
}

void mndSyncStart(SMnode *pMnode) {
  mInfo("vgId:1, start to start mnode sync");
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  if (syncStart(pMgmt->sync) < 0) {
    mError("vgId:1, failed to start sync, id:%" PRId64, pMgmt->sync);
    return;
  }
  mInfo("vgId:1, mnode sync started, id:%" PRId64, pMgmt->sync);
}

void mndSyncStop(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;

  (void)taosThreadMutexLock(&pMgmt->lock);
  if (pMgmt->transId != 0) {
    mInfo("vgId:1, is stopped and post sem, trans:%d", pMgmt->transId);
    pMgmt->transId = 0;
    pMgmt->transSec = 0;
    pMgmt->errCode = TSDB_CODE_APP_IS_STOPPING;
    (void)tsem_post(&pMgmt->syncSem);
  }
  (void)taosThreadMutexUnlock(&pMgmt->lock);
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
