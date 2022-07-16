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

static int32_t mndSyncEqMsg(const SMsgCb *msgcb, SRpcMsg *pMsg) {
  SMsgHead *pHead = pMsg->pCont;
  pHead->contLen = htonl(pHead->contLen);
  pHead->vgId = htonl(pHead->vgId);

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

  int32_t transId = sdbGetIdFromRaw(pMnode->pSdb, pRaw);
  pMgmt->errCode = cbMeta.code;
  mDebug("trans:%d, is proposed, saved:%d code:0x%x, apply index:%" PRId64 " term:%" PRIu64 " config:%" PRId64
         " role:%s raw:%p",
         transId, pMgmt->transId, cbMeta.code, cbMeta.index, cbMeta.term, cbMeta.lastConfigIndex, syncStr(cbMeta.state),
         pRaw);

  if (pMgmt->errCode == 0) {
    sdbWriteWithoutFree(pMnode->pSdb, pRaw);
    sdbSetApplyInfo(pMnode->pSdb, cbMeta.index, cbMeta.term, cbMeta.lastConfigIndex);
  }

  if (transId <= 0) {
    mError("trans:%d, invalid commit msg", transId);
  } else if (transId == pMgmt->transId) {
    if (pMgmt->errCode != 0) {
      mError("trans:%d, failed to propose since %s", transId, tstrerror(pMgmt->errCode));
    }
    pMgmt->transId = 0;
    tsem_post(&pMgmt->syncSem);
  } else {
    STrans *pTrans = mndAcquireTrans(pMnode, transId);
    if (pTrans != NULL) {
      mDebug("trans:%d, execute in mnode which not leader", transId);
      mndTransExecute(pMnode, pTrans);
      mndReleaseTrans(pMnode, pTrans);
      // sdbWriteFile(pMnode->pSdb, SDB_WRITE_DELTA);
    } else {
      mError("trans:%d, not found while execute in mnode since %s", transId, terrstr());
    }
  }
}

int32_t mndSyncGetSnapshot(struct SSyncFSM *pFsm, SSnapshot *pSnapshot, void *pReaderParam, void **ppReader) {
  mDebug("start to read snapshot from sdb in atomic way");
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
    mInfo("mnode sync restore finished, and will handle outstanding transactions");
    mndTransPullup(pMnode);
    mndSetRestore(pMnode, true);
  } else {
    mInfo("mnode sync restore finished");
  }
}

void mndReConfig(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SReConfigCbMeta cbMeta) {
  SMnode    *pMnode = pFsm->data;
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;

  pMgmt->errCode = cbMeta.code;
  mInfo("trans:-1, sync reconfig is proposed, saved:%d code:0x%x, index:%" PRId64 " term:%" PRId64, pMgmt->transId,
        cbMeta.code, cbMeta.index, cbMeta.term);

  if (pMgmt->transId == -1) {
    if (pMgmt->errCode != 0) {
      mError("trans:-1, failed to propose sync reconfig since %s", tstrerror(pMgmt->errCode));
    }
    pMgmt->transId = 0;
    tsem_post(&pMgmt->syncSem);
  }
}

int32_t mndSnapshotStartRead(struct SSyncFSM *pFsm, void *pParam, void **ppReader) {
  mDebug("start to read snapshot from sdb");
  SMnode *pMnode = pFsm->data;
  return sdbStartRead(pMnode->pSdb, (SSdbIter **)ppReader, NULL, NULL, NULL);
}

int32_t mndSnapshotStopRead(struct SSyncFSM *pFsm, void *pReader) {
  mDebug("stop to read snapshot from sdb");
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

int32_t mndSnapshotStopWrite(struct SSyncFSM *pFsm, void *pWriter, bool isApply) {
  mInfo("stop to apply snapshot to sdb, apply:%d", isApply);
  SMnode *pMnode = pFsm->data;
  return sdbStopWrite(pMnode->pSdb, pWriter, isApply);
}

int32_t mndSnapshotDoWrite(struct SSyncFSM *pFsm, void *pWriter, void *pBuf, int32_t len) {
  SMnode *pMnode = pFsm->data;
  return sdbDoWrite(pMnode->pSdb, pWriter, pBuf, len);
}

void mndLeaderTransfer(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  SMnode *pMnode = pFsm->data;
  atomic_store_8(&(pMnode->syncMgmt.leaderTransferFinish), 1);
  mDebug("vgId:1, mnd leader transfer finish");
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

  SSyncInfo syncInfo = {.vgId = 1, .FpSendMsg = mndSyncSendMsg, .FpEqMsg = mndSyncEqMsg};
  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s%ssync", pMnode->path, TD_DIRSEP);
  syncInfo.pWal = pMnode->pWal;
  syncInfo.pFsm = mndSyncMakeFsm(pMnode);
  syncInfo.isStandBy = pMgmt->standby;
  syncInfo.snapshotStrategy = SYNC_STRATEGY_STANDARD_SNAPSHOT;

  mInfo("start to open mnode sync, standby:%d", pMgmt->standby);
  if (pMgmt->standby || pMgmt->replica.id > 0) {
    SSyncCfg *pCfg = &syncInfo.syncCfg;
    pCfg->replicaNum = 1;
    pCfg->myIndex = 0;
    SNodeInfo *pNode = &pCfg->nodeInfo[0];
    tstrncpy(pNode->nodeFqdn, pMgmt->replica.fqdn, sizeof(pNode->nodeFqdn));
    pNode->nodePort = pMgmt->replica.port;
    mInfo("mnode ep:%s:%u", pNode->nodeFqdn, pNode->nodePort);
  }

  tsem_init(&pMgmt->syncSem, 0, 0);
  pMgmt->sync = syncOpen(&syncInfo);
  if (pMgmt->sync <= 0) {
    mError("failed to open sync since %s", terrstr());
    return -1;
  }

  // decrease election timer
  setPingTimerMS(pMgmt->sync, 5000);
  setElectTimerMS(pMgmt->sync, 600);
  setHeartbeatTimerMS(pMgmt->sync, 300);

  mDebug("mnode-sync is opened, id:%" PRId64, pMgmt->sync);
  return 0;
}

void mndCleanupSync(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  syncStop(pMgmt->sync);
  mDebug("mnode-sync is stopped, id:%" PRId64, pMgmt->sync);

  tsem_destroy(&pMgmt->syncSem);
  memset(pMgmt, 0, sizeof(SSyncMgmt));
}

int32_t mndSyncPropose(SMnode *pMnode, SSdbRaw *pRaw, int32_t transId) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  SRpcMsg    req = {.msgType = TDMT_MND_APPLY_MSG, .contLen = sdbGetRawTotalSize(pRaw)};
  req.pCont = rpcMallocCont(req.contLen);
  if (req.pCont == NULL) return -1;
  memcpy(req.pCont, pRaw, req.contLen);

  pMgmt->errCode = 0;
  pMgmt->transId = transId;
  mTrace("trans:%d, will be proposed", pMgmt->transId);

  const bool isWeak = false;
  int32_t    code = syncPropose(pMgmt->sync, &req, isWeak);
  if (code == 0) {
    tsem_wait(&pMgmt->syncSem);
  } else if (code == -1 && terrno == TSDB_CODE_SYN_NOT_LEADER) {
    terrno = TSDB_CODE_APP_NOT_READY;
  } else if (code == -1 && terrno == TSDB_CODE_SYN_INTERNAL_ERROR) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
  } else {
    terrno = TSDB_CODE_APP_ERROR;
  }

  rpcFreeCont(req.pCont);
  if (code != 0) {
    mError("trans:%d, failed to propose, code:0x%x", pMgmt->transId, code);
    return code;
  }

  return pMgmt->errCode;
}

void mndSyncStart(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  syncSetMsgCb(pMgmt->sync, &pMnode->msgCb);
  syncStart(pMgmt->sync);
  mDebug("mnode sync started, id:%" PRId64 " standby:%d", pMgmt->sync, pMgmt->standby);
}

void mndSyncStop(SMnode *pMnode) {
  if (pMnode->syncMgmt.transId != 0) {
    pMnode->syncMgmt.transId = 0;
    tsem_post(&pMnode->syncMgmt.syncSem);
  }
}

bool mndIsMaster(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;

  if (!syncIsReady(pMgmt->sync)) {
    // get terrno from syncIsReady
    // terrno = TSDB_CODE_SYN_NOT_LEADER;
    return false;
  }

  if (!pMnode->restored) {
    terrno = TSDB_CODE_APP_NOT_READY;
    return false;
  }

  return true;
}
