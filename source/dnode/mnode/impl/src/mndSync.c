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
  mDebug("trans:%d, is proposed, saved:%d code:0x%x, index:%" PRId64 " term:%" PRId64 " role:%s raw:%p", transId,
         pMgmt->transId, cbMeta.code, cbMeta.index, cbMeta.term, syncStr(cbMeta.state), pRaw);

  if (pMgmt->errCode == 0) {
    sdbWriteWithoutFree(pMnode->pSdb, pRaw);
    sdbSetApplyIndex(pMnode->pSdb, cbMeta.index);
    sdbSetApplyTerm(pMnode->pSdb, cbMeta.term);
  }

  if (pMgmt->transId == transId) {
    if (pMgmt->errCode != 0) {
      mError("trans:%d, failed to propose since %s", transId, tstrerror(pMgmt->errCode));
    }
    tsem_post(&pMgmt->syncSem);
  } else {
    STrans *pTrans = mndAcquireTrans(pMnode, transId);
    if (pTrans != NULL) {
      mndTransExecute(pMnode, pTrans);
      mndReleaseTrans(pMnode, pTrans);
    }

    if (cbMeta.index - sdbGetApplyIndex(pMnode->pSdb) > 100) {
      SSnapshotMeta sMeta = {0};
      if (syncGetSnapshotMeta(pMnode->syncMgmt.sync, &sMeta) == 0) {
        sdbSetCurConfig(pMnode->pSdb, sMeta.lastConfigIndex);
      }
      sdbWriteFile(pMnode->pSdb);
    }
  }
}

int32_t mndSyncGetSnapshot(struct SSyncFSM *pFsm, SSnapshot *pSnapshot) {
  SMnode *pMnode = pFsm->data;
  pSnapshot->lastApplyIndex = sdbGetCommitIndex(pMnode->pSdb);
  pSnapshot->lastApplyTerm = sdbGetCommitTerm(pMnode->pSdb);
  pSnapshot->lastConfigIndex = sdbGetCurConfig(pMnode->pSdb);
  return 0;
}

void mndRestoreFinish(struct SSyncFSM *pFsm) {
  SMnode *pMnode = pFsm->data;

  SSnapshotMeta sMeta = {0};
  if (syncGetSnapshotMeta(pMnode->syncMgmt.sync, &sMeta) == 0) {
    sdbSetCurConfig(pMnode->pSdb, sMeta.lastConfigIndex);
  }

  if (!pMnode->deploy) {
    mInfo("mnode sync restore finished, and will handle outstanding transactions");
    mndTransPullup(pMnode);
    mndSetRestore(pMnode, true);
  } else {
    mInfo("mnode sync restore finished, and will set ready after first deploy");
  }
}

void mndReConfig(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SReConfigCbMeta cbMeta) {
  SMnode    *pMnode = pFsm->data;
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;

#if 0
// send response
  SRpcMsg rpcMsg = {.msgType = pMsg->msgType, .contLen = pMsg->contLen, .conn.applyIndex = cbMeta.index};
  rpcMsg.pCont = rpcMallocCont(rpcMsg.contLen);
  memcpy(rpcMsg.pCont, pMsg->pCont, pMsg->contLen);
  syncGetAndDelRespRpc(pMnode->syncMgmt.sync, cbMeta.seqNum, &rpcMsg.info);
#endif

  pMgmt->errCode = cbMeta.code;
  mInfo("trans:-1, sync reconfig is proposed, saved:%d code:0x%x, index:%" PRId64 " term:%" PRId64, pMgmt->transId,
        cbMeta.code, cbMeta.index, cbMeta.term);

  if (pMgmt->transId == -1) {
    if (pMgmt->errCode != 0) {
      mError("trans:-1, failed to propose sync reconfig since %s", tstrerror(pMgmt->errCode));
    }
    tsem_post(&pMgmt->syncSem);
  }
}

int32_t mndSnapshotStartRead(struct SSyncFSM *pFsm, void **ppReader) {
  mInfo("start to read snapshot from sdb");
  SMnode *pMnode = pFsm->data;
  return sdbStartRead(pMnode->pSdb, (SSdbIter **)ppReader);
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

int32_t mndSnapshotStartWrite(struct SSyncFSM *pFsm, void **ppWriter) {
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

SSyncFSM *mndSyncMakeFsm(SMnode *pMnode) {
  SSyncFSM *pFsm = taosMemoryCalloc(1, sizeof(SSyncFSM));
  pFsm->data = pMnode;
  pFsm->FpCommitCb = mndSyncCommitMsg;
  pFsm->FpPreCommitCb = NULL;
  pFsm->FpRollBackCb = NULL;
  pFsm->FpRestoreFinishCb = mndRestoreFinish;
  pFsm->FpReConfigCb = mndReConfig;
  pFsm->FpGetSnapshot = mndSyncGetSnapshot;
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
  syncInfo.snapshotEnable = true;

  mInfo("start to open mnode sync, standby:%d", pMgmt->standby);
  if (pMgmt->standby || pMgmt->replica.id > 0) {
    SSyncCfg *pCfg = &syncInfo.syncCfg;
    pCfg->replicaNum = 1;
    pCfg->myIndex = 0;
    SNodeInfo *pNode = &pCfg->nodeInfo[0];
    tstrncpy(pNode->nodeFqdn, pMgmt->replica.fqdn, sizeof(pNode->nodeFqdn));
    pNode->nodePort = pMgmt->replica.port;
    mInfo("fqdn:%s port:%u", pNode->nodeFqdn, pNode->nodePort);
  }

  tsem_init(&pMgmt->syncSem, 0, 0);
  pMgmt->sync = syncOpen(&syncInfo);
  if (pMgmt->sync <= 0) {
    mError("failed to open sync since %s", terrstr());
    return -1;
  }

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
  SRpcMsg    rsp = {.code = TDMT_MND_APPLY_MSG, .contLen = sdbGetRawTotalSize(pRaw)};
  rsp.pCont = rpcMallocCont(rsp.contLen);
  if (rsp.pCont == NULL) return -1;
  memcpy(rsp.pCont, pRaw, rsp.contLen);

  pMgmt->errCode = 0;
  pMgmt->transId = transId;
  mTrace("trans:%d, will be proposed", pMgmt->transId);

  const bool isWeak = false;
  int32_t    code = syncPropose(pMgmt->sync, &rsp, isWeak);
  if (code == 0) {
    tsem_wait(&pMgmt->syncSem);
  } else if (code == -1 && terrno == TSDB_CODE_SYN_NOT_LEADER) {
    terrno = TSDB_CODE_APP_NOT_READY;
  } else if (code == -1 && terrno == TSDB_CODE_SYN_INTERNAL_ERROR) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
  } else {
    terrno = TSDB_CODE_APP_ERROR;
  }

  rpcFreeCont(rsp.pCont);
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

void mndSyncStop(SMnode *pMnode) {}

bool mndIsMaster(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;

  ESyncState state = syncGetMyRole(pMgmt->sync);
  if (state != TAOS_SYNC_STATE_LEADER) {
    terrno = TSDB_CODE_SYN_NOT_LEADER;
    return false;
  }

  if (!pMnode->restored) {
    terrno = TSDB_CODE_APP_NOT_READY;
    return false;
  }

  return true;
}
