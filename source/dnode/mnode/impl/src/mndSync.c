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

int32_t mndSyncEqMsg(const SMsgCb *msgcb, SRpcMsg *pMsg) { 
  SMsgHead *pHead = pMsg->pCont;
  pHead->contLen = htonl(pHead->contLen);
  pHead->vgId = htonl(pHead->vgId);

  return tmsgPutToQueue(msgcb, SYNC_QUEUE, pMsg); 
}

int32_t mndSyncSendMsg(const SEpSet *pEpSet, SRpcMsg *pMsg) { return tmsgSendReq(pEpSet, pMsg); }

void mndSyncCommitMsg(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  SMnode  *pMnode = pFsm->data;
  SSdbRaw *pRaw = pMsg->pCont;

  mTrace("raw:%p, apply to sdb, ver:%" PRId64 " role:%s", pRaw, cbMeta.index, syncStr(cbMeta.state));
  sdbWriteWithoutFree(pMnode->pSdb, pRaw);
  sdbSetApplyIndex(pMnode->pSdb, cbMeta.index);
  sdbSetApplyTerm(pMnode->pSdb, cbMeta.term);
  if (cbMeta.state == TAOS_SYNC_STATE_LEADER) {
    tsem_post(&pMnode->syncMgmt.syncSem);
  }
}

int32_t mndSyncGetSnapshot(struct SSyncFSM *pFsm, SSnapshot *pSnapshot) {
  SMnode *pMnode = pFsm->data;
  pSnapshot->lastApplyIndex = sdbGetApplyIndex(pMnode->pSdb);
  pSnapshot->lastApplyTerm = sdbGetApplyTerm(pMnode->pSdb);
  return 0;
}

void mndRestoreFinish(struct SSyncFSM *pFsm) {
  SMnode *pMnode = pFsm->data;
  if (!pMnode->deploy) {
    mndTransPullup(pMnode);
    pMnode->syncMgmt.restored = true;
  }
}

int32_t mndSnapshotRead(struct SSyncFSM* pFsm, const SSnapshot* pSnapshot, void** ppIter, char** ppBuf, int32_t* len) {
  /*
  SMnode *pMnode = pFsm->data;
  SSdbIter *pIter;
  if (iter == NULL) { 
    pIter = sdbIterInit(pMnode->sdb)
  } else {
    pIter = iter;
  }
  */

  return 0;
}

int32_t mndSnapshotApply(struct SSyncFSM* pFsm, const SSnapshot* pSnapshot, char* pBuf, int32_t len) {
  SMnode *pMnode = pFsm->data;
  sdbWrite(pMnode->pSdb, (SSdbRaw*)pBuf);
  return 0;
}

void mndReConfig(struct SSyncFSM *pFsm, SSyncCfg newCfg, SReConfigCbMeta cbMeta) {
  mInfo("mndReConfig cbMeta.code:%d, cbMeta.currentTerm:%" PRId64 ", cbMeta.term:%" PRId64 ", cbMeta.index:%" PRId64,
        cbMeta.code, cbMeta.currentTerm, cbMeta.term, cbMeta.index);
  SMnode *pMnode = pFsm->data;
  pMnode->syncMgmt.errCode = cbMeta.code;
  tsem_post(&pMnode->syncMgmt.syncSem);
}

SSyncFSM *mndSyncMakeFsm(SMnode *pMnode) {
  SSyncFSM *pFsm = taosMemoryCalloc(1, sizeof(SSyncFSM));
  pFsm->data = pMnode;

  pFsm->FpCommitCb = mndSyncCommitMsg;
  pFsm->FpPreCommitCb = NULL;
  pFsm->FpRollBackCb = NULL;

  pFsm->FpGetSnapshot = mndSyncGetSnapshot;
  pFsm->FpRestoreFinishCb = mndRestoreFinish;
  pFsm->FpSnapshotRead = mndSnapshotRead;
  pFsm->FpSnapshotApply = mndSnapshotApply;
  pFsm->FpReConfigCb = mndReConfig;
  
  return pFsm;
}

int32_t mndInitSync(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;

  char path[PATH_MAX + 20] = {0};
  snprintf(path, sizeof(path), "%s%swal", pMnode->path, TD_DIRSEP);
  SWalCfg cfg = {
      .vgId = 1,
      .fsyncPeriod = 0,
      .rollPeriod = -1,
      .segSize = -1,
      .retentionPeriod = -1,
      .retentionSize = -1,
      .level = TAOS_WAL_FSYNC,
  };

  pMgmt->pWal = walOpen(path, &cfg);
  if (pMgmt->pWal == NULL) {
    mError("failed to open wal since %s", terrstr());
    return -1;
  }

  SSyncInfo syncInfo = {.vgId = 1, .FpSendMsg = mndSyncSendMsg, .FpEqMsg = mndSyncEqMsg};
  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s%ssync", pMnode->path, TD_DIRSEP);
  syncInfo.pWal = pMgmt->pWal;
  syncInfo.pFsm = mndSyncMakeFsm(pMnode);

  SSyncCfg *pCfg = &syncInfo.syncCfg;
  pCfg->replicaNum = pMnode->replica;
  pCfg->myIndex = pMnode->selfIndex;
  mInfo("start to open mnode sync, replica:%d myindex:%d standby:%d", pCfg->replicaNum, pCfg->myIndex,
        pMgmt->standby);
  for (int32_t i = 0; i < pMnode->replica; ++i) {
    SNodeInfo *pNode = &pCfg->nodeInfo[i];
    tstrncpy(pNode->nodeFqdn, pMnode->replicas[i].fqdn, sizeof(pNode->nodeFqdn));
    pNode->nodePort = pMnode->replicas[i].port;
    mInfo("index:%d, fqdn:%s port:%d", i, pNode->nodeFqdn, pNode->nodePort);
  }

  tsem_init(&pMgmt->syncSem, 0, 0);
  pMgmt->sync = syncOpen(&syncInfo);
  if (pMgmt->sync <= 0) {
    mError("failed to open sync since %s", terrstr());
    return -1;
  }

  mDebug("mnode sync is opened, id:%" PRId64, pMgmt->sync);
  return 0;
}

void mndCleanupSync(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  syncStop(pMgmt->sync);
  mDebug("sync:%" PRId64 " is stopped", pMgmt->sync);

  tsem_destroy(&pMgmt->syncSem);
  if (pMgmt->pWal != NULL) {
    walClose(pMgmt->pWal);
  }

  memset(pMgmt, 0, sizeof(SSyncMgmt));
}

int32_t mndSyncPropose(SMnode *pMnode, SSdbRaw *pRaw) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  pMgmt->errCode = 0;

  SRpcMsg rsp = {.code = TDMT_MND_APPLY_MSG, .contLen = sdbGetRawTotalSize(pRaw)};
  rsp.pCont = rpcMallocCont(rsp.contLen);
  if (rsp.pCont == NULL) return -1;
  memcpy(rsp.pCont, pRaw, rsp.contLen);

  const bool isWeak = false;
  int32_t    code = syncPropose(pMgmt->sync, &rsp, isWeak);
  if (code == 0) {
    tsem_wait(&pMgmt->syncSem);
  } else if (code == TAOS_SYNC_PROPOSE_NOT_LEADER) {
    terrno = TSDB_CODE_APP_NOT_READY;
  } else if (code == TAOS_SYNC_PROPOSE_OTHER_ERROR) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
  } else {
    terrno = TSDB_CODE_APP_ERROR;
  }

  rpcFreeCont(rsp.pCont);
  if (code != 0) return code;
  return pMgmt->errCode;
}

void mndSyncStart(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  syncSetMsgCb(pMgmt->sync, &pMnode->msgCb);
  if (pMgmt->standby) {
    syncStartStandBy(pMgmt->sync);
  } else {
    syncStart(pMgmt->sync);
  }
  mDebug("sync:%" PRId64 " is started", pMgmt->sync);
}

void mndSyncStop(SMnode *pMnode) {}

bool mndIsMaster(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  ESyncState state = syncGetMyRole(pMgmt->sync);
  return (state == TAOS_SYNC_STATE_LEADER) && (pMnode->syncMgmt.restored);
}
