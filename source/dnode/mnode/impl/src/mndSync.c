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

int32_t mndSyncEqMsg(const SMsgCb *msgcb, SRpcMsg *pMsg) { return tmsgPutToQueue(msgcb, SYNC_QUEUE, pMsg); }

int32_t mndSyncSendMsg(const SEpSet *pEpSet, SRpcMsg *pMsg) { return tmsgSendReq(pEpSet, pMsg); }

void mndSyncCommitMsg(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  SMnode    *pMnode = pFsm->data;
  SSdb      *pSdb = pMnode->pSdb;
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  SSdbRaw   *pRaw = pMsg->pCont;

  SSnapshot snapshot = {0};
  (*pFsm->FpGetSnapshot)(pFsm, &snapshot);

  if (cbMeta.index > snapshot.lastApplyIndex) {
    mTrace("ver:%" PRId64 ", apply raw:%p to sdb, role:%s", cbMeta.index, pRaw, syncStr(cbMeta.state));
    sdbWriteWithoutFree(pSdb, pRaw);
    sdbSetApplyIndex(pSdb, cbMeta.index);
    if (cbMeta.state == TAOS_SYNC_STATE_LEADER) {
      tsem_post(&pMgmt->syncSem);
    }
  } else {
    mTrace("ver:%" PRId64 ", already apply raw:%p to sdb, last:%" PRId64, cbMeta.index, pRaw, snapshot.lastApplyIndex);
  }
}

static void mndSyncPreCommitMsg(SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  // strict consistent, do nothing
}

static void mndSyncRollBackMsg(SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  // strict consistent, do nothing
}

static int32_t mndSyncGetSnapshot(SSyncFSM *pFsm, SSnapshot *pSnapshot) {
  SMnode *pMnode = pFsm->data;
  pSnapshot->lastApplyIndex = sdbGetApplyIndex(pMnode->pSdb);
  return 0;
}

SSyncFSM *mndSyncMakeFsm(SMnode *pMnode) {
  SSyncFSM *pFsm = taosMemoryCalloc(1, sizeof(SSyncFSM));
  pFsm->data = pMnode;
  pFsm->FpCommitCb = mndSyncCommitMsg;
  pFsm->FpPreCommitCb = mndSyncPreCommitMsg;
  pFsm->FpRollBackCb = mndSyncRollBackMsg;
  pFsm->FpGetSnapshot = mndSyncGetSnapshot;
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
  for (int32_t i = 0; i < pMnode->replica; ++i) {
    SNodeInfo *pNode = &pCfg->nodeInfo[i];
    tstrncpy(pNode->nodeFqdn, pMnode->replicas[i].fqdn, sizeof(pNode->nodeFqdn));
    pNode->nodePort = pMnode->replicas[i].port;
  }

  tsem_init(&pMgmt->syncSem, 0, 0);
  pMgmt->sync = syncOpen(&syncInfo);
  if (pMgmt->sync <= 0) {
    mError("failed to open sync since %s", terrstr());
    return -1;
  }

  return 0;
}

void mndCleanupSync(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
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

  if (code != 0) return code;
  return pMgmt->errCode;
}

void mndSyncStart(SMnode *pMnode) {
  SSdb   *pSdb = pMnode->pSdb;
  int64_t lastApplyIndex = sdbGetApplyIndex(pSdb);

  syncSetMsgCb(pMnode->syncMgmt.sync, &pMnode->msgCb);
  syncStart(pMnode->syncMgmt.sync);

  int64_t applyIndex = sdbGetApplyIndex(pSdb);
  mndTransPullup(pMnode);
  mDebug("pullup trans finished, applyIndex:%" PRId64, applyIndex);
  if (applyIndex != lastApplyIndex) {
    mInfo("sdb restored from %" PRId64 " to %" PRId64 ", write file", lastApplyIndex, applyIndex);
    sdbWriteFile(pSdb);
  }

  pMnode->syncMgmt.restored = true;
}

void mndSyncStop(SMnode *pMnode) { syncStop(pMnode->syncMgmt.sync); }

bool mndIsMaster(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  pMgmt->state = syncGetMyRole(pMgmt->sync);
  return pMgmt->state == TAOS_SYNC_STATE_LEADER;
}

bool mndIsRestored(SMnode *pMnode) { return pMnode->syncMgmt.restored; }