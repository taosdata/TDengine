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

static int32_t mndInitWal(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;

  char path[PATH_MAX] = {0};
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
  if (pMgmt->pWal == NULL) return -1;

  return 0;
}

static void mndCloseWal(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  if (pMgmt->pWal != NULL) {
    walClose(pMgmt->pWal);
    pMgmt->pWal = NULL;
  }
}

static int32_t mndRestoreWal(SMnode *pMnode) {
  // do nothing
  return 0;

#if 0

  SWal   *pWal = pMnode->syncMgmt.pWal;
  SSdb   *pSdb = pMnode->pSdb;
  int64_t lastSdbVer = sdbUpdateVer(pSdb, 0);
  int32_t code = -1;

  SWalReadHandle *pHandle = walOpenReadHandle(pWal);
  if (pHandle == NULL) return -1;

  int64_t first = walGetFirstVer(pWal);
  int64_t last = walGetLastVer(pWal);
  mDebug("start to restore wal, sdbver:%" PRId64 ", first:%" PRId64 " last:%" PRId64, lastSdbVer, first, last);

  first = TMAX(lastSdbVer + 1, first);
  for (int64_t ver = first; ver >= 0 && ver <= last; ++ver) {
    if (walReadWithHandle(pHandle, ver) < 0) {
      mError("ver:%" PRId64 ", failed to read from wal since %s", ver, terrstr());
      goto _OVER;
    }

    SWalHead *pHead = pHandle->pHead;
    int64_t   sdbVer = sdbUpdateVer(pSdb, 0);
    if (sdbVer + 1 != ver) {
      terrno = TSDB_CODE_SDB_INVALID_WAl_VER;
      mError("ver:%" PRId64 ", failed to write to sdb, since inconsistent with sdbver:%" PRId64, ver, sdbVer);
      goto _OVER;
    }

    mTrace("ver:%" PRId64 ", will be restored, content:%p", ver, pHead->head.body);
    if (sdbWriteWithoutFree(pSdb, (void *)pHead->head.body) < 0) {
      mError("ver:%" PRId64 ", failed to write to sdb since %s", ver, terrstr());
      goto _OVER;
    }

    sdbUpdateVer(pSdb, 1);
    mDebug("ver:%" PRId64 ", is restored", ver);
  }

  int64_t sdbVer = sdbUpdateVer(pSdb, 0);
  mDebug("restore wal finished, sdbver:%" PRId64, sdbVer);

  mndTransPullup(pMnode);
  sdbVer = sdbUpdateVer(pSdb, 0);
  mDebug("pullup trans finished, sdbver:%" PRId64, sdbVer);

  if (sdbVer != lastSdbVer) {
    mInfo("sdb restored from %" PRId64 " to %" PRId64 ", write file", lastSdbVer, sdbVer);
    if (sdbWriteFile(pSdb) != 0) {
      goto _OVER;
    }

    if (walCommit(pWal, sdbVer) != 0) {
      goto _OVER;
    }

    if (walBeginSnapshot(pWal, sdbVer) < 0) {
      goto _OVER;
    }

    if (walEndSnapshot(pWal) < 0) {
      goto _OVER;
    }
  }

  code = 0;

_OVER:
  walCloseReadHandle(pHandle);
  return code;

#endif
}

int32_t mndSyncEqMsg(const SMsgCb *msgcb, SRpcMsg *pMsg) { return tmsgPutToQueue(msgcb, SYNC_QUEUE, pMsg); }

int32_t mndSyncSendMsg(const SEpSet *pEpSet, SRpcMsg *pMsg) { return tmsgSendReq(pEpSet, pMsg); }

void mndSyncCommitMsg(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  SyncIndex beginIndex = SYNC_INDEX_INVALID;
  if (pFsm->FpGetSnapshot != NULL) {
    SSnapshot snapshot = {0};
    pFsm->FpGetSnapshot(pFsm, &snapshot);
    beginIndex = snapshot.lastApplyIndex;
  }

  if (cbMeta.index > beginIndex) {
    SMnode    *pMnode = pFsm->data;
    SSyncMgmt *pMgmt = &pMnode->syncMgmt;

	if (cbMeta.term < cbMeta.currentTerm) {
		// restoring

    	SRpcMsg *pApplyMsg = (SRpcMsg *)pMsg;
    	pApplyMsg->info.node = pFsm->data;
    	mndProcessApplyMsg(pApplyMsg);
		//sdbUpdateVer(pMnode->pSdb, 1); ==> sdbSetVer(cbMeta.index, cbMeta.term);
		
		// mndTransPullup(pMnode);

	} else if (cbMeta.term == cbMeta.currentTerm) {
		// restore finish

    	SRpcMsg *pApplyMsg = (SRpcMsg *)pMsg;
    	pApplyMsg->info.node = pFsm->data;
    	mndProcessApplyMsg(pApplyMsg);
    	
		//sdbUpdateVer(pMnode->pSdb, 1); ==> sdbSetVer(cbMeta.index, cbMeta.term);

    	if (cbMeta.state == TAOS_SYNC_STATE_LEADER) {
    	  tsem_post(&pMgmt->syncSem);
    	}
	} else {
		ASSERT(0);
	}

  }
}

void mndSyncPreCommitMsg(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  // strict consistent, do nothing
}

void mndSyncRollBackMsg(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  // strict consistent, do nothing
}

int32_t mndSyncGetSnapshot(struct SSyncFSM *pFsm, SSnapshot *pSnapshot) {
  // snapshot
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
  tsem_init(&pMgmt->syncSem, 0, 0);

  if (mndInitWal(pMnode) < 0) {
    mError("failed to open wal since %s", terrstr());
    return -1;
  }

  SSyncInfo syncInfo = {.vgId = 1};
  SSyncCfg *pCfg = &(syncInfo.syncCfg);
  pCfg->replicaNum = pMnode->replica;
  pCfg->myIndex = pMnode->selfIndex;
  for (int32_t i = 0; i < pMnode->replica; ++i) {
    SNodeInfo *pNodeInfo = &pCfg->nodeInfo[i];
    tstrncpy(pNodeInfo->nodeFqdn, pMnode->replicas[i].fqdn, sizeof(pNodeInfo->nodeFqdn));
    pNodeInfo->nodePort = pMnode->replicas[i].port;
  }
  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s%ssync", pMnode->path, TD_DIRSEP);
  syncInfo.pWal = pMnode->syncMgmt.pWal;
  syncInfo.pFsm = mndSyncMakeFsm(pMnode);
  syncInfo.FpSendMsg = mndSyncSendMsg;
  syncInfo.FpEqMsg = mndSyncEqMsg;

  pMnode->syncMgmt.sync = syncOpen(&syncInfo);
  ASSERT(pMnode->syncMgmt.sync > 0);

  return 0;
}

void mndCleanupSync(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  tsem_destroy(&pMgmt->syncSem);
  mndCloseWal(pMnode);
}

static int32_t mndSyncApplyCb(struct SSyncFSM *fsm, SyncIndex index, const SSyncBuffer *buf, void *pData) {
  SMnode    *pMnode = pData;
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;

  pMgmt->errCode = 0;
  tsem_post(&pMgmt->syncSem);

  return 0;
}

int32_t mndSyncPropose(SMnode *pMnode, SSdbRaw *pRaw) {
  SWal *pWal = pMnode->syncMgmt.pWal;
  SSdb *pSdb = pMnode->pSdb;

#if 0
  int64_t ver = sdbUpdateVer(pSdb, 1);
  if (walWrite(pWal, ver, 1, pRaw, sdbGetRawTotalSize(pRaw)) < 0) {
    sdbUpdateVer(pSdb, -1);
    mError("ver:%" PRId64 ", failed to write raw:%p to wal since %s", ver, pRaw, terrstr());
    return -1;
  }

  mTrace("ver:%" PRId64 ", write to wal, raw:%p", ver, pRaw);
  walCommit(pWal, ver);
  walFsync(pWal, true);

  return 0;

#else

  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  pMgmt->errCode = 0;

  SRpcMsg rsp = {0};
  rsp.code = TDMT_MND_APPLY_MSG;
  rsp.contLen = sdbGetRawTotalSize(pRaw);
  rsp.pCont = rpcMallocCont(rsp.contLen);
  memcpy(rsp.pCont, pRaw, rsp.contLen);

  bool    isWeak = false;
  int32_t code = syncPropose(pMgmt->sync, &rsp, isWeak);
  if (code == 0) {
    tsem_wait(&pMgmt->syncSem);
  } else if (code == TAOS_SYNC_PROPOSE_NOT_LEADER) {
    terrno = TSDB_CODE_APP_NOT_READY;
    mError("failed to propose raw:%p since not leader", pRaw);
    return -1;
  } else if (code == TAOS_SYNC_PROPOSE_OTHER_ERROR) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    mError("failed to propose raw:%p since sync internal error", pRaw);
  } else {
    assert(0);
  }

  if (code != 0) return code;
  return pMgmt->errCode;
#endif
}

bool mndIsMaster(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  pMgmt->state = syncGetMyRole(pMgmt->sync);
  return pMgmt->state == TAOS_SYNC_STATE_LEADER;
}
