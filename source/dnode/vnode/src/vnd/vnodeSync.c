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

#include "vnd.h"

int32_t vnodeSyncOpen(SVnode *pVnode, char *path) {
  SSyncInfo syncInfo;
  syncInfo.vgId = pVnode->config.vgId;
  SSyncCfg *pCfg = &(syncInfo.syncCfg);
  pCfg->replicaNum = pVnode->config.syncCfg.replicaNum;
  pCfg->myIndex = pVnode->config.syncCfg.myIndex;
  memcpy(pCfg->nodeInfo, pVnode->config.syncCfg.nodeInfo, sizeof(pCfg->nodeInfo));

  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s/sync", path);
  syncInfo.pWal = pVnode->pWal;

  syncInfo.pFsm = syncVnodeMakeFsm(pVnode);
  syncInfo.rpcClient = NULL;
  syncInfo.FpSendMsg = vnodeSendMsg;
  syncInfo.queue = NULL;
  syncInfo.FpEqMsg = vnodeSyncEqMsg;

  pVnode->sync = syncOpen(&syncInfo);
  assert(pVnode->sync > 0);

  // for test
  setPingTimerMS(pVnode->sync, 3000);
  setElectTimerMS(pVnode->sync, 500);
  setHeartbeatTimerMS(pVnode->sync, 100);

  return 0;
}

int32_t vnodeSyncStart(SVnode *pVnode) {
  syncStart(pVnode->sync);
  return 0;
}

void vnodeSyncClose(SVnode *pVnode) {
  // stop by ref id
  syncStop(pVnode->sync);
}

void vnodeSyncSetQ(SVnode *pVnode, void *qHandle) { syncSetQ(pVnode->sync, (void *)(&(pVnode->msgCb))); }

void vnodeSyncSetRpc(SVnode *pVnode, void *rpcHandle) { syncSetRpc(pVnode->sync, (void *)(&(pVnode->msgCb))); }

int32_t vnodeSyncEqMsg(void *qHandle, SRpcMsg *pMsg) {
  int32_t ret = 0;
  SMsgCb *pMsgCb = qHandle;
  if (pMsgCb->queueFps[SYNC_QUEUE] != NULL) {
    tmsgPutToQueue(qHandle, SYNC_QUEUE, pMsg);
  } else {
    vError("vnodeSyncEqMsg queue is NULL, SYNC_QUEUE:%d", SYNC_QUEUE);
  }
  return ret;
}

int32_t vnodeSendMsg(void *rpcHandle, const SEpSet *pEpSet, SRpcMsg *pMsg) {
  int32_t ret = 0;
  SMsgCb *pMsgCb = rpcHandle;
  if (pMsgCb->queueFps[SYNC_QUEUE] != NULL) {
    pMsg->info.noResp = 1;
    tmsgSendReq(pEpSet, pMsg);
  } else {
    vError("vnodeSendMsg queue is NULL, SYNC_QUEUE:%d", SYNC_QUEUE);
  }
  return ret;
}

int32_t vnodeSyncGetSnapshotCb(struct SSyncFSM *pFsm, SSnapshot *pSnapshot) {
  SVnode *pVnode = (SVnode *)(pFsm->data);
  vnodeGetSnapshot(pVnode, pSnapshot);

  /*
  pSnapshot->data = NULL;
  pSnapshot->lastApplyIndex = 0;
  pSnapshot->lastApplyTerm = 0;
  */

  return 0;
}

void vnodeSyncCommitCb(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  SyncIndex beginIndex = SYNC_INDEX_INVALID;
  if (pFsm->FpGetSnapshot != NULL) {
    SSnapshot snapshot;
    pFsm->FpGetSnapshot(pFsm, &snapshot);
    beginIndex = snapshot.lastApplyIndex;
  }

  if (cbMeta.index > beginIndex) {
    char logBuf[256];
    snprintf(
        logBuf, sizeof(logBuf),
        "==callback== ==CommitCb== execute, pFsm:%p, index:%ld, isWeak:%d, code:%d, state:%d %s, beginIndex :%ld\n",
        pFsm, cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncUtilState2String(cbMeta.state), beginIndex);
    syncRpcMsgLog2(logBuf, (SRpcMsg *)pMsg);

    SVnode       *pVnode = (SVnode *)(pFsm->data);
    SyncApplyMsg *pSyncApplyMsg = syncApplyMsgBuild2(pMsg, pVnode->config.vgId, &cbMeta);
    SRpcMsg       applyMsg;
    syncApplyMsg2RpcMsg(pSyncApplyMsg, &applyMsg);
    syncApplyMsgDestroy(pSyncApplyMsg);

    /*
        SRpcMsg applyMsg;
        applyMsg = *pMsg;
        applyMsg.pCont = rpcMallocCont(applyMsg.contLen);
        assert(applyMsg.contLen == pMsg->contLen);
        memcpy(applyMsg.pCont, pMsg->pCont, applyMsg.contLen);
    */

    // recover handle for response
    SRpcMsg saveRpcMsg;
    int32_t ret = syncGetAndDelRespRpc(pVnode->sync, cbMeta.seqNum, &saveRpcMsg);
    if (ret == 1 && cbMeta.state == TAOS_SYNC_STATE_LEADER) {
      applyMsg.info = saveRpcMsg.info;
    } else {
      applyMsg.info.handle = NULL;
      applyMsg.info.ahandle = NULL;
    }

    // put to applyQ
    tmsgPutToQueue(&(pVnode->msgCb), APPLY_QUEUE, &applyMsg);

  } else {
    char logBuf[256];
    snprintf(logBuf, sizeof(logBuf),
             "==callback== ==CommitCb== do not execute, pFsm:%p, index:%ld, isWeak:%d, code:%d, state:%d %s, "
             "beginIndex :%ld\n",
             pFsm, cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncUtilState2String(cbMeta.state),
             beginIndex);
    syncRpcMsgLog2(logBuf, (SRpcMsg *)pMsg);
  }
}

void vnodeSyncPreCommitCb(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf),
           "==callback== ==PreCommitCb== pFsm:%p, index:%ld, isWeak:%d, code:%d, state:%d %s \n", pFsm, cbMeta.index,
           cbMeta.isWeak, cbMeta.code, cbMeta.state, syncUtilState2String(cbMeta.state));
  syncRpcMsgLog2(logBuf, (SRpcMsg *)pMsg);
}

void vnodeSyncRollBackCb(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf), "==callback== ==RollBackCb== pFsm:%p, index:%ld, isWeak:%d, code:%d, state:%d %s \n",
           pFsm, cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncUtilState2String(cbMeta.state));
  syncRpcMsgLog2(logBuf, (SRpcMsg *)pMsg);
}

SSyncFSM *syncVnodeMakeFsm(SVnode *pVnode) {
  SSyncFSM *pFsm = (SSyncFSM *)taosMemoryMalloc(sizeof(SSyncFSM));
  pFsm->data = pVnode;
  pFsm->FpCommitCb = vnodeSyncCommitCb;
  pFsm->FpPreCommitCb = vnodeSyncPreCommitCb;
  pFsm->FpRollBackCb = vnodeSyncRollBackCb;
  pFsm->FpGetSnapshot = vnodeSyncGetSnapshotCb;
  return pFsm;
}
