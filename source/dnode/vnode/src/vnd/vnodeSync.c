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
#include "vnd.h"

static int32_t   vnodeSyncEqMsg(const SMsgCb *msgcb, SRpcMsg *pMsg);
static int32_t   vnodeSyncSendMsg(const SEpSet *pEpSet, SRpcMsg *pMsg);
static SSyncFSM *vnodeSyncMakeFsm(SVnode *pVnode);
static void      vnodeSyncCommitMsg(SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta);
static void      vnodeSyncPreCommitMsg(SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta);
static void      vnodeSyncRollBackMsg(SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta);
static int32_t   vnodeSyncGetSnapshot(SSyncFSM *pFsm, SSnapshot *pSnapshot);

int32_t vnodeSyncOpen(SVnode *pVnode, char *path) {
  SSyncInfo syncInfo = {
      .vgId = pVnode->config.vgId,
      .isStandBy = pVnode->config.standby,
      .syncCfg = pVnode->config.syncCfg,
      .pWal = pVnode->pWal,
      .msgcb = NULL,
      .FpSendMsg = vnodeSyncSendMsg,
      .FpEqMsg = vnodeSyncEqMsg,
  };

  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s%ssync", path, TD_DIRSEP);
  syncInfo.pFsm = vnodeSyncMakeFsm(pVnode);

  pVnode->sync = syncOpen(&syncInfo);
  if (pVnode->sync <= 0) {
    vError("vgId:%d, failed to open sync since %s", pVnode->config.vgId, terrstr());
    return -1;
  }

  setPingTimerMS(pVnode->sync, 3000);
  setElectTimerMS(pVnode->sync, 500);
  setHeartbeatTimerMS(pVnode->sync, 100);
  return 0;
}

int32_t vnodeSyncAlter(SVnode *pVnode, SRpcMsg *pMsg) {
  SAlterVnodeReq req = {0};
  if (tDeserializeSAlterVnodeReq((char *)pMsg->pCont + sizeof(SMsgHead), pMsg->contLen - sizeof(SMsgHead), &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return TSDB_CODE_INVALID_MSG;
  }

  vInfo("vgId:%d, start to alter vnode replica to %d", TD_VID(pVnode), req.replica);
  SSyncCfg cfg = {.replicaNum = req.replica, .myIndex = req.selfIndex};
  for (int32_t r = 0; r < req.replica; ++r) {
    SNodeInfo *pNode = &cfg.nodeInfo[r];
    tstrncpy(pNode->nodeFqdn, req.replicas[r].fqdn, sizeof(pNode->nodeFqdn));
    pNode->nodePort = req.replicas[r].port;
    vInfo("vgId:%d, replica:%d %s:%u", TD_VID(pVnode), r, pNode->nodeFqdn, pNode->nodePort);
  }

  int32_t code = syncReconfig(pVnode->sync, &cfg);
  if (code == TAOS_SYNC_PROPOSE_SUCCESS) {
    // todo refactor
    SRpcMsg rsp = {.info = pMsg->info, .code = 0};
    tmsgSendRsp(&rsp);
    return TSDB_CODE_ACTION_IN_PROGRESS;
  }

  return code;
}

void vnodeSyncStart(SVnode *pVnode) {
  syncSetMsgCb(pVnode->sync, &pVnode->msgCb);
  if (pVnode->config.standby) {
    syncStartStandBy(pVnode->sync);
  } else {
    syncStart(pVnode->sync);
  }
}

void vnodeSyncClose(SVnode *pVnode) { syncStop(pVnode->sync); }

int32_t vnodeSyncEqMsg(const SMsgCb *msgcb, SRpcMsg *pMsg) {
  int32_t code = tmsgPutToQueue(msgcb, SYNC_QUEUE, pMsg);
  if (code != 0) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
  }
  return code;
}

int32_t vnodeSyncSendMsg(const SEpSet *pEpSet, SRpcMsg *pMsg) {
  int32_t code = tmsgSendReq(pEpSet, pMsg);
  if (code != 0) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
  }
  return code;
}

int32_t vnodeSyncGetSnapshot(SSyncFSM *pFsm, SSnapshot *pSnapshot) {
  vnodeGetSnapshot(pFsm->data, pSnapshot);
  return 0;
}

void vnodeSyncReconfig(struct SSyncFSM *pFsm, SSyncCfg newCfg, SReConfigCbMeta cbMeta) {
  SVnode *pVnode = pFsm->data;
  vInfo("vgId:%d, sync reconfig is confirmed", TD_VID(pVnode));

  // todo rpc response here
}

void vnodeSyncCommitMsg(SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  SyncIndex beginIndex = SYNC_INDEX_INVALID;
  if (pFsm->FpGetSnapshot != NULL) {
    SSnapshot snapshot = {0};
    pFsm->FpGetSnapshot(pFsm, &snapshot);
    beginIndex = snapshot.lastApplyIndex;
  }

  if (cbMeta.index > beginIndex) {
    char logBuf[256] = {0};
    snprintf(
        logBuf, sizeof(logBuf),
        "==callback== ==CommitCb== execute, pFsm:%p, index:%ld, isWeak:%d, code:%d, state:%d %s, beginIndex :%ld\n",
        pFsm, cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncUtilState2String(cbMeta.state), beginIndex);
    syncRpcMsgLog2(logBuf, (SRpcMsg *)pMsg);

    SVnode       *pVnode = pFsm->data;
    SyncApplyMsg *pSyncApplyMsg = syncApplyMsgBuild2(pMsg, pVnode->config.vgId, &cbMeta);
    SRpcMsg       applyMsg;
    syncApplyMsg2RpcMsg(pSyncApplyMsg, &applyMsg);
    syncApplyMsgDestroy(pSyncApplyMsg);

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
    char logBuf[256] = {0};
    snprintf(logBuf, sizeof(logBuf),
             "==callback== ==CommitCb== do not execute, pFsm:%p, index:%ld, isWeak:%d, code:%d, state:%d %s, "
             "beginIndex :%ld\n",
             pFsm, cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncUtilState2String(cbMeta.state),
             beginIndex);
    syncRpcMsgLog2(logBuf, (SRpcMsg *)pMsg);
  }
}

void vnodeSyncPreCommitMsg(SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  char logBuf[256] = {0};
  snprintf(logBuf, sizeof(logBuf),
           "==callback== ==PreCommitCb== pFsm:%p, index:%ld, isWeak:%d, code:%d, state:%d %s \n", pFsm, cbMeta.index,
           cbMeta.isWeak, cbMeta.code, cbMeta.state, syncUtilState2String(cbMeta.state));
  syncRpcMsgLog2(logBuf, (SRpcMsg *)pMsg);
}

void vnodeSyncRollBackMsg(SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  char logBuf[256] = {0};
  snprintf(logBuf, sizeof(logBuf), "==callback== ==RollBackCb== pFsm:%p, index:%ld, isWeak:%d, code:%d, state:%d %s \n",
           pFsm, cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncUtilState2String(cbMeta.state));
  syncRpcMsgLog2(logBuf, (SRpcMsg *)pMsg);
}

SSyncFSM *vnodeSyncMakeFsm(SVnode *pVnode) {
  SSyncFSM *pFsm = taosMemoryCalloc(1, sizeof(SSyncFSM));
  pFsm->data = pVnode;
  pFsm->FpCommitCb = vnodeSyncCommitMsg;
  pFsm->FpPreCommitCb = vnodeSyncPreCommitMsg;
  pFsm->FpRollBackCb = vnodeSyncRollBackMsg;
  pFsm->FpGetSnapshot = vnodeSyncGetSnapshot;
  pFsm->FpRestoreFinishCb = NULL;
  pFsm->FpReConfigCb = vnodeSyncReconfig;

  return pFsm;
}