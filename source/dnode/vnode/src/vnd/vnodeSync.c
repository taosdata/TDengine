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

static inline bool vnodeIsMsgBlock(tmsg_t type) {
  return (type == TDMT_VND_CREATE_TABLE) || (type == TDMT_VND_ALTER_CONFIRM) || (type == TDMT_VND_ALTER_REPLICA);
}

static inline bool vnodeIsMsgWeak(tmsg_t type) { return false; }

static inline void vnodeAccumBlockMsg(SVnode *pVnode, tmsg_t type) {
  if (!vnodeIsMsgBlock(type)) return;

  int32_t count = atomic_add_fetch_32(&pVnode->syncCount, 1);
  vTrace("vgId:%d, accum block, count:%d type:%s", pVnode->config.vgId, count, TMSG_INFO(type));
}

static inline void vnodeWaitBlockMsg(SVnode *pVnode) {
  int32_t count = atomic_load_32(&pVnode->syncCount);
  if (count <= 0) return;

  vTrace("vgId:%d, wait block finish, count:%d", pVnode->config.vgId, count);
  tsem_wait(&pVnode->syncSem);
  vTrace("vgId:%d, ===> block finish, count:%d", pVnode->config.vgId, count);
}

static inline void vnodePostBlockMsg(SVnode *pVnode, tmsg_t type) {
  if (!vnodeIsMsgBlock(type)) return;

  int32_t count = atomic_load_32(&pVnode->syncCount);
  if (count <= 0) return;

  count = atomic_sub_fetch_32(&pVnode->syncCount, 1);
  vTrace("vgId:%d, post block, count:%d type:%s", pVnode->config.vgId, count, TMSG_INFO(type));
  if (count <= 0) {
    tsem_post(&pVnode->syncSem);
  }
}

static int32_t vnodeProcessSyncReconfigReq(SVnode *pVnode, SRpcMsg *pMsg) {
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

  return syncReconfig(pVnode->sync, &cfg);
}

void vnodeProposeMsg(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SVnode  *pVnode = pInfo->ahandle;
  int32_t  vgId = pVnode->config.vgId;
  int32_t  code = 0;
  SRpcMsg *pMsg = NULL;

  for (int32_t m = 0; m < numOfMsgs; m++) {
    if (taosGetQitem(qall, (void **)&pMsg) == 0) continue;
    vTrace("vgId:%d, msg:%p get from vnode-write queue handle:%p", vgId, pMsg, pMsg->info.handle);

    if (pMsg->msgType == TDMT_VND_ALTER_REPLICA) {
      code = vnodeProcessSyncReconfigReq(pVnode, pMsg);
    } else {
      code = vnodePreprocessReq(pVnode, pMsg);
      if (code != 0) {
        vError("vgId:%d, failed to pre-process msg:%p since %s", vgId, pMsg, terrstr());
      } else {
        code = syncPropose(pVnode->sync, pMsg, vnodeIsMsgWeak(pMsg->msgType));
      }
    }

    if (code == 0) {
      vnodeAccumBlockMsg(pVnode, pMsg->msgType);
    } else if (code == TAOS_SYNC_PROPOSE_NOT_LEADER) {
      SEpSet newEpSet = {0};
      syncGetEpSet(pVnode->sync, &newEpSet);
      SEp *pEp = &newEpSet.eps[newEpSet.inUse];
      if (pEp->port == tsServerPort && strcmp(pEp->fqdn, tsLocalFqdn) == 0) {
        newEpSet.inUse = (newEpSet.inUse + 1) % newEpSet.numOfEps;
      }

      vTrace("vgId:%d, msg:%p is redirect since not leader, numOfEps:%d inUse:%d", vgId, pMsg, newEpSet.numOfEps,
             newEpSet.inUse);
      for (int32_t i = 0; i < newEpSet.numOfEps; ++i) {
        vTrace("vgId:%d, msg:%p redirect:%d ep:%s:%u", vgId, pMsg, i, newEpSet.eps[i].fqdn, newEpSet.eps[i].port);
      }

      SRpcMsg rsp = {.code = TSDB_CODE_RPC_REDIRECT, .info = pMsg->info};
      tmsgSendRedirectRsp(&rsp, &newEpSet);
    } else {
      if (terrno != 0) code = terrno;
      vError("vgId:%d, msg:%p failed to propose since %s, code:0x%x", vgId, pMsg, tstrerror(code), code);
      SRpcMsg rsp = {.code = code, .info = pMsg->info};
      tmsgSendRsp(&rsp);
    }

    vTrace("vgId:%d, msg:%p is freed, code:0x%x", vgId, pMsg, code);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }

  vnodeWaitBlockMsg(pVnode);
}

void vnodeApplyMsg(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SVnode  *pVnode = pInfo->ahandle;
  int32_t  vgId = pVnode->config.vgId;
  int32_t  code = 0;
  SRpcMsg *pMsg = NULL;

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    if (taosGetQitem(qall, (void **)&pMsg) == 0) continue;
    vTrace("vgId:%d, msg:%p get from vnode-apply queue", vgId, pMsg);

    // init response rpc msg
    SRpcMsg rsp = {0};

    // get original rpc msg
    assert(pMsg->msgType == TDMT_SYNC_APPLY_MSG);
    SyncApplyMsg *pSyncApplyMsg = syncApplyMsgFromRpcMsg2(pMsg);
    syncApplyMsgLog2("==vmProcessApplyQueue==", pSyncApplyMsg);
    SRpcMsg originalRpcMsg;
    syncApplyMsg2OriginalRpcMsg(pSyncApplyMsg, &originalRpcMsg);

    // apply data into tsdb
    if (vnodeProcessWriteReq(pVnode, &originalRpcMsg, pSyncApplyMsg->fsmMeta.index, &rsp) < 0) {
      rsp.code = terrno;
      vError("vgId:%d, msg:%p failed to apply since %s", vgId, pMsg, terrstr());
    }

    syncApplyMsgDestroy(pSyncApplyMsg);
    rpcFreeCont(originalRpcMsg.pCont);

    vnodePostBlockMsg(pVnode, originalRpcMsg.msgType);

    // if leader, send response
    if (pMsg->info.handle != NULL) {
      rsp.info = pMsg->info;
      tmsgSendRsp(&rsp);
    }

    vTrace("vgId:%d, msg:%p is freed, code:0x%x handle:%p", vgId, pMsg, rsp.code, pMsg->info.handle);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
}

static int32_t vnodeSyncEqMsg(const SMsgCb *msgcb, SRpcMsg *pMsg) {
  int32_t code = tmsgPutToQueue(msgcb, SYNC_QUEUE, pMsg);
  if (code != 0) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
  }
  return code;
}

static int32_t vnodeSyncSendMsg(const SEpSet *pEpSet, SRpcMsg *pMsg) {
  int32_t code = tmsgSendReq(pEpSet, pMsg);
  if (code != 0) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
  }
  return code;
}

static int32_t vnodeSyncGetSnapshot(SSyncFSM *pFsm, SSnapshot *pSnapshot) {
  vnodeGetSnapshot(pFsm->data, pSnapshot);
  return 0;
}

static void vnodeSyncReconfig(struct SSyncFSM *pFsm, SSyncCfg newCfg, SReConfigCbMeta cbMeta) {
  SVnode *pVnode = pFsm->data;
  vInfo("vgId:%d, sync reconfig is confirmed", TD_VID(pVnode));

  // todo rpc response here
}

static void vnodeSyncCommitMsg(SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
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

static void vnodeSyncPreCommitMsg(SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  char logBuf[256] = {0};
  snprintf(logBuf, sizeof(logBuf),
           "==callback== ==PreCommitCb== pFsm:%p, index:%ld, isWeak:%d, code:%d, state:%d %s \n", pFsm, cbMeta.index,
           cbMeta.isWeak, cbMeta.code, cbMeta.state, syncUtilState2String(cbMeta.state));
  syncRpcMsgLog2(logBuf, (SRpcMsg *)pMsg);
}

static void vnodeSyncRollBackMsg(SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  char logBuf[256] = {0};
  snprintf(logBuf, sizeof(logBuf), "==callback== ==RollBackCb== pFsm:%p, index:%ld, isWeak:%d, code:%d, state:%d %s \n",
           pFsm, cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncUtilState2String(cbMeta.state));
  syncRpcMsgLog2(logBuf, (SRpcMsg *)pMsg);
}

static SSyncFSM *vnodeSyncMakeFsm(SVnode *pVnode) {
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

void vnodeSyncStart(SVnode *pVnode) {
  syncSetMsgCb(pVnode->sync, &pVnode->msgCb);
  if (pVnode->config.standby) {
    syncStartStandBy(pVnode->sync);
  } else {
    syncStart(pVnode->sync);
  }
}

void vnodeSyncClose(SVnode *pVnode) { syncStop(pVnode->sync); }
