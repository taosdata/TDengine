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

static int32_t vnodeSetStandBy(SVnode *pVnode) {
  vInfo("vgId:%d, start to set standby", TD_VID(pVnode));

  if (syncSetStandby(pVnode->sync) == 0) {
    vInfo("vgId:%d, set standby success", TD_VID(pVnode));
    return 0;
  } else if (terrno != TSDB_CODE_SYN_IS_LEADER) {
    vError("vgId:%d, failed to set standby since %s", TD_VID(pVnode), terrstr());
    return -1;
  }

  vInfo("vgId:%d, start to transfer leader", TD_VID(pVnode));
  if (syncLeaderTransfer(pVnode->sync) != 0) {
    vError("vgId:%d, failed to transfer leader since:%s", TD_VID(pVnode), terrstr());
    return -1;
  } else {
    vInfo("vgId:%d, transfer leader success", TD_VID(pVnode));
  }

  if (syncSetStandby(pVnode->sync) == 0) {
    vInfo("vgId:%d, set standby success", TD_VID(pVnode));
    return 0;
  } else {
    vError("vgId:%d, failed to set standby since %s", TD_VID(pVnode), terrstr());
    return -1;
  }
}

static int32_t vnodeProcessAlterReplicaReq(SVnode *pVnode, SRpcMsg *pMsg) {
  SAlterVnodeReq req = {0};
  if (tDeserializeSAlterVnodeReq((char *)pMsg->pCont + sizeof(SMsgHead), pMsg->contLen - sizeof(SMsgHead), &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return TSDB_CODE_INVALID_MSG;
  }

  vInfo("vgId:%d, start to alter vnode replica to %d, handle:%p", TD_VID(pVnode), req.replica, pMsg->info.handle);
  SSyncCfg cfg = {.replicaNum = req.replica, .myIndex = req.selfIndex};
  for (int32_t r = 0; r < req.replica; ++r) {
    SNodeInfo *pNode = &cfg.nodeInfo[r];
    tstrncpy(pNode->nodeFqdn, req.replicas[r].fqdn, sizeof(pNode->nodeFqdn));
    pNode->nodePort = req.replicas[r].port;
    vInfo("vgId:%d, replica:%d %s:%u", TD_VID(pVnode), r, pNode->nodeFqdn, pNode->nodePort);
  }

  SRpcMsg rpcMsg = {.info = pMsg->info};
  if (syncReconfigBuild(pVnode->sync, &cfg, &rpcMsg) != 0) {
    vError("vgId:%d, failed to build reconfig msg since %s", TD_VID(pVnode), terrstr());
    return -1;
  }

  int32_t code = syncPropose(pVnode->sync, &rpcMsg, false);
  if (code != 0) {
    if (terrno != 0) code = terrno;

    vInfo("vgId:%d, failed to propose reconfig msg since %s", TD_VID(pVnode), terrstr());
    if (terrno == TSDB_CODE_SYN_IS_LEADER) {
      if (syncLeaderTransfer(pVnode->sync) != 0) {
        vError("vgId:%d, failed to transfer leader since %s", TD_VID(pVnode), terrstr());
      } else {
        vInfo("vgId:%d, transfer leader success", TD_VID(pVnode));
      }
    }
  }

  terrno = code;
  return code;
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
      code = vnodeProcessAlterReplicaReq(pVnode, pMsg);
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
    } else if (code == -1 && terrno == TSDB_CODE_SYN_NOT_LEADER) {
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
    vTrace("vgId:%d, msg:%p get from vnode-apply queue, index:%" PRId64 " type:%s handle:%p", vgId, pMsg,
           pMsg->info.conn.applyIndex, TMSG_INFO(pMsg->msgType), pMsg->info.handle);

    SRpcMsg rsp = {.code = pMsg->code, .info = pMsg->info};
    if (rsp.code == 0) {
      if (vnodeProcessWriteReq(pVnode, pMsg, pMsg->info.conn.applyIndex, &rsp) < 0) {
        rsp.code = terrno;
        vError("vgId:%d, msg:%p failed to apply since %s", vgId, pMsg, terrstr());
      }
    }

    vnodePostBlockMsg(pVnode, pMsg->msgType);
    if (rsp.info.handle != NULL) {
      tmsgSendRsp(&rsp);
    }

    vTrace("vgId:%d, msg:%p is freed, code:0x%x", vgId, pMsg, rsp.code);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
}

int32_t vnodeProcessSyncReq(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
  int32_t ret = 0;

  if (syncEnvIsStart()) {
    SSyncNode *pSyncNode = syncNodeAcquire(pVnode->sync);
    assert(pSyncNode != NULL);

    ESyncState state = syncGetMyRole(pVnode->sync);
    SyncTerm   currentTerm = syncGetMyTerm(pVnode->sync);

    SMsgHead *pHead = pMsg->pCont;

    char  logBuf[512] = {0};
    char *syncNodeStr = sync2SimpleStr(pVnode->sync);
    snprintf(logBuf, sizeof(logBuf), "==vnodeProcessSyncReq== msgType:%d, syncNode: %s", pMsg->msgType, syncNodeStr);
    static int64_t vndTick = 0;
    if (++vndTick % 10 == 1) {
      vTrace("sync trace msg:%s, %s", TMSG_INFO(pMsg->msgType), syncNodeStr);
    }
    syncRpcMsgLog2(logBuf, pMsg);
    taosMemoryFree(syncNodeStr);

    SRpcMsg *pRpcMsg = pMsg;

    if (pRpcMsg->msgType == TDMT_SYNC_TIMEOUT) {
      SyncTimeout *pSyncMsg = syncTimeoutFromRpcMsg2(pRpcMsg);
      assert(pSyncMsg != NULL);

      ret = syncNodeOnTimeoutCb(pSyncNode, pSyncMsg);
      syncTimeoutDestroy(pSyncMsg);

    } else if (pRpcMsg->msgType == TDMT_SYNC_PING) {
      SyncPing *pSyncMsg = syncPingFromRpcMsg2(pRpcMsg);
      assert(pSyncMsg != NULL);

      ret = syncNodeOnPingCb(pSyncNode, pSyncMsg);
      syncPingDestroy(pSyncMsg);

    } else if (pRpcMsg->msgType == TDMT_SYNC_PING_REPLY) {
      SyncPingReply *pSyncMsg = syncPingReplyFromRpcMsg2(pRpcMsg);
      assert(pSyncMsg != NULL);

      ret = syncNodeOnPingReplyCb(pSyncNode, pSyncMsg);
      syncPingReplyDestroy(pSyncMsg);

    } else if (pRpcMsg->msgType == TDMT_SYNC_CLIENT_REQUEST) {
      SyncClientRequest *pSyncMsg = syncClientRequestFromRpcMsg2(pRpcMsg);
      assert(pSyncMsg != NULL);

      ret = syncNodeOnClientRequestCb(pSyncNode, pSyncMsg);
      syncClientRequestDestroy(pSyncMsg);

    } else if (pRpcMsg->msgType == TDMT_SYNC_REQUEST_VOTE) {
      SyncRequestVote *pSyncMsg = syncRequestVoteFromRpcMsg2(pRpcMsg);
      assert(pSyncMsg != NULL);

      ret = syncNodeOnRequestVoteCb(pSyncNode, pSyncMsg);
      syncRequestVoteDestroy(pSyncMsg);

    } else if (pRpcMsg->msgType == TDMT_SYNC_REQUEST_VOTE_REPLY) {
      SyncRequestVoteReply *pSyncMsg = syncRequestVoteReplyFromRpcMsg2(pRpcMsg);
      assert(pSyncMsg != NULL);

      ret = syncNodeOnRequestVoteReplyCb(pSyncNode, pSyncMsg);
      syncRequestVoteReplyDestroy(pSyncMsg);

    } else if (pRpcMsg->msgType == TDMT_SYNC_APPEND_ENTRIES) {
      SyncAppendEntries *pSyncMsg = syncAppendEntriesFromRpcMsg2(pRpcMsg);
      assert(pSyncMsg != NULL);

      ret = syncNodeOnAppendEntriesCb(pSyncNode, pSyncMsg);
      syncAppendEntriesDestroy(pSyncMsg);

    } else if (pRpcMsg->msgType == TDMT_SYNC_APPEND_ENTRIES_REPLY) {
      SyncAppendEntriesReply *pSyncMsg = syncAppendEntriesReplyFromRpcMsg2(pRpcMsg);
      assert(pSyncMsg != NULL);

      ret = syncNodeOnAppendEntriesReplyCb(pSyncNode, pSyncMsg);
      syncAppendEntriesReplyDestroy(pSyncMsg);

    } else if (pRpcMsg->msgType == TDMT_SYNC_SET_VNODE_STANDBY) {
      ret = vnodeSetStandBy(pVnode);
      if (ret != 0 && terrno != 0) ret = terrno;
      SRpcMsg rsp = {.code = ret, .info = pMsg->info};
      tmsgSendRsp(&rsp);
    } else {
      vError("==vnodeProcessSyncReq== error msg type:%d", pRpcMsg->msgType);
      ret = -1;
    }

    syncNodeRelease(pSyncNode);
  } else {
    vError("==vnodeProcessSyncReq== error syncEnv stop");
    ret = -1;
  }

  if (ret != 0 && terrno == 0) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
  }
  return ret;
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

static void vnodeSyncReconfig(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SReConfigCbMeta cbMeta) {
  SVnode *pVnode = pFsm->data;

  SRpcMsg rpcMsg = {.msgType = pMsg->msgType, .contLen = pMsg->contLen};
  syncGetAndDelRespRpc(pVnode->sync, cbMeta.seqNum, &rpcMsg.info);
  rpcMsg.info.conn.applyIndex = cbMeta.index;

  vInfo("vgId:%d, alter vnode replica is confirmed, type:%s contLen:%d seq:%" PRIu64 " index:%" PRId64 " handle:%p",
        TD_VID(pVnode), TMSG_INFO(pMsg->msgType), pMsg->contLen, cbMeta.seqNum, cbMeta.index, rpcMsg.info.handle);
  if (rpcMsg.info.handle != NULL) {
    tmsgSendRsp(&rpcMsg);
  }

  vnodePostBlockMsg(pVnode, TDMT_VND_ALTER_REPLICA);
}

static void vnodeSyncCommitMsg(SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  SVnode   *pVnode = pFsm->data;
  SSnapshot snapshot = {0};
  SyncIndex beginIndex = SYNC_INDEX_INVALID;
  char      logBuf[256] = {0};

  if (pFsm->FpGetSnapshot != NULL) {
    (*pFsm->FpGetSnapshot)(pFsm, &snapshot);
    beginIndex = snapshot.lastApplyIndex;
  }

  if (cbMeta.index > beginIndex) {
    snprintf(
        logBuf, sizeof(logBuf),
        "==callback== ==CommitCb== execute, pFsm:%p, index:%ld, isWeak:%d, code:%d, state:%d %s, beginIndex :%ld\n",
        pFsm, cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncUtilState2String(cbMeta.state), beginIndex);
    syncRpcMsgLog2(logBuf, (SRpcMsg *)pMsg);

    SRpcMsg rpcMsg = {.msgType = pMsg->msgType, .contLen = pMsg->contLen};
    rpcMsg.pCont = rpcMallocCont(rpcMsg.contLen);
    memcpy(rpcMsg.pCont, pMsg->pCont, pMsg->contLen);
    syncGetAndDelRespRpc(pVnode->sync, cbMeta.seqNum, &rpcMsg.info);
    rpcMsg.info.conn.applyIndex = cbMeta.index;
    tmsgPutToQueue(&pVnode->msgCb, APPLY_QUEUE, &rpcMsg);

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

static int32_t vnodeSnapshotStartRead(struct SSyncFSM *pFsm, void **ppReader) { return 0; }

static int32_t vnodeSnapshotStopRead(struct SSyncFSM *pFsm, void *pReader) { return 0; }

static int32_t vnodeSnapshotDoRead(struct SSyncFSM *pFsm, void *pReader, void **ppBuf, int32_t *len) { return 0; }

static int32_t vnodeSnapshotStartWrite(struct SSyncFSM *pFsm, void **ppWriter) { return 0; }

static int32_t vnodeSnapshotStopWrite(struct SSyncFSM *pFsm, void *pWriter, bool isApply) { return 0; }

static int32_t vnodeSnapshotDoWrite(struct SSyncFSM *pFsm, void *pWriter, void *pBuf, int32_t len) { return 0; }

static SSyncFSM *vnodeSyncMakeFsm(SVnode *pVnode) {
  SSyncFSM *pFsm = taosMemoryCalloc(1, sizeof(SSyncFSM));
  pFsm->data = pVnode;
  pFsm->FpCommitCb = vnodeSyncCommitMsg;
  pFsm->FpPreCommitCb = vnodeSyncPreCommitMsg;
  pFsm->FpRollBackCb = vnodeSyncRollBackMsg;
  pFsm->FpGetSnapshot = vnodeSyncGetSnapshot;
  pFsm->FpRestoreFinishCb = NULL;
  pFsm->FpReConfigCb = vnodeSyncReconfig;
  pFsm->FpSnapshotStartRead = vnodeSnapshotStartRead;
  pFsm->FpSnapshotStopRead = vnodeSnapshotStopRead;
  pFsm->FpSnapshotDoRead = vnodeSnapshotDoRead;
  pFsm->FpSnapshotStartWrite = vnodeSnapshotStartWrite;
  pFsm->FpSnapshotStopWrite = vnodeSnapshotStopWrite;
  pFsm->FpSnapshotDoWrite = vnodeSnapshotDoWrite;

  return pFsm;
}

int32_t vnodeSyncOpen(SVnode *pVnode, char *path) {
  SSyncInfo syncInfo = {
      .snapshotEnable = false,
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
  syncStart(pVnode->sync);
}

void vnodeSyncClose(SVnode *pVnode) { syncStop(pVnode->sync); }
