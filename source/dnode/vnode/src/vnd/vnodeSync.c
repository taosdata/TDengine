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

#define BATCH_DISABLE 1

static inline bool vnodeIsMsgBlock(tmsg_t type) {
  return (type == TDMT_VND_CREATE_TABLE) || (type == TDMT_VND_ALTER_TABLE) || (type == TDMT_VND_DROP_TABLE) ||
         (type == TDMT_VND_UPDATE_TAG_VAL) || (type == TDMT_VND_ALTER_REPLICA);
}

static inline bool vnodeIsMsgWeak(tmsg_t type) { return false; }

static inline void vnodeWaitBlockMsg(SVnode *pVnode, const SRpcMsg *pMsg) {
  if (vnodeIsMsgBlock(pMsg->msgType)) {
    const STraceId *trace = &pMsg->info.traceId;
    vGTrace("vgId:%d, msg:%p wait block, type:%s", pVnode->config.vgId, pMsg, TMSG_INFO(pMsg->msgType));
    pVnode->blockCount = 1;
    tsem_wait(&pVnode->syncSem);
  }
}

static inline void vnodePostBlockMsg(SVnode *pVnode, const SRpcMsg *pMsg) {
  if (vnodeIsMsgBlock(pMsg->msgType)) {
    const STraceId *trace = &pMsg->info.traceId;
    if (pVnode->blockCount) {
      vGTrace("vgId:%d, msg:%p post block, type:%s", pVnode->config.vgId, pMsg, TMSG_INFO(pMsg->msgType));
      pVnode->blockCount = 0;
      tsem_post(&pVnode->syncSem);
    }
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
    vError("vgId:%d, failed to set standby after leader transfer since %s", TD_VID(pVnode), terrstr());
    return -1;
  }
}

static int32_t vnodeProcessAlterReplicaReq(SVnode *pVnode, SRpcMsg *pMsg) {
  SAlterVnodeReq req = {0};
  if (tDeserializeSAlterVnodeReq((char *)pMsg->pCont + sizeof(SMsgHead), pMsg->contLen - sizeof(SMsgHead), &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return TSDB_CODE_INVALID_MSG;
  }

  const STraceId *trace = &pMsg->info.traceId;
  vGTrace("vgId:%d, start to alter vnode replica to %d, handle:%p", TD_VID(pVnode), req.replica, pMsg->info.handle);

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

void vnodeRedirectRpcMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  SEpSet newEpSet = {0};
  syncGetRetryEpSet(pVnode->sync, &newEpSet);

  const STraceId *trace = &pMsg->info.traceId;
  vGTrace("vgId:%d, msg:%p is redirect since not leader, numOfEps:%d inUse:%d", pVnode->config.vgId, pMsg,
          newEpSet.numOfEps, newEpSet.inUse);
  for (int32_t i = 0; i < newEpSet.numOfEps; ++i) {
    vGTrace("vgId:%d, msg:%p redirect:%d ep:%s:%u", pVnode->config.vgId, pMsg, i, newEpSet.eps[i].fqdn,
            newEpSet.eps[i].port);
  }
  pMsg->info.hasEpSet = 1;

  SRpcMsg rsp = {.code = TSDB_CODE_RPC_REDIRECT, .info = pMsg->info, .msgType = pMsg->msgType + 1};
  tmsgSendRedirectRsp(&rsp, &newEpSet);
}

static void inline vnodeHandleWriteMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  SRpcMsg rsp = {.code = pMsg->code, .info = pMsg->info};
  if (vnodeProcessWriteMsg(pVnode, pMsg, pMsg->info.conn.applyIndex, &rsp) < 0) {
    rsp.code = terrno;
    const STraceId *trace = &pMsg->info.traceId;
    vGError("vgId:%d, msg:%p failed to apply right now since %s", pVnode->config.vgId, pMsg, terrstr());
  }
  if (rsp.info.handle != NULL) {
    tmsgSendRsp(&rsp);
  }
}

static void vnodeHandleProposeError(SVnode *pVnode, SRpcMsg *pMsg, int32_t code) {
  if (code == TSDB_CODE_SYN_NOT_LEADER) {
    vnodeRedirectRpcMsg(pVnode, pMsg);
  } else {
    const STraceId *trace = &pMsg->info.traceId;
    vGError("vgId:%d, msg:%p failed to propose since %s, code:0x%x", pVnode->config.vgId, pMsg, tstrerror(code), code);
    SRpcMsg rsp = {.code = code, .info = pMsg->info};
    if (rsp.info.handle != NULL) {
      tmsgSendRsp(&rsp);
    }
  }
}

static void vnodeHandleAlterReplicaReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = vnodeProcessAlterReplicaReq(pVnode, pMsg);

  if (code > 0) {
    ASSERT(0);
  } else if (code == 0) {
    vnodeWaitBlockMsg(pVnode, pMsg);
  } else {
    if (terrno != 0) code = terrno;
    vnodeHandleProposeError(pVnode, pMsg, code);
  }

  const STraceId *trace = &pMsg->info.traceId;
  vGTrace("vgId:%d, msg:%p is freed, code:0x%x", pVnode->config.vgId, pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void inline vnodeProposeBatchMsg(SVnode *pVnode, SRpcMsg **pMsgArr, bool *pIsWeakArr, int32_t *arrSize) {
  if (*arrSize <= 0) return;

#if BATCH_DISABLE
  int32_t code = syncPropose(pVnode->sync, pMsgArr[0], pIsWeakArr[0]);
#else
  int32_t code = syncProposeBatch(pVnode->sync, pMsgArr, pIsWeakArr, *arrSize);
#endif

  if (code > 0) {
    for (int32_t i = 0; i < *arrSize; ++i) {
      vnodeHandleWriteMsg(pVnode, pMsgArr[i]);
    }
  } else if (code == 0) {
    vnodeWaitBlockMsg(pVnode, pMsgArr[*arrSize - 1]);
  } else {
    if (terrno != 0) code = terrno;
    for (int32_t i = 0; i < *arrSize; ++i) {
      vnodeHandleProposeError(pVnode, pMsgArr[i], code);
    }
  }

  for (int32_t i = 0; i < *arrSize; ++i) {
    SRpcMsg        *pMsg = pMsgArr[i];
    const STraceId *trace = &pMsg->info.traceId;
    vGTrace("vgId:%d, msg:%p is freed, code:0x%x", pVnode->config.vgId, pMsg, code);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }

  *arrSize = 0;
}

void vnodeProposeWriteMsg(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SVnode   *pVnode = pInfo->ahandle;
  int32_t   vgId = pVnode->config.vgId;
  int32_t   code = 0;
  SRpcMsg  *pMsg = NULL;
  int32_t   arrayPos = 0;
  SRpcMsg **pMsgArr = taosMemoryCalloc(numOfMsgs, sizeof(SRpcMsg *));
  bool     *pIsWeakArr = taosMemoryCalloc(numOfMsgs, sizeof(bool));
  vTrace("vgId:%d, get %d msgs from vnode-write queue", vgId, numOfMsgs);

  for (int32_t msg = 0; msg < numOfMsgs; msg++) {
    if (taosGetQitem(qall, (void **)&pMsg) == 0) continue;
    bool isWeak = vnodeIsMsgWeak(pMsg->msgType);
    bool isBlock = vnodeIsMsgBlock(pMsg->msgType);

    const STraceId *trace = &pMsg->info.traceId;
    vGTrace("vgId:%d, msg:%p get from vnode-write queue, weak:%d block:%d msg:%d:%d pos:%d, handle:%p", vgId, pMsg,
            isWeak, isBlock, msg, numOfMsgs, arrayPos, pMsg->info.handle);

    if (!pVnode->restored) {
      vGError("vgId:%d, msg:%p failed to process since not leader", vgId, pMsg);
      terrno = TSDB_CODE_APP_NOT_READY;
      vnodeHandleProposeError(pVnode, pMsg, TSDB_CODE_APP_NOT_READY);
      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
      continue;
    }

    if (pMsgArr == NULL || pIsWeakArr == NULL) {
      vGError("vgId:%d, msg:%p failed to process since out of memory", vgId, pMsg);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      vnodeHandleProposeError(pVnode, pMsg, terrno);
      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
      continue;
    }

    code = vnodePreProcessWriteMsg(pVnode, pMsg);
    if (code != 0) {
      vGError("vgId:%d, msg:%p failed to pre-process since %s", vgId, pMsg, terrstr());
      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
      continue;
    }

    if (pMsg->msgType == TDMT_VND_ALTER_REPLICA) {
      vnodeHandleAlterReplicaReq(pVnode, pMsg);
      continue;
    }

    if (isBlock || BATCH_DISABLE) {
      vnodeProposeBatchMsg(pVnode, pMsgArr, pIsWeakArr, &arrayPos);
    }

    pMsgArr[arrayPos] = pMsg;
    pIsWeakArr[arrayPos] = isWeak;
    arrayPos++;

    if (isBlock || msg == numOfMsgs - 1 || BATCH_DISABLE) {
      vnodeProposeBatchMsg(pVnode, pMsgArr, pIsWeakArr, &arrayPos);
    }
  }

  taosMemoryFree(pMsgArr);
  taosMemoryFree(pIsWeakArr);
}

void vnodeApplyWriteMsg(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SVnode  *pVnode = pInfo->ahandle;
  int32_t  vgId = pVnode->config.vgId;
  int32_t  code = 0;
  SRpcMsg *pMsg = NULL;

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    if (taosGetQitem(qall, (void **)&pMsg) == 0) continue;
    const STraceId *trace = &pMsg->info.traceId;
    vGTrace("vgId:%d, msg:%p get from vnode-apply queue, type:%s handle:%p index:%" PRId64, vgId, pMsg,
            TMSG_INFO(pMsg->msgType), pMsg->info.handle, pMsg->info.conn.applyIndex);

    SRpcMsg rsp = {.code = pMsg->code, .info = pMsg->info};
    if (rsp.code == 0) {
      if (vnodeProcessWriteMsg(pVnode, pMsg, pMsg->info.conn.applyIndex, &rsp) < 0) {
        rsp.code = terrno;
        vGError("vgId:%d, msg:%p failed to apply since %s, index:%" PRId64, vgId, pMsg, terrstr(),
                pMsg->info.conn.applyIndex);
      }
    }

    vnodePostBlockMsg(pVnode, pMsg);
    if (rsp.info.handle != NULL) {
      tmsgSendRsp(&rsp);
    }

    vGTrace("vgId:%d, msg:%p is freed, code:0x%x index:%" PRId64, vgId, pMsg, rsp.code, pMsg->info.conn.applyIndex);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
}

int32_t vnodeProcessSyncMsg(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
  int32_t         code = 0;
  const STraceId *trace = &pMsg->info.traceId;

  if (!syncEnvIsStart()) {
    vGError("vgId:%d, msg:%p failed to process since sync env not start", pVnode->config.vgId);
    terrno = TSDB_CODE_APP_ERROR;
    return -1;
  }

  SSyncNode *pSyncNode = syncNodeAcquire(pVnode->sync);
  if (pSyncNode == NULL) {
    vGError("vgId:%d, msg:%p failed to process since invalid sync node", pVnode->config.vgId);
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }

  vGTrace("vgId:%d, sync msg:%p will be processed, type:%s", pVnode->config.vgId, pMsg, TMSG_INFO(pMsg->msgType));

  if (syncNodeStrategy(pSyncNode) == SYNC_STRATEGY_NO_SNAPSHOT) {
    if (pMsg->msgType == TDMT_SYNC_TIMEOUT) {
      SyncTimeout *pSyncMsg = syncTimeoutFromRpcMsg2(pMsg);
      ASSERT(pSyncMsg != NULL);
      code = syncNodeOnTimeoutCb(pSyncNode, pSyncMsg);
      syncTimeoutDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_PING) {
      SyncPing *pSyncMsg = syncPingFromRpcMsg2(pMsg);
      ASSERT(pSyncMsg != NULL);
      code = syncNodeOnPingCb(pSyncNode, pSyncMsg);
      syncPingDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_PING_REPLY) {
      SyncPingReply *pSyncMsg = syncPingReplyFromRpcMsg2(pMsg);
      ASSERT(pSyncMsg != NULL);
      code = syncNodeOnPingReplyCb(pSyncNode, pSyncMsg);
      syncPingReplyDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_CLIENT_REQUEST) {
      SyncClientRequest *pSyncMsg = syncClientRequestFromRpcMsg2(pMsg);
      ASSERT(pSyncMsg != NULL);
      code = syncNodeOnClientRequestCb(pSyncNode, pSyncMsg, NULL);
      syncClientRequestDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_CLIENT_REQUEST_BATCH) {
      SyncClientRequestBatch *pSyncMsg = syncClientRequestBatchFromRpcMsg(pMsg);
      ASSERT(pSyncMsg != NULL);
      code = syncNodeOnClientRequestBatchCb(pSyncNode, pSyncMsg);
      syncClientRequestBatchDestroyDeep(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_REQUEST_VOTE) {
      SyncRequestVote *pSyncMsg = syncRequestVoteFromRpcMsg2(pMsg);
      ASSERT(pSyncMsg != NULL);
      code = syncNodeOnRequestVoteCb(pSyncNode, pSyncMsg);
      syncRequestVoteDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_REQUEST_VOTE_REPLY) {
      SyncRequestVoteReply *pSyncMsg = syncRequestVoteReplyFromRpcMsg2(pMsg);
      ASSERT(pSyncMsg != NULL);
      code = syncNodeOnRequestVoteReplyCb(pSyncNode, pSyncMsg);
      syncRequestVoteReplyDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_APPEND_ENTRIES) {
      SyncAppendEntries *pSyncMsg = syncAppendEntriesFromRpcMsg2(pMsg);
      ASSERT(pSyncMsg != NULL);
      code = syncNodeOnAppendEntriesCb(pSyncNode, pSyncMsg);
      syncAppendEntriesDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_APPEND_ENTRIES_REPLY) {
      SyncAppendEntriesReply *pSyncMsg = syncAppendEntriesReplyFromRpcMsg2(pMsg);
      ASSERT(pSyncMsg != NULL);
      code = syncNodeOnAppendEntriesReplyCb(pSyncNode, pSyncMsg);
      syncAppendEntriesReplyDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_SET_VNODE_STANDBY) {
      code = vnodeSetStandBy(pVnode);
      if (code != 0 && terrno != 0) code = terrno;
      SRpcMsg rsp = {.code = code, .info = pMsg->info};
      tmsgSendRsp(&rsp);
    } else {
      vGError("vgId:%d, msg:%p failed to process since error msg type:%d", pVnode->config.vgId, pMsg->msgType);
      code = -1;
    }

  } else if (syncNodeStrategy(pSyncNode) == SYNC_STRATEGY_WAL_FIRST) {
    // use wal first strategy
    if (pMsg->msgType == TDMT_SYNC_TIMEOUT) {
      SyncTimeout *pSyncMsg = syncTimeoutFromRpcMsg2(pMsg);
      ASSERT(pSyncMsg != NULL);
      code = syncNodeOnTimeoutCb(pSyncNode, pSyncMsg);
      syncTimeoutDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_PING) {
      SyncPing *pSyncMsg = syncPingFromRpcMsg2(pMsg);
      ASSERT(pSyncMsg != NULL);
      code = syncNodeOnPingCb(pSyncNode, pSyncMsg);
      syncPingDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_PING_REPLY) {
      SyncPingReply *pSyncMsg = syncPingReplyFromRpcMsg2(pMsg);
      ASSERT(pSyncMsg != NULL);
      code = syncNodeOnPingReplyCb(pSyncNode, pSyncMsg);
      syncPingReplyDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_CLIENT_REQUEST) {
      SyncClientRequest *pSyncMsg = syncClientRequestFromRpcMsg2(pMsg);
      ASSERT(pSyncMsg != NULL);
      code = syncNodeOnClientRequestCb(pSyncNode, pSyncMsg, NULL);
      syncClientRequestDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_CLIENT_REQUEST_BATCH) {
      SyncClientRequestBatch *pSyncMsg = syncClientRequestBatchFromRpcMsg(pMsg);
      ASSERT(pSyncMsg != NULL);
      code = syncNodeOnClientRequestBatchCb(pSyncNode, pSyncMsg);
      syncClientRequestBatchDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_REQUEST_VOTE) {
      SyncRequestVote *pSyncMsg = syncRequestVoteFromRpcMsg2(pMsg);
      ASSERT(pSyncMsg != NULL);
      code = syncNodeOnRequestVoteSnapshotCb(pSyncNode, pSyncMsg);
      syncRequestVoteDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_REQUEST_VOTE_REPLY) {
      SyncRequestVoteReply *pSyncMsg = syncRequestVoteReplyFromRpcMsg2(pMsg);
      ASSERT(pSyncMsg != NULL);
      code = syncNodeOnRequestVoteReplySnapshotCb(pSyncNode, pSyncMsg);
      syncRequestVoteReplyDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_APPEND_ENTRIES_BATCH) {
      SyncAppendEntriesBatch *pSyncMsg = syncAppendEntriesBatchFromRpcMsg2(pMsg);
      ASSERT(pSyncMsg != NULL);
      code = syncNodeOnAppendEntriesSnapshot2Cb(pSyncNode, pSyncMsg);
      syncAppendEntriesBatchDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_APPEND_ENTRIES_REPLY) {
      SyncAppendEntriesReply *pSyncMsg = syncAppendEntriesReplyFromRpcMsg2(pMsg);
      ASSERT(pSyncMsg != NULL);
      code = syncNodeOnAppendEntriesReplySnapshot2Cb(pSyncNode, pSyncMsg);
      syncAppendEntriesReplyDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_SNAPSHOT_SEND) {
      SyncSnapshotSend *pSyncMsg = syncSnapshotSendFromRpcMsg2(pMsg);
      code = syncNodeOnSnapshotSendCb(pSyncNode, pSyncMsg);
      syncSnapshotSendDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_SNAPSHOT_RSP) {
      SyncSnapshotRsp *pSyncMsg = syncSnapshotRspFromRpcMsg2(pMsg);
      code = syncNodeOnSnapshotRspCb(pSyncNode, pSyncMsg);
      syncSnapshotRspDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_SET_VNODE_STANDBY) {
      code = vnodeSetStandBy(pVnode);
      if (code != 0 && terrno != 0) code = terrno;
      SRpcMsg rsp = {.code = code, .info = pMsg->info};
      tmsgSendRsp(&rsp);
    } else {
      vGError("vgId:%d, msg:%p failed to process since error msg type:%d", pVnode->config.vgId, pMsg->msgType);
      code = -1;
    }
  }

  vTrace("vgId:%d, sync msg:%p is processed, type:%s code:0x%x", pVnode->config.vgId, pMsg, TMSG_INFO(pMsg->msgType),
         code);
  syncNodeRelease(pSyncNode);
  if (code != 0 && terrno == 0) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
  }
  return code;
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
  syncGetAndDelRespRpc(pVnode->sync, cbMeta.newCfgSeqNum, &rpcMsg.info);
  rpcMsg.info.conn.applyIndex = cbMeta.index;

  const STraceId *trace = (STraceId *)&pMsg->info.traceId;
  vGTrace("vgId:%d, alter vnode replica is confirmed, type:%s contLen:%d seq:%" PRIu64 " handle:%p", TD_VID(pVnode),
          TMSG_INFO(pMsg->msgType), pMsg->contLen, cbMeta.seqNum, rpcMsg.info.handle);
  if (rpcMsg.info.handle != NULL) {
    tmsgSendRsp(&rpcMsg);
  }

  vnodePostBlockMsg(pVnode, pMsg);
}

static void vnodeSyncCommitMsg(SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  if (cbMeta.isWeak == 0) {
    SVnode *pVnode = pFsm->data;

    if (cbMeta.code == 0) {
      SRpcMsg rpcMsg = {.msgType = pMsg->msgType, .contLen = pMsg->contLen};
      rpcMsg.pCont = rpcMallocCont(rpcMsg.contLen);
      memcpy(rpcMsg.pCont, pMsg->pCont, pMsg->contLen);
      syncGetAndDelRespRpc(pVnode->sync, cbMeta.seqNum, &rpcMsg.info);
      rpcMsg.info.conn.applyIndex = cbMeta.index;
      rpcMsg.info.conn.applyTerm = cbMeta.term;

      vInfo("vgId:%d, commit-cb is excuted, fsm:%p, index:%" PRId64 ", term:%" PRIu64 ", msg-index:%" PRId64
            ", weak:%d, code:%d, state:%d %s, type:%s",
            syncGetVgId(pVnode->sync), pFsm, cbMeta.index, cbMeta.term, rpcMsg.info.conn.applyIndex, cbMeta.isWeak,
            cbMeta.code, cbMeta.state, syncUtilState2String(cbMeta.state), TMSG_INFO(pMsg->msgType));

      tmsgPutToQueue(&pVnode->msgCb, APPLY_QUEUE, &rpcMsg);
    } else {
      SRpcMsg rsp = {.code = cbMeta.code, .info = pMsg->info};
      vError("vgId:%d, commit-cb execute error, type:%s, index:%" PRId64 ", error:0x%x %s", syncGetVgId(pVnode->sync),
             TMSG_INFO(pMsg->msgType), cbMeta.index, cbMeta.code, tstrerror(cbMeta.code));
      if (rsp.info.handle != NULL) {
        tmsgSendRsp(&rsp);
      }
    }
  }
}

static void vnodeSyncPreCommitMsg(SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  if (cbMeta.isWeak == 1) {
    SVnode *pVnode = pFsm->data;
    vTrace("vgId:%d, pre-commit-cb is excuted, fsm:%p, index:%" PRId64 ", weak:%d, code:%d, state:%d %s, type:%s",
           syncGetVgId(pVnode->sync), pFsm, cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state,
           syncUtilState2String(cbMeta.state), TMSG_INFO(pMsg->msgType));

    if (cbMeta.code == 0) {
      SRpcMsg rpcMsg = {.msgType = pMsg->msgType, .contLen = pMsg->contLen};
      rpcMsg.pCont = rpcMallocCont(rpcMsg.contLen);
      memcpy(rpcMsg.pCont, pMsg->pCont, pMsg->contLen);
      syncGetAndDelRespRpc(pVnode->sync, cbMeta.seqNum, &rpcMsg.info);
      rpcMsg.info.conn.applyIndex = cbMeta.index;
      rpcMsg.info.conn.applyTerm = cbMeta.term;
      tmsgPutToQueue(&pVnode->msgCb, APPLY_QUEUE, &rpcMsg);
    } else {
      SRpcMsg rsp = {.code = cbMeta.code, .info = pMsg->info};
      vError("vgId:%d, pre-commit-cb execute error, type:%s, error:0x%x %s", syncGetVgId(pVnode->sync),
             TMSG_INFO(pMsg->msgType), cbMeta.code, tstrerror(cbMeta.code));
      if (rsp.info.handle != NULL) {
        tmsgSendRsp(&rsp);
      }
    }
  }
}

static void vnodeSyncRollBackMsg(SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  SVnode *pVnode = pFsm->data;
  vTrace("vgId:%d, rollback-cb is excuted, fsm:%p, index:%" PRId64 ", weak:%d, code:%d, state:%d %s, type:%s",
         syncGetVgId(pVnode->sync), pFsm, cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state,
         syncUtilState2String(cbMeta.state), TMSG_INFO(pMsg->msgType));
}

#define USE_TSDB_SNAPSHOT

static int32_t vnodeSnapshotStartRead(struct SSyncFSM *pFsm, void *pParam, void **ppReader) {
#ifdef USE_TSDB_SNAPSHOT
  SVnode         *pVnode = pFsm->data;
  SSnapshotParam *pSnapshotParam = pParam;
  int32_t code = vnodeSnapReaderOpen(pVnode, pSnapshotParam->start, pSnapshotParam->end, (SVSnapReader **)ppReader);
  return code;
#else
  *ppReader = taosMemoryMalloc(32);
  return 0;
#endif
}

static int32_t vnodeSnapshotStopRead(struct SSyncFSM *pFsm, void *pReader) {
#ifdef USE_TSDB_SNAPSHOT
  SVnode *pVnode = pFsm->data;
  int32_t code = vnodeSnapReaderClose(pReader);
  return code;
#else
  taosMemoryFree(pReader);
  return 0;
#endif
}

static int32_t vnodeSnapshotDoRead(struct SSyncFSM *pFsm, void *pReader, void **ppBuf, int32_t *len) {
#ifdef USE_TSDB_SNAPSHOT
  SVnode *pVnode = pFsm->data;
  int32_t code = vnodeSnapRead(pReader, (uint8_t **)ppBuf, len);
  return code;
#else
  static int32_t times = 0;
  if (times++ < 5) {
    *len = 64;
    *ppBuf = taosMemoryMalloc(*len);
    snprintf(*ppBuf, *len, "snapshot block %d", times);
  } else {
    *len = 0;
    *ppBuf = NULL;
  }
  return 0;
#endif
}

static int32_t vnodeSnapshotStartWrite(struct SSyncFSM *pFsm, void *pParam, void **ppWriter) {
#ifdef USE_TSDB_SNAPSHOT
  SVnode         *pVnode = pFsm->data;
  SSnapshotParam *pSnapshotParam = pParam;

  do {
    int32_t itemSize = tmsgGetQueueSize(&pVnode->msgCb, pVnode->config.vgId, APPLY_QUEUE);
    if (itemSize == 0) {
      vInfo("vgId:%d, start write vnode snapshot since apply queue is empty", pVnode->config.vgId);
      break;
    } else {
      vInfo("vgId:%d, write vnode snapshot later since %d items in apply queue", pVnode->config.vgId);
      taosMsleep(10);
    }
  } while (true);

  int32_t code = vnodeSnapWriterOpen(pVnode, pSnapshotParam->start, pSnapshotParam->end, (SVSnapWriter **)ppWriter);
  return code;
#else
  *ppWriter = taosMemoryMalloc(32);
  return 0;
#endif
}

static int32_t vnodeSnapshotStopWrite(struct SSyncFSM *pFsm, void *pWriter, bool isApply, SSnapshot *pSnapshot) {
#ifdef USE_TSDB_SNAPSHOT
  SVnode *pVnode = pFsm->data;
  vInfo("vgId:%d, stop write vnode snapshot, apply:%d, index:%" PRId64 " term:%" PRIu64 " config:%" PRId64,
        pVnode->config.vgId, isApply, pSnapshot->lastApplyIndex, pSnapshot->lastApplyTerm, pSnapshot->lastConfigIndex);

  int32_t code = vnodeSnapWriterClose(pWriter, !isApply, pSnapshot);
  vInfo("vgId:%d, apply vnode snapshot finished, code:0x%x", pVnode->config.vgId, code);
  return code;
#else
  taosMemoryFree(pWriter);
  return 0;
#endif
}

static int32_t vnodeSnapshotDoWrite(struct SSyncFSM *pFsm, void *pWriter, void *pBuf, int32_t len) {
#ifdef USE_TSDB_SNAPSHOT
  SVnode *pVnode = pFsm->data;
  vDebug("vgId:%d, continue write vnode snapshot, len:%d", pVnode->config.vgId, len);
  int32_t code = vnodeSnapWrite(pWriter, pBuf, len);
  vDebug("vgId:%d, continue write vnode snapshot finished, len:%d", pVnode->config.vgId, len);
  return code;
#else
  return 0;
#endif
}

static void vnodeLeaderTransfer(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  SVnode *pVnode = pFsm->data;
}

static void vnodeRestoreFinish(struct SSyncFSM *pFsm) {
  SVnode *pVnode = pFsm->data;
  pVnode->restored = true;
  vDebug("vgId:%d, sync restore finished", pVnode->config.vgId);
}

static SSyncFSM *vnodeSyncMakeFsm(SVnode *pVnode) {
  SSyncFSM *pFsm = taosMemoryCalloc(1, sizeof(SSyncFSM));
  pFsm->data = pVnode;
  pFsm->FpCommitCb = vnodeSyncCommitMsg;
  pFsm->FpPreCommitCb = vnodeSyncPreCommitMsg;
  pFsm->FpRollBackCb = vnodeSyncRollBackMsg;
  pFsm->FpGetSnapshotInfo = vnodeSyncGetSnapshot;
  pFsm->FpRestoreFinishCb = vnodeRestoreFinish;
  pFsm->FpLeaderTransferCb = vnodeLeaderTransfer;
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
      .snapshotStrategy = SYNC_STRATEGY_WAL_FIRST,
      //.snapshotStrategy = SYNC_STRATEGY_NO_SNAPSHOT,
      .batchSize = 1,
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

  setPingTimerMS(pVnode->sync, 5000);
  setElectTimerMS(pVnode->sync, 2800);
  setHeartbeatTimerMS(pVnode->sync, 900);
  return 0;
}

void vnodeSyncStart(SVnode *pVnode) {
  syncSetMsgCb(pVnode->sync, &pVnode->msgCb);
  syncStart(pVnode->sync);
}

void vnodeSyncClose(SVnode *pVnode) { syncStop(pVnode->sync); }

bool vnodeIsLeader(SVnode *pVnode) {
  if (!syncIsReady(pVnode->sync)) {
    return false;
  }

  if (!pVnode->restored) {
    terrno = TSDB_CODE_APP_NOT_READY;
    return false;
  }

  return true;
}