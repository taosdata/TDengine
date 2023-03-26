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
#include "sync.h"
#include "syncAppendEntries.h"
#include "syncAppendEntriesReply.h"
#include "syncCommit.h"
#include "syncElection.h"
#include "syncEnv.h"
#include "syncIndexMgr.h"
#include "syncInt.h"
#include "syncMessage.h"
#include "syncPipeline.h"
#include "syncRaftCfg.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncReplication.h"
#include "syncRequestVote.h"
#include "syncRequestVoteReply.h"
#include "syncRespMgr.h"
#include "syncSnapshot.h"
#include "syncTimeout.h"
#include "syncUtil.h"
#include "syncVoteMgr.h"
#include "tglobal.h"
#include "tref.h"

static void    syncNodeEqPingTimer(void* param, void* tmrId);
static void    syncNodeEqElectTimer(void* param, void* tmrId);
static void    syncNodeEqHeartbeatTimer(void* param, void* tmrId);
static int32_t syncNodeAppendNoop(SSyncNode* ths);
static void    syncNodeEqPeerHeartbeatTimer(void* param, void* tmrId);
static bool    syncIsConfigChanged(const SSyncCfg* pOldCfg, const SSyncCfg* pNewCfg);
static int32_t syncHbTimerInit(SSyncNode* pSyncNode, SSyncTimer* pSyncTimer, SRaftId destId);
static int32_t syncHbTimerStart(SSyncNode* pSyncNode, SSyncTimer* pSyncTimer);
static int32_t syncHbTimerStop(SSyncNode* pSyncNode, SSyncTimer* pSyncTimer);
static int32_t syncNodeUpdateNewConfigIndex(SSyncNode* ths, SSyncCfg* pNewCfg);
static bool    syncNodeInConfig(SSyncNode* pSyncNode, const SSyncCfg* config);
static void    syncNodeDoConfigChange(SSyncNode* pSyncNode, SSyncCfg* newConfig, SyncIndex lastConfigChangeIndex);
static bool    syncNodeIsOptimizedOneReplica(SSyncNode* ths, SRpcMsg* pMsg);

static bool    syncNodeCanChange(SSyncNode* pSyncNode);
static int32_t syncNodeLeaderTransfer(SSyncNode* pSyncNode);
static int32_t syncNodeLeaderTransferTo(SSyncNode* pSyncNode, SNodeInfo newLeader);
static int32_t syncDoLeaderTransfer(SSyncNode* ths, SRpcMsg* pRpcMsg, SSyncRaftEntry* pEntry);

static ESyncStrategy syncNodeStrategy(SSyncNode* pSyncNode);

int64_t syncOpen(SSyncInfo* pSyncInfo) {
  SSyncNode* pSyncNode = syncNodeOpen(pSyncInfo);
  if (pSyncNode == NULL) {
    sError("vgId:%d, failed to open sync node", pSyncInfo->vgId);
    return -1;
  }

  pSyncNode->rid = syncNodeAdd(pSyncNode);
  if (pSyncNode->rid < 0) {
    syncNodeClose(pSyncNode);
    return -1;
  }

  pSyncNode->pingBaseLine = pSyncInfo->pingMs;
  pSyncNode->pingTimerMS = pSyncInfo->pingMs;
  pSyncNode->electBaseLine = pSyncInfo->electMs;
  pSyncNode->hbBaseLine = pSyncInfo->heartbeatMs;
  pSyncNode->heartbeatTimerMS = pSyncInfo->heartbeatMs;
  pSyncNode->msgcb = pSyncInfo->msgcb;
  return pSyncNode->rid;
}

int32_t syncStart(int64_t rid) {
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    sError("failed to acquire rid:%" PRId64 " of tsNodeReftId for pSyncNode", rid);
    return -1;
  }

  if (syncNodeRestore(pSyncNode) < 0) {
    sError("vgId:%d, failed to restore sync log buffer since %s", pSyncNode->vgId, terrstr());
    goto _err;
  }

  if (syncNodeStart(pSyncNode) < 0) {
    sError("vgId:%d, failed to start sync node since %s", pSyncNode->vgId, terrstr());
    goto _err;
  }

  syncNodeRelease(pSyncNode);
  return 0;

_err:
  syncNodeRelease(pSyncNode);
  return -1;
}

void syncStop(int64_t rid) {
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode != NULL) {
    pSyncNode->isStart = false;
    syncNodeRelease(pSyncNode);
    syncNodeRemove(rid);
  }
}

void syncPreStop(int64_t rid) {
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode != NULL) {
    syncNodePreClose(pSyncNode);
    syncNodeRelease(pSyncNode);
  }
}

void syncPostStop(int64_t rid) {
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode != NULL) {
    syncNodePostClose(pSyncNode);
    syncNodeRelease(pSyncNode);
  }
}

static bool syncNodeCheckNewConfig(SSyncNode* pSyncNode, const SSyncCfg* pCfg) {
  if (!syncNodeInConfig(pSyncNode, pCfg)) return false;
  return abs(pCfg->replicaNum - pSyncNode->replicaNum) <= 1;
}

int32_t syncReconfig(int64_t rid, SSyncCfg* pNewCfg) {
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) return -1;

  if (!syncNodeCheckNewConfig(pSyncNode, pNewCfg)) {
    syncNodeRelease(pSyncNode);
    terrno = TSDB_CODE_SYN_NEW_CONFIG_ERROR;
    sError("vgId:%d, failed to reconfig since invalid new config", pSyncNode->vgId);
    return -1;
  }

  syncNodeUpdateNewConfigIndex(pSyncNode, pNewCfg);
  syncNodeDoConfigChange(pSyncNode, pNewCfg, SYNC_INDEX_INVALID);

  if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    syncNodeStopHeartbeatTimer(pSyncNode);

    for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
      syncHbTimerInit(pSyncNode, &pSyncNode->peerHeartbeatTimerArr[i], pSyncNode->replicasId[i]);
    }

    syncNodeStartHeartbeatTimer(pSyncNode);
    // syncNodeReplicate(pSyncNode);
  }

  syncNodeRelease(pSyncNode);
  return 0;
}

int32_t syncProcessMsg(int64_t rid, SRpcMsg* pMsg) {
  int32_t code = -1;
  if (!syncIsInit()) return code;

  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) return code;

  switch (pMsg->msgType) {
    case TDMT_SYNC_HEARTBEAT:
      code = syncNodeOnHeartbeat(pSyncNode, pMsg);
      break;
    case TDMT_SYNC_HEARTBEAT_REPLY:
      code = syncNodeOnHeartbeatReply(pSyncNode, pMsg);
      break;
    case TDMT_SYNC_TIMEOUT:
      code = syncNodeOnTimeout(pSyncNode, pMsg);
      break;
    case TDMT_SYNC_CLIENT_REQUEST:
      code = syncNodeOnClientRequest(pSyncNode, pMsg, NULL);
      break;
    case TDMT_SYNC_REQUEST_VOTE:
      code = syncNodeOnRequestVote(pSyncNode, pMsg);
      break;
    case TDMT_SYNC_REQUEST_VOTE_REPLY:
      code = syncNodeOnRequestVoteReply(pSyncNode, pMsg);
      break;
    case TDMT_SYNC_APPEND_ENTRIES:
      code = syncNodeOnAppendEntries(pSyncNode, pMsg);
      break;
    case TDMT_SYNC_APPEND_ENTRIES_REPLY:
      code = syncNodeOnAppendEntriesReply(pSyncNode, pMsg);
      break;
    case TDMT_SYNC_SNAPSHOT_SEND:
      code = syncNodeOnSnapshot(pSyncNode, pMsg);
      break;
    case TDMT_SYNC_SNAPSHOT_RSP:
      code = syncNodeOnSnapshotRsp(pSyncNode, pMsg);
      break;
    case TDMT_SYNC_LOCAL_CMD:
      code = syncNodeOnLocalCmd(pSyncNode, pMsg);
      break;
    default:
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      code = -1;
  }

  syncNodeRelease(pSyncNode);
  if (code != 0) {
    sDebug("vgId:%d, failed to process sync msg:%p type:%s since 0x%x", pSyncNode->vgId, pMsg, TMSG_INFO(pMsg->msgType),
           terrno);
  }
  return code;
}

int32_t syncLeaderTransfer(int64_t rid) {
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) return -1;

  int32_t ret = syncNodeLeaderTransfer(pSyncNode);
  syncNodeRelease(pSyncNode);
  return ret;
}

int32_t syncSendTimeoutRsp(int64_t rid, int64_t seq) {
  SSyncNode* pNode = syncNodeAcquire(rid);
  if (pNode == NULL) return -1;

  SRpcMsg rpcMsg = {0};
  int32_t ret = syncRespMgrGetAndDel(pNode->pSyncRespMgr, seq, &rpcMsg.info);
  rpcMsg.code = TSDB_CODE_SYN_TIMEOUT;

  syncNodeRelease(pNode);
  if (ret == 1) {
    sInfo("send timeout response, seq:%" PRId64 " handle:%p ahandle:%p", seq, rpcMsg.info.handle, rpcMsg.info.ahandle);
    rpcSendResponse(&rpcMsg);
    return 0;
  } else {
    sError("no message handle to send timeout response, seq:%" PRId64, seq);
    return -1;
  }
}

SyncIndex syncMinMatchIndex(SSyncNode* pSyncNode) {
  SyncIndex minMatchIndex = SYNC_INDEX_INVALID;

  if (pSyncNode->peersNum > 0) {
    minMatchIndex = syncIndexMgrGetIndex(pSyncNode->pMatchIndex, &(pSyncNode->peersId[0]));
  }

  for (int32_t i = 1; i < pSyncNode->peersNum; ++i) {
    SyncIndex matchIndex = syncIndexMgrGetIndex(pSyncNode->pMatchIndex, &(pSyncNode->peersId[i]));
    if (matchIndex < minMatchIndex) {
      minMatchIndex = matchIndex;
    }
  }
  return minMatchIndex;
}

int32_t syncBeginSnapshot(int64_t rid, int64_t lastApplyIndex) {
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    sError("sync begin snapshot error");
    return -1;
  }

  SyncIndex beginIndex = pSyncNode->pLogStore->syncLogBeginIndex(pSyncNode->pLogStore);
  SyncIndex endIndex = pSyncNode->pLogStore->syncLogEndIndex(pSyncNode->pLogStore);
  bool      isEmpty = pSyncNode->pLogStore->syncLogIsEmpty(pSyncNode->pLogStore);

  if (isEmpty || !(lastApplyIndex >= beginIndex && lastApplyIndex <= endIndex)) {
    sNTrace(pSyncNode, "new-snapshot-index:%" PRId64 ", empty:%d, do not delete wal", lastApplyIndex, isEmpty);
    syncNodeRelease(pSyncNode);
    return 0;
  }

  int32_t code = 0;
  int64_t logRetention = 0;

  if (syncNodeIsMnode(pSyncNode)) {
    // mnode
    logRetention = tsMndLogRetention;
  } else {
    // vnode
    if (pSyncNode->replicaNum > 1) {
      // multi replicas
      logRetention = SYNC_VNODE_LOG_RETENTION;
    }
  }

  if (pSyncNode->replicaNum > 1) {
    if (pSyncNode->state != TAOS_SYNC_STATE_LEADER && pSyncNode->state != TAOS_SYNC_STATE_FOLLOWER) {
      sNTrace(pSyncNode, "new-snapshot-index:%" PRId64 " candidate or unknown state, do not delete wal",
              lastApplyIndex);
      syncNodeRelease(pSyncNode);
      return 0;
    }
    logRetention = TMAX(logRetention, lastApplyIndex - pSyncNode->minMatchIndex + logRetention);
  }

_DEL_WAL:

  do {
    SSyncLogStoreData* pData = pSyncNode->pLogStore->data;
    SyncIndex          snapshotVer = walGetSnapshotVer(pData->pWal);
    SyncIndex          walCommitVer = walGetCommittedVer(pData->pWal);
    SyncIndex          wallastVer = walGetLastVer(pData->pWal);
    if (lastApplyIndex <= walCommitVer) {
      SyncIndex snapshottingIndex = atomic_load_64(&pSyncNode->snapshottingIndex);

      if (snapshottingIndex == SYNC_INDEX_INVALID) {
        atomic_store_64(&pSyncNode->snapshottingIndex, lastApplyIndex);
        pSyncNode->snapshottingTime = taosGetTimestampMs();

        code = walBeginSnapshot(pData->pWal, lastApplyIndex, logRetention);
        if (code == 0) {
          sNTrace(pSyncNode, "wal snapshot begin, index:%" PRId64 ", last apply index:%" PRId64,
                  pSyncNode->snapshottingIndex, lastApplyIndex);
        } else {
          sNError(pSyncNode, "wal snapshot begin error since:%s, index:%" PRId64 ", last apply index:%" PRId64,
                  terrstr(terrno), pSyncNode->snapshottingIndex, lastApplyIndex);
          atomic_store_64(&pSyncNode->snapshottingIndex, SYNC_INDEX_INVALID);
        }

      } else {
        sNTrace(pSyncNode, "snapshotting for %" PRId64 ", do not delete wal for new-snapshot-index:%" PRId64,
                snapshottingIndex, lastApplyIndex);
      }
    }
  } while (0);

  syncNodeRelease(pSyncNode);
  return code;
}

int32_t syncEndSnapshot(int64_t rid) {
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    sError("sync end snapshot error");
    return -1;
  }

  int32_t code = 0;
  if (atomic_load_64(&pSyncNode->snapshottingIndex) != SYNC_INDEX_INVALID) {
    SSyncLogStoreData* pData = pSyncNode->pLogStore->data;
    code = walEndSnapshot(pData->pWal);
    if (code != 0) {
      sNError(pSyncNode, "wal snapshot end error since:%s", terrstr());
      syncNodeRelease(pSyncNode);
      return -1;
    } else {
      sNTrace(pSyncNode, "wal snapshot end, index:%" PRId64, atomic_load_64(&pSyncNode->snapshottingIndex));
      atomic_store_64(&pSyncNode->snapshottingIndex, SYNC_INDEX_INVALID);
    }
  }

  syncNodeRelease(pSyncNode);
  return code;
}

int32_t syncStepDown(int64_t rid, SyncTerm newTerm) {
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    sError("sync step down error");
    return -1;
  }

  syncNodeStepDown(pSyncNode, newTerm);
  syncNodeRelease(pSyncNode);
  return 0;
}

bool syncNodeIsReadyForRead(SSyncNode* pSyncNode) {
  if (pSyncNode == NULL) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    sError("sync ready for read error");
    return false;
  }

  if (pSyncNode->state != TAOS_SYNC_STATE_LEADER) {
    terrno = TSDB_CODE_SYN_NOT_LEADER;
    return false;
  }

  if (!pSyncNode->restoreFinish) {
    terrno = TSDB_CODE_SYN_RESTORING;
    return false;
  }

  return true;
}

bool syncIsReadyForRead(int64_t rid) {
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    sError("sync ready for read error");
    return false;
  }

  bool ready = syncNodeIsReadyForRead(pSyncNode);

  syncNodeRelease(pSyncNode);
  return ready;
}

bool syncSnapshotSending(int64_t rid) {
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    return false;
  }

  bool b = syncNodeSnapshotSending(pSyncNode);
  syncNodeRelease(pSyncNode);
  return b;
}

bool syncSnapshotRecving(int64_t rid) {
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    return false;
  }

  bool b = syncNodeSnapshotRecving(pSyncNode);
  syncNodeRelease(pSyncNode);
  return b;
}

int32_t syncNodeLeaderTransfer(SSyncNode* pSyncNode) {
  if (pSyncNode->peersNum == 0) {
    sDebug("vgId:%d, only one replica, cannot leader transfer", pSyncNode->vgId);
    terrno = TSDB_CODE_SYN_ONE_REPLICA;
    return -1;
  }

  int32_t ret = 0;
  if (pSyncNode->state == TAOS_SYNC_STATE_LEADER && pSyncNode->replicaNum > 1) {
    SNodeInfo newLeader = (pSyncNode->peersNodeInfo)[0];
    if (pSyncNode->peersNum == 2) {
      SyncIndex matchIndex0 = syncIndexMgrGetIndex(pSyncNode->pMatchIndex, &(pSyncNode->peersId[0]));
      SyncIndex matchIndex1 = syncIndexMgrGetIndex(pSyncNode->pMatchIndex, &(pSyncNode->peersId[1]));
      if (matchIndex1 > matchIndex0) {
        newLeader = (pSyncNode->peersNodeInfo)[1];
      }
    }
    ret = syncNodeLeaderTransferTo(pSyncNode, newLeader);
  }

  return ret;
}

int32_t syncNodeLeaderTransferTo(SSyncNode* pSyncNode, SNodeInfo newLeader) {
  if (pSyncNode->replicaNum == 1) {
    sDebug("vgId:%d, only one replica, cannot leader transfer", pSyncNode->vgId);
    terrno = TSDB_CODE_SYN_ONE_REPLICA;
    return -1;
  }

  sNTrace(pSyncNode, "begin leader transfer to %s:%u", newLeader.nodeFqdn, newLeader.nodePort);

  SRpcMsg rpcMsg = {0};
  (void)syncBuildLeaderTransfer(&rpcMsg, pSyncNode->vgId);

  SyncLeaderTransfer* pMsg = rpcMsg.pCont;
  pMsg->newLeaderId.addr = SYNC_ADDR(&newLeader);
  pMsg->newLeaderId.vgId = pSyncNode->vgId;
  pMsg->newNodeInfo = newLeader;

  int32_t ret = syncNodePropose(pSyncNode, &rpcMsg, false, NULL);
  rpcFreeCont(rpcMsg.pCont);
  return ret;
}

SSyncState syncGetState(int64_t rid) {
  SSyncState state = {.state = TAOS_SYNC_STATE_ERROR};

  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode != NULL) {
    state.state = pSyncNode->state;
    state.restored = pSyncNode->restoreFinish;
    if (pSyncNode->vgId != 1) {
      state.canRead = syncNodeIsReadyForRead(pSyncNode);
    } else {
      state.canRead = state.restored;
    }
    syncNodeRelease(pSyncNode);
  }

  return state;
}

SyncIndex syncNodeGetSnapshotConfigIndex(SSyncNode* pSyncNode, SyncIndex snapshotLastApplyIndex) {
  ASSERT(pSyncNode->raftCfg.configIndexCount >= 1);
  SyncIndex lastIndex = (pSyncNode->raftCfg.configIndexArr)[0];

  for (int32_t i = 0; i < pSyncNode->raftCfg.configIndexCount; ++i) {
    if ((pSyncNode->raftCfg.configIndexArr)[i] > lastIndex &&
        (pSyncNode->raftCfg.configIndexArr)[i] <= snapshotLastApplyIndex) {
      lastIndex = (pSyncNode->raftCfg.configIndexArr)[i];
    }
  }
  sTrace("vgId:%d, sync get last config index, index:%" PRId64 " lcindex:%" PRId64, pSyncNode->vgId,
         snapshotLastApplyIndex, lastIndex);

  return lastIndex;
}

void syncGetRetryEpSet(int64_t rid, SEpSet* pEpSet) {
  pEpSet->numOfEps = 0;

  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) return;

  for (int32_t i = 0; i < pSyncNode->raftCfg.cfg.replicaNum; ++i) {
    SEp* pEp = &pEpSet->eps[i];
    tstrncpy(pEp->fqdn, pSyncNode->raftCfg.cfg.nodeInfo[i].nodeFqdn, TSDB_FQDN_LEN);
    pEp->port = (pSyncNode->raftCfg.cfg.nodeInfo)[i].nodePort;
    pEpSet->numOfEps++;
    sDebug("vgId:%d, sync get retry epset, index:%d %s:%d", pSyncNode->vgId, i, pEp->fqdn, pEp->port);
  }
  if (pEpSet->numOfEps > 0) {
    pEpSet->inUse = (pSyncNode->raftCfg.cfg.myIndex + 1) % pEpSet->numOfEps;
  }

  sInfo("vgId:%d, sync get retry epset numOfEps:%d inUse:%d", pSyncNode->vgId, pEpSet->numOfEps, pEpSet->inUse);
  syncNodeRelease(pSyncNode);
}

int32_t syncPropose(int64_t rid, SRpcMsg* pMsg, bool isWeak, int64_t* seq) {
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    sError("sync propose error");
    return -1;
  }

  int32_t ret = syncNodePropose(pSyncNode, pMsg, isWeak, seq);
  syncNodeRelease(pSyncNode);
  return ret;
}

int32_t syncNodePropose(SSyncNode* pSyncNode, SRpcMsg* pMsg, bool isWeak, int64_t* seq) {
  if (pSyncNode->state != TAOS_SYNC_STATE_LEADER) {
    terrno = TSDB_CODE_SYN_NOT_LEADER;
    sNError(pSyncNode, "sync propose not leader, type:%s", TMSG_INFO(pMsg->msgType));
    return -1;
  }

  // not restored, vnode enable
  if (!pSyncNode->restoreFinish && pSyncNode->vgId != 1) {
    terrno = TSDB_CODE_SYN_PROPOSE_NOT_READY;
    sNError(pSyncNode, "failed to sync propose since not ready, type:%s, last:%" PRId64 ", cmt:%" PRId64,
            TMSG_INFO(pMsg->msgType), syncNodeGetLastIndex(pSyncNode), pSyncNode->commitIndex);
    return -1;
  }

  // heartbeat timeout
  if (syncNodeHeartbeatReplyTimeout(pSyncNode)) {
    terrno = TSDB_CODE_SYN_PROPOSE_NOT_READY;
    sNError(pSyncNode, "failed to sync propose since hearbeat timeout, type:%s, last:%" PRId64 ", cmt:%" PRId64,
            TMSG_INFO(pMsg->msgType), syncNodeGetLastIndex(pSyncNode), pSyncNode->commitIndex);
    return -1;
  }

  // optimized one replica
  if (syncNodeIsOptimizedOneReplica(pSyncNode, pMsg)) {
    SyncIndex retIndex;
    int32_t   code = syncNodeOnClientRequest(pSyncNode, pMsg, &retIndex);
    if (code == 0) {
      pMsg->info.conn.applyIndex = retIndex;
      pMsg->info.conn.applyTerm = raftStoreGetTerm(pSyncNode);
      sTrace("vgId:%d, propose optimized msg, index:%" PRId64 " type:%s", pSyncNode->vgId, retIndex,
             TMSG_INFO(pMsg->msgType));
      return 1;
    } else {
      terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
      sError("vgId:%d, failed to propose optimized msg, index:%" PRId64 " type:%s", pSyncNode->vgId, retIndex,
             TMSG_INFO(pMsg->msgType));
      return -1;
    }
  } else {
    SRespStub stub = {.createTime = taosGetTimestampMs(), .rpcMsg = *pMsg};
    uint64_t  seqNum = syncRespMgrAdd(pSyncNode->pSyncRespMgr, &stub);
    SRpcMsg   rpcMsg = {0};
    int32_t   code = syncBuildClientRequest(&rpcMsg, pMsg, seqNum, isWeak, pSyncNode->vgId);
    if (code != 0) {
      sError("vgId:%d, failed to propose msg while serialize since %s", pSyncNode->vgId, terrstr());
      (void)syncRespMgrDel(pSyncNode->pSyncRespMgr, seqNum);
      return -1;
    }

    sNTrace(pSyncNode, "propose msg, type:%s", TMSG_INFO(pMsg->msgType));
    code = (*pSyncNode->syncEqMsg)(pSyncNode->msgcb, &rpcMsg);
    if (code != 0) {
      sError("vgId:%d, failed to propose msg while enqueue since %s", pSyncNode->vgId, terrstr());
      (void)syncRespMgrDel(pSyncNode->pSyncRespMgr, seqNum);
    }

    if (seq != NULL) *seq = seqNum;
    return code;
  }
}

static int32_t syncHbTimerInit(SSyncNode* pSyncNode, SSyncTimer* pSyncTimer, SRaftId destId) {
  pSyncTimer->pTimer = NULL;
  pSyncTimer->counter = 0;
  pSyncTimer->timerMS = pSyncNode->hbBaseLine;
  pSyncTimer->timerCb = syncNodeEqPeerHeartbeatTimer;
  pSyncTimer->destId = destId;
  pSyncTimer->timeStamp = taosGetTimestampMs();
  atomic_store_64(&pSyncTimer->logicClock, 0);
  return 0;
}

static int32_t syncHbTimerStart(SSyncNode* pSyncNode, SSyncTimer* pSyncTimer) {
  int32_t ret = 0;
  int64_t tsNow = taosGetTimestampMs();
  if (syncIsInit()) {
    SSyncHbTimerData* pData = syncHbTimerDataAcquire(pSyncTimer->hbDataRid);
    if (pData == NULL) {
      pData = taosMemoryMalloc(sizeof(SSyncHbTimerData));
      pData->rid = syncHbTimerDataAdd(pData);
    }
    pSyncTimer->hbDataRid = pData->rid;
    pSyncTimer->timeStamp = tsNow;

    pData->syncNodeRid = pSyncNode->rid;
    pData->pTimer = pSyncTimer;
    pData->destId = pSyncTimer->destId;
    pData->logicClock = pSyncTimer->logicClock;
    pData->execTime = tsNow + pSyncTimer->timerMS;

    taosTmrReset(pSyncTimer->timerCb, pSyncTimer->timerMS / HEARTBEAT_TICK_NUM, (void*)(pData->rid),
                 syncEnv()->pTimerManager, &pSyncTimer->pTimer);
  } else {
    sError("vgId:%d, start ctrl hb timer error, sync env is stop", pSyncNode->vgId);
  }
  return ret;
}

static int32_t syncHbTimerStop(SSyncNode* pSyncNode, SSyncTimer* pSyncTimer) {
  int32_t ret = 0;
  atomic_add_fetch_64(&pSyncTimer->logicClock, 1);
  taosTmrStop(pSyncTimer->pTimer);
  pSyncTimer->pTimer = NULL;
  syncHbTimerDataRemove(pSyncTimer->hbDataRid);
  pSyncTimer->hbDataRid = -1;
  return ret;
}

int32_t syncNodeLogStoreRestoreOnNeed(SSyncNode* pNode) {
  ASSERTS(pNode->pLogStore != NULL, "log store not created");
  ASSERTS(pNode->pFsm != NULL, "pFsm not registered");
  ASSERTS(pNode->pFsm->FpGetSnapshotInfo != NULL, "FpGetSnapshotInfo not registered");
  SSnapshot snapshot = {0};
  pNode->pFsm->FpGetSnapshotInfo(pNode->pFsm, &snapshot);

  SyncIndex commitIndex = snapshot.lastApplyIndex;
  SyncIndex firstVer = pNode->pLogStore->syncLogBeginIndex(pNode->pLogStore);
  SyncIndex lastVer = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
  if (lastVer < commitIndex || firstVer > commitIndex + 1) {
    if (pNode->pLogStore->syncLogRestoreFromSnapshot(pNode->pLogStore, commitIndex)) {
      sError("vgId:%d, failed to restore log store from snapshot since %s. lastVer:%" PRId64 ", snapshotVer:%" PRId64,
             pNode->vgId, terrstr(), lastVer, commitIndex);
      return -1;
    }
  }
  return 0;
}

// open/close --------------
SSyncNode* syncNodeOpen(SSyncInfo* pSyncInfo) {
  SSyncNode* pSyncNode = taosMemoryCalloc(1, sizeof(SSyncNode));
  if (pSyncNode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  if (!taosDirExist((char*)(pSyncInfo->path))) {
    if (taosMkDir(pSyncInfo->path) != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      sError("failed to create dir:%s since %s", pSyncInfo->path, terrstr());
      goto _error;
    }
  }

  memcpy(pSyncNode->path, pSyncInfo->path, sizeof(pSyncNode->path));
  snprintf(pSyncNode->raftStorePath, sizeof(pSyncNode->raftStorePath), "%s%sraft_store.json", pSyncInfo->path,
           TD_DIRSEP);
  snprintf(pSyncNode->configPath, sizeof(pSyncNode->configPath), "%s%sraft_config.json", pSyncInfo->path, TD_DIRSEP);

  if (!taosCheckExistFile(pSyncNode->configPath)) {
    // create a new raft config file
    sInfo("vgId:%d, create a new raft config file", pSyncNode->vgId);
    pSyncNode->raftCfg.isStandBy = pSyncInfo->isStandBy;
    pSyncNode->raftCfg.snapshotStrategy = pSyncInfo->snapshotStrategy;
    pSyncNode->raftCfg.lastConfigIndex = SYNC_INDEX_INVALID;
    pSyncNode->raftCfg.batchSize = pSyncInfo->batchSize;
    pSyncNode->raftCfg.cfg = pSyncInfo->syncCfg;
    pSyncNode->raftCfg.configIndexCount = 1;
    pSyncNode->raftCfg.configIndexArr[0] = -1;

    if (syncWriteCfgFile(pSyncNode) != 0) {
      sError("vgId:%d, failed to create sync cfg file", pSyncNode->vgId);
      goto _error;
    }
  } else {
    // update syncCfg by raft_config.json
    if (syncReadCfgFile(pSyncNode) != 0) {
      sError("vgId:%d, failed to read sync cfg file", pSyncNode->vgId);
      goto _error;
    }

    if (pSyncInfo->syncCfg.replicaNum > 0 && syncIsConfigChanged(&pSyncNode->raftCfg.cfg, &pSyncInfo->syncCfg)) {
      sInfo("vgId:%d, use sync config from input options and write to cfg file", pSyncNode->vgId);
      pSyncNode->raftCfg.cfg = pSyncInfo->syncCfg;
      if (syncWriteCfgFile(pSyncNode) != 0) {
        sError("vgId:%d, failed to write sync cfg file", pSyncNode->vgId);
        goto _error;
      }
    } else {
      sInfo("vgId:%d, use sync config from sync cfg file", pSyncNode->vgId);
      pSyncInfo->syncCfg = pSyncNode->raftCfg.cfg;
    }
  }

  // init by SSyncInfo
  pSyncNode->vgId = pSyncInfo->vgId;
  SSyncCfg* pCfg = &pSyncNode->raftCfg.cfg;
  bool      updated = false;
  sInfo("vgId:%d, start to open sync node, replica:%d selfIndex:%d", pSyncNode->vgId, pCfg->replicaNum, pCfg->myIndex);
  for (int32_t i = 0; i < pCfg->replicaNum; ++i) {
    SNodeInfo* pNode = &pCfg->nodeInfo[i];
    if (tmsgUpdateDnodeInfo(&pNode->nodeId, &pNode->clusterId, pNode->nodeFqdn, &pNode->nodePort)) {
      updated = true;
    }
    sInfo("vgId:%d, index:%d ep:%s:%u dnode:%d cluster:%" PRId64, pSyncNode->vgId, i, pNode->nodeFqdn, pNode->nodePort,
          pNode->nodeId, pNode->clusterId);
  }

  if (updated) {
    sInfo("vgId:%d, save config info since dnode info changed", pSyncNode->vgId);
    if (syncWriteCfgFile(pSyncNode) != 0) {
      sError("vgId:%d, failed to write sync cfg file on dnode info updated", pSyncNode->vgId);
      goto _error;
    }
  }

  pSyncNode->pWal = pSyncInfo->pWal;
  pSyncNode->msgcb = pSyncInfo->msgcb;
  pSyncNode->syncSendMSg = pSyncInfo->syncSendMSg;
  pSyncNode->syncEqMsg = pSyncInfo->syncEqMsg;
  pSyncNode->syncEqCtrlMsg = pSyncInfo->syncEqCtrlMsg;

  // create raft log ring buffer
  pSyncNode->pLogBuf = syncLogBufferCreate();
  if (pSyncNode->pLogBuf == NULL) {
    sError("failed to init sync log buffer since %s. vgId:%d", terrstr(), pSyncNode->vgId);
    goto _error;
  }

  // init internal
  pSyncNode->myNodeInfo = pSyncNode->raftCfg.cfg.nodeInfo[pSyncNode->raftCfg.cfg.myIndex];
  if (!syncUtilNodeInfo2RaftId(&pSyncNode->myNodeInfo, pSyncNode->vgId, &pSyncNode->myRaftId)) {
    sError("vgId:%d, failed to determine my raft member id", pSyncNode->vgId);
    goto _error;
  }

  // init peersNum, peers, peersId
  pSyncNode->peersNum = pSyncNode->raftCfg.cfg.replicaNum - 1;
  int32_t j = 0;
  for (int32_t i = 0; i < pSyncNode->raftCfg.cfg.replicaNum; ++i) {
    if (i != pSyncNode->raftCfg.cfg.myIndex) {
      pSyncNode->peersNodeInfo[j] = pSyncNode->raftCfg.cfg.nodeInfo[i];
      syncUtilNodeInfo2EpSet(&pSyncNode->peersNodeInfo[j], &pSyncNode->peersEpset[j]);
      j++;
    }
  }
  for (int32_t i = 0; i < pSyncNode->peersNum; ++i) {
    if (!syncUtilNodeInfo2RaftId(&pSyncNode->peersNodeInfo[i], pSyncNode->vgId, &pSyncNode->peersId[i])) {
      sError("vgId:%d, failed to determine raft member id, peer:%d", pSyncNode->vgId, i);
      goto _error;
    }
  }

  // init replicaNum, replicasId
  pSyncNode->replicaNum = pSyncNode->raftCfg.cfg.replicaNum;
  for (int32_t i = 0; i < pSyncNode->raftCfg.cfg.replicaNum; ++i) {
    if (!syncUtilNodeInfo2RaftId(&pSyncNode->raftCfg.cfg.nodeInfo[i], pSyncNode->vgId, &pSyncNode->replicasId[i])) {
      sError("vgId:%d, failed to determine raft member id, replica:%d", pSyncNode->vgId, i);
      goto _error;
    }
  }

  // init raft algorithm
  pSyncNode->pFsm = pSyncInfo->pFsm;
  pSyncInfo->pFsm = NULL;
  pSyncNode->quorum = syncUtilQuorum(pSyncNode->raftCfg.cfg.replicaNum);
  pSyncNode->leaderCache = EMPTY_RAFT_ID;

  // init life cycle outside

  // TLA+ Spec
  // InitHistoryVars == /\ elections = {}
  //                    /\ allLogs   = {}
  //                    /\ voterLog  = [i \in Server |-> [j \in {} |-> <<>>]]
  // InitServerVars == /\ currentTerm = [i \in Server |-> 1]
  //                   /\ state       = [i \in Server |-> Follower]
  //                   /\ votedFor    = [i \in Server |-> Nil]
  // InitCandidateVars == /\ votesResponded = [i \in Server |-> {}]
  //                      /\ votesGranted   = [i \in Server |-> {}]
  // \* The values nextIndex[i][i] and matchIndex[i][i] are never read, since the
  // \* leader does not send itself messages. It's still easier to include these
  // \* in the functions.
  // InitLeaderVars == /\ nextIndex  = [i \in Server |-> [j \in Server |-> 1]]
  //                   /\ matchIndex = [i \in Server |-> [j \in Server |-> 0]]
  // InitLogVars == /\ log          = [i \in Server |-> << >>]
  //                /\ commitIndex  = [i \in Server |-> 0]
  // Init == /\ messages = [m \in {} |-> 0]
  //         /\ InitHistoryVars
  //         /\ InitServerVars
  //         /\ InitCandidateVars
  //         /\ InitLeaderVars
  //         /\ InitLogVars
  //

  // init TLA+ server vars
  pSyncNode->state = TAOS_SYNC_STATE_FOLLOWER;
  if (raftStoreOpen(pSyncNode) != 0) {
    sError("vgId:%d, failed to open raft store at path %s", pSyncNode->vgId, pSyncNode->raftStorePath);
    goto _error;
  }

  // init TLA+ candidate vars
  pSyncNode->pVotesGranted = voteGrantedCreate(pSyncNode);
  if (pSyncNode->pVotesGranted == NULL) {
    sError("vgId:%d, failed to create VotesGranted", pSyncNode->vgId);
    goto _error;
  }
  pSyncNode->pVotesRespond = votesRespondCreate(pSyncNode);
  if (pSyncNode->pVotesRespond == NULL) {
    sError("vgId:%d, failed to create VotesRespond", pSyncNode->vgId);
    goto _error;
  }

  // init TLA+ leader vars
  pSyncNode->pNextIndex = syncIndexMgrCreate(pSyncNode);
  if (pSyncNode->pNextIndex == NULL) {
    sError("vgId:%d, failed to create SyncIndexMgr", pSyncNode->vgId);
    goto _error;
  }
  pSyncNode->pMatchIndex = syncIndexMgrCreate(pSyncNode);
  if (pSyncNode->pMatchIndex == NULL) {
    sError("vgId:%d, failed to create SyncIndexMgr", pSyncNode->vgId);
    goto _error;
  }

  // init TLA+ log vars
  pSyncNode->pLogStore = logStoreCreate(pSyncNode);
  if (pSyncNode->pLogStore == NULL) {
    sError("vgId:%d, failed to create SyncLogStore", pSyncNode->vgId);
    goto _error;
  }

  SyncIndex commitIndex = SYNC_INDEX_INVALID;
  if (pSyncNode->pFsm != NULL && pSyncNode->pFsm->FpGetSnapshotInfo != NULL) {
    SSnapshot snapshot = {0};
    pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
    if (snapshot.lastApplyIndex > commitIndex) {
      commitIndex = snapshot.lastApplyIndex;
      sNTrace(pSyncNode, "reset commit index by snapshot");
    }
  }
  pSyncNode->commitIndex = commitIndex;
  sInfo("vgId:%d, sync node commitIndex initialized as %" PRId64, pSyncNode->vgId, pSyncNode->commitIndex);

  // restore log store on need
  if (syncNodeLogStoreRestoreOnNeed(pSyncNode) < 0) {
    sError("vgId:%d, failed to restore log store since %s.", pSyncNode->vgId, terrstr());
    goto _error;
  }

  // timer ms init
  pSyncNode->pingBaseLine = PING_TIMER_MS;
  pSyncNode->electBaseLine = tsElectInterval;
  pSyncNode->hbBaseLine = tsHeartbeatInterval;

  // init ping timer
  pSyncNode->pPingTimer = NULL;
  pSyncNode->pingTimerMS = pSyncNode->pingBaseLine;
  atomic_store_64(&pSyncNode->pingTimerLogicClock, 0);
  atomic_store_64(&pSyncNode->pingTimerLogicClockUser, 0);
  pSyncNode->FpPingTimerCB = syncNodeEqPingTimer;
  pSyncNode->pingTimerCounter = 0;

  // init elect timer
  pSyncNode->pElectTimer = NULL;
  pSyncNode->electTimerMS = syncUtilElectRandomMS(pSyncNode->electBaseLine, 2 * pSyncNode->electBaseLine);
  atomic_store_64(&pSyncNode->electTimerLogicClock, 0);
  pSyncNode->FpElectTimerCB = syncNodeEqElectTimer;
  pSyncNode->electTimerCounter = 0;

  // init heartbeat timer
  pSyncNode->pHeartbeatTimer = NULL;
  pSyncNode->heartbeatTimerMS = pSyncNode->hbBaseLine;
  atomic_store_64(&pSyncNode->heartbeatTimerLogicClock, 0);
  atomic_store_64(&pSyncNode->heartbeatTimerLogicClockUser, 0);
  pSyncNode->FpHeartbeatTimerCB = syncNodeEqHeartbeatTimer;
  pSyncNode->heartbeatTimerCounter = 0;

  // init peer heartbeat timer
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    syncHbTimerInit(pSyncNode, &(pSyncNode->peerHeartbeatTimerArr[i]), (pSyncNode->replicasId)[i]);
  }

  // tools
  pSyncNode->pSyncRespMgr = syncRespMgrCreate(pSyncNode, SYNC_RESP_TTL_MS);
  if (pSyncNode->pSyncRespMgr == NULL) {
    sError("vgId:%d, failed to create SyncRespMgr", pSyncNode->vgId);
    goto _error;
  }

  // restore state
  pSyncNode->restoreFinish = false;

  // snapshot senders
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SSyncSnapshotSender* pSender = snapshotSenderCreate(pSyncNode, i);
    if (pSender == NULL) return NULL;

    pSyncNode->senders[i] = pSender;
    sSDebug(pSender, "snapshot sender create while open sync node, data:%p", pSender);
  }

  // snapshot receivers
  pSyncNode->pNewNodeReceiver = snapshotReceiverCreate(pSyncNode, EMPTY_RAFT_ID);
  if (pSyncNode->pNewNodeReceiver == NULL) return NULL;
  sRDebug(pSyncNode->pNewNodeReceiver, "snapshot receiver create while open sync node, data:%p",
          pSyncNode->pNewNodeReceiver);

  // is config changing
  pSyncNode->changing = false;

  // replication mgr
  if (syncNodeLogReplMgrInit(pSyncNode) < 0) {
    sError("vgId:%d, failed to init repl mgr since %s.", pSyncNode->vgId, terrstr());
    goto _error;
  }

  // peer state
  if (syncNodePeerStateInit(pSyncNode) < 0) {
    sError("vgId:%d, failed to init peer stat since %s.", pSyncNode->vgId, terrstr());
    goto _error;
  }

  //
  // min match index
  pSyncNode->minMatchIndex = SYNC_INDEX_INVALID;

  // start in syncNodeStart
  // start raft
  // syncNodeBecomeFollower(pSyncNode);

  int64_t timeNow = taosGetTimestampMs();
  pSyncNode->startTime = timeNow;
  pSyncNode->leaderTime = timeNow;
  pSyncNode->lastReplicateTime = timeNow;

  // snapshotting
  atomic_store_64(&pSyncNode->snapshottingIndex, SYNC_INDEX_INVALID);

  // init log buffer
  if (syncLogBufferInit(pSyncNode->pLogBuf, pSyncNode) < 0) {
    sError("vgId:%d, failed to init sync log buffer since %s", pSyncNode->vgId, terrstr());
    goto _error;
  }

  pSyncNode->isStart = true;
  pSyncNode->electNum = 0;
  pSyncNode->becomeLeaderNum = 0;
  pSyncNode->configChangeNum = 0;
  pSyncNode->hbSlowNum = 0;
  pSyncNode->hbrSlowNum = 0;
  pSyncNode->tmrRoutineNum = 0;

  sNInfo(pSyncNode, "sync open, node:%p electInterval:%d heartbeatInterval:%d heartbeatTimeout:%d", pSyncNode,
         tsElectInterval, tsHeartbeatInterval, tsHeartbeatTimeout);
  return pSyncNode;

_error:
  if (pSyncInfo->pFsm) {
    taosMemoryFree(pSyncInfo->pFsm);
    pSyncInfo->pFsm = NULL;
  }
  syncNodeClose(pSyncNode);
  pSyncNode = NULL;
  return NULL;
}

void syncNodeMaybeUpdateCommitBySnapshot(SSyncNode* pSyncNode) {
  if (pSyncNode->pFsm != NULL && pSyncNode->pFsm->FpGetSnapshotInfo != NULL) {
    SSnapshot snapshot = {0};
    pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
    if (snapshot.lastApplyIndex > pSyncNode->commitIndex) {
      pSyncNode->commitIndex = snapshot.lastApplyIndex;
    }
  }
}

int32_t syncNodeRestore(SSyncNode* pSyncNode) {
  ASSERTS(pSyncNode->pLogStore != NULL, "log store not created");
  ASSERTS(pSyncNode->pLogBuf != NULL, "ring log buffer not created");

  SyncIndex lastVer = pSyncNode->pLogStore->syncLogLastIndex(pSyncNode->pLogStore);
  SyncIndex commitIndex = pSyncNode->pLogStore->syncLogCommitIndex(pSyncNode->pLogStore);
  SyncIndex endIndex = pSyncNode->pLogBuf->endIndex;
  if (lastVer != -1 && endIndex != lastVer + 1) {
    terrno = TSDB_CODE_WAL_LOG_INCOMPLETE;
    sError("vgId:%d, failed to restore sync node since %s. expected lastLogIndex:%" PRId64 ", lastVer:%" PRId64 "",
           pSyncNode->vgId, terrstr(), endIndex - 1, lastVer);
    return -1;
  }

  ASSERT(endIndex == lastVer + 1);
  pSyncNode->commitIndex = TMAX(pSyncNode->commitIndex, commitIndex);
  sInfo("vgId:%d, restore sync until commitIndex:%" PRId64, pSyncNode->vgId, pSyncNode->commitIndex);

  if (syncLogBufferCommit(pSyncNode->pLogBuf, pSyncNode, pSyncNode->commitIndex) < 0) {
    return -1;
  }

  return 0;
}

int32_t syncNodeStart(SSyncNode* pSyncNode) {
  // start raft
  if (pSyncNode->replicaNum == 1) {
    raftStoreNextTerm(pSyncNode);
    syncNodeBecomeLeader(pSyncNode, "one replica start");

    // Raft 3.6.2 Committing entries from previous terms
    syncNodeAppendNoop(pSyncNode);
  } else {
    syncNodeBecomeFollower(pSyncNode, "first start");
  }

  int32_t ret = 0;
  ret = syncNodeStartPingTimer(pSyncNode);
  if (ret != 0) {
    sError("vgId:%d, failed to start ping timer since %s", pSyncNode->vgId, terrstr());
  }
  return ret;
}

int32_t syncNodeStartStandBy(SSyncNode* pSyncNode) {
  // state change
  pSyncNode->state = TAOS_SYNC_STATE_FOLLOWER;
  syncNodeStopHeartbeatTimer(pSyncNode);

  // reset elect timer, long enough
  int32_t electMS = TIMER_MAX_MS;
  int32_t ret = syncNodeRestartElectTimer(pSyncNode, electMS);
  if (ret < 0) {
    sError("vgId:%d, failed to restart elect timer since %s", pSyncNode->vgId, terrstr());
    return -1;
  }

  ret = syncNodeStartPingTimer(pSyncNode);
  if (ret < 0) {
    sError("vgId:%d, failed to start ping timer since %s", pSyncNode->vgId, terrstr());
    return -1;
  }
  return ret;
}

void syncNodePreClose(SSyncNode* pSyncNode) {
  ASSERT(pSyncNode != NULL);
  ASSERT(pSyncNode->pFsm != NULL);
  ASSERT(pSyncNode->pFsm->FpApplyQueueItems != NULL);

  // stop elect timer
  syncNodeStopElectTimer(pSyncNode);

  // stop heartbeat timer
  syncNodeStopHeartbeatTimer(pSyncNode);

  // stop ping timer
  syncNodeStopPingTimer(pSyncNode);

  // clean rsp
  syncRespCleanRsp(pSyncNode->pSyncRespMgr);
}

void syncNodePostClose(SSyncNode* pSyncNode) {
  if (pSyncNode->pNewNodeReceiver != NULL) {
    if (snapshotReceiverIsStart(pSyncNode->pNewNodeReceiver)) {
      snapshotReceiverStop(pSyncNode->pNewNodeReceiver);
    }

    sDebug("vgId:%d, snapshot receiver destroy while preclose sync node, data:%p", pSyncNode->vgId,
           pSyncNode->pNewNodeReceiver);
    snapshotReceiverDestroy(pSyncNode->pNewNodeReceiver);
    pSyncNode->pNewNodeReceiver = NULL;
  }
}

void syncHbTimerDataFree(SSyncHbTimerData* pData) { taosMemoryFree(pData); }

void syncNodeClose(SSyncNode* pSyncNode) {
  if (pSyncNode == NULL) return;
  sNInfo(pSyncNode, "sync close, node:%p", pSyncNode);

  syncRespCleanRsp(pSyncNode->pSyncRespMgr);

  syncNodeStopPingTimer(pSyncNode);
  syncNodeStopElectTimer(pSyncNode);
  syncNodeStopHeartbeatTimer(pSyncNode);
  syncNodeLogReplMgrDestroy(pSyncNode);

  syncRespMgrDestroy(pSyncNode->pSyncRespMgr);
  pSyncNode->pSyncRespMgr = NULL;
  voteGrantedDestroy(pSyncNode->pVotesGranted);
  pSyncNode->pVotesGranted = NULL;
  votesRespondDestory(pSyncNode->pVotesRespond);
  pSyncNode->pVotesRespond = NULL;
  syncIndexMgrDestroy(pSyncNode->pNextIndex);
  pSyncNode->pNextIndex = NULL;
  syncIndexMgrDestroy(pSyncNode->pMatchIndex);
  pSyncNode->pMatchIndex = NULL;
  logStoreDestory(pSyncNode->pLogStore);
  pSyncNode->pLogStore = NULL;
  syncLogBufferDestroy(pSyncNode->pLogBuf);
  pSyncNode->pLogBuf = NULL;

  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    if (pSyncNode->senders[i] != NULL) {
      sDebug("vgId:%d, snapshot sender destroy while close, data:%p", pSyncNode->vgId, pSyncNode->senders[i]);

      if (snapshotSenderIsStart(pSyncNode->senders[i])) {
        snapshotSenderStop(pSyncNode->senders[i], false);
      }

      snapshotSenderDestroy(pSyncNode->senders[i]);
      pSyncNode->senders[i] = NULL;
    }
  }

  if (pSyncNode->pNewNodeReceiver != NULL) {
    if (snapshotReceiverIsStart(pSyncNode->pNewNodeReceiver)) {
      snapshotReceiverStop(pSyncNode->pNewNodeReceiver);
    }

    sDebug("vgId:%d, snapshot receiver destroy while close, data:%p", pSyncNode->vgId, pSyncNode->pNewNodeReceiver);
    snapshotReceiverDestroy(pSyncNode->pNewNodeReceiver);
    pSyncNode->pNewNodeReceiver = NULL;
  }

  if (pSyncNode->pFsm != NULL) {
    taosMemoryFree(pSyncNode->pFsm);
  }

  raftStoreClose(pSyncNode);

  taosMemoryFree(pSyncNode);
}

ESyncStrategy syncNodeStrategy(SSyncNode* pSyncNode) { return pSyncNode->raftCfg.snapshotStrategy; }

// timer control --------------
int32_t syncNodeStartPingTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  if (syncIsInit()) {
    taosTmrReset(pSyncNode->FpPingTimerCB, pSyncNode->pingTimerMS, pSyncNode, syncEnv()->pTimerManager,
                 &pSyncNode->pPingTimer);
    atomic_store_64(&pSyncNode->pingTimerLogicClock, pSyncNode->pingTimerLogicClockUser);
  } else {
    sError("vgId:%d, start ping timer error, sync env is stop", pSyncNode->vgId);
  }
  return ret;
}

int32_t syncNodeStopPingTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  atomic_add_fetch_64(&pSyncNode->pingTimerLogicClockUser, 1);
  taosTmrStop(pSyncNode->pPingTimer);
  pSyncNode->pPingTimer = NULL;
  return ret;
}

int32_t syncNodeStartElectTimer(SSyncNode* pSyncNode, int32_t ms) {
  int32_t ret = 0;
  if (syncIsInit()) {
    pSyncNode->electTimerMS = ms;

    int64_t execTime = taosGetTimestampMs() + ms;
    atomic_store_64(&(pSyncNode->electTimerParam.executeTime), execTime);
    atomic_store_64(&(pSyncNode->electTimerParam.logicClock), pSyncNode->electTimerLogicClock);
    pSyncNode->electTimerParam.pSyncNode = pSyncNode;
    pSyncNode->electTimerParam.pData = NULL;

    taosTmrReset(pSyncNode->FpElectTimerCB, pSyncNode->electTimerMS, (void*)(pSyncNode->rid), syncEnv()->pTimerManager,
                 &pSyncNode->pElectTimer);

  } else {
    sError("vgId:%d, start elect timer error, sync env is stop", pSyncNode->vgId);
  }
  return ret;
}

int32_t syncNodeStopElectTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  atomic_add_fetch_64(&pSyncNode->electTimerLogicClock, 1);
  taosTmrStop(pSyncNode->pElectTimer);
  pSyncNode->pElectTimer = NULL;

  return ret;
}

int32_t syncNodeRestartElectTimer(SSyncNode* pSyncNode, int32_t ms) {
  int32_t ret = 0;
  syncNodeStopElectTimer(pSyncNode);
  syncNodeStartElectTimer(pSyncNode, ms);
  return ret;
}

void syncNodeResetElectTimer(SSyncNode* pSyncNode) {
  int32_t electMS;

  if (pSyncNode->raftCfg.isStandBy) {
    electMS = TIMER_MAX_MS;
  } else {
    electMS = syncUtilElectRandomMS(pSyncNode->electBaseLine, 2 * pSyncNode->electBaseLine);
  }

  (void)syncNodeRestartElectTimer(pSyncNode, electMS);

  sNTrace(pSyncNode, "reset elect timer, min:%d, max:%d, ms:%d", pSyncNode->electBaseLine, 2 * pSyncNode->electBaseLine,
          electMS);
}

static int32_t syncNodeDoStartHeartbeatTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  if (syncIsInit()) {
    taosTmrReset(pSyncNode->FpHeartbeatTimerCB, pSyncNode->heartbeatTimerMS, pSyncNode, syncEnv()->pTimerManager,
                 &pSyncNode->pHeartbeatTimer);
    atomic_store_64(&pSyncNode->heartbeatTimerLogicClock, pSyncNode->heartbeatTimerLogicClockUser);
  } else {
    sError("vgId:%d, start heartbeat timer error, sync env is stop", pSyncNode->vgId);
  }

  sNTrace(pSyncNode, "start heartbeat timer, ms:%d", pSyncNode->heartbeatTimerMS);
  return ret;
}

int32_t syncNodeStartHeartbeatTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;

#if 0
  pSyncNode->heartbeatTimerMS = pSyncNode->hbBaseLine;
  ret = syncNodeDoStartHeartbeatTimer(pSyncNode);
#endif

  for (int32_t i = 0; i < pSyncNode->peersNum; ++i) {
    SSyncTimer* pSyncTimer = syncNodeGetHbTimer(pSyncNode, &(pSyncNode->peersId[i]));
    if (pSyncTimer != NULL) {
      syncHbTimerStart(pSyncNode, pSyncTimer);
    }
  }

  return ret;
}

int32_t syncNodeStopHeartbeatTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;

#if 0
  atomic_add_fetch_64(&pSyncNode->heartbeatTimerLogicClockUser, 1);
  taosTmrStop(pSyncNode->pHeartbeatTimer);
  pSyncNode->pHeartbeatTimer = NULL;
#endif

  for (int32_t i = 0; i < pSyncNode->peersNum; ++i) {
    SSyncTimer* pSyncTimer = syncNodeGetHbTimer(pSyncNode, &(pSyncNode->peersId[i]));
    if (pSyncTimer != NULL) {
      syncHbTimerStop(pSyncNode, pSyncTimer);
    }
  }

  return ret;
}

int32_t syncNodeRestartHeartbeatTimer(SSyncNode* pSyncNode) {
  syncNodeStopHeartbeatTimer(pSyncNode);
  syncNodeStartHeartbeatTimer(pSyncNode);
  return 0;
}

int32_t syncNodeSendMsgById(const SRaftId* destRaftId, SSyncNode* pNode, SRpcMsg* pMsg) {
  SEpSet* epSet = NULL;
  for (int32_t i = 0; i < pNode->peersNum; ++i) {
    if (destRaftId->addr == pNode->peersId[i].addr) {
      epSet = &pNode->peersEpset[i];
      break;
    }
  }

  int32_t code = -1;
  if (pNode->syncSendMSg != NULL && epSet != NULL) {
    syncUtilMsgHtoN(pMsg->pCont);
    pMsg->info.noResp = 1;
    code = pNode->syncSendMSg(epSet, pMsg);
  }

  if (code < 0) {
    sError("vgId:%d, sync send msg by id error, epset:%p dnode:%d addr:%" PRId64 " err:0x%x", pNode->vgId, epSet,
           DID(destRaftId), destRaftId->addr, terrno);
    rpcFreeCont(pMsg->pCont);
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
  }

  return code;
}

inline bool syncNodeInConfig(SSyncNode* pNode, const SSyncCfg* pCfg) {
  bool b1 = false;
  bool b2 = false;

  for (int32_t i = 0; i < pCfg->replicaNum; ++i) {
    if (strcmp(pCfg->nodeInfo[i].nodeFqdn, pNode->myNodeInfo.nodeFqdn) == 0 &&
        pCfg->nodeInfo[i].nodePort == pNode->myNodeInfo.nodePort) {
      b1 = true;
      break;
    }
  }

  for (int32_t i = 0; i < pCfg->replicaNum; ++i) {
    SRaftId raftId = {
        .addr = SYNC_ADDR(&pCfg->nodeInfo[i]),
        .vgId = pNode->vgId,
    };

    if (syncUtilSameId(&raftId, &pNode->myRaftId)) {
      b2 = true;
      break;
    }
  }

  ASSERT(b1 == b2);
  return b1;
}

static bool syncIsConfigChanged(const SSyncCfg* pOldCfg, const SSyncCfg* pNewCfg) {
  if (pOldCfg->replicaNum != pNewCfg->replicaNum) return true;
  if (pOldCfg->myIndex != pNewCfg->myIndex) return true;
  for (int32_t i = 0; i < pOldCfg->replicaNum; ++i) {
    const SNodeInfo* pOldInfo = &pOldCfg->nodeInfo[i];
    const SNodeInfo* pNewInfo = &pNewCfg->nodeInfo[i];
    if (strcmp(pOldInfo->nodeFqdn, pNewInfo->nodeFqdn) != 0) return true;
    if (pOldInfo->nodePort != pNewInfo->nodePort) return true;
  }

  return false;
}

void syncNodeDoConfigChange(SSyncNode* pSyncNode, SSyncCfg* pNewConfig, SyncIndex lastConfigChangeIndex) {
  SSyncCfg oldConfig = pSyncNode->raftCfg.cfg;
  if (!syncIsConfigChanged(&oldConfig, pNewConfig)) {
    sInfo("vgId:1, sync not reconfig since not changed");
    return;
  }

  pSyncNode->raftCfg.cfg = *pNewConfig;
  pSyncNode->raftCfg.lastConfigIndex = lastConfigChangeIndex;

  pSyncNode->configChangeNum++;

  bool IamInOld = syncNodeInConfig(pSyncNode, &oldConfig);
  bool IamInNew = syncNodeInConfig(pSyncNode, pNewConfig);

  bool isDrop = false;
  bool isAdd = false;

  if (IamInOld && !IamInNew) {
    isDrop = true;
  } else {
    isDrop = false;
  }

  if (!IamInOld && IamInNew) {
    isAdd = true;
  } else {
    isAdd = false;
  }

  // log begin config change
  sNInfo(pSyncNode, "begin do config change, from %d to %d, replicas:%d", pSyncNode->vgId, oldConfig.replicaNum,
         pNewConfig->replicaNum);

  if (IamInNew) {
    pSyncNode->raftCfg.isStandBy = 0;  // change isStandBy to normal
  }
  if (isDrop) {
    pSyncNode->raftCfg.isStandBy = 1;  // set standby
  }

  // add last config index
  syncAddCfgIndex(pSyncNode, lastConfigChangeIndex);

  if (IamInNew) {
    //-----------------------------------------
    int32_t ret = 0;

    // save snapshot senders
    int32_t oldReplicaNum = pSyncNode->replicaNum;
    SRaftId oldReplicasId[TSDB_MAX_REPLICA];
    memcpy(oldReplicasId, pSyncNode->replicasId, sizeof(oldReplicasId));
    SSyncSnapshotSender* oldSenders[TSDB_MAX_REPLICA];
    for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
      oldSenders[i] = pSyncNode->senders[i];
      sSTrace(oldSenders[i], "snapshot sender save old");
    }

    // init internal
    pSyncNode->myNodeInfo = pSyncNode->raftCfg.cfg.nodeInfo[pSyncNode->raftCfg.cfg.myIndex];
    syncUtilNodeInfo2RaftId(&pSyncNode->myNodeInfo, pSyncNode->vgId, &pSyncNode->myRaftId);

    // init peersNum, peers, peersId
    pSyncNode->peersNum = pSyncNode->raftCfg.cfg.replicaNum - 1;
    int32_t j = 0;
    for (int32_t i = 0; i < pSyncNode->raftCfg.cfg.replicaNum; ++i) {
      if (i != pSyncNode->raftCfg.cfg.myIndex) {
        pSyncNode->peersNodeInfo[j] = pSyncNode->raftCfg.cfg.nodeInfo[i];
        syncUtilNodeInfo2EpSet(&pSyncNode->peersNodeInfo[j], &pSyncNode->peersEpset[j]);
        j++;
      }
    }
    for (int32_t i = 0; i < pSyncNode->peersNum; ++i) {
      syncUtilNodeInfo2RaftId(&pSyncNode->peersNodeInfo[i], pSyncNode->vgId, &pSyncNode->peersId[i]);
    }

    // init replicaNum, replicasId
    pSyncNode->replicaNum = pSyncNode->raftCfg.cfg.replicaNum;
    for (int32_t i = 0; i < pSyncNode->raftCfg.cfg.replicaNum; ++i) {
      syncUtilNodeInfo2RaftId(&pSyncNode->raftCfg.cfg.nodeInfo[i], pSyncNode->vgId, &pSyncNode->replicasId[i]);
    }

    // update quorum first
    pSyncNode->quorum = syncUtilQuorum(pSyncNode->raftCfg.cfg.replicaNum);

    syncIndexMgrUpdate(pSyncNode->pNextIndex, pSyncNode);
    syncIndexMgrUpdate(pSyncNode->pMatchIndex, pSyncNode);
    voteGrantedUpdate(pSyncNode->pVotesGranted, pSyncNode);
    votesRespondUpdate(pSyncNode->pVotesRespond, pSyncNode);

    // reset snapshot senders

    // clear new
    for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
      pSyncNode->senders[i] = NULL;
    }

    // reset new
    for (int32_t i = 0; i < pSyncNode->replicaNum; ++i) {
      // reset sender
      bool reset = false;
      for (int32_t j = 0; j < TSDB_MAX_REPLICA; ++j) {
        if (syncUtilSameId(&(pSyncNode->replicasId)[i], &oldReplicasId[j]) && oldSenders[j] != NULL) {
          sNTrace(pSyncNode, "snapshot sender reset for:%" PRId64 ", newIndex:%d, dnode:%d, %p",
                  (pSyncNode->replicasId)[i].addr, i, DID(&pSyncNode->replicasId[i]), oldSenders[j]);

          pSyncNode->senders[i] = oldSenders[j];
          oldSenders[j] = NULL;
          reset = true;

          // reset replicaIndex
          int32_t oldreplicaIndex = pSyncNode->senders[i]->replicaIndex;
          pSyncNode->senders[i]->replicaIndex = i;

          sNTrace(pSyncNode, "snapshot sender udpate replicaIndex from %d to %d, dnode:%d, %p, reset:%d",
                  oldreplicaIndex, i, DID(&pSyncNode->replicasId[i]), pSyncNode->senders[i], reset);

          break;
        }
      }
    }

    // create new
    for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
      if (pSyncNode->senders[i] == NULL) {
        pSyncNode->senders[i] = snapshotSenderCreate(pSyncNode, i);
        if (pSyncNode->senders[i] == NULL) {
          // will be created later while send snapshot
          sSError(pSyncNode->senders[i], "snapshot sender create failed while reconfig");
        } else {
          sSDebug(pSyncNode->senders[i], "snapshot sender create while reconfig, data:%p", pSyncNode->senders[i]);
        }
      } else {
        sSDebug(pSyncNode->senders[i], "snapshot sender already exist, data:%p", pSyncNode->senders[i]);
      }
    }

    // free old
    for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
      if (oldSenders[i] != NULL) {
        sSDebug(oldSenders[i], "snapshot sender destroy old, data:%p replica-index:%d", oldSenders[i], i);
        snapshotSenderDestroy(oldSenders[i]);
        oldSenders[i] = NULL;
      }
    }

    // persist cfg
    syncWriteCfgFile(pSyncNode);

    // change isStandBy to normal (election timeout)
    if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
      syncNodeBecomeLeader(pSyncNode, "");

      // Raft 3.6.2 Committing entries from previous terms
      syncNodeAppendNoop(pSyncNode);
      // syncMaybeAdvanceCommitIndex(pSyncNode);

    } else {
      syncNodeBecomeFollower(pSyncNode, "");
    }
  } else {
    // persist cfg
    syncWriteCfgFile(pSyncNode);
    sNInfo(pSyncNode, "do not config change from %d to %d", oldConfig.replicaNum, pNewConfig->replicaNum);
  }

_END:
  // log end config change
  sNInfo(pSyncNode, "end do config change, from %d to %d", oldConfig.replicaNum, pNewConfig->replicaNum);
}

// raft state change --------------
void syncNodeUpdateTerm(SSyncNode* pSyncNode, SyncTerm term) {
  if (term > raftStoreGetTerm(pSyncNode)) {
    raftStoreSetTerm(pSyncNode, term);
    char tmpBuf[64];
    snprintf(tmpBuf, sizeof(tmpBuf), "update term to %" PRId64, term);
    syncNodeBecomeFollower(pSyncNode, tmpBuf);
    raftStoreClearVote(pSyncNode);
  }
}

void syncNodeUpdateTermWithoutStepDown(SSyncNode* pSyncNode, SyncTerm term) {
  if (term > raftStoreGetTerm(pSyncNode)) {
    raftStoreSetTerm(pSyncNode, term);
  }
}

void syncNodeStepDown(SSyncNode* pSyncNode, SyncTerm newTerm) {
  SyncTerm currentTerm = raftStoreGetTerm(pSyncNode);
  if (currentTerm > newTerm) {
    sNTrace(pSyncNode, "step down, ignore, new-term:%" PRId64 ", current-term:%" PRId64, newTerm, currentTerm);
    return;
  }

  do {
    sNTrace(pSyncNode, "step down, new-term:%" PRId64 ", current-term:%" PRId64, newTerm, currentTerm);
  } while (0);

  if (currentTerm < newTerm) {
    raftStoreSetTerm(pSyncNode, newTerm);
    char tmpBuf[64];
    snprintf(tmpBuf, sizeof(tmpBuf), "step down, update term to %" PRId64, newTerm);
    syncNodeBecomeFollower(pSyncNode, tmpBuf);
    raftStoreClearVote(pSyncNode);

  } else {
    if (pSyncNode->state != TAOS_SYNC_STATE_FOLLOWER) {
      syncNodeBecomeFollower(pSyncNode, "step down");
    }
  }
}

void syncNodeLeaderChangeRsp(SSyncNode* pSyncNode) { syncRespCleanRsp(pSyncNode->pSyncRespMgr); }

void syncNodeBecomeFollower(SSyncNode* pSyncNode, const char* debugStr) {
  // maybe clear leader cache
  if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    pSyncNode->leaderCache = EMPTY_RAFT_ID;
  }

  pSyncNode->hbSlowNum = 0;

  // state change
  pSyncNode->state = TAOS_SYNC_STATE_FOLLOWER;
  syncNodeStopHeartbeatTimer(pSyncNode);

  // reset elect timer
  syncNodeResetElectTimer(pSyncNode);

  // send rsp to client
  syncNodeLeaderChangeRsp(pSyncNode);

  // call back
  if (pSyncNode->pFsm != NULL && pSyncNode->pFsm->FpBecomeFollowerCb != NULL) {
    pSyncNode->pFsm->FpBecomeFollowerCb(pSyncNode->pFsm);
  }

  // min match index
  pSyncNode->minMatchIndex = SYNC_INDEX_INVALID;

  // reset log buffer
  syncLogBufferReset(pSyncNode->pLogBuf, pSyncNode);

  // trace log
  sNTrace(pSyncNode, "become follower %s", debugStr);
}

// TLA+ Spec
// \* Candidate i transitions to leader.
// BecomeLeader(i) ==
//     /\ state[i] = Candidate
//     /\ votesGranted[i] \in Quorum
//     /\ state'      = [state EXCEPT ![i] = Leader]
//     /\ nextIndex'  = [nextIndex EXCEPT ![i] =
//                          [j \in Server |-> Len(log[i]) + 1]]
//     /\ matchIndex' = [matchIndex EXCEPT ![i] =
//                          [j \in Server |-> 0]]
//     /\ elections'  = elections \cup
//                          {[eterm     |-> currentTerm[i],
//                            eleader   |-> i,
//                            elog      |-> log[i],
//                            evotes    |-> votesGranted[i],
//                            evoterLog |-> voterLog[i]]}
//     /\ UNCHANGED <<messages, currentTerm, votedFor, candidateVars, logVars>>
//
void syncNodeBecomeLeader(SSyncNode* pSyncNode, const char* debugStr) {
  pSyncNode->leaderTime = taosGetTimestampMs();

  pSyncNode->becomeLeaderNum++;
  pSyncNode->hbrSlowNum = 0;

  // reset restoreFinish
  pSyncNode->restoreFinish = false;

  // state change
  pSyncNode->state = TAOS_SYNC_STATE_LEADER;

  // set leader cache
  pSyncNode->leaderCache = pSyncNode->myRaftId;

  for (int32_t i = 0; i < pSyncNode->pNextIndex->replicaNum; ++i) {
    SyncIndex lastIndex;
    SyncTerm  lastTerm;
    int32_t   code = syncNodeGetLastIndexTerm(pSyncNode, &lastIndex, &lastTerm);
    ASSERT(code == 0);
    pSyncNode->pNextIndex->index[i] = lastIndex + 1;
  }

  for (int32_t i = 0; i < pSyncNode->pMatchIndex->replicaNum; ++i) {
    // maybe overwrite myself, no harm
    // just do it!
    pSyncNode->pMatchIndex->index[i] = SYNC_INDEX_INVALID;
  }

  // init peer mgr
  syncNodePeerStateInit(pSyncNode);

#if 0
  // update sender private term
  SSyncSnapshotSender* pMySender = syncNodeGetSnapshotSender(pSyncNode, &(pSyncNode->myRaftId));
  if (pMySender != NULL) {
    for (int32_t i = 0; i < pSyncNode->pMatchIndex->replicaNum; ++i) {
      if (pSyncNode->senders[i]->privateTerm > pMySender->privateTerm) {
        pMySender->privateTerm = pSyncNode->senders[i]->privateTerm;
      }
    }
    (pMySender->privateTerm) += 100;
  }
#endif

  // close receiver
  if (snapshotReceiverIsStart(pSyncNode->pNewNodeReceiver)) {
    snapshotReceiverStop(pSyncNode->pNewNodeReceiver);
  }

  // stop elect timer
  syncNodeStopElectTimer(pSyncNode);

  // start heartbeat timer
  syncNodeStartHeartbeatTimer(pSyncNode);

  // send heartbeat right now
  syncNodeHeartbeatPeers(pSyncNode);

  // call back
  if (pSyncNode->pFsm != NULL && pSyncNode->pFsm->FpBecomeLeaderCb != NULL) {
    pSyncNode->pFsm->FpBecomeLeaderCb(pSyncNode->pFsm);
  }

  // min match index
  pSyncNode->minMatchIndex = SYNC_INDEX_INVALID;

  // reset log buffer
  syncLogBufferReset(pSyncNode->pLogBuf, pSyncNode);

  // trace log
  sNInfo(pSyncNode, "become leader %s", debugStr);
}

void syncNodeCandidate2Leader(SSyncNode* pSyncNode) {
  ASSERT(pSyncNode->state == TAOS_SYNC_STATE_CANDIDATE);
  bool granted = voteGrantedMajority(pSyncNode->pVotesGranted);
  if (!granted) {
    sError("vgId:%d, not granted by majority.", pSyncNode->vgId);
    return;
  }
  syncNodeBecomeLeader(pSyncNode, "candidate to leader");

  sNTrace(pSyncNode, "state change syncNodeCandidate2Leader");

  int32_t ret = syncNodeAppendNoop(pSyncNode);
  if (ret < 0) {
    sError("vgId:%d, failed to append noop entry since %s", pSyncNode->vgId, terrstr());
  }

  SyncIndex lastIndex = pSyncNode->pLogStore->syncLogLastIndex(pSyncNode->pLogStore);
  ASSERT(lastIndex >= 0);
  sInfo("vgId:%d, become leader. term:%" PRId64 ", commit index:%" PRId64 ", last index:%" PRId64 "", pSyncNode->vgId,
        raftStoreGetTerm(pSyncNode), pSyncNode->commitIndex, lastIndex);
}

bool syncNodeIsMnode(SSyncNode* pSyncNode) { return (pSyncNode->vgId == 1); }

int32_t syncNodePeerStateInit(SSyncNode* pSyncNode) {
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    pSyncNode->peerStates[i].lastSendIndex = SYNC_INDEX_INVALID;
    pSyncNode->peerStates[i].lastSendTime = 0;
  }

  return 0;
}

void syncNodeFollower2Candidate(SSyncNode* pSyncNode) {
  ASSERT(pSyncNode->state == TAOS_SYNC_STATE_FOLLOWER);
  pSyncNode->state = TAOS_SYNC_STATE_CANDIDATE;
  SyncIndex lastIndex = pSyncNode->pLogStore->syncLogLastIndex(pSyncNode->pLogStore);
  sInfo("vgId:%d, become candidate from follower. term:%" PRId64 ", commit index:%" PRId64 ", last index:%" PRId64,
        pSyncNode->vgId, raftStoreGetTerm(pSyncNode), pSyncNode->commitIndex, lastIndex);

  sNTrace(pSyncNode, "follower to candidate");
}

void syncNodeLeader2Follower(SSyncNode* pSyncNode) {
  ASSERT(pSyncNode->state == TAOS_SYNC_STATE_LEADER);
  syncNodeBecomeFollower(pSyncNode, "leader to follower");
  SyncIndex lastIndex = pSyncNode->pLogStore->syncLogLastIndex(pSyncNode->pLogStore);
  sInfo("vgId:%d, become follower from leader. term:%" PRId64 ", commit index:%" PRId64 ", last index:%" PRId64,
        pSyncNode->vgId, raftStoreGetTerm(pSyncNode), pSyncNode->commitIndex, lastIndex);

  sNTrace(pSyncNode, "leader to follower");
}

void syncNodeCandidate2Follower(SSyncNode* pSyncNode) {
  ASSERT(pSyncNode->state == TAOS_SYNC_STATE_CANDIDATE);
  syncNodeBecomeFollower(pSyncNode, "candidate to follower");
  SyncIndex lastIndex = pSyncNode->pLogStore->syncLogLastIndex(pSyncNode->pLogStore);
  sInfo("vgId:%d, become follower from candidate. term:%" PRId64 ", commit index:%" PRId64 ", last index:%" PRId64,
        pSyncNode->vgId, raftStoreGetTerm(pSyncNode), pSyncNode->commitIndex, lastIndex);

  sNTrace(pSyncNode, "candidate to follower");
}

// just called by syncNodeVoteForSelf
// need assert
void syncNodeVoteForTerm(SSyncNode* pSyncNode, SyncTerm term, SRaftId* pRaftId) {
  ASSERT(term == raftStoreGetTerm(pSyncNode));
  bool voted = raftStoreHasVoted(pSyncNode);
  ASSERT(!voted);

  raftStoreVote(pSyncNode, pRaftId);
}

// simulate get vote from outside
void syncNodeVoteForSelf(SSyncNode* pSyncNode, SyncTerm currentTerm) {
  syncNodeVoteForTerm(pSyncNode, currentTerm, &pSyncNode->myRaftId);

  SRpcMsg rpcMsg = {0};
  int32_t ret = syncBuildRequestVoteReply(&rpcMsg, pSyncNode->vgId);
  if (ret != 0) return;

  SyncRequestVoteReply* pMsg = rpcMsg.pCont;
  pMsg->srcId = pSyncNode->myRaftId;
  pMsg->destId = pSyncNode->myRaftId;
  pMsg->term = currentTerm;
  pMsg->voteGranted = true;

  voteGrantedVote(pSyncNode->pVotesGranted, pMsg);
  votesRespondAdd(pSyncNode->pVotesRespond, pMsg);
  rpcFreeCont(rpcMsg.pCont);
}

// return if has a snapshot
bool syncNodeHasSnapshot(SSyncNode* pSyncNode) {
  bool      ret = false;
  SSnapshot snapshot = {.data = NULL, .lastApplyIndex = -1, .lastApplyTerm = 0, .lastConfigIndex = -1};
  if (pSyncNode->pFsm->FpGetSnapshotInfo != NULL) {
    pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
    if (snapshot.lastApplyIndex >= SYNC_INDEX_BEGIN) {
      ret = true;
    }
  }
  return ret;
}

// return max(logLastIndex, snapshotLastIndex)
// if no snapshot and log, return -1
SyncIndex syncNodeGetLastIndex(const SSyncNode* pSyncNode) {
  SSnapshot snapshot = {.data = NULL, .lastApplyIndex = -1, .lastApplyTerm = 0, .lastConfigIndex = -1};
  if (pSyncNode->pFsm->FpGetSnapshotInfo != NULL) {
    pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
  }
  SyncIndex logLastIndex = pSyncNode->pLogStore->syncLogLastIndex(pSyncNode->pLogStore);

  SyncIndex lastIndex = logLastIndex > snapshot.lastApplyIndex ? logLastIndex : snapshot.lastApplyIndex;
  return lastIndex;
}

// return the last term of snapshot and log
// if error, return SYNC_TERM_INVALID (by syncLogLastTerm)
SyncTerm syncNodeGetLastTerm(SSyncNode* pSyncNode) {
  SyncTerm lastTerm = 0;
  if (syncNodeHasSnapshot(pSyncNode)) {
    // has snapshot
    SSnapshot snapshot = {.data = NULL, .lastApplyIndex = -1, .lastApplyTerm = 0, .lastConfigIndex = -1};
    if (pSyncNode->pFsm->FpGetSnapshotInfo != NULL) {
      pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
    }

    SyncIndex logLastIndex = pSyncNode->pLogStore->syncLogLastIndex(pSyncNode->pLogStore);
    if (logLastIndex > snapshot.lastApplyIndex) {
      lastTerm = pSyncNode->pLogStore->syncLogLastTerm(pSyncNode->pLogStore);
    } else {
      lastTerm = snapshot.lastApplyTerm;
    }

  } else {
    // no snapshot
    lastTerm = pSyncNode->pLogStore->syncLogLastTerm(pSyncNode->pLogStore);
  }

  return lastTerm;
}

// get last index and term along with snapshot
int32_t syncNodeGetLastIndexTerm(SSyncNode* pSyncNode, SyncIndex* pLastIndex, SyncTerm* pLastTerm) {
  *pLastIndex = syncNodeGetLastIndex(pSyncNode);
  *pLastTerm = syncNodeGetLastTerm(pSyncNode);
  return 0;
}

// return append-entries first try index
SyncIndex syncNodeSyncStartIndex(SSyncNode* pSyncNode) {
  SyncIndex syncStartIndex = syncNodeGetLastIndex(pSyncNode) + 1;
  return syncStartIndex;
}

// if index > 0, return index - 1
// else, return -1
SyncIndex syncNodeGetPreIndex(SSyncNode* pSyncNode, SyncIndex index) {
  SyncIndex preIndex = index - 1;
  if (preIndex < SYNC_INDEX_INVALID) {
    preIndex = SYNC_INDEX_INVALID;
  }

  return preIndex;
}

// if index < 0, return SYNC_TERM_INVALID
// if index == 0, return 0
// if index > 0, return preTerm
// if error, return SYNC_TERM_INVALID
SyncTerm syncNodeGetPreTerm(SSyncNode* pSyncNode, SyncIndex index) {
  if (index < SYNC_INDEX_BEGIN) {
    return SYNC_TERM_INVALID;
  }

  if (index == SYNC_INDEX_BEGIN) {
    return 0;
  }

  SyncTerm  preTerm = 0;
  SyncIndex preIndex = index - 1;

  SSyncRaftEntry* pPreEntry = NULL;
  SLRUCache*      pCache = pSyncNode->pLogStore->pCache;
  LRUHandle*      h = taosLRUCacheLookup(pCache, &preIndex, sizeof(preIndex));
  int32_t         code = 0;
  if (h) {
    pPreEntry = (SSyncRaftEntry*)taosLRUCacheValue(pCache, h);
    code = 0;

    pSyncNode->pLogStore->cacheHit++;
    sNTrace(pSyncNode, "hit cache index:%" PRId64 ", bytes:%u, %p", preIndex, pPreEntry->bytes, pPreEntry);

  } else {
    pSyncNode->pLogStore->cacheMiss++;
    sNTrace(pSyncNode, "miss cache index:%" PRId64, preIndex);

    code = pSyncNode->pLogStore->syncLogGetEntry(pSyncNode->pLogStore, preIndex, &pPreEntry);
  }

  SSnapshot snapshot = {.data = NULL,
                        .lastApplyIndex = SYNC_INDEX_INVALID,
                        .lastApplyTerm = SYNC_TERM_INVALID,
                        .lastConfigIndex = SYNC_INDEX_INVALID};

  if (code == 0) {
    ASSERT(pPreEntry != NULL);
    preTerm = pPreEntry->term;

    if (h) {
      taosLRUCacheRelease(pCache, h, false);
    } else {
      syncEntryDestroy(pPreEntry);
    }

    return preTerm;
  } else {
    if (pSyncNode->pFsm->FpGetSnapshotInfo != NULL) {
      pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
      if (snapshot.lastApplyIndex == preIndex) {
        return snapshot.lastApplyTerm;
      }
    }
  }

  sNError(pSyncNode, "sync node get pre term error, index:%" PRId64 ", snap-index:%" PRId64 ", snap-term:%" PRId64,
          index, snapshot.lastApplyIndex, snapshot.lastApplyTerm);
  return SYNC_TERM_INVALID;
}

// get pre index and term of "index"
int32_t syncNodeGetPreIndexTerm(SSyncNode* pSyncNode, SyncIndex index, SyncIndex* pPreIndex, SyncTerm* pPreTerm) {
  *pPreIndex = syncNodeGetPreIndex(pSyncNode, index);
  *pPreTerm = syncNodeGetPreTerm(pSyncNode, index);
  return 0;
}

static void syncNodeEqPingTimer(void* param, void* tmrId) {
  if (!syncIsInit()) return;

  SSyncNode* pNode = param;
  if (atomic_load_64(&pNode->pingTimerLogicClockUser) <= atomic_load_64(&pNode->pingTimerLogicClock)) {
    SRpcMsg rpcMsg = {0};
    int32_t code = syncBuildTimeout(&rpcMsg, SYNC_TIMEOUT_PING, atomic_load_64(&pNode->pingTimerLogicClock),
                                    pNode->pingTimerMS, pNode);
    if (code != 0) {
      sError("failed to build ping msg");
      rpcFreeCont(rpcMsg.pCont);
      return;
    }

    // sTrace("enqueue ping msg");
    code = pNode->syncEqMsg(pNode->msgcb, &rpcMsg);
    if (code != 0) {
      sError("failed to sync enqueue ping msg since %s", terrstr());
      rpcFreeCont(rpcMsg.pCont);
      return;
    }

    taosTmrReset(syncNodeEqPingTimer, pNode->pingTimerMS, pNode, syncEnv()->pTimerManager, &pNode->pPingTimer);
  }
}

static void syncNodeEqElectTimer(void* param, void* tmrId) {
  if (!syncIsInit()) return;

  int64_t    rid = (int64_t)param;
  SSyncNode* pNode = syncNodeAcquire(rid);

  if (pNode == NULL) return;

  if (pNode->syncEqMsg == NULL) {
    syncNodeRelease(pNode);
    return;
  }

  int64_t tsNow = taosGetTimestampMs();
  if (tsNow < pNode->electTimerParam.executeTime) {
    syncNodeRelease(pNode);
    return;
  }

  SRpcMsg rpcMsg = {0};
  int32_t code =
      syncBuildTimeout(&rpcMsg, SYNC_TIMEOUT_ELECTION, pNode->electTimerParam.logicClock, pNode->electTimerMS, pNode);

  if (code != 0) {
    sError("failed to build elect msg");
    syncNodeRelease(pNode);
    return;
  }

  SyncTimeout* pTimeout = rpcMsg.pCont;
  sNTrace(pNode, "enqueue elect msg lc:%" PRId64, pTimeout->logicClock);

  code = pNode->syncEqMsg(pNode->msgcb, &rpcMsg);
  if (code != 0) {
    sError("failed to sync enqueue elect msg since %s", terrstr());
    rpcFreeCont(rpcMsg.pCont);
    syncNodeRelease(pNode);
    return;
  }

  syncNodeRelease(pNode);
}

static void syncNodeEqHeartbeatTimer(void* param, void* tmrId) {
  if (!syncIsInit()) return;

  SSyncNode* pNode = param;
  if (pNode->replicaNum > 1) {
    if (atomic_load_64(&pNode->heartbeatTimerLogicClockUser) <= atomic_load_64(&pNode->heartbeatTimerLogicClock)) {
      SRpcMsg rpcMsg = {0};
      int32_t code = syncBuildTimeout(&rpcMsg, SYNC_TIMEOUT_HEARTBEAT, atomic_load_64(&pNode->heartbeatTimerLogicClock),
                                      pNode->heartbeatTimerMS, pNode);

      if (code != 0) {
        sError("failed to build heartbeat msg");
        return;
      }

      sTrace("vgId:%d, enqueue heartbeat timer", pNode->vgId);
      code = pNode->syncEqMsg(pNode->msgcb, &rpcMsg);
      if (code != 0) {
        sError("failed to enqueue heartbeat msg since %s", terrstr());
        rpcFreeCont(rpcMsg.pCont);
        return;
      }

      taosTmrReset(syncNodeEqHeartbeatTimer, pNode->heartbeatTimerMS, pNode, syncEnv()->pTimerManager,
                   &pNode->pHeartbeatTimer);

    } else {
      sTrace("==syncNodeEqHeartbeatTimer== heartbeatTimerLogicClock:%" PRId64 ", heartbeatTimerLogicClockUser:%" PRId64,
             pNode->heartbeatTimerLogicClock, pNode->heartbeatTimerLogicClockUser);
    }
  }
}

static void syncNodeEqPeerHeartbeatTimer(void* param, void* tmrId) {
  int64_t hbDataRid = (int64_t)param;
  int64_t tsNow = taosGetTimestampMs();

  SSyncHbTimerData* pData = syncHbTimerDataAcquire(hbDataRid);
  if (pData == NULL) {
    sError("hb timer get pData NULL, %" PRId64, hbDataRid);
    return;
  }

  SSyncNode* pSyncNode = syncNodeAcquire(pData->syncNodeRid);
  if (pSyncNode == NULL) {
    syncHbTimerDataRelease(pData);
    sError("hb timer get pSyncNode NULL");
    return;
  }

  SSyncTimer* pSyncTimer = pData->pTimer;

  if (!pSyncNode->isStart) {
    syncNodeRelease(pSyncNode);
    syncHbTimerDataRelease(pData);
    sError("vgId:%d, hb timer sync node already stop", pSyncNode->vgId);
    return;
  }

  if (pSyncNode->state != TAOS_SYNC_STATE_LEADER) {
    syncNodeRelease(pSyncNode);
    syncHbTimerDataRelease(pData);
    sError("vgId:%d, hb timer sync node not leader", pSyncNode->vgId);
    return;
  }

  // sTrace("vgId:%d, eq peer hb timer", pSyncNode->vgId);

  if (pSyncNode->replicaNum > 1) {
    int64_t timerLogicClock = atomic_load_64(&pSyncTimer->logicClock);
    int64_t msgLogicClock = atomic_load_64(&pData->logicClock);

    if (timerLogicClock == msgLogicClock) {
      if (tsNow > pData->execTime) {
        pData->execTime += pSyncTimer->timerMS;

        SRpcMsg rpcMsg = {0};
        (void)syncBuildHeartbeat(&rpcMsg, pSyncNode->vgId);

        pSyncNode->minMatchIndex = syncMinMatchIndex(pSyncNode);

        SyncHeartbeat* pSyncMsg = rpcMsg.pCont;
        pSyncMsg->srcId = pSyncNode->myRaftId;
        pSyncMsg->destId = pData->destId;
        pSyncMsg->term = raftStoreGetTerm(pSyncNode);
        pSyncMsg->commitIndex = pSyncNode->commitIndex;
        pSyncMsg->minMatchIndex = pSyncNode->minMatchIndex;
        pSyncMsg->privateTerm = 0;
        pSyncMsg->timeStamp = tsNow;

        // update reset time
        int64_t timerElapsed = tsNow - pSyncTimer->timeStamp;
        pSyncTimer->timeStamp = tsNow;

        // send msg
        syncLogSendHeartbeat(pSyncNode, pSyncMsg, false, timerElapsed, pData->execTime);
        syncNodeSendHeartbeat(pSyncNode, &pSyncMsg->destId, &rpcMsg);
      } else {
      }

      if (syncIsInit()) {
        // sTrace("vgId:%d, reset peer hb timer", pSyncNode->vgId);
        taosTmrReset(syncNodeEqPeerHeartbeatTimer, pSyncTimer->timerMS / HEARTBEAT_TICK_NUM, (void*)hbDataRid,
                     syncEnv()->pTimerManager, &pSyncTimer->pTimer);
      } else {
        sError("sync env is stop, reset peer hb timer error");
      }

    } else {
      sTrace("vgId:%d, do not send hb, timerLogicClock:%" PRId64 ", msgLogicClock:%" PRId64 "", pSyncNode->vgId,
             timerLogicClock, msgLogicClock);
    }
  }

  syncHbTimerDataRelease(pData);
  syncNodeRelease(pSyncNode);
}

static void deleteCacheEntry(const void* key, size_t keyLen, void* value) { taosMemoryFree(value); }

int32_t syncCacheEntry(SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry, LRUHandle** h) {
  SSyncLogStoreData* pData = pLogStore->data;
  sNTrace(pData->pSyncNode, "in cache index:%" PRId64 ", bytes:%u, %p", pEntry->index, pEntry->bytes, pEntry);

  int32_t   code = 0;
  int32_t   entryLen = sizeof(*pEntry) + pEntry->dataLen;
  LRUStatus status = taosLRUCacheInsert(pLogStore->pCache, &pEntry->index, sizeof(pEntry->index), pEntry, entryLen,
                                        deleteCacheEntry, h, TAOS_LRU_PRIORITY_LOW);
  if (status != TAOS_LRU_STATUS_OK) {
    code = -1;
  }

  return code;
}

int32_t syncNodeAppend(SSyncNode* ths, SSyncRaftEntry* pEntry) {
  if (pEntry->dataLen < sizeof(SMsgHead)) {
    sError("vgId:%d, cannot append an invalid client request with no msg head. type:%s, dataLen:%d", ths->vgId,
           TMSG_INFO(pEntry->originalRpcType), pEntry->dataLen);
    syncEntryDestroy(pEntry);
    return -1;
  }

  // append to log buffer
  if (syncLogBufferAppend(ths->pLogBuf, ths, pEntry) < 0) {
    sError("vgId:%d, failed to enqueue sync log buffer, index:%" PRId64, ths->vgId, pEntry->index);
    ASSERT(terrno != 0);
    (void)syncLogFsmExecute(ths, ths->pFsm, ths->state, raftStoreGetTerm(ths), pEntry, terrno);
    syncEntryDestroy(pEntry);
    return -1;
  }

  // proceed match index, with replicating on needed
  SyncIndex matchIndex = syncLogBufferProceed(ths->pLogBuf, ths, NULL);

  sTrace("vgId:%d, append raft entry. index:%" PRId64 ", term:%" PRId64 " pBuf: [%" PRId64 " %" PRId64 " %" PRId64
         ", %" PRId64 ")",
         ths->vgId, pEntry->index, pEntry->term, ths->pLogBuf->startIndex, ths->pLogBuf->commitIndex,
         ths->pLogBuf->matchIndex, ths->pLogBuf->endIndex);

  // multi replica
  if (ths->replicaNum > 1) {
    return 0;
  }

  // single replica
  (void)syncNodeUpdateCommitIndex(ths, matchIndex);

  if (syncLogBufferCommit(ths->pLogBuf, ths, ths->commitIndex) < 0) {
    sError("vgId:%d, failed to commit until commitIndex:%" PRId64 "", ths->vgId, ths->commitIndex);
    return -1;
  }

  return 0;
}

bool syncNodeHeartbeatReplyTimeout(SSyncNode* pSyncNode) {
  if (pSyncNode->replicaNum == 1) {
    return false;
  }

  int32_t toCount = 0;
  int64_t tsNow = taosGetTimestampMs();
  for (int32_t i = 0; i < pSyncNode->peersNum; ++i) {
    int64_t recvTime = syncIndexMgrGetRecvTime(pSyncNode->pMatchIndex, &(pSyncNode->peersId[i]));
    if (recvTime == 0 || recvTime == -1) {
      continue;
    }

    if (tsNow - recvTime > tsHeartbeatTimeout) {
      toCount++;
    }
  }

  bool b = (toCount >= pSyncNode->quorum ? true : false);

  return b;
}

bool syncNodeSnapshotSending(SSyncNode* pSyncNode) {
  if (pSyncNode == NULL) return false;
  bool b = false;
  for (int32_t i = 0; i < pSyncNode->replicaNum; ++i) {
    if (pSyncNode->senders[i] != NULL && pSyncNode->senders[i]->start) {
      b = true;
      break;
    }
  }
  return b;
}

bool syncNodeSnapshotRecving(SSyncNode* pSyncNode) {
  if (pSyncNode == NULL) return false;
  if (pSyncNode->pNewNodeReceiver == NULL) return false;
  if (pSyncNode->pNewNodeReceiver->start) return true;
  return false;
}

static int32_t syncNodeAppendNoop(SSyncNode* ths) {
  SyncIndex index = syncLogBufferGetEndIndex(ths->pLogBuf);
  SyncTerm  term = raftStoreGetTerm(ths);

  SSyncRaftEntry* pEntry = syncEntryBuildNoop(term, index, ths->vgId);
  if (pEntry == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  int32_t ret = syncNodeAppend(ths, pEntry);
  return 0;
}

static int32_t syncNodeAppendNoopOld(SSyncNode* ths) {
  int32_t ret = 0;

  SyncIndex       index = ths->pLogStore->syncLogWriteIndex(ths->pLogStore);
  SyncTerm        term = raftStoreGetTerm(ths);
  SSyncRaftEntry* pEntry = syncEntryBuildNoop(term, index, ths->vgId);
  ASSERT(pEntry != NULL);

  LRUHandle* h = NULL;

  if (ths->state == TAOS_SYNC_STATE_LEADER) {
    int32_t code = ths->pLogStore->syncLogAppendEntry(ths->pLogStore, pEntry, false);
    if (code != 0) {
      sError("append noop error");
      return -1;
    }

    syncCacheEntry(ths->pLogStore, pEntry, &h);
  }

  if (h) {
    taosLRUCacheRelease(ths->pLogStore->pCache, h, false);
  } else {
    syncEntryDestroy(pEntry);
  }

  return ret;
}

int32_t syncNodeOnHeartbeat(SSyncNode* ths, const SRpcMsg* pRpcMsg) {
  SyncHeartbeat* pMsg = pRpcMsg->pCont;

  const STraceId* trace = &pRpcMsg->info.traceId;
  char            tbuf[40] = {0};
  TRACE_TO_STR(trace, tbuf);

  int64_t tsMs = taosGetTimestampMs();
  int64_t timeDiff = tsMs - pMsg->timeStamp;
  syncLogRecvHeartbeat(ths, pMsg, timeDiff, tbuf);

  SRpcMsg rpcMsg = {0};
  (void)syncBuildHeartbeatReply(&rpcMsg, ths->vgId);
  SyncTerm currentTerm = raftStoreGetTerm(ths);

  SyncHeartbeatReply* pMsgReply = rpcMsg.pCont;
  pMsgReply->destId = pMsg->srcId;
  pMsgReply->srcId = ths->myRaftId;
  pMsgReply->term = currentTerm;
  pMsgReply->privateTerm = 8864;  // magic number
  pMsgReply->startTime = ths->startTime;
  pMsgReply->timeStamp = tsMs;

  if (pMsg->term == currentTerm && ths->state != TAOS_SYNC_STATE_LEADER) {
    syncIndexMgrSetRecvTime(ths->pNextIndex, &(pMsg->srcId), tsMs);

    syncNodeResetElectTimer(ths);
    ths->minMatchIndex = pMsg->minMatchIndex;

    if (ths->state == TAOS_SYNC_STATE_FOLLOWER) {
      // syncNodeFollowerCommit(ths, pMsg->commitIndex);
      SRpcMsg rpcMsgLocalCmd = {0};
      (void)syncBuildLocalCmd(&rpcMsgLocalCmd, ths->vgId);

      SyncLocalCmd* pSyncMsg = rpcMsgLocalCmd.pCont;
      pSyncMsg->cmd = SYNC_LOCAL_CMD_FOLLOWER_CMT;
      pSyncMsg->commitIndex = pMsg->commitIndex;
      pSyncMsg->currentTerm = pMsg->term;
      SyncIndex fcIndex = pSyncMsg->commitIndex;

      if (ths->syncEqMsg != NULL && ths->msgcb != NULL) {
        int32_t code = ths->syncEqMsg(ths->msgcb, &rpcMsgLocalCmd);
        if (code != 0) {
          sError("vgId:%d, sync enqueue fc-commit msg error, code:%d", ths->vgId, code);
          rpcFreeCont(rpcMsgLocalCmd.pCont);
        } else {
          sTrace("vgId:%d, sync enqueue fc-commit msg, fc-index:%" PRId64, ths->vgId, fcIndex);
        }
      }
    }
  }

  if (pMsg->term >= currentTerm && ths->state != TAOS_SYNC_STATE_FOLLOWER) {
    // syncNodeStepDown(ths, pMsg->term);
    SRpcMsg rpcMsgLocalCmd = {0};
    (void)syncBuildLocalCmd(&rpcMsgLocalCmd, ths->vgId);

    SyncLocalCmd* pSyncMsg = rpcMsgLocalCmd.pCont;
    pSyncMsg->cmd = SYNC_LOCAL_CMD_STEP_DOWN;
    pSyncMsg->currentTerm = pMsg->term;
    pSyncMsg->commitIndex = pMsg->commitIndex;

    if (ths->syncEqMsg != NULL && ths->msgcb != NULL) {
      int32_t code = ths->syncEqMsg(ths->msgcb, &rpcMsgLocalCmd);
      if (code != 0) {
        sError("vgId:%d, sync enqueue step-down msg error, code:%d", ths->vgId, code);
        rpcFreeCont(rpcMsgLocalCmd.pCont);
      } else {
        sTrace("vgId:%d, sync enqueue step-down msg, new-term:%" PRId64, ths->vgId, pSyncMsg->currentTerm);
      }
    }
  }

  /*
    // htonl
    SMsgHead* pHead = rpcMsg.pCont;
    pHead->contLen = htonl(pHead->contLen);
    pHead->vgId = htonl(pHead->vgId);
  */

  // reply
  syncNodeSendMsgById(&pMsgReply->destId, ths, &rpcMsg);
  return 0;
}

int32_t syncNodeOnHeartbeatReply(SSyncNode* ths, const SRpcMsg* pRpcMsg) {
  const STraceId* trace = &pRpcMsg->info.traceId;
  char            tbuf[40] = {0};
  TRACE_TO_STR(trace, tbuf);

  SyncHeartbeatReply* pMsg = pRpcMsg->pCont;
  SSyncLogReplMgr*    pMgr = syncNodeGetLogReplMgr(ths, &pMsg->srcId);
  if (pMgr == NULL) {
    sError("vgId:%d, failed to get log repl mgr for the peer at addr 0x016%" PRIx64 "", ths->vgId, pMsg->srcId.addr);
    return -1;
  }

  int64_t tsMs = taosGetTimestampMs();
  syncLogRecvHeartbeatReply(ths, pMsg, tsMs - pMsg->timeStamp, tbuf);

  syncIndexMgrSetRecvTime(ths->pMatchIndex, &pMsg->srcId, tsMs);

  return syncLogReplMgrProcessHeartbeatReply(pMgr, ths, pMsg);
}

int32_t syncNodeOnHeartbeatReplyOld(SSyncNode* ths, const SRpcMsg* pRpcMsg) {
  SyncHeartbeatReply* pMsg = pRpcMsg->pCont;

  const STraceId* trace = &pRpcMsg->info.traceId;
  char            tbuf[40] = {0};
  TRACE_TO_STR(trace, tbuf);

  int64_t tsMs = taosGetTimestampMs();
  int64_t timeDiff = tsMs - pMsg->timeStamp;
  syncLogRecvHeartbeatReply(ths, pMsg, timeDiff, tbuf);

  // update last reply time, make decision whether the other node is alive or not
  syncIndexMgrSetRecvTime(ths->pMatchIndex, &pMsg->srcId, tsMs);
  return 0;
}

int32_t syncNodeOnLocalCmd(SSyncNode* ths, const SRpcMsg* pRpcMsg) {
  SyncLocalCmd* pMsg = pRpcMsg->pCont;
  syncLogRecvLocalCmd(ths, pMsg, "");

  if (pMsg->cmd == SYNC_LOCAL_CMD_STEP_DOWN) {
    syncNodeStepDown(ths, pMsg->currentTerm);

  } else if (pMsg->cmd == SYNC_LOCAL_CMD_FOLLOWER_CMT) {
    if (syncLogBufferIsEmpty(ths->pLogBuf)) {
      sError("vgId:%d, sync log buffer is empty.", ths->vgId);
      return 0;
    }
    SyncTerm matchTerm = syncLogBufferGetLastMatchTerm(ths->pLogBuf);
    if (pMsg->currentTerm == matchTerm) {
      (void)syncNodeUpdateCommitIndex(ths, pMsg->commitIndex);
    }
    if (syncLogBufferCommit(ths->pLogBuf, ths, ths->commitIndex) < 0) {
      sError("vgId:%d, failed to commit raft log since %s. commit index:%" PRId64 "", ths->vgId, terrstr(),
             ths->commitIndex);
    }
  } else {
    sError("error local cmd");
  }

  return 0;
}

// TLA+ Spec
// ClientRequest(i, v) ==
//     /\ state[i] = Leader
//     /\ LET entry == [term  |-> currentTerm[i],
//                      value |-> v]
//            newLog == Append(log[i], entry)
//        IN  log' = [log EXCEPT ![i] = newLog]
//     /\ UNCHANGED <<messages, serverVars, candidateVars,
//                    leaderVars, commitIndex>>
//

int32_t syncNodeOnClientRequest(SSyncNode* ths, SRpcMsg* pMsg, SyncIndex* pRetIndex) {
  sNTrace(ths, "on client request");

  int32_t code = 0;

  SyncIndex       index = syncLogBufferGetEndIndex(ths->pLogBuf);
  SyncTerm        term = raftStoreGetTerm(ths);
  SSyncRaftEntry* pEntry = NULL;
  if (pMsg->msgType == TDMT_SYNC_CLIENT_REQUEST) {
    pEntry = syncEntryBuildFromClientRequest(pMsg->pCont, term, index);
  } else {
    pEntry = syncEntryBuildFromRpcMsg(pMsg, term, index);
  }

  if (pEntry == NULL) {
    sError("vgId:%d, failed to process client request since %s.", ths->vgId, terrstr());
    return -1;
  }

  if (ths->state == TAOS_SYNC_STATE_LEADER) {
    if (pRetIndex) {
      (*pRetIndex) = index;
    }

    int32_t code = syncNodeAppend(ths, pEntry);
    return code;
  } else {
    syncEntryDestroy(pEntry);
    pEntry = NULL;
    return -1;
  }
}

const char* syncStr(ESyncState state) {
  switch (state) {
    case TAOS_SYNC_STATE_FOLLOWER:
      return "follower";
    case TAOS_SYNC_STATE_CANDIDATE:
      return "candidate";
    case TAOS_SYNC_STATE_LEADER:
      return "leader";
    case TAOS_SYNC_STATE_ERROR:
      return "error";
    case TAOS_SYNC_STATE_OFFLINE:
      return "offline";
    default:
      return "unknown";
  }
}

int32_t syncNodeUpdateNewConfigIndex(SSyncNode* ths, SSyncCfg* pNewCfg) {
  for (int32_t i = 0; i < pNewCfg->replicaNum; ++i) {
    SRaftId raftId = {
        .addr = SYNC_ADDR(&pNewCfg->nodeInfo[i]),
        .vgId = ths->vgId,
    };

    if (syncUtilSameId(&(ths->myRaftId), &raftId)) {
      pNewCfg->myIndex = i;
      return 0;
    }
  }

  return -1;
}

bool syncNodeIsOptimizedOneReplica(SSyncNode* ths, SRpcMsg* pMsg) {
  return (ths->replicaNum == 1 && syncUtilUserCommit(pMsg->msgType) && ths->vgId != 1);
}

bool syncNodeInRaftGroup(SSyncNode* ths, SRaftId* pRaftId) {
  for (int32_t i = 0; i < ths->replicaNum; ++i) {
    if (syncUtilSameId(&((ths->replicasId)[i]), pRaftId)) {
      return true;
    }
  }
  return false;
}

SSyncSnapshotSender* syncNodeGetSnapshotSender(SSyncNode* ths, SRaftId* pDestId) {
  SSyncSnapshotSender* pSender = NULL;
  for (int32_t i = 0; i < ths->replicaNum; ++i) {
    if (syncUtilSameId(pDestId, &((ths->replicasId)[i]))) {
      pSender = (ths->senders)[i];
    }
  }
  return pSender;
}

SSyncTimer* syncNodeGetHbTimer(SSyncNode* ths, SRaftId* pDestId) {
  SSyncTimer* pTimer = NULL;
  for (int32_t i = 0; i < ths->replicaNum; ++i) {
    if (syncUtilSameId(pDestId, &((ths->replicasId)[i]))) {
      pTimer = &((ths->peerHeartbeatTimerArr)[i]);
    }
  }
  return pTimer;
}

SPeerState* syncNodeGetPeerState(SSyncNode* ths, const SRaftId* pDestId) {
  SPeerState* pState = NULL;
  for (int32_t i = 0; i < ths->replicaNum; ++i) {
    if (syncUtilSameId(pDestId, &((ths->replicasId)[i]))) {
      pState = &((ths->peerStates)[i]);
    }
  }
  return pState;
}

bool syncNodeNeedSendAppendEntries(SSyncNode* ths, const SRaftId* pDestId, const SyncAppendEntries* pMsg) {
  SPeerState* pState = syncNodeGetPeerState(ths, pDestId);
  if (pState == NULL) {
    sError("vgId:%d, replica maybe dropped", ths->vgId);
    return false;
  }

  SyncIndex sendIndex = pMsg->prevLogIndex + 1;
  int64_t   tsNow = taosGetTimestampMs();

  if (pState->lastSendIndex == sendIndex && tsNow - pState->lastSendTime < SYNC_APPEND_ENTRIES_TIMEOUT_MS) {
    return false;
  }

  return true;
}

bool syncNodeCanChange(SSyncNode* pSyncNode) {
  if (pSyncNode->changing) {
    sError("sync cannot change");
    return false;
  }

  if ((pSyncNode->commitIndex >= SYNC_INDEX_BEGIN)) {
    SyncIndex lastIndex = syncNodeGetLastIndex(pSyncNode);
    if (pSyncNode->commitIndex != lastIndex) {
      sError("sync cannot change2");
      return false;
    }
  }

  for (int32_t i = 0; i < pSyncNode->peersNum; ++i) {
    SSyncSnapshotSender* pSender = syncNodeGetSnapshotSender(pSyncNode, &(pSyncNode->peersId)[i]);
    if (pSender != NULL && pSender->start) {
      sError("sync cannot change3");
      return false;
    }
  }

  return true;
}
