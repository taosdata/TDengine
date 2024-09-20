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
#include "tmisce.h"
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
static int32_t syncNodeDoConfigChange(SSyncNode* pSyncNode, SSyncCfg* newConfig, SyncIndex lastConfigChangeIndex);
static bool    syncNodeIsOptimizedOneReplica(SSyncNode* ths, SRpcMsg* pMsg);

static bool    syncNodeCanChange(SSyncNode* pSyncNode);
static int32_t syncNodeLeaderTransfer(SSyncNode* pSyncNode);
static int32_t syncNodeLeaderTransferTo(SSyncNode* pSyncNode, SNodeInfo newLeader);
static int32_t syncDoLeaderTransfer(SSyncNode* ths, SRpcMsg* pRpcMsg, SSyncRaftEntry* pEntry);

static ESyncStrategy syncNodeStrategy(SSyncNode* pSyncNode);

int64_t syncOpen(SSyncInfo* pSyncInfo, int32_t vnodeVersion) {
  sInfo("vgId:%d, start to open sync", pSyncInfo->vgId);
  SSyncNode* pSyncNode = syncNodeOpen(pSyncInfo, vnodeVersion);
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
  sInfo("vgId:%d, sync opened", pSyncInfo->vgId);
  return pSyncNode->rid;
}

int32_t syncStart(int64_t rid) {
  int32_t    code = 0;
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    sError("failed to acquire rid:%" PRId64 " of tsNodeReftId for pSyncNode", rid);
    TAOS_RETURN(code);
  }
  sInfo("vgId:%d, begin to start sync", pSyncNode->vgId);

  if ((code = syncNodeRestore(pSyncNode)) < 0) {
    sError("vgId:%d, failed to restore sync log buffer since %s", pSyncNode->vgId, tstrerror(code));
    goto _err;
  }

  if ((code = syncNodeStart(pSyncNode)) < 0) {
    sError("vgId:%d, failed to start sync node since %s", pSyncNode->vgId, tstrerror(code));
    goto _err;
  }

  syncNodeRelease(pSyncNode);

  sInfo("vgId:%d, sync started", pSyncNode->vgId);

  TAOS_RETURN(code);

_err:
  syncNodeRelease(pSyncNode);
  TAOS_RETURN(code);
}

int32_t syncNodeGetConfig(int64_t rid, SSyncCfg* cfg) {
  int32_t    code = 0;
  SSyncNode* pSyncNode = syncNodeAcquire(rid);

  if (pSyncNode == NULL) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    sError("failed to acquire rid:%" PRId64 " of tsNodeReftId for pSyncNode", rid);
    TAOS_RETURN(code);
  }

  *cfg = pSyncNode->raftCfg.cfg;

  syncNodeRelease(pSyncNode);

  return 0;
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
    if (snapshotReceiverIsStart(pSyncNode->pNewNodeReceiver)) {
      sInfo("vgId:%d, stop snapshot receiver", pSyncNode->vgId);
      snapshotReceiverStop(pSyncNode->pNewNodeReceiver);
    }
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
  int32_t    code = 0;
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  if (pSyncNode->raftCfg.lastConfigIndex >= pNewCfg->lastIndex) {
    syncNodeRelease(pSyncNode);
    sInfo("vgId:%d, no need Reconfig, current index:%" PRId64 ", new index:%" PRId64, pSyncNode->vgId,
          pSyncNode->raftCfg.lastConfigIndex, pNewCfg->lastIndex);
    return 0;
  }

  if (!syncNodeCheckNewConfig(pSyncNode, pNewCfg)) {
    syncNodeRelease(pSyncNode);
    code = TSDB_CODE_SYN_NEW_CONFIG_ERROR;
    sError("vgId:%d, failed to reconfig since invalid new config", pSyncNode->vgId);
    TAOS_RETURN(code);
  }

  TAOS_CHECK_RETURN(syncNodeUpdateNewConfigIndex(pSyncNode, pNewCfg));

  if (syncNodeDoConfigChange(pSyncNode, pNewCfg, pNewCfg->lastIndex) != 0) {
    code = TSDB_CODE_SYN_NEW_CONFIG_ERROR;
    sError("vgId:%d, failed to reconfig since do change error", pSyncNode->vgId);
    TAOS_RETURN(code);
  }

  if (pSyncNode->state == TAOS_SYNC_STATE_LEADER || pSyncNode->state == TAOS_SYNC_STATE_ASSIGNED_LEADER) {
    // TODO check return value
    TAOS_CHECK_RETURN(syncNodeStopHeartbeatTimer(pSyncNode));

    for (int32_t i = 0; i < TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA; ++i) {
      TAOS_CHECK_RETURN(syncHbTimerInit(pSyncNode, &pSyncNode->peerHeartbeatTimerArr[i], pSyncNode->replicasId[i]));
    }

    TAOS_CHECK_RETURN(syncNodeStartHeartbeatTimer(pSyncNode));
    // syncNodeReplicate(pSyncNode);
  }

  syncNodeRelease(pSyncNode);
  TAOS_RETURN(code);
}

int32_t syncProcessMsg(int64_t rid, SRpcMsg* pMsg) {
  int32_t code = -1;
  if (!syncIsInit()) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

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
    case TDMT_SYNC_TIMEOUT_ELECTION:
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
    case TDMT_SYNC_FORCE_FOLLOWER:
      code = syncForceBecomeFollower(pSyncNode, pMsg);
      break;
    case TDMT_SYNC_SET_ASSIGNED_LEADER:
      code = syncBecomeAssignedLeader(pSyncNode, pMsg);
      break;
    default:
      code = TSDB_CODE_MSG_NOT_PROCESSED;
  }

  syncNodeRelease(pSyncNode);
  if (code != 0) {
    sDebug("vgId:%d, failed to process sync msg:%p type:%s since %s", pSyncNode->vgId, pMsg, TMSG_INFO(pMsg->msgType),
           tstrerror(code));
  }
  TAOS_RETURN(code);
}

int32_t syncLeaderTransfer(int64_t rid) {
  int32_t    code = 0;
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  int32_t ret = syncNodeLeaderTransfer(pSyncNode);
  syncNodeRelease(pSyncNode);
  return ret;
}

int32_t syncForceBecomeFollower(SSyncNode* ths, const SRpcMsg* pRpcMsg) {
  syncNodeBecomeFollower(ths, "force election");

  SRpcMsg rsp = {
      .code = 0,
      .pCont = pRpcMsg->info.rsp,
      .contLen = pRpcMsg->info.rspLen,
      .info = pRpcMsg->info,
  };
  tmsgSendRsp(&rsp);

  return 0;
}

int32_t syncBecomeAssignedLeader(SSyncNode* ths, SRpcMsg* pRpcMsg) {
  int32_t code = TSDB_CODE_MND_ARB_TOKEN_MISMATCH;
  void*   pHead = NULL;
  int32_t contLen = 0;

  SVArbSetAssignedLeaderReq req = {0};
  if (tDeserializeSVArbSetAssignedLeaderReq((char*)pRpcMsg->pCont + sizeof(SMsgHead), pRpcMsg->contLen, &req) != 0) {
    sError("vgId:%d, failed to deserialize SVArbSetAssignedLeaderReq", ths->vgId);
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  if (ths->arbTerm > req.arbTerm) {
    sInfo("vgId:%d, skip to set assigned leader, msg with lower term, local:%" PRId64 "msg:%" PRId64, ths->vgId,
          ths->arbTerm, req.arbTerm);
    goto _OVER;
  }

  ths->arbTerm = TMAX(req.arbTerm, ths->arbTerm);

  if (strncmp(req.memberToken, ths->arbToken, TSDB_ARB_TOKEN_SIZE) != 0) {
    sInfo("vgId:%d, skip to set assigned leader, token mismatch, local:%s, msg:%s", ths->vgId, ths->arbToken,
          req.memberToken);
    goto _OVER;
  }

  if (ths->state != TAOS_SYNC_STATE_ASSIGNED_LEADER) {
    code = TSDB_CODE_SUCCESS;
    raftStoreNextTerm(ths);
    if (terrno != TSDB_CODE_SUCCESS) {
      code = terrno;
      sError("vgId:%d, failed to set next term since:%s", ths->vgId, tstrerror(code));
      goto _OVER;
    }
    syncNodeBecomeAssignedLeader(ths);

    if ((code = syncNodeAppendNoop(ths)) < 0) {
      sError("vgId:%d, assigned leader failed to append noop entry since %s", ths->vgId, tstrerror(code));
    }
  }

  SVArbSetAssignedLeaderRsp rsp = {0};
  rsp.arbToken = req.arbToken;
  rsp.memberToken = req.memberToken;
  rsp.vgId = ths->vgId;

  contLen = tSerializeSVArbSetAssignedLeaderRsp(NULL, 0, &rsp);
  if (contLen <= 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    sError("vgId:%d, failed to serialize SVArbSetAssignedLeaderRsp", ths->vgId);
    goto _OVER;
  }
  pHead = rpcMallocCont(contLen);
  if (!pHead) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    sError("vgId:%d, failed to malloc memory for SVArbSetAssignedLeaderRsp", ths->vgId);
    goto _OVER;
  }
  if (tSerializeSVArbSetAssignedLeaderRsp(pHead, contLen, &rsp) <= 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    sError("vgId:%d, failed to serialize SVArbSetAssignedLeaderRsp", ths->vgId);
    rpcFreeCont(pHead);
    goto _OVER;
  }

  code = TSDB_CODE_SUCCESS;

_OVER:;
  SRpcMsg rspMsg = {
      .code = code,
      .pCont = pHead,
      .contLen = contLen,
      .info = pRpcMsg->info,
  };

  tmsgSendRsp(&rspMsg);

  tFreeSVArbSetAssignedLeaderReq(&req);
  TAOS_RETURN(code);
}

int32_t syncSendTimeoutRsp(int64_t rid, int64_t seq) {
  int32_t    code = 0;
  SSyncNode* pNode = syncNodeAcquire(rid);
  if (pNode == NULL) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  SRpcMsg rpcMsg = {0};
  int32_t ret = syncRespMgrGetAndDel(pNode->pSyncRespMgr, seq, &rpcMsg.info);
  rpcMsg.code = TSDB_CODE_SYN_TIMEOUT;

  syncNodeRelease(pNode);
  if (ret == 1) {
    sInfo("send timeout response, seq:%" PRId64 " handle:%p ahandle:%p", seq, rpcMsg.info.handle, rpcMsg.info.ahandle);
    code = rpcSendResponse(&rpcMsg);
    return code;
  } else {
    sError("no message handle to send timeout response, seq:%" PRId64, seq);
    return TSDB_CODE_SYN_INTERNAL_ERROR;
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

static SyncIndex syncLogRetentionIndex(SSyncNode* pSyncNode, int64_t bytes) {
  return pSyncNode->pLogStore->syncLogIndexRetention(pSyncNode->pLogStore, bytes);
}

int32_t syncBeginSnapshot(int64_t rid, int64_t lastApplyIndex) {
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  int32_t    code = 0;
  if (pSyncNode == NULL) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    sError("sync begin snapshot error");
    TAOS_RETURN(code);
  }

  SyncIndex beginIndex = pSyncNode->pLogStore->syncLogBeginIndex(pSyncNode->pLogStore);
  SyncIndex endIndex = pSyncNode->pLogStore->syncLogEndIndex(pSyncNode->pLogStore);
  bool      isEmpty = pSyncNode->pLogStore->syncLogIsEmpty(pSyncNode->pLogStore);

  if (isEmpty || !(lastApplyIndex >= beginIndex && lastApplyIndex <= endIndex)) {
    sNTrace(pSyncNode, "new-snapshot-index:%" PRId64 ", empty:%d, do not delete wal", lastApplyIndex, isEmpty);
    syncNodeRelease(pSyncNode);
    return 0;
  }

  int64_t logRetention = 0;

  if (syncNodeIsMnode(pSyncNode)) {
    // mnode
    logRetention = tsMndLogRetention;
  } else {
    // vnode
    if (pSyncNode->replicaNum > 1) {
      logRetention = SYNC_VNODE_LOG_RETENTION;
    }
  }

  if (pSyncNode->totalReplicaNum > 1) {
    if (pSyncNode->state != TAOS_SYNC_STATE_LEADER && pSyncNode->state != TAOS_SYNC_STATE_FOLLOWER &&
        pSyncNode->state != TAOS_SYNC_STATE_LEARNER && pSyncNode->state != TAOS_SYNC_STATE_ASSIGNED_LEADER) {
      sNTrace(pSyncNode, "new-snapshot-index:%" PRId64 " candidate or unknown state, do not delete wal",
              lastApplyIndex);
      syncNodeRelease(pSyncNode);
      return 0;
    }
    SyncIndex retentionIndex =
        TMAX(pSyncNode->minMatchIndex, syncLogRetentionIndex(pSyncNode, SYNC_WAL_LOG_RETENTION_SIZE));
    logRetention += TMAX(0, lastApplyIndex - retentionIndex);
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
                  terrstr(), pSyncNode->snapshottingIndex, lastApplyIndex);
          atomic_store_64(&pSyncNode->snapshottingIndex, SYNC_INDEX_INVALID);
        }

      } else {
        sNTrace(pSyncNode, "snapshotting for %" PRId64 ", do not delete wal for new-snapshot-index:%" PRId64,
                snapshottingIndex, lastApplyIndex);
      }
    }
  } while (0);

  syncNodeRelease(pSyncNode);
  TAOS_RETURN(code);
}

int32_t syncEndSnapshot(int64_t rid) {
  int32_t    code = 0;
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    sError("sync end snapshot error");
    TAOS_RETURN(code);
  }

  if (atomic_load_64(&pSyncNode->snapshottingIndex) != SYNC_INDEX_INVALID) {
    SSyncLogStoreData* pData = pSyncNode->pLogStore->data;
    code = walEndSnapshot(pData->pWal);
    if (code != 0) {
      sNError(pSyncNode, "wal snapshot end error since:%s", tstrerror(code));
      syncNodeRelease(pSyncNode);
      TAOS_RETURN(code);
    } else {
      sNTrace(pSyncNode, "wal snapshot end, index:%" PRId64, atomic_load_64(&pSyncNode->snapshottingIndex));
      atomic_store_64(&pSyncNode->snapshottingIndex, SYNC_INDEX_INVALID);
    }
  }

  syncNodeRelease(pSyncNode);
  TAOS_RETURN(code);
}

bool syncNodeIsReadyForRead(SSyncNode* pSyncNode) {
  if (pSyncNode == NULL) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    sError("sync ready for read error");
    return false;
  }

  if (pSyncNode->state != TAOS_SYNC_STATE_LEADER && pSyncNode->state != TAOS_SYNC_STATE_ASSIGNED_LEADER) {
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

#ifdef BUILD_NO_CALL
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
#endif

int32_t syncNodeLeaderTransfer(SSyncNode* pSyncNode) {
  if (pSyncNode->peersNum == 0) {
    sDebug("vgId:%d, only one replica, cannot leader transfer", pSyncNode->vgId);
    return 0;
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
    return TSDB_CODE_SYN_INTERNAL_ERROR;
  }

  sNTrace(pSyncNode, "begin leader transfer to %s:%u", newLeader.nodeFqdn, newLeader.nodePort);

  SRpcMsg rpcMsg = {0};
  TAOS_CHECK_RETURN(syncBuildLeaderTransfer(&rpcMsg, pSyncNode->vgId));

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
    state.roleTimeMs = pSyncNode->roleTimeMs;
    state.startTimeMs = pSyncNode->startTime;
    state.restored = pSyncNode->restoreFinish;
    if (pSyncNode->vgId != 1) {
      state.canRead = syncNodeIsReadyForRead(pSyncNode);
    } else {
      state.canRead = state.restored;
    }
    /*
    double progress = 0;
    if(pSyncNode->pLogBuf->totalIndex > 0 && pSyncNode->pLogBuf->commitIndex > 0){
      progress = (double)pSyncNode->pLogBuf->commitIndex/(double)pSyncNode->pLogBuf->totalIndex;
      state.progress = (int32_t)(progress * 100);
    }
    else{
      state.progress = -1;
    }
    sDebug("vgId:%d, learner progress state, commitIndex:%" PRId64 " totalIndex:%" PRId64 ", "
            "progress:%lf, progress:%d",
          pSyncNode->vgId,
         pSyncNode->pLogBuf->commitIndex, pSyncNode->pLogBuf->totalIndex, progress, state.progress);
    */
    state.term = raftStoreGetTerm(pSyncNode);
    syncNodeRelease(pSyncNode);
  }

  return state;
}

int32_t syncGetArbToken(int64_t rid, char* outToken) {
  int32_t    code = 0;
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  memset(outToken, 0, TSDB_ARB_TOKEN_SIZE);
  (void)taosThreadMutexLock(&pSyncNode->arbTokenMutex);
  strncpy(outToken, pSyncNode->arbToken, TSDB_ARB_TOKEN_SIZE);
  (void)taosThreadMutexUnlock(&pSyncNode->arbTokenMutex);

  syncNodeRelease(pSyncNode);
  TAOS_RETURN(code);
}

int32_t syncGetAssignedLogSynced(int64_t rid) {
  int32_t    code = 0;
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  if (pSyncNode->state != TAOS_SYNC_STATE_LEADER) {
    code = TSDB_CODE_VND_ARB_NOT_SYNCED;
    syncNodeRelease(pSyncNode);
    TAOS_RETURN(code);
  }

  bool isSync = pSyncNode->commitIndex >= pSyncNode->assignedCommitIndex;
  code = (isSync ? TSDB_CODE_SUCCESS : TSDB_CODE_VND_ARB_NOT_SYNCED);

  syncNodeRelease(pSyncNode);
  TAOS_RETURN(code);
}

int32_t syncUpdateArbTerm(int64_t rid, SyncTerm arbTerm) {
  int32_t    code = 0;
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  pSyncNode->arbTerm = TMAX(arbTerm, pSyncNode->arbTerm);
  syncNodeRelease(pSyncNode);
  TAOS_RETURN(code);
}

SyncIndex syncNodeGetSnapshotConfigIndex(SSyncNode* pSyncNode, SyncIndex snapshotLastApplyIndex) {
  if (!(pSyncNode->raftCfg.configIndexCount >= 1)) {
    sError("vgId:%d, failed get snapshot config index, configIndexCount:%d", pSyncNode->vgId,
           pSyncNode->raftCfg.configIndexCount);
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -2;
  }
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

  int j = 0;
  for (int32_t i = 0; i < pSyncNode->raftCfg.cfg.totalReplicaNum; ++i) {
    if (pSyncNode->raftCfg.cfg.nodeInfo[i].nodeRole == TAOS_SYNC_ROLE_LEARNER) continue;
    SEp* pEp = &pEpSet->eps[j];
    tstrncpy(pEp->fqdn, pSyncNode->raftCfg.cfg.nodeInfo[i].nodeFqdn, TSDB_FQDN_LEN);
    pEp->port = (pSyncNode->raftCfg.cfg.nodeInfo)[i].nodePort;
    pEpSet->numOfEps++;
    sDebug("vgId:%d, sync get retry epset, index:%d %s:%d", pSyncNode->vgId, i, pEp->fqdn, pEp->port);
    j++;
  }
  if (pEpSet->numOfEps > 0) {
    pEpSet->inUse = (pSyncNode->raftCfg.cfg.myIndex + 1) % pEpSet->numOfEps;
    // pEpSet->inUse = 0;
  }
  epsetSort(pEpSet);

  sInfo("vgId:%d, sync get retry epset numOfEps:%d inUse:%d", pSyncNode->vgId, pEpSet->numOfEps, pEpSet->inUse);
  syncNodeRelease(pSyncNode);
}

int32_t syncPropose(int64_t rid, SRpcMsg* pMsg, bool isWeak, int64_t* seq) {
  int32_t    code = 0;
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    sError("sync propose error");
    TAOS_RETURN(code);
  }

  int32_t ret = syncNodePropose(pSyncNode, pMsg, isWeak, seq);
  syncNodeRelease(pSyncNode);
  return ret;
}

int32_t syncCheckMember(int64_t rid) {
  int32_t    code = 0;
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    sError("sync propose error");
    TAOS_RETURN(code);
  }

  if (pSyncNode->myNodeInfo.nodeRole == TAOS_SYNC_ROLE_LEARNER) {
    syncNodeRelease(pSyncNode);
    return TSDB_CODE_SYN_WRONG_ROLE;
  }

  syncNodeRelease(pSyncNode);
  return 0;
}

int32_t syncIsCatchUp(int64_t rid) {
  int32_t    code = 0;
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    sError("sync Node Acquire error since %d", errno);
    TAOS_RETURN(code);
  }

  int32_t isCatchUp = 0;
  if (pSyncNode->pLogBuf->totalIndex < 0 || pSyncNode->pLogBuf->commitIndex < 0 ||
      pSyncNode->pLogBuf->totalIndex < pSyncNode->pLogBuf->commitIndex ||
      pSyncNode->pLogBuf->totalIndex - pSyncNode->pLogBuf->commitIndex > SYNC_LEARNER_CATCHUP) {
    sInfo("vgId:%d, Not catch up, wait one second, totalIndex:%" PRId64 " commitIndex:%" PRId64 " matchIndex:%" PRId64,
          pSyncNode->vgId, pSyncNode->pLogBuf->totalIndex, pSyncNode->pLogBuf->commitIndex,
          pSyncNode->pLogBuf->matchIndex);
    isCatchUp = 0;
  } else {
    sInfo("vgId:%d, Catch up, totalIndex:%" PRId64 " commitIndex:%" PRId64 " matchIndex:%" PRId64, pSyncNode->vgId,
          pSyncNode->pLogBuf->totalIndex, pSyncNode->pLogBuf->commitIndex, pSyncNode->pLogBuf->matchIndex);
    isCatchUp = 1;
  }

  syncNodeRelease(pSyncNode);
  return isCatchUp;
}

ESyncRole syncGetRole(int64_t rid) {
  int32_t    code = 0;
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    sError("sync Node Acquire error since %d", errno);
    TAOS_RETURN(code);
  }

  ESyncRole role = pSyncNode->raftCfg.cfg.nodeInfo[pSyncNode->raftCfg.cfg.myIndex].nodeRole;

  syncNodeRelease(pSyncNode);
  return role;
}

int64_t syncGetTerm(int64_t rid) {
  int32_t    code = 0;
  SSyncNode* pSyncNode = syncNodeAcquire(rid);
  if (pSyncNode == NULL) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    sError("sync Node Acquire error since %d", errno);
    TAOS_RETURN(code);
  }

  int64_t term = raftStoreGetTerm(pSyncNode);

  syncNodeRelease(pSyncNode);
  return term;
}

int32_t syncNodePropose(SSyncNode* pSyncNode, SRpcMsg* pMsg, bool isWeak, int64_t* seq) {
  int32_t code = 0;
  if (pSyncNode->state != TAOS_SYNC_STATE_LEADER && pSyncNode->state != TAOS_SYNC_STATE_ASSIGNED_LEADER) {
    code = TSDB_CODE_SYN_NOT_LEADER;
    sNWarn(pSyncNode, "sync propose not leader, type:%s", TMSG_INFO(pMsg->msgType));
    TAOS_RETURN(code);
  }

  if (!pSyncNode->restoreFinish) {
    code = TSDB_CODE_SYN_PROPOSE_NOT_READY;
    sNWarn(pSyncNode, "failed to sync propose since not ready, type:%s, last:%" PRId64 ", cmt:%" PRId64,
           TMSG_INFO(pMsg->msgType), syncNodeGetLastIndex(pSyncNode), pSyncNode->commitIndex);
    TAOS_RETURN(code);
  }

  // heartbeat timeout
  if (pSyncNode->state != TAOS_SYNC_STATE_ASSIGNED_LEADER && syncNodeHeartbeatReplyTimeout(pSyncNode)) {
    code = TSDB_CODE_SYN_PROPOSE_NOT_READY;
    sNError(pSyncNode, "failed to sync propose since heartbeat timeout, type:%s, last:%" PRId64 ", cmt:%" PRId64,
            TMSG_INFO(pMsg->msgType), syncNodeGetLastIndex(pSyncNode), pSyncNode->commitIndex);
    TAOS_RETURN(code);
  }

  // optimized one replica
  if (syncNodeIsOptimizedOneReplica(pSyncNode, pMsg)) {
    SyncIndex retIndex;
    int32_t   code = syncNodeOnClientRequest(pSyncNode, pMsg, &retIndex);
    if (code >= 0) {
      pMsg->info.conn.applyIndex = retIndex;
      pMsg->info.conn.applyTerm = raftStoreGetTerm(pSyncNode);

      // after raft member change, need to handle 1->2 switching point
      // at this point, need to switch entry handling thread
      if (pSyncNode->replicaNum == 1) {
        sTrace("vgId:%d, propose optimized msg, index:%" PRId64 " type:%s", pSyncNode->vgId, retIndex,
               TMSG_INFO(pMsg->msgType));
        return 1;
      } else {
        sTrace("vgId:%d, propose optimized msg, return to normal, index:%" PRId64
               " type:%s, "
               "handle:%p",
               pSyncNode->vgId, retIndex, TMSG_INFO(pMsg->msgType), pMsg->info.handle);
        return 0;
      }
    } else {
      code = TSDB_CODE_SYN_INTERNAL_ERROR;
      sError("vgId:%d, failed to propose optimized msg, index:%" PRId64 " type:%s", pSyncNode->vgId, retIndex,
             TMSG_INFO(pMsg->msgType));
      TAOS_RETURN(code);
    }
  } else {
    SRespStub stub = {.createTime = taosGetTimestampMs(), .rpcMsg = *pMsg};
    uint64_t  seqNum = syncRespMgrAdd(pSyncNode->pSyncRespMgr, &stub);
    SRpcMsg   rpcMsg = {0};
    int32_t   code = syncBuildClientRequest(&rpcMsg, pMsg, seqNum, isWeak, pSyncNode->vgId);
    if (code != 0) {
      sError("vgId:%d, failed to propose msg while serialize since %s", pSyncNode->vgId, terrstr());
      code = syncRespMgrDel(pSyncNode->pSyncRespMgr, seqNum);
      TAOS_RETURN(code);
    }

    sNTrace(pSyncNode, "propose msg, type:%s", TMSG_INFO(pMsg->msgType));
    code = (*pSyncNode->syncEqMsg)(pSyncNode->msgcb, &rpcMsg);
    if (code != 0) {
      sWarn("vgId:%d, failed to propose msg while enqueue since %s", pSyncNode->vgId, terrstr());
      TAOS_CHECK_RETURN(syncRespMgrDel(pSyncNode->pSyncRespMgr, seqNum));
    }

    if (seq != NULL) *seq = seqNum;
    TAOS_RETURN(code);
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
  int32_t code = 0;
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

    sTrace("vgId:%d, start hb timer, rid:%" PRId64 " addr:%" PRId64, pSyncNode->vgId, pData->rid, pData->destId.addr);

    TAOS_CHECK_RETURN(taosTmrReset(pSyncTimer->timerCb, pSyncTimer->timerMS / HEARTBEAT_TICK_NUM, (void*)(pData->rid),
                                   syncEnv()->pTimerManager, &pSyncTimer->pTimer));
  } else {
    code = TSDB_CODE_SYN_INTERNAL_ERROR;
    sError("vgId:%d, start ctrl hb timer error, sync env is stop", pSyncNode->vgId);
  }
  return code;
}

static int32_t syncHbTimerStop(SSyncNode* pSyncNode, SSyncTimer* pSyncTimer) {
  int32_t ret = 0;
  (void)atomic_add_fetch_64(&pSyncTimer->logicClock, 1);
  if (!taosTmrStop(pSyncTimer->pTimer)) {
    return TSDB_CODE_SYN_INTERNAL_ERROR;
  }
  pSyncTimer->pTimer = NULL;
  syncHbTimerDataRemove(pSyncTimer->hbDataRid);
  pSyncTimer->hbDataRid = -1;
  return ret;
}

int32_t syncNodeLogStoreRestoreOnNeed(SSyncNode* pNode) {
  int32_t code = 0;
  if (pNode->pLogStore == NULL) {
    sError("vgId:%d, log store not created", pNode->vgId);
    return TSDB_CODE_SYN_INTERNAL_ERROR;
  }
  if (pNode->pFsm == NULL) {
    sError("vgId:%d, pFsm not registered", pNode->vgId);
    return TSDB_CODE_SYN_INTERNAL_ERROR;
  }
  if (pNode->pFsm->FpGetSnapshotInfo == NULL) {
    sError("vgId:%d, FpGetSnapshotInfo not registered", pNode->vgId);
    return TSDB_CODE_SYN_INTERNAL_ERROR;
  }
  SSnapshot snapshot = {0};
  // TODO check return value
  (void)pNode->pFsm->FpGetSnapshotInfo(pNode->pFsm, &snapshot);

  SyncIndex commitIndex = snapshot.lastApplyIndex;
  SyncIndex firstVer = pNode->pLogStore->syncLogBeginIndex(pNode->pLogStore);
  SyncIndex lastVer = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
  if ((lastVer < commitIndex || firstVer > commitIndex + 1) || pNode->fsmState == SYNC_FSM_STATE_INCOMPLETE) {
    if ((code = pNode->pLogStore->syncLogRestoreFromSnapshot(pNode->pLogStore, commitIndex)) != 0) {
      sError("vgId:%d, failed to restore log store from snapshot since %s. lastVer:%" PRId64 ", snapshotVer:%" PRId64,
             pNode->vgId, terrstr(), lastVer, commitIndex);
      TAOS_RETURN(code);
    }
  }
  TAOS_RETURN(code);
}

// open/close --------------
SSyncNode* syncNodeOpen(SSyncInfo* pSyncInfo, int32_t vnodeVersion) {
  int32_t    code = 0;
  SSyncNode* pSyncNode = taosMemoryCalloc(1, sizeof(SSyncNode));
  if (pSyncNode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  if (!taosDirExist((char*)(pSyncInfo->path))) {
    if (taosMkDir(pSyncInfo->path) != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      sError("vgId:%d, failed to create dir:%s since %s", pSyncInfo->vgId, pSyncInfo->path, terrstr());
      goto _error;
    }
  }

  memcpy(pSyncNode->path, pSyncInfo->path, sizeof(pSyncNode->path));
  snprintf(pSyncNode->raftStorePath, sizeof(pSyncNode->raftStorePath), "%s%sraft_store.json", pSyncInfo->path,
           TD_DIRSEP);
  snprintf(pSyncNode->configPath, sizeof(pSyncNode->configPath), "%s%sraft_config.json", pSyncInfo->path, TD_DIRSEP);

  if (!taosCheckExistFile(pSyncNode->configPath)) {
    // create a new raft config file
    sInfo("vgId:%d, create a new raft config file", pSyncInfo->vgId);
    pSyncNode->vgId = pSyncInfo->vgId;
    pSyncNode->raftCfg.isStandBy = pSyncInfo->isStandBy;
    pSyncNode->raftCfg.snapshotStrategy = pSyncInfo->snapshotStrategy;
    pSyncNode->raftCfg.lastConfigIndex = pSyncInfo->syncCfg.lastIndex;
    pSyncNode->raftCfg.batchSize = pSyncInfo->batchSize;
    pSyncNode->raftCfg.cfg = pSyncInfo->syncCfg;
    pSyncNode->raftCfg.configIndexCount = 1;
    pSyncNode->raftCfg.configIndexArr[0] = -1;

    if ((code = syncWriteCfgFile(pSyncNode)) != 0) {
      terrno = code;
      sError("vgId:%d, failed to create sync cfg file", pSyncNode->vgId);
      goto _error;
    }
  } else {
    // update syncCfg by raft_config.json
    if ((code = syncReadCfgFile(pSyncNode)) != 0) {
      terrno = code;
      sError("vgId:%d, failed to read sync cfg file", pSyncNode->vgId);
      goto _error;
    }

    if (vnodeVersion > pSyncNode->raftCfg.cfg.changeVersion) {
      if (pSyncInfo->syncCfg.totalReplicaNum > 0 && syncIsConfigChanged(&pSyncNode->raftCfg.cfg, &pSyncInfo->syncCfg)) {
        sInfo("vgId:%d, use sync config from input options and write to cfg file", pSyncNode->vgId);
        pSyncNode->raftCfg.cfg = pSyncInfo->syncCfg;
        if ((code = syncWriteCfgFile(pSyncNode)) != 0) {
          terrno = code;
          sError("vgId:%d, failed to write sync cfg file", pSyncNode->vgId);
          goto _error;
        }
      } else {
        sInfo("vgId:%d, use sync config from sync cfg file", pSyncNode->vgId);
        pSyncInfo->syncCfg = pSyncNode->raftCfg.cfg;
      }
    } else {
      sInfo("vgId:%d, skip save sync cfg file since request ver:%d <= file ver:%d", pSyncNode->vgId, vnodeVersion,
            pSyncInfo->syncCfg.changeVersion);
    }
  }

  // init by SSyncInfo
  pSyncNode->vgId = pSyncInfo->vgId;
  SSyncCfg* pCfg = &pSyncNode->raftCfg.cfg;
  bool      updated = false;
  sInfo("vgId:%d, start to open sync node, totalReplicaNum:%d replicaNum:%d selfIndex:%d", pSyncNode->vgId,
        pCfg->totalReplicaNum, pCfg->replicaNum, pCfg->myIndex);
  for (int32_t i = 0; i < pCfg->totalReplicaNum; ++i) {
    SNodeInfo* pNode = &pCfg->nodeInfo[i];
    if (tmsgUpdateDnodeInfo(&pNode->nodeId, &pNode->clusterId, pNode->nodeFqdn, &pNode->nodePort)) {
      updated = true;
    }
    sInfo("vgId:%d, index:%d ep:%s:%u dnode:%d cluster:%" PRId64, pSyncNode->vgId, i, pNode->nodeFqdn, pNode->nodePort,
          pNode->nodeId, pNode->clusterId);
  }

  if (vnodeVersion > pSyncInfo->syncCfg.changeVersion) {
    if (updated) {
      sInfo("vgId:%d, save config info since dnode info changed", pSyncNode->vgId);
      if ((code = syncWriteCfgFile(pSyncNode)) != 0) {
        terrno = code;
        sError("vgId:%d, failed to write sync cfg file on dnode info updated", pSyncNode->vgId);
        goto _error;
      }
    }
  }

  pSyncNode->pWal = pSyncInfo->pWal;
  pSyncNode->msgcb = pSyncInfo->msgcb;
  pSyncNode->syncSendMSg = pSyncInfo->syncSendMSg;
  pSyncNode->syncEqMsg = pSyncInfo->syncEqMsg;
  pSyncNode->syncEqCtrlMsg = pSyncInfo->syncEqCtrlMsg;

  // create raft log ring buffer
  code = syncLogBufferCreate(&pSyncNode->pLogBuf);
  if (pSyncNode->pLogBuf == NULL) {
    sError("failed to init sync log buffer since %s. vgId:%d", tstrerror(code), pSyncNode->vgId);
    goto _error;
  }

  // init replicaNum, replicasId
  pSyncNode->replicaNum = pSyncNode->raftCfg.cfg.replicaNum;
  pSyncNode->totalReplicaNum = pSyncNode->raftCfg.cfg.totalReplicaNum;
  for (int32_t i = 0; i < pSyncNode->raftCfg.cfg.totalReplicaNum; ++i) {
    if (!syncUtilNodeInfo2RaftId(&pSyncNode->raftCfg.cfg.nodeInfo[i], pSyncNode->vgId, &pSyncNode->replicasId[i])) {
      terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
      sError("vgId:%d, failed to determine raft member id, replica:%d", pSyncNode->vgId, i);
      goto _error;
    }
  }

  // init internal
  pSyncNode->myNodeInfo = pSyncNode->raftCfg.cfg.nodeInfo[pSyncNode->raftCfg.cfg.myIndex];
  pSyncNode->myRaftId = pSyncNode->replicasId[pSyncNode->raftCfg.cfg.myIndex];

  // init peersNum, peers, peersId
  pSyncNode->peersNum = pSyncNode->raftCfg.cfg.totalReplicaNum - 1;
  int32_t j = 0;
  for (int32_t i = 0; i < pSyncNode->raftCfg.cfg.totalReplicaNum; ++i) {
    if (i != pSyncNode->raftCfg.cfg.myIndex) {
      pSyncNode->peersNodeInfo[j] = pSyncNode->raftCfg.cfg.nodeInfo[i];
      pSyncNode->peersId[j] = pSyncNode->replicasId[i];
      syncUtilNodeInfo2EpSet(&pSyncNode->peersNodeInfo[j], &pSyncNode->peersEpset[j]);
      j++;
    }
  }

  pSyncNode->arbTerm = -1;
  (void)taosThreadMutexInit(&pSyncNode->arbTokenMutex, NULL);
  syncUtilGenerateArbToken(pSyncNode->myNodeInfo.nodeId, pSyncInfo->vgId, pSyncNode->arbToken);
  sInfo("vgId:%d, generate arb token:%s", pSyncNode->vgId, pSyncNode->arbToken);

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
  pSyncNode->roleTimeMs = taosGetTimestampMs();
  if ((code = raftStoreOpen(pSyncNode)) != 0) {
    terrno = code;
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
    // TODO check return value
    (void)pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
    if (snapshot.lastApplyIndex > commitIndex) {
      commitIndex = snapshot.lastApplyIndex;
      sNTrace(pSyncNode, "reset commit index by snapshot");
    }
    pSyncNode->fsmState = snapshot.state;
    if (pSyncNode->fsmState == SYNC_FSM_STATE_INCOMPLETE) {
      sError("vgId:%d, fsm state is incomplete.", pSyncNode->vgId);
      if (pSyncNode->replicaNum == 1) {
        terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
        goto _error;
      }
    }
  }
  pSyncNode->commitIndex = commitIndex;
  sInfo("vgId:%d, sync node commitIndex initialized as %" PRId64, pSyncNode->vgId, pSyncNode->commitIndex);

  // restore log store on need
  if ((code = syncNodeLogStoreRestoreOnNeed(pSyncNode)) < 0) {
    terrno = code;
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
#ifdef BUILD_NO_CALL
  pSyncNode->FpHeartbeatTimerCB = syncNodeEqHeartbeatTimer;
#endif
  pSyncNode->heartbeatTimerCounter = 0;

  // init peer heartbeat timer
  for (int32_t i = 0; i < TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA; ++i) {
    if ((code = syncHbTimerInit(pSyncNode, &(pSyncNode->peerHeartbeatTimerArr[i]), (pSyncNode->replicasId)[i])) != 0) {
      errno = code;
      goto _error;
    }
  }

  // tools
  if ((code = syncRespMgrCreate(pSyncNode, SYNC_RESP_TTL_MS, &pSyncNode->pSyncRespMgr)) != 0) {
    sError("vgId:%d, failed to create SyncRespMgr", pSyncNode->vgId);
    goto _error;
  }
  if (pSyncNode->pSyncRespMgr == NULL) {
    sError("vgId:%d, failed to create SyncRespMgr", pSyncNode->vgId);
    goto _error;
  }

  // restore state
  pSyncNode->restoreFinish = false;

  // snapshot senders
  for (int32_t i = 0; i < TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA; ++i) {
    SSyncSnapshotSender* pSender = NULL;
    code = snapshotSenderCreate(pSyncNode, i, &pSender);
    if (pSender == NULL) return NULL;

    pSyncNode->senders[i] = pSender;
    sSDebug(pSender, "snapshot sender create while open sync node, data:%p", pSender);
  }

  // snapshot receivers
  code = snapshotReceiverCreate(pSyncNode, EMPTY_RAFT_ID, &pSyncNode->pNewNodeReceiver);
  if (pSyncNode->pNewNodeReceiver == NULL) return NULL;
  sRDebug(pSyncNode->pNewNodeReceiver, "snapshot receiver create while open sync node, data:%p",
          pSyncNode->pNewNodeReceiver);

  // is config changing
  pSyncNode->changing = false;

  // replication mgr
  if ((code = syncNodeLogReplInit(pSyncNode)) < 0) {
    terrno = code;
    sError("vgId:%d, failed to init repl mgr since %s.", pSyncNode->vgId, terrstr());
    goto _error;
  }

  // peer state
  if ((code = syncNodePeerStateInit(pSyncNode)) < 0) {
    terrno = code;
    sError("vgId:%d, failed to init peer stat since %s.", pSyncNode->vgId, terrstr());
    goto _error;
  }

  //
  // min match index
  pSyncNode->minMatchIndex = SYNC_INDEX_INVALID;

  // start in syncNodeStart
  // start raft

  int64_t timeNow = taosGetTimestampMs();
  pSyncNode->startTime = timeNow;
  pSyncNode->lastReplicateTime = timeNow;

  // snapshotting
  atomic_store_64(&pSyncNode->snapshottingIndex, SYNC_INDEX_INVALID);

  // init log buffer
  if ((code = syncLogBufferInit(pSyncNode->pLogBuf, pSyncNode)) < 0) {
    terrno = code;
    sError("vgId:%d, failed to init sync log buffer since %s", pSyncNode->vgId, terrstr());
    goto _error;
  }

  pSyncNode->isStart = true;
  pSyncNode->electNum = 0;
  pSyncNode->becomeLeaderNum = 0;
  pSyncNode->becomeAssignedLeaderNum = 0;
  pSyncNode->configChangeNum = 0;
  pSyncNode->hbSlowNum = 0;
  pSyncNode->hbrSlowNum = 0;
  pSyncNode->tmrRoutineNum = 0;

  sNInfo(pSyncNode, "sync node opened, node:%p electInterval:%d heartbeatInterval:%d heartbeatTimeout:%d", pSyncNode,
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

#ifdef BUILD_NO_CALL
void syncNodeMaybeUpdateCommitBySnapshot(SSyncNode* pSyncNode) {
  if (pSyncNode->pFsm != NULL && pSyncNode->pFsm->FpGetSnapshotInfo != NULL) {
    SSnapshot snapshot = {0};
    (void)pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
    if (snapshot.lastApplyIndex > pSyncNode->commitIndex) {
      pSyncNode->commitIndex = snapshot.lastApplyIndex;
    }
  }
}
#endif

int32_t syncNodeRestore(SSyncNode* pSyncNode) {
  int32_t code = 0;
  if (pSyncNode->pLogStore == NULL) {
    sError("vgId:%d, log store not created", pSyncNode->vgId);
    return TSDB_CODE_SYN_INTERNAL_ERROR;
  }
  if (pSyncNode->pLogBuf == NULL) {
    sError("vgId:%d, ring log buffer not created", pSyncNode->vgId);
    return TSDB_CODE_SYN_INTERNAL_ERROR;
  }

  (void)taosThreadMutexLock(&pSyncNode->pLogBuf->mutex);
  SyncIndex lastVer = pSyncNode->pLogStore->syncLogLastIndex(pSyncNode->pLogStore);
  SyncIndex commitIndex = pSyncNode->pLogStore->syncLogCommitIndex(pSyncNode->pLogStore);
  SyncIndex endIndex = pSyncNode->pLogBuf->endIndex;
  (void)taosThreadMutexUnlock(&pSyncNode->pLogBuf->mutex);

  if (lastVer != -1 && endIndex != lastVer + 1) {
    code = TSDB_CODE_WAL_LOG_INCOMPLETE;
    sWarn("vgId:%d, failed to restore sync node since %s. expected lastLogIndex:%" PRId64 ", lastVer:%" PRId64 "",
          pSyncNode->vgId, terrstr(), endIndex - 1, lastVer);
    // TAOS_RETURN(code);
  }

  // if (endIndex != lastVer + 1) return TSDB_CODE_SYN_INTERNAL_ERROR;
  pSyncNode->commitIndex = TMAX(pSyncNode->commitIndex, commitIndex);
  sInfo("vgId:%d, restore sync until commitIndex:%" PRId64, pSyncNode->vgId, pSyncNode->commitIndex);

  if (pSyncNode->fsmState != SYNC_FSM_STATE_INCOMPLETE &&
      (code = syncLogBufferCommit(pSyncNode->pLogBuf, pSyncNode, pSyncNode->commitIndex)) < 0) {
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

int32_t syncNodeStart(SSyncNode* pSyncNode) {
  // start raft
  sInfo("vgId:%d, begin to start sync node", pSyncNode->vgId);
  if (pSyncNode->raftCfg.cfg.nodeInfo[pSyncNode->raftCfg.cfg.myIndex].nodeRole == TAOS_SYNC_ROLE_LEARNER) {
    syncNodeBecomeLearner(pSyncNode, "first start");
  } else {
    if (pSyncNode->replicaNum == 1) {
      raftStoreNextTerm(pSyncNode);
      syncNodeBecomeLeader(pSyncNode, "one replica start");

      // Raft 3.6.2 Committing entries from previous terms
      TAOS_CHECK_RETURN(syncNodeAppendNoop(pSyncNode));
    } else {
      syncNodeBecomeFollower(pSyncNode, "first start");
    }
  }

  int32_t ret = 0;
  ret = syncNodeStartPingTimer(pSyncNode);
  if (ret != 0) {
    sError("vgId:%d, failed to start ping timer since %s", pSyncNode->vgId, tstrerror(ret));
  }
  sInfo("vgId:%d, sync node started", pSyncNode->vgId);
  return ret;
}

#ifdef BUILD_NO_CALL
int32_t syncNodeStartStandBy(SSyncNode* pSyncNode) {
  // state change
  int32_t code = 0;
  pSyncNode->state = TAOS_SYNC_STATE_FOLLOWER;
  pSyncNode->roleTimeMs = taosGetTimestampMs();
  // TODO check return value
  TAOS_CHECK_RETURN(syncNodeStopHeartbeatTimer(pSyncNode));

  // reset elect timer, long enough
  int32_t electMS = TIMER_MAX_MS;
  code = syncNodeRestartElectTimer(pSyncNode, electMS);
  if (code < 0) {
    sError("vgId:%d, failed to restart elect timer since %s", pSyncNode->vgId, terrstr());
    return -1;
  }

  code = syncNodeStartPingTimer(pSyncNode);
  if (code < 0) {
    sError("vgId:%d, failed to start ping timer since %s", pSyncNode->vgId, terrstr());
    return -1;
  }
  return code;
}
#endif

void syncNodePreClose(SSyncNode* pSyncNode) {
  int32_t code = 0;
  if (pSyncNode == NULL) {
    sError("failed to pre close sync node since sync node is null");
    return;
  }
  if (pSyncNode->pFsm == NULL) {
    sError("failed to pre close sync node since fsm is null");
    return;
  }
  if (pSyncNode->pFsm->FpApplyQueueItems == NULL) {
    sError("failed to pre close sync node since FpApplyQueueItems is null");
    return;
  }

  // stop elect timer
  if ((code = syncNodeStopElectTimer(pSyncNode)) != 0) {
    sError("vgId:%d, failed to stop elect timer since %s", pSyncNode->vgId, tstrerror(code));
    return;
  }

  // stop heartbeat timer
  if ((code = syncNodeStopHeartbeatTimer(pSyncNode)) != 0) {
    sError("vgId:%d, failed to stop heartbeat timer since %s", pSyncNode->vgId, tstrerror(code));
    return;
  }

  // stop ping timer
  if ((code = syncNodeStopPingTimer(pSyncNode)) != 0) {
    sError("vgId:%d, failed to stop ping timer since %s", pSyncNode->vgId, tstrerror(code));
    return;
  }

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
  int32_t code = 0;
  if (pSyncNode == NULL) return;
  sNInfo(pSyncNode, "sync close, node:%p", pSyncNode);

  syncRespCleanRsp(pSyncNode->pSyncRespMgr);

  if ((code = syncNodeStopPingTimer(pSyncNode)) != 0) {
    sError("vgId:%d, failed to stop ping timer since %s", pSyncNode->vgId, tstrerror(code));
    return;
  }
  if ((code = syncNodeStopElectTimer(pSyncNode)) != 0) {
    sError("vgId:%d, failed to stop elect timer since %s", pSyncNode->vgId, tstrerror(code));
    return;
  }
  if ((code = syncNodeStopHeartbeatTimer(pSyncNode)) != 0) {
    sError("vgId:%d, failed to stop heartbeat timer since %s", pSyncNode->vgId, tstrerror(code));
    return;
  }
  syncNodeLogReplDestroy(pSyncNode);

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

  (void)taosThreadMutexDestroy(&pSyncNode->arbTokenMutex);

  for (int32_t i = 0; i < TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA; ++i) {
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
  int32_t code = 0;
  if (syncIsInit()) {
    TAOS_CHECK_RETURN(taosTmrReset(pSyncNode->FpPingTimerCB, pSyncNode->pingTimerMS, (void*)pSyncNode->rid,
                                   syncEnv()->pTimerManager, &pSyncNode->pPingTimer));
    atomic_store_64(&pSyncNode->pingTimerLogicClock, pSyncNode->pingTimerLogicClockUser);
  } else {
    sError("vgId:%d, start ping timer error, sync env is stop", pSyncNode->vgId);
  }
  return code;
}

int32_t syncNodeStopPingTimer(SSyncNode* pSyncNode) {
  int32_t code = 0;
  (void)atomic_add_fetch_64(&pSyncNode->pingTimerLogicClockUser, 1);
  // TODO check return value
  TAOS_CHECK_RETURN(taosTmrStop(pSyncNode->pPingTimer));
  pSyncNode->pPingTimer = NULL;
  return code;
}

int32_t syncNodeStartElectTimer(SSyncNode* pSyncNode, int32_t ms) {
  int32_t code = 0;
  if (syncIsInit()) {
    pSyncNode->electTimerMS = ms;

    int64_t execTime = taosGetTimestampMs() + ms;
    atomic_store_64(&(pSyncNode->electTimerParam.executeTime), execTime);
    atomic_store_64(&(pSyncNode->electTimerParam.logicClock), pSyncNode->electTimerLogicClock);
    pSyncNode->electTimerParam.pSyncNode = pSyncNode;
    pSyncNode->electTimerParam.pData = NULL;

    TAOS_CHECK_RETURN(taosTmrReset(pSyncNode->FpElectTimerCB, pSyncNode->electTimerMS, (void*)(pSyncNode->rid),
                                   syncEnv()->pTimerManager, &pSyncNode->pElectTimer));
  } else {
    sError("vgId:%d, start elect timer error, sync env is stop", pSyncNode->vgId);
  }
  return code;
}

int32_t syncNodeStopElectTimer(SSyncNode* pSyncNode) {
  int32_t code = 0;
  (void)atomic_add_fetch_64(&pSyncNode->electTimerLogicClock, 1);
  // TODO check return value
  TAOS_CHECK_RETURN(taosTmrStop(pSyncNode->pElectTimer));
  pSyncNode->pElectTimer = NULL;

  return code;
}

int32_t syncNodeRestartElectTimer(SSyncNode* pSyncNode, int32_t ms) {
  int32_t ret = 0;
  TAOS_CHECK_RETURN(syncNodeStopElectTimer(pSyncNode));
  TAOS_CHECK_RETURN(syncNodeStartElectTimer(pSyncNode, ms));
  return ret;
}

void syncNodeResetElectTimer(SSyncNode* pSyncNode) {
  int32_t code = 0;
  int32_t electMS;

  if (pSyncNode->raftCfg.isStandBy) {
    electMS = TIMER_MAX_MS;
  } else {
    electMS = syncUtilElectRandomMS(pSyncNode->electBaseLine, 2 * pSyncNode->electBaseLine);
  }

  // TODO check return value
  if ((code = syncNodeRestartElectTimer(pSyncNode, electMS)) != 0) {
    sError("vgId:%d, failed to restart elect timer since %s", pSyncNode->vgId, terrstr());
    return;
  };

  sNTrace(pSyncNode, "reset elect timer, min:%d, max:%d, ms:%d", pSyncNode->electBaseLine, 2 * pSyncNode->electBaseLine,
          electMS);
}

#ifdef BUILD_NO_CALL
static int32_t syncNodeDoStartHeartbeatTimer(SSyncNode* pSyncNode) {
  int32_t code = 0;
  if (syncIsInit()) {
    TAOS_CHECK_RETURN(taosTmrReset(pSyncNode->FpHeartbeatTimerCB, pSyncNode->heartbeatTimerMS, (void*)pSyncNode->rid,
                                   syncEnv()->pTimerManager, &pSyncNode->pHeartbeatTimer));
    atomic_store_64(&pSyncNode->heartbeatTimerLogicClock, pSyncNode->heartbeatTimerLogicClockUser);
  } else {
    sError("vgId:%d, start heartbeat timer error, sync env is stop", pSyncNode->vgId);
  }

  sNTrace(pSyncNode, "start heartbeat timer, ms:%d", pSyncNode->heartbeatTimerMS);
  return code;
}
#endif

int32_t syncNodeStartHeartbeatTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;

#if 0
  pSyncNode->heartbeatTimerMS = pSyncNode->hbBaseLine;
  ret = syncNodeDoStartHeartbeatTimer(pSyncNode);
#endif

  for (int32_t i = 0; i < pSyncNode->peersNum; ++i) {
    SSyncTimer* pSyncTimer = syncNodeGetHbTimer(pSyncNode, &(pSyncNode->peersId[i]));
    if (pSyncTimer != NULL) {
      TAOS_CHECK_RETURN(syncHbTimerStart(pSyncNode, pSyncTimer));
    }
  }

  return ret;
}

int32_t syncNodeStopHeartbeatTimer(SSyncNode* pSyncNode) {
  int32_t code = 0;

#if 0
  //TODO check return value
  TAOS_CHECK_RETURN(atomic_add_fetch_64(&pSyncNode->heartbeatTimerLogicClockUser, 1));
  TAOS_CHECK_RETURN(taosTmrStop(pSyncNode->pHeartbeatTimer));
  pSyncNode->pHeartbeatTimer = NULL;
#endif

  for (int32_t i = 0; i < pSyncNode->peersNum; ++i) {
    SSyncTimer* pSyncTimer = syncNodeGetHbTimer(pSyncNode, &(pSyncNode->peersId[i]));
    if (pSyncTimer != NULL) {
      TAOS_CHECK_RETURN(syncHbTimerStop(pSyncNode, pSyncTimer));
    }
  }

  return code;
}

#ifdef BUILD_NO_CALL
int32_t syncNodeRestartHeartbeatTimer(SSyncNode* pSyncNode) {
  // TODO check return value
  int32_t code = 0;
  TAOS_CHECK_RETURN(syncNodeStopHeartbeatTimer(pSyncNode));
  TAOS_CHECK_RETURN(syncNodeStartHeartbeatTimer(pSyncNode));
  return 0;
}
#endif

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
    sError("vgId:%d, failed to send sync msg since %s. epset:%p dnode:%d addr:%" PRId64, pNode->vgId, tstrerror(code),
           epSet, DID(destRaftId), destRaftId->addr);
    rpcFreeCont(pMsg->pCont);
  }

  TAOS_RETURN(code);
}

inline bool syncNodeInConfig(SSyncNode* pNode, const SSyncCfg* pCfg) {
  bool b1 = false;
  bool b2 = false;

  for (int32_t i = 0; i < pCfg->totalReplicaNum; ++i) {
    if (strcmp(pCfg->nodeInfo[i].nodeFqdn, pNode->myNodeInfo.nodeFqdn) == 0 &&
        pCfg->nodeInfo[i].nodePort == pNode->myNodeInfo.nodePort) {
      b1 = true;
      break;
    }
  }

  for (int32_t i = 0; i < pCfg->totalReplicaNum; ++i) {
    SRaftId raftId = {
        .addr = SYNC_ADDR(&pCfg->nodeInfo[i]),
        .vgId = pNode->vgId,
    };

    if (syncUtilSameId(&raftId, &pNode->myRaftId)) {
      b2 = true;
      break;
    }
  }

  if (b1 != b2) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return false;
  }
  return b1;
}

static bool syncIsConfigChanged(const SSyncCfg* pOldCfg, const SSyncCfg* pNewCfg) {
  if (pOldCfg->totalReplicaNum != pNewCfg->totalReplicaNum) return true;
  if (pOldCfg->myIndex != pNewCfg->myIndex) return true;
  for (int32_t i = 0; i < pOldCfg->totalReplicaNum; ++i) {
    const SNodeInfo* pOldInfo = &pOldCfg->nodeInfo[i];
    const SNodeInfo* pNewInfo = &pNewCfg->nodeInfo[i];
    if (strcmp(pOldInfo->nodeFqdn, pNewInfo->nodeFqdn) != 0) return true;
    if (pOldInfo->nodePort != pNewInfo->nodePort) return true;
    if (pOldInfo->nodeRole != pNewInfo->nodeRole) return true;
  }

  return false;
}

int32_t syncNodeDoConfigChange(SSyncNode* pSyncNode, SSyncCfg* pNewConfig, SyncIndex lastConfigChangeIndex) {
  int32_t  code = 0;
  SSyncCfg oldConfig = pSyncNode->raftCfg.cfg;
  if (!syncIsConfigChanged(&oldConfig, pNewConfig)) {
    sInfo("vgId:1, sync not reconfig since not changed");
    return 0;
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
  sNInfo(pSyncNode, "begin do config change, from %d to %d, from %" PRId64 " to %" PRId64 ", replicas:%d",
         pSyncNode->vgId, oldConfig.totalReplicaNum, pNewConfig->totalReplicaNum, oldConfig.lastIndex,
         pNewConfig->lastIndex);

  if (IamInNew) {
    pSyncNode->raftCfg.isStandBy = 0;  // change isStandBy to normal
  }
  if (isDrop) {
    pSyncNode->raftCfg.isStandBy = 1;  // set standby
  }

  // add last config index
  SRaftCfg* pCfg = &pSyncNode->raftCfg;
  if (pCfg->configIndexCount >= MAX_CONFIG_INDEX_COUNT) {
    sNError(pSyncNode, "failed to add cfg index:%d since out of range", pCfg->configIndexCount);
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  pCfg->configIndexArr[pCfg->configIndexCount] = lastConfigChangeIndex;
  pCfg->configIndexCount++;

  if (IamInNew) {
    //-----------------------------------------
    int32_t ret = 0;

    // save snapshot senders
    SRaftId oldReplicasId[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];
    memcpy(oldReplicasId, pSyncNode->replicasId, sizeof(oldReplicasId));
    SSyncSnapshotSender* oldSenders[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];
    for (int32_t i = 0; i < TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA; ++i) {
      oldSenders[i] = pSyncNode->senders[i];
      sSTrace(oldSenders[i], "snapshot sender save old");
    }

    // init internal
    pSyncNode->myNodeInfo = pSyncNode->raftCfg.cfg.nodeInfo[pSyncNode->raftCfg.cfg.myIndex];
    TAOS_CHECK_RETURN(syncUtilNodeInfo2RaftId(&pSyncNode->myNodeInfo, pSyncNode->vgId, &pSyncNode->myRaftId));

    // init peersNum, peers, peersId
    pSyncNode->peersNum = pSyncNode->raftCfg.cfg.totalReplicaNum - 1;
    int32_t j = 0;
    for (int32_t i = 0; i < pSyncNode->raftCfg.cfg.totalReplicaNum; ++i) {
      if (i != pSyncNode->raftCfg.cfg.myIndex) {
        pSyncNode->peersNodeInfo[j] = pSyncNode->raftCfg.cfg.nodeInfo[i];
        syncUtilNodeInfo2EpSet(&pSyncNode->peersNodeInfo[j], &pSyncNode->peersEpset[j]);
        j++;
      }
    }
    for (int32_t i = 0; i < pSyncNode->peersNum; ++i) {
      TAOS_CHECK_RETURN(syncUtilNodeInfo2RaftId(&pSyncNode->peersNodeInfo[i], pSyncNode->vgId, &pSyncNode->peersId[i]));
    }

    // init replicaNum, replicasId
    pSyncNode->replicaNum = pSyncNode->raftCfg.cfg.replicaNum;
    pSyncNode->totalReplicaNum = pSyncNode->raftCfg.cfg.totalReplicaNum;
    for (int32_t i = 0; i < pSyncNode->raftCfg.cfg.totalReplicaNum; ++i) {
      TAOS_CHECK_RETURN(
          syncUtilNodeInfo2RaftId(&pSyncNode->raftCfg.cfg.nodeInfo[i], pSyncNode->vgId, &pSyncNode->replicasId[i]));
    }

    // update quorum first
    pSyncNode->quorum = syncUtilQuorum(pSyncNode->raftCfg.cfg.replicaNum);

    syncIndexMgrUpdate(pSyncNode->pNextIndex, pSyncNode);
    syncIndexMgrUpdate(pSyncNode->pMatchIndex, pSyncNode);
    voteGrantedUpdate(pSyncNode->pVotesGranted, pSyncNode);
    votesRespondUpdate(pSyncNode->pVotesRespond, pSyncNode);

    // reset snapshot senders

    // clear new
    for (int32_t i = 0; i < TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA; ++i) {
      pSyncNode->senders[i] = NULL;
    }

    // reset new
    for (int32_t i = 0; i < pSyncNode->totalReplicaNum; ++i) {
      // reset sender
      bool reset = false;
      for (int32_t j = 0; j < TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA; ++j) {
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
    for (int32_t i = 0; i < TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA; ++i) {
      if (pSyncNode->senders[i] == NULL) {
        TAOS_CHECK_RETURN(snapshotSenderCreate(pSyncNode, i, &pSyncNode->senders[i]));
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
    for (int32_t i = 0; i < TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA; ++i) {
      if (oldSenders[i] != NULL) {
        sSDebug(oldSenders[i], "snapshot sender destroy old, data:%p replica-index:%d", oldSenders[i], i);
        snapshotSenderDestroy(oldSenders[i]);
        oldSenders[i] = NULL;
      }
    }

    // persist cfg
    TAOS_CHECK_RETURN(syncWriteCfgFile(pSyncNode));
  } else {
    // persist cfg
    TAOS_CHECK_RETURN(syncWriteCfgFile(pSyncNode));
    sNInfo(pSyncNode, "do not config change from %d to %d", oldConfig.totalReplicaNum, pNewConfig->totalReplicaNum);
  }

_END:
  // log end config change
  sNInfo(pSyncNode, "end do config change, from %d to %d", oldConfig.totalReplicaNum, pNewConfig->totalReplicaNum);
  return 0;
}

// raft state change --------------
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

  if (pSyncNode->state == TAOS_SYNC_STATE_ASSIGNED_LEADER) {
    (void)taosThreadMutexLock(&pSyncNode->arbTokenMutex);
    syncUtilGenerateArbToken(pSyncNode->myNodeInfo.nodeId, pSyncNode->vgId, pSyncNode->arbToken);
    sInfo("vgId:%d, step down as assigned leader, new arbToken:%s", pSyncNode->vgId, pSyncNode->arbToken);
    (void)taosThreadMutexUnlock(&pSyncNode->arbTokenMutex);
  }

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
  int32_t code = 0;  // maybe clear leader cache
  if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    pSyncNode->leaderCache = EMPTY_RAFT_ID;
  }

  pSyncNode->hbSlowNum = 0;

  // state change
  pSyncNode->state = TAOS_SYNC_STATE_FOLLOWER;
  pSyncNode->roleTimeMs = taosGetTimestampMs();
  if ((code = syncNodeStopHeartbeatTimer(pSyncNode)) != 0) {
    sError("vgId:%d, failed to stop heartbeat timer since %s", pSyncNode->vgId, tstrerror(code));
    return;
  }

  // trace log
  sNTrace(pSyncNode, "become follower %s", debugStr);

  // send rsp to client
  syncNodeLeaderChangeRsp(pSyncNode);

  // call back
  if (pSyncNode->pFsm != NULL && pSyncNode->pFsm->FpBecomeFollowerCb != NULL) {
    pSyncNode->pFsm->FpBecomeFollowerCb(pSyncNode->pFsm);
  }

  // min match index
  pSyncNode->minMatchIndex = SYNC_INDEX_INVALID;

  // reset log buffer
  if ((code = syncLogBufferReset(pSyncNode->pLogBuf, pSyncNode)) != 0) {
    sError("vgId:%d, failed to reset log buffer since %s", pSyncNode->vgId, tstrerror(code));
    return;
  }

  // reset elect timer
  syncNodeResetElectTimer(pSyncNode);

  sInfo("vgId:%d, become follower. %s", pSyncNode->vgId, debugStr);
}

void syncNodeBecomeLearner(SSyncNode* pSyncNode, const char* debugStr) {
  pSyncNode->hbSlowNum = 0;

  // state change
  pSyncNode->state = TAOS_SYNC_STATE_LEARNER;
  pSyncNode->roleTimeMs = taosGetTimestampMs();

  // trace log
  sNTrace(pSyncNode, "become learner %s", debugStr);

  // call back
  if (pSyncNode->pFsm != NULL && pSyncNode->pFsm->FpBecomeLearnerCb != NULL) {
    pSyncNode->pFsm->FpBecomeLearnerCb(pSyncNode->pFsm);
  }

  // min match index
  pSyncNode->minMatchIndex = SYNC_INDEX_INVALID;

  // reset log buffer
  int32_t code = 0;
  if ((code = syncLogBufferReset(pSyncNode->pLogBuf, pSyncNode)) != 0) {
    sError("vgId:%d, failed to reset log buffer since %s", pSyncNode->vgId, tstrerror(code));
    return;
  };
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
  int32_t code = 0;
  pSyncNode->becomeLeaderNum++;
  pSyncNode->hbrSlowNum = 0;

  // reset restoreFinish
  pSyncNode->restoreFinish = false;

  // state change
  pSyncNode->state = TAOS_SYNC_STATE_LEADER;
  pSyncNode->roleTimeMs = taosGetTimestampMs();

  // set leader cache
  pSyncNode->leaderCache = pSyncNode->myRaftId;

  for (int32_t i = 0; i < pSyncNode->pNextIndex->replicaNum; ++i) {
    SyncIndex lastIndex;
    SyncTerm  lastTerm;
    int32_t   code = syncNodeGetLastIndexTerm(pSyncNode, &lastIndex, &lastTerm);
    if (code != 0) {
      sError("vgId:%d, failed to become leader since %s", pSyncNode->vgId, tstrerror(code));
      return;
    }
    pSyncNode->pNextIndex->index[i] = lastIndex + 1;
  }

  for (int32_t i = 0; i < pSyncNode->pMatchIndex->replicaNum; ++i) {
    // maybe overwrite myself, no harm
    // just do it!
    pSyncNode->pMatchIndex->index[i] = SYNC_INDEX_INVALID;
  }

  // init peer mgr
  if ((code = syncNodePeerStateInit(pSyncNode)) != 0) {
    sError("vgId:%d, failed to init peer state since %s", pSyncNode->vgId, tstrerror(code));
    return;
  }

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
  if ((code = syncNodeStopElectTimer(pSyncNode)) != 0) {
    sError("vgId:%d, failed to stop elect timer since %s", pSyncNode->vgId, tstrerror(code));
    return;
  }

  // start heartbeat timer
  if ((code = syncNodeStartHeartbeatTimer(pSyncNode)) != 0) {
    sError("vgId:%d, failed to start heartbeat timer since %s", pSyncNode->vgId, tstrerror(code));
    return;
  }

  // send heartbeat right now
  if ((code = syncNodeHeartbeatPeers(pSyncNode)) != 0) {
    sError("vgId:%d, failed to send heartbeat to peers since %s", pSyncNode->vgId, tstrerror(code));
    return;
  }

  // call back
  if (pSyncNode->pFsm != NULL && pSyncNode->pFsm->FpBecomeLeaderCb != NULL) {
    pSyncNode->pFsm->FpBecomeLeaderCb(pSyncNode->pFsm);
  }

  // min match index
  pSyncNode->minMatchIndex = SYNC_INDEX_INVALID;

  // reset log buffer
  if ((code = syncLogBufferReset(pSyncNode->pLogBuf, pSyncNode)) != 0) {
    sError("vgId:%d, failed to reset log buffer since %s", pSyncNode->vgId, tstrerror(code));
    return;
  }

  // trace log
  sNInfo(pSyncNode, "become leader %s", debugStr);
}

void syncNodeBecomeAssignedLeader(SSyncNode* pSyncNode) {
  int32_t code = 0;
  pSyncNode->becomeAssignedLeaderNum++;
  pSyncNode->hbrSlowNum = 0;

  // reset restoreFinish
  // pSyncNode->restoreFinish = false;

  // state change
  pSyncNode->state = TAOS_SYNC_STATE_ASSIGNED_LEADER;
  pSyncNode->roleTimeMs = taosGetTimestampMs();

  // set leader cache
  pSyncNode->leaderCache = pSyncNode->myRaftId;

  for (int32_t i = 0; i < pSyncNode->pNextIndex->replicaNum; ++i) {
    SyncIndex lastIndex;
    SyncTerm  lastTerm;
    int32_t   code = syncNodeGetLastIndexTerm(pSyncNode, &lastIndex, &lastTerm);
    if (code != 0) {
      sError("vgId:%d, failed to become assigned leader since %s", pSyncNode->vgId, tstrerror(code));
      return;
    }
    pSyncNode->pNextIndex->index[i] = lastIndex + 1;
  }

  for (int32_t i = 0; i < pSyncNode->pMatchIndex->replicaNum; ++i) {
    // maybe overwrite myself, no harm
    // just do it!
    pSyncNode->pMatchIndex->index[i] = SYNC_INDEX_INVALID;
  }

  // init peer mgr
  if ((code = syncNodePeerStateInit(pSyncNode)) != 0) {
    sError("vgId:%d, failed to init peer state since %s", pSyncNode->vgId, tstrerror(code));
    return;
  }

  // close receiver
  if (snapshotReceiverIsStart(pSyncNode->pNewNodeReceiver)) {
    snapshotReceiverStop(pSyncNode->pNewNodeReceiver);
  }

  // stop elect timer
  if ((code = syncNodeStopElectTimer(pSyncNode)) != 0) {
    sError("vgId:%d, failed to stop elect timer since %s", pSyncNode->vgId, tstrerror(code));
    return;
  }

  // start heartbeat timer
  if ((code = syncNodeStartHeartbeatTimer(pSyncNode)) != 0) {
    sError("vgId:%d, failed to start heartbeat timer since %s", pSyncNode->vgId, tstrerror(code));
    return;
  }

  // send heartbeat right now
  if ((code = syncNodeHeartbeatPeers(pSyncNode)) != 0) {
    sError("vgId:%d, failed to send heartbeat to peers since %s", pSyncNode->vgId, tstrerror(code));
    return;
  }

  // call back
  if (pSyncNode->pFsm != NULL && pSyncNode->pFsm->FpBecomeAssignedLeaderCb != NULL) {
    pSyncNode->pFsm->FpBecomeAssignedLeaderCb(pSyncNode->pFsm);
  }

  // min match index
  pSyncNode->minMatchIndex = SYNC_INDEX_INVALID;

  // reset log buffer
  if ((code = syncLogBufferReset(pSyncNode->pLogBuf, pSyncNode)) != 0) {
    sError("vgId:%d, failed to reset log buffer since %s", pSyncNode->vgId, tstrerror(code));
    return;
  }

  // trace log
  sNInfo(pSyncNode, "become assigned leader");
}

void syncNodeCandidate2Leader(SSyncNode* pSyncNode) {
  if (pSyncNode->state != TAOS_SYNC_STATE_CANDIDATE) {
    sError("vgId:%d, failed leader from candidate since node state is wrong:%d", pSyncNode->vgId, pSyncNode->state);
    return;
  }
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

  sInfo("vgId:%d, become leader. term:%" PRId64 ", commit index:%" PRId64 ", last index:%" PRId64 "", pSyncNode->vgId,
        raftStoreGetTerm(pSyncNode), pSyncNode->commitIndex, lastIndex);
}

bool syncNodeIsMnode(SSyncNode* pSyncNode) { return (pSyncNode->vgId == 1); }

int32_t syncNodePeerStateInit(SSyncNode* pSyncNode) {
  for (int32_t i = 0; i < TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA; ++i) {
    pSyncNode->peerStates[i].lastSendIndex = SYNC_INDEX_INVALID;
    pSyncNode->peerStates[i].lastSendTime = 0;
  }

  return 0;
}

void syncNodeFollower2Candidate(SSyncNode* pSyncNode) {
  if (pSyncNode->state != TAOS_SYNC_STATE_FOLLOWER) {
    sError("vgId:%d, failed candidate from follower since node state is wrong:%d", pSyncNode->vgId, pSyncNode->state);
    return;
  }
  pSyncNode->state = TAOS_SYNC_STATE_CANDIDATE;
  pSyncNode->roleTimeMs = taosGetTimestampMs();
  SyncIndex lastIndex = pSyncNode->pLogStore->syncLogLastIndex(pSyncNode->pLogStore);
  sInfo("vgId:%d, become candidate from follower. term:%" PRId64 ", commit index:%" PRId64 ", last index:%" PRId64,
        pSyncNode->vgId, raftStoreGetTerm(pSyncNode), pSyncNode->commitIndex, lastIndex);

  sNTrace(pSyncNode, "follower to candidate");
}

int32_t syncNodeAssignedLeader2Leader(SSyncNode* pSyncNode) {
  if (pSyncNode->state != TAOS_SYNC_STATE_ASSIGNED_LEADER) return TSDB_CODE_SYN_INTERNAL_ERROR;
  syncNodeBecomeLeader(pSyncNode, "assigned leader to leader");

  sNTrace(pSyncNode, "assigned leader to leader");

  int32_t ret = syncNodeAppendNoop(pSyncNode);
  if (ret < 0) {
    sError("vgId:%d, failed to append noop entry since %s", pSyncNode->vgId, tstrerror(ret));
  }

  SyncIndex lastIndex = pSyncNode->pLogStore->syncLogLastIndex(pSyncNode->pLogStore);
  sInfo("vgId:%d, become leader from assigned leader. term:%" PRId64 ", commit index:%" PRId64
        "assigned commit index:%" PRId64 ", last index:%" PRId64,
        pSyncNode->vgId, raftStoreGetTerm(pSyncNode), pSyncNode->commitIndex, pSyncNode->assignedCommitIndex,
        lastIndex);
  return 0;
}

// just called by syncNodeVoteForSelf
void syncNodeVoteForTerm(SSyncNode* pSyncNode, SyncTerm term, SRaftId* pRaftId) {
  SyncTerm storeTerm = raftStoreGetTerm(pSyncNode);
  if (term != storeTerm) {
    sError("vgId:%d, failed to vote for term, term:%" PRId64 ", storeTerm:%" PRId64, pSyncNode->vgId, term, storeTerm);
    return;
  }
  bool voted = raftStoreHasVoted(pSyncNode);
  if (voted) {
    sError("vgId:%d, failed to vote for term since not voted", pSyncNode->vgId);
    return;
  }

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
    // TODO check return value
    (void)pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
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
    // TODO check return value
    (void)pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
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
      // TODO check return value
      (void)pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
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

#ifdef BUILD_NO_CALL
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
    if (pPreEntry == NULL) return -1;
    preTerm = pPreEntry->term;

    if (h) {
      taosLRUCacheRelease(pCache, h, false);
    } else {
      syncEntryDestroy(pPreEntry);
    }

    return preTerm;
  } else {
    if (pSyncNode->pFsm->FpGetSnapshotInfo != NULL) {
      // TODO check return value
      (void)pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
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
#endif

static void syncNodeEqPingTimer(void* param, void* tmrId) {
  if (!syncIsInit()) return;

  int64_t    rid = (int64_t)param;
  SSyncNode* pNode = syncNodeAcquire(rid);

  if (pNode == NULL) return;

  if (atomic_load_64(&pNode->pingTimerLogicClockUser) <= atomic_load_64(&pNode->pingTimerLogicClock)) {
    SRpcMsg rpcMsg = {0};
    int32_t code = syncBuildTimeout(&rpcMsg, SYNC_TIMEOUT_PING, atomic_load_64(&pNode->pingTimerLogicClock),
                                    pNode->pingTimerMS, pNode);
    if (code != 0) {
      sError("failed to build ping msg");
      rpcFreeCont(rpcMsg.pCont);
      goto _out;
    }

    // sTrace("enqueue ping msg");
    code = pNode->syncEqMsg(pNode->msgcb, &rpcMsg);
    if (code != 0) {
      sError("failed to sync enqueue ping msg since %s", terrstr());
      rpcFreeCont(rpcMsg.pCont);
      goto _out;
    }

  _out:
    if ((code = taosTmrReset(syncNodeEqPingTimer, pNode->pingTimerMS, (void*)pNode->rid, syncEnv()->pTimerManager,
                             &pNode->pPingTimer)) != 0) {
      sError("failed to reset ping timer since %s", tstrerror(code));
    };
  }
  syncNodeRelease(pNode);
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

#ifdef BUILD_NO_CALL
static void syncNodeEqHeartbeatTimer(void* param, void* tmrId) {
  if (!syncIsInit()) return;

  int64_t    rid = (int64_t)param;
  SSyncNode* pNode = syncNodeAcquire(rid);

  if (pNode == NULL) return;

  if (pNode->totalReplicaNum > 1) {
    if (atomic_load_64(&pNode->heartbeatTimerLogicClockUser) <= atomic_load_64(&pNode->heartbeatTimerLogicClock)) {
      SRpcMsg rpcMsg = {0};
      int32_t code = syncBuildTimeout(&rpcMsg, SYNC_TIMEOUT_HEARTBEAT, atomic_load_64(&pNode->heartbeatTimerLogicClock),
                                      pNode->heartbeatTimerMS, pNode);

      if (code != 0) {
        sError("failed to build heartbeat msg");
        goto _out;
      }

      sTrace("vgId:%d, enqueue heartbeat timer", pNode->vgId);
      code = pNode->syncEqMsg(pNode->msgcb, &rpcMsg);
      if (code != 0) {
        sError("failed to enqueue heartbeat msg since %s", terrstr());
        rpcFreeCont(rpcMsg.pCont);
        goto _out;
      }

    _out:
      if (taosTmrReset(syncNodeEqHeartbeatTimer, pNode->heartbeatTimerMS, (void*)pNode->rid, syncEnv()->pTimerManager,
                       &pNode->pHeartbeatTimer) != 0)
        return;

    } else {
      sTrace("==syncNodeEqHeartbeatTimer== heartbeatTimerLogicClock:%" PRId64 ", heartbeatTimerLogicClockUser:%" PRId64,
             pNode->heartbeatTimerLogicClock, pNode->heartbeatTimerLogicClockUser);
    }
  }
}
#endif

static void syncNodeEqPeerHeartbeatTimer(void* param, void* tmrId) {
  int32_t code = 0;
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

  if (pSyncNode->state != TAOS_SYNC_STATE_LEADER && pSyncNode->state != TAOS_SYNC_STATE_ASSIGNED_LEADER) {
    syncNodeRelease(pSyncNode);
    syncHbTimerDataRelease(pData);
    sError("vgId:%d, hb timer sync node not leader", pSyncNode->vgId);
    return;
  }

  sTrace("vgId:%d, eq peer hb timer, rid:%" PRId64 " addr:%" PRId64, pSyncNode->vgId, hbDataRid, pData->destId.addr);

  if (pSyncNode->totalReplicaNum > 1) {
    int64_t timerLogicClock = atomic_load_64(&pSyncTimer->logicClock);
    int64_t msgLogicClock = atomic_load_64(&pData->logicClock);

    if (timerLogicClock == msgLogicClock) {
      if (tsNow > pData->execTime) {
        pData->execTime += pSyncTimer->timerMS;

        SRpcMsg rpcMsg = {0};
        if ((code = syncBuildHeartbeat(&rpcMsg, pSyncNode->vgId)) != 0) {
          sError("vgId:%d, failed to build heartbeat msg since %s", pSyncNode->vgId, tstrerror(code));
          syncNodeRelease(pSyncNode);
          syncHbTimerDataRelease(pData);
          return;
        }

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
        TRACE_SET_MSGID(&(rpcMsg.info.traceId), tGenIdPI64());
        STraceId* trace = &(rpcMsg.info.traceId);
        sGTrace("vgId:%d, send sync-heartbeat to dnode:%d", pSyncNode->vgId, DID(&(pSyncMsg->destId)));
        syncLogSendHeartbeat(pSyncNode, pSyncMsg, false, timerElapsed, pData->execTime);
        if ((code = syncNodeSendHeartbeat(pSyncNode, &pSyncMsg->destId, &rpcMsg)) != 0) {
          sError("vgId:%d, failed to send heartbeat to dnode:%d since %s", pSyncNode->vgId, DID(&(pSyncMsg->destId)),
                 tstrerror(code));
          syncNodeRelease(pSyncNode);
          syncHbTimerDataRelease(pData);
          return;
        }
      } else {
      }

      if (syncIsInit()) {
        // sTrace("vgId:%d, reset peer hb timer", pSyncNode->vgId);
        if ((code = taosTmrReset(syncNodeEqPeerHeartbeatTimer, pSyncTimer->timerMS / HEARTBEAT_TICK_NUM,
                                 (void*)hbDataRid, syncEnv()->pTimerManager, &pSyncTimer->pTimer)) != 0) {
          sError("vgId:%d, reset peer hb timer error, %s", pSyncNode->vgId, tstrerror(code));
          syncNodeRelease(pSyncNode);
          syncHbTimerDataRelease(pData);
          return;
        }
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

#ifdef BUILD_NO_CALL
static void deleteCacheEntry(const void* key, size_t keyLen, void* value, void* ud) {
  (void)ud;
  taosMemoryFree(value);
}

int32_t syncCacheEntry(SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry, LRUHandle** h) {
  SSyncLogStoreData* pData = pLogStore->data;
  sNTrace(pData->pSyncNode, "in cache index:%" PRId64 ", bytes:%u, %p", pEntry->index, pEntry->bytes, pEntry);

  int32_t   code = 0;
  int32_t   entryLen = sizeof(*pEntry) + pEntry->dataLen;
  LRUStatus status = taosLRUCacheInsert(pLogStore->pCache, &pEntry->index, sizeof(pEntry->index), pEntry, entryLen,
                                        deleteCacheEntry, h, TAOS_LRU_PRIORITY_LOW, NULL);
  if (status != TAOS_LRU_STATUS_OK) {
    code = -1;
  }

  return code;
}
#endif

void syncBuildConfigFromReq(SAlterVnodeReplicaReq* pReq, SSyncCfg* cfg) {  // TODO SAlterVnodeReplicaReq name is proper?
  cfg->replicaNum = 0;
  cfg->totalReplicaNum = 0;
  int32_t code = 0;

  for (int i = 0; i < pReq->replica; ++i) {
    SNodeInfo* pNode = &cfg->nodeInfo[i];
    pNode->nodeId = pReq->replicas[i].id;
    pNode->nodePort = pReq->replicas[i].port;
    tstrncpy(pNode->nodeFqdn, pReq->replicas[i].fqdn, sizeof(pNode->nodeFqdn));
    pNode->nodeRole = TAOS_SYNC_ROLE_VOTER;
    if ((code = tmsgUpdateDnodeInfo(&pNode->nodeId, &pNode->clusterId, pNode->nodeFqdn, &pNode->nodePort)) != 0) {
      sError("vgId:%d, failed to update dnode info since %s", pReq->vgId, tstrerror(code));
    }
    sInfo("vgId:%d, replica:%d ep:%s:%u dnode:%d nodeRole:%d", pReq->vgId, i, pNode->nodeFqdn, pNode->nodePort,
          pNode->nodeId, pNode->nodeRole);
    cfg->replicaNum++;
  }
  if (pReq->selfIndex != -1) {
    cfg->myIndex = pReq->selfIndex;
  }
  for (int i = cfg->replicaNum; i < pReq->replica + pReq->learnerReplica; ++i) {
    SNodeInfo* pNode = &cfg->nodeInfo[i];
    pNode->nodeId = pReq->learnerReplicas[cfg->totalReplicaNum].id;
    pNode->nodePort = pReq->learnerReplicas[cfg->totalReplicaNum].port;
    pNode->nodeRole = TAOS_SYNC_ROLE_LEARNER;
    tstrncpy(pNode->nodeFqdn, pReq->learnerReplicas[cfg->totalReplicaNum].fqdn, sizeof(pNode->nodeFqdn));
    if ((code = tmsgUpdateDnodeInfo(&pNode->nodeId, &pNode->clusterId, pNode->nodeFqdn, &pNode->nodePort)) != 0) {
      sError("vgId:%d, failed to update dnode info, %s", pReq->vgId, tstrerror(code));
    }
    sInfo("vgId:%d, replica:%d ep:%s:%u dnode:%d nodeRole:%d", pReq->vgId, i, pNode->nodeFqdn, pNode->nodePort,
          pNode->nodeId, pNode->nodeRole);
    cfg->totalReplicaNum++;
  }
  cfg->totalReplicaNum += pReq->replica;
  if (pReq->learnerSelfIndex != -1) {
    cfg->myIndex = pReq->replica + pReq->learnerSelfIndex;
  }
  cfg->changeVersion = pReq->changeVersion;
}

int32_t syncNodeCheckChangeConfig(SSyncNode* ths, SSyncRaftEntry* pEntry) {
  int32_t code = 0;
  if (pEntry->originalRpcType != TDMT_SYNC_CONFIG_CHANGE) {
    return TSDB_CODE_SYN_INTERNAL_ERROR;
  }

  SMsgHead* head = (SMsgHead*)pEntry->data;
  void*     pReq = POINTER_SHIFT(head, sizeof(SMsgHead));

  SAlterVnodeTypeReq req = {0};
  if (tDeserializeSAlterVnodeReplicaReq(pReq, head->contLen, &req) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    TAOS_RETURN(code);
  }

  SSyncCfg cfg = {0};
  syncBuildConfigFromReq(&req, &cfg);

  if (cfg.totalReplicaNum >= 1 &&
      (ths->state == TAOS_SYNC_STATE_LEADER || ths->state == TAOS_SYNC_STATE_ASSIGNED_LEADER)) {
    bool incfg = false;
    for (int32_t j = 0; j < cfg.totalReplicaNum; ++j) {
      if (strcmp(ths->myNodeInfo.nodeFqdn, cfg.nodeInfo[j].nodeFqdn) == 0 &&
          ths->myNodeInfo.nodePort == cfg.nodeInfo[j].nodePort) {
        incfg = true;
        break;
      }
    }

    if (!incfg) {
      SyncTerm currentTerm = raftStoreGetTerm(ths);
      syncNodeStepDown(ths, currentTerm);
      return 1;
    }
  }
  return 0;
}

void syncNodeLogConfigInfo(SSyncNode* ths, SSyncCfg* cfg, char* str) {
  sInfo("vgId:%d, %s. SyncNode, replicaNum:%d, peersNum:%d, lastConfigIndex:%" PRId64
        ", changeVersion:%d, "
        "restoreFinish:%d",
        ths->vgId, str, ths->replicaNum, ths->peersNum, ths->raftCfg.lastConfigIndex, ths->raftCfg.cfg.changeVersion,
        ths->restoreFinish);

  sInfo("vgId:%d, %s, myNodeInfo, clusterId:%" PRId64 ", nodeId:%d, Fqdn:%s, port:%d, role:%d", ths->vgId, str,
        ths->myNodeInfo.clusterId, ths->myNodeInfo.nodeId, ths->myNodeInfo.nodeFqdn, ths->myNodeInfo.nodePort,
        ths->myNodeInfo.nodeRole);

  for (int32_t i = 0; i < ths->peersNum; ++i) {
    sInfo("vgId:%d, %s, peersNodeInfo%d, clusterId:%" PRId64 ", nodeId:%d, Fqdn:%s, port:%d, role:%d", ths->vgId, str,
          i, ths->peersNodeInfo[i].clusterId, ths->peersNodeInfo[i].nodeId, ths->peersNodeInfo[i].nodeFqdn,
          ths->peersNodeInfo[i].nodePort, ths->peersNodeInfo[i].nodeRole);
  }

  for (int32_t i = 0; i < ths->peersNum; ++i) {
    char    buf[256];
    int32_t len = 256;
    int32_t n = 0;
    n += snprintf(buf + n, len - n, "%s", "{");
    for (int i = 0; i < ths->peersEpset->numOfEps; i++) {
      n += snprintf(buf + n, len - n, "%s:%d%s", ths->peersEpset->eps[i].fqdn, ths->peersEpset->eps[i].port,
                    (i + 1 < ths->peersEpset->numOfEps ? ", " : ""));
    }
    n += snprintf(buf + n, len - n, "%s", "}");

    sInfo("vgId:%d, %s, peersEpset%d, %s, inUse:%d", ths->vgId, str, i, buf, ths->peersEpset->inUse);
  }

  for (int32_t i = 0; i < ths->peersNum; ++i) {
    sInfo("vgId:%d, %s, peersId%d, addr:%" PRId64, ths->vgId, str, i, ths->peersId[i].addr);
  }

  for (int32_t i = 0; i < ths->raftCfg.cfg.totalReplicaNum; ++i) {
    sInfo("vgId:%d, %s, nodeInfo%d, clusterId:%" PRId64 ", nodeId:%d, Fqdn:%s, port:%d, role:%d", ths->vgId, str, i,
          ths->raftCfg.cfg.nodeInfo[i].clusterId, ths->raftCfg.cfg.nodeInfo[i].nodeId,
          ths->raftCfg.cfg.nodeInfo[i].nodeFqdn, ths->raftCfg.cfg.nodeInfo[i].nodePort,
          ths->raftCfg.cfg.nodeInfo[i].nodeRole);
  }

  for (int32_t i = 0; i < ths->raftCfg.cfg.totalReplicaNum; ++i) {
    sInfo("vgId:%d, %s, replicasId%d, addr:%" PRId64, ths->vgId, str, i, ths->replicasId[i].addr);
  }
}

int32_t syncNodeRebuildPeerAndCfg(SSyncNode* ths, SSyncCfg* cfg) {
  int32_t i = 0;

  // change peersNodeInfo
  i = 0;
  for (int32_t j = 0; j < cfg->totalReplicaNum; ++j) {
    if (!(strcmp(ths->myNodeInfo.nodeFqdn, cfg->nodeInfo[j].nodeFqdn) == 0 &&
          ths->myNodeInfo.nodePort == cfg->nodeInfo[j].nodePort)) {
      ths->peersNodeInfo[i].nodeRole = cfg->nodeInfo[j].nodeRole;
      ths->peersNodeInfo[i].clusterId = cfg->nodeInfo[j].clusterId;
      tstrncpy(ths->peersNodeInfo[i].nodeFqdn, cfg->nodeInfo[j].nodeFqdn, TSDB_FQDN_LEN);
      ths->peersNodeInfo[i].nodeId = cfg->nodeInfo[j].nodeId;
      ths->peersNodeInfo[i].nodePort = cfg->nodeInfo[j].nodePort;

      syncUtilNodeInfo2EpSet(&ths->peersNodeInfo[i], &ths->peersEpset[i]);

      if (!syncUtilNodeInfo2RaftId(&ths->peersNodeInfo[i], ths->vgId, &ths->peersId[i])) {
        sError("vgId:%d, failed to determine raft member id, peer:%d", ths->vgId, i);
        return -1;
      }

      i++;
    }
  }
  ths->peersNum = i;

  // change cfg nodeInfo
  ths->raftCfg.cfg.replicaNum = 0;
  i = 0;
  for (int32_t j = 0; j < cfg->totalReplicaNum; ++j) {
    if (cfg->nodeInfo[j].nodeRole == TAOS_SYNC_ROLE_VOTER) {
      ths->raftCfg.cfg.replicaNum++;
    }
    ths->raftCfg.cfg.nodeInfo[i].nodeRole = cfg->nodeInfo[j].nodeRole;
    ths->raftCfg.cfg.nodeInfo[i].clusterId = cfg->nodeInfo[j].clusterId;
    tstrncpy(ths->raftCfg.cfg.nodeInfo[i].nodeFqdn, cfg->nodeInfo[j].nodeFqdn, TSDB_FQDN_LEN);
    ths->raftCfg.cfg.nodeInfo[i].nodeId = cfg->nodeInfo[j].nodeId;
    ths->raftCfg.cfg.nodeInfo[i].nodePort = cfg->nodeInfo[j].nodePort;
    if ((strcmp(ths->myNodeInfo.nodeFqdn, cfg->nodeInfo[j].nodeFqdn) == 0 &&
         ths->myNodeInfo.nodePort == cfg->nodeInfo[j].nodePort)) {
      ths->raftCfg.cfg.myIndex = i;
    }
    i++;
  }
  ths->raftCfg.cfg.totalReplicaNum = i;

  return 0;
}

void syncNodeChangePeerAndCfgToVoter(SSyncNode* ths, SSyncCfg* cfg) {
  // change peersNodeInfo
  for (int32_t i = 0; i < ths->peersNum; ++i) {
    for (int32_t j = 0; j < cfg->totalReplicaNum; ++j) {
      if (strcmp(ths->peersNodeInfo[i].nodeFqdn, cfg->nodeInfo[j].nodeFqdn) == 0 &&
          ths->peersNodeInfo[i].nodePort == cfg->nodeInfo[j].nodePort) {
        if (cfg->nodeInfo[j].nodeRole == TAOS_SYNC_ROLE_VOTER) {
          ths->peersNodeInfo[i].nodeRole = TAOS_SYNC_ROLE_VOTER;
        }
      }
    }
  }

  // change cfg nodeInfo
  ths->raftCfg.cfg.replicaNum = 0;
  for (int32_t i = 0; i < ths->raftCfg.cfg.totalReplicaNum; ++i) {
    for (int32_t j = 0; j < cfg->totalReplicaNum; ++j) {
      if (strcmp(ths->raftCfg.cfg.nodeInfo[i].nodeFqdn, cfg->nodeInfo[j].nodeFqdn) == 0 &&
          ths->raftCfg.cfg.nodeInfo[i].nodePort == cfg->nodeInfo[j].nodePort) {
        if (cfg->nodeInfo[j].nodeRole == TAOS_SYNC_ROLE_VOTER) {
          ths->raftCfg.cfg.nodeInfo[i].nodeRole = TAOS_SYNC_ROLE_VOTER;
          ths->raftCfg.cfg.replicaNum++;
        }
      }
    }
  }
}

int32_t syncNodeRebuildAndCopyIfExist(SSyncNode* ths, int32_t oldtotalReplicaNum) {
  int32_t code = 0;
  // 1.rebuild replicasId, remove deleted one
  SRaftId oldReplicasId[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];
  memcpy(oldReplicasId, ths->replicasId, sizeof(oldReplicasId));

  ths->replicaNum = ths->raftCfg.cfg.replicaNum;
  ths->totalReplicaNum = ths->raftCfg.cfg.totalReplicaNum;
  for (int32_t i = 0; i < ths->raftCfg.cfg.totalReplicaNum; ++i) {
    if (!syncUtilNodeInfo2RaftId(&ths->raftCfg.cfg.nodeInfo[i], ths->vgId, &ths->replicasId[i]))
      return TSDB_CODE_SYN_INTERNAL_ERROR;
  }

  // 2.rebuild MatchIndex, remove deleted one
  SSyncIndexMgr* oldIndex = ths->pMatchIndex;

  ths->pMatchIndex = syncIndexMgrCreate(ths);
  if (ths->pMatchIndex == NULL) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  syncIndexMgrCopyIfExist(ths->pMatchIndex, oldIndex, oldReplicasId);

  syncIndexMgrDestroy(oldIndex);

  // 3.rebuild NextIndex, remove deleted one
  SSyncIndexMgr* oldNextIndex = ths->pNextIndex;

  ths->pNextIndex = syncIndexMgrCreate(ths);
  if (ths->pNextIndex == NULL) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  syncIndexMgrCopyIfExist(ths->pNextIndex, oldNextIndex, oldReplicasId);

  syncIndexMgrDestroy(oldNextIndex);

  // 4.rebuild pVotesGranted, pVotesRespond, no need to keep old vote state, only rebuild
  voteGrantedUpdate(ths->pVotesGranted, ths);
  votesRespondUpdate(ths->pVotesRespond, ths);

  // 5.rebuild logReplMgr
  for (int i = 0; i < oldtotalReplicaNum; ++i) {
    sDebug("vgId:%d, old logReplMgrs i:%d, peerId:%d, restoreed:%d, [%" PRId64 " %" PRId64 ", %" PRId64 ")", ths->vgId,
           i, ths->logReplMgrs[i]->peerId, ths->logReplMgrs[i]->restored, ths->logReplMgrs[i]->startIndex,
           ths->logReplMgrs[i]->matchIndex, ths->logReplMgrs[i]->endIndex);
  }

  SSyncLogReplMgr* oldLogReplMgrs = NULL;
  int64_t          length = sizeof(SSyncLogReplMgr) * (TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA);
  oldLogReplMgrs = taosMemoryMalloc(length);
  if (NULL == oldLogReplMgrs) return TSDB_CODE_OUT_OF_MEMORY;
  memset(oldLogReplMgrs, 0, length);

  for (int i = 0; i < oldtotalReplicaNum; i++) {
    oldLogReplMgrs[i] = *(ths->logReplMgrs[i]);
  }

  syncNodeLogReplDestroy(ths);
  if ((code = syncNodeLogReplInit(ths)) != 0) {
    taosMemoryFree(oldLogReplMgrs);
    TAOS_RETURN(code);
  }

  for (int i = 0; i < ths->totalReplicaNum; ++i) {
    for (int j = 0; j < oldtotalReplicaNum; j++) {
      if (syncUtilSameId(&ths->replicasId[i], &oldReplicasId[j])) {
        *(ths->logReplMgrs[i]) = oldLogReplMgrs[j];
        ths->logReplMgrs[i]->peerId = i;
      }
    }
  }

  for (int i = 0; i < ths->totalReplicaNum; ++i) {
    sDebug("vgId:%d, new logReplMgrs i:%d, peerId:%d, restoreed:%d, [%" PRId64 " %" PRId64 ", %" PRId64 ")", ths->vgId,
           i, ths->logReplMgrs[i]->peerId, ths->logReplMgrs[i]->restored, ths->logReplMgrs[i]->startIndex,
           ths->logReplMgrs[i]->matchIndex, ths->logReplMgrs[i]->endIndex);
  }

  // 6.rebuild sender
  for (int i = 0; i < oldtotalReplicaNum; ++i) {
    sDebug("vgId:%d, old sender i:%d, replicaIndex:%d, lastSendTime:%" PRId64, ths->vgId, i,
           ths->senders[i]->replicaIndex, ths->senders[i]->lastSendTime)
  }

  for (int32_t i = 0; i < TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA; ++i) {
    if (ths->senders[i] != NULL) {
      sDebug("vgId:%d, snapshot sender destroy while close, data:%p", ths->vgId, ths->senders[i]);

      if (snapshotSenderIsStart(ths->senders[i])) {
        snapshotSenderStop(ths->senders[i], false);
      }

      snapshotSenderDestroy(ths->senders[i]);
      ths->senders[i] = NULL;
    }
  }

  for (int32_t i = 0; i < TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA; ++i) {
    SSyncSnapshotSender* pSender = NULL;
    int32_t              code = snapshotSenderCreate(ths, i, &pSender);
    if (pSender == NULL) return terrno = code;

    ths->senders[i] = pSender;
    sSDebug(pSender, "snapshot sender create while open sync node, data:%p", pSender);
  }

  for (int i = 0; i < ths->totalReplicaNum; i++) {
    sDebug("vgId:%d, new sender i:%d, replicaIndex:%d, lastSendTime:%" PRId64, ths->vgId, i,
           ths->senders[i]->replicaIndex, ths->senders[i]->lastSendTime)
  }

  // 7.rebuild synctimer
  if ((code = syncNodeStopHeartbeatTimer(ths)) != 0) {
    taosMemoryFree(oldLogReplMgrs);
    TAOS_RETURN(code);
  }

  for (int32_t i = 0; i < TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA; ++i) {
    if ((code = syncHbTimerInit(ths, &ths->peerHeartbeatTimerArr[i], ths->replicasId[i])) != 0) {
      taosMemoryFree(oldLogReplMgrs);
      TAOS_RETURN(code);
    }
  }

  if ((code = syncNodeStartHeartbeatTimer(ths)) != 0) {
    taosMemoryFree(oldLogReplMgrs);
    TAOS_RETURN(code);
  }

  // 8.rebuild peerStates
  SPeerState oldState[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA] = {0};
  for (int i = 0; i < TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA; i++) {
    oldState[i] = ths->peerStates[i];
  }

  for (int i = 0; i < ths->totalReplicaNum; i++) {
    for (int j = 0; j < oldtotalReplicaNum; j++) {
      if (syncUtilSameId(&ths->replicasId[i], &oldReplicasId[j])) {
        ths->peerStates[i] = oldState[j];
      }
    }
  }

  taosMemoryFree(oldLogReplMgrs);

  return 0;
}

void syncNodeChangeToVoter(SSyncNode* ths) {
  // replicasId, only need to change replicaNum when 1->3
  ths->replicaNum = ths->raftCfg.cfg.replicaNum;
  sDebug("vgId:%d, totalReplicaNum:%d", ths->vgId, ths->totalReplicaNum);
  for (int32_t i = 0; i < ths->totalReplicaNum; ++i) {
    sDebug("vgId:%d, i:%d, replicaId.addr:%" PRIx64, ths->vgId, i, ths->replicasId[i].addr);
  }

  // pMatchIndex, pNextIndex, only need to change replicaNum when 1->3
  ths->pMatchIndex->replicaNum = ths->raftCfg.cfg.replicaNum;
  ths->pNextIndex->replicaNum = ths->raftCfg.cfg.replicaNum;

  sDebug("vgId:%d, pMatchIndex->totalReplicaNum:%d", ths->vgId, ths->pMatchIndex->totalReplicaNum);
  for (int32_t i = 0; i < ths->pMatchIndex->totalReplicaNum; ++i) {
    sDebug("vgId:%d, i:%d, match.index:%" PRId64, ths->vgId, i, ths->pMatchIndex->index[i]);
  }

  // pVotesGranted, pVotesRespond
  voteGrantedUpdate(ths->pVotesGranted, ths);
  votesRespondUpdate(ths->pVotesRespond, ths);

  // logRepMgrs
  // no need to change logRepMgrs when 1->3
}

void syncNodeResetPeerAndCfg(SSyncNode* ths) {
  SNodeInfo node = {0};
  for (int32_t i = 0; i < ths->peersNum; ++i) {
    memcpy(&ths->peersNodeInfo[i], &node, sizeof(SNodeInfo));
  }

  for (int32_t i = 0; i < ths->raftCfg.cfg.totalReplicaNum; ++i) {
    memcpy(&ths->raftCfg.cfg.nodeInfo[i], &node, sizeof(SNodeInfo));
  }
}

int32_t syncNodeChangeConfig(SSyncNode* ths, SSyncRaftEntry* pEntry, char* str) {
  int32_t code = 0;
  if (pEntry->originalRpcType != TDMT_SYNC_CONFIG_CHANGE) {
    return TSDB_CODE_SYN_INTERNAL_ERROR;
  }

  SMsgHead* head = (SMsgHead*)pEntry->data;
  void*     pReq = POINTER_SHIFT(head, sizeof(SMsgHead));

  SAlterVnodeTypeReq req = {0};
  if (tDeserializeSAlterVnodeReplicaReq(pReq, head->contLen, &req) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    TAOS_RETURN(code);
  }

  SSyncCfg cfg = {0};
  syncBuildConfigFromReq(&req, &cfg);

  if (cfg.changeVersion <= ths->raftCfg.cfg.changeVersion) {
    sInfo(
        "vgId:%d, skip conf change entry since lower version. "
        "this entry, index:%" PRId64 ", term:%" PRId64
        ", totalReplicaNum:%d, changeVersion:%d; "
        "current node, replicaNum:%d, peersNum:%d, lastConfigIndex:%" PRId64 ", changeVersion:%d",
        ths->vgId, pEntry->index, pEntry->term, cfg.totalReplicaNum, cfg.changeVersion, ths->replicaNum, ths->peersNum,
        ths->raftCfg.lastConfigIndex, ths->raftCfg.cfg.changeVersion);
    return 0;
  }

  if (strcmp(str, "Commit") == 0) {
    sInfo(
        "vgId:%d, change config from %s. "
        "this, i:%" PRId64
        ", trNum:%d, vers:%d; "
        "node, rNum:%d, pNum:%d, trNum:%d, "
        "buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64
        "), "
        "cond:(next i:%" PRId64 ", t:%" PRId64 " ==%s)",
        ths->vgId, str, pEntry->index - 1, cfg.totalReplicaNum, cfg.changeVersion, ths->replicaNum, ths->peersNum,
        ths->totalReplicaNum, ths->pLogBuf->startIndex, ths->pLogBuf->commitIndex, ths->pLogBuf->matchIndex,
        ths->pLogBuf->endIndex, pEntry->index, pEntry->term, TMSG_INFO(pEntry->originalRpcType));
  } else {
    sInfo(
        "vgId:%d, change config from %s. "
        "this, i:%" PRId64 ", t:%" PRId64
        ", trNum:%d, vers:%d; "
        "node, rNum:%d, pNum:%d, trNum:%d, "
        "buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64
        "), "
        "cond:(pre i:%" PRId64 "==ci:%" PRId64 ", bci:%" PRId64 ")",
        ths->vgId, str, pEntry->index, pEntry->term, cfg.totalReplicaNum, cfg.changeVersion, ths->replicaNum,
        ths->peersNum, ths->totalReplicaNum, ths->pLogBuf->startIndex, ths->pLogBuf->commitIndex,
        ths->pLogBuf->matchIndex, ths->pLogBuf->endIndex, pEntry->index - 1, ths->commitIndex,
        ths->pLogBuf->commitIndex);
  }

  syncNodeLogConfigInfo(ths, &cfg, "before config change");

  int32_t oldTotalReplicaNum = ths->totalReplicaNum;

  if (cfg.totalReplicaNum == 1 || cfg.totalReplicaNum == 2) {  // remove replica

    bool incfg = false;
    for (int32_t j = 0; j < cfg.totalReplicaNum; ++j) {
      if (strcmp(ths->myNodeInfo.nodeFqdn, cfg.nodeInfo[j].nodeFqdn) == 0 &&
          ths->myNodeInfo.nodePort == cfg.nodeInfo[j].nodePort) {
        incfg = true;
        break;
      }
    }

    if (incfg) {  // remove other
      syncNodeResetPeerAndCfg(ths);

      // no need to change myNodeInfo

      if ((code = syncNodeRebuildPeerAndCfg(ths, &cfg)) != 0) {
        TAOS_RETURN(code);
      };

      if ((code = syncNodeRebuildAndCopyIfExist(ths, oldTotalReplicaNum)) != 0) {
        TAOS_RETURN(code);
      };
    } else {  // remove myself
      // no need to do anything actually, to change the following to reduce distruptive server chance

      syncNodeResetPeerAndCfg(ths);

      // change myNodeInfo
      ths->myNodeInfo.nodeRole = TAOS_SYNC_ROLE_LEARNER;

      // change peer and cfg
      ths->peersNum = 0;
      memcpy(&ths->raftCfg.cfg.nodeInfo[0], &ths->myNodeInfo, sizeof(SNodeInfo));
      ths->raftCfg.cfg.replicaNum = 0;
      ths->raftCfg.cfg.totalReplicaNum = 1;

      // change other
      if ((code = syncNodeRebuildAndCopyIfExist(ths, oldTotalReplicaNum)) != 0) {
        TAOS_RETURN(code);
      }

      // change state
      ths->state = TAOS_SYNC_STATE_LEARNER;
    }

    ths->restoreFinish = false;
  } else {                            // add replica, or change replica type
    if (ths->totalReplicaNum == 3) {  // change replica type
      sInfo("vgId:%d, begin change replica type", ths->vgId);

      // change myNodeInfo
      for (int32_t j = 0; j < cfg.totalReplicaNum; ++j) {
        if (strcmp(ths->myNodeInfo.nodeFqdn, cfg.nodeInfo[j].nodeFqdn) == 0 &&
            ths->myNodeInfo.nodePort == cfg.nodeInfo[j].nodePort) {
          if (cfg.nodeInfo[j].nodeRole == TAOS_SYNC_ROLE_VOTER) {
            ths->myNodeInfo.nodeRole = TAOS_SYNC_ROLE_VOTER;
          }
        }
      }

      // change peer and cfg
      syncNodeChangePeerAndCfgToVoter(ths, &cfg);

      // change other
      syncNodeChangeToVoter(ths);

      // change state
      if (ths->state == TAOS_SYNC_STATE_LEARNER) {
        if (ths->myNodeInfo.nodeRole == TAOS_SYNC_ROLE_VOTER) {
          ths->state = TAOS_SYNC_STATE_FOLLOWER;
        }
      }

      ths->restoreFinish = false;
    } else {  // add replica
      sInfo("vgId:%d, begin add replica", ths->vgId);

      // no need to change myNodeInfo

      // change peer and cfg
      if ((code = syncNodeRebuildPeerAndCfg(ths, &cfg)) != 0) {
        TAOS_RETURN(code);
      };

      // change other
      if ((code = syncNodeRebuildAndCopyIfExist(ths, oldTotalReplicaNum)) != 0) {
        TAOS_RETURN(code);
      };

      // no need to change state

      if (ths->myNodeInfo.nodeRole == TAOS_SYNC_ROLE_LEARNER) {
        ths->restoreFinish = false;
      }
    }
  }

  ths->quorum = syncUtilQuorum(ths->replicaNum);

  ths->raftCfg.lastConfigIndex = pEntry->index;
  ths->raftCfg.cfg.lastIndex = pEntry->index;
  ths->raftCfg.cfg.changeVersion = cfg.changeVersion;

  syncNodeLogConfigInfo(ths, &cfg, "after config change");

  if ((code = syncWriteCfgFile(ths)) != 0) {
    sError("vgId:%d, failed to create sync cfg file", ths->vgId);
    TAOS_RETURN(code);
  };

  TAOS_RETURN(code);
}

int32_t syncNodeAppend(SSyncNode* ths, SSyncRaftEntry* pEntry) {
  int32_t code = -1;
  if (pEntry->dataLen < sizeof(SMsgHead)) {
    code = TSDB_CODE_SYN_INTERNAL_ERROR;
    sError("vgId:%d, cannot append an invalid client request with no msg head. type:%s, dataLen:%d", ths->vgId,
           TMSG_INFO(pEntry->originalRpcType), pEntry->dataLen);
    syncEntryDestroy(pEntry);
    pEntry = NULL;
    goto _out;
  }

  // append to log buffer
  if ((code = syncLogBufferAppend(ths->pLogBuf, ths, pEntry)) < 0) {
    sError("vgId:%d, failed to enqueue sync log buffer, index:%" PRId64, ths->vgId, pEntry->index);
    int32_t ret = 0;
    if ((ret = syncFsmExecute(ths, ths->pFsm, ths->state, raftStoreGetTerm(ths), pEntry, terrno, false)) != 0) {
      sError("vgId:%d, failed to execute fsm, since %s", ths->vgId, tstrerror(ret));
    }
    syncEntryDestroy(pEntry);
    pEntry = NULL;
    goto _out;
  }

  code = 0;
_out:;
  // proceed match index, with replicating on needed
  SyncIndex matchIndex = syncLogBufferProceed(ths->pLogBuf, ths, NULL, "Append");

  if (pEntry != NULL)
    sTrace("vgId:%d, append raft entry. index:%" PRId64 ", term:%" PRId64 " pBuf: [%" PRId64 " %" PRId64 " %" PRId64
           ", %" PRId64 ")",
           ths->vgId, pEntry->index, pEntry->term, ths->pLogBuf->startIndex, ths->pLogBuf->commitIndex,
           ths->pLogBuf->matchIndex, ths->pLogBuf->endIndex);

  if (code == 0 && ths->state == TAOS_SYNC_STATE_ASSIGNED_LEADER) {
    TAOS_CHECK_RETURN(syncNodeUpdateAssignedCommitIndex(ths, matchIndex));

    if (ths->fsmState != SYNC_FSM_STATE_INCOMPLETE &&
        syncLogBufferCommit(ths->pLogBuf, ths, ths->assignedCommitIndex) < 0) {
      sError("vgId:%d, failed to commit until commitIndex:%" PRId64 "", ths->vgId, ths->commitIndex);
      code = TSDB_CODE_SYN_INTERNAL_ERROR;
    }
  }

  // multi replica
  if (ths->replicaNum > 1) {
    TAOS_RETURN(code);
  }

  // single replica
  SyncIndex returnIndex = syncNodeUpdateCommitIndex(ths, matchIndex);
  sTrace("vgId:%d, update commit return index %" PRId64 "", ths->vgId, returnIndex);

  if (ths->fsmState != SYNC_FSM_STATE_INCOMPLETE &&
      (code = syncLogBufferCommit(ths->pLogBuf, ths, ths->commitIndex)) < 0) {
    sError("vgId:%d, failed to commit until commitIndex:%" PRId64 "", ths->vgId, ths->commitIndex);
  }

  TAOS_RETURN(code);
}

bool syncNodeHeartbeatReplyTimeout(SSyncNode* pSyncNode) {
  if (pSyncNode->totalReplicaNum == 1) {
    return false;
  }

  int32_t toCount = 0;
  int64_t tsNow = taosGetTimestampMs();
  for (int32_t i = 0; i < pSyncNode->peersNum; ++i) {
    if (pSyncNode->peersNodeInfo[i].nodeRole == TAOS_SYNC_ROLE_LEARNER) {
      continue;
    }
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
  for (int32_t i = 0; i < pSyncNode->totalReplicaNum; ++i) {
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
  int32_t   code = 0;
  SyncIndex index = syncLogBufferGetEndIndex(ths->pLogBuf);
  SyncTerm  term = raftStoreGetTerm(ths);

  SSyncRaftEntry* pEntry = syncEntryBuildNoop(term, index, ths->vgId);
  if (pEntry == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TAOS_RETURN(code);
  }

  code = syncNodeAppend(ths, pEntry);
  TAOS_RETURN(code);
}

#ifdef BUILD_NO_CALL
static int32_t syncNodeAppendNoopOld(SSyncNode* ths) {
  int32_t ret = 0;

  SyncIndex       index = ths->pLogStore->syncLogWriteIndex(ths->pLogStore);
  SyncTerm        term = raftStoreGetTerm(ths);
  SSyncRaftEntry* pEntry = syncEntryBuildNoop(term, index, ths->vgId);
  if (pEntry == NULL) return -1;

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
#endif

int32_t syncNodeOnHeartbeat(SSyncNode* ths, const SRpcMsg* pRpcMsg) {
  SyncHeartbeat* pMsg = pRpcMsg->pCont;
  bool           resetElect = false;

  const STraceId* trace = &pRpcMsg->info.traceId;
  char            tbuf[40] = {0};
  TRACE_TO_STR(trace, tbuf);

  int64_t tsMs = taosGetTimestampMs();
  int64_t timeDiff = tsMs - pMsg->timeStamp;
  syncLogRecvHeartbeat(ths, pMsg, timeDiff, tbuf);

  if (!syncNodeInRaftGroup(ths, &pMsg->srcId)) {
    sWarn(
        "vgId:%d, drop heartbeat msg from dnode:%d, because it come from another cluster:%d, differ from current "
        "cluster:%d",
        ths->vgId, DID(&(pMsg->srcId)), CID(&(pMsg->srcId)), CID(&(ths->myRaftId)));
    return 0;
  }

  SRpcMsg rpcMsg = {0};
  TAOS_CHECK_RETURN(syncBuildHeartbeatReply(&rpcMsg, ths->vgId));
  SyncTerm currentTerm = raftStoreGetTerm(ths);

  SyncHeartbeatReply* pMsgReply = rpcMsg.pCont;
  pMsgReply->destId = pMsg->srcId;
  pMsgReply->srcId = ths->myRaftId;
  pMsgReply->term = currentTerm;
  pMsgReply->privateTerm = 8864;  // magic number
  pMsgReply->startTime = ths->startTime;
  pMsgReply->timeStamp = tsMs;

  sGTrace("vgId:%d, process sync-heartbeat msg from dnode:%d, cluster:%d, Msgterm:%" PRId64 " currentTerm:%" PRId64,
          ths->vgId, DID(&(pMsg->srcId)), CID(&(pMsg->srcId)), pMsg->term, currentTerm);

  if (pMsg->term > currentTerm && ths->state == TAOS_SYNC_STATE_LEARNER) {
    raftStoreSetTerm(ths, pMsg->term);
    currentTerm = pMsg->term;
  }

  if (pMsg->term == currentTerm &&
      (ths->state != TAOS_SYNC_STATE_LEADER && ths->state != TAOS_SYNC_STATE_ASSIGNED_LEADER)) {
    syncIndexMgrSetRecvTime(ths->pNextIndex, &(pMsg->srcId), tsMs);
    resetElect = true;

    ths->minMatchIndex = pMsg->minMatchIndex;

    if (ths->state == TAOS_SYNC_STATE_FOLLOWER || ths->state == TAOS_SYNC_STATE_LEARNER) {
      SRpcMsg rpcMsgLocalCmd = {0};
      TAOS_CHECK_RETURN(syncBuildLocalCmd(&rpcMsgLocalCmd, ths->vgId));

      SyncLocalCmd* pSyncMsg = rpcMsgLocalCmd.pCont;
      pSyncMsg->cmd =
          (ths->state == TAOS_SYNC_STATE_LEARNER) ? SYNC_LOCAL_CMD_LEARNER_CMT : SYNC_LOCAL_CMD_FOLLOWER_CMT;
      pSyncMsg->commitIndex = pMsg->commitIndex;
      pSyncMsg->currentTerm = pMsg->term;

      if (ths->syncEqMsg != NULL && ths->msgcb != NULL) {
        int32_t code = ths->syncEqMsg(ths->msgcb, &rpcMsgLocalCmd);
        if (code != 0) {
          sError("vgId:%d, failed to enqueue commit msg from heartbeat since %s, code:%d", ths->vgId, terrstr(), code);
          rpcFreeCont(rpcMsgLocalCmd.pCont);
        } else {
          sTrace("vgId:%d, enqueue commit msg from heartbeat, commit-index:%" PRId64 ", term:%" PRId64, ths->vgId,
                 pMsg->commitIndex, pMsg->term);
        }
      }
    }
  }

  if (pMsg->term >= currentTerm &&
      (ths->state == TAOS_SYNC_STATE_LEADER || ths->state == TAOS_SYNC_STATE_ASSIGNED_LEADER)) {
    SRpcMsg rpcMsgLocalCmd = {0};
    TAOS_CHECK_RETURN(syncBuildLocalCmd(&rpcMsgLocalCmd, ths->vgId));

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
        sTrace("vgId:%d, sync enqueue step-down msg, new-term:%" PRId64, ths->vgId, pMsg->term);
      }
    }
  }

  // reply
  TAOS_CHECK_RETURN(syncNodeSendMsgById(&pMsgReply->destId, ths, &rpcMsg));

  if (resetElect) syncNodeResetElectTimer(ths);
  return 0;
}

int32_t syncNodeOnHeartbeatReply(SSyncNode* ths, const SRpcMsg* pRpcMsg) {
  int32_t         code = 0;
  const STraceId* trace = &pRpcMsg->info.traceId;
  char            tbuf[40] = {0};
  TRACE_TO_STR(trace, tbuf);

  SyncHeartbeatReply* pMsg = pRpcMsg->pCont;
  SSyncLogReplMgr*    pMgr = syncNodeGetLogReplMgr(ths, &pMsg->srcId);
  if (pMgr == NULL) {
    code = TSDB_CODE_SYN_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    sError("vgId:%d, failed to get log repl mgr for the peer at addr 0x016%" PRIx64 "", ths->vgId, pMsg->srcId.addr);
    TAOS_RETURN(code);
  }

  int64_t tsMs = taosGetTimestampMs();
  syncLogRecvHeartbeatReply(ths, pMsg, tsMs - pMsg->timeStamp, tbuf);

  syncIndexMgrSetRecvTime(ths->pMatchIndex, &pMsg->srcId, tsMs);

  return syncLogReplProcessHeartbeatReply(pMgr, ths, pMsg);
}

#ifdef BUILD_NO_CALL
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
#endif

int32_t syncNodeOnLocalCmd(SSyncNode* ths, const SRpcMsg* pRpcMsg) {
  SyncLocalCmd* pMsg = pRpcMsg->pCont;
  syncLogRecvLocalCmd(ths, pMsg, "");

  if (pMsg->cmd == SYNC_LOCAL_CMD_STEP_DOWN) {
    syncNodeStepDown(ths, pMsg->currentTerm);

  } else if (pMsg->cmd == SYNC_LOCAL_CMD_FOLLOWER_CMT || pMsg->cmd == SYNC_LOCAL_CMD_LEARNER_CMT) {
    if (syncLogBufferIsEmpty(ths->pLogBuf)) {
      sError("vgId:%d, sync log buffer is empty.", ths->vgId);
      return 0;
    }
    SyncTerm matchTerm = syncLogBufferGetLastMatchTerm(ths->pLogBuf);
    if (matchTerm < 0) {
      return TSDB_CODE_SYN_INTERNAL_ERROR;
    }
    if (pMsg->currentTerm == matchTerm) {
      SyncIndex returnIndex = syncNodeUpdateCommitIndex(ths, pMsg->commitIndex);
      sTrace("vgId:%d, update commit return index %" PRId64 "", ths->vgId, returnIndex);
    }
    if (ths->fsmState != SYNC_FSM_STATE_INCOMPLETE && syncLogBufferCommit(ths->pLogBuf, ths, ths->commitIndex) < 0) {
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
    return TSDB_CODE_SYN_INTERNAL_ERROR;
  }

  // 1->2, config change is add in write thread, and will continue in sync thread
  // need save message for it
  if (pMsg->msgType == TDMT_SYNC_CONFIG_CHANGE) {
    SRespStub stub = {.createTime = taosGetTimestampMs(), .rpcMsg = *pMsg};
    uint64_t  seqNum = syncRespMgrAdd(ths->pSyncRespMgr, &stub);
    pEntry->seqNum = seqNum;
  }

  if (ths->state == TAOS_SYNC_STATE_LEADER || ths->state == TAOS_SYNC_STATE_ASSIGNED_LEADER) {
    if (pRetIndex) {
      (*pRetIndex) = index;
    }

    if (pEntry->originalRpcType == TDMT_SYNC_CONFIG_CHANGE) {
      int32_t code = syncNodeCheckChangeConfig(ths, pEntry);
      if (code < 0) {
        sError("vgId:%d, failed to check change config since %s.", ths->vgId, terrstr());
        syncEntryDestroy(pEntry);
        pEntry = NULL;
        TAOS_RETURN(code);
      }

      if (code > 0) {
        SRpcMsg rsp = {.code = pMsg->code, .info = pMsg->info};
        if ((code = syncRespMgrGetAndDel(ths->pSyncRespMgr, pEntry->seqNum, &rsp.info)) != 0) {
          syncEntryDestroy(pEntry);
          pEntry = NULL;
          TAOS_RETURN(code);
        }
        if (rsp.info.handle != NULL) {
          tmsgSendRsp(&rsp);
        }
        syncEntryDestroy(pEntry);
        pEntry = NULL;
        TAOS_RETURN(code);
      }
    }

    code = syncNodeAppend(ths, pEntry);
    return code;
  } else {
    syncEntryDestroy(pEntry);
    pEntry = NULL;
    return TSDB_CODE_SYN_INTERNAL_ERROR;
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
    case TAOS_SYNC_STATE_LEARNER:
      return "learner";
    case TAOS_SYNC_STATE_ASSIGNED_LEADER:
      return "assigned leader";
    default:
      return "unknown";
  }
}

int32_t syncNodeUpdateNewConfigIndex(SSyncNode* ths, SSyncCfg* pNewCfg) {
  for (int32_t i = 0; i < pNewCfg->totalReplicaNum; ++i) {
    SRaftId raftId = {
        .addr = SYNC_ADDR(&pNewCfg->nodeInfo[i]),
        .vgId = ths->vgId,
    };

    if (syncUtilSameId(&(ths->myRaftId), &raftId)) {
      pNewCfg->myIndex = i;
      return 0;
    }
  }

  return TSDB_CODE_SYN_INTERNAL_ERROR;
}

bool syncNodeIsOptimizedOneReplica(SSyncNode* ths, SRpcMsg* pMsg) {
  return (ths->replicaNum == 1 && syncUtilUserCommit(pMsg->msgType) && ths->vgId != 1);
}

bool syncNodeInRaftGroup(SSyncNode* ths, SRaftId* pRaftId) {
  for (int32_t i = 0; i < ths->totalReplicaNum; ++i) {
    if (syncUtilSameId(&((ths->replicasId)[i]), pRaftId)) {
      return true;
    }
  }
  return false;
}

SSyncSnapshotSender* syncNodeGetSnapshotSender(SSyncNode* ths, SRaftId* pDestId) {
  SSyncSnapshotSender* pSender = NULL;
  for (int32_t i = 0; i < ths->totalReplicaNum; ++i) {
    if (syncUtilSameId(pDestId, &((ths->replicasId)[i]))) {
      pSender = (ths->senders)[i];
    }
  }
  return pSender;
}

SSyncTimer* syncNodeGetHbTimer(SSyncNode* ths, SRaftId* pDestId) {
  SSyncTimer* pTimer = NULL;
  for (int32_t i = 0; i < ths->totalReplicaNum; ++i) {
    if (syncUtilSameId(pDestId, &((ths->replicasId)[i]))) {
      pTimer = &((ths->peerHeartbeatTimerArr)[i]);
    }
  }
  return pTimer;
}

SPeerState* syncNodeGetPeerState(SSyncNode* ths, const SRaftId* pDestId) {
  SPeerState* pState = NULL;
  for (int32_t i = 0; i < ths->totalReplicaNum; ++i) {
    if (syncUtilSameId(pDestId, &((ths->replicasId)[i]))) {
      pState = &((ths->peerStates)[i]);
    }
  }
  return pState;
}

#ifdef BUILD_NO_CALL
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
#endif
