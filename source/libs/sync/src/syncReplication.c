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
#include "syncReplication.h"
#include "syncIndexMgr.h"
#include "syncPipeline.h"
#include "syncRaftEntry.h"
#include "syncRaftStore.h"
#include "syncUtil.h"

// TLA+ Spec
// AppendEntries(i, j) ==
//    /\ i /= j
//    /\ state[i] = Leader
//    /\ LET prevLogIndex == nextIndex[i][j] - 1
//           prevLogTerm == IF prevLogIndex > 0 THEN
//                              log[i][prevLogIndex].term
//                          ELSE
//                              0
//           \* Send up to 1 entry, constrained by the end of the log.
//           lastEntry == Min({Len(log[i]), nextIndex[i][j]})
//           entries == SubSeq(log[i], nextIndex[i][j], lastEntry)
//       IN Send([mtype          |-> AppendEntriesRequest,
//                mterm          |-> currentTerm[i],
//                mprevLogIndex  |-> prevLogIndex,
//                mprevLogTerm   |-> prevLogTerm,
//                mentries       |-> entries,
//                \* mlog is used as a history variable for the proof.
//                \* It would not exist in a real implementation.
//                mlog           |-> log[i],
//                mcommitIndex   |-> Min({commitIndex[i], lastEntry}),
//                msource        |-> i,
//                mdest          |-> j])
//    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars>>

int32_t syncNodeMaybeSendAppendEntries(SSyncNode* pSyncNode, const SRaftId* destRaftId, SRpcMsg* pRpcMsg);

int32_t syncNodeReplicateOne(SSyncNode* pSyncNode, SRaftId* pDestId, bool snapshot) {
  ASSERT(false && "deprecated");
  // next index
  SyncIndex nextIndex = syncIndexMgrGetIndex(pSyncNode->pNextIndex, pDestId);

  if (snapshot) {
    // maybe start snapshot
    SyncIndex logStartIndex = pSyncNode->pLogStore->syncLogBeginIndex(pSyncNode->pLogStore);
    SyncIndex logEndIndex = pSyncNode->pLogStore->syncLogEndIndex(pSyncNode->pLogStore);
    if (nextIndex < logStartIndex || nextIndex - 1 > logEndIndex) {
      sNTrace(pSyncNode, "maybe start snapshot for next-index:%" PRId64 ", start:%" PRId64 ", end:%" PRId64, nextIndex,
              logStartIndex, logEndIndex);
      // start snapshot
      int32_t code = syncNodeStartSnapshot(pSyncNode, pDestId);
    }
  }

  // pre index, pre term
  SyncIndex preLogIndex = syncNodeGetPreIndex(pSyncNode, nextIndex);
  SyncTerm  preLogTerm = syncNodeGetPreTerm(pSyncNode, nextIndex);

  // prepare entry
  SRpcMsg            rpcMsg = {0};
  SyncAppendEntries* pMsg = NULL;

  SSyncRaftEntry* pEntry = NULL;
  SLRUCache*      pCache = pSyncNode->pLogStore->pCache;
  LRUHandle*      h = taosLRUCacheLookup(pCache, &nextIndex, sizeof(nextIndex));
  int32_t         code = 0;
  if (h) {
    pEntry = (SSyncRaftEntry*)taosLRUCacheValue(pCache, h);
    code = 0;

    pSyncNode->pLogStore->cacheHit++;
    sNTrace(pSyncNode, "hit cache index:%" PRId64 ", bytes:%u, %p", nextIndex, pEntry->bytes, pEntry);

  } else {
    pSyncNode->pLogStore->cacheMiss++;
    sNTrace(pSyncNode, "miss cache index:%" PRId64, nextIndex);

    code = pSyncNode->pLogStore->syncLogGetEntry(pSyncNode->pLogStore, nextIndex, &pEntry);
  }

  if (code == 0) {
    ASSERT(pEntry != NULL);

    code = syncBuildAppendEntries(&rpcMsg, (int32_t)(pEntry->bytes), pSyncNode->vgId);
    ASSERT(code == 0);

    pMsg = rpcMsg.pCont;
    memcpy(pMsg->data, pEntry, pEntry->bytes);
  } else {
    if (terrno == TSDB_CODE_WAL_LOG_NOT_EXIST) {
      // no entry in log
      code = syncBuildAppendEntries(&rpcMsg, 0, pSyncNode->vgId);
      ASSERT(code == 0);

      pMsg = rpcMsg.pCont;
    } else {
      sNError(pSyncNode, "replicate to dnode:%d error, next-index:%" PRId64, DID(pDestId), nextIndex);
      return -1;
    }
  }

  if (h) {
    taosLRUCacheRelease(pCache, h, false);
  } else {
    syncEntryDestroy(pEntry);
  }

  // prepare msg
  ASSERT(pMsg != NULL);
  pMsg->srcId = pSyncNode->myRaftId;
  pMsg->destId = *pDestId;
  pMsg->term = pSyncNode->raftStore.currentTerm;
  pMsg->prevLogIndex = preLogIndex;
  pMsg->prevLogTerm = preLogTerm;
  pMsg->commitIndex = pSyncNode->commitIndex;
  pMsg->privateTerm = 0;
  // pMsg->privateTerm = syncIndexMgrGetTerm(pSyncNode->pNextIndex, pDestId);

  // send msg
  syncNodeMaybeSendAppendEntries(pSyncNode, pDestId, &rpcMsg);
  return 0;
}

int32_t syncNodeReplicate(SSyncNode* pNode) {
  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  taosThreadMutexLock(&pBuf->mutex);
  int32_t ret = syncNodeReplicateWithoutLock(pNode);
  taosThreadMutexUnlock(&pBuf->mutex);
  return ret;
}

int32_t syncNodeReplicateWithoutLock(SSyncNode* pNode) {
  if (pNode->state != TAOS_SYNC_STATE_LEADER || pNode->replicaNum == 1) {
    return -1;
  }
  for (int32_t i = 0; i < pNode->replicaNum; i++) {
    if (syncUtilSameId(&pNode->replicasId[i], &pNode->myRaftId)) {
      continue;
    }
    SSyncLogReplMgr* pMgr = pNode->logReplMgrs[i];
    (void)syncLogReplMgrReplicateOnce(pMgr, pNode);
  }
  return 0;
}

int32_t syncNodeReplicateOld(SSyncNode* pSyncNode) {
  if (pSyncNode->state != TAOS_SYNC_STATE_LEADER) {
    return -1;
  }

  sNTrace(pSyncNode, "do replicate");

  int32_t ret = 0;
  for (int i = 0; i < pSyncNode->peersNum; ++i) {
    SRaftId* pDestId = &(pSyncNode->peersId[i]);
    ret = syncNodeReplicateOne(pSyncNode, pDestId, true);
    if (ret != 0) {
      sError("vgId:%d, do append entries error for dnode:%d", pSyncNode->vgId, DID(pDestId));
    }
  }

  return 0;
}

int32_t syncNodeSendAppendEntries(SSyncNode* pSyncNode, const SRaftId* destRaftId, SRpcMsg* pRpcMsg) {
  SyncAppendEntries* pMsg = pRpcMsg->pCont;
  pMsg->destId = *destRaftId;
  syncNodeSendMsgById(destRaftId, pSyncNode, pRpcMsg);
  return 0;
}

int32_t syncNodeSendAppendEntriesOld(SSyncNode* pSyncNode, const SRaftId* destRaftId, SRpcMsg* pRpcMsg) {
  int32_t            ret = 0;
  SyncAppendEntries* pMsg = pRpcMsg->pCont;
  if (pMsg == NULL) {
    sError("vgId:%d, sync-append-entries msg is NULL", pSyncNode->vgId);
    return 0;
  }

  SPeerState* pState = syncNodeGetPeerState(pSyncNode, destRaftId);
  if (pState == NULL) {
    sError("vgId:%d, replica maybe dropped", pSyncNode->vgId);
    return 0;
  }

  // save index, otherwise pMsg will be free by rpc
  SyncIndex saveLastSendIndex = pState->lastSendIndex;
  bool      update = false;
  if (pMsg->dataLen > 0) {
    saveLastSendIndex = pMsg->prevLogIndex + 1;
    update = true;
  }

  syncLogSendAppendEntries(pSyncNode, pMsg, "");
  syncNodeSendMsgById(destRaftId, pSyncNode, pRpcMsg);

  if (update) {
    pState->lastSendIndex = saveLastSendIndex;
    pState->lastSendTime = taosGetTimestampMs();
  }

  return ret;
}

int32_t syncNodeMaybeSendAppendEntries(SSyncNode* pSyncNode, const SRaftId* destRaftId, SRpcMsg* pRpcMsg) {
  int32_t            ret = 0;
  SyncAppendEntries* pMsg = pRpcMsg->pCont;

  if (syncNodeNeedSendAppendEntries(pSyncNode, destRaftId, pMsg)) {
    ret = syncNodeSendAppendEntries(pSyncNode, destRaftId, pRpcMsg);
  } else {
    sNTrace(pSyncNode, "do not repcate to dnode:%d for index:%" PRId64, DID(destRaftId), pMsg->prevLogIndex + 1);
    rpcFreeCont(pRpcMsg->pCont);
  }

  return ret;
}

int32_t syncNodeSendHeartbeat(SSyncNode* pSyncNode, const SRaftId* destId, SRpcMsg* pMsg) {
  return syncNodeSendMsgById(destId, pSyncNode, pMsg);
}

int32_t syncNodeHeartbeatPeers(SSyncNode* pSyncNode) {
  int64_t ts = taosGetTimestampMs();
  for (int32_t i = 0; i < pSyncNode->peersNum; ++i) {
    SRpcMsg rpcMsg = {0};
    if (syncBuildHeartbeat(&rpcMsg, pSyncNode->vgId) != 0) {
      sError("vgId:%d, build sync-heartbeat error", pSyncNode->vgId);
      continue;
    }

    SyncHeartbeat* pSyncMsg = rpcMsg.pCont;
    pSyncMsg->srcId = pSyncNode->myRaftId;
    pSyncMsg->destId = pSyncNode->peersId[i];
    pSyncMsg->term = pSyncNode->raftStore.currentTerm;
    pSyncMsg->commitIndex = pSyncNode->commitIndex;
    pSyncMsg->minMatchIndex = syncMinMatchIndex(pSyncNode);
    pSyncMsg->privateTerm = 0;
    pSyncMsg->timeStamp = ts;

    // send msg
    syncLogSendHeartbeat(pSyncNode, pSyncMsg, true, 0, 0);
    syncNodeSendHeartbeat(pSyncNode, &pSyncMsg->destId, &rpcMsg);
  }

  return 0;
}
