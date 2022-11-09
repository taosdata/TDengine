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

#include "syncReplication.h"
#include "syncIndexMgr.h"
#include "syncMessage.h"
#include "syncRaftCfg.h"
#include "syncRaftEntry.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncSnapshot.h"
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

int32_t syncNodeReplicateOne(SSyncNode* pSyncNode, SRaftId* pDestId) {
  // next index
  SyncIndex nextIndex = syncIndexMgrGetIndex(pSyncNode->pNextIndex, pDestId);

  // maybe start snapshot
  SyncIndex logStartIndex = pSyncNode->pLogStore->syncLogBeginIndex(pSyncNode->pLogStore);
  SyncIndex logEndIndex = pSyncNode->pLogStore->syncLogEndIndex(pSyncNode->pLogStore);
  if (nextIndex < logStartIndex || nextIndex - 1 > logEndIndex) {
    sNTrace(pSyncNode, "maybe start snapshot for next-index:%" PRId64 ", start:%" PRId64 ", end:%" PRId64, nextIndex,
            logStartIndex, logEndIndex);
    // start snapshot
    // int32_t code = syncNodeStartSnapshot(pSyncNode, pDestId);
    return 0;
  }

  // pre index, pre term
  SyncIndex preLogIndex = syncNodeGetPreIndex(pSyncNode, nextIndex);
  SyncTerm  preLogTerm = syncNodeGetPreTerm(pSyncNode, nextIndex);

  // prepare entry
  SyncAppendEntries* pMsg = NULL;

  SSyncRaftEntry* pEntry;
  int32_t         code = pSyncNode->pLogStore->syncLogGetEntry(pSyncNode->pLogStore, nextIndex, &pEntry);

  if (code == 0) {
    ASSERT(pEntry != NULL);

    pMsg = syncAppendEntriesBuild(pEntry->bytes, pSyncNode->vgId);
    ASSERT(pMsg != NULL);

    // add pEntry into msg
    uint32_t len;
    char*    serialized = syncEntrySerialize(pEntry, &len);
    ASSERT(len == pEntry->bytes);
    memcpy(pMsg->data, serialized, len);

    taosMemoryFree(serialized);
    syncEntryDestory(pEntry);

  } else {
    if (terrno == TSDB_CODE_WAL_LOG_NOT_EXIST) {
      // no entry in log
      pMsg = syncAppendEntriesBuild(0, pSyncNode->vgId);
      ASSERT(pMsg != NULL);

    } else {
      do {
        char     host[64];
        uint16_t port;
        syncUtilU642Addr(pDestId->addr, host, sizeof(host), &port);
        sNError(pSyncNode, "replicate to %s:%d error, next-index:%" PRId64, host, port, nextIndex);
      } while (0);

      syncAppendEntriesDestroy(pMsg);
      return -1;
    }
  }

  // prepare msg
  ASSERT(pMsg != NULL);
  pMsg->srcId = pSyncNode->myRaftId;
  pMsg->destId = *pDestId;
  pMsg->term = pSyncNode->pRaftStore->currentTerm;
  pMsg->prevLogIndex = preLogIndex;
  pMsg->prevLogTerm = preLogTerm;
  pMsg->commitIndex = pSyncNode->commitIndex;
  pMsg->privateTerm = 0;
  // pMsg->privateTerm = syncIndexMgrGetTerm(pSyncNode->pNextIndex, pDestId);

  // send msg
  syncNodeMaybeSendAppendEntries(pSyncNode, pDestId, pMsg);
  syncAppendEntriesDestroy(pMsg);

  return 0;
}

int32_t syncNodeReplicate(SSyncNode* pSyncNode) {
  if (pSyncNode->state != TAOS_SYNC_STATE_LEADER) {
    return -1;
  }

  sNTrace(pSyncNode, "do replicate");

  int32_t ret = 0;
  for (int i = 0; i < pSyncNode->peersNum; ++i) {
    SRaftId* pDestId = &(pSyncNode->peersId[i]);
    ret = syncNodeReplicateOne(pSyncNode, pDestId);
    if (ret != 0) {
      char    host[64];
      int16_t port;
      syncUtilU642Addr(pDestId->addr, host, sizeof(host), &port);
      sError("vgId:%d, do append entries error for %s:%d", pSyncNode->vgId, host, port);
    }
  }

  return 0;
}

int32_t syncNodeSendAppendEntries(SSyncNode* pSyncNode, const SRaftId* destRaftId, const SyncAppendEntries* pMsg) {
  int32_t ret = 0;
  syncLogSendAppendEntries(pSyncNode, pMsg, "");

  SRpcMsg rpcMsg;
  syncAppendEntries2RpcMsg(pMsg, &rpcMsg);
  syncNodeSendMsgById(destRaftId, pSyncNode, &rpcMsg);

  SPeerState* pState = syncNodeGetPeerState(pSyncNode, destRaftId);
  if (pState == NULL) {
    sError("vgId:%d, replica maybe dropped", pSyncNode->vgId);
    return 0;
  }

  if (pMsg->dataLen > 0) {
    pState->lastSendIndex = pMsg->prevLogIndex + 1;
    pState->lastSendTime = taosGetTimestampMs();
  }

  return ret;
}

int32_t syncNodeMaybeSendAppendEntries(SSyncNode* pSyncNode, const SRaftId* destRaftId, const SyncAppendEntries* pMsg) {
  int32_t ret = 0;
  if (syncNodeNeedSendAppendEntries(pSyncNode, destRaftId, pMsg)) {
    ret = syncNodeSendAppendEntries(pSyncNode, destRaftId, pMsg);

  } else {
    char    logBuf[128];
    char    host[64];
    int16_t port;
    syncUtilU642Addr(destRaftId->addr, host, sizeof(host), &port);
    sNTrace(pSyncNode, "do not repcate to %s:%d for index:%" PRId64, host, port, pMsg->prevLogIndex + 1);
  }

  return ret;
}

int32_t syncNodeAppendEntries(SSyncNode* pSyncNode, const SRaftId* destRaftId, const SyncAppendEntries* pMsg) {
  int32_t ret = 0;
  syncLogSendAppendEntries(pSyncNode, pMsg, "");

  SRpcMsg rpcMsg;
  syncAppendEntries2RpcMsg(pMsg, &rpcMsg);
  syncNodeSendMsgById(destRaftId, pSyncNode, &rpcMsg);
  return ret;
}

int32_t syncNodeSendHeartbeat(SSyncNode* pSyncNode, const SRaftId* destRaftId, const SyncHeartbeat* pMsg) {
  int32_t ret = 0;
  syncLogSendHeartbeat(pSyncNode, pMsg, "");

  SRpcMsg rpcMsg;
  syncHeartbeat2RpcMsg(pMsg, &rpcMsg);
  syncNodeSendMsgById(&(pMsg->destId), pSyncNode, &rpcMsg);
  return ret;
}

int32_t syncNodeHeartbeatPeers(SSyncNode* pSyncNode) {
  for (int32_t i = 0; i < pSyncNode->peersNum; ++i) {
    SyncHeartbeat* pSyncMsg = syncHeartbeatBuild(pSyncNode->vgId);
    pSyncMsg->srcId = pSyncNode->myRaftId;
    pSyncMsg->destId = pSyncNode->peersId[i];
    pSyncMsg->term = pSyncNode->pRaftStore->currentTerm;
    pSyncMsg->commitIndex = pSyncNode->commitIndex;
    pSyncMsg->minMatchIndex = syncMinMatchIndex(pSyncNode);
    pSyncMsg->privateTerm = 0;

    SRpcMsg rpcMsg;
    syncHeartbeat2RpcMsg(pSyncMsg, &rpcMsg);

    // send msg
    syncNodeSendHeartbeat(pSyncNode, &(pSyncMsg->destId), pSyncMsg);

    syncHeartbeatDestroy(pSyncMsg);
  }

  return 0;
}