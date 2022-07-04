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
//
int32_t syncNodeAppendEntriesPeers(SSyncNode* pSyncNode) {
  ASSERT(pSyncNode->state == TAOS_SYNC_STATE_LEADER);

  syncIndexMgrLog2("==syncNodeAppendEntriesPeers== pNextIndex", pSyncNode->pNextIndex);
  syncIndexMgrLog2("==syncNodeAppendEntriesPeers== pMatchIndex", pSyncNode->pMatchIndex);
  logStoreSimpleLog2("==syncNodeAppendEntriesPeers==", pSyncNode->pLogStore);

  int32_t ret = 0;
  for (int i = 0; i < pSyncNode->peersNum; ++i) {
    SRaftId* pDestId = &(pSyncNode->peersId[i]);

    // set prevLogIndex
    SyncIndex nextIndex = syncIndexMgrGetIndex(pSyncNode->pNextIndex, pDestId);

    SyncIndex preLogIndex = nextIndex - 1;

    // set preLogTerm
    SyncTerm preLogTerm = 0;
    if (preLogIndex >= SYNC_INDEX_BEGIN) {
      SSyncRaftEntry* pPreEntry = pSyncNode->pLogStore->getEntry(pSyncNode->pLogStore, preLogIndex);
      ASSERT(pPreEntry != NULL);

      preLogTerm = pPreEntry->term;
      syncEntryDestory(pPreEntry);
    }

    // batch optimized
    // SyncIndex lastIndex = syncUtilMinIndex(pSyncNode->pLogStore->getLastIndex(pSyncNode->pLogStore), nextIndex);

    SyncAppendEntries* pMsg = NULL;
    SSyncRaftEntry*    pEntry = pSyncNode->pLogStore->getEntry(pSyncNode->pLogStore, nextIndex);
    if (pEntry != NULL) {
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
      // maybe overflow, send empty record
      pMsg = syncAppendEntriesBuild(0, pSyncNode->vgId);
      ASSERT(pMsg != NULL);
    }

    ASSERT(pMsg != NULL);
    pMsg->srcId = pSyncNode->myRaftId;
    pMsg->destId = *pDestId;
    pMsg->term = pSyncNode->pRaftStore->currentTerm;
    pMsg->prevLogIndex = preLogIndex;
    pMsg->prevLogTerm = preLogTerm;
    pMsg->commitIndex = pSyncNode->commitIndex;

    syncAppendEntriesLog2("==syncNodeAppendEntriesPeers==", pMsg);

    // send AppendEntries
    syncNodeAppendEntries(pSyncNode, pDestId, pMsg);
    syncAppendEntriesDestroy(pMsg);
  }

  return ret;
}

int32_t syncNodeAppendEntriesPeersSnapshot2(SSyncNode* pSyncNode) {
  if (pSyncNode->state != TAOS_SYNC_STATE_LEADER) {
    return -1;
  }

  int32_t ret = 0;
  for (int i = 0; i < pSyncNode->peersNum; ++i) {
    SRaftId* pDestId = &(pSyncNode->peersId[i]);

    // next index
    SyncIndex nextIndex = syncIndexMgrGetIndex(pSyncNode->pNextIndex, pDestId);

    // pre index, pre term
    SyncIndex preLogIndex = syncNodeGetPreIndex(pSyncNode, nextIndex);
    SyncTerm  preLogTerm = syncNodeGetPreTerm(pSyncNode, nextIndex);
    if (preLogTerm == SYNC_TERM_INVALID) {
      SSyncSnapshotSender* pSender = syncNodeGetSnapshotSender(pSyncNode, pDestId);
      ASSERT(pSender != NULL);
      ASSERT(!snapshotSenderIsStart(pSender));

      SyncIndex newNextIndex = syncNodeGetLastIndex(pSyncNode) + 1;
      syncIndexMgrSetIndex(pSyncNode->pNextIndex, pDestId, newNextIndex);
      syncIndexMgrSetIndex(pSyncNode->pMatchIndex, pDestId, SYNC_INDEX_INVALID);
      sError("vgId:%d sync get pre term error, nextIndex:%ld, update next-index:%ld, match-index:%d, raftid:%ld",
             pSyncNode->vgId, nextIndex, newNextIndex, SYNC_INDEX_INVALID, pDestId->addr);

      return -1;
    }

    SRpcMsg rpcMsgArr[SYNC_MAX_BATCH_SIZE];
    memset(rpcMsgArr, 0, sizeof(rpcMsgArr));

    int32_t getCount = 0;
    for (int32_t i = 0; i < pSyncNode->batchSize; ++i) {
      SSyncRaftEntry* pEntry;
      int32_t         code = pSyncNode->pLogStore->syncLogGetEntry(pSyncNode->pLogStore, nextIndex, &pEntry);
      if (code == 0) {
        ASSERT(pEntry != NULL);
        // get rpc msg [i] from entry
        syncEntryDestory(pEntry);
        getCount++;
      } else {
        break;
      }
    }

    SyncAppendEntriesBatch* pMsg = syncAppendEntriesBatchBuild(rpcMsgArr, getCount, pSyncNode->vgId);
    ASSERT(pMsg != NULL);

    // prepare msg
    pMsg->srcId = pSyncNode->myRaftId;
    pMsg->destId = *pDestId;
    pMsg->term = pSyncNode->pRaftStore->currentTerm;
    pMsg->prevLogIndex = preLogIndex;
    pMsg->prevLogTerm = preLogTerm;
    pMsg->commitIndex = pSyncNode->commitIndex;
    pMsg->privateTerm = 0;
    pMsg->dataCount = getCount;

    // send msg
    syncNodeAppendEntriesBatch(pSyncNode, pDestId, pMsg);
    syncAppendEntriesBatchDestroy(pMsg);
  }

  return 0;
}

int32_t syncNodeAppendEntriesPeersSnapshot(SSyncNode* pSyncNode) {
  ASSERT(pSyncNode->state == TAOS_SYNC_STATE_LEADER);

  syncIndexMgrLog2("begin append entries peers pNextIndex:", pSyncNode->pNextIndex);
  syncIndexMgrLog2("begin append entries peers pMatchIndex:", pSyncNode->pMatchIndex);
  logStoreSimpleLog2("begin append entries peers LogStore:", pSyncNode->pLogStore);
  if (gRaftDetailLog) {
    SSnapshot snapshot;
    pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
    sTrace("begin append entries peers, snapshot.lastApplyIndex:%ld, snapshot.lastApplyTerm:%lu",
           snapshot.lastApplyIndex, snapshot.lastApplyTerm);
  }

  int32_t ret = 0;
  for (int i = 0; i < pSyncNode->peersNum; ++i) {
    SRaftId* pDestId = &(pSyncNode->peersId[i]);

    // next index
    SyncIndex nextIndex = syncIndexMgrGetIndex(pSyncNode->pNextIndex, pDestId);

    // pre index, pre term
    SyncIndex preLogIndex = syncNodeGetPreIndex(pSyncNode, nextIndex);
    SyncTerm  preLogTerm = syncNodeGetPreTerm(pSyncNode, nextIndex);
    if (preLogTerm == SYNC_TERM_INVALID) {
      SyncIndex newNextIndex = syncNodeGetLastIndex(pSyncNode) + 1;
      syncIndexMgrSetIndex(pSyncNode->pNextIndex, pDestId, newNextIndex);
      syncIndexMgrSetIndex(pSyncNode->pMatchIndex, pDestId, SYNC_INDEX_INVALID);
      sError("vgId:%d sync get pre term error, nextIndex:%ld, update next-index:%ld, match-index:%d, raftid:%ld",
             pSyncNode->vgId, nextIndex, newNextIndex, SYNC_INDEX_INVALID, pDestId->addr);

      return -1;
    }

    // batch optimized
    // SyncIndex lastIndex = syncUtilMinIndex(pSyncNode->pLogStore->getLastIndex(pSyncNode->pLogStore), nextIndex);

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
        syncNodeLog3("", pSyncNode);
        ASSERT(0);
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
    syncNodeAppendEntries(pSyncNode, pDestId, pMsg);
    syncAppendEntriesDestroy(pMsg);
  }

  return ret;
}

int32_t syncNodeReplicate(SSyncNode* pSyncNode) {
  // start replicate
  int32_t ret = 0;

  if (pSyncNode->pRaftCfg->snapshotEnable) {
    ret = syncNodeAppendEntriesPeersSnapshot(pSyncNode);
  } else {
    ret = syncNodeAppendEntriesPeers(pSyncNode);
  }
  return ret;
}

int32_t syncNodeAppendEntries(SSyncNode* pSyncNode, const SRaftId* destRaftId, const SyncAppendEntries* pMsg) {
  int32_t ret = 0;

  do {
    char     host[128];
    uint16_t port;
    syncUtilU642Addr(destRaftId->addr, host, sizeof(host), &port);
    sDebug(
        "vgId:%d, send sync-append-entries to %s:%d, {term:%lu, pre-index:%ld, pre-term:%lu, pterm:%lu, commit:%ld, "
        "datalen:%d}",
        pSyncNode->vgId, host, port, pMsg->term, pMsg->prevLogIndex, pMsg->prevLogTerm, pMsg->privateTerm,
        pMsg->commitIndex, pMsg->dataLen);
  } while (0);

  SRpcMsg rpcMsg;
  syncAppendEntries2RpcMsg(pMsg, &rpcMsg);
  syncNodeSendMsgById(destRaftId, pSyncNode, &rpcMsg);
  return ret;
}

int32_t syncNodeAppendEntriesBatch(SSyncNode* pSyncNode, const SRaftId* destRaftId,
                                   const SyncAppendEntriesBatch* pMsg) {
  do {
    char     host[128];
    uint16_t port;
    syncUtilU642Addr(destRaftId->addr, host, sizeof(host), &port);
    sDebug(
        "vgId:%d, send sync-append-entries-batch to %s:%d, {term:%lu, pre-index:%ld, pre-term:%lu, pterm:%lu, "
        "commit:%ld, "
        "datalen:%d, dataCount:%d}",
        pSyncNode->vgId, host, port, pMsg->term, pMsg->prevLogIndex, pMsg->prevLogTerm, pMsg->privateTerm,
        pMsg->commitIndex, pMsg->dataLen, pMsg->dataCount);
  } while (0);

  SRpcMsg rpcMsg;
  syncAppendEntriesBatch2RpcMsg(pMsg, &rpcMsg);
  syncNodeSendMsgById(destRaftId, pSyncNode, &rpcMsg);
  return 0;
}