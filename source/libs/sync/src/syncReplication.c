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
#include "syncRaftEntry.h"
#include "syncRaftLog.h"
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
//
int32_t syncNodeAppendEntriesPeers(SSyncNode* pSyncNode) {
  assert(pSyncNode->state == TAOS_SYNC_STATE_LEADER);

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
      assert(pPreEntry != NULL);

      preLogTerm = pPreEntry->term;
      syncEntryDestory(pPreEntry);
    }

    // batch optimized
    // SyncIndex lastIndex = syncUtilMinIndex(pSyncNode->pLogStore->getLastIndex(pSyncNode->pLogStore), nextIndex);

    SyncAppendEntries* pMsg = NULL;
    SSyncRaftEntry*    pEntry = logStoreGetEntry(pSyncNode->pLogStore, nextIndex);
    if (pEntry != NULL) {
      pMsg = syncAppendEntriesBuild(pEntry->bytes);
      assert(pMsg != NULL);

      // add pEntry into msg
      uint32_t len;
      char*    serialized = syncEntrySerialize(pEntry, &len);
      assert(len == pEntry->bytes);
      memcpy(pMsg->data, serialized, len);

      taosMemoryFree(serialized);
      syncEntryDestory(pEntry);

    } else {
      // maybe overflow, send empty record
      pMsg = syncAppendEntriesBuild(0);
      assert(pMsg != NULL);
    }

    assert(pMsg != NULL);
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

int32_t syncNodeReplicate(SSyncNode* pSyncNode) {
  // start replicate
  int32_t ret = syncNodeAppendEntriesPeers(pSyncNode);
  return ret;
}

int32_t syncNodeAppendEntries(SSyncNode* pSyncNode, const SRaftId* destRaftId, const SyncAppendEntries* pMsg) {
  sTrace("syncNodeAppendEntries pSyncNode:%p ", pSyncNode);
  int32_t ret = 0;

  SRpcMsg rpcMsg;
  syncAppendEntries2RpcMsg(pMsg, &rpcMsg);
  syncNodeSendMsgById(destRaftId, pSyncNode, &rpcMsg);
  return ret;
}