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

#include "syncCommit.h"
#include "syncIndexMgr.h"
#include "syncInt.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncUtil.h"

// \* Leader i advances its commitIndex.
// \* This is done as a separate step from handling AppendEntries responses,
// \* in part to minimize atomic regions, and in part so that leaders of
// \* single-server clusters are able to mark entries committed.
// AdvanceCommitIndex(i) ==
//     /\ state[i] = Leader
//     /\ LET \* The set of servers that agree up through index.
//            Agree(index) == {i} \cup {k \in Server :
//                                          matchIndex[i][k] >= index}
//            \* The maximum indexes for which a quorum agrees
//            agreeIndexes == {index \in 1..Len(log[i]) :
//                                 Agree(index) \in Quorum}
//            \* New value for commitIndex'[i]
//            newCommitIndex ==
//               IF /\ agreeIndexes /= {}
//                  /\ log[i][Max(agreeIndexes)].term = currentTerm[i]
//               THEN
//                   Max(agreeIndexes)
//               ELSE
//                   commitIndex[i]
//        IN commitIndex' = [commitIndex EXCEPT ![i] = newCommitIndex]
//     /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, log>>
//
void syncMaybeAdvanceCommitIndex(SSyncNode* pSyncNode) {
  syncIndexMgrLog2("==syncNodeMaybeAdvanceCommitIndex== pNextIndex", pSyncNode->pNextIndex);
  syncIndexMgrLog2("==syncNodeMaybeAdvanceCommitIndex== pMatchIndex", pSyncNode->pMatchIndex);

  // update commit index
  SyncIndex newCommitIndex = pSyncNode->commitIndex;
  for (SyncIndex index = pSyncNode->pLogStore->getLastIndex(pSyncNode->pLogStore); index > pSyncNode->commitIndex;
       --index) {
    bool agree = syncAgree(pSyncNode, index);
    sTrace("syncMaybeAdvanceCommitIndex syncAgree:%d, index:%ld, pSyncNode->commitIndex:%ld", agree, index,
           pSyncNode->commitIndex);
    if (agree) {
      // term
      SSyncRaftEntry* pEntry = pSyncNode->pLogStore->getEntry(pSyncNode->pLogStore, index);
      assert(pEntry != NULL);

      // cannot commit, even if quorum agree. need check term!
      if (pEntry->term == pSyncNode->pRaftStore->currentTerm) {
        // update commit index
        newCommitIndex = index;
        break;
      }
    }
  }

  if (newCommitIndex > pSyncNode->commitIndex) {
    SyncIndex beginIndex = pSyncNode->commitIndex + 1;
    SyncIndex endIndex = newCommitIndex;

    sTrace("syncMaybeAdvanceCommitIndex sync commit %ld", newCommitIndex);

    // update commit index
    pSyncNode->commitIndex = newCommitIndex;

    // call back Wal
    pSyncNode->pLogStore->updateCommitIndex(pSyncNode->pLogStore, pSyncNode->commitIndex);

    // execute fsm
    if (pSyncNode->pFsm != NULL) {
      for (SyncIndex i = beginIndex; i <= endIndex; ++i) {
        if (i != SYNC_INDEX_INVALID) {
          SSyncRaftEntry* pEntry = pSyncNode->pLogStore->getEntry(pSyncNode->pLogStore, i);
          assert(pEntry != NULL);

          SRpcMsg rpcMsg;
          syncEntry2OriginalRpc(pEntry, &rpcMsg);

          if (pSyncNode->pFsm->FpCommitCb != NULL) {
            pSyncNode->pFsm->FpCommitCb(pSyncNode->pFsm, &rpcMsg, pEntry->index, pEntry->isWeak, 0);
          }

          rpcFreeCont(rpcMsg.pCont);
          syncEntryDestory(pEntry);
        }
      }
    }
  }
}

bool syncAgreeIndex(SSyncNode* pSyncNode, SRaftId* pRaftId, SyncIndex index) {
  // I am leader, I agree
  if (syncUtilSameId(pRaftId, &(pSyncNode->myRaftId)) && pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    return true;
  }

  // follower agree
  SyncIndex matchIndex = syncIndexMgrGetIndex(pSyncNode->pMatchIndex, pRaftId);
  if (matchIndex >= index) {
    return true;
  }

  // not agree
  return false;
}

bool syncAgree(SSyncNode* pSyncNode, SyncIndex index) {
  int agreeCount = 0;
  for (int i = 0; i < pSyncNode->replicaNum; ++i) {
    if (syncAgreeIndex(pSyncNode, &(pSyncNode->replicasId[i]), index)) {
      ++agreeCount;
    }
    if (agreeCount >= pSyncNode->quorum) {
      return true;
    }
  }
  return false;
}