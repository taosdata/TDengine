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
#include "syncRaftCfg.h"
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
        sTrace("syncMaybeAdvanceCommitIndex maybe to update, newCommitIndex:%ld commit, pSyncNode->commitIndex:%ld",
               newCommitIndex, pSyncNode->commitIndex);

        syncEntryDestory(pEntry);
        break;
      } else {
        sTrace(
            "syncMaybeAdvanceCommitIndex can not commit due to term not equal, pEntry->term:%lu, "
            "pSyncNode->pRaftStore->currentTerm:%lu",
            pEntry->term, pSyncNode->pRaftStore->currentTerm);
      }

      syncEntryDestory(pEntry);
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

          if (pSyncNode->pFsm->FpCommitCb != NULL && syncUtilUserCommit(pEntry->originalRpcType)) {
            SFsmCbMeta cbMeta;
            cbMeta.index = pEntry->index;
            cbMeta.isWeak = pEntry->isWeak;
            cbMeta.code = 0;
            cbMeta.state = pSyncNode->state;
            cbMeta.seqNum = pEntry->seqNum;
            cbMeta.term = pEntry->term;
            cbMeta.currentTerm = pSyncNode->pRaftStore->currentTerm;

            bool needExecute = true;
            if (pSyncNode->pSnapshot != NULL && cbMeta.index <= pSyncNode->pSnapshot->lastApplyIndex) {
              needExecute = false;
            }

            if (needExecute) {
              pSyncNode->pFsm->FpCommitCb(pSyncNode->pFsm, &rpcMsg, cbMeta);
            }
          }

          // config change
          if (pEntry->originalRpcType == TDMT_VND_SYNC_CONFIG_CHANGE) {
            SSyncCfg newSyncCfg;
            int32_t  ret = syncCfgFromStr(rpcMsg.pCont, &newSyncCfg);
            ASSERT(ret == 0);

            syncNodeUpdateConfig(pSyncNode, &newSyncCfg);
            if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
              syncNodeBecomeLeader(pSyncNode);
            } else {
              syncNodeBecomeFollower(pSyncNode);
            }

            // maybe newSyncCfg.myIndex is updated in syncNodeUpdateConfig
            if (pSyncNode->pFsm->FpReConfigCb != NULL) {
              SReConfigCbMeta cbMeta = {0};
              cbMeta.code = 0;
              cbMeta.currentTerm = pSyncNode->pRaftStore->currentTerm;
              cbMeta.index = pEntry->index;
              cbMeta.term = pEntry->term;
              pSyncNode->pFsm->FpReConfigCb(pSyncNode->pFsm, newSyncCfg, cbMeta);
            }
          }

          // restore finish
          if (pEntry->index == pSyncNode->pLogStore->getLastIndex(pSyncNode->pLogStore)) {
            if (pSyncNode->restoreFinish == false) {
              if (pSyncNode->pFsm->FpRestoreFinishCb != NULL) {
                pSyncNode->pFsm->FpRestoreFinishCb(pSyncNode->pFsm);
              }
              pSyncNode->restoreFinish = true;
              sInfo("==syncMaybeAdvanceCommitIndex== restoreFinish set true %p vgId:%d", pSyncNode, pSyncNode->vgId);

              /*
              tsem_post(&pSyncNode->restoreSem);
              sInfo("==syncMaybeAdvanceCommitIndex== RestoreFinish tsem_post %p", pSyncNode);
              */
            }
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
