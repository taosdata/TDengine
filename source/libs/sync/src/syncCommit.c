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
#include "syncCommit.h"
#include "syncIndexMgr.h"
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
void syncOneReplicaAdvance(SSyncNode* pSyncNode) {
  ASSERT(false && "deprecated");
  if (pSyncNode == NULL) {
    sError("pSyncNode is NULL");
    return;
  }

  if (pSyncNode->state != TAOS_SYNC_STATE_LEADER) {
    sNError(pSyncNode, "not leader, can not advance commit index");
    return;
  }

  if (pSyncNode->replicaNum != 1) {
    sNError(pSyncNode, "not one replica, can not advance commit index");
    return;
  }

  // advance commit index to snapshot first
  SSnapshot snapshot;
  pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
  if (snapshot.lastApplyIndex > 0 && snapshot.lastApplyIndex > pSyncNode->commitIndex) {
    SyncIndex commitBegin = pSyncNode->commitIndex;
    SyncIndex commitEnd = snapshot.lastApplyIndex;
    pSyncNode->commitIndex = snapshot.lastApplyIndex;
    sNTrace(pSyncNode, "commit by snapshot from index:%" PRId64 " to index:%" PRId64, commitBegin, commitEnd);
  }

  // advance commit index as large as possible
  SyncIndex lastIndex = syncNodeGetLastIndex(pSyncNode);
  if (lastIndex > pSyncNode->commitIndex) {
    sNTrace(pSyncNode, "commit by wal from index:%" PRId64 " to index:%" PRId64, pSyncNode->commitIndex + 1, lastIndex);
    pSyncNode->commitIndex = lastIndex;
  }

  // call back Wal
  SyncIndex walCommitVer = logStoreWalCommitVer(pSyncNode->pLogStore);
  if (pSyncNode->commitIndex > walCommitVer) {
    pSyncNode->pLogStore->syncLogUpdateCommitIndex(pSyncNode->pLogStore, pSyncNode->commitIndex);
  }
}

void syncMaybeAdvanceCommitIndex(SSyncNode* pSyncNode) {
  ASSERTS(false, "deprecated");
  if (pSyncNode == NULL) {
    sError("pSyncNode is NULL");
    return;
  }

  if (pSyncNode->state != TAOS_SYNC_STATE_LEADER) {
    sNError(pSyncNode, "not leader, can not advance commit index");
    return;
  }

  // advance commit index to sanpshot first
  SSnapshot snapshot;
  pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
  if (snapshot.lastApplyIndex > 0 && snapshot.lastApplyIndex > pSyncNode->commitIndex) {
    SyncIndex commitBegin = pSyncNode->commitIndex;
    SyncIndex commitEnd = snapshot.lastApplyIndex;
    pSyncNode->commitIndex = snapshot.lastApplyIndex;
    sNTrace(pSyncNode, "commit by snapshot from index:%" PRId64 " to index:%" PRId64, commitBegin, commitEnd);
  }

  // update commit index
  SyncIndex newCommitIndex = pSyncNode->commitIndex;
  for (SyncIndex index = syncNodeGetLastIndex(pSyncNode); index > pSyncNode->commitIndex; --index) {
    bool agree = syncAgree(pSyncNode, index);

    if (agree) {
      // term
      SSyncRaftEntry* pEntry = NULL;
      SLRUCache*      pCache = pSyncNode->pLogStore->pCache;
      LRUHandle*      h = taosLRUCacheLookup(pCache, &index, sizeof(index));
      if (h) {
        pEntry = (SSyncRaftEntry*)taosLRUCacheValue(pCache, h);

        pSyncNode->pLogStore->cacheHit++;
        sNTrace(pSyncNode, "hit cache index:%" PRId64 ", bytes:%u, %p", index, pEntry->bytes, pEntry);

      } else {
        pSyncNode->pLogStore->cacheMiss++;
        sNTrace(pSyncNode, "miss cache index:%" PRId64, index);

        int32_t code = pSyncNode->pLogStore->syncLogGetEntry(pSyncNode->pLogStore, index, &pEntry);
        if (code != 0) {
          sNError(pSyncNode, "advance commit index error, read wal index:%" PRId64, index);
          return;
        }
      }
      // cannot commit, even if quorum agree. need check term!
      if (pEntry->term <= pSyncNode->pRaftStore->currentTerm) {
        // update commit index
        newCommitIndex = index;

        if (h) {
          taosLRUCacheRelease(pCache, h, false);
        } else {
          syncEntryDestroy(pEntry);
        }

        break;
      } else {
        sNTrace(pSyncNode, "can not commit due to term not equal, index:%" PRId64 ", term:%" PRIu64, pEntry->index,
                pEntry->term);
      }

      if (h) {
        taosLRUCacheRelease(pCache, h, false);
      } else {
        syncEntryDestroy(pEntry);
      }
    }
  }

  // advance commit index as large as possible
  SyncIndex walCommitVer = logStoreWalCommitVer(pSyncNode->pLogStore);
  if (walCommitVer > newCommitIndex) {
    newCommitIndex = walCommitVer;
  }

  // maybe execute fsm
  if (newCommitIndex > pSyncNode->commitIndex) {
    SyncIndex beginIndex = pSyncNode->commitIndex + 1;
    SyncIndex endIndex = newCommitIndex;

    // update commit index
    pSyncNode->commitIndex = newCommitIndex;

    // call back Wal
    pSyncNode->pLogStore->syncLogUpdateCommitIndex(pSyncNode->pLogStore, pSyncNode->commitIndex);

    // execute fsm
    if (pSyncNode != NULL && pSyncNode->pFsm != NULL) {
      int32_t code = syncNodeDoCommit(pSyncNode, beginIndex, endIndex, pSyncNode->state);
      if (code != 0) {
        sNError(pSyncNode, "advance commit index error, do commit begin:%" PRId64 ", end:%" PRId64, beginIndex,
                endIndex);
        return;
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

static inline int64_t syncNodeAbs64(int64_t a, int64_t b) {
  ASSERT(a >= 0);
  ASSERT(b >= 0);

  int64_t c = a > b ? a - b : b - a;
  return c;
}

int32_t syncNodeDynamicQuorum(const SSyncNode* pSyncNode) {
  return pSyncNode->quorum;

#if 0
  int32_t quorum = 1;  // self

  int64_t timeNow = taosGetTimestampMs();
  for (int i = 0; i < pSyncNode->peersNum; ++i) {
    int64_t   peerStartTime = syncIndexMgrGetStartTime(pSyncNode->pNextIndex, &(pSyncNode->peersId)[i]);
    int64_t   peerRecvTime = syncIndexMgrGetRecvTime(pSyncNode->pNextIndex, &(pSyncNode->peersId)[i]);
    SyncIndex peerMatchIndex = syncIndexMgrGetIndex(pSyncNode->pMatchIndex, &(pSyncNode->peersId)[i]);

    int64_t recvTimeDiff = TABS(peerRecvTime - timeNow);
    int64_t startTimeDiff = TABS(peerStartTime - pSyncNode->startTime);
    int64_t logDiff = TABS(peerMatchIndex - syncNodeGetLastIndex(pSyncNode));

    /*
        int64_t recvTimeDiff = syncNodeAbs64(peerRecvTime, timeNow);
        int64_t startTimeDiff = syncNodeAbs64(peerStartTime, pSyncNode->startTime);
        int64_t logDiff = syncNodeAbs64(peerMatchIndex, syncNodeGetLastIndex(pSyncNode));
    */

    int32_t addQuorum = 0;

    if (recvTimeDiff < SYNC_MAX_RECV_TIME_RANGE_MS) {
      if (startTimeDiff < SYNC_MAX_START_TIME_RANGE_MS) {
        addQuorum = 1;
      } else {
        if (logDiff < SYNC_ADD_QUORUM_COUNT) {
          addQuorum = 1;
        } else {
          addQuorum = 0;
        }
      }
    } else {
      addQuorum = 0;
    }

    /*
        if (recvTimeDiff < SYNC_MAX_RECV_TIME_RANGE_MS) {
          addQuorum = 1;
        } else {
          addQuorum = 0;
        }

        if (startTimeDiff > SYNC_MAX_START_TIME_RANGE_MS) {
          addQuorum = 0;
        }
    */

    quorum += addQuorum;
  }

  ASSERT(quorum <= pSyncNode->replicaNum);

  if (quorum < pSyncNode->quorum) {
    quorum = pSyncNode->quorum;
  }

  return quorum;
#endif
}

/*
bool syncAgree(SSyncNode* pSyncNode, SyncIndex index) {
  int agreeCount = 0;
  for (int i = 0; i < pSyncNode->replicaNum; ++i) {
    if (syncAgreeIndex(pSyncNode, &(pSyncNode->replicasId[i]), index)) {
      ++agreeCount;
    }
    if (agreeCount >= syncNodeDynamicQuorum(pSyncNode)) {
      return true;
    }
  }
  return false;
}
*/

bool syncNodeAgreedUpon(SSyncNode* pNode, SyncIndex index) {
  int            count = 0;
  SSyncIndexMgr* pMatches = pNode->pMatchIndex;
  ASSERT(pNode->replicaNum == pMatches->replicaNum);

  for (int i = 0; i < pNode->replicaNum; i++) {
    SyncIndex matchIndex = pMatches->index[i];
    if (matchIndex >= index) {
      count++;
    }
  }

  return count >= pNode->quorum;
}

bool syncAgree(SSyncNode* pNode, SyncIndex index) {
  int agreeCount = 0;
  for (int i = 0; i < pNode->replicaNum; ++i) {
    if (syncAgreeIndex(pNode, &(pNode->replicasId[i]), index)) {
      ++agreeCount;
    }
    if (agreeCount >= pNode->quorum) {
      return true;
    }
  }
  return false;
}

int64_t syncNodeUpdateCommitIndex(SSyncNode* ths, SyncIndex commitIndex) {
  SyncIndex lastVer = ths->pLogStore->syncLogLastIndex(ths->pLogStore);
  commitIndex = TMAX(commitIndex, ths->commitIndex);
  ths->commitIndex = TMIN(commitIndex, lastVer);
  ths->pLogStore->syncLogUpdateCommitIndex(ths->pLogStore, ths->commitIndex);
  return ths->commitIndex;
}

int64_t syncNodeCheckCommitIndex(SSyncNode* ths, SyncIndex indexLikely) {
  if (indexLikely > ths->commitIndex && syncNodeAgreedUpon(ths, indexLikely)) {
    SyncIndex commitIndex = indexLikely;
    syncNodeUpdateCommitIndex(ths, commitIndex);
    sTrace("vgId:%d, agreed upon. role:%d, term:%" PRId64 ", index: %" PRId64 "", ths->vgId, ths->state,
           ths->pRaftStore->currentTerm, commitIndex);
  }
  return ths->commitIndex;
}
