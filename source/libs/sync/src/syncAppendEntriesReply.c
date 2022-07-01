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

#include "syncAppendEntriesReply.h"
#include "syncCommit.h"
#include "syncIndexMgr.h"
#include "syncInt.h"
#include "syncRaftCfg.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncSnapshot.h"
#include "syncUtil.h"
#include "syncVoteMgr.h"

// TLA+ Spec
// HandleAppendEntriesResponse(i, j, m) ==
//    /\ m.mterm = currentTerm[i]
//    /\ \/ /\ m.msuccess \* successful
//          /\ nextIndex'  = [nextIndex  EXCEPT ![i][j] = m.mmatchIndex + 1]
//          /\ matchIndex' = [matchIndex EXCEPT ![i][j] = m.mmatchIndex]
//       \/ /\ \lnot m.msuccess \* not successful
//          /\ nextIndex' = [nextIndex EXCEPT ![i][j] =
//                               Max({nextIndex[i][j] - 1, 1})]
//          /\ UNCHANGED <<matchIndex>>
//    /\ Discard(m)
//    /\ UNCHANGED <<serverVars, candidateVars, logVars, elections>>
//
int32_t syncNodeOnAppendEntriesReplyCb(SSyncNode* ths, SyncAppendEntriesReply* pMsg) {
  int32_t ret = 0;

  // print log
  syncAppendEntriesReplyLog2("==syncNodeOnAppendEntriesReplyCb==", pMsg);

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId)) && !ths->pRaftCfg->isStandBy) {
    syncNodeEventLog(ths, "recv sync-append-entries-reply,  maybe replica already dropped");
    return 0;
  }

  // drop stale response
  if (pMsg->term < ths->pRaftStore->currentTerm) {
    char logBuf[128];
    snprintf(logBuf, sizeof(logBuf), "recv sync-append-entries-reply, recv-term:%lu, drop stale response", pMsg->term);
    syncNodeEventLog(ths, logBuf);
    return 0;
  }

  if (gRaftDetailLog) {
    syncNodeEventLog(ths, "recv sync-append-entries-reply, before");
  }
  syncIndexMgrLog2("==syncNodeOnAppendEntriesReplyCb== before pNextIndex", ths->pNextIndex);
  syncIndexMgrLog2("==syncNodeOnAppendEntriesReplyCb== before pMatchIndex", ths->pMatchIndex);

  // no need this code, because if I receive reply.term, then I must have sent for that term.
  //  if (pMsg->term > ths->pRaftStore->currentTerm) {
  //    syncNodeUpdateTerm(ths, pMsg->term);
  //  }

  if (pMsg->term > ths->pRaftStore->currentTerm) {
    char logBuf[128];
    snprintf(logBuf, sizeof(logBuf), "recv sync-append-entries-reply, error term, recv-term:%lu", pMsg->term);
    syncNodeErrorLog(ths, logBuf);
    return -1;
  }

  ASSERT(pMsg->term == ths->pRaftStore->currentTerm);

  if (pMsg->success) {
    // nextIndex'  = [nextIndex  EXCEPT ![i][j] = m.mmatchIndex + 1]
    syncIndexMgrSetIndex(ths->pNextIndex, &(pMsg->srcId), pMsg->matchIndex + 1);

    // matchIndex' = [matchIndex EXCEPT ![i][j] = m.mmatchIndex]
    syncIndexMgrSetIndex(ths->pMatchIndex, &(pMsg->srcId), pMsg->matchIndex);

    // maybe commit
    syncMaybeAdvanceCommitIndex(ths);

  } else {
    SyncIndex nextIndex = syncIndexMgrGetIndex(ths->pNextIndex, &(pMsg->srcId));

    // notice! int64, uint64
    if (nextIndex > SYNC_INDEX_BEGIN) {
      --nextIndex;
    } else {
      nextIndex = SYNC_INDEX_BEGIN;
    }
    syncIndexMgrSetIndex(ths->pNextIndex, &(pMsg->srcId), nextIndex);
  }

  if (gRaftDetailLog) {
    syncNodeEventLog(ths, "recv sync-append-entries-reply, after");
  }
  syncIndexMgrLog2("==syncNodeOnAppendEntriesReplyCb== after pNextIndex", ths->pNextIndex);
  syncIndexMgrLog2("==syncNodeOnAppendEntriesReplyCb== after pMatchIndex", ths->pMatchIndex);

  return ret;
}

// only start once
static void syncNodeStartSnapshot(SSyncNode* ths, SyncIndex beginIndex, SyncIndex endIndex, SyncTerm lastApplyTerm,
                                  SyncAppendEntriesReply* pMsg) {
  // get sender
  SSyncSnapshotSender* pSender = syncNodeGetSnapshotSender(ths, &(pMsg->srcId));
  ASSERT(pSender != NULL);

  SSnapshot snapshot = {
      .data = NULL, .lastApplyIndex = endIndex, .lastApplyTerm = lastApplyTerm, .lastConfigIndex = SYNC_INDEX_INVALID};

  void*        pReader = NULL;
  SReaderParam readerParam = {.start = beginIndex, .end = endIndex};
  ths->pFsm->FpSnapshotStartRead(ths->pFsm, &pReader);
  if (!snapshotSenderIsStart(pSender) && pMsg->privateTerm < pSender->privateTerm) {
    ASSERT(pReader != NULL);
    snapshotSenderStart(pSender, snapshot, pReader);

  } else {
    if (pReader != NULL) {
      ths->pFsm->FpSnapshotStopRead(ths->pFsm, pReader);
    }
  }
}

int32_t syncNodeOnAppendEntriesReplySnapshot2Cb(SSyncNode* ths, SyncAppendEntriesReply* pMsg) {
  int32_t ret = 0;

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId)) && !ths->pRaftCfg->isStandBy) {
    syncNodeEventLog(ths, "recv sync-append-entries-reply,  maybe replica already dropped");
    return -1;
  }

  // drop stale response
  if (pMsg->term < ths->pRaftStore->currentTerm) {
    char logBuf[128];
    snprintf(logBuf, sizeof(logBuf), "recv sync-append-entries-reply, recv-term:%lu, drop stale response", pMsg->term);
    syncNodeEventLog(ths, logBuf);
    return -1;
  }

  // error term
  if (pMsg->term > ths->pRaftStore->currentTerm) {
    char logBuf[128];
    snprintf(logBuf, sizeof(logBuf), "recv sync-append-entries-reply, error term, recv-term:%lu", pMsg->term);
    syncNodeErrorLog(ths, logBuf);
    return -1;
  }

  ASSERT(pMsg->term == ths->pRaftStore->currentTerm);

  if (pMsg->success) {
    SyncIndex newNextIndex = pMsg->matchIndex + 1;
    SyncIndex newMatchIndex = pMsg->matchIndex;

    if (ths->pLogStore->syncLogExist(ths->pLogStore, newNextIndex) &&
        ths->pLogStore->syncLogExist(ths->pLogStore, newNextIndex - 1)) {
      // nextIndex'  = [nextIndex  EXCEPT ![i][j] = m.mmatchIndex + 1]
      syncIndexMgrSetIndex(ths->pNextIndex, &(pMsg->srcId), newNextIndex);

      // matchIndex' = [matchIndex EXCEPT ![i][j] = m.mmatchIndex]
      syncIndexMgrSetIndex(ths->pMatchIndex, &(pMsg->srcId), newMatchIndex);

      // maybe commit
      if (ths->state == TAOS_SYNC_STATE_LEADER) {
        syncMaybeAdvanceCommitIndex(ths);
      }
    } else {
      // start snapshot <match+1, old snapshot.end>
      SSnapshot snapshot;
      ths->pFsm->FpGetSnapshotInfo(ths->pFsm, &snapshot);
      syncNodeStartSnapshot(ths, newMatchIndex + 1, snapshot.lastApplyIndex, snapshot.lastApplyTerm, pMsg);

      syncIndexMgrSetIndex(ths->pNextIndex, &(pMsg->srcId), snapshot.lastApplyIndex + 1);
      syncIndexMgrSetIndex(ths->pMatchIndex, &(pMsg->srcId), newMatchIndex);
    }

  } else {
    SyncIndex nextIndex = syncIndexMgrGetIndex(ths->pNextIndex, &(pMsg->srcId));

    // notice! int64, uint64
    if (nextIndex > SYNC_INDEX_BEGIN) {
      --nextIndex;

      if (ths->pLogStore->syncLogExist(ths->pLogStore, nextIndex) &&
          ths->pLogStore->syncLogExist(ths->pLogStore, nextIndex - 1)) {
        // do nothing
      } else {
        SSyncRaftEntry* pEntry;
        int32_t         code = ths->pLogStore->syncLogGetEntry(ths->pLogStore, nextIndex, &pEntry);
        ASSERT(code == 0);
        syncNodeStartSnapshot(ths, SYNC_INDEX_BEGIN, nextIndex, pEntry->term, pMsg);

        // get sender
        SSyncSnapshotSender* pSender = syncNodeGetSnapshotSender(ths, &(pMsg->srcId));
        ASSERT(pSender != NULL);
        SyncIndex sentryIndex = pSender->snapshot.lastApplyIndex + 1;

        // update nextIndex to sentryIndex
        if (nextIndex <= sentryIndex) {
          nextIndex = sentryIndex;
        }
      }

    } else {
      nextIndex = SYNC_INDEX_BEGIN;
    }
    syncIndexMgrSetIndex(ths->pNextIndex, &(pMsg->srcId), nextIndex);
  }

  return 0;
}

int32_t syncNodeOnAppendEntriesReplySnapshotCb(SSyncNode* ths, SyncAppendEntriesReply* pMsg) {
  int32_t ret = 0;

  // print log
  syncAppendEntriesReplyLog2("==syncNodeOnAppendEntriesReplySnapshotCb==", pMsg);

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId)) && !ths->pRaftCfg->isStandBy) {
    syncNodeEventLog(ths, "recv sync-append-entries-reply,  maybe replica already dropped");
    return 0;
  }

  // drop stale response
  if (pMsg->term < ths->pRaftStore->currentTerm) {
    char logBuf[128];
    snprintf(logBuf, sizeof(logBuf), "recv sync-append-entries-reply, recv-term:%lu, drop stale response", pMsg->term);
    syncNodeEventLog(ths, logBuf);
    return 0;
  }

  if (gRaftDetailLog) {
    syncNodeEventLog(ths, "recv sync-append-entries-reply, before");
  }
  syncIndexMgrLog2("recv sync-append-entries-reply, before pNextIndex:", ths->pNextIndex);
  syncIndexMgrLog2("recv sync-append-entries-reply, before pMatchIndex:", ths->pMatchIndex);

  // no need this code, because if I receive reply.term, then I must have sent for that term.
  //  if (pMsg->term > ths->pRaftStore->currentTerm) {
  //    syncNodeUpdateTerm(ths, pMsg->term);
  //  }

  if (pMsg->term > ths->pRaftStore->currentTerm) {
    char logBuf[128];
    snprintf(logBuf, sizeof(logBuf), "recv sync-append-entries-reply, error term, recv-term:%lu", pMsg->term);
    syncNodeErrorLog(ths, logBuf);
    return -1;
  }

  ASSERT(pMsg->term == ths->pRaftStore->currentTerm);

  if (pMsg->success) {
    // nextIndex'  = [nextIndex  EXCEPT ![i][j] = m.mmatchIndex + 1]
    syncIndexMgrSetIndex(ths->pNextIndex, &(pMsg->srcId), pMsg->matchIndex + 1);

    if (gRaftDetailLog) {
      sTrace("update next match, index:%ld, success:%d", pMsg->matchIndex + 1, pMsg->success);
    }

    // matchIndex' = [matchIndex EXCEPT ![i][j] = m.mmatchIndex]
    syncIndexMgrSetIndex(ths->pMatchIndex, &(pMsg->srcId), pMsg->matchIndex);

    // maybe commit
    if (ths->state == TAOS_SYNC_STATE_LEADER) {
      syncMaybeAdvanceCommitIndex(ths);
    }

  } else {
    SyncIndex nextIndex = syncIndexMgrGetIndex(ths->pNextIndex, &(pMsg->srcId));
    if (gRaftDetailLog) {
      sTrace("update next index not match, begin, index:%ld, success:%d", nextIndex, pMsg->success);
    }

    // notice! int64, uint64
    if (nextIndex > SYNC_INDEX_BEGIN) {
      --nextIndex;

      // get sender
      SSyncSnapshotSender* pSender = syncNodeGetSnapshotSender(ths, &(pMsg->srcId));
      ASSERT(pSender != NULL);

      SSnapshot snapshot = {.data = NULL,
                            .lastApplyIndex = SYNC_INDEX_INVALID,
                            .lastApplyTerm = 0,
                            .lastConfigIndex = SYNC_INDEX_INVALID};
      void*     pReader = NULL;
      ths->pFsm->FpGetSnapshot(ths->pFsm, &snapshot, NULL, &pReader);
      if (snapshot.lastApplyIndex >= SYNC_INDEX_BEGIN && nextIndex <= snapshot.lastApplyIndex + 1 &&
          !snapshotSenderIsStart(pSender) && pMsg->privateTerm < pSender->privateTerm) {
        // has snapshot
        ASSERT(pReader != NULL);
        snapshotSenderStart(pSender, snapshot, pReader);

      } else {
        // no snapshot
        if (pReader != NULL) {
          ths->pFsm->FpSnapshotStopRead(ths->pFsm, pReader);
        }
      }

      SyncIndex sentryIndex = pSender->snapshot.lastApplyIndex + 1;

      // update nextIndex to sentryIndex
      if (nextIndex <= sentryIndex) {
        nextIndex = sentryIndex;
      }

    } else {
      nextIndex = SYNC_INDEX_BEGIN;
    }

    syncIndexMgrSetIndex(ths->pNextIndex, &(pMsg->srcId), nextIndex);
    if (gRaftDetailLog) {
      sTrace("update next index not match, end, index:%ld, success:%d", nextIndex, pMsg->success);
    }
  }

  if (gRaftDetailLog) {
    syncNodeEventLog(ths, "recv sync-append-entries-reply, after");
  }
  syncIndexMgrLog2("recv sync-append-entries-reply, after pNextIndex:", ths->pNextIndex);
  syncIndexMgrLog2("recv sync-append-entries-reply, after pMatchIndex:", ths->pMatchIndex);

  return 0;
}