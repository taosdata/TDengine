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

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId)) && !ths->pRaftCfg->isStandBy) {
    syncLogRecvAppendEntriesReply(ths, pMsg, "maybe replica already dropped");
    return -1;
  }

  // drop stale response
  if (pMsg->term < ths->pRaftStore->currentTerm) {
    syncLogRecvAppendEntriesReply(ths, pMsg, "drop stale response");
    return 0;
  }

  // no need this code, because if I receive reply.term, then I must have sent for that term.
  //  if (pMsg->term > ths->pRaftStore->currentTerm) {
  //    syncNodeUpdateTerm(ths, pMsg->term);
  //  }

  if (pMsg->term > ths->pRaftStore->currentTerm) {
    syncLogRecvAppendEntriesReply(ths, pMsg, "error term");
    return -1;
  }

  ASSERT(pMsg->term == ths->pRaftStore->currentTerm);

  SyncIndex beforeNextIndex = syncIndexMgrGetIndex(ths->pNextIndex, &(pMsg->srcId));
  SyncIndex beforeMatchIndex = syncIndexMgrGetIndex(ths->pMatchIndex, &(pMsg->srcId));

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

  SyncIndex afterNextIndex = syncIndexMgrGetIndex(ths->pNextIndex, &(pMsg->srcId));
  SyncIndex afterMatchIndex = syncIndexMgrGetIndex(ths->pMatchIndex, &(pMsg->srcId));
  do {
    char logBuf[256];
    snprintf(logBuf, sizeof(logBuf), "before next:%ld, match:%ld, after next:%ld, match:%ld", beforeNextIndex,
             beforeMatchIndex, afterNextIndex, afterMatchIndex);
    syncLogRecvAppendEntriesReply(ths, pMsg, logBuf);
  } while (0);

  return 0;
}

// only start once
static void syncNodeStartSnapshotOnce(SSyncNode* ths, SyncIndex beginIndex, SyncIndex endIndex, SyncTerm lastApplyTerm,
                                      SyncAppendEntriesReply* pMsg) {
  // get sender
  SSyncSnapshotSender* pSender = syncNodeGetSnapshotSender(ths, &(pMsg->srcId));
  ASSERT(pSender != NULL);

  if (snapshotSenderIsStart(pSender)) {
    do {
      char* eventLog = snapshotSender2SimpleStr(pSender, "snapshot sender already start");
      syncNodeErrorLog(ths, eventLog);
      taosMemoryFree(eventLog);
    } while (0);

    return;
  }

  SSnapshot snapshot = {
      .data = NULL, .lastApplyIndex = endIndex, .lastApplyTerm = lastApplyTerm, .lastConfigIndex = SYNC_INDEX_INVALID};
  void*          pReader = NULL;
  SSnapshotParam readerParam = {.start = beginIndex, .end = endIndex};
  int32_t        code = ths->pFsm->FpSnapshotStartRead(ths->pFsm, &readerParam, &pReader);
  ASSERT(code == 0);

  if (pMsg->privateTerm < pSender->privateTerm) {
    ASSERT(pReader != NULL);
    snapshotSenderStart(pSender, readerParam, snapshot, pReader);

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
    syncLogRecvAppendEntriesReply(ths, pMsg, "maybe replica already dropped");
    return -1;
  }

  // drop stale response
  if (pMsg->term < ths->pRaftStore->currentTerm) {
    syncLogRecvAppendEntriesReply(ths, pMsg, "drop stale response");
    return 0;
  }

  // error term
  if (pMsg->term > ths->pRaftStore->currentTerm) {
    syncLogRecvAppendEntriesReply(ths, pMsg, "error term");
    return -1;
  }

  ASSERT(pMsg->term == ths->pRaftStore->currentTerm);

  SyncIndex beforeNextIndex = syncIndexMgrGetIndex(ths->pNextIndex, &(pMsg->srcId));
  SyncIndex beforeMatchIndex = syncIndexMgrGetIndex(ths->pMatchIndex, &(pMsg->srcId));

  if (pMsg->success) {
    SyncIndex newNextIndex = pMsg->matchIndex + 1;
    SyncIndex newMatchIndex = pMsg->matchIndex;

    bool needStartSnapshot = false;
    if (newMatchIndex >= SYNC_INDEX_BEGIN && !ths->pLogStore->syncLogExist(ths->pLogStore, newMatchIndex)) {
      needStartSnapshot = true;
    }

    if (!needStartSnapshot) {
      // update next-index, match-index
      syncIndexMgrSetIndex(ths->pNextIndex, &(pMsg->srcId), newNextIndex);
      syncIndexMgrSetIndex(ths->pMatchIndex, &(pMsg->srcId), newMatchIndex);

      // maybe commit
      if (ths->state == TAOS_SYNC_STATE_LEADER) {
        syncMaybeAdvanceCommitIndex(ths);
      }

    } else {
      // start snapshot <match+1, old snapshot.end>
      SSnapshot oldSnapshot;
      ths->pFsm->FpGetSnapshotInfo(ths->pFsm, &oldSnapshot);
      if (oldSnapshot.lastApplyIndex > newMatchIndex) {
        syncNodeStartSnapshotOnce(ths, newMatchIndex + 1, oldSnapshot.lastApplyIndex, oldSnapshot.lastApplyTerm,
                                  pMsg);  // term maybe not ok?
      }

      syncIndexMgrSetIndex(ths->pNextIndex, &(pMsg->srcId), oldSnapshot.lastApplyIndex + 1);
      syncIndexMgrSetIndex(ths->pMatchIndex, &(pMsg->srcId), newMatchIndex);
    }

    // event log, update next-index
    do {
      char    host[64];
      int16_t port;
      syncUtilU642Addr(pMsg->srcId.addr, host, sizeof(host), &port);

      char logBuf[256];
      snprintf(logBuf, sizeof(logBuf), "reset next-index:%" PRId64 ", match-index:%" PRId64 " for %s:%d", newNextIndex,
               newMatchIndex, host, port);
      syncNodeEventLog(ths, logBuf);

    } while (0);

  } else {
    SyncIndex nextIndex = syncIndexMgrGetIndex(ths->pNextIndex, &(pMsg->srcId));

    if (nextIndex > SYNC_INDEX_BEGIN) {
      --nextIndex;

      bool needStartSnapshot = false;
      if (nextIndex >= SYNC_INDEX_BEGIN && !ths->pLogStore->syncLogExist(ths->pLogStore, nextIndex)) {
        needStartSnapshot = true;
      }
      if (nextIndex - 1 >= SYNC_INDEX_BEGIN && !ths->pLogStore->syncLogExist(ths->pLogStore, nextIndex - 1)) {
        needStartSnapshot = true;
      }

      if (!needStartSnapshot) {
        // do nothing

      } else {
        SSnapshot oldSnapshot;
        ths->pFsm->FpGetSnapshotInfo(ths->pFsm, &oldSnapshot);
        SyncTerm newSnapshotTerm = oldSnapshot.lastApplyTerm;

        SyncIndex endIndex;
        if (ths->pLogStore->syncLogExist(ths->pLogStore, nextIndex + 1)) {
          endIndex = nextIndex;
        } else {
          endIndex = oldSnapshot.lastApplyIndex;
        }
        syncNodeStartSnapshotOnce(ths, pMsg->matchIndex + 1, endIndex, newSnapshotTerm, pMsg);

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

    SyncIndex oldMatchIndex = syncIndexMgrGetIndex(ths->pMatchIndex, &(pMsg->srcId));
    if (pMsg->matchIndex > oldMatchIndex) {
      syncIndexMgrSetIndex(ths->pMatchIndex, &(pMsg->srcId), pMsg->matchIndex);
    }

    // event log, update next-index
    do {
      char    host[64];
      int16_t port;
      syncUtilU642Addr(pMsg->srcId.addr, host, sizeof(host), &port);

      SyncIndex newNextIndex = nextIndex;
      SyncIndex newMatchIndex = syncIndexMgrGetIndex(ths->pMatchIndex, &(pMsg->srcId));
      char      logBuf[256];
      snprintf(logBuf, sizeof(logBuf), "reset2 next-index:%" PRId64 ", match-index:%" PRId64 " for %s:%d", newNextIndex,
               newMatchIndex, host, port);
      syncNodeEventLog(ths, logBuf);

    } while (0);
  }

  SyncIndex afterNextIndex = syncIndexMgrGetIndex(ths->pNextIndex, &(pMsg->srcId));
  SyncIndex afterMatchIndex = syncIndexMgrGetIndex(ths->pMatchIndex, &(pMsg->srcId));
  do {
    char logBuf[256];
    snprintf(logBuf, sizeof(logBuf), "before next:%ld, match:%ld, after next:%ld, match:%ld", beforeNextIndex,
             beforeMatchIndex, afterNextIndex, afterMatchIndex);
    syncLogRecvAppendEntriesReply(ths, pMsg, logBuf);
  } while (0);

  return 0;
}

int32_t syncNodeOnAppendEntriesReplySnapshotCb(SSyncNode* ths, SyncAppendEntriesReply* pMsg) {
  int32_t ret = 0;

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId)) && !ths->pRaftCfg->isStandBy) {
    syncLogRecvAppendEntriesReply(ths, pMsg, "maybe replica already dropped");
    return -1;
  }

  // drop stale response
  if (pMsg->term < ths->pRaftStore->currentTerm) {
    syncLogRecvAppendEntriesReply(ths, pMsg, "drop stale response");
    return 0;
  }

  // no need this code, because if I receive reply.term, then I must have sent for that term.
  //  if (pMsg->term > ths->pRaftStore->currentTerm) {
  //    syncNodeUpdateTerm(ths, pMsg->term);
  //  }

  if (pMsg->term > ths->pRaftStore->currentTerm) {
    syncLogRecvAppendEntriesReply(ths, pMsg, "error term");
    return -1;
  }

  ASSERT(pMsg->term == ths->pRaftStore->currentTerm);

  SyncIndex beforeNextIndex = syncIndexMgrGetIndex(ths->pNextIndex, &(pMsg->srcId));
  SyncIndex beforeMatchIndex = syncIndexMgrGetIndex(ths->pMatchIndex, &(pMsg->srcId));

  if (pMsg->success) {
    // nextIndex'  = [nextIndex  EXCEPT ![i][j] = m.mmatchIndex + 1]
    syncIndexMgrSetIndex(ths->pNextIndex, &(pMsg->srcId), pMsg->matchIndex + 1);

    if (gRaftDetailLog) {
      sTrace("update next match, index:%" PRId64 ", success:%d", pMsg->matchIndex + 1, pMsg->success);
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
      sTrace("update next index not match, begin, index:%" PRId64 ", success:%d", nextIndex, pMsg->success);
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
        SSnapshotParam readerParam = {.start = 0, .end = snapshot.lastApplyIndex};
        snapshotSenderStart(pSender, readerParam, snapshot, pReader);

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
      sTrace("update next index not match, end, index:%" PRId64 ", success:%d", nextIndex, pMsg->success);
    }
  }

  SyncIndex afterNextIndex = syncIndexMgrGetIndex(ths->pNextIndex, &(pMsg->srcId));
  SyncIndex afterMatchIndex = syncIndexMgrGetIndex(ths->pMatchIndex, &(pMsg->srcId));
  do {
    char logBuf[256];
    snprintf(logBuf, sizeof(logBuf), "before next:%ld, match:%ld, after next:%ld, match:%ld", beforeNextIndex,
             beforeMatchIndex, afterNextIndex, afterMatchIndex);
    syncLogRecvAppendEntriesReply(ths, pMsg, logBuf);
  } while (0);

  return 0;
}