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
#include "syncReplication.h"
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

// only start once
static void syncNodeStartSnapshotOnce(SSyncNode* ths, SyncIndex beginIndex, SyncIndex endIndex, SyncTerm lastApplyTerm,
                                      SyncAppendEntriesReply* pMsg) {
  if (beginIndex > endIndex) {
    sNError(ths, "snapshot param error, start:%" PRId64 ", end:%" PRId64, beginIndex, endIndex);
    return;
  }

  // get sender
  SSyncSnapshotSender* pSender = syncNodeGetSnapshotSender(ths, &(pMsg->srcId));
  ASSERT(pSender != NULL);

  if (snapshotSenderIsStart(pSender)) {
    sSError(pSender, "snapshot sender already start");
    return;
  }

  SSnapshot snapshot = {
      .data = NULL, .lastApplyIndex = endIndex, .lastApplyTerm = lastApplyTerm, .lastConfigIndex = SYNC_INDEX_INVALID};
  void*          pReader = NULL;
  SSnapshotParam readerParam = {.start = beginIndex, .end = endIndex};
  int32_t        code = ths->pFsm->FpSnapshotStartRead(ths->pFsm, &readerParam, &pReader);
  ASSERT(code == 0);

#if 0
  if (pMsg->privateTerm < pSender->privateTerm) {
    ASSERT(pReader != NULL);
    snapshotSenderStart(pSender, readerParam, snapshot, pReader);

  } else {
    if (pReader != NULL) {
      ths->pFsm->FpSnapshotStopRead(ths->pFsm, pReader);
    }
  }
#endif
}

int32_t syncNodeOnAppendEntriesReply(SSyncNode* ths, SyncAppendEntriesReply* pMsg) {
  int32_t ret = 0;

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId))) {
    syncLogRecvAppendEntriesReply(ths, pMsg, "not in my config");
    return 0;
  }

  // drop stale response
  if (pMsg->term < ths->pRaftStore->currentTerm) {
    syncLogRecvAppendEntriesReply(ths, pMsg, "drop stale response");
    return 0;
  }

  if (ths->state == TAOS_SYNC_STATE_LEADER) {
    if (pMsg->term > ths->pRaftStore->currentTerm) {
      syncLogRecvAppendEntriesReply(ths, pMsg, "error term");
      syncNodeStepDown(ths, pMsg->term);
      return -1;
    }

    ASSERT(pMsg->term == ths->pRaftStore->currentTerm);

    if (pMsg->success) {
      SyncIndex oldMatchIndex = syncIndexMgrGetIndex(ths->pMatchIndex, &(pMsg->srcId));
      if (pMsg->matchIndex > oldMatchIndex) {
        syncIndexMgrSetIndex(ths->pMatchIndex, &(pMsg->srcId), pMsg->matchIndex);
        syncMaybeAdvanceCommitIndex(ths);
      }
      syncIndexMgrSetIndex(ths->pNextIndex, &(pMsg->srcId), pMsg->matchIndex + 1);

    } else {
      SyncIndex nextIndex = syncIndexMgrGetIndex(ths->pNextIndex, &(pMsg->srcId));
      if (nextIndex > SYNC_INDEX_BEGIN) {
        --nextIndex;
      }
      syncIndexMgrSetIndex(ths->pNextIndex, &(pMsg->srcId), nextIndex);
    }

    // send next append entries
    SPeerState* pState = syncNodeGetPeerState(ths, &(pMsg->srcId));
    ASSERT(pState != NULL);

    if (pMsg->lastSendIndex == pState->lastSendIndex) {
      syncNodeReplicateOne(ths, &(pMsg->srcId));
    }
  }

  syncLogRecvAppendEntriesReply(ths, pMsg, "process");
  return 0;
}