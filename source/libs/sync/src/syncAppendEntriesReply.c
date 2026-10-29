/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include "syncAppendEntriesReply.h"
#include "syncCommit.h"
#include "syncIndexMgr.h"
#include "syncPipeline.h"
#include "syncMessage.h"
#include "syncRaftEntry.h"
#include "syncRaftStore.h"
#include "syncReplication.h"
#include "syncSnapshot.h"
#include "syncUtil.h"

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

int32_t syncNodeOnAppendEntriesReply(SSyncNode* ths, const SRpcMsg* pRpcMsg) {
  int32_t                 code = 0;
  SyncAppendEntriesReply* pMsg = (SyncAppendEntriesReply*)pRpcMsg->pCont;
  int32_t                 ret = 0;

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId))) {
    syncLogRecvAppendEntriesReply(ths, pMsg, "not in my config", &pRpcMsg->info.traceId);
    return 0;
  }

  // drop stale response
  if (pMsg->term < raftStoreGetTerm(ths)) {
    syncLogRecvAppendEntriesReply(ths, pMsg, "drop stale response", &pRpcMsg->info.traceId);
    return 0;
  }

  if (ths->state == TAOS_SYNC_STATE_LEADER || ths->state == TAOS_SYNC_STATE_ASSIGNED_LEADER) {
    if (pMsg->term != raftStoreGetTerm(ths)) {
      syncLogRecvAppendEntriesReply(ths, pMsg, "error term", &pRpcMsg->info.traceId);
      syncNodeStepDown(ths, pMsg->term, pMsg->srcId, "appendEntryReply");
      return TSDB_CODE_SYN_WRONG_TERM;
    }

    sGDebug(&pRpcMsg->info.traceId, "vgId:%d, received append entries reply, src addr:0x%" PRIx64 ", term:%" PRId64 ", matchIndex:%" PRId64,
            pMsg->vgId, pMsg->srcId.addr, pMsg->term, pMsg->matchIndex);

    // TD-SEC-2026-044 (item 184): fetch the per-peer replication manager up front.
    // It holds the authoritative record (pMgr->endIndex - 1) of the highest log
    // index this leader has actually transmitted to the acknowledging peer. We use
    // it below to reject a forged reply that claims a matchIndex larger than what
    // was really sent, which would otherwise let a single spoofed reply fabricate a
    // raft quorum and commit entries no real follower stored (raft safety break).
    SSyncLogReplMgr* pMgr = syncNodeGetLogReplMgr(ths, &pMsg->srcId);
    if (pMgr == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      sError("vgId:%d, failed to get log repl mgr for src addr:0x%" PRIx64, ths->vgId, pMsg->srcId.addr);
      TAOS_RETURN(code);
    }

    if (pMsg->success) {
      SyncIndex oldMatchIndex = syncIndexMgrGetIndex(ths->pMatchIndex, &(pMsg->srcId));

      // TD-SEC-2026-044 (item 184): defense against a forged AppendEntriesReply.
      // The leader's own matchIndex always counts toward quorum, so a single
      // spoofed success reply with matchIndex = INT64_MAX is enough (3 replicas)
      // to declare every entry in the leader's log committed. A genuine follower
      // can only have matched log indexes the leader actually sent it; that upper
      // bound is pMgr->endIndex - 1 (the last index the leader replicated to this
      // peer). Any claim above it is impossible for a real follower and is treated
      // as a spoofing attempt: we clamp it down and do not advance the recorded
      // matchIndex / commit index beyond what was genuinely replicated.
      SyncIndex validMatchIndex = pMsg->matchIndex;
      SyncIndex maxSentIndex = (pMgr->endIndex > 0) ? (pMgr->endIndex - 1) : SYNC_INDEX_INVALID;
      if (validMatchIndex > maxSentIndex) {
        sWarn("vgId:%d, reject impossible matchIndex:%" PRId64 " claimed by dnode:%d, leader only sent up to %" PRId64
              " to this peer, possible forged AppendEntriesReply (TD-SEC-2026-044 item 184)",
              ths->vgId, pMsg->matchIndex, DID(&pMsg->srcId), maxSentIndex);
        validMatchIndex = maxSentIndex;
      }

      if (validMatchIndex > oldMatchIndex) {
        syncIndexMgrSetIndex(ths->pMatchIndex, &(pMsg->srcId), validMatchIndex);
      }

      // commit if needed
      SyncIndex indexLikely = TMIN(validMatchIndex, ths->pLogBuf->matchIndex);
      SyncIndex commitIndex = syncNodeCheckCommitIndex(ths, indexLikely, &pRpcMsg->info.traceId);
      if (ths->state == TAOS_SYNC_STATE_ASSIGNED_LEADER) {
        if (commitIndex >= ths->assignedCommitIndex) {
          // Backward compatibility: old version messages don't include appliedIndex field.
          // If message is from old version, default to commitIndex so gap=0, preserving old step-down behavior.
          SyncIndex peerAppliedIndex = commitIndex;
          if (pRpcMsg->contLen >= (int32_t)sizeof(SyncAppendEntriesReply)) {
            peerAppliedIndex = pMsg->appliedIndex;
          }
          SyncIndex appliedGap = commitIndex - peerAppliedIndex;
          if (tsSyncAssignedCheckAppliedGap == 0 || appliedGap <= tsSyncAssignedCheckAppliedGap) {
            sInfo("vgId:%d, going to step down from assigned leader by append entries reply, commitIndex:%" PRId64
                  ", assignedCommitIndex:%" PRId64 ", peerAppliedIndex:%" PRId64 ", appliedGap:%" PRId64,
                  ths->vgId, commitIndex, ths->assignedCommitIndex, peerAppliedIndex, appliedGap);
            syncNodeStepDown(ths, pMsg->term, pMsg->destId, "appendEntryReply");
          } else {
            sDebug("vgId:%d, delay step down from assigned leader, commitIndex:%" PRId64
                   ", assignedCommitIndex:%" PRId64 ", peerAppliedIndex:%" PRId64 ", appliedGap:%" PRId64
                   ", threshold:%" PRId64,
                   ths->vgId, commitIndex, ths->assignedCommitIndex, peerAppliedIndex, appliedGap,
                   tsSyncAssignedCheckAppliedGap);
          }
        }
      } else {
        TAOS_CHECK_RETURN(syncLogBufferCommit(ths->pLogBuf, ths, commitIndex, &pRpcMsg->info.traceId, "sync-append-entries-reply"));
      }
    }

    // replicate log (pMgr already fetched and validated above)
    TAOS_CHECK_RETURN(syncLogReplProcessReply(pMgr, ths, pMsg));
  }
  TAOS_RETURN(code);
}
