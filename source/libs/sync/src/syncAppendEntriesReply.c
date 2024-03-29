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
  SyncAppendEntriesReply* pMsg = (SyncAppendEntriesReply*)pRpcMsg->pCont;
  int32_t ret = 0;

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId))) {
    syncLogRecvAppendEntriesReply(ths, pMsg, "not in my config");
    return 0;
  }

  // drop stale response
  if (pMsg->term < raftStoreGetTerm(ths)) {
    syncLogRecvAppendEntriesReply(ths, pMsg, "drop stale response");
    return 0;
  }

  if (ths->state == TAOS_SYNC_STATE_LEADER) {
    if (pMsg->term > raftStoreGetTerm(ths)) {
      syncLogRecvAppendEntriesReply(ths, pMsg, "error term");
      syncNodeStepDown(ths, pMsg->term);
      return -1;
    }

    ASSERT(pMsg->term == raftStoreGetTerm(ths));

    sTrace("vgId:%d, received append entries reply. srcId:0x%016" PRIx64 ",  term:%" PRId64 ", matchIndex:%" PRId64 "",
           pMsg->vgId, pMsg->srcId.addr, pMsg->term, pMsg->matchIndex);

    if (pMsg->success) {
      SyncIndex oldMatchIndex = syncIndexMgrGetIndex(ths->pMatchIndex, &(pMsg->srcId));
      if (pMsg->matchIndex > oldMatchIndex) {
        syncIndexMgrSetIndex(ths->pMatchIndex, &(pMsg->srcId), pMsg->matchIndex);
      }

      // commit if needed
      SyncIndex indexLikely = TMIN(pMsg->matchIndex, ths->pLogBuf->matchIndex);
      SyncIndex commitIndex = syncNodeCheckCommitIndex(ths, indexLikely);
      (void)syncLogBufferCommit(ths->pLogBuf, ths, commitIndex);
    }

    // replicate log
    SSyncLogReplMgr* pMgr = syncNodeGetLogReplMgr(ths, &pMsg->srcId);
    if (pMgr == NULL) {
      sError("vgId:%d, failed to get log repl mgr for src addr: 0x%016" PRIx64, ths->vgId, pMsg->srcId.addr);
      return -1;
    }
    (void)syncLogReplProcessReply(pMgr, ths, pMsg);
  }
  return 0;
}
