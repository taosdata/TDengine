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
#include "syncAppendEntries.h"
#include "syncPipeline.h"
#include "syncMessage.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncReplication.h"
#include "syncUtil.h"
#include "syncCommit.h"

// TLA+ Spec
// HandleAppendEntriesRequest(i, j, m) ==
//    LET logOk == \/ m.mprevLogIndex = 0
//                 \/ /\ m.mprevLogIndex > 0
//                    /\ m.mprevLogIndex <= Len(log[i])
//                    /\ m.mprevLogTerm = log[i][m.mprevLogIndex].term
//    IN /\ m.mterm <= currentTerm[i]
//       /\ \/ /\ \* reject request
//                \/ m.mterm < currentTerm[i]
//                \/ /\ m.mterm = currentTerm[i]
//                   /\ state[i] = Follower
//                   /\ \lnot logOk
//             /\ Reply([mtype           |-> AppendEntriesResponse,
//                       mterm           |-> currentTerm[i],
//                       msuccess        |-> FALSE,
//                       mmatchIndex     |-> 0,
//                       msource         |-> i,
//                       mdest           |-> j],
//                       m)
//             /\ UNCHANGED <<serverVars, logVars>>
//          \/ \* return to follower state
//             /\ m.mterm = currentTerm[i]
//             /\ state[i] = Candidate
//             /\ state' = [state EXCEPT ![i] = Follower]
//             /\ UNCHANGED <<currentTerm, votedFor, logVars, messages>>
//          \/ \* accept request
//             /\ m.mterm = currentTerm[i]
//             /\ state[i] = Follower
//             /\ logOk
//             /\ LET index == m.mprevLogIndex + 1
//                IN \/ \* already done with request
//                       /\ \/ m.mentries = << >>
//                          \/ /\ m.mentries /= << >>
//                             /\ Len(log[i]) >= index
//                             /\ log[i][index].term = m.mentries[1].term
//                          \* This could make our commitIndex decrease (for
//                          \* example if we process an old, duplicated request),
//                          \* but that doesn't really affect anything.
//                       /\ commitIndex' = [commitIndex EXCEPT ![i] =
//                                              m.mcommitIndex]
//                       /\ Reply([mtype           |-> AppendEntriesResponse,
//                                 mterm           |-> currentTerm[i],
//                                 msuccess        |-> TRUE,
//                                 mmatchIndex     |-> m.mprevLogIndex +
//                                                     Len(m.mentries),
//                                 msource         |-> i,
//                                 mdest           |-> j],
//                                 m)
//                       /\ UNCHANGED <<serverVars, log>>
//                   \/ \* conflict: remove 1 entry
//                       /\ m.mentries /= << >>
//                       /\ Len(log[i]) >= index
//                       /\ log[i][index].term /= m.mentries[1].term
//                       /\ LET new == [index2 \in 1..(Len(log[i]) - 1) |->
//                                          log[i][index2]]
//                          IN log' = [log EXCEPT ![i] = new]
//                       /\ UNCHANGED <<serverVars, commitIndex, messages>>
//                   \/ \* no conflict: append entry
//                       /\ m.mentries /= << >>
//                       /\ Len(log[i]) = m.mprevLogIndex
//                       /\ log' = [log EXCEPT ![i] =
//                                      Append(log[i], m.mentries[1])]
//                       /\ UNCHANGED <<serverVars, commitIndex, messages>>
//       /\ UNCHANGED <<candidateVars, leaderVars>>
//

int32_t syncNodeOnAppendEntries(SSyncNode* ths, const SRpcMsg* pRpcMsg) {
  SyncAppendEntries* pMsg = pRpcMsg->pCont;
  SRpcMsg            rpcRsp = {0};
  bool               accepted = false;
  SSyncRaftEntry*    pEntry = NULL;
  bool               resetElect = false;

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId))) {
    syncLogRecvAppendEntries(ths, pMsg, "not in my config");
    goto _IGNORE;
  }

  int32_t code = syncBuildAppendEntriesReply(&rpcRsp, ths->vgId);
  if (code != 0) {
    syncLogRecvAppendEntries(ths, pMsg, "build rsp error");
    goto _IGNORE;
  }

  SyncAppendEntriesReply* pReply = rpcRsp.pCont;
  // prepare response msg
  pReply->srcId = ths->myRaftId;
  pReply->destId = pMsg->srcId;
  pReply->term = raftStoreGetTerm(ths);
  pReply->success = false;
  pReply->matchIndex = SYNC_INDEX_INVALID;
  pReply->lastSendIndex = pMsg->prevLogIndex + 1;
  pReply->startTime = ths->startTime;

  if (pMsg->term < raftStoreGetTerm(ths)) {
    goto _SEND_RESPONSE;
  }

  if (pMsg->term > raftStoreGetTerm(ths)) {
    pReply->term = pMsg->term;
  }

  if(ths->raftCfg.cfg.nodeInfo[ths->raftCfg.cfg.myIndex].nodeRole != TAOS_SYNC_ROLE_LEARNER){
    syncNodeStepDown(ths, pMsg->term);
    resetElect = true;
  }

  if (pMsg->dataLen < sizeof(SSyncRaftEntry)) {
    sError("vgId:%d, incomplete append entries received. prev index:%" PRId64 ", term:%" PRId64 ", datalen:%d",
           ths->vgId, pMsg->prevLogIndex, pMsg->prevLogTerm, pMsg->dataLen);
    goto _IGNORE;
  }

  pEntry = syncEntryBuildFromAppendEntries(pMsg);
  if (pEntry == NULL) {
    sError("vgId:%d, failed to get raft entry from append entries since %s", ths->vgId, terrstr());
    goto _IGNORE;
  }

  if (pMsg->prevLogIndex + 1 != pEntry->index || pEntry->term < 0) {
    sError("vgId:%d, invalid previous log index in msg. index:%" PRId64 ",  term:%" PRId64 ", prevLogIndex:%" PRId64
           ", prevLogTerm:%" PRId64,
           ths->vgId, pEntry->index, pEntry->term, pMsg->prevLogIndex, pMsg->prevLogTerm);
    goto _IGNORE;
  }

  sTrace("vgId:%d, recv append entries msg. index:%" PRId64 ", term:%" PRId64 ", preLogIndex:%" PRId64
         ", prevLogTerm:%" PRId64 " commitIndex:%" PRId64 " entryterm:%" PRId64,
         pMsg->vgId, pMsg->prevLogIndex + 1, pMsg->term, pMsg->prevLogIndex, pMsg->prevLogTerm, pMsg->commitIndex,
         pEntry->term);

  if (ths->fsmState == SYNC_FSM_STATE_INCOMPLETE) {
    pReply->fsmState = ths->fsmState;
    sWarn("vgId:%d, unable to accept, due to incomplete fsm state. index:%" PRId64, ths->vgId, pEntry->index);
    syncEntryDestroy(pEntry);
    goto _SEND_RESPONSE;
  }

  // accept
  if (syncLogBufferAccept(ths->pLogBuf, ths, pEntry, pMsg->prevLogTerm) < 0) {
    goto _SEND_RESPONSE;
  }
  accepted = true;

_SEND_RESPONSE:
  pEntry = NULL;
  pReply->matchIndex = syncLogBufferProceed(ths->pLogBuf, ths, &pReply->lastMatchTerm, "OnAppn");
  bool matched = (pReply->matchIndex >= pReply->lastSendIndex);
  if (accepted && matched) {
    pReply->success = true;
    // update commit index only after matching
    (void)syncNodeUpdateCommitIndex(ths, TMIN(pMsg->commitIndex, pReply->lastSendIndex));
  }

  // ack, i.e. send response
  (void)syncNodeSendMsgById(&pReply->destId, ths, &rpcRsp);

  // commit index, i.e. leader notice me
  if (ths->fsmState != SYNC_FSM_STATE_INCOMPLETE && syncLogBufferCommit(ths->pLogBuf, ths, ths->commitIndex) < 0) {
    sError("vgId:%d, failed to commit raft fsm log since %s.", ths->vgId, terrstr());
  }

  if (resetElect) syncNodeResetElectTimer(ths);
  return 0;

_IGNORE:
  rpcFreeCont(rpcRsp.pCont);
  syncEntryDestroy(pEntry);
  return 0;
}
