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
#include "syncRequestVote.h"
#include "syncMessage.h"
#include "syncRaftCfg.h"
#include "syncRaftStore.h"
#include "syncUtil.h"
#include "syncVoteMgr.h"

// TLA+ Spec
// HandleRequestVoteRequest(i, j, m) ==
//    LET logOk == \/ m.mlastLogTerm > LastTerm(log[i])
//                 \/ /\ m.mlastLogTerm = LastTerm(log[i])
//                    /\ m.mlastLogIndex >= Len(log[i])
//        grant == /\ m.mterm = currentTerm[i]
//                 /\ logOk
//                 /\ votedFor[i] \in {Nil, j}
//    IN /\ m.mterm <= currentTerm[i]
//       /\ \/ grant  /\ votedFor' = [votedFor EXCEPT ![i] = j]
//          \/ ~grant /\ UNCHANGED votedFor
//       /\ Reply([mtype        |-> RequestVoteResponse,
//                 mterm        |-> currentTerm[i],
//                 mvoteGranted |-> grant,
//                 \* mlog is used just for the `elections' history variable for
//                 \* the proof. It would not exist in a real implementation.
//                 mlog         |-> log[i],
//                 msource      |-> i,
//                 mdest        |-> j],
//                 m)
//       /\ UNCHANGED <<state, currentTerm, candidateVars, leaderVars, logVars>>
//

static bool syncNodeOnRequestVoteLogOK(SSyncNode* ths, SyncRequestVote* pMsg) {
  SyncTerm  myLastTerm = syncNodeGetLastTerm(ths);
  SyncIndex myLastIndex = syncNodeGetLastIndex(ths);

  if (myLastTerm == SYNC_TERM_INVALID) {
    sNWarn(ths,
           "logok:0, my last term invalid, {my-lterm:%" PRIu64 ", my-lindex:%" PRId64 ", recv-lterm:%" PRIu64
           ", recv-lindex:%" PRId64 ", recv-term:%" PRIu64 "}",
           myLastTerm, myLastIndex, pMsg->lastLogTerm, pMsg->lastLogIndex, pMsg->term);
    return false;
  }

  if (pMsg->lastLogTerm > myLastTerm) {
    sNInfo(ths,
           "logok:1, larger log term, {my-lterm:%" PRIu64 ", my-lindex:%" PRId64 ", recv-lterm:%" PRIu64
           ", recv-lindex:%" PRId64 ", recv-term:%" PRIu64 "}",
           myLastTerm, myLastIndex, pMsg->lastLogTerm, pMsg->lastLogIndex, pMsg->term);

    if (pMsg->lastLogIndex < ths->commitIndex) {
      sNWarn(ths,
             "logok:1, commit rollback required. {my-lterm:%" PRIu64 ", my-lindex:%" PRId64 ", recv-lterm:%" PRIu64
             ", recv-lindex:%" PRId64 ", recv-term:%" PRIu64 "}",
             myLastTerm, myLastIndex, pMsg->lastLogTerm, pMsg->lastLogIndex, pMsg->term);
    }
    return true;
  }

  if (pMsg->lastLogTerm == myLastTerm && pMsg->lastLogIndex >= myLastIndex) {
    sNInfo(ths,
           "logok:1, larger log index , {my-lterm:%" PRIu64 ", my-lindex:%" PRId64 ", recv-lterm:%" PRIu64
           ", recv-lindex:%" PRId64 ", recv-term:%" PRIu64 "}",
           myLastTerm, myLastIndex, pMsg->lastLogTerm, pMsg->lastLogIndex, pMsg->term);
    return true;
  }

  sNWarn(ths,
         "logok:0, {my-lterm:%" PRIu64 ", my-lindex:%" PRId64 ", recv-lterm:%" PRIu64 ", recv-lindex:%" PRId64
         ", recv-term:%" PRIu64 "}",
         myLastTerm, myLastIndex, pMsg->lastLogTerm, pMsg->lastLogIndex, pMsg->term);
  return false;
}

bool syncNodeOnRequestVoteAppliedIndexOK(SSyncNode* ths, const SyncRequestVote* pMsg, SyncIndex* pLocalApplied) {
  *pLocalApplied = syncNodeGetAppliedIndex(ths);

  if (*pLocalApplied < SYNC_INDEX_BEGIN || pMsg->candidateAppliedIndex < SYNC_INDEX_BEGIN) {
    sNInfo(ths,
           "appliedok:1, fallback due to unavailable applied index, {my-applied:%" PRId64 ", recv-applied:%" PRId64
           ", recv-term:%" PRIu64 "}",
           *pLocalApplied, pMsg->candidateAppliedIndex, pMsg->term);
    return true;
  }

  if (pMsg->candidateAppliedIndex < *pLocalApplied) {
    sNWarn(ths,
           "appliedok:0, candidate applied index behind local state, {my-applied:%" PRId64 ", recv-applied:%" PRId64
           ", recv-term:%" PRIu64 "}",
           *pLocalApplied, pMsg->candidateAppliedIndex, pMsg->term);
    return false;
  }

  sNInfo(ths,
         "appliedok:1, candidate applied index acceptable, {my-applied:%" PRId64 ", recv-applied:%" PRId64
         ", recv-term:%" PRIu64 "}",
         *pLocalApplied, pMsg->candidateAppliedIndex, pMsg->term);
  return true;
}

int32_t syncNodeOnRequestVote(SSyncNode* ths, const SRpcMsg* pRpcMsg) {
  int32_t          ret = 0;
  SyncRequestVote* pMsg = pRpcMsg->pCont;
  bool             resetElect = false;

  syncLogRecvRequestVote(ths, pMsg, -1, "", "recv", &pRpcMsg->info.traceId);

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &pMsg->srcId)) {
    syncLogRecvRequestVote(ths, pMsg, -1, "not in my config", "process", &pRpcMsg->info.traceId);

    TAOS_RETURN(TSDB_CODE_SYN_NOT_IN_RAFT_GROUP);
  }

  bool logOK = syncNodeOnRequestVoteLogOK(ths, pMsg);
  SyncIndex localAppliedIndex = SYNC_INDEX_INVALID;
  bool      appliedOK = syncNodeOnRequestVoteAppliedIndexOK(ths, pMsg, &localAppliedIndex);
  // maybe update term
  if (pMsg->term > raftStoreGetTerm(ths)) {
    syncNodeStepDown(ths, pMsg->term, pMsg->srcId, "requestVote-1");
  }
  SyncTerm currentTerm = raftStoreGetTerm(ths);
  if (!(pMsg->term <= currentTerm)) return TSDB_CODE_SYN_INTERNAL_ERROR;

  bool hasVoted = raftStoreHasVoted(ths);
  bool grant = (pMsg->term == currentTerm) && logOK && appliedOK &&
               ((!hasVoted) || syncUtilSameId(&ths->raftStore.voteFor, &pMsg->srcId));
  sInfo("vgId:%d, grant:%d, hasVoted:%d, voteFor:0x%" PRIx64 ", srcId:0x%" PRIx64 ", logOK:%d, msg term:%" PRId64
        ", current term:%" PRId64 ", appliedOK:%d, my applied:%" PRId64 ", recv applied:%" PRId64,
        ths->vgId, grant, hasVoted, ths->raftStore.voteFor.addr, pMsg->srcId.addr, logOK, pMsg->term, currentTerm,
        appliedOK, localAppliedIndex, pMsg->candidateAppliedIndex);
  if (grant) {
    // maybe has already voted for pMsg->srcId
    // vote again, no harm
    raftStoreVote(ths, &(pMsg->srcId));

    // candidate ?
    syncNodeStepDown(ths, currentTerm, pMsg->srcId, "requestVote-2");

    // forbid elect for this round
    resetElect = true;
  }

  // send msg
  SRpcMsg rpcMsg = {0};

  TAOS_CHECK_RETURN(syncBuildRequestVoteReply(&rpcMsg, ths->vgId));

  SyncRequestVoteReply* pReply = rpcMsg.pCont;
  pReply->srcId = ths->myRaftId;
  pReply->destId = pMsg->srcId;
  pReply->term = currentTerm;
  pReply->voteGranted = grant;
  if (!(!grant || pMsg->term == pReply->term)) return TSDB_CODE_SYN_INTERNAL_ERROR;

  // trace log
  syncLogRecvRequestVote(ths, pMsg, pReply->voteGranted, "", "proceed", &pRpcMsg->info.traceId);

  rpcMsg.info.traceId = pRpcMsg->info.traceId;
  TRACE_SET_MSGID(&(rpcMsg.info.traceId), tGenIdPI64());
  syncLogSendRequestVoteReply(ths, pReply, "", &rpcMsg.info.traceId);
  TAOS_CHECK_RETURN(syncNodeSendMsgById(&pReply->destId, ths, &rpcMsg));

  if (resetElect) syncNodeResetElectTimer(ths);

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}
