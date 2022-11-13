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

static bool syncNodeOnRequestVoteLogOK(SSyncNode* pSyncNode, SyncRequestVote* pMsg) {
  SyncTerm  myLastTerm = syncNodeGetLastTerm(pSyncNode);
  SyncIndex myLastIndex = syncNodeGetLastIndex(pSyncNode);

  if (pMsg->lastLogIndex < pSyncNode->commitIndex) {
    sNTrace(pSyncNode,
            "logok:0, {my-lterm:%" PRIu64 ", my-lindex:%" PRId64 ", recv-lterm:%" PRIu64 ", recv-lindex:%" PRId64
            ", recv-term:%" PRIu64 "}",
            myLastTerm, myLastIndex, pMsg->lastLogTerm, pMsg->lastLogIndex, pMsg->term);

    return false;
  }

  if (myLastTerm == SYNC_TERM_INVALID) {
    sNTrace(pSyncNode,
            "logok:0, {my-lterm:%" PRIu64 ", my-lindex:%" PRId64 ", recv-lterm:%" PRIu64 ", recv-lindex:%" PRId64
            ", recv-term:%" PRIu64 "}",
            myLastTerm, myLastIndex, pMsg->lastLogTerm, pMsg->lastLogIndex, pMsg->term);
    return false;
  }

  if (pMsg->lastLogTerm > myLastTerm) {
    sNTrace(pSyncNode,
            "logok:1, {my-lterm:%" PRIu64 ", my-lindex:%" PRId64 ", recv-lterm:%" PRIu64 ", recv-lindex:%" PRId64
            ", recv-term:%" PRIu64 "}",
            myLastTerm, myLastIndex, pMsg->lastLogTerm, pMsg->lastLogIndex, pMsg->term);
    return true;
  }

  if (pMsg->lastLogTerm == myLastTerm && pMsg->lastLogIndex >= myLastIndex) {
    sNTrace(pSyncNode,
            "logok:1, {my-lterm:%" PRIu64 ", my-lindex:%" PRId64 ", recv-lterm:%" PRIu64 ", recv-lindex:%" PRId64
            ", recv-term:%" PRIu64 "}",
            myLastTerm, myLastIndex, pMsg->lastLogTerm, pMsg->lastLogIndex, pMsg->term);
    return true;
  }

  sNTrace(pSyncNode,
          "logok:0, {my-lterm:%" PRIu64 ", my-lindex:%" PRId64 ", recv-lterm:%" PRIu64 ", recv-lindex:%" PRId64
          ", recv-term:%" PRIu64 "}",
          myLastTerm, myLastIndex, pMsg->lastLogTerm, pMsg->lastLogIndex, pMsg->term);
  return false;
}

static void syncLogRecvRequestVote(SSyncNode* pSyncNode, const SyncRequestVote* pMsg, const char* s) {
  char     logBuf[256];
  char     host[64];
  uint16_t port;
  syncUtilU642Addr(pMsg->srcId.addr, host, sizeof(host), &port);
  sNTrace(pSyncNode, "recv sync-request-vote from %s:%d, {term:%" PRId64 ", lindex:%" PRId64 ", lterm:%" PRId64 "}, %s",
          host, port, pMsg->term, pMsg->lastLogIndex, pMsg->lastLogTerm, s);
}

static void syncLogSendRequestVoteReply(SSyncNode* pSyncNode, const SyncRequestVoteReply* pMsg, const char* s) {
  char     host[64];
  uint16_t port;
  syncUtilU642Addr(pMsg->destId.addr, host, sizeof(host), &port);
  sNTrace(pSyncNode, "send sync-request-vote-reply to %s:%d {term:%" PRId64 ", grant:%d}, %s", host, port, pMsg->term,
          pMsg->voteGranted, s);
}

int32_t syncNodeOnRequestVote(SSyncNode* ths, const SRpcMsg* pRpcMsg) {
  int32_t          ret = 0;
  SyncRequestVote* pMsg = pRpcMsg->pCont;

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId))) {
    syncLogRecvRequestVote(ths, pMsg, "not in my config");
    return -1;
  }

  bool logOK = syncNodeOnRequestVoteLogOK(ths, pMsg);

  // maybe update term
  if (pMsg->term > ths->pRaftStore->currentTerm) {
    syncNodeStepDown(ths, pMsg->term);
    // syncNodeUpdateTerm(ths, pMsg->term);
  }
  ASSERT(pMsg->term <= ths->pRaftStore->currentTerm);

  bool grant = (pMsg->term == ths->pRaftStore->currentTerm) && logOK &&
               ((!raftStoreHasVoted(ths->pRaftStore)) || (syncUtilSameId(&(ths->pRaftStore->voteFor), &(pMsg->srcId))));
  if (grant) {
    // maybe has already voted for pMsg->srcId
    // vote again, no harm
    raftStoreVote(ths->pRaftStore, &(pMsg->srcId));

    // candidate ?
    syncNodeStepDown(ths, ths->pRaftStore->currentTerm);

    // forbid elect for this round
    syncNodeResetElectTimer(ths);
  }

  // send msg
  SRpcMsg rpcMsg = {0};
  ret = syncBuildRequestVoteReply(&rpcMsg, ths->vgId);
  ASSERT(ret == 0 );

  SyncRequestVoteReply* pReply = rpcMsg.pCont;
  pReply->srcId = ths->myRaftId;
  pReply->destId = pMsg->srcId;
  pReply->term = ths->pRaftStore->currentTerm;
  pReply->voteGranted = grant;

  // trace log
  do {
    char logBuf[32];
    snprintf(logBuf, sizeof(logBuf), "grant:%d", pReply->voteGranted);
    syncLogRecvRequestVote(ths, pMsg, logBuf);
    syncLogSendRequestVoteReply(ths, pReply, "");
  } while (0);

  syncNodeSendMsgById(&pReply->destId, ths, &rpcMsg);
  return 0;
}