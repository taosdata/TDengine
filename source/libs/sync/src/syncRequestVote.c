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

#include "syncRequestVote.h"
#include "syncInt.h"
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
int32_t syncNodeOnRequestVoteCb(SSyncNode* ths, SyncRequestVote* pMsg) {
  int32_t ret = 0;

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId)) && !ths->pRaftCfg->isStandBy) {
    syncLogRecvRequestVote(ths, pMsg, "maybe replica already dropped");
    return -1;
  }

  bool logOK = (pMsg->lastLogTerm > ths->pLogStore->getLastTerm(ths->pLogStore)) ||
               ((pMsg->lastLogTerm == ths->pLogStore->getLastTerm(ths->pLogStore)) &&
                (pMsg->lastLogIndex >= ths->pLogStore->getLastIndex(ths->pLogStore)));

  // maybe update term
  if (pMsg->term > ths->pRaftStore->currentTerm) {
    syncNodeUpdateTerm(ths, pMsg->term);
#if 0  
    if (logOK) {
      syncNodeUpdateTerm(ths, pMsg->term);
    } else {
      syncNodeUpdateTermWithoutStepDown(ths, pMsg->term);
    }
#endif
  }
  ASSERT(pMsg->term <= ths->pRaftStore->currentTerm);

  bool grant = (pMsg->term == ths->pRaftStore->currentTerm) && logOK &&
               ((!raftStoreHasVoted(ths->pRaftStore)) || (syncUtilSameId(&(ths->pRaftStore->voteFor), &(pMsg->srcId))));
  if (grant) {
    // maybe has already voted for pMsg->srcId
    // vote again, no harm
    raftStoreVote(ths->pRaftStore, &(pMsg->srcId));

    // forbid elect for this round
    syncNodeResetElectTimer(ths);
  }

  // send msg
  SyncRequestVoteReply* pReply = syncRequestVoteReplyBuild(ths->vgId);
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

  SRpcMsg rpcMsg;
  syncRequestVoteReply2RpcMsg(pReply, &rpcMsg);
  syncNodeSendMsgById(&pReply->destId, ths, &rpcMsg);
  syncRequestVoteReplyDestroy(pReply);

  return ret;
}

static bool syncNodeOnRequestVoteLogOK(SSyncNode* pSyncNode, SyncRequestVote* pMsg) {
  SyncTerm  myLastTerm = syncNodeGetLastTerm(pSyncNode);
  SyncIndex myLastIndex = syncNodeGetLastIndex(pSyncNode);

  if (myLastTerm == SYNC_TERM_INVALID) {
    do {
      char logBuf[128];
      snprintf(logBuf, sizeof(logBuf),
               "logok:0, {my-lterm:%" PRIu64 ", my-lindex:%" PRId64 ", recv-lterm:%" PRIu64 ", recv-lindex:%" PRId64
               ", recv-term:%" PRIu64 "}",
               myLastTerm, myLastIndex, pMsg->lastLogTerm, pMsg->lastLogIndex, pMsg->term);
      syncNodeEventLog(pSyncNode, logBuf);
    } while (0);

    return false;
  }

  if (pMsg->lastLogTerm > myLastTerm) {
    do {
      char logBuf[128];
      snprintf(logBuf, sizeof(logBuf),
               "logok:1, {my-lterm:%" PRIu64 ", my-lindex:%" PRId64 ", recv-lterm:%" PRIu64 ", recv-lindex:%" PRId64
               ", recv-term:%" PRIu64 "}",
               myLastTerm, myLastIndex, pMsg->lastLogTerm, pMsg->lastLogIndex, pMsg->term);
      syncNodeEventLog(pSyncNode, logBuf);
    } while (0);

    return true;
  }

  if (pMsg->lastLogTerm == myLastTerm && pMsg->lastLogIndex >= myLastIndex) {
    do {
      char logBuf[128];
      snprintf(logBuf, sizeof(logBuf),
               "logok:1, {my-lterm:%" PRIu64 ", my-lindex:%" PRId64 ", recv-lterm:%" PRIu64 ", recv-lindex:%" PRId64
               ", recv-term:%" PRIu64 "}",
               myLastTerm, myLastIndex, pMsg->lastLogTerm, pMsg->lastLogIndex, pMsg->term);
      syncNodeEventLog(pSyncNode, logBuf);
    } while (0);

    return true;
  }

  do {
    char logBuf[128];
    snprintf(logBuf, sizeof(logBuf),
             "logok:0, {my-lterm:%" PRIu64 ", my-lindex:%" PRId64 ", recv-lterm:%" PRIu64 ", recv-lindex:%" PRId64
             ", recv-term:%" PRIu64 "}",
             myLastTerm, myLastIndex, pMsg->lastLogTerm, pMsg->lastLogIndex, pMsg->term);
    syncNodeEventLog(pSyncNode, logBuf);
  } while (0);

  return false;
}

int32_t syncNodeOnRequestVoteSnapshotCb(SSyncNode* ths, SyncRequestVote* pMsg) {
  int32_t ret = 0;

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId)) && !ths->pRaftCfg->isStandBy) {
    syncLogRecvRequestVote(ths, pMsg, "maybe replica already dropped");
    return -1;
  }

  bool logOK = syncNodeOnRequestVoteLogOK(ths, pMsg);

  // maybe update term
  if (pMsg->term > ths->pRaftStore->currentTerm) {
    syncNodeUpdateTerm(ths, pMsg->term);
#if 0
    if (logOK) {
      syncNodeUpdateTerm(ths, pMsg->term);
    } else {
      syncNodeUpdateTermWithoutStepDown(ths, pMsg->term);
    }
#endif
  }
  ASSERT(pMsg->term <= ths->pRaftStore->currentTerm);

  bool grant = (pMsg->term == ths->pRaftStore->currentTerm) && logOK &&
               ((!raftStoreHasVoted(ths->pRaftStore)) || (syncUtilSameId(&(ths->pRaftStore->voteFor), &(pMsg->srcId))));
  if (grant) {
    // maybe has already voted for pMsg->srcId
    // vote again, no harm
    raftStoreVote(ths->pRaftStore, &(pMsg->srcId));

    // forbid elect for this round
    syncNodeResetElectTimer(ths);
  }

  // send msg
  SyncRequestVoteReply* pReply = syncRequestVoteReplyBuild(ths->vgId);
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

  SRpcMsg rpcMsg;
  syncRequestVoteReply2RpcMsg(pReply, &rpcMsg);
  syncNodeSendMsgById(&pReply->destId, ths, &rpcMsg);
  syncRequestVoteReplyDestroy(pReply);

  return 0;
}