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

  syncRequestVoteLog2("==syncNodeOnRequestVoteCb==", pMsg);

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId)) && !ths->pRaftCfg->isStandBy) {
    do {
      char     logBuf[256];
      char     host[64];
      uint16_t port;
      syncUtilU642Addr(pMsg->srcId.addr, host, sizeof(host), &port);
      snprintf(logBuf, sizeof(logBuf),
               "recv sync-request-vote from %s:%d, term:%" PRIu64 ", lindex:%" PRId64 ", lterm:%" PRIu64 ", maybe replica already dropped",
               host, port, pMsg->term, pMsg->lastLogIndex, pMsg->lastLogTerm);
      syncNodeEventLog(ths, logBuf);
    } while (0);

    return -1;
  }

  // maybe update term
  if (pMsg->term > ths->pRaftStore->currentTerm) {
    syncNodeUpdateTerm(ths, pMsg->term);
  }
  ASSERT(pMsg->term <= ths->pRaftStore->currentTerm);

  bool logOK = (pMsg->lastLogTerm > ths->pLogStore->getLastTerm(ths->pLogStore)) ||
               ((pMsg->lastLogTerm == ths->pLogStore->getLastTerm(ths->pLogStore)) &&
                (pMsg->lastLogIndex >= ths->pLogStore->getLastIndex(ths->pLogStore)));
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
    char     logBuf[256];
    char     host[64];
    uint16_t port;
    syncUtilU642Addr(pMsg->srcId.addr, host, sizeof(host), &port);
    snprintf(logBuf, sizeof(logBuf),
             "recv sync-request-vote from %s:%d, term:%" PRIu64 ", lindex:%" PRId64 ", lterm:%" PRIu64 ", reply-grant:%d", host, port,
             pMsg->term, pMsg->lastLogIndex, pMsg->lastLogTerm, pReply->voteGranted);
    syncNodeEventLog(ths, logBuf);
  } while (0);

  SRpcMsg rpcMsg;
  syncRequestVoteReply2RpcMsg(pReply, &rpcMsg);
  syncNodeSendMsgById(&pReply->destId, ths, &rpcMsg);
  syncRequestVoteReplyDestroy(pReply);

  return ret;
}

#if 0
int32_t syncNodeOnRequestVoteCb(SSyncNode* ths, SyncRequestVote* pMsg) {
  int32_t ret = 0;

  char logBuf[128] = {0};
  snprintf(logBuf, sizeof(logBuf), "==syncNodeOnRequestVoteCb== term:%" PRIu64, ths->pRaftStore->currentTerm);
  syncRequestVoteLog2(logBuf, pMsg);

  if (pMsg->term > ths->pRaftStore->currentTerm) {
    syncNodeUpdateTerm(ths, pMsg->term);
  }
  ASSERT(pMsg->term <= ths->pRaftStore->currentTerm);

  bool logOK = (pMsg->lastLogTerm > ths->pLogStore->getLastTerm(ths->pLogStore)) ||
               ((pMsg->lastLogTerm == ths->pLogStore->getLastTerm(ths->pLogStore)) &&
                (pMsg->lastLogIndex >= ths->pLogStore->getLastIndex(ths->pLogStore)));
  bool grant = (pMsg->term == ths->pRaftStore->currentTerm) && logOK &&
               ((!raftStoreHasVoted(ths->pRaftStore)) || (syncUtilSameId(&(ths->pRaftStore->voteFor), &(pMsg->srcId))));
  if (grant) {
    // maybe has already voted for pMsg->srcId
    // vote again, no harm
    raftStoreVote(ths->pRaftStore, &(pMsg->srcId));

    // forbid elect for this round
    syncNodeResetElectTimer(ths);
  }

  SyncRequestVoteReply* pReply = syncRequestVoteReplyBuild(ths->vgId);
  pReply->srcId = ths->myRaftId;
  pReply->destId = pMsg->srcId;
  pReply->term = ths->pRaftStore->currentTerm;
  pReply->voteGranted = grant;

  SRpcMsg rpcMsg;
  syncRequestVoteReply2RpcMsg(pReply, &rpcMsg);
  syncNodeSendMsgById(&pReply->destId, ths, &rpcMsg);
  syncRequestVoteReplyDestroy(pReply);

  return ret;
}
#endif

static bool syncNodeOnRequestVoteLogOK(SSyncNode* pSyncNode, SyncRequestVote* pMsg) {
  SyncTerm  myLastTerm = syncNodeGetLastTerm(pSyncNode);
  SyncIndex myLastIndex = syncNodeGetLastIndex(pSyncNode);

  if (myLastTerm == SYNC_TERM_INVALID) {
    return false;
  }

  if (pMsg->lastLogTerm > myLastTerm) {
    return true;
  }
  if (pMsg->lastLogTerm == myLastTerm && pMsg->lastLogIndex >= myLastIndex) {
    return true;
  }

  return false;
}

int32_t syncNodeOnRequestVoteSnapshotCb(SSyncNode* ths, SyncRequestVote* pMsg) {
  int32_t ret = 0;

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId)) && !ths->pRaftCfg->isStandBy) {
    do {
      char     logBuf[256];
      char     host[64];
      uint16_t port;
      syncUtilU642Addr(pMsg->srcId.addr, host, sizeof(host), &port);
      snprintf(logBuf, sizeof(logBuf),
               "recv sync-request-vote from %s:%d, term:%" PRIu64 ", lindex:%" PRId64 ", lterm:%" PRIu64 ", maybe replica already dropped",
               host, port, pMsg->term, pMsg->lastLogIndex, pMsg->lastLogTerm);
      syncNodeEventLog(ths, logBuf);
    } while (0);

    return -1;
  }

  // maybe update term
  if (pMsg->term > ths->pRaftStore->currentTerm) {
    syncNodeUpdateTerm(ths, pMsg->term);
  }
  ASSERT(pMsg->term <= ths->pRaftStore->currentTerm);

  bool logOK = syncNodeOnRequestVoteLogOK(ths, pMsg);
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
    char     logBuf[256];
    char     host[64];
    uint16_t port;
    syncUtilU642Addr(pMsg->srcId.addr, host, sizeof(host), &port);
    snprintf(logBuf, sizeof(logBuf),
             "recv sync-request-vote from %s:%d, term:%" PRIu64 ", lindex:%" PRId64 ", lterm:%" PRIu64 ", reply-grant:%d", host, port,
             pMsg->term, pMsg->lastLogIndex, pMsg->lastLogTerm, pReply->voteGranted);
    syncNodeEventLog(ths, logBuf);
  } while (0);

  SRpcMsg rpcMsg;
  syncRequestVoteReply2RpcMsg(pReply, &rpcMsg);
  syncNodeSendMsgById(&pReply->destId, ths, &rpcMsg);
  syncRequestVoteReplyDestroy(pReply);

  return 0;
}