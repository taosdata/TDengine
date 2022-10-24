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

#include "syncAppendEntries.h"
#include "syncInt.h"
#include "syncRaftCfg.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncSnapshot.h"
#include "syncUtil.h"
#include "syncVoteMgr.h"
#include "wal.h"

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

static int32_t syncNodeMakeLogSame(SSyncNode* ths, SyncAppendEntries* pMsg) {
  int32_t code;

  SyncIndex delBegin = pMsg->prevLogIndex + 1;
  SyncIndex delEnd = ths->pLogStore->syncLogLastIndex(ths->pLogStore);

  // invert roll back!
  for (SyncIndex index = delEnd; index >= delBegin; --index) {
    if (ths->pFsm->FpRollBackCb != NULL) {
      SSyncRaftEntry* pRollBackEntry;
      code = ths->pLogStore->syncLogGetEntry(ths->pLogStore, index, &pRollBackEntry);
      ASSERT(code == 0);
      ASSERT(pRollBackEntry != NULL);

      if (syncUtilUserRollback(pRollBackEntry->msgType)) {
        SRpcMsg rpcMsg;
        syncEntry2OriginalRpc(pRollBackEntry, &rpcMsg);

        SFsmCbMeta cbMeta = {0};
        cbMeta.index = pRollBackEntry->index;
        cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(ths, cbMeta.index);
        cbMeta.isWeak = pRollBackEntry->isWeak;
        cbMeta.code = 0;
        cbMeta.state = ths->state;
        cbMeta.seqNum = pRollBackEntry->seqNum;
        ths->pFsm->FpRollBackCb(ths->pFsm, &rpcMsg, cbMeta);
        rpcFreeCont(rpcMsg.pCont);
      }

      syncEntryDestory(pRollBackEntry);
    }
  }

  // delete confict entries
  code = ths->pLogStore->syncLogTruncate(ths->pLogStore, delBegin);
  ASSERT(code == 0);

  return code;
}

// if FromIndex > walCommitVer, return 0
// else return num of pass entries
static int32_t syncNodeDoMakeLogSame(SSyncNode* ths, SyncIndex FromIndex) {
  int32_t code = 0;
  int32_t pass = 0;

  SyncIndex delBegin = FromIndex;
  SyncIndex delEnd = ths->pLogStore->syncLogLastIndex(ths->pLogStore);

  // invert roll back!
  for (SyncIndex index = delEnd; index >= delBegin; --index) {
    if (ths->pFsm->FpRollBackCb != NULL) {
      SSyncRaftEntry* pRollBackEntry;
      code = ths->pLogStore->syncLogGetEntry(ths->pLogStore, index, &pRollBackEntry);
      ASSERT(code == 0);
      ASSERT(pRollBackEntry != NULL);

      if (syncUtilUserRollback(pRollBackEntry->msgType)) {
        SRpcMsg rpcMsg;
        syncEntry2OriginalRpc(pRollBackEntry, &rpcMsg);

        SFsmCbMeta cbMeta = {0};
        cbMeta.index = pRollBackEntry->index;
        cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(ths, cbMeta.index);
        cbMeta.isWeak = pRollBackEntry->isWeak;
        cbMeta.code = 0;
        cbMeta.state = ths->state;
        cbMeta.seqNum = pRollBackEntry->seqNum;
        ths->pFsm->FpRollBackCb(ths->pFsm, &rpcMsg, cbMeta);
        rpcFreeCont(rpcMsg.pCont);
      }

      syncEntryDestory(pRollBackEntry);
    }
  }

  // update delete begin
  SyncIndex walCommitVer = logStoreWalCommitVer(ths->pLogStore);

  if (delBegin <= walCommitVer) {
    delBegin = walCommitVer + 1;
    pass = walCommitVer - delBegin + 1;

    do {
      char logBuf[128];
      snprintf(logBuf, sizeof(logBuf), "update delete begin to %" PRId64, delBegin);
      syncNodeEventLog(ths, logBuf);
    } while (0);
  }

  // delete confict entries
  code = ths->pLogStore->syncLogTruncate(ths->pLogStore, delBegin);
  ASSERT(code == 0);

  do {
    char logBuf[128];
    snprintf(logBuf, sizeof(logBuf), "make log same from:%" PRId64 ", delbegin:%" PRId64 ", pass:%d", FromIndex,
             delBegin, pass);
    syncNodeEventLog(ths, logBuf);
  } while (0);

  return pass;
}

int32_t syncNodePreCommit(SSyncNode* ths, SSyncRaftEntry* pEntry, int32_t code) {
  SRpcMsg rpcMsg;
  syncEntry2OriginalRpc(pEntry, &rpcMsg);

  // leader transfer
  if (pEntry->originalRpcType == TDMT_SYNC_LEADER_TRANSFER) {
    int32_t code = syncDoLeaderTransfer(ths, &rpcMsg, pEntry);
    ASSERT(code == 0);
  }

  if (ths->pFsm != NULL) {
    if (ths->pFsm->FpPreCommitCb != NULL && syncUtilUserPreCommit(pEntry->originalRpcType)) {
      SFsmCbMeta cbMeta = {0};
      cbMeta.index = pEntry->index;
      cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(ths, cbMeta.index);
      cbMeta.isWeak = pEntry->isWeak;
      cbMeta.code = code;
      cbMeta.state = ths->state;
      cbMeta.seqNum = pEntry->seqNum;
      ths->pFsm->FpPreCommitCb(ths->pFsm, &rpcMsg, cbMeta);
    }
  }
  rpcFreeCont(rpcMsg.pCont);
  return 0;
}

static bool syncNodeOnAppendEntriesBatchLogOK(SSyncNode* pSyncNode, SyncAppendEntriesBatch* pMsg) {
  if (pMsg->prevLogIndex == SYNC_INDEX_INVALID) {
    return true;
  }

  SyncIndex myLastIndex = syncNodeGetLastIndex(pSyncNode);
  if (pMsg->prevLogIndex > myLastIndex) {
    sDebug("vgId:%d, sync log not ok, preindex:%" PRId64, pSyncNode->vgId, pMsg->prevLogIndex);
    return false;
  }

  SyncTerm myPreLogTerm = syncNodeGetPreTerm(pSyncNode, pMsg->prevLogIndex + 1);
  if (myPreLogTerm == SYNC_TERM_INVALID) {
    sDebug("vgId:%d, sync log not ok2, preindex:%" PRId64, pSyncNode->vgId, pMsg->prevLogIndex);
    return false;
  }

  if (pMsg->prevLogIndex <= myLastIndex && pMsg->prevLogTerm == myPreLogTerm) {
    return true;
  }

  sDebug("vgId:%d, sync log not ok3, preindex:%" PRId64, pSyncNode->vgId, pMsg->prevLogIndex);
  return false;
}

// really pre log match
// prevLogIndex == -1
static bool syncNodeOnAppendEntriesLogOK(SSyncNode* pSyncNode, SyncAppendEntries* pMsg) {
  if (pMsg->prevLogIndex == SYNC_INDEX_INVALID) {
    return true;
  }

  SyncIndex myLastIndex = syncNodeGetLastIndex(pSyncNode);
  if (pMsg->prevLogIndex > myLastIndex) {
    sDebug("vgId:%d, sync log not ok, preindex:%" PRId64, pSyncNode->vgId, pMsg->prevLogIndex);
    return false;
  }

  SyncTerm myPreLogTerm = syncNodeGetPreTerm(pSyncNode, pMsg->prevLogIndex + 1);
  if (myPreLogTerm == SYNC_TERM_INVALID) {
    sDebug("vgId:%d, sync log not ok2, preindex:%" PRId64, pSyncNode->vgId, pMsg->prevLogIndex);
    return false;
  }

  if (pMsg->prevLogIndex <= myLastIndex && pMsg->prevLogTerm == myPreLogTerm) {
    return true;
  }

  sDebug("vgId:%d, sync log not ok3, preindex:%" PRId64, pSyncNode->vgId, pMsg->prevLogIndex);
  return false;
}

int32_t syncNodeFollowerCommit(SSyncNode* ths, SyncIndex newCommitIndex) {
  // maybe update commit index, leader notice me
  if (newCommitIndex > ths->commitIndex) {
    // has commit entry in local
    if (newCommitIndex <= ths->pLogStore->syncLogLastIndex(ths->pLogStore)) {
      // advance commit index to sanpshot first
      SSnapshot snapshot;
      ths->pFsm->FpGetSnapshotInfo(ths->pFsm, &snapshot);
      if (snapshot.lastApplyIndex >= 0 && snapshot.lastApplyIndex > ths->commitIndex) {
        SyncIndex commitBegin = ths->commitIndex;
        SyncIndex commitEnd = snapshot.lastApplyIndex;
        ths->commitIndex = snapshot.lastApplyIndex;

        char eventLog[128];
        snprintf(eventLog, sizeof(eventLog), "commit by snapshot from index:%" PRId64 " to index:%" PRId64, commitBegin,
                 commitEnd);
        syncNodeEventLog(ths, eventLog);
      }

      SyncIndex beginIndex = ths->commitIndex + 1;
      SyncIndex endIndex = newCommitIndex;

      // update commit index
      ths->commitIndex = newCommitIndex;

      // call back Wal
      int32_t code = ths->pLogStore->syncLogUpdateCommitIndex(ths->pLogStore, ths->commitIndex);
      ASSERT(code == 0);

      code = syncNodeDoCommit(ths, beginIndex, endIndex, ths->state);
      ASSERT(code == 0);
    }
  }

  return 0;
}

int32_t syncNodeOnAppendEntries(SSyncNode* ths, SyncAppendEntries* pMsg) {
  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId))) {
    syncLogRecvAppendEntries(ths, pMsg, "not in my config");
    goto _IGNORE;
  }

  // prepare response msg
  SyncAppendEntriesReply* pReply = syncAppendEntriesReplyBuild(ths->vgId);
  pReply->srcId = ths->myRaftId;
  pReply->destId = pMsg->srcId;
  pReply->term = ths->pRaftStore->currentTerm;
  pReply->success = false;
  // pReply->matchIndex = ths->pLogStore->syncLogLastIndex(ths->pLogStore);
  pReply->matchIndex = SYNC_INDEX_INVALID;
  pReply->lastSendIndex = pMsg->prevLogIndex + 1;
  pReply->privateTerm = ths->pNewNodeReceiver->privateTerm;
  pReply->startTime = ths->startTime;

  if (pMsg->term < ths->pRaftStore->currentTerm) {
    syncLogRecvAppendEntries(ths, pMsg, "reject, small term");
    goto _SEND_RESPONSE;
  }

  if (pMsg->term > ths->pRaftStore->currentTerm) {
    pReply->term = pMsg->term;
  }

  syncNodeStepDown(ths, pMsg->term);
  syncNodeResetElectTimer(ths);

  SyncIndex startIndex = ths->pLogStore->syncLogBeginIndex(ths->pLogStore);
  SyncIndex lastIndex = ths->pLogStore->syncLogLastIndex(ths->pLogStore);

  if (pMsg->prevLogIndex > lastIndex) {
    syncLogRecvAppendEntries(ths, pMsg, "reject, index not match");
    goto _SEND_RESPONSE;
  }

  if (pMsg->prevLogIndex >= startIndex) {
    SyncTerm myPreLogTerm = syncNodeGetPreTerm(ths, pMsg->prevLogIndex + 1);
    ASSERT(myPreLogTerm != SYNC_TERM_INVALID);

    if (myPreLogTerm != pMsg->prevLogTerm) {
      syncLogRecvAppendEntries(ths, pMsg, "reject, pre-term not match");
      goto _SEND_RESPONSE;
    }
  }

  // accept
  pReply->success = true;
  bool hasAppendEntries = pMsg->dataLen > 0;
  if (hasAppendEntries) {
    SSyncRaftEntry* pAppendEntry = syncEntryDeserialize(pMsg->data, pMsg->dataLen);
    ASSERT(pAppendEntry != NULL);

    SyncIndex       appendIndex = pMsg->prevLogIndex + 1;
    SSyncRaftEntry* pLocalEntry = NULL;
    int32_t         code = ths->pLogStore->syncLogGetEntry(ths->pLogStore, appendIndex, &pLocalEntry);
    if (code == 0) {
      if (pLocalEntry->term == pAppendEntry->term) {
        // do nothing

        char logBuf[128];
        snprintf(logBuf, sizeof(logBuf), "log match, do nothing, index:%" PRId64, appendIndex);
        syncNodeEventLog(ths, logBuf);

      } else {
        // truncate
        code = ths->pLogStore->syncLogTruncate(ths->pLogStore, appendIndex);
        if (code != 0) {
          char logBuf[128];
          snprintf(logBuf, sizeof(logBuf), "ignore, truncate error, append-index:%" PRId64, appendIndex);
          syncLogRecvAppendEntries(ths, pMsg, logBuf);

          syncEntryDestory(pLocalEntry);
          syncEntryDestory(pAppendEntry);
          goto _IGNORE;
        }

        // append
        code = ths->pLogStore->syncLogAppendEntry(ths->pLogStore, pAppendEntry);
        if (code != 0) {
          char logBuf[128];
          snprintf(logBuf, sizeof(logBuf), "ignore, append error, append-index:%" PRId64, appendIndex);
          syncLogRecvAppendEntries(ths, pMsg, logBuf);

          syncEntryDestory(pLocalEntry);
          syncEntryDestory(pAppendEntry);
          goto _IGNORE;
        }
      }

    } else {
      if (terrno == TSDB_CODE_WAL_LOG_NOT_EXIST) {
        // log not exist

        // truncate
        code = ths->pLogStore->syncLogTruncate(ths->pLogStore, appendIndex);
        if (code != 0) {
          char logBuf[128];
          snprintf(logBuf, sizeof(logBuf), "ignore, log not exist, truncate error, append-index:%" PRId64, appendIndex);
          syncLogRecvAppendEntries(ths, pMsg, logBuf);

          syncEntryDestory(pLocalEntry);
          syncEntryDestory(pAppendEntry);
          goto _IGNORE;
        }

        // append
        code = ths->pLogStore->syncLogAppendEntry(ths->pLogStore, pAppendEntry);
        if (code != 0) {
          char logBuf[128];
          snprintf(logBuf, sizeof(logBuf), "ignore, log not exist, append error, append-index:%" PRId64, appendIndex);
          syncLogRecvAppendEntries(ths, pMsg, logBuf);

          syncEntryDestory(pLocalEntry);
          syncEntryDestory(pAppendEntry);
          goto _IGNORE;
        }

      } else {
        // error
        char logBuf[128];
        snprintf(logBuf, sizeof(logBuf), "ignore, get local entry error, append-index:%" PRId64, appendIndex);
        syncLogRecvAppendEntries(ths, pMsg, logBuf);

        syncEntryDestory(pLocalEntry);
        syncEntryDestory(pAppendEntry);
        goto _IGNORE;
      }
    }

    // update match index
    pReply->matchIndex = pAppendEntry->index;

    syncEntryDestory(pLocalEntry);
    syncEntryDestory(pAppendEntry);

  } else {
    // no append entries, do nothing
    // maybe has extra entries, no harm

    // update match index
    pReply->matchIndex = pMsg->prevLogIndex;
  }

  // maybe update commit index, leader notice me
  syncNodeFollowerCommit(ths, pMsg->commitIndex);

  syncLogRecvAppendEntries(ths, pMsg, "accept");
  goto _SEND_RESPONSE;

_IGNORE:
  syncAppendEntriesReplyDestroy(pReply);
  return 0;

_SEND_RESPONSE:
  // msg event log
  syncLogSendAppendEntriesReply(ths, pReply, "");

  // send response
  SRpcMsg rpcMsg;
  syncAppendEntriesReply2RpcMsg(pReply, &rpcMsg);
  syncNodeSendMsgById(&pReply->destId, ths, &rpcMsg);
  syncAppendEntriesReplyDestroy(pReply);

  return 0;
}