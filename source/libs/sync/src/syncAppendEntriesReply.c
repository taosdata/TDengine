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
    do {
      char logBuf[128];
      snprintf(logBuf, sizeof(logBuf), "snapshot param error, start:%" PRId64 ", end:%" PRId64, beginIndex, endIndex);
      syncNodeErrorLog(ths, logBuf);
    } while (0);

    return;
  }

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

int64_t syncNodeUpdateCommitIndex(SSyncNode* ths, SyncIndex commitIndex) {
  SyncIndex lastVer = ths->pLogStore->syncLogLastIndex(ths->pLogStore);
  commitIndex = TMAX(commitIndex, ths->commitIndex);
  ths->commitIndex = TMIN(commitIndex, lastVer);
  ths->pLogStore->syncLogUpdateCommitIndex(ths->pLogStore, ths->commitIndex);
  return ths->commitIndex;
}

int64_t syncNodeCheckCommitIndex(SSyncNode* ths, SyncIndex indexLikely) {
  if (indexLikely > ths->commitIndex && syncNodeAgreedUpon(ths, indexLikely)) {
    SyncIndex commitIndex = indexLikely;
    syncNodeUpdateCommitIndex(ths, commitIndex);
    sDebug("vgId:%d, agreed upon. role:%d, term:%" PRId64 ", index: %" PRId64 "", ths->vgId, ths->state,
           ths->pRaftStore->currentTerm, commitIndex);
  }
  return ths->commitIndex;
}

SSyncRaftEntry* syncLogBufferGetOneEntry(SSyncLogBuffer* pBuf, SSyncNode* pNode, SyncIndex index, bool* pInBuf) {
  SSyncRaftEntry* pEntry = NULL;
  if (index >= pBuf->endIndex) {
    return NULL;
  }
  if (index > pBuf->startIndex) {  // startIndex might be dummy
    *pInBuf = true;
    pEntry = pBuf->entries[index % pBuf->size].pItem;
  } else {
    *pInBuf = false;
    if (pNode->pLogStore->syncLogGetEntry(pNode->pLogStore, index, &pEntry) < 0) {
      sError("vgId:%d, failed to get log entry since %s. index:%" PRId64 "", pNode->vgId, terrstr(), index);
    }
  }
  return pEntry;
}

bool syncLogReplMgrValidate(SSyncLogReplMgr* pMgr) {
  ASSERT(pMgr->startIndex <= pMgr->endIndex);
  for (SyncIndex index = pMgr->startIndex; index < pMgr->endIndex; index++) {
    ASSERT(pMgr->states[(index + pMgr->size) % pMgr->size].barrier == false || index + 1 == pMgr->endIndex);
  }
  return true;
}

static FORCE_INLINE bool syncLogIsReplicationBarrier(SSyncRaftEntry* pEntry) {
  return pEntry->originalRpcType == TDMT_SYNC_NOOP;
}

int32_t syncLogBufferReplicateOneTo(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncIndex index, SyncTerm* pTerm,
                                    SRaftId* pDestId, bool* pBarrier) {
  SSyncRaftEntry*    pEntry = NULL;
  SyncAppendEntries* pMsgOut = NULL;
  bool               inBuf = false;
  int32_t            ret = -1;
  SyncTerm           prevLogTerm = -1;
  SSyncLogBuffer*    pBuf = pNode->pLogBuf;

  pEntry = syncLogBufferGetOneEntry(pBuf, pNode, index, &inBuf);
  if (pEntry == NULL) {
    sError("vgId:%d, failed to get raft entry for index: %" PRId64 "", pNode->vgId, index);
    goto _out;
  }
  *pBarrier = syncLogIsReplicationBarrier(pEntry);

  prevLogTerm = syncLogReplMgrGetPrevLogTerm(pMgr, pNode, index);
  if (prevLogTerm < 0 && terrno != TSDB_CODE_SUCCESS) {
    sError("vgId:%d, failed to get prev log term since %s. index: %" PRId64 "", pNode->vgId, terrstr(), index);
    goto _out;
  }
  if (pTerm) *pTerm = pEntry->term;

  pMsgOut = syncLogToAppendEntries(pNode, pEntry, prevLogTerm);
  if (pMsgOut == NULL) {
    sError("vgId:%d, failed to get append entries for index:%" PRId64 "", pNode->vgId, index);
    goto _out;
  }

  (void)syncNodeSendAppendEntries(pNode, pDestId, pMsgOut);
  ret = 0;

  sInfo("vgId:%d, replicate one msg index: %" PRId64 " term: %" PRId64 " prevterm: %" PRId64 " to dest: 0x%016" PRIx64,
        pNode->vgId, pEntry->index, pEntry->term, prevLogTerm, pDestId->addr);

_out:
  syncAppendEntriesDestroy(pMsgOut);
  pMsgOut = NULL;
  if (!inBuf) {
    syncEntryDestroy(pEntry);
    pEntry = NULL;
  }
  return ret;
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

    sDebug("vgId:%d received append entries reply. srcId:0x%016" PRIx64 ",  term:%" PRId64 ", matchIndex:%" PRId64 "",
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
    // ASSERT(pMgr != NULL);
    if (pMgr != NULL) {
      (void)syncLogReplMgrProcessReply(pMgr, ths, pMsg);
    }
  }
  return 0;
}

int32_t syncNodeOnAppendEntriesReplyOld(SSyncNode* ths, SyncAppendEntriesReply* pMsg) {
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
