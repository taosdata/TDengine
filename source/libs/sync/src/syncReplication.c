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
#include "syncReplication.h"
#include "syncIndexMgr.h"
#include "syncPipeline.h"
#include "syncRaftEntry.h"
#include "syncRaftStore.h"
#include "syncUtil.h"

// TLA+ Spec
// AppendEntries(i, j) ==
//    /\ i /= j
//    /\ state[i] = Leader
//    /\ LET prevLogIndex == nextIndex[i][j] - 1
//           prevLogTerm == IF prevLogIndex > 0 THEN
//                              log[i][prevLogIndex].term
//                          ELSE
//                              0
//           \* Send up to 1 entry, constrained by the end of the log.
//           lastEntry == Min({Len(log[i]), nextIndex[i][j]})
//           entries == SubSeq(log[i], nextIndex[i][j], lastEntry)
//       IN Send([mtype          |-> AppendEntriesRequest,
//                mterm          |-> currentTerm[i],
//                mprevLogIndex  |-> prevLogIndex,
//                mprevLogTerm   |-> prevLogTerm,
//                mentries       |-> entries,
//                \* mlog is used as a history variable for the proof.
//                \* It would not exist in a real implementation.
//                mlog           |-> log[i],
//                mcommitIndex   |-> Min({commitIndex[i], lastEntry}),
//                msource        |-> i,
//                mdest          |-> j])
//    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars>>

int32_t syncNodeReplicateReset(SSyncNode* pNode, SRaftId* pDestId) {
  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  (void)taosThreadMutexLock(&pBuf->mutex);
  SSyncLogReplMgr* pMgr = syncNodeGetLogReplMgr(pNode, pDestId);
  syncLogReplReset(pMgr);
  (void)taosThreadMutexUnlock(&pBuf->mutex);

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t syncNodeReplicate(SSyncNode* pNode) {
  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  int32_t         ret = 0;
  if ((ret = taosThreadMutexTryLock(&pBuf->mutex)) != 0) {
    return ret;
  }
  ret = syncNodeReplicateWithoutLock(pNode);
  (void)taosThreadMutexUnlock(&pBuf->mutex);

  TAOS_RETURN(ret);
}

int32_t syncNodeReplicateWithoutLock(SSyncNode* pNode) {
  if ((pNode->state != TAOS_SYNC_STATE_LEADER && pNode->state != TAOS_SYNC_STATE_ASSIGNED_LEADER) ||
      pNode->raftCfg.cfg.totalReplicaNum == 1) {
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }
  for (int32_t i = 0; i < pNode->totalReplicaNum; i++) {
    if (syncUtilSameId(&pNode->replicasId[i], &pNode->myRaftId)) {
      continue;
    }
    SSyncLogReplMgr* pMgr = pNode->logReplMgrs[i];
    int32_t          ret = 0;
    if ((ret = syncLogReplStart(pMgr, pNode)) != 0) {
      sWarn("vgId:%d, failed to start log replication to dnode:%d since %s", pNode->vgId, DID(&(pNode->replicasId[i])),
            tstrerror(ret));
    }
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t syncNodeSendAppendEntries(SSyncNode* pSyncNode, const SRaftId* destRaftId, SRpcMsg* pRpcMsg) {
  SyncAppendEntries* pMsg = pRpcMsg->pCont;
  pMsg->destId = *destRaftId;
  TAOS_CHECK_RETURN(syncNodeSendMsgById(destRaftId, pSyncNode, pRpcMsg));

  int64_t nRef = 0;
  if (pSyncNode != NULL) {
    nRef = atomic_add_fetch_64(&pSyncNode->sendCount, 1);
    if (nRef <= 0) {
      sError("vgId:%d, send count is %" PRId64, pSyncNode->vgId, nRef);
    }
  }

  SSyncLogReplMgr* mgr = syncNodeGetLogReplMgr(pSyncNode, (SRaftId*)destRaftId);
  if (mgr != NULL) {
    nRef = atomic_add_fetch_64(&mgr->sendCount, 1);
    if (nRef <= 0) {
      sError("vgId:%d, send count is %" PRId64, pSyncNode->vgId, nRef);
    }
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t syncNodeSendHeartbeat(SSyncNode* pSyncNode, const SRaftId* destId, SRpcMsg* pMsg) {
  SRaftId destIdTmp = *destId;
  TAOS_CHECK_RETURN(syncNodeSendMsgById(destId, pSyncNode, pMsg));

  int64_t tsMs = taosGetTimestampMs();
  syncIndexMgrSetSentTime(pSyncNode->pMatchIndex, &destIdTmp, tsMs);

  return TSDB_CODE_SUCCESS;
}

int32_t syncNodeHeartbeatPeers(SSyncNode* pSyncNode) {
  int64_t ts = taosGetTimestampMs();
  for (int32_t i = 0; i < pSyncNode->peersNum; ++i) {
    SRpcMsg rpcMsg = {0};
    if (syncBuildHeartbeat(&rpcMsg, pSyncNode->vgId) != 0) {
      sError("vgId:%d, build sync-heartbeat error", pSyncNode->vgId);
      continue;
    }

    SyncHeartbeat* pSyncMsg = rpcMsg.pCont;
    pSyncMsg->srcId = pSyncNode->myRaftId;
    pSyncMsg->destId = pSyncNode->peersId[i];
    pSyncMsg->term = raftStoreGetTerm(pSyncNode);
    pSyncMsg->commitIndex = pSyncNode->commitIndex;
    pSyncMsg->minMatchIndex = syncMinMatchIndex(pSyncNode);
    pSyncMsg->privateTerm = 0;
    pSyncMsg->timeStamp = ts;

    // send msg
    TRACE_SET_MSGID(&(rpcMsg.info.traceId), tGenIdPI64());
    sGTrace(&rpcMsg.info.traceId, "vgId:%d, send sync-heartbeat to dnode:%d", pSyncNode->vgId, DID(&(pSyncMsg->destId)));
    syncLogSendHeartbeat(pSyncNode, pSyncMsg, true, 0, 0);
    int32_t ret = syncNodeSendHeartbeat(pSyncNode, &pSyncMsg->destId, &rpcMsg);
    if (ret != 0) {
      sError("vgId:%d, failed to send sync-heartbeat since %s", pSyncNode->vgId, tstrerror(ret));
    }
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}
