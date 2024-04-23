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

#ifndef _TD_LIBS_SYNC_REPLICATION_H
#define _TD_LIBS_SYNC_REPLICATION_H

#ifdef __cplusplus
extern "C" {
#endif

#include "syncInt.h"

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

int32_t syncNodeHeartbeatPeers(SSyncNode* pSyncNode);
int32_t syncNodeSendHeartbeat(SSyncNode* pSyncNode, const SRaftId* pDestId, SRpcMsg* pMsg);

int32_t syncNodeReplicate(SSyncNode* pSyncNode);
int32_t syncNodeReplicateReset(SSyncNode* pSyncNode, SRaftId* pDestId);
int32_t syncNodeReplicateWithoutLock(SSyncNode* pNode);

int32_t syncNodeSendAppendEntries(SSyncNode* pNode, const SRaftId* destRaftId, SRpcMsg* pRpcMsg);

int32_t syncSnapSendMsg(SSyncSnapshotSender* pSender, int32_t seq, void* pBlock, int32_t len, int32_t typ);
int32_t syncSnapSendRsp(SSyncSnapshotReceiver* pReceiver, SyncSnapshotSend* pMsg, void* pBlock, int32_t len,
                        int32_t typ, int32_t code);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_REPLICATION_H*/
