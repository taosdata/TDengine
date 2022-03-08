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

#include "syncRaftLog.h"

int32_t raftLogAppendEntry(struct SSyncLogStore* pLogStore, SSyncBuffer* pBuf) { return 0; }

// get one log entry, user need to free pBuf->data
int32_t raftLogGetEntry(struct SSyncLogStore* pLogStore, SyncIndex index, SSyncBuffer* pBuf) { return 0; }

// TLA+ Spec
// \* Leader i advances its commitIndex.
// \* This is done as a separate step from handling AppendEntries responses,
// \* in part to minimize atomic regions, and in part so that leaders of
// \* single-server clusters are able to mark entries committed.
// AdvanceCommitIndex(i) ==
//     /\ state[i] = Leader
//     /\ LET \* The set of servers that agree up through index.
//            Agree(index) == {i} \cup {k \in Server :
//                                          matchIndex[i][k] >= index}
//            \* The maximum indexes for which a quorum agrees
//            agreeIndexes == {index \in 1..Len(log[i]) :
//                                 Agree(index) \in Quorum}
//            \* New value for commitIndex'[i]
//            newCommitIndex ==
//               IF /\ agreeIndexes /= {}
//                  /\ log[i][Max(agreeIndexes)].term = currentTerm[i]
//               THEN
//                   Max(agreeIndexes)
//               ELSE
//                   commitIndex[i]
//        IN commitIndex' = [commitIndex EXCEPT ![i] = newCommitIndex]
//     /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, log>>
//
int32_t raftLogupdateCommitIndex(struct SSyncLogStore* pLogStore, SyncIndex index) { return 0; }

// truncate log with index, entries after the given index (>index) will be deleted
int32_t raftLogTruncate(struct SSyncLogStore* pLogStore, SyncIndex index) { return 0; }

// return commit index of log
SyncIndex raftLogGetCommitIndex(struct SSyncLogStore* pLogStore) { return 0; }

// return index of last entry
SyncIndex raftLogGetLastIndex(struct SSyncLogStore* pLogStore) { return 0; }

// return term of last entry
SyncTerm raftLogGetLastTerm(struct SSyncLogStore* pLogStore) { return 0; }