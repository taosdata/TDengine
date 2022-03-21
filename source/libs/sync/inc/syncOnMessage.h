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

#ifndef _TD_LIBS_SYNC_ON_MESSAGE_H
#define _TD_LIBS_SYNC_ON_MESSAGE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "taosdef.h"

// TLA+ Spec
// Receive(m) ==
//     LET i == m.mdest
//         j == m.msource
//     IN \* Any RPC with a newer term causes the recipient to advance
//        \* its term first. Responses with stale terms are ignored.
//        \/ UpdateTerm(i, j, m)
//        \/ /\ m.mtype = RequestVoteRequest
//           /\ HandleRequestVoteRequest(i, j, m)
//        \/ /\ m.mtype = RequestVoteResponse
//           /\ \/ DropStaleResponse(i, j, m)
//              \/ HandleRequestVoteResponse(i, j, m)
//        \/ /\ m.mtype = AppendEntriesRequest
//           /\ HandleAppendEntriesRequest(i, j, m)
//        \/ /\ m.mtype = AppendEntriesResponse
//           /\ \/ DropStaleResponse(i, j, m)
//              \/ HandleAppendEntriesResponse(i, j, m)

// DuplicateMessage(m) ==
//     /\ Send(m)
//     /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars>>

// DropMessage(m) ==
//     /\ Discard(m)
//     /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars>>

// Next == /\ \/ \E i \in Server : Restart(i)
//            \/ \E i \in Server : Timeout(i)
//            \/ \E i,j \in Server : RequestVote(i, j)
//            \/ \E i \in Server : BecomeLeader(i)
//            \/ \E i \in Server, v \in Value : ClientRequest(i, v)
//            \/ \E i \in Server : AdvanceCommitIndex(i)
//            \/ \E i,j \in Server : AppendEntries(i, j)
//            \/ \E m \in DOMAIN messages : Receive(m)
//            \/ \E m \in DOMAIN messages : DuplicateMessage(m)
//            \/ \E m \in DOMAIN messages : DropMessage(m)
//            \* History variable that tracks every log ever:
//         /\ allLogs' = allLogs \cup {log[i] : i \in Server}
//

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_ON_MESSAGE_H*/
