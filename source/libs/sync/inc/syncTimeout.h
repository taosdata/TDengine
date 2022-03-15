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

#ifndef _TD_LIBS_SYNC_TIMEOUT_H
#define _TD_LIBS_SYNC_TIMEOUT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "syncInt.h"
#include "syncMessage.h"
#include "taosdef.h"

// TLA+ Spec
// Timeout(i) == /\ state[i] \in {Follower, Candidate}
//               /\ state' = [state EXCEPT ![i] = Candidate]
//               /\ currentTerm' = [currentTerm EXCEPT ![i] = currentTerm[i] + 1]
//               \* Most implementations would probably just set the local vote
//               \* atomically, but messaging localhost for it is weaker.
//               /\ votedFor' = [votedFor EXCEPT ![i] = Nil]
//               /\ votesResponded' = [votesResponded EXCEPT ![i] = {}]
//               /\ votesGranted'   = [votesGranted EXCEPT ![i] = {}]
//               /\ voterLog'       = [voterLog EXCEPT ![i] = [j \in {} |-> <<>>]]
//               /\ UNCHANGED <<messages, leaderVars, logVars>>
//
int32_t syncNodeOnTimeoutCb(SSyncNode* ths, SyncTimeout* pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_TIMEOUT_H*/
