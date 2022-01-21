/*
 * Copyright (c) 2019 TAOS Data, Inc. <cli@taosdata.com>
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

#include "syncInt.h"
#include "raft.h"
#include "sync_raft_impl.h"
#include "raft_log.h"
#include "raft_message.h"

static bool canGrantVoteMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg);

int syncRaftHandleVoteMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  SSyncMessage* pRespMsg;
  SNodeInfo* pNode = syncRaftGetNodeById(pRaft, pMsg->from);
  if (pNode == NULL) {
    return 0;
  }

  bool grant;
  SyncIndex lastIndex = syncRaftLogLastIndex(pRaft->log);
  SyncTerm lastTerm = syncRaftLogLastTerm(pRaft->log);

  grant = canGrantVoteMessage(pRaft, pMsg);
  pRespMsg = syncNewVoteRespMsg(pRaft->selfGroupId, pRaft->selfId, pMsg->vote.cType, !grant);
  if (pRespMsg == NULL) {
    return 0;
  }
  syncInfo("[%d:%d] [logterm: %" PRId64 ", index: %" PRId64 ", vote: %d] %s for %d"    
    "[logterm: %" PRId64 ", index: %" PRId64 "] at term %" PRId64 "",
    pRaft->selfGroupId, pRaft->selfId, lastTerm, lastIndex, pRaft->voteFor,
    grant ? "grant" : "reject",
    pMsg->from, pMsg->vote.lastTerm, pMsg->vote.lastIndex, pRaft->term);

  pRaft->io.send(pRespMsg, pNode);
  return 0;
}

static bool canGrantVoteMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  bool canVote = 
                  // We can vote if this is a repeat of a vote we've already cast...
                 pRaft->voteFor == pMsg->from ||
                  // ...we haven't voted and we don't think there's a leader yet in this term...
                 (pRaft->voteFor == SYNC_NON_NODE_ID && pRaft->leaderId == SYNC_NON_NODE_ID) ||
                  // ...or this is a PreVote for a future term...
                 (pMsg->vote.cType == SYNC_RAFT_CAMPAIGN_PRE_ELECTION && pMsg->term > pRaft->term);

  // ...and we believe the candidate is up to date.
  return canVote && syncRaftLogIsUptodate(pRaft->log, pMsg->vote.lastIndex, pMsg->vote.lastTerm);
}