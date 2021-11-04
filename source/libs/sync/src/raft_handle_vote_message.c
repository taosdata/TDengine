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
#include "raft_log.h"
#include "raft_message.h"

static bool canGrantVoteMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg);

int syncRaftHandleVoteMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  SSyncMessage* pRespMsg;
  int voteIndex = syncRaftConfigurationIndexOfVoter(pRaft, pMsg->from);
  if (voteIndex == -1) {
    return 0;
  }
  bool grant;
  SyncIndex lastIndex = syncRaftLogLastIndex(pRaft->log);
  SyncTerm lastTerm = syncRaftLogLastTerm(pRaft->log);

  grant = canGrantVoteMessage(pRaft, pMsg);
  pRespMsg = syncNewVoteRespMsg(pRaft->selfGroupId, pRaft->selfId, pMsg->to, pMsg->vote.cType, !grant);
  if (pRespMsg == NULL) {
    return 0;
  }
  syncInfo("[%d:%d] [logterm: %" PRId64 ", index: %" PRId64 ", vote: %d] %s for %d" \    
    "[logterm: %" PRId64 ", index: %" PRId64 ", vote: %d] at term %" PRId64 "",
    pRaft->selfGroupId, pRaft->selfId, lastTerm, lastIndex, pRaft->voteFor,
    grant ? "grant" : "reject",
    pMsg->from, pMsg->vote.lastTerm, pMsg->vote.lastIndex, pRaft->term);

  pRaft->io.send(pRespMsg, &(pRaft->cluster.nodeInfo[voteIndex]));
  return 0;
}

static bool canGrantVoteMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  if (!(pRaft->voteFor == SYNC_NON_NODE_ID || pMsg->term > pRaft->term || pRaft->voteFor == pMsg->from)) {
    return false;
  }
  if (!syncRaftLogIsUptodate(pRaft, pMsg->vote.lastIndex, pMsg->vote.lastTerm)) {
    return false;
  }

  return true;
}