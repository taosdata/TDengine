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
#include "raft_message.h"

int syncRaftHandleVoteRespMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  int granted, rejected;
  int quorum;
  int voterIndex;

  assert(pRaft->state == TAOS_SYNC_STATE_CANDIDATE);

  SNodeInfo* pNode = syncRaftGetNodeById(pRaft, pMsg->from);
  if (pNode == NULL) {
    syncError("[%d:%d] recv vote resp from unknown server %d", pRaft->selfGroupId, pRaft->selfId, pMsg->from);
    return 0;
  }

  if (pRaft->state != TAOS_SYNC_STATE_CANDIDATE) {
    syncError("[%d:%d] is not candidate, ignore vote resp", pRaft->selfGroupId, pRaft->selfId);
    return 0;
  }

  ESyncRaftVoteResult result = syncRaftPollVote(pRaft, pMsg->from, 
                                pMsg->voteResp.cType == SYNC_RAFT_CAMPAIGN_PRE_ELECTION,
                                !pMsg->voteResp.rejected, &rejected, &granted);

  syncInfo("[%d:%d] [quorum:%d] has received %d votes and %d vote rejections",
    pRaft->selfGroupId, pRaft->selfId, quorum, granted, rejected);

  if (result == SYNC_RAFT_VOTE_WON) {
    if (pRaft->candidateState.inPreVote) {
      syncRaftCampaign(pRaft, SYNC_RAFT_CAMPAIGN_ELECTION);
    } else {
      syncRaftBecomeLeader(pRaft);
      syncRaftBroadcastAppend(pRaft);
    }
  } else if (result == SYNC_RAFT_VOTE_LOST) {
		// pb.MsgPreVoteResp contains future term of pre-candidate
		// m.Term > r.Term; reuse r.Term    
    syncRaftBecomeFollower(pRaft, pRaft->term, SYNC_NON_NODE_ID);
  }

  return 0;
}