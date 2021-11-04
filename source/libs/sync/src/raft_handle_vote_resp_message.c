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
#include "raft_message.h"

int syncRaftHandleVoteRespMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  int granted, rejected;
  int quorum;
  int voterIndex;

  voterIndex = syncRaftConfigurationIndexOfVoter(pRaft, pMsg->from);
  if (voterIndex == -1) {
    syncError("[%d:%d] recv vote resp from unknown server %d", pRaft->selfGroupId, pRaft->selfId, pMsg->from);
    return 0;
  }

  if (pRaft->state != TAOS_SYNC_ROLE_CANDIDATE) {
    syncError("[%d:%d] is not candidate, ignore vote resp", pRaft->selfGroupId, pRaft->selfId);
    return 0;
  }

  granted = syncRaftNumOfGranted(pRaft, pMsg->from, 
                                pMsg->voteResp.cType == SYNC_RAFT_CAMPAIGN_PRE_ELECTION,
                                !pMsg->voteResp.rejected, &rejected);
  quorum = syncRaftQuorum(pRaft);

  syncInfo("[%d:%d] [quorum:%d] has received %d votes and %d vote rejections",
    pRaft->selfGroupId, pRaft->selfId, quorum, granted, rejected);

  if (granted >= quorum) {
    if (pMsg->voteResp.cType == SYNC_RAFT_CAMPAIGN_PRE_ELECTION) {
      syncRaftStartElection(pRaft, SYNC_RAFT_CAMPAIGN_ELECTION);
    } else {
      syncRaftBecomeLeader(pRaft);
      syncRaftTriggerReplicate(pRaft);
    }

    return 0;
  } else if (rejected == quorum) {
    syncRaftBecomeFollower(pRaft, pRaft->term, SYNC_NON_NODE_ID);
  }
  return 0;
}