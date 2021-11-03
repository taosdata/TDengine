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

static void campaign(SSyncRaft* pRaft, SyncRaftCampaignType cType);

void syncRaftHandleElectionMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  if (pRaft->state == TAOS_SYNC_ROLE_LEADER) {
    syncDebug("%d ignoring RAFT_MSG_INTERNAL_ELECTION because already leader", pRaft->selfId);
    return;
  }

  // TODO: is there pending uncommitted config?

  syncInfo("[%d:%d] is starting a new election at term %" PRId64 "", pRaft->selfGroupId, pRaft->selfId, pRaft->term);

  if (pRaft->preVote) {

  } else {

  }
}

static void campaign(SSyncRaft* pRaft, SyncRaftCampaignType cType) {
  SyncTerm term;
  RaftMessageType voteMsgType;

  if (cType == SYNC_RAFT_CAMPAIGN_PRE_ELECTION) {
    syncRaftBecomePreCandidate(pRaft);
    voteMsgType = RAFT_MSG_PRE_VOTE;
    // PreVote RPCs are sent for the next term before we've incremented r.Term.
    term = pRaft->term + 1;
  } else {
    syncRaftBecomeCandidate(pRaft);
    voteMsgType = RAFT_MSG_VOTE;
    term = pRaft->term;
  }

  int quorum = syncRaftQuorum(pRaft);
  int granted = syncRaftNumOfGranted(pRaft, pRaft->selfId, SyncRaftVoteRespMsgType(voteMsgType), true);
  if (quorum <= granted) {
		/**
     * We won the election after voting for ourselves (which must mean that
		 * this is a single-node cluster). Advance to the next state.
     **/
    if (cType == SYNC_RAFT_CAMPAIGN_PRE_ELECTION) {
      campaign(pRaft, SYNC_RAFT_CAMPAIGN_ELECTION);
    } else {
      syncRaftBecomeLeader(pRaft);
    }
    return;
  }

  // broadcast vote message to other peers

}