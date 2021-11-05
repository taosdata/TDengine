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

void syncRaftStartElection(SSyncRaft* pRaft, SyncRaftElectionType cType) {
  SyncTerm term;
  bool preVote;
  RaftMessageType voteMsgType;

  if (cType == SYNC_RAFT_CAMPAIGN_PRE_ELECTION) {
    syncRaftBecomePreCandidate(pRaft);
    preVote = true;
    // PreVote RPCs are sent for the next term before we've incremented r.Term.
    term = pRaft->term + 1;
  } else {
    syncRaftBecomeCandidate(pRaft);
    voteMsgType = RAFT_MSG_VOTE;
    term = pRaft->term;
    preVote = false;
  }

  int quorum = syncRaftQuorum(pRaft);
  int granted = syncRaftNumOfGranted(pRaft, pRaft->selfId, preVote, true, NULL);
  if (quorum <= granted) {
		/**
     * We won the election after voting for ourselves (which must mean that
		 * this is a single-node cluster). Advance to the next state.
     **/
    if (cType == SYNC_RAFT_CAMPAIGN_PRE_ELECTION) {
      syncRaftStartElection(pRaft, SYNC_RAFT_CAMPAIGN_ELECTION);
    } else {
      syncRaftBecomeLeader(pRaft);
    }
    return;
  }

  // broadcast vote message to other peers
  int i;
  SyncIndex lastIndex = syncRaftLogLastIndex(pRaft->log);
  SyncTerm lastTerm = syncRaftLogLastTerm(pRaft->log);
  for (i = 0; i < pRaft->cluster.replica; ++i) {
    if (i == pRaft->cluster.selfIndex) {
      continue;
    }

    SyncNodeId nodeId = pRaft->cluster.nodeInfo[i].nodeId;

    SSyncMessage* pMsg = syncNewVoteMsg(pRaft->selfGroupId, pRaft->selfId, 
                                        term, cType, lastIndex, lastTerm);
    if (pMsg == NULL) {
      continue;
    }

    syncInfo("[%d:%d] [logterm: %" PRId64 ", index: %" PRId64 "] sent %d request to %d at term %" PRId64 "", 
      pRaft->selfGroupId, pRaft->selfId, lastTerm, 
      lastIndex, voteMsgType, nodeId, pRaft->term);

    pRaft->io.send(pMsg, &(pRaft->cluster.nodeInfo[i]));
  }
}