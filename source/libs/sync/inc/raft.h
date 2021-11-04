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

#ifndef _TD_LIBS_SYNC_RAFT_H
#define _TD_LIBS_SYNC_RAFT_H

#include "sync.h"
#include "sync_type.h"
#include "raft_message.h"

#define SYNC_NON_NODE_ID -1

typedef struct SSyncRaftProgress SSyncRaftProgress;

typedef struct RaftLeaderState {
  int nProgress;
  SSyncRaftProgress* progress;
} RaftLeaderState;

typedef struct RaftCandidateState {
  /* votes results */
  SyncRaftVoteRespType votes[TSDB_MAX_REPLICA];

  /* true if in pre-vote phase */
  bool inPreVote;
} RaftCandidateState;

typedef struct SSyncRaftIOMethods {
  // send SSyncMessage to node
  int (*send)(const SSyncMessage* pMsg, const SNodeInfo* pNode);
} SSyncRaftIOMethods;

typedef int   (*SyncRaftStepFp)(SSyncRaft* pRaft, const SSyncMessage* pMsg);
typedef void  (*SyncRaftTickFp)(SSyncRaft* pRaft);

struct SSyncRaft {
  // owner sync node
  SSyncNode* pNode;

  //SSyncInfo info;
  SSyncFSM      fsm;
  SSyncLogStore logStore;
  SStateManager stateManager;

  SyncTerm term;
  SyncNodeId voteFor;

  SyncNodeId selfId;
  SyncGroupId selfGroupId;

  /**
   * the leader id
  **/
  SyncNodeId leaderId;

	/**
   * leadTransferee is id of the leader transfer target when its value is not zero.
	 * Follow the procedure defined in raft thesis 3.10.
   **/
  SyncNodeId leadTransferee;

	/** 
   * New configuration is ignored if there exists unapplied configuration.
   **/
	bool pendingConf;

  SSyncCluster cluster;

  ESyncRole state;

	/** 
   * number of ticks since it reached last electionTimeout when it is leader
	 * or candidate.
	 * number of ticks since it reached last electionTimeout or received a
	 * valid message from current leader when it is a follower.
  **/
  uint16_t electionElapsed;

	/**
   *  number of ticks since it reached last heartbeatTimeout.
	 * only leader keeps heartbeatElapsed.
   **/
  uint16_t heartbeatElapsed;

  // election timeout tick(random in [3:6] tick)
  uint16_t electionTimeoutTick;

  // heartbeat timeout tick(default: 1 tick)
  uint16_t heartbeatTimeoutTick;

  bool preVote;
  bool checkQuorum;

  SSyncRaftIOMethods io;

  // union different state data
  union {
    RaftLeaderState leaderState;
    RaftCandidateState candidateState;
  };
  
  SSyncRaftLog *log;

  SyncRaftStepFp stepFp;

  SyncRaftTickFp tickFp;
};

int32_t syncRaftStart(SSyncRaft* pRaft, const SSyncInfo* pInfo);
int32_t syncRaftStep(SSyncRaft* pRaft, const SSyncMessage* pMsg);
int32_t syncRaftTick(SSyncRaft* pRaft);

void syncRaftBecomeFollower(SSyncRaft* pRaft, SyncTerm term, SyncNodeId leaderId);
void syncRaftBecomePreCandidate(SSyncRaft* pRaft);
void syncRaftBecomeCandidate(SSyncRaft* pRaft);
void syncRaftBecomeLeader(SSyncRaft* pRaft);

void syncRaftStartElection(SSyncRaft* pRaft, SyncRaftElectionType cType);

void syncRaftTriggerReplicate(SSyncRaft* pRaft);

void syncRaftRandomizedElectionTimeout(SSyncRaft* pRaft);
bool syncRaftIsPromotable(SSyncRaft* pRaft);
bool syncRaftIsPastElectionTimeout(SSyncRaft* pRaft);
int  syncRaftQuorum(SSyncRaft* pRaft);
int  syncRaftNumOfGranted(SSyncRaft* pRaft, SyncNodeId id, 
                          bool preVote, bool accept, int* rejectNum);

#endif /* _TD_LIBS_SYNC_RAFT_H */