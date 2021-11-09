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
#include "sync_raft_impl.h"
#include "sync_raft_quorum.h"

typedef struct RaftLeaderState {

} RaftLeaderState;

typedef struct RaftCandidateState {
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

  SSyncCluster cluster; 

  int selfIndex;
  SyncNodeId selfId;
  SyncGroupId selfGroupId;

  SSyncRaftIOMethods io;

  SSyncFSM      fsm;
  SSyncLogStore logStore;
  SStateManager stateManager;

  union {
    RaftLeaderState leaderState;
    RaftCandidateState candidateState;
  };

  SyncTerm term;
  SyncNodeId voteFor;

  SSyncRaftLog *log;

  int maxMsgSize;
  SSyncRaftProgressTracker *tracker;

  ESyncRole state;

  // isLearner is true if the local raft node is a learner.
  bool isLearner;

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
   * Only one conf change may be pending (in the log, but not yet
	 * applied) at a time. This is enforced via pendingConfIndex, which
	 * is set to a value >= the log index of the latest pending
	 * configuration change (if any). Config changes are only allowed to
	 * be proposed if the leader's applied index is greater than this
	 * value.
   **/
  SyncIndex pendingConfigIndex;

	/** 
   * an estimate of the size of the uncommitted tail of the Raft log. Used to
	 * prevent unbounded log growth. Only maintained by the leader. Reset on
	 * term changes.
   **/
  uint32_t uncommittedSize;
 
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

  bool preVote;
  bool checkQuorum;

  int heartbeatTimeout;
  int electionTimeout;

	/**
   * randomizedElectionTimeout is a random number between
	 * [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	 * when raft changes its state to follower or candidate.
   **/
	int randomizedElectionTimeout;
	bool disableProposalForwarding;

  // current tick count since start up
  uint32_t currentTick;

  SyncRaftStepFp stepFp;

  SyncRaftTickFp tickFp;
};

int32_t syncRaftStart(SSyncRaft* pRaft, const SSyncInfo* pInfo);
int32_t syncRaftStep(SSyncRaft* pRaft, const SSyncMessage* pMsg);
int32_t syncRaftTick(SSyncRaft* pRaft);

#endif /* _TD_LIBS_SYNC_RAFT_H */