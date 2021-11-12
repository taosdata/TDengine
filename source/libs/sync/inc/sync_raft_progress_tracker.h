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

#ifndef _TD_LIBS_SYNC_RAFT_PROGRESS_TRACKER_H
#define _TD_LIBS_SYNC_RAFT_PROGRESS_TRACKER_H

#include "sync_type.h"
#include "sync_raft_quorum.h"
#include "sync_raft_quorum_joint.h"
#include "sync_raft_progress.h"

struct SSyncRaftProgressTrackerConfig {
  SSyncRaftQuorumJointConfig voters;

	// autoLeave is true if the configuration is joint and a transition to the
	// incoming configuration should be carried out automatically by Raft when
	// this is possible. If false, the configuration will be joint until the
	// application initiates the transition manually.
  bool autoLeave;

	// Learners is a set of IDs corresponding to the learners active in the
	// current configuration.
	//
	// Invariant: Learners and Voters does not intersect, i.e. if a peer is in
	// either half of the joint config, it can't be a learner; if it is a
	// learner it can't be in either half of the joint config. This invariant
	// simplifies the implementation since it allows peers to have clarity about
	// its current role without taking into account joint consensus.
  SSyncRaftNodeMap learners;

	// When we turn a voter into a learner during a joint consensus transition,
	// we cannot add the learner directly when entering the joint state. This is
	// because this would violate the invariant that the intersection of
	// voters and learners is empty. For example, assume a Voter is removed and
	// immediately re-added as a learner (or in other words, it is demoted):
	//
	// Initially, the configuration will be
	//
	//   voters:   {1 2 3}
	//   learners: {}
	//
	// and we want to demote 3. Entering the joint configuration, we naively get
	//
	//   voters:   {1 2} & {1 2 3}
	//   learners: {3}
	//
	// but this violates the invariant (3 is both voter and learner). Instead,
	// we get
	//
	//   voters:   {1 2} & {1 2 3}
	//   learners: {}
	//   next_learners: {3}
	//
	// Where 3 is now still purely a voter, but we are remembering the intention
	// to make it a learner upon transitioning into the final configuration:
	//
	//   voters:   {1 2}
	//   learners: {3}
	//   next_learners: {}
	//
	// Note that next_learners is not used while adding a learner that is not
	// also a voter in the joint config. In this case, the learner is added
	// right away when entering the joint configuration, so that it is caught up
	// as soon as possible.
  SSyncRaftNodeMap learnersNext;
};

struct SSyncRaftProgressTracker {
  SSyncRaftProgressTrackerConfig config;

  SSyncRaftProgressMap progressMap;

	ESyncRaftVoteType votes[TSDB_MAX_REPLICA];
  int maxInflightMsgs;
};

SSyncRaftProgressTracker* syncRaftOpenProgressTracker();

void syncRaftResetVotes(SSyncRaftProgressTracker*);

typedef void (*visitProgressFp)(int i, SSyncRaftProgress* progress, void* arg);
void syncRaftProgressVisit(SSyncRaftProgressTracker*, visitProgressFp visit, void* arg);

/**
 * syncRaftRecordVote records that the node with the given id voted for this Raft
 * instance if v == true (and declined it otherwise).
 **/
void syncRaftRecordVote(SSyncRaftProgressTracker* tracker, int i, bool grant);

void syncRaftCloneTrackerConfig(const SSyncRaftProgressTrackerConfig* config, SSyncRaftProgressTrackerConfig* result);

int syncRaftCheckProgress(const SSyncRaftProgressTrackerConfig* config, SSyncRaftProgressMap* progressMap);

/** 
 * syncRaftTallyVotes returns the number of granted and rejected Votes, and whether the
 * election outcome is known.
 **/
ESyncRaftVoteResult syncRaftTallyVotes(SSyncRaftProgressTracker* tracker, int* rejected, int *granted);

#endif  /* _TD_LIBS_SYNC_RAFT_PROGRESS_TRACKER_H */
