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

#ifndef TD_SYNC_RAFT_QUORUM_H
#define TD_SYNC_RAFT_QUORUM_H

/**
 * ESyncRaftVoteResult indicates the outcome of a vote.
 **/
typedef enum {
	/**
	 * SYNC_RAFT_VOTE_PENDING indicates that the decision of the vote depends on future
	 * votes, i.e. neither "yes" or "no" has reached quorum yet.
	 **/
	SYNC_RAFT_VOTE_PENDING = 1,

	/** 
	 * SYNC_RAFT_VOTE_LOST indicates that the quorum has voted "no".
	 **/
	SYNC_RAFT_VOTE_LOST = 2,

	/** 
	 * SYNC_RAFT_VOTE_WON indicates that the quorum has voted "yes".
	 **/
	SYNC_RAFT_VOTE_WON = 3,
} ESyncRaftVoteResult;

#endif /* TD_SYNC_RAFT_QUORUM_H */