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

#ifndef TD_SYNC_RAFT_PROTO_H
#define TD_SYNC_RAFT_PROTO_H

#include "sync_type.h"

typedef enum ESyncRaftConfChangeType {
	SYNC_RAFT_Conf_AddNode = 0,
  SYNC_RAFT_Conf_RemoveNode = 1,
  SYNC_RAFT_Conf_UpdateNode = 2,
  SYNC_RAFT_Conf_AddLearnerNode = 3,
} ESyncRaftConfChangeType;

// ConfChangeSingle is an individual configuration change operation. Multiple
// such operations can be carried out atomically via a ConfChangeV2.
typedef struct SSyncConfChangeSingle {
  ESyncRaftConfChangeType type;
  SyncNodeId nodeId;
} SSyncConfChangeSingle;

typedef struct SSyncConfChangeSingleArray {
  int n;
  SSyncConfChangeSingle* changes;
} SSyncConfChangeSingleArray;

typedef struct SSyncConfigState {
	// The voters in the incoming config. (If the configuration is not joint,
	// then the outgoing config is empty).
  SSyncRaftNodeMap voters;

  // The learners in the incoming config.
  SSyncRaftNodeMap learners;

  // The voters in the outgoing config.
  SSyncRaftNodeMap votersOutgoing;

	// The nodes that will become learners when the outgoing config is removed.
	// These nodes are necessarily currently in nodes_joint (or they would have
	// been added to the incoming config right away).
  SSyncRaftNodeMap learnersNext;

	// If set, the config is joint and Raft will automatically transition into
	// the final config (i.e. remove the outgoing config) when this is safe.
  bool autoLeave;
} SSyncConfigState;

#endif /* TD_SYNC_RAFT_PROTO_H */
