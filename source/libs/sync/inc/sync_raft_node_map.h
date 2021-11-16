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

#ifndef _TD_LIBS_SYNC_RAFT_NODE_MAP_H
#define _TD_LIBS_SYNC_RAFT_NODE_MAP_H

#include "sync.h"
#include "sync_type.h"

// TODO: is TSDB_MAX_REPLICA enough?
struct SSyncRaftNodeMap {
  int32_t   replica;
  SyncNodeId nodeId[TSDB_MAX_REPLICA];
};

bool syncRaftIsInNodeMap(const SSyncRaftNodeMap* nodeMap, SyncNodeId nodeId);

void syncRaftCopyNodeMap(const SSyncRaftNodeMap* nodeMap, SSyncRaftNodeMap* to);

void syncRaftUnionNodeMap(const SSyncRaftNodeMap* nodeMap, SSyncRaftNodeMap* to);

void syncRaftAddToNodeMap(SSyncRaftNodeMap* nodeMap, SyncNodeId nodeId);

#endif /* _TD_LIBS_SYNC_RAFT_NODE_MAP_H */