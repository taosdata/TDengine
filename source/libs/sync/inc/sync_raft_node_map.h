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

#include "thash.h"
#include "sync.h"
#include "sync_type.h"

struct SSyncRaftNodeMap {
  SHashObj* nodeIdMap;
};

void syncRaftInitNodeMap(SSyncRaftNodeMap* nodeMap);
void syncRaftFreeNodeMap(SSyncRaftNodeMap* nodeMap);

void syncRaftClearNodeMap(SSyncRaftNodeMap* nodeMap);

bool syncRaftIsInNodeMap(const SSyncRaftNodeMap* nodeMap, SyncNodeId nodeId);

void syncRaftCopyNodeMap(SSyncRaftNodeMap* from, SSyncRaftNodeMap* to);

void syncRaftUnionNodeMap(SSyncRaftNodeMap* nodeMap, SSyncRaftNodeMap* to);

void syncRaftAddToNodeMap(SSyncRaftNodeMap* nodeMap, SyncNodeId nodeId);

void syncRaftRemoveFromNodeMap(SSyncRaftNodeMap* nodeMap, SyncNodeId nodeId);

int32_t syncRaftNodeMapSize(const SSyncRaftNodeMap* nodeMap);

// return true if reach the end
bool syncRaftIterateNodeMap(const SSyncRaftNodeMap* nodeMap, SyncNodeId *pId);

bool syncRaftIsAllNodeInProgressMap(SSyncRaftNodeMap* nodeMap, SSyncRaftProgressMap* progressMap);

#endif /* _TD_LIBS_SYNC_RAFT_NODE_MAP_H */