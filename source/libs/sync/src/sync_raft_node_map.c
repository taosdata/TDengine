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

#include "sync_raft_node_map.h"
#include "sync_type.h"

bool syncRaftIsInNodeMap(const SSyncRaftNodeMap* nodeMap, SyncNodeId nodeId) {
  int i;

  for (i = 0; i < TSDB_MAX_REPLICA; ++i) {
    if (nodeId == nodeMap->nodeId[i]) {
      return true;
    }
  }

  return false;
}

void syncRaftCopyNodeMap(const SSyncRaftNodeMap* nodeMap, SSyncRaftNodeMap* to) {
  memcpy(to, nodeMap, sizeof(SSyncRaftNodeMap));
}

void syncRaftUnionNodeMap(const SSyncRaftNodeMap* nodeMap, SSyncRaftNodeMap* to) {
  int i, j, m;

  for (i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SyncNodeId id = nodeMap->nodeId[i];
    if (id == SYNC_NON_NODE_ID) {
      continue;
    }

    syncRaftAddToNodeMap(to, id);
  }
}

void syncRaftAddToNodeMap(SSyncRaftNodeMap* nodeMap, SyncNodeId nodeId) {
  assert(nodeMap->replica < TSDB_MAX_REPLICA);

  int i, j;
  for (i = 0, j = -1; i < TSDB_MAX_REPLICA; ++i) {
    SyncNodeId id = nodeMap->nodeId[i];
    if (id == SYNC_NON_NODE_ID) {
      if (j == -1) j = i;
      continue;
    }
    if (id == nodeId) {
      return;
    }
  }

  assert(j != -1);
  nodeMap->nodeId[j] = nodeId;
  nodeMap->replica += 1;
}