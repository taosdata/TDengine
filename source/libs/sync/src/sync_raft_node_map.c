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
#include "sync_raft_progress.h"

void syncRaftInitNodeMap(SSyncRaftNodeMap* nodeMap) {
  nodeMap->nodeIdMap = taosHashInit(TSDB_MAX_REPLICA, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
}

void syncRaftClearNodeMap(SSyncRaftNodeMap* nodeMap) {
  taosHashClear(nodeMap->nodeIdMap);
}

bool syncRaftIsInNodeMap(const SSyncRaftNodeMap* nodeMap, SyncNodeId nodeId) {
  SyncNodeId** ppId = (SyncNodeId**)taosHashGet(nodeMap->nodeIdMap, &nodeId, sizeof(SyncNodeId*));
  if (ppId == NULL) {
    return false;
  }
  return true;
}

void syncRaftCopyNodeMap(const SSyncRaftNodeMap* from, SSyncRaftNodeMap* to) {
  SyncNodeId** ppId = (SyncNodeId**)taosHashIterate(from->nodeIdMap, NULL);
  while (ppId) {
    taosHashPut(to->nodeIdMap, ppId, sizeof(SyncNodeId*), ppId, sizeof(SyncNodeId*));
    ppId = taosHashIterate(from->nodeIdMap, ppId);
  }
}

bool syncRaftIterateNodeMap(const SSyncRaftNodeMap* nodeMap, SyncNodeId *pId) {
  SyncNodeId **ppId = taosHashIterate(nodeMap->nodeIdMap, pId);
  if (ppId == NULL) {
    return true;
  }

  *pId = *(*ppId);
  return false;
}

bool syncRaftIsAllInProgressMap(const SSyncRaftNodeMap* nodeMap, const SSyncRaftProgressMap* progressMap) {
  SyncNodeId *pId = NULL;
  while (!syncRaftIterateNodeMap(nodeMap, pId)) {
    if (!syncRaftIsInProgressMap(progressMap, *pId)) {
      return false;
    }
  }

  return true;
}

void syncRaftUnionNodeMap(const SSyncRaftNodeMap* nodeMap, SSyncRaftNodeMap* to) {
  syncRaftCopyNodeMap(nodeMap, to);
}

void syncRaftAddToNodeMap(SSyncRaftNodeMap* nodeMap, SyncNodeId nodeId) {
  taosHashPut(nodeMap->nodeIdMap, &nodeId, sizeof(SyncNodeId*), &nodeId, sizeof(SyncNodeId*));
}

void syncRaftRemoveFromNodeMap(SSyncRaftNodeMap* nodeMap, SyncNodeId nodeId) {
  taosHashRemove(nodeMap->nodeIdMap, &nodeId, sizeof(SyncNodeId*));
}

int32_t syncRaftNodeMapSize(const SSyncRaftNodeMap* nodeMap) {
  return taosHashGetSize(nodeMap->nodeIdMap);
}