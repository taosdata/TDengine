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

#ifndef _TD_LIBS_SYNC_H
#define _TD_LIBS_SYNC_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "taosdef.h"

typedef int64_t SyncNodeId;
typedef int32_t SyncGroupId;
typedef int64_t   SyncIndex;
typedef uint64_t SSyncTerm;

typedef enum {
  TAOS_SYNC_ROLE_FOLLOWER = 0,
  TAOS_SYNC_ROLE_CANDIDATE = 1,
  TAOS_SYNC_ROLE_LEADER = 2,
} ESyncRole;

typedef struct {
  void*  data;
  size_t len;
} SSyncBuffer;

typedef struct {
  SyncNodeId nodeId;
  uint16_t  nodePort;  // node sync Port
  char      nodeFqdn[TSDB_FQDN_LEN]; // node FQDN  
} SNodeInfo;

typedef struct {
  int        selfIndex;
  int        nNode;
  SNodeInfo* nodeInfo;
} SSyncCluster;

typedef struct {
  int32_t  selfIndex;
  int nNode;
  SNodeInfo* node;
  ESyncRole*  role;
} SNodesRole;

typedef struct SSyncFSM {
  void* pData;

  // apply committed log, bufs will be free by raft module
  int (*applyLog)(struct SSyncFSM* fsm, SyncIndex index, const SSyncBuffer* buf, void* pData);

  // cluster commit callback
  int (*onClusterChanged)(struct SSyncFSM* fsm, const SSyncCluster* cluster, void* pData);

  // fsm return snapshot in ppBuf, bufs will be free by raft module
  // TODO: getSnapshot SHOULD be async?
  int (*getSnapshot)(struct SSyncFSM* fsm, SSyncBuffer** ppBuf, int* objId, bool* isLast);

  // fsm apply snapshot with pBuf data
  int (*applySnapshot)(struct SSyncFSM* fsm, SSyncBuffer* pBuf, int objId, bool isLast);

  // call when restore snapshot and log done
  int (*onRestoreDone)(struct SSyncFSM* fsm);

  void (*onRollback)(struct SSyncFSM* fsm, SyncIndex index, const SSyncBuffer* buf);

  void (*onRoleChanged)(struct SSyncFSM* fsm, const SNodesRole* pRole);

} SSyncFSM;

typedef struct SSyncLogStore {
  void* pData;

  // write log with given index
  int32_t (*logWrite)(struct SSyncLogStore* logStore, SyncIndex index, SSyncBuffer* pBuf);

  // mark log with given index has been commtted
  int32_t (*logCommit)(struct SSyncLogStore* logStore, SyncIndex index);

  // prune log before given index
  int32_t (*logPrune)(struct SSyncLogStore* logStore, SyncIndex index);

  // rollback log after given index
  int32_t (*logRollback)(struct SSyncLogStore* logStore, SyncIndex index);
} SSyncLogStore;

typedef struct SSyncServerState {
  SyncNodeId voteFor;
  SSyncTerm term;
} SSyncServerState;

typedef struct SSyncClusterConfig {
  // Log index number of current cluster config.
  SyncIndex index;
  
  // Log index number of previous cluster config.
  SyncIndex prevIndex;

  // current cluster
  const SSyncCluster* cluster;
} SSyncClusterConfig;

typedef struct SStateManager {
  void* pData;

  void (*saveServerState)(struct SStateManager* stateMng, const SSyncServerState* state);

  const SSyncServerState* (*readServerState)(struct SStateManager* stateMng);

  void (*saveCluster)(struct SStateManager* stateMng, const SSyncClusterConfig* cluster);

  const SSyncClusterConfig* (*readCluster)(struct SStateManager* stateMng);
} SStateManager;

typedef struct {
  SyncGroupId vgId;

  SyncIndex snapshotIndex;
  SSyncCluster syncCfg;

  SSyncFSM fsm;

  SSyncLogStore logStore;

  SStateManager stateManager;
} SSyncInfo;

struct SSyncNode;
typedef struct SSyncNode SSyncNode;

int32_t syncInit();
void    syncCleanUp();

SSyncNode syncStart(const SSyncInfo*);
void       syncStop(SyncNodeId);

int32_t syncPropose(SSyncNode syncNode, SSyncBuffer buffer, void* pData, bool isWeak);

int32_t syncAddNode(SSyncNode syncNode, const SNodeInfo *pNode);

int32_t syncRemoveNode(SSyncNode syncNode, const SNodeInfo *pNode);

extern int32_t  syncDebugFlag;

#ifdef __cplusplus
}
#endif

#endif  /*_TD_LIBS_SYNC_H*/
