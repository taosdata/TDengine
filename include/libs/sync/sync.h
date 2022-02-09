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

typedef int32_t  SyncNodeId;
typedef int32_t  SyncGroupId;
typedef int64_t  SyncIndex;
typedef uint64_t SyncTerm;

typedef enum {
  TAOS_SYNC_STATE_FOLLOWER = 0,
  TAOS_SYNC_STATE_CANDIDATE = 1,
  TAOS_SYNC_STATE_LEADER = 2,
} ESyncState;

typedef struct {
  void*  data;
  size_t len;
} SSyncBuffer;

typedef struct {
  SyncNodeId nodeId;
  uint16_t   nodePort;                 // node sync Port
  char       nodeFqdn[TSDB_FQDN_LEN];  // node FQDN
} SNodeInfo;

typedef struct {
  int32_t   selfIndex;
  int32_t   replica;
  SNodeInfo nodeInfo[TSDB_MAX_REPLICA];
} SSyncCluster;

typedef struct {
  int32_t   selfIndex;
  int32_t   replica;
  SNodeInfo node[TSDB_MAX_REPLICA];
  ESyncState role[TSDB_MAX_REPLICA];
} SNodesRole;

typedef struct SSyncFSM {
  void* pData;

  // apply committed log, bufs will be free by sync module
  int32_t (*applyLog)(struct SSyncFSM* fsm, SyncIndex index, const SSyncBuffer* buf, void* pData);

  // cluster commit callback
  int32_t (*onClusterChanged)(struct SSyncFSM* fsm, const SSyncCluster* cluster, void* pData);

  // fsm return snapshot in ppBuf, bufs will be free by sync module
  // TODO: getSnapshot SHOULD be async?
  int32_t (*getSnapshot)(struct SSyncFSM* fsm, SSyncBuffer** ppBuf, int32_t* objId, bool* isLast);

  // fsm apply snapshot with pBuf data
  int32_t (*applySnapshot)(struct SSyncFSM* fsm, SSyncBuffer* pBuf, int32_t objId, bool isLast);

  // call when restore snapshot and log done
  int32_t (*onRestoreDone)(struct SSyncFSM* fsm);

  void (*onRollback)(struct SSyncFSM* fsm, SyncIndex index, const SSyncBuffer* buf);

  void (*onRoleChanged)(struct SSyncFSM* fsm, const SNodesRole* pRole);

} SSyncFSM;

typedef struct SSyncLogStore {
  void* pData;

  // write log with given index
  int32_t (*logWrite)(struct SSyncLogStore* logStore, SyncIndex index, SSyncBuffer* pBuf);

  /** 
   * read log from given index(included) with limit, return the actual num in nBuf,
   * pBuf will be free in sync module
   **/
  int32_t (*logRead)(struct SSyncLogStore* logStore, SyncIndex index, int limit,
                      SSyncBuffer* pBuf, int* nBuf);

  // mark log with given index has been commtted
  int32_t (*logCommit)(struct SSyncLogStore* logStore, SyncIndex index);

  // prune log before given index(not included)
  int32_t (*logPrune)(struct SSyncLogStore* logStore, SyncIndex index);

  // rollback log after given index(included)
  int32_t (*logRollback)(struct SSyncLogStore* logStore, SyncIndex index);

  // return last index of log
  SyncIndex (*logLastIndex)(struct SSyncLogStore* logStore);
} SSyncLogStore;

typedef struct SStateManager {
  void* pData;

  // save serialized server state data, buffer will be free by Sync
  int32_t (*saveServerState)(struct SStateManager* stateMng, const char* buffer, int n);

  // read serialized server state data, buffer will be free by Sync
  int32_t (*readServerState)(struct SStateManager* stateMng, char** ppBuffer, int* n);

  // save serialized cluster state data, buffer will be free by Sync
  void (*saveClusterState)(struct SStateManager* stateMng, const char* buffer, int n);

  // read serialized cluster state data, buffer will be free by Sync
  int32_t (*readClusterState)(struct SStateManager* stateMng, char** ppBuffer, int* n);
} SStateManager;

typedef struct {
  SyncGroupId   vgId;
  SyncIndex     appliedIndex;
  SSyncCluster  syncCfg;
  SSyncFSM      fsm;
  SSyncLogStore logStore;
  SStateManager stateManager;
} SSyncInfo;

struct SSyncNode;
typedef struct SSyncNode SSyncNode;

int32_t syncInit();
void    syncCleanUp();

SSyncNode* syncStart(const SSyncInfo*);
void       syncReconfig(const SSyncNode*, const SSyncCluster*);
void       syncStop(const SSyncNode*);

int32_t syncPropose(SSyncNode* syncNode, const SSyncBuffer* pBuf, void* pData, bool isWeak);

int32_t syncAddNode(SSyncNode syncNode, const SNodeInfo *pNode);

int32_t syncRemoveNode(SSyncNode syncNode, const SNodeInfo *pNode);

extern int32_t sDebugFlag;

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_H*/
