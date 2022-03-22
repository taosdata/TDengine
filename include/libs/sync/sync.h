/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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
#include <tdatablock.h>
#include "taosdef.h"
#include "trpc.h"
#include "wal.h"

typedef uint64_t SyncNodeId;
typedef int32_t  SyncGroupId;
typedef int64_t  SyncIndex;
typedef uint64_t SyncTerm;

typedef enum {
  TAOS_SYNC_STATE_FOLLOWER = 100,
  TAOS_SYNC_STATE_CANDIDATE = 101,
  TAOS_SYNC_STATE_LEADER = 102,
} ESyncState;

typedef struct SSyncBuffer {
  void*  data;
  size_t len;
} SSyncBuffer;

typedef struct SNodeInfo {
  uint16_t nodePort;                 // node sync Port
  char     nodeFqdn[TSDB_FQDN_LEN];  // node FQDN
} SNodeInfo;

typedef struct SSyncCfg {
  int32_t   replicaNum;
  int32_t   myIndex;
  SNodeInfo nodeInfo[TSDB_MAX_REPLICA];
} SSyncCfg;

typedef struct SNodesRole {
  int32_t    replicaNum;
  SNodeInfo  nodeInfo[TSDB_MAX_REPLICA];
  ESyncState role[TSDB_MAX_REPLICA];
} SNodesRole;

// abstract definition of snapshot
typedef struct SSnapshot {
  void*     data;
  SyncIndex lastApplyIndex;
} SSnapshot;

typedef struct SSyncFSM {
  void* data;

  // when value in pBuf finish a raft flow, FpCommitCb is called, code indicates the result
  // user can do something according to the code and isWeak. for example, write data into tsdb
  void (*FpCommitCb)(struct SSyncFSM* pFsm, const SRpcMsg* pBuf, SyncIndex index, bool isWeak, int32_t code);

  // when value in pBuf has been written into local log store, FpPreCommitCb is called, code indicates the result
  // user can do something according to the code and isWeak. for example, write data into tsdb
  void (*FpPreCommitCb)(struct SSyncFSM* pFsm, const SRpcMsg* pBuf, SyncIndex index, bool isWeak, int32_t code);

  // when log entry is updated by a new one, FpRollBackCb is called
  // user can do something to roll back. for example, delete data from tsdb, or just ignore it
  void (*FpRollBackCb)(struct SSyncFSM* pFsm, const SRpcMsg* pBuf, SyncIndex index, bool isWeak, int32_t code);

  // user should implement this function, use "data" to take snapshot into "snapshot"
  int32_t (*FpTakeSnapshot)(SSnapshot* snapshot);

  // user should implement this function, restore "data" from "snapshot"
  int32_t (*FpRestoreSnapshot)(const SSnapshot* snapshot);

} SSyncFSM;

struct SSyncRaftEntry;
typedef struct SSyncRaftEntry SSyncRaftEntry;

// abstract definition of log store in raft
// SWal implements it
typedef struct SSyncLogStore {
  void* data;

  // append one log entry
  int32_t (*appendEntry)(struct SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry);

  // get one log entry, user need to free pEntry->pCont
  SSyncRaftEntry* (*getEntry)(struct SSyncLogStore* pLogStore, SyncIndex index);

  // truncate log with index, entries after the given index (>=index) will be deleted
  int32_t (*truncate)(struct SSyncLogStore* pLogStore, SyncIndex fromIndex);

  // return index of last entry
  SyncIndex (*getLastIndex)(struct SSyncLogStore* pLogStore);

  // return term of last entry
  SyncTerm (*getLastTerm)(struct SSyncLogStore* pLogStore);

  // update log store commit index with "index"
  int32_t (*updateCommitIndex)(struct SSyncLogStore* pLogStore, SyncIndex index);

  // return commit index of log
  SyncIndex (*getCommitIndex)(struct SSyncLogStore* pLogStore);

} SSyncLogStore;

// raft need to persist two variables in storage: currentTerm, voteFor
typedef struct SStateMgr {
  void* data;

  int32_t (*getCurrentTerm)(struct SStateMgr* pMgr, SyncTerm* pCurrentTerm);
  int32_t (*persistCurrentTerm)(struct SStateMgr* pMgr, SyncTerm pCurrentTerm);

  int32_t (*getVoteFor)(struct SStateMgr* pMgr, SyncNodeId* pVoteFor);
  int32_t (*persistVoteFor)(struct SStateMgr* pMgr, SyncNodeId voteFor);

  int32_t (*getSyncCfg)(struct SStateMgr* pMgr, SSyncCfg* pSyncCfg);
  int32_t (*persistSyncCfg)(struct SStateMgr* pMgr, SSyncCfg* pSyncCfg);

} SStateMgr;

typedef struct SSyncInfo {
  SyncGroupId vgId;
  SSyncCfg    syncCfg;
  char        path[TSDB_FILENAME_LEN];
  SWal*       pWal;
  SSyncFSM*   pFsm;

  void* rpcClient;
  int32_t (*FpSendMsg)(void* rpcClient, const SEpSet* pEpSet, SRpcMsg* pMsg);
  void* queue;
  int32_t (*FpEqMsg)(void* queue, SRpcMsg* pMsg);

} SSyncInfo;

struct SSyncNode;
typedef struct SSyncNode SSyncNode;

int32_t syncInit();
void    syncCleanUp();

int64_t    syncStart(const SSyncInfo* pSyncInfo);
void       syncStop(int64_t rid);
int32_t    syncReconfig(int64_t rid, const SSyncCfg* pSyncCfg);
int32_t    syncPropose(int64_t rid, const SRpcMsg* pMsg, bool isWeak);        // use this function
int32_t    syncForwardToPeer(int64_t rid, const SRpcMsg* pMsg, bool isWeak);  // just for compatibility
ESyncState syncGetMyRole(int64_t rid);
void       syncGetNodesRole(int64_t rid, SNodesRole* pNodeRole);

extern int32_t sDebugFlag;

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_H*/
