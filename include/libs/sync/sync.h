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
#include "cJSON.h"
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
  TAOS_SYNC_STATE_ERROR = 103,
} ESyncState;

typedef struct SNodeInfo {
  uint16_t nodePort;
  char     nodeFqdn[TSDB_FQDN_LEN];
} SNodeInfo;

typedef struct SSyncCfg {
  int32_t   replicaNum;
  int32_t   myIndex;
  SNodeInfo nodeInfo[TSDB_MAX_REPLICA];
} SSyncCfg;

typedef struct SRaftId {
  SyncNodeId  addr;
  SyncGroupId vgId;
} SRaftId;

typedef struct SSnapshot {
  void*     data;
  SyncIndex lastApplyIndex;
} SSnapshot;

typedef enum {
  TAOS_SYNC_FSM_CB_SUCCESS = 0,
  TAOS_SYNC_FSM_CB_OTHER_ERROR,
} ESyncFsmCbCode;

typedef struct SFsmCbMeta {
  SyncIndex  index;
  bool       isWeak;
  int32_t    code;
  ESyncState state;
  uint64_t   seqNum;
} SFsmCbMeta;

typedef struct SSyncFSM {
  void* data;

  void (*FpCommitCb)(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SFsmCbMeta cbMeta);
  void (*FpPreCommitCb)(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SFsmCbMeta cbMeta);
  void (*FpRollBackCb)(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SFsmCbMeta cbMeta);

  int32_t (*FpTakeSnapshot)(SSnapshot* snapshot);
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

int32_t     syncInit();
void        syncCleanUp();
int64_t     syncOpen(const SSyncInfo* pSyncInfo);
void        syncStart(int64_t rid);
void        syncStop(int64_t rid);
int32_t     syncReconfig(int64_t rid, const SSyncCfg* pSyncCfg);
ESyncState  syncGetMyRole(int64_t rid);
const char* syncGetMyRoleStr(int64_t rid);
SyncTerm    syncGetMyTerm(int64_t rid);

typedef enum {
  TAOS_SYNC_PROPOSE_SUCCESS = 0,
  TAOS_SYNC_PROPOSE_NOT_LEADER,
  TAOS_SYNC_PROPOSE_OTHER_ERROR,
} ESyncProposeCode;

int32_t syncPropose(int64_t rid, const SRpcMsg* pMsg, bool isWeak);

extern int32_t sDebugFlag;

//-----------------------------------------
struct SSyncNode;
typedef struct SSyncNode SSyncNode;

struct SSyncBuffer;
typedef struct SSyncBuffer SSyncBuffer;
//-----------------------------------------

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_H*/
