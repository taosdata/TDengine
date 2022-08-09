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

#include "cJSON.h"
#include "tdef.h"
#include "tmsgcb.h"

extern bool gRaftDetailLog;

#define SYNC_RESP_TTL_MS       10000000
#define SYNC_SPEED_UP_HB_TIMER 400
#define SYNC_SPEED_UP_AFTER_MS (1000 * 20)
#define SYNC_SLOW_DOWN_RANGE   100

#define SYNC_MAX_BATCH_SIZE 1
#define SYNC_INDEX_BEGIN    0
#define SYNC_INDEX_INVALID  -1
#define SYNC_TERM_INVALID   0xFFFFFFFFFFFFFFFF

typedef enum {
  SYNC_STRATEGY_NO_SNAPSHOT = 0,
  SYNC_STRATEGY_STANDARD_SNAPSHOT = 1,
  SYNC_STRATEGY_WAL_FIRST = 2,
} ESyncStrategy;

typedef uint64_t SyncNodeId;
typedef int32_t  SyncGroupId;
typedef int64_t  SyncIndex;
typedef uint64_t SyncTerm;

typedef struct SSyncNode      SSyncNode;
typedef struct SSyncBuffer    SSyncBuffer;
typedef struct SWal           SWal;
typedef struct SSyncRaftEntry SSyncRaftEntry;

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

typedef struct SFsmCbMeta {
  int32_t    code;
  SyncIndex  index;
  SyncTerm   term;
  uint64_t   seqNum;
  SyncIndex  lastConfigIndex;
  ESyncState state;
  SyncTerm   currentTerm;
  bool       isWeak;
  uint64_t   flag;
} SFsmCbMeta;

typedef struct SReConfigCbMeta {
  int32_t    code;
  SyncIndex  index;
  SyncTerm   term;
  uint64_t   seqNum;
  SyncIndex  lastConfigIndex;
  ESyncState state;
  SyncTerm   currentTerm;
  bool       isWeak;
  uint64_t   flag;

  // config info
  SSyncCfg  oldCfg;
  SSyncCfg  newCfg;
  SyncIndex newCfgIndex;
  SyncTerm  newCfgTerm;
  uint64_t  newCfgSeqNum;

} SReConfigCbMeta;

typedef struct SSnapshotParam {
  SyncIndex start;
  SyncIndex end;
} SSnapshotParam;

typedef struct SSnapshot {
  void*     data;
  SyncIndex lastApplyIndex;
  SyncTerm  lastApplyTerm;
  SyncIndex lastConfigIndex;
} SSnapshot;

typedef struct SSnapshotMeta {
  SyncIndex lastConfigIndex;
} SSnapshotMeta;

typedef struct SSyncFSM {
  void* data;

  void (*FpCommitCb)(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SFsmCbMeta cbMeta);
  void (*FpPreCommitCb)(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SFsmCbMeta cbMeta);
  void (*FpRollBackCb)(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SFsmCbMeta cbMeta);

  void (*FpRestoreFinishCb)(struct SSyncFSM* pFsm);
  void (*FpReConfigCb)(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SReConfigCbMeta cbMeta);
  void (*FpLeaderTransferCb)(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SFsmCbMeta cbMeta);

  int32_t (*FpGetSnapshot)(struct SSyncFSM* pFsm, SSnapshot* pSnapshot, void* pReaderParam, void** ppReader);
  int32_t (*FpGetSnapshotInfo)(struct SSyncFSM* pFsm, SSnapshot* pSnapshot);

  int32_t (*FpSnapshotStartRead)(struct SSyncFSM* pFsm, void* pReaderParam, void** ppReader);
  int32_t (*FpSnapshotStopRead)(struct SSyncFSM* pFsm, void* pReader);
  int32_t (*FpSnapshotDoRead)(struct SSyncFSM* pFsm, void* pReader, void** ppBuf, int32_t* len);

  int32_t (*FpSnapshotStartWrite)(struct SSyncFSM* pFsm, void* pWriterParam, void** ppWriter);
  int32_t (*FpSnapshotStopWrite)(struct SSyncFSM* pFsm, void* pWriter, bool isApply, SSnapshot* pSnapshot);
  int32_t (*FpSnapshotDoWrite)(struct SSyncFSM* pFsm, void* pWriter, void* pBuf, int32_t len);

} SSyncFSM;

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

  SyncIndex (*syncLogBeginIndex)(struct SSyncLogStore* pLogStore);
  SyncIndex (*syncLogEndIndex)(struct SSyncLogStore* pLogStore);
  bool (*syncLogIsEmpty)(struct SSyncLogStore* pLogStore);
  int32_t (*syncLogEntryCount)(struct SSyncLogStore* pLogStore);
  int32_t (*syncLogRestoreFromSnapshot)(struct SSyncLogStore* pLogStore, SyncIndex index);
  bool (*syncLogExist)(struct SSyncLogStore* pLogStore, SyncIndex index);

  SyncIndex (*syncLogWriteIndex)(struct SSyncLogStore* pLogStore);
  SyncIndex (*syncLogLastIndex)(struct SSyncLogStore* pLogStore);
  SyncTerm (*syncLogLastTerm)(struct SSyncLogStore* pLogStore);

  int32_t (*syncLogAppendEntry)(struct SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry);
  int32_t (*syncLogGetEntry)(struct SSyncLogStore* pLogStore, SyncIndex index, SSyncRaftEntry** ppEntry);
  int32_t (*syncLogTruncate)(struct SSyncLogStore* pLogStore, SyncIndex fromIndex);

} SSyncLogStore;

typedef struct SSyncInfo {
  bool          isStandBy;
  ESyncStrategy snapshotStrategy;
  SyncGroupId   vgId;
  int32_t       batchSize;
  SSyncCfg      syncCfg;
  char          path[TSDB_FILENAME_LEN];
  SWal*         pWal;
  SSyncFSM*     pFsm;
  SMsgCb*       msgcb;
  int32_t (*FpSendMsg)(const SEpSet* pEpSet, SRpcMsg* pMsg);
  int32_t (*FpEqMsg)(const SMsgCb* msgcb, SRpcMsg* pMsg);
} SSyncInfo;

int32_t     syncInit();
void        syncCleanUp();
int64_t     syncOpen(const SSyncInfo* pSyncInfo);
void        syncStart(int64_t rid);
void        syncStop(int64_t rid);
int32_t     syncSetStandby(int64_t rid);
ESyncState  syncGetMyRole(int64_t rid);
bool        syncIsReady(int64_t rid);
const char* syncGetMyRoleStr(int64_t rid);
bool        syncRestoreFinish(int64_t rid);
SyncTerm    syncGetMyTerm(int64_t rid);
SyncGroupId syncGetVgId(int64_t rid);
void        syncGetEpSet(int64_t rid, SEpSet* pEpSet);
void        syncGetRetryEpSet(int64_t rid, SEpSet* pEpSet);
int32_t     syncPropose(int64_t rid, SRpcMsg* pMsg, bool isWeak);
int32_t     syncProposeBatch(int64_t rid, SRpcMsg** pMsgPArr, bool* pIsWeakArr, int32_t arrSize);
bool        syncEnvIsStart();
const char* syncStr(ESyncState state);
bool        syncIsRestoreFinish(int64_t rid);
int32_t     syncGetSnapshotByIndex(int64_t rid, SyncIndex index, SSnapshot* pSnapshot);

int32_t syncReconfig(int64_t rid, const SSyncCfg* pNewCfg);

// build SRpcMsg, need to call syncPropose with SRpcMsg
int32_t syncReconfigBuild(int64_t rid, const SSyncCfg* pNewCfg, SRpcMsg* pRpcMsg);

int32_t syncLeaderTransfer(int64_t rid);
int32_t syncLeaderTransferTo(int64_t rid, SNodeInfo newLeader);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_H*/
