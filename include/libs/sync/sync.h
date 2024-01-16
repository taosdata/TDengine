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
#include "tlrucache.h"
#include "tmsgcb.h"

#define SYNC_RESP_TTL_MS             30000
#define SYNC_SPEED_UP_HB_TIMER       400
#define SYNC_SPEED_UP_AFTER_MS       (1000 * 20)
#define SYNC_SLOW_DOWN_RANGE         100
#define SYNC_MAX_READ_RANGE          2
#define SYNC_MAX_PROGRESS_WAIT_MS    4000
#define SYNC_MAX_START_TIME_RANGE_MS (1000 * 20)
#define SYNC_MAX_RECV_TIME_RANGE_MS  1200
#define SYNC_ADD_QUORUM_COUNT        3
#define SYNC_VNODE_LOG_RETENTION     (TSDB_SYNC_LOG_BUFFER_RETENTION + 1)
#define SNAPSHOT_WAIT_MS             1000 * 5

#define SYNC_WAL_LOG_RETENTION_SIZE (8LL * 1024 * 1024 * 1024)

#define SYNC_MAX_RETRY_BACKOFF         5
#define SYNC_LOG_REPL_RETRY_WAIT_MS    100
#define SYNC_APPEND_ENTRIES_TIMEOUT_MS 10000
#define SYNC_HEART_TIMEOUT_MS          1000 * 15

#define SYNC_HEARTBEAT_SLOW_MS       1500
#define SYNC_HEARTBEAT_REPLY_SLOW_MS 1500
#define SYNC_SNAP_RESEND_MS          1000 * 60
#define SYNC_SNAP_TIMEOUT_MS         1000 * 300

#define SYNC_VND_COMMIT_MIN_MS 3000

#define SYNC_MAX_BATCH_SIZE 1
#define SYNC_INDEX_BEGIN    0
#define SYNC_INDEX_INVALID  -1
#define SYNC_TERM_INVALID   -1

#define SYNC_LEARNER_CATCHUP 10

typedef enum {
  SYNC_STRATEGY_NO_SNAPSHOT = 0,
  SYNC_STRATEGY_STANDARD_SNAPSHOT = 1,
  SYNC_STRATEGY_WAL_FIRST = 2,
} ESyncStrategy;

typedef uint64_t SyncNodeId;
typedef int32_t  SyncGroupId;
typedef int64_t  SyncIndex;
typedef int64_t  SyncTerm;

typedef struct SSyncNode      SSyncNode;
typedef struct SWal           SWal;
typedef struct SSyncRaftEntry SSyncRaftEntry;

typedef enum {
  TAOS_SYNC_STATE_OFFLINE = 0,
  TAOS_SYNC_STATE_FOLLOWER = 100,
  TAOS_SYNC_STATE_CANDIDATE = 101,
  TAOS_SYNC_STATE_LEADER = 102,
  TAOS_SYNC_STATE_ERROR = 103,
  TAOS_SYNC_STATE_LEARNER = 104,
} ESyncState;

typedef enum {
  TAOS_SYNC_ROLE_VOTER = 0,
  TAOS_SYNC_ROLE_LEARNER = 1,
  TAOS_SYNC_ROLE_ERROR = 2,
} ESyncRole;

typedef enum {
  SYNC_FSM_STATE_COMPLETE = 0,
  SYNC_FSM_STATE_INCOMPLETE,
} ESyncFsmState;

typedef struct SNodeInfo {
  int64_t   clusterId;
  int32_t   nodeId;
  uint16_t  nodePort;
  char      nodeFqdn[TSDB_FQDN_LEN];
  ESyncRole nodeRole;
} SNodeInfo;

typedef struct SSyncTLV {
  int32_t typ;
  int32_t len;
  char    val[];
} SSyncTLV;

typedef struct SSyncCfg {
  int32_t   totalReplicaNum;
  int32_t   replicaNum;
  int32_t   myIndex;
  SNodeInfo nodeInfo[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];
  SyncIndex lastIndex;
  int32_t   changeVersion;
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
  SSyncTLV* data;
} SSnapshotParam;

typedef struct SSnapshot {
  int32_t       type;
  SSyncTLV* data;
  ESyncFsmState state;
  SyncIndex lastApplyIndex;
  SyncTerm  lastApplyTerm;
  SyncIndex lastConfigIndex;
} SSnapshot;

typedef struct SSnapshotMeta {
  SyncIndex lastConfigIndex;
} SSnapshotMeta;

typedef struct SSyncFSM {
  void* data;

  int32_t (*FpCommitCb)(const struct SSyncFSM* pFsm, SRpcMsg* pMsg, SFsmCbMeta* pMeta);
  SyncIndex (*FpAppliedIndexCb)(const struct SSyncFSM* pFsm);
  int32_t (*FpPreCommitCb)(const struct SSyncFSM* pFsm, SRpcMsg* pMsg, SFsmCbMeta* pMeta);
  void (*FpRollBackCb)(const struct SSyncFSM* pFsm, SRpcMsg* pMsg, SFsmCbMeta* pMeta);

  void (*FpRestoreFinishCb)(const struct SSyncFSM* pFsm, const SyncIndex commitIdx);
  void (*FpReConfigCb)(const struct SSyncFSM* pFsm, SRpcMsg* pMsg, SReConfigCbMeta* pMeta);
  void (*FpLeaderTransferCb)(const struct SSyncFSM* pFsm, SRpcMsg* pMsg, SFsmCbMeta* pMeta);
  bool (*FpApplyQueueEmptyCb)(const struct SSyncFSM* pFsm);
  int32_t (*FpApplyQueueItems)(const struct SSyncFSM* pFsm);

  void (*FpBecomeLeaderCb)(const struct SSyncFSM* pFsm);
  void (*FpBecomeFollowerCb)(const struct SSyncFSM* pFsm);
  void (*FpBecomeLearnerCb)(const struct SSyncFSM* pFsm);

  int32_t (*FpGetSnapshot)(const struct SSyncFSM* pFsm, SSnapshot* pSnapshot, void* pReaderParam, void** ppReader);
  int32_t (*FpGetSnapshotInfo)(const struct SSyncFSM* pFsm, SSnapshot* pSnapshot);

  int32_t (*FpSnapshotStartRead)(const struct SSyncFSM* pFsm, void* pReaderParam, void** ppReader);
  void (*FpSnapshotStopRead)(const struct SSyncFSM* pFsm, void* pReader);
  int32_t (*FpSnapshotDoRead)(const struct SSyncFSM* pFsm, void* pReader, void** ppBuf, int32_t* len);

  int32_t (*FpSnapshotStartWrite)(const struct SSyncFSM* pFsm, void* pWriterParam, void** ppWriter);
  int32_t (*FpSnapshotStopWrite)(const struct SSyncFSM* pFsm, void* pWriter, bool isApply, SSnapshot* pSnapshot);
  int32_t (*FpSnapshotDoWrite)(const struct SSyncFSM* pFsm, void* pWriter, void* pBuf, int32_t len);

} SSyncFSM;

// abstract definition of log store in raft
// SWal implements it
typedef struct SSyncLogStore {
  SLRUCache* pCache;
  int32_t    cacheHit;
  int32_t    cacheMiss;

  void* data;

  int32_t (*syncLogUpdateCommitIndex)(struct SSyncLogStore* pLogStore, SyncIndex index);
  SyncIndex (*syncLogCommitIndex)(struct SSyncLogStore* pLogStore);

  SyncIndex (*syncLogBeginIndex)(struct SSyncLogStore* pLogStore);
  SyncIndex (*syncLogEndIndex)(struct SSyncLogStore* pLogStore);

  int32_t (*syncLogEntryCount)(struct SSyncLogStore* pLogStore);
  int32_t (*syncLogRestoreFromSnapshot)(struct SSyncLogStore* pLogStore, SyncIndex index);
  bool (*syncLogIsEmpty)(struct SSyncLogStore* pLogStore);
  bool (*syncLogExist)(struct SSyncLogStore* pLogStore, SyncIndex index);

  SyncIndex (*syncLogWriteIndex)(struct SSyncLogStore* pLogStore);
  SyncIndex (*syncLogLastIndex)(struct SSyncLogStore* pLogStore);
  SyncIndex (*syncLogIndexRetention)(struct SSyncLogStore* pLogStore, int64_t bytes);
  SyncTerm (*syncLogLastTerm)(struct SSyncLogStore* pLogStore);

  int32_t (*syncLogAppendEntry)(struct SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry, bool forcSync);
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
  int32_t       pingMs;
  int32_t       electMs;
  int32_t       heartbeatMs;

  int32_t (*syncSendMSg)(const SEpSet* pEpSet, SRpcMsg* pMsg);
  int32_t (*syncEqMsg)(const SMsgCb* msgcb, SRpcMsg* pMsg);
  int32_t (*syncEqCtrlMsg)(const SMsgCb* msgcb, SRpcMsg* pMsg);
} SSyncInfo;

// if state == leader
//     if restored, display "leader"
//     if !restored && canRead, display "leader*"
//     if !restored && !canRead, display "leader**"
typedef struct SSyncState {
  ESyncState state;
  bool       restored;
  bool       canRead;
  int32_t    progress;
  SyncTerm   term;
  int64_t    roleTimeMs;
  int64_t    startTimeMs;
} SSyncState;

int32_t syncInit();
void    syncCleanUp();
int64_t syncOpen(SSyncInfo* pSyncInfo, int32_t vnodeVersion);
int32_t syncStart(int64_t rid);
void    syncStop(int64_t rid);
void    syncPreStop(int64_t rid);
void    syncPostStop(int64_t rid);
int32_t syncPropose(int64_t rid, SRpcMsg* pMsg, bool isWeak, int64_t* seq);
int32_t syncCheckMember(int64_t rid);
int32_t syncIsCatchUp(int64_t rid);
ESyncRole syncGetRole(int64_t rid);
int32_t   syncProcessMsg(int64_t rid, SRpcMsg* pMsg);
int32_t   syncReconfig(int64_t rid, SSyncCfg* pCfg);
int32_t   syncBeginSnapshot(int64_t rid, int64_t lastApplyIndex);
int32_t   syncEndSnapshot(int64_t rid);
int32_t   syncLeaderTransfer(int64_t rid);
int32_t   syncStepDown(int64_t rid, SyncTerm newTerm);
bool      syncIsReadyForRead(int64_t rid);
bool      syncSnapshotSending(int64_t rid);
bool      syncSnapshotRecving(int64_t rid);
int32_t   syncSendTimeoutRsp(int64_t rid, int64_t seq);
int32_t   syncForceBecomeFollower(SSyncNode* ths, const SRpcMsg* pRpcMsg);

SSyncState  syncGetState(int64_t rid);
void        syncGetRetryEpSet(int64_t rid, SEpSet* pEpSet);
const char* syncStr(ESyncState state);

int32_t    syncNodeGetConfig(int64_t rid, SSyncCfg *cfg);

// util
int32_t syncSnapInfoDataRealloc(SSnapshot* pSnap, int32_t size);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_H*/
