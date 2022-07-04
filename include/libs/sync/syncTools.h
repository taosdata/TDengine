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

#ifndef _TD_LIBS_SYNC_TOOLS_H
#define _TD_LIBS_SYNC_TOOLS_H

#ifdef __cplusplus
extern "C" {
#endif

#include "trpc.h"

// ------------------ ds -------------------
typedef struct SRaftId {
  SyncNodeId  addr;
  SyncGroupId vgId;
} SRaftId;

// ------------------ control -------------------
SSyncNode* syncNodeAcquire(int64_t rid);
void       syncNodeRelease(SSyncNode* pNode);

int32_t syncGetRespRpc(int64_t rid, uint64_t index, SRpcMsg* msg);
int32_t syncGetAndDelRespRpc(int64_t rid, uint64_t index, SRpcHandleInfo* pInfo);
void    syncSetMsgCb(int64_t rid, const SMsgCb* msgcb);
char*   sync2SimpleStr(int64_t rid);

// set timer ms
void setPingTimerMS(int64_t rid, int32_t pingTimerMS);
void setElectTimerMS(int64_t rid, int32_t electTimerMS);
void setHeartbeatTimerMS(int64_t rid, int32_t hbTimerMS);

// for compatibility, the same as syncPropose
int32_t syncForwardToPeer(int64_t rid, SRpcMsg* pMsg, bool isWeak);

// utils
const char* syncUtilState2String(ESyncState state);

// ------------------ for debug -------------------
void syncRpcMsgPrint(SRpcMsg* pMsg);
void syncRpcMsgPrint2(char* s, SRpcMsg* pMsg);
void syncRpcMsgLog(SRpcMsg* pMsg);
void syncRpcMsgLog2(char* s, SRpcMsg* pMsg);

// ------------------ for compile -------------------
typedef struct SSyncBuffer {
  void*  data;
  size_t len;
} SSyncBuffer;

typedef struct SNodesRole {
  int32_t    replicaNum;
  SNodeInfo  nodeInfo[TSDB_MAX_REPLICA];
  ESyncState role[TSDB_MAX_REPLICA];
} SNodesRole;

typedef struct SStateMgr {
  void* data;

  int32_t (*getCurrentTerm)(struct SStateMgr* pMgr, SyncTerm* pCurrentTerm);
  int32_t (*persistCurrentTerm)(struct SStateMgr* pMgr, SyncTerm pCurrentTerm);

  int32_t (*getVoteFor)(struct SStateMgr* pMgr, SyncNodeId* pVoteFor);
  int32_t (*persistVoteFor)(struct SStateMgr* pMgr, SyncNodeId voteFor);

  int32_t (*getSyncCfg)(struct SStateMgr* pMgr, SSyncCfg* pSyncCfg);
  int32_t (*persistSyncCfg)(struct SStateMgr* pMgr, SSyncCfg* pSyncCfg);

} SStateMgr;

// ------------------ for message process -------------------

// ---------------------------------------------
typedef struct SyncPing {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;
  // private data
  uint32_t dataLen;
  char     data[];
} SyncPing;

SyncPing* syncPingBuild(uint32_t dataLen);
SyncPing* syncPingBuild2(const SRaftId* srcId, const SRaftId* destId, int32_t vgId, const char* str);
SyncPing* syncPingBuild3(const SRaftId* srcId, const SRaftId* destId, int32_t vgId);
void      syncPingDestroy(SyncPing* pMsg);
void      syncPingSerialize(const SyncPing* pMsg, char* buf, uint32_t bufLen);
void      syncPingDeserialize(const char* buf, uint32_t len, SyncPing* pMsg);
char*     syncPingSerialize2(const SyncPing* pMsg, uint32_t* len);
SyncPing* syncPingDeserialize2(const char* buf, uint32_t len);
int32_t   syncPingSerialize3(const SyncPing* pMsg, char* buf, int32_t bufLen);
SyncPing* syncPingDeserialize3(void* buf, int32_t bufLen);
void      syncPing2RpcMsg(const SyncPing* pMsg, SRpcMsg* pRpcMsg);
void      syncPingFromRpcMsg(const SRpcMsg* pRpcMsg, SyncPing* pMsg);
SyncPing* syncPingFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*    syncPing2Json(const SyncPing* pMsg);
char*     syncPing2Str(const SyncPing* pMsg);

// for debug ----------------------
void syncPingPrint(const SyncPing* pMsg);
void syncPingPrint2(char* s, const SyncPing* pMsg);
void syncPingLog(const SyncPing* pMsg);
void syncPingLog2(char* s, const SyncPing* pMsg);

// ---------------------------------------------
typedef struct SyncPingReply {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;
  // private data
  uint32_t dataLen;
  char     data[];
} SyncPingReply;

SyncPingReply* syncPingReplyBuild(uint32_t dataLen);
SyncPingReply* syncPingReplyBuild2(const SRaftId* srcId, const SRaftId* destId, int32_t vgId, const char* str);
SyncPingReply* syncPingReplyBuild3(const SRaftId* srcId, const SRaftId* destId, int32_t vgId);
void           syncPingReplyDestroy(SyncPingReply* pMsg);
void           syncPingReplySerialize(const SyncPingReply* pMsg, char* buf, uint32_t bufLen);
void           syncPingReplyDeserialize(const char* buf, uint32_t len, SyncPingReply* pMsg);
char*          syncPingReplySerialize2(const SyncPingReply* pMsg, uint32_t* len);
SyncPingReply* syncPingReplyDeserialize2(const char* buf, uint32_t len);
int32_t        syncPingReplySerialize3(const SyncPingReply* pMsg, char* buf, int32_t bufLen);
SyncPingReply* syncPingReplyDeserialize3(void* buf, int32_t bufLen);
void           syncPingReply2RpcMsg(const SyncPingReply* pMsg, SRpcMsg* pRpcMsg);
void           syncPingReplyFromRpcMsg(const SRpcMsg* pRpcMsg, SyncPingReply* pMsg);
SyncPingReply* syncPingReplyFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*         syncPingReply2Json(const SyncPingReply* pMsg);
char*          syncPingReply2Str(const SyncPingReply* pMsg);

// for debug ----------------------
void syncPingReplyPrint(const SyncPingReply* pMsg);
void syncPingReplyPrint2(char* s, const SyncPingReply* pMsg);
void syncPingReplyLog(const SyncPingReply* pMsg);
void syncPingReplyLog2(char* s, const SyncPingReply* pMsg);

// ---------------------------------------------
typedef enum ESyncTimeoutType {
  SYNC_TIMEOUT_PING = 100,
  SYNC_TIMEOUT_ELECTION,
  SYNC_TIMEOUT_HEARTBEAT,
} ESyncTimeoutType;

typedef struct SyncTimeout {
  uint32_t         bytes;
  int32_t          vgId;
  uint32_t         msgType;
  ESyncTimeoutType timeoutType;
  uint64_t         logicClock;
  int32_t          timerMS;
  void*            data;  // need optimized
} SyncTimeout;

SyncTimeout* syncTimeoutBuild();
SyncTimeout* syncTimeoutBuild2(ESyncTimeoutType timeoutType, uint64_t logicClock, int32_t timerMS, int32_t vgId,
                               void* data);
void         syncTimeoutDestroy(SyncTimeout* pMsg);
void         syncTimeoutSerialize(const SyncTimeout* pMsg, char* buf, uint32_t bufLen);
void         syncTimeoutDeserialize(const char* buf, uint32_t len, SyncTimeout* pMsg);
char*        syncTimeoutSerialize2(const SyncTimeout* pMsg, uint32_t* len);
SyncTimeout* syncTimeoutDeserialize2(const char* buf, uint32_t len);
void         syncTimeout2RpcMsg(const SyncTimeout* pMsg, SRpcMsg* pRpcMsg);
void         syncTimeoutFromRpcMsg(const SRpcMsg* pRpcMsg, SyncTimeout* pMsg);
SyncTimeout* syncTimeoutFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*       syncTimeout2Json(const SyncTimeout* pMsg);
char*        syncTimeout2Str(const SyncTimeout* pMsg);

// for debug ----------------------
void syncTimeoutPrint(const SyncTimeout* pMsg);
void syncTimeoutPrint2(char* s, const SyncTimeout* pMsg);
void syncTimeoutLog(const SyncTimeout* pMsg);
void syncTimeoutLog2(char* s, const SyncTimeout* pMsg);

// ---------------------------------------------
typedef struct SyncClientRequest {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;          // TDMT_SYNC_CLIENT_REQUEST
  uint32_t originalRpcType;  // origin RpcMsg msgType
  uint64_t seqNum;
  bool     isWeak;
  uint32_t dataLen;  // origin RpcMsg.contLen
  char     data[];   // origin RpcMsg.pCont
} SyncClientRequest;

SyncClientRequest* syncClientRequestBuild(uint32_t dataLen);
SyncClientRequest* syncClientRequestBuild2(const SRpcMsg* pOriginalRpcMsg, uint64_t seqNum, bool isWeak,
                                           int32_t vgId);  // step 1
void               syncClientRequestDestroy(SyncClientRequest* pMsg);
void               syncClientRequestSerialize(const SyncClientRequest* pMsg, char* buf, uint32_t bufLen);
void               syncClientRequestDeserialize(const char* buf, uint32_t len, SyncClientRequest* pMsg);
char*              syncClientRequestSerialize2(const SyncClientRequest* pMsg, uint32_t* len);
SyncClientRequest* syncClientRequestDeserialize2(const char* buf, uint32_t len);
void               syncClientRequest2RpcMsg(const SyncClientRequest* pMsg, SRpcMsg* pRpcMsg);  // step 2
void               syncClientRequestFromRpcMsg(const SRpcMsg* pRpcMsg, SyncClientRequest* pMsg);
SyncClientRequest* syncClientRequestFromRpcMsg2(const SRpcMsg* pRpcMsg);  // step 3
cJSON*             syncClientRequest2Json(const SyncClientRequest* pMsg);
char*              syncClientRequest2Str(const SyncClientRequest* pMsg);

// for debug ----------------------
void syncClientRequestPrint(const SyncClientRequest* pMsg);
void syncClientRequestPrint2(char* s, const SyncClientRequest* pMsg);
void syncClientRequestLog(const SyncClientRequest* pMsg);
void syncClientRequestLog2(char* s, const SyncClientRequest* pMsg);

// ---------------------------------------------
typedef struct SRaftMeta {
  uint64_t seqNum;
  bool     isWeak;
} SRaftMeta;

// block1:
// block2: SRaftMeta array
// block3: rpc msg array (with pCont pointer)

typedef struct SyncClientRequestBatch {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;  // TDMT_SYNC_CLIENT_REQUEST_BATCH
  uint32_t dataCount;
  uint32_t dataLen;
  char     data[];  // block2, block3
} SyncClientRequestBatch;

SyncClientRequestBatch* syncClientRequestBatchBuild(SRpcMsg* rpcMsgArr, SRaftMeta* raftArr, int32_t arrSize,
                                                    int32_t vgId);
void                    syncClientRequestBatch2RpcMsg(const SyncClientRequestBatch* pSyncMsg, SRpcMsg* pRpcMsg);
void                    syncClientRequestBatchDestroy(SyncClientRequestBatch* pMsg);
void                    syncClientRequestBatchDestroyDeep(SyncClientRequestBatch* pMsg);
SRaftMeta*              syncClientRequestBatchMetaArr(const SyncClientRequestBatch* pSyncMsg);
SRpcMsg*                syncClientRequestBatchRpcMsgArr(const SyncClientRequestBatch* pSyncMsg);
SyncClientRequestBatch* syncClientRequestBatchFromRpcMsg(const SRpcMsg* pRpcMsg);
cJSON*                  syncClientRequestBatch2Json(const SyncClientRequestBatch* pMsg);
char*                   syncClientRequestBatch2Str(const SyncClientRequestBatch* pMsg);

// for debug ----------------------
void syncClientRequestBatchPrint(const SyncClientRequestBatch* pMsg);
void syncClientRequestBatchPrint2(char* s, const SyncClientRequestBatch* pMsg);
void syncClientRequestBatchLog(const SyncClientRequestBatch* pMsg);
void syncClientRequestBatchLog2(char* s, const SyncClientRequestBatch* pMsg);

// ---------------------------------------------
typedef struct SyncClientRequestReply {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  int32_t  errCode;
  SRaftId  leaderHint;
} SyncClientRequestReply;

// ---------------------------------------------
typedef struct SyncRequestVote {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;
  // private data
  SyncTerm  term;
  SyncIndex lastLogIndex;
  SyncTerm  lastLogTerm;
} SyncRequestVote;

SyncRequestVote* syncRequestVoteBuild(int32_t vgId);
void             syncRequestVoteDestroy(SyncRequestVote* pMsg);
void             syncRequestVoteSerialize(const SyncRequestVote* pMsg, char* buf, uint32_t bufLen);
void             syncRequestVoteDeserialize(const char* buf, uint32_t len, SyncRequestVote* pMsg);
char*            syncRequestVoteSerialize2(const SyncRequestVote* pMsg, uint32_t* len);
SyncRequestVote* syncRequestVoteDeserialize2(const char* buf, uint32_t len);
void             syncRequestVote2RpcMsg(const SyncRequestVote* pMsg, SRpcMsg* pRpcMsg);
void             syncRequestVoteFromRpcMsg(const SRpcMsg* pRpcMsg, SyncRequestVote* pMsg);
SyncRequestVote* syncRequestVoteFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*           syncRequestVote2Json(const SyncRequestVote* pMsg);
char*            syncRequestVote2Str(const SyncRequestVote* pMsg);

// for debug ----------------------
void syncRequestVotePrint(const SyncRequestVote* pMsg);
void syncRequestVotePrint2(char* s, const SyncRequestVote* pMsg);
void syncRequestVoteLog(const SyncRequestVote* pMsg);
void syncRequestVoteLog2(char* s, const SyncRequestVote* pMsg);

// ---------------------------------------------
typedef struct SyncRequestVoteReply {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;
  // private data
  SyncTerm term;
  bool     voteGranted;
} SyncRequestVoteReply;

SyncRequestVoteReply* syncRequestVoteReplyBuild(int32_t vgId);
void                  syncRequestVoteReplyDestroy(SyncRequestVoteReply* pMsg);
void                  syncRequestVoteReplySerialize(const SyncRequestVoteReply* pMsg, char* buf, uint32_t bufLen);
void                  syncRequestVoteReplyDeserialize(const char* buf, uint32_t len, SyncRequestVoteReply* pMsg);
char*                 syncRequestVoteReplySerialize2(const SyncRequestVoteReply* pMsg, uint32_t* len);
SyncRequestVoteReply* syncRequestVoteReplyDeserialize2(const char* buf, uint32_t len);
void                  syncRequestVoteReply2RpcMsg(const SyncRequestVoteReply* pMsg, SRpcMsg* pRpcMsg);
void                  syncRequestVoteReplyFromRpcMsg(const SRpcMsg* pRpcMsg, SyncRequestVoteReply* pMsg);
SyncRequestVoteReply* syncRequestVoteReplyFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*                syncRequestVoteReply2Json(const SyncRequestVoteReply* pMsg);
char*                 syncRequestVoteReply2Str(const SyncRequestVoteReply* pMsg);

// for debug ----------------------
void syncRequestVoteReplyPrint(const SyncRequestVoteReply* pMsg);
void syncRequestVoteReplyPrint2(char* s, const SyncRequestVoteReply* pMsg);
void syncRequestVoteReplyLog(const SyncRequestVoteReply* pMsg);
void syncRequestVoteReplyLog2(char* s, const SyncRequestVoteReply* pMsg);

// ---------------------------------------------
// data: entry

typedef struct SyncAppendEntries {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;

  // private data
  SyncTerm  term;
  SyncIndex prevLogIndex;
  SyncTerm  prevLogTerm;
  SyncIndex commitIndex;
  SyncTerm  privateTerm;
  uint32_t  dataLen;
  char      data[];
} SyncAppendEntries;

SyncAppendEntries* syncAppendEntriesBuild(uint32_t dataLen, int32_t vgId);
void               syncAppendEntriesDestroy(SyncAppendEntries* pMsg);
void               syncAppendEntriesSerialize(const SyncAppendEntries* pMsg, char* buf, uint32_t bufLen);
void               syncAppendEntriesDeserialize(const char* buf, uint32_t len, SyncAppendEntries* pMsg);
char*              syncAppendEntriesSerialize2(const SyncAppendEntries* pMsg, uint32_t* len);
SyncAppendEntries* syncAppendEntriesDeserialize2(const char* buf, uint32_t len);
void               syncAppendEntries2RpcMsg(const SyncAppendEntries* pMsg, SRpcMsg* pRpcMsg);
void               syncAppendEntriesFromRpcMsg(const SRpcMsg* pRpcMsg, SyncAppendEntries* pMsg);
SyncAppendEntries* syncAppendEntriesFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*             syncAppendEntries2Json(const SyncAppendEntries* pMsg);
char*              syncAppendEntries2Str(const SyncAppendEntries* pMsg);

// for debug ----------------------
void syncAppendEntriesPrint(const SyncAppendEntries* pMsg);
void syncAppendEntriesPrint2(char* s, const SyncAppendEntries* pMsg);
void syncAppendEntriesLog(const SyncAppendEntries* pMsg);
void syncAppendEntriesLog2(char* s, const SyncAppendEntries* pMsg);

// ---------------------------------------------

typedef struct SOffsetAndContLen {
  int32_t offset;
  int32_t contLen;
} SOffsetAndContLen;

// data:
// block1: SOffsetAndContLen Array
// block2: entry Array

typedef struct SyncAppendEntriesBatch {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;

  // private data
  SyncTerm  term;
  SyncIndex prevLogIndex;
  SyncTerm  prevLogTerm;
  SyncIndex commitIndex;
  SyncTerm  privateTerm;
  int32_t   dataCount;
  uint32_t  dataLen;
  char      data[];  // block1, block2
} SyncAppendEntriesBatch;

SyncAppendEntriesBatch* syncAppendEntriesBatchBuild(SSyncRaftEntry** entryPArr, int32_t arrSize, int32_t vgId);
SOffsetAndContLen*      syncAppendEntriesBatchMetaTableArray(SyncAppendEntriesBatch* pMsg);
void                    syncAppendEntriesBatchDestroy(SyncAppendEntriesBatch* pMsg);
void                    syncAppendEntriesBatchSerialize(const SyncAppendEntriesBatch* pMsg, char* buf, uint32_t bufLen);
void                    syncAppendEntriesBatchDeserialize(const char* buf, uint32_t len, SyncAppendEntriesBatch* pMsg);
char*                   syncAppendEntriesBatchSerialize2(const SyncAppendEntriesBatch* pMsg, uint32_t* len);
SyncAppendEntriesBatch* syncAppendEntriesBatchDeserialize2(const char* buf, uint32_t len);
void                    syncAppendEntriesBatch2RpcMsg(const SyncAppendEntriesBatch* pMsg, SRpcMsg* pRpcMsg);
void                    syncAppendEntriesBatchFromRpcMsg(const SRpcMsg* pRpcMsg, SyncAppendEntriesBatch* pMsg);
SyncAppendEntriesBatch* syncAppendEntriesBatchFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*                  syncAppendEntriesBatch2Json(const SyncAppendEntriesBatch* pMsg);
char*                   syncAppendEntriesBatch2Str(const SyncAppendEntriesBatch* pMsg);

// for debug ----------------------
void syncAppendEntriesBatchPrint(const SyncAppendEntriesBatch* pMsg);
void syncAppendEntriesBatchPrint2(char* s, const SyncAppendEntriesBatch* pMsg);
void syncAppendEntriesBatchLog(const SyncAppendEntriesBatch* pMsg);
void syncAppendEntriesBatchLog2(char* s, const SyncAppendEntriesBatch* pMsg);

// ---------------------------------------------
typedef struct SyncAppendEntriesReply {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;
  // private data
  SyncTerm  term;
  SyncTerm  privateTerm;
  bool      success;
  SyncIndex matchIndex;
} SyncAppendEntriesReply;

SyncAppendEntriesReply* syncAppendEntriesReplyBuild(int32_t vgId);
void                    syncAppendEntriesReplyDestroy(SyncAppendEntriesReply* pMsg);
void                    syncAppendEntriesReplySerialize(const SyncAppendEntriesReply* pMsg, char* buf, uint32_t bufLen);
void                    syncAppendEntriesReplyDeserialize(const char* buf, uint32_t len, SyncAppendEntriesReply* pMsg);
char*                   syncAppendEntriesReplySerialize2(const SyncAppendEntriesReply* pMsg, uint32_t* len);
SyncAppendEntriesReply* syncAppendEntriesReplyDeserialize2(const char* buf, uint32_t len);
void                    syncAppendEntriesReply2RpcMsg(const SyncAppendEntriesReply* pMsg, SRpcMsg* pRpcMsg);
void                    syncAppendEntriesReplyFromRpcMsg(const SRpcMsg* pRpcMsg, SyncAppendEntriesReply* pMsg);
SyncAppendEntriesReply* syncAppendEntriesReplyFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*                  syncAppendEntriesReply2Json(const SyncAppendEntriesReply* pMsg);
char*                   syncAppendEntriesReply2Str(const SyncAppendEntriesReply* pMsg);

// for debug ----------------------
void syncAppendEntriesReplyPrint(const SyncAppendEntriesReply* pMsg);
void syncAppendEntriesReplyPrint2(char* s, const SyncAppendEntriesReply* pMsg);
void syncAppendEntriesReplyLog(const SyncAppendEntriesReply* pMsg);
void syncAppendEntriesReplyLog2(char* s, const SyncAppendEntriesReply* pMsg);

// ---------------------------------------------
typedef struct SyncApplyMsg {
  uint32_t   bytes;
  int32_t    vgId;
  uint32_t   msgType;          // user SyncApplyMsg msgType
  uint32_t   originalRpcType;  // user RpcMsg msgType
  SFsmCbMeta fsmMeta;
  uint32_t   dataLen;  // user RpcMsg.contLen
  char       data[];   // user RpcMsg.pCont
} SyncApplyMsg;

SyncApplyMsg* syncApplyMsgBuild(uint32_t dataLen);
SyncApplyMsg* syncApplyMsgBuild2(const SRpcMsg* pOriginalRpcMsg, int32_t vgId, SFsmCbMeta* pMeta);
void          syncApplyMsgDestroy(SyncApplyMsg* pMsg);
void          syncApplyMsgSerialize(const SyncApplyMsg* pMsg, char* buf, uint32_t bufLen);
void          syncApplyMsgDeserialize(const char* buf, uint32_t len, SyncApplyMsg* pMsg);
char*         syncApplyMsgSerialize2(const SyncApplyMsg* pMsg, uint32_t* len);
SyncApplyMsg* syncApplyMsgDeserialize2(const char* buf, uint32_t len);
void syncApplyMsg2RpcMsg(const SyncApplyMsg* pMsg, SRpcMsg* pRpcMsg);     // SyncApplyMsg to SRpcMsg, put it into ApplyQ
void syncApplyMsgFromRpcMsg(const SRpcMsg* pRpcMsg, SyncApplyMsg* pMsg);  // get SRpcMsg from ApplyQ, to SyncApplyMsg
SyncApplyMsg* syncApplyMsgFromRpcMsg2(const SRpcMsg* pRpcMsg);
void syncApplyMsg2OriginalRpcMsg(const SyncApplyMsg* pMsg, SRpcMsg* pOriginalRpcMsg);  // SyncApplyMsg to OriginalRpcMsg
cJSON* syncApplyMsg2Json(const SyncApplyMsg* pMsg);
char*  syncApplyMsg2Str(const SyncApplyMsg* pMsg);

// for debug ----------------------
void syncApplyMsgPrint(const SyncApplyMsg* pMsg);
void syncApplyMsgPrint2(char* s, const SyncApplyMsg* pMsg);
void syncApplyMsgLog(const SyncApplyMsg* pMsg);
void syncApplyMsgLog2(char* s, const SyncApplyMsg* pMsg);

// ---------------------------------------------
typedef struct SyncSnapshotSend {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;

  SyncTerm  term;
  SyncIndex beginIndex;       // snapshot.beginIndex
  SyncIndex lastIndex;        // snapshot.lastIndex
  SyncTerm  lastTerm;         // snapshot.lastTerm
  SyncIndex lastConfigIndex;  // snapshot.lastConfigIndex
  SSyncCfg  lastConfig;
  SyncTerm  privateTerm;
  int32_t   seq;
  uint32_t  dataLen;
  char      data[];
} SyncSnapshotSend;

SyncSnapshotSend* syncSnapshotSendBuild(uint32_t dataLen, int32_t vgId);
void              syncSnapshotSendDestroy(SyncSnapshotSend* pMsg);
void              syncSnapshotSendSerialize(const SyncSnapshotSend* pMsg, char* buf, uint32_t bufLen);
void              syncSnapshotSendDeserialize(const char* buf, uint32_t len, SyncSnapshotSend* pMsg);
char*             syncSnapshotSendSerialize2(const SyncSnapshotSend* pMsg, uint32_t* len);
SyncSnapshotSend* syncSnapshotSendDeserialize2(const char* buf, uint32_t len);
void              syncSnapshotSend2RpcMsg(const SyncSnapshotSend* pMsg, SRpcMsg* pRpcMsg);
void              syncSnapshotSendFromRpcMsg(const SRpcMsg* pRpcMsg, SyncSnapshotSend* pMsg);
SyncSnapshotSend* syncSnapshotSendFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*            syncSnapshotSend2Json(const SyncSnapshotSend* pMsg);
char*             syncSnapshotSend2Str(const SyncSnapshotSend* pMsg);

// for debug ----------------------
void syncSnapshotSendPrint(const SyncSnapshotSend* pMsg);
void syncSnapshotSendPrint2(char* s, const SyncSnapshotSend* pMsg);
void syncSnapshotSendLog(const SyncSnapshotSend* pMsg);
void syncSnapshotSendLog2(char* s, const SyncSnapshotSend* pMsg);

// ---------------------------------------------
typedef struct SyncSnapshotRsp {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;

  SyncTerm  term;
  SyncIndex lastIndex;
  SyncTerm  lastTerm;
  SyncTerm  privateTerm;
  int32_t   ack;
  int32_t   code;
} SyncSnapshotRsp;

SyncSnapshotRsp* syncSnapshotRspBuild(int32_t vgId);
void             syncSnapshotRspDestroy(SyncSnapshotRsp* pMsg);
void             syncSnapshotRspSerialize(const SyncSnapshotRsp* pMsg, char* buf, uint32_t bufLen);
void             syncSnapshotRspDeserialize(const char* buf, uint32_t len, SyncSnapshotRsp* pMsg);
char*            syncSnapshotRspSerialize2(const SyncSnapshotRsp* pMsg, uint32_t* len);
SyncSnapshotRsp* syncSnapshotRspDeserialize2(const char* buf, uint32_t len);
void             syncSnapshotRsp2RpcMsg(const SyncSnapshotRsp* pMsg, SRpcMsg* pRpcMsg);
void             syncSnapshotRspFromRpcMsg(const SRpcMsg* pRpcMsg, SyncSnapshotRsp* pMsg);
SyncSnapshotRsp* syncSnapshotRspFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*           syncSnapshotRsp2Json(const SyncSnapshotRsp* pMsg);
char*            syncSnapshotRsp2Str(const SyncSnapshotRsp* pMsg);

// for debug ----------------------
void syncSnapshotRspPrint(const SyncSnapshotRsp* pMsg);
void syncSnapshotRspPrint2(char* s, const SyncSnapshotRsp* pMsg);
void syncSnapshotRspLog(const SyncSnapshotRsp* pMsg);
void syncSnapshotRspLog2(char* s, const SyncSnapshotRsp* pMsg);

// ---------------------------------------------
typedef struct SyncLeaderTransfer {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  /*
   SRaftId  srcId;
   SRaftId  destId;
   */
  SNodeInfo newNodeInfo;
  SRaftId   newLeaderId;
} SyncLeaderTransfer;

SyncLeaderTransfer* syncLeaderTransferBuild(int32_t vgId);
void                syncLeaderTransferDestroy(SyncLeaderTransfer* pMsg);
void                syncLeaderTransferSerialize(const SyncLeaderTransfer* pMsg, char* buf, uint32_t bufLen);
void                syncLeaderTransferDeserialize(const char* buf, uint32_t len, SyncLeaderTransfer* pMsg);
char*               syncLeaderTransferSerialize2(const SyncLeaderTransfer* pMsg, uint32_t* len);
SyncLeaderTransfer* syncLeaderTransferDeserialize2(const char* buf, uint32_t len);
void                syncLeaderTransfer2RpcMsg(const SyncLeaderTransfer* pMsg, SRpcMsg* pRpcMsg);
void                syncLeaderTransferFromRpcMsg(const SRpcMsg* pRpcMsg, SyncLeaderTransfer* pMsg);
SyncLeaderTransfer* syncLeaderTransferFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*              syncLeaderTransfer2Json(const SyncLeaderTransfer* pMsg);
char*               syncLeaderTransfer2Str(const SyncLeaderTransfer* pMsg);

// for debug ----------------------
void syncLeaderTransferPrint(const SyncLeaderTransfer* pMsg);
void syncLeaderTransferPrint2(char* s, const SyncLeaderTransfer* pMsg);
void syncLeaderTransferLog(const SyncLeaderTransfer* pMsg);
void syncLeaderTransferLog2(char* s, const SyncLeaderTransfer* pMsg);

// ---------------------------------------------
typedef struct SyncReconfigFinish {
  uint32_t  bytes;
  int32_t   vgId;
  uint32_t  msgType;
  SSyncCfg  oldCfg;
  SSyncCfg  newCfg;
  SyncIndex newCfgIndex;
  SyncTerm  newCfgTerm;
  uint64_t  newCfgSeqNum;

} SyncReconfigFinish;

SyncReconfigFinish* syncReconfigFinishBuild(int32_t vgId);
void                syncReconfigFinishDestroy(SyncReconfigFinish* pMsg);
void                syncReconfigFinishSerialize(const SyncReconfigFinish* pMsg, char* buf, uint32_t bufLen);
void                syncReconfigFinishDeserialize(const char* buf, uint32_t len, SyncReconfigFinish* pMsg);
char*               syncReconfigFinishSerialize2(const SyncReconfigFinish* pMsg, uint32_t* len);
SyncReconfigFinish* syncReconfigFinishDeserialize2(const char* buf, uint32_t len);
void                syncReconfigFinish2RpcMsg(const SyncReconfigFinish* pMsg, SRpcMsg* pRpcMsg);
void                syncReconfigFinishFromRpcMsg(const SRpcMsg* pRpcMsg, SyncReconfigFinish* pMsg);
SyncReconfigFinish* syncReconfigFinishFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*              syncReconfigFinish2Json(const SyncReconfigFinish* pMsg);
char*               syncReconfigFinish2Str(const SyncReconfigFinish* pMsg);

// for debug ----------------------
void syncReconfigFinishPrint(const SyncReconfigFinish* pMsg);
void syncReconfigFinishPrint2(char* s, const SyncReconfigFinish* pMsg);
void syncReconfigFinishLog(const SyncReconfigFinish* pMsg);
void syncReconfigFinishLog2(char* s, const SyncReconfigFinish* pMsg);

// on message ----------------------
int32_t syncNodeOnPingCb(SSyncNode* ths, SyncPing* pMsg);
int32_t syncNodeOnPingReplyCb(SSyncNode* ths, SyncPingReply* pMsg);
int32_t syncNodeOnTimeoutCb(SSyncNode* ths, SyncTimeout* pMsg);
int32_t syncNodeOnClientRequestCb(SSyncNode* ths, SyncClientRequest* pMsg, SyncIndex* pRetIndex);
int32_t syncNodeOnClientRequestBatchCb(SSyncNode* ths, SyncClientRequestBatch* pMsg);
int32_t syncNodeOnRequestVoteCb(SSyncNode* ths, SyncRequestVote* pMsg);
int32_t syncNodeOnRequestVoteReplyCb(SSyncNode* ths, SyncRequestVoteReply* pMsg);
int32_t syncNodeOnAppendEntriesCb(SSyncNode* ths, SyncAppendEntries* pMsg);
int32_t syncNodeOnAppendEntriesReplyCb(SSyncNode* ths, SyncAppendEntriesReply* pMsg);

int32_t syncNodeOnRequestVoteSnapshotCb(SSyncNode* ths, SyncRequestVote* pMsg);
int32_t syncNodeOnRequestVoteReplySnapshotCb(SSyncNode* ths, SyncRequestVoteReply* pMsg);
int32_t syncNodeOnAppendEntriesSnapshotCb(SSyncNode* ths, SyncAppendEntries* pMsg);
int32_t syncNodeOnAppendEntriesReplySnapshotCb(SSyncNode* ths, SyncAppendEntriesReply* pMsg);

int32_t syncNodeOnSnapshotSendCb(SSyncNode* ths, SyncSnapshotSend* pMsg);
int32_t syncNodeOnSnapshotRspCb(SSyncNode* ths, SyncSnapshotRsp* pMsg);

// -----------------------------------------
typedef int32_t (*FpOnPingCb)(SSyncNode* ths, SyncPing* pMsg);
typedef int32_t (*FpOnPingReplyCb)(SSyncNode* ths, SyncPingReply* pMsg);
typedef int32_t (*FpOnClientRequestCb)(SSyncNode* ths, SyncClientRequest* pMsg, SyncIndex* pRetIndex);
typedef int32_t (*FpOnRequestVoteCb)(SSyncNode* ths, SyncRequestVote* pMsg);
typedef int32_t (*FpOnRequestVoteReplyCb)(SSyncNode* ths, SyncRequestVoteReply* pMsg);
typedef int32_t (*FpOnAppendEntriesCb)(SSyncNode* ths, SyncAppendEntries* pMsg);
typedef int32_t (*FpOnAppendEntriesReplyCb)(SSyncNode* ths, SyncAppendEntriesReply* pMsg);
typedef int32_t (*FpOnTimeoutCb)(SSyncNode* pSyncNode, SyncTimeout* pMsg);
typedef int32_t (*FpOnSnapshotSendCb)(SSyncNode* ths, SyncSnapshotSend* pMsg);
typedef int32_t (*FpOnSnapshotRspCb)(SSyncNode* ths, SyncSnapshotRsp* pMsg);

// option ----------------------------------
bool syncNodeSnapshotEnable(SSyncNode* pSyncNode);

// ---------------------------------------------

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_TOOLS_H*/
