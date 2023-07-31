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

#ifndef _TD_LIBS_SYNC_TEST_H
#define _TD_LIBS_SYNC_TEST_H

#ifdef __cplusplus
extern "C" {
#endif

#include "syncInt.h"

#include "tref.h"
#include "wal.h"

#include "syncEnv.h"
#include "syncIO.h"
#include "syncIndexMgr.h"
#include "syncInt.h"
#include "syncMessage.h"
#include "syncRaftCfg.h"
#include "syncRaftEntry.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncRespMgr.h"
#include "syncSnapshot.h"
#include "syncUtil.h"
#include "syncVoteMgr.h"
#include "tref.h"

extern void addEpIntoEpSet(SEpSet* pEpSet, const char* fqdn, uint16_t port);

typedef struct SyncPing      SyncPing;
typedef struct SyncPingReply SyncPingReply;

typedef int32_t (*FpOnPingCb)(SSyncNode* ths, SyncPing* pMsg);
typedef int32_t (*FpOnPingReplyCb)(SSyncNode* ths, SyncPingReply* pMsg);
typedef int32_t (*FpOnClientRequestCb)(SSyncNode* ths, SRpcMsg* pMsg, SyncIndex* pRetIndex);
typedef int32_t (*FpOnRequestVoteCb)(SSyncNode* ths, SyncRequestVote* pMsg);
typedef int32_t (*FpOnRequestVoteReplyCb)(SSyncNode* ths, SyncRequestVoteReply* pMsg);
typedef int32_t (*FpOnAppendEntriesCb)(SSyncNode* ths, SyncAppendEntries* pMsg);
typedef int32_t (*FpOnAppendEntriesReplyCb)(SSyncNode* ths, SyncAppendEntriesReply* pMsg);
typedef int32_t (*FpOnTimeoutCb)(SSyncNode* pSyncNode, SyncTimeout* pMsg);
typedef int32_t (*FpOnSnapshotCb)(SSyncNode* ths, SyncSnapshotSend* pMsg);
typedef int32_t (*FpOnSnapshotReplyCb)(SSyncNode* ths, SyncSnapshotRsp* pMsg);

cJSON* syncEntry2Json(const SSyncRaftEntry* pEntry);
char*  syncEntry2Str(const SSyncRaftEntry* pEntry);
void   syncEntryPrint(const SSyncRaftEntry* pObj);
void   syncEntryPrint2(char* s, const SSyncRaftEntry* pObj);
void   syncEntryLog(const SSyncRaftEntry* pObj);
void   syncEntryLog2(char* s, const SSyncRaftEntry* pObj);

char*   syncCfg2Str(SSyncCfg* pSyncCfg);
int32_t syncCfgFromStr(const char* s, SSyncCfg* pSyncCfg);

cJSON* raftCache2Json(SRaftEntryHashCache* pObj);
char*  raftCache2Str(SRaftEntryHashCache* pObj);
void   raftCachePrint(SRaftEntryHashCache* pObj);
void   raftCachePrint2(char* s, SRaftEntryHashCache* pObj);
void   raftCacheLog(SRaftEntryHashCache* pObj);
void   raftCacheLog2(char* s, SRaftEntryHashCache* pObj);

cJSON* raftEntryCache2Json(SRaftEntryCache* pObj);
char*  raftEntryCache2Str(SRaftEntryCache* pObj);
void   raftEntryCachePrint(SRaftEntryCache* pObj);
void   raftEntryCachePrint2(char* s, SRaftEntryCache* pObj);
void   raftEntryCacheLog(SRaftEntryCache* pObj);
void   raftEntryCacheLog2(char* s, SRaftEntryCache* pObj);

int32_t raftStoreFromJson(SRaftStore* pRaftStore, cJSON* pJson);
cJSON*  raftStore2Json(SRaftStore* pRaftStore);
char*   raftStore2Str(SRaftStore* pRaftStore);
void    raftStorePrint(SRaftStore* pObj);
void    raftStorePrint2(char* s, SRaftStore* pObj);
void    raftStoreLog(SRaftStore* pObj);
void    raftStoreLog2(char* s, SRaftStore* pObj);

cJSON* syncAppendEntriesBatch2Json(const SyncAppendEntriesBatch* pMsg);
char*  syncAppendEntriesBatch2Str(const SyncAppendEntriesBatch* pMsg);
void   syncAppendEntriesBatchPrint(const SyncAppendEntriesBatch* pMsg);
void   syncAppendEntriesBatchPrint2(char* s, const SyncAppendEntriesBatch* pMsg);
void   syncAppendEntriesBatchLog(const SyncAppendEntriesBatch* pMsg);
void   syncAppendEntriesBatchLog2(char* s, const SyncAppendEntriesBatch* pMsg);

cJSON* logStore2Json(SSyncLogStore* pLogStore);
char*  logStore2Str(SSyncLogStore* pLogStore);
cJSON* logStoreSimple2Json(SSyncLogStore* pLogStore);
char*  logStoreSimple2Str(SSyncLogStore* pLogStore);
void   logStorePrint(SSyncLogStore* pLogStore);
void   logStorePrint2(char* s, SSyncLogStore* pLogStore);
void   logStoreLog(SSyncLogStore* pLogStore);
void   logStoreLog2(char* s, SSyncLogStore* pLogStore);
void   logStoreSimplePrint(SSyncLogStore* pLogStore);
void   logStoreSimplePrint2(char* s, SSyncLogStore* pLogStore);
void   logStoreSimpleLog(SSyncLogStore* pLogStore);
void   logStoreSimpleLog2(char* s, SSyncLogStore* pLogStore);

cJSON* syncNode2Json(const SSyncNode* pSyncNode);
char*  syncNode2Str(const SSyncNode* pSyncNode);
char*  syncNode2SimpleStr(const SSyncNode* pSyncNode);

cJSON* voteGranted2Json(SVotesGranted* pVotesGranted);
char*  voteGranted2Str(SVotesGranted* pVotesGranted);
cJSON* votesRespond2Json(SVotesRespond* pVotesRespond);
char*  votesRespond2Str(SVotesRespond* pVotesRespond);

cJSON* syncUtilNodeInfo2Json(const SNodeInfo* p);
char*  syncUtilRaftId2Str(const SRaftId* p);

cJSON* snapshotSender2Json(SSyncSnapshotSender* pSender);
char*  snapshotSender2Str(SSyncSnapshotSender* pSender);
cJSON* snapshotReceiver2Json(SSyncSnapshotReceiver* pReceiver);
char*  snapshotReceiver2Str(SSyncSnapshotReceiver* pReceiver);

cJSON* syncIndexMgr2Json(SSyncIndexMgr* pSyncIndexMgr);
char*  syncIndexMgr2Str(SSyncIndexMgr* pSyncIndexMgr);
void   syncIndexMgrPrint(SSyncIndexMgr* pObj);
void   syncIndexMgrPrint2(char* s, SSyncIndexMgr* pObj);
void   syncIndexMgrLog(SSyncIndexMgr* pObj);
void   syncIndexMgrLog2(char* s, SSyncIndexMgr* pObj);

cJSON* syncRpcMsg2Json(SRpcMsg* pRpcMsg);
cJSON* syncRpcUnknownMsg2Json();
char*  syncRpcMsg2Str(SRpcMsg* pRpcMsg);
void   syncRpcMsgPrint(SRpcMsg* pMsg);
void   syncRpcMsgPrint2(char* s, SRpcMsg* pMsg);
void   syncRpcMsgLog(SRpcMsg* pMsg);
void   syncRpcMsgLog2(char* s, SRpcMsg* pMsg);

// origin syncMessage
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
char*     syncPingSerialize2(const SyncPing* pMsg, uint32_t* len);
int32_t   syncPingSerialize3(const SyncPing* pMsg, char* buf, int32_t bufLen);
SyncPing* syncPingDeserialize3(void* buf, int32_t bufLen);
void      syncPing2RpcMsg(const SyncPing* pMsg, SRpcMsg* pRpcMsg);
void      syncPingFromRpcMsg(const SRpcMsg* pRpcMsg, SyncPing* pMsg);
cJSON*    syncPing2Json(const SyncPing* pMsg);
char*     syncPing2Str(const SyncPing* pMsg);
void      syncPingPrint(const SyncPing* pMsg);
void      syncPingPrint2(char* s, const SyncPing* pMsg);
void      syncPingLog(const SyncPing* pMsg);
void      syncPingLog2(char* s, const SyncPing* pMsg);
void      syncPingDestroy(SyncPing* pMsg);
void      syncPingSerialize(const SyncPing* pMsg, char* buf, uint32_t bufLen);
void      syncPingDeserialize(const char* buf, uint32_t len, SyncPing* pMsg);
SyncPing* syncPingDeserialize2(const char* buf, uint32_t len);
SyncPing* syncPingFromRpcMsg2(const SRpcMsg* pRpcMsg);

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

int32_t syncNodeOnPing(SSyncNode* ths, SyncPing* pMsg);
int32_t syncNodeOnPingReply(SSyncNode* ths, SyncPingReply* pMsg);
int32_t syncNodePing(SSyncNode* pSyncNode, const SRaftId* destRaftId, SyncPing* pMsg);
int32_t syncNodePingSelf(SSyncNode* pSyncNode);
int32_t syncNodePingPeers(SSyncNode* pSyncNode);
int32_t syncNodePingAll(SSyncNode* pSyncNode);

SyncTimeout* syncTimeoutBuildX();
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
void         syncTimeoutPrint(const SyncTimeout* pMsg);
void         syncTimeoutPrint2(char* s, const SyncTimeout* pMsg);
void         syncTimeoutLog(const SyncTimeout* pMsg);
void         syncTimeoutLog2(char* s, const SyncTimeout* pMsg);

SyncClientRequest* syncClientRequestAlloc(uint32_t dataLen);
void               syncClientRequest2RpcMsg(const SyncClientRequest* pMsg, SRpcMsg* pRpcMsg);  // step 2
void               syncClientRequestFromRpcMsg(const SRpcMsg* pRpcMsg, SyncClientRequest* pMsg);
cJSON*             syncClientRequest2Json(const SyncClientRequest* pMsg);
char*              syncClientRequest2Str(const SyncClientRequest* pMsg);
void               syncClientRequestPrint(const SyncClientRequest* pMsg);
void               syncClientRequestPrint2(char* s, const SyncClientRequest* pMsg);
void               syncClientRequestLog(const SyncClientRequest* pMsg);
void               syncClientRequestLog2(char* s, const SyncClientRequest* pMsg);

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

SyncHeartbeat* syncHeartbeatBuild(int32_t vgId);
void           syncHeartbeatDestroy(SyncHeartbeat* pMsg);
void           syncHeartbeatSerialize(const SyncHeartbeat* pMsg, char* buf, uint32_t bufLen);
void           syncHeartbeatDeserialize(const char* buf, uint32_t len, SyncHeartbeat* pMsg);
char*          syncHeartbeatSerialize2(const SyncHeartbeat* pMsg, uint32_t* len);
SyncHeartbeat* syncHeartbeatDeserialize2(const char* buf, uint32_t len);
void           syncHeartbeat2RpcMsg(const SyncHeartbeat* pMsg, SRpcMsg* pRpcMsg);
void           syncHeartbeatFromRpcMsg(const SRpcMsg* pRpcMsg, SyncHeartbeat* pMsg);
SyncHeartbeat* syncHeartbeatFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*         syncHeartbeat2Json(const SyncHeartbeat* pMsg);
char*          syncHeartbeat2Str(const SyncHeartbeat* pMsg);

// for debug ----------------------
void syncHeartbeatPrint(const SyncHeartbeat* pMsg);
void syncHeartbeatPrint2(char* s, const SyncHeartbeat* pMsg);
void syncHeartbeatLog(const SyncHeartbeat* pMsg);
void syncHeartbeatLog2(char* s, const SyncHeartbeat* pMsg);

SyncHeartbeatReply* syncHeartbeatReplyBuild(int32_t vgId);
void                syncHeartbeatReplyDestroy(SyncHeartbeatReply* pMsg);
void                syncHeartbeatReplySerialize(const SyncHeartbeatReply* pMsg, char* buf, uint32_t bufLen);
void                syncHeartbeatReplyDeserialize(const char* buf, uint32_t len, SyncHeartbeatReply* pMsg);
char*               syncHeartbeatReplySerialize2(const SyncHeartbeatReply* pMsg, uint32_t* len);
SyncHeartbeatReply* syncHeartbeatReplyDeserialize2(const char* buf, uint32_t len);
void                syncHeartbeatReply2RpcMsg(const SyncHeartbeatReply* pMsg, SRpcMsg* pRpcMsg);
void                syncHeartbeatReplyFromRpcMsg(const SRpcMsg* pRpcMsg, SyncHeartbeatReply* pMsg);
SyncHeartbeatReply* syncHeartbeatReplyFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*              syncHeartbeatReply2Json(const SyncHeartbeatReply* pMsg);
char*               syncHeartbeatReply2Str(const SyncHeartbeatReply* pMsg);

// for debug ----------------------
void syncHeartbeatReplyPrint(const SyncHeartbeatReply* pMsg);
void syncHeartbeatReplyPrint2(char* s, const SyncHeartbeatReply* pMsg);
void syncHeartbeatReplyLog(const SyncHeartbeatReply* pMsg);
void syncHeartbeatReplyLog2(char* s, const SyncHeartbeatReply* pMsg);

SyncPreSnapshot* syncPreSnapshotBuild(int32_t vgId);
void             syncPreSnapshotDestroy(SyncPreSnapshot* pMsg);
void             syncPreSnapshotSerialize(const SyncPreSnapshot* pMsg, char* buf, uint32_t bufLen);
void             syncPreSnapshotDeserialize(const char* buf, uint32_t len, SyncPreSnapshot* pMsg);
char*            syncPreSnapshotSerialize2(const SyncPreSnapshot* pMsg, uint32_t* len);
SyncPreSnapshot* syncPreSnapshotDeserialize2(const char* buf, uint32_t len);
void             syncPreSnapshot2RpcMsg(const SyncPreSnapshot* pMsg, SRpcMsg* pRpcMsg);
void             syncPreSnapshotFromRpcMsg(const SRpcMsg* pRpcMsg, SyncPreSnapshot* pMsg);
SyncPreSnapshot* syncPreSnapshotFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*           syncPreSnapshot2Json(const SyncPreSnapshot* pMsg);
char*            syncPreSnapshot2Str(const SyncPreSnapshot* pMsg);

// for debug ----------------------
void syncPreSnapshotPrint(const SyncPreSnapshot* pMsg);
void syncPreSnapshotPrint2(char* s, const SyncPreSnapshot* pMsg);
void syncPreSnapshotLog(const SyncPreSnapshot* pMsg);
void syncPreSnapshotLog2(char* s, const SyncPreSnapshot* pMsg);

SyncPreSnapshotReply* syncPreSnapshotReplyBuild(int32_t vgId);
void                  syncPreSnapshotReplyDestroy(SyncPreSnapshotReply* pMsg);
void                  syncPreSnapshotReplySerialize(const SyncPreSnapshotReply* pMsg, char* buf, uint32_t bufLen);
void                  syncPreSnapshotReplyDeserialize(const char* buf, uint32_t len, SyncPreSnapshotReply* pMsg);
char*                 syncPreSnapshotReplySerialize2(const SyncPreSnapshotReply* pMsg, uint32_t* len);
SyncPreSnapshotReply* syncPreSnapshotReplyDeserialize2(const char* buf, uint32_t len);
void                  syncPreSnapshotReply2RpcMsg(const SyncPreSnapshotReply* pMsg, SRpcMsg* pRpcMsg);
void                  syncPreSnapshotReplyFromRpcMsg(const SRpcMsg* pRpcMsg, SyncPreSnapshotReply* pMsg);
SyncPreSnapshotReply* syncPreSnapshotReplyFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*                syncPreSnapshotReply2Json(const SyncPreSnapshotReply* pMsg);
char*                 syncPreSnapshotReply2Str(const SyncPreSnapshotReply* pMsg);

// for debug ----------------------
void syncPreSnapshotReplyPrint(const SyncPreSnapshotReply* pMsg);
void syncPreSnapshotReplyPrint2(char* s, const SyncPreSnapshotReply* pMsg);
void syncPreSnapshotReplyLog(const SyncPreSnapshotReply* pMsg);
void syncPreSnapshotReplyLog2(char* s, const SyncPreSnapshotReply* pMsg);

// ---------------------------------------------
int32_t syncNodeOnPreSnapshot(SSyncNode* ths, SyncPreSnapshot* pMsg);
int32_t syncNodeOnPreSnapshotReply(SSyncNode* ths, SyncPreSnapshotReply* pMsg);

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

SyncLocalCmd* syncLocalCmdBuild(int32_t vgId);
void          syncLocalCmdDestroy(SyncLocalCmd* pMsg);
void          syncLocalCmdSerialize(const SyncLocalCmd* pMsg, char* buf, uint32_t bufLen);
void          syncLocalCmdDeserialize(const char* buf, uint32_t len, SyncLocalCmd* pMsg);
char*         syncLocalCmdSerialize2(const SyncLocalCmd* pMsg, uint32_t* len);
SyncLocalCmd* syncLocalCmdDeserialize2(const char* buf, uint32_t len);
void          syncLocalCmd2RpcMsg(const SyncLocalCmd* pMsg, SRpcMsg* pRpcMsg);
void          syncLocalCmdFromRpcMsg(const SRpcMsg* pRpcMsg, SyncLocalCmd* pMsg);
SyncLocalCmd* syncLocalCmdFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*        syncLocalCmd2Json(const SyncLocalCmd* pMsg);
char*         syncLocalCmd2Str(const SyncLocalCmd* pMsg);

// for debug ----------------------
void syncLocalCmdPrint(const SyncLocalCmd* pMsg);
void syncLocalCmdPrint2(char* s, const SyncLocalCmd* pMsg);
void syncLocalCmdLog(const SyncLocalCmd* pMsg);
void syncLocalCmdLog2(char* s, const SyncLocalCmd* pMsg);

char*    syncUtilPrintBin(char* ptr, uint32_t len);
char*    syncUtilPrintBin2(char* ptr, uint32_t len);
void     syncUtilU642Addr(uint64_t u64, char* host, int64_t len, uint16_t* port);
uint64_t syncUtilAddr2U64(const char* host, uint16_t port);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_TEST_H*/
