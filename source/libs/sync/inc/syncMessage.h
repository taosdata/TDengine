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

#ifndef _TD_LIBS_SYNC_MESSAGE_H
#define _TD_LIBS_SYNC_MESSAGE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "cJSON.h"
#include "sync.h"
#include "syncRaftEntry.h"
#include "taosdef.h"

// encode as uint32
typedef enum ESyncMessageType {
  SYNC_UNKNOWN = 9999,
  SYNC_TIMEOUT = 99,
  SYNC_PING = 101,
  SYNC_PING_REPLY = 103,
  SYNC_CLIENT_REQUEST = 105,
  SYNC_CLIENT_REQUEST_REPLY = 107,
  SYNC_REQUEST_VOTE = 109,
  SYNC_REQUEST_VOTE_REPLY = 111,
  SYNC_APPEND_ENTRIES = 113,
  SYNC_APPEND_ENTRIES_REPLY = 115,

} ESyncMessageType;

// ---------------------------------------------
cJSON* syncRpcMsg2Json(SRpcMsg* pRpcMsg);
cJSON* syncRpcUnknownMsg2Json();
char*  syncRpcMsg2Str(SRpcMsg* pRpcMsg);

// ---------------------------------------------
typedef enum ESyncTimeoutType {
  SYNC_TIMEOUT_PING = 100,
  SYNC_TIMEOUT_ELECTION,
  SYNC_TIMEOUT_HEARTBEAT,
} ESyncTimeoutType;

typedef struct SyncTimeout {
  uint32_t         bytes;
  uint32_t         msgType;
  ESyncTimeoutType timeoutType;
  uint64_t         logicClock;
  int32_t          timerMS;
  void*            data;
} SyncTimeout;

SyncTimeout* syncTimeoutBuild();
void         syncTimeoutDestroy(SyncTimeout* pMsg);
void         syncTimeoutSerialize(const SyncTimeout* pMsg, char* buf, uint32_t bufLen);
void         syncTimeoutDeserialize(const char* buf, uint32_t len, SyncTimeout* pMsg);
void         syncTimeout2RpcMsg(const SyncTimeout* pMsg, SRpcMsg* pRpcMsg);
void         syncTimeoutFromRpcMsg(const SRpcMsg* pRpcMsg, SyncTimeout* pMsg);
cJSON*       syncTimeout2Json(const SyncTimeout* pMsg);
SyncTimeout* syncTimeoutBuild2(ESyncTimeoutType timeoutType, uint64_t logicClock, int32_t timerMS, void* data);

// ---------------------------------------------
typedef struct SyncPing {
  uint32_t bytes;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;
  // private data
  uint32_t dataLen;
  char     data[];
} SyncPing;

#define SYNC_PING_FIX_LEN (sizeof(uint32_t) + sizeof(uint32_t) + sizeof(SRaftId) + sizeof(SRaftId) + sizeof(uint32_t))

SyncPing* syncPingBuild(uint32_t dataLen);
void      syncPingDestroy(SyncPing* pMsg);
void      syncPingSerialize(const SyncPing* pMsg, char* buf, uint32_t bufLen);
void      syncPingDeserialize(const char* buf, uint32_t len, SyncPing* pMsg);
void      syncPing2RpcMsg(const SyncPing* pMsg, SRpcMsg* pRpcMsg);
void      syncPingFromRpcMsg(const SRpcMsg* pRpcMsg, SyncPing* pMsg);
cJSON*    syncPing2Json(const SyncPing* pMsg);
SyncPing* syncPingBuild2(const SRaftId* srcId, const SRaftId* destId, const char* str);
SyncPing* syncPingBuild3(const SRaftId* srcId, const SRaftId* destId);

// ---------------------------------------------
typedef struct SyncPingReply {
  uint32_t bytes;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;
  // private data
  uint32_t dataLen;
  char     data[];
} SyncPingReply;

#define SYNC_PING_REPLY_FIX_LEN \
  (sizeof(uint32_t) + sizeof(uint32_t) + sizeof(SRaftId) + sizeof(SRaftId) + sizeof(uint32_t))

SyncPingReply* syncPingReplyBuild(uint32_t dataLen);
void           syncPingReplyDestroy(SyncPingReply* pMsg);
void           syncPingReplySerialize(const SyncPingReply* pMsg, char* buf, uint32_t bufLen);
void           syncPingReplyDeserialize(const char* buf, uint32_t len, SyncPingReply* pMsg);
void           syncPingReply2RpcMsg(const SyncPingReply* pMsg, SRpcMsg* pRpcMsg);
void           syncPingReplyFromRpcMsg(const SRpcMsg* pRpcMsg, SyncPingReply* pMsg);
cJSON*         syncPingReply2Json(const SyncPingReply* pMsg);
SyncPingReply* syncPingReplyBuild2(const SRaftId* srcId, const SRaftId* destId, const char* str);
SyncPingReply* syncPingReplyBuild3(const SRaftId* srcId, const SRaftId* destId);

// ---------------------------------------------
typedef struct SyncClientRequest {
  uint32_t bytes;
  uint32_t msgType;
  uint32_t originalRpcType;
  uint64_t seqNum;
  bool     isWeak;
  uint32_t dataLen;
  char     data[];
} SyncClientRequest;

#define SYNC_CLIENT_REQUEST_FIX_LEN \
  (sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(bool) + sizeof(uint32_t))

SyncClientRequest* syncClientRequestBuild(uint32_t dataLen);
void               syncClientRequestDestroy(SyncClientRequest* pMsg);
void               syncClientRequestSerialize(const SyncClientRequest* pMsg, char* buf, uint32_t bufLen);
void               syncClientRequestDeserialize(const char* buf, uint32_t len, SyncClientRequest* pMsg);
void               syncClientRequest2RpcMsg(const SyncClientRequest* pMsg, SRpcMsg* pRpcMsg);
void               syncClientRequestFromRpcMsg(const SRpcMsg* pRpcMsg, SyncClientRequest* pMsg);
cJSON*             syncClientRequest2Json(const SyncClientRequest* pMsg);
SyncClientRequest* syncClientRequestBuild2(const SRpcMsg* pOriginalRpcMsg, uint64_t seqNum, bool isWeak);

// ---------------------------------------------
typedef struct SyncClientRequestReply {
  uint32_t bytes;
  uint32_t msgType;
  int32_t  errCode;
  SRaftId  leaderHint;
} SyncClientRequestReply;

// ---------------------------------------------
typedef struct SyncRequestVote {
  uint32_t bytes;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;
  // private data
  SyncTerm  currentTerm;
  SyncIndex lastLogIndex;
  SyncTerm  lastLogTerm;
} SyncRequestVote;

SyncRequestVote* syncRequestVoteBuild();
void             syncRequestVoteDestroy(SyncRequestVote* pMsg);
void             syncRequestVoteSerialize(const SyncRequestVote* pMsg, char* buf, uint32_t bufLen);
void             syncRequestVoteDeserialize(const char* buf, uint32_t len, SyncRequestVote* pMsg);
void             syncRequestVote2RpcMsg(const SyncRequestVote* pMsg, SRpcMsg* pRpcMsg);
void             syncRequestVoteFromRpcMsg(const SRpcMsg* pRpcMsg, SyncRequestVote* pMsg);
cJSON*           syncRequestVote2Json(const SyncRequestVote* pMsg);

// ---------------------------------------------
typedef struct SyncRequestVoteReply {
  uint32_t bytes;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;
  // private data
  SyncTerm term;
  bool     voteGranted;
} SyncRequestVoteReply;

SyncRequestVoteReply* SyncRequestVoteReplyBuild();
void                  syncRequestVoteReplyDestroy(SyncRequestVoteReply* pMsg);
void                  syncRequestVoteReplySerialize(const SyncRequestVoteReply* pMsg, char* buf, uint32_t bufLen);
void                  syncRequestVoteReplyDeserialize(const char* buf, uint32_t len, SyncRequestVoteReply* pMsg);
void                  syncRequestVoteReply2RpcMsg(const SyncRequestVoteReply* pMsg, SRpcMsg* pRpcMsg);
void                  syncRequestVoteReplyFromRpcMsg(const SRpcMsg* pRpcMsg, SyncRequestVoteReply* pMsg);
cJSON*                syncRequestVoteReply2Json(const SyncRequestVoteReply* pMsg);

// ---------------------------------------------
typedef struct SyncAppendEntries {
  uint32_t bytes;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;
  // private data
  SyncIndex prevLogIndex;
  SyncTerm  prevLogTerm;
  SyncIndex commitIndex;
  uint32_t  dataLen;
  char      data[];
} SyncAppendEntries;

#define SYNC_APPEND_ENTRIES_FIX_LEN                                                                                 \
  (sizeof(uint32_t) + sizeof(uint32_t) + sizeof(SRaftId) + sizeof(SRaftId) + sizeof(SyncIndex) + sizeof(SyncTerm) + \
   sizeof(SyncIndex) + sizeof(uint32_t))

SyncAppendEntries* syncAppendEntriesBuild(uint32_t dataLen);
void               syncAppendEntriesDestroy(SyncAppendEntries* pMsg);
void               syncAppendEntriesSerialize(const SyncAppendEntries* pMsg, char* buf, uint32_t bufLen);
void               syncAppendEntriesDeserialize(const char* buf, uint32_t len, SyncAppendEntries* pMsg);
void               syncAppendEntries2RpcMsg(const SyncAppendEntries* pMsg, SRpcMsg* pRpcMsg);
void               syncAppendEntriesFromRpcMsg(const SRpcMsg* pRpcMsg, SyncAppendEntries* pMsg);
cJSON*             syncAppendEntries2Json(const SyncAppendEntries* pMsg);

// ---------------------------------------------
typedef struct SyncAppendEntriesReply {
  uint32_t bytes;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;
  // private data
  bool      success;
  SyncIndex matchIndex;
} SyncAppendEntriesReply;

SyncAppendEntriesReply* syncAppendEntriesReplyBuild();
void                    syncAppendEntriesReplyDestroy(SyncAppendEntriesReply* pMsg);
void                    syncAppendEntriesReplySerialize(const SyncAppendEntriesReply* pMsg, char* buf, uint32_t bufLen);
void                    syncAppendEntriesReplyDeserialize(const char* buf, uint32_t len, SyncAppendEntriesReply* pMsg);
void                    syncAppendEntriesReply2RpcMsg(const SyncAppendEntriesReply* pMsg, SRpcMsg* pRpcMsg);
void                    syncAppendEntriesReplyFromRpcMsg(const SRpcMsg* pRpcMsg, SyncAppendEntriesReply* pMsg);
cJSON*                  syncAppendEntriesReply2Json(const SyncAppendEntriesReply* pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_MESSAGE_H*/
