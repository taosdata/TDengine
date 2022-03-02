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

// encode as uint64
typedef enum ESyncMessageType {
  SYNC_PING = 101,
  SYNC_PING_REPLY = 103,
  SYNC_CLIENT_REQUEST,
  SYNC_CLIENT_REQUEST_REPLY,
  SYNC_REQUEST_VOTE,
  SYNC_REQUEST_VOTE_REPLY,
  SYNC_APPEND_ENTRIES,
  SYNC_APPEND_ENTRIES_REPLY,
} ESyncMessageType;

/*
typedef struct SRaftId {
  SyncNodeId  addr;  // typedef uint64_t SyncNodeId;
  SyncGroupId vgId;  // typedef int32_t  SyncGroupId;
} SRaftId;
*/

typedef struct SyncPing {
  uint32_t bytes;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;
  uint32_t dataLen;
  char     data[];
} SyncPing;

#define SYNC_PING_FIX_LEN (sizeof(uint32_t) + sizeof(uint32_t) + sizeof(SRaftId) + sizeof(SRaftId) + sizeof(uint32_t))

SyncPing* syncPingBuild(uint32_t dataLen);

void syncPingDestroy(SyncPing* pMsg);

void syncPingSerialize(const SyncPing* pMsg, char* buf, uint32_t bufLen);

void syncPingDeserialize(const char* buf, uint32_t len, SyncPing* pMsg);

void syncPing2RpcMsg(const SyncPing* pMsg, SRpcMsg* pRpcMsg);

void syncPingFromRpcMsg(const SRpcMsg* pRpcMsg, SyncPing* pMsg);

cJSON* syncPing2Json(const SyncPing* pMsg);

SyncPing* syncPingBuild2(const SRaftId* srcId, const SRaftId* destId, const char* str);

SyncPing* syncPingBuild3(const SRaftId* srcId, const SRaftId* destId);

typedef struct SyncPingReply {
  uint32_t bytes;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;
  uint32_t dataLen;
  char     data[];
} SyncPingReply;

#define SYNC_PING_REPLY_FIX_LEN \
  (sizeof(uint32_t) + sizeof(uint32_t) + sizeof(SRaftId) + sizeof(SRaftId) + sizeof(uint32_t))

SyncPingReply* syncPingReplyBuild(uint32_t dataLen);

void syncPingReplyDestroy(SyncPingReply* pMsg);

void syncPingReplySerialize(const SyncPingReply* pMsg, char* buf, uint32_t bufLen);

void syncPingReplyDeserialize(const char* buf, uint32_t len, SyncPingReply* pMsg);

void syncPingReply2RpcMsg(const SyncPingReply* pMsg, SRpcMsg* pRpcMsg);

void syncPingReplyFromRpcMsg(const SRpcMsg* pRpcMsg, SyncPingReply* pMsg);

cJSON* syncPingReply2Json(const SyncPingReply* pMsg);

SyncPingReply* syncPingReplyBuild2(const SRaftId* srcId, const SRaftId* destId, const char* str);

SyncPingReply* syncPingReplyBuild3(const SRaftId* srcId, const SRaftId* destId);

typedef struct SyncClientRequest {
  ESyncMessageType msgType;
  char*            data;
  uint32_t         dataLen;
  int64_t          seqNum;
  bool             isWeak;
} SyncClientRequest;

typedef struct SyncClientRequestReply {
  ESyncMessageType msgType;
  int32_t          errCode;
  SSyncBuffer*     pErrMsg;
  SSyncBuffer*     pLeaderHint;
} SyncClientRequestReply;

typedef struct SyncRequestVote {
  ESyncMessageType msgType;
  SyncTerm         currentTerm;
  SyncNodeId       nodeId;
  SyncGroupId      vgId;
  SyncIndex        lastLogIndex;
  SyncTerm         lastLogTerm;
} SyncRequestVote;

typedef struct SyncRequestVoteReply {
  ESyncMessageType msgType;
  SyncTerm         currentTerm;
  SyncNodeId       nodeId;
  SyncGroupId      vgId;
  bool             voteGranted;
} SyncRequestVoteReply;

typedef struct SyncAppendEntries {
  ESyncMessageType msgType;
  SyncTerm         currentTerm;
  SyncNodeId       nodeId;
  SyncIndex        prevLogIndex;
  SyncTerm         prevLogTerm;
  int32_t          entryCount;
  SSyncRaftEntry*  logEntries;
  SyncIndex        commitIndex;
} SyncAppendEntries;

typedef struct SyncAppendEntriesReply {
  ESyncMessageType msgType;
  SyncTerm         currentTerm;
  SyncNodeId       nodeId;
  bool             success;
  SyncIndex        matchIndex;
} SyncAppendEntriesReply;

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_MESSAGE_H*/
