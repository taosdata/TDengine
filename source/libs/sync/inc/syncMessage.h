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
#include "sync.h"
#include "syncRaftEntry.h"
#include "taosdef.h"

// encode as uint64
typedef enum ESyncMessageType {
  SYNC_PING = 0,
  SYNC_PING_REPLY,
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
  char*    data;
} SyncPing;

#define SYNC_PING_FIX_LEN (sizeof(uint32_t) + sizeof(uint32_t) + sizeof(SRaftId) + sizeof(SRaftId) + sizeof(uint32_t))

typedef struct SyncPingReply {
  uint32_t bytes;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;
  uint32_t dataLen;
  char*    data;
} SyncPingReply;

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

// ---- message build ----
SyncPing* syncPingBuild(uint32_t dataLen);

void syncPingDestroy(SyncPing* pSyncPing);

void syncPingSerialize(const SyncPing* pSyncPing, char* buf, uint32_t bufLen);

void syncPingDeserialize(const char* buf, uint32_t len, SyncPing* pSyncPing);

void syncPing2RpcMsg(const SyncPing* pSyncPing, SRpcMsg* pRpcMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_MESSAGE_H*/
