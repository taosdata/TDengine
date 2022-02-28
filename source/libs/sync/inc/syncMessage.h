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

typedef struct SyncPing {
  ESyncMessageType   msgType;
  const SSyncBuffer *pData;
} SyncPing, RaftPing;

typedef struct SyncPingReply {
  ESyncMessageType   msgType;
  const SSyncBuffer *pData;
} SyncPingReply, RaftPingReply;

typedef struct SyncClientRequest {
  ESyncMessageType   msgType;
  const SSyncBuffer *pData;
  int64_t            seqNum;
  bool               isWeak;
} SyncClientRequest, RaftClientRequest;

typedef struct SyncClientRequestReply {
  ESyncMessageType   msgType;
  int32_t            errCode;
  const SSyncBuffer *pErrMsg;
  const SSyncBuffer *pLeaderHint;
} SyncClientRequestReply, RaftClientRequestReply;

typedef struct SyncRequestVote {
  ESyncMessageType msgType;
  SyncTerm         currentTerm;
  SyncNodeId       nodeId;
  SyncGroupId      vgId;
  SyncIndex        lastLogIndex;
  SyncTerm         lastLogTerm;
} SyncRequestVote, RaftRequestVote;

typedef struct SyncRequestVoteReply {
  ESyncMessageType msgType;
  SyncTerm         currentTerm;
  SyncNodeId       nodeId;
  SyncGroupId      vgId;
  bool             voteGranted;
} SyncRequestVoteReply, RaftRequestVoteReply;

typedef struct SyncAppendEntries {
  ESyncMessageType msgType;
  SyncTerm         currentTerm;
  SyncNodeId       nodeId;
  SyncIndex        prevLogIndex;
  SyncTerm         prevLogTerm;
  int32_t          entryCount;
  SSyncRaftEntry * logEntries;
  SyncIndex        commitIndex;
} SyncAppendEntries, RaftAppendEntries;

typedef struct SyncAppendEntriesReply {
  ESyncMessageType msgType;
  SyncTerm         currentTerm;
  SyncNodeId       nodeId;
  bool             success;
  SyncIndex        matchIndex;
} SyncAppendEntriesReply, RaftAppendEntriesReply;

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_MESSAGE_H*/
