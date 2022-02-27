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

#ifndef _TD_LIBS_SYNC_INT_H
#define _TD_LIBS_SYNC_INT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "sync.h"
#include "taosdef.h"
#include "tlog.h"

extern int32_t sDebugFlag;

#define sFatal(...)                                        \
  {                                                        \
    if (sDebugFlag & DEBUG_FATAL) {                        \
      taosPrintLog("SYN FATAL ", sDebugFlag, __VA_ARGS__); \
    }                                                      \
  }
#define sError(...)                                        \
  {                                                        \
    if (sDebugFlag & DEBUG_ERROR) {                        \
      taosPrintLog("SYN ERROR ", sDebugFlag, __VA_ARGS__); \
    }                                                      \
  }
#define sWarn(...)                                        \
  {                                                       \
    if (sDebugFlag & DEBUG_WARN) {                        \
      taosPrintLog("SYN WARN ", sDebugFlag, __VA_ARGS__); \
    }                                                     \
  }
#define sInfo(...)                                   \
  {                                                  \
    if (sDebugFlag & DEBUG_INFO) {                   \
      taosPrintLog("SYN ", sDebugFlag, __VA_ARGS__); \
    }                                                \
  }
#define sDebug(...)                                  \
  {                                                  \
    if (sDebugFlag & DEBUG_DEBUG) {                  \
      taosPrintLog("SYN ", sDebugFlag, __VA_ARGS__); \
    }                                                \
  }
#define sTrace(...)                                  \
  {                                                  \
    if (sDebugFlag & DEBUG_TRACE) {                  \
      taosPrintLog("SYN ", sDebugFlag, __VA_ARGS__); \
    }                                                \
  }

struct SRaft;
typedef struct SRaft SRaft;

struct SyncPing;
typedef struct SyncPing SyncPing;

struct SyncPingReply;
typedef struct SyncPingReply SyncPingReply;

struct SyncRequestVote;
typedef struct SyncRequestVote SyncRequestVote;

struct SyncRequestVoteReply;
typedef struct SyncRequestVoteReply SyncRequestVoteReply;

struct SyncAppendEntries;
typedef struct SyncAppendEntries SyncAppendEntries;

struct SyncAppendEntriesReply;
typedef struct SyncAppendEntriesReply SyncAppendEntriesReply;

typedef struct SSyncNode {
  int8_t replica;
  int8_t quorum;

  SyncGroupId vgId;
  SSyncCfg    syncCfg;
  char        path[TSDB_FILENAME_LEN];

  SRaft* pRaft;

  int32_t (*FpPing)(struct SSyncNode* ths, const SyncPing* pMsg);

  int32_t (*FpOnPing)(struct SSyncNode* ths, SyncPing* pMsg);

  int32_t (*FpOnPingReply)(struct SSyncNode* ths, SyncPingReply* pMsg);

  int32_t (*FpRequestVote)(struct SSyncNode* ths, const SyncRequestVote* pMsg);

  int32_t (*FpOnRequestVote)(struct SSyncNode* ths, SyncRequestVote* pMsg);

  int32_t (*FpOnRequestVoteReply)(struct SSyncNode* ths, SyncRequestVoteReply* pMsg);

  int32_t (*FpAppendEntries)(struct SSyncNode* ths, const SyncAppendEntries* pMsg);

  int32_t (*FpOnAppendEntries)(struct SSyncNode* ths, SyncAppendEntries* pMsg);

  int32_t (*FpOnAppendEntriesReply)(struct SSyncNode* ths, SyncAppendEntriesReply* pMsg);

  int32_t (*FpSendMsg)(void* handle, const SEpSet* pEpSet, SRpcMsg* pMsg);

} SSyncNode;

SSyncNode* syncNodeOpen(const SSyncInfo* pSyncInfo);

void syncNodeClose(SSyncNode* pSyncNode);

static int32_t doSyncNodePing(struct SSyncNode* ths, const SyncPing* pMsg);

static int32_t onSyncNodePing(struct SSyncNode* ths, SyncPing* pMsg);

static int32_t onSyncNodePingReply(struct SSyncNode* ths, SyncPingReply* pMsg);

static int32_t doSyncNodeRequestVote(struct SSyncNode* ths, const SyncRequestVote* pMsg);

static int32_t onSyncNodeRequestVote(struct SSyncNode* ths, SyncRequestVote* pMsg);

static int32_t onSyncNodeRequestVoteReply(struct SSyncNode* ths, SyncRequestVoteReply* pMsg);

static int32_t doSyncNodeAppendEntries(struct SSyncNode* ths, const SyncAppendEntries* pMsg);

static int32_t onSyncNodeAppendEntries(struct SSyncNode* ths, SyncAppendEntries* pMsg);

static int32_t onSyncNodeAppendEntriesReply(struct SSyncNode* ths, SyncAppendEntriesReply* pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_INT_H*/
