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
#include "ttimer.h"

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
#define sInfo(...)                                        \
  {                                                       \
    if (sDebugFlag & DEBUG_INFO) {                        \
      taosPrintLog("SYN INFO ", sDebugFlag, __VA_ARGS__); \
    }                                                     \
  }
#define sDebug(...)                                        \
  {                                                        \
    if (sDebugFlag & DEBUG_DEBUG) {                        \
      taosPrintLog("SYN DEBUG ", sDebugFlag, __VA_ARGS__); \
    }                                                      \
  }
#define sTrace(...)                                        \
  {                                                        \
    if (sDebugFlag & DEBUG_TRACE) {                        \
      taosPrintLog("SYN TRACE ", sDebugFlag, __VA_ARGS__); \
    }                                                      \
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

struct SSyncEnv;
typedef struct SSyncEnv SSyncEnv;

typedef struct SRaftId {
  SyncNodeId  addr;  // typedef uint64_t SyncNodeId;
  SyncGroupId vgId;  // typedef int32_t  SyncGroupId;
} SRaftId;

typedef struct SSyncNode {
  SyncGroupId vgId;
  SSyncCfg    syncCfg;
  char        path[TSDB_FILENAME_LEN];
  SSyncFSM*   pFsm;

  // passed from outside
  void* rpcClient;
  int32_t (*FpSendMsg)(void* rpcClient, const SEpSet* pEpSet, SRpcMsg* pMsg);

  int32_t refCount;
  int64_t rid;

  SNodeInfo me;
  SNodeInfo peers[TSDB_MAX_REPLICA];
  int32_t   peersNum;

  ESyncRole role;
  SRaftId   raftId;

  tmr_h             pPingTimer;
  int32_t           pingTimerMS;
  uint8_t           pingTimerStart;
  TAOS_TMR_CALLBACK FpPingTimer;  // Timer Fp
  uint64_t          pingTimerCounter;

  tmr_h             pElectTimer;
  int32_t           electTimerMS;
  uint8_t           electTimerStart;
  TAOS_TMR_CALLBACK FpElectTimer;  // Timer Fp
  uint64_t          electTimerCounter;

  tmr_h             pHeartbeatTimer;
  int32_t           heartbeatTimerMS;
  uint8_t           heartbeatTimerStart;
  TAOS_TMR_CALLBACK FpHeartbeatTimer;  // Timer Fp
  uint64_t          heartbeatTimerCounter;

  // callback
  int32_t (*FpOnPing)(SSyncNode* ths, SyncPing* pMsg);

  int32_t (*FpOnPingReply)(SSyncNode* ths, SyncPingReply* pMsg);

  int32_t (*FpOnRequestVote)(SSyncNode* ths, SyncRequestVote* pMsg);

  int32_t (*FpOnRequestVoteReply)(SSyncNode* ths, SyncRequestVoteReply* pMsg);

  int32_t (*FpOnAppendEntries)(SSyncNode* ths, SyncAppendEntries* pMsg);

  int32_t (*FpOnAppendEntriesReply)(SSyncNode* ths, SyncAppendEntriesReply* pMsg);

} SSyncNode;

SSyncNode* syncNodeOpen(const SSyncInfo* pSyncInfo);

void syncNodeClose(SSyncNode* pSyncNode);

void syncNodePingAll(SSyncNode* pSyncNode);

void syncNodePingPeers(SSyncNode* pSyncNode);

void syncNodePingSelf(SSyncNode* pSyncNode);

int32_t syncNodeStartPingTimer(SSyncNode* pSyncNode);

int32_t syncNodeStopPingTimer(SSyncNode* pSyncNode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_INT_H*/
