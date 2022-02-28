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

#ifndef _TD_LIBS_SYNC_RAFT_H
#define _TD_LIBS_SYNC_RAFT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "sync.h"
#include "syncMessage.h"
#include "taosdef.h"

#if 0

typedef struct SRaftId {
  SyncNodeId  addr;
  SyncGroupId vgId;
} SRaftId;

typedef struct SRaft {
  SRaftId   id;
  SSyncFSM* pFsm;

  int32_t (*FpPing)(struct SRaft* ths, const RaftPing* pMsg);

  int32_t (*FpOnPing)(struct SRaft* ths, RaftPing* pMsg);

  int32_t (*FpOnPingReply)(struct SRaft* ths, RaftPingReply* pMsg);

  int32_t (*FpRequestVote)(struct SRaft* ths, const RaftRequestVote* pMsg);

  int32_t (*FpOnRequestVote)(struct SRaft* ths, RaftRequestVote* pMsg);

  int32_t (*FpOnRequestVoteReply)(struct SRaft* ths, RaftRequestVoteReply* pMsg);

  int32_t (*FpAppendEntries)(struct SRaft* ths, const RaftAppendEntries* pMsg);

  int32_t (*FpOnAppendEntries)(struct SRaft* ths, RaftAppendEntries* pMsg);

  int32_t (*FpOnAppendEntriesReply)(struct SRaft* ths, RaftAppendEntriesReply* pMsg);

} SRaft;

SRaft* raftOpen(SRaftId raftId, SSyncFSM* pFsm);

void raftClose(SRaft* pRaft);

static int32_t doRaftPing(struct SRaft* ths, const RaftPing* pMsg);

static int32_t onRaftPing(struct SRaft* ths, RaftPing* pMsg);

static int32_t onRaftPingReply(struct SRaft* ths, RaftPingReply* pMsg);

static int32_t doRaftRequestVote(struct SRaft* ths, const RaftRequestVote* pMsg);

static int32_t onRaftRequestVote(struct SRaft* ths, RaftRequestVote* pMsg);

static int32_t onRaftRequestVoteReply(struct SRaft* ths, RaftRequestVoteReply* pMsg);

static int32_t doRaftAppendEntries(struct SRaft* ths, const RaftAppendEntries* pMsg);

static int32_t onRaftAppendEntries(struct SRaft* ths, RaftAppendEntries* pMsg);

static int32_t onRaftAppendEntriesReply(struct SRaft* ths, RaftAppendEntriesReply* pMsg);

int32_t raftPropose(SRaft* pRaft, const SSyncBuffer* pBuf, bool isWeak);

static int raftSendMsg(SRaftId destRaftId, const void* pMsg, const SRaft* pRaft);

#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_RAFT_H*/
