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

#include "syncRaft.h"
#include "sync.h"

#if 0

SRaft* raftOpen(SRaftId raftId, SSyncFSM* pFsm) {
  SRaft* pRaft = (SRaft*)malloc(sizeof(SRaft));
  assert(pRaft != NULL);

  pRaft->id = raftId;
  pRaft->pFsm = pFsm;

  pRaft->FpPing = doRaftPing;
  pRaft->FpOnPing = onRaftPing;
  pRaft->FpOnPingReply = onRaftPingReply;

  pRaft->FpRequestVote = doRaftRequestVote;
  pRaft->FpOnRequestVote = onRaftRequestVote;
  pRaft->FpOnRequestVoteReply = onRaftRequestVoteReply;

  pRaft->FpAppendEntries = doRaftAppendEntries;
  pRaft->FpOnAppendEntries = onRaftAppendEntries;
  pRaft->FpOnAppendEntriesReply = onRaftAppendEntriesReply;

  return pRaft;
}

void raftClose(SRaft* pRaft) {
  assert(pRaft != NULL);
  free(pRaft);
}

static int32_t doRaftPing(struct SRaft* ths, const RaftPing* pMsg) { return 0; }

static int32_t onRaftPing(struct SRaft* ths, RaftPing* pMsg) { return 0; }

static int32_t onRaftPingReply(struct SRaft* ths, RaftPingReply* pMsg) { return 0; }

static int32_t doRaftRequestVote(struct SRaft* ths, const RaftRequestVote* pMsg) { return 0; }

static int32_t onRaftRequestVote(struct SRaft* ths, RaftRequestVote* pMsg) { return 0; }

static int32_t onRaftRequestVoteReply(struct SRaft* ths, RaftRequestVoteReply* pMsg) { return 0; }

static int32_t doRaftAppendEntries(struct SRaft* ths, const RaftAppendEntries* pMsg) { return 0; }

static int32_t onRaftAppendEntries(struct SRaft* ths, RaftAppendEntries* pMsg) { return 0; }

static int32_t onRaftAppendEntriesReply(struct SRaft* ths, RaftAppendEntriesReply* pMsg) { return 0; }

int32_t raftPropose(SRaft* pRaft, const SSyncBuffer* pBuf, bool isWeak) { return 0; }

static int raftSendMsg(SRaftId destRaftId, const void* pMsg, const SRaft* pRaft) { return 0; }

#endif