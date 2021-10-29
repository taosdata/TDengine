/*
 * Copyright (c) 2019 TAOS Data, Inc. <cli@taosdata.com>
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

#include "raft.h"
#include "syncInt.h"

#ifndef MIN
#define MIN(x, y) (((x) < (y)) ? (x) : (y))
#endif

#define RAFT_READ_LOG_MAX_NUM 100

int32_t syncRaftStart(SSyncRaft* pRaft, const SSyncInfo* pInfo) {
  SSyncNode* pNode = pRaft->pNode;
  SSyncServerState serverState;
  SStateManager* stateManager;
  SSyncLogStore* logStore;
  SSyncFSM* fsm;
  SyncIndex initIndex = pInfo->snapshotIndex;
  SSyncBuffer buffer[RAFT_READ_LOG_MAX_NUM];
  int nBuf, limit, i;
  
  memcpy(&pRaft->info, pInfo, sizeof(SSyncInfo));
  stateManager = &(pRaft->info.stateManager);
  logStore = &(pRaft->info.logStore);
  fsm = &(pRaft->info.fsm);

  // read server state
  if (stateManager->readServerState(stateManager, &serverState) != 0) {
    syncError("readServerState for vgid %d fail", pInfo->vgId);
    return -1;
  }
  assert(initIndex <= serverState.commitIndex);

  // restore fsm state from snapshot index + 1, until commitIndex
  ++initIndex;
  while (initIndex < serverState.commitIndex) {
    limit = MIN(RAFT_READ_LOG_MAX_NUM, serverState.commitIndex - initIndex);

    if (logStore->logRead(logStore, initIndex, limit, buffer, &nBuf) != 0) {
      return -1;
    }
    assert(limit == nBuf);
  
    for (i = 0; i < limit; ++i) {
      fsm->applyLog(fsm, initIndex + i, &(buffer[i]), NULL);
      free(buffer[i].data);
    }
    initIndex += nBuf;
  }
  assert(initIndex == serverState.commitIndex);

  syncInfo("restore vgid %d state: snapshot index:", pInfo->vgId);
  return 0;
}

int32_t syncRaftStep(SSyncRaft* pRaft, const RaftMessage* pMsg) {
  syncFreeMessage(pMsg);
  return 0;
}

int32_t syncRaftTick(SSyncRaft* pRaft) {
  return 0;
}