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

#define _DEFAULT_SOURCE
#include "syncMessage.h"
#include "syncRaftEntry.h"
#include "syncRaftStore.h"

int32_t syncBuildTimeout(SRpcMsg* pMsg, ESyncTimeoutType timeoutType, uint64_t logicClock, int32_t timerMS,
                         SSyncNode* pNode) {
  int32_t bytes = sizeof(SyncTimeout);
  pMsg->pCont = rpcMallocCont(bytes);
  pMsg->msgType = (timeoutType == SYNC_TIMEOUT_ELECTION) ? TDMT_SYNC_TIMEOUT_ELECTION : TDMT_SYNC_TIMEOUT;
  pMsg->contLen = bytes;
  if (pMsg->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SyncTimeout* pTimeout = pMsg->pCont;
  pTimeout->bytes = bytes;
  pTimeout->msgType = pMsg->msgType;
  pTimeout->vgId = pNode->vgId;
  pTimeout->timeoutType = timeoutType;
  pTimeout->logicClock = logicClock;
  pTimeout->timerMS = timerMS;
  pTimeout->timeStamp = taosGetTimestampMs();
  pTimeout->data = pNode;
  return 0;
}

int32_t syncBuildClientRequest(SRpcMsg* pMsg, const SRpcMsg* pOriginal, uint64_t seqNum, bool isWeak, int32_t vgId) {
  int32_t bytes = sizeof(SyncClientRequest) + pOriginal->contLen;
  pMsg->pCont = rpcMallocCont(bytes);
  pMsg->msgType = TDMT_SYNC_CLIENT_REQUEST;
  pMsg->contLen = bytes;
  if (pMsg->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SyncClientRequest* pClientRequest = pMsg->pCont;
  pClientRequest->bytes = bytes;
  pClientRequest->vgId = vgId;
  pClientRequest->msgType = TDMT_SYNC_CLIENT_REQUEST;
  pClientRequest->originalRpcType = pOriginal->msgType;
  pClientRequest->seqNum = seqNum;
  pClientRequest->isWeak = isWeak;
  pClientRequest->dataLen = pOriginal->contLen;
  memcpy(pClientRequest->data, (char*)pOriginal->pCont, pOriginal->contLen);

  return 0;
}

int32_t syncBuildClientRequestFromNoopEntry(SRpcMsg* pMsg, const SSyncRaftEntry* pEntry, int32_t vgId) {
  int32_t bytes = sizeof(SyncClientRequest) + pEntry->bytes;
  pMsg->pCont = rpcMallocCont(bytes);
  pMsg->msgType = TDMT_SYNC_CLIENT_REQUEST;
  pMsg->contLen = bytes;
  if (pMsg->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SyncClientRequest* pClientRequest = pMsg->pCont;
  pClientRequest->bytes = bytes;
  pClientRequest->vgId = vgId;
  pClientRequest->msgType = TDMT_SYNC_CLIENT_REQUEST;
  pClientRequest->originalRpcType = TDMT_SYNC_NOOP;
  pClientRequest->dataLen = pEntry->bytes;
  memcpy(pClientRequest->data, (char*)pEntry, pEntry->bytes);

  return 0;
}

int32_t syncBuildRequestVote(SRpcMsg* pMsg, int32_t vgId) {
  int32_t bytes = sizeof(SyncRequestVote);
  pMsg->pCont = rpcMallocCont(bytes);
  pMsg->msgType = TDMT_SYNC_REQUEST_VOTE;
  pMsg->contLen = bytes;
  if (pMsg->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SyncRequestVote* pRequestVote = pMsg->pCont;
  pRequestVote->bytes = bytes;
  pRequestVote->msgType = TDMT_SYNC_REQUEST_VOTE;
  pRequestVote->vgId = vgId;
  return 0;
}

int32_t syncBuildRequestVoteReply(SRpcMsg* pMsg, int32_t vgId) {
  int32_t bytes = sizeof(SyncRequestVoteReply);
  pMsg->pCont = rpcMallocCont(bytes);
  pMsg->msgType = TDMT_SYNC_REQUEST_VOTE_REPLY;
  pMsg->contLen = bytes;
  if (pMsg->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SyncRequestVoteReply* pRequestVoteReply = pMsg->pCont;
  pRequestVoteReply->bytes = bytes;
  pRequestVoteReply->msgType = TDMT_SYNC_REQUEST_VOTE_REPLY;
  pRequestVoteReply->vgId = vgId;
  return 0;
}

int32_t syncBuildAppendEntries(SRpcMsg* pMsg, int32_t dataLen, int32_t vgId) {
  int32_t bytes = sizeof(SyncAppendEntries) + dataLen;
  pMsg->pCont = rpcMallocCont(bytes);
  pMsg->msgType = TDMT_SYNC_APPEND_ENTRIES;
  pMsg->contLen = bytes;
  if (pMsg->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SyncAppendEntries* pAppendEntries = pMsg->pCont;
  pAppendEntries->bytes = bytes;
  pAppendEntries->vgId = vgId;
  pAppendEntries->msgType = TDMT_SYNC_APPEND_ENTRIES;
  pAppendEntries->dataLen = dataLen;
  return 0;
}

int32_t syncBuildAppendEntriesReply(SRpcMsg* pMsg, int32_t vgId) {
  int32_t bytes = sizeof(SyncAppendEntriesReply);
  pMsg->pCont = rpcMallocCont(bytes);
  pMsg->msgType = TDMT_SYNC_APPEND_ENTRIES_REPLY;
  pMsg->contLen = bytes;
  if (pMsg->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SyncAppendEntriesReply* pAppendEntriesReply = pMsg->pCont;
  pAppendEntriesReply->bytes = bytes;
  pAppendEntriesReply->msgType = TDMT_SYNC_APPEND_ENTRIES_REPLY;
  pAppendEntriesReply->vgId = vgId;
  return 0;
}

int32_t syncBuildAppendEntriesFromRaftEntry(SSyncNode* pNode, SSyncRaftEntry* pEntry, SyncTerm prevLogTerm,
                                            SRpcMsg* pRpcMsg) {
  uint32_t dataLen = pEntry->bytes;
  uint32_t bytes = sizeof(SyncAppendEntries) + dataLen;
  pRpcMsg->contLen = bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  if (pRpcMsg->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SyncAppendEntries* pMsg = pRpcMsg->pCont;
  pMsg->bytes = pRpcMsg->contLen;
  pMsg->msgType = pRpcMsg->msgType = TDMT_SYNC_APPEND_ENTRIES;
  pMsg->dataLen = dataLen;

  (void)memcpy(pMsg->data, pEntry, dataLen);

  pMsg->prevLogIndex = pEntry->index - 1;
  pMsg->prevLogTerm = prevLogTerm;
  pMsg->vgId = pNode->vgId;
  pMsg->srcId = pNode->myRaftId;
  pMsg->term = raftStoreGetTerm(pNode);
  pMsg->commitIndex = pNode->commitIndex;
  pMsg->privateTerm = 0;
  return 0;
}

int32_t syncBuildHeartbeat(SRpcMsg* pMsg, int32_t vgId) {
  int32_t bytes = sizeof(SyncHeartbeat);
  pMsg->pCont = rpcMallocCont(bytes);
  pMsg->msgType = TDMT_SYNC_HEARTBEAT;
  pMsg->contLen = bytes;
  if (pMsg->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SyncHeartbeat* pHeartbeat = pMsg->pCont;
  pHeartbeat->bytes = bytes;
  pHeartbeat->msgType = TDMT_SYNC_HEARTBEAT;
  pHeartbeat->vgId = vgId;
  return 0;
}

int32_t syncBuildHeartbeatReply(SRpcMsg* pMsg, int32_t vgId) {
  int32_t bytes = sizeof(SyncHeartbeatReply);
  pMsg->pCont = rpcMallocCont(bytes);
  pMsg->msgType = TDMT_SYNC_HEARTBEAT_REPLY;
  pMsg->contLen = bytes;
  if (pMsg->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SyncHeartbeatReply* pHeartbeatReply = pMsg->pCont;
  pHeartbeatReply->bytes = bytes;
  pHeartbeatReply->msgType = TDMT_SYNC_HEARTBEAT_REPLY;
  pHeartbeatReply->vgId = vgId;
  return 0;
}

int32_t syncBuildSnapshotSend(SRpcMsg* pMsg, int32_t dataLen, int32_t vgId) {
  int32_t bytes = sizeof(SyncSnapshotSend) + dataLen;
  pMsg->pCont = rpcMallocCont(bytes);
  pMsg->msgType = TDMT_SYNC_SNAPSHOT_SEND;
  pMsg->contLen = bytes;
  if (pMsg->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SyncSnapshotSend* pSnapshotSend = pMsg->pCont;
  pSnapshotSend->bytes = bytes;
  pSnapshotSend->vgId = vgId;
  pSnapshotSend->msgType = TDMT_SYNC_SNAPSHOT_SEND;
  pSnapshotSend->dataLen = dataLen;
  return 0;
}

int32_t syncBuildSnapshotSendRsp(SRpcMsg* pMsg, int32_t dataLen, int32_t vgId) {
  int32_t bytes = sizeof(SyncSnapshotRsp) + dataLen;
  pMsg->pCont = rpcMallocCont(bytes);
  pMsg->msgType = TDMT_SYNC_SNAPSHOT_RSP;
  pMsg->contLen = bytes;
  if (pMsg->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SyncSnapshotRsp* pPreSnapshotRsp = pMsg->pCont;
  pPreSnapshotRsp->bytes = bytes;
  pPreSnapshotRsp->msgType = TDMT_SYNC_SNAPSHOT_RSP;
  pPreSnapshotRsp->vgId = vgId;
  return 0;
}

int32_t syncBuildLeaderTransfer(SRpcMsg* pMsg, int32_t vgId) {
  int32_t bytes = sizeof(SyncLeaderTransfer);
  pMsg->pCont = rpcMallocCont(bytes);
  pMsg->msgType = TDMT_SYNC_LEADER_TRANSFER;
  pMsg->contLen = bytes;
  if (pMsg->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SyncLeaderTransfer* pLeaderTransfer = pMsg->pCont;
  pLeaderTransfer->bytes = bytes;
  pLeaderTransfer->msgType = TDMT_SYNC_LEADER_TRANSFER;
  pLeaderTransfer->vgId = vgId;
  return 0;
}

int32_t syncBuildLocalCmd(SRpcMsg* pMsg, int32_t vgId) {
  int32_t bytes = sizeof(SyncLocalCmd);
  pMsg->pCont = rpcMallocCont(bytes);
  pMsg->msgType = TDMT_SYNC_LOCAL_CMD;
  pMsg->contLen = bytes;
  if (pMsg->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SyncLocalCmd* pLocalCmd = pMsg->pCont;
  pLocalCmd->bytes = bytes;
  pLocalCmd->msgType = TDMT_SYNC_LOCAL_CMD;
  pLocalCmd->vgId = vgId;
  return 0;
}

const char* syncTimerTypeStr(enum ESyncTimeoutType timerType) {
  switch (timerType) {
    case SYNC_TIMEOUT_PING:
      return "ping";
    case SYNC_TIMEOUT_ELECTION:
      return "elect";
    case SYNC_TIMEOUT_HEARTBEAT:
      return "heartbeat";
    default:
      return "unknown";
  }
}

const char* syncLocalCmdGetStr(ESyncLocalCmd cmd) {
  switch (cmd) {
    case SYNC_LOCAL_CMD_STEP_DOWN:
      return "step-down";
    case SYNC_LOCAL_CMD_FOLLOWER_CMT:
      return "follower-commit";
    default:
      return "unknown-local-cmd";
  }
}
