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
#include "syncRaftCfg.h"
#include "syncRaftEntry.h"
#include "syncUtil.h"
#include "tcoding.h"

int32_t syncBuildTimeout(SRpcMsg* pMsg, ESyncTimeoutType timeoutType, uint64_t logicClock, int32_t timerMS,
                         SSyncNode* pNode) {
  int32_t bytes = sizeof(SyncTimeout);
  pMsg->pCont = rpcMallocCont(bytes);
  pMsg->msgType = TDMT_SYNC_TIMEOUT;
  pMsg->contLen = bytes;
  if (pMsg->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SyncTimeout* pTimeout = pMsg->pCont;
  pTimeout->bytes = bytes;
  pTimeout->msgType = TDMT_SYNC_TIMEOUT;
  pTimeout->vgId = pNode->vgId;
  pTimeout->timeoutType = timeoutType;
  pTimeout->logicClock = logicClock;
  pTimeout->timerMS = timerMS;
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

// ---- message process SyncHeartbeat----
SyncHeartbeat* syncHeartbeatBuild(int32_t vgId) {
  uint32_t       bytes = sizeof(SyncHeartbeat);
  SyncHeartbeat* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->vgId = vgId;
  pMsg->msgType = TDMT_SYNC_HEARTBEAT;
  return pMsg;
}

void syncHeartbeatDestroy(SyncHeartbeat* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncHeartbeatSerialize(const SyncHeartbeat* pMsg, char* buf, uint32_t bufLen) {
  ASSERT(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncHeartbeatDeserialize(const char* buf, uint32_t len, SyncHeartbeat* pMsg) {
  memcpy(pMsg, buf, len);
  ASSERT(len == pMsg->bytes);
}

char* syncHeartbeatSerialize2(const SyncHeartbeat* pMsg, uint32_t* len) {
  char* buf = taosMemoryMalloc(pMsg->bytes);
  ASSERT(buf != NULL);
  syncHeartbeatSerialize(pMsg, buf, pMsg->bytes);
  if (len != NULL) {
    *len = pMsg->bytes;
  }
  return buf;
}

SyncHeartbeat* syncHeartbeatDeserialize2(const char* buf, uint32_t len) {
  uint32_t       bytes = *((uint32_t*)buf);
  SyncHeartbeat* pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  syncHeartbeatDeserialize(buf, len, pMsg);
  ASSERT(len == pMsg->bytes);
  return pMsg;
}

void syncHeartbeat2RpcMsg(const SyncHeartbeat* pMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pMsg->msgType;
  pRpcMsg->contLen = pMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  syncHeartbeatSerialize(pMsg, pRpcMsg->pCont, pRpcMsg->contLen);
}

void syncHeartbeatFromRpcMsg(const SRpcMsg* pRpcMsg, SyncHeartbeat* pMsg) {
  syncHeartbeatDeserialize(pRpcMsg->pCont, pRpcMsg->contLen, pMsg);
}

SyncHeartbeat* syncHeartbeatFromRpcMsg2(const SRpcMsg* pRpcMsg) {
  SyncHeartbeat* pMsg = syncHeartbeatDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
  ASSERT(pMsg != NULL);
  return pMsg;
}

cJSON* syncHeartbeat2Json(const SyncHeartbeat* pMsg) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);

    cJSON* pSrcId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->srcId.addr);
    cJSON_AddStringToObject(pSrcId, "addr", u64buf);
    {
      uint64_t u64 = pMsg->srcId.addr;
      cJSON*   pTmp = pSrcId;
      char     host[128] = {0};
      uint16_t port;
      syncUtilU642Addr(u64, host, sizeof(host), &port);
      cJSON_AddStringToObject(pTmp, "addr_host", host);
      cJSON_AddNumberToObject(pTmp, "addr_port", port);
    }
    cJSON_AddNumberToObject(pSrcId, "vgId", pMsg->srcId.vgId);
    cJSON_AddItemToObject(pRoot, "srcId", pSrcId);

    cJSON* pDestId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->destId.addr);
    cJSON_AddStringToObject(pDestId, "addr", u64buf);
    {
      uint64_t u64 = pMsg->destId.addr;
      cJSON*   pTmp = pDestId;
      char     host[128] = {0};
      uint16_t port;
      syncUtilU642Addr(u64, host, sizeof(host), &port);
      cJSON_AddStringToObject(pTmp, "addr_host", host);
      cJSON_AddNumberToObject(pTmp, "addr_port", port);
    }
    cJSON_AddNumberToObject(pDestId, "vgId", pMsg->destId.vgId);
    cJSON_AddItemToObject(pRoot, "destId", pDestId);

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->privateTerm);
    cJSON_AddStringToObject(pRoot, "privateTerm", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pMsg->commitIndex);
    cJSON_AddStringToObject(pRoot, "commitIndex", u64buf);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncHeartbeat", pRoot);
  return pJson;
}

char* syncHeartbeat2Str(const SyncHeartbeat* pMsg) {
  cJSON* pJson = syncHeartbeat2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

void syncHeartbeatPrint(const SyncHeartbeat* pMsg) {
  char* serialized = syncHeartbeat2Str(pMsg);
  printf("syncHeartbeatPrint | len:%d | %s \n", (int32_t)strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncHeartbeatPrint2(char* s, const SyncHeartbeat* pMsg) {
  char* serialized = syncHeartbeat2Str(pMsg);
  printf("syncHeartbeatPrint2 | len:%d | %s | %s \n", (int32_t)strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncHeartbeatLog(const SyncHeartbeat* pMsg) {
  char* serialized = syncHeartbeat2Str(pMsg);
  sTrace("syncHeartbeatLog | len:%d | %s", (int32_t)strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncHeartbeatLog2(char* s, const SyncHeartbeat* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncHeartbeat2Str(pMsg);
    sTrace("syncHeartbeatLog2 | len:%d | %s | %s", (int32_t)strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// ---- message process SyncHeartbeatReply----
SyncHeartbeatReply* syncHeartbeatReplyBuild(int32_t vgId) {
  uint32_t            bytes = sizeof(SyncHeartbeatReply);
  SyncHeartbeatReply* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->vgId = vgId;
  pMsg->msgType = TDMT_SYNC_HEARTBEAT_REPLY;
  return pMsg;
}

void syncHeartbeatReplyDestroy(SyncHeartbeatReply* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncHeartbeatReplySerialize(const SyncHeartbeatReply* pMsg, char* buf, uint32_t bufLen) {
  ASSERT(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncHeartbeatReplyDeserialize(const char* buf, uint32_t len, SyncHeartbeatReply* pMsg) {
  memcpy(pMsg, buf, len);
  ASSERT(len == pMsg->bytes);
}

char* syncHeartbeatReplySerialize2(const SyncHeartbeatReply* pMsg, uint32_t* len) {
  char* buf = taosMemoryMalloc(pMsg->bytes);
  ASSERT(buf != NULL);
  syncHeartbeatReplySerialize(pMsg, buf, pMsg->bytes);
  if (len != NULL) {
    *len = pMsg->bytes;
  }
  return buf;
}

SyncHeartbeatReply* syncHeartbeatReplyDeserialize2(const char* buf, uint32_t len) {
  uint32_t            bytes = *((uint32_t*)buf);
  SyncHeartbeatReply* pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  syncHeartbeatReplyDeserialize(buf, len, pMsg);
  ASSERT(len == pMsg->bytes);
  return pMsg;
}

void syncHeartbeatReply2RpcMsg(const SyncHeartbeatReply* pMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pMsg->msgType;
  pRpcMsg->contLen = pMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  syncHeartbeatReplySerialize(pMsg, pRpcMsg->pCont, pRpcMsg->contLen);
}

void syncHeartbeatReplyFromRpcMsg(const SRpcMsg* pRpcMsg, SyncHeartbeatReply* pMsg) {
  syncHeartbeatReplyDeserialize(pRpcMsg->pCont, pRpcMsg->contLen, pMsg);
}

SyncHeartbeatReply* syncHeartbeatReplyFromRpcMsg2(const SRpcMsg* pRpcMsg) {
  SyncHeartbeatReply* pMsg = syncHeartbeatReplyDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
  ASSERT(pMsg != NULL);
  return pMsg;
}

cJSON* syncHeartbeatReply2Json(const SyncHeartbeatReply* pMsg) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);

    cJSON* pSrcId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->srcId.addr);
    cJSON_AddStringToObject(pSrcId, "addr", u64buf);
    {
      uint64_t u64 = pMsg->srcId.addr;
      cJSON*   pTmp = pSrcId;
      char     host[128] = {0};
      uint16_t port;
      syncUtilU642Addr(u64, host, sizeof(host), &port);
      cJSON_AddStringToObject(pTmp, "addr_host", host);
      cJSON_AddNumberToObject(pTmp, "addr_port", port);
    }
    cJSON_AddNumberToObject(pSrcId, "vgId", pMsg->srcId.vgId);
    cJSON_AddItemToObject(pRoot, "srcId", pSrcId);

    cJSON* pDestId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->destId.addr);
    cJSON_AddStringToObject(pDestId, "addr", u64buf);
    {
      uint64_t u64 = pMsg->destId.addr;
      cJSON*   pTmp = pDestId;
      char     host[128] = {0};
      uint16_t port;
      syncUtilU642Addr(u64, host, sizeof(host), &port);
      cJSON_AddStringToObject(pTmp, "addr_host", host);
      cJSON_AddNumberToObject(pTmp, "addr_port", port);
    }
    cJSON_AddNumberToObject(pDestId, "vgId", pMsg->destId.vgId);
    cJSON_AddItemToObject(pRoot, "destId", pDestId);

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->privateTerm);
    cJSON_AddStringToObject(pRoot, "privateTerm", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);

    cJSON_AddStringToObject(pRoot, "matchIndex", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pMsg->startTime);
    cJSON_AddStringToObject(pRoot, "startTime", u64buf);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncHeartbeatReply", pRoot);
  return pJson;
}

char* syncHeartbeatReply2Str(const SyncHeartbeatReply* pMsg) {
  cJSON* pJson = syncHeartbeatReply2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

void syncHeartbeatReplyPrint(const SyncHeartbeatReply* pMsg) {
  char* serialized = syncHeartbeatReply2Str(pMsg);
  printf("syncHeartbeatReplyPrint | len:%d | %s \n", (int32_t)strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncHeartbeatReplyPrint2(char* s, const SyncHeartbeatReply* pMsg) {
  char* serialized = syncHeartbeatReply2Str(pMsg);
  printf("syncHeartbeatReplyPrint2 | len:%d | %s | %s \n", (int32_t)strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncHeartbeatReplyLog(const SyncHeartbeatReply* pMsg) {
  char* serialized = syncHeartbeatReply2Str(pMsg);
  sTrace("syncHeartbeatReplyLog | len:%d | %s", (int32_t)strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncHeartbeatReplyLog2(char* s, const SyncHeartbeatReply* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncHeartbeatReply2Str(pMsg);
    sTrace("syncHeartbeatReplyLog2 | len:%d | %s | %s", (int32_t)strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// ---- message process SyncPreSnapshot----
SyncPreSnapshot* syncPreSnapshotBuild(int32_t vgId) {
  uint32_t         bytes = sizeof(SyncPreSnapshot);
  SyncPreSnapshot* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->vgId = vgId;
  pMsg->msgType = TDMT_SYNC_PRE_SNAPSHOT;
  return pMsg;
}

void syncPreSnapshotDestroy(SyncPreSnapshot* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncPreSnapshotSerialize(const SyncPreSnapshot* pMsg, char* buf, uint32_t bufLen) {
  ASSERT(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncPreSnapshotDeserialize(const char* buf, uint32_t len, SyncPreSnapshot* pMsg) {
  memcpy(pMsg, buf, len);
  ASSERT(len == pMsg->bytes);
}

char* syncPreSnapshotSerialize2(const SyncPreSnapshot* pMsg, uint32_t* len) {
  char* buf = taosMemoryMalloc(pMsg->bytes);
  ASSERT(buf != NULL);
  syncPreSnapshotSerialize(pMsg, buf, pMsg->bytes);
  if (len != NULL) {
    *len = pMsg->bytes;
  }
  return buf;
}

SyncPreSnapshot* syncPreSnapshotDeserialize2(const char* buf, uint32_t len) {
  uint32_t         bytes = *((uint32_t*)buf);
  SyncPreSnapshot* pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  syncPreSnapshotDeserialize(buf, len, pMsg);
  ASSERT(len == pMsg->bytes);
  return pMsg;
}

void syncPreSnapshot2RpcMsg(const SyncPreSnapshot* pMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pMsg->msgType;
  pRpcMsg->contLen = pMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  syncPreSnapshotSerialize(pMsg, pRpcMsg->pCont, pRpcMsg->contLen);
}

void syncPreSnapshotFromRpcMsg(const SRpcMsg* pRpcMsg, SyncPreSnapshot* pMsg) {
  syncPreSnapshotDeserialize(pRpcMsg->pCont, pRpcMsg->contLen, pMsg);
}

SyncPreSnapshot* syncPreSnapshotFromRpcMsg2(const SRpcMsg* pRpcMsg) {
  SyncPreSnapshot* pMsg = syncPreSnapshotDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
  ASSERT(pMsg != NULL);
  return pMsg;
}

cJSON* syncPreSnapshot2Json(const SyncPreSnapshot* pMsg) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);

    cJSON* pSrcId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->srcId.addr);
    cJSON_AddStringToObject(pSrcId, "addr", u64buf);
    {
      uint64_t u64 = pMsg->srcId.addr;
      cJSON*   pTmp = pSrcId;
      char     host[128] = {0};
      uint16_t port;
      syncUtilU642Addr(u64, host, sizeof(host), &port);
      cJSON_AddStringToObject(pTmp, "addr_host", host);
      cJSON_AddNumberToObject(pTmp, "addr_port", port);
    }
    cJSON_AddNumberToObject(pSrcId, "vgId", pMsg->srcId.vgId);
    cJSON_AddItemToObject(pRoot, "srcId", pSrcId);

    cJSON* pDestId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->destId.addr);
    cJSON_AddStringToObject(pDestId, "addr", u64buf);
    {
      uint64_t u64 = pMsg->destId.addr;
      cJSON*   pTmp = pDestId;
      char     host[128] = {0};
      uint16_t port;
      syncUtilU642Addr(u64, host, sizeof(host), &port);
      cJSON_AddStringToObject(pTmp, "addr_host", host);
      cJSON_AddNumberToObject(pTmp, "addr_port", port);
    }
    cJSON_AddNumberToObject(pDestId, "vgId", pMsg->destId.vgId);
    cJSON_AddItemToObject(pRoot, "destId", pDestId);

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncPreSnapshot", pRoot);
  return pJson;
}

char* syncPreSnapshot2Str(const SyncPreSnapshot* pMsg) {
  cJSON* pJson = syncPreSnapshot2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

void syncPreSnapshotPrint(const SyncPreSnapshot* pMsg) {
  char* serialized = syncPreSnapshot2Str(pMsg);
  printf("syncPreSnapshotPrint | len:%d | %s \n", (int32_t)strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncPreSnapshotPrint2(char* s, const SyncPreSnapshot* pMsg) {
  char* serialized = syncPreSnapshot2Str(pMsg);
  printf("syncPreSnapshotPrint2 | len:%d | %s | %s \n", (int32_t)strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncPreSnapshotLog(const SyncPreSnapshot* pMsg) {
  char* serialized = syncPreSnapshot2Str(pMsg);
  sTrace("syncPreSnapshotLog | len:%d | %s", (int32_t)strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncPreSnapshotLog2(char* s, const SyncPreSnapshot* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncPreSnapshot2Str(pMsg);
    sTrace("syncPreSnapshotLog2 | len:%d | %s | %s", (int32_t)strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// ---- message process SyncPreSnapshotReply----
SyncPreSnapshotReply* syncPreSnapshotReplyBuild(int32_t vgId) {
  uint32_t              bytes = sizeof(SyncPreSnapshotReply);
  SyncPreSnapshotReply* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->vgId = vgId;
  pMsg->msgType = TDMT_SYNC_PRE_SNAPSHOT_REPLY;
  return pMsg;
}

void syncPreSnapshotReplyDestroy(SyncPreSnapshotReply* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncPreSnapshotReplySerialize(const SyncPreSnapshotReply* pMsg, char* buf, uint32_t bufLen) {
  ASSERT(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncPreSnapshotReplyDeserialize(const char* buf, uint32_t len, SyncPreSnapshotReply* pMsg) {
  memcpy(pMsg, buf, len);
  ASSERT(len == pMsg->bytes);
}

char* syncPreSnapshotReplySerialize2(const SyncPreSnapshotReply* pMsg, uint32_t* len) {
  char* buf = taosMemoryMalloc(pMsg->bytes);
  ASSERT(buf != NULL);
  syncPreSnapshotReplySerialize(pMsg, buf, pMsg->bytes);
  if (len != NULL) {
    *len = pMsg->bytes;
  }
  return buf;
}

SyncPreSnapshotReply* syncPreSnapshotReplyDeserialize2(const char* buf, uint32_t len) {
  uint32_t              bytes = *((uint32_t*)buf);
  SyncPreSnapshotReply* pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  syncPreSnapshotReplyDeserialize(buf, len, pMsg);
  ASSERT(len == pMsg->bytes);
  return pMsg;
}

void syncPreSnapshotReply2RpcMsg(const SyncPreSnapshotReply* pMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pMsg->msgType;
  pRpcMsg->contLen = pMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  syncPreSnapshotReplySerialize(pMsg, pRpcMsg->pCont, pRpcMsg->contLen);
}

void syncPreSnapshotReplyFromRpcMsg(const SRpcMsg* pRpcMsg, SyncPreSnapshotReply* pMsg) {
  syncPreSnapshotReplyDeserialize(pRpcMsg->pCont, pRpcMsg->contLen, pMsg);
}

SyncPreSnapshotReply* syncPreSnapshotReplyFromRpcMsg2(const SRpcMsg* pRpcMsg) {
  SyncPreSnapshotReply* pMsg = syncPreSnapshotReplyDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
  ASSERT(pMsg != NULL);
  return pMsg;
}

cJSON* syncPreSnapshotReply2Json(const SyncPreSnapshotReply* pMsg) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);

    cJSON* pSrcId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->srcId.addr);
    cJSON_AddStringToObject(pSrcId, "addr", u64buf);
    {
      uint64_t u64 = pMsg->srcId.addr;
      cJSON*   pTmp = pSrcId;
      char     host[128] = {0};
      uint16_t port;
      syncUtilU642Addr(u64, host, sizeof(host), &port);
      cJSON_AddStringToObject(pTmp, "addr_host", host);
      cJSON_AddNumberToObject(pTmp, "addr_port", port);
    }
    cJSON_AddNumberToObject(pSrcId, "vgId", pMsg->srcId.vgId);
    cJSON_AddItemToObject(pRoot, "srcId", pSrcId);

    cJSON* pDestId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->destId.addr);
    cJSON_AddStringToObject(pDestId, "addr", u64buf);
    {
      uint64_t u64 = pMsg->destId.addr;
      cJSON*   pTmp = pDestId;
      char     host[128] = {0};
      uint16_t port;
      syncUtilU642Addr(u64, host, sizeof(host), &port);
      cJSON_AddStringToObject(pTmp, "addr_host", host);
      cJSON_AddNumberToObject(pTmp, "addr_port", port);
    }
    cJSON_AddNumberToObject(pDestId, "vgId", pMsg->destId.vgId);
    cJSON_AddItemToObject(pRoot, "destId", pDestId);

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pMsg->snapStart);
    cJSON_AddStringToObject(pRoot, "snap-start", u64buf);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncPreSnapshotReply", pRoot);
  return pJson;
}

char* syncPreSnapshotReply2Str(const SyncPreSnapshotReply* pMsg) {
  cJSON* pJson = syncPreSnapshotReply2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

void syncPreSnapshotReplyPrint(const SyncPreSnapshotReply* pMsg) {
  char* serialized = syncPreSnapshotReply2Str(pMsg);
  printf("syncPreSnapshotReplyPrint | len:%d | %s \n", (int32_t)strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncPreSnapshotReplyPrint2(char* s, const SyncPreSnapshotReply* pMsg) {
  char* serialized = syncPreSnapshotReply2Str(pMsg);
  printf("syncPreSnapshotReplyPrint2 | len:%d | %s | %s \n", (int32_t)strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncPreSnapshotReplyLog(const SyncPreSnapshotReply* pMsg) {
  char* serialized = syncPreSnapshotReply2Str(pMsg);
  sTrace("syncPreSnapshotReplyLog | len:%d | %s", (int32_t)strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncPreSnapshotReplyLog2(char* s, const SyncPreSnapshotReply* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncPreSnapshotReply2Str(pMsg);
    sTrace("syncPreSnapshotReplyLog2 | len:%d | %s | %s", (int32_t)strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// ---- message process SyncApplyMsg----
SyncApplyMsg* syncApplyMsgBuild(uint32_t dataLen) {
  uint32_t      bytes = sizeof(SyncApplyMsg) + dataLen;
  SyncApplyMsg* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->msgType = TDMT_SYNC_APPLY_MSG;
  pMsg->dataLen = dataLen;
  return pMsg;
}

SyncApplyMsg* syncApplyMsgBuild2(const SRpcMsg* pOriginalRpcMsg, int32_t vgId, SFsmCbMeta* pMeta) {
  SyncApplyMsg* pMsg = syncApplyMsgBuild(pOriginalRpcMsg->contLen);
  pMsg->vgId = vgId;
  pMsg->originalRpcType = pOriginalRpcMsg->msgType;
  pMsg->fsmMeta = *pMeta;
  memcpy(pMsg->data, pOriginalRpcMsg->pCont, pOriginalRpcMsg->contLen);
  return pMsg;
}

void syncApplyMsgDestroy(SyncApplyMsg* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncApplyMsgSerialize(const SyncApplyMsg* pMsg, char* buf, uint32_t bufLen) {
  ASSERT(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncApplyMsgDeserialize(const char* buf, uint32_t len, SyncApplyMsg* pMsg) {
  memcpy(pMsg, buf, len);
  ASSERT(len == pMsg->bytes);
}

char* syncApplyMsgSerialize2(const SyncApplyMsg* pMsg, uint32_t* len) {
  char* buf = taosMemoryMalloc(pMsg->bytes);
  ASSERT(buf != NULL);
  syncApplyMsgSerialize(pMsg, buf, pMsg->bytes);
  if (len != NULL) {
    *len = pMsg->bytes;
  }
  return buf;
}

SyncApplyMsg* syncApplyMsgDeserialize2(const char* buf, uint32_t len) {
  uint32_t      bytes = *((uint32_t*)buf);
  SyncApplyMsg* pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  syncApplyMsgDeserialize(buf, len, pMsg);
  ASSERT(len == pMsg->bytes);
  return pMsg;
}

// SyncApplyMsg to SRpcMsg, put it into ApplyQ
void syncApplyMsg2RpcMsg(const SyncApplyMsg* pMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pMsg->msgType;
  pRpcMsg->contLen = pMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  syncApplyMsgSerialize(pMsg, pRpcMsg->pCont, pRpcMsg->contLen);
}

// get SRpcMsg from ApplyQ, to SyncApplyMsg
void syncApplyMsgFromRpcMsg(const SRpcMsg* pRpcMsg, SyncApplyMsg* pMsg) {
  syncApplyMsgDeserialize(pRpcMsg->pCont, pRpcMsg->contLen, pMsg);
}

SyncApplyMsg* syncApplyMsgFromRpcMsg2(const SRpcMsg* pRpcMsg) {
  SyncApplyMsg* pMsg = syncApplyMsgDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
  return pMsg;
}

// SyncApplyMsg to OriginalRpcMsg
void syncApplyMsg2OriginalRpcMsg(const SyncApplyMsg* pMsg, SRpcMsg* pOriginalRpcMsg) {
  memset(pOriginalRpcMsg, 0, sizeof(*pOriginalRpcMsg));
  pOriginalRpcMsg->msgType = pMsg->originalRpcType;
  pOriginalRpcMsg->contLen = pMsg->dataLen;
  pOriginalRpcMsg->pCont = rpcMallocCont(pOriginalRpcMsg->contLen);
  memcpy(pOriginalRpcMsg->pCont, pMsg->data, pOriginalRpcMsg->contLen);
}

cJSON* syncApplyMsg2Json(const SyncApplyMsg* pMsg) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);
    cJSON_AddNumberToObject(pRoot, "originalRpcType", pMsg->originalRpcType);

    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pMsg->fsmMeta.index);
    cJSON_AddStringToObject(pRoot, "fsmMeta.index", u64buf);
    cJSON_AddNumberToObject(pRoot, "fsmMeta.isWeak", pMsg->fsmMeta.isWeak);
    cJSON_AddNumberToObject(pRoot, "fsmMeta.code", pMsg->fsmMeta.code);
    cJSON_AddNumberToObject(pRoot, "fsmMeta.state", pMsg->fsmMeta.state);
    cJSON_AddStringToObject(pRoot, "fsmMeta.state.str", syncStr(pMsg->fsmMeta.state));
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->fsmMeta.seqNum);
    cJSON_AddStringToObject(pRoot, "fsmMeta.seqNum", u64buf);

    cJSON_AddNumberToObject(pRoot, "dataLen", pMsg->dataLen);
    char* s;
    s = syncUtilPrintBin((char*)(pMsg->data), pMsg->dataLen);
    cJSON_AddStringToObject(pRoot, "data", s);
    taosMemoryFree(s);
    s = syncUtilPrintBin2((char*)(pMsg->data), pMsg->dataLen);
    cJSON_AddStringToObject(pRoot, "data2", s);
    taosMemoryFree(s);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncApplyMsg", pRoot);
  return pJson;
}

char* syncApplyMsg2Str(const SyncApplyMsg* pMsg) {
  cJSON* pJson = syncApplyMsg2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug ----------------------
void syncApplyMsgPrint(const SyncApplyMsg* pMsg) {
  char* serialized = syncApplyMsg2Str(pMsg);
  printf("syncApplyMsgPrint | len:%d | %s \n", (int32_t)strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncApplyMsgPrint2(char* s, const SyncApplyMsg* pMsg) {
  char* serialized = syncApplyMsg2Str(pMsg);
  printf("syncApplyMsgPrint2 | len:%d | %s | %s \n", (int32_t)strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncApplyMsgLog(const SyncApplyMsg* pMsg) {
  char* serialized = syncApplyMsg2Str(pMsg);
  sTrace("ssyncApplyMsgLog | len:%d | %s", (int32_t)strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncApplyMsgLog2(char* s, const SyncApplyMsg* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncApplyMsg2Str(pMsg);
    sTrace("syncApplyMsgLog2 | len:%d | %s | %s", (int32_t)strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// ---------------------------------------------
SyncSnapshotSend* syncSnapshotSendBuild(uint32_t dataLen, int32_t vgId) {
  uint32_t          bytes = sizeof(SyncSnapshotSend) + dataLen;
  SyncSnapshotSend* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->vgId = vgId;
  pMsg->msgType = TDMT_SYNC_SNAPSHOT_SEND;
  pMsg->dataLen = dataLen;
  return pMsg;
}

void syncSnapshotSendDestroy(SyncSnapshotSend* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncSnapshotSendSerialize(const SyncSnapshotSend* pMsg, char* buf, uint32_t bufLen) {
  ASSERT(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncSnapshotSendDeserialize(const char* buf, uint32_t len, SyncSnapshotSend* pMsg) {
  memcpy(pMsg, buf, len);
  ASSERT(len == pMsg->bytes);
  ASSERT(pMsg->bytes == sizeof(SyncSnapshotSend) + pMsg->dataLen);
}

char* syncSnapshotSendSerialize2(const SyncSnapshotSend* pMsg, uint32_t* len) {
  char* buf = taosMemoryMalloc(pMsg->bytes);
  ASSERT(buf != NULL);
  syncSnapshotSendSerialize(pMsg, buf, pMsg->bytes);
  if (len != NULL) {
    *len = pMsg->bytes;
  }
  return buf;
}

SyncSnapshotSend* syncSnapshotSendDeserialize2(const char* buf, uint32_t len) {
  uint32_t          bytes = *((uint32_t*)buf);
  SyncSnapshotSend* pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  syncSnapshotSendDeserialize(buf, len, pMsg);
  ASSERT(len == pMsg->bytes);
  return pMsg;
}

void syncSnapshotSend2RpcMsg(const SyncSnapshotSend* pMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pMsg->msgType;
  pRpcMsg->contLen = pMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  syncSnapshotSendSerialize(pMsg, pRpcMsg->pCont, pRpcMsg->contLen);
}

void syncSnapshotSendFromRpcMsg(const SRpcMsg* pRpcMsg, SyncSnapshotSend* pMsg) {
  syncSnapshotSendDeserialize(pRpcMsg->pCont, pRpcMsg->contLen, pMsg);
}

SyncSnapshotSend* syncSnapshotSendFromRpcMsg2(const SRpcMsg* pRpcMsg) {
  SyncSnapshotSend* pMsg = syncSnapshotSendDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
  ASSERT(pMsg != NULL);
  return pMsg;
}

SyncSnapshotRsp* syncSnapshotRspBuild(int32_t vgId) {
  uint32_t         bytes = sizeof(SyncSnapshotRsp);
  SyncSnapshotRsp* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->vgId = vgId;
  pMsg->msgType = TDMT_SYNC_SNAPSHOT_RSP;
  return pMsg;
}

void syncSnapshotRspDestroy(SyncSnapshotRsp* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncSnapshotRspSerialize(const SyncSnapshotRsp* pMsg, char* buf, uint32_t bufLen) {
  ASSERT(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncSnapshotRspDeserialize(const char* buf, uint32_t len, SyncSnapshotRsp* pMsg) {
  memcpy(pMsg, buf, len);
  ASSERT(len == pMsg->bytes);
}

char* syncSnapshotRspSerialize2(const SyncSnapshotRsp* pMsg, uint32_t* len) {
  char* buf = taosMemoryMalloc(pMsg->bytes);
  ASSERT(buf != NULL);
  syncSnapshotRspSerialize(pMsg, buf, pMsg->bytes);
  if (len != NULL) {
    *len = pMsg->bytes;
  }
  return buf;
}

SyncSnapshotRsp* syncSnapshotRspDeserialize2(const char* buf, uint32_t len) {
  uint32_t         bytes = *((uint32_t*)buf);
  SyncSnapshotRsp* pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  syncSnapshotRspDeserialize(buf, len, pMsg);
  ASSERT(len == pMsg->bytes);
  return pMsg;
}

void syncSnapshotRsp2RpcMsg(const SyncSnapshotRsp* pMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pMsg->msgType;
  pRpcMsg->contLen = pMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  syncSnapshotRspSerialize(pMsg, pRpcMsg->pCont, pRpcMsg->contLen);
}

void syncSnapshotRspFromRpcMsg(const SRpcMsg* pRpcMsg, SyncSnapshotRsp* pMsg) {
  syncSnapshotRspDeserialize(pRpcMsg->pCont, pRpcMsg->contLen, pMsg);
}

SyncSnapshotRsp* syncSnapshotRspFromRpcMsg2(const SRpcMsg* pRpcMsg) {
  SyncSnapshotRsp* pMsg = syncSnapshotRspDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
  ASSERT(pMsg != NULL);
  return pMsg;
}

cJSON* syncSnapshotRsp2Json(const SyncSnapshotRsp* pMsg) {
  char   u64buf[128];
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);

    cJSON* pSrcId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->srcId.addr);
    cJSON_AddStringToObject(pSrcId, "addr", u64buf);
    {
      uint64_t u64 = pMsg->srcId.addr;
      cJSON*   pTmp = pSrcId;
      char     host[128];
      uint16_t port;
      syncUtilU642Addr(u64, host, sizeof(host), &port);
      cJSON_AddStringToObject(pTmp, "addr_host", host);
      cJSON_AddNumberToObject(pTmp, "addr_port", port);
    }
    cJSON_AddNumberToObject(pSrcId, "vgId", pMsg->srcId.vgId);
    cJSON_AddItemToObject(pRoot, "srcId", pSrcId);

    cJSON* pDestId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->destId.addr);
    cJSON_AddStringToObject(pDestId, "addr", u64buf);
    {
      uint64_t u64 = pMsg->destId.addr;
      cJSON*   pTmp = pDestId;
      char     host[128];
      uint16_t port;
      syncUtilU642Addr(u64, host, sizeof(host), &port);
      cJSON_AddStringToObject(pTmp, "addr_host", host);
      cJSON_AddNumberToObject(pTmp, "addr_port", port);
    }
    cJSON_AddNumberToObject(pDestId, "vgId", pMsg->destId.vgId);
    cJSON_AddItemToObject(pRoot, "destId", pDestId);

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pMsg->startTime);
    cJSON_AddStringToObject(pRoot, "startTime", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pMsg->lastIndex);
    cJSON_AddStringToObject(pRoot, "lastIndex", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->lastTerm);
    cJSON_AddStringToObject(pRoot, "lastTerm", u64buf);

    cJSON_AddNumberToObject(pRoot, "ack", pMsg->ack);
    cJSON_AddNumberToObject(pRoot, "code", pMsg->code);

    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pMsg->snapBeginIndex);
    cJSON_AddStringToObject(pRoot, "snap-begin", u64buf);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncSnapshotRsp", pRoot);
  return pJson;
}

char* syncSnapshotRsp2Str(const SyncSnapshotRsp* pMsg) {
  cJSON* pJson = syncSnapshotRsp2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug ----------------------
void syncSnapshotRspPrint(const SyncSnapshotRsp* pMsg) {
  char* serialized = syncSnapshotRsp2Str(pMsg);
  printf("syncSnapshotRspPrint | len:%d | %s \n", (int32_t)strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncSnapshotRspPrint2(char* s, const SyncSnapshotRsp* pMsg) {
  char* serialized = syncSnapshotRsp2Str(pMsg);
  printf("syncSnapshotRspPrint2 | len:%d | %s | %s \n", (int32_t)strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncSnapshotRspLog(const SyncSnapshotRsp* pMsg) {
  char* serialized = syncSnapshotRsp2Str(pMsg);
  sTrace("syncSnapshotRspLog | len:%d | %s", (int32_t)strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncSnapshotRspLog2(char* s, const SyncSnapshotRsp* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncSnapshotRsp2Str(pMsg);
    sTrace("syncSnapshotRspLog2 | len:%d | %s | %s", (int32_t)strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// ---------------------------------------------
SyncLeaderTransfer* syncLeaderTransferBuild(int32_t vgId) {
  uint32_t            bytes = sizeof(SyncLeaderTransfer);
  SyncLeaderTransfer* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->vgId = vgId;
  pMsg->msgType = TDMT_SYNC_LEADER_TRANSFER;
  return pMsg;
}

void syncLeaderTransferDestroy(SyncLeaderTransfer* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncLeaderTransferSerialize(const SyncLeaderTransfer* pMsg, char* buf, uint32_t bufLen) {
  ASSERT(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncLeaderTransferDeserialize(const char* buf, uint32_t len, SyncLeaderTransfer* pMsg) {
  memcpy(pMsg, buf, len);
  ASSERT(len == pMsg->bytes);
}

char* syncLeaderTransferSerialize2(const SyncLeaderTransfer* pMsg, uint32_t* len) {
  char* buf = taosMemoryMalloc(pMsg->bytes);
  ASSERT(buf != NULL);
  syncLeaderTransferSerialize(pMsg, buf, pMsg->bytes);
  if (len != NULL) {
    *len = pMsg->bytes;
  }
  return buf;
}

SyncLeaderTransfer* syncLeaderTransferDeserialize2(const char* buf, uint32_t len) {
  uint32_t            bytes = *((uint32_t*)buf);
  SyncLeaderTransfer* pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  syncLeaderTransferDeserialize(buf, len, pMsg);
  ASSERT(len == pMsg->bytes);
  return pMsg;
}

void syncLeaderTransfer2RpcMsg(const SyncLeaderTransfer* pMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pMsg->msgType;
  pRpcMsg->contLen = pMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  syncLeaderTransferSerialize(pMsg, pRpcMsg->pCont, pRpcMsg->contLen);
}

void syncLeaderTransferFromRpcMsg(const SRpcMsg* pRpcMsg, SyncLeaderTransfer* pMsg) {
  syncLeaderTransferDeserialize(pRpcMsg->pCont, pRpcMsg->contLen, pMsg);
}

SyncLeaderTransfer* syncLeaderTransferFromRpcMsg2(const SRpcMsg* pRpcMsg) {
  SyncLeaderTransfer* pMsg = syncLeaderTransferDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
  ASSERT(pMsg != NULL);
  return pMsg;
}

cJSON* syncLeaderTransfer2Json(const SyncLeaderTransfer* pMsg) {
  char   u64buf[128];
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);

    /*
        cJSON* pSrcId = cJSON_CreateObject();
        snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->srcId.addr);
        cJSON_AddStringToObject(pSrcId, "addr", u64buf);
        {
          uint64_t u64 = pMsg->srcId.addr;
          cJSON*   pTmp = pSrcId;
          char     host[128];
          uint16_t port;
          syncUtilU642Addr(u64, host, sizeof(host), &port);
          cJSON_AddStringToObject(pTmp, "addr_host", host);
          cJSON_AddNumberToObject(pTmp, "addr_port", port);
        }
        cJSON_AddNumberToObject(pSrcId, "vgId", pMsg->srcId.vgId);
        cJSON_AddItemToObject(pRoot, "srcId", pSrcId);

        cJSON* pDestId = cJSON_CreateObject();
        snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->destId.addr);
        cJSON_AddStringToObject(pDestId, "addr", u64buf);
        {
          uint64_t u64 = pMsg->destId.addr;
          cJSON*   pTmp = pDestId;
          char     host[128];
          uint16_t port;
          syncUtilU642Addr(u64, host, sizeof(host), &port);
          cJSON_AddStringToObject(pTmp, "addr_host", host);
          cJSON_AddNumberToObject(pTmp, "addr_port", port);
        }
        cJSON_AddNumberToObject(pDestId, "vgId", pMsg->destId.vgId);
        cJSON_AddItemToObject(pRoot, "destId", pDestId);
    */

    cJSON* pNewerId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->newLeaderId.addr);
    cJSON_AddStringToObject(pNewerId, "addr", u64buf);
    {
      uint64_t u64 = pMsg->newLeaderId.addr;
      cJSON*   pTmp = pNewerId;
      char     host[128];
      uint16_t port;
      syncUtilU642Addr(u64, host, sizeof(host), &port);
      cJSON_AddStringToObject(pTmp, "addr_host", host);
      cJSON_AddNumberToObject(pTmp, "addr_port", port);
    }
    cJSON_AddNumberToObject(pNewerId, "vgId", pMsg->newLeaderId.vgId);
    cJSON_AddItemToObject(pRoot, "newLeaderId", pNewerId);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncLeaderTransfer", pRoot);
  return pJson;
}

char* syncLeaderTransfer2Str(const SyncLeaderTransfer* pMsg) {
  cJSON* pJson = syncLeaderTransfer2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

const char* syncLocalCmdGetStr(int32_t cmd) {
  if (cmd == SYNC_LOCAL_CMD_STEP_DOWN) {
    return "step-down";
  } else if (cmd == SYNC_LOCAL_CMD_FOLLOWER_CMT) {
    return "follower-commit";
  }

  return "unknown-local-cmd";
}

SyncLocalCmd* syncLocalCmdBuild(int32_t vgId) {
  uint32_t      bytes = sizeof(SyncLocalCmd);
  SyncLocalCmd* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->vgId = vgId;
  pMsg->msgType = TDMT_SYNC_LOCAL_CMD;
  return pMsg;
}

void syncLocalCmdDestroy(SyncLocalCmd* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncLocalCmdSerialize(const SyncLocalCmd* pMsg, char* buf, uint32_t bufLen) {
  ASSERT(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncLocalCmdDeserialize(const char* buf, uint32_t len, SyncLocalCmd* pMsg) {
  memcpy(pMsg, buf, len);
  ASSERT(len == pMsg->bytes);
}

char* syncLocalCmdSerialize2(const SyncLocalCmd* pMsg, uint32_t* len) {
  char* buf = taosMemoryMalloc(pMsg->bytes);
  ASSERT(buf != NULL);
  syncLocalCmdSerialize(pMsg, buf, pMsg->bytes);
  if (len != NULL) {
    *len = pMsg->bytes;
  }
  return buf;
}

SyncLocalCmd* syncLocalCmdDeserialize2(const char* buf, uint32_t len) {
  uint32_t      bytes = *((uint32_t*)buf);
  SyncLocalCmd* pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  syncLocalCmdDeserialize(buf, len, pMsg);
  ASSERT(len == pMsg->bytes);
  return pMsg;
}

void syncLocalCmd2RpcMsg(const SyncLocalCmd* pMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pMsg->msgType;
  pRpcMsg->contLen = pMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  syncLocalCmdSerialize(pMsg, pRpcMsg->pCont, pRpcMsg->contLen);
}

void syncLocalCmdFromRpcMsg(const SRpcMsg* pRpcMsg, SyncLocalCmd* pMsg) {
  syncLocalCmdDeserialize(pRpcMsg->pCont, pRpcMsg->contLen, pMsg);
}

SyncLocalCmd* syncLocalCmdFromRpcMsg2(const SRpcMsg* pRpcMsg) {
  SyncLocalCmd* pMsg = syncLocalCmdDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
  ASSERT(pMsg != NULL);
  return pMsg;
}

cJSON* syncLocalCmd2Json(const SyncLocalCmd* pMsg) {
  char   u64buf[128];
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);

    cJSON* pSrcId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->srcId.addr);
    cJSON_AddStringToObject(pSrcId, "addr", u64buf);
    {
      uint64_t u64 = pMsg->srcId.addr;
      cJSON*   pTmp = pSrcId;
      char     host[128];
      uint16_t port;
      syncUtilU642Addr(u64, host, sizeof(host), &port);
      cJSON_AddStringToObject(pTmp, "addr_host", host);
      cJSON_AddNumberToObject(pTmp, "addr_port", port);
    }
    cJSON_AddNumberToObject(pSrcId, "vgId", pMsg->srcId.vgId);
    cJSON_AddItemToObject(pRoot, "srcId", pSrcId);

    cJSON* pDestId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->destId.addr);
    cJSON_AddStringToObject(pDestId, "addr", u64buf);
    {
      uint64_t u64 = pMsg->destId.addr;
      cJSON*   pTmp = pDestId;
      char     host[128];
      uint16_t port;
      syncUtilU642Addr(u64, host, sizeof(host), &port);
      cJSON_AddStringToObject(pTmp, "addr_host", host);
      cJSON_AddNumberToObject(pTmp, "addr_port", port);
    }
    cJSON_AddNumberToObject(pDestId, "vgId", pMsg->destId.vgId);
    cJSON_AddItemToObject(pRoot, "destId", pDestId);

    cJSON_AddNumberToObject(pRoot, "cmd", pMsg->cmd);

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->sdNewTerm);
    cJSON_AddStringToObject(pRoot, "sd-new-term", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pMsg->fcIndex);
    cJSON_AddStringToObject(pRoot, "fc-index", u64buf);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncLocalCmd2Json", pRoot);
  return pJson;
}

char* syncLocalCmd2Str(const SyncLocalCmd* pMsg) {
  cJSON* pJson = syncLocalCmd2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug ----------------------
void syncLocalCmdPrint(const SyncLocalCmd* pMsg) {
  char* serialized = syncLocalCmd2Str(pMsg);
  printf("syncLocalCmdPrint | len:%d | %s \n", (int32_t)strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncLocalCmdPrint2(char* s, const SyncLocalCmd* pMsg) {
  char* serialized = syncLocalCmd2Str(pMsg);
  printf("syncLocalCmdPrint2 | len:%d | %s | %s \n", (int32_t)strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncLocalCmdLog(const SyncLocalCmd* pMsg) {
  char* serialized = syncLocalCmd2Str(pMsg);
  sTrace("syncLocalCmdLog | len:%d | %s", (int32_t)strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncLocalCmdLog2(char* s, const SyncLocalCmd* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncLocalCmd2Str(pMsg);
    sTrace("syncLocalCmdLog2 | len:%d | %s | %s", (int32_t)strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}
