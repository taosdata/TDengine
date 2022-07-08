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

#include "syncMessage.h"
#include "syncRaftCfg.h"
#include "syncRaftEntry.h"
#include "syncUtil.h"
#include "tcoding.h"

// ---------------------------------------------
cJSON* syncRpcMsg2Json(SRpcMsg* pRpcMsg) {
  cJSON* pRoot;

  // in compiler optimization, switch case = if else constants
  if (pRpcMsg->msgType == TDMT_SYNC_TIMEOUT) {
    SyncTimeout* pSyncMsg = syncTimeoutDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
    pRoot = syncTimeout2Json(pSyncMsg);
    syncTimeoutDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_SYNC_PING) {
    SyncPing* pSyncMsg = syncPingDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
    pRoot = syncPing2Json(pSyncMsg);
    syncPingDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_SYNC_PING_REPLY) {
    SyncPingReply* pSyncMsg = syncPingReplyDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
    pRoot = syncPingReply2Json(pSyncMsg);
    syncPingReplyDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_SYNC_CLIENT_REQUEST) {
    SyncClientRequest* pSyncMsg = syncClientRequestDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
    pRoot = syncClientRequest2Json(pSyncMsg);
    syncClientRequestDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_SYNC_CLIENT_REQUEST_REPLY) {
    pRoot = syncRpcUnknownMsg2Json();

  } else if (pRpcMsg->msgType == TDMT_SYNC_REQUEST_VOTE) {
    SyncRequestVote* pSyncMsg = syncRequestVoteDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
    pRoot = syncRequestVote2Json(pSyncMsg);
    syncRequestVoteDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_SYNC_REQUEST_VOTE_REPLY) {
    SyncRequestVoteReply* pSyncMsg = syncRequestVoteReplyDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
    pRoot = syncRequestVoteReply2Json(pSyncMsg);
    syncRequestVoteReplyDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_SYNC_APPEND_ENTRIES) {
    SyncAppendEntries* pSyncMsg = syncAppendEntriesDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
    pRoot = syncAppendEntries2Json(pSyncMsg);
    syncAppendEntriesDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_SYNC_APPEND_ENTRIES_REPLY) {
    SyncAppendEntriesReply* pSyncMsg = syncAppendEntriesReplyDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
    pRoot = syncAppendEntriesReply2Json(pSyncMsg);
    syncAppendEntriesReplyDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_SYNC_SNAPSHOT_SEND) {
    SyncSnapshotSend* pSyncMsg = syncSnapshotSendDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
    pRoot = syncSnapshotSend2Json(pSyncMsg);
    syncSnapshotSendDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_SYNC_SNAPSHOT_RSP) {
    SyncSnapshotRsp* pSyncMsg = syncSnapshotRspDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
    pRoot = syncSnapshotRsp2Json(pSyncMsg);
    syncSnapshotRspDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_SYNC_LEADER_TRANSFER) {
    SyncLeaderTransfer* pSyncMsg = syncLeaderTransferDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
    pRoot = syncLeaderTransfer2Json(pSyncMsg);
    syncLeaderTransferDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_SYNC_COMMON_RESPONSE) {
    pRoot = cJSON_CreateObject();
    char* s;
    s = syncUtilprintBin((char*)(pRpcMsg->pCont), pRpcMsg->contLen);
    cJSON_AddStringToObject(pRoot, "pCont", s);
    taosMemoryFree(s);
    s = syncUtilprintBin2((char*)(pRpcMsg->pCont), pRpcMsg->contLen);
    cJSON_AddStringToObject(pRoot, "pCont2", s);
    taosMemoryFree(s);

  } else {
    pRoot = cJSON_CreateObject();
    char* s;
    s = syncUtilprintBin((char*)(pRpcMsg->pCont), pRpcMsg->contLen);
    cJSON_AddStringToObject(pRoot, "pCont", s);
    taosMemoryFree(s);
    s = syncUtilprintBin2((char*)(pRpcMsg->pCont), pRpcMsg->contLen);
    cJSON_AddStringToObject(pRoot, "pCont2", s);
    taosMemoryFree(s);
  }

  cJSON_AddNumberToObject(pRoot, "msgType", pRpcMsg->msgType);
  cJSON_AddNumberToObject(pRoot, "contLen", pRpcMsg->contLen);
  cJSON_AddNumberToObject(pRoot, "code", pRpcMsg->code);
  // cJSON_AddNumberToObject(pRoot, "persist", pRpcMsg->persist);

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "RpcMsg", pRoot);
  return pJson;
}

cJSON* syncRpcUnknownMsg2Json() {
  cJSON* pRoot = cJSON_CreateObject();
  cJSON_AddNumberToObject(pRoot, "msgType", TDMT_SYNC_UNKNOWN);
  cJSON_AddStringToObject(pRoot, "data", "unknown message");

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncUnknown", pRoot);
  return pJson;
}

char* syncRpcMsg2Str(SRpcMsg* pRpcMsg) {
  cJSON* pJson = syncRpcMsg2Json(pRpcMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug ----------------------
void syncRpcMsgPrint(SRpcMsg* pMsg) {
  char* serialized = syncRpcMsg2Str(pMsg);
  printf("syncRpcMsgPrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncRpcMsgPrint2(char* s, SRpcMsg* pMsg) {
  char* serialized = syncRpcMsg2Str(pMsg);
  printf("syncRpcMsgPrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncRpcMsgLog(SRpcMsg* pMsg) {
  char* serialized = syncRpcMsg2Str(pMsg);
  sTrace("syncRpcMsgLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncRpcMsgLog2(char* s, SRpcMsg* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncRpcMsg2Str(pMsg);
    sTrace("syncRpcMsgLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// ---- message process SyncTimeout----
SyncTimeout* syncTimeoutBuild() {
  uint32_t     bytes = sizeof(SyncTimeout);
  SyncTimeout* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->msgType = TDMT_SYNC_TIMEOUT;
  return pMsg;
}

SyncTimeout* syncTimeoutBuild2(ESyncTimeoutType timeoutType, uint64_t logicClock, int32_t timerMS, int32_t vgId,
                               void* data) {
  SyncTimeout* pMsg = syncTimeoutBuild();
  pMsg->vgId = vgId;
  pMsg->timeoutType = timeoutType;
  pMsg->logicClock = logicClock;
  pMsg->timerMS = timerMS;
  pMsg->data = data;
  return pMsg;
}

void syncTimeoutDestroy(SyncTimeout* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncTimeoutSerialize(const SyncTimeout* pMsg, char* buf, uint32_t bufLen) {
  ASSERT(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncTimeoutDeserialize(const char* buf, uint32_t len, SyncTimeout* pMsg) {
  memcpy(pMsg, buf, len);
  ASSERT(len == pMsg->bytes);
}

char* syncTimeoutSerialize2(const SyncTimeout* pMsg, uint32_t* len) {
  char* buf = taosMemoryMalloc(pMsg->bytes);
  ASSERT(buf != NULL);
  syncTimeoutSerialize(pMsg, buf, pMsg->bytes);
  if (len != NULL) {
    *len = pMsg->bytes;
  }
  return buf;
}

SyncTimeout* syncTimeoutDeserialize2(const char* buf, uint32_t len) {
  uint32_t     bytes = *((uint32_t*)buf);
  SyncTimeout* pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  syncTimeoutDeserialize(buf, len, pMsg);
  ASSERT(len == pMsg->bytes);
  return pMsg;
}

void syncTimeout2RpcMsg(const SyncTimeout* pMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pMsg->msgType;
  pRpcMsg->contLen = pMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  syncTimeoutSerialize(pMsg, pRpcMsg->pCont, pRpcMsg->contLen);
}

void syncTimeoutFromRpcMsg(const SRpcMsg* pRpcMsg, SyncTimeout* pMsg) {
  syncTimeoutDeserialize(pRpcMsg->pCont, pRpcMsg->contLen, pMsg);
}

SyncTimeout* syncTimeoutFromRpcMsg2(const SRpcMsg* pRpcMsg) {
  SyncTimeout* pMsg = syncTimeoutDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
  ASSERT(pMsg != NULL);
  return pMsg;
}

cJSON* syncTimeout2Json(const SyncTimeout* pMsg) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);
    cJSON_AddNumberToObject(pRoot, "timeoutType", pMsg->timeoutType);
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->logicClock);
    cJSON_AddStringToObject(pRoot, "logicClock", u64buf);
    cJSON_AddNumberToObject(pRoot, "timerMS", pMsg->timerMS);
    snprintf(u64buf, sizeof(u64buf), "%p", pMsg->data);
    cJSON_AddStringToObject(pRoot, "data", u64buf);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncTimeout", pRoot);
  return pJson;
}

char* syncTimeout2Str(const SyncTimeout* pMsg) {
  cJSON* pJson = syncTimeout2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug ----------------------
void syncTimeoutPrint(const SyncTimeout* pMsg) {
  char* serialized = syncTimeout2Str(pMsg);
  printf("syncTimeoutPrint | len:%zu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncTimeoutPrint2(char* s, const SyncTimeout* pMsg) {
  char* serialized = syncTimeout2Str(pMsg);
  printf("syncTimeoutPrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncTimeoutLog(const SyncTimeout* pMsg) {
  char* serialized = syncTimeout2Str(pMsg);
  sTrace("syncTimeoutLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncTimeoutLog2(char* s, const SyncTimeout* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncTimeout2Str(pMsg);
    sTrace("syncTimeoutLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// ---- message process SyncPing----
SyncPing* syncPingBuild(uint32_t dataLen) {
  uint32_t  bytes = sizeof(SyncPing) + dataLen;
  SyncPing* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->msgType = TDMT_SYNC_PING;
  pMsg->dataLen = dataLen;
  return pMsg;
}

SyncPing* syncPingBuild2(const SRaftId* srcId, const SRaftId* destId, int32_t vgId, const char* str) {
  uint32_t  dataLen = strlen(str) + 1;
  SyncPing* pMsg = syncPingBuild(dataLen);
  pMsg->vgId = vgId;
  pMsg->srcId = *srcId;
  pMsg->destId = *destId;
  snprintf(pMsg->data, pMsg->dataLen, "%s", str);
  return pMsg;
}

SyncPing* syncPingBuild3(const SRaftId* srcId, const SRaftId* destId, int32_t vgId) {
  SyncPing* pMsg = syncPingBuild2(srcId, destId, vgId, "ping");
  return pMsg;
}

void syncPingDestroy(SyncPing* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncPingSerialize(const SyncPing* pMsg, char* buf, uint32_t bufLen) {
  ASSERT(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncPingDeserialize(const char* buf, uint32_t len, SyncPing* pMsg) {
  memcpy(pMsg, buf, len);
  ASSERT(len == pMsg->bytes);
  ASSERT(pMsg->bytes == sizeof(SyncPing) + pMsg->dataLen);
}

char* syncPingSerialize2(const SyncPing* pMsg, uint32_t* len) {
  char* buf = taosMemoryMalloc(pMsg->bytes);
  ASSERT(buf != NULL);
  syncPingSerialize(pMsg, buf, pMsg->bytes);
  if (len != NULL) {
    *len = pMsg->bytes;
  }
  return buf;
}

SyncPing* syncPingDeserialize2(const char* buf, uint32_t len) {
  uint32_t  bytes = *((uint32_t*)buf);
  SyncPing* pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  syncPingDeserialize(buf, len, pMsg);
  ASSERT(len == pMsg->bytes);
  return pMsg;
}

int32_t syncPingSerialize3(const SyncPing* pMsg, char* buf, int32_t bufLen) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) {
    return -1;
  }

  if (tEncodeU32(&encoder, pMsg->bytes) < 0) {
    return -1;
  }
  if (tEncodeI32(&encoder, pMsg->vgId) < 0) {
    return -1;
  }
  if (tEncodeU32(&encoder, pMsg->msgType) < 0) {
    return -1;
  }
  if (tEncodeU64(&encoder, pMsg->srcId.addr) < 0) {
    return -1;
  }
  if (tEncodeI32(&encoder, pMsg->srcId.vgId) < 0) {
    return -1;
  }
  if (tEncodeU64(&encoder, pMsg->destId.addr) < 0) {
    return -1;
  }
  if (tEncodeI32(&encoder, pMsg->destId.vgId) < 0) {
    return -1;
  }
  if (tEncodeU32(&encoder, pMsg->dataLen) < 0) {
    return -1;
  }
  if (tEncodeBinary(&encoder, pMsg->data, pMsg->dataLen)) {
    return -1;
  }

  tEndEncode(&encoder);
  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

SyncPing* syncPingDeserialize3(void* buf, int32_t bufLen) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);
  if (tStartDecode(&decoder) < 0) {
    return NULL;
  }

  SyncPing* pMsg = NULL;
  uint32_t  bytes;
  if (tDecodeU32(&decoder, &bytes) < 0) {
    return NULL;
  }

  pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  pMsg->bytes = bytes;

  if (tDecodeI32(&decoder, &pMsg->vgId) < 0) {
    return NULL;
  }
  if (tDecodeU32(&decoder, &pMsg->msgType) < 0) {
    return NULL;
  }
  if (tDecodeU64(&decoder, &pMsg->srcId.addr) < 0) {
    return NULL;
  }
  if (tDecodeI32(&decoder, &pMsg->srcId.vgId) < 0) {
    return NULL;
  }
  if (tDecodeU64(&decoder, &pMsg->destId.addr) < 0) {
    return NULL;
  }
  if (tDecodeI32(&decoder, &pMsg->destId.vgId) < 0) {
    return NULL;
  }
  if (tDecodeU32(&decoder, &pMsg->dataLen) < 0) {
    return NULL;
  }
  uint32_t len;
  char*    data = NULL;
  if (tDecodeBinary(&decoder, (uint8_t**)(&data), &len) < 0) {
    return NULL;
  }
  ASSERT(len = pMsg->dataLen);
  memcpy(pMsg->data, data, len);

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return pMsg;
}

void syncPing2RpcMsg(const SyncPing* pMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pMsg->msgType;
  pRpcMsg->contLen = pMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  syncPingSerialize(pMsg, pRpcMsg->pCont, pRpcMsg->contLen);
}

void syncPingFromRpcMsg(const SRpcMsg* pRpcMsg, SyncPing* pMsg) {
  syncPingDeserialize(pRpcMsg->pCont, pRpcMsg->contLen, pMsg);
}

SyncPing* syncPingFromRpcMsg2(const SRpcMsg* pRpcMsg) {
  SyncPing* pMsg = syncPingDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
  ASSERT(pMsg != NULL);
  return pMsg;
}

cJSON* syncPing2Json(const SyncPing* pMsg) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);

    cJSON* pSrcId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->srcId.addr);
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
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->destId.addr);
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

    cJSON_AddNumberToObject(pRoot, "dataLen", pMsg->dataLen);
    char* s;
    s = syncUtilprintBin((char*)(pMsg->data), pMsg->dataLen);
    cJSON_AddStringToObject(pRoot, "data", s);
    taosMemoryFree(s);
    s = syncUtilprintBin2((char*)(pMsg->data), pMsg->dataLen);
    cJSON_AddStringToObject(pRoot, "data2", s);
    taosMemoryFree(s);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncPing", pRoot);
  return pJson;
}

char* syncPing2Str(const SyncPing* pMsg) {
  cJSON* pJson = syncPing2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug ----------------------
void syncPingPrint(const SyncPing* pMsg) {
  char* serialized = syncPing2Str(pMsg);
  printf("syncPingPrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncPingPrint2(char* s, const SyncPing* pMsg) {
  char* serialized = syncPing2Str(pMsg);
  printf("syncPingPrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncPingLog(const SyncPing* pMsg) {
  char* serialized = syncPing2Str(pMsg);
  sTrace("syncPingLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncPingLog2(char* s, const SyncPing* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncPing2Str(pMsg);
    sTrace("syncPingLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// ---- message process SyncPingReply----
SyncPingReply* syncPingReplyBuild(uint32_t dataLen) {
  uint32_t       bytes = sizeof(SyncPingReply) + dataLen;
  SyncPingReply* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->msgType = TDMT_SYNC_PING_REPLY;
  pMsg->dataLen = dataLen;
  return pMsg;
}

SyncPingReply* syncPingReplyBuild2(const SRaftId* srcId, const SRaftId* destId, int32_t vgId, const char* str) {
  uint32_t       dataLen = strlen(str) + 1;
  SyncPingReply* pMsg = syncPingReplyBuild(dataLen);
  pMsg->vgId = vgId;
  pMsg->srcId = *srcId;
  pMsg->destId = *destId;
  snprintf(pMsg->data, pMsg->dataLen, "%s", str);
  return pMsg;
}

SyncPingReply* syncPingReplyBuild3(const SRaftId* srcId, const SRaftId* destId, int32_t vgId) {
  SyncPingReply* pMsg = syncPingReplyBuild2(srcId, destId, vgId, "pang");
  return pMsg;
}

void syncPingReplyDestroy(SyncPingReply* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncPingReplySerialize(const SyncPingReply* pMsg, char* buf, uint32_t bufLen) {
  ASSERT(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncPingReplyDeserialize(const char* buf, uint32_t len, SyncPingReply* pMsg) {
  memcpy(pMsg, buf, len);
  ASSERT(len == pMsg->bytes);
  ASSERT(pMsg->bytes == sizeof(SyncPingReply) + pMsg->dataLen);
}

char* syncPingReplySerialize2(const SyncPingReply* pMsg, uint32_t* len) {
  char* buf = taosMemoryMalloc(pMsg->bytes);
  ASSERT(buf != NULL);
  syncPingReplySerialize(pMsg, buf, pMsg->bytes);
  if (len != NULL) {
    *len = pMsg->bytes;
  }
  return buf;
}

SyncPingReply* syncPingReplyDeserialize2(const char* buf, uint32_t len) {
  uint32_t       bytes = *((uint32_t*)buf);
  SyncPingReply* pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  syncPingReplyDeserialize(buf, len, pMsg);
  ASSERT(len == pMsg->bytes);
  return pMsg;
}

int32_t syncPingReplySerialize3(const SyncPingReply* pMsg, char* buf, int32_t bufLen) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) {
    return -1;
  }

  if (tEncodeU32(&encoder, pMsg->bytes) < 0) {
    return -1;
  }
  if (tEncodeI32(&encoder, pMsg->vgId) < 0) {
    return -1;
  }
  if (tEncodeU32(&encoder, pMsg->msgType) < 0) {
    return -1;
  }
  if (tEncodeU64(&encoder, pMsg->srcId.addr) < 0) {
    return -1;
  }
  if (tEncodeI32(&encoder, pMsg->srcId.vgId) < 0) {
    return -1;
  }
  if (tEncodeU64(&encoder, pMsg->destId.addr) < 0) {
    return -1;
  }
  if (tEncodeI32(&encoder, pMsg->destId.vgId) < 0) {
    return -1;
  }
  if (tEncodeU32(&encoder, pMsg->dataLen) < 0) {
    return -1;
  }
  if (tEncodeBinary(&encoder, pMsg->data, pMsg->dataLen)) {
    return -1;
  }

  tEndEncode(&encoder);
  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

SyncPingReply* syncPingReplyDeserialize3(void* buf, int32_t bufLen) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);
  if (tStartDecode(&decoder) < 0) {
    return NULL;
  }

  SyncPingReply* pMsg = NULL;
  uint32_t       bytes;
  if (tDecodeU32(&decoder, &bytes) < 0) {
    return NULL;
  }

  pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  pMsg->bytes = bytes;

  if (tDecodeI32(&decoder, &pMsg->vgId) < 0) {
    return NULL;
  }
  if (tDecodeU32(&decoder, &pMsg->msgType) < 0) {
    return NULL;
  }
  if (tDecodeU64(&decoder, &pMsg->srcId.addr) < 0) {
    return NULL;
  }
  if (tDecodeI32(&decoder, &pMsg->srcId.vgId) < 0) {
    return NULL;
  }
  if (tDecodeU64(&decoder, &pMsg->destId.addr) < 0) {
    return NULL;
  }
  if (tDecodeI32(&decoder, &pMsg->destId.vgId) < 0) {
    return NULL;
  }
  if (tDecodeU32(&decoder, &pMsg->dataLen) < 0) {
    return NULL;
  }
  uint32_t len;
  char*    data = NULL;
  if (tDecodeBinary(&decoder, (uint8_t**)(&data), &len) < 0) {
    return NULL;
  }
  ASSERT(len = pMsg->dataLen);
  memcpy(pMsg->data, data, len);

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return pMsg;
}

void syncPingReply2RpcMsg(const SyncPingReply* pMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pMsg->msgType;
  pRpcMsg->contLen = pMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  syncPingReplySerialize(pMsg, pRpcMsg->pCont, pRpcMsg->contLen);
}

void syncPingReplyFromRpcMsg(const SRpcMsg* pRpcMsg, SyncPingReply* pMsg) {
  syncPingReplyDeserialize(pRpcMsg->pCont, pRpcMsg->contLen, pMsg);
}

SyncPingReply* syncPingReplyFromRpcMsg2(const SRpcMsg* pRpcMsg) {
  SyncPingReply* pMsg = syncPingReplyDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
  ASSERT(pMsg != NULL);
  return pMsg;
}

cJSON* syncPingReply2Json(const SyncPingReply* pMsg) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);

    cJSON* pSrcId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->srcId.addr);
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
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->destId.addr);
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

    cJSON_AddNumberToObject(pRoot, "dataLen", pMsg->dataLen);
    char* s;
    s = syncUtilprintBin((char*)(pMsg->data), pMsg->dataLen);
    cJSON_AddStringToObject(pRoot, "data", s);
    taosMemoryFree(s);
    s = syncUtilprintBin2((char*)(pMsg->data), pMsg->dataLen);
    cJSON_AddStringToObject(pRoot, "data2", s);
    taosMemoryFree(s);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncPingReply", pRoot);
  return pJson;
}

char* syncPingReply2Str(const SyncPingReply* pMsg) {
  cJSON* pJson = syncPingReply2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug ----------------------
void syncPingReplyPrint(const SyncPingReply* pMsg) {
  char* serialized = syncPingReply2Str(pMsg);
  printf("syncPingReplyPrint | len:%zu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncPingReplyPrint2(char* s, const SyncPingReply* pMsg) {
  char* serialized = syncPingReply2Str(pMsg);
  printf("syncPingReplyPrint2 | len:%zu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncPingReplyLog(const SyncPingReply* pMsg) {
  char* serialized = syncPingReply2Str(pMsg);
  sTrace("syncPingReplyLog | len:%zu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncPingReplyLog2(char* s, const SyncPingReply* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncPingReply2Str(pMsg);
    sTrace("syncPingReplyLog2 | len:%zu | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// ---- message process SyncClientRequest----
SyncClientRequest* syncClientRequestBuild(uint32_t dataLen) {
  uint32_t           bytes = sizeof(SyncClientRequest) + dataLen;
  SyncClientRequest* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->msgType = TDMT_SYNC_CLIENT_REQUEST;
  pMsg->seqNum = 0;
  pMsg->isWeak = false;
  pMsg->dataLen = dataLen;
  return pMsg;
}

// step 1. original SRpcMsg => SyncClientRequest, add seqNum, isWeak
SyncClientRequest* syncClientRequestBuild2(const SRpcMsg* pOriginalRpcMsg, uint64_t seqNum, bool isWeak, int32_t vgId) {
  SyncClientRequest* pMsg = syncClientRequestBuild(pOriginalRpcMsg->contLen);
  pMsg->vgId = vgId;
  pMsg->originalRpcType = pOriginalRpcMsg->msgType;
  pMsg->seqNum = seqNum;
  pMsg->isWeak = isWeak;
  memcpy(pMsg->data, pOriginalRpcMsg->pCont, pOriginalRpcMsg->contLen);
  return pMsg;
}

void syncClientRequestDestroy(SyncClientRequest* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncClientRequestSerialize(const SyncClientRequest* pMsg, char* buf, uint32_t bufLen) {
  ASSERT(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncClientRequestDeserialize(const char* buf, uint32_t len, SyncClientRequest* pMsg) {
  memcpy(pMsg, buf, len);
  ASSERT(len == pMsg->bytes);
}

char* syncClientRequestSerialize2(const SyncClientRequest* pMsg, uint32_t* len) {
  char* buf = taosMemoryMalloc(pMsg->bytes);
  ASSERT(buf != NULL);
  syncClientRequestSerialize(pMsg, buf, pMsg->bytes);
  if (len != NULL) {
    *len = pMsg->bytes;
  }
  return buf;
}

SyncClientRequest* syncClientRequestDeserialize2(const char* buf, uint32_t len) {
  uint32_t           bytes = *((uint32_t*)buf);
  SyncClientRequest* pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  syncClientRequestDeserialize(buf, len, pMsg);
  ASSERT(len == pMsg->bytes);
  return pMsg;
}

// step 2. SyncClientRequest => RpcMsg, to queue
void syncClientRequest2RpcMsg(const SyncClientRequest* pMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pMsg->msgType;
  pRpcMsg->contLen = pMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  syncClientRequestSerialize(pMsg, pRpcMsg->pCont, pRpcMsg->contLen);
}

void syncClientRequestFromRpcMsg(const SRpcMsg* pRpcMsg, SyncClientRequest* pMsg) {
  syncClientRequestDeserialize(pRpcMsg->pCont, pRpcMsg->contLen, pMsg);
}

// step 3. RpcMsg => SyncClientRequest, from queue
SyncClientRequest* syncClientRequestFromRpcMsg2(const SRpcMsg* pRpcMsg) {
  SyncClientRequest* pMsg = syncClientRequestDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
  ASSERT(pMsg != NULL);
  return pMsg;
}

cJSON* syncClientRequest2Json(const SyncClientRequest* pMsg) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);
    cJSON_AddNumberToObject(pRoot, "originalRpcType", pMsg->originalRpcType);
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->seqNum);
    cJSON_AddStringToObject(pRoot, "seqNum", u64buf);
    cJSON_AddNumberToObject(pRoot, "isWeak", pMsg->isWeak);
    cJSON_AddNumberToObject(pRoot, "dataLen", pMsg->dataLen);

    char* s;
    s = syncUtilprintBin((char*)(pMsg->data), pMsg->dataLen);
    cJSON_AddStringToObject(pRoot, "data", s);
    taosMemoryFree(s);
    s = syncUtilprintBin2((char*)(pMsg->data), pMsg->dataLen);
    cJSON_AddStringToObject(pRoot, "data2", s);
    taosMemoryFree(s);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncClientRequest", pRoot);
  return pJson;
}

char* syncClientRequest2Str(const SyncClientRequest* pMsg) {
  cJSON* pJson = syncClientRequest2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug ----------------------
void syncClientRequestPrint(const SyncClientRequest* pMsg) {
  char* serialized = syncClientRequest2Str(pMsg);
  printf("syncClientRequestPrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncClientRequestPrint2(char* s, const SyncClientRequest* pMsg) {
  char* serialized = syncClientRequest2Str(pMsg);
  printf("syncClientRequestPrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncClientRequestLog(const SyncClientRequest* pMsg) {
  char* serialized = syncClientRequest2Str(pMsg);
  sTrace("syncClientRequestLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncClientRequestLog2(char* s, const SyncClientRequest* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncClientRequest2Str(pMsg);
    sTrace("syncClientRequestLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// ---- message process SyncClientRequestBatch----

// block1:
// block2: SRaftMeta array
// block3: rpc msg array (with pCont)

SyncClientRequestBatch* syncClientRequestBatchBuild(SRpcMsg* rpcMsgArr, SRaftMeta* raftArr, int32_t arrSize,
                                                    int32_t vgId) {
  ASSERT(rpcMsgArr != NULL);
  ASSERT(arrSize > 0);

  int32_t dataLen = 0;
  int32_t raftMetaArrayLen = sizeof(SRaftMeta) * arrSize;
  int32_t rpcArrayLen = sizeof(SRpcMsg) * arrSize;
  dataLen += (raftMetaArrayLen + rpcArrayLen);

  uint32_t                bytes = sizeof(SyncClientRequestBatch) + dataLen;
  SyncClientRequestBatch* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->vgId = vgId;
  pMsg->msgType = TDMT_SYNC_CLIENT_REQUEST_BATCH;
  pMsg->dataCount = arrSize;
  pMsg->dataLen = dataLen;

  SRaftMeta* raftMetaArr = (SRaftMeta*)(pMsg->data);
  SRpcMsg*   msgArr = (SRpcMsg*)((char*)(pMsg->data) + raftMetaArrayLen);

  for (int i = 0; i < arrSize; ++i) {
    // init raftMetaArr
    raftMetaArr[i].isWeak = raftArr[i].isWeak;
    raftMetaArr[i].seqNum = raftArr[i].seqNum;

    // init msgArr
    msgArr[i] = rpcMsgArr[i];
  }

  return pMsg;
}

void syncClientRequestBatch2RpcMsg(const SyncClientRequestBatch* pSyncMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pSyncMsg->msgType;
  pRpcMsg->contLen = pSyncMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  memcpy(pRpcMsg->pCont, pSyncMsg, pRpcMsg->contLen);
}

void syncClientRequestBatchDestroy(SyncClientRequestBatch* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncClientRequestBatchDestroyDeep(SyncClientRequestBatch* pMsg) {
  if (pMsg != NULL) {
    int32_t  arrSize = pMsg->dataCount;
    int32_t  raftMetaArrayLen = sizeof(SRaftMeta) * arrSize;
    SRpcMsg* msgArr = (SRpcMsg*)((char*)(pMsg->data) + raftMetaArrayLen);
    for (int i = 0; i < arrSize; ++i) {
      if (msgArr[i].pCont != NULL) {
        rpcFreeCont(msgArr[i].pCont);
      }
    }

    taosMemoryFree(pMsg);
  }
}

SRaftMeta* syncClientRequestBatchMetaArr(const SyncClientRequestBatch* pSyncMsg) {
  SRaftMeta* raftMetaArr = (SRaftMeta*)(pSyncMsg->data);
  return raftMetaArr;
}

SRpcMsg* syncClientRequestBatchRpcMsgArr(const SyncClientRequestBatch* pSyncMsg) {
  int32_t  arrSize = pSyncMsg->dataCount;
  int32_t  raftMetaArrayLen = sizeof(SRaftMeta) * arrSize;
  SRpcMsg* msgArr = (SRpcMsg*)((char*)(pSyncMsg->data) + raftMetaArrayLen);
  return msgArr;
}

SyncClientRequestBatch* syncClientRequestBatchFromRpcMsg(const SRpcMsg* pRpcMsg) {
  SyncClientRequestBatch* pSyncMsg = taosMemoryMalloc(pRpcMsg->contLen);
  ASSERT(pSyncMsg != NULL);
  memcpy(pSyncMsg, pRpcMsg->pCont, pRpcMsg->contLen);
  ASSERT(pRpcMsg->contLen == pSyncMsg->bytes);

  return pSyncMsg;
}

cJSON* syncClientRequestBatch2Json(const SyncClientRequestBatch* pMsg) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);
    cJSON_AddNumberToObject(pRoot, "dataLen", pMsg->dataLen);
    cJSON_AddNumberToObject(pRoot, "dataCount", pMsg->dataCount);

    SRaftMeta* metaArr = syncClientRequestBatchMetaArr(pMsg);
    SRpcMsg*   msgArr = syncClientRequestBatchRpcMsgArr(pMsg);

    cJSON* pMetaArr = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "metaArr", pMetaArr);
    for (int i = 0; i < pMsg->dataCount; ++i) {
      cJSON* pMeta = cJSON_CreateObject();
      cJSON_AddNumberToObject(pMeta, "seqNum", metaArr[i].seqNum);
      cJSON_AddNumberToObject(pMeta, "isWeak", metaArr[i].isWeak);
      cJSON_AddItemToArray(pMetaArr, pMeta);
    }

    cJSON* pMsgArr = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "msgArr", pMsgArr);
    for (int i = 0; i < pMsg->dataCount; ++i) {
      cJSON* pRpcMsgJson = syncRpcMsg2Json(&msgArr[i]);
      cJSON_AddItemToArray(pMsgArr, pRpcMsgJson);
    }

    char* s;
    s = syncUtilprintBin((char*)(pMsg->data), pMsg->dataLen);
    cJSON_AddStringToObject(pRoot, "data", s);
    taosMemoryFree(s);
    s = syncUtilprintBin2((char*)(pMsg->data), pMsg->dataLen);
    cJSON_AddStringToObject(pRoot, "data2", s);
    taosMemoryFree(s);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncClientRequestBatch", pRoot);
  return pJson;
}

char* syncClientRequestBatch2Str(const SyncClientRequestBatch* pMsg) {
  cJSON* pJson = syncClientRequestBatch2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug ----------------------
void syncClientRequestBatchPrint(const SyncClientRequestBatch* pMsg) {
  char* serialized = syncClientRequestBatch2Str(pMsg);
  printf("syncClientRequestBatchPrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncClientRequestBatchPrint2(char* s, const SyncClientRequestBatch* pMsg) {
  char* serialized = syncClientRequestBatch2Str(pMsg);
  printf("syncClientRequestBatchPrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncClientRequestBatchLog(const SyncClientRequestBatch* pMsg) {
  char* serialized = syncClientRequestBatch2Str(pMsg);
  sTrace("syncClientRequestBatchLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncClientRequestBatchLog2(char* s, const SyncClientRequestBatch* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncClientRequestBatch2Str(pMsg);
    sTraceLong("syncClientRequestBatchLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// ---- message process SyncRequestVote----
SyncRequestVote* syncRequestVoteBuild(int32_t vgId) {
  uint32_t         bytes = sizeof(SyncRequestVote);
  SyncRequestVote* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->vgId = vgId;
  pMsg->msgType = TDMT_SYNC_REQUEST_VOTE;
  return pMsg;
}

void syncRequestVoteDestroy(SyncRequestVote* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncRequestVoteSerialize(const SyncRequestVote* pMsg, char* buf, uint32_t bufLen) {
  ASSERT(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncRequestVoteDeserialize(const char* buf, uint32_t len, SyncRequestVote* pMsg) {
  memcpy(pMsg, buf, len);
  ASSERT(len == pMsg->bytes);
}

char* syncRequestVoteSerialize2(const SyncRequestVote* pMsg, uint32_t* len) {
  char* buf = taosMemoryMalloc(pMsg->bytes);
  ASSERT(buf != NULL);
  syncRequestVoteSerialize(pMsg, buf, pMsg->bytes);
  if (len != NULL) {
    *len = pMsg->bytes;
  }
  return buf;
}

SyncRequestVote* syncRequestVoteDeserialize2(const char* buf, uint32_t len) {
  uint32_t         bytes = *((uint32_t*)buf);
  SyncRequestVote* pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  syncRequestVoteDeserialize(buf, len, pMsg);
  ASSERT(len == pMsg->bytes);
  return pMsg;
}

void syncRequestVote2RpcMsg(const SyncRequestVote* pMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pMsg->msgType;
  pRpcMsg->contLen = pMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  syncRequestVoteSerialize(pMsg, pRpcMsg->pCont, pRpcMsg->contLen);
}

void syncRequestVoteFromRpcMsg(const SRpcMsg* pRpcMsg, SyncRequestVote* pMsg) {
  syncRequestVoteDeserialize(pRpcMsg->pCont, pRpcMsg->contLen, pMsg);
}

SyncRequestVote* syncRequestVoteFromRpcMsg2(const SRpcMsg* pRpcMsg) {
  SyncRequestVote* pMsg = syncRequestVoteDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
  ASSERT(pMsg != NULL);
  return pMsg;
}

cJSON* syncRequestVote2Json(const SyncRequestVote* pMsg) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);

    cJSON* pSrcId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->srcId.addr);
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
    cJSON_AddNumberToObject(pDestId, "addr", pMsg->destId.addr);
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

    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%ld", pMsg->lastLogIndex);
    cJSON_AddStringToObject(pRoot, "lastLogIndex", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->lastLogTerm);
    cJSON_AddStringToObject(pRoot, "lastLogTerm", u64buf);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncRequestVote", pRoot);
  return pJson;
}

char* syncRequestVote2Str(const SyncRequestVote* pMsg) {
  cJSON* pJson = syncRequestVote2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug ----------------------
void syncRequestVotePrint(const SyncRequestVote* pMsg) {
  char* serialized = syncRequestVote2Str(pMsg);
  printf("syncRequestVotePrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncRequestVotePrint2(char* s, const SyncRequestVote* pMsg) {
  char* serialized = syncRequestVote2Str(pMsg);
  printf("syncRequestVotePrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncRequestVoteLog(const SyncRequestVote* pMsg) {
  char* serialized = syncRequestVote2Str(pMsg);
  sTrace("syncRequestVoteLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncRequestVoteLog2(char* s, const SyncRequestVote* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncRequestVote2Str(pMsg);
    sTrace("syncRequestVoteLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// ---- message process SyncRequestVoteReply----
SyncRequestVoteReply* syncRequestVoteReplyBuild(int32_t vgId) {
  uint32_t              bytes = sizeof(SyncRequestVoteReply);
  SyncRequestVoteReply* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->vgId = vgId;
  pMsg->msgType = TDMT_SYNC_REQUEST_VOTE_REPLY;
  return pMsg;
}

void syncRequestVoteReplyDestroy(SyncRequestVoteReply* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncRequestVoteReplySerialize(const SyncRequestVoteReply* pMsg, char* buf, uint32_t bufLen) {
  ASSERT(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncRequestVoteReplyDeserialize(const char* buf, uint32_t len, SyncRequestVoteReply* pMsg) {
  memcpy(pMsg, buf, len);
  ASSERT(len == pMsg->bytes);
}

char* syncRequestVoteReplySerialize2(const SyncRequestVoteReply* pMsg, uint32_t* len) {
  char* buf = taosMemoryMalloc(pMsg->bytes);
  ASSERT(buf != NULL);
  syncRequestVoteReplySerialize(pMsg, buf, pMsg->bytes);
  if (len != NULL) {
    *len = pMsg->bytes;
  }
  return buf;
}

SyncRequestVoteReply* syncRequestVoteReplyDeserialize2(const char* buf, uint32_t len) {
  uint32_t              bytes = *((uint32_t*)buf);
  SyncRequestVoteReply* pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  syncRequestVoteReplyDeserialize(buf, len, pMsg);
  ASSERT(len == pMsg->bytes);
  return pMsg;
}

void syncRequestVoteReply2RpcMsg(const SyncRequestVoteReply* pMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pMsg->msgType;
  pRpcMsg->contLen = pMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  syncRequestVoteReplySerialize(pMsg, pRpcMsg->pCont, pRpcMsg->contLen);
}

void syncRequestVoteReplyFromRpcMsg(const SRpcMsg* pRpcMsg, SyncRequestVoteReply* pMsg) {
  syncRequestVoteReplyDeserialize(pRpcMsg->pCont, pRpcMsg->contLen, pMsg);
}

SyncRequestVoteReply* syncRequestVoteReplyFromRpcMsg2(const SRpcMsg* pRpcMsg) {
  SyncRequestVoteReply* pMsg = syncRequestVoteReplyDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
  ASSERT(pMsg != NULL);
  return pMsg;
}

cJSON* syncRequestVoteReply2Json(const SyncRequestVoteReply* pMsg) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);

    cJSON* pSrcId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->srcId.addr);
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
    cJSON_AddNumberToObject(pDestId, "addr", pMsg->destId.addr);
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

    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);
    cJSON_AddNumberToObject(pRoot, "vote_granted", pMsg->voteGranted);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncRequestVoteReply", pRoot);
  return pJson;
}

char* syncRequestVoteReply2Str(const SyncRequestVoteReply* pMsg) {
  cJSON* pJson = syncRequestVoteReply2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug ----------------------
void syncRequestVoteReplyPrint(const SyncRequestVoteReply* pMsg) {
  char* serialized = syncRequestVoteReply2Str(pMsg);
  printf("syncRequestVoteReplyPrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncRequestVoteReplyPrint2(char* s, const SyncRequestVoteReply* pMsg) {
  char* serialized = syncRequestVoteReply2Str(pMsg);
  printf("syncRequestVoteReplyPrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncRequestVoteReplyLog(const SyncRequestVoteReply* pMsg) {
  char* serialized = syncRequestVoteReply2Str(pMsg);
  sTrace("syncRequestVoteReplyLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncRequestVoteReplyLog2(char* s, const SyncRequestVoteReply* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncRequestVoteReply2Str(pMsg);
    sTrace("syncRequestVoteReplyLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// ---- message process SyncAppendEntries----
SyncAppendEntries* syncAppendEntriesBuild(uint32_t dataLen, int32_t vgId) {
  uint32_t           bytes = sizeof(SyncAppendEntries) + dataLen;
  SyncAppendEntries* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->vgId = vgId;
  pMsg->msgType = TDMT_SYNC_APPEND_ENTRIES;
  pMsg->dataLen = dataLen;
  return pMsg;
}

void syncAppendEntriesDestroy(SyncAppendEntries* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncAppendEntriesSerialize(const SyncAppendEntries* pMsg, char* buf, uint32_t bufLen) {
  ASSERT(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncAppendEntriesDeserialize(const char* buf, uint32_t len, SyncAppendEntries* pMsg) {
  memcpy(pMsg, buf, len);
  ASSERT(len == pMsg->bytes);
  ASSERT(pMsg->bytes == sizeof(SyncAppendEntries) + pMsg->dataLen);
}

char* syncAppendEntriesSerialize2(const SyncAppendEntries* pMsg, uint32_t* len) {
  char* buf = taosMemoryMalloc(pMsg->bytes);
  ASSERT(buf != NULL);
  syncAppendEntriesSerialize(pMsg, buf, pMsg->bytes);
  if (len != NULL) {
    *len = pMsg->bytes;
  }
  return buf;
}

SyncAppendEntries* syncAppendEntriesDeserialize2(const char* buf, uint32_t len) {
  uint32_t           bytes = *((uint32_t*)buf);
  SyncAppendEntries* pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  syncAppendEntriesDeserialize(buf, len, pMsg);
  ASSERT(len == pMsg->bytes);
  return pMsg;
}

void syncAppendEntries2RpcMsg(const SyncAppendEntries* pMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pMsg->msgType;
  pRpcMsg->contLen = pMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  syncAppendEntriesSerialize(pMsg, pRpcMsg->pCont, pRpcMsg->contLen);
}

void syncAppendEntriesFromRpcMsg(const SRpcMsg* pRpcMsg, SyncAppendEntries* pMsg) {
  syncAppendEntriesDeserialize(pRpcMsg->pCont, pRpcMsg->contLen, pMsg);
}

SyncAppendEntries* syncAppendEntriesFromRpcMsg2(const SRpcMsg* pRpcMsg) {
  SyncAppendEntries* pMsg = syncAppendEntriesDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
  ASSERT(pMsg != NULL);
  return pMsg;
}

cJSON* syncAppendEntries2Json(const SyncAppendEntries* pMsg) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);

    cJSON* pSrcId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->srcId.addr);
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
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->destId.addr);
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

    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->privateTerm);
    cJSON_AddStringToObject(pRoot, "privateTerm", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%ld", pMsg->prevLogIndex);
    cJSON_AddStringToObject(pRoot, "prevLogIndex", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->prevLogTerm);
    cJSON_AddStringToObject(pRoot, "pre_log_term", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%ld", pMsg->commitIndex);
    cJSON_AddStringToObject(pRoot, "commitIndex", u64buf);

    cJSON_AddNumberToObject(pRoot, "dataLen", pMsg->dataLen);
    char* s;
    s = syncUtilprintBin((char*)(pMsg->data), pMsg->dataLen);
    cJSON_AddStringToObject(pRoot, "data", s);
    taosMemoryFree(s);
    s = syncUtilprintBin2((char*)(pMsg->data), pMsg->dataLen);
    cJSON_AddStringToObject(pRoot, "data2", s);
    taosMemoryFree(s);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncAppendEntries", pRoot);
  return pJson;
}

char* syncAppendEntries2Str(const SyncAppendEntries* pMsg) {
  cJSON* pJson = syncAppendEntries2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug ----------------------
void syncAppendEntriesPrint(const SyncAppendEntries* pMsg) {
  char* serialized = syncAppendEntries2Str(pMsg);
  printf("syncAppendEntriesPrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncAppendEntriesPrint2(char* s, const SyncAppendEntries* pMsg) {
  char* serialized = syncAppendEntries2Str(pMsg);
  printf("syncAppendEntriesPrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncAppendEntriesLog(const SyncAppendEntries* pMsg) {
  char* serialized = syncAppendEntries2Str(pMsg);
  sTrace("syncAppendEntriesLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncAppendEntriesLog2(char* s, const SyncAppendEntries* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncAppendEntries2Str(pMsg);
    sTrace("syncAppendEntriesLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// ---- message process SyncAppendEntriesBatch----

// block1: SOffsetAndContLen
// block2: SOffsetAndContLen Array
// block3: entry Array

SyncAppendEntriesBatch* syncAppendEntriesBatchBuild(SSyncRaftEntry** entryPArr, int32_t arrSize, int32_t vgId) {
  ASSERT(entryPArr != NULL);
  ASSERT(arrSize >= 0);

  int32_t dataLen = 0;
  int32_t metaArrayLen = sizeof(SOffsetAndContLen) * arrSize;  // <offset, contLen>
  int32_t entryArrayLen = 0;
  for (int i = 0; i < arrSize; ++i) {  // SRpcMsg pCont
    SSyncRaftEntry* pEntry = entryPArr[i];
    entryArrayLen += pEntry->bytes;
  }
  dataLen += (metaArrayLen + entryArrayLen);

  uint32_t                bytes = sizeof(SyncAppendEntriesBatch) + dataLen;
  SyncAppendEntriesBatch* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->vgId = vgId;
  pMsg->msgType = TDMT_SYNC_APPEND_ENTRIES_BATCH;
  pMsg->dataCount = arrSize;
  pMsg->dataLen = dataLen;

  SOffsetAndContLen* metaArr = (SOffsetAndContLen*)(pMsg->data);
  char*              pData = pMsg->data;

  for (int i = 0; i < arrSize; ++i) {
    // init meta <offset, contLen>
    if (i == 0) {
      metaArr[i].offset = metaArrayLen;
      metaArr[i].contLen = entryPArr[i]->bytes;
    } else {
      metaArr[i].offset = metaArr[i - 1].offset + metaArr[i - 1].contLen;
      metaArr[i].contLen = entryPArr[i]->bytes;
    }

    // init entry array
    ASSERT(metaArr[i].contLen == entryPArr[i]->bytes);
    memcpy(pData + metaArr[i].offset, entryPArr[i], metaArr[i].contLen);
  }

  return pMsg;
}

SOffsetAndContLen* syncAppendEntriesBatchMetaTableArray(SyncAppendEntriesBatch* pMsg) {
  return (SOffsetAndContLen*)(pMsg->data);
}

void syncAppendEntriesBatchDestroy(SyncAppendEntriesBatch* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncAppendEntriesBatchSerialize(const SyncAppendEntriesBatch* pMsg, char* buf, uint32_t bufLen) {
  ASSERT(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncAppendEntriesBatchDeserialize(const char* buf, uint32_t len, SyncAppendEntriesBatch* pMsg) {
  memcpy(pMsg, buf, len);
  ASSERT(len == pMsg->bytes);
  ASSERT(pMsg->bytes == sizeof(SyncAppendEntriesBatch) + pMsg->dataLen);
}

char* syncAppendEntriesBatchSerialize2(const SyncAppendEntriesBatch* pMsg, uint32_t* len) {
  char* buf = taosMemoryMalloc(pMsg->bytes);
  ASSERT(buf != NULL);
  syncAppendEntriesBatchSerialize(pMsg, buf, pMsg->bytes);
  if (len != NULL) {
    *len = pMsg->bytes;
  }
  return buf;
}

SyncAppendEntriesBatch* syncAppendEntriesBatchDeserialize2(const char* buf, uint32_t len) {
  uint32_t                bytes = *((uint32_t*)buf);
  SyncAppendEntriesBatch* pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  syncAppendEntriesBatchDeserialize(buf, len, pMsg);
  ASSERT(len == pMsg->bytes);
  return pMsg;
}

void syncAppendEntriesBatch2RpcMsg(const SyncAppendEntriesBatch* pMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pMsg->msgType;
  pRpcMsg->contLen = pMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  syncAppendEntriesBatchSerialize(pMsg, pRpcMsg->pCont, pRpcMsg->contLen);
}

void syncAppendEntriesBatchFromRpcMsg(const SRpcMsg* pRpcMsg, SyncAppendEntriesBatch* pMsg) {
  syncAppendEntriesBatchDeserialize(pRpcMsg->pCont, pRpcMsg->contLen, pMsg);
}

SyncAppendEntriesBatch* syncAppendEntriesBatchFromRpcMsg2(const SRpcMsg* pRpcMsg) {
  SyncAppendEntriesBatch* pMsg = syncAppendEntriesBatchDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
  ASSERT(pMsg != NULL);
  return pMsg;
}

cJSON* syncAppendEntriesBatch2Json(const SyncAppendEntriesBatch* pMsg) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);

    cJSON* pSrcId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->srcId.addr);
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
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->destId.addr);
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

    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%ld", pMsg->prevLogIndex);
    cJSON_AddStringToObject(pRoot, "prevLogIndex", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->prevLogTerm);
    cJSON_AddStringToObject(pRoot, "prevLogTerm", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%ld", pMsg->commitIndex);
    cJSON_AddStringToObject(pRoot, "commitIndex", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->privateTerm);
    cJSON_AddStringToObject(pRoot, "privateTerm", u64buf);

    cJSON_AddNumberToObject(pRoot, "dataCount", pMsg->dataCount);
    cJSON_AddNumberToObject(pRoot, "dataLen", pMsg->dataLen);

    int32_t metaArrayLen = sizeof(SOffsetAndContLen) * pMsg->dataCount;  // <offset, contLen>
    int32_t entryArrayLen = pMsg->dataLen - metaArrayLen;

    cJSON_AddNumberToObject(pRoot, "metaArrayLen", metaArrayLen);
    cJSON_AddNumberToObject(pRoot, "entryArrayLen", entryArrayLen);

    SOffsetAndContLen* metaArr = (SOffsetAndContLen*)(pMsg->data);

    cJSON* pMetaArr = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "metaArr", pMetaArr);
    for (int i = 0; i < pMsg->dataCount; ++i) {
      cJSON* pMeta = cJSON_CreateObject();
      cJSON_AddNumberToObject(pMeta, "offset", metaArr[i].offset);
      cJSON_AddNumberToObject(pMeta, "contLen", metaArr[i].contLen);
      cJSON_AddItemToArray(pMetaArr, pMeta);
    }

    cJSON* pEntryArr = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "entryArr", pEntryArr);
    for (int i = 0; i < pMsg->dataCount; ++i) {
      SSyncRaftEntry* pEntry = (SSyncRaftEntry*)(pMsg->data + metaArr[i].offset);
      cJSON*          pEntryJson = syncEntry2Json(pEntry);
      cJSON_AddItemToArray(pEntryArr, pEntryJson);
    }

    char* s;
    s = syncUtilprintBin((char*)(pMsg->data), pMsg->dataLen);
    cJSON_AddStringToObject(pRoot, "data", s);
    taosMemoryFree(s);
    s = syncUtilprintBin2((char*)(pMsg->data), pMsg->dataLen);
    cJSON_AddStringToObject(pRoot, "data2", s);
    taosMemoryFree(s);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncAppendEntriesBatch", pRoot);
  return pJson;
}

char* syncAppendEntriesBatch2Str(const SyncAppendEntriesBatch* pMsg) {
  cJSON* pJson = syncAppendEntriesBatch2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug ----------------------
void syncAppendEntriesBatchPrint(const SyncAppendEntriesBatch* pMsg) {
  char* serialized = syncAppendEntriesBatch2Str(pMsg);
  printf("syncAppendEntriesBatchPrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncAppendEntriesBatchPrint2(char* s, const SyncAppendEntriesBatch* pMsg) {
  char* serialized = syncAppendEntriesBatch2Str(pMsg);
  printf("syncAppendEntriesBatchPrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncAppendEntriesBatchLog(const SyncAppendEntriesBatch* pMsg) {
  char* serialized = syncAppendEntriesBatch2Str(pMsg);
  sTrace("syncAppendEntriesBatchLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncAppendEntriesBatchLog2(char* s, const SyncAppendEntriesBatch* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncAppendEntriesBatch2Str(pMsg);
    sTraceLong("syncAppendEntriesBatchLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// ---- message process SyncAppendEntriesReply----
SyncAppendEntriesReply* syncAppendEntriesReplyBuild(int32_t vgId) {
  uint32_t                bytes = sizeof(SyncAppendEntriesReply);
  SyncAppendEntriesReply* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->vgId = vgId;
  pMsg->msgType = TDMT_SYNC_APPEND_ENTRIES_REPLY;
  return pMsg;
}

void syncAppendEntriesReplyDestroy(SyncAppendEntriesReply* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncAppendEntriesReplySerialize(const SyncAppendEntriesReply* pMsg, char* buf, uint32_t bufLen) {
  ASSERT(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncAppendEntriesReplyDeserialize(const char* buf, uint32_t len, SyncAppendEntriesReply* pMsg) {
  memcpy(pMsg, buf, len);
  ASSERT(len == pMsg->bytes);
}

char* syncAppendEntriesReplySerialize2(const SyncAppendEntriesReply* pMsg, uint32_t* len) {
  char* buf = taosMemoryMalloc(pMsg->bytes);
  ASSERT(buf != NULL);
  syncAppendEntriesReplySerialize(pMsg, buf, pMsg->bytes);
  if (len != NULL) {
    *len = pMsg->bytes;
  }
  return buf;
}

SyncAppendEntriesReply* syncAppendEntriesReplyDeserialize2(const char* buf, uint32_t len) {
  uint32_t                bytes = *((uint32_t*)buf);
  SyncAppendEntriesReply* pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  syncAppendEntriesReplyDeserialize(buf, len, pMsg);
  ASSERT(len == pMsg->bytes);
  return pMsg;
}

void syncAppendEntriesReply2RpcMsg(const SyncAppendEntriesReply* pMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pMsg->msgType;
  pRpcMsg->contLen = pMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  syncAppendEntriesReplySerialize(pMsg, pRpcMsg->pCont, pRpcMsg->contLen);
}

void syncAppendEntriesReplyFromRpcMsg(const SRpcMsg* pRpcMsg, SyncAppendEntriesReply* pMsg) {
  syncAppendEntriesReplyDeserialize(pRpcMsg->pCont, pRpcMsg->contLen, pMsg);
}

SyncAppendEntriesReply* syncAppendEntriesReplyFromRpcMsg2(const SRpcMsg* pRpcMsg) {
  SyncAppendEntriesReply* pMsg = syncAppendEntriesReplyDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
  ASSERT(pMsg != NULL);
  return pMsg;
}

cJSON* syncAppendEntriesReply2Json(const SyncAppendEntriesReply* pMsg) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);

    cJSON* pSrcId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->srcId.addr);
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
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->destId.addr);
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

    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->privateTerm);
    cJSON_AddStringToObject(pRoot, "privateTerm", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);
    cJSON_AddNumberToObject(pRoot, "success", pMsg->success);
    snprintf(u64buf, sizeof(u64buf), "%ld", pMsg->matchIndex);
    cJSON_AddStringToObject(pRoot, "matchIndex", u64buf);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncAppendEntriesReply", pRoot);
  return pJson;
}

char* syncAppendEntriesReply2Str(const SyncAppendEntriesReply* pMsg) {
  cJSON* pJson = syncAppendEntriesReply2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug ----------------------
void syncAppendEntriesReplyPrint(const SyncAppendEntriesReply* pMsg) {
  char* serialized = syncAppendEntriesReply2Str(pMsg);
  printf("syncAppendEntriesReplyPrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncAppendEntriesReplyPrint2(char* s, const SyncAppendEntriesReply* pMsg) {
  char* serialized = syncAppendEntriesReply2Str(pMsg);
  printf("syncAppendEntriesReplyPrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncAppendEntriesReplyLog(const SyncAppendEntriesReply* pMsg) {
  char* serialized = syncAppendEntriesReply2Str(pMsg);
  sTrace("syncAppendEntriesReplyLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncAppendEntriesReplyLog2(char* s, const SyncAppendEntriesReply* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncAppendEntriesReply2Str(pMsg);
    sTrace("syncAppendEntriesReplyLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
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

    snprintf(u64buf, sizeof(u64buf), "%ld", pMsg->fsmMeta.index);
    cJSON_AddStringToObject(pRoot, "fsmMeta.index", u64buf);
    cJSON_AddNumberToObject(pRoot, "fsmMeta.isWeak", pMsg->fsmMeta.isWeak);
    cJSON_AddNumberToObject(pRoot, "fsmMeta.code", pMsg->fsmMeta.code);
    cJSON_AddNumberToObject(pRoot, "fsmMeta.state", pMsg->fsmMeta.state);
    cJSON_AddStringToObject(pRoot, "fsmMeta.state.str", syncUtilState2String(pMsg->fsmMeta.state));
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->fsmMeta.seqNum);
    cJSON_AddStringToObject(pRoot, "fsmMeta.seqNum", u64buf);

    cJSON_AddNumberToObject(pRoot, "dataLen", pMsg->dataLen);
    char* s;
    s = syncUtilprintBin((char*)(pMsg->data), pMsg->dataLen);
    cJSON_AddStringToObject(pRoot, "data", s);
    taosMemoryFree(s);
    s = syncUtilprintBin2((char*)(pMsg->data), pMsg->dataLen);
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
  printf("syncApplyMsgPrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncApplyMsgPrint2(char* s, const SyncApplyMsg* pMsg) {
  char* serialized = syncApplyMsg2Str(pMsg);
  printf("syncApplyMsgPrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncApplyMsgLog(const SyncApplyMsg* pMsg) {
  char* serialized = syncApplyMsg2Str(pMsg);
  sTrace("ssyncApplyMsgLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncApplyMsgLog2(char* s, const SyncApplyMsg* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncApplyMsg2Str(pMsg);
    sTrace("syncApplyMsgLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
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

cJSON* syncSnapshotSend2Json(const SyncSnapshotSend* pMsg) {
  char   u64buf[128];
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);

    cJSON* pSrcId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->srcId.addr);
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
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->destId.addr);
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

    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->privateTerm);
    cJSON_AddStringToObject(pRoot, "privateTerm", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%ld", pMsg->beginIndex);
    cJSON_AddStringToObject(pRoot, "beginIndex", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%ld", pMsg->lastIndex);
    cJSON_AddStringToObject(pRoot, "lastIndex", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%ld", pMsg->lastConfigIndex);
    cJSON_AddStringToObject(pRoot, "lastConfigIndex", u64buf);
    cJSON_AddItemToObject(pRoot, "lastConfig", syncCfg2Json((SSyncCfg*)&(pMsg->lastConfig)));

    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->lastTerm);
    cJSON_AddStringToObject(pRoot, "lastTerm", u64buf);

    cJSON_AddNumberToObject(pRoot, "seq", pMsg->seq);

    cJSON_AddNumberToObject(pRoot, "dataLen", pMsg->dataLen);
    char* s;
    s = syncUtilprintBin((char*)(pMsg->data), pMsg->dataLen);
    cJSON_AddStringToObject(pRoot, "data", s);
    taosMemoryFree(s);
    s = syncUtilprintBin2((char*)(pMsg->data), pMsg->dataLen);
    cJSON_AddStringToObject(pRoot, "data2", s);
    taosMemoryFree(s);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncSnapshotSend", pRoot);
  return pJson;
}

char* syncSnapshotSend2Str(const SyncSnapshotSend* pMsg) {
  cJSON* pJson = syncSnapshotSend2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug ----------------------
void syncSnapshotSendPrint(const SyncSnapshotSend* pMsg) {
  char* serialized = syncSnapshotSend2Str(pMsg);
  printf("syncSnapshotSendPrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncSnapshotSendPrint2(char* s, const SyncSnapshotSend* pMsg) {
  char* serialized = syncSnapshotSend2Str(pMsg);
  printf("syncSnapshotSendPrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncSnapshotSendLog(const SyncSnapshotSend* pMsg) {
  char* serialized = syncSnapshotSend2Str(pMsg);
  sTrace("syncSnapshotSendLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncSnapshotSendLog2(char* s, const SyncSnapshotSend* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncSnapshotSend2Str(pMsg);
    sTrace("syncSnapshotSendLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// ---------------------------------------------
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
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->srcId.addr);
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
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->destId.addr);
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

    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->privateTerm);
    cJSON_AddStringToObject(pRoot, "privateTerm", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%ld", pMsg->lastIndex);
    cJSON_AddStringToObject(pRoot, "lastIndex", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->lastTerm);
    cJSON_AddStringToObject(pRoot, "lastTerm", u64buf);

    cJSON_AddNumberToObject(pRoot, "ack", pMsg->ack);
    cJSON_AddNumberToObject(pRoot, "code", pMsg->code);
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
  printf("syncSnapshotRspPrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncSnapshotRspPrint2(char* s, const SyncSnapshotRsp* pMsg) {
  char* serialized = syncSnapshotRsp2Str(pMsg);
  printf("syncSnapshotRspPrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncSnapshotRspLog(const SyncSnapshotRsp* pMsg) {
  char* serialized = syncSnapshotRsp2Str(pMsg);
  sTrace("syncSnapshotRspLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncSnapshotRspLog2(char* s, const SyncSnapshotRsp* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncSnapshotRsp2Str(pMsg);
    sTrace("syncSnapshotRspLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
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
        snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->srcId.addr);
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
        snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->destId.addr);
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
    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->newLeaderId.addr);
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

// for debug ----------------------
void syncLeaderTransferPrint(const SyncLeaderTransfer* pMsg) {
  char* serialized = syncLeaderTransfer2Str(pMsg);
  printf("syncLeaderTransferPrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncLeaderTransferPrint2(char* s, const SyncLeaderTransfer* pMsg) {
  char* serialized = syncLeaderTransfer2Str(pMsg);
  printf("syncLeaderTransferPrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncLeaderTransferLog(const SyncLeaderTransfer* pMsg) {
  char* serialized = syncLeaderTransfer2Str(pMsg);
  sTrace("syncLeaderTransferLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncLeaderTransferLog2(char* s, const SyncLeaderTransfer* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncLeaderTransfer2Str(pMsg);
    sTrace("syncLeaderTransferLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

// ---------------------------------------------
SyncReconfigFinish* syncReconfigFinishBuild(int32_t vgId) {
  uint32_t            bytes = sizeof(SyncReconfigFinish);
  SyncReconfigFinish* pMsg = taosMemoryMalloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->vgId = vgId;
  pMsg->msgType = TDMT_SYNC_CONFIG_CHANGE_FINISH;
  return pMsg;
}

void syncReconfigFinishDestroy(SyncReconfigFinish* pMsg) {
  if (pMsg != NULL) {
    taosMemoryFree(pMsg);
  }
}

void syncReconfigFinishSerialize(const SyncReconfigFinish* pMsg, char* buf, uint32_t bufLen) {
  ASSERT(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncReconfigFinishDeserialize(const char* buf, uint32_t len, SyncReconfigFinish* pMsg) {
  memcpy(pMsg, buf, len);
  ASSERT(len == pMsg->bytes);
}

char* syncReconfigFinishSerialize2(const SyncReconfigFinish* pMsg, uint32_t* len) {
  char* buf = taosMemoryMalloc(pMsg->bytes);
  ASSERT(buf != NULL);
  syncReconfigFinishSerialize(pMsg, buf, pMsg->bytes);
  if (len != NULL) {
    *len = pMsg->bytes;
  }
  return buf;
}

SyncReconfigFinish* syncReconfigFinishDeserialize2(const char* buf, uint32_t len) {
  uint32_t            bytes = *((uint32_t*)buf);
  SyncReconfigFinish* pMsg = taosMemoryMalloc(bytes);
  ASSERT(pMsg != NULL);
  syncReconfigFinishDeserialize(buf, len, pMsg);
  ASSERT(len == pMsg->bytes);
  return pMsg;
}

void syncReconfigFinish2RpcMsg(const SyncReconfigFinish* pMsg, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pMsg->msgType;
  pRpcMsg->contLen = pMsg->bytes;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  syncReconfigFinishSerialize(pMsg, pRpcMsg->pCont, pRpcMsg->contLen);
}

void syncReconfigFinishFromRpcMsg(const SRpcMsg* pRpcMsg, SyncReconfigFinish* pMsg) {
  syncReconfigFinishDeserialize(pRpcMsg->pCont, pRpcMsg->contLen, pMsg);
}

SyncReconfigFinish* syncReconfigFinishFromRpcMsg2(const SRpcMsg* pRpcMsg) {
  SyncReconfigFinish* pMsg = syncReconfigFinishDeserialize2(pRpcMsg->pCont, pRpcMsg->contLen);
  ASSERT(pMsg != NULL);
  return pMsg;
}

cJSON* syncReconfigFinish2Json(const SyncReconfigFinish* pMsg) {
  char   u64buf[128];
  cJSON* pRoot = cJSON_CreateObject();

  if (pMsg != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
    cJSON_AddNumberToObject(pRoot, "vgId", pMsg->vgId);
    cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);

    cJSON* pOldCfg = syncCfg2Json((SSyncCfg*)(&(pMsg->oldCfg)));
    cJSON* pNewCfg = syncCfg2Json((SSyncCfg*)(&(pMsg->newCfg)));
    cJSON_AddItemToObject(pRoot, "oldCfg", pOldCfg);
    cJSON_AddItemToObject(pRoot, "newCfg", pNewCfg);

    snprintf(u64buf, sizeof(u64buf), "%ld", pMsg->newCfgIndex);
    cJSON_AddStringToObject(pRoot, "newCfgIndex", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->newCfgTerm);
    cJSON_AddStringToObject(pRoot, "newCfgTerm", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%lu", pMsg->newCfgSeqNum);
    cJSON_AddStringToObject(pRoot, "newCfgSeqNum", u64buf);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncReconfigFinish", pRoot);
  return pJson;
}

char* syncReconfigFinish2Str(const SyncReconfigFinish* pMsg) {
  cJSON* pJson = syncReconfigFinish2Json(pMsg);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug ----------------------
void syncReconfigFinishPrint(const SyncReconfigFinish* pMsg) {
  char* serialized = syncReconfigFinish2Str(pMsg);
  printf("syncReconfigFinishPrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncReconfigFinishPrint2(char* s, const SyncReconfigFinish* pMsg) {
  char* serialized = syncReconfigFinish2Str(pMsg);
  printf("syncReconfigFinishPrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncReconfigFinishLog(const SyncReconfigFinish* pMsg) {
  char* serialized = syncReconfigFinish2Str(pMsg);
  sTrace("syncReconfigFinishLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncReconfigFinishLog2(char* s, const SyncReconfigFinish* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncReconfigFinish2Str(pMsg);
    sTrace("syncReconfigFinishLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}