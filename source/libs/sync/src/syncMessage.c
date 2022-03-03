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
#include "syncRaft.h"
#include "syncUtil.h"
#include "tcoding.h"

void onMessage(SRaft* pRaft, void* pMsg) {}

// ---- message process SyncPing----
SyncPing* syncPingBuild(uint32_t dataLen) {
  uint32_t  bytes = SYNC_PING_FIX_LEN + dataLen;
  SyncPing* pMsg = malloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->msgType = SYNC_PING;
  pMsg->dataLen = dataLen;
}

void syncPingDestroy(SyncPing* pMsg) {
  if (pMsg != NULL) {
    free(pMsg);
  }
}

void syncPingSerialize(const SyncPing* pMsg, char* buf, uint32_t bufLen) {
  assert(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncPingDeserialize(const char* buf, uint32_t len, SyncPing* pMsg) {
  memcpy(pMsg, buf, len);
  assert(len == pMsg->bytes);
  assert(pMsg->bytes == SYNC_PING_FIX_LEN + pMsg->dataLen);
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

cJSON* syncPing2Json(const SyncPing* pMsg) {
  cJSON* pRoot = cJSON_CreateObject();
  cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
  cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);

  cJSON* pSrcId = cJSON_CreateObject();
  cJSON_AddNumberToObject(pSrcId, "addr", pMsg->srcId.addr);
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
  cJSON_AddNumberToObject(pDestId, "addr", pMsg->destId.addr);
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

  cJSON_AddNumberToObject(pRoot, "dataLen", pMsg->dataLen);
  cJSON_AddStringToObject(pRoot, "data", pMsg->data);

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncPing", pRoot);
  return pJson;
}

SyncPing* syncPingBuild2(const SRaftId* srcId, const SRaftId* destId, const char* str) {
  uint32_t  dataLen = strlen(str) + 1;
  SyncPing* pMsg = syncPingBuild(dataLen);
  pMsg->srcId = *srcId;
  pMsg->destId = *destId;
  snprintf(pMsg->data, pMsg->dataLen, "%s", str);
  return pMsg;
}

SyncPing* syncPingBuild3(const SRaftId* srcId, const SRaftId* destId) {
  SyncPing* pMsg = syncPingBuild2(srcId, destId, "ping");
  return pMsg;
}

// ---- message process SyncPingReply----
SyncPingReply* syncPingReplyBuild(uint32_t dataLen) {
  uint32_t       bytes = SYNC_PING_REPLY_FIX_LEN + dataLen;
  SyncPingReply* pMsg = malloc(bytes);
  memset(pMsg, 0, bytes);
  pMsg->bytes = bytes;
  pMsg->msgType = SYNC_PING_REPLY;
  pMsg->dataLen = dataLen;
}

void syncPingReplyDestroy(SyncPingReply* pMsg) {
  if (pMsg != NULL) {
    free(pMsg);
  }
}

void syncPingReplySerialize(const SyncPingReply* pMsg, char* buf, uint32_t bufLen) {
  assert(pMsg->bytes <= bufLen);
  memcpy(buf, pMsg, pMsg->bytes);
}

void syncPingReplyDeserialize(const char* buf, uint32_t len, SyncPingReply* pMsg) {
  memcpy(pMsg, buf, len);
  assert(len == pMsg->bytes);
  assert(pMsg->bytes == SYNC_PING_FIX_LEN + pMsg->dataLen);
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

cJSON* syncPingReply2Json(const SyncPingReply* pMsg) {
  cJSON* pRoot = cJSON_CreateObject();
  cJSON_AddNumberToObject(pRoot, "bytes", pMsg->bytes);
  cJSON_AddNumberToObject(pRoot, "msgType", pMsg->msgType);

  cJSON* pSrcId = cJSON_CreateObject();
  cJSON_AddNumberToObject(pSrcId, "addr", pMsg->srcId.addr);
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
  cJSON_AddNumberToObject(pDestId, "addr", pMsg->destId.addr);
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

  cJSON_AddNumberToObject(pRoot, "dataLen", pMsg->dataLen);
  cJSON_AddStringToObject(pRoot, "data", pMsg->data);

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SyncPingReply", pRoot);
  return pJson;
}

SyncPingReply* syncPingReplyBuild2(const SRaftId* srcId, const SRaftId* destId, const char* str) {
  uint32_t       dataLen = strlen(str) + 1;
  SyncPingReply* pMsg = syncPingReplyBuild(dataLen);
  pMsg->srcId = *srcId;
  pMsg->destId = *destId;
  snprintf(pMsg->data, pMsg->dataLen, "%s", str);
  return pMsg;
}

SyncPingReply* syncPingReplyBuild3(const SRaftId* srcId, const SRaftId* destId) {
  SyncPingReply* pMsg = syncPingReplyBuild2(srcId, destId, "pang");
  return pMsg;
}

#if 0
void syncPingSerialize(const SyncPing* pMsg, char** ppBuf, uint32_t* bufLen) {
  *bufLen = sizeof(SyncPing) + pMsg->dataLen;
  *ppBuf = (char*)malloc(*bufLen);
  void*    pStart = *ppBuf;
  uint32_t allBytes = *bufLen;

  int len = 0;
  len = taosEncodeFixedU32(&pStart, pMsg->msgType);
  allBytes -= len;
  assert(len > 0);
  pStart += len;

  len = taosEncodeFixedU64(&pStart, pMsg->srcId.addr);
  allBytes -= len;
  assert(len > 0);
  pStart += len;

  len = taosEncodeFixedI32(&pStart, pMsg->srcId.vgId);
  allBytes -= len;
  assert(len > 0);
  pStart += len;

  len = taosEncodeFixedU64(&pStart, pMsg->destId.addr);
  allBytes -= len;
  assert(len > 0);
  pStart += len;

  len = taosEncodeFixedI32(&pStart, pMsg->destId.vgId);
  allBytes -= len;
  assert(len > 0);
  pStart += len;

  len = taosEncodeFixedU32(&pStart, pMsg->dataLen);
  allBytes -= len;
  assert(len > 0);
  pStart += len;

  memcpy(pStart, pMsg->data, pMsg->dataLen);
  allBytes -= pMsg->dataLen;
  assert(allBytes == 0);
}


void syncPingDeserialize(const char* buf, uint32_t len, SyncPing* pMsg) {
  void*    pStart = (void*)buf;
  uint64_t u64;
  int32_t  i32;
  uint32_t u32;

  pStart = taosDecodeFixedU64(pStart, &u64);
  pMsg->msgType = u64;

  pStart = taosDecodeFixedU64(pStart, &u64);
  pMsg->srcId.addr = u64;

  pStart = taosDecodeFixedI32(pStart, &i32);
  pMsg->srcId.vgId = i32;

  pStart = taosDecodeFixedU64(pStart, &u64);
  pMsg->destId.addr = u64;

  pStart = taosDecodeFixedI32(pStart, &i32);
  pMsg->destId.vgId = i32;

  pStart = taosDecodeFixedU32(pStart, &u32);
  pMsg->dataLen = u32;
}
#endif