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
#include "syncBatch.h"
#include "syncTest.h"

// ---- message process SyncClientRequestBatch----

// block1:
// block2: SRaftMeta array
// block3: rpc msg array (with pCont)

SyncClientRequestBatch* syncClientRequestBatchBuild(SRpcMsg** rpcMsgPArr, SRaftMeta* raftArr, int32_t arrSize,
                                                    int32_t vgId) {
  ASSERT(rpcMsgPArr != NULL);
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
    msgArr[i] = *(rpcMsgPArr[i]);
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
    s = syncUtilPrintBin((char*)(pMsg->data), pMsg->dataLen);
    cJSON_AddStringToObject(pRoot, "data", s);
    taosMemoryFree(s);
    s = syncUtilPrintBin2((char*)(pMsg->data), pMsg->dataLen);
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
  printf("syncClientRequestBatchPrint | len:%d | %s \n", (int32_t)strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncClientRequestBatchPrint2(char* s, const SyncClientRequestBatch* pMsg) {
  char* serialized = syncClientRequestBatch2Str(pMsg);
  printf("syncClientRequestBatchPrint2 | len:%d | %s | %s \n", (int32_t)strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncClientRequestBatchLog(const SyncClientRequestBatch* pMsg) {
  char* serialized = syncClientRequestBatch2Str(pMsg);
  sTrace("syncClientRequestBatchLog | len:%d | %s", (int32_t)strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncClientRequestBatchLog2(char* s, const SyncClientRequestBatch* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncClientRequestBatch2Str(pMsg);
    sLTrace("syncClientRequestBatchLog2 | len:%d | %s | %s", (int32_t)strlen(serialized), s, serialized);
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
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->srcId.addr);
    cJSON_AddStringToObject(pSrcId, "addr", u64buf);
    {
      uint64_t u64 = pMsg->srcId.addr;
      cJSON*   pTmp = pSrcId;
      char     host[128] = {0};
      uint16_t port;
      // syncUtilU642Addr(u64, host, sizeof(host), &port);
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

    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pMsg->prevLogIndex);
    cJSON_AddStringToObject(pRoot, "prevLogIndex", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->prevLogTerm);
    cJSON_AddStringToObject(pRoot, "prevLogTerm", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pMsg->commitIndex);
    cJSON_AddStringToObject(pRoot, "commitIndex", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pMsg->privateTerm);
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
    s = syncUtilPrintBin((char*)(pMsg->data), pMsg->dataLen);
    cJSON_AddStringToObject(pRoot, "data", s);
    taosMemoryFree(s);
    s = syncUtilPrintBin2((char*)(pMsg->data), pMsg->dataLen);
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
  printf("syncAppendEntriesBatchPrint | len:%d | %s \n", (int32_t)strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncAppendEntriesBatchPrint2(char* s, const SyncAppendEntriesBatch* pMsg) {
  char* serialized = syncAppendEntriesBatch2Str(pMsg);
  printf("syncAppendEntriesBatchPrint2 | len:%d | %s | %s \n", (int32_t)strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncAppendEntriesBatchLog(const SyncAppendEntriesBatch* pMsg) {
  char* serialized = syncAppendEntriesBatch2Str(pMsg);
  sTrace("syncAppendEntriesBatchLog | len:%d | %s", (int32_t)strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncAppendEntriesBatchLog2(char* s, const SyncAppendEntriesBatch* pMsg) {
  if (gRaftDetailLog) {
    char* serialized = syncAppendEntriesBatch2Str(pMsg);
    sLTrace("syncAppendEntriesBatchLog2 | len:%d | %s | %s", (int32_t)strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

void syncLogSendAppendEntriesBatch(SSyncNode* pSyncNode, const SyncAppendEntriesBatch* pMsg, const char* s) {
  char     host[64];
  uint16_t port;
  syncUtilU642Addr(pMsg->destId.addr, host, sizeof(host), &port);

  sNTrace(pSyncNode,
          "send sync-append-entries-batch to %s:%d, {term:%" PRId64 ", pre-index:%" PRId64 ", pre-term:%" PRId64
          ", pterm:%" PRId64 ", cmt:%" PRId64 ", datalen:%d, count:%d}, %s",
          host, port, pMsg->term, pMsg->prevLogIndex, pMsg->prevLogTerm, pMsg->privateTerm, pMsg->commitIndex,
          pMsg->dataLen, pMsg->dataCount, s);
}

void syncLogRecvAppendEntriesBatch(SSyncNode* pSyncNode, const SyncAppendEntriesBatch* pMsg, const char* s) {
  char     host[64];
  uint16_t port;
  syncUtilU642Addr(pMsg->srcId.addr, host, sizeof(host), &port);

  sNTrace(pSyncNode,
          "recv sync-append-entries-batch from %s:%d, {term:%" PRId64 ", pre-index:%" PRId64 ", pre-term:%" PRId64
          ", pterm:%" PRId64 ", cmt:%" PRId64 ", datalen:%d, count:%d}, %s",
          host, port, pMsg->term, pMsg->prevLogIndex, pMsg->prevLogTerm, pMsg->privateTerm, pMsg->commitIndex,
          pMsg->dataLen, pMsg->dataCount, s);
}