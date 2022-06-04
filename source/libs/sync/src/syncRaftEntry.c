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

#include "syncRaftEntry.h"
#include "syncUtil.h"

SSyncRaftEntry* syncEntryBuild(uint32_t dataLen) {
  uint32_t        bytes = sizeof(SSyncRaftEntry) + dataLen;
  SSyncRaftEntry* pEntry = taosMemoryMalloc(bytes);
  assert(pEntry != NULL);
  memset(pEntry, 0, bytes);
  pEntry->bytes = bytes;
  pEntry->dataLen = dataLen;
  return pEntry;
}

// step 4. SyncClientRequest => SSyncRaftEntry, add term, index
SSyncRaftEntry* syncEntryBuild2(SyncClientRequest* pMsg, SyncTerm term, SyncIndex index) {
  SSyncRaftEntry* pEntry = syncEntryBuild3(pMsg, term, index);
  assert(pEntry != NULL);

  return pEntry;
}

SSyncRaftEntry* syncEntryBuild3(SyncClientRequest* pMsg, SyncTerm term, SyncIndex index) {
  SSyncRaftEntry* pEntry = syncEntryBuild(pMsg->dataLen);
  assert(pEntry != NULL);

  pEntry->msgType = pMsg->msgType;
  pEntry->originalRpcType = pMsg->originalRpcType;
  pEntry->seqNum = pMsg->seqNum;
  pEntry->isWeak = pMsg->isWeak;
  pEntry->term = term;
  pEntry->index = index;
  pEntry->dataLen = pMsg->dataLen;
  memcpy(pEntry->data, pMsg->data, pMsg->dataLen);

  return pEntry;
}

SSyncRaftEntry* syncEntryBuildNoop(SyncTerm term, SyncIndex index, int32_t vgId) {
  // init rpcMsg
  SMsgHead head;
  head.vgId = vgId;
  head.contLen = sizeof(SMsgHead);
  SRpcMsg rpcMsg;
  memset(&rpcMsg, 0, sizeof(SRpcMsg));
  rpcMsg.contLen = head.contLen;
  rpcMsg.pCont = rpcMallocCont(rpcMsg.contLen);
  rpcMsg.msgType = TDMT_VND_SYNC_NOOP;
  memcpy(rpcMsg.pCont, &head, sizeof(head));

  SSyncRaftEntry* pEntry = syncEntryBuild(rpcMsg.contLen);
  assert(pEntry != NULL);

  pEntry->msgType = TDMT_VND_SYNC_CLIENT_REQUEST;
  pEntry->originalRpcType = TDMT_VND_SYNC_NOOP;
  pEntry->seqNum = 0;
  pEntry->isWeak = 0;
  pEntry->term = term;
  pEntry->index = index;

  assert(pEntry->dataLen == rpcMsg.contLen);
  memcpy(pEntry->data, rpcMsg.pCont, rpcMsg.contLen);
  rpcFreeCont(rpcMsg.pCont);

  return pEntry;
}

void syncEntryDestory(SSyncRaftEntry* pEntry) {
  if (pEntry != NULL) {
    taosMemoryFree(pEntry);
  }
}

// step 5. SSyncRaftEntry => bin, to raft log
char* syncEntrySerialize(const SSyncRaftEntry* pEntry, uint32_t* len) {
  char* buf = taosMemoryMalloc(pEntry->bytes);
  assert(buf != NULL);
  memcpy(buf, pEntry, pEntry->bytes);
  if (len != NULL) {
    *len = pEntry->bytes;
  }
  return buf;
}

// step 6. bin => SSyncRaftEntry, from raft log
SSyncRaftEntry* syncEntryDeserialize(const char* buf, uint32_t len) {
  uint32_t        bytes = *((uint32_t*)buf);
  SSyncRaftEntry* pEntry = taosMemoryMalloc(bytes);
  assert(pEntry != NULL);
  memcpy(pEntry, buf, len);
  assert(len == pEntry->bytes);
  return pEntry;
}

cJSON* syncEntry2Json(const SSyncRaftEntry* pEntry) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pEntry != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pEntry->bytes);
    cJSON_AddNumberToObject(pRoot, "msgType", pEntry->msgType);
    cJSON_AddNumberToObject(pRoot, "originalRpcType", pEntry->originalRpcType);
    snprintf(u64buf, sizeof(u64buf), "%lu", pEntry->seqNum);
    cJSON_AddStringToObject(pRoot, "seqNum", u64buf);
    cJSON_AddNumberToObject(pRoot, "isWeak", pEntry->isWeak);
    snprintf(u64buf, sizeof(u64buf), "%lu", pEntry->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%lu", pEntry->index);
    cJSON_AddStringToObject(pRoot, "index", u64buf);
    cJSON_AddNumberToObject(pRoot, "dataLen", pEntry->dataLen);

    char* s;
    s = syncUtilprintBin((char*)(pEntry->data), pEntry->dataLen);
    cJSON_AddStringToObject(pRoot, "data", s);
    taosMemoryFree(s);

    s = syncUtilprintBin2((char*)(pEntry->data), pEntry->dataLen);
    cJSON_AddStringToObject(pRoot, "data2", s);
    taosMemoryFree(s);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SSyncRaftEntry", pRoot);
  return pJson;
}

char* syncEntry2Str(const SSyncRaftEntry* pEntry) {
  cJSON* pJson = syncEntry2Json(pEntry);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// step 7. SSyncRaftEntry => original SRpcMsg, commit to user, delete seqNum, isWeak, term, index
void syncEntry2OriginalRpc(const SSyncRaftEntry* pEntry, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pEntry->originalRpcType;
  pRpcMsg->contLen = pEntry->dataLen;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  memcpy(pRpcMsg->pCont, pEntry->data, pRpcMsg->contLen);
}

// for debug ----------------------
void syncEntryPrint(const SSyncRaftEntry* pObj) {
  char* serialized = syncEntry2Str(pObj);
  printf("syncEntryPrint | len:%zu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncEntryPrint2(char* s, const SSyncRaftEntry* pObj) {
  char* serialized = syncEntry2Str(pObj);
  printf("syncEntryPrint2 | len:%zu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncEntryLog(const SSyncRaftEntry* pObj) {
  char* serialized = syncEntry2Str(pObj);
  sTrace("syncEntryLog | len:%zu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncEntryLog2(char* s, const SSyncRaftEntry* pObj) {
  char* serialized = syncEntry2Str(pObj);
  sTrace("syncEntryLog2 | len:%zu | %s | %s", strlen(serialized), s, serialized);
  taosMemoryFree(serialized);
}
