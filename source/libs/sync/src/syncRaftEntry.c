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
  SSyncRaftEntry* pEntry = malloc(bytes);
  assert(pEntry != NULL);
  memset(pEntry, 0, bytes);
  pEntry->bytes = bytes;
  pEntry->dataLen = dataLen;
  return pEntry;
}

SSyncRaftEntry* syncEntryBuild2(SyncClientRequest* pMsg, SyncTerm term, SyncIndex index) {
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

void syncEntryDestory(SSyncRaftEntry* pEntry) {
  if (pEntry != NULL) {
    free(pEntry);
  }
}

char* syncEntrySerialize(const SSyncRaftEntry* pEntry, uint32_t* len) {
  char* buf = malloc(pEntry->bytes);
  assert(buf != NULL);
  memcpy(buf, pEntry, pEntry->bytes);
  if (len != NULL) {
    *len = pEntry->bytes;
  }
  return buf;
}

SSyncRaftEntry* syncEntryDeserialize(const char* buf, uint32_t len) {
  uint32_t        bytes = *((uint32_t*)buf);
  SSyncRaftEntry* pEntry = malloc(bytes);
  assert(pEntry != NULL);
  memcpy(pEntry, buf, len);
  assert(len == pEntry->bytes);
  return pEntry;
}

cJSON* syncEntry2Json(const SSyncRaftEntry* pEntry) {
  char u64buf[128];

  cJSON* pRoot = cJSON_CreateObject();
  cJSON_AddNumberToObject(pRoot, "bytes", pEntry->bytes);
  cJSON_AddNumberToObject(pRoot, "msgType", pEntry->msgType);
  cJSON_AddNumberToObject(pRoot, "originalRpcType", pEntry->originalRpcType);
  snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pEntry->seqNum);
  cJSON_AddStringToObject(pRoot, "seqNum", u64buf);
  cJSON_AddNumberToObject(pRoot, "isWeak", pEntry->isWeak);
  snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pEntry->term);
  cJSON_AddStringToObject(pRoot, "term", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pEntry->index);
  cJSON_AddStringToObject(pRoot, "index", u64buf);
  cJSON_AddNumberToObject(pRoot, "dataLen", pEntry->dataLen);

  char* s;
  s = syncUtilprintBin((char*)(pEntry->data), pEntry->dataLen);
  cJSON_AddStringToObject(pRoot, "data", s);
  free(s);

  s = syncUtilprintBin2((char*)(pEntry->data), pEntry->dataLen);
  cJSON_AddStringToObject(pRoot, "data2", s);
  free(s);

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

// for debug ----------------------
void syncEntryPrint(const SSyncRaftEntry* pObj) {
  char* serialized = syncEntry2Str(pObj);
  printf("syncEntryPrint | len:%zu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  free(serialized);
}

void syncEntryPrint2(char* s, const SSyncRaftEntry* pObj) {
  char* serialized = syncEntry2Str(pObj);
  printf("syncEntryPrint2 | len:%zu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  free(serialized);
}

void syncEntryLog(const SSyncRaftEntry* pObj) {
  char* serialized = syncEntry2Str(pObj);
  sTrace("syncEntryLog | len:%zu | %s", strlen(serialized), serialized);
  free(serialized);
}

void syncEntryLog2(char* s, const SSyncRaftEntry* pObj) {
  char* serialized = syncEntry2Str(pObj);
  sTrace("syncEntryLog2 | len:%zu | %s | %s", strlen(serialized), s, serialized);
  free(serialized);
}
