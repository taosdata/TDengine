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

SSyncRaftEntry* syncEntryBuild(SyncClientRequest* pMsg, SyncTerm term, SyncIndex index) {
  uint32_t        bytes = SYNC_ENTRY_FIX_LEN + pMsg->bytes;
  SSyncRaftEntry* pEntry = malloc(bytes);
  assert(pEntry != NULL);
  memset(pEntry, 0, bytes);

  pEntry->bytes = bytes;
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

void syncEntrySerialize(const SSyncRaftEntry* pEntry, char* buf, uint32_t bufLen) {
  assert(pEntry->bytes <= bufLen);
  memcpy(buf, pEntry, pEntry->bytes);
}

void syncEntryDeserialize(const char* buf, uint32_t len, SSyncRaftEntry* pEntry) {
  memcpy(pEntry, buf, len);
  assert(len == pEntry->bytes);
}

cJSON* syncEntry2Json(const SSyncRaftEntry* pEntry) {
  char u64buf[128];

  cJSON* pRoot = cJSON_CreateObject();
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