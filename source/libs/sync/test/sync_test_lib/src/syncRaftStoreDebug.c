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
#include "cJSON.h"
#include "syncTest.h"

int32_t raftStoreFromJson(SRaftStore *pRaftStore, cJSON *pJson) { return 0; }

cJSON *raftStore2Json(SRaftStore *pRaftStore) {
  char   u64buf[128] = {0};
  cJSON *pRoot = cJSON_CreateObject();

  if (pRaftStore != NULL) {
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pRaftStore->currentTerm);
    cJSON_AddStringToObject(pRoot, "currentTerm", u64buf);

    cJSON *pVoteFor = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pRaftStore->voteFor.addr);
    cJSON_AddStringToObject(pVoteFor, "addr", u64buf);
    {
      uint64_t u64 = pRaftStore->voteFor.addr;
      char     host[128] = {0};
      uint16_t port;
      syncUtilU642Addr(u64, host, sizeof(host), &port);
      cJSON_AddStringToObject(pVoteFor, "addr_host", host);
      cJSON_AddNumberToObject(pVoteFor, "addr_port", port);
    }
    cJSON_AddNumberToObject(pVoteFor, "vgId", pRaftStore->voteFor.vgId);
    cJSON_AddItemToObject(pRoot, "voteFor", pVoteFor);

    // int hasVoted = raftStoreHasVoted(pRaftStore);
    // cJSON_AddNumberToObject(pRoot, "hasVoted", hasVoted);
  }

  cJSON *pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SRaftStore", pRoot);
  return pJson;
}

char *raftStore2Str(SRaftStore *pRaftStore) {
  cJSON *pJson = raftStore2Json(pRaftStore);
  char  *serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug -------------------
void raftStorePrint(SRaftStore *pObj) {
  char *serialized = raftStore2Str(pObj);
  printf("raftStorePrint | len:%d | %s \n", (int32_t)strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void raftStorePrint2(char *s, SRaftStore *pObj) {
  char *serialized = raftStore2Str(pObj);
  printf("raftStorePrint2 | len:%d | %s | %s \n", (int32_t)strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}
void raftStoreLog(SRaftStore *pObj) {
  char *serialized = raftStore2Str(pObj);
  sTrace("raftStoreLog | len:%d | %s", (int32_t)strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void raftStoreLog2(char *s, SRaftStore *pObj) {
  char *serialized = raftStore2Str(pObj);
  sTrace("raftStoreLog2 | len:%d | %s | %s", (int32_t)strlen(serialized), s, serialized);
  taosMemoryFree(serialized);
}