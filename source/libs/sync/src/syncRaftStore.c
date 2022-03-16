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

#include "syncRaftStore.h"
#include "cJSON.h"
#include "syncEnv.h"
#include "syncUtil.h"

// private function
static int32_t raftStoreInit(SRaftStore *pRaftStore);
static bool    raftStoreFileExist(char *path);

// public function
SRaftStore *raftStoreOpen(const char *path) {
  int32_t ret;

  SRaftStore *pRaftStore = malloc(sizeof(SRaftStore));
  if (pRaftStore == NULL) {
    sError("raftStoreOpen malloc error");
    return NULL;
  }
  memset(pRaftStore, 0, sizeof(*pRaftStore));
  snprintf(pRaftStore->path, sizeof(pRaftStore->path), "%s", path);

  char storeBuf[RAFT_STORE_BLOCK_SIZE];
  memset(storeBuf, 0, sizeof(storeBuf));

  if (!raftStoreFileExist(pRaftStore->path)) {
    ret = raftStoreInit(pRaftStore);
    assert(ret == 0);
  }

  pRaftStore->pFile = taosOpenFile(path, TD_FILE_READ | TD_FILE_WRITE);
  assert(pRaftStore->pFile != NULL);

  int len = taosReadFile(pRaftStore->pFile, storeBuf, RAFT_STORE_BLOCK_SIZE);
  assert(len == RAFT_STORE_BLOCK_SIZE);

  ret = raftStoreDeserialize(pRaftStore, storeBuf, len);
  assert(ret == 0);

  return pRaftStore;
}

static int32_t raftStoreInit(SRaftStore *pRaftStore) {
  assert(pRaftStore != NULL);

  pRaftStore->pFile = taosOpenFile(pRaftStore->path, TD_FILE_CTEATE | TD_FILE_WRITE);
  assert(pRaftStore->pFile != NULL);

  pRaftStore->currentTerm = 0;
  pRaftStore->voteFor.addr = 0;
  pRaftStore->voteFor.vgId = 0;

  int32_t ret = raftStorePersist(pRaftStore);
  assert(ret == 0);

  taosCloseFile(&pRaftStore->pFile);
  return 0;
}

int32_t raftStoreClose(SRaftStore *pRaftStore) {
  assert(pRaftStore != NULL);

  taosCloseFile(&pRaftStore->pFile);
  free(pRaftStore);
  pRaftStore = NULL;
  return 0;
}

int32_t raftStorePersist(SRaftStore *pRaftStore) {
  assert(pRaftStore != NULL);

  int32_t ret;
  char    storeBuf[RAFT_STORE_BLOCK_SIZE];
  ret = raftStoreSerialize(pRaftStore, storeBuf, sizeof(storeBuf));
  assert(ret == 0);

  taosLSeekFile(pRaftStore->pFile, 0, SEEK_SET);

  ret = taosWriteFile(pRaftStore->pFile, storeBuf, sizeof(storeBuf));
  assert(ret == RAFT_STORE_BLOCK_SIZE);

  taosFsyncFile(pRaftStore->pFile);
  return 0;
}

static bool raftStoreFileExist(char *path) { return taosStatFile(path, NULL, NULL) >= 0; }

int32_t raftStoreSerialize(SRaftStore *pRaftStore, char *buf, size_t len) {
  assert(pRaftStore != NULL);

  cJSON *pRoot = cJSON_CreateObject();
  cJSON_AddNumberToObject(pRoot, "current_term", pRaftStore->currentTerm);
  cJSON_AddNumberToObject(pRoot, "vote_for_addr", pRaftStore->voteFor.addr);
  cJSON_AddNumberToObject(pRoot, "vote_for_vgid", pRaftStore->voteFor.vgId);

  char *serialized = cJSON_Print(pRoot);
  int   len2 = strlen(serialized);
  assert(len2 < len);
  memset(buf, 0, len);
  snprintf(buf, len, "%s", serialized);
  free(serialized);

  cJSON_Delete(pRoot);
  return 0;
}

int32_t raftStoreDeserialize(SRaftStore *pRaftStore, char *buf, size_t len) {
  assert(pRaftStore != NULL);

  assert(len > 0 && len <= RAFT_STORE_BLOCK_SIZE);
  cJSON *pRoot = cJSON_Parse(buf);

  cJSON *pCurrentTerm = cJSON_GetObjectItem(pRoot, "current_term");
  pRaftStore->currentTerm = pCurrentTerm->valueint;

  cJSON *pVoteForAddr = cJSON_GetObjectItem(pRoot, "vote_for_addr");
  pRaftStore->voteFor.addr = pVoteForAddr->valueint;

  cJSON *pVoteForVgid = cJSON_GetObjectItem(pRoot, "vote_for_vgid");
  pRaftStore->voteFor.vgId = pVoteForVgid->valueint;

  cJSON_Delete(pRoot);
  return 0;
}

bool raftStoreHasVoted(SRaftStore *pRaftStore) {
  bool b = syncUtilEmptyId(&(pRaftStore->voteFor));
  return b;
}

void raftStoreVote(SRaftStore *pRaftStore, SRaftId *pRaftId) {
  assert(!raftStoreHasVoted(pRaftStore));
  assert(!syncUtilEmptyId(pRaftId));
  pRaftStore->voteFor = *pRaftId;
  raftStorePersist(pRaftStore);
}

void raftStoreClearVote(SRaftStore *pRaftStore) {
  pRaftStore->voteFor = EMPTY_RAFT_ID;
  raftStorePersist(pRaftStore);
}

void raftStoreNextTerm(SRaftStore *pRaftStore) {
  ++(pRaftStore->currentTerm);
  raftStorePersist(pRaftStore);
}

void raftStoreSetTerm(SRaftStore *pRaftStore, SyncTerm term) {
  pRaftStore->currentTerm = term;
  raftStorePersist(pRaftStore);
}

cJSON *raftStore2Json(SRaftStore *pRaftStore) {
  char   u64buf[128];
  cJSON *pRoot = cJSON_CreateObject();

  if (pRaftStore != NULL) {
    snprintf(u64buf, sizeof(u64buf), "%lu", pRaftStore->currentTerm);
    cJSON_AddStringToObject(pRoot, "currentTerm", u64buf);

    cJSON *pVoteFor = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%lu", pRaftStore->voteFor.addr);
    cJSON_AddStringToObject(pVoteFor, "addr", u64buf);
    {
      uint64_t u64 = pRaftStore->voteFor.addr;
      char     host[128];
      uint16_t port;
      syncUtilU642Addr(u64, host, sizeof(host), &port);
      cJSON_AddStringToObject(pVoteFor, "addr_host", host);
      cJSON_AddNumberToObject(pVoteFor, "addr_port", port);
    }
    cJSON_AddNumberToObject(pVoteFor, "vgId", pRaftStore->voteFor.vgId);
    cJSON_AddItemToObject(pRoot, "voteFor", pVoteFor);
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
  printf("raftStorePrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  free(serialized);
}

void raftStorePrint2(char *s, SRaftStore *pObj) {
  char *serialized = raftStore2Str(pObj);
  printf("raftStorePrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  free(serialized);
}
void raftStoreLog(SRaftStore *pObj) {
  char *serialized = raftStore2Str(pObj);
  sTrace("raftStoreLog | len:%lu | %s", strlen(serialized), serialized);
  free(serialized);
}

void raftStoreLog2(char *s, SRaftStore *pObj) {
  char *serialized = raftStore2Str(pObj);
  sTrace("raftStoreLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
  free(serialized);
}
