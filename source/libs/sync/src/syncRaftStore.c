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
#include "syncRaftStore.h"
#include "syncUtil.h"

// private function
static int32_t raftStoreInit(SRaftStore *pRaftStore);
static bool    raftStoreFileExist(char *path);

// public function
SRaftStore *raftStoreOpen(const char *path) {
  int32_t ret;

  SRaftStore *pRaftStore = taosMemoryCalloc(1, sizeof(SRaftStore));
  if (pRaftStore == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  snprintf(pRaftStore->path, sizeof(pRaftStore->path), "%s", path);
  if (!raftStoreFileExist(pRaftStore->path)) {
    ret = raftStoreInit(pRaftStore);
    tAssert(ret == 0);
  }

  char storeBuf[RAFT_STORE_BLOCK_SIZE] = {0};
  pRaftStore->pFile = taosOpenFile(path, TD_FILE_READ | TD_FILE_WRITE);
  tAssert(pRaftStore->pFile != NULL);

  int len = taosReadFile(pRaftStore->pFile, storeBuf, RAFT_STORE_BLOCK_SIZE);
  tAssert(len > 0);

  ret = raftStoreDeserialize(pRaftStore, storeBuf, len);
  tAssert(ret == 0);

  return pRaftStore;
}

static int32_t raftStoreInit(SRaftStore *pRaftStore) {
  tAssert(pRaftStore != NULL);

  pRaftStore->pFile = taosOpenFile(pRaftStore->path, TD_FILE_CREATE | TD_FILE_WRITE);
  tAssert(pRaftStore->pFile != NULL);

  pRaftStore->currentTerm = 0;
  pRaftStore->voteFor.addr = 0;
  pRaftStore->voteFor.vgId = 0;

  int32_t ret = raftStorePersist(pRaftStore);
  tAssert(ret == 0);

  taosCloseFile(&pRaftStore->pFile);
  return 0;
}

int32_t raftStoreClose(SRaftStore *pRaftStore) {
  if (pRaftStore == NULL) return 0;

  taosCloseFile(&pRaftStore->pFile);
  taosMemoryFree(pRaftStore);
  pRaftStore = NULL;
  return 0;
}

int32_t raftStorePersist(SRaftStore *pRaftStore) {
  tAssert(pRaftStore != NULL);

  int32_t ret;
  char    storeBuf[RAFT_STORE_BLOCK_SIZE] = {0};
  ret = raftStoreSerialize(pRaftStore, storeBuf, sizeof(storeBuf));
  tAssert(ret == 0);

  taosLSeekFile(pRaftStore->pFile, 0, SEEK_SET);

  ret = taosWriteFile(pRaftStore->pFile, storeBuf, sizeof(storeBuf));
  tAssert(ret == RAFT_STORE_BLOCK_SIZE);

  taosFsyncFile(pRaftStore->pFile);
  return 0;
}

static bool raftStoreFileExist(char *path) {
  bool b = taosStatFile(path, NULL, NULL) >= 0;
  return b;
}

int32_t raftStoreSerialize(SRaftStore *pRaftStore, char *buf, size_t len) {
  tAssert(pRaftStore != NULL);

  cJSON *pRoot = cJSON_CreateObject();

  char u64Buf[128] = {0};
  snprintf(u64Buf, sizeof(u64Buf), "%" PRIu64 "", pRaftStore->currentTerm);
  cJSON_AddStringToObject(pRoot, "current_term", u64Buf);

  snprintf(u64Buf, sizeof(u64Buf), "%" PRIu64 "", pRaftStore->voteFor.addr);
  cJSON_AddStringToObject(pRoot, "vote_for_addr", u64Buf);

  cJSON_AddNumberToObject(pRoot, "vote_for_vgid", pRaftStore->voteFor.vgId);

  uint64_t u64 = pRaftStore->voteFor.addr;
  char     host[128] = {0};
  uint16_t port;
  syncUtilU642Addr(u64, host, sizeof(host), &port);
  cJSON_AddStringToObject(pRoot, "addr_host", host);
  cJSON_AddNumberToObject(pRoot, "addr_port", port);

  char *serialized = cJSON_Print(pRoot);
  int   len2 = strlen(serialized);
  tAssert(len2 < len);
  memset(buf, 0, len);
  snprintf(buf, len, "%s", serialized);
  taosMemoryFree(serialized);

  cJSON_Delete(pRoot);
  return 0;
}

int32_t raftStoreDeserialize(SRaftStore *pRaftStore, char *buf, size_t len) {
  tAssert(pRaftStore != NULL);

  tAssert(len > 0 && len <= RAFT_STORE_BLOCK_SIZE);
  cJSON *pRoot = cJSON_Parse(buf);

  cJSON *pCurrentTerm = cJSON_GetObjectItem(pRoot, "current_term");
  tAssert(cJSON_IsString(pCurrentTerm));
  sscanf(pCurrentTerm->valuestring, "%" PRIu64 "", &(pRaftStore->currentTerm));

  cJSON *pVoteForAddr = cJSON_GetObjectItem(pRoot, "vote_for_addr");
  tAssert(cJSON_IsString(pVoteForAddr));
  sscanf(pVoteForAddr->valuestring, "%" PRIu64 "", &(pRaftStore->voteFor.addr));

  cJSON *pVoteForVgid = cJSON_GetObjectItem(pRoot, "vote_for_vgid");
  pRaftStore->voteFor.vgId = pVoteForVgid->valueint;

  cJSON_Delete(pRoot);
  return 0;
}

bool raftStoreHasVoted(SRaftStore *pRaftStore) {
  bool b = syncUtilEmptyId(&(pRaftStore->voteFor));
  return (!b);
}

void raftStoreVote(SRaftStore *pRaftStore, SRaftId *pRaftId) {
  tAssert(!syncUtilEmptyId(pRaftId));
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
