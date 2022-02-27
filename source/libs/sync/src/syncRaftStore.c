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

// to complie success: FileIO interface is modified

SRaftStore *raftStoreOpen(const char *path) { return NULL;}

static int32_t raftStoreInit(SRaftStore *pRaftStore) { return 0;}

int32_t raftStoreClose(SRaftStore *pRaftStore) { return 0;}

int32_t raftStorePersist(SRaftStore *pRaftStore) { return 0;}

static bool raftStoreFileExist(char *path) { return 0;}

int32_t raftStoreSerialize(SRaftStore *pRaftStore, char *buf, size_t len) { return 0;}

int32_t raftStoreDeserialize(SRaftStore *pRaftStore, char *buf, size_t len) { return 0;}

void raftStorePrint(SRaftStore *pRaftStore) {}



#if 0

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

  pRaftStore->fd = taosOpenFileReadWrite(pRaftStore->path);
  if (pRaftStore->fd < 0) {
    return NULL;
  }

  int len = taosReadFile(pRaftStore->fd, storeBuf, sizeof(storeBuf));
  assert(len == RAFT_STORE_BLOCK_SIZE);

  ret = raftStoreDeserialize(pRaftStore, storeBuf, len);
  assert(ret == 0);

  return pRaftStore;
}

static int32_t raftStoreInit(SRaftStore *pRaftStore) {
  pRaftStore->fd = taosOpenFileCreateWrite(pRaftStore->path);
  if (pRaftStore->fd < 0) {
    return -1;
  }

  pRaftStore->currentTerm = 0;
  pRaftStore->voteFor.addr = 0;
  pRaftStore->voteFor.vgId = 0;

  int32_t ret = raftStorePersist(pRaftStore);
  assert(ret == 0);

  taosCloseFile(pRaftStore->fd);
  return 0;
}

int32_t raftStoreClose(SRaftStore *pRaftStore) {
  taosCloseFile(pRaftStore->fd);
  free(pRaftStore);
  return 0;
}

int32_t raftStorePersist(SRaftStore *pRaftStore) {
  int32_t ret;
  char    storeBuf[RAFT_STORE_BLOCK_SIZE];

  ret = raftStoreSerialize(pRaftStore, storeBuf, sizeof(storeBuf));
  assert(ret == 0);

  taosLSeekFile(pRaftStore->fd, 0, SEEK_SET);

  ret = taosWriteFile(pRaftStore->fd, storeBuf, sizeof(storeBuf));
  assert(ret == RAFT_STORE_BLOCK_SIZE);

  fsync(pRaftStore->fd);
  return 0;
}

static bool raftStoreFileExist(char *path) { return taosStatFile(path, NULL, NULL) >= 0; }

int32_t raftStoreSerialize(SRaftStore *pRaftStore, char *buf, size_t len) {
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

void raftStorePrint(SRaftStore *pRaftStore) {
  char storeBuf[RAFT_STORE_BLOCK_SIZE];
  raftStoreSerialize(pRaftStore, storeBuf, sizeof(storeBuf));
  printf("%s\n", storeBuf);
}

#endif
