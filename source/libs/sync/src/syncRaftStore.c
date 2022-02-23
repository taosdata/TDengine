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
#include "sync.h"

char *serialized;

void testJson() {
  FileFd raftStoreFd = taosOpenFileReadWrite("raft.store");

  uint64_t currentTerm = 100;
  uint64_t voteFor = 200;

  cJSON *pRoot = cJSON_CreateObject();
  cJSON_AddNumberToObject(pRoot, "current_term", currentTerm);
  cJSON_AddNumberToObject(pRoot, "vote_for", voteFor);

  serialized = cJSON_Print(pRoot);
  int len = strlen(serialized);
  printf("serialized: %s \n", serialized);

  taosWriteFile(raftStoreFd, serialized, len);
  taosCloseFile(raftStoreFd);
}

void testJson2() {
  cJSON *pRoot = cJSON_Parse(serialized);

  cJSON   *pCurrentTerm = cJSON_GetObjectItem(pRoot, "current_term");
  uint64_t currentTerm = pCurrentTerm->valueint;

  cJSON   *pVoteFor = cJSON_GetObjectItem(pRoot, "vote_for");
  uint64_t voteFor = pVoteFor->valueint;

  printf("read json: currentTerm:%lu, voteFor:%lu \n", currentTerm, voteFor);
}

int32_t currentTerm(SyncTerm *pCurrentTerm) { return 0; }

int32_t persistCurrentTerm(SyncTerm currentTerm) { return 0; }

int32_t voteFor(SRaftId *pRaftId) { return 0; }

int32_t persistVoteFor(SRaftId *pRaftId) { return 0; }