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
#include "tjson.h"

int32_t raftStoreReadFile(SSyncNode *pNode);
int32_t raftStoreWriteFile(SSyncNode *pNode);

static int32_t raftStoreDecode(const SJson *pJson, SRaftStore *pStore) {
  int32_t code = 0;

  tjsonGetNumberValue(pJson, "current_term", pStore->currentTerm, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "vote_for_addr", pStore->voteFor.addr, code);
  if (code < 0) return -1;
  tjsonGetInt32ValueFromDouble(pJson, "vote_for_vgid", pStore->voteFor.vgId, code);
  if (code < 0) return -1;

  return 0;
}

int32_t raftStoreReadFile(SSyncNode *pNode) {
  int32_t     code = -1;
  TdFilePtr   pFile = NULL;
  char       *pData = NULL;
  SJson      *pJson = NULL;
  const char *file = pNode->raftStorePath;
  SRaftStore *pStore = &pNode->raftStore;

  if (taosStatFile(file, NULL, NULL, NULL) < 0) {
    sInfo("vgId:%d, raft store file:%s not exist, use default value", pNode->vgId, file);
    pStore->currentTerm = 0;
    pStore->voteFor.addr = 0;
    pStore->voteFor.vgId = 0;
    return raftStoreWriteFile(pNode);
  }

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    sError("vgId:%d, failed to open raft store file:%s since %s", pNode->vgId, file, terrstr());
    goto _OVER;
  }

  int64_t size = 0;
  if (taosFStatFile(pFile, &size, NULL) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    sError("vgId:%d, failed to fstat raft store file:%s since %s", pNode->vgId, file, terrstr());
    goto _OVER;
  }

  pData = taosMemoryMalloc(size + 1);
  if (pData == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  if (taosReadFile(pFile, pData, size) != size) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    sError("vgId:%d, failed to read raft store file:%s since %s", pNode->vgId, file, terrstr());
    goto _OVER;
  }

  pData[size] = '\0';

  pJson = tjsonParse(pData);
  if (pJson == NULL) {
    terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  if (raftStoreDecode(pJson, pStore) < 0) {
    terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  code = 0;
  sInfo("vgId:%d, succceed to read raft store file %s", pNode->vgId, file);

_OVER:
  if (pData != NULL) taosMemoryFree(pData);
  if (pJson != NULL) cJSON_Delete(pJson);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    sError("vgId:%d, failed to read raft store file:%s since %s", pNode->vgId, file, terrstr());
  }
  return code;
}

static int32_t raftStoreEncode(SJson *pJson, SRaftStore *pStore) {
  if (tjsonAddIntegerToObject(pJson, "current_term", pStore->currentTerm) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "vote_for_addr", pStore->voteFor.addr) < 0) return -1;
  if (tjsonAddDoubleToObject(pJson, "vote_for_vgid", pStore->voteFor.vgId) < 0) return -1;
  return 0;
}

int32_t raftStoreWriteFile(SSyncNode *pNode) {
  int32_t     code = -1;
  char       *buffer = NULL;
  SJson      *pJson = NULL;
  TdFilePtr   pFile = NULL;
  const char *realfile = pNode->raftStorePath;
  SRaftStore *pStore = &pNode->raftStore;
  char        file[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s.bak", realfile);

  terrno = TSDB_CODE_OUT_OF_MEMORY;
  pJson = tjsonCreateObject();
  if (pJson == NULL) goto _OVER;
  if (raftStoreEncode(pJson, pStore) != 0) goto _OVER;
  buffer = tjsonToString(pJson);
  if (buffer == NULL) goto _OVER;
  terrno = 0;

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) goto _OVER;

  int32_t len = strlen(buffer);
  if (taosWriteFile(pFile, buffer, len) <= 0) goto _OVER;
  if (taosFsyncFile(pFile) < 0) goto _OVER;

  taosCloseFile(&pFile);
  if (taosRenameFile(file, realfile) != 0) goto _OVER;

  code = 0;
  sInfo("vgId:%d, succeed to write raft store file:%s, term:%" PRId64, pNode->vgId, realfile, pStore->currentTerm);

_OVER:
  if (pJson != NULL) tjsonDelete(pJson);
  if (buffer != NULL) taosMemoryFree(buffer);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    if (terrno == 0) terrno = TAOS_SYSTEM_ERROR(errno);
    sError("vgId:%d, failed to write raft store file:%s since %s", pNode->vgId, realfile, terrstr());
  }
  return code;
}

int32_t raftStoreOpen(SSyncNode *pNode) {
  taosThreadMutexInit(&pNode->raftStore.mutex, NULL);
  return raftStoreReadFile(pNode);
}

void raftStoreClose(SSyncNode *pNode) { taosThreadMutexDestroy(&pNode->raftStore.mutex); }

bool raftStoreHasVoted(SSyncNode *pNode) {
  taosThreadMutexLock(&pNode->raftStore.mutex);
  bool b = syncUtilEmptyId(&pNode->raftStore.voteFor);
  taosThreadMutexUnlock(&pNode->raftStore.mutex);
  return (!b);
}

void raftStoreVote(SSyncNode *pNode, SRaftId *pRaftId) {
  taosThreadMutexLock(&pNode->raftStore.mutex);
  pNode->raftStore.voteFor = *pRaftId;
  (void)raftStoreWriteFile(pNode);
  taosThreadMutexUnlock(&pNode->raftStore.mutex);
}

void raftStoreClearVote(SSyncNode *pNode) {
  taosThreadMutexLock(&pNode->raftStore.mutex);
  pNode->raftStore.voteFor = EMPTY_RAFT_ID;
  (void)raftStoreWriteFile(pNode);
  taosThreadMutexUnlock(&pNode->raftStore.mutex);
}

void raftStoreNextTerm(SSyncNode *pNode) {
  taosThreadMutexLock(&pNode->raftStore.mutex);
  pNode->raftStore.currentTerm++;
  (void)raftStoreWriteFile(pNode);
  taosThreadMutexUnlock(&pNode->raftStore.mutex);
}

void raftStoreSetTerm(SSyncNode *pNode, SyncTerm term) {
  taosThreadMutexLock(&pNode->raftStore.mutex);
  if (pNode->raftStore.currentTerm < term) {
    pNode->raftStore.currentTerm = term;
    (void)raftStoreWriteFile(pNode);
  }
  taosThreadMutexUnlock(&pNode->raftStore.mutex);
}

SyncTerm raftStoreGetTerm(SSyncNode *pNode) {
  taosThreadMutexLock(&pNode->raftStore.mutex);
  SyncTerm term = pNode->raftStore.currentTerm;
  taosThreadMutexUnlock(&pNode->raftStore.mutex);
  return term;
}
