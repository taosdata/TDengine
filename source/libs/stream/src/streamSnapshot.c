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

#include "streamSnapshot.h"
#include "rocksdb/c.h"
#include "tcommon.h"

typedef struct SBackendFile {
  SArray* pSst;
  char*   pCurrent;
  char*   pMainfest;
  char*   pOptions;
  char*   pCheckpointMeta;
} SBanckendFile;
struct SStreamSnapHandle {
  void*          handle;
  SArray*        fileList;
  SBanckendFile* pBackendFile;
};

struct SStreamSnapReader {
  void*   pMeta;
  int64_t sver;
  int64_t ever;
};

// SMetaSnapWriter ========================================
struct StreamSnapWriter {
  void*   pMeta;
  int64_t sver;
  int64_t ever;
};

const char* ROCKSDB_OPTIONS = "OPTIONS";
const char* ROCKSDB_MAINFEST = "MANIFEST";
const char* ROCKSDB_SST = "sst";
const char* ROCKSDB_CURRENT = "CURRENT";
const char* ROCKSDB_CHECKPOINT_META = "CHECKPOINT";

int32_t streamSnapHandleInit(SStreamSnapHandle* handle, char* path) {
  // impl later
  int32_t code = 0;
  handle->fileList = taosArrayInit(32, sizeof(void*));

  TdDirPtr pDir = taosOpenDir(path);
  if (NULL == pDir) {
    goto _err;
  }

  SBanckendFile* pFile = taosMemoryCalloc(1, sizeof(SBanckendFile));
  pFile->pSst = taosArrayInit(16, sizeof(void*));

  TdDirEntryPtr pDirEntry;
  while ((pDirEntry = taosReadDir(pDir)) != NULL) {
    char* name = taosGetDirEntryName(pDirEntry);
    if (strlen(name) >= strlen(ROCKSDB_CURRENT) && 0 == strncmp(name, ROCKSDB_CURRENT, strlen(ROCKSDB_CURRENT))) {
      pFile->pCurrent = taosStrdup(name);
      continue;
    }
    if (strlen(name) >= strlen(ROCKSDB_MAINFEST) && 0 == strncmp(name, ROCKSDB_MAINFEST, strlen(ROCKSDB_MAINFEST))) {
      pFile->pMainfest = taosStrdup(name);
      continue;
    }
    if (strlen(name) >= strlen(ROCKSDB_OPTIONS) && 0 == strncmp(name, ROCKSDB_OPTIONS, strlen(ROCKSDB_OPTIONS))) {
      pFile->pMainfest = taosStrdup(name);
      continue;
    }
    if (strlen(name) >= strlen(ROCKSDB_CHECKPOINT_META) &&
        0 == strncmp(name, ROCKSDB_CHECKPOINT_META, strlen(ROCKSDB_CHECKPOINT_META))) {
      pFile->pCheckpointMeta = taosStrdup(name);
      continue;
    }
    if (strlen(name) >= strlen(ROCKSDB_SST) &&
        0 == strncmp(name - strlen(ROCKSDB_SST), ROCKSDB_SST, strlen(ROCKSDB_SST))) {
      char* sst = taosStrdup(name);
      taosArrayPush(pFile->pSst, &sst);
    }
  }
  taosCloseDir(&pDir);

  handle->pBackendFile = pFile;
_err:
  code = -1;
  return code;
}
void streamSnapHandleDestroy(SStreamSnapHandle* handle) {
  SBanckendFile* pFile = handle->pBackendFile;
  taosMemoryFree(pFile->pCheckpointMeta);
  taosMemoryFree(pFile->pCurrent);
  taosMemoryFree(pFile->pMainfest);
  taosMemoryFree(pFile->pOptions);
  for (int i = 0; i < taosArrayGetSize(pFile->pSst); i++) {
    char* sst = taosArrayGetP(pFile->pSst, i);
    taosMemoryFree(sst);
  }
  taosArrayDestroy(pFile->pSst);
  taosMemoryFree(pFile);
  return;
}

int32_t streamSnapReaderOpen(void* pMeta, int64_t sver, int64_t ever, void** ppReader) {
  // impl later
  rocksdb_t* db = NULL;

  return 0;
}
int32_t streamSnapReaderClose(void** ppReader) {
  // impl later
  return 0;
}
int32_t streamSnapRead(void* pReader, uint8_t** ppData) {
  // impl later

  return 0;
}
// SMetaSnapWriter ========================================
int32_t streamSnapWriterOpen(void* pMeta, int64_t sver, int64_t ever, void** ppWriter) {
  // impl later
  return 0;
}
int32_t streamSnapWrite(void* pWriter, uint8_t* pData, uint32_t nData) {
  // impl later
  return 0;
}
int32_t streamSnapWriterClose(void** ppWriter, int8_t rollback) { return 0; }
