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
#include "query.h"
#include "rocksdb/c.h"
#include "tcommon.h"

enum SBackendFileType {
  ROCKSDB_OPTIONS_TYPE = 1,
  ROCKSDB_MAINFEST_TYPE = 2,
  ROCKSDB_SST_TYPE = 3,
  ROCKSDB_CURRENT_TYPE = 4,
  ROCKSDB_CHECKPOINT_META_TYPE = 5,
};

typedef struct SBackendFileItem {
  char*   name;
  int8_t  type;
  int64_t size;
} SBackendFileItem;
typedef struct SBackendFile {
  char*   pCurrent;
  char*   pMainfest;
  char*   pOptions;
  SArray* pSst;
  char*   pCheckpointMeta;
  char*   path;
} SBanckendFile;
struct SStreamSnapHandle {
  void*          handle;
  SBanckendFile* pBackendFile;
  int64_t        checkpointId;
  int64_t        seraial;
  int64_t        offset;
  TdFilePtr      fd;
  int8_t         filetype;
  SArray*        pFileList;
  int32_t        currFileIdx;
};
struct SStreamSnapBlockHdr {
  int8_t  type;
  int8_t  flag;
  int64_t index;
  char    name[128];
  int64_t size;
  int64_t totalSize;
  uint8_t data[];
};
struct SStreamSnapReader {
  void*             pMeta;
  int64_t           sver;
  int64_t           ever;
  SStreamSnapHandle handle;
};
struct SStreamSnapWriter {
  void*             pMeta;
  int64_t           sver;
  int64_t           ever;
  SStreamSnapHandle handle;
};
const char*    ROCKSDB_OPTIONS = "OPTIONS";
const char*    ROCKSDB_MAINFEST = "MANIFEST";
const char*    ROCKSDB_SST = "sst";
const char*    ROCKSDB_CURRENT = "CURRENT";
const char*    ROCKSDB_CHECKPOINT_META = "CHECKPOINT";
static int64_t kBlockSize = 64 * 1024;

int32_t streamSnapHandleInit(SStreamSnapHandle* handle, char* path);
void    streamSnapHandleDestroy(SStreamSnapHandle* handle);

int32_t streamSnapHandleInit(SStreamSnapHandle* handle, char* path) {
  // impl later
  int32_t code = 0;

  TdDirPtr pDir = taosOpenDir(path);
  if (NULL == pDir) {
    goto _err;
  }

  SBanckendFile* pFile = taosMemoryCalloc(1, sizeof(SBanckendFile));
  handle->checkpointId = 0;
  handle->seraial = 0;
  pFile->path = taosStrdup(path);
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

  SArray* list = taosArrayInit(64, sizeof(SBackendFileItem));

  SBackendFileItem item;
  // current
  item.name = pFile->pCurrent;
  item.type = ROCKSDB_CURRENT_TYPE;
  taosStatFile(pFile->pCurrent, &item.size, NULL);
  taosArrayPush(list, &item);
  // mainfest
  item.name = pFile->pMainfest;
  item.type = ROCKSDB_MAINFEST_TYPE;
  taosStatFile(pFile->pMainfest, &item.size, NULL);
  taosArrayPush(list, &item);
  // options
  item.name = pFile->pOptions;
  item.type = ROCKSDB_OPTIONS_TYPE;
  taosStatFile(pFile->pOptions, &item.size, NULL);
  taosArrayPush(list, &item);
  // sst
  for (int i = 0; i < taosArrayGetSize(pFile->pSst); i++) {
    char* sst = taosArrayGetP(pFile->pSst, i);
    item.name = sst;
    item.type = ROCKSDB_SST_TYPE;
    taosStatFile(sst, &item.size, NULL);
    taosArrayPush(list, &item);
  }
  // meta
  item.name = pFile->pCheckpointMeta;
  item.type = ROCKSDB_CHECKPOINT_META_TYPE;
  taosStatFile(pFile->pCheckpointMeta, &item.size, NULL);
  taosArrayPush(list, &item);

  handle->pBackendFile = pFile;

  handle->currFileIdx = 0;
  handle->pFileList = list;
  handle->fd = taosOpenFile(taosArrayGet(handle->pFileList, handle->currFileIdx), TD_FILE_READ);
  if (handle->fd == NULL) {
    goto _err;
  }
  handle->seraial = 0;
  handle->offset = 0;
  return 0;
_err:
  streamSnapHandleDestroy(handle);

  code = -1;
  return code;
}

void streamSnapHandleDestroy(SStreamSnapHandle* handle) {
  SBanckendFile* pFile = handle->pBackendFile;
  taosMemoryFree(pFile->pCheckpointMeta);
  taosMemoryFree(pFile->pCurrent);
  taosMemoryFree(pFile->pMainfest);
  taosMemoryFree(pFile->pOptions);
  taosMemoryFree(pFile->path);
  for (int i = 0; pFile->pSst != NULL && i < taosArrayGetSize(pFile->pSst); i++) {
    char* sst = taosArrayGetP(pFile->pSst, i);
    taosMemoryFree(sst);
  }
  taosArrayDestroy(pFile->pSst);
  taosMemoryFree(pFile);

  taosArrayDestroy(handle->pFileList);
  taosCloseFile(&handle->fd);
  return;
}

int32_t streamSnapReaderOpen(void* pMeta, int64_t sver, int64_t ever, SStreamSnapReader** ppReader) {
  // impl later
  SStreamSnapReader* pReader = taosMemoryCalloc(1, sizeof(SStreamSnapReader));
  if (pReader == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  const char* path = NULL;
  if (streamSnapHandleInit(&pReader->handle, (char*)path) < 0) {
    return -1;
  }

  *ppReader = pReader;

  return 0;
}
int32_t streamSnapReaderClose(SStreamSnapReader* pReader) {
  if (pReader == NULL) return 0;

  streamSnapHandleDestroy(&pReader->handle);
  taosMemoryFree(pReader);
  return 0;
}
int32_t streamSnapRead(SStreamSnapReader* pReader, uint8_t** ppData, int64_t* size) {
  // impl later
  int32_t            code = 0;
  SStreamSnapHandle* pHandle = &pReader->handle;
  SBackendFileItem*  item = taosArrayGet(pHandle->pFileList, pHandle->currFileIdx);

  uint8_t* buf = taosMemoryCalloc(1, sizeof(SStreamSnapBlockHdr) + kBlockSize);
  int64_t  nread = taosPReadFile(pHandle->fd, buf + sizeof(SStreamSnapBlockHdr), kBlockSize, pHandle->offset);
  if (nread == -1) {
    code = TAOS_SYSTEM_ERROR(terrno);
    qError("stream snap failed to read snap, file name:%s, reason:%s", item->name, tstrerror(code));
    return code;
    // handle later
    return -1;
  } else if (nread <= kBlockSize) {
    // left bytes less than kBlockSize
    pHandle->offset += nread;
    if (pHandle->offset >= item->size || nread < kBlockSize) {
      taosCloseFile(&pHandle->fd);
      pHandle->currFileIdx += 1;
    }
  } else {
    if (pHandle->currFileIdx >= taosArrayGetSize(pHandle->pFileList)) {
      // finish
      return 0;
    }
    item = taosArrayGet(pHandle->pFileList, pHandle->currFileIdx);
    pHandle->fd = taosOpenFile(item->name, TD_FILE_READ);
    nread = taosPReadFile(pHandle->fd, buf + sizeof(SStreamSnapBlockHdr), kBlockSize, pHandle->offset);
    pHandle->offset += nread;
  }

  SStreamSnapBlockHdr* pHdr = (SStreamSnapBlockHdr*)buf;
  pHdr->size = nread;
  pHdr->type = item->type;
  pHdr->totalSize = item->size;

  memcpy(pHdr->name, item->name, strlen(item->name));
  pHandle->seraial += nread;

  *ppData = buf;
  *size = sizeof(SStreamSnapBlockHdr) + nread;
  return 0;
}
// SMetaSnapWriter ========================================
int32_t streamSnapWriterOpen(void* pMeta, int64_t sver, int64_t ever, SStreamSnapWriter** ppWriter) {
  // impl later
  SStreamSnapWriter* pWriter = taosMemoryCalloc(1, sizeof(SStreamSnapWriter));
  if (pWriter == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  SStreamSnapHandle* handle = &pWriter->handle;

  const char*    path = NULL;
  SBanckendFile* pFile = taosMemoryCalloc(1, sizeof(SBanckendFile));
  pFile->path = taosStrdup(path);
  SArray* list = taosArrayInit(64, sizeof(SBackendFileItem));

  SBackendFileItem item;
  item.name = taosStrdup((char*)ROCKSDB_CURRENT);
  item.type = ROCKSDB_CURRENT_TYPE;
  taosArrayPush(list, &item);

  handle->pBackendFile = pFile;

  handle->pFileList = list;
  handle->currFileIdx = 0;
  handle->offset = 0;
  handle->fd = taosOpenFile(taosArrayGet(handle->pFileList, handle->currFileIdx), TD_FILE_WRITE);
  *ppWriter = pWriter;
  return 0;
}

int32_t streamSnapWrite(SStreamSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t code = 0;

  SStreamSnapBlockHdr* pHdr = (SStreamSnapBlockHdr*)pData;
  SStreamSnapHandle*   handle = &pWriter->handle;
  SBackendFileItem*    pItem = taosArrayGetP(handle->pFileList, handle->currFileIdx);
  if (strlen(pHdr->name) == strlen(pItem->name) && strcmp(pHdr->name, pItem->name) == 0) {
    if (taosPWriteFile(handle->fd, pHdr->data, pHdr->size, handle->offset) != pHdr->size) {
      code = TAOS_SYSTEM_ERROR(terrno);
      qError("stream snap failed to write snap, file name:%s, reason:%s", pHdr->name, tstrerror(code));
      return code;
    }
    handle->offset += pHdr->size;
  } else {
    taosCloseFile(&handle->fd);

    SBackendFileItem item;
    item.name = taosStrdup(pHdr->name);
    item.type = pHdr->type;
    taosArrayPush(handle->pFileList, &item);

    handle->offset = 0;
    handle->currFileIdx += 1;
    handle->fd = taosOpenFile(taosArrayGet(handle->pFileList, taosArrayGetSize(handle->pFileList) - 1), TD_FILE_WRITE);

    taosPWriteFile(handle->fd, pHdr->data, pHdr->size, handle->offset);
    handle->offset += pHdr->size;
  }

  // impl later
  return 0;
}
int32_t streamSnapWriterClose(SStreamSnapWriter* pWriter, int8_t rollback) {
  SStreamSnapHandle* handle = &pWriter->handle;
  for (int i = 0; i < taosArrayGetSize(handle->pFileList); i++) {
    SBackendFileItem* item = taosArrayGet(handle->pFileList, i);
    taosMemoryFree(item->name);
  }

  streamSnapHandleDestroy(handle);
  taosMemoryFree(pWriter);

  return 0;
}
