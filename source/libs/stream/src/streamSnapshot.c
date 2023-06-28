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
  int64_t totalSize;
  int64_t size;
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

// static void streamBuildFname(char* path, char* file, char* fullname)

#define STREAM_ROCKSDB_BUILD_FULLNAME(path, file, fullname) \
  do {                                                      \
    sprintf(fullname, "%s/%s", path, file);                 \
  } while (0)

int32_t streamSnapHandleInit(SStreamSnapHandle* pHandle, char* path) {
  // impl later
  int32_t code = 0;

  TdDirPtr pDir = taosOpenDir(path);
  if (NULL == pDir) {
    goto _err;
  }

  SBanckendFile* pFile = taosMemoryCalloc(1, sizeof(SBanckendFile));
  pHandle->checkpointId = 0;
  pHandle->seraial = 0;
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

  pHandle->pBackendFile = pFile;

  pHandle->currFileIdx = 0;
  pHandle->pFileList = list;

  char  fullname[256] = {0};
  char* file = taosArrayGet(pHandle->pFileList, pHandle->currFileIdx);
  STREAM_ROCKSDB_BUILD_FULLNAME(pFile->path, file, fullname);

  pHandle->fd = taosOpenFile(fullname, TD_FILE_READ);
  if (pHandle->fd == NULL) {
    goto _err;
  }
  pHandle->seraial = 0;
  pHandle->offset = 0;
  return 0;
_err:
  streamSnapHandleDestroy(pHandle);

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

int32_t streamSnapReaderOpen(void* pMeta, int64_t sver, int64_t ever, char* path, SStreamSnapReader** ppReader) {
  // impl later
  SStreamSnapReader* pReader = taosMemoryCalloc(1, sizeof(SStreamSnapReader));
  if (pReader == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  // const char* path = NULL;
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
  SBanckendFile*     pFile = pHandle->pBackendFile;

  SBackendFileItem* item = taosArrayGet(pHandle->pFileList, pHandle->currFileIdx);

  uint8_t* buf = taosMemoryCalloc(1, sizeof(SStreamSnapBlockHdr) + kBlockSize);
  int64_t  nread = taosPReadFile(pHandle->fd, buf + sizeof(SStreamSnapBlockHdr), kBlockSize, pHandle->offset);
  if (nread == -1) {
    code = TAOS_SYSTEM_ERROR(terrno);
    qError("stream snap failed to read snap, file name:%s, reason:%s", item->name, tstrerror(code));
    return code;
    // handle later
    return -1;
  } else if (nread > 0 && nread <= kBlockSize) {
    // left bytes less than kBlockSize
    pHandle->offset += nread;
    if (pHandle->offset >= item->size || nread < kBlockSize) {
      taosCloseFile(&pHandle->fd);
      pHandle->offset = 0;
      pHandle->currFileIdx += 1;
    }
  } else {
    if (pHandle->currFileIdx >= taosArrayGetSize(pHandle->pFileList)) {
      // finish
      *ppData = NULL;
      *size = 0;
      return 0;
    }
    item = taosArrayGet(pHandle->pFileList, pHandle->currFileIdx);
    char fullname[256] = {0};
    STREAM_ROCKSDB_BUILD_FULLNAME(pFile->path, item->name, fullname);
    pHandle->fd = taosOpenFile(fullname, TD_FILE_READ);

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
int32_t streamSnapWriterOpen(void* pMeta, int64_t sver, int64_t ever, char* path, SStreamSnapWriter** ppWriter) {
  // impl later
  SStreamSnapWriter* pWriter = taosMemoryCalloc(1, sizeof(SStreamSnapWriter));
  if (pWriter == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  SStreamSnapHandle* pHandle = &pWriter->handle;

  SBanckendFile* pFile = taosMemoryCalloc(1, sizeof(SBanckendFile));
  pFile->path = taosStrdup(path);
  SArray* list = taosArrayInit(64, sizeof(SBackendFileItem));

  SBackendFileItem item;
  item.name = taosStrdup((char*)ROCKSDB_CURRENT);
  item.type = ROCKSDB_CURRENT_TYPE;
  taosArrayPush(list, &item);

  pHandle->pBackendFile = pFile;

  pHandle->pFileList = list;
  pHandle->currFileIdx = 0;
  pHandle->offset = 0;
  pHandle->fd = taosOpenFile(taosArrayGet(pHandle->pFileList, pHandle->currFileIdx), TD_FILE_WRITE);
  *ppWriter = pWriter;
  return 0;
}

int32_t streamSnapWrite(SStreamSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t code = 0;

  SStreamSnapBlockHdr* pHdr = (SStreamSnapBlockHdr*)pData;
  SStreamSnapHandle*   pHandle = &pWriter->handle;
  SBanckendFile*       pFile = pHandle->pBackendFile;
  SBackendFileItem*    pItem = taosArrayGetP(pHandle->pFileList, pHandle->currFileIdx);
  if (strlen(pHdr->name) == strlen(pItem->name) && strcmp(pHdr->name, pItem->name) == 0) {
    if (taosPWriteFile(pHandle->fd, pHdr->data, pHdr->size, pHandle->offset) != pHdr->size) {
      code = TAOS_SYSTEM_ERROR(terrno);
      qError("stream snap failed to write snap, file name:%s, reason:%s", pHdr->name, tstrerror(code));
      return code;
    }
    pHandle->offset += pHdr->size;
  } else {
    taosCloseFile(&pHandle->fd);
    pHandle->offset = 0;
    pHandle->currFileIdx += 1;

    SBackendFileItem item;
    item.name = taosStrdup(pHdr->name);
    item.type = pHdr->type;
    taosArrayPush(pHandle->pFileList, &item);

    char  fullname[256] = {0};
    char* name = ((SBackendFileItem*)taosArrayGet(pHandle->pFileList, taosArrayGetSize(pHandle->pFileList) - 1))->name;
    STREAM_ROCKSDB_BUILD_FULLNAME(pFile->path, name, fullname);
    pHandle->fd = taosOpenFile(fullname, TD_FILE_WRITE);

    taosPWriteFile(pHandle->fd, pHdr->data, pHdr->size, pHandle->offset);
    pHandle->offset += pHdr->size;
  }

  // impl later
  return 0;
}
int32_t streamSnapWriterClose(SStreamSnapWriter* pWriter, int8_t rollback) {
  SStreamSnapHandle* handle = &pWriter->handle;
  if (qDebugFlag & DEBUG_DEBUG) {
    char* buf = (char*)taosMemoryMalloc(1024);
    int   n = sprintf(buf, "[");
    for (int i = 0; i < taosArrayGetSize(handle->pFileList); i++) {
      SBackendFileItem* item = taosArrayGet(handle->pFileList, i);
      if (i != taosArrayGetSize(handle->pFileList) - 1) {
        n += sprintf(buf + n, "%s %" PRId64 ",", item->name, item->size);
      } else {
        n += sprintf(buf + n, "%s %" PRId64 "]", item->name, item->size);
      }
    }
    qDebug("stream snap get file list, %s", buf);
    taosMemoryFree(buf);
  }
  for (int i = 0; i < taosArrayGetSize(handle->pFileList); i++) {
    SBackendFileItem* item = taosArrayGet(handle->pFileList, i);
    taosMemoryFree(item->name);
  }

  streamSnapHandleDestroy(handle);
  taosMemoryFree(pWriter);

  return 0;
}
