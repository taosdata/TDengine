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
#include "streamBackendRocksdb.h"
#include "streamInt.h"

enum SBackendFileType {
  ROCKSDB_OPTIONS_TYPE = 1,
  ROCKSDB_MAINFEST_TYPE = 2,
  ROCKSDB_SST_TYPE = 3,
  ROCKSDB_CURRENT_TYPE = 4,
  ROCKSDB_CHECKPOINT_META_TYPE = 5,
  ROCKSDB_CHECKPOINT_SELFCHECK_TYPE = 6,
};

typedef struct SBackendFileItem {
  char*   name;
  int8_t  type;
  int64_t size;
  int8_t  ref;
} SBackendFileItem;

typedef struct SBackendFile {
  char*   pCurrent;
  char*   pMainfest;
  char*   pOptions;
  SArray* pSst;
  char*   pCheckpointMeta;
  char*   path;

} SBanckendFile;

typedef struct SBackendSnapFiles2 {
  char*   pCurrent;
  char*   pMainfest;
  char*   pOptions;
  SArray* pSst;
  char*   pCheckpointMeta;
  char*   pCheckpointSelfcheck;
  char*   path;

  int64_t         checkpointId;
  int64_t         seraial;
  int64_t         offset;
  TdFilePtr       fd;
  int8_t          filetype;
  SArray*         pFileList;
  int32_t         currFileIdx;
  SStreamTaskSnap snapInfo;
  int8_t          inited;

} SBackendSnapFile2;
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
  char*          metaPath;

  void*   pMeta;
  SArray* pDbSnapSet;
  SArray* pSnapInfoSet;
  int32_t currIdx;
  int8_t  delFlag;  // 0 : not del, 1: del
};
struct SStreamSnapBlockHdr {
  int8_t  type;
  int8_t  flag;
  int64_t index;
  // int64_t streamId;
  // int64_t taskId;
  SStreamTaskSnap snapInfo;
  char            name[128];
  int64_t         totalSize;
  int64_t         size;
  uint8_t         data[];
};
struct SStreamSnapReader {
  void*             pMeta;
  int64_t           sver;
  int64_t           ever;
  SStreamSnapHandle handle;
  int64_t           checkpointId;
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
const char*    ROCKSDB_CHECKPOINT_SELF_CHECK = "info";
static int64_t kBlockSize = 64 * 1024;

int32_t streamSnapHandleInit(SStreamSnapHandle* handle, char* path, void* pMeta);
void    streamSnapHandleDestroy(SStreamSnapHandle* handle);

// static void streamBuildFname(char* path, char* file, char* fullname)

#define STREAM_ROCKSDB_BUILD_FULLNAME(path, file, fullname) \
  do {                                                      \
    sprintf(fullname, "%s%s%s", path, TD_DIRSEP, file);     \
  } while (0)

int32_t streamGetFileSize(char* path, char* name, int64_t* sz) {
  int32_t ret = 0;

  char* fullname = taosMemoryCalloc(1, strlen(path) + 32);
  sprintf(fullname, "%s%s%s", path, TD_DIRSEP, name);

  ret = taosStatFile(fullname, sz, NULL, NULL);
  taosMemoryFree(fullname);

  return ret;
}

TdFilePtr streamOpenFile(char* path, char* name, int32_t opt) {
  char fullname[256] = {0};

  STREAM_ROCKSDB_BUILD_FULLNAME(path, name, fullname);
  return taosOpenFile(fullname, opt);
}

int32_t streamCreateTaskDbSnapInfo(void* arg, char* path, SArray* pSnap) { return taskDbBuildSnap(arg, pSnap); }

int32_t streamDestroyTaskDbSnapInfo(void* arg, SArray* snap) { return taskDbDestroySnap(arg, snap); }

void snapFileDebugInfo(SBackendSnapFile2* pSnapFile) {
  if (qDebugFlag & DEBUG_DEBUG) {
    int16_t cap = 512;

    char* buf = taosMemoryCalloc(1, cap);
    if (buf == NULL) {
      stError("%s failed to alloc memory, reason:%s", STREAM_STATE_TRANSFER, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
      return;
    }

    int32_t nBytes = snprintf(buf + strlen(buf), cap, "[");
    if (nBytes <= 0 || nBytes >= cap) {
      taosMemoryFree(buf);
      stError("%s failed to write buf, reason:%s", STREAM_STATE_TRANSFER, tstrerror(TSDB_CODE_OUT_OF_RANGE));
      return;
    }

    if (pSnapFile->pCurrent) sprintf(buf, "current: %s,", pSnapFile->pCurrent);
    if (pSnapFile->pMainfest) sprintf(buf + strlen(buf), "MANIFEST: %s,", pSnapFile->pMainfest);
    if (pSnapFile->pOptions) sprintf(buf + strlen(buf), "options: %s,", pSnapFile->pOptions);
    if (pSnapFile->pSst) {
      for (int32_t i = 0; i < taosArrayGetSize(pSnapFile->pSst); i++) {
        char* name = taosArrayGetP(pSnapFile->pSst, i);
        if (strlen(buf) + strlen(name) < cap) sprintf(buf + strlen(buf), "%s,", name);
      }
    }
    if ((strlen(buf)) < cap) sprintf(buf + strlen(buf) - 1, "]");

    stInfo("%s %" PRId64 "-%" PRId64 " get file list: %s", STREAM_STATE_TRANSFER, pSnapFile->snapInfo.streamId,
           pSnapFile->snapInfo.taskId, buf);
    taosMemoryFree(buf);
  }
}

int32_t snapFileGenMeta(SBackendSnapFile2* pSnapFile) {
  void*            p = NULL;
  SBackendFileItem item = {0};
  item.ref = 1;

  // current
  item.name = pSnapFile->pCurrent;
  item.type = ROCKSDB_CURRENT_TYPE;
  int32_t code = streamGetFileSize(pSnapFile->path, item.name, &item.size);
  if (code) {
    stError("failed to get file size");
    return code;
  }

  p = taosArrayPush(pSnapFile->pFileList, &item);
  if (p == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  // mainfest
  item.name = pSnapFile->pMainfest;
  item.type = ROCKSDB_MAINFEST_TYPE;
  code = streamGetFileSize(pSnapFile->path, item.name, &item.size);
  if (code) {
    return code;
  }

  p = taosArrayPush(pSnapFile->pFileList, &item);
  if (p == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  // options
  item.name = pSnapFile->pOptions;
  item.type = ROCKSDB_OPTIONS_TYPE;
  code = streamGetFileSize(pSnapFile->path, item.name, &item.size);
  if (code) {
    return code;
  }

  p = taosArrayPush(pSnapFile->pFileList, &item);
  if (p == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  // sst
  for (int32_t i = 0; i < taosArrayGetSize(pSnapFile->pSst); i++) {
    char* sst = taosArrayGetP(pSnapFile->pSst, i);
    item.name = sst;
    item.type = ROCKSDB_SST_TYPE;
    code = streamGetFileSize(pSnapFile->path, item.name, &item.size);
    if (code) {
      return code;
    }

    p = taosArrayPush(pSnapFile->pFileList, &item);
    if (p == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  // meta
  item.name = pSnapFile->pCheckpointMeta;
  item.type = ROCKSDB_CHECKPOINT_META_TYPE;
  if (streamGetFileSize(pSnapFile->path, item.name, &item.size) == 0) {
    p = taosArrayPush(pSnapFile->pFileList, &item);
    if (p == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  item.name = pSnapFile->pCheckpointSelfcheck;
  item.type = ROCKSDB_CHECKPOINT_SELFCHECK_TYPE;

  if (streamGetFileSize(pSnapFile->path, item.name, &item.size) == 0) {
    p = taosArrayPush(pSnapFile->pFileList, &item);
    if (p == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return 0;
}

int32_t snapFileReadMeta(SBackendSnapFile2* pSnapFile) {
  int32_t  code = 0;
  TdDirPtr pDir = taosOpenDir(pSnapFile->path);
  if (NULL == pDir) {
    code = TAOS_SYSTEM_ERROR(errno);
    stError("%s failed to open %s, reason:%s", STREAM_STATE_TRANSFER, pSnapFile->path, tstrerror(code));
    return code;
  }

  TdDirEntryPtr pDirEntry;
  while ((pDirEntry = taosReadDir(pDir)) != NULL) {
    char* name = taosGetDirEntryName(pDirEntry);
    if (strlen(name) >= strlen(ROCKSDB_CURRENT) && 0 == strncmp(name, ROCKSDB_CURRENT, strlen(ROCKSDB_CURRENT))) {
      pSnapFile->pCurrent = taosStrdup(name);
      if (pSnapFile->pCurrent == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      continue;
    }
    if (strlen(name) >= strlen(ROCKSDB_MAINFEST) && 0 == strncmp(name, ROCKSDB_MAINFEST, strlen(ROCKSDB_MAINFEST))) {
      pSnapFile->pMainfest = taosStrdup(name);
      if (pSnapFile->pMainfest == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      continue;
    }
    if (strlen(name) >= strlen(ROCKSDB_OPTIONS) && 0 == strncmp(name, ROCKSDB_OPTIONS, strlen(ROCKSDB_OPTIONS))) {
      pSnapFile->pOptions = taosStrdup(name);
      if (pSnapFile->pOptions == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      continue;
    }
    if (strlen(name) >= strlen(ROCKSDB_CHECKPOINT_META) &&
        0 == strncmp(name, ROCKSDB_CHECKPOINT_META, strlen(ROCKSDB_CHECKPOINT_META))) {
      pSnapFile->pCheckpointMeta = taosStrdup(name);
      if (pSnapFile->pCheckpointMeta == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      continue;
    }
    if (strlen(name) >= strlen(ROCKSDB_CHECKPOINT_SELF_CHECK) &&
        0 == strncmp(name, ROCKSDB_CHECKPOINT_SELF_CHECK, strlen(ROCKSDB_CHECKPOINT_SELF_CHECK))) {
      pSnapFile->pCheckpointSelfcheck = taosStrdup(name);
      if (pSnapFile->pCheckpointSelfcheck == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      continue;
    }
    if (strlen(name) >= strlen(ROCKSDB_SST) &&
        0 == strncmp(name + strlen(name) - strlen(ROCKSDB_SST), ROCKSDB_SST, strlen(ROCKSDB_SST))) {
      char* sst = taosStrdup(name);
      if (sst == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }

      void* p = taosArrayPush(pSnapFile->pSst, &sst);
      if (p == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
    }
  }

  return taosCloseDir(&pDir);
}

int32_t streamBackendSnapInitFile(char* metaPath, SStreamTaskSnap* pSnap, SBackendSnapFile2* pSnapFile) {
  int32_t code = 0;
  int32_t nBytes = 0;
  int32_t cap = strlen(pSnap->dbPrefixPath) + 256;

  char* path = taosMemoryCalloc(1, cap);
  if (path == NULL) {
    return terrno;
  }

  nBytes = snprintf(path, cap, "%s%s%s%s%s%" PRId64 "", pSnap->dbPrefixPath, TD_DIRSEP, "checkpoints", TD_DIRSEP,
                    "checkpoint", pSnap->chkpId);
  if (nBytes <= 0 || nBytes >= cap) {
    code = TSDB_CODE_OUT_OF_RANGE;
    goto _ERROR;
  }

  if (!taosIsDir(path)) {
    code = TSDB_CODE_INVALID_MSG;
    goto _ERROR;
  }

  pSnapFile->pSst = taosArrayInit(16, sizeof(void*));
  pSnapFile->pFileList = taosArrayInit(64, sizeof(SBackendFileItem));
  if (pSnapFile->pSst == NULL || pSnapFile->pFileList == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _ERROR;
  }

  pSnapFile->path = path;
  pSnapFile->snapInfo = *pSnap;
  if ((code = snapFileReadMeta(pSnapFile)) != 0) {
    goto _ERROR;
  }
  if ((code = snapFileGenMeta(pSnapFile)) != 0) {
    goto _ERROR;
  }

  snapFileDebugInfo(pSnapFile);
  path = NULL;

_ERROR:
  taosMemoryFree(path);
  return code;
}
void snapFileDestroy(SBackendSnapFile2* pSnap) {
  taosMemoryFree(pSnap->pCheckpointMeta);
  taosMemoryFree(pSnap->pCurrent);
  taosMemoryFree(pSnap->pMainfest);
  taosMemoryFree(pSnap->pOptions);
  taosMemoryFree(pSnap->path);
  taosMemoryFree(pSnap->pCheckpointSelfcheck);
  for (int32_t i = 0; i < taosArrayGetSize(pSnap->pSst); i++) {
    char* sst = taosArrayGetP(pSnap->pSst, i);
    taosMemoryFree(sst);
  }
  // unite read/write snap file
  for (int32_t i = 0; i < taosArrayGetSize(pSnap->pFileList); i++) {
    SBackendFileItem* pItem = taosArrayGet(pSnap->pFileList, i);
    if (pItem == NULL) {
      continue;
    }

    if (pItem->ref == 0) {
      taosMemoryFree(pItem->name);
    }
  }
  taosArrayDestroy(pSnap->pFileList);
  taosArrayDestroy(pSnap->pSst);
  int32_t code = taosCloseFile(&pSnap->fd);
  if (code) {
    stError("failed to close snapshot fd");
  }
}

int32_t streamSnapHandleInit(SStreamSnapHandle* pHandle, char* path, void* pMeta) {
  int32_t code = 0;
  SArray* pDbSnapSet = NULL;

  SArray* pSnapInfoSet = taosArrayInit(4, sizeof(SStreamTaskSnap));
  if (pSnapInfoSet == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  code = streamCreateTaskDbSnapInfo(pMeta, path, pSnapInfoSet);
  if (code != 0) {
    stError("failed to do task db snap info, reason:%s", tstrerror(code));
    goto _err;
  }

  pDbSnapSet = taosArrayInit(8, sizeof(SBackendSnapFile2));
  if (pDbSnapSet == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pSnapInfoSet); i++) {
    SStreamTaskSnap* pSnap = taosArrayGet(pSnapInfoSet, i);

    SBackendSnapFile2 snapFile = {0};
    code = streamBackendSnapInitFile(path, pSnap, &snapFile);
    if (code) {
      goto _err;
    }

    void* p = taosArrayPush(pDbSnapSet, &snapFile);
    if (p == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  pHandle->pDbSnapSet = pDbSnapSet;
  pHandle->pSnapInfoSet = pSnapInfoSet;
  pHandle->currIdx = 0;
  pHandle->pMeta = pMeta;

  return code;

_err:
  taosArrayDestroy(pSnapInfoSet);
  taosArrayDestroy(pDbSnapSet);
  streamSnapHandleDestroy(pHandle);
  return code;
}

void streamSnapHandleDestroy(SStreamSnapHandle* handle) {
  if (handle->pDbSnapSet) {
    for (int32_t i = 0; i < taosArrayGetSize(handle->pDbSnapSet); i++) {
      SBackendSnapFile2* pSnapFile = taosArrayGet(handle->pDbSnapSet, i);
      snapFileDebugInfo(pSnapFile);
      snapFileDestroy(pSnapFile);
    }
    taosArrayDestroy(handle->pDbSnapSet);
  }

  (void) streamDestroyTaskDbSnapInfo(handle->pMeta, handle->pSnapInfoSet);
  if (handle->pSnapInfoSet) {
    for (int32_t i = 0; i < taosArrayGetSize(handle->pSnapInfoSet); i++) {
      SStreamTaskSnap* pSnap = taosArrayGet(handle->pSnapInfoSet, i);
      taosMemoryFree(pSnap->dbPrefixPath);
    }
    taosArrayDestroy(handle->pSnapInfoSet);
  }

  taosMemoryFree(handle->metaPath);
}

int32_t streamSnapReaderOpen(void* pMeta, int64_t sver, int64_t chkpId, char* path, SStreamSnapReader** ppReader) {
  // impl later
  SStreamSnapReader* pReader = taosMemoryCalloc(1, sizeof(SStreamSnapReader));
  if (pReader == NULL) {
    return terrno;
  }

  int32_t code = streamSnapHandleInit(&pReader->handle, (char*)path, pMeta);
  if (code != 0) {
    taosMemoryFree(pReader);
    return code;
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
  int32_t            idx = pHandle->currIdx;

  SBackendSnapFile2* pSnapFile = taosArrayGet(pHandle->pDbSnapSet, idx);
  if (pSnapFile == NULL) {
    return 0;
  }
  SBackendFileItem* item = NULL;

_NEXT:

  if (pSnapFile->fd == NULL) {
    if (pSnapFile->currFileIdx >= taosArrayGetSize(pSnapFile->pFileList)) {
      if (pHandle->currIdx + 1 < taosArrayGetSize(pHandle->pDbSnapSet)) {
        pHandle->currIdx += 1;

        pSnapFile = taosArrayGet(pHandle->pDbSnapSet, pHandle->currIdx);
        goto _NEXT;
      } else {
        *ppData = NULL;
        *size = 0;
        return 0;
      }

    } else {
      item = taosArrayGet(pSnapFile->pFileList, pSnapFile->currFileIdx);
      pSnapFile->fd = streamOpenFile(pSnapFile->path, item->name, TD_FILE_READ);
      stDebug("%s open file %s, current offset:%" PRId64 ", size:% " PRId64 ", file no.%d", STREAM_STATE_TRANSFER,
              item->name, (int64_t)pSnapFile->offset, item->size, pSnapFile->currFileIdx);
    }
  }
  item = taosArrayGet(pSnapFile->pFileList, pSnapFile->currFileIdx);

  stDebug("%s start to read file %s, current offset:%" PRId64 ", size:%" PRId64
          ", file no.%d, total set:%d, current set idx: %d",
          STREAM_STATE_TRANSFER, item->name, (int64_t)pSnapFile->offset, item->size, pSnapFile->currFileIdx,
          (int32_t)taosArrayGetSize(pHandle->pDbSnapSet), pHandle->currIdx);

  uint8_t* buf = taosMemoryCalloc(1, sizeof(SStreamSnapBlockHdr) + kBlockSize);
  int64_t  nread = taosPReadFile(pSnapFile->fd, buf + sizeof(SStreamSnapBlockHdr), kBlockSize, pSnapFile->offset);
  if (nread == -1) {
    taosMemoryFree(buf);
    code = TAOS_SYSTEM_ERROR(errno);
    stError("%s snap failed to read snap, file name:%s, type:%d,reason:%s", STREAM_STATE_TRANSFER, item->name,
            item->type, tstrerror(code));
    return code;
  } else if (nread > 0 && nread <= kBlockSize) {
    // left bytes less than kBlockSize
    stDebug("%s read file %s, current offset:%" PRId64 ",size:% " PRId64 ", file no.%d", STREAM_STATE_TRANSFER,
            item->name, (int64_t)pSnapFile->offset, item->size, pSnapFile->currFileIdx);
    pSnapFile->offset += nread;
    if (pSnapFile->offset >= item->size || nread < kBlockSize) {
      code = taosCloseFile(&pSnapFile->fd);
      if (code) {
        stError("failed to close snapshot fd");
      }

      pSnapFile->offset = 0;
      pSnapFile->currFileIdx += 1;
    }
  } else {
    stDebug("%s no data read, close file no.%d, move to next file, open and read", STREAM_STATE_TRANSFER,
            pSnapFile->currFileIdx);
    code = taosCloseFile(&pSnapFile->fd);
    if (code) {
      stError("failed to close snapshot fd");
    }

    pSnapFile->offset = 0;
    pSnapFile->currFileIdx += 1;

    if (pSnapFile->currFileIdx >= taosArrayGetSize(pSnapFile->pFileList)) {
      // finish
      if (pHandle->currIdx + 1 < taosArrayGetSize(pHandle->pDbSnapSet)) {
        // skip to next snap set
        pHandle->currIdx += 1;
        pSnapFile = taosArrayGet(pHandle->pDbSnapSet, pHandle->currIdx);
        goto _NEXT;
      } else {
        taosMemoryFree(buf);
        *ppData = NULL;
        *size = 0;
        return 0;
      }
    }
    item = taosArrayGet(pSnapFile->pFileList, pSnapFile->currFileIdx);
    pSnapFile->fd = streamOpenFile(pSnapFile->path, item->name, TD_FILE_READ);

    nread = taosPReadFile(pSnapFile->fd, buf + sizeof(SStreamSnapBlockHdr), kBlockSize, pSnapFile->offset);
    pSnapFile->offset += nread;

    stDebug("%s open file and read file %s, current offset:%" PRId64 ", size:% " PRId64 ", file no.%d",
            STREAM_STATE_TRANSFER, item->name, (int64_t)pSnapFile->offset, item->size, pSnapFile->currFileIdx);
  }

  SStreamSnapBlockHdr* pHdr = (SStreamSnapBlockHdr*)buf;
  pHdr->size = nread;
  pHdr->type = item->type;
  pHdr->totalSize = item->size;
  pHdr->snapInfo = pSnapFile->snapInfo;

  int32_t len = TMIN(strlen(item->name), tListLen(pHdr->name));
  memcpy(pHdr->name, item->name, len);

  pSnapFile->seraial += nread;

  *ppData = buf;
  *size = sizeof(SStreamSnapBlockHdr) + nread;
  return 0;
}
// SMetaSnapWriter ========================================
int32_t streamSnapWriterOpen(void* pMeta, int64_t sver, int64_t ever, char* path, SStreamSnapWriter** ppWriter) {
  // impl later
  int32_t            code = 0;
  SStreamSnapWriter* pWriter = taosMemoryCalloc(1, sizeof(SStreamSnapWriter));
  if (pWriter == NULL) {
    return terrno;
  }

  SStreamSnapHandle* pHandle = &pWriter->handle;
  pHandle->currIdx = 0;

  pHandle->metaPath = taosStrdup(path);
  if (pHandle->metaPath == NULL) {
    taosMemoryFree(pWriter);
    code = TSDB_CODE_OUT_OF_MEMORY;
    return code;
  }

  pHandle->pDbSnapSet = taosArrayInit(8, sizeof(SBackendSnapFile2));
  if (pHandle->pDbSnapSet == NULL) {
    int32_t c = streamSnapWriterClose(pWriter, 0);  // not override the error code, and igore this error code
    if (c) {
      stError("failed close snaphost writer");
    }

    code = TSDB_CODE_OUT_OF_MEMORY;
    return code;
  }

  SBackendSnapFile2 snapFile = {0};
  if (taosArrayPush(pHandle->pDbSnapSet, &snapFile) == NULL) {
    int32_t c = streamSnapWriterClose(pWriter, 0);
    if (c) {
      stError("failed close snaphost writer");
    }

    code = TSDB_CODE_OUT_OF_MEMORY;
    return code;
  }

  *ppWriter = pWriter;
  return 0;
}

int32_t snapInfoEqual(SStreamTaskSnap* a, SStreamTaskSnap* b) {
  if (a->streamId != b->streamId || a->taskId != b->taskId || a->chkpId != b->chkpId) {
    return 0;
  }
  return 1;
}

int32_t streamSnapWriteImpl(SStreamSnapWriter* pWriter, uint8_t* pData, uint32_t nData, SBackendSnapFile2* pSnapFile) {
  int32_t              code = -1;
  SStreamSnapBlockHdr* pHdr = (SStreamSnapBlockHdr*)pData;
  SStreamSnapHandle*   pHandle = &pWriter->handle;
  SBackendFileItem*    pItem = taosArrayGet(pSnapFile->pFileList, pSnapFile->currFileIdx);

  if (pSnapFile->fd == 0) {
    pSnapFile->fd = streamOpenFile(pSnapFile->path, pItem->name, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
    if (pSnapFile->fd == NULL) {
      code = TAOS_SYSTEM_ERROR(errno);
      stError("%s failed to open file name:%s%s%s, reason:%s", STREAM_STATE_TRANSFER, pHandle->metaPath, TD_DIRSEP,
              pHdr->name, tstrerror(code));
    }
  }

  if (strlen(pHdr->name) == strlen(pItem->name) && strcmp(pHdr->name, pItem->name) == 0) {
    int64_t bytes = taosPWriteFile(pSnapFile->fd, pHdr->data, pHdr->size, pSnapFile->offset);
    if (bytes != pHdr->size) {
      code = TAOS_SYSTEM_ERROR(errno);
      stError("%s failed to write snap, file name:%s, reason:%s", STREAM_STATE_TRANSFER, pHdr->name, tstrerror(code));
      goto _err;
    } else {
      stInfo("succ to write data %s", pItem->name);
    }
    pSnapFile->offset += bytes;
  } else {
    code = taosCloseFile(&pSnapFile->fd);
    if (code) {
      stError("failed to close snapshot fd");
    }

    pSnapFile->offset = 0;
    pSnapFile->currFileIdx += 1;

    SBackendFileItem item = {0};
    item.name = taosStrdup(pHdr->name);
    item.type = pHdr->type;
    if (item.name == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    void* p = taosArrayPush(pSnapFile->pFileList, &item);
    if (p == NULL) {  // can NOT goto _err here.
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    SBackendFileItem* pItem2 = taosArrayGet(pSnapFile->pFileList, pSnapFile->currFileIdx);
    pSnapFile->fd = streamOpenFile(pSnapFile->path, pItem2->name, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
    if (pSnapFile->fd == NULL) {
      code = TAOS_SYSTEM_ERROR(errno);
      stError("%s failed to open file name:%s%s%s, reason:%s", STREAM_STATE_TRANSFER, pSnapFile->path, TD_DIRSEP,
              pHdr->name, tstrerror(code));
      return code;
    }

    // open fd again, let's close fd during handle errors.
    if (taosPWriteFile(pSnapFile->fd, pHdr->data, pHdr->size, pSnapFile->offset) != pHdr->size) {
      code = TAOS_SYSTEM_ERROR(errno);
      stError("%s failed to write snap, file name:%s, reason:%s", STREAM_STATE_TRANSFER, pHdr->name, tstrerror(code));
      goto _err;
    }

    stInfo("succ to write data %s", pItem2->name);
    pSnapFile->offset += pHdr->size;
  }

  return TSDB_CODE_SUCCESS;

_err:
  (void) taosCloseFile(&pSnapFile->fd);
  return code;
}

int32_t streamSnapWrite(SStreamSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t code = 0;

  SStreamSnapBlockHdr* pHdr = (SStreamSnapBlockHdr*)pData;
  SStreamSnapHandle*   pHandle = &pWriter->handle;
  SStreamTaskSnap      snapInfo = pHdr->snapInfo;

  SBackendSnapFile2* pDbSnapFile = taosArrayGet(pHandle->pDbSnapSet, pHandle->currIdx);
  if (pDbSnapFile->inited == 0) {
    char idstr[64] = {0};
    sprintf(idstr, "0x%" PRIx64 "-0x%x", snapInfo.streamId, (int32_t)(snapInfo.taskId));

    char* path = taosMemoryCalloc(1, strlen(pHandle->metaPath) + 256);
    sprintf(path, "%s%s%s%s%s%s%s%" PRId64 "", pHandle->metaPath, TD_DIRSEP, idstr, TD_DIRSEP, "checkpoints", TD_DIRSEP,
            "checkpoint", snapInfo.chkpId);
    if (!taosIsDir(path)) {
      code = taosMulMkDir(path);
      stInfo("%s mkdir %s", STREAM_STATE_TRANSFER, path);
      if (code) {
        stError("s-task:0x%x failed to mkdir:%s", (int32_t) snapInfo.taskId, path);
        return code;
      }
    }

    pDbSnapFile->path = path;
    pDbSnapFile->snapInfo = snapInfo;
    pDbSnapFile->pFileList = taosArrayInit(64, sizeof(SBackendFileItem));
    pDbSnapFile->currFileIdx = 0;
    pDbSnapFile->offset = 0;

    SBackendFileItem item = {0};
    item.name = taosStrdup((char*)ROCKSDB_CURRENT);
    item.type = ROCKSDB_CURRENT_TYPE;

    void* p = taosArrayPush(pDbSnapFile->pFileList, &item);
    if (p == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    pDbSnapFile->inited = 1;
    return streamSnapWriteImpl(pWriter, pData, nData, pDbSnapFile);
  } else {
    if (snapInfoEqual(&snapInfo, &pDbSnapFile->snapInfo)) {
      return streamSnapWriteImpl(pWriter, pData, nData, pDbSnapFile);
    } else {
      SBackendSnapFile2 snapFile = {0};
      void* p = taosArrayPush(pHandle->pDbSnapSet, &snapFile);
      if (p == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      pHandle->currIdx += 1;
      return streamSnapWrite(pWriter, pData, nData);
    }
  }
}

int32_t streamSnapWriterClose(SStreamSnapWriter* pWriter, int8_t rollback) {
  if (pWriter == NULL) return 0;
  streamSnapHandleDestroy(&pWriter->handle);
  taosMemoryFree(pWriter);

  return 0;
}
