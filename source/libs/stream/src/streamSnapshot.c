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
#include "streamBackendRocksdb.h"
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
  int8_t         delFlag;
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
static int64_t kBlockSize = 64 * 1024;

int32_t streamSnapHandleInit(SStreamSnapHandle* handle, char* path, int64_t chkpId, void* pMeta);
void    streamSnapHandleDestroy(SStreamSnapHandle* handle);

// static void streamBuildFname(char* path, char* file, char* fullname)

#define STREAM_ROCKSDB_BUILD_FULLNAME(path, file, fullname) \
  do {                                                      \
    sprintf(fullname, "%s%s%s", path, TD_DIRSEP, file);     \
  } while (0)

int32_t streamGetFileSize(char* path, char* name, int64_t* sz) {
  int ret = 0;

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

int32_t streamSnapHandleInit(SStreamSnapHandle* pHandle, char* path, int64_t chkpId, void* pMeta) {
  // impl later
  int   len = strlen(path);
  char* tdir = taosMemoryCalloc(1, len + 256);
  memcpy(tdir, path, len);

  int32_t code = 0;

  int8_t validChkp = 0;
  if (chkpId != 0) {
    sprintf(tdir, "%s%s%s%s%s%scheckpoint%" PRId64 "", path, TD_DIRSEP, "stream", TD_DIRSEP, "checkpoints", TD_DIRSEP,
            chkpId);
    if (taosIsDir(tdir)) {
      validChkp = 1;
      qInfo("%s start to read snap %s", STREAM_STATE_TRANSFER, tdir);
      streamBackendAddInUseChkp(pMeta, chkpId);
    } else {
      qWarn("%s failed to read from %s, reason: dir not exist,retry to default state dir", STREAM_STATE_TRANSFER, tdir);
    }
  }

  // no checkpoint specified or not exists invalid checkpoint, do checkpoint at default path and translate it
  if (validChkp == 0) {
    sprintf(tdir, "%s%s%s%s%s", path, TD_DIRSEP, "stream", TD_DIRSEP, "state");
    char* chkpdir = taosMemoryCalloc(1, len + 256);
    sprintf(chkpdir, "%s%s%s", tdir, TD_DIRSEP, "tmp");
    taosMemoryFree(tdir);

    tdir = chkpdir;
    qInfo("%s start to trigger checkpoint on %s", STREAM_STATE_TRANSFER, tdir);

    code = streamBackendTriggerChkp(pMeta, tdir);
    if (code != 0) {
      qError("%s failed to trigger chekckpoint at %s", STREAM_STATE_TRANSFER, tdir);
      taosMemoryFree(tdir);
      return code;
    }
    pHandle->delFlag = 1;
    chkpId = 0;
  }

  qInfo("%s start to read dir: %s", STREAM_STATE_TRANSFER, tdir);

  TdDirPtr pDir = taosOpenDir(tdir);
  if (NULL == pDir) {
    qError("%s failed to open %s", STREAM_STATE_TRANSFER, tdir);
    goto _err;
  }

  SBanckendFile* pFile = taosMemoryCalloc(1, sizeof(SBanckendFile));
  pHandle->pBackendFile = pFile;
  pHandle->checkpointId = chkpId;
  pHandle->seraial = 0;

  pFile->path = tdir;
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
      pFile->pOptions = taosStrdup(name);
      continue;
    }
    if (strlen(name) >= strlen(ROCKSDB_CHECKPOINT_META) &&
        0 == strncmp(name, ROCKSDB_CHECKPOINT_META, strlen(ROCKSDB_CHECKPOINT_META))) {
      pFile->pCheckpointMeta = taosStrdup(name);
      continue;
    }
    if (strlen(name) >= strlen(ROCKSDB_SST) &&
        0 == strncmp(name + strlen(name) - strlen(ROCKSDB_SST), ROCKSDB_SST, strlen(ROCKSDB_SST))) {
      char* sst = taosStrdup(name);
      taosArrayPush(pFile->pSst, &sst);
    }
  }
  if (qDebugFlag & DEBUG_TRACE) {
    char* buf = (char*)taosMemoryMalloc(128 + taosArrayGetSize(pFile->pSst) * 16);
    sprintf(buf, "[current: %s,", pFile->pCurrent);
    sprintf(buf + strlen(buf), "MANIFEST: %s,", pFile->pMainfest);
    sprintf(buf + strlen(buf), "options: %s,", pFile->pOptions);

    for (int i = 0; i < taosArrayGetSize(pFile->pSst); i++) {
      char* name = taosArrayGetP(pFile->pSst, i);
      sprintf(buf + strlen(buf), "%s,", name);
    }
    sprintf(buf + strlen(buf) - 1, "]");

    qInfo("%s get file list: %s", STREAM_STATE_TRANSFER, buf);
    taosMemoryFree(buf);
  }

  taosCloseDir(&pDir);

  if (pFile->pCurrent == NULL) {
    qError("%s failed to open %s, reason: no valid file", STREAM_STATE_TRANSFER, tdir);
    code = -1;
    tdir = NULL;
    goto _err;
  }
  SArray* list = taosArrayInit(64, sizeof(SBackendFileItem));

  SBackendFileItem item;
  // current
  item.name = pFile->pCurrent;
  item.type = ROCKSDB_CURRENT_TYPE;
  streamGetFileSize(pFile->path, item.name, &item.size);
  taosArrayPush(list, &item);

  // mainfest
  item.name = pFile->pMainfest;
  item.type = ROCKSDB_MAINFEST_TYPE;
  streamGetFileSize(pFile->path, item.name, &item.size);
  taosArrayPush(list, &item);

  // options
  item.name = pFile->pOptions;
  item.type = ROCKSDB_OPTIONS_TYPE;
  streamGetFileSize(pFile->path, item.name, &item.size);
  taosArrayPush(list, &item);
  // sst
  for (int i = 0; i < taosArrayGetSize(pFile->pSst); i++) {
    char* sst = taosArrayGetP(pFile->pSst, i);
    item.name = sst;
    item.type = ROCKSDB_SST_TYPE;
    streamGetFileSize(pFile->path, item.name, &item.size);
    taosArrayPush(list, &item);
  }
  // meta
  item.name = pFile->pCheckpointMeta;
  item.type = ROCKSDB_CHECKPOINT_META_TYPE;
  if (streamGetFileSize(pFile->path, item.name, &item.size) == 0) {
    taosArrayPush(list, &item);
  }

  pHandle->pBackendFile = pFile;

  pHandle->currFileIdx = 0;
  pHandle->pFileList = list;
  pHandle->seraial = 0;
  pHandle->offset = 0;
  pHandle->handle = pMeta;
  return 0;
_err:
  streamSnapHandleDestroy(pHandle);
  taosMemoryFreeClear(tdir);

  code = -1;
  return code;
}

void streamSnapHandleDestroy(SStreamSnapHandle* handle) {
  SBanckendFile* pFile = handle->pBackendFile;

  if (handle->checkpointId == 0) {
    // del tmp dir
    if (pFile && taosIsDir(pFile->path)) {
      if (handle->delFlag) taosRemoveDir(pFile->path);
    }
  } else {
    streamBackendDelInUseChkp(handle->handle, handle->checkpointId);
  }
  if (pFile) {
    taosMemoryFree(pFile->pCheckpointMeta);
    taosMemoryFree(pFile->pCurrent);
    taosMemoryFree(pFile->pMainfest);
    taosMemoryFree(pFile->pOptions);
    taosMemoryFree(pFile->path);
    for (int i = 0; i < taosArrayGetSize(pFile->pSst); i++) {
      char* sst = taosArrayGetP(pFile->pSst, i);
      taosMemoryFree(sst);
    }
    taosArrayDestroy(pFile->pSst);
    taosMemoryFree(pFile);
  }
  taosArrayDestroy(handle->pFileList);
  taosCloseFile(&handle->fd);
  return;
}

int32_t streamSnapReaderOpen(void* pMeta, int64_t sver, int64_t chkpId, char* path, SStreamSnapReader** ppReader) {
  // impl later
  SStreamSnapReader* pReader = taosMemoryCalloc(1, sizeof(SStreamSnapReader));
  if (pReader == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (streamSnapHandleInit(&pReader->handle, (char*)path, chkpId, pMeta) < 0) {
    taosMemoryFree(pReader);
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

  if (pHandle->fd == NULL) {
    if (pHandle->currFileIdx >= taosArrayGetSize(pHandle->pFileList)) {
      // finish
      *ppData = NULL;
      *size = 0;
      return 0;
    } else {
      pHandle->fd = streamOpenFile(pFile->path, item->name, TD_FILE_READ);
      qDebug("%s open file %s, current offset:%" PRId64 ", size:% " PRId64 ", file no.%d", STREAM_STATE_TRANSFER,
             item->name, (int64_t)pHandle->offset, item->size, pHandle->currFileIdx);
    }
  }

  qDebug("%s start to read file %s, current offset:%" PRId64 ", size:%" PRId64 ", file no.%d", STREAM_STATE_TRANSFER,
         item->name, (int64_t)pHandle->offset, item->size, pHandle->currFileIdx);
  uint8_t* buf = taosMemoryCalloc(1, sizeof(SStreamSnapBlockHdr) + kBlockSize);
  int64_t  nread = taosPReadFile(pHandle->fd, buf + sizeof(SStreamSnapBlockHdr), kBlockSize, pHandle->offset);
  if (nread == -1) {
    code = TAOS_SYSTEM_ERROR(terrno);
    qError("%s snap failed to read snap, file name:%s, type:%d,reason:%s", STREAM_STATE_TRANSFER, item->name,
           item->type, tstrerror(code));
    return -1;
  } else if (nread > 0 && nread <= kBlockSize) {
    // left bytes less than kBlockSize
    qDebug("%s read file %s, current offset:%" PRId64 ",size:% " PRId64 ", file no.%d", STREAM_STATE_TRANSFER,
           item->name, (int64_t)pHandle->offset, item->size, pHandle->currFileIdx);
    pHandle->offset += nread;
    if (pHandle->offset >= item->size || nread < kBlockSize) {
      taosCloseFile(&pHandle->fd);
      pHandle->offset = 0;
      pHandle->currFileIdx += 1;
    }
  } else {
    qDebug("%s no data read, close file no.%d, move to next file, open and read", STREAM_STATE_TRANSFER,
           pHandle->currFileIdx);
    taosCloseFile(&pHandle->fd);
    pHandle->offset = 0;
    pHandle->currFileIdx += 1;

    if (pHandle->currFileIdx >= taosArrayGetSize(pHandle->pFileList)) {
      // finish
      *ppData = NULL;
      *size = 0;
      return 0;
    }
    item = taosArrayGet(pHandle->pFileList, pHandle->currFileIdx);
    pHandle->fd = streamOpenFile(pFile->path, item->name, TD_FILE_READ);

    nread = taosPReadFile(pHandle->fd, buf + sizeof(SStreamSnapBlockHdr), kBlockSize, pHandle->offset);
    pHandle->offset += nread;

    qDebug("%s open file and read file %s, current offset:%" PRId64 ", size:% " PRId64 ", file no.%d",
           STREAM_STATE_TRANSFER, item->name, (int64_t)pHandle->offset, item->size, pHandle->currFileIdx);
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

  *ppWriter = pWriter;
  return 0;
}

int32_t streamSnapWrite(SStreamSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t code = 0;

  SStreamSnapBlockHdr* pHdr = (SStreamSnapBlockHdr*)pData;
  SStreamSnapHandle*   pHandle = &pWriter->handle;
  SBanckendFile*       pFile = pHandle->pBackendFile;
  SBackendFileItem*    pItem = taosArrayGet(pHandle->pFileList, pHandle->currFileIdx);

  if (pHandle->fd == NULL) {
    pHandle->fd = streamOpenFile(pFile->path, pItem->name, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
    if (pHandle->fd == NULL) {
      code = TAOS_SYSTEM_ERROR(terrno);
      qError("%s failed to open file name:%s%s%s, reason:%s", STREAM_STATE_TRANSFER, pFile->path, TD_DIRSEP, pHdr->name,
             tstrerror(code));
    }
  }

  if (strlen(pHdr->name) == strlen(pItem->name) && strcmp(pHdr->name, pItem->name) == 0) {
    int64_t bytes = taosPWriteFile(pHandle->fd, pHdr->data, pHdr->size, pHandle->offset);
    if (bytes != pHdr->size) {
      code = TAOS_SYSTEM_ERROR(terrno);
      qError("%s failed to write snap, file name:%s, reason:%s", STREAM_STATE_TRANSFER, pHdr->name, tstrerror(code));
      return code;
    }
    pHandle->offset += bytes;
  } else {
    taosCloseFile(&pHandle->fd);
    pHandle->offset = 0;
    pHandle->currFileIdx += 1;

    SBackendFileItem item;
    item.name = taosStrdup(pHdr->name);
    item.type = pHdr->type;
    taosArrayPush(pHandle->pFileList, &item);

    SBackendFileItem* pItem = taosArrayGet(pHandle->pFileList, pHandle->currFileIdx);
    pHandle->fd = streamOpenFile(pFile->path, pItem->name, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
    if (pHandle->fd == NULL) {
      code = TAOS_SYSTEM_ERROR(terrno);
      qError("%s failed to open file name:%s%s%s, reason:%s", STREAM_STATE_TRANSFER, pFile->path, TD_DIRSEP, pHdr->name,
             tstrerror(code));
    }

    taosPWriteFile(pHandle->fd, pHdr->data, pHdr->size, pHandle->offset);
    pHandle->offset += pHdr->size;
  }

  // impl later
  return 0;
}
int32_t streamSnapWriterClose(SStreamSnapWriter* pWriter, int8_t rollback) {
  SStreamSnapHandle* handle = &pWriter->handle;
  if (qDebugFlag & DEBUG_TRACE) {
    char* buf = (char*)taosMemoryMalloc(128 + taosArrayGetSize(handle->pFileList) * 16);
    int   n = sprintf(buf, "[");
    for (int i = 0; i < taosArrayGetSize(handle->pFileList); i++) {
      SBackendFileItem* item = taosArrayGet(handle->pFileList, i);
      if (i != taosArrayGetSize(handle->pFileList) - 1) {
        n += sprintf(buf + n, "%s %" PRId64 ",", item->name, item->size);
      } else {
        n += sprintf(buf + n, "%s %" PRId64 "]", item->name, item->size);
      }
    }
    qDebug("%s snap get file list, %s", STREAM_STATE_TRANSFER, buf);
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
