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

#include "taoserror.h"
#include "walInt.h"

SWalReadHandle *walOpenReadHandle(SWal *pWal) {
  SWalReadHandle *pRead = taosMemoryMalloc(sizeof(SWalReadHandle));
  if (pRead == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pRead->pWal = pWal;
  pRead->pReadIdxTFile = NULL;
  pRead->pReadLogTFile = NULL;
  pRead->curVersion = -1;
  pRead->curFileFirstVer = -1;
  pRead->capacity = 0;
  pRead->status = 0;

  taosThreadMutexInit(&pRead->mutex, NULL);

  pRead->pHead = taosMemoryMalloc(sizeof(SWalCkHead));
  if (pRead->pHead == NULL) {
    terrno = TSDB_CODE_WAL_OUT_OF_MEMORY;
    taosMemoryFree(pRead);
    return NULL;
  }
  return pRead;
}

void walCloseReadHandle(SWalReadHandle *pRead) {
  taosCloseFile(&pRead->pReadIdxTFile);
  taosCloseFile(&pRead->pReadLogTFile);
  taosMemoryFreeClear(pRead->pHead);
  taosMemoryFree(pRead);
}

int32_t walRegisterRead(SWalReadHandle *pRead, int64_t ver) {
  // TODO
  return 0;
}

static int64_t walReadSeekFilePos(SWalReadHandle *pRead, int64_t fileFirstVer, int64_t ver) {
  int64_t ret = 0;

  TdFilePtr pIdxTFile = pRead->pReadIdxTFile;
  TdFilePtr pLogTFile = pRead->pReadLogTFile;

  // seek position
  int64_t offset = (ver - fileFirstVer) * sizeof(SWalIdxEntry);
  ret = taosLSeekFile(pIdxTFile, offset, SEEK_SET);
  if (ret < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("failed to seek idx file, ver %ld, pos: %ld, since %s", ver, offset, terrstr());
    return -1;
  }
  SWalIdxEntry entry = {0};
  if ((ret = taosReadFile(pIdxTFile, &entry, sizeof(SWalIdxEntry))) != sizeof(SWalIdxEntry)) {
    if (ret < 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      wError("failed to read idx file, since %s", terrstr());
    } else {
      terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
      wError("read idx file incompletely, read bytes %ld, bytes should be %lu", ret, sizeof(SWalIdxEntry));
    }
    return -1;
  }

  ASSERT(entry.ver == ver);
  ret = taosLSeekFile(pLogTFile, entry.offset, SEEK_SET);
  if (ret < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("failed to seek log file, ver %ld, pos: %ld, since %s", ver, entry.offset, terrstr());
    return -1;
  }
  return ret;
}

static int32_t walReadChangeFile(SWalReadHandle *pRead, int64_t fileFirstVer) {
  char fnameStr[WAL_FILE_LEN];

  taosCloseFile(&pRead->pReadIdxTFile);
  taosCloseFile(&pRead->pReadLogTFile);

  walBuildLogName(pRead->pWal, fileFirstVer, fnameStr);
  TdFilePtr pLogTFile = taosOpenFile(fnameStr, TD_FILE_READ);
  if (pLogTFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("cannot open file %s, since %s", fnameStr, terrstr());
    return -1;
  }

  pRead->pReadLogTFile = pLogTFile;

  walBuildIdxName(pRead->pWal, fileFirstVer, fnameStr);
  TdFilePtr pIdxTFile = taosOpenFile(fnameStr, TD_FILE_READ);
  if (pIdxTFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("cannot open file %s, since %s", fnameStr, terrstr());
    return -1;
  }

  pRead->pReadIdxTFile = pIdxTFile;
  return 0;
}

static int32_t walReadSeekVer(SWalReadHandle *pRead, int64_t ver) {
  SWal *pWal = pRead->pWal;
  if (ver == pRead->curVersion) {
    return 0;
  }
  if (ver > pWal->vers.lastVer || ver < pWal->vers.firstVer) {
    wError("invalid version: % " PRId64 ", first ver %ld, last ver %ld", ver, pWal->vers.firstVer, pWal->vers.lastVer);
    terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
    return -1;
  }
  if (ver < pWal->vers.snapshotVer) {
  }

  SWalFileInfo tmpInfo;
  tmpInfo.firstVer = ver;
  // bsearch in fileSet
  SWalFileInfo *pRet = taosArraySearch(pWal->fileInfoSet, &tmpInfo, compareWalFileInfo, TD_LE);
  ASSERT(pRet != NULL);
  if (pRead->curFileFirstVer != pRet->firstVer) {
    // error code set inner
    if (walReadChangeFile(pRead, pRet->firstVer) < 0) {
      return -1;
    }
  }

  // error code set inner
  if (walReadSeekFilePos(pRead, pRet->firstVer, ver) < 0) {
    return -1;
  }

  pRead->curVersion = ver;

  return 0;
}

void walSetReaderCapacity(SWalReadHandle *pRead, int32_t capacity) { pRead->capacity = capacity; }

int32_t walFetchHead(SWalReadHandle *pRead, int64_t ver, SWalCkHead *pHead) {
  int64_t code;

  // TODO: valid ver
  if (ver > pRead->pWal->vers.commitVer) {
    return -1;
  }

  if (pRead->curVersion != ver) {
    code = walReadSeekVer(pRead, ver);
    if (code < 0) return -1;
  }

  ASSERT(taosValidFile(pRead->pReadLogTFile) == true);

  code = taosReadFile(pRead->pReadLogTFile, pHead, sizeof(SWalCkHead));
  if (code != sizeof(SWalCkHead)) {
    return -1;
  }

  code = walValidHeadCksum(pHead);

  if (code != 0) {
    wError("unexpected wal log version: % " PRId64 ", since head checksum not passed", ver);
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    return -1;
  }

  return 0;
}

int32_t walSkipFetchBody(SWalReadHandle *pRead, const SWalCkHead *pHead) {
  int64_t code;

  ASSERT(pRead->curVersion == pHead->head.version);

  code = taosLSeekFile(pRead->pReadLogTFile, pHead->head.bodyLen, SEEK_CUR);
  if (code < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    pRead->curVersion = -1;
    return -1;
  }

  pRead->curVersion++;

  return 0;
}

int32_t walFetchBody(SWalReadHandle *pRead, SWalCkHead **ppHead) {
  SWalCont *pReadHead = &((*ppHead)->head);
  int64_t   ver = pReadHead->version;

  if (pRead->capacity < pReadHead->bodyLen) {
    void *ptr = taosMemoryRealloc(*ppHead, sizeof(SWalCkHead) + pReadHead->bodyLen);
    if (ptr == NULL) {
      terrno = TSDB_CODE_WAL_OUT_OF_MEMORY;
      return -1;
    }
    *ppHead = ptr;
    pReadHead = &((*ppHead)->head);
    pRead->capacity = pReadHead->bodyLen;
  }

  if (pReadHead->bodyLen != taosReadFile(pRead->pReadLogTFile, pReadHead->body, pReadHead->bodyLen)) {
    ASSERT(0);
    return -1;
  }

  if (pReadHead->version != ver) {
    wError("wal fetch body error: %" PRId64 ", read request version:%" PRId64 "", pRead->pHead->head.version, ver);
    pRead->curVersion = -1;
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    return -1;
  }

  if (walValidBodyCksum(*ppHead) != 0) {
    wError("wal fetch body error: % " PRId64 ", since body checksum not passed", ver);
    pRead->curVersion = -1;
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    return -1;
  }

  pRead->curVersion = ver + 1;
  return 0;
}

int32_t walReadWithHandle_s(SWalReadHandle *pRead, int64_t ver, SWalCont **ppHead) {
  taosThreadMutexLock(&pRead->mutex);
  if (walReadWithHandle(pRead, ver) < 0) {
    taosThreadMutexUnlock(&pRead->mutex);
    return -1;
  }
  *ppHead = taosMemoryMalloc(sizeof(SWalCont) + pRead->pHead->head.bodyLen);
  if (*ppHead == NULL) {
    taosThreadMutexUnlock(&pRead->mutex);
    return -1;
  }
  memcpy(*ppHead, &pRead->pHead->head, sizeof(SWalCont) + pRead->pHead->head.bodyLen);
  taosThreadMutexUnlock(&pRead->mutex);
  return 0;
}

int32_t walReadWithHandle(SWalReadHandle *pRead, int64_t ver) {
  int64_t code;

  if (pRead->pWal->vers.firstVer == -1) {
    terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
    return -1;
  }

  // TODO: check wal life
  if (pRead->curVersion != ver) {
    if (walReadSeekVer(pRead, ver) < 0) {
      wError("unexpected wal log version: % " PRId64 ", since %s", ver, terrstr());
      return -1;
    }
  }

  if (ver > pRead->pWal->vers.lastVer || ver < pRead->pWal->vers.firstVer) {
    wError("invalid version: % " PRId64 ", first ver %ld, last ver %ld", ver, pRead->pWal->vers.firstVer,
           pRead->pWal->vers.lastVer);
    terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
    return -1;
  }

  ASSERT(taosValidFile(pRead->pReadLogTFile) == true);

  code = taosReadFile(pRead->pReadLogTFile, pRead->pHead, sizeof(SWalCkHead));
  if (code != sizeof(SWalCkHead)) {
    if (code < 0)
      terrno = TAOS_SYSTEM_ERROR(errno);
    else {
      terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
      ASSERT(0);
    }
    return -1;
  }

  code = walValidHeadCksum(pRead->pHead);
  if (code != 0) {
    wError("unexpected wal log version: % " PRId64 ", since head checksum not passed", ver);
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    return -1;
  }

  if (pRead->capacity < pRead->pHead->head.bodyLen) {
    void *ptr = taosMemoryRealloc(pRead->pHead, sizeof(SWalCkHead) + pRead->pHead->head.bodyLen);
    if (ptr == NULL) {
      terrno = TSDB_CODE_WAL_OUT_OF_MEMORY;
      return -1;
    }
    pRead->pHead = ptr;
    pRead->capacity = pRead->pHead->head.bodyLen;
  }

  if ((code = taosReadFile(pRead->pReadLogTFile, pRead->pHead->head.body, pRead->pHead->head.bodyLen)) !=
      pRead->pHead->head.bodyLen) {
    if (code < 0)
      terrno = TAOS_SYSTEM_ERROR(errno);
    else {
      terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
      ASSERT(0);
    }
    return -1;
  }

  if (pRead->pHead->head.version != ver) {
    wError("unexpected wal log version: %" PRId64 ", read request version:%" PRId64 "", pRead->pHead->head.version,
           ver);
    pRead->curVersion = -1;
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    return -1;
  }

  code = walValidBodyCksum(pRead->pHead);
  if (code != 0) {
    wError("unexpected wal log version: % " PRId64 ", since body checksum not passed", ver);
    pRead->curVersion = -1;
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    return -1;
  }
  pRead->curVersion++;

  return 0;
}

#if 0
int32_t walRead(SWal *pWal, SWalHead **ppHead, int64_t ver) {
  int code;
  code = walSeekVer(pWal, ver);
  if (code != 0) {
    return code;
  }
  if (*ppHead == NULL) {
    void *ptr = taosMemoryRealloc(*ppHead, sizeof(SWalHead));
    if (ptr == NULL) {
      return -1;
    }
    *ppHead = ptr;
  }
  if (tfRead(pWal->pWriteLogTFile, *ppHead, sizeof(SWalHead)) != sizeof(SWalHead)) {
    return -1;
  }
  // TODO: endian compatibility processing after read
  if (walValidHeadCksum(*ppHead) != 0) {
    return -1;
  }
  void *ptr = taosMemoryRealloc(*ppHead, sizeof(SWalHead) + (*ppHead)->head.len);
  if (ptr == NULL) {
    taosMemoryFree(*ppHead);
    *ppHead = NULL;
    return -1;
  }
  if (tfRead(pWal->pWriteLogTFile, (*ppHead)->head.body, (*ppHead)->head.len) != (*ppHead)->head.len) {
    return -1;
  }
  // TODO: endian compatibility processing after read
  if (walValidBodyCksum(*ppHead) != 0) {
    return -1;
  }

  return 0;
}

int32_t walReadWithFp(SWal *pWal, FWalWrite writeFp, int64_t verStart, int32_t readNum) {
return 0;
}
#endif
