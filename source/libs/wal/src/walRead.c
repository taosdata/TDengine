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

static int32_t walFetchHeadNew(SWalReader *pRead, int64_t fetchVer);
static int32_t walFetchBodyNew(SWalReader *pRead);
static int32_t walSkipFetchBodyNew(SWalReader *pRead);

SWalReader *walOpenReader(SWal *pWal, SWalFilterCond *cond) {
  SWalReader *pRead = taosMemoryMalloc(sizeof(SWalReader));
  if (pRead == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pRead->pWal = pWal;
  pRead->pIdxFile = NULL;
  pRead->pLogFile = NULL;
  pRead->curVersion = -1;
  pRead->curFileFirstVer = -1;
  pRead->capacity = 0;
  if (cond)
    pRead->cond = *cond;
  else {
    pRead->cond.scanMeta = 0;
    pRead->cond.scanUncommited = 0;
  }

  taosThreadMutexInit(&pRead->mutex, NULL);

  pRead->pHead = taosMemoryMalloc(sizeof(SWalCkHead));
  if (pRead->pHead == NULL) {
    terrno = TSDB_CODE_WAL_OUT_OF_MEMORY;
    taosMemoryFree(pRead);
    return NULL;
  }

  return pRead;
}

void walCloseReader(SWalReader *pRead) {
  taosCloseFile(&pRead->pIdxFile);
  taosCloseFile(&pRead->pLogFile);
  taosMemoryFreeClear(pRead->pHead);
  taosMemoryFree(pRead);
}

int32_t walNextValidMsg(SWalReader *pRead) {
  int64_t fetchVer = pRead->curVersion;
  int64_t endVer = pRead->cond.scanUncommited ? walGetLastVer(pRead->pWal) : walGetCommittedVer(pRead->pWal);
  while (fetchVer <= endVer) {
    if (walFetchHeadNew(pRead, fetchVer) < 0) {
      return -1;
    }
    if (pRead->pHead->head.msgType == TDMT_VND_SUBMIT ||
        (IS_META_MSG(pRead->pHead->head.msgType) && pRead->cond.scanMeta)) {
      if (walFetchBodyNew(pRead) < 0) {
        return -1;
      }
      return 0;
    } else {
      if (walSkipFetchBodyNew(pRead) < 0) {
        return -1;
      }
      fetchVer++;
      ASSERT(fetchVer == pRead->curVersion);
    }
  }
  return -1;
}

static int64_t walReadSeekFilePos(SWalReader *pRead, int64_t fileFirstVer, int64_t ver) {
  int64_t ret = 0;

  TdFilePtr pIdxTFile = pRead->pIdxFile;
  TdFilePtr pLogTFile = pRead->pLogFile;

  // seek position
  int64_t offset = (ver - fileFirstVer) * sizeof(SWalIdxEntry);
  ret = taosLSeekFile(pIdxTFile, offset, SEEK_SET);
  if (ret < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, failed to seek idx file, ver:%" PRId64 ", pos:%" PRId64 ", since %s", pRead->pWal->cfg.vgId, ver,
           offset, terrstr());
    return -1;
  }
  SWalIdxEntry entry = {0};
  if ((ret = taosReadFile(pIdxTFile, &entry, sizeof(SWalIdxEntry))) != sizeof(SWalIdxEntry)) {
    if (ret < 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      wError("vgId:%d, failed to read idx file, since %s", pRead->pWal->cfg.vgId, terrstr());
    } else {
      terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
      wError("vgId:%d, read idx file incompletely, read bytes %" PRId64 ", bytes should be %" PRIu64,
             pRead->pWal->cfg.vgId, ret, sizeof(SWalIdxEntry));
    }
    return -1;
  }

  ASSERT(entry.ver == ver);
  ret = taosLSeekFile(pLogTFile, entry.offset, SEEK_SET);
  if (ret < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, failed to seek log file, ver:%" PRId64 ", pos:%" PRId64 ", since %s", pRead->pWal->cfg.vgId, ver,
           entry.offset, terrstr());
    return -1;
  }
  return ret;
}

static int32_t walReadChangeFile(SWalReader *pRead, int64_t fileFirstVer) {
  char fnameStr[WAL_FILE_LEN];

  taosCloseFile(&pRead->pIdxFile);
  taosCloseFile(&pRead->pLogFile);

  walBuildLogName(pRead->pWal, fileFirstVer, fnameStr);
  TdFilePtr pLogTFile = taosOpenFile(fnameStr, TD_FILE_READ);
  if (pLogTFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, cannot open file %s, since %s", pRead->pWal->cfg.vgId, fnameStr, terrstr());
    return -1;
  }

  pRead->pLogFile = pLogTFile;

  walBuildIdxName(pRead->pWal, fileFirstVer, fnameStr);
  TdFilePtr pIdxTFile = taosOpenFile(fnameStr, TD_FILE_READ);
  if (pIdxTFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, cannot open file %s, since %s", pRead->pWal->cfg.vgId, fnameStr, terrstr());
    return -1;
  }

  pRead->pIdxFile = pIdxTFile;
  return 0;
}

static int32_t walReadSeekVer(SWalReader *pRead, int64_t ver) {
  SWal *pWal = pRead->pWal;
  if (ver == pRead->curVersion) {
    return 0;
  }
  if (ver > pWal->vers.lastVer || ver < pWal->vers.firstVer) {
    wError("vgId:$d, invalid version:%" PRId64 ", first ver:%" PRId64 ", last ver:%" PRId64, pRead->pWal->cfg.vgId, ver,
           pWal->vers.firstVer, pWal->vers.lastVer);
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

void walSetReaderCapacity(SWalReader *pRead, int32_t capacity) { pRead->capacity = capacity; }

static int32_t walFetchHeadNew(SWalReader *pRead, int64_t fetchVer) {
  int64_t contLen;
  if (pRead->curVersion != fetchVer) {
    if (walReadSeekVer(pRead, fetchVer) < 0) return -1;
  }
  contLen = taosReadFile(pRead->pLogFile, pRead->pHead, sizeof(SWalCkHead));
  if (contLen != sizeof(SWalCkHead)) {
    if (contLen < 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
    } else {
      terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    }
    pRead->curVersion = -1;
    return -1;
  }
  return 0;
}

static int32_t walFetchBodyNew(SWalReader *pRead) {
  SWalCont *pReadHead = &pRead->pHead->head;
  int64_t   ver = pReadHead->version;

  if (pRead->capacity < pReadHead->bodyLen) {
    void *ptr = taosMemoryRealloc(pRead->pHead, sizeof(SWalCkHead) + pReadHead->bodyLen);
    if (ptr == NULL) {
      terrno = TSDB_CODE_WAL_OUT_OF_MEMORY;
      return -1;
    }
    pRead->pHead = ptr;
    pReadHead = &pRead->pHead->head;
    pRead->capacity = pReadHead->bodyLen;
  }

  if (pReadHead->bodyLen != taosReadFile(pRead->pLogFile, pReadHead->body, pReadHead->bodyLen)) {
    if (pReadHead->bodyLen < 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      wError("vgId:%d, wal fetch body error: %" PRId64 ", read request version:%" PRId64 ", since %s",
             pRead->pWal->cfg.vgId, pRead->pHead->head.version, ver, tstrerror(terrno));
    } else {
      wError("vgId:%d, wal fetch body error: %" PRId64 ", read request version:%" PRId64 ", since file corrupted",
             pRead->pWal->cfg.vgId, pRead->pHead->head.version, ver);
      terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    }
    pRead->curVersion = -1;
    ASSERT(0);
    return -1;
  }

  if (pReadHead->version != ver) {
    wError("vgId:%d, wal fetch body error: %" PRId64 ", read request version:%" PRId64, pRead->pWal->cfg.vgId,
           pRead->pHead->head.version, ver);
    pRead->curVersion = -1;
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    ASSERT(0);
    return -1;
  }

  if (walValidBodyCksum(pRead->pHead) != 0) {
    wError("vgId:%d, wal fetch body error: % " PRId64 ", since body checksum not passed", pRead->pWal->cfg.vgId, ver);
    pRead->curVersion = -1;
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    ASSERT(0);
    return -1;
  }

  pRead->curVersion = ver + 1;
  return 0;
}

static int32_t walSkipFetchBodyNew(SWalReader *pRead) {
  int64_t code;

  ASSERT(pRead->curVersion == pRead->pHead->head.version);

  code = taosLSeekFile(pRead->pLogFile, pRead->pHead->head.bodyLen, SEEK_CUR);
  if (code < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    pRead->curVersion = -1;
    return -1;
  }

  pRead->curVersion++;

  return 0;
}

int32_t walFetchHead(SWalReader *pRead, int64_t ver, SWalCkHead *pHead) {
  int64_t code;

  // TODO: valid ver
  if (ver > pRead->pWal->vers.commitVer) {
    return -1;
  }

  if (pRead->curVersion != ver) {
    code = walReadSeekVer(pRead, ver);
    if (code < 0) return -1;
  }

  ASSERT(taosValidFile(pRead->pLogFile) == true);

  code = taosReadFile(pRead->pLogFile, pHead, sizeof(SWalCkHead));
  if (code != sizeof(SWalCkHead)) {
    return -1;
  }

  code = walValidHeadCksum(pHead);

  if (code != 0) {
    wError("vgId:%d, unexpected wal log version:%" PRId64 ", since head checksum not passed", pRead->pWal->cfg.vgId,
           ver);
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    return -1;
  }

  return 0;
}

int32_t walSkipFetchBody(SWalReader *pRead, const SWalCkHead *pHead) {
  int64_t code;

  ASSERT(pRead->curVersion == pHead->head.version);

  code = taosLSeekFile(pRead->pLogFile, pHead->head.bodyLen, SEEK_CUR);
  if (code < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    pRead->curVersion = -1;
    return -1;
  }

  pRead->curVersion++;

  return 0;
}

int32_t walFetchBody(SWalReader *pRead, SWalCkHead **ppHead) {
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

  if (pReadHead->bodyLen != taosReadFile(pRead->pLogFile, pReadHead->body, pReadHead->bodyLen)) {
    ASSERT(0);
    return -1;
  }

  if (pReadHead->version != ver) {
    wError("vgId:%d, wal fetch body error: %" PRId64 ", read request version:%" PRId64, pRead->pWal->cfg.vgId,
           pRead->pHead->head.version, ver);
    pRead->curVersion = -1;
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    return -1;
  }

  if (walValidBodyCksum(*ppHead) != 0) {
    wError("vgId:%d, wal fetch body error: % " PRId64 ", since body checksum not passed", pRead->pWal->cfg.vgId, ver);
    pRead->curVersion = -1;
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    return -1;
  }

  pRead->curVersion = ver + 1;
  return 0;
}

int32_t walReadWithHandle_s(SWalReader *pRead, int64_t ver, SWalCont **ppHead) {
  taosThreadMutexLock(&pRead->mutex);
  if (walReadVer(pRead, ver) < 0) {
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

int32_t walReadVer(SWalReader *pRead, int64_t ver) {
  int64_t code;

  if (pRead->pWal->vers.firstVer == -1) {
    terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
    return -1;
  }

  // TODO: check wal life
  if (pRead->curVersion != ver) {
    if (walReadSeekVer(pRead, ver) < 0) {
      wError("vgId:%d, unexpected wal log version:%" PRId64 ", since %s", pRead->pWal->cfg.vgId, ver, terrstr());
      return -1;
    }
  }

  if (ver > pRead->pWal->vers.lastVer || ver < pRead->pWal->vers.firstVer) {
    wError("vgId:%d, invalid version:%" PRId64 ", first ver:%" PRId64 ", last ver:%" PRId64, pRead->pWal->cfg.vgId, ver,
           pRead->pWal->vers.firstVer, pRead->pWal->vers.lastVer);
    terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
    return -1;
  }

  ASSERT(taosValidFile(pRead->pLogFile) == true);

  code = taosReadFile(pRead->pLogFile, pRead->pHead, sizeof(SWalCkHead));
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
    wError("vgId:%d, unexpected wal log version:%" PRId64 ", since head checksum not passed", pRead->pWal->cfg.vgId,
           ver);
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

  if ((code = taosReadFile(pRead->pLogFile, pRead->pHead->head.body, pRead->pHead->head.bodyLen)) !=
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
    wError("vgId:%d, unexpected wal log version:%" PRId64 ", read request version:%" PRId64, pRead->pWal->cfg.vgId,
           pRead->pHead->head.version, ver);
    pRead->curVersion = -1;
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    return -1;
  }

  code = walValidBodyCksum(pRead->pHead);
  if (code != 0) {
    wError("vgId:%d, unexpected wal log version:%" PRId64 ", since body checksum not passed", pRead->pWal->cfg.vgId,
           ver);
    pRead->curVersion = -1;
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    return -1;
  }
  pRead->curVersion++;

  return 0;
}
