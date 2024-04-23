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

SWalReader *walOpenReader(SWal *pWal, SWalFilterCond *cond, int64_t id) {
  SWalReader *pReader = taosMemoryCalloc(1, sizeof(SWalReader));
  if (pReader == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pReader->pWal = pWal;
  pReader->readerId = (id != 0)? id:tGenIdPI64();
  pReader->pIdxFile = NULL;
  pReader->pLogFile = NULL;
  pReader->curVersion = -1;
  pReader->curFileFirstVer = -1;
  pReader->capacity = 0;
  if (cond) {
    pReader->cond = *cond;
  } else {
//    pReader->cond.scanUncommited = 0;
    pReader->cond.scanNotApplied = 0;
    pReader->cond.scanMeta = 0;
    pReader->cond.enableRef = 0;
  }

  taosThreadMutexInit(&pReader->mutex, NULL);

  pReader->pHead = taosMemoryMalloc(sizeof(SWalCkHead));
  if (pReader->pHead == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(pReader);
    return NULL;
  }

  /*if (pReader->cond.enableRef) {*/
  /* taosHashPut(pWal->pRefHash, &pReader->readerId, sizeof(int64_t), &pReader, sizeof(void *));*/
  /*}*/

  return pReader;
}

void walCloseReader(SWalReader *pReader) {
  if(pReader == NULL) return;

  taosCloseFile(&pReader->pIdxFile);
  taosCloseFile(&pReader->pLogFile);
  taosMemoryFreeClear(pReader->pHead);
  taosMemoryFree(pReader);
}

int32_t walNextValidMsg(SWalReader *pReader) {
  int64_t fetchVer = pReader->curVersion;
  int64_t lastVer = walGetLastVer(pReader->pWal);
  int64_t committedVer = walGetCommittedVer(pReader->pWal);
  int64_t appliedVer = walGetAppliedVer(pReader->pWal);

  wDebug("vgId:%d, wal start to fetch, index:%" PRId64 ", last:%" PRId64 " commit:%" PRId64 ", applied:%" PRId64,
         pReader->pWal->cfg.vgId, fetchVer, lastVer, committedVer, appliedVer);
  if (fetchVer > appliedVer) {
    terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
    return -1;
  }

  while (fetchVer <= appliedVer) {
    if (walFetchHead(pReader, fetchVer) < 0) {
      return -1;
    }

    int32_t type = pReader->pHead->head.msgType;
    if (type == TDMT_VND_SUBMIT || ((type == TDMT_VND_DELETE) && (pReader->cond.deleteMsg == 1)) ||
        (IS_META_MSG(type) && pReader->cond.scanMeta)) {
      int32_t code = walFetchBody(pReader);
      return (code == TSDB_CODE_SUCCESS)? 0:-1;
    } else {
      if (walSkipFetchBody(pReader) < 0) {
        return -1;
      }

      fetchVer = pReader->curVersion;
    }
  }

  return -1;
}

int64_t walReaderGetCurrentVer(const SWalReader *pReader) { return pReader->curVersion; }
int64_t walReaderGetValidFirstVer(const SWalReader *pReader) { return walGetFirstVer(pReader->pWal); }
void    walReaderSetSkipToVersion(SWalReader *pReader, int64_t ver) { atomic_store_64(&pReader->skipToVersion, ver); }

// this function is NOT multi-thread safe, and no need to be.
int64_t walReaderGetSkipToVersion(SWalReader *pReader) {
  int64_t newVersion = pReader->skipToVersion;
  pReader->skipToVersion = 0;
  return newVersion;
}

void walReaderValidVersionRange(SWalReader *pReader, int64_t *sver, int64_t *ever) {
  *sver = walGetFirstVer(pReader->pWal);
  int64_t lastVer = walGetLastVer(pReader->pWal);
  int64_t committedVer = walGetCommittedVer(pReader->pWal);
  *ever = pReader->cond.scanUncommited ? lastVer : committedVer;
}

void walReaderVerifyOffset(SWalReader *pWalReader, STqOffsetVal* pOffset){
  // if offset version is small than first version , let's seek to first version
  taosThreadMutexLock(&pWalReader->pWal->mutex);
  int64_t firstVer = walGetFirstVer((pWalReader)->pWal);
  taosThreadMutexUnlock(&pWalReader->pWal->mutex);

  if (pOffset->version < firstVer){
    pOffset->version = firstVer;
  }
}

static int64_t walReadSeekFilePos(SWalReader *pReader, int64_t fileFirstVer, int64_t ver) {
  int64_t ret = 0;

  TdFilePtr pIdxTFile = pReader->pIdxFile;
  TdFilePtr pLogTFile = pReader->pLogFile;

  // seek position
  int64_t offset = (ver - fileFirstVer) * sizeof(SWalIdxEntry);
  ret = taosLSeekFile(pIdxTFile, offset, SEEK_SET);
  if (ret < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, failed to seek idx file, index:%" PRId64 ", pos:%" PRId64 ", since %s", pReader->pWal->cfg.vgId,
           ver, offset, terrstr());
    return -1;
  }
  SWalIdxEntry entry = {0};
  if ((ret = taosReadFile(pIdxTFile, &entry, sizeof(SWalIdxEntry))) != sizeof(SWalIdxEntry)) {
    if (ret < 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      wError("vgId:%d, failed to read idx file, since %s", pReader->pWal->cfg.vgId, terrstr());
    } else {
      terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
      wError("vgId:%d, read idx file incompletely, read bytes %" PRId64 ", bytes should be %ld",
             pReader->pWal->cfg.vgId, ret, sizeof(SWalIdxEntry));
    }
    return -1;
  }

  ret = taosLSeekFile(pLogTFile, entry.offset, SEEK_SET);
  if (ret < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, failed to seek log file, index:%" PRId64 ", pos:%" PRId64 ", since %s", pReader->pWal->cfg.vgId,
           ver, entry.offset, terrstr());
    return -1;
  }
  return ret;
}

static int32_t walReadChangeFile(SWalReader *pReader, int64_t fileFirstVer) {
  char fnameStr[WAL_FILE_LEN] = {0};

  taosCloseFile(&pReader->pIdxFile);
  taosCloseFile(&pReader->pLogFile);

  walBuildLogName(pReader->pWal, fileFirstVer, fnameStr);
  TdFilePtr pLogFile = taosOpenFile(fnameStr, TD_FILE_READ);
  if (pLogFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, cannot open file %s, since %s", pReader->pWal->cfg.vgId, fnameStr, terrstr());
    return -1;
  }

  pReader->pLogFile = pLogFile;

  walBuildIdxName(pReader->pWal, fileFirstVer, fnameStr);
  TdFilePtr pIdxFile = taosOpenFile(fnameStr, TD_FILE_READ);
  if (pIdxFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, cannot open file %s, since %s", pReader->pWal->cfg.vgId, fnameStr, terrstr());
    return -1;
  }

  pReader->pIdxFile = pIdxFile;

  pReader->curFileFirstVer = fileFirstVer;

  return 0;
}

int32_t walReadSeekVerImpl(SWalReader *pReader, int64_t ver) {
  SWal *pWal = pReader->pWal;

  // bsearch in fileSet
  SWalFileInfo tmpInfo;
  tmpInfo.firstVer = ver;
  SWalFileInfo *pRet = taosArraySearch(pWal->fileInfoSet, &tmpInfo, compareWalFileInfo, TD_LE);
  if (pRet == NULL) {
    wError("failed to find WAL log file with ver:%" PRId64, ver);
    terrno = TSDB_CODE_WAL_INVALID_VER;
    return -1;
  }

  if (pReader->curFileFirstVer != pRet->firstVer) {
    // error code was set inner
    if (walReadChangeFile(pReader, pRet->firstVer) < 0) {
      return -1;
    }
  }

  // error code was set inner
  if (walReadSeekFilePos(pReader, pRet->firstVer, ver) < 0) {
    return -1;
  }

  wDebug("vgId:%d, wal version reset from %" PRId64 " to %" PRId64, pReader->pWal->cfg.vgId,
         pReader->curVersion, ver);

  pReader->curVersion = ver;
  return 0;
}

int32_t walReaderSeekVer(SWalReader *pReader, int64_t ver) {
  SWal *pWal = pReader->pWal;
  if (ver == pReader->curVersion) {
    wDebug("vgId:%d, wal index:%" PRId64 " match, no need to reset", pReader->pWal->cfg.vgId, ver);
    return 0;
  }

  if (ver > pWal->vers.lastVer || ver < pWal->vers.firstVer) {
    wInfo("vgId:%d, invalid index:%" PRId64 ", first index:%" PRId64 ", last index:%" PRId64, pReader->pWal->cfg.vgId,
           ver, pWal->vers.firstVer, pWal->vers.lastVer);
    terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
    return -1;
  }

  if (walReadSeekVerImpl(pReader, ver) < 0) {
    return -1;
  }

  return 0;
}


int32_t walFetchHead(SWalReader *pRead, int64_t ver) {
  int64_t code;
  int64_t contLen;
  bool    seeked = false;

  // TODO: valid ver
  if (ver > pRead->pWal->vers.commitVer) {
    return -1;
  }

  if (pRead->curVersion != ver) {
    code = walReaderSeekVer(pRead, ver);
    if (code < 0) {
      return -1;
    }
    seeked = true;
  }

  while (1) {
    contLen = taosReadFile(pRead->pLogFile, pRead->pHead, sizeof(SWalCkHead));
    if (contLen == sizeof(SWalCkHead)) {
      break;
    } else if (contLen == 0 && !seeked) {
      if(walReadSeekVerImpl(pRead, ver) < 0){
        return -1;
      }
      seeked = true;
      continue;
    } else {
      if (contLen < 0) {
        terrno = TAOS_SYSTEM_ERROR(errno);
      } else {
        terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
      }
      return -1;
    }
  }

  code = walValidHeadCksum(pRead->pHead);

  if (code != 0) {
    wError("vgId:%d, unexpected wal log index:%" PRId64 ", since head checksum not passed, 0x%"PRIx64, pRead->pWal->cfg.vgId, ver,
        pRead->readerId);
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    return -1;
  }

  return 0;
}

int32_t walSkipFetchBody(SWalReader *pRead) {
  wDebug("vgId:%d, skip:%" PRId64 ", first:%" PRId64 ", commit:%" PRId64 ", last:%" PRId64
         ", applied:%" PRId64 ", 0x%" PRIx64,
         pRead->pWal->cfg.vgId, pRead->pHead->head.version, pRead->pWal->vers.firstVer, pRead->pWal->vers.commitVer,
         pRead->pWal->vers.lastVer, pRead->pWal->vers.appliedVer, pRead->readerId);

  int64_t code = taosLSeekFile(pRead->pLogFile, pRead->pHead->head.bodyLen, SEEK_CUR);
  if (code < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  pRead->curVersion++;
  return 0;
}

int32_t walFetchBody(SWalReader *pRead) {
  SWalCont *pReadHead = &pRead->pHead->head;
  int64_t   ver = pReadHead->version;
  int32_t   vgId = pRead->pWal->cfg.vgId;
  int64_t   id = pRead->readerId;
  SWalVer  *pVer = &pRead->pWal->vers;

  wDebug("vgId:%d, fetch body:%" PRId64 ", first:%" PRId64 ", commit:%" PRId64 ", last:%" PRId64 ", applied:%" PRId64
         ", 0x%" PRIx64,
         vgId, ver, pVer->firstVer, pVer->commitVer, pVer->lastVer, pVer->appliedVer, id);

  if (pRead->capacity < pReadHead->bodyLen) {
    SWalCkHead *ptr = (SWalCkHead *)taosMemoryRealloc(pRead->pHead, sizeof(SWalCkHead) + pReadHead->bodyLen);
    if (ptr == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    pRead->pHead = ptr;
    pReadHead = &pRead->pHead->head;
    pRead->capacity = pReadHead->bodyLen;
  }

  if (pReadHead->bodyLen != taosReadFile(pRead->pLogFile, pReadHead->body, pReadHead->bodyLen)) {
    if (pReadHead->bodyLen < 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      wError("vgId:%d, wal fetch body error:%" PRId64 ", read request index:%" PRId64 ", since %s, 0x%"PRIx64,
             vgId, pReadHead->version, ver, tstrerror(terrno), id);
    } else {
      wError("vgId:%d, wal fetch body error:%" PRId64 ", read request index:%" PRId64 ", since file corrupted, 0x%"PRIx64,
             vgId, pReadHead->version, ver, id);
      terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    }
    return -1;
  }

  if (pReadHead->version != ver) {
    wError("vgId:%d, wal fetch body error, index:%" PRId64 ", read request index:%" PRId64", 0x%"PRIx64, vgId,
           pReadHead->version, ver, id);
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    return -1;
  }

  if (walValidBodyCksum(pRead->pHead) != 0) {
    wError("vgId:%d, wal fetch body error, index:%" PRId64 ", since body checksum not passed, 0x%" PRIx64, vgId, ver, id);
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    return -1;
  }

  pRead->curVersion++;
  return 0;
}

int32_t walReadVer(SWalReader *pReader, int64_t ver) {
  wDebug("vgId:%d, wal start to read index:%" PRId64, pReader->pWal->cfg.vgId, ver);
  int64_t contLen;
  int32_t code;
  bool    seeked = false;

  if (walIsEmpty(pReader->pWal)) {
    terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
    return -1;
  }

  if (ver > pReader->pWal->vers.lastVer || ver < pReader->pWal->vers.firstVer) {
    wDebug("vgId:%d, invalid index:%" PRId64 ", first index:%" PRId64 ", last index:%" PRId64, pReader->pWal->cfg.vgId,
           ver, pReader->pWal->vers.firstVer, pReader->pWal->vers.lastVer);
    terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
    return -1;
  }

  taosThreadMutexLock(&pReader->mutex);

  if (pReader->curVersion != ver) {
    if (walReaderSeekVer(pReader, ver) < 0) {
      wError("vgId:%d, unexpected wal log, index:%" PRId64 ", since %s", pReader->pWal->cfg.vgId, ver, terrstr());
      taosThreadMutexUnlock(&pReader->mutex);
      return -1;
    }
    seeked = true;
  }

  while (1) {
    contLen = taosReadFile(pReader->pLogFile, pReader->pHead, sizeof(SWalCkHead));
    if (contLen == sizeof(SWalCkHead)) {
      break;
    } else if (contLen == 0 && !seeked) {
      if(walReadSeekVerImpl(pReader, ver) < 0){
        taosThreadMutexUnlock(&pReader->mutex);
        return -1;
      }
      seeked = true;
      continue;
    } else {
      if (contLen < 0) {
        terrno = TAOS_SYSTEM_ERROR(errno);
      } else {
        terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
      }
      wError("vgId:%d, failed to read WAL record head, index:%" PRId64 ", from log file since %s",
             pReader->pWal->cfg.vgId, ver, terrstr());
      taosThreadMutexUnlock(&pReader->mutex);
      return -1;
    }
  }

  code = walValidHeadCksum(pReader->pHead);
  if (code != 0) {
    wError("vgId:%d, unexpected wal log, index:%" PRId64 ", since head checksum not passed", pReader->pWal->cfg.vgId,
           ver);
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    taosThreadMutexUnlock(&pReader->mutex);
    return -1;
  }

  if (pReader->capacity < pReader->pHead->head.bodyLen) {
    SWalCkHead *ptr =
        (SWalCkHead *)taosMemoryRealloc(pReader->pHead, sizeof(SWalCkHead) + pReader->pHead->head.bodyLen);
    if (ptr == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      taosThreadMutexUnlock(&pReader->mutex);
      return -1;
    }
    pReader->pHead = ptr;
    pReader->capacity = pReader->pHead->head.bodyLen;
  }

  if ((contLen = taosReadFile(pReader->pLogFile, pReader->pHead->head.body, pReader->pHead->head.bodyLen)) !=
      pReader->pHead->head.bodyLen) {
    if (contLen < 0)
      terrno = TAOS_SYSTEM_ERROR(errno);
    else {
      terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    }
    wError("vgId:%d, failed to read WAL record body, index:%" PRId64 ", from log file since %s",
           pReader->pWal->cfg.vgId, ver, terrstr());
    taosThreadMutexUnlock(&pReader->mutex);
    return -1;
  }

  if (pReader->pHead->head.version != ver) {
    wError("vgId:%d, unexpected wal log, index:%" PRId64 ", read request index:%" PRId64, pReader->pWal->cfg.vgId,
           pReader->pHead->head.version, ver);
//    pReader->curInvalid = 1;
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    taosThreadMutexUnlock(&pReader->mutex);
    return -1;
  }

  code = walValidBodyCksum(pReader->pHead);
  if (code != 0) {
    wError("vgId:%d, unexpected wal log, index:%" PRId64 ", since body checksum not passed", pReader->pWal->cfg.vgId,
           ver);
    uint32_t readCkSum = walCalcBodyCksum(pReader->pHead->head.body, pReader->pHead->head.bodyLen);
    uint32_t logCkSum = pReader->pHead->cksumBody;
    wError("checksum written into log:%u, checksum calculated:%u", logCkSum, readCkSum);
//    pReader->curInvalid = 1;
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    taosThreadMutexUnlock(&pReader->mutex);
    return -1;
  }
  pReader->curVersion++;

  taosThreadMutexUnlock(&pReader->mutex);

  return 0;
}

void walReadReset(SWalReader *pReader) {
  taosThreadMutexLock(&pReader->mutex);
  taosCloseFile(&pReader->pIdxFile);
  taosCloseFile(&pReader->pLogFile);
  pReader->curFileFirstVer = -1;
  pReader->curVersion = -1;
  taosThreadMutexUnlock(&pReader->mutex);
}
