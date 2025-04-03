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

#include "crypt.h"
#include "taoserror.h"
#include "wal.h"
#include "walInt.h"

SWalReader *walOpenReader(SWal *pWal, SWalFilterCond *cond, int64_t id) {
  SWalReader *pReader = taosMemoryCalloc(1, sizeof(SWalReader));
  if (pReader == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pReader->pWal = pWal;
  pReader->readerId = (id != 0) ? id : tGenIdPI64();
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

  terrno = taosThreadMutexInit(&pReader->mutex, NULL);
  if (terrno) {
    taosMemoryFree(pReader);
    return NULL;
  }

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
  if (pReader == NULL) return;

  TAOS_UNUSED(taosCloseFile(&pReader->pIdxFile));
  TAOS_UNUSED(taosCloseFile(&pReader->pLogFile));
  taosMemoryFreeClear(pReader->pHead);
  taosMemoryFree(pReader);
}

int32_t walNextValidMsg(SWalReader *pReader) {
  int64_t fetchVer = pReader->curVersion;
  int64_t lastVer = walGetLastVer(pReader->pWal);
  int64_t committedVer = walGetCommittedVer(pReader->pWal);
  int64_t appliedVer = walGetAppliedVer(pReader->pWal);

  wDebug("vgId:%d, wal start to fetch, index:%" PRId64 ", last index:%" PRId64 " commit index:%" PRId64
         ", applied index:%" PRId64,
         pReader->pWal->cfg.vgId, fetchVer, lastVer, committedVer, appliedVer);
  if (fetchVer > appliedVer) {
    TAOS_RETURN(TSDB_CODE_WAL_LOG_NOT_EXIST);
  }

  while (fetchVer <= appliedVer) {
    TAOS_CHECK_RETURN(walFetchHead(pReader, fetchVer));

    int32_t type = pReader->pHead->head.msgType;
    if (type == TDMT_VND_SUBMIT || ((type == TDMT_VND_DELETE) && (pReader->cond.deleteMsg == 1)) ||
        (IS_META_MSG(type) && pReader->cond.scanMeta)) {
      TAOS_RETURN(walFetchBody(pReader));
    } else if (type == TDMT_VND_DROP_TABLE && pReader->cond.scanDropCtb) {
      TAOS_RETURN(walFetchBody(pReader));
    } else {
      TAOS_CHECK_RETURN(walSkipFetchBody(pReader));

      fetchVer = pReader->curVersion;
    }
  }

  TAOS_RETURN(TSDB_CODE_FAILED);
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

void walReaderVerifyOffset(SWalReader *pWalReader, STqOffsetVal *pOffset) {
  // if offset version is small than first version , let's seek to first version
  TAOS_UNUSED(taosThreadRwlockRdlock(&pWalReader->pWal->mutex));
  int64_t firstVer = walGetFirstVer((pWalReader)->pWal);
  TAOS_UNUSED(taosThreadRwlockUnlock(&pWalReader->pWal->mutex));

  if (pOffset->version < firstVer) {
    pOffset->version = firstVer;
  }
}

static int32_t walReadSeekFilePos(SWalReader *pReader, int64_t fileFirstVer, int64_t ver) {
  int64_t ret = 0;

  TdFilePtr pIdxTFile = pReader->pIdxFile;
  TdFilePtr pLogTFile = pReader->pLogFile;

  // seek position
  int64_t offset = (ver - fileFirstVer) * sizeof(SWalIdxEntry);
  ret = taosLSeekFile(pIdxTFile, offset, SEEK_SET);
  if (ret < 0) {
    wError("vgId:%d, failed to seek idx file, index:%" PRId64 ", pos:%" PRId64 ", since %s", pReader->pWal->cfg.vgId,
           ver, offset, terrstr());

    TAOS_RETURN(terrno);
  }
  SWalIdxEntry entry = {0};
  if ((ret = taosReadFile(pIdxTFile, &entry, sizeof(SWalIdxEntry))) != sizeof(SWalIdxEntry)) {
    if (ret < 0) {
      wError("vgId:%d, failed to read idx file, since %s", pReader->pWal->cfg.vgId, terrstr());
      TAOS_RETURN(terrno);
    } else {
      wError("vgId:%d, read idx file incompletely, read bytes %" PRId64 ", bytes should be %d", pReader->pWal->cfg.vgId,
             ret, (int32_t)sizeof(SWalIdxEntry));
      TAOS_RETURN(TSDB_CODE_WAL_FILE_CORRUPTED);
    }
  }

  ret = taosLSeekFile(pLogTFile, entry.offset, SEEK_SET);
  if (ret < 0) {
    wError("vgId:%d, failed to seek log file, index:%" PRId64 ", pos:%" PRId64 ", since %s", pReader->pWal->cfg.vgId,
           ver, entry.offset, terrstr());
    TAOS_RETURN(terrno);
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t walReadChangeFile(SWalReader *pReader, int64_t fileFirstVer) {
  char fnameStr[WAL_FILE_LEN] = {0};

  TAOS_UNUSED(taosCloseFile(&pReader->pIdxFile));
  TAOS_UNUSED(taosCloseFile(&pReader->pLogFile));

  walBuildLogName(pReader->pWal, fileFirstVer, fnameStr);
  TdFilePtr pLogFile = taosOpenFile(fnameStr, TD_FILE_READ);
  if (pLogFile == NULL) {
    wError("vgId:%d, cannot open file %s, since %s", pReader->pWal->cfg.vgId, fnameStr, terrstr());
    TAOS_RETURN(terrno);
  }

  pReader->pLogFile = pLogFile;

  walBuildIdxName(pReader->pWal, fileFirstVer, fnameStr);
  TdFilePtr pIdxFile = taosOpenFile(fnameStr, TD_FILE_READ);
  if (pIdxFile == NULL) {
    wError("vgId:%d, cannot open file %s, since %s", pReader->pWal->cfg.vgId, fnameStr, terrstr());
    TAOS_RETURN(terrno);
  }

  pReader->pIdxFile = pIdxFile;

  pReader->curFileFirstVer = fileFirstVer;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t walReadSeekVerImpl(SWalReader *pReader, int64_t ver) {
  SWal *pWal = pReader->pWal;

  // bsearch in fileSet
  SWalFileInfo tmpInfo;
  tmpInfo.firstVer = ver;
  TAOS_UNUSED(taosThreadRwlockRdlock(&pWal->mutex));
  SWalFileInfo *globalRet = taosArraySearch(pWal->fileInfoSet, &tmpInfo, compareWalFileInfo, TD_LE);
  if (globalRet == NULL) {
    wError("vgId:%d, failed to find WAL log file with index:%" PRId64, pReader->pWal->cfg.vgId, ver);
    TAOS_UNUSED(taosThreadRwlockUnlock(&pWal->mutex));
    TAOS_RETURN(TSDB_CODE_WAL_INVALID_VER);
  }
  SWalFileInfo ret;
  TAOS_MEMCPY(&ret, globalRet, sizeof(SWalFileInfo));
  TAOS_UNUSED(taosThreadRwlockUnlock(&pWal->mutex));
  if (pReader->curFileFirstVer != ret.firstVer) {
    // error code was set inner
    TAOS_CHECK_RETURN(walReadChangeFile(pReader, ret.firstVer));
  }

  // error code was set inner
  TAOS_CHECK_RETURN(walReadSeekFilePos(pReader, ret.firstVer, ver));
  wDebug("vgId:%d, wal version reset from %" PRId64 " to %" PRId64, pReader->pWal->cfg.vgId, pReader->curVersion, ver);

  pReader->curVersion = ver;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t walReaderSeekVer(SWalReader *pReader, int64_t ver) {
  SWal *pWal = pReader->pWal;
  if (ver == pReader->curVersion) {
    wDebug("vgId:%d, wal index:%" PRId64 " match, no need to reset", pReader->pWal->cfg.vgId, ver);

    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  if (ver > pWal->vers.lastVer || ver < pWal->vers.firstVer) {
    wInfo("vgId:%d, invalid index:%" PRId64 ", first index:%" PRId64 ", last index:%" PRId64, pReader->pWal->cfg.vgId,
          ver, pWal->vers.firstVer, pWal->vers.lastVer);

    TAOS_RETURN(TSDB_CODE_WAL_LOG_NOT_EXIST);
  }

  TAOS_CHECK_RETURN(walReadSeekVerImpl(pReader, ver));

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t walFetchHead(SWalReader *pRead, int64_t ver) {
  int64_t code;
  int64_t contLen;
  bool    seeked = false;

  // TODO: valid ver
  if (ver > pRead->pWal->vers.commitVer) {
    TAOS_RETURN(TSDB_CODE_FAILED);
  }

  if (pRead->curVersion != ver) {
    TAOS_CHECK_RETURN(walReaderSeekVer(pRead, ver));

    seeked = true;
  }

  while (1) {
    contLen = taosReadFile(pRead->pLogFile, pRead->pHead, sizeof(SWalCkHead));
    if (contLen == sizeof(SWalCkHead)) {
      break;
    } else if (contLen == 0 && !seeked) {
      TAOS_CHECK_RETURN(walReadSeekVerImpl(pRead, ver));

      seeked = true;
      continue;
    } else {
      if (contLen < 0) {
        TAOS_RETURN(terrno);
      } else {
        TAOS_RETURN(TSDB_CODE_WAL_FILE_CORRUPTED);
      }
    }
  }

  code = walValidHeadCksum(pRead->pHead);

  if (code != 0) {
    wError("vgId:%d, unexpected wal log index:%" PRId64 ", since head checksum not passed, 0x%" PRIx64,
           pRead->pWal->cfg.vgId, ver, pRead->readerId);

    TAOS_RETURN(TSDB_CODE_WAL_FILE_CORRUPTED);
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t walSkipFetchBody(SWalReader *pRead) {
  wDebug("vgId:%d, skip:%" PRId64 ", first index:%" PRId64 ", commit index:%" PRId64 ", last index:%" PRId64
         ", applied index:%" PRId64 ", reader:0x%" PRIx64,
         pRead->pWal->cfg.vgId, pRead->pHead->head.version, pRead->pWal->vers.firstVer, pRead->pWal->vers.commitVer,
         pRead->pWal->vers.lastVer, pRead->pWal->vers.appliedVer, pRead->readerId);

  int32_t plainBodyLen = pRead->pHead->head.bodyLen;
  int32_t cryptedBodyLen = plainBodyLen;
  // TODO: dmchen emun
  if (pRead->pWal->cfg.encryptAlgorithm == 1) {
    cryptedBodyLen = ENCRYPTED_LEN(cryptedBodyLen);
  }
  int64_t ret = taosLSeekFile(pRead->pLogFile, cryptedBodyLen, SEEK_CUR);
  if (ret < 0) {
    TAOS_RETURN(terrno);
  }

  pRead->curVersion++;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t walFetchBody(SWalReader *pRead) {
  SWalCont *pReadHead = &pRead->pHead->head;
  int64_t   ver = pReadHead->version;
  int32_t   vgId = pRead->pWal->cfg.vgId;
  int64_t   id = pRead->readerId;
  SWalVer  *pVer = &pRead->pWal->vers;

  wDebug("vgId:%d, fetch body:%" PRId64 ", first index:%" PRId64 ", commit index:%" PRId64 ", last index:%" PRId64
         ", applied index:%" PRId64 ", reader:0x%" PRIx64,
         vgId, ver, pVer->firstVer, pVer->commitVer, pVer->lastVer, pVer->appliedVer, id);

  int32_t plainBodyLen = pReadHead->bodyLen;
  int32_t cryptedBodyLen = plainBodyLen;

  // TODO: dmchen emun
  if (pRead->pWal->cfg.encryptAlgorithm == 1) {
    cryptedBodyLen = ENCRYPTED_LEN(cryptedBodyLen);
  }

  if (pRead->capacity < cryptedBodyLen) {
    SWalCkHead *ptr = (SWalCkHead *)taosMemoryRealloc(pRead->pHead, sizeof(SWalCkHead) + cryptedBodyLen);
    if (ptr == NULL) {
      TAOS_RETURN(terrno);
    }
    pRead->pHead = ptr;
    pReadHead = &pRead->pHead->head;
    pRead->capacity = cryptedBodyLen;
  }

  if (cryptedBodyLen != taosReadFile(pRead->pLogFile, pReadHead->body, cryptedBodyLen)) {
    if (plainBodyLen < 0) {
      wError("vgId:%d, wal fetch body error:%" PRId64 ", read request index:%" PRId64 ", since %s, reader:0x%" PRIx64,
             vgId, pReadHead->version, ver, tstrerror(terrno), id);
      TAOS_RETURN(terrno);
    } else {
      wError("vgId:%d, wal fetch body error:%" PRId64 ", read request index:%" PRId64
             ", since file corrupted, reader:0x%" PRIx64,
             vgId, pReadHead->version, ver, id);
      TAOS_RETURN(TSDB_CODE_WAL_FILE_CORRUPTED);
    }
  }

  if (pReadHead->version != ver) {
    wError("vgId:%d, wal fetch body error, index:%" PRId64 ", read request index:%" PRId64 ", reader:0x%" PRIx64, vgId,
           pReadHead->version, ver, id);
    TAOS_RETURN(TSDB_CODE_WAL_FILE_CORRUPTED);
  }

  TAOS_CHECK_RETURN(decryptBody(&pRead->pWal->cfg, pRead->pHead, plainBodyLen, __FUNCTION__));

  if (walValidBodyCksum(pRead->pHead) != 0) {
    wError("vgId:%d, wal fetch body error, index:%" PRId64 ", since body checksum not passed, reader:0x%" PRIx64, vgId,
           ver, id);

    TAOS_RETURN(TSDB_CODE_WAL_FILE_CORRUPTED);
  }

  pRead->curVersion++;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t walReadVer(SWalReader *pReader, int64_t ver) {
  wDebug("vgId:%d, wal start to read index:%" PRId64, pReader->pWal->cfg.vgId, ver);
  int64_t contLen;
  int32_t code = 0;
  bool    seeked = false;

  if (walIsEmpty(pReader->pWal)) {
    TAOS_RETURN(TSDB_CODE_WAL_LOG_NOT_EXIST);
  }

  if (ver > pReader->pWal->vers.lastVer || ver < pReader->pWal->vers.firstVer) {
    wDebug("vgId:%d, invalid index:%" PRId64 ", first index:%" PRId64 ", last index:%" PRId64, pReader->pWal->cfg.vgId,
           ver, pReader->pWal->vers.firstVer, pReader->pWal->vers.lastVer);

    TAOS_RETURN(TSDB_CODE_WAL_LOG_NOT_EXIST);
  }

  if (taosThreadMutexLock(&pReader->mutex) != 0) {
    wError("vgId:%d, failed to lock mutex", pReader->pWal->cfg.vgId);
  }

  if (pReader->curVersion != ver) {
    code = walReaderSeekVer(pReader, ver);
    if (code) {
      wError("vgId:%d, unexpected wal log, index:%" PRId64 ", since %s", pReader->pWal->cfg.vgId, ver, terrstr());
      TAOS_UNUSED(taosThreadMutexUnlock(&pReader->mutex));

      TAOS_RETURN(code);
    }

    seeked = true;
  }

  while (1) {
    contLen = taosReadFile(pReader->pLogFile, pReader->pHead, sizeof(SWalCkHead));
    if (contLen == sizeof(SWalCkHead)) {
      break;
    } else if (contLen == 0 && !seeked) {
      code = walReadSeekVerImpl(pReader, ver);
      if (code) {
        TAOS_UNUSED(taosThreadMutexUnlock(&pReader->mutex));

        TAOS_RETURN(code);
      }
      seeked = true;
      continue;
    } else {
      if (contLen < 0) {
        code = terrno;
        wError("vgId:%d, failed to read WAL record head, index:%" PRId64 ", from log file since %s",
               pReader->pWal->cfg.vgId, ver, tstrerror(code));
      } else {
        code = TSDB_CODE_WAL_FILE_CORRUPTED;
        wError("vgId:%d, failed to read WAL record head, index:%" PRId64 ", not enough bytes read, readLen:%" PRId64
               ", "
               "expectedLen:%d",
               pReader->pWal->cfg.vgId, ver, contLen, (int32_t)sizeof(SWalCkHead));
      }
      TAOS_UNUSED(taosThreadMutexUnlock(&pReader->mutex));
      TAOS_RETURN(code);
    }
  }

  code = walValidHeadCksum(pReader->pHead);
  if (code != 0) {
    wError("vgId:%d, unexpected wal log, index:%" PRId64 ", since head checksum not passed", pReader->pWal->cfg.vgId,
           ver);
    TAOS_UNUSED(taosThreadMutexUnlock(&pReader->mutex));

    TAOS_RETURN(TSDB_CODE_WAL_FILE_CORRUPTED);
  }

  int32_t plainBodyLen = pReader->pHead->head.bodyLen;
  int32_t cryptedBodyLen = plainBodyLen;

  // TODO: dmchen emun
  if (pReader->pWal->cfg.encryptAlgorithm == 1) {
    cryptedBodyLen = ENCRYPTED_LEN(cryptedBodyLen);
  }

  if (pReader->capacity < cryptedBodyLen) {
    SWalCkHead *ptr = (SWalCkHead *)taosMemoryRealloc(pReader->pHead, sizeof(SWalCkHead) + cryptedBodyLen);
    if (ptr == NULL) {
      TAOS_UNUSED(taosThreadMutexUnlock(&pReader->mutex));

      TAOS_RETURN(terrno);
    }
    pReader->pHead = ptr;
    pReader->capacity = cryptedBodyLen;
  }

  if ((contLen = taosReadFile(pReader->pLogFile, pReader->pHead->head.body, cryptedBodyLen)) != cryptedBodyLen) {
    if (contLen < 0) {
      code = terrno;
    } else {
      code = TSDB_CODE_WAL_FILE_CORRUPTED;
    }
    wError("vgId:%d, failed to read WAL record body, index:%" PRId64 ", from log file since %s",
           pReader->pWal->cfg.vgId, ver, tstrerror(code));
    TAOS_UNUSED(taosThreadMutexUnlock(&pReader->mutex));
    TAOS_RETURN(code);
  }

  if (pReader->pHead->head.version != ver) {
    wError("vgId:%d, unexpected wal log, index:%" PRId64 ", read request index:%" PRId64, pReader->pWal->cfg.vgId,
           pReader->pHead->head.version, ver);
    //    pReader->curInvalid = 1;
    TAOS_UNUSED(taosThreadMutexUnlock(&pReader->mutex));

    TAOS_RETURN(TSDB_CODE_WAL_FILE_CORRUPTED);
  }

  code = decryptBody(&pReader->pWal->cfg, pReader->pHead, plainBodyLen, __FUNCTION__);
  if (code) {
    TAOS_UNUSED(taosThreadMutexUnlock(&pReader->mutex));

    TAOS_RETURN(code);
  }

  code = walValidBodyCksum(pReader->pHead);
  if (code != 0) {
    wError("vgId:%d, unexpected wal log, index:%" PRId64 ", since body checksum not passed", pReader->pWal->cfg.vgId,
           ver);
    uint32_t readCkSum = walCalcBodyCksum(pReader->pHead->head.body, plainBodyLen);
    uint32_t logCkSum = pReader->pHead->cksumBody;
    wError("checksum written into log:%u, checksum calculated:%u", logCkSum, readCkSum);
    //    pReader->curInvalid = 1;

    TAOS_UNUSED(taosThreadMutexUnlock(&pReader->mutex));

    TAOS_RETURN(TSDB_CODE_WAL_FILE_CORRUPTED);
  }
  pReader->curVersion++;

  TAOS_UNUSED(taosThreadMutexUnlock(&pReader->mutex));

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t decryptBody(SWalCfg *cfg, SWalCkHead *pHead, int32_t plainBodyLen, const char *func) {
  // TODO: dmchen emun
  if (cfg->encryptAlgorithm == 1) {
    int32_t cryptedBodyLen = ENCRYPTED_LEN(plainBodyLen);
    char   *newBody = taosMemoryMalloc(cryptedBodyLen);
    if (!newBody) {
      TAOS_RETURN(terrno);
    }

    SCryptOpts opts;
    opts.len = cryptedBodyLen;
    opts.source = pHead->head.body;
    opts.result = newBody;
    opts.unitLen = 16;
    tstrncpy((char *)opts.key, cfg->encryptKey, sizeof(opts.key));

    int32_t count = CBC_Decrypt(&opts);

    // wDebug("CBC_Decrypt cryptedBodyLen:%d, plainBodyLen:%d, %s", count, plainBodyLen, func);

    (void)memcpy(pHead->head.body, newBody, plainBodyLen);

    taosMemoryFree(newBody);
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

void walReadReset(SWalReader *pReader) {
  if ((taosThreadMutexLock(&pReader->mutex)) != 0) {
    wError("vgId:%d, failed to lock mutex", pReader->pWal->cfg.vgId);
  }

  TAOS_UNUSED(taosCloseFile(&pReader->pIdxFile));
  TAOS_UNUSED(taosCloseFile(&pReader->pLogFile));
  pReader->curFileFirstVer = -1;
  pReader->curVersion = -1;
  TAOS_UNUSED(taosThreadMutexUnlock(&pReader->mutex));
}
