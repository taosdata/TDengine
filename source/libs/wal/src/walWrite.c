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

#include "os.h"
#include "taoserror.h"
#include "tchecksum.h"
#include "tglobal.h"
#include "walInt.h"

int32_t walRestoreFromSnapshot(SWal *pWal, int64_t ver) {
  taosThreadMutexLock(&pWal->mutex);

  wInfo("vgId:%d, restore from snapshot, version %" PRId64, pWal->cfg.vgId, ver);

  void *pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pWal->pRefHash, pIter);
    if (pIter == NULL) break;
    SWalRef *pRef = *(SWalRef **)pIter;
    if (pRef->refVer != -1 && pRef->refVer <= ver) {
      taosHashCancelIterate(pWal->pRefHash, pIter);
      taosThreadMutexUnlock(&pWal->mutex);
      return -1;
    }
  }

  taosCloseFile(&pWal->pLogFile);
  taosCloseFile(&pWal->pIdxFile);

  if (pWal->vers.firstVer != -1) {
    int32_t fileSetSize = taosArrayGetSize(pWal->fileInfoSet);
    for (int32_t i = 0; i < fileSetSize; i++) {
      SWalFileInfo *pFileInfo = taosArrayGet(pWal->fileInfoSet, i);
      char          fnameStr[WAL_FILE_LEN];
      walBuildLogName(pWal, pFileInfo->firstVer, fnameStr);
      if (taosRemoveFile(fnameStr) < 0) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        wError("vgId:%d, restore from snapshot, cannot remove file %s since %s", pWal->cfg.vgId, fnameStr, terrstr());
        taosThreadMutexUnlock(&pWal->mutex);
        return -1;
      }
      wInfo("vgId:%d, restore from snapshot, remove file %s", pWal->cfg.vgId, fnameStr);

      walBuildIdxName(pWal, pFileInfo->firstVer, fnameStr);
      if (taosRemoveFile(fnameStr) < 0) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        wError("vgId:%d, cannot remove file %s since %s", pWal->cfg.vgId, fnameStr, terrstr());
        taosThreadMutexUnlock(&pWal->mutex);
        return -1;
      }
      wInfo("vgId:%d, restore from snapshot, remove file %s", pWal->cfg.vgId, fnameStr);
    }
  }
  (void)walRemoveMeta(pWal);

  pWal->writeCur = -1;
  pWal->totSize = 0;
  pWal->lastRollSeq = -1;

  taosArrayClear(pWal->fileInfoSet);
  pWal->vers.firstVer = ver + 1;
  pWal->vers.lastVer = ver;
  pWal->vers.commitVer = ver;
  pWal->vers.snapshotVer = ver;
  pWal->vers.verInSnapshotting = -1;

  taosThreadMutexUnlock(&pWal->mutex);
  return 0;
}

int32_t walApplyVer(SWal *pWal, int64_t ver) {
  // TODO: error check
  pWal->vers.appliedVer = ver;
  return 0;
}

int32_t walCommit(SWal *pWal, int64_t ver) {
  if (ver < pWal->vers.commitVer) {
    return 0;
  }
  if (ver > pWal->vers.lastVer || pWal->vers.commitVer < pWal->vers.snapshotVer) {
    terrno = TSDB_CODE_WAL_INVALID_VER;
    return -1;
  }
  pWal->vers.commitVer = ver;
  return 0;
}

int32_t walRollback(SWal *pWal, int64_t ver) {
  taosThreadMutexLock(&pWal->mutex);
  wInfo("vgId:%d, wal rollback for version %" PRId64, pWal->cfg.vgId, ver);
  int64_t code;
  char    fnameStr[WAL_FILE_LEN];
  if (ver > pWal->vers.lastVer || ver <= pWal->vers.commitVer || ver <= pWal->vers.snapshotVer) {
    terrno = TSDB_CODE_WAL_INVALID_VER;
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }

  // find correct file
  if (ver < walGetLastFileFirstVer(pWal)) {
    // change current files
    code = walChangeWrite(pWal, ver);
    if (code < 0) {
      taosThreadMutexUnlock(&pWal->mutex);
      return -1;
    }

    // delete files in descending order
    int fileSetSize = taosArrayGetSize(pWal->fileInfoSet);
    for (int i = pWal->writeCur + 1; i < fileSetSize; i++) {
      SWalFileInfo *pInfo = taosArrayPop(pWal->fileInfoSet);

      walBuildLogName(pWal, pInfo->firstVer, fnameStr);
      wDebug("vgId:%d, wal remove file %s for rollback", pWal->cfg.vgId, fnameStr);
      taosRemoveFile(fnameStr);
      walBuildIdxName(pWal, pInfo->firstVer, fnameStr);
      wDebug("vgId:%d, wal remove file %s for rollback", pWal->cfg.vgId, fnameStr);
      taosRemoveFile(fnameStr);
    }
  }

  walBuildIdxName(pWal, walGetCurFileFirstVer(pWal), fnameStr);
  taosCloseFile(&pWal->pIdxFile);
  TdFilePtr pIdxFile = taosOpenFile(fnameStr, TD_FILE_WRITE | TD_FILE_READ | TD_FILE_APPEND);

  if (pIdxFile == NULL) {
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  int64_t idxOff = walGetVerIdxOffset(pWal, ver);
  code = taosLSeekFile(pIdxFile, idxOff, SEEK_SET);
  if (code < 0) {
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  // read idx file and get log file pos
  SWalIdxEntry entry;
  if (taosReadFile(pIdxFile, &entry, sizeof(SWalIdxEntry)) != sizeof(SWalIdxEntry)) {
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }

  walBuildLogName(pWal, walGetCurFileFirstVer(pWal), fnameStr);
  taosCloseFile(&pWal->pLogFile);
  TdFilePtr pLogFile = taosOpenFile(fnameStr, TD_FILE_WRITE | TD_FILE_READ | TD_FILE_APPEND);
  wDebug("vgId:%d, wal truncate file %s", pWal->cfg.vgId, fnameStr);
  if (pLogFile == NULL) {
    // TODO
    terrno = TAOS_SYSTEM_ERROR(errno);
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  code = taosLSeekFile(pLogFile, entry.offset, SEEK_SET);
  if (code < 0) {
    // TODO
    terrno = TAOS_SYSTEM_ERROR(errno);
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  // validate offset
  SWalCkHead head;
  int64_t    size = taosReadFile(pLogFile, &head, sizeof(SWalCkHead));
  if (size != sizeof(SWalCkHead)) {
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  code = walValidHeadCksum(&head);

  if (code != 0) {
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  if (head.head.version != ver) {
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }

  // truncate old files
  code = taosFtruncateFile(pLogFile, entry.offset);
  if (code < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  code = taosFtruncateFile(pIdxFile, idxOff);
  if (code < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  pWal->vers.lastVer = ver - 1;
  ((SWalFileInfo *)taosArrayGetLast(pWal->fileInfoSet))->lastVer = ver - 1;
  ((SWalFileInfo *)taosArrayGetLast(pWal->fileInfoSet))->fileSize = entry.offset;

  taosCloseFile(&pIdxFile);
  taosCloseFile(&pLogFile);

  code = walSaveMeta(pWal);
  if (code < 0) {
    wError("vgId:%d, failed to save meta since %s", pWal->cfg.vgId, terrstr());
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }

  // unlock
  taosThreadMutexUnlock(&pWal->mutex);
  return 0;
}

static FORCE_INLINE int32_t walCheckAndRoll(SWal *pWal) {
  if (taosArrayGetSize(pWal->fileInfoSet) == 0) {
    if (walRollImpl(pWal) < 0) {
      return -1;
    }
    return 0;
  }

  int64_t passed = walGetSeq() - pWal->lastRollSeq;
  if (pWal->cfg.rollPeriod != -1 && pWal->cfg.rollPeriod != 0 && passed > pWal->cfg.rollPeriod) {
    if (walRollImpl(pWal) < 0) {
      return -1;
    }
  } else if (pWal->cfg.segSize != -1 && pWal->cfg.segSize != 0 && walGetLastFileSize(pWal) > pWal->cfg.segSize) {
    if (walRollImpl(pWal) < 0) {
      return -1;
    }
  }

  if (walGetLastFileCachedSize(pWal) > tsWalFsyncDataSizeLimit) {
    if (walSaveMeta(pWal) < 0) {
      return -1;
    }
  }

  return 0;
}

int32_t walBeginSnapshot(SWal *pWal, int64_t ver, int64_t logRetention) {
  taosThreadMutexLock(&pWal->mutex);
  ASSERT(logRetention >= 0);
  pWal->vers.verInSnapshotting = ver;
  pWal->vers.logRetention = logRetention;

  wDebug("vgId:%d, wal begin snapshot for version %" PRId64 ", log retention %" PRId64 " first ver %" PRId64
         ", last ver %" PRId64,
         pWal->cfg.vgId, ver, pWal->vers.logRetention, pWal->vers.firstVer, pWal->vers.lastVer);
  // check file rolling
  if (walGetLastFileSize(pWal) != 0) {
    if (walRollImpl(pWal) < 0) {
      wError("vgId:%d, failed to roll wal files since %s", pWal->cfg.vgId, terrstr());
      goto _err;
    }
  }

  taosThreadMutexUnlock(&pWal->mutex);
  return 0;

_err:
  taosThreadMutexUnlock(&pWal->mutex);
  return -1;
}

int32_t walEndSnapshot(SWal *pWal) {
  int32_t code = 0;
  taosThreadMutexLock(&pWal->mutex);
  int64_t ver = pWal->vers.verInSnapshotting;

  wDebug("vgId:%d, wal end snapshot for version %" PRId64 ", log retention %" PRId64 " first ver %" PRId64
         ", last ver %" PRId64,
         pWal->cfg.vgId, ver, pWal->vers.logRetention, pWal->vers.firstVer, pWal->vers.lastVer);

  if (ver == -1) {
    code = -1;
    goto END;
  }

  pWal->vers.snapshotVer = ver;
  int ts = taosGetTimestampSec();
  ver = TMAX(ver - pWal->vers.logRetention, pWal->vers.firstVer - 1);

  // compatible mode for refVer
  bool hasTopic = false;
  int64_t refVer = INT64_MAX;
  void *pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pWal->pRefHash, pIter);
    if (pIter == NULL) break;
    SWalRef *pRef = *(SWalRef **)pIter;
    if (pRef->refVer == -1) continue;
    refVer = TMIN(refVer, pRef->refVer - 1);
    hasTopic = true;
  }
  if (pWal->cfg.retentionPeriod == 0 && hasTopic) {
    wInfo("vgId:%d, wal found refVer:%" PRId64 " in compatible mode, ver:%" PRId64, pWal->cfg.vgId, refVer, ver);
    ver = TMIN(ver, refVer);
  }

  // find files safe to delete
  int          deleteCnt = 0;
  int64_t      newTotSize = pWal->totSize;
  SWalFileInfo tmp = {0};
  tmp.firstVer = ver;
  SWalFileInfo *pInfo = taosArraySearch(pWal->fileInfoSet, &tmp, compareWalFileInfo, TD_LE);

  if (pInfo) {
    wDebug("vgId:%d, wal search found file info. ver:%" PRId64 ", first:%" PRId64 " last:%" PRId64, pWal->cfg.vgId, ver,
           pInfo->firstVer, pInfo->lastVer);
    ASSERT(ver <= pInfo->lastVer);
    if (ver == pInfo->lastVer) {
      pInfo++;
    }

    // iterate files, until the searched result
    // delete according to file size or close time
    SWalFileInfo *pUntil = NULL;
    for (SWalFileInfo *iter = pWal->fileInfoSet->pData; iter < pInfo; iter++) {
      if ((pWal->cfg.retentionSize > 0 && newTotSize > pWal->cfg.retentionSize) ||
          (pWal->cfg.retentionPeriod == 0 ||
           pWal->cfg.retentionPeriod > 0 && iter->closeTs >= 0 && iter->closeTs + pWal->cfg.retentionPeriod < ts)) {
        newTotSize -= iter->fileSize;
        pUntil = iter;
      }
    }
    for (SWalFileInfo *iter = pWal->fileInfoSet->pData; iter <= pUntil; iter++) {
      deleteCnt++;
      taosArrayPush(pWal->toDeleteFiles, iter);
    }

    // make new array, remove files
    taosArrayPopFrontBatch(pWal->fileInfoSet, deleteCnt);
    if (taosArrayGetSize(pWal->fileInfoSet) == 0) {
      pWal->writeCur = -1;
      pWal->vers.firstVer = pWal->vers.lastVer + 1;
    } else {
      pWal->vers.firstVer = ((SWalFileInfo *)taosArrayGet(pWal->fileInfoSet, 0))->firstVer;
    }
  }

  // update meta
  pWal->writeCur = taosArrayGetSize(pWal->fileInfoSet) - 1;
  pWal->totSize = newTotSize;
  pWal->vers.verInSnapshotting = -1;

  code = walSaveMeta(pWal);
  if (code < 0) {
    goto END;
  }

  // delete files
  deleteCnt = taosArrayGetSize(pWal->toDeleteFiles);
  char fnameStr[WAL_FILE_LEN] = {0};
  pInfo = NULL;

  for (int i = 0; i < deleteCnt; i++) {
    pInfo = taosArrayGet(pWal->toDeleteFiles, i);

    walBuildLogName(pWal, pInfo->firstVer, fnameStr);
    if (taosRemoveFile(fnameStr) < 0 && errno != ENOENT) {
      wError("vgId:%d, failed to remove log file %s due to %s", pWal->cfg.vgId, fnameStr, strerror(errno));
      goto END;
    }
    walBuildIdxName(pWal, pInfo->firstVer, fnameStr);
    if (taosRemoveFile(fnameStr) < 0 && errno != ENOENT) {
      wError("vgId:%d, failed to remove idx file %s due to %s", pWal->cfg.vgId, fnameStr, strerror(errno));
      goto END;
    }
  }
  if (pInfo != NULL) {
    wInfo("vgId:%d, wal log files recycled. count:%d, until ver:%" PRId64 ", closeTs:%" PRId64, pWal->cfg.vgId,
          deleteCnt, pInfo->lastVer, pInfo->closeTs);
  }
  taosArrayClear(pWal->toDeleteFiles);

END:
  taosThreadMutexUnlock(&pWal->mutex);
  return code;
}

int32_t walRollImpl(SWal *pWal) {
  int32_t code = 0;

  if (pWal->pIdxFile != NULL) {
    code = taosFsyncFile(pWal->pIdxFile);
    if (code != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto END;
    }
    code = taosCloseFile(&pWal->pIdxFile);
    if (code != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto END;
    }
  }

  if (pWal->pLogFile != NULL) {
    code = taosFsyncFile(pWal->pLogFile);
    if (code != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto END;
    }
    code = taosCloseFile(&pWal->pLogFile);
    if (code != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto END;
    }
  }

  TdFilePtr pIdxFile, pLogFile;
  // create new file
  int64_t newFileFirstVer = pWal->vers.lastVer + 1;
  char    fnameStr[WAL_FILE_LEN];
  walBuildIdxName(pWal, newFileFirstVer, fnameStr);
  pIdxFile = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  if (pIdxFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    code = -1;
    goto END;
  }
  walBuildLogName(pWal, newFileFirstVer, fnameStr);
  pLogFile = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  wDebug("vgId:%d, wal create new file for write:%s", pWal->cfg.vgId, fnameStr);
  if (pLogFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    code = -1;
    goto END;
  }
  // error code was set inner
  code = walRollFileInfo(pWal);
  if (code != 0) {
    goto END;
  }

  // switch file
  pWal->pIdxFile = pIdxFile;
  pWal->pLogFile = pLogFile;
  pWal->writeCur = taosArrayGetSize(pWal->fileInfoSet) - 1;

  pWal->lastRollSeq = walGetSeq();

  code = walSaveMeta(pWal);
  if (code < 0) {
    wError("vgId:%d, failed to save meta since %s", pWal->cfg.vgId, terrstr());
    goto END;
  }

END:
  return code;
}

static int32_t walWriteIndex(SWal *pWal, int64_t ver, int64_t offset) {
  SWalIdxEntry  entry = {.ver = ver, .offset = offset};
  SWalFileInfo *pFileInfo = walGetCurFileInfo(pWal);

  int64_t idxOffset = (entry.ver - pFileInfo->firstVer) * sizeof(SWalIdxEntry);
  wDebug("vgId:%d, write index, index:%" PRId64 ", offset:%" PRId64 ", at %" PRId64, pWal->cfg.vgId, ver, offset,
         idxOffset);

  int64_t size = taosWriteFile(pWal->pIdxFile, &entry, sizeof(SWalIdxEntry));
  if (size != sizeof(SWalIdxEntry)) {
    wError("vgId:%d, failed to write idx entry due to %s. ver:%" PRId64, pWal->cfg.vgId, strerror(errno), ver);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  // check alignment of idx entries
  int64_t endOffset = taosLSeekFile(pWal->pIdxFile, 0, SEEK_END);
  if (endOffset < 0) {
    wFatal("vgId:%d, failed to seek end of WAL idxfile due to %s. ver:%" PRId64 "", pWal->cfg.vgId, strerror(errno),
           ver);
    taosMsleep(100);
    exit(EXIT_FAILURE);
  }
  return 0;
}

static FORCE_INLINE int32_t walWriteImpl(SWal *pWal, int64_t index, tmsg_t msgType, SWalSyncInfo syncMeta,
                                         const void *body, int32_t bodyLen) {
  int64_t code = 0;

  int64_t       offset = walGetCurFileOffset(pWal);
  SWalFileInfo *pFileInfo = walGetCurFileInfo(pWal);

  pWal->writeHead.head.version = index;
  pWal->writeHead.head.bodyLen = bodyLen;
  pWal->writeHead.head.msgType = msgType;
  pWal->writeHead.head.ingestTs = taosGetTimestampUs();

  // sync info for sync module
  pWal->writeHead.head.syncMeta = syncMeta;

  pWal->writeHead.cksumHead = walCalcHeadCksum(&pWal->writeHead);
  pWal->writeHead.cksumBody = walCalcBodyCksum(body, bodyLen);
  wDebug("vgId:%d, wal write log %" PRId64 ", msgType: %s, cksum head %u cksum body %u", pWal->cfg.vgId, index,
         TMSG_INFO(msgType), pWal->writeHead.cksumHead, pWal->writeHead.cksumBody);

  code = walWriteIndex(pWal, index, offset);
  if (code < 0) {
    goto END;
  }

  if (taosWriteFile(pWal->pLogFile, &pWal->writeHead, sizeof(SWalCkHead)) != sizeof(SWalCkHead)) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%" PRId64 ".log, failed to write since %s", pWal->cfg.vgId, walGetLastFileFirstVer(pWal),
           strerror(errno));
    code = -1;
    goto END;
  }

  if (taosWriteFile(pWal->pLogFile, (char *)body, bodyLen) != bodyLen) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%" PRId64 ".log, failed to write since %s", pWal->cfg.vgId, walGetLastFileFirstVer(pWal),
           strerror(errno));
    code = -1;
    goto END;
  }

  // set status
  if (pWal->vers.firstVer == -1) {
    pWal->vers.firstVer = 0;
  }
  pWal->vers.lastVer = index;
  pWal->totSize += sizeof(SWalCkHead) + bodyLen;
  pFileInfo->lastVer = index;
  pFileInfo->fileSize += sizeof(SWalCkHead) + bodyLen;

  return 0;

END:
  // recover in a reverse order
  if (taosFtruncateFile(pWal->pLogFile, offset) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wFatal("vgId:%d, failed to recover WAL logfile from write error since %s, offset:%" PRId64, pWal->cfg.vgId,
           terrstr(), offset);
    taosMsleep(100);
    exit(EXIT_FAILURE);
  }

  int64_t idxOffset = (index - pFileInfo->firstVer) * sizeof(SWalIdxEntry);
  if (taosFtruncateFile(pWal->pIdxFile, idxOffset) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wFatal("vgId:%d, failed to recover WAL idxfile from write error since %s, offset:%" PRId64, pWal->cfg.vgId,
           terrstr(), idxOffset);
    taosMsleep(100);
    exit(EXIT_FAILURE);
  }
  return -1;
}

int64_t walAppendLog(SWal *pWal, int64_t index, tmsg_t msgType, SWalSyncInfo syncMeta, const void *body,
                     int32_t bodyLen) {
  taosThreadMutexLock(&pWal->mutex);

  if (index != pWal->vers.lastVer + 1) {
    terrno = TSDB_CODE_WAL_INVALID_VER;
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }

  if (walCheckAndRoll(pWal) < 0) {
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }

  if (pWal->pLogFile == NULL || pWal->pIdxFile == NULL || pWal->writeCur < 0) {
    if (walInitWriteFile(pWal) < 0) {
      taosThreadMutexUnlock(&pWal->mutex);
      return -1;
    }
  }

  if (walWriteImpl(pWal, index, msgType, syncMeta, body, bodyLen) < 0) {
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }

  taosThreadMutexUnlock(&pWal->mutex);
  return index;
}

int32_t walWriteWithSyncInfo(SWal *pWal, int64_t index, tmsg_t msgType, SWalSyncInfo syncMeta, const void *body,
                             int32_t bodyLen) {
  int32_t code = 0;

  taosThreadMutexLock(&pWal->mutex);

  // concurrency control:
  // if logs are write with assigned index,
  // smaller index must be write before larger one
  if (index != pWal->vers.lastVer + 1) {
    terrno = TSDB_CODE_WAL_INVALID_VER;
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }

  if (walCheckAndRoll(pWal) < 0) {
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }

  if (pWal->pIdxFile == NULL || pWal->pLogFile == NULL || pWal->writeCur < 0) {
    if (walInitWriteFile(pWal) < 0) {
      taosThreadMutexUnlock(&pWal->mutex);
      return -1;
    }
  }

  if (walWriteImpl(pWal, index, msgType, syncMeta, body, bodyLen) < 0) {
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }

  taosThreadMutexUnlock(&pWal->mutex);
  return code;
}

int32_t walWrite(SWal *pWal, int64_t index, tmsg_t msgType, const void *body, int32_t bodyLen) {
  SWalSyncInfo syncMeta = {
      .isWeek = -1,
      .seqNum = UINT64_MAX,
      .term = UINT64_MAX,
  };
  return walWriteWithSyncInfo(pWal, index, msgType, syncMeta, body, bodyLen);
}

void walFsync(SWal *pWal, bool forceFsync) {
  taosThreadMutexLock(&pWal->mutex);
  if (forceFsync || (pWal->cfg.level == TAOS_WAL_FSYNC && pWal->cfg.fsyncPeriod == 0)) {
    wTrace("vgId:%d, fileId:%" PRId64 ".log, do fsync", pWal->cfg.vgId, walGetCurFileFirstVer(pWal));
    if (taosFsyncFile(pWal->pLogFile) < 0) {
      wError("vgId:%d, file:%" PRId64 ".log, fsync failed since %s", pWal->cfg.vgId, walGetCurFileFirstVer(pWal),
             strerror(errno));
    }
  }
  taosThreadMutexUnlock(&pWal->mutex);
}
