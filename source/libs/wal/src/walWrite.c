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
#include "walInt.h"

int32_t walRestoreFromSnapshot(SWal *pWal, int64_t ver) {
  taosThreadMutexLock(&pWal->mutex);

  void *pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pWal->pRefHash, pIter);
    if (pIter == NULL) break;
    SWalRef *pRef = (SWalRef *)pIter;
    if (pRef->refVer != -1 && pRef->refVer <= ver) {
      taosHashCancelIterate(pWal->pRefHash, pIter);
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
      taosRemoveFile(fnameStr);

      walBuildIdxName(pWal, pFileInfo->firstVer, fnameStr);
      taosRemoveFile(fnameStr);
    }
  }
  walRemoveMeta(pWal);

  pWal->writeCur = -1;
  pWal->totSize = 0;
  pWal->lastRollSeq = -1;

  taosArrayClear(pWal->fileInfoSet);
  pWal->vers.firstVer = -1;
  pWal->vers.lastVer = ver;
  pWal->vers.commitVer = ver - 1;
  pWal->vers.snapshotVer = ver - 1;
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
  ASSERT(pWal->vers.commitVer >= pWal->vers.snapshotVer);
  ASSERT(pWal->vers.commitVer <= pWal->vers.lastVer);
  if (ver < pWal->vers.commitVer) {
    return 0;
  }
  if (ver > pWal->vers.lastVer) {
    terrno = TSDB_CODE_WAL_INVALID_VER;
    return -1;
  }
  pWal->vers.commitVer = ver;
  return 0;
}

int32_t walRollback(SWal *pWal, int64_t ver) {
  taosThreadMutexLock(&pWal->mutex);
  int64_t code;
  char    fnameStr[WAL_FILE_LEN];
  if (ver > pWal->vers.lastVer || ver < pWal->vers.commitVer) {
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

    // delete files
    int fileSetSize = taosArrayGetSize(pWal->fileInfoSet);
    for (int i = pWal->writeCur + 1; i < fileSetSize; i++) {
      walBuildLogName(pWal, ((SWalFileInfo *)taosArrayGet(pWal->fileInfoSet, i))->firstVer, fnameStr);
      taosRemoveFile(fnameStr);
      walBuildIdxName(pWal, ((SWalFileInfo *)taosArrayGet(pWal->fileInfoSet, i))->firstVer, fnameStr);
      taosRemoveFile(fnameStr);
    }
    // pop from fileInfoSet
    taosArraySetSize(pWal->fileInfoSet, pWal->writeCur + 1);
  }

  walBuildIdxName(pWal, walGetCurFileFirstVer(pWal), fnameStr);
  TdFilePtr pIdxFile = taosOpenFile(fnameStr, TD_FILE_WRITE | TD_FILE_READ | TD_FILE_APPEND);

  if (pIdxFile == NULL) {
    ASSERT(0);
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  int64_t idxOff = walGetVerIdxOffset(pWal, ver);
  code = taosLSeekFile(pIdxFile, idxOff, SEEK_SET);
  if (code < 0) {
    ASSERT(0);
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  // read idx file and get log file pos
  SWalIdxEntry entry;
  if (taosReadFile(pIdxFile, &entry, sizeof(SWalIdxEntry)) != sizeof(SWalIdxEntry)) {
    ASSERT(0);
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  ASSERT(entry.ver == ver);

  walBuildLogName(pWal, walGetCurFileFirstVer(pWal), fnameStr);
  TdFilePtr pLogFile = taosOpenFile(fnameStr, TD_FILE_WRITE | TD_FILE_READ | TD_FILE_APPEND);
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
  ASSERT(taosValidFile(pLogFile));
  int64_t size = taosReadFile(pLogFile, &head, sizeof(SWalCkHead));
  if (size != sizeof(SWalCkHead)) {
    ASSERT(0);
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  code = walValidHeadCksum(&head);

  ASSERT(code == 0);
  if (code != 0) {
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    ASSERT(0);
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  if (head.head.version != ver) {
    ASSERT(0);
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }

  // truncate old files
  code = taosFtruncateFile(pLogFile, entry.offset);
  if (code < 0) {
    ASSERT(0);
    terrno = TAOS_SYSTEM_ERROR(errno);
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  code = taosFtruncateFile(pIdxFile, idxOff);
  if (code < 0) {
    ASSERT(0);
    terrno = TAOS_SYSTEM_ERROR(errno);
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  pWal->vers.lastVer = ver - 1;
  if (pWal->vers.lastVer < pWal->vers.firstVer) {
    ASSERT(pWal->vers.lastVer == pWal->vers.firstVer - 1);
    pWal->vers.firstVer = -1;
  }
  ((SWalFileInfo *)taosArrayGetLast(pWal->fileInfoSet))->lastVer = ver - 1;
  ((SWalFileInfo *)taosArrayGetLast(pWal->fileInfoSet))->fileSize = entry.offset;
  if (((SWalFileInfo *)taosArrayGetLast(pWal->fileInfoSet))->lastVer < ver - 1) {
    ASSERT(((SWalFileInfo *)taosArrayGetLast(pWal->fileInfoSet))->fileSize == 0);
    ((SWalFileInfo *)taosArrayGetLast(pWal->fileInfoSet))->firstVer = -1;
  }
  taosCloseFile(&pIdxFile);
  taosCloseFile(&pLogFile);

  taosFsyncFile(pWal->pLogFile);
  taosFsyncFile(pWal->pIdxFile);

  walSaveMeta(pWal);

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

  return 0;
}

int32_t walBeginSnapshot(SWal *pWal, int64_t ver) {
  pWal->vers.verInSnapshotting = ver;
  // check file rolling
  if (pWal->cfg.retentionPeriod == 0) {
    taosThreadMutexLock(&pWal->mutex);
    if (walGetLastFileSize(pWal) != 0) {
      walRollImpl(pWal);
    }
    taosThreadMutexUnlock(&pWal->mutex);
  }

  return 0;
}

int32_t walEndSnapshot(SWal *pWal) {
  int32_t code = 0;
  taosThreadMutexLock(&pWal->mutex);
  int64_t ver = pWal->vers.verInSnapshotting;
  if (ver == -1) {
    code = -1;
    goto END;
  };

  pWal->vers.snapshotVer = ver;
  int ts = taosGetTimestampSec();

  void *pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pWal->pRefHash, pIter);
    if (pIter == NULL) break;
    SWalRef *pRef = *(SWalRef **)pIter;
    if (pRef->refVer == -1) continue;
    ver = TMIN(ver, pRef->refVer);
  }

  int          deleteCnt = 0;
  int64_t      newTotSize = pWal->totSize;
  SWalFileInfo tmp;
  tmp.firstVer = ver;
  // find files safe to delete
  SWalFileInfo *pInfo = taosArraySearch(pWal->fileInfoSet, &tmp, compareWalFileInfo, TD_LE);
  if (pInfo) {
    if (ver >= pInfo->lastVer) {
      pInfo++;
    }
    // iterate files, until the searched result
    for (SWalFileInfo *iter = pWal->fileInfoSet->pData; iter < pInfo; iter++) {
      if ((pWal->cfg.retentionSize != -1 && newTotSize > pWal->cfg.retentionSize) ||
          (pWal->cfg.retentionPeriod != -1 && iter->closeTs + pWal->cfg.retentionPeriod > ts)) {
        // delete according to file size or close time
        deleteCnt++;
        newTotSize -= iter->fileSize;
      }
    }
    int32_t actualDelete = 0;
    char    fnameStr[WAL_FILE_LEN];
    // remove file
    for (int i = 0; i < deleteCnt; i++) {
      pInfo = taosArrayGet(pWal->fileInfoSet, i);
      walBuildLogName(pWal, pInfo->firstVer, fnameStr);
      if (taosRemoveFile(fnameStr) < 0) {
        goto UPDATE_META;
      }
      walBuildIdxName(pWal, pInfo->firstVer, fnameStr);
      if (taosRemoveFile(fnameStr) < 0) {
        ASSERT(0);
      }
      actualDelete++;
    }

  UPDATE_META:
    // make new array, remove files
    taosArrayPopFrontBatch(pWal->fileInfoSet, actualDelete);
    if (taosArrayGetSize(pWal->fileInfoSet) == 0) {
      pWal->writeCur = -1;
      pWal->vers.firstVer = -1;
    } else {
      pWal->vers.firstVer = ((SWalFileInfo *)taosArrayGet(pWal->fileInfoSet, 0))->firstVer;
    }
  }
  pWal->writeCur = taosArrayGetSize(pWal->fileInfoSet) - 1;
  pWal->totSize = newTotSize;
  pWal->vers.verInSnapshotting = -1;

  // save snapshot ver, commit ver
  code = walSaveMeta(pWal);
  if (code < 0) {
    goto END;
  }

END:
  taosThreadMutexUnlock(&pWal->mutex);
  return code;
}

int32_t walRollImpl(SWal *pWal) {
  int32_t code = 0;
  if (pWal->pIdxFile != NULL) {
    code = taosCloseFile(&pWal->pIdxFile);
    if (code != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto END;
    }
  }
  if (pWal->pLogFile != NULL) {
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
  ASSERT(pWal->writeCur >= 0);

  pWal->lastRollSeq = walGetSeq();

  walSaveMeta(pWal);

END:
  return code;
}

static int32_t walWriteIndex(SWal *pWal, int64_t ver, int64_t offset) {
  SWalIdxEntry entry = {.ver = ver, .offset = offset};
  int64_t      idxOffset = taosLSeekFile(pWal->pIdxFile, 0, SEEK_END);
  wDebug("vgId:%d, write index, index:%" PRId64 ", offset:%" PRId64 ", at %" PRId64, pWal->cfg.vgId, ver, offset,
         idxOffset);
  int64_t size = taosWriteFile(pWal->pIdxFile, &entry, sizeof(SWalIdxEntry));
  if (size != sizeof(SWalIdxEntry)) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    // TODO truncate
    return -1;
  }
  return 0;
}

// TODO  gurantee atomicity by truncate failed writing
static FORCE_INLINE int32_t walWriteImpl(SWal *pWal, int64_t index, tmsg_t msgType, SWalSyncInfo syncMeta,
                                         const void *body, int32_t bodyLen) {
  int64_t code = 0;

  int64_t offset = walGetCurFileOffset(pWal);

  pWal->writeHead.head.version = index;
  pWal->writeHead.head.bodyLen = bodyLen;
  pWal->writeHead.head.msgType = msgType;
  pWal->writeHead.head.ingestTs = 0;

  // sync info for sync module
  pWal->writeHead.head.syncMeta = syncMeta;

  pWal->writeHead.cksumHead = walCalcHeadCksum(&pWal->writeHead);
  pWal->writeHead.cksumBody = walCalcBodyCksum(body, bodyLen);

  if (taosWriteFile(pWal->pLogFile, &pWal->writeHead, sizeof(SWalCkHead)) != sizeof(SWalCkHead)) {
    // TODO ftruncate
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%" PRId64 ".log, failed to write since %s", pWal->cfg.vgId, walGetLastFileFirstVer(pWal),
           strerror(errno));
    code = -1;
    goto END;
  }

  if (taosWriteFile(pWal->pLogFile, (char *)body, bodyLen) != bodyLen) {
    // TODO ftruncate
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%" PRId64 ".log, failed to write since %s", pWal->cfg.vgId, walGetLastFileFirstVer(pWal),
           strerror(errno));
    code = -1;
    goto END;
  }

  code = walWriteIndex(pWal, index, offset);
  if (code < 0) {
    // TODO ftruncate
    goto END;
  }

  // set status
  if (pWal->vers.firstVer == -1) pWal->vers.firstVer = index;
  pWal->vers.lastVer = index;
  pWal->totSize += sizeof(SWalCkHead) + bodyLen;
  if (walGetCurFileInfo(pWal)->firstVer == -1) {
    walGetCurFileInfo(pWal)->firstVer = index;
  }
  walGetCurFileInfo(pWal)->lastVer = index;
  walGetCurFileInfo(pWal)->fileSize += sizeof(SWalCkHead) + bodyLen;

  return 0;
END:
  return -1;
}

int64_t walAppendLog(SWal *pWal, tmsg_t msgType, SWalSyncInfo syncMeta, const void *body, int32_t bodyLen) {
  taosThreadMutexLock(&pWal->mutex);

  int64_t index = pWal->vers.lastVer + 1;

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

  ASSERT(pWal->pLogFile != NULL && pWal->pIdxFile != NULL && pWal->writeCur >= 0);

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

  if (pWal->pIdxFile == NULL || pWal->pIdxFile == NULL || pWal->writeCur < 0) {
    if (walInitWriteFile(pWal) < 0) {
      taosThreadMutexUnlock(&pWal->mutex);
      return -1;
    }
  }

  ASSERT(pWal->pIdxFile != NULL && pWal->pLogFile != NULL && pWal->writeCur >= 0);

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
  if (forceFsync || (pWal->cfg.level == TAOS_WAL_FSYNC && pWal->cfg.fsyncPeriod == 0)) {
    wTrace("vgId:%d, fileId:%" PRId64 ".log, do fsync", pWal->cfg.vgId, walGetCurFileFirstVer(pWal));
    if (taosFsyncFile(pWal->pLogFile) < 0) {
      wError("vgId:%d, file:%" PRId64 ".log, fsync failed since %s", pWal->cfg.vgId, walGetCurFileFirstVer(pWal),
             strerror(errno));
    }
  }
}
