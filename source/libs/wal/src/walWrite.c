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
    if (pRef->ver != -1) {
      taosHashCancelIterate(pWal->pRefHash, pIter);
      return -1;
    }
  }

  taosCloseFile(&pWal->pWriteLogTFile);
  taosCloseFile(&pWal->pWriteIdxTFile);

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
    for (int i = pWal->writeCur; i < fileSetSize; i++) {
      walBuildLogName(pWal, ((SWalFileInfo *)taosArrayGet(pWal->fileInfoSet, i))->firstVer, fnameStr);
      taosRemoveFile(fnameStr);
      walBuildIdxName(pWal, ((SWalFileInfo *)taosArrayGet(pWal->fileInfoSet, i))->firstVer, fnameStr);
      taosRemoveFile(fnameStr);
    }
    // pop from fileInfoSet
    taosArraySetSize(pWal->fileInfoSet, pWal->writeCur + 1);
  }

  walBuildIdxName(pWal, walGetCurFileFirstVer(pWal), fnameStr);
  TdFilePtr pIdxTFile = taosOpenFile(fnameStr, TD_FILE_WRITE | TD_FILE_READ | TD_FILE_APPEND);

  if (pIdxTFile == NULL) {
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  int64_t idxOff = walGetVerIdxOffset(pWal, ver);
  code = taosLSeekFile(pIdxTFile, idxOff, SEEK_SET);
  if (code < 0) {
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  // read idx file and get log file pos
  SWalIdxEntry entry;
  if (taosReadFile(pIdxTFile, &entry, sizeof(SWalIdxEntry)) != sizeof(SWalIdxEntry)) {
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  ASSERT(entry.ver == ver);

  walBuildLogName(pWal, walGetCurFileFirstVer(pWal), fnameStr);
  TdFilePtr pLogTFile = taosOpenFile(fnameStr, TD_FILE_WRITE | TD_FILE_READ | TD_FILE_APPEND);
  if (pLogTFile == NULL) {
    // TODO
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  code = taosLSeekFile(pLogTFile, entry.offset, SEEK_SET);
  if (code < 0) {
    // TODO
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  // validate offset
  SWalCkHead head;
  ASSERT(taosValidFile(pLogTFile));
  int64_t size = taosReadFile(pLogTFile, &head, sizeof(SWalCkHead));
  if (size != sizeof(SWalCkHead)) {
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
  code = taosFtruncateFile(pLogTFile, entry.offset);
  if (code < 0) {
    ASSERT(0);
    terrno = TAOS_SYSTEM_ERROR(errno);
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  code = taosFtruncateFile(pIdxTFile, idxOff);
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
  taosCloseFile(&pIdxTFile);
  taosCloseFile(&pLogTFile);

  // unlock
  taosThreadMutexUnlock(&pWal->mutex);
  return 0;
}

int32_t walBeginSnapshot(SWal *pWal, int64_t ver) {
  pWal->vers.verInSnapshotting = ver;
  // check file rolling
  if (pWal->cfg.retentionPeriod == 0) {
    walRoll(pWal);
  }

  return 0;
}

int32_t walEndSnapshot(SWal *pWal) {
  int64_t ver = pWal->vers.verInSnapshotting;
  if (ver == -1) return 0;

  pWal->vers.snapshotVer = ver;
  int ts = taosGetTimestampSec();

  int          deleteCnt = 0;
  int64_t      newTotSize = pWal->totSize;
  SWalFileInfo tmp;
  tmp.firstVer = ver;
  // find files safe to delete
  SWalFileInfo *pInfo = taosArraySearch(pWal->fileInfoSet, &tmp, compareWalFileInfo, TD_LE);
  if (ver >= pInfo->lastVer) {
    pInfo++;
  }
  // iterate files, until the searched result
  for (SWalFileInfo *iter = pWal->fileInfoSet->pData; iter < pInfo; iter++) {
    if ((pWal->cfg.retentionSize != -1 && pWal->totSize > pWal->cfg.retentionSize) ||
        (pWal->cfg.retentionPeriod != -1 && iter->closeTs + pWal->cfg.retentionPeriod > ts)) {
      // delete according to file size or close time
      deleteCnt++;
      newTotSize -= iter->fileSize;
    }
  }
  char fnameStr[WAL_FILE_LEN];
  // remove file
  for (int i = 0; i < deleteCnt; i++) {
    pInfo = taosArrayGet(pWal->fileInfoSet, i);
    walBuildLogName(pWal, pInfo->firstVer, fnameStr);
    taosRemoveFile(fnameStr);
    walBuildIdxName(pWal, pInfo->firstVer, fnameStr);
    taosRemoveFile(fnameStr);
  }

  // make new array, remove files
  taosArrayPopFrontBatch(pWal->fileInfoSet, deleteCnt);
  if (taosArrayGetSize(pWal->fileInfoSet) == 0) {
    pWal->writeCur = -1;
    pWal->vers.firstVer = -1;
  } else {
    pWal->vers.firstVer = ((SWalFileInfo *)taosArrayGet(pWal->fileInfoSet, 0))->firstVer;
  }
  pWal->writeCur = taosArrayGetSize(pWal->fileInfoSet) - 1;
  pWal->totSize = newTotSize;
  pWal->vers.verInSnapshotting = -1;

  // save snapshot ver, commit ver
  int code = walSaveMeta(pWal);
  if (code < 0) {
    return -1;
  }

  return 0;
}

int walRoll(SWal *pWal) {
  int32_t code = 0;
  if (pWal->pWriteIdxTFile != NULL) {
    code = taosCloseFile(&pWal->pWriteIdxTFile);
    if (code != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
  }
  if (pWal->pWriteLogTFile != NULL) {
    code = taosCloseFile(&pWal->pWriteLogTFile);
    if (code != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
  }
  TdFilePtr pIdxTFile, pLogTFile;
  // create new file
  int64_t newFileFirstVersion = pWal->vers.lastVer + 1;
  char    fnameStr[WAL_FILE_LEN];
  walBuildIdxName(pWal, newFileFirstVersion, fnameStr);
  pIdxTFile = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  if (pIdxTFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  walBuildLogName(pWal, newFileFirstVersion, fnameStr);
  pLogTFile = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  if (pLogTFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  // terrno set inner
  code = walRollFileInfo(pWal);
  if (code != 0) {
    return -1;
  }

  // switch file
  pWal->pWriteIdxTFile = pIdxTFile;
  pWal->pWriteLogTFile = pLogTFile;
  pWal->writeCur = taosArrayGetSize(pWal->fileInfoSet) - 1;
  ASSERT(pWal->writeCur >= 0);

  pWal->lastRollSeq = walGetSeq();
  return 0;
}

static int walWriteIndex(SWal *pWal, int64_t ver, int64_t offset) {
  SWalIdxEntry entry = {.ver = ver, .offset = offset};
  int64_t      idxOffset = taosLSeekFile(pWal->pWriteIdxTFile, 0, SEEK_END);
  wDebug("write index, ver:%" PRId64 ", offset:%" PRId64 ", at %" PRId64, ver, offset, idxOffset);
  int64_t size = taosWriteFile(pWal->pWriteIdxTFile, &entry, sizeof(SWalIdxEntry));
  if (size != sizeof(SWalIdxEntry)) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    // TODO truncate
    return -1;
  }
  return 0;
}

int32_t walWriteWithSyncInfo(SWal *pWal, int64_t index, tmsg_t msgType, SSyncLogMeta syncMeta, const void *body,
                             int32_t bodyLen) {
  int32_t code = 0;

  // no wal
  if (pWal->cfg.level == TAOS_WAL_NOLOG) return 0;

  if (bodyLen > TSDB_MAX_WAL_SIZE) {
    terrno = TSDB_CODE_WAL_SIZE_LIMIT;
    return -1;
  }
  taosThreadMutexLock(&pWal->mutex);

  if (index == pWal->vers.lastVer + 1) {
    if (taosArrayGetSize(pWal->fileInfoSet) == 0) {
      pWal->vers.firstVer = index;
      if (walRoll(pWal) < 0) {
        taosThreadMutexUnlock(&pWal->mutex);
        return -1;
      }
    } else {
      int64_t passed = walGetSeq() - pWal->lastRollSeq;
      if (pWal->cfg.rollPeriod != -1 && pWal->cfg.rollPeriod != 0 && passed > pWal->cfg.rollPeriod) {
        if (walRoll(pWal) < 0) {
          taosThreadMutexUnlock(&pWal->mutex);
          return -1;
        }
      } else if (pWal->cfg.segSize != -1 && pWal->cfg.segSize != 0 && walGetLastFileSize(pWal) > pWal->cfg.segSize) {
        if (walRoll(pWal) < 0) {
          taosThreadMutexUnlock(&pWal->mutex);
          return -1;
        }
      }
    }
  } else {
    // reject skip log or rewrite log
    // must truncate explicitly first
    terrno = TSDB_CODE_WAL_INVALID_VER;
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }

  /*if (!tfValid(pWal->pWriteLogTFile)) return -1;*/

  ASSERT(pWal->writeCur >= 0);

  if (pWal->pWriteIdxTFile == NULL || pWal->pWriteLogTFile == NULL) {
    walSetWrite(pWal);
    taosLSeekFile(pWal->pWriteLogTFile, 0, SEEK_END);
    taosLSeekFile(pWal->pWriteIdxTFile, 0, SEEK_END);
  }

  pWal->writeHead.head.version = index;

  int64_t offset = walGetCurFileOffset(pWal);
  pWal->writeHead.head.bodyLen = bodyLen;
  pWal->writeHead.head.msgType = msgType;

  // sync info for sync module
  pWal->writeHead.head.syncMeta = syncMeta;

  pWal->writeHead.cksumHead = walCalcHeadCksum(&pWal->writeHead);
  pWal->writeHead.cksumBody = walCalcBodyCksum(body, bodyLen);

  if (taosWriteFile(pWal->pWriteLogTFile, &pWal->writeHead, sizeof(SWalCkHead)) != sizeof(SWalCkHead)) {
    // TODO ftruncate
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%" PRId64 ".log, failed to write since %s", pWal->cfg.vgId, walGetLastFileFirstVer(pWal),
           strerror(errno));
    return -1;
  }

  if (taosWriteFile(pWal->pWriteLogTFile, (char *)body, bodyLen) != bodyLen) {
    // TODO ftruncate
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%" PRId64 ".log, failed to write since %s", pWal->cfg.vgId, walGetLastFileFirstVer(pWal),
           strerror(errno));
    return -1;
  }

  code = walWriteIndex(pWal, index, offset);
  if (code != 0) {
    // TODO
    return -1;
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

  taosThreadMutexUnlock(&pWal->mutex);

  return 0;
}

int32_t walWrite(SWal *pWal, int64_t index, tmsg_t msgType, const void *body, int32_t bodyLen) {
  SSyncLogMeta syncMeta = {
      .isWeek = -1,
      .seqNum = UINT64_MAX,
      .term = UINT64_MAX,
  };
  return walWriteWithSyncInfo(pWal, index, msgType, syncMeta, body, bodyLen);
}

void walFsync(SWal *pWal, bool forceFsync) {
  if (forceFsync || (pWal->cfg.level == TAOS_WAL_FSYNC && pWal->cfg.fsyncPeriod == 0)) {
    wTrace("vgId:%d, fileId:%" PRId64 ".log, do fsync", pWal->cfg.vgId, walGetCurFileFirstVer(pWal));
    if (taosFsyncFile(pWal->pWriteLogTFile) < 0) {
      wError("vgId:%d, file:%" PRId64 ".log, fsync failed since %s", pWal->cfg.vgId, walGetCurFileFirstVer(pWal),
             strerror(errno));
    }
  }
}
