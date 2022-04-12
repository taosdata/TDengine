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

#define _DEFAULT_SOURCE

#include "os.h"
#include "taoserror.h"
#include "tchecksum.h"
#include "walInt.h"

int32_t walCommit(SWal *pWal, int64_t ver) {
  ASSERT(pWal->vers.commitVer >= pWal->vers.snapshotVer);
  ASSERT(pWal->vers.commitVer <= pWal->vers.lastVer);
  if (ver < pWal->vers.commitVer || ver > pWal->vers.lastVer) {
    terrno = TSDB_CODE_WAL_INVALID_VER;
    return -1;
  }
  pWal->vers.commitVer = ver;
  return 0;
}

int32_t walRollback(SWal *pWal, int64_t ver) {
  int  code;
  char fnameStr[WAL_FILE_LEN];
  if (ver > pWal->vers.lastVer || ver < pWal->vers.commitVer) {
    terrno = TSDB_CODE_WAL_INVALID_VER;
    return -1;
  }
  taosThreadMutexLock(&pWal->mutex);

  // find correct file
  if (ver < walGetLastFileFirstVer(pWal)) {
    // change current files
    code = walChangeWrite(pWal, ver);
    if (code < 0) {
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
  TdFilePtr pIdxTFile = taosOpenFile(fnameStr, TD_FILE_WRITE | TD_FILE_READ);

  // TODO:change to deserialize function
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
  // TODO:change to deserialize function
  SWalIdxEntry entry;
  if (taosReadFile(pIdxTFile, &entry, sizeof(SWalIdxEntry)) != sizeof(SWalIdxEntry)) {
    taosThreadMutexUnlock(&pWal->mutex);
    return -1;
  }
  ASSERT(entry.ver == ver);

  walBuildLogName(pWal, walGetCurFileFirstVer(pWal), fnameStr);
  TdFilePtr pLogTFile = taosOpenFile(fnameStr, TD_FILE_WRITE | TD_FILE_READ);
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
  SWalHead head;
  ASSERT(taosValidFile(pLogTFile));
  int size = taosReadFile(pLogTFile, &head, sizeof(SWalHead));
  if (size != sizeof(SWalHead)) {
    return -1;
  }
  code = walValidHeadCksum(&head);

  ASSERT(code == 0);
  if (code != 0) {
    return -1;
  }
  if (head.head.version != ver) {
    // TODO
    return -1;
  }
  // truncate old files
  code = taosFtruncateFile(pLogTFile, entry.offset);
  if (code < 0) {
    return -1;
  }
  code = taosFtruncateFile(pIdxTFile, idxOff);
  if (code < 0) {
    return -1;
  }
  pWal->vers.lastVer = ver - 1;
  ((SWalFileInfo *)taosArrayGetLast(pWal->fileInfoSet))->lastVer = ver - 1;
  ((SWalFileInfo *)taosArrayGetLast(pWal->fileInfoSet))->fileSize = entry.offset;

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
    SWalFileInfo *pInfo = taosArrayGet(pWal->fileInfoSet, i);
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
  int code = 0;
  if (pWal->pWriteIdxTFile != NULL) {
    code = taosCloseFile(&pWal->pWriteIdxTFile);
    if (code != 0) {
      return -1;
    }
  }
  if (pWal->pWriteLogTFile != NULL) {
    code = taosCloseFile(&pWal->pWriteLogTFile);
    if (code != 0) {
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
  int          size = taosWriteFile(pWal->pWriteIdxTFile, &entry, sizeof(SWalIdxEntry));
  if (size != sizeof(SWalIdxEntry)) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    // TODO truncate
    return -1;
  }
  return 0;
}

int64_t walWriteWithSyncInfo(SWal *pWal, int64_t index, tmsg_t msgType, SSyncLogMeta syncMeta, const void *body,
                             int32_t bodyLen) {
  if (pWal == NULL) return -1;
  int code = 0;

  // no wal
  if (pWal->cfg.level == TAOS_WAL_NOLOG) return 0;

  if (index == pWal->vers.lastVer + 1) {
    if (taosArrayGetSize(pWal->fileInfoSet) == 0) {
      pWal->vers.firstVer = index;
      code = walRoll(pWal);
      ASSERT(code == 0);
    } else {
      int64_t passed = walGetSeq() - pWal->lastRollSeq;
      if (pWal->cfg.rollPeriod != -1 && pWal->cfg.rollPeriod != 0 && passed > pWal->cfg.rollPeriod) {
        walRoll(pWal);
      } else if (pWal->cfg.segSize != -1 && pWal->cfg.segSize != 0 && walGetLastFileSize(pWal) > pWal->cfg.segSize) {
        walRoll(pWal);
      }
    }
  } else {
    // reject skip log or rewrite log
    // must truncate explicitly first
    terrno = TSDB_CODE_WAL_INVALID_VER;
    return -1;
  }
  /*if (!tfValid(pWal->pWriteLogTFile)) return -1;*/

  ASSERT(pWal->writeCur >= 0);

  taosThreadMutexLock(&pWal->mutex);

  if (pWal->pWriteIdxTFile == NULL || pWal->pWriteLogTFile == NULL) {
    walSetWrite(pWal);
    taosLSeekFile(pWal->pWriteLogTFile, 0, SEEK_END);
    taosLSeekFile(pWal->pWriteIdxTFile, 0, SEEK_END);
  }

  pWal->writeHead.head.version = index;

  int64_t offset = walGetCurFileOffset(pWal);
  pWal->writeHead.head.len = bodyLen;
  pWal->writeHead.head.msgType = msgType;

  // sync info
  pWal->writeHead.head.syncMeta = syncMeta;

  pWal->writeHead.cksumHead = walCalcHeadCksum(&pWal->writeHead);
  pWal->writeHead.cksumBody = walCalcBodyCksum(body, bodyLen);

  if (taosWriteFile(pWal->pWriteLogTFile, &pWal->writeHead, sizeof(SWalHead)) != sizeof(SWalHead)) {
    // ftruncate
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%" PRId64 ".log, failed to write since %s", pWal->cfg.vgId, walGetLastFileFirstVer(pWal),
           strerror(errno));
    return -1;
  }

  if (taosWriteFile(pWal->pWriteLogTFile, (char *)body, bodyLen) != bodyLen) {
    // ftruncate
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
  pWal->vers.lastVer = index;
  pWal->totSize += sizeof(SWalHead) + bodyLen;
  walGetCurFileInfo(pWal)->lastVer = index;
  walGetCurFileInfo(pWal)->fileSize += sizeof(SWalHead) + bodyLen;

  taosThreadMutexUnlock(&pWal->mutex);

  return 0;
}

int64_t walWrite(SWal *pWal, int64_t index, tmsg_t msgType, const void *body, int32_t bodyLen) {
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

/*static int walValidateOffset(SWal* pWal, int64_t ver) {*/
/*int code = 0;*/
/*SWalHead *pHead = NULL;*/
/*code = (int)walRead(pWal, &pHead, ver);*/
/*if(pHead->head.version != ver) {*/
/*return -1;*/
/*}*/
/*return 0;*/
/*}*/

/*static int64_t walGetOffset(SWal* pWal, int64_t ver) {*/
/*int code = walSeekVer(pWal, ver);*/
/*if(code != 0) {*/
/*return -1;*/
/*}*/

/*code = walValidateOffset(pWal, ver);*/
/*if(code != 0) {*/
/*return -1;*/
/*}*/

/*return 0;*/
/*}*/
