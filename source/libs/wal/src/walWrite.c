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
#include "os.h"
#include "taoserror.h"
#include "tchecksum.h"
#include "tglobal.h"
#include "walInt.h"

int32_t walRestoreFromSnapshot(SWal *pWal, int64_t ver) {
  int32_t code = 0;

  TAOS_UNUSED(taosThreadRwlockWrlock(&pWal->mutex));

  wInfo("vgId:%d, restore from snapshot, index:%" PRId64, pWal->cfg.vgId, ver);

  void *pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pWal->pRefHash, pIter);
    if (pIter == NULL) break;
    SWalRef *pRef = *(SWalRef **)pIter;
    if (pRef->refVer != -1 && pRef->refVer <= ver) {
      taosHashCancelIterate(pWal->pRefHash, pIter);
      TAOS_UNUSED(taosThreadRwlockUnlock(&pWal->mutex));

      TAOS_RETURN(TSDB_CODE_FAILED);
    }
  }

  TAOS_UNUSED(taosCloseFile(&pWal->pLogFile));
  TAOS_UNUSED(taosCloseFile(&pWal->pIdxFile));

  if (pWal->vers.firstVer != -1) {
    int32_t fileSetSize = taosArrayGetSize(pWal->fileInfoSet);
    for (int32_t i = 0; i < fileSetSize; i++) {
      SWalFileInfo *pFileInfo = taosArrayGet(pWal->fileInfoSet, i);
      char          fnameStr[WAL_FILE_LEN];
      walBuildLogName(pWal, pFileInfo->firstVer, fnameStr);
      if (taosRemoveFile(fnameStr) < 0) {
        wError("vgId:%d, restore from snapshot, cannot remove file %s since %s", pWal->cfg.vgId, fnameStr, terrstr());
        TAOS_UNUSED(taosThreadRwlockUnlock(&pWal->mutex));

        TAOS_RETURN(terrno);
      }
      wInfo("vgId:%d, restore from snapshot, remove file %s", pWal->cfg.vgId, fnameStr);

      walBuildIdxName(pWal, pFileInfo->firstVer, fnameStr);
      if (taosRemoveFile(fnameStr) < 0) {
        wError("vgId:%d, cannot remove file %s since %s", pWal->cfg.vgId, fnameStr, terrstr());
        TAOS_UNUSED(taosThreadRwlockUnlock(&pWal->mutex));

        TAOS_RETURN(terrno);
      }
      wInfo("vgId:%d, restore from snapshot, remove file %s", pWal->cfg.vgId, fnameStr);
    }
  }

  TAOS_CHECK_RETURN(walRemoveMeta(pWal));

  pWal->writeCur = -1;
  pWal->totSize = 0;
  pWal->lastRollSeq = -1;

  taosArrayClear(pWal->fileInfoSet);
  pWal->vers.firstVer = ver + 1;
  pWal->vers.lastVer = ver;
  pWal->vers.commitVer = ver;
  pWal->vers.snapshotVer = ver;
  pWal->vers.verInSnapshotting = -1;

  TAOS_UNUSED(taosThreadRwlockUnlock(&pWal->mutex));

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

void walApplyVer(SWal *pWal, int64_t ver) {
  // TODO: error check
  pWal->vers.appliedVer = ver;
}

int32_t walCommit(SWal *pWal, int64_t ver) {
  if (ver < pWal->vers.commitVer) TAOS_RETURN(TSDB_CODE_SUCCESS);
  if (ver > pWal->vers.lastVer || pWal->vers.commitVer < pWal->vers.snapshotVer) TAOS_RETURN(TSDB_CODE_WAL_INVALID_VER);

  pWal->vers.commitVer = ver;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int64_t walChangeWrite(SWal *pWal, int64_t ver) {
  int       code;
  TdFilePtr pIdxTFile, pLogTFile;
  char      fnameStr[WAL_FILE_LEN];
  if (pWal->pLogFile != NULL) {
    if (pWal->cfg.level != TAOS_WAL_SKIP && (code = taosFsyncFile(pWal->pLogFile)) != 0) {
      return -1;
    }
    code = taosCloseFile(&pWal->pLogFile);
    if (code != 0) {
      return -1;
    }
  }
  if (pWal->pIdxFile != NULL) {
    if (pWal->cfg.level != TAOS_WAL_SKIP && (code = taosFsyncFile(pWal->pIdxFile)) != 0) {
      return -1;
    }
    code = taosCloseFile(&pWal->pIdxFile);
    if (code != 0) {
      return -1;
    }
  }

  SWalFileInfo tmpInfo;
  tmpInfo.firstVer = ver;
  // bsearch in fileSet
  int32_t idx = taosArraySearchIdx(pWal->fileInfoSet, &tmpInfo, compareWalFileInfo, TD_LE);
  /*A(idx != -1);*/
  SWalFileInfo *pFileInfo = taosArrayGet(pWal->fileInfoSet, idx);

  int64_t fileFirstVer = pFileInfo->firstVer;
  walBuildIdxName(pWal, fileFirstVer, fnameStr);
  pIdxTFile = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  if (pIdxTFile == NULL) {
    pWal->pIdxFile = NULL;

    return -1;
  }
  walBuildLogName(pWal, fileFirstVer, fnameStr);
  pLogTFile = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  if (pLogTFile == NULL) {
    TAOS_UNUSED(taosCloseFile(&pIdxTFile));
    pWal->pLogFile = NULL;

    return -1;
  }

  pWal->pLogFile = pLogTFile;
  pWal->pIdxFile = pIdxTFile;
  pWal->writeCur = idx;

  return fileFirstVer;
}

int32_t walRollback(SWal *pWal, int64_t ver) {
  TAOS_UNUSED(taosThreadRwlockWrlock(&pWal->mutex));
  wInfo("vgId:%d, wal rollback for index:%" PRId64, pWal->cfg.vgId, ver);
  int32_t   code = 0;
  int32_t   lino = 0;
  int64_t   ret;
  char      fnameStr[WAL_FILE_LEN];
  TdFilePtr pIdxFile = NULL, pLogFile = NULL;
  if (ver > pWal->vers.lastVer || ver <= pWal->vers.commitVer || ver <= pWal->vers.snapshotVer) {
    code = TSDB_CODE_WAL_INVALID_VER;
    goto _exit;
  }

  // find correct file
  if (ver < walGetLastFileFirstVer(pWal)) {
    // change current files
    TAOS_CHECK_EXIT_SET_CODE(walChangeWrite(pWal, ver), code, terrno);

    // delete files in descending order
    int fileSetSize = taosArrayGetSize(pWal->fileInfoSet);
    for (int i = pWal->writeCur + 1; i < fileSetSize; i++) {
      SWalFileInfo *pInfo = taosArrayPop(pWal->fileInfoSet);

      walBuildLogName(pWal, pInfo->firstVer, fnameStr);
      wDebug("vgId:%d, wal remove file %s for rollback", pWal->cfg.vgId, fnameStr);
      if (taosRemoveFile(fnameStr) != 0) {
        wWarn("vgId:%d, failed to remove file %s for rollback since %s", pWal->cfg.vgId, fnameStr, terrstr());
      }
      walBuildIdxName(pWal, pInfo->firstVer, fnameStr);
      wDebug("vgId:%d, wal remove file %s for rollback", pWal->cfg.vgId, fnameStr);
      if (taosRemoveFile(fnameStr) != 0) {
        wWarn("vgId:%d, failed to remove file %s for rollback since %s", pWal->cfg.vgId, fnameStr, terrstr());
      }
    }
  }

  walBuildIdxName(pWal, walGetCurFileFirstVer(pWal), fnameStr);
  pIdxFile = taosOpenFile(fnameStr, TD_FILE_WRITE | TD_FILE_READ | TD_FILE_APPEND);
  TSDB_CHECK_NULL(pIdxFile, code, lino, _exit, terrno);
  int64_t idxOff = walGetVerIdxOffset(pWal, ver);
  ret = taosLSeekFile(pIdxFile, idxOff, SEEK_SET);
  if (ret < 0) {
    code = terrno;
    goto _exit;
  }
  // read idx file and get log file pos
  SWalIdxEntry entry;
  if (taosReadFile(pIdxFile, &entry, sizeof(SWalIdxEntry)) != sizeof(SWalIdxEntry)) {
    code = terrno;
    goto _exit;
  }

  walBuildLogName(pWal, walGetCurFileFirstVer(pWal), fnameStr);
  pLogFile = taosOpenFile(fnameStr, TD_FILE_WRITE | TD_FILE_READ | TD_FILE_APPEND);
  wDebug("vgId:%d, wal truncate file %s", pWal->cfg.vgId, fnameStr);
  TSDB_CHECK_NULL(pLogFile, code, lino, _exit, terrno);

  ret = taosLSeekFile(pLogFile, entry.offset, SEEK_SET);
  if (ret < 0) {
    // TODO
    code = terrno;
    goto _exit;
  }
  // validate offset
  SWalCkHead head;
  int64_t    size = taosReadFile(pLogFile, &head, sizeof(SWalCkHead));
  if (size != sizeof(SWalCkHead)) {
    code = terrno;
    goto _exit;
  }
  code = walValidHeadCksum(&head);

  if (code != 0 || head.head.version != ver) {
    code = TSDB_CODE_WAL_FILE_CORRUPTED;
    goto _exit;
  }

  // truncate old files
  if ((code = taosFtruncateFile(pLogFile, entry.offset)) < 0) goto _exit;

  if ((code = taosFtruncateFile(pIdxFile, idxOff)) < 0) goto _exit;

  pWal->vers.lastVer = ver - 1;
  ((SWalFileInfo *)taosArrayGetLast(pWal->fileInfoSet))->lastVer = ver - 1;
  ((SWalFileInfo *)taosArrayGetLast(pWal->fileInfoSet))->fileSize = entry.offset;

  TAOS_CHECK_EXIT(walSaveMeta(pWal));

_exit:
  if (code != 0) {
    wError("vgId:%d, %s failed at line %d since %s", pWal->cfg.vgId, __func__, lino, tstrerror(code));
  }
  TAOS_UNUSED(taosCloseFile(&pIdxFile));
  TAOS_UNUSED(taosCloseFile(&pLogFile));
  TAOS_UNUSED(taosThreadRwlockUnlock(&pWal->mutex));

  TAOS_RETURN(code);
}

int32_t walRollImpl(SWal *pWal) {
  int32_t code = 0, lino = 0;

  if (pWal->cfg.level == TAOS_WAL_SKIP && pWal->pIdxFile != NULL && pWal->pLogFile != NULL) {
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }
  if (pWal->pIdxFile != NULL) {
    if ((code = taosFsyncFile(pWal->pIdxFile)) != 0) {
      TAOS_CHECK_GOTO(terrno, &lino, _exit);
    }
    code = taosCloseFile(&pWal->pIdxFile);
    if (code != 0) {
      TAOS_CHECK_GOTO(terrno, &lino, _exit);
    }
  }

  if (pWal->pLogFile != NULL) {
    if ((code = taosFsyncFile(pWal->pLogFile)) != 0) {
      TAOS_CHECK_GOTO(terrno, &lino, _exit);
    }
    code = taosCloseFile(&pWal->pLogFile);
    if (code != 0) {
      TAOS_CHECK_GOTO(terrno, &lino, _exit);
    }
  }

  TdFilePtr pIdxFile, pLogFile;
  // create new file
  int64_t newFileFirstVer = pWal->vers.lastVer + 1;
  char    fnameStr[WAL_FILE_LEN];
  walBuildIdxName(pWal, newFileFirstVer, fnameStr);
  pIdxFile = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_READ | TD_FILE_WRITE | TD_FILE_APPEND);
  TSDB_CHECK_NULL(pIdxFile, code, lino, _exit, terrno);

  walBuildLogName(pWal, newFileFirstVer, fnameStr);
  pLogFile = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_READ | TD_FILE_WRITE | TD_FILE_APPEND);
  wDebug("vgId:%d, wal create new file for write:%s", pWal->cfg.vgId, fnameStr);
  TSDB_CHECK_NULL(pLogFile, code, lino, _exit, terrno);

  TAOS_CHECK_GOTO(walRollFileInfo(pWal), &lino, _exit);

  // switch file
  pWal->pIdxFile = pIdxFile;
  pWal->pLogFile = pLogFile;
  pWal->writeCur = taosArrayGetSize(pWal->fileInfoSet) - 1;

  pWal->lastRollSeq = walGetSeq();

  TAOS_CHECK_GOTO(walSaveMeta(pWal), &lino, _exit);

_exit:
  if (code) {
    wError("vgId:%d, %s failed at line %d since %s", pWal->cfg.vgId, __func__, lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}

static FORCE_INLINE int32_t walCheckAndRoll(SWal *pWal) {
  if (taosArrayGetSize(pWal->fileInfoSet) == 0) {
    TAOS_CHECK_RETURN(walRollImpl(pWal));

    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  int64_t passed = walGetSeq() - pWal->lastRollSeq;
  if (pWal->cfg.rollPeriod != -1 && pWal->cfg.rollPeriod != 0 && passed > pWal->cfg.rollPeriod) {
    TAOS_CHECK_RETURN(walRollImpl(pWal));
  } else if (pWal->cfg.segSize != -1 && pWal->cfg.segSize != 0 && walGetLastFileSize(pWal) > pWal->cfg.segSize) {
    TAOS_CHECK_RETURN(walRollImpl(pWal));
  }

  if (walGetLastFileCachedSize(pWal) > tsWalFsyncDataSizeLimit) {
    TAOS_CHECK_RETURN(walSaveMeta(pWal));
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t walBeginSnapshot(SWal *pWal, int64_t ver, int64_t logRetention) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pWal->cfg.level == TAOS_WAL_SKIP) {
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  if (logRetention < 0) {
    TAOS_RETURN(TSDB_CODE_FAILED);
  }

  TAOS_UNUSED(taosThreadRwlockWrlock(&pWal->mutex));
  pWal->vers.verInSnapshotting = ver;
  pWal->vers.logRetention = logRetention;

  wDebug("vgId:%d, wal begin snapshot for index:%" PRId64 ", log retention:%" PRId64 " first index:%" PRId64
         ", last index:%" PRId64,
         pWal->cfg.vgId, ver, pWal->vers.logRetention, pWal->vers.firstVer, pWal->vers.lastVer);
  // check file rolling
  if (walGetLastFileSize(pWal) != 0 && (code = walRollImpl(pWal)) < 0) goto _exit;

_exit:
  if (code) {
    wError("vgId:%d, %s failed since %s at line %d", pWal->cfg.vgId, __func__, tstrerror(code), lino);
  }
  TAOS_UNUSED(taosThreadRwlockUnlock(&pWal->mutex));
  TAOS_RETURN(code);
}

int32_t walEndSnapshot(SWal *pWal) {
  int32_t code = 0, lino = 0;

  if (pWal->cfg.level == TAOS_WAL_SKIP) {
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  TAOS_UNUSED(taosThreadRwlockWrlock(&pWal->mutex));
  int64_t ver = pWal->vers.verInSnapshotting;

  wDebug("vgId:%d, wal end snapshot for index:%" PRId64 ", log retention:%" PRId64 " first index:%" PRId64
         ", last index:%" PRId64,
         pWal->cfg.vgId, ver, pWal->vers.logRetention, pWal->vers.firstVer, pWal->vers.lastVer);

  if (ver == -1) {
    TAOS_CHECK_GOTO(TSDB_CODE_FAILED, &lino, _exit);
  }

  pWal->vers.snapshotVer = ver;
  int ts = taosGetTimestampSec();
  ver = TMAX(ver - pWal->vers.logRetention, pWal->vers.firstVer - 1);

  // compatible mode for refVer
  bool    hasTopic = false;
  int64_t refVer = INT64_MAX;
  void   *pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pWal->pRefHash, pIter);
    if (pIter == NULL) break;
    SWalRef *pRef = *(SWalRef **)pIter;
    if (pRef->refVer == -1) continue;
    refVer = TMIN(refVer, pRef->refVer - 1);
    hasTopic = true;
  }
  if (pWal->cfg.retentionPeriod == 0 && hasTopic) {
    wInfo("vgId:%d, wal found ref index:%" PRId64 " in compatible mode, index:%" PRId64, pWal->cfg.vgId, refVer, ver);
    ver = TMIN(ver, refVer);
  }

  // find files safe to delete
  int          deleteCnt = 0;
  int64_t      newTotSize = pWal->totSize;
  SWalFileInfo tmp = {0};
  tmp.firstVer = ver;
  SWalFileInfo *pInfo = taosArraySearch(pWal->fileInfoSet, &tmp, compareWalFileInfo, TD_LE);

  if (pInfo) {
    wDebug("vgId:%d, wal search found file info, index:%" PRId64 ", first index:%" PRId64 ", last index:%" PRId64,
           pWal->cfg.vgId, ver, pInfo->firstVer, pInfo->lastVer);
    if (ver > pInfo->lastVer) {
      TAOS_CHECK_GOTO(TSDB_CODE_FAILED, &lino, _exit);
    }

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
      if (taosArrayPush(pWal->toDeleteFiles, iter) == NULL) {
        wError("vgId:%d, failed to push file info to delete list", pWal->cfg.vgId);
      }
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

  TAOS_CHECK_GOTO(walSaveMeta(pWal), &lino, _exit);

  // delete files
  deleteCnt = taosArrayGetSize(pWal->toDeleteFiles);
  char fnameStr[WAL_FILE_LEN] = {0};
  pInfo = NULL;

  for (int i = 0; i < deleteCnt; i++) {
    pInfo = taosArrayGet(pWal->toDeleteFiles, i);

    walBuildLogName(pWal, pInfo->firstVer, fnameStr);
    if (taosRemoveFile(fnameStr) < 0 && ERRNO != ENOENT) {
      wError("vgId:%d, failed to remove log file %s since %s", pWal->cfg.vgId, fnameStr, strerror(ERRNO));
      goto _exit;
    }
    walBuildIdxName(pWal, pInfo->firstVer, fnameStr);
    if (taosRemoveFile(fnameStr) < 0 && ERRNO != ENOENT) {
      wError("vgId:%d, failed to remove idx file %s since %s", pWal->cfg.vgId, fnameStr, strerror(ERRNO));
      goto _exit;
    }
  }
  if (pInfo != NULL) {
    wInfo("vgId:%d, wal log files recycled, count:%d, until index:%" PRId64 ", closeTs:%" PRId64, pWal->cfg.vgId,
          deleteCnt, pInfo->lastVer, pInfo->closeTs);
  }
  taosArrayClear(pWal->toDeleteFiles);

_exit:
  taosThreadRwlockUnlock(&pWal->mutex);

  if (code) {
    wError("vgId:%d, %s failed at line %d since %s", pWal->cfg.vgId, __func__, lino, tstrerror(code));
  }

  return code;
}

static void walStopDnode(SWal *pWal) {
  if (pWal->stopDnode != NULL) {
    wWarn("vgId:%d, set stop dnode flag", pWal->cfg.vgId);
    pWal->stopDnode();
  }
}

static int32_t walWriteIndex(SWal *pWal, int64_t ver, int64_t offset, const STraceId *trace) {
  int32_t code = 0;

  SWalIdxEntry  entry = {.ver = ver, .offset = offset};
  SWalFileInfo *pFileInfo = walGetCurFileInfo(pWal);

  int64_t idxOffset = (entry.ver - pFileInfo->firstVer) * sizeof(SWalIdxEntry);
  wGTrace(trace, "vgId:%d, index:%" PRId64 ", write log entry at %" PRId64 ", offset:%" PRId64, pWal->cfg.vgId, ver,
          idxOffset, offset);

  int64_t size = taosWriteFile(pWal->pIdxFile, &entry, sizeof(SWalIdxEntry));
  if (size != sizeof(SWalIdxEntry)) {
    wGError(trace, "vgId:%d, index:%" PRId64 ", failed to write entry since %s", pWal->cfg.vgId, ver, strerror(ERRNO));
    walStopDnode(pWal);
    TAOS_RETURN(terrno);
  }

  // check alignment of idx entries
  int64_t endOffset = taosLSeekFile(pWal->pIdxFile, 0, SEEK_END);
  if (endOffset < 0) {
    wGFatal(trace, "vgId:%d, index:%" PRId64 ", failed to seek end of WAL idxfile since %s, endOffset:%" PRId64,
            pWal->cfg.vgId, ver, tstrerror(terrno), endOffset);
    taosMsleep(100);
    exit(EXIT_FAILURE);
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static FORCE_INLINE int32_t walWriteImpl(SWal *pWal, int64_t index, tmsg_t msgType, SWalSyncInfo syncMeta,
                                         const void *body, int32_t bodyLen, const STraceId *trace) {
  int32_t code = 0, lino = 0;
  int32_t plainBodyLen = bodyLen;

  int64_t       offset = walGetCurFileOffset(pWal);
  SWalFileInfo *pFileInfo = walGetCurFileInfo(pWal);

  pWal->writeHead.head.version = index;
  pWal->writeHead.head.bodyLen = plainBodyLen;
  pWal->writeHead.head.msgType = msgType;
  pWal->writeHead.head.ingestTs = taosGetTimestampUs();

  // sync info for sync module
  pWal->writeHead.head.syncMeta = syncMeta;

  pWal->writeHead.cksumHead = walCalcHeadCksum(&pWal->writeHead);
  pWal->writeHead.cksumBody = walCalcBodyCksum(body, plainBodyLen);
  wGDebug(trace, "vgId:%d, index:%" PRId64 ", write log, type:%s, cksum head:%u, cksum body:%u", pWal->cfg.vgId, index,
          TMSG_INFO(msgType), pWal->writeHead.cksumHead, pWal->writeHead.cksumBody);

  if (pWal->cfg.level != TAOS_WAL_SKIP) {
    TAOS_CHECK_GOTO(walWriteIndex(pWal, index, offset, trace), &lino, _exit);
  }

  if (pWal->cfg.level != TAOS_WAL_SKIP &&
      taosWriteFile(pWal->pLogFile, &pWal->writeHead, sizeof(SWalCkHead)) != sizeof(SWalCkHead)) {
    code = terrno;
    wGError(trace, "vgId:%d, file:%" PRId64 ".log, failed to write since %s", pWal->cfg.vgId,
            walGetLastFileFirstVer(pWal), strerror(ERRNO));
    walStopDnode(pWal);
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

  int32_t cyptedBodyLen = plainBodyLen;
  char   *buf = (char *)body;
  char   *newBody = NULL;
  char   *newBodyEncrypted = NULL;

  if (pWal->cfg.encryptAlgorithm == DND_CA_SM4) {
    cyptedBodyLen = ENCRYPTED_LEN(cyptedBodyLen);

    newBody = taosMemoryMalloc(cyptedBodyLen);
    TSDB_CHECK_NULL(newBody, code, lino, _exit, terrno);

    (void)memset(newBody, 0, cyptedBodyLen);
    (void)memcpy(newBody, body, plainBodyLen);

    newBodyEncrypted = taosMemoryMalloc(cyptedBodyLen);
    if (newBodyEncrypted == NULL) {
      wGError(trace, "vgId:%d, file:%" PRId64 ".log, failed to malloc since %s", pWal->cfg.vgId,
              walGetLastFileFirstVer(pWal), strerror(ERRNO));

      if (newBody != NULL) taosMemoryFreeClear(newBody);

      TAOS_CHECK_GOTO(terrno, &lino, _exit);
    }

    SCryptOpts opts;
    opts.len = cyptedBodyLen;
    opts.source = newBody;
    opts.result = newBodyEncrypted;
    opts.unitLen = 16;
    tstrncpy((char *)opts.key, pWal->cfg.encryptKey, ENCRYPT_KEY_LEN + 1);

    int32_t count = CBC_Encrypt(&opts);

    // wDebug("vgId:%d, file:%" PRId64 ".log, index:%" PRId64 ", CBC_Encrypt cryptedBodyLen:%d, plainBodyLen:%d, %s",
    //       pWal->cfg.vgId, walGetLastFileFirstVer(pWal), index, count, plainBodyLen, __FUNCTION__);

    buf = newBodyEncrypted;
  }

  if (pWal->cfg.level != TAOS_WAL_SKIP && taosWriteFile(pWal->pLogFile, (char *)buf, cyptedBodyLen) != cyptedBodyLen) {
    code = terrno;
    wGError(trace, "vgId:%d, file:%" PRId64 ".log, failed to write since %s", pWal->cfg.vgId,
            walGetLastFileFirstVer(pWal), strerror(ERRNO));

    if (pWal->cfg.encryptAlgorithm == DND_CA_SM4) {
      taosMemoryFreeClear(newBody);
      taosMemoryFreeClear(newBodyEncrypted);
    }

    walStopDnode(pWal);

    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

  if (pWal->cfg.encryptAlgorithm == DND_CA_SM4) {
    taosMemoryFreeClear(newBody);
    taosMemoryFreeClear(newBodyEncrypted);
  }

  // set status
  if (pWal->vers.firstVer == -1) {
    pWal->vers.firstVer = index;
  }
  pWal->vers.lastVer = index;
  pWal->totSize += sizeof(SWalCkHead) + cyptedBodyLen;
  pFileInfo->lastVer = index;
  pFileInfo->fileSize += sizeof(SWalCkHead) + cyptedBodyLen;

  return 0;

_exit:
  if (code) {
    wGError(trace, "vgId:%d, %s failed at line %d since %s", pWal->cfg.vgId, __func__, lino, tstrerror(code));
  }

  // recover in a reverse order
  if (taosFtruncateFile(pWal->pLogFile, offset) < 0) {
    wGFatal(trace, "vgId:%d, failed to recover WAL logfile from write error since %s, offset:%" PRId64, pWal->cfg.vgId,
            terrstr(), offset);
    taosMsleep(100);
    exit(EXIT_FAILURE);
  }

  int64_t idxOffset = (index - pFileInfo->firstVer) * sizeof(SWalIdxEntry);
  if (taosFtruncateFile(pWal->pIdxFile, idxOffset) < 0) {
    wGFatal(trace, "vgId:%d, failed to recover WAL idxfile from write error since %s, offset:%" PRId64, pWal->cfg.vgId,
            terrstr(), idxOffset);
    taosMsleep(100);
    exit(EXIT_FAILURE);
  }

  TAOS_RETURN(TSDB_CODE_FAILED);
}

static int32_t walInitWriteFile(SWal *pWal) {
  TdFilePtr     pIdxTFile, pLogTFile;
  int64_t       fileFirstVer = -1;
  int32_t       code = 0;
  SWalFileInfo *pRet = taosArrayGetLast(pWal->fileInfoSet);
  if (pRet == NULL) {
    fileFirstVer = pWal->vers.lastVer + 1;
  }
  fileFirstVer = pRet->firstVer;

  char fnameStr[WAL_FILE_LEN];
  walBuildIdxName(pWal, fileFirstVer, fnameStr);
  pIdxTFile = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_READ | TD_FILE_WRITE | TD_FILE_APPEND);
  if (pIdxTFile == NULL) {
    TAOS_RETURN(terrno);
  }
  walBuildLogName(pWal, fileFirstVer, fnameStr);
  pLogTFile = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_READ |TD_FILE_WRITE | TD_FILE_APPEND);
  if (pLogTFile == NULL) {
    TAOS_UNUSED(taosCloseFile(&pIdxTFile));
    TAOS_RETURN(terrno);
  }
  // switch file
  pWal->pIdxFile = pIdxTFile;
  pWal->pLogFile = pLogTFile;
  if (taosArrayGetSize(pWal->fileInfoSet) == 0) {
    code = walRollFileInfo(pWal);
    if (code < 0) {
      wError("vgId:%d, failed to roll file info while init write file since %s", pWal->cfg.vgId, terrstr());
      TAOS_RETURN(code);
    }
  }
  pWal->writeCur = taosArrayGetSize(pWal->fileInfoSet) - 1;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t walAppendLog(SWal *pWal, int64_t index, tmsg_t msgType, SWalSyncInfo syncMeta, const void *body,
                     int32_t bodyLen, const STraceId *trace) {
  int32_t code = 0, lino = 0;

  TAOS_UNUSED(taosThreadRwlockWrlock(&pWal->mutex));

  if (index != pWal->vers.lastVer + 1) {
    TAOS_CHECK_GOTO(TSDB_CODE_WAL_INVALID_VER, &lino, _exit);
  }

  TAOS_CHECK_GOTO(walCheckAndRoll(pWal), &lino, _exit);

  if (pWal->pLogFile == NULL || pWal->pIdxFile == NULL || pWal->writeCur < 0) {
    TAOS_CHECK_GOTO(walInitWriteFile(pWal), &lino, _exit);
  }

  TAOS_CHECK_GOTO(walWriteImpl(pWal, index, msgType, syncMeta, body, bodyLen, trace), &lino, _exit);

_exit:
  if (code) {
    wError("vgId:%d, %s failed at line %d since %s", pWal->cfg.vgId, __func__, lino, tstrerror(code));
  }

  TAOS_UNUSED(taosThreadRwlockUnlock(&pWal->mutex));
  return code;
}

int32_t walFsync(SWal *pWal, bool forceFsync) {
  int32_t code = 0;

  if (pWal->cfg.level == TAOS_WAL_SKIP) {
    return code;
  }

  TAOS_UNUSED(taosThreadRwlockWrlock(&pWal->mutex));
  if (forceFsync || (pWal->cfg.level == TAOS_WAL_FSYNC && pWal->cfg.fsyncPeriod == 0)) {
    wTrace("vgId:%d, fileId:%" PRId64 ".log, do fsync", pWal->cfg.vgId, walGetCurFileFirstVer(pWal));
    if (taosFsyncFile(pWal->pLogFile) < 0) {
      wError("vgId:%d, file:%" PRId64 ".log, fsync failed since %s", pWal->cfg.vgId, walGetCurFileFirstVer(pWal),
             strerror(ERRNO));
      code = terrno;
    }
  }
  TAOS_UNUSED(taosThreadRwlockUnlock(&pWal->mutex));

  return code;
}
