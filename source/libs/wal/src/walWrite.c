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
#include "tfile.h"
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
  if (ver == pWal->vers.lastVer) {
    return 0;
  }
  if (ver > pWal->vers.lastVer || ver < pWal->vers.commitVer) {
    terrno = TSDB_CODE_WAL_INVALID_VER;
    return -1;
  }
  pthread_mutex_lock(&pWal->mutex);

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
      remove(fnameStr);
      walBuildIdxName(pWal, ((SWalFileInfo *)taosArrayGet(pWal->fileInfoSet, i))->firstVer, fnameStr);
      remove(fnameStr);
    }
    // pop from fileInfoSet
    taosArraySetSize(pWal->fileInfoSet, pWal->writeCur + 1);
  }

  walBuildIdxName(pWal, walGetCurFileFirstVer(pWal), fnameStr);
  int64_t idxTfd = tfOpenReadWrite(fnameStr);

  // TODO:change to deserialize function
  if (idxTfd < 0) {
    pthread_mutex_unlock(&pWal->mutex);
    return -1;
  }
  int64_t idxOff = walGetVerIdxOffset(pWal, ver);
  code = tfLseek(idxTfd, idxOff, SEEK_SET);
  if (code < 0) {
    pthread_mutex_unlock(&pWal->mutex);
    return -1;
  }
  // read idx file and get log file pos
  // TODO:change to deserialize function
  SWalIdxEntry entry;
  if (tfRead(idxTfd, &entry, sizeof(SWalIdxEntry)) != sizeof(SWalIdxEntry)) {
    pthread_mutex_unlock(&pWal->mutex);
    return -1;
  }
  ASSERT(entry.ver == ver);

  walBuildLogName(pWal, walGetCurFileFirstVer(pWal), fnameStr);
  int64_t logTfd = tfOpenReadWrite(fnameStr);
  if (logTfd < 0) {
    // TODO
    pthread_mutex_unlock(&pWal->mutex);
    return -1;
  }
  code = tfLseek(logTfd, entry.offset, SEEK_SET);
  if (code < 0) {
    // TODO
    pthread_mutex_unlock(&pWal->mutex);
    return -1;
  }
  // validate offset
  SWalHead head;
  ASSERT(tfValid(logTfd));
  int size = tfRead(logTfd, &head, sizeof(SWalHead));
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
  code = tfFtruncate(logTfd, entry.offset);
  if (code < 0) {
    return -1;
  }
  code = tfFtruncate(idxTfd, idxOff);
  if (code < 0) {
    return -1;
  }
  pWal->vers.lastVer = ver - 1;
  ((SWalFileInfo *)taosArrayGetLast(pWal->fileInfoSet))->lastVer = ver - 1;
  ((SWalFileInfo *)taosArrayGetLast(pWal->fileInfoSet))->fileSize = entry.offset;

  // unlock
  pthread_mutex_unlock(&pWal->mutex);
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
    if ((pWal->cfg.retentionSize != -1 && pWal->totSize > pWal->cfg.retentionSize)
        || (pWal->cfg.retentionPeriod != -1 && iter->closeTs + pWal->cfg.retentionPeriod > ts)) {
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
    remove(fnameStr);
    walBuildIdxName(pWal, pInfo->firstVer, fnameStr);
    remove(fnameStr);
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
  if (pWal->writeIdxTfd != -1) {
    code = tfClose(pWal->writeIdxTfd);
    if (code != 0) {
      return -1;
    }
  }
  if (pWal->writeLogTfd != -1) {
    code = tfClose(pWal->writeLogTfd);
    if (code != 0) {
      return -1;
    }
  }
  int64_t idxTfd, logTfd;
  // create new file
  int64_t newFileFirstVersion = pWal->vers.lastVer + 1;
  char    fnameStr[WAL_FILE_LEN];
  walBuildIdxName(pWal, newFileFirstVersion, fnameStr);
  idxTfd = tfOpenCreateWriteAppend(fnameStr);
  if (idxTfd < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  walBuildLogName(pWal, newFileFirstVersion, fnameStr);
  logTfd = tfOpenCreateWriteAppend(fnameStr);
  if (logTfd < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  code = walRollFileInfo(pWal);
  if (code != 0) {
    return -1;
  }

  // switch file
  pWal->writeIdxTfd = idxTfd;
  pWal->writeLogTfd = logTfd;
  pWal->writeCur = taosArrayGetSize(pWal->fileInfoSet) - 1;
  ASSERT(pWal->writeCur >= 0);

  pWal->lastRollSeq = walGetSeq();
  return 0;
}

static int walWriteIndex(SWal *pWal, int64_t ver, int64_t offset) {
  SWalIdxEntry entry = {.ver = ver, .offset = offset};
  int          size = tfWrite(pWal->writeIdxTfd, &entry, sizeof(SWalIdxEntry));
  if (size != sizeof(SWalIdxEntry)) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    // TODO truncate
    return -1;
  }
  return 0;
}

int64_t walWrite(SWal *pWal, int64_t index, tmsg_t msgType, const void *body, int32_t bodyLen) {
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
    return -1;
  }
  /*if (!tfValid(pWal->writeLogTfd)) return -1;*/

  ASSERT(pWal->writeCur >= 0);

  pthread_mutex_lock(&pWal->mutex);

  if (pWal->writeIdxTfd == -1 || pWal->writeLogTfd == -1) {
    walSetWrite(pWal);
    tfLseek(pWal->writeLogTfd, 0, SEEK_END);
    tfLseek(pWal->writeIdxTfd, 0, SEEK_END);
  }

  pWal->writeHead.head.version = index;

  int64_t offset = walGetCurFileOffset(pWal);
  pWal->writeHead.head.len = bodyLen;
  pWal->writeHead.head.msgType = msgType;
  pWal->writeHead.cksumHead = walCalcHeadCksum(&pWal->writeHead);
  pWal->writeHead.cksumBody = walCalcBodyCksum(body, bodyLen);

  if (tfWrite(pWal->writeLogTfd, &pWal->writeHead, sizeof(SWalHead)) != sizeof(SWalHead)) {
    // ftruncate
    code = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%" PRId64 ".log, failed to write since %s", pWal->cfg.vgId, walGetLastFileFirstVer(pWal),
           strerror(errno));
  }

  if (tfWrite(pWal->writeLogTfd, (char *)body, bodyLen) != bodyLen) {
    // ftruncate
    code = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%" PRId64 ".log, failed to write since %s", pWal->cfg.vgId, walGetLastFileFirstVer(pWal),
           strerror(errno));
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

  pthread_mutex_unlock(&pWal->mutex);

  return code;
}

void walFsync(SWal *pWal, bool forceFsync) {
  if (forceFsync || (pWal->cfg.level == TAOS_WAL_FSYNC && pWal->cfg.fsyncPeriod == 0)) {
    wTrace("vgId:%d, fileId:%" PRId64 ".log, do fsync", pWal->cfg.vgId, walGetCurFileFirstVer(pWal));
    if (tfFsync(pWal->writeLogTfd) < 0) {
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
