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

static void walFtruncate(SWal *pWal, int64_t ver);

int32_t walCommit(SWal *pWal, int64_t ver) {
  ASSERT(pWal->snapshotVersion <= pWal->commitVersion);
  ASSERT(pWal->commitVersion <= pWal->lastVersion);
  ASSERT(ver >= pWal->commitVersion);
  ASSERT(ver <= pWal->lastVersion);
  pWal->commitVersion = ver;
  return 0;
}

int32_t walRollback(SWal *pWal, int64_t ver) {
  //TODO: ftruncate
  ASSERT(ver > pWal->commitVersion);
  ASSERT(ver <= pWal->lastVersion);
  //seek position
  walSeekVer(pWal, ver); 
  walFtruncate(pWal, ver);
  return 0;
}

int32_t walTakeSnapshot(SWal *pWal, int64_t ver) {
  pWal->snapshotVersion = ver;

  WalFileInfo tmp;
  tmp.firstVer = ver;
  //mark files safe to delete
  WalFileInfo* pInfo = taosArraySearch(pWal->fileInfoSet, &tmp, compareWalFileInfo, TD_LE);
  //iterate files, until the searched result
  //if totSize > rtSize, delete
  //if createTs > retentionTs, delete

  //save snapshot ver, commit ver

  //make new array, remove files

  return 0;
}

#if 0
static int32_t walRestoreWalFile(SWal *pWal, void *pVnode, FWalWrite writeFp, char *name, int64_t fileId);

int32_t walRenew(void *handle) {
  if (handle == NULL) return 0;

  SWal *  pWal = handle;
  int32_t code = 0;

  /*if (pWal->stop) {*/
    /*wDebug("vgId:%d, do not create a new wal file", pWal->vgId);*/
    /*return 0;*/
  /*}*/

  pthread_mutex_lock(&pWal->mutex);

  if (tfValid(pWal->logTfd)) {
    tfClose(pWal->logTfd);
    wDebug("vgId:%d, file:%s, it is closed while renew", pWal->vgId, pWal->logName);
  }

  /*if (pWal->keep == TAOS_WAL_KEEP) {*/
    /*pWal->fileId = 0;*/
  /*} else {*/
    /*if (walGetNewFile(pWal, &pWal->fileId) != 0) pWal->fileId = 0;*/
    /*pWal->fileId++;*/
  /*}*/

  snprintf(pWal->logName, sizeof(pWal->logName), "%s/%s%" PRId64, pWal->path, WAL_PREFIX, pWal->curFileId);
  pWal->logTfd = tfOpenCreateWrite(pWal->logName);

  if (!tfValid(pWal->logTfd)) {
    code = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%s, failed to open since %s", pWal->vgId, pWal->logName, strerror(errno));
  } else {
    wDebug("vgId:%d, file:%s, it is created and open while renew", pWal->vgId, pWal->logName);
  }

  pthread_mutex_unlock(&pWal->mutex);

  return code;
}

void walRemoveOneOldFile(void *handle) {
  SWal *pWal = handle;
  if (pWal == NULL) return;
  /*if (pWal->keep == TAOS_WAL_KEEP) return;*/
  if (!tfValid(pWal->logTfd)) return;

  pthread_mutex_lock(&pWal->mutex);

  // remove the oldest wal file
  int64_t oldFileId = -1;
  if (walGetOldFile(pWal, pWal->curFileId, WAL_FILE_NUM, &oldFileId) == 0) {
    char walName[WAL_FILE_LEN] = {0};
    snprintf(walName, sizeof(walName), "%s/%s%" PRId64, pWal->path, WAL_PREFIX, oldFileId);

    if (remove(walName) < 0) {
      wError("vgId:%d, file:%s, failed to remove since %s", pWal->vgId, walName, strerror(errno));
    } else {
      wInfo("vgId:%d, file:%s, it is removed", pWal->vgId, walName);
    }
  }

  pthread_mutex_unlock(&pWal->mutex);
}

void walRemoveAllOldFiles(void *handle) {
  if (handle == NULL) return;

  SWal *  pWal = handle;
  int64_t fileId = -1;

  pthread_mutex_lock(&pWal->mutex);
  
  tfClose(pWal->logTfd);
  wDebug("vgId:%d, file:%s, it is closed before remove all wals", pWal->vgId, pWal->logName);

  while (walGetNextFile(pWal, &fileId) >= 0) {
    snprintf(pWal->logName, sizeof(pWal->logName), "%s/%s%" PRId64, pWal->path, WAL_PREFIX, fileId);

    if (remove(pWal->logName) < 0) {
      wError("vgId:%d, wal:%p file:%s, failed to remove since %s", pWal->vgId, pWal, pWal->logName, strerror(errno));
    } else {
      wInfo("vgId:%d, wal:%p file:%s, it is removed", pWal->vgId, pWal, pWal->logName);
    }
  }
  pthread_mutex_unlock(&pWal->mutex);
}
#endif

int walRoll(SWal *pWal) {
  int code = 0;
  if(pWal->curIdxTfd != -1) {
    code = tfClose(pWal->curIdxTfd);
    if(code != 0) {
      return -1;
    }
  }
  if(pWal->curLogTfd != -1) {
    code = tfClose(pWal->curLogTfd);
    if(code != 0) {
      return -1;
    }
  }
  int64_t idxTfd, logTfd;
  //create new file
  int64_t newFileFirstVersion = pWal->lastVersion + 1;
  char fnameStr[WAL_FILE_LEN];
  walBuildIdxName(pWal, newFileFirstVersion, fnameStr);
  idxTfd = tfOpenCreateWrite(fnameStr);
  if(idxTfd < 0) {
    ASSERT(0);
    return -1;
  }
  walBuildLogName(pWal, newFileFirstVersion, fnameStr);
  logTfd = tfOpenCreateWrite(fnameStr);
  if(logTfd < 0) {
    ASSERT(0);
    return -1;
  }
  code = walRollFileInfo(pWal);
  if(code != 0) {
    ASSERT(0);
    return -1;
  }

  //switch file
  pWal->curIdxTfd = idxTfd;
  pWal->curLogTfd = logTfd;
  //change status
  pWal->curStatus = WAL_CUR_FILE_WRITABLE & WAL_CUR_POS_WRITABLE;

  pWal->lastRollSeq = walGetSeq();
  return 0;
}

int walChangeFileToLast(SWal *pWal) {
  int64_t idxTfd, logTfd;
  WalFileInfo* pRet = taosArrayGetLast(pWal->fileInfoSet);
  ASSERT(pRet != NULL);
  int64_t fileFirstVer = pRet->firstVer;

  char fnameStr[WAL_FILE_LEN];
  walBuildIdxName(pWal, fileFirstVer, fnameStr);
  idxTfd = tfOpenReadWrite(fnameStr);
  if(idxTfd < 0) {
    return -1;
  }
  walBuildLogName(pWal, fileFirstVer, fnameStr);
  logTfd = tfOpenReadWrite(fnameStr);
  if(logTfd < 0) {
    return -1;
  }
  //switch file
  pWal->curIdxTfd = idxTfd;
  pWal->curLogTfd = logTfd;
  //change status
  pWal->curVersion = fileFirstVer;
  pWal->curStatus = WAL_CUR_FILE_WRITABLE;
  return 0;
}

static int walWriteIndex(SWal *pWal, int64_t ver, int64_t offset) {
  int code = 0;
  //get index file
  if(!tfValid(pWal->curIdxTfd)) {
    code = TAOS_SYSTEM_ERROR(errno);

    WalFileInfo* pInfo = taosArrayGet(pWal->fileInfoSet, pWal->fileCursor);
    wError("vgId:%d, file:%"PRId64".idx, failed to open since %s", pWal->vgId, pInfo->firstVer, strerror(errno));
    return code;
  }
  int64_t writeBuf[2] = { ver, offset };
  int size = tfWrite(pWal->curIdxTfd, writeBuf, sizeof(writeBuf));
  if(size != sizeof(writeBuf)) {
    return -1;
  }
  return 0;
}

int64_t walWrite(SWal *pWal, int64_t index, uint8_t msgType, const void *body, int32_t bodyLen) {
  if (pWal == NULL) return -1;
  int code = 0;

  // no wal
  if (pWal->level == TAOS_WAL_NOLOG) return 0;

  if (index == pWal->lastVersion + 1) {
    if(taosArrayGetSize(pWal->fileInfoSet) == 0) {
      code = walRoll(pWal);
      ASSERT(code == 0);
    } else {
      int64_t passed = walGetSeq() - pWal->lastRollSeq;
      if(pWal->rollPeriod != -1 && passed > pWal->rollPeriod) {
        walRoll(pWal);
      } else if(pWal->segSize != -1 && walGetLastFileSize(pWal) > pWal->segSize) {
        walRoll(pWal);
      }
    }
  } else {
    //reject skip log or rewrite log
    //must truncate explicitly first
    return -1;
  }
  /*if (!tfValid(pWal->curLogTfd)) return 0;*/

  pWal->head.version = index;

  pWal->head.signature = WAL_SIGNATURE;
  pWal->head.len = bodyLen;
  pWal->head.msgType = msgType;

  pWal->head.cksumHead = taosCalcChecksum(0, (const uint8_t*)&pWal->head, sizeof(SWalHead)- sizeof(uint32_t)*2);
  pWal->head.cksumBody = taosCalcChecksum(0, (const uint8_t*)&body, bodyLen);

  pthread_mutex_lock(&pWal->mutex);

  if (tfWrite(pWal->curLogTfd, &pWal->head, sizeof(SWalHead)) != sizeof(SWalHead)) {
    //ftruncate
    code = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%"PRId64".log, failed to write since %s", pWal->vgId, walGetLastFileFirstVer(pWal), strerror(errno));
  }

  if (tfWrite(pWal->curLogTfd, &body, bodyLen) != bodyLen) {
    //ftruncate
    code = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%"PRId64".log, failed to write since %s", pWal->vgId, walGetLastFileFirstVer(pWal), strerror(errno));
  }
  code = walWriteIndex(pWal, index, walGetCurFileOffset(pWal));
  if(code != 0) {
    //TODO
  }

  //set status
  pWal->lastVersion = index;
  walGetCurFileInfo(pWal)->lastVer = index;
  walGetCurFileInfo(pWal)->fileSize += sizeof(SWalHead) + bodyLen;
  
  pthread_mutex_unlock(&pWal->mutex);

  return code;
}

void walFsync(SWal *pWal, bool forceFsync) {
  if (pWal == NULL || !tfValid(pWal->curLogTfd)) return;

  if (forceFsync || (pWal->level == TAOS_WAL_FSYNC && pWal->fsyncPeriod == 0)) {
    wTrace("vgId:%d, fileId:%"PRId64".log, do fsync", pWal->vgId, walGetCurFileFirstVer(pWal));
    if (tfFsync(pWal->curLogTfd) < 0) {
      wError("vgId:%d, file:%"PRId64".log, fsync failed since %s", pWal->vgId, walGetCurFileFirstVer(pWal), strerror(errno));
    }
  }
}

#if 0
int32_t walRestore(void *handle, void *pVnode, FWalWrite writeFp) {
  if (handle == NULL) return -1;

  SWal *  pWal = handle;
  int32_t count = 0;
  int32_t code = 0;
  int64_t fileId = -1;

  while ((code = walGetNextFile(pWal, &fileId)) >= 0) {
    /*if (fileId == pWal->curFileId) continue;*/

    char walName[WAL_FILE_LEN];
    snprintf(walName, sizeof(pWal->logName), "%s/%s%" PRId64, pWal->path, WAL_PREFIX, fileId);

    wInfo("vgId:%d, file:%s, will be restored", pWal->vgId, walName);
    code = walRestoreWalFile(pWal, pVnode, writeFp, walName, fileId);
    if (code != TSDB_CODE_SUCCESS) {
      wError("vgId:%d, file:%s, failed to restore since %s", pWal->vgId, walName, tstrerror(code));
      continue;
    }

    wInfo("vgId:%d, file:%s, restore success, wver:%" PRIu64, pWal->vgId, walName, pWal->curVersion);

    count++;
  }

  /*if (pWal->keep != TAOS_WAL_KEEP) return TSDB_CODE_SUCCESS;*/

  if (count == 0) {
    wDebug("vgId:%d, wal file not exist, renew it", pWal->vgId);
    return walRenew(pWal);
  } else {
    // open the existing WAL file in append mode
    /*pWal->curFileId = 0;*/
    snprintf(pWal->logName, sizeof(pWal->logName), "%s/%s%" PRId64, pWal->path, WAL_PREFIX, pWal->curFileId);
    pWal->logTfd = tfOpenCreateWriteAppend(pWal->logName);
    if (!tfValid(pWal->logTfd)) {
      wError("vgId:%d, file:%s, failed to open since %s", pWal->vgId, pWal->logName, strerror(errno));
      return TAOS_SYSTEM_ERROR(errno);
    }
    wDebug("vgId:%d, file:%s, it is created and open while restore", pWal->vgId, pWal->logName);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t walGetWalFile(void *handle, char *fileName, int64_t *fileId) {
  if (handle == NULL) return -1;
  SWal *pWal = handle;

  if (*fileId == 0) *fileId = -1;

  pthread_mutex_lock(&(pWal->mutex));

  int32_t code = walGetNextFile(pWal, fileId);
  if (code >= 0) {
    sprintf(fileName, "wal/%s%" PRId64, WAL_PREFIX, *fileId);
    /*code = (*fileId == pWal->curFileId) ? 0 : 1;*/
  }

  wDebug("vgId:%d, get wal file, code:%d curId:%" PRId64 " outId:%" PRId64, pWal->vgId, code, pWal->curFileId, *fileId);
  pthread_mutex_unlock(&(pWal->mutex));

  return code;
}
#endif

static int walValidateOffset(SWal* pWal, int64_t ver) {
  int code = 0;
  SWalHead *pHead = NULL;
  code = (int)walRead(pWal, &pHead, ver);
  if(pHead->version != ver) {
    return -1;
  }
  return 0;
}

static int64_t walGetOffset(SWal* pWal, int64_t ver) {
  int code = walSeekVer(pWal, ver);
  if(code != 0) {
    return -1;
  }

  code = walValidateOffset(pWal, ver);
  if(code != 0) {
    return -1;
  }

  return 0;
}

static void walFtruncate(SWal *pWal, int64_t ver) {
  int64_t tfd = pWal->curLogTfd;
  tfFtruncate(tfd, ver);
  tfFsync(tfd);
  tfd = pWal->curIdxTfd;
  tfFtruncate(tfd, ver * WAL_IDX_ENTRY_SIZE);
  tfFsync(tfd);
}

#if 0
static int32_t walSkipCorruptedRecord(SWal *pWal, SWalHead *pHead, int64_t tfd, int64_t *offset) {
  int64_t pos = *offset;
  while (1) {
    pos++;

    if (tfLseek(tfd, pos, SEEK_SET) < 0) {
      wError("vgId:%d, failed to seek from corrupted wal file since %s", pWal->vgId, strerror(errno));
      return TSDB_CODE_WAL_FILE_CORRUPTED;
    }

    if (tfRead(tfd, pHead, sizeof(SWalHead)) <= 0) {
      wError("vgId:%d, read to end of corrupted wal file, offset:%" PRId64, pWal->vgId, pos);
      return TSDB_CODE_WAL_FILE_CORRUPTED;
    }

    if (pHead->signature != WAL_SIGNATURE) {
      continue;
    }

    if (pHead->sver >= 1) {
      if (tfRead(tfd, pHead->cont, pHead->len) < pHead->len) {
	wError("vgId:%d, read to end of corrupted wal file, offset:%" PRId64, pWal->vgId, pos);
	return TSDB_CODE_WAL_FILE_CORRUPTED;
      }

      if (walValidateChecksum(pHead)) {
	wInfo("vgId:%d, wal whole cksum check passed, offset:%" PRId64, pWal->vgId, pos);
	*offset = pos;
	return TSDB_CODE_SUCCESS;
      }
    }
  }

  return TSDB_CODE_WAL_FILE_CORRUPTED;
}

static int32_t walRestoreWalFile(SWal *pWal, void *pVnode, FWalWrite writeFp, char *name, int64_t fileId) {
  int32_t size = WAL_MAX_SIZE;
  void *  buffer = malloc(size);
  if (buffer == NULL) {
    wError("vgId:%d, file:%s, failed to open for restore since %s", pWal->vgId, name, strerror(errno));
    return TAOS_SYSTEM_ERROR(errno);
  }

  int64_t tfd = tfOpenReadWrite(name);
  if (!tfValid(tfd)) {
    wError("vgId:%d, file:%s, failed to open for restore since %s", pWal->vgId, name, strerror(errno));
    tfree(buffer);
    return TAOS_SYSTEM_ERROR(errno);
  } else {
    wDebug("vgId:%d, file:%s, open for restore", pWal->vgId, name);
  }

  int32_t   code = TSDB_CODE_SUCCESS;
  int64_t   offset = 0;
  SWalHead *pHead = buffer;

  while (1) {
    int32_t ret = (int32_t)tfRead(tfd, pHead, sizeof(SWalHead));
    if (ret == 0) break;

    if (ret < 0) {
      wError("vgId:%d, file:%s, failed to read wal head since %s", pWal->vgId, name, strerror(errno));
      code = TAOS_SYSTEM_ERROR(errno);
      break;
    }

    if (ret < sizeof(SWalHead)) {
      wError("vgId:%d, file:%s, failed to read wal head, ret is %d", pWal->vgId, name, ret);
      walFtruncate(pWal, tfd, offset);
      break;
    }

    if ((pHead->sver == 0 && !walValidateChecksum(pHead)) || pHead->sver < 0 || pHead->sver > 2) {
      wError("vgId:%d, file:%s, wal head cksum is messed up, hver:%" PRIu64 " len:%d offset:%" PRId64, pWal->vgId, name,
             pHead->version, pHead->len, offset);
      code = walSkipCorruptedRecord(pWal, pHead, tfd, &offset);
      if (code != TSDB_CODE_SUCCESS) {
        walFtruncate(pWal, tfd, offset);
        break;
      }
    }

    if (pHead->len < 0 || pHead->len > size - sizeof(SWalHead)) {
      wError("vgId:%d, file:%s, wal head len out of range, hver:%" PRIu64 " len:%d offset:%" PRId64, pWal->vgId, name,
             pHead->version, pHead->len, offset);
      code = walSkipCorruptedRecord(pWal, pHead, tfd, &offset);
      if (code != TSDB_CODE_SUCCESS) {
        walFtruncate(pWal, tfd, offset);
        break;
      }
    }

    ret = (int32_t)tfRead(tfd, pHead->cont, pHead->len);
    if (ret < 0) {
      wError("vgId:%d, file:%s, failed to read wal body since %s", pWal->vgId, name, strerror(errno));
      code = TAOS_SYSTEM_ERROR(errno);
      break;
    }

    if (ret < pHead->len) {
      wError("vgId:%d, file:%s, failed to read wal body, ret:%d len:%d", pWal->vgId, name, ret, pHead->len);
      offset += sizeof(SWalHead);
      continue;
    }

    if ((pHead->sver >= 1) && !walValidateChecksum(pHead)) {
      wError("vgId:%d, file:%s, wal whole cksum is messed up, hver:%" PRIu64 " len:%d offset:%" PRId64, pWal->vgId, name,
             pHead->version, pHead->len, offset);
      code = walSkipCorruptedRecord(pWal, pHead, tfd, &offset);
      if (code != TSDB_CODE_SUCCESS) {
        walFtruncate(pWal, tfd, offset);
        break;
      }
    }

    offset = offset + sizeof(SWalHead) + pHead->len;

    wTrace("vgId:%d, restore wal, fileId:%" PRId64 " hver:%" PRIu64 " wver:%" PRIu64 " len:%d offset:%" PRId64,
           pWal->vgId, fileId, pHead->version, pWal->curVersion, pHead->len, offset);

    pWal->curVersion = pHead->version;

    // wInfo("writeFp: %ld", offset);
    (*writeFp)(pVnode, pHead);
  }

  tfClose(tfd);
  tfree(buffer);

  wDebug("vgId:%d, file:%s, it is closed after restore", pWal->vgId, name);
  return code;
}
#endif
