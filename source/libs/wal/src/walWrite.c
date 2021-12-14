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

int32_t walCommit(SWal *pWal, int64_t ver) {
  ASSERT(pWal->vers.commitVer >= pWal->vers.snapshotVer);
  ASSERT(pWal->vers.commitVer <= pWal->vers.lastVer);
  if(ver < pWal->vers.commitVer || ver > pWal->vers.lastVer) {
    return -1;
  }
  pWal->vers.commitVer = ver;
  return 0;
}

int32_t walRollback(SWal *pWal, int64_t ver) {
  int code;
  char fnameStr[WAL_FILE_LEN];
  if(ver == pWal->vers.lastVer) {
    return 0;
  }
  if(ver > pWal->vers.lastVer || ver < pWal->vers.commitVer) {
    return -1;
  }
  pthread_mutex_lock(&pWal->mutex);

  //find correct file
  if(ver < walGetLastFileFirstVer(pWal)) {
    //close current files
    tfClose(pWal->writeIdxTfd);
    tfClose(pWal->writeLogTfd);
    //open old files
    code = walChangeFile(pWal, ver);
    if(code != 0) {
      return -1;
    }

    //delete files
    int fileSetSize = taosArrayGetSize(pWal->fileInfoSet);
    for(int i = pWal->writeCur; i < fileSetSize; i++) {
      walBuildLogName(pWal, ((WalFileInfo*)taosArrayGet(pWal->fileInfoSet, i))->firstVer, fnameStr);
      remove(fnameStr);
      walBuildIdxName(pWal, ((WalFileInfo*)taosArrayGet(pWal->fileInfoSet, i))->firstVer, fnameStr);
      remove(fnameStr);
    }
    //pop from fileInfoSet
    taosArraySetSize(pWal->fileInfoSet, pWal->writeCur + 1);
  }

  walBuildIdxName(pWal, walGetCurFileFirstVer(pWal), fnameStr);
  int64_t idxTfd = tfOpenReadWrite(fnameStr);

  //change to deserialize function

  if(idxTfd < 0) {
    pthread_mutex_unlock(&pWal->mutex);
    return -1;
  }
  int idxOff = (ver - walGetCurFileFirstVer(pWal)) * WAL_IDX_ENTRY_SIZE;
  code = tfLseek(idxTfd, idxOff, SEEK_SET);
  if(code < 0) {
    pthread_mutex_unlock(&pWal->mutex);
    return -1;
  }
  //read idx file and get log file pos
  //TODO:change to deserialize function
  WalIdxEntry entry;
  if(tfRead(idxTfd, &entry, sizeof(WalIdxEntry)) != sizeof(WalIdxEntry)) {
    pthread_mutex_unlock(&pWal->mutex);
    return -1;
  }
  ASSERT(entry.ver == ver);

  walBuildLogName(pWal, walGetCurFileFirstVer(pWal), fnameStr);
  int64_t logTfd = tfOpenReadWrite(fnameStr);
  if(logTfd < 0) {
    //TODO
    pthread_mutex_unlock(&pWal->mutex);
    return -1;
  }
  code = tfLseek(logTfd, entry.offset, SEEK_SET);
  if(code < 0) {
    //TODO
    pthread_mutex_unlock(&pWal->mutex);
    return -1;
  }
  //validate offset
  SWalHead head;
  ASSERT(tfValid(logTfd));
  int size = tfRead(logTfd, &head, sizeof(SWalHead));
  if(size != sizeof(SWalHead)) {
    return -1;
  }
  code = walValidHeadCksum(&head);

  ASSERT(code == 0);
  if(code != 0) {
    return -1;
  }
  if(head.head.version != ver) {
    //TODO
    return -1;
  }
  //truncate old files
  code = tfFtruncate(logTfd, entry.offset);
  if(code < 0) {
    return -1;
  }
  code = tfFtruncate(idxTfd, idxOff);
  if(code < 0) {
    return -1;
  }
  pWal->vers.lastVer = ver - 1;
  ((WalFileInfo*)taosArrayGetLast(pWal->fileInfoSet))->lastVer = ver - 1;
  ((WalFileInfo*)taosArrayGetLast(pWal->fileInfoSet))->fileSize = entry.offset;

  //unlock
  pthread_mutex_unlock(&pWal->mutex);
  return 0;
}

int32_t walBeginTakeSnapshot(SWal* pWal, int64_t ver) {
  pWal->vers.verInSnapshotting = ver;
  //check file rolling
  if(pWal->cfg.retentionPeriod == 0) {
    walRoll(pWal);
  }

  return 0;
}

int32_t walEndTakeSnapshot(SWal *pWal) {
  int64_t ver = pWal->vers.verInSnapshotting;
  if(ver == -1) return -1;

  pWal->vers.snapshotVer = ver;
  int ts = taosGetTimestampSec();

  int deleteCnt = 0;
  int64_t newTotSize = pWal->totSize;
  WalFileInfo tmp;
  tmp.firstVer = ver;
  //find files safe to delete
  WalFileInfo* pInfo = taosArraySearch(pWal->fileInfoSet, &tmp, compareWalFileInfo, TD_LE);
  if(ver >= pInfo->lastVer) {
    pInfo++;
  }
  //iterate files, until the searched result
  for(WalFileInfo* iter = pWal->fileInfoSet->pData; iter < pInfo; iter++) {
    if(pWal->totSize > pWal->cfg.retentionSize ||
        iter->closeTs + pWal->cfg.retentionPeriod > ts) {
      //delete according to file size or close time
      deleteCnt++;
      newTotSize -= iter->fileSize;
    }
  }
  char fnameStr[WAL_FILE_LEN];
  //remove file
  for(int i = 0; i < deleteCnt; i++) {
    WalFileInfo* pInfo = taosArrayGet(pWal->fileInfoSet, i);
    walBuildLogName(pWal, pInfo->firstVer, fnameStr); 
    remove(fnameStr);
    walBuildIdxName(pWal, pInfo->firstVer, fnameStr); 
    remove(fnameStr);
  }

  //make new array, remove files
  taosArrayPopFrontBatch(pWal->fileInfoSet, deleteCnt); 
  if(taosArrayGetSize(pWal->fileInfoSet) == 0) {
    pWal->writeCur = -1;
    pWal->vers.firstVer = -1;
  } else {
    pWal->vers.firstVer = ((WalFileInfo*)taosArrayGet(pWal->fileInfoSet, 0))->firstVer;
  }
  pWal->writeCur = taosArrayGetSize(pWal->fileInfoSet) - 1;;
  pWal->totSize = newTotSize;
  pWal->vers.verInSnapshotting = -1;

  //save snapshot ver, commit ver
  int code = walWriteMeta(pWal);
  if(code != 0) {
    return -1;
  }

  return 0;
}

int walRoll(SWal *pWal) {
  int code = 0;
  if(pWal->writeIdxTfd != -1) {
    code = tfClose(pWal->writeIdxTfd);
    if(code != 0) {
      return -1;
    }
  }
  if(pWal->writeLogTfd != -1) {
    code = tfClose(pWal->writeLogTfd);
    if(code != 0) {
      return -1;
    }
  }
  int64_t idxTfd, logTfd;
  //create new file
  int64_t newFileFirstVersion = pWal->vers.lastVer + 1;
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
  pWal->writeIdxTfd = idxTfd;
  pWal->writeLogTfd = logTfd;
  pWal->writeCur = taosArrayGetSize(pWal->fileInfoSet) - 1;
  //change status
  pWal->curStatus = WAL_CUR_FILE_WRITABLE & WAL_CUR_POS_WRITABLE;

  pWal->lastRollSeq = walGetSeq();
  return 0;
}

static int walWriteIndex(SWal *pWal, int64_t ver, int64_t offset) {
  WalIdxEntry entry = { .ver = ver, .offset = offset };
  int size = tfWrite(pWal->writeIdxTfd, &entry, sizeof(WalIdxEntry));
  if(size != sizeof(WalIdxEntry)) {
    //TODO truncate
    return -1;
  }
  return 0;
}

int64_t walWrite(SWal *pWal, int64_t index, uint8_t msgType, const void *body, int32_t bodyLen) {
  if (pWal == NULL) return -1;
  int code = 0;

  // no wal
  if (pWal->cfg.level == TAOS_WAL_NOLOG) return 0;

  if (index == pWal->vers.lastVer + 1) {
    if(taosArrayGetSize(pWal->fileInfoSet) == 0) {
      pWal->vers.firstVer = index;
      code = walRoll(pWal);
      ASSERT(code == 0);
    } else {
      int64_t passed = walGetSeq() - pWal->lastRollSeq;
      if(pWal->cfg.rollPeriod != -1 && pWal->cfg.rollPeriod != 0 && passed > pWal->cfg.rollPeriod) {
        walRoll(pWal);
      } else if(pWal->cfg.segSize != -1 && pWal->cfg.segSize != 0 && walGetLastFileSize(pWal) > pWal->cfg.segSize) {
        walRoll(pWal);
      }
    }
  } else {
    //reject skip log or rewrite log
    //must truncate explicitly first
    return -1;
  }
  /*if (!tfValid(pWal->writeLogTfd)) return -1;*/

  pthread_mutex_lock(&pWal->mutex);
  pWal->writeHead.head.version = index;

  int64_t offset = walGetCurFileOffset(pWal);
  pWal->writeHead.head.len = bodyLen;
  pWal->writeHead.head.msgType = msgType;
  pWal->writeHead.cksumHead = walCalcHeadCksum(&pWal->writeHead);
  pWal->writeHead.cksumBody = walCalcBodyCksum(body, bodyLen);

  if (tfWrite(pWal->writeLogTfd, &pWal->writeHead, sizeof(SWalHead)) != sizeof(SWalHead)) {
    //ftruncate
    code = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%"PRId64".log, failed to write since %s", pWal->cfg.vgId, walGetLastFileFirstVer(pWal), strerror(errno));
  }

  if (tfWrite(pWal->writeLogTfd, (char*)body, bodyLen) != bodyLen) {
    //ftruncate
    code = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%"PRId64".log, failed to write since %s", pWal->cfg.vgId, walGetLastFileFirstVer(pWal), strerror(errno));
  }
  code = walWriteIndex(pWal, index, offset);
  if(code != 0) {
    //TODO
    return -1;
  }

  //set status
  pWal->vers.lastVer = index;
  pWal->totSize += sizeof(SWalHead) + bodyLen;
  walGetCurFileInfo(pWal)->lastVer = index;
  walGetCurFileInfo(pWal)->fileSize += sizeof(SWalHead) + bodyLen;
  
  pthread_mutex_unlock(&pWal->mutex);

  return code;
}

void walFsync(SWal *pWal, bool forceFsync) {
  if (forceFsync || (pWal->cfg.level == TAOS_WAL_FSYNC && pWal->cfg.fsyncPeriod == 0)) {
    wTrace("vgId:%d, fileId:%"PRId64".log, do fsync", pWal->cfg.vgId, walGetCurFileFirstVer(pWal));
    if (tfFsync(pWal->writeLogTfd) < 0) {
      wError("vgId:%d, file:%"PRId64".log, fsync failed since %s", pWal->cfg.vgId, walGetCurFileFirstVer(pWal), strerror(errno));
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
