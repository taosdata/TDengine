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

#include "cJSON.h"
#include "os.h"
#include "taoserror.h"
#include "tglobal.h"
#include "tutil.h"
#include "walInt.h"


bool FORCE_INLINE walLogExist(SWal* pWal, int64_t ver) {
  return !walIsEmpty(pWal) && walGetFirstVer(pWal) <= ver && walGetLastVer(pWal) >= ver;
}

bool FORCE_INLINE walIsEmpty(SWal* pWal) {
  return (pWal->vers.firstVer == -1 || pWal->vers.lastVer < pWal->vers.firstVer);  // [firstVer, lastVer + 1)
}

int64_t FORCE_INLINE walGetFirstVer(SWal* pWal) { return pWal->vers.firstVer; }

int64_t FORCE_INLINE walGetSnapshotVer(SWal* pWal) { return pWal->vers.snapshotVer; }

int64_t FORCE_INLINE walGetLastVer(SWal* pWal) { return pWal->vers.lastVer; }

int64_t FORCE_INLINE walGetCommittedVer(SWal* pWal) { return pWal->vers.commitVer; }

int64_t FORCE_INLINE walGetAppliedVer(SWal* pWal) { return pWal->vers.appliedVer; }

static FORCE_INLINE int walBuildMetaName(SWal* pWal, int metaVer, char* buf) {
  return sprintf(buf, "%s/meta-ver%d", pWal->path, metaVer);
}

static FORCE_INLINE int walBuildTmpMetaName(SWal* pWal, char* buf) {
  return sprintf(buf, "%s/meta-ver.tmp", pWal->path);
}

static FORCE_INLINE int64_t walScanLogGetLastVer(SWal* pWal, int32_t fileIdx) {
  int32_t       sz = taosArrayGetSize(pWal->fileInfoSet);
  SWalFileInfo* pFileInfo = taosArrayGet(pWal->fileInfoSet, fileIdx);
  char          fnameStr[WAL_FILE_LEN];
  walBuildLogName(pWal, pFileInfo->firstVer, fnameStr);

  int64_t fileSize = 0;
  taosStatFile(fnameStr, &fileSize, NULL, NULL);

  TdFilePtr pFile = taosOpenFile(fnameStr, TD_FILE_READ | TD_FILE_WRITE);
  if (pFile == NULL) {
    wError("vgId:%d, failed to open file due to %s. file:%s", pWal->cfg.vgId, strerror(errno), fnameStr);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  // ensure size as non-negative
  pFileInfo->fileSize = TMAX(0, pFileInfo->fileSize);

  int64_t  stepSize = WAL_SCAN_BUF_SIZE;
  uint64_t magic = WAL_MAGIC;
  int64_t  walCkHeadSz = sizeof(SWalCkHead);
  int64_t  end = fileSize;
  int64_t  capacity = 0;
  int64_t  readSize = 0;
  char*    buf = NULL;
  int64_t  offset = TMIN(pFileInfo->fileSize, fileSize);
  int64_t  retVer = -1;
  int64_t  lastEntryBeginOffset = 0;
  int64_t  lastEntryEndOffset = 0;
  int64_t  recordLen = 0;
  bool     forwardStage = false;

  // check recover size
  if (2 * tsWalFsyncDataSizeLimit + offset < end) {
    wWarn("vgId:%d, possibly corrupted WAL range exceeds size limit (i.e. %" PRId64 " bytes). offset:%" PRId64
          ", end:%" PRId64 ", file:%s",
          pWal->cfg.vgId, 2 * tsWalFsyncDataSizeLimit, offset, end, fnameStr);
  }

  // search for the valid last WAL entry, e.g. block by block
  while (1) {
    offset = (lastEntryEndOffset > 0) ? offset : TMAX(0, offset - stepSize + walCkHeadSz - 1);
    end = TMIN(offset + stepSize, fileSize);

    readSize = end - offset;
    capacity = readSize + sizeof(magic);

    void* ptr = taosMemoryRealloc(buf, capacity);
    if (ptr == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    buf = ptr;

    int64_t ret = taosLSeekFile(pFile, offset, SEEK_SET);
    if (ret < 0) {
      wError("vgId:%d, failed to lseek file due to %s. offset:%" PRId64 "", pWal->cfg.vgId, strerror(errno), offset);
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    if (readSize != taosReadFile(pFile, buf, readSize)) {
      wError("vgId:%d, failed to read file due to %s. readSize:%" PRId64 ", file:%s", pWal->cfg.vgId, strerror(errno),
             readSize, fnameStr);
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    char*       candidate = NULL;
    char*       haystack = buf;
    int64_t     pos = 0;
    SWalCkHead* logContent = NULL;

    while (true) {
      forwardStage = (lastEntryEndOffset > 0 || offset == 0);
      terrno = TSDB_CODE_SUCCESS;
      if (forwardStage) {
        candidate = (readSize - (haystack - buf)) > 0 ? haystack : NULL;
      } else {
        candidate = tmemmem(haystack, readSize - (haystack - buf), (char*)&magic, sizeof(magic));
      }

      if (candidate == NULL) break;
      pos = candidate - buf;

      // validate head
      int64_t len = readSize - pos;
      if (len < walCkHeadSz) {
        break;
      }

      logContent = (SWalCkHead*)(buf + pos);
      if (walValidHeadCksum(logContent) != 0) {
        terrno = TSDB_CODE_WAL_CHKSUM_MISMATCH;
        wWarn("vgId:%d, failed to validate checksum of wal entry header. offset:%" PRId64 ", file:%s", pWal->cfg.vgId,
              offset + pos, fnameStr);
        haystack = buf + pos + 1;
        if (forwardStage) {
          break;
        } else {
          continue;
        }
      }

      // validate body
      recordLen = walCkHeadSz + logContent->head.bodyLen;
      if (len < recordLen) {
        int64_t extraSize = recordLen - len;
        if (capacity < readSize + extraSize + sizeof(magic)) {
          capacity += extraSize;
          void* ptr = taosMemoryRealloc(buf, capacity);
          if (ptr == NULL) {
            terrno = TSDB_CODE_OUT_OF_MEMORY;
            goto _err;
          }
          buf = ptr;
        }
        int64_t ret = taosLSeekFile(pFile, offset + readSize, SEEK_SET);
        if (ret < 0) {
          wError("vgId:%d, failed to lseek file due to %s. offset:%" PRId64 "", pWal->cfg.vgId, strerror(errno),
                 offset);
          terrno = TAOS_SYSTEM_ERROR(errno);
          break;
        }
        if (extraSize != taosReadFile(pFile, buf + readSize, extraSize)) {
          wError("vgId:%d, failed to read file due to %s. offset:%" PRId64 ", extraSize:%" PRId64 ", file:%s",
                 pWal->cfg.vgId, strerror(errno), offset + readSize, extraSize, fnameStr);
          terrno = TAOS_SYSTEM_ERROR(errno);
          break;
        }
      }

      logContent = (SWalCkHead*)(buf + pos);
      if (walValidBodyCksum(logContent) != 0) {
        terrno = TSDB_CODE_WAL_CHKSUM_MISMATCH;
        wWarn("vgId:%d, failed to validate checksum of wal entry body. offset:%" PRId64 ", file:%s", pWal->cfg.vgId,
              offset + pos, fnameStr);
        haystack = buf + pos + 1;
        if (forwardStage) {
          break;
        } else {
          continue;
        }
      }

      // found one
      retVer = logContent->head.version;
      lastEntryBeginOffset = offset + pos;
      lastEntryEndOffset = offset + pos + recordLen;

      // try next
      haystack = buf + pos + recordLen;
    }

    offset = (lastEntryEndOffset > 0) ? lastEntryEndOffset : offset;
    if (forwardStage && (terrno != TSDB_CODE_SUCCESS || end == fileSize)) break;
  }

  if (retVer < 0) {
    terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
  }

  // truncate file
  if (lastEntryEndOffset != fileSize) {
    wWarn("vgId:%d, repair meta truncate file %s to %" PRId64 ", orig size %" PRId64, pWal->cfg.vgId, fnameStr,
          lastEntryEndOffset, fileSize);

    if (taosFtruncateFile(pFile, lastEntryEndOffset) < 0) {
      wError("failed to truncate file due to %s. file:%s", strerror(errno), fnameStr);
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    if (taosFsyncFile(pFile) < 0) {
      wError("failed to fsync file due to %s. file:%s", strerror(errno), fnameStr);
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
  }

  pFileInfo->fileSize = lastEntryEndOffset;

  taosCloseFile(&pFile);
  taosMemoryFree(buf);
  return retVer;

_err:
  taosCloseFile(&pFile);
  taosMemoryFree(buf);
  return -1;
}

static void walRebuildFileInfoSet(SArray* metaLogList, SArray* actualLogList) {
  int metaFileNum = taosArrayGetSize(metaLogList);
  int actualFileNum = taosArrayGetSize(actualLogList);
  int j = 0;

  // both of the lists in asc order
  for (int i = 0; i < actualFileNum; i++) {
    SWalFileInfo* pLogInfo = taosArrayGet(actualLogList, i);
    while (j < metaFileNum) {
      SWalFileInfo* pMetaInfo = taosArrayGet(metaLogList, j);
      if (pMetaInfo->firstVer < pLogInfo->firstVer) {
        j++;
      } else if (pMetaInfo->firstVer == pLogInfo->firstVer) {
        (*pLogInfo) = *pMetaInfo;
        j++;
        break;
      } else {
        break;
      }
    }
  }

  taosArrayClear(metaLogList);

  for (int i = 0; i < actualFileNum; i++) {
    SWalFileInfo* pFileInfo = taosArrayGet(actualLogList, i);
    taosArrayPush(metaLogList, pFileInfo);
  }
}

void walAlignVersions(SWal* pWal) {
  if (pWal->vers.firstVer > pWal->vers.snapshotVer + 1) {
    wWarn("vgId:%d, firstVer:%" PRId64 " is larger than snapshotVer:%" PRId64 " + 1. align with it.", pWal->cfg.vgId,
          pWal->vers.firstVer, pWal->vers.snapshotVer);
    pWal->vers.firstVer = pWal->vers.snapshotVer + 1;
  }
  if (pWal->vers.lastVer < pWal->vers.snapshotVer) {
    wWarn("vgId:%d, lastVer:%" PRId64 " is less than snapshotVer:%" PRId64 ". align with it.", pWal->cfg.vgId,
          pWal->vers.lastVer, pWal->vers.snapshotVer);
    if (pWal->vers.lastVer < pWal->vers.firstVer) {
      pWal->vers.firstVer = pWal->vers.snapshotVer + 1;
    }
    pWal->vers.lastVer = pWal->vers.snapshotVer;
  }
  // reset commitVer and appliedVer
  pWal->vers.commitVer = pWal->vers.snapshotVer;
  pWal->vers.appliedVer = pWal->vers.snapshotVer;
  wInfo("vgId:%d, reset commitVer to %" PRId64, pWal->cfg.vgId, pWal->vers.commitVer);
}

int walRepairLogFileTs(SWal* pWal, bool* updateMeta) {
  int32_t sz = taosArrayGetSize(pWal->fileInfoSet);
  int32_t fileIdx = -1;
  int32_t lastCloseTs = 0;
  char    fnameStr[WAL_FILE_LEN] = {0};

  while (++fileIdx < sz - 1) {
    SWalFileInfo* pFileInfo = taosArrayGet(pWal->fileInfoSet, fileIdx);
    if (pFileInfo->closeTs != -1) {
      lastCloseTs = pFileInfo->closeTs;
      continue;
    }

    walBuildLogName(pWal, pFileInfo->firstVer, fnameStr);
    int32_t mtime = 0;
    if (taosStatFile(fnameStr, NULL, &mtime, NULL) < 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      wError("vgId:%d, failed to stat file due to %s, file:%s", pWal->cfg.vgId, strerror(errno), fnameStr);
      return -1;
    }

    if (updateMeta != NULL) *updateMeta = true;
    if (pFileInfo->createTs == -1) pFileInfo->createTs = lastCloseTs;
    pFileInfo->closeTs = mtime;
    lastCloseTs = pFileInfo->closeTs;
  }

  return 0;
}

bool walLogEntriesComplete(const SWal* pWal) {
  int32_t sz = taosArrayGetSize(pWal->fileInfoSet);
  bool    complete = true;
  int32_t fileIdx = -1;
  int64_t index = pWal->vers.firstVer;

  while (++fileIdx < sz) {
    SWalFileInfo* pFileInfo = taosArrayGet(pWal->fileInfoSet, fileIdx);
    if (pFileInfo->firstVer != index) {
      break;
    }
    index = pFileInfo->lastVer + ((fileIdx + 1 < sz) ? 1 : 0);
  }
  // empty is regarded as complete
  if (sz != 0) {
    complete = (index == pWal->vers.lastVer);
  }

  if (!complete) {
    wError("vgId:%d, WAL log entries incomplete in range [%" PRId64 ", %" PRId64 "], index:%" PRId64
           ", snaphotVer:%" PRId64,
           pWal->cfg.vgId, pWal->vers.firstVer, pWal->vers.lastVer, index, pWal->vers.snapshotVer);
    terrno = TSDB_CODE_WAL_LOG_INCOMPLETE;
  }

  return complete;
}

int walTrimIdxFile(SWal* pWal, int32_t fileIdx) {
  SWalFileInfo* pFileInfo = taosArrayGet(pWal->fileInfoSet, fileIdx);
  ASSERT(pFileInfo != NULL);
  char fnameStr[WAL_FILE_LEN];
  walBuildIdxName(pWal, pFileInfo->firstVer, fnameStr);

  int64_t fileSize = 0;
  taosStatFile(fnameStr, &fileSize, NULL, NULL);
  int64_t records = TMAX(0, pFileInfo->lastVer - pFileInfo->firstVer + 1);
  int64_t lastEndOffset = records * sizeof(SWalIdxEntry);

  if (fileSize <= lastEndOffset) {
    return 0;
  }

  TdFilePtr pFile = taosOpenFile(fnameStr, TD_FILE_READ | TD_FILE_WRITE);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  wInfo("vgId:%d, trim idx file. file: %s, size: %" PRId64 ", offset: %" PRId64, pWal->cfg.vgId, fnameStr, fileSize,
        lastEndOffset);

  taosFtruncateFile(pFile, lastEndOffset);
  taosCloseFile(&pFile);
  return 0;
}

int walCheckAndRepairMeta(SWal* pWal) {
  // load log files, get first/snapshot/last version info
  const char* logPattern = "^[0-9]+.log$";
  const char* idxPattern = "^[0-9]+.idx$";
  regex_t     logRegPattern;
  regex_t     idxRegPattern;

  regcomp(&logRegPattern, logPattern, REG_EXTENDED);
  regcomp(&idxRegPattern, idxPattern, REG_EXTENDED);

  TdDirPtr pDir = taosOpenDir(pWal->path);
  if (pDir == NULL) {
    regfree(&logRegPattern);
    regfree(&idxRegPattern);
    wError("vgId:%d, path:%s, failed to open since %s", pWal->cfg.vgId, pWal->path, strerror(errno));
    return -1;
  }

  SArray* actualLog = taosArrayInit(8, sizeof(SWalFileInfo));

  // scan log files and build new meta
  TdDirEntryPtr pDirEntry;
  while ((pDirEntry = taosReadDir(pDir)) != NULL) {
    char* name = taosDirEntryBaseName(taosGetDirEntryName(pDirEntry));
    int   code = regexec(&logRegPattern, name, 0, NULL, 0);
    if (code == 0) {
      SWalFileInfo fileInfo;
      memset(&fileInfo, -1, sizeof(SWalFileInfo));
      sscanf(name, "%" PRId64 ".log", &fileInfo.firstVer);
      taosArrayPush(actualLog, &fileInfo);
    }
  }

  taosCloseDir(&pDir);
  regfree(&logRegPattern);
  regfree(&idxRegPattern);

  taosArraySort(actualLog, compareWalFileInfo);

  int     metaFileNum = taosArrayGetSize(pWal->fileInfoSet);
  int     actualFileNum = taosArrayGetSize(actualLog);
  int64_t firstVerPrev = pWal->vers.firstVer;
  int64_t lastVerPrev = pWal->vers.lastVer;
  int64_t totSize = 0;
  bool    updateMeta = (metaFileNum != actualFileNum);

  // rebuild meta of file info
  walRebuildFileInfoSet(pWal->fileInfoSet, actualLog);
  taosArrayDestroy(actualLog);

  int32_t sz = taosArrayGetSize(pWal->fileInfoSet);

  // scan and determine the lastVer
  int32_t fileIdx = sz;

  while (--fileIdx >= 0) {
    char          fnameStr[WAL_FILE_LEN];
    int64_t       fileSize = 0;
    SWalFileInfo* pFileInfo = taosArrayGet(pWal->fileInfoSet, fileIdx);

    walBuildLogName(pWal, pFileInfo->firstVer, fnameStr);
    int32_t code = taosStatFile(fnameStr, &fileSize, NULL, NULL);
    if (code < 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      wError("failed to stat file since %s. file:%s", terrstr(), fnameStr);
      return -1;
    }

    if (pFileInfo->lastVer >= pFileInfo->firstVer && fileSize == pFileInfo->fileSize) {
      totSize += pFileInfo->fileSize;
      continue;
    }
    updateMeta = true;

    (void)walTrimIdxFile(pWal, fileIdx);

    int64_t lastVer = walScanLogGetLastVer(pWal, fileIdx);
    if (lastVer < 0) {
      if (terrno != TSDB_CODE_WAL_LOG_NOT_EXIST) {
        wError("failed to scan wal last ver since %s", terrstr());
        return -1;
      }
      // empty log file
      lastVer = pFileInfo->firstVer - 1;
    }

    // update lastVer
    pFileInfo->lastVer = lastVer;
    totSize += pFileInfo->fileSize;
  }

  // reset vers info and so on
  actualFileNum = taosArrayGetSize(pWal->fileInfoSet);
  pWal->writeCur = actualFileNum - 1;
  pWal->totSize = totSize;
  pWal->vers.lastVer = -1;
  if (actualFileNum > 0) {
    pWal->vers.firstVer = ((SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, 0))->firstVer;
    pWal->vers.lastVer = ((SWalFileInfo*)taosArrayGetLast(pWal->fileInfoSet))->lastVer;
  }
  (void)walAlignVersions(pWal);

  // repair ts of files
  if (walRepairLogFileTs(pWal, &updateMeta) < 0) {
    return -1;
  }

  // update meta file
  if (updateMeta) {
    (void)walSaveMeta(pWal);
  }

  if (!walLogEntriesComplete(pWal)) {
    return -1;
  }

  return 0;
}

int walReadLogHead(TdFilePtr pLogFile, int64_t offset, SWalCkHead* pCkHead) {
  if (taosLSeekFile(pLogFile, offset, SEEK_SET) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosReadFile(pLogFile, pCkHead, sizeof(SWalCkHead)) != sizeof(SWalCkHead)) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (walValidHeadCksum(pCkHead) != 0) {
    terrno = TSDB_CODE_WAL_CHKSUM_MISMATCH;
    return -1;
  }

  return 0;
}

int walCheckAndRepairIdxFile(SWal* pWal, int32_t fileIdx) {
  int32_t       sz = taosArrayGetSize(pWal->fileInfoSet);
  SWalFileInfo* pFileInfo = taosArrayGet(pWal->fileInfoSet, fileIdx);
  char          fnameStr[WAL_FILE_LEN];
  walBuildIdxName(pWal, pFileInfo->firstVer, fnameStr);
  char fLogNameStr[WAL_FILE_LEN];
  walBuildLogName(pWal, pFileInfo->firstVer, fLogNameStr);
  int64_t fileSize = 0;

  if (taosStatFile(fnameStr, &fileSize, NULL, NULL) < 0 && errno != ENOENT) {
    wError("vgId:%d, failed to stat file due to %s. file:%s", pWal->cfg.vgId, strerror(errno), fnameStr);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (fileSize == (pFileInfo->lastVer - pFileInfo->firstVer + 1) * sizeof(SWalIdxEntry)) {
    return 0;
  }

  // start to repair
  int64_t      offset = fileSize - fileSize % sizeof(SWalIdxEntry);
  TdFilePtr    pLogFile = NULL;
  TdFilePtr    pIdxFile = NULL;
  SWalIdxEntry idxEntry = {.ver = pFileInfo->firstVer - 1, .offset = -sizeof(SWalCkHead)};
  SWalCkHead   ckHead;
  memset(&ckHead, 0, sizeof(ckHead));
  ckHead.head.version = idxEntry.ver;

  pIdxFile = taosOpenFile(fnameStr, TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE);
  if (pIdxFile == NULL) {
    wError("vgId:%d, failed to open file due to %s. file:%s", pWal->cfg.vgId, strerror(errno), fnameStr);
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  pLogFile = taosOpenFile(fLogNameStr, TD_FILE_READ);
  if (pLogFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, cannot open file %s, since %s", pWal->cfg.vgId, fLogNameStr, terrstr());
    goto _err;
  }

  // determine the last valid entry end, i.e. offset
  while ((offset -= sizeof(SWalIdxEntry)) >= 0) {
    if (taosLSeekFile(pIdxFile, offset, SEEK_SET) < 0) {
      wError("vgId:%d, failed to seek file due to %s. offset:%" PRId64 ", file:%s", pWal->cfg.vgId, strerror(errno),
             offset, fnameStr);
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    if (taosReadFile(pIdxFile, &idxEntry, sizeof(SWalIdxEntry)) != sizeof(SWalIdxEntry)) {
      wError("vgId:%d, failed to read file due to %s. offset:%" PRId64 ", file:%s", pWal->cfg.vgId, strerror(errno),
             offset, fnameStr);
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    if (idxEntry.ver > pFileInfo->lastVer) {
      continue;
    }

    if (offset != (idxEntry.ver - pFileInfo->firstVer) * sizeof(SWalIdxEntry)) {
      continue;
    }

    if (walReadLogHead(pLogFile, idxEntry.offset, &ckHead) < 0) {
      wWarn("vgId:%d, failed to read log file since %s. file:%s, offset:%" PRId64 ", idx entry ver:%" PRId64 "",
            pWal->cfg.vgId, terrstr(), fLogNameStr, idxEntry.offset, idxEntry.ver);
      continue;
    }

    if (idxEntry.ver == ckHead.head.version) {
      break;
    }
  }
  offset += sizeof(SWalIdxEntry);

  /*A(offset == (idxEntry.ver - pFileInfo->firstVer + 1) * sizeof(SWalIdxEntry));*/

  // ftruncate idx file
  if (offset < fileSize) {
    if (taosFtruncateFile(pIdxFile, offset) < 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      wError("vgId:%d, failed to ftruncate file since %s. offset:%" PRId64 ", file:%s", pWal->cfg.vgId, terrstr(),
             offset, fnameStr);
      goto _err;
    }
  }

  // rebuild idx file
  if (taosLSeekFile(pIdxFile, 0, SEEK_END) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, failed to seek file since %s. offset:%" PRId64 ", file:%s", pWal->cfg.vgId, terrstr(), offset,
           fnameStr);
    goto _err;
  }

  int64_t count = 0;
  while (idxEntry.ver < pFileInfo->lastVer) {
    /*A(idxEntry.ver == ckHead.head.version);*/

    idxEntry.ver += 1;
    idxEntry.offset += sizeof(SWalCkHead) + ckHead.head.bodyLen;

    if (walReadLogHead(pLogFile, idxEntry.offset, &ckHead) < 0) {
      wError("vgId:%d, failed to read wal log head since %s. index:%" PRId64 ", offset:%" PRId64 ", file:%s",
             pWal->cfg.vgId, terrstr(), idxEntry.ver, idxEntry.offset, fLogNameStr);
      goto _err;
    }
    if (taosWriteFile(pIdxFile, &idxEntry, sizeof(SWalIdxEntry)) < 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      wError("vgId:%d, failed to append file since %s. file:%s", pWal->cfg.vgId, terrstr(), fnameStr);
      goto _err;
    }
    count++;
  }

  if (taosFsyncFile(pIdxFile) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, faild to fsync file since %s. file:%s", pWal->cfg.vgId, terrstr(), fnameStr);
    goto _err;
  }

  if (count > 0) {
    wInfo("vgId:%d, rebuilt %" PRId64 " wal idx entries until lastVer: %" PRId64, pWal->cfg.vgId, count,
          pFileInfo->lastVer);
  }

  (void)taosCloseFile(&pLogFile);
  (void)taosCloseFile(&pIdxFile);
  return 0;

_err:
  (void)taosCloseFile(&pLogFile);
  (void)taosCloseFile(&pIdxFile);
  return -1;
}

int64_t walGetVerRetention(SWal* pWal, int64_t bytes) {
  int64_t ver = -1;
  int64_t totSize = 0;
  taosThreadMutexLock(&pWal->mutex);
  int32_t fileIdx = taosArrayGetSize(pWal->fileInfoSet);
  while (--fileIdx) {
    SWalFileInfo* pInfo = taosArrayGet(pWal->fileInfoSet, fileIdx);
    if (totSize >= bytes) {
      ver = pInfo->lastVer;
      break;
    }
    totSize += pInfo->fileSize;
  }
  taosThreadMutexUnlock(&pWal->mutex);
  return ver + 1;
}

int walCheckAndRepairIdx(SWal* pWal) {
  int32_t sz = taosArrayGetSize(pWal->fileInfoSet);
  int32_t fileIdx = sz;
  while (--fileIdx >= 0) {
    if (walCheckAndRepairIdxFile(pWal, fileIdx) < 0) {
      wError("vgId:%d, failed to repair idx file since %s. fileIdx:%d", pWal->cfg.vgId, terrstr(), fileIdx);
      return -1;
    }
  }
  return 0;
}

int walRollFileInfo(SWal* pWal) {
  int64_t ts = taosGetTimestampSec();

  SArray* pArray = pWal->fileInfoSet;
  if (taosArrayGetSize(pArray) != 0) {
    SWalFileInfo* pInfo = taosArrayGetLast(pArray);
    pInfo->lastVer = pWal->vers.lastVer;
    pInfo->closeTs = ts;
  }

  // TODO: change to emplace back
  SWalFileInfo* pNewInfo = taosMemoryMalloc(sizeof(SWalFileInfo));
  if (pNewInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  pNewInfo->firstVer = pWal->vers.lastVer + 1;
  pNewInfo->lastVer = -1;
  pNewInfo->createTs = ts;
  pNewInfo->closeTs = -1;
  pNewInfo->fileSize = 0;
  pNewInfo->syncedOffset = 0;
  taosArrayPush(pArray, pNewInfo);
  taosMemoryFree(pNewInfo);
  return 0;
}

char* walMetaSerialize(SWal* pWal) {
  char   buf[30];
  int    sz = taosArrayGetSize(pWal->fileInfoSet);
  cJSON* pRoot = cJSON_CreateObject();
  cJSON* pMeta = cJSON_CreateObject();
  cJSON* pFiles = cJSON_CreateArray();
  cJSON* pField;
  if (pRoot == NULL || pMeta == NULL || pFiles == NULL) {
    if (pRoot) {
      cJSON_Delete(pRoot);
    }
    if (pMeta) {
      cJSON_Delete(pMeta);
    }
    if (pFiles) {
      cJSON_Delete(pFiles);
    }
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  cJSON_AddItemToObject(pRoot, "meta", pMeta);
  sprintf(buf, "%" PRId64, pWal->vers.firstVer);
  cJSON_AddStringToObject(pMeta, "firstVer", buf);
  sprintf(buf, "%" PRId64, pWal->vers.snapshotVer);
  cJSON_AddStringToObject(pMeta, "snapshotVer", buf);
  sprintf(buf, "%" PRId64, pWal->vers.commitVer);
  cJSON_AddStringToObject(pMeta, "commitVer", buf);
  sprintf(buf, "%" PRId64, pWal->vers.lastVer);
  cJSON_AddStringToObject(pMeta, "lastVer", buf);

  cJSON_AddItemToObject(pRoot, "files", pFiles);
  SWalFileInfo* pData = pWal->fileInfoSet->pData;
  for (int i = 0; i < sz; i++) {
    SWalFileInfo* pInfo = &pData[i];
    cJSON_AddItemToArray(pFiles, pField = cJSON_CreateObject());
    if (pField == NULL) {
      cJSON_Delete(pRoot);
      return NULL;
    }
    // cjson only support int32_t or double
    // string are used to prohibit the loss of precision
    sprintf(buf, "%" PRId64, pInfo->firstVer);
    cJSON_AddStringToObject(pField, "firstVer", buf);
    sprintf(buf, "%" PRId64, pInfo->lastVer);
    cJSON_AddStringToObject(pField, "lastVer", buf);
    sprintf(buf, "%" PRId64, pInfo->createTs);
    cJSON_AddStringToObject(pField, "createTs", buf);
    sprintf(buf, "%" PRId64, pInfo->closeTs);
    cJSON_AddStringToObject(pField, "closeTs", buf);
    sprintf(buf, "%" PRId64, pInfo->fileSize);
    cJSON_AddStringToObject(pField, "fileSize", buf);
  }
  char* serialized = cJSON_Print(pRoot);
  cJSON_Delete(pRoot);
  return serialized;
}

int walMetaDeserialize(SWal* pWal, const char* bytes) {
  /*A(taosArrayGetSize(pWal->fileInfoSet) == 0);*/
  cJSON *pRoot, *pMeta, *pFiles, *pInfoJson, *pField;
  pRoot = cJSON_Parse(bytes);
  if (!pRoot) goto _err;
  pMeta = cJSON_GetObjectItem(pRoot, "meta");
  if (!pMeta) goto _err;
  pField = cJSON_GetObjectItem(pMeta, "firstVer");
  if (!pField) goto _err;
  pWal->vers.firstVer = atoll(cJSON_GetStringValue(pField));
  pField = cJSON_GetObjectItem(pMeta, "snapshotVer");
  if (!pField) goto _err;
  pWal->vers.snapshotVer = atoll(cJSON_GetStringValue(pField));
  pField = cJSON_GetObjectItem(pMeta, "commitVer");
  if (!pField) goto _err;
  pWal->vers.commitVer = atoll(cJSON_GetStringValue(pField));
  pField = cJSON_GetObjectItem(pMeta, "lastVer");
  if (!pField) goto _err;
  pWal->vers.lastVer = atoll(cJSON_GetStringValue(pField));

  pFiles = cJSON_GetObjectItem(pRoot, "files");
  int sz = cJSON_GetArraySize(pFiles);
  // deserialize
  SArray* pArray = pWal->fileInfoSet;
  taosArrayEnsureCap(pArray, sz);

  for (int i = 0; i < sz; i++) {
    pInfoJson = cJSON_GetArrayItem(pFiles, i);
    if (!pInfoJson) goto _err;

    SWalFileInfo info = {0};

    pField = cJSON_GetObjectItem(pInfoJson, "firstVer");
    if (!pField) goto _err;
    info.firstVer = atoll(cJSON_GetStringValue(pField));
    pField = cJSON_GetObjectItem(pInfoJson, "lastVer");
    if (!pField) goto _err;
    info.lastVer = atoll(cJSON_GetStringValue(pField));
    pField = cJSON_GetObjectItem(pInfoJson, "createTs");
    if (!pField) goto _err;
    info.createTs = atoll(cJSON_GetStringValue(pField));
    pField = cJSON_GetObjectItem(pInfoJson, "closeTs");
    if (!pField) goto _err;
    info.closeTs = atoll(cJSON_GetStringValue(pField));
    pField = cJSON_GetObjectItem(pInfoJson, "fileSize");
    if (!pField) goto _err;
    info.fileSize = atoll(cJSON_GetStringValue(pField));
    taosArrayPush(pArray, &info);
  }
  pWal->fileInfoSet = pArray;
  pWal->writeCur = sz - 1;
  cJSON_Delete(pRoot);
  return 0;

_err:
  cJSON_Delete(pRoot);
  return -1;
}

static int walFindCurMetaVer(SWal* pWal) {
  const char* pattern = "^meta-ver[0-9]+$";
  regex_t     walMetaRegexPattern;
  regcomp(&walMetaRegexPattern, pattern, REG_EXTENDED);

  TdDirPtr pDir = taosOpenDir(pWal->path);
  if (pDir == NULL) {
    wError("vgId:%d, path:%s, failed to open since %s", pWal->cfg.vgId, pWal->path, strerror(errno));
    return -1;
  }

  TdDirEntryPtr pDirEntry;

  // find existing meta-ver[x].json
  int metaVer = -1;
  while ((pDirEntry = taosReadDir(pDir)) != NULL) {
    char* name = taosDirEntryBaseName(taosGetDirEntryName(pDirEntry));
    int   code = regexec(&walMetaRegexPattern, name, 0, NULL, 0);
    if (code == 0) {
      sscanf(name, "meta-ver%d", &metaVer);
      wDebug("vgId:%d, wal find current meta: %s is the meta file, ver %d", pWal->cfg.vgId, name, metaVer);
      break;
    }
    wDebug("vgId:%d, wal find current meta: %s is not meta file", pWal->cfg.vgId, name);
  }
  taosCloseDir(&pDir);
  regfree(&walMetaRegexPattern);
  return metaVer;
}

void walUpdateSyncedOffset(SWal* pWal) {
  SWalFileInfo* pFileInfo = walGetCurFileInfo(pWal);
  if (pFileInfo == NULL) return;
  pFileInfo->syncedOffset = pFileInfo->fileSize;
}

int walSaveMeta(SWal* pWal) {
  int  metaVer = walFindCurMetaVer(pWal);
  char fnameStr[WAL_FILE_LEN];
  char tmpFnameStr[WAL_FILE_LEN];
  int  n;

  // fsync the idx and log file at first to ensure validity of meta
  if (taosFsyncFile(pWal->pIdxFile) < 0) {
    wError("vgId:%d, failed to sync idx file due to %s", pWal->cfg.vgId, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosFsyncFile(pWal->pLogFile) < 0) {
    wError("vgId:%d, failed to sync log file due to %s", pWal->cfg.vgId, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  // update synced offset
  (void)walUpdateSyncedOffset(pWal);

  // flush to a tmpfile
  n = walBuildTmpMetaName(pWal, tmpFnameStr);
  if (n >= sizeof(tmpFnameStr)) {
    return -1;
  }

  TdFilePtr pMetaFile = taosOpenFile(tmpFnameStr, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pMetaFile == NULL) {
    wError("vgId:%d, failed to open file due to %s. file:%s", pWal->cfg.vgId, strerror(errno), tmpFnameStr);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  char* serialized = walMetaSerialize(pWal);
  int   len = strlen(serialized);
  if (len != taosWriteFile(pMetaFile, serialized, len)) {
    wError("vgId:%d, failed to write file due to %s. file:%s", pWal->cfg.vgId, strerror(errno), tmpFnameStr);
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosFsyncFile(pMetaFile) < 0) {
    wError("vgId:%d, failed to sync file due to %s. file:%s", pWal->cfg.vgId, strerror(errno), tmpFnameStr);
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosCloseFile(&pMetaFile) < 0) {
    wError("vgId:%d, failed to close file due to %s. file:%s", pWal->cfg.vgId, strerror(errno), tmpFnameStr);
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // rename it
  n = walBuildMetaName(pWal, metaVer + 1, fnameStr);
  if (n >= sizeof(fnameStr)) {
    goto _err;
  }

  if (taosRenameFile(tmpFnameStr, fnameStr) < 0) {
    wError("failed to rename file due to %s. dest:%s", strerror(errno), fnameStr);
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // delete old file
  if (metaVer > -1) {
    walBuildMetaName(pWal, metaVer, fnameStr);
    taosRemoveFile(fnameStr);
  }
  taosMemoryFree(serialized);
  return 0;

_err:
  taosCloseFile(&pMetaFile);
  taosMemoryFree(serialized);
  return -1;
}

int walLoadMeta(SWal* pWal) {
  // find existing meta file
  int metaVer = walFindCurMetaVer(pWal);
  if (metaVer == -1) {
    wDebug("vgId:%d, wal find meta ver %d", pWal->cfg.vgId, metaVer);
    return -1;
  }
  char fnameStr[WAL_FILE_LEN];
  walBuildMetaName(pWal, metaVer, fnameStr);
  // read metafile
  int64_t fileSize = 0;
  taosStatFile(fnameStr, &fileSize, NULL, NULL);
  if (fileSize == 0) {
    (void)taosRemoveFile(fnameStr);
    wDebug("vgId:%d, wal find empty meta ver %d", pWal->cfg.vgId, metaVer);
    return -1;
  }
  int   size = (int)fileSize;
  char* buf = taosMemoryMalloc(size + 5);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  memset(buf, 0, size + 5);
  TdFilePtr pFile = taosOpenFile(fnameStr, TD_FILE_READ);
  if (pFile == NULL) {
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    taosMemoryFree(buf);
    return -1;
  }
  if (taosReadFile(pFile, buf, size) != size) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    taosCloseFile(&pFile);
    taosMemoryFree(buf);
    return -1;
  }
  // load into fileInfoSet
  int code = walMetaDeserialize(pWal, buf);
  if (code < 0) {
    wError("failed to deserialize wal meta. file:%s", fnameStr);
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
  }
  taosCloseFile(&pFile);
  taosMemoryFree(buf);
  return code;
}

int walRemoveMeta(SWal* pWal) {
  int metaVer = walFindCurMetaVer(pWal);
  if (metaVer == -1) return 0;
  char fnameStr[WAL_FILE_LEN];
  walBuildMetaName(pWal, metaVer, fnameStr);
  return taosRemoveFile(fnameStr);
}
