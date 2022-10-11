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
#include "tutil.h"
#include "walInt.h"

bool FORCE_INLINE walLogExist(SWal* pWal, int64_t ver) {
  return !walIsEmpty(pWal) && walGetFirstVer(pWal) <= ver && walGetLastVer(pWal) >= ver;
}

bool FORCE_INLINE walIsEmpty(SWal* pWal) { return pWal->vers.firstVer == -1; }

int64_t FORCE_INLINE walGetFirstVer(SWal* pWal) { return pWal->vers.firstVer; }

int64_t FORCE_INLINE walGetSnaphostVer(SWal* pWal) { return pWal->vers.snapshotVer; }

int64_t FORCE_INLINE walGetLastVer(SWal* pWal) { return pWal->vers.lastVer; }

int64_t FORCE_INLINE walGetCommittedVer(SWal* pWal) { return pWal->vers.commitVer; }

int64_t FORCE_INLINE walGetAppliedVer(SWal* pWal) { return pWal->vers.appliedVer; }

static FORCE_INLINE int walBuildMetaName(SWal* pWal, int metaVer, char* buf) {
  return sprintf(buf, "%s/meta-ver%d", pWal->path, metaVer);
}

static FORCE_INLINE int walBuildTmpMetaName(SWal* pWal, char* buf) {
  return sprintf(buf, "%s/meta-ver.tmp", pWal->path);
}

static FORCE_INLINE int64_t walScanLogGetLastVer(SWal* pWal) {
  int32_t sz = taosArrayGetSize(pWal->fileInfoSet);
  if (sz <= 0) {
    wError("No WAL log file found.");
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    return -1;
  }

  SWalFileInfo* pLastFileInfo = taosArrayGet(pWal->fileInfoSet, sz - 1);
  char          fnameStr[WAL_FILE_LEN];
  walBuildLogName(pWal, pLastFileInfo->firstVer, fnameStr);

  int64_t fileSize = 0;
  taosStatFile(fnameStr, &fileSize, NULL);
  int32_t readSize = TMIN(WAL_SCAN_BUF_SIZE, fileSize);
  pLastFileInfo->fileSize = fileSize;

  TdFilePtr pFile = taosOpenFile(fnameStr, TD_FILE_READ | TD_FILE_WRITE);
  if (pFile == NULL) {
    wError("failed to open file due to %s. file:%s", strerror(errno), fnameStr);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  uint64_t magic = WAL_MAGIC;

  char* buf = taosMemoryMalloc(readSize + sizeof(uint64_t));
  if (buf == NULL) {
    terrno = TSDB_CODE_WAL_OUT_OF_MEMORY;
    goto _err;
  }

  int64_t offset;
  offset = taosLSeekFile(pFile, -readSize, SEEK_END);
  if (readSize != taosReadFile(pFile, buf, readSize)) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  int64_t walCkHeadSz = sizeof(SWalCkHead);
  char* found = NULL;
  while (1) {
    char* haystack = buf;
    char* candidate = NULL;
    while ((candidate = tmemmem(haystack, readSize - (haystack - buf), (char*)&magic, sizeof(uint64_t))) != NULL) {
      // read and validate
      int64_t len = readSize - (candidate - buf);
      if (len < walCkHeadSz) {
          break;
      }
      SWalCkHead* logContent = (SWalCkHead*)candidate;
      if (walValidHeadCksum(logContent) == 0 && walValidBodyCksum(logContent) == 0) {
        found = candidate;
      }
      haystack = candidate + 1;
    }
    if (found || offset == 0) break;

    // go backwards, i.e. by at most one WAL scan buf size
    offset = TMAX(0, offset - readSize + walCkHeadSz);
    int64_t ret = taosLSeekFile(pFile, offset, SEEK_SET);
    if (ret < 0)  {
      wError("failed to lseek file due to %s. offset:%lld", strerror(errno), offset);
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
    ASSERT(offset == ret);
    if (readSize != taosReadFile(pFile, buf, readSize)) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
  }

  if (found == NULL) {
    wError("WAL log file corrupted: no valid WAL record found. file: %s", fnameStr);
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    taosRemoveFile(fnameStr);
    wWarn("vgId:%d, remove corrupted WAL log file: %s", pWal->cfg.vgId, fnameStr);
    goto _err;
  }

  // truncate file
  SWalCkHead* lastEntry = (SWalCkHead*)found;
  int64_t     retVer = lastEntry->head.version;
  int64_t     lastEntryBeginOffset = offset + (int64_t)((char*)found - (char*)buf);
  int64_t     lastEntryEndOffset = lastEntryBeginOffset + sizeof(SWalCkHead) + lastEntry->head.bodyLen;
  if (lastEntryEndOffset != fileSize) {
    wWarn("vgId:%d repair meta truncate file %s to %ld, orig size %ld", pWal->cfg.vgId, fnameStr, lastEntryEndOffset,
          fileSize);
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
    ((SWalFileInfo*)taosArrayGetLast(pWal->fileInfoSet))->fileSize = lastEntryEndOffset;
    pWal->totSize -= (fileSize - lastEntryEndOffset);
  }

  taosCloseFile(&pFile);
  taosMemoryFree(buf);
  return retVer;

_err:
  taosCloseFile(&pFile);
  taosMemoryFree(buf);
  return -1;
}

int walCheckAndRepairMeta(SWal* pWal) {
  // load log files, get first/snapshot/last version info
  const char* logPattern = "^[0-9]+.log$";
  const char* idxPattern = "^[0-9]+.idx$";
  regex_t     logRegPattern;
  regex_t     idxRegPattern;
  SArray*     actualLog = taosArrayInit(8, sizeof(SWalFileInfo));

  regcomp(&logRegPattern, logPattern, REG_EXTENDED);
  regcomp(&idxRegPattern, idxPattern, REG_EXTENDED);

  TdDirPtr pDir = taosOpenDir(pWal->path);
  if (pDir == NULL) {
    wError("vgId:%d, path:%s, failed to open since %s", pWal->cfg.vgId, pWal->path, strerror(errno));
    return -1;
  }

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

  int metaFileNum = taosArrayGetSize(pWal->fileInfoSet);
  int actualFileNum = taosArrayGetSize(actualLog);

#if 0
  for (int32_t fileNo = actualFileNum - 1; fileNo >= 0; fileNo--) {
    SWalFileInfo* pFileInfo = taosArrayGet(pLogInfoArray, fileNo);
    char          fnameStr[WAL_FILE_LEN];
    walBuildLogName(pWal, pFileInfo->firstVer, fnameStr);
    int64_t fileSize = 0;
    taosStatFile(fnameStr, &fileSize, NULL);
    if (fileSize == 0) {
      taosRemoveFile(fnameStr);
      walBuildIdxName(pWal, pFileInfo->firstVer, fnameStr);
      taosRemoveFile(fnameStr);
      taosArrayPop(pLogInfoArray);
    } else {
      break;
    }
  }

  actualFileNum = taosArrayGetSize(pLogInfoArray);
#endif

  if (metaFileNum > actualFileNum) {
    taosArrayPopFrontBatch(pWal->fileInfoSet, metaFileNum - actualFileNum);
  } else if (metaFileNum < actualFileNum) {
    for (int i = metaFileNum; i < actualFileNum; i++) {
      SWalFileInfo* pFileInfo = taosArrayGet(actualLog, i);
      taosArrayPush(pWal->fileInfoSet, pFileInfo);
    }
  }
  taosArrayDestroy(actualLog);

  pWal->writeCur = actualFileNum - 1;
  if (actualFileNum > 0) {
    pWal->vers.firstVer = ((SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, 0))->firstVer;

    SWalFileInfo* pLastFileInfo = taosArrayGet(pWal->fileInfoSet, actualFileNum - 1);
    char          fnameStr[WAL_FILE_LEN];
    walBuildLogName(pWal, pLastFileInfo->firstVer, fnameStr);
    int64_t fileSize = 0;
    taosStatFile(fnameStr, &fileSize, NULL);
    /*ASSERT(fileSize != 0);*/

    if (metaFileNum != actualFileNum || pLastFileInfo->fileSize != fileSize) {
      pLastFileInfo->fileSize = fileSize;
      int64_t lastVer = walScanLogGetLastVer(pWal);
      if (lastVer < 0) {
        wError("failed to scan wal last ver since %s", tstrerror(terrno));
        return -1;
      }
      pWal->vers.lastVer = lastVer;
      ((SWalFileInfo*)taosArrayGetLast(pWal->fileInfoSet))->lastVer = pWal->vers.lastVer;

      (void)walSaveMeta(pWal);
    }
  }

  return 0;
}

int walCheckAndRepairIdx(SWal* pWal) {
  int32_t sz = taosArrayGetSize(pWal->fileInfoSet);
  for (int32_t i = 0; i < sz; i++) {
    SWalFileInfo* pFileInfo = taosArrayGet(pWal->fileInfoSet, i);

    char fnameStr[WAL_FILE_LEN];
    walBuildIdxName(pWal, pFileInfo->firstVer, fnameStr);
    int64_t   fsize;
    TdFilePtr pIdxFile = taosOpenFile(fnameStr, TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE);
    if (pIdxFile == NULL) {
      ASSERT(0);
      terrno = TAOS_SYSTEM_ERROR(errno);
      wError("vgId:%d, cannot open file %s, since %s", pWal->cfg.vgId, fnameStr, terrstr());
      return -1;
    }

    taosFStatFile(pIdxFile, &fsize, NULL);
    if (fsize == (pFileInfo->lastVer - pFileInfo->firstVer + 1) * sizeof(SWalIdxEntry)) {
      taosCloseFile(&pIdxFile);
      continue;
    }

    int32_t left = fsize % sizeof(SWalIdxEntry);
    int64_t offset = taosLSeekFile(pIdxFile, -left, SEEK_END);
    if (left != 0) {
      taosFtruncateFile(pIdxFile, offset);
      wWarn("vgId:%d wal truncate file %s to offset %ld since size invalid, file size %ld", pWal->cfg.vgId, fnameStr,
            offset, fsize);
    }
    offset -= sizeof(SWalIdxEntry);

    SWalIdxEntry idxEntry = {.ver = pFileInfo->firstVer};
    while (1) {
      if (offset < 0) {
        taosLSeekFile(pIdxFile, 0, SEEK_SET);
        taosWriteFile(pIdxFile, &idxEntry, sizeof(SWalIdxEntry));
        break;
      }
      taosLSeekFile(pIdxFile, offset, SEEK_SET);
      int64_t contLen = taosReadFile(pIdxFile, &idxEntry, sizeof(SWalIdxEntry));
      if (contLen < 0 || contLen != sizeof(SWalIdxEntry)) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }
      if ((idxEntry.ver - pFileInfo->firstVer) * sizeof(SWalIdxEntry) != offset) {
        taosFtruncateFile(pIdxFile, offset);
        wWarn("vgId:%d wal truncate file %s to offset %ld since entry invalid, entry ver %ld, entry offset %ld",
              pWal->cfg.vgId, fnameStr, offset, idxEntry.ver, idxEntry.offset);
        offset -= sizeof(SWalIdxEntry);
      } else {
        break;
      }
    }

    if (idxEntry.ver < pFileInfo->lastVer) {
      char fLogNameStr[WAL_FILE_LEN];
      walBuildLogName(pWal, pFileInfo->firstVer, fLogNameStr);
      TdFilePtr pLogFile = taosOpenFile(fLogNameStr, TD_FILE_READ);
      if (pLogFile == NULL) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        wError("vgId:%d, cannot open file %s, since %s", pWal->cfg.vgId, fLogNameStr, terrstr());
        return -1;
      }
      while (idxEntry.ver < pFileInfo->lastVer) {
        taosLSeekFile(pLogFile, idxEntry.offset, SEEK_SET);
        SWalCkHead ckHead;
        taosReadFile(pLogFile, &ckHead, sizeof(SWalCkHead));
        if (idxEntry.ver != ckHead.head.version) {
          // todo truncate this idx also
          taosCloseFile(&pLogFile);
          wError("vgId:%d, invalid repair case, log seek to %ld to find ver %ld, actual ver %ld", pWal->cfg.vgId,
                 idxEntry.offset, idxEntry.ver, ckHead.head.version);
          return -1;
        }
        idxEntry.ver = ckHead.head.version + 1;
        idxEntry.offset = idxEntry.offset + sizeof(SWalCkHead) + ckHead.head.bodyLen;
        wWarn("vgId:%d wal idx append new entry %ld %ld", pWal->cfg.vgId, idxEntry.ver, idxEntry.offset);
        taosWriteFile(pIdxFile, &idxEntry, sizeof(SWalIdxEntry));
      }
      taosCloseFile(&pLogFile);
    }
    taosCloseFile(&pIdxFile);
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
    terrno = TSDB_CODE_WAL_OUT_OF_MEMORY;
    return -1;
  }
  pNewInfo->firstVer = pWal->vers.lastVer + 1;
  pNewInfo->lastVer = -1;
  pNewInfo->createTs = ts;
  pNewInfo->closeTs = -1;
  pNewInfo->fileSize = 0;
  taosArrayPush(pArray, pNewInfo);
  taosMemoryFree(pNewInfo);
  return 0;
}

char* walMetaSerialize(SWal* pWal) {
  char buf[30];
  ASSERT(pWal->fileInfoSet);
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
    terrno = TSDB_CODE_WAL_OUT_OF_MEMORY;
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
  ASSERT(taosArrayGetSize(pWal->fileInfoSet) == 0);
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
  SWalFileInfo* pData = pArray->pData;
  for (int i = 0; i < sz; i++) {
    cJSON*        pInfoJson = cJSON_GetArrayItem(pFiles, i);
    if (!pInfoJson) goto _err;
    SWalFileInfo* pInfo = &pData[i];
    pField = cJSON_GetObjectItem(pInfoJson, "firstVer");
    if (!pField) goto _err;
    pInfo->firstVer = atoll(cJSON_GetStringValue(pField));
    pField = cJSON_GetObjectItem(pInfoJson, "lastVer");
    if (!pField) goto _err;
    pInfo->lastVer = atoll(cJSON_GetStringValue(pField));
    pField = cJSON_GetObjectItem(pInfoJson, "createTs");
    if (!pField) goto _err;
    pInfo->createTs = atoll(cJSON_GetStringValue(pField));
    pField = cJSON_GetObjectItem(pInfoJson, "closeTs");
    if (!pField) goto _err;
    pInfo->closeTs = atoll(cJSON_GetStringValue(pField));
    pField = cJSON_GetObjectItem(pInfoJson, "fileSize");
    if (!pField) goto _err;
    pInfo->fileSize = atoll(cJSON_GetStringValue(pField));
  }
  taosArraySetSize(pArray, sz);
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

int walSaveMeta(SWal* pWal) {
  int  metaVer = walFindCurMetaVer(pWal);
  char fnameStr[WAL_FILE_LEN];
  char tmpFnameStr[WAL_FILE_LEN];
  int n;

  // flush to a tmpfile
  n = walBuildTmpMetaName(pWal, tmpFnameStr);
  ASSERT(n < sizeof(tmpFnameStr) && "Buffer overflow of file name");

  TdFilePtr pMetaFile = taosOpenFile(tmpFnameStr, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pMetaFile == NULL) {
    wError("failed to open file due to %s. file:%s", strerror(errno), tmpFnameStr);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  char* serialized = walMetaSerialize(pWal);
  int   len = strlen(serialized);
  if (len != taosWriteFile(pMetaFile, serialized, len)) {
    wError("failed to write file due to %s. file:%s", strerror(errno), tmpFnameStr);
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosFsyncFile(pMetaFile) < 0) {
    wError("failed to sync file due to %s. file:%s", strerror(errno), tmpFnameStr);
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosCloseFile(&pMetaFile) < 0) {
    wError("failed to close file due to %s. file:%s", strerror(errno), tmpFnameStr);
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // rename it
  n = walBuildMetaName(pWal, metaVer + 1, fnameStr);
  ASSERT(n < sizeof(fnameStr) && "Buffer overflow of file name");

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
  ASSERT(pWal->fileInfoSet->size == 0);
  // find existing meta file
  int metaVer = walFindCurMetaVer(pWal);
  if (metaVer == -1) {
    wDebug("vgId:%d wal find meta ver %d", pWal->cfg.vgId, metaVer);
    return -1;
  }
  char fnameStr[WAL_FILE_LEN];
  walBuildMetaName(pWal, metaVer, fnameStr);
  // read metafile
  int64_t fileSize = 0;
  taosStatFile(fnameStr, &fileSize, NULL);
  int   size = (int)fileSize;
  char* buf = taosMemoryMalloc(size + 5);
  if (buf == NULL) {
    terrno = TSDB_CODE_WAL_OUT_OF_MEMORY;
    return -1;
  }
  memset(buf, 0, size + 5);
  TdFilePtr pFile = taosOpenFile(fnameStr, TD_FILE_READ);
  if (pFile == NULL) {
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
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
  taosRemoveFile(fnameStr);
  return 0;
}
