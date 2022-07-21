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

static FORCE_INLINE int64_t walScanLogGetLastVer(SWal* pWal) {
  int32_t sz = taosArrayGetSize(pWal->fileInfoSet);
  ASSERT(sz > 0);
#if 0
  for (int i = 0; i < sz; i++) {
    SWalFileInfo* pFileInfo = taosArrayGet(pWal->fileInfoSet, i);
  }
#endif

  SWalFileInfo* pLastFileInfo = taosArrayGet(pWal->fileInfoSet, sz - 1);
  char          fnameStr[WAL_FILE_LEN];
  walBuildLogName(pWal, pLastFileInfo->firstVer, fnameStr);

  int64_t fileSize = 0;
  taosStatFile(fnameStr, &fileSize, NULL);
  int32_t readSize = TMIN(WAL_SCAN_BUF_SIZE, fileSize);
  pLastFileInfo->fileSize = fileSize;

  TdFilePtr pFile = taosOpenFile(fnameStr, TD_FILE_READ);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  uint64_t magic = WAL_MAGIC;

  char* buf = taosMemoryMalloc(readSize + 5);
  if (buf == NULL) {
    taosCloseFile(&pFile);
    terrno = TSDB_CODE_WAL_OUT_OF_MEMORY;
    return -1;
  }

  int64_t offset;
  offset = taosLSeekFile(pFile, -readSize, SEEK_END);
  if (readSize != taosReadFile(pFile, buf, readSize)) {
    taosMemoryFree(buf);
    taosCloseFile(&pFile);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  char* found = NULL;
  while (1) {
    char* haystack = buf;
    char* candidate;
    while ((candidate = tmemmem(haystack, readSize - (haystack - buf), (char*)&magic, sizeof(uint64_t))) != NULL) {
      // read and validate
      SWalCkHead* logContent = (SWalCkHead*)candidate;
      if (walValidHeadCksum(logContent) == 0 && walValidBodyCksum(logContent) == 0) {
        found = candidate;
      }
      haystack = candidate + 1;
    }
    if (found || offset == 0) break;
    offset = TMIN(0, offset - readSize + sizeof(uint64_t));
    int64_t offset2 = taosLSeekFile(pFile, offset, SEEK_SET);
    ASSERT(offset == offset2);
    if (readSize != taosReadFile(pFile, buf, readSize)) {
      taosMemoryFree(buf);
      taosCloseFile(&pFile);
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
#if 0
    if (found == buf) {
      SWalCkHead* logContent = (SWalCkHead*)found;
      if (walValidHeadCksum(logContent) != 0 || walValidBodyCksum(logContent) != 0) {
        // file has to be deleted
        taosMemoryFree(buf);
        taosCloseFile(&pFile);
        terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
        return -1;
      }
    }
#endif
  }
  // TODO truncate file

  if (found == NULL) {
    // file corrupted, no complete log
    // TODO delete and search in previous files
    ASSERT(0);
    terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
    return -1;
  }
  SWalCkHead* lastEntry = (SWalCkHead*)found;
  int64_t     retVer = lastEntry->head.version;
  taosCloseFile(&pFile);
  taosMemoryFree(buf);

  return retVer;
}

int walCheckAndRepairMeta(SWal* pWal) {
  // load log files, get first/snapshot/last version info
  const char* logPattern = "^[0-9]+.log$";
  const char* idxPattern = "^[0-9]+.idx$";
  regex_t     logRegPattern;
  regex_t     idxRegPattern;
  SArray*     pLogInfoArray = taosArrayInit(8, sizeof(SWalFileInfo));

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
      taosArrayPush(pLogInfoArray, &fileInfo);
    }
  }

  taosCloseDir(&pDir);
  regfree(&logRegPattern);
  regfree(&idxRegPattern);

  taosArraySort(pLogInfoArray, compareWalFileInfo);

  int metaFileNum = taosArrayGetSize(pWal->fileInfoSet);
  int actualFileNum = taosArrayGetSize(pLogInfoArray);

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
      SWalFileInfo* pFileInfo = taosArrayGet(pLogInfoArray, i);
      taosArrayPush(pWal->fileInfoSet, pFileInfo);
    }
  }
  taosArrayDestroy(pLogInfoArray);

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
      pWal->vers.lastVer = walScanLogGetLastVer(pWal);
      ((SWalFileInfo*)taosArrayGetLast(pWal->fileInfoSet))->lastVer = pWal->vers.lastVer;
      ASSERT(pWal->vers.lastVer != -1);

      int code = walSaveMeta(pWal);
      if (code < 0) {
        taosArrayDestroy(pLogInfoArray);
        return -1;
      }
    }
  }

  // TODO: set fileSize and lastVer if necessary

  return 0;
}

int walCheckAndRepairIdx(SWal* pWal) {
  // TODO: iterate all log files
  // if idx not found, scan log and write idx
  // if found, check complete by first and last entry of each idx file
  // if idx incomplete, binary search last valid entry, and then build other part
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
  int    sz = pWal->fileInfoSet->size;
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
  pMeta = cJSON_GetObjectItem(pRoot, "meta");
  pField = cJSON_GetObjectItem(pMeta, "firstVer");
  pWal->vers.firstVer = atoll(cJSON_GetStringValue(pField));
  pField = cJSON_GetObjectItem(pMeta, "snapshotVer");
  pWal->vers.snapshotVer = atoll(cJSON_GetStringValue(pField));
  pField = cJSON_GetObjectItem(pMeta, "commitVer");
  pWal->vers.commitVer = atoll(cJSON_GetStringValue(pField));
  pField = cJSON_GetObjectItem(pMeta, "lastVer");
  pWal->vers.lastVer = atoll(cJSON_GetStringValue(pField));

  pFiles = cJSON_GetObjectItem(pRoot, "files");
  int sz = cJSON_GetArraySize(pFiles);
  // deserialize
  SArray* pArray = pWal->fileInfoSet;
  taosArrayEnsureCap(pArray, sz);
  SWalFileInfo* pData = pArray->pData;
  for (int i = 0; i < sz; i++) {
    cJSON*        pInfoJson = cJSON_GetArrayItem(pFiles, i);
    SWalFileInfo* pInfo = &pData[i];
    pField = cJSON_GetObjectItem(pInfoJson, "firstVer");
    pInfo->firstVer = atoll(cJSON_GetStringValue(pField));
    pField = cJSON_GetObjectItem(pInfoJson, "lastVer");
    pInfo->lastVer = atoll(cJSON_GetStringValue(pField));
    pField = cJSON_GetObjectItem(pInfoJson, "createTs");
    pInfo->createTs = atoll(cJSON_GetStringValue(pField));
    pField = cJSON_GetObjectItem(pInfoJson, "closeTs");
    pInfo->closeTs = atoll(cJSON_GetStringValue(pField));
    pField = cJSON_GetObjectItem(pInfoJson, "fileSize");
    pInfo->fileSize = atoll(cJSON_GetStringValue(pField));
  }
  taosArraySetSize(pArray, sz);
  pWal->fileInfoSet = pArray;
  pWal->writeCur = sz - 1;
  cJSON_Delete(pRoot);
  return 0;
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
      break;
    }
  }
  taosCloseDir(&pDir);
  regfree(&walMetaRegexPattern);
  return metaVer;
}

int walSaveMeta(SWal* pWal) {
  int  metaVer = walFindCurMetaVer(pWal);
  char fnameStr[WAL_FILE_LEN];
  walBuildMetaName(pWal, metaVer + 1, fnameStr);
  TdFilePtr pMataFile = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_WRITE);
  if (pMataFile == NULL) {
    return -1;
  }
  char* serialized = walMetaSerialize(pWal);
  int   len = strlen(serialized);
  if (len != taosWriteFile(pMataFile, serialized, len)) {
    // TODO:clean file
    return -1;
  }

  taosCloseFile(&pMataFile);
  // delete old file
  if (metaVer > -1) {
    walBuildMetaName(pWal, metaVer, fnameStr);
    taosRemoveFile(fnameStr);
  }
  taosMemoryFree(serialized);
  return 0;
}

int walLoadMeta(SWal* pWal) {
  ASSERT(pWal->fileInfoSet->size == 0);
  // find existing meta file
  int metaVer = walFindCurMetaVer(pWal);
  if (metaVer == -1) {
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
