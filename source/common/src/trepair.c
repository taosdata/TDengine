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
#include "trepair.h"

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>

#include "tjson.h"
#include "tutil.h"

typedef struct {
  const char *name;
  int32_t     value;
} SRepairOptionPair;

static const SRepairOptionPair kNodeTypeMap[] = {
    {"vnode", REPAIR_NODE_TYPE_VNODE},
    {"mnode", REPAIR_NODE_TYPE_MNODE},
    {"dnode", REPAIR_NODE_TYPE_DNODE},
    {"snode", REPAIR_NODE_TYPE_SNODE},
};

static const SRepairOptionPair kFileTypeMap[] = {
    {"wal", REPAIR_FILE_TYPE_WAL},
    {"tsdb", REPAIR_FILE_TYPE_TSDB},
    {"meta", REPAIR_FILE_TYPE_META},
    {"tdb", REPAIR_FILE_TYPE_META},  // backward-compatible alias
    {"data", REPAIR_FILE_TYPE_DATA},
    {"config", REPAIR_FILE_TYPE_CONFIG},
    {"checkpoint", REPAIR_FILE_TYPE_CHECKPOINT},
};

static const SRepairOptionPair kModeMap[] = {
    {"force", REPAIR_MODE_FORCE},
    {"replica", REPAIR_MODE_REPLICA},
    {"copy", REPAIR_MODE_COPY},
};

static const char *kMetaRequiredFiles[] = {
    "table.db",
    "schema.db",
    "uid.idx",
    "name.idx",
};

static const char *kMetaOptionalIndexFiles[] = {
    "ctb.idx",
    "suid.idx",
    "tag.idx",
    "sma.idx",
    "ctime.idx",
    "ncol.idx",
    "stream.task.db",
};

#define REPAIR_SESSION_DIR_PREFIX  "repair-"
#define REPAIR_SESSION_LOG_NAME    "repair.log"
#define REPAIR_SESSION_STATE_NAME  "repair.state.json"
#define REPAIR_MAX_STATE_FILE_SIZE (1024 * 1024)

typedef struct {
  bool    found;
  int64_t startTimeMs;
  int32_t doneVnodes;
  int32_t totalVnodes;
  char    sessionId[REPAIR_SESSION_ID_LEN];
  char    sessionDir[PATH_MAX];
  char    logPath[PATH_MAX];
  char    statePath[PATH_MAX];
} SRepairResumeCandidate;

static int32_t tRepairParseOption(const char *input, const SRepairOptionPair *pMap, int32_t mapSize, int32_t *pValue) {
  if (input == NULL || pMap == NULL || pValue == NULL || mapSize <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  for (int32_t i = 0; i < mapSize; ++i) {
    if (taosStrcasecmp(input, pMap[i].name) == 0) {
      *pValue = pMap[i].value;
      return TSDB_CODE_SUCCESS;
    }
  }

  return TSDB_CODE_INVALID_PARA;
}

static int32_t tRepairParseStringOption(const char *input, char *output, int32_t outputSize) {
  if (input == NULL || output == NULL || outputSize <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t len = strlen(input);
  if (len <= 0 || len >= outputSize) {
    return TSDB_CODE_INVALID_PARA;
  }

  tstrncpy(output, input, outputSize);
  return TSDB_CODE_SUCCESS;
}

static bool tRepairIsFileTypeCompatible(ERepairNodeType nodeType, ERepairFileType fileType) {
  switch (nodeType) {
    case REPAIR_NODE_TYPE_VNODE:
      return fileType == REPAIR_FILE_TYPE_WAL || fileType == REPAIR_FILE_TYPE_TSDB ||
             fileType == REPAIR_FILE_TYPE_META;
    case REPAIR_NODE_TYPE_MNODE:
      return fileType == REPAIR_FILE_TYPE_WAL || fileType == REPAIR_FILE_TYPE_DATA;
    case REPAIR_NODE_TYPE_DNODE:
      return fileType == REPAIR_FILE_TYPE_CONFIG;
    case REPAIR_NODE_TYPE_SNODE:
      return fileType == REPAIR_FILE_TYPE_CHECKPOINT;
    default:
      return false;
  }
}

static char *tRepairTrimSpace(char *str) {
  while (*str != '\0' && isspace((unsigned char)*str)) {
    ++str;
  }

  char *end = str + strlen(str);
  while (end > str && isspace((unsigned char)*(end - 1))) {
    --end;
  }
  *end = '\0';

  return str;
}

static int32_t tRepairAppendVnodeId(SRepairCtx *pCtx, int32_t vnodeId) {
  if (pCtx->vnodeIdNum >= REPAIR_MAX_VNODE_IDS) {
    return TSDB_CODE_INVALID_PARA;
  }

  for (int32_t i = 0; i < pCtx->vnodeIdNum; ++i) {
    if (pCtx->vnodeIds[i] == vnodeId) {
      return TSDB_CODE_INVALID_PARA;
    }
  }

  pCtx->vnodeIds[pCtx->vnodeIdNum++] = vnodeId;
  return TSDB_CODE_SUCCESS;
}

static int32_t tRepairParseVnodeIdList(char *vnodeIdList, SRepairCtx *pCtx) {
  if (vnodeIdList == NULL || pCtx == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  char *savePtr = NULL;
  for (char *token = strtok_r(vnodeIdList, ",", &savePtr); token != NULL; token = strtok_r(NULL, ",", &savePtr)) {
    char *trimmed = tRepairTrimSpace(token);
    if (*trimmed == '\0') {
      return TSDB_CODE_INVALID_PARA;
    }

    errno = 0;
    char   *endPtr = NULL;
    int32_t parsed = taosStr2Int32(trimmed, &endPtr, 10);
    if (errno != 0 || endPtr == NULL || endPtr == trimmed || *endPtr != '\0' || parsed < 0) {
      return TSDB_CODE_INVALID_PARA;
    }

    int32_t code = tRepairAppendVnodeId(pCtx, parsed);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return pCtx->vnodeIdNum > 0 ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_PARA;
}

static const char *tRepairGetVnodeFileSubDir(ERepairFileType fileType) {
  switch (fileType) {
    case REPAIR_FILE_TYPE_WAL:
      return "wal";
    case REPAIR_FILE_TYPE_TSDB:
      return "tsdb";
    case REPAIR_FILE_TYPE_META:
      return "meta";
    default:
      return NULL;
  }
}

static const char *tRepairGetNodeTypeName(ERepairNodeType nodeType) {
  switch (nodeType) {
    case REPAIR_NODE_TYPE_VNODE:
      return "vnode";
    case REPAIR_NODE_TYPE_MNODE:
      return "mnode";
    case REPAIR_NODE_TYPE_DNODE:
      return "dnode";
    case REPAIR_NODE_TYPE_SNODE:
      return "snode";
    default:
      return "invalid";
  }
}

static const char *tRepairGetFileTypeName(ERepairFileType fileType) {
  switch (fileType) {
    case REPAIR_FILE_TYPE_WAL:
      return "wal";
    case REPAIR_FILE_TYPE_TSDB:
      return "tsdb";
    case REPAIR_FILE_TYPE_META:
      return "meta";
    case REPAIR_FILE_TYPE_DATA:
      return "data";
    case REPAIR_FILE_TYPE_CONFIG:
      return "config";
    case REPAIR_FILE_TYPE_CHECKPOINT:
      return "checkpoint";
    default:
      return "invalid";
  }
}

static const char *tRepairGetModeName(ERepairMode mode) {
  switch (mode) {
    case REPAIR_MODE_FORCE:
      return "force";
    case REPAIR_MODE_REPLICA:
      return "replica";
    case REPAIR_MODE_COPY:
      return "copy";
    default:
      return "invalid";
  }
}

static int32_t tRepairBuildVnodePath(const char *dataDir, int32_t vnodeId, const char *subDir, char *path,
                                     int32_t pathSize) {
  if (dataDir == NULL || dataDir[0] == '\0' || vnodeId < 0 || path == NULL || pathSize <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t len = 0;
  if (subDir == NULL || subDir[0] == '\0') {
    len = tsnprintf(path, pathSize, "%s%svnode%svnode%d", dataDir, TD_DIRSEP, TD_DIRSEP, vnodeId);
  } else {
    len = tsnprintf(path, pathSize, "%s%svnode%svnode%d%s%s", dataDir, TD_DIRSEP, TD_DIRSEP, vnodeId, TD_DIRSEP,
                    subDir);
  }

  if (len <= 0 || len >= pathSize) {
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tRepairBuildBackupBaseDir(const SRepairCtx *pCtx, const char *dataDir, char *backupBaseDir,
                                         int32_t backupBaseDirSize) {
  if (pCtx == NULL || dataDir == NULL || dataDir[0] == '\0' || backupBaseDir == NULL || backupBaseDirSize <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (pCtx->hasBackupPath) {
    return tRepairParseStringOption(pCtx->backupPath, backupBaseDir, backupBaseDirSize);
  }

  int32_t len = tsnprintf(backupBaseDir, backupBaseDirSize, "%s%sbackup", dataDir, TD_DIRSEP);
  if (len <= 0 || len >= backupBaseDirSize) {
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tRepairBuildBackupDir(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId, char *backupDir,
                                     int32_t backupDirSize) {
  if (pCtx == NULL || !pCtx->enabled || dataDir == NULL || dataDir[0] == '\0' || vnodeId < 0 || backupDir == NULL ||
      backupDirSize <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (pCtx->sessionId[0] == '\0') {
    return TSDB_CODE_INVALID_PARA;
  }

  if (pCtx->nodeType != REPAIR_NODE_TYPE_VNODE) {
    return TSDB_CODE_INVALID_PARA;
  }

  bool    shouldRepair = false;
  int32_t code = tRepairShouldRepairVnode(pCtx, vnodeId, &shouldRepair);
  if (code != TSDB_CODE_SUCCESS || !shouldRepair) {
    return TSDB_CODE_INVALID_PARA;
  }

  const char *fileSubDir = tRepairGetVnodeFileSubDir(pCtx->fileType);
  if (fileSubDir == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  char backupBaseDir[PATH_MAX] = {0};
  code = tRepairBuildBackupBaseDir(pCtx, dataDir, backupBaseDir, sizeof(backupBaseDir));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int32_t len = tsnprintf(backupDir, backupDirSize, "%s%s%s%svnode%d%s%s", backupBaseDir, TD_DIRSEP, pCtx->sessionId,
                          TD_DIRSEP, vnodeId, TD_DIRSEP, fileSubDir);
  if (len <= 0 || len >= backupDirSize) {
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tRepairBuildSessionDir(const SRepairCtx *pCtx, const char *dataDir, char *sessionDir,
                                      int32_t sessionDirSize) {
  if (pCtx == NULL || !pCtx->enabled || dataDir == NULL || dataDir[0] == '\0' || sessionDir == NULL ||
      sessionDirSize <= 0 || pCtx->sessionId[0] == '\0') {
    return TSDB_CODE_INVALID_PARA;
  }

  char backupBaseDir[PATH_MAX] = {0};
  int32_t code = tRepairBuildBackupBaseDir(pCtx, dataDir, backupBaseDir, sizeof(backupBaseDir));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int32_t len = tsnprintf(sessionDir, sessionDirSize, "%s%s%s", backupBaseDir, TD_DIRSEP, pCtx->sessionId);
  if (len <= 0 || len >= sessionDirSize) {
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tRepairBuildSessionFilePath(const char *sessionDir, const char *fileName, char *filePath,
                                           int32_t filePathSize) {
  if (sessionDir == NULL || sessionDir[0] == '\0' || fileName == NULL || fileName[0] == '\0' || filePath == NULL ||
      filePathSize <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t len = tsnprintf(filePath, filePathSize, "%s%s%s", sessionDir, TD_DIRSEP, fileName);
  if (len <= 0 || len >= filePathSize) {
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tRepairBuildPathWithEntry(const char *basePath, const char *entryName, char *outPath, int32_t outPathSize) {
  if (basePath == NULL || basePath[0] == '\0' || entryName == NULL || entryName[0] == '\0' || outPath == NULL ||
      outPathSize <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t len = tsnprintf(outPath, outPathSize, "%s%s%s", basePath, TD_DIRSEP, entryName);
  if (len <= 0 || len >= outPathSize) {
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

static bool tRepairStringEndsWithIgnoreCase(const char *str, const char *suffix) {
  if (str == NULL || suffix == NULL) {
    return false;
  }

  size_t strLen = strlen(str);
  size_t suffixLen = strlen(suffix);
  if (suffixLen <= 0 || suffixLen > strLen) {
    return false;
  }

  return taosStrcasecmp(str + strLen - suffixLen, suffix) == 0;
}

typedef enum {
  REPAIR_TSDB_FILE_KIND_UNKNOWN = 0,
  REPAIR_TSDB_FILE_KIND_HEAD,
  REPAIR_TSDB_FILE_KIND_DATA,
  REPAIR_TSDB_FILE_KIND_SMA,
  REPAIR_TSDB_FILE_KIND_STT,
} ERepairTsdbFileKind;

static ERepairTsdbFileKind tRepairClassifyTsdbFile(const char *fileName) {
  if (fileName == NULL || fileName[0] == '\0') {
    return REPAIR_TSDB_FILE_KIND_UNKNOWN;
  }

  if (tRepairStringEndsWithIgnoreCase(fileName, ".head")) {
    return REPAIR_TSDB_FILE_KIND_HEAD;
  }
  if (tRepairStringEndsWithIgnoreCase(fileName, ".data")) {
    return REPAIR_TSDB_FILE_KIND_DATA;
  }
  if (tRepairStringEndsWithIgnoreCase(fileName, ".sma")) {
    return REPAIR_TSDB_FILE_KIND_SMA;
  }
  if (tRepairStringEndsWithIgnoreCase(fileName, ".stt")) {
    return REPAIR_TSDB_FILE_KIND_STT;
  }

  return REPAIR_TSDB_FILE_KIND_UNKNOWN;
}

static void tRepairCountTsdbFileBySuffix(const char *fileName, SRepairTsdbScanResult *pResult) {
  if (fileName == NULL || fileName[0] == '\0' || pResult == NULL) {
    return;
  }

  switch (tRepairClassifyTsdbFile(fileName)) {
    case REPAIR_TSDB_FILE_KIND_HEAD:
      ++pResult->headFiles;
      return;
    case REPAIR_TSDB_FILE_KIND_DATA:
      ++pResult->dataFiles;
      return;
    case REPAIR_TSDB_FILE_KIND_SMA:
      ++pResult->smaFiles;
      return;
    case REPAIR_TSDB_FILE_KIND_STT:
      ++pResult->sttFiles;
      return;
    case REPAIR_TSDB_FILE_KIND_UNKNOWN:
    default:
      ++pResult->unknownFiles;
      return;
  }
}

static int32_t tRepairScanTsdbDirRecursive(const char *dirPath, SRepairTsdbScanResult *pResult) {
  if (dirPath == NULL || dirPath[0] == '\0' || pResult == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (!taosDirExist(dirPath) || !taosIsDir(dirPath)) {
    return TSDB_CODE_INVALID_PARA;
  }

  TdDirPtr pDir = taosOpenDir(dirPath);
  if (pDir == NULL) {
    return terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  int32_t       code = TSDB_CODE_SUCCESS;
  TdDirEntryPtr pDirEntry = NULL;
  while ((pDirEntry = taosReadDir(pDir)) != NULL) {
    char *entryName = taosGetDirEntryName(pDirEntry);
    if (entryName == NULL || strcmp(entryName, ".") == 0 || strcmp(entryName, "..") == 0) {
      continue;
    }

    if (taosDirEntryIsDir(pDirEntry)) {
      char entryPath[PATH_MAX] = {0};
      code = tRepairBuildPathWithEntry(dirPath, entryName, entryPath, sizeof(entryPath));
      if (code != TSDB_CODE_SUCCESS) {
        break;
      }

      code = tRepairScanTsdbDirRecursive(entryPath, pResult);
      if (code != TSDB_CODE_SUCCESS) {
        break;
      }
      continue;
    }

    tRepairCountTsdbFileBySuffix(entryName, pResult);
  }

  if (taosCloseDir(&pDir) != 0 && code == TSDB_CODE_SUCCESS) {
    code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  return code;
}

static void tRepairRecordCorruptedBlockPath(const char *dirPath, SRepairTsdbBlockReport *pReport) {
  if (dirPath == NULL || dirPath[0] == '\0' || pReport == NULL ||
      pReport->reportedCorruptedBlocks >= REPAIR_TSDB_MAX_REPORTED_BLOCKS) {
    return;
  }

  tstrncpy(pReport->corruptedBlockPaths[pReport->reportedCorruptedBlocks], dirPath,
           sizeof(pReport->corruptedBlockPaths[pReport->reportedCorruptedBlocks]));
  ++pReport->reportedCorruptedBlocks;
}

static void tRepairRecordMissingMetaFile(SRepairMetaScanResult *pResult, const char *fileName) {
  if (pResult == NULL || fileName == NULL || fileName[0] == '\0') {
    return;
  }

  if (pResult->missingRequiredFiles < REPAIR_META_MAX_MISSING_FILES) {
    tstrncpy(pResult->missingRequiredFileNames[pResult->missingRequiredFiles], fileName, REPAIR_META_FILE_NAME_LEN);
  }
  ++pResult->missingRequiredFiles;
}

static int32_t tRepairAnalyzeTsdbDirRecursive(const char *dirPath, SRepairTsdbBlockReport *pReport) {
  if (dirPath == NULL || dirPath[0] == '\0' || pReport == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (!taosDirExist(dirPath) || !taosIsDir(dirPath)) {
    return TSDB_CODE_INVALID_PARA;
  }

  TdDirPtr pDir = taosOpenDir(dirPath);
  if (pDir == NULL) {
    return terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       localHeadFiles = 0;
  int32_t       localDataFiles = 0;
  int32_t       localKnownFiles = 0;
  int32_t       localUnknownFiles = 0;
  TdDirEntryPtr pDirEntry = NULL;
  while ((pDirEntry = taosReadDir(pDir)) != NULL) {
    char *entryName = taosGetDirEntryName(pDirEntry);
    if (entryName == NULL || strcmp(entryName, ".") == 0 || strcmp(entryName, "..") == 0) {
      continue;
    }

    if (taosDirEntryIsDir(pDirEntry)) {
      char entryPath[PATH_MAX] = {0};
      code = tRepairBuildPathWithEntry(dirPath, entryName, entryPath, sizeof(entryPath));
      if (code != TSDB_CODE_SUCCESS) {
        break;
      }

      code = tRepairAnalyzeTsdbDirRecursive(entryPath, pReport);
      if (code != TSDB_CODE_SUCCESS) {
        break;
      }
      continue;
    }

    switch (tRepairClassifyTsdbFile(entryName)) {
      case REPAIR_TSDB_FILE_KIND_HEAD:
        ++localHeadFiles;
        ++localKnownFiles;
        break;
      case REPAIR_TSDB_FILE_KIND_DATA:
        ++localDataFiles;
        ++localKnownFiles;
        break;
      case REPAIR_TSDB_FILE_KIND_SMA:
      case REPAIR_TSDB_FILE_KIND_STT:
        ++localKnownFiles;
        break;
      case REPAIR_TSDB_FILE_KIND_UNKNOWN:
      default:
        ++localUnknownFiles;
        break;
    }
  }

  if (taosCloseDir(&pDir) != 0 && code == TSDB_CODE_SUCCESS) {
    code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pReport->unknownFiles += localUnknownFiles;
  if (localKnownFiles <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  ++pReport->totalBlocks;
  if (localHeadFiles > 0 && localDataFiles > 0) {
    ++pReport->recoverableBlocks;
  } else {
    ++pReport->corruptedBlocks;
    tRepairRecordCorruptedBlockPath(dirPath, pReport);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tRepairCopyDirRecursive(const char *srcDir, const char *dstDir);
static int32_t tRepairResetDir(const char *dirPath);

static int32_t tRepairBuildPathWithOptionalRelative(const char *basePath, const char *relativePath, char *outPath,
                                                    int32_t outPathSize) {
  if (basePath == NULL || basePath[0] == '\0' || outPath == NULL || outPathSize <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (relativePath == NULL || relativePath[0] == '\0') {
    return tRepairParseStringOption(basePath, outPath, outPathSize);
  }

  int32_t len = tsnprintf(outPath, outPathSize, "%s%s%s", basePath, TD_DIRSEP, relativePath);
  if (len <= 0 || len >= outPathSize) {
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tRepairCollectTsdbDirLocalStats(const char *dirPath, int32_t *pHeadFiles, int32_t *pDataFiles,
                                               int32_t *pKnownFiles, int32_t *pUnknownFiles) {
  if (dirPath == NULL || dirPath[0] == '\0' || pHeadFiles == NULL || pDataFiles == NULL || pKnownFiles == NULL ||
      pUnknownFiles == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pHeadFiles = 0;
  *pDataFiles = 0;
  *pKnownFiles = 0;
  *pUnknownFiles = 0;

  TdDirPtr pDir = taosOpenDir(dirPath);
  if (pDir == NULL) {
    return terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  int32_t       code = TSDB_CODE_SUCCESS;
  TdDirEntryPtr pDirEntry = NULL;
  while ((pDirEntry = taosReadDir(pDir)) != NULL) {
    char *entryName = taosGetDirEntryName(pDirEntry);
    if (entryName == NULL || strcmp(entryName, ".") == 0 || strcmp(entryName, "..") == 0 ||
        taosDirEntryIsDir(pDirEntry)) {
      continue;
    }

    switch (tRepairClassifyTsdbFile(entryName)) {
      case REPAIR_TSDB_FILE_KIND_HEAD:
        ++(*pHeadFiles);
        ++(*pKnownFiles);
        break;
      case REPAIR_TSDB_FILE_KIND_DATA:
        ++(*pDataFiles);
        ++(*pKnownFiles);
        break;
      case REPAIR_TSDB_FILE_KIND_SMA:
      case REPAIR_TSDB_FILE_KIND_STT:
        ++(*pKnownFiles);
        break;
      case REPAIR_TSDB_FILE_KIND_UNKNOWN:
      default:
        ++(*pUnknownFiles);
        break;
    }
  }

  if (taosCloseDir(&pDir) != 0 && code == TSDB_CODE_SUCCESS) {
    code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }
  return code;
}

static int32_t tRepairRebuildTsdbDirRecursive(const char *srcDir, const char *dstBaseDir, const char *relativePath,
                                              SRepairTsdbBlockReport *pReport) {
  if (srcDir == NULL || srcDir[0] == '\0' || dstBaseDir == NULL || dstBaseDir[0] == '\0' || pReport == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (!taosDirExist(srcDir) || !taosIsDir(srcDir)) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t localHeadFiles = 0;
  int32_t localDataFiles = 0;
  int32_t localKnownFiles = 0;
  int32_t localUnknownFiles = 0;
  int32_t code = tRepairCollectTsdbDirLocalStats(srcDir, &localHeadFiles, &localDataFiles, &localKnownFiles,
                                                 &localUnknownFiles);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pReport->unknownFiles += localUnknownFiles;
  if (localKnownFiles > 0) {
    ++pReport->totalBlocks;
    if (localHeadFiles > 0 && localDataFiles > 0) {
      ++pReport->recoverableBlocks;

      char dstDir[PATH_MAX] = {0};
      code = tRepairBuildPathWithOptionalRelative(dstBaseDir, relativePath, dstDir, sizeof(dstDir));
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      code = tRepairResetDir(dstDir);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      return tRepairCopyDirRecursive(srcDir, dstDir);
    }

    ++pReport->corruptedBlocks;
    tRepairRecordCorruptedBlockPath(srcDir, pReport);
    return TSDB_CODE_SUCCESS;
  }

  TdDirPtr pDir = taosOpenDir(srcDir);
  if (pDir == NULL) {
    return terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  TdDirEntryPtr pDirEntry = NULL;
  while ((pDirEntry = taosReadDir(pDir)) != NULL) {
    if (!taosDirEntryIsDir(pDirEntry)) {
      continue;
    }

    char *entryName = taosGetDirEntryName(pDirEntry);
    if (entryName == NULL || strcmp(entryName, ".") == 0 || strcmp(entryName, "..") == 0) {
      continue;
    }

    char childSrcDir[PATH_MAX] = {0};
    code = tRepairBuildPathWithEntry(srcDir, entryName, childSrcDir, sizeof(childSrcDir));
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }

    char childRelativePath[PATH_MAX] = {0};
    if (relativePath == NULL || relativePath[0] == '\0') {
      code = tRepairParseStringOption(entryName, childRelativePath, sizeof(childRelativePath));
    } else {
      int32_t len = tsnprintf(childRelativePath, sizeof(childRelativePath), "%s%s%s", relativePath, TD_DIRSEP,
                              entryName);
      code = (len > 0 && len < (int32_t)sizeof(childRelativePath)) ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_PARA;
    }
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }

    code = tRepairRebuildTsdbDirRecursive(childSrcDir, dstBaseDir, childRelativePath, pReport);
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }
  }

  if (taosCloseDir(&pDir) != 0 && code == TSDB_CODE_SUCCESS) {
    code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }
  return code;
}

static int32_t tRepairCopyDirRecursive(const char *srcDir, const char *dstDir) {
  if (srcDir == NULL || srcDir[0] == '\0' || dstDir == NULL || dstDir[0] == '\0') {
    return TSDB_CODE_INVALID_PARA;
  }

  if (!taosDirExist(srcDir) || !taosIsDir(srcDir)) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (taosMulMkDir(dstDir) != 0) {
    return terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  TdDirPtr pDir = taosOpenDir(srcDir);
  if (pDir == NULL) {
    return terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  int32_t     code = TSDB_CODE_SUCCESS;
  TdDirEntryPtr pDirEntry = NULL;
  while ((pDirEntry = taosReadDir(pDir)) != NULL) {
    char *entryName = taosGetDirEntryName(pDirEntry);
    if (entryName == NULL || strcmp(entryName, ".") == 0 || strcmp(entryName, "..") == 0) {
      continue;
    }

    char srcPath[PATH_MAX] = {0};
    char dstPath[PATH_MAX] = {0};
    code = tRepairBuildPathWithEntry(srcDir, entryName, srcPath, sizeof(srcPath));
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }
    code = tRepairBuildPathWithEntry(dstDir, entryName, dstPath, sizeof(dstPath));
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }

    if (taosDirEntryIsDir(pDirEntry)) {
      code = tRepairCopyDirRecursive(srcPath, dstPath);
    } else {
      int64_t copied = taosCopyFile(srcPath, dstPath);
      if (copied < 0) {
        code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
      }
    }

    if (code != TSDB_CODE_SUCCESS) {
      break;
    }
  }

  if (taosCloseDir(&pDir) != 0 && code == TSDB_CODE_SUCCESS) {
    code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  return code;
}

static int32_t tRepairResetDir(const char *dirPath) {
  if (dirPath == NULL || dirPath[0] == '\0') {
    return TSDB_CODE_INVALID_PARA;
  }

  if (taosDirExist(dirPath)) {
    taosRemoveDir(dirPath);
  }

  if (taosMulMkDir(dirPath) != 0) {
    return terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tRepairBuildVnodeTargetAndBackupPath(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId,
                                                    char *targetPath, int32_t targetPathSize, char *backupDir,
                                                    int32_t backupDirSize) {
  if (pCtx == NULL || !pCtx->enabled || dataDir == NULL || dataDir[0] == '\0' || vnodeId < 0 || targetPath == NULL ||
      targetPathSize <= 0 || backupDir == NULL || backupDirSize <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (pCtx->nodeType != REPAIR_NODE_TYPE_VNODE) {
    return TSDB_CODE_INVALID_PARA;
  }

  bool    shouldRepair = false;
  int32_t code = tRepairShouldRepairVnode(pCtx, vnodeId, &shouldRepair);
  if (code != TSDB_CODE_SUCCESS || !shouldRepair) {
    return TSDB_CODE_INVALID_PARA;
  }

  code = tRepairBuildVnodeTargetPath(dataDir, vnodeId, pCtx->fileType, targetPath, targetPathSize);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  return tRepairBuildBackupDir(pCtx, dataDir, vnodeId, backupDir, backupDirSize);
}

static int32_t tRepairReadTextFile(const char *filePath, char **ppContent, int64_t *pContentLen) {
  if (filePath == NULL || filePath[0] == '\0' || ppContent == NULL || pContentLen == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *ppContent = NULL;
  *pContentLen = 0;

  int64_t fileSize = 0;
  if (taosStatFile(filePath, &fileSize, NULL, NULL) != 0 || fileSize <= 0 || fileSize > REPAIR_MAX_STATE_FILE_SIZE) {
    return TSDB_CODE_INVALID_PARA;
  }

  TdFilePtr pFile = taosOpenFile(filePath, TD_FILE_READ);
  if (pFile == NULL) {
    return terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  char *pContent = taosMemoryMalloc(fileSize + 1);
  if (pContent == NULL) {
    (void)taosCloseFile(&pFile);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int64_t nread = taosReadFile(pFile, pContent, fileSize);
  if (nread != fileSize) {
    code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  if (taosCloseFile(&pFile) != 0 && code == TSDB_CODE_SUCCESS) {
    code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pContent);
    return code;
  }

  pContent[fileSize] = '\0';
  *ppContent = pContent;
  *pContentLen = fileSize;
  return TSDB_CODE_SUCCESS;
}

static bool tRepairMatchOptionalStateField(const SJson *pJson, const char *fieldName, bool hasExpected,
                                           const char *expectedValue) {
  if (pJson == NULL || fieldName == NULL || fieldName[0] == '\0') {
    return false;
  }

  bool hasField = tjsonGetObjectItem(pJson, fieldName) != NULL;
  if (!hasExpected) {
    return !hasField;
  }
  if (!hasField || expectedValue == NULL) {
    return false;
  }

  char    value[PATH_MAX] = {0};
  int32_t code = tjsonGetStringValue2(pJson, fieldName, value, sizeof(value));
  return code == TSDB_CODE_SUCCESS && strcmp(value, expectedValue) == 0;
}

static bool tRepairMatchStateVnodeList(const SRepairCtx *pCtx, const SJson *pJson, int32_t stateTotalVnodes) {
  if (pCtx == NULL || pJson == NULL) {
    return false;
  }

  if (pCtx->nodeType != REPAIR_NODE_TYPE_VNODE) {
    return stateTotalVnodes == 0;
  }

  if (!pCtx->hasVnodeIdList || pCtx->vnodeIdNum <= 0 || pCtx->vnodeIdNum != stateTotalVnodes) {
    return false;
  }

  char stateVnodeIdList[PATH_MAX] = {0};
  if (tjsonGetStringValue2(pJson, "vnodeIdList", stateVnodeIdList, sizeof(stateVnodeIdList)) != TSDB_CODE_SUCCESS) {
    return false;
  }

  SRepairCtx stateCtx = {0};
  stateCtx.enabled = true;
  char vnodeIdBuf[PATH_MAX] = {0};
  tstrncpy(vnodeIdBuf, stateVnodeIdList, sizeof(vnodeIdBuf));
  if (tRepairParseVnodeIdList(vnodeIdBuf, &stateCtx) != TSDB_CODE_SUCCESS || stateCtx.vnodeIdNum != pCtx->vnodeIdNum) {
    return false;
  }

  for (int32_t i = 0; i < pCtx->vnodeIdNum; ++i) {
    if (pCtx->vnodeIds[i] != stateCtx.vnodeIds[i]) {
      return false;
    }
  }

  return true;
}

static bool tRepairStatusCanResume(const char *status) {
  if (status == NULL || status[0] == '\0') {
    return false;
  }

  return taosStrcasecmp(status, "initialized") == 0 || taosStrcasecmp(status, "running") == 0;
}

static bool tRepairBuildResumeCandidate(const SRepairCtx *pCtx, const char *sessionDirName, const char *sessionDir,
                                        const char *statePath, SRepairResumeCandidate *pCandidate) {
  if (pCtx == NULL || sessionDirName == NULL || sessionDirName[0] == '\0' || sessionDir == NULL ||
      sessionDir[0] == '\0' || statePath == NULL || statePath[0] == '\0' || pCandidate == NULL) {
    return false;
  }

  char   *stateContent = NULL;
  int64_t stateContentLen = 0;
  if (tRepairReadTextFile(statePath, &stateContent, &stateContentLen) != TSDB_CODE_SUCCESS || stateContentLen <= 0) {
    return false;
  }

  bool   matched = false;
  SJson *pJson = tjsonParse(stateContent);
  taosMemoryFree(stateContent);
  if (pJson == NULL) {
    return false;
  }

  do {
    char sessionId[REPAIR_SESSION_ID_LEN] = {0};
    char status[32] = {0};
    int64_t startTimeMs = 0;
    int32_t nodeTypeCode = 0;
    int32_t fileTypeCode = 0;
    int32_t modeCode = 0;
    int32_t doneVnodes = 0;
    int32_t totalVnodes = 0;

    if (tjsonGetStringValue2(pJson, "sessionId", sessionId, sizeof(sessionId)) != TSDB_CODE_SUCCESS ||
        tjsonGetBigIntValue(pJson, "startTimeMs", &startTimeMs) != TSDB_CODE_SUCCESS ||
        tjsonGetIntValue(pJson, "nodeTypeCode", &nodeTypeCode) != TSDB_CODE_SUCCESS ||
        tjsonGetIntValue(pJson, "fileTypeCode", &fileTypeCode) != TSDB_CODE_SUCCESS ||
        tjsonGetIntValue(pJson, "modeCode", &modeCode) != TSDB_CODE_SUCCESS ||
        tjsonGetStringValue2(pJson, "status", status, sizeof(status)) != TSDB_CODE_SUCCESS ||
        tjsonGetIntValue(pJson, "doneVnodes", &doneVnodes) != TSDB_CODE_SUCCESS ||
        tjsonGetIntValue(pJson, "totalVnodes", &totalVnodes) != TSDB_CODE_SUCCESS) {
      break;
    }

    if (startTimeMs <= 0 || doneVnodes < 0 || totalVnodes < 0 || doneVnodes > totalVnodes) {
      break;
    }
    if (strcmp(sessionId, sessionDirName) != 0) {
      break;
    }
    if (!tRepairStatusCanResume(status)) {
      break;
    }
    if (nodeTypeCode != (int32_t)pCtx->nodeType || fileTypeCode != (int32_t)pCtx->fileType ||
        modeCode != (int32_t)pCtx->mode) {
      break;
    }
    if (!tRepairMatchStateVnodeList(pCtx, pJson, totalVnodes)) {
      break;
    }
    if (!tRepairMatchOptionalStateField(pJson, "backupPath", pCtx->hasBackupPath, pCtx->backupPath)) {
      break;
    }
    if (!tRepairMatchOptionalStateField(pJson, "replicaNode", pCtx->hasReplicaNode, pCtx->replicaNode)) {
      break;
    }

    memset(pCandidate, 0, sizeof(*pCandidate));
    pCandidate->found = true;
    pCandidate->startTimeMs = startTimeMs;
    pCandidate->doneVnodes = doneVnodes;
    pCandidate->totalVnodes = totalVnodes;
    if (tRepairParseStringOption(sessionId, pCandidate->sessionId, sizeof(pCandidate->sessionId)) !=
            TSDB_CODE_SUCCESS ||
        tRepairParseStringOption(sessionDir, pCandidate->sessionDir, sizeof(pCandidate->sessionDir)) !=
            TSDB_CODE_SUCCESS ||
        tRepairBuildSessionFilePath(sessionDir, REPAIR_SESSION_LOG_NAME, pCandidate->logPath, sizeof(pCandidate->logPath)) !=
            TSDB_CODE_SUCCESS ||
        tRepairBuildSessionFilePath(sessionDir, REPAIR_SESSION_STATE_NAME, pCandidate->statePath,
                                    sizeof(pCandidate->statePath)) != TSDB_CODE_SUCCESS) {
      memset(pCandidate, 0, sizeof(*pCandidate));
      break;
    }

    matched = true;
  } while (0);

  tjsonDelete(pJson);
  return matched;
}

static int32_t tRepairWriteFileAtomically(const char *filePath, const char *content, int64_t contentLen) {
  if (filePath == NULL || filePath[0] == '\0' || content == NULL || contentLen < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  char tempPath[PATH_MAX] = {0};
  int32_t len = tsnprintf(tempPath, sizeof(tempPath), "%s.tmp", filePath);
  if (len <= 0 || len >= (int32_t)sizeof(tempPath)) {
    return TSDB_CODE_INVALID_PARA;
  }

  TdFilePtr pFile = taosOpenFile(tempPath, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    return terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  if (contentLen > 0) {
    int64_t written = taosWriteFile(pFile, content, contentLen);
    if (written != contentLen) {
      code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    int32_t syncCode = taosFsyncFile(pFile);
    if (syncCode != 0) {
      code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
    }
  }

  if (taosCloseFile(&pFile) != 0 && code == TSDB_CODE_SUCCESS) {
    code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  if (code != TSDB_CODE_SUCCESS) {
    (void)taosRemoveFile(tempPath);
    return code;
  }

  if (taosRenameFile(tempPath, filePath) != 0) {
    code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
    (void)taosRemoveFile(tempPath);
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tRepairWriteSessionStateInternal(const SRepairCtx *pCtx, const char *statePath, const char *step,
                                                const char *status, int32_t doneVnodes, int32_t totalVnodes) {
  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) {
    return terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  if (tjsonAddStringToObject(pJson, "sessionId", pCtx->sessionId) != TSDB_CODE_SUCCESS ||
      tjsonAddIntegerToObject(pJson, "startTimeMs", (uint64_t)pCtx->startTimeMs) != TSDB_CODE_SUCCESS ||
      tjsonAddStringToObject(pJson, "nodeType", tRepairGetNodeTypeName(pCtx->nodeType)) != TSDB_CODE_SUCCESS ||
      tjsonAddIntegerToObject(pJson, "nodeTypeCode", (uint64_t)pCtx->nodeType) != TSDB_CODE_SUCCESS ||
      tjsonAddStringToObject(pJson, "fileType", tRepairGetFileTypeName(pCtx->fileType)) != TSDB_CODE_SUCCESS ||
      tjsonAddIntegerToObject(pJson, "fileTypeCode", (uint64_t)pCtx->fileType) != TSDB_CODE_SUCCESS ||
      tjsonAddStringToObject(pJson, "mode", tRepairGetModeName(pCtx->mode)) != TSDB_CODE_SUCCESS ||
      tjsonAddIntegerToObject(pJson, "modeCode", (uint64_t)pCtx->mode) != TSDB_CODE_SUCCESS ||
      tjsonAddStringToObject(pJson, "step", step) != TSDB_CODE_SUCCESS ||
      tjsonAddStringToObject(pJson, "status", status) != TSDB_CODE_SUCCESS ||
      tjsonAddIntegerToObject(pJson, "doneVnodes", (uint64_t)doneVnodes) != TSDB_CODE_SUCCESS ||
      tjsonAddIntegerToObject(pJson, "totalVnodes", (uint64_t)totalVnodes) != TSDB_CODE_SUCCESS ||
      tjsonAddIntegerToObject(pJson, "updatedAtMs", (uint64_t)taosGetTimestampMs()) != TSDB_CODE_SUCCESS) {
    code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  if (code == TSDB_CODE_SUCCESS && pCtx->hasVnodeIdList) {
    if (tjsonAddStringToObject(pJson, "vnodeIdList", pCtx->vnodeIdList) != TSDB_CODE_SUCCESS) {
      code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
    }
  }
  if (code == TSDB_CODE_SUCCESS && pCtx->hasBackupPath) {
    if (tjsonAddStringToObject(pJson, "backupPath", pCtx->backupPath) != TSDB_CODE_SUCCESS) {
      code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
    }
  }
  if (code == TSDB_CODE_SUCCESS && pCtx->hasReplicaNode) {
    if (tjsonAddStringToObject(pJson, "replicaNode", pCtx->replicaNode) != TSDB_CODE_SUCCESS) {
      code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
    }
  }

  char *serialized = NULL;
  if (code == TSDB_CODE_SUCCESS) {
    serialized = tjsonToString(pJson);
    if (serialized == NULL) {
      code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    code = tRepairWriteFileAtomically(statePath, serialized, strlen(serialized));
  }

  if (serialized != NULL) {
    taosMemoryFree(serialized);
  }
  tjsonDelete(pJson);
  return code;
}

int32_t tRepairParseNodeType(const char *pNodeType, ERepairNodeType *pParsedNodeType) {
  if (pParsedNodeType == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t parsed = REPAIR_NODE_TYPE_INVALID;
  int32_t code = tRepairParseOption(pNodeType, kNodeTypeMap, (int32_t)tListLen(kNodeTypeMap), &parsed);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  *pParsedNodeType = (ERepairNodeType)parsed;
  return TSDB_CODE_SUCCESS;
}

int32_t tRepairParseFileType(const char *pFileType, ERepairFileType *pParsedFileType) {
  if (pParsedFileType == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t parsed = REPAIR_FILE_TYPE_INVALID;
  int32_t code = tRepairParseOption(pFileType, kFileTypeMap, (int32_t)tListLen(kFileTypeMap), &parsed);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  *pParsedFileType = (ERepairFileType)parsed;
  return TSDB_CODE_SUCCESS;
}

int32_t tRepairParseMode(const char *pMode, ERepairMode *pParsedMode) {
  if (pParsedMode == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t parsed = REPAIR_MODE_INVALID;
  int32_t code = tRepairParseOption(pMode, kModeMap, (int32_t)tListLen(kModeMap), &parsed);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  *pParsedMode = (ERepairMode)parsed;
  return TSDB_CODE_SUCCESS;
}

int32_t tRepairParseCliOption(SRepairCliArgs *pCliArgs, const char *pOptionName, const char *pOptionValue) {
  if (pCliArgs == NULL || pOptionName == NULL || pOptionValue == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (taosStrcasecmp(pOptionName, "node-type") == 0) {
    int32_t code = tRepairParseNodeType(pOptionValue, &pCliArgs->nodeType);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pCliArgs->hasNodeType = true;
    return TSDB_CODE_SUCCESS;
  }

  if (taosStrcasecmp(pOptionName, "file-type") == 0) {
    int32_t code = tRepairParseFileType(pOptionValue, &pCliArgs->fileType);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pCliArgs->hasFileType = true;
    return TSDB_CODE_SUCCESS;
  }

  if (taosStrcasecmp(pOptionName, "vnode-id") == 0) {
    int32_t code = tRepairParseStringOption(pOptionValue, pCliArgs->vnodeIdList, PATH_MAX);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pCliArgs->hasVnodeIdList = true;
    return TSDB_CODE_SUCCESS;
  }

  if (taosStrcasecmp(pOptionName, "backup-path") == 0) {
    int32_t code = tRepairParseStringOption(pOptionValue, pCliArgs->backupPath, PATH_MAX);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pCliArgs->hasBackupPath = true;
    return TSDB_CODE_SUCCESS;
  }

  if (taosStrcasecmp(pOptionName, "mode") == 0) {
    int32_t code = tRepairParseMode(pOptionValue, &pCliArgs->mode);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pCliArgs->hasMode = true;
    return TSDB_CODE_SUCCESS;
  }

  if (taosStrcasecmp(pOptionName, "replica-node") == 0) {
    int32_t code = tRepairParseStringOption(pOptionValue, pCliArgs->replicaNode, PATH_MAX);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pCliArgs->hasReplicaNode = true;
    return TSDB_CODE_SUCCESS;
  }

  return TSDB_CODE_INVALID_PARA;
}

int32_t tRepairValidateCliArgs(const SRepairCliArgs *pCliArgs) {
  if (pCliArgs == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (!pCliArgs->hasNodeType || !pCliArgs->hasFileType || !pCliArgs->hasMode) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (!tRepairIsFileTypeCompatible(pCliArgs->nodeType, pCliArgs->fileType)) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (pCliArgs->nodeType == REPAIR_NODE_TYPE_VNODE) {
    if (!pCliArgs->hasVnodeIdList) {
      return TSDB_CODE_INVALID_PARA;
    }
  } else if (pCliArgs->hasVnodeIdList) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (pCliArgs->mode == REPAIR_MODE_COPY) {
    if (!pCliArgs->hasReplicaNode) {
      return TSDB_CODE_INVALID_PARA;
    }
  } else if (pCliArgs->hasReplicaNode) {
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tRepairInitCtx(const SRepairCliArgs *pCliArgs, int64_t startTimeMs, SRepairCtx *pCtx) {
  if (pCliArgs == NULL || pCtx == NULL || startTimeMs <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = tRepairValidateCliArgs(pCliArgs);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  memset(pCtx, 0, sizeof(*pCtx));
  pCtx->enabled = true;
  pCtx->startTimeMs = startTimeMs;
  pCtx->nodeType = pCliArgs->nodeType;
  pCtx->fileType = pCliArgs->fileType;
  pCtx->mode = pCliArgs->mode;

  pCtx->hasVnodeIdList = pCliArgs->hasVnodeIdList;
  if (pCliArgs->hasVnodeIdList) {
    tstrncpy(pCtx->vnodeIdList, pCliArgs->vnodeIdList, sizeof(pCtx->vnodeIdList));
    char vnodeIdBuf[PATH_MAX] = {0};
    tstrncpy(vnodeIdBuf, pCtx->vnodeIdList, sizeof(vnodeIdBuf));
    code = tRepairParseVnodeIdList(vnodeIdBuf, pCtx);
    if (code != TSDB_CODE_SUCCESS) {
      memset(pCtx, 0, sizeof(*pCtx));
      return code;
    }
  }

  pCtx->hasBackupPath = pCliArgs->hasBackupPath;
  if (pCliArgs->hasBackupPath) {
    tstrncpy(pCtx->backupPath, pCliArgs->backupPath, sizeof(pCtx->backupPath));
  }

  pCtx->hasReplicaNode = pCliArgs->hasReplicaNode;
  if (pCliArgs->hasReplicaNode) {
    tstrncpy(pCtx->replicaNode, pCliArgs->replicaNode, sizeof(pCtx->replicaNode));
  }

  int32_t len = tsnprintf(pCtx->sessionId, sizeof(pCtx->sessionId), "repair-%" PRId64, startTimeMs);
  if (len <= 0 || len >= (int32_t)sizeof(pCtx->sessionId)) {
    memset(pCtx, 0, sizeof(*pCtx));
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tRepairShouldRepairVnode(const SRepairCtx *pCtx, int32_t vnodeId, bool *pShouldRepair) {
  if (pCtx == NULL || pShouldRepair == NULL || vnodeId < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (!pCtx->hasVnodeIdList || pCtx->vnodeIdNum <= 0) {
    *pShouldRepair = true;
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < pCtx->vnodeIdNum; ++i) {
    if (pCtx->vnodeIds[i] == vnodeId) {
      *pShouldRepair = true;
      return TSDB_CODE_SUCCESS;
    }
  }

  *pShouldRepair = false;
  return TSDB_CODE_SUCCESS;
}

int32_t tRepairNeedRunWalForceRepair(const SRepairCtx *pCtx, bool *pNeedRun) {
  if (pCtx == NULL || !pCtx->enabled || pNeedRun == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pNeedRun = pCtx->nodeType == REPAIR_NODE_TYPE_VNODE && pCtx->fileType == REPAIR_FILE_TYPE_WAL &&
              pCtx->mode == REPAIR_MODE_FORCE;
  return TSDB_CODE_SUCCESS;
}

int32_t tRepairNeedRunTsdbForceRepair(const SRepairCtx *pCtx, bool *pNeedRun) {
  if (pCtx == NULL || !pCtx->enabled || pNeedRun == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pNeedRun = pCtx->nodeType == REPAIR_NODE_TYPE_VNODE && pCtx->fileType == REPAIR_FILE_TYPE_TSDB &&
              pCtx->mode == REPAIR_MODE_FORCE;
  return TSDB_CODE_SUCCESS;
}

int32_t tRepairNeedRunMetaForceRepair(const SRepairCtx *pCtx, bool *pNeedRun) {
  if (pCtx == NULL || !pCtx->enabled || pNeedRun == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pNeedRun = pCtx->nodeType == REPAIR_NODE_TYPE_VNODE && pCtx->fileType == REPAIR_FILE_TYPE_META &&
              pCtx->mode == REPAIR_MODE_FORCE;
  return TSDB_CODE_SUCCESS;
}

int32_t tRepairBuildVnodeTargetPath(const char *dataDir, int32_t vnodeId, ERepairFileType fileType, char *targetPath,
                                    int32_t targetPathSize) {
  const char *subDir = tRepairGetVnodeFileSubDir(fileType);
  if (subDir == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  return tRepairBuildVnodePath(dataDir, vnodeId, subDir, targetPath, targetPathSize);
}

int32_t tRepairScanTsdbFiles(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId, SRepairTsdbScanResult *pResult) {
  if (pCtx == NULL || !pCtx->enabled || dataDir == NULL || dataDir[0] == '\0' || vnodeId < 0 || pResult == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  memset(pResult, 0, sizeof(*pResult));

  if (pCtx->nodeType != REPAIR_NODE_TYPE_VNODE || pCtx->fileType != REPAIR_FILE_TYPE_TSDB) {
    return TSDB_CODE_INVALID_PARA;
  }

  bool    shouldRepair = false;
  int32_t code = tRepairShouldRepairVnode(pCtx, vnodeId, &shouldRepair);
  if (code != TSDB_CODE_SUCCESS || !shouldRepair) {
    return TSDB_CODE_INVALID_PARA;
  }

  char targetPath[PATH_MAX] = {0};
  code = tRepairBuildVnodeTargetPath(dataDir, vnodeId, REPAIR_FILE_TYPE_TSDB, targetPath, sizeof(targetPath));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = tRepairScanTsdbDirRecursive(targetPath, pResult);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (pResult->headFiles <= 0 || pResult->dataFiles <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tRepairScanMetaFiles(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId, SRepairMetaScanResult *pResult) {
  if (pCtx == NULL || !pCtx->enabled || dataDir == NULL || dataDir[0] == '\0' || vnodeId < 0 || pResult == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  memset(pResult, 0, sizeof(*pResult));
  pResult->requiredFiles = (int32_t)tListLen(kMetaRequiredFiles);

  if (pCtx->nodeType != REPAIR_NODE_TYPE_VNODE || pCtx->fileType != REPAIR_FILE_TYPE_META) {
    return TSDB_CODE_INVALID_PARA;
  }

  bool    shouldRepair = false;
  int32_t code = tRepairShouldRepairVnode(pCtx, vnodeId, &shouldRepair);
  if (code != TSDB_CODE_SUCCESS || !shouldRepair) {
    return TSDB_CODE_INVALID_PARA;
  }

  char targetPath[PATH_MAX] = {0};
  code = tRepairBuildVnodeTargetPath(dataDir, vnodeId, REPAIR_FILE_TYPE_META, targetPath, sizeof(targetPath));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (!taosDirExist(targetPath) || !taosIsDir(targetPath)) {
    return TSDB_CODE_INVALID_PARA;
  }

  for (int32_t i = 0; i < (int32_t)tListLen(kMetaRequiredFiles); ++i) {
    char filePath[PATH_MAX] = {0};
    code = tRepairBuildPathWithEntry(targetPath, kMetaRequiredFiles[i], filePath, sizeof(filePath));
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (taosCheckExistFile(filePath)) {
      ++pResult->presentRequiredFiles;
    } else {
      tRepairRecordMissingMetaFile(pResult, kMetaRequiredFiles[i]);
    }
  }

  for (int32_t i = 0; i < (int32_t)tListLen(kMetaOptionalIndexFiles); ++i) {
    char filePath[PATH_MAX] = {0};
    code = tRepairBuildPathWithEntry(targetPath, kMetaOptionalIndexFiles[i], filePath, sizeof(filePath));
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (taosCheckExistFile(filePath)) {
      ++pResult->optionalIndexFiles;
    }
  }

  if (pResult->presentRequiredFiles < pResult->requiredFiles) {
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tRepairAnalyzeTsdbBlocks(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId,
                                 SRepairTsdbBlockReport *pReport) {
  if (pCtx == NULL || !pCtx->enabled || dataDir == NULL || dataDir[0] == '\0' || vnodeId < 0 || pReport == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  memset(pReport, 0, sizeof(*pReport));

  if (pCtx->nodeType != REPAIR_NODE_TYPE_VNODE || pCtx->fileType != REPAIR_FILE_TYPE_TSDB) {
    return TSDB_CODE_INVALID_PARA;
  }

  bool    shouldRepair = false;
  int32_t code = tRepairShouldRepairVnode(pCtx, vnodeId, &shouldRepair);
  if (code != TSDB_CODE_SUCCESS || !shouldRepair) {
    return TSDB_CODE_INVALID_PARA;
  }

  char targetPath[PATH_MAX] = {0};
  code = tRepairBuildVnodeTargetPath(dataDir, vnodeId, REPAIR_FILE_TYPE_TSDB, targetPath, sizeof(targetPath));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (!taosDirExist(targetPath) || !taosIsDir(targetPath)) {
    return TSDB_CODE_INVALID_PARA;
  }

  code = tRepairAnalyzeTsdbDirRecursive(targetPath, pReport);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (pReport->totalBlocks <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tRepairRebuildTsdbBlocks(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId, const char *outputDir,
                                 SRepairTsdbBlockReport *pReport) {
  if (pCtx == NULL || !pCtx->enabled || dataDir == NULL || dataDir[0] == '\0' || vnodeId < 0 || outputDir == NULL ||
      outputDir[0] == '\0' || pReport == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  memset(pReport, 0, sizeof(*pReport));

  if (pCtx->nodeType != REPAIR_NODE_TYPE_VNODE || pCtx->fileType != REPAIR_FILE_TYPE_TSDB) {
    return TSDB_CODE_INVALID_PARA;
  }

  bool    shouldRepair = false;
  int32_t code = tRepairShouldRepairVnode(pCtx, vnodeId, &shouldRepair);
  if (code != TSDB_CODE_SUCCESS || !shouldRepair) {
    return TSDB_CODE_INVALID_PARA;
  }

  char targetPath[PATH_MAX] = {0};
  code = tRepairBuildVnodeTargetPath(dataDir, vnodeId, REPAIR_FILE_TYPE_TSDB, targetPath, sizeof(targetPath));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (!taosDirExist(targetPath) || !taosIsDir(targetPath)) {
    return TSDB_CODE_INVALID_PARA;
  }

  code = tRepairResetDir(outputDir);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = tRepairRebuildTsdbDirRecursive(targetPath, outputDir, "", pReport);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (pReport->recoverableBlocks <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tRepairPrecheck(const SRepairCtx *pCtx, const char *dataDir, int64_t minDiskAvailBytes) {
  if (pCtx == NULL || !pCtx->enabled || dataDir == NULL || dataDir[0] == '\0' || minDiskAvailBytes < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (!taosDirExist(dataDir)) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (pCtx->hasBackupPath && !taosDirExist(pCtx->backupPath)) {
    return TSDB_CODE_INVALID_PARA;
  }

  char dataDirBuf[PATH_MAX] = {0};
  int32_t code = tRepairParseStringOption(dataDir, dataDirBuf, sizeof(dataDirBuf));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SDiskSize diskSize = {0};
  code = taosGetDiskSize(dataDirBuf, &diskSize);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (minDiskAvailBytes > 0 && diskSize.avail < minDiskAvailBytes) {
    return TSDB_CODE_NO_ENOUGH_DISKSPACE;
  }

  if (pCtx->nodeType != REPAIR_NODE_TYPE_VNODE) {
    return TSDB_CODE_SUCCESS;
  }

  if (pCtx->vnodeIdNum <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  for (int32_t i = 0; i < pCtx->vnodeIdNum; ++i) {
    char vnodeDir[PATH_MAX] = {0};
    code = tRepairBuildVnodePath(dataDir, pCtx->vnodeIds[i], NULL, vnodeDir, sizeof(vnodeDir));
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (!taosDirExist(vnodeDir)) {
      return TSDB_CODE_INVALID_PARA;
    }

    char targetPath[PATH_MAX] = {0};
    code = tRepairBuildVnodeTargetPath(dataDir, pCtx->vnodeIds[i], pCtx->fileType, targetPath, sizeof(targetPath));
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (!taosCheckExistFile(targetPath)) {
      return TSDB_CODE_INVALID_PARA;
    }

    if (pCtx->fileType == REPAIR_FILE_TYPE_TSDB) {
      SRepairTsdbScanResult scanResult = {0};
      code = tRepairScanTsdbFiles(pCtx, dataDir, pCtx->vnodeIds[i], &scanResult);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    } else if (pCtx->fileType == REPAIR_FILE_TYPE_META) {
      SRepairMetaScanResult scanResult = {0};
      code = tRepairScanMetaFiles(pCtx, dataDir, pCtx->vnodeIds[i], &scanResult);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tRepairPrepareBackupDir(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId, char *backupDir,
                                int32_t backupDirSize) {
  int32_t code = tRepairBuildBackupDir(pCtx, dataDir, vnodeId, backupDir, backupDirSize);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = taosMulMkDir(backupDir);
  if (code != 0) {
    return terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tRepairBackupVnodeTarget(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId, char *backupDir,
                                 int32_t backupDirSize) {
  if (backupDir != NULL && backupDirSize > 0) {
    backupDir[0] = '\0';
  }

  if (pCtx == NULL || !pCtx->enabled || dataDir == NULL || dataDir[0] == '\0' || vnodeId < 0 || backupDir == NULL ||
      backupDirSize <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  char targetPath[PATH_MAX] = {0};
  int32_t code = tRepairBuildVnodeTargetAndBackupPath(pCtx, dataDir, vnodeId, targetPath, sizeof(targetPath), backupDir,
                                                       backupDirSize);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (!taosDirExist(targetPath) || !taosIsDir(targetPath)) {
    return TSDB_CODE_INVALID_PARA;
  }

  code = tRepairResetDir(backupDir);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  return tRepairCopyDirRecursive(targetPath, backupDir);
}

int32_t tRepairRollbackVnodeTarget(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId) {
  if (pCtx == NULL || !pCtx->enabled || dataDir == NULL || dataDir[0] == '\0' || vnodeId < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  char targetPath[PATH_MAX] = {0};
  char backupDir[PATH_MAX] = {0};
  int32_t code = tRepairBuildVnodeTargetAndBackupPath(pCtx, dataDir, vnodeId, targetPath, sizeof(targetPath), backupDir,
                                                       sizeof(backupDir));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (!taosDirExist(backupDir) || !taosIsDir(backupDir)) {
    return TSDB_CODE_INVALID_PARA;
  }

  code = tRepairResetDir(targetPath);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  return tRepairCopyDirRecursive(backupDir, targetPath);
}

int32_t tRepairPrepareSessionFiles(const SRepairCtx *pCtx, const char *dataDir, char *sessionDir, int32_t sessionDirSize,
                                   char *logPath, int32_t logPathSize, char *statePath, int32_t statePathSize) {
  if (sessionDir != NULL && sessionDirSize > 0) {
    sessionDir[0] = '\0';
  }
  if (logPath != NULL && logPathSize > 0) {
    logPath[0] = '\0';
  }
  if (statePath != NULL && statePathSize > 0) {
    statePath[0] = '\0';
  }

  if (pCtx == NULL || !pCtx->enabled || dataDir == NULL || dataDir[0] == '\0' || sessionDir == NULL ||
      sessionDirSize <= 0 || logPath == NULL || logPathSize <= 0 || statePath == NULL || statePathSize <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = tRepairBuildSessionDir(pCtx, dataDir, sessionDir, sessionDirSize);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (taosMulMkDir(sessionDir) != 0) {
    return terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  code = tRepairBuildSessionFilePath(sessionDir, REPAIR_SESSION_LOG_NAME, logPath, logPathSize);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = tRepairBuildSessionFilePath(sessionDir, REPAIR_SESSION_STATE_NAME, statePath, statePathSize);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  TdFilePtr pLogFile = taosOpenFile(logPath, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  if (pLogFile == NULL) {
    return terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }
  if (taosCloseFile(&pLogFile) != 0) {
    return terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  int32_t totalVnodes = pCtx->nodeType == REPAIR_NODE_TYPE_VNODE ? pCtx->vnodeIdNum : 0;
  if (totalVnodes < 0) {
    totalVnodes = 0;
  }

  code = tRepairWriteSessionState(pCtx, statePath, "init", "initialized", 0, totalVnodes);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  return tRepairAppendSessionLog(logPath, "repair session initialized");
}

int32_t tRepairAppendSessionLog(const char *logPath, const char *message) {
  if (logPath == NULL || logPath[0] == '\0' || message == NULL || message[0] == '\0') {
    return TSDB_CODE_INVALID_PARA;
  }

  TdFilePtr pFile = taosOpenFile(logPath, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  if (pFile == NULL) {
    return terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  char    prefix[64] = {0};
  int32_t prefixLen = tsnprintf(prefix, sizeof(prefix), "[%" PRId64 "] ", taosGetTimestampMs());
  if (prefixLen <= 0 || prefixLen >= (int32_t)sizeof(prefix)) {
    code = TSDB_CODE_INVALID_PARA;
  }

  if (code == TSDB_CODE_SUCCESS) {
    int64_t written = taosWriteFile(pFile, prefix, prefixLen);
    if (written != prefixLen) {
      code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
    }
  }

  int32_t messageLen = strlen(message);
  if (code == TSDB_CODE_SUCCESS) {
    int64_t written = taosWriteFile(pFile, message, messageLen);
    if (written != messageLen) {
      code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    int64_t written = taosWriteFile(pFile, "\n", 1);
    if (written != 1) {
      code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
    }
  }

  if (code == TSDB_CODE_SUCCESS && taosFsyncFile(pFile) != 0) {
    code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  if (taosCloseFile(&pFile) != 0 && code == TSDB_CODE_SUCCESS) {
    code = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  return code;
}

int32_t tRepairWriteSessionState(const SRepairCtx *pCtx, const char *statePath, const char *step, const char *status,
                                 int32_t doneVnodes, int32_t totalVnodes) {
  if (pCtx == NULL || !pCtx->enabled || statePath == NULL || statePath[0] == '\0' || step == NULL || step[0] == '\0' ||
      status == NULL || status[0] == '\0' || doneVnodes < 0 || totalVnodes < 0 || doneVnodes > totalVnodes) {
    return TSDB_CODE_INVALID_PARA;
  }

  return tRepairWriteSessionStateInternal(pCtx, statePath, step, status, doneVnodes, totalVnodes);
}

int32_t tRepairTryResumeSession(SRepairCtx *pCtx, const char *dataDir, char *sessionDir, int32_t sessionDirSize,
                                char *logPath, int32_t logPathSize, char *statePath, int32_t statePathSize,
                                int32_t *pDoneVnodes, int32_t *pTotalVnodes, bool *pResumed) {
  if (sessionDir != NULL && sessionDirSize > 0) {
    sessionDir[0] = '\0';
  }
  if (logPath != NULL && logPathSize > 0) {
    logPath[0] = '\0';
  }
  if (statePath != NULL && statePathSize > 0) {
    statePath[0] = '\0';
  }

  if (pCtx == NULL || !pCtx->enabled || dataDir == NULL || dataDir[0] == '\0' || sessionDir == NULL ||
      sessionDirSize <= 0 || logPath == NULL || logPathSize <= 0 || statePath == NULL || statePathSize <= 0 ||
      pDoneVnodes == NULL || pTotalVnodes == NULL || pResumed == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pDoneVnodes = 0;
  *pTotalVnodes = pCtx->nodeType == REPAIR_NODE_TYPE_VNODE ? pCtx->vnodeIdNum : 0;
  *pResumed = false;

  char backupBaseDir[PATH_MAX] = {0};
  int32_t code = tRepairBuildBackupBaseDir(pCtx, dataDir, backupBaseDir, sizeof(backupBaseDir));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (!taosDirExist(backupBaseDir)) {
    return TSDB_CODE_SUCCESS;
  }

  TdDirPtr pDir = taosOpenDir(backupBaseDir);
  if (pDir == NULL) {
    return terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  SRepairResumeCandidate bestCandidate = {0};
  TdDirEntryPtr          pDirEntry = NULL;
  while ((pDirEntry = taosReadDir(pDir)) != NULL) {
    if (!taosDirEntryIsDir(pDirEntry)) {
      continue;
    }

    char *entryName = taosGetDirEntryName(pDirEntry);
    if (entryName == NULL || strcmp(entryName, ".") == 0 || strcmp(entryName, "..") == 0) {
      continue;
    }
    if (strncmp(entryName, REPAIR_SESSION_DIR_PREFIX, strlen(REPAIR_SESSION_DIR_PREFIX)) != 0) {
      continue;
    }

    char sessionPath[PATH_MAX] = {0};
    int32_t pathLen = tsnprintf(sessionPath, sizeof(sessionPath), "%s%s%s", backupBaseDir, TD_DIRSEP, entryName);
    if (pathLen <= 0 || pathLen >= (int32_t)sizeof(sessionPath)) {
      continue;
    }

    char stateFilePath[PATH_MAX] = {0};
    if (tRepairBuildSessionFilePath(sessionPath, REPAIR_SESSION_STATE_NAME, stateFilePath, sizeof(stateFilePath)) !=
        TSDB_CODE_SUCCESS) {
      continue;
    }
    if (!taosCheckExistFile(stateFilePath)) {
      continue;
    }

    SRepairResumeCandidate candidate = {0};
    if (!tRepairBuildResumeCandidate(pCtx, entryName, sessionPath, stateFilePath, &candidate)) {
      continue;
    }

    if (!bestCandidate.found || candidate.startTimeMs > bestCandidate.startTimeMs) {
      bestCandidate = candidate;
    }
  }

  if (taosCloseDir(&pDir) != 0) {
    return terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
  }

  if (!bestCandidate.found) {
    return TSDB_CODE_SUCCESS;
  }

  if (tRepairParseStringOption(bestCandidate.sessionId, pCtx->sessionId, sizeof(pCtx->sessionId)) !=
      TSDB_CODE_SUCCESS) {
    return TSDB_CODE_INVALID_PARA;
  }
  pCtx->startTimeMs = bestCandidate.startTimeMs;

  tstrncpy(sessionDir, bestCandidate.sessionDir, sessionDirSize);
  tstrncpy(logPath, bestCandidate.logPath, logPathSize);
  tstrncpy(statePath, bestCandidate.statePath, statePathSize);
  *pDoneVnodes = bestCandidate.doneVnodes;
  *pTotalVnodes = bestCandidate.totalVnodes;
  *pResumed = true;
  return TSDB_CODE_SUCCESS;
}

int32_t tRepairNeedReportProgress(int64_t nowMs, int64_t intervalMs, int64_t *pLastReportMs, bool *pNeedReport) {
  if (nowMs < 0 || intervalMs <= 0 || pLastReportMs == NULL || pNeedReport == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (*pLastReportMs <= 0 || nowMs < *pLastReportMs || (nowMs - *pLastReportMs) >= intervalMs) {
    *pNeedReport = true;
    *pLastReportMs = nowMs;
    return TSDB_CODE_SUCCESS;
  }

  *pNeedReport = false;
  return TSDB_CODE_SUCCESS;
}

int32_t tRepairBuildProgressLine(const SRepairCtx *pCtx, const char *step, int32_t doneVnodes, int32_t totalVnodes,
                                 char *line, int32_t lineSize) {
  if (pCtx == NULL || !pCtx->enabled || step == NULL || step[0] == '\0' || line == NULL || lineSize <= 0 ||
      doneVnodes < 0 || totalVnodes < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (totalVnodes > 0 && doneVnodes > totalVnodes) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (totalVnodes == 0 && doneVnodes != 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t progress = totalVnodes > 0 ? (doneVnodes * 100) / totalVnodes : 100;
  int32_t len = tsnprintf(line, lineSize, "repair progress: session=%s step=%s vnode=%d/%d progress=%d%%",
                          pCtx->sessionId, step, doneVnodes, totalVnodes, progress);
  if (len <= 0 || len >= lineSize) {
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tRepairBuildSummaryLine(const SRepairCtx *pCtx, int32_t successVnodes, int32_t failedVnodes, int64_t elapsedMs,
                                char *line, int32_t lineSize) {
  if (pCtx == NULL || !pCtx->enabled || successVnodes < 0 || failedVnodes < 0 || elapsedMs < 0 || line == NULL ||
      lineSize <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  const char *status = "success";
  if (failedVnodes > 0 && successVnodes > 0) {
    status = "partial";
  } else if (failedVnodes > 0) {
    status = "failed";
  }

  int32_t len =
      tsnprintf(line, lineSize, "repair summary: session=%s status=%s successVnodes=%d failedVnodes=%d elapsedMs=%" PRId64,
                pCtx->sessionId, status, successVnodes, failedVnodes, elapsedMs);
  if (len <= 0 || len >= lineSize) {
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}
