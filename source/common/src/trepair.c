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

  const char *subDir = tRepairGetVnodeFileSubDir(pCtx->fileType);
  if (subDir == NULL) {
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
    code = tRepairBuildVnodePath(dataDir, pCtx->vnodeIds[i], subDir, targetPath, sizeof(targetPath));
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (!taosCheckExistFile(targetPath)) {
      return TSDB_CODE_INVALID_PARA;
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

  code = tRepairBuildSessionFilePath(sessionDir, "repair.log", logPath, logPathSize);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = tRepairBuildSessionFilePath(sessionDir, "repair.state.json", statePath, statePathSize);
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
