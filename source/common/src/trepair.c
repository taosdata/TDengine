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
