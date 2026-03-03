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
