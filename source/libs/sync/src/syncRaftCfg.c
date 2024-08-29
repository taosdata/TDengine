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
#include "syncRaftCfg.h"
#include "syncUtil.h"
#include "tjson.h"

static const char *syncRoleToStr(ESyncRole role) {
  switch (role) {
    case TAOS_SYNC_ROLE_VOTER:
      return "true";
    case TAOS_SYNC_ROLE_LEARNER:
      return "false";
    default:
      return "unknown";
  }
}

static const ESyncRole syncStrToRole(char *str) {
  if (strcmp(str, "true") == 0) {
    return TAOS_SYNC_ROLE_VOTER;
  } else if (strcmp(str, "false") == 0) {
    return TAOS_SYNC_ROLE_LEARNER;
  } else {
    return TAOS_SYNC_ROLE_ERROR;
  }
}

static int32_t syncEncodeSyncCfg(const void *pObj, SJson *pJson) {
  SSyncCfg *pCfg = (SSyncCfg *)pObj;
  int32_t   code = 0, lino = 0;

  TAOS_CHECK_EXIT(tjsonAddDoubleToObject(pJson, "replicaNum", pCfg->replicaNum));
  TAOS_CHECK_EXIT(tjsonAddDoubleToObject(pJson, "myIndex", pCfg->myIndex));
  TAOS_CHECK_EXIT(tjsonAddDoubleToObject(pJson, "changeVersion", pCfg->changeVersion));

  SJson *nodeInfo = tjsonCreateArray();
  if (nodeInfo == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  if ((code = tjsonAddItemToObject(pJson, "nodeInfo", nodeInfo)) < 0) {
    tjsonDelete(nodeInfo);
    TAOS_CHECK_EXIT(code);
  }

  for (int32_t i = 0; i < pCfg->totalReplicaNum; ++i) {
    SJson *info = tjsonCreateObject();
    if (info == NULL) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    TAOS_CHECK_GOTO(tjsonAddDoubleToObject(info, "nodePort", pCfg->nodeInfo[i].nodePort), NULL, _err);
    TAOS_CHECK_GOTO(tjsonAddStringToObject(info, "nodeFqdn", pCfg->nodeInfo[i].nodeFqdn), NULL, _err);
    TAOS_CHECK_GOTO(tjsonAddIntegerToObject(info, "nodeId", pCfg->nodeInfo[i].nodeId), NULL, _err);
    TAOS_CHECK_GOTO(tjsonAddIntegerToObject(info, "clusterId", pCfg->nodeInfo[i].clusterId), NULL, _err);
    TAOS_CHECK_GOTO(tjsonAddStringToObject(info, "isReplica", syncRoleToStr(pCfg->nodeInfo[i].nodeRole)), NULL, _err);
    TAOS_CHECK_GOTO(tjsonAddItemToArray(nodeInfo, info), NULL, _err);
    continue;

  _err:
    tjsonDelete(info);
    break;
  }

_exit:
  if (code < 0) {
    sError("failed to encode sync cfg at line %d since %s", lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}

static int32_t syncEncodeRaftCfg(const void *pObj, SJson *pJson) {
  SRaftCfg *pCfg = (SRaftCfg *)pObj;
  int32_t   code = 0;
  int32_t   lino = 0;

  TAOS_CHECK_EXIT(tjsonAddObject(pJson, "SSyncCfg", syncEncodeSyncCfg, (void *)&pCfg->cfg));
  TAOS_CHECK_EXIT(tjsonAddDoubleToObject(pJson, "isStandBy", pCfg->isStandBy));
  TAOS_CHECK_EXIT(tjsonAddDoubleToObject(pJson, "snapshotStrategy", pCfg->snapshotStrategy));
  TAOS_CHECK_EXIT(tjsonAddDoubleToObject(pJson, "batchSize", pCfg->batchSize));
  TAOS_CHECK_EXIT(tjsonAddIntegerToObject(pJson, "lastConfigIndex", pCfg->lastConfigIndex));
  TAOS_CHECK_EXIT(tjsonAddDoubleToObject(pJson, "configIndexCount", pCfg->configIndexCount));

  SJson *configIndexArr = tjsonCreateArray();
  if (configIndexArr == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  if ((code = tjsonAddItemToObject(pJson, "configIndexArr", configIndexArr)) < 0) {
    tjsonDelete(configIndexArr);
    TAOS_CHECK_EXIT(code);
  }

  for (int32_t i = 0; i < pCfg->configIndexCount; ++i) {
    SJson *configIndex = tjsonCreateObject();
    if (configIndex == NULL) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    TAOS_CHECK_EXIT(tjsonAddIntegerToObject(configIndex, "index", pCfg->configIndexArr[i]));
    TAOS_CHECK_EXIT(tjsonAddItemToArray(configIndexArr, configIndex));
    continue;

  _err:
    tjsonDelete(configIndex);
    break;
  }

_exit:
  if (code < 0) {
    sError("failed to encode raft cfg at line %d since %s", lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}

int32_t syncWriteCfgFile(SSyncNode *pNode) {
  int32_t     code = 0, lino = 0;
  char       *buffer = NULL;
  SJson      *pJson = NULL;
  TdFilePtr   pFile = NULL;
  const char *realfile = pNode->configPath;
  SRaftCfg   *pCfg = &pNode->raftCfg;
  char        file[PATH_MAX] = {0};

  (void)snprintf(file, sizeof(file), "%s.bak", realfile);

  if ((pJson = tjsonCreateObject()) == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  TAOS_CHECK_EXIT(tjsonAddObject(pJson, "RaftCfg", syncEncodeRaftCfg, pCfg));
  buffer = tjsonToString(pJson);
  if (buffer == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) {
    code = terrno ? terrno : TAOS_SYSTEM_ERROR(errno);
    TAOS_CHECK_EXIT(code);
  }

  int32_t len = strlen(buffer);
  if (taosWriteFile(pFile, buffer, len) <= 0) {
    TAOS_CHECK_EXIT(TAOS_SYSTEM_ERROR(errno));
  }

  if (taosFsyncFile(pFile) < 0) {
    TAOS_CHECK_EXIT(TAOS_SYSTEM_ERROR(errno));
  }

  (void)taosCloseFile(&pFile);
  if (taosRenameFile(file, realfile) != 0) {
    TAOS_CHECK_EXIT(TAOS_SYSTEM_ERROR(errno));
  }

  sInfo("vgId:%d, succeed to write sync cfg file:%s, len:%d, lastConfigIndex:%" PRId64 ", changeVersion:%d",
        pNode->vgId, realfile, len, pNode->raftCfg.lastConfigIndex, pNode->raftCfg.cfg.changeVersion);

_exit:
  if (pJson != NULL) tjsonDelete(pJson);
  if (buffer != NULL) taosMemoryFree(buffer);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    sError("vgId:%d, failed to write sync cfg file:%s since %s", pNode->vgId, realfile, tstrerror(code));
  }

  TAOS_RETURN(code);
}

static int32_t syncDecodeSyncCfg(const SJson *pJson, void *pObj) {
  SSyncCfg *pCfg = (SSyncCfg *)pObj;
  int32_t   code = 0;

  tjsonGetInt32ValueFromDouble(pJson, "replicaNum", pCfg->replicaNum, code);
  if (code < 0) return TSDB_CODE_INVALID_JSON_FORMAT;
  tjsonGetInt32ValueFromDouble(pJson, "myIndex", pCfg->myIndex, code);
  if (code < 0) return TSDB_CODE_INVALID_JSON_FORMAT;
  tjsonGetInt32ValueFromDouble(pJson, "changeVersion", pCfg->changeVersion, code);
  if (code < 0) return TSDB_CODE_INVALID_JSON_FORMAT;

  SJson *nodeInfo = tjsonGetObjectItem(pJson, "nodeInfo");
  if (nodeInfo == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;
  pCfg->totalReplicaNum = tjsonGetArraySize(nodeInfo);

  for (int32_t i = 0; i < pCfg->totalReplicaNum; ++i) {
    SJson *info = tjsonGetArrayItem(nodeInfo, i);
    if (info == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;
    tjsonGetUInt16ValueFromDouble(info, "nodePort", pCfg->nodeInfo[i].nodePort, code);
    if (code < 0) return TSDB_CODE_INVALID_JSON_FORMAT;
    code = tjsonGetStringValue(info, "nodeFqdn", pCfg->nodeInfo[i].nodeFqdn);
    if (code < 0) return TSDB_CODE_INVALID_JSON_FORMAT;
    tjsonGetNumberValue(info, "nodeId", pCfg->nodeInfo[i].nodeId, code);
    tjsonGetNumberValue(info, "clusterId", pCfg->nodeInfo[i].clusterId, code);
    char role[10] = {0};
    code = tjsonGetStringValue(info, "isReplica", role);
    if (code < 0) return TSDB_CODE_INVALID_JSON_FORMAT;
    if (strlen(role) != 0) {
      pCfg->nodeInfo[i].nodeRole = syncStrToRole(role);
    } else {
      pCfg->nodeInfo[i].nodeRole = TAOS_SYNC_ROLE_VOTER;
    }
  }

  return 0;
}

static int32_t syncDecodeRaftCfg(const SJson *pJson, void *pObj) {
  SRaftCfg *pCfg = (SRaftCfg *)pObj;
  int32_t   code = 0;

  TAOS_CHECK_RETURN(tjsonToObject(pJson, "SSyncCfg", syncDecodeSyncCfg, (void *)&pCfg->cfg));

  tjsonGetInt8ValueFromDouble(pJson, "isStandBy", pCfg->isStandBy, code);
  if (code < 0) return TSDB_CODE_INVALID_JSON_FORMAT;
  tjsonGetInt8ValueFromDouble(pJson, "snapshotStrategy", pCfg->snapshotStrategy, code);
  if (code < 0) return TSDB_CODE_INVALID_JSON_FORMAT;
  tjsonGetInt32ValueFromDouble(pJson, "batchSize", pCfg->batchSize, code);
  if (code < 0) return TSDB_CODE_INVALID_JSON_FORMAT;
  tjsonGetNumberValue(pJson, "lastConfigIndex", pCfg->lastConfigIndex, code);
  if (code < 0) return TSDB_CODE_INVALID_JSON_FORMAT;
  tjsonGetInt32ValueFromDouble(pJson, "configIndexCount", pCfg->configIndexCount, code);
  if (code < 0) return TSDB_CODE_INVALID_JSON_FORMAT;

  SJson *configIndexArr = tjsonGetObjectItem(pJson, "configIndexArr");
  if (configIndexArr == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;

  pCfg->configIndexCount = tjsonGetArraySize(configIndexArr);
  for (int32_t i = 0; i < pCfg->configIndexCount; ++i) {
    SJson *configIndex = tjsonGetArrayItem(configIndexArr, i);
    if (configIndex == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;
    tjsonGetNumberValue(configIndex, "index", pCfg->configIndexArr[i], code);
    if (code < 0) return TSDB_CODE_INVALID_JSON_FORMAT;
  }

  return 0;
}

int32_t syncReadCfgFile(SSyncNode *pNode) {
  int32_t     code = 0;
  TdFilePtr   pFile = NULL;
  char       *pData = NULL;
  SJson      *pJson = NULL;
  const char *file = pNode->configPath;
  SRaftCfg   *pCfg = &pNode->raftCfg;

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    sError("vgId:%d, failed to open sync cfg file:%s since %s", pNode->vgId, file, tstrerror(code));
    goto _OVER;
  }

  int64_t size = 0;
  if (taosFStatFile(pFile, &size, NULL) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    sError("vgId:%d, failed to fstat sync cfg file:%s since %s", pNode->vgId, file, tstrerror(code));
    goto _OVER;
  }

  pData = taosMemoryMalloc(size + 1);
  if (pData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  if (taosReadFile(pFile, pData, size) != size) {
    code = TAOS_SYSTEM_ERROR(errno);
    sError("vgId:%d, failed to read sync cfg file:%s since %s", pNode->vgId, file, tstrerror(code));
    goto _OVER;
  }

  pData[size] = '\0';

  pJson = tjsonParse(pData);
  if (pJson == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  if (tjsonToObject(pJson, "RaftCfg", syncDecodeRaftCfg, (void *)pCfg) < 0) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  sInfo("vgId:%d, succceed to read sync cfg file %s, changeVersion:%d", pNode->vgId, file, pCfg->cfg.changeVersion);

_OVER:
  if (pData != NULL) taosMemoryFree(pData);
  if (pJson != NULL) cJSON_Delete(pJson);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    sError("vgId:%d, failed to read sync cfg file:%s since %s", pNode->vgId, file, tstrerror(code));
  }

  TAOS_RETURN(code);
}
