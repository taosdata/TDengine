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

const char* syncRoleToStr(ESyncRole role) {
  switch (role) {
    case TAOS_SYNC_ROLE_VOTER:
      return "true";
    case TAOS_SYNC_ROLE_LEARNER:
      return "false";
    default:
      return "unknown";
  }
}

const ESyncRole syncStrToRole(char* str) {
  if(strcmp(str, "true") == 0){
    return TAOS_SYNC_ROLE_VOTER;
  }
  if(strcmp(str, "false") == 0){
    return TAOS_SYNC_ROLE_LEARNER;
  }

  return TAOS_SYNC_ROLE_ERROR;
}

static int32_t syncEncodeSyncCfg(const void *pObj, SJson *pJson) {
  SSyncCfg *pCfg = (SSyncCfg *)pObj;
  if (tjsonAddDoubleToObject(pJson, "replicaNum", pCfg->replicaNum) < 0) return -1;
  if (tjsonAddDoubleToObject(pJson, "myIndex", pCfg->myIndex) < 0) return -1;
  if (tjsonAddDoubleToObject(pJson, "changeVersion", pCfg->changeVersion) < 0) return -1;

  SJson *nodeInfo = tjsonCreateArray();
  if (nodeInfo == NULL) return -1;
  if (tjsonAddItemToObject(pJson, "nodeInfo", nodeInfo) < 0) return -1;
  for (int32_t i = 0; i < pCfg->totalReplicaNum; ++i) {
    SJson *info = tjsonCreateObject();
    if (info == NULL) return -1;
    if (tjsonAddDoubleToObject(info, "nodePort", pCfg->nodeInfo[i].nodePort) < 0) return -1;
    if (tjsonAddStringToObject(info, "nodeFqdn", pCfg->nodeInfo[i].nodeFqdn) < 0) return -1;
    if (tjsonAddIntegerToObject(info, "nodeId", pCfg->nodeInfo[i].nodeId) < 0) return -1;
    if (tjsonAddIntegerToObject(info, "clusterId", pCfg->nodeInfo[i].clusterId) < 0) return -1;
    if (tjsonAddStringToObject(info, "isReplica", syncRoleToStr(pCfg->nodeInfo[i].nodeRole)) < 0) return -1;
    if (tjsonAddItemToArray(nodeInfo, info) < 0) return -1;
  }

  return 0;
}

static int32_t syncEncodeRaftCfg(const void *pObj, SJson *pJson) {
  SRaftCfg *pCfg = (SRaftCfg *)pObj;
  if (tjsonAddObject(pJson, "SSyncCfg", syncEncodeSyncCfg, (void *)&pCfg->cfg) < 0) return -1;
  if (tjsonAddDoubleToObject(pJson, "isStandBy", pCfg->isStandBy) < 0) return -1;
  if (tjsonAddDoubleToObject(pJson, "snapshotStrategy", pCfg->snapshotStrategy) < 0) return -1;
  if (tjsonAddDoubleToObject(pJson, "batchSize", pCfg->batchSize) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "lastConfigIndex", pCfg->lastConfigIndex) < 0) return -1;
  if (tjsonAddDoubleToObject(pJson, "configIndexCount", pCfg->configIndexCount) < 0) return -1;

  SJson *configIndexArr = tjsonCreateArray();
  if (configIndexArr == NULL) return -1;
  if (tjsonAddItemToObject(pJson, "configIndexArr", configIndexArr) < 0) return -1;
  for (int32_t i = 0; i < pCfg->configIndexCount; ++i) {
    SJson *configIndex = tjsonCreateObject();
    if (configIndex == NULL) return -1;
    if (tjsonAddIntegerToObject(configIndex, "index", pCfg->configIndexArr[i]) < 0) return -1;
    if (tjsonAddItemToArray(configIndexArr, configIndex) < 0) return -1;
  }

  return 0;
}

int32_t syncWriteCfgFile(SSyncNode *pNode) {
  int32_t     code = -1;
  char       *buffer = NULL;
  SJson      *pJson = NULL;
  TdFilePtr   pFile = NULL;
  const char *realfile = pNode->configPath;
  SRaftCfg   *pCfg = &pNode->raftCfg;
  char        file[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s.bak", realfile);

  terrno = TSDB_CODE_OUT_OF_MEMORY;
  pJson = tjsonCreateObject();
  if (pJson == NULL) goto _OVER;
  if (tjsonAddObject(pJson, "RaftCfg", syncEncodeRaftCfg, pCfg) < 0) goto _OVER;
  buffer = tjsonToString(pJson);
  if (buffer == NULL) goto _OVER;
  terrno = 0;

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) goto _OVER;

  int32_t len = strlen(buffer);
  if (taosWriteFile(pFile, buffer, len) <= 0) goto _OVER;
  if (taosFsyncFile(pFile) < 0) goto _OVER;

  taosCloseFile(&pFile);
  if (taosRenameFile(file, realfile) != 0) goto _OVER;

  code = 0;
  sInfo("vgId:%d, succeed to write sync cfg file:%s, len:%d, lastConfigIndex:%" PRId64 ", "
        "changeVersion:%d", pNode->vgId, 
        realfile, len, pNode->raftCfg.lastConfigIndex, pNode->raftCfg.cfg.changeVersion);

_OVER:
  if (pJson != NULL) tjsonDelete(pJson);
  if (buffer != NULL) taosMemoryFree(buffer);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    if (terrno == 0) terrno = TAOS_SYSTEM_ERROR(errno);
    sError("vgId:%d, failed to write sync cfg file:%s since %s", pNode->vgId, realfile, terrstr());
  }
  return code;
}

static int32_t syncDecodeSyncCfg(const SJson *pJson, void *pObj) {
  SSyncCfg *pCfg = (SSyncCfg *)pObj;
  int32_t   code = 0;

  tjsonGetInt32ValueFromDouble(pJson, "replicaNum", pCfg->replicaNum, code);
  if (code < 0) return -1;
  tjsonGetInt32ValueFromDouble(pJson, "myIndex", pCfg->myIndex, code);
  if (code < 0) return -1;
  tjsonGetInt32ValueFromDouble(pJson, "changeVersion", pCfg->changeVersion, code);
  if (code < 0) return -1;

  SJson *nodeInfo = tjsonGetObjectItem(pJson, "nodeInfo");
  if (nodeInfo == NULL) return -1;
  pCfg->totalReplicaNum = tjsonGetArraySize(nodeInfo);

  for (int32_t i = 0; i < pCfg->totalReplicaNum; ++i) {
    SJson *info = tjsonGetArrayItem(nodeInfo, i);
    if (info == NULL) return -1;
    tjsonGetUInt16ValueFromDouble(info, "nodePort", pCfg->nodeInfo[i].nodePort, code);
    if (code < 0) return -1;
    code = tjsonGetStringValue(info, "nodeFqdn", pCfg->nodeInfo[i].nodeFqdn);
    if (code < 0) return -1;
    tjsonGetNumberValue(info, "nodeId", pCfg->nodeInfo[i].nodeId, code);
    tjsonGetNumberValue(info, "clusterId", pCfg->nodeInfo[i].clusterId, code);
    char role[10] = {0};
    code = tjsonGetStringValue(info, "isReplica", role);
    if(code < 0) return -1;
    if(strlen(role) != 0){
      pCfg->nodeInfo[i].nodeRole = syncStrToRole(role);
    }
    else{
      pCfg->nodeInfo[i].nodeRole = TAOS_SYNC_ROLE_VOTER;
    }
  }

  return 0;
}

static int32_t syncDecodeRaftCfg(const SJson *pJson, void *pObj) {
  SRaftCfg *pCfg = (SRaftCfg *)pObj;
  int32_t   code = 0;

  if (tjsonToObject(pJson, "SSyncCfg", syncDecodeSyncCfg, (void *)&pCfg->cfg) < 0) return -1;

  tjsonGetInt8ValueFromDouble(pJson, "isStandBy", pCfg->isStandBy, code);
  if (code < 0) return -1;
  tjsonGetInt8ValueFromDouble(pJson, "snapshotStrategy", pCfg->snapshotStrategy, code);
  if (code < 0) return -1;
  tjsonGetInt32ValueFromDouble(pJson, "batchSize", pCfg->batchSize, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "lastConfigIndex", pCfg->lastConfigIndex, code);
  if (code < 0) return -1;
  tjsonGetInt32ValueFromDouble(pJson, "configIndexCount", pCfg->configIndexCount, code);

  SJson *configIndexArr = tjsonGetObjectItem(pJson, "configIndexArr");
  if (configIndexArr == NULL) return -1;

  pCfg->configIndexCount = tjsonGetArraySize(configIndexArr);
  for (int32_t i = 0; i < pCfg->configIndexCount; ++i) {
    SJson *configIndex = tjsonGetArrayItem(configIndexArr, i);
    if (configIndex == NULL) return -1;
    tjsonGetNumberValue(configIndex, "index", pCfg->configIndexArr[i], code);
    if (code < 0) return -1;
  }

  return 0;
}

int32_t syncReadCfgFile(SSyncNode *pNode) {
  int32_t     code = -1;
  TdFilePtr   pFile = NULL;
  char       *pData = NULL;
  SJson      *pJson = NULL;
  const char *file = pNode->configPath;
  SRaftCfg   *pCfg = &pNode->raftCfg;

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    sError("vgId:%d, failed to open sync cfg file:%s since %s", pNode->vgId, file, terrstr());
    goto _OVER;
  }

  int64_t size = 0;
  if (taosFStatFile(pFile, &size, NULL) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    sError("vgId:%d, failed to fstat sync cfg file:%s since %s", pNode->vgId, file, terrstr());
    goto _OVER;
  }

  pData = taosMemoryMalloc(size + 1);
  if (pData == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  if (taosReadFile(pFile, pData, size) != size) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    sError("vgId:%d, failed to read sync cfg file:%s since %s", pNode->vgId, file, terrstr());
    goto _OVER;
  }

  pData[size] = '\0';

  pJson = tjsonParse(pData);
  if (pJson == NULL) {
    terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  if (tjsonToObject(pJson, "RaftCfg", syncDecodeRaftCfg, (void *)pCfg) < 0) {
    terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  code = 0;
  sInfo("vgId:%d, succceed to read sync cfg file %s, changeVersion:%d", 
    pNode->vgId, file, pCfg->cfg.changeVersion);

_OVER:
  if (pData != NULL) taosMemoryFree(pData);
  if (pJson != NULL) cJSON_Delete(pJson);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    sError("vgId:%d, failed to read sync cfg file:%s since %s", pNode->vgId, file, terrstr());
  }
  return code;
}

int32_t syncAddCfgIndex(SSyncNode *pNode, SyncIndex cfgIndex) {
  SRaftCfg *pCfg = &pNode->raftCfg;
  if (pCfg->configIndexCount < MAX_CONFIG_INDEX_COUNT) {
    return -1;
  }

  pCfg->configIndexArr[pCfg->configIndexCount] = cfgIndex;
  pCfg->configIndexCount++;
  return 0;
}