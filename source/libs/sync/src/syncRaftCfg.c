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

// file must already exist!
SRaftCfgIndex *raftCfgIndexOpen(const char *path) {
  SRaftCfgIndex *pRaftCfgIndex = taosMemoryMalloc(sizeof(SRaftCfg));
  snprintf(pRaftCfgIndex->path, sizeof(pRaftCfgIndex->path), "%s", path);

  pRaftCfgIndex->pFile = taosOpenFile(pRaftCfgIndex->path, TD_FILE_READ | TD_FILE_WRITE);
  tAssert(pRaftCfgIndex->pFile != NULL);

  taosLSeekFile(pRaftCfgIndex->pFile, 0, SEEK_SET);

  int32_t bufLen = MAX_CONFIG_INDEX_COUNT * 16;
  char   *pBuf = taosMemoryMalloc(bufLen);
  memset(pBuf, 0, bufLen);
  int64_t len = taosReadFile(pRaftCfgIndex->pFile, pBuf, bufLen);
  tAssert(len > 0);

  int32_t ret = raftCfgIndexFromStr(pBuf, pRaftCfgIndex);
  tAssert(ret == 0);

  taosMemoryFree(pBuf);

  return pRaftCfgIndex;
}

int32_t raftCfgIndexClose(SRaftCfgIndex *pRaftCfgIndex) {
  if (pRaftCfgIndex != NULL) {
    int64_t ret = taosCloseFile(&(pRaftCfgIndex->pFile));
    tAssert(ret == 0);
    taosMemoryFree(pRaftCfgIndex);
  }
  return 0;
}

int32_t raftCfgIndexPersist(SRaftCfgIndex *pRaftCfgIndex) {
  tAssert(pRaftCfgIndex != NULL);

  char *s = raftCfgIndex2Str(pRaftCfgIndex);
  taosLSeekFile(pRaftCfgIndex->pFile, 0, SEEK_SET);

  int64_t ret = taosWriteFile(pRaftCfgIndex->pFile, s, strlen(s) + 1);
  tAssert(ret == strlen(s) + 1);

  taosMemoryFree(s);
  taosFsyncFile(pRaftCfgIndex->pFile);
  return 0;
}

int32_t raftCfgIndexAddConfigIndex(SRaftCfgIndex *pRaftCfgIndex, SyncIndex configIndex) {
  tAssert(pRaftCfgIndex->configIndexCount <= MAX_CONFIG_INDEX_COUNT);
  (pRaftCfgIndex->configIndexArr)[pRaftCfgIndex->configIndexCount] = configIndex;
  ++(pRaftCfgIndex->configIndexCount);
  return 0;
}

cJSON *raftCfgIndex2Json(SRaftCfgIndex *pRaftCfgIndex) {
  cJSON *pRoot = cJSON_CreateObject();

  cJSON_AddNumberToObject(pRoot, "configIndexCount", pRaftCfgIndex->configIndexCount);
  cJSON *pIndexArr = cJSON_CreateArray();
  cJSON_AddItemToObject(pRoot, "configIndexArr", pIndexArr);
  for (int i = 0; i < pRaftCfgIndex->configIndexCount; ++i) {
    char buf64[128];
    snprintf(buf64, sizeof(buf64), "%" PRId64, (pRaftCfgIndex->configIndexArr)[i]);
    cJSON *pIndexObj = cJSON_CreateObject();
    cJSON_AddStringToObject(pIndexObj, "index", buf64);
    cJSON_AddItemToArray(pIndexArr, pIndexObj);
  }

  cJSON *pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SRaftCfgIndex", pRoot);
  return pJson;
}

char *raftCfgIndex2Str(SRaftCfgIndex *pRaftCfgIndex) {
  cJSON *pJson = raftCfgIndex2Json(pRaftCfgIndex);
  char  *serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

int32_t raftCfgIndexFromJson(const cJSON *pRoot, SRaftCfgIndex *pRaftCfgIndex) {
  cJSON *pJson = cJSON_GetObjectItem(pRoot, "SRaftCfgIndex");

  cJSON *pJsonConfigIndexCount = cJSON_GetObjectItem(pJson, "configIndexCount");
  pRaftCfgIndex->configIndexCount = cJSON_GetNumberValue(pJsonConfigIndexCount);

  cJSON *pIndexArr = cJSON_GetObjectItem(pJson, "configIndexArr");
  int    arraySize = cJSON_GetArraySize(pIndexArr);
  tAssert(arraySize == pRaftCfgIndex->configIndexCount);

  memset(pRaftCfgIndex->configIndexArr, 0, sizeof(pRaftCfgIndex->configIndexArr));
  for (int i = 0; i < arraySize; ++i) {
    cJSON *pIndexObj = cJSON_GetArrayItem(pIndexArr, i);
    tAssert(pIndexObj != NULL);

    cJSON *pIndex = cJSON_GetObjectItem(pIndexObj, "index");
    tAssert(cJSON_IsString(pIndex));
    (pRaftCfgIndex->configIndexArr)[i] = atoll(pIndex->valuestring);
  }

  return 0;
}

int32_t raftCfgIndexFromStr(const char *s, SRaftCfgIndex *pRaftCfgIndex) {
  cJSON *pRoot = cJSON_Parse(s);
  tAssert(pRoot != NULL);

  int32_t ret = raftCfgIndexFromJson(pRoot, pRaftCfgIndex);
  tAssert(ret == 0);

  cJSON_Delete(pRoot);
  return 0;
}

int32_t raftCfgIndexCreateFile(const char *path) {
  TdFilePtr pFile = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE);
  if (pFile == NULL) {
    int32_t     err = terrno;
    const char *errStr = tstrerror(err);
    int32_t     sysErr = errno;
    const char *sysErrStr = strerror(errno);
    sError("create raft cfg index file error, err:%d %X, msg:%s, syserr:%d, sysmsg:%s", err, err, errStr, sysErr,
           sysErrStr);
    tAssert(0);

    return -1;
  }

  SRaftCfgIndex raftCfgIndex;
  memset(raftCfgIndex.configIndexArr, 0, sizeof(raftCfgIndex.configIndexArr));
  raftCfgIndex.configIndexCount = 1;
  raftCfgIndex.configIndexArr[0] = -1;

  char   *s = raftCfgIndex2Str(&raftCfgIndex);
  int64_t ret = taosWriteFile(pFile, s, strlen(s) + 1);
  tAssert(ret == strlen(s) + 1);

  taosMemoryFree(s);
  taosCloseFile(&pFile);
  return 0;
}

// ---------------------------------------
// file must already exist!
SRaftCfg *raftCfgOpen(const char *path) {
  SRaftCfg *pCfg = taosMemoryMalloc(sizeof(SRaftCfg));
  snprintf(pCfg->path, sizeof(pCfg->path), "%s", path);

  pCfg->pFile = taosOpenFile(pCfg->path, TD_FILE_READ | TD_FILE_WRITE);
  tAssert(pCfg->pFile != NULL);

  taosLSeekFile(pCfg->pFile, 0, SEEK_SET);

  char buf[CONFIG_FILE_LEN] = {0};
  int  len = taosReadFile(pCfg->pFile, buf, sizeof(buf));
  tAssert(len > 0);

  int32_t ret = raftCfgFromStr(buf, pCfg);
  tAssert(ret == 0);

  return pCfg;
}

int32_t raftCfgClose(SRaftCfg *pRaftCfg) {
  int64_t ret = taosCloseFile(&(pRaftCfg->pFile));
  tAssert(ret == 0);
  taosMemoryFree(pRaftCfg);
  return 0;
}

int32_t raftCfgPersist(SRaftCfg *pRaftCfg) {
  tAssert(pRaftCfg != NULL);

  char *s = raftCfg2Str(pRaftCfg);
  taosLSeekFile(pRaftCfg->pFile, 0, SEEK_SET);

  char buf[CONFIG_FILE_LEN] = {0};
  memset(buf, 0, sizeof(buf));

  if (strlen(s) + 1 > CONFIG_FILE_LEN) {
    sError("too long config str:%s", s);
    tAssert(0);
  }

  snprintf(buf, sizeof(buf), "%s", s);
  int64_t ret = taosWriteFile(pRaftCfg->pFile, buf, sizeof(buf));
  tAssert(ret == sizeof(buf));

  // int64_t ret = taosWriteFile(pRaftCfg->pFile, s, strlen(s) + 1);
  // tAssert(ret == strlen(s) + 1);

  taosMemoryFree(s);
  taosFsyncFile(pRaftCfg->pFile);
  return 0;
}

int32_t raftCfgAddConfigIndex(SRaftCfg *pRaftCfg, SyncIndex configIndex) {
  tAssert(pRaftCfg->configIndexCount <= MAX_CONFIG_INDEX_COUNT);
  (pRaftCfg->configIndexArr)[pRaftCfg->configIndexCount] = configIndex;
  ++(pRaftCfg->configIndexCount);
  return 0;
}

cJSON *syncCfg2Json(SSyncCfg *pSyncCfg) {
  char   u64buf[128] = {0};
  cJSON *pRoot = cJSON_CreateObject();

  if (pSyncCfg != NULL) {
    cJSON_AddNumberToObject(pRoot, "replicaNum", pSyncCfg->replicaNum);
    cJSON_AddNumberToObject(pRoot, "myIndex", pSyncCfg->myIndex);

    cJSON *pNodeInfoArr = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "nodeInfo", pNodeInfoArr);
    for (int i = 0; i < pSyncCfg->replicaNum; ++i) {
      cJSON *pNodeInfo = cJSON_CreateObject();
      cJSON_AddNumberToObject(pNodeInfo, "nodePort", ((pSyncCfg->nodeInfo)[i]).nodePort);
      cJSON_AddStringToObject(pNodeInfo, "nodeFqdn", ((pSyncCfg->nodeInfo)[i]).nodeFqdn);
      cJSON_AddItemToArray(pNodeInfoArr, pNodeInfo);
    }
  }

  return pRoot;
}

int32_t syncCfgFromJson(const cJSON *pRoot, SSyncCfg *pSyncCfg) {
  memset(pSyncCfg, 0, sizeof(SSyncCfg));
  // cJSON *pJson = cJSON_GetObjectItem(pRoot, "SSyncCfg");
  const cJSON *pJson = pRoot;

  cJSON *pReplicaNum = cJSON_GetObjectItem(pJson, "replicaNum");
  tAssert(cJSON_IsNumber(pReplicaNum));
  pSyncCfg->replicaNum = cJSON_GetNumberValue(pReplicaNum);

  cJSON *pMyIndex = cJSON_GetObjectItem(pJson, "myIndex");
  tAssert(cJSON_IsNumber(pMyIndex));
  pSyncCfg->myIndex = cJSON_GetNumberValue(pMyIndex);

  cJSON *pNodeInfoArr = cJSON_GetObjectItem(pJson, "nodeInfo");
  int    arraySize = cJSON_GetArraySize(pNodeInfoArr);
  tAssert(arraySize == pSyncCfg->replicaNum);

  for (int i = 0; i < arraySize; ++i) {
    cJSON *pNodeInfo = cJSON_GetArrayItem(pNodeInfoArr, i);
    tAssert(pNodeInfo != NULL);

    cJSON *pNodePort = cJSON_GetObjectItem(pNodeInfo, "nodePort");
    tAssert(cJSON_IsNumber(pNodePort));
    ((pSyncCfg->nodeInfo)[i]).nodePort = cJSON_GetNumberValue(pNodePort);

    cJSON *pNodeFqdn = cJSON_GetObjectItem(pNodeInfo, "nodeFqdn");
    tAssert(cJSON_IsString(pNodeFqdn));
    snprintf(((pSyncCfg->nodeInfo)[i]).nodeFqdn, sizeof(((pSyncCfg->nodeInfo)[i]).nodeFqdn), "%s",
             pNodeFqdn->valuestring);
  }

  return 0;
}

cJSON *raftCfg2Json(SRaftCfg *pRaftCfg) {
  cJSON *pRoot = cJSON_CreateObject();
  cJSON_AddItemToObject(pRoot, "SSyncCfg", syncCfg2Json(&(pRaftCfg->cfg)));
  cJSON_AddNumberToObject(pRoot, "isStandBy", pRaftCfg->isStandBy);
  cJSON_AddNumberToObject(pRoot, "snapshotStrategy", pRaftCfg->snapshotStrategy);
  cJSON_AddNumberToObject(pRoot, "batchSize", pRaftCfg->batchSize);

  char buf64[128];
  snprintf(buf64, sizeof(buf64), "%" PRId64, pRaftCfg->lastConfigIndex);
  cJSON_AddStringToObject(pRoot, "lastConfigIndex", buf64);

  cJSON_AddNumberToObject(pRoot, "configIndexCount", pRaftCfg->configIndexCount);
  cJSON *pIndexArr = cJSON_CreateArray();
  cJSON_AddItemToObject(pRoot, "configIndexArr", pIndexArr);
  for (int i = 0; i < pRaftCfg->configIndexCount; ++i) {
    snprintf(buf64, sizeof(buf64), "%" PRId64, (pRaftCfg->configIndexArr)[i]);
    cJSON *pIndexObj = cJSON_CreateObject();
    cJSON_AddStringToObject(pIndexObj, "index", buf64);
    cJSON_AddItemToArray(pIndexArr, pIndexObj);
  }

  cJSON *pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "RaftCfg", pRoot);
  return pJson;
}

char *raftCfg2Str(SRaftCfg *pRaftCfg) {
  cJSON *pJson = raftCfg2Json(pRaftCfg);
  char  *serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

int32_t raftCfgCreateFile(SSyncCfg *pCfg, SRaftCfgMeta meta, const char *path) {
  TdFilePtr pFile = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE);
  if (pFile == NULL) {
    int32_t     err = terrno;
    const char *errStr = tstrerror(err);
    int32_t     sysErr = errno;
    const char *sysErrStr = strerror(errno);
    sError("create raft cfg file error, err:%d %X, msg:%s, syserr:%d, sysmsg:%s", err, err, errStr, sysErr, sysErrStr);
    return -1;
  }

  SRaftCfg raftCfg;
  raftCfg.cfg = *pCfg;
  raftCfg.isStandBy = meta.isStandBy;
  raftCfg.batchSize = meta.batchSize;
  raftCfg.snapshotStrategy = meta.snapshotStrategy;
  raftCfg.lastConfigIndex = meta.lastConfigIndex;
  raftCfg.configIndexCount = 1;
  memset(raftCfg.configIndexArr, 0, sizeof(raftCfg.configIndexArr));
  raftCfg.configIndexArr[0] = -1;
  char *s = raftCfg2Str(&raftCfg);

  char buf[CONFIG_FILE_LEN] = {0};
  memset(buf, 0, sizeof(buf));
  tAssert(strlen(s) + 1 <= CONFIG_FILE_LEN);
  snprintf(buf, sizeof(buf), "%s", s);
  int64_t ret = taosWriteFile(pFile, buf, sizeof(buf));
  tAssert(ret == sizeof(buf));

  // int64_t ret = taosWriteFile(pFile, s, strlen(s) + 1);
  // tAssert(ret == strlen(s) + 1);

  taosMemoryFree(s);
  taosCloseFile(&pFile);
  return 0;
}

int32_t raftCfgFromJson(const cJSON *pRoot, SRaftCfg *pRaftCfg) {
  // memset(pRaftCfg, 0, sizeof(SRaftCfg));
  cJSON *pJson = cJSON_GetObjectItem(pRoot, "RaftCfg");

  cJSON *pJsonIsStandBy = cJSON_GetObjectItem(pJson, "isStandBy");
  pRaftCfg->isStandBy = cJSON_GetNumberValue(pJsonIsStandBy);

  cJSON *pJsonBatchSize = cJSON_GetObjectItem(pJson, "batchSize");
  pRaftCfg->batchSize = cJSON_GetNumberValue(pJsonBatchSize);

  cJSON *pJsonSnapshotStrategy = cJSON_GetObjectItem(pJson, "snapshotStrategy");
  pRaftCfg->snapshotStrategy = cJSON_GetNumberValue(pJsonSnapshotStrategy);

  cJSON *pJsonLastConfigIndex = cJSON_GetObjectItem(pJson, "lastConfigIndex");
  pRaftCfg->lastConfigIndex = atoll(cJSON_GetStringValue(pJsonLastConfigIndex));

  cJSON *pJsonConfigIndexCount = cJSON_GetObjectItem(pJson, "configIndexCount");
  pRaftCfg->configIndexCount = cJSON_GetNumberValue(pJsonConfigIndexCount);

  cJSON *pIndexArr = cJSON_GetObjectItem(pJson, "configIndexArr");
  int    arraySize = cJSON_GetArraySize(pIndexArr);
  tAssert(arraySize == pRaftCfg->configIndexCount);

  memset(pRaftCfg->configIndexArr, 0, sizeof(pRaftCfg->configIndexArr));
  for (int i = 0; i < arraySize; ++i) {
    cJSON *pIndexObj = cJSON_GetArrayItem(pIndexArr, i);
    tAssert(pIndexObj != NULL);

    cJSON *pIndex = cJSON_GetObjectItem(pIndexObj, "index");
    tAssert(cJSON_IsString(pIndex));
    (pRaftCfg->configIndexArr)[i] = atoll(pIndex->valuestring);
  }

  cJSON  *pJsonSyncCfg = cJSON_GetObjectItem(pJson, "SSyncCfg");
  int32_t code = syncCfgFromJson(pJsonSyncCfg, &(pRaftCfg->cfg));
  tAssert(code == 0);

  return code;
}

int32_t raftCfgFromStr(const char *s, SRaftCfg *pRaftCfg) {
  cJSON *pRoot = cJSON_Parse(s);
  tAssert(pRoot != NULL);

  int32_t ret = raftCfgFromJson(pRoot, pRaftCfg);
  tAssert(ret == 0);

  cJSON_Delete(pRoot);
  return 0;
}
