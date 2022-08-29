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

#include "syncRaftCfg.h"
#include "cJSON.h"
#include "syncEnv.h"
#include "syncUtil.h"

// file must already exist!
SRaftCfgIndex *raftCfgIndexOpen(const char *path) {
  SRaftCfgIndex *pRaftCfgIndex = taosMemoryMalloc(sizeof(SRaftCfg));
  snprintf(pRaftCfgIndex->path, sizeof(pRaftCfgIndex->path), "%s", path);

  pRaftCfgIndex->pFile = taosOpenFile(pRaftCfgIndex->path, TD_FILE_READ | TD_FILE_WRITE);
  ASSERT(pRaftCfgIndex->pFile != NULL);

  taosLSeekFile(pRaftCfgIndex->pFile, 0, SEEK_SET);

  int32_t bufLen = MAX_CONFIG_INDEX_COUNT * 16;
  char   *pBuf = taosMemoryMalloc(bufLen);
  memset(pBuf, 0, bufLen);
  int64_t len = taosReadFile(pRaftCfgIndex->pFile, pBuf, bufLen);
  ASSERT(len > 0);

  int32_t ret = raftCfgIndexFromStr(pBuf, pRaftCfgIndex);
  ASSERT(ret == 0);

  taosMemoryFree(pBuf);

  return pRaftCfgIndex;
}

int32_t raftCfgIndexClose(SRaftCfgIndex *pRaftCfgIndex) {
  if (pRaftCfgIndex != NULL) {
    int64_t ret = taosCloseFile(&(pRaftCfgIndex->pFile));
    ASSERT(ret == 0);
    taosMemoryFree(pRaftCfgIndex);
  }
  return 0;
}

int32_t raftCfgIndexPersist(SRaftCfgIndex *pRaftCfgIndex) {
  ASSERT(pRaftCfgIndex != NULL);

  char *s = raftCfgIndex2Str(pRaftCfgIndex);
  taosLSeekFile(pRaftCfgIndex->pFile, 0, SEEK_SET);

  int64_t ret = taosWriteFile(pRaftCfgIndex->pFile, s, strlen(s) + 1);
  ASSERT(ret == strlen(s) + 1);

  taosMemoryFree(s);
  taosFsyncFile(pRaftCfgIndex->pFile);
  return 0;
}

int32_t raftCfgIndexAddConfigIndex(SRaftCfgIndex *pRaftCfgIndex, SyncIndex configIndex) {
  ASSERT(pRaftCfgIndex->configIndexCount <= MAX_CONFIG_INDEX_COUNT);
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
  ASSERT(arraySize == pRaftCfgIndex->configIndexCount);

  memset(pRaftCfgIndex->configIndexArr, 0, sizeof(pRaftCfgIndex->configIndexArr));
  for (int i = 0; i < arraySize; ++i) {
    cJSON *pIndexObj = cJSON_GetArrayItem(pIndexArr, i);
    ASSERT(pIndexObj != NULL);

    cJSON *pIndex = cJSON_GetObjectItem(pIndexObj, "index");
    ASSERT(cJSON_IsString(pIndex));
    (pRaftCfgIndex->configIndexArr)[i] = atoll(pIndex->valuestring);
  }

  return 0;
}

int32_t raftCfgIndexFromStr(const char *s, SRaftCfgIndex *pRaftCfgIndex) {
  cJSON *pRoot = cJSON_Parse(s);
  ASSERT(pRoot != NULL);

  int32_t ret = raftCfgIndexFromJson(pRoot, pRaftCfgIndex);
  ASSERT(ret == 0);

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
    ASSERT(0);

    return -1;
  }

  SRaftCfgIndex raftCfgIndex;
  memset(raftCfgIndex.configIndexArr, 0, sizeof(raftCfgIndex.configIndexArr));
  raftCfgIndex.configIndexCount = 1;
  raftCfgIndex.configIndexArr[0] = -1;

  char   *s = raftCfgIndex2Str(&raftCfgIndex);
  int64_t ret = taosWriteFile(pFile, s, strlen(s) + 1);
  ASSERT(ret == strlen(s) + 1);

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
  ASSERT(pCfg->pFile != NULL);

  taosLSeekFile(pCfg->pFile, 0, SEEK_SET);

  char buf[CONFIG_FILE_LEN] = {0};
  int  len = taosReadFile(pCfg->pFile, buf, sizeof(buf));
  ASSERT(len > 0);

  int32_t ret = raftCfgFromStr(buf, pCfg);
  ASSERT(ret == 0);

  return pCfg;
}

int32_t raftCfgClose(SRaftCfg *pRaftCfg) {
  int64_t ret = taosCloseFile(&(pRaftCfg->pFile));
  ASSERT(ret == 0);
  taosMemoryFree(pRaftCfg);
  return 0;
}

int32_t raftCfgPersist(SRaftCfg *pRaftCfg) {
  ASSERT(pRaftCfg != NULL);

  char *s = raftCfg2Str(pRaftCfg);
  taosLSeekFile(pRaftCfg->pFile, 0, SEEK_SET);

  char buf[CONFIG_FILE_LEN] = {0};
  memset(buf, 0, sizeof(buf));

  if (strlen(s) + 1 > CONFIG_FILE_LEN) {
    sError("too long config str:%s", s);
    ASSERT(0);
  }

  snprintf(buf, sizeof(buf), "%s", s);
  int64_t ret = taosWriteFile(pRaftCfg->pFile, buf, sizeof(buf));
  ASSERT(ret == sizeof(buf));

  // int64_t ret = taosWriteFile(pRaftCfg->pFile, s, strlen(s) + 1);
  // ASSERT(ret == strlen(s) + 1);

  taosMemoryFree(s);
  taosFsyncFile(pRaftCfg->pFile);
  return 0;
}

int32_t raftCfgAddConfigIndex(SRaftCfg *pRaftCfg, SyncIndex configIndex) {
  ASSERT(pRaftCfg->configIndexCount <= MAX_CONFIG_INDEX_COUNT);
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

char *syncCfg2Str(SSyncCfg *pSyncCfg) {
  cJSON *pJson = syncCfg2Json(pSyncCfg);
  char  *serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

char *syncCfg2SimpleStr(SSyncCfg *pSyncCfg) {
  if (pSyncCfg != NULL) {
    int32_t len = 512;
    char   *s = taosMemoryMalloc(len);
    memset(s, 0, len);

    snprintf(s, len, "{r-num:%d, my:%d, ", pSyncCfg->replicaNum, pSyncCfg->myIndex);
    char *p = s + strlen(s);
    for (int i = 0; i < pSyncCfg->replicaNum; ++i) {
      /*
      if (p + 128 + 32 > s + len) {
        break;
      }
      */
      char buf[128 + 32];
      snprintf(buf, sizeof(buf), "%s:%d, ", pSyncCfg->nodeInfo[i].nodeFqdn, pSyncCfg->nodeInfo[i].nodePort);
      strncpy(p, buf, sizeof(buf));
      p = s + strlen(s);
    }
    strcpy(p - 2, "}");

    return s;
  }

  return NULL;
}

int32_t syncCfgFromJson(const cJSON *pRoot, SSyncCfg *pSyncCfg) {
  memset(pSyncCfg, 0, sizeof(SSyncCfg));
  // cJSON *pJson = cJSON_GetObjectItem(pRoot, "SSyncCfg");
  const cJSON *pJson = pRoot;

  cJSON *pReplicaNum = cJSON_GetObjectItem(pJson, "replicaNum");
  ASSERT(cJSON_IsNumber(pReplicaNum));
  pSyncCfg->replicaNum = cJSON_GetNumberValue(pReplicaNum);

  cJSON *pMyIndex = cJSON_GetObjectItem(pJson, "myIndex");
  ASSERT(cJSON_IsNumber(pMyIndex));
  pSyncCfg->myIndex = cJSON_GetNumberValue(pMyIndex);

  cJSON *pNodeInfoArr = cJSON_GetObjectItem(pJson, "nodeInfo");
  int    arraySize = cJSON_GetArraySize(pNodeInfoArr);
  ASSERT(arraySize == pSyncCfg->replicaNum);

  for (int i = 0; i < arraySize; ++i) {
    cJSON *pNodeInfo = cJSON_GetArrayItem(pNodeInfoArr, i);
    ASSERT(pNodeInfo != NULL);

    cJSON *pNodePort = cJSON_GetObjectItem(pNodeInfo, "nodePort");
    ASSERT(cJSON_IsNumber(pNodePort));
    ((pSyncCfg->nodeInfo)[i]).nodePort = cJSON_GetNumberValue(pNodePort);

    cJSON *pNodeFqdn = cJSON_GetObjectItem(pNodeInfo, "nodeFqdn");
    ASSERT(cJSON_IsString(pNodeFqdn));
    snprintf(((pSyncCfg->nodeInfo)[i]).nodeFqdn, sizeof(((pSyncCfg->nodeInfo)[i]).nodeFqdn), "%s",
             pNodeFqdn->valuestring);
  }

  return 0;
}

int32_t syncCfgFromStr(const char *s, SSyncCfg *pSyncCfg) {
  cJSON *pRoot = cJSON_Parse(s);
  ASSERT(pRoot != NULL);

  int32_t ret = syncCfgFromJson(pRoot, pSyncCfg);
  ASSERT(ret == 0);

  cJSON_Delete(pRoot);
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
  ASSERT(pCfg != NULL);

  TdFilePtr pFile = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE);
  if (pFile == NULL) {
    int32_t     err = terrno;
    const char *errStr = tstrerror(err);
    int32_t     sysErr = errno;
    const char *sysErrStr = strerror(errno);
    sError("create raft cfg file error, err:%d %X, msg:%s, syserr:%d, sysmsg:%s", err, err, errStr, sysErr, sysErrStr);
    ASSERT(0);

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
  ASSERT(strlen(s) + 1 <= CONFIG_FILE_LEN);
  snprintf(buf, sizeof(buf), "%s", s);
  int64_t ret = taosWriteFile(pFile, buf, sizeof(buf));
  ASSERT(ret == sizeof(buf));

  // int64_t ret = taosWriteFile(pFile, s, strlen(s) + 1);
  // ASSERT(ret == strlen(s) + 1);

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
  ASSERT(arraySize == pRaftCfg->configIndexCount);

  memset(pRaftCfg->configIndexArr, 0, sizeof(pRaftCfg->configIndexArr));
  for (int i = 0; i < arraySize; ++i) {
    cJSON *pIndexObj = cJSON_GetArrayItem(pIndexArr, i);
    ASSERT(pIndexObj != NULL);

    cJSON *pIndex = cJSON_GetObjectItem(pIndexObj, "index");
    ASSERT(cJSON_IsString(pIndex));
    (pRaftCfg->configIndexArr)[i] = atoll(pIndex->valuestring);
  }

  cJSON  *pJsonSyncCfg = cJSON_GetObjectItem(pJson, "SSyncCfg");
  int32_t code = syncCfgFromJson(pJsonSyncCfg, &(pRaftCfg->cfg));
  ASSERT(code == 0);

  return code;
}

int32_t raftCfgFromStr(const char *s, SRaftCfg *pRaftCfg) {
  cJSON *pRoot = cJSON_Parse(s);
  ASSERT(pRoot != NULL);

  int32_t ret = raftCfgFromJson(pRoot, pRaftCfg);
  ASSERT(ret == 0);

  cJSON_Delete(pRoot);
  return 0;
}

// for debug ----------------------
void syncCfgPrint(SSyncCfg *pCfg) {
  char *serialized = syncCfg2Str(pCfg);
  printf("syncCfgPrint | len:%" PRIu64 " | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncCfgPrint2(char *s, SSyncCfg *pCfg) {
  char *serialized = syncCfg2Str(pCfg);
  printf("syncCfgPrint2 | len:%" PRIu64 " | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncCfgLog(SSyncCfg *pCfg) {
  char *serialized = syncCfg2Str(pCfg);
  sTrace("syncCfgLog | len:%" PRIu64 " | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncCfgLog2(char *s, SSyncCfg *pCfg) {
  char *serialized = syncCfg2Str(pCfg);
  sTrace("syncCfgLog2 | len:%" PRIu64 " | %s | %s", strlen(serialized), s, serialized);
  taosMemoryFree(serialized);
}

void syncCfgLog3(char *s, SSyncCfg *pCfg) {
  char *serialized = syncCfg2SimpleStr(pCfg);
  sTrace("syncCfgLog3 | len:%" PRIu64 " | %s | %s", strlen(serialized), s, serialized);
  taosMemoryFree(serialized);
}

void raftCfgPrint(SRaftCfg *pCfg) {
  char *serialized = raftCfg2Str(pCfg);
  printf("raftCfgPrint | len:%" PRIu64 " | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void raftCfgPrint2(char *s, SRaftCfg *pCfg) {
  char *serialized = raftCfg2Str(pCfg);
  printf("raftCfgPrint2 | len:%" PRIu64 " | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void raftCfgLog(SRaftCfg *pCfg) {
  char *serialized = raftCfg2Str(pCfg);
  sTrace("raftCfgLog | len:%" PRIu64 " | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void raftCfgLog2(char *s, SRaftCfg *pCfg) {
  char *serialized = raftCfg2Str(pCfg);
  sTrace("raftCfgLog2 | len:%" PRIu64 " | %s | %s", strlen(serialized), s, serialized);
  taosMemoryFree(serialized);
}

// ---------
void raftCfgIndexPrint(SRaftCfgIndex *pCfg) {
  char *serialized = raftCfgIndex2Str(pCfg);
  printf("raftCfgIndexPrint | len:%" PRIu64 " | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void raftCfgIndexPrint2(char *s, SRaftCfgIndex *pCfg) {
  char *serialized = raftCfgIndex2Str(pCfg);
  printf("raftCfgIndexPrint2 | len:%" PRIu64 " | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void raftCfgIndexLog(SRaftCfgIndex *pCfg) {
  char *serialized = raftCfgIndex2Str(pCfg);
  sTrace("raftCfgIndexLog | len:%" PRIu64 " | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void raftCfgIndexLog2(char *s, SRaftCfgIndex *pCfg) {
  char *serialized = raftCfgIndex2Str(pCfg);
  sTrace("raftCfgIndexLog2 | len:%" PRIu64 " | %s | %s", strlen(serialized), s, serialized);
  taosMemoryFree(serialized);
}
