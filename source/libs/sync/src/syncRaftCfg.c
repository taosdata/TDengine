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
SRaftCfg *raftCfgOpen(const char *path) {
  SRaftCfg *pCfg = taosMemoryMalloc(sizeof(SRaftCfg));
  snprintf(pCfg->path, sizeof(pCfg->path), "%s", path);

  pCfg->pFile = taosOpenFile(pCfg->path, TD_FILE_READ | TD_FILE_WRITE);
  assert(pCfg->pFile != NULL);

  taosLSeekFile(pCfg->pFile, 0, SEEK_SET);

  char buf[1024];
  int  len = taosReadFile(pCfg->pFile, buf, sizeof(buf));
  assert(len > 0);

  int32_t ret = syncCfgFromStr(buf, &(pCfg->cfg));
  assert(ret == 0);

  return pCfg;
}

int32_t raftCfgClose(SRaftCfg *pRaftCfg) {
  int64_t ret = taosCloseFile(&(pRaftCfg->pFile));
  assert(ret == 0);
  taosMemoryFree(pRaftCfg);
  return 0;
}

int32_t raftCfgPersist(SRaftCfg *pRaftCfg) {
  assert(pRaftCfg != NULL);

  char *s = syncCfg2Str(&(pRaftCfg->cfg));
  taosLSeekFile(pRaftCfg->pFile, 0, SEEK_SET);
  int64_t ret = taosWriteFile(pRaftCfg->pFile, s, strlen(s) + 1);
  assert(ret == strlen(s) + 1);
  taosMemoryFree(s);

  taosFsyncFile(pRaftCfg->pFile);
  return 0;
}

cJSON *syncCfg2Json(SSyncCfg *pSyncCfg) {
  char   u64buf[128];
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

  cJSON *pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SSyncCfg", pRoot);
  return pJson;
}

char *syncCfg2Str(SSyncCfg *pSyncCfg) {
  cJSON *pJson = syncCfg2Json(pSyncCfg);
  char * serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

int32_t syncCfgFromJson(const cJSON *pRoot, SSyncCfg *pSyncCfg) {
  memset(pSyncCfg, 0, sizeof(SSyncCfg));
  cJSON *pJson = cJSON_GetObjectItem(pRoot, "SSyncCfg");

  cJSON *pReplicaNum = cJSON_GetObjectItem(pJson, "replicaNum");
  assert(cJSON_IsNumber(pReplicaNum));
  pSyncCfg->replicaNum = cJSON_GetNumberValue(pReplicaNum);

  cJSON *pMyIndex = cJSON_GetObjectItem(pJson, "myIndex");
  assert(cJSON_IsNumber(pMyIndex));
  pSyncCfg->myIndex = cJSON_GetNumberValue(pMyIndex);

  cJSON *pNodeInfoArr = cJSON_GetObjectItem(pJson, "nodeInfo");
  int    arraySize = cJSON_GetArraySize(pNodeInfoArr);
  assert(arraySize == pSyncCfg->replicaNum);

  for (int i = 0; i < arraySize; ++i) {
    cJSON *pNodeInfo = cJSON_GetArrayItem(pNodeInfoArr, i);
    assert(pNodeInfo != NULL);

    cJSON *pNodePort = cJSON_GetObjectItem(pNodeInfo, "nodePort");
    assert(cJSON_IsNumber(pNodePort));
    ((pSyncCfg->nodeInfo)[i]).nodePort = cJSON_GetNumberValue(pNodePort);

    cJSON *pNodeFqdn = cJSON_GetObjectItem(pNodeInfo, "nodeFqdn");
    assert(cJSON_IsString(pNodeFqdn));
    snprintf(((pSyncCfg->nodeInfo)[i]).nodeFqdn, sizeof(((pSyncCfg->nodeInfo)[i]).nodeFqdn), "%s",
             pNodeFqdn->valuestring);
  }

  return 0;
}

int32_t syncCfgFromStr(const char *s, SSyncCfg *pSyncCfg) {
  cJSON *pRoot = cJSON_Parse(s);
  assert(pRoot != NULL);

  int32_t ret = syncCfgFromJson(pRoot, pSyncCfg);
  assert(ret == 0);

  cJSON_Delete(pRoot);
  return 0;
}

cJSON *raftCfg2Json(SRaftCfg *pRaftCfg) {
  cJSON *pJson = syncCfg2Json(&(pRaftCfg->cfg));
  return pJson;
}

char *raftCfg2Str(SRaftCfg *pRaftCfg) {
  char *s = syncCfg2Str(&(pRaftCfg->cfg));
  return s;
}

int32_t syncCfgCreateFile(SSyncCfg *pCfg, const char *path) {
  assert(pCfg != NULL);

  TdFilePtr pFile = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE);
  assert(pFile != NULL);

  char *  s = syncCfg2Str(pCfg);
  int64_t ret = taosWriteFile(pFile, s, strlen(s) + 1);
  assert(ret == strlen(s) + 1);

  taosMemoryFree(s);
  taosCloseFile(&pFile);
  return 0;
}

// for debug ----------------------
void syncCfgPrint(SSyncCfg *pCfg) {
  char *serialized = syncCfg2Str(pCfg);
  printf("syncCfgPrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncCfgPrint2(char *s, SSyncCfg *pCfg) {
  char *serialized = syncCfg2Str(pCfg);
  printf("syncCfgPrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncCfgLog(SSyncCfg *pCfg) {
  char *serialized = syncCfg2Str(pCfg);
  sTrace("syncCfgLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncCfgLog2(char *s, SSyncCfg *pCfg) {
  char *serialized = syncCfg2Str(pCfg);
  sTrace("syncCfgLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
  taosMemoryFree(serialized);
}

void raftCfgPrint(SRaftCfg *pCfg) {
  char *serialized = raftCfg2Str(pCfg);
  printf("raftCfgPrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void raftCfgPrint2(char *s, SRaftCfg *pCfg) {
  char *serialized = raftCfg2Str(pCfg);
  printf("raftCfgPrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void raftCfgLog(SRaftCfg *pCfg) {
  char *serialized = raftCfg2Str(pCfg);
  sTrace("raftCfgLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void raftCfgLog2(char *s, SRaftCfg *pCfg) {
  char *serialized = raftCfg2Str(pCfg);
  sTrace("raftCfgLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
  taosMemoryFree(serialized);
}
