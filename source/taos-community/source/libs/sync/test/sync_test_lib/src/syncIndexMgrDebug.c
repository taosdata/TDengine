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
#include "syncTest.h"

void syncIndexMgrPrint(SSyncIndexMgr *pObj) {
  char *serialized = syncIndexMgr2Str(pObj);
  printf("syncIndexMgrPrint | len:%" PRIu64 " | %s \n", (uint64_t)strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncIndexMgrPrint2(char *s, SSyncIndexMgr *pObj) {
  char *serialized = syncIndexMgr2Str(pObj);
  printf("syncIndexMgrPrint2 | len:%" PRIu64 " | %s | %s \n", (uint64_t)strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncIndexMgrLog(SSyncIndexMgr *pObj) {
  char *serialized = syncIndexMgr2Str(pObj);
  sTrace("syncIndexMgrLog | len:%" PRIu64 " | %s", (uint64_t)strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncIndexMgrLog2(char *s, SSyncIndexMgr *pObj) {
  if (gRaftDetailLog) {
    char *serialized = syncIndexMgr2Str(pObj);
    sTrace("syncIndexMgrLog2 | len:%" PRIu64 " | %s | %s", (uint64_t)strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

cJSON *syncIndexMgr2Json(SSyncIndexMgr *pSyncIndexMgr) {
  char   u64buf[128] = {0};
  cJSON *pRoot = cJSON_CreateObject();

  if (pSyncIndexMgr != NULL) {
    cJSON_AddNumberToObject(pRoot, "replicaNum", pSyncIndexMgr->replicaNum);
    cJSON *pReplicas = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "replicas", pReplicas);
    for (int i = 0; i < pSyncIndexMgr->replicaNum; ++i) {
      // cJSON_AddItemToArray(pReplicas, syncUtilRaftId2Json(&(*(pSyncIndexMgr->replicas))[i]));
    }

    {
      int *arr = (int *)taosMemoryMalloc(sizeof(int) * pSyncIndexMgr->replicaNum);
      for (int i = 0; i < pSyncIndexMgr->replicaNum; ++i) {
        arr[i] = pSyncIndexMgr->index[i];
      }
      cJSON *pIndex = cJSON_CreateIntArray(arr, pSyncIndexMgr->replicaNum);
      taosMemoryFree(arr);
      cJSON_AddItemToObject(pRoot, "index", pIndex);
    }

    {
      int *arr = (int *)taosMemoryMalloc(sizeof(int) * pSyncIndexMgr->replicaNum);
      for (int i = 0; i < pSyncIndexMgr->replicaNum; ++i) {
        arr[i] = pSyncIndexMgr->privateTerm[i];
      }
      cJSON *pIndex = cJSON_CreateIntArray(arr, pSyncIndexMgr->replicaNum);
      taosMemoryFree(arr);
      cJSON_AddItemToObject(pRoot, "privateTerm", pIndex);
    }

    // snprintf(u64buf, sizeof(u64buf), "%p", pSyncIndexMgr->pSyncNode);
    cJSON_AddStringToObject(pRoot, "pSyncNode", u64buf);
  }

  cJSON *pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "pSyncIndexMgr", pRoot);
  return pJson;
}

char *syncIndexMgr2Str(SSyncIndexMgr *pSyncIndexMgr) {
  cJSON *pJson = syncIndexMgr2Json(pSyncIndexMgr);
  char  *serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}
