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

cJSON *voteGranted2Json(SVotesGranted *pVotesGranted) {
  char   u64buf[128] = {0};
  cJSON *pRoot = cJSON_CreateObject();

  if (pVotesGranted != NULL) {
    cJSON_AddNumberToObject(pRoot, "replicaNum", pVotesGranted->replicaNum);
    cJSON *pReplicas = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "replicas", pReplicas);
    for (int32_t i = 0; i < pVotesGranted->replicaNum; ++i) {
      // cJSON_AddItemToArray(pReplicas, syncUtilRaftId2Json(&(*(pVotesGranted->replicas))[i]));
    }
    int32_t *arr = (int32_t *)taosMemoryMalloc(sizeof(int32_t) * pVotesGranted->replicaNum);
    for (int32_t i = 0; i < pVotesGranted->replicaNum; ++i) {
      arr[i] = pVotesGranted->isGranted[i];
    }
    cJSON *pIsGranted = cJSON_CreateIntArray(arr, pVotesGranted->replicaNum);
    taosMemoryFree(arr);
    cJSON_AddItemToObject(pRoot, "isGranted", pIsGranted);

    cJSON_AddNumberToObject(pRoot, "votes", pVotesGranted->votes);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pVotesGranted->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);
    cJSON_AddNumberToObject(pRoot, "quorum", pVotesGranted->quorum);
    cJSON_AddNumberToObject(pRoot, "toLeader", pVotesGranted->toLeader);
    snprintf(u64buf, sizeof(u64buf), "%p", pVotesGranted->pNode);
    cJSON_AddStringToObject(pRoot, "pSyncNode", u64buf);

    bool majority = voteGrantedMajority(pVotesGranted);
    cJSON_AddNumberToObject(pRoot, "majority", majority);
  }

  cJSON *pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SVotesGranted", pRoot);
  return pJson;
}

char *voteGranted2Str(SVotesGranted *pVotesGranted) {
  cJSON *pJson = voteGranted2Json(pVotesGranted);
  char  *serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

cJSON *votesRespond2Json(SVotesRespond *pVotesRespond) {
  char   u64buf[128] = {0};
  cJSON *pRoot = cJSON_CreateObject();

  if (pVotesRespond != NULL) {
    cJSON_AddNumberToObject(pRoot, "replicaNum", pVotesRespond->replicaNum);
    cJSON *pReplicas = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "replicas", pReplicas);
    for (int32_t i = 0; i < pVotesRespond->replicaNum; ++i) {
      // cJSON_AddItemToArray(pReplicas, syncUtilRaftId2Json(&(*(pVotesRespond->replicas))[i]));
    }
    int32_t  respondNum = 0;
    int32_t *arr = (int32_t *)taosMemoryMalloc(sizeof(int32_t) * pVotesRespond->replicaNum);
    for (int32_t i = 0; i < pVotesRespond->replicaNum; ++i) {
      arr[i] = pVotesRespond->isRespond[i];
      if (pVotesRespond->isRespond[i]) {
        respondNum++;
      }
    }
    cJSON *pIsRespond = cJSON_CreateIntArray(arr, pVotesRespond->replicaNum);
    taosMemoryFree(arr);
    cJSON_AddItemToObject(pRoot, "isRespond", pIsRespond);
    cJSON_AddNumberToObject(pRoot, "respondNum", respondNum);

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pVotesRespond->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pVotesRespond->pNode);
    cJSON_AddStringToObject(pRoot, "pSyncNode", u64buf);
  }

  cJSON *pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SVotesRespond", pRoot);
  return pJson;
}

char *votesRespond2Str(SVotesRespond *pVotesRespond) {
  cJSON *pJson = votesRespond2Json(pVotesRespond);
  char  *serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}
