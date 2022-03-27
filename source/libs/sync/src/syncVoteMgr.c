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

#include "syncVoteMgr.h"
#include "syncUtil.h"

// SVotesGranted -----------------------------
static void voteGrantedClearVotes(SVotesGranted *pVotesGranted) {
  memset(pVotesGranted->isGranted, 0, sizeof(pVotesGranted->isGranted));
  pVotesGranted->votes = 0;
}

SVotesGranted *voteGrantedCreate(SSyncNode *pSyncNode) {
  SVotesGranted *pVotesGranted = taosMemoryMalloc(sizeof(SVotesGranted));
  assert(pVotesGranted != NULL);
  memset(pVotesGranted, 0, sizeof(SVotesGranted));

  pVotesGranted->replicas = &(pSyncNode->replicasId);
  pVotesGranted->replicaNum = pSyncNode->replicaNum;
  voteGrantedClearVotes(pVotesGranted);

  pVotesGranted->term = 0;
  pVotesGranted->quorum = pSyncNode->quorum;
  pVotesGranted->toLeader = false;
  pVotesGranted->pSyncNode = pSyncNode;

  return pVotesGranted;
}

void voteGrantedDestroy(SVotesGranted *pVotesGranted) {
  if (pVotesGranted != NULL) {
    taosMemoryFree(pVotesGranted);
  }
}

bool voteGrantedMajority(SVotesGranted *pVotesGranted) {
  bool ret = pVotesGranted->votes >= pVotesGranted->quorum;
  return ret;
}

void voteGrantedVote(SVotesGranted *pVotesGranted, SyncRequestVoteReply *pMsg) {
  assert(pMsg->voteGranted == true);
  assert(pMsg->term == pVotesGranted->term);
  assert(syncUtilSameId(&pVotesGranted->pSyncNode->myRaftId, &pMsg->destId));

  int j = -1;
  for (int i = 0; i < pVotesGranted->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pVotesGranted->replicas))[i]), &(pMsg->srcId))) {
      j = i;
      break;
    }
  }
  assert(j != -1);
  assert(j >= 0 && j < pVotesGranted->replicaNum);

  if (pVotesGranted->isGranted[j] != true) {
    ++(pVotesGranted->votes);
    pVotesGranted->isGranted[j] = true;
  }
  assert(pVotesGranted->votes <= pVotesGranted->replicaNum);
}

void voteGrantedReset(SVotesGranted *pVotesGranted, SyncTerm term) {
  pVotesGranted->term = term;
  voteGrantedClearVotes(pVotesGranted);
  pVotesGranted->toLeader = false;
}

cJSON *voteGranted2Json(SVotesGranted *pVotesGranted) {
  char   u64buf[128];
  cJSON *pRoot = cJSON_CreateObject();

  if (pVotesGranted != NULL) {
    cJSON_AddNumberToObject(pRoot, "replicaNum", pVotesGranted->replicaNum);
    cJSON *pReplicas = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "replicas", pReplicas);
    for (int i = 0; i < pVotesGranted->replicaNum; ++i) {
      cJSON_AddItemToArray(pReplicas, syncUtilRaftId2Json(&(*(pVotesGranted->replicas))[i]));
    }
    int *arr = (int *)taosMemoryMalloc(sizeof(int) * pVotesGranted->replicaNum);
    for (int i = 0; i < pVotesGranted->replicaNum; ++i) {
      arr[i] = pVotesGranted->isGranted[i];
    }
    cJSON *pIsGranted = cJSON_CreateIntArray(arr, pVotesGranted->replicaNum);
    taosMemoryFree(arr);
    cJSON_AddItemToObject(pRoot, "isGranted", pIsGranted);

    cJSON_AddNumberToObject(pRoot, "votes", pVotesGranted->votes);
    snprintf(u64buf, sizeof(u64buf), "%lu", pVotesGranted->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);
    cJSON_AddNumberToObject(pRoot, "quorum", pVotesGranted->quorum);
    cJSON_AddNumberToObject(pRoot, "toLeader", pVotesGranted->toLeader);
    snprintf(u64buf, sizeof(u64buf), "%p", pVotesGranted->pSyncNode);
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
  char * serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug -------------------
void voteGrantedPrint(SVotesGranted *pObj) {
  char *serialized = voteGranted2Str(pObj);
  printf("voteGrantedPrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void voteGrantedPrint2(char *s, SVotesGranted *pObj) {
  char *serialized = voteGranted2Str(pObj);
  printf("voteGrantedPrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void voteGrantedLog(SVotesGranted *pObj) {
  char *serialized = voteGranted2Str(pObj);
  sTrace("voteGrantedLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void voteGrantedLog2(char *s, SVotesGranted *pObj) {
  char *serialized = voteGranted2Str(pObj);
  sTrace("voteGrantedLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
  taosMemoryFree(serialized);
}

// SVotesRespond -----------------------------
SVotesRespond *votesRespondCreate(SSyncNode *pSyncNode) {
  SVotesRespond *pVotesRespond = taosMemoryMalloc(sizeof(SVotesRespond));
  assert(pVotesRespond != NULL);
  memset(pVotesRespond, 0, sizeof(SVotesRespond));

  pVotesRespond->replicas = &(pSyncNode->replicasId);
  pVotesRespond->replicaNum = pSyncNode->replicaNum;
  pVotesRespond->term = 0;
  pVotesRespond->pSyncNode = pSyncNode;

  return pVotesRespond;
}

void votesRespondDestory(SVotesRespond *pVotesRespond) {
  if (pVotesRespond != NULL) {
    taosMemoryFree(pVotesRespond);
  }
}

bool votesResponded(SVotesRespond *pVotesRespond, const SRaftId *pRaftId) {
  bool ret = false;
  for (int i = 0; i < pVotesRespond->replicaNum; ++i) {
    if (syncUtilSameId(&(*pVotesRespond->replicas)[i], pRaftId) && pVotesRespond->isRespond[i]) {
      ret = true;
      break;
    }
  }
  return ret;
}

void votesRespondAdd(SVotesRespond *pVotesRespond, const SyncRequestVoteReply *pMsg) {
  assert(pVotesRespond->term == pMsg->term);
  for (int i = 0; i < pVotesRespond->replicaNum; ++i) {
    if (syncUtilSameId(&((*(pVotesRespond->replicas))[i]), &pMsg->srcId)) {
      // assert(pVotesRespond->isRespond[i] == false);
      pVotesRespond->isRespond[i] = true;
      return;
    }
  }
  assert(0);
}

void votesRespondReset(SVotesRespond *pVotesRespond, SyncTerm term) {
  pVotesRespond->term = term;
  memset(pVotesRespond->isRespond, 0, sizeof(pVotesRespond->isRespond));
  /*
    for (int i = 0; i < pVotesRespond->replicaNum; ++i) {
      pVotesRespond->isRespond[i] = false;
    }
  */
}

cJSON *votesRespond2Json(SVotesRespond *pVotesRespond) {
  char   u64buf[128];
  cJSON *pRoot = cJSON_CreateObject();

  if (pVotesRespond != NULL) {
    cJSON_AddNumberToObject(pRoot, "replicaNum", pVotesRespond->replicaNum);
    cJSON *pReplicas = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "replicas", pReplicas);
    for (int i = 0; i < pVotesRespond->replicaNum; ++i) {
      cJSON_AddItemToArray(pReplicas, syncUtilRaftId2Json(&(*(pVotesRespond->replicas))[i]));
    }
    int  respondNum = 0;
    int *arr = (int *)taosMemoryMalloc(sizeof(int) * pVotesRespond->replicaNum);
    for (int i = 0; i < pVotesRespond->replicaNum; ++i) {
      arr[i] = pVotesRespond->isRespond[i];
      if (pVotesRespond->isRespond[i]) {
        respondNum++;
      }
    }
    cJSON *pIsRespond = cJSON_CreateIntArray(arr, pVotesRespond->replicaNum);
    taosMemoryFree(arr);
    cJSON_AddItemToObject(pRoot, "isRespond", pIsRespond);
    cJSON_AddNumberToObject(pRoot, "respondNum", respondNum);

    snprintf(u64buf, sizeof(u64buf), "%lu", pVotesRespond->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pVotesRespond->pSyncNode);
    cJSON_AddStringToObject(pRoot, "pSyncNode", u64buf);
  }

  cJSON *pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SVotesRespond", pRoot);
  return pJson;
}

char *votesRespond2Str(SVotesRespond *pVotesRespond) {
  cJSON *pJson = votesRespond2Json(pVotesRespond);
  char * serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// for debug -------------------
void votesRespondPrint(SVotesRespond *pObj) {
  char *serialized = votesRespond2Str(pObj);
  printf("votesRespondPrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void votesRespondPrint2(char *s, SVotesRespond *pObj) {
  char *serialized = votesRespond2Str(pObj);
  printf("votesRespondPrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void votesRespondLog(SVotesRespond *pObj) {
  char *serialized = votesRespond2Str(pObj);
  sTrace("votesRespondLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void votesRespondLog2(char *s, SVotesRespond *pObj) {
  char *serialized = votesRespond2Str(pObj);
  sTrace("votesRespondLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
  taosMemoryFree(serialized);
}
