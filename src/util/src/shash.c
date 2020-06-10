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

#include <ctype.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "shash.h"
#include "tlog.h"

typedef struct _str_node_t {
  char *              string;
  struct _str_node_t *prev;
  struct _str_node_t *next;
  char                data[];
} SHashNode;

typedef struct {
  SHashNode **hashList;
  uint32_t    maxSessions;
  uint32_t    dataSize;
  uint32_t (*hashFp)(void *, char *string);
  pthread_mutex_t mutex;
} SHashObj;

uint32_t taosHashString(void *handle, char *string) {
  SHashObj *pObj = (SHashObj *)handle;
  uint32_t  hash = 0, hashv;
  char *    c;

  c = string;
  while (*c) {
    hash += *((int *)c);
    c += 4;
  }

  hashv = hash / pObj->maxSessions;
  hash = (hashv + hash % pObj->maxSessions) % pObj->maxSessions;

  return hash;
}

uint32_t taosHashStringStep1(void *handle, char *string) {
  SHashObj *pObj = (SHashObj *)handle;
  uint32_t  hash = 0, hashv;
  char *    c;

  c = string;
  while (*c) {
    hash += *c;
    c++;
  }

  hashv = hash / pObj->maxSessions;
  hash = (hashv + hash % pObj->maxSessions) % pObj->maxSessions;

  return hash;
}

void *taosAddStrHashWithSize(void *handle, char *string, char *pData, int dataSize) {
  uint32_t   hash;
  SHashNode *pNode;
  SHashObj * pObj;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return NULL;
  if (string == NULL || string[0] == 0) return NULL;

  hash = (*pObj->hashFp)(pObj, string);

  pthread_mutex_lock(&pObj->mutex);

  pNode = (SHashNode *)malloc(sizeof(SHashNode) + (size_t)dataSize + strlen(string) + 1);
  memcpy(pNode->data, pData, (size_t)dataSize);
  pNode->prev = 0;
  pNode->next = pObj->hashList[hash];
  pNode->string = pNode->data + dataSize;
  strcpy(pNode->string, string);

  if (pObj->hashList[hash] != 0) (pObj->hashList[hash])->prev = pNode;
  pObj->hashList[hash] = pNode;

  pthread_mutex_unlock(&pObj->mutex);

  pTrace("hash:%d:%s is added", hash, string);

  return pNode->data;
}

void *taosAddStrHash(void *handle, char *string, char *pData) {
  if (string == NULL || string[0] == 0) return NULL;

  SHashObj *pObj = (SHashObj *)handle;
  return taosAddStrHashWithSize(handle, string, pData, pObj->dataSize);
}

void taosDeleteStrHashNode(void *handle, char *string, void *pDeleteNode) {
  uint32_t   hash;
  SHashNode *pNode;
  SHashObj * pObj;
  bool       find = false;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return;
  if (string == NULL || string[0] == 0) return;

  hash = (*(pObj->hashFp))(pObj, string);

  pthread_mutex_lock(&pObj->mutex);

  pNode = pObj->hashList[hash];

  while (pNode) {
    if (strcmp(pNode->string, string) != 0) continue;
    if (pNode->data == pDeleteNode) {
      find = true;
      break;
    }

    pNode = pNode->next;
  }

  if (find) {
    if (pNode->prev) {
      pNode->prev->next = pNode->next;
    } else {
      pObj->hashList[hash] = pNode->next;
    }

    if (pNode->next) {
      pNode->next->prev = pNode->prev;
    }

    pTrace("hash:%d:%s:%p is removed", hash, string, pNode);

    free(pNode);
  }

  pthread_mutex_unlock(&pObj->mutex);
}

void taosDeleteStrHash(void *handle, char *string) {
  uint32_t   hash;
  SHashNode *pNode;
  SHashObj * pObj;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return;
  if (string == NULL || string[0] == 0) return;

  hash = (*(pObj->hashFp))(pObj, string);

  pthread_mutex_lock(&pObj->mutex);

  pNode = pObj->hashList[hash];
  while (pNode) {
    if (strcmp(pNode->string, string) == 0) break;

    pNode = pNode->next;
  }

  if (pNode) {
    if (pNode->prev) {
      pNode->prev->next = pNode->next;
    } else {
      pObj->hashList[hash] = pNode->next;
    }

    if (pNode->next) {
      pNode->next->prev = pNode->prev;
    }

    pTrace("hash:%d:%s:%p is removed", hash, string, pNode);

    free(pNode);
  }

  pthread_mutex_unlock(&pObj->mutex);
}

void *taosGetStrHashData(void *handle, char *string) {
  uint32_t   hash;
  SHashNode *pNode;
  SHashObj * pObj;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return NULL;
  if (string == NULL || string[0] == 0) return NULL;

  hash = (*pObj->hashFp)(pObj, string);

  pthread_mutex_lock(&pObj->mutex);

  pNode = pObj->hashList[hash];

  while (pNode) {
    if (strcmp(pNode->string, string) == 0) {
      pTrace("hash:%d:%s is retrieved", hash, string);
      break;
    }

    pNode = pNode->next;
  }

  pthread_mutex_unlock(&pObj->mutex);

  if (pNode) return pNode->data;

  return NULL;
}

void *taosInitStrHash(uint32_t maxSessions, uint32_t dataSize, uint32_t (*fp)(void *, char *)) {
  SHashObj *pObj;

  pObj = (SHashObj *)malloc(sizeof(SHashObj));
  if (pObj == NULL) {
    return NULL;
  }

  memset(pObj, 0, sizeof(SHashObj));
  pObj->maxSessions = maxSessions;
  pObj->dataSize = dataSize;
  pObj->hashFp = fp;

  pObj->hashList = (SHashNode **)malloc(sizeof(SHashNode *) * (size_t)maxSessions);
  if (pObj->hashList == NULL) {
    free(pObj);
    return NULL;
  }
  memset(pObj->hashList, 0, sizeof(SHashNode *) * (size_t)maxSessions);

  pthread_mutex_init(&pObj->mutex, NULL);

  return pObj;
}

void taosCleanUpStrHashWithFp(void *handle, void (*fp)(char *)) {
  SHashObj * pObj;
  SHashNode *pNode, *pNext;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions <= 0) return;

  pthread_mutex_lock(&pObj->mutex);

  if (pObj->hashList) {
    for (int i = 0; i < pObj->maxSessions; ++i) {
      pNode = pObj->hashList[i];
      while (pNode) {
        pNext = pNode->next;
        if (fp != NULL) fp(pNode->data);
        free(pNode);
        pNode = pNext;
      }
    }

    free(pObj->hashList);
  }

  pthread_mutex_unlock(&pObj->mutex);

  pthread_mutex_destroy(&pObj->mutex);

  memset(pObj, 0, sizeof(SHashObj));
  free(pObj);
}

void taosCleanUpStrHash(void *handle) { taosCleanUpStrHashWithFp(handle, NULL); }

char *taosVisitStrHashWithFp(void *handle, int (*fp)(char *)) {
  SHashObj * pObj;
  SHashNode *pNode, *pNext;
  char *     pData = NULL;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions <= 0) return NULL;

  pthread_mutex_lock(&pObj->mutex);

  if (pObj->hashList) {
    for (int i = 0; i < pObj->maxSessions; ++i) {
      pNode = pObj->hashList[i];
      while (pNode) {
        pNext = pNode->next;
        int flag = fp(pNode->data);
        if (flag) {
          pData = pNode->data;
          goto VisitEnd;
        }

        pNode = pNext;
      }
    }
  }

VisitEnd:

  pthread_mutex_unlock(&pObj->mutex);
  return pData;
}
