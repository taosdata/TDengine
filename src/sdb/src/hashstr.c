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

#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "tsdb.h"

#define MAX_STR_LEN 40

typedef struct _str_node_t {
  char                string[TSDB_METER_ID_LEN];
  int                 hash;
  struct _str_node_t *prev;
  struct _str_node_t *next;
  char                data[];
} SHashNode;

typedef struct {
  SHashNode **hashList;
  int         maxSessions;
  int         dataSize;
} SHashObj;

int sdbHashString(void *handle, char *string) {
  SHashObj *   pObj = (SHashObj *)handle;
  unsigned int hash = 0, hashv;
  char *       c;
  int          len = strlen(string);

  c = string;

  while (len >= 4) {
    hash += *((int *)c);
    c += 4;
    len -= 4;
  }

  while (len > 0) {
    hash += *c;
    c++;
    len--;
  }

  hashv = hash / pObj->maxSessions;
  hash = (hashv + hash % pObj->maxSessions) % pObj->maxSessions;

  return hash;
}

void *sdbAddStrHash(void *handle, void *key, void *pData) {
  int        hash;
  SHashNode *pNode;
  SHashObj * pObj;
  char *     string = (char *)key;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return NULL;

  hash = sdbHashString(pObj, string);

  int size = sizeof(SHashNode) + pObj->dataSize;
  pNode = (SHashNode *)malloc(size);
  memset(pNode, 0, size);
  strcpy(pNode->string, string);
  memcpy(pNode->data, pData, pObj->dataSize);
  pNode->prev = 0;
  pNode->next = pObj->hashList[hash];
  pNode->hash = hash;

  if (pObj->hashList[hash] != 0) (pObj->hashList[hash])->prev = pNode;
  pObj->hashList[hash] = pNode;

  return pNode->data;
}

void sdbDeleteStrHash(void *handle, void *key) {
  int        hash;
  SHashNode *pNode;
  SHashObj * pObj;
  char *     string = (char *)key;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return;

  hash = sdbHashString(pObj, string);
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

    memset(pNode, 0, sizeof(SHashNode));
    free(pNode);
  }
}

void *sdbGetStrHashData(void *handle, void *key) {
  int        hash;
  SHashNode *pNode;
  SHashObj * pObj;
  char *     string = (char *)key;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return NULL;

  hash = sdbHashString(pObj, string);
  pNode = pObj->hashList[hash];

  while (pNode) {
    if (strcmp(pNode->string, string) == 0) {
      break;
    }
    pNode = pNode->next;
  }

  if (pNode) return pNode->data;

  return NULL;
}

void *sdbOpenStrHash(int maxSessions, int dataSize) {
  SHashObj *pObj;

  pObj = (SHashObj *)malloc(sizeof(SHashObj));
  if (pObj == NULL) {
    return NULL;
  }

  memset(pObj, 0, sizeof(SHashObj));
  pObj->maxSessions = maxSessions;
  pObj->dataSize = dataSize;

  pObj->hashList = (SHashNode **)malloc(sizeof(SHashNode *) * maxSessions);
  if (pObj->hashList == NULL) {
    free(pObj);
    return NULL;
  }
  memset(pObj->hashList, 0, sizeof(SHashNode *) * maxSessions);

  return (void *)pObj;
}

void sdbCloseStrHash(void *handle) {
  SHashObj  *pObj;
  SHashNode *pNode, *pNext;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions <= 0) return;

  if (pObj->hashList) {
    for (int i = 0; i < pObj->maxSessions; ++i) {
      pNode = pObj->hashList[i];
      while (pNode) {
        pNext = pNode->next;
        free(pNode);
        pNode = pNext;
      }
    }

    free(pObj->hashList);
  }

  memset(pObj, 0, sizeof(SHashObj));
  free(pObj);
}

void *sdbFetchStrHashData(void *handle, void *ptr, void **ppMeta) {
  SHashObj  *pObj = (SHashObj *)handle;
  SHashNode *pNode = (SHashNode *)ptr;
  int        hash = 0;

  *ppMeta = NULL;
  if (pObj == NULL || pObj->maxSessions <= 0) return NULL;
  if (pObj->hashList == NULL) return NULL;

  if (pNode) {
    hash = pNode->hash + 1;
    pNode = pNode->next;
  }

  if (pNode == NULL) {
    for (int i = hash; i < pObj->maxSessions; ++i) {
      pNode = pObj->hashList[i];
      if (pNode) break;
    }
  }

  if (pNode) *ppMeta = pNode->data;

  return pNode;
}
