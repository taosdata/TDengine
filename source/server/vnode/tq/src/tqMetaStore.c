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
#include "tqMetaStore.h"
//TODO:replace by a abstract file layer
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#define TQ_PAGE_SIZE 4096
#define TQ_META_NAME "tq.meta"
#define TQ_IDX_NAME  "tq.idx"


static int32_t       tqHandlePutCommitted(TqMetaStore*, int64_t key, void* value);
static TqMetaHandle* tqHandleGetUncommitted(TqMetaStore*, int64_t key);

typedef struct TqMetaPageBuf {
  int16_t offset;
  char buffer[TQ_PAGE_SIZE];
} TqMetaPageBuf;

TqMetaStore* tqStoreOpen(const char* path,
    int serializer(TqGroupHandle*, void**),
    const void* deserializer(const void*, TqGroupHandle*),
    void deleter(void*)) {
  //concat data file name and index file name
  size_t pathLen = strlen(path);
  char name[pathLen+10];
  strcpy(name, path);
  strcat(name, "/" TQ_META_NAME);
  int fileFd = open(name, O_WRONLY | O_CREAT | O_EXCL, 0755);
  if(fileFd < 0) return NULL;
  TqMetaStore* pMeta = malloc(sizeof(TqMetaStore)); 
  if(pMeta == NULL) {
    //close
    return NULL;
  }
  memset(pMeta, 0, sizeof(TqMetaStore));
  pMeta->fileFd = fileFd;
  
  strcpy(name, path);
  strcat(name, "/" TQ_IDX_NAME);
  int idxFd = open(name, O_WRONLY | O_CREAT | O_EXCL, 0755);
  if(idxFd < 0) {
    //close file
    //free memory
    return NULL;
  }
  pMeta->idxFd = idxFd;
  pMeta->unpersistHead = malloc(sizeof(TqMetaList));
  if(pMeta->unpersistHead == NULL) {
    //close file
    //free memory
    return NULL;
  }
  pMeta->serializer = serializer;
  pMeta->deserializer = deserializer;
  pMeta->deleter = deleter;
  return pMeta;
}

int32_t tqStoreClose(TqMetaStore* pMeta) {
  //commit data and idx
  tqStorePersist(pMeta);
  ASSERT(pMeta->unpersistHead && pMeta->unpersistHead->next==NULL);
  close(pMeta->fileFd);
  close(pMeta->idxFd);
  //free memory
  for(int i = 0; i < TQ_BUCKET_SIZE; i++) {
    TqMetaList* node = pMeta->bucket[i];
    pMeta->bucket[i] = NULL;
    while(node) {
      ASSERT(node->unpersistNext == NULL);
      ASSERT(node->unpersistPrev == NULL);
      if(node->handle.valueInTxn) {
        pMeta->deleter(node->handle.valueInTxn);
      }
      if(node->handle.valueInUse) {
        pMeta->deleter(node->handle.valueInUse);
      }
      TqMetaList* next = node->next;
      free(node);
      node = next;
    }
  }
  free(pMeta);
  return 0;
}

int32_t tqStoreDelete(TqMetaStore* pMeta) {
  //close file
  //delete file
  //free memory
  return 0;
}

int32_t tqStorePersist(TqMetaStore* pMeta) {
  int64_t idxBuf[3];
  TqMetaList *pHead = pMeta->unpersistHead;
  TqMetaList *pNode = pHead->unpersistNext;
  while(pHead != pNode) {
    ASSERT(pNode->handle.valueInUse != NULL);
    //serialize
    void* pBytes = NULL;
    int sz = pMeta->serializer(pNode->handle.valueInUse, &pBytes);
    ASSERT(pBytes != NULL);
    //get current offset
    //append data
    int nBytes = write(pMeta->fileFd, pBytes, sz);
    //TODO: handle error in tfile
    ASSERT(nBytes == sz);

    //write idx
    //TODO: endian check and convert
    idxBuf[0] = pNode->handle.key;
    idxBuf[1] = pNode->handle.offset;
    idxBuf[2] = (int64_t)sz;
    nBytes = write(pMeta->idxFd, idxBuf, sizeof(idxBuf));
    //TODO: handle error in tfile
    ASSERT(nBytes == sizeof(idxBuf));

    //remove from unpersist list
    pHead->unpersistNext = pNode->unpersistNext;
    pHead->unpersistNext->unpersistPrev = pHead;

    pNode->unpersistPrev = pNode->unpersistNext = NULL;
    pNode = pHead->unpersistNext;
  }
  //TODO:fsync and return upper layer
  return 0;
}

static int32_t tqHandlePutCommitted(TqMetaStore* pMeta, int64_t key, void* value) {
  int64_t bucketKey = key & TQ_BUCKET_SIZE;
  TqMetaList* pNode = pMeta->bucket[bucketKey];
  while(pNode) {
    if(pNode->handle.key == key) {
      //TODO: think about thread safety
      pMeta->deleter(pNode->handle.valueInUse);
      //change pointer ownership
      pNode->handle.valueInUse = value;
    } else {
      pNode = pNode->next;
    }
  }
  return 0;
}

TqMetaHandle* tqHandleGet(TqMetaStore* pMeta, int64_t key) {
  int64_t bucketKey = key & TQ_BUCKET_SIZE;
  TqMetaList* pNode = pMeta->bucket[bucketKey];
  while(pNode) {
    if(pNode->handle.key == key) {
      if(pNode->handle.valueInUse != NULL) {
        return &pNode->handle;
      } else {
        return NULL;
      }
    } else {
      pNode = pNode->next;
    }
  }
  return NULL;
}

int32_t tqHandlePut(TqMetaStore* pMeta, int64_t key, void* value) {
  int64_t bucketKey = key & TQ_BUCKET_SIZE;
  TqMetaList* pNode = pMeta->bucket[bucketKey];
  while(pNode) {
    if(pNode->handle.key == key) {
      //TODO: think about thread safety
      pMeta->deleter(pNode->handle.valueInTxn);
      //change pointer ownership
      pNode->handle.valueInTxn = value;
    } else {
      pNode = pNode->next;
    }
  }
  return 0;
}

static TqMetaHandle* tqHandleGetUncommitted(TqMetaStore* pMeta, int64_t key) {
  int64_t bucketKey = key & TQ_BUCKET_SIZE;
  TqMetaList* pNode = pMeta->bucket[bucketKey];
  while(pNode) {
    if(pNode->handle.key == key) {
      if(pNode->handle.valueInTxn != NULL) {
        return &pNode->handle;
      } else {
        return NULL;
      }
    } else {
      pNode = pNode->next;
    }
  }
  return NULL;
}

int32_t tqHandleCommit(TqMetaStore* pMeta, int64_t key) {
  int64_t bucketKey = key & TQ_BUCKET_SIZE;
  TqMetaList* pNode = pMeta->bucket[bucketKey];
  while(pNode) {
    if(pNode->handle.key == key) {
      if(pNode->handle.valueInUse != NULL) {
        pMeta->deleter(pNode->handle.valueInUse);
      }
      pNode->handle.valueInUse = pNode->handle.valueInTxn;
      if(pNode->unpersistNext == NULL) {
        pNode->unpersistNext = pMeta->unpersistHead->unpersistNext;
        pNode->unpersistPrev = pMeta->unpersistHead;
        pMeta->unpersistHead->unpersistNext->unpersistPrev = pNode;
        pMeta->unpersistHead->unpersistNext = pNode;
      }
      return 0;
    } else {
      pNode = pNode->next;
    }
  }
  return -1;
}

int32_t tqHandleAbort(TqMetaStore* pMeta, int64_t key) {
  int64_t bucketKey = key & TQ_BUCKET_SIZE;
  TqMetaList* pNode = pMeta->bucket[bucketKey];
  while(pNode) {
    if(pNode->handle.key == key) {
      if(pNode->handle.valueInTxn != NULL) {
        pMeta->deleter(pNode->handle.valueInTxn);
        pNode->handle.valueInTxn = NULL;
        return 0;
      }
      return -1;
    } else {
      pNode = pNode->next;
    }
  }
  return -2;
}

int32_t tqHandleDel(TqMetaStore* pMeta, int64_t key) {
  int64_t bucketKey = key & TQ_BUCKET_SIZE;
  TqMetaList* pNode = pMeta->bucket[bucketKey];
  while(pNode) {
    if(pNode->handle.key == key) {
      if(pNode->handle.valueInUse != NULL) {
        pMeta->deleter(pNode->handle.valueInUse);
        pNode->handle.valueInUse = NULL;
        //if not in unpersist, put into unpersist
        if(pNode->unpersistNext == NULL) {
          pNode->unpersistNext = pMeta->unpersistHead->unpersistNext;
          pNode->unpersistPrev = pMeta->unpersistHead;
          pMeta->unpersistHead->unpersistNext->unpersistPrev = pNode;
          pMeta->unpersistHead->unpersistNext = pNode;
        }
        return 0;
      }
      return -1;
    } else {
      pNode = pNode->next;
    }
  }
  return -2;
}

int32_t tqHandleClear(TqMetaStore* pMeta, int64_t key) {
  int64_t bucketKey = key & TQ_BUCKET_SIZE;
  TqMetaList* pNode = pMeta->bucket[bucketKey];
  bool exist = false;
  while(pNode) {
    if(pNode->handle.key == key) {
      if(pNode->handle.valueInUse != NULL) {
        exist = true;
        pMeta->deleter(pNode->handle.valueInUse);
        pNode->handle.valueInUse = NULL;
      }
      if(pNode->handle.valueInTxn != NULL) {
        exist = true;
        pMeta->deleter(pNode->handle.valueInTxn);
        pNode->handle.valueInTxn = NULL;
      }
      if(exist) {
        if(pNode->unpersistNext == NULL) {
          pNode->unpersistNext = pMeta->unpersistHead->unpersistNext;
          pNode->unpersistPrev = pMeta->unpersistHead;
          pMeta->unpersistHead->unpersistNext->unpersistPrev = pNode;
          pMeta->unpersistHead->unpersistNext = pNode;
        }
        return 0;
      }
      return -1;
    } else {
      pNode = pNode->next;
    }
  }
  return -2;
}
