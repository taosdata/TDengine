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
//TODO:replace by an abstract file layer
#include "osDir.h"
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#define TQ_META_NAME "tq.meta"
#define TQ_IDX_NAME  "tq.idx"


static int32_t tqHandlePutCommitted(TqMetaStore*, int64_t key, void* value);
static void*   tqHandleGetUncommitted(TqMetaStore*, int64_t key);

typedef struct TqMetaPageBuf {
  int16_t offset;
  char buffer[TQ_PAGE_SIZE];
} TqMetaPageBuf;

TqMetaStore* tqStoreOpen(const char* path,
    int serializer(const void* pObj, void** ppBytes),
    const void* deserializer(const void* pBytes, void** ppObj),
    void deleter(void* pObj)) {
  TqMetaStore* pMeta = malloc(sizeof(TqMetaStore)); 
  if(pMeta == NULL) {
    //close
    return NULL;
  }
  memset(pMeta, 0, sizeof(TqMetaStore));

  //concat data file name and index file name
  size_t pathLen = strlen(path);
  pMeta->dirPath = malloc(pathLen+1);
  if(pMeta->dirPath != NULL) {
    //TODO: memory insufficient
  }
  strcpy(pMeta->dirPath, path);
  
  char name[pathLen+10];

  strcpy(name, path);
  if(!taosDirExist(name) && !taosMkDir(name)) {
    ASSERT(false);
  }
  strcat(name, "/" TQ_IDX_NAME);
  int idxFd = open(name, O_RDWR | O_CREAT, 0755);
  if(idxFd < 0) {
    ASSERT(false);
    //close file
    //free memory
    return NULL;
  }

  pMeta->idxFd = idxFd;
  pMeta->unpersistHead = malloc(sizeof(TqMetaList));
  if(pMeta->unpersistHead == NULL) {
    ASSERT(false);
    //close file
    //free memory
    return NULL;
  }
  memset(pMeta->unpersistHead, 0, sizeof(TqMetaList));
  pMeta->unpersistHead->unpersistNext
    = pMeta->unpersistHead->unpersistPrev
    = pMeta->unpersistHead;

  strcpy(name, path);
  strcat(name, "/" TQ_META_NAME);
  int fileFd = open(name, O_RDWR | O_CREAT, 0755);
  if(fileFd < 0){
    ASSERT(false);
    return NULL;
  }

  pMeta->fileFd = fileFd;
  
  pMeta->serializer = serializer;
  pMeta->deserializer = deserializer;
  pMeta->deleter = deleter;

  //read idx file and load into memory
  char idxBuf[TQ_PAGE_SIZE];
  char* dataBuf = malloc(TQ_PAGE_SIZE);
  if(dataBuf == NULL) {
    //TODO:memory insufficient
  }
  int dataBufSize = TQ_PAGE_SIZE;
  int idxRead, dataReadSize;
  while((idxRead = read(idxFd, idxBuf, TQ_PAGE_SIZE))) {
    if(idxRead == -1) {
      //TODO: handle error
      ASSERT(false);
    }
    //loop read every entry
    for(int i = 0; i < idxRead; i += TQ_IDX_ENTRY_SIZE) {
      TqMetaList *pNode = malloc(sizeof(TqMetaList));
      if(pNode == NULL) {
        //TODO: free memory and return error
      }
      memset(pNode, 0, sizeof(TqMetaList));
      memcpy(&pNode->handle, &idxBuf[i], TQ_IDX_ENTRY_SIZE);
      lseek(fileFd, pNode->handle.offset, SEEK_CUR);
      if(dataBufSize < pNode->handle.serializedSize) {
        void *ptr = realloc(dataBuf, pNode->handle.serializedSize);
        if(ptr == NULL) {
          //TODO: memory insufficient
        }
        dataBuf = ptr;
        dataBufSize = pNode->handle.serializedSize;
      }
      if(read(fileFd, dataBuf, pNode->handle.serializedSize) != pNode->handle.serializedSize) {
        //TODO: read error
      }
      pMeta->deserializer(dataBuf, &pNode->handle.valueInUse);

      //put into list
      int bucketKey = pNode->handle.key & TQ_BUCKET_SIZE;
      pNode->next = pMeta->bucket[bucketKey];
      pMeta->bucket[bucketKey] = pNode;
    }
  }
  free(dataBuf);
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
    TqMetaList* pNode = pMeta->bucket[i];
    pMeta->bucket[i] = NULL;
    while(pNode) {
      ASSERT(pNode->unpersistNext == NULL);
      ASSERT(pNode->unpersistPrev == NULL);
      if(pNode->handle.valueInTxn) {
        pMeta->deleter(pNode->handle.valueInTxn);
      }
      if(pNode->handle.valueInUse) {
        pMeta->deleter(pNode->handle.valueInUse);
      }
      TqMetaList* next = pNode->next;
      free(pNode);
      pNode = next;
    }
  }
  free(pMeta->dirPath);
  free(pMeta->unpersistHead);
  free(pMeta);
  return 0;
}

int32_t tqStoreDelete(TqMetaStore* pMeta) {
  close(pMeta->fileFd);
  close(pMeta->idxFd);
  //free memory
  for(int i = 0; i < TQ_BUCKET_SIZE; i++) {
    TqMetaList* pNode = pMeta->bucket[i];
    pMeta->bucket[i] = NULL;
    while(pNode) {
      if(pNode->handle.valueInTxn) {
        pMeta->deleter(pNode->handle.valueInTxn);
      }
      if(pNode->handle.valueInUse) {
        pMeta->deleter(pNode->handle.valueInUse);
      }
      TqMetaList* next = pNode->next;
      free(pNode);
      pNode = next;
    }
  }
  free(pMeta->unpersistHead);
  taosRemoveDir(pMeta->dirPath);
  free(pMeta->dirPath);
  free(pMeta);
  return 0;
}

//TODO: wrap in tfile
int32_t tqStorePersist(TqMetaStore* pMeta) {
  char writeBuf[TQ_PAGE_SIZE];
  int64_t* bufPtr = (int64_t*)writeBuf;
  TqMetaList *pHead = pMeta->unpersistHead;
  TqMetaList *pNode = pHead->unpersistNext;
  while(pHead != pNode) {
    if(pNode->handle.valueInUse == NULL) {
      //put delete token in data file
      uint32_t delete = TQ_ACTION_DELETE;
      int nBytes = write(pMeta->fileFd, &delete, sizeof(uint32_t));
      ASSERT(nBytes == sizeof(uint32_t));

      //remove from list
      int bucketKey = pNode->handle.key & TQ_BUCKET_SIZE;
      TqMetaList* pBucketHead = pMeta->bucket[bucketKey];
      if(pBucketHead == pNode) {
        pMeta->bucket[bucketKey] = pBucketHead->next;
      } else {
        TqMetaList* pBucketNode = pBucketHead;
        while(pBucketNode->next != NULL
            && pBucketNode->next != pNode) {
          pBucketNode = pBucketNode->next; 
        }
        if(pBucketNode->next != NULL) {
          ASSERT(pBucketNode->next == pNode);
          pBucketNode->next = pNode->next;
          if(pNode->handle.valueInUse) {
            pMeta->deleter(pNode->handle.valueInUse);
            pNode->handle.valueInUse = NULL;
          }
          free(pNode);
        }
      }
    } else {
      //TODO: do not allocate each time
      //serialize
      void* pBytes = NULL;
      int sz = pMeta->serializer(pNode->handle.valueInUse, &pBytes);
      ASSERT(pBytes != NULL);
      //get current offset
      //append data
      int64_t offset = lseek(pMeta->fileFd, 0, SEEK_CUR);
      int nBytes = write(pMeta->fileFd, pBytes, sz);
      free(pBytes);
      //TODO: handle error in tfile
      ASSERT(nBytes == sz);

      pNode->handle.offset = offset;
      pNode->handle.serializedSize = sz;

      //write idx
      //TODO: endian check and convert
      *(bufPtr++) = pNode->handle.key;
      *(bufPtr++) = pNode->handle.offset;
      *(bufPtr++) = (int64_t)sz;
      if((char*)(bufPtr + 3) > writeBuf + TQ_PAGE_SIZE) {
        nBytes = write(pMeta->idxFd, writeBuf, sizeof(writeBuf));
        //TODO: handle error in tfile
        ASSERT(nBytes == sizeof(writeBuf));
        memset(writeBuf, 0, TQ_PAGE_SIZE);
        bufPtr = (int64_t*)writeBuf;
      }
    }

    //remove from unpersist list
    pHead->unpersistNext = pNode->unpersistNext;
    pHead->unpersistNext->unpersistPrev = pHead;

    pNode->unpersistPrev = pNode->unpersistNext = NULL;
    pNode = pHead->unpersistNext;
  }
  //write left bytes
  if((char*)bufPtr != writeBuf) {
    int used = (char*)bufPtr - writeBuf;
    int nBytes = write(pMeta->idxFd, writeBuf, used);
    //TODO: handle error in tfile
    ASSERT(nBytes == used);
  }
  //TODO: using fsync in tfile
  fsync(pMeta->idxFd);
  fsync(pMeta->fileFd);
  return 0;
}

static int32_t tqHandlePutCommitted(TqMetaStore* pMeta, int64_t key, void* value) {
  int64_t bucketKey = key & TQ_BUCKET_SIZE;
  TqMetaList* pNode = pMeta->bucket[bucketKey];
  while(pNode) {
    if(pNode->handle.key == key) {
      //TODO: think about thread safety
      if(pNode->handle.valueInUse) {
        pMeta->deleter(pNode->handle.valueInUse);
      }
      //change pointer ownership
      pNode->handle.valueInUse = value;
      return 0;
    } else {
      pNode = pNode->next;
    }
  }
  TqMetaList *pNewNode = malloc(sizeof(TqMetaList));
  if(pNewNode == NULL) {
    //TODO: memory error
    return -1;
  }
  memset(pNewNode, 0, sizeof(TqMetaList));
  pNewNode->handle.key = key;
  pNewNode->handle.valueInUse = value;
  //put into unpersist list
  pNewNode->unpersistPrev = pMeta->unpersistHead;
  pNewNode->unpersistNext = pMeta->unpersistHead->unpersistNext;
  pMeta->unpersistHead->unpersistNext->unpersistPrev = pNewNode;
  pMeta->unpersistHead->unpersistNext = pNewNode;
  return 0;
}

void* tqHandleGet(TqMetaStore* pMeta, int64_t key) {
  int64_t bucketKey = key & TQ_BUCKET_SIZE;
  TqMetaList* pNode = pMeta->bucket[bucketKey];
  while(pNode) {
    if(pNode->handle.key == key) {
      if(pNode->handle.valueInUse != NULL) {
        return pNode->handle.valueInUse;
      } else {
        return NULL;
      }
    } else {
      pNode = pNode->next;
    }
  }
  return NULL;
}

int32_t tqHandleMovePut(TqMetaStore* pMeta, int64_t key, void* value) {
  int64_t bucketKey = key & TQ_BUCKET_SIZE;
  TqMetaList* pNode = pMeta->bucket[bucketKey];
  while(pNode) {
    if(pNode->handle.key == key) {
      //TODO: think about thread safety
      if(pNode->handle.valueInTxn) {
        pMeta->deleter(pNode->handle.valueInTxn);
      }
      //change pointer ownership
      pNode->handle.valueInTxn = value;
      return 0;
    } else {
      pNode = pNode->next;
    }
  }
  TqMetaList *pNewNode = malloc(sizeof(TqMetaList));
  if(pNewNode == NULL) {
    //TODO: memory error
    return -1;
  }
  memset(pNewNode, 0, sizeof(TqMetaList));
  pNewNode->handle.key = key;
  pNewNode->handle.valueInTxn = value;
  pNewNode->next = pMeta->bucket[bucketKey];
  pMeta->bucket[bucketKey] = pNewNode;
  return 0;
}

int32_t tqHandleCopyPut(TqMetaStore* pMeta, int64_t key, void* value, size_t vsize) {
  void *vmem = malloc(vsize);
  if(vmem == NULL) {
    //TODO: memory error
    return -1;
  }
  memcpy(vmem, value, vsize);
  int64_t bucketKey = key & TQ_BUCKET_SIZE;
  TqMetaList* pNode = pMeta->bucket[bucketKey];
  while(pNode) {
    if(pNode->handle.key == key) {
      //TODO: think about thread safety
      if(pNode->handle.valueInTxn) {
        pMeta->deleter(pNode->handle.valueInTxn);
      }
      //change pointer ownership
      pNode->handle.valueInTxn = vmem;
      return 0;
    } else {
      pNode = pNode->next;
    }
  }
  TqMetaList *pNewNode = malloc(sizeof(TqMetaList));
  if(pNewNode == NULL) {
    //TODO: memory error
    return -1;
  }
  memset(pNewNode, 0, sizeof(TqMetaList));
  pNewNode->handle.key = key;
  pNewNode->handle.valueInTxn = vmem;
  pNewNode->next = pMeta->bucket[bucketKey];
  pMeta->bucket[bucketKey] = pNewNode;
  return 0;
}

static void* tqHandleGetUncommitted(TqMetaStore* pMeta, int64_t key) {
  int64_t bucketKey = key & TQ_BUCKET_SIZE;
  TqMetaList* pNode = pMeta->bucket[bucketKey];
  while(pNode) {
    if(pNode->handle.key == key) {
      if(pNode->handle.valueInTxn != NULL) {
        return pNode->handle.valueInTxn;
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
      if(pNode->handle.valueInUse) {
        pMeta->deleter(pNode->handle.valueInUse);
      }
      pNode->handle.valueInUse = pNode->handle.valueInTxn;
      pNode->handle.valueInTxn = NULL;
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
      pMeta->deleter(pNode->handle.valueInTxn);
      pNode->handle.valueInTxn = NULL;
      return 0;
    } else {
      pNode = pNode->next;
    }
  }
  //no such key
  return -1;
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
