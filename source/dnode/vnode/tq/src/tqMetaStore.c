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

static inline void tqLinkUnpersist(TqMetaStore *pMeta, TqMetaList* pNode) {
  if(pNode->unpersistNext == NULL) {
    pNode->unpersistNext = pMeta->unpersistHead->unpersistNext;
    pNode->unpersistPrev = pMeta->unpersistHead;
    pMeta->unpersistHead->unpersistNext->unpersistPrev = pNode;
    pMeta->unpersistHead->unpersistNext = pNode;
  }
}

static inline int tqSeekLastPage(int fd) {
  int offset = lseek(fd, 0, SEEK_END);
  int pageNo = offset / TQ_PAGE_SIZE;
  int curPageOffset = pageNo * TQ_PAGE_SIZE;
  return lseek(fd, curPageOffset, SEEK_SET);
}

//TODO: the struct is tightly coupled with index entry
typedef struct TqIdxPageHead {
  int16_t writeOffset;
  int8_t  unused[14];
} TqIdxPageHead;

typedef struct TqIdxPageBuf {
  TqIdxPageHead head;
  char buffer[TQ_IDX_PAGE_BODY_SIZE];
} TqIdxPageBuf;

static inline int tqReadLastPage(int fd, TqIdxPageBuf* pBuf) {
  int offset = tqSeekLastPage(fd);
  int nBytes;
  if((nBytes = read(fd, pBuf, TQ_PAGE_SIZE)) == -1) {
    return -1;
  }
  if(nBytes == 0) {
    memset(pBuf, 0, TQ_PAGE_SIZE);
    pBuf->head.writeOffset = TQ_IDX_PAGE_HEAD_SIZE;
  }
  ASSERT(nBytes == 0 || nBytes == pBuf->head.writeOffset);

  return lseek(fd, offset, SEEK_SET);
}

TqMetaStore* tqStoreOpen(const char* path,
    int serializer(const void* pObj, TqSerializedHead** ppHead),
    const void* deserializer(const TqSerializedHead* pHead, void** ppObj),
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
  /*char idxBuf[TQ_PAGE_SIZE];*/
  TqIdxPageBuf idxBuf;
  TqSerializedHead* serializedObj = malloc(TQ_PAGE_SIZE);
  if(serializedObj == NULL) {
    //TODO:memory insufficient
  }
  int idxRead;
  int allocated = TQ_PAGE_SIZE;
  bool readEnd = false;
  while((idxRead = read(idxFd, &idxBuf, TQ_PAGE_SIZE))) {
    if(idxRead == -1) {
      //TODO: handle error
      ASSERT(false);
    }
    ASSERT(idxBuf.head.writeOffset == idxRead);
    //loop read every entry
    for(int i = 0; i < idxBuf.head.writeOffset - TQ_IDX_PAGE_HEAD_SIZE; i += TQ_IDX_SIZE) {
      TqMetaList *pNode = malloc(sizeof(TqMetaList));
      if(pNode == NULL) {
        //TODO: free memory and return error
      }
      memset(pNode, 0, sizeof(TqMetaList));
      memcpy(&pNode->handle, &idxBuf.buffer[i], TQ_IDX_SIZE);

      lseek(fileFd, pNode->handle.offset, SEEK_SET);
      if(allocated < pNode->handle.serializedSize) {
        void *ptr = realloc(serializedObj, pNode->handle.serializedSize);
        if(ptr == NULL) {
          //TODO: memory insufficient
        }
        serializedObj = ptr;
        allocated = pNode->handle.serializedSize;
      }
      serializedObj->ssize = pNode->handle.serializedSize;
      if(read(fileFd, serializedObj, pNode->handle.serializedSize) != pNode->handle.serializedSize) {
        //TODO: read error
      }
      if(serializedObj->action == TQ_ACTION_INUSE) {
        if(serializedObj->ssize != sizeof(TqSerializedHead)) {
          pMeta->deserializer(serializedObj, &pNode->handle.valueInUse);
        } else {
          pNode->handle.valueInUse = TQ_DELETE_TOKEN;
        }
      } else if(serializedObj->action == TQ_ACTION_INTXN) {
        if(serializedObj->ssize != sizeof(TqSerializedHead)) {
          pMeta->deserializer(serializedObj, &pNode->handle.valueInTxn);
        } else {
          pNode->handle.valueInTxn = TQ_DELETE_TOKEN;
        }
      } else if(serializedObj->action == TQ_ACTION_INUSE_CONT) {
        if(serializedObj->ssize != sizeof(TqSerializedHead)) {
          pMeta->deserializer(serializedObj, &pNode->handle.valueInUse);
        } else {
          pNode->handle.valueInUse = TQ_DELETE_TOKEN;
        }
        TqSerializedHead* ptr = POINTER_SHIFT(serializedObj, serializedObj->ssize);
        if(ptr->ssize != sizeof(TqSerializedHead)) {
          pMeta->deserializer(ptr, &pNode->handle.valueInTxn);
        } else {
          pNode->handle.valueInTxn = TQ_DELETE_TOKEN;
        }
      } else {
        ASSERT(0);
      }

      //put into list
      int bucketKey = pNode->handle.key & TQ_BUCKET_SIZE;
      TqMetaList* pBucketNode = pMeta->bucket[bucketKey];
      if(pBucketNode == NULL) {
        pMeta->bucket[bucketKey] = pNode;
      } else if(pBucketNode->handle.key == pNode->handle.key) {
        pNode->next = pBucketNode->next;
        pMeta->bucket[bucketKey] = pNode;
      } else {
        while(pBucketNode->next &&
            pBucketNode->next->handle.key == pNode->handle.key) {
          pBucketNode = pBucketNode->next; 
        }
        if(pBucketNode->next) {
          ASSERT(pBucketNode->next->handle.key == pNode->handle.key);
          TqMetaList *pNodeTmp = pBucketNode->next;
          pBucketNode->next = pNodeTmp->next;
          pBucketNode = pNodeTmp;
        } else {
          pBucketNode = NULL;
        }
      }
      if(pBucketNode) {
        if(pBucketNode->handle.valueInUse
            && pBucketNode->handle.valueInUse != TQ_DELETE_TOKEN) {
          pMeta->deleter(pBucketNode->handle.valueInUse);
        }
        if(pBucketNode->handle.valueInTxn
            && pBucketNode->handle.valueInTxn != TQ_DELETE_TOKEN) {
          pMeta->deleter(pBucketNode->handle.valueInTxn);
        }
        free(pBucketNode);
      }
    }
  }
  free(serializedObj);
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
    while(pNode) {
      ASSERT(pNode->unpersistNext == NULL);
      ASSERT(pNode->unpersistPrev == NULL);
      if(pNode->handle.valueInTxn
          && pNode->handle.valueInTxn != TQ_DELETE_TOKEN) {
        pMeta->deleter(pNode->handle.valueInTxn);
      }
      if(pNode->handle.valueInUse
          && pNode->handle.valueInUse != TQ_DELETE_TOKEN) {
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
      if(pNode->handle.valueInTxn
          && pNode->handle.valueInTxn != TQ_DELETE_TOKEN) {
        pMeta->deleter(pNode->handle.valueInTxn);
      }
      if(pNode->handle.valueInUse
          && pNode->handle.valueInUse != TQ_DELETE_TOKEN) {
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
  TqIdxPageBuf idxBuf;
  int64_t* bufPtr = (int64_t*)idxBuf.buffer;
  TqMetaList *pHead = pMeta->unpersistHead;
  TqMetaList *pNode = pHead->unpersistNext;
  TqSerializedHead *pSHead = malloc(sizeof(TqSerializedHead));
  if(pSHead == NULL) {
    //TODO: memory error
    return -1;
  }
  pSHead->ver = TQ_SVER;
  pSHead->checksum = 0;
  pSHead->ssize = sizeof(TqSerializedHead);
  int allocatedSize = sizeof(TqSerializedHead);
  int offset = lseek(pMeta->fileFd, 0, SEEK_CUR);

  tqReadLastPage(pMeta->idxFd, &idxBuf);

  if(idxBuf.head.writeOffset == TQ_PAGE_SIZE) {
    lseek(pMeta->idxFd, 0, SEEK_END);
    memset(&idxBuf, 0, TQ_PAGE_SIZE);
    idxBuf.head.writeOffset = TQ_IDX_PAGE_HEAD_SIZE;
  } else {
    bufPtr = POINTER_SHIFT(&idxBuf, idxBuf.head.writeOffset);
  }

  while(pHead != pNode) {
    int nBytes = 0;

    if(pNode->handle.valueInUse) {
      if(pNode->handle.valueInTxn) {
        pSHead->action = TQ_ACTION_INUSE_CONT;
      } else {
        pSHead->action = TQ_ACTION_INUSE;
      }

      if(pNode->handle.valueInUse == TQ_DELETE_TOKEN) {
        pSHead->ssize = sizeof(TqSerializedHead);
      } else {
        pMeta->serializer(pNode->handle.valueInUse, &pSHead);
      }
      nBytes = write(pMeta->fileFd, pSHead, pSHead->ssize);
      ASSERT(nBytes == pSHead->ssize);
    }

    if(pNode->handle.valueInTxn) {
      pSHead->action = TQ_ACTION_INTXN;
      if(pNode->handle.valueInTxn == TQ_DELETE_TOKEN) {
        pSHead->ssize = sizeof(TqSerializedHead);
      } else {
        pMeta->serializer(pNode->handle.valueInTxn, &pSHead);
      }
      int nBytesTxn = write(pMeta->fileFd, pSHead, pSHead->ssize);
      ASSERT(nBytesTxn == pSHead->ssize);
      nBytes += nBytesTxn;
    }
    pNode->handle.offset = offset;
    offset += nBytes;

    //write idx file
    //TODO: endian check and convert
    *(bufPtr++) = pNode->handle.key;
    *(bufPtr++) = pNode->handle.offset;
    *(bufPtr++) = (int64_t)nBytes;
    idxBuf.head.writeOffset += TQ_IDX_SIZE;
    if(idxBuf.head.writeOffset >= TQ_PAGE_SIZE) {
      nBytes = write(pMeta->idxFd, &idxBuf, TQ_PAGE_SIZE);
      //TODO: handle error with tfile
      ASSERT(nBytes == TQ_PAGE_SIZE);
      memset(&idxBuf, 0, TQ_PAGE_SIZE);
      bufPtr = (int64_t*)&idxBuf.buffer;
    }
    //remove from unpersist list
    pHead->unpersistNext = pNode->unpersistNext;
    pHead->unpersistNext->unpersistPrev = pHead;
    pNode->unpersistPrev = pNode->unpersistNext = NULL;
    pNode = pHead->unpersistNext;

    //remove from bucket
    if(pNode->handle.valueInUse == TQ_DELETE_TOKEN &&
        pNode->handle.valueInTxn == NULL
        ) {
      int bucketKey = pNode->handle.key & TQ_BUCKET_SIZE;
      TqMetaList* pBucketHead = pMeta->bucket[bucketKey];
      if(pBucketHead == pNode) {
        pMeta->bucket[bucketKey] = pNode->next;
      } else {
        TqMetaList* pBucketNode = pBucketHead;
        while(pBucketNode->next != NULL
            && pBucketNode->next != pNode) {
          pBucketNode = pBucketNode->next; 
        }
        //impossible for pBucket->next == NULL
        ASSERT(pBucketNode->next == pNode);
        pBucketNode->next = pNode->next;
      }
      free(pNode);
    }
  }

  //write left bytes
  free(pSHead);
  //TODO: write new version in tfile
  if((char*)bufPtr != idxBuf.buffer) {
    int nBytes = write(pMeta->idxFd, &idxBuf, idxBuf.head.writeOffset);
    //TODO: handle error in tfile
    ASSERT(nBytes == idxBuf.head.writeOffset);
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
      if(pNode->handle.valueInUse
          && pNode->handle.valueInUse != TQ_DELETE_TOKEN) {
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
      if(pNode->handle.valueInUse != NULL
          && pNode->handle.valueInUse != TQ_DELETE_TOKEN) {
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
      if(pNode->handle.valueInTxn
          && pNode->handle.valueInTxn != TQ_DELETE_TOKEN) {
        pMeta->deleter(pNode->handle.valueInTxn);
      }
      //change pointer ownership
      pNode->handle.valueInTxn = value;
      tqLinkUnpersist(pMeta, pNode);
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
  tqLinkUnpersist(pMeta, pNewNode);
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
      if(pNode->handle.valueInTxn
          && pNode->handle.valueInTxn != TQ_DELETE_TOKEN) {
        pMeta->deleter(pNode->handle.valueInTxn);
      }
      //change pointer ownership
      pNode->handle.valueInTxn = vmem;
      tqLinkUnpersist(pMeta, pNode);
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
  tqLinkUnpersist(pMeta, pNewNode);
  return 0;
}

static void* tqHandleGetUncommitted(TqMetaStore* pMeta, int64_t key) {
  int64_t bucketKey = key & TQ_BUCKET_SIZE;
  TqMetaList* pNode = pMeta->bucket[bucketKey];
  while(pNode) {
    if(pNode->handle.key == key) {
      if(pNode->handle.valueInTxn != NULL
          && pNode->handle.valueInTxn != TQ_DELETE_TOKEN) {
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
      if(pNode->handle.valueInUse
          && pNode->handle.valueInUse != TQ_DELETE_TOKEN) {
        pMeta->deleter(pNode->handle.valueInUse);
      }
      pNode->handle.valueInUse = pNode->handle.valueInTxn;
      pNode->handle.valueInTxn = NULL;
      tqLinkUnpersist(pMeta, pNode);
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
      if(pNode->handle.valueInTxn) {
        if(pNode->handle.valueInTxn != TQ_DELETE_TOKEN) {
          pMeta->deleter(pNode->handle.valueInTxn);
        }
        pNode->handle.valueInTxn = NULL;
        tqLinkUnpersist(pMeta, pNode);
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
    if(pNode->handle.valueInTxn != TQ_DELETE_TOKEN) {
      if(pNode->handle.valueInTxn) {
        pMeta->deleter(pNode->handle.valueInTxn);
      }
      pNode->handle.valueInTxn = TQ_DELETE_TOKEN;
      tqLinkUnpersist(pMeta, pNode);
      return 0;
    } else {
      pNode = pNode->next;
    }
  }
  //no such key
  return -1;
}

//TODO: clean deleted idx and data from persistent file
int32_t tqStoreCompact(TqMetaStore *pMeta) {
  return 0;
}
