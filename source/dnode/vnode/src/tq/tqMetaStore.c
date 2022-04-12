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
#include "vnodeInt.h"
// TODO:replace by an abstract file layer
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include "osDir.h"

#define TQ_META_NAME "tq.meta"
#define TQ_IDX_NAME "tq.idx"

static int32_t tqHandlePutCommitted(STqMetaStore*, int64_t key, void* value);
static void*   tqHandleGetUncommitted(STqMetaStore*, int64_t key);

static inline void tqLinkUnpersist(STqMetaStore* pMeta, STqMetaList* pNode) {
  if (pNode->unpersistNext == NULL) {
    pNode->unpersistNext = pMeta->unpersistHead->unpersistNext;
    pNode->unpersistPrev = pMeta->unpersistHead;
    pMeta->unpersistHead->unpersistNext->unpersistPrev = pNode;
    pMeta->unpersistHead->unpersistNext = pNode;
  }
}

static inline int64_t tqSeekLastPage(TdFilePtr pFile) {
  int offset = taosLSeekFile(pFile, 0, SEEK_END);
  int pageNo = offset / TQ_PAGE_SIZE;
  int curPageOffset = pageNo * TQ_PAGE_SIZE;
  return taosLSeekFile(pFile, curPageOffset, SEEK_SET);
}

// TODO: the struct is tightly coupled with index entry
typedef struct STqIdxPageHead {
  int16_t writeOffset;
  int8_t  unused[14];
} STqIdxPageHead;

typedef struct STqIdxPageBuf {
  STqIdxPageHead head;
  char           buffer[TQ_IDX_PAGE_BODY_SIZE];
} STqIdxPageBuf;

static inline int tqReadLastPage(TdFilePtr pFile, STqIdxPageBuf* pBuf) {
  int offset = tqSeekLastPage(pFile);
  int nBytes;
  if ((nBytes = taosReadFile(pFile, pBuf, TQ_PAGE_SIZE)) == -1) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  if (nBytes == 0) {
    memset(pBuf, 0, TQ_PAGE_SIZE);
    pBuf->head.writeOffset = TQ_IDX_PAGE_HEAD_SIZE;
  }
  ASSERT(nBytes == 0 || nBytes == pBuf->head.writeOffset);

  return taosLSeekFile(pFile, offset, SEEK_SET);
}

STqMetaStore* tqStoreOpen(STQ* pTq, const char* path, FTqSerialize serializer, FTqDeserialize deserializer,
                          FTqDelete deleter, int32_t tqConfigFlag) {
  STqMetaStore* pMeta = taosMemoryCalloc(1, sizeof(STqMetaStore));
  if (pMeta == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
    return NULL;
  }
  pMeta->pTq = pTq;

  // concat data file name and index file name
  size_t pathLen = strlen(path);
  pMeta->dirPath = taosMemoryMalloc(pathLen + 1);
  if (pMeta->dirPath == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
    taosMemoryFree(pMeta);
    return NULL;
  }
  strcpy(pMeta->dirPath, path);

  char name[pathLen + 10];

  strcpy(name, path);
  if (!taosDirExist(name) && taosMkDir(name) != 0) {
    terrno = TSDB_CODE_TQ_FAILED_TO_CREATE_DIR;
    tqError("failed to create dir:%s since %s ", name, terrstr());
  }
  strcat(name, "/" TQ_IDX_NAME);
  TdFilePtr pIdxFile = taosOpenFile(name, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ);
  if (pIdxFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    tqError("failed to open file:%s since %s ", name, terrstr());
    // free memory
    return NULL;
  }

  pMeta->pIdxFile = pIdxFile;
  pMeta->unpersistHead = taosMemoryCalloc(1, sizeof(STqMetaList));
  if (pMeta->unpersistHead == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
    return NULL;
  }
  pMeta->unpersistHead->unpersistNext = pMeta->unpersistHead->unpersistPrev = pMeta->unpersistHead;

  strcpy(name, path);
  strcat(name, "/" TQ_META_NAME);
  TdFilePtr pFile = taosOpenFile(name, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    tqError("failed to open file:%s since %s", name, terrstr());
    return NULL;
  }

  pMeta->pFile = pFile;

  pMeta->pSerializer = serializer;
  pMeta->pDeserializer = deserializer;
  pMeta->pDeleter = deleter;
  pMeta->tqConfigFlag = tqConfigFlag;

  // read idx file and load into memory
  STqIdxPageBuf      idxBuf;
  STqSerializedHead* serializedObj = taosMemoryMalloc(TQ_PAGE_SIZE);
  if (serializedObj == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
  }
  int  idxRead;
  int  allocated = TQ_PAGE_SIZE;
  bool readEnd = false;
  while ((idxRead = taosReadFile(pIdxFile, &idxBuf, TQ_PAGE_SIZE))) {
    if (idxRead == -1) {
      // TODO: handle error
      terrno = TAOS_SYSTEM_ERROR(errno);
      tqError("failed to read tq index file since %s", terrstr());
    }
    ASSERT(idxBuf.head.writeOffset == idxRead);
    // loop read every entry
    for (int i = 0; i < idxBuf.head.writeOffset - TQ_IDX_PAGE_HEAD_SIZE; i += TQ_IDX_SIZE) {
      STqMetaList* pNode = taosMemoryCalloc(1, sizeof(STqMetaList));
      if (pNode == NULL) {
        terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
        // TODO: free memory
      }
      memcpy(&pNode->handle, &idxBuf.buffer[i], TQ_IDX_SIZE);

      taosLSeekFile(pFile, pNode->handle.offset, SEEK_SET);
      if (allocated < pNode->handle.serializedSize) {
        void* ptr = taosMemoryRealloc(serializedObj, pNode->handle.serializedSize);
        if (ptr == NULL) {
          terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
          // TODO: free memory
        }
        serializedObj = ptr;
        allocated = pNode->handle.serializedSize;
      }
      serializedObj->ssize = pNode->handle.serializedSize;
      if (taosReadFile(pFile, serializedObj, pNode->handle.serializedSize) != pNode->handle.serializedSize) {
        // TODO: read error
      }
      if (serializedObj->action == TQ_ACTION_INUSE) {
        if (serializedObj->ssize != sizeof(STqSerializedHead)) {
          pMeta->pDeserializer(pTq, serializedObj, &pNode->handle.valueInUse);
        } else {
          pNode->handle.valueInUse = TQ_DELETE_TOKEN;
        }
      } else if (serializedObj->action == TQ_ACTION_INTXN) {
        if (serializedObj->ssize != sizeof(STqSerializedHead)) {
          pMeta->pDeserializer(pTq, serializedObj, &pNode->handle.valueInTxn);
        } else {
          pNode->handle.valueInTxn = TQ_DELETE_TOKEN;
        }
      } else if (serializedObj->action == TQ_ACTION_INUSE_CONT) {
        if (serializedObj->ssize != sizeof(STqSerializedHead)) {
          pMeta->pDeserializer(pTq, serializedObj, &pNode->handle.valueInUse);
        } else {
          pNode->handle.valueInUse = TQ_DELETE_TOKEN;
        }
        STqSerializedHead* ptr = POINTER_SHIFT(serializedObj, serializedObj->ssize);
        if (ptr->ssize != sizeof(STqSerializedHead)) {
          pMeta->pDeserializer(pTq, ptr, &pNode->handle.valueInTxn);
        } else {
          pNode->handle.valueInTxn = TQ_DELETE_TOKEN;
        }
      } else {
        ASSERT(0);
      }

      // put into list
      int          bucketKey = pNode->handle.key & TQ_BUCKET_MASK;
      STqMetaList* pBucketNode = pMeta->bucket[bucketKey];
      if (pBucketNode == NULL) {
        pMeta->bucket[bucketKey] = pNode;
      } else if (pBucketNode->handle.key == pNode->handle.key) {
        pNode->next = pBucketNode->next;
        pMeta->bucket[bucketKey] = pNode;
      } else {
        while (pBucketNode->next && pBucketNode->next->handle.key != pNode->handle.key) {
          pBucketNode = pBucketNode->next;
        }
        if (pBucketNode->next) {
          ASSERT(pBucketNode->next->handle.key == pNode->handle.key);
          STqMetaList* pNodeFound = pBucketNode->next;
          pNode->next = pNodeFound->next;
          pBucketNode->next = pNode;
          pBucketNode = pNodeFound;
        } else {
          pNode->next = pMeta->bucket[bucketKey];
          pMeta->bucket[bucketKey] = pNode;
          pBucketNode = NULL;
        }
      }
      if (pBucketNode) {
        if (pBucketNode->handle.valueInUse && pBucketNode->handle.valueInUse != TQ_DELETE_TOKEN) {
          pMeta->pDeleter(pBucketNode->handle.valueInUse);
        }
        if (pBucketNode->handle.valueInTxn && pBucketNode->handle.valueInTxn != TQ_DELETE_TOKEN) {
          pMeta->pDeleter(pBucketNode->handle.valueInTxn);
        }
        taosMemoryFree(pBucketNode);
      }
    }
  }
  taosMemoryFree(serializedObj);
  return pMeta;
}

int32_t tqStoreClose(STqMetaStore* pMeta) {
  // commit data and idx
  tqStorePersist(pMeta);
  ASSERT(pMeta->unpersistHead && pMeta->unpersistHead->next == NULL);
  taosCloseFile(&pMeta->pFile);
  taosCloseFile(&pMeta->pIdxFile);
  // free memory
  for (int i = 0; i < TQ_BUCKET_SIZE; i++) {
    STqMetaList* pNode = pMeta->bucket[i];
    while (pNode) {
      ASSERT(pNode->unpersistNext == NULL);
      ASSERT(pNode->unpersistPrev == NULL);
      if (pNode->handle.valueInTxn && pNode->handle.valueInTxn != TQ_DELETE_TOKEN) {
        pMeta->pDeleter(pNode->handle.valueInTxn);
      }
      if (pNode->handle.valueInUse && pNode->handle.valueInUse != TQ_DELETE_TOKEN) {
        pMeta->pDeleter(pNode->handle.valueInUse);
      }
      STqMetaList* next = pNode->next;
      taosMemoryFree(pNode);
      pNode = next;
    }
  }
  taosMemoryFree(pMeta->dirPath);
  taosMemoryFree(pMeta->unpersistHead);
  taosMemoryFree(pMeta);
  return 0;
}

int32_t tqStoreDelete(STqMetaStore* pMeta) {
  taosCloseFile(&pMeta->pFile);
  taosCloseFile(&pMeta->pIdxFile);
  // free memory
  for (int i = 0; i < TQ_BUCKET_SIZE; i++) {
    STqMetaList* pNode = pMeta->bucket[i];
    pMeta->bucket[i] = NULL;
    while (pNode) {
      if (pNode->handle.valueInTxn && pNode->handle.valueInTxn != TQ_DELETE_TOKEN) {
        pMeta->pDeleter(pNode->handle.valueInTxn);
      }
      if (pNode->handle.valueInUse && pNode->handle.valueInUse != TQ_DELETE_TOKEN) {
        pMeta->pDeleter(pNode->handle.valueInUse);
      }
      STqMetaList* next = pNode->next;
      taosMemoryFree(pNode);
      pNode = next;
    }
  }
  taosMemoryFree(pMeta->unpersistHead);
  taosRemoveDir(pMeta->dirPath);
  taosMemoryFree(pMeta->dirPath);
  taosMemoryFree(pMeta);
  return 0;
}

int32_t tqStorePersist(STqMetaStore* pMeta) {
  STqIdxPageBuf      idxBuf;
  int64_t*           bufPtr = (int64_t*)idxBuf.buffer;
  STqMetaList*       pHead = pMeta->unpersistHead;
  STqMetaList*       pNode = pHead->unpersistNext;
  STqSerializedHead* pSHead = taosMemoryMalloc(sizeof(STqSerializedHead));
  if (pSHead == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
    return -1;
  }
  pSHead->ver = TQ_SVER;
  pSHead->checksum = 0;
  pSHead->ssize = sizeof(STqSerializedHead);
  /*int allocatedSize = sizeof(STqSerializedHead);*/
  int offset = taosLSeekFile(pMeta->pFile, 0, SEEK_CUR);

  tqReadLastPage(pMeta->pIdxFile, &idxBuf);

  if (idxBuf.head.writeOffset == TQ_PAGE_SIZE) {
    taosLSeekFile(pMeta->pIdxFile, 0, SEEK_END);
    memset(&idxBuf, 0, TQ_PAGE_SIZE);
    idxBuf.head.writeOffset = TQ_IDX_PAGE_HEAD_SIZE;
  } else {
    bufPtr = POINTER_SHIFT(&idxBuf, idxBuf.head.writeOffset);
  }

  while (pHead != pNode) {
    int nBytes = 0;

    if (pNode->handle.valueInUse) {
      if (pNode->handle.valueInTxn) {
        pSHead->action = TQ_ACTION_INUSE_CONT;
      } else {
        pSHead->action = TQ_ACTION_INUSE;
      }

      if (pNode->handle.valueInUse == TQ_DELETE_TOKEN) {
        pSHead->ssize = sizeof(STqSerializedHead);
      } else {
        pMeta->pSerializer(pNode->handle.valueInUse, &pSHead);
      }
      nBytes = taosWriteFile(pMeta->pFile, pSHead, pSHead->ssize);
      ASSERT(nBytes == pSHead->ssize);
    }

    if (pNode->handle.valueInTxn) {
      pSHead->action = TQ_ACTION_INTXN;
      if (pNode->handle.valueInTxn == TQ_DELETE_TOKEN) {
        pSHead->ssize = sizeof(STqSerializedHead);
      } else {
        pMeta->pSerializer(pNode->handle.valueInTxn, &pSHead);
      }
      int nBytesTxn = taosWriteFile(pMeta->pFile, pSHead, pSHead->ssize);
      ASSERT(nBytesTxn == pSHead->ssize);
      nBytes += nBytesTxn;
    }
    pNode->handle.offset = offset;
    offset += nBytes;

    // write idx file
    // TODO: endian check and convert
    *(bufPtr++) = pNode->handle.key;
    *(bufPtr++) = pNode->handle.offset;
    *(bufPtr++) = (int64_t)nBytes;
    idxBuf.head.writeOffset += TQ_IDX_SIZE;

    if (idxBuf.head.writeOffset >= TQ_PAGE_SIZE) {
      nBytes = taosWriteFile(pMeta->pIdxFile, &idxBuf, TQ_PAGE_SIZE);
      // TODO: handle error with tfile
      ASSERT(nBytes == TQ_PAGE_SIZE);
      memset(&idxBuf, 0, TQ_PAGE_SIZE);
      idxBuf.head.writeOffset = TQ_IDX_PAGE_HEAD_SIZE;
      bufPtr = (int64_t*)&idxBuf.buffer;
    }
    // remove from unpersist list
    pHead->unpersistNext = pNode->unpersistNext;
    pHead->unpersistNext->unpersistPrev = pHead;
    pNode->unpersistPrev = pNode->unpersistNext = NULL;
    pNode = pHead->unpersistNext;

    // remove from bucket
    if (pNode->handle.valueInUse == TQ_DELETE_TOKEN && pNode->handle.valueInTxn == NULL) {
      int          bucketKey = pNode->handle.key & TQ_BUCKET_MASK;
      STqMetaList* pBucketHead = pMeta->bucket[bucketKey];
      if (pBucketHead == pNode) {
        pMeta->bucket[bucketKey] = pNode->next;
      } else {
        STqMetaList* pBucketNode = pBucketHead;
        while (pBucketNode->next != NULL && pBucketNode->next != pNode) {
          pBucketNode = pBucketNode->next;
        }
        // impossible for pBucket->next == NULL
        ASSERT(pBucketNode->next == pNode);
        pBucketNode->next = pNode->next;
      }
      taosMemoryFree(pNode);
    }
  }

  // write left bytes
  taosMemoryFree(pSHead);
  // TODO: write new version in tfile
  if ((char*)bufPtr != idxBuf.buffer) {
    int nBytes = taosWriteFile(pMeta->pIdxFile, &idxBuf, idxBuf.head.writeOffset);
    // TODO: handle error in tfile
    ASSERT(nBytes == idxBuf.head.writeOffset);
  }
  // TODO: using fsync in tfile
  taosFsyncFile(pMeta->pIdxFile);
  taosFsyncFile(pMeta->pFile);
  return 0;
}

static int32_t tqHandlePutCommitted(STqMetaStore* pMeta, int64_t key, void* value) {
  int64_t      bucketKey = key & TQ_BUCKET_MASK;
  STqMetaList* pNode = pMeta->bucket[bucketKey];
  while (pNode) {
    if (pNode->handle.key == key) {
      if (pNode->handle.valueInUse && pNode->handle.valueInUse != TQ_DELETE_TOKEN) {
        pMeta->pDeleter(pNode->handle.valueInUse);
      }
      // change pointer ownership
      pNode->handle.valueInUse = value;
      return 0;
    } else {
      pNode = pNode->next;
    }
  }
  STqMetaList* pNewNode = taosMemoryCalloc(1, sizeof(STqMetaList));
  if (pNewNode == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
    return -1;
  }
  pNewNode->handle.key = key;
  pNewNode->handle.valueInUse = value;
  pNewNode->next = pMeta->bucket[bucketKey];
  // put into unpersist list
  pNewNode->unpersistPrev = pMeta->unpersistHead;
  pNewNode->unpersistNext = pMeta->unpersistHead->unpersistNext;
  pMeta->unpersistHead->unpersistNext->unpersistPrev = pNewNode;
  pMeta->unpersistHead->unpersistNext = pNewNode;
  return 0;
}

void* tqHandleGet(STqMetaStore* pMeta, int64_t key) {
  int64_t      bucketKey = key & TQ_BUCKET_MASK;
  STqMetaList* pNode = pMeta->bucket[bucketKey];
  while (pNode) {
    if (pNode->handle.key == key) {
      if (pNode->handle.valueInUse != NULL && pNode->handle.valueInUse != TQ_DELETE_TOKEN) {
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

void* tqHandleTouchGet(STqMetaStore* pMeta, int64_t key) {
  int64_t      bucketKey = key & TQ_BUCKET_MASK;
  STqMetaList* pNode = pMeta->bucket[bucketKey];
  while (pNode) {
    if (pNode->handle.key == key) {
      if (pNode->handle.valueInUse != NULL && pNode->handle.valueInUse != TQ_DELETE_TOKEN) {
        tqLinkUnpersist(pMeta, pNode);
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

static inline int32_t tqHandlePutImpl(STqMetaStore* pMeta, int64_t key, void* value) {
  int64_t      bucketKey = key & TQ_BUCKET_MASK;
  STqMetaList* pNode = pMeta->bucket[bucketKey];
  while (pNode) {
    if (pNode->handle.key == key) {
      if (pNode->handle.valueInTxn) {
        if (tqDupIntxnReject(pMeta->tqConfigFlag)) {
          terrno = TSDB_CODE_TQ_META_KEY_DUP_IN_TXN;
          return -1;
        }
        if (pNode->handle.valueInTxn != TQ_DELETE_TOKEN) {
          pMeta->pDeleter(pNode->handle.valueInTxn);
        }
      }
      pNode->handle.valueInTxn = value;
      tqLinkUnpersist(pMeta, pNode);
      return 0;
    } else {
      pNode = pNode->next;
    }
  }
  STqMetaList* pNewNode = taosMemoryCalloc(1, sizeof(STqMetaList));
  if (pNewNode == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
    return -1;
  }
  pNewNode->handle.key = key;
  pNewNode->handle.valueInTxn = value;
  pNewNode->next = pMeta->bucket[bucketKey];
  pMeta->bucket[bucketKey] = pNewNode;
  tqLinkUnpersist(pMeta, pNewNode);
  return 0;
}

int32_t tqHandleMovePut(STqMetaStore* pMeta, int64_t key, void* value) { return tqHandlePutImpl(pMeta, key, value); }

int32_t tqHandleCopyPut(STqMetaStore* pMeta, int64_t key, void* value, size_t vsize) {
  void* vmem = taosMemoryMalloc(vsize);
  if (vmem == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
    return -1;
  }
  memcpy(vmem, value, vsize);
  return tqHandlePutImpl(pMeta, key, vmem);
}

static void* tqHandleGetUncommitted(STqMetaStore* pMeta, int64_t key) {
  int64_t      bucketKey = key & TQ_BUCKET_MASK;
  STqMetaList* pNode = pMeta->bucket[bucketKey];
  while (pNode) {
    if (pNode->handle.key == key) {
      if (pNode->handle.valueInTxn != NULL && pNode->handle.valueInTxn != TQ_DELETE_TOKEN) {
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

int32_t tqHandleCommit(STqMetaStore* pMeta, int64_t key) {
  int64_t      bucketKey = key & TQ_BUCKET_MASK;
  STqMetaList* pNode = pMeta->bucket[bucketKey];
  while (pNode) {
    if (pNode->handle.key == key) {
      if (pNode->handle.valueInTxn == NULL) {
        terrno = TSDB_CODE_TQ_META_KEY_NOT_IN_TXN;
        return -1;
      }
      if (pNode->handle.valueInUse && pNode->handle.valueInUse != TQ_DELETE_TOKEN) {
        pMeta->pDeleter(pNode->handle.valueInUse);
      }
      pNode->handle.valueInUse = pNode->handle.valueInTxn;
      pNode->handle.valueInTxn = NULL;
      tqLinkUnpersist(pMeta, pNode);
      return 0;
    } else {
      pNode = pNode->next;
    }
  }
  terrno = TSDB_CODE_TQ_META_NO_SUCH_KEY;
  return -1;
}

int32_t tqHandleAbort(STqMetaStore* pMeta, int64_t key) {
  int64_t      bucketKey = key & TQ_BUCKET_MASK;
  STqMetaList* pNode = pMeta->bucket[bucketKey];
  while (pNode) {
    if (pNode->handle.key == key) {
      if (pNode->handle.valueInTxn) {
        if (pNode->handle.valueInTxn != TQ_DELETE_TOKEN) {
          pMeta->pDeleter(pNode->handle.valueInTxn);
        }
        pNode->handle.valueInTxn = NULL;
        tqLinkUnpersist(pMeta, pNode);
        return 0;
      }
      terrno = TSDB_CODE_TQ_META_KEY_NOT_IN_TXN;
      return -1;
    } else {
      pNode = pNode->next;
    }
  }
  terrno = TSDB_CODE_TQ_META_NO_SUCH_KEY;
  return -1;
}

int32_t tqHandleDel(STqMetaStore* pMeta, int64_t key) {
  int64_t      bucketKey = key & TQ_BUCKET_MASK;
  STqMetaList* pNode = pMeta->bucket[bucketKey];
  while (pNode) {
    if (pNode->handle.key == key) {
      if (pNode->handle.valueInTxn != TQ_DELETE_TOKEN) {
        if (pNode->handle.valueInTxn) {
          pMeta->pDeleter(pNode->handle.valueInTxn);
        }

        pNode->handle.valueInTxn = TQ_DELETE_TOKEN;
        tqLinkUnpersist(pMeta, pNode);
        return 0;
      }
    } else {
      pNode = pNode->next;
    }
  }
  terrno = TSDB_CODE_TQ_META_NO_SUCH_KEY;
  return -1;
}

int32_t tqHandlePurge(STqMetaStore* pMeta, int64_t key) {
  int64_t      bucketKey = key & TQ_BUCKET_MASK;
  STqMetaList* pNode = pMeta->bucket[bucketKey];
  while (pNode) {
    if (pNode->handle.key == key) {
      pNode->handle.valueInUse = TQ_DELETE_TOKEN;
      tqLinkUnpersist(pMeta, pNode);
      return 0;
    } else {
      pNode = pNode->next;
    }
  }
  terrno = TSDB_CODE_TQ_META_NO_SUCH_KEY;
  return -1;
}

// TODO: clean deleted idx and data from persistent file
int32_t tqStoreCompact(STqMetaStore* pMeta) { return 0; }
