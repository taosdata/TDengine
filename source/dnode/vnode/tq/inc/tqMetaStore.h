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

#ifndef _TQ_META_STORE_H_
#define _TQ_META_STORE_H_

#include "os.h"
#include "tq.h"


#ifdef __cplusplus
extern "C" {
#endif

#define TQ_BUCKET_SIZE 0xFF
#define TQ_PAGE_SIZE 4096
//key + offset + size
#define TQ_IDX_ENTRY_SIZE 24

inline static int TqMaxEntryOnePage() { //170
  return TQ_PAGE_SIZE / TQ_IDX_ENTRY_SIZE;
}

inline static int TqEmptyTail() { //16
  return TQ_PAGE_SIZE - TqMaxEntryOnePage();
}

typedef struct TqMetaHandle {
  int64_t key;
  int64_t offset;
  int64_t serializedSize;
  void*   valueInUse;
  void*   valueInTxn;
} TqMetaHandle;

typedef struct TqMetaList {
  TqMetaHandle handle;
  struct TqMetaList* next;
  //struct TqMetaList* inTxnPrev;
  //struct TqMetaList* inTxnNext;
  struct TqMetaList* unpersistPrev;
  struct TqMetaList* unpersistNext;
} TqMetaList;

typedef struct TqMetaStore {
  TqMetaList* bucket[TQ_BUCKET_SIZE];
  //a table head
  TqMetaList* unpersistHead;
  int fileFd; //TODO:temporaral use, to be replaced by unified tfile
  int idxFd;  //TODO:temporaral use, to be replaced by unified tfile
  char* dirPath;
  int (*serializer)(const void* pObj, void** ppBytes);
  const void* (*deserializer)(const void* pBytes, void** ppObj);
  void  (*deleter)(void*);
} TqMetaStore;

TqMetaStore*  tqStoreOpen(const char* path,
    int serializer(const void* pObj, void** ppBytes),
    const void* deserializer(const void* pBytes, void** ppObj),
    void deleter(void* pObj));
int32_t       tqStoreClose(TqMetaStore*);
//int32_t       tqStoreDelete(TqMetaStore*);
//int32_t       TqStoreCommitAll(TqMetaStore*);
int32_t       tqStorePersist(TqMetaStore*);

void*   tqHandleGet(TqMetaStore*, int64_t key);
int32_t tqHandleMovePut(TqMetaStore*, int64_t key, void* value);
int32_t tqHandleCopyPut(TqMetaStore*, int64_t key, void* value, size_t vsize);
//do commit
int32_t tqHandleCommit(TqMetaStore*, int64_t key);
//delete uncommitted
int32_t tqHandleAbort(TqMetaStore*, int64_t key);
//delete committed
int32_t tqHandleDel(TqMetaStore*, int64_t key);
//delete both committed and uncommitted
int32_t tqHandleClear(TqMetaStore*, int64_t key);

#ifdef __cplusplus
}
#endif

#endif /* ifndef _TQ_META_STORE_H_ */
