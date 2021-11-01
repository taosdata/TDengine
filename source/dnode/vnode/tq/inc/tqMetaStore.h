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

#define TQ_INUSE_SIZE 0xFF
#define TQ_PAGE_SIZE 4096

#ifdef __cplusplus
extern "C" {
#endif

typedef struct TqMetaHandle {
  int64_t key;
  int64_t offset;
  void *valueInUse;
  void *valueInTxn;
} TqMetaHandle;

typedef struct TqMetaList {
  TqMetaHandle handle;
  struct TqMetaList* next;
  struct TqMetaList* inTxnPrev;
  struct TqMetaList* inTxnNext;
  struct TqMetaList* unpersistPrev;
  struct TqMetaList* unpersistNext;
} TqMetaList;

typedef struct TqMetaStore {
  TqMetaList* inUse[TQ_INUSE_SIZE];
  //a table head, key is empty
  TqMetaList* unpersistHead;
  int fileFd; //TODO:temporaral use
  int idxFd;  //TODO:temporaral use
  void* (*serializer)(void*);
  void* (*deserializer)(void*);
  void  (*deleter)(void*);
} TqMetaStore;

TqMetaStore*  tqStoreOpen(const char* path, void* serializer(void* ), void* deserializer(void*), void deleter(void*));
int32_t       tqStoreClose(TqMetaStore*);
int32_t       tqStoreDelete(TqMetaStore*);
//int32_t       TqStoreCommitAll(TqMetaStore*);
int32_t       tqStorePersist(TqMetaStore*);

TqMetaHandle* tqHandleGetInUse(TqMetaStore*, int64_t key);
int32_t       tqHandlePutInUse(TqMetaStore*, TqMetaHandle* handle);
TqMetaHandle* tqHandleGetInTxn(TqMetaStore*, int64_t key);
int32_t       tqHandlePutInTxn(TqMetaStore*, TqMetaHandle* handle);
//delete in-use-handle, make in-txn-handle in use
int32_t       tqHandleCommit(TqMetaStore*, int64_t key);
//delete in-txn-handle
int32_t       tqHandleAbort(TqMetaStore*, int64_t key);
//delete in-use-handle
int32_t       tqHandleDel(TqMetaStore*, int64_t key);
//delete in-use-handle and in-txn-handle
int32_t       tqHandleClear(TqMetaStore*, int64_t key);

#ifdef __cplusplus
}
#endif

#endif /* ifndef _TQ_META_STORE_H_ */
