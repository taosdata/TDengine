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
  TqMetaList* unpersistHead;
  //deserializer
  //serializer
  //deleter
} TqMetaStore;

typedef struct TqMetaPageBuf {
  int16_t offset;
  char buffer[TQ_PAGE_SIZE];
} TqMetaPageBuf;

TqMetaStore*  TqStoreOpen(const char* path, void* serializer(void* ), void* deserializer(void*));
int32_t       TqStoreClose(TqMetaStore*);
int32_t       TqStoreDelete(TqMetaStore*);
int32_t       TqStoreCommitAll(TqMetaStore*);
int32_t       TqStorePersist(TqMetaStore*);

TqMetaHandle* TqHandleGetInUse(TqMetaStore*, int64_t key);
int32_t       TqHandlePutInUse(TqMetaStore*, TqMetaHandle* handle);
TqMetaHandle* TqHandleGetInTxn(TqMetaStore*, int64_t key);
int32_t       TqHandlePutInTxn(TqMetaStore*, TqMetaHandle* handle);
//delete in-use-handle, make in-txn-handle in use
int32_t       TqHandleCommit(TqMetaStore*, int64_t key);
//delete in-txn-handle
int32_t       TqHandleAbort(TqMetaStore*, int64_t key);
//delete in-use-handle
int32_t       TqHandleDel(TqMetaStore*, int64_t key);
//delete in-use-handle and in-txn-handle
int32_t       TqHandleClear(TqMetaStore*, int64_t key);

#ifdef __cplusplus
}
#endif

#endif /* ifndef _TQ_META_STORE_H_ */
