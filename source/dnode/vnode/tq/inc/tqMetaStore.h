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


#ifdef __cplusplus
extern "C" {
#endif

#define TQ_BUCKET_MASK 0xFF
#define TQ_BUCKET_SIZE 256

#define TQ_PAGE_SIZE 4096
//key + offset + size
#define TQ_IDX_SIZE 24
//4096 / 24
#define TQ_MAX_IDX_ONE_PAGE 170
//24 * 170
#define TQ_IDX_PAGE_BODY_SIZE 4080
//4096 - 4080
#define TQ_IDX_PAGE_HEAD_SIZE 16

#define TQ_ACTION_CONST      0
#define TQ_ACTION_INUSE      1
#define TQ_ACTION_INUSE_CONT 2
#define TQ_ACTION_INTXN      3

#define TQ_SVER              0

//TODO: inplace mode is not implemented
#define TQ_UPDATE_INPLACE    0
#define TQ_UPDATE_APPEND     1

#define TQ_DUP_INTXN_REWRITE 0
#define TQ_DUP_INTXN_REJECT  2

static inline bool TqUpdateAppend(int32_t tqConfigFlag) {
  return tqConfigFlag & TQ_UPDATE_APPEND;
}

static inline bool TqDupIntxnReject(int32_t tqConfigFlag) {
  return tqConfigFlag & TQ_DUP_INTXN_REJECT;
}

static const int8_t TQ_CONST_DELETE = TQ_ACTION_CONST;
#define TQ_DELETE_TOKEN  (void*)&TQ_CONST_DELETE

typedef struct TqSerializedHead {
  int16_t ver;
  int16_t action;
  int32_t checksum;
  int64_t ssize;
  char    content[];
} TqSerializedHead;

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
  int32_t tqConfigFlag;
  int (*serializer)(const void* pObj, TqSerializedHead** ppHead);
  const void* (*deserializer)(const TqSerializedHead* pHead, void** ppObj);
  void  (*deleter)(void*);
} TqMetaStore;

TqMetaStore*  tqStoreOpen(const char* path,
    int serializer(const void* pObj, TqSerializedHead** ppHead),
    const void* deserializer(const TqSerializedHead* pHead, void** ppObj),
    void deleter(void* pObj),
    int32_t tqConfigFlag
  );
int32_t       tqStoreClose(TqMetaStore*);
//int32_t       tqStoreDelete(TqMetaStore*);
//int32_t       TqStoreCommitAll(TqMetaStore*);
int32_t       tqStorePersist(TqMetaStore*);
//clean deleted idx and data from persistent file
int32_t       tqStoreCompact(TqMetaStore*);

void*   tqHandleGet(TqMetaStore*, int64_t key);
//make it unpersist
void*   tqHandleTouchGet(TqMetaStore*, int64_t key);
int32_t tqHandleMovePut(TqMetaStore*, int64_t key, void* value);
int32_t tqHandleCopyPut(TqMetaStore*, int64_t key, void* value, size_t vsize);
//delete committed kv pair
//notice that a delete action still needs to be committed
int32_t tqHandleDel(TqMetaStore*, int64_t key);
int32_t tqHandleCommit(TqMetaStore*, int64_t key);
int32_t tqHandleAbort(TqMetaStore*, int64_t key);

#ifdef __cplusplus
}
#endif

#endif /* ifndef _TQ_META_STORE_H_ */
