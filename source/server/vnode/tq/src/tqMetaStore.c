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
#include <unistd.h>

typedef struct TqMetaPageBuf {
  int16_t offset;
  char buffer[TQ_PAGE_SIZE];
} TqMetaPageBuf;

TqMetaStore* tqStoreOpen(const char* path, void* serializer(void*),
    void* deserializer(void*), void deleter(void*)) {
  //concat data file name and index file name
  int fileFd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0755);
  if(fileFd < 0) return NULL;
  TqMetaStore* pMeta = malloc(sizeof(TqMetaStore)); 
  if(pMeta == NULL) {
    //close
    return NULL;
  }
  memset(pMeta, 0, sizeof(TqMetaStore));
  pMeta->fileFd = fileFd;
  
  int idxFd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0755);
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
  //close file
  //free memory
  return 0;
}

int32_t tqStoreDelete(TqMetaStore* pMeta) {
  //close file
  //delete file
  //free memory
  return 0;
}

int32_t tqStorePersist(TqMetaStore* pMeta) {
  TqMetaList *node = pMeta->unpersistHead;
  while(node->unpersistNext) {
    //serialize
    //append data
    //write offset and idx
    //remove from unpersist list
  }
  return 0;
}

int32_t tqHandlePutInUse(TqMetaStore* pMeta, TqMetaHandle* handle) {
  return 0;
}

TqMetaHandle* tqHandleGetInUse(TqMetaStore* pMeta, int64_t key) {
  return NULL;
}

int32_t tqHandlePutInTxn(TqMetaStore* pMeta, TqMetaHandle* handle) {
  return 0;
}

TqMetaHandle* tqHandleGetInTxn(TqMetaStore* pMeta, int64_t key) {
  return NULL;
}

int32_t tqHandleCommit(TqMetaStore* pMeta, int64_t key) {
  return 0;
}

int32_t tqHandleAbort(TqMetaStore* pMeta, int64_t key) {
  return 0;
}

int32_t tqHandleDel(TqMetaStore* pMeta, int64_t key) {
  return 0;
}

int32_t tqHandleClear(TqMetaStore* pMeta, int64_t key) {
  return 0;
}
