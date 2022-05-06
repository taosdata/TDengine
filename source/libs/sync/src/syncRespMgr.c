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

#include "syncRespMgr.h"

SSyncRespMgr *syncRespMgrCreate(void *data, int64_t ttl) {
  SSyncRespMgr *pObj = (SSyncRespMgr *)taosMemoryMalloc(sizeof(SSyncRespMgr));
  memset(pObj, 0, sizeof(SSyncRespMgr));

  pObj->pRespHash =
      taosHashInit(sizeof(uint64_t), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  assert(pObj->pRespHash != NULL);
  pObj->ttl = ttl;
  pObj->data = data;
  pObj->seqNum = 0;
  taosThreadMutexInit(&(pObj->mutex), NULL);

  return pObj;
}

void syncRespMgrDestroy(SSyncRespMgr *pObj) {
  taosThreadMutexLock(&(pObj->mutex));
  taosHashCleanup(pObj->pRespHash);
  taosThreadMutexUnlock(&(pObj->mutex));
  taosThreadMutexDestroy(&(pObj->mutex));
  taosMemoryFree(pObj);
}

int64_t syncRespMgrAdd(SSyncRespMgr *pObj, SRespStub *pStub) {
  taosThreadMutexLock(&(pObj->mutex));

  uint64_t keyCode = ++(pObj->seqNum);
  taosHashPut(pObj->pRespHash, &keyCode, sizeof(keyCode), pStub, sizeof(SRespStub));

  taosThreadMutexUnlock(&(pObj->mutex));
  return keyCode;
}

int32_t syncRespMgrDel(SSyncRespMgr *pObj, uint64_t index) {
  taosThreadMutexLock(&(pObj->mutex));

  taosHashRemove(pObj->pRespHash, &index, sizeof(index));

  taosThreadMutexUnlock(&(pObj->mutex));
  return 0;
}

int32_t syncRespMgrGet(SSyncRespMgr *pObj, uint64_t index, SRespStub *pStub) {
  taosThreadMutexLock(&(pObj->mutex));

  void *pTmp = taosHashGet(pObj->pRespHash, &index, sizeof(index));
  if (pTmp != NULL) {
    memcpy(pStub, pTmp, sizeof(SRespStub));
    taosThreadMutexUnlock(&(pObj->mutex));
    return 1;  // get one object
  }
  taosThreadMutexUnlock(&(pObj->mutex));
  return 0;  // get none object
}

int32_t syncRespMgrGetAndDel(SSyncRespMgr *pObj, uint64_t index, SRespStub *pStub) {
  taosThreadMutexLock(&(pObj->mutex));

  void *pTmp = taosHashGet(pObj->pRespHash, &index, sizeof(index));
  if (pTmp != NULL) {
    memcpy(pStub, pTmp, sizeof(SRespStub));
    taosThreadMutexUnlock(&(pObj->mutex));
    taosHashRemove(pObj->pRespHash, &index, sizeof(index));
    return 1;  // get one object
  }
  taosThreadMutexUnlock(&(pObj->mutex));
  return 0;  // get none object
}

void syncRespClean(SSyncRespMgr *pObj) {
  taosThreadMutexLock(&(pObj->mutex));
  syncRespCleanByTTL(pObj, pObj->ttl);
  taosThreadMutexUnlock(&(pObj->mutex));
}

void syncRespCleanByTTL(SSyncRespMgr *pObj, int64_t ttl) {}