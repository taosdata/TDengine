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
#include "syncRaftEntry.h"
#include "syncRaftStore.h"

SSyncRespMgr *syncRespMgrCreate(void *data, int64_t ttl) {
  SSyncRespMgr *pObj = (SSyncRespMgr *)taosMemoryMalloc(sizeof(SSyncRespMgr));
  if (pObj == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  memset(pObj, 0, sizeof(SSyncRespMgr));

  pObj->pRespHash =
      taosHashInit(sizeof(uint64_t), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  ASSERT(pObj->pRespHash != NULL);
  pObj->ttl = ttl;
  pObj->data = data;
  pObj->seqNum = 0;
  taosThreadMutexInit(&(pObj->mutex), NULL);

  return pObj;
}

void syncRespMgrDestroy(SSyncRespMgr *pObj) {
  if (pObj != NULL) {
    taosThreadMutexLock(&(pObj->mutex));
    taosHashCleanup(pObj->pRespHash);
    taosThreadMutexUnlock(&(pObj->mutex));
    taosThreadMutexDestroy(&(pObj->mutex));
    taosMemoryFree(pObj);
  }
}

int64_t syncRespMgrAdd(SSyncRespMgr *pObj, SRespStub *pStub) {
  taosThreadMutexLock(&(pObj->mutex));

  uint64_t keyCode = ++(pObj->seqNum);
  taosHashPut(pObj->pRespHash, &keyCode, sizeof(keyCode), pStub, sizeof(SRespStub));

  sNTrace(pObj->data, "save message handle, type:%s seq:%" PRIu64 " handle:%p", TMSG_INFO(pStub->rpcMsg.msgType),
          keyCode, pStub->rpcMsg.info.handle);
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

    sNTrace(pObj->data, "get message handle, type:%s seq:%" PRIu64 " handle:%p", TMSG_INFO(pStub->rpcMsg.msgType),
            index, pStub->rpcMsg.info.handle);
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

    sNTrace(pObj->data, "get-and-del message handle, type:%s seq:%" PRIu64 " handle:%p",
            TMSG_INFO(pStub->rpcMsg.msgType), index, pStub->rpcMsg.info.handle);
    taosHashRemove(pObj->pRespHash, &index, sizeof(index));
    taosThreadMutexUnlock(&(pObj->mutex));
    return 1;  // get one object
  }
  taosThreadMutexUnlock(&(pObj->mutex));
  return 0;  // get none object
}

void syncRespCleanRsp(SSyncRespMgr *pObj) {
  taosThreadMutexLock(&(pObj->mutex));
  syncRespCleanByTTL(pObj, -1, true);
  taosThreadMutexUnlock(&(pObj->mutex));
}

void syncRespClean(SSyncRespMgr *pObj) {
  taosThreadMutexLock(&(pObj->mutex));
  syncRespCleanByTTL(pObj, pObj->ttl, false);
  taosThreadMutexUnlock(&(pObj->mutex));
}

void syncRespCleanByTTL(SSyncRespMgr *pObj, int64_t ttl, bool rsp) {
  SRespStub *pStub = (SRespStub *)taosHashIterate(pObj->pRespHash, NULL);
  int        cnt = 0;
  int        sum = 0;
  SSyncNode *pSyncNode = pObj->data;

  SArray *delIndexArray = taosArrayInit(0, sizeof(uint64_t));
  ASSERT(delIndexArray != NULL);
  sDebug("vgId:%d, resp mgr begin clean by ttl", pSyncNode->vgId);

  while (pStub) {
    size_t    len;
    void *    key = taosHashGetKey(pStub, &len);
    uint64_t *pSeqNum = (uint64_t *)key;
    sum++;

    int64_t nowMS = taosGetTimestampMs();
    if (nowMS - pStub->createTime > ttl || -1 == ttl) {
      taosArrayPush(delIndexArray, pSeqNum);
      cnt++;

      SFsmCbMeta cbMeta = {
          .index = SYNC_INDEX_INVALID,
          .lastConfigIndex = SYNC_INDEX_INVALID,
          .isWeak = false,
          .code = TSDB_CODE_SYN_TIMEOUT,
          .state = pSyncNode->state,
          .seqNum = *pSeqNum,
          .term = SYNC_TERM_INVALID,
          .currentTerm = pSyncNode->pRaftStore->currentTerm,
          .flag = 0,
      };

      pStub->rpcMsg.pCont = NULL;
      pStub->rpcMsg.contLen = 0;

      // TODO: and make rpcMsg body, call commit cb
      // pSyncNode->pFsm->FpCommitCb(pSyncNode->pFsm, &(pStub->rpcMsg), cbMeta);

      pStub->rpcMsg.code = TSDB_CODE_SYN_NOT_LEADER;
      if (pStub->rpcMsg.info.handle != NULL) {
        tmsgSendRsp(&(pStub->rpcMsg));
      }
    }

    pStub = (SRespStub *)taosHashIterate(pObj->pRespHash, pStub);
  }

  int32_t arraySize = taosArrayGetSize(delIndexArray);
  sDebug("vgId:%d, resp mgr end clean by ttl, sum:%d, cnt:%d, array-size:%d", pSyncNode->vgId, sum, cnt, arraySize);

  for (int32_t i = 0; i < arraySize; ++i) {
    uint64_t *pSeqNum = taosArrayGet(delIndexArray, i);
    taosHashRemove(pObj->pRespHash, pSeqNum, sizeof(uint64_t));
    sDebug("vgId:%d, resp mgr clean by ttl, seq:%" PRId64 "", pSyncNode->vgId, *pSeqNum);
  }
  taosArrayDestroy(delIndexArray);
}
