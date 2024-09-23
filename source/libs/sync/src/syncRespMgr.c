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

#define _DEFAULT_SOURCE
#include "syncRespMgr.h"
#include "syncRaftEntry.h"
#include "syncRaftStore.h"
#include "syncUtil.h"

int32_t syncRespMgrCreate(void *data, int64_t ttl, SSyncRespMgr **ppObj) {
  SSyncRespMgr *pObj = NULL;

  *ppObj = NULL;

  if ((pObj = taosMemoryCalloc(1, sizeof(SSyncRespMgr))) == NULL) {
    TAOS_RETURN(terrno);
  }

  pObj->pRespHash =
      taosHashInit(sizeof(uint64_t), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (pObj->pRespHash == NULL) {
    taosMemoryFree(pObj);
    TAOS_RETURN(terrno);
  }

  pObj->ttl = ttl;
  pObj->data = data;
  pObj->seqNum = 0;
  (void)taosThreadMutexInit(&(pObj->mutex), NULL);

  SSyncNode *pNode = pObj->data;
  sDebug("vgId:%d, resp manager create", pNode->vgId);

  *ppObj = pObj;

  TAOS_RETURN(0);
}

void syncRespMgrDestroy(SSyncRespMgr *pObj) {
  if (pObj == NULL) return;

  SSyncNode *pNode = pObj->data;
  sDebug("vgId:%d, resp manager destroy", pNode->vgId);

  (void)taosThreadMutexLock(&pObj->mutex);
  taosHashCleanup(pObj->pRespHash);
  (void)taosThreadMutexUnlock(&pObj->mutex);
  (void)taosThreadMutexDestroy(&(pObj->mutex));
  taosMemoryFree(pObj);
}

uint64_t syncRespMgrAdd(SSyncRespMgr *pObj, const SRespStub *pStub) {
  (void)taosThreadMutexLock(&pObj->mutex);

  uint64_t seq = ++(pObj->seqNum);
  int32_t  code = taosHashPut(pObj->pRespHash, &seq, sizeof(uint64_t), pStub, sizeof(SRespStub));
  sNTrace(pObj->data, "save message handle:%p, type:%s seq:%" PRIu64 " code:0x%x", pStub->rpcMsg.info.handle,
          TMSG_INFO(pStub->rpcMsg.msgType), seq, code);

  (void)taosThreadMutexUnlock(&pObj->mutex);
  return seq;
}

int32_t syncRespMgrDel(SSyncRespMgr *pObj, uint64_t seq) {
  (void)taosThreadMutexLock(&pObj->mutex);

  int32_t code = taosHashRemove(pObj->pRespHash, &seq, sizeof(seq));
  sNTrace(pObj->data, "remove message handle, seq:%" PRIu64 " code:%d", seq, code);

  (void)taosThreadMutexUnlock(&pObj->mutex);
  return code;
}

int32_t syncRespMgrGet(SSyncRespMgr *pObj, uint64_t seq, SRespStub *pStub) {
  (void)taosThreadMutexLock(&pObj->mutex);

  SRespStub *pTmp = taosHashGet(pObj->pRespHash, &seq, sizeof(uint64_t));
  if (pTmp != NULL) {
    (void)memcpy(pStub, pTmp, sizeof(SRespStub));
    sNTrace(pObj->data, "get message handle, type:%s seq:%" PRIu64 " handle:%p", TMSG_INFO(pStub->rpcMsg.msgType), seq,
            pStub->rpcMsg.info.handle);

    (void)taosThreadMutexUnlock(&pObj->mutex);
    return 1;  // get one object
  } else {
    sNError(pObj->data, "get message handle, no object of seq:%" PRIu64, seq);
  }

  (void)taosThreadMutexUnlock(&pObj->mutex);
  return 0;  // get none object
}

int32_t syncRespMgrGetAndDel(SSyncRespMgr *pObj, uint64_t seq, SRpcHandleInfo *pInfo) {
  (void)taosThreadMutexLock(&pObj->mutex);

  SRespStub *pStub = taosHashGet(pObj->pRespHash, &seq, sizeof(uint64_t));
  if (pStub != NULL) {
    *pInfo = pStub->rpcMsg.info;
    sNTrace(pObj->data, "get-and-del message handle:%p, type:%s seq:%" PRIu64, pStub->rpcMsg.info.handle,
            TMSG_INFO(pStub->rpcMsg.msgType), seq);
    (void)taosHashRemove(pObj->pRespHash, &seq, sizeof(uint64_t));

    (void)taosThreadMutexUnlock(&pObj->mutex);
    return 1;  // get one object
  } else {
    sNTrace(pObj->data, "get-and-del message handle, no object of seq:%" PRIu64, seq);
  }

  (void)taosThreadMutexUnlock(&pObj->mutex);
  return 0;  // get none object
}

static int32_t syncRespCleanByTTL(SSyncRespMgr *pObj, int64_t ttl, bool rsp) {
  SRespStub *pStub = (SRespStub *)taosHashIterate(pObj->pRespHash, NULL);
  int        cnt = 0;
  int        sum = 0;
  SSyncNode *pNode = pObj->data;

  SArray *delIndexArray = taosArrayInit(4, sizeof(uint64_t));
  if (delIndexArray == NULL) {
    TAOS_RETURN(terrno);
  }

  sDebug("vgId:%d, resp manager begin clean by ttl", pNode->vgId);
  while (pStub) {
    size_t    len;
    void     *key = taosHashGetKey(pStub, &len);
    uint64_t *pSeqNum = (uint64_t *)key;
    sum++;

    int64_t nowMS = taosGetTimestampMs();
    if (nowMS - pStub->createTime > ttl || -1 == ttl) {
      if (taosArrayPush(delIndexArray, pSeqNum) == NULL) {
        return terrno;
      }
      cnt++;

      SFsmCbMeta cbMeta = {
          .index = SYNC_INDEX_INVALID,
          .lastConfigIndex = SYNC_INDEX_INVALID,
          .isWeak = false,
          .code = TSDB_CODE_SYN_TIMEOUT,
          .state = pNode->state,
          .seqNum = *pSeqNum,
          .term = SYNC_TERM_INVALID,
          .currentTerm = SYNC_TERM_INVALID,
          .flag = 0,
      };

      pStub->rpcMsg.pCont = NULL;
      pStub->rpcMsg.contLen = 0;

      SRpcMsg rpcMsg = {.info = pStub->rpcMsg.info, .code = TSDB_CODE_SYN_TIMEOUT};
      sInfo("vgId:%d, message handle:%p expired, type:%s ahandle:%p", pNode->vgId, rpcMsg.info.handle,
            TMSG_INFO(pStub->rpcMsg.msgType), rpcMsg.info.ahandle);
      (void)rpcSendResponse(&rpcMsg);
    }

    pStub = taosHashIterate(pObj->pRespHash, pStub);
  }

  int32_t arraySize = taosArrayGetSize(delIndexArray);
  sDebug("vgId:%d, resp manager end clean by ttl, sum:%d, cnt:%d, array-size:%d", pNode->vgId, sum, cnt, arraySize);

  for (int32_t i = 0; i < arraySize; ++i) {
    uint64_t *pSeqNum = taosArrayGet(delIndexArray, i);
    (void)taosHashRemove(pObj->pRespHash, pSeqNum, sizeof(uint64_t));
    sDebug("vgId:%d, resp manager clean by ttl, seq:%" PRId64, pNode->vgId, *pSeqNum);
  }
  taosArrayDestroy(delIndexArray);

  return 0;
}

void syncRespCleanRsp(SSyncRespMgr *pObj) {
  if (pObj == NULL) return;

  SSyncNode *pNode = pObj->data;
  sTrace("vgId:%d, clean all resp", pNode->vgId);

  (void)taosThreadMutexLock(&pObj->mutex);
  (void)syncRespCleanByTTL(pObj, -1, true);
  (void)taosThreadMutexUnlock(&pObj->mutex);
}

void syncRespClean(SSyncRespMgr *pObj) {
  SSyncNode *pNode = pObj->data;
  sTrace("vgId:%d, clean resp by ttl", pNode->vgId);

  (void)taosThreadMutexLock(&pObj->mutex);
  (void)syncRespCleanByTTL(pObj, pObj->ttl, false);
  (void)taosThreadMutexUnlock(&pObj->mutex);
}
