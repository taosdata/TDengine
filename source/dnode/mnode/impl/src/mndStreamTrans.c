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

#include "mndStream.h"
#include "mndTrans.h"

typedef struct SKeyInfo {
  void*   pKey;
  int32_t keyLen;
} SKeyInfo;

static int32_t clearFinishedTrans(SMnode* pMnode);

int32_t mndStreamRegisterTrans(STrans* pTrans, const char* pName, const char* pSrcDb, const char* pDstDb) {
  SStreamTransInfo info = {.transId = pTrans->id, .startTime = taosGetTimestampMs(), .name = pName};
  taosHashPut(execInfo.transMgmt.pDBTrans, pSrcDb, strlen(pSrcDb), &info, sizeof(SStreamTransInfo));

  if (strcmp(pSrcDb, pDstDb) != 0) {
    taosHashPut(execInfo.transMgmt.pDBTrans, pDstDb, strlen(pDstDb), &info, sizeof(SStreamTransInfo));
  }

  return 0;
}

int32_t clearFinishedTrans(SMnode* pMnode) {
  size_t  keyLen = 0;
  SArray* pList = taosArrayInit(4, sizeof(SKeyInfo));
  void*   pIter = NULL;

  while ((pIter = taosHashIterate(execInfo.transMgmt.pDBTrans, pIter)) != NULL) {
    SStreamTransInfo* pEntry = (SStreamTransInfo*)pIter;

    // let's clear the finished trans
    STrans* pTrans = mndAcquireTrans(pMnode, pEntry->transId);
    if (pTrans == NULL) {
      void* pKey = taosHashGetKey(pEntry, &keyLen);
      // key is the name of src/dst db name
      SKeyInfo info = {.pKey = pKey, .keyLen = keyLen};
      mDebug("transId:%d %s startTs:%" PRId64 " cleared since finished", pEntry->transId, pEntry->name,
             pEntry->startTime);
      taosArrayPush(pList, &info);
    } else {
      mndReleaseTrans(pMnode, pTrans);
    }
  }

  size_t num = taosArrayGetSize(pList);
  for (int32_t i = 0; i < num; ++i) {
    SKeyInfo* pKey = taosArrayGet(pList, i);
    taosHashRemove(execInfo.transMgmt.pDBTrans, pKey->pKey, pKey->keyLen);
  }

  mDebug("clear %d finished stream-trans, remained:%d", (int32_t)num, taosHashGetSize(execInfo.transMgmt.pDBTrans));

  terrno = TSDB_CODE_SUCCESS;
  taosArrayDestroy(pList);
  return 0;
}

bool streamTransConflictOtherTrans(SMnode* pMnode, const char* pSrcDb, const char* pDstDb, bool lock) {
  if (lock) {
    taosThreadMutexLock(&execInfo.lock);
  }

  int32_t num = taosHashGetSize(execInfo.transMgmt.pDBTrans);
  if (num <= 0) {
    if (lock) {
      taosThreadMutexUnlock(&execInfo.lock);
    }
    return false;
  }

  clearFinishedTrans(pMnode);

  SStreamTransInfo *pEntry = taosHashGet(execInfo.transMgmt.pDBTrans, pSrcDb, strlen(pSrcDb));
  if (pEntry != NULL) {
    if (lock) {
      taosThreadMutexUnlock(&execInfo.lock);
    }
    mWarn("conflict with other transId:%d in Db:%s, trans:%s", pEntry->transId, pSrcDb, pEntry->name);
    return true;
  }

  pEntry = taosHashGet(execInfo.transMgmt.pDBTrans, pDstDb, strlen(pDstDb));
  if (pEntry != NULL) {
    if (lock) {
      taosThreadMutexUnlock(&execInfo.lock);
    }
    mWarn("conflict with other transId:%d in Db:%s, trans:%s", pEntry->transId, pSrcDb, pEntry->name);
    return true;
  }

  if (lock) {
    taosThreadMutexUnlock(&execInfo.lock);
  }

  return false;
}

int32_t mndAddtoCheckpointWaitingList(SStreamObj* pStream, int64_t checkpointId) {
  SCheckpointCandEntry* pEntry = taosHashGet(execInfo.transMgmt.pWaitingList, &pStream->uid, sizeof(pStream->uid));
  if (pEntry == NULL) {
    SCheckpointCandEntry entry = {.streamId = pStream->uid,
                                  .checkpointTs = taosGetTimestampMs(),
                                  .checkpointId = checkpointId,
                                  .pName = taosStrdup(pStream->name)};

    taosHashPut(execInfo.transMgmt.pWaitingList, &pStream->uid, sizeof(pStream->uid), &entry, sizeof(entry));
    int32_t size = taosHashGetSize(execInfo.transMgmt.pWaitingList);

    mDebug("stream:%" PRIx64 " add into waiting list due to conflict, ts:%" PRId64 " , checkpointId: %" PRId64
           ", total in waitingList:%d",
           pStream->uid, entry.checkpointTs, checkpointId, size);
  } else {
    mDebug("stream:%" PRIx64 " ts:%" PRId64 ", checkpointId:%" PRId64 " already in waiting list, no need to add into",
           pStream->uid, pEntry->checkpointTs, checkpointId);
  }

  return TSDB_CODE_SUCCESS;
}
