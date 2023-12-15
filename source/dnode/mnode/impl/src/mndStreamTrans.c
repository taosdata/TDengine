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

int32_t mndStreamRegisterTrans(STrans* pTrans, const char* pTransName, int64_t streamUid) {
  SStreamTransInfo info = {
      .transId = pTrans->id, .startTime = taosGetTimestampMs(), .name = pTransName, .streamUid = streamUid};
  taosHashPut(execInfo.transMgmt.pDBTrans, &streamUid, sizeof(streamUid), &info, sizeof(SStreamTransInfo));
  return 0;
}

int32_t clearFinishedTrans(SMnode* pMnode) {
  size_t  keyLen = 0;
  void*   pIter = NULL;
  SArray* pList = taosArrayInit(4, sizeof(SKeyInfo));

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

bool streamTransConflictOtherTrans(SMnode* pMnode, int64_t streamUid, const char* pTransName, bool lock) {
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

  SStreamTransInfo *pEntry = taosHashGet(execInfo.transMgmt.pDBTrans, &streamUid, sizeof(streamUid));
  if (pEntry != NULL) {
    SStreamTransInfo tInfo = *pEntry;

    if (lock) {
      taosThreadMutexUnlock(&execInfo.lock);
    }

    if (strcmp(tInfo.name, MND_STREAM_CHECKPOINT_NAME) == 0) {
      if (strcmp(pTransName, MND_STREAM_DROP_NAME) != 0) {
        mWarn("conflict with other transId:%d streamUid:%" PRIx64 ", trans:%s", tInfo.transId, tInfo.streamUid,
              tInfo.name);
        return true;
      }
    } else if ((strcmp(tInfo.name, MND_STREAM_CREATE_NAME) == 0) ||
               (strcmp(tInfo.name, MND_STREAM_DROP_NAME) == 0)) {
      mWarn("conflict with other transId:%d streamUid:%" PRIx64 ", trans:%s", tInfo.transId, tInfo.streamUid,
            tInfo.name);
      return true;
    }
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
