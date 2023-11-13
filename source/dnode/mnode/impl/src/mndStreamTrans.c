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

#include "mndTrans.h"
#include "mndStream.h"

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
  SArray* pList = taosArrayInit(4, sizeof(SKeyInfo));
  size_t  keyLen = 0;

  taosThreadMutexLock(&execInfo.lock);

  void* pIter = NULL;
  while ((pIter = taosHashIterate(execInfo.transMgmt.pDBTrans, pIter)) != NULL) {
    SStreamTransInfo *pEntry = (SStreamTransInfo *)pIter;
    STrans* pTrans = mndAcquireTrans(pMnode, pEntry->transId);

    // let's clear the finished trans
    if (pTrans == NULL) {
      void* pKey = taosHashGetKey(pEntry, &keyLen);
      // key is the name of src/dst db name
      SKeyInfo info = {.pKey = pKey, .keyLen = keyLen};

      mDebug("transId:%d %s startTs:%" PRId64 "cleared due to finished", pEntry->transId, pEntry->name,
             pEntry->startTime);
      taosArrayPush(pList, &info);
    } else {
      mndReleaseTrans(pMnode, pTrans);
    }
  }

  size_t num = taosArrayGetSize(pList);
  for(int32_t i = 0; i < num; ++i) {
    SKeyInfo* pKey = taosArrayGet(pList, i);
    taosHashRemove(execInfo.transMgmt.pDBTrans, pKey->pKey, pKey->keyLen);
  }

  mDebug("clear %d finished stream-trans, remained:%d", (int32_t) num, taosHashGetSize(execInfo.transMgmt.pDBTrans));
  taosThreadMutexUnlock(&execInfo.lock);

  terrno = TSDB_CODE_SUCCESS;
  taosArrayDestroy(pList);
  return 0;
}

bool streamTransConflictOtherTrans(SMnode* pMnode, const char* pSrcDb, const char* pDstDb) {
  clearFinishedTrans(pMnode);

  taosThreadMutexLock(&execInfo.lock);
  int32_t num = taosHashGetSize(execInfo.transMgmt.pDBTrans);
  if (num <= 0) {
    taosThreadMutexUnlock(&execInfo.lock);
    return false;
  }

  SStreamTransInfo *pEntry = taosHashGet(execInfo.transMgmt.pDBTrans, pSrcDb, strlen(pSrcDb));
  if (pEntry != NULL) {
    taosThreadMutexUnlock(&execInfo.lock);
    return true;
  }

  pEntry = taosHashGet(execInfo.transMgmt.pDBTrans, pDstDb, strlen(pDstDb));
  if (pEntry != NULL) {
    taosThreadMutexUnlock(&execInfo.lock);
    return true;
  }

  taosThreadMutexUnlock(&execInfo.lock);
  return false;
}



