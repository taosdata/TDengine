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
#include "tdbInt.h"
#include "tq.h"

int tqExecKeyCompare(const void* pKey1, int32_t kLen1, const void* pKey2, int32_t kLen2) {
  return strcmp(pKey1, pKey2);
}

int32_t tqMetaOpen(STQ* pTq) {
  if (tdbOpen(pTq->path, 16 * 1024, 1, &pTq->pMetaStore) < 0) {
    ASSERT(0);
  }

  if (tdbTbOpen("handles", -1, -1, tqExecKeyCompare, pTq->pMetaStore, &pTq->pExecStore) < 0) {
    ASSERT(0);
  }

  TXN txn;

  if (tdbTxnOpen(&txn, 0, tdbDefaultMalloc, tdbDefaultFree, NULL, 0) < 0) {
    ASSERT(0);
  }

  TBC* pCur;
  if (tdbTbcOpen(pTq->pExecStore, &pCur, &txn) < 0) {
    ASSERT(0);
  }

  void* pKey;
  int   kLen;
  void* pVal;
  int   vLen;

  tdbTbcMoveToFirst(pCur);
  SDecoder decoder;

  while (tdbTbcNext(pCur, &pKey, &kLen, &pVal, &vLen) == 0) {
    STqHandle handle;
    tDecoderInit(&decoder, (uint8_t*)pVal, vLen);
    tDecodeSTqHandle(&decoder, &handle);
    handle.pWalReader = walOpenReadHandle(pTq->pVnode->pWal);
    for (int32_t i = 0; i < 5; i++) {
      handle.execHandle.pExecReader[i] = tqInitSubmitMsgScanner(pTq->pVnode->pMeta);
    }
    if (handle.execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
      for (int32_t i = 0; i < 5; i++) {
        SReadHandle reader = {
            .reader = handle.execHandle.pExecReader[i],
            .meta = pTq->pVnode->pMeta,
            .pMsgCb = &pTq->pVnode->msgCb,
        };
        handle.execHandle.exec.execCol.task[i] =
            qCreateStreamExecTaskInfo(handle.execHandle.exec.execCol.qmsg, &reader);
        ASSERT(handle.execHandle.exec.execCol.task[i]);
      }
    } else {
      handle.execHandle.exec.execDb.pFilterOutTbUid =
          taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    }
    taosHashPut(pTq->handles, pKey, kLen, &handle, sizeof(STqHandle));
  }

  if (tdbTxnClose(&txn) < 0) {
    ASSERT(0);
  }
  return 0;
}

int32_t tqMetaClose(STQ* pTq) {
  tdbClose(pTq->pMetaStore);
  return 0;
}

int32_t tqMetaSaveHandle(STQ* pTq, const char* key, const STqHandle* pHandle) {
  int32_t code;
  int32_t vlen;
  tEncodeSize(tEncodeSTqHandle, pHandle, vlen, code);
  ASSERT(code == 0);

  void* buf = taosMemoryCalloc(1, vlen);
  if (buf == NULL) {
    ASSERT(0);
  }

  SEncoder encoder;
  tEncoderInit(&encoder, buf, vlen);

  if (tEncodeSTqHandle(&encoder, pHandle) < 0) {
    ASSERT(0);
  }

  TXN txn;

  if (tdbTxnOpen(&txn, 0, tdbDefaultMalloc, tdbDefaultFree, NULL, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED) < 0) {
    ASSERT(0);
  }

  if (tdbBegin(pTq->pMetaStore, &txn) < 0) {
    ASSERT(0);
  }

  if (tdbTbUpsert(pTq->pExecStore, key, (int)strlen(key), buf, vlen, &txn) < 0) {
    ASSERT(0);
  }

  if (tdbCommit(pTq->pMetaStore, &txn) < 0) {
    ASSERT(0);
  }

  tEncoderClear(&encoder);
  taosMemoryFree(buf);
  return 0;
}

int32_t tqMetaDeleteHandle(STQ* pTq, const char* key) {
  TXN txn;

  if (tdbTxnOpen(&txn, 0, tdbDefaultMalloc, tdbDefaultFree, NULL, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED) < 0) {
    ASSERT(0);
  }

  if (tdbBegin(pTq->pMetaStore, &txn) < 0) {
    ASSERT(0);
  }

  if (tdbTbDelete(pTq->pExecStore, key, (int)strlen(key), &txn) < 0) {
    /*ASSERT(0);*/
  }

  if (tdbCommit(pTq->pMetaStore, &txn) < 0) {
    ASSERT(0);
  }

  return 0;
}
