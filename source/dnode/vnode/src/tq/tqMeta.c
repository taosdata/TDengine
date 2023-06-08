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

int32_t tEncodeSTqHandle(SEncoder* pEncoder, const STqHandle* pHandle) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeCStr(pEncoder, pHandle->subKey) < 0) return -1;
  if (tEncodeI8(pEncoder, pHandle->fetchMeta) < 0) return -1;
  if (tEncodeI64(pEncoder, pHandle->consumerId) < 0) return -1;
  if (tEncodeI64(pEncoder, pHandle->snapshotVer) < 0) return -1;
  if (tEncodeI32(pEncoder, pHandle->epoch) < 0) return -1;
  if (tEncodeI8(pEncoder, pHandle->execHandle.subType) < 0) return -1;
  if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
    if (tEncodeCStr(pEncoder, pHandle->execHandle.execCol.qmsg) < 0) return -1;
  } else if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__DB) {
    int32_t size = taosHashGetSize(pHandle->execHandle.execDb.pFilterOutTbUid);
    if (tEncodeI32(pEncoder, size) < 0) return -1;
    void* pIter = NULL;
    pIter = taosHashIterate(pHandle->execHandle.execDb.pFilterOutTbUid, pIter);
    while (pIter) {
      int64_t* tbUid = (int64_t*)taosHashGetKey(pIter, NULL);
      if (tEncodeI64(pEncoder, *tbUid) < 0) return -1;
      pIter = taosHashIterate(pHandle->execHandle.execDb.pFilterOutTbUid, pIter);
    }
  } else if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__TABLE) {
    if (tEncodeI64(pEncoder, pHandle->execHandle.execTb.suid) < 0) return -1;
    if (pHandle->execHandle.execTb.qmsg != NULL){
      if (tEncodeCStr(pEncoder, pHandle->execHandle.execTb.qmsg) < 0) return -1;
    }
  }
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeSTqHandle(SDecoder* pDecoder, STqHandle* pHandle) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pHandle->subKey) < 0) return -1;
  if (tDecodeI8(pDecoder, &pHandle->fetchMeta) < 0) return -1;
  if (tDecodeI64(pDecoder, &pHandle->consumerId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pHandle->snapshotVer) < 0) return -1;
  if (tDecodeI32(pDecoder, &pHandle->epoch) < 0) return -1;
  if (tDecodeI8(pDecoder, &pHandle->execHandle.subType) < 0) return -1;
  if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
    if (tDecodeCStrAlloc(pDecoder, &pHandle->execHandle.execCol.qmsg) < 0) return -1;
  } else if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__DB) {
    pHandle->execHandle.execDb.pFilterOutTbUid =
        taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    int32_t size = 0;
    if (tDecodeI32(pDecoder, &size) < 0) return -1;
    for (int32_t i = 0; i < size; i++) {
      int64_t tbUid = 0;
      if (tDecodeI64(pDecoder, &tbUid) < 0) return -1;
      taosHashPut(pHandle->execHandle.execDb.pFilterOutTbUid, &tbUid, sizeof(int64_t), NULL, 0);
    }
  } else if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__TABLE) {
    if (tDecodeI64(pDecoder, &pHandle->execHandle.execTb.suid) < 0) return -1;
    if (!tDecodeIsEnd(pDecoder)){
      if (tDecodeCStrAlloc(pDecoder, &pHandle->execHandle.execTb.qmsg) < 0) return -1;
    }
  }
  tEndDecode(pDecoder);
  return 0;
}

int32_t tqMetaOpen(STQ* pTq) {
  if (tdbOpen(pTq->path, 16 * 1024, 1, &pTq->pMetaDB, 0) < 0) {
    return -1;
  }

  if (tdbTbOpen("tq.db", -1, -1, NULL, pTq->pMetaDB, &pTq->pExecStore, 0) < 0) {
    return -1;
  }

  if (tdbTbOpen("tq.check.db", -1, -1, NULL, pTq->pMetaDB, &pTq->pCheckStore, 0) < 0) {
    return -1;
  }

  if (tqMetaRestoreHandle(pTq) < 0) {
    return -1;
  }

  if (tqMetaRestoreCheckInfo(pTq) < 0) {
    return -1;
  }

  return 0;
}

int32_t tqMetaClose(STQ* pTq) {
  if (pTq->pExecStore) {
    tdbTbClose(pTq->pExecStore);
  }
  if (pTq->pCheckStore) {
    tdbTbClose(pTq->pCheckStore);
  }
  tdbClose(pTq->pMetaDB);
  return 0;
}

int32_t tqMetaSaveCheckInfo(STQ* pTq, const char* key, const void* value, int32_t vLen) {
  TXN* txn;

  if (tdbBegin(pTq->pMetaDB, &txn, tdbDefaultMalloc, tdbDefaultFree, NULL, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED) <
      0) {
    return -1;
  }

  if (tdbTbUpsert(pTq->pCheckStore, key, strlen(key), value, vLen, txn) < 0) {
    return -1;
  }

  if (tdbCommit(pTq->pMetaDB, txn) < 0) {
    return -1;
  }

  if (tdbPostCommit(pTq->pMetaDB, txn) < 0) {
    return -1;
  }

  return 0;
}

int32_t tqMetaDeleteCheckInfo(STQ* pTq, const char* key) {
  TXN* txn;

  if (tdbBegin(pTq->pMetaDB, &txn, tdbDefaultMalloc, tdbDefaultFree, NULL, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED) <
      0) {
    return -1;
  }

  if (tdbTbDelete(pTq->pCheckStore, key, (int)strlen(key), txn) < 0) {
    tqWarn("vgId:%d, tq try delete checkinfo failed %s", pTq->pVnode->config.vgId, key);
  }

  if (tdbCommit(pTq->pMetaDB, txn) < 0) {
    return -1;
  }

  if (tdbPostCommit(pTq->pMetaDB, txn) < 0) {
    return -1;
  }

  return 0;
}

int32_t tqMetaRestoreCheckInfo(STQ* pTq) {
  TBC* pCur = NULL;
  if (tdbTbcOpen(pTq->pCheckStore, &pCur, NULL) < 0) {
    return -1;
  }

  void*    pKey = NULL;
  int      kLen = 0;
  void*    pVal = NULL;
  int      vLen = 0;
  SDecoder decoder;

  tdbTbcMoveToFirst(pCur);

  while (tdbTbcNext(pCur, &pKey, &kLen, &pVal, &vLen) == 0) {
    STqCheckInfo info;
    tDecoderInit(&decoder, (uint8_t*)pVal, vLen);
    if (tDecodeSTqCheckInfo(&decoder, &info) < 0) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      tdbFree(pKey);
      tdbFree(pVal);
      tdbTbcClose(pCur);
      return -1;
    }
    tDecoderClear(&decoder);
    if (taosHashPut(pTq->pCheckInfo, info.topic, strlen(info.topic), &info, sizeof(STqCheckInfo)) < 0) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      tdbFree(pKey);
      tdbFree(pVal);
      tdbTbcClose(pCur);
      return -1;
    }
  }
  tdbFree(pKey);
  tdbFree(pVal);
  tdbTbcClose(pCur);
  return 0;
}

int32_t tqMetaSaveHandle(STQ* pTq, const char* key, const STqHandle* pHandle) {
  int32_t code;
  int32_t vlen;
  tEncodeSize(tEncodeSTqHandle, pHandle, vlen, code);
  if (code < 0) {
    return -1;
  }

  tqDebug("tq save %s(%d) handle consumer:0x%" PRIx64 " epoch:%d vgId:%d", pHandle->subKey,
          (int32_t)strlen(pHandle->subKey), pHandle->consumerId, pHandle->epoch, TD_VID(pTq->pVnode));

  void* buf = taosMemoryCalloc(1, vlen);
  if (buf == NULL) {
    return -1;
  }

  SEncoder encoder;
  tEncoderInit(&encoder, buf, vlen);

  if (tEncodeSTqHandle(&encoder, pHandle) < 0) {
    tEncoderClear(&encoder);
    taosMemoryFree(buf);
    return -1;
  }

  TXN* txn;

  if (tdbBegin(pTq->pMetaDB, &txn, tdbDefaultMalloc, tdbDefaultFree, NULL, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED) <
      0) {
    tEncoderClear(&encoder);
    taosMemoryFree(buf);
    return -1;
  }

  if (tdbTbUpsert(pTq->pExecStore, key, (int)strlen(key), buf, vlen, txn) < 0) {
    tEncoderClear(&encoder);
    taosMemoryFree(buf);
    return -1;
  }

  if (tdbCommit(pTq->pMetaDB, txn) < 0) {
    tEncoderClear(&encoder);
    taosMemoryFree(buf);
    return -1;
  }

  if (tdbPostCommit(pTq->pMetaDB, txn) < 0) {
    tEncoderClear(&encoder);
    taosMemoryFree(buf);
    return -1;
  }

  tEncoderClear(&encoder);
  taosMemoryFree(buf);
  return 0;
}

int32_t tqMetaDeleteHandle(STQ* pTq, const char* key) {
  TXN* txn;

  if (tdbBegin(pTq->pMetaDB, &txn, tdbDefaultMalloc, tdbDefaultFree, NULL, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED) <
      0) {
    return -1;
  }

  if (tdbTbDelete(pTq->pExecStore, key, (int)strlen(key), txn) < 0) {
  }

  if (tdbCommit(pTq->pMetaDB, txn) < 0) {
    return -1;
  }

  if (tdbPostCommit(pTq->pMetaDB, txn) < 0) {
    return -1;
  }

  return 0;
}

int32_t tqMetaRestoreHandle(STQ* pTq) {
  int  code = 0;
  TBC* pCur = NULL;
  if (tdbTbcOpen(pTq->pExecStore, &pCur, NULL) < 0) {
    return -1;
  }

  int32_t  vgId = TD_VID(pTq->pVnode);
  void*    pKey = NULL;
  int      kLen = 0;
  void*    pVal = NULL;
  int      vLen = 0;
  SDecoder decoder;

  tdbTbcMoveToFirst(pCur);

  while (tdbTbcNext(pCur, &pKey, &kLen, &pVal, &vLen) == 0) {
    STqHandle handle = {0};
    tDecoderInit(&decoder, (uint8_t*)pVal, vLen);
    tDecodeSTqHandle(&decoder, &handle);
    tDecoderClear(&decoder);

    handle.pRef = walOpenRef(pTq->pVnode->pWal);
    if (handle.pRef == NULL) {
      code = -1;
      goto end;
    }
    walSetRefVer(handle.pRef, handle.snapshotVer);

    SReadHandle reader = {
        .vnode = pTq->pVnode,
        .initTableReader = true,
        .initTqReader = true,
        .version = handle.snapshotVer
    };

    initStorageAPI(&reader.api);

    if (handle.execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
      handle.execHandle.task =
          qCreateQueueExecTaskInfo(handle.execHandle.execCol.qmsg, &reader, vgId, &handle.execHandle.numOfCols, 0);
      if (handle.execHandle.task == NULL) {
        tqError("cannot create exec task for %s", handle.subKey);
        code = -1;
        goto end;
      }
      void* scanner = NULL;
      qExtractStreamScanner(handle.execHandle.task, &scanner);
      if (scanner == NULL) {
        tqError("cannot extract stream scanner for %s", handle.subKey);
        code = -1;
        goto end;
      }
      handle.execHandle.pTqReader = qExtractReaderFromStreamScanner(scanner);
      if (handle.execHandle.pTqReader == NULL) {
        tqError("cannot extract exec reader for %s", handle.subKey);
        code = -1;
        goto end;
      }
    } else if (handle.execHandle.subType == TOPIC_SUB_TYPE__DB) {
      handle.pWalReader = walOpenReader(pTq->pVnode->pWal, NULL);
      handle.execHandle.pTqReader = tqReaderOpen(pTq->pVnode);

      buildSnapContext(reader.vnode, reader.version, 0, handle.execHandle.subType, handle.fetchMeta,
                       (SSnapContext**)(&reader.sContext));
      handle.execHandle.task = qCreateQueueExecTaskInfo(NULL, &reader, vgId, NULL, 0);
    } else if (handle.execHandle.subType == TOPIC_SUB_TYPE__TABLE) {
      handle.pWalReader = walOpenReader(pTq->pVnode->pWal, NULL);

      if(handle.execHandle.execTb.qmsg != NULL && strcmp(handle.execHandle.execTb.qmsg, "") != 0) {
        if (nodesStringToNode(handle.execHandle.execTb.qmsg, &handle.execHandle.execTb.node) != 0) {
          tqError("nodesStringToNode error in sub stable, since %s", terrstr());
          return -1;
        }
      }
      buildSnapContext(reader.vnode, reader.version, handle.execHandle.execTb.suid, handle.execHandle.subType,
                       handle.fetchMeta, (SSnapContext**)(&reader.sContext));
      handle.execHandle.task = qCreateQueueExecTaskInfo(NULL, &reader, vgId, NULL, 0);

      SArray* tbUidList = NULL;
      int ret = qGetTableList(handle.execHandle.execTb.suid, pTq->pVnode, handle.execHandle.execTb.node, &tbUidList, handle.execHandle.task);
      if(ret != TDB_CODE_SUCCESS) {
        tqError("qGetTableList error:%d handle %s consumer:0x%" PRIx64, ret, handle.subKey, handle.consumerId);
        taosArrayDestroy(tbUidList);
        goto end;
      }
      tqDebug("vgId:%d, tq try to get ctb for stb subscribe, suid:%" PRId64, pTq->pVnode->config.vgId, handle.execHandle.execTb.suid);
      handle.execHandle.pTqReader = tqReaderOpen(pTq->pVnode);
      tqReaderSetTbUidList(handle.execHandle.pTqReader, tbUidList, NULL);
      taosArrayDestroy(tbUidList);
    }
    tqDebug("tq restore %s consumer %" PRId64 " vgId:%d", handle.subKey, handle.consumerId, vgId);
    taosHashPut(pTq->pHandle, pKey, kLen, &handle, sizeof(STqHandle));
  }

end:
  tdbFree(pKey);
  tdbFree(pVal);
  tdbTbcClose(pCur);
  return code;
}
