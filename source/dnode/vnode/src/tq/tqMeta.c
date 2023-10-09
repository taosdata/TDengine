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

//  if (tqMetaRestoreHandle(pTq) < 0) {
//    return -1;
//  }

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
  void* buf = NULL;
  SEncoder encoder;
  tEncodeSize(tEncodeSTqHandle, pHandle, vlen, code);
  if (code < 0) {
    goto end;
  }

  tqDebug("tq save %s(%d) handle consumer:0x%" PRIx64 " epoch:%d vgId:%d", pHandle->subKey,
          (int32_t)strlen(pHandle->subKey), pHandle->consumerId, pHandle->epoch, TD_VID(pTq->pVnode));

  buf = taosMemoryCalloc(1, vlen);
  if (buf == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }


  tEncoderInit(&encoder, buf, vlen);

  code = tEncodeSTqHandle(&encoder, pHandle);
  if (code < 0) {
    goto end;
  }

  TXN* txn = NULL;
  code = tdbBegin(pTq->pMetaDB, &txn, tdbDefaultMalloc, tdbDefaultFree, NULL, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
  if (code < 0) {
    goto end;
  }

  code = tdbTbUpsert(pTq->pExecStore, key, (int)strlen(key), buf, vlen, txn);
  if (code < 0) {
    goto end;
  }

  code = tdbCommit(pTq->pMetaDB, txn);
  if (code < 0) {
    goto end;
  }

  code = tdbPostCommit(pTq->pMetaDB, txn);
end:
  tEncoderClear(&encoder);
  taosMemoryFree(buf);
  return code;
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

static int buildHandle(STQ* pTq, STqHandle* handle){
  SVnode* pVnode = pTq->pVnode;
  int32_t vgId = TD_VID(pVnode);

  handle->pRef = walOpenRef(pVnode->pWal);
  if (handle->pRef == NULL) {
    return -1;
  }
  walSetRefVer(handle->pRef, handle->snapshotVer);

  SReadHandle reader = {
      .vnode = pVnode,
      .initTableReader = true,
      .initTqReader = true,
      .version = handle->snapshotVer,
  };

  initStorageAPI(&reader.api);

  if (handle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
    handle->execHandle.task =
        qCreateQueueExecTaskInfo(handle->execHandle.execCol.qmsg, &reader, vgId, &handle->execHandle.numOfCols, handle->consumerId);
    if (handle->execHandle.task == NULL) {
      tqError("cannot create exec task for %s", handle->subKey);
      return -1;
    }
    void* scanner = NULL;
    qExtractStreamScanner(handle->execHandle.task, &scanner);
    if (scanner == NULL) {
      tqError("cannot extract stream scanner for %s", handle->subKey);
      return -1;
    }
    handle->execHandle.pTqReader = qExtractReaderFromStreamScanner(scanner);
    if (handle->execHandle.pTqReader == NULL) {
      tqError("cannot extract exec reader for %s", handle->subKey);
      return -1;
    }
  } else if (handle->execHandle.subType == TOPIC_SUB_TYPE__DB) {
    handle->pWalReader = walOpenReader(pVnode->pWal, NULL, 0);
    handle->execHandle.pTqReader = tqReaderOpen(pVnode);

    buildSnapContext(reader.vnode, reader.version, 0, handle->execHandle.subType, handle->fetchMeta,
                     (SSnapContext**)(&reader.sContext));
    handle->execHandle.task = qCreateQueueExecTaskInfo(NULL, &reader, vgId, NULL, handle->consumerId);
  } else if (handle->execHandle.subType == TOPIC_SUB_TYPE__TABLE) {
    handle->pWalReader = walOpenReader(pVnode->pWal, NULL, 0);

    if(handle->execHandle.execTb.qmsg != NULL && strcmp(handle->execHandle.execTb.qmsg, "") != 0) {
      if (nodesStringToNode(handle->execHandle.execTb.qmsg, &handle->execHandle.execTb.node) != 0) {
        tqError("nodesStringToNode error in sub stable, since %s", terrstr());
        return -1;
      }
    }
    buildSnapContext(reader.vnode, reader.version, handle->execHandle.execTb.suid, handle->execHandle.subType,
                     handle->fetchMeta, (SSnapContext**)(&reader.sContext));
    handle->execHandle.task = qCreateQueueExecTaskInfo(NULL, &reader, vgId, NULL, handle->consumerId);

    SArray* tbUidList = NULL;
    int ret = qGetTableList(handle->execHandle.execTb.suid, pVnode, handle->execHandle.execTb.node, &tbUidList, handle->execHandle.task);
    if(ret != TDB_CODE_SUCCESS) {
      tqError("qGetTableList error:%d handle %s consumer:0x%" PRIx64, ret, handle->subKey, handle->consumerId);
      taosArrayDestroy(tbUidList);
      return -1;
    }
    tqInfo("vgId:%d, tq try to get ctb for stb subscribe, suid:%" PRId64, pVnode->config.vgId, handle->execHandle.execTb.suid);
    handle->execHandle.pTqReader = tqReaderOpen(pVnode);
    tqReaderSetTbUidList(handle->execHandle.pTqReader, tbUidList, NULL);
    taosArrayDestroy(tbUidList);
  }
  return 0;
}

static int restoreHandle(STQ* pTq, void* pVal, int vLen, STqHandle* handle){
  int32_t  vgId = TD_VID(pTq->pVnode);
  SDecoder decoder;
  int32_t code = 0;
  tDecoderInit(&decoder, (uint8_t*)pVal, vLen);
  code = tDecodeSTqHandle(&decoder, handle);
  if (code) goto end;
  code = buildHandle(pTq, handle);
  if (code) goto end;
  tqInfo("restoreHandle %s consumer 0x%" PRIx64 " vgId:%d", handle->subKey, handle->consumerId, vgId);
  code = taosHashPut(pTq->pHandle, handle->subKey, strlen(handle->subKey), handle, sizeof(STqHandle));

end:
  tDecoderClear(&decoder);
  return code;
}

int32_t tqCreateHandle(STQ* pTq, SMqRebVgReq* req, STqHandle* handle){
  int32_t  vgId = TD_VID(pTq->pVnode);

  memcpy(handle->subKey, req->subKey, TSDB_SUBSCRIBE_KEY_LEN);
  handle->consumerId = req->newConsumerId;
  handle->epoch = -1;

  handle->execHandle.subType = req->subType;
  handle->fetchMeta = req->withMeta;
  if(req->subType == TOPIC_SUB_TYPE__COLUMN){
    handle->execHandle.execCol.qmsg = taosStrdup(req->qmsg);
  }else if(req->subType == TOPIC_SUB_TYPE__DB){
    handle->execHandle.execDb.pFilterOutTbUid =
        taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  }else if(req->subType == TOPIC_SUB_TYPE__TABLE){
    handle->execHandle.execTb.suid = req->suid;
    handle->execHandle.execTb.qmsg = taosStrdup(req->qmsg);
  }

  handle->snapshotVer = walGetCommittedVer(pTq->pVnode->pWal);

  if(buildHandle(pTq, handle) < 0){
    return -1;
  }
  tqInfo("tqCreateHandle %s consumer 0x%" PRIx64 " vgId:%d", handle->subKey, handle->consumerId, vgId);
  return taosHashPut(pTq->pHandle, handle->subKey, strlen(handle->subKey), handle, sizeof(STqHandle));
}

//int32_t tqMetaRestoreHandle(STQ* pTq) {
//  int  code = 0;
//  TBC* pCur = NULL;
//  if (tdbTbcOpen(pTq->pExecStore, &pCur, NULL) < 0) {
//    return -1;
//  }
//
//  void*    pKey = NULL;
//  int      kLen = 0;
//  void*    pVal = NULL;
//  int      vLen = 0;
//
//  tdbTbcMoveToFirst(pCur);
//
//  while (tdbTbcNext(pCur, &pKey, &kLen, &pVal, &vLen) == 0) {
//    STqHandle handle = {0};
//    code = restoreHandle(pTq, pVal, vLen, &handle);
//    if (code < 0) {
//      tqDestroyTqHandle(&handle);
//      break;
//    }
//  }
//
//  tdbFree(pKey);
//  tdbFree(pVal);
//  tdbTbcClose(pCur);
//  return code;
//}

int32_t tqMetaGetHandle(STQ* pTq, const char* key) {
  void*    pVal = NULL;
  int      vLen = 0;

  if (tdbTbGet(pTq->pExecStore, key, (int)strlen(key), &pVal, &vLen) < 0) {
    return -1;
  }
  STqHandle handle = {0};
  int code = restoreHandle(pTq, pVal, vLen, &handle);
  if (code < 0){
    tqDestroyTqHandle(&handle);
  }
  tdbFree(pVal);
  return code;
}
