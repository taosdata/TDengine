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
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pHandle->subKey));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pHandle->fetchMeta));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pHandle->consumerId));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pHandle->snapshotVer));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pHandle->epoch));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pHandle->execHandle.subType));
  if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pHandle->execHandle.execCol.qmsg));
  } else if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__DB) {
    int32_t size = taosHashGetSize(pHandle->execHandle.execDb.pFilterOutTbUid);
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, size));
    void* pIter = NULL;
    pIter = taosHashIterate(pHandle->execHandle.execDb.pFilterOutTbUid, pIter);
    while (pIter) {
      int64_t* tbUid = (int64_t*)taosHashGetKey(pIter, NULL);
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, *tbUid));
      pIter = taosHashIterate(pHandle->execHandle.execDb.pFilterOutTbUid, pIter);
    }
  } else if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__TABLE) {
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pHandle->execHandle.execTb.suid));
    if (pHandle->execHandle.execTb.qmsg != NULL) {
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pHandle->execHandle.execTb.qmsg));
    }
  }
  tEndEncode(pEncoder);
_exit:
  if (code) {
    return code;
  } else {
    return pEncoder->pos;
  }
}

int32_t tDecodeSTqHandle(SDecoder* pDecoder, STqHandle* pHandle) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pHandle->subKey));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pHandle->fetchMeta));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pHandle->consumerId));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pHandle->snapshotVer));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pHandle->epoch));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pHandle->execHandle.subType));
  if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
    TAOS_CHECK_EXIT(tDecodeCStrAlloc(pDecoder, &pHandle->execHandle.execCol.qmsg));
  } else if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__DB) {
    pHandle->execHandle.execDb.pFilterOutTbUid =
        taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    if (pHandle->execHandle.execDb.pFilterOutTbUid == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    int32_t size = 0;
    TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &size));
    for (int32_t i = 0; i < size; i++) {
      int64_t tbUid = 0;
      TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &tbUid));
      TAOS_CHECK_EXIT(taosHashPut(pHandle->execHandle.execDb.pFilterOutTbUid, &tbUid, sizeof(int64_t), NULL, 0));
    }
  } else if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__TABLE) {
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pHandle->execHandle.execTb.suid));
    if (!tDecodeIsEnd(pDecoder)) {
      TAOS_CHECK_EXIT(tDecodeCStrAlloc(pDecoder, &pHandle->execHandle.execTb.qmsg));
    }
  }
  tEndDecode(pDecoder);

_exit:
  return code;
}

int32_t tqMetaDecodeCheckInfo(STqCheckInfo* info, void* pVal, int32_t vLen) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, (uint8_t*)pVal, vLen);
  int32_t code = tDecodeSTqCheckInfo(&decoder, info);
  tDecoderClear(&decoder);

  if (code != 0) {
    tDeleteSTqCheckInfo(info);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return code;
}

int32_t tqMetaDecodeOffsetInfo(STqOffset* info, void* pVal, int32_t vLen) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, (uint8_t*)pVal, vLen);
  int32_t code = tDecodeSTqOffset(&decoder, info);
  tDecoderClear(&decoder);

  if (code != 0) {
    tDeleteSTqOffset(info);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return code;
}

int32_t tqMetaSaveOffset(STQ* pTq, STqOffset* pOffset) {
  void*    buf = NULL;
  int32_t  code = TDB_CODE_SUCCESS;
  int32_t  vlen;
  SEncoder encoder = {0};
  tEncodeSize(tEncodeSTqOffset, pOffset, vlen, code);
  if (code < 0) {
    goto END;
  }

  buf = taosMemoryCalloc(1, vlen);
  if (buf == NULL) {
    code = terrno;
    goto END;
  }

  tEncoderInit(&encoder, buf, vlen);
  code = tEncodeSTqOffset(&encoder, pOffset);
  if (code < 0) {
    goto END;
  }

  TQ_ERR_GO_TO_END(tqMetaSaveInfo(pTq, pTq->pOffsetStore, pOffset->subKey, strlen(pOffset->subKey), buf, vlen));

END:
  tEncoderClear(&encoder);
  taosMemoryFree(buf);
  return code;
}

int32_t tqMetaSaveInfo(STQ* pTq, TTB* ttb, const void* key, int32_t kLen, const void* value, int32_t vLen) {
  int32_t code = TDB_CODE_SUCCESS;
  TXN*    txn = NULL;

  TQ_ERR_GO_TO_END(
      tdbBegin(pTq->pMetaDB, &txn, tdbDefaultMalloc, tdbDefaultFree, NULL, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED));
  TQ_ERR_GO_TO_END(tdbTbUpsert(ttb, key, kLen, value, vLen, txn));
  TQ_ERR_GO_TO_END(tdbCommit(pTq->pMetaDB, txn));
  TQ_ERR_GO_TO_END(tdbPostCommit(pTq->pMetaDB, txn));

  return 0;

END:
  tdbAbort(pTq->pMetaDB, txn);
  return code;
}

int32_t tqMetaDeleteInfo(STQ* pTq, TTB* ttb, const void* key, int32_t kLen) {
  int32_t code = TDB_CODE_SUCCESS;
  TXN*    txn = NULL;

  TQ_ERR_GO_TO_END(
      tdbBegin(pTq->pMetaDB, &txn, tdbDefaultMalloc, tdbDefaultFree, NULL, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED));
  TQ_ERR_GO_TO_END(tdbTbDelete(ttb, key, kLen, txn));
  TQ_ERR_GO_TO_END(tdbCommit(pTq->pMetaDB, txn));
  TQ_ERR_GO_TO_END(tdbPostCommit(pTq->pMetaDB, txn));

  return 0;

END:
  tdbAbort(pTq->pMetaDB, txn);
  return code;
}

int32_t tqMetaGetOffset(STQ* pTq, const char* subkey, STqOffset** pOffset) {
  void* data = taosHashGet(pTq->pOffset, subkey, strlen(subkey));
  if (data == NULL) {
    int vLen = 0;
    if (tdbTbGet(pTq->pOffsetStore, subkey, strlen(subkey), &data, &vLen) < 0) {
      tdbFree(data);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    STqOffset offset = {0};
    if (tqMetaDecodeOffsetInfo(&offset, data, vLen) != TDB_CODE_SUCCESS) {
      tdbFree(data);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    if (taosHashPut(pTq->pOffset, subkey, strlen(subkey), &offset, sizeof(STqOffset)) != 0) {
      tDeleteSTqOffset(&offset);
      tdbFree(data);
      return terrno;
    }
    tdbFree(data);

    *pOffset = taosHashGet(pTq->pOffset, subkey, strlen(subkey));
    if (*pOffset == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  } else {
    *pOffset = data;
  }
  return 0;
}

int32_t tqMetaSaveHandle(STQ* pTq, const char* key, const STqHandle* pHandle) {
  int32_t  code = TDB_CODE_SUCCESS;
  int32_t  vlen;
  void*    buf = NULL;
  SEncoder encoder = {0};
  tEncodeSize(tEncodeSTqHandle, pHandle, vlen, code);
  if (code < 0) {
    goto END;
  }

  tqDebug("tq save %s(%d) handle consumer:0x%" PRIx64 " epoch:%d vgId:%d", pHandle->subKey,
          (int32_t)strlen(pHandle->subKey), pHandle->consumerId, pHandle->epoch, TD_VID(pTq->pVnode));

  buf = taosMemoryCalloc(1, vlen);
  if (buf == NULL) {
    code = terrno;
    goto END;
  }

  tEncoderInit(&encoder, buf, vlen);
  code = tEncodeSTqHandle(&encoder, pHandle);
  if (code < 0) {
    goto END;
  }

  TQ_ERR_GO_TO_END(tqMetaSaveInfo(pTq, pTq->pExecStore, key, (int)strlen(key), buf, vlen));

END:
  tEncoderClear(&encoder);
  taosMemoryFree(buf);
  return code;
}

static int tqMetaInitHandle(STQ* pTq, STqHandle* handle) {
  int32_t code = TDB_CODE_SUCCESS;

  SVnode* pVnode = pTq->pVnode;
  int32_t vgId = TD_VID(pVnode);

  handle->pRef = walOpenRef(pVnode->pWal);

  TQ_NULL_GO_TO_END(handle->pRef);
  TQ_ERR_GO_TO_END(walSetRefVer(handle->pRef, handle->snapshotVer));

  SReadHandle reader = {
      .vnode = pVnode,
      .initTableReader = true,
      .initTqReader = true,
      .version = handle->snapshotVer,
  };

  initStorageAPI(&reader.api);

  if (handle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
    handle->execHandle.task = qCreateQueueExecTaskInfo(handle->execHandle.execCol.qmsg, &reader, vgId,
                                                       &handle->execHandle.numOfCols, handle->consumerId);
    TQ_NULL_GO_TO_END(handle->execHandle.task);
    void* scanner = NULL;
    qExtractStreamScanner(handle->execHandle.task, &scanner);
    TQ_NULL_GO_TO_END(scanner);
    handle->execHandle.pTqReader = qExtractReaderFromStreamScanner(scanner);
    TQ_NULL_GO_TO_END(handle->execHandle.pTqReader);
  } else if (handle->execHandle.subType == TOPIC_SUB_TYPE__DB) {
    handle->pWalReader = walOpenReader(pVnode->pWal, NULL, 0);
    TQ_NULL_GO_TO_END(handle->pWalReader);
    handle->execHandle.pTqReader = tqReaderOpen(pVnode);
    TQ_NULL_GO_TO_END(handle->execHandle.pTqReader);
    TQ_ERR_GO_TO_END(buildSnapContext(reader.vnode, reader.version, 0, handle->execHandle.subType, handle->fetchMeta,
                                      (SSnapContext**)(&reader.sContext)));
    handle->execHandle.task = qCreateQueueExecTaskInfo(NULL, &reader, vgId, NULL, handle->consumerId);
    TQ_NULL_GO_TO_END(handle->execHandle.task);
  } else if (handle->execHandle.subType == TOPIC_SUB_TYPE__TABLE) {
    handle->pWalReader = walOpenReader(pVnode->pWal, NULL, 0);
    TQ_NULL_GO_TO_END(handle->pWalReader);
    if (handle->execHandle.execTb.qmsg != NULL && strcmp(handle->execHandle.execTb.qmsg, "") != 0) {
      if (nodesStringToNode(handle->execHandle.execTb.qmsg, &handle->execHandle.execTb.node) != 0) {
        tqError("nodesStringToNode error in sub stable, since %s", terrstr());
        return TSDB_CODE_SCH_INTERNAL_ERROR;
      }
    }
    TQ_ERR_GO_TO_END(buildSnapContext(reader.vnode, reader.version, handle->execHandle.execTb.suid,
                                      handle->execHandle.subType, handle->fetchMeta,
                                      (SSnapContext**)(&reader.sContext)));
    handle->execHandle.task = qCreateQueueExecTaskInfo(NULL, &reader, vgId, NULL, handle->consumerId);
    TQ_NULL_GO_TO_END(handle->execHandle.task);
    SArray* tbUidList = NULL;
    int     ret = qGetTableList(handle->execHandle.execTb.suid, pVnode, handle->execHandle.execTb.node, &tbUidList,
                                handle->execHandle.task);
    if (ret != TDB_CODE_SUCCESS) {
      tqError("qGetTableList error:%d handle %s consumer:0x%" PRIx64, ret, handle->subKey, handle->consumerId);
      taosArrayDestroy(tbUidList);
      return TSDB_CODE_SCH_INTERNAL_ERROR;
    }
    tqInfo("vgId:%d, tq try to get ctb for stb subscribe, suid:%" PRId64, pVnode->config.vgId,
           handle->execHandle.execTb.suid);
    handle->execHandle.pTqReader = tqReaderOpen(pVnode);
    TQ_NULL_GO_TO_END(handle->execHandle.pTqReader);
    tqReaderSetTbUidList(handle->execHandle.pTqReader, tbUidList, NULL);
    taosArrayDestroy(tbUidList);
  }

END:
  return code;
}

static int32_t tqMetaRestoreHandle(STQ* pTq, void* pVal, int vLen, STqHandle* handle) {
  int32_t  vgId = TD_VID(pTq->pVnode);
  SDecoder decoder = {0};
  int32_t  code = TDB_CODE_SUCCESS;

  tDecoderInit(&decoder, (uint8_t*)pVal, vLen);
  TQ_ERR_GO_TO_END(tDecodeSTqHandle(&decoder, handle));
  TQ_ERR_GO_TO_END(tqMetaInitHandle(pTq, handle));
  tqInfo("tqMetaRestoreHandle %s consumer 0x%" PRIx64 " vgId:%d", handle->subKey, handle->consumerId, vgId);
  code = taosHashPut(pTq->pHandle, handle->subKey, strlen(handle->subKey), handle, sizeof(STqHandle));

END:
  tDecoderClear(&decoder);
  return code;
}

int32_t tqMetaCreateHandle(STQ* pTq, SMqRebVgReq* req, STqHandle* handle) {
  int32_t vgId = TD_VID(pTq->pVnode);

  (void)memcpy(handle->subKey, req->subKey, TSDB_SUBSCRIBE_KEY_LEN);
  handle->consumerId = req->newConsumerId;

  handle->execHandle.subType = req->subType;
  handle->fetchMeta = req->withMeta;
  if (req->subType == TOPIC_SUB_TYPE__COLUMN) {
    void* tmp = taosStrdup(req->qmsg);
    if (tmp == NULL) {
      return terrno;
    }
    handle->execHandle.execCol.qmsg = tmp;
  } else if (req->subType == TOPIC_SUB_TYPE__DB) {
    handle->execHandle.execDb.pFilterOutTbUid =
        taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    if (handle->execHandle.execDb.pFilterOutTbUid == NULL) {
      return terrno;
    }
  } else if (req->subType == TOPIC_SUB_TYPE__TABLE) {
    handle->execHandle.execTb.suid = req->suid;
    void* tmp = taosStrdup(req->qmsg);
    if (tmp == NULL) {
      return terrno;
    }
    handle->execHandle.execTb.qmsg = tmp;
  }

  handle->snapshotVer = walGetCommittedVer(pTq->pVnode->pWal);

  int32_t code = tqMetaInitHandle(pTq, handle);
  if (code != 0) {
    return code;
  }
  tqInfo("tqMetaCreateHandle %s consumer 0x%" PRIx64 " vgId:%d, snapshotVer:%" PRId64, handle->subKey,
         handle->consumerId, vgId, handle->snapshotVer);
  return taosHashPut(pTq->pHandle, handle->subKey, strlen(handle->subKey), handle, sizeof(STqHandle));
}

static int32_t tqMetaTransformInfo(TDB* pMetaDB, TTB* pOld, TTB* pNew) {
  TBC*  pCur = NULL;
  void* pKey = NULL;
  int   kLen = 0;
  void* pVal = NULL;
  int   vLen = 0;
  TXN*  txn = NULL;

  int32_t code = TDB_CODE_SUCCESS;

  TQ_ERR_GO_TO_END(tdbTbcOpen(pOld, &pCur, NULL));
  TQ_ERR_GO_TO_END(
      tdbBegin(pMetaDB, &txn, tdbDefaultMalloc, tdbDefaultFree, NULL, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED));

  TQ_ERR_GO_TO_END(tdbTbcMoveToFirst(pCur));
  while (tdbTbcNext(pCur, &pKey, &kLen, &pVal, &vLen) == 0) {
    TQ_ERR_GO_TO_END(tdbTbUpsert(pNew, pKey, kLen, pVal, vLen, txn));
  }

  TQ_ERR_GO_TO_END(tdbCommit(pMetaDB, txn));
  TQ_ERR_GO_TO_END(tdbPostCommit(pMetaDB, txn));

END:
  tdbFree(pKey);
  tdbFree(pVal);
  tdbTbcClose(pCur);
  return code;
}

int32_t tqMetaGetHandle(STQ* pTq, const char* key, STqHandle** pHandle) {
  void* data = taosHashGet(pTq->pHandle, key, strlen(key));
  if (data == NULL) {
    int vLen = 0;
    if (tdbTbGet(pTq->pExecStore, key, (int)strlen(key), &data, &vLen) < 0) {
      tdbFree(data);
      return TSDB_CODE_MND_SUBSCRIBE_NOT_EXIST;
    }
    STqHandle handle = {0};
    if (tqMetaRestoreHandle(pTq, data, vLen, &handle) != 0) {
      tdbFree(data);
      tqDestroyTqHandle(&handle);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    tdbFree(data);
    *pHandle = taosHashGet(pTq->pHandle, key, strlen(key));
    if (*pHandle == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  } else {
    *pHandle = data;
  }
  return TDB_CODE_SUCCESS;
}

int32_t tqMetaOpenTdb(STQ* pTq) {
  int32_t code = TDB_CODE_SUCCESS;
  TQ_ERR_GO_TO_END(tdbOpen(pTq->path, 16 * 1024, 1, &pTq->pMetaDB, 0, 0, NULL));
  TQ_ERR_GO_TO_END(tdbTbOpen("tq.db", -1, -1, NULL, pTq->pMetaDB, &pTq->pExecStore, 0));
  TQ_ERR_GO_TO_END(tdbTbOpen("tq.check.db", -1, -1, NULL, pTq->pMetaDB, &pTq->pCheckStore, 0));
  TQ_ERR_GO_TO_END(tdbTbOpen("tq.offset.db", -1, -1, NULL, pTq->pMetaDB, &pTq->pOffsetStore, 0));

END:
  return code;
}

static int32_t replaceTqPath(char** path) {
  char*   tpath = NULL;
  int32_t code = tqBuildFName(&tpath, *path, TQ_SUBSCRIBE_NAME);
  if (code != 0) {
    return code;
  }
  taosMemoryFree(*path);
  *path = tpath;
  return TDB_CODE_SUCCESS;
}

static int32_t tqMetaRestoreCheckInfo(STQ* pTq) {
  TBC*         pCur = NULL;
  void*        pKey = NULL;
  int          kLen = 0;
  void*        pVal = NULL;
  int          vLen = 0;
  int32_t      code = 0;
  STqCheckInfo info = {0};

  TQ_ERR_GO_TO_END(tdbTbcOpen(pTq->pCheckStore, &pCur, NULL));
  TQ_ERR_GO_TO_END(tdbTbcMoveToFirst(pCur));

  while (tdbTbcNext(pCur, &pKey, &kLen, &pVal, &vLen) == 0) {
    TQ_ERR_GO_TO_END(tqMetaDecodeCheckInfo(&info, pVal, vLen));
    TQ_ERR_GO_TO_END(taosHashPut(pTq->pCheckInfo, info.topic, strlen(info.topic), &info, sizeof(STqCheckInfo)));
  }
  info.colIdList = NULL;

END:
  tdbFree(pKey);
  tdbFree(pVal);
  tdbTbcClose(pCur);
  tDeleteSTqCheckInfo(&info);
  return code;
}

int32_t tqMetaOpen(STQ* pTq) {
  char*   maindb = NULL;
  char*   offsetNew = NULL;
  int32_t code = TDB_CODE_SUCCESS;
  TQ_ERR_GO_TO_END(tqBuildFName(&maindb, pTq->path, TDB_MAINDB_NAME));
  if (!taosCheckExistFile(maindb)) {
    TQ_ERR_GO_TO_END(replaceTqPath(&pTq->path));
    TQ_ERR_GO_TO_END(tqMetaOpenTdb(pTq));
  } else {
    TQ_ERR_GO_TO_END(tqMetaTransform(pTq));
    TQ_ERR_GO_TO_END(taosRemoveFile(maindb));
  }

  TQ_ERR_GO_TO_END(tqBuildFName(&offsetNew, pTq->path, TQ_OFFSET_NAME));
  if (taosCheckExistFile(offsetNew)) {
    TQ_ERR_GO_TO_END(tqOffsetRestoreFromFile(pTq, offsetNew));
    TQ_ERR_GO_TO_END(taosRemoveFile(offsetNew));
  }

  TQ_ERR_GO_TO_END(tqMetaRestoreCheckInfo(pTq));

END:
  taosMemoryFree(maindb);
  taosMemoryFree(offsetNew);
  return code;
}

int32_t tqMetaTransform(STQ* pTq) {
  int32_t code = TDB_CODE_SUCCESS;
  TDB*    pMetaDB = NULL;
  TTB*    pExecStore = NULL;
  TTB*    pCheckStore = NULL;
  char*   offsetNew = NULL;
  char*   offset = NULL;
  TQ_ERR_GO_TO_END(tqBuildFName(&offset, pTq->path, TQ_OFFSET_NAME));

  TQ_ERR_GO_TO_END(tdbOpen(pTq->path, 16 * 1024, 1, &pMetaDB, 0, 0, NULL));
  TQ_ERR_GO_TO_END(tdbTbOpen("tq.db", -1, -1, NULL, pMetaDB, &pExecStore, 0));
  TQ_ERR_GO_TO_END(tdbTbOpen("tq.check.db", -1, -1, NULL, pMetaDB, &pCheckStore, 0));

  TQ_ERR_GO_TO_END(replaceTqPath(&pTq->path));
  TQ_ERR_GO_TO_END(tqMetaOpenTdb(pTq));

  TQ_ERR_GO_TO_END(tqMetaTransformInfo(pTq->pMetaDB, pExecStore, pTq->pExecStore));
  TQ_ERR_GO_TO_END(tqMetaTransformInfo(pTq->pMetaDB, pCheckStore, pTq->pCheckStore));

  TQ_ERR_GO_TO_END(tqBuildFName(&offsetNew, pTq->path, TQ_OFFSET_NAME));

  if (taosCheckExistFile(offset)) {
    if (taosCopyFile(offset, offsetNew) < 0) {
      tqError("copy offset file error");
    } else {
      TQ_ERR_GO_TO_END(taosRemoveFile(offset));
    }
  }

END:
  taosMemoryFree(offset);
  taosMemoryFree(offsetNew);

  tdbTbClose(pExecStore);
  tdbTbClose(pCheckStore);
  tdbClose(pMetaDB);
  return code;
}

void tqMetaClose(STQ* pTq) {
  int32_t ret = 0;
  if (pTq->pExecStore) {
    tdbTbClose(pTq->pExecStore);
  }
  if (pTq->pCheckStore) {
    tdbTbClose(pTq->pCheckStore);
  }
  if (pTq->pOffsetStore) {
    tdbTbClose(pTq->pOffsetStore);
  }
  tdbClose(pTq->pMetaDB);
}
