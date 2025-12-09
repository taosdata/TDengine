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

#include "meta.h"

// SMetaSnapReader ========================================
struct SMetaSnapReader {
  SMeta*  pMeta;
  int64_t sver;
  int64_t ever;
  TBC*    pTbc;
  int32_t iLoop;
};

int32_t metaSnapReaderOpen(SMeta* pMeta, int64_t sver, int64_t ever, SMetaSnapReader** ppReader) {
  int32_t          code = 0;
  int32_t          lino;
  int32_t          c = 0;
  SMetaSnapReader* pReader = NULL;

  // alloc
  pReader = (SMetaSnapReader*)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }
  pReader->pMeta = pMeta;
  pReader->sver = sver;
  pReader->ever = ever;

  // impl
  code = tdbTbcOpen(pMeta->pTbDb, &pReader->pTbc, NULL);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tdbTbcMoveTo(pReader->pTbc, &(STbDbKey){.version = sver, .uid = INT64_MIN}, sizeof(STbDbKey), &c);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pMeta->pVnode), __func__, __FILE__, lino, tstrerror(code));
    metaSnapReaderClose(&pReader);
    *ppReader = NULL;
  } else {
    metaInfo("vgId:%d, %s success", TD_VID(pMeta->pVnode), __func__);
    *ppReader = pReader;
  }
  return code;
}

void metaSnapReaderClose(SMetaSnapReader** ppReader) {
  if (ppReader && *ppReader) {
    tdbTbcClose((*ppReader)->pTbc);
    taosMemoryFree(*ppReader);
    *ppReader = NULL;
  }
}

extern int metaDecodeEntryImpl(SDecoder* pCoder, SMetaEntry* pME, bool headerOnly);

static int32_t metaDecodeEntryHeader(void* data, int32_t size, SMetaEntry* entry) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, (uint8_t*)data, size);

  int32_t code = metaDecodeEntryImpl(&decoder, entry, true);
  if (code) {
    tDecoderClear(&decoder);
    return code;
  }

  tDecoderClear(&decoder);
  return 0;
}

int32_t metaSnapRead(SMetaSnapReader* pReader, uint8_t** ppData) {
  int32_t     code = 0;
  const void* pKey = NULL;
  const void* pData = NULL;
  int32_t     nKey = 0;
  int32_t     nData = 0;
  STbDbKey    key;
  int32_t     c;

  *ppData = NULL;
  while (pReader->iLoop < 2) {
    if (tdbTbcGet(pReader->pTbc, &pKey, &nKey, &pData, &nData) != 0 || ((STbDbKey*)pKey)->version > pReader->ever) {
      pReader->iLoop++;

      // Reopen the cursor to read from the beginning
      tdbTbcClose(pReader->pTbc);
      pReader->pTbc = NULL;
      code = tdbTbcOpen(pReader->pMeta->pTbDb, &pReader->pTbc, NULL);
      if (code) {
        metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pReader->pMeta->pVnode), __func__, __FILE__, __LINE__,
                  tstrerror(code));
        goto _exit;
      }

      code = tdbTbcMoveTo(pReader->pTbc, &(STbDbKey){.version = pReader->sver, .uid = INT64_MIN}, sizeof(STbDbKey), &c);
      if (code) {
        metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pReader->pMeta->pVnode), __func__, __FILE__, __LINE__,
                  tstrerror(code));
        goto _exit;
      }

      continue;
    }

    // Decode meta entry
    SMetaEntry entry = {0};
    code = metaDecodeEntryHeader((void*)pData, nData, &entry);
    if (code) {
      metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pReader->pMeta->pVnode), __func__, __FILE__, __LINE__,
                tstrerror(code));
      goto _exit;
    }

    key = ((STbDbKey*)pKey)[0];
    if (key.version < pReader->sver                                       //
        || (pReader->iLoop == 0 && TABS(entry.type) != TSDB_SUPER_TABLE)  // First loop send super table entry
        || (pReader->iLoop == 1 && TABS(entry.type) == TSDB_SUPER_TABLE)  // Second loop send non-super table entry
    ) {
      if (tdbTbcMoveToNext(pReader->pTbc) != 0) {
        metaTrace("vgId:%d, vnode snapshot meta read data done", TD_VID(pReader->pMeta->pVnode));
      }
      continue;
    }

    if (!pData || !nData) {
      metaError("meta/snap: invalide nData: %" PRId32 " meta snap read failed.", nData);
      goto _exit;
    }

    *ppData = taosMemoryMalloc(sizeof(SSnapDataHdr) + nData);
    if (*ppData == NULL) {
      code = terrno;
      goto _exit;
    }

    SSnapDataHdr* pHdr = (SSnapDataHdr*)(*ppData);
    pHdr->type = SNAP_DATA_META;
    pHdr->size = nData;
    memcpy(pHdr->data, pData, nData);

    metaDebug("vgId:%d, vnode snapshot meta read data, version:%" PRId64 " uid:%" PRId64 " blockLen:%d",
              TD_VID(pReader->pMeta->pVnode), key.version, key.uid, nData);

    if (tdbTbcMoveToNext(pReader->pTbc) != 0) {
      metaTrace("vgId:%d, vnode snapshot meta read data done", TD_VID(pReader->pMeta->pVnode));
    }
    break;
  }

_exit:
  if (code) {
    metaError("vgId:%d, vnode snapshot meta read data failed since %s", TD_VID(pReader->pMeta->pVnode),
              tstrerror(code));
  }
  return code;
}

// SMetaSnapWriter ========================================
struct SMetaSnapWriter {
  SMeta*  pMeta;
  int64_t sver;
  int64_t ever;
};

int32_t metaSnapWriterOpen(SMeta* pMeta, int64_t sver, int64_t ever, SMetaSnapWriter** ppWriter) {
  int32_t          code = 0;
  int32_t          lino;
  SMetaSnapWriter* pWriter;

  // alloc
  pWriter = (SMetaSnapWriter*)taosMemoryCalloc(1, sizeof(*pWriter));
  if (pWriter == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }
  pWriter->pMeta = pMeta;
  pWriter->sver = sver;
  pWriter->ever = ever;

  code = metaBegin(pMeta, META_BEGIN_HEAP_NIL);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pMeta->pVnode), __func__, __FILE__, lino, tstrerror(code));
    taosMemoryFree(pWriter);
    *ppWriter = NULL;
  } else {
    metaDebug("vgId:%d, %s success", TD_VID(pMeta->pVnode), __func__);
    *ppWriter = pWriter;
  }
  return code;
}

int32_t metaSnapWriterClose(SMetaSnapWriter** ppWriter, int8_t rollback) {
  int32_t          code = 0;
  SMetaSnapWriter* pWriter = *ppWriter;

  if (rollback) {
    metaInfo("vgId:%d, meta snapshot writer close and rollback start ", TD_VID(pWriter->pMeta->pVnode));
    code = metaAbort(pWriter->pMeta);
    metaInfo("vgId:%d, meta snapshot writer close and rollback finished, code:0x%x", TD_VID(pWriter->pMeta->pVnode),
             code);
    if (code) goto _err;
  } else {
    code = metaCommit(pWriter->pMeta, pWriter->pMeta->txn);
    if (code) goto _err;
    code = metaFinishCommit(pWriter->pMeta, pWriter->pMeta->txn);
    if (code) goto _err;
  }
  taosMemoryFree(pWriter);
  *ppWriter = NULL;

  return code;

_err:
  metaError("vgId:%d, meta snapshot writer close failed since %s", TD_VID(pWriter->pMeta->pVnode), tstrerror(code));
  return code;
}

int32_t metaSnapWrite(SMetaSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SMeta*     pMeta = pWriter->pMeta;
  SMetaEntry metaEntry = {0};
  SDecoder*  pDecoder = &(SDecoder){0};

  tDecoderInit(pDecoder, pData + sizeof(SSnapDataHdr), nData - sizeof(SSnapDataHdr));
  code = metaDecodeEntry(pDecoder, &metaEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  metaHandleSyncEntry(pMeta, &metaEntry);

_exit:
  if (code) {
    metaError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pMeta->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }
  tDecoderClear(pDecoder);
  return code;
}

typedef struct STableInfoForChildTable {
  char*           tableName;
  SSchemaWrapper* schemaRow;
  SSchemaWrapper* tagRow;
  SExtSchema*     pExtSchemas;
} STableInfoForChildTable;

static void destroySTableInfoForChildTable(void* data) {
  STableInfoForChildTable* pData = (STableInfoForChildTable*)data;
  taosMemoryFree(pData->tableName);
  tDeleteSchemaWrapper(pData->schemaRow);
  tDeleteSchemaWrapper(pData->tagRow);
  taosMemoryFreeClear(pData->pExtSchemas);
}

static int32_t MoveToSnapShotVersion(SSnapContext* ctx) {
  int32_t code = 0;
  tdbTbcClose((TBC*)ctx->pCur);
  code = tdbTbcOpen(ctx->pMeta->pTbDb, (TBC**)&ctx->pCur, NULL);
  if (code != 0) {
    return TAOS_GET_TERRNO(code);
  }
  STbDbKey key = {.version = ctx->snapVersion, .uid = INT64_MAX};
  int      c = 0;
  code = tdbTbcMoveTo((TBC*)ctx->pCur, &key, sizeof(key), &c);
  if (code != 0) {
    return TAOS_GET_TERRNO(code);
  }
  if (c < 0) {
    if (tdbTbcMoveToPrev((TBC*)ctx->pCur) != 0) {
      metaTrace("vgId:%d, vnode snapshot move to prev failed", TD_VID(ctx->pMeta->pVnode));
    }
  }
  return 0;
}

static int32_t MoveToPosition(SSnapContext* ctx, int64_t ver, int64_t uid) {
  tdbTbcClose((TBC*)ctx->pCur);
  int32_t code = tdbTbcOpen(ctx->pMeta->pTbDb, (TBC**)&ctx->pCur, NULL);
  if (code != 0) {
    return TAOS_GET_TERRNO(code);
  }
  STbDbKey key = {.version = ver, .uid = uid};
  int      c = 0;
  code = tdbTbcMoveTo((TBC*)ctx->pCur, &key, sizeof(key), &c);
  if (code != 0) {
    return TAOS_GET_TERRNO(code);
  }
  return c;
}

static int32_t MoveToFirst(SSnapContext* ctx) {
  tdbTbcClose((TBC*)ctx->pCur);
  int32_t code = tdbTbcOpen(ctx->pMeta->pTbDb, (TBC**)&ctx->pCur, NULL);
  if (code != 0) {
    return TAOS_GET_TERRNO(code);
  }
  code = tdbTbcMoveToFirst((TBC*)ctx->pCur);
  if (code != 0) {
    return TAOS_GET_TERRNO(code);
  }
  return 0;
}

static int32_t saveSuperTableInfoForChildTable(SMetaEntry* me, SHashObj* suidInfo) {
  STableInfoForChildTable* data = (STableInfoForChildTable*)taosHashGet(suidInfo, &me->uid, sizeof(tb_uid_t));
  if (data) {
    return 0;
  }
  int32_t                 code = 0;
  STableInfoForChildTable dataTmp = {0};
  dataTmp.tableName = taosStrdup(me->name);
  if (dataTmp.tableName == NULL) {
    code = terrno;
    goto END;
  }
  dataTmp.schemaRow = tCloneSSchemaWrapper(&me->stbEntry.schemaRow);
  if (dataTmp.schemaRow == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto END;
  }
  dataTmp.tagRow = tCloneSSchemaWrapper(&me->stbEntry.schemaTag);
  if (dataTmp.tagRow == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto END;
  }
  if (me->pExtSchemas != NULL) {
    dataTmp.pExtSchemas = taosMemoryMalloc(sizeof(SExtSchema) * me->stbEntry.schemaRow.nCols);
    if (dataTmp.pExtSchemas == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto END;
    }
    memcpy(dataTmp.pExtSchemas, me->pExtSchemas, sizeof(SExtSchema) * me->stbEntry.schemaRow.nCols);
  }
  
  code = taosHashPut(suidInfo, &me->uid, sizeof(tb_uid_t), &dataTmp, sizeof(STableInfoForChildTable));
  if (code != 0) {
    goto END;
  }
  return 0;

END:
  destroySTableInfoForChildTable(&dataTmp);
  return TAOS_GET_TERRNO(code);
}

int32_t buildSnapContext(SVnode* pVnode, int64_t snapVersion, int64_t suid, int8_t subType, int8_t withMeta,
                         SSnapContext** ctxRet) {
  int32_t code = 0;
  int32_t lino = 0;
  SDecoder   dc = {0};
  void* pKey = NULL;
  void* pVal = NULL;
  int   vLen = 0, kLen = 0;

  metaRLock(pVnode->pMeta);
  SSnapContext* ctx = taosMemoryCalloc(1, sizeof(SSnapContext));
  TSDB_CHECK_NULL(ctx, code, lino, END, terrno);
  *ctxRet = ctx;
  ctx->pMeta = pVnode->pMeta;
  ctx->snapVersion = snapVersion;
  ctx->suid = suid;
  ctx->subType = subType;
  ctx->queryMeta = withMeta;
  ctx->withMeta = withMeta;
  ctx->idVersion = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  TSDB_CHECK_NULL(ctx->idVersion, code, lino, END, terrno);
  ctx->suidInfo = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  TSDB_CHECK_NULL(ctx->suidInfo, code, lino, END, terrno);
  taosHashSetFreeFp(ctx->suidInfo, destroySTableInfoForChildTable);

  ctx->index = 0;
  ctx->idList = taosArrayInit(100, sizeof(int64_t));
  TSDB_CHECK_NULL(ctx->idList, code, lino, END, terrno);

  metaDebug("tmqsnap init snapVersion:%" PRIi64, ctx->snapVersion);
  code = MoveToFirst(ctx);
  TSDB_CHECK_CODE(code, lino, END);
  while (1) {
    int32_t ret = tdbTbcNext((TBC*)ctx->pCur, &pKey, &kLen, &pVal, &vLen);
    if (ret < 0) break;
    STbDbKey* tmp = (STbDbKey*)pKey;
    if (tmp->version > ctx->snapVersion) break;

    SIdInfo* idData = (SIdInfo*)taosHashGet(ctx->idVersion, &tmp->uid, sizeof(tb_uid_t));
    if (idData) {
      continue;
    }

    // check if table exist for now, need optimize later
    if (tdbTbGet(ctx->pMeta->pUidIdx, &tmp->uid, sizeof(tb_uid_t), NULL, NULL) < 0) {
      continue;
    }

    SMetaEntry me = {0};
    tDecoderInit(&dc, pVal, vLen);
    code = metaDecodeEntry(&dc, &me);
    TSDB_CHECK_CODE(code, lino, END);
    if (ctx->subType == TOPIC_SUB_TYPE__TABLE) {
      if ((me.uid != ctx->suid && me.type == TSDB_SUPER_TABLE) ||
          (me.ctbEntry.suid != ctx->suid && me.type == TSDB_CHILD_TABLE)) {
        tDecoderClear(&dc);
        continue;
      }
    } else if (ctx->subType == TOPIC_SUB_TYPE__DB) {
      if (me.type == TSDB_VIRTUAL_NORMAL_TABLE ||
          me.type == TSDB_VIRTUAL_CHILD_TABLE ||
          TABLE_IS_VIRTUAL(me.flags)) {
        tDecoderClear(&dc);
        continue;
      }
    }

    TSDB_CHECK_NULL(taosArrayPush(ctx->idList, &tmp->uid), code, lino, END, terrno);
    metaDebug("tmqsnap init idlist name:%s, uid:%" PRIi64, me.name, tmp->uid);
    tDecoderClear(&dc);

    SIdInfo info = {0};
    code = taosHashPut(ctx->idVersion, &tmp->uid, sizeof(tb_uid_t), &info, sizeof(SIdInfo));
    TSDB_CHECK_CODE(code, lino, END);
  }
  taosHashClear(ctx->idVersion);

  code = MoveToSnapShotVersion(ctx);
  TSDB_CHECK_CODE(code, lino, END);

  while (1) {
    int32_t ret = tdbTbcPrev((TBC*)ctx->pCur, &pKey, &kLen, &pVal, &vLen);
    if (ret < 0) break;

    STbDbKey* tmp = (STbDbKey*)pKey;
    SIdInfo*  idData = (SIdInfo*)taosHashGet(ctx->idVersion, &tmp->uid, sizeof(tb_uid_t));
    if (idData) {
      continue;
    }
    SIdInfo info = {.version = tmp->version, .index = 0};
    code = taosHashPut(ctx->idVersion, &tmp->uid, sizeof(tb_uid_t), &info, sizeof(SIdInfo));
    TSDB_CHECK_CODE(code, lino, END);

    SMetaEntry me = {0};
    tDecoderInit(&dc, pVal, vLen);
    code = metaDecodeEntry(&dc, &me);
    TSDB_CHECK_CODE(code, lino, END);

    if (ctx->subType == TOPIC_SUB_TYPE__TABLE) {
      if ((me.uid != ctx->suid && me.type == TSDB_SUPER_TABLE) ||
          (me.ctbEntry.suid != ctx->suid && me.type == TSDB_CHILD_TABLE)) {
        tDecoderClear(&dc);
        continue;
      }
    } else if (ctx->subType == TOPIC_SUB_TYPE__DB) {
      if (me.type == TSDB_VIRTUAL_NORMAL_TABLE ||
          me.type == TSDB_VIRTUAL_CHILD_TABLE ||
          TABLE_IS_VIRTUAL(me.flags)) {
        tDecoderClear(&dc);
        continue;
      }
    }

    if ((ctx->subType == TOPIC_SUB_TYPE__DB && me.type == TSDB_SUPER_TABLE) ||
        (ctx->subType == TOPIC_SUB_TYPE__TABLE && me.uid == ctx->suid)) {
      code = saveSuperTableInfoForChildTable(&me, ctx->suidInfo);
      TSDB_CHECK_CODE(code, lino, END);
    }
    tDecoderClear(&dc);

  }

  for (int i = 0; i < taosArrayGetSize(ctx->idList); i++) {
    int64_t* uid = taosArrayGet(ctx->idList, i);
    TSDB_CHECK_NULL(uid, code, lino, END, terrno);
    SIdInfo* idData = (SIdInfo*)taosHashGet(ctx->idVersion, uid, sizeof(int64_t));
    TSDB_CHECK_NULL(idData, code, lino, END, terrno);

    idData->index = i;
    metaDebug("tmqsnap init idVersion uid:%" PRIi64 " version:%" PRIi64 " index:%d", *uid, idData->version, idData->index);
  }

END:
  tdbFree(pKey);
  tdbFree(pVal);
  tDecoderClear(&dc);

  if (ctx != NULL) {
    tdbTbcClose((TBC*)ctx->pCur);
    ctx->pCur = NULL;
  }
  metaULock(pVnode->pMeta);

  if(code != 0) {
    destroySnapContext(ctx);
    *ctxRet = NULL;
    metaError("tmqsnap build snap context failed line:%d since %s", lino, tstrerror(code));
  }
  return code;
}

void destroySnapContext(SSnapContext* ctx) {
  if (ctx == NULL) {
    return;
  }
  taosArrayDestroy(ctx->idList);
  taosHashCleanup(ctx->idVersion);
  taosHashCleanup(ctx->suidInfo);
  taosMemoryFree(ctx);
}

static int32_t buildNormalChildTableInfo(SVCreateTbReq* req, void** pBuf, int32_t* contLen) {
  int32_t            ret = 0;
  SVCreateTbBatchReq reqs = {0};

  reqs.pArray = taosArrayInit(1, sizeof(struct SVCreateTbReq));
  if (NULL == reqs.pArray) {
    ret = TAOS_GET_TERRNO(TSDB_CODE_OUT_OF_MEMORY);
    goto end;
  }
  if (taosArrayPush(reqs.pArray, req) == NULL) {
    ret = TAOS_GET_TERRNO(TSDB_CODE_OUT_OF_MEMORY);
    goto end;
  }
  reqs.nReqs = 1;

  tEncodeSize(tEncodeSVCreateTbBatchReq, &reqs, *contLen, ret);
  if (ret < 0) {
    ret = TAOS_GET_TERRNO(ret);
    goto end;
  }
  *contLen += sizeof(SMsgHead);
  *pBuf = taosMemoryMalloc(*contLen);
  if (NULL == *pBuf) {
    ret = TAOS_GET_TERRNO(TSDB_CODE_OUT_OF_MEMORY);
    goto end;
  }
  SEncoder coder = {0};
  tEncoderInit(&coder, POINTER_SHIFT(*pBuf, sizeof(SMsgHead)), *contLen);
  ret = tEncodeSVCreateTbBatchReq(&coder, &reqs);
  tEncoderClear(&coder);

  if (ret < 0) {
    taosMemoryFreeClear(*pBuf);
    ret = TAOS_GET_TERRNO(ret);
    goto end;
  }

end:
  taosArrayDestroy(reqs.pArray);
  return ret;
}

static int32_t buildSuperTableInfo(SVCreateStbReq* req, void** pBuf, int32_t* contLen) {
  int32_t ret = 0;
  tEncodeSize(tEncodeSVCreateStbReq, req, *contLen, ret);
  if (ret < 0) {
    return TAOS_GET_TERRNO(ret);
  }

  *contLen += sizeof(SMsgHead);
  *pBuf = taosMemoryMalloc(*contLen);
  if (NULL == *pBuf) {
    return TAOS_GET_TERRNO(TSDB_CODE_OUT_OF_MEMORY);
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, POINTER_SHIFT(*pBuf, sizeof(SMsgHead)), *contLen);
  ret = tEncodeSVCreateStbReq(&encoder, req);
  tEncoderClear(&encoder);
  if (ret < 0) {
    taosMemoryFreeClear(*pBuf);
    return TAOS_GET_TERRNO(ret);
  }
  return 0;
}

int32_t setForSnapShot(SSnapContext* ctx, int64_t uid) {
  if (uid == 0) {
    ctx->index = 0;
    return 0;
  }

  SIdInfo* idInfo = (SIdInfo*)taosHashGet(ctx->idVersion, &uid, sizeof(tb_uid_t));
  if (idInfo == NULL) {
    return terrno;
  }

  ctx->index = idInfo->index;

  return 0;
}

void taosXSetTablePrimaryKey(SSnapContext* ctx, int64_t uid) {
  bool            ret = false;
  SSchemaWrapper* schema = metaGetTableSchema(ctx->pMeta, uid, -1, 1, NULL, 0);
  if (schema && schema->nCols >= 2 && schema->pSchema[1].flags & COL_IS_KEY) {
    ret = true;
  }
  tDeleteSchemaWrapper(schema);
  ctx->hasPrimaryKey = ret;
}

bool taosXGetTablePrimaryKey(SSnapContext* ctx) { return ctx->hasPrimaryKey; }

int32_t getTableInfoFromSnapshot(SSnapContext* ctx, void** pBuf, int32_t* contLen, int16_t* type, int64_t* uid) {
  int32_t ret = 0;
  int32_t lino = 0;
  void*   pKey = NULL;
  void*   pVal = NULL;
  int     vLen = 0, kLen = 0;
  SDecoder   dc = {0};
  SArray* tagName = NULL;
  SArray* pTagVals = NULL;

  metaRLock(ctx->pMeta);
  while (1) {
    if (ctx->index >= taosArrayGetSize(ctx->idList)) {
      metaDebug("tmqsnap get meta end");
      ctx->index = 0;
      ctx->queryMeta = 0;  // change to get data
      goto END;
    }

    int64_t* uidTmp = taosArrayGet(ctx->idList, ctx->index);
    TSDB_CHECK_NULL(uidTmp, ret, lino, END, terrno);
    ctx->index++;
    SIdInfo* idInfo = (SIdInfo*)taosHashGet(ctx->idVersion, uidTmp, sizeof(tb_uid_t));
    TSDB_CHECK_NULL(idInfo, ret, lino, END, terrno);

    *uid = *uidTmp;
    ret = MoveToPosition(ctx, idInfo->version, *uidTmp);
    if (ret == 0) {
      break;
    }
    metaDebug("tmqsnap get meta not exist uid:%" PRIi64 " version:%" PRIi64, *uid, idInfo->version);
  }

  ret = tdbTbcGet((TBC*)ctx->pCur, (const void**)&pKey, &kLen, (const void**)&pVal, &vLen);
  TSDB_CHECK_CONDITION(ret >= 0, ret, lino, END, TAOS_GET_TERRNO(ret));
  SMetaEntry me = {0};
  tDecoderInit(&dc, pVal, vLen);
  ret = metaDecodeEntry(&dc, &me);
  TSDB_CHECK_CONDITION(ret >= 0, ret, lino, END, TAOS_GET_TERRNO(ret));
  metaDebug("tmqsnap get meta uid:%" PRIi64 " name:%s index:%d", *uid, me.name, ctx->index - 1);

  if ((ctx->subType == TOPIC_SUB_TYPE__DB && me.type == TSDB_SUPER_TABLE) ||
      (ctx->subType == TOPIC_SUB_TYPE__TABLE && me.uid == ctx->suid)) {
    SVCreateStbReq req = {0};
    req.name = me.name;
    req.suid = me.uid;
    req.schemaRow = me.stbEntry.schemaRow;
    req.schemaTag = me.stbEntry.schemaTag;
    req.schemaRow.version = 1;
    req.schemaTag.version = 1;
    req.colCmpr = me.colCmpr;
    req.pExtSchemas = me.pExtSchemas;

    ret = buildSuperTableInfo(&req, pBuf, contLen);
    *type = TDMT_VND_CREATE_STB;
  } else if ((ctx->subType == TOPIC_SUB_TYPE__DB && me.type == TSDB_CHILD_TABLE) ||
             (ctx->subType == TOPIC_SUB_TYPE__TABLE && me.type == TSDB_CHILD_TABLE && me.ctbEntry.suid == ctx->suid)) {
    STableInfoForChildTable* data =
        (STableInfoForChildTable*)taosHashGet(ctx->suidInfo, &me.ctbEntry.suid, sizeof(tb_uid_t));
    TSDB_CHECK_NULL(data, ret, lino, END, terrno);

    SVCreateTbReq req = {0};

    req.type = TSDB_CHILD_TABLE;
    req.name = me.name;
    req.uid = me.uid;
    req.commentLen = -1;
    req.ctb.suid = me.ctbEntry.suid;
    req.ctb.tagNum = data->tagRow->nCols;
    req.ctb.stbName = data->tableName;

    tagName = taosArrayInit(req.ctb.tagNum, TSDB_COL_NAME_LEN);
    TSDB_CHECK_NULL(tagName, ret, lino, END, terrno);
    STag* p = (STag*)me.ctbEntry.pTags;
    if (tTagIsJson(p)) {
      if (p->nTag != 0) {
        SSchema* schema = &data->tagRow->pSchema[0];
        TSDB_CHECK_NULL(taosArrayPush(tagName, schema->name), ret, lino, END, terrno);
      }
    } else {
      ret = tTagToValArray((const STag*)p, &pTagVals);
      TSDB_CHECK_CODE(ret, lino, END);
      int16_t nCols = taosArrayGetSize(pTagVals);
      for (int j = 0; j < nCols; ++j) {
        STagVal* pTagVal = (STagVal*)taosArrayGet(pTagVals, j);
        for (int i = 0; pTagVal && i < data->tagRow->nCols; i++) {
          SSchema* schema = &data->tagRow->pSchema[i];
          if (schema->colId == pTagVal->cid) {
            TSDB_CHECK_NULL(taosArrayPush(tagName, schema->name), ret, lino, END, terrno);
          }
        }
      }
    }
    req.ctb.pTag = me.ctbEntry.pTags;
    req.ctb.tagName = tagName;
    ret = buildNormalChildTableInfo(&req, pBuf, contLen);
    *type = TDMT_VND_CREATE_TABLE;
  } else if (ctx->subType == TOPIC_SUB_TYPE__DB && me.type == TSDB_NORMAL_TABLE) {
    SVCreateTbReq req = {0};
    req.type = TSDB_NORMAL_TABLE;
    req.name = me.name;
    req.uid = me.uid;
    req.commentLen = -1;
    req.ntb.schemaRow = me.ntbEntry.schemaRow;
    req.colCmpr = me.colCmpr;
    req.pExtSchemas = me.pExtSchemas;
    ret = buildNormalChildTableInfo(&req, pBuf, contLen);
    *type = TDMT_VND_CREATE_TABLE;
  } else {
    metaError("meta/snap: invalid topic sub type: %" PRId8 " get meta from snap failed.", ctx->subType);
    ret = TSDB_CODE_SDB_INVALID_TABLE_TYPE;
  }

END:
  tdbTbcClose((TBC*)ctx->pCur);
  ctx->pCur = NULL;
  taosArrayDestroy(pTagVals);
  taosArrayDestroy(tagName);
  tDecoderClear(&dc);
  metaULock(ctx->pMeta);

  if(ret != 0) {
    metaError("tmqsnap get table info from snapshot failed line:%d since %s", lino, tstrerror(ret));
  }
  return ret;
}

int32_t getMetaTableInfoFromSnapshot(SSnapContext* ctx, SMetaTableInfo* result) {
  void* pKey = NULL;
  void* pVal = NULL;
  int   vLen = 0;
  int   kLen = 0;
  int32_t code = 0;
  int32_t lino = 0;
  SDecoder   dc = {0};

  metaRLock(ctx->pMeta);
  while (1) {
    if (ctx->index >= taosArrayGetSize(ctx->idList)) {
      metaDebug("tmqsnap get uid info end");
      goto END;
    }
    int64_t* uidTmp = taosArrayGet(ctx->idList, ctx->index);
    TSDB_CHECK_NULL(uidTmp, code, lino, END, terrno);
    ctx->index++;
    SIdInfo* idInfo = (SIdInfo*)taosHashGet(ctx->idVersion, uidTmp, sizeof(tb_uid_t));
    TSDB_CHECK_NULL(idInfo, code, lino, END, terrno);

    if (MoveToPosition(ctx, idInfo->version, *uidTmp) != 0) {
      metaDebug("tmqsnap getMetaTableInfoFromSnapshot not exist uid:%" PRIi64 " version:%" PRIi64, *uidTmp,
                idInfo->version);
      continue;
    }
    code = tdbTbcGet((TBC*)ctx->pCur, (const void**)&pKey, &kLen, (const void**)&pVal, &vLen);
    TSDB_CHECK_CODE(code, lino, END);
    SMetaEntry me = {0};
    tDecoderInit(&dc, pVal, vLen);
    code = metaDecodeEntry(&dc, &me);
    TSDB_CHECK_CODE(code, lino, END);
    metaDebug("tmqsnap get uid info uid:%" PRIi64 " name:%s index:%d", me.uid, me.name, ctx->index - 1);

    if ((ctx->subType == TOPIC_SUB_TYPE__DB && me.type == TSDB_CHILD_TABLE) ||
        (ctx->subType == TOPIC_SUB_TYPE__TABLE && me.type == TSDB_CHILD_TABLE && me.ctbEntry.suid == ctx->suid)) {
      STableInfoForChildTable* data =
          (STableInfoForChildTable*)taosHashGet(ctx->suidInfo, &me.ctbEntry.suid, sizeof(tb_uid_t));
      TSDB_CHECK_NULL(data, code, lino, END, terrno);
      result->suid = me.ctbEntry.suid;
      result->schema = tCloneSSchemaWrapper(data->schemaRow);
      if (data->pExtSchemas != NULL) {
        result->pExtSchemas = taosMemoryMalloc(sizeof(SExtSchema) * data->schemaRow->nCols);
        TSDB_CHECK_NULL(result->pExtSchemas, code, lino, END, terrno);
        memcpy(result->pExtSchemas, data->pExtSchemas, sizeof(SExtSchema) * data->schemaRow->nCols);
      }
    } else if (ctx->subType == TOPIC_SUB_TYPE__DB && me.type == TSDB_NORMAL_TABLE) {
      result->suid = 0;
      result->schema = tCloneSSchemaWrapper(&me.ntbEntry.schemaRow);
      if (me.pExtSchemas != NULL) {
        result->pExtSchemas = taosMemoryMalloc(sizeof(SExtSchema) * me.ntbEntry.schemaRow.nCols);
        TSDB_CHECK_NULL(result->pExtSchemas, code, lino, END, terrno);
        memcpy(result->pExtSchemas, me.pExtSchemas, sizeof(SExtSchema) * me.ntbEntry.schemaRow.nCols);
      }
    } else {
      metaDebug("tmqsnap get uid continue");
      tDecoderClear(&dc);
      continue;
    }
    result->uid = me.uid;
    tstrncpy(result->tbName, me.name, TSDB_TABLE_NAME_LEN);
    TSDB_CHECK_NULL(result->schema, code, lino, END, TAOS_GET_TERRNO(TSDB_CODE_OUT_OF_MEMORY));
    break;
  }

END:
  tDecoderClear(&dc);
  tdbTbcClose((TBC*)ctx->pCur);
  ctx->pCur = NULL;
  metaULock(ctx->pMeta);

  if (code != 0) {
    metaError("tmqsnap get meta table info from snapshot failed line:%d since %s", lino, tstrerror(code));
  }
  return code;
}
