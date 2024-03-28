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
};

int32_t metaSnapReaderOpen(SMeta* pMeta, int64_t sver, int64_t ever, SMetaSnapReader** ppReader) {
  int32_t          code = 0;
  int32_t          c = 0;
  SMetaSnapReader* pReader = NULL;

  // alloc
  pReader = (SMetaSnapReader*)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pReader->pMeta = pMeta;
  pReader->sver = sver;
  pReader->ever = ever;

  // impl
  code = tdbTbcOpen(pMeta->pTbDb, &pReader->pTbc, NULL);
  if (code) {
    taosMemoryFree(pReader);
    goto _err;
  }

  code = tdbTbcMoveTo(pReader->pTbc, &(STbDbKey){.version = sver, .uid = INT64_MIN}, sizeof(STbDbKey), &c);
  if (code) {
    taosMemoryFree(pReader);
    goto _err;
  }

  metaInfo("vgId:%d, vnode snapshot meta reader opened", TD_VID(pMeta->pVnode));

  *ppReader = pReader;
  return code;

_err:
  metaError("vgId:%d, vnode snapshot meta reader open failed since %s", TD_VID(pMeta->pVnode), tstrerror(code));
  *ppReader = NULL;
  return code;
}

int32_t metaSnapReaderClose(SMetaSnapReader** ppReader) {
  int32_t code = 0;

  tdbTbcClose((*ppReader)->pTbc);
  taosMemoryFree(*ppReader);
  *ppReader = NULL;

  return code;
}

int32_t metaSnapRead(SMetaSnapReader* pReader, uint8_t** ppData) {
  int32_t     code = 0;
  const void* pKey = NULL;
  const void* pData = NULL;
  int32_t     nKey = 0;
  int32_t     nData = 0;
  STbDbKey    key;

  *ppData = NULL;
  for (;;) {
    if (tdbTbcGet(pReader->pTbc, &pKey, &nKey, &pData, &nData)) {
      goto _exit;
    }

    key = ((STbDbKey*)pKey)[0];
    if (key.version > pReader->ever) {
      goto _exit;
    }

    if (key.version < pReader->sver) {
      tdbTbcMoveToNext(pReader->pTbc);
      continue;
    }

    if (!pData || !nData) {
      metaError("meta/snap: invalide nData: %" PRId32 " meta snap read failed.", nData);
      goto _exit;
    }

    *ppData = taosMemoryMalloc(sizeof(SSnapDataHdr) + nData);
    if (*ppData == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }

    SSnapDataHdr* pHdr = (SSnapDataHdr*)(*ppData);
    pHdr->type = SNAP_DATA_META;
    pHdr->size = nData;
    memcpy(pHdr->data, pData, nData);

    metaDebug("vgId:%d, vnode snapshot meta read data, version:%" PRId64 " uid:%" PRId64 " blockLen:%d",
              TD_VID(pReader->pMeta->pVnode), key.version, key.uid, nData);

    tdbTbcMoveToNext(pReader->pTbc);
    break;
  }

_exit:
  return code;

_err:
  metaError("vgId:%d, vnode snapshot meta read data failed since %s", TD_VID(pReader->pMeta->pVnode), tstrerror(code));
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
  SMetaSnapWriter* pWriter;

  // alloc
  pWriter = (SMetaSnapWriter*)taosMemoryCalloc(1, sizeof(*pWriter));
  if (pWriter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pWriter->pMeta = pMeta;
  pWriter->sver = sver;
  pWriter->ever = ever;

  metaBegin(pMeta, META_BEGIN_HEAP_NIL);

  *ppWriter = pWriter;
  return code;

_err:
  metaError("vgId:%d, meta snapshot writer open failed since %s", TD_VID(pMeta->pVnode), tstrerror(code));
  *ppWriter = NULL;
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
  int32_t    line = 0;
  SMeta*     pMeta = pWriter->pMeta;
  SMetaEntry metaEntry = {0};
  SDecoder*  pDecoder = &(SDecoder){0};

  tDecoderInit(pDecoder, pData + sizeof(SSnapDataHdr), nData - sizeof(SSnapDataHdr));
  code = metaDecodeEntry(pDecoder, &metaEntry);
  VND_CHECK_CODE(code, line, _err);

  code = metaHandleEntry(pMeta, &metaEntry);
  VND_CHECK_CODE(code, line, _err);

  tDecoderClear(pDecoder);
  return code;

_err:
  tDecoderClear(pDecoder);
  metaError("vgId:%d, vnode snapshot meta write failed since %s at line:%d", TD_VID(pMeta->pVnode), terrstr(), line);
  return code;
}

typedef struct STableInfoForChildTable {
  char*           tableName;
  SSchemaWrapper* schemaRow;
  SSchemaWrapper* tagRow;
} STableInfoForChildTable;

static void destroySTableInfoForChildTable(void* data) {
  STableInfoForChildTable* pData = (STableInfoForChildTable*)data;
  taosMemoryFree(pData->tableName);
  tDeleteSchemaWrapper(pData->schemaRow);
  tDeleteSchemaWrapper(pData->tagRow);
}

static void MoveToSnapShotVersion(SSnapContext* ctx) {
  tdbTbcClose((TBC*)ctx->pCur);
  tdbTbcOpen(ctx->pMeta->pTbDb, (TBC**)&ctx->pCur, NULL);
  STbDbKey key = {.version = ctx->snapVersion, .uid = INT64_MAX};
  int      c = 0;
  tdbTbcMoveTo((TBC*)ctx->pCur, &key, sizeof(key), &c);
  if (c < 0) {
    tdbTbcMoveToPrev((TBC*)ctx->pCur);
  }
}

static int32_t MoveToPosition(SSnapContext* ctx, int64_t ver, int64_t uid) {
  tdbTbcClose((TBC*)ctx->pCur);
  tdbTbcOpen(ctx->pMeta->pTbDb, (TBC**)&ctx->pCur, NULL);
  STbDbKey key = {.version = ver, .uid = uid};
  int      c = 0;
  tdbTbcMoveTo((TBC*)ctx->pCur, &key, sizeof(key), &c);
  return c;
}

static void MoveToFirst(SSnapContext* ctx) {
  tdbTbcClose((TBC*)ctx->pCur);
  tdbTbcOpen(ctx->pMeta->pTbDb, (TBC**)&ctx->pCur, NULL);
  tdbTbcMoveToFirst((TBC*)ctx->pCur);
}

static void saveSuperTableInfoForChildTable(SMetaEntry* me, SHashObj* suidInfo) {
  STableInfoForChildTable* data = (STableInfoForChildTable*)taosHashGet(suidInfo, &me->uid, sizeof(tb_uid_t));
  if (data) {
    return;
  }
  STableInfoForChildTable dataTmp = {0};
  dataTmp.tableName = taosStrdup(me->name);

  dataTmp.schemaRow = tCloneSSchemaWrapper(&me->stbEntry.schemaRow);
  dataTmp.tagRow = tCloneSSchemaWrapper(&me->stbEntry.schemaTag);
  taosHashPut(suidInfo, &me->uid, sizeof(tb_uid_t), &dataTmp, sizeof(STableInfoForChildTable));
}

int32_t buildSnapContext(SVnode* pVnode, int64_t snapVersion, int64_t suid, int8_t subType, int8_t withMeta,
                         SSnapContext** ctxRet) {
  SSnapContext* ctx = taosMemoryCalloc(1, sizeof(SSnapContext));
  if (ctx == NULL) return -1;
  *ctxRet = ctx;
  ctx->pMeta = pVnode->pMeta;
  ctx->snapVersion = snapVersion;
  ctx->suid = suid;
  ctx->subType = subType;
  ctx->queryMeta = withMeta;
  ctx->withMeta = withMeta;
  ctx->idVersion = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  if (ctx->idVersion == NULL) {
    return -1;
  }

  ctx->suidInfo = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  if (ctx->suidInfo == NULL) {
    return -1;
  }
  taosHashSetFreeFp(ctx->suidInfo, destroySTableInfoForChildTable);

  ctx->index = 0;
  ctx->idList = taosArrayInit(100, sizeof(int64_t));
  void* pKey = NULL;
  void* pVal = NULL;
  int   vLen = 0, kLen = 0;

  metaDebug("tmqsnap init snapVersion:%" PRIi64, ctx->snapVersion);
  MoveToFirst(ctx);
  while (1) {
    int32_t ret = tdbTbcNext((TBC*)ctx->pCur, &pKey, &kLen, &pVal, &vLen);
    if (ret < 0) break;
    STbDbKey* tmp = (STbDbKey*)pKey;
    if (tmp->version > ctx->snapVersion) break;

    SIdInfo* idData = (SIdInfo*)taosHashGet(ctx->idVersion, &tmp->uid, sizeof(tb_uid_t));
    if (idData) {
      continue;
    }

    if (tdbTbGet(ctx->pMeta->pUidIdx, &tmp->uid, sizeof(tb_uid_t), NULL, NULL) <
        0) {  // check if table exist for now, need optimize later
      continue;
    }

    SDecoder   dc = {0};
    SMetaEntry me = {0};
    tDecoderInit(&dc, pVal, vLen);
    metaDecodeEntry(&dc, &me);
    if (ctx->subType == TOPIC_SUB_TYPE__TABLE) {
      if ((me.uid != ctx->suid && me.type == TSDB_SUPER_TABLE) ||
          (me.ctbEntry.suid != ctx->suid && me.type == TSDB_CHILD_TABLE)) {
        tDecoderClear(&dc);
        continue;
      }
    }

    taosArrayPush(ctx->idList, &tmp->uid);
    metaDebug("tmqsnap init idlist name:%s, uid:%" PRIi64, me.name, tmp->uid);
    SIdInfo info = {0};
    taosHashPut(ctx->idVersion, &tmp->uid, sizeof(tb_uid_t), &info, sizeof(SIdInfo));

    tDecoderClear(&dc);
  }
  taosHashClear(ctx->idVersion);

  MoveToSnapShotVersion(ctx);
  while (1) {
    int32_t ret = tdbTbcPrev((TBC*)ctx->pCur, &pKey, &kLen, &pVal, &vLen);
    if (ret < 0) break;

    STbDbKey* tmp = (STbDbKey*)pKey;
    SIdInfo*  idData = (SIdInfo*)taosHashGet(ctx->idVersion, &tmp->uid, sizeof(tb_uid_t));
    if (idData) {
      continue;
    }
    SIdInfo info = {.version = tmp->version, .index = 0};
    taosHashPut(ctx->idVersion, &tmp->uid, sizeof(tb_uid_t), &info, sizeof(SIdInfo));

    SDecoder   dc = {0};
    SMetaEntry me = {0};
    tDecoderInit(&dc, pVal, vLen);
    metaDecodeEntry(&dc, &me);
    if (ctx->subType == TOPIC_SUB_TYPE__TABLE) {
      if ((me.uid != ctx->suid && me.type == TSDB_SUPER_TABLE) ||
          (me.ctbEntry.suid != ctx->suid && me.type == TSDB_CHILD_TABLE)) {
        tDecoderClear(&dc);
        continue;
      }
    }

    if ((ctx->subType == TOPIC_SUB_TYPE__DB && me.type == TSDB_SUPER_TABLE) ||
        (ctx->subType == TOPIC_SUB_TYPE__TABLE && me.uid == ctx->suid)) {
      saveSuperTableInfoForChildTable(&me, ctx->suidInfo);
    }
    tDecoderClear(&dc);
  }

  for (int i = 0; i < taosArrayGetSize(ctx->idList); i++) {
    int64_t* uid = taosArrayGet(ctx->idList, i);
    SIdInfo* idData = (SIdInfo*)taosHashGet(ctx->idVersion, uid, sizeof(int64_t));
    if (!idData) {
      metaError("meta/snap: null idData");
      return TSDB_CODE_FAILED;
    }

    idData->index = i;
    metaDebug("tmqsnap init idVersion uid:%" PRIi64 " version:%" PRIi64 " index:%d", *uid, idData->version,
              idData->index);
  }

  tdbFree(pKey);
  tdbFree(pVal);
  return TDB_CODE_SUCCESS;
}

int32_t destroySnapContext(SSnapContext* ctx) {
  tdbTbcClose((TBC*)ctx->pCur);
  taosArrayDestroy(ctx->idList);
  taosHashCleanup(ctx->idVersion);
  taosHashCleanup(ctx->suidInfo);
  taosMemoryFree(ctx);
  return 0;
}

static int32_t buildNormalChildTableInfo(SVCreateTbReq* req, void** pBuf, int32_t* contLen) {
  int32_t            ret = 0;
  SVCreateTbBatchReq reqs = {0};

  reqs.pArray = taosArrayInit(1, sizeof(struct SVCreateTbReq));
  if (NULL == reqs.pArray) {
    ret = -1;
    goto end;
  }
  taosArrayPush(reqs.pArray, req);
  reqs.nReqs = 1;

  tEncodeSize(tEncodeSVCreateTbBatchReq, &reqs, *contLen, ret);
  if (ret < 0) {
    ret = -1;
    goto end;
  }
  *contLen += sizeof(SMsgHead);
  *pBuf = taosMemoryMalloc(*contLen);
  if (NULL == *pBuf) {
    ret = -1;
    goto end;
  }
  SEncoder coder = {0};
  tEncoderInit(&coder, POINTER_SHIFT(*pBuf, sizeof(SMsgHead)), *contLen);
  if (tEncodeSVCreateTbBatchReq(&coder, &reqs) < 0) {
    taosMemoryFreeClear(*pBuf);
    tEncoderClear(&coder);
    ret = -1;
    goto end;
  }
  tEncoderClear(&coder);

end:
  taosArrayDestroy(reqs.pArray);
  return ret;
}

static int32_t buildSuperTableInfo(SVCreateStbReq* req, void** pBuf, int32_t* contLen) {
  int32_t ret = 0;
  tEncodeSize(tEncodeSVCreateStbReq, req, *contLen, ret);
  if (ret < 0) {
    return -1;
  }

  *contLen += sizeof(SMsgHead);
  *pBuf = taosMemoryMalloc(*contLen);
  if (NULL == *pBuf) {
    return -1;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, POINTER_SHIFT(*pBuf, sizeof(SMsgHead)), *contLen);
  if (tEncodeSVCreateStbReq(&encoder, req) < 0) {
    taosMemoryFreeClear(*pBuf);
    tEncoderClear(&encoder);
    return -1;
  }
  tEncoderClear(&encoder);
  return 0;
}

int32_t setForSnapShot(SSnapContext* ctx, int64_t uid) {
  int c = 0;

  if (uid == 0) {
    ctx->index = 0;
    return c;
  }

  SIdInfo* idInfo = (SIdInfo*)taosHashGet(ctx->idVersion, &uid, sizeof(tb_uid_t));
  if (!idInfo) {
    return -1;
  }

  ctx->index = idInfo->index;

  return c;
}

int32_t getTableInfoFromSnapshot(SSnapContext* ctx, void** pBuf, int32_t* contLen, int16_t* type, int64_t* uid) {
  int32_t ret = 0;
  void*   pKey = NULL;
  void*   pVal = NULL;
  int     vLen = 0, kLen = 0;

  while (1) {
    if (ctx->index >= taosArrayGetSize(ctx->idList)) {
      metaDebug("tmqsnap get meta end");
      ctx->index = 0;
      ctx->queryMeta = 0;  // change to get data
      return 0;
    }

    int64_t* uidTmp = taosArrayGet(ctx->idList, ctx->index);
    ctx->index++;
    SIdInfo* idInfo = (SIdInfo*)taosHashGet(ctx->idVersion, uidTmp, sizeof(tb_uid_t));
    if (!idInfo) {
      metaError("meta/snap: null idInfo");
      return TSDB_CODE_FAILED;
    }

    *uid = *uidTmp;
    ret = MoveToPosition(ctx, idInfo->version, *uidTmp);
    if (ret == 0) {
      break;
    }
    metaDebug("tmqsnap get meta not exist uid:%" PRIi64 " version:%" PRIi64, *uid, idInfo->version);
  }

  tdbTbcGet((TBC*)ctx->pCur, (const void**)&pKey, &kLen, (const void**)&pVal, &vLen);
  SDecoder   dc = {0};
  SMetaEntry me = {0};
  tDecoderInit(&dc, pVal, vLen);
  metaDecodeEntry(&dc, &me);
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

    ret = buildSuperTableInfo(&req, pBuf, contLen);
    *type = TDMT_VND_CREATE_STB;

  } else if ((ctx->subType == TOPIC_SUB_TYPE__DB && me.type == TSDB_CHILD_TABLE) ||
             (ctx->subType == TOPIC_SUB_TYPE__TABLE && me.type == TSDB_CHILD_TABLE && me.ctbEntry.suid == ctx->suid)) {
    STableInfoForChildTable* data =
        (STableInfoForChildTable*)taosHashGet(ctx->suidInfo, &me.ctbEntry.suid, sizeof(tb_uid_t));
    if (!data) {
      metaError("meta/snap: null data");
      return TSDB_CODE_FAILED;
    }

    SVCreateTbReq req = {0};

    req.type = TSDB_CHILD_TABLE;
    req.name = me.name;
    req.uid = me.uid;
    req.commentLen = -1;
    req.ctb.suid = me.ctbEntry.suid;
    req.ctb.tagNum = data->tagRow->nCols;
    req.ctb.stbName = data->tableName;

    SArray* tagName = taosArrayInit(req.ctb.tagNum, TSDB_COL_NAME_LEN);
    STag*   p = (STag*)me.ctbEntry.pTags;
    if (tTagIsJson(p)) {
      if (p->nTag != 0) {
        SSchema* schema = &data->tagRow->pSchema[0];
        taosArrayPush(tagName, schema->name);
      }
    } else {
      SArray* pTagVals = NULL;
      if (tTagToValArray((const STag*)p, &pTagVals) != 0) {
        metaError("meta/snap: tag to val array failed.");
        return TSDB_CODE_FAILED;
      }
      int16_t nCols = taosArrayGetSize(pTagVals);
      for (int j = 0; j < nCols; ++j) {
        STagVal* pTagVal = (STagVal*)taosArrayGet(pTagVals, j);
        for (int i = 0; i < data->tagRow->nCols; i++) {
          SSchema* schema = &data->tagRow->pSchema[i];
          if (schema->colId == pTagVal->cid) {
            taosArrayPush(tagName, schema->name);
          }
        }
      }
      taosArrayDestroy(pTagVals);
    }
    //    SIdInfo* sidInfo = (SIdInfo*)taosHashGet(ctx->idVersion, &me.ctbEntry.suid, sizeof(tb_uid_t));
    //    if(sidInfo->version >= idInfo->version){
    //      // need parse tag
    //      STag* p = (STag*)me.ctbEntry.pTags;
    //      SArray* pTagVals = NULL;
    //      if (tTagToValArray((const STag*)p, &pTagVals) != 0) {
    //      }
    //
    //      int16_t nCols = taosArrayGetSize(pTagVals);
    //      for (int j = 0; j < nCols; ++j) {
    //        STagVal* pTagVal = (STagVal*)taosArrayGet(pTagVals, j);
    //      }
    //    }else{
    req.ctb.pTag = me.ctbEntry.pTags;
    //    }

    req.ctb.tagName = tagName;
    ret = buildNormalChildTableInfo(&req, pBuf, contLen);
    *type = TDMT_VND_CREATE_TABLE;
    taosArrayDestroy(tagName);
  } else if (ctx->subType == TOPIC_SUB_TYPE__DB) {
    SVCreateTbReq req = {0};
    req.type = TSDB_NORMAL_TABLE;
    req.name = me.name;
    req.uid = me.uid;
    req.commentLen = -1;
    req.ntb.schemaRow = me.ntbEntry.schemaRow;
    ret = buildNormalChildTableInfo(&req, pBuf, contLen);
    *type = TDMT_VND_CREATE_TABLE;
  } else {
    metaError("meta/snap: invalid topic sub type: %" PRId8 " get meta from snap failed.", ctx->subType);
    ret = -1;
  }
  tDecoderClear(&dc);

  return ret;
}

SMetaTableInfo getMetaTableInfoFromSnapshot(SSnapContext* ctx) {
  SMetaTableInfo result = {0};
  void*          pKey = NULL;
  void*          pVal = NULL;
  int            vLen, kLen;

  while (1) {
    if (ctx->index >= taosArrayGetSize(ctx->idList)) {
      metaDebug("tmqsnap get uid info end");
      return result;
    }
    int64_t* uidTmp = taosArrayGet(ctx->idList, ctx->index);
    ctx->index++;
    SIdInfo* idInfo = (SIdInfo*)taosHashGet(ctx->idVersion, uidTmp, sizeof(tb_uid_t));
    if (!idInfo) {
      metaError("meta/snap: null idInfo");
      return result;
    }

    int32_t ret = MoveToPosition(ctx, idInfo->version, *uidTmp);
    if (ret != 0) {
      metaDebug("tmqsnap getMetaTableInfoFromSnapshot not exist uid:%" PRIi64 " version:%" PRIi64, *uidTmp,
                idInfo->version);
      continue;
    }
    tdbTbcGet((TBC*)ctx->pCur, (const void**)&pKey, &kLen, (const void**)&pVal, &vLen);
    SDecoder   dc = {0};
    SMetaEntry me = {0};
    tDecoderInit(&dc, pVal, vLen);
    metaDecodeEntry(&dc, &me);
    metaDebug("tmqsnap get uid info uid:%" PRIi64 " name:%s index:%d", me.uid, me.name, ctx->index - 1);

    if (ctx->subType == TOPIC_SUB_TYPE__DB && me.type == TSDB_CHILD_TABLE) {
      STableInfoForChildTable* data =
          (STableInfoForChildTable*)taosHashGet(ctx->suidInfo, &me.ctbEntry.suid, sizeof(tb_uid_t));
      result.uid = me.uid;
      result.suid = me.ctbEntry.suid;
      result.schema = tCloneSSchemaWrapper(data->schemaRow);
      strcpy(result.tbName, me.name);
      tDecoderClear(&dc);
      break;
    } else if (ctx->subType == TOPIC_SUB_TYPE__DB && me.type == TSDB_NORMAL_TABLE) {
      result.uid = me.uid;
      result.suid = 0;
      strcpy(result.tbName, me.name);
      result.schema = tCloneSSchemaWrapper(&me.ntbEntry.schemaRow);
      tDecoderClear(&dc);
      break;
    } else if (ctx->subType == TOPIC_SUB_TYPE__TABLE && me.type == TSDB_CHILD_TABLE && me.ctbEntry.suid == ctx->suid) {
      STableInfoForChildTable* data =
          (STableInfoForChildTable*)taosHashGet(ctx->suidInfo, &me.ctbEntry.suid, sizeof(tb_uid_t));
      result.uid = me.uid;
      result.suid = me.ctbEntry.suid;
      strcpy(result.tbName, me.name);
      result.schema = tCloneSSchemaWrapper(data->schemaRow);
      tDecoderClear(&dc);
      break;
    } else {
      metaDebug("tmqsnap get uid continue");
      tDecoderClear(&dc);
      continue;
    }
  }

  return result;
}
