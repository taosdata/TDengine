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

    tdbTbcMoveToNext(pReader->pTbc);
    break;
  }

  ASSERT(pData && nData);

  *ppData = taosMemoryMalloc(sizeof(SSnapDataHdr) + nData);
  if (*ppData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  SSnapDataHdr* pHdr = (SSnapDataHdr*)(*ppData);
  pHdr->type = SNAP_DATA_META;
  pHdr->size = nData;
  memcpy(pHdr->data, pData, nData);

  metaInfo("vgId:%d, vnode snapshot meta read data, version:%" PRId64 " uid:%" PRId64 " nData:%d",
           TD_VID(pReader->pMeta->pVnode), key.version, key.uid, nData);

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

  metaBegin(pMeta);

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
    ASSERT(0);
  } else {
    code = metaCommit(pWriter->pMeta);
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
  SMeta*     pMeta = pWriter->pMeta;
  SMetaEntry metaEntry = {0};
  SDecoder*  pDecoder = &(SDecoder){0};

  tDecoderInit(pDecoder, pData + sizeof(SSnapDataHdr), nData - sizeof(SSnapDataHdr));
  metaDecodeEntry(pDecoder, &metaEntry);

  code = metaHandleEntry(pMeta, &metaEntry);
  if (code) goto _err;

  tDecoderClear(pDecoder);
  return code;

_err:
  metaError("vgId:%d, vnode snapshot meta write failed since %s", TD_VID(pMeta->pVnode), tstrerror(code));
  return code;
}

typedef struct STableInfoForChildTable{
  char            *tableName;
  SArray          *tagName;
  SSchemaWrapper  *schemaRow;
}STableInfoForChildTable;

static void destroySTableInfoForChildTable(void* data) {
  STableInfoForChildTable* pData = (STableInfoForChildTable*)data;
  taosMemoryFree(pData->tagName);
  taosArrayDestroy(pData->tagName);
  tDeleteSSchemaWrapper(pData->schemaRow);
}

int32_t buildSnapContext(SMeta* pMeta, int64_t snapVersion, int64_t suid, int8_t subType, bool withMeta, SSnapContext* ctx){
  ctx->pMeta = pMeta;
  ctx->snapVersion = snapVersion;
  ctx->suid = suid;
  ctx->subType = subType;
  ctx->queryMetaOrData = withMeta;
  int32_t ret = tdbTbcOpen(pMeta->pTbDb, &ctx->pCur, NULL);
  if (ret < 0) {
    return -1;
  }
  ctx->idVersion = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  if(ctx->idVersion == NULL){
    return -1;
  }

  ctx->suidInfo = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  if(ctx->suidInfo == NULL){
    return -1;
  }
  taosHashSetFreeFp(ctx->suidInfo, destroySTableInfoForChildTable);

  void *pKey = NULL;
  void *pVal = NULL;
  int   vLen, kLen;

  tdbTbcMoveToFirst(ctx->pCur);
  while(1){
    ret = tdbTbcNext(ctx->pCur, &pKey, &kLen, &pVal, &vLen);
    if (ret < 0) break;

    STbDbKey *tmp = (STbDbKey*)pKey;
    if(tmp->version > ctx->snapVersion) break;
    taosHashPut(ctx->idVersion, &tmp->uid, sizeof(tb_uid_t), &tmp->version, sizeof(int64_t));
  }
  tdbTbcMoveToFirst(ctx->pCur);
  return TDB_CODE_SUCCESS;
}

int32_t destroySnapContext(SSnapContext* ctx){
  tdbTbcClose(ctx->pCur);
  taosHashCleanup(ctx->idVersion);
  taosHashCleanup(ctx->suidInfo);

  return 0;
}

static int32_t buildNormalChildTableInfo(SVCreateTbReq *req, void **pBuf, int32_t *contLen){
  int32_t ret = 0;
  SVCreateTbBatchReq reqs = {};

  reqs.pArray = taosArrayInit(1, sizeof(struct SVCreateTbReq));
  if (NULL == reqs.pArray){
    ret = -1;
    goto end;
  }
  taosArrayPush(reqs.pArray, &req);
  reqs.nReqs = 1;

  tEncodeSize(tEncodeSVCreateTbBatchReq, &reqs, *contLen, ret);
  if(ret < 0){
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
  tEncoderInit(&coder, *pBuf + sizeof(SMsgHead), *contLen);
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

static int32_t buildSuperTableInfo(SVCreateStbReq *req, void **pBuf, int32_t *contLen){
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

  SEncoder       encoder = {0};
  tEncoderInit(&encoder, *pBuf + sizeof(SMsgHead), *contLen);
  if (tEncodeSVCreateStbReq(&encoder, req) < 0) {
    taosMemoryFreeClear(*pBuf);
    tEncoderClear(&encoder);
    return -1;
  }
  tEncoderClear(&encoder);
  return 0;
}

static void saveSuperTableInfoForChildTable(SMetaEntry *me, SHashObj *suidInfo){
  STableInfoForChildTable dataTmp = {0};
  dataTmp.tableName = strdup(me->name);
  dataTmp.tagName = taosArrayInit(me->stbEntry.schemaTag.nCols, TSDB_COL_NAME_LEN);
  for(int i = 0; i < me->stbEntry.schemaTag.nCols; i++){
    SSchema *schema = &me->stbEntry.schemaTag.pSchema[i];
    taosArrayPush(dataTmp.tagName, schema->name);
  }
  dataTmp.schemaRow = tCloneSSchemaWrapper(&me->stbEntry.schemaRow);

  STableInfoForChildTable* data = (STableInfoForChildTable*)taosHashGet(suidInfo, &me->uid, sizeof(tb_uid_t));
  if(data){
    destroySTableInfoForChildTable(data);
  }
  taosHashPut(suidInfo, &me->uid, sizeof(tb_uid_t), &dataTmp, sizeof(STableInfoForChildTable));
}

int32_t setMetaForSnapShot(SSnapContext* ctx, int64_t uid, int64_t ver){
  int c = 0;
  ctx->queryMetaOrData = true;  // change to get data
  if(uid == 0 && ver == 0){
    tdbTbcMoveToFirst(ctx->pCur);
    return c;
  }
  STbDbKey key = {.version = ver, .uid = uid};
  tdbTbcMoveTo(ctx->pCur, &key, sizeof(key), &c);

  return c;
}

int32_t setDataForSnapShot(SSnapContext* ctx, int64_t uid){
  int c = 0;
  ctx->queryMetaOrData = false;  // change to get data

  if(uid == 0){
    tdbTbcMoveToFirst(ctx->pCur);
    return c;
  }

  int64_t* ver = (int64_t*)taosHashGet(ctx->idVersion, &uid, sizeof(tb_uid_t));
  if(!ver){
    return -1;
  }

  STbDbKey key = {.version = *ver, .uid = uid};
  tdbTbcMoveTo(ctx->pCur, &key, sizeof(key), &c);

  return c;
}

int32_t getMetafromSnapShot(SSnapContext* ctx, void **pBuf, int32_t *contLen, int16_t *type){
  int32_t ret = 0;
  void *pKey = NULL;
  void *pVal = NULL;
  int   vLen, kLen;

  while(1){
    ret = tdbTbcNext(ctx->pCur, &pKey, &kLen, &pVal, &vLen);
    if (ret < 0) {
      ctx->queryMetaOrData = false; // change to get data
      tdbTbcMoveToFirst(ctx->pCur);
      return 0;
    }

    STbDbKey *tmp = (STbDbKey*)pKey;
    if(tmp->version > ctx->snapVersion) {
      tdbTbcMoveToFirst(ctx->pCur);
      ctx->queryMetaOrData = false; // change to get data
      return 0;
    }
    int64_t* ver = (int64_t*)taosHashGet(ctx->idVersion, &tmp->uid, sizeof(tb_uid_t));
    ASSERT(ver);
    if(*ver > tmp->version){
      continue;
    }
    ASSERT(*ver == tmp->version);

    SDecoder   dc = {0};
    SMetaEntry me = {0};
    tDecoderInit(&dc, pVal, vLen);
    metaDecodeEntry(&dc, &me);

    if ((ctx->subType == TOPIC_SUB_TYPE__DB && me.type == TSDB_SUPER_TABLE)
        || (ctx->subType == TOPIC_SUB_TYPE__TABLE && me.uid == ctx->suid)) {
      saveSuperTableInfoForChildTable(&me, ctx->suidInfo);

      SVCreateStbReq req = {0};
      req.name = me.name;
      req.suid = me.uid;
      req.schemaRow = me.stbEntry.schemaRow;
      req.schemaTag = me.stbEntry.schemaTag;

      ret = buildSuperTableInfo(&req, pBuf, contLen);
      tDecoderClear(&dc);
      *type = TDMT_VND_CREATE_STB;
      break;
    } else if ((ctx->subType == TOPIC_SUB_TYPE__DB && me.type == TSDB_CHILD_TABLE)
               || (ctx->subType == TOPIC_SUB_TYPE__TABLE && me.type == TSDB_CHILD_TABLE && me.ctbEntry.suid == ctx->suid)) {

      STableInfoForChildTable* data = (STableInfoForChildTable*)taosHashGet(ctx->suidInfo, &me.ctbEntry.suid, sizeof(tb_uid_t));
      ASSERT(data);
      SVCreateTbReq req = {0};

      req.type = TD_CHILD_TABLE;
      req.name = me.name;
      req.uid = me.uid;
      req.commentLen = -1;
      req.ctb.suid = me.ctbEntry.suid;
      req.ctb.tagNum = taosArrayGetSize(data->tagName);
      req.ctb.name = data->tableName;
      req.ctb.pTag = me.ctbEntry.pTags;
      req.ctb.tagName = data->tagName;
      ret = buildNormalChildTableInfo(&req, pBuf, contLen);
      tDecoderClear(&dc);
      *type = TDMT_VND_CREATE_TABLE;
      break;
    } else if(ctx->subType == TOPIC_SUB_TYPE__DB){
      SVCreateTbReq req = {0};
      req.type = TD_NORMAL_TABLE;
      req.name = me.name;
      req.uid = me.uid;
      req.commentLen = -1;
      req.ntb.schemaRow = me.ntbEntry.schemaRow;
      ret = buildNormalChildTableInfo(&req, pBuf, contLen);
      tDecoderClear(&dc);
      *type = TDMT_VND_CREATE_TABLE;
      break;
    } else{
      tDecoderClear(&dc);
      continue;
    }
  }

  return ret;
}

SMetaTableInfo getUidfromSnapShot(SSnapContext* ctx){
  SMetaTableInfo result = {0};
  int32_t ret = 0;
  void *pKey = NULL;
  void *pVal = NULL;
  int   vLen, kLen;

  while(1){
    ret = tdbTbcNext(ctx->pCur, &pKey, &kLen, &pVal, &vLen);
    if (ret < 0) {
      return result;
    }

    STbDbKey *tmp = (STbDbKey*)pKey;
    if(tmp->version > ctx->snapVersion) {
      return result;
    }
    int64_t* ver = (int64_t*)taosHashGet(ctx->idVersion, &tmp->uid, sizeof(tb_uid_t));
    ASSERT(ver);
    if(*ver > tmp->version){
      continue;
    }
    ASSERT(*ver == tmp->version);

    SDecoder   dc = {0};
    SMetaEntry me = {0};
    tDecoderInit(&dc, pVal, vLen);
    metaDecodeEntry(&dc, &me);

    if (ctx->subType == TOPIC_SUB_TYPE__DB && me.type == TSDB_CHILD_TABLE){
      STableInfoForChildTable* data = (STableInfoForChildTable*)taosHashGet(ctx->suidInfo, &me.ctbEntry.suid, sizeof(tb_uid_t));
      result.uid = me.uid;
      result.suid = me.ctbEntry.suid;
      result.schema = data->schemaRow;
      tDecoderClear(&dc);
      break;
    } else if (ctx->subType == TOPIC_SUB_TYPE__DB && me.type == TSDB_NORMAL_TABLE) {
      result.uid = me.uid;
      result.suid = 0;
      result.schema = &me.ntbEntry.schemaRow;
      tDecoderClear(&dc);
      break;
    } else if(ctx->subType == TOPIC_SUB_TYPE__TABLE && me.type == TSDB_CHILD_TABLE && me.ctbEntry.suid == ctx->suid) {
      STableInfoForChildTable* data = (STableInfoForChildTable*)taosHashGet(ctx->suidInfo, &me.ctbEntry.suid, sizeof(tb_uid_t));
      result.uid = me.uid;
      result.suid = me.ctbEntry.suid;
      result.schema = data->schemaRow;
      tDecoderClear(&dc);
      break;
    } else{
      tDecoderClear(&dc);
      continue;
    }
  }

  return result;
}
