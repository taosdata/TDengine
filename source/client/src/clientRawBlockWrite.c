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

#include <string.h>
#include "cJSON.h"
#include "clientInt.h"
#include "nodes.h"
#include "osMemPool.h"
#include "osMemory.h"
#include "parser.h"
#include "taosdef.h"
#include "tarray.h"
#include "tbase64.h"
#include "tcol.h"
#include "tcompression.h"
#include "tdatablock.h"
#include "tdataformat.h"
#include "tdef.h"
#include "tglobal.h"
#include "tmsgtype.h"

#define RAW_LOG_END                                                           \
  if (code != 0) {                                                            \
    uError("%s failed at line:%d since:%s", __func__, lino, tstrerror(code)); \
  } else {                                                                    \
    uDebug("%s return success", __func__);                                    \
  }

#define RAW_LOG_START uDebug("%s start", __func__);

#define RAW_NULL_CHECK(c) \
  do {                    \
    if (c == NULL) {      \
      lino = __LINE__;    \
      code = terrno;      \
      goto end;           \
    }                     \
  } while (0)

#define RAW_FALSE_CHECK(c)           \
  do {                               \
    if (!(c)) {                      \
      code = TSDB_CODE_INVALID_PARA; \
      lino = __LINE__;               \
      goto end;                      \
    }                                \
  } while (0)

#define RAW_RETURN_CHECK(c) \
  do {                      \
    code = c;               \
    if (code != 0) {        \
      lino = __LINE__;      \
      goto end;             \
    }                       \
  } while (0)

#define LOG_ID_TAG   "connId:0x%" PRIx64 ", QID:0x%" PRIx64
#define LOG_ID_VALUE *(int64_t*)taos, pRequest->requestId

#define TMQ_META_VERSION "1.0"

static cJSON* tmqAddObjectToArray(cJSON* array) {
  cJSON* item = cJSON_CreateObject();
  if (cJSON_AddItemToArray(array, item)) {
    return item;
  }
  cJSON_Delete(item);
  return NULL;
}

static cJSON* tmqAddStringToArray(cJSON* array, const char* str) {
  cJSON* item = cJSON_CreateString(str);
  if (cJSON_AddItemToArray(array, item)) {
    return item;
  }
  cJSON_Delete(item);
  return NULL;
}
 
#define ADD_TO_JSON_STRING(JSON,NAME,VALUE) \
  RAW_NULL_CHECK(cJSON_AddStringToObject(JSON, NAME, VALUE));

#define ADD_TO_JSON_BOOL(JSON,NAME,VALUE) \
  RAW_NULL_CHECK(cJSON_AddBoolToObject(JSON, NAME, VALUE));

#define ADD_TO_JSON_NUMBER(JSON,NAME,VALUE) \
  RAW_NULL_CHECK(cJSON_AddNumberToObject(JSON, NAME, VALUE));

static int32_t  tmqWriteBatchMetaDataImpl(TAOS* taos, void* meta, uint32_t metaLen);
static tb_uid_t processSuid(tb_uid_t suid, char* db) {
  if (db == NULL) {
    return suid;
  }
  return suid + MurmurHash3_32(db, strlen(db));
}

static int32_t getLength(int8_t type, int32_t bytes, int32_t typeMod) {
  int32_t length = 0;
  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_VARBINARY || type == TSDB_DATA_TYPE_GEOMETRY) {
    length = bytes - VARSTR_HEADER_SIZE;
  } else if (type == TSDB_DATA_TYPE_NCHAR) {
    length = (bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
  } else if (IS_STR_DATA_BLOB(type)) {
    length = bytes - BLOBSTR_HEADER_SIZE;
  } else if (type == TSDB_DATA_TYPE_DECIMAL || type == TSDB_DATA_TYPE_DECIMAL64) {
    length = typeMod;
  }
  return length;
}

static int32_t buildCreateTableJson(SSchemaWrapper* schemaRow, SSchemaWrapper* schemaTag, SExtSchema* pExtSchemas, char* name, int64_t id, int8_t t,
                                 bool isVirtual, SColRefWrapper* colRef, SColCmprWrapper* pColCmprRow, cJSON** pJson) {
  if (schemaRow == NULL || name == NULL || pColCmprRow == NULL || pJson == NULL) {
    uError("invalid parameter, schemaRow:%p, name:%p, pColCmprRow:%p, pJson:%p", schemaRow, name, pColCmprRow, pJson);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int8_t  buildDefaultCompress = 0;
  if (pColCmprRow->nCols <= 0) {
    buildDefaultCompress = 1;
  }
  RAW_LOG_START
  char*  string = NULL;
  cJSON* json = cJSON_CreateObject();
  RAW_NULL_CHECK(json);

  ADD_TO_JSON_STRING(json, "type", "create");
  ADD_TO_JSON_STRING(json, "tableType", (t == TSDB_SUPER_TABLE ? "super" : "normal"));
  if (isVirtual){
    ADD_TO_JSON_BOOL(json, "isVirtual", isVirtual);
  }
  ADD_TO_JSON_STRING(json, "tableName", name);

  cJSON* columns = cJSON_AddArrayToObject(json, "columns");
  RAW_NULL_CHECK(columns);

  for (int i = 0; i < schemaRow->nCols; i++) {
    cJSON* column = tmqAddObjectToArray(columns);
    RAW_NULL_CHECK(column);
    SSchema* s = schemaRow->pSchema + i;
    ADD_TO_JSON_STRING(column, "name", s->name);
    ADD_TO_JSON_NUMBER(column, "type", s->type);
    int32_t typeMod = 0;
    if (pExtSchemas != NULL) {
      typeMod = pExtSchemas[i].typeMod;
    }
    int32_t length = getLength(s->type, s->bytes, typeMod);
    if (length > 0) {
      ADD_TO_JSON_NUMBER(column, "length", length);
    }

    if (isVirtual && colRef != NULL && i < colRef->nCols){
      SColRef* pColRef = colRef->pColRef + i;
      if (pColRef->hasRef) {
        cJSON* ref = cJSON_AddObjectToObject(column, "ref");
        RAW_NULL_CHECK(ref);
        ADD_TO_JSON_STRING(ref, "refDbName", pColRef->refDbName);
        ADD_TO_JSON_STRING(ref, "refTableName", pColRef->refTableName);
        ADD_TO_JSON_STRING(ref, "refColName", pColRef->refColName);
      }
    }
    ADD_TO_JSON_BOOL(column, "isPrimarykey", (s->flags & COL_IS_KEY));
    if (pColCmprRow == NULL) {
      continue;
    }

    uint32_t alg = 0;
    if (buildDefaultCompress) {
      alg = createDefaultColCmprByType(s->type);
    } else {
      SColCmpr* pColCmpr = pColCmprRow->pColCmpr + i;
      alg = pColCmpr->alg;
    }
    const char* encode = columnEncodeStr(COMPRESS_L1_TYPE_U32(alg));
    RAW_NULL_CHECK(encode);
    const char* compress = columnCompressStr(COMPRESS_L2_TYPE_U32(alg));
    RAW_NULL_CHECK(compress);
    const char* level = columnLevelStr(COMPRESS_L2_TYPE_LEVEL_U32(alg));
    RAW_NULL_CHECK(level);

    ADD_TO_JSON_STRING(column, "encode", encode);
    ADD_TO_JSON_STRING(column, "compress", compress);
    ADD_TO_JSON_STRING(column, "level", level);
  }

  cJSON* tags = cJSON_AddArrayToObject(json, "tags");
  RAW_NULL_CHECK(tags);

  for (int i = 0; schemaTag && i < schemaTag->nCols; i++) {
    cJSON* tag = tmqAddObjectToArray(tags);
    RAW_NULL_CHECK(tag);
    SSchema* s = schemaTag->pSchema + i;
    ADD_TO_JSON_STRING(tag, "name", s->name);
    ADD_TO_JSON_NUMBER(tag, "type", s->type);
    int32_t length = getLength(s->type, s->bytes, 0);
    if (length > 0) {
      ADD_TO_JSON_NUMBER(tag, "length", length);
    }
  }

end:
  *pJson = json;
  RAW_LOG_END
  return code;
}

static int32_t setCompressOption(cJSON* json, uint32_t para) {
  if (json == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  uint8_t encode = COMPRESS_L1_TYPE_U32(para);
  int32_t code = 0;
  int32_t lino = 0;
  RAW_LOG_START
  if (encode != 0) {
    const char* encodeStr = columnEncodeStr(encode);
    RAW_NULL_CHECK(encodeStr);
    ADD_TO_JSON_STRING(json, "encode", encodeStr);
    goto end;
  }
  uint8_t compress = COMPRESS_L2_TYPE_U32(para);
  if (compress != 0) {
    const char* compressStr = columnCompressStr(compress);
    RAW_NULL_CHECK(compressStr);
    ADD_TO_JSON_STRING(json, "compress", compressStr);
    goto end;
  }
  uint8_t level = COMPRESS_L2_TYPE_LEVEL_U32(para);
  if (level != 0) {
    const char* levelStr = columnLevelStr(level);
    RAW_NULL_CHECK(levelStr);
    ADD_TO_JSON_STRING(json, "level", levelStr);
    goto end;
  }

end:
  RAW_LOG_END
  return code;
}
static int32_t buildAlterSTableJson(void* alterData, int32_t alterDataLen, cJSON** pJson) {
  if (alterData == NULL || pJson == NULL) {
    uError("invalid parameter in %s alterData:%p", __func__, alterData);
    return TSDB_CODE_INVALID_PARA;
  }
  SMAlterStbReq req = {0};
  cJSON*        json = NULL;
  char*         string = NULL;
  int32_t       code = 0;
  int32_t       lino = 0;
  RAW_LOG_START
  RAW_RETURN_CHECK(tDeserializeSMAlterStbReq(alterData, alterDataLen, &req));
  json = cJSON_CreateObject();
  RAW_NULL_CHECK(json);
  ADD_TO_JSON_STRING(json, "type", "alter");
  SName name = {0};
  RAW_RETURN_CHECK(tNameFromString(&name, req.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE));
  ADD_TO_JSON_STRING(json, "tableType", "super");
  ADD_TO_JSON_STRING(json, "tableName", name.tname);
  ADD_TO_JSON_NUMBER(json, "alterType", req.alterType);

  switch (req.alterType) {
    case TSDB_ALTER_TABLE_ADD_TAG:
    case TSDB_ALTER_TABLE_ADD_COLUMN: {
      if (taosArrayGetSize(req.pFields) != 1) {
        uError("invalid field num %" PRIzu " for alter type %d", taosArrayGetSize(req.pFields), req.alterType);
        cJSON_Delete(json);
        json = NULL;
        code = TSDB_CODE_INVALID_PARA;
        goto end;
      }
      TAOS_FIELD* field = taosArrayGet(req.pFields, 0);
      RAW_NULL_CHECK(field);
      ADD_TO_JSON_STRING(json, "colName", field->name);
      ADD_TO_JSON_NUMBER(json, "colType", field->type);
      int32_t typeMode = 0;
      if (taosArrayGetSize(req.pTypeMods) > 0) {
        typeMode = *(STypeMod*)taosArrayGet(req.pTypeMods, 0);
      }
      int32_t length = getLength(field->type, field->bytes, typeMode);
      if (length > 0) {
        ADD_TO_JSON_NUMBER(json, "colLength", length);
      }

      break;
    }
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION: {
      SFieldWithOptions* field = taosArrayGet(req.pFields, 0);
      RAW_NULL_CHECK(field);
      ADD_TO_JSON_STRING(json, "colName", field->name);
      ADD_TO_JSON_NUMBER(json, "colType", field->type);
      int32_t typeMode = 0;
      if (taosArrayGetSize(req.pTypeMods) > 0) {
        typeMode = *(STypeMod*)taosArrayGet(req.pTypeMods, 0);
      }
      int32_t length = getLength(field->type, field->bytes, typeMode);
      if (length > 0) {
        ADD_TO_JSON_NUMBER(json, "colLength", length);
      }

      RAW_RETURN_CHECK(setCompressOption(json, field->compress));
      break;
    }
    case TSDB_ALTER_TABLE_DROP_TAG:
    case TSDB_ALTER_TABLE_DROP_COLUMN: {
      TAOS_FIELD* field = taosArrayGet(req.pFields, 0);
      RAW_NULL_CHECK(field);
      ADD_TO_JSON_STRING(json, "colName", field->name);
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_TAG_BYTES:
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES: {
      if (taosArrayGetSize(req.pFields) != 1) {
        uError("invalid field num %" PRIzu " for alter type %d", taosArrayGetSize(req.pFields), req.alterType);
        cJSON_Delete(json);
        json = NULL;
        code = TSDB_CODE_INVALID_PARA;
        goto end;
      }
      TAOS_FIELD* field = taosArrayGet(req.pFields, 0);
      RAW_NULL_CHECK(field);
      ADD_TO_JSON_STRING(json, "colName", field->name);
      ADD_TO_JSON_NUMBER(json, "colType", field->type);
      int32_t length = getLength(field->type, field->bytes, 0);
      if (length > 0) {
        ADD_TO_JSON_NUMBER(json, "colLength", length);
      }

      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_TAG_NAME:
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME: {
      TAOS_FIELD* oldField = taosArrayGet(req.pFields, 0);
      RAW_NULL_CHECK(oldField);
      TAOS_FIELD* newField = taosArrayGet(req.pFields, 1);
      RAW_NULL_CHECK(newField);
      ADD_TO_JSON_STRING(json, "colName", oldField->name);
      ADD_TO_JSON_STRING(json, "colNewName", newField->name);
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS: {
      TAOS_FIELD* field = taosArrayGet(req.pFields, 0);
      RAW_NULL_CHECK(field);
      ADD_TO_JSON_STRING(json, "colName", field->name);
      RAW_RETURN_CHECK(setCompressOption(json, field->bytes));
      break;
    }
    default:
      break;
  }

end:
  tFreeSMAltertbReq(&req);
  *pJson = json;
  RAW_LOG_END
  return code;
}

static int32_t processCreateStb(SMqMetaRsp* metaRsp, cJSON** pJson) {
  if (metaRsp == NULL || pJson == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SVCreateStbReq req = {0};
  SDecoder       coder = {0};

  RAW_LOG_START
  // decode and process req
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);

  RAW_RETURN_CHECK(tDecodeSVCreateStbReq(&coder, &req));
  RAW_RETURN_CHECK(buildCreateTableJson(&req.schemaRow, &req.schemaTag, req.pExtSchemas, req.name, req.suid,
                                        TSDB_SUPER_TABLE, req.virtualStb, NULL, &req.colCmpr, pJson));

end:
  tDecoderClear(&coder);
  RAW_LOG_END
  return code;
}

static int32_t processAlterStb(SMqMetaRsp* metaRsp, cJSON** pJson) {
  if (metaRsp == NULL || pJson == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  SVCreateStbReq req = {0};
  SDecoder       coder = {0};
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  RAW_LOG_START

  // decode and process req
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);

  RAW_RETURN_CHECK(tDecodeSVCreateStbReq(&coder, &req));
  RAW_RETURN_CHECK(buildAlterSTableJson(req.alterOriData, req.alterOriDataLen, pJson));

end:
  tDecoderClear(&coder);
  RAW_LOG_END
  return code;
}

static int32_t buildChildElement(cJSON* json, SVCreateTbReq* pCreateReq) {
  if (json == NULL || pCreateReq == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  STag*   pTag = (STag*)pCreateReq->ctb.pTag;
  char*   sname = pCreateReq->ctb.stbName;
  char*   name = pCreateReq->name;
  SArray* tagName = pCreateReq->ctb.tagName;
  int64_t id = pCreateReq->uid;
  uint8_t tagNum = pCreateReq->ctb.tagNum;
  int32_t code = 0;
  int32_t lino = 0;
  SArray* pTagVals = NULL;
  char*   pJson = NULL;
  char*   buf = NULL;
  RAW_LOG_START

  ADD_TO_JSON_STRING(json, "tableName", name);

  if (pCreateReq->type == TSDB_VIRTUAL_CHILD_TABLE) {
    cJSON* refs = cJSON_AddArrayToObject(json, "refs");
    RAW_NULL_CHECK(refs);

    for (int i = 0; i < pCreateReq->colRef.nCols; i++) {
      SColRef* pColRef = pCreateReq->colRef.pColRef + i;
  
      if (!pColRef->hasRef) {
        continue;
      }
      cJSON* ref = tmqAddObjectToArray(refs);
      RAW_NULL_CHECK(ref);
      ADD_TO_JSON_STRING(ref, "colName", pColRef->colName);
      ADD_TO_JSON_STRING(ref, "refDbName", pColRef->refDbName);
      ADD_TO_JSON_STRING(ref, "refTableName", pColRef->refTableName);
      ADD_TO_JSON_STRING(ref, "refColName", pColRef->refColName);
    }
  }

  ADD_TO_JSON_STRING(json, "using", sname);
  ADD_TO_JSON_NUMBER(json, "tagNum", tagNum);

  cJSON* tags = cJSON_AddArrayToObject(json, "tags");
  RAW_NULL_CHECK(tags);
  RAW_RETURN_CHECK(tTagToValArray(pTag, &pTagVals));
  if (tTagIsJson(pTag)) {
    STag* p = (STag*)pTag;
    if (p->nTag == 0) {
      uWarn("p->nTag == 0");
      goto end;
    }
    parseTagDatatoJson(pTag, &pJson, NULL);
    RAW_NULL_CHECK(pJson);
    cJSON* tag = tmqAddObjectToArray(tags);
    RAW_NULL_CHECK(tag);
    STagVal* pTagVal = taosArrayGet(pTagVals, 0);
    RAW_NULL_CHECK(pTagVal);
    char* ptname = taosArrayGet(tagName, 0);
    RAW_NULL_CHECK(ptname);
    ADD_TO_JSON_STRING(tag, "name", ptname);
    ADD_TO_JSON_NUMBER(tag, "type", TSDB_DATA_TYPE_JSON);
    ADD_TO_JSON_STRING(tag, "value", pJson);
    goto end;
  }

  for (int i = 0; i < taosArrayGetSize(pTagVals); i++) {
    STagVal* pTagVal = (STagVal*)taosArrayGet(pTagVals, i);
    RAW_NULL_CHECK(pTagVal);
    cJSON* tag = tmqAddObjectToArray(tags);
    RAW_NULL_CHECK(tag);
    char* ptname = taosArrayGet(tagName, i);
    RAW_NULL_CHECK(ptname);
    ADD_TO_JSON_STRING(tag, "name", ptname);
    ADD_TO_JSON_NUMBER(tag, "type", pTagVal->type);

    if (IS_VAR_DATA_TYPE(pTagVal->type)) {
      if (IS_STR_DATA_BLOB(pTagVal->type)) {
        goto end;
      }
      int64_t bufSize = 0;
      if (pTagVal->type == TSDB_DATA_TYPE_VARBINARY) {
        bufSize = pTagVal->nData * 2 + 2 + 3;
      } else {
        bufSize = pTagVal->nData + 3;
      }
      buf = taosMemoryCalloc(bufSize, 1);
      RAW_NULL_CHECK(buf);
      code = dataConverToStr(buf, bufSize, pTagVal->type, pTagVal->pData, pTagVal->nData, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        uError("convert tag value to string failed");
        goto end;
      }

      ADD_TO_JSON_STRING(tag, "value", buf)
      taosMemoryFreeClear(buf);
    } else {
      double val = 0;
      GET_TYPED_DATA(val, double, pTagVal->type, &pTagVal->i64, 0);  // currently tag type can't be decimal, so pass 0 as typeMod
      ADD_TO_JSON_NUMBER(tag, "value", val)
    }
  }

end:
  taosMemoryFree(pJson);
  taosArrayDestroy(pTagVals);
  taosMemoryFree(buf);
  RAW_LOG_END
  return code;
}

static int32_t buildCreateCTableJson(SVCreateTbReq* pCreateReq, int32_t nReqs, cJSON** pJson) {
  if (pJson == NULL || pCreateReq == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = 0;
  int32_t lino = 0;
  RAW_LOG_START
  char*  string = NULL;
  cJSON* json = cJSON_CreateObject();
  RAW_NULL_CHECK(json);
  ADD_TO_JSON_STRING(json, "type", "create");
  ADD_TO_JSON_STRING(json, "tableType", "child");

  RAW_RETURN_CHECK(buildChildElement(json, pCreateReq));
  cJSON* createList = cJSON_AddArrayToObject(json, "createList");
  RAW_NULL_CHECK(createList);

  for (int i = 0; nReqs > 1 && i < nReqs; i++) {
    cJSON* create = tmqAddObjectToArray(createList);
    RAW_NULL_CHECK(create);
    RAW_RETURN_CHECK(buildChildElement(create, pCreateReq + i));
  }

end:
  *pJson = json;
  RAW_LOG_END
  return code;
}

static int32_t processCreateTable(SMqMetaRsp* metaRsp, cJSON** pJson) {
  if (pJson == NULL || metaRsp == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SDecoder           decoder = {0};
  SVCreateTbBatchReq req = {0};
  SVCreateTbReq*     pCreateReq;
  RAW_LOG_START
  // decode
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  RAW_RETURN_CHECK(tDecodeSVCreateTbBatchReq(&decoder, &req));
  // loop to create table
  if (req.nReqs > 0) {
    pCreateReq = req.pReqs;
    if (pCreateReq->type == TSDB_CHILD_TABLE || pCreateReq->type == TSDB_VIRTUAL_CHILD_TABLE) {
      RAW_RETURN_CHECK(buildCreateCTableJson(req.pReqs, req.nReqs, pJson));
    } else if (pCreateReq->type == TSDB_NORMAL_TABLE || pCreateReq->type == TSDB_VIRTUAL_NORMAL_TABLE) {
      RAW_RETURN_CHECK(buildCreateTableJson(&pCreateReq->ntb.schemaRow, NULL, pCreateReq->pExtSchemas, pCreateReq->name,
                                            pCreateReq->uid, pCreateReq->type, pCreateReq->type == TSDB_NORMAL_TABLE ? false : true, 
                                            &pCreateReq->colRef, &pCreateReq->colCmpr, pJson));
    }
  }

end:
  tDeleteSVCreateTbBatchReq(&req);
  tDecoderClear(&decoder);
  RAW_LOG_END
  return code;
}

static int32_t processAutoCreateTable(SMqDataRsp* rsp, char** string) {
  int32_t lino = 0;
  int32_t code = TSDB_CODE_SUCCESS;
  RAW_LOG_START
  RAW_FALSE_CHECK(rsp != NULL && string != NULL);
  SDecoder*      decoder = NULL;
  SVCreateTbReq* pCreateReq = NULL;
  RAW_FALSE_CHECK(rsp->createTableNum > 0);

  decoder = taosMemoryCalloc(rsp->createTableNum, sizeof(SDecoder));
  RAW_NULL_CHECK(decoder);
  pCreateReq = taosMemoryCalloc(rsp->createTableNum, sizeof(SVCreateTbReq));
  RAW_NULL_CHECK(pCreateReq);

  // loop to create table
  for (int32_t iReq = 0; iReq < rsp->createTableNum; iReq++) {
    // decode
    void** data = taosArrayGet(rsp->createTableReq, iReq);
    RAW_NULL_CHECK(data);
    int32_t* len = taosArrayGet(rsp->createTableLen, iReq);
    RAW_NULL_CHECK(len);
    tDecoderInit(&decoder[iReq], *data, *len);
    RAW_RETURN_CHECK(tDecodeSVCreateTbReq(&decoder[iReq], pCreateReq + iReq));

    if (pCreateReq[iReq].type != TSDB_CHILD_TABLE && pCreateReq[iReq].type != TSDB_NORMAL_TABLE) {
      uError("%s failed. pCreateReq[iReq].type:%d invalid", __func__, pCreateReq[iReq].type);
      code = TSDB_CODE_INVALID_PARA;
      goto end;
    }
  }
  cJSON* pJson = NULL;
  if (pCreateReq->type == TSDB_NORMAL_TABLE) {
    RAW_RETURN_CHECK(buildCreateTableJson(&pCreateReq->ntb.schemaRow, NULL, pCreateReq->pExtSchemas, pCreateReq->name,
                                          pCreateReq->uid, TSDB_NORMAL_TABLE, false, NULL, &pCreateReq->colCmpr, &pJson));
  } else if (pCreateReq->type == TSDB_CHILD_TABLE) {
    RAW_RETURN_CHECK(buildCreateCTableJson(pCreateReq, rsp->createTableNum, &pJson));
  }

  *string = cJSON_PrintUnformatted(pJson);
  cJSON_Delete(pJson);

  uDebug("auto created table return, sql json:%s", *string);

end:
  RAW_LOG_END
  for (int i = 0; decoder && pCreateReq && i < rsp->createTableNum; i++) {
    tDecoderClear(&decoder[i]);
    taosMemoryFreeClear(pCreateReq[i].comment);
    if (pCreateReq[i].type == TSDB_CHILD_TABLE) {
      taosArrayDestroy(pCreateReq[i].ctb.tagName);
    }
  }
  taosMemoryFree(decoder);
  taosMemoryFree(pCreateReq);
  return code;
}

static int32_t processAlterTable(SMqMetaRsp* metaRsp, cJSON** pJson) {
  if (pJson == NULL || metaRsp == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  SDecoder     decoder = {0};
  SVAlterTbReq vAlterTbReq = {0};
  char*        string = NULL;
  cJSON*       json = NULL;
  int32_t      code = 0;
  int32_t      lino = 0;
  char*        buf = NULL;
  char*        buf1 = NULL;
  char*        buf2 = NULL;
  SNode*       pWhere = NULL;

  RAW_LOG_START

  // decode
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  RAW_RETURN_CHECK(tDecodeSVAlterTbReq(&decoder, &vAlterTbReq));

  json = cJSON_CreateObject();
  RAW_NULL_CHECK(json);
  ADD_TO_JSON_STRING(json, "type", "alter");

  char* tableType = NULL;
  if (vAlterTbReq.action == TSDB_ALTER_TABLE_UPDATE_MULTI_TABLE_TAG_VAL){
    tableType = "child";
  } else if (vAlterTbReq.action == TSDB_ALTER_TABLE_UPDATE_CHILD_TABLE_TAG_VAL){
    tableType = "super";
  } else if (vAlterTbReq.action == TSDB_ALTER_TABLE_ALTER_COLUMN_REF ||
             vAlterTbReq.action == TSDB_ALTER_TABLE_REMOVE_COLUMN_REF) {
    tableType = "";
  } else {
    tableType = "normal";
  }

  ADD_TO_JSON_STRING(json, "tableType", tableType);
  ADD_TO_JSON_STRING(json, "tableName", vAlterTbReq.action == TSDB_ALTER_TABLE_UPDATE_MULTI_TABLE_TAG_VAL ? "" : vAlterTbReq.tbName);
  ADD_TO_JSON_NUMBER(json, "alterType", vAlterTbReq.action);

  uDebug("alter table action:%d", vAlterTbReq.action);
  switch (vAlterTbReq.action) {
    case TSDB_ALTER_TABLE_ADD_COLUMN: 
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COLUMN_REF: {
      ADD_TO_JSON_STRING(json, "colName", vAlterTbReq.colName);
      ADD_TO_JSON_NUMBER(json, "colType", vAlterTbReq.type);

      int32_t length = getLength(vAlterTbReq.type, vAlterTbReq.bytes, vAlterTbReq.typeMod);
      if (length > 0) {
        ADD_TO_JSON_NUMBER(json, "colLength", length);
      }

      if (vAlterTbReq.action == TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COLUMN_REF) {
        ADD_TO_JSON_STRING(json, "refDbName", vAlterTbReq.refDbName);
        ADD_TO_JSON_STRING(json, "refTbName", vAlterTbReq.refTbName);
        ADD_TO_JSON_STRING(json, "refColName", vAlterTbReq.refColName);
      }
      break;
    }
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION: {
      ADD_TO_JSON_STRING(json, "colName", vAlterTbReq.colName);
      ADD_TO_JSON_NUMBER(json, "colType", vAlterTbReq.type);

      int32_t length = getLength(vAlterTbReq.type, vAlterTbReq.bytes, vAlterTbReq.typeMod);
      if (length > 0) {
        ADD_TO_JSON_NUMBER(json, "colLength", length);
      }
      RAW_RETURN_CHECK(setCompressOption(json, vAlterTbReq.compress));
      break;
    }
    case TSDB_ALTER_TABLE_DROP_COLUMN: {
      ADD_TO_JSON_STRING(json, "colName", vAlterTbReq.colName);
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES: {
      ADD_TO_JSON_STRING(json, "colName", vAlterTbReq.colName);
      ADD_TO_JSON_NUMBER(json, "colType", vAlterTbReq.colModType);
      int32_t length = getLength(vAlterTbReq.colModType, vAlterTbReq.colModBytes, vAlterTbReq.typeMod);
      if (length > 0) {
        ADD_TO_JSON_NUMBER(json, "colLength", length);
      }
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME: {
      ADD_TO_JSON_STRING(json, "colName", vAlterTbReq.colName);
      ADD_TO_JSON_STRING(json, "colNewName", vAlterTbReq.colNewName);
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_MULTI_TABLE_TAG_VAL: {
      int32_t nTables = taosArrayGetSize(vAlterTbReq.tables);
      if (nTables <= 0) {
        code = TSDB_CODE_INVALID_PARA;
        uError("processAlterTable parse multi tables error");
        goto end;
      }

      cJSON* tables = cJSON_AddArrayToObject(json, "tables");
      RAW_NULL_CHECK(tables);

      for (int32_t i = 0; i < nTables; i++) {
        SUpdateTableTagVal* pTable = taosArrayGet(vAlterTbReq.tables, i);
        cJSON* tableObj = tmqAddObjectToArray(tables);
        RAW_NULL_CHECK(tableObj);

        ADD_TO_JSON_STRING(tableObj, "tableName", pTable->tbName);

        int32_t nTags = taosArrayGetSize(pTable->tags);
        cJSON* tags = cJSON_AddArrayToObject(tableObj, "tags");
        RAW_NULL_CHECK(tags);

        for (int32_t j = 0; j < nTags; j++) {
          cJSON* member = tmqAddObjectToArray(tags);
          RAW_NULL_CHECK(member);

          SUpdatedTagVal* pTagVal = taosArrayGet(pTable->tags, j);
          ADD_TO_JSON_STRING(member, "colName", pTagVal->tagName);

          if (pTagVal->regexp != NULL) {
            ADD_TO_JSON_STRING(member, "regexp", pTagVal->regexp);
            ADD_TO_JSON_STRING(member, "replacement", pTagVal->replacement);
          } else {
            bool isNull = pTagVal->isNull;
            if (!isNull) {
              int64_t bufSize = 0;
              if (pTagVal->tagType == TSDB_DATA_TYPE_VARBINARY) {
                bufSize = pTagVal->nTagVal * 2 + 2 + 3;
              } else {
                bufSize = pTagVal->nTagVal + 3;
              }
              buf1 = taosMemoryCalloc(bufSize, 1);
              RAW_NULL_CHECK(buf1);
              code = dataConverToStr(buf1, bufSize, pTagVal->tagType, pTagVal->pTagVal, pTagVal->nTagVal, NULL);
              if (code != TSDB_CODE_SUCCESS) {
                uError("convert tag value to string failed");
                goto end;
              }
              ADD_TO_JSON_STRING(member, "colValue", buf1);
              taosMemoryFreeClear(buf1);
            }
            ADD_TO_JSON_BOOL(member, "colValueNull", isNull);
          }
        }
      }
      break;
    }

    case TSDB_ALTER_TABLE_UPDATE_CHILD_TABLE_TAG_VAL: {
      int32_t nTags = taosArrayGetSize(vAlterTbReq.pMultiTag);
      if (nTags <= 0) {
        code = TSDB_CODE_INVALID_PARA;
        uError("processAlterTable parse multi tags error");
        goto end;
      }

      cJSON* tags = cJSON_AddArrayToObject(json, "tags");
      RAW_NULL_CHECK(tags);

      for (int32_t i = 0; i < nTags; i++) {
        cJSON* member = tmqAddObjectToArray(tags);
        RAW_NULL_CHECK(member);

        SUpdatedTagVal* pTagVal = taosArrayGet(vAlterTbReq.pMultiTag, i);
        ADD_TO_JSON_STRING(member, "colName", pTagVal->tagName);

        if (pTagVal->regexp != NULL) {
          ADD_TO_JSON_STRING(member, "regexp", pTagVal->regexp);
          ADD_TO_JSON_STRING(member, "replacement", pTagVal->replacement);
        } else {
          bool isNull = pTagVal->isNull;
          if (!isNull) {
            int64_t bufSize = 0;
            if (pTagVal->tagType == TSDB_DATA_TYPE_VARBINARY) {
              bufSize = pTagVal->nTagVal * 2 + 2 + 3;
            } else {
              bufSize = pTagVal->nTagVal + 3;
            }
            buf1 = taosMemoryCalloc(bufSize, 1);
            RAW_NULL_CHECK(buf1);
            code = dataConverToStr(buf1, bufSize, pTagVal->tagType, pTagVal->pTagVal, pTagVal->nTagVal, NULL);
            if (code != TSDB_CODE_SUCCESS) {
              uError("convert tag value to string failed");
              goto end;
            }
            ADD_TO_JSON_STRING(member, "colValue", buf1);
            taosMemoryFreeClear(buf1);
          }
          ADD_TO_JSON_BOOL(member, "colValueNull", isNull);
        }
      }

      if (vAlterTbReq.whereLen > 0) {
        buf1 = taosMemoryCalloc(vAlterTbReq.whereLen, 1);
        RAW_NULL_CHECK(buf1);
        buf2 = taosMemoryCalloc(vAlterTbReq.whereLen, 1);
        RAW_NULL_CHECK(buf2);
        memcpy(buf2, vAlterTbReq.where, vAlterTbReq.whereLen);
        RAW_RETURN_CHECK(nodesMsgToNode(buf2, vAlterTbReq.whereLen, &pWhere));
        int32_t tlen = 0;
        RAW_RETURN_CHECK(nodesNodeToSQL(pWhere, buf1, vAlterTbReq.whereLen, &tlen));
        if (tlen >= 1) buf1[tlen - 1] = 0;
        ADD_TO_JSON_STRING(json, "where", buf1 + 1);
        taosMemoryFreeClear(buf1);
        taosMemoryFreeClear(buf2);
        nodesDestroyNode(pWhere);
      }
      break;
    }

    case TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS: {
      ADD_TO_JSON_STRING(json, "colName", vAlterTbReq.colName);
      RAW_RETURN_CHECK(setCompressOption(json, vAlterTbReq.compress));
      break;
    }
    case TSDB_ALTER_TABLE_ALTER_COLUMN_REF: {
      ADD_TO_JSON_STRING(json, "colName", vAlterTbReq.colName);
      ADD_TO_JSON_STRING(json, "refDbName", vAlterTbReq.refDbName);
      ADD_TO_JSON_STRING(json, "refTbName", vAlterTbReq.refTbName);
      ADD_TO_JSON_STRING(json, "refColName", vAlterTbReq.refColName);
      break;
    }
    case TSDB_ALTER_TABLE_REMOVE_COLUMN_REF:{
      ADD_TO_JSON_STRING(json, "colName", vAlterTbReq.colName);
      break;
    }
    default:
      break;
  }

end:
  destroyAlterTbReq(&vAlterTbReq);
  tDecoderClear(&decoder);
  taosMemoryFree(buf);
  taosMemoryFree(buf1);
  taosMemoryFree(buf2);
  *pJson = json;
  RAW_LOG_END
  return code;
}

static int32_t processDropSTable(SMqMetaRsp* metaRsp, cJSON** pJson) {
  if (pJson == NULL || metaRsp == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  SDecoder     decoder = {0};
  SVDropStbReq req = {0};
  cJSON*       json = NULL;
  int32_t      code = 0;
  int32_t      lino = 0;
  RAW_LOG_START

  // decode
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  RAW_RETURN_CHECK(tDecodeSVDropStbReq(&decoder, &req));

  json = cJSON_CreateObject();
  RAW_NULL_CHECK(json);
  ADD_TO_JSON_STRING(json, "type", "drop");
  ADD_TO_JSON_STRING(json, "tableType", "super");
  ADD_TO_JSON_STRING(json, "tableName", req.name);

end:
  tDecoderClear(&decoder);
  *pJson = json;
  RAW_LOG_END
  return code;
}
static int32_t processDeleteTable(SMqMetaRsp* metaRsp, cJSON** pJson) {
  if (pJson == NULL || metaRsp == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  SDeleteRes req = {0};
  SDecoder   coder = {0};
  cJSON*     json = NULL;
  int32_t    code = 0;
  int32_t    lino = 0;
  RAW_LOG_START

  // decode and process req
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);

  tDecoderInit(&coder, data, len);
  RAW_RETURN_CHECK(tDecodeDeleteRes(&coder, &req));
  //  getTbName(req.tableFName);
  char sql[256] = {0};
  (void)snprintf(sql, sizeof(sql), "delete from `%s` where `%s` >= %" PRId64 " and `%s` <= %" PRId64, req.tableFName,
                 req.tsColName, req.skey, req.tsColName, req.ekey);

  json = cJSON_CreateObject();
  RAW_NULL_CHECK(json);
  ADD_TO_JSON_STRING(json, "type", "delete");
  ADD_TO_JSON_STRING(json, "sql", sql);

end:
  tDecoderClear(&coder);
  *pJson = json;
  RAW_LOG_END
  return code;
}

static int32_t processDropTable(SMqMetaRsp* metaRsp, cJSON** pJson) {
  if (pJson == NULL || metaRsp == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  SDecoder         decoder = {0};
  SVDropTbBatchReq req = {0};
  cJSON*           json = NULL;
  int32_t          code = 0;
  int32_t          lino = 0;
  RAW_LOG_START
  // decode
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  RAW_RETURN_CHECK(tDecodeSVDropTbBatchReq(&decoder, &req));

  json = cJSON_CreateObject();
  RAW_NULL_CHECK(json);
  ADD_TO_JSON_STRING(json, "type", "drop");

  cJSON* tableNameList = cJSON_AddArrayToObject(json, "tableNameList");
  RAW_NULL_CHECK(tableNameList);

  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    SVDropTbReq* pDropTbReq = req.pReqs + iReq;
    RAW_NULL_CHECK(tmqAddStringToArray(tableNameList, pDropTbReq->name));
  }

end:
  tDecoderClear(&decoder);
  *pJson = json;
  RAW_LOG_END
  return code;
}

static int32_t taosCreateStb(TAOS* taos, void* meta, uint32_t metaLen) {
  if (taos == NULL || meta == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  SVCreateStbReq req = {0};
  SDecoder       coder = {0};
  SMCreateStbReq pReq = {0};
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SRequestObj*   pRequest = NULL;
  SCmdMsgInfo    pCmdMsg = {0};
  RAW_LOG_START

  RAW_RETURN_CHECK(buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0));
  uDebug(LOG_ID_TAG " create stable, meta:%p, metaLen:%d", LOG_ID_VALUE, meta, metaLen);
  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    uError(LOG_ID_TAG " %s no database selected", LOG_ID_VALUE, __func__);
    goto end;
  }
  // decode and process req
  void*    data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  uint32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  RAW_RETURN_CHECK(tDecodeSVCreateStbReq(&coder, &req));

  int8_t           createDefaultCompress = 0;
  SColCmprWrapper* p = &req.colCmpr;
  if (p->nCols == 0) {
    createDefaultCompress = 1;
  }
  // build create stable
  pReq.pColumns = taosArrayInit(req.schemaRow.nCols, sizeof(SFieldWithOptions));
  RAW_NULL_CHECK(pReq.pColumns);
  for (int32_t i = 0; i < req.schemaRow.nCols; i++) {
    SSchema*          pSchema = req.schemaRow.pSchema + i;
    SFieldWithOptions field = {.type = pSchema->type, .flags = pSchema->flags, .bytes = pSchema->bytes};
    tstrncpy(field.name, pSchema->name, TSDB_COL_NAME_LEN);

    if (createDefaultCompress) {
      field.compress = createDefaultColCmprByType(pSchema->type);
    } else {
      SColCmpr* pCmp = &req.colCmpr.pColCmpr[i];
      field.compress = pCmp->alg;
    }
    if (req.pExtSchemas) field.typeMod = req.pExtSchemas[i].typeMod;
    RAW_NULL_CHECK(taosArrayPush(pReq.pColumns, &field));
  }
  pReq.pTags = taosArrayInit(req.schemaTag.nCols, sizeof(SField));
  RAW_NULL_CHECK(pReq.pTags);
  for (int32_t i = 0; i < req.schemaTag.nCols; i++) {
    SSchema* pSchema = req.schemaTag.pSchema + i;
    SField   field = {.type = pSchema->type, .flags = pSchema->flags, .bytes = pSchema->bytes};
    tstrncpy(field.name, pSchema->name, TSDB_COL_NAME_LEN);
    RAW_NULL_CHECK(taosArrayPush(pReq.pTags, &field));
  }

  pReq.colVer = req.schemaRow.version;
  pReq.tagVer = req.schemaTag.version;
  pReq.numOfColumns = req.schemaRow.nCols;
  pReq.numOfTags = req.schemaTag.nCols;
  pReq.commentLen = -1;
  pReq.suid = processSuid(req.suid, pRequest->pDb);
  pReq.source = TD_REQ_FROM_TAOX;
  pReq.igExists = true;
  pReq.virtualStb = req.virtualStb;

  uDebug(LOG_ID_TAG " create stable name:%s suid:%" PRId64 " processSuid:%" PRId64, LOG_ID_VALUE, req.name, req.suid,
         pReq.suid);
  STscObj* pTscObj = pRequest->pTscObj;
  SName    tableName = {0};
  toName(pTscObj->acctId, pRequest->pDb, req.name, &tableName);
  RAW_RETURN_CHECK(tNameExtractFullName(&tableName, pReq.name));
  pCmdMsg.epSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);
  pCmdMsg.msgType = TDMT_MND_CREATE_STB;
  pCmdMsg.msgLen = tSerializeSMCreateStbReq(NULL, 0, &pReq);
  RAW_FALSE_CHECK(pCmdMsg.msgLen > 0);
  pCmdMsg.pMsg = taosMemoryMalloc(pCmdMsg.msgLen);
  RAW_NULL_CHECK(pCmdMsg.pMsg);
  RAW_FALSE_CHECK(tSerializeSMCreateStbReq(pCmdMsg.pMsg, pCmdMsg.msgLen, &pReq) > 0);

  SQuery pQuery = {0};
  pQuery.execMode = QUERY_EXEC_MODE_RPC;
  pQuery.pCmdMsg = &pCmdMsg;
  pQuery.msgType = pQuery.pCmdMsg->msgType;
  pQuery.stableQuery = true;

  launchQueryImpl(pRequest, &pQuery, true, NULL);  // ignore, because return value is pRequest

  if (pRequest->code == TSDB_CODE_SUCCESS) {
    SCatalog* pCatalog = NULL;
    RAW_RETURN_CHECK(catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog));
    RAW_RETURN_CHECK(catalogRemoveTableMeta(pCatalog, &tableName));
  }

  code = pRequest->code;
  uDebug(LOG_ID_TAG " create stable return, msg:%s", LOG_ID_VALUE, tstrerror(code));

end:
  destroyRequest(pRequest);
  tFreeSMCreateStbReq(&pReq);
  tDecoderClear(&coder);
  taosMemoryFree(pCmdMsg.pMsg);
  RAW_LOG_END
  return code;
}

static int32_t taosDropStb(TAOS* taos, void* meta, uint32_t metaLen) {
  if (taos == NULL || meta == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  SVDropStbReq req = {0};
  SDecoder     coder = {0};
  SMDropStbReq pReq = {0};
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SRequestObj* pRequest = NULL;
  SCmdMsgInfo  pCmdMsg = {0};

  RAW_LOG_START
  RAW_RETURN_CHECK(buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0));
  uDebug(LOG_ID_TAG " drop stable, meta:%p, metaLen:%d", LOG_ID_VALUE, meta, metaLen);
  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
    uError(LOG_ID_TAG " %s no database selected", LOG_ID_VALUE, __func__);
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void*    data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  uint32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  RAW_RETURN_CHECK(tDecodeSVDropStbReq(&coder, &req));
  SCatalog* pCatalog = NULL;
  RAW_RETURN_CHECK(catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog));
  SRequestConnInfo conn = {.pTrans = pRequest->pTscObj->pAppInfo->pTransporter,
                           .requestId = pRequest->requestId,
                           .requestObjRefId = pRequest->self,
                           .mgmtEps = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp)};
  SName            pName = {0};
  toName(pRequest->pTscObj->acctId, pRequest->pDb, req.name, &pName);
  STableMeta* pTableMeta = NULL;
  code = catalogGetTableMeta(pCatalog, &conn, &pName, &pTableMeta);
  if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
    uInfo(LOG_ID_TAG " stable %s not exist, ignore drop", LOG_ID_VALUE, req.name);
    code = TSDB_CODE_SUCCESS;
    taosMemoryFreeClear(pTableMeta);
    goto end;
  }
  RAW_RETURN_CHECK(code);
  pReq.suid = pTableMeta->uid;
  taosMemoryFreeClear(pTableMeta);

  // build drop stable
  pReq.igNotExists = true;
  pReq.source = TD_REQ_FROM_TAOX;
  //  pReq.suid = processSuid(req.suid, pRequest->pDb);

  uDebug(LOG_ID_TAG " drop stable name:%s suid:%" PRId64 " new suid:%" PRId64, LOG_ID_VALUE, req.name, req.suid,
         pReq.suid);
  STscObj* pTscObj = pRequest->pTscObj;
  SName    tableName = {0};
  toName(pTscObj->acctId, pRequest->pDb, req.name, &tableName);
  RAW_RETURN_CHECK(tNameExtractFullName(&tableName, pReq.name));

  pCmdMsg.epSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);
  pCmdMsg.msgType = TDMT_MND_DROP_STB;
  pCmdMsg.msgLen = tSerializeSMDropStbReq(NULL, 0, &pReq);
  RAW_FALSE_CHECK(pCmdMsg.msgLen > 0);
  pCmdMsg.pMsg = taosMemoryMalloc(pCmdMsg.msgLen);
  RAW_NULL_CHECK(pCmdMsg.pMsg);
  RAW_FALSE_CHECK(tSerializeSMDropStbReq(pCmdMsg.pMsg, pCmdMsg.msgLen, &pReq) > 0);

  SQuery pQuery = {0};
  pQuery.execMode = QUERY_EXEC_MODE_RPC;
  pQuery.pCmdMsg = &pCmdMsg;
  pQuery.msgType = pQuery.pCmdMsg->msgType;
  pQuery.stableQuery = true;

  launchQueryImpl(pRequest, &pQuery, true, NULL);  // ignore, because return value is pRequest
  if (pRequest->code == TSDB_CODE_SUCCESS) {
    // ignore the error code
    RAW_RETURN_CHECK(catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog));
    RAW_RETURN_CHECK(catalogRemoveTableMeta(pCatalog, &tableName));
  }

  code = pRequest->code;
  uDebug(LOG_ID_TAG " drop stable return, msg:%s", LOG_ID_VALUE, tstrerror(code));

end:
  RAW_LOG_END
  destroyRequest(pRequest);
  tDecoderClear(&coder);
  return code;
}

typedef struct SVgroupCreateTableBatch {
  SVCreateTbBatchReq req;
  SVgroupInfo        info;
  char               dbName[TSDB_DB_NAME_LEN];
} SVgroupCreateTableBatch;

static void destroyCreateTbReqBatch(void* data) {
  if (data == NULL) {
    uError("invalid parameter in %s", __func__);
    return;
  }
  SVgroupCreateTableBatch* pTbBatch = (SVgroupCreateTableBatch*)data;
  taosArrayDestroy(pTbBatch->req.pArray);
}

static const SSchema* getNormalColSchema(const STableMeta* pTableMeta, const char* pColName) {
  for (int32_t i = 0; i < pTableMeta->tableInfo.numOfColumns; ++i) {
    const SSchema* pSchema = pTableMeta->schema + i;
    if (0 == strcmp(pColName, pSchema->name)) {
      return pSchema;
    }
  }
  return NULL;
}

static STableMeta* getTableMeta(SCatalog* pCatalog, SRequestConnInfo* conn, char* dbName, char* tbName, int32_t acctId){
  SName       sName = {0};
  toName(acctId, dbName, tbName, &sName);
  STableMeta* pTableMeta = NULL;
  int32_t code = catalogGetTableMeta(pCatalog, conn, &sName, &pTableMeta);
  if (code != 0) {
    uError("failed to get table meta for reference table:%s.%s", dbName, tbName);
    taosMemoryFreeClear(pTableMeta);
    terrno = code;
    return NULL;
  }
  return pTableMeta;
}

static int32_t checkColRef(STableMeta* pTableMeta, char* colName, uint8_t precision, const SSchema* pSchema) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pTableMeta->tableInfo.precision != precision) {
    code = TSDB_CODE_PAR_INVALID_REF_COLUMN_TYPE;
    uError("timestamp precision of virtual table and its reference table do not match");
    goto end;
  }
  // org table cannot has composite primary key
  if (pTableMeta->tableInfo.numOfColumns > 1 && pTableMeta->schema[1].flags & COL_IS_KEY) {
    code = TSDB_CODE_PAR_INVALID_REF_COLUMN;
    uError("virtual table's column:\"%s\"'s reference can not from table with composite key", colName);
    goto end;
  }

  // org table must be child table or normal table
  if (pTableMeta->tableType != TSDB_NORMAL_TABLE && pTableMeta->tableType != TSDB_CHILD_TABLE) {
    code = TSDB_CODE_PAR_INVALID_REF_COLUMN;
    uError("virtual table's column:\"%s\"'s reference can only be normal table or child table", colName);
    goto end;
  }

  const SSchema* pRefCol = getNormalColSchema(pTableMeta, colName);
  if (NULL == pRefCol) {
    code = TSDB_CODE_PAR_INVALID_REF_COLUMN;
    uError("virtual table's column:\"%s\"'s reference column:\"%s\" not exist", pSchema->name, colName);
    goto end;
  }

  if (pRefCol->type != pSchema->type || pRefCol->bytes != pSchema->bytes) {
    code = TSDB_CODE_PAR_INVALID_REF_COLUMN_TYPE;
    uError("virtual table's column:\"%s\"'s type and reference column:\"%s\"'s type not match, %d %d %d %d",
            pSchema->name, colName, pSchema->type, pSchema->bytes, pRefCol->type, pRefCol->bytes);
    goto end;
  }

end:
  return code;
}

static int32_t checkColRefForCreate(SCatalog* pCatalog, SRequestConnInfo* conn, SColRef* pColRef, int32_t acctId, uint8_t precision, SSchema* pSchema) {
  STableMeta* pTableMeta = getTableMeta(pCatalog, conn, pColRef->refDbName, pColRef->refTableName, acctId);
  if (pTableMeta == NULL) {
      return terrno;
  }
  int32_t code = checkColRef(pTableMeta, pColRef->refColName, precision, pSchema);
  taosMemoryFreeClear(pTableMeta);
  return code;
}

static int32_t checkColRefForAdd(SCatalog* pCatalog, SRequestConnInfo* conn, int32_t acctId, char* dbName, char* tbName, char* colName, 
  char* dbNameSrc, char* tbNameSrc, char* colNameSrc, int8_t type, int32_t bytes) {
  int32_t code = 0;
  STableMeta* pTableMeta = getTableMeta(pCatalog, conn, dbName, tbName, acctId);
  if (pTableMeta == NULL) {
    code = terrno;
    goto end;
  }
  STableMeta* pTableMetaSrc = getTableMeta(pCatalog, conn, dbNameSrc, tbNameSrc, acctId);
  if (pTableMetaSrc == NULL) {
    code = terrno;
    goto end;
  }

  SSchema pSchema = {.type = type, .bytes = bytes};
  tstrncpy(pSchema.name, colNameSrc, TSDB_COL_NAME_LEN);
  code = checkColRef(pTableMeta, colName, pTableMetaSrc->tableInfo.precision, &pSchema);

end:
  taosMemoryFreeClear(pTableMeta);
  taosMemoryFreeClear(pTableMetaSrc);
  return code;
}

static int32_t checkColRefForAlter(SCatalog* pCatalog, SRequestConnInfo* conn, int32_t acctId, char* dbName, char* tbName, char* colName, 
  char* dbNameSrc, char* tbNameSrc, char* colNameSrc) {
  int32_t code = 0;
  STableMeta* pTableMeta = getTableMeta(pCatalog, conn, dbName, tbName, acctId);
  if (pTableMeta == NULL) {
    code = terrno;
    goto end;
  }
  STableMeta* pTableMetaSrc = getTableMeta(pCatalog, conn, dbNameSrc, tbNameSrc, acctId);
  if (pTableMetaSrc == NULL) {
    code = terrno;
    goto end;
  }
  const SSchema* pSchema = getNormalColSchema(pTableMetaSrc, colNameSrc);
  if (NULL == pSchema) {
    code = TSDB_CODE_PAR_INVALID_REF_COLUMN;
    uError("virtual table's column:\"%s\" not exist", colNameSrc);
    goto end;
  }

  code = checkColRef(pTableMeta, colName, pTableMetaSrc->tableInfo.precision, pSchema);

end:
  taosMemoryFreeClear(pTableMeta);
  taosMemoryFreeClear(pTableMetaSrc);
  return code;
}

static int32_t taosCreateTable(TAOS* taos, void* meta, uint32_t metaLen) {
  if (taos == NULL || meta == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  SVCreateTbBatchReq req = {0};
  SDecoder           coder = {0};
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SRequestObj*       pRequest = NULL;
  SQuery*            pQuery = NULL;
  SHashObj*          pVgroupHashmap = NULL;

  RAW_LOG_START
  SArray* pTagList = taosArrayInit(0, POINTER_BYTES);
  RAW_NULL_CHECK(pTagList);
  RAW_RETURN_CHECK(buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0));
  uDebug(LOG_ID_TAG " create table, meta:%p, metaLen:%d", LOG_ID_VALUE, meta, metaLen);

  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
    uError(LOG_ID_TAG " %s no database selected", LOG_ID_VALUE, __func__);
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void*    data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  uint32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  RAW_RETURN_CHECK(tDecodeSVCreateTbBatchReq(&coder, &req));
  STscObj* pTscObj = pRequest->pTscObj;

  SVCreateTbReq* pCreateReq = NULL;
  SCatalog*      pCatalog = NULL;
  RAW_RETURN_CHECK(catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog));
  pVgroupHashmap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  RAW_NULL_CHECK(pVgroupHashmap);
  taosHashSetFreeFp(pVgroupHashmap, destroyCreateTbReqBatch);

  SRequestConnInfo conn = {.pTrans = pTscObj->pAppInfo->pTransporter,
                           .requestId = pRequest->requestId,
                           .requestObjRefId = pRequest->self,
                           .mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp)};

  pRequest->tableList = taosArrayInit(req.nReqs, sizeof(SName));
  RAW_NULL_CHECK(pRequest->tableList);
  // loop to create table
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pCreateReq = req.pReqs + iReq;

    SVgroupInfo pInfo = {0};
    SName       pName = {0};
    toName(pTscObj->acctId, pRequest->pDb, pCreateReq->name, &pName);
    RAW_RETURN_CHECK(catalogGetTableHashVgroup(pCatalog, &conn, &pName, &pInfo));

    pCreateReq->flags |= TD_CREATE_IF_NOT_EXISTS;
    // change tag cid to new cid
    if (pCreateReq->type == TSDB_CHILD_TABLE || pCreateReq->type == TSDB_VIRTUAL_CHILD_TABLE) {
      STableMeta* pTableMeta = NULL;
      SName       sName = {0};
      tb_uid_t    oldSuid = pCreateReq->ctb.suid;
      //      pCreateReq->ctb.suid = processSuid(pCreateReq->ctb.suid, pRequest->pDb);
      toName(pTscObj->acctId, pRequest->pDb, pCreateReq->ctb.stbName, &sName);
      code = catalogGetTableMeta(pCatalog, &conn, &sName, &pTableMeta);
      if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
        uInfo(LOG_ID_TAG " super table %s not exist, ignore create child table %s", LOG_ID_VALUE,
              pCreateReq->ctb.stbName, pCreateReq->name);
        code = TSDB_CODE_SUCCESS;
        taosMemoryFreeClear(pTableMeta);
        continue;
      }

      RAW_RETURN_CHECK(code);
      pCreateReq->ctb.suid = pTableMeta->uid;

      bool changeDB = strlen(tmqWriteRefDB) > 0;
      for (int32_t i = 0; changeDB && i < pCreateReq->colRef.nCols; i++) {
        SColRef* pColRef = pCreateReq->colRef.pColRef + i;
        tstrncpy(pColRef->refDbName, tmqWriteRefDB, TSDB_DB_NAME_LEN);
      }

      for (int32_t i = 0; tmqWriteCheckRef && i < pCreateReq->colRef.nCols && i < pTableMeta->tableInfo.numOfColumns; i++) {
        SColRef* pColRef = pCreateReq->colRef.pColRef + i;
        if (!pColRef || !pColRef->hasRef) continue;
        SSchema* pSchema = pTableMeta->schema + i;
        RAW_RETURN_CHECK(checkColRefForCreate(pCatalog, &conn, pColRef, pTscObj->acctId, pTableMeta->tableInfo.precision, pSchema));
      }
      
      SArray* pTagVals = NULL;
      code = tTagToValArray((STag*)pCreateReq->ctb.pTag, &pTagVals);
      if (code != TSDB_CODE_SUCCESS) {
        uError("create tb invalid tag data %s", pCreateReq->name);
        taosMemoryFreeClear(pTableMeta);
        goto end;
      }

      bool rebuildTag = false;
      for (int32_t i = 0; i < taosArrayGetSize(pCreateReq->ctb.tagName); i++) {
        char* tName = taosArrayGet(pCreateReq->ctb.tagName, i);
        if (tName == NULL) {
          continue;
        }
        for (int32_t j = pTableMeta->tableInfo.numOfColumns;
             j < pTableMeta->tableInfo.numOfColumns + pTableMeta->tableInfo.numOfTags; j++) {
          SSchema* tag = &pTableMeta->schema[j];
          if (strcmp(tag->name, tName) == 0 && tag->type != TSDB_DATA_TYPE_JSON) {
            STagVal* pTagVal = (STagVal*)taosArrayGet(pTagVals, i);
            if (pTagVal) {
              if (pTagVal->cid != tag->colId) {
                pTagVal->cid = tag->colId;
                rebuildTag = true;
              }
            } else {
              uError("create tb invalid data %s, size:%d index:%d cid:%d", pCreateReq->name,
                     (int)taosArrayGetSize(pTagVals), i, tag->colId);
            }
          }
        }
      }
      taosMemoryFreeClear(pTableMeta);
      if (rebuildTag) {
        STag* ppTag = NULL;
        code = tTagNew(pTagVals, 1, false, &ppTag);
        taosArrayDestroy(pTagVals);
        pTagVals = NULL;
        if (code != TSDB_CODE_SUCCESS) {
          goto end;
        }
        if (NULL == taosArrayPush(pTagList, &ppTag)) {
          code = terrno;
          tTagFree(ppTag);
          goto end;
        }
        pCreateReq->ctb.pTag = (uint8_t*)ppTag;
      }
      taosArrayDestroy(pTagVals);
    }
    RAW_NULL_CHECK(taosArrayPush(pRequest->tableList, &pName));

    SVgroupCreateTableBatch* pTableBatch = taosHashGet(pVgroupHashmap, &pInfo.vgId, sizeof(pInfo.vgId));
    if (pTableBatch == NULL) {
      SVgroupCreateTableBatch tBatch = {0};
      tBatch.info = pInfo;
      tstrncpy(tBatch.dbName, pRequest->pDb, TSDB_DB_NAME_LEN);

      tBatch.req.pArray = taosArrayInit(4, sizeof(struct SVCreateTbReq));
      RAW_NULL_CHECK(tBatch.req.pArray);
      RAW_NULL_CHECK(taosArrayPush(tBatch.req.pArray, pCreateReq));
      tBatch.req.source = TD_REQ_FROM_TAOX;
      RAW_RETURN_CHECK(taosHashPut(pVgroupHashmap, &pInfo.vgId, sizeof(pInfo.vgId), &tBatch, sizeof(tBatch)));
    } else {  // add to the correct vgroup
      RAW_NULL_CHECK(taosArrayPush(pTableBatch->req.pArray, pCreateReq));
    }
  }

  if (taosHashGetSize(pVgroupHashmap) == 0) {
    goto end;
  }
  SArray* pBufArray = NULL;
  RAW_RETURN_CHECK(serializeVgroupsCreateTableBatch(pVgroupHashmap, &pBufArray));
  pQuery = NULL;
  code = nodesMakeNode(QUERY_NODE_QUERY, (SNode**)&pQuery);
  if (TSDB_CODE_SUCCESS != code) goto end;
  pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  pQuery->msgType = TDMT_VND_CREATE_TABLE;
  pQuery->stableQuery = false;
  code = nodesMakeNode(QUERY_NODE_CREATE_TABLE_STMT, &pQuery->pRoot);
  if (TSDB_CODE_SUCCESS != code) goto end;
  RAW_NULL_CHECK(pQuery->pRoot);

  RAW_RETURN_CHECK(rewriteToVnodeModifyOpStmt(pQuery, pBufArray));

  launchQueryImpl(pRequest, pQuery, true, NULL);
  if (pRequest->code == TSDB_CODE_SUCCESS) {
    RAW_RETURN_CHECK(removeMeta(pTscObj, pRequest->tableList, false));
  }

  code = pRequest->code;
  uDebug(LOG_ID_TAG " create table return, msg:%s", LOG_ID_VALUE, tstrerror(code));

end:
  tDeleteSVCreateTbBatchReq(&req);

  taosHashCleanup(pVgroupHashmap);
  destroyRequest(pRequest);
  tDecoderClear(&coder);
  qDestroyQuery(pQuery);
  taosArrayDestroyP(pTagList, NULL);
  RAW_LOG_END
  return code;
}

typedef struct SVgroupDropTableBatch {
  SVDropTbBatchReq req;
  SVgroupInfo      info;
  char             dbName[TSDB_DB_NAME_LEN];
} SVgroupDropTableBatch;

static void destroyDropTbReqBatch(void* data) {
  if (data == NULL) {
    uError("invalid parameter in %s", __func__);
    return;
  }
  SVgroupDropTableBatch* pTbBatch = (SVgroupDropTableBatch*)data;
  taosArrayDestroy(pTbBatch->req.pArray);
}

static int32_t taosDropTable(TAOS* taos, void* meta, uint32_t metaLen) {
  if (taos == NULL || meta == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  SVDropTbBatchReq req = {0};
  SDecoder         coder = {0};
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SRequestObj*     pRequest = NULL;
  SQuery*          pQuery = NULL;
  SHashObj*        pVgroupHashmap = NULL;

  RAW_RETURN_CHECK(buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0));
  uDebug(LOG_ID_TAG " drop table, meta:%p, len:%d", LOG_ID_VALUE, meta, metaLen);

  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
    uError(LOG_ID_TAG " %s no database selected", LOG_ID_VALUE, __func__);
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void*    data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  uint32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  RAW_RETURN_CHECK(tDecodeSVDropTbBatchReq(&coder, &req));
  STscObj* pTscObj = pRequest->pTscObj;

  SVDropTbReq* pDropReq = NULL;
  SCatalog*    pCatalog = NULL;
  RAW_RETURN_CHECK(catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog));

  pVgroupHashmap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  RAW_NULL_CHECK(pVgroupHashmap);
  taosHashSetFreeFp(pVgroupHashmap, destroyDropTbReqBatch);

  SRequestConnInfo conn = {.pTrans = pTscObj->pAppInfo->pTransporter,
                           .requestId = pRequest->requestId,
                           .requestObjRefId = pRequest->self,
                           .mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp)};
  pRequest->tableList = taosArrayInit(req.nReqs, sizeof(SName));
  RAW_NULL_CHECK(pRequest->tableList);
  // loop to create table
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pDropReq = req.pReqs + iReq;
    pDropReq->igNotExists = true;
    //    pDropReq->suid = processSuid(pDropReq->suid, pRequest->pDb);

    SVgroupInfo pInfo = {0};
    SName       pName = {0};
    toName(pTscObj->acctId, pRequest->pDb, pDropReq->name, &pName);
    RAW_RETURN_CHECK(catalogGetTableHashVgroup(pCatalog, &conn, &pName, &pInfo));

    STableMeta* pTableMeta = NULL;
    code = catalogGetTableMeta(pCatalog, &conn, &pName, &pTableMeta);
    if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
      code = TSDB_CODE_SUCCESS;
      uInfo(LOG_ID_TAG " table %s not exist, ignore drop", LOG_ID_VALUE, pDropReq->name);
      taosMemoryFreeClear(pTableMeta);
      continue;
    }
    RAW_RETURN_CHECK(code);
    tb_uid_t oldSuid = pDropReq->suid;
    pDropReq->suid = pTableMeta->suid;
    taosMemoryFreeClear(pTableMeta);
    uDebug(LOG_ID_TAG " drop table name:%s suid:%" PRId64 " new suid:%" PRId64, LOG_ID_VALUE, pDropReq->name, oldSuid,
           pDropReq->suid);

    RAW_NULL_CHECK(taosArrayPush(pRequest->tableList, &pName));
    SVgroupDropTableBatch* pTableBatch = taosHashGet(pVgroupHashmap, &pInfo.vgId, sizeof(pInfo.vgId));
    if (pTableBatch == NULL) {
      SVgroupDropTableBatch tBatch = {0};
      tBatch.info = pInfo;
      tBatch.req.pArray = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SVDropTbReq));
      RAW_NULL_CHECK(tBatch.req.pArray);
      RAW_NULL_CHECK(taosArrayPush(tBatch.req.pArray, pDropReq));
      RAW_RETURN_CHECK(taosHashPut(pVgroupHashmap, &pInfo.vgId, sizeof(pInfo.vgId), &tBatch, sizeof(tBatch)));
    } else {  // add to the correct vgroup
      RAW_NULL_CHECK(taosArrayPush(pTableBatch->req.pArray, pDropReq));
    }
  }

  if (taosHashGetSize(pVgroupHashmap) == 0) {
    goto end;
  }
  SArray* pBufArray = NULL;
  RAW_RETURN_CHECK(serializeVgroupsDropTableBatch(pVgroupHashmap, &pBufArray));
  code = nodesMakeNode(QUERY_NODE_QUERY, (SNode**)&pQuery);
  if (TSDB_CODE_SUCCESS != code) goto end;
  pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  pQuery->msgType = TDMT_VND_DROP_TABLE;
  pQuery->stableQuery = false;
  pQuery->pRoot = NULL;
  code = nodesMakeNode(QUERY_NODE_DROP_TABLE_STMT, &pQuery->pRoot);
  if (TSDB_CODE_SUCCESS != code) goto end;
  RAW_RETURN_CHECK(rewriteToVnodeModifyOpStmt(pQuery, pBufArray));

  launchQueryImpl(pRequest, pQuery, true, NULL);
  if (pRequest->code == TSDB_CODE_SUCCESS) {
    RAW_RETURN_CHECK(removeMeta(pTscObj, pRequest->tableList, false));
  }
  code = pRequest->code;
  uDebug(LOG_ID_TAG " drop table return, msg:%s", LOG_ID_VALUE, tstrerror(code));

end:
  taosHashCleanup(pVgroupHashmap);
  destroyRequest(pRequest);
  tDecoderClear(&coder);
  qDestroyQuery(pQuery);
  RAW_LOG_END
  return code;
}

static int32_t taosDeleteData(TAOS* taos, void* meta, uint32_t metaLen) {
  if (taos == NULL || meta == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  SDeleteRes req = {0};
  SDecoder   coder = {0};
  char       sql[256] = {0};
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    lino = 0;
  uDebug("connId:0x%" PRIx64 " delete data, meta:%p, len:%d", *(int64_t*)taos, meta, metaLen);

  // decode and process req
  void*    data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  uint32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  RAW_RETURN_CHECK(tDecodeDeleteRes(&coder, &req));
  (void)snprintf(sql, sizeof(sql), "delete from `%s` where `%s` >= %" PRId64 " and `%s` <= %" PRId64, req.tableFName,
                 req.tsColName, req.skey, req.tsColName, req.ekey);

  TAOS_RES* res = taosQueryImpl(taos, sql, false, TD_REQ_FROM_TAOX);
  RAW_NULL_CHECK(res);
  SRequestObj* pRequest = (SRequestObj*)res;
  code = pRequest->code;
  if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST || code == TSDB_CODE_PAR_GET_META_ERROR) {
    code = TSDB_CODE_SUCCESS;
  }
  taos_free_result(res);
  uDebug("connId:0x%" PRIx64 " delete data sql:%s, code:%s", *(int64_t*)taos, sql, tstrerror(code));

end:
  RAW_LOG_END
  tDecoderClear(&coder);
  return code;
}

static int32_t taosAlterTable(TAOS* taos, void* meta, uint32_t metaLen) {
  if (taos == NULL || meta == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  SVAlterTbReq   req = {0};
  SDecoder       dcoder = {0};
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SRequestObj*   pRequest = NULL;
  SQuery*        pQuery = NULL;
  SArray*        pArray = NULL;
  SVgDataBlocks* pVgData = NULL;
  SArray*        pVgList = NULL;
  SEncoder       coder = {0};
  SHashObj*      pVgroupHashmap = NULL;

  RAW_RETURN_CHECK(buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0));
  uDebug(LOG_ID_TAG " alter table, meta:%p, len:%d", LOG_ID_VALUE, meta, metaLen);
  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
    uError(LOG_ID_TAG " %s no database selected", LOG_ID_VALUE, __func__);
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void*    data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  uint32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&dcoder, data, len);
  RAW_RETURN_CHECK(tDecodeSVAlterTbReq(&dcoder, &req));
  // do not deal TSDB_ALTER_TABLE_UPDATE_OPTIONS
  if (req.action == TSDB_ALTER_TABLE_UPDATE_OPTIONS) {
    uInfo(LOG_ID_TAG " alter table action is UPDATE_OPTIONS, ignore", LOG_ID_VALUE);
    goto end;
  }

  STscObj*  pTscObj = pRequest->pTscObj;
  SCatalog* pCatalog = NULL;
  RAW_RETURN_CHECK(catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog));
  SRequestConnInfo conn = {.pTrans = pTscObj->pAppInfo->pTransporter,
                           .requestId = pRequest->requestId,
                           .requestObjRefId = pRequest->self,
                           .mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp)};

  // Handle Type 1 batch modification with vnode grouping
  if (req.action == TSDB_ALTER_TABLE_UPDATE_MULTI_TABLE_TAG_VAL) {
    if (req.tables == NULL || taosArrayGetSize(req.tables) == 0) {
      uError(LOG_ID_TAG " Type 1 batch alter has empty tables array", LOG_ID_VALUE);
      code = TSDB_CODE_INVALID_PARA;
      goto end;
    }

    int32_t nTables = taosArrayGetSize(req.tables);
    uDebug(LOG_ID_TAG " Type 1 batch alter with %d tables, grouping by vnode", LOG_ID_VALUE, nTables);

    // Create hashmap to group tables by vgId
    pVgroupHashmap = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
    RAW_NULL_CHECK(pVgroupHashmap);

    // Group tables by vnode
    for (int32_t i = 0; i < nTables; i++) {
      SUpdateTableTagVal* pTable = taosArrayGet(req.tables, i);
      if (pTable == NULL || pTable->tbName == NULL) {
        uWarn(LOG_ID_TAG " Type 1 batch alter table[%d] has invalid name, skip", LOG_ID_VALUE, i);
        continue;
      }

      // Query vnode for this table
      SVgroupInfo vgInfo = {0};
      SName pName = {0};
      toName(pTscObj->acctId, pRequest->pDb, pTable->tbName, &pName);
      code = catalogGetTableHashVgroup(pCatalog, &conn, &pName, &vgInfo);
      if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
        uWarn(LOG_ID_TAG " Type 1 batch alter table %s not found, skip", LOG_ID_VALUE, pTable->tbName);
        code = TSDB_CODE_SUCCESS;
        continue;
      }
      RAW_RETURN_CHECK(code);

      // Add table to corresponding vnode's array
      SArray** ppTables = taosHashGet(pVgroupHashmap, &vgInfo.vgId, sizeof(int32_t));
      if (ppTables == NULL) {
        SArray* pTables = taosArrayInit(16, sizeof(SUpdateTableTagVal));
        RAW_NULL_CHECK(pTables);
        RAW_RETURN_CHECK(taosHashPut(pVgroupHashmap, &vgInfo.vgId, sizeof(int32_t), &pTables, sizeof(void*)));
        ppTables = taosHashGet(pVgroupHashmap, &vgInfo.vgId, sizeof(int32_t));
      }

      SArray* pTables = *ppTables;
      RAW_NULL_CHECK(taosArrayPush(pTables, pTable));
    }

    // Build and send separate request for each vnode
    pArray = taosArrayInit(taosHashGetSize(pVgroupHashmap), sizeof(void*));
    RAW_NULL_CHECK(pArray);

    void* pIter = taosHashIterate(pVgroupHashmap, NULL);
    while (pIter) {
      size_t keyLen = 0;
      int32_t* pVgId = taosHashGetKey(pIter, &keyLen);
      SArray* pTables = *(SArray**)pIter;
      int32_t nTablesInVg = taosArrayGetSize(pTables);

      uDebug(LOG_ID_TAG " Type 1 batch alter: vgId:%d has %d tables", LOG_ID_VALUE, *pVgId, nTablesInVg);

      // Build SVAlterTbReq for this vnode
      SVAlterTbReq vgReq = {0};
      vgReq.action = req.action;
      vgReq.tbName = req.tbName;
      vgReq.source = TD_REQ_FROM_TAOX;
      vgReq.tables = pTables;

      if (strlen(tmqWriteRefDB) > 0) {
        vgReq.refDbName = tmqWriteRefDB;
      }

      // Encode request
      int tlen = 0;
      tEncodeSize(tEncodeSVAlterTbReq, &vgReq, tlen, code);
      if (code < 0) {
        uError(LOG_ID_TAG " Type 1 batch alter encode failed for vgId:%d, code:%s",
               LOG_ID_VALUE, *pVgId, tstrerror(code));
        taosHashCancelIterate(pVgroupHashmap, pIter);
        goto end;
      }

      tlen += sizeof(SMsgHead);
      void* pMsg = taosMemoryMalloc(tlen);
      if (pMsg == NULL) {
        code = terrno;
        uError(LOG_ID_TAG " Type 1 batch alter malloc failed for vgId:%d, size:%d",
               LOG_ID_VALUE, *pVgId, tlen);
        taosHashCancelIterate(pVgroupHashmap, pIter);
        goto end;
      }

      ((SMsgHead*)pMsg)->vgId = htonl(*pVgId);
      ((SMsgHead*)pMsg)->contLen = htonl(tlen);
      void* pBuf = POINTER_SHIFT(pMsg, sizeof(SMsgHead));

      SEncoder vgCoder = {0};
      tEncoderInit(&vgCoder, pBuf, tlen - sizeof(SMsgHead));
      code = tEncodeSVAlterTbReq(&vgCoder, &vgReq);
      tEncoderClear(&vgCoder);

      if (code < 0) {
        uError(LOG_ID_TAG " Type 1 batch alter encode2 failed for vgId:%d, code:%s",
               LOG_ID_VALUE, *pVgId, tstrerror(code));
        taosMemoryFree(pMsg);
        taosHashCancelIterate(pVgroupHashmap, pIter);
        goto end;
      }

      // Create VgDataBlocks for this vnode
      pVgData = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
      if (pVgData == NULL) {
        code = terrno;
        taosMemoryFree(pMsg);
        taosHashCancelIterate(pVgroupHashmap, pIter);
        goto end;
      }

      // Query vgroup info for first table to get endpoint
      SUpdateTableTagVal* pFirstTable = taosArrayGet(pTables, 0);
      SVgroupInfo vgInfo = {0};
      SName pName = {0};
      toName(pTscObj->acctId, pRequest->pDb, pFirstTable->tbName, &pName);
      code = catalogGetTableHashVgroup(pCatalog, &conn, &pName, &vgInfo);
      if (code != TSDB_CODE_SUCCESS) {
        taosMemoryFree(pMsg);
        taosHashCancelIterate(pVgroupHashmap, pIter);
        goto end;
      }

      pVgData->vg = vgInfo;
      pVgData->pData = pMsg;
      pVgData->size = tlen;
      pVgData->numOfTables = nTablesInVg;

      if (taosArrayPush(pArray, &pVgData) == NULL) {
        code = terrno;
        taosMemoryFree(pMsg);
        taosHashCancelIterate(pVgroupHashmap, pIter);
        goto end;
      }

      pVgData = NULL;  // Ownership transferred to pArray
      pIter = taosHashIterate(pVgroupHashmap, pIter);
    }

    uInfo(LOG_ID_TAG " Type 1 batch alter: grouped %d tables into %d vnodes",
          LOG_ID_VALUE, nTables, (int32_t)taosArrayGetSize(pArray));
  } else if (req.action == TSDB_ALTER_TABLE_UPDATE_CHILD_TABLE_TAG_VAL) {
    char dbFName[TSDB_DB_FNAME_LEN] = {0};
    SName pName = {TSDB_TABLE_NAME_T, pTscObj->acctId, {0}, {0}};
    tstrncpy(pName.dbname, pRequest->pDb, sizeof(pName.dbname));
    (void)tNameGetFullDbName(&pName, dbFName);
    RAW_RETURN_CHECK(catalogGetDBVgList(pCatalog, &conn, dbFName, &pVgList));

    pArray = taosArrayInit(taosArrayGetSize(pVgList), sizeof(void*));
    RAW_NULL_CHECK(pArray);
    for (int i = 0; i < taosArrayGetSize(pVgList); ++i) {
      SVgroupInfo* pInfo = (SVgroupInfo*)taosArrayGet(pVgList, i);
      pVgData = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
      RAW_NULL_CHECK(pVgData);
      pVgData->vg = *pInfo;

      int tlen = 0;
      req.source = TD_REQ_FROM_TAOX;

      if (strlen(tmqWriteRefDB) > 0) {
        req.refDbName = tmqWriteRefDB;
      }

      if (req.action == TSDB_ALTER_TABLE_ALTER_COLUMN_REF && tmqWriteCheckRef) {
        RAW_RETURN_CHECK(checkColRefForAlter(pCatalog, &conn, pTscObj->acctId, req.refDbName, req.refTbName, req.refColName,
          pRequest->pDb, req.tbName, req.colName));
      }else if (req.action == TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COLUMN_REF && tmqWriteCheckRef) {
        RAW_RETURN_CHECK(checkColRefForAdd(pCatalog, &conn, pTscObj->acctId, req.refDbName, req.refTbName, req.refColName,
          pRequest->pDb, req.tbName, req.colName, req.type, req.bytes));
      }

      tEncodeSize(tEncodeSVAlterTbReq, &req, tlen, code);
      RAW_RETURN_CHECK(code);
      tlen += sizeof(SMsgHead);
      void* pMsg = taosMemoryMalloc(tlen);
      RAW_NULL_CHECK(pMsg);
      ((SMsgHead*)pMsg)->vgId = htonl(pInfo->vgId);
      ((SMsgHead*)pMsg)->contLen = htonl(tlen);
      void* pBuf = POINTER_SHIFT(pMsg, sizeof(SMsgHead));
      tEncoderInit(&coder, pBuf, tlen - sizeof(SMsgHead));
      RAW_RETURN_CHECK(tEncodeSVAlterTbReq(&coder, &req));
      tEncoderClear(&coder);

      pVgData->pData = pMsg;
      pVgData->size = tlen;

      pVgData->numOfTables = 1;
      RAW_NULL_CHECK(taosArrayPush(pArray, &pVgData));
      pVgData = NULL;  // Ownership transferred to pArray
    }
  } else {
    // Single table or Type 2 modification - original logic
    SVgroupInfo pInfo = {0};
    SName       pName = {0};
    toName(pTscObj->acctId, pRequest->pDb, req.tbName, &pName);
    RAW_RETURN_CHECK(catalogGetTableHashVgroup(pCatalog, &conn, &pName, &pInfo));
    pArray = taosArrayInit(1, sizeof(void*));
    RAW_NULL_CHECK(pArray);

    pVgData = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
    RAW_NULL_CHECK(pVgData);
    pVgData->vg = pInfo;

    int tlen = 0;
    req.source = TD_REQ_FROM_TAOX;

    if (strlen(tmqWriteRefDB) > 0) {
      req.refDbName = tmqWriteRefDB;
    }

    if (req.action == TSDB_ALTER_TABLE_ALTER_COLUMN_REF && tmqWriteCheckRef) {
      RAW_RETURN_CHECK(checkColRefForAlter(pCatalog, &conn, pTscObj->acctId, req.refDbName, req.refTbName, req.refColName,
        pRequest->pDb, req.tbName, req.colName));
    }else if (req.action == TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COLUMN_REF && tmqWriteCheckRef) {
      RAW_RETURN_CHECK(checkColRefForAdd(pCatalog, &conn, pTscObj->acctId, req.refDbName, req.refTbName, req.refColName,
        pRequest->pDb, req.tbName, req.colName, req.type, req.bytes));
    }

    tEncodeSize(tEncodeSVAlterTbReq, &req, tlen, code);
    RAW_RETURN_CHECK(code);
    tlen += sizeof(SMsgHead);
    void* pMsg = taosMemoryMalloc(tlen);
    RAW_NULL_CHECK(pMsg);
    ((SMsgHead*)pMsg)->vgId = htonl(pInfo.vgId);
    ((SMsgHead*)pMsg)->contLen = htonl(tlen);
    void* pBuf = POINTER_SHIFT(pMsg, sizeof(SMsgHead));
    tEncoderInit(&coder, pBuf, tlen - sizeof(SMsgHead));
    RAW_RETURN_CHECK(tEncodeSVAlterTbReq(&coder, &req));

    pVgData->pData = pMsg;
    pVgData->size = tlen;

    pVgData->numOfTables = 1;
    RAW_NULL_CHECK(taosArrayPush(pArray, &pVgData));
    pVgData = NULL;
  }

  pQuery = NULL;
  RAW_RETURN_CHECK(nodesMakeNode(QUERY_NODE_QUERY, (SNode**)&pQuery));
  if (NULL == pQuery) goto end;
  pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  pQuery->msgType = TDMT_VND_ALTER_TABLE;
  pQuery->stableQuery = false;
  pQuery->pRoot = NULL;
  RAW_RETURN_CHECK(nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT, &pQuery->pRoot));
  RAW_RETURN_CHECK(rewriteToVnodeModifyOpStmt(pQuery, pArray));

  launchQueryImpl(pRequest, pQuery, true, NULL);
  pArray = NULL;

  code = pRequest->code;
  if (code == TSDB_CODE_TDB_TABLE_NOT_EXIST || code == TSDB_CODE_NOT_FOUND) {
    code = TSDB_CODE_SUCCESS;
  }

  if (pRequest->code == TSDB_CODE_SUCCESS) {
    SExecResult* pRes = &pRequest->body.resInfo.execRes;
    if (pRes->res != NULL) {
      code = handleAlterTbExecRes(pRes->res, pCatalog);
    }
  }
  uDebug(LOG_ID_TAG " alter table return, meta:%p, len:%d, msg:%s", LOG_ID_VALUE, meta, metaLen, tstrerror(code));

end:
  // Cleanup vnode grouping hashmap
  if (pVgroupHashmap != NULL) {
    void* pIter = taosHashIterate(pVgroupHashmap, NULL);
    while (pIter) {
      SArray* pTables = *(SArray**)pIter;
      taosArrayDestroy(pTables);
      pIter = taosHashIterate(pVgroupHashmap, pIter);
    }
    taosHashCleanup(pVgroupHashmap);
  }

  for (int i = 0; i < taosArrayGetSize(pArray); ++i) {
    SVgDataBlocks* pData = (SVgDataBlocks*)taosArrayGetP(pArray, i);
    if (pData && pData->pData) {
      taosMemoryFreeClear(pData->pData);
      taosMemoryFreeClear(pData);
    }
  }
  taosArrayDestroy(pArray);
  
  if (pVgData) {
    taosMemoryFreeClear(pVgData->pData);
    taosMemoryFreeClear(pVgData);
  }
  taosArrayDestroy(pVgList);
  destroyRequest(pRequest);
  tDecoderClear(&dcoder);
  qDestroyQuery(pQuery);
  destroyAlterTbReq(&req);
  tEncoderClear(&coder);
  RAW_LOG_END
  return code;
}

int taos_write_raw_block_with_fields(TAOS* taos, int rows, char* pData, const char* tbname, TAOS_FIELD* fields,
                                     int numFields) {
  return taos_write_raw_block_with_fields_with_reqid(taos, rows, pData, tbname, fields, numFields, 0);
}

int taos_write_raw_block_with_fields_with_reqid(TAOS* taos, int rows, char* pData, const char* tbname,
                                                TAOS_FIELD* fields, int numFields, int64_t reqid) {
  if (taos == NULL || pData == NULL || tbname == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  STableMeta* pTableMeta = NULL;
  SQuery*     pQuery = NULL;
  SHashObj*   pVgHash = NULL;

  SRequestObj* pRequest = NULL;
  RAW_RETURN_CHECK(buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, reqid));

  uDebug(LOG_ID_TAG " write raw block with field, rows:%d, pData:%p, tbname:%s, fields:%p, numFields:%d", LOG_ID_VALUE,
         rows, pData, tbname, fields, numFields);

  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
    uError(LOG_ID_TAG " %s no database selected", LOG_ID_VALUE, __func__);
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }

  SName pName = {TSDB_TABLE_NAME_T, pRequest->pTscObj->acctId, {0}, {0}};
  tstrncpy(pName.dbname, pRequest->pDb, sizeof(pName.dbname));
  tstrncpy(pName.tname, tbname, sizeof(pName.tname));

  struct SCatalog* pCatalog = NULL;
  RAW_RETURN_CHECK(catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog));

  SRequestConnInfo conn = {0};
  conn.pTrans = pRequest->pTscObj->pAppInfo->pTransporter;
  conn.requestId = pRequest->requestId;
  conn.requestObjRefId = pRequest->self;
  conn.mgmtEps = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp);

  SVgroupInfo vgData = {0};
  RAW_RETURN_CHECK(catalogGetTableHashVgroup(pCatalog, &conn, &pName, &vgData));
  RAW_RETURN_CHECK(catalogGetTableMeta(pCatalog, &conn, &pName, &pTableMeta));
  RAW_RETURN_CHECK(smlInitHandle(&pQuery));
  pVgHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  RAW_NULL_CHECK(pVgHash);
  RAW_RETURN_CHECK(
      taosHashPut(pVgHash, (const char*)&vgData.vgId, sizeof(vgData.vgId), (char*)&vgData, sizeof(vgData)));
  RAW_RETURN_CHECK(rawBlockBindData(pQuery, pTableMeta, pData, NULL, fields, numFields, false, NULL, 0, false));
  RAW_RETURN_CHECK(smlBuildOutput(pQuery, pVgHash));

  launchQueryImpl(pRequest, pQuery, true, NULL);
  code = pRequest->code;
  uDebug(LOG_ID_TAG " write raw block with field return, msg:%s", LOG_ID_VALUE, tstrerror(code));

end:
  taosMemoryFreeClear(pTableMeta);
  qDestroyQuery(pQuery);
  destroyRequest(pRequest);
  taosHashCleanup(pVgHash);
  RAW_LOG_END
  return code;
}

int taos_write_raw_block(TAOS* taos, int rows, char* pData, const char* tbname) {
  return taos_write_raw_block_with_reqid(taos, rows, pData, tbname, 0);
}

int taos_write_raw_block_with_reqid(TAOS* taos, int rows, char* pData, const char* tbname, int64_t reqid) {
  if (taos == NULL || pData == NULL || tbname == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  STableMeta* pTableMeta = NULL;
  SQuery*     pQuery = NULL;
  SHashObj*   pVgHash = NULL;

  SRequestObj* pRequest = NULL;
  RAW_RETURN_CHECK(buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, reqid));

  uDebug(LOG_ID_TAG " write raw block, rows:%d, pData:%p, tbname:%s", LOG_ID_VALUE, rows, pData, tbname);

  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
    uError(LOG_ID_TAG " %s no database selected", LOG_ID_VALUE, __func__);
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }

  SName pName = {TSDB_TABLE_NAME_T, pRequest->pTscObj->acctId, {0}, {0}};
  tstrncpy(pName.dbname, pRequest->pDb, sizeof(pName.dbname));
  tstrncpy(pName.tname, tbname, sizeof(pName.tname));

  struct SCatalog* pCatalog = NULL;
  RAW_RETURN_CHECK(catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog));

  SRequestConnInfo conn = {0};
  conn.pTrans = pRequest->pTscObj->pAppInfo->pTransporter;
  conn.requestId = pRequest->requestId;
  conn.requestObjRefId = pRequest->self;
  conn.mgmtEps = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp);

  SVgroupInfo vgData = {0};
  RAW_RETURN_CHECK(catalogGetTableHashVgroup(pCatalog, &conn, &pName, &vgData));
  RAW_RETURN_CHECK(catalogGetTableMeta(pCatalog, &conn, &pName, &pTableMeta));
  RAW_RETURN_CHECK(smlInitHandle(&pQuery));
  pVgHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  RAW_NULL_CHECK(pVgHash);
  RAW_RETURN_CHECK(
      taosHashPut(pVgHash, (const char*)&vgData.vgId, sizeof(vgData.vgId), (char*)&vgData, sizeof(vgData)));
  RAW_RETURN_CHECK(rawBlockBindData(pQuery, pTableMeta, pData, NULL, NULL, 0, false, NULL, 0, false));
  RAW_RETURN_CHECK(smlBuildOutput(pQuery, pVgHash));

  launchQueryImpl(pRequest, pQuery, true, NULL);
  code = pRequest->code;
  uDebug(LOG_ID_TAG " write raw block return, msg:%s", LOG_ID_VALUE, tstrerror(code));

end:
  taosMemoryFreeClear(pTableMeta);
  qDestroyQuery(pQuery);
  destroyRequest(pRequest);
  taosHashCleanup(pVgHash);
  RAW_LOG_END
  return code;
}

static void* getRawDataFromRes(void* pRetrieve) {
  if (pRetrieve == NULL) {
    uError("invalid parameter in %s", __func__);
    return NULL;
  }
  void* rawData = NULL;
  // deal with compatibility
  if (*(int64_t*)pRetrieve == RETRIEVE_TABLE_RSP_VERSION) {
    rawData = ((SRetrieveTableRsp*)pRetrieve)->data;
  } else if (*(int64_t*)pRetrieve == RETRIEVE_TABLE_RSP_TMQ_VERSION ||
             *(int64_t*)pRetrieve == RETRIEVE_TABLE_RSP_TMQ_RAW_VERSION) {
    rawData = ((SRetrieveTableRspForTmq*)pRetrieve)->data;
  }
  return rawData;
}

static int32_t buildCreateTbMap(SMqDataRsp* rsp, SHashObj* pHashObj) {
  if (rsp == NULL || pHashObj == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  // find schema data info
  int32_t       code = 0;
  int32_t       lino = 0;
  SVCreateTbReq pCreateReq = {0};
  SDecoder      decoderTmp = {0};
  RAW_LOG_START
  for (int j = 0; j < rsp->createTableNum; j++) {
    void** dataTmp = taosArrayGet(rsp->createTableReq, j);
    RAW_NULL_CHECK(dataTmp);
    int32_t* lenTmp = taosArrayGet(rsp->createTableLen, j);
    RAW_NULL_CHECK(lenTmp);

    tDecoderInit(&decoderTmp, *dataTmp, *lenTmp);
    RAW_RETURN_CHECK(tDecodeSVCreateTbReq(&decoderTmp, &pCreateReq));

    if (pCreateReq.type != TSDB_CHILD_TABLE) {
      uError("invalid table type %d in %s", pCreateReq.type, __func__);
      code = TSDB_CODE_INVALID_MSG;
      goto end;
    }
    if (taosHashGet(pHashObj, pCreateReq.name, strlen(pCreateReq.name)) == NULL) {
      RAW_RETURN_CHECK(
          taosHashPut(pHashObj, pCreateReq.name, strlen(pCreateReq.name), &pCreateReq, sizeof(SVCreateTbReq)));
    } else {
      tDestroySVCreateTbReq(&pCreateReq, TSDB_MSG_FLG_DECODE);
    }

    tDecoderClear(&decoderTmp);
    pCreateReq = (SVCreateTbReq){0};
  }

end:
  tDecoderClear(&decoderTmp);
  tDestroySVCreateTbReq(&pCreateReq, TSDB_MSG_FLG_DECODE);
  RAW_LOG_END
  return code;
}

typedef enum {
  WRITE_RAW_INIT_START = 0,
  WRITE_RAW_INIT_OK,
  WRITE_RAW_INIT_FAIL,
} WRITE_RAW_INIT_STATUS;

static SHashObj* writeRawCache = NULL;
static int8_t    initFlag = 0;
static int8_t    initedFlag = WRITE_RAW_INIT_START;

typedef struct {
  SHashObj* pVgHash;
  SHashObj* pNameHash;
  SHashObj* pMetaHash;
} rawCacheInfo;

typedef struct {
  SVgroupInfo vgInfo;
  int64_t     uid;
  int64_t     suid;
} tbInfo;

static void tmqFreeMeta(void* data) {
  if (data == NULL) {
    uError("invalid parameter in %s", __func__);
    return;
  }
  STableMeta* pTableMeta = *(STableMeta**)data;
  taosMemoryFree(pTableMeta);
}

static void freeRawCache(void* data) {
  if (data == NULL) {
    uError("invalid parameter in %s", __func__);
    return;
  }
  rawCacheInfo* pRawCache = (rawCacheInfo*)data;
  taosHashCleanup(pRawCache->pMetaHash);
  taosHashCleanup(pRawCache->pNameHash);
  taosHashCleanup(pRawCache->pVgHash);
}

static int32_t initRawCacheHash() {
  if (writeRawCache == NULL) {
    writeRawCache = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    if (writeRawCache == NULL) {
      return terrno;
    }
    taosHashSetFreeFp(writeRawCache, freeRawCache);
  }
  return 0;
}

static bool needRefreshMeta(void* rawData, STableMeta* pTableMeta, SSchemaWrapper* pSW) {
  if (rawData == NULL || pSW == NULL) {
    return false;
  }
  if (pTableMeta == NULL) {
    uError("invalid parameter in %s", __func__);
    return false;
  }
  char* p = (char*)rawData;
  // | version | total length | total rows | blankFill | total columns | flag seg| block group id | column schema | each
  // column length |
  p += sizeof(int32_t);
  p += sizeof(int32_t);
  p += sizeof(int32_t);
  p += sizeof(int32_t);
  p += sizeof(int32_t);
  p += sizeof(uint64_t);
  int8_t* fields = p;

  if (pSW->nCols != pTableMeta->tableInfo.numOfColumns) {
    return true;
  }

  for (int i = 0; i < pSW->nCols; i++) {
    int j = 0;
    for (; j < pTableMeta->tableInfo.numOfColumns; j++) {
      SSchema*    pColSchema = &pTableMeta->schema[j];
      SSchemaExt* pColExtSchema = &pTableMeta->schemaExt[j];
      char*       fieldName = pSW->pSchema[i].name;

      if (strcmp(pColSchema->name, fieldName) == 0) {
        if (checkSchema(pColSchema, pColExtSchema, fields, NULL, 0) != 0) {
          return true;
        }
        break;
      }
    }
    fields += sizeof(int8_t) + sizeof(int32_t);

    if (j == pTableMeta->tableInfo.numOfColumns) return true;
  }
  return false;
}

static int32_t getRawCache(SHashObj** pVgHash, SHashObj** pNameHash, SHashObj** pMetaHash, void* key) {
  if (pVgHash == NULL || pNameHash == NULL || pMetaHash == NULL || key == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = 0;
  int32_t lino = 0;
  RAW_LOG_START
  void* cacheInfo = taosHashGet(writeRawCache, &key, POINTER_BYTES);
  if (cacheInfo == NULL) {
    *pVgHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
    RAW_NULL_CHECK(*pVgHash);
    *pNameHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
    RAW_NULL_CHECK(*pNameHash);
    *pMetaHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
    RAW_NULL_CHECK(*pMetaHash);
    taosHashSetFreeFp(*pMetaHash, tmqFreeMeta);
    rawCacheInfo info = {*pVgHash, *pNameHash, *pMetaHash};
    RAW_RETURN_CHECK(taosHashPut(writeRawCache, &key, POINTER_BYTES, &info, sizeof(rawCacheInfo)));
  } else {
    rawCacheInfo* info = (rawCacheInfo*)cacheInfo;
    *pVgHash = info->pVgHash;
    *pNameHash = info->pNameHash;
    *pMetaHash = info->pMetaHash;
  }

end:
  if (code != 0) {
    taosHashCleanup(*pMetaHash);
    taosHashCleanup(*pNameHash);
    taosHashCleanup(*pVgHash);
  }
  RAW_LOG_END
  return code;
}

static int32_t buildRawRequest(TAOS* taos, SRequestObj** pRequest, SCatalog** pCatalog, SRequestConnInfo* conn) {
  if (taos == NULL || pRequest == NULL || pCatalog == NULL || conn == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = 0;
  int32_t lino = 0;
  RAW_LOG_START
  RAW_RETURN_CHECK(buildRequest(*(int64_t*)taos, "", 0, NULL, false, pRequest, 0));
  (*pRequest)->syncQuery = true;
  if (!(*pRequest)->pDb) {
    uError("%s no database selected", __func__);
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }

  RAW_RETURN_CHECK(catalogGetHandle((*pRequest)->pTscObj->pAppInfo->clusterId, pCatalog));
  conn->pTrans = (*pRequest)->pTscObj->pAppInfo->pTransporter;
  conn->requestId = (*pRequest)->requestId;
  conn->requestObjRefId = (*pRequest)->self;
  conn->mgmtEps = getEpSet_s(&(*pRequest)->pTscObj->pAppInfo->mgmtEp);

end:
  RAW_LOG_END
  return code;
}

typedef int32_t _raw_decode_func_(SDecoder* pDecoder, SMqDataRsp* pRsp);
static int32_t  decodeRawData(SDecoder* decoder, void* data, uint32_t dataLen, _raw_decode_func_ func,
                              SMqRspObj* rspObj) {
  if (decoder == NULL || data == NULL || func == NULL || rspObj == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  int8_t dataVersion = *(int8_t*)data;
  if (dataVersion >= MQ_DATA_RSP_VERSION) {
    data = POINTER_SHIFT(data, sizeof(int8_t) + sizeof(int32_t));
    if (dataLen < sizeof(int8_t) + sizeof(int32_t)) {
      return TSDB_CODE_INVALID_PARA;
    }
    dataLen -= sizeof(int8_t) + sizeof(int32_t);
  }

  rspObj->resIter = -1;
  tDecoderInit(decoder, data, dataLen);
  int32_t code = func(decoder, &rspObj->dataRsp);
  if (code != 0) {
    SET_ERROR_MSG("decode mq taosx data rsp failed");
  }
  return code;
}

static int32_t processCacheMeta(SHashObj* pVgHash, SHashObj* pNameHash, SHashObj* pMetaHash,
                                SVCreateTbReq* pCreateReqDst, SCatalog* pCatalog, SRequestConnInfo* conn, SName* pName,
                                STableMeta** pMeta, SSchemaWrapper* pSW, void* rawData, int32_t retry) {
  if (pVgHash == NULL || pNameHash == NULL || pMetaHash == NULL || pCatalog == NULL || conn == NULL || pName == NULL ||
      pMeta == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = 0;
  int32_t lino = 0;
  RAW_LOG_START
  STableMeta* pTableMeta = NULL;
  tbInfo*     tmpInfo = (tbInfo*)taosHashGet(pNameHash, pName->tname, strlen(pName->tname));
  if (tmpInfo == NULL || retry > 0) {
    tbInfo info = {0};

    RAW_RETURN_CHECK(catalogGetTableHashVgroup(pCatalog, conn, pName, &info.vgInfo));
    if (pCreateReqDst && tmpInfo == NULL) {  // change stable name to get meta
      tstrncpy(pName->tname, pCreateReqDst->ctb.stbName, TSDB_TABLE_NAME_LEN);
    }
    RAW_RETURN_CHECK(catalogGetTableMeta(pCatalog, conn, pName, &pTableMeta));
    info.uid = pTableMeta->uid;
    if (pTableMeta->tableType == TSDB_CHILD_TABLE) {
      info.suid = pTableMeta->suid;
    } else {
      info.suid = pTableMeta->uid;
    }
    code = taosHashPut(pMetaHash, &info.suid, LONG_BYTES, &pTableMeta, POINTER_BYTES);
    RAW_RETURN_CHECK(code);

    uDebug("put table meta to hash1, suid:%" PRId64 ", metaHashSIze:%d, nameHashSize:%d, vgHashSize:%d", info.suid,
           taosHashGetSize(pMetaHash), taosHashGetSize(pNameHash), taosHashGetSize(pVgHash));
    if (pCreateReqDst) {
      pTableMeta->vgId = info.vgInfo.vgId;
      pTableMeta->uid = pCreateReqDst->uid;
      pCreateReqDst->ctb.suid = pTableMeta->suid;
    }

    RAW_RETURN_CHECK(taosHashPut(pNameHash, pName->tname, strlen(pName->tname), &info, sizeof(tbInfo)));
    tmpInfo = (tbInfo*)taosHashGet(pNameHash, pName->tname, strlen(pName->tname));
    RAW_RETURN_CHECK(
        taosHashPut(pVgHash, &info.vgInfo.vgId, sizeof(info.vgInfo.vgId), &info.vgInfo, sizeof(SVgroupInfo)));
  }

  if (pTableMeta == NULL || retry > 0) {
    STableMeta** pTableMetaTmp = (STableMeta**)taosHashGet(pMetaHash, &tmpInfo->suid, LONG_BYTES);
    if (pTableMetaTmp == NULL || retry > 0 || needRefreshMeta(rawData, *pTableMetaTmp, pSW)) {
      RAW_RETURN_CHECK(catalogGetTableMeta(pCatalog, conn, pName, &pTableMeta));
      code = taosHashPut(pMetaHash, &tmpInfo->suid, LONG_BYTES, &pTableMeta, POINTER_BYTES);
      RAW_RETURN_CHECK(code);
      uDebug("put table meta to hash2, suid:%" PRId64 ", metaHashSIze:%d, nameHashSize:%d, vgHashSize:%d",
             tmpInfo->suid, taosHashGetSize(pMetaHash), taosHashGetSize(pNameHash), taosHashGetSize(pVgHash));
    } else {
      pTableMeta = *pTableMetaTmp;
      pTableMeta->uid = tmpInfo->uid;
      pTableMeta->vgId = tmpInfo->vgInfo.vgId;
    }
  }
  *pMeta = pTableMeta;
  pTableMeta = NULL;

end:
  taosMemoryFree(pTableMeta);
  RAW_LOG_END
  return code;
}

static int32_t tmqWriteRawDataImpl(TAOS* taos, void* data, uint32_t dataLen) {
  if (taos == NULL || data == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   lino = 0;
  SQuery*   pQuery = NULL;
  SMqRspObj rspObj = {0};
  SDecoder  decoder = {0};

  SRequestObj*     pRequest = NULL;
  SCatalog*        pCatalog = NULL;
  SRequestConnInfo conn = {0};
  RAW_RETURN_CHECK(buildRawRequest(taos, &pRequest, &pCatalog, &conn));
  uDebug(LOG_ID_TAG " write raw data, data:%p, dataLen:%d", LOG_ID_VALUE, data, dataLen);
  RAW_RETURN_CHECK(decodeRawData(&decoder, data, dataLen, tDecodeMqDataRsp, &rspObj));

  SHashObj* pVgHash = NULL;
  SHashObj* pNameHash = NULL;
  SHashObj* pMetaHash = NULL;
  RAW_RETURN_CHECK(getRawCache(&pVgHash, &pNameHash, &pMetaHash, taos));
  int retry = 0;
  while (1) {
    RAW_RETURN_CHECK(smlInitHandle(&pQuery));
    uDebug(LOG_ID_TAG " write raw meta data block num:%d", LOG_ID_VALUE, rspObj.dataRsp.blockNum);
    while (++rspObj.resIter < rspObj.dataRsp.blockNum) {
      const char* tbName = (const char*)taosArrayGetP(rspObj.dataRsp.blockTbName, rspObj.resIter);
      RAW_NULL_CHECK(tbName);
      SSchemaWrapper* pSW = (SSchemaWrapper*)taosArrayGetP(rspObj.dataRsp.blockSchema, rspObj.resIter);
      RAW_NULL_CHECK(pSW);
      void* pRetrieve = taosArrayGetP(rspObj.dataRsp.blockData, rspObj.resIter);
      RAW_NULL_CHECK(pRetrieve);
      void* rawData = getRawDataFromRes(pRetrieve);
      RAW_NULL_CHECK(rawData);

      uTrace(LOG_ID_TAG " write raw data block tbname:%s", LOG_ID_VALUE, tbName);
      SName pName = {TSDB_TABLE_NAME_T, pRequest->pTscObj->acctId, {0}, {0}};
      tstrncpy(pName.dbname, pRequest->pDb, TSDB_DB_NAME_LEN);
      tstrncpy(pName.tname, tbName, TSDB_TABLE_NAME_LEN);

      STableMeta* pTableMeta = NULL;
      RAW_RETURN_CHECK(processCacheMeta(pVgHash, pNameHash, pMetaHash, NULL, pCatalog, &conn, &pName, &pTableMeta, pSW,
                                        rawData, retry));
      char err[ERR_MSG_LEN] = {0};
      code = rawBlockBindData(pQuery, pTableMeta, rawData, NULL, pSW, pSW->nCols, true, err, ERR_MSG_LEN, true);
      if (code != TSDB_CODE_SUCCESS) {
        SET_ERROR_MSG("table:%s, err:%s", pName.tname, err);
        goto end;
      }
    }
    RAW_RETURN_CHECK(smlBuildOutput(pQuery, pVgHash));
    launchQueryImpl(pRequest, pQuery, true, NULL);
    code = pRequest->code;

    if (NEED_CLIENT_HANDLE_ERROR(code) && retry++ < 3) {
      uInfo("write raw retry:%d/3 end code:%d, msg:%s", retry, code, tstrerror(code));
      qDestroyQuery(pQuery);
      pQuery = NULL;
      rspObj.resIter = -1;
      continue;
    }
    break;
  }
  uDebug(LOG_ID_TAG " write raw data return, msg:%s", LOG_ID_VALUE, tstrerror(code));

end:
  tDeleteMqDataRsp(&rspObj.dataRsp);
  tDecoderClear(&decoder);
  qDestroyQuery(pQuery);
  destroyRequest(pRequest);
  RAW_LOG_END
  return code;
}

static int32_t tmqWriteRawMetaDataImpl(TAOS* taos, void* data, uint32_t dataLen) {
  if (taos == NULL || data == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   lino = 0;
  SQuery*   pQuery = NULL;
  SMqRspObj rspObj = {0};
  SDecoder  decoder = {0};
  SHashObj* pCreateTbHash = NULL;

  SRequestObj*     pRequest = NULL;
  SCatalog*        pCatalog = NULL;
  SRequestConnInfo conn = {0};

  RAW_RETURN_CHECK(buildRawRequest(taos, &pRequest, &pCatalog, &conn));
  uDebug(LOG_ID_TAG " write raw metadata, data:%p, dataLen:%d", LOG_ID_VALUE, data, dataLen);
  RAW_RETURN_CHECK(decodeRawData(&decoder, data, dataLen, tDecodeSTaosxRsp, &rspObj));

  pCreateTbHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  RAW_NULL_CHECK(pCreateTbHash);
  RAW_RETURN_CHECK(buildCreateTbMap(&rspObj.dataRsp, pCreateTbHash));

  SHashObj* pVgHash = NULL;
  SHashObj* pNameHash = NULL;
  SHashObj* pMetaHash = NULL;
  RAW_RETURN_CHECK(getRawCache(&pVgHash, &pNameHash, &pMetaHash, taos));
  int retry = 0;
  while (1) {
    RAW_RETURN_CHECK(smlInitHandle(&pQuery));
    uDebug(LOG_ID_TAG " write raw meta data block num:%d", LOG_ID_VALUE, rspObj.dataRsp.blockNum);
    while (++rspObj.resIter < rspObj.dataRsp.blockNum) {
      const char* tbName = (const char*)taosArrayGetP(rspObj.dataRsp.blockTbName, rspObj.resIter);
      RAW_NULL_CHECK(tbName);
      SSchemaWrapper* pSW = (SSchemaWrapper*)taosArrayGetP(rspObj.dataRsp.blockSchema, rspObj.resIter);
      RAW_NULL_CHECK(pSW);
      void* pRetrieve = taosArrayGetP(rspObj.dataRsp.blockData, rspObj.resIter);
      RAW_NULL_CHECK(pRetrieve);
      void* rawData = getRawDataFromRes(pRetrieve);
      RAW_NULL_CHECK(rawData);

      uTrace(LOG_ID_TAG " write raw data block tbname:%s", LOG_ID_VALUE, tbName);
      SName pName = {TSDB_TABLE_NAME_T, pRequest->pTscObj->acctId, {0}, {0}};
      tstrncpy(pName.dbname, pRequest->pDb, TSDB_DB_NAME_LEN);
      tstrncpy(pName.tname, tbName, TSDB_TABLE_NAME_LEN);

      // find schema data info
      SVCreateTbReq* pCreateReqDst = (SVCreateTbReq*)taosHashGet(pCreateTbHash, pName.tname, strlen(pName.tname));
      STableMeta*    pTableMeta = NULL;
      RAW_RETURN_CHECK(processCacheMeta(pVgHash, pNameHash, pMetaHash, pCreateReqDst, pCatalog, &conn, &pName,
                                        &pTableMeta, pSW, rawData, retry));
      char err[ERR_MSG_LEN] = {0};
      code =
          rawBlockBindData(pQuery, pTableMeta, rawData, pCreateReqDst, pSW, pSW->nCols, true, err, ERR_MSG_LEN, true);
      if (code != TSDB_CODE_SUCCESS) {
        SET_ERROR_MSG("table:%s, err:%s", pName.tname, err);
        goto end;
      }
    }
    RAW_RETURN_CHECK(smlBuildOutput(pQuery, pVgHash));
    launchQueryImpl(pRequest, pQuery, true, NULL);
    code = pRequest->code;

    if (NEED_CLIENT_HANDLE_ERROR(code) && retry++ < 3) {
      uInfo("write raw retry:%d/3 end code:%d, msg:%s", retry, code, tstrerror(code));
      qDestroyQuery(pQuery);
      pQuery = NULL;
      rspObj.resIter = -1;
      continue;
    }
    break;
  }
  uDebug(LOG_ID_TAG " write raw metadata return, msg:%s", LOG_ID_VALUE, tstrerror(code));

end:
  tDeleteSTaosxRsp(&rspObj.dataRsp);
  void* pIter = taosHashIterate(pCreateTbHash, NULL);
  while (pIter) {
    tDestroySVCreateTbReq(pIter, TSDB_MSG_FLG_DECODE);
    pIter = taosHashIterate(pCreateTbHash, pIter);
  }
  taosHashCleanup(pCreateTbHash);
  tDecoderClear(&decoder);
  qDestroyQuery(pQuery);
  destroyRequest(pRequest);
  RAW_LOG_END
  return code;
}

static int32_t tmqWriteRawRawDataImpl(TAOS* taos, void* data, uint32_t dataLen) {
  if (taos == NULL || data == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   lino = 0;
  SQuery*   pQuery = NULL;
  SHashObj* pVgroupHash = NULL;
  SMqRspObj rspObj = {0};
  SDecoder  decoder = {0};

  SRequestObj*     pRequest = NULL;
  SCatalog*        pCatalog = NULL;
  SRequestConnInfo conn = {0};

  RAW_RETURN_CHECK(buildRawRequest(taos, &pRequest, &pCatalog, &conn));
  uDebug(LOG_ID_TAG " write raw rawdata, data:%p, dataLen:%d", LOG_ID_VALUE, data, dataLen);
  RAW_RETURN_CHECK(decodeRawData(&decoder, data, dataLen, tDecodeMqDataRsp, &rspObj));

  SHashObj* pVgHash = NULL;
  SHashObj* pNameHash = NULL;
  SHashObj* pMetaHash = NULL;
  RAW_RETURN_CHECK(getRawCache(&pVgHash, &pNameHash, &pMetaHash, taos));
  int retry = 0;
  while (1) {
    RAW_RETURN_CHECK(smlInitHandle(&pQuery));
    uDebug(LOG_ID_TAG " write raw rawdata block num:%d", LOG_ID_VALUE, rspObj.dataRsp.blockNum);
    SVnodeModifyOpStmt* pStmt = (SVnodeModifyOpStmt*)(pQuery)->pRoot;
    pVgroupHash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
    RAW_NULL_CHECK(pVgroupHash);
    pStmt->pVgDataBlocks = taosArrayInit(8, POINTER_BYTES);
    RAW_NULL_CHECK(pStmt->pVgDataBlocks);

    while (++rspObj.resIter < rspObj.dataRsp.blockNum) {
      const char* tbName = (const char*)taosArrayGetP(rspObj.dataRsp.blockTbName, rspObj.resIter);
      RAW_NULL_CHECK(tbName);
      void* pRetrieve = taosArrayGetP(rspObj.dataRsp.blockData, rspObj.resIter);
      RAW_NULL_CHECK(pRetrieve);
      void* rawData = getRawDataFromRes(pRetrieve);
      RAW_NULL_CHECK(rawData);

      uTrace(LOG_ID_TAG " write raw data block tbname:%s", LOG_ID_VALUE, tbName);
      SName pName = {TSDB_TABLE_NAME_T, pRequest->pTscObj->acctId, {0}, {0}};
      tstrncpy(pName.dbname, pRequest->pDb, TSDB_DB_NAME_LEN);
      tstrncpy(pName.tname, tbName, TSDB_TABLE_NAME_LEN);

      // find schema data info
      STableMeta* pTableMeta = NULL;
      RAW_RETURN_CHECK(processCacheMeta(pVgHash, pNameHash, pMetaHash, NULL, pCatalog, &conn, &pName, &pTableMeta, NULL,
                                        NULL, retry));
      char err[ERR_MSG_LEN] = {0};
      code = rawBlockBindRawData(pVgroupHash, pStmt->pVgDataBlocks, pTableMeta, rawData);
      if (code != TSDB_CODE_SUCCESS) {
        SET_ERROR_MSG("table:%s, err:%s", pName.tname, err);
        goto end;
      }
    }
    taosHashCleanup(pVgroupHash);
    pVgroupHash = NULL;

    RAW_RETURN_CHECK(smlBuildOutputRaw(pQuery, pVgHash));
    launchQueryImpl(pRequest, pQuery, true, NULL);
    code = pRequest->code;

    if (NEED_CLIENT_HANDLE_ERROR(code) && retry++ < 3) {
      uInfo("write raw retry:%d/3 end code:%d, msg:%s", retry, code, tstrerror(code));
      qDestroyQuery(pQuery);
      pQuery = NULL;
      rspObj.resIter = -1;
      continue;
    }
    break;
  }
  uDebug(LOG_ID_TAG " write raw rawdata return, msg:%s", LOG_ID_VALUE, tstrerror(code));

end:
  tDeleteMqDataRsp(&rspObj.dataRsp);
  tDecoderClear(&decoder);
  qDestroyQuery(pQuery);
  taosHashCleanup(pVgroupHash);
  destroyRequest(pRequest);
  RAW_LOG_END
  return code;
}

static int32_t processSimpleMeta(SMqMetaRsp* pMetaRsp, cJSON** meta) {
  if (pMetaRsp == NULL || meta == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = 0;
  int32_t lino = 0;
  RAW_LOG_START
  if (pMetaRsp->resMsgType == TDMT_VND_CREATE_STB) {
    RAW_RETURN_CHECK(processCreateStb(pMetaRsp, meta));
  } else if (pMetaRsp->resMsgType == TDMT_VND_ALTER_STB) {
    RAW_RETURN_CHECK(processAlterStb(pMetaRsp, meta));
  } else if (pMetaRsp->resMsgType == TDMT_VND_DROP_STB) {
    RAW_RETURN_CHECK(processDropSTable(pMetaRsp, meta));
  } else if (pMetaRsp->resMsgType == TDMT_VND_CREATE_TABLE) {
    RAW_RETURN_CHECK(processCreateTable(pMetaRsp, meta));
  } else if (pMetaRsp->resMsgType == TDMT_VND_ALTER_TABLE) {
    RAW_RETURN_CHECK(processAlterTable(pMetaRsp, meta));
  } else if (pMetaRsp->resMsgType == TDMT_VND_DROP_TABLE) {
    RAW_RETURN_CHECK(processDropTable(pMetaRsp, meta));
  } else if (pMetaRsp->resMsgType == TDMT_VND_DELETE) {
    RAW_RETURN_CHECK(processDeleteTable(pMetaRsp, meta));
  }

end:
  RAW_LOG_END
  return code;
}

static int32_t processBatchMetaToJson(SMqBatchMetaRsp* pMsgRsp, char** string) {
  if (pMsgRsp == NULL || string == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  SDecoder        coder = {0};
  SMqBatchMetaRsp rsp = {0};
  int32_t         code = 0;
  int32_t         lino = 0;
  cJSON*          pJson = NULL;
  tDecoderInit(&coder, pMsgRsp->pMetaBuff, pMsgRsp->metaBuffLen);
  RAW_RETURN_CHECK(tDecodeMqBatchMetaRsp(&coder, &rsp));

  pJson = cJSON_CreateObject();
  RAW_NULL_CHECK(pJson);
  RAW_FALSE_CHECK(cJSON_AddStringToObject(pJson, "tmq_meta_version", TMQ_META_VERSION));
  cJSON* pMetaArr = cJSON_AddArrayToObject(pJson, "metas");
  RAW_NULL_CHECK(pMetaArr);

  int32_t num = taosArrayGetSize(rsp.batchMetaReq);
  for (int32_t i = 0; i < num; i++) {
    int32_t* len = taosArrayGet(rsp.batchMetaLen, i);
    RAW_NULL_CHECK(len);
    void* tmpBuf = taosArrayGetP(rsp.batchMetaReq, i);
    RAW_NULL_CHECK(tmpBuf);
    SDecoder   metaCoder = {0};
    SMqMetaRsp metaRsp = {0};
    tDecoderInit(&metaCoder, POINTER_SHIFT(tmpBuf, sizeof(SMqRspHead)), *len - sizeof(SMqRspHead));
    RAW_RETURN_CHECK(tDecodeMqMetaRsp(&metaCoder, &metaRsp));
    cJSON* pItem = NULL;
    RAW_RETURN_CHECK(processSimpleMeta(&metaRsp, &pItem));
    tDeleteMqMetaRsp(&metaRsp);
    if (pItem != NULL) RAW_FALSE_CHECK(cJSON_AddItemToArray(pMetaArr, pItem));
  }

  char* fullStr = cJSON_PrintUnformatted(pJson);
  *string = fullStr;

end:
  cJSON_Delete(pJson);
  tDeleteMqBatchMetaRsp(&rsp);
  RAW_LOG_END
  return code;
}

char* tmq_get_json_meta(TAOS_RES* res) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  char*   string = NULL;
  RAW_LOG_START
  RAW_NULL_CHECK(res);
  RAW_FALSE_CHECK(TD_RES_TMQ_META(res) || TD_RES_TMQ_METADATA(res) || TD_RES_TMQ_BATCH_META(res));

  SMqRspObj* rspObj = (SMqRspObj*)res;
  if (TD_RES_TMQ_METADATA(res)) {
    RAW_RETURN_CHECK(processAutoCreateTable(&rspObj->dataRsp, &string));
  } else if (TD_RES_TMQ_BATCH_META(res)) {
    RAW_RETURN_CHECK(processBatchMetaToJson(&rspObj->batchMetaRsp, &string));
  } else if (TD_RES_TMQ_META(res)) {
    cJSON* pJson = NULL;
    RAW_RETURN_CHECK(processSimpleMeta(&rspObj->metaRsp, &pJson));
    string = cJSON_PrintUnformatted(pJson);
    cJSON_Delete(pJson);
  } else {
    uError("tmq_get_json_meta res:%d, invalid type", *(int8_t*)res);
  }

  uDebug("tmq_get_json_meta string:%s", string);

end:
  RAW_LOG_END
  return string;
}

void tmq_free_json_meta(char* jsonMeta) { taosMemoryFreeClear(jsonMeta); }

static int32_t getOffSetLen(const SMqDataRsp* pRsp) {
  if (pRsp == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t pos = 0;
  int32_t code = 0;
  int32_t lino = 0;
  RAW_LOG_START
  SEncoder coder = {0};
  tEncoderInit(&coder, NULL, 0);
  RAW_RETURN_CHECK(tEncodeSTqOffsetVal(&coder, &pRsp->reqOffset));
  RAW_RETURN_CHECK(tEncodeSTqOffsetVal(&coder, &pRsp->rspOffset));
  pos = coder.pos;
  tEncoderClear(&coder);

end:
  if (code != 0) {
    uError("getOffSetLen failed, code:%d", code);
    return code;
  } else {
    uDebug("getOffSetLen success, len:%d", pos);
    return pos;
  }
}

typedef int32_t __encode_func__(SEncoder* pEncoder, const SMqDataRsp* pRsp);
static int32_t  encodeMqDataRsp(__encode_func__* encodeFunc, SMqDataRsp* rspObj, tmq_raw_data* raw) {
  if (raw == NULL || encodeFunc == NULL || rspObj == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  uint32_t len = 0;
  int32_t  code = 0;
  int32_t  lino = 0;
  SEncoder encoder = {0};
  void*    buf = NULL;
  tEncodeSize(encodeFunc, rspObj, len, code);
  RAW_FALSE_CHECK(code >= 0);
  len += sizeof(int8_t) + sizeof(int32_t);
  buf = taosMemoryCalloc(1, len);
  RAW_NULL_CHECK(buf);
  tEncoderInit(&encoder, buf, len);
  RAW_RETURN_CHECK(tEncodeI8(&encoder, MQ_DATA_RSP_VERSION));
  int32_t offsetLen = getOffSetLen(rspObj);
  RAW_FALSE_CHECK(offsetLen > 0);
  RAW_RETURN_CHECK(tEncodeI32(&encoder, offsetLen));
  RAW_RETURN_CHECK(encodeFunc(&encoder, rspObj));

  raw->raw = buf;
  buf = NULL;
  raw->raw_len = len;

end:
  RAW_LOG_END
  return code;
}

int32_t tmq_get_raw(TAOS_RES* res, tmq_raw_data* raw) {
  if (raw == NULL || res == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  RAW_LOG_START
  *raw = (tmq_raw_data){0};
  SMqRspObj* rspObj = ((SMqRspObj*)res);
  if (TD_RES_TMQ_META(res)) {
    raw->raw = rspObj->metaRsp.metaRsp;
    raw->raw_len = rspObj->metaRsp.metaRspLen >= 0 ? rspObj->metaRsp.metaRspLen : 0;
    raw->raw_type = rspObj->metaRsp.resMsgType;
    uDebug("tmq get raw type meta:%p", raw);
  } else if (TD_RES_TMQ(res)) {
    RAW_RETURN_CHECK(encodeMqDataRsp(tEncodeMqDataRsp, &rspObj->dataRsp, raw));
    raw->raw_type = RES_TYPE__TMQ;
    uDebug("tmq get raw type data:%p", raw);
  } else if (TD_RES_TMQ_METADATA(res)) {
    RAW_RETURN_CHECK(encodeMqDataRsp(tEncodeSTaosxRsp, &rspObj->dataRsp, raw));
    raw->raw_type = RES_TYPE__TMQ_METADATA;
    uDebug("tmq get raw type metadata:%p", raw);
  } else if (TD_RES_TMQ_BATCH_META(res)) {
    raw->raw = rspObj->batchMetaRsp.pMetaBuff;
    raw->raw_len = rspObj->batchMetaRsp.metaBuffLen;
    raw->raw_type = rspObj->resType;
    uDebug("tmq get raw batch meta:%p", raw);
  } else if (TD_RES_TMQ_RAW(res)) {
    raw->raw = rspObj->dataRsp.rawData;
    rspObj->dataRsp.rawData = NULL;
    raw->raw_len = rspObj->dataRsp.len;
    raw->raw_type = rspObj->resType;
    uDebug("tmq get raw raw:%p", raw);
  } else {
    uError("tmq get raw error type:%d", *(int8_t*)res);
    code = TSDB_CODE_TMQ_INVALID_MSG;
  }

end:
  RAW_LOG_END
  return code;
}

void tmq_free_raw(tmq_raw_data raw) {
  uDebug("tmq free raw data type:%d", raw.raw_type);
  if (raw.raw_type == RES_TYPE__TMQ || raw.raw_type == RES_TYPE__TMQ_METADATA) {
    taosMemoryFree(raw.raw);
  } else if (raw.raw_type == RES_TYPE__TMQ_RAWDATA && raw.raw != NULL) {
    taosMemoryFree(POINTER_SHIFT(raw.raw, -sizeof(SMqRspHead)));
  }
  (void)memset(terrMsg, 0, ERR_MSG_LEN);
}

static int32_t writeRawInit() {
  while (atomic_load_8(&initedFlag) == WRITE_RAW_INIT_START) {
    int8_t old = atomic_val_compare_exchange_8(&initFlag, 0, 1);
    if (old == 0) {
      int32_t code = initRawCacheHash();
      if (code != 0) {
        uError("tmq writeRawImpl init error:%d", code);
        atomic_store_8(&initedFlag, WRITE_RAW_INIT_FAIL);
        return code;
      }
      atomic_store_8(&initedFlag, WRITE_RAW_INIT_OK);
    }
  }

  if (atomic_load_8(&initedFlag) == WRITE_RAW_INIT_FAIL) {
    return TSDB_CODE_INTERNAL_ERROR;
  }
  return 0;
}

static int32_t writeRawImpl(TAOS* taos, void* buf, uint32_t len, uint16_t type) {
  if (taos == NULL || buf == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  if (writeRawInit() != 0) {
    return TSDB_CODE_INTERNAL_ERROR;
  }

  if (type == TDMT_VND_CREATE_STB) {
    return taosCreateStb(taos, buf, len);
  } else if (type == TDMT_VND_ALTER_STB) {
    return taosCreateStb(taos, buf, len);
  } else if (type == TDMT_VND_DROP_STB) {
    return taosDropStb(taos, buf, len);
  } else if (type == TDMT_VND_CREATE_TABLE) {
    return taosCreateTable(taos, buf, len);
  } else if (type == TDMT_VND_ALTER_TABLE) {
    return taosAlterTable(taos, buf, len);
  } else if (type == TDMT_VND_DROP_TABLE) {
    return taosDropTable(taos, buf, len);
  } else if (type == TDMT_VND_DELETE) {
    return taosDeleteData(taos, buf, len);
  } else if (type == RES_TYPE__TMQ_METADATA) {
    return tmqWriteRawMetaDataImpl(taos, buf, len);
  } else if (type == RES_TYPE__TMQ_RAWDATA) {
    return tmqWriteRawRawDataImpl(taos, buf, len);
  } else if (type == RES_TYPE__TMQ) {
    return tmqWriteRawDataImpl(taos, buf, len);
  } else if (type == RES_TYPE__TMQ_BATCH_META) {
    return tmqWriteBatchMetaDataImpl(taos, buf, len);
  }
  return TSDB_CODE_INVALID_PARA;
}

int32_t tmq_write_raw(TAOS* taos, tmq_raw_data raw) {
  if (taos == NULL || raw.raw == NULL || raw.raw_len <= 0) {
    SET_ERROR_MSG("taos:%p or data:%p is NULL or raw_len <= 0", taos, raw.raw);
    return TSDB_CODE_INVALID_PARA;
  }
  taosClearErrMsg();  // clear global error message

  return writeRawImpl(taos, raw.raw, raw.raw_len, raw.raw_type);
}

static int32_t tmqWriteBatchMetaDataImpl(TAOS* taos, void* meta, uint32_t metaLen) {
  if (taos == NULL || meta == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  SMqBatchMetaRsp rsp = {0};
  SDecoder        coder = {0};
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;

  RAW_LOG_START
  // decode and process req
  tDecoderInit(&coder, meta, metaLen);
  RAW_RETURN_CHECK(tDecodeMqBatchMetaRsp(&coder, &rsp));
  int32_t num = taosArrayGetSize(rsp.batchMetaReq);
  for (int32_t i = 0; i < num; i++) {
    int32_t* len = taosArrayGet(rsp.batchMetaLen, i);
    RAW_NULL_CHECK(len);
    void* tmpBuf = taosArrayGetP(rsp.batchMetaReq, i);
    RAW_NULL_CHECK(tmpBuf);
    SDecoder   metaCoder = {0};
    SMqMetaRsp metaRsp = {0};
    tDecoderInit(&metaCoder, POINTER_SHIFT(tmpBuf, sizeof(SMqRspHead)), *len - sizeof(SMqRspHead));
    RAW_RETURN_CHECK(tDecodeMqMetaRsp(&metaCoder, &metaRsp));
    code = writeRawImpl(taos, metaRsp.metaRsp, metaRsp.metaRspLen, metaRsp.resMsgType);
    tDeleteMqMetaRsp(&metaRsp);
    if (code != TSDB_CODE_SUCCESS) {
      goto end;
    }
  }

end:
  tDeleteMqBatchMetaRsp(&rsp);
  RAW_LOG_END
  return code;
}
