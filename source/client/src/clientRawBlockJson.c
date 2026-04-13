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

// This file contains JSON construction functions for TMQ meta responses.
// Extracted from clientRawBlockWrite.c for better code organization.

#include "cJSON.h"
#include "clientInt.h"
#include "clientRawBlockWrite.h"
#include "nodes.h"
#include "taosdef.h"
#include "tcol.h"
#include "tcompression.h"
#include "tdataformat.h"
#include "tdef.h"

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

static int32_t addUpdatedTagValToJson(cJSON* tags, SUpdatedTagVal* pTagVal) {
  if (tags == NULL || pTagVal == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = 0;
  int32_t lino = 0;
  char*   buf = NULL;

  cJSON* member = tmqAddObjectToArray(tags);
  RAW_NULL_CHECK(member);

  ADD_TO_JSON_STRING(member, "colName", pTagVal->tagName);
  uDebug("%s tagName:%s, tagType:%d, isNull:%d, hasRegexp:%d", __func__,
         pTagVal->tagName, pTagVal->tagType, pTagVal->isNull, pTagVal->regexp != NULL);

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
      buf = taosMemoryCalloc(bufSize, 1);
      RAW_NULL_CHECK(buf);
      code = dataConverToStr(buf, bufSize, pTagVal->tagType, pTagVal->pTagVal, pTagVal->nTagVal, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        uError("%s convert tag value to string failed, tagName:%s, tagType:%d, nTagVal:%d, code:%s",
               __func__, pTagVal->tagName, pTagVal->tagType, pTagVal->nTagVal, tstrerror(code));
        goto end;
      }
      ADD_TO_JSON_STRING(member, "colValue", buf);
    }
    ADD_TO_JSON_BOOL(member, "colValueNull", isNull);
  }

end:
  taosMemoryFree(buf);
  RAW_LOG_END
  return code;
}

static int32_t processAlterTable(SMqMetaRsp* metaRsp, cJSON** pJson) {
  if (pJson == NULL || metaRsp == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  SDecoder     decoder = {0};
  SVAlterTbReq vAlterTbReq = {0};
  cJSON*       json = NULL;
  int32_t      code = 0;
  int32_t      lino = 0;
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
          SUpdatedTagVal* pTagVal = taosArrayGet(pTable->tags, j);
          RAW_RETURN_CHECK(addUpdatedTagValToJson(tags, pTagVal));
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
        SUpdatedTagVal* pTagVal = taosArrayGet(vAlterTbReq.pMultiTag, i);
        RAW_RETURN_CHECK(addUpdatedTagValToJson(tags, pTagVal));
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
  nodesDestroyNode(pWhere);
  destroyAlterTbReq(&vAlterTbReq);
  tDecoderClear(&decoder);
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

static int32_t processSimpleMeta(SMqMetaRsp* pMetaRsp, cJSON** meta) {
  if (pMetaRsp == NULL || meta == NULL) {
    uError("invalid parameter in %s", __func__);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = 0;
  int32_t lino = 0;
  RAW_LOG_START
  uDebug("%s resMsgType:%d", __func__, pMetaRsp->resMsgType);
  switch (pMetaRsp->resMsgType) {
    case TDMT_VND_CREATE_STB:
      RAW_RETURN_CHECK(processCreateStb(pMetaRsp, meta));
      break;
    case TDMT_VND_ALTER_STB:
      RAW_RETURN_CHECK(processAlterStb(pMetaRsp, meta));
      break;
    case TDMT_VND_DROP_STB:
      RAW_RETURN_CHECK(processDropSTable(pMetaRsp, meta));
      break;
    case TDMT_VND_CREATE_TABLE:
      RAW_RETURN_CHECK(processCreateTable(pMetaRsp, meta));
      break;
    case TDMT_VND_ALTER_TABLE:
      RAW_RETURN_CHECK(processAlterTable(pMetaRsp, meta));
      break;
    case TDMT_VND_DROP_TABLE:
      RAW_RETURN_CHECK(processDropTable(pMetaRsp, meta));
      break;
    case TDMT_VND_DELETE:
      RAW_RETURN_CHECK(processDeleteTable(pMetaRsp, meta));
      break;
    default:
      uError("%s unknown resMsgType:%d", __func__, pMetaRsp->resMsgType);
      break;
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
