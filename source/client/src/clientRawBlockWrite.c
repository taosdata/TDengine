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

#include "cJSON.h"
#include "clientInt.h"
#include "parser.h"
#include "tcol.h"
#include "tcompression.h"
#include "tdatablock.h"
#include "tdef.h"
#include "tglobal.h"
#include "tmsgtype.h"

#define RAW_NULL_CHECK(c) \
  do {                    \
    if (c == NULL) {      \
      code = terrno;      \
      goto end;           \
    }                     \
  } while (0)

#define RAW_FALSE_CHECK(c)           \
  do {                               \
    if (!c) {                        \
      code = TSDB_CODE_INVALID_PARA; \
      goto end;                      \
    }                                \
  } while (0)

#define RAW_RETURN_CHECK(c) \
  do {                      \
    code = c;               \
    if (code != 0) {        \
      goto end;             \
    }                       \
  } while (0)

#define LOG_ID_TAG   "connId:0x%" PRIx64 ",QID:0x%" PRIx64
#define LOG_ID_VALUE *(int64_t*)taos, pRequest->requestId

#define TMQ_META_VERSION "1.0"

static int32_t  tmqWriteBatchMetaDataImpl(TAOS* taos, void* meta, int32_t metaLen);
static tb_uid_t processSuid(tb_uid_t suid, char* db) { return suid + MurmurHash3_32(db, strlen(db)); }
static void buildCreateTableJson(SSchemaWrapper* schemaRow, SSchemaWrapper* schemaTag, char* name, int64_t id, int8_t t,
                                 SColCmprWrapper* pColCmprRow, cJSON** pJson) {
  int32_t code = TSDB_CODE_SUCCESS;
  int8_t  buildDefaultCompress = 0;
  if (pColCmprRow->nCols <= 0) {
    buildDefaultCompress = 1;
  }

  char*  string = NULL;
  cJSON* json = cJSON_CreateObject();
  RAW_NULL_CHECK(json);
  cJSON* type = cJSON_CreateString("create");
  RAW_NULL_CHECK(type);

  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "type", type));
  cJSON* tableType = cJSON_CreateString(t == TSDB_NORMAL_TABLE ? "normal" : "super");
  RAW_NULL_CHECK(tableType);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "tableType", tableType));
  cJSON* tableName = cJSON_CreateString(name);
  RAW_NULL_CHECK(tableName);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "tableName", tableName));

  cJSON* columns = cJSON_CreateArray();
  RAW_NULL_CHECK(columns);
  for (int i = 0; i < schemaRow->nCols; i++) {
    cJSON* column = cJSON_CreateObject();
    RAW_NULL_CHECK(column);
    SSchema* s = schemaRow->pSchema + i;
    cJSON*   cname = cJSON_CreateString(s->name);
    RAW_NULL_CHECK(cname);
    RAW_FALSE_CHECK(cJSON_AddItemToObject(column, "name", cname));
    cJSON* ctype = cJSON_CreateNumber(s->type);
    RAW_NULL_CHECK(ctype);
    RAW_FALSE_CHECK(cJSON_AddItemToObject(column, "type", ctype));
    if (s->type == TSDB_DATA_TYPE_BINARY || s->type == TSDB_DATA_TYPE_VARBINARY || s->type == TSDB_DATA_TYPE_GEOMETRY) {
      int32_t length = s->bytes - VARSTR_HEADER_SIZE;
      cJSON*  cbytes = cJSON_CreateNumber(length);
      RAW_NULL_CHECK(cbytes);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(column, "length", cbytes));
    } else if (s->type == TSDB_DATA_TYPE_NCHAR) {
      int32_t length = (s->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
      cJSON*  cbytes = cJSON_CreateNumber(length);
      RAW_NULL_CHECK(cbytes);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(column, "length", cbytes));
    }
    cJSON* isPk = cJSON_CreateBool(s->flags & COL_IS_KEY);
    RAW_NULL_CHECK(isPk);
    RAW_FALSE_CHECK(cJSON_AddItemToObject(column, "isPrimarykey", isPk));
    RAW_FALSE_CHECK(cJSON_AddItemToArray(columns, column));

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

    cJSON* encodeJson = cJSON_CreateString(encode);
    RAW_NULL_CHECK(encodeJson);
    RAW_FALSE_CHECK(cJSON_AddItemToObject(column, "encode", encodeJson));

    cJSON* compressJson = cJSON_CreateString(compress);
    RAW_NULL_CHECK(compressJson);
    RAW_FALSE_CHECK(cJSON_AddItemToObject(column, "compress", compressJson));

    cJSON* levelJson = cJSON_CreateString(level);
    RAW_NULL_CHECK(levelJson);
    RAW_FALSE_CHECK(cJSON_AddItemToObject(column, "level", levelJson));
  }
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "columns", columns));

  cJSON* tags = cJSON_CreateArray();
  RAW_NULL_CHECK(tags);
  for (int i = 0; schemaTag && i < schemaTag->nCols; i++) {
    cJSON* tag = cJSON_CreateObject();
    RAW_NULL_CHECK(tag);
    SSchema* s = schemaTag->pSchema + i;
    cJSON*   tname = cJSON_CreateString(s->name);
    RAW_NULL_CHECK(tname);
    RAW_FALSE_CHECK(cJSON_AddItemToObject(tag, "name", tname));
    cJSON* ttype = cJSON_CreateNumber(s->type);
    RAW_NULL_CHECK(ttype);
    RAW_FALSE_CHECK(cJSON_AddItemToObject(tag, "type", ttype));
    if (s->type == TSDB_DATA_TYPE_BINARY || s->type == TSDB_DATA_TYPE_VARBINARY || s->type == TSDB_DATA_TYPE_GEOMETRY) {
      int32_t length = s->bytes - VARSTR_HEADER_SIZE;
      cJSON*  cbytes = cJSON_CreateNumber(length);
      RAW_NULL_CHECK(cbytes);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(tag, "length", cbytes));
    } else if (s->type == TSDB_DATA_TYPE_NCHAR) {
      int32_t length = (s->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
      cJSON*  cbytes = cJSON_CreateNumber(length);
      RAW_NULL_CHECK(cbytes);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(tag, "length", cbytes));
    }
    RAW_FALSE_CHECK(cJSON_AddItemToArray(tags, tag));
  }
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "tags", tags));

end:
  *pJson = json;
}

static int32_t setCompressOption(cJSON* json, uint32_t para) {
  uint8_t encode = COMPRESS_L1_TYPE_U32(para);
  int32_t code = 0;
  if (encode != 0) {
    const char* encodeStr = columnEncodeStr(encode);
    RAW_NULL_CHECK(encodeStr);
    cJSON* encodeJson = cJSON_CreateString(encodeStr);
    RAW_NULL_CHECK(encodeJson);
    RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "encode", encodeJson));
    return code;
  }
  uint8_t compress = COMPRESS_L2_TYPE_U32(para);
  if (compress != 0) {
    const char* compressStr = columnCompressStr(compress);
    RAW_NULL_CHECK(compressStr);
    cJSON* compressJson = cJSON_CreateString(compressStr);
    RAW_NULL_CHECK(compressJson);
    RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "compress", compressJson));
    return code;
  }
  uint8_t level = COMPRESS_L2_TYPE_LEVEL_U32(para);
  if (level != 0) {
    const char* levelStr = columnLevelStr(level);
    RAW_NULL_CHECK(levelStr);
    cJSON* levelJson = cJSON_CreateString(levelStr);
    RAW_NULL_CHECK(levelJson);
    RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "level", levelJson));
    return code;
  }

end:
  return code;
}
static void buildAlterSTableJson(void* alterData, int32_t alterDataLen, cJSON** pJson) {
  SMAlterStbReq req = {0};
  cJSON*        json = NULL;
  char*         string = NULL;
  int32_t       code = 0;

  if (tDeserializeSMAlterStbReq(alterData, alterDataLen, &req) != 0) {
    goto end;
  }

  json = cJSON_CreateObject();
  RAW_NULL_CHECK(json);
  cJSON* type = cJSON_CreateString("alter");
  RAW_NULL_CHECK(type);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "type", type));
  SName name = {0};
  RAW_RETURN_CHECK(tNameFromString(&name, req.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE));
  cJSON* tableType = cJSON_CreateString("super");
  RAW_NULL_CHECK(tableType);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "tableType", tableType));
  cJSON* tableName = cJSON_CreateString(name.tname);
  RAW_NULL_CHECK(tableName);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "tableName", tableName));

  cJSON* alterType = cJSON_CreateNumber(req.alterType);
  RAW_NULL_CHECK(alterType);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "alterType", alterType));
  switch (req.alterType) {
    case TSDB_ALTER_TABLE_ADD_TAG:
    case TSDB_ALTER_TABLE_ADD_COLUMN: {
      TAOS_FIELD* field = taosArrayGet(req.pFields, 0);
      RAW_NULL_CHECK(field);
      cJSON* colName = cJSON_CreateString(field->name);
      RAW_NULL_CHECK(colName);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colName", colName));
      cJSON* colType = cJSON_CreateNumber(field->type);
      RAW_NULL_CHECK(colType);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colType", colType));

      if (field->type == TSDB_DATA_TYPE_BINARY || field->type == TSDB_DATA_TYPE_VARBINARY ||
          field->type == TSDB_DATA_TYPE_GEOMETRY) {
        int32_t length = field->bytes - VARSTR_HEADER_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        RAW_NULL_CHECK(cbytes);
        RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colLength", cbytes));
      } else if (field->type == TSDB_DATA_TYPE_NCHAR) {
        int32_t length = (field->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        RAW_NULL_CHECK(cbytes);
        RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colLength", cbytes));
      }
      break;
    }
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION: {
      SFieldWithOptions* field = taosArrayGet(req.pFields, 0);
      RAW_NULL_CHECK(field);
      cJSON* colName = cJSON_CreateString(field->name);
      RAW_NULL_CHECK(colName);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colName", colName));
      cJSON* colType = cJSON_CreateNumber(field->type);
      RAW_NULL_CHECK(colType);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colType", colType));

      if (field->type == TSDB_DATA_TYPE_BINARY || field->type == TSDB_DATA_TYPE_VARBINARY ||
          field->type == TSDB_DATA_TYPE_GEOMETRY) {
        int32_t length = field->bytes - VARSTR_HEADER_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        RAW_NULL_CHECK(cbytes);
        RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colLength", cbytes));
      } else if (field->type == TSDB_DATA_TYPE_NCHAR) {
        int32_t length = (field->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        RAW_NULL_CHECK(cbytes);
        RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colLength", cbytes));
      }
      RAW_RETURN_CHECK(setCompressOption(json, field->compress));
      break;
    }
    case TSDB_ALTER_TABLE_DROP_TAG:
    case TSDB_ALTER_TABLE_DROP_COLUMN: {
      TAOS_FIELD* field = taosArrayGet(req.pFields, 0);
      RAW_NULL_CHECK(field);
      cJSON* colName = cJSON_CreateString(field->name);
      RAW_NULL_CHECK(colName);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colName", colName));
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_TAG_BYTES:
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES: {
      TAOS_FIELD* field = taosArrayGet(req.pFields, 0);
      RAW_NULL_CHECK(field);
      cJSON* colName = cJSON_CreateString(field->name);
      RAW_NULL_CHECK(colName);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colName", colName));
      cJSON* colType = cJSON_CreateNumber(field->type);
      RAW_NULL_CHECK(colType);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colType", colType));
      if (field->type == TSDB_DATA_TYPE_BINARY || field->type == TSDB_DATA_TYPE_VARBINARY ||
          field->type == TSDB_DATA_TYPE_GEOMETRY) {
        int32_t length = field->bytes - VARSTR_HEADER_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        RAW_NULL_CHECK(cbytes);
        RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colLength", cbytes));
      } else if (field->type == TSDB_DATA_TYPE_NCHAR) {
        int32_t length = (field->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        RAW_NULL_CHECK(cbytes);
        RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colLength", cbytes));
      }
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_TAG_NAME:
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME: {
      TAOS_FIELD* oldField = taosArrayGet(req.pFields, 0);
      RAW_NULL_CHECK(oldField);
      TAOS_FIELD* newField = taosArrayGet(req.pFields, 1);
      RAW_NULL_CHECK(newField);
      cJSON* colName = cJSON_CreateString(oldField->name);
      RAW_NULL_CHECK(colName);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colName", colName));
      cJSON* colNewName = cJSON_CreateString(newField->name);
      RAW_NULL_CHECK(colNewName);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colNewName", colNewName));
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS: {
      TAOS_FIELD* field = taosArrayGet(req.pFields, 0);
      RAW_NULL_CHECK(field);
      cJSON* colName = cJSON_CreateString(field->name);
      RAW_NULL_CHECK(colName);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colName", colName));
      RAW_RETURN_CHECK(setCompressOption(json, field->bytes));
      break;
    }
    default:
      break;
  }

end:
  tFreeSMAltertbReq(&req);
  *pJson = json;
}

static void processCreateStb(SMqMetaRsp* metaRsp, cJSON** pJson) {
  SVCreateStbReq req = {0};
  SDecoder       coder;

  uDebug("create stable data:%p", metaRsp);
  // decode and process req
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);

  if (tDecodeSVCreateStbReq(&coder, &req) < 0) {
    goto end;
  }
  buildCreateTableJson(&req.schemaRow, &req.schemaTag, req.name, req.suid, TSDB_SUPER_TABLE, &req.colCmpr, pJson);

end:
  uDebug("create stable return");
  tDecoderClear(&coder);
}

static void processAlterStb(SMqMetaRsp* metaRsp, cJSON** pJson) {
  SVCreateStbReq req = {0};
  SDecoder       coder = {0};
  uDebug("alter stable data:%p", metaRsp);

  // decode and process req
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);

  if (tDecodeSVCreateStbReq(&coder, &req) < 0) {
    goto end;
  }
  buildAlterSTableJson(req.alterOriData, req.alterOriDataLen, pJson);

end:
  uDebug("alter stable return");
  tDecoderClear(&coder);
}

static void buildChildElement(cJSON* json, SVCreateTbReq* pCreateReq) {
  STag*   pTag = (STag*)pCreateReq->ctb.pTag;
  char*   sname = pCreateReq->ctb.stbName;
  char*   name = pCreateReq->name;
  SArray* tagName = pCreateReq->ctb.tagName;
  int64_t id = pCreateReq->uid;
  uint8_t tagNum = pCreateReq->ctb.tagNum;
  int32_t code = 0;
  cJSON*  tags = NULL;
  cJSON*  tableName = cJSON_CreateString(name);
  RAW_NULL_CHECK(tableName);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "tableName", tableName));
  cJSON* using = cJSON_CreateString(sname);
  RAW_NULL_CHECK(using);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "using", using));
  cJSON* tagNumJson = cJSON_CreateNumber(tagNum);
  RAW_NULL_CHECK(tagNumJson);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "tagNum", tagNumJson));

  tags = cJSON_CreateArray();
  RAW_NULL_CHECK(tags);
  SArray* pTagVals = NULL;
  RAW_RETURN_CHECK(tTagToValArray(pTag, &pTagVals));

  if (tTagIsJson(pTag)) {
    STag* p = (STag*)pTag;
    if (p->nTag == 0) {
      uError("p->nTag == 0");
      goto end;
    }
    char* pJson = NULL;
    parseTagDatatoJson(pTag, &pJson);
    if (pJson == NULL) {
      uError("parseTagDatatoJson failed, pJson == NULL");
      goto end;
    }
    cJSON* tag = cJSON_CreateObject();
    RAW_NULL_CHECK(tag);
    STagVal* pTagVal = taosArrayGet(pTagVals, 0);
    RAW_NULL_CHECK(pTagVal);
    char* ptname = taosArrayGet(tagName, 0);
    RAW_NULL_CHECK(ptname);
    cJSON* tname = cJSON_CreateString(ptname);
    RAW_NULL_CHECK(tname);
    RAW_FALSE_CHECK(cJSON_AddItemToObject(tag, "name", tname));
    cJSON* ttype = cJSON_CreateNumber(TSDB_DATA_TYPE_JSON);
    RAW_NULL_CHECK(ttype);
    RAW_FALSE_CHECK(cJSON_AddItemToObject(tag, "type", ttype));
    cJSON* tvalue = cJSON_CreateString(pJson);
    RAW_NULL_CHECK(tvalue);
    RAW_FALSE_CHECK(cJSON_AddItemToObject(tag, "value", tvalue));
    RAW_FALSE_CHECK(cJSON_AddItemToArray(tags, tag));
    taosMemoryFree(pJson);
    goto end;
  }

  for (int i = 0; i < taosArrayGetSize(pTagVals); i++) {
    STagVal* pTagVal = (STagVal*)taosArrayGet(pTagVals, i);
    RAW_NULL_CHECK(pTagVal);
    cJSON* tag = cJSON_CreateObject();
    RAW_NULL_CHECK(tag);
    char* ptname = taosArrayGet(tagName, i);
    RAW_NULL_CHECK(ptname);
    cJSON* tname = cJSON_CreateString(ptname);
    RAW_NULL_CHECK(tname);
    RAW_FALSE_CHECK(cJSON_AddItemToObject(tag, "name", tname));
    cJSON* ttype = cJSON_CreateNumber(pTagVal->type);
    RAW_NULL_CHECK(ttype);
    RAW_FALSE_CHECK(cJSON_AddItemToObject(tag, "type", ttype));

    cJSON* tvalue = NULL;
    if (IS_VAR_DATA_TYPE(pTagVal->type)) {
      char*   buf = NULL;
      int64_t bufSize = 0;
      if (pTagVal->type == TSDB_DATA_TYPE_VARBINARY) {
        bufSize = pTagVal->nData * 2 + 2 + 3;
      } else {
        bufSize = pTagVal->nData + 3;
      }
      buf = taosMemoryCalloc(bufSize, 1);

      RAW_NULL_CHECK(buf);
      if (!buf) goto end;
      if (dataConverToStr(buf, bufSize, pTagVal->type, pTagVal->pData, pTagVal->nData, NULL) != TSDB_CODE_SUCCESS) {
        taosMemoryFree(buf);
        goto end;
      }

      tvalue = cJSON_CreateString(buf);
      RAW_NULL_CHECK(tvalue);
      taosMemoryFree(buf);
    } else {
      double val = 0;
      GET_TYPED_DATA(val, double, pTagVal->type, &pTagVal->i64);
      tvalue = cJSON_CreateNumber(val);
      RAW_NULL_CHECK(tvalue);
    }

    RAW_FALSE_CHECK(cJSON_AddItemToObject(tag, "value", tvalue));
    RAW_FALSE_CHECK(cJSON_AddItemToArray(tags, tag));
  }

end:
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "tags", tags));
  taosArrayDestroy(pTagVals);
}

static void buildCreateCTableJson(SVCreateTbReq* pCreateReq, int32_t nReqs, cJSON** pJson) {
  int32_t code = 0;
  char*   string = NULL;
  cJSON*  json = cJSON_CreateObject();
  RAW_NULL_CHECK(json);
  cJSON* type = cJSON_CreateString("create");
  RAW_NULL_CHECK(type);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "type", type));

  cJSON* tableType = cJSON_CreateString("child");
  RAW_NULL_CHECK(tableType);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "tableType", tableType));

  buildChildElement(json, pCreateReq);
  cJSON* createList = cJSON_CreateArray();
  RAW_NULL_CHECK(createList);
  for (int i = 0; nReqs > 1 && i < nReqs; i++) {
    cJSON* create = cJSON_CreateObject();
    RAW_NULL_CHECK(create);
    buildChildElement(create, pCreateReq + i);
    RAW_FALSE_CHECK(cJSON_AddItemToArray(createList, create));
  }
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "createList", createList));

end:
  *pJson = json;
}

static void processCreateTable(SMqMetaRsp* metaRsp, cJSON** pJson) {
  SDecoder           decoder = {0};
  SVCreateTbBatchReq req = {0};
  SVCreateTbReq*     pCreateReq;
  // decode
  uDebug("create table data:%p", metaRsp);
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  if (tDecodeSVCreateTbBatchReq(&decoder, &req) < 0) {
    goto end;
  }

  // loop to create table
  if (req.nReqs > 0) {
    pCreateReq = req.pReqs;
    if (pCreateReq->type == TSDB_CHILD_TABLE) {
      buildCreateCTableJson(req.pReqs, req.nReqs, pJson);
    } else if (pCreateReq->type == TSDB_NORMAL_TABLE) {
      buildCreateTableJson(&pCreateReq->ntb.schemaRow, NULL, pCreateReq->name, pCreateReq->uid, TSDB_NORMAL_TABLE,
                           &pCreateReq->colCmpr, pJson);
    }
  }

end:
  uDebug("create table return");
  tDeleteSVCreateTbBatchReq(&req);
  tDecoderClear(&decoder);
}

static void processAutoCreateTable(SMqDataRsp* rsp, char** string) {
  SDecoder*      decoder = NULL;
  SVCreateTbReq* pCreateReq = NULL;
  int32_t        code = 0;
  uDebug("auto create table data:%p", rsp);
  if (rsp->createTableNum <= 0) {
    uError("processAutoCreateTable rsp->createTableNum <= 0");
    goto end;
  }

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
    if (tDecodeSVCreateTbReq(&decoder[iReq], pCreateReq + iReq) < 0) {
      goto end;
    }

    if (pCreateReq[iReq].type != TSDB_CHILD_TABLE) {
      uError("processAutoCreateTable pCreateReq[iReq].type != TSDB_CHILD_TABLE");
      goto end;
    }
  }
  cJSON* pJson = NULL;
  buildCreateCTableJson(pCreateReq, rsp->createTableNum, &pJson);
  *string = cJSON_PrintUnformatted(pJson);
  cJSON_Delete(pJson);

end:
  uDebug("auto created table return, sql json:%s", *string);
  for (int i = 0; decoder && pCreateReq && i < rsp->createTableNum; i++) {
    tDecoderClear(&decoder[i]);
    taosMemoryFreeClear(pCreateReq[i].comment);
    if (pCreateReq[i].type == TSDB_CHILD_TABLE) {
      taosArrayDestroy(pCreateReq[i].ctb.tagName);
    }
  }
  taosMemoryFree(decoder);
  taosMemoryFree(pCreateReq);
}

static void processAlterTable(SMqMetaRsp* metaRsp, cJSON** pJson) {
  SDecoder     decoder = {0};
  SVAlterTbReq vAlterTbReq = {0};
  char*        string = NULL;
  cJSON*       json = NULL;
  int32_t      code = 0;

  uDebug("alter table data:%p", metaRsp);
  // decode
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  if (tDecodeSVAlterTbReq(&decoder, &vAlterTbReq) < 0) {
    uError("tDecodeSVAlterTbReq error");
    goto end;
  }

  json = cJSON_CreateObject();
  RAW_NULL_CHECK(json);
  cJSON* type = cJSON_CreateString("alter");
  RAW_NULL_CHECK(type);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "type", type));
  cJSON* tableType = cJSON_CreateString(vAlterTbReq.action == TSDB_ALTER_TABLE_UPDATE_TAG_VAL ? "child" : "normal");
  RAW_NULL_CHECK(tableType);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "tableType", tableType));
  cJSON* tableName = cJSON_CreateString(vAlterTbReq.tbName);
  RAW_NULL_CHECK(tableName);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "tableName", tableName));
  cJSON* alterType = cJSON_CreateNumber(vAlterTbReq.action);
  RAW_NULL_CHECK(alterType);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "alterType", alterType));

  switch (vAlterTbReq.action) {
    case TSDB_ALTER_TABLE_ADD_COLUMN: {
      cJSON* colName = cJSON_CreateString(vAlterTbReq.colName);
      RAW_NULL_CHECK(colName);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colName", colName));
      cJSON* colType = cJSON_CreateNumber(vAlterTbReq.type);
      RAW_NULL_CHECK(colType);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colType", colType));

      if (vAlterTbReq.type == TSDB_DATA_TYPE_BINARY || vAlterTbReq.type == TSDB_DATA_TYPE_VARBINARY ||
          vAlterTbReq.type == TSDB_DATA_TYPE_GEOMETRY) {
        int32_t length = vAlterTbReq.bytes - VARSTR_HEADER_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        RAW_NULL_CHECK(cbytes);
        RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colLength", cbytes));
      } else if (vAlterTbReq.type == TSDB_DATA_TYPE_NCHAR) {
        int32_t length = (vAlterTbReq.bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        RAW_NULL_CHECK(cbytes);
        RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colLength", cbytes));
      }
      break;
    }
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION: {
      cJSON* colName = cJSON_CreateString(vAlterTbReq.colName);
      RAW_NULL_CHECK(colName);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colName", colName));
      cJSON* colType = cJSON_CreateNumber(vAlterTbReq.type);
      RAW_NULL_CHECK(colType);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colType", colType));

      if (vAlterTbReq.type == TSDB_DATA_TYPE_BINARY || vAlterTbReq.type == TSDB_DATA_TYPE_VARBINARY ||
          vAlterTbReq.type == TSDB_DATA_TYPE_GEOMETRY) {
        int32_t length = vAlterTbReq.bytes - VARSTR_HEADER_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        RAW_NULL_CHECK(cbytes);
        RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colLength", cbytes));
      } else if (vAlterTbReq.type == TSDB_DATA_TYPE_NCHAR) {
        int32_t length = (vAlterTbReq.bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        RAW_NULL_CHECK(cbytes);
        RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colLength", cbytes));
      }
      RAW_RETURN_CHECK(setCompressOption(json, vAlterTbReq.compress));
      break;
    }
    case TSDB_ALTER_TABLE_DROP_COLUMN: {
      cJSON* colName = cJSON_CreateString(vAlterTbReq.colName);
      RAW_NULL_CHECK(colName);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colName", colName));
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES: {
      cJSON* colName = cJSON_CreateString(vAlterTbReq.colName);
      RAW_NULL_CHECK(colName);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colName", colName));
      cJSON* colType = cJSON_CreateNumber(vAlterTbReq.colModType);
      RAW_NULL_CHECK(colType);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colType", colType));
      if (vAlterTbReq.colModType == TSDB_DATA_TYPE_BINARY || vAlterTbReq.colModType == TSDB_DATA_TYPE_VARBINARY ||
          vAlterTbReq.colModType == TSDB_DATA_TYPE_GEOMETRY) {
        int32_t length = vAlterTbReq.colModBytes - VARSTR_HEADER_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        RAW_NULL_CHECK(cbytes);
        RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colLength", cbytes));
      } else if (vAlterTbReq.colModType == TSDB_DATA_TYPE_NCHAR) {
        int32_t length = (vAlterTbReq.colModBytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        RAW_NULL_CHECK(cbytes);
        RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colLength", cbytes));
      }
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME: {
      cJSON* colName = cJSON_CreateString(vAlterTbReq.colName);
      RAW_NULL_CHECK(colName);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colName", colName));
      cJSON* colNewName = cJSON_CreateString(vAlterTbReq.colNewName);
      RAW_NULL_CHECK(colNewName);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colNewName", colNewName));
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_TAG_VAL: {
      cJSON* tagName = cJSON_CreateString(vAlterTbReq.tagName);
      RAW_NULL_CHECK(tagName);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colName", tagName));

      bool isNull = vAlterTbReq.isNull;
      if (vAlterTbReq.tagType == TSDB_DATA_TYPE_JSON) {
        STag* jsonTag = (STag*)vAlterTbReq.pTagVal;
        if (jsonTag->nTag == 0) isNull = true;
      }
      if (!isNull) {
        char* buf = NULL;

        if (vAlterTbReq.tagType == TSDB_DATA_TYPE_JSON) {
          if (!tTagIsJson(vAlterTbReq.pTagVal)) {
            uError("processAlterTable isJson false");
            goto end;
          }
          parseTagDatatoJson(vAlterTbReq.pTagVal, &buf);
          if (buf == NULL) {
            uError("parseTagDatatoJson failed, buf == NULL");
            goto end;
          }
        } else {
          int64_t bufSize = 0;
          if (vAlterTbReq.tagType == TSDB_DATA_TYPE_VARBINARY) {
            bufSize = vAlterTbReq.nTagVal * 2 + 2 + 3;
          } else {
            bufSize = vAlterTbReq.nTagVal + 3;
          }
          buf = taosMemoryCalloc(bufSize, 1);
          RAW_NULL_CHECK(buf);
          if (dataConverToStr(buf, bufSize, vAlterTbReq.tagType, vAlterTbReq.pTagVal, vAlterTbReq.nTagVal, NULL) !=
              TSDB_CODE_SUCCESS) {
            taosMemoryFree(buf);
            goto end;
          }
        }

        cJSON* colValue = cJSON_CreateString(buf);
        RAW_NULL_CHECK(colValue);
        RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colValue", colValue));
        taosMemoryFree(buf);
      }

      cJSON* isNullCJson = cJSON_CreateBool(isNull);
      RAW_NULL_CHECK(isNullCJson);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colValueNull", isNullCJson));
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS: {
      cJSON* colName = cJSON_CreateString(vAlterTbReq.colName);
      RAW_NULL_CHECK(colName);
      RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "colName", colName));
      RAW_RETURN_CHECK(setCompressOption(json, vAlterTbReq.compress));
      break;
    }
    default:
      break;
  }

end:
  uDebug("alter table return");
  tDecoderClear(&decoder);
  *pJson = json;
}

static void processDropSTable(SMqMetaRsp* metaRsp, cJSON** pJson) {
  SDecoder     decoder = {0};
  SVDropStbReq req = {0};
  cJSON*       json = NULL;
  int32_t      code = 0;

  uDebug("processDropSTable data:%p", metaRsp);

  // decode
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  if (tDecodeSVDropStbReq(&decoder, &req) < 0) {
    uError("tDecodeSVDropStbReq failed");
    goto end;
  }

  json = cJSON_CreateObject();
  RAW_NULL_CHECK(json);
  cJSON* type = cJSON_CreateString("drop");
  RAW_NULL_CHECK(type);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "type", type));
  cJSON* tableType = cJSON_CreateString("super");
  RAW_NULL_CHECK(tableType);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "tableType", tableType));
  cJSON* tableName = cJSON_CreateString(req.name);
  RAW_NULL_CHECK(tableName);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "tableName", tableName));

end:
  uDebug("processDropSTable return");
  tDecoderClear(&decoder);
  *pJson = json;
}
static void processDeleteTable(SMqMetaRsp* metaRsp, cJSON** pJson) {
  SDeleteRes req = {0};
  SDecoder   coder = {0};
  cJSON*     json = NULL;
  int32_t    code = 0;

  uDebug("processDeleteTable data:%p", metaRsp);
  // decode and process req
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);

  tDecoderInit(&coder, data, len);
  if (tDecodeDeleteRes(&coder, &req) < 0) {
    uError("tDecodeDeleteRes failed");
    goto end;
  }

  //  getTbName(req.tableFName);
  char sql[256] = {0};
  (void)snprintf(sql, sizeof(sql), "delete from `%s` where `%s` >= %" PRId64 " and `%s` <= %" PRId64, req.tableFName,
                 req.tsColName, req.skey, req.tsColName, req.ekey);

  json = cJSON_CreateObject();
  RAW_NULL_CHECK(json);
  cJSON* type = cJSON_CreateString("delete");
  RAW_NULL_CHECK(type);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "type", type));
  cJSON* sqlJson = cJSON_CreateString(sql);
  RAW_NULL_CHECK(sqlJson);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "sql", sqlJson));

end:
  uDebug("processDeleteTable return");
  tDecoderClear(&coder);
  *pJson = json;
}

static void processDropTable(SMqMetaRsp* metaRsp, cJSON** pJson) {
  SDecoder         decoder = {0};
  SVDropTbBatchReq req = {0};
  cJSON*           json = NULL;
  int32_t          code = 0;

  uDebug("processDropTable data:%p", metaRsp);
  // decode
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  if (tDecodeSVDropTbBatchReq(&decoder, &req) < 0) {
    uError("tDecodeSVDropTbBatchReq failed");
    goto end;
  }

  json = cJSON_CreateObject();
  RAW_NULL_CHECK(json);
  cJSON* type = cJSON_CreateString("drop");
  RAW_NULL_CHECK(type);
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "type", type));
  cJSON* tableNameList = cJSON_CreateArray();
  RAW_NULL_CHECK(tableNameList);
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    SVDropTbReq* pDropTbReq = req.pReqs + iReq;
    cJSON*       tableName = cJSON_CreateString(pDropTbReq->name);
    RAW_NULL_CHECK(tableName);
    RAW_FALSE_CHECK(cJSON_AddItemToArray(tableNameList, tableName));
  }
  RAW_FALSE_CHECK(cJSON_AddItemToObject(json, "tableNameList", tableNameList));

end:
  uDebug("processDropTable return");
  tDecoderClear(&decoder);
  *pJson = json;
}

static int32_t taosCreateStb(TAOS* taos, void* meta, int32_t metaLen) {
  SVCreateStbReq req = {0};
  SDecoder       coder;
  SMCreateStbReq pReq = {0};
  int32_t        code = TSDB_CODE_SUCCESS;
  SRequestObj*   pRequest = NULL;

  RAW_RETURN_CHECK(buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0));
  uDebug(LOG_ID_TAG " create stable, meta:%p, metaLen:%d", LOG_ID_VALUE, meta, metaLen);
  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void*   data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  int32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  if (tDecodeSVCreateStbReq(&coder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

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

  uDebug(LOG_ID_TAG " create stable name:%s suid:%" PRId64 " processSuid:%" PRId64, LOG_ID_VALUE, req.name, req.suid,
         pReq.suid);
  STscObj* pTscObj = pRequest->pTscObj;
  SName    tableName = {0};
  toName(pTscObj->acctId, pRequest->pDb, req.name, &tableName);
  RAW_RETURN_CHECK(tNameExtractFullName(&tableName, pReq.name));
  SCmdMsgInfo pCmdMsg = {0};
  pCmdMsg.epSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);
  pCmdMsg.msgType = TDMT_MND_CREATE_STB;
  pCmdMsg.msgLen = tSerializeSMCreateStbReq(NULL, 0, &pReq);
  if (pCmdMsg.msgLen <= 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }
  pCmdMsg.pMsg = taosMemoryMalloc(pCmdMsg.msgLen);
  RAW_NULL_CHECK(pCmdMsg.pMsg);
  if (tSerializeSMCreateStbReq(pCmdMsg.pMsg, pCmdMsg.msgLen, &pReq) <= 0) {
    code = TSDB_CODE_INVALID_PARA;
    taosMemoryFree(pCmdMsg.pMsg);
    goto end;
  }

  SQuery pQuery = {0};
  pQuery.execMode = QUERY_EXEC_MODE_RPC;
  pQuery.pCmdMsg = &pCmdMsg;
  pQuery.msgType = pQuery.pCmdMsg->msgType;
  pQuery.stableQuery = true;

  launchQueryImpl(pRequest, &pQuery, true, NULL);  // ignore, because return value is pRequest

  taosMemoryFree(pCmdMsg.pMsg);

  if (pRequest->code == TSDB_CODE_SUCCESS) {
    SCatalog* pCatalog = NULL;
    RAW_RETURN_CHECK(catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog));
    RAW_RETURN_CHECK(catalogRemoveTableMeta(pCatalog, &tableName));
  }

  code = pRequest->code;

end:
  uDebug(LOG_ID_TAG " create stable return, msg:%s", LOG_ID_VALUE, tstrerror(code));
  destroyRequest(pRequest);
  tFreeSMCreateStbReq(&pReq);
  tDecoderClear(&coder);
  return code;
}

static int32_t taosDropStb(TAOS* taos, void* meta, int32_t metaLen) {
  SVDropStbReq req = {0};
  SDecoder     coder = {0};
  SMDropStbReq pReq = {0};
  int32_t      code = TSDB_CODE_SUCCESS;
  SRequestObj* pRequest = NULL;

  RAW_RETURN_CHECK(buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0));
  uDebug(LOG_ID_TAG " drop stable, meta:%p, metaLen:%d", LOG_ID_VALUE, meta, metaLen);
  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void*   data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  int32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  if (tDecodeSVDropStbReq(&coder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

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
    code = TSDB_CODE_SUCCESS;
    taosMemoryFreeClear(pTableMeta);
    goto end;
  }
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }
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
  if (tNameExtractFullName(&tableName, pReq.name) != 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

  SCmdMsgInfo pCmdMsg = {0};
  pCmdMsg.epSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);
  pCmdMsg.msgType = TDMT_MND_DROP_STB;
  pCmdMsg.msgLen = tSerializeSMDropStbReq(NULL, 0, &pReq);
  if (pCmdMsg.msgLen <= 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }
  pCmdMsg.pMsg = taosMemoryMalloc(pCmdMsg.msgLen);
  RAW_NULL_CHECK(pCmdMsg.pMsg);
  if (tSerializeSMDropStbReq(pCmdMsg.pMsg, pCmdMsg.msgLen, &pReq) <= 0) {
    code = TSDB_CODE_INVALID_PARA;
    taosMemoryFree(pCmdMsg.pMsg);
    goto end;
  }

  SQuery pQuery = {0};
  pQuery.execMode = QUERY_EXEC_MODE_RPC;
  pQuery.pCmdMsg = &pCmdMsg;
  pQuery.msgType = pQuery.pCmdMsg->msgType;
  pQuery.stableQuery = true;

  launchQueryImpl(pRequest, &pQuery, true, NULL);  // ignore, because return value is pRequest
  taosMemoryFree(pCmdMsg.pMsg);
  if (pRequest->code == TSDB_CODE_SUCCESS) {
    // ignore the error code
    RAW_RETURN_CHECK(catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog));
    RAW_RETURN_CHECK(catalogRemoveTableMeta(pCatalog, &tableName));
  }

  code = pRequest->code;

end:
  uDebug(LOG_ID_TAG " drop stable return, msg:%s", LOG_ID_VALUE, tstrerror(code));
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
  SVgroupCreateTableBatch* pTbBatch = (SVgroupCreateTableBatch*)data;
  taosArrayDestroy(pTbBatch->req.pArray);
}

static int32_t taosCreateTable(TAOS* taos, void* meta, int32_t metaLen) {
  SVCreateTbBatchReq req = {0};
  SDecoder           coder = {0};
  int32_t            code = TSDB_CODE_SUCCESS;
  SRequestObj*       pRequest = NULL;
  SQuery*            pQuery = NULL;
  SHashObj*          pVgroupHashmap = NULL;
  SArray*            pTagList = taosArrayInit(0, POINTER_BYTES);
  RAW_NULL_CHECK(pTagList);
  RAW_RETURN_CHECK(buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0));
  uDebug(LOG_ID_TAG " create table, meta:%p, metaLen:%d", LOG_ID_VALUE, meta, metaLen);

  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void*   data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  int32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  if (tDecodeSVCreateTbBatchReq(&coder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

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
    code = catalogGetTableHashVgroup(pCatalog, &conn, &pName, &pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      goto end;
    }

    pCreateReq->flags |= TD_CREATE_IF_NOT_EXISTS;
    // change tag cid to new cid
    if (pCreateReq->type == TSDB_CHILD_TABLE) {
      STableMeta* pTableMeta = NULL;
      SName       sName = {0};
      tb_uid_t    oldSuid = pCreateReq->ctb.suid;
      //      pCreateReq->ctb.suid = processSuid(pCreateReq->ctb.suid, pRequest->pDb);
      toName(pTscObj->acctId, pRequest->pDb, pCreateReq->ctb.stbName, &sName);
      code = catalogGetTableMeta(pCatalog, &conn, &sName, &pTableMeta);
      if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
        code = TSDB_CODE_SUCCESS;
        taosMemoryFreeClear(pTableMeta);
        continue;
      }

      if (code != TSDB_CODE_SUCCESS) {
        goto end;
      }
      pCreateReq->ctb.suid = pTableMeta->uid;

      SArray* pTagVals = NULL;
      code = tTagToValArray((STag*)pCreateReq->ctb.pTag, &pTagVals);
      if (code != TSDB_CODE_SUCCESS) {
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

end:
  uDebug(LOG_ID_TAG " create table return, msg:%s", LOG_ID_VALUE, tstrerror(code));
  tDeleteSVCreateTbBatchReq(&req);

  taosHashCleanup(pVgroupHashmap);
  destroyRequest(pRequest);
  tDecoderClear(&coder);
  qDestroyQuery(pQuery);
  taosArrayDestroyP(pTagList, taosMemoryFree);
  return code;
}

typedef struct SVgroupDropTableBatch {
  SVDropTbBatchReq req;
  SVgroupInfo      info;
  char             dbName[TSDB_DB_NAME_LEN];
} SVgroupDropTableBatch;

static void destroyDropTbReqBatch(void* data) {
  SVgroupDropTableBatch* pTbBatch = (SVgroupDropTableBatch*)data;
  taosArrayDestroy(pTbBatch->req.pArray);
}

static int32_t taosDropTable(TAOS* taos, void* meta, int32_t metaLen) {
  SVDropTbBatchReq req = {0};
  SDecoder         coder = {0};
  int32_t          code = TSDB_CODE_SUCCESS;
  SRequestObj*     pRequest = NULL;
  SQuery*          pQuery = NULL;
  SHashObj*        pVgroupHashmap = NULL;

  RAW_RETURN_CHECK(buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0));
  uDebug(LOG_ID_TAG " drop table, meta:%p, len:%d", LOG_ID_VALUE, meta, metaLen);

  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void*   data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  int32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  if (tDecodeSVDropTbBatchReq(&coder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

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
      taosMemoryFreeClear(pTableMeta);
      continue;
    }
    if (code != TSDB_CODE_SUCCESS) {
      goto end;
    }
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

end:
  uDebug(LOG_ID_TAG " drop table return, msg:%s", LOG_ID_VALUE, tstrerror(code));
  taosHashCleanup(pVgroupHashmap);
  destroyRequest(pRequest);
  tDecoderClear(&coder);
  qDestroyQuery(pQuery);
  return code;
}

static int32_t taosDeleteData(TAOS* taos, void* meta, int32_t metaLen) {
  SDeleteRes req = {0};
  SDecoder   coder = {0};
  char       sql[256] = {0};
  int32_t    code = TSDB_CODE_SUCCESS;

  uDebug("connId:0x%" PRIx64 " delete data, meta:%p, len:%d", *(int64_t*)taos, meta, metaLen);

  // decode and process req
  void*   data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  int32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  if (tDecodeDeleteRes(&coder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

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

end:
  uDebug("connId:0x%" PRIx64 " delete data sql:%s, code:%s", *(int64_t*)taos, sql, tstrerror(code));
  tDecoderClear(&coder);
  return code;
}

static int32_t taosAlterTable(TAOS* taos, void* meta, int32_t metaLen) {
  SVAlterTbReq   req = {0};
  SDecoder       dcoder = {0};
  int32_t        code = TSDB_CODE_SUCCESS;
  SRequestObj*   pRequest = NULL;
  SQuery*        pQuery = NULL;
  SArray*        pArray = NULL;
  SVgDataBlocks* pVgData = NULL;

  RAW_RETURN_CHECK(buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0));
  uDebug(LOG_ID_TAG " alter table, meta:%p, len:%d", LOG_ID_VALUE, meta, metaLen);
  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void*   data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  int32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&dcoder, data, len);
  if (tDecodeSVAlterTbReq(&dcoder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

  // do not deal TSDB_ALTER_TABLE_UPDATE_OPTIONS
  if (req.action == TSDB_ALTER_TABLE_UPDATE_OPTIONS) {
    goto end;
  }

  STscObj*  pTscObj = pRequest->pTscObj;
  SCatalog* pCatalog = NULL;
  RAW_RETURN_CHECK(catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog));
  SRequestConnInfo conn = {.pTrans = pTscObj->pAppInfo->pTransporter,
                           .requestId = pRequest->requestId,
                           .requestObjRefId = pRequest->self,
                           .mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp)};

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
  tEncodeSize(tEncodeSVAlterTbReq, &req, tlen, code);
  if (code != 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  tlen += sizeof(SMsgHead);
  void* pMsg = taosMemoryMalloc(tlen);
  RAW_NULL_CHECK(pMsg);
  ((SMsgHead*)pMsg)->vgId = htonl(pInfo.vgId);
  ((SMsgHead*)pMsg)->contLen = htonl(tlen);
  void*    pBuf = POINTER_SHIFT(pMsg, sizeof(SMsgHead));
  SEncoder coder = {0};
  tEncoderInit(&coder, pBuf, tlen - sizeof(SMsgHead));
  code = tEncodeSVAlterTbReq(&coder, &req);
  if (code != 0) {
    tEncoderClear(&coder);
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  tEncoderClear(&coder);

  pVgData->pData = pMsg;
  pVgData->size = tlen;

  pVgData->numOfTables = 1;
  RAW_NULL_CHECK(taosArrayPush(pArray, &pVgData));

  pQuery = NULL;
  code = nodesMakeNode(QUERY_NODE_QUERY, (SNode**)&pQuery);
  if (NULL == pQuery) goto end;
  pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  pQuery->msgType = TDMT_VND_ALTER_TABLE;
  pQuery->stableQuery = false;
  pQuery->pRoot = NULL;
  code = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT, &pQuery->pRoot);
  if (TSDB_CODE_SUCCESS != code) goto end;
  RAW_RETURN_CHECK(rewriteToVnodeModifyOpStmt(pQuery, pArray));

  launchQueryImpl(pRequest, pQuery, true, NULL);

  pVgData = NULL;
  pArray = NULL;
  code = pRequest->code;
  if (code == TSDB_CODE_TDB_TABLE_NOT_EXIST) {
    code = TSDB_CODE_SUCCESS;
  }

  if (pRequest->code == TSDB_CODE_SUCCESS) {
    SExecResult* pRes = &pRequest->body.resInfo.execRes;
    if (pRes->res != NULL) {
      code = handleAlterTbExecRes(pRes->res, pCatalog);
    }
  }
end:
  uDebug(LOG_ID_TAG " alter table return, meta:%p, len:%d, msg:%s", LOG_ID_VALUE, meta, metaLen, tstrerror(code));
  taosArrayDestroy(pArray);
  if (pVgData) taosMemoryFreeClear(pVgData->pData);
  taosMemoryFreeClear(pVgData);
  destroyRequest(pRequest);
  tDecoderClear(&dcoder);
  qDestroyQuery(pQuery);
  return code;
}

int taos_write_raw_block_with_fields(TAOS* taos, int rows, char* pData, const char* tbname, TAOS_FIELD* fields,
                                     int numFields) {
  return taos_write_raw_block_with_fields_with_reqid(taos, rows, pData, tbname, fields, numFields, 0);
}

int taos_write_raw_block_with_fields_with_reqid(TAOS* taos, int rows, char* pData, const char* tbname,
                                                TAOS_FIELD* fields, int numFields, int64_t reqid) {
  if (!taos || !pData || !tbname) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t     code = TSDB_CODE_SUCCESS;
  STableMeta* pTableMeta = NULL;
  SQuery*     pQuery = NULL;
  SHashObj*   pVgHash = NULL;

  SRequestObj* pRequest = NULL;
  RAW_RETURN_CHECK(createRequest(*(int64_t*)taos, TSDB_SQL_INSERT, reqid, &pRequest));

  uDebug(LOG_ID_TAG " write raw block with field, rows:%d, pData:%p, tbname:%s, fields:%p, numFields:%d", LOG_ID_VALUE,
         rows, pData, tbname, fields, numFields);

  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
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

end:
  uDebug(LOG_ID_TAG " write raw block with field return, msg:%s", LOG_ID_VALUE, tstrerror(code));
  taosMemoryFreeClear(pTableMeta);
  qDestroyQuery(pQuery);
  destroyRequest(pRequest);
  taosHashCleanup(pVgHash);
  return code;
}

int taos_write_raw_block(TAOS* taos, int rows, char* pData, const char* tbname) {
  return taos_write_raw_block_with_reqid(taos, rows, pData, tbname, 0);
}

int taos_write_raw_block_with_reqid(TAOS* taos, int rows, char* pData, const char* tbname, int64_t reqid) {
  if (!taos || !pData || !tbname) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t     code = TSDB_CODE_SUCCESS;
  STableMeta* pTableMeta = NULL;
  SQuery*     pQuery = NULL;
  SHashObj*   pVgHash = NULL;

  SRequestObj* pRequest = NULL;
  RAW_RETURN_CHECK(createRequest(*(int64_t*)taos, TSDB_SQL_INSERT, reqid, &pRequest));

  uDebug(LOG_ID_TAG " write raw block, rows:%d, pData:%p, tbname:%s", LOG_ID_VALUE, rows, pData, tbname);

  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
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

end:
  uDebug(LOG_ID_TAG " write raw block return, msg:%s", LOG_ID_VALUE, tstrerror(code));
  taosMemoryFreeClear(pTableMeta);
  qDestroyQuery(pQuery);
  destroyRequest(pRequest);
  taosHashCleanup(pVgHash);
  return code;
}

static void* getRawDataFromRes(void* pRetrieve) {
  void* rawData = NULL;
  // deal with compatibility
  if (*(int64_t*)pRetrieve == 0) {
    rawData = ((SRetrieveTableRsp*)pRetrieve)->data;
  } else if (*(int64_t*)pRetrieve == 1) {
    rawData = ((SRetrieveTableRspForTmq*)pRetrieve)->data;
  }
  return rawData;
}

static int32_t buildCreateTbMap(SMqDataRsp* rsp, SHashObj* pHashObj) {
  // find schema data info
  int32_t       code = 0;
  SVCreateTbReq pCreateReq = {0};
  SDecoder      decoderTmp = {0};

  for (int j = 0; j < rsp->createTableNum; j++) {
    void** dataTmp = taosArrayGet(rsp->createTableReq, j);
    RAW_NULL_CHECK(dataTmp);
    int32_t* lenTmp = taosArrayGet(rsp->createTableLen, j);
    RAW_NULL_CHECK(lenTmp);

    tDecoderInit(&decoderTmp, *dataTmp, *lenTmp);
    RAW_RETURN_CHECK(tDecodeSVCreateTbReq(&decoderTmp, &pCreateReq));

    if (pCreateReq.type != TSDB_CHILD_TABLE) {
      code = TSDB_CODE_INVALID_MSG;
      goto end;
    }
    if (taosHashGet(pHashObj, pCreateReq.name, strlen(pCreateReq.name)) == NULL) {
      RAW_RETURN_CHECK(
          taosHashPut(pHashObj, pCreateReq.name, strlen(pCreateReq.name), &pCreateReq, sizeof(SVCreateTbReq)));
    } else {
      tDestroySVCreateTbReq(&pCreateReq, TSDB_MSG_FLG_DECODE);
      pCreateReq = (SVCreateTbReq){0};
    }

    tDecoderClear(&decoderTmp);
  }
  return 0;

end:
  tDecoderClear(&decoderTmp);
  tDestroySVCreateTbReq(&pCreateReq, TSDB_MSG_FLG_DECODE);
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
  STableMeta* pTableMeta = *(STableMeta**)data;
  taosMemoryFree(pTableMeta);
}

static void freeRawCache(void* data) {
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
      SSchema* pColSchema = &pTableMeta->schema[j];
      char*    fieldName = pSW->pSchema[i].name;

      if (strcmp(pColSchema->name, fieldName) == 0) {
        if (*fields != pColSchema->type || *(int32_t*)(fields + sizeof(int8_t)) != pColSchema->bytes) {
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
  int32_t code = 0;
  void*   cacheInfo = taosHashGet(writeRawCache, &key, POINTER_BYTES);
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

  return 0;
end:
  taosHashCleanup(*pMetaHash);
  taosHashCleanup(*pNameHash);
  taosHashCleanup(*pVgHash);
  return code;
}

static int32_t buildRawRequest(TAOS* taos, SRequestObj** pRequest, SCatalog** pCatalog, SRequestConnInfo* conn) {
  int32_t code = 0;
  RAW_RETURN_CHECK(createRequest(*(int64_t*)taos, TSDB_SQL_INSERT, 0, pRequest));
  (*pRequest)->syncQuery = true;
  if (!(*pRequest)->pDb) {
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }

  RAW_RETURN_CHECK(catalogGetHandle((*pRequest)->pTscObj->pAppInfo->clusterId, pCatalog));
  conn->pTrans = (*pRequest)->pTscObj->pAppInfo->pTransporter;
  conn->requestId = (*pRequest)->requestId;
  conn->requestObjRefId = (*pRequest)->self;
  conn->mgmtEps = getEpSet_s(&(*pRequest)->pTscObj->pAppInfo->mgmtEp);

end:
  return code;
}

typedef int32_t _raw_decode_func_(SDecoder* pDecoder, SMqDataRsp* pRsp);
static int32_t  decodeRawData(SDecoder* decoder, void* data, int32_t dataLen, _raw_decode_func_ func,
                              SMqRspObj* rspObj) {
   int8_t dataVersion = *(int8_t*)data;
   if (dataVersion >= MQ_DATA_RSP_VERSION) {
     data = POINTER_SHIFT(data, sizeof(int8_t) + sizeof(int32_t));
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
  int32_t     code = 0;
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
    if (code != 0) {
      taosMemoryFree(pTableMeta);
      goto end;
    }
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
      if (code != 0) {
        taosMemoryFree(pTableMeta);
        goto end;
      }

    } else {
      pTableMeta = *pTableMetaTmp;
      pTableMeta->uid = tmpInfo->uid;
      pTableMeta->vgId = tmpInfo->vgInfo.vgId;
    }
  }
  *pMeta = pTableMeta;

end:
  return code;
}

static int32_t tmqWriteRawDataImpl(TAOS* taos, void* data, int32_t dataLen) {
  int32_t   code = TSDB_CODE_SUCCESS;
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
      if (!rspObj.dataRsp.withSchema) {
        goto end;
      }

      const char* tbName = (const char*)taosArrayGetP(rspObj.dataRsp.blockTbName, rspObj.resIter);
      RAW_NULL_CHECK(tbName);
      SSchemaWrapper* pSW = (SSchemaWrapper*)taosArrayGetP(rspObj.dataRsp.blockSchema, rspObj.resIter);
      RAW_NULL_CHECK(pSW);
      void* pRetrieve = taosArrayGetP(rspObj.dataRsp.blockData, rspObj.resIter);
      RAW_NULL_CHECK(pRetrieve);
      void* rawData = getRawDataFromRes(pRetrieve);
      RAW_NULL_CHECK(rawData);

      uDebug(LOG_ID_TAG " write raw data block tbname:%s", LOG_ID_VALUE, tbName);
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

end:
  uDebug(LOG_ID_TAG " write raw data return, msg:%s", LOG_ID_VALUE, tstrerror(code));
  tDeleteMqDataRsp(&rspObj.dataRsp);
  tDecoderClear(&decoder);
  qDestroyQuery(pQuery);
  destroyRequest(pRequest);
  return code;
}

static int32_t tmqWriteRawMetaDataImpl(TAOS* taos, void* data, int32_t dataLen) {
  int32_t   code = TSDB_CODE_SUCCESS;
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
      if (!rspObj.dataRsp.withSchema) {
        goto end;
      }

      const char* tbName = (const char*)taosArrayGetP(rspObj.dataRsp.blockTbName, rspObj.resIter);
      RAW_NULL_CHECK(tbName);
      SSchemaWrapper* pSW = (SSchemaWrapper*)taosArrayGetP(rspObj.dataRsp.blockSchema, rspObj.resIter);
      RAW_NULL_CHECK(pSW);
      void* pRetrieve = taosArrayGetP(rspObj.dataRsp.blockData, rspObj.resIter);
      RAW_NULL_CHECK(pRetrieve);
      void* rawData = getRawDataFromRes(pRetrieve);
      RAW_NULL_CHECK(rawData);

      uDebug(LOG_ID_TAG " write raw data block tbname:%s", LOG_ID_VALUE, tbName);
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

end:
  uDebug(LOG_ID_TAG " write raw metadata return, msg:%s", LOG_ID_VALUE, tstrerror(code));
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
  return code;
}

static void processSimpleMeta(SMqMetaRsp* pMetaRsp, cJSON** meta) {
  if (pMetaRsp->resMsgType == TDMT_VND_CREATE_STB) {
    processCreateStb(pMetaRsp, meta);
  } else if (pMetaRsp->resMsgType == TDMT_VND_ALTER_STB) {
    processAlterStb(pMetaRsp, meta);
  } else if (pMetaRsp->resMsgType == TDMT_VND_DROP_STB) {
    processDropSTable(pMetaRsp, meta);
  } else if (pMetaRsp->resMsgType == TDMT_VND_CREATE_TABLE) {
    processCreateTable(pMetaRsp, meta);
  } else if (pMetaRsp->resMsgType == TDMT_VND_ALTER_TABLE) {
    processAlterTable(pMetaRsp, meta);
  } else if (pMetaRsp->resMsgType == TDMT_VND_DROP_TABLE) {
    processDropTable(pMetaRsp, meta);
  } else if (pMetaRsp->resMsgType == TDMT_VND_DROP_TABLE) {
    processDropTable(pMetaRsp, meta);
  } else if (pMetaRsp->resMsgType == TDMT_VND_DELETE) {
    processDeleteTable(pMetaRsp, meta);
  }
}

static void processBatchMetaToJson(SMqBatchMetaRsp* pMsgRsp, char** string) {
  SDecoder        coder;
  SMqBatchMetaRsp rsp = {0};
  int32_t         code = 0;
  cJSON*          pJson = NULL;
  tDecoderInit(&coder, pMsgRsp->pMetaBuff, pMsgRsp->metaBuffLen);
  if (tDecodeMqBatchMetaRsp(&coder, &rsp) < 0) {
    goto end;
  }

  pJson = cJSON_CreateObject();
  RAW_NULL_CHECK(pJson);
  RAW_FALSE_CHECK(cJSON_AddStringToObject(pJson, "tmq_meta_version", TMQ_META_VERSION));
  cJSON* pMetaArr = cJSON_CreateArray();
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
    if (tDecodeMqMetaRsp(&metaCoder, &metaRsp) < 0) {
      goto end;
    }
    cJSON* pItem = NULL;
    processSimpleMeta(&metaRsp, &pItem);
    tDeleteMqMetaRsp(&metaRsp);
    RAW_FALSE_CHECK(cJSON_AddItemToArray(pMetaArr, pItem));
  }

  RAW_FALSE_CHECK(cJSON_AddItemToObject(pJson, "metas", pMetaArr));
  tDeleteMqBatchMetaRsp(&rsp);
  char* fullStr = cJSON_PrintUnformatted(pJson);
  cJSON_Delete(pJson);
  *string = fullStr;
  return;

end:
  cJSON_Delete(pJson);
  tDeleteMqBatchMetaRsp(&rsp);
}

char* tmq_get_json_meta(TAOS_RES* res) {
  if (res == NULL) return NULL;
  uDebug("tmq_get_json_meta res:%p", res);
  if (!TD_RES_TMQ_META(res) && !TD_RES_TMQ_METADATA(res) && !TD_RES_TMQ_BATCH_META(res)) {
    return NULL;
  }

  char*      string = NULL;
  SMqRspObj* rspObj = (SMqRspObj*)res;
  if (TD_RES_TMQ_METADATA(res)) {
    processAutoCreateTable(&rspObj->dataRsp, &string);
  } else if (TD_RES_TMQ_BATCH_META(res)) {
    processBatchMetaToJson(&rspObj->batchMetaRsp, &string);
  } else if (TD_RES_TMQ_META(res)) {
    cJSON* pJson = NULL;
    processSimpleMeta(&rspObj->metaRsp, &pJson);
    string = cJSON_PrintUnformatted(pJson);
    cJSON_Delete(pJson);
  } else {
    uError("tmq_get_json_meta res:%d, invalid type", *(int8_t*)res);
  }

  uDebug("tmq_get_json_meta string:%s", string);
  return string;
}

void tmq_free_json_meta(char* jsonMeta) { taosMemoryFreeClear(jsonMeta); }

static int32_t getOffSetLen(const SMqDataRsp* pRsp) {
  SEncoder coder = {0};
  tEncoderInit(&coder, NULL, 0);
  if (tEncodeSTqOffsetVal(&coder, &pRsp->reqOffset) < 0) return -1;
  if (tEncodeSTqOffsetVal(&coder, &pRsp->rspOffset) < 0) return -1;
  int32_t pos = coder.pos;
  tEncoderClear(&coder);
  return pos;
}

typedef int32_t __encode_func__(SEncoder* pEncoder, const SMqDataRsp* pRsp);
static int32_t  encodeMqDataRsp(__encode_func__* encodeFunc, SMqDataRsp* rspObj, tmq_raw_data* raw) {
   int32_t  len = 0;
   int32_t  code = 0;
   SEncoder encoder = {0};
   void*    buf = NULL;
   tEncodeSize(encodeFunc, rspObj, len, code);
   if (code < 0) {
     code = TSDB_CODE_INVALID_MSG;
     goto FAILED;
  }
   len += sizeof(int8_t) + sizeof(int32_t);
   buf = taosMemoryCalloc(1, len);
   if (buf == NULL) {
     code = terrno;
     goto FAILED;
  }
   tEncoderInit(&encoder, buf, len);
   if (tEncodeI8(&encoder, MQ_DATA_RSP_VERSION) < 0) {
     code = TSDB_CODE_INVALID_MSG;
     goto FAILED;
  }
   int32_t offsetLen = getOffSetLen(rspObj);
   if (offsetLen <= 0) {
     code = TSDB_CODE_INVALID_MSG;
     goto FAILED;
  }
   if (tEncodeI32(&encoder, offsetLen) < 0) {
     code = TSDB_CODE_INVALID_MSG;
     goto FAILED;
  }
   if (encodeFunc(&encoder, rspObj) < 0) {
     code = TSDB_CODE_INVALID_MSG;
     goto FAILED;
  }
   tEncoderClear(&encoder);

   raw->raw = buf;
   raw->raw_len = len;
   return code;
FAILED:
  tEncoderClear(&encoder);
  taosMemoryFree(buf);
  return code;
}

int32_t tmq_get_raw(TAOS_RES* res, tmq_raw_data* raw) {
  if (!raw || !res) {
    return TSDB_CODE_INVALID_PARA;
  }
  SMqRspObj* rspObj = ((SMqRspObj*)res);
  if (TD_RES_TMQ_META(res)) {
    raw->raw = rspObj->metaRsp.metaRsp;
    raw->raw_len = rspObj->metaRsp.metaRspLen;
    raw->raw_type = rspObj->metaRsp.resMsgType;
    uDebug("tmq get raw type meta:%p", raw);
  } else if (TD_RES_TMQ(res)) {
    int32_t code = encodeMqDataRsp(tEncodeMqDataRsp, &rspObj->dataRsp, raw);
    if (code != 0) {
      uError("tmq get raw type error:%d", terrno);
      return code;
    }
    raw->raw_type = RES_TYPE__TMQ;
    uDebug("tmq get raw type data:%p", raw);
  } else if (TD_RES_TMQ_METADATA(res)) {
    int32_t code = encodeMqDataRsp(tEncodeSTaosxRsp, &rspObj->dataRsp, raw);
    if (code != 0) {
      uError("tmq get raw type error:%d", terrno);
      return code;
    }
    raw->raw_type = RES_TYPE__TMQ_METADATA;
    uDebug("tmq get raw type metadata:%p", raw);
  } else if (TD_RES_TMQ_BATCH_META(res)) {
    raw->raw = rspObj->batchMetaRsp.pMetaBuff;
    raw->raw_len = rspObj->batchMetaRsp.metaBuffLen;
    raw->raw_type = rspObj->resType;
    uDebug("tmq get raw batch meta:%p", raw);
  } else {
    uError("tmq get raw error type:%d", *(int8_t*)res);
    return TSDB_CODE_TMQ_INVALID_MSG;
  }
  return TSDB_CODE_SUCCESS;
}

void tmq_free_raw(tmq_raw_data raw) {
  uDebug("tmq free raw data type:%d", raw.raw_type);
  if (raw.raw_type == RES_TYPE__TMQ || raw.raw_type == RES_TYPE__TMQ_METADATA) {
    taosMemoryFree(raw.raw);
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

  return writeRawImpl(taos, raw.raw, raw.raw_len, raw.raw_type);
}

static int32_t tmqWriteBatchMetaDataImpl(TAOS* taos, void* meta, int32_t metaLen) {
  if (taos == NULL || meta == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SMqBatchMetaRsp rsp = {0};
  SDecoder        coder = {0};
  int32_t         code = TSDB_CODE_SUCCESS;

  // decode and process req
  tDecoderInit(&coder, meta, metaLen);
  if (tDecodeMqBatchMetaRsp(&coder, &rsp) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }
  int32_t num = taosArrayGetSize(rsp.batchMetaReq);
  for (int32_t i = 0; i < num; i++) {
    int32_t* len = taosArrayGet(rsp.batchMetaLen, i);
    RAW_NULL_CHECK(len);
    void* tmpBuf = taosArrayGetP(rsp.batchMetaReq, i);
    RAW_NULL_CHECK(tmpBuf);
    SDecoder   metaCoder = {0};
    SMqMetaRsp metaRsp = {0};
    tDecoderInit(&metaCoder, POINTER_SHIFT(tmpBuf, sizeof(SMqRspHead)), *len - sizeof(SMqRspHead));
    if (tDecodeMqMetaRsp(&metaCoder, &metaRsp) < 0) {
      code = TSDB_CODE_INVALID_PARA;
      goto end;
    }
    code = writeRawImpl(taos, metaRsp.metaRsp, metaRsp.metaRspLen, metaRsp.resMsgType);
    tDeleteMqMetaRsp(&metaRsp);
    if (code != TSDB_CODE_SUCCESS) {
      goto end;
    }
  }

end:
  tDeleteMqBatchMetaRsp(&rsp);
  return code;
}
