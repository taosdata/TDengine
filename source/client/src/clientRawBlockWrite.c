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
#include "clientLog.h"
#include "parser.h"
#include "tdatablock.h"
#include "tdef.h"
#include "tglobal.h"
#include "tmsgtype.h"
#include "tqueue.h"
#include "tref.h"
#include "ttimer.h"

static tb_uid_t processSuid(tb_uid_t suid, char* db){
  return suid + MurmurHash3_32(db, strlen(db));
}

static char* buildCreateTableJson(SSchemaWrapper* schemaRow, SSchemaWrapper* schemaTag, char* name, int64_t id,
                                  int8_t t) {
  char*  string = NULL;
  cJSON* json = cJSON_CreateObject();
  if (json == NULL) {
    return NULL;
  }
  cJSON* type = cJSON_CreateString("create");
  cJSON_AddItemToObject(json, "type", type);

  //  char uid[32] = {0};
  //  sprintf(uid, "%"PRIi64, id);
  //  cJSON* id_ = cJSON_CreateString(uid);
  //  cJSON_AddItemToObject(json, "id", id_);
  cJSON* tableType = cJSON_CreateString(t == TSDB_NORMAL_TABLE ? "normal" : "super");
  cJSON_AddItemToObject(json, "tableType", tableType);
  cJSON* tableName = cJSON_CreateString(name);
  cJSON_AddItemToObject(json, "tableName", tableName);
  //  cJSON* version = cJSON_CreateNumber(1);
  //  cJSON_AddItemToObject(json, "version", version);

  cJSON* columns = cJSON_CreateArray();
  for (int i = 0; i < schemaRow->nCols; i++) {
    cJSON*   column = cJSON_CreateObject();
    SSchema* s = schemaRow->pSchema + i;
    cJSON*   cname = cJSON_CreateString(s->name);
    cJSON_AddItemToObject(column, "name", cname);
    cJSON* ctype = cJSON_CreateNumber(s->type);
    cJSON_AddItemToObject(column, "type", ctype);
    if (s->type == TSDB_DATA_TYPE_BINARY) {
      int32_t length = s->bytes - VARSTR_HEADER_SIZE;
      cJSON*  cbytes = cJSON_CreateNumber(length);
      cJSON_AddItemToObject(column, "length", cbytes);
    } else if (s->type == TSDB_DATA_TYPE_NCHAR) {
      int32_t length = (s->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
      cJSON*  cbytes = cJSON_CreateNumber(length);
      cJSON_AddItemToObject(column, "length", cbytes);
    }
    cJSON_AddItemToArray(columns, column);
  }
  cJSON_AddItemToObject(json, "columns", columns);

  cJSON* tags = cJSON_CreateArray();
  for (int i = 0; schemaTag && i < schemaTag->nCols; i++) {
    cJSON*   tag = cJSON_CreateObject();
    SSchema* s = schemaTag->pSchema + i;
    cJSON*   tname = cJSON_CreateString(s->name);
    cJSON_AddItemToObject(tag, "name", tname);
    cJSON* ttype = cJSON_CreateNumber(s->type);
    cJSON_AddItemToObject(tag, "type", ttype);
    if (s->type == TSDB_DATA_TYPE_BINARY) {
      int32_t length = s->bytes - VARSTR_HEADER_SIZE;
      cJSON*  cbytes = cJSON_CreateNumber(length);
      cJSON_AddItemToObject(tag, "length", cbytes);
    } else if (s->type == TSDB_DATA_TYPE_NCHAR) {
      int32_t length = (s->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
      cJSON*  cbytes = cJSON_CreateNumber(length);
      cJSON_AddItemToObject(tag, "length", cbytes);
    }
    cJSON_AddItemToArray(tags, tag);
  }
  cJSON_AddItemToObject(json, "tags", tags);

  string = cJSON_PrintUnformatted(json);
  cJSON_Delete(json);
  return string;
}

static char* buildAlterSTableJson(void* alterData, int32_t alterDataLen) {
  SMAlterStbReq req = {0};
  cJSON*        json = NULL;
  char*         string = NULL;

  if (tDeserializeSMAlterStbReq(alterData, alterDataLen, &req) != 0) {
    goto end;
  }

  json = cJSON_CreateObject();
  if (json == NULL) {
    goto end;
  }
  cJSON* type = cJSON_CreateString("alter");
  cJSON_AddItemToObject(json, "type", type);
  //  cJSON* uid = cJSON_CreateNumber(id);
  //  cJSON_AddItemToObject(json, "uid", uid);
  SName name = {0};
  tNameFromString(&name, req.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  cJSON* tableType = cJSON_CreateString("super");
  cJSON_AddItemToObject(json, "tableType", tableType);
  cJSON* tableName = cJSON_CreateString(name.tname);
  cJSON_AddItemToObject(json, "tableName", tableName);

  cJSON* alterType = cJSON_CreateNumber(req.alterType);
  cJSON_AddItemToObject(json, "alterType", alterType);
  switch (req.alterType) {
    case TSDB_ALTER_TABLE_ADD_TAG:
    case TSDB_ALTER_TABLE_ADD_COLUMN: {
      TAOS_FIELD* field = taosArrayGet(req.pFields, 0);
      cJSON*      colName = cJSON_CreateString(field->name);
      cJSON_AddItemToObject(json, "colName", colName);
      cJSON* colType = cJSON_CreateNumber(field->type);
      cJSON_AddItemToObject(json, "colType", colType);

      if (field->type == TSDB_DATA_TYPE_BINARY) {
        int32_t length = field->bytes - VARSTR_HEADER_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      } else if (field->type == TSDB_DATA_TYPE_NCHAR) {
        int32_t length = (field->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      }
      break;
    }
    case TSDB_ALTER_TABLE_DROP_TAG:
    case TSDB_ALTER_TABLE_DROP_COLUMN: {
      TAOS_FIELD* field = taosArrayGet(req.pFields, 0);
      cJSON*      colName = cJSON_CreateString(field->name);
      cJSON_AddItemToObject(json, "colName", colName);
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_TAG_BYTES:
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES: {
      TAOS_FIELD* field = taosArrayGet(req.pFields, 0);
      cJSON*      colName = cJSON_CreateString(field->name);
      cJSON_AddItemToObject(json, "colName", colName);
      cJSON* colType = cJSON_CreateNumber(field->type);
      cJSON_AddItemToObject(json, "colType", colType);
      if (field->type == TSDB_DATA_TYPE_BINARY) {
        int32_t length = field->bytes - VARSTR_HEADER_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      } else if (field->type == TSDB_DATA_TYPE_NCHAR) {
        int32_t length = (field->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      }
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_TAG_NAME:
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME: {
      TAOS_FIELD* oldField = taosArrayGet(req.pFields, 0);
      TAOS_FIELD* newField = taosArrayGet(req.pFields, 1);
      cJSON*      colName = cJSON_CreateString(oldField->name);
      cJSON_AddItemToObject(json, "colName", colName);
      cJSON* colNewName = cJSON_CreateString(newField->name);
      cJSON_AddItemToObject(json, "colNewName", colNewName);
      break;
    }
    default:
      break;
  }
  string = cJSON_PrintUnformatted(json);

  end:
  cJSON_Delete(json);
  tFreeSMAltertbReq(&req);
  return string;
}

static char* processCreateStb(SMqMetaRsp* metaRsp) {
  SVCreateStbReq req = {0};
  SDecoder       coder;
  char*          string = NULL;

  // decode and process req
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);

  if (tDecodeSVCreateStbReq(&coder, &req) < 0) {
    goto _err;
  }
  string = buildCreateTableJson(&req.schemaRow, &req.schemaTag, req.name, req.suid, TSDB_SUPER_TABLE);

  _err:
  tDecoderClear(&coder);
  return string;
}

static char* processAlterStb(SMqMetaRsp* metaRsp) {
  SVCreateStbReq req = {0};
  SDecoder       coder;
  char*          string = NULL;

  // decode and process req
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);

  if (tDecodeSVCreateStbReq(&coder, &req) < 0) {
    goto _err;
  }
  string = buildAlterSTableJson(req.alterOriData, req.alterOriDataLen);

  _err:
  tDecoderClear(&coder);
  return string;
}

static void buildChildElement(cJSON* json, SVCreateTbReq* pCreateReq) {
  STag*   pTag = (STag*)pCreateReq->ctb.pTag;
  char*   sname = pCreateReq->ctb.stbName;
  char*   name = pCreateReq->name;
  SArray* tagName = pCreateReq->ctb.tagName;
  int64_t id = pCreateReq->uid;
  uint8_t tagNum = pCreateReq->ctb.tagNum;

  cJSON* tableName = cJSON_CreateString(name);
  cJSON_AddItemToObject(json, "tableName", tableName);
  cJSON* using = cJSON_CreateString(sname);
  cJSON_AddItemToObject(json, "using", using);
  cJSON* tagNumJson = cJSON_CreateNumber(tagNum);
  cJSON_AddItemToObject(json, "tagNum", tagNumJson);
  //  cJSON* version = cJSON_CreateNumber(1);
  //  cJSON_AddItemToObject(json, "version", version);

  cJSON*  tags = cJSON_CreateArray();
  SArray* pTagVals = NULL;
  int32_t code = tTagToValArray(pTag, &pTagVals);
  if (code) {
    goto end;
  }

  if (tTagIsJson(pTag)) {
    STag* p = (STag*)pTag;
    if (p->nTag == 0) {
      goto end;
    }
    char*    pJson = parseTagDatatoJson(pTag);
    cJSON*   tag = cJSON_CreateObject();
    STagVal* pTagVal = taosArrayGet(pTagVals, 0);

    char*  ptname = taosArrayGet(tagName, 0);
    cJSON* tname = cJSON_CreateString(ptname);
    cJSON_AddItemToObject(tag, "name", tname);
    //    cJSON* cid_ = cJSON_CreateString("");
    //    cJSON_AddItemToObject(tag, "cid", cid_);
    cJSON* ttype = cJSON_CreateNumber(TSDB_DATA_TYPE_JSON);
    cJSON_AddItemToObject(tag, "type", ttype);
    cJSON* tvalue = cJSON_CreateString(pJson);
    cJSON_AddItemToObject(tag, "value", tvalue);
    cJSON_AddItemToArray(tags, tag);
    taosMemoryFree(pJson);
    goto end;
  }

  for (int i = 0; i < taosArrayGetSize(pTagVals); i++) {
    STagVal* pTagVal = (STagVal*)taosArrayGet(pTagVals, i);

    cJSON* tag = cJSON_CreateObject();

    char*  ptname = taosArrayGet(tagName, i);
    cJSON* tname = cJSON_CreateString(ptname);
    cJSON_AddItemToObject(tag, "name", tname);
    //    cJSON* cid = cJSON_CreateNumber(pTagVal->cid);
    //    cJSON_AddItemToObject(tag, "cid", cid);
    cJSON* ttype = cJSON_CreateNumber(pTagVal->type);
    cJSON_AddItemToObject(tag, "type", ttype);

    cJSON* tvalue = NULL;
    if (IS_VAR_DATA_TYPE(pTagVal->type)) {
      char* buf = taosMemoryCalloc(pTagVal->nData + 3, 1);
      if (!buf) goto end;
      dataConverToStr(buf, pTagVal->type, pTagVal->pData, pTagVal->nData, NULL);
      tvalue = cJSON_CreateString(buf);
      taosMemoryFree(buf);
    } else {
      double val = 0;
      GET_TYPED_DATA(val, double, pTagVal->type, &pTagVal->i64);
      tvalue = cJSON_CreateNumber(val);
    }

    cJSON_AddItemToObject(tag, "value", tvalue);
    cJSON_AddItemToArray(tags, tag);
  }

  end:
  cJSON_AddItemToObject(json, "tags", tags);
  taosArrayDestroy(pTagVals);
}

static char* buildCreateCTableJson(SVCreateTbReq* pCreateReq, int32_t nReqs) {
  char*  string = NULL;
  cJSON* json = cJSON_CreateObject();
  if (json == NULL) {
    return NULL;
  }
  cJSON* type = cJSON_CreateString("create");
  cJSON_AddItemToObject(json, "type", type);
  //  char cid[32] = {0};
  //  sprintf(cid, "%"PRIi64, id);
  //  cJSON* cid_ = cJSON_CreateString(cid);
  //  cJSON_AddItemToObject(json, "id", cid_);

  cJSON* tableType = cJSON_CreateString("child");
  cJSON_AddItemToObject(json, "tableType", tableType);

  buildChildElement(json, pCreateReq);
  cJSON* createList = cJSON_CreateArray();
  for (int i = 0; nReqs > 1 && i < nReqs; i++) {
    cJSON* create = cJSON_CreateObject();
    buildChildElement(create, pCreateReq + i);
    cJSON_AddItemToArray(createList, create);
  }
  cJSON_AddItemToObject(json, "createList", createList);
  string = cJSON_PrintUnformatted(json);
  cJSON_Delete(json);
  return string;
}

static char* processCreateTable(SMqMetaRsp* metaRsp) {
  SDecoder           decoder = {0};
  SVCreateTbBatchReq req = {0};
  SVCreateTbReq*     pCreateReq;
  char*              string = NULL;
  // decode
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  if (tDecodeSVCreateTbBatchReq(&decoder, &req) < 0) {
    goto _exit;
  }

  // loop to create table
  if (req.nReqs > 0) {
    pCreateReq = req.pReqs;
    if (pCreateReq->type == TSDB_CHILD_TABLE) {
      string = buildCreateCTableJson(req.pReqs, req.nReqs);
    } else if (pCreateReq->type == TSDB_NORMAL_TABLE) {
      string =
          buildCreateTableJson(&pCreateReq->ntb.schemaRow, NULL, pCreateReq->name, pCreateReq->uid, TSDB_NORMAL_TABLE);
    }
  }

  _exit:
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pCreateReq = req.pReqs + iReq;
    taosMemoryFreeClear(pCreateReq->comment);
    if (pCreateReq->type == TSDB_CHILD_TABLE) {
      taosArrayDestroy(pCreateReq->ctb.tagName);
    }
  }
  tDecoderClear(&decoder);
  return string;
}

static char* processAutoCreateTable(STaosxRsp* rsp) {
  ASSERT(rsp->createTableNum != 0);

  SDecoder*      decoder = taosMemoryCalloc(rsp->createTableNum, sizeof(SDecoder));
  SVCreateTbReq* pCreateReq = taosMemoryCalloc(rsp->createTableNum, sizeof(SVCreateTbReq));
  char*          string = NULL;

  // loop to create table
  for (int32_t iReq = 0; iReq < rsp->createTableNum; iReq++) {
    // decode
    void**   data = taosArrayGet(rsp->createTableReq, iReq);
    int32_t* len = taosArrayGet(rsp->createTableLen, iReq);
    tDecoderInit(&decoder[iReq], *data, *len);
    if (tDecodeSVCreateTbReq(&decoder[iReq], pCreateReq + iReq) < 0) {
      goto _exit;
    }

    ASSERT(pCreateReq[iReq].type == TSDB_CHILD_TABLE);
  }
  string = buildCreateCTableJson(pCreateReq, rsp->createTableNum);

  _exit:
  for (int i = 0; i < rsp->createTableNum; i++) {
    tDecoderClear(&decoder[i]);
    taosMemoryFreeClear(pCreateReq[i].comment);
    if (pCreateReq[i].type == TSDB_CHILD_TABLE) {
      taosArrayDestroy(pCreateReq[i].ctb.tagName);
    }
  }
  taosMemoryFree(decoder);
  taosMemoryFree(pCreateReq);
  return string;
}

static char* processAlterTable(SMqMetaRsp* metaRsp) {
  SDecoder     decoder = {0};
  SVAlterTbReq vAlterTbReq = {0};
  char*        string = NULL;
  cJSON*       json = NULL;

  // decode
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  if (tDecodeSVAlterTbReq(&decoder, &vAlterTbReq) < 0) {
    goto _exit;
  }

  json = cJSON_CreateObject();
  if (json == NULL) {
    goto _exit;
  }
  cJSON* type = cJSON_CreateString("alter");
  cJSON_AddItemToObject(json, "type", type);
  //  cJSON* uid = cJSON_CreateNumber(id);
  //  cJSON_AddItemToObject(json, "uid", uid);
  cJSON* tableType = cJSON_CreateString(vAlterTbReq.action == TSDB_ALTER_TABLE_UPDATE_TAG_VAL ? "child" : "normal");
  cJSON_AddItemToObject(json, "tableType", tableType);
  cJSON* tableName = cJSON_CreateString(vAlterTbReq.tbName);
  cJSON_AddItemToObject(json, "tableName", tableName);
  cJSON* alterType = cJSON_CreateNumber(vAlterTbReq.action);
  cJSON_AddItemToObject(json, "alterType", alterType);

  switch (vAlterTbReq.action) {
    case TSDB_ALTER_TABLE_ADD_COLUMN: {
      cJSON* colName = cJSON_CreateString(vAlterTbReq.colName);
      cJSON_AddItemToObject(json, "colName", colName);
      cJSON* colType = cJSON_CreateNumber(vAlterTbReq.type);
      cJSON_AddItemToObject(json, "colType", colType);

      if (vAlterTbReq.type == TSDB_DATA_TYPE_BINARY) {
        int32_t length = vAlterTbReq.bytes - VARSTR_HEADER_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      } else if (vAlterTbReq.type == TSDB_DATA_TYPE_NCHAR) {
        int32_t length = (vAlterTbReq.bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      }
      break;
    }
    case TSDB_ALTER_TABLE_DROP_COLUMN: {
      cJSON* colName = cJSON_CreateString(vAlterTbReq.colName);
      cJSON_AddItemToObject(json, "colName", colName);
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES: {
      cJSON* colName = cJSON_CreateString(vAlterTbReq.colName);
      cJSON_AddItemToObject(json, "colName", colName);
      cJSON* colType = cJSON_CreateNumber(vAlterTbReq.colModType);
      cJSON_AddItemToObject(json, "colType", colType);
      if (vAlterTbReq.colModType == TSDB_DATA_TYPE_BINARY) {
        int32_t length = vAlterTbReq.colModBytes - VARSTR_HEADER_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      } else if (vAlterTbReq.colModType == TSDB_DATA_TYPE_NCHAR) {
        int32_t length = (vAlterTbReq.colModBytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
        cJSON*  cbytes = cJSON_CreateNumber(length);
        cJSON_AddItemToObject(json, "colLength", cbytes);
      }
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME: {
      cJSON* colName = cJSON_CreateString(vAlterTbReq.colName);
      cJSON_AddItemToObject(json, "colName", colName);
      cJSON* colNewName = cJSON_CreateString(vAlterTbReq.colNewName);
      cJSON_AddItemToObject(json, "colNewName", colNewName);
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_TAG_VAL: {
      cJSON* tagName = cJSON_CreateString(vAlterTbReq.tagName);
      cJSON_AddItemToObject(json, "colName", tagName);

      bool isNull = vAlterTbReq.isNull;
      if (vAlterTbReq.tagType == TSDB_DATA_TYPE_JSON) {
        STag* jsonTag = (STag*)vAlterTbReq.pTagVal;
        if (jsonTag->nTag == 0) isNull = true;
      }
      if (!isNull) {
        char* buf = NULL;

        if (vAlterTbReq.tagType == TSDB_DATA_TYPE_JSON) {
          ASSERT(tTagIsJson(vAlterTbReq.pTagVal) == true);
          buf = parseTagDatatoJson(vAlterTbReq.pTagVal);
        } else {
          buf = taosMemoryCalloc(vAlterTbReq.nTagVal + 1, 1);
          dataConverToStr(buf, vAlterTbReq.tagType, vAlterTbReq.pTagVal, vAlterTbReq.nTagVal, NULL);
        }

        cJSON* colValue = cJSON_CreateString(buf);
        cJSON_AddItemToObject(json, "colValue", colValue);
        taosMemoryFree(buf);
      }

      cJSON* isNullCJson = cJSON_CreateBool(isNull);
      cJSON_AddItemToObject(json, "colValueNull", isNullCJson);
      break;
    }
    default:
      break;
  }
  string = cJSON_PrintUnformatted(json);

  _exit:
  cJSON_Delete(json);
  tDecoderClear(&decoder);
  return string;
}

static char* processDropSTable(SMqMetaRsp* metaRsp) {
  SDecoder     decoder = {0};
  SVDropStbReq req = {0};
  char*        string = NULL;
  cJSON*       json = NULL;

  // decode
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  if (tDecodeSVDropStbReq(&decoder, &req) < 0) {
    goto _exit;
  }

  json = cJSON_CreateObject();
  if (json == NULL) {
    goto _exit;
  }
  cJSON* type = cJSON_CreateString("drop");
  cJSON_AddItemToObject(json, "type", type);
  cJSON* tableType = cJSON_CreateString("super");
  cJSON_AddItemToObject(json, "tableType", tableType);
  cJSON* tableName = cJSON_CreateString(req.name);
  cJSON_AddItemToObject(json, "tableName", tableName);

  string = cJSON_PrintUnformatted(json);

  _exit:
  cJSON_Delete(json);
  tDecoderClear(&decoder);
  return string;
}
static char* processDeleteTable(SMqMetaRsp* metaRsp){
  SDeleteRes req = {0};
  SDecoder   coder = {0};
  int32_t    code = TSDB_CODE_SUCCESS;
  cJSON*     json = NULL;
  char*      string = NULL;

  // decode and process req
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);

  tDecoderInit(&coder, data, len);
  if (tDecodeDeleteRes(&coder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto _exit;
  }

  //  getTbName(req.tableFName);
  char sql[256] = {0};
  snprintf(sql, sizeof(sql), "delete from `%s` where `%s` >= %" PRId64 " and `%s` <= %" PRId64, req.tableFName,
           req.tsColName, req.skey, req.tsColName, req.ekey);
  uDebug("delete sql:%s\n", sql);

  json = cJSON_CreateObject();
  if (json == NULL) {
    goto _exit;
  }
  cJSON* type = cJSON_CreateString("delete");
  cJSON_AddItemToObject(json, "type", type);
  cJSON* sqlJson = cJSON_CreateString(sql);
  cJSON_AddItemToObject(json, "sql", sqlJson);

  string = cJSON_PrintUnformatted(json);

  _exit:
  cJSON_Delete(json);
  tDecoderClear(&coder);
  return string;
}

static char* processDropTable(SMqMetaRsp* metaRsp) {
  SDecoder         decoder = {0};
  SVDropTbBatchReq req = {0};
  char*            string = NULL;
  cJSON*           json = NULL;

  // decode
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  if (tDecodeSVDropTbBatchReq(&decoder, &req) < 0) {
    goto _exit;
  }

  json = cJSON_CreateObject();
  if (json == NULL) {
    goto _exit;
  }
  cJSON* type = cJSON_CreateString("drop");
  cJSON_AddItemToObject(json, "type", type);
  //  cJSON* uid = cJSON_CreateNumber(id);
  //  cJSON_AddItemToObject(json, "uid", uid);
  //  cJSON* tableType = cJSON_CreateString("normal");
  //  cJSON_AddItemToObject(json, "tableType", tableType);

  cJSON* tableNameList = cJSON_CreateArray();
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    SVDropTbReq* pDropTbReq = req.pReqs + iReq;

    cJSON* tableName = cJSON_CreateString(pDropTbReq->name);
    cJSON_AddItemToArray(tableNameList, tableName);
  }
  cJSON_AddItemToObject(json, "tableNameList", tableNameList);

  string = cJSON_PrintUnformatted(json);

  _exit:
  cJSON_Delete(json);
  tDecoderClear(&decoder);
  return string;
}

static int32_t taosCreateStb(TAOS* taos, void* meta, int32_t metaLen) {
  SVCreateStbReq req = {0};
  SDecoder       coder;
  SMCreateStbReq pReq = {0};
  int32_t        code = TSDB_CODE_SUCCESS;
  SRequestObj*   pRequest = NULL;

  code = buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

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
  // build create stable
  pReq.pColumns = taosArrayInit(req.schemaRow.nCols, sizeof(SField));
  for (int32_t i = 0; i < req.schemaRow.nCols; i++) {
    SSchema* pSchema = req.schemaRow.pSchema + i;
    SField   field = {.type = pSchema->type, .bytes = pSchema->bytes};
    strcpy(field.name, pSchema->name);
    taosArrayPush(pReq.pColumns, &field);
  }
  pReq.pTags = taosArrayInit(req.schemaTag.nCols, sizeof(SField));
  for (int32_t i = 0; i < req.schemaTag.nCols; i++) {
    SSchema* pSchema = req.schemaTag.pSchema + i;
    SField   field = {.type = pSchema->type, .bytes = pSchema->bytes};
    strcpy(field.name, pSchema->name);
    taosArrayPush(pReq.pTags, &field);
  }

  pReq.colVer = req.schemaRow.version;
  pReq.tagVer = req.schemaTag.version;
  pReq.numOfColumns = req.schemaRow.nCols;
  pReq.numOfTags = req.schemaTag.nCols;
  pReq.commentLen = -1;
  pReq.suid = processSuid(req.suid, pRequest->pDb);
  pReq.source = TD_REQ_FROM_TAOX;
  pReq.igExists = true;

  STscObj* pTscObj = pRequest->pTscObj;
  SName    tableName;
  tNameExtractFullName(toName(pTscObj->acctId, pRequest->pDb, req.name, &tableName), pReq.name);

  SCmdMsgInfo pCmdMsg = {0};
  pCmdMsg.epSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);
  pCmdMsg.msgType = TDMT_MND_CREATE_STB;
  pCmdMsg.msgLen = tSerializeSMCreateStbReq(NULL, 0, &pReq);
  pCmdMsg.pMsg = taosMemoryMalloc(pCmdMsg.msgLen);
  if (NULL == pCmdMsg.pMsg) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  tSerializeSMCreateStbReq(pCmdMsg.pMsg, pCmdMsg.msgLen, &pReq);

  SQuery pQuery = {0};
  pQuery.execMode = QUERY_EXEC_MODE_RPC;
  pQuery.pCmdMsg = &pCmdMsg;
  pQuery.msgType = pQuery.pCmdMsg->msgType;
  pQuery.stableQuery = true;

  launchQueryImpl(pRequest, &pQuery, true, NULL);

  if (pRequest->code == TSDB_CODE_SUCCESS) {
    SCatalog* pCatalog = NULL;
    catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog);
    catalogRemoveTableMeta(pCatalog, &tableName);
  }

  code = pRequest->code;
  taosMemoryFree(pCmdMsg.pMsg);

  end:
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

  code = buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

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

  // build drop stable
  pReq.igNotExists = true;
  pReq.source = TD_REQ_FROM_TAOX;
  pReq.suid = processSuid(req.suid, pRequest->pDb);

  STscObj* pTscObj = pRequest->pTscObj;
  SName    tableName = {0};
  tNameExtractFullName(toName(pTscObj->acctId, pRequest->pDb, req.name, &tableName), pReq.name);

  SCmdMsgInfo pCmdMsg = {0};
  pCmdMsg.epSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);
  pCmdMsg.msgType = TDMT_MND_DROP_STB;
  pCmdMsg.msgLen = tSerializeSMDropStbReq(NULL, 0, &pReq);
  pCmdMsg.pMsg = taosMemoryMalloc(pCmdMsg.msgLen);
  if (NULL == pCmdMsg.pMsg) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  tSerializeSMDropStbReq(pCmdMsg.pMsg, pCmdMsg.msgLen, &pReq);

  SQuery pQuery = {0};
  pQuery.execMode = QUERY_EXEC_MODE_RPC;
  pQuery.pCmdMsg = &pCmdMsg;
  pQuery.msgType = pQuery.pCmdMsg->msgType;
  pQuery.stableQuery = true;

  launchQueryImpl(pRequest, &pQuery, true, NULL);

  if (pRequest->code == TSDB_CODE_SUCCESS) {
    SCatalog* pCatalog = NULL;
    catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog);
    catalogRemoveTableMeta(pCatalog, &tableName);
  }

  code = pRequest->code;
  taosMemoryFree(pCmdMsg.pMsg);

  end:
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

  code = buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

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
  code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  pVgroupHashmap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (NULL == pVgroupHashmap) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  taosHashSetFreeFp(pVgroupHashmap, destroyCreateTbReqBatch);

  SRequestConnInfo conn = {.pTrans = pTscObj->pAppInfo->pTransporter,
      .requestId = pRequest->requestId,
      .requestObjRefId = pRequest->self,
      .mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp)};

  pRequest->tableList = taosArrayInit(req.nReqs, sizeof(SName));
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
    taosArrayPush(pRequest->tableList, &pName);

    pCreateReq->flags |= TD_CREATE_IF_NOT_EXISTS;
    // change tag cid to new cid
    if (pCreateReq->type == TSDB_CHILD_TABLE) {
      STableMeta* pTableMeta = NULL;
      SName       sName = {0};
      pCreateReq->ctb.suid = processSuid(pCreateReq->ctb.suid, pRequest->pDb);
      toName(pTscObj->acctId, pRequest->pDb, pCreateReq->ctb.stbName, &sName);
      code = catalogGetTableMeta(pCatalog, &conn, &sName, &pTableMeta);
      if (code != TSDB_CODE_SUCCESS) {
        uError("taosCreateTable:catalogGetTableMeta failed. table name: %s", pCreateReq->ctb.stbName);
        goto end;
      }

      for (int32_t i = 0; i < taosArrayGetSize(pCreateReq->ctb.tagName); i++) {
        char* tName = taosArrayGet(pCreateReq->ctb.tagName, i);
        for (int32_t j = pTableMeta->tableInfo.numOfColumns;
             j < pTableMeta->tableInfo.numOfColumns + pTableMeta->tableInfo.numOfTags; j++) {
          SSchema* tag = &pTableMeta->schema[j];
          if (strcmp(tag->name, tName) == 0 && tag->type != TSDB_DATA_TYPE_JSON) {
            tTagSetCid((STag*)pCreateReq->ctb.pTag, i, tag->colId);
          }
        }
      }
      taosMemoryFreeClear(pTableMeta);
    }

    SVgroupCreateTableBatch* pTableBatch = taosHashGet(pVgroupHashmap, &pInfo.vgId, sizeof(pInfo.vgId));
    if (pTableBatch == NULL) {
      SVgroupCreateTableBatch tBatch = {0};
      tBatch.info = pInfo;
      strcpy(tBatch.dbName, pRequest->pDb);

      tBatch.req.pArray = taosArrayInit(4, sizeof(struct SVCreateTbReq));
      taosArrayPush(tBatch.req.pArray, pCreateReq);

      taosHashPut(pVgroupHashmap, &pInfo.vgId, sizeof(pInfo.vgId), &tBatch, sizeof(tBatch));
    } else {  // add to the correct vgroup
      taosArrayPush(pTableBatch->req.pArray, pCreateReq);
    }
  }

  SArray* pBufArray = serializeVgroupsCreateTableBatch(pVgroupHashmap);
  if (NULL == pBufArray) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }

  pQuery = (SQuery*)nodesMakeNode(QUERY_NODE_QUERY);
  pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  pQuery->msgType = TDMT_VND_CREATE_TABLE;
  pQuery->stableQuery = false;
  pQuery->pRoot = nodesMakeNode(QUERY_NODE_CREATE_TABLE_STMT);

  code = rewriteToVnodeModifyOpStmt(pQuery, pBufArray);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  launchQueryImpl(pRequest, pQuery, true, NULL);
  if (pRequest->code == TSDB_CODE_SUCCESS) {
    removeMeta(pTscObj, pRequest->tableList);
  }

  code = pRequest->code;

  end:
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pCreateReq = req.pReqs + iReq;
    taosMemoryFreeClear(pCreateReq->comment);
    if (pCreateReq->type == TSDB_CHILD_TABLE) {
      taosArrayDestroy(pCreateReq->ctb.tagName);
    }
  }

  taosHashCleanup(pVgroupHashmap);
  destroyRequest(pRequest);
  tDecoderClear(&coder);
  qDestroyQuery(pQuery);
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

  code = buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }
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
  code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  pVgroupHashmap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (NULL == pVgroupHashmap) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  taosHashSetFreeFp(pVgroupHashmap, destroyDropTbReqBatch);

  SRequestConnInfo conn = {.pTrans = pTscObj->pAppInfo->pTransporter,
      .requestId = pRequest->requestId,
      .requestObjRefId = pRequest->self,
      .mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp)};
  pRequest->tableList = taosArrayInit(req.nReqs, sizeof(SName));
  // loop to create table
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pDropReq = req.pReqs + iReq;
    pDropReq->igNotExists = true;
    pDropReq->suid = processSuid(pDropReq->suid, pRequest->pDb);

    SVgroupInfo pInfo = {0};
    SName       pName = {0};
    toName(pTscObj->acctId, pRequest->pDb, pDropReq->name, &pName);
    code = catalogGetTableHashVgroup(pCatalog, &conn, &pName, &pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      goto end;
    }

    taosArrayPush(pRequest->tableList, &pName);
    SVgroupDropTableBatch* pTableBatch = taosHashGet(pVgroupHashmap, &pInfo.vgId, sizeof(pInfo.vgId));
    if (pTableBatch == NULL) {
      SVgroupDropTableBatch tBatch = {0};
      tBatch.info = pInfo;
      tBatch.req.pArray = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SVDropTbReq));
      taosArrayPush(tBatch.req.pArray, pDropReq);

      taosHashPut(pVgroupHashmap, &pInfo.vgId, sizeof(pInfo.vgId), &tBatch, sizeof(tBatch));
    } else {  // add to the correct vgroup
      taosArrayPush(pTableBatch->req.pArray, pDropReq);
    }
  }

  SArray* pBufArray = serializeVgroupsDropTableBatch(pVgroupHashmap);
  if (NULL == pBufArray) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }

  pQuery = (SQuery*)nodesMakeNode(QUERY_NODE_QUERY);
  pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  pQuery->msgType = TDMT_VND_DROP_TABLE;
  pQuery->stableQuery = false;
  pQuery->pRoot = nodesMakeNode(QUERY_NODE_DROP_TABLE_STMT);

  code = rewriteToVnodeModifyOpStmt(pQuery, pBufArray);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  launchQueryImpl(pRequest, pQuery, true, NULL);
  if (pRequest->code == TSDB_CODE_SUCCESS) {
    removeMeta(pTscObj, pRequest->tableList);
  }
  code = pRequest->code;

  end:
  taosHashCleanup(pVgroupHashmap);
  destroyRequest(pRequest);
  tDecoderClear(&coder);
  qDestroyQuery(pQuery);
  return code;
}

// delete from db.tabl where ..       -> delete from tabl where ..
// delete from db    .tabl where ..   -> delete from tabl where ..
// static void getTbName(char *sql){
//  char *ch = sql;
//
//  bool inBackQuote = false;
//  int8_t dotIndex = 0;
//  while(*ch != '\0'){
//    if(!inBackQuote && *ch == '`'){
//      inBackQuote = true;
//      ch++;
//      continue;
//    }
//
//    if(inBackQuote && *ch == '`'){
//      inBackQuote = false;
//      ch++;
//
//      continue;
//    }
//
//    if(!inBackQuote && *ch == '.'){
//      dotIndex ++;
//      if(dotIndex == 2){
//        memmove(sql, ch + 1, strlen(ch + 1) + 1);
//        break;
//      }
//    }
//    ch++;
//  }
//}

static int32_t taosDeleteData(TAOS* taos, void* meta, int32_t metaLen) {
  SDeleteRes req = {0};
  SDecoder   coder = {0};
  int32_t    code = TSDB_CODE_SUCCESS;

  // decode and process req
  void*   data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  int32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  if (tDecodeDeleteRes(&coder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

  //  getTbName(req.tableFName);
  char sql[256] = {0};
  snprintf(sql, sizeof(sql), "delete from `%s` where `%s` >= %" PRId64 " and `%s` <= %" PRId64, req.tableFName,
           req.tsColName, req.skey, req.tsColName, req.ekey);
  uDebug("delete sql:%s\n", sql);

  TAOS_RES*    res = taos_query(taos, sql);
  SRequestObj* pRequest = (SRequestObj*)res;
  code = pRequest->code;
  if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
    code = TSDB_CODE_SUCCESS;
  }
  taos_free_result(res);

  end:
  tDecoderClear(&coder);
  return code;
}

static int32_t taosAlterTable(TAOS* taos, void* meta, int32_t metaLen) {
  SVAlterTbReq   req = {0};
  SDecoder       coder = {0};
  int32_t        code = TSDB_CODE_SUCCESS;
  SRequestObj*   pRequest = NULL;
  SQuery*        pQuery = NULL;
  SArray*        pArray = NULL;
  SVgDataBlocks* pVgData = NULL;

  code = buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0);

  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }
  // decode and process req
  void*   data = POINTER_SHIFT(meta, sizeof(SMsgHead));
  int32_t len = metaLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);
  if (tDecodeSVAlterTbReq(&coder, &req) < 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }

  // do not deal TSDB_ALTER_TABLE_UPDATE_OPTIONS
  if (req.action == TSDB_ALTER_TABLE_UPDATE_OPTIONS) {
    goto end;
  }

  STscObj*  pTscObj = pRequest->pTscObj;
  SCatalog* pCatalog = NULL;
  code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  SRequestConnInfo conn = {.pTrans = pTscObj->pAppInfo->pTransporter,
      .requestId = pRequest->requestId,
      .requestObjRefId = pRequest->self,
      .mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp)};

  SVgroupInfo pInfo = {0};
  SName       pName = {0};
  toName(pTscObj->acctId, pRequest->pDb, req.tbName, &pName);
  code = catalogGetTableHashVgroup(pCatalog, &conn, &pName, &pInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  pArray = taosArrayInit(1, sizeof(void*));
  if (NULL == pArray) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }

  pVgData = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
  if (NULL == pVgData) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  pVgData->vg = pInfo;
  pVgData->pData = taosMemoryMalloc(metaLen);
  if (NULL == pVgData->pData) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  memcpy(pVgData->pData, meta, metaLen);
  ((SMsgHead*)pVgData->pData)->vgId = htonl(pInfo.vgId);
  pVgData->size = metaLen;
  pVgData->numOfTables = 1;
  taosArrayPush(pArray, &pVgData);

  pQuery = (SQuery*)nodesMakeNode(QUERY_NODE_QUERY);
  pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  pQuery->msgType = TDMT_VND_ALTER_TABLE;
  pQuery->stableQuery = false;
  pQuery->pRoot = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);

  code = rewriteToVnodeModifyOpStmt(pQuery, pArray);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

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
  taosArrayDestroy(pArray);
  if (pVgData) taosMemoryFreeClear(pVgData->pData);
  taosMemoryFreeClear(pVgData);
  destroyRequest(pRequest);
  tDecoderClear(&coder);
  qDestroyQuery(pQuery);
  return code;
}

typedef struct {
  SVgroupInfo vg;
  void*       data;
} VgData;

static void destroyVgHash(void* data) {
  VgData* vgData = (VgData*)data;
  taosMemoryFreeClear(vgData->data);
}

int taos_write_raw_block_with_fields(TAOS* taos, int rows, char* pData, const char* tbname, TAOS_FIELD *fields, int numFields){
  int32_t     code = TSDB_CODE_SUCCESS;
  STableMeta* pTableMeta = NULL;
  SQuery*     pQuery = NULL;
  SSubmitReq* subReq = NULL;

  SRequestObj* pRequest = (SRequestObj*)createRequest(*(int64_t*)taos, TSDB_SQL_INSERT, 0);
  if (!pRequest) {
    uError("WriteRaw:createRequest error request is null");
    code = terrno;
    goto end;
  }

  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
    uError("WriteRaw:not use db");
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }

  SName pName = {TSDB_TABLE_NAME_T, pRequest->pTscObj->acctId, {0}, {0}};
  tstrncpy(pName.dbname, pRequest->pDb, sizeof(pName.dbname));
  tstrncpy(pName.tname, tbname, sizeof(pName.tname));

  struct SCatalog* pCatalog = NULL;
  code = catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    uError("WriteRaw: get gatlog error");
    goto end;
  }

  SRequestConnInfo conn = {0};
  conn.pTrans = pRequest->pTscObj->pAppInfo->pTransporter;
  conn.requestId = pRequest->requestId;
  conn.requestObjRefId = pRequest->self;
  conn.mgmtEps = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp);

  SVgroupInfo vgData = {0};
  code = catalogGetTableHashVgroup(pCatalog, &conn, &pName, &vgData);
  if (code != TSDB_CODE_SUCCESS) {
    uError("WriteRaw:catalogGetTableHashVgroup failed. table name: %s", tbname);
    goto end;
  }

  code = catalogGetTableMeta(pCatalog, &conn, &pName, &pTableMeta);
  if (code != TSDB_CODE_SUCCESS) {
    uError("WriteRaw:catalogGetTableMeta failed. table name: %s", tbname);
    goto end;
  }
  uint64_t suid = (TSDB_NORMAL_TABLE == pTableMeta->tableType ? 0 : pTableMeta->suid);
  uint64_t uid = pTableMeta->uid;
  int32_t  numOfCols = pTableMeta->tableInfo.numOfColumns;

  uint16_t fLen = 0;
  int32_t  rowSize = 0;
  int16_t  nVar = 0;
  for (int i = 0; i < pTableMeta->tableInfo.numOfColumns; i++) {
    SSchema* schema = pTableMeta->schema + i;
    fLen += TYPE_BYTES[schema->type];
    rowSize += schema->bytes;
    if (IS_VAR_DATA_TYPE(schema->type)) {
      nVar++;
    }
  }

  fLen -= sizeof(TSKEY);

  int32_t extendedRowSize = rowSize + TD_ROW_HEAD_LEN - sizeof(TSKEY) + nVar * sizeof(VarDataOffsetT) +
                            (int32_t)TD_BITMAP_BYTES(numOfCols - 1);
  int32_t schemaLen = 0;
  int32_t submitLen = sizeof(SSubmitBlk) + schemaLen + rows * extendedRowSize;

  int32_t totalLen = sizeof(SSubmitReq) + submitLen;
  subReq = taosMemoryCalloc(1, totalLen);
  SSubmitBlk* blk = POINTER_SHIFT(subReq, sizeof(SSubmitReq));
  void*       blkSchema = POINTER_SHIFT(blk, sizeof(SSubmitBlk));
  STSRow*     rowData = POINTER_SHIFT(blkSchema, schemaLen);

  SRowBuilder rb = {0};
  tdSRowInit(&rb, pTableMeta->sversion);
  tdSRowSetTpInfo(&rb, numOfCols, fLen);
  int32_t dataLen = 0;

  // | version | total length | total rows | total columns | flag seg| block group id | column schema | each column length |
  char*    pStart = pData + getVersion1BlockMetaSize(pData, numFields);
  int32_t* colLength = (int32_t*)pStart;
  pStart += sizeof(int32_t) * numFields;

  SResultColumn* pCol = taosMemoryCalloc(numFields, sizeof(SResultColumn));

  for (int32_t i = 0; i < numFields; ++i) {
    if (IS_VAR_DATA_TYPE(fields[i].type)) {
      pCol[i].offset = (int32_t*)pStart;
      pStart += rows * sizeof(int32_t);
    } else {
      pCol[i].nullbitmap = pStart;
      pStart += BitmapLen(rows);
    }

    pCol[i].pData = pStart;
    pStart += colLength[i];
  }

  SHashObj* schemaHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  for (int i = 0; i < numFields; i++) {
    TAOS_FIELD* schema = &fields[i];
    taosHashPut(schemaHash, schema->name, strlen(schema->name), &i, sizeof(int32_t));
  }

  for (int32_t j = 0; j < rows; j++) {
    tdSRowResetBuf(&rb, rowData);
    int32_t offset = 0;
    for (int32_t k = 0; k < numOfCols; k++) {
      const SSchema* pColumn = &pTableMeta->schema[k];
      int32_t*       index = taosHashGet(schemaHash, pColumn->name, strlen(pColumn->name));
      if (!index) {   // add none
        tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NONE, NULL, false, offset, k);
      }else{
        if (IS_VAR_DATA_TYPE(pColumn->type)) {
          if (pCol[*index].offset[j] != -1) {
            char* data = pCol[*index].pData + pCol[*index].offset[j];
            tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NORM, data, true, offset, k);
          } else {
            tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NULL, NULL, false, offset, k);
          }
        } else {
          if (!colDataIsNull_f(pCol[*index].nullbitmap, j)) {
            char* data = pCol[*index].pData + pColumn->bytes * j;
            tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NORM, data, true, offset, k);
          } else {
            tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NULL, NULL, false, offset, k);
          }
        }
      }

      if (pColumn->colId != PRIMARYKEY_TIMESTAMP_COL_ID) {
        offset += TYPE_BYTES[pColumn->type];
      }
    }
    tdSRowEnd(&rb);
    int32_t rowLen = TD_ROW_LEN(rowData);
    rowData = POINTER_SHIFT(rowData, rowLen);
    dataLen += rowLen;
  }

  taosHashCleanup(schemaHash);
  taosMemoryFree(pCol);

  blk->uid = htobe64(uid);
  blk->suid = htobe64(suid);
  blk->sversion = htonl(pTableMeta->sversion);
  blk->schemaLen = htonl(schemaLen);
  blk->numOfRows = htonl(rows);
  blk->dataLen = htonl(dataLen);
  subReq->length = sizeof(SSubmitReq) + sizeof(SSubmitBlk) + schemaLen + dataLen;
  subReq->numOfBlocks = 1;

  pQuery = (SQuery*)nodesMakeNode(QUERY_NODE_QUERY);
  if (NULL == pQuery) {
    uError("create SQuery error");
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  pQuery->haveResultSet = false;
  pQuery->msgType = TDMT_VND_SUBMIT;
  pQuery->pRoot = (SNode*)nodesMakeNode(QUERY_NODE_VNODE_MODIF_STMT);
  if (NULL == pQuery->pRoot) {
    uError("create pQuery->pRoot error");
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  SVnodeModifOpStmt* nodeStmt = (SVnodeModifOpStmt*)(pQuery->pRoot);
  nodeStmt->pDataBlocks = taosArrayInit(1, POINTER_BYTES);

  SVgDataBlocks* dst = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
  if (NULL == dst) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  dst->vg = vgData;
  dst->numOfTables = subReq->numOfBlocks;
  dst->size = subReq->length;
  dst->pData = (char*)subReq;
  subReq->header.vgId = htonl(dst->vg.vgId);
  subReq->version = htonl(1);
  subReq->header.contLen = htonl(subReq->length);
  subReq->length = htonl(subReq->length);
  subReq->numOfBlocks = htonl(subReq->numOfBlocks);
  subReq = NULL;  // no need free
  taosArrayPush(nodeStmt->pDataBlocks, &dst);

  launchQueryImpl(pRequest, pQuery, true, NULL);
  code = pRequest->code;

  end:
  taosMemoryFreeClear(pTableMeta);
  qDestroyQuery(pQuery);
  destroyRequest(pRequest);
  taosMemoryFree(subReq);
  return code;
}

int taos_write_raw_block(TAOS* taos, int rows, char* pData, const char* tbname) {
  int32_t     code = TSDB_CODE_SUCCESS;
  STableMeta* pTableMeta = NULL;
  SQuery*     pQuery = NULL;
  SSubmitReq* subReq = NULL;

  SRequestObj* pRequest = (SRequestObj*)createRequest(*(int64_t*)taos, TSDB_SQL_INSERT, 0);
  if (!pRequest) {
    uError("WriteRaw:createRequest error request is null");
    code = terrno;
    goto end;
  }

  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
    uError("WriteRaw:not use db");
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }

  SName pName = {TSDB_TABLE_NAME_T, pRequest->pTscObj->acctId, {0}, {0}};
  tstrncpy(pName.dbname, pRequest->pDb, sizeof(pName.dbname));
  tstrncpy(pName.tname, tbname, sizeof(pName.tname));

  struct SCatalog* pCatalog = NULL;
  code = catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    uError("WriteRaw: get gatlog error");
    goto end;
  }

  SRequestConnInfo conn = {0};
  conn.pTrans = pRequest->pTscObj->pAppInfo->pTransporter;
  conn.requestId = pRequest->requestId;
  conn.requestObjRefId = pRequest->self;
  conn.mgmtEps = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp);

  SVgroupInfo vgData = {0};
  code = catalogGetTableHashVgroup(pCatalog, &conn, &pName, &vgData);
  if (code != TSDB_CODE_SUCCESS) {
    uError("WriteRaw:catalogGetTableHashVgroup failed. table name: %s", tbname);
    goto end;
  }

  code = catalogGetTableMeta(pCatalog, &conn, &pName, &pTableMeta);
  if (code != TSDB_CODE_SUCCESS) {
    uError("WriteRaw:catalogGetTableMeta failed. table name: %s", tbname);
    goto end;
  }
  uint64_t suid = (TSDB_NORMAL_TABLE == pTableMeta->tableType ? 0 : pTableMeta->suid);
  uint64_t uid = pTableMeta->uid;
  int32_t  numOfCols = pTableMeta->tableInfo.numOfColumns;

  uint16_t fLen = 0;
  int32_t  rowSize = 0;
  int16_t  nVar = 0;
  for (int i = 0; i < numOfCols; i++) {
    SSchema* schema = pTableMeta->schema + i;
    fLen += TYPE_BYTES[schema->type];
    rowSize += schema->bytes;
    if (IS_VAR_DATA_TYPE(schema->type)) {
      nVar++;
    }
  }
  fLen -= sizeof(TSKEY);

  int32_t extendedRowSize = rowSize + TD_ROW_HEAD_LEN - sizeof(TSKEY) + nVar * sizeof(VarDataOffsetT) +
                            (int32_t)TD_BITMAP_BYTES(numOfCols - 1);
  int32_t schemaLen = 0;
  int32_t submitLen = sizeof(SSubmitBlk) + schemaLen + rows * extendedRowSize;

  int32_t totalLen = sizeof(SSubmitReq) + submitLen;
  subReq = taosMemoryCalloc(1, totalLen);
  SSubmitBlk* blk = POINTER_SHIFT(subReq, sizeof(SSubmitReq));
  void*       blkSchema = POINTER_SHIFT(blk, sizeof(SSubmitBlk));
  STSRow*     rowData = POINTER_SHIFT(blkSchema, schemaLen);

  SRowBuilder rb = {0};
  tdSRowInit(&rb, pTableMeta->sversion);
  tdSRowSetTpInfo(&rb, numOfCols, fLen);
  int32_t dataLen = 0;

  // | version | total length | total rows | total columns | flag seg| block group id | column schema | each column length |
  char*    pStart = pData + getVersion1BlockMetaSize(pData, numOfCols);
  int32_t* colLength = (int32_t*)pStart;
  pStart += sizeof(int32_t) * numOfCols;

  SResultColumn* pCol = taosMemoryCalloc(numOfCols, sizeof(SResultColumn));

  for (int32_t i = 0; i < numOfCols; ++i) {
    if (IS_VAR_DATA_TYPE(pTableMeta->schema[i].type)) {
      pCol[i].offset = (int32_t*)pStart;
      pStart += rows * sizeof(int32_t);
    } else {
      pCol[i].nullbitmap = pStart;
      pStart += BitmapLen(rows);
    }

    pCol[i].pData = pStart;
    pStart += colLength[i];
  }

  for (int32_t j = 0; j < rows; j++) {
    tdSRowResetBuf(&rb, rowData);
    int32_t offset = 0;
    for (int32_t k = 0; k < numOfCols; k++) {
      const SSchema* pColumn = &pTableMeta->schema[k];

      if (IS_VAR_DATA_TYPE(pColumn->type)) {
        if (pCol[k].offset[j] != -1) {
          char* data = pCol[k].pData + pCol[k].offset[j];
          tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NORM, data, true, offset, k);
        } else {
          tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NULL, NULL, false, offset, k);
        }
      } else {
        if (!colDataIsNull_f(pCol[k].nullbitmap, j)) {
          char* data = pCol[k].pData + pColumn->bytes * j;
          tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NORM, data, true, offset, k);
        } else {
          tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NULL, NULL, false, offset, k);
        }
      }

      if (pColumn->colId != PRIMARYKEY_TIMESTAMP_COL_ID) {
        offset += TYPE_BYTES[pColumn->type];
      }
    }
    tdSRowEnd(&rb);
    int32_t rowLen = TD_ROW_LEN(rowData);
    rowData = POINTER_SHIFT(rowData, rowLen);
    dataLen += rowLen;
  }

  taosMemoryFree(pCol);

  blk->uid = htobe64(uid);
  blk->suid = htobe64(suid);
  blk->sversion = htonl(pTableMeta->sversion);
  blk->schemaLen = htonl(schemaLen);
  blk->numOfRows = htonl(rows);
  blk->dataLen = htonl(dataLen);
  subReq->length = sizeof(SSubmitReq) + sizeof(SSubmitBlk) + schemaLen + dataLen;
  subReq->numOfBlocks = 1;

  pQuery = (SQuery*)nodesMakeNode(QUERY_NODE_QUERY);
  if (NULL == pQuery) {
    uError("create SQuery error");
    code = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(subReq);
    goto end;
  }
  pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  pQuery->haveResultSet = false;
  pQuery->msgType = TDMT_VND_SUBMIT;
  pQuery->pRoot = (SNode*)nodesMakeNode(QUERY_NODE_VNODE_MODIF_STMT);
  if (NULL == pQuery->pRoot) {
    uError("create pQuery->pRoot error");
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  SVnodeModifOpStmt* nodeStmt = (SVnodeModifOpStmt*)(pQuery->pRoot);
  nodeStmt->pDataBlocks = taosArrayInit(1, POINTER_BYTES);

  SVgDataBlocks* dst = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
  if (NULL == dst) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  dst->vg = vgData;
  dst->numOfTables = subReq->numOfBlocks;
  dst->size = subReq->length;
  dst->pData = (char*)subReq;
  subReq->header.vgId = htonl(dst->vg.vgId);
  subReq->version = htonl(1);
  subReq->header.contLen = htonl(subReq->length);
  subReq->length = htonl(subReq->length);
  subReq->numOfBlocks = htonl(subReq->numOfBlocks);
  subReq = NULL;  // no need free
  taosArrayPush(nodeStmt->pDataBlocks, &dst);

  launchQueryImpl(pRequest, pQuery, true, NULL);
  code = pRequest->code;

  end:
  taosMemoryFreeClear(pTableMeta);
  qDestroyQuery(pQuery);
  destroyRequest(pRequest);
  taosMemoryFree(subReq);
  return code;
}

static int32_t tmqWriteRawDataImpl(TAOS* taos, void* data, int32_t dataLen) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SHashObj*   pVgHash = NULL;
  SQuery*     pQuery = NULL;
  SMqRspObj   rspObj = {0};
  SDecoder    decoder = {0};
  STableMeta* pTableMeta = NULL;

  terrno = TSDB_CODE_SUCCESS;
  SRequestObj* pRequest = (SRequestObj*)createRequest(*(int64_t*)taos, TSDB_SQL_INSERT, 0);
  if (!pRequest) {
    uError("WriteRaw:createRequest error request is null");
    return terrno;
  }

  pRequest->syncQuery = true;
  rspObj.resIter = -1;
  rspObj.resType = RES_TYPE__TMQ;

  tDecoderInit(&decoder, data, dataLen);
  code = tDecodeSMqDataRsp(&decoder, &rspObj.rsp);
  if (code != 0) {
    uError("WriteRaw:decode smqDataRsp error");
    code = TSDB_CODE_INVALID_MSG;
    goto end;
  }

  if (!pRequest->pDb) {
    uError("WriteRaw:not use db");
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }

  pVgHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  taosHashSetFreeFp(pVgHash, destroyVgHash);
  struct SCatalog* pCatalog = NULL;
  code = catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    uError("WriteRaw: get gatlog error");
    goto end;
  }

  SRequestConnInfo conn = {0};
  conn.pTrans = pRequest->pTscObj->pAppInfo->pTransporter;
  conn.requestId = pRequest->requestId;
  conn.requestObjRefId = pRequest->self;
  conn.mgmtEps = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp);

  uDebug("raw data block num:%d\n", rspObj.rsp.blockNum);
  while (++rspObj.resIter < rspObj.rsp.blockNum) {
    SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)taosArrayGetP(rspObj.rsp.blockData, rspObj.resIter);
    if (!rspObj.rsp.withSchema) {
      uError("WriteRaw:no schema, iter:%d", rspObj.resIter);
      goto end;
    }
    SSchemaWrapper* pSW = (SSchemaWrapper*)taosArrayGetP(rspObj.rsp.blockSchema, rspObj.resIter);
    setResSchemaInfo(&rspObj.resInfo, pSW->pSchema, pSW->nCols);

    code = setQueryResultFromRsp(&rspObj.resInfo, pRetrieve, false, false);
    if (code != TSDB_CODE_SUCCESS) {
      uError("WriteRaw: setQueryResultFromRsp error");
      goto end;
    }

    const char* tbName = (const char*)taosArrayGetP(rspObj.rsp.blockTbName, rspObj.resIter);
    if (!tbName) {
      uError("WriteRaw: tbname is null");
      code = TSDB_CODE_TMQ_INVALID_MSG;
      goto end;
    }

    uDebug("raw data tbname:%s\n", tbName);
    SName pName = {TSDB_TABLE_NAME_T, pRequest->pTscObj->acctId, {0}, {0}};
    strcpy(pName.dbname, pRequest->pDb);
    strcpy(pName.tname, tbName);

    VgData vgData = {0};
    code = catalogGetTableHashVgroup(pCatalog, &conn, &pName, &(vgData.vg));
    if (code != TSDB_CODE_SUCCESS) {
      uError("WriteRaw:catalogGetTableHashVgroup failed. table name: %s", tbName);
      goto end;
    }

    code = catalogGetTableMeta(pCatalog, &conn, &pName, &pTableMeta);
    if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
      uError("WriteRaw:catalogGetTableMeta table not exist. table name: %s", tbName);
      code = TSDB_CODE_SUCCESS;
      continue;
    }
    if (code != TSDB_CODE_SUCCESS) {
      uError("WriteRaw:catalogGetTableMeta failed. table name: %s", tbName);
      goto end;
    }

    uint16_t fLen = 0;
    int32_t  rowSize = 0;
    int16_t  nVar = 0;
    for (int i = 0; i < pTableMeta->tableInfo.numOfColumns; i++) {
      SSchema* schema = &pTableMeta->schema[i];
      fLen += TYPE_BYTES[schema->type];
      rowSize += schema->bytes;
      if (IS_VAR_DATA_TYPE(schema->type)) {
        nVar++;
      }
    }
    fLen -= sizeof(TSKEY);

    int32_t rows = rspObj.resInfo.numOfRows;
    int32_t extendedRowSize = rowSize + TD_ROW_HEAD_LEN - sizeof(TSKEY) + nVar * sizeof(VarDataOffsetT) +
                              (int32_t)TD_BITMAP_BYTES(pTableMeta->tableInfo.numOfColumns - 1);
    int32_t schemaLen = 0;
    int32_t submitLen = sizeof(SSubmitBlk) + schemaLen + rows * extendedRowSize;

    SSubmitReq* subReq = NULL;
    SSubmitBlk* blk = NULL;
    void*       hData = taosHashGet(pVgHash, &vgData.vg.vgId, sizeof(vgData.vg.vgId));
    if (hData) {
      vgData = *(VgData*)hData;

      int32_t totalLen = ((SSubmitReq*)(vgData.data))->length + submitLen;
      void*   tmp = taosMemoryRealloc(vgData.data, totalLen);
      if (tmp == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto end;
      }
      vgData.data = tmp;
      ((VgData*)hData)->data = tmp;
      subReq = (SSubmitReq*)(vgData.data);
      blk = POINTER_SHIFT(vgData.data, subReq->length);
    } else {
      int32_t totalLen = sizeof(SSubmitReq) + submitLen;
      void*   tmp = taosMemoryCalloc(1, totalLen);
      if (tmp == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto end;
      }
      vgData.data = tmp;
      taosHashPut(pVgHash, (const char*)&vgData.vg.vgId, sizeof(vgData.vg.vgId), (char*)&vgData, sizeof(vgData));
      subReq = (SSubmitReq*)(vgData.data);
      subReq->length = sizeof(SSubmitReq);
      subReq->numOfBlocks = 0;

      blk = POINTER_SHIFT(vgData.data, sizeof(SSubmitReq));
    }

    // pSW->pSchema should be same as pTableMeta->schema
    //    ASSERT(pSW->nCols == pTableMeta->tableInfo.numOfColumns);
    uint64_t suid = (TSDB_NORMAL_TABLE == pTableMeta->tableType ? 0 : pTableMeta->suid);
    uint64_t uid = pTableMeta->uid;
    int16_t  sver = pTableMeta->sversion;

    void*   blkSchema = POINTER_SHIFT(blk, sizeof(SSubmitBlk));
    STSRow* rowData = POINTER_SHIFT(blkSchema, schemaLen);

    SRowBuilder rb = {0};
    tdSRowInit(&rb, sver);
    tdSRowSetTpInfo(&rb, pTableMeta->tableInfo.numOfColumns, fLen);
    int32_t totalLen = 0;

    SHashObj* schemaHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    for (int i = 0; i < pSW->nCols; i++) {
      SSchema* schema = &pSW->pSchema[i];
      taosHashPut(schemaHash, schema->name, strlen(schema->name), &i, sizeof(int32_t));
    }

    for (int32_t j = 0; j < rows; j++) {
      tdSRowResetBuf(&rb, rowData);

      doSetOneRowPtr(&rspObj.resInfo);
      rspObj.resInfo.current += 1;

      int32_t offset = 0;
      for (int32_t k = 0; k < pTableMeta->tableInfo.numOfColumns; k++) {
        const SSchema* pColumn = &pTableMeta->schema[k];
        int32_t*       index = taosHashGet(schemaHash, pColumn->name, strlen(pColumn->name));
        if (!index) {
          tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NONE, NULL, false, offset, k);
        } else {
          char* colData = rspObj.resInfo.row[*index];
          if (!colData) {
            tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NULL, NULL, false, offset, k);
          } else {
            if (IS_VAR_DATA_TYPE(pColumn->type)) {
              colData -= VARSTR_HEADER_SIZE;
            }
            tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NORM, colData, true, offset, k);
          }
        }
        if (pColumn->colId != PRIMARYKEY_TIMESTAMP_COL_ID) {
          offset += TYPE_BYTES[pColumn->type];
        }
      }
      tdSRowEnd(&rb);
      int32_t rowLen = TD_ROW_LEN(rowData);
      rowData = POINTER_SHIFT(rowData, rowLen);
      totalLen += rowLen;
    }

    taosHashCleanup(schemaHash);
    blk->uid = htobe64(uid);
    blk->suid = htobe64(suid);
    blk->sversion = htonl(sver);
    blk->schemaLen = htonl(schemaLen);
    blk->numOfRows = htonl(rows);
    blk->dataLen = htonl(totalLen);
    subReq->length += sizeof(SSubmitBlk) + schemaLen + totalLen;
    subReq->numOfBlocks++;
    taosMemoryFreeClear(pTableMeta);
    rspObj.resInfo.pRspMsg = NULL;
    doFreeReqResultInfo(&rspObj.resInfo);
  }

  pQuery = (SQuery*)nodesMakeNode(QUERY_NODE_QUERY);
  if (NULL == pQuery) {
    uError("create SQuery error");
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  pQuery->haveResultSet = false;
  pQuery->msgType = TDMT_VND_SUBMIT;
  pQuery->pRoot = (SNode*)nodesMakeNode(QUERY_NODE_VNODE_MODIF_STMT);
  if (NULL == pQuery->pRoot) {
    uError("create pQuery->pRoot error");
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  SVnodeModifOpStmt* nodeStmt = (SVnodeModifOpStmt*)(pQuery->pRoot);

  int32_t numOfVg = taosHashGetSize(pVgHash);
  nodeStmt->pDataBlocks = taosArrayInit(numOfVg, POINTER_BYTES);

  VgData* vData = (VgData*)taosHashIterate(pVgHash, NULL);
  while (vData) {
    SVgDataBlocks* dst = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
    if (NULL == dst) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto end;
    }
    dst->vg = vData->vg;
    SSubmitReq* subReq = (SSubmitReq*)(vData->data);
    dst->numOfTables = subReq->numOfBlocks;
    dst->size = subReq->length;
    dst->pData = (char*)subReq;
    vData->data = NULL;  // no need free
    subReq->header.vgId = htonl(dst->vg.vgId);
    subReq->version = htonl(1);
    subReq->header.contLen = htonl(subReq->length);
    subReq->length = htonl(subReq->length);
    subReq->numOfBlocks = htonl(subReq->numOfBlocks);
    taosArrayPush(nodeStmt->pDataBlocks, &dst);
    vData = (VgData*)taosHashIterate(pVgHash, vData);
  }

  launchQueryImpl(pRequest, pQuery, true, NULL);
  code = pRequest->code;

  end:
  tDeleteSMqDataRsp(&rspObj.rsp);
  rspObj.resInfo.pRspMsg = NULL;
  doFreeReqResultInfo(&rspObj.resInfo);
  tDecoderClear(&decoder);
  qDestroyQuery(pQuery);
  destroyRequest(pRequest);
  taosHashCleanup(pVgHash);
  taosMemoryFreeClear(pTableMeta);
  return code;
}


static int32_t tmqWriteRawMetaDataImpl(TAOS* taos, void* data, int32_t dataLen) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SHashObj*      pVgHash = NULL;
  SQuery*        pQuery = NULL;
  SMqTaosxRspObj rspObj = {0};
  SDecoder       decoder = {0};
  STableMeta*    pTableMeta = NULL;
  void*          schemaContent = NULL;

  terrno = TSDB_CODE_SUCCESS;
  SRequestObj* pRequest = (SRequestObj*)createRequest(*(int64_t*)taos, TSDB_SQL_INSERT, 0);
  if (!pRequest) {
    uError("WriteRaw:createRequest error request is null");
    return terrno;
  }

  pRequest->syncQuery = true;
  rspObj.resIter = -1;
  rspObj.resType = RES_TYPE__TMQ_METADATA;

  tDecoderInit(&decoder, data, dataLen);
  code = tDecodeSTaosxRsp(&decoder, &rspObj.rsp);
  if (code != 0) {
    uError("WriteRaw:decode smqDataRsp error");
    code = TSDB_CODE_INVALID_MSG;
    goto end;
  }

  if (!pRequest->pDb) {
    uError("WriteRaw:not use db");
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }

  pVgHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  taosHashSetFreeFp(pVgHash, destroyVgHash);
  struct SCatalog* pCatalog = NULL;
  code = catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    uError("WriteRaw: get gatlog error");
    goto end;
  }

  SRequestConnInfo conn = {0};
  conn.pTrans = pRequest->pTscObj->pAppInfo->pTransporter;
  conn.requestId = pRequest->requestId;
  conn.requestObjRefId = pRequest->self;
  conn.mgmtEps = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp);

  uDebug("raw data block num:%d\n", rspObj.rsp.blockNum);
  while (++rspObj.resIter < rspObj.rsp.blockNum) {
    SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)taosArrayGetP(rspObj.rsp.blockData, rspObj.resIter);
    if (!rspObj.rsp.withSchema) {
      uError("WriteRaw:no schema, iter:%d", rspObj.resIter);
      goto end;
    }
    SSchemaWrapper* pSW = (SSchemaWrapper*)taosArrayGetP(rspObj.rsp.blockSchema, rspObj.resIter);
    setResSchemaInfo(&rspObj.resInfo, pSW->pSchema, pSW->nCols);

    code = setQueryResultFromRsp(&rspObj.resInfo, pRetrieve, false, false);
    if (code != TSDB_CODE_SUCCESS) {
      uError("WriteRaw: setQueryResultFromRsp error");
      goto end;
    }

    const char* tbName = (const char*)taosArrayGetP(rspObj.rsp.blockTbName, rspObj.resIter);
    if (!tbName) {
      uError("WriteRaw: tbname is null");
      code = TSDB_CODE_TMQ_INVALID_MSG;
      goto end;
    }

    uDebug("raw data tbname:%s\n", tbName);
    SName pName = {TSDB_TABLE_NAME_T, pRequest->pTscObj->acctId, {0}, {0}};
    strcpy(pName.dbname, pRequest->pDb);
    strcpy(pName.tname, tbName);

    VgData vgData = {0};
    code = catalogGetTableHashVgroup(pCatalog, &conn, &pName, &(vgData.vg));
    if (code != TSDB_CODE_SUCCESS) {
      uError("WriteRaw:catalogGetTableHashVgroup failed. table name: %s", tbName);
      goto end;
    }

    // find schema data info
    int32_t schemaLen = 0;
    void*   schemaData = NULL;
    for (int j = 0; j < rspObj.rsp.createTableNum; j++) {
      void**   dataTmp = taosArrayGet(rspObj.rsp.createTableReq, j);
      int32_t* lenTmp = taosArrayGet(rspObj.rsp.createTableLen, j);

      SDecoder      decoderTmp = {0};
      SVCreateTbReq pCreateReq = {0};

      do{
        tDecoderInit(&decoderTmp, *dataTmp, *lenTmp);
        if (tDecodeSVCreateTbReq(&decoderTmp, &pCreateReq) < 0) {
          code = TSDB_CODE_MSG_DECODE_ERROR;
          break;
        }

        if (strcmp(tbName, pCreateReq.name) != 0) {
          break;
        }

        pCreateReq.ctb.suid = processSuid(pCreateReq.ctb.suid, pRequest->pDb);

        int32_t len = 0;
        tEncodeSize(tEncodeSVCreateTbReq, &pCreateReq, len, code);
        if(code != 0) {
          code = TSDB_CODE_MSG_ENCODE_ERROR;
          break;
        }
        taosMemoryFree(schemaContent);
        schemaContent = taosMemoryMalloc(len);
        if(!schemaContent) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          break;
        }
        SEncoder encoder = {0};
        tEncoderInit(&encoder, schemaContent, len);
        code = tEncodeSVCreateTbReq(&encoder, &pCreateReq);
        if (code != 0) {
          tEncoderClear(&encoder);
          code = TSDB_CODE_MSG_ENCODE_ERROR;
          break;
        }
        schemaLen = len;
        schemaData = schemaContent;
        strcpy(pName.tname, pCreateReq.ctb.stbName);
        tEncoderClear(&encoder);
      }while(0);
      tDecoderClear(&decoderTmp);
      taosMemoryFreeClear(pCreateReq.comment);
      taosArrayDestroy(pCreateReq.ctb.tagName);
      if(code != 0) goto end;
      if(schemaLen != 0) break;
    }

    code = catalogGetTableMeta(pCatalog, &conn, &pName, &pTableMeta);
    if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
      uError("WriteRaw:catalogGetTableMeta table not exist. table name: %s", tbName);
      code = TSDB_CODE_SUCCESS;
      continue;
    }
    if (code != TSDB_CODE_SUCCESS) {
      uError("WriteRaw:catalogGetTableMeta failed. table name: %s", tbName);
      goto end;
    }

    uint16_t fLen = 0;
    int32_t  rowSize = 0;
    int16_t  nVar = 0;
    for (int i = 0; i < pTableMeta->tableInfo.numOfColumns; i++) {
      SSchema* schema = &pTableMeta->schema[i];
      fLen += TYPE_BYTES[schema->type];
      rowSize += schema->bytes;
      if (IS_VAR_DATA_TYPE(schema->type)) {
        nVar++;
      }
    }
    fLen -= sizeof(TSKEY);

    int32_t rows = rspObj.resInfo.numOfRows;
    int32_t extendedRowSize = rowSize + TD_ROW_HEAD_LEN - sizeof(TSKEY) + nVar * sizeof(VarDataOffsetT) +
                              (int32_t)TD_BITMAP_BYTES(pTableMeta->tableInfo.numOfColumns - 1);

    int32_t submitLen = sizeof(SSubmitBlk) + schemaLen + rows * extendedRowSize;

    SSubmitReq* subReq = NULL;
    SSubmitBlk* blk = NULL;
    void*       hData = taosHashGet(pVgHash, &vgData.vg.vgId, sizeof(vgData.vg.vgId));
    if (hData) {
      vgData = *(VgData*)hData;

      int32_t totalLen = ((SSubmitReq*)(vgData.data))->length + submitLen;
      void*   tmp = taosMemoryRealloc(vgData.data, totalLen);
      if (tmp == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto end;
      }
      vgData.data = tmp;
      ((VgData*)hData)->data = tmp;
      subReq = (SSubmitReq*)(vgData.data);
      blk = POINTER_SHIFT(vgData.data, subReq->length);
    } else {
      int32_t totalLen = sizeof(SSubmitReq) + submitLen;
      void*   tmp = taosMemoryCalloc(1, totalLen);
      if (tmp == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto end;
      }
      vgData.data = tmp;
      taosHashPut(pVgHash, (const char*)&vgData.vg.vgId, sizeof(vgData.vg.vgId), (char*)&vgData, sizeof(vgData));
      subReq = (SSubmitReq*)(vgData.data);
      subReq->length = sizeof(SSubmitReq);
      subReq->numOfBlocks = 0;

      blk = POINTER_SHIFT(vgData.data, sizeof(SSubmitReq));
    }

    // pSW->pSchema should be same as pTableMeta->schema
    //    ASSERT(pSW->nCols == pTableMeta->tableInfo.numOfColumns);
    uint64_t suid = (TSDB_NORMAL_TABLE == pTableMeta->tableType ? 0 : pTableMeta->suid);
    uint64_t uid = pTableMeta->uid;
    int16_t  sver = pTableMeta->sversion;

    void* blkSchema = POINTER_SHIFT(blk, sizeof(SSubmitBlk));
    if (schemaData) {
      memcpy(blkSchema, schemaData, schemaLen);
    }
    STSRow* rowData = POINTER_SHIFT(blkSchema, schemaLen);

    SRowBuilder rb = {0};
    tdSRowInit(&rb, sver);
    tdSRowSetTpInfo(&rb, pTableMeta->tableInfo.numOfColumns, fLen);
    int32_t totalLen = 0;

    SHashObj* schemaHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    for (int i = 0; i < pSW->nCols; i++) {
      SSchema* schema = &pSW->pSchema[i];
      taosHashPut(schemaHash, schema->name, strlen(schema->name), &i, sizeof(int32_t));
    }

    for (int32_t j = 0; j < rows; j++) {
      tdSRowResetBuf(&rb, rowData);

      doSetOneRowPtr(&rspObj.resInfo);
      rspObj.resInfo.current += 1;

      int32_t offset = 0;
      for (int32_t k = 0; k < pTableMeta->tableInfo.numOfColumns; k++) {
        const SSchema* pColumn = &pTableMeta->schema[k];
        int32_t*       index = taosHashGet(schemaHash, pColumn->name, strlen(pColumn->name));
        if (!index) {
          tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NONE, NULL, false, offset, k);
        } else {
          char* colData = rspObj.resInfo.row[*index];
          if (!colData) {
            tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NULL, NULL, false, offset, k);
          } else {
            if (IS_VAR_DATA_TYPE(pColumn->type)) {
              colData -= VARSTR_HEADER_SIZE;
            }
            tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NORM, colData, true, offset, k);
          }
        }
        if (pColumn->colId != PRIMARYKEY_TIMESTAMP_COL_ID) {
          offset += TYPE_BYTES[pColumn->type];
        }
      }
      tdSRowEnd(&rb);
      int32_t rowLen = TD_ROW_LEN(rowData);
      rowData = POINTER_SHIFT(rowData, rowLen);
      totalLen += rowLen;
    }

    taosHashCleanup(schemaHash);
    blk->uid = htobe64(uid);
    blk->suid = htobe64(suid);
    blk->sversion = htonl(sver);
    blk->schemaLen = htonl(schemaLen);
    blk->numOfRows = htonl(rows);
    blk->dataLen = htonl(totalLen);
    subReq->length += sizeof(SSubmitBlk) + schemaLen + totalLen;
    subReq->numOfBlocks++;
    taosMemoryFreeClear(pTableMeta);
    rspObj.resInfo.pRspMsg = NULL;
    doFreeReqResultInfo(&rspObj.resInfo);
  }

  pQuery = (SQuery*)nodesMakeNode(QUERY_NODE_QUERY);
  if (NULL == pQuery) {
    uError("create SQuery error");
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  pQuery->haveResultSet = false;
  pQuery->msgType = TDMT_VND_SUBMIT;
  pQuery->pRoot = (SNode*)nodesMakeNode(QUERY_NODE_VNODE_MODIF_STMT);
  if (NULL == pQuery->pRoot) {
    uError("create pQuery->pRoot error");
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  SVnodeModifOpStmt* nodeStmt = (SVnodeModifOpStmt*)(pQuery->pRoot);

  int32_t numOfVg = taosHashGetSize(pVgHash);
  nodeStmt->pDataBlocks = taosArrayInit(numOfVg, POINTER_BYTES);

  VgData* vData = (VgData*)taosHashIterate(pVgHash, NULL);
  while (vData) {
    SVgDataBlocks* dst = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
    if (NULL == dst) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto end;
    }
    dst->vg = vData->vg;
    SSubmitReq* subReq = (SSubmitReq*)(vData->data);
    dst->numOfTables = subReq->numOfBlocks;
    dst->size = subReq->length;
    dst->pData = (char*)subReq;
    vData->data = NULL;  // no need free
    subReq->header.vgId = htonl(dst->vg.vgId);
    subReq->version = htonl(1);
    subReq->header.contLen = htonl(subReq->length);
    subReq->length = htonl(subReq->length);
    subReq->numOfBlocks = htonl(subReq->numOfBlocks);
    taosArrayPush(nodeStmt->pDataBlocks, &dst);
    vData = (VgData*)taosHashIterate(pVgHash, vData);
  }

  launchQueryImpl(pRequest, pQuery, true, NULL);
  code = pRequest->code;

  end:
  tDeleteSTaosxRsp(&rspObj.rsp);
  rspObj.resInfo.pRspMsg = NULL;
  doFreeReqResultInfo(&rspObj.resInfo);
  tDecoderClear(&decoder);
  qDestroyQuery(pQuery);
  destroyRequest(pRequest);
  taosHashCleanup(pVgHash);
  taosMemoryFreeClear(pTableMeta);
  taosMemoryFree(schemaContent);
  return code;
}

char* tmq_get_json_meta(TAOS_RES* res) {
  if (!TD_RES_TMQ_META(res) && !TD_RES_TMQ_METADATA(res)) {
    return NULL;
  }

  if (TD_RES_TMQ_METADATA(res)) {
    SMqTaosxRspObj* pMetaDataRspObj = (SMqTaosxRspObj*)res;
    return processAutoCreateTable(&pMetaDataRspObj->rsp);
  }

  SMqMetaRspObj* pMetaRspObj = (SMqMetaRspObj*)res;
  if (pMetaRspObj->metaRsp.resMsgType == TDMT_VND_CREATE_STB) {
    return processCreateStb(&pMetaRspObj->metaRsp);
  } else if (pMetaRspObj->metaRsp.resMsgType == TDMT_VND_ALTER_STB) {
    return processAlterStb(&pMetaRspObj->metaRsp);
  } else if (pMetaRspObj->metaRsp.resMsgType == TDMT_VND_DROP_STB) {
    return processDropSTable(&pMetaRspObj->metaRsp);
  } else if (pMetaRspObj->metaRsp.resMsgType == TDMT_VND_CREATE_TABLE) {
    return processCreateTable(&pMetaRspObj->metaRsp);
  } else if (pMetaRspObj->metaRsp.resMsgType == TDMT_VND_ALTER_TABLE) {
    return processAlterTable(&pMetaRspObj->metaRsp);
  } else if (pMetaRspObj->metaRsp.resMsgType == TDMT_VND_DROP_TABLE) {
    return processDropTable(&pMetaRspObj->metaRsp);
  } else if (pMetaRspObj->metaRsp.resMsgType == TDMT_VND_DROP_TABLE) {
    return processDropTable(&pMetaRspObj->metaRsp);
  } else if (pMetaRspObj->metaRsp.resMsgType == TDMT_VND_DELETE) {
    return processDeleteTable(&pMetaRspObj->metaRsp);
  }

  return NULL;
}

void tmq_free_json_meta(char* jsonMeta) { taosMemoryFreeClear(jsonMeta); }

int32_t tmq_get_raw(TAOS_RES* res, tmq_raw_data* raw) {
  if (!raw || !res) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (TD_RES_TMQ_META(res)) {
    SMqMetaRspObj* pMetaRspObj = (SMqMetaRspObj*)res;
    raw->raw = pMetaRspObj->metaRsp.metaRsp;
    raw->raw_len = pMetaRspObj->metaRsp.metaRspLen;
    raw->raw_type = pMetaRspObj->metaRsp.resMsgType;
  } else if (TD_RES_TMQ(res)) {
    SMqRspObj* rspObj = ((SMqRspObj*)res);

    int32_t len = 0;
    int32_t code = 0;
    tEncodeSize(tEncodeSMqDataRsp, &rspObj->rsp, len, code);
    if (code < 0) {
      return -1;
    }

    void*    buf = taosMemoryCalloc(1, len);
    SEncoder encoder = {0};
    tEncoderInit(&encoder, buf, len);
    tEncodeSMqDataRsp(&encoder, &rspObj->rsp);
    tEncoderClear(&encoder);

    raw->raw = buf;
    raw->raw_len = len;
    raw->raw_type = RES_TYPE__TMQ;
  } else if (TD_RES_TMQ_METADATA(res)) {
    SMqTaosxRspObj* rspObj = ((SMqTaosxRspObj*)res);

    int32_t len = 0;
    int32_t code = 0;
    tEncodeSize(tEncodeSTaosxRsp, &rspObj->rsp, len, code);
    if (code < 0) {
      return -1;
    }

    void*    buf = taosMemoryCalloc(1, len);
    SEncoder encoder = {0};
    tEncoderInit(&encoder, buf, len);
    tEncodeSTaosxRsp(&encoder, &rspObj->rsp);
    tEncoderClear(&encoder);

    raw->raw = buf;
    raw->raw_len = len;
    raw->raw_type = RES_TYPE__TMQ_METADATA;
  } else {
    return TSDB_CODE_TMQ_INVALID_MSG;
  }
  return TSDB_CODE_SUCCESS;
}

void tmq_free_raw(tmq_raw_data raw) {
  if (raw.raw_type == RES_TYPE__TMQ || raw.raw_type == RES_TYPE__TMQ_METADATA) {
    taosMemoryFree(raw.raw);
  }
}

int32_t tmq_write_raw(TAOS* taos, tmq_raw_data raw) {
  if (!taos) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (raw.raw_type == TDMT_VND_CREATE_STB) {
    return taosCreateStb(taos, raw.raw, raw.raw_len);
  } else if (raw.raw_type == TDMT_VND_ALTER_STB) {
    return taosCreateStb(taos, raw.raw, raw.raw_len);
  } else if (raw.raw_type == TDMT_VND_DROP_STB) {
    return taosDropStb(taos, raw.raw, raw.raw_len);
  } else if (raw.raw_type == TDMT_VND_CREATE_TABLE) {
    return taosCreateTable(taos, raw.raw, raw.raw_len);
  } else if (raw.raw_type == TDMT_VND_ALTER_TABLE) {
    return taosAlterTable(taos, raw.raw, raw.raw_len);
  } else if (raw.raw_type == TDMT_VND_DROP_TABLE) {
    return taosDropTable(taos, raw.raw, raw.raw_len);
  } else if (raw.raw_type == TDMT_VND_DELETE) {
    return taosDeleteData(taos, raw.raw, raw.raw_len);
  } else if (raw.raw_type == RES_TYPE__TMQ) {
    return tmqWriteRawDataImpl(taos, raw.raw, raw.raw_len);
  } else if (raw.raw_type == RES_TYPE__TMQ_METADATA) {
    return tmqWriteRawMetaDataImpl(taos, raw.raw, raw.raw_len);
  }
  return TSDB_CODE_INVALID_PARA;
}
