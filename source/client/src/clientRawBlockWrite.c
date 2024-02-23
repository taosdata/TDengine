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
#include "tdatablock.h"
#include "tdef.h"
#include "tglobal.h"
#include "tmsgtype.h"

#define LOG_ID_TAG   "connId:0x%" PRIx64 ",reqId:0x%" PRIx64
#define LOG_ID_VALUE *(int64_t*)taos, pRequest->requestId

static tb_uid_t processSuid(tb_uid_t suid, char* db) { return suid + MurmurHash3_32(db, strlen(db)); }

static char* buildCreateTableJson(SSchemaWrapper* schemaRow, SSchemaWrapper* schemaTag, char* name, int64_t id,
                                  int8_t t) {
  char*  string = NULL;
  cJSON* json = cJSON_CreateObject();
  if (json == NULL) {
    uError("create json object failed") return NULL;
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
    if (s->type == TSDB_DATA_TYPE_BINARY || s->type == TSDB_DATA_TYPE_VARBINARY || s->type == TSDB_DATA_TYPE_GEOMETRY) {
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
    if (s->type == TSDB_DATA_TYPE_BINARY || s->type == TSDB_DATA_TYPE_VARBINARY || s->type == TSDB_DATA_TYPE_GEOMETRY) {
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
    uError("create json object failed");
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

      if (field->type == TSDB_DATA_TYPE_BINARY || field->type == TSDB_DATA_TYPE_VARBINARY ||
          field->type == TSDB_DATA_TYPE_GEOMETRY) {
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
      if (field->type == TSDB_DATA_TYPE_BINARY || field->type == TSDB_DATA_TYPE_VARBINARY ||
          field->type == TSDB_DATA_TYPE_GEOMETRY) {
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

  uDebug("create stable data:%p", metaRsp);
  // decode and process req
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);

  if (tDecodeSVCreateStbReq(&coder, &req) < 0) {
    goto _err;
  }
  string = buildCreateTableJson(&req.schemaRow, &req.schemaTag, req.name, req.suid, TSDB_SUPER_TABLE);
_err:
  uDebug("create stable return, sql json:%s", string);
  tDecoderClear(&coder);
  return string;
}

static char* processAlterStb(SMqMetaRsp* metaRsp) {
  SVCreateStbReq req = {0};
  SDecoder       coder;
  char*          string = NULL;
  uDebug("alter stable data:%p", metaRsp);

  // decode and process req
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);

  if (tDecodeSVCreateStbReq(&coder, &req) < 0) {
    goto _err;
  }
  string = buildAlterSTableJson(req.alterOriData, req.alterOriDataLen);
_err:
  uDebug("alter stable return, sql json:%s", string);
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
    uError("tTagToValArray failed code:%d", code);
    goto end;
  }

  if (tTagIsJson(pTag)) {
    STag* p = (STag*)pTag;
    if (p->nTag == 0) {
      uError("p->nTag == 0");
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
      char* buf = NULL;
      if (pTagVal->type == TSDB_DATA_TYPE_VARBINARY) {
        buf = taosMemoryCalloc(pTagVal->nData * 2 + 2 + 3, 1);
      } else {
        buf = taosMemoryCalloc(pTagVal->nData + 3, 1);
      }

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
    uError("create json object failed");
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
  uDebug("create table data:%p", metaRsp);
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
  uDebug("create table return, sql json:%s", string);
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pCreateReq = req.pReqs + iReq;
    taosMemoryFreeClear(pCreateReq->comment);
    taosMemoryFreeClear(pCreateReq->sql);
    if (pCreateReq->type == TSDB_CHILD_TABLE) {
      taosArrayDestroy(pCreateReq->ctb.tagName);
    }
  }
  tDecoderClear(&decoder);
  return string;
}

static char* processAutoCreateTable(STaosxRsp* rsp) {
  uDebug("auto create table data:%p", rsp);
  if (rsp->createTableNum <= 0) {
    uError("processAutoCreateTable rsp->createTableNum <= 0");
    goto _exit;
  }

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

    if (pCreateReq[iReq].type != TSDB_CHILD_TABLE) {
      uError("processAutoCreateTable pCreateReq[iReq].type != TSDB_CHILD_TABLE");
      goto _exit;
    }
  }
  string = buildCreateCTableJson(pCreateReq, rsp->createTableNum);
_exit:
  uDebug("auto created table return, sql json:%s", string);
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

  uDebug("alter table data:%p", metaRsp);
  // decode
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  if (tDecodeSVAlterTbReq(&decoder, &vAlterTbReq) < 0) {
    uError("tDecodeSVAlterTbReq error");
    goto _exit;
  }

  json = cJSON_CreateObject();
  if (json == NULL) {
    uError("create json object failed");
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

      if (vAlterTbReq.type == TSDB_DATA_TYPE_BINARY || vAlterTbReq.type == TSDB_DATA_TYPE_VARBINARY ||
          vAlterTbReq.type == TSDB_DATA_TYPE_GEOMETRY) {
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
      if (vAlterTbReq.colModType == TSDB_DATA_TYPE_BINARY || vAlterTbReq.colModType == TSDB_DATA_TYPE_VARBINARY ||
          vAlterTbReq.colModType == TSDB_DATA_TYPE_GEOMETRY) {
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
          if (!tTagIsJson(vAlterTbReq.pTagVal)) {
            uError("processAlterTable isJson false");
            goto _exit;
          }
          buf = parseTagDatatoJson(vAlterTbReq.pTagVal);
        } else {
          if (vAlterTbReq.tagType == TSDB_DATA_TYPE_VARBINARY) {
            buf = taosMemoryCalloc(vAlterTbReq.nTagVal * 2 + 2 + 3, 1);
          } else {
            buf = taosMemoryCalloc(vAlterTbReq.nTagVal + 3, 1);
          }
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
  uDebug("alter table return, sql json:%s", string);
  cJSON_Delete(json);
  tDecoderClear(&decoder);
  return string;
}

static char* processDropSTable(SMqMetaRsp* metaRsp) {
  SDecoder     decoder = {0};
  SVDropStbReq req = {0};
  char*        string = NULL;
  cJSON*       json = NULL;
  uDebug("processDropSTable data:%p", metaRsp);

  // decode
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  if (tDecodeSVDropStbReq(&decoder, &req) < 0) {
    uError("tDecodeSVDropStbReq failed");
    goto _exit;
  }

  json = cJSON_CreateObject();
  if (json == NULL) {
    uError("create json object failed");
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
  uDebug("processDropSTable return, sql json:%s", string);
  cJSON_Delete(json);
  tDecoderClear(&decoder);
  return string;
}
static char* processDeleteTable(SMqMetaRsp* metaRsp) {
  SDeleteRes req = {0};
  SDecoder   coder = {0};
  cJSON*     json = NULL;
  char*      string = NULL;

  uDebug("processDeleteTable data:%p", metaRsp);
  // decode and process req
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);

  tDecoderInit(&coder, data, len);
  if (tDecodeDeleteRes(&coder, &req) < 0) {
    uError("tDecodeDeleteRes failed");
    goto _exit;
  }

  //  getTbName(req.tableFName);
  char sql[256] = {0};
  snprintf(sql, sizeof(sql), "delete from `%s` where `%s` >= %" PRId64 " and `%s` <= %" PRId64, req.tableFName,
           req.tsColName, req.skey, req.tsColName, req.ekey);

  json = cJSON_CreateObject();
  if (json == NULL) {
    uError("creaet json object failed");
    goto _exit;
  }
  cJSON* type = cJSON_CreateString("delete");
  cJSON_AddItemToObject(json, "type", type);
  cJSON* sqlJson = cJSON_CreateString(sql);
  cJSON_AddItemToObject(json, "sql", sqlJson);

  string = cJSON_PrintUnformatted(json);
_exit:
  uDebug("processDeleteTable return, sql json:%s", string);
  cJSON_Delete(json);
  tDecoderClear(&coder);
  return string;
}

static char* processDropTable(SMqMetaRsp* metaRsp) {
  SDecoder         decoder = {0};
  SVDropTbBatchReq req = {0};
  char*            string = NULL;
  cJSON*           json = NULL;

  uDebug("processDropTable data:%p", metaRsp);
  // decode
  void*   data = POINTER_SHIFT(metaRsp->metaRsp, sizeof(SMsgHead));
  int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
  tDecoderInit(&decoder, data, len);
  if (tDecodeSVDropTbBatchReq(&decoder, &req) < 0) {
    uError("tDecodeSVDropTbBatchReq failed");
    goto _exit;
  }

  json = cJSON_CreateObject();
  if (json == NULL) {
    uError("create json object failed");
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
  uDebug("processDropTable return, json sql:%s", string);
  cJSON_Delete(json);
  tDecoderClear(&decoder);
  return string;
}

static int32_t taosCreateStb(TAOS* taos, void* meta, int32_t metaLen) {
  if (taos == NULL || meta == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  SVCreateStbReq req = {0};
  SDecoder       coder;
  SMCreateStbReq pReq = {0};
  int32_t        code = TSDB_CODE_SUCCESS;
  SRequestObj*   pRequest = NULL;

  code = buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return code;
  }
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
  // build create stable
  pReq.pColumns = taosArrayInit(req.schemaRow.nCols, sizeof(SField));
  for (int32_t i = 0; i < req.schemaRow.nCols; i++) {
    SSchema* pSchema = req.schemaRow.pSchema + i;
    SField   field = {.type = pSchema->type, .flags = pSchema->flags, .bytes = pSchema->bytes};
    strcpy(field.name, pSchema->name);
    taosArrayPush(pReq.pColumns, &field);
  }
  pReq.pTags = taosArrayInit(req.schemaTag.nCols, sizeof(SField));
  for (int32_t i = 0; i < req.schemaTag.nCols; i++) {
    SSchema* pSchema = req.schemaTag.pSchema + i;
    SField   field = {.type = pSchema->type, .flags = pSchema->flags, .bytes = pSchema->bytes};
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

  uDebug(LOG_ID_TAG " create stable name:%s suid:%" PRId64 " processSuid:%" PRId64, LOG_ID_VALUE, req.name, req.suid,
         pReq.suid);
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
  uDebug(LOG_ID_TAG " create stable return, msg:%s", LOG_ID_VALUE, tstrerror(code));
  destroyRequest(pRequest);
  tFreeSMCreateStbReq(&pReq);
  tDecoderClear(&coder);
  terrno = code;
  return code;
}

static int32_t taosDropStb(TAOS* taos, void* meta, int32_t metaLen) {
  if (taos == NULL || meta == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  SVDropStbReq req = {0};
  SDecoder     coder = {0};
  SMDropStbReq pReq = {0};
  int32_t      code = TSDB_CODE_SUCCESS;
  SRequestObj* pRequest = NULL;

  code = buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return code;
  }

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
  code = catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }
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
    //    SCatalog* pCatalog = NULL;
    catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCatalog);
    catalogRemoveTableMeta(pCatalog, &tableName);
  }

  code = pRequest->code;
  taosMemoryFree(pCmdMsg.pMsg);

end:
  uDebug(LOG_ID_TAG " drop stable return, msg:%s", LOG_ID_VALUE, tstrerror(code));
  destroyRequest(pRequest);
  tDecoderClear(&coder);
  terrno = code;
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
  if (taos == NULL || meta == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  SVCreateTbBatchReq req = {0};
  SDecoder           coder = {0};
  int32_t            code = TSDB_CODE_SUCCESS;
  SRequestObj*       pRequest = NULL;
  SQuery*            pQuery = NULL;
  SHashObj*          pVgroupHashmap = NULL;

  code = buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return code;
  }

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
    taosArrayPush(pRequest->tableList, &pName);

    SVgroupCreateTableBatch* pTableBatch = taosHashGet(pVgroupHashmap, &pInfo.vgId, sizeof(pInfo.vgId));
    if (pTableBatch == NULL) {
      SVgroupCreateTableBatch tBatch = {0};
      tBatch.info = pInfo;
      strcpy(tBatch.dbName, pRequest->pDb);

      tBatch.req.pArray = taosArrayInit(4, sizeof(struct SVCreateTbReq));
      taosArrayPush(tBatch.req.pArray, pCreateReq);
      tBatch.req.source = TD_REQ_FROM_TAOX;

      taosHashPut(pVgroupHashmap, &pInfo.vgId, sizeof(pInfo.vgId), &tBatch, sizeof(tBatch));
    } else {  // add to the correct vgroup
      taosArrayPush(pTableBatch->req.pArray, pCreateReq);
    }
  }

  if (taosHashGetSize(pVgroupHashmap) == 0) {
    goto end;
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
    removeMeta(pTscObj, pRequest->tableList, false);
  }

  code = pRequest->code;

end:
  uDebug(LOG_ID_TAG " create table return, msg:%s", LOG_ID_VALUE, tstrerror(code));
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
  terrno = code;
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
  if (taos == NULL || meta == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  SVDropTbBatchReq req = {0};
  SDecoder         coder = {0};
  int32_t          code = TSDB_CODE_SUCCESS;
  SRequestObj*     pRequest = NULL;
  SQuery*          pQuery = NULL;
  SHashObj*        pVgroupHashmap = NULL;

  code = buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return code;
  }
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
    //    pDropReq->suid = processSuid(pDropReq->suid, pRequest->pDb);

    SVgroupInfo pInfo = {0};
    SName       pName = {0};
    toName(pTscObj->acctId, pRequest->pDb, pDropReq->name, &pName);
    code = catalogGetTableHashVgroup(pCatalog, &conn, &pName, &pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      goto end;
    }

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

  if (taosHashGetSize(pVgroupHashmap) == 0) {
    goto end;
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
    removeMeta(pTscObj, pRequest->tableList, false);
  }
  code = pRequest->code;

end:
  uDebug(LOG_ID_TAG " drop table return, msg:%s", LOG_ID_VALUE, tstrerror(code));
  taosHashCleanup(pVgroupHashmap);
  destroyRequest(pRequest);
  tDecoderClear(&coder);
  qDestroyQuery(pQuery);
  terrno = code;
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
  if (taos == NULL || meta == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
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

  //  getTbName(req.tableFName);
  snprintf(sql, sizeof(sql), "delete from `%s` where `%s` >= %" PRId64 " and `%s` <= %" PRId64, req.tableFName,
           req.tsColName, req.skey, req.tsColName, req.ekey);

  TAOS_RES*    res = taos_query(taos, sql);
  SRequestObj* pRequest = (SRequestObj*)res;
  code = pRequest->code;
  if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST || code == TSDB_CODE_PAR_GET_META_ERROR) {
    code = TSDB_CODE_SUCCESS;
  }
  taos_free_result(res);

end:
  uDebug("connId:0x%" PRIx64 " delete data sql:%s, code:%s", *(int64_t*)taos, sql, tstrerror(code));
  tDecoderClear(&coder);
  terrno = code;
  return code;
}

static int32_t taosAlterTable(TAOS* taos, void* meta, int32_t metaLen) {
  if (taos == NULL || meta == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  SVAlterTbReq   req = {0};
  SDecoder       dcoder = {0};
  int32_t        code = TSDB_CODE_SUCCESS;
  SRequestObj*   pRequest = NULL;
  SQuery*        pQuery = NULL;
  SArray*        pArray = NULL;
  SVgDataBlocks* pVgData = NULL;

  code = buildRequest(*(int64_t*)taos, "", 0, NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return code;
  }
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

  int         tlen = 0;
  req.source = TD_REQ_FROM_TAOX;
  tEncodeSize(tEncodeSVAlterTbReq, &req, tlen, code);
  if(code != 0){
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  tlen += sizeof(SMsgHead);
  void* pMsg = taosMemoryMalloc(tlen);
  if (NULL == pMsg) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  ((SMsgHead*)pMsg)->vgId = htonl(pInfo.vgId);
  ((SMsgHead*)pMsg)->contLen = htonl(tlen);
  void*    pBuf = POINTER_SHIFT(pMsg, sizeof(SMsgHead));
  SEncoder coder = {0};
  tEncoderInit(&coder, pBuf, tlen - sizeof(SMsgHead));
  code = tEncodeSVAlterTbReq(&coder, &req);
  if(code != 0){
    tEncoderClear(&coder);
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  tEncoderClear(&coder);

  pVgData->pData = pMsg;
  pVgData->size = tlen;

  pVgData->numOfTables = 1;
  taosArrayPush(pArray, &pVgData);

  pQuery = (SQuery*)nodesMakeNode(QUERY_NODE_QUERY);
  if (NULL == pQuery) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
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
  uDebug(LOG_ID_TAG " alter table return, meta:%p, len:%d, msg:%s", LOG_ID_VALUE, meta, metaLen, tstrerror(code));
  taosArrayDestroy(pArray);
  if (pVgData) taosMemoryFreeClear(pVgData->pData);
  taosMemoryFreeClear(pVgData);
  destroyRequest(pRequest);
  tDecoderClear(&dcoder);
  qDestroyQuery(pQuery);
  terrno = code;
  return code;
}

int taos_write_raw_block_with_fields(TAOS* taos, int rows, char* pData, const char* tbname, TAOS_FIELD* fields,
                                     int numFields) {
  return taos_write_raw_block_with_fields_with_reqid(taos, rows, pData, tbname, fields, numFields, 0);
}

int taos_write_raw_block_with_fields_with_reqid(TAOS* taos, int rows, char* pData, const char* tbname,
                                                TAOS_FIELD* fields, int numFields, int64_t reqid) {
  if (!taos || !pData || !tbname) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  int32_t     code = TSDB_CODE_SUCCESS;
  STableMeta* pTableMeta = NULL;
  SQuery*     pQuery = NULL;
  SHashObj*   pVgHash = NULL;

  SRequestObj* pRequest = (SRequestObj*)createRequest(*(int64_t*)taos, TSDB_SQL_INSERT, reqid);
  if (!pRequest) {
    return terrno;
  }

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
  code = catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
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
    goto end;
  }

  code = catalogGetTableMeta(pCatalog, &conn, &pName, &pTableMeta);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }
  //  uError("td23101 0vgId:%d, vgId:%d, name:%s, uid:%"PRIu64, vgData.vgId, pTableMeta->vgId, tbname, pTableMeta->uid);

  pQuery = smlInitHandle();
  if (pQuery == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  pVgHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  taosHashPut(pVgHash, (const char*)&vgData.vgId, sizeof(vgData.vgId), (char*)&vgData, sizeof(vgData));

  code = rawBlockBindData(pQuery, pTableMeta, pData, NULL, fields, numFields, false);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  code = smlBuildOutput(pQuery, pVgHash);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  launchQueryImpl(pRequest, pQuery, true, NULL);
  code = pRequest->code;

end:
  uDebug(LOG_ID_TAG " write raw block with field return, msg:%s", LOG_ID_VALUE, tstrerror(code));
  taosMemoryFreeClear(pTableMeta);
  qDestroyQuery(pQuery);
  destroyRequest(pRequest);
  taosHashCleanup(pVgHash);
  terrno = code;
  return code;
}

int taos_write_raw_block(TAOS* taos, int rows, char* pData, const char* tbname) {
  return taos_write_raw_block_with_reqid(taos, rows, pData, tbname, 0);
}

int taos_write_raw_block_with_reqid(TAOS* taos, int rows, char* pData, const char* tbname, int64_t reqid) {
  if (!taos || !pData || !tbname) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  int32_t     code = TSDB_CODE_SUCCESS;
  STableMeta* pTableMeta = NULL;
  SQuery*     pQuery = NULL;
  SHashObj*   pVgHash = NULL;

  SRequestObj* pRequest = (SRequestObj*)createRequest(*(int64_t*)taos, TSDB_SQL_INSERT, reqid);
  if (!pRequest) {
    return terrno;
  }

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
  code = catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
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
    goto end;
  }

  code = catalogGetTableMeta(pCatalog, &conn, &pName, &pTableMeta);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }
  pQuery = smlInitHandle();
  if (pQuery == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  pVgHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  taosHashPut(pVgHash, (const char*)&vgData.vgId, sizeof(vgData.vgId), (char*)&vgData, sizeof(vgData));

  code = rawBlockBindData(pQuery, pTableMeta, pData, NULL, NULL, 0, false);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  code = smlBuildOutput(pQuery, pVgHash);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  launchQueryImpl(pRequest, pQuery, true, NULL);
  code = pRequest->code;

end:
  uDebug(LOG_ID_TAG " write raw block return, msg:%s", LOG_ID_VALUE, tstrerror(code));
  taosMemoryFreeClear(pTableMeta);
  qDestroyQuery(pQuery);
  destroyRequest(pRequest);
  taosHashCleanup(pVgHash);
  terrno = code;
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
  ASSERT(rawData != NULL);
  return rawData;
}

static int32_t tmqWriteRawDataImpl(TAOS* taos, void* data, int32_t dataLen) {
  if (taos == NULL || data == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  int32_t     code = TSDB_CODE_SUCCESS;
  SHashObj*   pVgHash = NULL;
  SQuery*     pQuery = NULL;
  SMqRspObj   rspObj = {0};
  SDecoder    decoder = {0};
  STableMeta* pTableMeta = NULL;

  terrno = TSDB_CODE_SUCCESS;
  SRequestObj* pRequest = (SRequestObj*)createRequest(*(int64_t*)taos, TSDB_SQL_INSERT, 0);
  if (!pRequest) {
    return terrno;
  }

  uDebug(LOG_ID_TAG " write raw data, data:%p, dataLen:%d", LOG_ID_VALUE, data, dataLen);
  pRequest->syncQuery = true;
  rspObj.resIter = -1;
  rspObj.resType = RES_TYPE__TMQ;

  tDecoderInit(&decoder, data, dataLen);
  code = tDecodeMqDataRsp(&decoder, &rspObj.rsp);
  if (code != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto end;
  }

  if (!pRequest->pDb) {
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }

  struct SCatalog* pCatalog = NULL;
  code = catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  SRequestConnInfo conn = {0};
  conn.pTrans = pRequest->pTscObj->pAppInfo->pTransporter;
  conn.requestId = pRequest->requestId;
  conn.requestObjRefId = pRequest->self;
  conn.mgmtEps = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp);

  pQuery = smlInitHandle();
  if (pQuery == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  pVgHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  while (++rspObj.resIter < rspObj.rsp.blockNum) {
    void* pRetrieve = taosArrayGetP(rspObj.rsp.blockData, rspObj.resIter);
    if (!rspObj.rsp.withSchema) {
      goto end;
    }

    const char* tbName = (const char*)taosArrayGetP(rspObj.rsp.blockTbName, rspObj.resIter);
    if (!tbName) {
      code = TSDB_CODE_TMQ_INVALID_MSG;
      goto end;
    }

    SName pName = {TSDB_TABLE_NAME_T, pRequest->pTscObj->acctId, {0}, {0}};
    strcpy(pName.dbname, pRequest->pDb);
    strcpy(pName.tname, tbName);

    code = catalogGetTableMeta(pCatalog, &conn, &pName, &pTableMeta);
    //    if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
    //      uError("WriteRaw:catalogGetTableMeta table not exist. table name: %s", tbName);
    //      code = TSDB_CODE_SUCCESS;
    //      continue;
    //    }
    if (code != TSDB_CODE_SUCCESS) {
      goto end;
    }

    SVgroupInfo vg;
    code = catalogGetTableHashVgroup(pCatalog, &conn, &pName, &vg);
    if (code != TSDB_CODE_SUCCESS) {
      goto end;
    }

    void* hData = taosHashGet(pVgHash, &vg.vgId, sizeof(vg.vgId));
    if (hData == NULL) {
      taosHashPut(pVgHash, (const char*)&vg.vgId, sizeof(vg.vgId), (char*)&vg, sizeof(vg));
    }

    SSchemaWrapper* pSW = (SSchemaWrapper*)taosArrayGetP(rspObj.rsp.blockSchema, rspObj.resIter);
    TAOS_FIELD*     fields = taosMemoryCalloc(pSW->nCols, sizeof(TAOS_FIELD));
    if (fields == NULL) {
      goto end;
    }
    for (int i = 0; i < pSW->nCols; i++) {
      fields[i].type = pSW->pSchema[i].type;
      fields[i].bytes = pSW->pSchema[i].bytes;
      tstrncpy(fields[i].name, pSW->pSchema[i].name, tListLen(pSW->pSchema[i].name));
    }
    void* rawData = getRawDataFromRes(pRetrieve);
    code = rawBlockBindData(pQuery, pTableMeta, rawData, NULL, fields, pSW->nCols, true);
    taosMemoryFree(fields);
    if (code != TSDB_CODE_SUCCESS) {
      goto end;
    }
    taosMemoryFreeClear(pTableMeta);
  }

  code = smlBuildOutput(pQuery, pVgHash);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  launchQueryImpl(pRequest, pQuery, true, NULL);
  code = pRequest->code;

end:
  uDebug(LOG_ID_TAG " write raw data return, msg:%s", LOG_ID_VALUE, tstrerror(code));
  tDeleteMqDataRsp(&rspObj.rsp);
  tDecoderClear(&decoder);
  qDestroyQuery(pQuery);
  destroyRequest(pRequest);
  taosHashCleanup(pVgHash);
  taosMemoryFreeClear(pTableMeta);
  terrno = code;
  return code;
}

static int32_t tmqWriteRawMetaDataImpl(TAOS* taos, void* data, int32_t dataLen) {
  if (taos == NULL || data == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  int32_t        code = TSDB_CODE_SUCCESS;
  SHashObj*      pVgHash = NULL;
  SQuery*        pQuery = NULL;
  SMqTaosxRspObj rspObj = {0};
  SDecoder       decoder = {0};
  STableMeta*    pTableMeta = NULL;
  SVCreateTbReq* pCreateReqDst = NULL;

  terrno = TSDB_CODE_SUCCESS;
  SRequestObj* pRequest = (SRequestObj*)createRequest(*(int64_t*)taos, TSDB_SQL_INSERT, 0);
  if (!pRequest) {
    return terrno;
  }
  uDebug(LOG_ID_TAG " write raw metadata, data:%p, dataLen:%d", LOG_ID_VALUE, data, dataLen);
  pRequest->syncQuery = true;
  rspObj.resIter = -1;
  rspObj.resType = RES_TYPE__TMQ_METADATA;

  tDecoderInit(&decoder, data, dataLen);
  code = tDecodeSTaosxRsp(&decoder, &rspObj.rsp);
  if (code != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto end;
  }

  if (!pRequest->pDb) {
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }

  struct SCatalog* pCatalog = NULL;
  code = catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  SRequestConnInfo conn = {0};
  conn.pTrans = pRequest->pTscObj->pAppInfo->pTransporter;
  conn.requestId = pRequest->requestId;
  conn.requestObjRefId = pRequest->self;
  conn.mgmtEps = getEpSet_s(&pRequest->pTscObj->pAppInfo->mgmtEp);

  pQuery = smlInitHandle();
  if (pQuery == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  pVgHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);

  uDebug(LOG_ID_TAG " write raw metadata block num:%d", LOG_ID_VALUE, rspObj.rsp.blockNum);
  while (++rspObj.resIter < rspObj.rsp.blockNum) {
    void* pRetrieve = taosArrayGetP(rspObj.rsp.blockData, rspObj.resIter);
    if (!rspObj.rsp.withSchema) {
      goto end;
    }

    const char* tbName = (const char*)taosArrayGetP(rspObj.rsp.blockTbName, rspObj.resIter);
    if (!tbName) {
      code = TSDB_CODE_TMQ_INVALID_MSG;
      goto end;
    }

    uDebug(LOG_ID_TAG " write raw metadata block tbname:%s", LOG_ID_VALUE, tbName);
    SName pName = {TSDB_TABLE_NAME_T, pRequest->pTscObj->acctId, {0}, {0}};
    strcpy(pName.dbname, pRequest->pDb);
    strcpy(pName.tname, tbName);

    // find schema data info
    for (int j = 0; j < rspObj.rsp.createTableNum; j++) {
      void**   dataTmp = taosArrayGet(rspObj.rsp.createTableReq, j);
      int32_t* lenTmp = taosArrayGet(rspObj.rsp.createTableLen, j);

      SDecoder      decoderTmp = {0};
      SVCreateTbReq pCreateReq = {0};
      tDecoderInit(&decoderTmp, *dataTmp, *lenTmp);
      if (tDecodeSVCreateTbReq(&decoderTmp, &pCreateReq) < 0) {
        tDecoderClear(&decoderTmp);
        tDestroySVCreateTbReq(&pCreateReq, TSDB_MSG_FLG_DECODE);
        code = TSDB_CODE_TMQ_INVALID_MSG;
        goto end;
      }

      if (pCreateReq.type != TSDB_CHILD_TABLE) {
        code = TSDB_CODE_TSC_INVALID_VALUE;
        tDecoderClear(&decoderTmp);
        tDestroySVCreateTbReq(&pCreateReq, TSDB_MSG_FLG_DECODE);
        goto end;
      }
      if (strcmp(tbName, pCreateReq.name) == 0) {
        cloneSVreateTbReq(&pCreateReq, &pCreateReqDst);
        //        pCreateReqDst->ctb.suid = processSuid(pCreateReqDst->ctb.suid, pRequest->pDb);
        tDecoderClear(&decoderTmp);
        tDestroySVCreateTbReq(&pCreateReq, TSDB_MSG_FLG_DECODE);
        break;
      }
      tDecoderClear(&decoderTmp);
      tDestroySVCreateTbReq(&pCreateReq, TSDB_MSG_FLG_DECODE);
    }

    SVgroupInfo vg;
    code = catalogGetTableHashVgroup(pCatalog, &conn, &pName, &vg);
    if (code != TSDB_CODE_SUCCESS) {
      goto end;
    }

    if (pCreateReqDst) {  // change stable name to get meta
      strcpy(pName.tname, pCreateReqDst->ctb.stbName);
    }
    code = catalogGetTableMeta(pCatalog, &conn, &pName, &pTableMeta);
    //    if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
    //      uError("WriteRaw:catalogGetTableMeta table not exist. table name: %s", tbName);
    //      code = TSDB_CODE_SUCCESS;
    //      continue;
    //    }
    if (code != TSDB_CODE_SUCCESS) {
      goto end;
    }

    if (pCreateReqDst) {
      pTableMeta->vgId = vg.vgId;
      pTableMeta->uid = pCreateReqDst->uid;
      pCreateReqDst->ctb.suid = pTableMeta->suid;
    }
    void* hData = taosHashGet(pVgHash, &vg.vgId, sizeof(vg.vgId));
    if (hData == NULL) {
      taosHashPut(pVgHash, (const char*)&vg.vgId, sizeof(vg.vgId), (char*)&vg, sizeof(vg));
    }

    SSchemaWrapper* pSW = (SSchemaWrapper*)taosArrayGetP(rspObj.rsp.blockSchema, rspObj.resIter);
    TAOS_FIELD*     fields = taosMemoryCalloc(pSW->nCols, sizeof(TAOS_FIELD));
    if (fields == NULL) {
      goto end;
    }
    for (int i = 0; i < pSW->nCols; i++) {
      fields[i].type = pSW->pSchema[i].type;
      fields[i].bytes = pSW->pSchema[i].bytes;
      tstrncpy(fields[i].name, pSW->pSchema[i].name, tListLen(pSW->pSchema[i].name));
    }
    void* rawData = getRawDataFromRes(pRetrieve);
    code = rawBlockBindData(pQuery, pTableMeta, rawData, &pCreateReqDst, fields, pSW->nCols, true);
    taosMemoryFree(fields);
    if (code != TSDB_CODE_SUCCESS) {
      goto end;
    }
    taosMemoryFreeClear(pTableMeta);
  }

  code = smlBuildOutput(pQuery, pVgHash);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  launchQueryImpl(pRequest, pQuery, true, NULL);
  code = pRequest->code;

end:
  uDebug(LOG_ID_TAG " write raw metadata return, msg:%s", LOG_ID_VALUE, tstrerror(code));
  tDeleteSTaosxRsp(&rspObj.rsp);
  tDecoderClear(&decoder);
  qDestroyQuery(pQuery);
  destroyRequest(pRequest);
  taosHashCleanup(pVgHash);
  taosMemoryFreeClear(pTableMeta);
  if (pCreateReqDst) {
    tdDestroySVCreateTbReq(pCreateReqDst);
    taosMemoryFree(pCreateReqDst);
  }
  terrno = code;
  return code;
}

char* tmq_get_json_meta(TAOS_RES* res) {
  if (res == NULL) return NULL;
  uDebug("tmq_get_json_meta res:%p", res);
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
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  if (TD_RES_TMQ_META(res)) {
    SMqMetaRspObj* pMetaRspObj = (SMqMetaRspObj*)res;
    raw->raw = pMetaRspObj->metaRsp.metaRsp;
    raw->raw_len = pMetaRspObj->metaRsp.metaRspLen;
    raw->raw_type = pMetaRspObj->metaRsp.resMsgType;
    uDebug("tmq get raw type meta:%p", raw);
  } else if (TD_RES_TMQ(res)) {
    SMqRspObj* rspObj = ((SMqRspObj*)res);

    int32_t len = 0;
    int32_t code = 0;
    tEncodeSize(tEncodeMqDataRsp, &rspObj->rsp, len, code);
    if (code < 0) {
      return -1;
    }

    void*    buf = taosMemoryCalloc(1, len);
    SEncoder encoder = {0};
    tEncoderInit(&encoder, buf, len);
    tEncodeMqDataRsp(&encoder, &rspObj->rsp);
    tEncoderClear(&encoder);

    raw->raw = buf;
    raw->raw_len = len;
    raw->raw_type = RES_TYPE__TMQ;
    uDebug("tmq get raw type data:%p", raw);
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
    uDebug("tmq get raw type metadata:%p", raw);
  } else {
    uError("tmq get raw error type:%d", *(int8_t*)res);
    terrno = TSDB_CODE_TMQ_INVALID_MSG;
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

void tmq_free_raw(tmq_raw_data raw) {
  uDebug("tmq free raw data type:%d", raw.raw_type);
  if (raw.raw_type == RES_TYPE__TMQ || raw.raw_type == RES_TYPE__TMQ_METADATA) {
    taosMemoryFree(raw.raw);
  }
}

int32_t tmq_write_raw(TAOS* taos, tmq_raw_data raw) {
  if (!taos) {
    goto end;
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

end:
  terrno = TSDB_CODE_INVALID_PARA;
  return terrno;
}
