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

#define _DEFAULT_SOURCE
#include "os.h"
#include "tglobal.h"
#include "httpLog.h"
#include "httpJson.h"
#include "httpAdminHandle.h"
#include "httpAdminJson.h"

void adminStartSqlJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  TAOS_FIELD *fields = taos_fetch_fields(result);
  int32_t         num_fields = taos_num_fields(result);

  httpInitJsonBuf(jsonBuf, pContext);
  httpWriteJsonBufHead(jsonBuf);

  // object begin
  httpJsonToken(jsonBuf, JsonObjStt);

  // code
  httpJsonItemToken(jsonBuf);
  httpJsonPair(jsonBuf, ADMIN_JSON_STATUS, ADMIN_JSON_STATUS_LEN, ADMIN_JSON_SUCCESS, ADMIN_JSON_SUCCESS_LEN);

  // head begin
  httpJsonItemToken(jsonBuf);
  httpJsonPairHead(jsonBuf, ADMIN_JSON_HEAD, ADMIN_JSON_HEAD_LEN);
  // head array begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonArrStt);

  if (num_fields == 0) {
    httpJsonItemToken(jsonBuf);
    httpJsonString(jsonBuf, ADMIN_JSON_AFFECT_ROWS, ADMIN_JSON_AFFECT_ROWS_LEN);
  } else {
    for (int32_t i = 0; i < num_fields; ++i) {
      httpJsonItemToken(jsonBuf);
      httpJsonString(jsonBuf, fields[i].name, (int32_t)strlen(fields[i].name));
    }
  }

  // head array end
  httpJsonToken(jsonBuf, JsonArrEnd);

  // data begin
  httpJsonItemToken(jsonBuf);
  httpJsonPairHead(jsonBuf, ADMIN_JSON_DATA, ADMIN_JSON_DATA_LEN);
  // data array begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonArrStt);
}

void adminBuildSqlAffectRowJson(HttpContext *pContext, HttpSqlCmd *cmd, int32_t affect_rows) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  // data row array begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonArrStt);

  httpJsonItemToken(jsonBuf);
  httpJsonInt(jsonBuf, affect_rows);
  // data row array end
  httpJsonToken(jsonBuf, JsonArrEnd);

  cmd->numOfRows = affect_rows;
}

bool adminBuildSqlJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result, int32_t numOfRows) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return false;

  int32_t     num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);
  for (int32_t i = 0; i < numOfRows; ++i) {
    TAOS_ROW row = taos_fetch_row(result);
    int32_t* length = taos_fetch_lengths(result);

    if (cmd->numOfRows >= tsRestRowLimit) {
      break;
    }

    cmd->numOfRows++;

    // data row array begin
    httpJsonItemToken(jsonBuf);
    httpJsonToken(jsonBuf, JsonArrStt);

    for (int32_t i = 0; i < num_fields; i++) {
      httpJsonItemToken(jsonBuf);
      if (row[i] == NULL) {
        httpJsonString(jsonBuf, "NULL", 4);
        continue;
      }

      switch (fields[i].type) {
        case TSDB_DATA_TYPE_BOOL:
        case TSDB_DATA_TYPE_TINYINT:
          httpJsonInt(jsonBuf, *((int8_t *)row[i]));
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          httpJsonInt(jsonBuf, *((int16_t *)row[i]));
          break;
        case TSDB_DATA_TYPE_INT:
          httpJsonInt(jsonBuf, *((int32_t *)row[i]));
          break;
        case TSDB_DATA_TYPE_BIGINT:
          httpJsonInt64(jsonBuf, *((int64_t *)row[i]));
          break;
        case TSDB_DATA_TYPE_FLOAT:
          httpJsonFloat(jsonBuf, *((float *)row[i]));
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          httpJsonDouble(jsonBuf, *((double *)row[i]));
          break;
        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_NCHAR:
          httpJsonStringForTransMean(jsonBuf, row[i], length[i]);
          break;
        case TSDB_DATA_TYPE_TIMESTAMP:
          httpJsonTimestamp(jsonBuf, *((int64_t *)row[i]), taos_result_precision(result) == TSDB_TIME_PRECISION_MICRO);
          break;
        default:
          break;
      }
    }

    // data row array end
    httpJsonToken(jsonBuf, JsonArrEnd);
  }

  if (cmd->numOfRows >= tsRestRowLimit) {
    httpDebug("context:%p, fd:%d, user:%s, retrieve rows:%d larger than limit:%d, abort retrieve", pContext,
              pContext->fd, pContext->user, cmd->numOfRows, tsRestRowLimit);
    return false;
  } else {
    if (pContext->fd <= 0) {
      httpError("context:%p, fd:%d, user:%s, connection is closed, abort retrieve", pContext, pContext->fd,
                pContext->user);
      return false;
    } else {
      httpDebug("context:%p, fd:%d, user:%s, total rows:%d retrieved", pContext, pContext->fd, pContext->user,
                cmd->numOfRows);
      return true;
    }
  }
}

bool adminBuildSqlAllJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result, int32_t numOfRows) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return false;

  int32_t     num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);
  for (int32_t i = 0; i < numOfRows; ++i) {
    TAOS_ROW row = taos_fetch_row(result);

    cmd->numOfRows++;

    // data row array begin
    httpJsonItemToken(jsonBuf);
    httpJsonToken(jsonBuf, JsonArrStt);

    for (int32_t i = 0; i < num_fields; i++) {
      httpJsonItemToken(jsonBuf);
      if (row[i] == NULL) {
        httpJsonString(jsonBuf, "NULL", 4);
        continue;
      }

      switch (fields[i].type) {
        case TSDB_DATA_TYPE_BOOL:
        case TSDB_DATA_TYPE_TINYINT:
          httpJsonInt(jsonBuf, *((int8_t *)row[i]));
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          httpJsonInt(jsonBuf, *((int16_t *)row[i]));
          break;
        case TSDB_DATA_TYPE_INT:
          httpJsonInt(jsonBuf, *((int32_t *)row[i]));
          break;
        case TSDB_DATA_TYPE_BIGINT:
          httpJsonInt64(jsonBuf, *((int64_t *)row[i]));
          break;
        case TSDB_DATA_TYPE_FLOAT:
          httpJsonFloat(jsonBuf, *((float *)row[i]));
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          httpJsonDouble(jsonBuf, *((double *)row[i]));
          break;
        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_NCHAR:
          httpJsonStringForTransMean(jsonBuf, row[i], fields[i].bytes);
          break;
        case TSDB_DATA_TYPE_TIMESTAMP:
          httpJsonTimestamp(jsonBuf, *((int64_t *)row[i]), taos_result_precision(result) == TSDB_TIME_PRECISION_MICRO);
          break;
        default:
          break;
      }
    }

    // data row array end
    httpJsonToken(jsonBuf, JsonArrEnd);
  }

  return true;
}

void adminStopSqlJson(HttpContext *pContext, HttpSqlCmd *cmd) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  // array end
  httpJsonToken(jsonBuf, JsonArrEnd);

  httpJsonItemToken(jsonBuf);
  httpJsonPairHead(jsonBuf, ADMIN_JSON_ROWS, ADMIN_JSON_ROWS_LEN);
  httpJsonInt(jsonBuf, cmd->numOfRows);

  // object end
  httpJsonToken(jsonBuf, JsonObjEnd);

  httpWriteJsonBufEnd(jsonBuf);
}

void adminInitInfoJson(HttpContext *pContext) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  httpInitJsonBuf(jsonBuf, pContext);
  httpWriteJsonBufHead(jsonBuf);

  // object begin
  httpJsonToken(jsonBuf, JsonObjStt);

  // code and data
  httpJsonItemToken(jsonBuf);
  httpJsonPair(jsonBuf, ADMIN_JSON_STATUS, ADMIN_JSON_STATUS_LEN, ADMIN_JSON_SUCCESS, ADMIN_JSON_SUCCESS_LEN);
  httpJsonItemToken(jsonBuf);
  httpJsonPairHead(jsonBuf, ADMIN_JSON_DATA, ADMIN_JSON_DATA_LEN);

  // array begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonArrStt);

  // obj begin
  httpJsonToken(jsonBuf, JsonObjStt);
}

bool adminBuildInfoJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result, int32_t numOfRows) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return false;

  // hackway
  if (cmd->values == ADMIN_JSON_TABLES_TYPE) {
    int32_t num_fields = taos_num_fields(result);
    for (int32_t i = 0; i < numOfRows; ++i) {
      TAOS_ROW row = taos_fetch_row(result);
      if (num_fields >= 3) {
        int32_t tables = *((int32_t *)row[2]);
        cmd->numOfRows += tables;
      }
    }
  } else {
    cmd->numOfRows += numOfRows;
  }

  return true;
}

void adminStopInfoJson(HttpContext *pContext, HttpSqlCmd *cmd) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  httpJsonItemToken(jsonBuf);

  switch (cmd->values) {
    case ADMIN_JSON_DBS_TYPE:
      httpJsonPairIntVal(jsonBuf, ADMIN_JSON_DBS, ADMIN_JSON_DBS_LEN, cmd->numOfRows);
      break;
    case ADMIN_JSON_TABLES_TYPE:
      httpJsonPairIntVal(jsonBuf, ADMIN_JSON_TABLES, ADMIN_JSON_TABLES_LEN, cmd->numOfRows);
      break;
    case ADMIN_JSON_USERS_TYPE:
      httpJsonPairIntVal(jsonBuf, ADMIN_JSON_USERS, ADMIN_JSON_USERS_LEN, cmd->numOfRows);
      break;
    case ADMIN_JSON_MNODES_TYPE:
      httpJsonPairIntVal(jsonBuf, ADMIN_JSON_MNODES, ADMIN_JSON_MNODES_LEN, cmd->numOfRows);
      break;
    case ADMIN_JSON_DNODES_TYPE:
      httpJsonPairIntVal(jsonBuf, ADMIN_JSON_DNODES, ADMIN_JSON_DNODES_LEN, cmd->numOfRows);
      break;
    default:
      httpJsonPairIntVal(jsonBuf, "unknown", 7, cmd->numOfRows);
      break;
  }
}

void adminCleanInfoJson(HttpContext *pContext) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  // object end
  httpJsonToken(jsonBuf, JsonObjEnd);

  ////array end
  httpJsonToken(jsonBuf, JsonArrEnd);

  // object end
  httpJsonToken(jsonBuf, JsonObjEnd);

  httpWriteJsonBufEnd(jsonBuf);
}

void adminStartMetaJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  httpInitJsonBuf(jsonBuf, pContext);
  httpWriteJsonBufHead(jsonBuf);

  // object begin
  httpJsonToken(jsonBuf, JsonObjStt);

  // status, and data
  httpJsonItemToken(jsonBuf);
  httpJsonPair(jsonBuf, ADMIN_JSON_STATUS, ADMIN_JSON_STATUS_LEN, ADMIN_JSON_SUCCESS, ADMIN_JSON_SUCCESS_LEN);

  // head begin
  httpJsonItemToken(jsonBuf);
  httpJsonPairHead(jsonBuf, ADMIN_JSON_HEAD, ADMIN_JSON_HEAD_LEN);
  // head array begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonArrStt);

  httpJsonItemToken(jsonBuf);
  httpJsonString(jsonBuf, ADMIN_JSON_COL_TYPE, ADMIN_JSON_COL_TYPE_LEN);
  httpJsonItemToken(jsonBuf);
  httpJsonString(jsonBuf, ADMIN_JSON_COL_NAME, ADMIN_JSON_COL_NAME_LEN);
  httpJsonItemToken(jsonBuf);
  httpJsonString(jsonBuf, ADMIN_JSON_COL_BYTES, ADMIN_JSON_COL_BYTES_LEN);

  // head array end
  httpJsonToken(jsonBuf, JsonArrEnd);

  // data begin
  httpJsonItemToken(jsonBuf);
  httpJsonPairHead(jsonBuf, ADMIN_JSON_DATA, ADMIN_JSON_DATA_LEN);
  // data array begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonArrStt);

  int32_t     num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  for (int32_t i = 0; i < num_fields; ++i) {
    // data row array begin
    httpJsonItemToken(jsonBuf);
    httpJsonToken(jsonBuf, JsonArrStt);

    httpJsonItemToken(jsonBuf);
    switch (fields[i].type) {
      case TSDB_DATA_TYPE_BOOL:
        httpJsonString(jsonBuf, "bool", 4);
        break;
      case TSDB_DATA_TYPE_TINYINT:
        httpJsonString(jsonBuf, "tinyint", 7);
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        httpJsonString(jsonBuf, "smallint", 8);
        break;
      case TSDB_DATA_TYPE_INT:
        httpJsonString(jsonBuf, "int", 3);
        break;
      case TSDB_DATA_TYPE_BIGINT:
        httpJsonString(jsonBuf, "bigint", 6);
        break;
      case TSDB_DATA_TYPE_FLOAT:
        httpJsonString(jsonBuf, "float", 5);
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        httpJsonString(jsonBuf, "double", 6);
        break;
      case TSDB_DATA_TYPE_BINARY:
        httpJsonString(jsonBuf, "binary", 6);
        break;
      case TSDB_DATA_TYPE_NCHAR:
        httpJsonString(jsonBuf, "nchar", 5);
        break;
      case TSDB_DATA_TYPE_TIMESTAMP:
        httpJsonString(jsonBuf, "timestamp", 9);
        break;
      default:
        httpJsonString(jsonBuf, "unknown", 7);
        break;
    }

    httpJsonItemToken(jsonBuf);
    httpJsonString(jsonBuf, fields[i].name, (int32_t)strlen(fields[i].name));
    httpJsonItemToken(jsonBuf);
    httpJsonInt(jsonBuf, fields[i].bytes);

    // data row array end
    httpJsonToken(jsonBuf, JsonArrEnd);
  }

  cmd->numOfRows = num_fields;
}

bool adminBuildMetaJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result, int32_t numOfRows) { return false; }

void adminStopMetaJson(HttpContext *pContext, HttpSqlCmd *cmd) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  int32_t num_fields = cmd->numOfRows;

  // array end
  httpJsonToken(jsonBuf, JsonArrEnd);

  httpJsonItemToken(jsonBuf);
  httpJsonPairHead(jsonBuf, ADMIN_JSON_ROWS, ADMIN_JSON_ROWS_LEN);
  httpJsonInt(jsonBuf, num_fields);

  // object end
  httpJsonToken(jsonBuf, JsonObjEnd);

  httpWriteJsonBufEnd(jsonBuf);
}

void adminInitSqlsJson(HttpContext *pContext) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  httpInitJsonBuf(jsonBuf, pContext);
  httpWriteJsonBufHead(jsonBuf);

  // array begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonArrStt);
}

void adminCleanSqlsJson(HttpContext *pContext) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  // array end
  httpJsonToken(jsonBuf, JsonArrEnd);

  httpWriteJsonBufEnd(jsonBuf);
}

void adminStartSqlsJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  TAOS_FIELD *fields = taos_fetch_fields(result);
  int32_t         num_fields = taos_num_fields(result);

  // object begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonObjStt);

  // code
  httpJsonItemToken(jsonBuf);
  httpJsonPair(jsonBuf, ADMIN_JSON_STATUS, ADMIN_JSON_STATUS_LEN, ADMIN_JSON_SUCCESS, ADMIN_JSON_SUCCESS_LEN);

  // head begin
  httpJsonItemToken(jsonBuf);
  httpJsonPairHead(jsonBuf, ADMIN_JSON_HEAD, ADMIN_JSON_HEAD_LEN);
  // head array begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonArrStt);

  if (num_fields == 0) {
    httpJsonItemToken(jsonBuf);
    httpJsonString(jsonBuf, ADMIN_JSON_AFFECT_ROWS, ADMIN_JSON_AFFECT_ROWS_LEN);
  } else {
    for (int32_t i = 0; i < num_fields; ++i) {
      httpJsonItemToken(jsonBuf);
      httpJsonString(jsonBuf, fields[i].name, (int32_t)strlen(fields[i].name));
    }
  }

  // head array end
  httpJsonToken(jsonBuf, JsonArrEnd);

  // data begin
  httpJsonItemToken(jsonBuf);
  httpJsonPairHead(jsonBuf, ADMIN_JSON_DATA, ADMIN_JSON_DATA_LEN);
  // data array begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonArrStt);
}

void adminStopSqlsJson(HttpContext *pContext, HttpSqlCmd *cmd) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  // array end
  httpJsonToken(jsonBuf, JsonArrEnd);

  httpJsonItemToken(jsonBuf);
  httpJsonPairHead(jsonBuf, ADMIN_JSON_ROWS, ADMIN_JSON_ROWS_LEN);
  httpJsonInt(jsonBuf, cmd->numOfRows);

  // object end
  httpJsonToken(jsonBuf, JsonObjEnd);
}