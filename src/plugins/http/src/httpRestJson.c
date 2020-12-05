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
#include "httpRestHandle.h"
#include "httpRestJson.h"

void restBuildSqlAffectRowsJson(HttpContext *pContext, HttpSqlCmd *cmd, int32_t affect_rows) {
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

void restStartSqlJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  TAOS_FIELD *fields = taos_fetch_fields(result);
  int32_t     num_fields = taos_num_fields(result);

  httpInitJsonBuf(jsonBuf, pContext);
  httpWriteJsonBufHead(jsonBuf);

  // object begin
  httpJsonToken(jsonBuf, JsonObjStt);

  // status, and data
  httpJsonItemToken(jsonBuf);
  httpJsonPair(jsonBuf, REST_JSON_STATUS, REST_JSON_STATUS_LEN, REST_JSON_SUCCESS, REST_JSON_SUCCESS_LEN);

  // head begin
  httpJsonItemToken(jsonBuf);
  httpJsonPairHead(jsonBuf, REST_JSON_HEAD, REST_JSON_HEAD_LEN);
  // head array begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonArrStt);

  if (num_fields == 0) {
    httpJsonItemToken(jsonBuf);
    httpJsonString(jsonBuf, REST_JSON_AFFECT_ROWS, REST_JSON_AFFECT_ROWS_LEN);
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
  httpJsonPairHead(jsonBuf, REST_JSON_DATA, REST_JSON_DATA_LEN);
  // data array begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonArrStt);
}

bool restBuildSqlJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result, int32_t numOfRows, int32_t timestampFormat) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return false;

  int32_t     num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  for (int32_t k = 0; k < numOfRows; ++k) {
    TAOS_ROW row = taos_fetch_row(result);
    if (row == NULL) {
      continue;
    }
    int32_t* length = taos_fetch_lengths(result);

    // data row array begin
    httpJsonItemToken(jsonBuf);
    httpJsonToken(jsonBuf, JsonArrStt);

    for (int32_t i = 0; i < num_fields; i++) {
      httpJsonItemToken(jsonBuf);

      if (row[i] == NULL) {
        httpJsonOriginString(jsonBuf, "null", 4);
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
          httpJsonFloat(jsonBuf, GET_FLOAT_VAL(row[i]));
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          httpJsonDouble(jsonBuf, GET_DOUBLE_VAL(row[i]));
          break;
        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_NCHAR:
          httpJsonStringForTransMean(jsonBuf, (char*)row[i], length[i]);
          break;
        case TSDB_DATA_TYPE_TIMESTAMP:
          if (timestampFormat == REST_TIMESTAMP_FMT_LOCAL_STRING) {
            httpJsonTimestamp(jsonBuf, *((int64_t *)row[i]), taos_result_precision(result) == TSDB_TIME_PRECISION_MICRO);
          } else if (timestampFormat == REST_TIMESTAMP_FMT_TIMESTAMP) {
            httpJsonInt64(jsonBuf, *((int64_t *)row[i]));
          } else {
            httpJsonUtcTimestamp(jsonBuf, *((int64_t *)row[i]), taos_result_precision(result) == TSDB_TIME_PRECISION_MICRO);
          }
          break;
        default:
          break;
      }
    }

    // data row array end
    httpJsonToken(jsonBuf, JsonArrEnd);   
    cmd->numOfRows ++;

    if (pContext->fd <= 0) {
      httpError("context:%p, fd:%d, user:%s, conn closed, abort retrieve", pContext, pContext->fd, pContext->user);
      return false;
    }

    if (cmd->numOfRows >= tsRestRowLimit) {
      httpDebug("context:%p, fd:%d, user:%s, retrieve rows:%d larger than limit:%d, abort retrieve", pContext,
                pContext->fd, pContext->user, cmd->numOfRows, tsRestRowLimit);
      return false;
    }
  }

  httpDebug("context:%p, fd:%d, user:%s, retrieved row:%d", pContext, pContext->fd, pContext->user, cmd->numOfRows);
  return true;
}

bool restBuildSqlTimestampJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result, int32_t numOfRows) {
  return restBuildSqlJson(pContext,cmd, result, numOfRows, REST_TIMESTAMP_FMT_TIMESTAMP);
}

bool restBuildSqlLocalTimeStringJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result, int32_t numOfRows) {
  return restBuildSqlJson(pContext,cmd, result, numOfRows, REST_TIMESTAMP_FMT_LOCAL_STRING);
}

bool restBuildSqlUtcTimeStringJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result, int32_t numOfRows) {
  return restBuildSqlJson(pContext,cmd, result, numOfRows, REST_TIMESTAMP_FMT_UTC_STRING);
}

void restStopSqlJson(HttpContext *pContext, HttpSqlCmd *cmd) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  // data array end
  httpJsonToken(jsonBuf, JsonArrEnd);

  // rows
  httpJsonItemToken(jsonBuf);
  httpJsonPairHead(jsonBuf, REST_JSON_ROWS, REST_JSON_ROWS_LEN);
  httpJsonInt64(jsonBuf, cmd->numOfRows);

  // object end
  httpJsonToken(jsonBuf, JsonObjEnd);

  httpWriteJsonBufEnd(jsonBuf);
}
