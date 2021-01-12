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
#include "httpGcHandle.h"
#include "httpGcJson.h"
#include "httpJson.h"
#include "httpResp.h"

unsigned char *base64_decode(const char *value, int inlen, int *outlen);

void gcInitQueryJson(HttpContext *pContext) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  httpInitJsonBuf(jsonBuf, pContext);
  httpWriteJsonBufHead(jsonBuf);

  // data array begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonArrStt);
}

void gcCleanQueryJson(HttpContext *pContext) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  // array end
  httpJsonToken(jsonBuf, JsonArrEnd);

  httpWriteJsonBufEnd(jsonBuf);
}

void gcWriteTargetStartJson(JsonBuf *jsonBuf, char *refId, char *target) {
  if (strlen(target) == 0) {
    target = refId;
  }

  // object begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonObjStt);

  // target section
  httpJsonPair(jsonBuf, "refId", 5, refId, (int32_t)strlen(refId));
  httpJsonPair(jsonBuf, "target", 6, target, (int32_t)strlen(target));

  // data begin
  httpJsonPairHead(jsonBuf, "datapoints", 10);

  // data array begin
  httpJsonToken(jsonBuf, JsonArrStt);
}

void gcWriteTargetEndJson(JsonBuf *jsonBuf) {
  // data array end
  httpJsonToken(jsonBuf, JsonArrEnd);

  // object end
  httpJsonToken(jsonBuf, JsonObjEnd);
}

void gcStopQueryJson(HttpContext *pContext, HttpSqlCmd *cmd) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  // write end of target
  if (cmd->numOfRows != 0) {
    gcWriteTargetEndJson(jsonBuf);
  }
}

bool gcBuildQueryJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result, int32_t numOfRows) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return false;

  int32_t  num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);
  if (num_fields == 0) {
    return false;
  }

  int32_t precision = taos_result_precision(result);

  // such as select count(*) from sys.cpu
  // such as select count(*) from sys.cpu group by ipaddr
  // such as select count(*) from sys.cpu interval(1d)
  // such as select count(*) from sys.cpu interval(1d) group by ipaddr
  // such as select count(*) count(*) from sys.cpu group by ipaddr interval(1d)
  int32_t dataFields = -1;
  int32_t groupFields = -1;
  bool hasTimestamp = fields[0].type == TSDB_DATA_TYPE_TIMESTAMP;
  if (hasTimestamp) {
    dataFields = 1;
    if (num_fields > 2) groupFields = num_fields - 1;
  } else {
    dataFields = 0;
    if (num_fields > 1) groupFields = num_fields - 1;
  }

  char *refIdBuffer = httpGetCmdsString(pContext, cmd->values);
  char *aliasBuffer = httpGetCmdsString(pContext, cmd->table);
  char *targetBuffer = httpGetCmdsString(pContext, cmd->timestamp);

  if (groupFields == -1 && cmd->numOfRows == 0) {
    gcWriteTargetStartJson(jsonBuf, refIdBuffer, aliasBuffer);
  }
  cmd->numOfRows += numOfRows;

  for (int32_t k = 0; k < numOfRows; ++k) {
    TAOS_ROW row = taos_fetch_row(result);
    if (row == NULL) {
      cmd->numOfRows--;
      continue;
    }
    int32_t* length = taos_fetch_lengths(result);

    // for group by
    if (groupFields != -1) {
      char target[HTTP_GC_TARGET_SIZE] = {0};
      int32_t len;
      len = snprintf(target,HTTP_GC_TARGET_SIZE,"%s{",aliasBuffer);
      for (int32_t i = dataFields + 1; i<num_fields; i++){
          switch (fields[i].type) {
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_TINYINT:
            len += snprintf(target + len, HTTP_GC_TARGET_SIZE - len, "%s:%d", fields[i].name, *((int8_t *)row[i]));
            break;
          case TSDB_DATA_TYPE_SMALLINT:
            len += snprintf(target + len, HTTP_GC_TARGET_SIZE - len, "%s:%d", fields[i].name, *((int16_t *)row[i]));
            break;
          case TSDB_DATA_TYPE_INT:
            len += snprintf(target + len, HTTP_GC_TARGET_SIZE - len, "%s:%d,", fields[i].name, *((int32_t *)row[i]));
            break;
          case TSDB_DATA_TYPE_BIGINT:
            len += snprintf(target + len, HTTP_GC_TARGET_SIZE - len, "%s:%" PRId64, fields[i].name, *((int64_t *)row[i]));
            break;
          case TSDB_DATA_TYPE_FLOAT:
            len += snprintf(target + len, HTTP_GC_TARGET_SIZE - len, "%s:%.5f", fields[i].name, GET_FLOAT_VAL(row[i]));
            break;
          case TSDB_DATA_TYPE_DOUBLE:
            len += snprintf(target + len, HTTP_GC_TARGET_SIZE - len, "%s:%.9f", fields[i].name, GET_DOUBLE_VAL(row[i]));
            break;
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR:
            if (row[i]!= NULL){            
              len += snprintf(target + len, HTTP_GC_TARGET_SIZE - len, "%s:", fields[i].name);
              memcpy(target + len, (char *) row[i], length[i]);
              len = (int32_t)strlen(target);
            }
            break;
          default:
            len += snprintf(target + len, HTTP_GC_TARGET_SIZE - len, "%s:%s", fields[i].name, "-");
            break;
        }
        if(i < num_fields - 1 ){
            len += snprintf(target + len, HTTP_GC_TARGET_SIZE - len, ", ");
        }

      }
      len += snprintf(target + len, HTTP_GC_TARGET_SIZE - len, "}");

      if (strcmp(target, targetBuffer) != 0) {
        // first target not write this section
        if (strlen(targetBuffer) != 0) {
          gcWriteTargetEndJson(jsonBuf);
        }

        // start new target
        gcWriteTargetStartJson(jsonBuf, refIdBuffer, target);
        strncpy(targetBuffer, target, HTTP_GC_TARGET_SIZE);
      }
    }  // end of group by

    // data row array begin
    httpJsonItemToken(jsonBuf);
    httpJsonToken(jsonBuf, JsonArrStt);

    for (int32_t i = dataFields; i >= 0; i--) {
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
          httpJsonStringForTransMean(jsonBuf, (char*)row[i], fields[i].bytes);
          break;
        case TSDB_DATA_TYPE_TIMESTAMP:
          if (precision == TSDB_TIME_PRECISION_MILLI) { //ms
            httpJsonInt64(jsonBuf, *((int64_t *)row[i]));
          } else {
            httpJsonInt64(jsonBuf, *((int64_t *)row[i]) / 1000);
          }
          break;
        default:
          httpJsonString(jsonBuf, "-", 1);
          break;
      }
    }

    if (dataFields == 0) {
      httpJsonItemToken(jsonBuf);
      httpJsonString(jsonBuf, "-", 1);
    }

    // data row array end
    httpJsonToken(jsonBuf, JsonArrEnd);
  }

  return true;
}

void gcSendHeartBeatResp(HttpContext *pContext, HttpSqlCmd *cmd) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  char *desc = "Grafana server receive a quest from you!";

  httpInitJsonBuf(jsonBuf, pContext);

  httpJsonToken(jsonBuf, JsonObjStt);
  httpJsonPair(jsonBuf, "message", (int32_t)strlen("message"), desc, (int32_t)strlen(desc));
  httpJsonToken(jsonBuf, JsonObjEnd);

  char head[1024];

  int32_t hLen = sprintf(head, httpRespTemplate[HTTP_RESPONSE_GRAFANA], httpVersionStr[pContext->parser->httpVersion],
                         httpKeepAliveStr[pContext->parser->keepAlive], (jsonBuf->lst - jsonBuf->buf));
  httpWriteBuf(pContext, head, hLen);
  httpWriteBuf(pContext, jsonBuf->buf, (int32_t)(jsonBuf->lst - jsonBuf->buf));
}
