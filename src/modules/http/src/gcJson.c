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

#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "gcHandle.h"
#include "gcJson.h"
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
  httpJsonPair(jsonBuf, "refId", 5, refId, (int)strlen(refId));
  httpJsonPair(jsonBuf, "target", 6, target, (int)strlen(target));

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
  gcWriteTargetEndJson(jsonBuf);
}

bool gcBuildQueryJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result, int numOfRows) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return false;

  int         num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);
  if (num_fields == 0) {
    return false;
  }

  // such as select count(*) from sys.cpu
  // such as select count(*) from sys.cpu group by ipaddr
  // such as select count(*) from sys.cpu interval(1d)
  // such as select count(*) from sys.cpu interval(1d) group by ipaddr
  // such as select count(*) count(*) from sys.cpu group by ipaddr interval(1d)
  int  dataFields = -1;
  int  groupFields = -1;
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
    cmd->numOfRows += numOfRows;
  }

  for (int i = 0; i < numOfRows; ++i) {
    TAOS_ROW row = taos_fetch_row(result);

    // for group by
    if (groupFields != -1) {
      char target[HTTP_GC_TARGET_SIZE];

      switch (fields[groupFields].type) {
        case TSDB_DATA_TYPE_BOOL:
        case TSDB_DATA_TYPE_TINYINT:
          snprintf(target, HTTP_GC_TARGET_SIZE, "%s%d", aliasBuffer, *((int8_t *)row[groupFields]));
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          snprintf(target, HTTP_GC_TARGET_SIZE, "%s%d", aliasBuffer, *((int16_t *)row[groupFields]));
          break;
        case TSDB_DATA_TYPE_INT:
          snprintf(target, HTTP_GC_TARGET_SIZE, "%s%d", aliasBuffer, *((int32_t *)row[groupFields]));
          break;
        case TSDB_DATA_TYPE_BIGINT:
          snprintf(target, HTTP_GC_TARGET_SIZE, "%s%ld", aliasBuffer, *((int64_t *)row[groupFields]));
          break;
        case TSDB_DATA_TYPE_FLOAT:
          snprintf(target, HTTP_GC_TARGET_SIZE, "%s%.5f", aliasBuffer, *((float *)row[groupFields]));
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          snprintf(target, HTTP_GC_TARGET_SIZE, "%s%.9f", aliasBuffer, *((double *)row[groupFields]));
          break;
        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_NCHAR:
          snprintf(target, HTTP_GC_TARGET_SIZE, "%s%s", aliasBuffer, (char *)row[groupFields]);
          break;
        case TSDB_DATA_TYPE_TIMESTAMP:
          snprintf(target, HTTP_GC_TARGET_SIZE, "%s%ld", aliasBuffer, *((int64_t *)row[groupFields]));
          break;
        default:
          snprintf(target, HTTP_GC_TARGET_SIZE, "%s%s", aliasBuffer, "invalidcol");
          break;
      }

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

    for (int i = dataFields; i >= 0; i--) {
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
          httpJsonInt64(jsonBuf, *((int64_t *)row[i]));
          break;
        default:
          httpJsonString(jsonBuf, "invalidcol", 10);
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
  httpJsonPair(jsonBuf, "message", (int)strlen("message"), desc, (int)strlen(desc));
  httpJsonToken(jsonBuf, JsonObjEnd);

  char head[1024];

  int hLen = sprintf(head, httpRespTemplate[HTTP_RESPONSE_GRAFANA], httpVersionStr[pContext->httpVersion],
                     httpKeepAliveStr[pContext->httpKeepAlive], (jsonBuf->lst - jsonBuf->buf));
  httpWriteBuf(pContext, head, hLen);
  httpWriteBuf(pContext, jsonBuf->buf, (int)(jsonBuf->lst - jsonBuf->buf));
}
