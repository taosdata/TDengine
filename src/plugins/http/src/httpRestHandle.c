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
#include "taoserror.h"
#include "httpLog.h"
#include "httpRestHandle.h"
#include "httpRestJson.h"

static HttpDecodeMethod restDecodeMethod = {"rest", restProcessRequest};
static HttpDecodeMethod restDecodeMethod2 = {"restful", restProcessRequest};
static HttpEncodeMethod restEncodeSqlTimestampMethod = {
  .startJsonFp          = restStartSqlJson,         
  .stopJsonFp           = restStopSqlJson, 
  .buildQueryJsonFp     = restBuildSqlTimestampJson,
  .buildAffectRowJsonFp = restBuildSqlAffectRowsJson, 
  .initJsonFp           = NULL, 
  .cleanJsonFp          = NULL,
  .checkFinishedFp      = NULL,
  .setNextCmdFp         = NULL
};

static HttpEncodeMethod restEncodeSqlLocalTimeStringMethod = {
  .startJsonFp          = restStartSqlJson,         
  .stopJsonFp           = restStopSqlJson, 
  .buildQueryJsonFp     = restBuildSqlLocalTimeStringJson,
  .buildAffectRowJsonFp = restBuildSqlAffectRowsJson, 
  .initJsonFp           = NULL, 
  .cleanJsonFp          = NULL,
  .checkFinishedFp      = NULL,
  .setNextCmdFp         = NULL
};

static HttpEncodeMethod restEncodeSqlUtcTimeStringMethod = {
  .startJsonFp          = restStartSqlJson,         
  .stopJsonFp           = restStopSqlJson, 
  .buildQueryJsonFp     = restBuildSqlUtcTimeStringJson,
  .buildAffectRowJsonFp = restBuildSqlAffectRowsJson, 
  .initJsonFp           = NULL, 
  .cleanJsonFp          = NULL,
  .checkFinishedFp      = NULL,
  .setNextCmdFp         = NULL
};

void restInitHandle(HttpServer* pServer) {
  httpAddMethod(pServer, &restDecodeMethod);
  httpAddMethod(pServer, &restDecodeMethod2);
}

bool restGetUserFromUrl(HttpContext* pContext) {
  HttpParser* pParser = pContext->parser;
  if (pParser->path[REST_USER_URL_POS].pos >= TSDB_USER_LEN || pParser->path[REST_USER_URL_POS].pos <= 0) {
    return false;
  }

  tstrncpy(pContext->user, pParser->path[REST_USER_URL_POS].str, TSDB_USER_LEN);
  return true;
}

bool restGetPassFromUrl(HttpContext* pContext) {
  HttpParser* pParser = pContext->parser;
  if (pParser->path[REST_PASS_URL_POS].pos >= TSDB_KEY_LEN || pParser->path[REST_PASS_URL_POS].pos <= 0) {
    return false;
  }

  tstrncpy(pContext->pass, pParser->path[REST_PASS_URL_POS].str, TSDB_KEY_LEN);
  return true;
}

bool restProcessLoginRequest(HttpContext* pContext) {
  httpDebug("context:%p, fd:%d, user:%s, process restful login msg", pContext, pContext->fd, pContext->user);
  pContext->reqType = HTTP_REQTYPE_LOGIN;
  return true;
}

bool restProcessSqlRequest(HttpContext* pContext, int32_t timestampFmt) {
  httpDebug("context:%p, fd:%d, user:%s, process restful sql msg", pContext, pContext->fd, pContext->user);

  char* sql = pContext->parser->body.str;
  if (sql == NULL) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_SQL_INPUT);
    return false;
  }


  /*
   * for async test
   *
  if (httpCheckUsedbSql(sql)) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_EXEC_USEDB);
    return false;
  }
  */

  HttpSqlCmd* cmd = &(pContext->singleCmd);
  cmd->nativSql = sql;

  pContext->reqType = HTTP_REQTYPE_SINGLE_SQL;
  if (timestampFmt == REST_TIMESTAMP_FMT_LOCAL_STRING) {
    pContext->encodeMethod = &restEncodeSqlLocalTimeStringMethod;
  } else if (timestampFmt == REST_TIMESTAMP_FMT_TIMESTAMP) {
    pContext->encodeMethod = &restEncodeSqlTimestampMethod;
  } else if (timestampFmt == REST_TIMESTAMP_FMT_UTC_STRING) {
    pContext->encodeMethod = &restEncodeSqlUtcTimeStringMethod;
  }

  return true;
}

bool restProcessRequest(struct HttpContext* pContext) {
  if (httpUrlMatch(pContext, REST_ACTION_URL_POS, "login")) {
    restGetUserFromUrl(pContext);
    restGetPassFromUrl(pContext);
  }

  if (strlen(pContext->user) == 0 || strlen(pContext->pass) == 0) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_AUTH_INFO);
    return false;
  }

  if (httpUrlMatch(pContext, REST_ACTION_URL_POS, "sql")) {
    return restProcessSqlRequest(pContext, REST_TIMESTAMP_FMT_LOCAL_STRING);
  } else if (httpUrlMatch(pContext, REST_ACTION_URL_POS, "sqlt")) {
    return restProcessSqlRequest(pContext, REST_TIMESTAMP_FMT_TIMESTAMP);
  } else if (httpUrlMatch(pContext, REST_ACTION_URL_POS, "sqlutc")) {
    return restProcessSqlRequest(pContext, REST_TIMESTAMP_FMT_UTC_STRING);
  } else if (httpUrlMatch(pContext, REST_ACTION_URL_POS, "login")) {
    return restProcessLoginRequest(pContext);
  } else {
  }

  httpSendErrorResp(pContext, TSDB_CODE_HTTP_INVLALID_URL);
  return false;
}
