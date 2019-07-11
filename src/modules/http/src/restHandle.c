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

#include "restHandle.h"
#include "restJson.h"

static HttpDecodeMethod restDecodeMethod = {"rest", restProcessRequest};
static HttpDecodeMethod restDecodeMethod2 = {"restful", restProcessRequest};
static HttpEncodeMethod restEncodeSqlMethod = {
    restStartSqlJson, restStopSqlJson, restBuildSqlJson, restBuildSqlAffectRowsJson, NULL, NULL, NULL, NULL};
static HttpEncodeMethod restEncodeSqlTimeMethod = {
    restStartSqlJson, restStopSqlJson, restBuildSqlTimeJson, restBuildSqlAffectRowsJson, NULL, NULL, NULL, NULL};

void restInitHandle(HttpServer* pServer) {
  httpAddMethod(pServer, &restDecodeMethod);
  httpAddMethod(pServer, &restDecodeMethod2);
}

bool restGetUserFromUrl(HttpContext* pContext) {
  HttpParser* pParser = &pContext->pThread->parser;
  if (pParser->path[REST_USER_URL_POS].len > TSDB_USER_LEN - 1 || pParser->path[REST_USER_URL_POS].len <= 0) {
    return false;
  }

  strcpy(pContext->user, pParser->path[REST_USER_URL_POS].pos);
  return true;
}

bool restGetPassFromUrl(HttpContext* pContext) {
  HttpParser* pParser = &pContext->pThread->parser;
  if (pParser->path[REST_PASS_URL_POS].len > TSDB_PASSWORD_LEN - 1 || pParser->path[REST_PASS_URL_POS].len <= 0) {
    return false;
  }

  strcpy(pContext->pass, pParser->path[REST_PASS_URL_POS].pos);
  return true;
}

bool restProcessLoginRequest(HttpContext* pContext) {
  httpTrace("context:%p, fd:%d, ip:%s, user:%s, process restful login msg", pContext, pContext->fd, pContext->ipstr,
            pContext->user);
  pContext->reqType = HTTP_REQTYPE_LOGIN;
  return true;
}

bool restProcessSqlRequest(HttpContext* pContext, int isSqlT) {
  httpTrace("context:%p, fd:%d, ip:%s, user:%s, process restful sql msg", pContext, pContext->fd, pContext->ipstr,
            pContext->user);

  char* sql = pContext->pThread->parser.data.pos;
  if (sql == NULL) {
    httpSendErrorResp(pContext, HTTP_NO_SQL_INPUT);
    return false;
  }

  if (httpCheckUsedbSql(sql)) {
    httpSendErrorResp(pContext, HTTP_NO_EXEC_USEDB);
    return false;
  }

  HttpSqlCmd* cmd = &(pContext->singleCmd);
  cmd->nativSql = sql;

  pContext->reqType = HTTP_REQTYPE_SINGLE_SQL;
  if (!isSqlT)
    pContext->encodeMethod = &restEncodeSqlMethod;
  else
    pContext->encodeMethod = &restEncodeSqlTimeMethod;

  return true;
}

bool restProcessRequest(struct HttpContext* pContext) {
  if (httpUrlMatch(pContext, REST_ACTION_URL_POS, "login")) {
    restGetUserFromUrl(pContext);
    restGetPassFromUrl(pContext);
  }

  if (strlen(pContext->user) == 0 || strlen(pContext->pass) == 0) {
    httpSendErrorResp(pContext, HTTP_PARSE_USR_ERROR);
    return false;
  }

  if (httpUrlMatch(pContext, REST_ACTION_URL_POS, "sql")) {
    return restProcessSqlRequest(pContext, 0);
  } else if (httpUrlMatch(pContext, REST_ACTION_URL_POS, "sqlt")) {
    return restProcessSqlRequest(pContext, 1);
  } else if (httpUrlMatch(pContext, REST_ACTION_URL_POS, "login")) {
    return restProcessLoginRequest(pContext);
  } else {
  }

  httpSendErrorResp(pContext, HTTP_PARSE_URL_ERROR);
  return false;
}
