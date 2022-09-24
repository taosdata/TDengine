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
#include "tglobal.h"

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
  if (pParser->path[REST_USER_USEDB_URL_POS].pos >= TSDB_USER_LEN || pParser->path[REST_USER_USEDB_URL_POS].pos <= 0) {
    return false;
  }

  tstrncpy(pContext->user, pParser->path[REST_USER_USEDB_URL_POS].str, TSDB_USER_LEN);
  return true;
}

bool restGetPassFromUrl(HttpContext* pContext) {
  HttpParser* pParser = pContext->parser;
  if (pParser->path[REST_PASS_URL_POS].pos >= HTTP_PASSWORD_LEN || pParser->path[REST_PASS_URL_POS].pos <= 0) {
    return false;
  }

  tstrncpy(pContext->pass, pParser->path[REST_PASS_URL_POS].str, HTTP_PASSWORD_LEN);
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

  /* find if there is db_name in url */
  pContext->db[0] = '\0';

  HttpString *path = &pContext->parser->path[REST_USER_USEDB_URL_POS];
  if (tsHttpDbNameMandatory) {
    if (path->pos == 0) {
      httpError("context:%p, fd:%d, user:%s, database name is mandatory", pContext, pContext->fd, pContext->user);
      httpSendErrorResp(pContext, TSDB_CODE_HTTP_INVALID_URL);
      return false;
    }
  }

  if (path->pos > 0 && !(strlen(sql) > 4 && (sql[0] == 'u' || sql[0] == 'U') &&
      (sql[1] == 's' || sql[1] == 'S') && (sql[2] == 'e' || sql[2] == 'E') && sql[3] == ' '))
  {
    snprintf(pContext->db, /*TSDB_ACCT_ID_LEN + */TSDB_DB_NAME_LEN, "%s", path->str);
  }

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

#define REST_FUNC_URL_POS   2
#define REST_OUTP_URL_POS   3
#define REST_AGGR_URL_POS   4
#define REST_BUFF_URL_POS   5

#define HTTP_FUNC_LEN  32
#define HTTP_OUTP_LEN  16
#define HTTP_AGGR_LEN  2
#define HTTP_BUFF_LEN  32

static int udfSaveFile(const char *fname, const char *content, long len) {
  int fd = open(fname, O_WRONLY | O_CREAT | O_EXCL | O_BINARY, 0755);
  if (fd < 0)
    return -1;
  if (taosWrite(fd, (void *)content, len) < 0)
    return -1;

  return 0;
}

static bool restProcessUdfRequest(HttpContext* pContext) {
  HttpParser* pParser = pContext->parser;
  if (pParser->path[REST_FUNC_URL_POS].pos >= HTTP_FUNC_LEN || pParser->path[REST_FUNC_URL_POS].pos <= 0) {
    return false;
  }

  if (pParser->path[REST_OUTP_URL_POS].pos >= HTTP_OUTP_LEN || pParser->path[REST_OUTP_URL_POS].pos <= 0) {
    return false;
  }

  if (pParser->path[REST_AGGR_URL_POS].pos >= HTTP_AGGR_LEN || pParser->path[REST_AGGR_URL_POS].pos <= 0) {
    return false;
  }

  if (pParser->path[REST_BUFF_URL_POS].pos >= HTTP_BUFF_LEN || pParser->path[REST_BUFF_URL_POS].pos <= 0) {
    return false;
  }

  char* sql = pContext->parser->body.str;
  int len = pContext->parser->body.pos;
  if (sql == NULL) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_SQL_INPUT);
    return false;
  }

  char udfDir[256] = {0};
  char buf[64] = "udf-";
  char funcName[64] = {0};
  int aggr = 0;
  char outputType[16] = {0};
  char buffSize[32] = {0};

  tstrncpy(funcName, pParser->path[REST_FUNC_URL_POS].str, HTTP_FUNC_LEN);
  tstrncpy(buf + 4, funcName, HTTP_FUNC_LEN);

  if (pParser->path[REST_AGGR_URL_POS].str[0] != '0') {
    aggr = 1;
  }

  tstrncpy(outputType, pParser->path[REST_OUTP_URL_POS].str, HTTP_OUTP_LEN);
  tstrncpy(buffSize, pParser->path[REST_BUFF_URL_POS].str, HTTP_BUFF_LEN);

  taosGetTmpfilePath(funcName, udfDir);

  udfSaveFile(udfDir, sql, len);

  tfree(sql);
  pContext->parser->body.str = malloc(1024);
  sql = pContext->parser->body.str;
  int sqlLen = sprintf(sql, "create %s function %s as \"%s\" outputtype %s bufsize %s",
          aggr == 1  ? "aggregate" : " ", funcName, udfDir, outputType, buffSize);

  pContext->parser->body.pos = sqlLen;
  pContext->parser->body.size = sqlLen + 1;

  HttpSqlCmd* cmd = &(pContext->singleCmd);
  cmd->nativSql = sql;

  pContext->reqType = HTTP_REQTYPE_SINGLE_SQL;
  pContext->encodeMethod = &restEncodeSqlLocalTimeStringMethod;

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
  } else if (httpUrlMatch(pContext, REST_ACTION_URL_POS, "udf")) {
    return restProcessUdfRequest(pContext);
  } else {
  }

  httpSendErrorResp(pContext, TSDB_CODE_HTTP_INVALID_URL);
  return false;
}
