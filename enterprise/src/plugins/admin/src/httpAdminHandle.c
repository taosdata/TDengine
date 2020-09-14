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
#include "taoserror.h"
#include "httpAdminHandle.h"
#include "httpAdminJson.h"

static HttpDecodeMethod adminDecodeMethod = {"admin", adminProcessRequest};
static HttpEncodeMethod adminEncodeSqlMethod = {
  .startJsonFp          = adminStartSqlJson,         
  .stopJsonFp           = adminStopSqlJson, 
  .buildQueryJsonFp     = adminBuildSqlJson,
  .buildAffectRowJsonFp = adminBuildSqlAffectRowJson, 
  .initJsonFp           = NULL, 
  .cleanJsonFp          = NULL,
  .checkFinishedFp      = NULL,
  .setNextCmdFp         = NULL
};

static HttpEncodeMethod adminEncodeSqlAllMethod = {
  .startJsonFp          = adminStartSqlJson,         
  .stopJsonFp           = adminStopSqlJson, 
  .buildQueryJsonFp     = adminBuildSqlAllJson,
  .buildAffectRowJsonFp = adminBuildSqlAffectRowJson, 
  .initJsonFp           = NULL, 
  .cleanJsonFp          = NULL,
  .checkFinishedFp      = NULL,
  .setNextCmdFp         = NULL
};

static HttpEncodeMethod adminEncodeInfoMethod =  {
  .startJsonFp          = NULL,         
  .stopJsonFp           = adminStopInfoJson, 
  .buildQueryJsonFp     = adminBuildInfoJson,
  .buildAffectRowJsonFp = NULL, 
  .initJsonFp           = adminInitInfoJson, 
  .cleanJsonFp          = adminCleanInfoJson,
  .checkFinishedFp      = NULL,
  .setNextCmdFp         = NULL
};

static HttpEncodeMethod adminEncodeMetaMethod = {
  .startJsonFp          = adminStartMetaJson,         
  .stopJsonFp           = adminStopMetaJson, 
  .buildQueryJsonFp     = adminBuildMetaJson,
  .buildAffectRowJsonFp = NULL, 
  .initJsonFp           = NULL, 
  .cleanJsonFp          = NULL,
  .checkFinishedFp      = NULL,
  .setNextCmdFp         = NULL
};

static HttpEncodeMethod adminEncodeSqlsMethod =  {
  .startJsonFp          = adminStartSqlsJson,         
  .stopJsonFp           = adminStopSqlsJson, 
  .buildQueryJsonFp     = adminBuildSqlAllJson,
  .buildAffectRowJsonFp = NULL, 
  .initJsonFp           = adminInitSqlsJson, 
  .cleanJsonFp          = adminCleanSqlsJson,
  .checkFinishedFp      = NULL,
  .setNextCmdFp         = NULL
};

void adminInitHandle(HttpServer* pServer) {
  httpAddMethod(pServer, &adminDecodeMethod);
}

bool adminGetUserFromUrl(HttpContext* pContext) {
  HttpParser* pParser = pContext->parser;
  if (pParser->path[ADMIN_USER_URL_POS].pos >= TSDB_USER_LEN || pParser->path[ADMIN_USER_URL_POS].pos <= 0) {
    return false;
  }

  strcpy(pContext->user, pParser->path[ADMIN_USER_URL_POS].str);
  return true;
}

bool adminGetPassFromUrl(HttpContext* pContext) {
  HttpParser* pParser = pContext->parser;
  if (pParser->path[ADMIN_PASS_URL_POS].pos >= TSDB_PASSWORD_LEN || pParser->path[ADMIN_PASS_URL_POS].pos <= 0) {
    return false;
  }

  strcpy(pContext->pass, pParser->path[ADMIN_PASS_URL_POS].str);
  return true;
}

bool adminProcessLoginRequest(HttpContext* pContext) {
  httpDebug("context:%p, fd:%d, user:%s, process admin login msg", pContext, pContext->fd, pContext->user);
  pContext->reqType = HTTP_REQTYPE_LOGIN;
  return true;
}

bool adminProcessLogoutRequest(HttpContext* pContext) {
  httpDebug("context:%p, fd:%d, user:%s, process admin logout msg", pContext, pContext->fd, pContext->user);
  httpSendSuccResp(pContext, "logout success");
  pContext->reqType = HTTP_REQTYPE_OTHERS;
  return false;
}

bool adminProcessSqlRequest(HttpContext* pContext) {
  httpDebug("context:%p, fd:%d, user:%s, process admin query part msg", pContext, pContext->fd, pContext->user);

  char* sql = pContext->parser->body.str;
  if (sql == NULL) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_SQL_INPUT);
    return false;
  }

  if (httpCheckUsedbSql(sql)) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_EXEC_USEDB);
    return false;
  }

  HttpSqlCmd* cmd = &(pContext->singleCmd);
  cmd->nativSql = sql;

  pContext->reqType = HTTP_REQTYPE_SINGLE_SQL;
  pContext->encodeMethod = &adminEncodeSqlMethod;

  return true;
}

bool adminProcessSqlAllRequest(HttpContext* pContext) {
  httpDebug("context:%p, fd:%d, user:%s, process admin query all msg", pContext, pContext->fd, pContext->user);

  char* sql = pContext->parser->body.str;
  if (sql == NULL) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_SQL_INPUT);
    return false;
  }

  if (httpCheckUsedbSql(sql)) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_EXEC_USEDB);
    return false;
  }

  HttpSqlCmd* cmd = &(pContext->singleCmd);
  cmd->nativSql = sql;

  pContext->reqType = HTTP_REQTYPE_SINGLE_SQL;
  pContext->encodeMethod = &adminEncodeSqlAllMethod;

  return true;
}

bool adminProcessSqlsRequest(HttpContext* pContext) {
  httpDebug("context:%p, fd:%d, user:%s, process multi-sqls msg", pContext, pContext->fd, pContext->user);

  char* sql = pContext->parser->body.str;
  if (sql == NULL) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_SQL_INPUT);
    return false;
  }

  int32_t cmdSize = 0;

  for (int32_t i = 0; sql[i] != 0; ++i) {
    if (sql[i] == ';') cmdSize++;
  }

  if (!httpMallocMultiCmds(pContext, cmdSize + 1, HTTP_BUFFER_SIZE)) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_ENOUGH_MEMORY);
    return false;
  }

  while (true) {
    char* dot = strstr(sql, ";");
    if (dot == NULL) break;

    HttpSqlCmd* cmd = httpNewSqlCmd(pContext);
    if (cmd == NULL) {
      httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_ENOUGH_MEMORY);
      return false;
    }

    *dot = 0;
    cmd->sql = httpAddToSqlCmdBuffer(pContext, sql);

    sql = dot + 1;
  }

  pContext->reqType = HTTP_REQTYPE_MULTI_SQL;
  pContext->encodeMethod = &adminEncodeSqlsMethod;

  return true;
}

bool adminProcessInfoRequest(HttpContext* pContext) {
  httpDebug("context:%p, fd:%d, user:%s, process admin info msg", pContext, pContext->fd, pContext->user);

  if (!httpMallocMultiCmds(pContext, 10, HTTP_BUFFER_SIZE)) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_ENOUGH_MEMORY);
    return false;
  }

  if (!httpMallocMultiCmds(pContext, 6, HTTP_BUFFER_SIZE)) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_ENOUGH_MEMORY);
    return false;
  }

  HttpSqlCmd* cmd;

  cmd = httpNewSqlCmd(pContext);
  if (cmd == NULL) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_ENOUGH_MEMORY);
    return false;
  }
  cmd->sql = httpAddToSqlCmdBuffer(pContext, "show databases");
  cmd->values = ADMIN_JSON_DBS_TYPE;  // hack way

  cmd = httpNewSqlCmd(pContext);
  if (cmd == NULL) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_ENOUGH_MEMORY);
    return false;
  }
  cmd->sql = httpAddToSqlCmdBuffer(pContext, "show databases");
  cmd->values = ADMIN_JSON_TABLES_TYPE;  // hack way

  cmd = httpNewSqlCmd(pContext);
  if (cmd == NULL) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_ENOUGH_MEMORY);
    return false;
  }
  cmd->sql = httpAddToSqlCmdBuffer(pContext, "show users");
  cmd->values = ADMIN_JSON_USERS_TYPE;  // hack way

  cmd = httpNewSqlCmd(pContext);
  if (cmd == NULL) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_ENOUGH_MEMORY);
    return false;
  }
  cmd->sql = httpAddToSqlCmdBuffer(pContext, "show mnodes");
  cmd->values = ADMIN_JSON_MNODES_TYPE;  // hack way

  cmd = httpNewSqlCmd(pContext);
  if (cmd == NULL) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_ENOUGH_MEMORY);
    return false;
  }
  cmd->sql = httpAddToSqlCmdBuffer(pContext, "show dnodes");
  cmd->values = ADMIN_JSON_DNODES_TYPE;  // hack way

  pContext->reqType = HTTP_REQTYPE_MULTI_SQL;
  pContext->encodeMethod = &adminEncodeInfoMethod;

  return true;
}

bool adminProcessMetaRequest(HttpContext* pContext) {
  httpDebug("context:%p, fd:%d, user:%s, process admin table meta msg", pContext, pContext->fd, pContext->user);

  char* sql = pContext->parser->body.str;
  if (sql == NULL) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_SQL_INPUT);
    return false;
  }

  if (httpCheckUsedbSql(sql)) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_EXEC_USEDB);
    return false;
  }

  HttpSqlCmd* cmd = &(pContext->singleCmd);
  cmd->nativSql = sql;

  pContext->reqType = HTTP_REQTYPE_SINGLE_SQL;
  pContext->encodeMethod = &adminEncodeMetaMethod;

  return true;
}

bool adminProcessRequest(struct HttpContext* pContext) {
  if (httpUrlMatch(pContext, ADMIN_ACTION_URL_POS, "login")) {
    adminGetUserFromUrl(pContext);
    adminGetPassFromUrl(pContext);
  }

  if (strlen(pContext->user) == 0 || strlen(pContext->pass) == 0) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_AUTH_INFO);
    return false;
  }

  if (httpUrlMatch(pContext, ADMIN_ACTION_URL_POS, "sql")) {
    return adminProcessSqlRequest(pContext);
  } else if (httpUrlMatch(pContext, ADMIN_ACTION_URL_POS, "sqls")) {
    return adminProcessSqlsRequest(pContext);
  } else if (httpUrlMatch(pContext, ADMIN_ACTION_URL_POS, "meta")) {
    return adminProcessMetaRequest(pContext);
  } else if (httpUrlMatch(pContext, ADMIN_ACTION_URL_POS, "info")) {
    return adminProcessInfoRequest(pContext);
  } else if (httpUrlMatch(pContext, ADMIN_ACTION_URL_POS, "login")) {
    return adminProcessLoginRequest(pContext);
  } else if (httpUrlMatch(pContext, ADMIN_ACTION_URL_POS, "logout")) {
    return adminProcessLogoutRequest(pContext);
  } else if (httpUrlMatch(pContext, ADMIN_ACTION_URL_POS, "all")) {
    return adminProcessSqlAllRequest(pContext);
  } else {
  }

  httpSendErrorResp(pContext, TSDB_CODE_HTTP_INVLALID_URL);
  return false;
}
