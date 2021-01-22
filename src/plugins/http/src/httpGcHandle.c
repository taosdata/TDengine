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
#include "taosdef.h"
#include "taoserror.h"
#include "cJSON.h"
#include "httpLog.h"
#include "httpGcHandle.h"
#include "httpGcJson.h"

static HttpDecodeMethod gcDecodeMethod = {"grafana", gcProcessRequest};
static HttpEncodeMethod gcHeartBeatMethod = {
  .startJsonFp          = NULL,         
  .stopJsonFp           = gcSendHeartBeatResp, 
  .buildQueryJsonFp     = NULL,
  .buildAffectRowJsonFp = NULL, 
  .initJsonFp           = NULL, 
  .cleanJsonFp          = NULL,
  .checkFinishedFp      = NULL,
  .setNextCmdFp         = NULL
};

static HttpEncodeMethod gcQueryMethod = {
  .startJsonFp          = NULL,         
  .stopJsonFp           = gcStopQueryJson, 
  .buildQueryJsonFp     = gcBuildQueryJson,
  .buildAffectRowJsonFp = NULL, 
  .initJsonFp           = gcInitQueryJson, 
  .cleanJsonFp          = gcCleanQueryJson,
  .checkFinishedFp      = NULL,
  .setNextCmdFp         = NULL
};

void gcInitHandle(HttpServer* pServer) { httpAddMethod(pServer, &gcDecodeMethod); }

bool gcGetUserFromUrl(HttpContext* pContext) {
  HttpParser* pParser = pContext->parser;
  if (pParser->path[GC_USER_URL_POS].pos >= TSDB_USER_LEN || pParser->path[GC_USER_URL_POS].pos <= 0) {
    return false;
  }

  tstrncpy(pContext->user, pParser->path[GC_USER_URL_POS].str, TSDB_USER_LEN);
  return true;
}

bool gcGetPassFromUrl(HttpContext* pContext) {
  HttpParser* pParser = pContext->parser;
  if (pParser->path[GC_PASS_URL_POS].pos >= HTTP_PASSWORD_LEN || pParser->path[GC_PASS_URL_POS].pos <= 0) {
    return false;
  }

  tstrncpy(pContext->pass, pParser->path[GC_PASS_URL_POS].str, HTTP_PASSWORD_LEN);
  return true;
}

bool gcProcessLoginRequest(HttpContext* pContext) {
  httpDebug("context:%p, fd:%d, user:%s, process grafana login msg", pContext, pContext->fd, pContext->user);
  pContext->reqType = HTTP_REQTYPE_LOGIN;
  return true;
}

/**
 * Process the query request
 * @param fd for http send back
 * @param context is taos conn
 * @param filter, the request format is json, such as
 */

// https://github.com/grafana/grafana/blob/master/docs/sources/plugins/developing/datasources.md
// input
//[{
//  "refId": "A",
//  "alias" : "taosd",
//  "sql" : "select first(taosd) from sys.mem where ts > now-6h and ts < now interval(20000a)"
//},
//{
//  "refId": "B",
//  "alias" : "system",
//  "sql" : "select first(taosd) from sys.mem where ts > now-6h and ts < now interval(20000a)"
//}]
// output
//[{
//  "datapoints": [[339.386719,
//    1537873132000],
//    [339.656250,
//    1537873162400],
//    [339.656250,
//    1537873192600],
//    [339.656250,
//    1537873222800],
//    [339.589844,
//    1537873253200],
//    [339.964844,
//    1537873283400],
//    [340.093750,
//    1537873313800],
//    [340.093750,
//    1537873344000],
//    [340.093750,
//    1537873374200],
//    [340.093750,
//    1537873404600]],
//    "refId": "A",
//    "target" : "taosd"
//},
//{
//  "datapoints": [[339.386719,
//  1537873132000],
//  [339.656250,
//  1537873162400],
//  [339.656250,
//  1537873192600],
//  [339.656250,
//  1537873222800],
//  [339.589844,
//  1537873253200],
//  [339.964844,
//  1537873283400],
//  [340.093750,
//  1537873313800],
//  [340.093750,
//  1537873344000],
//  [340.093750,
//  1537873374200],
//  [340.093750,
//  1537873404600]],
//  "refId": "B",
//  "target" : "system"
//}]

bool gcProcessQueryRequest(HttpContext* pContext) {
  httpDebug("context:%p, fd:%d, process grafana query msg", pContext, pContext->fd);

  char* filter = pContext->parser->body.str;
  if (filter == NULL) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_MSG_INPUT);
    return false;
  }

  cJSON* root = cJSON_Parse(filter);
  if (root == NULL) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_GC_REQ_PARSE_ERROR);
    return false;
  }

  int32_t size = cJSON_GetArraySize(root);
  if (size <= 0) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_GC_QUERY_NULL);
    cJSON_Delete(root);
    return false;
  }

  if (size > 100) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_GC_QUERY_SIZE);
    cJSON_Delete(root);
    return false;
  }

  if (!httpMallocMultiCmds(pContext, size, HTTP_BUFFER_SIZE)) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_ENOUGH_MEMORY);
    cJSON_Delete(root);
    return false;
  }

  for (int32_t i = 0; i < size; ++i) {
    cJSON* query = cJSON_GetArrayItem(root, i);
    if (query == NULL) continue;

    cJSON* refId = cJSON_GetObjectItem(query, "refId");
    if (refId == NULL || refId->valuestring == NULL || strlen(refId->valuestring) == 0) {
      httpDebug("context:%p, fd:%d, user:%s, refId is null", pContext, pContext->fd, pContext->user);
      continue;
    }

    int32_t refIdBuffer = httpAddToSqlCmdBuffer(pContext, refId->valuestring);
    if (refIdBuffer == -1) {
      httpWarn("context:%p, fd:%d, user:%s, refId buffer is full", pContext, pContext->fd, pContext->user);
      break;
    }

    cJSON*  alias = cJSON_GetObjectItem(query, "alias");
    int32_t aliasBuffer = -1;
    if (!(alias == NULL || alias->valuestring == NULL || strlen(alias->valuestring) == 0)) {
      aliasBuffer = httpAddToSqlCmdBuffer(pContext, alias->valuestring);
      if (aliasBuffer == -1) {
        httpWarn("context:%p, fd:%d, user:%s, alias buffer is full", pContext, pContext->fd, pContext->user);
        break;
      }
    }
    if (aliasBuffer == -1) {
      aliasBuffer = httpAddToSqlCmdBuffer(pContext, "");
    }

    cJSON* sql = cJSON_GetObjectItem(query, "sql");
    if (sql == NULL || sql->valuestring == NULL || strlen(sql->valuestring) == 0) {
      httpDebug("context:%p, fd:%d, user:%s, sql is null", pContext, pContext->fd, pContext->user);
      continue;
    }

    int32_t sqlBuffer = httpAddToSqlCmdBuffer(pContext, sql->valuestring);
    if (sqlBuffer == -1) {
      httpWarn("context:%p, fd:%d, user:%s, sql buffer is full", pContext, pContext->fd, pContext->user);
      break;
    }

    HttpSqlCmd* cmd = httpNewSqlCmd(pContext);
    if (cmd == NULL) {
      httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_ENOUGH_MEMORY);
      cJSON_Delete(root);
      return false;
    }

    cmd->sql = sqlBuffer;
    cmd->values = refIdBuffer;
    cmd->table = aliasBuffer;
    cmd->numOfRows = 0;                                                                 // hack way as target flags
    cmd->timestamp = httpAddToSqlCmdBufferWithSize(pContext, HTTP_GC_TARGET_SIZE + 1);  // hack way

    if (cmd->timestamp == -1) {
      httpWarn("context:%p, fd:%d, user:%s, cant't malloc target size, sql buffer is full", pContext, pContext->fd,
               pContext->user);
      break;
    }
  }

  pContext->reqType = HTTP_REQTYPE_MULTI_SQL;
  pContext->encodeMethod = &gcQueryMethod;
  pContext->multiCmds->pos = 0;

  return true;
}

bool gcProcessHeartbeatRequest(HttpContext* pContext) {
  httpDebug("context:%p, fd:%d, process grafana heartbeat msg", pContext, pContext->fd);
  pContext->reqType = HTTP_REQTYPE_HEARTBEAT;
  pContext->encodeMethod = &gcHeartBeatMethod;
  return true;
}

/**
 * Process get/post/options msg, such as login and logout
 */
bool gcProcessRequest(struct HttpContext* pContext) {
  if (httpUrlMatch(pContext, GC_ACTION_URL_POS, "login")) {
    gcGetUserFromUrl(pContext);
    gcGetPassFromUrl(pContext);
  }

  if (strlen(pContext->user) == 0 || strlen(pContext->pass) == 0) {
    httpSendErrorResp(pContext, TSDB_CODE_HTTP_NO_AUTH_INFO);
    return false;
  }

  if (httpUrlMatch(pContext, GC_ACTION_URL_POS, "query")) {
    return gcProcessQueryRequest(pContext);
  } else if (httpUrlMatch(pContext, GC_ACTION_URL_POS, "heartbeat")) {
    return gcProcessHeartbeatRequest(pContext);
  } else if (httpUrlMatch(pContext, GC_ACTION_URL_POS, "login")) {
    return gcProcessLoginRequest(pContext);
  } else {
    return gcProcessHeartbeatRequest(pContext);
  }
}
