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

#include "httpResp.h"
#include <stdio.h>
#include <string.h>
#include "httpCode.h"
#include "httpJson.h"

extern char *tsError[];

const char *httpKeepAliveStr[] = {"", "Connection: Keep-Alive\r\n", "Connection: Close\r\n"};

const char *httpVersionStr[] = {"HTTP/1.0", "HTTP/1.1", "HTTP/1.2"};

const char *httpRespTemplate[] = {
    // HTTP_RESPONSE_JSON_OK
    // HTTP_RESPONSE_JSON_ERROR
    "{\"status\":\"succ\",\"code\":%d,\"desc\":\"%s\"}",
    "{\"status\":\"error\",\"code\":%d,\"desc\":\"%s\"}",
    // HTTP_RESPONSE_OK
    // HTTP_RESPONSE_ERROR
    "%s 200 OK\r\nAccess-Control-Allow-Origin:*\r\n%sContent-Type: application/json;charset=utf-8\r\nContent-Length: %d\r\n\r\n",
    "%s %d %s\r\nAccess-Control-Allow-Origin:*\r\n%sContent-Type: application/json;charset=utf-8\r\nContent-Length: %d\r\n\r\n",
    // HTTP_RESPONSE_CHUNKED_UN_COMPRESS, HTTP_RESPONSE_CHUNKED_COMPRESS
    "%s 200 OK\r\nAccess-Control-Allow-Origin:*\r\n%sContent-Type: application/json;charset=utf-8\r\nTransfer-Encoding: chunked\r\n\r\n",
    "%s 200 OK\r\nAccess-Control-Allow-Origin:*\r\n%sContent-Type: application/json;charset=utf-8\r\nContent-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n",
    // HTTP_RESPONSE_OPTIONS
    "%s 200 OK\r\nAccess-Control-Allow-Origin:*\r\n%sContent-Type: application/json;charset=utf-8\r\nContent-Length: %d\r\nAccess-Control-Allow-Methods: *\r\nAccess-Control-Max-Age: 3600\r\nAccess-Control-Allow-Headers: Origin, X-Requested-With, Content-Type, Accept, authorization\r\n\r\n",
    // HTTP_RESPONSE_GRAFANA
    "%s 200 OK\r\nAccess-Control-Allow-Origin:*\r\n%sAccess-Control-Allow-Methods:POST, GET, OPTIONS, DELETE, PUT\r\nAccess-Control-Allow-Headers:Accept, Content-Type\r\nContent-Type: application/json;charset=utf-8\r\nContent-Length: %d\r\n\r\n"
};

void httpSendErrResp(HttpContext *pContext, int httpCode, char *httpCodeStr, int errNo, char *desc) {
  httpError("context:%p, fd:%d, ip:%s, code:%d, error:%d:%s", pContext, pContext->fd, pContext->ipstr, httpCode, errNo,
            desc);

  char head[512] = {0};
  char body[512] = {0};

  int bodyLen = sprintf(body, httpRespTemplate[HTTP_RESPONSE_JSON_ERROR], errNo, desc);
  int headLen = sprintf(head, httpRespTemplate[HTTP_RESPONSE_ERROR], httpVersionStr[pContext->httpVersion], httpCode,
                        httpCodeStr, httpKeepAliveStr[pContext->httpKeepAlive], bodyLen);

  httpWriteBuf(pContext, head, headLen);
  httpWriteBuf(pContext, body, bodyLen);
  httpCloseContextByApp(pContext);
}

void httpSendErrorRespWithDesc(HttpContext *pContext, int errNo, char *desc) {
  int   httpCode = 500;
  char *httpCodeStr = "Internal Server Error";
  switch (errNo) {
    case HTTP_SUCCESS:
      httpCode = 200;
      httpCodeStr = "OK";
      break;
    case HTTP_SERVER_OFFLINE:
    case HTTP_UNSUPPORT_URL:
      httpCode = 404;
      httpCodeStr = "Not Found";
      break;
    case HTTP_PARSE_HTTP_METHOD_ERROR:
      httpCode = 405;
      httpCodeStr = "Method Not Allowed";
      break;
    case HTTP_PARSE_HTTP_VERSION_ERROR:
      httpCode = 505;
      httpCodeStr = "HTTP Version Not Supported";
      break;
    case HTTP_PARSE_HEAD_ERROR:
      httpCode = 406;
      httpCodeStr = "Not Acceptable";
      break;
    case HTTP_REQUSET_TOO_BIG:
      httpCode = 413;
      httpCodeStr = "Request Entity Too Large";
      break;
    case HTTP_PARSE_BODY_ERROR:
    case HTTP_PARSE_CHUNKED_BODY_ERROR:
      httpCode = 409;
      httpCodeStr = "Conflict";
      break;
    case HTTP_PARSE_URL_ERROR:
      httpCode = 414;
      httpCodeStr = "Request-URI Invalid";
      break;
    case HTTP_INVALID_AUTH_TOKEN:
    case HTTP_PARSE_USR_ERROR:
      httpCode = 401;
      httpCodeStr = "Unauthorized";
      break;
    case HTTP_NO_SQL_INPUT:
      httpCode = 400;
      httpCodeStr = "Bad Request";
      break;
    case HTTP_SESSION_FULL:
      httpCode = 421;
      httpCodeStr = "Too many connections";
      break;
    case HTTP_NO_ENOUGH_MEMORY:
    case HTTP_GEN_TAOSD_TOKEN_ERR:
      httpCode = 507;
      httpCodeStr = "Insufficient Storage";
      break;
    case HTTP_INVALID_DB_TABLE:
    case HTTP_NO_EXEC_USEDB:
    case HTTP_PARSE_GC_REQ_ERROR:
    case HTTP_INVALID_MULTI_REQUEST:
    case HTTP_NO_MSG_INPUT:
      httpCode = 400;
      httpCodeStr = "Bad Request";
      break;
    case HTTP_NO_ENOUGH_SESSIONS:
      httpCode = 421;
      httpCodeStr = "Too many connections";
      break;
    // telegraf
    case HTTP_TG_DB_NOT_INPUT:
    case HTTP_TG_DB_TOO_LONG:
    case HTTP_TG_INVALID_JSON:
    case HTTP_TG_METRICS_NULL:
    case HTTP_TG_METRICS_SIZE:
    case HTTP_TG_METRIC_NULL:
    case HTTP_TG_METRIC_TYPE:
    case HTTP_TG_METRIC_NAME_NULL:
    case HTTP_TG_METRIC_NAME_LONG:
    case HTTP_TG_TIMESTAMP_NULL:
    case HTTP_TG_TIMESTAMP_TYPE:
    case HTTP_TG_TIMESTAMP_VAL_NULL:
    case HTTP_TG_TAGS_NULL:
    case HTTP_TG_TAGS_SIZE_0:
    case HTTP_TG_TAGS_SIZE_LONG:
    case HTTP_TG_TAG_NULL:
    case HTTP_TG_TAG_NAME_NULL:
    case HTTP_TG_TAG_NAME_SIZE:
    case HTTP_TG_TAG_VALUE_TYPE:
    case HTTP_TG_TAG_VALUE_NULL:
    case HTTP_TG_TABLE_NULL:
    case HTTP_TG_TABLE_SIZE:
    case HTTP_TG_FIELDS_NULL:
    case HTTP_TG_FIELDS_SIZE_0:
    case HTTP_TG_FIELDS_SIZE_LONG:
    case HTTP_TG_FIELD_NULL:
    case HTTP_TG_FIELD_NAME_NULL:
    case HTTP_TG_FIELD_NAME_SIZE:
    case HTTP_TG_FIELD_VALUE_TYPE:
    case HTTP_TG_FIELD_VALUE_NULL:
    case HTTP_INVALID_BASIC_AUTH_TOKEN:
    case HTTP_INVALID_TAOSD_AUTH_TOKEN:
    case HTTP_TG_HOST_NOT_STRING:
    // grafana
    case HTTP_GC_QUERY_NULL:
    case HTTP_GC_QUERY_SIZE:
      httpCode = 400;
      httpCodeStr = "Bad Request";
      break;
    default:
      httpError("context:%p, fd:%d, ip:%s, error:%d not recognized", pContext, pContext->fd, pContext->ipstr, errNo);
      break;
  }

  if (desc == NULL) {
    httpSendErrResp(pContext, httpCode, httpCodeStr, errNo + 1000, httpMsg[errNo]);
  } else {
    httpSendErrResp(pContext, httpCode, httpCodeStr, errNo + 1000, desc);
  }
}

void httpSendErrorResp(HttpContext *pContext, int errNo) { httpSendErrorRespWithDesc(pContext, errNo, NULL); }

void httpSendTaosdErrorResp(HttpContext *pContext, int errCode) {
  int httpCode = 400;
  httpSendErrResp(pContext, httpCode, "Bad Request", errCode, tsError[errCode]);
}

void httpSendSuccResp(HttpContext *pContext, char *desc) {
  char head[1024] = {0};
  char body[1024] = {0};

  int bodyLen = sprintf(body, httpRespTemplate[HTTP_RESPONSE_JSON_OK], HTTP_SUCCESS, desc);
  int headLen = sprintf(head, httpRespTemplate[HTTP_RESPONSE_OK], httpVersionStr[pContext->httpVersion],
                        httpKeepAliveStr[pContext->httpKeepAlive], bodyLen);

  httpWriteBuf(pContext, head, headLen);
  httpWriteBuf(pContext, body, bodyLen);
  httpCloseContextByApp(pContext);
}

void httpSendOptionResp(HttpContext *pContext, char *desc) {
  char head[1024] = {0};
  char body[1024] = {0};

  int bodyLen = sprintf(body, httpRespTemplate[HTTP_RESPONSE_JSON_OK], HTTP_SUCCESS, desc);
  int headLen = sprintf(head, httpRespTemplate[HTTP_RESPONSE_OPTIONS], httpVersionStr[pContext->httpVersion],
                        httpKeepAliveStr[pContext->httpKeepAlive], bodyLen);

  httpWriteBuf(pContext, head, headLen);
  httpWriteBuf(pContext, body, bodyLen);
  httpCloseContextByApp(pContext);
}
