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
#include "taosmsg.h"
#include "httpLog.h"
#include "httpResp.h"
#include "httpJson.h"
#include "httpContext.h"

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

static void httpSendErrorRespImp(HttpContext *pContext, int32_t httpCode, char *httpCodeStr, int32_t errNo, const char *desc) {
  httpError("context:%p, fd:%d, code:%d, error:%s", pContext, pContext->fd, httpCode, desc);

  char head[512] = {0};
  char body[512] = {0};

  int8_t httpVersion = 0;
  int8_t keepAlive = 0;
  if (pContext->parser != NULL) {
    httpVersion = pContext->parser->httpVersion;
    keepAlive = pContext->parser->keepAlive;
  }

  int32_t bodyLen = sprintf(body, httpRespTemplate[HTTP_RESPONSE_JSON_ERROR], errNo, desc);
  int32_t headLen = sprintf(head, httpRespTemplate[HTTP_RESPONSE_ERROR], httpVersionStr[httpVersion], httpCode,
                            httpCodeStr, httpKeepAliveStr[keepAlive], bodyLen);

  httpWriteBuf(pContext, head, headLen);
  httpWriteBuf(pContext, body, bodyLen);
  httpCloseContextByApp(pContext);
}

void httpSendErrorResp(HttpContext *pContext, int32_t errNo) {
  int32_t httpCode = 500;
  if (errNo == TSDB_CODE_SUCCESS)
    httpCode = 200;
  else if (errNo == TSDB_CODE_HTTP_SERVER_OFFLINE)
    httpCode = 404;
  else if (errNo == TSDB_CODE_HTTP_UNSUPPORT_URL)
    httpCode = 404;
  else if (errNo == TSDB_CODE_HTTP_INVLALID_URL)
    httpCode = 404;
  else if (errNo == TSDB_CODE_HTTP_NO_ENOUGH_MEMORY)
    httpCode = 507;
  else if (errNo == TSDB_CODE_HTTP_REQUSET_TOO_BIG)
    httpCode = 413;
  else if (errNo == TSDB_CODE_HTTP_NO_AUTH_INFO)
    httpCode = 401;
  else if (errNo == TSDB_CODE_HTTP_NO_MSG_INPUT)
    httpCode = 400;
  else if (errNo == TSDB_CODE_HTTP_NO_SQL_INPUT)
    httpCode = 400;
  else if (errNo == TSDB_CODE_HTTP_NO_EXEC_USEDB)
    httpCode = 400;
  else if (errNo == TSDB_CODE_HTTP_SESSION_FULL)
    httpCode = 421;
  else if (errNo == TSDB_CODE_HTTP_GEN_TAOSD_TOKEN_ERR)
    httpCode = 507;
  else if (errNo == TSDB_CODE_HTTP_INVALID_MULTI_REQUEST)
    httpCode = 400;
  else if (errNo == TSDB_CODE_HTTP_CREATE_GZIP_FAILED)
    httpCode = 507;
  else if (errNo == TSDB_CODE_HTTP_FINISH_GZIP_FAILED)
    httpCode = 507;
  else if (errNo == TSDB_CODE_HTTP_INVALID_VERSION)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_INVALID_CONTENT_LENGTH)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_INVALID_AUTH_TYPE)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_INVALID_AUTH_FORMAT)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_INVALID_BASIC_AUTH)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_INVALID_TAOSD_AUTH)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_PARSE_METHOD_FAILED)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_PARSE_TARGET_FAILED)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_PARSE_VERSION_FAILED)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_PARSE_SP_FAILED)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_PARSE_STATUS_FAILED)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_PARSE_PHRASE_FAILED)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_PARSE_CRLF_FAILED)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_PARSE_HEADER_FAILED)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_PARSE_HEADER_KEY_FAILED)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_PARSE_HEADER_VAL_FAILED)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_PARSE_CHUNK_SIZE_FAILED)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_PARSE_CHUNK_FAILED)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_PARSE_END_FAILED)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_PARSE_INVALID_STATE)
    httpCode = 406;
  else if (errNo == TSDB_CODE_HTTP_PARSE_ERROR_STATE)
    httpCode = 406;
  else
    httpCode = 400;

  if (pContext->parser && pContext->parser->httpCode != 0) {
    httpCode = pContext->parser->httpCode;
  }

  char *httpCodeStr = httpGetStatusDesc(httpCode);
  httpSendErrorRespImp(pContext, httpCode, httpCodeStr, errNo & 0XFFFF, tstrerror(errNo));
}

void httpSendTaosdInvalidSqlErrorResp(HttpContext *pContext, char *errMsg) {
  int32_t httpCode = 400;
  char    temp[512] = {0};
  int32_t len = sprintf(temp, "invalid SQL: %s", errMsg);

  for (int32_t i = 0; i < len; ++i) {
    if (temp[i] == '\"') {
      temp[i] = '\'';
    } else if (temp[i] == '\n') {
      temp[i] = ' ';
    } else {
    }
  }

  httpSendErrorRespImp(pContext, httpCode, "Bad Request", TSDB_CODE_TSC_INVALID_SQL & 0XFFFF, temp);
}

void httpSendSuccResp(HttpContext *pContext, char *desc) {
  char head[1024] = {0};
  char body[1024] = {0};

  int8_t httpVersion = 0;
  int8_t keepAlive = 0;
  if (pContext->parser != NULL) {
    httpVersion = pContext->parser->httpVersion;
    keepAlive = pContext->parser->keepAlive;
  }

  int32_t bodyLen = sprintf(body, httpRespTemplate[HTTP_RESPONSE_JSON_OK], TSDB_CODE_SUCCESS, desc);
  int32_t headLen = sprintf(head, httpRespTemplate[HTTP_RESPONSE_OK], httpVersionStr[httpVersion],
                            httpKeepAliveStr[keepAlive], bodyLen);

  httpWriteBuf(pContext, head, headLen);
  httpWriteBuf(pContext, body, bodyLen);
  httpCloseContextByApp(pContext);
}

void httpSendOptionResp(HttpContext *pContext, char *desc) {
  char head[1024] = {0};
  char body[1024] = {0};

  int8_t httpVersion = 0;
  int8_t keepAlive = 0;
  if (pContext->parser != NULL) {
    httpVersion = pContext->parser->httpVersion;
    keepAlive = pContext->parser->keepAlive;
  }

  int32_t bodyLen = sprintf(body, httpRespTemplate[HTTP_RESPONSE_JSON_OK], TSDB_CODE_SUCCESS, desc);
  int32_t headLen = sprintf(head, httpRespTemplate[HTTP_RESPONSE_OPTIONS], httpVersionStr[httpVersion],
                            httpKeepAliveStr[keepAlive], bodyLen);

  httpWriteBuf(pContext, head, headLen);
  httpWriteBuf(pContext, body, bodyLen);
  httpCloseContextByApp(pContext);
}
