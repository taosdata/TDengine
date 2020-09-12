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
#include "taos.h"
#include "tglobal.h"
#include "tsocket.h"
#include "ttimer.h"
#include "httpInt.h"
#include "httpResp.h"
#include "httpAuth.h"
#include "httpServer.h"
#include "httpContext.h"
#include "httpHandle.h"

void httpToLowerUrl(char* url) {
  /*ignore case */
  while (*url) {
    if (*url >= 'A' && *url <= 'Z') {
      *url = *url | 0x20;
    }
    url++;
  }
}

bool httpUrlMatch(HttpContext* pContext, int pos, char* cmp) {
  HttpParser* pParser = &pContext->parser;

  if (pos < 0 || pos >= HTTP_MAX_URL) {
    return false;
  }

  if (pParser->path[pos].len <= 0) {
    return false;
  }

  if (strcmp(pParser->path[pos].pos, cmp) != 0) {
    return false;
  }

  return true;
}

// /account/db/meter HTTP/1.1\r\nHost
bool httpParseURL(HttpContext* pContext) {
  HttpParser* pParser = &pContext->parser;
  char* pSeek;
  char* pEnd = strchr(pParser->pLast, ' ');
  if (pEnd == NULL) {
    httpSendErrorResp(pContext, HTTP_UNSUPPORT_URL);    
    return false;
  }

  if (*pParser->pLast != '/') {
    httpSendErrorResp(pContext, HTTP_UNSUPPORT_URL);
    return false;
  }
  pParser->pLast++;

  for (int i = 0; i < HTTP_MAX_URL; i++) {
    pSeek = strchr(pParser->pLast, '/');
    if (pSeek == NULL) {
      break;
    }
    pParser->path[i].pos = pParser->pLast;
    if (pSeek <= pEnd) {
      pParser->path[i].len = (int16_t)(pSeek - pParser->pLast);
      pParser->path[i].pos[pParser->path[i].len] = 0;
      httpToLowerUrl(pParser->path[i].pos);
      pParser->pLast = pSeek + 1;
    } else {
      pParser->path[i].len = (int16_t)(pEnd - pParser->pLast);
      pParser->path[i].pos[pParser->path[i].len] = 0;
      httpToLowerUrl(pParser->path[i].pos);
      pParser->pLast = pEnd + 1;
      break;
    }
  }
  pParser->pLast = pEnd + 1;

  if (pParser->path[0].len == 0) {
    httpSendErrorResp(pContext, HTTP_UNSUPPORT_URL);
    return false;
  }

  return true;
}

bool httpParseHttpVersion(HttpContext* pContext) {
  HttpParser* pParser = &pContext->parser;
  char* pEnd = strchr(pParser->pLast, '1');
  if (pEnd == NULL) {
    httpError("context:%p, fd:%d, can't find http version at position:%s", pContext, pContext->fd, pParser->pLast);
    httpSendErrorResp(pContext, HTTP_PARSE_HTTP_VERSION_ERROR);
    return false;
  }

  if (*(pEnd + 1) != '.') {
    httpError("context:%p, fd:%d, can't find http version at position:%s", pContext, pContext->fd, pParser->pLast);
    httpSendErrorResp(pContext, HTTP_PARSE_HTTP_VERSION_ERROR);
    return false;
  }

  if (*(pEnd + 2) == '0')
    pContext->httpVersion = HTTP_VERSION_10;
  else if (*(pEnd + 2) == '1')
    pContext->httpVersion = HTTP_VERSION_11;
  else if (*(pEnd + 2) == '2')
    pContext->httpVersion = HTTP_VERSION_11;
  else
    pContext->httpVersion = HTTP_VERSION_10;

  httpDebug("context:%p, fd:%d, httpVersion:1.%d", pContext, pContext->fd, pContext->httpVersion);
  return true;
}

bool httpGetNextLine(HttpContext* pContext) {
  HttpParser* pParser = &pContext->parser;
  while (pParser->buffer + pParser->bufsize - pParser->pCur++ > 0) {
    if (*(pParser->pCur) == '\n' && *(pParser->pCur - 1) == '\r') {
      // cut the string
      *pParser->pCur = 0;
      return true;
    }
  }

  httpSendErrorResp(pContext, HTTP_PARSE_HEAD_ERROR);

  return false;
}

bool httpGetHttpMethod(HttpContext* pContext) {
  HttpParser* pParser = &pContext->parser;
  char*       pSeek = strchr(pParser->pLast, ' ');

  if (pSeek == NULL) {
    httpError("context:%p, fd:%d, failed to parse httpMethod", pContext, pContext->fd);
    httpSendErrorResp(pContext, HTTP_PARSE_HTTP_METHOD_ERROR);
    return false;
  }

  pParser->method.pos = pParser->pLast;
  pParser->method.len = (int16_t)(pSeek - pParser->pLast);
  pParser->method.pos[pParser->method.len] = 0;
  pParser->pLast = pSeek + 1;

  httpTrace("context:%p, fd:%d, httpMethod:%s", pContext, pContext->fd, pParser->method.pos);
  return true;
}

bool httpGetDecodeMethod(HttpContext* pContext) {
  HttpParser* pParser = &pContext->parser;

  HttpServer* pServer = &tsHttpServer;
  int         methodLen = pServer->methodScannerLen;
  for (int i = 0; i < methodLen; i++) {
    HttpDecodeMethod* method = pServer->methodScanner[i];
    if (strcmp(method->module, pParser->path[0].pos) != 0) {
      continue;
    }
    pParser->pMethod = method;
    return true;
  }

  httpError("context:%p, fd:%d, error:the url is not support, method:%s, path:%s",
            pContext, pContext->fd, pParser->method.pos, pParser->path[0].pos);
  httpSendErrorResp(pContext, HTTP_UNSUPPORT_URL);

  return false;
}

bool httpParseHead(HttpContext* pContext) {
  HttpParser* pParser = &pContext->parser;
  if (strncasecmp(pParser->pLast, "Content-Length: ", 16) == 0) {
    pParser->data.len = (int32_t)atoi(pParser->pLast + 16);
    httpTrace("context:%p, fd:%d, Content-Length:%d", pContext, pContext->fd,
              pParser->data.len);
  } else if (strncasecmp(pParser->pLast, "Accept-Encoding: ", 17) == 0) {
    if (tsHttpEnableCompress && strstr(pParser->pLast + 17, "gzip") != NULL) {
      pContext->acceptEncoding = HTTP_COMPRESS_GZIP;
      httpTrace("context:%p, fd:%d, Accept-Encoding:gzip", pContext, pContext->fd);
    } else {
      pContext->acceptEncoding = HTTP_COMPRESS_IDENTITY;
      httpTrace("context:%p, fd:%d, Accept-Encoding:identity", pContext, pContext->fd);
    }
  } else if (strncasecmp(pParser->pLast, "Content-Encoding: ", 18) == 0) {
    if (strstr(pParser->pLast + 18, "gzip") != NULL) {
      pContext->contentEncoding = HTTP_COMPRESS_GZIP;
      httpTrace("context:%p, fd:%d, Content-Encoding:gzip", pContext, pContext->fd);
    } else {
      pContext->contentEncoding = HTTP_COMPRESS_IDENTITY;
      httpTrace("context:%p, fd:%d, Content-Encoding:identity", pContext, pContext->fd);
    }
  } else if (strncasecmp(pParser->pLast, "Connection: ", 12) == 0) {
    if (strncasecmp(pParser->pLast + 12, "Keep-Alive", 10) == 0) {
      pContext->httpKeepAlive = HTTP_KEEPALIVE_ENABLE;
    } else {
      pContext->httpKeepAlive = HTTP_KEEPALIVE_DISABLE;
    }
    httpTrace("context:%p, fd:%d, keepAlive:%d", pContext, pContext->fd, pContext->httpKeepAlive);
  } else if (strncasecmp(pParser->pLast, "Transfer-Encoding: ", 19) == 0) {
    if (strncasecmp(pParser->pLast + 19, "chunked", 7) == 0) {
      pContext->httpChunked = HTTP_CHUNKED;
    }
  } else if (strncasecmp(pParser->pLast, "Authorization: ", 15) == 0) {
    if (strncasecmp(pParser->pLast + 15, "Basic ", 6) == 0) {
      pParser->token.pos = pParser->pLast + 21;
      pParser->token.len = (int16_t)(pParser->pCur - pParser->token.pos - 1);
      bool parsed = httpParseBasicAuthToken(pContext, pParser->token.pos, pParser->token.len);
      if (!parsed) {
        httpSendErrorResp(pContext, HTTP_INVALID_BASIC_AUTH_TOKEN);
        return false;
      }
    } else if (strncasecmp(pParser->pLast + 15, "Taosd ", 6) == 0) {
      pParser->token.pos = pParser->pLast + 21;
      pParser->token.len = (int16_t)(pParser->pCur - pParser->token.pos - 1);
      bool parsed = httpParseTaosdAuthToken(pContext, pParser->token.pos, pParser->token.len);
      if (!parsed) {
        httpSendErrorResp(pContext, HTTP_INVALID_TAOSD_AUTH_TOKEN);
        return false;
      }
    } else {
      httpSendErrorResp(pContext, HTTP_INVALID_AUTH_TOKEN);
      return false;
    }
  } else {
  }

  return true;
}

bool httpDecodeRequest(HttpContext* pContext) {
  HttpParser* pParser = &pContext->parser;
  if (pParser->pMethod->decodeFp == NULL) {
    return false;
  }

  return (*pParser->pMethod->decodeFp)(pContext);
}

/**
 * Process the request from http pServer
 */
bool httpProcessData(HttpContext* pContext) {
  if (!httpAlterContextState(pContext, HTTP_CONTEXT_STATE_READY, HTTP_CONTEXT_STATE_HANDLING)) {
    httpDebug("context:%p, fd:%d, state:%s not in ready state, stop process request", pContext, pContext->fd,
              httpContextStateStr(pContext->state));
    httpCloseContextByApp(pContext);
    return false;
  }

  // handle Cross-domain request
  if (strcmp(pContext->parser.method.pos, "OPTIONS") == 0) {
    httpDebug("context:%p, fd:%d, process options request", pContext, pContext->fd);
    httpSendOptionResp(pContext, "process options request success");
  } else {
    if (!httpDecodeRequest(pContext)) {
      /*
       * httpCloseContextByApp has been called when parsing the error
       */
      //httpCloseContextByApp(pContext);
    } else {
      httpProcessRequest(pContext);
    }
  }

  return true;
}
