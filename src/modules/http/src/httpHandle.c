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

#include <arpa/inet.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "http.h"
#include "httpCode.h"
#include "httpHandle.h"
#include "httpResp.h"
#include "shash.h"
#include "taos.h"
#include "tglobalcfg.h"
#include "tsocket.h"
#include "ttimer.h"

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
  HttpParser* pParser = &pContext->pThread->parser;

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
  HttpParser* pParser = &pContext->pThread->parser;

  char* pSeek;
  char* pEnd = strchr(pParser->pLast, ' ');
  if (*pParser->pLast != '/') {
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

  // for (int i = 0; i < HTTP_MAX_URL; i++) {
  //    if (pParser->path[i].len > 0) {
  //        httpTrace("url_pos: %d, path: [%s]", i, pParser->path[i].pos);
  //    }
  //}

  if (pParser->path[0].len == 0) {
    httpSendErrorResp(pContext, HTTP_UNSUPPORT_URL);
    return false;
  }

  return true;
}

bool httpParseHttpVersion(HttpContext* pContext) {
  HttpParser* pParser = &pContext->pThread->parser;

  char* pEnd = strchr(pParser->pLast, '1');

  if (*(pEnd + 1) != '.') {
    httpError("context:%p, fd:%d, ip:%s, can't find http version at position:%s", pContext, pContext->fd,
              pContext->ipstr, pParser->pLast);
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

  httpTrace("context:%p, fd:%d, ip:%s, httpVersion:1.%d", pContext, pContext->fd, pContext->ipstr,
            pContext->httpVersion);
  return true;
}

bool httpGetNextLine(HttpContext* pContext) {
  HttpParser* pParser = &pContext->pThread->parser;
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
  HttpParser* pParser = &pContext->pThread->parser;
  char*       pSeek = strchr(pParser->pLast, ' ');
  if (pSeek == NULL) {
    httpSendErrorResp(pContext, HTTP_PARSE_HTTP_METHOD_ERROR);
    return false;
  }
  pParser->method.pos = pParser->pLast;
  pParser->method.len = (int16_t)(pSeek - pParser->pLast);
  pParser->method.pos[pParser->method.len] = 0;
  pParser->pLast = pSeek + 1;

  httpTrace("context:%p, fd:%d, ip:%s, httpMethod:%s", pContext, pContext->fd, pContext->ipstr, pParser->method.pos);
  return true;
}

bool httpGetDecodeMethod(HttpContext* pContext) {
  HttpParser* pParser = &pContext->pThread->parser;
  HttpServer* pServer = pContext->pThread->pServer;
  int         methodLen = pServer->methodScannerLen;
  for (int i = 0; i < methodLen; i++) {
    HttpDecodeMethod* method = pServer->methodScanner[i];
    if (strcmp(method->module, pParser->path[0].pos) != 0) {
      continue;
    }
    pParser->pMethod = method;
    return true;
  }

  httpError("context:%p, fd:%d, ip:%s, error:the url is not support, method:%s, path:%s",
            pContext, pContext->fd, pContext->ipstr, pParser->method.pos, pParser->path[0].pos);
  httpSendErrorResp(pContext, HTTP_UNSUPPORT_URL);

  return false;
}

bool httpParseHead(HttpContext* pContext) {
  HttpParser* pParser = &pContext->pThread->parser;
  if (strncasecmp(pParser->pLast, "Content-Length: ", 16) == 0) {
    pParser->data.len = (int32_t)atoi(pParser->pLast + 16);
    httpTrace("context:%p, fd:%d, ip:%s, Content-Length:%d", pContext, pContext->fd, pContext->ipstr,
              pParser->data.len);
  } else if (tsHttpEnableCompress && strncasecmp(pParser->pLast, "Accept-Encoding: ", 17) == 0) {
    if (strstr(pParser->pLast + 17, "deflate") != NULL) {
      pContext->compress = JsonCompress;
    }
    httpTrace("context:%p, fd:%d, ip:%s, Accept-Encoding:%s", pContext, pContext->fd, pContext->ipstr,
              pContext->compress == JsonCompress ? "deflate" : "identity");
  } else if (strncasecmp(pParser->pLast, "Connection: ", 12) == 0) {
    if (strncasecmp(pParser->pLast + 12, "Keep-Alive", 10) == 0) {
      pContext->httpKeepAlive = HTTP_KEEPALIVE_ENABLE;
    } else {
      pContext->httpKeepAlive = HTTP_KEEPALIVE_DISABLE;
    }
    httpTrace("context:%p, fd:%d, ip:%s, keepAlive:%d", pContext, pContext->fd, pContext->ipstr,
              pContext->httpKeepAlive);
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
      httpSendErrorResp(pContext, HTTP_INVALID_TAOSD_AUTH_TOKEN);
    } else {
      httpSendErrorResp(pContext, HTTP_INVALID_AUTH_TOKEN);
      return false;
    }
  } else {
  }

  return true;
}

bool httpParseChunkedBody(HttpContext* pContext, HttpParser* pParser, bool test) {
  char*  pEnd = pParser->buffer + pParser->bufsize;
  char*  pRet = pParser->data.pos;
  char*  pSize = pParser->data.pos;
  size_t size = strtoul(pSize, NULL, 16);
  if (size <= 0) return false;

  while (size > 0) {
    char* pData = strstr(pSize, "\r\n");
    if (pData == NULL || pData >= pEnd) return false;
    pData += 2;

    pSize = strstr(pData, "\r\n");
    if (pSize == NULL || pSize >= pEnd) return false;
    if ((size_t)(pSize - pData) != size) return false;
    pSize += 2;

    if (!test) {
      memmove(pRet, pData, size);
      pRet += size;
    }

    size = strtoul(pSize, NULL, 16);
  }

  if (!test) {
    *pRet = '\0';
  }

  return true;
}

bool httpReadChunkedBody(HttpContext* pContext, HttpParser* pParser) {
  for (int tryTimes = 0; tryTimes < 100; ++tryTimes) {
    bool parsedOk = httpParseChunkedBody(pContext, pParser, true);
    if (parsedOk) {
      // httpTrace("context:%p, fd:%d, ip:%s, chunked body read finished",
      // pContext, pContext->fd, pContext->ipstr);
      httpParseChunkedBody(pContext, pParser, false);
      return true;
    } else {
      httpTrace("context:%p, fd:%d, ip:%s, chunked body not finished, continue read", pContext, pContext->fd,
                pContext->ipstr);
      if (!httpReadDataImp(pContext)) {
        httpError("context:%p, fd:%d, ip:%s, read chunked request error", pContext, pContext->fd, pContext->ipstr);
        return false;
      } else {
        taosMsleep(1);
      }
    }
  }

  httpError("context:%p, fd:%d, ip:%s, chunked body parsed error", pContext, pContext->fd, pContext->ipstr);
  httpSendErrorResp(pContext, HTTP_PARSE_CHUNKED_BODY_ERROR);

  return false;
}

bool httpReadUnChunkedBody(HttpContext* pContext, HttpParser* pParser) {
  for (int tryTimes = 0; tryTimes < 100; ++tryTimes) {
    int dataReadLen = pParser->bufsize - (int)(pParser->data.pos - pParser->buffer);
    if (dataReadLen > pParser->data.len) {
      httpError("context:%p, fd:%d, ip:%s, un-chunked body length invalid, dataReadLen:%d > pContext->data.len:%d",
                pContext, pContext->fd, pContext->ipstr, dataReadLen, pParser->data.len);
      httpSendErrorResp(pContext, HTTP_PARSE_BODY_ERROR);
      return false;
    } else if (dataReadLen < pParser->data.len) {
      httpTrace("context:%p, fd:%d, ip:%s, un-chunked body not finished, dataReadLen:%d < pContext->data.len:%d, continue read",
                pContext, pContext->fd, pContext->ipstr, dataReadLen, pParser->data.len);
      if (!httpReadDataImp(pContext)) {
        httpError("context:%p, fd:%d, ip:%s, read chunked request error", pContext, pContext->fd, pContext->ipstr);
        return false;
      } else {
        taosMsleep(1);
      }
    } else {
      return true;
    }
  }

  int dataReadLen = pParser->bufsize - (int)(pParser->data.pos - pParser->buffer);
  if (dataReadLen != pParser->data.len) {
    httpError("context:%p, fd:%d, ip:%s, un-chunked body length error, dataReadLen:%d != pContext->data.len:%d",
              pContext, pContext->fd, pContext->ipstr, dataReadLen, pParser->data.len);
    httpSendErrorResp(pContext, HTTP_PARSE_BODY_ERROR);
    return false;
  }

  httpTrace("context:%p, fd:%d, ip:%s, un-chunked body read over, dataReadLen:%d == pContext->data.len:%d",
            pContext, pContext->fd, pContext->ipstr, dataReadLen, pParser->data.len);
  return true;
}

bool httpParseRequest(HttpContext* pContext) {
  HttpParser* pParser = &pContext->pThread->parser;

  if (!httpGetHttpMethod(pContext)) {
    return false;
  }

  if (!httpParseURL(pContext)) {
    return false;
  }

  if (!httpParseHttpVersion(pContext)) {
    return false;
  }

  if (!httpGetDecodeMethod(pContext)) {
    return false;
  }

  do {
    if (!httpGetNextLine(pContext)) {
      return false;
    }

    // Empty line, end of the HTTP HEAD
    if (pParser->pCur - pParser->pLast == 1) {
      pParser->data.pos = ++pParser->pCur;
      break;
    }

    if (!httpParseHead(pContext)) {
      return false;
    }

    pParser->pLast = ++pParser->pCur;
  } while (1);

  if (pContext->httpChunked == HTTP_UNCUNKED) {
    if (!httpReadUnChunkedBody(pContext, pParser)) {
      return false;
    }
  } else {
    if (!httpReadChunkedBody(pContext, pParser)) {
      return false;
    }
  }

  httpTrace("context:%p, fd:%d, ip:%s, parse http request ok", pContext, pContext->fd, pContext->ipstr);
  return true;
}

bool httpDecodeRequest(HttpContext* pContext) {
  HttpParser* pParser = &pContext->pThread->parser;
  if (pParser->pMethod->decodeFp == NULL) {
    return false;
  }

  return (*pParser->pMethod->decodeFp)(pContext);
}

/**
 * Process the request from http pServer
 */
bool httpProcessData(HttpContext* pContext) {
  httpInitContext(pContext);

  if (!httpParseRequest(pContext)) {
    httpCloseContextByApp(pContext);
    return HTTP_PROCESS_ERROR;
  }

  // handle Cross-domain request
  if (strcmp(pContext->pThread->parser.method.pos, "OPTIONS") == 0) {
    httpTrace("context:%p, fd:%d, ip:%s, process options request", pContext, pContext->fd, pContext->ipstr);
    httpSendOptionResp(pContext, "process options request success");
    return HTTP_PROCESS_SUCCESS;
  }

  if (!httpDecodeRequest(pContext)) {
    httpCloseContextByApp(pContext);
    return HTTP_PROCESS_SUCCESS;
  }

  httpProcessRequest(pContext);
  return HTTP_PROCESS_SUCCESS;
}
