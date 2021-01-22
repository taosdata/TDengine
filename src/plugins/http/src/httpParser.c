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
#include "httpContext.h"
#include "httpParser.h"
#include "httpGzip.h"
#include "httpAuth.h"

static void httpOnData(ehttp_gzip_t *gzip, void *arg, const char *buf, int32_t len);

static HttpStatus httpStatusCodes[] = {
  {100, "Continue"},
  {101, "Switching Protocol"},
  {102, "Processing (WebDAV)"},
  {103, "Early Hints"},
  {200, "OK"},
  {201, "Created"},
  {202, "Accepted"},
  {203, "Non-Authoritative Information"},
  {204, "No Content"},
  {205, "Reset Content"},
  {206, "Partial Content"},
  {207, "Multi-Status (WebDAV)"},
  {208, "Already Reported (WebDAV)"},
  {226, "IM Used (HTTP Delta encoding)"},
  {300, "Multiple Choice"},
  {301, "Moved Permanently"},
  {302, "Found"},
  {303, "See Other"},
  {304, "Not Modified"},
  {305, "Use Proxy"},
  {306, "unused"},
  {307, "Temporary Redirect"},
  {308, "Permanent Redirect"},
  {400, "Bad Request"},
  {401, "Unauthorized"},
  {402, "Payment Required"},
  {403, "Forbidden"},
  {404, "Not Found"},
  {405, "Method Not Allowed"},
  {406, "Not Acceptable"},
  {407, "Proxy Authentication Required"},
  {408, "Request Timeout"},
  {409, "Conflict"},
  {410, "Gone"},
  {411, "Length Required"},
  {412, "Precondition Failed"},
  {413, "Payload Too Large"},
  {414, "URI Too Long"},
  {415, "Unsupported Media Type"},
  {416, "Range Not Satisfiable"},
  {417, "Expectation Failed"},
  {418, "I'm a teapot"},
  {421, "Misdirected Request"},
  {422, "Unprocessable Entity (WebDAV)"},
  {423, "Locked (WebDAV)"},
  {424, "Failed Dependency (WebDAV)"},
  {425, "Too Early"},
  {426, "Upgrade Required"},
  {428, "Precondition Required"},
  {429, "Too Many Requests"},
  {431, "Request Header Fields Too Large"},
  {451, "Unavailable For Legal Reasons"},
  {500, "Internal Server Error"},
  {501, "Not Implemented"},
  {502, "Bad Gateway"},
  {503, "Service Unavailable"},
  {504, "Gateway Timeout"},
  {505, "HTTP Version Not Supported"},
  {506, "Variant Also Negotiates"},
  {507, "Insufficient Storage"},
  {508, "Loop Detected (WebDAV)"},
  {510, "Not Extended"},
  {511, "Network Authentication Required"},
  {0,   NULL}
};

char *httpGetStatusDesc(int32_t statusCode) {
  HttpStatus *p = httpStatusCodes;
  while (p->code != 0) {
    if (p->code == statusCode) return p->desc;
    ++p;
  }
  return "Unknow status code";
}

static void httpCleanupString(HttpString *str) {
  free(str->str);
  str->str = NULL;
  str->pos = 0;
  str->size = 0;
}

static int32_t httpAppendString(HttpString *str, const char *s, int32_t len) {
  if (str->size == 0) {
    str->pos = 0;
    str->size = 64;
    str->str = malloc(str->size);
  } else if (str->pos + len + 1 >= str->size) {
    str->size += len;
    str->size *= 4;
    str->str = realloc(str->str, str->size);
  } else {
  }

  if (str->str == NULL) return -1;

  memcpy(str->str + str->pos, s, len);
  str->pos += len;
  str->str[str->pos] = 0;
  return 0;
}

static void httpClearString(HttpString *str) {
  if (str->str) {
    str->str[0] = '\0';
    str->pos = 0;
  }
}

static int32_t httpOnError(HttpParser *parser, int32_t httpCode, int32_t parseCode) {
  HttpContext *pContext = parser->pContext;
  if (httpCode != 0) parser->httpCode = httpCode;
  if (parseCode != 0) parser->parseCode = parseCode;

  httpError("context:%p, fd:%d, parse failed, httpCode:%d parseCode:%d reason:%s", pContext, pContext->fd, httpCode,
            parseCode & 0XFFFF, tstrerror(parseCode));
  return 0;
}

static int32_t httpOnRequestLine(HttpParser *pParser, char *method, char *target, char *version) {
  HttpContext *pContext = pParser->pContext;
  httpDebug("context:%p, fd:%d, method:%s target:%s version:%s", pContext, pContext->fd, method, target, version);

  // parse url
  char *pStart = target + 1;
  for (int32_t i = 0; i < HTTP_MAX_URL; i++) {
    char *pSeek = strchr(pStart, '/');
    if (pSeek == NULL) {
      (void)httpAppendString(pParser->path + i, pStart, (int32_t)strlen(pStart));
      break;
    } else {
      (void)httpAppendString(pParser->path + i, pStart, (int32_t)(pSeek - pStart));
    }
    pStart = pSeek + 1;
  }

  // parse decode method
  for (int32_t i = 0; i < tsHttpServer.methodScannerLen; i++) {
    HttpDecodeMethod *method = tsHttpServer.methodScanner[i];
    if (strcmp(method->module, pParser->path[0].str) == 0) {
      pContext->decodeMethod = method;
      break;
    }
  }

  if (pContext->decodeMethod != NULL) {
    httpTrace("context:%p, fd:%d, decode method is %s", pContext, pContext->fd, pContext->decodeMethod->module);
  } else {
    httpError("context:%p, fd:%d, the url is not support, target:%s", pContext, pContext->fd, target);
    httpOnError(pParser, 0, TSDB_CODE_HTTP_UNSUPPORT_URL);
    return -1;
  }

  // parse version
  if (pParser->httpVersion < HTTP_VERSION_10 || pParser->httpVersion > HTTP_VERSION_12) {
    httpError("context:%p, fd:%d, unsupport httpVersion %d", pContext, pContext->fd, pParser->httpVersion);
    httpOnError(pParser, 0, TSDB_CODE_HTTP_INVALID_VERSION);
  } else {
    httpTrace("context:%p, fd:%d, httpVersion:1.%d", pContext, pContext->fd, pParser->httpVersion);
  }

  return 0;
}

static int32_t httpOnStatusLine(HttpParser *pParser, int32_t code, const char *reason) {
  HttpContext *pContext = pParser->pContext;
  httpError("context:%p, fd:%d, status line, code:%d reason:%s", pContext, pContext->fd, code, reason);
  return 0;
}

static int32_t httpOnParseHeaderField(HttpParser *parser, const char *key, const char *val) {
  HttpContext *pContext = parser->pContext;
  httpTrace("context:%p, fd:%d, key:%s val:%s", pContext, pContext->fd, key, val);

  if (0 == strcasecmp(key, "Content-Length")) {
    int32_t len = 0;
    int32_t bytes = 0;
    int32_t n = sscanf(val, "%d%n", &len, &bytes);
    if (n == 1 && bytes == strlen(val)) {
      parser->contentLength = len;
      parser->chunkSize = len;
      parser->contentLengthSpecified = 1;
      httpTrace("context:%p, fd:%d, contentLength:%d chunkSize:%d contentLengthSpecified:%d", pContext, pContext->fd,
                parser->contentLength, parser->chunkSize, parser->contentLengthSpecified);
      return 0;
    } else {
      httpError("context:%p, fd:%d, failed to parser %s:%s", pContext, pContext->fd, key, val);
      httpOnError(parser, 0, TSDB_CODE_HTTP_INVALID_CONTENT_LENGTH);
      return -1;
    }
  }

  else if (0 == strcasecmp(key, "Accept-Encoding")) {
    if (strstr(val, "gzip")) {
      parser->acceptEncodingGzip = 1;
      httpTrace("context:%p, fd:%d, acceptEncodingGzip:%d", pContext, pContext->fd, parser->acceptEncodingGzip);
    }
    if (strstr(val, "chunked")) {
      parser->acceptEncodingChunked = 1;
      httpTrace("context:%p, fd:%d, acceptEncodingChunked:%d", pContext, pContext->fd, parser->acceptEncodingChunked);
    }
    return 0;
  }

  else if (strncasecmp(key, "Connection: ", 12) == 0) {
    if (strncasecmp(val, "Keep-Alive", 10) == 0) {
      parser->keepAlive = HTTP_KEEPALIVE_ENABLE;
    } else {
      parser->keepAlive = HTTP_KEEPALIVE_DISABLE;
    }
    httpTrace("context:%p, fd:%d, keepAlive:%d", pContext, pContext->fd, pContext->parser->keepAlive);
  }
#if 0
  else if (0 == strcasecmp(key, "Content-Encoding")) {
    if (0 == strcmp(val, "gzip")) {
      parser->contentChunked = 1;
      httpTrace("context:%p, fd:%d, contentChunked:%d", pContext, pContext->fd, parser->contentChunked);
    }
    return 0;
  }
#endif

  else if (0 == strcasecmp(key, "Transfer-Encoding") || 0 == strcasecmp(key, "Content-Encoding")) {
    if (strstr(val, "gzip")) {
      parser->transferGzip = 1;
      ehttp_gzip_conf_t      conf = {0};
      ehttp_gzip_callbacks_t callbacks = {0};

      callbacks.on_data = httpOnData;

      parser->gzip = ehttp_gzip_create_decompressor(conf, callbacks, parser);

      if (!parser->gzip) {
        httpError("context:%p, fd:%d, failed to create gzip decompressor", pContext, pContext->fd);
        httpOnError(parser, 0, TSDB_CODE_HTTP_CREATE_GZIP_FAILED);
        return -1;
      }
    }
    if (strstr(val, "chunked")) {
      parser->transferChunked = 1;
      httpTrace("context:%p, fd:%d, transferChunked:%d", pContext, pContext->fd, parser->transferChunked);
    }
    return 0;
  }

  else if (0 == strcasecmp(key, "Authorization")) {
    char *  t = NULL;
    char *  s = NULL;
    int32_t bytes = 0;
    int32_t n = sscanf(val, "%ms %ms%n", &t, &s, &bytes);
    if (n == 2 && t && s && bytes == strlen(val)) {
      if (strcmp(t, "Basic") == 0) {
        free(parser->authContent);
        parser->authContent = s;
        parser->authType = HTTP_BASIC_AUTH;
        s = NULL;
        free(t);
        free(s);
        httpTrace("context:%p, fd:%d, basic auth:%s", pContext, pContext->fd, parser->authContent);
        int32_t ok = httpParseBasicAuthToken(pContext, parser->authContent, (int32_t)strlen(parser->authContent));
        if (ok != 0) {
          httpOnError(parser, 0, TSDB_CODE_HTTP_INVALID_BASIC_AUTH);
          return -1;
        }
        return 0;
      } else if (strcmp(t, "Taosd") == 0) {
        free(parser->authContent);
        parser->authContent = s;
        parser->authType = HTTP_TAOSD_AUTH;
        s = NULL;
        free(t);
        free(s);
        httpTrace("context:%p, fd:%d, taosd auth:%s", pContext, pContext->fd, parser->authContent);
        int32_t ok = httpParseTaosdAuthToken(pContext, parser->authContent, (int32_t)strlen(parser->authContent));
        if (ok != 0) {
          httpOnError(parser, 0, TSDB_CODE_HTTP_INVALID_TAOSD_AUTH);
          return -1;
        }
        return 0;
      } else {
        parser->authType = HTTP_INVALID_AUTH;
        httpError("context:%p, fd:%d, invalid auth, t:%s s:%s", pContext, pContext->fd, t, s);
        httpOnError(parser, 0, TSDB_CODE_HTTP_INVALID_AUTH_TYPE);
        free(t);
        free(s);
        return -1;
      }
    } else {
      parser->authType = HTTP_INVALID_AUTH;
      httpError("context:%p, fd:%d, parse auth failed, t:%s s:%s", pContext, pContext->fd, t, s);
      httpOnError(parser, 0, TSDB_CODE_HTTP_INVALID_AUTH_FORMAT);
      free(t);
      free(s);
      return -1;
    }
  }

  return 0;
}

static int32_t httpOnBody(HttpParser *parser, const char *chunk, int32_t len) {
  HttpContext *pContext = parser->pContext;
  HttpString * buf = &parser->body;
  if (parser->parseCode != TSDB_CODE_SUCCESS) return -1;

  if (buf->size <= 0) {
    buf->size = MIN(len + 2, HTTP_BUFFER_SIZE);
    buf->str = malloc(buf->size);
  }

  int32_t newSize = buf->pos + len + 1;
  if (newSize >= buf->size) {
    if (buf->size >= HTTP_BUFFER_SIZE) {
      httpError("context:%p, fd:%d, failed parse body, exceeding buffer size %d", pContext, pContext->fd, buf->size);
      httpOnError(parser, 0, TSDB_CODE_HTTP_REQUSET_TOO_BIG);
      return -1;
    }

    newSize = MAX(newSize, HTTP_BUFFER_INIT);
    newSize *= 4;
    newSize = MIN(newSize, HTTP_BUFFER_SIZE);
    buf->str = realloc(buf->str, newSize);
    buf->size = newSize;

    if (buf->str == NULL) {
      httpError("context:%p, fd:%d, failed parse body, realloc %d failed", pContext, pContext->fd, buf->size);
      httpOnError(parser, 0, TSDB_CODE_HTTP_NO_ENOUGH_MEMORY);
      return -1;
    }
  }

  memcpy(buf->str + buf->pos, chunk, len);
  buf->pos += len;
  buf->str[buf->pos] = 0;

  return 0;
}

static int32_t httpOnEnd(HttpParser *parser) {
  HttpContext *pContext = parser->pContext;
  parser->parsed = true;

  if (parser->parseCode != TSDB_CODE_SUCCESS) {
    return -1;
  }

  httpTrace("context:%p, fd:%d, parse success", pContext, pContext->fd);
  return 0;
}

static HTTP_PARSER_STATE httpTopStack(HttpParser *parser) {
  HttpStack *stack = &parser->stacks;
  ASSERT(stack->pos >= 1);

  return stack->stacks[stack->pos - 1];
}

static int32_t httpPushStack(HttpParser *parser, HTTP_PARSER_STATE state) {
  HttpStack *stack = &parser->stacks;
  if (stack->size == 0) {
    stack->pos = 0;
    stack->size = 32;
    stack->stacks = malloc(stack->size * sizeof(int8_t));
  } else if (stack->pos + 1 > stack->size) {
    stack->size *= 2;
    stack->stacks = realloc(stack->stacks, stack->size * sizeof(int8_t));
  } else {
  }

  if (stack->stacks == NULL) return -1;

  stack->stacks[stack->pos] = state;
  stack->pos++;

  return 0;
}

static int32_t httpPopStack(HttpParser *parser) {
  HttpStack *stack = &parser->stacks;
  ASSERT(stack->pos >= 1);
  stack->pos--;
  return 0;
}

static void httpClearStack(HttpStack *stack) { stack->pos = 0; }

static int32_t httpCleanupStack(HttpStack *stack) {
  free(stack->stacks);
  memset(stack, 0, sizeof(HttpStack));

  return 0;
}

void httpInitParser(HttpParser *parser) {
  HttpContext *pContext = parser->pContext;
  httpTrace("context:%p, fd:%d, init parser", pContext, pContext->fd);

  parser->parsed = false;
  parser->inited = 1;
  parser->httpVersion = 0;
  parser->acceptEncodingGzip = 0;
  parser->acceptEncodingChunked = 0;
  parser->contentLengthSpecified = 0;
  parser->contentChunked = 0;
  parser->transferGzip = 0;
  parser->transferChunked = 0;
  parser->keepAlive = 0;
  parser->authType = 0;
  parser->contentLength = 0;
  parser->chunkSize = 0;
  parser->receivedChunkSize = 0;
  parser->receivedSize = 0;
  parser->statusCode = 0;
  parser->httpCode = 0;
  parser->parseCode = 0;

  free(parser->method);         parser->method          = NULL;
  free(parser->target);         parser->target          = NULL;
  free(parser->version);        parser->version         = NULL;
  free(parser->reasonPhrase);   parser->reasonPhrase    = NULL;
  free(parser->key);            parser->key             = NULL;
  free(parser->val);            parser->val             = NULL;
  free(parser->authContent);    parser->authContent     = NULL;

  httpClearStack(&parser->stacks);
  httpClearString(&parser->str);
  httpClearString(&parser->body);
  for (int32_t i = 0; i < HTTP_MAX_URL; ++i) {
    httpClearString(&parser->path[i]);
  }

  if (parser->gzip != NULL) {
    ehttp_gzip_destroy(parser->gzip);
    parser->gzip = NULL;
  }

  httpPushStack(parser, HTTP_PARSER_BEGIN);
}

HttpParser *httpCreateParser(HttpContext *pContext) {
  HttpParser *parser = calloc(1, sizeof(HttpParser));
  if (!parser) return NULL;
  httpTrace("context:%p, fd:%d, create parser", pContext, pContext->fd);

  parser->pContext = pContext;
  return parser;
}

void httpClearParser(HttpParser *parser) {
  HttpContext *pContext = parser->pContext;
  httpTrace("context:%p, fd:%d, clear parser", pContext, pContext->fd);

  pContext->parser->inited = 0;
  pContext->parser->parsed = false;
}

void httpDestroyParser(HttpParser *parser) {
  if (!parser) return;

  HttpContext *pContext = parser->pContext;
  httpTrace("context:%p, fd:%d, destroy parser", pContext, pContext->fd);

  free(parser->method);         parser->method          = NULL;
  free(parser->target);         parser->target          = NULL;
  free(parser->version);        parser->version         = NULL;
  free(parser->reasonPhrase);   parser->reasonPhrase    = NULL;
  free(parser->key);            parser->key             = NULL;
  free(parser->val);            parser->val             = NULL;
  free(parser->authContent);    parser->authContent     = NULL;

  httpCleanupStack(&parser->stacks);
  httpCleanupString(&parser->str);
  httpCleanupString(&parser->body);
  for (int32_t i = 0; i < HTTP_MAX_URL; ++i) {
    httpCleanupString(&parser->path[i]);
  }

  if (parser->gzip != NULL) {
    ehttp_gzip_destroy(parser->gzip);
    parser->gzip = NULL;
  }

  free(parser);
}

#define is_token(c) (strchr("!#$%&'*+-.^_`|~", c) || isdigit(c) || isalpha(c))

char *httpDecodeUrl(const char *enc) {
  int32_t    ok = 1;
  HttpString str = {0};
  while (*enc) {
    char *p = strchr(enc, '%');
    if (!p) break;
    int32_t hex, cnt;
    int32_t n = sscanf(p + 1, "%2x%n", &hex, &cnt);
    if (n != 1 && cnt != 2) {
      ok = 0;
      break;
    }
    if (httpAppendString(&str, enc, (int32_t)(p - enc))) {
      ok = 0;
      break;
    }
    char c = (char)hex;
    if (httpAppendString(&str, &c, 1)) {
      ok = 0;
      break;
    }
    enc = p + 3;
  }
  char *dec = NULL;
  if (ok && *enc) {
    if (httpAppendString(&str, enc, (int32_t)strlen(enc))) {
      ok = 0;
    }
  }
  if (ok) {
    dec = str.str;
    str.str = NULL;
  }
  httpCleanupString(&str);
  return dec;
}

static void httpOnData(ehttp_gzip_t *gzip, void *arg, const char *buf, int32_t len) {
  HttpParser *parser = (HttpParser *)arg;
  httpOnBody(parser, buf, len);
}

static int32_t httpParserOnBegin(HttpParser *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  HttpContext *pContext = parser->pContext;
  int32_t      ok = 0;
  do {
    if (c == 'G' || c == 'P' || c == 'H' || c == 'D' || c == 'C' || c == 'O' || c == 'T') {
      if (httpAppendString(&parser->str, &c, 1)) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_METHOD_FAILED);
        break;
      }
      httpPopStack(parser);
      httpPushStack(parser, HTTP_PARSER_REQUEST_OR_RESPONSE);
      break;
    }
    httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
    ok = -1;
    httpOnError(parser, 400, TSDB_CODE_HTTP_PARSE_METHOD_FAILED);
  } while (0);
  return ok;
}

static int32_t httpParserOnRquestOrResponse(HttpParser *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  HttpContext *pContext = parser->pContext;
  int32_t      ok = 0;
  do {
    if (parser->str.pos == 1) {
      if (c == 'T' && parser->str.str[0] == 'H') {
        httpPopStack(parser);
        httpPushStack(parser, HTTP_PARSER_END);
        httpPushStack(parser, HTTP_PARSER_HEADER);
        httpPushStack(parser, HTTP_PARSER_CRLF);
        httpPushStack(parser, HTTP_PARSER_REASON_PHRASE);
        httpPushStack(parser, HTTP_PARSER_SP);
        httpPushStack(parser, HTTP_PARSER_STATUS_CODE);
        httpPushStack(parser, HTTP_PARSER_SP);
        httpPushStack(parser, HTTP_PARSER_HTTP_VERSION);
        *again = 1;
        break;
      }
      httpPopStack(parser);
      httpPushStack(parser, HTTP_PARSER_END);
      httpPushStack(parser, HTTP_PARSER_HEADER);
      httpPushStack(parser, HTTP_PARSER_CRLF);
      httpPushStack(parser, HTTP_PARSER_HTTP_VERSION);
      httpPushStack(parser, HTTP_PARSER_SP);
      httpPushStack(parser, HTTP_PARSER_TARGET);
      httpPushStack(parser, HTTP_PARSER_SP);
      httpPushStack(parser, HTTP_PARSER_METHOD);
      *again = 1;
      break;
    }

    httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
    ok = -1;
    httpOnError(parser, 400, TSDB_CODE_HTTP_PARSE_METHOD_FAILED);
  } while (0);
  return ok;
}

static int32_t httpParserOnMethod(HttpParser *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  HttpContext *pContext = parser->pContext;
  int32_t      ok = 0;
  do {
    if (isalnum(c) || strchr("!#$%&'*+-.^_`|~", c)) {
      if (httpAppendString(&parser->str, &c, 1)) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_METHOD_FAILED);
        break;
      }
      break;
    }
    parser->method = strdup(parser->str.str);
    if (!parser->method) {
      httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
      ok = -1;
      httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_METHOD_FAILED);
      break;
    } else {
      httpTrace("context:%p, fd:%d, httpMethod:%s", pContext, pContext->fd, parser->method);
    }
    httpClearString(&parser->str);
    httpPopStack(parser);
    *again = 1;
  } while (0);
  return ok;
}

static int32_t httpParserOnTarget(HttpParser *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  HttpContext *pContext = parser->pContext;
  int32_t      ok = 0;
  do {
    if (!isspace(c) && c != '\r' && c != '\n') {
      if (httpAppendString(&parser->str, &c, 1)) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_TARGET_FAILED);
        break;
      }
      break;
    }
    parser->target = strdup(parser->str.str);
    if (!parser->target) {
      httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
      ok = -1;
      httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_TARGET_FAILED);
      break;
    }
    httpClearString(&parser->str);
    httpPopStack(parser);
    *again = 1;
  } while (0);
  return ok;
}

static int32_t httpParserOnVersion(HttpParser *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  HttpContext *pContext = parser->pContext;
  int32_t      ok = 0;
  do {
    const char *prefix = "HTTP/1.";
    int32_t     len = (int32_t)strlen(prefix);
    if (parser->str.pos < len) {
      if (prefix[parser->str.pos] != c) {
        httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
        ok = -1;
        httpOnError(parser, 400, TSDB_CODE_HTTP_PARSE_VERSION_FAILED);
        break;
      }
      if (httpAppendString(&parser->str, &c, 1)) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_VERSION_FAILED);
        break;
      }
      break;
    }

    if (c != '0' && c != '1' && c != '2') {
      httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
      ok = -1;
      httpOnError(parser, 400, TSDB_CODE_HTTP_PARSE_VERSION_FAILED);
      break;
    }

    if (httpAppendString(&parser->str, &c, 1)) {
      httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
      ok = -1;
      httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_VERSION_FAILED);
      break;
    }

    if (c == '0')
      parser->httpVersion = HTTP_VERSION_10;
    else if (c == '1')
      parser->httpVersion = HTTP_VERSION_11;
    else if (c == '2')
      parser->httpVersion = HTTP_VERSION_12;
    else {
    }

    parser->version = strdup(parser->str.str);
    if (!parser->version) {
      httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
      ok = -1;
      httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_VERSION_FAILED);
      break;
    }

    if (parser->method) {
      ok = httpOnRequestLine(parser, parser->method, parser->target, parser->version);
    }

    httpClearString(&parser->str);
    httpPopStack(parser);
  } while (0);
  return ok;
}

static int32_t httpParserOnSp(HttpParser *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  HttpContext *pContext = parser->pContext;
  int32_t      ok = 0;
  do {
    if (c == ' ') {
      httpPopStack(parser);
      break;
    }
    httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
    ok = -1;
    httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_SP_FAILED);
  } while (0);
  return ok;
}

static int32_t httpParserOnStatusCode(HttpParser *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  HttpContext *pContext = parser->pContext;
  int32_t      ok = 0;
  do {
    if (isdigit(c)) {
      if (httpAppendString(&parser->str, &c, 1)) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_STATUS_FAILED);
        break;
      }
      if (parser->str.pos < 3) break;

      sscanf(parser->str.str, "%d", &parser->statusCode);
      httpClearString(&parser->str);
      httpPopStack(parser);
      break;
    }
    httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
    ok = -1;
    httpOnError(parser, 400, TSDB_CODE_HTTP_PARSE_STATUS_FAILED);
  } while (0);
  return ok;
}

static int32_t httpParserOnReasonPhrase(HttpParser *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  HttpContext *pContext = parser->pContext;
  int32_t      ok = 0;
  do {
    if (c == '\r') {
      parser->reasonPhrase = strdup(parser->str.str);
      if (!parser->reasonPhrase) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_PHRASE_FAILED);
        break;
      }
      ok = httpOnStatusLine(parser, parser->statusCode, parser->reasonPhrase);
      httpClearString(&parser->str);
      httpPopStack(parser);
      *again = 1;
      break;
    }
    if (httpAppendString(&parser->str, &c, 1)) {
      httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
      ok = -1;
      httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_PHRASE_FAILED);
      break;
    }
  } while (0);
  return ok;
}

static int32_t httpParserPostProcess(HttpParser *parser) {
  HttpContext *pContext = parser->pContext;
  if (parser->gzip) {
    if (ehttp_gzip_finish(parser->gzip)) {
      httpError("context:%p, fd:%d, gzip failed", pContext, pContext->fd);
      httpOnError(parser, 507, TSDB_CODE_HTTP_FINISH_GZIP_FAILED);
      return -1;
    }
  }
  httpOnEnd(parser);
  return 0;
}

static int32_t httpParserOnCrlf(HttpParser *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  HttpContext *pContext = parser->pContext;
  int32_t      ok = 0;
  do {
    const char *s = "\r\n";
    int32_t     len = (int32_t)strlen(s);
    if (s[parser->str.pos] != c) {
      httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
      ok = -1;
      httpOnError(parser, 400, TSDB_CODE_HTTP_PARSE_CRLF_FAILED);
      break;
    }
    if (httpAppendString(&parser->str, &c, 1)) {
      httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
      ok = -1;
      httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_CRLF_FAILED);
      break;
    }
    if (parser->str.pos == len) {
      httpClearString(&parser->str);
      httpPopStack(parser);
      if (httpTopStack(parser) == HTTP_PARSER_END) {
        ok = httpParserPostProcess(parser);
      }
    }
    break;
  } while (0);
  return ok;
}

static int32_t httpParserOnHeader(HttpParser *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  HttpContext *pContext = parser->pContext;
  int32_t      ok = 0;
  do {
    if (c == '\r') {
      httpPopStack(parser);
      if (parser->transferChunked) {
        httpPushStack(parser, HTTP_PARSER_CHUNK_SIZE);
        httpPushStack(parser, HTTP_PARSER_CRLF);
      } else {
        if (parser->contentLength > 0) {
          httpPushStack(parser, HTTP_PARSER_CHUNK);
        }
        httpPushStack(parser, HTTP_PARSER_CRLF);
      }
      *again = 1;
      break;
    }
    if (c != ' ' && c != '\t' && c != ':') {
      if (httpAppendString(&parser->str, &c, 1)) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_HEADER_FAILED);
        break;
      }
      httpPushStack(parser, HTTP_PARSER_CRLF);
      httpPushStack(parser, HTTP_PARSER_HEADER_VAL);
      httpPushStack(parser, HTTP_PARSER_SP);
      httpPushStack(parser, HTTP_PARSER_HEADER_KEY);
      break;
    }
    httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
    ok = -1;
    httpOnError(parser, 400, TSDB_CODE_HTTP_PARSE_HEADER_FAILED);
  } while (0);
  return ok;
}

static int32_t httpParserOnHeaderKey(HttpParser *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  HttpContext *pContext = parser->pContext;
  int32_t      ok = 0;
  do {
    if (isalnum(c) || strchr("!#$%&'*+-.^_`|~", c)) {
      if (httpAppendString(&parser->str, &c, 1)) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_HEADER_KEY_FAILED);
        break;
      }
      break;
    }
    if (c == ':') {
      parser->key = strdup(parser->str.str);
      if (!parser->key) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_HEADER_KEY_FAILED);
        break;
      }
      httpClearString(&parser->str);
      httpPopStack(parser);
      break;
    }
    httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
    ok = -1;
    httpOnError(parser, 400, TSDB_CODE_HTTP_PARSE_HEADER_KEY_FAILED);
  } while (0);
  return ok;
}

static int32_t httpParserOnHeaderVal(HttpParser *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  HttpContext *pContext = parser->pContext;
  int32_t      ok = 0;
  do {
    if (c != '\r' && c != '\n' && (!isspace(c) || parser->str.pos > 0)) {
      if (httpAppendString(&parser->str, &c, 1)) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        parser->parseCode = TSDB_CODE_HTTP_PARSE_HEADER_VAL_FAILED;
        httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_HEADER_VAL_FAILED);
        break;
      }
      break;
    }
    const char *val = parser->str.str;
    ok = httpOnParseHeaderField(parser, parser->key, val);
    free(parser->key);
    parser->key = NULL;
    val = NULL;
    if (ok == -1) break;
    httpClearString(&parser->str);
    httpPopStack(parser);
    *again = 1;
  } while (0);
  return ok;
}

static int32_t httpParserOnChunkSize(HttpParser *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  HttpContext *pContext = parser->pContext;
  int32_t      ok = 0;
  int32_t      bytes;
  int32_t      len;
  int32_t      n;
  do {
    if (isxdigit(c)) {
      if (httpAppendString(&parser->str, &c, 1)) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_CHUNK_SIZE_FAILED);
        break;
      }
      break;
    }
    if (c == '\r') {
      n = sscanf(parser->str.str, "%x%n", &len, &bytes);
      if (n == 1 && bytes == strlen(parser->str.str) && len >= 0) {
        if (len == 0) {
          if (parser->contentLengthSpecified == 0 || parser->receivedSize == parser->contentLength) {
            httpClearString(&parser->str);
            httpPopStack(parser);
            httpPushStack(parser, HTTP_PARSER_CRLF);
            httpPushStack(parser, HTTP_PARSER_CRLF);
            *again = 1;
            break;
          }
        } else {
          if (parser->contentLengthSpecified == 0 || parser->receivedSize + len <= parser->contentLength) {
            parser->chunkSize = len;
            httpClearString(&parser->str);
            httpPopStack(parser);
            httpPushStack(parser, HTTP_PARSER_CHUNK_SIZE);
            httpPushStack(parser, HTTP_PARSER_CRLF);
            httpPushStack(parser, HTTP_PARSER_CHUNK);
            httpPushStack(parser, HTTP_PARSER_CRLF);
            *again = 1;
            break;
          }
        }
      }
    }
    httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
    ok = -1;
    httpOnError(parser, 400, TSDB_CODE_HTTP_PARSE_CHUNK_SIZE_FAILED);
  } while (0);
  return ok;
}

static int32_t httpParserOnChunk(HttpParser *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  HttpContext *pContext = parser->pContext;
  int32_t      ok = 0;
  do {
    if (httpAppendString(&parser->str, &c, 1)) {
      httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
      ok = -1;
      httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_CHUNK_FAILED);
      break;
    }
    ++parser->receivedSize;
    ++parser->receivedChunkSize;
    if (parser->receivedChunkSize < parser->chunkSize) break;

    if (parser->gzip) {
      if (ehttp_gzip_write(parser->gzip, parser->str.str, parser->str.pos)) {
        httpError("context:%p, fd:%d, gzip failed", pContext, pContext->fd);
        ok = -1;
        httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_CHUNK_FAILED);
        break;
      }
    } else {
      httpOnBody(parser, parser->str.str, parser->str.pos);
    }
    parser->receivedChunkSize = 0;
    httpClearString(&parser->str);
    httpPopStack(parser);
    if (httpTopStack(parser) == HTTP_PARSER_END) {
      ok = httpParserPostProcess(parser);
    }
  } while (0);
  return ok;
}

static int32_t httpParserOnEnd(HttpParser *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  HttpContext *pContext = parser->pContext;
  int32_t      ok = 0;
  do {
    ok = -1;
    httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
    httpOnError(parser, 507, TSDB_CODE_HTTP_PARSE_END_FAILED);
  } while (0);
  return ok;
}

static int32_t httpParseChar(HttpParser *parser, const char c, int32_t *again) {
  HttpContext *     pContext = parser->pContext;
  int32_t           ok = 0;
  HTTP_PARSER_STATE state = httpTopStack(parser);
  do {
    if (state == HTTP_PARSER_BEGIN) {
      ok = httpParserOnBegin(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_REQUEST_OR_RESPONSE) {
      ok = httpParserOnRquestOrResponse(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_METHOD) {
      ok = httpParserOnMethod(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_TARGET) {
      ok = httpParserOnTarget(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_HTTP_VERSION) {
      ok = httpParserOnVersion(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_SP) {
      ok = httpParserOnSp(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_STATUS_CODE) {
      ok = httpParserOnStatusCode(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_REASON_PHRASE) {
      ok = httpParserOnReasonPhrase(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_CRLF) {
      ok = httpParserOnCrlf(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_HEADER) {
      ok = httpParserOnHeader(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_HEADER_KEY) {
      ok = httpParserOnHeaderKey(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_HEADER_VAL) {
      ok = httpParserOnHeaderVal(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_CHUNK_SIZE) {
      ok = httpParserOnChunkSize(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_CHUNK) {
      ok = httpParserOnChunk(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_END) {
      ok = httpParserOnEnd(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_ERROR) {
      ok = -2;
      break;
    }

    ok = -1;
    httpError("context:%p, fd:%d, unknown parse state:%d", pContext, pContext->fd, state);
    httpOnError(parser, 500, TSDB_CODE_HTTP_PARSE_INVALID_STATE);
  } while (0);

  if (ok == -1) {
    httpError("context:%p, fd:%d, failed to parse, state:%d", pContext, pContext->fd, state);
    httpPushStack(parser, HTTP_PARSER_ERROR);
  }

  if (ok == -2) {
    ok = -1;
    httpError("context:%p, fd:%d, failed to parse, invalid state", pContext, pContext->fd);
    httpOnError(parser, 500, TSDB_CODE_HTTP_PARSE_ERROR_STATE);
  }

  return ok;
}

int32_t httpParseBuf(HttpParser *parser, const char *buf, int32_t len) {
  HttpContext *pContext = parser->pContext;
  const char * p = buf;
  int32_t      ret = 0;
  int32_t      i = 0;

  while (i < len) {
    int32_t again = 0;
    ret = httpParseChar(parser, *p, &again);
    if (ret != 0) {
      httpError("context:%p, fd:%d, parse failed, ret:%d i:%d len:%d buf:%s", pContext, pContext->fd, ret, i, len, buf);
      break;
    }
    if (again) continue;
    ++p;
    ++i;
  }

  return ret;
}
