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
#include "taosmsg.h"
#include "tsocket.h"
#include "tutil.h"
#include "ttimer.h"
#include "tglobal.h"
#include "tcache.h"
#include "hash.h"
#include "httpInt.h"
#include "httpResp.h"
#include "httpSql.h"
#include "httpSession.h"
#include "httpContext.h"
#include "elog.h"

extern bool httpGetHttpMethod(HttpContext* pContext);
extern bool httpParseURL(HttpContext* pContext);
extern bool httpParseHttpVersion(HttpContext* pContext);
extern bool httpGetDecodeMethod(HttpContext* pContext);
extern bool httpParseHead(HttpContext* pContext);

static void httpParseOnRequestLine(void *arg, const char *method, const char *target, const char *version, const char *target_raw);
static void httpParseOnStatusLine(void *arg, const char *version, int status_code, const char *reason_phrase);
static void httpParseOnHeaderField(void *arg, const char *key, const char *val);
static void httpParseOnBody(void *arg, const char *chunk, size_t len);
static void httpParseOnEnd(void *arg);
static void httpParseOnError(void *arg, int status_code);

static void httpDestroyContext(void *data);

static void httpRemoveContextFromEpoll(HttpContext *pContext) {
  HttpThread *pThread = pContext->pThread;
  if (pContext->fd >= 0) {
    epoll_ctl(pThread->pollFd, EPOLL_CTL_DEL, pContext->fd, NULL);
    int32_t fd = atomic_val_compare_exchange_32(&pContext->fd, pContext->fd, -1);
    taosCloseSocket(fd);
  }
}

static void httpDestroyContext(void *data) {
  HttpContext *pContext = *(HttpContext **)data;
  if (pContext->fd > 0) taosClose(pContext->fd);

  HttpThread *pThread = pContext->pThread;
  httpRemoveContextFromEpoll(pContext);
  httpReleaseSession(pContext);
  atomic_sub_fetch_32(&pThread->numOfContexts, 1);
  
  httpDebug("context:%p, is destroyed, refCount:%d data:%p thread:%s numOfContexts:%d", pContext, pContext->refCount,
            data, pContext->pThread->label, pContext->pThread->numOfContexts);
  pContext->pThread = 0;
  pContext->state = HTTP_CONTEXT_STATE_CLOSED;

  // avoid double free
  httpFreeJsonBuf(pContext);
  httpFreeMultiCmds(pContext);

  if (pContext->parser.parser) {
    httpParserDestroy(pContext->parser.parser);
    pContext->parser.parser = NULL;
  }

  taosTFree(pContext);
}

bool httpInitContexts() {
  tsHttpServer.contextCache = taosCacheInit(TSDB_DATA_TYPE_BIGINT, 2, true, httpDestroyContext, "restc");
  if (tsHttpServer.contextCache == NULL) {
    httpError("failed to init context cache");
    return false;
  }

  return true;
}

void httpCleanupContexts() {
  if (tsHttpServer.contextCache != NULL) {
    SCacheObj *cache = tsHttpServer.contextCache;
    httpInfo("context cache is cleanuping, size:%" PRIzu "", taosHashGetSize(cache->pHashTable));
    taosCacheCleanup(tsHttpServer.contextCache);
    tsHttpServer.contextCache = NULL;
  }
}

const char *httpContextStateStr(HttpContextState state) {
  switch (state) {
    case HTTP_CONTEXT_STATE_READY:
      return "ready";
    case HTTP_CONTEXT_STATE_HANDLING:
      return "handling";
    case HTTP_CONTEXT_STATE_DROPPING:
      return "dropping";
    case HTTP_CONTEXT_STATE_CLOSED:
      return "closed";
    default:
      return "unknown";
  }
}

void httpNotifyContextClose(HttpContext *pContext) { 
  shutdown(pContext->fd, SHUT_WR); 
}

bool httpAlterContextState(HttpContext *pContext, HttpContextState srcState, HttpContextState destState) {
  return (atomic_val_compare_exchange_32(&pContext->state, srcState, destState) == srcState);
}

HttpContext *httpCreateContext(int32_t fd) {
  HttpContext *pContext = calloc(1, sizeof(HttpContext));
  if (pContext == NULL) return NULL;

  pContext->fd = fd;
  pContext->httpVersion = HTTP_VERSION_10;
  pContext->lastAccessTime = taosGetTimestampSec();
  pContext->state = HTTP_CONTEXT_STATE_READY;

  uint64_t handleVal = (uint64_t)pContext;
  HttpContext **ppContext = taosCachePut(tsHttpServer.contextCache, &handleVal, sizeof(int64_t), &pContext, sizeof(int64_t), 3000);
  pContext->ppContext = ppContext;
  httpDebug("context:%p, fd:%d, is created, data:%p", pContext, fd, ppContext);

  // set the ref to 0 
  taosCacheRelease(tsHttpServer.contextCache, (void**)&ppContext, false);

  return pContext;
}

HttpContext *httpGetContext(void *ptr) {
  uint64_t handleVal = (uint64_t)ptr;
  HttpContext **ppContext = taosCacheAcquireByKey(tsHttpServer.contextCache, &handleVal, sizeof(HttpContext *));
  EQ_ASSERT(ppContext);
  EQ_ASSERT(*ppContext);

  if (ppContext) {
    HttpContext *pContext = *ppContext;
    if (pContext) {
      int32_t refCount = atomic_add_fetch_32(&pContext->refCount, 1);
      httpDebug("context:%p, fd:%d, is accquired, data:%p refCount:%d", pContext, pContext->fd, ppContext, refCount);
      return pContext;
    }
  }
  return NULL;
}

void httpReleaseContext(HttpContext *pContext) {
  int32_t refCount = atomic_sub_fetch_32(&pContext->refCount, 1);
  if (refCount < 0) {
    httpError("context:%p, is already released, refCount:%d", pContext, refCount);
    return;
  }

  HttpContext **ppContext = pContext->ppContext;
  httpDebug("context:%p, is released, data:%p refCount:%d", pContext, ppContext, refCount);

  if (tsHttpServer.contextCache != NULL) {
    taosCacheRelease(tsHttpServer.contextCache, (void **)(&ppContext), false);
  } else {
    httpDebug("context:%p, won't be destroyed for cache is already released", pContext);
    // httpDestroyContext((void **)(&ppContext));
  }
}

bool httpInitContext(HttpContext *pContext) {
  pContext->accessTimes++;
  pContext->lastAccessTime = taosGetTimestampSec();
  pContext->httpVersion = HTTP_VERSION_10;
  pContext->httpKeepAlive = HTTP_KEEPALIVE_NO_INPUT;
  pContext->httpChunked = HTTP_UNCUNKED;
  pContext->acceptEncoding = HTTP_COMPRESS_IDENTITY;
  pContext->contentEncoding = HTTP_COMPRESS_IDENTITY;
  pContext->reqType = HTTP_REQTYPE_OTHERS;
  pContext->encodeMethod = NULL;
  pContext->timer = NULL;
  memset(&pContext->singleCmd, 0, sizeof(HttpSqlCmd));

  HttpParser *pParser = &pContext->parser;
  memset(pParser, 0, sizeof(HttpParser));
  pParser->pCur = pParser->pLast = pParser->buffer;

  HttpParserCallbackObj callbacks = {
    httpParseOnRequestLine,
    httpParseOnStatusLine,
    httpParseOnHeaderField,
    httpParseOnBody,
    httpParseOnEnd,
    httpParseOnError
  };
  HttpParserConfObj conf = {
    .flush_block_size = 0
  };
  pParser->parser = httpParserCreate(callbacks, conf, pContext);
  pParser->inited = 1;

  httpDebug("context:%p, fd:%d, parsed:%d", pContext, pContext->fd, pContext->parsed);
  return true;
}

void httpCloseContextByApp(HttpContext *pContext) {
  pContext->parsed = false;
  bool keepAlive = true;

  if (pContext->httpVersion == HTTP_VERSION_10 && pContext->httpKeepAlive != HTTP_KEEPALIVE_ENABLE) {
    keepAlive = false;
  } else if (pContext->httpVersion != HTTP_VERSION_10 && pContext->httpKeepAlive == HTTP_KEEPALIVE_DISABLE) {
    keepAlive = false;
  } else {
  }

  if (keepAlive) {
    if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_HANDLING, HTTP_CONTEXT_STATE_READY)) {
      httpDebug("context:%p, fd:%d, last state:handling, keepAlive:true, reuse context", pContext, pContext->fd);
    } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_DROPPING, HTTP_CONTEXT_STATE_CLOSED)) {
      httpRemoveContextFromEpoll(pContext);
      httpDebug("context:%p, fd:%d, ast state:dropping, keepAlive:true, close connect", pContext, pContext->fd);
    } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_READY, HTTP_CONTEXT_STATE_READY)) {
      httpDebug("context:%p, fd:%d, last state:ready, keepAlive:true, reuse context", pContext, pContext->fd);
    } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_CLOSED, HTTP_CONTEXT_STATE_CLOSED)) {
      httpRemoveContextFromEpoll(pContext);
      httpDebug("context:%p, fd:%d, last state:ready, keepAlive:true, close connect", pContext, pContext->fd);
    } else {
      httpRemoveContextFromEpoll(pContext);
      httpError("context:%p, fd:%d, last state:%s:%d, keepAlive:true, close connect", pContext, pContext->fd,
                httpContextStateStr(pContext->state), pContext->state);
    }
  } else {
    httpRemoveContextFromEpoll(pContext);
    httpDebug("context:%p, fd:%d, ilast state:%s:%d, keepAlive:false, close context", pContext, pContext->fd,
              httpContextStateStr(pContext->state), pContext->state);
  }

  httpReleaseContext(pContext);
}

void httpCloseContextByServer(HttpContext *pContext) {
  if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_HANDLING, HTTP_CONTEXT_STATE_DROPPING)) {
    httpDebug("context:%p, fd:%d, epoll finished, still used by app", pContext, pContext->fd);
  } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_DROPPING, HTTP_CONTEXT_STATE_DROPPING)) {
    httpDebug("context:%p, fd:%d, epoll already finished, wait app finished", pContext, pContext->fd);
  } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_READY, HTTP_CONTEXT_STATE_CLOSED)) {
    httpDebug("context:%p, fd:%d, epoll finished, close connect", pContext, pContext->fd);
  } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_CLOSED, HTTP_CONTEXT_STATE_CLOSED)) {
    httpDebug("context:%p, fd:%d, epoll finished, will be closed soon", pContext, pContext->fd);
  } else {
    httpError("context:%p, fd:%d, unknown state:%d", pContext, pContext->fd, pContext->state);
  }

  pContext->parsed = false;
  httpRemoveContextFromEpoll(pContext);
}

static void httpParseOnRequestLine(void *arg, const char *method, const char *target, const char *version, const char *target_raw) {
  HttpContext *pContext = (HttpContext*)arg;
  HttpParser  *pParser  = &pContext->parser;

  int avail = sizeof(pParser->buffer) - (pParser->pLast - pParser->buffer);
  int n = snprintf(pParser->pLast, avail, "%s %s %s\r\n", method, target_raw, version);
  char *last = pParser->pLast;

  do {
    if (n >= avail) {
      httpDebug("context:%p, fd:%d, request line(%s,%s,%s,%s), exceeding buffer size", pContext, pContext->fd, method,
                target, version, target_raw);
      break;
    }
    pParser->bufsize += n;

    if (!httpGetHttpMethod(pContext)) {
      httpDebug("context:%p, fd:%d, request line(%s,%s,%s,%s), parse http method failed", pContext, pContext->fd,
                method, target, version, target_raw);
      break;
    }
    if (!httpParseURL(pContext)) {
      httpDebug("context:%p, fd:%d, request line(%s,%s,%s,%s), parse http url failed", pContext, pContext->fd, method,
                target, version, target_raw);
      break;
    }
    if (!httpParseHttpVersion(pContext)) {
      httpDebug("context:%p, fd:%d, request line(%s,%s,%s,%s), parse http version failed", pContext, pContext->fd,
                method, target, version, target_raw);
      break;
    }
    if (!httpGetDecodeMethod(pContext)) {
      httpDebug("context:%p, fd:%d, request line(%s,%s,%s,%s), get decode method failed", pContext, pContext->fd,
                method, target, version, target_raw);
      break;
    }

    last += n;
    pParser->pLast = last;
    return;
  } while (0);

  pParser->failed |= EHTTP_CONTEXT_PROCESS_FAILED;
}

static void httpParseOnStatusLine(void *arg, const char *version, int status_code, const char *reason_phrase) {
  HttpContext *pContext = (HttpContext*)arg;
  HttpParser  *pParser  = &pContext->parser;

  httpDebug("context:%p, fd:%d, failed to parse status line ", pContext, pContext->fd);
  pParser->failed |= EHTTP_CONTEXT_PROCESS_FAILED;
}

static void httpParseOnHeaderField(void *arg, const char *key, const char *val) {
  HttpContext *pContext = (HttpContext*)arg;
  HttpParser  *pParser  = &pContext->parser;

  if (pParser->failed) return;

  httpDebug("context:%p, fd:%d, key:%s val:%s", pContext, pContext->fd, key, val);
  int   avail = sizeof(pParser->buffer) - (pParser->pLast - pParser->buffer);
  int   n = snprintf(pParser->pLast, avail, "%s: %s\r\n", key, val);
  char *last = pParser->pLast;

  do {
    if (n >= avail) {
      httpDebug("context:%p, fd:%d, header field(%s,%s), exceeding buffer size", pContext, pContext->fd, key, val);
      break;
    }
    pParser->bufsize += n;
    pParser->pCur = pParser->pLast + n;

    if (!httpParseHead(pContext)) {
      httpDebug("context:%p, fd:%d, header field(%s,%s), parse failed", pContext, pContext->fd, key, val);
      break;
    }

    last += n;
    pParser->pLast = last;
    return;
  } while (0);

  pParser->failed |= EHTTP_CONTEXT_PROCESS_FAILED;
}

static void httpParseOnBody(void *arg, const char *chunk, size_t len) {
  HttpContext *pContext = (HttpContext*)arg;
  HttpParser  *pParser  = &pContext->parser;

  if (pParser->failed) return;

  if (pParser->data.pos == 0) {
    pParser->data.pos = pParser->pLast;
    pParser->data.len = 0;
  }

  int avail = sizeof(pParser->buffer) - (pParser->pLast - pParser->buffer);
  if (len + 1 >= avail) {
    httpError("context:%p, fd:%d, failed parse body, exceeding buffer size", pContext, pContext->fd);
    pParser->failed |= EHTTP_CONTEXT_PROCESS_FAILED;
    return;
  }

  memcpy(pParser->pLast, chunk, len);
  pParser->pLast    += len;
  pParser->data.len += len;
}

static void httpParseOnEnd(void *arg) {
  HttpContext *pContext = (HttpContext*)arg;
  HttpParser  *pParser  = &pContext->parser;

  if (pParser->failed) return;

  if (pParser->data.pos == 0) pParser->data.pos = pParser->pLast;

  if (!pContext->parsed) {
    pContext->parsed = true;
  }

  httpDebug("context:%p, fd:%d, parse success", pContext, pContext->fd);
}

static void httpParseOnError(void *arg, int status_code) {
  HttpContext *pContext = (HttpContext *)arg;
  HttpParser * pParser = &pContext->parser;

  httpError("context:%p, fd:%d, failed to parse, status_code:%d", pContext, pContext->fd, status_code);
  pParser->failed |= EHTTP_CONTEXT_PARSER_FAILED;
}
