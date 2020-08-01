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
#include "ttime.h"
#include "ttimer.h"
#include "tglobal.h"
#include "tcache.h"
#include "hash.h"
#include "httpInt.h"
#include "httpResp.h"
#include "httpSql.h"
#include "httpSession.h"

#include "elog.h"

// dirty tweak
extern bool httpGetHttpMethod(HttpContext* pContext);
extern bool httpParseURL(HttpContext* pContext);
extern bool httpParseHttpVersion(HttpContext* pContext);
extern bool httpGetDecodeMethod(HttpContext* pContext);
extern bool httpParseHead(HttpContext* pContext);

static void on_request_line(void *arg, const char *method, const char *target, const char *version, const char *target_raw);
static void on_status_line(void *arg, const char *version, int status_code, const char *reason_phrase);
static void on_header_field(void *arg, const char *key, const char *val);
static void on_body(void *arg, const char *chunk, size_t len);
static void on_end(void *arg);
static void on_error(void *arg, int status_code);

static void httpRemoveContextFromEpoll(HttpContext *pContext) {
  HttpThread *pThread = pContext->pThread;
  if (pContext->fd >= 0) {
    epoll_ctl(pThread->pollFd, EPOLL_CTL_DEL, pContext->fd, NULL);
    taosCloseSocket(pContext->fd);
    pContext->fd = -1;
  }
}

static void httpDestroyContext(void *data) {
  HttpContext *pContext = *(HttpContext **)data;
  if (pContext->fd > 0) tclose(pContext->fd);

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

  tfree(pContext);
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
    httpInfo("context cache is cleanuping, size:%zu", taosHashGetSize(cache->pHashTable));
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

  HttpContext **ppContext = taosCachePut(tsHttpServer.contextCache, &pContext, sizeof(int64_t), &pContext, sizeof(int64_t), 3);
  pContext->ppContext = ppContext;
  httpDebug("context:%p, fd:%d, is created, data:%p", pContext, fd, ppContext);

  // set the ref to 0 
  taosCacheRelease(tsHttpServer.contextCache, (void**)&ppContext, false);

  return pContext;
}

HttpContext *httpGetContext(void *ptr) {
  HttpContext **ppContext = taosCacheAcquireByKey(tsHttpServer.contextCache, &ptr, sizeof(HttpContext *));

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

  if (pContext->parser.parser) {
    ehttp_parser_destroy(pContext->parser.parser);
    pContext->parser.parser = NULL;
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

  ehttp_parser_callbacks_t callbacks = {
    on_request_line,
    on_status_line,
    on_header_field,
    on_body,
    on_end,
    on_error
  };
  ehttp_parser_conf_t conf = {
    .flush_block_size = 0
  };
  pParser->parser = ehttp_parser_create(callbacks, conf, pContext);
  pParser->inited = 1;

  httpDebug("context:%p, fd:%d, ip:%s, thread:%s, accessTimes:%d, parsed:%d",
          pContext, pContext->fd, pContext->ipstr, pContext->pThread->label, pContext->accessTimes, pContext->parsed);
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
      httpDebug("context:%p, fd:%d, ip:%s, last state:handling, keepAlive:true, reuse context", pContext, pContext->fd,
                pContext->ipstr);
    } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_DROPPING, HTTP_CONTEXT_STATE_CLOSED)) {
      httpRemoveContextFromEpoll(pContext);
      httpDebug("context:%p, fd:%d, ip:%s, last state:dropping, keepAlive:true, close connect", pContext, pContext->fd,
                pContext->ipstr);
    } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_READY, HTTP_CONTEXT_STATE_READY)) {
      httpDebug("context:%p, fd:%d, ip:%s, last state:ready, keepAlive:true, reuse context", pContext, pContext->fd,
                pContext->ipstr);
    } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_CLOSED, HTTP_CONTEXT_STATE_CLOSED)) {
      httpRemoveContextFromEpoll(pContext);
      httpDebug("context:%p, fd:%d, ip:%s, last state:ready, keepAlive:true, close connect", pContext, pContext->fd,
                pContext->ipstr);
    } else {
      httpRemoveContextFromEpoll(pContext);
      httpError("context:%p, fd:%d, ip:%s, last state:%s:%d, keepAlive:true, close connect", pContext, pContext->fd,
                pContext->ipstr, httpContextStateStr(pContext->state), pContext->state);
    }
  } else {
    httpRemoveContextFromEpoll(pContext);
    httpDebug("context:%p, fd:%d, ip:%s, last state:%s:%d, keepAlive:false, close context", pContext, pContext->fd,
              pContext->ipstr, httpContextStateStr(pContext->state), pContext->state);
  }

  httpReleaseContext(pContext);
}

void httpCloseContextByServer(HttpContext *pContext) {
  if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_HANDLING, HTTP_CONTEXT_STATE_DROPPING)) {
    httpDebug("context:%p, fd:%d, ip:%s, epoll finished, still used by app", pContext, pContext->fd, pContext->ipstr);
  } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_DROPPING, HTTP_CONTEXT_STATE_DROPPING)) {
    httpDebug("context:%p, fd:%d, ip:%s, epoll already finished, wait app finished", pContext, pContext->fd, pContext->ipstr);
  } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_READY, HTTP_CONTEXT_STATE_CLOSED)) {
    httpDebug("context:%p, fd:%d, ip:%s, epoll finished, close connect", pContext, pContext->fd, pContext->ipstr);
  } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_CLOSED, HTTP_CONTEXT_STATE_CLOSED)) {
    httpDebug("context:%p, fd:%d, ip:%s, epoll finished, will be closed soon", pContext, pContext->fd, pContext->ipstr);
  } else {
    httpError("context:%p, fd:%d, ip:%s, unknown state:%d", pContext, pContext->fd, pContext->ipstr, pContext->state);
  }

  pContext->parsed = false;
  httpRemoveContextFromEpoll(pContext);
  httpReleaseContext(pContext);
}





static void on_request_line(void *arg, const char *method, const char *target, const char *version, const char *target_raw) {
  HttpContext *pContext = (HttpContext*)arg;
  HttpParser  *pParser  = &pContext->parser;

  int avail = sizeof(pParser->buffer) - (pParser->pLast - pParser->buffer);
  int n = snprintf(pParser->pLast, avail,
                   "%s %s %s\r\n", method, target_raw, version);

  char *last = pParser->pLast;

  do {
    if (n>=avail) {
      httpDebug("context:%p, fd:%d, ip:%s, thread:%s, accessTimes:%d, on_request_line(%s,%s,%s,%s), exceeding buffer size",
              pContext, pContext->fd, pContext->ipstr, pContext->pThread->label, pContext->accessTimes, method, target, version, target_raw);
      break;
    }
    pParser->bufsize += n;

    if (!httpGetHttpMethod(pContext)) {
      httpDebug("context:%p, fd:%d, ip:%s, thread:%s, accessTimes:%d, on_request_line(%s,%s,%s,%s), parse http method failed",
              pContext, pContext->fd, pContext->ipstr, pContext->pThread->label, pContext->accessTimes, method, target, version, target_raw);
      break;
    }
    if (!httpParseURL(pContext)) {
      httpDebug("context:%p, fd:%d, ip:%s, thread:%s, accessTimes:%d, on_request_line(%s,%s,%s,%s), parse http url failed",
              pContext, pContext->fd, pContext->ipstr, pContext->pThread->label, pContext->accessTimes, method, target, version, target_raw);
      break;
    }
    if (!httpParseHttpVersion(pContext)) {
      httpDebug("context:%p, fd:%d, ip:%s, thread:%s, accessTimes:%d, on_request_line(%s,%s,%s,%s), parse http version failed",
              pContext, pContext->fd, pContext->ipstr, pContext->pThread->label, pContext->accessTimes, method, target, version, target_raw);
      break;
    }
    if (!httpGetDecodeMethod(pContext)) {
      httpDebug("context:%p, fd:%d, ip:%s, thread:%s, accessTimes:%d, on_request_line(%s,%s,%s,%s), get decode method failed",
              pContext, pContext->fd, pContext->ipstr, pContext->pThread->label, pContext->accessTimes, method, target, version, target_raw);
      break;
    }

    last              += n;
    pParser->pLast     = last;
    return;
  } while (0);

  pParser->failed |= EHTTP_CONTEXT_PROCESS_FAILED;
}

static void on_status_line(void *arg, const char *version, int status_code, const char *reason_phrase) {
  HttpContext *pContext = (HttpContext*)arg;
  HttpParser  *pParser  = &pContext->parser;

  pParser->failed |= EHTTP_CONTEXT_PROCESS_FAILED;
}

static void on_header_field(void *arg, const char *key, const char *val) {
  HttpContext *pContext = (HttpContext*)arg;
  HttpParser  *pParser  = &pContext->parser;

  if (pParser->failed) return;

  int avail = sizeof(pParser->buffer) - (pParser->pLast - pParser->buffer);
  int n = snprintf(pParser->pLast, avail,
                   "%s: %s\r\n", key, val);

  char *last = pParser->pLast;

  do {
    if (n>=avail) {
      httpDebug("context:%p, fd:%d, ip:%s, thread:%s, accessTimes:%d, on_header_field(%s,%s), exceeding buffer size",
              pContext, pContext->fd, pContext->ipstr, pContext->pThread->label, pContext->accessTimes, key, val);
      break;
    }
    pParser->bufsize += n;
    pParser->pCur     = pParser->pLast;

    if (!httpParseHead(pContext)) {
      httpDebug("context:%p, fd:%d, ip:%s, thread:%s, accessTimes:%d, on_header_field(%s,%s), parse head failed",
              pContext, pContext->fd, pContext->ipstr, pContext->pThread->label, pContext->accessTimes, key, val);
      break;
    }

    last              += n;
    pParser->pLast     = last;
    return;
  } while (0);

  pParser->failed |= EHTTP_CONTEXT_PROCESS_FAILED;
}

static void on_body(void *arg, const char *chunk, size_t len) {
  HttpContext *pContext = (HttpContext*)arg;
  HttpParser  *pParser  = &pContext->parser;

  if (pParser->failed) return;

  if (!pContext->parsed) {
    pContext->parsed = true;
  }

  A("not implemented yet");
}

static void on_end(void *arg) {
  HttpContext *pContext = (HttpContext*)arg;
  HttpParser  *pParser  = &pContext->parser;

  if (pParser->failed) return;

  if (!pContext->parsed) {
    pContext->parsed = true;
  }
}

static void on_error(void *arg, int status_code) {
  HttpContext *pContext = (HttpContext*)arg;
  HttpParser  *pParser  = &pContext->parser;

  D("==");
  pParser->failed |= EHTTP_CONTEXT_PARSER_FAILED;
}

