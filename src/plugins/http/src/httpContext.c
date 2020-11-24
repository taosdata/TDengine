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
#include "httpParser.h"

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

  if (pContext->parser) {
    httpDestroyParser(pContext->parser);
    pContext->parser = NULL;
  }

  tfree(pContext);
}

bool httpInitContexts() {
  tsHttpServer.contextCache = taosCacheInit(TSDB_CACHE_PTR_KEY, 2, true, httpDestroyContext, "restc");
  if (tsHttpServer.contextCache < 0) {
    httpError("failed to init context cache");
    return false;
  }

  return true;
}

void httpCleanupContexts() {
  if (tsHttpServer.contextCache < 0) {
    httpInfo("context cache is cleanuping");
    taosCacheCleanup(tsHttpServer.contextCache);
    tsHttpServer.contextCache = -1;
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
  pContext->lastAccessTime = taosGetTimestampSec();
  pContext->state = HTTP_CONTEXT_STATE_READY;
  pContext->parser = httpCreateParser(pContext);

  TSDB_CACHE_PTR_TYPE handleVal = (TSDB_CACHE_PTR_TYPE)pContext;
  HttpContext **ppContext = taosCachePut(tsHttpServer.contextCache, &handleVal, sizeof(TSDB_CACHE_PTR_TYPE), &pContext,
                                         sizeof(TSDB_CACHE_PTR_TYPE), 3000);
  pContext->ppContext = ppContext;
  httpDebug("context:%p, fd:%d, is created, data:%p", pContext, fd, ppContext);

  // set the ref to 0 
  taosCacheRelease((void**)&ppContext);

  return pContext;
}

HttpContext *httpGetContext(void *ptr) {
  TSDB_CACHE_PTR_TYPE handleVal = (TSDB_CACHE_PTR_TYPE)ptr;
  HttpContext **ppContext = taosCacheAcquireByKey(tsHttpServer.contextCache, &handleVal, sizeof(TSDB_CACHE_PTR_TYPE));

  if (ppContext) {
    HttpContext *pContext = *ppContext;
    if (pContext) {
      int32_t refCount = atomic_add_fetch_32(&pContext->refCount, 1);
      httpTrace("context:%p, fd:%d, is accquired, data:%p refCount:%d", pContext, pContext->fd, ppContext, refCount);
      return pContext;
    }
  }
  return NULL;
}

void httpReleaseContext(HttpContext *pContext, bool clearRes) {
  int32_t refCount = atomic_sub_fetch_32(&pContext->refCount, 1);
  if (refCount < 0) {
    httpError("context:%p, is already released, refCount:%d", pContext, refCount);
    return;
  }

  if (clearRes) {
    httpClearParser(pContext->parser);
  }

  HttpContext **ppContext = pContext->ppContext;
  httpTrace("context:%p, is released, data:%p refCount:%d", pContext, ppContext, refCount);

  if (tsHttpServer.contextCache >= 0) {
    taosCacheRelease(ppContext);
    ppContext = NULL;
  } else {
    httpDebug("context:%p, won't be destroyed for cache is already released", pContext);
    // httpDestroyContext((void **)(&ppContext));
  }
}

bool httpInitContext(HttpContext *pContext) {
  pContext->accessTimes++;
  pContext->lastAccessTime = taosGetTimestampSec();

  pContext->reqType = HTTP_REQTYPE_OTHERS;
  pContext->encodeMethod = NULL;
  memset(&pContext->singleCmd, 0, sizeof(HttpSqlCmd));


  httpTrace("context:%p, fd:%d, parsed:%d", pContext, pContext->fd, pContext->parsed);
  return true;
}

void httpCloseContextByApp(HttpContext *pContext) {
  HttpParser *parser = pContext->parser;
  pContext->parsed = false;
  bool keepAlive = true;

  if (parser->httpVersion == HTTP_VERSION_10 && parser->keepAlive != HTTP_KEEPALIVE_ENABLE) {
    keepAlive = false;
  } else if (parser->httpVersion != HTTP_VERSION_10 && parser->keepAlive == HTTP_KEEPALIVE_DISABLE) {
    keepAlive = false;
  } else {
  }

  if (keepAlive) {
    if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_HANDLING, HTTP_CONTEXT_STATE_READY)) {
      httpTrace("context:%p, fd:%d, last state:handling, keepAlive:true, reuse context", pContext, pContext->fd);
    } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_DROPPING, HTTP_CONTEXT_STATE_CLOSED)) {
      httpRemoveContextFromEpoll(pContext);
      httpTrace("context:%p, fd:%d, ast state:dropping, keepAlive:true, close connect", pContext, pContext->fd);
    } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_READY, HTTP_CONTEXT_STATE_READY)) {
      httpTrace("context:%p, fd:%d, last state:ready, keepAlive:true, reuse context", pContext, pContext->fd);
    } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_CLOSED, HTTP_CONTEXT_STATE_CLOSED)) {
      httpRemoveContextFromEpoll(pContext);
      httpTrace("context:%p, fd:%d, last state:ready, keepAlive:true, close connect", pContext, pContext->fd);
    } else {
      httpRemoveContextFromEpoll(pContext);
      httpError("context:%p, fd:%d, last state:%s:%d, keepAlive:true, close connect", pContext, pContext->fd,
                httpContextStateStr(pContext->state), pContext->state);
    }
  } else {
    httpRemoveContextFromEpoll(pContext);
    httpTrace("context:%p, fd:%d, ilast state:%s:%d, keepAlive:false, close context", pContext, pContext->fd,
              httpContextStateStr(pContext->state), pContext->state);
  }

  httpReleaseContext(pContext, true);
}

void httpCloseContextByServer(HttpContext *pContext) {
  if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_HANDLING, HTTP_CONTEXT_STATE_DROPPING)) {
    httpTrace("context:%p, fd:%d, epoll finished, still used by app", pContext, pContext->fd);
  } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_DROPPING, HTTP_CONTEXT_STATE_DROPPING)) {
    httpTrace("context:%p, fd:%d, epoll already finished, wait app finished", pContext, pContext->fd);
  } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_READY, HTTP_CONTEXT_STATE_CLOSED)) {
    httpTrace("context:%p, fd:%d, epoll finished, close connect", pContext, pContext->fd);
  } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_CLOSED, HTTP_CONTEXT_STATE_CLOSED)) {
    httpTrace("context:%p, fd:%d, epoll finished, will be closed soon", pContext, pContext->fd);
  } else {
    httpError("context:%p, fd:%d, unknown state:%d", pContext, pContext->fd, pContext->state);
  }

  pContext->parsed = false;
  httpRemoveContextFromEpoll(pContext);
  httpReleaseContext(pContext, true);
}
