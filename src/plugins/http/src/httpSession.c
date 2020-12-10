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
#include "taoserror.h"
#include "tglobal.h"
#include "tcache.h"
#include "httpInt.h"
#include "httpContext.h"
#include "httpSession.h"

void httpCreateSession(HttpContext *pContext, void *taos) {
  HttpServer *server = &tsHttpServer;
  httpReleaseSession(pContext);

  pthread_mutex_lock(&server->serverMutex);

  HttpSession session;
  memset(&session, 0, sizeof(HttpSession));
  session.taos = taos;
  session.refCount = 1;
  int32_t len = snprintf(session.id, HTTP_SESSION_ID_LEN, "%s.%s", pContext->user, pContext->pass);

  pContext->session = taosCachePut(server->sessionCache, session.id, len, &session, sizeof(HttpSession), tsHttpSessionExpire * 1000);
  // void *temp = pContext->session;
  // taosCacheRelease(server->sessionCache, (void **)&temp, false);

  if (pContext->session == NULL) {
    httpError("context:%p, fd:%d, user:%s, error:%s", pContext, pContext->fd, pContext->user,
              tstrerror(TSDB_CODE_HTTP_SESSION_FULL));
    taos_close(taos);
    pthread_mutex_unlock(&server->serverMutex);
    return;
  }

  httpDebug("context:%p, fd:%d, user:%s, create a new session:%p:%p sessionRef:%d", pContext, pContext->fd,
            pContext->user, pContext->session, pContext->session->taos, pContext->session->refCount);
  pthread_mutex_unlock(&server->serverMutex);
}

static void httpFetchSessionImp(HttpContext *pContext) {
  HttpServer *server = &tsHttpServer;
  pthread_mutex_lock(&server->serverMutex);

  char sessionId[HTTP_SESSION_ID_LEN];
  int32_t len = snprintf(sessionId, HTTP_SESSION_ID_LEN, "%s.%s", pContext->user, pContext->pass);

  pContext->session = taosCacheAcquireByKey(server->sessionCache, sessionId, len);
  if (pContext->session != NULL) {
    atomic_add_fetch_32(&pContext->session->refCount, 1);
    httpDebug("context:%p, fd:%d, user:%s, find an exist session:%p:%p, sessionRef:%d", pContext, pContext->fd,
              pContext->user, pContext->session, pContext->session->taos, pContext->session->refCount);
  } else {
    httpDebug("context:%p, fd:%d, user:%s, session not found", pContext, pContext->fd, pContext->user);
  }

  pthread_mutex_unlock(&server->serverMutex);
}

void httpGetSession(HttpContext *pContext) {
  if (pContext->session == NULL) {
    httpFetchSessionImp(pContext);
  } else {
    char sessionId[HTTP_SESSION_ID_LEN];
    snprintf(sessionId, HTTP_SESSION_ID_LEN, "%s.%s", pContext->user, pContext->pass);
    httpReleaseSession(pContext);
    httpFetchSessionImp(pContext);
  }
}

void httpReleaseSession(HttpContext *pContext) {
  if (pContext == NULL || pContext->session == NULL) return;

  int32_t refCount = atomic_sub_fetch_32(&pContext->session->refCount, 1);
  assert(refCount >= 0);
  httpDebug("context:%p, release session:%p:%p, sessionRef:%d", pContext, pContext->session, pContext->session->taos,
            pContext->session->refCount);

  taosCacheRelease(tsHttpServer.sessionCache, (void **)&pContext->session, false);
  pContext->session = NULL;
}

static void httpDestroySession(void *data) {
  HttpSession *session = data;
  httpDebug("session:%p:%p, is destroyed, sessionRef:%d", session, session->taos, session->refCount);

  if (session->taos != NULL) {
    taos_close(session->taos);
    session->taos = NULL;
  }
}

void httpCleanUpSessions() {
  if (tsHttpServer.sessionCache != NULL) {
    SCacheObj *cache = tsHttpServer.sessionCache;
    httpInfo("session cache is cleanuping, size:%d", taosHashGetSize(cache->pHashTable));
    taosCacheCleanup(tsHttpServer.sessionCache);
    tsHttpServer.sessionCache = NULL;
  }
}

bool httpInitSessions() {
  tsHttpServer.sessionCache = taosCacheInit(TSDB_DATA_TYPE_BINARY, 5, false, httpDestroySession, "rests");
  if (tsHttpServer.sessionCache == NULL) {
    httpError("failed to init session cache");
    return false;
  }

  return true;
}
