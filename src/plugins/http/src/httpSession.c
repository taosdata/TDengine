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
#include "hash.h"
#include "taos.h"
#include "ttime.h"
#include "ttimer.h"
#include "http.h"
#include "httpLog.h"
#include "httpCode.h"
#include "httpHandle.h"
#include "httpResp.h"

void httpAccessSession(HttpContext *pContext) {
  HttpServer *server = pContext->pThread->pServer;
  pthread_mutex_lock(&server->serverMutex);
  if (pContext->session == pContext->session->signature) {
    pContext->session->expire = (int) taosGetTimestampSec() + pContext->pThread->pServer->sessionExpire;
  }
  pthread_mutex_unlock(&server->serverMutex);
}

void httpCreateSession(HttpContext *pContext, void *taos) {
  HttpServer *server = pContext->pThread->pServer;
  pthread_mutex_lock(&server->serverMutex);

  if (pContext->session != NULL && pContext->session == pContext->session->signature) {
    httpTrace("context:%p, fd:%d, ip:%s, user:%s, set exist session:%p:%p expired", pContext, pContext->fd,
              pContext->ipstr, pContext->user, pContext->session, pContext->session->taos);
    pContext->session->expire = 0;
    pContext->session->access--;
  }

  HttpSession session;
  session.taos = taos;
  session.expire = (int)taosGetTimestampSec() + server->sessionExpire;
  session.access = 1;
  int sessionIdLen = snprintf(session.id, HTTP_SESSION_ID_LEN, "%s.%s", pContext->user, pContext->pass);

  taosHashPut(server->pSessionHash, session.id, sessionIdLen, (char *)(&session), sizeof(HttpSession));
  pContext->session = taosHashGet(server->pSessionHash, session.id, sessionIdLen);

  if (pContext->session == NULL) {
    httpError("context:%p, fd:%d, ip:%s, user:%s, error:%s", pContext, pContext->fd, pContext->ipstr, pContext->user,
              httpMsg[HTTP_SESSION_FULL]);
    taos_close(taos);
    pthread_mutex_unlock(&server->serverMutex);
    return;
  }

  pContext->session->signature = pContext->session;
  httpTrace("context:%p, fd:%d, ip:%s, user:%s, create a new session:%p:%p", pContext, pContext->fd, pContext->ipstr,
            pContext->user, pContext->session, pContext->session->taos);
  pthread_mutex_unlock(&server->serverMutex);
}

void httpFetchSessionImp(HttpContext *pContext) {
  HttpServer *server = pContext->pThread->pServer;
  pthread_mutex_lock(&server->serverMutex);

  char sessionId[HTTP_SESSION_ID_LEN];
  int sessonIdLen = snprintf(sessionId, HTTP_SESSION_ID_LEN, "%s.%s", pContext->user, pContext->pass);

  pContext->session = taosHashGet(server->pSessionHash, sessionId, sessonIdLen);
  if (pContext->session != NULL && pContext->session == pContext->session->signature) {
    pContext->session->access++;
    httpTrace("context:%p, fd:%d, ip:%s, user:%s, find an exist session:%p:%p, access:%d, expire:%d",
              pContext, pContext->fd, pContext->ipstr, pContext->user, pContext->session,
              pContext->session->taos, pContext->session->access, pContext->session->expire);
    pContext->session->expire = (int)taosGetTimestampSec() + server->sessionExpire;
  } else {
    httpTrace("context:%p, fd:%d, ip:%s, user:%s, session not found", pContext, pContext->fd, pContext->ipstr,
              pContext->user);
  }

  pthread_mutex_unlock(&server->serverMutex);
}

void httpFetchSession(HttpContext *pContext) {
  if (pContext->session == NULL) {
    httpFetchSessionImp(pContext);
  } else {
    char sessionId[HTTP_SESSION_ID_LEN];
    snprintf(sessionId, HTTP_SESSION_ID_LEN, "%s.%s", pContext->user, pContext->pass);
    if (strcmp(pContext->session->id, sessionId) != 0) {
      httpError("context:%p, fd:%d, ip:%s, user:%s, password may be changed", pContext, pContext->fd, pContext->ipstr, pContext->user);
      httpRestoreSession(pContext);
      httpFetchSessionImp(pContext);
    }
  }
}

void httpRestoreSession(HttpContext *pContext) {
  HttpServer * server = pContext->pThread->pServer;

  // all access to the session is via serverMutex
  pthread_mutex_lock(&server->serverMutex);
  HttpSession *session = pContext->session;
  if (session == NULL || session != session->signature) {
    pthread_mutex_unlock(&server->serverMutex);
    return;
  }
  session->access--;
  httpTrace("context:%p, ip:%s, user:%s, restore session:%p:%p, access:%d, expire:%d",
            pContext, pContext->ipstr, pContext->user, session, session->taos,
            session->access, pContext->session->expire);
  pContext->session = NULL;
  pthread_mutex_unlock(&server->serverMutex);
}

void httpResetSession(HttpSession *pSession) {
  httpTrace("close session:%p:%p", pSession, pSession->taos);
  if (pSession->taos != NULL) {
    taos_close(pSession->taos);
    pSession->taos = NULL;
  }
  pSession->signature = NULL;
}

void httpRemoveAllSessions(HttpServer *pServer) {
  SHashMutableIterator *pIter = taosHashCreateIter(pServer->pSessionHash);

  while (taosHashIterNext(pIter)) {
    HttpSession *pSession = taosHashIterGet(pIter);
    if (pSession == NULL) continue;
    httpResetSession(pSession);
  }

  taosHashDestroyIter(pIter);
}

bool httpInitAllSessions(HttpServer *pServer) {
  if (pServer->pSessionHash == NULL) {
    pServer->pSessionHash = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true);
  }
  if (pServer->pSessionHash == NULL) {
    httpError("http init session pool failed");
    return false;
  }
  if (pServer->expireTimer == NULL) {
    taosTmrReset(httpProcessSessionExpire, 50000, pServer, pServer->timerHandle, &pServer->expireTimer);
  }

  return true;
}

bool httpSessionExpired(HttpSession *pSession) {
  time_t cur = taosGetTimestampSec();

  if (pSession->taos != NULL) {
    if (pSession->expire > cur) {
      return false;  // un-expired, so return false
    }
    if (pSession->access > 0) {
      httpTrace("session:%p:%p is expired, but still access:%d", pSession, pSession->taos,
                pSession->access);
      return false;  // still used, so return false
    }
    httpTrace("need close session:%p:%p for it expired, cur:%ld, expire:%d, invertal:%ld",
              pSession, pSession->taos, cur, pSession->expire, cur - pSession->expire);
  }

  return true;
}

void httpRemoveExpireSessions(HttpServer *pServer) {  
  SHashMutableIterator *pIter = taosHashCreateIter(pServer->pSessionHash);

  while (taosHashIterNext(pIter)) {
    HttpSession *pSession = taosHashIterGet(pIter);
    if (pSession == NULL) continue;

    pthread_mutex_lock(&pServer->serverMutex);
    if (httpSessionExpired(pSession)) {
      httpResetSession(pSession);
      taosHashRemove(pServer->pSessionHash, pSession->id, strlen(pSession->id));
    }
    pthread_mutex_unlock(&pServer->serverMutex);
  }

  taosHashDestroyIter(pIter);
}

void httpProcessSessionExpire(void *handle, void *tmrId) {
  HttpServer *pServer = (HttpServer *)handle;
  httpRemoveExpireSessions(pServer);
  taosTmrReset(httpProcessSessionExpire, 60000, pServer, pServer->timerHandle, &pServer->expireTimer);
}
