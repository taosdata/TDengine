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

#include "gcHandle.h"
#include "httpHandle.h"
#include "restHandle.h"
#include "tgHandle.h"

static HttpServer *httpServer = NULL;

int httpInitSystem() {
  taos_init();

  httpServer = (HttpServer *)malloc(sizeof(HttpServer));
  memset(httpServer, 0, sizeof(HttpServer));

  strcpy(httpServer->label, "taosh");
  strcpy(httpServer->serverIp, tsHttpIp);
  httpServer->serverPort = tsHttpPort;
  httpServer->cacheContext = tsHttpCacheSessions;
  httpServer->sessionExpire = tsHttpSessionExpire;
  httpServer->numOfThreads = tsHttpMaxThreads;
  httpServer->processData = httpProcessData;

  pthread_mutex_init(&httpServer->serverMutex, NULL);

  restInitHandle(httpServer);
  gcInitHandle(httpServer);
  tgInitHandle(httpServer);

  return 0;
}

int httpStartSystem() {
  httpPrint("starting to initialize http service ...");

  if (httpServer == NULL) {
    httpError("http server is null");
    return -1;
  }

  if (httpServer->pContextPool == NULL) {
    httpServer->pContextPool = taosMemPoolInit(httpServer->cacheContext, sizeof(HttpContext));
  }
  if (httpServer->pContextPool == NULL) {
    httpError("http init context pool failed");
    return -1;
  }

  if (httpServer->timerHandle == NULL) {
    httpServer->timerHandle = taosTmrInit(5, 1000, 60000, "http");
  }
  if (httpServer->timerHandle == NULL) {
    httpError("http init timer failed");
    return -1;
  }

  if (!httpInitAllSessions(httpServer)) {
    httpError("http init session failed");
    return -1;
  }

  if (!httpInitConnect(httpServer)) {
    httpError("http init server failed");
    return -1;
  }

  return 0;
}

void httpStopSystem() {
  if (httpServer != NULL) {
    httpServer->online = false;
  }
}

void httpCleanUpSystem() {
  httpPrint("http service cleanup");
  httpStopSystem();
#if 0
  if (httpServer == NULL) {
    return;
  }

  if (httpServer->expireTimer != NULL) {
    taosTmrStopA(&(httpServer->expireTimer));
  }

  if (httpServer->timerHandle != NULL) {
    taosTmrCleanUp(httpServer->timerHandle);
    httpServer->timerHandle = NULL;
  }

  httpCleanUpConnect(httpServer);
  httpRemoveAllSessions(httpServer);

  if (httpServer->pContextPool != NULL) {
    taosMemPoolCleanUp(httpServer->pContextPool);
    httpServer->pContextPool = NULL;
  }

  pthread_mutex_destroy(&httpServer->serverMutex);

  tfree(httpServer);
#endif
}

void httpGetReqCount(int32_t *httpReqestNum) {
  if (httpServer != NULL) {
    *httpReqestNum = __sync_fetch_and_and(&httpServer->requestNum, 0);
  } else {
    *httpReqestNum = 0;
  }
}
