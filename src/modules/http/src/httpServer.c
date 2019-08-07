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
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <syslog.h>
#include <unistd.h>

#include "taosmsg.h"
#include "tlog.h"
#include "tlog.h"
#include "tsocket.h"
#include "tutil.h"
#include "ttime.h"
#include "ttimer.h"

#include "http.h"
#include "httpCode.h"
#include "httpHandle.h"
#include "httpResp.h"

#ifndef EPOLLWAKEUP
 #define EPOLLWAKEUP (1u << 29)
#endif

void httpFreeContext(HttpServer *pServer, HttpContext *pContext);

/**
 * context will be reused while connection exist
 * multiCmds and jsonBuf will be malloc after taos_query_a called
 * and won't be freed until connection closed
 */
HttpContext *httpCreateContext(HttpServer *pServer) {
  HttpContext *pContext = (HttpContext *)taosMemPoolMalloc(pServer->pContextPool);
  if (pContext != NULL) {
    pContext->fromMemPool = 1;
    httpTrace("context:%p, is malloced from mempool", pContext);
  } else {
    pContext = (HttpContext *)malloc(sizeof(HttpContext));
    if (pContext == NULL) {
      return NULL;
    } else {
      memset(pContext, 0, sizeof(HttpContext));
    }
    httpTrace("context:%p, is malloced from raw memory", pContext);
  }

  pContext->signature = pContext;
  pContext->httpVersion = HTTP_VERSION_10;
  pContext->lastAccessTime = taosGetTimestampSec();
  if (pthread_mutex_init(&(pContext->mutex), NULL) < 0) {
    httpFreeContext(pServer, pContext);
    return NULL;
  }

  return pContext;
}

void httpFreeContext(HttpServer *pServer, HttpContext *pContext) {
  pthread_mutex_unlock(&pContext->mutex);
  pthread_mutex_destroy(&pContext->mutex);

  if (pContext->fromMemPool) {
    httpTrace("context:%p, is freed from mempool", pContext);
    taosMemPoolFree(pServer->pContextPool, (char *)pContext);
  } else {
    httpTrace("context:%p, is freed from raw memory", pContext);
    tfree(pContext);
  }
}

void httpCleanUpContextTimer(HttpContext *pContext) {
  if (pContext->readTimer != NULL) {
    taosTmrStopA(&pContext->readTimer);
    pContext->readTimer = NULL;
    httpTrace("context:%p, fd:%d, ip:%s, close readTimer:%p", pContext, pContext->fd, pContext->ipstr, pContext->readTimer);
  }
}

void httpCleanUpContext(HttpThread *pThread, HttpContext *pContext) {
  // for not keep-alive
  httpCleanUpContextTimer(pContext);

  if (pContext->fd >= 0) {
    epoll_ctl(pThread->pollFd, EPOLL_CTL_DEL, pContext->fd, NULL);
    taosCloseSocket(pContext->fd);
    pContext->fd = -1;
  }

  httpRestoreSession(pContext);

  pthread_mutex_lock(&pThread->threadMutex);

  pThread->numOfFds--;
  httpTrace("context:%p, ip:%s, thread:%s, numOfFds:%d, fd is cleaned up", pContext, pContext->ipstr, pThread->label, pThread->numOfFds);
  if (pThread->numOfFds < 0) {
    httpError("context:%p, ip:%s, thread:%s, number of FDs:%d shall never be negative",
              pContext, pContext->ipstr, pThread->label, pThread->numOfFds);
    pThread->numOfFds = 0;
  }

  // remove from the link list
  if (pContext->prev) {
    (pContext->prev)->next = pContext->next;
  } else {
    pThread->pHead = pContext->next;
  }

  if (pContext->next) {
    (pContext->next)->prev = pContext->prev;
  }

  pthread_mutex_unlock(&pThread->threadMutex);

  httpTrace("context:%p, ip:%s, thread:%s, numOfFds:%d, context is cleaned up", pContext, pContext->ipstr,
            pThread->label, pThread->numOfFds);

  pContext->signature = 0;
  pContext->fd = -1;
  pContext->pThread = 0;
  pContext->prev = 0;
  pContext->next = 0;

  // avoid double free
  httpFreeJsonBuf(pContext);
  httpFreeMultiCmds(pContext);
  httpFreeContext(pThread->pServer, pContext);
}

bool httpInitContext(HttpContext *pContext) {
  pContext->accessTimes++;
  pContext->lastAccessTime = taosGetTimestampSec();
  pContext->httpVersion = HTTP_VERSION_10;
  pContext->httpKeepAlive = HTTP_KEEPALIVE_NO_INPUT;
  pContext->httpChunked = HTTP_UNCUNKED;
  pContext->acceptEncoding = HTTP_COMPRESS_IDENTITY;
  pContext->contentEncoding = HTTP_COMPRESS_IDENTITY;
  pContext->usedByEpoll = 1;
  pContext->usedByApp = 0;
  pContext->reqType = HTTP_REQTYPE_OTHERS;
  pContext->encodeMethod = NULL;
  pContext->readTimer = NULL;
  memset(&pContext->singleCmd, 0, sizeof(HttpSqlCmd));

  HttpParser *pParser = &pContext->parser;
  memset(pParser, 0, sizeof(HttpParser));
  pParser->pCur = pParser->pLast = pParser->buffer;

  httpTrace("context:%p, fd:%d, ip:%s, thread:%s, accessTimes:%d, parsed:%d",
          pContext, pContext->fd, pContext->ipstr, pContext->pThread->label, pContext->accessTimes, pContext->parsed);
  return true;
}

void httpCloseContextByApp(HttpContext *pContext) {
  HttpThread *pThread = pContext->pThread;
  if (pContext->signature != pContext || pContext->pThread != pThread) {
    return;
  }

  pthread_mutex_lock(&pContext->mutex);
  pContext->parsed = false;

  httpTrace("context:%p, fd:%d, ip:%s, app use finished, usedByEpoll:%d, usedByApp:%d, httpVersion:1.%d, keepAlive:%d",
            pContext, pContext->fd, pContext->ipstr, pContext->usedByEpoll, pContext->usedByApp, pContext->httpVersion,
            pContext->httpKeepAlive);

  if (!pContext->usedByEpoll) {
    httpCleanUpContext(pThread, pContext);
  } else {
    if (pContext->httpVersion == HTTP_VERSION_10 && pContext->httpKeepAlive != HTTP_KEEPALIVE_ENABLE) {
      httpCleanUpContext(pThread, pContext);
    } else if (pContext->httpVersion != HTTP_VERSION_10 && pContext->httpKeepAlive == HTTP_KEEPALIVE_DISABLE) {
      httpCleanUpContext(pThread, pContext);
    } else {
      pContext->usedByApp = 0;
      pthread_mutex_unlock(&pContext->mutex);
    }
  }
}

void httpCloseContextByServer(HttpThread *pThread, HttpContext *pContext) {
  if (pContext->signature != pContext || pContext->pThread != pThread) {
    return;
  }
  pthread_mutex_lock(&pContext->mutex);
  pContext->usedByEpoll = 0;
  pContext->parsed = false;

  httpTrace("context:%p, fd:%d, ip:%s, epoll use finished, usedByEpoll:%d, usedByApp:%d",
            pContext, pContext->fd, pContext->ipstr, pContext->usedByEpoll, pContext->usedByApp);

  if (pContext->fd >= 0) {
    epoll_ctl(pThread->pollFd, EPOLL_CTL_DEL, pContext->fd, NULL);
    taosCloseSocket(pContext->fd);
    pContext->fd = -1;
  }

  if (!pContext->usedByApp) {
    httpCleanUpContext(pThread, pContext);
  } else {
    pthread_mutex_unlock(&pContext->mutex);
  }
}

void httpCloseContextByServerFromTimer(void *param, void *tmrId) {
  HttpContext *pContext = (HttpContext *)param;
  httpError("context:%p, fd:%d, ip:%s, read http body error, time expired, readTimer:%p", pContext, pContext->fd, pContext->ipstr, tmrId);
  httpSendErrorResp(pContext, HTTP_PARSE_BODY_ERROR);
  httpCloseContextByServer(pContext->pThread, pContext);
}

void httpCleanUpConnect(HttpServer *pServer) {
  int         i;
  HttpThread *pThread;

  if (pServer == NULL) return;

  pthread_cancel(pServer->thread);
  pthread_join(pServer->thread, NULL);

  for (i = 0; i < pServer->numOfThreads; ++i) {
    pThread = pServer->pThreads + i;
    taosCloseSocket(pThread->pollFd);

    while (pThread->pHead) {
      httpCleanUpContext(pThread, pThread->pHead);
    }

    pthread_cancel(pThread->thread);
    pthread_join(pThread->thread, NULL);
    pthread_cond_destroy(&(pThread->fdReady));
    pthread_mutex_destroy(&(pThread->threadMutex));
  }

  tfree(pServer->pThreads);
  httpTrace("http server:%s is cleaned up", pServer->label);
}

void httpCloseDeadConnects(HttpThread *pThread) {
  int32_t thresholdSec = taosGetTimestampSec() - 3600;
  HttpContext *pContext = (HttpContext*)pThread->pHead;
  while (pContext != NULL && pContext == pContext->signature) {
    HttpContext *pContextNext = pContext->next;
    if (pContext->lastAccessTime < thresholdSec) {
      httpPrint("context:%p, fd:%d, ip:%s, lastAccessTime:%d smaller then threshold:%d, so close it",
              pContext, pContext->fd, pContext->ipstr, pContext->lastAccessTime, thresholdSec);
      httpCloseContextByServer(pThread, pContext);
    }
    pContext = pContextNext;
  }
}

// read all the data, then just discard it
void httpReadDirtyData(int fd) {
  char data[1024] = {0};
  int  len = (int)taosReadSocket(fd, data, 1024);
  while (len >= sizeof(data)) {
    len = (int)taosReadSocket(fd, data, 1024);
  }
}

bool httpReadDataImp(HttpContext *pContext) {
  HttpParser *pParser = &pContext->parser;

  int blocktimes = 0;
  while (pParser->bufsize <= (HTTP_BUFFER_SIZE - HTTP_STEP_SIZE)) {
    int nread = (int)taosReadSocket(pContext->fd, pParser->buffer + pParser->bufsize, HTTP_STEP_SIZE);
    if (nread >= 0 && nread < HTTP_STEP_SIZE) {
      pParser->bufsize += nread;
      break;
    } else if (nread < 0) {
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
        if (blocktimes++ > HTTP_READ_RETRY_TIMES) {
          taosMsleep(HTTP_READ_WAIT_TIME_MS);
          httpTrace("context:%p, fd:%d, ip:%s, read from socket error:%d, error times:%d",
                    pContext, pContext->fd, pContext->ipstr, errno, blocktimes);
          break;
        }
        continue;
      } else {
        httpError("context:%p, fd:%d, ip:%s, read from socket error:%d, close connect",
                  pContext, pContext->fd, pContext->ipstr, errno);
        return false;
      }
    } else {
      pParser->bufsize += nread;
    }

    if (pParser->bufsize >= (HTTP_BUFFER_SIZE - HTTP_STEP_SIZE)) {
      httpReadDirtyData(pContext->fd);
      httpError("context:%p, fd:%d, ip:%s, thread:%s, request big than:%d",
                pContext, pContext->fd, pContext->ipstr, pContext->pThread->label, HTTP_BUFFER_SIZE);
      httpSendErrorResp(pContext, HTTP_REQUSET_TOO_BIG);
      return false;
    }
  }

  pParser->buffer[pParser->bufsize] = 0;
  httpTrace("context:%p, fd:%d, ip:%s, thread:%s, read size:%d",
            pContext, pContext->fd, pContext->ipstr, pContext->pThread->label, pParser->bufsize);

  return true;
}

bool httpUnCompressData(HttpContext *pContext) {
  if (pContext->contentEncoding == HTTP_COMPRESS_GZIP) {
    char   *decompressBuf = calloc(HTTP_DECOMPRESS_BUF_SIZE, 1);
    int32_t decompressBufLen = pContext->parser.bufsize;

    int ret = httpGzipDeCompress(pContext->parser.data.pos, pContext->parser.data.len, decompressBuf, &decompressBufLen);

    if (ret == 0) {
      memcpy(pContext->parser.data.pos, decompressBuf, decompressBufLen);
      pContext->parser.data.pos[decompressBufLen] = 0;
      httpDump("context:%p, fd:%d, ip:%s, rawSize:%d, decompressSize:%d, content:%s",
                pContext, pContext->fd, pContext->ipstr, pContext->parser.data.len, decompressBufLen,  decompressBuf);
    } else {
      httpError("context:%p, fd:%d, ip:%s, failed to decompress data, rawSize:%d, error:%d",
                pContext, pContext->fd, pContext->ipstr, pContext->parser.data.len, ret);
      return false;
    }
  } else {
    httpDump("context:%p, fd:%d, ip:%s, content:%s", pContext, pContext->fd, pContext->ipstr, pContext->parser.data.pos);
  }

  return true;
}

bool httpReadData(HttpThread *pThread, HttpContext *pContext) {
  if (!pContext->parsed) {
    httpInitContext(pContext);
  }

  if (!httpReadDataImp(pContext)) {
    httpTrace("context:%p, fd:%d, ip:%s, read data error, close connect", pContext, pContext->fd, pContext->ipstr);
    httpCloseContextByServer(pThread, pContext);
    return false;
  }

  if (!httpParseRequest(pContext)) {
    httpTrace("context:%p, fd:%d, ip:%s, failed to parse http head, close connect", pContext, pContext->fd, pContext->ipstr);
    httpCloseContextByServer(pThread, pContext);
    return false;
  }

  int ret = httpCheckReadCompleted(pContext);
  if (ret == HTTP_CHECK_BODY_CONTINUE) {
    taosTmrReset(httpCloseContextByServerFromTimer, HTTP_EXPIRED_TIME, pContext, pThread->pServer->timerHandle, &pContext->readTimer);
    httpTrace("context:%p, fd:%d, ip:%s, not finished yet, try another times, readTimer:%p", pContext, pContext->fd, pContext->ipstr, pContext->readTimer);
    return false;
  } else if (ret == HTTP_CHECK_BODY_SUCCESS){
    httpCleanUpContextTimer(pContext);
    if (httpUnCompressData(pContext)) {
      return true;
    } else {
      httpCloseContextByServer(pThread, pContext);
      return false;
    }
  } else {
    httpCleanUpContextTimer(pContext);
    httpError("context:%p, fd:%d, ip:%s, failed to read http body, close connect", pContext, pContext->fd, pContext->ipstr);
    httpCloseContextByServer(pThread, pContext);
    return false;
  }
}

void httpProcessHttpData(void *param) {
  HttpThread * pThread = (HttpThread *)param;
  HttpContext *pContext;
  int          fdNum;

  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGPIPE);
  pthread_sigmask(SIG_SETMASK, &set, NULL);

  while (1) {
    pthread_mutex_lock(&pThread->threadMutex);
    if (pThread->numOfFds < 1) {
      pthread_cond_wait(&pThread->fdReady, &pThread->threadMutex);
    }
    pthread_mutex_unlock(&pThread->threadMutex);

    struct epoll_event events[HTTP_MAX_EVENTS];
    //-1 means uncertainty, 0-nowait, 1-wait 1 ms, set it from -1 to 1
    fdNum = epoll_wait(pThread->pollFd, events, HTTP_MAX_EVENTS, 1);
    if (fdNum <= 0) continue;

    for (int i = 0; i < fdNum; ++i) {
      pContext = events[i].data.ptr;
      if (pContext->signature != pContext || pContext->pThread != pThread || pContext->fd <= 0) {
        continue;
      }

      if (events[i].events & EPOLLPRI) {
        httpTrace("context:%p, fd:%d, ip:%s, EPOLLPRI events occured, close connect", pContext, pContext->fd,
                  pContext->ipstr);
        httpCloseContextByServer(pThread, pContext);
        continue;
      }

      if (events[i].events & EPOLLRDHUP) {
        httpTrace("context:%p, fd:%d, ip:%s, EPOLLRDHUP events occured, close connect",
                  pContext, pContext->fd, pContext->ipstr);
        httpCloseContextByServer(pThread, pContext);
        continue;
      }

      if (events[i].events & EPOLLERR) {
        httpTrace("context:%p, fd:%d, ip:%s, EPOLLERR events occured, close connect", pContext, pContext->fd,
                  pContext->ipstr);
        httpCloseContextByServer(pThread, pContext);
        continue;
      }

      if (events[i].events & EPOLLHUP) {
        httpTrace("context:%p, fd:%d, ip:%s, EPOLLHUP events occured, close connect", pContext, pContext->fd,
                  pContext->ipstr);
        httpCloseContextByServer(pThread, pContext);
        continue;
      }

      if (pContext->usedByApp) {
        httpTrace("context:%p, fd:%d, ip:%s, still used by app, accessTimes:%d, try again",
                  pContext, pContext->fd, pContext->ipstr, pContext->accessTimes);
        continue;
      }

      if (!httpReadData(pThread, pContext)) {
        continue;
      }

      if (!pContext->pThread->pServer->online) {
        httpSendErrorResp(pContext, HTTP_SERVER_OFFLINE);
        httpTrace("context:%p, fd:%d, ip:%s, server is not online", pContext, pContext->fd, pContext->ipstr);
        httpCloseContextByServer(pThread, pContext);
        continue;
      }

      __sync_fetch_and_add(&pThread->pServer->requestNum, 1);

      if (!(*(pThread->processData))(pContext)) {
        httpError("context:%p, fd:%d, ip:%s, app force closed", pContext, pContext->fd, pContext->ipstr,
                  pContext->accessTimes);
        httpCloseContextByServer(pThread, pContext);
      }
    }
  }
}

void httpAcceptHttpConnection(void *arg) {
  int                connFd = -1;
  struct sockaddr_in clientAddr;
  int                sockFd;
  int                threadId = 0;
  const int          connThreshold = 2 * tsHttpCacheSessions / tsHttpMaxThreads;
  HttpThread *       pThread;
  HttpServer *       pServer;
  HttpContext *      pContext;

  pServer = (HttpServer *)arg;

  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGPIPE);
  pthread_sigmask(SIG_SETMASK, &set, NULL);

  sockFd = taosOpenTcpServerSocket(pServer->serverIp, pServer->serverPort);

  if (sockFd < 0) {
    httpError("http server:%s, failed to open http socket, ip:%s:%u", pServer->label, pServer->serverIp,
              pServer->serverPort);
    return;
  } else {
    httpPrint("http service init success at ip:%s:%u", pServer->serverIp, pServer->serverPort);
    pServer->online = true;
  }

  while (1) {
    socklen_t addrlen = sizeof(clientAddr);
    connFd = (int)accept(sockFd, (struct sockaddr *)&clientAddr, &addrlen);

    if (connFd < 3) {
      httpError("http server:%s, accept connect failure, errno:%d, reason:%s", pServer->label, errno, strerror(errno));
      continue;
    }

    taosKeepTcpAlive(connFd);
    taosSetNonblocking(connFd, 1);

    // pick up the thread to handle this connection
    pThread = pServer->pThreads + threadId;

    pContext = httpCreateContext(pServer);
    if (pContext == NULL) {
      httpError("fd:%d, ip:%s:%u, no enough resource to allocate http context", connFd, inet_ntoa(clientAddr.sin_addr),
                htons(clientAddr.sin_port));
      taosCloseSocket(connFd);
      continue;
    }

    httpTrace("context:%p, fd:%d, ip:%s:%u, thread:%s, accept a new connection", pContext, connFd,
              inet_ntoa(clientAddr.sin_addr), htons(clientAddr.sin_port), pThread->label);

    pContext->fd = connFd;
    sprintf(pContext->ipstr, "%s:%d", inet_ntoa(clientAddr.sin_addr), htons(clientAddr.sin_port));
    pContext->pThread = pThread;

    struct epoll_event event;
    event.events = EPOLLIN | EPOLLPRI | EPOLLWAKEUP | EPOLLERR | EPOLLHUP | EPOLLRDHUP;

    event.data.ptr = pContext;
    if (epoll_ctl(pThread->pollFd, EPOLL_CTL_ADD, connFd, &event) < 0) {
      httpError("context:%p, fd:%d, ip:%s:%u, thread:%s, failed to add http fd for epoll, error:%s",
                pContext, connFd, inet_ntoa(clientAddr.sin_addr), htons(clientAddr.sin_port), pThread->label,
                strerror(errno));
      httpFreeContext(pThread->pServer, pContext);
      tclose(connFd);
      continue;
    }

    // notify the data process, add into the FdObj list
    pthread_mutex_lock(&(pThread->threadMutex));

    pContext->next = pThread->pHead;

    if (pThread->pHead) (pThread->pHead)->prev = pContext;

    pThread->pHead = pContext;

    pThread->numOfFds++;
    pthread_cond_signal(&pThread->fdReady);

    pthread_mutex_unlock(&(pThread->threadMutex));

    httpTrace("context:%p, fd:%d, ip:%s:%u, thread:%s, numOfFds:%d, begin read request",
              pContext, connFd, inet_ntoa(clientAddr.sin_addr), htons(clientAddr.sin_port), pThread->label,
              pThread->numOfFds);

    if (pThread->numOfFds > connThreshold) {
      httpCloseDeadConnects(pThread);
    }
    // pick up next thread for next connection
    threadId++;
    threadId = threadId % pServer->numOfThreads;
  }
}

bool httpInitConnect(HttpServer *pServer) {
  int            i;
  pthread_attr_t thattr;
  HttpThread *   pThread;

  pServer->pThreads = (HttpThread *)malloc(sizeof(HttpThread) * (size_t)pServer->numOfThreads);
  if (pServer->pThreads == NULL) {
    httpError("init error no enough memory");
    return false;
  }
  memset(pServer->pThreads, 0, sizeof(HttpThread) * (size_t)pServer->numOfThreads);

  pThread = pServer->pThreads;
  for (i = 0; i < pServer->numOfThreads; ++i) {
    sprintf(pThread->label, "%s%d", pServer->label, i);
    pThread->pServer = pServer;
    pThread->processData = pServer->processData;
    pThread->threadId = i;

    if (pthread_mutex_init(&(pThread->threadMutex), NULL) < 0) {
      httpError("http thread:%s, failed to init HTTP process data mutex, reason:%s", pThread->label, strerror(errno));
      return false;
    }

    if (pthread_cond_init(&(pThread->fdReady), NULL) != 0) {
      httpError("http thread:%s, init HTTP condition variable failed, reason:%s", pThread->label, strerror(errno));
      return false;
    }

    pThread->pollFd = epoll_create(HTTP_MAX_EVENTS);  // size does not matter
    if (pThread->pollFd < 0) {
      httpError("http thread:%s, failed to create HTTP epoll", pThread->label);
      return false;
    }

    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
    if (pthread_create(&(pThread->thread), &thattr, (void *)httpProcessHttpData, (void *)(pThread)) != 0) {
      httpError("http thread:%s, failed to create HTTP process data thread, reason:%s",
                pThread->label, strerror(errno));
      return false;
    }

    httpTrace("http thread:%p:%s, initialized", pThread, pThread->label);
    pThread++;
  }

  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  if (pthread_create(&(pServer->thread), &thattr, (void *)httpAcceptHttpConnection, (void *)(pServer)) != 0) {
    httpError("http server:%s, failed to create Http accept thread, reason:%s", pServer->label, strerror(errno));
    return false;
  }

  pthread_attr_destroy(&thattr);

  httpTrace("http server:%s, initialized, ip:%s:%u, numOfThreads:%d", pServer->label, pServer->serverIp,
            pServer->serverPort, pServer->numOfThreads);
  return true;
}
