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
#include "httpInt.h"
#include "httpContext.h"
#include "httpResp.h"
#include "httpUtil.h"

static bool httpReadData(HttpContext *pContext);

static void httpStopThread(HttpThread* pThread) {
  pThread->stop = true;

  // signal the thread to stop, try graceful method first,
  // and use pthread_cancel when failed
  struct epoll_event event = { .events = EPOLLIN };
  eventfd_t fd = eventfd(1, 0);
  if (fd == -1) {
    httpError("%s, failed to create eventfd, will call pthread_cancel instead, which may result in data corruption: %s", pThread->label, strerror(errno));
    pThread->stop = true;
    pthread_cancel(pThread->thread);
  } else if (epoll_ctl(pThread->pollFd, EPOLL_CTL_ADD, fd, &event) < 0) {
    httpError("%s, failed to call epoll_ctl, will call pthread_cancel instead, which may result in data corruption: %s", pThread->label, strerror(errno));
    pthread_cancel(pThread->thread);
  }

  pthread_join(pThread->thread, NULL);
  if (fd != -1) {
    taosCloseSocket(fd);
  }

  taosCloseSocket(pThread->pollFd);
  pthread_mutex_destroy(&(pThread->threadMutex));
}

void httpCleanUpConnect() {
  HttpServer *pServer = &tsHttpServer;
  if (pServer->pThreads == NULL) return;

  pthread_join(pServer->thread, NULL);
  for (int32_t i = 0; i < pServer->numOfThreads; ++i) {
    HttpThread* pThread = pServer->pThreads + i;
    if (pThread != NULL) {
      httpStopThread(pThread);
    }
  }

  httpDebug("http server:%s is cleaned up", pServer->label);
}

static void httpProcessHttpData(void *param) {
  HttpServer  *pServer = &tsHttpServer;
  HttpThread  *pThread = (HttpThread *)param;
  HttpContext *pContext;
  int32_t      fdNum;

  taosSetMaskSIGPIPE();

  while (1) {
    struct epoll_event events[HTTP_MAX_EVENTS];
    //-1 means uncertainty, 0-nowait, 1-wait 1 ms, set it from -1 to 1
    fdNum = epoll_wait(pThread->pollFd, events, HTTP_MAX_EVENTS, TAOS_EPOLL_WAIT_TIME);
    if (pThread->stop) {
      httpDebug("%p, http thread get stop event, exiting...", pThread);
      break;
    }
    if (fdNum <= 0) continue;

    for (int32_t i = 0; i < fdNum; ++i) {
      pContext = httpGetContext(events[i].data.ptr);
      if (pContext == NULL) {
        httpError("context:%p, is already released, close connect", events[i].data.ptr);
        //epoll_ctl(pThread->pollFd, EPOLL_CTL_DEL, events[i].data.fd, NULL);
        //taosClose(events[i].data.fd);
        continue;
      }

      if (events[i].events & EPOLLPRI) {
        httpDebug("context:%p, fd:%d, state:%s, EPOLLPRI events occured, accessed:%d, close connect", pContext,
                  pContext->fd, httpContextStateStr(pContext->state), pContext->accessTimes);
        httpCloseContextByServer(pContext);
        continue;
      }

      if (events[i].events & EPOLLRDHUP) {
        httpDebug("context:%p, fd:%d, state:%s, EPOLLRDHUP events occured, accessed:%d, close connect", pContext,
                  pContext->fd, httpContextStateStr(pContext->state), pContext->accessTimes);
        httpCloseContextByServer(pContext);
        continue;
      }

      if (events[i].events & EPOLLERR) {
        httpDebug("context:%p, fd:%d, state:%s, EPOLLERR events occured, accessed:%d, close connect", pContext,
                  pContext->fd, httpContextStateStr(pContext->state), pContext->accessTimes);
        httpCloseContextByServer(pContext);
        continue;
      }

      if (events[i].events & EPOLLHUP) {
        httpDebug("context:%p, fd:%d, state:%s, EPOLLHUP events occured, accessed:%d, close connect", pContext,
                  pContext->fd, httpContextStateStr(pContext->state), pContext->accessTimes);
        httpCloseContextByServer(pContext);
        continue;
      }

      if (!httpAlterContextState(pContext, HTTP_CONTEXT_STATE_READY, HTTP_CONTEXT_STATE_READY)) {
        httpDebug("context:%p, fd:%d, state:%s, not in ready state, ignore read events", pContext, pContext->fd,
                  httpContextStateStr(pContext->state));
        httpReleaseContext(pContext, true);
        continue;
      }

      if (pServer->status != HTTP_SERVER_RUNNING) {
        httpDebug("context:%p, fd:%d, state:%s, server is not running, accessed:%d, close connect", pContext,
                  pContext->fd, httpContextStateStr(pContext->state), pContext->accessTimes);
        httpSendErrorResp(pContext, TSDB_CODE_HTTP_SERVER_OFFLINE);
        httpNotifyContextClose(pContext);
      } else {
        if (httpReadData(pContext)) {
          (*(pThread->processData))(pContext);
          atomic_fetch_add_32(&pServer->requestNum, 1);
        } else {
          httpReleaseContext(pContext, false);
        }
      }
    }
  }
}

static void *httpAcceptHttpConnection(void *arg) {
  int32_t            connFd = -1;
  struct sockaddr_in clientAddr;
  int32_t            threadId = 0;
  HttpServer *       pServer = &tsHttpServer;
  HttpThread *       pThread = NULL;
  HttpContext *      pContext = NULL;
  int32_t            totalFds = 0;

  taosSetMaskSIGPIPE();

  pServer->fd = taosOpenTcpServerSocket(pServer->serverIp, pServer->serverPort);

  if (pServer->fd < 0) {
    httpError("http server:%s, failed to open http socket, ip:%s:%u error:%s", pServer->label,
              taosIpStr(pServer->serverIp), pServer->serverPort, strerror(errno));
    return NULL;
  } else {
    httpInfo("http server init success at %u", pServer->serverPort);
    pServer->status = HTTP_SERVER_RUNNING;
  }

  while (1) {
    socklen_t addrlen = sizeof(clientAddr);
    connFd = (int32_t)accept(pServer->fd, (struct sockaddr *)&clientAddr, &addrlen);
    if (connFd == -1) {
      if (errno == EINVAL) {
        httpDebug("http server:%s socket was shutdown, exiting...", pServer->label);
        break;
      }
      httpError("http server:%s, accept connect failure, errno:%d reason:%s", pServer->label, errno, strerror(errno));
      continue;
    }

    totalFds = 1;
    for (int32_t i = 0; i < pServer->numOfThreads; ++i) {
      totalFds += pServer->pThreads[i].numOfContexts;
    }

#if 0
    if (totalFds > tsHttpCacheSessions * 100) {
      httpError("fd:%d, ip:%s:%u, totalFds:%d larger than httpCacheSessions:%d*100, refuse connection", connFd,
                taosInetNtoa(clientAddr.sin_addr), htons(clientAddr.sin_port), totalFds, tsHttpCacheSessions);
      taosCloseSocket(connFd);
      continue;
    }
#endif    

    taosKeepTcpAlive(connFd);
    taosSetNonblocking(connFd, 1);

    // pick up the thread to handle this connection
    pThread = pServer->pThreads + threadId;

    pContext = httpCreateContext(connFd);
    if (pContext == NULL) {
      httpError("fd:%d, ip:%s:%u, no enough resource to allocate http context", connFd, taosInetNtoa(clientAddr.sin_addr),
                htons(clientAddr.sin_port));
      taosCloseSocket(connFd);
      continue;
    }

    pContext->pThread = pThread;
    sprintf(pContext->ipstr, "%s:%u", taosInetNtoa(clientAddr.sin_addr), htons(clientAddr.sin_port));
    
    struct epoll_event event;
    event.events = EPOLLIN | EPOLLPRI | EPOLLWAKEUP | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
    event.data.ptr = pContext;
    if (epoll_ctl(pThread->pollFd, EPOLL_CTL_ADD, connFd, &event) < 0) {
      httpError("context:%p, fd:%d, ip:%s, thread:%s, failed to add http fd for epoll, error:%s", pContext, connFd,
                pContext->ipstr, pThread->label, strerror(errno));
      taosClose(pContext->fd);
      httpReleaseContext(pContext, true);
      continue;
    }

    // notify the data process, add into the FdObj list
    atomic_add_fetch_32(&pThread->numOfContexts, 1);
    httpDebug("context:%p, fd:%d, ip:%s, thread:%s numOfContexts:%d totalContext:%d, accept a new connection", pContext,
              connFd, pContext->ipstr, pThread->label, pThread->numOfContexts, totalFds);

    // pick up next thread for next connection
    threadId++;
    threadId = threadId % pServer->numOfThreads;
  }

  taosCloseSocket(pServer->fd);
  return NULL;
}

bool httpInitConnect() {
  HttpServer *pServer = &tsHttpServer;
  pServer->pThreads = calloc(pServer->numOfThreads, sizeof(HttpThread));
  if (pServer->pThreads == NULL) {
    httpError("init error no enough memory");
    return false;
  }

  HttpThread *pThread = pServer->pThreads;
  for (int32_t i = 0; i < pServer->numOfThreads; ++i) {
    sprintf(pThread->label, "%s%d", pServer->label, i);
    pThread->processData = pServer->processData;
    pThread->threadId = i;

    if (pthread_mutex_init(&(pThread->threadMutex), NULL) < 0) {
      httpError("http thread:%s, failed to init HTTP process data mutex, reason:%s", pThread->label, strerror(errno));
      return false;
    }

    pThread->pollFd = (SOCKET)epoll_create(HTTP_MAX_EVENTS);  // size does not matter
    if (pThread->pollFd < 0) {
      httpError("http thread:%s, failed to create HTTP epoll", pThread->label);
      pthread_mutex_destroy(&(pThread->threadMutex));
      return false;
    }

    pthread_attr_t thattr;
    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
    if (pthread_create(&(pThread->thread), &thattr, (void *)httpProcessHttpData, (void *)(pThread)) != 0) {
      httpError("http thread:%s, failed to create HTTP process data thread, reason:%s", pThread->label,
                strerror(errno));
      pthread_mutex_destroy(&(pThread->threadMutex));        
      return false;
    }
    pthread_attr_destroy(&thattr);

    httpDebug("http thread:%p:%s, initialized", pThread, pThread->label);
    pThread++;
  }

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  if (pthread_create(&(pServer->thread), &thattr, (void *)httpAcceptHttpConnection, (void *)(pServer)) != 0) {
    httpError("http server:%s, failed to create Http accept thread, reason:%s", pServer->label, strerror(errno));
    httpCleanUpConnect();
    return false;
  }
  pthread_attr_destroy(&thattr);

  httpDebug("http server:%s, initialized, ip:%s:%u, numOfThreads:%d", pServer->label, taosIpStr(pServer->serverIp),
            pServer->serverPort, pServer->numOfThreads);
  return true;
}

static bool httpReadData(HttpContext *pContext) {
  HttpParser *pParser = pContext->parser;
  if (!pParser->inited) {
    httpInitParser(pParser);
  }

  if (pParser->parsed) {
    httpDebug("context:%p, fd:%d, not in ready state, parsed:%d", pContext, pContext->fd, pParser->parsed);    
    return false;
  }

  pContext->accessTimes++;
  pContext->lastAccessTime = taosGetTimestampSec();
  char buf[HTTP_STEP_SIZE + 1] = {0};

  while (1) {
    int32_t nread = (int32_t)taosReadSocket(pContext->fd, buf, HTTP_STEP_SIZE);
    if (nread > 0) {
      buf[nread] = '\0';
      httpTraceL("context:%p, fd:%d, nread:%d content:%s", pContext, pContext->fd, nread, buf);
      int32_t ok = httpParseBuf(pParser, buf, nread);

      if (ok) {
        httpError("context:%p, fd:%d, parse failed, ret:%d code:%d close connect", pContext, pContext->fd, ok,
                  pParser->parseCode);
        httpSendErrorResp(pContext, pParser->parseCode);
        httpNotifyContextClose(pContext);
        return false;
      }

      if (pParser->parseCode) {
        httpError("context:%p, fd:%d, parse failed, code:%d close connect", pContext, pContext->fd, pParser->parseCode);
        httpSendErrorResp(pContext, pParser->parseCode);
        httpNotifyContextClose(pContext);
        return false;
      }

      if (!pParser->parsed) {
        httpTrace("context:%p, fd:%d, read not finished", pContext, pContext->fd);
        continue;
      } else {
        httpDebug("context:%p, fd:%d, bodyLen:%d", pContext, pContext->fd, pParser->body.pos);
        return true;
      }
    } else if (nread < 0) {
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
        httpDebug("context:%p, fd:%d, read from socket error:%d, wait another event", pContext, pContext->fd, errno);
        return false;  // later again
      } else {
        httpError("context:%p, fd:%d, read from socket error:%d, close connect", pContext, pContext->fd, errno);
        return false;
      }
    } else {
      httpError("context:%p, fd:%d, nread:%d, wait another event", pContext, pContext->fd, nread);
      return false;
    }
  }
}
