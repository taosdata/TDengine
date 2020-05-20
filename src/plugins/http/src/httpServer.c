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
#include "http.h"
#include "httpLog.h"
#include "httpCode.h"
#include "httpHandle.h"
#include "httpResp.h"

#ifndef EPOLLWAKEUP
 #define EPOLLWAKEUP (1u << 29)
#endif

const char* httpContextStateStr(HttpContextState state) {
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

void httpRemoveContextFromEpoll(HttpThread *pThread, HttpContext *pContext) {
  if (pContext->fd >= 0) {
    epoll_ctl(pThread->pollFd, EPOLL_CTL_DEL, pContext->fd, NULL);
    taosCloseSocket(pContext->fd);
    pContext->fd = -1;
  }
}

bool httpAlterContextState(HttpContext *pContext, HttpContextState srcState, HttpContextState destState) {
  return (atomic_val_compare_exchange_32(&pContext->state, srcState, destState) == srcState);
}

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
  pContext->state = HTTP_CONTEXT_STATE_READY;
  return pContext;
}

void httpFreeContext(HttpServer *pServer, HttpContext *pContext) {
  if (pContext->fromMemPool) {
    httpTrace("context:%p, is freed from mempool", pContext);
    taosMemPoolFree(pServer->pContextPool, (char *)pContext);
  } else {
    httpTrace("context:%p, is freed from raw memory", pContext);
    tfree(pContext);
  }
}

void httpCleanUpContextTimer(HttpContext *pContext) {
  if (pContext->timer != NULL) {
    taosTmrStopA(&pContext->timer);
    //httpTrace("context:%p, ip:%s, close timer:%p", pContext, pContext->ipstr, pContext->timer);
    pContext->timer = NULL;
  }
}

void httpCleanUpContext(HttpContext *pContext, void *unused) {
  httpTrace("context:%p, start the clean up operation, sig:%p", pContext, pContext->signature);
  void *sig = atomic_val_compare_exchange_ptr(&pContext->signature, pContext, 0);
  if (sig == NULL) {
    httpTrace("context:%p is freed by another thread.", pContext);
    return;
  }

  HttpThread *pThread = pContext->pThread;

  httpCleanUpContextTimer(pContext);

  httpRemoveContextFromEpoll(pThread, pContext);

  httpRestoreSession(pContext);

  pthread_mutex_lock(&pThread->threadMutex);

  pThread->numOfFds--;
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
  pContext->state = HTTP_CONTEXT_STATE_READY;

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
  pContext->reqType = HTTP_REQTYPE_OTHERS;
  pContext->encodeMethod = NULL;
  pContext->timer = NULL;
  memset(&pContext->singleCmd, 0, sizeof(HttpSqlCmd));

  HttpParser *pParser = &pContext->parser;
  memset(pParser, 0, sizeof(HttpParser));
  pParser->pCur = pParser->pLast = pParser->buffer;

  httpTrace("context:%p, fd:%d, ip:%s, thread:%s, accessTimes:%d, parsed:%d",
          pContext, pContext->fd, pContext->ipstr, pContext->pThread->label, pContext->accessTimes, pContext->parsed);
  return true;
}


void httpCloseContext(HttpThread *pThread, HttpContext *pContext) {
  taosTmrReset((TAOS_TMR_CALLBACK)httpCleanUpContext, HTTP_DELAY_CLOSE_TIME_MS, pContext, pThread->pServer->timerHandle, &pContext->timer);
  httpTrace("context:%p, fd:%d, ip:%s, state:%s will be closed after:%d ms, timer:%p",
          pContext, pContext->fd, pContext->ipstr, httpContextStateStr(pContext->state), HTTP_DELAY_CLOSE_TIME_MS, pContext->timer);
}

void httpCloseContextByApp(HttpContext *pContext) {
  HttpThread *pThread = pContext->pThread;
  pContext->parsed = false;

  bool keepAlive = true;
  if (pContext->httpVersion == HTTP_VERSION_10 && pContext->httpKeepAlive != HTTP_KEEPALIVE_ENABLE) {
    keepAlive = false;
  } else if (pContext->httpVersion != HTTP_VERSION_10 && pContext->httpKeepAlive == HTTP_KEEPALIVE_DISABLE) {
    keepAlive = false;
  } else {}

  if (keepAlive) {
    if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_HANDLING, HTTP_CONTEXT_STATE_READY)) {
      httpTrace("context:%p, fd:%d, ip:%s, last state:handling, keepAlive:true, reuse connect",
              pContext, pContext->fd, pContext->ipstr);
    } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_DROPPING, HTTP_CONTEXT_STATE_CLOSED)) {
      httpRemoveContextFromEpoll(pThread, pContext);
      httpTrace("context:%p, fd:%d, ip:%s, last state:dropping, keepAlive:true, close connect",
              pContext, pContext->fd, pContext->ipstr);
      httpCloseContext(pThread, pContext);
    } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_READY, HTTP_CONTEXT_STATE_READY)) {
      httpTrace("context:%p, fd:%d, ip:%s, last state:ready, keepAlive:true, reuse connect",
              pContext, pContext->fd, pContext->ipstr);
    } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_CLOSED, HTTP_CONTEXT_STATE_CLOSED)) {
      httpRemoveContextFromEpoll(pThread, pContext);
      httpTrace("context:%p, fd:%d, ip:%s, last state:ready, keepAlive:true, close connect",
                pContext, pContext->fd, pContext->ipstr);
      httpCloseContext(pThread, pContext);
    } else {
      httpRemoveContextFromEpoll(pThread, pContext);
      httpError("context:%p, fd:%d, ip:%s, last state:%s:%d, keepAlive:true, close connect",
              pContext, pContext->fd, pContext->ipstr, httpContextStateStr(pContext->state), pContext->state);
      httpCloseContext(pThread, pContext);
    }
  } else {
    httpRemoveContextFromEpoll(pThread, pContext);
    httpTrace("context:%p, fd:%d, ip:%s, last state:%s:%d, keepAlive:false, close connect",
              pContext, pContext->fd, pContext->ipstr, httpContextStateStr(pContext->state), pContext->state);
    httpCloseContext(pThread, pContext);
  }
}

void httpCloseContextByServer(HttpThread *pThread, HttpContext *pContext) {
  httpRemoveContextFromEpoll(pThread, pContext);
  pContext->parsed = false;
  
  if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_HANDLING, HTTP_CONTEXT_STATE_DROPPING)) {
    httpTrace("context:%p, fd:%d, ip:%s, epoll finished, still used by app", pContext, pContext->fd, pContext->ipstr);
  } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_DROPPING, HTTP_CONTEXT_STATE_DROPPING)) {
    httpTrace("context:%p, fd:%d, ip:%s, epoll already finished, wait app finished", pContext, pContext->fd, pContext->ipstr);
  } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_READY, HTTP_CONTEXT_STATE_CLOSED)) {
    httpTrace("context:%p, fd:%d, ip:%s, epoll finished, close context", pContext, pContext->fd, pContext->ipstr);
    httpCloseContext(pThread, pContext);
  } else if (httpAlterContextState(pContext, HTTP_CONTEXT_STATE_CLOSED, HTTP_CONTEXT_STATE_CLOSED)) {
    httpTrace("context:%p, fd:%d, ip:%s, epoll finished, will be closed soon", pContext, pContext->fd, pContext->ipstr);
    httpCloseContext(pThread, pContext);
  } else {
    httpError("context:%p, fd:%d, ip:%s, unknown state:%d", pContext, pContext->fd, pContext->ipstr, pContext->state);
    httpCloseContext(pThread, pContext);
  }
}

void httpCloseContextByServerForExpired(void *param, void *tmrId) {
  HttpContext *pContext = (HttpContext *)param;
  httpRemoveContextFromEpoll(pContext->pThread, pContext);
  httpError("context:%p, fd:%d, ip:%s, read http body error, time expired, timer:%p", pContext, pContext->fd, pContext->ipstr, tmrId);
  httpSendErrorResp(pContext, HTTP_PARSE_BODY_ERROR);
  httpCloseContextByServer(pContext->pThread, pContext);
}


static void httpStopThread(HttpThread* pThread) {
  pThread->stop = true;

  // signal the thread to stop, try graceful method first,
  // and use pthread_cancel when failed
  struct epoll_event event = { .events = EPOLLIN };
  eventfd_t fd = eventfd(1, 0);
  if (fd == -1) {
    pthread_cancel(pThread->thread);
  } else if (epoll_ctl(pThread->pollFd, EPOLL_CTL_ADD, fd, &event) < 0) {
    pthread_cancel(pThread->thread);
  }

  pthread_join(pThread->thread, NULL);
  if (fd != -1) {
    close(fd);
  }

  close(pThread->pollFd);
  pthread_mutex_destroy(&(pThread->threadMutex));

  //while (pThread->pHead) {
  //  httpCleanUpContext(pThread->pHead, 0);
  //}
}


void httpCleanUpConnect(HttpServer *pServer) {
  if (pServer == NULL) return;

  shutdown(pServer->fd, SHUT_RD);
  pthread_join(pServer->thread, NULL);

  for (int i = 0; i < pServer->numOfThreads; ++i) {
    HttpThread* pThread = pServer->pThreads + i;
    if (pThread != NULL) {
      httpStopThread(pThread);
    }
  }

  tfree(pServer->pThreads);
  httpTrace("http server:%s is cleaned up", pServer->label);
}

// read all the data, then just discard it
void httpReadDirtyData(HttpContext *pContext) {
  int fd = pContext->fd;
  char data[1024] = {0};
  int  len = (int)taosReadSocket(fd, data, 1024);
  while (len >= sizeof(data)) {
    len = (int)taosReadSocket(fd, data, 1024);
  }
}

bool httpReadDataImp(HttpContext *pContext) {
  HttpParser *pParser = &pContext->parser;

  while (pParser->bufsize <= (HTTP_BUFFER_SIZE - HTTP_STEP_SIZE)) {
    int nread = (int)taosReadSocket(pContext->fd, pParser->buffer + pParser->bufsize, HTTP_STEP_SIZE);
    if (nread >= 0 && nread < HTTP_STEP_SIZE) {
      pParser->bufsize += nread;
      break;
    } else if (nread < 0) {
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
        httpTrace("context:%p, fd:%d, ip:%s, read from socket error:%d, wait another event",
                  pContext, pContext->fd, pContext->ipstr, errno);
        break;
      } else {
        httpError("context:%p, fd:%d, ip:%s, read from socket error:%d, close connect",
                  pContext, pContext->fd, pContext->ipstr, errno);
        return false;
      }
    } else {
      pParser->bufsize += nread;
    }

    if (pParser->bufsize >= (HTTP_BUFFER_SIZE - HTTP_STEP_SIZE)) {
      httpReadDirtyData(pContext);
      httpError("context:%p, fd:%d, ip:%s, thread:%s, request big than:%d",
                pContext, pContext->fd, pContext->ipstr, pContext->pThread->label, HTTP_BUFFER_SIZE);
      httpRemoveContextFromEpoll(pContext->pThread, pContext);
      httpSendErrorResp(pContext, HTTP_REQUSET_TOO_BIG);
      return false;
    }
  }

  pParser->buffer[pParser->bufsize] = 0;

  return true;
}

bool httpDecompressData(HttpContext *pContext) {
  if (pContext->contentEncoding != HTTP_COMPRESS_GZIP) {
    httpDump("context:%p, fd:%d, ip:%s, content:%s", pContext, pContext->fd, pContext->ipstr, pContext->parser.data.pos);
    return true;
  }

  char   *decompressBuf = calloc(HTTP_DECOMPRESS_BUF_SIZE, 1);
  int32_t decompressBufLen = HTTP_DECOMPRESS_BUF_SIZE;
  size_t  bufsize = sizeof(pContext->parser.buffer) - (pContext->parser.data.pos - pContext->parser.buffer) - 1;
  if (decompressBufLen > (int)bufsize) {
    decompressBufLen = (int)bufsize;
  }

  int ret = httpGzipDeCompress(pContext->parser.data.pos, pContext->parser.data.len, decompressBuf, &decompressBufLen);

  if (ret == 0) {
    memcpy(pContext->parser.data.pos, decompressBuf, decompressBufLen);
    pContext->parser.data.pos[decompressBufLen] = 0;
    httpDump("context:%p, fd:%d, ip:%s, rawSize:%d, decompressSize:%d, content:%s",
              pContext, pContext->fd, pContext->ipstr, pContext->parser.data.len, decompressBufLen,  decompressBuf);
    pContext->parser.data.len = decompressBufLen;
  } else {
    httpError("context:%p, fd:%d, ip:%s, failed to decompress data, rawSize:%d, error:%d",
              pContext, pContext->fd, pContext->ipstr, pContext->parser.data.len, ret);
  }

  free(decompressBuf);
  return ret == 0;
}

bool httpReadData(HttpThread *pThread, HttpContext *pContext) {
  if (!pContext->parsed) {
    httpInitContext(pContext);
  }

  if (!httpReadDataImp(pContext)) {
    httpCloseContextByServer(pThread, pContext);
    return false;
  }

  if (!httpParseRequest(pContext)) {
    httpCloseContextByServer(pThread, pContext);
    return false;
  }

  int ret = httpCheckReadCompleted(pContext);
  if (ret == HTTP_CHECK_BODY_CONTINUE) {
    taosTmrReset(httpCloseContextByServerForExpired, HTTP_EXPIRED_TIME, pContext, pThread->pServer->timerHandle, &pContext->timer);
    //httpTrace("context:%p, fd:%d, ip:%s, not finished yet, try another times, timer:%p", pContext, pContext->fd, pContext->ipstr, pContext->timer);
    return false;
  } else if (ret == HTTP_CHECK_BODY_SUCCESS){
    httpCleanUpContextTimer(pContext);
    httpTrace("context:%p, fd:%d, ip:%s, thread:%s, read size:%d, dataLen:%d",
              pContext, pContext->fd, pContext->ipstr, pContext->pThread->label, pContext->parser.bufsize, pContext->parser.data.len);
    if (httpDecompressData(pContext)) {
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
  HttpThread  *pThread = (HttpThread *)param;
  HttpContext *pContext;
  int          fdNum;

  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGPIPE);
  pthread_sigmask(SIG_SETMASK, &set, NULL);

  while (1) {
    struct epoll_event events[HTTP_MAX_EVENTS];
    //-1 means uncertainty, 0-nowait, 1-wait 1 ms, set it from -1 to 1
    fdNum = epoll_wait(pThread->pollFd, events, HTTP_MAX_EVENTS, 1);
    if (pThread->stop) {
      httpTrace("%p, http thread get stop event, exiting...", pThread);
      break;
    }
    if (fdNum <= 0) continue;

    for (int i = 0; i < fdNum; ++i) {
      pContext = events[i].data.ptr;
      if (pContext->signature != pContext || pContext->pThread != pThread || pContext->fd <= 0) {
        continue;
      }

      if (events[i].events & EPOLLPRI) {
        httpTrace("context:%p, fd:%d, ip:%s, state:%s, EPOLLPRI events occured, accessed:%d, close connect",
                  pContext, pContext->fd, pContext->ipstr, httpContextStateStr(pContext->state), pContext->accessTimes);
        httpRemoveContextFromEpoll(pThread, pContext);
        httpCloseContextByServer(pThread, pContext);
        continue;
      }

      if (events[i].events & EPOLLRDHUP) {
        httpTrace("context:%p, fd:%d, ip:%s, state:%s, EPOLLRDHUP events occured, accessed:%d, close connect",
                  pContext, pContext->fd, pContext->ipstr, httpContextStateStr(pContext->state), pContext->accessTimes);
        httpRemoveContextFromEpoll(pThread, pContext);
        httpCloseContextByServer(pThread, pContext);
        continue;
      }

      if (events[i].events & EPOLLERR) {
        httpTrace("context:%p, fd:%d, ip:%s, state:%s, EPOLLERR events occured, accessed:%d, close connect",
                  pContext, pContext->fd, pContext->ipstr, httpContextStateStr(pContext->state), pContext->accessTimes);
        httpRemoveContextFromEpoll(pThread, pContext);
        httpCloseContextByServer(pThread, pContext);
        continue;
      }

      if (events[i].events & EPOLLHUP) {
        httpTrace("context:%p, fd:%d, ip:%s, state:%s, EPOLLHUP events occured, accessed:%d, close connect",
                  pContext, pContext->fd, pContext->ipstr, httpContextStateStr(pContext->state), pContext->accessTimes);
        httpRemoveContextFromEpoll(pThread, pContext);
        httpCloseContextByServer(pThread, pContext);
        continue;
      }

      if (!httpAlterContextState(pContext, HTTP_CONTEXT_STATE_READY, HTTP_CONTEXT_STATE_READY)) {
        httpTrace("context:%p, fd:%d, ip:%s, state:%s, not in ready state, ignore read events",
                pContext, pContext->fd, pContext->ipstr, httpContextStateStr(pContext->state));
        continue;
      }

      if (!pContext->pThread->pServer->online) {
        httpTrace("context:%p, fd:%d, ip:%s, state:%s, server is not online, accessed:%d, close connect",
                  pContext, pContext->fd, pContext->ipstr, httpContextStateStr(pContext->state), pContext->accessTimes);
        httpRemoveContextFromEpoll(pThread, pContext);
        httpReadDirtyData(pContext);
        httpSendErrorResp(pContext, HTTP_SERVER_OFFLINE);
        httpCloseContextByServer(pThread, pContext);
        continue;
      } else {
        if (httpReadData(pThread, pContext)) {
          (*(pThread->processData))(pContext);
          atomic_fetch_add_32(&pThread->pServer->requestNum, 1);
        }
      }
    }
  }
}

void* httpAcceptHttpConnection(void *arg) {
  int                connFd = -1;
  struct sockaddr_in clientAddr;
  int                threadId = 0;
  HttpThread *       pThread;
  HttpServer *       pServer;
  HttpContext *      pContext;
  int                totalFds;

  pServer = (HttpServer *)arg;

  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGPIPE);
  pthread_sigmask(SIG_SETMASK, &set, NULL);

  pServer->fd = taosOpenTcpServerSocket(pServer->serverIp, pServer->serverPort);

  if (pServer->fd < 0) {
    httpError("http server:%s, failed to open http socket, ip:%s:%u error:%s", pServer->label, taosIpStr(pServer->serverIp),
              pServer->serverPort, strerror(errno));
    return NULL;
  } else {
    httpPrint("http service init success at %u", pServer->serverPort);
    pServer->online = true;
  }

  while (1) {
    socklen_t addrlen = sizeof(clientAddr);
    connFd = (int)accept(pServer->fd, (struct sockaddr *)&clientAddr, &addrlen);
    if (connFd == -1) {
      if (errno == EINVAL) {
        httpTrace("%s HTTP server socket was shutdown, exiting...", pServer->label);
        break;
      }
      httpError("http server:%s, accept connect failure, errno:%d, reason:%s", pServer->label, errno, strerror(errno));
      continue;
    }

    totalFds = 1;
    for (int i = 0; i < pServer->numOfThreads; ++i) {
      totalFds += pServer->pThreads[i].numOfFds;
    }

    if (totalFds > tsHttpCacheSessions * 100) {
      httpError("fd:%d, ip:%s:%u, totalFds:%d larger than httpCacheSessions:%d*100, refuse connection",
              connFd, inet_ntoa(clientAddr.sin_addr), htons(clientAddr.sin_port), totalFds, tsHttpCacheSessions);
      taosCloseSocket(connFd);
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

    httpTrace("context:%p, fd:%d, ip:%s:%u, thread:%s, numOfFds:%d, totalFds:%d, accept a new connection",
            pContext, connFd, inet_ntoa(clientAddr.sin_addr), htons(clientAddr.sin_port), pThread->label,
            pThread->numOfFds, totalFds);

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

    pthread_mutex_unlock(&(pThread->threadMutex));

    // pick up next thread for next connection
    threadId++;
    threadId = threadId % pServer->numOfThreads;
  }

  close(pServer->fd);
  return NULL;
}

bool httpInitConnect(HttpServer *pServer) {
  int            i;
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

    pThread->pollFd = epoll_create(HTTP_MAX_EVENTS);  // size does not matter
    if (pThread->pollFd < 0) {
      httpError("http thread:%s, failed to create HTTP epoll", pThread->label);
      return false;
    }

    pthread_attr_t thattr;
    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
    if (pthread_create(&(pThread->thread), &thattr, (void *)httpProcessHttpData, (void *)(pThread)) != 0) {
      httpError("http thread:%s, failed to create HTTP process data thread, reason:%s",
                pThread->label, strerror(errno));
      return false;
    }
    pthread_attr_destroy(&thattr);

    httpTrace("http thread:%p:%s, initialized", pThread, pThread->label);
    pThread++;
  }

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  if (pthread_create(&(pServer->thread), &thattr, (void *)httpAcceptHttpConnection, (void *)(pServer)) != 0) {
    httpError("http server:%s, failed to create Http accept thread, reason:%s", pServer->label, strerror(errno));
    return false;
  }
  pthread_attr_destroy(&thattr);

  httpTrace("http server:%s, initialized, ip:%s:%u, numOfThreads:%d", pServer->label, taosIpStr(pServer->serverIp),
            pServer->serverPort, pServer->numOfThreads);
  return true;
}
