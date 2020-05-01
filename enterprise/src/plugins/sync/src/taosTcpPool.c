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

#include "os.h"
#include "tulog.h"
#include "tutil.h"
#include "tsocket.h"
#include "taosTcpPool.h"

typedef struct SThreadObj {
  pthread_t thread;
  int       threadId;
  int       pollFd;
  int       numOfFds;
  struct SThreadPool *pPool;
} SThreadObj;

typedef struct SThreadPool {
  SPoolInfo    info;
  SThreadObj **pThread;
  pthread_t    thread;
  int          nextId;
} SThreadPool;

static void *taosAcceptPeerTcpConnection(void *argv);
static void  taosProcessTcpData(void *param);
static SThreadObj *taosGetTcpThread(SThreadPool *pPool);

void *taosOpenTcpThreadPool(SPoolInfo *pInfo)
{
  pthread_attr_t thattr;

  SThreadPool *pPool = calloc(sizeof(SThreadPool), 1);
  pPool->info = *pInfo;
  
  pPool->pThread = (SThreadObj **) calloc(sizeof(SThreadObj *), pInfo->numOfThreads);
  if (pPool->pThread == NULL) {
    uError("TCP server, no enough memory");
    free(pPool);
    return NULL;
  }

  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  if (pthread_create(&(pPool->thread), &thattr, (void *) taosAcceptPeerTcpConnection, pPool) != 0) {
    uError("TCP server, failed to create accept thread, reason:%s", strerror(errno));
    free(pPool->pThread); free(pPool);
    return NULL;
  }

  pthread_attr_destroy(&thattr);

  return pPool;
}

void taosCloseTcpThreadPool(void *param)
{
  SThreadPool *pPool = (SThreadPool *)param;
  SThreadObj  *pThread;

  pthread_cancel(pPool->thread);
  pthread_join(pPool->thread, NULL);

  for (int i = 0; i < pPool->info.numOfThreads; ++i) {
    pThread = pPool->pThread[i];
    if (pThread) {
      close(pThread->pollFd);
      pthread_cancel(pThread->thread);
      pthread_join(pThread->thread, NULL);
      tfree(pThread);
    }
  }

  tfree(pPool->pThread);
  tfree(pPool);
}

void *taosAllocateTcpThread(void *param, void *pPeer, int connFd)
{
  struct epoll_event event;
  SThreadPool *pPool = (SThreadPool *)param;

  event.events = EPOLLIN | EPOLLPRI;
  event.data.ptr = pPeer;

  SThreadObj *pThread = taosGetTcpThread(pPool);

  if (pThread ) {
    if (epoll_ctl(pThread->pollFd, EPOLL_CTL_ADD, connFd, &event) < 0) {
      uError("failed to add fd:%d(%s)", connFd, strerror(errno));
      pThread = NULL;
    } else {
      pThread->numOfFds++;
      //uTrace("fd:%d is added, num:%d", connFd, pThread->numOfFds);
    }
  }

  return pThread;
}

void taosFreeTcpThread(void *param, int *pfd)
{
  if (*pfd < 0) return;

  SThreadObj *pThread = (SThreadObj *)param;

  epoll_ctl(pThread->pollFd, EPOLL_CTL_DEL, *pfd, NULL);
  taosCloseTcpSocket(*pfd);
  pThread->numOfFds--;
  //uTrace("fd:%d is removed, num:%d", *pfd, pThread->numOfFds);
  
  *pfd = -1;
}

#define maxEvents 10

static void taosProcessTcpData(void *param) {
  SThreadObj        *pThread = (SThreadObj *) param;
  SThreadPool       *pPool = pThread->pPool;
  SPoolInfo         *pInfo = &pPool->info;
  struct epoll_event events[maxEvents];

  void *buffer = malloc(pInfo->bufferSize);

  taosBlockSIGPIPE();

  while (1) {
    int fdNum = epoll_wait(pThread->pollFd, events, maxEvents, -1);
    if (fdNum < 0) { 
      uError("epoll_wait failed (%s)", strerror(errno));
      continue;
    }

    for (int i = 0; i < fdNum; ++i) {
      void *ahandle = events[i].data.ptr;
      if (ahandle == NULL) continue;

      if (events[i].events & EPOLLERR) {
        (*pInfo->processBrokenLink)(ahandle);
        continue;
      }

      if (events[i].events & EPOLLHUP) {
        (*pInfo->processBrokenLink)(ahandle);
        continue;
      }

      if ((*pInfo->processIncomingMsg)(ahandle, buffer) < 0) {
        (*pInfo->processBrokenLink)(ahandle);
        continue;
      }
    }
  }

  free (buffer);
}

static void *taosAcceptPeerTcpConnection(void *argv) {
  SThreadPool   *pPool = (SThreadPool *)argv;
  SPoolInfo     *pInfo = &pPool->info;

  taosBlockSIGPIPE();

  int tcpFd = taosOpenTcpServerSocket(pInfo->serverIp, pInfo->port);
  if (tcpFd < 0) {
    uError("failed to create TCP server socket, port:%d (%s)", pInfo->port, strerror(errno));
    return NULL;
  }

  while (1) {
    struct sockaddr_in clientAddr;
    socklen_t addrlen = sizeof(clientAddr);
    int connFd = accept(tcpFd, (struct sockaddr *) &clientAddr, &addrlen);
    if (connFd < 0) {
      uError("TCP accept failure, reason:%s", strerror(errno));
      continue;
    }

    taosKeepTcpAlive(connFd);
    (*pInfo->processIncomingConn)(connFd, clientAddr.sin_addr.s_addr);
  }

  return NULL;
}

static SThreadObj *taosGetTcpThread(SThreadPool *pPool) {
  SThreadObj *pThread = pPool->pThread[pPool->nextId];

  if (pThread) return pThread;

  pThread = (SThreadObj *) calloc(1, sizeof(SThreadObj));
  if (pThread == NULL) return NULL;

  pThread->pPool = pPool;
  pThread->pollFd = epoll_create(10);  // size does not matter
  if (pThread->pollFd < 0) {
    free(pThread);
    return NULL;
  }

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  int ret = pthread_create(&(pThread->thread), &thattr, (void *) taosProcessTcpData, pThread);
  pthread_attr_destroy(&thattr);

  if (ret != 0) {
    free(pThread);
    return NULL;
  }

  pPool->pThread[pPool->nextId] = pThread;
  pPool->nextId++;
  pPool->nextId = pPool->nextId % pPool->info.numOfThreads;

  return pThread;
}


