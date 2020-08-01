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
#include "taoserror.h"
#include "taosTcpPool.h"

typedef struct SThreadObj {
  pthread_t thread;
  bool      stop;
  int       pollFd;
  int       numOfFds;
  struct SPoolObj *pPool;
} SThreadObj;

typedef struct SPoolObj {
  SPoolInfo    info;
  SThreadObj **pThread;
  pthread_t    thread;
  int          nextId;
  int          acceptFd;  // FD for accept new connection
} SPoolObj;

typedef struct {
  SThreadObj *pThread;
  void       *ahandle;
  int         fd;
  int         closedByApp; 
} SConnObj;

static void *taosAcceptPeerTcpConnection(void *argv);
static void *taosProcessTcpData(void *param);
static SThreadObj *taosGetTcpThread(SPoolObj *pPool);
static void taosStopPoolThread(SThreadObj* pThread);

void *taosOpenTcpThreadPool(SPoolInfo *pInfo)
{
  pthread_attr_t thattr;

  SPoolObj *pPool = calloc(sizeof(SPoolObj), 1);
  if (pPool == NULL) {
    uError("TCP server, no enough memory");
    return NULL;
  }

  pPool->info = *pInfo;
  
  pPool->pThread = (SThreadObj **) calloc(sizeof(SThreadObj *), pInfo->numOfThreads);
  if (pPool->pThread == NULL) {
    uError("TCP server, no enough memory");
    free(pPool);
    return NULL;
  }

  pPool->acceptFd = taosOpenTcpServerSocket(pInfo->serverIp, pInfo->port);
  if (pPool->acceptFd < 0) {
    free(pPool->pThread); free(pPool);
    uError("failed to create TCP server socket, port:%d (%s)", pInfo->port, strerror(errno));
    return NULL;
  }

  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  if (pthread_create(&(pPool->thread), &thattr, (void *) taosAcceptPeerTcpConnection, pPool) != 0) {
    uError("TCP server, failed to create accept thread, reason:%s", strerror(errno));
    close(pPool->acceptFd);
    free(pPool->pThread); free(pPool);
    return NULL;
  }

  pthread_attr_destroy(&thattr);

  uDebug("%p TCP pool is created", pPool);
  return pPool;
}

void taosCloseTcpThreadPool(void *param)
{
  SPoolObj    *pPool = (SPoolObj *)param;
  SThreadObj  *pThread;

  shutdown(pPool->acceptFd, SHUT_RD);  
  pthread_join(pPool->thread, NULL);

  for (int i = 0; i < pPool->info.numOfThreads; ++i) {
    pThread = pPool->pThread[i];
    if (pThread) taosStopPoolThread(pThread); 
  }

  taosTFree(pPool->pThread);
  free(pPool);
  uDebug("%p TCP pool is closed", pPool);
}

void *taosAllocateTcpConn(void *param, void *pPeer, int connFd)
{
  struct epoll_event event;
  SPoolObj *pPool = (SPoolObj *)param;

  SConnObj *pConn = (SConnObj *) calloc(sizeof(SConnObj), 1);
  if (pConn == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno); 
    return NULL;
  }

  SThreadObj *pThread = taosGetTcpThread(pPool);
  if (pThread == NULL) {
    free(pConn);
    return NULL;
  }

  pConn->fd = connFd;
  pConn->pThread = pThread;
  pConn->ahandle = pPeer;
  pConn->closedByApp = 0;

  event.events = EPOLLIN | EPOLLRDHUP;
  event.data.ptr = pConn;

  if (epoll_ctl(pThread->pollFd, EPOLL_CTL_ADD, connFd, &event) < 0) {
    uError("failed to add fd:%d(%s)", connFd, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno); 
    free(pConn);
    pConn = NULL;
  } else {
    pThread->numOfFds++;
    uDebug("%p fd:%d is added to epoll thread, num:%d", pThread, connFd, pThread->numOfFds);
  }

  return pConn;
}

void taosFreeTcpConn(void *param)
{
  SConnObj *pConn = (SConnObj *)param;
  SThreadObj   *pThread = pConn->pThread;

  uDebug("%p TCP connection will be closed, fd:%d", pThread, pConn->fd);
  pConn->closedByApp = 1;
  shutdown(pConn->fd, SHUT_WR);
}

static void taosProcessBrokenLink(SConnObj *pConn) {
  SThreadObj *pThread = pConn->pThread;
  SPoolObj   *pPool = pThread->pPool;
  SPoolInfo  *pInfo = &pPool->info;
  
  if (pConn->closedByApp == 0) shutdown(pConn->fd, SHUT_WR);
  (*pInfo->processBrokenLink)(pConn->ahandle);

  pThread->numOfFds--;
  epoll_ctl(pThread->pollFd, EPOLL_CTL_DEL, pConn->fd, NULL);
  uDebug("%p fd:%d is removed from epoll thread, num:%d", pThread, pConn->fd, pThread->numOfFds);
  taosClose(pConn->fd);
  free(pConn);
}

#define maxEvents 10

static void *taosProcessTcpData(void *param) {
  SThreadObj     *pThread = (SThreadObj *) param;
  SPoolObj       *pPool = pThread->pPool;
  SPoolInfo      *pInfo = &pPool->info;
  SConnObj       *pConn = NULL;
  struct epoll_event events[maxEvents];

  void *buffer = malloc(pInfo->bufferSize);
  taosBlockSIGPIPE();

  while (1) {
    if (pThread->stop) break; 
    int fdNum = epoll_wait(pThread->pollFd, events, maxEvents, -1);
    if (pThread->stop) {
      uDebug("%p TCP epoll thread is exiting...", pThread);
      break;
    }

    if (fdNum < 0) { 
      uError("epoll_wait failed (%s)", strerror(errno));
      continue;
    }

    for (int i = 0; i < fdNum; ++i) {
      pConn = events[i].data.ptr;
      assert(pConn);

      if (events[i].events & EPOLLERR) {
        taosProcessBrokenLink(pConn);
        continue;
      }

      if (events[i].events & EPOLLHUP) {
        taosProcessBrokenLink(pConn);
        continue;
      }

      if (events[i].events & EPOLLRDHUP) {
        taosProcessBrokenLink(pConn);
        continue;
      }

      if (pConn->closedByApp == 0) {
        if ((*pInfo->processIncomingMsg)(pConn->ahandle, buffer) < 0) {
          taosFreeTcpConn(pConn);
          continue;
        }
      } 
    }
  }

  close(pThread->pollFd);
  free(pThread);
  free(buffer);
  uDebug("%p TCP epoll thread exits", pThread);
  return NULL;  
}

static void *taosAcceptPeerTcpConnection(void *argv) {
  SPoolObj   *pPool = (SPoolObj *)argv;
  SPoolInfo  *pInfo = &pPool->info;

  taosBlockSIGPIPE();

  while (1) {
    struct sockaddr_in clientAddr;
    socklen_t addrlen = sizeof(clientAddr);
    int connFd = accept(pPool->acceptFd, (struct sockaddr *) &clientAddr, &addrlen);
    if (connFd < 0) {
      if (errno == EINVAL) {
        uDebug("%p TCP server accept is exiting...", pPool);
        break;
      } else {
        uError("TCP accept failure, reason:%s", strerror(errno));
        continue;
      }
    }

    //uDebug("TCP connection from: 0x%x:%d", clientAddr.sin_addr.s_addr, clientAddr.sin_port); 
    taosKeepTcpAlive(connFd);
    (*pInfo->processIncomingConn)(connFd, clientAddr.sin_addr.s_addr);
  }

  taosClose(pPool->acceptFd);
  return NULL;
}

static SThreadObj *taosGetTcpThread(SPoolObj *pPool) {
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
    close(pThread->pollFd);
    free(pThread);
    return NULL;
  }

  uDebug("%p TCP epoll thread is created", pThread);
  pPool->pThread[pPool->nextId] = pThread;
  pPool->nextId++;
  pPool->nextId = pPool->nextId % pPool->info.numOfThreads;

  return pThread;
}

static void taosStopPoolThread(SThreadObj* pThread) {
  pThread->stop = true;
  
  if (pThread->thread == pthread_self()) {
    pthread_detach(pthread_self());
    return;
  }

  // save thread ID into a local variable, since pThread is freed when the thread exits 
  pthread_t thread = pThread->thread;

  // signal the thread to stop, try graceful method first,
  // and use pthread_cancel when failed
  struct epoll_event event = { .events = EPOLLIN };
  eventfd_t fd = eventfd(1, 0);
  if (fd == -1) {
    // failed to create eventfd, call pthread_cancel instead, which may result in data corruption
    uError("failed to create eventfd(%s)", strerror(errno));
    pthread_cancel(pThread->thread);
  } else if (epoll_ctl(pThread->pollFd, EPOLL_CTL_ADD, fd, &event) < 0) {
    // failed to call epoll_ctl, call pthread_cancel instead, which may result in data corruption
    uError("failed to call epoll_ctl(%s)", strerror(errno));
    pthread_cancel(pThread->thread);
  }

  pthread_join(thread, NULL);
  taosClose(fd);
}

