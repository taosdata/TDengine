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
#include "ttcpserver.h"
#include "tutil.h"

#define TAOS_IPv4ADDR_LEN 16
#ifndef EPOLLWAKEUP
  #define EPOLLWAKEUP (1u << 29)
#endif

typedef struct _fd_obj {
  int                 fd;       // TCP socket FD
  void *              thandle;  // handle from upper layer, like TAOS
  char                ipstr[TAOS_IPv4ADDR_LEN];
  unsigned int        ip;
  unsigned short      port;
  struct _thread_obj *pThreadObj;
  struct _fd_obj *    prev, *next;
} SFdObj;

typedef struct _thread_obj {
  pthread_t       thread;
  SFdObj *        pHead;
  pthread_mutex_t threadMutex;
  pthread_cond_t  fdReady;
  int             pollFd;
  int             numOfFds;
  int             threadId;
  char            label[12];
  // char    buffer[128000];  // buffer to receive data
  void *shandle;  // handle passed by upper layer during server initialization
  void *(*processData)(char *data, int dataLen, unsigned int ip, short port, void *shandle, void *thandle,
                       void *chandle);
} SThreadObj;

typedef struct {
  char        ip[40];
  short       port;
  char        label[12];
  int         numOfThreads;
  void *      shandle;
  SThreadObj *pThreadObj;
  pthread_t   thread;
} SServerObj;

static void taosCleanUpFdObj(SFdObj *pFdObj) {
  SThreadObj *pThreadObj;

  if (pFdObj == NULL) return;

  pThreadObj = pFdObj->pThreadObj;
  if (pThreadObj == NULL) {
    tError("FdObj double clean up!!!");
    return;
  }

  epoll_ctl(pThreadObj->pollFd, EPOLL_CTL_DEL, pFdObj->fd, NULL);
  close(pFdObj->fd);

  pthread_mutex_lock(&pThreadObj->threadMutex);

  pThreadObj->numOfFds--;

  if (pThreadObj->numOfFds < 0)
    tError("%s TCP thread:%d, number of FDs shall never be negative", pThreadObj->label, pThreadObj->threadId);

  // remove from the FdObject list

  if (pFdObj->prev) {
    (pFdObj->prev)->next = pFdObj->next;
  } else {
    pThreadObj->pHead = pFdObj->next;
  }

  if (pFdObj->next) {
    (pFdObj->next)->prev = pFdObj->prev;
  }

  pthread_mutex_unlock(&pThreadObj->threadMutex);

  // notify the upper layer, so it will clean the associated context
  if (pFdObj->thandle) (*(pThreadObj->processData))(NULL, 0, 0, 0, pThreadObj->shandle, pFdObj->thandle, NULL);

  tTrace("%s TCP thread:%d, FD is cleaned up, numOfFds:%d", pThreadObj->label, pThreadObj->threadId,
         pThreadObj->numOfFds);

  memset(pFdObj, 0, sizeof(SFdObj));

  tfree(pFdObj);
}

void taosCloseTcpServerConnection(void *chandle) {
  SFdObj *pFdObj = (SFdObj *)chandle;

  if (pFdObj == NULL) return;

  taosCleanUpFdObj(pFdObj);
}

void taosCleanUpTcpServer(void *handle) {
  int         i;
  SThreadObj *pThreadObj;
  SServerObj *pServerObj = (SServerObj *)handle;

  if (pServerObj == NULL) return;

  pthread_cancel(pServerObj->thread);
  pthread_join(pServerObj->thread, NULL);

  for (i = 0; i < pServerObj->numOfThreads; ++i) {
    pThreadObj = pServerObj->pThreadObj + i;

    while (pThreadObj->pHead) {
      taosCleanUpFdObj(pThreadObj->pHead);
      pThreadObj->pHead = pThreadObj->pHead;
    }

    close(pThreadObj->pollFd);
    pthread_cancel(pThreadObj->thread);
    pthread_join(pThreadObj->thread, NULL);
    pthread_cond_destroy(&(pThreadObj->fdReady));
    pthread_mutex_destroy(&(pThreadObj->threadMutex));
  }

  tfree(pServerObj->pThreadObj);
  tTrace("TCP:%s, TCP server is cleaned up", pServerObj->label);

  tfree(pServerObj);
}

#define maxEvents 10

static void taosProcessTcpData(void *param) {
  SThreadObj *       pThreadObj;
  int                i, fdNum;
  SFdObj *           pFdObj;
  struct epoll_event events[maxEvents];

  pThreadObj = (SThreadObj *)param;

  while (1) {
    pthread_mutex_lock(&pThreadObj->threadMutex);
    if (pThreadObj->numOfFds < 1) {
      pthread_cond_wait(&pThreadObj->fdReady, &pThreadObj->threadMutex);
    }
    pthread_mutex_unlock(&pThreadObj->threadMutex);

    fdNum = epoll_wait(pThreadObj->pollFd, events, maxEvents, -1);
    if (fdNum < 0) continue;

    for (i = 0; i < fdNum; ++i) {
      pFdObj = events[i].data.ptr;

      if (events[i].events & EPOLLERR) {
        tTrace("%s TCP thread:%d, error happened on FD", pThreadObj->label, pThreadObj->threadId);
        taosCleanUpFdObj(pFdObj);
        continue;
      }

      if (events[i].events & EPOLLHUP) {
        tTrace("%s TCP thread:%d, FD hang up", pThreadObj->label, pThreadObj->threadId);
        taosCleanUpFdObj(pFdObj);
        continue;
      }

      void *buffer = malloc(1024);
      int   headLen = taosReadMsg(pFdObj->fd, buffer, sizeof(STaosHeader));
      if (headLen != sizeof(STaosHeader)) {
        tError("%s read error, headLen:%d", pThreadObj->label, headLen);
        taosCleanUpFdObj(pFdObj);
        tfree(buffer);
        continue;
      }

      int dataLen = (int32_t)htonl((uint32_t)((STaosHeader *)buffer)->msgLen);
      if (dataLen > 1024) buffer = realloc(buffer, (size_t)dataLen);

      int leftLen = dataLen - headLen;
      int retLen = taosReadMsg(pFdObj->fd, buffer + headLen, leftLen);

      // tTrace("%s TCP data is received, ip:%s port:%u len:%d",
      // pThreadObj->label, pFdObj->ipstr, pFdObj->port, dataLen);

      if (leftLen != retLen) {
        tError("%s read error, leftLen:%d retLen:%d", pThreadObj->label, leftLen, retLen);
        taosCleanUpFdObj(pFdObj);
        tfree(buffer);
        continue;
      }

      pFdObj->thandle = (*(pThreadObj->processData))(buffer, dataLen, pFdObj->ip, (int16_t)pFdObj->port,
                                                     pThreadObj->shandle, pFdObj->thandle, pFdObj);

      if (pFdObj->thandle == NULL) taosCleanUpFdObj(pFdObj);
    }
  }
}

void taosAcceptTcpConnection(void *arg) {
  int                connFd = -1;
  struct sockaddr_in clientAddr;
  int                sockFd;
  int                threadId = 0;
  SThreadObj *       pThreadObj;
  SServerObj *       pServerObj;
  SFdObj *           pFdObj;
  struct epoll_event event;

  pServerObj = (SServerObj *)arg;

  sockFd = taosOpenTcpServerSocket(pServerObj->ip, pServerObj->port);

  if (sockFd < 0) {
    tError("%s failed to open TCP socket, ip:%s, port:%u", pServerObj->label, pServerObj->ip, pServerObj->port);
    return;
  } else {
    tTrace("%s TCP server is ready, ip:%s, port:%u", pServerObj->label, pServerObj->ip, pServerObj->port);
  }

  while (1) {
    socklen_t addrlen = sizeof(clientAddr);
    connFd = accept(sockFd, (struct sockaddr *)&clientAddr, &addrlen);

    if (connFd < 0) {
      tError("%s TCP accept failure, errno:%d, reason:%s", pServerObj->label, errno, strerror(errno));
      continue;
    }

    tTrace("%s TCP connection from ip:%s port:%u", pServerObj->label, inet_ntoa(clientAddr.sin_addr),
           htons(clientAddr.sin_port));
    taosKeepTcpAlive(connFd);

    // pick up the thread to handle this connection
    pThreadObj = pServerObj->pThreadObj + threadId;

    pFdObj = (SFdObj *)malloc(sizeof(SFdObj));
    if (pFdObj == NULL) {
      tError("%s no enough resource to allocate TCP FD IDs", pServerObj->label);
      close(connFd);
      continue;
    }

    memset(pFdObj, 0, sizeof(SFdObj));
    pFdObj->fd = connFd;
    strcpy(pFdObj->ipstr, inet_ntoa(clientAddr.sin_addr));
    pFdObj->ip = clientAddr.sin_addr.s_addr;
    pFdObj->port = htons(clientAddr.sin_port);
    pFdObj->pThreadObj = pThreadObj;

    event.events = EPOLLIN | EPOLLPRI | EPOLLWAKEUP;
    event.data.ptr = pFdObj;
    if (epoll_ctl(pThreadObj->pollFd, EPOLL_CTL_ADD, connFd, &event) < 0) {
      tError("%s failed to add TCP FD for epoll, error:%s", pServerObj->label, strerror(errno));
      tfree(pFdObj);
      close(connFd);
      continue;
    }

    // notify the data process, add into the FdObj list
    pthread_mutex_lock(&(pThreadObj->threadMutex));

    pFdObj->next = pThreadObj->pHead;

    if (pThreadObj->pHead) (pThreadObj->pHead)->prev = pFdObj;

    pThreadObj->pHead = pFdObj;

    pThreadObj->numOfFds++;
    pthread_cond_signal(&pThreadObj->fdReady);

    pthread_mutex_unlock(&(pThreadObj->threadMutex));

    tTrace("%s TCP thread:%d, a new connection, ip:%s port:%u, numOfFds:%d", pServerObj->label, pThreadObj->threadId,
           pFdObj->ipstr, pFdObj->port, pThreadObj->numOfFds);

    // pick up next thread for next connection
    threadId++;
    threadId = threadId % pServerObj->numOfThreads;
  }
}

void taosAcceptUDConnection(void *arg) {
  int                connFd = -1;
  int                sockFd;
  int                threadId = 0;
  SThreadObj *       pThreadObj;
  SServerObj *       pServerObj;
  SFdObj *           pFdObj;
  struct epoll_event event;

  pServerObj = (SServerObj *)arg;
  sockFd = taosOpenUDServerSocket(pServerObj->ip, pServerObj->port);

  if (sockFd < 0) {
    tError("%s failed to open UD socket, ip:%s, port:%u", pServerObj->label, pServerObj->ip, pServerObj->port);
    return;
  } else {
    tTrace("%s UD server is ready, ip:%s, port:%u", pServerObj->label, pServerObj->ip, pServerObj->port);
  }

  while (1) {
    connFd = accept(sockFd, NULL, NULL);

    if (connFd < 0) {
      tError("%s UD accept failure, errno:%d, reason:%s", pServerObj->label, errno, strerror(errno));
      continue;
    }

    // pick up the thread to handle this connection
    pThreadObj = pServerObj->pThreadObj + threadId;

    pFdObj = (SFdObj *)malloc(sizeof(SFdObj));
    if (pFdObj == NULL) {
      tError("%s no enough resource to allocate TCP FD IDs", pServerObj->label);
      close(connFd);
      continue;
    }

    memset(pFdObj, 0, sizeof(SFdObj));
    pFdObj->fd = connFd;
    pFdObj->pThreadObj = pThreadObj;

    event.events = EPOLLIN | EPOLLPRI | EPOLLWAKEUP;
    event.data.ptr = pFdObj;
    if (epoll_ctl(pThreadObj->pollFd, EPOLL_CTL_ADD, connFd, &event) < 0) {
      tError("%s failed to add UD FD for epoll, error:%s", pServerObj->label, strerror(errno));
      tfree(pFdObj);
      close(connFd);
      continue;
    }

    // notify the data process, add into the FdObj list
    pthread_mutex_lock(&(pThreadObj->threadMutex));

    pFdObj->next = pThreadObj->pHead;

    if (pThreadObj->pHead) (pThreadObj->pHead)->prev = pFdObj;

    pThreadObj->pHead = pFdObj;

    pThreadObj->numOfFds++;
    pthread_cond_signal(&pThreadObj->fdReady);

    pthread_mutex_unlock(&(pThreadObj->threadMutex));

    tTrace("%s UD thread:%d, a new connection, numOfFds:%d", pServerObj->label, pThreadObj->threadId,
           pThreadObj->numOfFds);

    // pick up next thread for next connection
    threadId++;
    threadId = threadId % pServerObj->numOfThreads;
  }
}

void *taosInitTcpServer(char *ip, short port, char *label, int numOfThreads, void *fp, void *shandle) {
  int            i;
  SServerObj *   pServerObj;
  pthread_attr_t thattr;
  SThreadObj *   pThreadObj;

  pServerObj = (SServerObj *)malloc(sizeof(SServerObj));
  strcpy(pServerObj->ip, ip);
  pServerObj->port = port;
  strcpy(pServerObj->label, label);
  pServerObj->numOfThreads = numOfThreads;

  pServerObj->pThreadObj = (SThreadObj *)malloc(sizeof(SThreadObj) * (size_t)numOfThreads);
  if (pServerObj->pThreadObj == NULL) {
    tError("TCP:%s no enough memory", label);
    return NULL;
  }
  memset(pServerObj->pThreadObj, 0, sizeof(SThreadObj) * (size_t)numOfThreads);

  pThreadObj = pServerObj->pThreadObj;
  for (i = 0; i < numOfThreads; ++i) {
    pThreadObj->processData = fp;
    strcpy(pThreadObj->label, label);
    pThreadObj->shandle = shandle;

    if (pthread_mutex_init(&(pThreadObj->threadMutex), NULL) < 0) {
      tError("%s failed to init TCP process data mutex, reason:%s", label, strerror(errno));
      return NULL;
    }

    if (pthread_cond_init(&(pThreadObj->fdReady), NULL) != 0) {
      tError("%s init TCP condition variable failed, reason:%s\n", label, strerror(errno));
      return NULL;
    }

    pThreadObj->pollFd = epoll_create(10);  // size does not matter
    if (pThreadObj->pollFd < 0) {
      tError("%s failed to create TCP epoll", label);
      return NULL;
    }

    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
    if (pthread_create(&(pThreadObj->thread), &thattr, (void *)taosProcessTcpData, (void *)(pThreadObj)) != 0) {
      tError("%s failed to create TCP process data thread, reason:%s", label, strerror(errno));
      return NULL;
    }

    pThreadObj->threadId = i;
    pThreadObj++;
  }

  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  if (pthread_create(&(pServerObj->thread), &thattr, (void *)taosAcceptTcpConnection, (void *)(pServerObj)) != 0) {
    tError("%s failed to create TCP accept thread, reason:%s", label, strerror(errno));
    return NULL;
  }

  /*
    if ( pthread_create(&(pServerObj->thread), &thattr,
    (void*)taosAcceptUDConnection, (void *)(pServerObj)) != 0 ) {
      tError("%s failed to create UD accept thread, reason:%s", label,
    strerror(errno));
      return NULL;
    }
  */
  pthread_attr_destroy(&thattr);
  tTrace("%s TCP server is initialized, ip:%s port:%u numOfThreads:%d", label, ip, port, numOfThreads);

  return (void *)pServerObj;
}

void taosListTcpConnection(void *handle, char *buffer) {
  SServerObj *pServerObj;
  SThreadObj *pThreadObj;
  SFdObj *    pFdObj;
  int         i, numOfFds, numOfConns;
  char *      msg;

  pServerObj = (SServerObj *)handle;
  buffer[0] = 0;
  msg = buffer;
  numOfConns = 0;

  pThreadObj = pServerObj->pThreadObj;

  for (i = 0; i < pServerObj->numOfThreads; ++i) {
    numOfFds = 0;
    sprintf(msg, "TCP:%s Thread:%d number of connections:%d\n", pServerObj->label, pThreadObj->threadId,
            pThreadObj->numOfFds);
    msg = msg + strlen(msg);
    pFdObj = pThreadObj->pHead;
    while (pFdObj) {
      sprintf("   ip:%s port:%u\n", pFdObj->ipstr, pFdObj->port);
      msg = msg + strlen(msg);
      numOfFds++;
      numOfConns++;
      pFdObj = pFdObj->next;
    }

    if (numOfFds != pThreadObj->numOfFds)
      tError("TCP:%s thread:%d BIG error, numOfFds:%d actual numOfFds:%d", pServerObj->label, pThreadObj->threadId,
             pThreadObj->numOfFds, numOfFds);

    pThreadObj++;
  }

  sprintf(msg, "TCP:%s total connections:%d\n", pServerObj->label, numOfConns);

  return;
}

int taosSendTcpServerData(uint32_t ip, short port, char *data, int len, void *chandle) {
  SFdObj *pFdObj = (SFdObj *)chandle;

  if (chandle == NULL) return -1;

  return (int)send(pFdObj->fd, data, (size_t)len, 0);
}
