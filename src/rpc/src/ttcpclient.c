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
#include "ttcpclient.h"
#include "tutil.h"

#ifndef EPOLLWAKEUP
  #define EPOLLWAKEUP (1u << 29)
#endif

typedef struct _tcp_fd {
  int                 fd;  // TCP socket FD
  void *              thandle;
  uint32_t            ip;
  char                ipstr[20];
  short               port;
  struct _tcp_client *pTcp;
  struct _tcp_fd *    prev, *next;
} STcpFd;

typedef struct _tcp_client {
  pthread_t       thread;
  STcpFd *        pHead;
  pthread_mutex_t mutex;
  pthread_cond_t  fdReady;
  int             pollFd;
  int             numOfFds;
  char            label[12];
  char            ipstr[20];
  void *          shandle;  // handle passed by upper layer during server initialization
  void *(*processData)(char *data, int dataLen, unsigned int ip, short port, void *shandle, void *thandle,
                       void *chandle);
  // char   buffer[128000];
} STcpClient;

#define maxTcpEvents 100

static void taosCleanUpTcpFdObj(STcpFd *pFdObj) {
  STcpClient *pTcp;

  if (pFdObj == NULL) return;

  pTcp = pFdObj->pTcp;
  if (pTcp == NULL) {
    tError("double free TcpFdObj!!!!");
    return;
  }

  epoll_ctl(pTcp->pollFd, EPOLL_CTL_DEL, pFdObj->fd, NULL);
  close(pFdObj->fd);

  pthread_mutex_lock(&pTcp->mutex);

  pTcp->numOfFds--;

  if (pTcp->numOfFds < 0) tError("%s number of TCP FDs shall never be negative", pTcp->label);

  // remove from the FdObject list

  if (pFdObj->prev) {
    (pFdObj->prev)->next = pFdObj->next;
  } else {
    pTcp->pHead = pFdObj->next;
  }

  if (pFdObj->next) {
    (pFdObj->next)->prev = pFdObj->prev;
  }

  pthread_mutex_unlock(&pTcp->mutex);

  // notify the upper layer to clean the associated context
  if (pFdObj->thandle) (*(pTcp->processData))(NULL, 0, 0, 0, pTcp->shandle, pFdObj->thandle, NULL);

  tTrace("%s TCP FD is cleaned up, numOfFds:%d", pTcp->label, pTcp->numOfFds);

  memset(pFdObj, 0, sizeof(STcpFd));

  tfree(pFdObj);
}

void taosCleanUpTcpClient(void *chandle) {
  STcpClient *pTcp = (STcpClient *)chandle;
  if (pTcp == NULL) return;

  while (pTcp->pHead) {
    taosCleanUpTcpFdObj(pTcp->pHead);
    pTcp->pHead = pTcp->pHead->next;
  }

  close(pTcp->pollFd);

  pthread_cancel(pTcp->thread);
  pthread_join(pTcp->thread, NULL);

  // tTrace (":%s, all connections are cleaned up", pTcp->label);

  tfree(pTcp);
}

static void *taosReadTcpData(void *param) {
  STcpClient *       pTcp = (STcpClient *)param;
  int                i, fdNum;
  STcpFd *           pFdObj;
  struct epoll_event events[maxTcpEvents];

  while (1) {
    pthread_mutex_lock(&pTcp->mutex);
    if (pTcp->numOfFds < 1) pthread_cond_wait(&pTcp->fdReady, &pTcp->mutex);
    pthread_mutex_unlock(&pTcp->mutex);

    fdNum = epoll_wait(pTcp->pollFd, events, maxTcpEvents, -1);
    if (fdNum < 0) continue;

    for (i = 0; i < fdNum; ++i) {
      pFdObj = events[i].data.ptr;

      if (events[i].events & EPOLLERR) {
        tTrace("%s TCP error happened on FD\n", pTcp->label);
        taosCleanUpTcpFdObj(pFdObj);
        continue;
      }

      if (events[i].events & EPOLLHUP) {
        tTrace("%s TCP FD hang up\n", pTcp->label);
        taosCleanUpTcpFdObj(pFdObj);
        continue;
      }

      void *buffer = malloc(1024);
      int   headLen = taosReadMsg(pFdObj->fd, buffer, sizeof(STaosHeader));
      if (headLen != sizeof(STaosHeader)) {
        tError("%s read error, headLen:%d", pTcp->label, headLen);
        taosCleanUpTcpFdObj(pFdObj);
        continue;
      }

      int dataLen = (int32_t)htonl((uint32_t)((STaosHeader *)buffer)->msgLen);
      if (dataLen > 1024) buffer = realloc(buffer, (size_t)dataLen);

      int leftLen = dataLen - headLen;
      int retLen = taosReadMsg(pFdObj->fd, buffer + headLen, leftLen);

      //tTrace("%s TCP data is received, ip:%s port:%u len:%d", pTcp->label, pFdObj->ipstr, pFdObj->port, dataLen);

      if (leftLen != retLen) {
        tError("%s read error, leftLen:%d retLen:%d", pTcp->label, leftLen, retLen);
        taosCleanUpTcpFdObj(pFdObj);
        continue;
      }

      pFdObj->thandle =
          (*(pTcp->processData))(buffer, dataLen, pFdObj->ip, pFdObj->port, pTcp->shandle, pFdObj->thandle, pFdObj);

      if (pFdObj->thandle == NULL) taosCleanUpTcpFdObj(pFdObj);
    }
  }

  return NULL;
}

void *taosInitTcpClient(char *ip, short port, char *label, int num, void *fp, void *shandle) {
  STcpClient *   pTcp;
  pthread_attr_t thattr;

  pTcp = (STcpClient *)malloc(sizeof(STcpClient));
  memset(pTcp, 0, sizeof(STcpClient));
  strcpy(pTcp->label, label);
  strcpy(pTcp->ipstr, ip);
  pTcp->shandle = shandle;

  if (pthread_mutex_init(&(pTcp->mutex), NULL) < 0) {
    tError("%s failed to init TCP mutex, reason:%s", label, strerror(errno));
    return NULL;
  }

  if (pthread_cond_init(&(pTcp->fdReady), NULL) != 0) {
    tError("%s init TCP condition variable failed, reason:%s\n", label, strerror(errno));
    return NULL;
  }

  pTcp->pollFd = epoll_create(10);  // size does not matter
  if (pTcp->pollFd < 0) {
    tError("%s failed to create TCP epoll", label);
    return NULL;
  }

  pTcp->processData = fp;

  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  if (pthread_create(&(pTcp->thread), &thattr, taosReadTcpData, (void *)(pTcp)) != 0) {
    tError("%s failed to create TCP read data thread, reason:%s", label, strerror(errno));
    return NULL;
  }

  tTrace("%s TCP client is initialized, ip:%s port:%u", label, ip, port);

  return pTcp;
}

void taosCloseTcpClientConnection(void *chandle) {
  STcpFd *pFdObj = (STcpFd *)chandle;

  if (pFdObj == NULL) return;

  taosCleanUpTcpFdObj(pFdObj);
}

void *taosOpenTcpClientConnection(void *shandle, void *thandle, char *ip, short port) {
  STcpClient *       pTcp = (STcpClient *)shandle;
  STcpFd *           pFdObj;
  struct epoll_event event;
  struct in_addr     destIp;
  int                fd;

  /*
    if ( (strcmp(ip, "127.0.0.1") == 0 ) || (strcmp(ip, "localhost") == 0 ) ) {
      fd = taosOpenUDClientSocket(ip, port);
    } else {
      fd = taosOpenTcpClientSocket(ip, port, pTcp->ipstr);
    }
  */

  fd = taosOpenTcpClientSocket(ip, port, pTcp->ipstr);

  if (fd <= 0) return NULL;

  pFdObj = (STcpFd *)malloc(sizeof(STcpFd));
  if (pFdObj == NULL) {
    tError("%s no enough resource to allocate TCP FD IDs", pTcp->label);
    tclose(fd);
    return NULL;
  }

  memset(pFdObj, 0, sizeof(STcpFd));
  pFdObj->fd = fd;
  strcpy(pFdObj->ipstr, ip);
  inet_aton(ip, &destIp);
  pFdObj->ip = destIp.s_addr;
  pFdObj->port = port;
  pFdObj->pTcp = pTcp;
  pFdObj->thandle = thandle;

  event.events = EPOLLIN | EPOLLPRI | EPOLLWAKEUP;
  event.data.ptr = pFdObj;
  if (epoll_ctl(pTcp->pollFd, EPOLL_CTL_ADD, fd, &event) < 0) {
    tError("%s failed to add TCP FD for epoll, error:%s", pTcp->label, strerror(errno));
    tfree(pFdObj);
    tclose(fd);
    return NULL;
  }

  // notify the data process, add into the FdObj list
  pthread_mutex_lock(&(pTcp->mutex));

  pFdObj->next = pTcp->pHead;

  if (pTcp->pHead) (pTcp->pHead)->prev = pFdObj;

  pTcp->pHead = pFdObj;

  pTcp->numOfFds++;
  pthread_cond_signal(&pTcp->fdReady);

  pthread_mutex_unlock(&(pTcp->mutex));

  tTrace("%s TCP connection to ip:%s port:%u is created, numOfFds:%d", pTcp->label, ip, port, pTcp->numOfFds);

  return pFdObj;
}

int taosSendTcpClientData(uint32_t ip, short port, char *data, int len, void *chandle) {
  STcpFd *pFdObj = (STcpFd *)chandle;

  if (chandle == NULL) return -1;

  return (int)send(pFdObj->fd, data, (size_t)len, 0);
}
