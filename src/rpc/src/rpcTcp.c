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
#include "tsocket.h"
#include "tutil.h"
#include "rpcLog.h"
#include "rpcHead.h"
#include "rpcTcp.h"

#ifndef EPOLLWAKEUP
  #define EPOLLWAKEUP (1u << 29)
#endif

typedef struct SFdObj {
  void              *signature;
  int                fd;       // TCP socket FD
  void              *thandle;  // handle from upper layer, like TAOS
  uint32_t           ip;
  uint16_t           port;
  struct SThreadObj *pThreadObj;
  struct SFdObj     *prev;
  struct SFdObj     *next;
} SFdObj;

typedef struct SThreadObj {
  pthread_t       thread;
  SFdObj *        pHead;
  pthread_mutex_t mutex;
  uint32_t        ip;
  bool            stop;
  int             pollFd;
  int             numOfFds;
  int             threadId;
  char            label[12];
  void           *shandle;  // handle passed by upper layer during server initialization
  void           *(*processData)(SRecvInfo *pPacket);
} SThreadObj;

typedef struct {
  int         fd;
  uint32_t    ip;
  uint16_t    port;
  char        label[12];
  int         numOfThreads;
  void *      shandle;
  SThreadObj *pThreadObj;
  pthread_t   thread;
} SServerObj;

static void   *taosProcessTcpData(void *param);
static SFdObj *taosMallocFdObj(SThreadObj *pThreadObj, int fd);
static void    taosFreeFdObj(SFdObj *pFdObj);
static void    taosReportBrokenLink(SFdObj *pFdObj);
static void*   taosAcceptTcpConnection(void *arg);

void *taosInitTcpServer(uint32_t ip, uint16_t port, char *label, int numOfThreads, void *fp, void *shandle) {
  SServerObj *pServerObj;
  SThreadObj *pThreadObj;

  pServerObj = (SServerObj *)calloc(sizeof(SServerObj), 1);
  pServerObj->ip = ip;
  pServerObj->port = port;
  strcpy(pServerObj->label, label);
  pServerObj->numOfThreads = numOfThreads;

  pServerObj->pThreadObj = (SThreadObj *)calloc(sizeof(SThreadObj), numOfThreads);
  if (pServerObj->pThreadObj == NULL) {
    tError("TCP:%s no enough memory", label);
    free(pServerObj);
    return NULL;
  }

  int code = 0;
  pThreadObj = pServerObj->pThreadObj;
  for (int i = 0; i < numOfThreads; ++i) {
    pThreadObj->processData = fp;
    strcpy(pThreadObj->label, label);
    pThreadObj->shandle = shandle;

    code = pthread_mutex_init(&(pThreadObj->mutex), NULL);
    if (code < 0) {
      tError("%s failed to init TCP process data mutex(%s)", label, strerror(errno));
      break;;
    }

    pThreadObj->pollFd = epoll_create(10);  // size does not matter
    if (pThreadObj->pollFd < 0) {
      tError("%s failed to create TCP epoll", label);
      code = -1;
      break;
    }

    pthread_attr_t thattr;
    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
    code = pthread_create(&(pThreadObj->thread), &thattr, taosProcessTcpData, (void *)(pThreadObj));
    pthread_attr_destroy(&thattr);
    if (code != 0) {
      tError("%s failed to create TCP process data thread(%s)", label, strerror(errno));
      break;
    }

    pThreadObj->threadId = i;
    pThreadObj++;
  }

  if (code == 0) { 
    pthread_attr_t thattr;
    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
    code = pthread_create(&(pServerObj->thread), &thattr, (void *)taosAcceptTcpConnection, (void *)(pServerObj));
    pthread_attr_destroy(&thattr);
    if (code != 0) {
      tError("%s failed to create TCP accept thread(%s)", label, strerror(errno));
    }
  }

  if (code != 0) {
    free(pServerObj->pThreadObj);
    free(pServerObj);
    pServerObj = NULL;
  } else {
    tTrace("%s TCP server is initialized, ip:0x%x port:%hu numOfThreads:%d", label, ip, port, numOfThreads);
  }

  return (void *)pServerObj;
}

static void taosStopTcpThread(SThreadObj* pThreadObj) {
  pThreadObj->stop = true;

  // signal the thread to stop, try graceful method first,
  // and use pthread_cancel when failed
  struct epoll_event event = { .events = EPOLLIN };
  eventfd_t fd = eventfd(1, 0);
  if (fd == -1) {
    pthread_cancel(pThreadObj->thread);
  } else if (epoll_ctl(pThreadObj->pollFd, EPOLL_CTL_ADD, fd, &event) < 0) {
    pthread_cancel(pThreadObj->thread);
  }

  pthread_join(pThreadObj->thread, NULL);
  close(pThreadObj->pollFd);
  if (fd != -1) {
    close(fd);
  }

  while (pThreadObj->pHead) {
    SFdObj *pFdObj = pThreadObj->pHead;
    pThreadObj->pHead = pFdObj->next;
    taosFreeFdObj(pFdObj);
  }
}


void taosCleanUpTcpServer(void *handle) {
  SServerObj *pServerObj = handle;
  SThreadObj *pThreadObj;

  if (pServerObj == NULL) return;

  shutdown(pServerObj->fd, SHUT_RD);
  pthread_join(pServerObj->thread, NULL);

  for (int i = 0; i < pServerObj->numOfThreads; ++i) {
    pThreadObj = pServerObj->pThreadObj + i;
    taosStopTcpThread(pThreadObj);
    pthread_mutex_destroy(&(pThreadObj->mutex));
  }

  tTrace("TCP:%s, TCP server is cleaned up", pServerObj->label);

  tfree(pServerObj->pThreadObj);
  tfree(pServerObj);
}

static void* taosAcceptTcpConnection(void *arg) {
  int                connFd = -1;
  struct sockaddr_in caddr;
  int                threadId = 0;
  SThreadObj        *pThreadObj;
  SServerObj        *pServerObj;

  pServerObj = (SServerObj *)arg;

  pServerObj->fd = taosOpenTcpServerSocket(pServerObj->ip, pServerObj->port);
  if (pServerObj->fd < 0) return NULL; 

  tTrace("%s TCP server is ready, ip:0x%x:%hu", pServerObj->label, pServerObj->ip, pServerObj->port);

  while (1) {
    socklen_t addrlen = sizeof(caddr);
    connFd = accept(pServerObj->fd, (struct sockaddr *)&caddr, &addrlen);
    if (connFd == -1) {
      if (errno == EINVAL) {
        tTrace("%s TCP server socket was shutdown, exiting...", pServerObj->label);
        break;
      }
      tError("%s TCP accept failure(%s)", pServerObj->label, errno, strerror(errno));
      continue;
    }

    tTrace("%s TCP connection from ip:%s:%hu", pServerObj->label, inet_ntoa(caddr.sin_addr), caddr.sin_port);
    taosKeepTcpAlive(connFd);

    // pick up the thread to handle this connection
    pThreadObj = pServerObj->pThreadObj + threadId;

    SFdObj *pFdObj = taosMallocFdObj(pThreadObj, connFd);
    if (pFdObj) {
      pFdObj->ip = caddr.sin_addr.s_addr;
      pFdObj->port = caddr.sin_port;
      tTrace("%s new connection from %s:%hu, FD:%p, numOfFds:%d", pServerObj->label, 
              inet_ntoa(caddr.sin_addr), pFdObj->port, pFdObj, pThreadObj->numOfFds);
    } else {
      close(connFd);
      tError("%s failed to malloc FdObj(%s)", pServerObj->label, strerror(errno));
    }  

    // pick up next thread for next connection
    threadId++;
    threadId = threadId % pServerObj->numOfThreads;
  }

  close(pServerObj->fd);
  return NULL;
}

void *taosInitTcpClient(uint32_t ip, uint16_t port, char *label, int num, void *fp, void *shandle) {
  SThreadObj    *pThreadObj;
  pthread_attr_t thattr;

  pThreadObj = (SThreadObj *)malloc(sizeof(SThreadObj));
  memset(pThreadObj, 0, sizeof(SThreadObj));
  strcpy(pThreadObj->label, label);
  pThreadObj->ip = ip;
  pThreadObj->shandle = shandle;

  if (pthread_mutex_init(&(pThreadObj->mutex), NULL) < 0) {
    tError("%s failed to init TCP client mutex(%s)", label, strerror(errno));
    return NULL;
  }

  pThreadObj->pollFd = epoll_create(10);  // size does not matter
  if (pThreadObj->pollFd < 0) {
    tError("%s failed to create TCP client epoll", label);
    return NULL;
  }

  pThreadObj->processData = fp;

  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  int code = pthread_create(&(pThreadObj->thread), &thattr, taosProcessTcpData, (void *)(pThreadObj));
  pthread_attr_destroy(&thattr);
  if (code != 0) {
    tError("%s failed to create TCP read data thread(%s)", label, strerror(errno));
    return NULL;
  }

  tTrace("%s TCP client is initialized, ip:%s:%hu", label, ip, port);

  return pThreadObj;
}

void taosCleanUpTcpClient(void *chandle) {
  SThreadObj *pThreadObj = chandle;
  if (pThreadObj == NULL) return;

  taosStopTcpThread(pThreadObj);
  tTrace (":%s, all connections are cleaned up", pThreadObj->label);

  tfree(pThreadObj);
}

void *taosOpenTcpClientConnection(void *shandle, void *thandle, uint32_t ip, uint16_t port) {
  SThreadObj *    pThreadObj = shandle;

  int fd = taosOpenTcpClientSocket(ip, port, pThreadObj->ip);
  if (fd <= 0) return NULL;

  SFdObj *pFdObj = taosMallocFdObj(pThreadObj, fd);
  
  if (pFdObj) {
    pFdObj->thandle = thandle;
    pFdObj->port = port;
    pFdObj->ip = ip;
    tTrace("%s %p, TCP connection to 0x%x:%hu is created, FD:%p numOfFds:%d", 
            pThreadObj->label, thandle, ip, port, pFdObj, pThreadObj->numOfFds);
  } else {
    close(fd);
    tError("%s failed to malloc client FdObj(%s)", pThreadObj->label, strerror(errno));
  }

  return pFdObj;
}

void taosCloseTcpConnection(void *chandle) {
  SFdObj *pFdObj = chandle;
  if (pFdObj == NULL) return;

  taosFreeFdObj(pFdObj);
}

int taosSendTcpData(uint32_t ip, uint16_t port, void *data, int len, void *chandle) {
  SFdObj *pFdObj = chandle;

  if (chandle == NULL) return -1;

  return (int)send(pFdObj->fd, data, (size_t)len, 0);
}

static void taosReportBrokenLink(SFdObj *pFdObj) {

  SThreadObj *pThreadObj = pFdObj->pThreadObj;

  // notify the upper layer, so it will clean the associated context
  if (pFdObj->thandle) {
    SRecvInfo recvInfo;
    recvInfo.msg = NULL;
    recvInfo.msgLen = 0;
    recvInfo.ip = 0;
    recvInfo.port = 0;
    recvInfo.shandle = pThreadObj->shandle;
    recvInfo.thandle = pFdObj->thandle;;
    recvInfo.chandle = NULL;
    recvInfo.connType = RPC_CONN_TCP;
    (*(pThreadObj->processData))(&recvInfo);
  } 
}

#define maxEvents 10

static void *taosProcessTcpData(void *param) {
  SThreadObj        *pThreadObj = param;
  SFdObj            *pFdObj;
  struct epoll_event events[maxEvents];
  SRecvInfo          recvInfo;
  SRpcHead           rpcHead;

  while (1) {
    int fdNum = epoll_wait(pThreadObj->pollFd, events, maxEvents, -1);
    if (pThreadObj->stop) {
      tTrace("%s, tcp thread get stop event, exiting...", pThreadObj->label);
      break;
    }
    if (fdNum < 0) continue;

    for (int i = 0; i < fdNum; ++i) {
      pFdObj = events[i].data.ptr;

      if (events[i].events & EPOLLERR) {
        tTrace("%s %p, error happened on FD", pThreadObj->label, pFdObj->thandle);
        taosReportBrokenLink(pFdObj);
        continue;
      }

      if (events[i].events & EPOLLHUP) {
        tTrace("%s %p, FD hang up", pThreadObj->label, pFdObj->thandle);
        taosReportBrokenLink(pFdObj);
        continue;
      }

      int32_t headLen = taosReadMsg(pFdObj->fd, &rpcHead, sizeof(SRpcHead));
      if (headLen != sizeof(SRpcHead)) {
        tError("%s %p, read error, headLen:%d", pThreadObj->label, pFdObj->thandle, headLen);
        taosReportBrokenLink(pFdObj);
        continue;
      }

      int32_t msgLen = (int32_t)htonl((uint32_t)rpcHead.msgLen);
      char   *buffer = malloc(msgLen + tsRpcOverhead);
      if ( NULL == buffer) {
        tError("%s %p, TCP malloc(size:%d) fail", pThreadObj->label, pFdObj->thandle, msgLen);
        taosReportBrokenLink(pFdObj);
        continue;
      }

      char   *msg = buffer + tsRpcOverhead;
      int32_t leftLen = msgLen - headLen;
      int32_t retLen = taosReadMsg(pFdObj->fd, msg + headLen, leftLen);

      if (leftLen != retLen) {
        tError("%s %p, read error, leftLen:%d retLen:%d", 
                pThreadObj->label, pFdObj->thandle, leftLen, retLen);
        taosReportBrokenLink(pFdObj);
        tfree(buffer);
        continue;
      }

      // tTrace("%s TCP data is received, ip:0x%x:%u len:%d", pThreadObj->label, pFdObj->ip, pFdObj->port, msgLen);

      memcpy(msg, &rpcHead, sizeof(SRpcHead));
      recvInfo.msg = msg;
      recvInfo.msgLen = msgLen;
      recvInfo.ip = pFdObj->ip;
      recvInfo.port = pFdObj->port;
      recvInfo.shandle = pThreadObj->shandle;
      recvInfo.thandle = pFdObj->thandle;;
      recvInfo.chandle = pFdObj;
      recvInfo.connType = RPC_CONN_TCP;

      pFdObj->thandle = (*(pThreadObj->processData))(&recvInfo);
      if (pFdObj->thandle == NULL) taosFreeFdObj(pFdObj);
    }
  }

  return NULL;
}

static SFdObj *taosMallocFdObj(SThreadObj *pThreadObj, int fd) {
  struct epoll_event event;

  SFdObj *pFdObj = (SFdObj *)calloc(sizeof(SFdObj), 1);
  if (pFdObj == NULL) return NULL;

  pFdObj->fd = fd;
  pFdObj->pThreadObj = pThreadObj;
  pFdObj->signature = pFdObj;

  event.events = EPOLLIN | EPOLLPRI | EPOLLWAKEUP;
  event.data.ptr = pFdObj;
  if (epoll_ctl(pThreadObj->pollFd, EPOLL_CTL_ADD, fd, &event) < 0) {
    tfree(pFdObj);
    return NULL;
  }

  // notify the data process, add into the FdObj list
  pthread_mutex_lock(&(pThreadObj->mutex));
  pFdObj->next = pThreadObj->pHead;
  if (pThreadObj->pHead) (pThreadObj->pHead)->prev = pFdObj;
  pThreadObj->pHead = pFdObj;
  pThreadObj->numOfFds++;
  pthread_mutex_unlock(&(pThreadObj->mutex));

  return pFdObj;
}

static void taosFreeFdObj(SFdObj *pFdObj) {

  if (pFdObj == NULL) return;
  if (pFdObj->signature != pFdObj) return;

  SThreadObj *pThreadObj = pFdObj->pThreadObj;
  pthread_mutex_lock(&pThreadObj->mutex);

  if (pFdObj->signature == NULL) {
    pthread_mutex_unlock(&pThreadObj->mutex);
    return;
  }

  pFdObj->signature = NULL;
  epoll_ctl(pThreadObj->pollFd, EPOLL_CTL_DEL, pFdObj->fd, NULL);
  close(pFdObj->fd);

  pThreadObj->numOfFds--;

  if (pThreadObj->numOfFds < 0)
    tError("%s %p, TCP thread:%d, number of FDs is negative!!!", 
            pThreadObj->label, pFdObj->thandle, pThreadObj->threadId);

  // remove from the FdObject list

  if (pFdObj->prev) {
    (pFdObj->prev)->next = pFdObj->next;
  } else {
    pThreadObj->pHead = pFdObj->next;
  }

  if (pFdObj->next) {
    (pFdObj->next)->prev = pFdObj->prev;
  }

  pthread_mutex_unlock(&pThreadObj->mutex);

  tTrace("%s %p, FD:%p is cleaned, numOfFds:%d", 
          pThreadObj->label, pFdObj->thandle, pFdObj, pThreadObj->numOfFds);

  tfree(pFdObj);
}
