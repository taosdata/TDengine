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
#include "taosdef.h"
#include "taoserror.h" 
#include "rpcLog.h"
#include "rpcHead.h"
#include "rpcTcp.h"
#ifdef WINDOWS
#include "wepoll.h"
#endif

#ifndef EPOLLWAKEUP
  #define EPOLLWAKEUP (1u << 29)
#endif

typedef struct SFdObj {
  void              *signature;
  SOCKET             fd;          // TCP socket FD
  int                closedByApp; // 1: already closed by App
  void              *thandle;     // handle from upper layer, like TAOS
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
  SOCKET          pollFd;
  int             numOfFds;
  int             threadId;
  char            label[TSDB_LABEL_LEN];
  void           *shandle;  // handle passed by upper layer during server initialization
  void           *(*processData)(SRecvInfo *pPacket);
} SThreadObj;

typedef struct {
  SOCKET      fd;
  uint32_t    ip;
  uint16_t    port;
  char        label[TSDB_LABEL_LEN];
  int         numOfThreads;
  void *      shandle;
  SThreadObj **pThreadObj;
  pthread_t   thread;
} SServerObj;

static void   *taosProcessTcpData(void *param);
static SFdObj *taosMallocFdObj(SThreadObj *pThreadObj, SOCKET fd);
static void    taosFreeFdObj(SFdObj *pFdObj);
static void    taosReportBrokenLink(SFdObj *pFdObj);
static void   *taosAcceptTcpConnection(void *arg);

void *taosInitTcpServer(uint32_t ip, uint16_t port, char *label, int numOfThreads, void *fp, void *shandle) {
  SServerObj *pServerObj;
  SThreadObj *pThreadObj;

  pServerObj = (SServerObj *)calloc(sizeof(SServerObj), 1);
  if (pServerObj == NULL) {
    tError("TCP:%s no enough memory", label);
    terrno = TAOS_SYSTEM_ERROR(errno); 
    return NULL;
  }

  pServerObj->fd = -1;
  taosResetPthread(&pServerObj->thread);
  pServerObj->ip = ip;
  pServerObj->port = port;
  tstrncpy(pServerObj->label, label, sizeof(pServerObj->label));
  pServerObj->numOfThreads = numOfThreads;

  pServerObj->pThreadObj = (SThreadObj **)calloc(sizeof(SThreadObj *), numOfThreads);
  if (pServerObj->pThreadObj == NULL) {
    tError("TCP:%s no enough memory", label);
    terrno = TAOS_SYSTEM_ERROR(errno); 
    free(pServerObj);
    return NULL;
  }

  int code = 0;
  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

  // initialize parameters in case it may encounter error later 
  for (int i = 0; i < numOfThreads; ++i) {
    pThreadObj = (SThreadObj *)calloc(sizeof(SThreadObj), 1);
    if (pThreadObj == NULL) {
      tError("TCP:%s no enough memory", label);
      terrno = TAOS_SYSTEM_ERROR(errno); 
      for (int j=0; j<i; ++j) free(pServerObj->pThreadObj[j]);
      free(pServerObj->pThreadObj);
      free(pServerObj);
      return NULL;
    }
      
    pServerObj->pThreadObj[i] = pThreadObj;
    pThreadObj->pollFd = -1;
    taosResetPthread(&pThreadObj->thread);
    pThreadObj->processData = fp;
    tstrncpy(pThreadObj->label, label, sizeof(pThreadObj->label));
    pThreadObj->shandle = shandle;
  }

  // initialize mutex, thread, fd which may fail
  for (int i = 0; i < numOfThreads; ++i) {
    pThreadObj = pServerObj->pThreadObj[i];
    code = pthread_mutex_init(&(pThreadObj->mutex), NULL);
    if (code < 0) {
      tError("%s failed to init TCP process data mutex(%s)", label, strerror(errno));
      break;
    }

    pThreadObj->pollFd = (int64_t)epoll_create(10);  // size does not matter
    if (pThreadObj->pollFd < 0) {
      tError("%s failed to create TCP epoll", label);
      code = -1;
      break;
    }

    code = pthread_create(&(pThreadObj->thread), &thattr, taosProcessTcpData, (void *)(pThreadObj));
    if (code != 0) {
      tError("%s failed to create TCP process data thread(%s)", label, strerror(errno));
      break;
    }

    pThreadObj->threadId = i;
  }

  pServerObj->fd = taosOpenTcpServerSocket(pServerObj->ip, pServerObj->port);
  if (pServerObj->fd < 0) code = -1; 

  if (code == 0) { 
    code = pthread_create(&pServerObj->thread, &thattr, taosAcceptTcpConnection, (void *)pServerObj);
    if (code != 0) {
      tError("%s failed to create TCP accept thread(%s)", label, strerror(code));
    }
  }

  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno); 
    taosCleanUpTcpServer(pServerObj);
    pServerObj = NULL;
  } else {
    tDebug("%s TCP server is initialized, ip:0x%x port:%hu numOfThreads:%d", label, ip, port, numOfThreads);
  }

  pthread_attr_destroy(&thattr);
  return (void *)pServerObj;
}

static void taosStopTcpThread(SThreadObj* pThreadObj) {
  // save thread into local variable and signal thread to stop 
  pthread_t thread = pThreadObj->thread; 
  if (!taosCheckPthreadValid(thread)) {
    return;
  }
  pThreadObj->stop = true;
  if (taosComparePthread(thread, pthread_self())) {
    pthread_detach(pthread_self());
    return;
  }
  pthread_join(thread, NULL);
}

void taosStopTcpServer(void *handle) {
  SServerObj *pServerObj = handle;

  if (pServerObj == NULL) return;
  if(pServerObj->fd >=0) shutdown(pServerObj->fd, SHUT_RD);

  if (taosCheckPthreadValid(pServerObj->thread)) {
    if (taosComparePthread(pServerObj->thread, pthread_self())) {
      pthread_detach(pthread_self());
    } else {
      pthread_join(pServerObj->thread, NULL);
    }
  }

  tDebug("%s TCP server is stopped", pServerObj->label);
}

void taosCleanUpTcpServer(void *handle) {
  SServerObj *pServerObj = handle;
  SThreadObj *pThreadObj;
  if (pServerObj == NULL) return;

  for (int i = 0; i < pServerObj->numOfThreads; ++i) {
    pThreadObj = pServerObj->pThreadObj[i];
    taosStopTcpThread(pThreadObj);
  }

  tDebug("%s TCP server is cleaned up", pServerObj->label);

  tfree(pServerObj->pThreadObj);
  tfree(pServerObj);
}

static void *taosAcceptTcpConnection(void *arg) {
  SOCKET             connFd = -1;
  struct sockaddr_in caddr;
  int                threadId = 0;
  SThreadObj        *pThreadObj;
  SServerObj        *pServerObj;

  pServerObj = (SServerObj *)arg;
  tDebug("%s TCP server is ready, ip:0x%x:%hu", pServerObj->label, pServerObj->ip, pServerObj->port);

  while (1) {
    socklen_t addrlen = sizeof(caddr);
    connFd = accept(pServerObj->fd, (struct sockaddr *)&caddr, &addrlen);
    if (connFd == -1) {
      if (errno == EINVAL) {
        tDebug("%s TCP server stop accepting new connections, exiting", pServerObj->label);
        break;
      }

      tError("%s TCP accept failure(%s)", pServerObj->label, strerror(errno));
      continue;
    }

    taosKeepTcpAlive(connFd);
    struct timeval to={5, 0};
    int32_t ret = taosSetSockOpt(connFd, SOL_SOCKET, SO_RCVTIMEO, &to, sizeof(to));
    if (ret != 0) {
      taosCloseSocket(connFd);
      tError("%s failed to set recv timeout fd(%s)for connection from:%s:%hu", pServerObj->label, strerror(errno),
             taosInetNtoa(caddr.sin_addr), htons(caddr.sin_port));
      continue;
    }
    

    // pick up the thread to handle this connection
    pThreadObj = pServerObj->pThreadObj[threadId];

    SFdObj *pFdObj = taosMallocFdObj(pThreadObj, connFd);
    if (pFdObj) {
      pFdObj->ip = caddr.sin_addr.s_addr;
      pFdObj->port = htons(caddr.sin_port);
      tDebug("%s new TCP connection from %s:%hu, fd:%d FD:%p numOfFds:%d", pServerObj->label, 
              taosInetNtoa(caddr.sin_addr), pFdObj->port, connFd, pFdObj, pThreadObj->numOfFds);
    } else {
      taosCloseSocket(connFd);
      tError("%s failed to malloc FdObj(%s) for connection from:%s:%hu", pServerObj->label, strerror(errno),
             taosInetNtoa(caddr.sin_addr), htons(caddr.sin_port));
    }  

    // pick up next thread for next connection
    threadId++;
    threadId = threadId % pServerObj->numOfThreads;
  }

  taosCloseSocket(pServerObj->fd);
  return NULL;
}

void *taosInitTcpClient(uint32_t ip, uint16_t port, char *label, int num, void *fp, void *shandle) {
  SThreadObj    *pThreadObj;
  pthread_attr_t thattr;

  pThreadObj = (SThreadObj *)malloc(sizeof(SThreadObj));
  memset(pThreadObj, 0, sizeof(SThreadObj));
  tstrncpy(pThreadObj->label, label, sizeof(pThreadObj->label));
  pThreadObj->ip = ip;
  pThreadObj->shandle = shandle;

  if (pthread_mutex_init(&(pThreadObj->mutex), NULL) < 0) {
    tError("%s failed to init TCP client mutex(%s)", label, strerror(errno));
    free(pThreadObj);
    terrno = TAOS_SYSTEM_ERROR(errno); 
    return NULL;
  }

  pThreadObj->pollFd = (SOCKET)epoll_create(10);  // size does not matter
  if (pThreadObj->pollFd < 0) {
    tError("%s failed to create TCP client epoll", label);
    free(pThreadObj);
    terrno = TAOS_SYSTEM_ERROR(errno); 
    return NULL;
  }

  pThreadObj->processData = fp;

  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  int code = pthread_create(&(pThreadObj->thread), &thattr, taosProcessTcpData, (void *)(pThreadObj));
  pthread_attr_destroy(&thattr);
  if (code != 0) {
    taosCloseSocket(pThreadObj->pollFd);
    free(pThreadObj);
    terrno = TAOS_SYSTEM_ERROR(errno); 
    tError("%s failed to create TCP read data thread(%s)", label, strerror(errno));
    return NULL;
  }

  tDebug("%s TCP client is initialized, ip:%u:%hu", label, ip, port);

  return pThreadObj;
}

void taosStopTcpClient(void *chandle) {
  SThreadObj *pThreadObj = chandle;
  if (pThreadObj == NULL) return;

  tDebug ("%s TCP client is stopped", pThreadObj->label);
}

void taosCleanUpTcpClient(void *chandle) {
  SThreadObj *pThreadObj = chandle;
  if (pThreadObj == NULL) return;

  tDebug ("%s TCP client will be cleaned up", pThreadObj->label);
  taosStopTcpThread(pThreadObj);
}

void *taosOpenTcpClientConnection(void *shandle, void *thandle, uint32_t ip, uint16_t port) {
  SThreadObj *    pThreadObj = shandle;

  SOCKET fd = taosOpenTcpClientSocket(ip, port, pThreadObj->ip);
  if (fd < 0) return NULL;

  struct sockaddr_in sin;
  uint16_t localPort = 0;
  unsigned int addrlen = sizeof(sin);
  if (getsockname(fd, (struct sockaddr *)&sin, &addrlen) == 0 &&
      sin.sin_family == AF_INET && addrlen == sizeof(sin)) {
    localPort = (uint16_t)ntohs(sin.sin_port);
  }

  SFdObj *pFdObj = taosMallocFdObj(pThreadObj, fd);
  
  if (pFdObj) {
    pFdObj->thandle = thandle;
    pFdObj->port = port;
    pFdObj->ip = ip;
    tDebug("%s %p TCP connection to 0x%x:%hu is created, localPort:%hu FD:%p numOfFds:%d", 
            pThreadObj->label, thandle, ip, port, localPort, pFdObj, pThreadObj->numOfFds);
  } else {
    tError("%s failed to malloc client FdObj(%s)", pThreadObj->label, strerror(errno));
    taosCloseSocket(fd);
  }

  return pFdObj;
}

void taosCloseTcpConnection(void *chandle) {
  SFdObj *pFdObj = chandle;
  if (pFdObj == NULL || pFdObj->signature != pFdObj) return;

  SThreadObj *pThreadObj = pFdObj->pThreadObj;
  tDebug("%s %p TCP connection will be closed, FD:%p", pThreadObj->label, pFdObj->thandle, pFdObj); 

  // pFdObj->thandle = NULL;
  pFdObj->closedByApp = 1;
  shutdown(pFdObj->fd, SHUT_WR);
}

int taosSendTcpData(uint32_t ip, uint16_t port, void *data, int len, void *chandle) {
  SFdObj *pFdObj = chandle;
  if (pFdObj == NULL || pFdObj->signature != pFdObj) return -1;
  SThreadObj *pThreadObj = pFdObj->pThreadObj;

  int ret = taosWriteMsg(pFdObj->fd, data, len);
  tTrace("%s %p TCP data is sent, FD:%p fd:%d bytes:%d", pThreadObj->label, pFdObj->thandle, pFdObj, pFdObj->fd, ret); 

  return ret;
}

static void taosReportBrokenLink(SFdObj *pFdObj) {

  SThreadObj *pThreadObj = pFdObj->pThreadObj;

  // notify the upper layer, so it will clean the associated context
  if (pFdObj->closedByApp == 0) {
    shutdown(pFdObj->fd, SHUT_WR);

    SRecvInfo recvInfo;
    recvInfo.msg = NULL;
    recvInfo.msgLen = 0;
    recvInfo.ip = 0;
    recvInfo.port = 0;
    recvInfo.shandle = pThreadObj->shandle;
    recvInfo.thandle = pFdObj->thandle;
    recvInfo.chandle = NULL;
    recvInfo.connType = RPC_CONN_TCP;
    (*(pThreadObj->processData))(&recvInfo);
  } 

  taosFreeFdObj(pFdObj);
}

static int taosReadTcpData(SFdObj *pFdObj, SRecvInfo *pInfo) {
  SRpcHead    rpcHead;
  int32_t     msgLen, leftLen, retLen, headLen;
  char       *buffer, *msg;

  SThreadObj *pThreadObj = pFdObj->pThreadObj;

  headLen = taosReadMsg(pFdObj->fd, &rpcHead, sizeof(SRpcHead));
  if (headLen != sizeof(SRpcHead)) {
    tDebug("%s %p read error, FD:%p headLen:%d", pThreadObj->label, pFdObj->thandle, pFdObj, headLen);
    return -1; 
  }

  msgLen = (int32_t)htonl((uint32_t)rpcHead.msgLen);
  int32_t size = msgLen + tsRpcOverhead;
  buffer = malloc(size);
  if (NULL == buffer) {
    tError("%s %p TCP malloc(size:%d) fail", pThreadObj->label, pFdObj->thandle, msgLen);
    return -1;
  } else {
    tTrace("%s %p read data, FD:%p fd:%d TCP malloc mem:%p", pThreadObj->label, pFdObj->thandle, pFdObj, pFdObj->fd, buffer);
  }

  msg = buffer + tsRpcOverhead;
  leftLen = msgLen - headLen;
  retLen = taosReadMsg(pFdObj->fd, msg + headLen, leftLen);

  if (leftLen != retLen) {
    tError("%s %p read error, leftLen:%d retLen:%d FD:%p", 
            pThreadObj->label, pFdObj->thandle, leftLen, retLen, pFdObj);
    free(buffer);
    return -1;
  }

  memcpy(msg, &rpcHead, sizeof(SRpcHead));
  
  pInfo->msg = msg;
  pInfo->msgLen = msgLen;
  pInfo->ip = pFdObj->ip;
  pInfo->port = pFdObj->port;
  pInfo->shandle = pThreadObj->shandle;
  pInfo->thandle = pFdObj->thandle;
  pInfo->chandle = pFdObj;
  pInfo->connType = RPC_CONN_TCP;

  if (pFdObj->closedByApp) {
    free(buffer); 
    return -1;
  }

  return 0;
}

#define maxEvents 10

static void *taosProcessTcpData(void *param) {
  SThreadObj        *pThreadObj = param;
  SFdObj            *pFdObj;
  struct epoll_event events[maxEvents];
  SRecvInfo          recvInfo;
 
  while (1) {
    int fdNum = epoll_wait(pThreadObj->pollFd, events, maxEvents, TAOS_EPOLL_WAIT_TIME);
    if (pThreadObj->stop) {
      tDebug("%s TCP thread get stop event, exiting...", pThreadObj->label);
      break;
    }
    if (fdNum < 0) continue;

    for (int i = 0; i < fdNum; ++i) {
      pFdObj = events[i].data.ptr;

      if (events[i].events & EPOLLERR) {
        tDebug("%s %p FD:%p epoll errors", pThreadObj->label, pFdObj->thandle, pFdObj);
        taosReportBrokenLink(pFdObj);
        continue;
      }

      if (events[i].events & EPOLLRDHUP) {
        tDebug("%s %p FD:%p RD hang up", pThreadObj->label, pFdObj->thandle, pFdObj);
        taosReportBrokenLink(pFdObj);
        continue;
      }

      if (events[i].events & EPOLLHUP) {
        tDebug("%s %p FD:%p hang up", pThreadObj->label, pFdObj->thandle, pFdObj);
        taosReportBrokenLink(pFdObj);
        continue;
      }

      if (taosReadTcpData(pFdObj, &recvInfo) < 0) {
        shutdown(pFdObj->fd, SHUT_WR); 
        continue;
      }

      pFdObj->thandle = (*(pThreadObj->processData))(&recvInfo);
      if (pFdObj->thandle == NULL) taosFreeFdObj(pFdObj);
    }

    if (pThreadObj->stop) break; 
  }

  if (pThreadObj->pollFd >=0) taosCloseSocket(pThreadObj->pollFd);

  while (pThreadObj->pHead) {
    SFdObj *pFdObj = pThreadObj->pHead;
    pThreadObj->pHead = pFdObj->next;
    taosReportBrokenLink(pFdObj);
  }

  pthread_mutex_destroy(&(pThreadObj->mutex));
  tDebug("%s TCP thread exits ...", pThreadObj->label);
  tfree(pThreadObj);

  return NULL;
}

static SFdObj *taosMallocFdObj(SThreadObj *pThreadObj, SOCKET fd) {
  struct epoll_event event;

  SFdObj *pFdObj = (SFdObj *)calloc(sizeof(SFdObj), 1);
  if (pFdObj == NULL) {
    return NULL;
  }

  pFdObj->closedByApp = 0;
  pFdObj->fd = fd;
  pFdObj->pThreadObj = pThreadObj;
  pFdObj->signature = pFdObj;

  event.events = EPOLLIN | EPOLLRDHUP;
  event.data.ptr = pFdObj;
  if (epoll_ctl(pThreadObj->pollFd, EPOLL_CTL_ADD, fd, &event) < 0) {
    tfree(pFdObj);
    terrno = TAOS_SYSTEM_ERROR(errno); 
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
  taosCloseSocket(pFdObj->fd);

  pThreadObj->numOfFds--;
  if (pThreadObj->numOfFds < 0)
    tError("%s %p TCP thread:%d, number of FDs is negative!!!", 
            pThreadObj->label, pFdObj->thandle, pThreadObj->threadId);

  if (pFdObj->prev) {
    (pFdObj->prev)->next = pFdObj->next;
  } else {
    pThreadObj->pHead = pFdObj->next;
  }

  if (pFdObj->next) {
    (pFdObj->next)->prev = pFdObj->prev;
  }

  pthread_mutex_unlock(&pThreadObj->mutex);

  tDebug("%s %p TCP connection is closed, FD:%p fd:%d numOfFds:%d", 
          pThreadObj->label, pFdObj->thandle, pFdObj, pFdObj->fd, pThreadObj->numOfFds);

  tfree(pFdObj);
}
