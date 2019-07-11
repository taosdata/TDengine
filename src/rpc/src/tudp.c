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
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <syslog.h>
#include <unistd.h>

#include "taosmsg.h"
#include "thash.h"
#include "thaship.h"
#include "tlog.h"
#include "tlog.h"
#include "tsocket.h"
#include "tsystem.h"
#include "ttimer.h"
#include "tudp.h"
#include "tutil.h"

#define RPC_MAX_UDP_CONNS 256
#define RPC_MAX_UDP_PKTS 1000
#define RPC_UDP_BUF_TIME 5  // mseconds
#define RPC_MAX_UDP_SIZE 65480

int tsUdpDelay = 0;

typedef struct {
  void *          signature;
  int             index;
  int             fd;
  short           port;       // peer port
  short           localPort;  // local port
  char            label[12];  // copy from udpConnSet;
  pthread_t       thread;
  pthread_mutex_t mutex;
  void *          tmrCtrl;    // copy from UdpConnSet;
  void *          hash;
  void *          shandle;    // handle passed by upper layer during server initialization
  void *          pSet;
  void *(*processData)(char *data, int dataLen, unsigned int ip, short port, void *shandle, void *thandle,
                       void *chandle);
  char buffer[RPC_MAX_UDP_SIZE];  // buffer to receive data
} SUdpConn;

typedef struct {
  int       index;
  int       server;
  char      ip[16];   // local IP
  short     port;     // local Port
  void *    shandle;  // handle passed by upper layer during server initialization
  int       threads;
  char      label[12];
  void *    tmrCtrl;
  pthread_t tcpThread;
  int       tcpFd;
  void *(*fp)(char *data, int dataLen, uint32_t ip, short port, void *shandle, void *thandle, void *chandle);
  SUdpConn udpConn[];
} SUdpConnSet;

typedef struct {
  void *             signature;
  uint32_t           ip;    // dest IP
  short              port;  // dest Port
  SUdpConn *         pConn;
  struct sockaddr_in destAdd;
  void *             msgHdr;
  int                totalLen;
  void *             timer;
  int                emptyNum;
} SUdpBuf;

typedef struct {
  uint64_t handle;
  uint16_t port;
  int32_t  msgLen;
} SPacketInfo;

typedef struct {
  int          fd;
  uint32_t     ip;
  uint16_t     port;
  SUdpConnSet *pSet;
} STransfer;

typedef struct {
  void *       pTimer;
  SUdpConnSet *pSet;
  SUdpConn *   pConn;
  int          dataLen;
  uint32_t     ip;
  uint16_t     port;
  char         data[96];
} SMonitor;

typedef struct {
  uint64_t handle;
  uint64_t hash;
} SHandleViaTcp;

bool taosCheckHandleViaTcpValid(SHandleViaTcp *handleViaTcp) {
  return handleViaTcp->hash == taosHashUInt64(handleViaTcp->handle);
}

void taosInitHandleViaTcp(SHandleViaTcp *handleViaTcp, uint64_t handle) {
  handleViaTcp->handle = handle;
  handleViaTcp->hash = taosHashUInt64(handleViaTcp->handle);
}

void taosProcessMonitorTimer(void *param, void *tmrId) {
  SMonitor *pMonitor = (SMonitor *)param;
  if (pMonitor->pTimer != tmrId) return;

  SUdpConnSet *pSet = pMonitor->pSet;
  pMonitor->pTimer = NULL;

  if (pSet) {
    char *data = malloc((size_t)pMonitor->dataLen);
    memcpy(data, pMonitor->data, (size_t)pMonitor->dataLen);

    tTrace("%s monitor timer is expired, update the link status", pSet->label);
    (*pSet->fp)(data, pMonitor->dataLen, pMonitor->ip, 0, pSet->shandle, NULL, NULL);
    taosTmrReset(taosProcessMonitorTimer, 200, pMonitor, pSet->tmrCtrl, &pMonitor->pTimer);
  }

  if (pMonitor->pSet == NULL) {
    taosTmrStopA(&pMonitor->pTimer);
    free(pMonitor);
  }
}

void *taosReadTcpData(void *argv) {
  SMonitor *   pMonitor = (SMonitor *)argv;
  STaosHeader *pHead = (STaosHeader *)pMonitor->data;
  SPacketInfo *pInfo = (SPacketInfo *)pHead->content;
  SUdpConnSet *pSet = pMonitor->pSet;
  int          retLen, fd;
  char         ipstr[64];

  pInfo->msgLen = (int32_t)htonl((uint32_t)pInfo->msgLen);

  tinet_ntoa(ipstr, pMonitor->ip);
  tTrace("%s receive packet via TCP:%s:%d, msgLen:%d, handle:0x%x, source:0x%08x dest:0x%08x tranId:%d",
         pSet->label, ipstr, pInfo->port, pInfo->msgLen, pInfo->handle, pHead->sourceId, pHead->destId, pHead->tranId);

  fd = taosOpenTcpClientSocket(ipstr, (int16_t)pInfo->port, tsLocalIp);
  if (fd < 0) {
    tError("%s failed to open TCP client socket ip:%s:%d", pSet->label, ipstr, pInfo->port);
    pMonitor->pSet = NULL;
    return NULL;
  }

  SHandleViaTcp handleViaTcp;
  taosInitHandleViaTcp(&handleViaTcp, pInfo->handle);
  retLen = (int)taosWriteSocket(fd, (char *)&handleViaTcp, sizeof(SHandleViaTcp));

  if (retLen != (int)sizeof(SHandleViaTcp)) {
    tError("%s failed to send handle:0x%x to server, retLen:%d", pSet->label, pInfo->handle, retLen);
    pMonitor->pSet = NULL;
  } else {
    tTrace("%s handle:0x%x is sent to server", pSet->label, pInfo->handle);
    char *buffer = malloc((size_t)pInfo->msgLen);
    retLen = taosReadMsg(fd, buffer, pInfo->msgLen);
    pMonitor->pSet = NULL;

    if (retLen != pInfo->msgLen) {
      tError("%s failed to read data from server, msgLen:%d retLen:%d", pSet->label, pInfo->msgLen, retLen);
    } else {
      (*pSet->fp)(buffer, pInfo->msgLen, pMonitor->ip, (int16_t)pInfo->port, pSet->shandle, NULL, pMonitor->pConn);
    }
  }

  taosCloseTcpSocket(fd);

  return NULL;
}

int taosReceivePacketViaTcp(uint32_t ip, STaosHeader *pHead, SUdpConn *pConn) {
  SUdpConnSet *  pSet = pConn->pSet;
  SPacketInfo *  pInfo = (SPacketInfo *)pHead->content;
  int            code = 0;
  pthread_attr_t thattr;
  pthread_t      thread;

  tTrace("%s receive packet via TCP, handle:0x%x, source:0x%08x dest:0x%08x tranId:%d",
         pSet->label, pInfo->handle, pHead->sourceId, pHead->destId, pHead->tranId);

  SMonitor *pMonitor = (SMonitor *)calloc(1, sizeof(SMonitor));
  pMonitor->dataLen = sizeof(STaosHeader) + sizeof(SPacketInfo);
  memcpy(pMonitor->data, pHead, (size_t)pMonitor->dataLen);
  pMonitor->pSet = pSet;
  pMonitor->ip = ip;
  pMonitor->port = pInfo->port;
  pMonitor->pConn = pConn;
  taosTmrReset(taosProcessMonitorTimer, 0, pMonitor, pSet->tmrCtrl, &pMonitor->pTimer);

  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);
  code = pthread_create(&(thread), &thattr, taosReadTcpData, (void *)pMonitor);
  if (code < 0) {
    tTrace("%s faile to create thread to read tcp data, reason:%s", pSet->label, strerror(errno));
  }

  return code;
}

void *taosRecvUdpData(void *param) {
  struct sockaddr_in sourceAdd;
  unsigned int       addLen, dataLen;
  SUdpConn *         pConn = (SUdpConn *)param;
  short              port;
  int                minSize = sizeof(STaosHeader);

  memset(&sourceAdd, 0, sizeof(sourceAdd));
  addLen = sizeof(sourceAdd);
  tTrace("%s UDP thread is created, index:%d", pConn->label, pConn->index);

  while (1) {
    dataLen =
        (uint32_t)recvfrom(pConn->fd, pConn->buffer, sizeof(pConn->buffer), 0, (struct sockaddr *)&sourceAdd, &addLen);
    tTrace("%s msg is recv from 0x%x:%hu len:%d", pConn->label, sourceAdd.sin_addr.s_addr, ntohs(sourceAdd.sin_port),
           dataLen);

    if (dataLen < sizeof(STaosHeader)) {
      tError("%s recvfrom failed, reason:%s\n", pConn->label, strerror(errno));
      continue;
    }

    port = (int16_t)ntohs(sourceAdd.sin_port);

    int   processedLen = 0, leftLen = 0;
    int   msgLen = 0;
    int   count = 0;
    char *msg = pConn->buffer;
    while (processedLen < (int)dataLen) {
      leftLen = dataLen - processedLen;
      STaosHeader *pHead = (STaosHeader *)msg;
      msgLen = (int32_t)htonl((uint32_t)pHead->msgLen);
      if (leftLen < minSize || msgLen > leftLen || msgLen < minSize) {
        tError("%s msg is messed up, dataLen:%d processedLen:%d count:%d msgLen:%d",
               pConn->label, dataLen, processedLen, count, msgLen);
        break;
      }

      if (pHead->tcp == 1) {
        taosReceivePacketViaTcp(sourceAdd.sin_addr.s_addr, (STaosHeader *)msg, pConn);
      } else {
        char *data = malloc((size_t)msgLen);
        memcpy(data, msg, (size_t)msgLen);
        (*(pConn->processData))(data, msgLen, sourceAdd.sin_addr.s_addr, port, pConn->shandle, NULL, pConn);
      }

      processedLen += msgLen;
      msg += msgLen;
      count++;
    }

    // tTrace("%s %d UDP packets are received together", pConn->label, count);
  }

  return NULL;
}

void *taosTransferDataViaTcp(void *argv) {
  STransfer *  pTransfer = (STransfer *)argv;
  int          connFd = pTransfer->fd;
  int          msgLen, retLen, leftLen;
  uint64_t     handle;
  STaosHeader *pHeader = NULL, head;
  SUdpConnSet *pSet = pTransfer->pSet;

  SHandleViaTcp handleViaTcp;
  retLen = taosReadMsg(connFd, &handleViaTcp, sizeof(SHandleViaTcp));

  if (retLen != sizeof(SHandleViaTcp)) {
    tError("%s UDP server failed to read handle, retLen:%d", pSet->label, retLen);
    taosCloseSocket(connFd);
    free(pTransfer);
    return NULL;
  }

  if (!taosCheckHandleViaTcpValid(&handleViaTcp)) {
    tError("%s UDP server read handle via tcp invalid, handle:%ld, hash:%ld", pSet->label, handleViaTcp.handle,
           handleViaTcp.hash);
    taosCloseSocket(connFd);
    free(pTransfer);
    return NULL;
  }

  handle = handleViaTcp.handle;

  if (handle == 0) {
    // receive a packet from client
    tTrace("%s data will be received via TCP from 0x%x:%d", pSet->label, pTransfer->ip, pTransfer->port);
    retLen = taosReadMsg(connFd, &head, sizeof(STaosHeader));
    if (retLen != (int)sizeof(STaosHeader)) {
      tError("%s failed to read msg header, retLen:%d", pSet->label, retLen);
    } else {
      SMonitor *pMonitor = (SMonitor *)calloc(1, sizeof(SMonitor));
      pMonitor->dataLen = sizeof(STaosHeader);
      memcpy(pMonitor->data, &head, (size_t)pMonitor->dataLen);
      ((STaosHeader *)pMonitor->data)->msgLen = (int32_t)htonl(sizeof(STaosHeader));
      ((STaosHeader *)pMonitor->data)->tcp = 1;
      pMonitor->ip = pTransfer->ip;
      pMonitor->port = head.port;
      pMonitor->pSet = pSet;
      taosTmrReset(taosProcessMonitorTimer, 0, pMonitor, pSet->tmrCtrl, &pMonitor->pTimer);

      msgLen = (int32_t)htonl((uint32_t)head.msgLen);
      char *buffer = malloc((size_t)msgLen);
      leftLen = msgLen - (int)sizeof(STaosHeader);
      retLen = taosReadMsg(connFd, buffer + sizeof(STaosHeader), leftLen);
      pMonitor->pSet = NULL;

      if (retLen != leftLen) {
        tError("%s failed to read data from client, leftLen:%d retLen:%d, error:%s",
               pSet->label, leftLen, retLen, strerror(errno));
      } else {
        tTrace("%s data is received from client via TCP from 0x%x:%d, msgLen:%d", pSet->label, pTransfer->ip,
               pTransfer->port, msgLen);
        pSet->index = (pSet->index + 1) % pSet->threads;
        SUdpConn *pConn = pSet->udpConn + pSet->index;
        memcpy(buffer, &head, sizeof(STaosHeader));
        (*pSet->fp)(buffer, msgLen, pTransfer->ip, head.port, pSet->shandle, NULL, pConn);
      }

      taosWriteMsg(connFd, &handleViaTcp, sizeof(SHandleViaTcp));
    }
  } else {
    // send a packet to client
    tTrace("%s send packet to client via TCP, handle:0x%x", pSet->label, handle);
    pHeader = (STaosHeader *)handle;
    msgLen = (int32_t)htonl((uint32_t)pHeader->msgLen);

    if (pHeader->tcp != 0 || msgLen < 1024) {
      tError("%s invalid handle:%p, connection shall be closed", pSet->label, pHeader);
    } else {
      SMonitor *pMonitor = (SMonitor *)calloc(1, sizeof(SMonitor));
      pMonitor->dataLen = sizeof(STaosHeader);
      memcpy(pMonitor->data, (void *)handle, (size_t)pMonitor->dataLen);
      STaosHeader *pThead = (STaosHeader *)pMonitor->data;
      pThead->tcp = 1;
      pThead->msgType = (char)(pHeader->msgType - 1);
      pThead->msgLen = (int32_t)htonl(sizeof(STaosHeader));
      pMonitor->ip = pTransfer->ip;
      pMonitor->port = pTransfer->port;
      pMonitor->pSet = pSet;
      taosTmrReset(taosProcessMonitorTimer, 200, pMonitor, pSet->tmrCtrl, &pMonitor->pTimer);

      retLen = taosWriteMsg(connFd, (void *)handle, msgLen);
      pMonitor->pSet = NULL;

      if (retLen != msgLen) {
        tError("%s failed to send data to client, msgLen:%d retLen:%d", pSet->label, msgLen, retLen);
      } else {
        tTrace("%s data is sent to client successfully via TCP to 0x%x:%d, size:%d",
               pSet->label, pTransfer->ip, pTransfer->port, msgLen);
      }
    }
  }

  // retLen = taosReadMsg(connFd, &handleViaTcp, sizeof(handleViaTcp));
  free(pTransfer);
  taosCloseSocket(connFd);

  return NULL;
}

void *taosUdpTcpConnection(void *argv) {
  int                connFd = -1;
  struct sockaddr_in clientAddr;
  pthread_attr_t     thattr;
  pthread_t          thread;
  uint32_t           sourceIp;
  char               ipstr[20];

  SUdpConnSet *pSet = (SUdpConnSet *)argv;

  pSet->tcpFd = taosOpenTcpServerSocket(pSet->ip, pSet->port);
  if (pSet->tcpFd < 0) {
    tPrint("%s failed to create TCP socket %s:%d for UDP server, reason:%s", pSet->label, pSet->ip, pSet->port,
           strerror(errno));
    taosKillSystem();
    return NULL;
  }

  tTrace("%s UDP server is created, ip:%s:%d", pSet->label, pSet->ip, pSet->port);

  while (1) {
    if (pSet->tcpFd < 0) break;
    socklen_t addrlen = sizeof(clientAddr);
    connFd = accept(pSet->tcpFd, (struct sockaddr *)&clientAddr, &addrlen);

    if (connFd < 0) {
      tError("%s UDP server TCP accept failure, reason:%s", pSet->label, strerror(errno));
      continue;
    }

    sourceIp = clientAddr.sin_addr.s_addr;
    tinet_ntoa(ipstr, sourceIp);
    tTrace("%s UDP server TCP connection from ip:%s:%u", pSet->label, ipstr, htons(clientAddr.sin_port));

    STransfer *pTransfer = malloc(sizeof(STransfer));
    pTransfer->fd = connFd;
    pTransfer->ip = sourceIp;
    pTransfer->port = clientAddr.sin_port;
    pTransfer->pSet = pSet;

    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);
    if (pthread_create(&(thread), &thattr, taosTransferDataViaTcp, (void *)pTransfer) < 0) {
      tTrace("%s faile to create thread for UDP server, reason:%s", pSet->label, strerror(errno));
      taosCloseSocket(connFd);
    }
  }

  return NULL;
}

void *taosInitUdpConnection(char *ip, short port, char *label, int threads, void *fp, void *shandle) {
  pthread_attr_t thAttr;
  SUdpConn *     pConn;
  SUdpConnSet *  pSet;

  int size = (int)sizeof(SUdpConnSet) + threads * (int)sizeof(SUdpConn);
  pSet = (SUdpConnSet *)malloc((size_t)size);
  if (pSet == NULL) {
    tError("%s failed to allocate UdpConn", label);
    return NULL;
  }

  memset(pSet, 0, (size_t)size);
  strcpy(pSet->ip, ip);
  pSet->port = port;
  pSet->threads = threads;
  pSet->shandle = shandle;
  pSet->fp = fp;
  pSet->tcpFd = -1;
  strcpy(pSet->label, label);

  //  if ( tsUdpDelay ) {
  char udplabel[12];
  sprintf(udplabel, "%s.b", label);
  pSet->tmrCtrl = taosTmrInit(RPC_MAX_UDP_CONNS * threads, 5, 5000, udplabel);
  //  }

  short ownPort;
  for (int i = 0; i < threads; ++i) {
    pConn = pSet->udpConn + i;
    ownPort = (int16_t)(port ? port + i : 0);
    pConn->fd = taosOpenUdpSocket(ip, ownPort);
    if (pConn->fd < 0) {
      tError("%s failed to open UDP socket %s:%d", label, ip, port);
      return NULL;
    }

    struct sockaddr_in sin;
    unsigned int       addrlen = sizeof(sin);
    if (getsockname(pConn->fd, (struct sockaddr *)&sin, &addrlen) == 0 && sin.sin_family == AF_INET &&
        addrlen == sizeof(sin)) {
      pConn->localPort = (int16_t)ntohs(sin.sin_port);
    }

    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);
    if (pthread_create(&pConn->thread, &thAttr, taosRecvUdpData, pConn) != 0) {
      close(pConn->fd);
      tError("%s failed to create thread to process UDP data, reason:%s", label, strerror(errno));
      return NULL;
    }

    strcpy(pConn->label, label);
    pConn->shandle = shandle;
    pConn->processData = fp;
    pConn->index = i;
    pConn->pSet = pSet;
    pConn->signature = pConn;
    if (tsUdpDelay) {
      pConn->hash = taosOpenIpHash(RPC_MAX_UDP_CONNS);
      pthread_mutex_init(&pConn->mutex, NULL);
      pConn->tmrCtrl = pSet->tmrCtrl;
    }
  }

  pthread_attr_destroy(&thAttr);
  tTrace("%s UDP connection is initialized, ip:%s port:%u threads:%d", label, ip, port, threads);

  return pSet;
}

void *taosInitUdpServer(char *ip, short port, char *label, int threads, void *fp, void *shandle) {
  SUdpConnSet *pSet;
  pSet = taosInitUdpConnection(ip, port, label, threads, fp, shandle);
  if (pSet == NULL) return NULL;

  pSet->server = 1;
  pSet->fp = fp;

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);

  // not support by windows
  // pthread_t thread;
  // pSet->tcpThread = pthread_create(&(thread), &thattr, taosUdpTcpConnection,
  // pSet);
  pthread_create(&(pSet->tcpThread), &thattr, taosUdpTcpConnection, pSet);

  return pSet;
}

void *taosInitUdpClient(char *ip, short port, char *label, int threads, void *fp, void *shandle) {
  return taosInitUdpConnection(ip, port, label, threads, fp, shandle);
}

void taosCleanUpUdpConnection(void *handle) {
  SUdpConnSet *pSet = (SUdpConnSet *)handle;
  SUdpConn *   pConn;

  if (pSet == NULL) return;
  if (pSet->server == 1) {
    pthread_cancel(pSet->tcpThread);
  }

  for (int i = 0; i < pSet->threads; ++i) {
    pConn = pSet->udpConn + i;
    pConn->signature = NULL;
    taosCloseSocket(pConn->fd);
    if (pConn->hash) {
      taosCloseIpHash(pConn->hash);
      pthread_mutex_destroy(&pConn->mutex);
    }

    pthread_cancel(pConn->thread);
    pthread_join(pConn->thread, NULL);
    tTrace("chandle:%p is closed", pConn);
  }

  if (pSet->tcpFd >= 0) taosCloseTcpSocket(pSet->tcpFd);
  pSet->tcpFd = -1;
  taosTmrCleanUp(pSet->tmrCtrl);
  tfree(pSet);
}

void *taosOpenUdpConnection(void *shandle, void *thandle, char *ip, short port) {
  SUdpConnSet *pSet = (SUdpConnSet *)shandle;

  pSet->index = (pSet->index + 1) % pSet->threads;

  SUdpConn *pConn = pSet->udpConn + pSet->index;
  pConn->port = port;

  tTrace("%s UDP connection is setup, ip: %s:%d, local: %s:%d", pConn->label, ip, port, pSet->ip,
         ntohs((uint16_t)pConn->localPort));

  return pConn;
}

void taosRemoveUdpBuf(SUdpBuf *pBuf) {
  taosTmrStopA(&pBuf->timer);
  taosDeleteIpHash(pBuf->pConn->hash, pBuf->ip, pBuf->port);

  // tTrace("%s UDP buffer to:0x%lld:%d is removed", pBuf->pConn->label,
  // pBuf->ip, pBuf->port);

  pBuf->signature = NULL;
  taosFreeMsgHdr(pBuf->msgHdr);
  free(pBuf);
}

void taosProcessUdpBufTimer(void *param, void *tmrId) {
  SUdpBuf *pBuf = (SUdpBuf *)param;
  if (pBuf->signature != param) return;
  if (pBuf->timer != tmrId) return;

  SUdpConn *pConn = pBuf->pConn;

  pthread_mutex_lock(&pConn->mutex);

  if (taosMsgHdrSize(pBuf->msgHdr) > 0) {
    taosSendMsgHdr(pBuf->msgHdr, pConn->fd);
    pBuf->totalLen = 0;
    pBuf->emptyNum = 0;
  } else {
    pBuf->emptyNum++;
    if (pBuf->emptyNum > 200) {
      taosRemoveUdpBuf(pBuf);
      pBuf = NULL;
    }
  }

  pthread_mutex_unlock(&pConn->mutex);

  if (pBuf) taosTmrReset(taosProcessUdpBufTimer, RPC_UDP_BUF_TIME, pBuf, pConn->tmrCtrl, &pBuf->timer);
}

SUdpBuf *taosCreateUdpBuf(SUdpConn *pConn, uint32_t ip, short port) {
  SUdpBuf *pBuf = (SUdpBuf *)malloc(sizeof(SUdpBuf));
  memset(pBuf, 0, sizeof(SUdpBuf));

  pBuf->ip = ip;
  pBuf->port = port;
  pBuf->pConn = pConn;

  pBuf->destAdd.sin_family = AF_INET;
  pBuf->destAdd.sin_addr.s_addr = ip;
  pBuf->destAdd.sin_port = (uint16_t)htons((uint16_t)port);
  taosInitMsgHdr(&(pBuf->msgHdr), &(pBuf->destAdd), RPC_MAX_UDP_PKTS);
  pBuf->signature = pBuf;
  taosTmrReset(taosProcessUdpBufTimer, RPC_UDP_BUF_TIME, pBuf, pConn->tmrCtrl, &pBuf->timer);

  // tTrace("%s UDP buffer to:0x%lld:%d is created", pBuf->pConn->label,
  // pBuf->ip, pBuf->port);

  return pBuf;
}

int taosSendPacketViaTcp(uint32_t ip, short port, char *data, int dataLen, void *chandle) {
  SUdpConn *   pConn = (SUdpConn *)chandle;
  SUdpConnSet *pSet = (SUdpConnSet *)pConn->pSet;
  int          code = -1, retLen, msgLen;
  char         ipstr[64];
  char         buffer[128];
  STaosHeader *pHead;

  if (pSet->server) {
    // send from server

    pHead = (STaosHeader *)buffer;
    memcpy(pHead, data, sizeof(STaosHeader));
    pHead->tcp = 1;

    SPacketInfo *pInfo = (SPacketInfo *)pHead->content;
    pInfo->handle = (uint64_t)data;
    pInfo->port = (uint16_t)pSet->port;
    pInfo->msgLen = pHead->msgLen;

    msgLen = sizeof(STaosHeader) + sizeof(SPacketInfo);
    pHead->msgLen = (int32_t)htonl((uint32_t)msgLen);
    code = taosSendUdpData(ip, port, buffer, msgLen, chandle);
    tTrace("%s data from server will be sent via TCP:%d, msgType:%d, length:%d, handle:0x%x",
           pSet->label, pInfo->port, pHead->msgType, htonl((uint32_t)pInfo->msgLen), pInfo->handle);
    if (code > 0) code = dataLen;
  } else {
    // send from client
    tTrace("%s data will be sent via TCP from client", pSet->label);

    // send a UDP header first to set up the connection
    pHead = (STaosHeader *)buffer;
    memcpy(pHead, data, sizeof(STaosHeader));
    pHead->tcp = 2;
    msgLen = sizeof(STaosHeader);
    pHead->msgLen = (int32_t)htonl(msgLen);
    code = taosSendUdpData(ip, port, buffer, msgLen, chandle);

    pHead = (STaosHeader *)data;

    tinet_ntoa(ipstr, ip);
    int fd = taosOpenTcpClientSocket(ipstr, pConn->port, tsLocalIp);
    if (fd < 0) {
      tError("%s failed to open TCP socket to:%s:%u to send packet", pSet->label, ipstr, pConn->port);
    } else {
      SHandleViaTcp handleViaTcp;
      taosInitHandleViaTcp(&handleViaTcp, 0);
      retLen = (int)taosWriteSocket(fd, (char *)&handleViaTcp, sizeof(SHandleViaTcp));

      if (retLen != (int)sizeof(handleViaTcp)) {
        tError("%s failed to send handle to server, retLen:%d", pSet->label, retLen);
      } else {
        retLen = taosWriteMsg(fd, data, dataLen);
        if (retLen != dataLen) {
          tError("%s failed to send data via TCP, dataLen:%d, retLen:%d, error:%s", pSet->label, dataLen, retLen,
                 strerror(errno));
        } else {
          code = dataLen;
          tTrace("%s data is sent via TCP successfully", pSet->label);
        }
      }

      taosReadMsg(fd, (char *)&handleViaTcp, sizeof(SHandleViaTcp));

      taosCloseTcpSocket(fd);
    }
  }

  return code;
}

int taosSendUdpData(uint32_t ip, short port, char *data, int dataLen, void *chandle) {
  SUdpConn *pConn = (SUdpConn *)chandle;
  SUdpBuf * pBuf;

  if (pConn == NULL || pConn->signature != pConn) return -1;

  if (dataLen >= RPC_MAX_UDP_SIZE) return taosSendPacketViaTcp(ip, port, data, dataLen, chandle);

  if (pConn->hash == NULL) {
    struct sockaddr_in destAdd;
    memset(&destAdd, 0, sizeof(destAdd));
    destAdd.sin_family = AF_INET;
    destAdd.sin_addr.s_addr = ip;
    destAdd.sin_port = htons((uint16_t)port);

    int ret = (int)sendto(pConn->fd, data, (size_t)dataLen, 0, (struct sockaddr *)&destAdd, sizeof(destAdd));
    tTrace("%s msg is sent to 0x%x:%hu len:%d ret:%d localPort:%hu chandle:0x%x", pConn->label, destAdd.sin_addr.s_addr,
           port, dataLen, ret, pConn->localPort, chandle);

    return ret;
  }

  pthread_mutex_lock(&pConn->mutex);

  pBuf = (SUdpBuf *)taosGetIpHash(pConn->hash, ip, port);
  if (pBuf == NULL) {
    pBuf = taosCreateUdpBuf(pConn, ip, port);
    taosAddIpHash(pConn->hash, pBuf, ip, port);
  }

  if ((pBuf->totalLen + dataLen > RPC_MAX_UDP_SIZE) || (taosMsgHdrSize(pBuf->msgHdr) >= RPC_MAX_UDP_PKTS)) {
    taosTmrReset(taosProcessUdpBufTimer, RPC_UDP_BUF_TIME, pBuf, pConn->tmrCtrl, &pBuf->timer);

    taosSendMsgHdr(pBuf->msgHdr, pConn->fd);
    pBuf->totalLen = 0;
  }

  taosSetMsgHdrData(pBuf->msgHdr, data, dataLen);

  pBuf->totalLen += dataLen;

  pthread_mutex_unlock(&pConn->mutex);

  return dataLen;
}
