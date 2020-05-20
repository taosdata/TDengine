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
#include "tsystem.h"
#include "ttimer.h"
#include "tutil.h"
#include "rpcLog.h"
#include "rpcUdp.h"
#include "rpcHead.h"

#define RPC_MAX_UDP_CONNS 256
#define RPC_MAX_UDP_PKTS 1000
#define RPC_UDP_BUF_TIME 5  // mseconds
#define RPC_MAX_UDP_SIZE 65480

typedef struct {
  void           *signature;
  int             index;
  int             fd;
  uint16_t        port;       // peer port
  uint16_t        localPort;  // local port
  char            label[12];  // copy from udpConnSet;
  pthread_t       thread;
  void           *hash;
  void           *shandle;  // handle passed by upper layer during server initialization
  void           *pSet;
  void         *(*processData)(SRecvInfo *pRecv);
  char           *buffer;  // buffer to receive data
} SUdpConn;

typedef struct {
  int       index;
  int       server;
  uint32_t  ip;       // local IP
  uint16_t  port;     // local Port
  void     *shandle;  // handle passed by upper layer during server initialization
  int       threads;
  char      label[12];
  void     *(*fp)(SRecvInfo *pPacket);
  SUdpConn  udpConn[];
} SUdpConnSet;

static void *taosRecvUdpData(void *param);

void *taosInitUdpConnection(uint32_t ip, uint16_t port, char *label, int threads, void *fp, void *shandle) {
  SUdpConn    *pConn;
  SUdpConnSet *pSet;

  int size = (int)sizeof(SUdpConnSet) + threads * (int)sizeof(SUdpConn);
  pSet = (SUdpConnSet *)malloc((size_t)size);
  if (pSet == NULL) {
    tError("%s failed to allocate UdpConn", label);
    return NULL;
  }

  memset(pSet, 0, (size_t)size);
  pSet->ip = ip;
  pSet->port = port;
  pSet->shandle = shandle;
  pSet->fp = fp;
  strcpy(pSet->label, label);

  uint16_t ownPort;
  for (int i = 0; i < threads; ++i) {
    pConn = pSet->udpConn + i;
    ownPort = (port ? port + i : 0);
    pConn->fd = taosOpenUdpSocket(ip, ownPort);
    if (pConn->fd < 0) {
      tError("%s failed to open UDP socket %x:%hu", label, ip, port);
      taosCleanUpUdpConnection(pSet);
      return NULL;
    }

    pConn->buffer = malloc(RPC_MAX_UDP_SIZE);
    if (NULL == pConn->buffer) {
      tError("%s failed to malloc recv buffer", label);
      taosCleanUpUdpConnection(pSet);
      return NULL;
    }

    struct sockaddr_in sin;
    unsigned int       addrlen = sizeof(sin);
    if (getsockname(pConn->fd, (struct sockaddr *)&sin, &addrlen) == 0 && sin.sin_family == AF_INET &&
        addrlen == sizeof(sin)) {
      pConn->localPort = (uint16_t)ntohs(sin.sin_port);
    }

    strcpy(pConn->label, label);
    pConn->shandle = shandle;
    pConn->processData = fp;
    pConn->index = i;
    pConn->pSet = pSet;
    pConn->signature = pConn;

    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);
    int code = pthread_create(&pConn->thread, &thAttr, taosRecvUdpData, pConn);
    pthread_attr_destroy(&thAttr);
    if (code != 0) {
      tError("%s failed to create thread to process UDP data, reason:%s", label, strerror(errno));
      taosCloseSocket(pConn->fd);
      taosCleanUpUdpConnection(pSet);
      return NULL;
    }

    ++pSet->threads;
  }

  tTrace("%s UDP connection is initialized, ip:%x port:%hu threads:%d", label, ip, port, threads);

  return pSet;
}

void taosCleanUpUdpConnection(void *handle) {
  SUdpConnSet *pSet = (SUdpConnSet *)handle;
  SUdpConn    *pConn;

  if (pSet == NULL) return;

  for (int i = 0; i < pSet->threads; ++i) {
    pConn = pSet->udpConn + i;
    pConn->signature = NULL;
    // shutdown to signal the thread to exit
    shutdown(pConn->fd, SHUT_RD);
  }

  for (int i = 0; i < pSet->threads; ++i) {
    pConn = pSet->udpConn + i;
    pthread_join(pConn->thread, NULL);
    free(pConn->buffer);
    taosCloseSocket(pConn->fd);
    tTrace("chandle:%p is closed", pConn);
  }

  tfree(pSet);
}

void *taosOpenUdpConnection(void *shandle, void *thandle, uint32_t ip, uint16_t port) {
  SUdpConnSet *pSet = (SUdpConnSet *)shandle;

  pSet->index = (pSet->index + 1) % pSet->threads;

  SUdpConn *pConn = pSet->udpConn + pSet->index;
  pConn->port = port;

  tTrace("%s UDP connection is setup, ip:%x:%hu, local:%x:%d", pConn->label, ip, port, pSet->ip, pConn->localPort);

  return pConn;
}

static void *taosRecvUdpData(void *param) {
  SUdpConn          *pConn = param;
  struct sockaddr_in sourceAdd;
  ssize_t            dataLen;
  unsigned int       addLen;
  uint16_t           port;
  SRecvInfo          recvInfo;

  memset(&sourceAdd, 0, sizeof(sourceAdd));
  addLen = sizeof(sourceAdd);
  tTrace("%s UDP thread is created, index:%d", pConn->label, pConn->index);
  char *msg = pConn->buffer;

  while (1) {
    dataLen = recvfrom(pConn->fd, pConn->buffer, RPC_MAX_UDP_SIZE, 0, (struct sockaddr *)&sourceAdd, &addLen);
    if(dataLen == 0) {
      tTrace("data length is 0, socket was closed, exiting");
      break;
    }

    port = ntohs(sourceAdd.sin_port);

    if (dataLen < sizeof(SRpcHead)) {
      tError("%s recvfrom failed, reason:%s\n", pConn->label, strerror(errno));
      continue;
    }

    char *tmsg = malloc(dataLen + tsRpcOverhead);
    if (NULL == tmsg) {
      tError("%s failed to allocate memory, size:%d", pConn->label, dataLen);
      continue;
    }

    tmsg += tsRpcOverhead;  // overhead for SRpcReqContext
    memcpy(tmsg, msg, dataLen);
    recvInfo.msg = tmsg;
    recvInfo.msgLen = dataLen;
    recvInfo.ip = sourceAdd.sin_addr.s_addr;
    recvInfo.port = port;
    recvInfo.shandle = pConn->shandle;
    recvInfo.thandle = NULL;
    recvInfo.chandle = pConn;
    recvInfo.connType = 0;
    (*(pConn->processData))(&recvInfo);
  }

  return NULL;
}

int taosSendUdpData(uint32_t ip, uint16_t port, void *data, int dataLen, void *chandle) {
  SUdpConn *pConn = (SUdpConn *)chandle;

  if (pConn == NULL || pConn->signature != pConn) return -1;

  struct sockaddr_in destAdd;
  memset(&destAdd, 0, sizeof(destAdd));
  destAdd.sin_family = AF_INET;
  destAdd.sin_addr.s_addr = ip;
  destAdd.sin_port = htons(port);

  int ret = (int)sendto(pConn->fd, data, (size_t)dataLen, 0, (struct sockaddr *)&destAdd, sizeof(destAdd));

  return ret;
}

