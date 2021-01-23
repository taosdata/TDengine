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
#include "taosdef.h"
#include "taoserror.h"
#include "rpcLog.h"
#include "rpcUdp.h"
#include "rpcHead.h"

#define RPC_MAX_UDP_CONNS 256
#define RPC_MAX_UDP_PKTS 1000
#define RPC_UDP_BUF_TIME 5  // mseconds
#define RPC_MAX_UDP_SIZE 65480

typedef struct {
  int             index;
  SOCKET          fd;
  uint16_t        port;       // peer port
  uint16_t        localPort;  // local port
  char            label[TSDB_LABEL_LEN];  // copy from udpConnSet;
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
  char      label[TSDB_LABEL_LEN];
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
    terrno = TAOS_SYSTEM_ERROR(errno);
    return NULL;
  }

  memset(pSet, 0, (size_t)size);
  pSet->ip = ip;
  pSet->port = port;
  pSet->shandle = shandle;
  pSet->fp = fp;
  pSet->threads = threads;
  tstrncpy(pSet->label, label, sizeof(pSet->label));

  pthread_attr_t thAttr;
  pthread_attr_init(&thAttr);
  pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

  int i;
  uint16_t ownPort;
  for (i = 0; i < threads; ++i) {
    pConn = pSet->udpConn + i;
    ownPort = (port ? port + i : 0);
    pConn->fd = taosOpenUdpSocket(ip, ownPort);
    if (pConn->fd < 0) {
      tError("%s failed to open UDP socket %x:%hu", label, ip, port);
      break;
    }

    pConn->buffer = malloc(RPC_MAX_UDP_SIZE);
    if (NULL == pConn->buffer) {
      tError("%s failed to malloc recv buffer", label);
      break;
    }

    struct sockaddr_in sin;
    unsigned int addrlen = sizeof(sin);
    if (getsockname(pConn->fd, (struct sockaddr *)&sin, &addrlen) == 0 && 
        sin.sin_family == AF_INET && addrlen == sizeof(sin)) {
      pConn->localPort = (uint16_t)ntohs(sin.sin_port);
    }

    tstrncpy(pConn->label, label, sizeof(pConn->label));
    pConn->shandle = shandle;
    pConn->processData = fp;
    pConn->index = i;
    pConn->pSet = pSet;

    int code = pthread_create(&pConn->thread, &thAttr, taosRecvUdpData, pConn);
    if (code != 0) {
      tError("%s failed to create thread to process UDP data(%s)", label, strerror(errno));
      break;
    }
  }

  pthread_attr_destroy(&thAttr);

  if (i != threads) { 
    terrno = TAOS_SYSTEM_ERROR(errno);
    taosCleanUpUdpConnection(pSet);
    return NULL;
  }

  tDebug("%s UDP connection is initialized, ip:%x:%hu threads:%d", label, ip, port, threads);
  return pSet;
}

void taosStopUdpConnection(void *handle) {
  SUdpConnSet *pSet = (SUdpConnSet *)handle;
  SUdpConn    *pConn;

  if (pSet == NULL) return;

  for (int i = 0; i < pSet->threads; ++i) {
    pConn = pSet->udpConn + i;
    if (pConn->fd >=0) shutdown(pConn->fd, SHUT_RDWR);
    if (pConn->fd >=0) taosCloseSocket(pConn->fd);
    pConn->fd = -1;
  }

  for (int i = 0; i < pSet->threads; ++i) {
    pConn = pSet->udpConn + i;
    if (taosCheckPthreadValid(pConn->thread)) {
      pthread_join(pConn->thread, NULL);
    }
    tfree(pConn->buffer);
    // tTrace("%s UDP thread is closed, index:%d", pConn->label, i);
  }

  tDebug("%s UDP is stopped", pSet->label);
}

void taosCleanUpUdpConnection(void *handle) {
  SUdpConnSet *pSet = (SUdpConnSet *)handle;
  SUdpConn    *pConn;

  if (pSet == NULL) return;

  for (int i = 0; i < pSet->threads; ++i) {
    pConn = pSet->udpConn + i;
    if (pConn->fd >=0) taosCloseSocket(pConn->fd);
  }

  tDebug("%s UDP is cleaned up", pSet->label);
  tfree(pSet);
}

void *taosOpenUdpConnection(void *shandle, void *thandle, uint32_t ip, uint16_t port) {
  SUdpConnSet *pSet = (SUdpConnSet *)shandle;

  pSet->index = (pSet->index + 1) % pSet->threads;

  SUdpConn *pConn = pSet->udpConn + pSet->index;
  pConn->port = port;

  tDebug("%s UDP connection is setup, ip:%x:%hu localPort:%hu", pConn->label, ip, port, pConn->localPort);

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
  tDebug("%s UDP thread is created, index:%d", pConn->label, pConn->index);
  char *msg = pConn->buffer;

  while (1) {
    dataLen = recvfrom(pConn->fd, pConn->buffer, RPC_MAX_UDP_SIZE, 0, (struct sockaddr *)&sourceAdd, &addLen);
    if(dataLen <= 0) {
      tDebug("%s UDP socket(fd:%d) receive dataLen(%d) error(%s)", pConn->label, pConn->fd, (int32_t)dataLen, strerror(errno));
      // for windows usage, remote shutdown also returns - 1 in windows client
      if (-1 == pConn->fd) {
        break;
      } else {
        continue;
      }
    }

    port = ntohs(sourceAdd.sin_port);

    if (dataLen < sizeof(SRpcHead)) {
      tError("%s recvfrom failed(%s)", pConn->label, strerror(errno));
      continue;
    }

    int32_t size = dataLen + tsRpcOverhead;
    char *tmsg = malloc(size);
    if (NULL == tmsg) {
      tError("%s failed to allocate memory, size:%" PRId64, pConn->label, (int64_t)dataLen);
      continue;
    } else {
      tTrace("UDP malloc mem:%p size:%d", tmsg, size);
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

  if (pConn == NULL) return -1;

  struct sockaddr_in destAdd;
  memset(&destAdd, 0, sizeof(destAdd));
  destAdd.sin_family = AF_INET;
  destAdd.sin_addr.s_addr = ip;
  destAdd.sin_port = htons(port);

  int ret = (int)taosSendto(pConn->fd, data, (size_t)dataLen, 0, (struct sockaddr *)&destAdd, sizeof(destAdd));

  return ret;
}

