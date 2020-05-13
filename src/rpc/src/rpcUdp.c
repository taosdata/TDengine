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
#include "rpcHaship.h"
#include "rpcUdp.h"
#include "rpcHead.h"

#define RPC_MAX_UDP_CONNS 256
#define RPC_MAX_UDP_PKTS 1000
#define RPC_UDP_BUF_TIME 5  // mseconds
#define RPC_MAX_UDP_SIZE 65480

int tsUdpDelay = 0;

typedef struct {
  void           *signature;
  int             index;
  int             fd;
  uint16_t        port;       // peer port
  uint16_t        localPort;  // local port
  char            label[12];  // copy from udpConnSet;
  pthread_t       thread;
  pthread_mutex_t mutex;
  void           *tmrCtrl;  // copy from UdpConnSet;
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
  void     *tmrCtrl;
  void     *(*fp)(SRecvInfo *pPacket);
  SUdpConn  udpConn[];
} SUdpConnSet;

typedef struct {
  void              *signature;
  uint32_t           ip;    // dest IP
  uint16_t           port;  // dest Port
  SUdpConn          *pConn;
  struct sockaddr_in destAdd;
  void              *msgHdr;
  int                totalLen;
  void              *timer;
  int                emptyNum;
} SUdpBuf;

static void *taosRecvUdpData(void *param);
static SUdpBuf *taosCreateUdpBuf(SUdpConn *pConn, uint32_t ip, uint16_t port);
static void taosProcessUdpBufTimer(void *param, void *tmrId);

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

  if ( tsUdpDelay ) {
    char udplabel[12];
    sprintf(udplabel, "%s.b", label);
    pSet->tmrCtrl = taosTmrInit(RPC_MAX_UDP_CONNS * threads, 5, 5000, udplabel);
    if (pSet->tmrCtrl == NULL) {
      tError("%s failed to initialize tmrCtrl") taosCleanUpUdpConnection(pSet);
      return NULL;
    }
  }

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
    if (tsUdpDelay) {
      pConn->hash = rpcOpenIpHash(RPC_MAX_UDP_CONNS);
      pthread_mutex_init(&pConn->mutex, NULL);
      pConn->tmrCtrl = pSet->tmrCtrl;
    }

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
    free(pConn->buffer);
    pthread_cancel(pConn->thread);
    taosCloseSocket(pConn->fd);
    if (pConn->hash) {
      rpcCloseIpHash(pConn->hash);
      pthread_mutex_destroy(&pConn->mutex);
    }
  }

  for (int i = 0; i < pSet->threads; ++i) {
    pConn = pSet->udpConn + i;
    pthread_join(pConn->thread, NULL);
    tTrace("chandle:%p is closed", pConn);
  }

  taosTmrCleanUp(pSet->tmrCtrl);
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
  int                dataLen;
  unsigned int       addLen;
  uint16_t           port;
  int                minSize = sizeof(SRpcHead);
  SRecvInfo          recvInfo;

  memset(&sourceAdd, 0, sizeof(sourceAdd));
  addLen = sizeof(sourceAdd);
  tTrace("%s UDP thread is created, index:%d", pConn->label, pConn->index);

  while (1) {
    dataLen = recvfrom(pConn->fd, pConn->buffer, RPC_MAX_UDP_SIZE, 0, (struct sockaddr *)&sourceAdd, &addLen);
    port = ntohs(sourceAdd.sin_port);
    tTrace("%s msg is recv from 0x%x:%hu len:%d", pConn->label, sourceAdd.sin_addr.s_addr, port, dataLen);

    if (dataLen < sizeof(SRpcHead)) {
      tError("%s recvfrom failed, reason:%s\n", pConn->label, strerror(errno));
      continue;
    }

    int   processedLen = 0, leftLen = 0;
    int   msgLen = 0;
    int   count = 0;
    char *msg = pConn->buffer;
    while (processedLen < dataLen) {
      leftLen = dataLen - processedLen;
      SRpcHead *pHead = (SRpcHead *)msg;
      msgLen = htonl((uint32_t)pHead->msgLen);
      if (leftLen < minSize || msgLen > leftLen || msgLen < minSize) {
        tError("%s msg is messed up, dataLen:%d processedLen:%d count:%d msgLen:%d", pConn->label, dataLen,
               processedLen, count, msgLen);
        break;
      }

      char *tmsg = malloc((size_t)msgLen + tsRpcOverhead);
      if (NULL == tmsg) {
        tError("%s failed to allocate memory, size:%d", pConn->label, msgLen);
        break;
      }

      tmsg += tsRpcOverhead;  // overhead for SRpcReqContext
      memcpy(tmsg, msg, (size_t)msgLen);
      recvInfo.msg = tmsg;
      recvInfo.msgLen = msgLen;
      recvInfo.ip = sourceAdd.sin_addr.s_addr;
      recvInfo.port = port;
      recvInfo.shandle = pConn->shandle;
      recvInfo.thandle = NULL;
      recvInfo.chandle = pConn;
      recvInfo.connType = 0;
      (*(pConn->processData))(&recvInfo);

      processedLen += msgLen;
      msg += msgLen;
      count++;
    }

    // tTrace("%s %d UDP packets are received together", pConn->label, count);
  }

  return NULL;
}

int taosSendUdpData(uint32_t ip, uint16_t port, void *data, int dataLen, void *chandle) {
  SUdpConn *pConn = (SUdpConn *)chandle;
  SUdpBuf  *pBuf;

  if (pConn == NULL || pConn->signature != pConn) return -1;

  if (pConn->hash == NULL) {
    struct sockaddr_in destAdd;
    memset(&destAdd, 0, sizeof(destAdd));
    destAdd.sin_family = AF_INET;
    destAdd.sin_addr.s_addr = ip;
    destAdd.sin_port = htons(port);

    //tTrace("%s msg is sent to 0x%x:%hu len:%d ret:%d localPort:%hu chandle:0x%x", pConn->label, destAdd.sin_addr.s_addr,
    //       port, dataLen, ret, pConn->localPort, chandle);
    int ret = (int)sendto(pConn->fd, data, (size_t)dataLen, 0, (struct sockaddr *)&destAdd, sizeof(destAdd));

    return ret;
  }

  pthread_mutex_lock(&pConn->mutex);

  pBuf = (SUdpBuf *)rpcGetIpHash(pConn->hash, ip, port);
  if (pBuf == NULL) {
    pBuf = taosCreateUdpBuf(pConn, ip, port);
    rpcAddIpHash(pConn->hash, pBuf, ip, port);
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

void taosFreeMsgHdr(void *hdr) {
  struct msghdr *msgHdr = (struct msghdr *)hdr;
  free(msgHdr->msg_iov);
}

int taosMsgHdrSize(void *hdr) {
  struct msghdr *msgHdr = (struct msghdr *)hdr;
  return (int)msgHdr->msg_iovlen;
}

void taosSendMsgHdr(void *hdr, int fd) {
  struct msghdr *msgHdr = (struct msghdr *)hdr;
  sendmsg(fd, msgHdr, 0);
  msgHdr->msg_iovlen = 0;
}

void taosInitMsgHdr(void **hdr, void *dest, int maxPkts) {
  struct msghdr *msgHdr = (struct msghdr *)malloc(sizeof(struct msghdr));
  memset(msgHdr, 0, sizeof(struct msghdr));
  *hdr = msgHdr;
  struct sockaddr_in *destAdd = (struct sockaddr_in *)dest;

  msgHdr->msg_name = destAdd;
  msgHdr->msg_namelen = sizeof(struct sockaddr_in);
  int size = (int)sizeof(struct iovec) * maxPkts;
  msgHdr->msg_iov = (struct iovec *)malloc((size_t)size);
  memset(msgHdr->msg_iov, 0, (size_t)size);
}

void taosSetMsgHdrData(void *hdr, char *data, int dataLen) {
  struct msghdr *msgHdr = (struct msghdr *)hdr;
  msgHdr->msg_iov[msgHdr->msg_iovlen].iov_base = data;
  msgHdr->msg_iov[msgHdr->msg_iovlen].iov_len = (size_t)dataLen;
  msgHdr->msg_iovlen++;
}

void taosRemoveUdpBuf(SUdpBuf *pBuf) {
  taosTmrStopA(&pBuf->timer);
  rpcDeleteIpHash(pBuf->pConn->hash, pBuf->ip, pBuf->port);

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

static SUdpBuf *taosCreateUdpBuf(SUdpConn *pConn, uint32_t ip, uint16_t port) {
  SUdpBuf *pBuf = (SUdpBuf *)malloc(sizeof(SUdpBuf));
  memset(pBuf, 0, sizeof(SUdpBuf));

  pBuf->ip = ip;
  pBuf->port = port;
  pBuf->pConn = pConn;

  pBuf->destAdd.sin_family = AF_INET;
  pBuf->destAdd.sin_addr.s_addr = ip;
  pBuf->destAdd.sin_port = (uint16_t)htons(port);
  taosInitMsgHdr(&(pBuf->msgHdr), &(pBuf->destAdd), RPC_MAX_UDP_PKTS);
  pBuf->signature = pBuf;
  taosTmrReset(taosProcessUdpBufTimer, RPC_UDP_BUF_TIME, pBuf, pConn->tmrCtrl, &pBuf->timer);

  // tTrace("%s UDP buffer to:0x%lld:%d is created", pBuf->pConn->label,
  // pBuf->ip, pBuf->port);

  return pBuf;
}


