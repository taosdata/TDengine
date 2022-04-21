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

#define _GNU_SOURCE
#include "shellInt.h"

void shellTestNetWork() {}

#if 0
#define ALLOW_FORBID_FUNC
#include "os.h"
#include "rpcHead.h"
#include "syncMsg.h"
#include "taosdef.h"
#include "taoserror.h"
#include "tchecksum.h"
#include "tglobal.h"
#include "tlog.h"
#include "tmsg.h"
#include "trpc.h"

#include "osSocket.h"

#define MAX_PKG_LEN       (64 * 1000)
#define MAX_SPEED_PKG_LEN (1024 * 1024 * 1024)
#define MIN_SPEED_PKG_LEN 1024
#define MAX_SPEED_PKG_NUM 10000
#define MIN_SPEED_PKG_NUM 1
#define BUFFER_SIZE       (MAX_PKG_LEN + 1024)

extern int tsRpcMaxUdpSize;

typedef struct {
  char *   hostFqdn;
  uint32_t hostIp;
  int32_t  port;
  int32_t  pktLen;
} STestInfo;

static void *taosNetBindUdpPort(void *sarg) {
  STestInfo *pinfo = (STestInfo *)sarg;
  int32_t    port = pinfo->port;
  SOCKET     serverSocket;
  char       buffer[BUFFER_SIZE];
  int32_t    iDataNum;
  socklen_t  sin_size;
  int32_t bufSize = 1024000;

  struct sockaddr_in server_addr;
  struct sockaddr_in clientAddr;

  setThreadName("netBindUdpPort");

  if ((serverSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0) {
    uError("failed to create UDP socket since %s", strerror(errno));
    return NULL;
  }

  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(serverSocket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    uError("failed to bind UDP port:%d since %s", port, strerror(errno));
    return NULL;
  }

  TdSocketPtr pSocket = (TdSocketPtr)taosMemoryMalloc(sizeof(TdSocket));
  if (pSocket == NULL) {
    taosCloseSocketNoCheck1(serverSocket);
    return NULL;
  }
  pSocket->fd = serverSocket;
  pSocket->refId = 0;

  if (taosSetSockOpt(pSocket, SOL_SOCKET, SO_SNDBUF, (void *)&bufSize, sizeof(bufSize)) != 0) {
    uError("failed to set the send buffer size for UDP socket\n");
    taosCloseSocket(&pSocket);
    return NULL;
  }

  if (taosSetSockOpt(pSocket, SOL_SOCKET, SO_RCVBUF, (void *)&bufSize, sizeof(bufSize)) != 0) {
    uError("failed to set the receive buffer size for UDP socket\n");
    taosCloseSocket(&pSocket);
    return NULL;
  }

  uInfo("UDP server at port:%d is listening", port);

  while (1) {
    memset(buffer, 0, BUFFER_SIZE);
    sin_size = sizeof(*(struct sockaddr *)&server_addr);
    iDataNum = recvfrom(serverSocket, buffer, BUFFER_SIZE, 0, (struct sockaddr *)&clientAddr, &sin_size);

    if (iDataNum < 0) {
      uDebug("failed to perform recvfrom func at %d since %s", port, strerror(errno));
      continue;
    }

    uInfo("UDP: recv:%d bytes from %s at %d", iDataNum, taosInetNtoa(clientAddr.sin_addr), port);

    if (iDataNum > 0) {
      iDataNum = taosSendto(pSocket, buffer, iDataNum, 0, (struct sockaddr *)&clientAddr, (int32_t)sin_size);
    }

    uInfo("UDP: send:%d bytes to %s at %d", iDataNum, taosInetNtoa(clientAddr.sin_addr), port);
  }

  taosCloseSocket(&pSocket);
  return NULL;
}

static void *taosNetBindTcpPort(void *sarg) {
  struct sockaddr_in server_addr;
  struct sockaddr_in clientAddr;

  STestInfo *pinfo = sarg;
  int32_t    port = pinfo->port;
  SOCKET     serverSocket;
  int32_t    addr_len = sizeof(clientAddr);
  SOCKET     client;
  char       buffer[BUFFER_SIZE];

  setThreadName("netBindTcpPort");

  if ((serverSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
    uError("failed to create TCP socket since %s", strerror(errno));
    return NULL;
  }

  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

  int32_t reuse = 1;
  TdSocketPtr pSocket = (TdSocketPtr)taosMemoryMalloc(sizeof(TdSocket));
  if (pSocket == NULL) {
    taosCloseSocketNoCheck1(serverSocket);
    return NULL;
  }
  pSocket->fd = serverSocket;
  pSocket->refId = 0;

  if (taosSetSockOpt(pSocket, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse, sizeof(reuse)) < 0) {
    uError("setsockopt SO_REUSEADDR failed: %d (%s)", errno, strerror(errno));
    taosCloseSocket(&pSocket);
    return NULL;
  }

  if (bind(serverSocket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    uError("failed to bind TCP port:%d since %s", port, strerror(errno));
    taosCloseSocket(&pSocket);
    return NULL;
  }

  if (taosKeepTcpAlive(pSocket) < 0) {
    uError("failed to set tcp server keep-alive option since %s", strerror(errno));
    taosCloseSocket(&pSocket);
    return NULL;
  }

  if (listen(serverSocket, 10) < 0) {
    uError("failed to listen TCP port:%d since %s", port, strerror(errno));
    taosCloseSocket(&pSocket);
    return NULL;
  }

  uInfo("TCP server at port:%d is listening", port);

  while (1) {
    client = accept(serverSocket, (struct sockaddr *)&clientAddr, (socklen_t *)&addr_len);
    if (client < 0) {
      uDebug("TCP: failed to accept at port:%d since %s", port, strerror(errno));
      continue;
    }

    int32_t ret = taosReadMsg(pSocket, buffer, pinfo->pktLen);
    if (ret < 0 || ret != pinfo->pktLen) {
      uError("TCP: failed to read %d bytes at port:%d since %s", pinfo->pktLen, port, strerror(errno));
      taosCloseSocket(&pSocket);
      return NULL;
    }

    uInfo("TCP: read:%d bytes from %s at %d", pinfo->pktLen, taosInetNtoa(clientAddr.sin_addr), port);

    ret = taosWriteMsg(pSocket, buffer, pinfo->pktLen);
    if (ret < 0) {
      uError("TCP: failed to write %d bytes at %d since %s", pinfo->pktLen, port, strerror(errno));
      taosCloseSocket(&pSocket);
      return NULL;
    }

    uInfo("TCP: write:%d bytes to %s at %d", pinfo->pktLen, taosInetNtoa(clientAddr.sin_addr), port);
  }

  taosCloseSocket(&pSocket);
  return NULL;
}

static int32_t taosNetCheckTcpPort(STestInfo *info) {
  SOCKET  clientSocket;
  char    buffer[BUFFER_SIZE] = {0};

  if ((clientSocket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    uError("failed to create TCP client socket since %s", strerror(errno));
    return -1;
  }

  int32_t reuse = 1;
  TdSocketPtr pSocket = (TdSocketPtr)taosMemoryMalloc(sizeof(TdSocket));
  if (pSocket == NULL) {
    taosCloseSocketNoCheck1(clientSocket);
    return -1;
  }
  pSocket->fd = clientSocket;
  pSocket->refId = 0;

  if (taosSetSockOpt(pSocket, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse, sizeof(reuse)) < 0) {
    uError("setsockopt SO_REUSEADDR failed: %d (%s)", errno, strerror(errno));
    taosCloseSocket(&pSocket);
    return -1;
  }

  struct sockaddr_in serverAddr;
  memset((char *)&serverAddr, 0, sizeof(serverAddr));
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = (uint16_t)htons((uint16_t)info->port);
  serverAddr.sin_addr.s_addr = info->hostIp;

  if (connect(clientSocket, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0) {
    uError("TCP: failed to connect port %s:%d since %s", taosIpStr(info->hostIp), info->port, strerror(errno));
    taosCloseSocket(&pSocket);
    return -1;
  }

  taosKeepTcpAlive(pSocket);

  sprintf(buffer, "client send TCP pkg to %s:%d, content: 1122334455", taosIpStr(info->hostIp), info->port);
  sprintf(buffer + info->pktLen - 16, "1122334455667788");

  int32_t ret = taosWriteMsg(pSocket, buffer, info->pktLen);
  if (ret < 0) {
    uError("TCP: failed to write msg to %s:%d since %s", taosIpStr(info->hostIp), info->port, strerror(errno));
    taosCloseSocket(&pSocket);
    return -1;
  }

  ret = taosReadMsg(pSocket, buffer, info->pktLen);
  if (ret < 0) {
    uError("TCP: failed to read msg from %s:%d since %s", taosIpStr(info->hostIp), info->port, strerror(errno));
    taosCloseSocket(&pSocket);
    return -1;
  }

  taosCloseSocket(&pSocket);
  return 0;
}

static int32_t taosNetCheckUdpPort(STestInfo *info) {
  SOCKET  clientSocket;
  char    buffer[BUFFER_SIZE] = {0};
  int32_t iDataNum = 0;
  int32_t bufSize = 1024000;

  struct sockaddr_in serverAddr;

  if ((clientSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0) {
    uError("failed to create udp client socket since %s", strerror(errno));
    return -1;
  }

  TdSocketPtr pSocket = (TdSocketPtr)taosMemoryMalloc(sizeof(TdSocket));
  if (pSocket == NULL) {
    taosCloseSocketNoCheck1(clientSocket);
    return -1;
  }
  pSocket->fd = clientSocket;
  pSocket->refId = 0;

  if (taosSetSockOpt(pSocket, SOL_SOCKET, SO_SNDBUF, (void *)&bufSize, sizeof(bufSize)) != 0) {
    uError("failed to set the send buffer size for UDP socket\n");
    taosCloseSocket(&pSocket);
    return -1;
  }

  if (taosSetSockOpt(pSocket, SOL_SOCKET, SO_RCVBUF, (void *)&bufSize, sizeof(bufSize)) != 0) {
    uError("failed to set the receive buffer size for UDP socket\n");
    taosCloseSocket(&pSocket);
    return -1;
  }

  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(info->port);
  serverAddr.sin_addr.s_addr = info->hostIp;

  struct in_addr ipStr;
  memcpy(&ipStr, &info->hostIp, 4);
  sprintf(buffer, "client send udp pkg to %s:%d, content: 1122334455", taosInetNtoa(ipStr), info->port);
  sprintf(buffer + info->pktLen - 16, "1122334455667788");

  socklen_t sin_size = sizeof(*(struct sockaddr *)&serverAddr);

  iDataNum = taosSendto(pSocket, buffer, info->pktLen, 0, (struct sockaddr *)&serverAddr, (int32_t)sin_size);
  if (iDataNum < 0 || iDataNum != info->pktLen) {
    uError("UDP: failed to perform sendto func since %s", strerror(errno));
    taosCloseSocket(&pSocket);
    return -1;
  }

  memset(buffer, 0, BUFFER_SIZE);
  sin_size = sizeof(*(struct sockaddr *)&serverAddr);
  iDataNum = recvfrom(clientSocket, buffer, BUFFER_SIZE, 0, (struct sockaddr *)&serverAddr, &sin_size);

  if (iDataNum < 0 || iDataNum != info->pktLen) {
    uError("UDP: received ack:%d bytes(expect:%d) from port:%d since %s", iDataNum, info->pktLen, info->port,
    strerror(errno)); taosCloseSocket(&pSocket); return -1;
  }

  taosCloseSocket(&pSocket);
  return 0;
}

static void taosNetCheckPort(uint32_t hostIp, int32_t startPort, int32_t endPort, int32_t pktLen) {
  int32_t   ret;
  STestInfo info;

  memset(&info, 0, sizeof(STestInfo));
  info.hostIp = hostIp;
  info.pktLen = pktLen;

  for (int32_t port = startPort; port <= endPort; port++) {
    info.port = port;
    ret = taosNetCheckTcpPort(&info);
    if (ret != 0) {
      printf("failed to test TCP port:%d\n", port);
    } else {
      printf("successed to test TCP port:%d\n", port);
    }

    ret = taosNetCheckUdpPort(&info);
    if (ret != 0) {
      printf("failed to test UDP port:%d\n", port);
    } else {
      printf("successed to test UDP port:%d\n", port);
    }
  }
}

static void taosNetTestClient(char *host, int32_t startPort, int32_t pkgLen) {
  uInfo("work as client, host:%s Port:%d pkgLen:%d\n", host, startPort, pkgLen);

  uint32_t serverIp = taosGetIpv4FromFqdn(host);
  if (serverIp == 0xFFFFFFFF) {
    uError("failed to resolve fqdn:%s", host);
    exit(-1);
  }

  uInfo("server ip:%s is resolved from host:%s", taosIpStr(serverIp), host);
  taosNetCheckPort(serverIp, startPort, startPort, pkgLen);
}

static void taosNetTestServer(char *host, int32_t startPort, int32_t pkgLen) {
  uInfo("work as server, host:%s Port:%d pkgLen:%d\n", host, startPort, pkgLen);

  int32_t port = startPort;
  int32_t num = 1;
  if (num < 0) num = 1;

  TdThread *pids = taosMemoryMalloc(2 * num * sizeof(TdThread));
  STestInfo *tinfos = taosMemoryMalloc(num * sizeof(STestInfo));
  STestInfo *uinfos = taosMemoryMalloc(num * sizeof(STestInfo));

  for (int32_t i = 0; i < num; i++) {
    STestInfo *tcpInfo = tinfos + i;
    tcpInfo->port = port + i;
    tcpInfo->pktLen = pkgLen;

    if (taosThreadCreate(pids + i, NULL, taosNetBindTcpPort, tcpInfo) != 0) {
      uInfo("failed to create TCP test thread, %s:%d", tcpInfo->hostFqdn, tcpInfo->port);
      exit(-1);
    }

    STestInfo *udpInfo = uinfos + i;
    udpInfo->port = port + i;
    tcpInfo->pktLen = pkgLen;
    if (taosThreadCreate(pids + num + i, NULL, taosNetBindUdpPort, udpInfo) != 0) {
      uInfo("failed to create UDP test thread, %s:%d", tcpInfo->hostFqdn, tcpInfo->port);
      exit(-1);
    }
  }

  for (int32_t i = 0; i < num; i++) {
    taosThreadJoin(pids[i], NULL);
    taosThreadJoin(pids[(num + i)], NULL);
  }
}

static void taosNetCheckSpeed(char *host, int32_t port, int32_t pkgLen,
                              int32_t pkgNum, char *pkgType) {
#if 0

  // record config
  int32_t compressTmp = tsCompressMsgSize;
  int32_t maxUdpSize  = tsRpcMaxUdpSize;
  int32_t forceTcp  = tsRpcForceTcp;

  if (0 == strcmp("tcp", pkgType)){
    tsRpcForceTcp = 1;
    tsRpcMaxUdpSize = 0;            // force tcp
  } else {
    tsRpcForceTcp = 0;
    tsRpcMaxUdpSize = INT_MAX;
  }
  tsCompressMsgSize = -1;

  SEpSet epSet;
  SRpcMsg   reqMsg;
  SRpcMsg   rspMsg;
  void *    pRpcConn;
  char secretEncrypt[32] = {0};
  char    spi = 0;
  pRpcConn = taosNetInitRpc(secretEncrypt, spi);
  if (NULL == pRpcConn) {
    uError("failed to init client rpc");
    return;
  }

  printf("check net spend, host:%s port:%d pkgLen:%d pkgNum:%d pkgType:%s\n\n", host, port, pkgLen, pkgNum, pkgType);
  int32_t totalSucc = 0;
  uint64_t startT = taosGetTimestampUs();
  for (int32_t i = 1; i <= pkgNum; i++) {
    uint64_t startTime = taosGetTimestampUs();

    memset(&epSet, 0, sizeof(SEpSet));
    strcpy(epSet.eps[0].fqdn, host);
    epSet.eps[0].port = port;
    epSet.numOfEps = 1;

    reqMsg.msgType = TDMT_DND_NETWORK_TEST;
    reqMsg.pCont = rpcMallocCont(pkgLen);
    reqMsg.contLen = pkgLen;
    reqMsg.code = 0;
    reqMsg.handle = NULL;   // rpc handle returned to app
    reqMsg.ahandle = NULL;  // app handle set by client
    strcpy(reqMsg.pCont, "nettest speed");

    rpcSendRecv(pRpcConn, &epSet, &reqMsg, &rspMsg);

    int code = 0;
    if ((rspMsg.code != 0) || (rspMsg.msgType != TDMT_DND_NETWORK_TEST + 1)) {
      uError("ret code 0x%x %s", rspMsg.code, tstrerror(rspMsg.code));
      code = -1;
    }else{
      totalSucc ++;
    }

    rpcFreeCont(rspMsg.pCont);

    uint64_t endTime = taosGetTimestampUs();
    uint64_t el = endTime - startTime;
    printf("progress:%5d/%d\tstatus:%d\tcost:%8.2lf ms\tspeed:%8.2lf MB/s\n", i, pkgNum, code, el/1000.0,
    pkgLen/(el/1000000.0)/1024.0/1024.0);
  }
  int64_t endT = taosGetTimestampUs();
  uint64_t elT = endT - startT;
  printf("\ntotal succ:%5d/%d\tcost:%8.2lf ms\tspeed:%8.2lf MB/s\n", totalSucc, pkgNum, elT/1000.0,
  pkgLen/(elT/1000000.0)/1024.0/1024.0*totalSucc);

  rpcClose(pRpcConn);

  // return config
  tsCompressMsgSize = compressTmp;
  tsRpcMaxUdpSize = maxUdpSize;
  tsRpcForceTcp = forceTcp;
  return;
#endif
}

void taosNetTest(char *role, char *host, int32_t port, int32_t pkgLen, int32_t pkgNum, char *pkgType) {
  tsLogEmbedded = 1;
  if (host == NULL) host = tsLocalFqdn;
  if (port == 0) port = tsServerPort;
  if (0 == strcmp("speed", role)) {
    if (pkgLen <= MIN_SPEED_PKG_LEN) pkgLen = MIN_SPEED_PKG_LEN;
    if (pkgLen > MAX_SPEED_PKG_LEN) pkgLen = MAX_SPEED_PKG_LEN;
    if (pkgNum <= MIN_SPEED_PKG_NUM) pkgNum = MIN_SPEED_PKG_NUM;
    if (pkgNum > MAX_SPEED_PKG_NUM) pkgNum = MAX_SPEED_PKG_NUM;
  } else {
    if (pkgLen <= 10) pkgLen = 1000;
    if (pkgLen > MAX_PKG_LEN) pkgLen = MAX_PKG_LEN;
  }

  if (0 == strcmp("client", role)) {
    taosNetTestClient(host, port, pkgLen);
  } else if (0 == strcmp("server", role)) {
    taosNetTestServer(host, port, pkgLen);
  } else if (0 == strcmp("speed", role)) {
    tsLogEmbedded = 0;
    char type[10] = {0};
    taosNetCheckSpeed(host, port, pkgLen, pkgNum, strtolower(type, pkgType));
  } else {
    TASSERT(1);
  }

  tsLogEmbedded = 0;
}

#endif