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

#define _DEFAULT_SOURCE
#include "os.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "taoserror.h"
#include "tulog.h"
#include "tglobal.h"
#include "tsocket.h"
#include "trpc.h"
#include "rpcHead.h"

#define MAX_PKG_LEN (64 * 1000)
#define BUFFER_SIZE (MAX_PKG_LEN + 1024)

extern int32_t tsRpcMaxUdpSize;

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

  struct sockaddr_in server_addr;
  struct sockaddr_in clientAddr;

  if ((serverSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0) {
    uError("failed to create udp socket since %s", strerror(errno));
    return NULL;
  }

  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(serverSocket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    uError("failed to bind udp port:%d since %s", port, strerror(errno));
    return NULL;
  }

  uInfo("udp server at port:%d is listening", port);

  while (1) {
    memset(buffer, 0, BUFFER_SIZE);
    sin_size = sizeof(*(struct sockaddr *)&server_addr);
    iDataNum = recvfrom(serverSocket, buffer, BUFFER_SIZE, 0, (struct sockaddr *)&clientAddr, &sin_size);

    if (iDataNum < 0) {
      uDebug("failed to perform recvfrom func at %d since %s", port, strerror(errno));
      continue;
    }

    if (iDataNum > 0) {
      uInfo("UDP: recv:%d bytes from %s:%d", iDataNum, taosInetNtoa(clientAddr.sin_addr), port);
      sendto(serverSocket, buffer, iDataNum, 0, (struct sockaddr *)&clientAddr, (int32_t)sin_size);
    }
  }

  taosCloseSocket(serverSocket);
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
  int32_t    iDataNum = 0;

  if ((serverSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
    uError("failed to create tcp socket since %s", strerror(errno));
    return NULL;
  }

  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(serverSocket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    uError("failed to bind tcp port:%d since %s", port, strerror(errno));
    return NULL;
  }

  if (listen(serverSocket, 5) < 0) {
    uError("failed to listen tcp port:%d since %s", port, strerror(errno));
    return NULL;
  }

  uInfo("tcp server at port:%d is listening", port);

  while (1) {
    client = accept(serverSocket, (struct sockaddr *)&clientAddr, (socklen_t *)&addr_len);
    if (client < 0) {
      uDebug("failed to accept from tcp port:%d since %s", port, strerror(errno));
      continue;
    }

    iDataNum = 0;
    memset(buffer, 0, BUFFER_SIZE);
    int32_t nleft, nread;
    char *  ptr = buffer;
    nleft = pinfo->pktLen;

    while (nleft > 0) {
      nread = recv(client, ptr, BUFFER_SIZE, 0);

      if (nread == 0) {
        break;
      } else if (nread < 0) {
        if (errno == EINTR) {
          continue;
        } else {
          uError("failed to perform recv func at %d since %s", port, strerror(errno));
          taosCloseSocket(serverSocket);
          return NULL;
        }
      } else {
        nleft -= nread;
        ptr += nread;
        iDataNum += nread;
      }
    }

    if (iDataNum > 0) {
      uInfo("TCP: recv:%d bytes from %s:%d", iDataNum, taosInetNtoa(clientAddr.sin_addr), port);
      send(client, buffer, iDataNum, 0);
    }
  }
  
  taosCloseSocket(serverSocket);
  return NULL;
}

static int32_t taosNetCheckTcpPort(STestInfo *info) {
  SOCKET  clientSocket;
  char    sendbuf[BUFFER_SIZE];
  char    recvbuf[BUFFER_SIZE];
  int32_t iDataNum = 0;

  struct sockaddr_in serverAddr;
  if ((clientSocket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    uError("failed to create tcp client socket since %s", strerror(errno));
    return -1;
  }

  // set send and recv overtime
  struct timeval timeout;
  timeout.tv_sec = 2;   // s
  timeout.tv_usec = 0;  // us
  if (setsockopt(clientSocket, SOL_SOCKET, SO_SNDTIMEO, (char *)&timeout, sizeof(struct timeval)) == -1) {
    uError("failed to setsockopt send timer since %s", strerror(errno));
  }
  if (setsockopt(clientSocket, SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout, sizeof(struct timeval)) == -1) {
    uError("failed to setsockopt recv timer since %s", strerror(errno));
  }

  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(info->port);
  serverAddr.sin_addr.s_addr = info->hostIp;

  if (connect(clientSocket, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0) {
    uError("failed to connect port:%d since %s", info->port, strerror(errno));
    return -1;
  }

  memset(sendbuf, 0, BUFFER_SIZE);
  memset(recvbuf, 0, BUFFER_SIZE);

  struct in_addr ipStr;
  memcpy(&ipStr, &info->hostIp, 4);
  sprintf(sendbuf, "client send tcp pkg to %s:%d, content: 1122334455", taosInetNtoa(ipStr), info->port);
  sprintf(sendbuf + info->pktLen - 16, "1122334455667788");

  send(clientSocket, sendbuf, info->pktLen, 0);

  memset(recvbuf, 0, BUFFER_SIZE);
  int32_t nleft, nread;
  char *  ptr = recvbuf;
  nleft = info->pktLen;

  while (nleft > 0) {
    nread = recv(clientSocket, ptr, BUFFER_SIZE, 0);;
  
    if (nread == 0) {
      break;
    } else if (nread < 0) {
      if (errno == EINTR) {
        continue;
      } else {
        uError("faild to recv pkg from TCP port:%d since %s", info->port, strerror(errno));
        taosCloseSocket(clientSocket);
        return -1;
      }
    } else {
      nleft -= nread;
      ptr += nread;
      iDataNum += nread;
    }
  }

  if (iDataNum < info->pktLen) {
    uError("TCP: received ack:%d bytes, less than send:%d bytes from port:%d", iDataNum, info->pktLen, info->port);
    return -1;
  }

  taosCloseSocket(clientSocket);
  return 0;
}

static int32_t taosNetCheckUdpPort(STestInfo *info) {
  SOCKET  clientSocket;
  char    sendbuf[BUFFER_SIZE];
  char    recvbuf[BUFFER_SIZE];
  int32_t iDataNum = 0;

  struct sockaddr_in serverAddr;

  if ((clientSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0) {
    uError("failed to create udp client socket since %s", strerror(errno));
    return -1;
  }

  // set overtime
  struct timeval timeout;
  timeout.tv_sec = 2;   // s
  timeout.tv_usec = 0;  // us
  if (setsockopt(clientSocket, SOL_SOCKET, SO_SNDTIMEO, (char *)&timeout, sizeof(struct timeval)) == -1) {
    uError("failed to setsockopt send timer since %s", strerror(errno));
  }
  if (setsockopt(clientSocket, SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout, sizeof(struct timeval)) == -1) {
    uError("failed to setsockopt recv timer since %s", strerror(errno));
  }

  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(info->port);
  serverAddr.sin_addr.s_addr = info->hostIp;

  memset(sendbuf, 0, BUFFER_SIZE);
  memset(recvbuf, 0, BUFFER_SIZE);

  struct in_addr ipStr;
  memcpy(&ipStr, &info->hostIp, 4);
  sprintf(sendbuf, "client send udp pkg to %s:%d, content: 1122334455", taosInetNtoa(ipStr), info->port);
  sprintf(sendbuf + info->pktLen - 16, "1122334455667788");

  socklen_t sin_size = sizeof(*(struct sockaddr *)&serverAddr);

  int32_t code = sendto(clientSocket, sendbuf, info->pktLen, 0, (struct sockaddr *)&serverAddr, (int32_t)sin_size);
  if (code < 0) {
    uError("failed to perform sendto func since %s", strerror(errno));
    return -1;
  }

  iDataNum = recvfrom(clientSocket, recvbuf, BUFFER_SIZE, 0, (struct sockaddr *)&serverAddr, &sin_size);

  if (iDataNum < info->pktLen) {
    uError("UDP: received ack:%d bytes, less than send:%d bytes from port:%d", iDataNum, info->pktLen, info->port);
    return -1;
  }

  taosCloseSocket(clientSocket);
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
      uError("failed to test tcp port:%d", port);
    } else {
      uInfo("successed to test tcp port:%d", port);
    }

    ret = taosNetCheckUdpPort(&info);
    if (ret != 0) {
      uError("failed to test udp port:%d", port);
    } else {
      uInfo("successed to test udp port:%d", port);
    }
  }
  return;
}

void *taosNetInitRpc(char *secretEncrypt, char spi) {
  SRpcInit rpcInit;
  void *   pRpcConn = NULL;

  char user[] = "nettestinternal";
  char pass[] = "nettestinternal";
  taosEncryptPass((uint8_t *)pass, strlen(pass), secretEncrypt);

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort = 0;
  rpcInit.label = "NT";
  rpcInit.numOfThreads = 1;  // every DB connection has only one thread
  rpcInit.cfp = NULL;
  rpcInit.sessions = 16;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.user = user;
  rpcInit.idleTime = 2000;
  rpcInit.ckey = "key";
  rpcInit.spi = spi;
  rpcInit.secret = secretEncrypt;

  pRpcConn = rpcOpen(&rpcInit);
  return pRpcConn;
}

static int32_t taosNetCheckRpc(const char* serverFqdn, uint16_t port, uint16_t pktLen, char spi, SStartupStep *pStep) {
  SRpcEpSet epSet;
  SRpcMsg   reqMsg;
  SRpcMsg   rspMsg;
  void *    pRpcConn;

  char secretEncrypt[32] = {0};

  pRpcConn = taosNetInitRpc(secretEncrypt, spi);
  if (NULL == pRpcConn) {
    uError("failed to init client rpc");
    return -1;
  }

  memset(&epSet, 0, sizeof(SRpcEpSet));
  epSet.inUse = 0;
  epSet.numOfEps = 1;
  epSet.port[0] = port;
  strcpy(epSet.fqdn[0], serverFqdn);

  reqMsg.msgType = TSDB_MSG_TYPE_NETWORK_TEST;
  reqMsg.pCont = rpcMallocCont(pktLen);
  reqMsg.contLen = pktLen;
  reqMsg.code = 0;
  reqMsg.handle = NULL;   // rpc handle returned to app
  reqMsg.ahandle = NULL;  // app handle set by client
  strcpy(reqMsg.pCont, "nettest");

  rpcSendRecv(pRpcConn, &epSet, &reqMsg, &rspMsg);

  if ((rspMsg.code != 0) || (rspMsg.msgType != TSDB_MSG_TYPE_NETWORK_TEST + 1)) {
    uDebug("ret code 0x%x %s", rspMsg.code, tstrerror(rspMsg.code));
    return -1;
  }

  int32_t code = 0;
  if (pStep != NULL && rspMsg.pCont != NULL && rspMsg.contLen > 0 && rspMsg.contLen <= sizeof(SStartupStep)) {
    memcpy(pStep, rspMsg.pCont, rspMsg.contLen);
    code = 1;
  }

  rpcFreeCont(rspMsg.pCont);
  rpcClose(pRpcConn);
  return code;
}

static int32_t taosNetParseStartup(SStartupStep *pCont) {
  SStartupStep *pStep = pCont;
  uInfo("step:%s desc:%s", pStep->name, pStep->desc);

  if (pStep->finished) {
    uInfo("check startup finished");
  }

  return pStep->finished ? 0 : 1;
}

static void taosNetTestStartup(char *host, int32_t port) {
  uInfo("check startup, host:%s port:%d\n", host, port);

  SStartupStep *pStep = malloc(sizeof(SStartupStep));
  while (1) {
    int32_t code = taosNetCheckRpc(host, port, 20, 0, pStep);
    if (code > 0) {
      code = taosNetParseStartup(pStep);
    }

    if (code > 0) {
      uDebug("continue check startup step");
    } else {
      break;
    }
    taosMsleep(500);
  }

  free(pStep);
}

static void taosNetTestRpc(char *host, int32_t startPort, int32_t pkgLen) {
  int32_t endPort = startPort + 9;
  char    spi = 0;

  uInfo("check rpc, host:%s startPort:%d endPort:%d pkgLen:%d\n", host, startPort, endPort, pkgLen);
    
  for (uint16_t port = startPort; port <= endPort; port++) {
    int32_t sendpkgLen;
    if (pkgLen <= tsRpcMaxUdpSize) {
      sendpkgLen = tsRpcMaxUdpSize + 1000;
    } else {
      sendpkgLen = pkgLen;
    }

    int32_t ret = taosNetCheckRpc(host, port, sendpkgLen, spi, NULL);
    if (ret < 0) {
      uError("failed to test tcp port:%d", port);
    } else {
      uInfo("successed to test tcp port:%d", port);
    }

    if (pkgLen >= tsRpcMaxUdpSize) {
      sendpkgLen = tsRpcMaxUdpSize - 1000;
    } else {
      sendpkgLen = pkgLen;
    }

    ret = taosNetCheckRpc(host, port, pkgLen, spi, NULL);
    if (ret < 0) {
      uError("failed to test udp port:%d", port);
    } else {
      uInfo("successed to test udp port:%d", port);
    }
  }
}

static void taosNetTestClient(char *host, int32_t startPort, int32_t pkgLen) {
  int32_t endPort = startPort + 11;
  uInfo("work as client, host:%s startPort:%d endPort:%d pkgLen:%d\n", host, startPort, endPort, pkgLen);

  uint32_t serverIp = taosGetIpFromFqdn(host);
  if (serverIp == 0xFFFFFFFF) {
    uError("failed to resolve fqdn:%s", host);
    exit(-1);
  }

  uInfo("server ip:%s is resolved from host:%s", taosIpStr(serverIp), host);
  taosNetCheckPort(serverIp, startPort, endPort, pkgLen);
}

static void taosNetTestServer(char *host, int32_t startPort, int32_t pkgLen) {
  int32_t endPort = startPort + 11;
  uInfo("work as server, host:%s startPort:%d endPort:%d pkgLen:%d\n", host, startPort, endPort, pkgLen);

  int32_t port = startPort;
  int32_t num = endPort - startPort + 1;
  if (num < 0) num = 1;

  pthread_t *pids = malloc(2 * num * sizeof(pthread_t));
  STestInfo *tinfos = malloc(num * sizeof(STestInfo));
  STestInfo *uinfos = malloc(num * sizeof(STestInfo));

  for (int32_t i = 0; i < num; i++) {
    STestInfo *tcpInfo = tinfos + i;
    tcpInfo->port = port + i;
    tcpInfo->pktLen = pkgLen;

    if (pthread_create(pids + i, NULL, taosNetBindTcpPort, tcpInfo) != 0) {
      uInfo("failed to create tcp test thread, %s:%d", tcpInfo->hostFqdn, tcpInfo->port);
      exit(-1);
    }

    STestInfo *udpInfo = uinfos + i;
    udpInfo->port = (uint16_t)(port + i);
    if (pthread_create(pids + num + i, NULL, taosNetBindUdpPort, udpInfo) != 0) {
      uInfo("failed to create udp test thread, %s:%d", tcpInfo->hostFqdn, tcpInfo->port);
      exit(-1);
    }
  }

  for (int32_t i = 0; i < num; i++) {
    pthread_join(pids[i], NULL);
    pthread_join(pids[(num + i)], NULL);
  }
}

void taosNetTest(char *role, char *host, int32_t port, int32_t pkgLen) {
  tscEmbedded = 1;
  if (host == NULL) host = tsLocalFqdn;
  if (port == 0) port = tsServerPort;
  if (pkgLen <= 10) pkgLen = 1000;
  if (pkgLen > MAX_PKG_LEN) pkgLen = MAX_PKG_LEN;

  if (0 == strcmp("client", role)) {
    taosNetTestClient(host, port, pkgLen);
  } else if (0 == strcmp("server", role)) {
    taosNetTestServer(host, port, pkgLen);
  } else if (0 == strcmp("rpc", role)) {
    taosNetTestRpc(host, port, pkgLen);
  } else if (0 == strcmp("startup", role)) {
    taosNetTestStartup(host, port);
  } else {
    taosNetTestStartup(host, port);
  }

  tscEmbedded = 0;
}
