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
#include "taosdef.h"
#include "taosmsg.h"
#include "taoserror.h"
#include "tulog.h"
#include "tconfig.h"
#include "tglobal.h"
#include "tsocket.h"
#include "trpc.h"
#include "rpcHead.h"
#include "tutil.h"
#include "tnettest.h"

#define    MAX_PKG_LEN          (64*1000)
#define    BUFFER_SIZE          (MAX_PKG_LEN + 1024)

typedef struct {
  uint32_t hostIp;
  uint16_t port;
  uint16_t pktLen;
} info_s;

extern int tsRpcMaxUdpSize;

static char g_user[TSDB_USER_LEN+1]     = {0};
static char g_pass[TSDB_PASSWORD_LEN+1] = {0};
static char g_serverFqdn[TSDB_FQDN_LEN] = {0};
static uint16_t g_startPort = 0;
static uint16_t g_endPort   = 6042;
static uint32_t g_pktLen    = 0;


static void *bindUdpPort(void *sarg) {
  info_s *pinfo = (info_s *)sarg;
  int     port = pinfo->port;
  SOCKET  serverSocket;

  struct sockaddr_in server_addr;
  struct sockaddr_in clientAddr;
  char               buffer[BUFFER_SIZE];
  int                iDataNum;
    
  if ((serverSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0) {
    perror("socket");
    return NULL;
  }

  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(serverSocket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    perror("connect");
    return NULL;
  }

  socklen_t sin_size;

  while (1) {
    memset(buffer, 0, BUFFER_SIZE);

    sin_size = sizeof(*(struct sockaddr *)&server_addr);

    iDataNum = recvfrom(serverSocket, buffer, BUFFER_SIZE, 0, (struct sockaddr *)&clientAddr, &sin_size);

    if (iDataNum < 0) {
      perror("recvfrom null");
      continue;
    }
    if (iDataNum > 0) {
      printf("recv Client: %s pkg from UDP port: %d, pkg len: %d\n", taosInetNtoa(clientAddr.sin_addr), port, iDataNum);
      //printf("Read msg from udp:%s ... %s\n", buffer, buffer+iDataNum-16);

      sendto(serverSocket, buffer, iDataNum, 0, (struct sockaddr *)&clientAddr, (int)sin_size);
    }
  }

  taosCloseSocket(serverSocket);
  return NULL;
}

static void *bindTcpPort(void *sarg) {
  info_s *pinfo = (info_s *)sarg;
  int     port = pinfo->port;
  SOCKET  serverSocket;

  struct sockaddr_in server_addr;
  struct sockaddr_in clientAddr;
  int                addr_len = sizeof(clientAddr);
  SOCKET             client;
  char               buffer[BUFFER_SIZE];
  int                iDataNum = 0;

  if ((serverSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
    printf("socket() fail: %s", strerror(errno));
    return NULL;
  }

  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(serverSocket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    printf("port:%d bind() fail: %s", port, strerror(errno));
    return NULL;
  }

  if (listen(serverSocket, 5) < 0) {
    printf("listen() fail: %s", strerror(errno));
    return NULL;
  }

  //printf("Bind port: %d success\n", port);
  while (1) {
    client = accept(serverSocket, (struct sockaddr *)&clientAddr, (socklen_t *)&addr_len);
    if (client < 0) {
      printf("accept() fail: %s", strerror(errno));
      continue;
    }

    iDataNum = 0;
    memset(buffer, 0, BUFFER_SIZE);
    int   nleft, nread;
    char *ptr = buffer;
    nleft = pinfo->pktLen;
    while (nleft > 0) {
      nread = recv(client, ptr, BUFFER_SIZE, 0);

      if (nread == 0) {
        break;
      } else if (nread < 0) {
        if (errno == EINTR) {
          continue;
        } else {
          printf("recv Client: %s pkg from TCP port: %d fail:%s.\n", taosInetNtoa(clientAddr.sin_addr), port, strerror(errno));
          taosCloseSocket(serverSocket);
          return NULL;
        }
      } else {
        nleft -= nread;
        ptr += nread;
        iDataNum += nread;
      }      
    }
    
    printf("recv Client: %s pkg from TCP port: %d, pkg len: %d\n", taosInetNtoa(clientAddr.sin_addr), port, iDataNum);
    if (iDataNum > 0) {
      send(client, buffer, iDataNum, 0);
    }
  }
  
  taosCloseSocket(serverSocket);
  return NULL;
}

static int checkTcpPort(info_s *info) {
  struct sockaddr_in serverAddr;
  SOCKET             clientSocket;
  char               sendbuf[BUFFER_SIZE];
  char               recvbuf[BUFFER_SIZE];
  int                iDataNum = 0;
  if ((clientSocket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    printf("socket() fail: %s\n", strerror(errno));
    return -1;
  }

  // set send and recv overtime
  struct timeval timeout;  
  timeout.tv_sec = 2;  //s
  timeout.tv_usec = 0; //us  
  if (setsockopt(clientSocket, SOL_SOCKET,SO_SNDTIMEO, (char *)&timeout, sizeof(struct timeval)) == -1) {
    perror("setsockopt send timer failed:");
  }
  if (setsockopt(clientSocket, SOL_SOCKET,SO_RCVTIMEO, (char *)&timeout, sizeof(struct timeval)) == -1) {
    perror("setsockopt recv timer failed:");
  }

  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(info->port);

  serverAddr.sin_addr.s_addr = info->hostIp;

  //printf("=================================\n");
  if (connect(clientSocket, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0) {
    printf("connect() fail: %s\t", strerror(errno));
    return -1;
  }
  //printf("Connect to: %s:%d...success\n", host, port);
  memset(sendbuf, 0, BUFFER_SIZE);
  memset(recvbuf, 0, BUFFER_SIZE);

  struct in_addr ipStr;
  memcpy(&ipStr, &info->hostIp, 4);
  sprintf(sendbuf, "client send tcp pkg to %s:%d, content: 1122334455", taosInetNtoa(ipStr), info->port);
  sprintf(sendbuf + info->pktLen - 16, "1122334455667788");

  send(clientSocket, sendbuf, info->pktLen, 0);

  memset(recvbuf, 0, BUFFER_SIZE);
  int   nleft, nread;
  char *ptr = recvbuf;
  nleft = info->pktLen;
  while (nleft > 0) {
    nread = recv(clientSocket, ptr, BUFFER_SIZE, 0);;
  
    if (nread == 0) {
      break;
    } else if (nread < 0) {
      if (errno == EINTR) {
        continue;
      } else {
        printf("recv ack pkg from TCP port: %d fail:%s.\n", info->port, strerror(errno));
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
    printf("recv ack pkg len: %d, less than req pkg len: %d from tcp port: %d\n", iDataNum, info->pktLen, info->port);
    return -1;
  }
  //printf("Read ack pkg len:%d from tcp port: %d, buffer: %s  %s\n", info->pktLen, port, recvbuf, recvbuf+iDataNum-8);

  taosCloseSocket(clientSocket);
  return 0;
}

static int checkUdpPort(info_s *info) {
  struct sockaddr_in serverAddr;
  SOCKET             clientSocket;
  char               sendbuf[BUFFER_SIZE];
  char               recvbuf[BUFFER_SIZE];
  int                iDataNum = 0;
  if ((clientSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0) {
    perror("socket");
    return -1;
  }

  // set overtime 
  struct timeval timeout;
  timeout.tv_sec = 2;  //s
  timeout.tv_usec = 0; //us
  if (setsockopt(clientSocket, SOL_SOCKET,SO_SNDTIMEO, (char *)&timeout, sizeof(struct timeval)) == -1) {
    perror("setsockopt send timer failed:");
  }
  if (setsockopt(clientSocket, SOL_SOCKET,SO_RCVTIMEO, (char *)&timeout, sizeof(struct timeval)) == -1) {
    perror("setsockopt recv timer failed:");
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

  int code = sendto(clientSocket, sendbuf, info->pktLen, 0, (struct sockaddr *)&serverAddr, (int)sin_size);
  if (code < 0) {
    perror("sendto");
    return -1;
  }

  iDataNum = recvfrom(clientSocket, recvbuf, BUFFER_SIZE, 0, (struct sockaddr *)&serverAddr, &sin_size);

  if (iDataNum < info->pktLen) {
    printf("Read ack pkg len: %d, less than req pkg len: %d from udp port: %d\t\t", iDataNum, info->pktLen, info->port);
    return -1;
  }
  
  //printf("Read ack pkg len:%d from udp port: %d, buffer: %s  %s\n", info->pktLen, port, recvbuf, recvbuf+iDataNum-8);
  taosCloseSocket(clientSocket);
  return 0;
}

static void checkPort(uint32_t hostIp, uint16_t startPort, uint16_t maxPort, uint16_t pktLen) {
  int ret;
  info_s info;
  memset(&info, 0, sizeof(info_s));
  info.hostIp   = hostIp;
  info.pktLen   = pktLen;

  for (uint16_t port = startPort; port <= maxPort; port++) {
    //printf("test: %s:%d\n", info.host, port);
    printf("\n");

    info.port = port;
    ret = checkTcpPort(&info);
    if (ret != 0) {
      printf("tcp port:%d test fail.\t\n", port);
    } else {
      printf("tcp port:%d test ok.\t\t", port);
    }
    
    ret = checkUdpPort(&info);
    if (ret != 0) {
      printf("udp port:%d test fail.\t\n", port);
    } else {
      printf("udp port:%d test ok.\t\t", port);
    }
  }
  
  printf("\n");
  return ;
}

void* tnetInitRpc(char* secretEncrypt, char spi) {
  SRpcInit rpcInit;
  void* pRpcConn = NULL;

  taosEncryptPass((uint8_t *)g_pass, strlen(g_pass), secretEncrypt);
      
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort = 0;
  rpcInit.label = "NET-TEST";
  rpcInit.numOfThreads = 1;  // every DB connection has only one thread
  rpcInit.cfp = NULL;
  rpcInit.sessions = 16;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.user = g_user;
  rpcInit.idleTime = 2000;
  rpcInit.ckey = "key";
  rpcInit.spi = spi;
  rpcInit.secret = secretEncrypt;

  pRpcConn = rpcOpen(&rpcInit);
  return pRpcConn;
}

static int rpcCheckPortImpl(const char* serverFqdn, uint16_t port, uint16_t pktLen, char spi) {  
  SRpcEpSet epSet;
  SRpcMsg   reqMsg;
  SRpcMsg   rspMsg;
  void*     pRpcConn;

  char secretEncrypt[32] = {0};

  pRpcConn = tnetInitRpc(secretEncrypt, spi);
  if (NULL == pRpcConn) {
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
  reqMsg.ahandle = NULL;   // app handle set by client

  rpcSendRecv(pRpcConn, &epSet, &reqMsg, &rspMsg);

  // handle response
  if ((rspMsg.code != 0) || (rspMsg.msgType != TSDB_MSG_TYPE_NETWORK_TEST + 1)) {
    //printf("code:%d[%s]\n", rspMsg.code, tstrerror(rspMsg.code));
    return -1;
  }
  
  rpcFreeCont(rspMsg.pCont);

  rpcClose(pRpcConn);

  return 0;
}

static void rpcCheckPort(uint32_t hostIp) {
  int ret;
  char spi;

  for (uint16_t port = g_startPort; port <= g_endPort; port++) {
    //printf("test: %s:%d\n", info.host, port);
    printf("\n");

    //================ check tcp port ================ 
    int32_t pktLen;
    if (g_pktLen <= tsRpcMaxUdpSize) {
      pktLen = tsRpcMaxUdpSize + 1000;
    } else {
      pktLen = g_pktLen;
    }

    spi = 1;
    ret = rpcCheckPortImpl(g_serverFqdn, port, pktLen, spi);
    if (ret != 0) {
      spi = 0;
      ret = rpcCheckPortImpl(g_serverFqdn, port, pktLen, spi);
      if (ret != 0) {        
        printf("TCP port:%d test fail.\t\t", port);
      } else {
        //printf("tcp port:%d test ok.\t\t", port);
        printf("TCP port:\033[32m%d test OK\033[0m\t\t", port);
      }      
    } else {
      //printf("tcp port:%d test ok.\t\t", port);
      printf("TCP port:\033[32m%d test OK\033[0m\t\t", port);
    }

    //================ check udp port ================ 
    if (g_pktLen >= tsRpcMaxUdpSize) {
      pktLen = tsRpcMaxUdpSize - 1000;
    } else {
      pktLen = g_pktLen;
    }
    
    spi = 0;
    ret = rpcCheckPortImpl(g_serverFqdn, port, pktLen, spi);
    if (ret != 0) {
      spi = 1;
      ret = rpcCheckPortImpl(g_serverFqdn, port, pktLen, spi);
      if (ret != 0) {        
        printf("udp port:%d test fail.\t\n", port);
      } else {
        //printf("udp port:%d test ok.\t\n", port);
        printf("UDP port:\033[32m%d test OK\033[0m\t\n", port);
      }      
    } else {
      //printf("udp port:%d test ok.\t\n", port);
      printf("UDP port:\033[32m%d test OK\033[0m\t\n", port);
    }
  }
  
  printf("\n");
  return ;
}

static void taosNetTestClient(int flag) {
  uint32_t serverIp = taosGetIpFromFqdn(g_serverFqdn);
  if (serverIp == 0xFFFFFFFF) {
    printf("Failed to resolve FQDN:%s", g_serverFqdn); 
    exit(-1);
  }

  if (0 == flag) {
    checkPort(serverIp, g_startPort, g_endPort, g_pktLen);
  } else {
    rpcCheckPort(serverIp);
  }
  
  return;
}

static void taosNetTestServer(uint16_t startPort, uint16_t endPort, int pktLen) {

  int port = startPort;
  int num = endPort - startPort + 1;

  if (num < 0) {
    num = 1;
  }
  
  pthread_t *pids = malloc(2 * num * sizeof(pthread_t));
  info_s *     tinfos = malloc(num * sizeof(info_s));
  info_s *     uinfos = malloc(num * sizeof(info_s));

  for (size_t i = 0; i < num; i++) {
    info_s *tcpInfo = tinfos + i;
    tcpInfo->port = (uint16_t)(port + i);
    tcpInfo->pktLen = pktLen;

    if (pthread_create(pids + i, NULL, bindTcpPort, tcpInfo) != 0) 
    {
      printf("create thread fail, port:%d.\n", port);
      exit(-1);
    }

    info_s *udpInfo = uinfos + i;
    udpInfo->port = (uint16_t)(port + i);
    if (pthread_create(pids + num + i, NULL, bindUdpPort, udpInfo) != 0)
    {                          
      printf("create thread fail, port:%d.\n", port);
      exit(-1);
    }
  }
  
  for (int i = 0; i < num; i++) {
    pthread_join(pids[i], NULL);
    pthread_join(pids[(num + i)], NULL);
  }
}


void taosNetTest(CmdArguments *args) {
  if (0 == args->pktLen) {
    g_pktLen = 1000;
  } else {
    g_pktLen = args->pktLen;
  }
  
  if (args->port && args->endPort) {
    if (args->port > args->endPort) {
      printf("endPort[%d] must not lesss port[%d]\n", args->endPort, args->port);
      exit(-1);
    }
  }  
 
  if (args->host && args->host[0] != 0) {
    if (strlen(args->host) >= TSDB_EP_LEN) {
      printf("host invalid: %s\n", args->host);
      exit(-1);
    }

    taosGetFqdnPortFromEp(args->host, g_serverFqdn, &g_startPort);
  } else {
    tstrncpy(g_serverFqdn, "127.0.0.1", TSDB_IPv4ADDR_LEN);    
    g_startPort = tsServerPort;
  }
  
  if (args->port) {
    g_startPort = args->port;
  }
  
  if (args->endPort) {
    g_endPort = args->endPort;
  }
  
  if (g_startPort > g_endPort) {
    printf("endPort[%d] must not lesss port[%d]\n", g_endPort, g_startPort);
    exit(-1);
  }

  
  if (args->is_use_passwd) {
    if (args->password == NULL) args->password = getpass("Enter password: ");
  } else {
    args->password = TSDB_DEFAULT_PASS;
  }
  tstrncpy(g_pass, args->password, TSDB_PASSWORD_LEN);

  if (args->user == NULL) {
    args->user = TSDB_DEFAULT_USER;
  }
  tstrncpy(g_user, args->user, TSDB_USER_LEN);
  
  if (0 == strcmp("client", args->netTestRole)) {
    printf("host: %s\tstart port: %d\tend port: %d\tpacket len: %d\n", g_serverFqdn, g_startPort, g_endPort, g_pktLen);
    taosNetTestClient(0);
  } else if (0 == strcmp("clients", args->netTestRole)) {
    printf("host: %s\tstart port: %d\tend port: %d\tpacket len: %d\n", g_serverFqdn, g_startPort, g_endPort, g_pktLen);
    taosNetTestClient(1);
  } else if (0 == strcmp("server", args->netTestRole)) {
    taosNetTestServer(g_startPort, g_endPort, g_pktLen);
  }
}

