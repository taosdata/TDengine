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

#include <argp.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <wordexp.h>

#define    MAX_PKG_LEN          (64*1000)
#define    BUFFER_SIZE          (MAX_PKG_LEN + 1024)
#define    TSDB_FQDN_LEN        128
#define    TSDB_IPv4ADDR_LEN    16

typedef struct {
  uint16_t port;
  uint32_t hostIp;
  char     fqdn[TSDB_FQDN_LEN];
  uint16_t pktLen;
} info_s;

typedef struct Arguments {
  char     host[TSDB_IPv4ADDR_LEN];
  char     fqdn[TSDB_FQDN_LEN];
  uint16_t port;
  uint16_t max_port;
  uint16_t pktLen;
} SArguments;

static struct argp_option options[] = {
    {0, 'h', "host ip", 0, "The host ip to connect to TDEngine. Default is localhost.", 0},
    {0, 'p', "port", 0, "The TCP or UDP port number to use for the connection. Default is 6030.", 1},
    {0, 'm', "max port", 0, "The max TCP or UDP port number to use for the connection. Default is 6042.", 2},
    {0, 'f', "host fqdn", 0, "The host fqdn to connect to TDEngine.", 3},
    {0, 'l', "test pkg len", 0, "The len of pkg for test. Default is 1000 Bytes, max not greater than 64k Bytes.\nNotes: This parameter must be consistent between the client and the server.", 3}};

static error_t parse_opt(int key, char *arg, struct argp_state *state) {
  wordexp_t full_path;
  SArguments *arguments = state->input;
  switch (key) {
    case 'h':
      if (wordexp(arg, &full_path, 0) != 0) {
        fprintf(stderr, "Invalid host ip %s\n", arg);
        return -1;
      }
      strcpy(arguments->host, full_path.we_wordv[0]);
      wordfree(&full_path);
      break;
    case 'p':
      arguments->port = atoi(arg);
      break;
    case 'm':
      arguments->max_port = atoi(arg);
      break;
    case 'l':
      arguments->pktLen = atoi(arg);
      break;
    case 'f':
      if (wordexp(arg, &full_path, 0) != 0) {
        fprintf(stderr, "Invalid host fqdn %s\n", arg);
        return -1;
      }
      strcpy(arguments->fqdn, full_path.we_wordv[0]);
      wordfree(&full_path);
      break;    

    default:
      return ARGP_ERR_UNKNOWN;
  }
  return 0;
}

static struct argp argp = {options, parse_opt, 0, 0};

int checkTcpPort(info_s *info) {
  int   clientSocket;

  struct sockaddr_in serverAddr;
  char               sendbuf[BUFFER_SIZE];
  char               recvbuf[BUFFER_SIZE];
  int                iDataNum = 0;
  if ((clientSocket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    printf("socket() fail: %s\n", strerror(errno));
    return -1;
  }
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(info->port);

  serverAddr.sin_addr.s_addr = info->hostIp;

  //printf("=================================\n");
  if (connect(clientSocket, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0) {
    printf("connect() fail: %s\n", strerror(errno));
    return -1;
  }
  //printf("Connect to: %s:%d...success\n", host, port);
  memset(sendbuf, 0, BUFFER_SIZE);
  memset(recvbuf, 0, BUFFER_SIZE);

  struct in_addr ipStr;
  memcpy(&ipStr, &info->hostIp, 4);
  sprintf(sendbuf, "client send tcp pkg to %s:%d, content: 1122334455", inet_ntoa(ipStr), info->port);
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
        close(clientSocket);
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

  close(clientSocket);
  return 0;
}

int checkUdpPort(info_s *info) {
  int   clientSocket;

  struct sockaddr_in serverAddr;
  char               sendbuf[BUFFER_SIZE];
  char               recvbuf[BUFFER_SIZE];
  int                iDataNum = 0;
  if ((clientSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0) {
    perror("socket");
    return -1;
  }
  
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(info->port);
  serverAddr.sin_addr.s_addr = info->hostIp;
  
  memset(sendbuf, 0, BUFFER_SIZE);
  memset(recvbuf, 0, BUFFER_SIZE);

  struct in_addr ipStr;
  memcpy(&ipStr, &info->hostIp, 4);
  sprintf(sendbuf, "client send udp pkg to %s:%d, content: 1122334455", inet_ntoa(ipStr), info->port);
  sprintf(sendbuf + info->pktLen - 16, "1122334455667788");

  socklen_t sin_size = sizeof(*(struct sockaddr *)&serverAddr);

  int code = sendto(clientSocket, sendbuf, info->pktLen, 0, (struct sockaddr *)&serverAddr, (int)sin_size);
  if (code < 0) {
    perror("sendto");
    return -1;
  }

  iDataNum = recvfrom(clientSocket, recvbuf, BUFFER_SIZE, 0, (struct sockaddr *)&serverAddr, &sin_size);

  if (iDataNum < info->pktLen) {
    printf("Read ack pkg len: %d, less than req pkg len: %d from udp port: %d\n", iDataNum, info->pktLen, info->port);
    return -1;
  }
  
  //printf("Read ack pkg len:%d from udp port: %d, buffer: %s  %s\n", info->pktLen, port, recvbuf, recvbuf+iDataNum-8);
  close(clientSocket);
  return 0;
}

int32_t getIpFromFqdn(const char *fqdn, uint32_t* ip) {
  struct addrinfo hints = {0};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  struct addrinfo *result = NULL;

  int32_t ret = getaddrinfo(fqdn, NULL, &hints, &result);
  if (result) {
    struct sockaddr *sa = result->ai_addr;
    struct sockaddr_in *si = (struct sockaddr_in*)sa;
    struct in_addr ia = si->sin_addr;
    *ip = ia.s_addr;
    freeaddrinfo(result);
    return 0;
  } else {
    printf("Failed get the ip address from fqdn:%s, code:%d, reason:%s", fqdn, ret, gai_strerror(ret));
    return -1;
  }
}

void checkPort(uint32_t hostIp, uint16_t startPort, uint16_t maxPort, uint16_t pktLen) {
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
      printf("tcp port:%d test fail.\t\t", port);
    } else {
      printf("tcp port:%d test ok.\t\t", port);
    }
    
    ret = checkUdpPort(&info);
    if (ret != 0) {
      printf("udp port:%d test fail.\t\t", port);
    } else {
      printf("udp port:%d test ok.\t\t", port);
    }
  }
  
  printf("\n");
  return ;
}

int main(int argc, char *argv[]) {
  SArguments arguments = {"127.0.0.1", "", 6030, 6042, 1000};
  int        ret;
  
  argp_parse(&argp, argc, argv, 0, 0, &arguments);
  if (arguments.pktLen > MAX_PKG_LEN) {
    printf("test pkg len overflow: %d, max len not greater than %d bytes\n", arguments.pktLen, MAX_PKG_LEN);
    exit(0);
  }

  printf("host ip: %s\thost fqdn: %s\tport: %d\tmax_port: %d\tpkgLen: %d\n", arguments.host, arguments.fqdn, arguments.port, arguments.max_port, arguments.pktLen);

  if (arguments.host[0] != 0) {
    printf("\nstart connect to %s test:\n", arguments.host);
    checkPort(inet_addr(arguments.host), arguments.port, arguments.max_port, arguments.pktLen);
    printf("\n");
  }

  if (arguments.fqdn[0] != 0) {
    uint32_t hostIp = 0;
    ret = getIpFromFqdn(arguments.fqdn, &hostIp);
    if (ret) {
      printf("\n");
      return 0;    
    }
    printf("\nstart connetc to %s test:\n", arguments.fqdn);
    checkPort(hostIp, arguments.port, arguments.max_port, arguments.pktLen);
    printf("\n");
  }

  return 0;
}
