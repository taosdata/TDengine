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

#define MAX_PKG_LEN (64*1000)
#define BUFFER_SIZE (MAX_PKG_LEN + 1024)

typedef struct {
  int   port;
  char *host;
  uint16_t pktLen;
} info_s;

typedef struct Arguments {
  char *   host;
  uint16_t port;
  uint16_t max_port;
  uint16_t pktLen;
} SArguments;

static struct argp_option options[] = {
    {0, 'h', "host", 0, "The host to connect to TDEngine. Default is localhost.", 0},
    {0, 'p', "port", 0, "The TCP or UDP port number to use for the connection. Default is 6030.", 1},
    {0, 'm', "max port", 0, "The max TCP or UDP port number to use for the connection. Default is 6060.", 2},
    {0, 'l', "test pkg len", 0, "The len of pkg for test. Default is 1000 Bytes, max not greater than 64k Bytes.\nNotes: This parameter must be consistent between the client and the server.", 3}};

static error_t parse_opt(int key, char *arg, struct argp_state *state) {

  SArguments *arguments = state->input;
  switch (key) {
    case 'h':
      arguments->host = arg;
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

    default:
      return ARGP_ERR_UNKNOWN;
  }
  return 0;
}

static struct argp argp = {options, parse_opt, 0, 0};

int checkTcpPort(info_s *info) {
  int   port = info->port;
  char *host = info->host;
  int   clientSocket;

  struct sockaddr_in serverAddr;
  char               sendbuf[BUFFER_SIZE];
  char               recvbuf[BUFFER_SIZE];
  int                iDataNum;
  if ((clientSocket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    printf("socket() fail: %s\n", strerror(errno));
    return -1;
  }
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(port);

  serverAddr.sin_addr.s_addr = inet_addr(host);

  //printf("=================================\n");
  if (connect(clientSocket, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0) {
    printf("connect() fail: %s\n", strerror(errno));
    return -1;
  }
  //printf("Connect to: %s:%d...success\n", host, port);
  memset(sendbuf, 0, BUFFER_SIZE);
  memset(recvbuf, 0, BUFFER_SIZE);

  sprintf(sendbuf, "client send tcp pkg to %s:%d, content: 1122334455", host, port);
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
        printf("recv ack pkg from TCP port: %d fail:%s.\n", port, strerror(errno));
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
    printf("recv ack pkg len: %d, less than req pkg len: %d from tcp port: %d\n", iDataNum, info->pktLen, port);
    return -1;
  }
  //printf("Read ack pkg len:%d from tcp port: %d, buffer: %s  %s\n", info->pktLen, port, recvbuf, recvbuf+iDataNum-8);

  close(clientSocket);
  return 0;
}

int checkUdpPort(info_s *info) {
  int   port = info->port;
  char *host = info->host;
  int   clientSocket;

  struct sockaddr_in serverAddr;
  char               sendbuf[BUFFER_SIZE];
  char               recvbuf[BUFFER_SIZE];
  int                iDataNum;
  if ((clientSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0) {
    perror("socket");
    return -1;
  }
  
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(port);
  serverAddr.sin_addr.s_addr = inet_addr(host);
  
  memset(sendbuf, 0, BUFFER_SIZE);
  memset(recvbuf, 0, BUFFER_SIZE);

  sprintf(sendbuf, "client send udp pkg to %s:%d, content: 1122334455", host, port);
  sprintf(sendbuf + info->pktLen - 16, "1122334455667788");

  socklen_t sin_size = sizeof(*(struct sockaddr *)&serverAddr);

  int code = sendto(clientSocket, sendbuf, info->pktLen, 0, (struct sockaddr *)&serverAddr, (int)sin_size);
  if (code < 0) {
    perror("sendto");
    return -1;
  }

  iDataNum = recvfrom(clientSocket, recvbuf, BUFFER_SIZE, 0, (struct sockaddr *)&serverAddr, &sin_size);

  if (iDataNum < info->pktLen) {
    printf("Read ack pkg len: %d, less than req pkg len: %d from udp port: %d\n", iDataNum, info->pktLen, port);
    return -1;
  }
  
  //printf("Read ack pkg len:%d from udp port: %d, buffer: %s  %s\n", info->pktLen, port, recvbuf, recvbuf+iDataNum-8);
  close(clientSocket);
  return 0;
}

int main(int argc, char *argv[]) {
  SArguments arguments = {"127.0.0.1", 6030, 6060, 1000};
  info_s  info;
  int ret;
  
  argp_parse(&argp, argc, argv, 0, 0, &arguments);
  if (arguments.pktLen > MAX_PKG_LEN) {
    printf("test pkg len overflow: %d, max len not greater than %d bytes\n", arguments.pktLen, MAX_PKG_LEN);
    exit(0);
  }

  printf("host: %s\tport: %d\tmax_port: %d\tpkgLen: %d\n", arguments.host, arguments.port, arguments.max_port, arguments.pktLen);

  int   port = arguments.port;

  info.host = arguments.host;
  info.pktLen = arguments.pktLen;

  for (; port <= arguments.max_port; port++) {
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
  return 0;
}
