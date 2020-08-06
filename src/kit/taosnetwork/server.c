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
#include <pthread.h>

#define MAX_PKG_LEN (64*1000)
#define BUFFER_SIZE (MAX_PKG_LEN + 1024)

typedef struct {
  int port;
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
    {0, 'p', "port", 0, "The TCP or UDP port number to use for the connection. Default is 6041.", 1},
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

static void *bindTcpPort(void *sarg) {
  info_s *pinfo = (info_s *)sarg;
  int   port = pinfo->port;
  int   serverSocket;

  struct sockaddr_in server_addr;
  struct sockaddr_in clientAddr;
  int                addr_len = sizeof(clientAddr);
  int                client;
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
          printf("recv Client: %s pkg from TCP port: %d fail:%s.\n", inet_ntoa(clientAddr.sin_addr), port, strerror(errno));
          close(serverSocket);
          return NULL;
        }
      } else {
        nleft -= nread;
        ptr += nread;
        iDataNum += nread;
      }      
    }
    
    printf("recv Client: %s pkg from TCP port: %d, pkg len: %d\n", inet_ntoa(clientAddr.sin_addr), port, iDataNum);
    if (iDataNum > 0) {
      send(client, buffer, iDataNum, 0);
      break;
    }
  }
  close(serverSocket);
  return NULL;
}

static void *bindUdpPort(void *sarg) {
  info_s *pinfo = (info_s *)sarg;
  int   port = pinfo->port;
  int   serverSocket;

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
      printf("recv Client: %s pkg from UDP port: %d, pkg len: %d\n", inet_ntoa(clientAddr.sin_addr), port, iDataNum);
      //printf("Read msg from udp:%s ... %s\n", buffer, buffer+iDataNum-16);

      sendto(serverSocket, buffer, iDataNum, 0, (struct sockaddr *)&clientAddr, (int)sin_size);
    }
  }

  close(serverSocket);
  return NULL;
}


int main(int argc, char *argv[]) {
  SArguments arguments = {"127.0.0.1", 6030, 6060, 1000};
  argp_parse(&argp, argc, argv, 0, 0, &arguments);
  if (arguments.pktLen > MAX_PKG_LEN) {
    printf("test pkg len overflow: %d, max len not greater than %d bytes\n", arguments.pktLen, MAX_PKG_LEN);
    exit(0);
  }

  int port = arguments.port;

  int num = arguments.max_port - arguments.port + 1;

  if (num < 0) {
    num = 1;
  }
  pthread_t *pids = malloc(2 * num * sizeof(pthread_t));
  info_s *     tinfos = malloc(num * sizeof(info_s));
  info_s *     uinfos = malloc(num * sizeof(info_s));

  for (size_t i = 0; i < num; i++) {
    info_s *tcpInfo = tinfos + i;
    tcpInfo->port = port + i;
    tcpInfo->pktLen = arguments.pktLen;

    if (pthread_create(pids + i, NULL, bindTcpPort, tcpInfo) != 0) 
    {
      printf("create thread fail, port:%d.\n", port);
      exit(-1);
    }

    info_s *udpInfo = uinfos + i;
    udpInfo->port = port + i;
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
