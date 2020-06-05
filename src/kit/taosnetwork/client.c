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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define BUFFER_SIZE 200

typedef struct {
  int   port;
  char *host[15];
} info;

void *checkPort(void *sarg) {
  info *pinfo = (info *)sarg;
  int   port = pinfo->port;
  char *host = *pinfo->host;
  int   clientSocket;

  struct sockaddr_in serverAddr;
  char               sendbuf[BUFFER_SIZE];
  char               recvbuf[BUFFER_SIZE];
  int                iDataNum;
  if ((clientSocket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    perror("socket");
    return NULL;
  }
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(port);

  serverAddr.sin_addr.s_addr = inet_addr(host);

  printf("=================================\n");
  if (connect(clientSocket, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0) {
    perror("connect");
    return NULL;
  }
  printf("Connect to: %s:%d...success\n", host, port);

  sprintf(sendbuf, "send port_%d", port);
  send(clientSocket, sendbuf, strlen(sendbuf), 0);
  printf("Send msg_%d: %s\n", port, sendbuf);

  recvbuf[0] = '\0';
  iDataNum = recv(clientSocket, recvbuf, BUFFER_SIZE, 0);
  recvbuf[iDataNum] = '\0';
  printf("Read ack msg_%d: %s\n", port, recvbuf);

  printf("=================================\n");
  close(clientSocket);
  return NULL;
}

void *checkUPort(void *sarg) {
  info *pinfo = (info *)sarg;
  int   port = pinfo->port;
  char *host = *pinfo->host;
  int   clientSocket;

  struct sockaddr_in serverAddr;
  char               sendbuf[BUFFER_SIZE];
  char               recvbuf[BUFFER_SIZE];
  int                iDataNum;
  if ((clientSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0) {
    perror("socket");
    return NULL;
  }
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(port);

  serverAddr.sin_addr.s_addr = inet_addr(host);

  printf("=================================\n");

  sprintf(sendbuf, "send msg port_%d by udp", port);

  socklen_t sin_size = sizeof(*(struct sockaddr*)&serverAddr);

  sendto(clientSocket, sendbuf, strlen(sendbuf), 0, (struct sockaddr *)&serverAddr, (int)sin_size);

  printf("Send msg_%d by udp: %s\n", port, sendbuf);

  recvbuf[0] = '\0';
  iDataNum = recvfrom(clientSocket, recvbuf, BUFFER_SIZE, 0, (struct sockaddr *)&serverAddr, &sin_size);
  recvbuf[iDataNum] = '\0';
  printf("Read ack msg_%d from udp: %s\n", port, recvbuf);

  printf("=================================\n");
  close(clientSocket);
  return NULL;
}

int main() {
  int   port = 6020;
  char *host = "127.0.0.1";
  info *tinfo = malloc(sizeof(info));
  info *uinfo = malloc(sizeof(info));

  for (size_t i = 0; i < 30; i++) {
    port++;
    printf("For test: %s:%d\n", host, port);

    *tinfo->host = host;
    tinfo->port = port;
    checkPort(tinfo);

    *uinfo->host = host;
    uinfo->port = port;
    checkUPort(uinfo);
  }
  free(tinfo);
  free(uinfo);
}