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
  int port;
  int type;  // 0: tcp, 1: udo, default: 0
} info;

static void *bindPort(void *sarg) {
  info *pinfo = (info *)sarg;
  int   port = pinfo->port;
  int   type = pinfo->type;
  int   serverSocket;

  struct sockaddr_in server_addr;
  struct sockaddr_in clientAddr;
  int                addr_len = sizeof(clientAddr);
  int                client;
  char               buffer[BUFFER_SIZE];
  int                iDataNum;

  if ((serverSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
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

  if (listen(serverSocket, 5) < 0) {
    perror("listen");
    return NULL;
  }

  printf("Bind port: %d success\n", port);
  while (1) {
    client = accept(serverSocket, (struct sockaddr *)&clientAddr, (socklen_t *)&addr_len);
    if (client < 0) {
      perror("accept");
      continue;
    }
    printf("=================================\n");

    printf("Client ip is %s, Server port is %d\n", inet_ntoa(clientAddr.sin_addr), port);
    while (1) {
      buffer[0] = '\0';
      iDataNum = recv(client, buffer, BUFFER_SIZE, 0);

      if (iDataNum < 0) {
        perror("recv null");
        continue;
      }
      if (iDataNum > 0) {
        buffer[iDataNum] = '\0';
        printf("read msg:%s\n", buffer);
        if (strcmp(buffer, "quit") == 0) break;
        buffer[0] = '\0';

        sprintf(buffer, "ack port_%d", port);
        printf("send ack msg:%s\n", buffer);

        send(client, buffer, strlen(buffer), 0);
        break;
      }
    }
    printf("=================================\n");
  }
  close(serverSocket);
  return NULL;
}

static void *bindUPort(void *sarg) {
  info *pinfo = (info *)sarg;
  int   port = pinfo->port;
  int   type = pinfo->type;
  int   serverSocket;

  struct sockaddr_in server_addr;
  struct sockaddr_in clientAddr;
  int                addr_len = sizeof(clientAddr);
  int                client;
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
  printf("Bind port: %d success\n", port);

  while (1) {
    buffer[0] = '\0';

    sin_size = sizeof(*(struct sockaddr *)&server_addr);

    iDataNum = recvfrom(serverSocket, buffer, BUFFER_SIZE, 0, (struct sockaddr *)&clientAddr, &sin_size);

    if (iDataNum < 0) {
      perror("recvfrom null");
      continue;
    }
    if (iDataNum > 0) {
      printf("=================================\n");

      printf("Client ip is %s, Server port is %d\n", inet_ntoa(clientAddr.sin_addr), port);
      buffer[iDataNum] = '\0';
      printf("Read msg from udp:%s\n", buffer);
      if (strcmp(buffer, "quit") == 0) break;
      buffer[0] = '\0';

      sprintf(buffer, "ack port_%d by udp", port);
      printf("Send ack msg by udp:%s\n", buffer);

      sendto(serverSocket, buffer, strlen(buffer), 0, (struct sockaddr *)&clientAddr, (int)sin_size);

      send(client, buffer, strlen(buffer), 0);
      printf("=================================\n");
    }
  }

  close(serverSocket);
  return NULL;
}


int main() {
  int        port = 6020;
  pthread_t *pids = malloc(60 * sizeof(pthread_t));
  info *     infos = malloc(30 * sizeof(info));
  info *     uinfos = malloc(30 * sizeof(info));

  for (size_t i = 0; i < 30; i++) {
    port++;

    info *pinfo = infos++;
    pinfo->port = port;

    if (pthread_create(pids + i, NULL, bindPort, pinfo) != 0)  //创建线程
    {                                                          //创建线程失败
      printf("创建线程失败: %d.\n", port);
      exit(0);
    }

    info *uinfo = uinfos++;
    uinfo->port = port;
    uinfo->type = 1;
    if (pthread_create(pids + 30 + i, NULL, bindUPort, uinfo) != 0)  //创建线程
    {                                                                //创建线程失败
      printf("创建线程失败: %d.\n", port);
      exit(0);
    }
  }
  for (int i = 0; i < 30; i++) {
    pthread_join(pids[i], NULL);
    pthread_join(pids[(10 + i)], NULL);
  }
}
