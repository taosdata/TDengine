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

#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#define SERVER_PORT 8000
#define SIZE 200

int main() {
  struct sockaddr_in servaddr, cliaddr;
  socklen_t          cliaddr_len;
  int                client_sockfd;
  char               buf[SIZE];
  char               recvbuf[SIZE];

  int i, n, flag = 0;

  int len, iDataNum;

  client_sockfd = socket(AF_INET, SOCK_STREAM, 0);
  bzero(&servaddr, sizeof(servaddr));
  servaddr.sin_family = AF_INET;
  servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
  servaddr.sin_port = htons(SERVER_PORT);

  if (connect(client_sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
    printf("Connected error..\n");
    return 0;
  }
  printf("Connected to server..\n");

  /*循环的发送接收信息并打印接收信息（可以按需发送）--recv返回接收到的字节数，send返回发送的字节数*/
  while (1) {
    printf("Enter string to send:");
    scanf("%s", buf);
    if (!strcmp(buf, "quit")) {
      break;
    }
    len = (sizeof buf);

    recvbuf[0] = '\0';

    iDataNum = recv(client_sockfd, recvbuf, SIZE, 0);

    recvbuf[iDataNum] = '\0';

    printf("%s\n", recvbuf);
  }
  return 0;
}
