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
  int                listenfd, connfd;
  char               buf[BUFSIZ];
  int                i, n, flag = 0;

  listenfd = socket(AF_INET, SOCK_STREAM, 0);
  bzero(&servaddr, sizeof(servaddr));
  servaddr.sin_family = AF_INET;
  servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
  servaddr.sin_port = htons(SERVER_PORT);
  bind(listenfd, (struct sockaddr *)&servaddr, sizeof(servaddr));
  listen(listenfd, 20);

  printf("Accepting connections..\n");
  while (1) {
    cliaddr_len = sizeof(cliaddr);
    connfd = accept(listenfd, (struct sockaddr *)&cliaddr,
                    &cliaddr_len);  //如果得不到客户端发来的消息，将会被阻塞，一直等到消息到来
    n = read(connfd, buf, SIZE);    //如果n<=0，表示客户端已断开
    while (1) {
      if (n != 0) {
        for (i = 0; i < n; i++) printf("%c", buf[i]);  //输出客户端发来的信息
      } else {
        printf("Client say close the connection..\n");
        break;
      }
      n = read(connfd, buf, SIZE);
    }
    close(connfd);
  }
}
