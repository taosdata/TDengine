// Write CPP code here
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#define MAX 80
#define PORT 8080
#define SA struct sockaddr
void func(int sockfd, int size) {
  char *buff = taosMemoryMalloc(size + 1);
  if (buff == NULL) {
    printf("failed to allocate memory\n");
    return;
  }
  int counter = 0;
  while (1) {
    memset(buff, 0, size);
    int bytes = 0;
    while (bytes < size) {
      bytes += write(sockfd, buff + bytes, size - bytes);
    }
    printf("%d: send data to server, bytes:%d\n", counter, bytes);

    memset(buff, 0, size);
    bytes = 0;
    while (bytes < size) {
      bytes += read(sockfd, buff + bytes, size - bytes);
    }
    printf("%d: recv data from server, bytes:%d\n", counter, bytes);
    counter++;
    sleep(1);
  }
}

int main(int argc, char const *argv[]) {
  int sockfd, connfd;
  struct sockaddr_in servaddr, cli;

  if (argc < 4) {
    printf("%s serverip port size", argv[0]);
    return -1;
  }

  // socket create and varification
  sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd == -1) {
    printf("socket creation failed...\n");
    exit(0);
  } else
    printf("Socket successfully created..\n");
  bzero(&servaddr, sizeof(servaddr));

  // assign IP, PORT
  servaddr.sin_family = AF_INET;
  servaddr.sin_addr.s_addr = inet_addr(argv[1]);
  servaddr.sin_port = htons(atoi(argv[2]));

  // connect the client socket to server socket
  if (connect(sockfd, (SA *)&servaddr, sizeof(servaddr)) != 0) {
    printf("connection with the server failed...\n");
    exit(0);
  } else
    printf("connected to the server..\n");

  // function for chat
  func(sockfd, atoi(argv[3]));

  // close the socket
  close(sockfd);
}