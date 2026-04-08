#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#define SA struct sockaddr

// Function designed for chat between client and server.
void func(int sockfd, int size) {
  char *buff = taosMemoryMalloc(size + 1);
  if (buff == NULL) {
    printf("failed to allocate memory\n");
    return;
  }

  int n;
  // infinite loop for chat
  int counter = 0;
  for (;;) {
    memset(buff, 0, size);

    // read the message from client and copy it in buffer
    int bytes = 0;
    while (bytes < size) {
      bytes += read(sockfd, buff + bytes, size - bytes);
    }
    // print buffer which contains the client contents
    printf("%d: recv data from client: %d bytes\n", counter, bytes);
    memset(buff, 0, size);
    n = 0;
    // copy server message in the buffer
    strcpy(buff, "message from server....");

    // and send that buffer to client
    bytes = 0;
    while (bytes < size) {
      bytes += write(sockfd, buff + bytes, size - bytes);
    }
    printf("%d: send data to client: %d bytes\n", counter, bytes);
    counter++;
  }
}

// Driver function
int main(int argc, char const *argv[]) {
  int sockfd, connfd, len;
  struct sockaddr_in servaddr, cli;

  if (argc < 4) {
    printf("%s ip_addr port size", argv[0]);
    return -1;
  }

  // socket create and verification
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

  // Binding newly created socket to given IP and verification
  if ((bind(sockfd, (SA *)&servaddr, sizeof(servaddr))) != 0) {
    printf("socket bind failed...\n");
    exit(0);
  } else
    printf("Socket successfully binded..\n");

  // Now server is ready to listen and verification
  if ((listen(sockfd, 5)) != 0) {
    printf("Listen failed...\n");
    exit(0);
  } else
    printf("Server listening..\n");
  len = sizeof(cli);

  // Accept the data packet from client and verification
  connfd = accept(sockfd, (SA *)&cli, &len);
  if (connfd < 0) {
    printf("server acccept failed...\n");
    exit(0);
  } else
    printf("server acccept the client...\n");

  // Function for chatting between client and server
  func(connfd, atoi(argv[3]));

  // After chatting close the socket
  close(sockfd);
}