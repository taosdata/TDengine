#include <arpa/inet.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define DEFAULT_PORT 12345
#define DEFAULT_SHOW false

#define BUFFER_SIZE 10240

#define ERROR(...)                                                             \
  do {                                                                         \
    fprintf(stderr, __VA_ARGS__);                                              \
    exit(EXIT_FAILURE);                                                        \
  } while (0)

#define CHECK(cond, ...)                                                       \
  if (!(cond))                                                                 \
  ERROR(__VA_ARGS__)

static void parseArgs(int32_t argc, char *argv[], int32_t *serverPort,
                      int32_t *showRequest) {
  int32_t opt;
  while ((opt = getopt(argc, argv, "sp:")) != -1) {
    switch (opt) {
    case 'p':
      *serverPort = atoi(optarg);
      CHECK(*serverPort > 0, "Invalid server port\n");
      break;
    case 's':
      *showRequest = 1;
      break;
    default:
      ERROR("Usage: %s [-p port] [-s]\n", argv[0]);
    }
  }
}

int32_t main(int32_t argc, char *argv[]) {
  int32_t serverPort = DEFAULT_PORT;
  int32_t showRequest = DEFAULT_SHOW;
  parseArgs(argc, argv, &serverPort, &showRequest);

  struct sockaddr_in serverAddr = {
      .sin_family = AF_INET,
      .sin_port = htons(serverPort),
      .sin_addr.s_addr = INADDR_ANY,
  };
  int32_t serverFd = socket(AF_INET, SOCK_STREAM, 0);
  CHECK(serverFd != -1, "Failed to create socket\n");
  CHECK(bind(serverFd, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) !=
            -1,
        "Failed to bind\n");
  CHECK(listen(serverFd, 5) != -1, "Failed to listen\n");
  printf("Server is listening on port %d...\n", serverPort);

  while (true) {
    char *buffer = malloc(BUFFER_SIZE);

    struct sockaddr_in clientAddr;
    socklen_t addrLen;
    int32_t clientFd =
        accept(serverFd, (struct sockaddr *)&clientAddr, &addrLen);
    CHECK(clientFd != -1, "Failed to accept\n");
    printf("Client connected.\n");

    while (true) {
      size_t nrecv = recv(clientFd, buffer, BUFFER_SIZE - 1, 0);
      if (nrecv > 0) {
        if (showRequest) {
          buffer[nrecv] = '\0';
          printf("Received from client: %s\n", buffer);
        }
      } else if (nrecv == 0) {
        printf("Client disconnected.\n");
        break;
      } else {
        ERROR("Failed to Receive\n");
      }
    }
    close(clientFd);
    free(buffer);
  }
  close(serverFd);
  return 0;
}
