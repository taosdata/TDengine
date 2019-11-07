#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>
#include <unistd.h>

char buffer[65536];

int main(int argc, char *argv[]) {
  int fd;
  struct sockaddr_in localAddr, sourceAdd;
  unsigned int addLen, dataLen;

  if (argc < 3) {
    printf("usage: %s server-ip port\n", argv[0]);
    exit(0);
  }

  memset((char *)&localAddr, 0, sizeof(localAddr));
  localAddr.sin_family = AF_INET;
  localAddr.sin_addr.s_addr = inet_addr(argv[1]);
  localAddr.sin_port = htons(atoi(argv[2]));

  if ((fd = (int)socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    printf("ERROR, failed to setup UDP server at %s:%s, reason:%s\n", argv[1],
           argv[2], strerror(errno));
    exit(1);
  }

  /* bind socket to local address */
  if (bind(fd, (struct sockaddr *)&localAddr, sizeof(localAddr)) < 0) {
    printf("ERROR, failed to bind udp socket at %s:%s, %s\n", argv[1], argv[2],
           strerror(errno));
    close(fd);
    return -1;
  }

  memset(&sourceAdd, 0, sizeof(sourceAdd));
  addLen = sizeof(sourceAdd);
  char sourceIp[24];

  while (1) {
    dataLen = recvfrom(fd, buffer, sizeof(buffer), 0,
                       (struct sockaddr *)&sourceAdd, &addLen);
    strcpy(sourceIp, inet_ntoa(sourceAdd.sin_addr));
    printf("%d bytes received from %s:%d\n", dataLen, sourceIp,
           ntohs(sourceAdd.sin_port));

    int ret = sendto(fd, buffer, dataLen, 0, (struct sockaddr *)&sourceAdd,
                     sizeof(sourceAdd));
    if (ret < dataLen) {
      printf("ERROR, failed to send packet to:%s:%d, ret:%d reason:%s\n",
             sourceIp, ntohs(sourceAdd.sin_port), ret, strerror(errno));
    }
  }
}
