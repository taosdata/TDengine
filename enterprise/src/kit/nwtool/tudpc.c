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
  int bytes = 1024;
  int interval = 1;  // seconds
  int fd;
  struct sockaddr_in destAdd;
  unsigned int addLen, dataLen;
  struct timeval systemTime;
  int64_t st, et;

  if (argc < 3) {
    printf("usage: %s server-ip port bytes interval\n", argv[0]);
    exit(0);
  }

  if (argc >= 4) bytes = atoi(argv[3]);
  if (argc >= 5) interval = atoi(argv[4]);

  if ((fd = (int)socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    printf("ERROR, failed to setup UDP socket, reason:%s\n", strerror(errno));
    exit(1);
  }

  strcpy(buffer, "TAOS Data, Inc.");
  addLen = sizeof(destAdd);

  memset(&destAdd, 0, sizeof(destAdd));
  destAdd.sin_family = AF_INET;
  destAdd.sin_addr.s_addr = inet_addr(argv[1]);
  destAdd.sin_port = htons(atoi(argv[2]));

  while (1) {
    gettimeofday(&systemTime, NULL);
    st = systemTime.tv_sec * 1000000 + systemTime.tv_usec;

    int ret = sendto(fd, buffer, bytes, 0, (struct sockaddr *)&destAdd,
                     sizeof(destAdd));
    if (ret < bytes) {
      printf("failed to send packet to:%s:%s, ret:%d reason:%s\n", argv[1],
             argv[2], ret, strerror(errno));
    } else {
      // printf("%d bytes packet is sent\n", bytes);
    }

    dataLen = recvfrom(fd, buffer, sizeof(buffer), 0,
                       (struct sockaddr *)&destAdd, &addLen);

    gettimeofday(&systemTime, NULL);
    et = systemTime.tv_sec * 1000000 + systemTime.tv_usec;

    printf("%d bytes response received, round trip time:%.3f(ms)\n", dataLen,
           (et - st) / 1000.0);

    sleep(interval);
  }
}
