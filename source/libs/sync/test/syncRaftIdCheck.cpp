#include <gtest/gtest.h>
#include "syncTest.h"

void usage(char* exe) {
  printf("Usage: %s host port \n", exe);
  printf("Usage: %s u64 \n", exe);
}

int main(int argc, char** argv) {
  if (argc == 2) {
    uint64_t u64 = atoll(argv[1]);
    char     host[128];
    uint16_t port;
    syncUtilU642Addr(u64, host, sizeof(host), &port);
    printf("%" PRIu64 " -> %s:%d \n", u64, host, port);

  } else if (argc == 3) {
    uint64_t u64;
    char*    host = argv[1];
    uint16_t port = atoi(argv[2]);
    u64 = syncUtilAddr2U64(host, port);
    printf("%s:%d ->: %" PRIu64 " \n", host, port, u64);
  } else {
    usage(argv[0]);
    exit(-1);
  }

  return 0;
}
