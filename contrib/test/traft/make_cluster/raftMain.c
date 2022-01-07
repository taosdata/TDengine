#include <assert.h>
#include <getopt.h>
#include <pthread.h>
#include <raft.h>
#include <raft/uv.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include "common.h"
#include "config.h"
#include "console.h"
#include "raftServer.h"
#include "simpleHash.h"
#include "util.h"

const char *exe_name;

void *startConsoleFunc(void *param) {
  RaftServer *pRaftServer = (RaftServer *)param;
  console(pRaftServer);
  return NULL;
}

void usage() {
  printf("\nusage: \n");
  printf("%s --addr=127.0.0.1:10000 --dir=./data \n", exe_name);
  printf("\n");
}

RaftServerConfig gConfig;
RaftServer       gRaftServer;

int main(int argc, char **argv) {
  srand(time(NULL));
  int32_t ret;

  exe_name = argv[0];
  if (argc < 3) {
    usage();
    exit(-1);
  }

  ret = parseConf(argc, argv, &gConfig);
  if (ret != 0) {
    usage();
    exit(-1);
  }
  printConf(&gConfig);

  if (!dirOK(gConfig.baseDir)) {
    ret = mkdir(gConfig.baseDir, 0775);
    if (ret != 0) {
      fprintf(stderr, "mkdir error, %s \n", gConfig.baseDir);
      exit(-1);
    }
  }

  ret = raftServerInit(&gRaftServer, &gConfig);
  if (ret != 0) {
    fprintf(stderr, "raftServerInit error \n");
    exit(-1);
  }

  /*
    pthread_t tidRaftServer;
    pthread_create(&tidRaftServer, NULL, startServerFunc, &gRaftServer);
  */

  pthread_t tidConsole;
  pthread_create(&tidConsole, NULL, startConsoleFunc, &gRaftServer);

  while (1) {
    sleep(10);
  }

  return 0;
}
