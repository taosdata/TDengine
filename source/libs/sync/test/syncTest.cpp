#include <stdio.h>
#include "gtest/gtest.h"
#include "syncIO.h"
#include "syncInt.h"
#include "syncRaftStore.h"

void *pingFunc(void *param) {
  SSyncIO *io = (SSyncIO *)param;
  while (1) {
    sDebug("io->ping");
    // io->ping(io);
    sleep(1);
  }
  return NULL;
}

int main() {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

  sTrace("sync log test: trace");
  sDebug("sync log test: debug");
  sInfo("sync log test: info");
  sWarn("sync log test: warn");
  sError("sync log test: error");
  sFatal("sync log test: fatal");

  SRaftStore *pRaftStore = raftStoreOpen("./raft_store.json");
  // assert(pRaftStore != NULL);

  // raftStorePrint(pRaftStore);

  // pRaftStore->currentTerm = 100;
  // pRaftStore->voteFor.addr = 200;
  // pRaftStore->voteFor.vgId = 300;

  // raftStorePrint(pRaftStore);

  // raftStorePersist(pRaftStore);

  // sDebug("sync test");

  // SSyncIO *syncIO = syncIOCreate();
  // assert(syncIO != NULL);

  // syncIO->start(syncIO);

  // sleep(2);

  // pthread_t tid;
  // pthread_create(&tid, NULL, pingFunc, syncIO);

  // while (1) {
  //   sleep(1);
  // }
  return 0;
}
